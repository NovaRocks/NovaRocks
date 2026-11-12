// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

use std::sync::Arc;

use crate::iceberg::spec::{FormatVersion, ManifestFile, ManifestStatus, TableMetadata};
use crate::iceberg::{Error, ErrorKind, Result};
use crate::manifest::binding::BoundEntry;
use crate::manifest::source::{BindingOrigin, ManifestSource};

use super::manifest_encoding::{ManifestWriteSource, SourcedManifestEntry};
use super::model::{AddedContent, ArtifactWriter, FrozenEntry};
use super::preparation::PreparationContext;
use super::staging::{ManifestEntryWrite, write_sourced_manifest};

mod packing;
mod policy;
mod window;

pub(crate) use window::ordered_window;

pub(crate) struct MergeCandidate<'a> {
    descriptor: ManifestFile,
    committed: Option<(i64, &'a str)>,
}

impl<'a> MergeCandidate<'a> {
    pub(crate) fn committed(
        descriptor: ManifestFile,
        snapshot_id: i64,
        list_path: &'a str,
    ) -> Self {
        Self {
            descriptor,
            committed: Some((snapshot_id, list_path)),
        }
    }

    pub(crate) fn pending(descriptor: ManifestFile) -> Self {
        Self {
            descriptor,
            committed: None,
        }
    }

    fn source(&self, snapshot_id: i64) -> ManifestSource<'_> {
        match self.committed {
            Some((snapshot_id, list_path)) => {
                ManifestSource::committed(snapshot_id, list_path, &self.descriptor)
            }
            None => ManifestSource::pending(snapshot_id, &self.descriptor),
        }
    }
}

/// The candidates in each content stream are supplied in new-before-retained
/// order. Existing descriptors are untouched unless their bin is selected.
pub(crate) async fn merge_manifests(
    artifacts: &dyn ArtifactWriter,
    metadata: &TableMetadata,
    snapshot_id: i64,
    context: PreparationContext,
    candidates: Vec<MergeCandidate<'_>>,
) -> Result<Vec<ManifestFile>> {
    merge_manifests_scoped(artifacts, metadata, snapshot_id, context, candidates, None).await
}

pub(crate) async fn merge_manifests_scoped(
    artifacts: &dyn ArtifactWriter,
    metadata: &TableMetadata,
    snapshot_id: i64,
    context: PreparationContext,
    candidates: Vec<MergeCandidate<'_>>,
    scope: Option<&crate::manifest::target_query::ManifestReadScope>,
) -> Result<Vec<ManifestFile>> {
    let layouts = packing::plan(candidates, context.merge_policy.map_err(invalid)?)?;
    let results = ordered_window(
        context.io_window_width,
        layouts,
        || artifacts.check_active(),
        |layout| async move {
            match layout {
                packing::Layout::Keep(candidates) => Ok(candidates
                    .into_iter()
                    .map(|candidate| candidate.descriptor)
                    .collect::<Vec<_>>()),
                packing::Layout::Merge(bin) => Ok(vec![
                    merge_bin(artifacts, metadata, snapshot_id, bin, scope).await?,
                ]),
            }
        },
    )
    .await?;
    Ok(results.into_iter().flatten().collect())
}

async fn merge_bin(
    artifacts: &dyn ArtifactWriter,
    metadata: &TableMetadata,
    snapshot_id: i64,
    bin: Vec<MergeCandidate<'_>>,
    scope: Option<&crate::manifest::target_query::ManifestReadScope>,
) -> Result<ManifestFile> {
    let spec_id = bin[0].descriptor.partition_spec_id;
    let content = bin[0].descriptor.content;
    let spec = metadata.partition_spec_by_id(spec_id).ok_or_else(|| {
        invalid(format!(
            "Missing partition spec {spec_id} for manifest merge"
        ))
    })?;
    let mut entries = Vec::new();
    for candidate in bin {
        artifacts.check_active()?;
        let source = candidate.source(snapshot_id);
        let cached = scope.and_then(|scope| scope.cached_source(source));
        let loaded;
        let retained;
        let raw = match cached {
            Some(bound) => bound.raw(),
            None => {
                retained = match scope {
                    Some(scope) => scope.cached_raw(source).await?,
                    None => None,
                };
                match retained.as_deref() {
                    Some(raw) => raw.manifest(),
                    None => {
                        loaded = source.load(artifacts.file_io()).await?;
                        &loaded
                    }
                }
            }
        };
        let encoding_source = Arc::new(ManifestWriteSource::from_header(raw.metadata())?);
        let fresh = if cached.is_none() {
            Some(source.bind(raw)?)
        } else {
            None
        };
        let entries_for_source = cached
            .into_iter()
            .flat_map(|bound| bound.entries(source).map(Ok))
            .chain(fresh.into_iter().flatten());
        for bound in entries_for_source {
            artifacts.check_active()?;
            if let Some(entry) = merged_entry(bound?, raw.metadata().format_version, snapshot_id)? {
                entries.push(SourcedManifestEntry {
                    source: encoding_source.clone(),
                    entry,
                });
            }
        }
    }
    write_sourced_manifest(artifacts, metadata, snapshot_id, spec, content, entries).await
}

fn merged_entry(
    bound: BoundEntry<'_>,
    input_version: FormatVersion,
    snapshot_id: i64,
) -> Result<Option<ManifestEntryWrite>> {
    let current_step = bound.sequences.snapshot_id == Some(snapshot_id);
    let file = bound.raw.data_file();
    let entry = match (bound.raw.status, current_step) {
        (ManifestStatus::Deleted, false) => return Ok(None),
        (ManifestStatus::Deleted, true) => ManifestEntryWrite::Deleted {
            file: file.clone(),
            frozen: FrozenEntry::from_bound_entry(&bound)?,
        },
        (ManifestStatus::Added, true) => {
            if !matches!(bound.source.origin, BindingOrigin::Pending { .. })
                || (input_version != FormatVersion::V1 && bound.sequences.file_sequence.is_some())
            {
                return Err(invalid(
                    "Current-step added manifest entry has an assigned file sequence",
                ));
            }
            let spec_id = bound.source.descriptor.partition_spec_id;
            let file = if file.first_row_id() == bound.first_row_id {
                file.clone()
            } else {
                super::data_file::clone_data_file_with_first_row_id(
                    file,
                    spec_id,
                    bound.first_row_id,
                )
                .map_err(invalid)?
            };
            let added = match bound.sequences.data_sequence {
                Some(sequence) => AddedContent::rewritten_data(file, spec_id, sequence)?,
                None => AddedContent::new_logical_data(file, spec_id)?,
            };
            ManifestEntryWrite::Added(added)
        }
        (_, _) => ManifestEntryWrite::Existing {
            file: file.clone(),
            frozen: FrozenEntry::from_bound_entry(&bound)?,
        },
    };
    Ok(Some(entry))
}

fn invalid(message: impl Into<String>) -> Error {
    Error::new(ErrorKind::DataInvalid, message)
}

#[cfg(test)]
mod tests;
