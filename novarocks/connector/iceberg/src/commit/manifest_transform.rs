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

use std::collections::BTreeMap;
use std::sync::Arc;

use crate::iceberg::spec::{ManifestContentType, ManifestFile, TableMetadata};
use crate::iceberg::{Error, ErrorKind, Result};
use crate::manifest::source::{BindingOrigin, ManifestSource};

use super::manifest_encoding::{ManifestWriteSource, SourcedManifestEntry};
use super::manifest_merge::{MergeCandidate, ordered_window};
use super::manifest_sources::{LocatedEntry, SnapshotSources};
use super::model::ArtifactWriter;
use super::staging::{ManifestEntryWrite, write_sourced_manifest};

/// Ordinary snapshot producers all filter before the operation's standard merge.
pub(crate) async fn ordinary_manifests(
    view: &super::staging::StagedView<'_>,
    snapshot_id: i64,
    sources: Option<&SnapshotSources<'_>>,
    removed: &[&LocatedEntry],
    added: Vec<ManifestFile>,
) -> Result<Vec<ManifestFile>> {
    let mut candidates = added
        .into_iter()
        .map(MergeCandidate::pending)
        .collect::<Vec<_>>();
    if let Some(sources) = sources {
        candidates.extend(
            apply_removals(
                view.metadata(),
                view.artifacts(),
                view.preparation().io_window_width,
                snapshot_id,
                sources,
                removed.iter().copied(),
            )
            .await?,
        );
    } else if !removed.is_empty() {
        return Err(invalid("Manifest removal requires a completed parent list"));
    }
    super::manifest_merge::merge_manifests(
        view.artifacts(),
        view.metadata(),
        snapshot_id,
        view.preparation(),
        candidates,
    )
    .await
}

type OrdinalSelection<'a> = BTreeMap<usize, &'a LocatedEntry>;

async fn apply_removals<'a, 'entry>(
    metadata: &TableMetadata,
    artifacts: &dyn ArtifactWriter,
    io_window_width: usize,
    snapshot_id: i64,
    sources: &'a SnapshotSources<'_>,
    removed: impl IntoIterator<Item = &'entry LocatedEntry>,
) -> Result<Vec<MergeCandidate<'a>>> {
    let mut selections = BTreeMap::<usize, OrdinalSelection<'_>>::new();
    for entry in removed {
        if selections
            .entry(entry.source_index)
            .or_default()
            .insert(entry.ordinal, entry)
            .is_some()
        {
            return Err(invalid("Multiple removals identify one manifest entry"));
        }
    }
    let mut inputs = Vec::new();
    for (index, source) in sources.sources().enumerate() {
        let selected = selections.remove(&index).unwrap_or_default();
        if !selected.is_empty()
            || source.descriptor.has_added_files()
            || source.descriptor.has_existing_files()
        {
            inputs.push((source, selected));
        }
    }
    if !selections.is_empty() {
        return Err(invalid("Manifest removal refers to an absent source"));
    }
    ordered_window(
        io_window_width,
        inputs,
        || artifacts.check_active(),
        |(source, selected)| async move {
            if selected.is_empty() {
                let BindingOrigin::Committed {
                    snapshot_id,
                    list_path,
                } = source.origin
                else {
                    return Err(invalid("Manifest filter requires a completed list source"));
                };
                Ok(MergeCandidate::committed(
                    source.descriptor.clone(),
                    snapshot_id,
                    list_path,
                ))
            } else {
                filter_source(metadata, artifacts, snapshot_id, source, selected)
                    .await
                    .map(MergeCandidate::pending)
            }
        },
    )
    .await
}

async fn filter_source(
    metadata: &TableMetadata,
    artifacts: &dyn ArtifactWriter,
    snapshot_id: i64,
    source: ManifestSource<'_>,
    mut selected: OrdinalSelection<'_>,
) -> Result<ManifestFile> {
    artifacts.check_active()?;
    let raw = source.load(artifacts.file_io()).await?;
    artifacts.check_active()?;
    let spec = metadata
        .partition_spec_by_id(source.descriptor.partition_spec_id)
        .ok_or_else(|| invalid("Manifest filter references an absent partition spec"))?;
    let storage = Arc::new(ManifestWriteSource::from_header(raw.metadata())?);
    let mut output = Vec::new();
    let mut first_explicit = None;
    let mut unassigned = false;
    for bound in source.bind(&raw)? {
        artifacts.check_active()?;
        let entry = bound?;
        if !entry.raw.is_alive() {
            continue;
        }
        let located = LocatedEntry::from_bound(0, &entry)?;
        let file = entry.raw.data_file().clone();
        let write = if let Some(expected) = selected.remove(&entry.ordinal) {
            if expected.frozen != located.frozen
                || expected.content_type != located.content_type
                || expected.file_format != located.file_format
                || expected.logical_size != located.logical_size
            {
                return Err(invalid("Selected manifest entry changed since observation"));
            }
            ManifestEntryWrite::Deleted {
                file,
                frozen: located.frozen,
            }
        } else {
            if source.descriptor.content == ManifestContentType::Data {
                match entry.first_row_id {
                    Some(id) => {
                        let id = u64::try_from(id).map_err(|_| invalid("Negative first row ID"))?;
                        first_explicit =
                            Some(first_explicit.map_or(id, |first: u64| first.min(id)));
                    }
                    None => unassigned = true,
                }
            }
            ManifestEntryWrite::Existing {
                file,
                frozen: located.frozen,
            }
        };
        output.push(SourcedManifestEntry {
            source: Arc::clone(&storage),
            entry: write,
        });
    }
    if !selected.is_empty() {
        return Err(invalid("Selected live manifest entry is absent"));
    }
    drop(raw);
    let mut descriptor = write_sourced_manifest(
        artifacts,
        metadata,
        snapshot_id,
        spec,
        source.descriptor.content,
        output,
    )
    .await?;
    if !unassigned {
        descriptor.first_row_id = first_explicit;
    }
    Ok(descriptor)
}

fn invalid(message: &str) -> Error {
    Error::new(ErrorKind::DataInvalid, message)
}
