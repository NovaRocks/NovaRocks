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

//! Frozen whole-table physical rewrite and read-only compaction group facts.
//!
//! The preparer preserves source age and logical identity, while the actual
//! manifest-list writer assigns only historically unassigned row ranges.

#[cfg(test)]
use std::collections::BTreeMap;
use std::collections::HashMap;

use crate::iceberg::io::FileIO;
#[cfg(test)]
use crate::iceberg::spec::{DataContentType, FormatVersion, Operation};
use crate::iceberg::spec::{DataFile, ManifestContentType};
use crate::iceberg::table::Table;
#[cfg(test)]
use async_trait::async_trait;
use novarocks_spi::connector::{ConnectorError, ConnectorErrorKind};

struct LiveManifestEntry {
    data_file: DataFile,
    partition_spec_id: i32,
    sequence_number: i64,
}

#[derive(Default)]
pub(crate) struct LiveFiles {
    data_files: Vec<LiveManifestEntry>,
    delete_files: Vec<LiveManifestEntry>,
}

/// Provider-private view of how many data files the rewrite action would put
/// into a single group. Only the scalar is ever published; the grouping rule
/// stays inside this module.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct LiveDataFileCompactionStats {
    pub max_compactable_data_files: i64,
}

/// Enumerates the table's live manifests and reports the largest group this
/// rewrite action would form. `preserve_row_lineage` decides whether the data
/// sequence number participates in grouping, because preserve-mode rewrites
/// must not fold rows with different sequence numbers into one output file.
pub async fn current_live_data_file_compaction_stats(
    table: &Table,
    file_io: &FileIO,
    preserve_row_lineage: bool,
) -> Result<LiveDataFileCompactionStats, ConnectorError> {
    let live = enumerate_live_files(table, file_io).await?;
    compactable_file_groups(&live, preserve_row_lineage)
        .map_err(|error| unavailable_compaction(error.to_string()))
}

/// The same grouping author serves the original CPU source and legacy caller.
pub(crate) fn compactable_file_groups(
    live: &LiveFiles,
    preserve_row_lineage: bool,
) -> crate::iceberg::Result<LiveDataFileCompactionStats> {
    let mut groups: HashMap<String, i64> = HashMap::new();
    for entry in &live.data_files {
        let sequence = if preserve_row_lineage {
            Some(entry.sequence_number)
        } else {
            None
        };
        let key = format!(
            "spec={};partition={:?};sequence={:?}",
            entry.partition_spec_id,
            entry.data_file.partition(),
            sequence
        );
        let count = groups.entry(key).or_insert(0);
        *count = count
            .checked_add(1)
            .ok_or_else(|| rewrite_invalid("live data file compaction group count overflow"))?;
    }
    Ok(LiveDataFileCompactionStats {
        max_compactable_data_files: groups.into_values().max().unwrap_or(0),
    })
}

async fn enumerate_live_files(
    table: &Table,
    file_io: &FileIO,
) -> Result<LiveFiles, ConnectorError> {
    let mut out = LiveFiles::default();
    let m = table.metadata();
    let snapshot = match m.current_snapshot() {
        Some(s) => s,
        None => return Ok(out),
    };
    let list = crate::manifest::load_manifest_list(snapshot, file_io, m)
        .await
        .map_err(|e| unavailable_compaction(format!("load manifest list failed: {e}")))?;

    for mf in list.entries() {
        let manifest = crate::manifest::load_manifest(mf, file_io)
            .await
            .map_err(|error| {
                if error.kind() == crate::iceberg::ErrorKind::FeatureUnsupported {
                    crate::catalog_control::statistics::scoped_cpu::scoped_snapshot::raw_read_failure(error)
                } else {
                    unavailable_compaction(format!("load manifest {} failed: {error}", mf.manifest_path))
                }
            })?;
        append_live_manifest(&mut out, mf, &manifest);
    }
    Ok(out)
}

fn unavailable_compaction(message: impl Into<String>) -> ConnectorError {
    ConnectorError::new(ConnectorErrorKind::Unavailable, message.into())
        .with_retryable_before_progress()
}

/// Preserve exact live-file and inherited-sequence semantics in one author.
pub(crate) fn append_live_manifest(
    out: &mut LiveFiles,
    descriptor: &crate::iceberg::spec::ManifestFile,
    manifest: &crate::iceberg::spec::Manifest,
) {
    for entry in manifest.entries() {
        if !entry.is_alive() {
            continue;
        }
        let live = LiveManifestEntry {
            data_file: entry.data_file().clone(),
            partition_spec_id: descriptor.partition_spec_id,
            sequence_number: entry
                .sequence_number()
                .unwrap_or(descriptor.sequence_number),
        };
        match descriptor.content {
            ManifestContentType::Data => out.data_files.push(live),
            ManifestContentType::Deletes => out.delete_files.push(live),
        }
    }
}

/// Whole-table physical replacement under the operation's frozen dependency.
/// It never expands the removed set to include a concurrent writer's files.
#[cfg(test)]
pub(crate) struct RewriteDataFilesPreparer;

#[cfg(test)]
#[async_trait]
impl super::staging::Preparer for RewriteDataFilesPreparer {
    async fn prepare(
        &self,
        view: &super::staging::StagedView<'_>,
        intent: &super::model::OperationIntent,
    ) -> crate::iceberg::Result<super::staging::PreparedChange> {
        let (sources, live) = view.observe_live_sources().await?;
        let removed = intent
            .changes()
            .removed
            .iter()
            .map(|f| f.identity().clone())
            .collect::<std::collections::BTreeSet<_>>();
        if removed != live.keys().cloned().collect() {
            return Err(rewrite_invalid(
                "Whole-data rewrite frozen set does not equal the target-ref live set",
            ));
        }
        prepare_physical_rewrite(view, intent, (sources.as_ref(), &live), &removed).await
    }
}

/// Shared by whole-table and selected-data replacement. All inputs are
/// frozen before this attempt; live entries retain their actual source facts.
#[cfg(test)]
pub(crate) async fn prepare_physical_rewrite(
    view: &super::staging::StagedView<'_>,
    intent: &super::model::OperationIntent,
    observation: (
        Option<&super::manifest_sources::SnapshotSources<'_>>,
        &BTreeMap<super::model::EntryIdentity, super::manifest_sources::LocatedEntry>,
    ),
    removed: &std::collections::BTreeSet<super::model::EntryIdentity>,
) -> crate::iceberg::Result<super::staging::PreparedChange> {
    use super::model::SeqField;
    use super::staging::{ManifestEntryWrite, write_manifest};
    let metadata = view.metadata();
    let (sources, live) = observation;
    if metadata.format_version() == FormatVersion::V1 {
        return Err(rewrite_invalid(
            "Physical rewrite does not support V1 tables",
        ));
    }
    if live.is_empty() && intent.changes().added.is_empty() {
        return Ok(super::staging::PreparedChange::default());
    }
    let source = intent
        .start()
        .ok_or_else(|| rewrite_invalid("Physical rewrite has no frozen starting snapshot"))?;
    for frozen in &intent.changes().removed {
        let current = live
            .get(frozen.identity())
            .ok_or_else(|| rewrite_invalid("Frozen rewrite entry is no longer live"))?;
        if current.frozen != *frozen {
            return Err(rewrite_invalid(
                "Frozen rewrite entry facts differ from the live entry",
            ));
        }
    }
    let mut added_groups: BTreeMap<(i32, bool), Vec<super::model::AddedContent>> = BTreeMap::new();
    for added in &intent.changes().added {
        if added.data_sequence() != SeqField::Explicit(source.sequence_number) {
            return Err(rewrite_invalid(
                "Physical rewrite output must carry the frozen starting data sequence",
            ));
        }
        if added.file().content_type() != DataContentType::Data {
            return Err(rewrite_invalid(
                "Whole-data rewrite output must contain only data files",
            ));
        }
        added_groups
            .entry((
                added.partition_spec_id(),
                added.file().first_row_id().is_some(),
            ))
            .or_default()
            .push(added.clone());
    }
    let snapshot_id = view.stage_snapshot_id()?;
    let mut additions = Vec::new();
    for (ordinal, ((spec_id, assigned), added)) in added_groups.into_iter().enumerate() {
        let basis = super::model::ManifestReuseBasis::added(
            view.stage_key(),
            super::model::AddedManifestAuthor::PhysicalRewrite,
            ordinal,
            super::manifest_encoding::encoding_basis(metadata)?,
            added.iter().map(|file| file.file().file_path()),
        );
        if let Some(manifest) = view.artifacts().find_reusable_manifest(&basis)? {
            additions.push(manifest);
            continue;
        }
        let spec = metadata.partition_spec_by_id(spec_id).ok_or_else(|| {
            rewrite_invalid(format!("Rewrite partition spec {spec_id} is absent"))
        })?;
        let assigned_min = (metadata.format_version() == FormatVersion::V3 && assigned)
            .then(|| {
                added
                    .iter()
                    .filter_map(|entry| entry.file().first_row_id())
                    .min()
            })
            .flatten();
        let mut manifest = write_manifest(
            view.artifacts(),
            metadata.format_version(),
            snapshot_id,
            metadata.current_schema().clone(),
            spec.as_ref().clone(),
            ManifestContentType::Data,
            added.into_iter().map(ManifestEntryWrite::Added),
        )
        .await?;
        if let Some(first) = assigned_min {
            manifest.first_row_id = Some(
                u64::try_from(first)
                    .map_err(|_| rewrite_invalid("Assigned rewrite row ID must be nonnegative"))?,
            );
        }
        view.artifacts()
            .remember_reusable_manifest(basis, manifest.clone())?;
        additions.push(manifest);
    }
    let deleted = live
        .iter()
        .filter(|(identity, _)| removed.contains(*identity))
        .map(|(_, entry)| entry)
        .collect::<Vec<_>>();
    let manifests = super::manifest_transform::ordinary_manifests(
        view,
        snapshot_id,
        sources,
        &deleted,
        additions,
    )
    .await?;
    let mut summary = super::overwrite::snapshot_file_summary(&intent.changes().added, &deleted)?;
    super::overwrite::set_physical_rows_after_removal(view, &mut summary)?;
    super::overwrite::prepare_ordinary_snapshot_change(
        view,
        intent,
        snapshot_id,
        Operation::Replace,
        manifests,
        summary,
        false,
    )
    .await
}

fn rewrite_invalid(message: impl Into<String>) -> crate::iceberg::Error {
    crate::iceberg::Error::new(crate::iceberg::ErrorKind::DataInvalid, message)
}

#[cfg(test)]
mod tests;
