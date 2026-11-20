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

//! `RewriteManifestsCommit` — group manifests by (partition_spec_id,
//! content_type) and merge each group into a single manifest, emitting a
//! single `operation=replace` snapshot.
//!
//! Spec: docs/design/specs/2026-05-07-iceberg-snapshot-lifecycle-design.md §5.
//!
//! Key properties:
//! * snapshot.sequence_number = last_sequence_number + 1 (catalog invariant —
//!   iceberg-rs strictly increases snapshot seq per commit). The per-entry
//!   data_sequence_number / file_sequence_number inside merged manifests are
//!   preserved unchanged.
//! * v3 row-lineage fields (first_row_id, referenced_data_file, etc.) round-trip via
//!   ManifestEntry's public fields
//! * DELETED entries are dropped from merged manifests
//! * ADDED + EXISTING entries become EXISTING in the merged manifest

use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;
#[cfg(test)]
use std::sync::Mutex;

use super::super::manifest_encoding::{
    ManifestWriteSource, SourcedManifestEntry, descriptor_owned_bytes, encoding_basis,
};
use super::super::model::ManifestRewriteReuse;
use super::super::model::{ArtifactWriter, FrozenEntry};
use super::super::staging::{ManifestEntryWrite, write_sourced_manifest};
use crate::iceberg::spec::{ManifestContentType, ManifestFile, Operation, Snapshot, Summary};
use crate::iceberg::{TableRequirement, TableUpdate};
#[cfg(test)]
use crate::iceberg::{
    io::FileIO,
    spec::{DataFileBuilder, FormatVersion, ManifestEntry, ManifestStatus, ManifestWriterBuilder},
};
use crate::manifest::source::ManifestSource;

fn finalize_snapshot_summary(
    mut properties: HashMap<String, String>,
    previous: Option<&Summary>,
) -> Result<HashMap<String, String>, crate::iceberg::Error> {
    const TOTALS: [(&str, &str, &str); 6] = [
        ("total-data-files", "added-data-files", "deleted-data-files"),
        (
            "total-delete-files",
            "added-delete-files",
            "removed-delete-files",
        ),
        ("total-records", "added-records", "deleted-records"),
        ("total-files-size", "added-files-size", "removed-files-size"),
        (
            "total-position-deletes",
            "added-position-deletes",
            "removed-position-deletes",
        ),
        (
            "total-equality-deletes",
            "added-equality-deletes",
            "removed-equality-deletes",
        ),
    ];
    for (total, added, removed) in TOTALS {
        let base = match previous {
            None => 0,
            Some(summary) => match summary
                .additional_properties
                .get(total)
                .and_then(|value| value.parse::<u64>().ok())
            {
                Some(value) => value,
                None => continue,
            },
        };
        let added = properties
            .get(added)
            .and_then(|value| value.parse::<u64>().ok())
            .unwrap_or(0);
        let removed = properties
            .get(removed)
            .and_then(|value| value.parse::<u64>().ok())
            .unwrap_or(0);
        properties.insert(
            total.to_string(),
            base.saturating_add(added)
                .saturating_sub(removed)
                .to_string(),
        );
    }
    properties.insert("engine-name".to_string(), "novarocks".to_string());
    properties.insert(
        "engine-version".to_string(),
        env!("CARGO_PKG_VERSION").to_string(),
    );
    if let Some(previous) = previous {
        for (key, value) in &previous.additional_properties {
            if key.starts_with(crate::document_storage::keyed::keys::RECORD_PREFIX) {
                crate::document_storage::keyed::record::decode_record(key, value)
                    .map_err(keyed_record_error)?;
                properties.insert(key.clone(), value.clone());
            }
        }
    }
    Ok(properties)
}

fn keyed_record_error(error: novarocks_spi::connector::ConnectorError) -> crate::iceberg::Error {
    crate::iceberg::Error::new(
        crate::iceberg::ErrorKind::DataInvalid,
        format!("rewrite Iceberg document manifest: {error}"),
    )
}

/// Stable byte encoding for `ManifestContentType` used as `BTreeMap` key.
/// Data → 0, Deletes → 1. This ensures deterministic iteration order.
fn content_type_key(c: ManifestContentType) -> u8 {
    match c {
        ManifestContentType::Data => 0,
        ManifestContentType::Deletes => 1,
    }
}

/// Spec §5.2 Step 2: group manifest list entries by (partition_spec_id, content_type).
///
/// Uses `BTreeMap<(i32, u8), ...>` so iteration order is deterministic across
/// runs: spec_id ascending, then content_type by encoded byte (Data=0,
/// Deletes=1). This guarantees that the order of entries in the new manifest
/// list — and the order of physical-delete calls — is consistent.
pub(crate) fn group_manifests_by_spec_and_content(
    manifests: &[ManifestFile],
) -> BTreeMap<(i32, u8), Vec<ManifestFile>> {
    let mut groups: BTreeMap<(i32, u8), Vec<ManifestFile>> = BTreeMap::new();
    for m in manifests {
        let key = (m.partition_spec_id, content_type_key(m.content));
        groups.entry(key).or_default().push(m.clone());
    }
    groups
}

/// Merge all entries from a group of manifest files into one new manifest.
/// Drops DELETED entries (spec §5.2 Step 3). Sets remaining entries' status
/// to EXISTING (round-tripping snapshot_id, sequence_number, file_sequence_number
/// and all DataFile v3 row-lineage fields via ManifestWriter::add_existing_file).
async fn merge_manifest_group(
    artifacts: &dyn ArtifactWriter,
    metadata: &crate::iceberg::spec::TableMetadata,
    parent: &Snapshot,
    group: &[ManifestFile],
    snapshot_id: i64,
) -> crate::iceberg::Result<ManifestFile> {
    let first = group
        .first()
        .ok_or_else(|| rewrite_error("Manifest merge requires a nonempty input group"))?;
    let spec_id = first.partition_spec_id;
    let content = first.content;
    let spec = metadata
        .partition_spec_by_id(spec_id)
        .ok_or_else(|| rewrite_error("Manifest merge references a missing partition spec"))?;
    let mut entries = Vec::new();
    for descriptor in group {
        if descriptor.partition_spec_id != spec_id || descriptor.content != content {
            return Err(rewrite_error(
                "Manifest merge input group has conflicting spec or content identities",
            ));
        }
        artifacts.check_active()?;
        let source =
            ManifestSource::committed(parent.snapshot_id(), parent.manifest_list(), descriptor);
        let raw = source.load(artifacts.file_io()).await?;
        let encoding = Arc::new(ManifestWriteSource::from_header(raw.metadata())?);
        for bound in source.bind(&raw)? {
            artifacts.check_active()?;
            let entry = bound?;
            if entry.raw.is_alive() {
                entries.push(SourcedManifestEntry {
                    source: Arc::clone(&encoding),
                    entry: ManifestEntryWrite::Existing {
                        file: entry.raw.data_file().clone(),
                        frozen: FrozenEntry::from_bound_entry(&entry)?,
                    },
                });
            }
        }
    }
    write_sourced_manifest(
        artifacts,
        metadata,
        snapshot_id,
        spec,
        content,
        entries,
        &group
            .iter()
            .map(|manifest| manifest.manifest_path.clone())
            .collect::<Vec<_>>(),
    )
    .await
}

#[cfg(test)]
fn carried_identity(
    entry: &ManifestEntry,
) -> Result<(i64, i64, Option<i64>), crate::iceberg::Error> {
    let snapshot = entry
        .snapshot_id
        .ok_or_else(|| rewrite_error("Carried manifest entry has no original snapshot identity"))?;
    let sequence = entry
        .sequence_number
        .filter(|sequence| *sequence >= 0)
        .ok_or_else(|| rewrite_error("Carried manifest entry has no actual data sequence"))?;
    if entry
        .file_sequence_number
        .is_some_and(|sequence| sequence < 0)
    {
        return Err(rewrite_error(
            "Carried manifest entry has a negative file sequence",
        ));
    }
    Ok((snapshot, sequence, entry.file_sequence_number))
}

pub(crate) struct RewriteManifestsPreparer {
    request: Option<novarocks_spi::connector::ConnectorRequestContext>,
}
impl RewriteManifestsPreparer {
    pub(crate) fn for_request(request: novarocks_spi::connector::ConnectorRequestContext) -> Self {
        Self {
            request: Some(request),
        }
    }
    #[cfg(test)]
    pub(crate) fn new() -> Self {
        Self { request: None }
    }
}

#[async_trait::async_trait]
impl crate::commit::staging::Preparer for RewriteManifestsPreparer {
    async fn prepare(
        &self,
        view: &crate::commit::staging::StagedView<'_>,
        intent: &crate::commit::model::OperationIntent,
    ) -> crate::iceberg::Result<crate::commit::staging::PreparedChange> {
        use crate::commit::staging::PreparedChange;
        let metadata = view.metadata();
        view.artifacts().check_active()?;
        let carry = crate::commit::slot_change::carry_records_and_closure(metadata)?;
        let parent = metadata.snapshot_for_ref(intent.target_ref());
        let no_op = || PreparedChange {
            requirements: vec![TableRequirement::RefSnapshotIdMatch {
                r#ref: intent.target_ref().to_owned(),
                snapshot_id: parent.map(|snapshot| snapshot.snapshot_id()),
            }],
            updates: Vec::new(),
        };
        let Some(parent) = parent else {
            return Ok(no_op());
        };
        let latest =
            crate::manifest::load_manifest_list(parent, view.artifacts().file_io(), metadata)
                .await?;
        view.artifacts().check_active()?;
        let latest = latest.entries();
        let encoding_basis = encoding_basis(metadata)?;
        if let Some(request) = &self.request {
            // These are actual borrowed descriptor facts, not compressed-file
            // bytes or an invented bound for the SDK's decoded DataFile graph.
            let old = view
                .artifacts()
                .rewrite_reuse_backing_bytes(view.stage_key())?;
            let required=descriptor_owned_bytes(latest)?.checked_add(old).and_then(|value| value.checked_add(encoding_basis.capacity() as u64)).and_then(|v|v.checked_mul(8)).and_then(|v|v.checked_add(2*(novarocks_spi::connector::MAX_CONNECTOR_METADATA_MAINTENANCE_PROVIDER_PAYLOAD_BYTES+novarocks_spi::connector::MAX_CONNECTOR_METADATA_MAINTENANCE_MARKER_BYTES) as u64)).and_then(std::num::NonZeroU64::new).ok_or_else(||rewrite_error("manifest backing overflow"))?;
            request
                .reserve_owned_metadata(required)
                .await
                .map_err(|e| {
                    rewrite_error("original manifest backing admission refused").with_source(e)
                })?;
        }
        let cached =
            view.artifacts()
                .find_reusable_rewrite(view.stage_key(), &encoding_basis, latest)?;
        let groups = if cached.is_none() {
            let groups = group_manifests_by_spec_and_content(latest);
            if groups.values().all(|group| group.len() <= 1) {
                return Ok(no_op());
            }
            Some(groups)
        } else {
            None
        };
        let snapshot_id = view.stage_snapshot_id()?;
        let reusable = match cached {
            Some(cache) => cache,
            None => {
                // This is a policy operation: invalidated selections are
                // replanned, rather than treated as an explicit-input promise.
                let groups = groups.expect("invalidated rewrite selected nontrivial groups");
                let mut sources = Vec::new();
                let mut outputs = Vec::new();
                for group in groups.values().filter(|group| group.len() > 1) {
                    let output = merge_manifest_group(
                        view.artifacts(),
                        metadata,
                        parent,
                        group,
                        snapshot_id,
                    )
                    .await?;
                    view.artifacts().check_active()?;
                    sources.extend(group.iter().cloned());
                    outputs.push(output);
                }
                let cache = ManifestRewriteReuse {
                    sources,
                    outputs,
                    encoding_basis,
                };
                view.artifacts()
                    .remember_reusable_rewrite(view.stage_key(), cache.clone())?;
                cache
            }
        };
        let mut manifests = latest
            .iter()
            .filter(|manifest| {
                !reusable
                    .sources
                    .iter()
                    .any(|source| source.manifest_path == manifest.manifest_path)
            })
            .cloned()
            .collect::<Vec<_>>();
        manifests.extend(reusable.outputs.iter().cloned());
        let properties = finalize_snapshot_summary(
            HashMap::from([
                (
                    "replaced-manifests-count".to_owned(),
                    reusable.sources.len().to_string(),
                ),
                (
                    "added-manifests-count".to_owned(),
                    reusable.outputs.len().to_string(),
                ),
            ]),
            Some(parent.summary()),
        )?;
        let mut change = crate::commit::overwrite::prepare_snapshot_change(
            view,
            intent,
            snapshot_id,
            Operation::Replace,
            manifests,
            properties,
            false,
        )
        .await?;
        if !carry.properties.is_empty() {
            change.updates.push(TableUpdate::SetProperties {
                updates: carry.properties,
            });
        }
        crate::document_storage::keyed::write::trace_updates(
            metadata.properties(),
            &change.updates,
            carry
                .summary
                .iter()
                .map(|(key, value)| key.len() + value.len())
                .sum(),
        );
        Ok(change)
    }
}

fn rewrite_error(message: &str) -> crate::iceberg::Error {
    crate::iceberg::Error::new(crate::iceberg::ErrorKind::DataInvalid, message)
}

#[cfg(test)]
#[path = "rewrite_manifests/tests/keyed_record.rs"]
mod keyed_record_tests;

#[cfg(test)]
#[path = "rewrite_manifests/tests/row_id.rs"]
mod row_id_rewrite_tests;
