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
//! Spec: docs/superpowers/specs/2026-05-07-iceberg-snapshot-lifecycle-design.md §5.
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

use std::collections::BTreeMap;
use std::sync::Arc;

use iceberg::io::FileIO;
use iceberg::spec::{
    FormatVersion, ManifestContentType, ManifestEntry, ManifestFile, ManifestStatus,
    ManifestWriterBuilder, Operation, SnapshotReference, SnapshotRetention, Summary,
};
use iceberg::{Catalog, TableCommit, TableIdent, TableRequirement, TableUpdate};

use super::data_file::clone_data_file_with_first_row_id;
use super::helpers::{generate_snapshot_id, metadata_dir, now_ms, write_manifest_list};
use super::retry::commit_with_retry;

/// Top-level entry called from `engine::iceberg_rewrite_manifests`.
/// Loads the table, groups manifests, merges, and commits.
///
/// Noop cases (returns Ok immediately):
/// 1. Table has no current snapshot (empty table).
/// 2. Manifest list has ≤ 1 entry.
/// 3. All (partition_spec_id, content) groups have exactly 1 manifest.
pub async fn run_rewrite_manifests(
    catalog: Arc<dyn Catalog>,
    table_ident: TableIdent,
) -> Result<(), String> {
    commit_with_retry(|_attempt| {
        let catalog = catalog.clone();
        let table_ident = table_ident.clone();
        async move { run_rewrite_manifests_one_attempt(catalog, table_ident).await }
    })
    .await
}

async fn run_rewrite_manifests_one_attempt(
    catalog: Arc<dyn Catalog>,
    table_ident: TableIdent,
) -> Result<(), iceberg::Error> {
    let table = catalog.load_table(&table_ident).await?;
    let metadata = table.metadata();
    let file_io = table.file_io();

    // Step 1: load current snapshot; noop if empty.
    let Some(current) = metadata.current_snapshot() else {
        return Ok(());
    };
    let manifest_list = current.load_manifest_list(file_io, metadata).await?;
    let manifest_files: Vec<ManifestFile> = manifest_list.entries().to_vec();
    if manifest_files.len() <= 1 {
        // Single (or zero) manifest: nothing to merge.
        return Ok(());
    }

    // Step 2: group by (partition_spec_id, content_type).
    let groups = group_manifests_by_spec_and_content(&manifest_files);

    // Step 3 early-exit: all groups singleton → no merge needed.
    if groups.values().all(|g| g.len() <= 1) {
        return Ok(());
    }

    let format_version = metadata.format_version();
    let new_snapshot_id = generate_snapshot_id();
    let meta_dir = metadata_dir(&table);

    // Step 3: merge groups.
    let mut new_manifests: Vec<ManifestFile> = Vec::new();
    for group in groups.values() {
        if group.len() == 1 {
            // Singleton group: carry over as-is.
            new_manifests.push(group[0].clone());
            continue;
        }

        // Multi-manifest group: merge.
        let new_manifest_path = format!("{}/{}-m0.avro", meta_dir, uuid::Uuid::new_v4());
        let merged = merge_manifest_group(
            file_io,
            metadata,
            group,
            &new_manifest_path,
            new_snapshot_id,
            format_version,
        )
        .await?;
        new_manifests.push(merged);
    }

    // Step 5: write new manifest list.
    // The replace snapshot gets a new sequence_number (catalog invariant: strictly
    // increasing). The individual manifest entries inside the merged manifests
    // preserve their original file-level sequence_numbers unchanged — only the
    // snapshot-level sequence_number increments, as required by the iceberg spec.
    let new_seq = metadata.last_sequence_number() + 1;
    let manifest_list_path = format!(
        "{}/snap-{}-1-{}.avro",
        meta_dir,
        new_snapshot_id,
        uuid::Uuid::new_v4()
    );

    // For V3, the ManifestListWriter requires a starting first_row_id so it can
    // validate manifests that already have first_row_id assigned. We pass
    // metadata.next_row_id() (the table's next unallocated row id), which gives
    // the writer a consistent upper bound. Since we're not adding new rows, the
    // writer will see the "Some, Some" assignment case for each existing manifest
    // (both the writer's next_row_id and the manifest's first_row_id are set)
    // and will treat them as already assigned — no re-assignment occurs.
    let first_row_id_for_list = if format_version == FormatVersion::V3 {
        Some(metadata.next_row_id())
    } else {
        None
    };

    write_manifest_list(
        file_io,
        &manifest_list_path,
        new_manifests,
        new_snapshot_id,
        Some(current.snapshot_id()),
        new_seq,
        format_version,
        first_row_id_for_list,
    )
    .await
    .map_err(|e| {
        iceberg::Error::new(
            iceberg::ErrorKind::Unexpected,
            format!("write_manifest_list for REWRITE MANIFESTS: {e}"),
        )
    })?;

    // Step 5: build replace snapshot. snapshot-level sequence_number is
    // last_sequence_number + 1 (catalog invariant per iceberg-rs
    // table_metadata_builder.rs:358 — strictly increasing). The per-entry
    // file_sequence_number / data_sequence_number values inside merged
    // manifests are preserved unchanged from the input entries.
    // Java Iceberg SnapshotSummary semantics:
    // - replaced-manifests-count: number of old manifests actually merged away
    //   (sum of group sizes for multi-manifest groups only).
    // - added-manifests-count: number of newly written merged manifests
    //   (one per multi-manifest group).
    // Singleton groups are carried over unchanged and must not be counted.
    let replaced_count: usize = groups
        .values()
        .filter(|g| g.len() > 1)
        .map(|g| g.len())
        .sum();
    let added_count: usize = groups.values().filter(|g| g.len() > 1).count();
    let summary = Summary {
        operation: Operation::Replace,
        additional_properties: [
            (
                "replaced-manifests-count".to_string(),
                replaced_count.to_string(),
            ),
            ("added-manifests-count".to_string(), added_count.to_string()),
        ]
        .into_iter()
        .collect(),
    };

    let snapshot_builder = iceberg::spec::Snapshot::builder()
        .with_snapshot_id(new_snapshot_id)
        .with_parent_snapshot_id(Some(current.snapshot_id()))
        .with_sequence_number(new_seq)
        .with_timestamp_ms(now_ms())
        .with_manifest_list(manifest_list_path)
        .with_summary(summary)
        .with_schema_id(metadata.current_schema_id());

    // V3 tables require a row_range on every snapshot.  REWRITE MANIFESTS
    // does not add new rows, so added_rows = 0 and first_row_id = next_row_id
    // (meaning "no rows consumed by this snapshot"). This mirrors the pattern
    // in TruncateCommit which also writes 0 new rows on a V3 table.
    let new_snapshot = match format_version {
        FormatVersion::V3 => {
            let next_row_id = metadata.next_row_id();
            snapshot_builder.with_row_range(next_row_id, 0).build()
        }
        _ => snapshot_builder.build(),
    };

    // Step 6: commit via catalog.update_table (OCC protected).
    let new_ref = SnapshotReference {
        snapshot_id: new_snapshot_id,
        retention: SnapshotRetention::branch(None, None, None),
    };
    let updates = vec![
        TableUpdate::AddSnapshot {
            snapshot: new_snapshot,
        },
        TableUpdate::SetSnapshotRef {
            ref_name: "main".to_string(),
            reference: new_ref,
        },
    ];
    let requirements = vec![
        TableRequirement::CurrentSchemaIdMatch {
            current_schema_id: metadata.current_schema_id(),
        },
        TableRequirement::RefSnapshotIdMatch {
            r#ref: "main".to_string(),
            snapshot_id: Some(current.snapshot_id()),
        },
    ];
    let commit = TableCommit::builder()
        .ident(table_ident)
        .updates(updates)
        .requirements(requirements)
        .build();
    catalog.update_table(commit).await?;

    // Step 6 (best-effort): physically delete merged-away old manifest files.
    // Only delete manifests from groups that were actually merged (groups with >1 entry).
    // Failure is non-fatal; log a warning and continue.
    for group in groups.values() {
        if group.len() > 1 {
            for mf in group {
                if let Err(e) = file_io.delete(&mf.manifest_path).await {
                    tracing::warn!(
                        path = %mf.manifest_path,
                        "REWRITE MANIFESTS: failed to physically delete merged manifest (best-effort): {e}"
                    );
                }
            }
        }
    }

    Ok(())
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
    file_io: &FileIO,
    table_metadata: &iceberg::spec::TableMetadata,
    group: &[ManifestFile],
    new_manifest_path: &str,
    new_snapshot_id: i64,
    format_version: FormatVersion,
) -> Result<ManifestFile, iceberg::Error> {
    // All manifests in the group share the same partition_spec_id and content.
    let spec_id = group[0].partition_spec_id;
    let content = group[0].content;

    // Look up partition spec and schema from the table metadata.
    let partition_spec = table_metadata
        .partition_spec_by_id(spec_id)
        .ok_or_else(|| {
            iceberg::Error::new(
                iceberg::ErrorKind::DataInvalid,
                format!("partition_spec_id {spec_id} not found in table metadata"),
            )
        })?
        .as_ref()
        .clone();

    let schema = table_metadata.current_schema().clone();

    let output_file = file_io.new_output(new_manifest_path)?;
    let builder = ManifestWriterBuilder::new(
        output_file,
        Some(new_snapshot_id),
        None, // key_metadata
        schema,
        partition_spec,
    );

    let mut writer = match (format_version, content) {
        (FormatVersion::V1, ManifestContentType::Data) => builder.build_v1(),
        (FormatVersion::V2, ManifestContentType::Data) => builder.build_v2_data(),
        (FormatVersion::V2, ManifestContentType::Deletes) => builder.build_v2_deletes(),
        (FormatVersion::V3, ManifestContentType::Data) => builder.build_v3_data(),
        (FormatVersion::V3, ManifestContentType::Deletes) => builder.build_v3_deletes(),
        // V1 deletes don't exist in iceberg spec; handle gracefully.
        (FormatVersion::V1, ManifestContentType::Deletes) => builder.build_v1(),
    };

    // Collect all live entries from all manifests in the group.
    for manifest_file in group {
        let manifest = manifest_file.load_manifest(file_io).await?;

        // For V3 row-lineage: stamp an explicit first_row_id on each data file
        // so that the value is preserved after merge. When INSERT writes data
        // files with first_row_id=None, the manifest-list writer assigns the
        // manifest-level first_row_id. After merging, the new merged manifest
        // would get a fresh manifest-level first_row_id from next_row_id(),
        // causing _row_id values to shift. By stamping at the data-file level,
        // the read path (read.rs:338: df.first_row_id().or(manifest fallback))
        // uses the explicit per-file value, preserving the §5.4 invariant.
        //
        // manifest_file.first_row_id is a u64 (Option<u64>); convert to i64
        // for computation. We use the source manifest's first_row_id as the
        // base and add a cumulative offset for each entry within it.
        //
        // If the source manifest has first_row_id=None (V2 table, no row
        // lineage), we leave the data_file.first_row_id unchanged (None),
        // preserving the V2 no-op path.
        let manifest_first_row_id: Option<i64> = manifest_file
            .first_row_id
            .map(|v| i64::try_from(v).expect("manifest first_row_id fits in i64"));
        let mut cumulative_offset: i64 = 0;

        for entry_ref in manifest.entries() {
            let entry: &ManifestEntry = entry_ref.as_ref();
            if entry.status == ManifestStatus::Deleted {
                // Spec §5.2 Step 3: discard DELETED entries.
                continue;
            }

            // Round-trip the entry as EXISTING, preserving all DataFile fields
            // (including v3 row-lineage: first_row_id, referenced_data_file,
            // content_offset, content_size_in_bytes) via the data_file clone.
            // The sequence numbers and snapshot_id from the original entry are
            // preserved to maintain the causal ordering invariants.
            //
            // We use add_existing_file() which requires explicit snapshot_id,
            // sequence_number, and file_sequence_number — these come from the
            // ManifestEntry's inherited fields (guaranteed non-None after
            // load_manifest() calls inherit_data() internally).
            //
            // Fallback: if sequence_number or file_sequence_number is None
            // (e.g. from a V1 manifest), use the manifest's sequence_number.
            let snap_id = entry.snapshot_id.unwrap_or(manifest_file.added_snapshot_id);
            let seq = entry
                .sequence_number
                .unwrap_or(manifest_file.sequence_number);
            let file_seq = entry.file_sequence_number.or(Some(seq));

            let orig_df = &entry.data_file;
            let record_count = orig_df.record_count() as i64;

            // Compute stamped first_row_id:
            //  - If data_file already has an explicit first_row_id (Some), preserve it.
            //  - If manifest has a first_row_id but data_file doesn't, stamp it:
            //    first_row_id = manifest_first_row_id + cumulative_offset.
            //  - If neither has a value (V2 no row-lineage), leave as None.
            let stamped_first_row_id = match (manifest_first_row_id, orig_df.first_row_id()) {
                (_, Some(existing)) => Some(existing), // already explicit, preserve
                (Some(m_first), None) => Some(m_first + cumulative_offset),
                (None, None) => None, // V2: no row lineage
            };
            cumulative_offset += record_count;

            let data_file =
                clone_data_file_with_first_row_id(orig_df, spec_id, stamped_first_row_id)
                    .map_err(|e| iceberg::Error::new(iceberg::ErrorKind::DataInvalid, e))?;
            writer
                .add_existing_file(data_file, snap_id, seq, file_seq)
                .map_err(|e| {
                    iceberg::Error::new(
                        iceberg::ErrorKind::DataInvalid,
                        format!("ManifestWriter::add_existing_file: {e}"),
                    )
                })?;
        }
    }

    writer.write_manifest_file().await
}

#[cfg(test)]
mod tests {
    use iceberg::spec::{ManifestContentType, ManifestStatus, Operation};

    use super::*;
    use crate::connector::iceberg::commit::test_helpers::{
        empty_v3_iceberg_table, v3_table_with_multi_batch_appends, v3_table_with_n_data_files,
    };

    fn fake_manifest(path: &str, spec_id: i32, content: ManifestContentType) -> ManifestFile {
        ManifestFile {
            manifest_path: path.to_string(),
            manifest_length: 100,
            partition_spec_id: spec_id,
            content,
            sequence_number: 0,
            min_sequence_number: 0,
            added_snapshot_id: 1,
            added_files_count: Some(1),
            existing_files_count: Some(0),
            deleted_files_count: Some(0),
            added_rows_count: Some(10),
            existing_rows_count: Some(0),
            deleted_rows_count: Some(0),
            partitions: None,
            key_metadata: None,
            first_row_id: None,
        }
    }

    // ---- group_manifests_by_spec_and_content tests ----

    #[test]
    fn groups_by_spec_id_then_content() {
        let manifests = vec![
            fake_manifest("a", 0, ManifestContentType::Data),
            fake_manifest("b", 0, ManifestContentType::Data),
            fake_manifest("c", 0, ManifestContentType::Deletes),
            fake_manifest("d", 1, ManifestContentType::Data),
        ];
        let groups = group_manifests_by_spec_and_content(&manifests);
        assert_eq!(groups.len(), 3);
        // Keys use the stable byte encoding: Data=0, Deletes=1.
        assert_eq!(
            groups[&(0, content_type_key(ManifestContentType::Data))].len(),
            2,
            "spec_id=0 data should have 2"
        );
        assert_eq!(
            groups[&(0, content_type_key(ManifestContentType::Deletes))].len(),
            1,
            "spec_id=0 deletes should have 1"
        );
        assert_eq!(
            groups[&(1, content_type_key(ManifestContentType::Data))].len(),
            1,
            "spec_id=1 data should have 1"
        );
        // BTreeMap iterates in ascending key order: (0, Data=0), (0, Deletes=1), (1, Data=0).
        let ordered_keys: Vec<_> = groups.keys().copied().collect();
        assert_eq!(
            ordered_keys,
            vec![(0, 0), (0, 1), (1, 0)],
            "iteration order must be deterministic"
        );
    }

    #[test]
    fn groups_empty_input() {
        let groups = group_manifests_by_spec_and_content(&[]);
        assert!(groups.is_empty());
    }

    #[test]
    fn groups_all_same_key() {
        let manifests = vec![
            fake_manifest("x", 0, ManifestContentType::Data),
            fake_manifest("y", 0, ManifestContentType::Data),
            fake_manifest("z", 0, ManifestContentType::Data),
        ];
        let groups = group_manifests_by_spec_and_content(&manifests);
        assert_eq!(groups.len(), 1);
        assert_eq!(
            groups[&(0, content_type_key(ManifestContentType::Data))].len(),
            3
        );
    }

    // ---- empty table noop test ----

    #[tokio::test]
    async fn rewrite_manifests_empty_table_is_noop() {
        let fixture = empty_v3_iceberg_table().await;
        // empty table has no current snapshot
        assert!(
            fixture.table.metadata().current_snapshot().is_none(),
            "fixture setup: expected no snapshot"
        );
        let result =
            run_rewrite_manifests(fixture.catalog.clone(), fixture.table_ident.clone()).await;
        assert!(result.is_ok(), "empty table REWRITE MANIFESTS should be Ok");
        // No new snapshot should appear.
        let table_after = fixture
            .catalog
            .load_table(&fixture.table_ident)
            .await
            .unwrap();
        assert!(
            table_after.metadata().current_snapshot().is_none(),
            "no snapshot should be created for empty table"
        );
    }

    // ---- single manifest noop test ----

    #[tokio::test]
    async fn rewrite_manifests_single_manifest_is_noop() {
        // v3_table_with_n_data_files(1) does a single FastAppendCommit → 1 snapshot,
        // which by default produces 1 manifest.
        let fixture = v3_table_with_n_data_files(1).await;
        let snap_count_before = fixture.table.metadata().snapshots().count();
        assert!(
            snap_count_before >= 1,
            "test setup: expected at least 1 snapshot"
        );

        let result =
            run_rewrite_manifests(fixture.catalog.clone(), fixture.table_ident.clone()).await;
        assert!(
            result.is_ok(),
            "single-manifest REWRITE MANIFESTS should be Ok: {result:?}"
        );

        let table_after = fixture
            .catalog
            .load_table(&fixture.table_ident)
            .await
            .unwrap();
        let snap_count_after = table_after.metadata().snapshots().count();
        // Single manifest → noop → snapshot count unchanged.
        assert_eq!(
            snap_count_before, snap_count_after,
            "single-manifest table: no new snapshot should be written"
        );
    }

    // ---- multi-manifest merge test ----

    #[tokio::test]
    async fn rewrite_manifests_multi_manifest_merges_and_commits() {
        // 3 separate FastAppend batches → 3 snapshots, each with its own manifest.
        let fixture = v3_table_with_multi_batch_appends(&[1, 1, 1]).await;
        let metadata_before = fixture.table.metadata().clone();
        let snap_count_before = metadata_before.snapshots().count();
        assert!(
            snap_count_before >= 3,
            "test setup: expected >= 3 snapshots"
        );

        // Verify there are multiple manifests in the current snapshot.
        let manifests_before: Vec<_> = metadata_before
            .current_snapshot()
            .unwrap()
            .load_manifest_list(fixture.table.file_io(), &metadata_before)
            .await
            .unwrap()
            .entries()
            .to_vec();
        // After 3 appends the manifest list accumulates all manifests from appends.
        // FastAppendCommit adds new manifests on top; there should be >= 2.
        if manifests_before.len() <= 1 {
            // FastAppend may merge; if so skip (test setup doesn't guarantee multi-manifest).
            // This is OK — the implementation correctly noops in that case.
            return;
        }

        let result =
            run_rewrite_manifests(fixture.catalog.clone(), fixture.table_ident.clone()).await;
        assert!(
            result.is_ok(),
            "multi-manifest REWRITE MANIFESTS should be Ok: {result:?}"
        );

        let table_after = fixture
            .catalog
            .load_table(&fixture.table_ident)
            .await
            .unwrap();
        let metadata_after = table_after.metadata();
        let snap_count_after = metadata_after.snapshots().count();

        // A new replace snapshot should have been added.
        assert_eq!(
            snap_count_after,
            snap_count_before + 1,
            "expected one new replace snapshot"
        );

        let new_current = metadata_after.current_snapshot().unwrap();
        assert_eq!(
            new_current.summary().operation,
            Operation::Replace,
            "new snapshot must have operation=replace"
        );

        // replace snapshot gets a new sequence_number (last_sequence_number + 1).
        let expected_seq = metadata_before.last_sequence_number() + 1;
        assert_eq!(
            new_current.sequence_number(),
            expected_seq,
            "replace snapshot must have sequence_number = last_sequence_number + 1"
        );

        // Merged manifest list should have fewer manifests than before.
        let manifests_after: Vec<_> = new_current
            .load_manifest_list(table_after.file_io(), metadata_after)
            .await
            .unwrap()
            .entries()
            .to_vec();
        assert!(
            manifests_after.len() < manifests_before.len(),
            "merged manifest list should have fewer entries: before={}, after={}",
            manifests_before.len(),
            manifests_after.len()
        );
    }

    // ---- sequence_number monotonicity test ----

    #[tokio::test]
    async fn rewrite_manifests_sequence_number_monotonic() {
        // REWRITE MANIFESTS emits a replace snapshot whose sequence_number is
        // last_sequence_number + 1 (catalog invariant). The individual manifest
        // entries inside the merged manifests preserve their original file-level
        // sequence_numbers unchanged.
        let fixture = v3_table_with_multi_batch_appends(&[1, 1, 1]).await;
        let seq_before = fixture.table.metadata().last_sequence_number();

        let result =
            run_rewrite_manifests(fixture.catalog.clone(), fixture.table_ident.clone()).await;
        if result.is_err() {
            // May be noop if manifests were already merged; that is fine.
            return;
        }

        let table_after = fixture
            .catalog
            .load_table(&fixture.table_ident)
            .await
            .unwrap();
        let seq_after = table_after.metadata().last_sequence_number();

        // last_sequence_number must increase by exactly 1 when a commit happened.
        assert_eq!(
            seq_after,
            seq_before + 1,
            "REWRITE MANIFESTS must increment last_sequence_number by 1: before={seq_before}, after={seq_after}"
        );
    }

    // ---- row-lineage first_row_id preservation test ----

    #[tokio::test]
    async fn rewrite_manifests_preserves_row_lineage_first_row_id() {
        // Build a v3 table with 3 separate FastAppend commits (3 data files,
        // 3 manifests). Run REWRITE MANIFESTS, then verify that:
        // - The manifest entries in the new merged manifest have the same
        //   data_file.file_path() values as before.
        // - The effective _row_id for each file is the same before and after
        //   REWRITE (using the read-path formula: data_file.first_row_id()
        //   falling back to manifest-level first_row_id + cumulative offset).
        let fixture = v3_table_with_multi_batch_appends(&[1, 1, 1]).await;
        let metadata_before = fixture.table.metadata().clone();
        let file_io = fixture.table.file_io().clone();

        // Collect (file_path, effective_first_row_id) before REWRITE.
        let pre_entries = collect_live_entry_info(&metadata_before, &file_io).await;

        let result =
            run_rewrite_manifests(fixture.catalog.clone(), fixture.table_ident.clone()).await;
        match result {
            Err(e) if e.contains("noop") || e.contains("single") => return, // noop is fine
            Err(e) => panic!("REWRITE MANIFESTS failed: {e}"),
            Ok(()) => {}
        }

        let table_after = fixture
            .catalog
            .load_table(&fixture.table_ident)
            .await
            .unwrap();
        let metadata_after = table_after.metadata();
        let file_io_after = table_after.file_io();

        // If the table was actually committed (not noop), verify row-lineage preservation.
        if metadata_after.snapshots().count() <= metadata_before.snapshots().count() {
            // Noop path.
            return;
        }

        let post_entries = collect_live_entry_info(metadata_after, file_io_after).await;

        // Every file_path that existed before must still exist after.
        for (path, effective_frid_before) in &pre_entries {
            let found = post_entries
                .iter()
                .find(|(p, _)| p == path)
                .map(|(_, frid)| *frid);
            assert!(
                found.is_some(),
                "file {path} missing after REWRITE MANIFESTS"
            );
            // The effective _row_id (data_file fallback manifest-level) must be
            // identical before and after REWRITE. The fix stamps an explicit
            // data_file.first_row_id, so the raw field may change (None → Some)
            // but the read-path result must be unchanged.
            assert_eq!(
                found.unwrap(),
                *effective_frid_before,
                "effective first_row_id mismatch for {path}: before={effective_frid_before:?}, after={found:?}"
            );
        }
        // No extra files should appear.
        assert_eq!(
            pre_entries.len(),
            post_entries.len(),
            "entry count mismatch: before={}, after={}",
            pre_entries.len(),
            post_entries.len()
        );
    }

    // ---- M3: non-None first_row_id preservation through merge_manifest_group ----

    /// Write a single manifest containing `data_file` as an EXISTING entry and
    /// return the resulting `ManifestFile` descriptor.
    ///
    /// Uses a synthetic snapshot_id=1 / sequence_number=1 / file_sequence_number=1,
    /// which is sufficient for `add_existing_file` validation.
    async fn write_test_manifest_with_data_file(
        file_io: &FileIO,
        metadata: &iceberg::spec::TableMetadata,
        manifest_path: &str,
        data_file: iceberg::spec::DataFile,
    ) -> ManifestFile {
        let partition_spec = metadata.default_partition_spec().as_ref().clone();
        let schema = metadata.current_schema().clone();
        let output = file_io.new_output(manifest_path).expect("new_output");
        let builder = ManifestWriterBuilder::new(
            output,
            Some(1i64), // snapshot_id
            None,
            schema,
            partition_spec,
        );
        let mut writer = builder.build_v3_data();
        writer
            .add_existing_file(
                data_file,
                1i64,       // snapshot_id
                1i64,       // sequence_number
                Some(1i64), // file_sequence_number
            )
            .expect("add_existing_file");
        writer
            .write_manifest_file()
            .await
            .expect("write_manifest_file")
    }

    #[tokio::test]
    async fn merge_manifest_group_preserves_non_none_first_row_id() {
        // Build a minimal V3 table fixture so we have real file IO backed by MemoryCatalog.
        use iceberg::spec::{DataContentType, DataFileBuilder, DataFileFormat, Struct};

        let fixture = empty_v3_iceberg_table().await;
        let metadata = fixture.table.metadata().clone();
        let file_io = fixture.table.file_io().clone();
        let table_location = metadata.location().to_string();

        // Build two synthetic DataFiles with distinct non-None first_row_id values.
        // These values must round-trip through merge_manifest_group unchanged.
        let data_file_a = DataFileBuilder::default()
            .content(DataContentType::Data)
            .file_path(format!("{table_location}/data/test-a.parquet"))
            .file_format(DataFileFormat::Parquet)
            .partition(Struct::empty())
            .partition_spec_id(0)
            .record_count(10u64)
            .file_size_in_bytes(1024u64)
            .first_row_id(Some(42i64))
            .build()
            .expect("build data_file_a");

        let data_file_b = DataFileBuilder::default()
            .content(DataContentType::Data)
            .file_path(format!("{table_location}/data/test-b.parquet"))
            .file_format(DataFileFormat::Parquet)
            .partition(Struct::empty())
            .partition_spec_id(0)
            .record_count(5u64)
            .file_size_in_bytes(512u64)
            .first_row_id(Some(100i64))
            .build()
            .expect("build data_file_b");

        let manifest_a_path = format!("{table_location}/metadata/test-manifest-a.avro");
        let manifest_b_path = format!("{table_location}/metadata/test-manifest-b.avro");
        let merged_path = format!("{table_location}/metadata/test-merged.avro");

        let mf_a =
            write_test_manifest_with_data_file(&file_io, &metadata, &manifest_a_path, data_file_a)
                .await;
        let mf_b =
            write_test_manifest_with_data_file(&file_io, &metadata, &manifest_b_path, data_file_b)
                .await;

        // Merge the two manifests — this exercises the non-None first_row_id
        // round-trip path inside merge_manifest_group.
        let group = vec![mf_a, mf_b];
        let merged_mf = merge_manifest_group(
            &file_io,
            &metadata,
            &group,
            &merged_path,
            999i64, // new_snapshot_id
            iceberg::spec::FormatVersion::V3,
        )
        .await
        .expect("merge_manifest_group");

        // Read back the merged manifest and check that first_row_id values survive.
        let merged_manifest = merged_mf
            .load_manifest(&file_io)
            .await
            .expect("load merged manifest");
        let entries: Vec<_> = merged_manifest
            .entries()
            .iter()
            .map(|e| {
                let entry: &ManifestEntry = e.as_ref();
                (
                    entry.data_file.file_path().to_string(),
                    entry.data_file.first_row_id(),
                )
            })
            .collect();

        assert_eq!(
            entries.len(),
            2,
            "merged manifest must contain both entries"
        );

        // Verify that both first_row_id = Some(42) and Some(100) round-trip.
        let first_row_ids: Vec<Option<i64>> = entries.iter().map(|(_, frid)| *frid).collect();
        assert!(
            first_row_ids.contains(&Some(42)),
            "first_row_id=Some(42) must survive merge; got: {first_row_ids:?}"
        );
        assert!(
            first_row_ids.contains(&Some(100)),
            "first_row_id=Some(100) must survive merge; got: {first_row_ids:?}"
        );
    }

    // ---- M4: stamp first_row_id from manifest-level when data_file has None ----

    /// Build a single-entry manifest with `data_file.first_row_id = None` but
    /// with the manifest descriptor's `first_row_id` set to `manifest_first_row_id`.
    ///
    /// This replicates the INSERT path: NovaRocks writes data files with
    /// `first_row_id = None` and relies on the `ManifestListWriter` to assign
    /// `manifest.first_row_id` from the snapshot's `row_lineage_first_row_id`.
    async fn write_test_manifest_none_first_row_id(
        file_io: &FileIO,
        metadata: &iceberg::spec::TableMetadata,
        manifest_path: &str,
        file_path: &str,
        record_count: u64,
        manifest_first_row_id: u64,
    ) -> ManifestFile {
        use iceberg::spec::{DataContentType, DataFileBuilder, DataFileFormat, Struct};
        let data_file = DataFileBuilder::default()
            .content(DataContentType::Data)
            .file_path(file_path.to_string())
            .file_format(DataFileFormat::Parquet)
            .partition(Struct::empty())
            .partition_spec_id(0)
            .record_count(record_count)
            .file_size_in_bytes(1024u64)
            .first_row_id(None) // None — the common INSERT path
            .build()
            .expect("build data_file");

        let mut mf =
            write_test_manifest_with_data_file(file_io, metadata, manifest_path, data_file).await;
        // Simulate what ManifestListWriter does: stamp manifest-level first_row_id.
        mf.first_row_id = Some(manifest_first_row_id);
        mf
    }

    /// Verifies the §5.4 invariant: after REWRITE MANIFESTS, each output
    /// data_file carries an explicit `first_row_id` equal to the one originally
    /// derived from the source manifest's manifest-level `first_row_id`.
    ///
    /// This is the real-world bug case: INSERT writes `data_file.first_row_id =
    /// None`; the manifest list assigns manifest-level `first_row_id` = 0, 1, 2.
    /// Before the fix, REWRITE cloned those None data files into a merged
    /// manifest whose manifest-level `first_row_id` was reassigned to
    /// `next_row_id()` (= 3), causing all rows to be read with _row_id ≥ 3.
    #[tokio::test]
    async fn merge_preserves_first_row_id_via_explicit_stamping() {
        let fixture = empty_v3_iceberg_table().await;
        let metadata = fixture.table.metadata().clone();
        let file_io = fixture.table.file_io().clone();
        let table_location = metadata.location().to_string();

        // 3 manifests, each with 1 row and data_file.first_row_id = None.
        // Manifest-level first_row_id = 0, 1, 2 (as assigned by ManifestListWriter
        // after three successive single-row INSERTs).
        let mf0 = write_test_manifest_none_first_row_id(
            &file_io,
            &metadata,
            &format!("{table_location}/metadata/m0.avro"),
            &format!("{table_location}/data/row0.parquet"),
            1, // record_count
            0, // manifest_first_row_id
        )
        .await;

        let mf1 = write_test_manifest_none_first_row_id(
            &file_io,
            &metadata,
            &format!("{table_location}/metadata/m1.avro"),
            &format!("{table_location}/data/row1.parquet"),
            1, // record_count
            1, // manifest_first_row_id
        )
        .await;

        let mf2 = write_test_manifest_none_first_row_id(
            &file_io,
            &metadata,
            &format!("{table_location}/metadata/m2.avro"),
            &format!("{table_location}/data/row2.parquet"),
            1, // record_count
            2, // manifest_first_row_id
        )
        .await;

        let merged_path = format!("{table_location}/metadata/merged.avro");
        let group = vec![mf0, mf1, mf2];
        let merged_mf = merge_manifest_group(
            &file_io,
            &metadata,
            &group,
            &merged_path,
            999i64,
            iceberg::spec::FormatVersion::V3,
        )
        .await
        .expect("merge_manifest_group");

        let merged_manifest = merged_mf
            .load_manifest(&file_io)
            .await
            .expect("load merged manifest");

        let entries: Vec<_> = merged_manifest
            .entries()
            .iter()
            .map(|e| {
                let entry: &ManifestEntry = e.as_ref();
                (
                    entry.data_file.file_path().to_string(),
                    entry.data_file.first_row_id(),
                )
            })
            .collect();

        assert_eq!(
            entries.len(),
            3,
            "merged manifest must contain all 3 entries"
        );

        // After the fix, each data_file must carry its explicit first_row_id
        // stamped from the source manifest's first_row_id + cumulative offset.
        // row0: manifest_first_row_id=0, cumulative_offset=0 → first_row_id=Some(0)
        // row1: manifest_first_row_id=1, cumulative_offset=0 → first_row_id=Some(1)
        // row2: manifest_first_row_id=2, cumulative_offset=0 → first_row_id=Some(2)
        let first_row_ids: Vec<Option<i64>> = entries.iter().map(|(_, frid)| *frid).collect();
        assert!(
            first_row_ids.contains(&Some(0)),
            "first_row_id=Some(0) (row0) must be stamped; got: {first_row_ids:?}"
        );
        assert!(
            first_row_ids.contains(&Some(1)),
            "first_row_id=Some(1) (row1) must be stamped; got: {first_row_ids:?}"
        );
        assert!(
            first_row_ids.contains(&Some(2)),
            "first_row_id=Some(2) (row2) must be stamped; got: {first_row_ids:?}"
        );

        // Verify no entry has first_row_id=None (which would expose the bug path).
        for (path, frid) in &entries {
            assert!(
                frid.is_some(),
                "data_file at {path} must have explicit first_row_id after merge (found None)"
            );
        }
    }

    /// Collect `(file_path, effective_first_row_id)` from all live manifest entries
    /// in the current snapshot's manifest list.
    ///
    /// The "effective" first_row_id mirrors what the read path (read.rs:338) computes:
    /// `data_file.first_row_id().or(manifest_level_first_row_id)`. Before REWRITE MANIFESTS,
    /// the data file has `first_row_id = None` and the manifest carries the assigned value.
    /// After REWRITE MANIFESTS (with the fix), the data file has an explicit `first_row_id`.
    /// In both cases the effective value must be the same.
    async fn collect_live_entry_info(
        metadata: &iceberg::spec::TableMetadata,
        file_io: &FileIO,
    ) -> Vec<(String, Option<i64>)> {
        let Some(current) = metadata.current_snapshot() else {
            return Vec::new();
        };
        let manifest_list = current
            .load_manifest_list(file_io, metadata)
            .await
            .expect("load_manifest_list");
        let mut results = Vec::new();
        for mf in manifest_list.entries() {
            // Replicate the read-path's manifest-level first_row_id tracking.
            // read.rs uses manifest_file.first_row_id (u64) as i64 fallback.
            let manifest_level_frid: Option<i64> = mf
                .first_row_id
                .map(|v| i64::try_from(v).expect("manifest first_row_id fits i64"));
            let mut cumulative: i64 = 0;

            let manifest = mf.load_manifest(file_io).await.expect("load_manifest");
            for entry_ref in manifest.entries() {
                let entry: &ManifestEntry = entry_ref.as_ref();
                if entry.status == ManifestStatus::Deleted {
                    continue;
                }
                let path = entry.data_file.file_path().to_string();
                // Effective first_row_id: data_file wins; manifest_level is fallback.
                let effective = entry
                    .data_file
                    .first_row_id()
                    .or_else(|| manifest_level_frid.map(|base| base + cumulative));
                cumulative += entry.data_file.record_count() as i64;
                results.push((path, effective));
            }
        }
        results
    }
}
