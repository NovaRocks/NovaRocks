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

//! Metadata-only v3 row-lineage DV commit.
//!
//! The BE `DeletionVectors` sink has already read and merged old DVs and
//! written the replacement Puffin files. This commit action only validates
//! those descriptors and registers them in Iceberg metadata.

use std::collections::{BTreeMap, HashSet};
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use iceberg::io::FileIO;
use iceberg::spec::{
    DataContentType, DataFileFormat, FormatVersion, ManifestFile, Operation, SchemaRef, Snapshot,
    SnapshotReference, SnapshotRetention, Summary,
};
use iceberg::table::Table;
use iceberg::transaction::{ActionCommit, ApplyTransactionAction, Transaction, TransactionAction};
use iceberg::{TableRequirement, TableUpdate};
use uuid::Uuid;

use super::abort::AbortLog;
use super::action::{CommitCtx, IcebergCommitAction, merge_snapshot_summary_properties};
use super::fast_append::carry_forward_puffin_stats;
use super::helpers::{
    effective_next_row_id, finalize_snapshot_summary, generate_snapshot_id, metadata_dir, now_ms,
    required_target_ref_snapshot_id, snapshot_summary, snapshot_total_records,
    target_ref_snapshot_id, write_manifest_list,
};
use super::row_delta_dv_metadata::{
    WrittenDvFile, build_snapshot_index_metadata_only, dv_summary, dv_total_records,
    group_live_files_by_partition_spec, group_written_dvs_by_partition_spec, partition_spec_by_id,
    to_iceberg_unexpected, write_added_dv_manifest, write_existing_delete_manifest,
};
use super::types::{CommitOutcome, WrittenFile};

pub struct RowDeltaDvFromFilesCommit;

#[async_trait]
impl IcebergCommitAction for RowDeltaDvFromFilesCommit {
    async fn commit(&self, ctx: CommitCtx<'_>) -> Result<CommitOutcome, String> {
        let written = ctx.collector.take_written_files()?;
        let groups = ctx.collector.take_delete_groups();
        if !groups.is_empty() {
            return Err(
                "RowDeltaDvFromFilesCommit does not accept coordinator delete groups; expected BE-written Puffin DV files"
                    .to_string(),
            );
        }

        if written.is_empty() {
            let id = target_ref_snapshot_id(ctx.table.metadata(), ctx.target_ref).unwrap_or(0);
            return Ok(CommitOutcome {
                new_snapshot_id: id,
                written_manifest_paths: vec![],
            });
        }
        let (written_dvs, written_data) = partition_written_for_dv_from_files(written)?;
        validate_unique_referenced_files(&written_dvs)?;

        let manifest_paths_out: Arc<Mutex<Vec<String>>> = Arc::new(Mutex::new(Vec::new()));
        let action = RowDeltaDvFromFilesTxnAction {
            written_dvs,
            written: written_data,
            commit_uuid: ctx.commit_uuid,
            file_io: ctx.file_io.clone(),
            schema: ctx.table.metadata().current_schema().clone(),
            schema_id: ctx.table.metadata().current_schema_id(),
            row_lineage_first_row_id: effective_next_row_id(ctx.table.metadata())?,
            abort_handle: ctx.abort_handle.clone(),
            manifest_paths_out: manifest_paths_out.clone(),
            target_ref: ctx.target_ref.to_string(),
            snapshot_properties: ctx.snapshot_properties.clone(),
        };

        let prev_snapshot_id = target_ref_snapshot_id(ctx.table.metadata(), ctx.target_ref);
        let tx = Transaction::new(ctx.table);
        let tx = action
            .apply(tx)
            .map_err(|e| format!("RowDeltaDvFromFiles apply failed: {e}"))?;
        let table_after = tx
            .commit(ctx.catalog)
            .await
            .map_err(|e| format!("RowDeltaDvFromFiles commit failed: {e}"))?;
        let new_snapshot_id = required_target_ref_snapshot_id(
            table_after.metadata(),
            ctx.target_ref,
            "RowDeltaDvFromFiles",
        )?;
        if let Some(prev) = prev_snapshot_id {
            carry_forward_puffin_stats(&table_after, ctx.catalog, new_snapshot_id, prev).await;
        }
        let written_manifest_paths = manifest_paths_out
            .lock()
            .expect("manifest_paths_out poisoned")
            .clone();
        Ok(CommitOutcome {
            new_snapshot_id,
            written_manifest_paths,
        })
    }
}

struct RowDeltaDvFromFilesTxnAction {
    written_dvs: Vec<WrittenDvFile>,
    /// Replacement data files produced by an MOR UPDATE. Empty for a plain
    /// metadata-only DELETE. Each file already carries stored row-lineage
    /// columns, so the snapshot must NOT allocate fresh row IDs for them — the
    /// added-data manifest is marked with `first_row_id` to suppress
    /// allocation.
    written: Vec<WrittenFile>,
    commit_uuid: Uuid,
    file_io: FileIO,
    schema: SchemaRef,
    schema_id: i32,
    row_lineage_first_row_id: u64,
    abort_handle: Arc<AbortLog>,
    manifest_paths_out: Arc<Mutex<Vec<String>>>,
    target_ref: String,
    snapshot_properties: BTreeMap<String, String>,
}

#[async_trait]
impl TransactionAction for RowDeltaDvFromFilesTxnAction {
    async fn commit(self: Arc<Self>, table: &Table) -> iceberg::Result<ActionCommit> {
        let m = table.metadata();
        let format_version = m.format_version();
        if format_version != FormatVersion::V3 {
            return Err(iceberg::Error::new(
                iceberg::ErrorKind::DataInvalid,
                "RowDeltaDvFromFilesCommit requires an Iceberg v3 table",
            ));
        }
        let new_seq = m.last_sequence_number() + 1;
        let new_snapshot_id = generate_snapshot_id();
        let target_ref = &self.target_ref;
        let parent_snapshot_id = target_ref_snapshot_id(m, target_ref);
        let metadata_dir = metadata_dir(table);
        let touched_files = self
            .written_dvs
            .iter()
            .map(|dv| dv.referenced_data_file.clone())
            .collect::<HashSet<_>>();
        let index =
            build_snapshot_index_metadata_only(table, &self.file_io, &touched_files, target_ref)
                .await
                .map_err(to_iceberg_unexpected)?;

        for referenced in &touched_files {
            if !index.data_files.contains_key(referenced) {
                return Err(to_iceberg_unexpected(format!(
                    "row-lineage DELETE referenced data file `{referenced}` is not present in the current snapshot"
                )));
            }
        }

        let mut new_manifests = index.untouched_manifests;
        for (idx, (spec_id, files)) in
            group_live_files_by_partition_spec(index.touched_delete_existing)
                .into_iter()
                .enumerate()
        {
            let path = format!(
                "{metadata_dir}/{}-row-delta-dv-existing-{idx}.avro",
                self.commit_uuid,
            );
            self.abort_handle.record_manifest(path.clone());
            self.manifest_paths_out
                .lock()
                .expect("manifest_paths_out poisoned")
                .push(path.clone());
            let mf = write_existing_delete_manifest(
                &self.file_io,
                &path,
                &files,
                partition_spec_by_id(m, spec_id)?,
                self.schema.clone(),
                new_snapshot_id,
            )
            .await
            .map_err(to_iceberg_unexpected)?;
            new_manifests.push(mf);
        }

        for (idx, (spec_id, dvs)) in
            group_written_dvs_by_partition_spec(&self.written_dvs, &index.data_files)
                .map_err(to_iceberg_unexpected)?
                .into_iter()
                .enumerate()
        {
            let added_path = format!(
                "{metadata_dir}/{}-row-delta-dv-added-{idx}.avro",
                self.commit_uuid
            );
            self.abort_handle.record_manifest(added_path.clone());
            self.manifest_paths_out
                .lock()
                .expect("manifest_paths_out poisoned")
                .push(added_path.clone());
            let added = write_added_dv_manifest(
                &self.file_io,
                &added_path,
                &dvs,
                &index.data_files,
                partition_spec_by_id(m, spec_id)?,
                self.schema.clone(),
                new_seq,
                new_snapshot_id,
            )
            .await
            .map_err(to_iceberg_unexpected)?;
            new_manifests.push(added);
        }

        if !self.written.is_empty() {
            let data_path = format!(
                "{metadata_dir}/{}-row-delta-update-data-0.avro",
                self.commit_uuid
            );
            self.abort_handle.record_manifest(data_path.clone());
            self.manifest_paths_out
                .lock()
                .expect("manifest_paths_out poisoned")
                .push(data_path.clone());
            let data_manifest = super::overwrite::write_added_data_manifest(
                &self.file_io,
                &data_path,
                &self.written,
                m.default_partition_spec().clone(),
                self.schema.clone(),
                new_seq,
                new_snapshot_id,
                format_version,
            )
            .await
            .map_err(to_iceberg_unexpected)?;
            // The replacement data files reuse the matched rows' `_row_id`s
            // (stored in the row-lineage columns). Mark the manifest as
            // already-assigned so the v3 manifest-list writer does NOT
            // allocate fresh row IDs for them.
            new_manifests.push(mark_replacement_manifest_row_id_assigned(
                data_manifest,
                self.row_lineage_first_row_id,
            ));
        }

        let manifest_list_path = format!(
            "{metadata_dir}/snap-{}-{}.avro",
            new_snapshot_id, self.commit_uuid
        );
        self.abort_handle
            .record_manifest(manifest_list_path.clone());
        self.manifest_paths_out
            .lock()
            .expect("manifest_paths_out poisoned")
            .push(manifest_list_path.clone());
        let manifest_list_next_row_id = write_manifest_list(
            &self.file_io,
            &manifest_list_path,
            new_manifests,
            new_snapshot_id,
            parent_snapshot_id,
            new_seq,
            format_version,
            Some(self.row_lineage_first_row_id),
        )
        .await
        .map_err(to_iceberg_unexpected)?;
        if manifest_list_next_row_id != Some(self.row_lineage_first_row_id) {
            return Err(to_iceberg_unexpected(format!(
                "row-lineage DELETE must not allocate row IDs: expected next-row-id {}, got {manifest_list_next_row_id:?}",
                self.row_lineage_first_row_id
            )));
        }

        let added_position_deletes = self.written_dvs.iter().try_fold(0u64, |sum, dv| {
            sum.checked_add(dv.cardinality)
                .ok_or_else(|| to_iceberg_unexpected("DV cardinality overflow".to_string()))
        })?;
        let newly_deleted_records = added_position_deletes
            .checked_sub(index.replaced_delete_records)
            .ok_or_else(|| {
                to_iceberg_unexpected(format!(
                    "DV delete summary underflow: added_position_deletes={added_position_deletes}, replaced_position_deletes={}",
                    index.replaced_delete_records
                ))
            })?;
        let added_data_records = self.written.iter().try_fold(0u64, |sum, file| {
            sum.checked_add(file.record_count).ok_or_else(|| {
                to_iceberg_unexpected("DV added data record count overflow".to_string())
            })
        })?;
        let total_records = dv_total_records(
            snapshot_total_records(m, parent_snapshot_id).map_err(to_iceberg_unexpected)?,
            newly_deleted_records,
            added_data_records,
        )
        .map_err(to_iceberg_unexpected)?;

        let mut dv_props = dv_summary(
            &self.written_dvs,
            &self.written,
            total_records,
            newly_deleted_records,
            index.replaced_delete_files,
            index.replaced_delete_records,
        )
        .map_err(to_iceberg_unexpected)?;
        if index.replaced_delete_files_size > 0 {
            dv_props.insert(
                "removed-files-size".to_string(),
                index.replaced_delete_files_size.to_string(),
            );
        }
        let parent_summary =
            snapshot_summary(m, parent_snapshot_id).map_err(to_iceberg_unexpected)?;
        let summary_props = merge_snapshot_summary_properties(
            finalize_snapshot_summary(dv_props, parent_summary, false),
            &self.snapshot_properties,
        )
        .map_err(to_iceberg_unexpected)?;
        let snapshot = Snapshot::builder()
            .with_snapshot_id(new_snapshot_id)
            .with_parent_snapshot_id(parent_snapshot_id)
            .with_sequence_number(new_seq)
            .with_timestamp_ms(now_ms())
            .with_manifest_list(manifest_list_path)
            .with_summary(Summary {
                operation: Operation::Delete,
                additional_properties: summary_props,
            })
            .with_schema_id(self.schema_id)
            .with_row_range(self.row_lineage_first_row_id, 0)
            .build();

        Ok(ActionCommit::new(
            vec![
                TableUpdate::AddSnapshot { snapshot },
                TableUpdate::SetSnapshotRef {
                    ref_name: target_ref.clone(),
                    reference: SnapshotReference {
                        snapshot_id: new_snapshot_id,
                        retention: SnapshotRetention::Branch {
                            min_snapshots_to_keep: None,
                            max_snapshot_age_ms: None,
                            max_ref_age_ms: None,
                        },
                    },
                },
            ],
            vec![
                TableRequirement::CurrentSchemaIdMatch {
                    current_schema_id: m.current_schema_id(),
                },
                TableRequirement::DefaultSpecIdMatch {
                    default_spec_id: m.default_partition_spec_id(),
                },
                TableRequirement::RefSnapshotIdMatch {
                    r#ref: target_ref.clone(),
                    snapshot_id: parent_snapshot_id,
                },
            ],
        ))
    }
}

fn dv_descriptor_from_written(file: &WrittenFile) -> Result<WrittenDvFile, String> {
    if file.format != DataFileFormat::Puffin {
        return Err(format!(
            "RowDeltaDvFromFilesCommit expected Puffin DV file {}, got format {:?}",
            file.path, file.format
        ));
    }
    if file.content != DataContentType::PositionDeletes {
        return Err(format!(
            "RowDeltaDvFromFilesCommit expected PositionDeletes content for {}, got {:?}",
            file.path, file.content
        ));
    }
    let referenced_data_file = file
        .referenced_data_file
        .as_ref()
        .filter(|path| !path.is_empty())
        .cloned()
        .ok_or_else(|| {
            format!(
                "RowDeltaDvFromFilesCommit Puffin DV {} missing referenced_data_file",
                file.path
            )
        })?;
    let content_offset =
        require_non_negative_i64(file.content_offset, "content_offset", &file.path)?;
    let content_size_in_bytes = require_positive_i64(
        file.content_size_in_bytes,
        "content_size_in_bytes",
        &file.path,
    )?;
    let cardinality = file.cardinality.ok_or_else(|| {
        format!(
            "RowDeltaDvFromFilesCommit Puffin DV {} missing cardinality",
            file.path
        )
    })?;
    if file.record_count != cardinality {
        return Err(format!(
            "RowDeltaDvFromFilesCommit Puffin DV {} record_count {} does not match cardinality {}",
            file.path, file.record_count, cardinality
        ));
    }
    if file.file_size_in_bytes == 0 {
        return Err(format!(
            "RowDeltaDvFromFilesCommit Puffin DV {} missing file_size_in_bytes",
            file.path
        ));
    }
    validate_content_range(
        content_offset,
        content_size_in_bytes,
        file.file_size_in_bytes,
        &file.path,
    )?;

    Ok(WrittenDvFile {
        path: file.path.clone(),
        referenced_data_file,
        cardinality,
        content_offset,
        content_size_in_bytes,
        file_size_in_bytes: file.file_size_in_bytes,
    })
}

fn require_non_negative_i64(value: Option<i64>, field: &str, path: &str) -> Result<i64, String> {
    match value {
        Some(value) if value >= 0 => Ok(value),
        Some(value) => Err(format!(
            "RowDeltaDvFromFilesCommit Puffin DV {path} {field} must be non-negative, got {value}"
        )),
        None => Err(format!(
            "RowDeltaDvFromFilesCommit Puffin DV {path} missing {field}"
        )),
    }
}

fn require_positive_i64(value: Option<i64>, field: &str, path: &str) -> Result<i64, String> {
    match value {
        Some(value) if value > 0 => Ok(value),
        Some(value) => Err(format!(
            "RowDeltaDvFromFilesCommit Puffin DV {path} {field} must be positive, got {value}"
        )),
        None => Err(format!(
            "RowDeltaDvFromFilesCommit Puffin DV {path} missing {field}"
        )),
    }
}

fn validate_content_range(
    content_offset: i64,
    content_size_in_bytes: i64,
    file_size_in_bytes: u64,
    path: &str,
) -> Result<(), String> {
    let content_end = content_offset
        .checked_add(content_size_in_bytes)
        .ok_or_else(|| {
            format!(
                "RowDeltaDvFromFilesCommit Puffin DV {path} content range overflows i64: offset={content_offset}, size={content_size_in_bytes}"
            )
        })?;
    let content_end = u64::try_from(content_end).map_err(|_| {
        format!(
            "RowDeltaDvFromFilesCommit Puffin DV {path} content range must be non-negative, got end={content_end}"
        )
    })?;
    if content_end > file_size_in_bytes {
        return Err(format!(
            "RowDeltaDvFromFilesCommit Puffin DV {path} content range offset={content_offset}, size={content_size_in_bytes} exceeds file_size_in_bytes={file_size_in_bytes}"
        ));
    }
    Ok(())
}

fn validate_unique_referenced_files(dvs: &[WrittenDvFile]) -> Result<(), String> {
    let mut seen = HashSet::new();
    for dv in dvs {
        if !seen.insert(dv.referenced_data_file.as_str()) {
            return Err(format!(
                "RowDeltaDvFromFilesCommit received multiple Puffin DV files for data file `{}`; per-file shuffle must produce exactly one merged DV",
                dv.referenced_data_file
            ));
        }
    }
    Ok(())
}

/// Partition BE-written files into Puffin DV descriptors and replacement data
/// files. A MOR UPDATE commits both in one snapshot: the updated rows arrive as
/// `(Data, *)` files and the old-version deletes arrive as `(PositionDeletes,
/// Puffin)` DV files. Any other combination — notably a Parquet
/// position-delete — is a contract violation and is rejected.
fn partition_written_for_dv_from_files(
    written: Vec<WrittenFile>,
) -> Result<(Vec<WrittenDvFile>, Vec<WrittenFile>), String> {
    let mut dvs = Vec::new();
    let mut data = Vec::new();
    for file in written {
        match (file.content, file.format) {
            (DataContentType::PositionDeletes, DataFileFormat::Puffin) => {
                dvs.push(dv_descriptor_from_written(&file)?);
            }
            (DataContentType::Data, _) => {
                data.push(file);
            }
            (content, format) => {
                return Err(format!(
                    "RowDeltaDvFromFilesCommit received unsupported written file {} with content {:?} and format {:?}; expected Puffin PositionDeletes or Data",
                    file.path, content, format
                ));
            }
        }
    }
    Ok((dvs, data))
}

fn mark_replacement_manifest_row_id_assigned(
    mut manifest: ManifestFile,
    row_lineage_first_row_id: u64,
) -> ManifestFile {
    // MOR UPDATE replacement files carry stored row-lineage columns. The
    // manifest first-row-id is assigned only to prevent the v3 manifest-list
    // writer from allocating new row IDs for those replacement rows.
    manifest.first_row_id = Some(row_lineage_first_row_id);
    manifest
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::collections::HashMap;

    use iceberg::spec::{DataContentType, DataFileBuilder, DataFileFormat, Struct};
    use uuid::Uuid;

    use super::super::action::CommitCtx;
    use super::super::collector::IcebergCommitCollector;
    use super::super::position_delete_writer::PositionDeleteGroup;
    use super::super::row_delta_dv_metadata::{LiveFile, dv_data_file};
    use super::super::test_helpers::empty_v3_iceberg_table;
    use super::super::types::CommitOpKind;
    use super::*;
    use crate::common::types::UniqueId;

    #[test]
    fn descriptor_conversion_builds_puffin_dv_data_file() {
        let written = test_written_puffin_dv_file();
        let dv = dv_descriptor_from_written(&written).expect("descriptor");
        let referenced = test_live_file(7);

        let data_file = dv_data_file(&dv, &referenced).expect("data file");

        assert_eq!(data_file.file_path(), "s3://b/data/dv-00000000.puffin");
        assert_eq!(data_file.content_type(), DataContentType::PositionDeletes);
        assert_eq!(data_file.file_format(), DataFileFormat::Puffin);
        assert_eq!(
            data_file.referenced_data_file().as_deref(),
            Some("s3://b/data/f.parquet")
        );
        assert_eq!(data_file.content_offset(), Some(4));
        assert_eq!(data_file.content_size_in_bytes(), Some(12));
        assert_eq!(data_file.record_count(), 3);
        assert_eq!(data_file.file_size_in_bytes(), 40);
    }

    #[test]
    fn descriptor_conversion_rejects_malformed_written_files() {
        let mut file = test_written_puffin_dv_file();
        file.format = DataFileFormat::Parquet;
        assert!(
            dv_descriptor_from_written(&file)
                .unwrap_err()
                .contains("Puffin")
        );

        let mut file = test_written_puffin_dv_file();
        file.referenced_data_file = None;
        assert!(
            dv_descriptor_from_written(&file)
                .unwrap_err()
                .contains("referenced_data_file")
        );

        let mut file = test_written_puffin_dv_file();
        file.content_offset = Some(-1);
        assert!(
            dv_descriptor_from_written(&file)
                .unwrap_err()
                .contains("content_offset")
        );

        let mut file = test_written_puffin_dv_file();
        file.cardinality = None;
        assert!(
            dv_descriptor_from_written(&file)
                .unwrap_err()
                .contains("cardinality")
        );

        let mut file = test_written_puffin_dv_file();
        file.record_count = 2;
        assert!(
            dv_descriptor_from_written(&file)
                .unwrap_err()
                .contains("record_count")
        );
    }

    #[test]
    fn descriptor_conversion_rejects_zero_and_out_of_bounds_content_range() {
        let mut file = test_written_puffin_dv_file();
        file.content_size_in_bytes = Some(0);
        assert!(
            dv_descriptor_from_written(&file)
                .unwrap_err()
                .contains("content_size_in_bytes")
        );

        let mut file = test_written_puffin_dv_file();
        file.content_offset = Some(35);
        file.content_size_in_bytes = Some(6);
        assert!(
            dv_descriptor_from_written(&file)
                .unwrap_err()
                .contains("file_size_in_bytes")
        );

        let mut file = test_written_puffin_dv_file();
        file.content_offset = Some(i64::MAX);
        file.content_size_in_bytes = Some(1);
        file.file_size_in_bytes = u64::MAX;
        assert!(
            dv_descriptor_from_written(&file)
                .unwrap_err()
                .contains("overflows")
        );
    }

    #[test]
    fn duplicate_referenced_data_file_is_rejected() {
        let left = dv_descriptor_from_written(&test_written_puffin_dv_file()).unwrap();
        let mut right_file = test_written_puffin_dv_file();
        right_file.path = "s3://b/data/dv-00000001.puffin".to_string();
        let right = dv_descriptor_from_written(&right_file).unwrap();

        let err = validate_unique_referenced_files(&[left, right]).unwrap_err();

        assert!(err.contains("multiple Puffin DV files"));
        assert!(err.contains("s3://b/data/f.parquet"));
    }

    #[test]
    fn source_does_not_call_puffin_read_or_write_helpers() {
        let source = include_str!("row_delta_dv_from_files.rs");
        assert!(!source.contains(concat!("read_", "deletion_vector_puffin")));
        assert!(!source.contains(concat!("write_", "single_deletion_vector_puffin")));
    }

    #[test]
    fn commit_partitions_data_and_dv_written_files() {
        let dv = test_written_puffin_dv_file();
        let data = test_written_data_file("s3://b/data/new.parquet", 5);

        let (dvs, data_files) =
            partition_written_for_dv_from_files(vec![dv, data]).expect("partition");
        assert_eq!(dvs.len(), 1);
        assert_eq!(data_files.len(), 1);
        assert_eq!(dvs[0].path, "s3://b/data/dv-00000000.puffin");
        assert_eq!(data_files[0].path, "s3://b/data/new.parquet");

        let mut bad_parquet_position_delete = test_written_data_file("s3://b/data/pd.parquet", 1);
        bad_parquet_position_delete.content = DataContentType::PositionDeletes;
        bad_parquet_position_delete.format = DataFileFormat::Parquet;
        assert!(
            partition_written_for_dv_from_files(vec![bad_parquet_position_delete]).is_err(),
            "Parquet PositionDeletes must be rejected"
        );
    }

    #[tokio::test]
    async fn commit_drains_writer_files_before_rejecting_coordinator_groups() {
        let fixture = empty_v3_iceberg_table().await;
        let metadata = fixture.table.metadata().clone();
        let finst_id = UniqueId { hi: 7007, lo: 1 };
        crate::runtime::sink_commit::unregister(finst_id);
        crate::runtime::sink_commit::register(finst_id);
        crate::runtime::sink_commit::add(
            finst_id,
            crate::types::TSinkCommitInfo {
                iceberg_data_file: Some(test_thrift_puffin_dv_file()),
                hive_file_info: None,
                is_overwrite: None,
                staging_dir: None,
                is_rewrite: None,
            },
        );

        let collector = IcebergCommitCollector::new(
            CommitOpKind::RowDeltaDvFromFiles,
            fixture.table_ident.clone(),
            metadata.current_snapshot().map(|s| s.snapshot_id()),
            metadata.last_sequence_number(),
            metadata.current_schema().clone(),
            metadata.default_partition_spec().clone(),
            format!("{}/staging", metadata.location()),
            finst_id,
        )
        .with_table_metadata(metadata);
        collector.inject_delete_group(PositionDeleteGroup {
            referenced_data_file: "s3://b/data/f.parquet".to_string(),
            partition_spec_id: 0,
            partition_values: Struct::empty(),
            positions: vec![1],
        });

        let file_io = fixture.table.file_io().clone();
        let snapshot_properties = BTreeMap::new();
        let ctx = CommitCtx {
            collector: &collector,
            table: &fixture.table,
            catalog: fixture.catalog.as_ref(),
            file_io: &file_io,
            commit_uuid: Uuid::new_v4(),
            abort_handle: collector.abort_log.clone(),
            target_ref: "main",
            snapshot_properties: &snapshot_properties,
        };

        let err = RowDeltaDvFromFilesCommit.commit(ctx).await.unwrap_err();

        assert!(err.contains("does not accept coordinator delete groups"));
        assert_eq!(
            collector.abort_log.drain_data_files(),
            vec!["s3://b/data/dv-00000000.puffin".to_string()]
        );
        crate::runtime::sink_commit::unregister(finst_id);
    }

    fn test_live_file(partition_spec_id: i32) -> LiveFile {
        let data_file = DataFileBuilder::default()
            .content(DataContentType::Data)
            .file_path(format!("s3://b/data/f-{partition_spec_id}.parquet"))
            .file_format(DataFileFormat::Parquet)
            .partition(Struct::empty())
            .partition_spec_id(partition_spec_id)
            .record_count(10)
            .file_size_in_bytes(100)
            .build()
            .unwrap();
        LiveFile {
            data_file,
            partition_spec_id,
            snapshot_id: 11,
            sequence_number: 12,
            file_sequence_number: Some(13),
        }
    }

    fn test_written_puffin_dv_file() -> WrittenFile {
        WrittenFile {
            path: "s3://b/data/dv-00000000.puffin".to_string(),
            format: DataFileFormat::Puffin,
            content: DataContentType::PositionDeletes,
            partition_values: Struct::empty(),
            partition_spec_id: 0,
            record_count: 3,
            file_size_in_bytes: 40,
            split_offsets: Vec::new(),
            column_sizes: HashMap::new(),
            value_counts: HashMap::new(),
            null_value_counts: HashMap::new(),
            lower_bounds: HashMap::new(),
            upper_bounds: HashMap::new(),
            key_metadata: None,
            referenced_data_file: Some("s3://b/data/f.parquet".to_string()),
            equality_ids: None,
            first_row_id: None,
            content_offset: Some(4),
            content_size_in_bytes: Some(12),
            cardinality: Some(3),
        }
    }

    fn test_written_data_file(path: &str, record_count: u64) -> WrittenFile {
        WrittenFile {
            path: path.to_string(),
            format: DataFileFormat::Parquet,
            content: DataContentType::Data,
            partition_values: Struct::empty(),
            partition_spec_id: 0,
            record_count,
            file_size_in_bytes: 20,
            split_offsets: Vec::new(),
            column_sizes: HashMap::new(),
            value_counts: HashMap::new(),
            null_value_counts: HashMap::new(),
            lower_bounds: HashMap::new(),
            upper_bounds: HashMap::new(),
            key_metadata: None,
            referenced_data_file: None,
            equality_ids: None,
            first_row_id: Some(0),
            content_offset: None,
            content_size_in_bytes: None,
            cardinality: None,
        }
    }

    fn test_thrift_puffin_dv_file() -> crate::types::TIcebergDataFile {
        crate::types::TIcebergDataFile {
            path: Some("s3://b/data/dv-00000000.puffin".to_string()),
            format: Some("puffin".to_string()),
            record_count: Some(3),
            file_size_in_bytes: Some(40),
            partition_path: None,
            split_offsets: None,
            column_stats: None,
            partition_null_fingerprint: None,
            file_content: Some(crate::types::TIcebergFileContent::POSITION_DELETES),
            referenced_data_file: Some("s3://b/data/f.parquet".to_string()),
            first_row_id: None,
            equality_ids: None,
            key_metadata: None,
            partition_values_descriptor: Some(crate::types::TIcebergPartitionDescriptor {
                values: Some(vec![]),
            }),
            partition_spec_id: Some(0),
            content_offset: Some(4),
            content_size_in_bytes: Some(12),
            cardinality: Some(3),
        }
    }
}
