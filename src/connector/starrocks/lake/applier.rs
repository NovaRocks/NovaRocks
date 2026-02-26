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

use crate::connector::starrocks::lake::txn_log::{
    ensure_rowset_segment_meta_consistency, normalize_rowset_shared_segments,
};
use crate::formats::starrocks::writer::bundle_meta::next_rowset_id;
use crate::runtime::starlet_shard_registry::S3StoreConfig;
use crate::service::grpc_client::proto::starrocks::{
    KeysType, TabletMetadataPb, TabletSchemaPb, TxnLogPb, txn_log_pb,
};

enum TxnLogDispatch<'a> {
    Write(&'a txn_log_pb::OpWrite),
    SchemaChange(&'a txn_log_pb::OpSchemaChange),
    AlterMetadata(&'a txn_log_pb::OpAlterMetadata),
    Unsupported(&'static str),
    Empty,
}

fn dispatch_txn_log(txn_log: &TxnLogPb) -> TxnLogDispatch<'_> {
    if let Some(op_write) = txn_log.op_write.as_ref() {
        return TxnLogDispatch::Write(op_write);
    }
    if txn_log.op_compaction.is_some() {
        return TxnLogDispatch::Unsupported("op_compaction");
    }
    if let Some(op_schema_change) = txn_log.op_schema_change.as_ref() {
        return TxnLogDispatch::SchemaChange(op_schema_change);
    }
    if let Some(op_alter_metadata) = txn_log.op_alter_metadata.as_ref() {
        return TxnLogDispatch::AlterMetadata(op_alter_metadata);
    }
    if txn_log.op_replication.is_some() {
        return TxnLogDispatch::Unsupported("op_replication");
    }
    TxnLogDispatch::Empty
}

pub(crate) fn apply_txn_log_to_metadata(
    metadata: &mut TabletMetadataPb,
    txn_log: &TxnLogPb,
    default_schema_id: i64,
    tablet_schema: &TabletSchemaPb,
    tablet_root_path: &str,
    s3_config: Option<&S3StoreConfig>,
    apply_version: i64,
) -> Result<(), String> {
    match dispatch_txn_log(txn_log) {
        TxnLogDispatch::Write(op_write) => {
            let schema_id = maybe_update_metadata_schema_for_write(
                metadata,
                op_write,
                default_schema_id,
                tablet_schema,
                txn_log.tablet_id,
                txn_log.txn_id,
            )?;
            let txn_id = txn_log.txn_id.ok_or_else(|| {
                format!(
                    "txn log missing txn_id for publish_version: tablet_id={:?}",
                    txn_log.tablet_id
                )
            })?;
            if apply_version <= 0 {
                return Err(format!(
                    "invalid apply_version for publish_version: {}",
                    apply_version
                ));
            }

            if is_primary_keys_table(tablet_schema)? {
                return super::pk_applier::apply_primary_key_write_log_to_metadata(
                    metadata,
                    op_write,
                    schema_id,
                    tablet_schema,
                    tablet_root_path,
                    s3_config,
                    apply_version,
                    txn_id,
                );
            }

            if let Some(rowset) = op_write.rowset.as_ref()
                && (rowset.num_rows.unwrap_or(0) > 0 || rowset.delete_predicate.is_some())
            {
                let mut new_rowset = rowset.clone();
                ensure_rowset_segment_meta_consistency(&new_rowset)?;
                normalize_rowset_shared_segments(&mut new_rowset);
                let rowset_id = metadata
                    .next_rowset_id
                    .unwrap_or_else(|| next_rowset_id(&metadata.rowsets));
                new_rowset.id = Some(rowset_id);
                metadata.next_rowset_id.replace(
                    rowset_id.saturating_add(std::cmp::max(1, new_rowset.segments.len()) as u32),
                );
                if !metadata.rowset_to_schema.is_empty() {
                    metadata.rowset_to_schema.insert(rowset_id, schema_id);
                }
                metadata.rowsets.push(new_rowset);
            }
            Ok(())
        }
        TxnLogDispatch::SchemaChange(op_schema_change) => {
            if is_primary_keys_table(tablet_schema)? {
                return Err(format!(
                    "publish_version does not support op_schema_change for PRIMARY_KEYS yet: tablet_id={:?} txn_id={:?}",
                    txn_log.tablet_id, txn_log.txn_id
                ));
            }
            apply_schema_change_log(metadata, op_schema_change, default_schema_id)
        }
        TxnLogDispatch::AlterMetadata(op_alter_metadata) => {
            apply_alter_metadata_log(metadata, op_alter_metadata, txn_log)
        }
        TxnLogDispatch::Unsupported(op_name) => Err(format!(
            "publish_version does not support txn log operation {} yet: tablet_id={:?} txn_id={:?}",
            op_name, txn_log.tablet_id, txn_log.txn_id
        )),
        TxnLogDispatch::Empty => Ok(()),
    }
}

fn maybe_update_metadata_schema_for_write(
    metadata: &mut TabletMetadataPb,
    op_write: &txn_log_pb::OpWrite,
    default_schema_id: i64,
    runtime_schema: &TabletSchemaPb,
    tablet_id: Option<i64>,
    txn_id: Option<i64>,
) -> Result<i64, String> {
    let schema_key = op_write.schema_key.as_ref().ok_or_else(|| {
        format!(
            "op_write missing schema_key for publish_version: tablet_id={tablet_id:?} txn_id={txn_id:?}"
        )
    })?;
    let schema_id = schema_key
        .schema_id
        .filter(|v| *v > 0)
        .unwrap_or(default_schema_id);
    if schema_id <= 0 {
        return Err(format!(
            "op_write has non-positive schema_id after fallback: tablet_id={tablet_id:?} txn_id={txn_id:?} schema_id={schema_id} default_schema_id={default_schema_id}"
        ));
    }

    let current_schema_id = metadata
        .schema
        .as_ref()
        .and_then(|schema| schema.id)
        .unwrap_or(0);
    if current_schema_id == schema_id || metadata.historical_schemas.contains_key(&schema_id) {
        return Ok(schema_id);
    }

    let resolved_schema = if runtime_schema.id == Some(schema_id) {
        Some(runtime_schema.clone())
    } else {
        metadata.historical_schemas.get(&schema_id).cloned()
    };
    let resolved_schema = resolved_schema.ok_or_else(|| {
        format!(
            "publish_version cannot resolve schema from schema_key: tablet_id={tablet_id:?} txn_id={txn_id:?} schema_id={schema_id} runtime_schema_id={:?} historical_schema_ids={:?}",
            runtime_schema.id,
            metadata.historical_schemas.keys().collect::<Vec<_>>()
        )
    })?;

    apply_tablet_schema_update(
        metadata,
        &resolved_schema,
        format!("op_write schema update: tablet_id={tablet_id:?} txn_id={txn_id:?}"),
    )?;
    Ok(schema_id)
}

fn apply_tablet_schema_update(
    metadata: &mut TabletMetadataPb,
    new_schema: &TabletSchemaPb,
    reason: String,
) -> Result<(), String> {
    let new_schema_id = new_schema
        .id
        .ok_or_else(|| format!("tablet schema update missing schema id: reason={reason}"))?;
    if new_schema_id <= 0 {
        return Err(format!(
            "tablet schema update has non-positive schema id: reason={reason} schema_id={new_schema_id}"
        ));
    }

    if let Some(existing_schema) = metadata.schema.as_ref() {
        let existing_schema_id = existing_schema.id.unwrap_or(0);
        if existing_schema_id == new_schema_id {
            metadata
                .historical_schemas
                .entry(new_schema_id)
                .or_insert_with(|| existing_schema.clone());
            return Ok(());
        }

        if existing_schema_id > 0 {
            if metadata.rowset_to_schema.is_empty() && !metadata.rowsets.is_empty() {
                let existing_rowset_ids = metadata
                    .rowsets
                    .iter()
                    .map(|rowset| {
                        rowset.id.ok_or_else(|| {
                            format!(
                                "tablet rowset id is missing when switching schema: reason={reason} existing_schema_id={existing_schema_id}"
                            )
                        })
                    })
                    .collect::<Result<Vec<_>, String>>()?;
                for rowset_id in existing_rowset_ids {
                    metadata
                        .rowset_to_schema
                        .entry(rowset_id)
                        .or_insert(existing_schema_id);
                }
            }
            metadata
                .historical_schemas
                .entry(existing_schema_id)
                .or_insert_with(|| existing_schema.clone());
        }
    } else if !metadata.rowsets.is_empty() {
        // Recover from malformed metadata produced by historical schema-change/rollup paths:
        // rowsets exist but schema is absent. Bind existing rowsets to the incoming schema id.
        let existing_rowset_ids = metadata
            .rowsets
            .iter()
            .map(|rowset| {
                rowset.id.ok_or_else(|| {
                    format!(
                        "tablet rowset id is missing when recovering schema-less metadata: reason={reason}"
                    )
                })
            })
            .collect::<Result<Vec<_>, String>>()?;
        for rowset_id in existing_rowset_ids {
            metadata
                .rowset_to_schema
                .entry(rowset_id)
                .or_insert(new_schema_id);
        }
    }

    metadata
        .historical_schemas
        .entry(new_schema_id)
        .or_insert_with(|| new_schema.clone());
    metadata.schema = Some(new_schema.clone());
    Ok(())
}

fn apply_alter_metadata_log(
    metadata: &mut TabletMetadataPb,
    op_alter_metadata: &txn_log_pb::OpAlterMetadata,
    txn_log: &TxnLogPb,
) -> Result<(), String> {
    for update_info in &op_alter_metadata.metadata_update_infos {
        if let Some(enable_persistent_index) = update_info.enable_persistent_index {
            metadata.enable_persistent_index = Some(enable_persistent_index);
        }
        if let Some(persistent_index_type) = update_info.persistent_index_type {
            metadata.persistent_index_type = Some(persistent_index_type);
        }
        if let Some(compaction_strategy) = update_info.compaction_strategy {
            metadata.compaction_strategy = Some(compaction_strategy);
        }
        if let Some(flat_json_config) = update_info.flat_json_config.as_ref() {
            metadata.flat_json_config = Some(flat_json_config.clone());
        }
        if let Some(tablet_schema) = update_info.tablet_schema.as_ref() {
            apply_tablet_schema_update(
                metadata,
                tablet_schema,
                format!(
                    "op_alter_metadata schema update: tablet_id={:?} txn_id={:?}",
                    txn_log.tablet_id, txn_log.txn_id
                ),
            )?;
        }
    }
    Ok(())
}

fn apply_schema_change_log(
    metadata: &mut TabletMetadataPb,
    op_schema_change: &txn_log_pb::OpSchemaChange,
    default_schema_id: i64,
) -> Result<(), String> {
    for rowset in &op_schema_change.rowsets {
        let mut new_rowset = rowset.clone();
        ensure_rowset_segment_meta_consistency(&new_rowset)?;
        normalize_rowset_shared_segments(&mut new_rowset);
        let rowset_id = metadata
            .next_rowset_id
            .unwrap_or_else(|| next_rowset_id(&metadata.rowsets));
        new_rowset.id = Some(rowset_id);
        metadata
            .next_rowset_id
            .replace(rowset_id.saturating_add(std::cmp::max(1, new_rowset.segments.len()) as u32));
        if !metadata.rowset_to_schema.is_empty() {
            metadata
                .rowset_to_schema
                .insert(rowset_id, default_schema_id);
        }
        metadata.rowsets.push(new_rowset);
    }
    if let Some(delvec_meta) = op_schema_change.delvec_meta.as_ref() {
        metadata.delvec_meta = Some(delvec_meta.clone());
    }
    Ok(())
}

fn is_primary_keys_table(tablet_schema: &TabletSchemaPb) -> Result<bool, String> {
    let keys_type_raw = tablet_schema
        .keys_type
        .ok_or_else(|| "tablet schema missing keys_type for publish_version".to_string())?;
    let keys_type = KeysType::try_from(keys_type_raw).map_err(|_| {
        format!(
            "unknown keys_type in tablet schema for publish_version: {}",
            keys_type_raw
        )
    })?;
    Ok(keys_type == KeysType::PrimaryKeys)
}

#[cfg(test)]
mod tests {
    use tempfile::tempdir;

    use crate::service::grpc_client::proto::starrocks::{
        ColumnPb, CompactionStrategyPb, KeysType, MetadataUpdateInfoPb, RowsetMetadataPb,
        TableSchemaKeyPb, TabletMetadataPb, TabletSchemaPb, TxnLogPb, txn_log_pb,
    };

    use super::apply_txn_log_to_metadata;

    fn test_tablet_schema() -> TabletSchemaPb {
        TabletSchemaPb {
            keys_type: Some(KeysType::DupKeys as i32),
            column: vec![ColumnPb {
                unique_id: 1,
                name: Some("c1".to_string()),
                r#type: "BIGINT".to_string(),
                is_key: Some(true),
                aggregation: None,
                is_nullable: Some(false),
                default_value: None,
                precision: None,
                frac: None,
                length: None,
                index_length: None,
                is_bf_column: None,
                referenced_column_id: None,
                referenced_column: None,
                has_bitmap_index: None,
                visible: None,
                children_columns: Vec::new(),
                is_auto_increment: Some(false),
                agg_state_desc: None,
            }],
            num_short_key_columns: Some(1),
            num_rows_per_row_block: None,
            bf_fpp: None,
            next_column_unique_id: Some(2),
            deprecated_is_in_memory: None,
            deprecated_id: None,
            compression_type: None,
            sort_key_idxes: vec![0],
            schema_version: Some(0),
            sort_key_unique_ids: vec![1],
            table_indices: Vec::new(),
            compression_level: None,
            id: Some(1),
        }
    }

    fn test_tablet_schema_v2(schema_id: i64, schema_version: i32) -> TabletSchemaPb {
        TabletSchemaPb {
            keys_type: Some(KeysType::DupKeys as i32),
            column: vec![
                ColumnPb {
                    unique_id: 1,
                    name: Some("c1".to_string()),
                    r#type: "BIGINT".to_string(),
                    is_key: Some(true),
                    aggregation: None,
                    is_nullable: Some(false),
                    default_value: None,
                    precision: None,
                    frac: None,
                    length: None,
                    index_length: None,
                    is_bf_column: None,
                    referenced_column_id: None,
                    referenced_column: None,
                    has_bitmap_index: None,
                    visible: None,
                    children_columns: Vec::new(),
                    is_auto_increment: Some(false),
                    agg_state_desc: None,
                },
                ColumnPb {
                    unique_id: 2,
                    name: Some("c2".to_string()),
                    r#type: "INT".to_string(),
                    is_key: Some(false),
                    aggregation: Some("NONE".to_string()),
                    is_nullable: Some(true),
                    default_value: Some(b"7".to_vec()),
                    precision: None,
                    frac: None,
                    length: None,
                    index_length: None,
                    is_bf_column: None,
                    referenced_column_id: None,
                    referenced_column: None,
                    has_bitmap_index: None,
                    visible: None,
                    children_columns: Vec::new(),
                    is_auto_increment: Some(false),
                    agg_state_desc: None,
                },
            ],
            num_short_key_columns: Some(1),
            num_rows_per_row_block: None,
            bf_fpp: None,
            next_column_unique_id: Some(3),
            deprecated_is_in_memory: None,
            deprecated_id: None,
            compression_type: None,
            sort_key_idxes: vec![0],
            schema_version: Some(schema_version),
            sort_key_unique_ids: vec![1],
            table_indices: Vec::new(),
            compression_level: None,
            id: Some(schema_id),
        }
    }

    fn test_rowset(segment_name: &str, row_count: i64, data_size: i64) -> RowsetMetadataPb {
        RowsetMetadataPb {
            id: None,
            overlapped: Some(false),
            segments: vec![segment_name.to_string()],
            num_rows: Some(row_count),
            data_size: Some(data_size),
            delete_predicate: None,
            num_dels: Some(0),
            segment_size: vec![u64::try_from(data_size).unwrap_or(0)],
            max_compact_input_rowset_id: None,
            version: None,
            del_files: Vec::new(),
            segment_encryption_metas: Vec::new(),
            next_compaction_offset: None,
            bundle_file_offsets: vec![0],
            shared_segments: vec![false],
            record_predicate: None,
            segment_metas: Vec::new(),
        }
    }

    #[test]
    fn apply_txn_log_assigns_rowset_id_and_next_rowset_id() {
        let mut meta = TabletMetadataPb {
            id: Some(1),
            version: Some(1),
            schema: None,
            rowsets: Vec::new(),
            next_rowset_id: Some(5),
            cumulative_point: Some(0),
            delvec_meta: None,
            compaction_inputs: Vec::new(),
            prev_garbage_version: None,
            orphan_files: Vec::new(),
            enable_persistent_index: None,
            persistent_index_type: None,
            commit_time: None,
            source_schema: None,
            sstable_meta: None,
            dcg_meta: None,
            historical_schemas: std::collections::HashMap::new(),
            rowset_to_schema: std::collections::HashMap::new(),
            gtid: Some(0),
            compaction_strategy: None,
            flat_json_config: None,
        };
        let txn_log = TxnLogPb {
            tablet_id: Some(1),
            txn_id: Some(10),
            op_write: Some(txn_log_pb::OpWrite {
                rowset: Some(test_rowset("seg_a.dat", 3, 24)),
                txn_meta: None,
                dels: Vec::new(),
                rewrite_segments: Vec::new(),
                del_encryption_metas: Vec::new(),
                ssts: Vec::new(),
                schema_key: Some(TableSchemaKeyPb {
                    db_id: Some(1),
                    table_id: Some(2),
                    schema_id: Some(1),
                }),
            }),
            op_compaction: None,
            op_schema_change: None,
            op_alter_metadata: None,
            op_replication: None,
            partition_id: Some(1),
            load_id: None,
        };

        let tmp = tempdir().expect("create tempdir");
        let root = tmp.path().to_string_lossy().to_string();
        apply_txn_log_to_metadata(
            &mut meta,
            &txn_log,
            3,
            &test_tablet_schema(),
            &root,
            None,
            2,
        )
        .expect("apply txn log");
        assert_eq!(meta.rowsets.len(), 1);
        assert_eq!(meta.rowsets[0].id, Some(5));
        assert_eq!(meta.next_rowset_id, Some(6));
    }

    #[test]
    fn apply_txn_log_rejects_unsupported_operation_with_explicit_name() {
        let mut meta = TabletMetadataPb {
            id: Some(2),
            version: Some(1),
            schema: None,
            rowsets: Vec::new(),
            next_rowset_id: Some(1),
            cumulative_point: Some(0),
            delvec_meta: None,
            compaction_inputs: Vec::new(),
            prev_garbage_version: None,
            orphan_files: Vec::new(),
            enable_persistent_index: None,
            persistent_index_type: None,
            commit_time: None,
            source_schema: None,
            sstable_meta: None,
            dcg_meta: None,
            historical_schemas: std::collections::HashMap::new(),
            rowset_to_schema: std::collections::HashMap::new(),
            gtid: Some(0),
            compaction_strategy: None,
            flat_json_config: None,
        };
        let txn_log = TxnLogPb {
            tablet_id: Some(2),
            txn_id: Some(20),
            op_write: None,
            op_compaction: Some(Default::default()),
            op_schema_change: None,
            op_alter_metadata: None,
            op_replication: None,
            partition_id: Some(1),
            load_id: None,
        };
        let tmp = tempdir().expect("create tempdir");
        let root = tmp.path().to_string_lossy().to_string();
        let err = apply_txn_log_to_metadata(
            &mut meta,
            &txn_log,
            3,
            &test_tablet_schema(),
            &root,
            None,
            2,
        )
        .expect_err("compaction should fail");
        assert!(err.contains("op_compaction"), "unexpected error: {err}");
    }

    #[test]
    fn apply_txn_log_applies_schema_change_rowsets() {
        let mut meta = TabletMetadataPb {
            id: Some(3),
            version: Some(1),
            schema: None,
            rowsets: Vec::new(),
            next_rowset_id: Some(10),
            cumulative_point: Some(0),
            delvec_meta: None,
            compaction_inputs: Vec::new(),
            prev_garbage_version: None,
            orphan_files: Vec::new(),
            enable_persistent_index: None,
            persistent_index_type: None,
            commit_time: None,
            source_schema: None,
            sstable_meta: None,
            dcg_meta: None,
            historical_schemas: std::collections::HashMap::new(),
            rowset_to_schema: std::collections::HashMap::from([(1_u32, 1_i64)]),
            gtid: Some(0),
            compaction_strategy: None,
            flat_json_config: None,
        };
        let txn_log = TxnLogPb {
            tablet_id: Some(3),
            txn_id: Some(30),
            op_write: None,
            op_compaction: None,
            op_schema_change: Some(txn_log_pb::OpSchemaChange {
                rowsets: vec![test_rowset("seg_sc.dat", 4, 32)],
                linked_segment: Some(false),
                alter_version: Some(5),
                delvec_meta: None,
            }),
            op_alter_metadata: None,
            op_replication: None,
            partition_id: Some(1),
            load_id: None,
        };
        let tmp = tempdir().expect("create tempdir");
        let root = tmp.path().to_string_lossy().to_string();
        apply_txn_log_to_metadata(
            &mut meta,
            &txn_log,
            99,
            &test_tablet_schema(),
            &root,
            None,
            2,
        )
        .expect("apply schema change txn log");
        assert_eq!(meta.rowsets.len(), 1);
        assert_eq!(meta.rowsets[0].id, Some(10));
        assert_eq!(meta.next_rowset_id, Some(11));
        assert_eq!(meta.rowset_to_schema.get(&10), Some(&99));
    }

    #[test]
    fn apply_txn_log_updates_schema_from_runtime_schema_key() {
        let old_schema = test_tablet_schema();
        let new_schema = test_tablet_schema_v2(2, 2);
        let mut existing_rowset = test_rowset("seg_old.dat", 2, 24);
        existing_rowset.id = Some(7);

        let mut meta = TabletMetadataPb {
            id: Some(4),
            version: Some(2),
            schema: Some(old_schema.clone()),
            rowsets: vec![existing_rowset],
            next_rowset_id: Some(8),
            cumulative_point: Some(0),
            delvec_meta: None,
            compaction_inputs: Vec::new(),
            prev_garbage_version: None,
            orphan_files: Vec::new(),
            enable_persistent_index: None,
            persistent_index_type: None,
            commit_time: None,
            source_schema: None,
            sstable_meta: None,
            dcg_meta: None,
            historical_schemas: std::collections::HashMap::new(),
            rowset_to_schema: std::collections::HashMap::new(),
            gtid: Some(0),
            compaction_strategy: None,
            flat_json_config: None,
        };
        let txn_log = TxnLogPb {
            tablet_id: Some(4),
            txn_id: Some(40),
            op_write: Some(txn_log_pb::OpWrite {
                rowset: None,
                txn_meta: None,
                dels: Vec::new(),
                rewrite_segments: Vec::new(),
                del_encryption_metas: Vec::new(),
                ssts: Vec::new(),
                schema_key: Some(TableSchemaKeyPb {
                    db_id: Some(1),
                    table_id: Some(2),
                    schema_id: Some(2),
                }),
            }),
            op_compaction: None,
            op_schema_change: None,
            op_alter_metadata: None,
            op_replication: None,
            partition_id: Some(1),
            load_id: None,
        };

        let tmp = tempdir().expect("create tempdir");
        let root = tmp.path().to_string_lossy().to_string();
        apply_txn_log_to_metadata(&mut meta, &txn_log, 1, &new_schema, &root, None, 3)
            .expect("apply op_write schema update");
        assert_eq!(meta.schema.as_ref().and_then(|s| s.id), Some(2));
        assert_eq!(meta.rowset_to_schema.get(&7), Some(&1));
        assert!(meta.historical_schemas.contains_key(&1));
        assert!(meta.historical_schemas.contains_key(&2));
    }

    #[test]
    fn apply_txn_log_applies_alter_metadata_schema_update() {
        let old_schema = test_tablet_schema();
        let new_schema = test_tablet_schema_v2(3, 3);
        let mut existing_rowset = test_rowset("seg_old_2.dat", 1, 12);
        existing_rowset.id = Some(9);

        let mut meta = TabletMetadataPb {
            id: Some(5),
            version: Some(2),
            schema: Some(old_schema),
            rowsets: vec![existing_rowset],
            next_rowset_id: Some(10),
            cumulative_point: Some(0),
            delvec_meta: None,
            compaction_inputs: Vec::new(),
            prev_garbage_version: None,
            orphan_files: Vec::new(),
            enable_persistent_index: None,
            persistent_index_type: None,
            commit_time: None,
            source_schema: None,
            sstable_meta: None,
            dcg_meta: None,
            historical_schemas: std::collections::HashMap::new(),
            rowset_to_schema: std::collections::HashMap::new(),
            gtid: Some(0),
            compaction_strategy: None,
            flat_json_config: None,
        };
        let txn_log = TxnLogPb {
            tablet_id: Some(5),
            txn_id: Some(50),
            op_write: None,
            op_compaction: None,
            op_schema_change: None,
            op_alter_metadata: Some(txn_log_pb::OpAlterMetadata {
                metadata_update_infos: vec![MetadataUpdateInfoPb {
                    enable_persistent_index: Some(true),
                    tablet_schema: Some(new_schema),
                    persistent_index_type: None,
                    bundle_tablet_metadata: None,
                    compaction_strategy: Some(CompactionStrategyPb::Default as i32),
                    flat_json_config: None,
                }],
            }),
            op_replication: None,
            partition_id: Some(1),
            load_id: None,
        };

        let tmp = tempdir().expect("create tempdir");
        let root = tmp.path().to_string_lossy().to_string();
        apply_txn_log_to_metadata(
            &mut meta,
            &txn_log,
            1,
            &test_tablet_schema_v2(3, 3),
            &root,
            None,
            3,
        )
        .expect("apply op_alter_metadata");
        assert_eq!(meta.schema.as_ref().and_then(|s| s.id), Some(3));
        assert_eq!(meta.rowset_to_schema.get(&9), Some(&1));
        assert_eq!(meta.enable_persistent_index, Some(true));
        assert_eq!(
            meta.compaction_strategy,
            Some(CompactionStrategyPb::Default as i32)
        );
    }

    #[test]
    fn apply_txn_log_recovers_schema_when_metadata_schema_is_missing() {
        let runtime_schema = test_tablet_schema_v2(7, 1);
        let mut existing_rowset = test_rowset("seg_missing_schema.dat", 3, 21);
        existing_rowset.id = Some(12);
        let mut meta = TabletMetadataPb {
            id: Some(6),
            version: Some(2),
            schema: None,
            rowsets: vec![existing_rowset],
            next_rowset_id: Some(13),
            cumulative_point: Some(0),
            delvec_meta: None,
            compaction_inputs: Vec::new(),
            prev_garbage_version: None,
            orphan_files: Vec::new(),
            enable_persistent_index: None,
            persistent_index_type: None,
            commit_time: None,
            source_schema: None,
            sstable_meta: None,
            dcg_meta: None,
            historical_schemas: std::collections::HashMap::new(),
            rowset_to_schema: std::collections::HashMap::new(),
            gtid: Some(0),
            compaction_strategy: None,
            flat_json_config: None,
        };
        let txn_log = TxnLogPb {
            tablet_id: Some(6),
            txn_id: Some(60),
            op_write: Some(txn_log_pb::OpWrite {
                rowset: None,
                txn_meta: None,
                dels: Vec::new(),
                rewrite_segments: Vec::new(),
                del_encryption_metas: Vec::new(),
                ssts: Vec::new(),
                schema_key: Some(TableSchemaKeyPb {
                    db_id: Some(1),
                    table_id: Some(2),
                    schema_id: Some(7),
                }),
            }),
            op_compaction: None,
            op_schema_change: None,
            op_alter_metadata: None,
            op_replication: None,
            partition_id: Some(1),
            load_id: None,
        };
        let tmp = tempdir().expect("create tempdir");
        let root = tmp.path().to_string_lossy().to_string();
        apply_txn_log_to_metadata(&mut meta, &txn_log, 7, &runtime_schema, &root, None, 3)
            .expect("recover schema-less metadata");
        assert_eq!(meta.schema.as_ref().and_then(|s| s.id), Some(7));
        assert_eq!(meta.rowset_to_schema.get(&12), Some(&7));
        assert!(meta.historical_schemas.contains_key(&7));
    }
}
