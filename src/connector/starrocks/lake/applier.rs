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
    KeysType, TabletMetadataPb, TabletSchemaPb, TxnLogPb,
};

enum TxnLogDispatch<'a> {
    Write(&'a crate::service::grpc_client::proto::starrocks::txn_log_pb::OpWrite),
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
    if txn_log.op_schema_change.is_some() {
        return TxnLogDispatch::Unsupported("op_schema_change");
    }
    if txn_log.op_alter_metadata.is_some() {
        return TxnLogDispatch::Unsupported("op_alter_metadata");
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
            let schema_id = op_write
                .schema_key
                .as_ref()
                .and_then(|k| k.schema_id)
                .filter(|v| *v > 0)
                .unwrap_or(default_schema_id);
            if op_write.schema_key.is_none() {
                return Err(format!(
                    "op_write missing schema_key for publish_version: tablet_id={:?} txn_id={:?}",
                    txn_log.tablet_id, txn_log.txn_id
                ));
            }
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
        TxnLogDispatch::Unsupported(op_name) => Err(format!(
            "publish_version does not support txn log operation {} yet: tablet_id={:?} txn_id={:?}",
            op_name, txn_log.tablet_id, txn_log.txn_id
        )),
        TxnLogDispatch::Empty => Ok(()),
    }
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
        ColumnPb, KeysType, RowsetMetadataPb, TableSchemaKeyPb, TabletMetadataPb, TabletSchemaPb,
        TxnLogPb, txn_log_pb,
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
                    schema_id: Some(3),
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
}
