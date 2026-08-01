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

use std::cmp::Ordering;
use std::collections::{HashMap, HashSet};
use std::hash::{Hash, Hasher};
use std::sync::Arc;

use arrow::array::{
    Array, ArrayRef, BinaryArray, BooleanArray, Date32Array, Decimal128Array, Decimal256Array,
    FixedSizeBinaryArray, Float32Array, Float64Array, Int8Array, Int16Array, Int32Array,
    Int64Array, StringArray, TimestampMicrosecondArray, UInt32Array, new_null_array,
};
use arrow::compute::{cast, concat, take};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit};
use arrow::record_batch::RecordBatch;
use arrow_buffer::i256;
use chrono::{Datelike, NaiveDate, NaiveDateTime};

use crate::common::ids::SlotId;
use crate::common::types::UniqueId;
use crate::connector::schema::{self, BeTabletWriteLoadLogRecord, BeTxnActiveRecord};
use crate::connector::starrocks::ObjectStoreProfile;
use crate::connector::starrocks::lake::context::{
    PartialUpdateWriteMode, TabletWriteContext, register_tablet_runtime, with_txn_log_append_lock,
};
use crate::connector::starrocks::lake::delete_payload_codec::{
    decode_delete_keys_payload, encode_delete_keys_payload,
};
use crate::connector::starrocks::lake::storage_domain::{
    StorageCombinedTransactionLog, StorageRowset, StorageSchemaKey, StorageTabletMetadata,
    StorageTransactionLog, StorageWriteOperation,
};
use crate::connector::starrocks::schema::{
    StarRocksColumnSchema, StarRocksKeysType, StarRocksTabletSchema,
};
use crate::connector::starrocks::sink::auto_increment::allocate_auto_increment_ids;
use crate::exec::chunk::Chunk;
use crate::formats::starrocks::metadata::{
    StarRocksDeletePredicateRaw, StarRocksSegmentFile, StarRocksTabletSnapshot,
    load_bundle_segment_footers, load_tablet_snapshot_with_metadata_provider,
    tablet_operator_relative_path,
};
use crate::formats::starrocks::plan::{
    StarRocksOutputColumnHint, StarRocksPhysicalColumnBinding,
    build_native_read_plan_with_output_hints,
};
use crate::formats::starrocks::reader::build_native_record_batch;
use crate::formats::starrocks::writer::bundle_meta::load_latest_tablet_metadata_with_provider;
use crate::formats::starrocks::writer::io::{read_bytes, read_bytes_if_exists, write_bytes};
use crate::formats::starrocks::writer::layout::{
    DATA_DIR, build_data_file_name, join_tablet_path, txn_log_file_path,
    txn_log_file_path_with_load_id,
};
use crate::formats::starrocks::writer::{
    StarRocksWriteFormat, build_single_segment_facts, build_starrocks_native_segment_bytes,
    build_txn_data_file_name, read_bundle_parquet_snapshot_if_any, sort_batch_for_native_write,
    write_parquet_file,
};
use crate::novarocks_logging::info;
use crate::runtime::starlet_shard_registry::S3StoreConfig;
use novarocks_types::decimal::{LEGACY_DECIMALV2_PRECISION, LEGACY_DECIMALV2_SCALE};

pub(crate) fn append_lake_txn_log_with_chunk_rowset(
    ctx: &TabletWriteContext,
    chunk: &Chunk,
    txn_id: i64,
    driver_id: i32,
    file_seq: u64,
    write_format: StarRocksWriteFormat,
    partition_id: i64,
    load_id: Option<&UniqueId>,
) -> Result<(), String> {
    let batch_slot_ids = slot_ids_from_chunk(chunk);
    append_lake_txn_log_with_rowset_impl(
        ctx,
        &chunk.batch,
        Some(&batch_slot_ids),
        txn_id,
        driver_id,
        file_seq,
        write_format,
        partition_id,
        load_id,
    )
}

fn append_lake_txn_log_with_rowset_impl(
    ctx: &TabletWriteContext,
    batch: &RecordBatch,
    batch_slot_ids: Option<&[Option<SlotId>]>,
    txn_id: i64,
    driver_id: i32,
    file_seq: u64,
    write_format: StarRocksWriteFormat,
    partition_id: i64,
    load_id: Option<&UniqueId>,
) -> Result<(), String> {
    tracing::info!(
        "append_lake_txn_log_with_rowset_impl: rows={} columns={} schema_columns={} tablet_id={}",
        batch.num_rows(),
        batch.num_columns(),
        ctx.tablet_schema.column.len(),
        ctx.tablet_id
    );
    let begin_time_ms = unix_millis_now();
    if ctx.table_id <= 0 {
        return Err(format!("invalid table_id for lake write: {}", ctx.table_id));
    }
    if ctx.tablet_id <= 0 {
        return Err(format!(
            "invalid tablet_id for lake write: {}",
            ctx.tablet_id
        ));
    }
    if txn_id <= 0 {
        return Err(format!("invalid txn_id for lake write: {}", txn_id));
    }
    if batch.num_rows() == 0 {
        return Err("cannot append empty record batch into lake txn log".to_string());
    }

    let storage_metadata = ctx.storage_metadata_provider.as_deref().ok_or_else(|| {
        "StarRocks lake transaction writes require a storage metadata provider".to_string()
    })?;

    // A full-schema upsert has no partial-update state to materialize and no
    // delete-key control column to interpret.  Let the compat-owned codec
    // carry the transaction log at the file boundary in that common path.
    // The remaining routing variants deliberately retain the legacy path
    // until their partial-update facts are domainized as well.
    if can_append_full_upsert_through_storage_provider(ctx, batch) {
        return append_full_upsert_through_storage_provider(
            ctx,
            batch,
            txn_id,
            driver_id,
            file_seq,
            write_format,
            partition_id,
            load_id,
            begin_time_ms,
        );
    }

    register_tablet_runtime(ctx)?;
    let schema_key = build_storage_table_schema_key(ctx)?;

    let legacy_log_path = txn_log_file_path(&ctx.tablet_root_path, ctx.tablet_id, txn_id)?;
    let primary_log_path = if let Some(load_id) = load_id {
        txn_log_file_path_with_load_id(&ctx.tablet_root_path, ctx.tablet_id, txn_id, load_id)?
    } else {
        legacy_log_path.clone()
    };
    with_txn_log_append_lock(ctx.tablet_id, txn_id, || {
        let mut txn_log = match read_storage_txn_log_if_exists(storage_metadata, &primary_log_path)?
        {
            Some(existing) => existing,
            None => StorageTransactionLog {
                tablet_id: Some(ctx.tablet_id),
                txn_id: Some(txn_id),
                write: Some(StorageWriteOperation {
                    rowset: None,
                    txn_meta: None,
                    dels: Vec::new(),
                    rewrite_segments: Vec::new(),
                    del_encryption_metas: Vec::new(),
                    ssts: Vec::new(),
                    schema_key: Some(schema_key.clone()),
                }),
                compaction: None,
                schema_change: None,
                alter_metadata: None,
                replication: None,
                partition_id: Some(partition_id),
                load_id: load_id.map(|id| (id.high(), id.low())),
            },
        };
        let write_routing = resolve_lake_batch_write_routing_with_slots(
            ctx,
            batch,
            batch_slot_ids,
            Some(&txn_log),
        )?;
        let (mut incoming_rowset, incoming_dels) = match write_routing {
            LakeBatchWriteRouting::Empty => (
                StorageRowset {
                    id: None,
                    overlapped: Some(false),
                    segments: Vec::new(),
                    num_rows: Some(0),
                    data_size: Some(0),
                    delete_predicate: None,
                    num_dels: Some(0),
                    segment_size: Vec::new(),
                    max_compact_input_rowset_id: None,
                    version: None,
                    del_files: Vec::new(),
                    segment_encryption_metas: Vec::new(),
                    next_compaction_offset: None,
                    bundle_file_offsets: Vec::new(),
                    shared_segments: Vec::new(),
                    record_predicate: None,
                    segment_metas: Vec::new(),
                },
                Vec::new(),
            ),
            LakeBatchWriteRouting::DeleteKeysOnly { key_batch } => {
                let del_file_name = build_txn_delete_file_name(
                    ctx.tablet_id,
                    txn_id,
                    driver_id,
                    file_seq,
                    load_id,
                )?;
                if txn_log_contains_del_file(&txn_log, &del_file_name) {
                    return Ok(());
                }
                let del_file_path = join_tablet_path(
                    &ctx.tablet_root_path,
                    &format!("{DATA_DIR}/{del_file_name}"),
                )?;
                let del_payload = encode_delete_keys_file_payload(&key_batch, &ctx.tablet_schema)?;
                write_bytes(&del_file_path, del_payload)?;
                (
                    StorageRowset {
                        id: None,
                        overlapped: Some(false),
                        segments: Vec::new(),
                        num_rows: Some(0),
                        data_size: Some(0),
                        delete_predicate: None,
                        num_dels: Some(key_batch.num_rows() as i64),
                        segment_size: Vec::new(),
                        max_compact_input_rowset_id: None,
                        version: None,
                        del_files: Vec::new(),
                        segment_encryption_metas: Vec::new(),
                        next_compaction_offset: None,
                        bundle_file_offsets: Vec::new(),
                        shared_segments: Vec::new(),
                        record_predicate: None,
                        segment_metas: Vec::new(),
                    },
                    vec![del_file_name],
                )
            }
            LakeBatchWriteRouting::Upsert { data_batch } => {
                let data_file_name = build_txn_upsert_data_file_name(
                    ctx,
                    txn_id,
                    driver_id,
                    file_seq,
                    write_format,
                    load_id,
                )?;
                if txn_log_contains_data_segment(&txn_log, &data_file_name) {
                    return Ok(());
                }
                let rowset = build_rowset_for_upsert_batch(
                    ctx,
                    &data_batch,
                    txn_id,
                    driver_id,
                    file_seq,
                    write_format,
                    load_id,
                )?;
                (rowset, Vec::new())
            }
            LakeBatchWriteRouting::Mixed {
                upsert_batch,
                delete_key_batch,
            } => {
                let data_file_name = build_txn_upsert_data_file_name(
                    ctx,
                    txn_id,
                    driver_id,
                    file_seq,
                    write_format,
                    load_id,
                )?;
                let del_file_name = build_txn_delete_file_name(
                    ctx.tablet_id,
                    txn_id,
                    driver_id,
                    file_seq,
                    load_id,
                )?;
                if txn_log_contains_data_segment(&txn_log, &data_file_name)
                    || txn_log_contains_del_file(&txn_log, &del_file_name)
                {
                    return Ok(());
                }
                let mut rowset = build_rowset_for_upsert_batch(
                    ctx,
                    &upsert_batch,
                    txn_id,
                    driver_id,
                    file_seq,
                    write_format,
                    load_id,
                )?;
                let del_file_path = join_tablet_path(
                    &ctx.tablet_root_path,
                    &format!("{DATA_DIR}/{del_file_name}"),
                )?;
                let del_payload =
                    encode_delete_keys_file_payload(&delete_key_batch, &ctx.tablet_schema)?;
                write_bytes(&del_file_path, del_payload)?;
                rowset.num_dels = Some(delete_key_batch.num_rows() as i64);
                (rowset, vec![del_file_name])
            }
        };
        if ctx.partial_update.merge_condition.is_some() {
            info!(
                target: "novarocks::sink",
                table_id = ctx.table_id,
                tablet_id = ctx.tablet_id,
                txn_id,
                incoming_segments = ?incoming_rowset.segments,
                incoming_num_rows = incoming_rowset.num_rows.unwrap_or(0),
                incoming_num_dels = incoming_rowset.num_dels.unwrap_or(0),
                incoming_data_size = incoming_rowset.data_size.unwrap_or(0),
                incoming_del_files = ?incoming_dels,
                "OLAP_TABLE_SINK append_lake_txn_log incoming rowset"
            );
        }
        normalize_storage_rowset_shared_segments(&mut incoming_rowset);
        ensure_storage_rowset_segment_meta_consistency(&incoming_rowset)?;
        upsert_storage_write_rowset_in_txn_log(
            &mut txn_log,
            ctx.tablet_id,
            txn_id,
            partition_id,
            &incoming_rowset,
            &incoming_dels,
            load_id,
            &schema_key,
        )?;
        if ctx.partial_update.merge_condition.is_some()
            && let Some(op_write) = txn_log.write.as_ref()
            && let Some(rowset) = op_write.rowset.as_ref()
        {
            info!(
                target: "novarocks::sink",
                table_id = ctx.table_id,
                tablet_id = ctx.tablet_id,
                txn_id,
                merged_segments = ?rowset.segments,
                merged_num_rows = rowset.num_rows.unwrap_or(0),
                merged_num_dels = rowset.num_dels.unwrap_or(0),
                merged_data_size = rowset.data_size.unwrap_or(0),
                merged_del_files = ?op_write.dels,
                "OLAP_TABLE_SINK append_lake_txn_log merged rowset in primary log"
            );
        }
        write_storage_txn_log(storage_metadata, &primary_log_path, &txn_log)?;

        // Keep writing a legacy plain txn log for compatibility with FE requests
        // that do not carry load_ids.
        if load_id.is_some() && legacy_log_path != primary_log_path {
            let mut legacy_log =
                match read_storage_txn_log_if_exists(storage_metadata, &legacy_log_path)? {
                    Some(existing) => existing,
                    None => StorageTransactionLog {
                        tablet_id: Some(ctx.tablet_id),
                        txn_id: Some(txn_id),
                        write: Some(StorageWriteOperation {
                            rowset: None,
                            txn_meta: None,
                            dels: Vec::new(),
                            rewrite_segments: Vec::new(),
                            del_encryption_metas: Vec::new(),
                            ssts: Vec::new(),
                            schema_key: Some(schema_key.clone()),
                        }),
                        compaction: None,
                        schema_change: None,
                        alter_metadata: None,
                        replication: None,
                        partition_id: Some(partition_id),
                        load_id: None,
                    },
                };
            upsert_storage_write_rowset_in_txn_log(
                &mut legacy_log,
                ctx.tablet_id,
                txn_id,
                partition_id,
                &incoming_rowset,
                &incoming_dels,
                None,
                &schema_key,
            )?;
            if ctx.partial_update.merge_condition.is_some()
                && let Some(op_write) = legacy_log.write.as_ref()
                && let Some(rowset) = op_write.rowset.as_ref()
            {
                info!(
                    target: "novarocks::sink",
                    table_id = ctx.table_id,
                    tablet_id = ctx.tablet_id,
                    txn_id,
                    merged_segments = ?rowset.segments,
                    merged_num_rows = rowset.num_rows.unwrap_or(0),
                    merged_num_dels = rowset.num_dels.unwrap_or(0),
                    merged_data_size = rowset.data_size.unwrap_or(0),
                    merged_del_files = ?op_write.dels,
                    "OLAP_TABLE_SINK append_lake_txn_log merged rowset in legacy log"
                );
            }
            write_storage_txn_log(storage_metadata, &legacy_log_path, &legacy_log)?;
        }

        let backend_id = crate::runtime::backend_id::backend_id().unwrap_or(-1);
        let mut rowset_id = String::new();
        let mut output_rows = 0_i64;
        let mut output_bytes = 0_i64;
        let mut output_segments = 0_i32;
        let mut num_delfile = 0_i64;
        if let Some(op_write) = txn_log.write.as_ref() {
            if let Some(rowset) = op_write.rowset.as_ref() {
                rowset_id = rowset.id.map(|value| value.to_string()).unwrap_or_default();
                output_rows = rowset.num_rows.unwrap_or(0).max(0);
                output_bytes = rowset.data_size.unwrap_or(0).max(0);
                output_segments = i32::try_from(rowset.segments.len()).unwrap_or(i32::MAX);
            }
            num_delfile = i64::try_from(op_write.dels.len()).unwrap_or(i64::MAX);
        }
        let label =
            load_id.map(|value| crate::common::types::format_uuid(value.high(), value.low()));
        let finish_time_ms = unix_millis_now();
        schema::record_tablet_write_load_log(BeTabletWriteLoadLogRecord {
            backend_id,
            begin_time_ms,
            finish_time_ms,
            txn_id,
            tablet_id: ctx.tablet_id,
            table_id: ctx.table_id,
            partition_id,
            input_rows: i64::try_from(batch.num_rows()).unwrap_or(i64::MAX),
            input_bytes: i64::try_from(batch.get_array_memory_size()).unwrap_or(i64::MAX),
            output_rows,
            output_bytes,
            output_segments,
            label,
        });
        schema::record_be_txn_active(BeTxnActiveRecord {
            backend_id,
            load_id_hi: load_id.map(|value| value.high()),
            load_id_lo: load_id.map(|value| value.low()),
            txn_id,
            partition_id,
            tablet_id: ctx.tablet_id,
            rowset_id: Some(rowset_id),
            num_segment: i64::from(output_segments),
            num_delfile,
            num_row: output_rows,
            data_size: output_bytes,
        });
        Ok(())
    })
}

fn can_append_full_upsert_through_storage_provider(
    ctx: &TabletWriteContext,
    batch: &RecordBatch,
) -> bool {
    ctx.storage_metadata_provider.is_some()
        && batch.num_columns() == ctx.tablet_schema.column.len()
        && !batch
            .schema()
            .fields()
            .iter()
            .any(|field| field.name() == LOAD_OP_COLUMN)
        && ctx.partial_update.merge_condition.is_none()
        && ctx.partial_update.column_to_expr_value.is_empty()
        && !ctx.partial_update.auto_increment.miss_auto_increment_column
        && !ctx
            .partial_update
            .auto_increment
            .null_expr_in_auto_increment
        && ctx
            .partial_update
            .auto_increment
            .auto_increment_column_idx
            .is_none()
        && ctx
            .partial_update
            .auto_increment
            .auto_increment_column_name
            .is_none()
        && !ctx.partial_update.auto_increment.auto_increment_in_sort_key
        && ctx.partial_update.auto_increment.fe_addr.is_none()
        && ctx
            .partial_update
            .auto_increment
            .frontend_provider
            .is_none()
}

fn append_full_upsert_through_storage_provider(
    ctx: &TabletWriteContext,
    batch: &RecordBatch,
    txn_id: i64,
    driver_id: i32,
    file_seq: u64,
    write_format: StarRocksWriteFormat,
    partition_id: i64,
    load_id: Option<&UniqueId>,
    begin_time_ms: i64,
) -> Result<(), String> {
    let provider = ctx.storage_metadata_provider.as_deref().ok_or_else(|| {
        "StarRocks lake transaction writes require a storage metadata provider".to_string()
    })?;
    register_tablet_runtime(ctx)?;
    let schema_key = build_storage_table_schema_key(ctx)?;
    let legacy_log_path = txn_log_file_path(&ctx.tablet_root_path, ctx.tablet_id, txn_id)?;
    let primary_log_path = if let Some(load_id) = load_id {
        txn_log_file_path_with_load_id(&ctx.tablet_root_path, ctx.tablet_id, txn_id, load_id)?
    } else {
        legacy_log_path.clone()
    };
    with_txn_log_append_lock(ctx.tablet_id, txn_id, || {
        let mut txn_log = read_storage_txn_log_if_exists(provider, &primary_log_path)?
            .unwrap_or_else(|| StorageTransactionLog {
                tablet_id: Some(ctx.tablet_id),
                txn_id: Some(txn_id),
                write: Some(StorageWriteOperation {
                    schema_key: Some(schema_key.clone()),
                    ..StorageWriteOperation::default()
                }),
                partition_id: Some(partition_id),
                load_id: load_id.map(|id| (id.high(), id.low())),
                ..StorageTransactionLog::default()
            });
        let data_file_name = build_txn_upsert_data_file_name(
            ctx,
            txn_id,
            driver_id,
            file_seq,
            write_format,
            load_id,
        )?;
        if storage_txn_log_contains_data_segment(&txn_log, &data_file_name) {
            return Ok(());
        }
        let incoming_rowset = build_rowset_facts_for_upsert_batch(
            ctx,
            batch,
            txn_id,
            driver_id,
            file_seq,
            write_format,
            load_id,
        )?;
        upsert_storage_write_rowset_in_txn_log(
            &mut txn_log,
            ctx.tablet_id,
            txn_id,
            partition_id,
            &incoming_rowset,
            &[],
            load_id,
            &schema_key,
        )?;
        write_storage_txn_log(provider, &primary_log_path, &txn_log)?;

        if load_id.is_some() && legacy_log_path != primary_log_path {
            let mut legacy_log = read_storage_txn_log_if_exists(provider, &legacy_log_path)?
                .unwrap_or_else(|| StorageTransactionLog {
                    tablet_id: Some(ctx.tablet_id),
                    txn_id: Some(txn_id),
                    write: Some(StorageWriteOperation {
                        schema_key: Some(schema_key.clone()),
                        ..StorageWriteOperation::default()
                    }),
                    partition_id: Some(partition_id),
                    ..StorageTransactionLog::default()
                });
            upsert_storage_write_rowset_in_txn_log(
                &mut legacy_log,
                ctx.tablet_id,
                txn_id,
                partition_id,
                &incoming_rowset,
                &[],
                None,
                &schema_key,
            )?;
            write_storage_txn_log(provider, &legacy_log_path, &legacy_log)?;
        }
        record_storage_write_load_facts(
            ctx,
            batch,
            txn_id,
            partition_id,
            load_id,
            begin_time_ms,
            txn_log.write.as_ref(),
        );
        Ok(())
    })
}

fn build_txn_upsert_data_file_name(
    ctx: &TabletWriteContext,
    txn_id: i64,
    driver_id: i32,
    file_seq: u64,
    write_format: StarRocksWriteFormat,
    load_id: Option<&UniqueId>,
) -> Result<String, String> {
    let write_format = resolve_batch_write_format(write_format, &ctx.tablet_schema)?;
    build_txn_data_file_name(
        ctx.tablet_id,
        txn_id,
        driver_id,
        file_seq,
        write_format,
        load_id,
    )
}

fn read_storage_txn_log_if_exists(
    provider: &dyn crate::connector::starrocks::ports::StorageMetadataProvider,
    path: &str,
) -> Result<Option<StorageTransactionLog>, String> {
    read_bytes_if_exists(path)?
        .map(|bytes| provider.decode_transaction_log(&bytes))
        .transpose()
}

fn write_storage_txn_log(
    provider: &dyn crate::connector::starrocks::ports::StorageMetadataProvider,
    path: &str,
    txn_log: &StorageTransactionLog,
) -> Result<(), String> {
    write_bytes(path, provider.encode_transaction_log(txn_log)?)
}

fn storage_txn_log_contains_data_segment(
    txn_log: &StorageTransactionLog,
    data_file_name: &str,
) -> bool {
    txn_log
        .write
        .as_ref()
        .and_then(|write| write.rowset.as_ref())
        .is_some_and(|rowset| {
            rowset
                .segments
                .iter()
                .any(|segment| segment == data_file_name)
        })
}

fn record_storage_write_load_facts(
    ctx: &TabletWriteContext,
    batch: &RecordBatch,
    txn_id: i64,
    partition_id: i64,
    load_id: Option<&UniqueId>,
    begin_time_ms: i64,
    write: Option<&StorageWriteOperation>,
) {
    let backend_id = crate::runtime::backend_id::backend_id().unwrap_or(-1);
    let rowset = write.and_then(|write| write.rowset.as_ref());
    let rowset_id = rowset
        .and_then(|rowset| rowset.id)
        .map(|id| id.to_string())
        .unwrap_or_default();
    let output_rows = rowset
        .and_then(|rowset| rowset.num_rows)
        .unwrap_or(0)
        .max(0);
    let output_bytes = rowset
        .and_then(|rowset| rowset.data_size)
        .unwrap_or(0)
        .max(0);
    let output_segments = rowset
        .map(|rowset| i32::try_from(rowset.segments.len()).unwrap_or(i32::MAX))
        .unwrap_or(0);
    let num_delfile = write
        .map(|write| i64::try_from(write.dels.len()).unwrap_or(i64::MAX))
        .unwrap_or(0);
    let label = load_id.map(|value| crate::common::types::format_uuid(value.high(), value.low()));
    schema::record_tablet_write_load_log(BeTabletWriteLoadLogRecord {
        backend_id,
        begin_time_ms,
        finish_time_ms: unix_millis_now(),
        txn_id,
        tablet_id: ctx.tablet_id,
        table_id: ctx.table_id,
        partition_id,
        input_rows: i64::try_from(batch.num_rows()).unwrap_or(i64::MAX),
        input_bytes: i64::try_from(batch.get_array_memory_size()).unwrap_or(i64::MAX),
        output_rows,
        output_bytes,
        output_segments,
        label,
    });
    schema::record_be_txn_active(BeTxnActiveRecord {
        backend_id,
        load_id_hi: load_id.map(|value| value.high()),
        load_id_lo: load_id.map(|value| value.low()),
        txn_id,
        partition_id,
        tablet_id: ctx.tablet_id,
        rowset_id: Some(rowset_id),
        num_segment: i64::from(output_segments),
        num_delfile,
        num_row: output_rows,
        data_size: output_bytes,
    });
}

fn txn_log_contains_data_segment(txn_log: &StorageTransactionLog, data_file_name: &str) -> bool {
    txn_log
        .write
        .as_ref()
        .and_then(|op_write| op_write.rowset.as_ref())
        .is_some_and(|rowset| {
            rowset
                .segments
                .iter()
                .any(|segment| segment == data_file_name)
        })
}

fn txn_log_contains_del_file(txn_log: &StorageTransactionLog, del_file_name: &str) -> bool {
    txn_log
        .write
        .as_ref()
        .is_some_and(|op_write| op_write.dels.iter().any(|del| del == del_file_name))
}

#[allow(dead_code)]
pub(crate) fn append_lake_txn_log_empty_rowset(
    ctx: &TabletWriteContext,
    txn_id: i64,
    partition_id: i64,
    load_id: Option<&UniqueId>,
) -> Result<(), String> {
    if ctx.table_id <= 0 {
        return Err(format!("invalid table_id for lake write: {}", ctx.table_id));
    }
    if ctx.tablet_id <= 0 {
        return Err(format!(
            "invalid tablet_id for lake write: {}",
            ctx.tablet_id
        ));
    }
    if txn_id <= 0 {
        return Err(format!("invalid txn_id for lake write: {}", txn_id));
    }

    let storage_metadata = ctx.storage_metadata_provider.as_deref().ok_or_else(|| {
        "StarRocks lake transaction writes require a storage metadata provider".to_string()
    })?;

    register_tablet_runtime(ctx)?;
    let schema_key = build_storage_table_schema_key(ctx)?;
    let mut incoming_rowset = StorageRowset {
        id: None,
        overlapped: Some(false),
        segments: Vec::new(),
        num_rows: Some(0),
        data_size: Some(0),
        delete_predicate: None,
        num_dels: Some(0),
        segment_size: Vec::new(),
        max_compact_input_rowset_id: None,
        version: None,
        del_files: Vec::new(),
        segment_encryption_metas: Vec::new(),
        next_compaction_offset: None,
        bundle_file_offsets: Vec::new(),
        shared_segments: Vec::new(),
        record_predicate: None,
        segment_metas: Vec::new(),
    };
    normalize_storage_rowset_shared_segments(&mut incoming_rowset);
    ensure_storage_rowset_segment_meta_consistency(&incoming_rowset)?;

    let legacy_log_path = txn_log_file_path(&ctx.tablet_root_path, ctx.tablet_id, txn_id)?;
    let primary_log_path = if let Some(load_id) = load_id {
        txn_log_file_path_with_load_id(&ctx.tablet_root_path, ctx.tablet_id, txn_id, load_id)?
    } else {
        legacy_log_path.clone()
    };
    with_txn_log_append_lock(ctx.tablet_id, txn_id, || {
        let mut txn_log = match read_storage_txn_log_if_exists(storage_metadata, &primary_log_path)?
        {
            Some(existing) => existing,
            None => StorageTransactionLog {
                tablet_id: Some(ctx.tablet_id),
                txn_id: Some(txn_id),
                write: Some(StorageWriteOperation {
                    rowset: None,
                    txn_meta: None,
                    dels: Vec::new(),
                    rewrite_segments: Vec::new(),
                    del_encryption_metas: Vec::new(),
                    ssts: Vec::new(),
                    schema_key: Some(schema_key.clone()),
                }),
                compaction: None,
                schema_change: None,
                alter_metadata: None,
                replication: None,
                partition_id: Some(partition_id),
                load_id: load_id.map(|id| (id.high(), id.low())),
            },
        };
        upsert_storage_write_rowset_in_txn_log(
            &mut txn_log,
            ctx.tablet_id,
            txn_id,
            partition_id,
            &incoming_rowset,
            &[],
            load_id,
            &schema_key,
        )?;
        write_storage_txn_log(storage_metadata, &primary_log_path, &txn_log)?;

        // Keep writing a legacy plain txn log for compatibility with FE requests
        // that do not carry load_ids.
        if load_id.is_some() && legacy_log_path != primary_log_path {
            let mut legacy_log =
                match read_storage_txn_log_if_exists(storage_metadata, &legacy_log_path)? {
                    Some(existing) => existing,
                    None => StorageTransactionLog {
                        tablet_id: Some(ctx.tablet_id),
                        txn_id: Some(txn_id),
                        write: Some(StorageWriteOperation {
                            rowset: None,
                            txn_meta: None,
                            dels: Vec::new(),
                            rewrite_segments: Vec::new(),
                            del_encryption_metas: Vec::new(),
                            ssts: Vec::new(),
                            schema_key: Some(schema_key.clone()),
                        }),
                        compaction: None,
                        schema_change: None,
                        alter_metadata: None,
                        replication: None,
                        partition_id: Some(partition_id),
                        load_id: None,
                    },
                };
            upsert_storage_write_rowset_in_txn_log(
                &mut legacy_log,
                ctx.tablet_id,
                txn_id,
                partition_id,
                &incoming_rowset,
                &[],
                None,
                &schema_key,
            )?;
            write_storage_txn_log(storage_metadata, &legacy_log_path, &legacy_log)?;
        }
        Ok(())
    })
}

const LOAD_OP_COLUMN: &str = "__op";
const OP_TYPE_UPSERT: i8 = 0;

enum LakeBatchWriteRouting {
    Empty,
    DeleteKeysOnly {
        key_batch: RecordBatch,
    },
    Upsert {
        data_batch: RecordBatch,
    },
    Mixed {
        upsert_batch: RecordBatch,
        delete_key_batch: RecordBatch,
    },
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ResolvedPartialWriteMode {
    Row,
    ColumnUpsert,
    ColumnUpdate,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum OpBatchKind {
    UpsertOnly,
    DeleteOnly,
    Mixed,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct OpBatchSummary {
    kind: OpBatchKind,
    upsert_rows: usize,
    delete_rows: usize,
}

struct ParsedOpBatch {
    summary: OpBatchSummary,
    upsert_row_indexes: Vec<u32>,
    delete_row_indexes: Vec<u32>,
}

fn slot_ids_from_chunk(chunk: &Chunk) -> Vec<Option<SlotId>> {
    chunk
        .chunk_schema()
        .slots()
        .iter()
        .map(|slot| Some(slot.slot_id()))
        .collect::<Vec<_>>()
}

fn debug_batch_fields(batch: &RecordBatch, batch_slot_ids: &[Option<SlotId>]) -> Vec<String> {
    batch
        .schema()
        .fields()
        .iter()
        .enumerate()
        .map(|(idx, field)| {
            let slot = batch_slot_ids
                .get(idx)
                .and_then(|slot| *slot)
                .map(|slot_id| slot_id.to_string())
                .unwrap_or_else(|| "none".to_string());
            format!("{}:{}(slot={})", idx, field.name(), slot)
        })
        .collect::<Vec<_>>()
}

fn resolve_lake_batch_write_routing_with_slots(
    ctx: &TabletWriteContext,
    batch: &RecordBatch,
    batch_slot_ids: Option<&[Option<SlotId>]>,
    existing_txn_log: Option<&StorageTransactionLog>,
) -> Result<LakeBatchWriteRouting, String> {
    let is_primary_keys_table = is_primary_keys_table_for_write(&ctx.tablet_schema)?;
    let parsed_op = parse_op_batch(batch)?;
    if is_primary_keys_table {
        let batch_fields = batch_slot_ids
            .map(|slot_ids| debug_batch_fields(batch, slot_ids))
            .unwrap_or_default();
        let parsed_summary = parsed_op
            .as_ref()
            .map(|parsed| {
                format!(
                    "kind={:?},upsert_rows={},delete_rows={}",
                    parsed.summary.kind, parsed.summary.upsert_rows, parsed.summary.delete_rows
                )
            })
            .unwrap_or_else(|| "none".to_string());
        info!(
            target: "novarocks::sink",
            tablet_id = ctx.tablet_id,
            parsed_op = %parsed_summary,
            batch_fields = ?batch_fields,
            "OLAP_TABLE_SINK resolve lake batch write routing for primary-key table"
        );
    }

    if !is_primary_keys_table {
        if parsed_op.is_some() {
            return Err(format!(
                "non-primary key lake write does not support '{}' control column",
                LOAD_OP_COLUMN
            ));
        }
        let data_batch = materialize_non_primary_auto_increment_batch(ctx, batch)?;
        let full_schema_batch =
            materialize_non_primary_full_schema_batch(ctx, &data_batch, batch_slot_ids)?;
        return Ok(LakeBatchWriteRouting::Upsert {
            data_batch: full_schema_batch,
        });
    }

    let full_schema_col_count = ctx.tablet_schema.column.len();
    let data_batch = match parsed_op.as_ref() {
        Some(_) => strip_last_op_control_column(batch)?,
        None => batch.clone(),
    };
    let data_slot_ids = match parsed_op.as_ref() {
        Some(_) => {
            batch_slot_ids.map(|slot_ids| slot_ids[..slot_ids.len().saturating_sub(1)].to_vec())
        }
        None => batch_slot_ids.map(|slot_ids| slot_ids.to_vec()),
    };
    let mode = resolve_effective_partial_write_mode(ctx, data_batch.num_columns());
    if parsed_op.is_none() && data_batch.num_columns() < full_schema_col_count {
        let schema_to_batch =
            resolve_schema_column_batch_indexes(ctx, &data_batch, data_slot_ids.as_deref())?;
        let referenced_schema_indexes = schema_to_batch
            .iter()
            .enumerate()
            .filter_map(|(schema_idx, batch_idx)| batch_idx.map(|_| schema_idx))
            .collect::<Vec<_>>();
        let all_batch_columns_are_pk_only = referenced_schema_indexes.len()
            == data_batch.num_columns()
            && referenced_schema_indexes.iter().all(|schema_idx| {
                ctx.tablet_schema.column[*schema_idx]
                    .is_key
                    .unwrap_or(false)
            });
        if all_batch_columns_are_pk_only {
            return Err(format!(
                "primary key delete-key batch requires explicit '{}' control column",
                LOAD_OP_COLUMN
            ));
        }
    }
    let has_upsert_rows = parsed_op
        .as_ref()
        .is_none_or(|parsed| parsed.summary.kind != OpBatchKind::DeleteOnly);
    validate_partial_update_sort_key_conflict(
        ctx,
        &data_batch,
        data_slot_ids.as_deref(),
        has_upsert_rows,
    )?;

    let Some(parsed) = parsed_op else {
        return resolve_upsert_batch_for_mode(
            ctx,
            data_batch,
            data_slot_ids.as_deref(),
            mode,
            existing_txn_log,
            full_schema_col_count,
        );
    };

    match parsed.summary.kind {
        OpBatchKind::DeleteOnly => {
            let key_batch =
                project_batch_to_primary_key_columns(ctx, &data_batch, data_slot_ids.as_deref())?;
            let preview = key_batch
                .columns()
                .iter()
                .enumerate()
                .map(|(idx, col)| match col.data_type() {
                    DataType::Int32 => col
                        .as_any()
                        .downcast_ref::<Int32Array>()
                        .map(|arr| {
                            if arr.is_empty() {
                                format!("col{idx}=[]")
                            } else if arr.is_null(0) {
                                format!("col{idx}=[NULL]")
                            } else {
                                format!("col{idx}=[{}]", arr.value(0))
                            }
                        })
                        .unwrap_or_else(|| format!("col{idx}=[int32-downcast-failed]")),
                    DataType::Int64 => col
                        .as_any()
                        .downcast_ref::<Int64Array>()
                        .map(|arr| {
                            if arr.is_empty() {
                                format!("col{idx}=[]")
                            } else if arr.is_null(0) {
                                format!("col{idx}=[NULL]")
                            } else {
                                format!("col{idx}=[{}]", arr.value(0))
                            }
                        })
                        .unwrap_or_else(|| format!("col{idx}=[int64-downcast-failed]")),
                    DataType::Utf8 => col
                        .as_any()
                        .downcast_ref::<StringArray>()
                        .map(|arr| {
                            if arr.is_empty() {
                                format!("col{idx}=[]")
                            } else if arr.is_null(0) {
                                format!("col{idx}=[NULL]")
                            } else {
                                format!("col{idx}=[{}]", arr.value(0))
                            }
                        })
                        .unwrap_or_else(|| format!("col{idx}=[utf8-downcast-failed]")),
                    other => format!("col{idx}=[type={other:?}]"),
                })
                .collect::<Vec<_>>();
            info!(
                target: "novarocks::sink",
                tablet_id = ctx.tablet_id,
                key_rows = key_batch.num_rows(),
                key_preview = ?preview,
                "OLAP_TABLE_SINK delete-only key batch preview"
            );
            Ok(LakeBatchWriteRouting::DeleteKeysOnly { key_batch })
        }
        OpBatchKind::UpsertOnly => resolve_upsert_batch_for_mode(
            ctx,
            data_batch,
            data_slot_ids.as_deref(),
            mode,
            existing_txn_log,
            full_schema_col_count,
        ),
        OpBatchKind::Mixed => {
            let upsert_source = take_batch_rows(&data_batch, &parsed.upsert_row_indexes)?;
            let delete_rows_batch = take_batch_rows(&data_batch, &parsed.delete_row_indexes)?;
            let delete_key_batch = project_batch_to_primary_key_columns(
                ctx,
                &delete_rows_batch,
                data_slot_ids.as_deref(),
            )?;
            let upsert_routing = resolve_upsert_batch_for_mode(
                ctx,
                upsert_source,
                data_slot_ids.as_deref(),
                mode,
                existing_txn_log,
                full_schema_col_count,
            )?;
            match upsert_routing {
                LakeBatchWriteRouting::Upsert { data_batch } => Ok(LakeBatchWriteRouting::Mixed {
                    upsert_batch: data_batch,
                    delete_key_batch,
                }),
                LakeBatchWriteRouting::Empty => Ok(LakeBatchWriteRouting::DeleteKeysOnly {
                    key_batch: delete_key_batch,
                }),
                other => Err(format!(
                    "unexpected upsert routing in mixed op path: {:?}",
                    routing_debug_tag(&other)
                )),
            }
        }
    }
}

fn validate_partial_update_sort_key_conflict(
    ctx: &TabletWriteContext,
    data_batch: &RecordBatch,
    data_batch_slot_ids: Option<&[Option<SlotId>]>,
    has_upsert_rows: bool,
) -> Result<(), String> {
    if !has_upsert_rows {
        return Ok(());
    }
    // Match StarRocks: sort-key conflict checks only matter for partial updates.
    if data_batch.num_columns() >= ctx.tablet_schema.column.len() {
        return Ok(());
    }

    let mut sort_key_idxes = ctx
        .tablet_schema
        .sort_key_idxes
        .iter()
        .filter_map(|idx| usize::try_from(*idx).ok())
        .collect::<Vec<_>>();
    if sort_key_idxes.is_empty() {
        return Ok(());
    }
    sort_key_idxes.sort_unstable();
    sort_key_idxes.dedup();

    let schema_to_batch =
        resolve_schema_column_batch_indexes(ctx, data_batch, data_batch_slot_ids)?;
    let referenced_columns = schema_to_batch
        .iter()
        .enumerate()
        .filter_map(|(schema_idx, batch_idx)| batch_idx.map(|_| schema_idx))
        .collect::<Vec<_>>();
    if referenced_columns.len() >= ctx.tablet_schema.column.len() {
        return Ok(());
    }

    let referenced_set = referenced_columns.iter().copied().collect::<HashSet<_>>();
    let contains_all_sort_keys = sort_key_idxes
        .iter()
        .all(|idx| referenced_set.contains(idx));
    let sort_key_set = sort_key_idxes.iter().copied().collect::<HashSet<_>>();
    let num_key_columns = ctx
        .tablet_schema
        .column
        .iter()
        .filter(|column| column.is_key.unwrap_or(false))
        .count();
    let contains_non_pk_sort_key = referenced_columns
        .iter()
        .any(|idx| *idx >= num_key_columns && sort_key_set.contains(idx));

    let mode = ctx.partial_update.mode.clone();
    let mut conflict = false;
    if matches!(
        mode,
        PartialUpdateWriteMode::Row
            | PartialUpdateWriteMode::Auto
            | PartialUpdateWriteMode::Unknown
            | PartialUpdateWriteMode::ColumnUpsert
    ) && !contains_all_sort_keys
    {
        conflict = true;
    }
    if matches!(
        mode,
        PartialUpdateWriteMode::ColumnUpdate | PartialUpdateWriteMode::ColumnUpsert
    ) && contains_non_pk_sort_key
    {
        conflict = true;
    }
    if !conflict {
        return Ok(());
    }

    if matches!(mode, PartialUpdateWriteMode::ColumnUpdate) {
        return Err(
            "column mode partial update on table with sort key cannot update sort key column"
                .to_string(),
        );
    }
    Err("partial update on table with sort key must provide all sort key columns".to_string())
}

fn resolve_effective_partial_write_mode(
    ctx: &TabletWriteContext,
    input_columns: usize,
) -> ResolvedPartialWriteMode {
    // Full-schema writes should always behave as row-mode upserts, even if FE
    // sends a column-mode hint.
    if input_columns >= ctx.tablet_schema.column.len() {
        return ResolvedPartialWriteMode::Row;
    }

    match ctx.partial_update.mode {
        PartialUpdateWriteMode::Row => ResolvedPartialWriteMode::Row,
        PartialUpdateWriteMode::ColumnUpsert => ResolvedPartialWriteMode::ColumnUpsert,
        PartialUpdateWriteMode::ColumnUpdate => ResolvedPartialWriteMode::ColumnUpdate,
        PartialUpdateWriteMode::Auto | PartialUpdateWriteMode::Unknown => {
            ResolvedPartialWriteMode::ColumnUpsert
        }
    }
}

fn resolve_upsert_batch_for_mode(
    ctx: &TabletWriteContext,
    data_batch: RecordBatch,
    data_batch_slot_ids: Option<&[Option<SlotId>]>,
    mode: ResolvedPartialWriteMode,
    existing_txn_log: Option<&StorageTransactionLog>,
    full_schema_col_count: usize,
) -> Result<LakeBatchWriteRouting, String> {
    let data_batch =
        materialize_present_auto_increment_column(ctx, &data_batch, data_batch_slot_ids)?;
    if ctx.partial_update.merge_condition.is_some() {
        info!(
            target: "novarocks::sink",
            table_id = ctx.table_id,
            tablet_id = ctx.tablet_id,
            mode = ?mode,
            input_columns = data_batch.num_columns(),
            full_schema_columns = full_schema_col_count,
            input_rows = data_batch.num_rows(),
            "OLAP_TABLE_SINK resolving upsert batch with merge condition"
        );
    }
    match mode {
        ResolvedPartialWriteMode::Row => {
            if data_batch.num_columns() == full_schema_col_count {
                // When miss_auto_increment_column is true, the batch has all columns
                // but the auto-increment column has placeholder 0 values.  We must go
                // through the partial upsert path so existing auto-increment values are
                // preserved for existing rows and new IDs are allocated for new rows.
                if ctx.partial_update.merge_condition.is_none()
                    && !ctx.partial_update.auto_increment.miss_auto_increment_column
                {
                    return Ok(LakeBatchWriteRouting::Upsert { data_batch });
                }
                let materialized = materialize_partial_upsert_batch(
                    ctx,
                    &data_batch,
                    data_batch_slot_ids,
                    mode,
                    existing_txn_log,
                )?;
                info!(
                    target: "novarocks::sink",
                    table_id = ctx.table_id,
                    tablet_id = ctx.tablet_id,
                    mode = ?mode,
                    input_rows = data_batch.num_rows(),
                    materialized_rows = materialized.num_rows(),
                    "OLAP_TABLE_SINK materialized merge-condition batch in row mode"
                );
                if materialized.num_rows() == 0 {
                    return Ok(LakeBatchWriteRouting::Empty);
                }
                return Ok(LakeBatchWriteRouting::Upsert {
                    data_batch: materialized,
                });
            }
            if data_batch.num_columns() > full_schema_col_count {
                return Err(format!(
                    "primary key row-mode write has too many columns: data_columns={} schema_columns={}",
                    data_batch.num_columns(),
                    full_schema_col_count
                ));
            }
            let materialized = materialize_partial_upsert_batch(
                ctx,
                &data_batch,
                data_batch_slot_ids,
                mode,
                existing_txn_log,
            )?;
            if materialized.num_rows() == 0 {
                Ok(LakeBatchWriteRouting::Empty)
            } else {
                Ok(LakeBatchWriteRouting::Upsert {
                    data_batch: materialized,
                })
            }
        }
        ResolvedPartialWriteMode::ColumnUpsert | ResolvedPartialWriteMode::ColumnUpdate => {
            let materialized = materialize_partial_upsert_batch(
                ctx,
                &data_batch,
                data_batch_slot_ids,
                mode,
                existing_txn_log,
            )?;
            if ctx.partial_update.merge_condition.is_some() {
                info!(
                    target: "novarocks::sink",
                    table_id = ctx.table_id,
                    tablet_id = ctx.tablet_id,
                    mode = ?mode,
                    input_rows = data_batch.num_rows(),
                    materialized_rows = materialized.num_rows(),
                    "OLAP_TABLE_SINK materialized merge-condition batch in column mode"
                );
            }
            if materialized.num_rows() == 0 {
                Ok(LakeBatchWriteRouting::Empty)
            } else {
                Ok(LakeBatchWriteRouting::Upsert {
                    data_batch: materialized,
                })
            }
        }
    }
}

fn materialize_non_primary_auto_increment_batch(
    ctx: &TabletWriteContext,
    batch: &RecordBatch,
) -> Result<RecordBatch, String> {
    let auto_policy = &ctx.partial_update.auto_increment;
    let Some(auto_col_idx) = auto_policy.auto_increment_column_idx else {
        return Ok(batch.clone());
    };
    if auto_col_idx >= batch.num_columns() {
        return Err(format!(
            "non-primary key write auto_increment column index out of range: index={} input_columns={}",
            auto_col_idx,
            batch.num_columns()
        ));
    }

    let auto_col_name = auto_policy
        .auto_increment_column_name
        .as_deref()
        .filter(|name| !name.trim().is_empty())
        .unwrap_or("<auto_increment>");
    let auto_col = batch.column(auto_col_idx);
    let null_rows = auto_col.null_count();
    if null_rows == 0 {
        return Ok(batch.clone());
    }
    if auto_policy.null_expr_in_auto_increment {
        return Err(format!(
            "NULL value in auto increment column '{}'",
            auto_col_name
        ));
    }

    let fe_addr = auto_policy.fe_addr.as_ref().ok_or_else(|| {
        "non-primary key write cannot allocate auto_increment id without FE address".to_string()
    })?;
    let allocated_ids = allocate_auto_increment_ids(
        auto_policy.frontend_provider.as_deref(),
        fe_addr,
        ctx.table_id,
        null_rows,
    )?;
    let filled_auto_col =
        fill_auto_increment_column_nulls(auto_col.as_ref(), &allocated_ids, auto_col_name)?;

    let mut columns = batch.columns().to_vec();
    columns[auto_col_idx] = filled_auto_col;
    RecordBatch::try_new(batch.schema(), columns).map_err(|e| {
        format!(
            "build non-primary key batch after auto_increment materialization failed: {}",
            e
        )
    })
}

fn materialize_present_auto_increment_column(
    ctx: &TabletWriteContext,
    batch: &RecordBatch,
    batch_slot_ids: Option<&[Option<SlotId>]>,
) -> Result<RecordBatch, String> {
    let auto_policy = &ctx.partial_update.auto_increment;
    let Some(auto_col_idx) = auto_policy.auto_increment_column_idx else {
        return Ok(batch.clone());
    };
    // When miss_auto_increment_column is true, the partial upsert path handles
    // allocation.  Do not allocate here to avoid double allocation.
    if auto_policy.miss_auto_increment_column {
        return Ok(batch.clone());
    }
    let schema_to_batch = resolve_schema_column_batch_indexes(ctx, batch, batch_slot_ids)?;
    let Some(batch_idx) = schema_to_batch.get(auto_col_idx).and_then(|idx| *idx) else {
        return Ok(batch.clone());
    };

    let auto_col_name = auto_policy
        .auto_increment_column_name
        .as_deref()
        .filter(|name| !name.trim().is_empty())
        .or_else(|| {
            ctx.tablet_schema
                .column
                .get(auto_col_idx)
                .and_then(|column| column.name.as_deref())
                .filter(|name| !name.trim().is_empty())
        })
        .unwrap_or("<auto_increment>");
    let auto_col = batch.column(batch_idx);
    let null_rows = auto_col.null_count();
    if null_rows == 0 {
        return Ok(batch.clone());
    }
    if auto_policy.null_expr_in_auto_increment {
        return Err(format!(
            "NULL value in auto increment column '{}'",
            auto_col_name
        ));
    }

    let fe_addr = auto_policy
        .fe_addr
        .as_ref()
        .ok_or_else(|| "write cannot allocate auto_increment id without FE address".to_string())?;
    let allocated_ids = allocate_auto_increment_ids(
        auto_policy.frontend_provider.as_deref(),
        fe_addr,
        ctx.table_id,
        null_rows,
    )?;
    let filled_auto_col =
        fill_auto_increment_column_nulls(auto_col.as_ref(), &allocated_ids, auto_col_name)?;

    let mut columns = batch.columns().to_vec();
    columns[batch_idx] = filled_auto_col;
    RecordBatch::try_new(batch.schema(), columns).map_err(|e| {
        format!(
            "build write batch after auto_increment materialization failed: {}",
            e
        )
    })
}

fn materialize_non_primary_full_schema_batch(
    ctx: &TabletWriteContext,
    batch: &RecordBatch,
    batch_slot_ids: Option<&[Option<SlotId>]>,
) -> Result<RecordBatch, String> {
    let schema_col_count = ctx.tablet_schema.column.len();
    if schema_col_count == 0 {
        return Ok(batch.clone());
    }
    if batch.num_columns() < schema_col_count {
        return Err(format!(
            "non-primary key write requires full schema columns: input_columns={} schema_columns={}",
            batch.num_columns(),
            schema_col_count
        ));
    }
    if batch.num_columns() == schema_col_count {
        // StarRocks treats full-schema duplicate-key writes as positional writes.
        // The sink side has already aligned full target columns in schema order.
        return Ok(batch.clone());
    }

    let schema_to_batch = resolve_schema_column_batch_indexes(ctx, batch, batch_slot_ids)?;
    let mut projected_batch_indexes = Vec::with_capacity(schema_col_count);
    let mut used_batch_indexes = HashSet::with_capacity(schema_col_count);
    for (schema_idx, schema_col) in ctx.tablet_schema.column.iter().enumerate() {
        let Some(batch_idx) = schema_to_batch.get(schema_idx).and_then(|v| *v) else {
            let schema_name = schema_col.name.as_deref().unwrap_or("<unnamed>");
            return Err(format!(
                "non-primary key write missing schema column in input batch: tablet_id={} schema_index={} schema_column='{}' input_columns={}",
                ctx.tablet_id,
                schema_idx,
                schema_name,
                batch.num_columns()
            ));
        };
        if !used_batch_indexes.insert(batch_idx) {
            let schema_name = schema_col.name.as_deref().unwrap_or("<unnamed>");
            return Err(format!(
                "non-primary key write has duplicate source column mapping: tablet_id={} schema_index={} schema_column='{}' batch_index={}",
                ctx.tablet_id, schema_idx, schema_name, batch_idx
            ));
        }
        projected_batch_indexes.push(batch_idx);
    }
    project_batch_by_columns(
        batch,
        &projected_batch_indexes,
        "non-primary key full-schema write",
    )
}

fn fill_auto_increment_column_nulls(
    column: &dyn Array,
    allocated_ids: &[i64],
    column_name: &str,
) -> Result<ArrayRef, String> {
    let mut auto_pos = 0usize;
    let filled: ArrayRef = match column.data_type() {
        DataType::Int64 => {
            let typed = column
                .as_any()
                .downcast_ref::<Int64Array>()
                .ok_or_else(|| "downcast Int64Array failed".to_string())?;
            let mut values = Vec::with_capacity(typed.len());
            for row_idx in 0..typed.len() {
                if typed.is_null(row_idx) {
                    let auto_id = *allocated_ids.get(auto_pos).ok_or_else(|| {
                        "allocate_auto_increment_ids returned fewer ids than requested".to_string()
                    })?;
                    auto_pos = auto_pos.saturating_add(1);
                    values.push(auto_id);
                } else {
                    values.push(typed.value(row_idx));
                }
            }
            Arc::new(Int64Array::from(values))
        }
        DataType::Int32 => {
            let typed = column
                .as_any()
                .downcast_ref::<Int32Array>()
                .ok_or_else(|| "downcast Int32Array failed".to_string())?;
            let mut values = Vec::with_capacity(typed.len());
            for row_idx in 0..typed.len() {
                if typed.is_null(row_idx) {
                    let auto_id = *allocated_ids.get(auto_pos).ok_or_else(|| {
                        "allocate_auto_increment_ids returned fewer ids than requested".to_string()
                    })?;
                    let casted = i32::try_from(auto_id).map_err(|_| {
                        format!(
                            "auto_increment value overflow for INT column '{}'",
                            column_name
                        )
                    })?;
                    auto_pos = auto_pos.saturating_add(1);
                    values.push(casted);
                } else {
                    values.push(typed.value(row_idx));
                }
            }
            Arc::new(Int32Array::from(values))
        }
        other => {
            return Err(format!(
                "unsupported auto_increment column type in non-primary key write: column='{}' type={:?}",
                column_name, other
            ));
        }
    };

    if auto_pos != allocated_ids.len() {
        return Err(format!(
            "allocate_auto_increment_ids returned unexpected id count: expected={} actual={}",
            auto_pos,
            allocated_ids.len()
        ));
    }
    Ok(filled)
}

fn routing_debug_tag(routing: &LakeBatchWriteRouting) -> &'static str {
    match routing {
        LakeBatchWriteRouting::Empty => "empty",
        LakeBatchWriteRouting::DeleteKeysOnly { .. } => "delete_keys_only",
        LakeBatchWriteRouting::Upsert { .. } => "upsert",
        LakeBatchWriteRouting::Mixed { .. } => "mixed",
    }
}

fn is_primary_keys_table_for_write(tablet_schema: &StarRocksTabletSchema) -> Result<bool, String> {
    let keys_type = tablet_schema
        .keys_type
        .ok_or_else(|| "tablet schema missing keys_type for lake write".to_string())?;
    Ok(keys_type == StarRocksKeysType::Primary)
}

fn parse_op_batch(batch: &RecordBatch) -> Result<Option<ParsedOpBatch>, String> {
    let op_indexes = batch
        .schema()
        .fields()
        .iter()
        .enumerate()
        .filter_map(|(idx, field)| (field.name() == LOAD_OP_COLUMN).then_some(idx))
        .collect::<Vec<_>>();
    if op_indexes.is_empty() {
        return Ok(None);
    }
    if op_indexes.len() > 1 {
        return Err(format!(
            "write batch contains duplicated '{}' columns at indexes {:?}",
            LOAD_OP_COLUMN, op_indexes
        ));
    }
    let op_index = op_indexes[0];
    if op_index + 1 != batch.num_columns() {
        return Err(format!(
            "write batch requires '{}' to be the last column: op_index={} num_columns={}",
            LOAD_OP_COLUMN,
            op_index,
            batch.num_columns()
        ));
    }

    let mut upsert_row_indexes = Vec::new();
    let mut delete_row_indexes = Vec::new();
    let mut upsert_rows = 0usize;
    let mut delete_rows = 0usize;
    let op_array = batch.column(op_index);
    let mut classify_row = |row_idx: usize, is_upsert: bool| -> Result<(), String> {
        let row_u32 = u32::try_from(row_idx).map_err(|_| {
            format!(
                "row index overflow while parsing '{}': {}",
                LOAD_OP_COLUMN, row_idx
            )
        })?;
        if is_upsert {
            upsert_rows = upsert_rows.saturating_add(1);
            upsert_row_indexes.push(row_u32);
        } else {
            delete_rows = delete_rows.saturating_add(1);
            delete_row_indexes.push(row_u32);
        }
        Ok(())
    };

    // Keep StarRocks-compatible behavior:
    // __op == 0 -> UPSERT, any non-zero value -> DELETE.
    match op_array.data_type() {
        DataType::Int8 => {
            let typed = op_array
                .as_any()
                .downcast_ref::<Int8Array>()
                .ok_or_else(|| {
                    format!("downcast '{}' column to Int8Array failed", LOAD_OP_COLUMN)
                })?;
            for row_idx in 0..typed.len() {
                if typed.is_null(row_idx) {
                    return Err(format!(
                        "write batch '{}' column contains NULL at row_idx={}",
                        LOAD_OP_COLUMN, row_idx
                    ));
                }
                classify_row(row_idx, typed.value(row_idx) == OP_TYPE_UPSERT)?;
            }
        }
        DataType::Int32 => {
            let typed = op_array
                .as_any()
                .downcast_ref::<Int32Array>()
                .ok_or_else(|| {
                    format!("downcast '{}' column to Int32Array failed", LOAD_OP_COLUMN)
                })?;
            for row_idx in 0..typed.len() {
                if typed.is_null(row_idx) {
                    return Err(format!(
                        "write batch '{}' column contains NULL at row_idx={}",
                        LOAD_OP_COLUMN, row_idx
                    ));
                }
                classify_row(row_idx, typed.value(row_idx) == i32::from(OP_TYPE_UPSERT))?;
            }
        }
        other => {
            return Err(format!(
                "write batch '{}' column type mismatch: expected Int8(TINYINT) or Int32(INT), got={:?}",
                LOAD_OP_COLUMN, other
            ));
        }
    }

    let kind = if upsert_rows > 0 && delete_rows > 0 {
        OpBatchKind::Mixed
    } else if delete_rows > 0 {
        OpBatchKind::DeleteOnly
    } else {
        OpBatchKind::UpsertOnly
    };
    Ok(Some(ParsedOpBatch {
        summary: OpBatchSummary {
            kind,
            upsert_rows,
            delete_rows,
        },
        upsert_row_indexes,
        delete_row_indexes,
    }))
}

fn strip_last_op_control_column(batch: &RecordBatch) -> Result<RecordBatch, String> {
    if batch.num_columns() == 0 {
        return Err(format!(
            "cannot remove '{}' from empty write batch",
            LOAD_OP_COLUMN
        ));
    }
    let schema = batch.schema();
    let op_index = batch.num_columns() - 1;
    let op_field = schema.fields().get(op_index).ok_or_else(|| {
        format!(
            "missing '{}' field at expected index {}",
            LOAD_OP_COLUMN, op_index
        )
    })?;
    if op_field.name() != LOAD_OP_COLUMN {
        return Err(format!(
            "expected '{}' at index {}, got '{}'",
            LOAD_OP_COLUMN,
            op_index,
            op_field.name()
        ));
    }

    let projected_columns = batch
        .columns()
        .iter()
        .take(op_index)
        .cloned()
        .collect::<Vec<_>>();
    let projected_fields = schema
        .fields()
        .iter()
        .take(op_index)
        .cloned()
        .collect::<Vec<_>>();
    let projected_schema = std::sync::Arc::new(arrow::datatypes::Schema::new(projected_fields));
    RecordBatch::try_new(projected_schema, projected_columns).map_err(|e| {
        format!(
            "build write batch without '{}' failed: {}",
            LOAD_OP_COLUMN, e
        )
    })
}

fn take_batch_rows(batch: &RecordBatch, row_indexes: &[u32]) -> Result<RecordBatch, String> {
    if row_indexes.len() == batch.num_rows() {
        return Ok(batch.clone());
    }
    let index_array = UInt32Array::from(row_indexes.to_vec());
    let mut projected_columns = Vec::with_capacity(batch.num_columns());
    for (col_idx, array) in batch.columns().iter().enumerate() {
        let taken = take(array.as_ref(), &index_array, None).map_err(|e| {
            format!(
                "take rows from batch failed: column_index={} selected_rows={} error={}",
                col_idx,
                row_indexes.len(),
                e
            )
        })?;
        projected_columns.push(taken);
    }
    RecordBatch::try_new(batch.schema(), projected_columns).map_err(|e| {
        format!(
            "build batch after row selection failed: selected_rows={} error={}",
            row_indexes.len(),
            e
        )
    })
}

fn project_batch_to_primary_key_columns(
    ctx: &TabletWriteContext,
    batch: &RecordBatch,
    batch_slot_ids: Option<&[Option<SlotId>]>,
) -> Result<RecordBatch, String> {
    let schema_to_batch = resolve_schema_column_batch_indexes(ctx, batch, batch_slot_ids)?;
    let key_schema_indexes = primary_key_schema_indexes(&ctx.tablet_schema)?;
    if key_schema_indexes.is_empty() {
        return Err(format!(
            "invalid tablet schema for delete-key routing: no key columns (tablet_id={})",
            ctx.tablet_id
        ));
    }
    let mut key_batch_indexes = Vec::with_capacity(key_schema_indexes.len());
    for key_schema_idx in key_schema_indexes {
        let key_batch_idx = schema_to_batch
            .get(key_schema_idx)
            .and_then(|idx| *idx)
            .ok_or_else(|| {
                let key_name = ctx.tablet_schema.column[key_schema_idx]
                    .name
                    .clone()
                    .unwrap_or_else(|| format!("<key_{key_schema_idx}>"));
                let available_fields = batch_slot_ids
                    .map(|slot_ids| debug_batch_fields(batch, slot_ids).join(", "))
                    .unwrap_or_default();
                let key_slot_binding = ctx
                    .partial_update
                    .schema_slot_bindings
                    .get(key_schema_idx)
                    .and_then(|slot| *slot)
                    .map(|slot| slot.to_string())
                    .unwrap_or_else(|| "none".to_string());
                format!(
                    "delete-key batch missing primary key column '{}' (schema_index={} tablet_id={} key_slot_binding={} available_fields=[{}])",
                    key_name, key_schema_idx, ctx.tablet_id, key_slot_binding, available_fields
                )
            })?;
        key_batch_indexes.push(key_batch_idx);
    }
    project_batch_by_columns(batch, &key_batch_indexes, "primary key delete-key")
}

fn project_batch_by_columns(
    batch: &RecordBatch,
    column_indexes: &[usize],
    context: &str,
) -> Result<RecordBatch, String> {
    if column_indexes.is_empty() {
        return Err(format!(
            "cannot project batch columns for {}: column index list is empty",
            context
        ));
    }
    let schema = batch.schema();
    let mut projected_columns = Vec::with_capacity(column_indexes.len());
    let mut projected_fields = Vec::with_capacity(column_indexes.len());
    for &col_idx in column_indexes {
        let array = batch.columns().get(col_idx).cloned().ok_or_else(|| {
            format!(
                "project batch columns for {} failed: column index {} out of range (num_columns={})",
                context,
                col_idx,
                batch.num_columns()
            )
        })?;
        let field = schema.fields().get(col_idx).cloned().ok_or_else(|| {
            format!(
                "project batch fields for {} failed: field index {} out of range (num_fields={})",
                context,
                col_idx,
                schema.fields().len()
            )
        })?;
        projected_columns.push(array);
        projected_fields.push(field);
    }
    let projected_schema = Arc::new(Schema::new(projected_fields));
    RecordBatch::try_new(projected_schema, projected_columns).map_err(|e| {
        format!(
            "build projected batch for {} failed: selected_columns={} error={}",
            context,
            column_indexes.len(),
            e
        )
    })
}

type MaterializedRow = Vec<ArrayRef>;
type VisibleRowMap = HashMap<Vec<u8>, MaterializedRow>;

fn primary_key_schema_indexes(tablet_schema: &StarRocksTabletSchema) -> Result<Vec<usize>, String> {
    let key_indexes = tablet_schema
        .column
        .iter()
        .enumerate()
        .filter_map(|(idx, col)| col.is_key.unwrap_or(false).then_some(idx))
        .collect::<Vec<_>>();
    if key_indexes.is_empty() {
        return Err("tablet schema has no primary key columns for partial update".to_string());
    }
    Ok(key_indexes)
}

fn resolve_schema_column_batch_indexes(
    ctx: &TabletWriteContext,
    batch: &RecordBatch,
    batch_slot_ids: Option<&[Option<SlotId>]>,
) -> Result<Vec<Option<usize>>, String> {
    let mut name_to_index = HashMap::new();
    for (idx, field) in batch.schema().fields().iter().enumerate() {
        let normalized = normalize_identifier(field.name());
        if !normalized.is_empty() {
            name_to_index.entry(normalized).or_insert(idx);
        }
    }
    let mut slot_to_index = HashMap::new();
    if let Some(slot_ids) = batch_slot_ids {
        if slot_ids.len() != batch.num_columns() {
            return Err(format!(
                "schema slot id length mismatch: batch_columns={} slot_ids={}",
                batch.num_columns(),
                slot_ids.len()
            ));
        }
        for (idx, slot_id) in slot_ids.iter().enumerate() {
            if let Some(slot_id) = slot_id {
                slot_to_index.entry(*slot_id).or_insert(idx);
            }
        }
    }

    let mut schema_to_batch = Vec::with_capacity(ctx.tablet_schema.column.len());
    for (schema_idx, schema_col) in ctx.tablet_schema.column.iter().enumerate() {
        let by_slot = ctx
            .partial_update
            .schema_slot_bindings
            .get(schema_idx)
            .and_then(|slot| *slot)
            .and_then(|slot_id| slot_to_index.get(&slot_id).copied());
        let by_name = schema_col
            .name
            .as_deref()
            .map(normalize_identifier)
            .and_then(|name| name_to_index.get(&name).copied());
        schema_to_batch.push(by_slot.or(by_name));
    }
    Ok(schema_to_batch)
}

fn normalize_identifier(raw: &str) -> String {
    raw.trim()
        .trim_matches('`')
        .trim_matches('"')
        .to_ascii_lowercase()
}

pub(crate) fn build_tablet_output_schema(
    tablet_schema: &StarRocksTabletSchema,
) -> Result<SchemaRef, String> {
    let mut fields = Vec::with_capacity(tablet_schema.column.len());
    for (idx, column) in tablet_schema.column.iter().enumerate() {
        let name = column
            .name
            .as_deref()
            .map(str::trim)
            .filter(|v| !v.is_empty())
            .map(ToString::to_string)
            .unwrap_or_else(|| format!("c{idx}"));
        let data_type = resolve_tablet_column_arrow_type(column)?;
        fields.push(Field::new(
            name,
            data_type,
            column.is_nullable.unwrap_or(true),
        ));
    }
    Ok(Arc::new(Schema::new(fields)))
}

fn resolve_tablet_column_arrow_type(column: &StarRocksColumnSchema) -> Result<DataType, String> {
    let type_name = column.r#type.trim().to_ascii_uppercase();
    let base = type_name
        .split('(')
        .next()
        .unwrap_or(type_name.as_str())
        .trim();
    match base {
        "BOOLEAN" => Ok(DataType::Boolean),
        "TINYINT" => Ok(DataType::Int8),
        "SMALLINT" => Ok(DataType::Int16),
        "INT" => Ok(DataType::Int32),
        "BIGINT" => Ok(DataType::Int64),
        "FLOAT" => Ok(DataType::Float32),
        "DOUBLE" => Ok(DataType::Float64),
        "DATE" | "DATE_V2" => Ok(DataType::Date32),
        "DATETIME" | "DATETIME_V2" | "TIMESTAMP" => {
            Ok(DataType::Timestamp(TimeUnit::Microsecond, None))
        }
        "CHAR" | "VARCHAR" | "STRING" => Ok(DataType::Utf8),
        "BINARY" | "VARBINARY" => Ok(DataType::Binary),
        // StarRocks stores BITMAP/OBJECT/HLL/PERCENTILE in binary payloads.
        // Partial-update materialization must allow these columns instead of
        // failing fast when FE chooses AUTO/COLUMN_* paths.
        "OBJECT" | "BITMAP" | "HLL" | "PERCENTILE" | "JSON" | "VARIANT" => Ok(DataType::Binary),
        "LARGEINT" => Ok(DataType::FixedSizeBinary(
            novarocks_types::largeint::LARGEINT_BYTE_WIDTH,
        )),
        "DECIMAL" | "DECIMALV2" | "DECIMAL32" | "DECIMAL64" | "DECIMAL128" => {
            let (precision, scale) = resolve_decimal_precision_scale(column)?;
            Ok(DataType::Decimal128(precision, scale))
        }
        "ARRAY" => {
            let item_col = column.children_columns.first().ok_or_else(|| {
                format!(
                    "ARRAY column '{}' has no children in tablet schema",
                    column.name.as_deref().unwrap_or("<unnamed>")
                )
            })?;
            let item_type = resolve_tablet_column_arrow_type(item_col)?;
            let item_nullable = item_col.is_nullable.unwrap_or(true);
            Ok(DataType::List(Arc::new(Field::new(
                "item",
                item_type,
                item_nullable,
            ))))
        }
        "MAP" => {
            let key_col = column.children_columns.first().ok_or_else(|| {
                format!(
                    "MAP column '{}' has no key child in tablet schema",
                    column.name.as_deref().unwrap_or("<unnamed>")
                )
            })?;
            let val_col = column.children_columns.get(1).ok_or_else(|| {
                format!(
                    "MAP column '{}' has no value child in tablet schema",
                    column.name.as_deref().unwrap_or("<unnamed>")
                )
            })?;
            let key_type = resolve_tablet_column_arrow_type(key_col)?;
            let val_type = resolve_tablet_column_arrow_type(val_col)?;
            let key_nullable = key_col.is_nullable.unwrap_or(false);
            let val_nullable = val_col.is_nullable.unwrap_or(true);
            let entries_field = Field::new(
                "entries",
                DataType::Struct(arrow::datatypes::Fields::from(vec![
                    Field::new("key", key_type, key_nullable),
                    Field::new("value", val_type, val_nullable),
                ])),
                false,
            );
            Ok(DataType::Map(Arc::new(entries_field), false))
        }
        "STRUCT" => {
            let mut fields = Vec::with_capacity(column.children_columns.len());
            for child in &column.children_columns {
                let child_name = child.name.as_deref().unwrap_or("field").to_string();
                let child_type = resolve_tablet_column_arrow_type(child)?;
                let child_nullable = child.is_nullable.unwrap_or(true);
                fields.push(Field::new(child_name, child_type, child_nullable));
            }
            Ok(DataType::Struct(arrow::datatypes::Fields::from(fields)))
        }
        other => Err(format!(
            "unsupported column type for partial update materialization: column='{}' type='{}'",
            column
                .name
                .clone()
                .unwrap_or_else(|| "<unnamed>".to_string()),
            other
        )),
    }
}

fn resolve_decimal_precision_scale(column: &StarRocksColumnSchema) -> Result<(u8, i8), String> {
    let base_type = column
        .r#type
        .trim()
        .split('(')
        .next()
        .unwrap_or(column.r#type.as_str())
        .trim();
    if base_type.eq_ignore_ascii_case("DECIMALV2") {
        return Ok((LEGACY_DECIMALV2_PRECISION, LEGACY_DECIMALV2_SCALE));
    }
    if let (Some(precision), Some(scale)) = (column.precision, column.frac) {
        let precision_u8 = u8::try_from(precision)
            .map_err(|_| format!("invalid decimal precision in tablet schema: {}", precision))?;
        let scale_i8 = i8::try_from(scale)
            .map_err(|_| format!("invalid decimal scale in tablet schema: {}", scale))?;
        return Ok((precision_u8, scale_i8));
    }
    let type_name = column.r#type.trim();
    if let Some(start) = type_name.find('(')
        && let Some(end_rel) = type_name[start + 1..].find(')')
    {
        let end = start + 1 + end_rel;
        let args = type_name[start + 1..end].split(',').collect::<Vec<_>>();
        if args.len() == 2 {
            let precision = args[0].trim().parse::<u8>().map_err(|e| {
                format!(
                    "parse decimal precision from type '{}' failed: {}",
                    type_name, e
                )
            })?;
            let scale = args[1].trim().parse::<i8>().map_err(|e| {
                format!(
                    "parse decimal scale from type '{}' failed: {}",
                    type_name, e
                )
            })?;
            return Ok((precision, scale));
        }
    }
    Ok((38, 0))
}

fn build_primary_key_output_schema_from_schema(
    output_schema: &SchemaRef,
    key_schema_indexes: &[usize],
) -> Result<SchemaRef, String> {
    let mut fields = Vec::with_capacity(key_schema_indexes.len());
    for key_idx in key_schema_indexes {
        let field = output_schema
            .fields()
            .get(*key_idx)
            .cloned()
            .ok_or_else(|| {
                format!(
                    "primary key schema index out of range: key_idx={} output_fields={}",
                    key_idx,
                    output_schema.fields().len()
                )
            })?;
        fields.push(field);
    }
    Ok(Arc::new(Schema::new(fields)))
}

fn encode_primary_keys_from_key_batch(
    key_batch: &RecordBatch,
    key_output_schema: &SchemaRef,
) -> Result<Vec<Vec<u8>>, String> {
    let payload = encode_delete_keys_payload(key_batch)?;
    decode_delete_keys_payload(&payload, key_output_schema)
}

fn append_batch_rows_to_visible_map(
    rows: &mut VisibleRowMap,
    batch: &RecordBatch,
    key_schema_indexes: &[usize],
    key_output_schema: &SchemaRef,
    context: &str,
) -> Result<(), String> {
    if batch.num_rows() == 0 {
        return Ok(());
    }
    let key_batch = project_batch_by_columns(batch, key_schema_indexes, context)?;
    let keys = encode_primary_keys_from_key_batch(&key_batch, key_output_schema)?;
    if keys.len() != batch.num_rows() {
        return Err(format!(
            "key encoding row count mismatch for {}: keys={} rows={}",
            context,
            keys.len(),
            batch.num_rows()
        ));
    }
    for (row_idx, key) in keys.into_iter().enumerate() {
        let mut row = Vec::with_capacity(batch.num_columns());
        for col_idx in 0..batch.num_columns() {
            row.push(batch.column(col_idx).slice(row_idx, 1));
        }
        rows.insert(key, row);
    }
    Ok(())
}

fn build_partial_update_base_rows(
    ctx: &TabletWriteContext,
    output_schema: &SchemaRef,
    key_schema_indexes: &[usize],
    key_output_schema: &SchemaRef,
    existing_txn_log: Option<&StorageTransactionLog>,
) -> Result<VisibleRowMap, String> {
    let mut rows = HashMap::new();
    let published_batch = load_published_visible_batch(ctx, output_schema)?;
    append_batch_rows_to_visible_map(
        &mut rows,
        &published_batch,
        key_schema_indexes,
        key_output_schema,
        "published snapshot",
    )?;
    if let Some(txn_log) = existing_txn_log {
        apply_existing_txn_log_overlay(
            ctx,
            txn_log,
            &mut rows,
            output_schema,
            key_schema_indexes,
            key_output_schema,
        )?;
    }
    Ok(rows)
}

// Build output column hints for partial-update read by mapping each output field to
// the unique_id and default_value from the current (post-ALTER) tablet schema.
// This allows reading old rowsets that lack newly-added columns: the read plan will
// fill the missing column using the schema default via build_missing_output_schema_column_plan.
fn build_partial_update_column_hints(
    output_schema: &SchemaRef,
    current_schema: &StarRocksTabletSchema,
) -> Vec<StarRocksOutputColumnHint> {
    let schema_map: HashMap<String, (Option<u32>, Option<String>)> = current_schema
        .column
        .iter()
        .filter_map(|col| {
            let name = col.name.as_deref()?;
            let normalized = name.trim().to_ascii_lowercase();
            if normalized.is_empty() {
                return None;
            }
            let unique_id = if col.unique_id > 0 {
                u32::try_from(col.unique_id).ok()
            } else {
                None
            };
            let default_value = col
                .default_value
                .as_ref()
                .map(|v| String::from_utf8_lossy(v).into_owned());
            Some((normalized, (unique_id, default_value)))
        })
        .collect();
    output_schema
        .fields()
        .iter()
        .map(|field| {
            let normalized = field.name().trim().to_ascii_lowercase();
            let (schema_unique_id, fallback_default_literal) =
                schema_map.get(&normalized).cloned().unwrap_or((None, None));
            StarRocksOutputColumnHint {
                schema_unique_id,
                physical_binding: StarRocksPhysicalColumnBinding::LegacyName,
                fallback_default_literal,
            }
        })
        .collect()
}

fn load_published_visible_batch(
    ctx: &TabletWriteContext,
    output_schema: &SchemaRef,
) -> Result<RecordBatch, String> {
    let storage_metadata_provider = ctx.storage_metadata_provider.as_deref().ok_or_else(|| {
        "storage metadata capability is unavailable because no compat provider is installed"
            .to_string()
    })?;
    let (latest_version, _) = load_latest_tablet_metadata_with_provider(
        &ctx.tablet_root_path,
        ctx.tablet_id,
        storage_metadata_provider,
    )?;
    if latest_version <= 0 {
        return Ok(RecordBatch::new_empty(output_schema.clone()));
    }
    let object_store_profile = build_metadata_object_store_profile_for_partial(
        &ctx.tablet_root_path,
        ctx.s3_config.as_ref(),
    )?;
    let snapshot = match load_tablet_snapshot_with_metadata_provider(
        ctx.tablet_id,
        latest_version,
        &ctx.tablet_root_path,
        object_store_profile.as_ref(),
        storage_metadata_provider,
    ) {
        Ok(snapshot) => snapshot,
        Err(err) if is_missing_tablet_metadata_error(&err) => {
            return Ok(RecordBatch::new_empty(output_schema.clone()));
        }
        Err(err) => return Err(err),
    };
    if let Some(batch) = read_bundle_parquet_snapshot_if_any(&snapshot, output_schema.clone())? {
        return Ok(batch);
    }
    if snapshot.segment_files.is_empty() {
        return Ok(RecordBatch::new_empty(output_schema.clone()));
    }
    let segment_footers = load_bundle_segment_footers(
        &snapshot,
        &ctx.tablet_root_path,
        object_store_profile.as_ref(),
    )?;
    let output_hints = build_partial_update_column_hints(output_schema, &ctx.tablet_schema);
    let plan = build_native_read_plan_with_output_hints(
        &snapshot,
        &segment_footers,
        output_schema,
        &output_hints,
        None,
    )?;
    build_native_record_batch(
        &plan,
        &segment_footers,
        &ctx.tablet_root_path,
        object_store_profile.as_ref(),
        output_schema,
        &[],
    )
}

fn apply_existing_txn_log_overlay(
    ctx: &TabletWriteContext,
    txn_log: &StorageTransactionLog,
    rows: &mut VisibleRowMap,
    output_schema: &SchemaRef,
    key_schema_indexes: &[usize],
    key_output_schema: &SchemaRef,
) -> Result<(), String> {
    let Some(op_write) = txn_log.write.as_ref() else {
        return Ok(());
    };
    if let Some(rowset) = op_write.rowset.as_ref()
        && rowset.num_rows.unwrap_or(0) > 0
        && !rowset.segments.is_empty()
    {
        let rowset_batch = load_rowset_batch_for_partial_update(ctx, rowset, output_schema)?;
        append_batch_rows_to_visible_map(
            rows,
            &rowset_batch,
            key_schema_indexes,
            key_output_schema,
            "txn log op_write rowset",
        )?;
    }
    for del_file in &op_write.dels {
        let delete_keys = load_delete_keys_from_del_file(ctx, del_file, key_output_schema)?;
        for key in delete_keys {
            rows.remove(&key);
        }
    }
    Ok(())
}

pub(crate) fn load_rowset_batch_for_partial_update(
    ctx: &TabletWriteContext,
    rowset: &StorageRowset,
    output_schema: &SchemaRef,
) -> Result<RecordBatch, String> {
    if rowset.segments.is_empty() || rowset.num_rows.unwrap_or(0) <= 0 {
        return Ok(RecordBatch::new_empty(output_schema.clone()));
    }
    let snapshot = build_rowset_snapshot_for_partial_update(ctx, rowset, 0, &[])?;
    load_rowset_batch_from_partial_update_snapshot(ctx, snapshot, output_schema)
}

pub(crate) fn load_rowset_batch_for_partial_update_with_delete_predicates(
    ctx: &TabletWriteContext,
    rowset: &StorageRowset,
    rowset_visibility_version: i64,
    delete_predicates: &[StarRocksDeletePredicateRaw],
    output_schema: &SchemaRef,
) -> Result<RecordBatch, String> {
    if rowset.segments.is_empty() || rowset.num_rows.unwrap_or(0) <= 0 {
        return Ok(RecordBatch::new_empty(output_schema.clone()));
    }
    let snapshot = build_rowset_snapshot_for_partial_update(
        ctx,
        rowset,
        rowset_visibility_version,
        delete_predicates,
    )?;
    load_rowset_batch_from_partial_update_snapshot(ctx, snapshot, output_schema)
}

fn load_rowset_batch_from_partial_update_snapshot(
    ctx: &TabletWriteContext,
    snapshot: StarRocksTabletSnapshot,
    output_schema: &SchemaRef,
) -> Result<RecordBatch, String> {
    let object_store_profile = build_metadata_object_store_profile_for_partial(
        &ctx.tablet_root_path,
        ctx.s3_config.as_ref(),
    )?;
    if let Some(batch) = read_bundle_parquet_snapshot_if_any(&snapshot, output_schema.clone())? {
        return Ok(batch);
    }
    let segment_footers = load_bundle_segment_footers(
        &snapshot,
        &ctx.tablet_root_path,
        object_store_profile.as_ref(),
    )?;
    let output_hints = build_partial_update_column_hints(output_schema, &ctx.tablet_schema);
    let plan = build_native_read_plan_with_output_hints(
        &snapshot,
        &segment_footers,
        output_schema,
        &output_hints,
        None,
    )?;
    build_native_record_batch(
        &plan,
        &segment_footers,
        &ctx.tablet_root_path,
        object_store_profile.as_ref(),
        output_schema,
        &[],
    )
}

fn build_rowset_snapshot_for_partial_update(
    ctx: &TabletWriteContext,
    rowset: &StorageRowset,
    rowset_visibility_version: i64,
    delete_predicates: &[StarRocksDeletePredicateRaw],
) -> Result<StarRocksTabletSnapshot, String> {
    let rowset_id = rowset.id.unwrap_or(1);
    let schema_id = ctx.tablet_schema.id.filter(|id| *id > 0);
    let mut segment_files = Vec::with_capacity(rowset.segments.len());
    for (idx, segment_name) in rowset.segments.iter().enumerate() {
        let relative_path = format!("{DATA_DIR}/{}", segment_name.trim_start_matches('/'));
        let path = join_tablet_path(&ctx.tablet_root_path, &relative_path)?;
        let operator_relative_path =
            tablet_operator_relative_path(&ctx.tablet_root_path, &relative_path)?;
        let segment_id = rowset_id
            .checked_add(u32::try_from(idx).map_err(|_| {
                format!(
                    "segment index overflow while building rowset snapshot for partial update: index={}",
                    idx
                )
            })?)
            .ok_or_else(|| {
                format!(
                    "segment id overflow while building rowset snapshot for partial update: rowset_id={} index={}",
                    rowset_id, idx
                )
            })?;
        segment_files.push(StarRocksSegmentFile {
            name: segment_name.clone(),
            relative_path: operator_relative_path,
            path,
            rowset_version: rowset_visibility_version,
            schema_id,
            segment_id: Some(segment_id),
            bundle_file_offset: rowset.bundle_file_offsets.get(idx).copied(),
            segment_size: rowset.segment_size.get(idx).copied(),
        });
    }
    // Synthetic rowset snapshots are not metadata versions. Use a stable negative key to avoid
    // colliding with real metadata-version cache entries in segment footer cache.
    let snapshot_version = synthetic_rowset_snapshot_version(rowset);
    Ok(StarRocksTabletSnapshot {
        tablet_id: ctx.tablet_id,
        version: snapshot_version,
        metadata_path: String::new(),
        tablet_schema: ctx.tablet_schema.clone(),
        historical_schemas: schema_id
            .map(|id| std::collections::BTreeMap::from([(id, ctx.tablet_schema.clone())]))
            .unwrap_or_default(),
        total_num_rows: rowset.num_rows.unwrap_or(0).max(0) as u64,
        rowset_count: 1,
        segment_files,
        delete_predicates: delete_predicates
            .iter()
            .filter(|pred| pred.version >= rowset_visibility_version)
            .cloned()
            .collect(),
        delvec_meta: Default::default(),
    })
}

fn synthetic_rowset_snapshot_version(rowset: &StorageRowset) -> i64 {
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    rowset.id.hash(&mut hasher);
    rowset.version.hash(&mut hasher);
    rowset.num_rows.hash(&mut hasher);
    rowset.num_dels.hash(&mut hasher);
    rowset.segments.hash(&mut hasher);
    rowset.segment_size.hash(&mut hasher);
    rowset.bundle_file_offsets.hash(&mut hasher);
    let bucket = (i64::MAX as u64).saturating_sub(1);
    let positive = (hasher.finish() % bucket).saturating_add(1);
    -(positive as i64)
}

fn is_missing_tablet_metadata_error(error: &str) -> bool {
    let lowered = error.to_ascii_lowercase();
    lowered.contains("metadata file not found:")
        || lowered.contains("bundle metadata does not contain tablet page:")
        || lowered.contains("bundle metadata missing tablet page for tablet_id=")
}

pub(crate) fn build_metadata_object_store_profile_for_partial(
    tablet_root_path: &str,
    s3_config: Option<&S3StoreConfig>,
) -> Result<Option<ObjectStoreProfile>, String> {
    crate::connector::starrocks::fs_access::object_store_profile_for_tablet_root(
        tablet_root_path,
        s3_config,
    )
}

fn load_delete_keys_from_del_file(
    ctx: &TabletWriteContext,
    del_file_name: &str,
    key_output_schema: &SchemaRef,
) -> Result<Vec<Vec<u8>>, String> {
    let del_file_path = join_tablet_path(
        &ctx.tablet_root_path,
        &format!("{DATA_DIR}/{}", del_file_name.trim_start_matches('/')),
    )?;
    let payload = read_bytes(&del_file_path)?;
    decode_delete_keys_payload(&payload, key_output_schema)
}

fn materialize_partial_upsert_batch(
    ctx: &TabletWriteContext,
    data_batch: &RecordBatch,
    data_batch_slot_ids: Option<&[Option<SlotId>]>,
    mode: ResolvedPartialWriteMode,
    existing_txn_log: Option<&StorageTransactionLog>,
) -> Result<RecordBatch, String> {
    let output_schema = build_tablet_output_schema(&ctx.tablet_schema)?;
    let schema_to_batch =
        resolve_schema_column_batch_indexes(ctx, data_batch, data_batch_slot_ids)?;
    let key_schema_indexes = primary_key_schema_indexes(&ctx.tablet_schema)?;
    let mut key_batch_indexes = Vec::with_capacity(key_schema_indexes.len());
    for key_schema_idx in &key_schema_indexes {
        let key_batch_idx = schema_to_batch
            .get(*key_schema_idx)
            .and_then(|idx| *idx)
            .ok_or_else(|| {
                let key_name = ctx.tablet_schema.column[*key_schema_idx]
                    .name
                    .clone()
                    .unwrap_or_else(|| format!("<key_{}>", key_schema_idx));
                format!(
                    "partial update write missing primary key column '{}' (schema_index={})",
                    key_name, key_schema_idx
                )
            })?;
        key_batch_indexes.push(key_batch_idx);
    }
    let key_batch = project_batch_by_columns(data_batch, &key_batch_indexes, "partial update key")?;
    let key_output_schema =
        build_primary_key_output_schema_from_schema(&output_schema, &key_schema_indexes)?;
    let keys = encode_primary_keys_from_key_batch(&key_batch, &key_output_schema)?;
    let mut base_rows = build_partial_update_base_rows(
        ctx,
        &output_schema,
        &key_schema_indexes,
        &key_output_schema,
        existing_txn_log,
    )?;
    let merge_condition_idx = resolve_merge_condition_column_index(ctx)?;
    let default_values =
        build_partial_update_default_values(ctx, &output_schema, &schema_to_batch)?;

    let auto_policy = &ctx.partial_update.auto_increment;
    if auto_policy.miss_auto_increment_column && auto_policy.auto_increment_in_sort_key {
        return Err(
            "partial update does not support missing auto_increment column when it is in sort key"
                .to_string(),
        );
    }
    let auto_col_idx = auto_policy.auto_increment_column_idx;
    let mut auto_ids = Vec::new();
    let mut auto_pos = 0usize;
    let mut skipped_column_update_missing_base = 0usize;
    let mut skipped_by_merge_condition = 0usize;
    let mut kept_rows = 0usize;

    let mut output_fragments: Vec<Vec<ArrayRef>> = (0..output_schema.fields().len())
        .map(|_| Vec::new())
        .collect();
    for row_idx in 0..data_batch.num_rows() {
        let key = keys.get(row_idx).cloned().ok_or_else(|| {
            format!(
                "partial update key row mismatch: row_idx={} key_count={}",
                row_idx,
                keys.len()
            )
        })?;
        let existing_row = base_rows.get(&key).cloned();
        if existing_row.is_none() && mode == ResolvedPartialWriteMode::ColumnUpdate {
            skipped_column_update_missing_base =
                skipped_column_update_missing_base.saturating_add(1);
            continue;
        }
        if let Some(old_row) = existing_row.as_ref()
            && let Some(merge_idx) = merge_condition_idx
        {
            let new_value = if let Some(src_idx) = schema_to_batch.get(merge_idx).and_then(|v| *v) {
                data_batch.column(src_idx).slice(row_idx, 1)
            } else {
                old_row[merge_idx].clone()
            };
            if scalar_array_gt(&old_row[merge_idx], &new_value)? {
                skipped_by_merge_condition = skipped_by_merge_condition.saturating_add(1);
                continue;
            }
        }

        let mut materialized_row = Vec::with_capacity(output_schema.fields().len());
        for col_idx in 0..output_schema.fields().len() {
            // When miss_auto_increment_column is true, the auto-increment column in
            // the batch has a 0 placeholder.  For existing rows use the existing value;
            // for new rows allocate a fresh ID (fall through below).
            let is_auto_placeholder =
                auto_policy.miss_auto_increment_column && auto_col_idx == Some(col_idx);
            if is_auto_placeholder {
                if let Some(old_row) = existing_row.as_ref() {
                    materialized_row.push(old_row[col_idx].clone());
                    continue;
                }
                // New row — fall through to auto_increment allocation below.
            } else if let Some(src_idx) = schema_to_batch.get(col_idx).and_then(|v| *v) {
                let col = data_batch.column(src_idx).slice(row_idx, 1);
                let expected_type = output_schema.field(col_idx).data_type();
                let col = if col.data_type() != expected_type {
                    cast(&col, expected_type).map_err(|e| {
                        format!(
                            "cast partial-update column {} from {:?} to {:?} failed: {}",
                            col_idx,
                            col.data_type(),
                            expected_type,
                            e
                        )
                    })?
                } else {
                    col
                };
                materialized_row.push(col);
                continue;
            }
            if let Some(old_row) = existing_row.as_ref() {
                materialized_row.push(old_row[col_idx].clone());
                continue;
            }
            if auto_policy.miss_auto_increment_column && auto_col_idx == Some(col_idx) {
                let fe_addr = auto_policy.fe_addr.as_ref().ok_or_else(|| {
                    "partial update cannot allocate auto_increment id without FE address"
                        .to_string()
                })?;
                if auto_pos >= auto_ids.len() {
                    let remaining = data_batch.num_rows().saturating_sub(row_idx).max(1);
                    auto_ids = allocate_auto_increment_ids(
                        auto_policy.frontend_provider.as_deref(),
                        fe_addr,
                        ctx.table_id,
                        remaining,
                    )?;
                    auto_pos = 0;
                }
                let auto_id = *auto_ids.get(auto_pos).ok_or_else(|| {
                    "allocate_auto_increment_ids returned fewer ids than requested".to_string()
                })?;
                auto_pos = auto_pos.saturating_add(1);
                materialized_row.push(build_auto_increment_singleton_array(
                    auto_id,
                    output_schema.field(col_idx).data_type(),
                )?);
                continue;
            }
            materialized_row.push(default_values[col_idx].clone());
        }

        for (col_idx, value) in materialized_row.iter().enumerate() {
            output_fragments[col_idx].push(value.clone());
        }
        kept_rows = kept_rows.saturating_add(1);
        base_rows.insert(key, materialized_row);
    }

    if ctx.partial_update.merge_condition.is_some() {
        info!(
            target: "novarocks::sink",
            table_id = ctx.table_id,
            tablet_id = ctx.tablet_id,
            mode = ?mode,
            input_rows = data_batch.num_rows(),
            base_rows_before = base_rows.len().saturating_sub(kept_rows),
            kept_rows,
            skipped_column_update_missing_base,
            skipped_by_merge_condition,
            merge_condition_column = ?ctx.partial_update.merge_condition,
            "OLAP_TABLE_SINK materialize_partial_upsert_batch summary"
        );
    }

    if output_fragments.is_empty() || output_fragments[0].is_empty() {
        return Ok(RecordBatch::new_empty(output_schema));
    }

    let mut output_columns = Vec::with_capacity(output_fragments.len());
    for (col_idx, fragments) in output_fragments.into_iter().enumerate() {
        if fragments.len() == 1 {
            output_columns.push(fragments[0].clone());
            continue;
        }
        let concat_input = fragments.iter().map(|v| v.as_ref()).collect::<Vec<_>>();
        let merged = concat(&concat_input).map_err(|e| {
            format!(
                "concat materialized partial-update column failed: col_idx={} rows={} error={}",
                col_idx,
                concat_input.len(),
                e
            )
        })?;
        output_columns.push(merged);
    }

    let output_row_count = output_columns
        .first()
        .map(|column| column.len())
        .unwrap_or_default();
    RecordBatch::try_new(output_schema, output_columns).map_err(|e| {
        format!(
            "build materialized partial-update batch failed: rows={} error={}",
            output_row_count, e
        )
    })
}

fn resolve_merge_condition_column_index(ctx: &TabletWriteContext) -> Result<Option<usize>, String> {
    let Some(merge_condition) = ctx.partial_update.merge_condition.as_ref() else {
        return Ok(None);
    };
    let normalized = normalize_identifier(merge_condition);
    if normalized.is_empty() {
        return Ok(None);
    }
    for (idx, column) in ctx.tablet_schema.column.iter().enumerate() {
        if column
            .name
            .as_deref()
            .is_some_and(|name| normalize_identifier(name) == normalized)
        {
            return Ok(Some(idx));
        }
    }
    Err(format!(
        "merge_condition column '{}' not found in tablet schema",
        merge_condition
    ))
}

fn build_partial_update_default_values(
    ctx: &TabletWriteContext,
    output_schema: &SchemaRef,
    schema_to_batch: &[Option<usize>],
) -> Result<Vec<ArrayRef>, String> {
    let mut out = Vec::with_capacity(output_schema.fields().len());
    for (idx, field) in output_schema.fields().iter().enumerate() {
        if schema_to_batch.get(idx).and_then(|v| *v).is_some() {
            out.push(new_null_array(field.data_type(), 1));
            continue;
        }
        let column = ctx.tablet_schema.column.get(idx).ok_or_else(|| {
            format!(
                "tablet schema column index out of range while building defaults: idx={} columns={}",
                idx,
                ctx.tablet_schema.column.len()
            )
        })?;
        if let Some(raw_default) = resolve_default_literal_for_column(ctx, column) {
            out.push(parse_default_literal_to_singleton_array(
                field.data_type(),
                &raw_default,
            )?);
            continue;
        }
        if field.is_nullable() {
            out.push(new_null_array(field.data_type(), 1));
            continue;
        }
        return Err(format!(
            "partial update missing default value for non-nullable column '{}'",
            field.name()
        ));
    }
    Ok(out)
}

fn resolve_default_literal_for_column(
    ctx: &TabletWriteContext,
    column: &StarRocksColumnSchema,
) -> Option<String> {
    if let Some(name) = column.name.as_deref()
        && let Some(expr_default) = ctx.partial_update.expr_default_value_for(name)
    {
        return Some(expr_default.to_string());
    }
    let unique_id_key = column.unique_id.to_string();
    if let Some(expr_default) = ctx.partial_update.column_to_expr_value.get(&unique_id_key) {
        return Some(expr_default.clone());
    }
    column
        .default_value
        .as_ref()
        .map(|raw| String::from_utf8_lossy(raw).to_string())
}

pub(crate) fn parse_default_literal_to_singleton_array(
    data_type: &DataType,
    literal: &str,
) -> Result<ArrayRef, String> {
    let normalized = literal.trim();
    if normalized.eq_ignore_ascii_case("null") {
        return Ok(new_null_array(data_type, 1));
    }
    let unquoted = strip_wrapping_quotes(normalized);
    match data_type {
        DataType::Boolean => {
            let parsed = match unquoted.to_ascii_lowercase().as_str() {
                "1" | "true" => true,
                "0" | "false" => false,
                other => {
                    return Err(format!("parse BOOLEAN default literal failed: '{}'", other));
                }
            };
            Ok(Arc::new(BooleanArray::from(vec![Some(parsed)])))
        }
        DataType::Int8 => Ok(Arc::new(Int8Array::from(vec![Some(
            unquoted
                .parse::<i8>()
                .map_err(|e| format!("parse INT8 default literal '{}' failed: {}", unquoted, e))?,
        )]))),
        DataType::Int16 => Ok(Arc::new(Int16Array::from(vec![Some(
            unquoted
                .parse::<i16>()
                .map_err(|e| format!("parse INT16 default literal '{}' failed: {}", unquoted, e))?,
        )]))),
        DataType::Int32 => Ok(Arc::new(Int32Array::from(vec![Some(
            unquoted
                .parse::<i32>()
                .map_err(|e| format!("parse INT32 default literal '{}' failed: {}", unquoted, e))?,
        )]))),
        DataType::Int64 => Ok(Arc::new(Int64Array::from(vec![Some(
            unquoted
                .parse::<i64>()
                .map_err(|e| format!("parse INT64 default literal '{}' failed: {}", unquoted, e))?,
        )]))),
        DataType::Float32 => Ok(Arc::new(Float32Array::from(vec![Some(
            unquoted
                .parse::<f32>()
                .map_err(|e| format!("parse FLOAT default literal '{}' failed: {}", unquoted, e))?,
        )]))),
        DataType::Float64 => Ok(Arc::new(Float64Array::from(vec![Some(
            unquoted.parse::<f64>().map_err(|e| {
                format!("parse DOUBLE default literal '{}' failed: {}", unquoted, e)
            })?,
        )]))),
        DataType::Date32 => Ok(Arc::new(Date32Array::from(vec![Some(
            parse_date32_default_literal(unquoted)?,
        )]))),
        DataType::Timestamp(TimeUnit::Microsecond, None) => Ok(Arc::new(
            TimestampMicrosecondArray::from(vec![Some(parse_timestamp_default_literal(unquoted)?)]),
        )),
        DataType::FixedSizeBinary(width)
            if *width == novarocks_types::largeint::LARGEINT_BYTE_WIDTH =>
        {
            let parsed = unquoted.parse::<i128>().map_err(|e| {
                format!(
                    "parse LARGEINT default literal '{}' failed: {}",
                    unquoted, e
                )
            })?;
            novarocks_types::largeint::array_from_i128(&[Some(parsed)])
        }
        DataType::Decimal128(precision, scale) => {
            let parsed = parse_decimal128_default_literal(unquoted, *precision, *scale)?;
            let array = Decimal128Array::from(vec![Some(parsed)])
                .with_precision_and_scale(*precision, *scale)
                .map_err(|e| format!("build DECIMAL default array failed: {}", e))?;
            Ok(Arc::new(array))
        }
        DataType::Decimal256(precision, scale) => {
            use arrow::array::Decimal256Array;
            let parsed = parse_decimal256_default_literal(unquoted, *precision, *scale)?;
            let array = Decimal256Array::from(vec![Some(parsed)])
                .with_precision_and_scale(*precision, *scale)
                .map_err(|e| format!("build DECIMAL256 default array failed: {}", e))?;
            Ok(Arc::new(array))
        }
        DataType::Utf8 => Ok(Arc::new(StringArray::from(vec![Some(
            unquoted.to_string(),
        )]))),
        DataType::Binary => Ok(Arc::new(BinaryArray::from(vec![Some(unquoted.as_bytes())]))),
        // Complex types (ARRAY, MAP, STRUCT): StarRocks serializes them as JSON in ColumnPB.
        DataType::List(_) | DataType::Map(_, _) | DataType::Struct(_) => {
            let json: serde_json::Value = serde_json::from_str(normalized).map_err(|e| {
                format!(
                    "parse complex default literal as JSON failed: '{}' error={}",
                    normalized, e
                )
            })?;
            json_value_to_arrow_singleton_array(&json, data_type)
        }
        other => Err(format!(
            "unsupported default literal type for partial update: {:?}",
            other
        )),
    }
}

/// Build a singleton (1-row) Arrow array from a JSON value for a given Arrow DataType.
/// Used to materialize complex-type column default values from their JSON representation
/// stored in StarRocks ColumnPB (serialized via `cast_type_to_json_str` on the BE).
fn json_value_to_arrow_singleton_array(
    json: &serde_json::Value,
    data_type: &DataType,
) -> Result<ArrayRef, String> {
    use arrow::array::{
        Decimal256Array, Float32Array, Float64Array, Int8Array, Int16Array, Int32Array, Int64Array,
        ListArray, MapArray, StructArray,
    };
    use arrow::datatypes::Fields;
    use arrow_buffer::{NullBufferBuilder, OffsetBuffer};

    if json.is_null() {
        return Ok(new_null_array(data_type, 1));
    }

    match data_type {
        // ── Scalar types ──────────────────────────────────────────────────────────
        DataType::Boolean => {
            let v = match json {
                serde_json::Value::Bool(b) => *b,
                serde_json::Value::Number(n) => n
                    .as_i64()
                    .map(|i| i != 0)
                    .ok_or_else(|| format!("cannot parse JSON number as boolean: {}", n))?,
                serde_json::Value::String(s) => match s.to_ascii_lowercase().as_str() {
                    "true" | "1" => true,
                    "false" | "0" => false,
                    other => {
                        return Err(format!("cannot parse JSON string as boolean: '{}'", other));
                    }
                },
                other => return Err(format!("unexpected JSON type for boolean: {:?}", other)),
            };
            Ok(Arc::new(BooleanArray::from(vec![Some(v)])))
        }
        DataType::Int8 => {
            let s = json_value_to_string(json)?;
            let v = s
                .trim()
                .parse::<i8>()
                .map_err(|e| format!("parse JSON '{}' as Int8 failed: {}", s, e))?;
            Ok(Arc::new(Int8Array::from(vec![Some(v)])))
        }
        DataType::Int16 => {
            let s = json_value_to_string(json)?;
            let v = s
                .trim()
                .parse::<i16>()
                .map_err(|e| format!("parse JSON '{}' as Int16 failed: {}", s, e))?;
            Ok(Arc::new(Int16Array::from(vec![Some(v)])))
        }
        DataType::Int32 => {
            let s = json_value_to_string(json)?;
            let v = s
                .trim()
                .parse::<i32>()
                .map_err(|e| format!("parse JSON '{}' as Int32 failed: {}", s, e))?;
            Ok(Arc::new(Int32Array::from(vec![Some(v)])))
        }
        DataType::Int64 => {
            let s = json_value_to_string(json)?;
            let v = s
                .trim()
                .parse::<i64>()
                .map_err(|e| format!("parse JSON '{}' as Int64 failed: {}", s, e))?;
            Ok(Arc::new(Int64Array::from(vec![Some(v)])))
        }
        DataType::Float32 => {
            let s = json_value_to_string(json)?;
            let v = s
                .trim()
                .parse::<f32>()
                .map_err(|e| format!("parse JSON '{}' as Float32 failed: {}", s, e))?;
            Ok(Arc::new(Float32Array::from(vec![Some(v)])))
        }
        DataType::Float64 => {
            let s = json_value_to_string(json)?;
            let v = s
                .trim()
                .parse::<f64>()
                .map_err(|e| format!("parse JSON '{}' as Float64 failed: {}", s, e))?;
            Ok(Arc::new(Float64Array::from(vec![Some(v)])))
        }
        DataType::Utf8 => {
            let s = json_value_to_string(json)?;
            Ok(Arc::new(StringArray::from(vec![Some(s)])))
        }
        DataType::Binary => {
            let s = json_value_to_string(json)?;
            Ok(Arc::new(BinaryArray::from(vec![Some(s.as_bytes())])))
        }
        DataType::Date32 => {
            let s = json_value_to_string(json)?;
            Ok(Arc::new(Date32Array::from(vec![Some(
                parse_date32_default_literal(&s)?,
            )])))
        }
        DataType::Timestamp(TimeUnit::Microsecond, None) => {
            let s = json_value_to_string(json)?;
            Ok(Arc::new(TimestampMicrosecondArray::from(vec![Some(
                parse_timestamp_default_literal(&s)?,
            )])))
        }
        DataType::Decimal128(precision, scale) => {
            let s = json_value_to_string(json)?;
            let v = parse_decimal128_default_literal(&s, *precision, *scale)?;
            let array = Decimal128Array::from(vec![Some(v)])
                .with_precision_and_scale(*precision, *scale)
                .map_err(|e| format!("build Decimal128 array failed: {}", e))?;
            Ok(Arc::new(array))
        }
        DataType::Decimal256(precision, scale) => {
            let s = json_value_to_string(json)?;
            let v = parse_decimal256_default_literal(&s, *precision, *scale)?;
            let array = Decimal256Array::from(vec![Some(v)])
                .with_precision_and_scale(*precision, *scale)
                .map_err(|e| format!("build Decimal256 array failed: {}", e))?;
            Ok(Arc::new(array))
        }
        DataType::FixedSizeBinary(width)
            if *width == novarocks_types::largeint::LARGEINT_BYTE_WIDTH =>
        {
            let s = json_value_to_string(json)?;
            let parsed = s
                .trim()
                .parse::<i128>()
                .map_err(|e| format!("parse LARGEINT default literal '{}' failed: {}", s, e))?;
            novarocks_types::largeint::array_from_i128(&[Some(parsed)])
        }
        // ── ARRAY ─────────────────────────────────────────────────────────────────
        DataType::List(item_field) => {
            let arr = json
                .as_array()
                .ok_or_else(|| format!("expected JSON array for List type, got: {}", json))?;
            let mut element_arrays: Vec<ArrayRef> = Vec::with_capacity(arr.len());
            for item in arr {
                element_arrays.push(json_value_to_arrow_singleton_array(
                    item,
                    item_field.data_type(),
                )?);
            }
            // Concatenate all element singletons into a values array.
            let values: ArrayRef = if element_arrays.is_empty() {
                arrow::array::new_empty_array(item_field.data_type())
            } else {
                let refs: Vec<&dyn arrow::array::Array> =
                    element_arrays.iter().map(|a| a.as_ref()).collect();
                concat(&refs).map_err(|e| format!("concat List element arrays failed: {}", e))?
            };
            let n = values.len() as i32;
            let offsets = OffsetBuffer::new(arrow_buffer::ScalarBuffer::from(vec![0i32, n]));
            let list = ListArray::new(item_field.clone(), offsets, values, None);
            Ok(Arc::new(list))
        }
        // ── MAP ───────────────────────────────────────────────────────────────────
        DataType::Map(entry_field, sorted) => {
            // entry_field is a non-nullable Struct with exactly two fields: key and value.
            let (key_field, value_field) = match entry_field.data_type() {
                DataType::Struct(fields) if fields.len() == 2 => {
                    (fields[0].clone(), fields[1].clone())
                }
                other => {
                    return Err(format!(
                        "unexpected Map entry field type (expected 2-field struct): {:?}",
                        other
                    ));
                }
            };
            let obj = json
                .as_object()
                .ok_or_else(|| format!("expected JSON object for Map type, got: {}", json))?;
            let mut key_arrays: Vec<ArrayRef> = Vec::with_capacity(obj.len());
            let mut val_arrays: Vec<ArrayRef> = Vec::with_capacity(obj.len());
            for (k, v) in obj {
                // Keys in JSON are always strings; parse to key_field type.
                let key_json = serde_json::Value::String(k.clone());
                key_arrays.push(json_value_to_arrow_singleton_array(
                    &key_json,
                    key_field.data_type(),
                )?);
                val_arrays.push(json_value_to_arrow_singleton_array(
                    v,
                    value_field.data_type(),
                )?);
            }
            let keys: ArrayRef = if key_arrays.is_empty() {
                arrow::array::new_empty_array(key_field.data_type())
            } else {
                let refs: Vec<&dyn arrow::array::Array> =
                    key_arrays.iter().map(|a| a.as_ref()).collect();
                concat(&refs).map_err(|e| format!("concat Map key arrays failed: {}", e))?
            };
            let vals: ArrayRef = if val_arrays.is_empty() {
                arrow::array::new_empty_array(value_field.data_type())
            } else {
                let refs: Vec<&dyn arrow::array::Array> =
                    val_arrays.iter().map(|a| a.as_ref()).collect();
                concat(&refs).map_err(|e| format!("concat Map value arrays failed: {}", e))?
            };
            let n = keys.len() as i32;
            let offsets = OffsetBuffer::new(arrow_buffer::ScalarBuffer::from(vec![0i32, n]));
            let entry_struct = StructArray::new(
                Fields::from(vec![
                    key_field.as_ref().clone(),
                    value_field.as_ref().clone(),
                ]),
                vec![keys, vals],
                None,
            );
            let map = MapArray::try_new(entry_field.clone(), offsets, entry_struct, None, *sorted)
                .map_err(|e| format!("build Map singleton array failed: {}", e))?;
            Ok(Arc::new(map))
        }
        // ── STRUCT ────────────────────────────────────────────────────────────────
        DataType::Struct(fields) => {
            let obj = json
                .as_object()
                .ok_or_else(|| format!("expected JSON object for Struct type, got: {}", json))?;
            let mut columns: Vec<ArrayRef> = Vec::with_capacity(fields.len());
            let mut null_builder = NullBufferBuilder::new(1);
            null_builder.append_non_null();
            for field in fields.iter() {
                let v = obj.get(field.name()).unwrap_or(&serde_json::Value::Null);
                columns.push(json_value_to_arrow_singleton_array(v, field.data_type())?);
            }
            let null_buf = null_builder.finish();
            let struct_array = StructArray::new(fields.clone(), columns, null_buf);
            Ok(Arc::new(struct_array))
        }
        other => Err(format!(
            "unsupported JSON default literal type: {:?}",
            other
        )),
    }
}

fn json_value_to_string(json: &serde_json::Value) -> Result<String, String> {
    match json {
        serde_json::Value::String(s) => Ok(s.clone()),
        serde_json::Value::Number(n) => Ok(n.to_string()),
        serde_json::Value::Bool(b) => Ok(if *b {
            "true".to_string()
        } else {
            "false".to_string()
        }),
        other => Err(format!("cannot convert JSON value to string: {:?}", other)),
    }
}

fn strip_wrapping_quotes(raw: &str) -> &str {
    if raw.len() >= 2 {
        let bytes = raw.as_bytes();
        let first = bytes[0];
        let last = bytes[bytes.len() - 1];
        if (first == b'\'' && last == b'\'') || (first == b'"' && last == b'"') {
            return &raw[1..raw.len() - 1];
        }
    }
    raw
}

fn parse_date32_default_literal(raw: &str) -> Result<i32, String> {
    const UNIX_EPOCH_DAY_OFFSET: i32 = 719_163;
    if let Ok(date) = NaiveDate::parse_from_str(raw, "%Y-%m-%d") {
        return Ok(date.num_days_from_ce() - UNIX_EPOCH_DAY_OFFSET);
    }
    if let Ok(date_time) = NaiveDateTime::parse_from_str(raw, "%Y-%m-%d %H:%M:%S%.f") {
        return Ok(date_time.date().num_days_from_ce() - UNIX_EPOCH_DAY_OFFSET);
    }
    Err(format!("parse DATE default literal failed: '{}'", raw))
}

fn parse_timestamp_default_literal(raw: &str) -> Result<i64, String> {
    if let Ok(date_time) = NaiveDateTime::parse_from_str(raw, "%Y-%m-%d %H:%M:%S%.f") {
        return Ok(date_time.and_utc().timestamp_micros());
    }
    if let Ok(date) = NaiveDate::parse_from_str(raw, "%Y-%m-%d") {
        let date_time = date
            .and_hms_opt(0, 0, 0)
            .ok_or_else(|| format!("parse DATETIME default literal failed: '{}'", raw))?;
        return Ok(date_time.and_utc().timestamp_micros());
    }
    Err(format!("parse DATETIME default literal failed: '{}'", raw))
}

fn parse_decimal128_default_literal(raw: &str, precision: u8, scale: i8) -> Result<i128, String> {
    if scale < 0 {
        return Err(format!(
            "invalid decimal scale for default literal: {}",
            scale
        ));
    }
    let scale_usize = usize::try_from(scale)
        .map_err(|_| format!("invalid decimal scale for default literal: {}", scale))?;
    let mut value = raw.trim();
    let negative = value.starts_with('-');
    if negative || value.starts_with('+') {
        value = &value[1..];
    }
    let parts = value.split('.').collect::<Vec<_>>();
    if parts.len() > 2 {
        return Err(format!("parse DECIMAL default literal failed: '{}'", raw));
    }
    let integer_digits = parts[0].chars().filter(|c| *c != '_').collect::<String>();
    let mut fractional_digits = if parts.len() == 2 {
        parts[1].chars().filter(|c| *c != '_').collect::<String>()
    } else {
        String::new()
    };
    if fractional_digits.len() > scale_usize {
        return Err(format!(
            "decimal default literal scale overflow: literal='{}' scale={}",
            raw, scale
        ));
    }
    while fractional_digits.len() < scale_usize {
        fractional_digits.push('0');
    }
    let combined = format!("{integer_digits}{fractional_digits}");
    let mut parsed = combined.parse::<i128>().map_err(|e| {
        format!(
            "parse DECIMAL default literal '{}' as integer payload failed: {}",
            raw, e
        )
    })?;
    if negative {
        parsed = -parsed;
    }
    let digit_count = combined.chars().filter(|c| c.is_ascii_digit()).count();
    if digit_count > usize::from(precision) {
        return Err(format!(
            "decimal default literal precision overflow: literal='{}' precision={}",
            raw, precision
        ));
    }
    Ok(parsed)
}

fn parse_decimal256_default_literal(
    raw: &str,
    precision: u8,
    scale: i8,
) -> Result<arrow::datatypes::i256, String> {
    use arrow::datatypes::i256;
    if scale < 0 {
        return Err(format!(
            "invalid decimal scale for default literal: {}",
            scale
        ));
    }
    let scale_usize = usize::try_from(scale)
        .map_err(|_| format!("invalid decimal scale for default literal: {}", scale))?;
    let mut value = raw.trim();
    let negative = value.starts_with('-');
    if negative || value.starts_with('+') {
        value = &value[1..];
    }
    let parts = value.split('.').collect::<Vec<_>>();
    if parts.len() > 2 {
        return Err(format!(
            "parse DECIMAL256 default literal failed: '{}'",
            raw
        ));
    }
    let integer_digits = parts[0].chars().filter(|c| *c != '_').collect::<String>();
    let mut fractional_digits = if parts.len() == 2 {
        parts[1].chars().filter(|c| *c != '_').collect::<String>()
    } else {
        String::new()
    };
    if fractional_digits.len() > scale_usize {
        return Err(format!(
            "decimal256 default literal scale overflow: literal='{}' scale={}",
            raw, scale
        ));
    }
    while fractional_digits.len() < scale_usize {
        fractional_digits.push('0');
    }
    let combined = format!("{integer_digits}{fractional_digits}");
    let digit_count = combined.chars().filter(|c| c.is_ascii_digit()).count();
    if digit_count > usize::from(precision) {
        return Err(format!(
            "decimal256 default literal precision overflow: literal='{}' precision={}",
            raw, precision
        ));
    }
    // Parse combined digit string as i256
    let mut result = i256::ZERO;
    let ten = i256::from_i128(10);
    for ch in combined.chars() {
        let d = ch
            .to_digit(10)
            .ok_or_else(|| format!("non-digit character '{}' in decimal literal '{}'", ch, raw))?;
        result = result
            .wrapping_mul(ten)
            .wrapping_add(i256::from_i128(d as i128));
    }
    if negative {
        result = result.wrapping_neg();
    }
    Ok(result)
}

fn build_auto_increment_singleton_array(
    value: i64,
    data_type: &DataType,
) -> Result<ArrayRef, String> {
    match data_type {
        DataType::Int64 => Ok(Arc::new(Int64Array::from(vec![Some(value)]))),
        DataType::Int32 => {
            let value_i32 = i32::try_from(value)
                .map_err(|_| format!("auto_increment value overflow for INT: {}", value))?;
            Ok(Arc::new(Int32Array::from(vec![Some(value_i32)])))
        }
        other => Err(format!(
            "unsupported auto_increment column type in partial update: {:?}",
            other
        )),
    }
}

fn scalar_array_gt(left: &ArrayRef, right: &ArrayRef) -> Result<bool, String> {
    let right = if left.data_type() == right.data_type() {
        right.clone()
    } else {
        cast(right.as_ref(), left.data_type()).map_err(|e| {
            format!(
                "cast merge_condition value failed: from={:?} to={:?} error={}",
                right.data_type(),
                left.data_type(),
                e
            )
        })?
    };
    if left.is_null(0) || right.is_null(0) {
        return Ok(false);
    }
    match left.data_type() {
        DataType::Boolean => {
            let left = left
                .as_any()
                .downcast_ref::<BooleanArray>()
                .ok_or_else(|| "downcast merge_condition left BOOLEAN failed".to_string())?;
            let right = right
                .as_any()
                .downcast_ref::<BooleanArray>()
                .ok_or_else(|| "downcast merge_condition right BOOLEAN failed".to_string())?;
            Ok(left.value(0) & !right.value(0))
        }
        DataType::Int8 => {
            let left = left
                .as_any()
                .downcast_ref::<Int8Array>()
                .ok_or_else(|| "downcast merge_condition left INT8 failed".to_string())?;
            let right = right
                .as_any()
                .downcast_ref::<Int8Array>()
                .ok_or_else(|| "downcast merge_condition right INT8 failed".to_string())?;
            Ok(left.value(0) > right.value(0))
        }
        DataType::Int16 => {
            let left = left
                .as_any()
                .downcast_ref::<Int16Array>()
                .ok_or_else(|| "downcast merge_condition left INT16 failed".to_string())?;
            let right = right
                .as_any()
                .downcast_ref::<Int16Array>()
                .ok_or_else(|| "downcast merge_condition right INT16 failed".to_string())?;
            Ok(left.value(0) > right.value(0))
        }
        DataType::Int32 => {
            let left = left
                .as_any()
                .downcast_ref::<Int32Array>()
                .ok_or_else(|| "downcast merge_condition left INT32 failed".to_string())?;
            let right = right
                .as_any()
                .downcast_ref::<Int32Array>()
                .ok_or_else(|| "downcast merge_condition right INT32 failed".to_string())?;
            Ok(left.value(0) > right.value(0))
        }
        DataType::Int64 => {
            let left = left
                .as_any()
                .downcast_ref::<Int64Array>()
                .ok_or_else(|| "downcast merge_condition left INT64 failed".to_string())?;
            let right = right
                .as_any()
                .downcast_ref::<Int64Array>()
                .ok_or_else(|| "downcast merge_condition right INT64 failed".to_string())?;
            Ok(left.value(0) > right.value(0))
        }
        DataType::Float32 => {
            let left = left
                .as_any()
                .downcast_ref::<Float32Array>()
                .ok_or_else(|| "downcast merge_condition left FLOAT failed".to_string())?;
            let right = right
                .as_any()
                .downcast_ref::<Float32Array>()
                .ok_or_else(|| "downcast merge_condition right FLOAT failed".to_string())?;
            Ok(left
                .value(0)
                .partial_cmp(&right.value(0))
                .unwrap_or(Ordering::Equal)
                == Ordering::Greater)
        }
        DataType::Float64 => {
            let left = left
                .as_any()
                .downcast_ref::<Float64Array>()
                .ok_or_else(|| "downcast merge_condition left DOUBLE failed".to_string())?;
            let right = right
                .as_any()
                .downcast_ref::<Float64Array>()
                .ok_or_else(|| "downcast merge_condition right DOUBLE failed".to_string())?;
            Ok(left
                .value(0)
                .partial_cmp(&right.value(0))
                .unwrap_or(Ordering::Equal)
                == Ordering::Greater)
        }
        DataType::Date32 => {
            let left = left
                .as_any()
                .downcast_ref::<Date32Array>()
                .ok_or_else(|| "downcast merge_condition left DATE failed".to_string())?;
            let right = right
                .as_any()
                .downcast_ref::<Date32Array>()
                .ok_or_else(|| "downcast merge_condition right DATE failed".to_string())?;
            Ok(left.value(0) > right.value(0))
        }
        DataType::Timestamp(TimeUnit::Microsecond, None) => {
            let left = left
                .as_any()
                .downcast_ref::<TimestampMicrosecondArray>()
                .ok_or_else(|| "downcast merge_condition left DATETIME failed".to_string())?;
            let right = right
                .as_any()
                .downcast_ref::<TimestampMicrosecondArray>()
                .ok_or_else(|| "downcast merge_condition right DATETIME failed".to_string())?;
            Ok(left.value(0) > right.value(0))
        }
        DataType::FixedSizeBinary(width)
            if *width == novarocks_types::largeint::LARGEINT_BYTE_WIDTH =>
        {
            let left = left
                .as_any()
                .downcast_ref::<FixedSizeBinaryArray>()
                .ok_or_else(|| "downcast merge_condition left LARGEINT failed".to_string())?;
            let right = right
                .as_any()
                .downcast_ref::<FixedSizeBinaryArray>()
                .ok_or_else(|| "downcast merge_condition right LARGEINT failed".to_string())?;
            Ok(novarocks_types::largeint::value_at(left, 0)?
                > novarocks_types::largeint::value_at(right, 0)?)
        }
        DataType::Decimal128(_, _) => {
            let left = left
                .as_any()
                .downcast_ref::<Decimal128Array>()
                .ok_or_else(|| "downcast merge_condition left DECIMAL failed".to_string())?;
            let right = right
                .as_any()
                .downcast_ref::<Decimal128Array>()
                .ok_or_else(|| "downcast merge_condition right DECIMAL failed".to_string())?;
            Ok(left.value(0) > right.value(0))
        }
        DataType::Utf8 => {
            let left = left
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| "downcast merge_condition left VARCHAR failed".to_string())?;
            let right = right
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| "downcast merge_condition right VARCHAR failed".to_string())?;
            Ok(left.value(0) > right.value(0))
        }
        DataType::Binary => {
            let left = left
                .as_any()
                .downcast_ref::<BinaryArray>()
                .ok_or_else(|| "downcast merge_condition left BINARY failed".to_string())?;
            let right = right
                .as_any()
                .downcast_ref::<BinaryArray>()
                .ok_or_else(|| "downcast merge_condition right BINARY failed".to_string())?;
            Ok(left.value(0) > right.value(0))
        }
        other => Err(format!(
            "unsupported merge_condition column type for partial update: {:?}",
            other
        )),
    }
}

fn native_writer_supports_column(column: &StarRocksColumnSchema) -> bool {
    let type_name = column.r#type.trim().to_ascii_uppercase();
    let base_type = type_name.split('(').next().unwrap_or(type_name.as_str());
    let type_supported = matches!(
        base_type,
        "TINYINT"
            | "SMALLINT"
            | "INT"
            | "BIGINT"
            | "LARGEINT"
            | "DECIMAL32"
            | "DECIMAL64"
            | "DECIMAL128"
            | "DECIMAL256"
            | "DATE"
            | "DATE_V2"
            | "DATETIME"
            | "DATETIME_V2"
            | "TIMESTAMP"
            | "FLOAT"
            | "DOUBLE"
            | "BOOLEAN"
            | "CHAR"
            | "VARCHAR"
            | "STRING"
            | "BINARY"
            | "VARBINARY"
            | "HLL"
            | "PERCENTILE"
            | "BITMAP"
            | "OBJECT"
            | "JSON"
            | "ARRAY"
            | "MAP"
            | "STRUCT"
    );
    if !type_supported {
        return false;
    }
    column
        .children_columns
        .iter()
        .all(native_writer_supports_column)
}

fn resolve_batch_write_format(
    requested: StarRocksWriteFormat,
    tablet_schema: &StarRocksTabletSchema,
) -> Result<StarRocksWriteFormat, String> {
    match requested {
        StarRocksWriteFormat::Native => {
            let unsupported = tablet_schema
                .column
                .iter()
                .any(|col| !native_writer_supports_column(col));
            if unsupported {
                let unsupported_columns = tablet_schema
                    .column
                    .iter()
                    .filter(|col| !native_writer_supports_column(col))
                    .map(|col| {
                        format!(
                            "{}:{}",
                            col.name.as_deref().unwrap_or("<unnamed>"),
                            col.r#type
                        )
                    })
                    .collect::<Vec<_>>()
                    .join(", ");
                Err(format!(
                    "native lake write does not support current schema columns: schema_id={:?}, unsupported=[{}]",
                    tablet_schema.id, unsupported_columns
                ))
            } else {
                Ok(StarRocksWriteFormat::Native)
            }
        }
        other => Ok(other),
    }
}

pub(crate) fn build_rowset_for_upsert_batch(
    ctx: &TabletWriteContext,
    batch: &RecordBatch,
    txn_id: i64,
    driver_id: i32,
    file_seq: u64,
    write_format: StarRocksWriteFormat,
    load_id: Option<&UniqueId>,
) -> Result<StorageRowset, String> {
    let write_format = resolve_batch_write_format(write_format, &ctx.tablet_schema)?;
    let sorted_batch = sort_batch_for_native_write(batch, &ctx.tablet_schema)?;
    let write_batch = filter_decimal_cast_overflow_rows(&sorted_batch, &ctx.tablet_schema)?;
    if write_batch.num_rows() == 0 {
        return Ok(StorageRowset {
            id: None,
            overlapped: Some(false),
            segments: Vec::new(),
            num_rows: Some(0),
            data_size: Some(0),
            delete_predicate: None,
            num_dels: Some(0),
            segment_size: Vec::new(),
            max_compact_input_rowset_id: None,
            version: None,
            del_files: Vec::new(),
            segment_encryption_metas: Vec::new(),
            next_compaction_offset: None,
            bundle_file_offsets: Vec::new(),
            shared_segments: Vec::new(),
            record_predicate: None,
            segment_metas: Vec::new(),
        });
    }

    let data_file_name = build_txn_data_file_name(
        ctx.tablet_id,
        txn_id,
        driver_id,
        file_seq,
        write_format,
        load_id,
    )?;
    let data_file_path = join_tablet_path(
        &ctx.tablet_root_path,
        &format!("{DATA_DIR}/{data_file_name}"),
    )?;
    let row_count = write_batch.num_rows() as i64;
    let (data_file_size, bundle_file_offsets, segment_metas) = match write_format {
        StarRocksWriteFormat::Native => {
            let segment_meta = build_single_segment_facts(&write_batch, &ctx.tablet_schema)?;
            let segment_bytes =
                build_starrocks_native_segment_bytes(&write_batch, &ctx.tablet_schema)?;
            let segment_size = segment_bytes.len() as u64;
            write_bytes(&data_file_path, segment_bytes)?;
            (segment_size, Vec::new(), vec![segment_meta])
        }
        StarRocksWriteFormat::Parquet => {
            let parquet_size = write_parquet_file(&data_file_path, &write_batch)?;
            (parquet_size, Vec::new(), Vec::new())
        }
    };
    Ok(StorageRowset {
        id: None,
        overlapped: Some(false),
        segments: vec![data_file_name],
        num_rows: Some(row_count),
        data_size: Some(data_file_size as i64),
        delete_predicate: None,
        num_dels: Some(0),
        segment_size: vec![data_file_size],
        max_compact_input_rowset_id: None,
        version: None,
        del_files: Vec::new(),
        segment_encryption_metas: Vec::new(),
        next_compaction_offset: None,
        bundle_file_offsets,
        shared_segments: vec![false],
        record_predicate: None,
        segment_metas,
    })
}

/// Builds the execution-owned facts for a lake upsert rowset.  Unlike the
/// legacy helper above this does not construct `StorageRowset`; compat
/// serializes these facts only after the transaction state machine is done.
pub(crate) fn build_rowset_facts_for_upsert_batch(
    ctx: &TabletWriteContext,
    batch: &RecordBatch,
    txn_id: i64,
    driver_id: i32,
    file_seq: u64,
    write_format: StarRocksWriteFormat,
    load_id: Option<&UniqueId>,
) -> Result<StorageRowset, String> {
    let write_format = resolve_batch_write_format(write_format, &ctx.tablet_schema)?;
    let sorted_batch = sort_batch_for_native_write(batch, &ctx.tablet_schema)?;
    let write_batch = filter_decimal_cast_overflow_rows(&sorted_batch, &ctx.tablet_schema)?;
    if write_batch.num_rows() == 0 {
        return Ok(StorageRowset {
            overlapped: Some(false),
            num_rows: Some(0),
            data_size: Some(0),
            num_dels: Some(0),
            ..StorageRowset::default()
        });
    }

    let data_file_name = build_txn_data_file_name(
        ctx.tablet_id,
        txn_id,
        driver_id,
        file_seq,
        write_format,
        load_id,
    )?;
    let data_file_path = join_tablet_path(
        &ctx.tablet_root_path,
        &format!("{DATA_DIR}/{data_file_name}"),
    )?;
    let row_count = write_batch.num_rows() as i64;
    let (data_file_size, bundle_file_offsets, segment_metas) = match write_format {
        StarRocksWriteFormat::Native => {
            let segment_meta = build_single_segment_facts(&write_batch, &ctx.tablet_schema)?;
            let segment_bytes =
                build_starrocks_native_segment_bytes(&write_batch, &ctx.tablet_schema)?;
            let segment_size = segment_bytes.len() as u64;
            write_bytes(&data_file_path, segment_bytes)?;
            (segment_size, Vec::new(), vec![segment_meta])
        }
        StarRocksWriteFormat::Parquet => {
            let parquet_size = write_parquet_file(&data_file_path, &write_batch)?;
            (parquet_size, Vec::new(), Vec::new())
        }
    };
    Ok(StorageRowset {
        overlapped: Some(false),
        segments: vec![data_file_name],
        num_rows: Some(row_count),
        data_size: Some(data_file_size as i64),
        num_dels: Some(0),
        segment_size: vec![data_file_size],
        bundle_file_offsets,
        shared_segments: vec![false],
        segment_metas,
        ..StorageRowset::default()
    })
}

/// Return 10^precision as i256. Used to check whether a DECIMAL256 unscaled value
/// exceeds the declared precision (valid range is |value| < 10^precision).
fn decimal256_max_unscaled(precision: u8) -> i256 {
    let mut result = i256::ONE;
    let ten = i256::from_i128(10);
    for _ in 0..precision {
        result = result.wrapping_mul(ten);
    }
    result
}

/// Return 10^exp as i128. Returns None if the result overflows i128.
fn pow10_i128(exp: u32) -> Option<i128> {
    let mut out: i128 = 1;
    for _ in 0..exp {
        out = out.checked_mul(10)?;
    }
    Some(out)
}

fn filter_decimal_cast_overflow_rows(
    batch: &RecordBatch,
    tablet_schema: &StarRocksTabletSchema,
) -> Result<RecordBatch, String> {
    if batch.num_rows() == 0 {
        return Ok(batch.clone());
    }
    if tablet_schema.keys_type == Some(StarRocksKeysType::Primary) {
        return Ok(batch.clone());
    }

    let mut rejected = vec![false; batch.num_rows()];
    let mut has_decimal_column = false;
    let column_count = batch.num_columns().min(tablet_schema.column.len());

    for column_idx in 0..column_count {
        let schema_col = &tablet_schema.column[column_idx];
        let type_name = schema_col.r#type.trim().to_ascii_uppercase();
        if !type_name.starts_with("DECIMAL") {
            continue;
        }
        if !schema_col.is_nullable.unwrap_or(true) {
            continue;
        }
        has_decimal_column = true;

        let precision_i32 = schema_col.precision.ok_or_else(|| {
            format!(
                "missing decimal precision while filtering overflow rows: column_index={}, column_name={}",
                column_idx,
                schema_col.name.as_deref().unwrap_or("")
            )
        })?;
        let scale_i32 = schema_col.frac.ok_or_else(|| {
            format!(
                "missing decimal scale while filtering overflow rows: column_index={}, column_name={}",
                column_idx,
                schema_col.name.as_deref().unwrap_or("")
            )
        })?;
        let precision = u8::try_from(precision_i32).map_err(|_| {
            format!(
                "decimal precision overflow while filtering overflow rows: column_index={}, column_name={}, precision={}",
                column_idx,
                schema_col.name.as_deref().unwrap_or(""),
                precision_i32
            )
        })?;
        let scale = i8::try_from(scale_i32).map_err(|_| {
            format!(
                "decimal scale overflow while filtering overflow rows: column_index={}, column_name={}, scale={}",
                column_idx,
                schema_col.name.as_deref().unwrap_or(""),
                scale_i32
            )
        })?;

        let target_type = if precision > 38 {
            DataType::Decimal256(precision, scale)
        } else {
            DataType::Decimal128(precision, scale)
        };
        let source = batch.column(column_idx);

        // For DECIMAL256 columns, Arrow's cast is a no-op when the source type already
        // matches the target (same precision and scale). In this case, values that exceed
        // the declared precision (but fit in i256) would not be detected by the cast-based
        // approach. Directly validate each non-null i256 value against the precision bound.
        if precision > 38
            && let Some(dec256) = source.as_any().downcast_ref::<Decimal256Array>()
        {
            let max_unscaled = decimal256_max_unscaled(precision);
            for (row, is_rejected) in rejected.iter_mut().enumerate().take(batch.num_rows()) {
                if !dec256.is_null(row) {
                    let val = dec256.value(row);
                    let abs_val = val.wrapping_abs();
                    if abs_val >= max_unscaled {
                        *is_rejected = true;
                    }
                }
            }
            continue;
        }

        // For DECIMAL128 columns, Arrow's cast is a no-op when source and target types match
        // (same precision and scale), so it cannot detect values that exceed the declared
        // precision.  Directly validate each non-null i128 value against the precision bound.
        if precision <= 38
            && let Some(dec128) = source.as_any().downcast_ref::<Decimal128Array>()
        {
            let max_unscaled = pow10_i128(u32::from(precision)).unwrap_or(i128::MAX);
            for (row, is_rejected) in rejected.iter_mut().enumerate().take(batch.num_rows()) {
                if !dec128.is_null(row) {
                    let abs_val = dec128.value(row).unsigned_abs();
                    if abs_val >= max_unscaled as u128 {
                        *is_rejected = true;
                    }
                }
            }
            continue;
        }

        let casted = cast(source.as_ref(), &target_type).map_err(|e| {
            format!(
                "cast decimal column while filtering overflow rows failed: column_index={}, column_name={}, target={:?}, error={}",
                column_idx,
                schema_col.name.as_deref().unwrap_or(""),
                target_type,
                e
            )
        })?;

        for (row, is_rejected) in rejected.iter_mut().enumerate().take(batch.num_rows()) {
            if !source.is_null(row) && casted.is_null(row) {
                *is_rejected = true;
            }
        }
    }

    if !has_decimal_column {
        return Ok(batch.clone());
    }

    let kept = rejected.iter().filter(|v| !**v).count();
    if kept == batch.num_rows() {
        return Ok(batch.clone());
    }
    if kept == 0 {
        let empty_indices = UInt32Array::from(Vec::<u32>::new());
        let mut columns = Vec::with_capacity(batch.num_columns());
        for (column_idx, array) in batch.columns().iter().enumerate() {
            let taken = take(array.as_ref(), &empty_indices, None).map_err(|e| {
                format!(
                    "take empty rows while filtering overflow rows failed: column_index={}, error={}",
                    column_idx, e
                )
            })?;
            columns.push(taken);
        }
        return RecordBatch::try_new(batch.schema(), columns)
            .map_err(|e| format!("build empty filtered record batch failed: {e}"));
    }

    let mut kept_indices = Vec::with_capacity(kept);
    for (row, rejected_row) in rejected.into_iter().enumerate() {
        if !rejected_row {
            kept_indices.push(row as u32);
        }
    }
    let index_array = UInt32Array::from(kept_indices);
    let mut columns = Vec::with_capacity(batch.num_columns());
    for (column_idx, array) in batch.columns().iter().enumerate() {
        let taken = take(array.as_ref(), &index_array, None).map_err(|e| {
            format!(
                "take filtered rows failed: column_index={}, error={}",
                column_idx, e
            )
        })?;
        columns.push(taken);
    }
    RecordBatch::try_new(batch.schema(), columns)
        .map_err(|e| format!("build filtered record batch failed: {e}"))
}

fn build_txn_delete_file_name(
    tablet_id: i64,
    txn_id: i64,
    driver_id: i32,
    file_seq: u64,
    load_id: Option<&UniqueId>,
) -> Result<String, String> {
    let seed_name = build_txn_data_file_name(
        tablet_id,
        txn_id,
        driver_id,
        file_seq,
        StarRocksWriteFormat::Native,
        load_id,
    )?;
    if let Some(prefix) = seed_name.strip_suffix(".dat") {
        return Ok(format!("{prefix}.del"));
    }
    Ok(format!("{seed_name}.del"))
}

fn encode_delete_keys_file_payload(
    batch: &RecordBatch,
    tablet_schema: &StarRocksTabletSchema,
) -> Result<Vec<u8>, String> {
    let key_col_count = tablet_schema
        .column
        .iter()
        .filter(|col| col.is_key.unwrap_or(false))
        .count();
    if key_col_count == 0 {
        return Err(format!(
            "invalid tablet schema for delete-key payload encoding: no key columns (schema_id={:?})",
            tablet_schema.id
        ));
    }
    if batch.num_columns() != key_col_count {
        return Err(format!(
            "delete-key batch column count mismatch: expected_key_columns={} actual_columns={}",
            key_col_count,
            batch.num_columns()
        ));
    }

    crate::connector::starrocks::lake::delete_payload_codec::encode_delete_keys_payload(batch)
}

fn build_storage_table_schema_key(ctx: &TabletWriteContext) -> Result<StorageSchemaKey, String> {
    if ctx.db_id <= 0 {
        return Err(format!("invalid db_id for lake write: {}", ctx.db_id));
    }
    let schema_id = ctx
        .tablet_schema
        .id
        .filter(|value| *value > 0)
        .ok_or_else(|| "tablet schema id is missing".to_string())?;
    Ok(StorageSchemaKey {
        db_id: Some(ctx.db_id),
        table_id: Some(ctx.table_id),
        schema_id: Some(schema_id),
    })
}

fn ensure_storage_table_schema_key_equals(
    actual: &StorageSchemaKey,
    expected: &StorageSchemaKey,
) -> Result<(), String> {
    if actual != expected {
        return Err(format!(
            "txn log schema_key mismatch: expected=(db_id={:?}, table_id={:?}, schema_id={:?}) actual=(db_id={:?}, table_id={:?}, schema_id={:?})",
            expected.db_id,
            expected.table_id,
            expected.schema_id,
            actual.db_id,
            actual.table_id,
            actual.schema_id
        ));
    }
    Ok(())
}

fn normalize_storage_rowset_shared_segments(rowset: &mut StorageRowset) {
    let segment_count = rowset.segments.len();
    rowset.shared_segments.resize(segment_count, false);
}

fn ensure_storage_rowset_segment_meta_consistency(rowset: &StorageRowset) -> Result<(), String> {
    if !rowset.segment_metas.is_empty() && rowset.segment_metas.len() != rowset.segments.len() {
        return Err(format!(
            "rowset segment_metas/segments length mismatch: segment_metas={} segments={}",
            rowset.segment_metas.len(),
            rowset.segments.len()
        ));
    }
    Ok(())
}

fn merge_storage_rowset(
    target: &mut StorageRowset,
    incoming: &StorageRowset,
) -> Result<(), String> {
    ensure_storage_rowset_segment_meta_consistency(target)?;
    ensure_storage_rowset_segment_meta_consistency(incoming)?;
    normalize_storage_rowset_shared_segments(target);
    let mut incoming = incoming.clone();
    normalize_storage_rowset_shared_segments(&mut incoming);

    let existing_segments = target.segments.iter().collect::<HashSet<_>>();
    let duplicate_count = incoming
        .segments
        .iter()
        .filter(|segment| existing_segments.contains(segment))
        .count();
    if duplicate_count > 0 {
        if duplicate_count == incoming.segments.len() {
            return Ok(());
        }
        return Err(format!(
            "detected partial duplicate segments while merging txn rowset: duplicate_count={} incoming_segments={} existing_segments={}",
            duplicate_count,
            incoming.segments.len(),
            target.segments.len()
        ));
    }

    let incoming_num_rows = incoming.num_rows.unwrap_or(0);
    let incoming_data_size = incoming.data_size.unwrap_or(0);
    let incoming_num_dels = incoming.num_dels.unwrap_or(0);
    let incoming_overlapped = incoming.overlapped.unwrap_or(false);

    target.segments.extend(incoming.segments);
    target.segment_size.extend(incoming.segment_size);
    target
        .segment_encryption_metas
        .extend(incoming.segment_encryption_metas);
    target
        .bundle_file_offsets
        .extend(incoming.bundle_file_offsets);
    target.segment_metas.extend(incoming.segment_metas);
    target.shared_segments.extend(incoming.shared_segments);
    normalize_storage_rowset_shared_segments(target);
    ensure_storage_rowset_segment_meta_consistency(target)?;
    target.num_rows = Some(
        target
            .num_rows
            .unwrap_or(0)
            .saturating_add(incoming_num_rows),
    );
    target.data_size = Some(
        target
            .data_size
            .unwrap_or(0)
            .saturating_add(incoming_data_size),
    );
    target.num_dels = Some(
        target
            .num_dels
            .unwrap_or(0)
            .saturating_add(incoming_num_dels),
    );
    target.overlapped = Some(
        target.overlapped.unwrap_or(false) || incoming_overlapped || target.segments.len() > 1,
    );
    Ok(())
}

fn upsert_storage_write_rowset_in_txn_log(
    txn_log: &mut StorageTransactionLog,
    tablet_id: i64,
    txn_id: i64,
    partition_id: i64,
    incoming_rowset: &StorageRowset,
    incoming_dels: &[String],
    expected_load_id: Option<&UniqueId>,
    expected_schema_key: &StorageSchemaKey,
) -> Result<(), String> {
    if txn_log.tablet_id != Some(tablet_id) || txn_log.txn_id != Some(txn_id) {
        return Err(format!(
            "txn log identity mismatch: expected tablet={} txn={} actual tablet={:?} txn={:?}",
            tablet_id, txn_id, txn_log.tablet_id, txn_log.txn_id
        ));
    }
    if txn_log.compaction.is_some()
        || txn_log.schema_change.is_some()
        || txn_log.alter_metadata.is_some()
        || txn_log.replication.is_some()
    {
        return Err(format!(
            "unsupported mixed txn log operation for tablet={} txn={}",
            tablet_id, txn_id
        ));
    }
    if let Some(load_id) = expected_load_id {
        let expected = (load_id.high(), load_id.low());
        if let Some(actual) = txn_log.load_id
            && actual != expected
        {
            return Err(format!(
                "txn log load_id mismatch for tablet={} txn={}: expected=({}, {}) actual=({}, {})",
                tablet_id, txn_id, expected.0, expected.1, actual.0, actual.1
            ));
        }
        txn_log.load_id = Some(expected);
    }
    let write = txn_log.write.get_or_insert_with(|| StorageWriteOperation {
        schema_key: Some(expected_schema_key.clone()),
        ..StorageWriteOperation::default()
    });
    if let Some(actual) = write.schema_key.as_ref() {
        ensure_storage_table_schema_key_equals(actual, expected_schema_key)?;
    } else {
        write.schema_key = Some(expected_schema_key.clone());
    }
    match write.rowset.as_mut() {
        Some(existing) => merge_storage_rowset(existing, incoming_rowset)?,
        None => write.rowset = Some(incoming_rowset.clone()),
    }
    if write.del_encryption_metas.is_empty() {
        write.del_encryption_metas = vec![Vec::new(); write.dels.len()];
    } else if write.del_encryption_metas.len() != write.dels.len() {
        return Err(format!(
            "txn log op_write dels/encryption metas length mismatch for tablet={} txn={}: dels={} del_encryption_metas={}",
            tablet_id,
            txn_id,
            write.dels.len(),
            write.del_encryption_metas.len()
        ));
    }
    for name in incoming_dels {
        if !write.dels.iter().any(|existing| existing == name) {
            write.dels.push(name.clone());
            write.del_encryption_metas.push(Vec::new());
        }
    }
    if partition_id > 0 {
        txn_log.partition_id = Some(partition_id);
    }
    Ok(())
}

pub(crate) fn write_txn_log_file(
    provider: &dyn crate::connector::starrocks::ports::StorageMetadataProvider,
    path: &str,
    txn_log: &StorageTransactionLog,
) -> Result<(), String> {
    write_storage_txn_log(provider, path, txn_log)
}

pub(crate) fn read_txn_log_if_exists(
    provider: &dyn crate::connector::starrocks::ports::StorageMetadataProvider,
    path: &str,
) -> Result<Option<StorageTransactionLog>, String> {
    read_storage_txn_log_if_exists(provider, path)
}

pub(crate) fn read_combined_txn_log_if_exists(
    provider: &dyn crate::connector::starrocks::ports::StorageMetadataProvider,
    path: &str,
) -> Result<Option<StorageCombinedTransactionLog>, String> {
    let maybe_bytes = read_bytes_if_exists(path)?;
    let Some(bytes) = maybe_bytes else {
        return Ok(None);
    };
    provider.decode_combined_transaction_log(&bytes).map(Some)
}

fn unix_millis_now() -> i64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    let Ok(duration) = SystemTime::now().duration_since(UNIX_EPOCH) else {
        return 0;
    };
    i64::try_from(duration.as_millis()).unwrap_or(i64::MAX)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;

    #[derive(Default)]
    struct RecordingStorageMetadataProvider {
        encoded_logs: Mutex<Vec<StorageTransactionLog>>,
    }

    impl crate::connector::starrocks::ports::StorageMetadataProvider
        for RecordingStorageMetadataProvider
    {
        fn encode_tablet_schema(
            &self,
            _: &crate::connector::starrocks::schema::StarRocksTabletSchema,
        ) -> Result<Vec<u8>, String> {
            unreachable!("the full-upsert test only writes transaction logs")
        }

        fn decode_tablet_schema(
            &self,
            _: &[u8],
        ) -> Result<crate::connector::starrocks::schema::StarRocksTabletSchema, String> {
            unreachable!("the full-upsert test only writes transaction logs")
        }

        fn decode_tablet_metadata(
            &self,
            _: &[u8],
        ) -> Result<crate::connector::starrocks::lake::storage_domain::StorageTabletMetadata, String>
        {
            unreachable!("the full-upsert test only writes transaction logs")
        }

        fn encode_tablet_metadata(
            &self,
            _: &crate::connector::starrocks::lake::storage_domain::StorageTabletMetadata,
        ) -> Result<Vec<u8>, String> {
            unreachable!("the full-upsert test only writes transaction logs")
        }

        fn decode_bundle_metadata(
            &self,
            _: &[u8],
        ) -> Result<crate::connector::starrocks::lake::storage_domain::StorageBundleMetadata, String>
        {
            unreachable!("the full-upsert test only writes transaction logs")
        }

        fn decode_bundle_file(
            &self,
            _: &[u8],
        ) -> Result<crate::connector::starrocks::lake::storage_domain::StorageBundleFile, String>
        {
            unreachable!("the full-upsert test only writes transaction logs")
        }

        fn encode_bundle_file(
            &self,
            _: &crate::connector::starrocks::lake::storage_domain::StorageBundleFile,
        ) -> Result<Vec<u8>, String> {
            unreachable!("the full-upsert test only writes transaction logs")
        }

        fn rewrite_tablet_metadata_version(&self, _: &[u8], _: i64) -> Result<Vec<u8>, String> {
            unreachable!("the full-upsert test only writes transaction logs")
        }

        fn decode_transaction_log(&self, _: &[u8]) -> Result<StorageTransactionLog, String> {
            unreachable!("the first full-upsert test write has no existing transaction log")
        }

        fn encode_transaction_log(&self, log: &StorageTransactionLog) -> Result<Vec<u8>, String> {
            self.encoded_logs.lock().unwrap().push(log.clone());
            Ok(vec![1])
        }

        fn decode_combined_transaction_log(
            &self,
            _: &[u8],
        ) -> Result<
            crate::connector::starrocks::lake::storage_domain::StorageCombinedTransactionLog,
            String,
        > {
            unreachable!("the full-upsert test only writes transaction logs")
        }

        fn encode_combined_transaction_log(
            &self,
            _: &crate::connector::starrocks::lake::storage_domain::StorageCombinedTransactionLog,
        ) -> Result<Vec<u8>, String> {
            unreachable!("the full-upsert test only writes transaction logs")
        }
    }

    fn storage_rowset(segment: &str, rows: i64) -> StorageRowset {
        StorageRowset {
            overlapped: Some(false),
            segments: vec![segment.to_string()],
            num_rows: Some(rows),
            data_size: Some(rows * 10),
            num_dels: Some(0),
            segment_size: vec![u64::try_from(rows * 10).unwrap()],
            ..StorageRowset::default()
        }
    }

    #[test]
    fn storage_txn_log_upsert_merges_facts_and_keeps_duplicate_retry_idempotent() {
        let schema_key = StorageSchemaKey {
            db_id: Some(10),
            table_id: Some(20),
            schema_id: Some(30),
        };
        let mut log = StorageTransactionLog {
            tablet_id: Some(40),
            txn_id: Some(50),
            ..StorageTransactionLog::default()
        };

        upsert_storage_write_rowset_in_txn_log(
            &mut log,
            40,
            50,
            60,
            &storage_rowset("a.dat", 2),
            &[],
            None,
            &schema_key,
        )
        .unwrap();
        upsert_storage_write_rowset_in_txn_log(
            &mut log,
            40,
            50,
            60,
            &storage_rowset("b.dat", 3),
            &[],
            None,
            &schema_key,
        )
        .unwrap();
        upsert_storage_write_rowset_in_txn_log(
            &mut log,
            40,
            50,
            60,
            &storage_rowset("b.dat", 3),
            &[],
            None,
            &schema_key,
        )
        .unwrap();

        let write = log.write.as_ref().unwrap();
        let rowset = write.rowset.as_ref().unwrap();
        assert_eq!(rowset.segments, vec!["a.dat", "b.dat"]);
        assert_eq!(rowset.num_rows, Some(5));
        assert_eq!(rowset.data_size, Some(50));
        assert_eq!(log.partition_id, Some(60));
        assert_eq!(write.schema_key.as_ref(), Some(&schema_key));
    }

    #[test]
    fn storage_txn_log_upsert_rejects_partial_duplicate_segments() {
        let schema_key = StorageSchemaKey {
            db_id: Some(10),
            table_id: Some(20),
            schema_id: Some(30),
        };
        let mut log = StorageTransactionLog {
            tablet_id: Some(40),
            txn_id: Some(50),
            ..StorageTransactionLog::default()
        };
        upsert_storage_write_rowset_in_txn_log(
            &mut log,
            40,
            50,
            60,
            &storage_rowset("a.dat", 2),
            &[],
            None,
            &schema_key,
        )
        .unwrap();
        let mut mixed = storage_rowset("a.dat", 2);
        mixed.segments.push("b.dat".to_string());
        mixed.segment_size.push(20);
        let error = upsert_storage_write_rowset_in_txn_log(
            &mut log,
            40,
            50,
            60,
            &mixed,
            &[],
            None,
            &schema_key,
        )
        .unwrap_err();
        assert!(error.contains("partial duplicate segments"));
    }

    #[test]
    fn full_schema_upsert_uses_the_storage_metadata_provider_for_txn_log_bytes() {
        use arrow::datatypes::{DataType, Field};

        let root = std::env::temp_dir().join(format!(
            "novarocks-storage-domain-upsert-{}",
            std::process::id()
        ));
        std::fs::create_dir_all(&root).unwrap();
        let provider = Arc::new(RecordingStorageMetadataProvider::default());
        let context = TabletWriteContext {
            db_id: 10,
            table_id: 20,
            tablet_id: 30,
            tablet_root_path: root.to_string_lossy().into_owned(),
            tablet_schema: StarRocksTabletSchema {
                id: Some(40),
                keys_type: Some(StarRocksKeysType::Duplicate),
                column: vec![StarRocksColumnSchema {
                    unique_id: 1,
                    name: Some("k".to_string()),
                    r#type: "INT".to_string(),
                    is_key: Some(true),
                    ..StarRocksColumnSchema::default()
                }],
                sort_key_idxes: vec![0],
                ..StarRocksTabletSchema::default()
            },
            s3_config: None,
            storage_metadata_provider: Some(provider.clone()),
            partial_update: Default::default(),
        };
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new("k", DataType::Int32, false)])),
            vec![Arc::new(Int32Array::from(vec![7]))],
        )
        .unwrap();

        append_lake_txn_log_with_rowset_impl(
            &context,
            &batch,
            None,
            50,
            0,
            0,
            StarRocksWriteFormat::Native,
            60,
            None,
        )
        .unwrap();

        let encoded = provider.encoded_logs.lock().unwrap();
        assert_eq!(encoded.len(), 1);
        assert_eq!(encoded[0].tablet_id, Some(30));
        assert_eq!(encoded[0].txn_id, Some(50));
        assert_eq!(
            encoded[0]
                .write
                .as_ref()
                .and_then(|write| write.rowset.as_ref())
                .map(|rowset| rowset.num_rows),
            Some(Some(1))
        );
        drop(encoded);

        append_lake_txn_log_empty_rowset(&context, 51, 60, None).unwrap();
        let encoded = provider.encoded_logs.lock().unwrap();
        assert_eq!(encoded.len(), 2);
        assert_eq!(encoded[1].txn_id, Some(51));
        assert_eq!(
            encoded[1]
                .write
                .as_ref()
                .and_then(|write| write.rowset.as_ref())
                .map(|rowset| rowset.num_rows),
            Some(Some(0))
        );
        drop(encoded);
        std::fs::remove_dir_all(root).unwrap();
    }
}
