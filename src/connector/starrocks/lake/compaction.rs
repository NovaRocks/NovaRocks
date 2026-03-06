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

use arrow::record_batch::RecordBatch;

use crate::connector::starrocks::lake::context::{TabletWriteContext, with_txn_log_append_lock};
use crate::connector::starrocks::lake::transactions::resolve_tablet_location;
use crate::connector::starrocks::lake::txn_log::{
    build_metadata_object_store_profile_for_partial, build_rowset_for_upsert_batch,
    build_tablet_output_schema, write_txn_log_file,
};
use crate::formats::starrocks::metadata::{load_bundle_segment_footers, load_tablet_snapshot};
use crate::formats::starrocks::plan::build_native_read_plan;
use crate::formats::starrocks::reader::build_native_record_batch;
use crate::formats::starrocks::writer::StarRocksWriteFormat;
use crate::formats::starrocks::writer::bundle_meta::load_tablet_metadata_at_version;
use crate::formats::starrocks::writer::parquet::read_bundle_parquet_snapshot_if_any;
use crate::novarocks_logging::warn;
use crate::service::grpc_client::proto::starrocks::{
    AbortCompactionRequest, AbortCompactionResponse, CompactRequest, CompactResponse, CompactStat,
    KeysType, StatusPb, TxnLogPb, txn_log_pb,
};

const STATUS_CODE_OK: i32 = 0;

pub(crate) fn compact(request: &CompactRequest) -> Result<CompactResponse, String> {
    let txn_id = request
        .txn_id
        .ok_or_else(|| "compact missing txn_id".to_string())?;
    if txn_id <= 0 {
        return Err(format!("compact has non-positive txn_id={txn_id}"));
    }
    let version = request
        .version
        .ok_or_else(|| "compact missing version".to_string())?;
    if version <= 0 {
        return Err(format!("compact has non-positive version={version}"));
    }
    if request.tablet_ids.is_empty() {
        return Err("compact missing tablet_ids".to_string());
    }

    let skip_write_txnlog = request.skip_write_txnlog.unwrap_or(false);
    let mut failed_tablets = Vec::new();
    let mut compact_stats = Vec::new();
    let mut txn_logs = Vec::new();
    let mut success_compaction_input_file_size = 0_i64;

    for tablet_id in &request.tablet_ids {
        match compact_one_tablet(*tablet_id, txn_id, version, skip_write_txnlog) {
            Ok(result) => {
                success_compaction_input_file_size = success_compaction_input_file_size
                    .saturating_add(result.success_compaction_input_file_size);
                compact_stats.push(result.stat);
                if skip_write_txnlog {
                    txn_logs.push(result.txn_log);
                }
            }
            Err(err) => {
                warn!(
                    "compact failed: tablet_id={}, txn_id={}, version={}, error={}",
                    tablet_id, txn_id, version, err
                );
                failed_tablets.push(*tablet_id);
            }
        }
    }

    Ok(CompactResponse {
        failed_tablets,
        status: Some(StatusPb {
            status_code: STATUS_CODE_OK,
            error_msgs: Vec::new(),
        }),
        compact_stats,
        success_compaction_input_file_size: Some(success_compaction_input_file_size),
        txn_logs,
        subtask_statuses: Vec::new(),
    })
}

pub(crate) fn abort_compaction(
    request: &AbortCompactionRequest,
) -> Result<AbortCompactionResponse, String> {
    let txn_id = request
        .txn_id
        .ok_or_else(|| "abort_compaction missing txn_id".to_string())?;
    if txn_id <= 0 {
        return Err(format!("abort_compaction has non-positive txn_id={txn_id}"));
    }
    Ok(AbortCompactionResponse {
        status: Some(StatusPb {
            status_code: STATUS_CODE_OK,
            error_msgs: Vec::new(),
        }),
    })
}

struct CompactOneTabletOutput {
    txn_log: TxnLogPb,
    stat: CompactStat,
    success_compaction_input_file_size: i64,
}

fn compact_one_tablet(
    tablet_id: i64,
    txn_id: i64,
    version: i64,
    skip_write_txnlog: bool,
) -> Result<CompactOneTabletOutput, String> {
    if tablet_id <= 0 {
        return Err(format!("compact has non-positive tablet_id={tablet_id}"));
    }

    let (tablet_root_path, s3_config) = resolve_tablet_location("compact", tablet_id)?;
    let metadata = load_tablet_metadata_at_version(&tablet_root_path, tablet_id, version)?
        .ok_or_else(|| {
            format!(
                "compact metadata not found for tablet_id={} version={}",
                tablet_id, version
            )
        })?;
    let object_store_profile =
        build_metadata_object_store_profile_for_partial(&tablet_root_path, s3_config.as_ref())?;
    let snapshot = load_tablet_snapshot(
        tablet_id,
        version,
        &tablet_root_path,
        object_store_profile.as_ref(),
    )?;
    let tablet_schema = snapshot.tablet_schema.clone();
    if tablet_schema.keys_type == Some(KeysType::PrimaryKeys as i32) {
        return Err(format!(
            "compact does not support PRIMARY_KEYS yet: tablet_id={tablet_id}"
        ));
    }

    let input_rowsets = metadata
        .rowsets
        .iter()
        .map(|rowset| {
            rowset.id.ok_or_else(|| {
                format!(
                    "compact rowset id is missing: tablet_id={} version={} rowset_count={}",
                    tablet_id,
                    version,
                    metadata.rowsets.len()
                )
            })
        })
        .collect::<Result<Vec<_>, String>>()?;
    let read_segment_count = metadata
        .rowsets
        .iter()
        .map(|rowset| rowset.segments.len() as i64)
        .sum::<i64>();
    let success_compaction_input_file_size = metadata
        .rowsets
        .iter()
        .map(|rowset| rowset.data_size.unwrap_or(0).max(0))
        .sum::<i64>();

    let mut output_rowset = None;
    let mut write_segment_count = 0_i64;
    let mut write_segment_bytes = 0_i64;

    if input_rowsets.len() > 1 {
        let output_schema = build_tablet_output_schema(&tablet_schema)?;
        let batch = load_compaction_visible_batch(
            &snapshot,
            &tablet_root_path,
            object_store_profile.as_ref(),
            &output_schema,
        )?;
        if batch.num_rows() > 0 {
            let ctx = TabletWriteContext {
                db_id: 0,
                table_id: 0,
                tablet_id,
                tablet_root_path: tablet_root_path.clone(),
                tablet_schema: tablet_schema.clone(),
                s3_config,
                partial_update: Default::default(),
            };
            let new_rowset = build_rowset_for_upsert_batch(
                &ctx,
                &batch,
                txn_id,
                0,
                0,
                StarRocksWriteFormat::Native,
                None,
            )?;
            write_segment_count = new_rowset.segments.len() as i64;
            write_segment_bytes = new_rowset.data_size.unwrap_or(0).max(0);
            output_rowset = Some(new_rowset);
        }
    }

    let txn_log =
        build_compaction_txn_log(tablet_id, txn_id, version, &input_rowsets, output_rowset);
    if !skip_write_txnlog {
        let path = crate::formats::starrocks::writer::layout::txn_log_file_path(
            &tablet_root_path,
            tablet_id,
            txn_id,
        )?;
        with_txn_log_append_lock(tablet_id, txn_id, || write_txn_log_file(&path, &txn_log))?;
    }

    Ok(CompactOneTabletOutput {
        txn_log,
        stat: CompactStat {
            tablet_id: Some(tablet_id),
            read_time_remote: Some(0),
            read_bytes_remote: Some(0),
            read_time_local: Some(0),
            read_bytes_local: Some(0),
            total_compact_input_file_size: Some(success_compaction_input_file_size),
            read_segment_count: Some(read_segment_count),
            write_segment_count: Some(write_segment_count),
            write_segment_bytes: Some(write_segment_bytes),
            write_time_remote: Some(0),
            sub_task_count: Some(1),
            in_queue_time_sec: Some(0),
        },
        success_compaction_input_file_size,
    })
}

fn load_compaction_visible_batch(
    snapshot: &crate::formats::starrocks::metadata::StarRocksTabletSnapshot,
    tablet_root_path: &str,
    object_store_profile: Option<&crate::connector::starrocks::ObjectStoreProfile>,
    output_schema: &arrow::datatypes::SchemaRef,
) -> Result<RecordBatch, String> {
    if let Some(batch) = read_bundle_parquet_snapshot_if_any(snapshot, output_schema.clone())? {
        return Ok(batch);
    }
    if snapshot.segment_files.is_empty() {
        return Ok(RecordBatch::new_empty(output_schema.clone()));
    }
    let segment_footers =
        load_bundle_segment_footers(snapshot, tablet_root_path, object_store_profile)?;
    let plan = build_native_read_plan(snapshot, &segment_footers, output_schema)?;
    build_native_record_batch(
        &plan,
        &segment_footers,
        tablet_root_path,
        object_store_profile,
        output_schema,
        &[],
    )
}

fn build_compaction_txn_log(
    tablet_id: i64,
    txn_id: i64,
    version: i64,
    input_rowsets: &[u32],
    output_rowset: Option<crate::service::grpc_client::proto::starrocks::RowsetMetadataPb>,
) -> TxnLogPb {
    let new_segment_count = output_rowset
        .as_ref()
        .map(|rowset| i32::try_from(rowset.segments.len()).unwrap_or(i32::MAX))
        .unwrap_or(0);
    TxnLogPb {
        tablet_id: Some(tablet_id),
        txn_id: Some(txn_id),
        op_write: None,
        op_compaction: Some(txn_log_pb::OpCompaction {
            input_rowsets: input_rowsets.to_vec(),
            output_rowset,
            input_sstables: Vec::new(),
            output_sstable: None,
            compact_version: Some(version),
            new_segment_offset: Some(0),
            new_segment_count: Some(new_segment_count),
            ssts: Vec::new(),
        }),
        op_schema_change: None,
        op_alter_metadata: None,
        op_replication: None,
        partition_id: None,
        load_id: None,
    }
}
