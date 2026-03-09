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

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::thread::sleep;
use std::time::Duration;

use arrow::array::{Array, ArrayRef, BooleanArray, UInt32Array, new_empty_array, new_null_array};
use arrow::compute::{cast, filter_record_batch, take};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;

use crate::agent_service::{
    TAlterJobType, TAlterMaterializedViewParam, TAlterTabletReqV2, TCompactionStrategy,
    TPersistentIndexType, TTabletMetaInfo, TTabletType, TUpdateTabletMetaInfoReq,
};
use crate::common::ids::SlotId;
use crate::connector::starrocks::lake::context::{
    PartialUpdateWritePolicy, TabletWriteContext, get_tablet_runtime, register_tablet_runtime,
    update_tablet_runtime_schema, with_txn_log_append_lock,
};
use crate::connector::starrocks::lake::schema::build_tablet_schema_pb_from_thrift;
use crate::connector::starrocks::lake::txn_log::{
    build_tablet_output_schema, load_rowset_batch_for_partial_update,
    parse_default_literal_to_singleton_array, read_txn_log_if_exists, write_txn_log_file,
};
use crate::exec::chunk::{Chunk, field_with_slot_id};
use crate::exec::expr::ExprArena;
use crate::formats::starrocks::writer::bundle_meta::{
    empty_tablet_metadata, load_tablet_metadata_at_version, write_initial_meta_file,
    write_standalone_meta_file,
};
use crate::formats::starrocks::writer::io::{read_bytes_if_exists, write_bytes};
use crate::formats::starrocks::writer::layout::{
    DATA_DIR, initial_meta_file_path, join_tablet_path, standalone_meta_file_path,
    txn_log_file_path,
};
use crate::formats::starrocks::writer::{
    StarRocksWriteFormat, build_single_segment_metadata, build_starrocks_native_segment_bytes,
    build_txn_data_file_name, sort_batch_for_native_write,
};
use crate::lower::expr::lower_t_expr;
use crate::lower::layout::{Layout, normalize_slot_name};
use crate::runtime::starlet_shard_registry::{self, S3StoreConfig};
use crate::service::grpc_client::proto::starrocks::{
    CompactionStrategyPb, FlatJsonConfigPb, KeysType, MetadataUpdateInfoPb, PersistentIndexTypePb,
    RowsetMetadataPb, TabletMetadataPb, TabletSchemaPb, TxnLogPb, txn_log_pb,
};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum AlterMode {
    SchemaChange,
    Rollup,
}

const ALTER_METADATA_LOAD_MAX_ATTEMPTS: usize = 30;
const ALTER_METADATA_LOAD_RETRY_INTERVAL_MS: u64 = 100;

pub(crate) fn execute_alter_tablet_task(request: &TAlterTabletReqV2) -> Result<(), String> {
    let alter_mode = validate_schema_change_request(request)?;

    let base_tablet_id = request.base_tablet_id;
    let new_tablet_id = request.new_tablet_id;
    let alter_version = request
        .alter_version
        .ok_or_else(|| "alter task missing alter_version".to_string())?;
    if alter_version <= 0 {
        return Err(format!(
            "alter task has invalid alter_version={alter_version}"
        ));
    }
    let txn_id = request
        .txn_id
        .ok_or_else(|| "alter task missing txn_id".to_string())?;
    if txn_id <= 0 {
        return Err(format!("alter task has invalid txn_id={txn_id}"));
    }

    let materialized_view_param_summary = request
        .materialized_view_params
        .as_ref()
        .map(|params| {
            params
                .iter()
                .map(|param| {
                    format!(
                        "{}=>origin={:?},mv_expr={}",
                        param.column_name,
                        param.origin_column_name.as_deref(),
                        param.mv_expr.is_some()
                    )
                })
                .collect::<Vec<_>>()
                .join("; ")
        })
        .unwrap_or_default();
    let slot_desc_summary = request
        .desc_tbl
        .as_ref()
        .and_then(|tbl| tbl.slot_descriptors.as_ref())
        .map(|slots| {
            slots
                .iter()
                .map(|slot| {
                    format!(
                        "slot_id={:?},col={:?},physical={:?},parent={:?}",
                        slot.id, slot.col_name, slot.col_physical_name, slot.parent
                    )
                })
                .collect::<Vec<_>>()
                .join("; ")
        })
        .unwrap_or_default();

    tracing::info!(
        alter_mode = ?alter_mode,
        base_tablet_id,
        new_tablet_id,
        base_schema_hash = request.base_schema_hash,
        new_schema_hash = request.new_schema_hash,
        alter_version,
        txn_id,
        columns_len = request.columns.as_ref().map(|v| v.len()).unwrap_or(0),
        base_table_column_names_len = request
            .base_table_column_names
            .as_ref()
            .map(|v| v.len())
            .unwrap_or(0),
        materialized_view_param_count = request
            .materialized_view_params
            .as_ref()
            .map(|v| v.len())
            .unwrap_or(0),
        materialized_view_param_summary,
        slot_desc_summary,
        "schema_change alter task received"
    );
    let (base_root_path, base_s3) = resolve_tablet_location("alter_base_tablet", base_tablet_id)?;
    let (new_root_path, new_s3) = resolve_tablet_location("alter_new_tablet", new_tablet_id)?;

    let base_metadata = load_tablet_metadata_for_alter_with_retry(
        "alter_base_tablet",
        &base_root_path,
        base_tablet_id,
        alter_version,
        true,
    )?;
    let new_metadata = load_tablet_metadata_for_alter_with_retry(
        "alter_new_tablet",
        &new_root_path,
        new_tablet_id,
        1,
        true,
    )?;

    let base_read_schema = if let Some(read_schema) = request.base_tablet_read_schema.as_ref() {
        build_tablet_schema_pb_from_thrift(read_schema)?
    } else {
        resolve_tablet_schema_from_metadata_or_runtime(
            "alter_base_tablet",
            &base_metadata,
            base_tablet_id,
            alter_version,
        )?
    };
    let new_metadata_schema = resolve_tablet_schema_from_metadata_or_runtime(
        "alter_new_tablet",
        &new_metadata,
        new_tablet_id,
        1,
    )?;
    tracing::info!(
        base_schema_columns = base_read_schema.column.len(),
        new_metadata_schema_columns = new_metadata_schema.column.len(),
        "schema_change resolved base/new metadata schemas"
    );
    let new_schema = resolve_target_schema(
        request,
        &base_read_schema,
        &new_metadata_schema,
        new_tablet_id,
    )?;
    tracing::info!(
        target_schema_columns = new_schema.column.len(),
        "schema_change resolved target schema"
    );

    ensure_non_primary_keys_schema(&base_read_schema, "base_tablet_read_schema")?;
    ensure_non_primary_keys_schema(&new_schema, "new_tablet_schema")?;

    let base_ctx = TabletWriteContext {
        db_id: 0,
        table_id: 0,
        tablet_id: base_tablet_id,
        tablet_root_path: base_root_path,
        tablet_schema: base_read_schema.clone(),
        s3_config: base_s3,
        partial_update: PartialUpdateWritePolicy::default(),
    };
    let new_ctx = TabletWriteContext {
        db_id: 0,
        table_id: 0,
        tablet_id: new_tablet_id,
        tablet_root_path: new_root_path,
        tablet_schema: new_schema.clone(),
        s3_config: new_s3,
        partial_update: PartialUpdateWritePolicy::default(),
    };
    // Some rollup/sc tablets may not have been pre-registered in runtime registry (for example
    // metadata is lazily visible via shard path). Register both base/new tablet runtimes to keep
    // later publish_version lookup consistent.
    register_tablet_runtime(&base_ctx)?;
    register_tablet_runtime(&new_ctx)?;
    if should_patch_initial_metadata_schema(&new_metadata, &new_schema) {
        let mut patched_meta = new_metadata.clone();
        patched_meta.schema = Some(new_schema.clone());
        patched_meta.id = Some(new_tablet_id);
        patched_meta.version = Some(1);
        let initial_path = initial_meta_file_path(&new_ctx.tablet_root_path)?;
        if read_bytes_if_exists(&initial_path)?.is_some() {
            write_initial_meta_file(&new_ctx.tablet_root_path, &patched_meta)?;
        } else {
            let standalone_path =
                standalone_meta_file_path(&new_ctx.tablet_root_path, new_tablet_id, 1)?;
            if read_bytes_if_exists(&standalone_path)?.is_some() {
                write_standalone_meta_file(
                    &new_ctx.tablet_root_path,
                    new_tablet_id,
                    1,
                    &patched_meta,
                )?;
            } else {
                return Err(format!(
                    "schema_change could not find initial v1 metadata layout for new tablet: tablet_id={} root_path={}",
                    new_tablet_id, new_ctx.tablet_root_path
                ));
            }
        }
    }

    let source_output_schema = build_tablet_output_schema(&base_read_schema)?;
    let mut rewritten_rowsets = Vec::with_capacity(base_metadata.rowsets.len());
    for (rowset_idx, source_rowset) in base_metadata.rowsets.iter().enumerate() {
        let source_batch =
            load_rowset_batch_for_partial_update(&base_ctx, source_rowset, &source_output_schema)?;
        let transformed = transform_rowset_batch(
            &source_batch,
            &base_read_schema,
            &new_schema,
            request,
            alter_mode,
            rowset_idx,
        )?;
        let rewritten_rowset =
            write_rewritten_rowset(&new_ctx, source_rowset, &transformed, txn_id, rowset_idx)?;
        rewritten_rowsets.push(rewritten_rowset);
    }

    write_schema_change_txn_log(
        &new_ctx.tablet_root_path,
        new_tablet_id,
        txn_id,
        alter_version,
        rewritten_rowsets,
    )
}

pub(crate) fn execute_update_tablet_meta_info_task(
    request: &TUpdateTabletMetaInfoReq,
) -> Result<(), String> {
    let tablet_type = request.tablet_type.unwrap_or(TTabletType::TABLET_TYPE_DISK);
    if tablet_type != TTabletType::TABLET_TYPE_LAKE {
        return Err(format!(
            "update_tablet_meta_info unsupported tablet_type={tablet_type:?} (only TABLET_TYPE_LAKE is supported)"
        ));
    }
    let txn_id = request
        .txn_id
        .ok_or_else(|| "update_tablet_meta_info missing txn_id".to_string())?;
    if txn_id <= 0 {
        return Err(format!(
            "update_tablet_meta_info has invalid txn_id={txn_id}"
        ));
    }
    let tablet_meta_infos = request
        .tablet_meta_infos
        .as_ref()
        .ok_or_else(|| "update_tablet_meta_info missing tablet_meta_infos".to_string())?;
    for tablet_meta_info in tablet_meta_infos {
        execute_single_tablet_meta_update(tablet_meta_info, txn_id)?;
    }
    Ok(())
}

fn load_tablet_metadata_for_alter_with_retry(
    op: &str,
    tablet_root_path: &str,
    tablet_id: i64,
    version: i64,
    allow_missing_page_as_empty: bool,
) -> Result<TabletMetadataPb, String> {
    for attempt in 1..=ALTER_METADATA_LOAD_MAX_ATTEMPTS {
        match load_tablet_metadata_at_version(tablet_root_path, tablet_id, version) {
            Ok(Some(metadata)) => return Ok(metadata),
            Ok(None) => {
                if attempt == ALTER_METADATA_LOAD_MAX_ATTEMPTS {
                    return Err(format!(
                        "{op} metadata not found after retries: tablet_id={} version={} attempts={}",
                        tablet_id, version, ALTER_METADATA_LOAD_MAX_ATTEMPTS
                    ));
                }
                tracing::debug!(
                    op,
                    tablet_id,
                    version,
                    attempt,
                    max_attempts = ALTER_METADATA_LOAD_MAX_ATTEMPTS,
                    "alter task metadata not found, waiting for create/metadata visibility"
                );
            }
            Err(err) if is_retryable_alter_metadata_load_error(&err) => {
                if attempt == ALTER_METADATA_LOAD_MAX_ATTEMPTS {
                    if allow_missing_page_as_empty && is_missing_tablet_page_in_bundle_error(&err) {
                        tracing::warn!(
                            op,
                            tablet_id,
                            version,
                            attempts = ALTER_METADATA_LOAD_MAX_ATTEMPTS,
                            error = %err,
                            "alter task fallback to empty metadata after missing tablet page retries"
                        );
                        let mut metadata = empty_tablet_metadata(tablet_id);
                        metadata.version = Some(version);
                        return Ok(metadata);
                    }
                    return Err(format!(
                        "{op} metadata load failed after retries: tablet_id={} version={} attempts={} last_error={}",
                        tablet_id, version, ALTER_METADATA_LOAD_MAX_ATTEMPTS, err
                    ));
                }
                tracing::debug!(
                    op,
                    tablet_id,
                    version,
                    attempt,
                    max_attempts = ALTER_METADATA_LOAD_MAX_ATTEMPTS,
                    error = %err,
                    "alter task metadata is not visible yet, retrying"
                );
            }
            Err(err) => return Err(err),
        }
        sleep(Duration::from_millis(ALTER_METADATA_LOAD_RETRY_INTERVAL_MS));
    }
    Err(format!(
        "{op} exhausted metadata retry attempts unexpectedly: tablet_id={} version={} attempts={}",
        tablet_id, version, ALTER_METADATA_LOAD_MAX_ATTEMPTS
    ))
}

fn execute_single_tablet_meta_update(
    tablet_meta_info: &TTabletMetaInfo,
    txn_id: i64,
) -> Result<(), String> {
    let tablet_id = tablet_meta_info
        .tablet_id
        .ok_or_else(|| "update_tablet_meta_info tablet_meta_info missing tablet_id".to_string())?;
    if tablet_id <= 0 {
        return Err(format!(
            "update_tablet_meta_info has invalid tablet_id={tablet_id}"
        ));
    }
    let (tablet_root_path, _s3) = resolve_tablet_location("update_tablet_meta_info", tablet_id)?;
    let metadata_update_info = build_metadata_update_info(tablet_meta_info)?;
    let updated_schema = metadata_update_info.tablet_schema.clone();
    write_update_tablet_meta_txn_log(&tablet_root_path, tablet_id, txn_id, metadata_update_info)?;
    if let Some(schema) = updated_schema.as_ref() {
        update_tablet_runtime_schema(tablet_id, schema)?;
    }
    Ok(())
}

fn build_metadata_update_info(
    tablet_meta_info: &TTabletMetaInfo,
) -> Result<MetadataUpdateInfoPb, String> {
    let tablet_schema = tablet_meta_info
        .tablet_schema
        .as_ref()
        .map(build_tablet_schema_pb_from_thrift)
        .transpose()?;
    let persistent_index_type = tablet_meta_info
        .persistent_index_type
        .map(map_update_tablet_meta_persistent_index_type)
        .transpose()?;
    let compaction_strategy = tablet_meta_info
        .compaction_strategy
        .map(map_update_tablet_meta_compaction_strategy)
        .transpose()?;
    let flat_json_config = tablet_meta_info
        .flat_json_config
        .as_ref()
        .map(|cfg| FlatJsonConfigPb {
            flat_json_enable: cfg.flat_json_enable,
            flat_json_null_factor: cfg.flat_json_null_factor.map(|v| v.0),
            flat_json_sparsity_factor: cfg.flat_json_sparsity_factor.map(|v| v.0),
            flat_json_max_column_max: cfg.flat_json_column_max,
        });
    Ok(MetadataUpdateInfoPb {
        enable_persistent_index: tablet_meta_info.enable_persistent_index,
        tablet_schema,
        persistent_index_type,
        bundle_tablet_metadata: tablet_meta_info.bundle_tablet_metadata,
        compaction_strategy,
        flat_json_config,
    })
}

fn map_update_tablet_meta_persistent_index_type(
    persistent_index_type: TPersistentIndexType,
) -> Result<i32, String> {
    if persistent_index_type == TPersistentIndexType::LOCAL {
        return Ok(PersistentIndexTypePb::Local as i32);
    }
    if persistent_index_type == TPersistentIndexType::CLOUD_NATIVE {
        return Ok(PersistentIndexTypePb::CloudNative as i32);
    }
    Err(format!(
        "update_tablet_meta_info unsupported persistent_index_type={persistent_index_type:?}"
    ))
}

fn map_update_tablet_meta_compaction_strategy(
    compaction_strategy: TCompactionStrategy,
) -> Result<i32, String> {
    if compaction_strategy == TCompactionStrategy::DEFAULT {
        return Ok(CompactionStrategyPb::Default as i32);
    }
    if compaction_strategy == TCompactionStrategy::REAL_TIME {
        return Ok(CompactionStrategyPb::RealTime as i32);
    }
    Err(format!(
        "update_tablet_meta_info unsupported compaction_strategy={compaction_strategy:?}"
    ))
}

fn write_update_tablet_meta_txn_log(
    tablet_root_path: &str,
    tablet_id: i64,
    txn_id: i64,
    metadata_update_info: MetadataUpdateInfoPb,
) -> Result<(), String> {
    let txn_log_path = txn_log_file_path(tablet_root_path, tablet_id, txn_id)?;
    with_txn_log_append_lock(tablet_id, txn_id, || {
        let mut txn_log = match read_txn_log_if_exists(&txn_log_path)? {
            Some(existing) => existing,
            None => TxnLogPb {
                tablet_id: Some(tablet_id),
                txn_id: Some(txn_id),
                op_write: None,
                op_compaction: None,
                op_schema_change: None,
                op_alter_metadata: None,
                op_replication: None,
                partition_id: None,
                load_id: None,
            },
        };
        if txn_log.tablet_id != Some(tablet_id) {
            return Err(format!(
                "update_tablet_meta_info txn log tablet_id mismatch: expected={} actual={:?}",
                tablet_id, txn_log.tablet_id
            ));
        }
        if txn_log.txn_id != Some(txn_id) {
            return Err(format!(
                "update_tablet_meta_info txn log txn_id mismatch: expected={} actual={:?}",
                txn_id, txn_log.txn_id
            ));
        }
        if txn_log.op_write.is_some()
            || txn_log.op_compaction.is_some()
            || txn_log.op_schema_change.is_some()
            || txn_log.op_replication.is_some()
        {
            return Err(format!(
                "update_tablet_meta_info does not support mixed txn log operation: tablet_id={} txn_id={}",
                tablet_id, txn_id
            ));
        }
        txn_log
            .op_alter_metadata
            .get_or_insert_with(|| txn_log_pb::OpAlterMetadata {
                metadata_update_infos: Vec::new(),
            })
            .metadata_update_infos
            .push(metadata_update_info);
        write_txn_log_file(&txn_log_path, &txn_log)
    })
}

fn is_retryable_alter_metadata_load_error(error: &str) -> bool {
    let lowered = error.to_ascii_lowercase();
    is_missing_tablet_page_in_bundle_error(&lowered) || lowered.contains("metadata file not found:")
}

fn is_missing_tablet_page_in_bundle_error(error: &str) -> bool {
    error.contains("bundle metadata missing tablet page for tablet_id=")
        || error.contains("bundle metadata does not contain tablet page:")
}

fn is_expected_initial_metadata_without_schema(metadata: &TabletMetadataPb, version: i64) -> bool {
    version == 1
        && metadata.schema.is_none()
        && metadata.rowsets.is_empty()
        && metadata.historical_schemas.is_empty()
}

fn should_patch_initial_metadata_schema(
    metadata: &TabletMetadataPb,
    target_schema: &TabletSchemaPb,
) -> bool {
    metadata.schema.as_ref() != Some(target_schema)
}

fn resolve_tablet_schema_from_metadata_or_runtime(
    op: &str,
    metadata: &TabletMetadataPb,
    tablet_id: i64,
    version: i64,
) -> Result<TabletSchemaPb, String> {
    if let Some(schema) = metadata.schema.clone() {
        return Ok(schema);
    }
    let runtime = get_tablet_runtime(tablet_id).map_err(|runtime_err| {
        format!(
            "{op} tablet metadata missing schema and runtime schema lookup failed: tablet_id={} version={} error={}",
            tablet_id, version, runtime_err
        )
    })?;
    if is_expected_initial_metadata_without_schema(metadata, version) {
        tracing::debug!(
            op,
            tablet_id,
            version,
            "alter task initial metadata does not embed schema, using runtime schema"
        );
    } else {
        tracing::warn!(
            op,
            tablet_id,
            version,
            "alter task metadata missing schema, falling back to runtime schema"
        );
    }
    Ok(runtime.schema)
}

fn validate_schema_change_request(request: &TAlterTabletReqV2) -> Result<AlterMode, String> {
    if request.base_tablet_id <= 0 {
        return Err(format!(
            "alter task has non-positive base_tablet_id={}",
            request.base_tablet_id
        ));
    }
    if request.new_tablet_id <= 0 {
        return Err(format!(
            "alter task has non-positive new_tablet_id={}",
            request.new_tablet_id
        ));
    }

    let tablet_type = request.tablet_type.unwrap_or(TTabletType::TABLET_TYPE_DISK);
    if tablet_type != TTabletType::TABLET_TYPE_LAKE {
        return Err(format!(
            "alter task unsupported tablet_type={tablet_type:?} (only TABLET_TYPE_LAKE is supported)"
        ));
    }

    let alter_job_type = request
        .alter_job_type
        .unwrap_or(TAlterJobType::SCHEMA_CHANGE);
    match alter_job_type {
        TAlterJobType::SCHEMA_CHANGE => {
            if request
                .materialized_view_params
                .as_ref()
                .is_some_and(|v| !v.is_empty())
            {
                return Err(
                    "alter task does not support materialized_view_params in SCHEMA_CHANGE V1"
                        .to_string(),
                );
            }
            if request.materialized_column_req.is_some() {
                return Err(
                    "alter task does not support materialized_column_req in SCHEMA_CHANGE V1"
                        .to_string(),
                );
            }
            if request.where_expr.is_some() {
                return Err(
                    "alter task does not support where_expr in SCHEMA_CHANGE V1".to_string()
                );
            }
            Ok(AlterMode::SchemaChange)
        }
        TAlterJobType::ROLLUP => {
            if request.materialized_column_req.is_some() {
                return Err(
                    "alter task does not support materialized_column_req in ROLLUP V1".to_string(),
                );
            }
            if request.query_options.is_none() || request.query_globals.is_none() {
                return Err("alter task missing query_options/query_globals for ROLLUP".to_string());
            }
            if request.desc_tbl.is_none() {
                return Err("alter task missing desc_tbl for ROLLUP".to_string());
            }
            Ok(AlterMode::Rollup)
        }
        _ => Err(format!(
            "alter task unsupported alter_job_type={alter_job_type:?} (supported: SCHEMA_CHANGE, ROLLUP)"
        )),
    }
}

fn resolve_tablet_location(
    op: &str,
    tablet_id: i64,
) -> Result<(String, Option<S3StoreConfig>), String> {
    match get_tablet_runtime(tablet_id) {
        Ok(runtime) => Ok((runtime.root_path, runtime.s3_config)),
        Err(runtime_err) => {
            let mut infos = starlet_shard_registry::select_infos(&[tablet_id]);
            if let Some(info) = infos.remove(&tablet_id) {
                return Ok((info.full_path, info.s3));
            }
            Err(format!(
                "{op} missing tablet runtime for tablet_id={tablet_id}: {runtime_err}"
            ))
        }
    }
}

fn ensure_non_primary_keys_schema(schema: &TabletSchemaPb, context: &str) -> Result<(), String> {
    let keys_type_raw = schema
        .keys_type
        .ok_or_else(|| format!("{context} missing keys_type"))?;
    let keys_type = KeysType::try_from(keys_type_raw)
        .map_err(|_| format!("{context} has unknown keys_type={keys_type_raw}"))?;
    if keys_type == KeysType::PrimaryKeys {
        return Err(format!(
            "{context} PRIMARY_KEYS is unsupported for SCHEMA_CHANGE V1"
        ));
    }
    Ok(())
}

fn resolve_target_schema(
    request: &TAlterTabletReqV2,
    base_read_schema: &TabletSchemaPb,
    new_metadata_schema: &TabletSchemaPb,
    new_tablet_id: i64,
) -> Result<TabletSchemaPb, String> {
    if let Ok(runtime) = get_tablet_runtime(new_tablet_id) {
        if &runtime.schema != new_metadata_schema {
            tracing::info!(
                tablet_id = new_tablet_id,
                metadata_schema_id = new_metadata_schema.id,
                runtime_schema_id = runtime.schema.id,
                "schema_change target schema uses runtime schema instead of on-disk metadata"
            );
        }
        return Ok(runtime.schema);
    }
    if request.new_schema_hash != request.base_schema_hash
        && schemas_equivalent(base_read_schema, new_metadata_schema)
    {
        return Err(format!(
            "alter task target schema unresolved: new_schema_hash={} base_schema_hash={} but new tablet metadata schema is equivalent to base schema and runtime schema is unavailable",
            request.new_schema_hash, request.base_schema_hash
        ));
    }
    Ok(new_metadata_schema.clone())
}

fn schemas_equivalent(lhs: &TabletSchemaPb, rhs: &TabletSchemaPb) -> bool {
    lhs.keys_type == rhs.keys_type
        && lhs.num_short_key_columns == rhs.num_short_key_columns
        && lhs.sort_key_idxes == rhs.sort_key_idxes
        && lhs.sort_key_unique_ids == rhs.sort_key_unique_ids
        && lhs.column.len() == rhs.column.len()
        && lhs
            .column
            .iter()
            .zip(rhs.column.iter())
            .all(|(l, r)| columns_equivalent(l, r))
}

fn columns_equivalent(
    lhs: &crate::service::grpc_client::proto::starrocks::ColumnPb,
    rhs: &crate::service::grpc_client::proto::starrocks::ColumnPb,
) -> bool {
    lhs.unique_id == rhs.unique_id
        && lhs.name == rhs.name
        && lhs.r#type == rhs.r#type
        && lhs.is_key == rhs.is_key
        && lhs.is_nullable == rhs.is_nullable
        && lhs.aggregation == rhs.aggregation
        && lhs.default_value == rhs.default_value
        && lhs.precision == rhs.precision
        && lhs.frac == rhs.frac
        && lhs.index_length == rhs.index_length
        && lhs.children_columns.len() == rhs.children_columns.len()
        && lhs
            .children_columns
            .iter()
            .zip(rhs.children_columns.iter())
            .all(|(l, r)| columns_equivalent(l, r))
}

fn build_unique_id_index_map(
    schema: &TabletSchemaPb,
    context: &str,
) -> Result<HashMap<i32, usize>, String> {
    let mut out = HashMap::with_capacity(schema.column.len());
    for (idx, column) in schema.column.iter().enumerate() {
        let unique_id = column.unique_id;
        if unique_id < 0 {
            return Err(format!(
                "{context} column has negative unique_id: index={} name={} unique_id={}",
                idx,
                column.name.as_deref().unwrap_or("<unnamed>"),
                unique_id
            ));
        }
        if out.insert(unique_id, idx).is_some() {
            return Err(format!(
                "{context} duplicate unique_id detected: unique_id={}",
                unique_id
            ));
        }
    }
    Ok(out)
}

fn transform_rowset_batch(
    source_batch: &RecordBatch,
    source_schema: &TabletSchemaPb,
    target_schema: &TabletSchemaPb,
    request: &TAlterTabletReqV2,
    alter_mode: AlterMode,
    rowset_idx: usize,
) -> Result<RecordBatch, String> {
    match alter_mode {
        AlterMode::SchemaChange => transform_rowset_batch_schema_change(
            source_batch,
            source_schema,
            target_schema,
            rowset_idx,
        ),
        AlterMode::Rollup => transform_rowset_batch_rollup(
            source_batch,
            source_schema,
            target_schema,
            request,
            rowset_idx,
        ),
    }
}

fn transform_rowset_batch_schema_change(
    source_batch: &RecordBatch,
    source_schema: &TabletSchemaPb,
    target_schema: &TabletSchemaPb,
    rowset_idx: usize,
) -> Result<RecordBatch, String> {
    let target_output_schema = build_tablet_output_schema(target_schema)?;
    if source_batch.num_rows() == 0 {
        return Ok(RecordBatch::new_empty(target_output_schema));
    }

    let source_uid_to_index =
        build_unique_id_index_map(source_schema, "schema_change source schema")?;
    let mut target_columns = Vec::with_capacity(target_schema.column.len());

    for (target_idx, target_col) in target_schema.column.iter().enumerate() {
        let target_field = target_output_schema
            .fields()
            .get(target_idx)
            .ok_or_else(|| {
                format!(
                    "schema_change target output schema index out of range: rowset_idx={} column_index={}",
                    rowset_idx, target_idx
                )
            })?;
        if target_col.unique_id < 0 {
            return Err(format!(
                "schema_change target column has negative unique_id: rowset_idx={} column_index={} name={}",
                rowset_idx,
                target_idx,
                target_col.name.as_deref().unwrap_or("<unnamed>")
            ));
        }
        let target_name = target_col.name.as_deref().unwrap_or("<unnamed>");
        if let Some(source_idx) = source_uid_to_index.get(&target_col.unique_id).copied() {
            let source_array = source_batch
                .columns()
                .get(source_idx)
                .cloned()
                .ok_or_else(|| {
                    format!(
                        "schema_change source batch column index out of range: rowset_idx={} source_index={} source_columns={}",
                        rowset_idx,
                        source_idx,
                        source_batch.num_columns()
                    )
                })?;
            let transformed = cast_source_column_to_target(
                &source_array,
                target_field.data_type(),
                rowset_idx,
                source_idx,
                target_idx,
                target_name,
            )?;
            ensure_target_non_nullable_column(
                &transformed,
                target_col,
                rowset_idx,
                target_idx,
                target_name,
            )?;
            target_columns.push(transformed);
            continue;
        }

        let missing_column = build_missing_column_array(
            target_col,
            target_field.data_type(),
            source_batch.num_rows(),
            rowset_idx,
            target_idx,
        )?;
        ensure_target_non_nullable_column(
            &missing_column,
            target_col,
            rowset_idx,
            target_idx,
            target_name,
        )?;
        target_columns.push(missing_column);
    }

    RecordBatch::try_new(target_output_schema, target_columns).map_err(|e| {
        format!(
            "schema_change build transformed rowset batch failed: rowset_idx={} rows={} error={}",
            rowset_idx,
            source_batch.num_rows(),
            e
        )
    })
}

fn transform_rowset_batch_rollup(
    source_batch: &RecordBatch,
    source_schema: &TabletSchemaPb,
    target_schema: &TabletSchemaPb,
    request: &TAlterTabletReqV2,
    rowset_idx: usize,
) -> Result<RecordBatch, String> {
    let target_output_schema = build_tablet_output_schema(target_schema)?;
    if source_batch.num_rows() == 0 {
        return Ok(RecordBatch::new_empty(target_output_schema));
    }

    let materialized_param_map = build_rollup_materialized_param_map(request)?;
    let filtered_source_batch =
        apply_rollup_where_expr(source_batch, source_schema, request, rowset_idx)?;
    if filtered_source_batch.num_rows() == 0 {
        return Ok(RecordBatch::new_empty(target_output_schema));
    }

    let source_uid_to_index = build_unique_id_index_map(source_schema, "rollup source schema")?;
    let source_name_to_index = build_source_name_index_map(source_schema, "rollup source schema")?;
    let need_mv_expr_eval = materialized_param_map
        .values()
        .any(|param| param.mv_expr.is_some());
    let eval_input = if need_mv_expr_eval {
        Some(build_rollup_expr_input(
            request,
            &filtered_source_batch,
            source_schema,
            rowset_idx,
        )?)
    } else {
        None
    };

    let mut target_columns = Vec::with_capacity(target_schema.column.len());
    for (target_idx, target_col) in target_schema.column.iter().enumerate() {
        let target_field = target_output_schema
            .fields()
            .get(target_idx)
            .ok_or_else(|| {
                format!(
                    "rollup target output schema index out of range: rowset_idx={} column_index={}",
                    rowset_idx, target_idx
                )
            })?;
        let target_name = target_col.name.as_deref().unwrap_or("<unnamed>");
        let target_name_key = normalize_slot_name(target_name);

        let output_array = if let Some(mv_param) =
            materialized_param_map.get(&target_name_key).copied()
        {
            if let Some(mv_expr) = mv_param.mv_expr.as_ref() {
                let eval_input = eval_input.as_ref().ok_or_else(|| {
                    format!(
                        "rollup mv_expr evaluation context is missing: rowset_idx={} target_index={} target_name={}",
                        rowset_idx, target_idx, target_name
                    )
                })?;
                let expr_array = eval_rollup_expr(
                    mv_expr,
                    eval_input,
                    "materialized_view_params.mv_expr",
                    rowset_idx,
                    target_idx,
                    target_name,
                )?;
                if expr_array.len() != filtered_source_batch.num_rows() {
                    return Err(format!(
                        "rollup mv_expr result row count mismatch: rowset_idx={} target_index={} target_name={} expected_rows={} actual_rows={}",
                        rowset_idx,
                        target_idx,
                        target_name,
                        filtered_source_batch.num_rows(),
                        expr_array.len()
                    ));
                }
                cast_rollup_expr_to_target(
                    &expr_array,
                    target_field.data_type(),
                    rowset_idx,
                    target_idx,
                    target_name,
                )?
            } else {
                let origin_name = mv_param
                    .origin_column_name
                    .as_deref()
                    .filter(|v| !v.trim().is_empty())
                    .ok_or_else(|| {
                        format!(
                            "rollup materialized_view_param missing origin_column_name without mv_expr: rowset_idx={} target_index={} target_name={}",
                            rowset_idx, target_idx, target_name
                        )
                    })?;
                let source_idx =
                    resolve_source_column_index_by_name(&source_name_to_index, origin_name).ok_or_else(
                        || {
                            format!(
                                "rollup origin column not found in source schema: rowset_idx={} target_index={} target_name={} origin_column_name={}",
                                rowset_idx, target_idx, target_name, origin_name
                            )
                        },
                    )?;
                let source_array = filtered_source_batch
                    .columns()
                    .get(source_idx)
                    .cloned()
                    .ok_or_else(|| {
                        format!(
                            "rollup source batch column index out of range for origin column: rowset_idx={} source_index={} source_columns={}",
                            rowset_idx,
                            source_idx,
                            filtered_source_batch.num_columns()
                        )
                    })?;
                cast_source_column_to_target(
                    &source_array,
                    target_field.data_type(),
                    rowset_idx,
                    source_idx,
                    target_idx,
                    target_name,
                )?
            }
        } else if let Some(source_idx) =
            resolve_source_column_index_by_name(&source_name_to_index, target_name)
                .or_else(|| source_uid_to_index.get(&target_col.unique_id).copied())
        {
            let source_array = filtered_source_batch
                .columns()
                .get(source_idx)
                .cloned()
                .ok_or_else(|| {
                    format!(
                        "rollup source batch column index out of range: rowset_idx={} source_index={} source_columns={}",
                        rowset_idx,
                        source_idx,
                        filtered_source_batch.num_columns()
                    )
                })?;
            cast_source_column_to_target(
                &source_array,
                target_field.data_type(),
                rowset_idx,
                source_idx,
                target_idx,
                target_name,
            )?
        } else {
            build_missing_column_array(
                target_col,
                target_field.data_type(),
                filtered_source_batch.num_rows(),
                rowset_idx,
                target_idx,
            )?
        };

        ensure_target_non_nullable_column(
            &output_array,
            target_col,
            rowset_idx,
            target_idx,
            target_name,
        )?;
        target_columns.push(output_array);
    }

    RecordBatch::try_new(target_output_schema, target_columns).map_err(|e| {
        format!(
            "rollup build transformed rowset batch failed: rowset_idx={} rows={} error={}",
            rowset_idx,
            filtered_source_batch.num_rows(),
            e
        )
    })
}

fn build_source_name_index_map(
    schema: &TabletSchemaPb,
    context: &str,
) -> Result<HashMap<String, usize>, String> {
    let mut out = HashMap::with_capacity(schema.column.len());
    for (idx, column) in schema.column.iter().enumerate() {
        let Some(name) = column.name.as_deref().filter(|v| !v.trim().is_empty()) else {
            continue;
        };
        let normalized = normalize_slot_name(name);
        if out.insert(normalized.clone(), idx).is_some() {
            return Err(format!(
                "{context} duplicate column name detected after normalization: name={} normalized={}",
                name, normalized
            ));
        }
    }
    Ok(out)
}

fn resolve_source_column_index_by_name(
    source_name_to_index: &HashMap<String, usize>,
    source_name: &str,
) -> Option<usize> {
    source_name_to_index
        .get(&normalize_slot_name(source_name))
        .copied()
}

fn build_rollup_materialized_param_map<'a>(
    request: &'a TAlterTabletReqV2,
) -> Result<HashMap<String, &'a TAlterMaterializedViewParam>, String> {
    let mut out = HashMap::new();
    let params = request
        .materialized_view_params
        .as_ref()
        .map(Vec::as_slice)
        .unwrap_or(&[]);
    for (idx, param) in params.iter().enumerate() {
        let name = param.column_name.trim();
        if name.is_empty() {
            return Err(format!(
                "rollup materialized_view_params[{}] has empty column_name",
                idx
            ));
        }
        let key = normalize_slot_name(name);
        if out.insert(key.clone(), param).is_some() {
            return Err(format!(
                "rollup materialized_view_params duplicate column_name after normalization: column_name={} normalized={}",
                name, key
            ));
        }
    }
    Ok(out)
}

struct RollupExprInput {
    chunk: Chunk,
    layout: Layout,
}

fn build_rollup_expr_input(
    request: &TAlterTabletReqV2,
    source_batch: &RecordBatch,
    source_schema: &TabletSchemaPb,
    rowset_idx: usize,
) -> Result<RollupExprInput, String> {
    let desc_tbl = request
        .desc_tbl
        .as_ref()
        .ok_or_else(|| "rollup expression evaluation requires desc_tbl".to_string())?;
    let slot_descs = desc_tbl.slot_descriptors.as_ref().ok_or_else(|| {
        "rollup expression evaluation requires desc_tbl.slot_descriptors".to_string()
    })?;
    let source_name_to_index = build_source_name_index_map(source_schema, "rollup source schema")?;

    let mut fields = Vec::new();
    let mut arrays = Vec::new();
    let mut order = Vec::new();
    let mut seen_slots = HashSet::new();

    for slot_desc in slot_descs {
        let (Some(tuple_id), Some(raw_slot_id)) = (slot_desc.parent, slot_desc.id) else {
            continue;
        };
        let Some(slot_name) = slot_desc
            .col_name
            .as_ref()
            .filter(|v| !v.trim().is_empty())
            .or_else(|| {
                slot_desc
                    .col_physical_name
                    .as_ref()
                    .filter(|v| !v.trim().is_empty())
            })
        else {
            continue;
        };
        let Some(source_idx) =
            resolve_source_column_index_by_name(&source_name_to_index, slot_name)
        else {
            continue;
        };
        let slot_id = SlotId::try_from(raw_slot_id).map_err(|e| {
            format!(
                "rollup descriptor slot id conversion failed: rowset_idx={} slot_id={} error={}",
                rowset_idx, raw_slot_id, e
            )
        })?;
        if !seen_slots.insert(slot_id) {
            return Err(format!(
                "rollup descriptor contains duplicate slot id: rowset_idx={} slot_id={}",
                rowset_idx, raw_slot_id
            ));
        }
        let source_batch_schema = source_batch.schema();
        let source_field = source_batch_schema
            .fields()
            .get(source_idx)
            .ok_or_else(|| {
                format!(
                    "rollup source schema index out of range for expression slot mapping: rowset_idx={} source_index={} source_columns={}",
                    rowset_idx,
                    source_idx,
                    source_batch.num_columns()
                )
            })?;
        let field = field_with_slot_id(
            Field::new(
                slot_name,
                source_field.data_type().clone(),
                slot_desc.is_nullable.unwrap_or(source_field.is_nullable()),
            ),
            slot_id,
        );
        fields.push(field);
        arrays.push(
            source_batch
                .columns()
                .get(source_idx)
                .cloned()
                .ok_or_else(|| {
                    format!(
                        "rollup source batch index out of range while building expression input: rowset_idx={} source_index={} source_columns={}",
                        rowset_idx,
                        source_idx,
                        source_batch.num_columns()
                    )
                })?,
        );
        order.push((tuple_id, raw_slot_id));
    }

    if fields.is_empty() {
        return Err(format!(
            "rollup cannot map descriptor slots to source schema for expression evaluation: rowset_idx={}",
            rowset_idx
        ));
    }

    let eval_schema = Arc::new(Schema::new(fields));
    let eval_batch = RecordBatch::try_new(eval_schema, arrays).map_err(|e| {
        format!(
            "rollup failed to build expression input batch: rowset_idx={} error={}",
            rowset_idx, e
        )
    })?;
    let chunk = Chunk::try_new(eval_batch).map_err(|e| {
        format!(
            "rollup failed to initialize expression input chunk: rowset_idx={} error={}",
            rowset_idx, e
        )
    })?;
    let index = order.iter().enumerate().map(|(i, key)| (*key, i)).collect();
    Ok(RollupExprInput {
        chunk,
        layout: Layout { order, index },
    })
}

fn eval_rollup_expr(
    expr: &crate::exprs::TExpr,
    eval_input: &RollupExprInput,
    expr_context: &str,
    rowset_idx: usize,
    target_idx: usize,
    target_name: &str,
) -> Result<ArrayRef, String> {
    let mut arena = ExprArena::default();
    let expr_id = lower_t_expr(expr, &mut arena, &eval_input.layout, None, None).map_err(|e| {
        format!(
            "rollup lower expression failed: rowset_idx={} target_index={} target_name={} context={} error={}",
            rowset_idx, target_idx, target_name, expr_context, e
        )
    })?;
    arena.eval(expr_id, &eval_input.chunk).map_err(|e| {
        format!(
            "rollup evaluate expression failed: rowset_idx={} target_index={} target_name={} context={} error={}",
            rowset_idx, target_idx, target_name, expr_context, e
        )
    })
}

fn apply_rollup_where_expr(
    source_batch: &RecordBatch,
    source_schema: &TabletSchemaPb,
    request: &TAlterTabletReqV2,
    rowset_idx: usize,
) -> Result<RecordBatch, String> {
    let Some(where_expr) = request.where_expr.as_ref() else {
        return Ok(source_batch.clone());
    };
    let eval_input = build_rollup_expr_input(request, source_batch, source_schema, rowset_idx)?;
    let predicate = eval_rollup_expr(
        where_expr,
        &eval_input,
        "where_expr",
        rowset_idx,
        usize::MAX,
        "<where_expr>",
    )?;
    apply_rollup_where_predicate(source_batch, &predicate, rowset_idx)
}

fn apply_rollup_where_predicate(
    source_batch: &RecordBatch,
    predicate: &ArrayRef,
    rowset_idx: usize,
) -> Result<RecordBatch, String> {
    let predicate_bool = if predicate.data_type() == &DataType::Boolean {
        predicate.clone()
    } else {
        cast(predicate.as_ref(), &DataType::Boolean).map_err(|e| {
            format!(
                "rollup cast where_expr result to boolean failed: rowset_idx={} from={:?} error={}",
                rowset_idx,
                predicate.data_type(),
                e
            )
        })?
    };
    let predicate_bool = predicate_bool
        .as_any()
        .downcast_ref::<BooleanArray>()
        .ok_or_else(|| {
            format!(
                "rollup where_expr did not produce boolean result after cast: rowset_idx={} result_type={:?}",
                rowset_idx,
                predicate_bool.data_type()
            )
        })?;
    if predicate_bool.len() != source_batch.num_rows() {
        return Err(format!(
            "rollup where_expr row count mismatch: rowset_idx={} expected_rows={} actual_rows={}",
            rowset_idx,
            source_batch.num_rows(),
            predicate_bool.len()
        ));
    }
    if predicate_bool.is_empty() {
        return Ok(source_batch.clone());
    }

    let keep = (0..predicate_bool.len())
        .map(|row| !predicate_bool.is_null(row) && predicate_bool.value(row))
        .collect::<Vec<_>>();
    if keep.iter().all(|v| *v) {
        return Ok(source_batch.clone());
    }
    if keep.iter().all(|v| !*v) {
        return Ok(RecordBatch::new_empty(source_batch.schema()));
    }
    let mask = BooleanArray::from(keep);
    filter_record_batch(source_batch, &mask).map_err(|e| {
        format!(
            "rollup apply where_expr filter failed: rowset_idx={} rows={} error={}",
            rowset_idx,
            source_batch.num_rows(),
            e
        )
    })
}

fn cast_rollup_expr_to_target(
    expr_array: &ArrayRef,
    target_type: &DataType,
    rowset_idx: usize,
    target_idx: usize,
    target_name: &str,
) -> Result<ArrayRef, String> {
    if expr_array.data_type() == target_type {
        return Ok(expr_array.clone());
    }
    cast(expr_array.as_ref(), target_type).map_err(|e| {
        format!(
            "rollup cast mv_expr result failed: rowset_idx={} target_index={} target_name={} from={:?} to={:?} error={}",
            rowset_idx,
            target_idx,
            target_name,
            expr_array.data_type(),
            target_type,
            e
        )
    })
}

fn ensure_target_non_nullable_column(
    array: &ArrayRef,
    target_col: &crate::service::grpc_client::proto::starrocks::ColumnPb,
    rowset_idx: usize,
    target_idx: usize,
    target_name: &str,
) -> Result<(), String> {
    if target_col.is_nullable.unwrap_or(true) {
        return Ok(());
    }
    if array.null_count() > 0 {
        return Err(format!(
            "schema_change produced null values for non-nullable target column: rowset_idx={} target_index={} target_name={} null_count={}",
            rowset_idx,
            target_idx,
            target_name,
            array.null_count()
        ));
    }
    Ok(())
}

fn cast_source_column_to_target(
    source_array: &ArrayRef,
    target_type: &DataType,
    rowset_idx: usize,
    source_idx: usize,
    target_idx: usize,
    target_name: &str,
) -> Result<ArrayRef, String> {
    if source_array.data_type() == target_type {
        return Ok(source_array.clone());
    }
    let casted = cast(source_array.as_ref(), target_type).map_err(|e| {
        format!(
            "schema_change cast column failed: rowset_idx={} source_index={} target_index={} target_name={} from={:?} to={:?} error={}",
            rowset_idx,
            source_idx,
            target_idx,
            target_name,
            source_array.data_type(),
            target_type,
            e
        )
    })?;
    for row_idx in 0..source_array.len() {
        if !source_array.is_null(row_idx) && casted.is_null(row_idx) {
            return Err(format!(
                "schema_change cast produced null for non-null source value: rowset_idx={} source_index={} target_index={} target_name={} row_idx={} from={:?} to={:?}",
                rowset_idx,
                source_idx,
                target_idx,
                target_name,
                row_idx,
                source_array.data_type(),
                target_type
            ));
        }
    }
    Ok(casted)
}

fn build_missing_column_array(
    target_col: &crate::service::grpc_client::proto::starrocks::ColumnPb,
    target_type: &DataType,
    row_count: usize,
    rowset_idx: usize,
    target_idx: usize,
) -> Result<ArrayRef, String> {
    if row_count == 0 {
        return Ok(new_empty_array(target_type));
    }
    if let Some(raw_default) = target_col.default_value.as_ref() {
        let literal = String::from_utf8_lossy(raw_default).to_string();
        let singleton = parse_default_literal_to_singleton_array(target_type, &literal).map_err(|e| {
            format!(
                "schema_change parse default literal failed: rowset_idx={} target_index={} target_name={} literal={} error={}",
                rowset_idx,
                target_idx,
                target_col.name.as_deref().unwrap_or("<unnamed>"),
                literal,
                e
            )
        })?;
        return repeat_singleton_array(&singleton, row_count, rowset_idx, target_idx);
    }
    if target_col.is_nullable.unwrap_or(true) {
        return Ok(new_null_array(target_type, row_count));
    }
    Err(format!(
        "schema_change missing default value for non-nullable added column: rowset_idx={} target_index={} target_name={}",
        rowset_idx,
        target_idx,
        target_col.name.as_deref().unwrap_or("<unnamed>")
    ))
}

fn repeat_singleton_array(
    singleton: &ArrayRef,
    row_count: usize,
    rowset_idx: usize,
    target_idx: usize,
) -> Result<ArrayRef, String> {
    if singleton.len() != 1 {
        return Err(format!(
            "schema_change singleton default array length mismatch: rowset_idx={} target_index={} len={}",
            rowset_idx,
            target_idx,
            singleton.len()
        ));
    }
    let index = UInt32Array::from(vec![0_u32; row_count]);
    take(singleton.as_ref(), &index, None).map_err(|e| {
        format!(
            "schema_change repeat singleton default array failed: rowset_idx={} target_index={} rows={} error={}",
            rowset_idx, target_idx, row_count, e
        )
    })
}

fn write_rewritten_rowset(
    new_ctx: &TabletWriteContext,
    source_rowset: &RowsetMetadataPb,
    transformed_batch: &RecordBatch,
    txn_id: i64,
    rowset_idx: usize,
) -> Result<RowsetMetadataPb, String> {
    if transformed_batch.num_rows() == 0 {
        return Ok(RowsetMetadataPb {
            id: None,
            overlapped: source_rowset.overlapped.or(Some(false)),
            segments: Vec::new(),
            num_rows: source_rowset.num_rows.or(Some(0)),
            data_size: Some(0),
            delete_predicate: source_rowset.delete_predicate.clone(),
            num_dels: source_rowset.num_dels.or(Some(0)),
            segment_size: Vec::new(),
            max_compact_input_rowset_id: source_rowset.max_compact_input_rowset_id,
            version: None,
            del_files: Vec::new(),
            segment_encryption_metas: Vec::new(),
            next_compaction_offset: source_rowset.next_compaction_offset,
            bundle_file_offsets: Vec::new(),
            shared_segments: Vec::new(),
            record_predicate: source_rowset.record_predicate.clone(),
            segment_metas: Vec::new(),
        });
    }

    let sorted_batch = sort_batch_for_native_write(transformed_batch, &new_ctx.tablet_schema)?;
    let segment_meta = build_single_segment_metadata(&sorted_batch, &new_ctx.tablet_schema)?;
    let segment_bytes =
        build_starrocks_native_segment_bytes(&sorted_batch, &new_ctx.tablet_schema)?;
    let segment_size = segment_bytes.len() as u64;
    let driver_id = i32::try_from(rowset_idx).map_err(|_| {
        format!(
            "schema_change rowset index overflow while generating data file name: rowset_idx={}",
            rowset_idx
        )
    })?;
    let data_file_name = build_txn_data_file_name(
        new_ctx.tablet_id,
        txn_id,
        driver_id,
        0,
        StarRocksWriteFormat::Native,
        None,
    )?;
    let data_file_path = join_tablet_path(
        &new_ctx.tablet_root_path,
        &format!("{DATA_DIR}/{data_file_name}"),
    )?;
    write_bytes(&data_file_path, segment_bytes)?;

    Ok(RowsetMetadataPb {
        id: None,
        overlapped: source_rowset.overlapped.or(Some(false)),
        segments: vec![data_file_name],
        num_rows: Some(sorted_batch.num_rows() as i64),
        data_size: Some(segment_size as i64),
        delete_predicate: source_rowset.delete_predicate.clone(),
        num_dels: source_rowset.num_dels.or(Some(0)),
        segment_size: vec![segment_size],
        max_compact_input_rowset_id: source_rowset.max_compact_input_rowset_id,
        version: None,
        del_files: Vec::new(),
        segment_encryption_metas: Vec::new(),
        next_compaction_offset: source_rowset.next_compaction_offset,
        bundle_file_offsets: vec![0],
        shared_segments: vec![false],
        record_predicate: source_rowset.record_predicate.clone(),
        segment_metas: vec![segment_meta],
    })
}

fn write_schema_change_txn_log(
    tablet_root_path: &str,
    new_tablet_id: i64,
    txn_id: i64,
    alter_version: i64,
    rewritten_rowsets: Vec<RowsetMetadataPb>,
) -> Result<(), String> {
    let txn_log_path = txn_log_file_path(tablet_root_path, new_tablet_id, txn_id)?;
    with_txn_log_append_lock(new_tablet_id, txn_id, || {
        let mut txn_log = match read_txn_log_if_exists(&txn_log_path)? {
            Some(existing) => existing,
            None => TxnLogPb {
                tablet_id: Some(new_tablet_id),
                txn_id: Some(txn_id),
                op_write: None,
                op_compaction: None,
                op_schema_change: None,
                op_alter_metadata: None,
                op_replication: None,
                partition_id: None,
                load_id: None,
            },
        };
        if txn_log.tablet_id != Some(new_tablet_id) {
            return Err(format!(
                "alter task txn log tablet_id mismatch: expected={} actual={:?}",
                new_tablet_id, txn_log.tablet_id
            ));
        }
        if txn_log.txn_id != Some(txn_id) {
            return Err(format!(
                "alter task txn log txn_id mismatch: expected={} actual={:?}",
                txn_id, txn_log.txn_id
            ));
        }
        if txn_log.op_write.is_some()
            || txn_log.op_compaction.is_some()
            || txn_log.op_alter_metadata.is_some()
            || txn_log.op_replication.is_some()
        {
            return Err(format!(
                "alter task does not support mixed txn log operation: tablet_id={} txn_id={}",
                new_tablet_id, txn_id
            ));
        }
        txn_log.op_schema_change = Some(txn_log_pb::OpSchemaChange {
            rowsets: rewritten_rowsets,
            linked_segment: Some(false),
            alter_version: Some(alter_version),
            delvec_meta: None,
        });
        write_txn_log_file(&txn_log_path, &txn_log)
    })
}

#[cfg(test)]
mod tests {
    use super::{
        is_expected_initial_metadata_without_schema, resolve_target_schema, schemas_equivalent,
        should_patch_initial_metadata_schema,
    };
    use crate::agent_service::TAlterTabletReqV2;
    use crate::connector::starrocks::lake::context::PartialUpdateWritePolicy;
    use crate::connector::starrocks::lake::context::{
        TabletWriteContext, clear_tablet_runtime_cache_for_test, register_tablet_runtime,
    };
    use crate::service::grpc_client::proto::starrocks::{
        ColumnPb, RowsetMetadataPb, TabletMetadataPb, TabletSchemaPb,
    };
    use tempfile::tempdir;

    #[test]
    fn initial_empty_v1_metadata_without_schema_is_expected() {
        let mut metadata = TabletMetadataPb::default();
        metadata.id = Some(123);
        metadata.version = Some(1);
        assert!(is_expected_initial_metadata_without_schema(&metadata, 1));
    }

    #[test]
    fn non_initial_or_non_empty_metadata_without_schema_is_not_expected() {
        let mut metadata = TabletMetadataPb::default();
        metadata.id = Some(123);
        metadata.version = Some(2);
        assert!(!is_expected_initial_metadata_without_schema(&metadata, 2));

        let mut metadata_with_rowset = TabletMetadataPb::default();
        metadata_with_rowset.id = Some(123);
        metadata_with_rowset.version = Some(1);
        metadata_with_rowset
            .rowsets
            .push(RowsetMetadataPb::default());
        assert!(!is_expected_initial_metadata_without_schema(
            &metadata_with_rowset,
            1
        ));
    }

    #[test]
    fn schemas_equivalent_compares_nested_children_recursively() {
        let lhs = test_schema(
            1,
            vec![test_struct_column(
                10,
                "c1",
                vec![
                    test_scalar_column(11, "v1", "INT"),
                    test_scalar_column(12, "v2", "INT"),
                ],
            )],
        );
        let rhs = test_schema(
            2,
            vec![test_struct_column(
                10,
                "c1",
                vec![
                    test_scalar_column(11, "v1", "INT"),
                    test_scalar_column(12, "v2", "INT"),
                    test_scalar_column(13, "v3", "INT"),
                ],
            )],
        );
        assert!(
            !schemas_equivalent(&lhs, &rhs),
            "nested child add/drop must be treated as schema change"
        );
    }

    #[test]
    fn schemas_equivalent_ignores_schema_id_when_column_layout_matches() {
        let lhs = test_schema(
            1,
            vec![test_struct_column(
                10,
                "c1",
                vec![test_scalar_column(11, "v1", "INT")],
            )],
        );
        let rhs = test_schema(
            99,
            vec![test_struct_column(
                10,
                "c1",
                vec![test_scalar_column(11, "v1", "INT")],
            )],
        );
        assert!(
            schemas_equivalent(&lhs, &rhs),
            "schema id alone should not make target schema unresolved"
        );
    }

    #[test]
    fn missing_or_mismatched_initial_schema_requires_patch() {
        let target = test_schema(7, vec![test_scalar_column(11, "v1", "INT")]);

        let missing = TabletMetadataPb::default();
        assert!(should_patch_initial_metadata_schema(&missing, &target));

        let mut mismatched = TabletMetadataPb::default();
        mismatched.schema = Some(test_schema(7, vec![test_scalar_column(12, "v2", "INT")]));
        assert!(should_patch_initial_metadata_schema(&mismatched, &target));

        let mut matched = TabletMetadataPb::default();
        matched.schema = Some(target.clone());
        assert!(!should_patch_initial_metadata_schema(&matched, &target));
    }

    #[test]
    fn resolve_target_schema_prefers_runtime_schema_for_new_tablet() {
        clear_tablet_runtime_cache_for_test();
        let root = tempdir().expect("tempdir");
        let runtime_schema = test_schema(
            88,
            vec![
                test_scalar_column(10, "ds", "DATE"),
                test_scalar_column(11, "tag", "VARCHAR"),
                test_scalar_column(12, "mv_sum_v1", "BIGINT"),
            ],
        );
        let runtime_ctx = TabletWriteContext {
            db_id: 0,
            table_id: 0,
            tablet_id: 70001,
            tablet_root_path: root.path().to_string_lossy().into_owned(),
            tablet_schema: runtime_schema.clone(),
            s3_config: None,
            partial_update: PartialUpdateWritePolicy::default(),
        };
        register_tablet_runtime(&runtime_ctx).expect("register runtime");

        let metadata_schema = test_schema(
            77,
            vec![
                test_scalar_column(10, "ds", "DATE"),
                test_scalar_column(13, "k1", "VARCHAR"),
                test_scalar_column(14, "v1", "INT"),
            ],
        );
        let base_schema = metadata_schema.clone();
        let request = test_alter_request(1, 2);

        let resolved = resolve_target_schema(&request, &base_schema, &metadata_schema, 70001)
            .expect("resolve target schema");

        assert_eq!(resolved, runtime_schema);
        clear_tablet_runtime_cache_for_test();
    }

    #[test]
    fn resolve_target_schema_errors_when_only_stale_metadata_is_available() {
        clear_tablet_runtime_cache_for_test();
        let base_schema = test_schema(
            77,
            vec![
                test_scalar_column(10, "ds", "DATE"),
                test_scalar_column(13, "k1", "VARCHAR"),
                test_scalar_column(14, "v1", "INT"),
            ],
        );
        let request = test_alter_request(1, 2);

        let err = resolve_target_schema(&request, &base_schema, &base_schema, 70002)
            .expect_err("stale metadata without runtime should fail");

        assert!(
            err.contains("target schema unresolved"),
            "unexpected error: {err}"
        );
    }

    fn test_schema(schema_id: i64, columns: Vec<ColumnPb>) -> TabletSchemaPb {
        TabletSchemaPb {
            keys_type: Some(0),
            column: columns,
            num_short_key_columns: Some(1),
            num_rows_per_row_block: None,
            bf_fpp: None,
            next_column_unique_id: Some(100),
            deprecated_is_in_memory: None,
            deprecated_id: None,
            compression_type: None,
            sort_key_idxes: vec![0],
            schema_version: Some(0),
            sort_key_unique_ids: vec![10],
            table_indices: Vec::new(),
            compression_level: None,
            id: Some(schema_id),
        }
    }

    fn test_struct_column(unique_id: i32, name: &str, children: Vec<ColumnPb>) -> ColumnPb {
        let mut column = test_scalar_column(unique_id, name, "STRUCT");
        column.children_columns = children;
        column
    }

    fn test_scalar_column(unique_id: i32, name: &str, ty: &str) -> ColumnPb {
        ColumnPb {
            unique_id,
            name: Some(name.to_string()),
            r#type: ty.to_string(),
            aggregation: None,
            is_key: Some(false),
            is_nullable: Some(true),
            default_value: None,
            index_length: None,
            precision: None,
            frac: None,
            length: None,
            children_columns: Vec::new(),
            has_bitmap_index: None,
            is_bf_column: None,
            visible: None,
            is_auto_increment: None,
            referenced_column_id: None,
            referenced_column: None,
            agg_state_desc: None,
        }
    }

    fn test_alter_request(base_schema_hash: i32, new_schema_hash: i32) -> TAlterTabletReqV2 {
        TAlterTabletReqV2::new(
            1,
            2,
            base_schema_hash,
            new_schema_hash,
            None::<i64>,
            None::<Vec<crate::agent_service::TAlterMaterializedViewParam>>,
            None::<crate::agent_service::TTabletType>,
            None::<i64>,
            None::<crate::agent_service::TAlterTabletMaterializedColumnReq>,
            None::<i64>,
            None::<crate::internal_service::TQueryGlobals>,
            None::<crate::internal_service::TQueryOptions>,
            None::<Vec<crate::descriptors::TColumn>>,
            None::<crate::agent_service::TAlterJobType>,
            None::<crate::descriptors::TDescriptorTable>,
            None::<crate::exprs::TExpr>,
            None::<Vec<String>>,
            None::<crate::agent_service::TTabletSchema>,
        )
    }
}
