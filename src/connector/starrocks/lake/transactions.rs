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

use futures::TryStreamExt;
use opendal::ErrorKind;
use prost::Message;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::fs;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::thread::sleep;
use std::time::{Duration, Instant};
use tokio::runtime::Runtime;

use crate::connector::starrocks::lake::abort_executor::abort_one_tablet;
use crate::connector::starrocks::lake::abort_policy::should_skip_abort_cleanup;
use crate::connector::starrocks::lake::applier::apply_txn_log_to_metadata;
use crate::connector::starrocks::lake::context::{
    TabletRuntimeEntry, get_tablet_runtime, remove_tablet_runtime, with_txn_log_append_lock,
};
use crate::connector::starrocks::lake::replay_policy::{
    MissingTxnLogPolicy, decide_missing_txn_log_policy,
};
use crate::connector::starrocks::lake::txn_loader::{LoadedTxnLog, load_txn_logs_for_publish};
use crate::connector::starrocks::lake::txn_log::{
    ensure_rowset_segment_meta_consistency, normalize_rowset_shared_segments,
    read_txn_log_if_exists, write_txn_log_file,
};
use crate::formats::starrocks::writer::bundle_meta::{
    decode_bundle_metadata_from_bytes, decode_tablet_metadata_from_bundle_bytes,
    empty_tablet_metadata, load_tablet_metadata_at_version, next_rowset_id,
    parse_bundle_version_from_meta_file_name, write_bundle_meta_file,
};
use crate::formats::starrocks::writer::io::delete_path_if_exists;
use crate::formats::starrocks::writer::layout::{
    DATA_DIR, LOG_DIR, META_DIR, bundle_meta_file_path, join_tablet_path, txn_log_file_path,
};
use crate::fs::path::{ScanPathScheme, classify_scan_paths, resolve_opendal_paths};
use crate::runtime::starlet_shard_registry::{self, S3StoreConfig};
use crate::service::grpc_client::proto::starrocks::{
    AbortTxnRequest, AbortTxnResponse, CombinedTxnLogPb, DeleteDataRequest, DeleteDataResponse,
    DeletePredicatePb, DeleteTabletRequest, DeleteTabletResponse, DropTableRequest,
    DropTableResponse, PublishVersionRequest, PublishVersionResponse, RowsetMetadataPb, StatusPb,
    TableSchemaKeyPb, TabletInfoPb, TabletMetadataPb, TabletStatRequest, TabletStatResponse,
    TxnInfoPb, TxnLogPb, TxnTypePb, VacuumRequest, VacuumResponse, tablet_stat_response,
    txn_log_pb,
};
use crate::novarocks_logging::{info, warn};

const MAX_PUBLISH_WORKERS: usize = 8;
const STATUS_CODE_OK: i32 = 0;
const DEFAULT_GET_TABLET_STATS_TIMEOUT_MS: i64 = 5 * 60 * 1000;

pub(crate) fn publish_version(
    request: &PublishVersionRequest,
) -> Result<PublishVersionResponse, String> {
    if request.tablet_ids.is_empty() {
        return Ok(PublishVersionResponse {
            failed_tablets: Vec::new(),
            compaction_scores: HashMap::new(),
            status: None,
            tablet_row_nums: HashMap::new(),
            tablet_metas: HashMap::new(),
            tablet_ranges: HashMap::new(),
        });
    }

    let txn_infos = build_publish_txn_infos(request)?;
    if txn_infos.is_empty() {
        return Err("publish_version requires txn_infos or txn_ids".to_string());
    }

    let new_version = request
        .new_version
        .ok_or_else(|| "publish_version missing new_version".to_string())?;
    if new_version <= 0 {
        return Err(format!(
            "publish_version has invalid new_version={new_version}"
        ));
    }
    let base_version = request
        .base_version
        .unwrap_or_else(|| new_version.saturating_sub(txn_infos.len() as i64));
    if base_version < 0 {
        return Err(format!(
            "publish_version has invalid base_version={base_version}"
        ));
    }
    let expected_new_version = base_version.saturating_add(txn_infos.len() as i64);
    if expected_new_version != new_version {
        return Err(format!(
            "publish_version version mismatch: base_version={} txn_count={} expected_new_version={} actual_new_version={}",
            base_version,
            txn_infos.len(),
            expected_new_version,
            new_version
        ));
    }

    let mut failed_tablets = Vec::new();
    let mut tablet_row_nums = HashMap::new();
    let worker_count = publish_worker_count(request.tablet_ids.len());
    if worker_count <= 1 {
        for tablet_id in &request.tablet_ids {
            let publish_res = publish_one_tablet(*tablet_id, base_version, new_version, &txn_infos);
            collect_publish_result(
                *tablet_id,
                publish_res,
                base_version,
                new_version,
                txn_infos.len(),
                &mut failed_tablets,
                &mut tablet_row_nums,
            );
        }
    } else {
        let next_idx = AtomicUsize::new(0);
        let mut worker_join_failed = false;
        std::thread::scope(|scope| {
            let mut handles = Vec::with_capacity(worker_count);
            for _ in 0..worker_count {
                let tablet_ids = &request.tablet_ids;
                let txn_infos = &txn_infos;
                let next_idx = &next_idx;
                handles.push(scope.spawn(move || {
                    let mut local =
                        Vec::with_capacity((tablet_ids.len() / worker_count).saturating_add(1));
                    loop {
                        let idx = next_idx.fetch_add(1, Ordering::Relaxed);
                        if idx >= tablet_ids.len() {
                            break;
                        }
                        let tablet_id = tablet_ids[idx];
                        let publish_res =
                            publish_one_tablet(tablet_id, base_version, new_version, txn_infos);
                        local.push((tablet_id, publish_res));
                    }
                    local
                }));
            }
            for handle in handles {
                match handle.join() {
                    Ok(results) => {
                        for (tablet_id, publish_res) in results {
                            collect_publish_result(
                                tablet_id,
                                publish_res,
                                base_version,
                                new_version,
                                txn_infos.len(),
                                &mut failed_tablets,
                                &mut tablet_row_nums,
                            );
                        }
                    }
                    Err(_) => worker_join_failed = true,
                }
            }
        });
        if worker_join_failed {
            return Err("publish_version worker panicked".to_string());
        }
    }
    failed_tablets.sort_unstable();

    Ok(PublishVersionResponse {
        failed_tablets,
        compaction_scores: HashMap::new(),
        status: None,
        tablet_row_nums,
        tablet_metas: HashMap::new(),
        tablet_ranges: HashMap::new(),
    })
}

fn publish_worker_count(tablet_count: usize) -> usize {
    if tablet_count <= 1 {
        return 1;
    }
    let cpu = std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(1);
    tablet_count.min(cpu.max(1)).min(MAX_PUBLISH_WORKERS)
}

fn collect_publish_result(
    tablet_id: i64,
    publish_res: Result<TabletMetadataPb, String>,
    base_version: i64,
    new_version: i64,
    txn_infos_len: usize,
    failed_tablets: &mut Vec<i64>,
    tablet_row_nums: &mut HashMap<i64, i64>,
) {
    match publish_res {
        Ok(metadata) => {
            if base_version == 1 {
                tablet_row_nums.insert(tablet_id, tablet_row_count(&metadata));
            }
        }
        Err(err) => {
            warn!(
                "publish_version failed for tablet_id={}, base_version={}, new_version={}, txn_infos_len={}, error={}",
                tablet_id, base_version, new_version, txn_infos_len, err
            );
            failed_tablets.push(tablet_id);
        }
    }
}

fn tablet_row_count(metadata: &TabletMetadataPb) -> i64 {
    metadata
        .rowsets
        .iter()
        .map(|rowset| rowset.num_rows.unwrap_or(0))
        .sum()
}

pub(crate) fn abort_txn(request: &AbortTxnRequest) -> Result<AbortTxnResponse, String> {
    if should_skip_abort_cleanup(request.skip_cleanup) {
        return Ok(AbortTxnResponse {
            failed_tablets: Vec::new(),
        });
    }
    if request.tablet_ids.is_empty() {
        return Ok(AbortTxnResponse {
            failed_tablets: Vec::new(),
        });
    }

    let txn_infos = build_abort_txn_infos(request)?;
    if txn_infos.is_empty() {
        return Err("abort_txn requires txn_infos or txn_ids".to_string());
    }

    let mut failed_tablets = Vec::new();
    let mut combined_logs_to_delete = HashSet::new();
    for tablet_id in &request.tablet_ids {
        if abort_one_tablet(*tablet_id, &txn_infos, &mut combined_logs_to_delete).is_err() {
            failed_tablets.push(*tablet_id);
        }
    }

    for path in combined_logs_to_delete {
        let _ = delete_path_if_exists(&path);
    }

    Ok(AbortTxnResponse { failed_tablets })
}

pub(crate) fn drop_table(request: &DropTableRequest) -> Result<DropTableResponse, String> {
    let path = request
        .path
        .as_deref()
        .map(str::trim)
        .filter(|v| !v.is_empty())
        .ok_or_else(|| "drop_table missing path".to_string())?;

    let tablet_id = request.tablet_id.filter(|v| *v > 0);
    let s3_config = tablet_id
        .and_then(|id| {
            get_tablet_runtime(id)
                .ok()
                .and_then(|runtime| runtime.s3_config.clone())
                .or_else(|| {
                    starlet_shard_registry::select_infos(&[id])
                        .get(&id)
                        .and_then(|info| info.s3.clone())
                })
        })
        .or_else(|| starlet_shard_registry::find_s3_config_for_path(path))
        .or_else(|| starlet_shard_registry::infer_s3_config_for_path(path));

    drop_table_path(path, s3_config.as_ref())?;
    if let Some(tablet_id) = tablet_id
        && let Err(err) = remove_tablet_runtime(tablet_id)
    {
        warn!(
            "drop_table cleanup tablet runtime failed: tablet_id={}, error={}",
            tablet_id, err
        );
    }
    Ok(DropTableResponse {
        pad: None,
        status: Some(StatusPb {
            status_code: STATUS_CODE_OK,
            error_msgs: Vec::new(),
        }),
    })
}

pub(crate) fn delete_data(request: &DeleteDataRequest) -> Result<DeleteDataResponse, String> {
    let tablet_ids = normalize_tablet_ids("delete_data", &request.tablet_ids)?;
    let txn_id = request
        .txn_id
        .ok_or_else(|| "delete_data missing txn_id".to_string())?;
    if txn_id <= 0 {
        return Err(format!("delete_data has non-positive txn_id={txn_id}"));
    }
    let delete_predicate = request
        .delete_predicate
        .as_ref()
        .ok_or_else(|| "delete_data missing delete_predicate".to_string())?;

    let mut failed_tablets = Vec::new();
    for tablet_id in tablet_ids {
        if let Err(err) = append_delete_data_txn_log(
            tablet_id,
            txn_id,
            delete_predicate,
            request.schema_key.as_ref(),
        ) {
            warn!(
                "delete_data failed to append txn log: tablet_id={}, txn_id={}, error={}",
                tablet_id, txn_id, err
            );
            failed_tablets.push(tablet_id);
        }
    }

    Ok(DeleteDataResponse { failed_tablets })
}

fn append_delete_data_txn_log(
    tablet_id: i64,
    txn_id: i64,
    delete_predicate: &DeletePredicatePb,
    request_schema_key: Option<&TableSchemaKeyPb>,
) -> Result<(), String> {
    let (tablet_root_path, _) = resolve_tablet_location("delete_data", tablet_id)?;
    let txn_log_path = txn_log_file_path(&tablet_root_path, tablet_id, txn_id)?;
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
                "delete_data txn log tablet_id mismatch: expected={} actual={:?}",
                tablet_id, txn_log.tablet_id
            ));
        }
        if txn_log.txn_id != Some(txn_id) {
            return Err(format!(
                "delete_data txn log txn_id mismatch: expected={} actual={:?}",
                txn_id, txn_log.txn_id
            ));
        }
        if txn_log.op_compaction.is_some()
            || txn_log.op_schema_change.is_some()
            || txn_log.op_alter_metadata.is_some()
            || txn_log.op_replication.is_some()
        {
            return Err(format!(
                "delete_data does not support mixed txn log operation: tablet_id={} txn_id={}",
                tablet_id, txn_id
            ));
        }

        let schema_key = resolve_delete_data_schema_key(
            tablet_id,
            request_schema_key,
            txn_log
                .op_write
                .as_ref()
                .and_then(|op| op.schema_key.as_ref()),
        )?;
        let op_write = txn_log.op_write.get_or_insert_with(|| txn_log_pb::OpWrite {
            rowset: None,
            txn_meta: None,
            dels: Vec::new(),
            rewrite_segments: Vec::new(),
            del_encryption_metas: Vec::new(),
            ssts: Vec::new(),
            schema_key: Some(schema_key.clone()),
        });
        if let Some(existing_schema_key) = op_write.schema_key.as_ref()
            && existing_schema_key.schema_id.is_some()
            && schema_key.schema_id.is_some()
            && existing_schema_key.schema_id != schema_key.schema_id
        {
            return Err(format!(
                "delete_data schema_id mismatch for tablet_id={} txn_id={}: existing={:?} incoming={:?}",
                tablet_id, txn_id, existing_schema_key.schema_id, schema_key.schema_id
            ));
        }
        if op_write.schema_key.is_none() {
            op_write.schema_key = Some(schema_key);
        }

        let rowset = op_write.rowset.get_or_insert_with(|| RowsetMetadataPb {
            id: None,
            overlapped: Some(false),
            segments: Vec::new(),
            num_rows: Some(0),
            data_size: Some(0),
            delete_predicate: Some(delete_predicate.clone()),
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
        if !rowset.segments.is_empty() || rowset.num_rows.unwrap_or(0) > 0 {
            return Err(format!(
                "delete_data found non-empty write rowset in same txn: tablet_id={} txn_id={} segments={} num_rows={}",
                tablet_id,
                txn_id,
                rowset.segments.len(),
                rowset.num_rows.unwrap_or(0)
            ));
        }
        if let Some(existing_predicate) = rowset.delete_predicate.as_ref() {
            if existing_predicate != delete_predicate {
                return Err(format!(
                    "delete_data conflicting delete_predicate in same txn: tablet_id={} txn_id={}",
                    tablet_id, txn_id
                ));
            }
        } else {
            rowset.delete_predicate = Some(delete_predicate.clone());
        }
        normalize_rowset_shared_segments(rowset);
        ensure_rowset_segment_meta_consistency(rowset)?;
        write_txn_log_file(&txn_log_path, &txn_log)
    })
}

fn resolve_delete_data_schema_key(
    tablet_id: i64,
    request_schema_key: Option<&TableSchemaKeyPb>,
    existing_schema_key: Option<&TableSchemaKeyPb>,
) -> Result<TableSchemaKeyPb, String> {
    if let (Some(request), Some(existing)) = (request_schema_key, existing_schema_key)
        && request.schema_id.is_some()
        && existing.schema_id.is_some()
        && request.schema_id != existing.schema_id
    {
        return Err(format!(
            "delete_data schema_key mismatch for tablet_id={}: request_schema_id={:?} existing_schema_id={:?}",
            tablet_id, request.schema_id, existing.schema_id
        ));
    }
    if let Some(existing) = existing_schema_key
        && existing.schema_id.unwrap_or(0) > 0
    {
        return Ok(existing.clone());
    }
    if let Some(request) = request_schema_key
        && request.schema_id.unwrap_or(0) > 0
    {
        return Ok(request.clone());
    }
    if let Ok(runtime) = get_tablet_runtime(tablet_id)
        && let Some(schema_id) = runtime.schema.id
        && schema_id > 0
    {
        let mut fallback = request_schema_key.cloned().unwrap_or_default();
        fallback.schema_id = Some(schema_id);
        return Ok(fallback);
    }
    Err(format!(
        "delete_data missing schema_key with valid schema_id for tablet_id={}",
        tablet_id
    ))
}

pub(crate) fn delete_tablet(request: &DeleteTabletRequest) -> Result<DeleteTabletResponse, String> {
    let tablet_ids = normalize_tablet_ids("delete_tablet", &request.tablet_ids)?;
    if let Some((root_path, s3_config)) = resolve_delete_tablet_root_location(&tablet_ids)? {
        delete_tablets_in_root(&root_path, &tablet_ids, s3_config.as_ref())?;
    } else {
        warn!(
            "delete_tablet skipped physical cleanup because no runtime location is available: tablet_count={}",
            tablet_ids.len()
        );
    }

    for tablet_id in &tablet_ids {
        if let Err(err) = remove_tablet_runtime(*tablet_id) {
            warn!(
                "delete_tablet cleanup tablet runtime failed: tablet_id={}, error={}",
                tablet_id, err
            );
        }
    }

    Ok(DeleteTabletResponse {
        failed_tablets: Vec::new(),
        status: Some(ok_status_pb()),
    })
}

fn resolve_delete_tablet_root_location(
    tablet_ids: &[i64],
) -> Result<Option<(String, Option<S3StoreConfig>)>, String> {
    let mut root_path: Option<String> = None;
    let mut s3_config: Option<Option<S3StoreConfig>> = None;
    let mut resolved_any = false;

    for tablet_id in tablet_ids {
        let (tablet_root, tablet_s3) = match resolve_tablet_location("delete_tablet", *tablet_id) {
            Ok(v) => v,
            Err(err) => {
                warn!(
                    "delete_tablet skip tablet without runtime location: tablet_id={}, error={}",
                    tablet_id, err
                );
                continue;
            }
        };
        resolved_any = true;

        let normalized_root = tablet_root.trim().trim_end_matches('/').to_string();
        if normalized_root.is_empty() {
            return Err(format!(
                "delete_tablet resolved empty root path for tablet_id={tablet_id}"
            ));
        }

        if let Some(existing_root) = root_path.as_ref() {
            if existing_root != &normalized_root {
                return Err(format!(
                    "delete_tablet requires all resolved tablets in one root path: tablet_id={} root_path={} expected_root_path={}",
                    tablet_id, normalized_root, existing_root
                ));
            }
        } else {
            root_path = Some(normalized_root);
        }

        if let Some(existing_s3) = s3_config.as_ref() {
            if existing_s3 != &tablet_s3 {
                return Err(format!(
                    "delete_tablet resolved inconsistent S3 config for tablet_id={tablet_id}"
                ));
            }
        } else {
            s3_config = Some(tablet_s3);
        }
    }

    if !resolved_any {
        return Ok(None);
    }
    let root_path =
        root_path.ok_or_else(|| "delete_tablet missing tablet root path".to_string())?;
    Ok(Some((root_path, s3_config.flatten())))
}

pub(crate) fn vacuum(request: &VacuumRequest) -> Result<VacuumResponse, String> {
    let tablet_infos = normalize_vacuum_tablet_infos(request)?;
    if tablet_infos.is_empty() {
        return Ok(VacuumResponse {
            status: Some(ok_status_pb()),
            vacuumed_files: Some(0),
            vacuumed_file_size: Some(0),
            vacuumed_version: Some(0),
            tablet_infos: Vec::new(),
            extra_file_size: Some(0),
        });
    }

    let tablet_ids = tablet_infos
        .iter()
        .filter_map(|info| info.tablet_id)
        .collect::<Vec<_>>();
    let (root_path, s3_config) = resolve_tablet_root_location("vacuum", &tablet_ids)?;
    let target_set = tablet_ids.iter().copied().collect::<HashSet<_>>();

    let min_retain_version = request
        .min_retain_version
        .or_else(|| {
            tablet_infos
                .iter()
                .filter_map(|info| info.min_version)
                .filter(|v| *v > 0)
                .min()
        })
        .unwrap_or(1);
    if min_retain_version <= 0 {
        return Err(format!(
            "vacuum has non-positive min_retain_version={min_retain_version}"
        ));
    }

    let grace_timestamp = request.grace_timestamp.unwrap_or(0);
    let mut retain_versions = request
        .retain_versions
        .iter()
        .copied()
        .filter(|v| *v > 0)
        .collect::<HashSet<_>>();
    retain_versions.insert(min_retain_version);
    for info in &tablet_infos {
        if let Some(version) = info.min_version.filter(|v| *v > 0) {
            retain_versions.insert(version);
        }
    }

    let mut vacuumed_files = 0_i64;
    let mut vacuumed_file_size = 0_i64;
    let mut last_version_before_grace: Option<i64> = None;
    let mut blocked_by_shared_bundle = false;
    let mut deleted_any_meta = false;
    let mut versions_to_delete = Vec::new();

    for version in list_bundle_versions(&root_path, s3_config.as_ref())? {
        if version >= min_retain_version || retain_versions.contains(&version) {
            continue;
        }
        let meta_path = bundle_meta_file_path(&root_path, version)?;
        let bytes = match crate::formats::starrocks::writer::io::read_bytes_if_exists(&meta_path)? {
            Some(v) => v,
            None => continue,
        };
        let (bundle, _) = decode_bundle_metadata_from_bytes(&bytes).map_err(|err| {
            format!(
                "vacuum decode bundle metadata failed: root_path={} version={} error={}",
                root_path, version, err
            )
        })?;

        let bundle_tablets = bundle
            .tablet_meta_pages
            .keys()
            .copied()
            .collect::<HashSet<_>>();
        if bundle_tablets.is_empty() || !bundle_tablets.iter().any(|id| target_set.contains(id)) {
            continue;
        }
        if bundle_tablets.iter().any(|id| !target_set.contains(id)) {
            blocked_by_shared_bundle = true;
            continue;
        }

        let mut max_commit_time = 0_i64;
        for tablet_id in &tablet_ids {
            if !bundle.tablet_meta_pages.contains_key(tablet_id) {
                continue;
            }
            let metadata = decode_tablet_metadata_from_bundle_bytes(&bytes, *tablet_id, version)
                .map_err(|err| {
                    format!(
                        "vacuum decode tablet metadata failed: root_path={} tablet_id={} version={} error={}",
                        root_path, tablet_id, version, err
                    )
                })?;
            max_commit_time = max_commit_time.max(metadata.commit_time.unwrap_or(0));
        }

        if grace_timestamp > 0 && max_commit_time >= grace_timestamp {
            continue;
        }

        last_version_before_grace = Some(
            last_version_before_grace
                .map(|existing| existing.max(version))
                .unwrap_or(version),
        );
        versions_to_delete.push(version);
    }

    for version in versions_to_delete {
        if Some(version) == last_version_before_grace {
            continue;
        }
        let path = bundle_meta_file_path(&root_path, version)?;
        if let Some(size) = delete_file_with_stats(&path, s3_config.as_ref())? {
            vacuumed_files = vacuumed_files.saturating_add(1);
            vacuumed_file_size = vacuumed_file_size.saturating_add(size);
            deleted_any_meta = true;
        }
    }

    if request.delete_txn_log.unwrap_or(false) {
        let min_active_txn_id = request.min_active_txn_id.unwrap_or(0);
        if min_active_txn_id > 0 {
            let log_dir = join_tablet_path(&root_path, LOG_DIR)?;
            for file_name in list_directory_file_names(&log_dir, s3_config.as_ref())? {
                if let Some((tablet_id, txn_id)) = parse_txn_log_file_name(&file_name) {
                    if target_set.contains(&tablet_id) && txn_id < min_active_txn_id {
                        let path = join_tablet_path(&root_path, &format!("{LOG_DIR}/{file_name}"))?;
                        if let Some(size) = delete_file_with_stats(&path, s3_config.as_ref())? {
                            vacuumed_files = vacuumed_files.saturating_add(1);
                            vacuumed_file_size = vacuumed_file_size.saturating_add(size);
                        }
                    }
                    continue;
                }
                if let Some(txn_id) = parse_combined_txn_log_file_name(&file_name) {
                    if txn_id >= min_active_txn_id {
                        continue;
                    }
                    let path = join_tablet_path(&root_path, &format!("{LOG_DIR}/{file_name}"))?;
                    let combined_log =
                        match crate::formats::starrocks::writer::io::read_bytes_if_exists(&path)? {
                            Some(bytes) => CombinedTxnLogPb::decode(bytes.as_slice()),
                            None => continue,
                        };
                    match combined_log {
                        Ok(log) => {
                            let contains_alive = log
                                .txn_logs
                                .iter()
                                .filter_map(|entry| entry.tablet_id)
                                .any(|tablet_id| !target_set.contains(&tablet_id));
                            if !contains_alive {
                                if let Some(size) =
                                    delete_file_with_stats(&path, s3_config.as_ref())?
                                {
                                    vacuumed_files = vacuumed_files.saturating_add(1);
                                    vacuumed_file_size = vacuumed_file_size.saturating_add(size);
                                }
                            }
                        }
                        Err(err) => {
                            warn!(
                                "vacuum skip malformed combined txn log: path={}, error={}",
                                path, err
                            );
                        }
                    }
                }
            }
        }
    }

    let mut vacuumed_version = last_version_before_grace.unwrap_or(min_retain_version);
    if !deleted_any_meta && blocked_by_shared_bundle {
        if let Some(min_version) = tablet_infos
            .iter()
            .filter_map(|info| info.min_version)
            .filter(|v| *v > 0)
            .min()
        {
            vacuumed_version = min_version;
        }
    }

    let response_tablet_infos = tablet_infos
        .into_iter()
        .map(|info| TabletInfoPb {
            tablet_id: info.tablet_id,
            min_version: Some(vacuumed_version.max(1)),
        })
        .collect::<Vec<_>>();

    Ok(VacuumResponse {
        status: Some(ok_status_pb()),
        vacuumed_files: Some(vacuumed_files),
        vacuumed_file_size: Some(vacuumed_file_size),
        vacuumed_version: Some(vacuumed_version),
        tablet_infos: response_tablet_infos,
        extra_file_size: Some(0),
    })
}

pub(crate) fn get_tablet_stats(request: &TabletStatRequest) -> Result<TabletStatResponse, String> {
    if request.tablet_infos.is_empty() {
        return Err("get_tablet_stats missing tablet_infos".to_string());
    }

    let timeout_ms = request
        .timeout_ms
        .unwrap_or(DEFAULT_GET_TABLET_STATS_TIMEOUT_MS)
        .max(0);
    let deadline = Instant::now()
        .checked_add(Duration::from_millis(timeout_ms as u64))
        .unwrap_or_else(Instant::now);

    let mut tablet_stats = Vec::with_capacity(request.tablet_infos.len());
    for tablet_info in &request.tablet_infos {
        if Instant::now() >= deadline {
            break;
        }

        let tablet_id = tablet_info.tablet_id.unwrap_or(0);
        let version = tablet_info.version.unwrap_or(0);
        if tablet_id <= 0 || version <= 0 {
            warn!(
                "get_tablet_stats skip invalid tablet info: tablet_id={}, version={}",
                tablet_id, version
            );
            continue;
        }

        match compute_tablet_stat(tablet_id, version) {
            Ok(Some(stat)) => tablet_stats.push(stat),
            Ok(None) => {}
            Err(err) => warn!(
                "get_tablet_stats failed to collect tablet stat: tablet_id={}, version={}, error={}",
                tablet_id, version, err
            ),
        }
    }

    Ok(TabletStatResponse { tablet_stats })
}

fn compute_tablet_stat(
    tablet_id: i64,
    version: i64,
) -> Result<Option<tablet_stat_response::TabletStat>, String> {
    let (root_path, _) = resolve_tablet_location("get_tablet_stats", tablet_id)?;
    let metadata = match load_tablet_metadata_at_version(&root_path, tablet_id, version)? {
        Some(v) => v,
        None => return Ok(None),
    };

    let mut num_rows = 0_i64;
    let mut data_size = 0_i64;
    for rowset in &metadata.rowsets {
        let rows = rowset.num_rows.unwrap_or(0).max(0);
        let deletes = rowset.num_dels.unwrap_or(0).max(0);
        num_rows = num_rows.saturating_add(rows.saturating_sub(deletes));
        data_size = data_size.saturating_add(rowset.data_size.unwrap_or(0).max(0));
    }
    if let Some(delvec_meta) = metadata.delvec_meta.as_ref() {
        for file in delvec_meta.version_to_file.values() {
            data_size = data_size.saturating_add(file.size.unwrap_or(0).max(0));
        }
    }

    Ok(Some(tablet_stat_response::TabletStat {
        tablet_id: Some(tablet_id),
        num_rows: Some(num_rows),
        data_size: Some(data_size),
    }))
}

fn ok_status_pb() -> StatusPb {
    StatusPb {
        status_code: STATUS_CODE_OK,
        error_msgs: Vec::new(),
    }
}

fn normalize_tablet_ids(op: &str, raw_ids: &[i64]) -> Result<Vec<i64>, String> {
    if raw_ids.is_empty() {
        return Err(format!("{op} missing tablet_ids"));
    }
    let mut tablet_ids = Vec::with_capacity(raw_ids.len());
    for tablet_id in raw_ids {
        if *tablet_id <= 0 {
            return Err(format!("{op} has non-positive tablet_id={tablet_id}"));
        }
        tablet_ids.push(*tablet_id);
    }
    tablet_ids.sort_unstable();
    tablet_ids.dedup();
    Ok(tablet_ids)
}

fn normalize_vacuum_tablet_infos(request: &VacuumRequest) -> Result<Vec<TabletInfoPb>, String> {
    let mut per_tablet_min_version = BTreeMap::<i64, i64>::new();
    let default_min_version = request.min_retain_version.filter(|v| *v > 0);

    if !request.tablet_infos.is_empty() {
        for info in &request.tablet_infos {
            let tablet_id = info.tablet_id.ok_or_else(|| {
                "vacuum tablet_infos contains entry without tablet_id".to_string()
            })?;
            if tablet_id <= 0 {
                return Err(format!("vacuum has non-positive tablet_id={tablet_id}"));
            }
            let min_version = info
                .min_version
                .filter(|v| *v > 0)
                .or(default_min_version)
                .unwrap_or(0);
            per_tablet_min_version
                .entry(tablet_id)
                .and_modify(|existing| *existing = (*existing).max(min_version))
                .or_insert(min_version);
        }
    } else {
        for tablet_id in &request.tablet_ids {
            if *tablet_id <= 0 {
                return Err(format!("vacuum has non-positive tablet_id={tablet_id}"));
            }
            let min_version = default_min_version.unwrap_or(0);
            per_tablet_min_version
                .entry(*tablet_id)
                .and_modify(|existing| *existing = (*existing).max(min_version))
                .or_insert(min_version);
        }
    }

    Ok(per_tablet_min_version
        .into_iter()
        .map(|(tablet_id, min_version)| TabletInfoPb {
            tablet_id: Some(tablet_id),
            min_version: (min_version > 0).then_some(min_version),
        })
        .collect())
}

fn resolve_tablet_root_location(
    op: &str,
    tablet_ids: &[i64],
) -> Result<(String, Option<S3StoreConfig>), String> {
    let mut root_path: Option<String> = None;
    let mut s3_config: Option<Option<S3StoreConfig>> = None;
    for tablet_id in tablet_ids {
        let (tablet_root, tablet_s3) = resolve_tablet_location(op, *tablet_id)?;
        let normalized_root = tablet_root.trim().trim_end_matches('/').to_string();
        if normalized_root.is_empty() {
            return Err(format!(
                "{op} resolved empty root path for tablet_id={tablet_id}"
            ));
        }
        if let Some(existing_root) = root_path.as_ref() {
            if existing_root != &normalized_root {
                return Err(format!(
                    "{op} requires all tablets in one root path: tablet_id={} root_path={} expected_root_path={}",
                    tablet_id, normalized_root, existing_root
                ));
            }
        } else {
            root_path = Some(normalized_root);
        }

        if let Some(existing_s3) = s3_config.as_ref() {
            if existing_s3 != &tablet_s3 {
                return Err(format!(
                    "{op} resolved inconsistent S3 config for tablet_id={tablet_id}"
                ));
            }
        } else {
            s3_config = Some(tablet_s3);
        }
    }
    let root_path = root_path.ok_or_else(|| format!("{op} missing tablet root path"))?;
    Ok((root_path, s3_config.flatten()))
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

fn delete_tablets_in_root(
    root_path: &str,
    tablet_ids: &[i64],
    s3_config: Option<&S3StoreConfig>,
) -> Result<(), String> {
    let target_set = tablet_ids.iter().copied().collect::<HashSet<_>>();
    let mut file_paths = HashSet::<String>::new();
    let mut data_file_names = HashSet::<String>::new();
    let mut has_target_metadata = false;
    let mut has_alive_tablets_in_bundle = false;

    for version in list_bundle_versions(root_path, s3_config)? {
        let meta_path = bundle_meta_file_path(root_path, version)?;
        let bytes = match crate::formats::starrocks::writer::io::read_bytes_if_exists(&meta_path)? {
            Some(v) => v,
            None => continue,
        };
        let (bundle, _) = decode_bundle_metadata_from_bytes(&bytes).map_err(|err| {
            format!(
                "delete_tablet decode bundle metadata failed: root_path={} version={} error={}",
                root_path, version, err
            )
        })?;
        if !bundle
            .tablet_meta_pages
            .keys()
            .any(|tablet_id| target_set.contains(tablet_id))
        {
            continue;
        }
        has_target_metadata = true;

        let contains_alive = bundle
            .tablet_meta_pages
            .keys()
            .any(|tablet_id| !target_set.contains(tablet_id));
        if contains_alive {
            has_alive_tablets_in_bundle = true;
        } else {
            file_paths.insert(meta_path);
            for tablet_id in tablet_ids {
                if !bundle.tablet_meta_pages.contains_key(tablet_id) {
                    continue;
                }
                let metadata =
                    decode_tablet_metadata_from_bundle_bytes(&bytes, *tablet_id, version).map_err(
                        |err| {
                            format!(
                                "delete_tablet decode tablet metadata failed: root_path={} tablet_id={} version={} error={}",
                                root_path, tablet_id, version, err
                            )
                        },
                    )?;
                collect_unshared_data_files_from_metadata(&metadata, &mut data_file_names);
            }
        }
    }

    // If all bundle metadata files only contain deleting tablets, dropping the whole
    // root path is the safest and closest behavior to StarRocks delete_tablets.
    if has_target_metadata && !has_alive_tablets_in_bundle {
        return drop_table_path(root_path, s3_config);
    }

    for file_name in data_file_names {
        let path = join_data_file_path(root_path, &file_name)?;
        file_paths.insert(path);
    }

    let log_dir = join_tablet_path(root_path, LOG_DIR)?;
    for file_name in list_directory_file_names(&log_dir, s3_config)? {
        if let Some((tablet_id, _txn_id)) = parse_txn_log_file_name(&file_name) {
            if target_set.contains(&tablet_id) {
                file_paths.insert(join_tablet_path(
                    root_path,
                    &format!("{LOG_DIR}/{file_name}"),
                )?);
            }
            continue;
        }
        if parse_combined_txn_log_file_name(&file_name).is_some() {
            let path = join_tablet_path(root_path, &format!("{LOG_DIR}/{file_name}"))?;
            let combined_log = crate::formats::starrocks::writer::io::read_bytes_if_exists(&path)?
                .and_then(|bytes| CombinedTxnLogPb::decode(bytes.as_slice()).ok());
            if let Some(log) = combined_log {
                let contains_alive = log
                    .txn_logs
                    .iter()
                    .filter_map(|entry| entry.tablet_id)
                    .any(|tablet_id| !target_set.contains(&tablet_id));
                if !contains_alive {
                    file_paths.insert(path);
                }
            }
        }
    }

    let mut sorted_paths = file_paths.into_iter().collect::<Vec<_>>();
    sorted_paths.sort_unstable();
    for path in sorted_paths {
        delete_path_if_exists(&path)?;
    }
    Ok(())
}

fn list_bundle_versions(
    root_path: &str,
    s3_config: Option<&S3StoreConfig>,
) -> Result<Vec<i64>, String> {
    let meta_dir = join_tablet_path(root_path, META_DIR)?;
    let mut versions = list_directory_file_names(&meta_dir, s3_config)?
        .into_iter()
        .filter_map(|name| parse_bundle_version_from_meta_file_name(&name))
        .collect::<Vec<_>>();
    versions.sort_unstable();
    versions.dedup();
    Ok(versions)
}

fn list_directory_file_names(
    dir_path: &str,
    s3_config: Option<&S3StoreConfig>,
) -> Result<Vec<String>, String> {
    match classify_scan_paths([dir_path])? {
        ScanPathScheme::Local => {
            let dir = std::path::PathBuf::from(dir_path);
            if !dir.exists() {
                return Ok(Vec::new());
            }
            if !dir.is_dir() {
                return Err(format!("path is not a directory: {dir_path}"));
            }
            let mut names = Vec::new();
            let entries = fs::read_dir(&dir)
                .map_err(|e| format!("read directory failed: path={}, error={}", dir_path, e))?;
            for entry in entries {
                let entry = entry.map_err(|e| {
                    format!("iterate directory failed: path={}, error={}", dir_path, e)
                })?;
                let file_type = entry.file_type().map_err(|e| {
                    format!(
                        "read directory entry file type failed: path={}, error={}",
                        dir_path, e
                    )
                })?;
                if !file_type.is_file() {
                    continue;
                }
                let Some(name) = entry.file_name().to_str().map(|v| v.to_string()) else {
                    continue;
                };
                names.push(name);
            }
            Ok(names)
        }
        ScanPathScheme::Oss => {
            let object_store_cfg = s3_config
                .ok_or_else(|| {
                    format!("missing S3 config for object-store directory listing: path={dir_path}")
                })?
                .to_object_store_config();
            let input_paths = vec![dir_path.to_string()];
            let (op, resolved) = resolve_opendal_paths(&input_paths, Some(&object_store_cfg))?;
            let rel_path = resolved
                .paths
                .first()
                .cloned()
                .ok_or_else(|| format!("resolved empty path list for path={dir_path}"))?;
            let list_prefix = if rel_path.is_empty() {
                String::new()
            } else {
                format!("{}/", rel_path.trim_end_matches('/'))
            };

            let rt = Runtime::new().map_err(|e| format!("init tokio runtime failed: {e}"))?;
            rt.block_on(async move {
                let mut names = Vec::new();
                let mut lister = op
                    .lister_with(&list_prefix)
                    .recursive(false)
                    .await
                    .map_err(|e| {
                        format!(
                            "list object-store directory failed: path={}, error={}",
                            dir_path, e
                        )
                    })?;
                while let Some(entry) = lister.try_next().await.map_err(|e| {
                    format!(
                        "iterate object-store directory failed: path={}, error={}",
                        dir_path, e
                    )
                })? {
                    let path = entry.path().trim_end_matches('/');
                    if path.is_empty() {
                        continue;
                    }
                    let name = path.rsplit('/').next().unwrap_or(path).trim();
                    if name.is_empty() {
                        continue;
                    }
                    names.push(name.to_string());
                }
                Ok(names)
            })
        }
    }
}

fn parse_txn_log_file_name(name: &str) -> Option<(i64, i64)> {
    let stem = name.strip_suffix(".log")?;
    let parts = stem.split('_').collect::<Vec<_>>();
    if parts.len() != 2 && parts.len() != 4 {
        return None;
    }
    let tablet_id = parse_hex_i64(parts[0])?;
    let txn_id = parse_hex_i64(parts[1])?;
    if tablet_id <= 0 || txn_id <= 0 {
        return None;
    }
    Some((tablet_id, txn_id))
}

fn parse_combined_txn_log_file_name(name: &str) -> Option<i64> {
    let stem = name.strip_suffix(".logs")?;
    let txn_id = parse_hex_i64(stem)?;
    if txn_id <= 0 {
        return None;
    }
    Some(txn_id)
}

fn parse_hex_i64(token: &str) -> Option<i64> {
    let parsed = u64::from_str_radix(token, 16).ok()?;
    if parsed > i64::MAX as u64 {
        return None;
    }
    Some(parsed as i64)
}

fn collect_unshared_data_files_from_metadata(
    metadata: &TabletMetadataPb,
    out: &mut HashSet<String>,
) {
    for rowset in &metadata.rowsets {
        collect_unshared_data_files_from_rowset(rowset, out);
    }
    for rowset in &metadata.compaction_inputs {
        collect_unshared_data_files_from_rowset(rowset, out);
    }
    if let Some(delvec_meta) = metadata.delvec_meta.as_ref() {
        for file in delvec_meta.version_to_file.values() {
            if file.shared.unwrap_or(false) {
                continue;
            }
            if let Some(name) = file
                .name
                .as_ref()
                .map(|v| v.trim())
                .filter(|v| !v.is_empty())
            {
                out.insert(name.to_string());
            }
        }
    }
    for file in &metadata.orphan_files {
        if file.shared.unwrap_or(false) {
            continue;
        }
        if let Some(name) = file
            .name
            .as_ref()
            .map(|v| v.trim())
            .filter(|v| !v.is_empty())
        {
            out.insert(name.to_string());
        }
    }
    if let Some(sstable_meta) = metadata.sstable_meta.as_ref() {
        for sstable in &sstable_meta.sstables {
            if sstable.shared.unwrap_or(false) {
                continue;
            }
            if let Some(name) = sstable
                .filename
                .as_ref()
                .map(|v| v.trim())
                .filter(|v| !v.is_empty())
            {
                out.insert(name.to_string());
            }
        }
    }
    if let Some(dcg_meta) = metadata.dcg_meta.as_ref() {
        for dcg in dcg_meta.dcgs.values() {
            for (idx, file_name) in dcg.column_files.iter().enumerate() {
                if dcg.shared_files.get(idx).copied().unwrap_or(false) {
                    continue;
                }
                let trimmed = file_name.trim();
                if !trimmed.is_empty() {
                    out.insert(trimmed.to_string());
                }
            }
        }
    }
}

fn collect_unshared_data_files_from_rowset(rowset: &RowsetMetadataPb, out: &mut HashSet<String>) {
    for (idx, segment) in rowset.segments.iter().enumerate() {
        if rowset.shared_segments.get(idx).copied().unwrap_or(false) {
            continue;
        }
        let trimmed = segment.trim();
        if !trimmed.is_empty() {
            out.insert(trimmed.to_string());
        }
    }
    for del_file in &rowset.del_files {
        if del_file.shared.unwrap_or(false) {
            continue;
        }
        if let Some(name) = del_file
            .name
            .as_ref()
            .map(|v| v.trim())
            .filter(|v| !v.is_empty())
        {
            out.insert(name.to_string());
        }
    }
}

fn join_data_file_path(root_path: &str, file_name: &str) -> Result<String, String> {
    let file_name = file_name.trim();
    if file_name.is_empty() {
        return Err("data file name is empty".to_string());
    }
    if file_name.contains('/') {
        join_tablet_path(root_path, file_name)
    } else {
        join_tablet_path(root_path, &format!("{DATA_DIR}/{file_name}"))
    }
}

fn delete_file_with_stats(
    path: &str,
    s3_config: Option<&S3StoreConfig>,
) -> Result<Option<i64>, String> {
    let Some(size) = file_size_if_exists(path, s3_config)? else {
        return Ok(None);
    };
    delete_path_if_exists(path)?;
    Ok(Some(size))
}

fn file_size_if_exists(
    path: &str,
    s3_config: Option<&S3StoreConfig>,
) -> Result<Option<i64>, String> {
    match classify_scan_paths([path])? {
        ScanPathScheme::Local => {
            let path_buf = std::path::PathBuf::from(path);
            if !path_buf.exists() {
                return Ok(None);
            }
            let metadata = fs::metadata(&path_buf)
                .map_err(|e| format!("stat file failed: path={}, error={}", path, e))?;
            if !metadata.is_file() {
                return Ok(None);
            }
            Ok(Some(i64::try_from(metadata.len()).unwrap_or(i64::MAX)))
        }
        ScanPathScheme::Oss => {
            let object_store_cfg = s3_config
                .ok_or_else(|| format!("missing S3 config for object-store stat: path={path}"))?
                .to_object_store_config();
            let (op, rel) =
                crate::fs::oss::resolve_oss_operator_and_path_with_config(path, &object_store_cfg)?;
            match crate::fs::oss::oss_block_on(op.stat(&rel))? {
                Ok(meta) => Ok(Some(
                    i64::try_from(meta.content_length()).unwrap_or(i64::MAX),
                )),
                Err(err) if err.kind() == ErrorKind::NotFound => Ok(None),
                Err(err) => Err(format!(
                    "stat object failed: path={} relative_path={} error={}",
                    path, rel, err
                )),
            }
        }
    }
}

fn drop_table_path(path: &str, s3_config: Option<&S3StoreConfig>) -> Result<(), String> {
    let scheme = classify_scan_paths([path])?;
    let input_paths = vec![path.to_string()];

    let object_store_cfg = if matches!(scheme, ScanPathScheme::Oss) {
        Some(
            s3_config
                .ok_or_else(|| {
                    format!(
                        "drop_table missing S3 config for object-store path={path}; \
                        expected tablet runtime or AddShard cache to provide credentials"
                    )
                })?
                .to_object_store_config(),
        )
    } else {
        None
    };

    let (op, resolved) = resolve_opendal_paths(&input_paths, object_store_cfg.as_ref())?;
    let rel_path = resolved
        .paths
        .first()
        .ok_or_else(|| format!("drop_table resolved empty path list for path={path}"))?
        .as_str();
    if rel_path.is_empty() {
        return Err(format!(
            "drop_table path resolves to empty relative path: path={path}"
        ));
    }

    let rt = Runtime::new().map_err(|e| format!("init tokio runtime failed: {e}"))?;
    let max_attempts = 3usize;
    for attempt in 1..=max_attempts {
        match rt.block_on(op.remove_all(rel_path)) {
            Ok(()) => return Ok(()),
            Err(e) if e.is_temporary() && attempt < max_attempts => {
                warn!(
                    "drop_table temporary remove failure, will retry: path={}, attempt={}/{}, error={}",
                    path, attempt, max_attempts, e
                );
                sleep(Duration::from_millis((attempt as u64) * 300));
            }
            Err(e) => {
                return Err(format!(
                    "drop_table remove path failed: path={path}, error={e}"
                ));
            }
        }
    }
    Err(format!(
        "drop_table remove path failed after retries: path={path}"
    ))
}

#[allow(dead_code)]
fn build_publish_txn_infos(request: &PublishVersionRequest) -> Result<Vec<TxnInfoPb>, String> {
    if !request.txn_infos.is_empty() {
        let mut txn_infos = Vec::with_capacity(request.txn_infos.len());
        let mut seen_txn_ids = HashSet::with_capacity(request.txn_infos.len());
        for info in &request.txn_infos {
            let txn_id = info.txn_id.ok_or_else(|| {
                "publish_version txn_infos contains entry without txn_id".to_string()
            })?;
            if txn_id <= 0 {
                return Err(format!(
                    "publish_version txn_infos has non-positive txn_id={txn_id}"
                ));
            }
            if !seen_txn_ids.insert(txn_id) {
                return Err(format!(
                    "publish_version txn_infos has duplicate txn_id={txn_id}"
                ));
            }
            txn_infos.push(info.clone());
        }
        return Ok(txn_infos);
    }

    if !request.txn_ids.is_empty() {
        let mut txn_infos = Vec::with_capacity(request.txn_ids.len());
        let mut seen_txn_ids = HashSet::with_capacity(request.txn_ids.len());
        for txn_id in &request.txn_ids {
            if *txn_id <= 0 {
                return Err(format!(
                    "publish_version txn_ids has non-positive txn_id={txn_id}"
                ));
            }
            if !seen_txn_ids.insert(*txn_id) {
                return Err(format!(
                    "publish_version txn_ids has duplicate txn_id={txn_id}"
                ));
            }
            txn_infos.push(TxnInfoPb {
                txn_id: Some(*txn_id),
                commit_time: request.commit_time,
                combined_txn_log: Some(false),
                txn_type: Some(TxnTypePb::TxnNormal as i32),
                force_publish: Some(false),
                rebuild_pindex: Some(false),
                gtid: Some(0),
                load_ids: Vec::new(),
            });
        }
        return Ok(txn_infos);
    }

    Ok(Vec::new())
}

fn build_abort_txn_infos(request: &AbortTxnRequest) -> Result<Vec<TxnInfoPb>, String> {
    if !request.txn_infos.is_empty() {
        let mut txn_infos = Vec::with_capacity(request.txn_infos.len());
        let mut seen_txn_ids = HashSet::with_capacity(request.txn_infos.len());
        for info in &request.txn_infos {
            let txn_id = info
                .txn_id
                .ok_or_else(|| "abort_txn txn_infos contains entry without txn_id".to_string())?;
            if txn_id <= 0 {
                return Err(format!(
                    "abort_txn txn_infos has non-positive txn_id={txn_id}"
                ));
            }
            if !seen_txn_ids.insert(txn_id) {
                return Err(format!("abort_txn txn_infos has duplicate txn_id={txn_id}"));
            }
            txn_infos.push(info.clone());
        }
        return Ok(txn_infos);
    }

    if !request.txn_ids.is_empty() {
        let mut txn_infos = Vec::with_capacity(request.txn_ids.len());
        let mut seen_txn_ids = HashSet::with_capacity(request.txn_ids.len());
        for (idx, txn_id) in request.txn_ids.iter().enumerate() {
            if *txn_id <= 0 {
                return Err(format!(
                    "abort_txn txn_ids has non-positive txn_id={txn_id}"
                ));
            }
            if !seen_txn_ids.insert(*txn_id) {
                return Err(format!("abort_txn txn_ids has duplicate txn_id={txn_id}"));
            }
            let txn_type = if idx < request.txn_types.len() {
                Some(request.txn_types[idx])
            } else {
                Some(TxnTypePb::TxnNormal as i32)
            };
            txn_infos.push(TxnInfoPb {
                txn_id: Some(*txn_id),
                commit_time: None,
                combined_txn_log: Some(false),
                txn_type,
                force_publish: Some(false),
                rebuild_pindex: Some(false),
                gtid: Some(0),
                load_ids: Vec::new(),
            });
        }
        return Ok(txn_infos);
    }

    Ok(Vec::new())
}

#[derive(Debug)]
struct PublishTabletState {
    tablet_id: i64,
    root_path: String,
    schema_id: i64,
    current_base_version: i64,
    new_version: i64,
    metadata: TabletMetadataPb,
}

enum PublishInit {
    AlreadyPublished(TabletMetadataPb),
    Ready(PublishTabletState),
}

enum TxnStepDecision {
    ApplyLogs(Vec<LoadedTxnLog>),
    SkipTxn,
    ReturnPublished(TabletMetadataPb),
}

fn publish_one_tablet(
    tablet_id: i64,
    base_version: i64,
    new_version: i64,
    txn_infos: &[TxnInfoPb],
) -> Result<TabletMetadataPb, String> {
    if tablet_id <= 0 {
        return Err(format!(
            "publish_version has non-positive tablet_id={tablet_id}"
        ));
    }
    let runtime = get_tablet_runtime(tablet_id)?;
    let mut state = match initialize_publish_state(
        tablet_id,
        base_version,
        new_version,
        &runtime,
        txn_infos,
    )? {
        PublishInit::AlreadyPublished(meta) => return Ok(meta),
        PublishInit::Ready(state) => state,
    };

    for (txn_idx, txn_info) in txn_infos.iter().enumerate() {
        let txn_id = txn_info
            .txn_id
            .ok_or_else(|| "publish_version txn_info missing txn_id".to_string())?;
        let force_publish = txn_info.force_publish.unwrap_or(false);
        let txn_logs = load_txn_logs_for_publish(&state.root_path, tablet_id, txn_info)?;
        if txn_logs.is_empty() {
            info!(
                target: "novarocks::lake",
                tablet_id,
                txn_id,
                txn_idx,
                txn_count = txn_infos.len(),
                base_version = state.current_base_version,
                target_version = state.new_version,
                root_path = %state.root_path,
                combined_txn_log = txn_info.combined_txn_log.unwrap_or(false),
                load_id_count = txn_info.load_ids.len(),
                "LAKE_PUBLISH no txn logs loaded for publish step"
            );
        } else {
            info!(
                target: "novarocks::lake",
                tablet_id,
                txn_id,
                txn_idx,
                txn_count = txn_infos.len(),
                loaded_txn_logs = txn_logs.len(),
                base_version = state.current_base_version,
                target_version = state.new_version,
                "LAKE_PUBLISH loaded txn logs for publish step"
            );
        }
        match decide_txn_step(
            &mut state,
            txn_infos.len(),
            txn_idx,
            txn_id,
            force_publish,
            txn_logs,
        )? {
            TxnStepDecision::ApplyLogs(logs) => {
                let apply_version = state.current_base_version.saturating_add(1);
                let before_rowset_count = state.metadata.rowsets.len();
                let before_total_rows = tablet_row_count(&state.metadata);
                let log_count = logs.len();
                for loaded in logs {
                    apply_txn_log_to_metadata(
                        &mut state.metadata,
                        &loaded.log,
                        state.schema_id,
                        &runtime.schema,
                        &state.root_path,
                        runtime.s3_config.as_ref(),
                        apply_version,
                    )?;
                }
                let after_rowset_count = state.metadata.rowsets.len();
                let after_total_rows = tablet_row_count(&state.metadata);
                let delvec_segment_count = state
                    .metadata
                    .delvec_meta
                    .as_ref()
                    .map(|meta| meta.delvecs.len())
                    .unwrap_or(0);
                info!(
                    target: "novarocks::lake",
                    tablet_id,
                    txn_id,
                    apply_version,
                    log_count,
                    before_rowset_count,
                    after_rowset_count,
                    before_total_rows,
                    after_total_rows,
                    delvec_segment_count,
                    "LAKE_PUBLISH applied txn logs to metadata"
                );
                state.current_base_version = apply_version;
            }
            TxnStepDecision::SkipTxn => {
                info!(
                    target: "novarocks::lake",
                    tablet_id,
                    txn_id,
                    txn_idx,
                    txn_count = txn_infos.len(),
                    current_base_version = state.current_base_version,
                    target_version = state.new_version,
                    "LAKE_PUBLISH skipped publish step due missing log policy"
                );
                continue;
            }
            TxnStepDecision::ReturnPublished(meta) => {
                info!(
                    target: "novarocks::lake",
                    tablet_id,
                    txn_id,
                    txn_idx,
                    txn_count = txn_infos.len(),
                    returned_meta_version = meta.version.unwrap_or(0),
                    returned_rowset_count = meta.rowsets.len(),
                    returned_total_rows = tablet_row_count(&meta),
                    "LAKE_PUBLISH returned already-published metadata"
                );
                return Ok(meta);
            }
        }
    }

    finalize_publish_metadata(&mut state, txn_infos);

    write_bundle_meta_file(
        &state.root_path,
        tablet_id,
        state.new_version,
        &runtime.schema,
        &state.metadata,
    )?;
    info!(
        target: "novarocks::lake",
        tablet_id,
        new_version = state.new_version,
        final_rowset_count = state.metadata.rowsets.len(),
        final_total_rows = tablet_row_count(&state.metadata),
        "LAKE_PUBLISH wrote final bundle metadata"
    );

    // Keep BE-compatible conservative cleanup semantics:
    // - Only single publish may cleanup txn logs immediately.
    // - Batch publish keeps txn logs for retry/mode-switch replay safety.
    if should_cleanup_txn_log_after_publish(txn_infos) {
        let txn_id = txn_infos[0]
            .txn_id
            .ok_or_else(|| "publish_version txn_info missing txn_id".to_string())?;
        let path = txn_log_file_path(&state.root_path, tablet_id, txn_id)?;
        if let Err(err) = delete_path_if_exists(&path) {
            warn!(
                "publish_version cleanup txn log failed: tablet_id={}, path={}, error={}",
                tablet_id, path, err
            );
        }
    }
    Ok(state.metadata)
}

fn initialize_publish_state(
    tablet_id: i64,
    base_version: i64,
    new_version: i64,
    runtime: &TabletRuntimeEntry,
    txn_infos: &[TxnInfoPb],
) -> Result<PublishInit, String> {
    let has_logs_for_this_tablet =
        !has_no_publish_logs_for_tablet(&runtime.root_path, tablet_id, txn_infos)?;
    let existing_published =
        probe_new_version_metadata(&runtime.root_path, tablet_id, new_version, true)?;
    if let Some(existing) = existing_published
        && !has_logs_for_this_tablet
        && is_existing_metadata_published_for_target_txn(&existing, txn_infos)
    {
        return Ok(PublishInit::AlreadyPublished(existing));
    }
    if base_version > new_version {
        return Err(format!(
            "publish_version invalid versions for tablet_id={}: base_version={} > new_version={}",
            tablet_id, base_version, new_version
        ));
    }
    let current_base_version = base_version;
    let can_bootstrap_missing_base_metadata =
        current_base_version == 1 || is_internal_statistics_tablet_root(&runtime.root_path);
    let metadata = if current_base_version == 0 {
        empty_tablet_metadata(tablet_id)
    } else {
        let base_metadata = load_tablet_metadata_with_missing_page_policy(
            &runtime.root_path,
            tablet_id,
            current_base_version,
            true,
        )?;
        match base_metadata {
            Some(meta) => meta,
            None => {
                if let Some(published_meta) =
                    probe_new_version_metadata(&runtime.root_path, tablet_id, new_version, true)?
                        .filter(|_| !has_logs_for_this_tablet)
                        .filter(|meta| {
                            is_existing_metadata_published_for_target_txn(meta, txn_infos)
                        })
                {
                    return Ok(PublishInit::AlreadyPublished(published_meta));
                }
                let no_txn_logs_for_this_tablet =
                    has_no_publish_logs_for_tablet(&runtime.root_path, tablet_id, txn_infos)?;
                if can_bootstrap_missing_base_metadata || no_txn_logs_for_this_tablet {
                    // Allow untouched tablets to advance publish lineage even when
                    // partition-root bundle metadata doesn't contain a tablet page yet.
                    empty_tablet_metadata(tablet_id)
                } else {
                    return Err(format!(
                        "base tablet metadata not found: tablet_id={} base_version={}",
                        tablet_id, current_base_version
                    ));
                }
            }
        }
    };
    let schema_id = runtime
        .schema
        .id
        .filter(|v| *v > 0)
        .ok_or_else(|| format!("tablet schema id is missing for tablet_id={tablet_id}"))?;
    Ok(PublishInit::Ready(PublishTabletState {
        tablet_id,
        root_path: runtime.root_path.clone(),
        schema_id,
        current_base_version,
        new_version,
        metadata,
    }))
}

fn is_existing_metadata_published_for_target_txn(
    metadata: &TabletMetadataPb,
    txn_infos: &[TxnInfoPb],
) -> bool {
    let Some(last_txn) = txn_infos.last() else {
        return false;
    };
    if let Some(expected_gtid) = last_txn.gtid {
        if expected_gtid <= 0 {
            return false;
        }
        if metadata.gtid == Some(expected_gtid) {
            return true;
        }
    }
    if let Some(expected_commit_time) = last_txn.commit_time {
        if expected_commit_time <= 0 {
            return false;
        }
        return metadata.commit_time == Some(expected_commit_time);
    }
    false
}

fn is_internal_statistics_tablet_root(root_path: &str) -> bool {
    let lowered = root_path.to_ascii_lowercase();
    lowered.contains("/db10001/") || lowered.contains("db10001/")
}

fn has_no_publish_logs_for_tablet(
    root_path: &str,
    tablet_id: i64,
    txn_infos: &[TxnInfoPb],
) -> Result<bool, String> {
    for txn_info in txn_infos {
        let logs = load_txn_logs_for_publish(root_path, tablet_id, txn_info)?;
        if !logs.is_empty() {
            return Ok(false);
        }
    }
    Ok(true)
}

fn decide_txn_step(
    state: &mut PublishTabletState,
    txn_count: usize,
    txn_idx: usize,
    txn_id: i64,
    force_publish: bool,
    txn_logs: Vec<LoadedTxnLog>,
) -> Result<TxnStepDecision, String> {
    if !txn_logs.is_empty() {
        return Ok(TxnStepDecision::ApplyLogs(txn_logs));
    }

    let published_meta = if txn_idx == 0 && txn_count == 1 {
        // Duplicate single publish branch follows BE behavior: treat missing tablet page
        // in shared bundle as absent metadata instead of hard failure.
        probe_new_version_metadata(&state.root_path, state.tablet_id, state.new_version, true)?
    } else if force_publish {
        None
    } else {
        probe_new_version_metadata(&state.root_path, state.tablet_id, state.new_version, false)?
    };

    let next_base_version = state.current_base_version.saturating_add(1);
    let next_base_meta = if txn_idx == 0 && txn_count > 1 {
        // Batch publish may switch from single mode; probe base+1 metadata.
        load_tablet_metadata_at_version(&state.root_path, state.tablet_id, next_base_version)?
    } else {
        None
    };

    let policy = decide_missing_txn_log_policy(
        txn_count,
        txn_idx,
        force_publish,
        state.current_base_version,
        published_meta.is_some(),
        next_base_meta.is_some(),
    );
    match policy {
        MissingTxnLogPolicy::ReturnPublished => Ok(TxnStepDecision::ReturnPublished(
            published_meta.ok_or_else(|| {
                format!(
                    "published metadata disappeared unexpectedly: tablet_id={} version={}",
                    state.tablet_id, state.new_version
                )
            })?,
        )),
        MissingTxnLogPolicy::SkipTxn => Ok(TxnStepDecision::SkipTxn),
        MissingTxnLogPolicy::AdvanceToNextBaseVersion => {
            state.metadata = next_base_meta.ok_or_else(|| {
                format!(
                    "expected next base metadata missing unexpectedly: tablet_id={} expected_meta_version={}",
                    state.tablet_id, next_base_version
                )
            })?;
            state.current_base_version = next_base_version;
            Ok(TxnStepDecision::SkipTxn)
        }
        MissingTxnLogPolicy::ErrorSinglePublishMissingLog => Err(format!(
            "txn log not found for publish_version: tablet_id={} txn_id={} (single publish)",
            state.tablet_id, txn_id
        )),
        MissingTxnLogPolicy::ErrorBatchMissingLogAndMetadata {
            expected_meta_version,
        } => Err(format!(
            "txn log and corresponding tablet metadata are both missing for batch publish: tablet_id={} txn_id={} expected_meta_version={}",
            state.tablet_id, txn_id, expected_meta_version
        )),
        MissingTxnLogPolicy::ErrorMissingLogAtTxnIndex => Err(format!(
            "txn log not found for publish_version: tablet_id={} txn_id={} txn_index={}",
            state.tablet_id, txn_id, txn_idx
        )),
    }
}

fn finalize_publish_metadata(state: &mut PublishTabletState, txn_infos: &[TxnInfoPb]) {
    state.metadata.id = Some(state.tablet_id);
    state.metadata.version = Some(state.new_version);
    if let Some(commit_time) = txn_infos.last().and_then(|v| v.commit_time) {
        state.metadata.commit_time = Some(commit_time);
    }
    if let Some(gtid) = txn_infos.last().and_then(|v| v.gtid) {
        state.metadata.gtid = Some(gtid);
    }
    if state.metadata.next_rowset_id.is_none() {
        state.metadata.next_rowset_id = Some(next_rowset_id(&state.metadata.rowsets));
    }
}

fn probe_new_version_metadata(
    root_path: &str,
    tablet_id: i64,
    new_version: i64,
    missing_page_as_none: bool,
) -> Result<Option<TabletMetadataPb>, String> {
    load_tablet_metadata_with_missing_page_policy(
        root_path,
        tablet_id,
        new_version,
        missing_page_as_none,
    )
}

fn load_tablet_metadata_with_missing_page_policy(
    root_path: &str,
    tablet_id: i64,
    version: i64,
    missing_page_as_none: bool,
) -> Result<Option<TabletMetadataPb>, String> {
    match load_tablet_metadata_at_version(root_path, tablet_id, version) {
        Ok(v) => Ok(v),
        Err(err) if missing_page_as_none && is_missing_tablet_page_in_bundle_error(&err) => {
            Ok(None)
        }
        Err(err) => Err(err),
    }
}

fn should_cleanup_txn_log_after_publish(txn_infos: &[TxnInfoPb]) -> bool {
    if txn_infos.len() != 1 {
        return false;
    }
    !txn_infos[0].combined_txn_log.unwrap_or(false)
}

fn is_missing_tablet_page_in_bundle_error(error: &str) -> bool {
    error.contains("bundle metadata missing tablet page for tablet_id=")
        || error.contains("bundle metadata does not contain tablet page:")
}

#[cfg(test)]
mod tests {
    use std::io::Cursor;
    use std::sync::Arc;

    use arrow::array::{Int8Array, Int32Array, Int64Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use prost::Message;
    use roaring::RoaringBitmap;
    use tempfile::tempdir;

    use super::{abort_txn, delete_tablet, drop_table, get_tablet_stats, publish_version, vacuum};
    use crate::connector::starrocks::lake::context::{
        TabletWriteContext, clear_persisted_tablet_runtime_for_test,
        clear_tablet_runtime_cache_for_test, register_tablet_runtime,
    };
    use crate::connector::starrocks::lake::{
        append_lake_txn_log_with_rowset, txn_log::write_txn_log_file,
    };
    use crate::formats::starrocks::writer::StarRocksWriteFormat;
    use crate::formats::starrocks::writer::bundle_meta::{
        load_tablet_metadata_at_version, write_bundle_meta_file,
    };
    use crate::formats::starrocks::writer::io::{read_bytes, write_bytes};
    use crate::formats::starrocks::writer::layout::{
        DATA_DIR, bundle_meta_file_path, combined_txn_log_file_path, join_tablet_path,
        txn_log_file_path, txn_log_file_path_with_load_id,
    };
    use crate::service::grpc_client::proto::starrocks::{
        AbortTxnRequest, ColumnPb, CombinedTxnLogPb, DeleteTabletRequest, DelvecMetadataPb,
        DropTableRequest, FileMetaPb, KeysType, PUniqueId, PublishVersionRequest, RowsetMetadataPb,
        TableSchemaKeyPb, TabletInfoPb, TabletSchemaPb, TabletStatRequest, TxnInfoPb, TxnLogPb,
        TxnTypePb, VacuumRequest, tablet_stat_request, txn_log_pb,
    };

    fn test_tablet_schema(schema_id: i64) -> TabletSchemaPb {
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
            id: Some(schema_id),
        }
    }

    fn test_context(
        root: &str,
        table_id: i64,
        tablet_id: i64,
        schema_id: i64,
    ) -> TabletWriteContext {
        TabletWriteContext {
            db_id: 6001,
            table_id,
            tablet_id,
            tablet_root_path: root.to_string(),
            tablet_schema: test_tablet_schema(schema_id),
            s3_config: None,
            partial_update: Default::default(),
        }
    }

    fn test_primary_key_tablet_schema(schema_id: i64) -> TabletSchemaPb {
        TabletSchemaPb {
            keys_type: Some(KeysType::PrimaryKeys as i32),
            column: vec![
                ColumnPb {
                    unique_id: 1,
                    name: Some("k1".to_string()),
                    r#type: "INT".to_string(),
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
                    name: Some("v1".to_string()),
                    r#type: "BIGINT".to_string(),
                    is_key: Some(false),
                    aggregation: None,
                    is_nullable: Some(true),
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
            ],
            num_short_key_columns: Some(1),
            num_rows_per_row_block: None,
            bf_fpp: None,
            next_column_unique_id: Some(3),
            deprecated_is_in_memory: None,
            deprecated_id: None,
            compression_type: None,
            sort_key_idxes: vec![0],
            schema_version: Some(0),
            sort_key_unique_ids: vec![1],
            table_indices: Vec::new(),
            compression_level: None,
            id: Some(schema_id),
        }
    }

    fn test_pk_context(
        root: &str,
        table_id: i64,
        tablet_id: i64,
        schema_id: i64,
    ) -> TabletWriteContext {
        TabletWriteContext {
            db_id: 6001,
            table_id,
            tablet_id,
            tablet_root_path: root.to_string(),
            tablet_schema: test_primary_key_tablet_schema(schema_id),
            s3_config: None,
            partial_update: Default::default(),
        }
    }

    fn test_primary_key_composite_tablet_schema(schema_id: i64) -> TabletSchemaPb {
        TabletSchemaPb {
            keys_type: Some(KeysType::PrimaryKeys as i32),
            column: vec![
                ColumnPb {
                    unique_id: 1,
                    name: Some("k1".to_string()),
                    r#type: "VARCHAR".to_string(),
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
                    name: Some("k2".to_string()),
                    r#type: "INT".to_string(),
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
                    unique_id: 3,
                    name: Some("v1".to_string()),
                    r#type: "BIGINT".to_string(),
                    is_key: Some(false),
                    aggregation: None,
                    is_nullable: Some(true),
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
            ],
            num_short_key_columns: Some(2),
            num_rows_per_row_block: None,
            bf_fpp: None,
            next_column_unique_id: Some(4),
            deprecated_is_in_memory: None,
            deprecated_id: None,
            compression_type: None,
            sort_key_idxes: vec![0, 1],
            schema_version: Some(0),
            sort_key_unique_ids: vec![1, 2],
            table_indices: Vec::new(),
            compression_level: None,
            id: Some(schema_id),
        }
    }

    fn test_pk_composite_context(
        root: &str,
        table_id: i64,
        tablet_id: i64,
        schema_id: i64,
    ) -> TabletWriteContext {
        TabletWriteContext {
            db_id: 6001,
            table_id,
            tablet_id,
            tablet_root_path: root.to_string(),
            tablet_schema: test_primary_key_composite_tablet_schema(schema_id),
            s3_config: None,
            partial_update: Default::default(),
        }
    }

    fn pk_mixed_batch_with_op(keys: Vec<i32>, values: Vec<i64>, ops: Vec<i8>) -> RecordBatch {
        assert_eq!(keys.len(), values.len());
        assert_eq!(keys.len(), ops.len());
        let schema = Arc::new(Schema::new(vec![
            Field::new("k1", DataType::Int32, false),
            Field::new("v1", DataType::Int64, true),
            Field::new("__op", DataType::Int8, false),
        ]));
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int32Array::from(keys)),
                Arc::new(Int64Array::from(values)),
                Arc::new(Int8Array::from(ops)),
            ],
        )
        .expect("build mixed primary key batch")
    }

    fn pk_composite_mixed_batch_with_op(
        k1: Vec<&str>,
        k2: Vec<i32>,
        values: Vec<i64>,
        ops: Vec<i8>,
    ) -> RecordBatch {
        assert_eq!(k1.len(), k2.len());
        assert_eq!(k1.len(), values.len());
        assert_eq!(k1.len(), ops.len());
        let schema = Arc::new(Schema::new(vec![
            Field::new("k1", DataType::Utf8, false),
            Field::new("k2", DataType::Int32, false),
            Field::new("v1", DataType::Int64, true),
            Field::new("__op", DataType::Int8, false),
        ]));
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(k1)),
                Arc::new(Int32Array::from(k2)),
                Arc::new(Int64Array::from(values)),
                Arc::new(Int8Array::from(ops)),
            ],
        )
        .expect("build composite mixed primary key batch")
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

    fn test_write_txn_log(
        tablet_id: i64,
        txn_id: i64,
        segment_name: &str,
        row_count: i64,
    ) -> TxnLogPb {
        TxnLogPb {
            tablet_id: Some(tablet_id),
            txn_id: Some(txn_id),
            op_write: Some(txn_log_pb::OpWrite {
                rowset: Some(test_rowset(
                    segment_name,
                    row_count,
                    row_count.saturating_mul(8),
                )),
                txn_meta: None,
                dels: Vec::new(),
                rewrite_segments: Vec::new(),
                del_encryption_metas: Vec::new(),
                ssts: Vec::new(),
                schema_key: Some(TableSchemaKeyPb {
                    db_id: Some(1),
                    table_id: Some(1),
                    schema_id: Some(1),
                }),
            }),
            op_compaction: None,
            op_schema_change: None,
            op_alter_metadata: None,
            op_replication: None,
            partition_id: Some(1),
            load_id: None,
        }
    }

    fn default_txn_info(txn_id: i64) -> TxnInfoPb {
        TxnInfoPb {
            txn_id: Some(txn_id),
            commit_time: Some(123),
            combined_txn_log: Some(false),
            txn_type: Some(TxnTypePb::TxnNormal as i32),
            force_publish: Some(false),
            rebuild_pindex: Some(false),
            gtid: Some(0),
            load_ids: Vec::new(),
        }
    }

    fn test_abort_request(tablet_id: i64, txn_info: TxnInfoPb) -> AbortTxnRequest {
        AbortTxnRequest {
            tablet_ids: vec![tablet_id],
            txn_ids: Vec::new(),
            skip_cleanup: Some(false),
            txn_types: Vec::new(),
            txn_infos: vec![txn_info],
        }
    }

    #[test]
    fn abort_txn_deletes_per_tablet_log_and_segment_files() {
        let tmp = tempdir().expect("create tempdir");
        let root = tmp.path().to_string_lossy().to_string();
        let tablet_id = 88101;
        let txn_id = 9101;
        let ctx = test_context(&root, 7001, tablet_id, 4001);
        register_tablet_runtime(&ctx).expect("register tablet runtime");

        let seg_name = "seg_abort_single.dat";
        let seg_path =
            join_tablet_path(&root, &format!("{DATA_DIR}/{seg_name}")).expect("build segment path");
        write_bytes(&seg_path, vec![1, 2, 3]).expect("write segment file");
        let log_path = txn_log_file_path(&root, tablet_id, txn_id).expect("build txn log path");
        write_txn_log_file(
            &log_path,
            &test_write_txn_log(tablet_id, txn_id, seg_name, 3),
        )
        .expect("write txn log");

        let req = test_abort_request(tablet_id, default_txn_info(txn_id));
        let resp = abort_txn(&req).expect("abort txn");
        assert!(
            resp.failed_tablets.is_empty(),
            "unexpected failed tablets: {:?}",
            resp.failed_tablets
        );
        assert!(
            !std::path::Path::new(&log_path).exists(),
            "txn log should be deleted after abort: {}",
            log_path
        );
        assert!(
            !std::path::Path::new(&seg_path).exists(),
            "segment should be deleted after abort: {}",
            seg_path
        );
    }

    #[test]
    fn abort_txn_deletes_combined_log_after_success() {
        let tmp = tempdir().expect("create tempdir");
        let root = tmp.path().to_string_lossy().to_string();
        let tablet_id = 88102;
        let txn_id = 9102;
        let ctx = test_context(&root, 7002, tablet_id, 4002);
        register_tablet_runtime(&ctx).expect("register tablet runtime");

        let seg_name = "seg_abort_combined.dat";
        let seg_path =
            join_tablet_path(&root, &format!("{DATA_DIR}/{seg_name}")).expect("build segment path");
        write_bytes(&seg_path, vec![7, 8, 9]).expect("write segment file");

        let combined = CombinedTxnLogPb {
            txn_logs: vec![test_write_txn_log(tablet_id, txn_id, seg_name, 5)],
        };
        let combined_path = combined_txn_log_file_path(&root, txn_id).expect("build combined path");
        write_bytes(&combined_path, combined.encode_to_vec()).expect("write combined txn log");

        let mut txn_info = default_txn_info(txn_id);
        txn_info.combined_txn_log = Some(true);
        let req = test_abort_request(tablet_id, txn_info);
        let resp = abort_txn(&req).expect("abort txn");
        assert!(
            resp.failed_tablets.is_empty(),
            "unexpected failed tablets: {:?}",
            resp.failed_tablets
        );
        assert!(
            !std::path::Path::new(&combined_path).exists(),
            "combined txn log should be deleted after abort: {}",
            combined_path
        );
        assert!(
            !std::path::Path::new(&seg_path).exists(),
            "segment should be deleted after abort: {}",
            seg_path
        );
    }

    #[test]
    fn publish_version_supports_multi_stmt_logs_with_load_ids() {
        let tmp = tempdir().expect("create tempdir");
        let root = tmp.path().to_string_lossy().to_string();
        let tablet_id = 88010;
        let txn_id = 1001;
        let ctx = test_context(&root, 7001, tablet_id, 4001);
        register_tablet_runtime(&ctx).expect("register tablet runtime");

        let load_id_1 = PUniqueId { hi: 11, lo: 22 };
        let load_id_2 = PUniqueId { hi: 33, lo: 44 };
        let path_1 =
            txn_log_file_path_with_load_id(&root, tablet_id, txn_id, &load_id_1).expect("path1");
        let path_2 =
            txn_log_file_path_with_load_id(&root, tablet_id, txn_id, &load_id_2).expect("path2");
        write_txn_log_file(
            &path_1,
            &test_write_txn_log(tablet_id, txn_id, "seg_a.dat", 2),
        )
        .expect("write path1");
        write_txn_log_file(
            &path_2,
            &test_write_txn_log(tablet_id, txn_id, "seg_b.dat", 3),
        )
        .expect("write path2");

        let mut txn_info = default_txn_info(txn_id);
        txn_info.load_ids = vec![load_id_1, load_id_2];
        let req = PublishVersionRequest {
            tablet_ids: vec![tablet_id],
            txn_ids: Vec::new(),
            base_version: Some(1),
            new_version: Some(2),
            commit_time: Some(123),
            timeout_ms: None,
            txn_infos: vec![txn_info],
            rebuild_pindex_tablet_ids: Vec::new(),
            enable_aggregate_publish: None,
            resharding_tablet_infos: Vec::new(),
        };

        let resp = publish_version(&req).expect("publish version");
        assert!(
            resp.failed_tablets.is_empty(),
            "unexpected failed tablets: {:?}",
            resp.failed_tablets
        );
        let meta = load_tablet_metadata_at_version(&root, tablet_id, 2)
            .expect("load v2 meta")
            .expect("v2 meta exists");
        assert_eq!(meta.rowsets.len(), 2);
        assert!(std::path::Path::new(&path_1).exists());
        assert!(std::path::Path::new(&path_2).exists());
    }

    #[test]
    fn publish_version_reads_combined_txn_log_when_requested() {
        let tmp = tempdir().expect("create tempdir");
        let root = tmp.path().to_string_lossy().to_string();
        let tablet_id = 88003;
        let txn_id = 2001;
        let ctx = test_context(&root, 7003, tablet_id, 4003);
        register_tablet_runtime(&ctx).expect("register tablet runtime");

        let combined = CombinedTxnLogPb {
            txn_logs: vec![test_write_txn_log(tablet_id, txn_id, "seg_combined.dat", 5)],
        };
        let path = combined_txn_log_file_path(&root, txn_id).expect("build combined path");
        write_bytes(&path, combined.encode_to_vec()).expect("write combined txn log");

        let mut txn_info = default_txn_info(txn_id);
        txn_info.combined_txn_log = Some(true);
        let req = PublishVersionRequest {
            tablet_ids: vec![tablet_id],
            txn_ids: Vec::new(),
            base_version: Some(1),
            new_version: Some(2),
            commit_time: Some(789),
            timeout_ms: None,
            txn_infos: vec![txn_info],
            rebuild_pindex_tablet_ids: Vec::new(),
            enable_aggregate_publish: None,
            resharding_tablet_infos: Vec::new(),
        };

        let resp = publish_version(&req).expect("publish version should succeed");
        assert!(
            resp.failed_tablets.is_empty(),
            "unexpected failed tablets: {:?}",
            resp.failed_tablets
        );
    }

    #[test]
    fn publish_version_recovers_runtime_from_persisted_cache_after_restart() {
        let tmp = tempdir().expect("create tempdir");
        let root = tmp.path().to_string_lossy().to_string();
        let tablet_id = 88005;
        let txn_id = 3001;
        clear_persisted_tablet_runtime_for_test(tablet_id).expect("clear persisted runtime");
        clear_tablet_runtime_cache_for_test();

        let ctx = test_context(&root, 7005, tablet_id, 4005);
        register_tablet_runtime(&ctx).expect("register tablet runtime");
        let log = test_write_txn_log(tablet_id, txn_id, "seg_restart.dat", 7);
        let path = txn_log_file_path(&root, tablet_id, txn_id).expect("build txn log path");
        write_txn_log_file(&path, &log).expect("write txn log");

        clear_tablet_runtime_cache_for_test();

        let req = PublishVersionRequest {
            tablet_ids: vec![tablet_id],
            txn_ids: Vec::new(),
            base_version: Some(1),
            new_version: Some(2),
            commit_time: Some(111),
            timeout_ms: None,
            txn_infos: vec![default_txn_info(txn_id)],
            rebuild_pindex_tablet_ids: Vec::new(),
            enable_aggregate_publish: None,
            resharding_tablet_infos: Vec::new(),
        };
        let resp = publish_version(&req).expect("publish version should succeed");
        assert!(
            resp.failed_tablets.is_empty(),
            "unexpected failed tablets: {:?}",
            resp.failed_tablets
        );
        assert!(
            !std::path::Path::new(&path).exists(),
            "txn log should be cleaned after successful publish: {}",
            path
        );
    }

    #[test]
    fn publish_version_keeps_combined_log_after_success() {
        let tmp = tempdir().expect("create tempdir");
        let root = tmp.path().to_string_lossy().to_string();
        let tablet_id = 88006;
        let txn_id = 3002;
        clear_persisted_tablet_runtime_for_test(tablet_id).expect("clear persisted runtime");
        clear_tablet_runtime_cache_for_test();

        let ctx = test_context(&root, 7006, tablet_id, 4006);
        register_tablet_runtime(&ctx).expect("register tablet runtime");

        let combined = CombinedTxnLogPb {
            txn_logs: vec![test_write_txn_log(
                tablet_id,
                txn_id,
                "seg_combined_a.dat",
                3,
            )],
        };
        let path = combined_txn_log_file_path(&root, txn_id).expect("build combined txn log path");
        write_bytes(&path, combined.encode_to_vec()).expect("write combined txn log");

        let mut txn_info = default_txn_info(txn_id);
        txn_info.combined_txn_log = Some(true);
        let req = PublishVersionRequest {
            tablet_ids: vec![tablet_id],
            txn_ids: Vec::new(),
            base_version: Some(1),
            new_version: Some(2),
            commit_time: Some(222),
            timeout_ms: None,
            txn_infos: vec![txn_info],
            rebuild_pindex_tablet_ids: Vec::new(),
            enable_aggregate_publish: None,
            resharding_tablet_infos: Vec::new(),
        };
        let resp = publish_version(&req).expect("publish version should succeed");
        assert!(
            resp.failed_tablets.is_empty(),
            "unexpected failed tablets: {:?}",
            resp.failed_tablets
        );
        assert!(
            std::path::Path::new(&path).exists(),
            "combined txn log should be kept for conservative replay safety: {}",
            path
        );
    }

    #[test]
    fn publish_version_keeps_batch_txn_logs_for_retry_safety() {
        let tmp = tempdir().expect("create tempdir");
        let root = tmp.path().to_string_lossy().to_string();
        let tablet_id = 88061;
        let txn1_id = 5101;
        let txn2_id = 5102;
        let ctx = test_context(&root, 7061, tablet_id, 4061);
        register_tablet_runtime(&ctx).expect("register tablet runtime");

        let txn1_path = txn_log_file_path(&root, tablet_id, txn1_id).expect("build txn1 path");
        let txn2_path = txn_log_file_path(&root, tablet_id, txn2_id).expect("build txn2 path");
        write_txn_log_file(
            &txn1_path,
            &test_write_txn_log(tablet_id, txn1_id, "seg_batch_txn1.dat", 2),
        )
        .expect("write txn1 log");
        write_txn_log_file(
            &txn2_path,
            &test_write_txn_log(tablet_id, txn2_id, "seg_batch_txn2.dat", 3),
        )
        .expect("write txn2 log");

        let req = PublishVersionRequest {
            tablet_ids: vec![tablet_id],
            txn_ids: Vec::new(),
            base_version: Some(1),
            new_version: Some(3),
            commit_time: Some(333),
            timeout_ms: None,
            txn_infos: vec![default_txn_info(txn1_id), default_txn_info(txn2_id)],
            rebuild_pindex_tablet_ids: Vec::new(),
            enable_aggregate_publish: None,
            resharding_tablet_infos: Vec::new(),
        };
        let resp = publish_version(&req).expect("publish version should succeed");
        assert!(
            resp.failed_tablets.is_empty(),
            "unexpected failed tablets: {:?}",
            resp.failed_tablets
        );
        assert!(
            std::path::Path::new(&txn1_path).exists(),
            "batch publish should keep txn1 log for replay safety: {}",
            txn1_path
        );
        assert!(
            std::path::Path::new(&txn2_path).exists(),
            "batch publish should keep txn2 log for replay safety: {}",
            txn2_path
        );
    }

    #[test]
    fn publish_version_handles_first_missing_txn_log_in_batch_retry() {
        let tmp = tempdir().expect("create tempdir");
        let root = tmp.path().to_string_lossy().to_string();
        let tablet_id = 88002;
        let ctx = test_context(&root, 7002, tablet_id, 4002);
        register_tablet_runtime(&ctx).expect("register tablet runtime");

        let mut version_2_meta =
            crate::formats::starrocks::writer::bundle_meta::empty_tablet_metadata(tablet_id);
        version_2_meta.version = Some(2);
        version_2_meta.rowsets = vec![test_rowset("seg_prev.dat", 1, 8)];
        version_2_meta.next_rowset_id = Some(2);
        write_bundle_meta_file(&root, tablet_id, 2, &ctx.tablet_schema, &version_2_meta)
            .expect("write version-2 metadata");

        let txn2_id = 1002;
        let txn2_log = test_write_txn_log(tablet_id, txn2_id, "seg_retry_txn2.dat", 3);
        let txn2_path = txn_log_file_path(&root, tablet_id, txn2_id).expect("build txn2 path");
        write_txn_log_file(&txn2_path, &txn2_log).expect("write txn2 log");

        let req = PublishVersionRequest {
            tablet_ids: vec![tablet_id],
            txn_ids: Vec::new(),
            base_version: Some(1),
            new_version: Some(3),
            commit_time: Some(456),
            timeout_ms: None,
            txn_infos: vec![default_txn_info(1001), default_txn_info(txn2_id)],
            rebuild_pindex_tablet_ids: Vec::new(),
            enable_aggregate_publish: None,
            resharding_tablet_infos: Vec::new(),
        };

        let resp = publish_version(&req).expect("publish version should succeed");
        assert!(
            resp.failed_tablets.is_empty(),
            "unexpected failed tablets: {:?}",
            resp.failed_tablets
        );
    }

    #[test]
    fn publish_version_applies_primary_key_mixed_op_and_persists_delvec_visibility() {
        let tmp = tempdir().expect("create tempdir");
        let root = tmp.path().to_string_lossy().to_string();
        let tablet_id = 88071;
        let txn_id = 6209;
        let ctx = test_pk_context(&root, 7071, tablet_id, 4071);
        register_tablet_runtime(&ctx).expect("register primary key tablet runtime");

        // Same-key UPSERT then DELETE in one batch should keep rowset data file,
        // but publish must mark that row deleted via delvec.
        let batch = pk_mixed_batch_with_op(vec![1, 1], vec![11, 12], vec![0, 1]);
        append_lake_txn_log_with_rowset(
            &ctx,
            &batch,
            txn_id,
            0,
            0,
            StarRocksWriteFormat::Native,
            1,
            None,
        )
        .expect("append primary key mixed op txn log");

        let req = PublishVersionRequest {
            tablet_ids: vec![tablet_id],
            txn_ids: Vec::new(),
            base_version: Some(1),
            new_version: Some(2),
            commit_time: Some(456),
            timeout_ms: None,
            txn_infos: vec![default_txn_info(txn_id)],
            rebuild_pindex_tablet_ids: Vec::new(),
            enable_aggregate_publish: None,
            resharding_tablet_infos: Vec::new(),
        };
        let resp = publish_version(&req).expect("publish version should succeed");
        assert!(
            resp.failed_tablets.is_empty(),
            "unexpected failed tablets: {:?}",
            resp.failed_tablets
        );

        let meta = load_tablet_metadata_at_version(&root, tablet_id, 2)
            .expect("load published metadata")
            .expect("published metadata exists");
        assert_eq!(meta.rowsets.len(), 1);
        let rowset = &meta.rowsets[0];
        assert_eq!(rowset.num_rows, Some(1));
        assert_eq!(rowset.num_dels, Some(1));

        let rowset_id = rowset.id.expect("rowset id should be assigned");
        let segment_id = rowset_id;
        let delvec_meta = meta
            .delvec_meta
            .as_ref()
            .expect("delvec metadata should exist");
        let page = delvec_meta
            .delvecs
            .get(&segment_id)
            .expect("segment delvec should exist");
        assert_eq!(page.version, Some(2));

        let file_meta = delvec_meta
            .version_to_file
            .get(&2)
            .expect("version delvec file should exist");
        let file_name = file_meta
            .name
            .as_deref()
            .expect("delvec file name should exist");
        let file_path =
            join_tablet_path(&root, &format!("{DATA_DIR}/{file_name}")).expect("build delvec path");
        let file_bytes = read_bytes(&file_path).expect("read delvec file");
        let offset = usize::try_from(page.offset.unwrap_or(0)).expect("offset usize");
        let size = usize::try_from(page.size.unwrap_or(0)).expect("size usize");
        let end = offset.saturating_add(size);
        assert!(end <= file_bytes.len(), "delvec page range should be valid");
        let payload = &file_bytes[offset..end];
        assert!(!payload.is_empty(), "delvec payload should not be empty");
        assert_eq!(payload[0], 0x01, "unexpected delvec format version");

        let mut cursor = Cursor::new(&payload[1..]);
        let bitmap = RoaringBitmap::deserialize_from(&mut cursor).expect("decode delvec bitmap");
        assert!(bitmap.contains(0), "row 0 should be marked deleted");
        assert_eq!(bitmap.len(), 1);
    }

    #[test]
    fn publish_version_applies_primary_key_composite_mixed_op_and_persists_delvec_visibility() {
        let tmp = tempdir().expect("create tempdir");
        let root = tmp.path().to_string_lossy().to_string();
        let tablet_id = 88072;
        let txn_id = 6210;
        let ctx = test_pk_composite_context(&root, 7072, tablet_id, 4072);
        register_tablet_runtime(&ctx).expect("register primary key composite tablet runtime");

        let batch = pk_composite_mixed_batch_with_op(
            vec!["a\0b", "a\0b"],
            vec![7, 7],
            vec![11, 12],
            vec![0, 1],
        );
        append_lake_txn_log_with_rowset(
            &ctx,
            &batch,
            txn_id,
            0,
            0,
            StarRocksWriteFormat::Native,
            1,
            None,
        )
        .expect("append composite primary key mixed op txn log");

        let req = PublishVersionRequest {
            tablet_ids: vec![tablet_id],
            txn_ids: Vec::new(),
            base_version: Some(1),
            new_version: Some(2),
            commit_time: Some(457),
            timeout_ms: None,
            txn_infos: vec![default_txn_info(txn_id)],
            rebuild_pindex_tablet_ids: Vec::new(),
            enable_aggregate_publish: None,
            resharding_tablet_infos: Vec::new(),
        };
        let resp = publish_version(&req).expect("publish version should succeed");
        assert!(
            resp.failed_tablets.is_empty(),
            "unexpected failed tablets: {:?}",
            resp.failed_tablets
        );

        let meta = load_tablet_metadata_at_version(&root, tablet_id, 2)
            .expect("load published metadata")
            .expect("published metadata exists");
        assert_eq!(meta.rowsets.len(), 1);
        let rowset = &meta.rowsets[0];
        assert_eq!(rowset.num_rows, Some(1));
        assert_eq!(rowset.num_dels, Some(1));

        let rowset_id = rowset.id.expect("rowset id should be assigned");
        let segment_id = rowset_id;
        let delvec_meta = meta
            .delvec_meta
            .as_ref()
            .expect("delvec metadata should exist");
        let page = delvec_meta
            .delvecs
            .get(&segment_id)
            .expect("segment delvec should exist");
        assert_eq!(page.version, Some(2));

        let file_meta = delvec_meta
            .version_to_file
            .get(&2)
            .expect("version delvec file should exist");
        let file_name = file_meta
            .name
            .as_deref()
            .expect("delvec file name should exist");
        let file_path =
            join_tablet_path(&root, &format!("{DATA_DIR}/{file_name}")).expect("build delvec path");
        let file_bytes = read_bytes(&file_path).expect("read delvec file");
        let offset = usize::try_from(page.offset.unwrap_or(0)).expect("offset usize");
        let size = usize::try_from(page.size.unwrap_or(0)).expect("size usize");
        let end = offset.saturating_add(size);
        assert!(end <= file_bytes.len(), "delvec page range should be valid");
        let payload = &file_bytes[offset..end];
        assert!(!payload.is_empty(), "delvec payload should not be empty");
        assert_eq!(payload[0], 0x01, "unexpected delvec format version");

        let mut cursor = Cursor::new(&payload[1..]);
        let bitmap = RoaringBitmap::deserialize_from(&mut cursor).expect("decode delvec bitmap");
        assert!(bitmap.contains(0), "row 0 should be marked deleted");
        assert_eq!(bitmap.len(), 1);
    }

    #[test]
    fn get_tablet_stats_returns_visible_rows_and_data_size() {
        let tmp = tempdir().expect("create tempdir");
        let root = tmp.path().to_string_lossy().to_string();
        let tablet_id = 88091;
        let ctx = test_context(&root, 7091, tablet_id, 4091);
        register_tablet_runtime(&ctx).expect("register tablet runtime");

        let mut metadata =
            crate::formats::starrocks::writer::bundle_meta::empty_tablet_metadata(tablet_id);
        metadata.version = Some(3);
        let mut rowset = test_rowset("seg_stats.dat", 10, 80);
        rowset.num_dels = Some(4);
        metadata.rowsets = vec![rowset];
        metadata.delvec_meta = Some(DelvecMetadataPb {
            version_to_file: [(
                3,
                FileMetaPb {
                    name: Some("stats_3.delvec".to_string()),
                    size: Some(12),
                    shared: Some(false),
                    encryption_meta: None,
                },
            )]
            .into_iter()
            .collect(),
            delvecs: std::collections::HashMap::new(),
        });
        write_bundle_meta_file(&root, tablet_id, 3, &ctx.tablet_schema, &metadata)
            .expect("write metadata");

        let request = TabletStatRequest {
            tablet_infos: vec![tablet_stat_request::TabletInfo {
                tablet_id: Some(tablet_id),
                version: Some(3),
            }],
            timeout_ms: None,
        };
        let response = get_tablet_stats(&request).expect("get tablet stats should succeed");
        assert_eq!(response.tablet_stats.len(), 1);
        let stat = &response.tablet_stats[0];
        assert_eq!(stat.tablet_id, Some(tablet_id));
        assert_eq!(stat.num_rows, Some(6));
        assert_eq!(stat.data_size, Some(92));
    }

    #[test]
    fn get_tablet_stats_rejects_empty_tablet_infos() {
        let request = TabletStatRequest {
            tablet_infos: Vec::new(),
            timeout_ms: None,
        };
        let err = get_tablet_stats(&request).expect_err("empty request should fail");
        assert!(
            err.contains("missing tablet_infos"),
            "unexpected error: {}",
            err
        );
    }

    #[test]
    fn drop_table_removes_local_directory_recursively() {
        let tmp = tempdir().expect("create tempdir");
        let root = tmp.path().join("drop_table_case");
        let nested_dir = root.join("nested");
        std::fs::create_dir_all(&nested_dir).expect("create nested dir");
        std::fs::write(nested_dir.join("segment.dat"), b"abc").expect("write nested file");

        let req = DropTableRequest {
            tablet_id: Some(99001),
            path: Some(root.to_string_lossy().to_string()),
        };
        let resp = drop_table(&req).expect("drop table should succeed");
        assert_eq!(resp.status.as_ref().map(|v| v.status_code), Some(0));
        assert!(
            !root.exists(),
            "drop_table should remove directory recursively"
        );

        // Idempotency: dropping the same path again should still succeed.
        let resp2 = drop_table(&req).expect("drop table should be idempotent");
        assert_eq!(resp2.status.as_ref().map(|v| v.status_code), Some(0));
    }

    #[test]
    fn delete_tablet_drops_local_partition_root_when_all_tablets_are_deleted() {
        let tmp = tempdir().expect("create tempdir");
        let root = tmp.path().join("delete_tablet_case");
        std::fs::create_dir_all(&root).expect("create root dir");
        let root_str = root.to_string_lossy().to_string();
        let tablet_id = 99201;
        let txn_id = 6123;
        let ctx = test_context(&root_str, 7301, tablet_id, 4301);
        register_tablet_runtime(&ctx).expect("register tablet runtime");

        let mut metadata =
            crate::formats::starrocks::writer::bundle_meta::empty_tablet_metadata(tablet_id);
        metadata.version = Some(1);
        metadata.rowsets = vec![test_rowset("seg_delete_tablet.dat", 2, 16)];
        write_bundle_meta_file(&root_str, tablet_id, 1, &ctx.tablet_schema, &metadata)
            .expect("write metadata");

        let seg_path = join_tablet_path(&root_str, &format!("{DATA_DIR}/seg_delete_tablet.dat"))
            .expect("build segment path");
        write_bytes(&seg_path, vec![1, 2, 3, 4]).expect("write segment");
        let log_path = txn_log_file_path(&root_str, tablet_id, txn_id).expect("build txn log path");
        write_txn_log_file(
            &log_path,
            &test_write_txn_log(tablet_id, txn_id, "seg_delete_tablet.dat", 2),
        )
        .expect("write txn log");

        let resp = delete_tablet(&DeleteTabletRequest {
            tablet_ids: vec![tablet_id],
        })
        .expect("delete tablet should succeed");
        assert!(
            resp.failed_tablets.is_empty(),
            "unexpected failed tablets: {:?}",
            resp.failed_tablets
        );
        assert_eq!(resp.status.as_ref().map(|v| v.status_code), Some(0));
        assert!(
            !root.exists(),
            "delete_tablet should remove root path when all bundle tablets are deleted"
        );
    }

    #[test]
    fn delete_tablet_is_idempotent_when_runtime_is_already_removed() {
        let tmp = tempdir().expect("create tempdir");
        let root = tmp.path().join("delete_tablet_idempotent");
        std::fs::create_dir_all(&root).expect("create root dir");
        let root_str = root.to_string_lossy().to_string();
        let tablet_id = 99202;
        let ctx = test_context(&root_str, 7303, tablet_id, 4303);
        register_tablet_runtime(&ctx).expect("register tablet runtime");

        let mut metadata =
            crate::formats::starrocks::writer::bundle_meta::empty_tablet_metadata(tablet_id);
        metadata.version = Some(1);
        write_bundle_meta_file(&root_str, tablet_id, 1, &ctx.tablet_schema, &metadata)
            .expect("write metadata");

        let first = delete_tablet(&DeleteTabletRequest {
            tablet_ids: vec![tablet_id],
        })
        .expect("first delete should succeed");
        assert_eq!(first.status.as_ref().map(|v| v.status_code), Some(0));
        assert!(first.failed_tablets.is_empty());
        assert!(
            !root.exists(),
            "first delete should remove tablet root path: {}",
            root_str
        );

        let second = delete_tablet(&DeleteTabletRequest {
            tablet_ids: vec![tablet_id],
        })
        .expect("second delete should still succeed");
        assert_eq!(second.status.as_ref().map(|v| v.status_code), Some(0));
        assert!(
            second.failed_tablets.is_empty(),
            "idempotent delete should not report failed tablets: {:?}",
            second.failed_tablets
        );
    }

    #[test]
    fn delete_tablet_allows_missing_runtime_entries() {
        let tmp = tempdir().expect("create tempdir");
        let root = tmp.path().join("delete_tablet_missing_runtime");
        std::fs::create_dir_all(&root).expect("create root dir");
        let root_str = root.to_string_lossy().to_string();
        let tablet_id = 99203;
        let missing_tablet_id = 1999203;
        let ctx = test_context(&root_str, 7304, tablet_id, 4304);
        register_tablet_runtime(&ctx).expect("register tablet runtime");

        let mut metadata =
            crate::formats::starrocks::writer::bundle_meta::empty_tablet_metadata(tablet_id);
        metadata.version = Some(1);
        write_bundle_meta_file(&root_str, tablet_id, 1, &ctx.tablet_schema, &metadata)
            .expect("write metadata");

        let response = delete_tablet(&DeleteTabletRequest {
            tablet_ids: vec![tablet_id, missing_tablet_id],
        })
        .expect("delete with missing runtime should succeed");
        assert_eq!(response.status.as_ref().map(|v| v.status_code), Some(0));
        assert!(
            response.failed_tablets.is_empty(),
            "delete should stay success when some runtimes are missing: {:?}",
            response.failed_tablets
        );
        assert!(
            !root.exists(),
            "delete should still cleanup resolved tablet root path: {}",
            root_str
        );
    }

    #[test]
    fn vacuum_deletes_old_metadata_and_old_txn_logs() {
        let tmp = tempdir().expect("create tempdir");
        let root = tmp.path().join("vacuum_case");
        std::fs::create_dir_all(&root).expect("create root dir");
        let root_str = root.to_string_lossy().to_string();
        let tablet_id = 99301;
        let ctx = test_context(&root_str, 7302, tablet_id, 4302);
        register_tablet_runtime(&ctx).expect("register tablet runtime");

        let versions = vec![(1_i64, 10_i64), (2, 20), (3, 30), (4, 40)];
        for (version, commit_time) in versions {
            let mut metadata =
                crate::formats::starrocks::writer::bundle_meta::empty_tablet_metadata(tablet_id);
            metadata.version = Some(version);
            metadata.commit_time = Some(commit_time);
            metadata.rowsets = vec![test_rowset(&format!("seg_v{}.dat", version), 1, 8)];
            write_bundle_meta_file(&root_str, tablet_id, version, &ctx.tablet_schema, &metadata)
                .expect("write metadata version");
        }

        for txn_id in [1_i64, 2, 3, 4, 5] {
            let path = txn_log_file_path(&root_str, tablet_id, txn_id).expect("build txn log path");
            write_bytes(&path, vec![0xAB, 0xCD]).expect("write txn log file");
        }

        let req = VacuumRequest {
            tablet_ids: Vec::new(),
            min_retain_version: Some(4),
            grace_timestamp: Some(25),
            min_active_txn_id: Some(4),
            delete_txn_log: Some(true),
            partition_id: Some(990001),
            tablet_infos: vec![TabletInfoPb {
                tablet_id: Some(tablet_id),
                min_version: Some(4),
            }],
            enable_file_bundling: Some(true),
            retain_versions: Vec::new(),
        };
        let resp = vacuum(&req).expect("vacuum should succeed");
        assert_eq!(resp.status.as_ref().map(|v| v.status_code), Some(0));
        assert_eq!(
            resp.vacuumed_version,
            Some(2),
            "vacuum should keep the latest metadata version before grace timestamp"
        );

        let meta_v1_path = bundle_meta_file_path(&root_str, 1).expect("build meta v1 path");
        let meta_v2_path = bundle_meta_file_path(&root_str, 2).expect("build meta v2 path");
        let meta_v3_path = bundle_meta_file_path(&root_str, 3).expect("build meta v3 path");
        let meta_v4_path = bundle_meta_file_path(&root_str, 4).expect("build meta v4 path");
        assert!(
            !std::path::Path::new(&meta_v1_path).exists(),
            "metadata before last pre-grace version should be vacuumed"
        );
        assert!(std::path::Path::new(&meta_v2_path).exists());
        assert!(std::path::Path::new(&meta_v3_path).exists());
        assert!(std::path::Path::new(&meta_v4_path).exists());

        for txn_id in [1_i64, 2, 3] {
            let path = txn_log_file_path(&root_str, tablet_id, txn_id).expect("build old txn log");
            assert!(
                !std::path::Path::new(&path).exists(),
                "old txn log should be vacuumed: {}",
                path
            );
        }
        for txn_id in [4_i64, 5] {
            let path = txn_log_file_path(&root_str, tablet_id, txn_id).expect("build kept txn log");
            assert!(
                std::path::Path::new(&path).exists(),
                "active/new txn log should be retained: {}",
                path
            );
        }
    }
}
