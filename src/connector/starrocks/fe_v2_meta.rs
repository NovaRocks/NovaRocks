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
use std::net::{SocketAddr, TcpStream};
use std::sync::{Mutex, OnceLock};
use std::time::Duration;

use thrift::protocol::{TBinaryInputProtocol, TBinaryOutputProtocol};
use thrift::transport::{TBufferedReadTransport, TBufferedWriteTransport, TIoChannel, TTcpChannel};

use crate::connector::starrocks::lake::context::get_tablet_runtime;
use crate::frontend_service::{self, FrontendServiceSyncClient, TFrontendServiceSyncClient};
use crate::runtime::query_context::{QueryId, query_context_manager};
use crate::runtime::starlet_shard_registry;
use crate::service::disk_report;
use crate::status_code;
use crate::types;

#[derive(Clone, Debug)]
pub(crate) struct LakeTableIdentity {
    pub(crate) catalog: String,
    pub(crate) db_name: String,
    pub(crate) table_name: String,
    pub(crate) db_id: i64,
    pub(crate) table_id: i64,
    pub(crate) schema_id: i64,
}

impl LakeTableIdentity {
    pub(crate) fn cache_key(&self) -> String {
        format!(
            "{}:{}:{}:{}",
            self.catalog, self.db_id, self.table_id, self.schema_id
        )
    }
}

#[derive(Copy, Clone, Debug)]
pub(crate) struct LakeScanTabletRef {
    pub(crate) tablet_id: i64,
    pub(crate) partition_id: i64,
    pub(crate) version: i64,
}

#[derive(Copy, Clone, Debug)]
pub(crate) struct LakeTabletPartitionRef {
    pub(crate) tablet_id: i64,
}

static TABLE_IDENTITY_NAME_CACHE: OnceLock<Mutex<HashMap<String, (String, String)>>> =
    OnceLock::new();

const FE_META_FETCH_TIMEOUT_SECS: u64 = 5;
const FE_PARTITIONS_META_MAX_PAGES: usize = 1024;

fn table_identity_name_cache() -> &'static Mutex<HashMap<String, (String, String)>> {
    TABLE_IDENTITY_NAME_CACHE.get_or_init(|| Mutex::new(HashMap::new()))
}

fn table_identity_name_key(catalog: &str, db_id: i64, table_id: i64) -> String {
    format!("{catalog}:{db_id}:{table_id}")
}

fn is_unknown_identity_name(v: &str) -> bool {
    let trimmed = v.trim();
    trimmed.is_empty()
        || trimmed.eq_ignore_ascii_case("__unknown_db__")
        || trimmed.eq_ignore_ascii_case("__unknown_table__")
}

pub(crate) fn cache_table_identity_names(table: &LakeTableIdentity) {
    if is_unknown_identity_name(&table.db_name) || is_unknown_identity_name(&table.table_name) {
        return;
    }
    if table.db_id <= 0 || table.table_id <= 0 {
        return;
    }
    if let Ok(mut guard) = table_identity_name_cache().lock() {
        guard.insert(
            table_identity_name_key(&table.catalog, table.db_id, table.table_id),
            (table.db_name.clone(), table.table_name.clone()),
        );
    }
}

pub(crate) fn find_cached_table_identity_names(
    catalog: &str,
    db_id: i64,
    table_id: i64,
) -> Option<(String, String)> {
    if catalog.trim().is_empty() || db_id <= 0 || table_id <= 0 {
        return None;
    }
    let guard = table_identity_name_cache().lock().ok()?;
    guard
        .get(&table_identity_name_key(catalog, db_id, table_id))
        .cloned()
}

#[cfg(test)]
fn clear_table_identity_name_cache_for_test() {
    if let Ok(mut guard) = table_identity_name_cache().lock() {
        guard.clear();
    }
}

pub(crate) fn resolve_tablet_paths_for_lake_scan(
    query_id: Option<QueryId>,
    fe_addr: Option<&types::TNetworkAddress>,
    table: &LakeTableIdentity,
    ranges: &[LakeScanTabletRef],
) -> Result<HashMap<i64, String>, String> {
    if ranges.is_empty() {
        return Ok(HashMap::new());
    }

    if ranges.iter().any(|r| r.version <= 0) {
        return Err("lake scan contains non-positive version".to_string());
    }
    if ranges.iter().any(|r| r.partition_id <= 0) {
        return Err("lake scan contains non-positive partition_id".to_string());
    }

    let refs = ranges
        .iter()
        .map(|r| LakeTabletPartitionRef {
            tablet_id: r.tablet_id,
        })
        .collect::<Vec<_>>();
    resolve_tablet_paths_for_refs(query_id, fe_addr, table, &refs)
}

pub(crate) fn resolve_tablet_paths_for_lake_meta_scan(
    query_id: Option<QueryId>,
    fe_addr: Option<&types::TNetworkAddress>,
    table: &LakeTableIdentity,
    tablet_ids: &[i64],
) -> Result<HashMap<i64, String>, String> {
    if tablet_ids.is_empty() {
        return Ok(HashMap::new());
    }
    if tablet_ids.iter().any(|tablet_id| *tablet_id <= 0) {
        return Err("lake meta scan contains non-positive tablet_id".to_string());
    }
    let refs = tablet_ids
        .iter()
        .map(|tablet_id| LakeTabletPartitionRef {
            tablet_id: *tablet_id,
        })
        .collect::<Vec<_>>();
    resolve_tablet_paths_for_refs(query_id, fe_addr, table, &refs)
}

pub(crate) fn resolve_tablet_paths_for_olap_sink(
    query_id: Option<QueryId>,
    fe_addr: Option<&types::TNetworkAddress>,
    table: &LakeTableIdentity,
    refs: &[LakeTabletPartitionRef],
) -> Result<HashMap<i64, String>, String> {
    resolve_tablet_paths_for_refs(query_id, fe_addr, table, refs)
}

fn resolve_tablet_paths_for_refs(
    query_id: Option<QueryId>,
    fe_addr: Option<&types::TNetworkAddress>,
    table: &LakeTableIdentity,
    refs: &[LakeTabletPartitionRef],
) -> Result<HashMap<i64, String>, String> {
    if refs.is_empty() {
        return Ok(HashMap::new());
    }

    let cache_key = table.cache_key();
    if let Some(qid) = query_id {
        if let Some(paths) = query_context_manager().lake_tablet_paths(qid, &cache_key) {
            if paths_cover_refs(&paths, refs) {
                cache_table_identity_names(table);
                return Ok(select_paths_for_refs(paths, refs)?);
            }
        }
    }

    let requested_tablet_ids = refs.iter().map(|r| r.tablet_id).collect::<Vec<_>>();
    let mut local_paths = starlet_shard_registry::select_paths(&requested_tablet_ids);
    let recovered_paths =
        recover_missing_paths_from_persisted_runtime(&requested_tablet_ids, &local_paths);
    if !recovered_paths.is_empty() {
        let upserted = starlet_shard_registry::upsert_many(
            recovered_paths
                .iter()
                .map(|(tablet_id, path)| (*tablet_id, path.clone())),
        );
        if upserted > 0 {
            tracing::warn!(
                target: "novarocks::lake",
                table = %table.cache_key(),
                recovered = upserted,
                "recovered tablet root paths from persisted runtime because AddShard cache was incomplete"
            );
        }
        local_paths.extend(recovered_paths);
    }

    let mut fe_recovery_error = None;
    let missing_after_local = collect_missing_tablet_ids(&requested_tablet_ids, &local_paths);
    if !missing_after_local.is_empty() {
        match recover_missing_paths_from_fe(fe_addr, table, &missing_after_local) {
            Ok(recovered_paths) => {
                if !recovered_paths.is_empty() {
                    let recovered_infos = recovered_paths
                        .iter()
                        .map(|(tablet_id, path)| {
                            (
                                *tablet_id,
                                crate::runtime::starlet_shard_registry::StarletShardInfo {
                                    full_path: path.clone(),
                                    // Build the inferred config before upsert_many_infos acquires
                                    // the shard registry mutex to avoid lock re-entry deadlock.
                                    s3: starlet_shard_registry::infer_s3_config_for_path(path),
                                },
                            )
                        })
                        .collect::<Vec<_>>();
                    let upserted = starlet_shard_registry::upsert_many_infos(recovered_infos);
                    if upserted > 0 {
                        tracing::warn!(
                            target: "novarocks::lake",
                            table = %table.cache_key(),
                            recovered = upserted,
                            "recovered tablet root paths from FE fallback because local shard cache was incomplete"
                        );
                    }
                    local_paths.extend(recovered_paths);
                }
            }
            Err(err) => {
                tracing::warn!(
                    target: "novarocks::lake",
                    table = %table.cache_key(),
                    tablet_ids = ?missing_after_local,
                    error = %err,
                    "failed to recover missing tablet root paths from FE fallback"
                );
                fe_recovery_error = Some(err);
            }
        }
    }

    if !paths_cover_refs(&local_paths, refs) {
        let missing = collect_missing_tablet_ids(&requested_tablet_ids, &local_paths);
        let fe_context = fe_recovery_error
            .map(|err| format!("; fe_fallback_error={err}"))
            .unwrap_or_default();
        return Err(format!(
            "missing shard path for tablet_ids={missing:?} after local AddShard cache, \
            persisted runtime, and FE fallback{fe_context}"
        ));
    }

    if let Some(qid) = query_id {
        let _ = query_context_manager().set_lake_tablet_paths(qid, cache_key, local_paths.clone());
    }
    cache_table_identity_names(table);
    select_paths_for_refs(local_paths, refs)
}

fn recover_missing_paths_from_persisted_runtime(
    requested_tablet_ids: &[i64],
    resolved_paths: &HashMap<i64, String>,
) -> HashMap<i64, String> {
    let mut recovered = HashMap::new();
    for tablet_id in requested_tablet_ids {
        if resolved_paths.contains_key(tablet_id) {
            continue;
        }
        let Ok(runtime) = get_tablet_runtime(*tablet_id) else {
            continue;
        };
        let root_path = runtime.root_path.trim().trim_end_matches('/').to_string();
        if root_path.is_empty() {
            continue;
        }
        recovered.insert(*tablet_id, root_path);
    }
    recovered
}

fn collect_missing_tablet_ids(
    requested_tablet_ids: &[i64],
    resolved_paths: &HashMap<i64, String>,
) -> Vec<i64> {
    let mut missing = requested_tablet_ids
        .iter()
        .copied()
        .filter(|tablet_id| !resolved_paths.contains_key(tablet_id))
        .collect::<Vec<_>>();
    missing.sort_unstable();
    missing.dedup();
    missing
}

fn recover_missing_paths_from_fe(
    fe_addr: Option<&types::TNetworkAddress>,
    table: &LakeTableIdentity,
    missing_tablet_ids: &[i64],
) -> Result<HashMap<i64, String>, String> {
    if missing_tablet_ids.is_empty() {
        return Ok(HashMap::new());
    }
    let fe_addr = resolve_frontend_addr(fe_addr).ok_or_else(|| {
        "missing FE address for metadata fallback (coord is absent and heartbeat cache is empty)"
            .to_string()
    })?;

    let tablet_to_partition = fetch_tablet_partition_ids_from_fe(&fe_addr, missing_tablet_ids)?;
    if tablet_to_partition.is_empty() {
        return Ok(HashMap::new());
    }

    let mut partition_to_tablets: HashMap<i64, Vec<i64>> = HashMap::new();
    for (tablet_id, partition_id) in tablet_to_partition {
        partition_to_tablets
            .entry(partition_id)
            .or_default()
            .push(tablet_id);
    }
    let target_partition_ids = partition_to_tablets.keys().copied().collect::<Vec<_>>();
    let mut partition_to_path =
        fetch_partition_storage_paths_from_fe(&fe_addr, table, &target_partition_ids)?;
    if partition_to_path.len() < target_partition_ids.len() {
        recover_partition_storage_paths_from_table_root(
            &fe_addr,
            table,
            &target_partition_ids,
            &mut partition_to_path,
        )?;
    }

    let mut recovered = HashMap::new();
    for (partition_id, tablet_ids) in partition_to_tablets {
        let Some(path) = partition_to_path.get(&partition_id) else {
            continue;
        };
        let Some(partition_path) = normalize_storage_path(path) else {
            continue;
        };
        for tablet_id in tablet_ids {
            recovered.insert(tablet_id, partition_path.clone());
        }
    }
    Ok(recovered)
}

fn recover_partition_storage_paths_from_table_root(
    fe_addr: &types::TNetworkAddress,
    table: &LakeTableIdentity,
    target_partition_ids: &[i64],
    partition_to_path: &mut HashMap<i64, String>,
) -> Result<(), String> {
    if target_partition_ids.is_empty() {
        return Ok(());
    }
    let missing_partition_ids = target_partition_ids
        .iter()
        .copied()
        .filter(|partition_id| !partition_to_path.contains_key(partition_id))
        .collect::<Vec<_>>();
    if missing_partition_ids.is_empty() {
        return Ok(());
    }

    let Some(table_root_path) = fetch_table_storage_root_from_fe(fe_addr, table)? else {
        return Ok(());
    };
    for partition_id in missing_partition_ids {
        let path = format!("{table_root_path}/{partition_id}");
        if let Some(normalized) = normalize_storage_path(&path) {
            partition_to_path.entry(partition_id).or_insert(normalized);
        }
    }
    Ok(())
}

fn fetch_table_storage_root_from_fe(
    fe_addr: &types::TNetworkAddress,
    table: &LakeTableIdentity,
) -> Result<Option<String>, String> {
    if table.db_id <= 0 || table.table_id <= 0 {
        return Ok(None);
    }
    use mysql::prelude::Queryable;

    let mut last_error = None;
    for query_port in infer_fe_query_ports(fe_addr) {
        let opts = mysql::OptsBuilder::default()
            .ip_or_hostname(Some(fe_addr.hostname.clone()))
            .tcp_port(query_port)
            .user(Some("root"))
            .read_timeout(Some(Duration::from_secs(FE_META_FETCH_TIMEOUT_SECS)))
            .write_timeout(Some(Duration::from_secs(FE_META_FETCH_TIMEOUT_SECS)))
            .tcp_connect_timeout(Some(Duration::from_secs(FE_META_FETCH_TIMEOUT_SECS)))
            .prefer_socket(false);
        let pool = match mysql::Pool::new(opts) {
            Ok(pool) => pool,
            Err(err) => {
                last_error = Some(format!(
                    "connect FE query port failed host={} port={} error={}",
                    fe_addr.hostname, query_port, err
                ));
                continue;
            }
        };
        let mut conn = match pool.get_conn() {
            Ok(conn) => conn,
            Err(err) => {
                last_error = Some(format!(
                    "open FE query connection failed host={} port={} error={}",
                    fe_addr.hostname, query_port, err
                ));
                continue;
            }
        };
        let query = format!("SHOW PROC '/dbs/{}'", table.db_id);
        let rows = match conn.query::<mysql::Row, _>(&query) {
            Ok(rows) => rows,
            Err(err) => {
                last_error = Some(format!(
                    "FE SHOW PROC query failed host={} port={} query={} error={}",
                    fe_addr.hostname, query_port, query, err
                ));
                continue;
            }
        };
        if let Some(path) = parse_table_storage_root_from_proc_rows(rows, table.table_id) {
            return Ok(Some(path));
        }
    }
    if let Some(err) = last_error {
        tracing::warn!(
            target: "novarocks::lake",
            table = %table.cache_key(),
            error = %err,
            "failed to recover table storage root path from FE SHOW PROC fallback"
        );
    }
    Ok(None)
}

fn infer_fe_query_ports(fe_addr: &types::TNetworkAddress) -> Vec<u16> {
    let mut ports = Vec::new();
    if fe_addr.port > 0 {
        if let Ok(v) = u16::try_from(fe_addr.port as i64 + 10) {
            ports.push(v);
        }
        if let Ok(v) = u16::try_from(fe_addr.port)
            && !ports.contains(&v)
        {
            ports.push(v);
        }
    }
    if ports.is_empty() {
        ports.push(9030_u16);
    }
    ports
}

fn parse_table_storage_root_from_proc_rows(rows: Vec<mysql::Row>, table_id: i64) -> Option<String> {
    for row in rows {
        let row_table_id = row.as_ref(0).and_then(mysql_value_to_i64)?;
        if row_table_id != table_id {
            continue;
        }
        for idx in (0..row.len()).rev() {
            let Some(raw) = row.as_ref(idx).and_then(mysql_value_to_string) else {
                continue;
            };
            if !looks_like_storage_path(&raw) {
                continue;
            }
            if let Some(path) = normalize_storage_path(&raw) {
                return Some(path);
            }
        }
    }
    None
}

fn looks_like_storage_path(path: &str) -> bool {
    let trimmed = path.trim();
    trimmed.starts_with("s3://")
        || trimmed.starts_with("oss://")
        || trimmed.starts_with("file:/")
        || trimmed.starts_with('/')
}

fn mysql_value_to_i64(value: &mysql::Value) -> Option<i64> {
    match value {
        mysql::Value::Int(v) => Some(*v),
        mysql::Value::UInt(v) => i64::try_from(*v).ok(),
        mysql::Value::Bytes(bytes) => std::str::from_utf8(bytes).ok()?.trim().parse::<i64>().ok(),
        _ => None,
    }
}

fn mysql_value_to_string(value: &mysql::Value) -> Option<String> {
    match value {
        mysql::Value::Bytes(bytes) => std::str::from_utf8(bytes)
            .ok()
            .map(|v| v.trim().to_string()),
        mysql::Value::Int(v) => Some(v.to_string()),
        mysql::Value::UInt(v) => Some(v.to_string()),
        _ => None,
    }
}

fn resolve_frontend_addr(
    fe_addr: Option<&types::TNetworkAddress>,
) -> Option<types::TNetworkAddress> {
    fe_addr.cloned().or_else(disk_report::latest_fe_addr)
}

fn fetch_tablet_partition_ids_from_fe(
    fe_addr: &types::TNetworkAddress,
    tablet_ids: &[i64],
) -> Result<HashMap<i64, i64>, String> {
    if tablet_ids.is_empty() {
        return Ok(HashMap::new());
    }

    let mut request = frontend_service::TPartitionMetaRequest::default();
    request.tablet_ids = Some(tablet_ids.to_vec());
    let response = with_frontend_client(fe_addr, |client| {
        client
            .get_partition_meta(request)
            .map_err(|e| format!("FE getPartitionMeta rpc failed: {e}"))
    })?;

    let status = response
        .status
        .as_ref()
        .ok_or_else(|| "FE getPartitionMeta returned empty status".to_string())?;
    if status.status_code != status_code::TStatusCode::OK {
        return Err(format!(
            "FE getPartitionMeta returned non-OK: status={:?}",
            status
        ));
    }

    let partition_metas = response.partition_metas.unwrap_or_default();
    let tablet_idx = response.tablet_id_partition_meta_index.unwrap_or_default();
    let mut result = HashMap::new();
    for tablet_id in tablet_ids {
        let Some(idx) = tablet_idx.get(tablet_id) else {
            continue;
        };
        let idx = usize::try_from(*idx).map_err(|_| {
            format!(
                "FE getPartitionMeta returned invalid negative index for tablet_id={}",
                tablet_id
            )
        })?;
        let partition_id = partition_metas
            .get(idx)
            .and_then(|meta| meta.partition_id)
            .filter(|v| *v > 0)
            .ok_or_else(|| {
                format!(
                    "FE getPartitionMeta returned missing partition_id for tablet_id={}",
                    tablet_id
                )
            })?;
        result.insert(*tablet_id, partition_id);
    }
    Ok(result)
}

fn fetch_partition_storage_paths_from_fe(
    fe_addr: &types::TNetworkAddress,
    table: &LakeTableIdentity,
    partition_ids: &[i64],
) -> Result<HashMap<i64, String>, String> {
    if partition_ids.is_empty() {
        return Ok(HashMap::new());
    }

    let target_partition_ids = partition_ids
        .iter()
        .copied()
        .filter(|partition_id| *partition_id > 0)
        .collect::<HashSet<_>>();
    if target_partition_ids.is_empty() {
        return Ok(HashMap::new());
    }

    let mut resolved = HashMap::new();
    let mut offsets = vec![table.table_id.max(0)];
    if offsets[0] != 0 {
        offsets.push(0);
    }
    for offset_start in offsets {
        let mut offset = offset_start;
        for _ in 0..FE_PARTITIONS_META_MAX_PAGES {
            let mut request = frontend_service::TGetPartitionsMetaRequest::default();
            // FE getPartitionsMeta expects auth_info and will throw if absent.
            // For internal metadata fallback we use a minimal local-root identity.
            let mut auth = frontend_service::TAuthInfo::default();
            auth.user = Some("root".to_string());
            auth.user_ip = Some("127.0.0.1".to_string());
            auth.pattern = Some("%".to_string());
            auth.catalog_name = Some(table.catalog.clone());
            request.auth_info = Some(auth);
            request.start_table_id_offset = Some(offset);
            let response = with_frontend_client(fe_addr, |client| {
                client
                    .get_partitions_meta(request)
                    .map_err(|e| format!("FE getPartitionsMeta rpc failed: {e}"))
            })?;

            if let Some(partitions) = response.partitions_meta_infos.as_ref() {
                collect_partition_storage_paths(
                    table,
                    &target_partition_ids,
                    partitions,
                    &mut resolved,
                );
            }
            if resolved.len() >= target_partition_ids.len() {
                return Ok(resolved);
            }

            let next_offset = response.next_table_id_offset.unwrap_or(0);
            if next_offset <= 0 || next_offset == offset {
                break;
            }
            offset = next_offset;
        }
        if resolved.len() >= target_partition_ids.len() {
            break;
        }
    }
    Ok(resolved)
}

fn collect_partition_storage_paths(
    table: &LakeTableIdentity,
    target_partition_ids: &HashSet<i64>,
    partitions: &[frontend_service::TPartitionMetaInfo],
    out: &mut HashMap<i64, String>,
) {
    for partition in partitions {
        let partition_id = partition.partition_id.unwrap_or_default();
        if !target_partition_ids.contains(&partition_id) {
            continue;
        }
        if !partition_meta_matches_table(table, partition) {
            continue;
        }
        let Some(storage_path) = partition
            .storage_path
            .as_deref()
            .and_then(normalize_storage_path)
        else {
            continue;
        };
        out.entry(partition_id).or_insert(storage_path);
    }
}

fn partition_meta_matches_table(
    table: &LakeTableIdentity,
    partition: &frontend_service::TPartitionMetaInfo,
) -> bool {
    if !is_unknown_identity_name(&table.db_name) {
        let Some(db_name) = partition.db_name.as_deref() else {
            return false;
        };
        if db_name.trim() != table.db_name {
            return false;
        }
    }
    if !is_unknown_identity_name(&table.table_name) {
        let Some(table_name) = partition.table_name.as_deref() else {
            return false;
        };
        if table_name.trim() != table.table_name {
            return false;
        }
    }
    true
}

fn normalize_storage_path(path: &str) -> Option<String> {
    let normalized = path.trim().trim_end_matches('/');
    if normalized.is_empty() {
        None
    } else {
        Some(normalized.to_string())
    }
}

fn with_frontend_client<T>(
    fe_addr: &types::TNetworkAddress,
    f: impl FnOnce(&mut dyn TFrontendServiceSyncClient) -> Result<T, String>,
) -> Result<T, String> {
    let addr: SocketAddr = format!("{}:{}", fe_addr.hostname, fe_addr.port)
        .parse()
        .map_err(|e| format!("invalid FE address: {e}"))?;
    let stream = TcpStream::connect_timeout(&addr, Duration::from_secs(FE_META_FETCH_TIMEOUT_SECS))
        .map_err(|e| format!("connect FE failed: {e}"))?;
    let _ = stream.set_read_timeout(Some(Duration::from_secs(FE_META_FETCH_TIMEOUT_SECS)));
    let _ = stream.set_write_timeout(Some(Duration::from_secs(FE_META_FETCH_TIMEOUT_SECS)));
    let _ = stream.set_nodelay(true);

    let channel = TTcpChannel::with_stream(stream);
    let (i_chan, o_chan) = channel
        .split()
        .map_err(|e| format!("split FE thrift channel failed: {e}"))?;
    let i_trans = TBufferedReadTransport::new(i_chan);
    let o_trans = TBufferedWriteTransport::new(o_chan);
    let i_prot = TBinaryInputProtocol::new(i_trans, true);
    let o_prot = TBinaryOutputProtocol::new(o_trans, true);
    let mut client = FrontendServiceSyncClient::new(i_prot, o_prot);
    f(&mut client)
}

fn paths_cover_refs(paths: &HashMap<i64, String>, refs: &[LakeTabletPartitionRef]) -> bool {
    refs.iter().all(|r| paths.contains_key(&r.tablet_id))
}

fn select_paths_for_refs(
    full_paths: HashMap<i64, String>,
    refs: &[LakeTabletPartitionRef],
) -> Result<HashMap<i64, String>, String> {
    let mut selected = HashMap::with_capacity(refs.len());
    for r in refs {
        let raw_path = full_paths.get(&r.tablet_id).ok_or_else(|| {
            format!(
                "missing tablet path for tablet_id={} in resolved map",
                r.tablet_id
            )
        })?;
        let path = normalize_selected_tablet_path(raw_path).ok_or_else(|| {
            format!(
                "invalid tablet path for tablet_id={} in resolved map: {}",
                r.tablet_id, raw_path
            )
        })?;
        selected.insert(r.tablet_id, path);
    }
    Ok(selected)
}

fn normalize_selected_tablet_path(path: &str) -> Option<String> {
    normalize_storage_path(path)
}

#[cfg(test)]
mod tests {
    use super::{
        LakeTableIdentity, LakeTabletPartitionRef, cache_table_identity_names,
        clear_table_identity_name_cache_for_test, find_cached_table_identity_names,
        resolve_tablet_paths_for_refs,
    };
    use crate::connector::starrocks::lake::context::{
        TabletWriteContext, clear_persisted_tablet_runtime_for_test,
        clear_tablet_runtime_cache_for_test, register_tablet_runtime,
    };
    use crate::runtime::starlet_shard_registry;
    use crate::service::grpc_client::proto::starrocks::TabletSchemaPb;
    use std::sync::{Mutex, MutexGuard, OnceLock};

    fn global_test_guard() -> MutexGuard<'static, ()> {
        static TEST_GUARD: OnceLock<Mutex<()>> = OnceLock::new();
        TEST_GUARD
            .get_or_init(|| Mutex::new(()))
            .lock()
            .expect("lock fe_v2_meta test guard")
    }

    fn sample_table_identity() -> LakeTableIdentity {
        LakeTableIdentity {
            catalog: "default_catalog".to_string(),
            db_name: "db1".to_string(),
            table_name: "t1".to_string(),
            db_id: 9101,
            table_id: 9201,
            schema_id: 9301,
        }
    }

    #[test]
    fn resolve_tablet_paths_uses_add_shard_registry() {
        let _guard = global_test_guard();
        starlet_shard_registry::clear_for_test();
        clear_tablet_runtime_cache_for_test();
        clear_persisted_tablet_runtime_for_test(9_001_001).expect("cleanup 9001001");
        clear_persisted_tablet_runtime_for_test(9_001_002).expect("cleanup 9001002");
        let table = sample_table_identity();
        let inserted = starlet_shard_registry::upsert_many(vec![
            (9_001_001_i64, "s3://bucket/root/p10".to_string()),
            (9_001_002_i64, "s3://bucket/root/p11".to_string()),
        ]);
        assert_eq!(inserted, 2);

        let refs = vec![
            LakeTabletPartitionRef {
                tablet_id: 9_001_001,
            },
            LakeTabletPartitionRef {
                tablet_id: 9_001_002,
            },
        ];
        let resolved = resolve_tablet_paths_for_refs(None, None, &table, &refs).unwrap();
        assert_eq!(resolved.len(), 2);
        assert_eq!(
            resolved.get(&9_001_001),
            Some(&"s3://bucket/root/p10".to_string())
        );
        assert_eq!(
            resolved.get(&9_001_002),
            Some(&"s3://bucket/root/p11".to_string())
        );
    }

    #[test]
    fn resolve_tablet_paths_keeps_partition_path_when_numeric_leaf_mismatch() {
        let _guard = global_test_guard();
        starlet_shard_registry::clear_for_test();
        clear_tablet_runtime_cache_for_test();
        clear_persisted_tablet_runtime_for_test(9_004_051).expect("cleanup 9004051");
        clear_persisted_tablet_runtime_for_test(9_004_053).expect("cleanup 9004053");
        let table = sample_table_identity();
        let inserted = starlet_shard_registry::upsert_many(vec![
            (9_004_051_i64, "s3://bucket/root/db1/p1/9004051".to_string()),
            // Keep the FE-provided partition path as-is for all tablets in the same partition.
            (9_004_053_i64, "s3://bucket/root/db1/p1/9004051".to_string()),
        ]);
        assert_eq!(inserted, 2);

        let refs = vec![
            LakeTabletPartitionRef {
                tablet_id: 9_004_051,
            },
            LakeTabletPartitionRef {
                tablet_id: 9_004_053,
            },
        ];
        let resolved = resolve_tablet_paths_for_refs(None, None, &table, &refs).unwrap();
        assert_eq!(
            resolved.get(&9_004_051),
            Some(&"s3://bucket/root/db1/p1/9004051".to_string())
        );
        assert_eq!(
            resolved.get(&9_004_053),
            Some(&"s3://bucket/root/db1/p1/9004051".to_string())
        );
    }

    #[test]
    fn resolve_tablet_paths_fails_when_add_shard_cache_missing() {
        let _guard = global_test_guard();
        starlet_shard_registry::clear_for_test();
        clear_tablet_runtime_cache_for_test();
        clear_persisted_tablet_runtime_for_test(9_002_001).expect("cleanup 9002001");
        clear_persisted_tablet_runtime_for_test(9_002_002).expect("cleanup 9002002");
        let table = sample_table_identity();
        let inserted = starlet_shard_registry::upsert_many(vec![(
            9_002_001_i64,
            "s3://bucket/root/p10".to_string(),
        )]);
        assert_eq!(inserted, 1);

        let refs = vec![
            LakeTabletPartitionRef {
                tablet_id: 9_002_001,
            },
            LakeTabletPartitionRef {
                tablet_id: 9_002_002,
            },
        ];
        let err = resolve_tablet_paths_for_refs(None, None, &table, &refs).unwrap_err();
        assert!(err.contains("after local AddShard cache"), "err={err}");
        assert!(err.contains("9002002"), "err={err}");
    }

    #[test]
    fn resolve_tablet_paths_recovers_from_persisted_runtime() {
        let _guard = global_test_guard();
        starlet_shard_registry::clear_for_test();
        clear_tablet_runtime_cache_for_test();
        clear_persisted_tablet_runtime_for_test(9_003_001).expect("cleanup 9003001");

        let persisted = TabletWriteContext {
            db_id: 11,
            table_id: 12,
            tablet_id: 9_003_001,
            tablet_root_path: "s3://bucket/persisted/t9003001".to_string(),
            tablet_schema: TabletSchemaPb::default(),
            s3_config: Some(crate::runtime::starlet_shard_registry::S3StoreConfig {
                endpoint: "http://127.0.0.1:9000".to_string(),
                bucket: "bucket".to_string(),
                root: "persisted".to_string(),
                access_key_id: "ak".to_string(),
                access_key_secret: "sk".to_string(),
                region: Some("us-east-1".to_string()),
                enable_path_style_access: Some(true),
            }),
            partial_update: Default::default(),
        };
        register_tablet_runtime(&persisted).expect("persist runtime");

        let table = sample_table_identity();
        let refs = vec![LakeTabletPartitionRef {
            tablet_id: 9_003_001,
        }];
        let resolved = resolve_tablet_paths_for_refs(None, None, &table, &refs).unwrap();
        assert_eq!(
            resolved.get(&9_003_001),
            Some(&"s3://bucket/persisted/t9003001".to_string())
        );

        let cache_paths = starlet_shard_registry::select_paths(&[9_003_001]);
        assert_eq!(
            cache_paths.get(&9_003_001),
            Some(&"s3://bucket/persisted/t9003001".to_string())
        );

        clear_tablet_runtime_cache_for_test();
        clear_persisted_tablet_runtime_for_test(9_003_001).expect("cleanup 9003001");
    }

    #[test]
    fn table_identity_name_cache_roundtrip() {
        let _guard = global_test_guard();
        clear_table_identity_name_cache_for_test();
        let table = sample_table_identity();
        cache_table_identity_names(&table);
        let cached = find_cached_table_identity_names(&table.catalog, table.db_id, table.table_id);
        assert_eq!(cached, Some(("db1".to_string(), "t1".to_string())));
    }

    #[test]
    fn table_identity_name_cache_skips_unknown_placeholders() {
        let _guard = global_test_guard();
        clear_table_identity_name_cache_for_test();
        let table = LakeTableIdentity {
            catalog: "default_catalog".to_string(),
            db_name: "__unknown_db__".to_string(),
            table_name: "t1".to_string(),
            db_id: 19_101,
            table_id: 19_201,
            schema_id: 19_301,
        };
        cache_table_identity_names(&table);
        let cached = find_cached_table_identity_names(&table.catalog, table.db_id, table.table_id);
        assert!(cached.is_none());
    }
}
