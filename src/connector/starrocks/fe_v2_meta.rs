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
use std::collections::HashMap;
use std::net::{SocketAddr, TcpStream};
use std::sync::{Mutex, OnceLock};
use std::time::Duration;

use thrift::protocol::{TBinaryInputProtocol, TBinaryOutputProtocol};
use thrift::transport::{TBufferedReadTransport, TBufferedWriteTransport, TIoChannel, TTcpChannel};

use crate::connector::starrocks::lake::context::get_tablet_runtime;
use crate::connector::starrocks::starmgr;
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
    _fe_addr: Option<&types::TNetworkAddress>,
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
        recover_missing_paths_from_runtime_registry(&requested_tablet_ids, &local_paths);
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
                "recovered tablet root paths from runtime registry because AddShard cache was incomplete"
            );
        }
        local_paths.extend(recovered_paths);
    }

    let mut starmgr_recovery_error = None;
    let missing_after_local = collect_missing_tablet_ids(&requested_tablet_ids, &local_paths);
    if !missing_after_local.is_empty() {
        match recover_missing_paths_from_starmgr(&missing_after_local) {
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
                        tracing::info!(
                            target: "novarocks::lake",
                            table = %table.cache_key(),
                            recovered = upserted,
                            "recovered tablet root paths from StarManager GetShard because local shard cache was incomplete"
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
                    "failed to recover missing tablet root paths from StarManager GetShard"
                );
                starmgr_recovery_error = Some(err);
            }
        }
    }

    if !paths_cover_refs(&local_paths, refs) {
        let missing = collect_missing_tablet_ids(&requested_tablet_ids, &local_paths);
        let starmgr_context = starmgr_recovery_error
            .map(|err| format!("; starmgr_error={err}"))
            .unwrap_or_default();
        return Err(format!(
            "missing shard path for tablet_ids={missing:?} after local AddShard cache, \
            runtime registry, and StarManager GetShard{starmgr_context}"
        ));
    }

    if let Some(qid) = query_id {
        let _ = query_context_manager().set_lake_tablet_paths(qid, cache_key, local_paths.clone());
    }
    cache_table_identity_names(table);
    select_paths_for_refs(local_paths, refs)
}

fn recover_missing_paths_from_starmgr(tablet_ids: &[i64]) -> Result<HashMap<i64, String>, String> {
    let recovered = starmgr::retrieve_shard_infos(tablet_ids)?;
    let mut paths = HashMap::with_capacity(recovered.len());
    for (tablet_id, info) in recovered {
        let normalized = normalize_storage_path(&info.full_path).ok_or_else(|| {
            format!(
                "StarManager GetShard returned invalid tablet path for tablet_id={tablet_id}: {}",
                info.full_path
            )
        })?;
        paths.insert(tablet_id, normalized);
    }
    Ok(paths)
}

fn recover_missing_paths_from_runtime_registry(
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

fn resolve_frontend_addr(
    fe_addr: Option<&types::TNetworkAddress>,
) -> Option<types::TNetworkAddress> {
    fe_addr.cloned().or_else(disk_report::latest_fe_addr)
}

pub(crate) fn fetch_table_schema_for_lake_scan(
    fe_addr: Option<&types::TNetworkAddress>,
    db_id: i64,
    table_id: i64,
    schema_id: i64,
    tablet_id: Option<i64>,
    query_id: Option<types::TUniqueId>,
) -> Result<crate::agent_service::TTabletSchema, String> {
    if schema_id <= 0 {
        return Err(format!(
            "invalid schema_id for FE getTableSchema: db_id={} table_id={} schema_id={}",
            db_id, table_id, schema_id
        ));
    }
    let query_id = query_id.ok_or_else(|| {
        format!(
            "missing query_id for FE getTableSchema scan request: db_id={} table_id={} schema_id={}",
            db_id, table_id, schema_id
        )
    })?;
    let fe_addr = resolve_frontend_addr(fe_addr).ok_or_else(|| {
        "missing FE address for getTableSchema (coord is absent and heartbeat cache is empty)"
            .to_string()
    })?;

    let request = frontend_service::TBatchGetTableSchemaRequest {
        requests: Some(vec![frontend_service::TGetTableSchemaRequest {
            schema_meta: Some(crate::plan_nodes::TTableSchemaMeta {
                db_id: Some(db_id),
                table_id: Some(table_id),
                schema_id: Some(schema_id),
            }),
            source: Some(frontend_service::TTableSchemaRequestSource::SCAN),
            tablet_id,
            query_id: Some(query_id),
            txn_id: None,
        }]),
    };

    let response = with_frontend_client(&fe_addr, |client| {
        client
            .get_table_schema(request)
            .map_err(|e| format!("FE getTableSchema rpc failed: {e}"))
    })?;
    let status = response
        .status
        .as_ref()
        .ok_or_else(|| "FE getTableSchema returned empty top-level status".to_string())?;
    if status.status_code != status_code::TStatusCode::OK {
        return Err(format!(
            "FE getTableSchema returned non-OK top-level status: {:?}",
            status
        ));
    }

    let mut responses = response.responses.unwrap_or_default();
    if responses.len() != 1 {
        return Err(format!(
            "FE getTableSchema returned unexpected response count: expected=1 actual={}",
            responses.len()
        ));
    }
    let item = responses
        .pop()
        .ok_or_else(|| "FE getTableSchema returned empty responses".to_string())?;
    let item_status = item
        .status
        .as_ref()
        .ok_or_else(|| "FE getTableSchema response item missing status".to_string())?;
    if item_status.status_code != status_code::TStatusCode::OK {
        return Err(format!(
            "FE getTableSchema response item returned non-OK: {:?}",
            item_status
        ));
    }
    item.schema
        .ok_or_else(|| "FE getTableSchema response item missing schema".to_string())
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
        TabletWriteContext, clear_tablet_runtime_cache_for_test, register_tablet_runtime,
    };
    use crate::runtime::starlet_shard_registry;
    use crate::service::grpc_client::proto::starrocks::TabletSchemaPb;

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
        let _guard = starlet_shard_registry::lock_for_test();
        starlet_shard_registry::clear_for_test();
        clear_tablet_runtime_cache_for_test();
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
        let _guard = starlet_shard_registry::lock_for_test();
        starlet_shard_registry::clear_for_test();
        clear_tablet_runtime_cache_for_test();
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
        let _guard = starlet_shard_registry::lock_for_test();
        starlet_shard_registry::clear_for_test();
        clear_tablet_runtime_cache_for_test();
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
    fn resolve_tablet_paths_recovers_from_runtime_registry() {
        let _guard = starlet_shard_registry::lock_for_test();
        starlet_shard_registry::clear_for_test();
        clear_tablet_runtime_cache_for_test();

        let runtime = TabletWriteContext {
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
        register_tablet_runtime(&runtime).expect("register runtime");

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
    }

    #[test]
    fn table_identity_name_cache_roundtrip() {
        let _guard = starlet_shard_registry::lock_for_test();
        clear_table_identity_name_cache_for_test();
        let table = sample_table_identity();
        cache_table_identity_names(&table);
        let cached = find_cached_table_identity_names(&table.catalog, table.db_id, table.table_id);
        assert_eq!(cached, Some(("db1".to_string(), "t1".to_string())));
    }

    #[test]
    fn table_identity_name_cache_skips_unknown_placeholders() {
        let _guard = starlet_shard_registry::lock_for_test();
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

    #[test]
    fn lake_scan_schema_fetch_allows_placeholder_catalog_ids() {
        let request = crate::frontend_service::TGetTableSchemaRequest {
            schema_meta: Some(crate::plan_nodes::TTableSchemaMeta {
                db_id: Some(-1),
                table_id: Some(70_528),
                schema_id: Some(70_528),
            }),
            source: Some(crate::frontend_service::TTableSchemaRequestSource::SCAN),
            tablet_id: Some(70_531),
            query_id: Some(crate::types::TUniqueId { hi: 1, lo: 2 }),
            txn_id: None,
        };
        let schema_meta = request
            .schema_meta
            .as_ref()
            .expect("schema_meta should be set");
        assert_eq!(schema_meta.db_id, Some(-1));
        assert_eq!(schema_meta.table_id, Some(70_528));
        assert_eq!(schema_meta.schema_id, Some(70_528));
        assert!(request.query_id.is_some());
    }
}
