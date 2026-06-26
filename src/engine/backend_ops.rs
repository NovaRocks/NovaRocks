use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use arrow::array::StringArray;
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;

use crate::common::app_config::ClusterRole;
use crate::engine::{QueryResult, QueryResultColumn, StandaloneState, StatementResult};
use crate::meta::MetaStoreProvider;
use crate::meta::repository::backend::StoredBackend;
use crate::runtime::backend_registry::{BackendRegistry, BackendState, BeId, HeartbeatOutcome};
use crate::runtime::query_state::in_flight_table;
use crate::sql::parser::ast::{AddBackendStmt, DropBackendStmt};

pub(crate) fn ensure_backend_registry(
    state: &Arc<StandaloneState>,
) -> Result<Arc<BackendRegistry>, String> {
    if let Some(registry) = crate::runtime::backend_registry::backend_registry() {
        return Ok(registry);
    }

    let cfg = crate::novarocks_config::config().map_err(|e| format!("read config failed: {e}"))?;
    let registry = Arc::new(BackendRegistry::new(cfg.cluster.heartbeat_timeout_retries));

    if let Some(provider) = state.metadata_provider.as_ref() {
        let read = provider
            .begin_read()
            .map_err(|e| format!("open backend metadata read transaction failed: {e}"))?;
        for stored in state
            .backend_repo
            .list_backends(read.as_ref())
            .map_err(|e| format!("load backend metadata failed: {e}"))?
        {
            let be_id = BeId::try_from(stored.be_id)
                .map_err(|_| format!("invalid persisted backend id {}", stored.be_id))?;
            registry.restore_backend(
                be_id,
                parse_backend_addr(&stored.endpoint)?,
                backend_state_from_str(&stored.state)?,
            );
        }
    }

    let seed_endpoints = cfg
        .cluster
        .backends
        .iter()
        .map(|addr| parse_backend_addr(addr))
        .collect::<Result<Vec<_>, _>>()?;
    registry.seed_from_config(&seed_endpoints);

    let installed =
        crate::runtime::backend_registry::install_backend_registry(Arc::clone(&registry));
    #[cfg(not(test))]
    if installed {
        crate::runtime::heartbeat_mgr::spawn(
            Arc::clone(&registry),
            Duration::from_millis(cfg.cluster.heartbeat_interval_ms),
            Arc::new(crate::runtime::registry_cleanup::QueryCleanupSink::new()),
        );
    }
    #[cfg(test)]
    let _ = installed;

    Ok(crate::runtime::backend_registry::backend_registry().unwrap_or(registry))
}

pub(crate) fn wait_for_configured_backends_live(
    _state: &Arc<StandaloneState>,
) -> Result<(), String> {
    let cfg = crate::novarocks_config::config().map_err(|e| format!("read config failed: {e}"))?;
    let configured = cfg
        .cluster
        .backends
        .iter()
        .map(|addr| parse_backend_addr(addr))
        .collect::<Result<Vec<_>, _>>()?;
    let Some(registry) = crate::runtime::backend_registry::backend_registry() else {
        return Err("role=fe backend registry is not initialized".to_string());
    };

    wait_for_configured_backends_live_with(
        registry.as_ref(),
        &configured,
        Duration::from_secs(5),
        Duration::from_millis(cfg.cluster.heartbeat_interval_ms.max(10).min(200)),
        crate::runtime::heartbeat_mgr::grpc_heartbeat,
    )
}

fn wait_for_configured_backends_live_with<F>(
    registry: &BackendRegistry,
    configured: &[SocketAddr],
    timeout: Duration,
    retry_interval: Duration,
    send: F,
) -> Result<(), String>
where
    F: Fn(BeId, SocketAddr) -> HeartbeatOutcome,
{
    if configured.is_empty() {
        return Ok(());
    }

    let deadline = std::time::Instant::now() + timeout;
    loop {
        crate::runtime::heartbeat_mgr::run_one_round(registry, &send);
        if configured_backend_live(registry, configured) {
            return Ok(());
        }
        if std::time::Instant::now() >= deadline {
            let configured = configured
                .iter()
                .map(ToString::to_string)
                .collect::<Vec<_>>()
                .join(", ");
            let snapshot = registry
                .snapshot()
                .into_iter()
                .map(|entry| {
                    format!(
                        "be_id={} endpoint={} state={} err={}",
                        entry.be_id,
                        entry.endpoint,
                        backend_state_to_str(entry.state),
                        entry.last_err.unwrap_or_default()
                    )
                })
                .collect::<Vec<_>>()
                .join("; ");
            return Err(format!(
                "role=fe startup timed out waiting for at least one configured backend to become Live; configured=[{configured}] registry=[{snapshot}]"
            ));
        }
        std::thread::sleep(retry_interval);
    }
}

fn configured_backend_live(registry: &BackendRegistry, configured: &[SocketAddr]) -> bool {
    registry
        .snapshot()
        .into_iter()
        .any(|entry| configured.contains(&entry.endpoint) && entry.state == BackendState::Live)
}

pub(crate) fn install_all_in_one_backend_registry(
    endpoint: SocketAddr,
    heartbeat_timeout_retries: u32,
) -> Result<Arc<BackendRegistry>, String> {
    if let Some(registry) = crate::runtime::backend_registry::backend_registry() {
        validate_all_in_one_loopback_registry(registry.as_ref(), endpoint)?;
        return Ok(registry);
    }

    let registry = Arc::new(BackendRegistry::new(heartbeat_timeout_retries));
    let be_id = registry.add_backend_with_state(endpoint, BackendState::Live);
    if be_id != 0 {
        return Err(format!(
            "all-in-one loopback backend must be backend 0, got {be_id}"
        ));
    }
    registry.apply_heartbeat_result(
        be_id,
        HeartbeatOutcome::Ok {
            start_epoch: crate::runtime::start_epoch::start_epoch(),
            version: env!("CARGO_PKG_VERSION").to_string(),
            num_cores: std::thread::available_parallelism()
                .map(|n| n.get().min(u32::MAX as usize) as u32)
                .unwrap_or(1),
            now_ms: current_time_millis(),
        },
    );

    crate::runtime::backend_registry::install_backend_registry(Arc::clone(&registry));
    Ok(crate::runtime::backend_registry::backend_registry().unwrap_or(registry))
}

fn validate_all_in_one_loopback_registry(
    registry: &BackendRegistry,
    endpoint: SocketAddr,
) -> Result<(), String> {
    let snapshot = registry.snapshot();
    let valid = snapshot.len() == 1
        && snapshot[0].be_id == 0
        && snapshot[0].endpoint == endpoint
        && snapshot[0].state == BackendState::Live;
    if valid {
        return Ok(());
    }
    let actual = snapshot
        .iter()
        .map(|entry| {
            format!(
                "be_id={} endpoint={} state={}",
                entry.be_id,
                entry.endpoint,
                backend_state_to_str(entry.state)
            )
        })
        .collect::<Vec<_>>()
        .join(", ");
    Err(format!(
        "all-in-one loopback backend registry mismatch: expected exactly one Live backend be_id=0 endpoint={endpoint}, got [{}]",
        actual
    ))
}

fn current_time_millis() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

pub(crate) fn live_backend_dispatch_entries() -> Result<Vec<(usize, SocketAddr)>, String> {
    if let Some(registry) = crate::runtime::backend_registry::backend_registry() {
        let live = registry.live_endpoints();
        if live.is_empty() {
            return Err("no live backend available".to_string());
        }
        return Ok(live
            .into_iter()
            .map(|(be_id, endpoint)| (be_id as usize, endpoint))
            .collect());
    }

    configured_backend_entries()
}

fn configured_backend_entries() -> Result<Vec<(usize, SocketAddr)>, String> {
    let cfg = crate::novarocks_config::config()
        .map_err(|e| format!("role=fe: cannot read config: {e}"))?;
    if cfg.cluster.backends.is_empty() {
        return Err("no live backend available".to_string());
    }
    cfg.cluster
        .backends
        .iter()
        .enumerate()
        .map(|(idx, backend)| {
            backend
                .parse::<SocketAddr>()
                .map(|endpoint| (idx, endpoint))
                .map_err(|e| format!("role=fe: invalid backend addr '{backend}': {e}"))
        })
        .collect()
}

pub(crate) fn execute_add_backend(
    state: &Arc<StandaloneState>,
    stmt: AddBackendStmt,
) -> Result<StatementResult, String> {
    require_role(&[ClusterRole::Fe], "ADD BACKEND")?;
    let endpoint = parse_backend_addr(&stmt.addr)?;
    let registry = ensure_backend_registry(state)?;
    let be_id = registry.add_backend(endpoint);
    persist_backend(state, be_id, endpoint, BackendState::Registering)?;
    Ok(StatementResult::Ok)
}

pub(crate) fn execute_drop_backend(
    state: &Arc<StandaloneState>,
    stmt: DropBackendStmt,
) -> Result<StatementResult, String> {
    require_role(&[ClusterRole::Fe], "DROP BACKEND")?;
    let endpoint = parse_backend_addr(&stmt.addr)?;
    let registry = ensure_backend_registry(state)?;
    let be_id = registry.mark_decommissioning(endpoint)?;

    if stmt.force || !in_flight_table().backend_has_inflight(be_id as usize) {
        force_remove_backend(state, registry.as_ref(), be_id, endpoint, stmt.force)?;
    } else {
        persist_backend(state, be_id, endpoint, BackendState::Decommissioning)?;
        spawn_decommission_watcher(state, registry, be_id, endpoint)?;
    }

    Ok(StatementResult::Ok)
}

pub(crate) fn execute_show_backends(
    state: &Arc<StandaloneState>,
) -> Result<StatementResult, String> {
    match current_role()? {
        ClusterRole::Fe => {
            let registry = ensure_backend_registry(state)?;
            Ok(StatementResult::Query(show_backends_result(
                registry.snapshot(),
            )?))
        }
        ClusterRole::AllInOne => {
            let registry = crate::runtime::backend_registry::backend_registry()
                .ok_or_else(|| "all-in-one backend registry is not initialized".to_string())?;
            Ok(StatementResult::Query(show_backends_result(
                registry.snapshot(),
            )?))
        }
        ClusterRole::Be => Err("SHOW BACKENDS is not available in role=be".to_string()),
    }
}

fn show_backends_result(
    mut entries: Vec<crate::runtime::backend_registry::BackendEntry>,
) -> Result<QueryResult, String> {
    entries.sort_by_key(|entry| entry.be_id);
    let column_names = [
        "BackendId",
        "Host",
        "GrpcPort",
        "State",
        "Alive",
        "LastHeartbeatMs",
        "StartEpoch",
        "Version",
        "NumCores",
        "ScheduledFragments",
        "ErrMsg",
    ];
    let mut columns = vec![Vec::<String>::new(); column_names.len()];
    for entry in entries {
        columns[0].push(entry.be_id.to_string());
        columns[1].push(entry.endpoint.ip().to_string());
        columns[2].push(entry.endpoint.port().to_string());
        columns[3].push(backend_state_to_str(entry.state).to_string());
        columns[4].push((entry.state == BackendState::Live).to_string());
        columns[5].push(entry.last_heartbeat_ms.to_string());
        columns[6].push(entry.start_epoch.to_string());
        columns[7].push(entry.version);
        columns[8].push(entry.num_cores.to_string());
        columns[9].push(entry.scheduled_fragments.to_string());
        columns[10].push(entry.last_err.unwrap_or_default());
    }

    let fields = column_names
        .iter()
        .map(|name| Field::new(*name, DataType::Utf8, false))
        .collect::<Vec<_>>();
    let arrays = columns
        .into_iter()
        .map(|values| Arc::new(StringArray::from(values)) as Arc<dyn arrow::array::Array>)
        .collect::<Vec<_>>();
    let batch = RecordBatch::try_new(Arc::new(Schema::new(fields)), arrays)
        .map_err(|e| format!("build SHOW BACKENDS result failed: {e}"))?;
    Ok(QueryResult {
        columns: column_names
            .iter()
            .map(|name| QueryResultColumn {
                name: (*name).to_string(),
                data_type: DataType::Utf8,
                nullable: false,
                logical_type: None,
            })
            .collect(),
        chunks: vec![crate::engine::record_batch_to_chunk(batch)?],
    })
}

fn spawn_decommission_watcher(
    state: &Arc<StandaloneState>,
    registry: Arc<BackendRegistry>,
    be_id: BeId,
    endpoint: SocketAddr,
) -> Result<(), String> {
    let cfg = crate::novarocks_config::config().map_err(|e| format!("read config failed: {e}"))?;
    let timeout = Duration::from_secs(cfg.cluster.decommission_timeout_secs);
    let provider = state.metadata_provider.clone();
    std::thread::Builder::new()
        .name(format!("backend-decommission-{be_id}"))
        .spawn(move || {
            let start = std::time::Instant::now();
            loop {
                if !in_flight_table().backend_has_inflight(be_id as usize) {
                    registry.remove(be_id);
                    let _ = delete_backend_with_provider(provider.as_ref(), &endpoint);
                    return;
                }
                if start.elapsed() >= timeout {
                    fail_backend_queries(be_id, format!("backend {be_id} decommission timed out"));
                    registry.remove(be_id);
                    let _ = delete_backend_with_provider(provider.as_ref(), &endpoint);
                    return;
                }
                std::thread::sleep(Duration::from_millis(200));
            }
        })
        .map_err(|e| format!("spawn backend decommission watcher failed: {e}"))?;
    Ok(())
}

fn force_remove_backend(
    state: &Arc<StandaloneState>,
    registry: &BackendRegistry,
    be_id: BeId,
    endpoint: SocketAddr,
    fail_queries: bool,
) -> Result<(), String> {
    if fail_queries {
        fail_backend_queries(be_id, format!("backend {be_id} dropped forcefully"));
    }
    registry.remove(be_id);
    delete_backend(state, &endpoint)?;
    Ok(())
}

fn fail_backend_queries(be_id: BeId, reason: String) {
    for query_id in in_flight_table().on_backend_failed(be_id as usize, reason.clone()) {
        crate::cancel_query_by_id(query_id, reason.clone());
    }
}

fn persist_backend(
    state: &Arc<StandaloneState>,
    be_id: BeId,
    endpoint: SocketAddr,
    backend_state: BackendState,
) -> Result<(), String> {
    let Some(provider) = state.metadata_provider.as_ref() else {
        return Ok(());
    };
    let mut txn = provider
        .begin_write("persist backend metadata")
        .map_err(|e| format!("open backend metadata write transaction failed: {e}"))?;
    state
        .backend_repo
        .upsert_backend(
            txn.as_mut(),
            &StoredBackend {
                be_id: i64::from(be_id),
                endpoint: endpoint.to_string(),
                state: backend_state_to_str(backend_state).to_string(),
            },
        )
        .map_err(|e| format!("persist backend metadata failed: {e}"))?;
    txn.commit()
        .map_err(|e| format!("commit backend metadata failed: {e}"))?;
    Ok(())
}

fn delete_backend(state: &Arc<StandaloneState>, endpoint: &SocketAddr) -> Result<(), String> {
    delete_backend_with_provider(state.metadata_provider.as_ref(), endpoint)
}

fn delete_backend_with_provider(
    provider: Option<&Arc<dyn MetaStoreProvider>>,
    endpoint: &SocketAddr,
) -> Result<(), String> {
    let Some(provider) = provider else {
        return Ok(());
    };
    let mut txn = provider
        .begin_write("delete backend metadata")
        .map_err(|e| format!("open backend metadata delete transaction failed: {e}"))?;
    crate::meta::repository::backend::BackendMetaRepository
        .delete_backend(txn.as_mut(), &endpoint.to_string())
        .map_err(|e| format!("delete backend metadata failed: {e}"))?;
    txn.commit()
        .map_err(|e| format!("commit backend metadata delete failed: {e}"))?;
    Ok(())
}

fn require_role(allowed: &[ClusterRole], statement: &str) -> Result<(), String> {
    let role = current_role()?;
    if allowed.contains(&role) {
        return Ok(());
    }
    match role {
        ClusterRole::Be => Err(format!(
            "{statement} is not available in role=be; backend management is owned by StarRocks FE"
        )),
        ClusterRole::AllInOne => Err(format!("{statement} requires role=fe")),
        ClusterRole::Fe => Err(format!("{statement} is not allowed for current role")),
    }
}

fn current_role() -> Result<ClusterRole, String> {
    crate::novarocks_config::config()
        .map(|cfg| cfg.cluster.role)
        .map_err(|e| format!("read config failed: {e}"))
}

fn parse_backend_addr(addr: &str) -> Result<SocketAddr, String> {
    addr.parse::<SocketAddr>()
        .map_err(|e| format!("invalid backend address '{addr}': {e}"))
}

fn backend_state_to_str(state: BackendState) -> &'static str {
    match state {
        BackendState::Registering => "Registering",
        BackendState::Live => "Live",
        BackendState::Lost => "Lost",
        BackendState::Decommissioning => "Decommissioning",
    }
}

fn backend_state_from_str(state: &str) -> Result<BackendState, String> {
    match state {
        "Registering" => Ok(BackendState::Registering),
        "Live" => Ok(BackendState::Live),
        "Lost" => Ok(BackendState::Lost),
        "Decommissioning" => Ok(BackendState::Decommissioning),
        other => Err(format!("invalid persisted backend state '{other}'")),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::runtime::backend_registry::BackendRegistryTestGuard as BackendRegistryReset;

    #[test]
    fn all_in_one_loopback_registry_installs_live_backend_zero() {
        let _guard = crate::engine::acquire_standalone_test_guard();
        let _registry = BackendRegistryReset::new();
        let endpoint: std::net::SocketAddr = "127.0.0.1:19070".parse().unwrap();

        let registry = install_all_in_one_backend_registry(endpoint, 3)
            .expect("install all-in-one loopback backend");
        let live = registry.live_endpoints();
        let snapshot = registry.snapshot();

        assert_eq!(live, vec![(0, endpoint)]);
        assert_eq!(snapshot.len(), 1);
        assert_eq!(snapshot[0].be_id, 0);
        assert_eq!(snapshot[0].endpoint, endpoint);
        assert_eq!(snapshot[0].state, BackendState::Live);
        assert_eq!(snapshot[0].version, env!("CARGO_PKG_VERSION"));
        assert!(snapshot[0].num_cores > 0);
        assert!(snapshot[0].last_heartbeat_ms > 0);
        assert_eq!(
            live_backend_dispatch_entries().expect("dispatch entries"),
            vec![(0usize, endpoint)]
        );
    }

    #[test]
    fn all_in_one_loopback_registry_rejects_mismatched_existing_registry() {
        let _guard = crate::engine::acquire_standalone_test_guard();
        let _registry = BackendRegistryReset::new();
        let existing_endpoint: std::net::SocketAddr = "127.0.0.1:19070".parse().unwrap();
        let requested_endpoint: std::net::SocketAddr = "127.0.0.1:19071".parse().unwrap();
        let registry = Arc::new(BackendRegistry::new(3));
        registry.add_backend_with_state(existing_endpoint, BackendState::Live);
        crate::runtime::backend_registry::replace_backend_registry_for_test(Some(registry));

        let err = match install_all_in_one_backend_registry(requested_endpoint, 3) {
            Ok(_) => panic!("mismatched existing registry must fail"),
            Err(err) => err,
        };

        assert!(
            err.contains("all-in-one loopback backend registry"),
            "{err}"
        );
        assert!(err.contains("127.0.0.1:19071"), "{err}");
    }

    #[test]
    fn wait_for_configured_backends_live_returns_immediately_without_configured_backends() {
        let registry = Arc::new(BackendRegistry::new(3));

        wait_for_configured_backends_live_with(
            &registry,
            &[],
            Duration::from_millis(1),
            Duration::from_millis(1),
            |_be_id, _endpoint| HeartbeatOutcome::failed("should not heartbeat"),
        )
        .expect("empty configured backend list must not wait");
    }

    #[test]
    fn wait_for_configured_backends_live_heartbeats_until_one_backend_is_live() {
        let endpoint: std::net::SocketAddr = "127.0.0.1:19070".parse().unwrap();
        let registry = Arc::new(BackendRegistry::new(3));
        let be_id = registry.add_backend(endpoint);

        wait_for_configured_backends_live_with(
            &registry,
            &[endpoint],
            Duration::from_millis(50),
            Duration::from_millis(1),
            move |actual_be_id, actual_endpoint| {
                assert_eq!(actual_be_id, be_id);
                assert_eq!(actual_endpoint, endpoint);
                HeartbeatOutcome::ok(7, 100)
            },
        )
        .expect("configured backend should become live");

        assert_eq!(registry.live_endpoints(), vec![(be_id, endpoint)]);
    }
}
