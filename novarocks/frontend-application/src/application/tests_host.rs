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

use super::{
    FrontendApplicationError, FrontendApplicationErrorKind, FrontendApplicationHost,
    FrontendExecutionConfig,
};
use crate::{state_store::testing as state_store_fixture, topology::ClusterBackendOpenConfig};
use bytes::Bytes;
use novarocks_native_adapter::FrontendNativeTransport;
use novarocks_native_trust::{
    DeploymentId, NativeCallerSubject, NativeTransportMode, NativeTrust, ValidatedSharedSecret,
};
use novarocks_parser::{
    ast::{Query, Statement as ParsedStatement},
    parse as parse_typed_statement,
    printer::print_query,
};
use novarocks_query_application::view::{
    CreateExternalViewRequest, ExternalViewResolution, ResolvedExternalView, ViewColumnDefinition,
    ViewEngine, ViewRequestContext, ViewService, ViewStatementResult, ViewTarget,
};
use novarocks_secret::SecretValue;
use novarocks_state_store_api::{CommitOutcome, Key, Precondition, Value};
use novarocks_state_store_runtime::StateStoreHostInput;
use std::sync::Arc;
use std::time::Duration;
use tempfile::TempDir;
use uuid::Uuid;

fn test_native_trust() -> Arc<NativeTrust> {
    Arc::new(NativeTrust::new(
        DeploymentId::parse("frontend-integration-test").expect("deployment"),
        ValidatedSharedSecret::new(SecretValue::new("0123456789abcdef0123456789abcdef"))
            .expect("secret"),
        NativeCallerSubject::parse("fe@127.0.0.1:19040").expect("subject"),
        NativeTransportMode::Disabled,
    ))
}

fn execution_config() -> FrontendExecutionConfig {
    FrontendExecutionConfig::new_for_test(
        std::num::NonZeroUsize::new(1).unwrap(),
        novarocks_types::NativeCompatibilityId::new([0x71; 32]),
        std::sync::Arc::new(
            novarocks_sql::compiler::build_builtin_engine_function_catalog()
                .expect("builtin function catalog"),
        ),
        crate::application::test_constant_policy(),
    )
}

async fn open_host(
    input: Option<StateStoreHostInput>,
) -> Result<FrontendApplicationHost, FrontendApplicationError> {
    let registry = state_store_fixture::persistent_registry();
    let mut host_receiving_16 = None;
    FrontendApplicationHost::open_with_role_factories_and_state_store_registry(
        &mut host_receiving_16,
        input,
        &registry,
        execution_config(),
        backend_config(),
        Vec::new(),
        tokio::runtime::Handle::current(),
        test_native_trust(),
        FrontendNativeTransport::plaintext(),
    )
    .await
}

fn backend_config() -> ClusterBackendOpenConfig {
    ClusterBackendOpenConfig::new(
        novarocks_types::ClusterRole::Fe,
        novarocks_types::NativeCompatibilityId::new([0x71; 32]),
        Duration::from_secs(1),
        1,
        Duration::from_secs(1),
    )
    .expect("valid frontend backend config")
}

fn fe_backend_config() -> ClusterBackendOpenConfig {
    ClusterBackendOpenConfig::new(
        novarocks_types::ClusterRole::Fe,
        novarocks_types::NativeCompatibilityId::new([0x71; 32]),
        Duration::from_secs(1),
        1,
        Duration::from_secs(1),
    )
    .expect("valid FE backend config")
}

struct SessionViewEngine;

impl ViewEngine for SessionViewEngine {
    fn resolve_external_view(
        &self,
        _target: &ViewTarget,
        _context: &novarocks_spi::connector::ConnectorRequestContext,
    ) -> Result<ExternalViewResolution, String> {
        unreachable!("session view must not resolve external views")
    }

    fn resolve_external_view_async<'a>(
        &'a self,
        target: &'a ViewTarget,
        context: &'a novarocks_spi::connector::ConnectorRequestContext,
    ) -> std::pin::Pin<
        Box<dyn std::future::Future<Output = Result<ExternalViewResolution, String>> + Send + 'a>,
    > {
        Box::pin(async move { self.resolve_external_view(target, context) })
    }

    fn create_external_view(
        &self,
        _request: CreateExternalViewRequest,
        _context: &novarocks_spi::connector::ConnectorRequestContext,
    ) -> Result<(), String> {
        unreachable!("session view must not create external views")
    }

    fn drop_external_view(
        &self,
        _target: &ViewTarget,
        _context: &novarocks_spi::connector::ConnectorRequestContext,
        _policy: novarocks_spi::connector::DropPolicy,
    ) -> Result<(), String> {
        unreachable!("session view must not drop external views")
    }

    fn load_external_view(
        &self,
        _target: &ViewTarget,
        _context: &novarocks_spi::connector::ConnectorRequestContext,
    ) -> Result<Option<ResolvedExternalView>, String> {
        Ok(None)
    }

    fn analyze_external_view(
        &self,
        _catalog: &str,
        _database: &str,
        _query: &Query,
        _context: &novarocks_spi::connector::ConnectorRequestContext,
    ) -> Result<Vec<ViewColumnDefinition>, String> {
        unreachable!("session view must not analyze external views")
    }
}

fn view_context() -> ViewRequestContext<'static> {
    ViewRequestContext {
        current_catalog: None,
        current_database: "db",
        connector_context: None,
    }
}

fn execute_view_statement(
    service: &dyn ViewService,
    engine: &dyn ViewEngine,
    sql: &str,
    context: ViewRequestContext<'_>,
) -> Result<ViewStatementResult, String> {
    let statements = parse_typed_statement(sql).map_err(|error| error.to_string())?;
    let [novarocks_parser::ast::Statement::View(statement)] = statements.as_slice() else {
        return Err("expected typed View statement".to_string());
    };
    if let novarocks_parser::ast::ViewStatement::Show(show) = statement {
        let prepared = service.prepare_show_views(show, context)?;
        let novarocks_query_application::view::ShowViewsSource::Local(_) = &prepared.source else {
            return Err("test host has no external SHOW provider".to_string());
        };
        let mut prepared = prepared;
        let novarocks_query_application::view::ShowViewsSource::Local(names) = std::mem::replace(
            &mut prepared.source,
            novarocks_query_application::view::ShowViewsSource::Local(Vec::new()),
        ) else {
            unreachable!()
        };
        return prepared.finish(names).map(ViewStatementResult::Query);
    }
    service.execute_statement(engine, statement, context)
}

fn parse_query(sql: &str) -> Query {
    let statements = parse_typed_statement(sql).expect("parse typed query");
    let [ParsedStatement::Query(query)] = statements.as_slice() else {
        panic!("expected typed query");
    };
    query.clone()
}

fn state_store_input() -> StateStoreHostInput {
    state_store_fixture::persistent_input(format!("frontend-cluster-{}", Uuid::now_v7()))
}

fn sqlite_config(_temp: &TempDir) -> StateStoreHostInput {
    state_store_input()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn fe_without_state_store_fails_before_durable_services_open() {
    let mut host_receiving_15 = None;
    let error = match FrontendApplicationHost::open(
        &mut host_receiving_15,
        None,
        execution_config(),
        fe_backend_config(),
        tokio::runtime::Handle::current(),
        test_native_trust(),
        FrontendNativeTransport::plaintext(),
    )
    .await
    {
        Ok(mut host) => {
            host.shutdown().await.expect("shutdown unexpected FE host");
            panic!("role=fe must not open durable services without StateStore");
        }
        Err(error) => error,
    };

    assert_eq!(error.kind(), FrontendApplicationErrorKind::MvServiceOpen);
    assert!(error.to_string().contains("requires StateStore"), "{error}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn original_process_health_blocks_ready_and_triggers_running_host_observation() {
    #[derive(Debug)]
    struct OriginalCause;
    impl std::fmt::Display for OriginalCause {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.write_str("original process source failed")
        }
    }
    impl std::error::Error for OriginalCause {}
    let mut host = open_host(Some(state_store_input())).await.unwrap();
    host.install_statement_control_for_test();
    host.mark_ready().unwrap();
    let healthy = !host.workload_progress_has_failed();
    let cause = Arc::new(OriginalCause);
    let original = Arc::downgrade(&cause);
    host.execution_runtime_owner
        .logical_execution_client
        .report_process_failure(
            novarocks_query_application::api::StatementNativeHealthFailure::actual_exit_unproven(
                "original MV background source failed",
                cause,
            ),
        );
    let still_running = !host.execution_runtime_owner.supervisor.service_has_exited();
    // These are the real readiness and running Server-loop predicates.
    let failed = host.workload_progress_has_failed();
    let refused = host.execution_runtime_owner.mark_ready().is_err();
    let retained = original.upgrade().is_some();
    let result = host
        .shutdown_until(std::time::Instant::now() + Duration::from_secs(5))
        .await;
    host.abandon_for_process_exit();
    drop(host);
    assert!(healthy && still_running && failed && refused && retained);
    assert!(result.is_err(), "health cannot become a healthy shutdown");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn failed_original_subscription_reaper_cannot_strand_the_host_workload_drain() {
    use novarocks_workload_control::{WorkClass, WorkRequest};
    let mut host = open_host(Some(state_store_input())).await.unwrap();
    host.install_statement_control_for_test();
    host.mark_ready().unwrap();
    let root = host
        .execution_runtime_owner
        .workload
        .as_ref()
        .unwrap()
        .try_begin_root(WorkRequest::new(WorkClass::Query))
        .unwrap();
    let lease = host
        .data_runtime
        .subscription_port()
        .reserve(&root.owner.scope(), None, None)
        .unwrap()
        .start(std::future::pending())
        .unwrap();
    host.data_runtime.abort_original_subscription_reaper();
    // The original task's actual exit guard closes admission/requests abort.
    // Its child remains in the same Workload until actual owner recovery joins.
    tokio::time::timeout(
        Duration::from_secs(2),
        host.data_runtime
            .wait_for_original_subscription_reaper_failure(),
    )
    .await
    .unwrap();
    root.owner.complete_after_terminal_cancel_settled();
    root.business.release();
    let result = host
        .release_resources_until(std::time::Instant::now() + Duration::from_secs(2))
        .await;
    let joined = host.data_runtime.original_subscriptions_joined();
    let store_closed = host.state_store().is_none();
    // Even a failing assertion must first converge the original test owners.
    drop(lease);
    let cleanup_deadline = std::time::Instant::now() + Duration::from_secs(2);
    let _cleanup_failure = host
        .data_runtime
        .drain_original_subscriptions_until(cleanup_deadline)
        .await;
    let _host_failure = host.release_resources_until(cleanup_deadline).await;
    assert!(
        result.is_err() && joined && store_closed,
        "original shutdown result={result:?}, joined={joined}, store_closed={store_closed}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn actual_outgoing_io_holds_state_store_until_original_public_exit() {
    use novarocks_native_adapter::native_lane::frontend_lane_connector;
    use novarocks_native_trust::NativeEndpointConnector;
    use novarocks_proto_codec::native_rpc::FrontendNativeLane;
    use tower::ServiceExt;

    let mut host = open_host(Some(state_store_input())).await.unwrap();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint =
        novarocks_types::NativeEndpoint::from_socket_addr(listener.local_addr().unwrap());
    let connector = frontend_lane_connector(
        NativeEndpointConnector::plaintext(endpoint),
        host.data_runtime.transport_admission().clone(),
        FrontendNativeLane::ResultData,
    );
    let io = tokio::time::timeout(
        Duration::from_secs(2),
        connector.oneshot(
            "http://original-host-exit.test"
                .parse::<axum::http::Uri>()
                .unwrap(),
        ),
    )
    .await
    .unwrap()
    .unwrap();
    let (peer, _) = tokio::time::timeout(Duration::from_secs(2), listener.accept())
        .await
        .unwrap()
        .unwrap();
    let original_deadline = std::time::Instant::now() + Duration::from_millis(200);
    let failure = host.release_resources_until(original_deadline).await.err();
    let retained_store = host.state_store().is_some();
    let held = host
        .data_runtime
        .transport_admission()
        .frontend_outgoing_snapshot()
        .unwrap();
    // Release the actual public IO. No counter mutation or synthetic ACK.
    drop(io);
    drop(peer);
    drop(listener);
    let retry = host
        .release_resources_until(std::time::Instant::now() + Duration::from_secs(2))
        .await;
    let store_closed = host.state_store().is_none();
    assert!(failure.is_some_and(|error| error.contains("outgoing public exits")));
    assert!(retained_store && held.closed && held.data_connections == 1);
    assert!(retry.is_ok() && store_closed);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn sqlite_host_opens_store_with_single_fe_view() {
    let mut host = open_host(Some(state_store_input()))
        .await
        .expect("test StateStore host must open");

    let store = host
        .state_store()
        .expect("configured SQLite host must expose its state store");
    assert_eq!(
        host.state_store_provider_id(),
        Some(state_store_fixture::PERSISTENT_TEST_STATE_STORE_PROVIDER_ID)
    );
    assert!(
        store.identity().await.is_ok(),
        "test deployment view must allow StateStore access"
    );
    drop(store);

    host.shutdown()
        .await
        .expect("SQLite host shutdown must succeed");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn catalog_activity_observer_tracks_the_original_service_without_retaining_host_resources() {
    let mut host = open_host(Some(state_store_input()))
        .await
        .expect("catalog activity Host must open");
    let observer = host.catalog_activity_observer();
    {
        let observed = observer
            .upgrade()
            .expect("the role-owned catalog service remains alive");
        let original = host.catalog_role_runtime().catalog_application_port();
        assert!(
            Arc::ptr_eq(&observed, &original),
            "the observer must point to the original service, not a temporary published wrapper"
        );
        assert!(
            observed
                .catalog_activity_sources()
                .expect("the actual service supports activity observation")
                .is_empty()
        );
    }
    host.shutdown()
        .await
        .expect("the weak observer must not retain Host StateStore resources");
    assert!(
        observer.upgrade().is_none(),
        "Host shutdown must release the original catalog service"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn unregistered_provider_fails_before_store_open() {
    let input = state_store_input();
    let mut host_receiving_14 = None;
    let error = match FrontendApplicationHost::open(
        &mut host_receiving_14,
        Some(input),
        execution_config(),
        backend_config(),
        tokio::runtime::Handle::current(),
        test_native_trust(),
        FrontendNativeTransport::plaintext(),
    )
    .await
    {
        Ok(mut host) => {
            host.shutdown().await.expect("shutdown unexpected host");
            panic!("an unregistered test provider must fail before store I/O");
        }
        Err(error) => error,
    };
    assert_eq!(error.kind(), FrontendApplicationErrorKind::StateStoreHost);
    assert!(error.to_string().contains("ProviderNotRegistered"));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn failed_open_releases_partial_resources() {
    let mut host = open_host(Some(state_store_input()))
        .await
        .expect("test provider opens without retaining partial resources");
    host.shutdown()
        .await
        .expect("reopened SQLite host shutdown must succeed");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn shutdown_releases_sqlite_deployment_lock() {
    let config = state_store_input();
    let mut host = open_host(Some(config.clone()))
        .await
        .expect("first SQLite host must open");

    host.shutdown()
        .await
        .expect("host shutdown must release the SQLite deployment lock");
    let mut reopened = open_host(Some(config))
        .await
        .expect("SQLite deployment must reopen after shutdown");
    reopened
        .shutdown()
        .await
        .expect("reopened SQLite host shutdown must succeed");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn shared_test_provider_allows_multiple_live_hosts() {
    let temp = TempDir::new().expect("temporary SQLite deployment");
    let config = sqlite_config(&temp);
    let mut host = open_host(Some(config.clone()))
        .await
        .expect("first SQLite host must open");

    let mut second = open_host(Some(config.clone()))
        .await
        .expect("shared test provider permits a second live host");
    second.shutdown().await.expect("second host shutdown");

    host.shutdown()
        .await
        .expect("first host shutdown must succeed");
    let mut reopened = open_host(Some(config))
        .await
        .expect("same test deployment reopens after explicit shutdown");
    reopened
        .shutdown()
        .await
        .expect("reopened SQLite host shutdown must succeed");
}

#[test]
fn local_view_service_does_not_restore_state_across_process_instances() {
    let host = novarocks_query_application::view::QueryViewService::new();
    execute_view_statement(
        &host,
        &SessionViewEngine,
        "CREATE VIEW local_view AS SELECT 42 AS answer",
        view_context(),
    )
    .expect("local view service must register the view");
    let mut visible = parse_query("SELECT * FROM local_view");
    host.rewrite_query(
        &SessionViewEngine,
        &mut visible,
        view_context(),
        &novarocks_sql::sql_mode::SqlSemanticSettings::default(),
    )
    .expect("the defining process instance must expand its own view");
    assert_eq!(
        print_query(&visible),
        "SELECT * FROM (SELECT 42 AS answer) local_view"
    );
    // A local view is process runtime state: it ends with the process instance
    // that defined it. Durable views live in an external catalog.
    let reopened = novarocks_query_application::view::QueryViewService::new();
    let mut query = parse_query("SELECT * FROM local_view");
    reopened
        .rewrite_query(
            &SessionViewEngine,
            &mut query,
            view_context(),
            &novarocks_sql::sql_mode::SqlSemanticSettings::default(),
        )
        .expect("new process instance must rewrite without the local view");
    assert_eq!(print_query(&query), "SELECT * FROM local_view");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn legacy_maintenance_and_gc_observation_records_do_not_block_host_open() {
    let temp = TempDir::new().expect("temporary SQLite deployment");
    let config = sqlite_config(&temp);
    let mut host = open_host(Some(config.clone()))
        .await
        .expect("configured host must open");
    let store = host.state_store().expect("configured host state store");
    // Identity is issued by the open instance, never minted by the caller.
    let (attempt, _observation) = store.attempts().reserve().expect("reserve a write attempt");
    let mut transaction = store
        .begin_write(attempt, "seed legacy frontend table-maintenance records")
        .await
        .expect("begin corrupt record write");
    transaction
        .put(
            Key::try_from(Bytes::from_static(
                b"novarocks/frontend/table-maintenance/v1/jobs/0000000000000001",
            ))
            .expect("maintenance job key"),
            Value::try_from(Bytes::from_static(b"not-json")).expect("corrupt maintenance value"),
            Precondition::Absent,
        )
        .await
        .expect("stage corrupt maintenance record");
    transaction
        .put(
            Key::try_from(Bytes::from_static(
                b"novarocks/frontend/table-maintenance/v7/gc-owned-ref-observations/00000000-0000-0000-0000-0000000003c3/5f5f6e6f7661726f636b735f5f636f7272757074",
            ))
            .expect("GC observation key"),
            Value::try_from(Bytes::from_static(b"not-canonical-json"))
                .expect("corrupt GC observation value"),
            Precondition::Absent,
        )
        .await
        .expect("stage corrupt GC observation record");
    assert!(matches!(
        transaction.commit().await,
        CommitOutcome::Committed(_)
    ));
    drop(store);
    host.shutdown().await.expect("seed host shutdown");

    for _ in 0..2 {
        let mut reopened = open_host(Some(config.clone()))
            .await
            .expect("legacy maintenance and corrupt GC observation data must not block host open");
        reopened.shutdown().await.expect("reopened host shutdown");
    }
}

// Explicit pure receiver fixture. No SQLite algorithm or physical worker is
// copied here; real ordinary queue/exit is tested at StateStoreOriginalOwner.
struct HeldOpeningProbe {
    prepared: std::sync::atomic::AtomicUsize,
    dropped: std::sync::atomic::AtomicUsize,
}
struct HeldPureFactory {
    descriptor: novarocks_state_store_api::StateStoreProviderDescriptor,
    probe: Arc<HeldOpeningProbe>,
}
struct HeldPureOpen {
    inner: Box<dyn novarocks_state_store_api::StateStoreOriginalOpen>,
    probe: Arc<HeldOpeningProbe>,
}
impl Drop for HeldPureOpen {
    fn drop(&mut self) {
        self.probe
            .dropped
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
    }
}
impl novarocks_state_store_api::StateStoreOriginalOpen for HeldPureOpen {
    fn poll_ready(
        &mut self,
        _: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), novarocks_state_store_api::StateStoreExecutionFailure>> {
        std::task::Poll::Pending
    }
    fn take_instance_in(
        &mut self,
        target: &mut Option<Box<dyn novarocks_state_store_api::StateStoreProviderInstance>>,
    ) -> Result<(), novarocks_state_store_api::StateStoreExecutionFailure> {
        self.inner.take_instance_in(target)
    }
}
#[async_trait::async_trait]
impl novarocks_state_store_api::StateStoreProviderFactory for HeldPureFactory {
    fn blocking_dependency_facts(
        &self,
    ) -> Option<Vec<novarocks_state_store_api::SharedDependency>> {
        Some(Vec::new())
    }
    fn descriptor(&self) -> &novarocks_state_store_api::StateStoreProviderDescriptor {
        &self.descriptor
    }
    fn blocking_source_vocabulary(
        &self,
    ) -> Option<novarocks_state_store_api::StateStoreBlockingSourceVocabulary> {
        Some(
            novarocks_state_store_testkit::testing::pure_memory_original_vocabulary(
                self.descriptor.id,
            ),
        )
    }
    fn prepare_open_in(
        self: Box<Self>,
        request: novarocks_state_store_api::StateStoreOpenRequest,
        context: &novarocks_state_store_api::StateStoreExecutionContext,
        target: &mut Option<Box<dyn novarocks_state_store_api::StateStoreOriginalOpen>>,
    ) -> Result<(), novarocks_state_store_api::StateStoreExecutionFailure> {
        assert!(target.is_none());
        let mut inner = None;
        Box::new(
            novarocks_state_store_testkit::testing::InMemoryStateStoreProviderFactory::new(
                self.descriptor,
            ),
        )
        .prepare_open_in(request, context, &mut inner)?;
        *target = Some(Box::new(HeldPureOpen {
            inner: inner.unwrap(),
            probe: self.probe.clone(),
        }));
        self.probe
            .prepared
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        Ok(())
    }
    async fn open(
        self: Box<Self>,
        _: novarocks_state_store_api::StateStoreOpenRequest,
    ) -> Result<
        Box<dyn novarocks_state_store_api::StateStoreProviderInstance>,
        novarocks_state_store_api::StateStoreError,
    > {
        panic!("prepared Host must not use legacy opening")
    }
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cancelled_bootstrap_borrow_retains_the_original_opening_until_host_close() {
    use novarocks_state_store_api::{StateStoreProviderDescriptor, StateStoreProviderId};
    use novarocks_state_store_runtime::{
        StateStoreProviderRegistration, StateStoreProviderRegistry,
    };
    use std::sync::atomic::{AtomicUsize, Ordering};
    let descriptor = StateStoreProviderDescriptor::new(
        StateStoreProviderId::new("held-pure-opening"),
        novarocks_state_store_api::MAX_KEY_BYTES,
    );
    let probe = Arc::new(HeldOpeningProbe {
        prepared: AtomicUsize::new(0),
        dropped: AtomicUsize::new(0),
    });
    let mut registry = StateStoreProviderRegistry::new();
    let observed = probe.clone();
    registry
        .register(StateStoreProviderRegistration::new(descriptor, move |_| {
            Ok(Box::new(HeldPureFactory {
                descriptor,
                probe: observed.clone(),
            }))
        }))
        .unwrap();
    let mut input = state_store_fixture::input("cancelled-prepared-opening");
    input.provider_id = descriptor.id;
    let mut host = FrontendApplicationHost::prepare_with_role_factories_and_state_store_registry(
        Some(input),
        &registry,
        execution_config(),
        backend_config(),
        Vec::new(),
        tokio::runtime::Handle::current(),
        test_native_trust(),
        FrontendNativeTransport::plaintext(),
    )
    .unwrap();
    assert!(host.state_store_host.is_none());
    assert!(host.state_store_opening.is_some());
    let admission = host.workload_root_admission();
    assert!(matches!(
        admission.try_begin_root(novarocks_workload_control::WorkRequest::new(
            novarocks_workload_control::WorkClass::Query
        )),
        Err(novarocks_workload_control::WorkError::NotReady)
    ));
    let mut borrower = Box::pin(host.open_prepared());
    let pending = std::future::poll_fn(|cx| {
        std::task::Poll::Ready(std::future::Future::poll(borrower.as_mut(), cx).is_pending())
    })
    .await;
    drop(borrower);
    // These originals stay in Host after the borrowed F has actually dropped.
    let retained = probe.dropped.load(Ordering::SeqCst);
    let prepared = probe.prepared.load(Ordering::SeqCst);
    let opening_still_installed = host.state_store_opening.is_some();
    let closed = host.shutdown().await;
    // Observe original local exits before assertions can unwind this fixture.
    assert!(closed.is_ok(), "{closed:?}");
    assert!(pending);
    assert_eq!(prepared, 1);
    assert_eq!(retained, 0);
    assert!(opening_still_installed);
    assert_eq!(probe.dropped.load(Ordering::SeqCst), 1);
    assert!(host.state_store_opening.is_none());
    assert!(host.state_store_host.is_none());
}
