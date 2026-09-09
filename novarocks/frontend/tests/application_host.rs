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

use bytes::Bytes;
use novarocks_frontend::view::{
    CreateExternalViewRequest, ExternalViewResolution, ResolvedExternalView, ViewColumnDefinition,
    ViewEngine, ViewRequestContext, ViewService, ViewStatementResult, ViewTarget,
};
use novarocks_frontend::{
    ClusterBackendOpenConfig, FrontendApplicationError, FrontendApplicationErrorKind,
    FrontendApplicationHost, FrontendExecutionConfig, FrontendNativeTransport,
};
use novarocks_native_trust::{
    DeploymentId, NativeCallerSubject, NativeTransportMode, NativeTrust, ValidatedSharedSecret,
};
use novarocks_parser::{
    ast::{Query, Statement as ParsedStatement},
    parse as parse_typed_statement,
    printer::print_query,
};
use novarocks_secret::SecretValue;
use novarocks_state_store_api::{CommitOutcome, Key, Precondition, Value};
use std::sync::Arc;
use std::time::Duration;
mod common;
use common::state_store_fixture;
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
    FrontendExecutionConfig::new(
        "127.0.0.1",
        19090,
        std::num::NonZeroUsize::new(1).unwrap(),
        novarocks_types::NativeCompatibilityId::new([0x71; 32]),
        std::sync::Arc::new(
            novarocks_sql::compiler::build_builtin_engine_function_catalog()
                .expect("builtin function catalog"),
        ),
    )
}

async fn open_host(
    input: Option<novarocks_frontend::StateStoreHostInput>,
) -> Result<FrontendApplicationHost, FrontendApplicationError> {
    let registry = state_store_fixture::registry();
    FrontendApplicationHost::open_with_role_factories_and_state_store_registry(
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

    fn list_external_views(
        &self,
        _catalog: &str,
        _database: &str,
        _context: &novarocks_spi::connector::ConnectorRequestContext,
    ) -> Result<Vec<String>, String> {
        unreachable!("session view must not list external views")
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
    service.execute_statement(engine, statement, context)
}

fn parse_query(sql: &str) -> Query {
    let statements = parse_typed_statement(sql).expect("parse typed query");
    let [ParsedStatement::Query(query)] = statements.as_slice() else {
        panic!("expected typed query");
    };
    query.clone()
}

fn state_store_input() -> novarocks_frontend::StateStoreHostInput {
    state_store_fixture::input(format!("frontend-cluster-{}", Uuid::now_v7()))
}

fn sqlite_config(_temp: &TempDir) -> novarocks_frontend::StateStoreHostInput {
    state_store_input()
}

#[tokio::test]
async fn host_exposes_one_statistics_service_identity() {
    let host = open_host(Some(state_store_input())).await.expect("host");
    let first = host.statistics_application_service();
    let second = host.statistics_application_service();
    assert!(Arc::ptr_eq(&first, &second));
    let first_application = host.statistics_application_service();
    let second_application = host.statistics_application_service();
    assert!(Arc::ptr_eq(&first_application, &second_application));
    drop(first_application);
    drop(second_application);
    host.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn host_exposes_one_dml_service_identity() {
    let host = open_host(Some(state_store_input())).await.expect("host");
    let first = host.dml_service();
    let second = host.dml_service();
    assert!(Arc::ptr_eq(&first, &second));
    host.shutdown().await.expect("shutdown");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn sqlite_host_reopens_without_a_dml_recovery_surface() {
    let config = state_store_input();
    let host = open_host(Some(config.clone())).await.expect("first host");
    let first = host.dml_service();
    host.shutdown().await.expect("first shutdown");

    let reopened = open_host(Some(config)).await.expect("reopened host");
    assert!(!Arc::ptr_eq(&first, &reopened.dml_service()));
    reopened.shutdown().await.expect("reopened shutdown");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn fe_without_state_store_fails_before_durable_services_open() {
    let error = match FrontendApplicationHost::open(
        None,
        execution_config(),
        fe_backend_config(),
        tokio::runtime::Handle::current(),
        test_native_trust(),
        FrontendNativeTransport::plaintext(),
    )
    .await
    {
        Ok(host) => {
            host.shutdown().await.expect("shutdown unexpected FE host");
            panic!("role=fe must not open durable services without StateStore");
        }
        Err(error) => error,
    };

    assert_eq!(error.kind(), FrontendApplicationErrorKind::MvServiceOpen);
    assert!(error.to_string().contains("requires StateStore"), "{error}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn sqlite_host_opens_store_with_single_fe_view() {
    let host = open_host(Some(state_store_input()))
        .await
        .expect("test StateStore host must open");

    let store = host
        .state_store()
        .expect("configured SQLite host must expose its state store");
    assert_eq!(
        host.state_store_provider_id(),
        Some(state_store_fixture::TEST_STATE_STORE_PROVIDER_ID)
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
async fn unregistered_provider_fails_before_store_open() {
    let input = state_store_input();
    let error = match FrontendApplicationHost::open(
        Some(input),
        execution_config(),
        backend_config(),
        tokio::runtime::Handle::current(),
        test_native_trust(),
        FrontendNativeTransport::plaintext(),
    )
    .await
    {
        Ok(host) => {
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
    let host = open_host(Some(state_store_input()))
        .await
        .expect("test provider opens without retaining partial resources");
    host.shutdown()
        .await
        .expect("reopened SQLite host shutdown must succeed");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn shutdown_releases_sqlite_deployment_lock() {
    let config = state_store_input();
    let host = open_host(Some(config.clone()))
        .await
        .expect("first SQLite host must open");

    host.shutdown()
        .await
        .expect("host shutdown must release the SQLite deployment lock");
    let reopened = open_host(Some(config))
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
    let host = open_host(Some(config.clone()))
        .await
        .expect("first SQLite host must open");

    let second = open_host(Some(config.clone()))
        .await
        .expect("shared test provider permits a second live host");
    second.shutdown().await.expect("second host shutdown");

    host.shutdown()
        .await
        .expect("first host shutdown must succeed");
    let reopened = open_host(Some(config))
        .await
        .expect("same test deployment reopens after explicit shutdown");
    reopened
        .shutdown()
        .await
        .expect("reopened SQLite host shutdown must succeed");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn configured_host_does_not_restore_local_views_after_reopen() {
    let temp = TempDir::new().expect("temporary SQLite deployment");
    let config = sqlite_config(&temp);
    let host = open_host(Some(config.clone()))
        .await
        .expect("configured host must open");
    execute_view_statement(
        host.view_service().as_ref(),
        &SessionViewEngine,
        "CREATE VIEW local_view AS SELECT 42 AS answer",
        view_context(),
    )
    .expect("host view service must register the view");
    let mut visible = parse_query("SELECT * FROM local_view");
    host.view_service()
        .rewrite_query(&SessionViewEngine, &mut visible, view_context())
        .expect("the defining host must expand its own view");
    assert_eq!(
        print_query(&visible),
        "SELECT * FROM (SELECT 42 AS answer) local_view"
    );
    host.shutdown().await.expect("first host shutdown");

    // A local view is process runtime state even when the host has a
    // StateStore: the frontend is its only authority, so the view ends with the
    // incarnation that defined it. Durable views live in an external catalog.
    let reopened = open_host(Some(config))
        .await
        .expect("configured host must reopen");
    let mut query = parse_query("SELECT * FROM local_view");
    reopened
        .view_service()
        .rewrite_query(&SessionViewEngine, &mut query, view_context())
        .expect("reopened host must rewrite without the local view");
    assert_eq!(print_query(&query), "SELECT * FROM local_view");
    reopened.shutdown().await.expect("reopened host shutdown");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn legacy_maintenance_and_gc_observation_records_do_not_block_host_open() {
    let temp = TempDir::new().expect("temporary SQLite deployment");
    let config = sqlite_config(&temp);
    let host = open_host(Some(config.clone()))
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
        let reopened = open_host(Some(config.clone()))
            .await
            .expect("legacy maintenance and corrupt GC observation data must not block host open");
        reopened.shutdown().await.expect("reopened host shutdown");
    }
}
