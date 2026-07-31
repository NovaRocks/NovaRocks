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
use novarocks::engine::table_maintenance::{
    MaintenanceActionOutcome, MaintenanceActionRequest, MaintenanceRequestContext,
    MaintenanceTarget, TableMaintenanceEngine,
};
use novarocks::engine::view::{
    CreateExternalViewRequest, ResolvedExternalView, ViewColumnDefinition, ViewEngine,
    ViewRequestContext, ViewSqlDialect, ViewTarget,
};
use novarocks_frontend::dml::{DmlErrorKind, DmlOperationId};
use novarocks_frontend::view::repository::database_key;
use novarocks_frontend::{
    ClusterBackendOpenConfig, FrontendApplicationError, FrontendApplicationErrorKind,
    FrontendApplicationHost, FrontendExecutionConfig,
};
use novarocks_spi::state_store::{CommitOutcome, Key, Precondition, TransactionId, Value};
use novarocks_state_store::{
    SQLITE_STATE_STORE_PROVIDER_ID, StateStoreAppConfig, StateStoreConfig, StateStoreHostConfig,
    StateStoreLimitOverrides, StateStoreProviderConfig,
};
use sqlparser::ast::{Query, Statement};
use sqlparser::parser::Parser;
use std::sync::Arc;
use std::time::Duration;
use tempfile::TempDir;
use uuid::Uuid;

fn execution_config() -> FrontendExecutionConfig {
    FrontendExecutionConfig::new("127.0.0.1", 19090, std::num::NonZeroUsize::new(1).unwrap())
}

async fn open_host(
    config: Option<StateStoreHostConfig>,
) -> Result<FrontendApplicationHost, FrontendApplicationError> {
    FrontendApplicationHost::open(config, execution_config(), backend_config()).await
}

fn backend_config() -> ClusterBackendOpenConfig {
    ClusterBackendOpenConfig::new(
        novarocks::common::app_config::ClusterRole::AllInOne,
        Vec::new(),
        Duration::from_secs(1),
        1,
        Duration::from_secs(1),
    )
    .expect("valid all-in-one backend config")
}

fn fe_backend_config() -> ClusterBackendOpenConfig {
    ClusterBackendOpenConfig::new(
        novarocks::common::app_config::ClusterRole::Fe,
        Vec::new(),
        Duration::from_secs(1),
        1,
        Duration::from_secs(1),
    )
    .expect("valid FE backend config")
}

struct SessionViewEngine;

struct SessionMaintenanceEngine;

impl TableMaintenanceEngine for SessionMaintenanceEngine {
    fn resolve_target(
        &self,
        _name_parts: &[String],
        _context: MaintenanceRequestContext<'_>,
    ) -> Result<MaintenanceTarget, String> {
        unreachable!("ordinary SQL must not resolve a maintenance target")
    }

    fn reject_user_action_on_mv(&self, _target: &MaintenanceTarget) -> Result<(), String> {
        unreachable!("ordinary SQL must not run a maintenance MV guard")
    }

    fn current_snapshot_id(&self, _target: &MaintenanceTarget) -> Result<i64, String> {
        unreachable!("ordinary SQL must not read a maintenance snapshot")
    }

    fn execute_action(
        &self,
        _request: MaintenanceActionRequest,
    ) -> Result<MaintenanceActionOutcome, String> {
        unreachable!("ordinary SQL must not execute a maintenance action")
    }
}

impl ViewEngine for SessionViewEngine {
    fn validate_iceberg_catalog(&self, _catalog: &str) -> Result<(), String> {
        unreachable!("session view must not access an external catalog")
    }

    fn is_rest_iceberg_catalog(&self, _catalog: &str) -> bool {
        false
    }

    fn table_exists(
        &self,
        _target: &ViewTarget,
        _context: &novarocks_spi::connector::ConnectorRequestContext,
    ) -> Result<bool, String> {
        unreachable!("session view must not probe external tables")
    }

    fn view_exists(
        &self,
        _target: &ViewTarget,
        _context: &novarocks_spi::connector::ConnectorRequestContext,
    ) -> Result<bool, String> {
        unreachable!("session view must not probe external views")
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
    ) -> Result<Option<ResolvedExternalView>, String> {
        Ok(None)
    }

    fn list_external_views(&self, _catalog: &str, _database: &str) -> Result<Vec<String>, String> {
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

fn parse_query(sql: &str) -> Box<Query> {
    let mut parser = Parser::new(&ViewSqlDialect).try_with_sql(sql).unwrap();
    match parser.parse_statement().unwrap() {
        Statement::Query(query) => query,
        other => panic!("expected query, got {other:?}"),
    }
}

fn sqlite_config(temp: &TempDir) -> StateStoreHostConfig {
    StateStoreHostConfig {
        state_store: StateStoreAppConfig {
            store: StateStoreConfig {
                cluster_id: "frontend-cluster".to_owned(),
                limits: StateStoreLimitOverrides::default(),
                provider: StateStoreProviderConfig::Sqlite {
                    path: temp.path().join("state-store.sqlite"),
                    deployment_owner: "frontend-fe".to_owned(),
                },
            },
            mysql_client: None,
        },
        foundationdb_client: None,
    }
}

#[tokio::test]
async fn host_exposes_one_statistics_service_identity() {
    let host = open_host(None).await.expect("host");
    let first = host.statistics_service();
    let second = host.statistics_service();
    assert!(Arc::ptr_eq(&first, &second));
    host.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn host_exposes_one_dml_service_identity() {
    let host = open_host(None).await.expect("host");
    let first = host.dml_service();
    let second = host.dml_service();
    assert!(Arc::ptr_eq(&first, &second));
    host.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn absent_state_store_builds_dml_service_with_disabled_journal() {
    let host = open_host(None).await.expect("host");
    let error = host
        .dml_service()
        .load_operation(DmlOperationId::new_v7())
        .expect_err("disabled DML journal must reject operation access");
    assert_eq!(error.kind(), DmlErrorKind::JournalUnavailable);
    assert!(
        error
            .to_string()
            .contains("state store is required for Iceberg INSERT"),
        "{error}"
    );
    host.shutdown().await.expect("shutdown");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn sqlite_host_reopens_dml_journal_after_shutdown() {
    let temp = TempDir::new().expect("temporary SQLite deployment");
    let config = sqlite_config(&temp);
    let host = open_host(Some(config.clone())).await.expect("first host");
    assert!(
        host.dml_service()
            .list_unfinished_operations()
            .expect("first DML journal")
            .is_empty()
    );
    host.shutdown().await.expect("first shutdown");

    let reopened = open_host(Some(config)).await.expect("reopened host");
    assert!(
        reopened
            .dml_service()
            .list_unfinished_operations()
            .expect("reopened DML journal")
            .is_empty()
    );
    reopened.shutdown().await.expect("reopened shutdown");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn absent_config_opens_disabled_host() {
    let host = open_host(None)
        .await
        .expect("absent state store configuration must open a disabled host");

    assert!(host.state_store().is_none());
    assert_eq!(host.state_store_provider_id(), None);
    assert!(
        host.table_maintenance_service()
            .try_handle_statement(
                &SessionMaintenanceEngine,
                "SELECT 1",
                MaintenanceRequestContext {
                    current_catalog: None,
                    current_database: "db",
                },
            )
            .expect("ordinary SQL must pass through the maintenance service")
            .is_none()
    );
    let _query_execution = host.query_execution_service();
    let _report_handler = host.coordinator_report_handler();
    let _backend_activity = host.backend_query_activity();
    let _backend_event_sink = host.backend_query_event_sink();
    assert!(
        host.view_service()
            .try_handle_statement(
                &SessionViewEngine,
                "CREATE VIEW memory_view AS SELECT 1",
                view_context(),
            )
            .is_ok()
    );
    host.shutdown()
        .await
        .expect("disabled host shutdown must succeed");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn fe_without_state_store_fails_before_frontend_services_open() {
    let error =
        match FrontendApplicationHost::open(None, execution_config(), fe_backend_config()).await {
            Ok(host) => {
                host.shutdown().await.expect("shutdown unexpected FE host");
                panic!("role=fe must not open without StateStore membership authority");
            }
            Err(error) => error,
        };

    assert_eq!(
        error.kind(),
        FrontendApplicationErrorKind::ClusterBackendOpen
    );
    assert!(error.to_string().contains("requires StateStore"), "{error}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn sqlite_host_opens_store_with_single_fe_view() {
    let temp = TempDir::new().expect("temporary SQLite deployment");
    let host = open_host(Some(sqlite_config(&temp)))
        .await
        .expect("SQLite host must open its state store");

    let store = host
        .state_store()
        .expect("configured SQLite host must expose its state store");
    assert_eq!(
        host.state_store_provider_id(),
        Some(SQLITE_STATE_STORE_PROVIDER_ID)
    );
    assert!(
        store.identity().await.is_ok(),
        "single-FE deployment view must allow SQLite store access"
    );
    drop(store);

    host.shutdown()
        .await
        .expect("SQLite host shutdown must succeed");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn unsupported_provider_fails_before_store_open() {
    let mysql_config = StateStoreHostConfig {
        state_store: StateStoreAppConfig {
            store: StateStoreConfig {
                cluster_id: "frontend-cluster".to_owned(),
                limits: StateStoreLimitOverrides::default(),
                provider: StateStoreProviderConfig::Mysql {
                    database: "frontend_control_plane".to_owned(),
                },
            },
            mysql_client: None,
        },
        foundationdb_client: None,
    };
    let foundationdb_config = StateStoreHostConfig {
        state_store: StateStoreAppConfig {
            store: StateStoreConfig {
                cluster_id: "frontend-cluster".to_owned(),
                limits: StateStoreLimitOverrides::default(),
                provider: StateStoreProviderConfig::Foundationdb {
                    cluster_file: "/definitely/not/an/fdb/cluster-file".into(),
                    keyspace_id: Uuid::nil(),
                },
            },
            mysql_client: None,
        },
        foundationdb_client: None,
    };

    for config in [mysql_config, foundationdb_config] {
        let error = match open_host(Some(config)).await {
            Ok(_) => panic!("deferred provider must be rejected before runtime or store I/O"),
            Err(error) => error,
        };

        assert_eq!(error.kind(), FrontendApplicationErrorKind::DeploymentSource);
        assert!(error.to_string().contains("UnsupportedProvider"));
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn failed_open_releases_partial_resources() {
    let temp = TempDir::new().expect("temporary SQLite deployment");
    let non_directory_parent = temp.path().join("not-a-directory");
    std::fs::write(&non_directory_parent, b"not a directory")
        .expect("create regular file for SQLite parent failure");
    let config = StateStoreHostConfig {
        state_store: StateStoreAppConfig {
            store: StateStoreConfig {
                cluster_id: "frontend-cluster".to_owned(),
                limits: StateStoreLimitOverrides::default(),
                provider: StateStoreProviderConfig::Sqlite {
                    path: non_directory_parent.join("state-store.sqlite"),
                    deployment_owner: "frontend-fe".to_owned(),
                },
            },
            mysql_client: None,
        },
        foundationdb_client: None,
    };

    let error = match open_host(Some(config.clone())).await {
        Ok(_) => panic!("unopenable SQLite path must fail host initialization"),
        Err(error) => error,
    };
    assert_eq!(error.kind(), FrontendApplicationErrorKind::StateStoreHost);

    let host = open_host(Some(sqlite_config(&temp)))
        .await
        .expect("failed open must not retain partial runtime resources");
    host.shutdown()
        .await
        .expect("reopened SQLite host shutdown must succeed");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn shutdown_releases_sqlite_deployment_lock() {
    let temp = TempDir::new().expect("temporary SQLite deployment");
    let config = sqlite_config(&temp);
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
async fn shutdown_is_required_to_reopen_same_deployment() {
    let temp = TempDir::new().expect("temporary SQLite deployment");
    let config = sqlite_config(&temp);
    let host = open_host(Some(config.clone()))
        .await
        .expect("first SQLite host must open");

    let error = match open_host(Some(config.clone())).await {
        Ok(_) => panic!("second live SQLite host must be rejected"),
        Err(error) => error,
    };
    assert_eq!(error.kind(), FrontendApplicationErrorKind::StateStoreHost);

    host.shutdown()
        .await
        .expect("first host shutdown must succeed");
    let reopened = open_host(Some(config))
        .await
        .expect("same SQLite deployment must reopen after explicit shutdown");
    reopened
        .shutdown()
        .await
        .expect("reopened SQLite host shutdown must succeed");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn configured_host_restores_views_through_its_service_after_reopen() {
    let temp = TempDir::new().expect("temporary SQLite deployment");
    let config = sqlite_config(&temp);
    let host = open_host(Some(config.clone()))
        .await
        .expect("configured host must open");
    host.view_service()
        .try_handle_statement(
            &SessionViewEngine,
            "CREATE VIEW durable_view AS SELECT 42 AS answer",
            view_context(),
        )
        .expect("host view service must persist the view");
    host.shutdown().await.expect("first host shutdown");

    let reopened = open_host(Some(config))
        .await
        .expect("configured host must reopen");
    let mut query = parse_query("SELECT * FROM durable_view");
    reopened
        .view_service()
        .rewrite_query(&SessionViewEngine, &mut query, view_context())
        .expect("reopened host must restore the view");
    assert_eq!(
        query.to_string(),
        "SELECT * FROM (SELECT 42 AS answer) durable_view"
    );
    reopened.shutdown().await.expect("reopened host shutdown");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn corrupt_view_record_fails_host_open_at_the_view_service_boundary() {
    let temp = TempDir::new().expect("temporary SQLite deployment");
    let config = sqlite_config(&temp);
    let host = open_host(Some(config.clone()))
        .await
        .expect("configured host must open");
    let store = host.state_store().expect("configured host state store");
    let mut transaction = store
        .begin_write(
            TransactionId::from(Uuid::now_v7()),
            "seed corrupt frontend view record",
        )
        .await
        .expect("begin corrupt record write");
    transaction
        .put(
            database_key("default_catalog", "db").expect("view database key"),
            Value::try_from(Bytes::from_static(b"not-json")).expect("corrupt value"),
            Precondition::Absent,
        )
        .await
        .expect("stage corrupt record");
    assert!(matches!(
        transaction.commit().await,
        CommitOutcome::Committed(_)
    ));
    drop(store);
    host.shutdown().await.expect("seed host shutdown");

    let error = match open_host(Some(config)).await {
        Ok(_) => panic!("corrupt durable view metadata must reject host open"),
        Err(error) => error,
    };
    assert_eq!(error.kind(), FrontendApplicationErrorKind::ViewServiceOpen);
    assert!(error.to_string().contains("decode frontend view database"));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn view_open_failure_precedes_table_maintenance_open_failure() {
    let temp = TempDir::new().expect("temporary SQLite deployment");
    let config = sqlite_config(&temp);
    let host = open_host(Some(config.clone()))
        .await
        .expect("configured host must open");
    let store = host.state_store().expect("configured host state store");
    let mut transaction = store
        .begin_write(
            TransactionId::from(Uuid::now_v7()),
            "seed corrupt frontend application records",
        )
        .await
        .expect("begin corrupt record write");
    transaction
        .put(
            database_key("default_catalog", "db").expect("view database key"),
            Value::try_from(Bytes::from_static(b"not-json")).expect("corrupt view value"),
            Precondition::Absent,
        )
        .await
        .expect("stage corrupt view record");
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
    assert!(matches!(
        transaction.commit().await,
        CommitOutcome::Committed(_)
    ));
    drop(store);
    host.shutdown().await.expect("seed host shutdown");

    let error = match open_host(Some(config)).await {
        Ok(_) => panic!("corrupt durable application metadata must reject host open"),
        Err(error) => error,
    };
    assert_eq!(error.kind(), FrontendApplicationErrorKind::ViewServiceOpen);
    assert!(error.to_string().contains("decode frontend view database"));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn corrupt_table_maintenance_record_fails_open_and_releases_partial_resources() {
    let temp = TempDir::new().expect("temporary SQLite deployment");
    let config = sqlite_config(&temp);
    let host = open_host(Some(config.clone()))
        .await
        .expect("configured host must open");
    let store = host.state_store().expect("configured host state store");
    let mut transaction = store
        .begin_write(
            TransactionId::from(Uuid::now_v7()),
            "seed corrupt frontend table-maintenance record",
        )
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
    assert!(matches!(
        transaction.commit().await,
        CommitOutcome::Committed(_)
    ));
    drop(store);
    host.shutdown().await.expect("seed host shutdown");

    for _ in 0..2 {
        let error = match open_host(Some(config.clone())).await {
            Ok(_) => panic!("corrupt maintenance metadata must reject host open"),
            Err(error) => error,
        };
        assert_eq!(
            error.kind(),
            FrontendApplicationErrorKind::TableMaintenanceServiceOpen
        );
        assert!(
            error
                .to_string()
                .contains("open frontend optimize job repository")
        );
    }
}
