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
use std::num::NonZeroUsize;
use std::sync::{Arc, Mutex, OnceLock};
use std::time::{Duration, Instant};

use arrow::array::{Array, StringArray};
use bytes::Bytes;
use novarocks_frontend::FrontendViewService;
use novarocks_frontend::view::{
    CreateExternalViewRequest, ExternalViewResolution, ResolvedExternalView, ViewColumnDefinition,
    ViewEngine, ViewRequestContext, ViewService, ViewSqlDialect, ViewStatementResult, ViewTarget,
};
use novarocks_spi::{
    connector::{ConnectorCancellation, ConnectorRequestContext},
    state_store::{FeDeploymentView, StateStore},
};
use novarocks_state_store::{
    StateStoreAppConfig, StateStoreConfig, StateStoreHost, StateStoreHostConfig,
    StateStoreLimitOverrides, StateStoreProviderConfig, builtin_state_store_provider_registry,
};
use sqlparser::ast::{DataType, Query, Statement};
use sqlparser::parser::Parser;
use tempfile::TempDir;

#[derive(Default)]
struct FakeViewEngine {
    rest_catalogs: Mutex<HashSet<String>>,
    tables: Mutex<HashSet<ViewTarget>>,
    views: Mutex<HashMap<ViewTarget, ResolvedExternalView>>,
    created: Mutex<Vec<CreateExternalViewRequest>>,
    table_probe_failures: Mutex<HashSet<ViewTarget>>,
    analyzed_queries: Mutex<Vec<String>>,
}

impl FakeViewEngine {
    fn with_rest_catalog(self, catalog: &str) -> Self {
        self.rest_catalogs
            .lock()
            .unwrap()
            .insert(catalog.to_string());
        self
    }

    fn insert_view(&self, target: ViewTarget, sql: &str, default_database: &str) {
        self.views.lock().unwrap().insert(
            target,
            ResolvedExternalView {
                sql: sql.to_string(),
                dialect: "starrocks".to_string(),
                default_database: default_database.to_string(),
                column_names: vec!["a".to_string()],
                comment: None,
                properties: HashMap::new(),
            },
        );
    }
}

impl ViewEngine for FakeViewEngine {
    fn resolve_external_view(
        &self,
        target: &ViewTarget,
        _context: &ConnectorRequestContext,
    ) -> Result<ExternalViewResolution, String> {
        if !self.rest_catalogs.lock().unwrap().contains(&target.catalog) {
            return Err("Unsupported: connector has no view metadata capability".to_string());
        }
        if self.table_probe_failures.lock().unwrap().contains(target) {
            return Err("table probe failed".to_string());
        }
        if self.tables.lock().unwrap().contains(target) {
            return Ok(ExternalViewResolution::Table);
        }
        Ok(self
            .views
            .lock()
            .unwrap()
            .get(target)
            .cloned()
            .map(ExternalViewResolution::View)
            .unwrap_or(ExternalViewResolution::Missing))
    }

    fn create_external_view(
        &self,
        request: CreateExternalViewRequest,
        _context: &ConnectorRequestContext,
    ) -> Result<(), String> {
        if self.tables.lock().unwrap().contains(&request.target) {
            return Err(format!(
                "a table named {}.{}.{} already exists",
                request.target.catalog, request.target.database, request.target.view
            ));
        }
        let mut views = self.views.lock().unwrap();
        if views.contains_key(&request.target) && !request.or_replace && !request.if_not_exists {
            return Err(format!(
                "view already exists: {}.{}.{}",
                request.target.catalog, request.target.database, request.target.view
            ));
        }
        views.insert(
            request.target.clone(),
            ResolvedExternalView {
                sql: request.sql.clone(),
                dialect: "starrocks".to_string(),
                default_database: request.target.database.clone(),
                column_names: request
                    .columns
                    .iter()
                    .map(|column| column.name.clone())
                    .collect(),
                comment: request.comment.clone(),
                properties: request.properties.iter().cloned().collect(),
            },
        );
        drop(views);
        self.created.lock().unwrap().push(request);
        Ok(())
    }

    fn drop_external_view(
        &self,
        target: &ViewTarget,
        _context: &ConnectorRequestContext,
        policy: novarocks_spi::connector::DropPolicy,
    ) -> Result<(), String> {
        if self.views.lock().unwrap().remove(target).is_some() {
            Ok(())
        } else if policy == novarocks_spi::connector::DropPolicy::NoOpIfMissing {
            Ok(())
        } else {
            Err(format!(
                "unknown view: {}.{}.{}",
                target.catalog, target.database, target.view
            ))
        }
    }

    fn load_external_view(
        &self,
        target: &ViewTarget,
        _context: &ConnectorRequestContext,
    ) -> Result<Option<ResolvedExternalView>, String> {
        Ok(self.views.lock().unwrap().get(target).cloned())
    }

    fn list_external_views(
        &self,
        catalog: &str,
        database: &str,
        _context: &ConnectorRequestContext,
    ) -> Result<Vec<String>, String> {
        Ok(self
            .views
            .lock()
            .unwrap()
            .keys()
            .filter(|target| target.catalog == catalog && target.database == database)
            .map(|target| target.view.clone())
            .collect())
    }

    fn analyze_external_view(
        &self,
        _catalog: &str,
        _database: &str,
        query: &Query,
        _context: &ConnectorRequestContext,
    ) -> Result<Vec<ViewColumnDefinition>, String> {
        self.analyzed_queries
            .lock()
            .unwrap()
            .push(query.to_string());
        let width = match query.body.as_ref() {
            sqlparser::ast::SetExpr::Select(select) => select.projection.len(),
            _ => 1,
        };
        Ok((0..width)
            .map(|index| ViewColumnDefinition {
                name: format!("c{}", index + 1),
                data_type: DataType::BigInt(None),
                nullable: false,
            })
            .collect())
    }
}

struct NeverCancelled;

impl ConnectorCancellation for NeverCancelled {
    fn is_cancelled(&self) -> bool {
        false
    }
}

fn connector_context() -> &'static ConnectorRequestContext {
    static CONTEXT: OnceLock<ConnectorRequestContext> = OnceLock::new();
    CONTEXT.get_or_init(|| {
        ConnectorRequestContext::try_new(
            Instant::now() + Duration::from_secs(300),
            Arc::new(NeverCancelled),
            novarocks_spi::connector::MAX_CONNECTOR_HANDLE_PAYLOAD_BYTES,
            novarocks_spi::connector::MAX_CONNECTOR_TOTAL_PAYLOAD_BYTES,
        )
        .unwrap()
    })
}

fn context<'a>(catalog: Option<&'a str>, database: &'a str) -> ViewRequestContext<'a> {
    ViewRequestContext {
        current_catalog: catalog,
        current_database: database,
        connector_context: Some(connector_context()),
    }
}

fn parse_query(sql: &str) -> Query {
    let mut parser = Parser::new(&ViewSqlDialect).try_with_sql(sql).unwrap();
    let Statement::Query(query) = parser.parse_statement().unwrap() else {
        panic!("expected query");
    };
    *query
}

fn query_result(
    result: Option<ViewStatementResult>,
) -> novarocks::runtime::query_result::QueryResult {
    let Some(ViewStatementResult::Query(result)) = result else {
        panic!("expected query result");
    };
    result
}

fn query_rows(result: &novarocks::runtime::query_result::QueryResult) -> Vec<String> {
    result
        .chunks
        .iter()
        .flat_map(|chunk| {
            let values = chunk
                .batch
                .column(0)
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap();
            (0..values.len())
                .map(|index| values.value(index).to_string())
                .collect::<Vec<_>>()
        })
        .collect()
}

fn query_rows_at(
    result: &novarocks::runtime::query_result::QueryResult,
    column: usize,
) -> Vec<String> {
    result
        .chunks
        .iter()
        .flat_map(|chunk| {
            let values = chunk
                .batch
                .column(column)
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap();
            (0..values.len())
                .map(|index| values.value(index).to_string())
                .collect::<Vec<_>>()
        })
        .collect()
}

async fn open_sqlite_store(
    path: &std::path::Path,
) -> (StateStoreHost, std::sync::Arc<dyn StateStore>) {
    let registry = builtin_state_store_provider_registry().unwrap();
    let host = StateStoreHost::open(
        &registry,
        StateStoreHostConfig {
            state_store: StateStoreAppConfig {
                store: StateStoreConfig {
                    cluster_id: "view-service-test".to_string(),
                    limits: StateStoreLimitOverrides::default(),
                    provider: StateStoreProviderConfig::Sqlite {
                        path: path.to_path_buf(),
                        deployment_owner: "view-service-fe".to_string(),
                    },
                },
                mysql_client: None,
            },
            foundationdb_client: None,
        },
        FeDeploymentView {
            active_fe_count: NonZeroUsize::new(1).unwrap(),
            topology_revision: Bytes::from_static(b"view-service-topology"),
        },
        Instant::now() + Duration::from_secs(5),
    )
    .await
    .unwrap();
    let store = host.state_store().unwrap();
    (host, store)
}

#[tokio::test(flavor = "multi_thread")]
async fn session_view_ddl_show_and_rewrite_preserve_existing_behavior() {
    let service = FrontendViewService::open(None, tokio::runtime::Handle::current())
        .await
        .unwrap();
    let engine = FakeViewEngine::default();
    let ctx = context(None, "db");

    service
        .try_handle_statement(&engine, "CREATE VIEW v2 AS SELECT 2 AS a", ctx)
        .unwrap();
    service
        .try_handle_statement(&engine, "CREATE VIEW v1 AS SELECT * FROM v2", ctx)
        .unwrap();
    assert_eq!(
        service
            .try_handle_statement(&engine, "CREATE VIEW v1 AS SELECT 1", ctx)
            .unwrap_err(),
        "view already exists: db.v1"
    );

    let show = query_result(
        service
            .try_handle_statement(&engine, "SHOW   VIEWS", ctx)
            .unwrap(),
    );
    assert_eq!(show.columns[0].name, "Views_in_db");
    assert_eq!(query_rows(&show), vec!["v1", "v2"]);

    let mut query = parse_query("SELECT x.a FROM v1 AS x");
    service.rewrite_query(&engine, &mut query, ctx).unwrap();
    assert_eq!(
        query.to_string(),
        "SELECT x.a FROM (SELECT * FROM (SELECT 2 AS a) v2) AS x"
    );

    service
        .try_handle_statement(&engine, "CREATE OR REPLACE VIEW v1 AS SELECT 3 AS a", ctx)
        .unwrap();
    let mut replaced = parse_query("SELECT * FROM v1");
    service.rewrite_query(&engine, &mut replaced, ctx).unwrap();
    assert_eq!(replaced.to_string(), "SELECT * FROM (SELECT 3 AS a) v1");

    service
        .try_handle_statement(&engine, "DROP VIEW v1", ctx)
        .unwrap();
    let mut dropped = parse_query("SELECT * FROM v1");
    service.rewrite_query(&engine, &mut dropped, ctx).unwrap();
    assert_eq!(dropped.to_string(), "SELECT * FROM v1");
}

#[tokio::test(flavor = "multi_thread")]
async fn default_catalog_one_two_and_three_part_names_share_session_registry() {
    let service = FrontendViewService::open(None, tokio::runtime::Handle::current())
        .await
        .unwrap();
    let engine = FakeViewEngine::default();

    service
        .try_handle_statement(
            &engine,
            "CREATE VIEW default_catalog.db.v AS SELECT 1 AS a",
            context(None, "other"),
        )
        .unwrap();
    let mut two_part = parse_query("SELECT * FROM db.v");
    service
        .rewrite_query(&engine, &mut two_part, context(None, "other"))
        .unwrap();
    assert_eq!(two_part.to_string(), "SELECT * FROM (SELECT 1 AS a) v");

    service
        .try_handle_statement(
            &engine,
            "CREATE VIEW local AS SELECT 2 AS a",
            context(Some("default_catalog"), "db"),
        )
        .unwrap();
    let mut one_part = parse_query("SELECT * FROM local");
    service
        .rewrite_query(
            &engine,
            &mut one_part,
            context(Some("default_catalog"), "db"),
        )
        .unwrap();
    assert_eq!(one_part.to_string(), "SELECT * FROM (SELECT 2 AS a) local");
}

#[tokio::test(flavor = "multi_thread")]
async fn iceberg_ddl_routes_names_and_freezes_alias_and_table_shadow_rules() {
    let service = FrontendViewService::open(None, tokio::runtime::Handle::current())
        .await
        .unwrap();
    let engine = FakeViewEngine::default().with_rest_catalog("ice");
    let shadow = ViewTarget {
        catalog: "ice".to_string(),
        database: "db".to_string(),
        view: "shadow".to_string(),
    };
    engine.tables.lock().unwrap().insert(shadow);

    assert_eq!(
        service
            .try_handle_statement(
                &engine,
                "CREATE VIEW ice.db.shadow AS SELECT 1",
                context(None, "db"),
            )
            .unwrap_err(),
        "a table named ice.db.shadow already exists"
    );

    service
        .try_handle_statement(
            &engine,
            "CREATE VIEW db.v (left_col, right_col) AS SELECT 1, 2",
            context(Some("ice"), "ignored"),
        )
        .unwrap();
    let created = engine.created.lock().unwrap();
    assert_eq!(created[0].target.catalog, "ice");
    assert_eq!(created[0].target.database, "db");
    assert_eq!(
        created[0]
            .columns
            .iter()
            .map(|column| column.name.as_str())
            .collect::<Vec<_>>(),
        vec!["left_col", "right_col"]
    );
    drop(created);

    assert!(
        service
            .try_handle_statement(
                &engine,
                "CREATE VIEW ice.db.bad (only_one) AS SELECT 1, 2",
                context(None, "db"),
            )
            .unwrap_err()
            .contains("view column list has 1 names but the SELECT produces 2 columns")
    );

    let analyzed_before = engine.analyzed_queries.lock().unwrap().len();
    service
        .try_handle_statement(
            &engine,
            "CREATE VIEW IF NOT EXISTS ice.db.v AS SELECT 9",
            context(None, "db"),
        )
        .unwrap();
    assert_eq!(
        engine.analyzed_queries.lock().unwrap().len(),
        analyzed_before + 1
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn iceberg_show_create_escapes_comment_and_show_views_is_sorted() {
    let service = FrontendViewService::open(None, tokio::runtime::Handle::current())
        .await
        .unwrap();
    let engine = FakeViewEngine::default().with_rest_catalog("ice");
    let target = ViewTarget {
        catalog: "ice".to_string(),
        database: "db".to_string(),
        view: "v".to_string(),
    };
    engine.views.lock().unwrap().insert(
        target,
        ResolvedExternalView {
            sql: "SELECT 1 AS a".to_string(),
            dialect: "starrocks".to_string(),
            default_database: "db".to_string(),
            column_names: vec!["a".to_string()],
            comment: Some("say \"hello\"".to_string()),
            properties: HashMap::new(),
        },
    );
    engine.insert_view(
        ViewTarget {
            catalog: "ice".to_string(),
            database: "db".to_string(),
            view: "a".to_string(),
        },
        "SELECT 2",
        "db",
    );

    let show_create = query_result(
        service
            .try_handle_statement(&engine, "SHOW   CREATE VIEW ice.db.v", context(None, "db"))
            .unwrap(),
    );
    assert_eq!(show_create.columns[0].name, "View");
    assert_eq!(show_create.columns[1].name, "Create View");
    assert_eq!(query_rows_at(&show_create, 0), vec!["v"]);
    assert_eq!(
        query_rows_at(&show_create, 1),
        vec!["CREATE VIEW `ice`.`db`.`v` (`a`)\nCOMMENT \"say \\\"hello\\\"\"\nAS SELECT 1 AS a;"]
    );

    let show = query_result(
        service
            .try_handle_statement(
                &engine,
                "SHOW VIEWS FROM db",
                context(Some("ice"), "ignored"),
            )
            .unwrap(),
    );
    assert_eq!(query_rows(&show), vec!["a", "v"]);
}

#[tokio::test(flavor = "multi_thread")]
async fn rewrite_is_session_first_and_preserves_external_resolution_rules() {
    let service = FrontendViewService::open(None, tokio::runtime::Handle::current())
        .await
        .unwrap();
    let engine = FakeViewEngine::default().with_rest_catalog("ice");
    service
        .try_handle_statement(
            &engine,
            "CREATE VIEW local AS SELECT 7 AS a",
            context(None, "db"),
        )
        .unwrap();
    engine.insert_view(
        ViewTarget {
            catalog: "ice".to_string(),
            database: "db".to_string(),
            view: "local".to_string(),
        },
        "SELECT 8 AS a",
        "db",
    );
    engine.insert_view(
        ViewTarget {
            catalog: "ice".to_string(),
            database: "db".to_string(),
            view: "base".to_string(),
        },
        "SELECT 1 AS a",
        "db",
    );
    engine.insert_view(
        ViewTarget {
            catalog: "ice".to_string(),
            database: "db".to_string(),
            view: "nested".to_string(),
        },
        "SELECT * FROM base",
        "db",
    );

    let mut local = parse_query("SELECT * FROM local");
    service
        .rewrite_query(&engine, &mut local, context(Some("ice"), "db"))
        .unwrap();
    assert_eq!(local.to_string(), "SELECT * FROM (SELECT 7 AS a) local");

    let mut nested = parse_query("SELECT * FROM nested");
    service
        .rewrite_query(&engine, &mut nested, context(Some("ice"), "db"))
        .unwrap();
    assert_eq!(
        nested.to_string(),
        "SELECT * FROM (SELECT * FROM (SELECT 1 AS a) base) nested"
    );

    let mut cte = parse_query("WITH nested AS (SELECT 3 AS a) SELECT * FROM nested");
    service
        .rewrite_query(&engine, &mut cte, context(Some("ice"), "db"))
        .unwrap();
    assert_eq!(
        cte.to_string(),
        "WITH nested AS (SELECT 3 AS a) SELECT * FROM nested"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn session_cte_shadows_a_same_named_session_view() {
    let service = FrontendViewService::open(None, tokio::runtime::Handle::current())
        .await
        .unwrap();
    let engine = FakeViewEngine::default();
    service
        .try_handle_statement(
            &engine,
            "CREATE VIEW nested AS SELECT 7 AS a",
            context(None, "db"),
        )
        .unwrap();

    let mut query = parse_query("WITH nested AS (SELECT 3 AS a) SELECT * FROM nested");
    service
        .rewrite_query(&engine, &mut query, context(None, "db"))
        .unwrap();
    assert_eq!(
        query.to_string(),
        "WITH nested AS (SELECT 3 AS a) SELECT * FROM nested"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn session_cte_scope_flows_into_nested_queries_and_recursive_bodies() {
    let service = FrontendViewService::open(None, tokio::runtime::Handle::current())
        .await
        .unwrap();
    let engine = FakeViewEngine::default();
    for view in ["nested", "first", "second"] {
        service
            .try_handle_statement(
                &engine,
                &format!("CREATE VIEW {view} AS SELECT 7 AS a"),
                context(None, "db"),
            )
            .unwrap();
    }

    for sql in [
        "WITH nested AS (SELECT 3 AS a) \
         SELECT * FROM (SELECT * FROM nested) AS s",
        "WITH first AS (SELECT 1 AS a), \
         second AS (SELECT * FROM (SELECT * FROM first) AS s) \
         SELECT * FROM second",
        "WITH RECURSIVE nested AS (\
         SELECT 1 AS a UNION ALL SELECT * FROM nested\
         ) SELECT * FROM nested",
    ] {
        let mut query = parse_query(sql);
        let expected = query.to_string();
        service
            .rewrite_query(&engine, &mut query, context(None, "db"))
            .unwrap();
        assert_eq!(query.to_string(), expected, "input: {sql}");
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn external_cte_scope_flows_into_nested_queries_and_recursive_bodies() {
    let service = FrontendViewService::open(None, tokio::runtime::Handle::current())
        .await
        .unwrap();
    let engine = FakeViewEngine::default().with_rest_catalog("ice");
    for view in ["nested", "first", "second"] {
        engine.insert_view(
            ViewTarget {
                catalog: "ice".to_string(),
                database: "db".to_string(),
                view: view.to_string(),
            },
            "SELECT 7 AS a",
            "db",
        );
    }

    for sql in [
        "WITH nested AS (SELECT 3 AS a) \
         SELECT * FROM (SELECT * FROM nested) AS s",
        "WITH first AS (SELECT 1 AS a), \
         second AS (SELECT * FROM (SELECT * FROM first) AS s) \
         SELECT * FROM second",
        "WITH RECURSIVE nested AS (\
         SELECT 1 AS a UNION ALL SELECT * FROM nested\
         ) SELECT * FROM nested",
    ] {
        let mut query = parse_query(sql);
        let expected = query.to_string();
        service
            .rewrite_query(&engine, &mut query, context(Some("ice"), "db"))
            .unwrap();
        assert_eq!(query.to_string(), expected, "input: {sql}");
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn external_view_qualification_preserves_ctes_inside_nested_queries() {
    let service = FrontendViewService::open(None, tokio::runtime::Handle::current())
        .await
        .unwrap();
    let engine = FakeViewEngine::default().with_rest_catalog("ice");
    engine.insert_view(
        ViewTarget {
            catalog: "ice".to_string(),
            database: "db".to_string(),
            view: "wrapper".to_string(),
        },
        "WITH nested AS (SELECT 3 AS a) \
         SELECT * FROM (SELECT * FROM nested) AS s",
        "db",
    );

    let mut query = parse_query("SELECT * FROM wrapper");
    service
        .rewrite_query(&engine, &mut query, context(Some("ice"), "db"))
        .unwrap();
    let rendered = query.to_string();
    assert!(!rendered.contains("ice.db.nested"), "got: {rendered}");
    assert!(rendered.contains("FROM nested"), "got: {rendered}");
}

#[tokio::test(flavor = "multi_thread")]
async fn spi5b_rewrite_resolves_table_view_and_admission_failure_with_one_control_result() {
    let service = FrontendViewService::open(None, tokio::runtime::Handle::current())
        .await
        .unwrap();
    let engine = FakeViewEngine::default().with_rest_catalog("ice");
    let table = ViewTarget {
        catalog: "ice".to_string(),
        database: "db".to_string(),
        view: "table_wins".to_string(),
    };
    engine.tables.lock().unwrap().insert(table.clone());
    engine.insert_view(table, "SELECT 1", "db");
    let failed = ViewTarget {
        catalog: "ice".to_string(),
        database: "db".to_string(),
        view: "probe_failed".to_string(),
    };
    engine
        .table_probe_failures
        .lock()
        .unwrap()
        .insert(failed.clone());
    engine.insert_view(failed, "SELECT 2", "db");
    engine.insert_view(
        ViewTarget {
            catalog: "ice".to_string(),
            database: "db".to_string(),
            view: "cycle_a".to_string(),
        },
        "SELECT * FROM cycle_b",
        "db",
    );
    engine.insert_view(
        ViewTarget {
            catalog: "ice".to_string(),
            database: "db".to_string(),
            view: "cycle_b".to_string(),
        },
        "SELECT * FROM cycle_a",
        "db",
    );

    let mut unchanged = parse_query("SELECT * FROM table_wins JOIN probe_failed ON true");
    service
        .rewrite_query(&engine, &mut unchanged, context(Some("ice"), "db"))
        .unwrap();
    assert_eq!(
        unchanged.to_string(),
        "SELECT * FROM table_wins JOIN probe_failed ON true"
    );

    let mut cycle = parse_query("SELECT * FROM cycle_a");
    assert_eq!(
        service
            .rewrite_query(&engine, &mut cycle, context(Some("ice"), "db"))
            .unwrap_err(),
        "circular view reference: ice.db.cycle_a"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn dropping_external_database_does_not_remove_default_catalog_views() {
    let service = FrontendViewService::open(None, tokio::runtime::Handle::current())
        .await
        .unwrap();
    let engine = FakeViewEngine::default();
    service
        .try_handle_statement(
            &engine,
            "CREATE VIEW v AS SELECT 1 AS a",
            context(None, "db"),
        )
        .unwrap();

    service.drop_database("ice", "db").unwrap();
    let show = query_result(
        service
            .try_handle_statement(&engine, "SHOW VIEWS", context(None, "db"))
            .unwrap(),
    );
    assert_eq!(query_rows(&show), vec!["v"]);

    service.drop_database("default_catalog", "db").unwrap();
    let show = query_result(
        service
            .try_handle_statement(&engine, "SHOW VIEWS", context(None, "db"))
            .unwrap(),
    );
    assert!(query_rows(&show).is_empty());
}

#[tokio::test(flavor = "multi_thread")]
async fn configured_service_restores_durable_views_and_never_publishes_failed_replace() {
    let temp = TempDir::new().unwrap();
    let (_host, store) = open_sqlite_store(&temp.path().join("state.sqlite")).await;
    let service = FrontendViewService::open(
        Some(std::sync::Arc::clone(&store)),
        tokio::runtime::Handle::current(),
    )
    .await
    .unwrap();
    let engine = FakeViewEngine::default();
    service
        .try_handle_statement(
            &engine,
            "CREATE VIEW v AS SELECT 1 AS a",
            context(None, "db"),
        )
        .unwrap();

    let oversized = format!(
        "CREATE OR REPLACE VIEW v AS SELECT '{}' AS a",
        "x".repeat(70 * 1024)
    );
    assert!(
        service
            .try_handle_statement(&engine, &oversized, context(None, "db"))
            .unwrap_err()
            .contains("encode frontend view database default_catalog.db failed")
    );
    let mut unchanged = parse_query("SELECT * FROM v");
    service
        .rewrite_query(&engine, &mut unchanged, context(None, "db"))
        .unwrap();
    assert_eq!(unchanged.to_string(), "SELECT * FROM (SELECT 1 AS a) v");
    drop(service);

    let reopened = FrontendViewService::open(Some(store), tokio::runtime::Handle::current())
        .await
        .unwrap();
    let mut restored = parse_query("SELECT * FROM v");
    reopened
        .rewrite_query(&engine, &mut restored, context(None, "db"))
        .unwrap();
    assert_eq!(restored.to_string(), "SELECT * FROM (SELECT 1 AS a) v");
}

#[tokio::test(flavor = "multi_thread")]
async fn starrocks_view_sql_uses_the_same_parser_before_and_after_restart() {
    let temp = TempDir::new().unwrap();
    let (_host, store) = open_sqlite_store(&temp.path().join("state.sqlite")).await;
    let service = FrontendViewService::open(
        Some(std::sync::Arc::clone(&store)),
        tokio::runtime::Handle::current(),
    )
    .await
    .unwrap();
    let engine = FakeViewEngine::default().with_rest_catalog("ice");
    service
        .try_handle_statement(
            &engine,
            "CREATE VIEW dialect_view AS \
             SELECT first_value(a IGNORE NULLS) OVER () AS x FROM t",
            context(None, "db"),
        )
        .unwrap();
    drop(service);

    let reopened = FrontendViewService::open(Some(store), tokio::runtime::Handle::current())
        .await
        .unwrap();
    let mut query = parse_query("SELECT * FROM dialect_view");
    reopened
        .rewrite_query(&engine, &mut query, context(None, "db"))
        .unwrap();
    let rendered = query.to_string();
    assert!(
        rendered.contains("first_value(a IGNORE NULLS) OVER ()"),
        "got: {rendered}"
    );

    engine.insert_view(
        ViewTarget {
            catalog: "ice".to_string(),
            database: "db".to_string(),
            view: "dialect_view".to_string(),
        },
        "SELECT first_value(a IGNORE NULLS) OVER () AS x FROM t",
        "db",
    );
    let mut external = parse_query("SELECT * FROM dialect_view");
    reopened
        .rewrite_query(&engine, &mut external, context(Some("ice"), "db"))
        .unwrap();
    let rendered = external.to_string();
    assert!(
        rendered.contains("first_value(a IGNORE NULLS) OVER ()"),
        "got: {rendered}"
    );
}
