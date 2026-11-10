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
use std::sync::{Mutex, OnceLock};
use std::time::{Duration, Instant};

use crate::api::QueryResult;
use crate::persisted_query_definition::{PersistedQueryDefinition, PersistedQueryDialect};
use crate::view::{
    CreateExternalViewRequest, ResolvedExternalView, ViewColumnDefinition, ViewEngine,
    ViewRequestContext, ViewService, ViewStatementResult, ViewTarget,
};
use crate::view_service::QueryViewService;
use arrow::array::{Array, StringArray};
use novarocks_parser::ast::{Query, SetExpr, Statement as ParsedStatement};
use novarocks_spi::connector::{ConnectorRequestContext, ConnectorStopOwner};

#[derive(Default)]
struct FakeViewEngine {
    analyze_local_sql: bool,
    tables: Mutex<HashSet<ViewTarget>>,
    views: Mutex<HashMap<ViewTarget, ResolvedExternalView>>,
    created: Mutex<Vec<CreateExternalViewRequest>>,
    analyzed_count: Mutex<usize>,
}

impl FakeViewEngine {
    fn insert_view(&self, target: ViewTarget, sql: &str, default_database: &str) {
        let definition = PersistedQueryDefinition::new(
            sql,
            PersistedQueryDialect::StarRocks,
            &target.catalog,
            default_database,
        )
        .unwrap();
        self.views.lock().unwrap().insert(
            target,
            ResolvedExternalView {
                definition,
                comment: None,
                properties: HashMap::new(),
            },
        );
    }
}

impl ViewEngine for FakeViewEngine {
    fn create_external_view(
        &self,
        request: CreateExternalViewRequest,
        _context: &ConnectorRequestContext,
    ) -> Result<(), crate::api::CommandError> {
        if self.tables.lock().unwrap().contains(&request.target) {
            return Err(format!(
                "a table named {}.{}.{} already exists",
                request.target.catalog, request.target.database, request.target.view
            )
            .into());
        }
        let mut views = self.views.lock().unwrap();
        if views.contains_key(&request.target) && !request.or_replace && !request.if_not_exists {
            return Err(format!(
                "view already exists: {}.{}.{}",
                request.target.catalog, request.target.database, request.target.view
            )
            .into());
        }
        views.insert(
            request.target.clone(),
            ResolvedExternalView {
                definition: request.definition.clone(),
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
    ) -> Result<(), crate::api::CommandError> {
        if self.views.lock().unwrap().remove(target).is_some()
            || policy == novarocks_spi::connector::DropPolicy::NoOpIfMissing
        {
            Ok(())
        } else {
            Err(format!(
                "unknown view: {}.{}.{}",
                target.catalog, target.database, target.view
            )
            .into())
        }
    }

    fn load_external_view(
        &self,
        target: &ViewTarget,
        _context: &ConnectorRequestContext,
    ) -> Result<Option<ResolvedExternalView>, crate::api::CommandError> {
        Ok(self.views.lock().unwrap().get(target).cloned())
    }

    fn analyze_view_definition(
        &self,
        request: crate::view::ViewDefinitionAnalysisRequest<'_>,
        _context: &ConnectorRequestContext,
    ) -> Result<Vec<ViewColumnDefinition>, novarocks_sql::compiler::SqlCompileError> {
        *self.analyzed_count.lock().unwrap() += 1;
        if self.analyze_local_sql {
            return declarations::analyze_local_definition(request);
        }
        let width = match request.query.body.as_ref() {
            SetExpr::Select(select) => select.projection.len(),
            _ => 1,
        };
        Ok((0..width)
            .map(|index| ViewColumnDefinition {
                name: format!("c{}", index + 1).into(),
                value_type: novarocks_type_contract::FunctionValueType::new(
                    arrow::datatypes::DataType::Int64,
                    true,
                ),
            })
            .collect())
    }
}

fn connector_context() -> &'static ConnectorRequestContext {
    static CONTEXT: OnceLock<ConnectorRequestContext> = OnceLock::new();
    CONTEXT.get_or_init(|| {
        ConnectorRequestContext::try_new(
            Instant::now() + Duration::from_secs(300),
            ConnectorStopOwner::new().view(),
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

trait ViewServiceTestExt {
    fn try_handle_statement(
        &self,
        engine: &FakeViewEngine,
        sql: &str,
        context: ViewRequestContext<'_>,
    ) -> Result<Option<ViewStatementResult>, String>;
}

impl ViewServiceTestExt for QueryViewService {
    fn try_handle_statement(
        &self,
        engine: &FakeViewEngine,
        sql: &str,
        context: ViewRequestContext<'_>,
    ) -> Result<Option<ViewStatementResult>, String> {
        let statements = novarocks_parser::parse(sql).map_err(|error| error.to_string())?;
        let [ParsedStatement::View(statement)] = statements.as_slice() else {
            return Ok(None);
        };
        if let novarocks_parser::ast::ViewStatement::Show(show) = statement {
            let prepared = self.prepare_show_views(show, context)?;
            let names = match &prepared.source {
                crate::view::ShowViewsSource::Local(names) => names.clone(),
                crate::view::ShowViewsSource::External { catalog, namespace } => engine
                    .views
                    .lock()
                    .unwrap()
                    .keys()
                    .filter(|target| &target.catalog == catalog && &target.database == namespace)
                    .map(|target| target.view.clone())
                    .collect(),
            };
            return prepared
                .finish(names)
                .map(ViewStatementResult::Query)
                .map(Some);
        }
        self.execute_statement(engine, statement, context)
            .map(Some)
            .map_err(|error| error.to_string())
    }
}

fn parse_query(sql: &str) -> Query {
    let statements = novarocks_parser::parse(sql).expect("parse typed query");
    let [ParsedStatement::Query(query)] = statements.as_slice() else {
        panic!("expected typed query");
    };
    query.clone()
}

fn query_result(result: Option<ViewStatementResult>) -> QueryResult {
    let Some(ViewStatementResult::Query(result)) = result else {
        panic!("expected query result");
    };
    result
}

fn query_rows(result: &QueryResult) -> Vec<String> {
    result
        .batches
        .iter()
        .flat_map(|batch| {
            let values = batch
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

fn query_rows_at(result: &QueryResult, column: usize) -> Vec<String> {
    result
        .batches
        .iter()
        .flat_map(|batch| {
            let values = batch
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

#[test]
fn virtual_view_mutations_refuse_before_analysis_or_external_changes() {
    let service = QueryViewService::new();
    let engine = FakeViewEngine::default();
    engine.insert_view(
        ViewTarget {
            catalog: "system".into(),
            database: "metadata".into(),
            view: "v".into(),
        },
        "SELECT 1 AS a",
        "metadata",
    );
    for (catalog, database, name) in [
        (None, "db", "system.metadata.v"),
        (None, "db", "system.information_schema.v"),
        (None, "db", "warehouse.information_schema.v"),
        (None, "db", "information_schema.v"),
        (Some("system"), "metadata", "v"),
        (Some("warehouse"), "information_schema", "v"),
        (Some("warehouse"), "db", "information_schema.v"),
        (Some("DEFAULT_CATALOG"), "INFORMATION_SCHEMA", "v"),
    ] {
        for sql in [
            format!("CREATE VIEW {name} AS SELECT 1 AS a"),
            format!("CREATE VIEW IF NOT EXISTS {name} AS SELECT 1 AS a"),
            format!("DROP VIEW {name}"),
            format!("DROP VIEW IF EXISTS {name}"),
        ] {
            let statements = novarocks_parser::parse(&sql).unwrap();
            let [ParsedStatement::View(statement)] = statements.as_slice() else {
                panic!("view command")
            };
            let error =
                match service.execute_statement(&engine, statement, context(catalog, database)) {
                    Err(error) => error,
                    Ok(_) => panic!("virtual view mutation succeeded: {sql}"),
                };
            assert_eq!(
                error.kind(),
                crate::api::CommandErrorKind::Unsupported,
                "{sql}"
            );
            assert_eq!(*engine.analyzed_count.lock().unwrap(), 0);
            assert!(engine.created.lock().unwrap().is_empty());
            assert_eq!(
                engine.views.lock().unwrap().len(),
                1,
                "read-only rejection precedes conditional DROP"
            );
        }
    }
    assert_eq!(
        service
            .catalog_snapshot()
            .unwrap()
            .names_in_namespace("default_catalog", "information_schema")
            .count(),
        0
    );
}

#[test]
fn session_view_ddl_and_show_preserve_registry_behavior() {
    let service = QueryViewService::new();
    let engine = FakeViewEngine {
        analyze_local_sql: true,
        ..Default::default()
    };
    let ctx = context(None, "db");
    for sql in [
        "CREATE VIEW v2 AS SELECT 2 AS a",
        "CREATE VIEW v1 AS SELECT * FROM v2",
    ] {
        service.try_handle_statement(&engine, sql, ctx).unwrap();
    }
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
    assert_eq!(show.columns[0].name(), "Views_in_db");
    assert_eq!(query_rows(&show), vec!["v1", "v2"]);
    declarations::analyze_snapshot_query(
        &service.catalog_snapshot().unwrap(),
        "SELECT x.a FROM v1 AS x",
        "db",
        &novarocks_sql::sql_mode::SqlSemanticSettings::default(),
    )
    .unwrap();
    service
        .try_handle_statement(&engine, "CREATE OR REPLACE VIEW v1 AS SELECT 3 AS a", ctx)
        .unwrap();
    let replaced = service.catalog_snapshot().unwrap();
    assert_eq!(
        replaced
            .get(&novarocks_types::naming::TableIdentity::new(
                "default_catalog",
                "db",
                "v1"
            ))
            .unwrap()
            .raw_sql
            .as_ref(),
        "SELECT 3 AS a"
    );
    service
        .try_handle_statement(&engine, "DROP VIEW v1", ctx)
        .unwrap();
    assert!(
        service
            .catalog_snapshot()
            .unwrap()
            .get(&novarocks_types::naming::TableIdentity::new(
                "default_catalog",
                "db",
                "v1"
            ))
            .is_none()
    );
}

#[test]
fn local_views_do_not_exist_on_a_new_service_instance() {
    let service = QueryViewService::new();
    let engine = FakeViewEngine {
        analyze_local_sql: true,
        ..Default::default()
    };
    service
        .try_handle_statement(
            &engine,
            "CREATE VIEW v AS SELECT 1 AS a",
            context(None, "db"),
        )
        .unwrap();
    let identity = novarocks_types::naming::TableIdentity::new("default_catalog", "db", "v");
    let original = service.catalog_snapshot().unwrap();
    assert!(original.get(&identity).is_some());
    drop(service);
    let restarted = QueryViewService::new();
    assert!(
        restarted
            .catalog_snapshot()
            .unwrap()
            .get(&identity)
            .is_none()
    );
    assert!(original.get(&identity).is_some());
    let show = query_result(
        restarted
            .try_handle_statement(&engine, "SHOW VIEWS", context(None, "db"))
            .unwrap(),
    );
    assert!(query_rows(&show).is_empty());
}

#[test]
fn ignore_nulls_uses_the_original_parser_and_preserves_local_and_external_raw_source() {
    // This checks parsing and persisted source, not first_value execution capability.
    let body = "SELECT /* original */ first_value(a IGNORE NULLS) OVER () AS x FROM t";
    let parsed = parse_query(body);
    let SetExpr::Select(select) = parsed.body.as_ref() else {
        panic!("select")
    };
    let novarocks_parser::ast::SelectItem::ExprWithAlias {
        expr: novarocks_parser::ast::Expr::FunctionCall(call),
        ..
    } = &select.projection[0]
    else {
        panic!("original function call")
    };
    assert_eq!(
        call.null_treatment,
        Some(novarocks_parser::ast::NullTreatment::IgnoreNulls)
    );
    assert!(call.over.is_some());
    let service = QueryViewService::new();
    let engine = FakeViewEngine::default();
    service
        .try_handle_statement(
            &engine,
            &format!("CREATE VIEW dialect_view AS {body}"),
            context(None, "db"),
        )
        .unwrap();
    let snapshot = service.catalog_snapshot().unwrap();
    assert_eq!(
        snapshot
            .get(&novarocks_types::naming::TableIdentity::new(
                "default_catalog",
                "db",
                "dialect_view"
            ))
            .unwrap()
            .raw_sql
            .as_ref(),
        body
    );
    let target = ViewTarget {
        catalog: "ice".into(),
        database: "db".into(),
        view: "external_dialect_view".into(),
    };
    engine.insert_view(target.clone(), body, "db");
    let external = engine
        .load_external_view(&target, connector_context())
        .unwrap()
        .unwrap();
    assert_eq!(external.definition.raw_query_source, body);
    assert_eq!(parse_query(&external.definition.raw_query_source), parsed);
}

#[test]
fn iceberg_ddl_routes_names_and_freezes_alias_and_table_shadow_rules() {
    let service = QueryViewService::new();
    let engine = FakeViewEngine {
        analyze_local_sql: true,
        ..Default::default()
    };
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
                "CREATE VIEW ice.db.shadow AS SELECT 1 AS a",
                context(None, "db"),
            )
            .unwrap_err(),
        "a table named ice.db.shadow already exists"
    );

    service
        .try_handle_statement(
            &engine,
            "CREATE VIEW db.v AS SELECT /* preserve */ 1 AS left_col,\n  2 AS right_col",
            context(Some("ice"), "ignored"),
        )
        .unwrap();
    let created = engine.created.lock().unwrap();
    assert_eq!(created[0].target.catalog, "ice");
    assert_eq!(created[0].target.database, "db");
    assert_eq!(
        created[0].definition.raw_query_source,
        "SELECT /* preserve */ 1 AS left_col,\n  2 AS right_col"
    );
    assert_eq!(
        created[0].definition.resolution.default_catalog, "ice",
        "external view must freeze its creation catalog"
    );
    assert_eq!(
        created[0]
            .columns
            .iter()
            .map(|column| column.name.as_ref())
            .collect::<Vec<_>>(),
        vec!["left_col", "right_col"]
    );
    drop(created);

    let analyzed_before = *engine.analyzed_count.lock().unwrap();
    assert!(
        service
            .try_handle_statement(
                &engine,
                "CREATE VIEW ice.db.bad (only_one) AS SELECT 1, 2",
                context(None, "db"),
            )
            .is_err()
    );
    assert_eq!(*engine.analyzed_count.lock().unwrap(), analyzed_before);
    assert!(
        engine
            .created
            .lock()
            .unwrap()
            .iter()
            .all(|request| request.target.view != "bad")
    );

    let analyzed_before = *engine.analyzed_count.lock().unwrap();
    service
        .try_handle_statement(
            &engine,
            "CREATE VIEW IF NOT EXISTS ice.db.v AS SELECT 9 AS a",
            context(None, "db"),
        )
        .unwrap();
    assert_eq!(*engine.analyzed_count.lock().unwrap(), analyzed_before + 1);
}

#[test]
fn iceberg_show_create_escapes_comment_and_show_views_is_sorted() {
    let service = QueryViewService::new();
    let engine = FakeViewEngine::default();
    let target = ViewTarget {
        catalog: "ice".to_string(),
        database: "db".to_string(),
        view: "v".to_string(),
    };
    engine.views.lock().unwrap().insert(
        target,
        ResolvedExternalView {
            definition: PersistedQueryDefinition::new(
                "SELECT /* raw body */\n 1 AS a",
                PersistedQueryDialect::StarRocks,
                "ice",
                "db",
            )
            .unwrap(),
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
    assert_eq!(show_create.columns[0].name(), "View");
    assert_eq!(show_create.columns[1].name(), "Create View");
    assert_eq!(query_rows_at(&show_create, 0), vec!["v"]);
    assert_eq!(
        query_rows_at(&show_create, 1),
        vec![
            "CREATE VIEW `ice`.`db`.`v`\nCOMMENT \"say \\\"hello\\\"\"\nAS SELECT /* raw body */\n 1 AS a;"
        ]
    );

    let displayed = query_rows_at(&show_create, 1);
    let reparsed = novarocks_parser::parse(&displayed[0]).unwrap();
    let [ParsedStatement::View(novarocks_parser::ast::ViewStatement::Create(create))] =
        reparsed.as_slice()
    else {
        panic!("SHOW CREATE must be a supported CREATE VIEW statement")
    };
    assert_eq!(create.raw_query_source, "SELECT /* raw body */\n 1 AS a");

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

#[test]
fn dropping_external_database_does_not_remove_default_catalog_views() {
    let service = QueryViewService::new();
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

#[test]
fn show_views_pure_declaration_keeps_external_selection_and_result_semantics() {
    let service = QueryViewService::new();
    let statements = novarocks_parser::parse("SHOW VIEWS FROM db").unwrap();
    let [ParsedStatement::View(novarocks_parser::ast::ViewStatement::Show(show))] =
        statements.as_slice()
    else {
        panic!("SHOW declaration");
    };
    let prepared = service
        .prepare_show_views(show, context(Some("ice"), "ignored"))
        .unwrap();
    assert!(
        matches!(&prepared.source, crate::view::ShowViewsSource::External { catalog, namespace } if catalog == "ice" && namespace == "db")
    );
    let result = prepared
        .finish(vec!["z".into(), "a".into(), "z".into()])
        .unwrap();
    assert_eq!(result.columns[0].name(), "Views_in_db");
    assert_eq!(query_rows(&result), ["a", "z"]);
    assert!(
        service
            .prepare_show_views(
                show,
                ViewRequestContext {
                    current_catalog: Some("ice"),
                    current_database: "db",
                    connector_context: None,
                }
            )
            .is_err()
    );
}

#[test]
fn show_views_cannot_fall_back_to_synchronous_statement_dispatch() {
    let service = QueryViewService::new();
    let engine = FakeViewEngine::default();
    let statements = novarocks_parser::parse("SHOW VIEWS").unwrap();
    let [ParsedStatement::View(show)] = statements.as_slice() else {
        panic!("SHOW declaration");
    };
    assert_eq!(
        service
            .execute_statement(&engine, show, context(None, "db"))
            .unwrap_err()
            .to_string(),
        "SHOW VIEWS requires its original governed read source"
    );
}

#[path = "view_tests/declarations.rs"]
mod declarations;
