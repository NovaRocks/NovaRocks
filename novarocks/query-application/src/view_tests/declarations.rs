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

use super::*;
use novarocks_sql::analyze_error::AnalyzeErrorKind;
use novarocks_sql::compiler::{
    CatalogRelationFact, SqlCapturedCatalogPreparation, SqlCompileControl, SqlCompileError,
};
use novarocks_types::naming::TableIdentity;

// The fixture supplies only immutable local relation facts. SQL remains the
// sole dependency collector, declaration validator, and type author.
pub(super) fn analyze_local_definition(
    request: crate::view::ViewDefinitionAnalysisRequest<'_>,
) -> Result<Vec<ViewColumnDefinition>, SqlCompileError> {
    let mut preparation = SqlCapturedCatalogPreparation::try_new_definition(
        request.query.clone(),
        request.catalog,
        request.database,
        Some(TableIdentity::new(
            &request.target.catalog,
            &request.target.database,
            &request.target.view,
        )),
        SqlCompileControl::unbounded(),
    )?;
    while !preparation.needs().is_empty() {
        let facts = preparation
            .needs()
            .iter()
            .map(|need| match request.local_views.get(need.relation()) {
                Some(view) => CatalogRelationFact::view(need, view.clone()),
                None => CatalogRelationFact::missing(need, "absent local fixture relation"),
            })
            .collect::<Result<Vec<_>, _>>()
            .map_err(|error| SqlCompileError::Compilation(error.to_string()))?;
        preparation = preparation.continue_catalog(facts.into_boxed_slice())?;
    }
    preparation
        .analyze_view_definition(
            novarocks_sql::compiler::builtin_sql_function_catalog(),
            crate::completed_plan_fixture::test_constant_policy(),
        )
        .map(|columns| columns.into_vec())
}

fn engine() -> FakeViewEngine {
    FakeViewEngine {
        analyze_local_sql: true,
        ..Default::default()
    }
}

pub(super) fn execute(
    service: &QueryViewService,
    engine: &FakeViewEngine,
    sql: &str,
) -> Result<ViewStatementResult, crate::api::CommandError> {
    let statements = novarocks_parser::parse(sql).unwrap();
    let [ParsedStatement::View(statement)] = statements.as_slice() else {
        panic!("view statement fixture")
    };
    service.execute_statement(engine, statement, context(None, "db"))
}

fn identity(name: &str) -> TableIdentity {
    TableIdentity::new(crate::view_service::DEFAULT_CATALOG, "db", name)
}

fn assert_refused(
    service: &QueryViewService,
    engine: &FakeViewEngine,
    sql: &str,
    kind: AnalyzeErrorKind,
) {
    let error = execute(service, engine, sql).unwrap_err();
    assert_eq!(
        error.analyze_error().map(|error| error.kind()),
        Some(kind),
        "{error:?}"
    );
}

#[test]
fn local_declarations_keep_natural_names_complete_types_and_immutable_versions() {
    let service = QueryViewService::new();
    let engine = engine();
    execute(
        &service,
        &engine,
        "CREATE VIEW v AS SELECT /* original */ 1 AS public_x",
    )
    .unwrap();
    let original = service.catalog_snapshot().unwrap();
    let view = original.get(&identity("v")).unwrap();
    assert_eq!(view.raw_sql.as_ref(), "SELECT /* original */ 1 AS public_x");
    assert_eq!(
        view.columns[0].value_type.data_type(),
        &arrow::datatypes::DataType::Int64
    );
    assert_eq!(view.columns[0].name.as_ref(), "public_x");
    assert!(view.columns[0].value_type.nullable());
    let old_version = view.version.clone();
    let old_type = view.columns[0].value_type.clone();
    execute(
        &service,
        &engine,
        "CREATE OR REPLACE VIEW v AS SELECT 'text' AS renamed",
    )
    .unwrap();
    let replacement = service.catalog_snapshot().unwrap();
    let replaced = replacement.get(&identity("v")).unwrap();
    assert_ne!(replaced.version, old_version);
    assert_ne!(replaced.columns[0].value_type, old_type);
    assert_eq!(original.get(&identity("v")).unwrap().version, old_version);
    execute(&service, &engine, "DROP VIEW v").unwrap();
    assert!(
        service
            .catalog_snapshot()
            .unwrap()
            .get(&identity("v"))
            .is_none()
    );
    assert!(original.get(&identity("v")).is_some());
    assert!(replacement.get(&identity("v")).is_some());
}

#[test]
fn invalid_local_declarations_never_publish_a_registry_entry() {
    let service = QueryViewService::new();
    let engine = engine();
    for (sql, kind) in [
        (
            "CREATE VIEW v AS SELECT 1",
            AnalyzeErrorKind::MissingColumnName,
        ),
        (
            "CREATE VIEW v AS SELECT 1 AS a, 2 AS A",
            AnalyzeErrorKind::DuplicateColumnName,
        ),
        (
            "CREATE VIEW v AS SELECT * FROM (SELECT 1 AS a) AS r(a,b)",
            AnalyzeErrorKind::MismatchedColumnAliases,
        ),
    ] {
        assert_refused(&service, &engine, sql, kind);
        assert!(
            service
                .catalog_snapshot()
                .unwrap()
                .get(&identity("v"))
                .is_none()
        );
    }
}

#[test]
fn local_recursion_preserves_the_original_analysis_layer_and_failed_replacement() {
    let service = QueryViewService::new();
    let engine = engine();
    assert_refused(
        &service,
        &engine,
        "CREATE VIEW a AS SELECT * FROM a",
        AnalyzeErrorKind::UnknownTable,
    );
    execute(&service, &engine, "CREATE VIEW a AS SELECT 1 AS x").unwrap();
    let original = service.catalog_snapshot().unwrap();
    let version = original.get(&identity("a")).unwrap().version.clone();
    assert_refused(
        &service,
        &engine,
        "CREATE OR REPLACE VIEW a AS SELECT * FROM a",
        AnalyzeErrorKind::ViewIsRecursive,
    );
    execute(&service, &engine, "CREATE VIEW b AS SELECT * FROM a").unwrap();
    assert_refused(
        &service,
        &engine,
        "CREATE OR REPLACE VIEW a AS SELECT * FROM b",
        AnalyzeErrorKind::InvalidView,
    );
    assert_eq!(
        service
            .catalog_snapshot()
            .unwrap()
            .get(&identity("a"))
            .unwrap()
            .version,
        version
    );
}

#[test]
fn local_view_relation_alias_does_not_hide_changed_dependency_names() {
    let service = QueryViewService::new();
    let engine = engine();
    execute(&service, &engine, "CREATE VIEW base AS SELECT 1 AS x").unwrap();
    execute(
        &service,
        &engine,
        "CREATE VIEW renamed AS SELECT * FROM base",
    )
    .unwrap();
    analyze_snapshot_query(
        &service.catalog_snapshot().unwrap(),
        "SELECT * FROM renamed AS r(public_x)",
        "db",
        &novarocks_sql::sql_mode::SqlSemanticSettings::default(),
    )
    .unwrap();
    execute(
        &service,
        &engine,
        "CREATE OR REPLACE VIEW base AS SELECT 2 AS y",
    )
    .unwrap();
    assert_refused(
        &service,
        &engine,
        "CREATE VIEW consumer AS SELECT * FROM renamed AS r(public_x)",
        AnalyzeErrorKind::ViewIsStale,
    );
    assert!(
        service
            .catalog_snapshot()
            .unwrap()
            .get(&identity("consumer"))
            .is_none()
    );
}

// Capture uses the original SQL dependency collector and only the immutable
// snapshot's declarations. No test walker qualifies or expands stored SQL.
fn capture_snapshot_query(
    snapshot: &crate::view::LocalViewSnapshot,
    sql: &str,
    database: &str,
) -> Result<
    (
        novarocks_sql::compiler::SqlCapturedCatalog,
        Vec<TableIdentity>,
    ),
    SqlCompileError,
> {
    let mut preparation = SqlCapturedCatalogPreparation::try_new(
        parse_query(sql),
        Some("default_catalog"),
        database,
        SqlCompileControl::unbounded(),
    )?;
    let mut lookups = Vec::new();
    while !preparation.needs().is_empty() {
        let facts = preparation
            .needs()
            .iter()
            .map(|need| {
                lookups.push(need.relation().clone());
                match snapshot.get(need.relation()) {
                    Some(view) => CatalogRelationFact::view(need, view.clone()),
                    None => CatalogRelationFact::missing(need, "absent local fixture relation"),
                }
            })
            .collect::<Result<Vec<_>, _>>()
            .map_err(|error| SqlCompileError::Compilation(error.to_string()))?;
        preparation = preparation.continue_catalog(facts.into_boxed_slice())?;
    }
    Ok((preparation.capture(&[])?, lookups))
}

pub(super) fn analyze_snapshot_query(
    snapshot: &crate::view::LocalViewSnapshot,
    sql: &str,
    database: &str,
    semantics: &novarocks_sql::sql_mode::SqlSemanticSettings,
) -> Result<(), SqlCompileError> {
    use novarocks_sql::compiler::{
        SessionOptimizerSettings, SqlAnalyzeRequest, SqlCompileIntent, SqlCompiler,
        SqlPlanningEnvironment, SqlSessionContext, SqlStatementInput, builtin_sql_function_catalog,
        noop_constant_evaluator,
    };
    let (catalog, _) = capture_snapshot_query(snapshot, sql, database)?;
    SqlCompiler::analyze(SqlAnalyzeRequest::new(
        SqlStatementInput::parsed_query(Box::new(parse_query(sql))),
        SqlCompileIntent::AnalyzeOnly,
        SqlSessionContext {
            current_catalog: Some("default_catalog".into()),
            current_database: database.into(),
            optimizer_settings: SessionOptimizerSettings::default(),
            sql_semantics: semantics.clone(),
        },
        SqlPlanningEnvironment::Distributed,
        &catalog,
        builtin_sql_function_catalog(),
        noop_constant_evaluator(),
        None,
        crate::completed_plan_fixture::test_constant_policy(),
        SqlCompileControl::unbounded(),
    ))
    .map(|_| ())
}

#[test]
fn one_two_and_three_part_names_use_the_same_registry_declaration_through_sql() {
    let service = QueryViewService::new();
    let engine = engine();
    service
        .try_handle_statement(
            &engine,
            "CREATE VIEW default_catalog.db.v AS SELECT 1 AS a",
            context(None, "other"),
        )
        .unwrap();
    let snapshot = service.catalog_snapshot().unwrap();
    let original = snapshot.get(&identity("v")).unwrap();
    for (sql, database, lookup_catalog) in [
        ("SELECT * FROM v", "db", None),
        ("SELECT * FROM db.v", "other", None),
        (
            "SELECT * FROM default_catalog.db.v",
            "other",
            Some("default_catalog"),
        ),
    ] {
        let (catalog, lookups) = capture_snapshot_query(&snapshot, sql, database).unwrap();
        assert_eq!(lookups, [identity("v")]);
        let selected = catalog
            .provider()
            .resolve_view_for_analysis(lookup_catalog, "db", "v")
            .unwrap()
            .unwrap();
        assert_eq!(selected, original.as_ref());
        analyze_snapshot_query(
            &snapshot,
            sql,
            database,
            &novarocks_sql::sql_mode::SqlSemanticSettings::default(),
        )
        .unwrap();
    }
}

#[test]
fn creation_database_is_distinct_from_target_and_consumer_databases() {
    let service = QueryViewService::new();
    let engine = engine();
    for (sql, database) in [
        ("CREATE VIEW base AS SELECT 1 AS a", "caller"),
        (
            "CREATE VIEW base AS SELECT 'different namespace' AS a",
            "creation",
        ),
        (
            "CREATE VIEW default_catalog.creation.v AS SELECT * FROM base",
            "caller",
        ),
    ] {
        service
            .try_handle_statement(&engine, sql, context(None, database))
            .unwrap();
    }
    let snapshot = service.catalog_snapshot().unwrap();
    let target = TableIdentity::new("default_catalog", "creation", "v");
    let saved = snapshot.get(&target).unwrap();
    assert_eq!(saved.default_database.as_ref(), "caller");
    let (_, lookups) =
        capture_snapshot_query(&snapshot, "SELECT * FROM creation.v", "other").unwrap();
    assert_eq!(
        lookups,
        [
            target,
            TableIdentity::new("default_catalog", "caller", "base")
        ]
    );
    analyze_snapshot_query(
        &snapshot,
        "SELECT * FROM creation.v",
        "other",
        &novarocks_sql::sql_mode::SqlSemanticSettings::default(),
    )
    .unwrap();
}

#[test]
fn saved_with_and_derived_cte_scopes_are_analyzed_without_physical_cte_lookups() {
    let service = QueryViewService::new();
    let engine = engine();
    for (name, body) in [
        (
            "nested",
            "WITH t AS (SELECT 1 AS a) SELECT a FROM (SELECT * FROM t) d",
        ),
        (
            "preceding",
            "WITH t AS (SELECT 1 AS a), u AS (SELECT * FROM (SELECT * FROM t) d) SELECT * FROM u",
        ),
    ] {
        execute(&service, &engine, &format!("CREATE VIEW {name} AS {body}")).unwrap();
        let snapshot = service.catalog_snapshot().unwrap();
        let sql = format!("WITH t AS (SELECT 'caller CTE' AS a) SELECT * FROM {name}");
        let (_, lookups) = capture_snapshot_query(&snapshot, &sql, "db").unwrap();
        assert_eq!(lookups, [identity(name)]);
        analyze_snapshot_query(
            &snapshot,
            &sql,
            "db",
            &novarocks_sql::sql_mode::SqlSemanticSettings::default(),
        )
        .unwrap();
    }
}

#[test]
fn actual_saved_view_analysis_respects_caller_legacy_semantics_and_select_local_overrides() {
    use novarocks_sql::sql_mode::{SqlMode, SqlSemanticSettings};
    let service = QueryViewService::new();
    let engine = engine();
    execute(&service, &engine, "CREATE VIEW v AS SELECT 1 AS a").unwrap();
    let snapshot = service.catalog_snapshot().unwrap();
    for (caller, hint) in [
        (
            SqlSemanticSettings::default()
                .with_sql_mode(SqlMode::from_assignment("GROUP_CONCAT_LEGACY")),
            "sql_mode=32",
        ),
        (
            SqlSemanticSettings::default().with_decimal_overflow_to_double(true),
            "decimal_overflow_to_double=false",
        ),
    ] {
        let original_caller = caller.clone();
        for sql in [
            "SELECT * FROM v",
            "WITH c AS (SELECT * FROM v) SELECT * FROM c",
            "SELECT * FROM (SELECT * FROM v) d",
            "SELECT 1 AS a UNION ALL SELECT * FROM v",
        ] {
            let error = analyze_snapshot_query(&snapshot, sql, "db", &caller).unwrap_err();
            let SqlCompileError::Analyze(error) = error else {
                panic!("original analysis refusal")
            };
            assert_eq!(error.kind(), AnalyzeErrorKind::UnsupportedExpression);
            analyze_snapshot_query(&snapshot, sql, "db", &SqlSemanticSettings::default()).unwrap();
        }
        for sql in [
            format!("SELECT /*+ SET_VAR({hint}) */ * FROM v"),
            format!("WITH c AS (SELECT /*+ SET_VAR({hint}) */ * FROM v) SELECT * FROM c"),
            format!("SELECT * FROM (SELECT /*+ SET_VAR({hint}) */ * FROM v) d"),
            format!("SELECT 1 AS a UNION ALL SELECT /*+ SET_VAR({hint}) */ * FROM v"),
        ] {
            analyze_snapshot_query(&snapshot, &sql, "db", &caller).unwrap();
        }
        assert_eq!(caller, original_caller);
    }
}
