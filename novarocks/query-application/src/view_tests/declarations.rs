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
            request.columns,
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

fn execute(
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
fn local_declarations_keep_source_header_complete_types_and_immutable_versions() {
    let service = QueryViewService::new();
    let engine = engine();
    execute(
        &service,
        &engine,
        "CREATE VIEW v(public_x) AS SELECT /* original */ 1 AS x",
    )
    .unwrap();
    let original = service.catalog_snapshot().unwrap();
    let view = original.get(&identity("v")).unwrap();
    assert_eq!(view.raw_sql.as_ref(), "SELECT /* original */ 1 AS x");
    assert_eq!(view.columns[0].source_field_name.as_deref(), Some("x"));
    assert_eq!(view.columns[0].name.as_ref(), "public_x");
    assert!(!view.columns[0].value_type.nullable());
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
            "CREATE VIEW v(a,b) AS SELECT 1",
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
fn local_view_public_alias_does_not_hide_a_changed_dependency_header() {
    let service = QueryViewService::new();
    let engine = engine();
    execute(&service, &engine, "CREATE VIEW base AS SELECT 1 AS x").unwrap();
    execute(
        &service,
        &engine,
        "CREATE VIEW renamed(public_x) AS SELECT * FROM base",
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
        "CREATE VIEW consumer AS SELECT * FROM renamed",
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
