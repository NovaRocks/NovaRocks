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
use std::error::Error;
use std::sync::Arc;

use arrow::datatypes::DataType;
use novarocks_sql::analyze_error::{AnalyzeError, AnalyzeErrorKind};
use novarocks_sql::compiler::{
    SqlAnalyzeRequest, SqlCancellationObservation, SqlCompileControl, SqlCompileError,
    SqlCompileIntent, SqlCompiler, SqlMvRefreshAnalysisContext, SqlPlannerTableSnapshot,
    SqlPlanningEnvironment, SqlStatementInput, analyze_mv_refresh_input,
    builtin_sql_function_catalog,
};
use novarocks_sql::planning::catalog::{
    PlannerMemoryCatalog, PlannerTableProvider, ResolvedAnalyzerTable, SqlViewColumn,
    SqlViewDefinition, SqlViewDialect, SqlViewOrigin,
};
use novarocks_sql::planning::mv::SqlResolvedMvRefreshInput;
use novarocks_sql::sql_mode::{SqlSemanticSettings, StatementSessionEnvironment};
use novarocks_sql_testkit::{parse_query, sql_fixture_constant_policy};
use novarocks_type_contract::{
    FunctionValueType, StatementStartUtc, StatementTimeEnvironment, TimeZoneKey,
};
use novarocks_types::naming::TableIdentity;

use crate::fixtures::session;

const DATABASE: &str = "fixture";
const VOLATILE_RANDOM: &str = "volatile function 'random'";
const SESSION_ZONE: &str = "session time zone";
const EXPLICIT_ZONE: &str = "explicit time zone";
const CALLER_SESSION: &str = "caller's session";

fn analyze(
    sql: &str,
    catalog: &dyn PlannerTableProvider,
    control: SqlCompileControl,
) -> Result<SqlResolvedMvRefreshInput, SqlCompileError> {
    let catalog = SqlPlannerTableSnapshot::new(catalog);
    analyze_mv_refresh_input(SqlMvRefreshAnalysisContext {
        query: parse_query(sql),
        current_database: DATABASE.into(),
        catalog: &catalog,
        functions: builtin_sql_function_catalog(),
        constant_policy: sql_fixture_constant_policy(),
        control,
    })
}

fn analyze_definition(sql: &str) -> Result<SqlResolvedMvRefreshInput, SqlCompileError> {
    analyze(
        sql,
        &PlannerMemoryCatalog::default(),
        SqlCompileControl::unbounded(),
    )
}

fn analyze_error(failure: SqlCompileError) -> AnalyzeError {
    let SqlCompileError::Analyze(error) = failure else {
        panic!("expected a typed SQL analysis error: {failure:?}");
    };
    error
}

fn assert_effect(failure: SqlCompileError, diagnostic: &str) -> AnalyzeError {
    let error = analyze_error(failure);
    assert_eq!(
        error.kind(),
        AnalyzeErrorKind::MvDefinitionEffect,
        "{error}"
    );
    assert!(error.message().contains(diagnostic), "{error}");
    error
}

fn assert_source_location(error: &AnalyzeError, sql: &str, expected: &str) {
    let span = error.span().expect("original SQL location");
    let fragment = &sql[span.start()..span.end()];
    assert!(fragment.contains(expected), "{fragment:?} in {sql:?}");
}

struct StoredViewCatalog(SqlViewDefinition);

impl StoredViewCatalog {
    fn new(sql: &str, value_type: FunctionValueType) -> Self {
        Self(SqlViewDefinition {
            identity: TableIdentity::new("iceberg", DATABASE, "v"),
            origin: SqlViewOrigin::Iceberg {
                uuid: [7; 16],
                version_id: 1,
            },
            dialect: SqlViewDialect::Trino,
            version: "fixture-view-version-1".into(),
            raw_sql: sql.into(),
            default_catalog: "iceberg".into(),
            default_database: DATABASE.into(),
            columns: Arc::new([SqlViewColumn {
                name: "x".into(),
                value_type,
            }]),
        })
    }

    fn analyze_mv(&self) -> Result<SqlResolvedMvRefreshInput, SqlCompileError> {
        analyze("SELECT x FROM v", self, SqlCompileControl::unbounded())
    }

    fn analyze_ordinary_read(&self, semantics: SqlSemanticSettings) -> Result<(), SqlCompileError> {
        let catalog = SqlPlannerTableSnapshot::new(self);
        SqlCompiler::analyze(SqlAnalyzeRequest::new(
            SqlStatementInput::parsed_query(parse_query("SELECT x FROM v")),
            SqlCompileIntent::AnalyzeOnly,
            session(semantics, false),
            SqlPlanningEnvironment::Distributed,
            &catalog,
            builtin_sql_function_catalog(),
            novarocks_functions::testkit::shared_installed_builtin_owner_catalogue(),
            None,
            sql_fixture_constant_policy(),
            SqlCompileControl::unbounded(),
        ))?;
        Ok(())
    }
}

impl PlannerTableProvider for StoredViewCatalog {
    fn resolve_table_for_analysis(
        &self,
        _catalog: Option<&str>,
        database: &str,
        table: &str,
    ) -> Result<ResolvedAnalyzerTable, String> {
        Err(format!("Unknown fixture table {database}.{table}"))
    }

    fn resolve_view_for_analysis(
        &self,
        catalog: Option<&str>,
        database: &str,
        view: &str,
    ) -> Result<Option<&SqlViewDefinition>, AnalyzeError> {
        Ok((catalog.is_none_or(|name| name == self.0.identity.catalog)
            && database == self.0.identity.namespace
            && view == self.0.identity.table)
            .then_some(&self.0))
    }
}

#[test]
fn entire_definition_includes_unreachable_calls_and_discarded_exists_projection() {
    for (dependent, deterministic) in [
        (
            "SELECT IF(false, random(), 0e0) AS x",
            "SELECT IF(false, abs(1e0), 0e0) AS x",
        ),
        (
            "WITH q AS (SELECT random() AS r) SELECT q.r AS x FROM q",
            "WITH q AS (SELECT abs(1e0) AS r) SELECT q.r AS x FROM q",
        ),
        (
            "SELECT r AS x FROM (SELECT random() AS r) q",
            "SELECT r AS x FROM (SELECT abs(1e0) AS r) q",
        ),
        (
            "SELECT transform(ARRAY[1e0], n -> n + random()) AS x",
            "SELECT transform(ARRAY[1e0], n -> n + abs(1e0)) AS x",
        ),
        (
            "SELECT 1 AS x WHERE EXISTS (SELECT random() AS r FROM (VALUES 1) t(k))",
            "SELECT 1 AS x WHERE EXISTS (SELECT abs(1e0) AS r FROM (VALUES 1) t(k))",
        ),
    ] {
        let error = assert_effect(analyze_definition(dependent).unwrap_err(), VOLATILE_RANDOM);
        assert_source_location(&error, dependent, "random()");
        let admitted = analyze_definition(deterministic).unwrap();
        assert_eq!(admitted.analysis_facts().output_columns[0].name, "x");
    }
}

#[test]
fn expanded_view_retains_original_effect_source_and_ordinary_read() {
    let sql = "SELECT IF(false, random(), 0e0) AS x";
    let view = StoredViewCatalog::new(sql, FunctionValueType::new(DataType::Float64, false));
    let error = assert_effect(view.analyze_mv().unwrap_err(), VOLATILE_RANDOM);
    assert_eq!(error.stored_view_definition_source(), Some(sql));
    let original = error
        .source()
        .and_then(|cause| cause.downcast_ref::<AnalyzeError>())
        .expect("original stored view analysis cause");
    assert_eq!(original.kind(), AnalyzeErrorKind::MvDefinitionEffect);
    assert_source_location(original, sql, "random()");
    view.analyze_ordinary_read(SqlSemanticSettings::default())
        .unwrap();
}

#[test]
fn caller_session_is_detected_before_current_syntax_becomes_a_constant() {
    for (syntax, nullable) in [
        ("CURRENT_USER", false),
        ("CURRENT_CATALOG", true),
        ("CURRENT_SCHEMA", true),
    ] {
        let sql = format!("SELECT {syntax} AS x");
        let error = assert_effect(analyze_definition(&sql).unwrap_err(), CALLER_SESSION);
        assert_source_location(&error, &sql, syntax);
        let view = StoredViewCatalog::new(&sql, FunctionValueType::new(DataType::Utf8, nullable));
        let error = assert_effect(view.analyze_mv().unwrap_err(), CALLER_SESSION);
        assert_eq!(error.stored_view_definition_source(), Some(sql.as_str()));
        view.analyze_ordinary_read(SqlSemanticSettings::default().with_statement_session(Some(
            StatementSessionEnvironment::new(
                "reader".into(),
                Some("caller".into()),
                Some("session_schema".into()),
            ),
        )))
        .unwrap();
    }
}

#[test]
fn temporal_policy_uses_selected_overloads_and_conversion_sites() {
    let implicit = "SELECT to_unixtime(TIMESTAMP '2024-01-01 00:00:00') AS x";
    let error = assert_effect(analyze_definition(implicit).unwrap_err(), SESSION_ZONE);
    assert_source_location(&error, implicit, "TIMESTAMP '2024-01-01 00:00:00'");
    assert!(error.message().contains(EXPLICIT_ZONE), "{error}");

    let epoch = "SELECT from_unixtime(0e0) AS x";
    let error = assert_effect(analyze_definition(epoch).unwrap_err(), SESSION_ZONE);
    assert_source_location(&error, epoch, "from_unixtime(0e0)");
    assert!(error.message().contains(EXPLICIT_ZONE), "{error}");

    let clock = "SELECT CURRENT_TIMESTAMP AS x";
    let error = assert_effect(analyze_definition(clock).unwrap_err(), "statement");
    assert_source_location(&error, clock, "CURRENT_TIMESTAMP");

    for sql in [
        "SELECT to_unixtime(TIMESTAMP '2024-01-01 00:00:00 UTC') AS x",
        "SELECT from_unixtime(0e0, 'UTC') AS x",
        "SELECT CAST(DECIMAL '12.5' AS DECIMAL(4,1)) + DECIMAL '1.0' AS x",
    ] {
        let admitted = analyze_definition(sql).unwrap();
        assert_eq!(admitted.analysis_facts().output_columns[0].name, "x");
    }
}

#[test]
fn expanded_view_retains_explicit_and_dependent_time_zone_semantics() {
    let dependent =
        "SELECT CAST(TIMESTAMP '2024-01-01 00:00:00' AS TIMESTAMP(6) WITH TIME ZONE) AS x";
    let deterministic =
        "SELECT CAST(TIMESTAMP '2024-01-01 00:00:00 UTC' AS TIMESTAMP(6) WITH TIME ZONE) AS x";
    let declared = novarocks_expression::typing::temporal_function_value_type(
        novarocks_type_contract::TemporalType::TimestampWithTimeZone(
            novarocks_type_contract::TemporalPrecision::MICROSECONDS,
        ),
        false,
    )
    .unwrap();
    let view = StoredViewCatalog::new(dependent, declared.clone());
    let error = assert_effect(view.analyze_mv().unwrap_err(), SESSION_ZONE);
    assert_eq!(error.stored_view_definition_source(), Some(dependent));
    assert!(error.message().contains(EXPLICIT_ZONE), "{error}");
    view.analyze_ordinary_read(SqlSemanticSettings::default().with_statement_time(
        StatementTimeEnvironment {
            start: StatementStartUtc::try_new(1_704_067_200, 0).unwrap(),
            zone: TimeZoneKey::parse("America/New_York").unwrap(),
        },
    ))
    .unwrap();

    let admitted = StoredViewCatalog::new(deterministic, declared)
        .analyze_mv()
        .unwrap();
    let [dependency] = admitted.view_dependencies() else {
        panic!("expected the exact source view version");
    };
    assert_eq!(dependency.version(), "fixture-view-version-1");
    assert_eq!(dependency.iceberg_version(), Some(([7; 16], 1)));
}

#[test]
fn order_by_scope_fallback_commits_only_the_adopted_effects() {
    // The projection shadows the input stamp. In its scope, to_unixtime
    // entails a session-zone conversion before the qualified tie is resolved.
    // The FROM scope resolves both names using its already zoned stamp.
    let deterministic = "SELECT TIMESTAMP '2024-01-01 00:00:00' AS stamp         FROM (VALUES (TIMESTAMP '2024-01-01 00:00:00 UTC', 2)) t(stamp, tie)         ORDER BY to_unixtime(stamp) + t.tie";
    analyze_definition(deterministic).unwrap();

    // Reverse the timestamp types. Only the FROM-scope attempt can resolve
    // t.tie, so its implicit conversion controls the verdict.
    let dependent = "SELECT TIMESTAMP '2024-01-01 00:00:00 UTC' AS stamp         FROM (VALUES (TIMESTAMP '2024-01-01 00:00:00', 2)) t(stamp, tie)         ORDER BY to_unixtime(stamp) + t.tie";
    let error = assert_effect(analyze_definition(dependent).unwrap_err(), SESSION_ZONE);
    assert!(error.message().contains(EXPLICIT_ZONE), "{error}");

    let missing = deterministic.replace("t.tie", "t.missing");
    let independent = missing.replace("to_unixtime(stamp)", "0e0");
    let baseline = analyze_error(analyze_definition(&independent).unwrap_err());
    let error = analyze_error(analyze_definition(&missing).unwrap_err());
    assert_eq!(error.kind(), AnalyzeErrorKind::UnknownColumn, "{error}");
    assert_eq!(error.message(), baseline.message());
}

#[test]
fn stored_view_order_by_refusal_rebases_the_original_span_once() {
    let sql = "SELECT 1 AS x ORDER BY from_unixtime(0e0)";
    let view = StoredViewCatalog::new(sql, FunctionValueType::new(DataType::Int32, false));
    let error = assert_effect(view.analyze_mv().unwrap_err(), SESSION_ZONE);
    assert_eq!(error.stored_view_definition_source(), Some(sql));
    let original = error
        .source()
        .and_then(|cause| cause.downcast_ref::<AnalyzeError>())
        .expect("original stored view analysis cause");
    assert_source_location(original, sql, "from_unixtime(0e0)");
    assert!(original.source().is_none(), "{original}");
}

#[test]
fn compilation_cancellation_retains_its_typed_result() {
    struct Cancelled;
    impl SqlCancellationObservation for Cancelled {
        fn is_cancelled(&self) -> bool {
            true
        }
    }

    let result = analyze(
        "SELECT IF(false, random(), 0e0) AS x",
        &PlannerMemoryCatalog::default(),
        SqlCompileControl::new(None, Arc::new(Cancelled)),
    );
    assert_eq!(result.unwrap_err(), SqlCompileError::Cancelled);
}
