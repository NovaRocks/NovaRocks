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

//! Query-application SQL admission that is independent of a product route.

use crate::session_error::{QueryServiceError, QueryServiceErrorKind};
use novarocks_parser::ast::{self, Statement as ParsedStatement};
use novarocks_workload_control::WorkClass;

/// Connection management has no authority in a statement-scoped client session.
/// Refuse its admitted syntax before creating any statement work or effects.
pub fn admit_statement_session(statement: &ParsedStatement) -> Result<(), QueryServiceError> {
    if matches!(
        statement,
        ParsedStatement::Session(ast::SessionStatement::Kill(_))
            | ParsedStatement::ShowProcessList(_)
    ) {
        return Err(QueryServiceError::new(
            QueryServiceErrorKind::Unsupported,
            "connection management SQL is unavailable for statement-scoped client sessions",
        ));
    }
    Ok(())
}

/// Reject uncaptured semantic settings before any definition side effects.
/// Metadata-only statements remain available; refresh/repartition replays the
/// stored query and must obey the same explicit consumer boundary.
pub fn admit_persisted_definition_semantics(
    source: &str,
    statement: &ParsedStatement,
    session: &novarocks_sql::sql_mode::SqlSemanticSettings,
) -> Result<(), QueryServiceError> {
    let definition = match statement {
        ParsedStatement::View(ast::ViewStatement::Create(view)) => Some(&view.query),
        ParsedStatement::MaterializedView(ast::MaterializedViewStatement::Create(mv)) => {
            Some(&mv.query)
        }
        _ => None,
    };
    let (legacy, numeric, overflow_error) = if let Some(query) = definition {
        (
            novarocks_sql::sql_mode::query_uses_group_concat_legacy(session, query).map_err(
                |error| QueryServiceError::from_user_error(error.to_user_error(Some(source))),
            )?,
            novarocks_sql::sql_mode::query_uses_decimal_overflow_to_double(session, query)
                .map_err(|error| {
                    QueryServiceError::from_user_error(error.to_user_error(Some(source)))
                })?,
            novarocks_sql::sql_mode::query_uses_error_if_overflow(session, query).map_err(
                |error| QueryServiceError::from_user_error(error.to_user_error(Some(source))),
            )?,
        )
    } else if matches!(
        statement,
        ParsedStatement::MaterializedView(ast::MaterializedViewStatement::Refresh(_))
            | ParsedStatement::MaterializedView(ast::MaterializedViewStatement::ExplainRefresh(_))
    ) || matches!(statement, ParsedStatement::MaterializedView(ast::MaterializedViewStatement::Alter(mv))
            if matches!(&mv.action, ast::MaterializedViewAlterAction::Repartition(_)))
    {
        (
            session.sql_mode().group_concat_legacy(),
            session.decimal_overflow_to_double(),
            session.sql_mode().error_if_overflow(),
        )
    } else {
        return Ok(());
    };
    if legacy || numeric || overflow_error {
        let message = if numeric {
            "unsupported semantic setting: decimal_overflow_to_double=true is not captured for persistent VIEW or MATERIALIZED VIEW definition replay"
        } else if legacy {
            "unsupported semantic setting: GROUP_CONCAT_LEGACY is not captured for persistent VIEW or MATERIALIZED VIEW definition replay"
        } else {
            "unsupported semantic setting: ERROR_IF_OVERFLOW is not captured for persistent VIEW or MATERIALIZED VIEW definition replay"
        };
        return Err(QueryServiceError::from_user_error(
            crate::sql::session_admit::SessionAdmitError::PersistedDefinitionSemanticsUnsupported
                .to_user_error(source, statement.span(), message),
        ));
    }
    Ok(())
}

/// Return whether this admitted statement needs the provider-publication
/// deadline policy rather than only the session deadline.
pub fn requires_lake_publication_deadline(statement: &ParsedStatement) -> bool {
    match statement {
        ParsedStatement::Dml(_) | ParsedStatement::Table(_) | ParsedStatement::Iceberg(_) => true,
        ParsedStatement::Catalog(statement) => {
            !matches!(statement, ast::CatalogStatement::ShowCreateTable(_))
        }
        ParsedStatement::Maintenance(_) => true,
        ParsedStatement::MaterializedView(statement) => {
            !matches!(statement, ast::MaterializedViewStatement::ExplainRefresh(_))
        }
        ParsedStatement::View(statement) => !matches!(statement, ast::ViewStatement::ShowCreate(_)),
        ParsedStatement::Statistics(statement) => {
            matches!(statement, ast::StatisticsStatement::AnalyzeTable(_))
        }
        ParsedStatement::ShowProcessList(_) => false,
        ParsedStatement::Session(_)
        | ParsedStatement::Query(_)
        | ParsedStatement::ExplainQuery(_) => false,
    }
}

/// Classify a statement that has already been routed to the typed command
/// path. Plain queries and session commands must use their dedicated routes.
pub fn typed_statement_work_class(statement: &ParsedStatement) -> WorkClass {
    match statement {
        ParsedStatement::Dml(_)
        | ParsedStatement::ExplainQuery(_)
        | ParsedStatement::Statistics(ast::StatisticsStatement::AnalyzeTable(_))
        | ParsedStatement::Maintenance(ast::MaintenanceStatement::Optimize(_)) => WorkClass::Query,
        ParsedStatement::Maintenance(ast::MaintenanceStatement::Call(call))
            if matches!(call.procedure.parts.as_slice(), [_, namespace, procedure]
                if namespace.value.eq_ignore_ascii_case("system")
                    && (procedure.value.eq_ignore_ascii_case("rewrite_data_files")
                        || procedure.value.eq_ignore_ascii_case("rewrite_position_delete_files"))) =>
        {
            WorkClass::Query
        }
        ParsedStatement::Session(_) | ParsedStatement::Query(_) => {
            unreachable!("session and plain query statements do not use the typed route")
        }
        _ => WorkClass::Management,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sql::parse_single_statement;

    #[test]
    fn statement_sessions_refuse_connection_management_but_preserve_ordinary_sql() {
        for sql in [
            "KILL 7",
            "KILL QUERY 7",
            "KILL CONNECTION 7",
            "SHOW PROCESSLIST",
            "SHOW FULL PROCESSLIST",
        ] {
            let statement = parse_single_statement(sql).unwrap();
            let error = admit_statement_session(&statement).unwrap_err();
            assert_eq!(error.kind(), QueryServiceErrorKind::Unsupported, "{sql}");
        }
        for sql in [
            "SELECT 1",
            "USE db",
            "SET SESSION pipeline_dop = 1",
            "SHOW STATS FOR lake.db.events",
        ] {
            admit_statement_session(&parse_single_statement(sql).unwrap()).unwrap();
        }
    }

    #[test]
    fn definitions_and_direct_replay_have_explicit_semantic_admission() {
        use novarocks_sql::sql_mode::{SqlMode, SqlSemanticSettings};
        let modern = SqlSemanticSettings::default();
        let legacy = modern
            .clone()
            .with_sql_mode(SqlMode::from_assignment("GROUP_CONCAT_LEGACY"));
        for prefix in [
            "CREATE VIEW v AS",
            "CREATE OR REPLACE VIEW v AS",
            "CREATE MATERIALIZED VIEW mv DISTRIBUTED BY HASH(k) BUCKETS 1 AS",
        ] {
            let sql = format!("{prefix} SELECT 1");
            let statement = parse_single_statement(&sql).unwrap();
            assert!(admit_persisted_definition_semantics(&sql, &statement, &modern).is_ok());
            let error =
                admit_persisted_definition_semantics(&sql, &statement, &legacy).unwrap_err();
            assert_eq!(
                error.user_error().unwrap().code().as_str(),
                "sql.admit.persisted_definition_semantics_unsupported"
            );
            let sql = format!("{prefix} SELECT /*+ SET_VAR(sql_mode=32) */ 1");
            let statement = parse_single_statement(&sql).unwrap();
            assert!(admit_persisted_definition_semantics(&sql, &statement, &legacy).is_ok());
            let sql = format!(
                "{prefix} SELECT * FROM (SELECT /*+ SET_VAR(sql_mode='GROUP_CONCAT_LEGACY') */ 1) d"
            );
            let statement = parse_single_statement(&sql).unwrap();
            assert!(admit_persisted_definition_semantics(&sql, &statement, &modern).is_err());
        }
        for sql in [
            "REFRESH MATERIALIZED VIEW mv",
            "EXPLAIN REFRESH MATERIALIZED VIEW mv",
        ] {
            let statement = parse_single_statement(sql).unwrap();
            assert!(admit_persisted_definition_semantics(sql, &statement, &legacy).is_err());
            assert!(admit_persisted_definition_semantics(sql, &statement, &modern).is_ok());
        }
        for sql in [
            "SELECT 1",
            "DROP VIEW v",
            "SHOW CREATE VIEW v",
            "DROP MATERIALIZED VIEW mv",
            "ALTER MATERIALIZED VIEW mv PAUSE REFRESH",
        ] {
            let statement = parse_single_statement(sql).unwrap();
            assert!(admit_persisted_definition_semantics(sql, &statement, &legacy).is_ok());
        }
    }

    #[test]
    fn typed_statement_work_class_keeps_data_plane_work_distinct_from_management() {
        let explain = parse_single_statement("EXPLAIN SELECT 1").expect("parse explain");
        let dml = parse_single_statement("INSERT INTO target VALUES (1)").expect("parse DML");
        let management =
            parse_single_statement("CREATE DATABASE governed_management").expect("parse DDL");

        assert_eq!(typed_statement_work_class(&explain), WorkClass::Query);
        assert_eq!(typed_statement_work_class(&dml), WorkClass::Query);
        assert_eq!(
            typed_statement_work_class(&management),
            WorkClass::Management
        );
        // OPTIMIZE retains this exact SQL permit until its statement exits.
        let optimize = parse_single_statement("ALTER TABLE lake.db.events OPTIMIZE")
            .expect("parse statement-owned optimize");
        assert_eq!(typed_statement_work_class(&optimize), WorkClass::Query);
    }

    #[test]
    fn analyze_retains_the_original_query_permit() {
        // ANALYZE is ordinary query work under its original SQL permit.
        for sql in [
            "ANALYZE TABLE lake.db.events",
            "ANALYZE TABLE lake.db.events (event_id)",
        ] {
            let statement = parse_single_statement(sql).expect("parse ANALYZE");
            assert!(matches!(
                statement,
                ParsedStatement::Statistics(ast::StatisticsStatement::AnalyzeTable(_))
            ));
            assert_eq!(
                typed_statement_work_class(&statement),
                WorkClass::Query,
                "{sql}"
            );
        }
    }

    #[test]
    fn statistics_observation_remains_management() {
        let statement = parse_single_statement("SHOW STATS FOR lake.db.events").unwrap();
        assert_eq!(
            typed_statement_work_class(&statement),
            WorkClass::Management
        );
        assert!(!requires_lake_publication_deadline(&statement));
    }

    #[test]
    fn publication_deadline_applies_only_to_effect_capable_statement_shapes() {
        let query = parse_single_statement("SELECT 1").expect("parse query");
        let dml = parse_single_statement("INSERT INTO target VALUES (1)").expect("parse DML");

        assert!(!requires_lake_publication_deadline(&query));
        assert!(requires_lake_publication_deadline(&dml));
    }
    #[test]
    fn persisted_definition_guard_uses_effective_root_and_nested_settings() {
        let disabled = novarocks_sql::sql_mode::SqlSemanticSettings::default();
        let enabled = disabled.clone().with_decimal_overflow_to_double(true);
        for prefix in [
            "CREATE VIEW v AS ",
            "CREATE OR REPLACE VIEW v AS ",
            "CREATE MATERIALIZED VIEW mv DISTRIBUTED BY HASH(v) BUCKETS 1 AS ",
        ] {
            for (settings, query, rejected) in [
                (&disabled, "SELECT 1", false),
                (&enabled, "SELECT 1", true),
                (
                    &disabled,
                    "SELECT /*+ SET_VAR(decimal_overflow_to_double=1) */ 1",
                    true,
                ),
                (
                    &enabled,
                    "SELECT /*+ SET_VAR(decimal_overflow_to_double=0) */ 1",
                    false,
                ),
                (
                    &disabled,
                    "SELECT 1 FROM (SELECT /*+ SET_VAR(decimal_overflow_to_double=1) */ 1 x) t",
                    true,
                ),
                (
                    &enabled,
                    "SELECT /*+ SET_VAR(decimal_overflow_to_double=0) */ 1 FROM (SELECT 1 x) t",
                    false,
                ),
                (
                    &disabled,
                    "WITH t AS (SELECT /*+ SET_VAR(decimal_overflow_to_double=1) */ 1) SELECT 1 FROM t",
                    true,
                ),
                (
                    &disabled,
                    "SELECT 1 UNION ALL SELECT /*+ SET_VAR(decimal_overflow_to_double=1) */ 1",
                    true,
                ),
            ] {
                let sql = format!("{prefix}{query}");
                let statement = parse_single_statement(&sql).unwrap();
                let result = admit_persisted_definition_semantics(&sql, &statement, settings);
                assert_eq!(result.is_err(), rejected, "{sql}");
                if rejected {
                    let error = result.unwrap_err();
                    assert_eq!(
                        error.user_error().unwrap().code().as_str(),
                        "sql.admit.persisted_definition_semantics_unsupported"
                    );
                    assert!(error.user_error().unwrap().location().is_some());
                }
            }
        }
        let sql = "CREATE VIEW v AS SELECT 1 FROM (SELECT /*+ SET_VAR(decimal_overflow_to_double='bad') */ 1) t";
        let statement = parse_single_statement(sql).unwrap();
        let error = admit_persisted_definition_semantics(sql, &statement, &disabled).unwrap_err();
        assert_eq!(
            error.user_error().unwrap().code().as_str(),
            "sql.analyze.invalid_argument"
        );
        let sql = "DROP VIEW v";
        assert!(
            admit_persisted_definition_semantics(
                sql,
                &parse_single_statement(sql).unwrap(),
                &enabled
            )
            .is_ok()
        );
    }
    #[test]
    fn error_overflow_definition_admission_rejects_inherited_and_nested_policy_before_effects() {
        use novarocks_sql::sql_mode::{SqlMode, SqlSemanticSettings};
        let default = SqlSemanticSettings::default();
        let error = default
            .clone()
            .with_sql_mode(SqlMode::from_assignment("ERROR_IF_OVERFLOW"));
        for prefix in [
            "CREATE VIEW v AS ",
            "CREATE OR REPLACE VIEW v AS ",
            "CREATE MATERIALIZED VIEW mv DISTRIBUTED BY HASH(x) BUCKETS 1 AS ",
        ] {
            for (snapshot, query, rejected) in [
                (&default, "SELECT 1 x", false),
                (&error, "SELECT 1 x", true),
                (
                    &default,
                    "SELECT /*+ SET_VAR(sql_mode='ERROR_IF_OVERFLOW') */ 1 x",
                    true,
                ),
                (&error, "SELECT /*+ SET_VAR(sql_mode=32) */ 1 x", false),
                (
                    &default,
                    "SELECT 1 x FROM (SELECT /*+ SET_VAR(sql_mode='ERROR_IF_OVERFLOW') */ 1 x) t",
                    true,
                ),
                (
                    &error,
                    "SELECT /*+ SET_VAR(sql_mode=32) */ 1 x FROM (SELECT 1 x) t",
                    false,
                ),
            ] {
                let sql = format!("{prefix}{query}");
                let statement = parse_single_statement(&sql).unwrap();
                let outcome = admit_persisted_definition_semantics(&sql, &statement, snapshot);
                assert_eq!(outcome.is_err(), rejected, "{sql}");
                if rejected {
                    let error = outcome.unwrap_err();
                    assert_eq!(
                        error.user_error().unwrap().code().as_str(),
                        "sql.admit.persisted_definition_semantics_unsupported"
                    );
                    assert!(error.user_error().unwrap().location().is_some());
                    assert!(
                        error
                            .to_string()
                            .contains("ERROR_IF_OVERFLOW is not captured")
                    );
                }
            }
        }
    }
    #[test]
    fn direct_rewrite_calls_keep_the_original_sql_query_permit_class() {
        for sql in [
            "CALL ice.system.rewrite_data_files(table => 'db.orders')",
            "CALL ice.system.rewrite_position_delete_files(table => 'db.orders')",
            "CALL ice.SYSTEM.ReWrItE_DaTa_FiLeS(table => 'db.orders')",
        ] {
            let statement = parse_single_statement(sql).expect("original typed CALL parser");
            assert_eq!(typed_statement_work_class(&statement), WorkClass::Query);
        }
        for sql in [
            "CALL ice.system.remove_orphan_files(table => 'db.orders', older_than => TIMESTAMP '2020-01-01 00:00:00')",
            "CALL ice.other.rewrite_data_files(table => 'db.orders')",
            "CALL ice.system.rewrite_manifests(table => 'db.orders')",
            "SHOW STATS FOR lake.db.events",
        ] {
            let statement = parse_single_statement(sql).expect("original typed management parser");
            assert_eq!(
                typed_statement_work_class(&statement),
                WorkClass::Management
            );
        }
    }
}
