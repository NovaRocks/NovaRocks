// SPDX-License-Identifier: Apache-2.0

use novarocks_query_application::sql::{
    parse_single_statement,
    trino_admission::{
        TrinoStatementAction, TrinoStatementPreparation, admit_trino_statement_in_namespace,
    },
};

fn admit(sql: &str, catalog: Option<&str>, schema: Option<&str>) -> TrinoStatementAction {
    admit_trino_statement_in_namespace(sql, parse_single_statement(sql).unwrap(), catalog, schema)
        .unwrap()
}

#[test]
fn kill_query_binds_only_the_standard_runtime_procedure() {
    for (sql, catalog, schema) in [
        ("CALL system.runtime.kill_query('query_a')", None, None),
        (
            "CALL SYSTEM.RUNTIME.KILL_QUERY(message => 'stop', query_id => 'query_a')",
            None,
            None,
        ),
        (
            r#"CALL system.runtime.kill_query("QUERY_ID" => 'query_a', "MESSAGE" => NULL)"#,
            None,
            None,
        ),
        (
            "CALL runtime.kill_query('query_' || 'a', CAST(NULL AS VARCHAR))",
            Some("system"),
            None,
        ),
        (
            "CALL kill_query('query_a')",
            Some("system"),
            Some("runtime"),
        ),
    ] {
        assert!(
            matches!(
                admit(sql, catalog, schema),
                TrinoStatementAction::Prepare(TrinoStatementPreparation::KillQuery(_))
            ),
            "{sql}"
        );
    }
    for (sql, catalog, schema) in [
        (
            "CALL lake.runtime.kill_query('a')",
            Some("system"),
            Some("runtime"),
        ),
        ("CALL runtime.kill_query('a')", None, None),
        ("CALL kill_query('a')", Some("system"), Some("metadata")),
    ] {
        assert!(
            matches!(
                admit(sql, catalog, schema),
                TrinoStatementAction::Prepare(TrinoStatementPreparation::Call(_))
            ),
            "{sql}"
        );
    }
}

#[test]
fn kill_query_rejects_incomplete_and_ambiguous_argument_lists() {
    for sql in [
        "CALL system.runtime.kill_query()",
        "CALL system.runtime.kill_query(message => 'only message')",
        "CALL system.runtime.kill_query('query_a', message => 'mixed')",
        "CALL system.runtime.kill_query('query_a', 'message', 'third')",
        "CALL system.runtime.kill_query(query_id => 'query_a', QUERY_ID => 'duplicate')",
        "CALL system.runtime.kill_query(query_id => 'query_a', other => 'unknown')",
        r#"CALL system.runtime.kill_query("query_id" => 'query_a')"#,
    ] {
        let error = admit_trino_statement_in_namespace(
            sql,
            parse_single_statement(sql).unwrap(),
            None,
            None,
        )
        .unwrap_err();
        assert_eq!(
            error.user_error().unwrap().code().as_str(),
            "sql.admit.kill_query_arguments",
            "{sql}"
        );
    }
}
