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
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};

use novarocks_query_application::{
    session_effects::{PreparedStatementEffect, SessionEffects},
    session_error::QueryServiceError,
    session_value::{PreparedStatementDefinition, PreparedStatementSnapshot},
    sql::{
        parse_single_statement,
        trino_admission::{
            AdmittedTrinoStatement, TrinoDescriptionKind, TrinoQueryAction, TrinoSessionAction,
            TrinoStatementAction, TrinoStatementPreparation, TrinoStatementSource,
            admit_trino_statement_request,
        },
    },
    statement_kind::StatementKind,
};

fn admit(
    sql: &str,
    snapshot: &PreparedStatementSnapshot,
) -> Result<AdmittedTrinoStatement, QueryServiceError> {
    admit_trino_statement_request(
        Arc::from(sql),
        parse_single_statement(sql).unwrap(),
        snapshot,
    )
}

fn one_definition(name: &str, sql: &str) -> PreparedStatementSnapshot {
    PreparedStatementSnapshot::new(
        vec![(
            name.into(),
            PreparedStatementDefinition::new(Arc::from(sql), ()),
        )],
        (),
    )
    .unwrap()
}

fn error_code(error: &QueryServiceError) -> &str {
    error.user_error().unwrap().code().as_str()
}

#[test]
fn prepare_and_deallocate_publish_the_original_name_and_replayable_definition() {
    let original = one_definition("a,=+é", "SELECT 11");
    let admitted = admit(
        r#"PREPARE "a,=+é" FROM SELECT /* retained */ ? FROM absent_table"#,
        &original,
    )
    .unwrap();
    let TrinoStatementAction::Session(TrinoSessionAction::Prepare { name, sql }) = admitted.action
    else {
        panic!("expected PREPARE session action");
    };
    assert_eq!(name, "a,=+é");
    assert_eq!(sql.as_ref(), "SELECT /* retained */ ? FROM absent_table");
    assert!(admitted.parameters.is_none());
    parse_single_statement(&sql).unwrap();
    assert!(original.get("a,=+é").is_some());
    assert_eq!(original.get("a,=+é").unwrap().sql().as_ref(), "SELECT 11");

    let mut effects = SessionEffects::prepare(name.clone(), sql.clone());
    assert!(!effects.is_empty());
    assert_eq!(
        effects.prepared_statements().cloned().collect::<Vec<_>>(),
        [PreparedStatementEffect::Added { name, sql }],
    );

    let deallocate = admit(r#"DEALLOCATE PREPARE "a,=+é""#, &original).unwrap();
    let TrinoStatementAction::Session(TrinoSessionAction::DeallocatePrepare { name }) =
        deallocate.action
    else {
        panic!("expected DEALLOCATE PREPARE session action");
    };
    effects.append(SessionEffects::deallocate_prepare(name.clone()));
    assert_eq!(
        effects.prepared_statements().last(),
        Some(&PreparedStatementEffect::Removed { name }),
    );
    assert!(!StatementKind::Prepare.is_row_kind());
    assert!(!StatementKind::DeallocatePrepare.is_row_kind());
}

#[test]
fn named_execution_uses_an_exact_request_snapshot_and_the_effective_owner() {
    let insertion = one_definition("p", "INSERT INTO test_table VALUES (?)");
    let old = one_definition("p", "SELECT 11");
    let new = one_definition("p", "SELECT 22");
    for (snapshot, expected) in [(&old, "SELECT 11"), (&new, "SELECT 22")] {
        let result = admit("EXECUTE p", snapshot).unwrap();
        assert_eq!(result.source.sql().as_ref(), expected);
        assert!(matches!(
            result.action,
            TrinoStatementAction::Query(TrinoQueryAction::Read(_))
        ));
    }
    let result = admit("EXECUTE p USING CAST(NULL AS BIGINT)", &insertion).unwrap();
    assert_eq!(
        result.source.sql().as_ref(),
        "INSERT INTO test_table VALUES (?)"
    );
    assert!(matches!(
        result.action,
        TrinoStatementAction::Prepare(TrinoStatementPreparation::Insert(_))
    ));
    let parameters = result.parameters.unwrap();
    assert_eq!(parameters.positions().len(), 1);
    let arg = parameters.argument_at(parameters.positions()[0]).unwrap();
    let arg_sql = parameters.argument_sql().unwrap();
    assert_eq!(
        &arg_sql[arg.span().start()..arg.span().end()],
        "CAST(NULL AS BIGINT)"
    );
    for sql in [
        "EXECUTE P",
        "DEALLOCATE PREPARE P",
        "DESCRIBE INPUT P",
        "DESCRIBE OUTPUT P",
    ] {
        let error = admit(sql, &insertion).unwrap_err();
        assert_eq!(
            error_code(&error),
            "sql.admit.prepared_statement_not_found",
            "{sql}"
        );
        assert!(error.user_error().unwrap().location().is_none());
    }
}

#[test]
fn description_keeps_unbound_parameters_and_the_original_target_action() {
    let write = one_definition("p", "INSERT INTO test_table VALUES (?)");
    for (sql, expected) in [
        ("DESCRIBE INPUT p", TrinoDescriptionKind::Input),
        ("DESCRIBE OUTPUT p", TrinoDescriptionKind::Output),
    ] {
        let result = admit(sql, &write).unwrap();
        let parameters = result.parameters.unwrap();
        assert!(parameters.is_description());
        assert_eq!(parameters.positions().len(), 1);
        let TrinoStatementAction::Describe(action) = result.action else {
            panic!("expected description action");
        };
        assert_eq!(action.kind, expected);
        assert!(matches!(
            *action.target,
            TrinoStatementAction::Prepare(TrinoStatementPreparation::Insert(_))
        ));
    }

    let sql = "DESCRIBE OUTPUT (WITH t(x) AS (VALUES ?) SELECT x, ? FROM t)";
    let result = admit(sql, &PreparedStatementSnapshot::default()).unwrap();
    assert_eq!(result.source.sql().as_ref(), sql);
    let parameters = result.parameters.unwrap();
    assert!(parameters.is_description());
    assert_eq!(parameters.positions().len(), 2);
    assert!(StatementKind::DescribeInput.is_row_kind());
    assert!(StatementKind::DescribeOutput.is_row_kind());
}

#[test]
fn application_distinguishes_bad_bindings_and_persistent_definitions() {
    let empty = PreparedStatementSnapshot::default();
    for (sql, code) in [
        ("SELECT ?", "sql.admit.invalid_parameter_usage"),
        (
            "EXECUTE IMMEDIATE 'SELECT ?, ?' USING 1",
            "sql.admit.invalid_parameter_usage",
        ),
        (
            "EXECUTE IMMEDIATE 'SELECT ?' USING 1, 2",
            "sql.admit.invalid_parameter_usage",
        ),
        (
            "EXECUTE IMMEDIATE 'SELECT ?' USING ?",
            "sql.admit.invalid_parameter_usage",
        ),
        (
            "EXECUTE IMMEDIATE 'SELECT ?' USING x",
            "sql.admit.expression_not_constant",
        ),
        (
            "PREPARE p FROM PREPARE q FROM SELECT 1",
            "sql.admit.unsupported_statement",
        ),
        (
            "PREPARE p FROM EXECUTE q",
            "sql.admit.unsupported_statement",
        ),
        (
            "PREPARE p FROM DEALLOCATE PREPARE q",
            "sql.admit.unsupported_statement",
        ),
        (
            "CREATE VIEW v AS SELECT ?",
            "sql.admit.unsupported_statement",
        ),
        (
            "CREATE MATERIALIZED VIEW v AS SELECT ?",
            "sql.admit.unsupported_statement",
        ),
        (
            "EXECUTE IMMEDIATE 'CREATE VIEW v AS SELECT ?' USING 1",
            "sql.admit.unsupported_statement",
        ),
        ("SET PATH lake.s", "sql.admit.unsupported_statement"),
    ] {
        assert_eq!(error_code(&admit(sql, &empty).unwrap_err()), code, "{sql}");
    }
    for sql in [
        "CREATE VIEW v AS SELECT 1",
        "CREATE MATERIALIZED VIEW v AS SELECT 1",
        "EXECUTE IMMEDIATE 'SELECT ?' USING CAST(NULL AS BIGINT)",
    ] {
        admit(sql, &empty).unwrap();
    }
}

#[test]
fn diagnostics_carry_the_source_of_the_reported_coordinate() {
    let outer = "/* é😀 */ EXECUTE IMMEDIATE 'SELECT ?, ?' USING 1";
    let missing = admit(outer, &PreparedStatementSnapshot::default()).unwrap_err();
    let source = missing.trino_statement_source().unwrap();
    assert_eq!(source.request_sql().as_ref(), outer);
    assert_eq!(source.sql().as_ref(), "SELECT ?, ?");
    let anchor = source.immediate_literal_span().unwrap();
    assert_eq!(&outer[anchor.start()..anchor.end()], "'SELECT ?, ?'");
    assert_eq!(
        missing.user_error().unwrap().location().unwrap().column(),
        1
    );

    let outer = "EXECUTE p
USING /* é😀 */ missing_column";
    let supplied = admit(outer, &one_definition("p", "SELECT ?")).unwrap_err();
    let source = supplied.trino_statement_source().unwrap();
    assert_eq!(source.sql().as_ref(), outer);
    assert!(source.immediate_literal_span().is_none());
    assert_eq!(supplied.user_error().unwrap().location().unwrap().line(), 2);
    let reused =
        supplied.with_trino_statement_source(TrinoStatementSource::request(Arc::from("SELECT ?")));
    assert_eq!(
        reused.trino_statement_source().unwrap().sql().as_ref(),
        outer
    );

    let named = admit(
        "EXECUTE p",
        &one_definition(
            "p",
            "
 SELECT ?",
        ),
    )
    .unwrap_err();
    let source = named.trino_statement_source().unwrap();
    assert_eq!(
        source.sql().as_ref(),
        "
 SELECT ?"
    );
    assert_eq!(source.request_sql().as_ref(), "EXECUTE p");
    assert!(source.immediate_literal_span().is_none());
    assert_eq!(named.user_error().unwrap().location().unwrap().line(), 2);
}

struct AllocationDrop(Arc<AtomicUsize>);

impl Drop for AllocationDrop {
    fn drop(&mut self) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}

#[test]
fn selected_named_source_retains_only_its_definition_after_snapshot_exit() {
    let mapping = Arc::new(AtomicUsize::new(0));
    let selected = Arc::new(AtomicUsize::new(0));
    let other = Arc::new(AtomicUsize::new(0));
    let snapshot = PreparedStatementSnapshot::new(
        vec![
            (
                "selected".into(),
                PreparedStatementDefinition::new(
                    Arc::from("SELECT ?"),
                    AllocationDrop(selected.clone()),
                ),
            ),
            (
                "other".into(),
                PreparedStatementDefinition::new(
                    Arc::from("SELECT ?"),
                    AllocationDrop(other.clone()),
                ),
            ),
        ],
        AllocationDrop(mapping.clone()),
    )
    .unwrap();
    let error = admit("EXECUTE selected", &snapshot).unwrap_err();
    let error_alias = error.clone();
    drop(snapshot);
    assert_eq!(mapping.load(Ordering::SeqCst), 1);
    assert_eq!(other.load(Ordering::SeqCst), 1);
    assert_eq!(selected.load(Ordering::SeqCst), 0);
    drop(error);
    assert_eq!(selected.load(Ordering::SeqCst), 0);
    drop(error_alias);
    assert_eq!(selected.load(Ordering::SeqCst), 1);
}
