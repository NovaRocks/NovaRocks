// SPDX-License-Identifier: Apache-2.0

//! The P09 statement gate uses the grammar coverage author's original samples.

use novarocks_parser::{ast::TrinoStatement, parser::parse_statement};
use novarocks_query_application::{
    session_error::QueryServiceError,
    sql::{
        property_evaluation::table,
        trino_admission::{TrinoStatementAction, TrinoStatementPreparation, admit_trino_statement},
        trino_table_command,
    },
    view::ViewCommand,
};
use novarocks_sql::compiler::SqlCompileControl;

const COVERAGE: &str = include_str!("../../../parser/tests/it/grammar-coverage.tsv");
const UNSUPPORTED: &str = "sql.admit.unsupported_statement";
const UNSUPPORTED_CREATE_FORM: &str = "sql.admit.create_table_unsupported_form";

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Owner {
    Root,
    CreateTable,
    Ctas,
    Alter,
    View,
    CreateBranch,
    FastForwardBranch,
    ShowBranches,
    TableExecute,
}

fn statement_witnesses() -> impl Iterator<
    Item = (
        usize,
        &'static str,
        &'static str,
        &'static str,
        &'static str,
    ),
> {
    COVERAGE.lines().enumerate().filter_map(|(line, row)| {
        if row.starts_with('#') {
            return None;
        }
        let fields = row.split('\t').collect::<Vec<_>>();
        assert_eq!(fields.len(), 10, "grammar coverage line {}", line + 1);
        if fields[6] != "parsed-not-supported"
            || !matches!(
                fields[1],
                "statement"
                    | "columnDefinition"
                    | "likeClause"
                    | "explainOption"
                    | "transactionMode"
                    | "levelOfIsolation"
                    | "pathElement"
                    | "pathSpecification"
                    | "privilege"
                    | "privilegeOrRole"
                    | "entityKind"
                    | "ownedEntityKind"
                    | "grantObject"
                    | "grantor"
                    | "principal"
                    | "authorizationUser"
            )
        {
            return None;
        }
        assert!(matches!(fields[0], "alternative" | "optional"), "{row}");
        assert_eq!(fields[7], "accept", "{row}");
        assert!(!fields[5].is_empty(), "{row}");
        Some((line + 1, fields[1], fields[2], fields[4], fields[5]))
    })
}

fn expected_owner(rule: &str, alternative: &str, clause: &str) -> Owner {
    match (rule, alternative) {
        ("columnDefinition" | "likeClause", _) => Owner::CreateTable,
        ("statement", "9") if !matches!(clause, "1" | "g1.b1") => Owner::Ctas,
        ("statement", "15" | "18" | "19" | "20" | "21" | "22" | "25" | "26") => Owner::Alter,
        ("statement", "28") => Owner::TableExecute,
        ("statement", "32") => Owner::View,
        ("statement", "44") => Owner::CreateBranch,
        ("statement", "46") => Owner::FastForwardBranch,
        ("statement", "47") => Owner::ShowBranches,
        _ => Owner::Root,
    }
}

fn expected_subject(rule: &str, alternative: &str) -> &'static str {
    match rule {
        "columnDefinition" | "likeClause" => return "CREATE TABLE",
        "explainOption" => return "EXPLAIN",
        "transactionMode" | "levelOfIsolation" => return "START TRANSACTION",
        "pathElement" | "pathSpecification" => return "SET PATH",
        "privilege" => return "DENY",
        "privilegeOrRole" => return "GRANT",
        "entityKind" | "grantObject" => return "SHOW GRANTS",
        "ownedEntityKind" | "principal" => return "SET AUTHORIZATION",
        "grantor" => return "CREATE ROLE",
        "authorizationUser" => return "SET SESSION AUTHORIZATION",
        "statement" => {}
        _ => panic!("missing statement oracle for {rule}"),
    }
    match alternative {
        "4" => "CREATE CATALOG AUTHORIZATION",
        "5" => "DROP CATALOG CASCADE",
        "6" => "CREATE SCHEMA AUTHORIZATION",
        "8" => "ALTER SCHEMA RENAME",
        "9" => "TABLE AS",
        "10" => "CREATE OR REPLACE TABLE",
        "15" => "COMMENT ON TABLE",
        "16" => "COMMENT ON VIEW",
        "17" => "COMMENT ON MATERIALIZED VIEW",
        "18" => "COMMENT ON COLUMN",
        "19" => "ALTER TABLE RENAME TO",
        "20" => "ADD COLUMN",
        "21" => "RENAME COLUMN",
        "22" => "DROP COLUMN",
        "23" => "ALTER COLUMN SET DEFAULT",
        "24" => "ALTER COLUMN DROP DEFAULT",
        "25" => "SET DATA TYPE",
        "26" => "DROP NOT NULL",
        "29" => "SET AUTHORIZATION",
        "31" => "MATERIALIZED VIEW",
        "32" => "CREATE VIEW",
        "35" => "ALTER MATERIALIZED VIEW RENAME",
        "37" => "ALTER MATERIALIZED VIEW EXECUTE",
        "39" => "ALTER VIEW RENAME",
        "40" => "REFRESH VIEW",
        "42" => "CREATE FUNCTION",
        "43" => "DROP FUNCTION",
        "44" => "CREATE BRANCH FROM",
        "46" => "FAST FORWARD BRANCH",
        "47" => "SHOW BRANCHES",
        "48" => "CREATE ROLE",
        "49" => "DROP ROLE",
        "50" | "51" => "GRANT",
        "52" | "53" => "REVOKE",
        "54" => "DENY",
        "55" => "SET ROLE",
        "56" => "SHOW GRANTS",
        "60" => "SHOW CREATE SCHEMA",
        "62" => "SHOW CREATE MATERIALIZED VIEW",
        "63" => "SHOW CREATE FUNCTION",
        "64" => "SHOW TABLES",
        "65" => "SHOW SCHEMAS",
        "66" => "SHOW CATALOGS",
        "67" | "72" | "73" => "SHOW COLUMNS",
        "69" => "SHOW STATS FOR",
        "70" => "SHOW ROLES",
        "71" => "SHOW ROLE GRANTS",
        "74" => "SHOW FUNCTIONS",
        "75" => "SHOW SESSION",
        "76" => "SET SESSION AUTHORIZATION",
        "77" => "RESET SESSION AUTHORIZATION",
        "80" => "START TRANSACTION",
        "90" => "SET PATH",
        _ => panic!("missing statement oracle for alternative {alternative}"),
    }
}

fn assert_refusal(line: usize, source: &str, owner: Owner, subject: &str) {
    use TrinoStatementAction::Prepare;
    use TrinoStatementPreparation as Prep;

    let statement = parse_statement(source)
        .unwrap_or_else(|error| panic!("coverage line {line} did not parse: {error}\n{source}"));
    let actual = admit_trino_statement(source, statement);
    let control = SqlCompileControl::unbounded();
    let result: Result<(), QueryServiceError> = match (owner, actual) {
        (Owner::Root, result) => result.map(drop),
        (Owner::CreateTable, Ok(Prepare(Prep::CreateTable(node)))) => {
            table::validate_trino_create_table(source, &node, &control)
        }
        (Owner::Ctas, Ok(Prepare(Prep::CreateTableAsSelect(node)))) => {
            table::validate_trino_ctas(source, &node, &control)
        }
        (Owner::View, Ok(Prepare(Prep::CreateView(node)))) => {
            ViewCommand::trino_create(source, &node).map(drop)
        }
        (Owner::Alter, Ok(Prepare(Prep::RenameTable(node)))) => {
            trino_table_command::lower_trino_rename_table(source, &node).map(drop)
        }
        (Owner::Alter, Ok(Prepare(Prep::AddColumn(node)))) => {
            trino_table_command::lower_trino_add_column(
                source,
                &node,
                &novarocks_sql::compiler::SqlCompileControl::unbounded(),
            )
            .map(drop)
        }
        (Owner::Alter, Ok(Prepare(Prep::RenameColumn(node)))) => {
            trino_table_command::lower_trino_rename_column(source, &node).map(drop)
        }
        (Owner::Alter, Ok(Prepare(Prep::DropColumn(node)))) => {
            trino_table_command::lower_trino_drop_column(source, &node).map(drop)
        }
        (Owner::Alter, Ok(Prepare(Prep::SetColumnType(node)))) => {
            trino_table_command::lower_trino_set_column_type(
                source,
                &node,
                &novarocks_sql::compiler::SqlCompileControl::unbounded(),
            )
            .map(drop)
        }
        (Owner::Alter, Ok(Prepare(Prep::DropNotNullConstraint(node)))) => {
            trino_table_command::lower_trino_drop_not_null(source, &node).map(drop)
        }
        (Owner::Alter, Ok(Prepare(Prep::CommentTable(node) | Prep::CommentColumn(node)))) => {
            trino_table_command::lower_trino_table_comment(source, &node).map(drop)
        }
        (Owner::CreateBranch, Ok(Prepare(Prep::CreateBranch(node)))) => {
            trino_table_command::validate_trino_create_branch(source, &node)
        }
        (Owner::FastForwardBranch, Ok(Prepare(Prep::FastForwardBranch(node)))) => {
            trino_table_command::lower_trino_fast_forward_branch(source, &node).map(drop)
        }
        (Owner::ShowBranches, Ok(Prepare(Prep::ShowBranches(node)))) => {
            trino_table_command::lower_trino_show_branches(source, &node).map(drop)
        }
        (_, other) => panic!("coverage line {line} {owner:?} reached {other:?}: {source}"),
    };
    let error = match result {
        Err(error) => error,
        Ok(()) => panic!("coverage line {line} unexpectedly accepted: {source}"),
    };
    let user = error.user_error().unwrap_or_else(|| {
        panic!("coverage line {line} lost the typed refusal: {source}\n{error:?}")
    });
    let expected = match owner {
        Owner::CreateTable | Owner::Ctas => UNSUPPORTED_CREATE_FORM,
        _ => UNSUPPORTED,
    };
    assert_eq!(
        user.code().as_str(),
        expected,
        "coverage line {line}: {source}"
    );
    assert!(
        user.message().to_ascii_uppercase().contains(subject),
        "coverage line {line} expected a refusal naming {subject}: {source}\n{error:?}"
    );
    assert!(
        user.location().is_some(),
        "coverage line {line} lost the source location: {source}"
    );
}

#[test]
fn each_pure_parsed_only_statement_witness_has_its_typed_gate() {
    let mut exercised = 0;
    for (line, rule, alternative, clause, source) in statement_witnesses() {
        let owner = expected_owner(rule, alternative, clause);
        // These cases are named in a separate test with the exact admitted
        // AST family. Their actual provider-based refusal remains outstanding.
        if owner == Owner::TableExecute {
            continue;
        }
        assert_refusal(line, source, owner, expected_subject(rule, alternative));
        exercised += 1;
    }
    assert_eq!(
        exercised, 273,
        "reconcile the statement gate with the grammar ledger"
    );
}

#[test]
fn provider_required_witnesses_preserve_the_original_execute_predicate() {
    let mut required = Vec::new();
    for (line, rule, alternative, clause, source) in statement_witnesses() {
        if expected_owner(rule, alternative, clause) != Owner::TableExecute {
            continue;
        }
        required.push(line);
        let statement = parse_statement(source).unwrap();
        let TrinoStatementAction::Prepare(TrinoStatementPreparation::TableExecute(node)) =
            admit_trino_statement(source, statement).unwrap()
        else {
            panic!("coverage line {line} must retain the TABLE EXECUTE provider requirement");
        };
        assert_eq!(node.procedure_name.value, "optimize_manifests");
        let predicate = node.selection.as_ref().unwrap().span();
        assert_eq!(&source[predicate.start()..predicate.end()], "id > 0");
    }
    assert_eq!(required, [80, 1889]);
}

#[test]
fn uncertain_create_target_precedes_shape_validation() {
    use TrinoStatementAction::Prepare;
    use TrinoStatementPreparation as Prep;

    let source = "CREATE TABLE IF NOT EXISTS lake.ns.t (LIKE lake.ns.source INCLUDING PROPERTIES)";
    assert!(matches!(
        admit_trino_statement(source, parse_statement(source).unwrap()).unwrap(),
        Prepare(Prep::CreateTable(node)) if node.save_mode == novarocks_parser::ast::StatementSaveMode::Ignore
    ));
    let source = "CREATE TABLE IF NOT EXISTS lake.ns.t (renamed) AS SELECT 1 WITH NO DATA";
    assert!(matches!(
        admit_trino_statement(source, parse_statement(source).unwrap()).unwrap(),
        Prepare(Prep::CreateTableAsSelect(node)) if node.save_mode == novarocks_parser::ast::StatementSaveMode::Ignore
    ));
}

#[test]
fn commit_and_rollback_have_their_distinct_transaction_result() {
    for source in ["COMMIT", "COMMIT WORK", "ROLLBACK", "ROLLBACK WORK"] {
        let node = parse_statement(source).unwrap();
        assert!(matches!(
            node,
            TrinoStatement::Commit(_) | TrinoStatement::Rollback(_)
        ));
        let error = admit_trino_statement(source, node).unwrap_err();
        assert_eq!(
            error.user_error().unwrap().code().as_str(),
            "sql.admit.not_in_transaction"
        );
    }
}
