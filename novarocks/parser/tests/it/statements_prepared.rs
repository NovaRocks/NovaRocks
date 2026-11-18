// SPDX-License-Identifier: Apache-2.0

use novarocks_parser::ast::{Expr, SyntaxEq, TrinoStatement};
use novarocks_parser::{trino_parser, trino_printer};

#[test]
fn functions_and_prepared_statements_keep_their_actual_syntax() {
    for sql in [
        "CREATE OR REPLACE FUNCTION c.s.f(x bigint) RETURNS bigint RETURN x",
        "CREATE FUNCTION f(x bigint) RETURNS bigint BEGIN RETURN x; END",
        "DROP FUNCTION IF EXISTS c.s.f(x bigint, varchar)",
        "DROP FUNCTION f()",
        "PREPARE s FROM SELECT ?",
        "PREPARE s FROM INSERT INTO ice.ns.t VALUES ?",
        "DEALLOCATE PREPARE s",
        "EXECUTE s USING ?, 2 + 3",
        "EXECUTE IMMEDIATE USING ?",
        "EXECUTE IMMEDIATE 'SELECT ?' USING ?",
        "DESCRIBE INPUT s",
        "DESCRIBE OUTPUT s",
        "DESCRIBE OUTPUT (WITH q AS (VALUES ?) SELECT ? FROM q)",
    ] {
        let ast =
            trino_parser::parse_statement(sql).unwrap_or_else(|error| panic!("{sql}: {error}"));
        assert_eq!(ast.span().end(), sql.len());
        let printed = trino_printer::print_statement(&ast).unwrap();
        assert!(
            ast.syntax_eq(&trino_parser::parse_statement(&printed).unwrap()),
            "{sql}: {printed}"
        );
    }
}

#[test]
fn immediate_payload_is_text_and_parameters_are_real_expressions() {
    let sql = "EXECUTE IMMEDIATE 'SELECT ?' USING 1 + ?, ?";
    let TrinoStatement::ExecuteImmediate(node) = trino_parser::parse_statement(sql).unwrap() else {
        panic!("execute immediate")
    };
    assert_eq!(node.statement_text, "SELECT ?");
    let Expr::Parameter(last) = &node.parameters[1] else {
        panic!("parameter")
    };
    assert_eq!(last.position, 1);
}

#[test]
fn prepared_statement_boundaries_reject_trailing_or_missing_syntax() {
    for sql in [
        "PREPARE s FROM",
        "EXECUTE s USING",
        "DEALLOCATE s",
        "DROP FUNCTION f",
        "DESCRIBE OUTPUT (WITH SESSION x=1 SELECT 1)",
        "EXECUTE IMMEDIATE 'SELECT 1';",
        "PREPARE s FROM SELECT 1; SELECT 2",
    ] {
        assert!(trino_parser::parse_statement(sql).is_err(), "{sql}");
    }
}
