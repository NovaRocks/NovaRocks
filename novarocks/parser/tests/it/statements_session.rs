// SPDX-License-Identifier: Apache-2.0

use novarocks_parser::ast::{Expr, SyntaxEq, TrinoStatement, Visit};
use novarocks_parser::{trino_parser, trino_printer};

#[test]
fn session_explain_and_transaction_roundtrip() {
    for sql in [
        "USE schema",
        "USE \"catalog\".s",
        "SET SESSION ice.incremental_refresh_enabled = NOT false",
        "RESET SESSION ice.incremental_refresh_enabled",
        "SET SESSION authorization = 1",
        "RESET SESSION authorization.value",
        "SET SESSION AUTHORIZATION \"a user\"",
        "SET SESSION AUTHORIZATION 'a user'",
        "RESET SESSION AUTHORIZATION",
        "EXPLAIN (TYPE LOGICAL, FORMAT JSON, TYPE IO) SELECT 1",
        "EXPLAIN ANALYZE VERBOSE WITH SESSION query_timeout_ms = 1 SELECT 2",
        "EXPLAIN (SELECT 1)",
        "EXPLAIN EXPLAIN SELECT 1",
        "START TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY",
        "START TRANSACTION READ WRITE, ISOLATION LEVEL READ UNCOMMITTED",
        "START TRANSACTION",
        "COMMIT WORK",
        "ROLLBACK WORK",
    ] {
        let ast =
            trino_parser::parse_statement(sql).unwrap_or_else(|error| panic!("{sql}: {error}"));
        assert_eq!(ast.span().start(), 0, "{sql}");
        assert_eq!(ast.span().end(), sql.len(), "{sql}");
        let rendered = trino_printer::print_statement(&ast).unwrap();
        let reparsed = trino_parser::parse_statement(&rendered).unwrap();
        assert!(ast.syntax_eq(&reparsed), "{sql}: {rendered}");
    }
}

#[test]
fn statement_boundary_refuses_separators_and_invalid_options() {
    for sql in [
        "",
        "USE a.b.c",
        "SET SESSION x = 1;",
        "RESET SESSION x SELECT 1",
        "EXPLAIN () SELECT 1",
        "EXPLAIN (FORMAT INVALID) SELECT 1",
        "EXPLAIN (TYPE IO,) SELECT 1",
        "START TRANSACTION READ ONLY,",
        "COMMIT; ROLLBACK",
    ] {
        assert!(trino_parser::parse_statement(sql).is_err(), "{sql}");
    }
}

#[test]
fn completed_statement_uses_the_original_query_parameter_order() {
    #[derive(Default)]
    struct ParameterOrder(Vec<usize>);
    impl Visit for ParameterOrder {
        fn visit_expr(&mut self, node: &Expr) {
            if let Expr::Parameter(parameter) = node {
                self.0.push(parameter.position);
            }
            novarocks_parser::ast::walk_expr(self, node);
        }
    }

    let ast = trino_parser::parse_statement(
        "EXPLAIN ANALYZE WITH SESSION query_timeout_ms = ? SELECT ?, CAST(? AS BIGINT)",
    )
    .unwrap();
    let TrinoStatement::ExplainAnalyze(explain) = &ast else {
        panic!("EXPLAIN ANALYZE")
    };
    let TrinoStatement::Query(query) = explain.statement.as_ref() else {
        panic!("query")
    };
    let Expr::Parameter(parameter) = &query.session_properties[0].value else {
        panic!("parameter")
    };
    assert_eq!(parameter.position, 2);
    let mut positions = ParameterOrder::default();
    positions.visit_trino_statement(&ast);
    positions.0.sort_unstable();
    assert_eq!(positions.0, [0, 1, 2]);
    let rendered = trino_printer::print_statement(&ast).unwrap();
    assert!(ast.syntax_eq(&trino_parser::parse_statement(&rendered).unwrap()));
}

#[test]
fn explain_prefers_the_complete_inner_analyze_statement() {
    let statement = trino_parser::parse_statement("EXPLAIN ANALYZE verbose").unwrap();
    let TrinoStatement::Explain(explain) = statement else {
        panic!("plain EXPLAIN")
    };
    assert!(matches!(&*explain.statement, TrinoStatement::Analyze(_)));
    let statement = trino_parser::parse_statement("EXPLAIN ANALYZE ANALYZE t").unwrap();
    let TrinoStatement::ExplainAnalyze(explain) = statement else {
        panic!("EXPLAIN ANALYZE")
    };
    assert!(matches!(&*explain.statement, TrinoStatement::Analyze(_)));
}
