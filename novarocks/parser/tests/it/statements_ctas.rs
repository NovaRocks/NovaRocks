// SPDX-License-Identifier: Apache-2.0

use novarocks_parser::ast::{
    Expr, Parameter, SetExpr, StatementSaveMode, SyntaxEq, TrinoStatement, Visit,
};
use novarocks_parser::{ParseError, ParserError, Span, parser, printer};

fn roundtrip(source: &str) -> TrinoStatement {
    let original =
        parser::parse_statement(source).unwrap_or_else(|error| panic!("{source}: {error}"));
    let printed =
        printer::print_statement(&original).unwrap_or_else(|error| panic!("{original:?}: {error}"));
    let reparsed =
        parser::parse_statement(&printed).unwrap_or_else(|error| panic!("{printed}: {error}"));
    assert!(original.syntax_eq(&reparsed), "{source}\n{printed}");
    original
}

fn assert_parameters(statement: &TrinoStatement, source: &str, positions: &[usize]) {
    struct Parameters(Vec<(Span, usize)>);
    impl Visit for Parameters {
        fn visit_parameter(&mut self, value: &Parameter) {
            self.0.push((value.span, value.position));
        }
    }
    let mut actual = Parameters(Vec::new());
    actual.visit_trino_statement(statement);
    actual.0.sort_by_key(|(span, _)| span.start());
    let expected = source
        .match_indices('?')
        .map(|(offset, _)| Span::new(offset, offset + 1))
        .zip(positions.iter().copied())
        .collect::<Vec<_>>();
    assert_eq!(actual.0, expected);
}

#[test]
fn ctas_owns_the_real_query_span_and_following_no_data() {
    let source = "  CREATE TABLE IF NOT EXISTS lake.ns.t (\"A.B\", b) COMMENT 'a''b' WITH (\"k.opt\" = ?, reset = DEFAULT) AS (SELECT ? AS x, ? AS y) WITH NO DATA";
    let statement = roundtrip(source);
    let TrinoStatement::CreateTableAsSelect(ctas) = &statement else {
        panic!("expected CTAS");
    };
    assert_eq!(ctas.save_mode, StatementSaveMode::Ignore);
    assert_eq!(ctas.name.parts[2].value, "t");
    assert_eq!(ctas.comment.as_deref(), Some("a'b"));
    assert!(!ctas.with_data);
    assert_eq!(ctas.span.start(), 2);
    assert_eq!(ctas.span.end(), source.len());
    let [quoted, plain] = ctas.column_aliases.as_deref().unwrap() else {
        panic!("expected two aliases");
    };
    assert_eq!(quoted.value, "A.B");
    assert!(quoted.quoted);
    assert_eq!(&source[quoted.span.start()..quoted.span.end()], "\"A.B\"");
    assert_eq!(plain.value, "b");
    assert!(!plain.quoted);
    assert_eq!(ctas.properties[0].name.value, "k.opt");
    assert!(ctas.properties[0].name.quoted);
    assert!(
        matches!(&ctas.properties[0].value, Some(Expr::Parameter(value)) if value.position == 0)
    );
    assert!(ctas.properties[1].value.is_none());

    let query_span = ctas.query.span;
    assert_eq!(
        &source[query_span.start()..query_span.end()],
        "(SELECT ? AS x, ? AS y)"
    );
    let SetExpr::Query(parenthesized) = ctas.query.body.as_ref() else {
        panic!("the first rootQuery alternative owns a TableSubquery");
    };
    let inner = parenthesized.query.span;
    assert_eq!(&source[inner.start()..inner.end()], "SELECT ? AS x, ? AS y");
    assert_parameters(&statement, source, &[0, 1, 2]);
}

#[test]
fn ctas_parentheses_do_not_flatten_a_set_input_or_wrap_a_cte_root() {
    let source =
        "CREATE TABLE t WITH (p = ?) AS (WITH c AS (SELECT ? AS x) SELECT ? AS x FROM c) WITH DATA";
    let cte_statement = roundtrip(source);
    let TrinoStatement::CreateTableAsSelect(cte) = &cte_statement else {
        panic!("expected CTAS");
    };
    assert!(cte.with_data);
    assert!(cte.column_aliases.is_none());
    assert!(cte.query.with.is_some());
    assert!(matches!(cte.query.body.as_ref(), SetExpr::Select(_)));
    let span = cte.query.span;
    assert_eq!(
        &source[span.start()..span.end()],
        "WITH c AS (SELECT ? AS x) SELECT ? AS x FROM c"
    );
    // The table property is visited first. Within rootQuery, its body precedes its CTE.
    assert_parameters(&cte_statement, source, &[0, 2, 1]);

    let inline_function =
        roundtrip("CREATE TABLE t AS (WITH FUNCTION f() RETURNS BIGINT RETURN 1 SELECT 1 AS x)");
    let TrinoStatement::CreateTableAsSelect(inline) = inline_function else {
        panic!("expected CTAS");
    };
    assert_eq!(inline.query.functions.len(), 1);
    assert!(matches!(inline.query.body.as_ref(), SetExpr::Select(_)));

    let union = roundtrip("CREATE TABLE t AS (SELECT 1 AS x) UNION ALL SELECT 2 AS x");
    let TrinoStatement::CreateTableAsSelect(union) = union else {
        panic!("expected CTAS");
    };
    let SetExpr::SetOperation(set) = union.query.body.as_ref() else {
        panic!("the parentheses enclose only the left UNION input");
    };
    assert!(matches!(&set.inputs[0], SetExpr::Query(_)));
}

#[test]
fn ctas_and_ordinary_table_own_distinct_complete_alternatives() {
    assert!(matches!(
        roundtrip("CREATE TABLE t (id bigint)"),
        TrinoStatement::CreateTable(_)
    ));
    assert!(matches!(
        roundtrip("CREATE TABLE t (id) AS SELECT 1"),
        TrinoStatement::CreateTableAsSelect(_)
    ));
    let default_data = roundtrip("CREATE TABLE t AS SELECT 1 AS id");
    let explicit_data = roundtrip("CREATE TABLE t AS SELECT 1 AS id WITH DATA");
    let no_data = roundtrip("CREATE TABLE t AS SELECT 1 AS id WITH NO DATA");
    assert!(default_data.syntax_eq(&explicit_data));
    assert!(!no_data.syntax_eq(&default_data));
    assert!(matches!(
        roundtrip("CREATE OR REPLACE TABLE t AS SELECT 1 AS id"),
        TrinoStatement::CreateTableAsSelect(node) if node.save_mode == StatementSaveMode::Replace
    ));

    let source = "  CREATE OR REPLACE TABLE IF NOT EXISTS t AS SELECT 9223372036854775808";
    let error = parser::parse_statement(source).unwrap_err();
    let ParserError::Parse(ParseError::UnexpectedToken { span, found, .. }) = error else {
        panic!("expected the save-mode parse error");
    };
    assert_eq!(&source[span.start()..span.end()], "CREATE");
    assert_eq!(found, "OR REPLACE and IF NOT EXISTS");

    let source =
        "CREATE OR REPLACE TABLE IF NOT EXISTS t WITH (x = 9223372036854775808) AS SELECT 1";
    let error = parser::parse_statement(source).unwrap_err();
    let ParserError::Parse(ParseError::UnexpectedToken { span, .. }) = error else {
        panic!("expected the property literal parse error");
    };
    assert_eq!(&source[span.start()..span.end()], "9223372036854775808");

    for source in [
        "CREATE TABLE t () AS SELECT 1",
        "CREATE TABLE t (id bigint) AS SELECT 1",
        "CREATE TABLE t (id) SELECT 1",
        "CREATE TABLE t AS (SELECT 1 WITH NO DATA)",
        "CREATE TABLE t AS WITH SESSION x = 1 SELECT 1",
        "CREATE TABLE t AS (WITH SESSION x = 1 SELECT 1)",
        "CREATE TABLE t AS SELECT 1 WITH NO",
        "CREATE TABLE t AS SELECT 1 WITH DATA WITH NO DATA",
        "CREATE TABLE t AS SELECT 1;",
    ] {
        assert!(parser::parse_statement(source).is_err(), "{source}");
    }
}

#[test]
fn ctas_printer_refuses_fields_outside_its_grammar() {
    let mut alias_stmt = parser::parse_statement("CREATE TABLE t AS SELECT 1").unwrap();
    let TrinoStatement::CreateTableAsSelect(alias_node) = &mut alias_stmt else {
        panic!("expected CTAS");
    };
    alias_node.column_aliases = Some(Vec::new());
    assert!(matches!(
        printer::print_statement(&alias_stmt),
        Err(printer::PrintError::Unrepresentable {
            fact: "an empty CTAS column alias list",
        })
    ));

    let mut session_stmt = parser::parse_statement("CREATE TABLE t AS SELECT 1").unwrap();
    let TrinoStatement::CreateTableAsSelect(session_node) = &mut session_stmt else {
        panic!("expected CTAS");
    };
    let session_query = parser::parse_query("WITH SESSION p = 1 SELECT 1").unwrap();
    session_node.query.session_properties = session_query.session_properties;
    assert!(matches!(
        printer::print_statement(&session_stmt),
        Err(printer::PrintError::Unrepresentable {
            fact: "WITH SESSION in a CTAS query",
        })
    ));
}
