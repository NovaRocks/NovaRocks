// SPDX-License-Identifier: Apache-2.0

use novarocks_parser::ast::{
    Ident, Parameter, Relation, SetExpr, ShowCreateKind, SyntaxEq, TrinoStatement, Visit,
};
use novarocks_parser::{ParserError, SYNTHETIC, Span, parser, printer};

fn roundtrip(source: &str) -> TrinoStatement {
    let original =
        parser::parse_statement(source).unwrap_or_else(|error| panic!("{source}: {error}"));
    let output = printer::print_statement(&original).unwrap();
    let reparsed = parser::parse_statement(&output).unwrap();
    assert!(original.syntax_eq(&reparsed), "{source}\n{output}");
    original
}

#[test]
fn show_create_keeps_every_pinned_kind_even_without_a_product_consumer() {
    let kinds = [
        ("TABLE", ShowCreateKind::Table),
        ("SCHEMA", ShowCreateKind::Schema),
        ("VIEW", ShowCreateKind::View),
        ("MATERIALIZED VIEW", ShowCreateKind::MaterializedView),
        ("FUNCTION", ShowCreateKind::Function),
    ];
    for (label, expected) in kinds {
        let source = format!("  SHOW CREATE {label} lake.\"a.b\"");
        let TrinoStatement::ShowCreate(show) = roundtrip(&source) else {
            panic!("expected SHOW CREATE");
        };
        assert_eq!(show.kind, expected);
        assert_eq!(show.span.start(), 2);
        assert_eq!(show.span.end(), source.len());
        assert_eq!(show.name.parts.len(), 2);
        assert!(show.name.parts[1].quoted);
        assert_eq!(show.name.parts[1].value, "a.b");
        let name_span = show.name.span;
        assert_eq!(&source[name_span.start()..name_span.end()], "lake.\"a.b\"");
    }
    assert!(!roundtrip("SHOW CREATE FUNCTION t").syntax_eq(&roundtrip("SHOW CREATE TABLE t")));
}

#[test]
fn show_stats_distinguishes_a_table_from_the_same_table_inside_a_query() {
    let source = "  SHOW STATS FOR lake.\"a.b\"";
    let table_statement = roundtrip(source);
    let TrinoStatement::ShowStats(show) = &table_statement else {
        panic!("expected SHOW STATS");
    };
    let Relation::Table(table) = &show.relation else {
        panic!("expected the actual direct Table relation");
    };
    assert_eq!(show.span.start(), 2);
    assert_eq!(table.span, SYNTHETIC);
    assert!(table.branch.is_none() && table.query_period.is_none());
    assert_eq!(table.name.parts[1].value, "a.b");
    assert!(table.name.parts[1].quoted);
    let name_span = table.name.span;
    assert_eq!(&source[name_span.start()..name_span.end()], "lake.\"a.b\"");

    let query_statement = roundtrip("SHOW STATS FOR (TABLE lake.\"a.b\")");
    assert!(!table_statement.syntax_eq(&query_statement));
    let TrinoStatement::ShowStats(show) = query_statement else {
        panic!("expected SHOW STATS");
    };
    let Relation::TableSubquery(table) = &show.relation else {
        panic!("expected the actual query relation");
    };
    assert_eq!(table.span, SYNTHETIC);
    assert!(matches!(table.query.body.as_ref(), SetExpr::Table(_)));
}

#[test]
fn show_stats_query_keeps_the_real_root_span_and_its_original_parameter_children() {
    let source = "SHOW STATS FOR (WITH c AS (SELECT ? AS id) SELECT ? AS id FROM c)";
    let statement = roundtrip(source);
    let TrinoStatement::ShowStats(show) = &statement else {
        panic!("expected SHOW STATS");
    };
    let Relation::TableSubquery(table) = &show.relation else {
        panic!("expected a query target");
    };
    let query_span = table.query.span;
    assert_eq!(
        &source[query_span.start()..query_span.end()],
        "WITH c AS (SELECT ? AS id) SELECT ? AS id FROM c",
    );
    assert!(table.query.with.is_some());

    struct Parameters(Vec<(Span, usize)>);
    impl Visit for Parameters {
        fn visit_parameter(&mut self, parameter: &Parameter) {
            self.0.push((parameter.span, parameter.position));
        }
    }
    let mut actual = Parameters(Vec::new());
    actual.visit_trino_statement(&statement);
    actual.0.sort_by_key(|(span, _)| span.start());
    let expected = source
        .match_indices('?')
        .zip([1, 0])
        .map(|((offset, _), position)| (Span::new(offset, offset + 1), position))
        .collect::<Vec<_>>();
    assert_eq!(actual.0, expected);
}

#[test]
fn show_shape_boundaries_refuse_syntax_and_unrepresentable_typed_targets() {
    for source in [
        "SHOW CREATE DATABASE d",
        "SHOW CREATE FUNCTION f(bigint)",
        "SHOW CREATE TABLE",
        "SHOW STATS FOR TABLE t",
        "SHOW STATS FOR t @branch",
        "SHOW STATS FOR t AS alias",
        "SHOW STATS FOR (WITH SESSION p = 1 SELECT 1)",
        "SHOW STATS FOR (SELECT 1);",
    ] {
        assert!(
            matches!(parser::parse_statement(source), Err(ParserError::Parse(_))),
            "{source}",
        );
    }

    let mut statement = parser::parse_statement("SHOW STATS FOR t").unwrap();
    let TrinoStatement::ShowStats(show) = &mut statement else {
        panic!("expected SHOW STATS");
    };
    let Relation::Table(table) = &mut show.relation else {
        panic!("expected a direct Table");
    };
    table.branch = Some(Ident {
        value: "b".to_owned(),
        quoted: false,
        span: SYNTHETIC,
    });
    assert!(matches!(
        printer::print_statement(&statement),
        Err(printer::PrintError::Unrepresentable {
            fact: "a versioned SHOW STATS target",
        }),
    ));
}
