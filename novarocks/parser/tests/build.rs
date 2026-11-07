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

//! Syntax constructors share the parser's shapes, using fixed SQL as the oracle.
//! These pure tests need no fixture or process and run with the parser package.

use novarocks_parser::{
    SYNTHETIC, Span,
    ast::{self, BinaryOperator, Expr, GroupBy, JoinConstraint, JoinOperator, SetExpr, SyntaxEq},
    build::{self, BuildError},
};

fn expression(source: &str) -> Expr {
    let tokens = novarocks_parser::lex(source).expect("expression must lex");
    novarocks_parser::parser::parse_expression(source, &tokens).expect("expression must parse")
}

fn query(source: &str) -> ast::Query {
    let mut statements = novarocks_parser::parse(source).expect("query must parse");
    let [ast::Statement::Query(_)] = statements.as_slice() else {
        panic!("expected exactly one query");
    };
    let ast::Statement::Query(query) = statements.pop().expect("one query") else {
        unreachable!();
    };
    query
}

fn name(value: &str) -> ast::ObjectName {
    build::object_name(vec![build::ident(value, None)]).expect("one identifier")
}

fn id(value: &str) -> Expr {
    build::identifier(build::ident(value, None))
}

fn assert_expression(built: Expr, source: &str) {
    let parsed = expression(source);
    assert!(
        built.syntax_eq(&parsed),
        "built={built:#?}\nparsed={parsed:#?}"
    );
}

fn select_body(value: i64) -> SetExpr {
    SetExpr::Select(Box::new(
        build::select(
            vec![build::item(build::integer(value), None, false)],
            Vec::new(),
            None,
            GroupBy::None,
        )
        .expect("nonempty projection"),
    ))
}

#[test]
fn identifiers_literals_and_unary_nodes_match_parser_shapes() {
    assert_expression(id("field"), "field");
    assert_expression(build::identifier(build::ident("a b", Some('`'))), "`a b`");
    assert_expression(build::identifier(build::ident("a`b", Some('`'))), "`a``b`");
    assert_expression(
        build::column(build::ident("a", None), build::ident("k1", Some('`'))),
        "a.`k1`",
    );
    for value in [
        "0",
        "-1",
        "-9223372036854775808",
        "+1",
        "01.5e+2",
        "-1.25",
        "-1e3",
    ] {
        assert_expression(
            build::number(value).expect("admitted numeric spelling"),
            value,
        );
    }
    assert_expression(build::integer(i64::MIN), "-9223372036854775808");
    assert_expression(build::integer(i64::MAX), "9223372036854775807");
    assert_expression(build::string("a\\b'c"), "'a\\\\b''c'");
    assert_expression(build::hex_string("aB01"), "X'aB01'");
    assert_expression(build::boolean(true), "TRUE");
    assert_expression(build::boolean(false), "FALSE");
    assert_expression(build::null(), "NULL");
    assert_expression(build::nested(id("x")), "(x)");
    assert_expression(build::not(id("x")), "NOT x");
    assert_expression(
        build::unary(ast::UnaryOperator::Minus, build::integer(1)).expect("signed integer"),
        "-1",
    );
    assert_expression(
        build::unary(ast::UnaryOperator::Minus, build::integer(-1)).expect("nested minus"),
        "- -1",
    );
    assert_expression(
        build::unary(ast::UnaryOperator::BitwiseNot, id("x")).expect("bitwise not"),
        "~x",
    );
    assert!(matches!(
        build::number("9223372036854775808"),
        Err(BuildError::Number(_))
    ));
}

#[test]
fn type_nodes_preserve_nested_arguments_and_comma_trivia() {
    let parameter = |value: i64| {
        ast::TypeNameArgument::Literal(ast::Literal {
            kind: ast::LiteralKind::Number(value.to_string()),
            span: SYNTHETIC,
        })
    };
    for spaced in [false, true] {
        let decimal = build::type_name(
            name("DECIMAL"),
            vec![parameter(12), parameter(2)],
            vec![spaced],
        )
        .expect("valid type arguments");
        let separator = if spaced { ", " } else { "," };
        assert_expression(
            build::cast(build::null(), decimal),
            &format!("CAST(NULL AS DECIMAL(12{separator}2))"),
        );
    }
    let field = ast::StructField {
        name: build::ident("a b", Some('`')),
        data_type: build::simple_type("INT"),
        span: SYNTHETIC,
    };
    let structure = build::type_name(
        name("STRUCT"),
        vec![ast::TypeNameArgument::Field(field)],
        Vec::new(),
    )
    .expect("struct field");
    let array = build::type_name(
        name("ARRAY"),
        vec![ast::TypeNameArgument::Type(structure)],
        Vec::new(),
    )
    .expect("array element");
    let map = build::type_name(
        name("MAP"),
        vec![
            ast::TypeNameArgument::Type(build::simple_type("STRING")),
            ast::TypeNameArgument::Type(array),
        ],
        vec![true],
    )
    .expect("map arguments");
    assert_expression(
        build::cast(build::null(), map),
        "CAST(NULL AS MAP<STRING, ARRAY<STRUCT<`a b` INT>>>)",
    );
}

#[test]
fn expression_combinations_match_parser_without_reparsing_children() {
    assert_expression(
        build::binary(id("a"), BinaryOperator::Add, build::integer(2)),
        "a + 2",
    );
    assert_expression(build::is_null(id("a"), false), "a IS NULL");
    assert_expression(build::is_null(id("a"), true), "a IS NOT NULL");
    assert_expression(
        build::case_when(
            vec![
                (build::is_null(id("a"), false), build::integer(1)),
                (build::is_null(id("b"), true), build::integer(2)),
            ],
            Some(build::integer(0)),
        )
        .expect("nonempty paired CASE branches"),
        "CASE WHEN a IS NULL THEN 1 WHEN b IS NOT NULL THEN 2 ELSE 0 END",
    );
    assert_expression(
        build::function(name("f"), vec![id("a"), build::integer(1)]),
        "f(a, 1)",
    );
    assert_expression(build::function(name("f"), Vec::new()), "f()");
    assert_expression(build::count_star(), "COUNT(*)");
    assert_expression(
        build::array(vec![build::integer(1), build::null()]),
        "[1, NULL]",
    );
    assert_expression(build::array(Vec::new()), "[]");
    assert_expression(
        build::tuple(vec![build::integer(1)]).expect("single parentheses"),
        "(1)",
    );
    assert_expression(
        build::tuple(vec![build::integer(1), build::string("x")]).expect("tuple"),
        "(1, 'x')",
    );
    assert_expression(
        build::map(vec![(build::string("x"), build::integer(1))]),
        "MAP{'x': 1}",
    );
    assert_expression(build::map(Vec::new()), "MAP{}");
}

#[test]
fn boolean_chains_reuse_parser_balancing_and_preserve_operand_order() {
    for length in [1, 4, 5] {
        let operands = (0..length)
            .map(|index| id(&format!("x{index}")))
            .collect::<Vec<_>>();
        let sources = (0..length)
            .map(|index| format!("x{index}"))
            .collect::<Vec<_>>();
        assert_expression(
            build::and_all(operands.clone()).expect("operands"),
            &sources.join(" AND "),
        );
        assert_expression(
            build::or_all(operands).expect("operands"),
            &sources.join(" OR "),
        );
    }
    let built = build::binary(
        build::binary(id("a"), BinaryOperator::And, id("b")),
        BinaryOperator::And,
        build::binary(id("c"), BinaryOperator::And, id("d")),
    );
    assert_expression(built, "a AND b AND c AND d");
    assert!(build::and_all(Vec::new()).is_none());
    assert!(build::or_all(Vec::new()).is_none());
}

#[test]
fn select_relation_aliases_and_grouping_match_parser() {
    let source_name = build::object_name(vec![
        build::ident("cat", Some('`')),
        build::ident("db", Some('`')),
        build::ident("source", Some('`')),
    ])
    .expect("qualified name");
    let left = build::relation(build::table(
        source_name,
        Some(ast::TableVersion {
            kind: ast::TableVersionKind::ForVersionAsOf,
            value: build::string("branch\\tail"),
            span: SYNTHETIC,
        }),
        Some(build::alias(build::ident("s", None), Vec::new(), true)),
    ));
    let derived = build::derived(
        build::query(build::values(vec![vec![build::integer(1)]]).expect("values")),
        Some(build::alias(
            build::ident("v", None),
            vec![build::ident("k1", None)],
            false,
        )),
    );
    let joined = build::join(
        left,
        JoinOperator::LeftOuter,
        derived,
        JoinConstraint::On(build::binary(
            build::column(build::ident("s", None), build::ident("k1", None)),
            BinaryOperator::Equal,
            build::column(build::ident("v", None), build::ident("k1", None)),
        )),
    );
    let column = build::column(build::ident("s", None), build::ident("k1", None));
    let select = build::select(
        vec![
            build::item(column.clone(), Some(build::ident("k", None)), true),
            build::item(build::count_star(), Some(build::ident("n", None)), false),
        ],
        vec![joined],
        Some(build::is_null(column.clone(), true)),
        build::group_by(vec![column]),
    )
    .expect("nonempty SELECT");
    let built = build::query(SetExpr::Select(Box::new(select)));
    let parsed = query(
        "SELECT s.k1 AS k, COUNT(*) n FROM `cat`.`db`.`source` FOR VERSION AS OF 'branch\\\\tail' AS s \
         LEFT JOIN (VALUES (1)) v(k1) ON s.k1 = v.k1 WHERE s.k1 IS NOT NULL GROUP BY s.k1",
    );
    assert!(
        built.syntax_eq(&parsed),
        "built={built:#?}\nparsed={parsed:#?}"
    );

    let select = build::select(
        vec![
            build::wildcard(),
            build::qualified_wildcard(vec![build::ident("t", None)]).expect("prefix"),
        ],
        vec![build::relation(build::table(name("t"), None, None))],
        None,
        build::group_by(Vec::new()),
    )
    .expect("wildcard SELECT");
    assert!(
        build::query(SetExpr::Select(Box::new(select))).syntax_eq(&query("SELECT *, t.* FROM t"))
    );
}

#[test]
fn values_and_union_shapes_match_parser_balancing() {
    let values = build::values(vec![
        vec![build::integer(1), build::string("a")],
        vec![build::integer(2), build::string("b")],
    ])
    .expect("values");
    assert!(build::query(values).syntax_eq(&query("VALUES (1, 'a'), (2, 'b')")));
    for length in [1, 4, 5] {
        let body =
            build::union_all((0..length).map(select_body).collect()).expect("union operands");
        let source = (0..length)
            .map(|value| format!("SELECT {value}"))
            .collect::<Vec<_>>()
            .join(" UNION ALL ");
        assert!(build::query(body).syntax_eq(&query(&source)));
    }
}

#[test]
fn derived_expressions_retain_source_locations_and_envelopes_are_synthetic() {
    let source_span = Span::new(50, 65);
    let source = build::identifier(build::ident_at("user_value", None, source_span));
    let cast = build::cast(source.clone(), build::simple_type("INT"));
    assert_eq!(cast.span(), source_span);
    assert_eq!(build::nested(cast.clone()).span(), source_span);
    assert_eq!(build::not(source.clone()).span(), source_span);
    assert_eq!(build::is_null(source.clone(), true).span(), source_span);
    assert_eq!(
        build::binary(source.clone(), BinaryOperator::Add, build::integer(1)).span(),
        source_span
    );
    assert_eq!(
        build::and_all(vec![source.clone(), build::boolean(true)])
            .expect("chain")
            .span(),
        source_span
    );
    let built = build::query(SetExpr::Select(Box::new(
        build::select(
            vec![build::item(cast, None, false)],
            Vec::new(),
            None,
            GroupBy::None,
        )
        .expect("SELECT"),
    )));
    assert_eq!(built.span, SYNTHETIC);
    assert_eq!(built.body.span(), SYNTHETIC);
    assert_eq!(Span::new(100, 105).cover(source_span), Span::new(50, 105));
    assert_eq!(SYNTHETIC.cover(source_span), source_span);
    assert_eq!(source_span.cover(SYNTHETIC), source_span);
    assert_eq!(Span::new(7, 7).cover(SYNTHETIC), SYNTHETIC);

    let mut user_body = select_body(1);
    let SetExpr::Select(select) = &mut user_body else {
        unreachable!()
    };
    select.span = source_span;
    assert_eq!(
        build::union_all(vec![user_body, select_body(2)])
            .expect("union")
            .span(),
        source_span
    );
}

#[test]
fn builders_reject_missing_required_structure() {
    assert_eq!(
        build::object_name(Vec::new()),
        Err(BuildError::EmptyObjectName)
    );
    assert_eq!(
        build::select(Vec::new(), Vec::new(), None, GroupBy::None),
        Err(BuildError::EmptyProjection)
    );
    assert_eq!(build::values(Vec::new()), Err(BuildError::EmptyValues));
    assert_eq!(
        build::values(vec![Vec::new()]),
        Err(BuildError::EmptyValuesRow)
    );
    assert_eq!(
        build::case_when(Vec::new(), None),
        Err(BuildError::EmptyCase)
    );
    assert_eq!(build::tuple(Vec::new()), Err(BuildError::EmptyTuple));
    assert_eq!(build::union_all(Vec::new()), Err(BuildError::EmptyUnion));
    assert_eq!(
        build::qualified_wildcard(Vec::new()),
        Err(BuildError::EmptyWildcardPrefix)
    );
    assert_eq!(
        build::type_name(name("INT"), Vec::new(), vec![false]),
        Err(BuildError::TypeArgumentSeparators {
            arguments: 0,
            separators: 1
        }),
    );
}
