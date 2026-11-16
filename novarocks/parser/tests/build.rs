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

//! Source-shaped constructors use fixed SQL as the oracle; domain-authored
//! values preserve their original AST facts without source lexing.
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
fn query_suffixes_keep_specification_and_parenthesized_owners_distinct() {
    use ast::{Literal, LiteralKind, Visit};
    for source in [
        "SELECT 11 ORDER BY 1 LIMIT 5 OFFSET 2",
        "WITH c AS (SELECT 11) SELECT 13 ORDER BY 1 LIMIT 5 OFFSET 2",
    ] {
        let parsed = query(source);
        assert!(parsed.modifiers.is_empty());
        let SetExpr::Select(select) = parsed.body.as_ref() else {
            panic!("SELECT")
        };
        assert_eq!(select.modifiers.order_by.len(), 1);
        assert!(select.modifiers.limit.is_some());
        assert!(select.modifiers.offset.is_some());
        ast::validate_statement(&ast::Statement::Query(parsed.clone())).unwrap();
        assert!(parsed.syntax_eq(&query(&novarocks_parser::printer::print_query(&parsed))));
    }
    for source in [
        "VALUES (11) ORDER BY 1 LIMIT 5 OFFSET 2",
        "SELECT 11 UNION ALL SELECT 13 ORDER BY 1 LIMIT 5 OFFSET 2",
        "(SELECT 11 ORDER BY 1 LIMIT 7 OFFSET 3) ORDER BY 1 LIMIT 5 OFFSET 2",
    ] {
        let parsed = query(source);
        assert_eq!(parsed.modifiers.order_by.len(), 1);
        assert!(parsed.modifiers.limit.is_some());
        assert!(parsed.modifiers.offset.is_some());
        assert!(parsed.syntax_eq(&query(&novarocks_parser::printer::print_query(&parsed))));
    }
    #[derive(Default)]
    struct Numbers(Vec<String>);
    impl Visit for Numbers {
        fn visit_literal(&mut self, literal: &Literal) {
            if let LiteralKind::Long {
                spelling: value, ..
            } = &literal.kind
            {
                self.0.push(value.clone());
            }
        }
    }
    let mut parsed = query("(SELECT 11 ORDER BY 1 LIMIT 7 OFFSET 3) ORDER BY 1 LIMIT 5 OFFSET 2");
    let mut numbers = Numbers::default();
    numbers.visit_query(&parsed);
    assert_eq!(numbers.0, ["11", "1", "7", "3", "1", "5", "2"]);
    let SetExpr::Query(inner) = parsed.body.as_mut() else {
        panic!("parenthesized query")
    };
    let SetExpr::Select(select) = inner.body.as_mut() else {
        panic!("SELECT")
    };
    inner.modifiers = std::mem::take(&mut select.modifiers);
    let error = ast::validate_statement(&ast::Statement::Query(parsed)).unwrap_err();
    assert!(matches!(
        error,
        novarocks_parser::ValidateError::InvalidStructure {
            violation: novarocks_parser::StructuralViolation::InvalidQueryModifierOwner,
            ..
        }
    ));
}

#[test]
fn query_session_assignments_preserve_names_values_and_traversal() {
    use ast::{Fold, Literal, LiteralKind, Visit};

    let mut built = build::query(select_body(1));
    built.session_properties.push(ast::SessionProperty {
        name: build::object_name(vec![
            build::ident("iceberg", None),
            build::ident("property", None),
        ])
        .unwrap(),
        value: build::integer(5),
        span: SYNTHETIC,
    });
    #[derive(Default)]
    struct Facts {
        names: Vec<String>,
        numbers: Vec<String>,
    }
    impl Visit for Facts {
        fn visit_ident(&mut self, ident: &ast::Ident) {
            self.names.push(ident.value.clone());
        }
        fn visit_literal(&mut self, literal: &Literal) {
            if let LiteralKind::Long {
                spelling: value, ..
            } = &literal.kind
            {
                self.numbers.push(value.clone());
            }
        }
    }
    let mut facts = Facts::default();
    facts.visit_query(&built);
    assert_eq!(facts.names, ["iceberg", "property"]);
    assert_eq!(facts.numbers, ["5", "1"]);
    assert_eq!(
        novarocks_parser::printer::print_query(&built),
        "WITH SESSION iceberg.property = 5 SELECT 1"
    );
    let mut union = build::query(build::union_all(vec![select_body(1), select_body(2)]).unwrap());
    union.session_properties = built.session_properties.clone();
    assert_eq!(
        novarocks_parser::printer::print_query(&union),
        "WITH SESSION iceberg.property = 5 SELECT 1 UNION ALL SELECT 2"
    );

    struct Increment;
    impl Fold for Increment {
        fn fold_literal(&mut self, mut literal: Literal) -> Literal {
            if let LiteralKind::Long {
                spelling: value,
                parsed_value,
            } = &mut literal.kind
            {
                *parsed_value += 1;
                *value = parsed_value.to_string();
            }
            literal
        }
    }
    let folded = Increment.fold_query(built.clone());
    assert_eq!(folded.session_properties[0].value, build::integer(6));
    assert_eq!(built.session_properties[0].value, build::integer(5));
    assert!(!built.syntax_eq(&folded));
    let mut relocated = built.clone();
    relocated.session_properties[0].span = Span::new(10, 20);
    relocated.session_properties[0].name.parts[0].span = Span::new(10, 12);
    assert!(built.syntax_eq(&relocated));
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
    assert_expression(build::binary_literal(vec![0xAB, 1]), "X'AB01'");
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
fn domain_authored_decimal_literals_keep_their_value_beyond_source_integer_width() {
    // A typed caller can author a DECIMAL value wider than a source integer.
    // Literal construction preserves that value; source-number admission keeps
    // its existing width check and cannot serve as the oracle for this input.
    let digits = "12345678901234567890123456789012345678";
    let kind = ast::LiteralKind::Decimal(digits.into());
    let value = build::literal(kind.clone());
    let Expr::Literal(literal) = value else {
        panic!("authored decimal literal");
    };
    assert_eq!(literal.kind, kind);
    assert_eq!(literal.span, SYNTHETIC);
    assert!(matches!(build::number(digits), Err(BuildError::Number(_))));
}

#[test]
fn one_field_value_tuples_remain_distinct_from_source_parentheses() {
    let field = build::literal(ast::LiteralKind::Decimal(
        "12345678901234567890123456789012345678".into(),
    ));
    let value = build::tuple_value(vec![field.clone()]).expect("one-field value tuple");
    let Expr::Tuple(tuple) = value else {
        panic!("typed one-field tuple");
    };
    assert_eq!(tuple.expressions, vec![field.clone()]);
    assert_eq!(tuple.span, SYNTHETIC);
    let source_parentheses = build::tuple(vec![field.clone()]).expect("source parentheses");
    let Expr::Nested(nested) = source_parentheses else {
        panic!("source parentheses retain their Nested shape");
    };
    assert_eq!(*nested.expression, field);
    assert_eq!(nested.span, SYNTHETIC);
}

#[test]
fn type_nodes_preserve_nested_arguments_without_comma_trivia() {
    let decimal = build::type_name(
        build::ident("DECIMAL", None),
        vec![build::numeric_parameter(12), build::numeric_parameter(2)],
    );
    assert_expression(
        build::cast(build::null(), decimal),
        "CAST(NULL AS DECIMAL(12, 2))",
    );
    let field = ast::RowDataTypeField {
        name: Some(build::ident("a b", Some('`'))),
        data_type: build::simple_type("INT"),
        span: SYNTHETIC,
    };
    let structure = build::row_type(vec![field]);
    let array = build::type_name(
        build::ident("ARRAY", None),
        vec![build::type_parameter(structure)],
    );
    let map = build::type_name(
        build::ident("MAP", None),
        vec![
            build::type_parameter(build::simple_type("STRING")),
            build::type_parameter(array),
        ],
    );
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
fn boolean_chains_preserve_ordered_operands_in_one_logical_node() {
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
    let built = build::and_all(vec![id("a"), id("b"), id("c"), id("d")]).unwrap();
    let Expr::Logical(node) = &built else {
        panic!("logical")
    };
    assert_eq!(node.terms.len(), 4);
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
        JoinConstraint::On(build::comparison(
            build::column(build::ident("s", None), build::ident("k1", None)),
            ast::ComparisonOperator::Equal,
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
fn values_and_union_shapes_retain_ordered_inputs() {
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
fn union_all_preserves_input_order_and_set_boundaries() {
    let parsed = query("SELECT 1 UNION ALL SELECT 2 UNION ALL SELECT 3");
    let SetExpr::SetOperation(operation) = parsed.body.as_ref() else {
        panic!("UNION ALL")
    };
    assert_eq!(operation.inputs.len(), 3);
    let values = operation
        .inputs
        .iter()
        .map(|input| {
            let SetExpr::Select(select) = input else {
                panic!("SELECT operand")
            };
            let ast::SelectItem::UnnamedExpr(Expr::Literal(value)) = &select.projection[0] else {
                panic!("literal projection")
            };
            value.kind.clone()
        })
        .collect::<Vec<_>>();
    assert_eq!(
        values,
        [1, 2, 3].map(|n| ast::LiteralKind::Long {
            spelling: n.to_string(),
            parsed_value: (n.to_string()).parse::<i64>().unwrap()
        })
    );
    assert!(query("SELECT 1 UNION SELECT 2").syntax_eq(&query("SELECT 1 UNION DISTINCT SELECT 2")));
    for source in [
        "SELECT 1 UNION SELECT 2 UNION ALL SELECT 3",
        "(SELECT 1 UNION ALL SELECT 2) UNION ALL SELECT 3",
        "SELECT 1 UNION ALL (SELECT 2 UNION ALL SELECT 3)",
    ] {
        let parsed = query(source);
        let SetExpr::SetOperation(operation) = parsed.body.as_ref() else {
            panic!("set operation")
        };
        assert_eq!(operation.inputs.len(), 2, "{source}");
        assert!(parsed.syntax_eq(&query(&novarocks_parser::printer::print_query(&parsed))));
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
    assert_eq!(build::tuple_value(Vec::new()), Err(BuildError::EmptyTuple));
    assert_eq!(build::union_all(Vec::new()), Err(BuildError::EmptyUnion));
    assert_eq!(
        build::qualified_wildcard(Vec::new()),
        Err(BuildError::EmptyWildcardPrefix)
    );
}

#[test]
fn public_literals_retain_categories_decoded_bytes_and_numeric_facts() {
    let parsed = query(
        "SELECT 0001, 1.20, 1e0, X'ab01', DECIMAL '12345678901234567890123456789012345678', DECIMAL '-123.45', 1e999",
    );
    let SetExpr::Select(select) = parsed.body.as_ref() else {
        panic!("SELECT")
    };
    let kinds: Vec<_> = select
        .projection
        .iter()
        .map(|item| {
            let ast::SelectItem::UnnamedExpr(Expr::Literal(value)) = item else {
                panic!("literal")
            };
            &value.kind
        })
        .collect();
    assert!(
        matches!(kinds[0], ast::LiteralKind::Long { spelling, parsed_value: 1 } if spelling == "0001")
    );
    assert!(matches!(kinds[1], ast::LiteralKind::Decimal(value) if value == "1.20"));
    assert!(matches!(kinds[2], ast::LiteralKind::Double(value) if value.get() == 1.0));
    assert!(matches!(kinds[3], ast::LiteralKind::Binary(value) if value == &[0xAB, 1]));
    assert!(
        matches!(kinds[4], ast::LiteralKind::Decimal(value) if value == "12345678901234567890123456789012345678")
    );
    assert!(matches!(kinds[5], ast::LiteralKind::Decimal(value) if value == "-123.45"));
    assert!(matches!(kinds[6], ast::LiteralKind::Double(value) if value.get() == f64::INFINITY));
    let rendered = novarocks_parser::printer::print_query(&parsed);
    assert!(rendered.contains("DECIMAL '12345678901234567890123456789012345678'"));
    assert!(parsed.syntax_eq(&query(&rendered)));
    let compact = query("SELECT CAST(NULL AS DECIMAL(12,2))");
    let spaced = query("SELECT CAST(NULL AS DECIMAL(12, 2))");
    assert!(compact.syntax_eq(&spaced));
}

#[test]
fn decimal_constructor_text_is_not_reinterpreted_as_an_unrelated_number_expression() {
    // Trino's DecimalLiteral retains decoded constructor text even when the
    // later numeric analyzer will reject its value. Printing is syntax-only.
    for value in ["", ".", "1..2", "1-2.3", "1+2.3", "-0.0", "+2.3", "1e0"] {
        let built = build::literal(ast::LiteralKind::Decimal(value.into()));
        let rendered = novarocks_parser::printer::print_expr(&built);
        assert_eq!(rendered, format!("DECIMAL '{value}'"));
        assert!(built.syntax_eq(&expression(&rendered)));
    }
}

#[test]
fn logical_nodes_keep_source_order_grouping_and_every_visitor_child() {
    use ast::{Fold, Visit};
    let source = "a = 1 AND (b = 2 OR c = 3 OR d = 4) AND e = 5";
    let parsed = expression(source);
    let Expr::Logical(logical) = &parsed else {
        panic!("logical")
    };
    assert_eq!(logical.operator, ast::LogicalOperator::And);
    assert_eq!(logical.terms.len(), 3);
    let Expr::Nested(group) = &logical.terms[1] else {
        panic!("explicit parentheses")
    };
    let Expr::Logical(or) = group.expression.as_ref() else {
        panic!("OR")
    };
    assert_eq!(or.operator, ast::LogicalOperator::Or);
    assert_eq!(or.terms.len(), 3);
    #[derive(Default)]
    struct Names(Vec<String>);
    impl Visit for Names {
        fn visit_ident(&mut self, value: &ast::Ident) {
            self.0.push(value.value.clone());
        }
    }
    let mut names = Names::default();
    names.visit_expr(&parsed);
    assert_eq!(names.0, ["a", "b", "c", "d", "e"]);
    struct Identity;
    impl Fold for Identity {}
    assert_eq!(Identity.fold_expr(parsed.clone()), parsed);
    assert!(parsed.syntax_eq(&expression(&novarocks_parser::printer::print_expr(&parsed))));
    let mut changed = parsed.clone();
    let Expr::Logical(value) = &mut changed else {
        unreachable!()
    };
    value.span = SYNTHETIC;
    assert!(parsed.syntax_eq(&changed));
    let Expr::Logical(value) = &mut changed else {
        unreachable!()
    };
    value.terms.swap(0, 2);
    assert!(!parsed.syntax_eq(&changed));
}

#[test]
fn public_wide_logical_chains_are_flat_without_changing_order_or_literal_facts() {
    use ast::{Fold, Visit};
    struct Identity;
    impl Fold for Identity {}
    #[derive(Default)]
    struct Values(Vec<i64>);
    impl Visit for Values {
        fn visit_literal(&mut self, value: &ast::Literal) {
            if let ast::LiteralKind::Long { parsed_value, .. } = value.kind {
                self.0.push(parsed_value);
            }
        }
    }
    for operator in ["AND", "OR"] {
        let source = (0..20_000)
            .map(|value| value.to_string())
            .collect::<Vec<_>>()
            .join(&format!(" {operator} "));
        let parsed = expression(&source);
        let Expr::Logical(node) = &parsed else {
            panic!("logical")
        };
        assert_eq!(node.terms.len(), 20_000);
        assert!(
            node.terms
                .iter()
                .all(|term| matches!(term, Expr::Literal(_)))
        );
        let cloned = parsed.clone();
        assert_eq!(parsed, cloned);
        let folded = Identity.fold_expr(cloned);
        assert!(parsed.syntax_eq(&folded));
        let mut values = Values::default();
        values.visit_expr(&folded);
        assert_eq!(values.0, (0..20_000).collect::<Vec<i64>>());
        let rendered = novarocks_parser::printer::print_expr(&parsed);
        assert_eq!(rendered, source);
        assert!(parsed.syntax_eq(&expression(&rendered)));
    }
}

#[test]
fn logical_minimum_arity_is_checked_without_catalog_or_analysis() {
    for terms in [vec![], vec![build::boolean(true)]] {
        let mut parsed = query("SELECT TRUE");
        let SetExpr::Select(select) = parsed.body.as_mut() else {
            panic!("SELECT")
        };
        let span = Span::new(30, 40);
        select.projection[0] = ast::SelectItem::UnnamedExpr(Expr::Logical(ast::LogicalExpr {
            operator: ast::LogicalOperator::And,
            terms: terms.into(),
            span,
        }));
        let error = ast::validate_statement(&ast::Statement::Query(parsed)).unwrap_err();
        assert!(
            matches!(error, novarocks_parser::ValidateError::InvalidStructure { violation: novarocks_parser::StructuralViolation::InvalidLogicalTermCount, span: actual } if actual == span)
        );
    }
}

#[test]
fn actual_recursive_logical_storage_guards_clone_walk_fold_compare_validation_print_and_drop() {
    use ast::{Fold, Visit};
    let mut node = build::boolean(true);
    for index in 0..20_000 {
        node = Expr::Logical(ast::LogicalExpr {
            operator: if index % 2 == 0 {
                ast::LogicalOperator::And
            } else {
                ast::LogicalOperator::Or
            },
            terms: vec![node, build::boolean(false)].into(),
            span: SYNTHETIC,
        });
    }
    let cloned = node.clone();
    assert_eq!(node, cloned);
    assert!(node.syntax_eq(&cloned));
    struct Identity;
    impl Fold for Identity {}
    let folded = Identity.fold_expr(cloned);
    assert!(node.syntax_eq(&folded));
    struct Count(usize);
    impl Visit for Count {
        fn visit_logical_expr(&mut self, node: &ast::LogicalExpr) {
            self.0 += 1;
            ast::walk_logical_expr(self, node);
        }
    }
    let mut count = Count(0);
    count.visit_expr(&node);
    assert_eq!(count.0, 20_000);
    let rendered = novarocks_parser::printer::print_expr(&node);
    assert!(rendered.starts_with('('));
    assert!(rendered.ends_with("FALSE"));
    let mut parsed = query("SELECT TRUE");
    let SetExpr::Select(select) = parsed.body.as_mut() else {
        unreachable!()
    };
    select.projection[0] = ast::SelectItem::UnnamedExpr(folded);
    ast::validate_statement(&ast::Statement::Query(parsed)).unwrap();
    drop(node);
}

#[test]
fn public_predicate_producers_print_and_reparse_without_losing_their_fields() {
    for source in [
        "a = b",
        "a != b",
        "a < b",
        "a <= b",
        "a > b",
        "a >= b",
        "a <=> b",
        "a IS NOT DISTINCT FROM b",
        "a IS DISTINCT FROM b",
        "a BETWEEN 1 AND 3",
        "a NOT BETWEEN 1 AND 3",
        "a IS NULL",
        "a IS NOT NULL",
        "a IS TRUE",
        "a IS NOT FALSE",
        "a IS UNKNOWN",
        "a LIKE 'x%'",
        "a NOT LIKE 'x%' ESCAPE '_'",
        "a IN (1, 2, NULL)",
        "a NOT IN (SELECT b FROM t)",
        "(a + 1) >= 2 AND (b IS NULL OR c NOT IN (3, 4))",
    ] {
        let parsed = expression(source);
        let rendered = novarocks_parser::printer::print_expr(&parsed);
        assert!(
            parsed.syntax_eq(&expression(&rendered)),
            "{source} -> {rendered}"
        );
    }
    let nullsafe = expression("a <=> b");
    assert!(nullsafe.syntax_eq(&expression("a IS NOT DISTINCT FROM b")));
    assert!(!nullsafe.syntax_eq(&expression("a IS DISTINCT FROM b")));
    let Expr::Predicated(in_node) = expression("a NOT IN (1, 2)") else {
        panic!("IN predicate")
    };
    let ast::Predicate::In(in_node) = &in_node.predicate else {
        unreachable!()
    };
    assert!(in_node.negated);
    let Expr::InList(list) = &in_node.value_list else {
        panic!("real IN list expression")
    };
    assert_eq!(list.values.len(), 2);
    assert!(matches!(
        build::in_list(id("a"), Vec::new(), false),
        Err(BuildError::EmptyInList)
    ));
    assert!(novarocks_parser::parse("SELECT a IN ()").is_err());
}

#[test]
fn distinct_from_in_projection_keeps_the_later_relation_boundary() {
    for source in [
        "SELECT NULL IS DISTINCT FROM NULL",
        "SELECT NULL IS NOT DISTINCT FROM NULL",
        "SELECT a IS /* boundary */ NOT DISTINCT /* value */ FROM b AS result FROM t",
        "SELECT a IS DISTINCT FROM b, c IS NOT DISTINCT FROM d FROM t WHERE a IS DISTINCT FROM b",
    ] {
        let parsed = novarocks_parser::parse(source).unwrap();
        let rendered = novarocks_parser::printer::print_statement(&parsed[0]);
        let reparsed = novarocks_parser::parse(&rendered).unwrap();
        let (ast::Statement::Query(left), ast::Statement::Query(right)) =
            (&parsed[0], &reparsed[0])
        else {
            panic!("query")
        };
        assert!(left.syntax_eq(right), "{source} -> {rendered}");
    }
}

#[test]
fn control_producers_keep_branch_wrappers_safe_flags_and_declaration_spans() {
    for source in [
        "SELECT CASE WHEN a THEN b WHEN c THEN d END",
        "SELECT CASE a WHEN 1 THEN b WHEN 2 THEN c ELSE NULL END",
        "SELECT CASE WHEN a THEN CASE b WHEN 1 THEN c END ELSE d END",
        "SELECT CAST(a AS integer), TRY_CAST(b AS varchar)",
        "SELECT array_map(x -> x + 1, [1,2])",
        "SELECT array_map((x, y) -> x + y, [1], [2])",
        "SELECT EXISTS (SELECT 1), NOT EXISTS (SELECT 2)",
    ] {
        let parsed = novarocks_parser::parse(source).unwrap();
        let rendered = novarocks_parser::printer::print_statement(&parsed[0]);
        let reparsed = novarocks_parser::parse(&rendered).unwrap();
        let (ast::Statement::Query(left), ast::Statement::Query(right)) =
            (&parsed[0], &reparsed[0])
        else {
            panic!("query")
        };
        assert!(left.syntax_eq(right), "{source} -> {rendered}");
    }
    let Expr::Cast(ordinary) = expression("CAST(a AS integer)") else {
        panic!("cast")
    };
    let Expr::Cast(safe) = expression("TRY_CAST(a AS integer)") else {
        panic!("try cast")
    };
    assert!(!ordinary.safe);
    assert!(safe.safe);
    assert!(!ordinary.syntax_eq(&safe));
    let source = "NOT EXISTS (SELECT 1)";
    let Expr::Unary(negation) = expression(source) else {
        panic!("NOT")
    };
    assert_eq!(negation.operator, ast::UnaryOperator::Not);
    assert_eq!(negation.span, novarocks_parser::Span::new(0, source.len()));
    let Expr::Exists(exists) = negation.expression.as_ref() else {
        panic!("exists")
    };
    assert_eq!(exists.span.start(), 4);
    let Expr::Subquery(subquery) = &exists.subquery else {
        panic!("subquery wrapper")
    };
    assert_eq!(subquery.span, novarocks_parser::Span::new(11, source.len()));
    let Expr::Lambda(lambda) = expression("(x, y) -> x + y") else {
        panic!("lambda")
    };
    assert_eq!(lambda.arguments[0].span, novarocks_parser::Span::new(1, 2));
    assert_eq!(lambda.arguments[1].span, novarocks_parser::Span::new(4, 5));
    assert!(expression("x -> x").syntax_eq(&expression("(x) -> x")));
}
