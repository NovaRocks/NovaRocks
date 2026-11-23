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

use novarocks_parser::{
    Span,
    ast::{Expr, LiteralKind, SyntaxEq},
    canonical_call,
    parser::parse_expression,
    printer::print_expression,
};

#[test]
fn dedicated_syntax_maps_to_canonical_name_and_ordered_source_children() {
    let cases: &[(&str, &str, &[&str])] = &[
        ("ROW(a, b, c)", "row", &["a", "b", "c"]),
        ("(a, b)", "row", &["a", "b"]),
        ("IF(predicate, yes, no)", "if", &["predicate", "yes", "no"]),
        ("NULLIF(a, b)", "nullif", &["a", "b"]),
        ("COALESCE(a, b, c)", "coalesce", &["a", "b", "c"]),
        ("FORMAT(pattern, a, b)", "format", &["pattern", "a", "b"]),
        ("TRIM(text_value)", "trim", &["text_value"]),
        ("TRIM(BOTH FROM text_value)", "trim", &["text_value"]),
        ("TRIM(LEADING FROM text_value)", "ltrim", &["text_value"]),
        ("TRIM(TRAILING FROM text_value)", "rtrim", &["text_value"]),
        (
            "TRIM(BOTH chars FROM text_value)",
            "trim",
            &["text_value", "chars"],
        ),
        (
            "TRIM(LEADING chars FROM text_value)",
            "ltrim",
            &["text_value", "chars"],
        ),
        (
            "TRIM(TRAILING chars FROM text_value)",
            "rtrim",
            &["text_value", "chars"],
        ),
        ("TRIM(text_value, chars)", "trim", &["text_value", "chars"]),
        ("OVERLAY(s PLACING r FROM p)", "$overlay", &["s", "r", "p"]),
        (
            "OVERLAY(s PLACING r FROM p FOR n)",
            "$overlay",
            &["s", "r", "p", "n"],
        ),
        ("EXTRACT(YEAR FROM x)", "year", &["x"]),
        ("EXTRACT(MONTH FROM x)", "month", &["x"]),
        ("EXTRACT(DAY FROM x)", "day", &["x"]),
        ("EXTRACT(HOUR FROM x)", "hour", &["x"]),
        ("EXTRACT(MINUTE FROM x)", "minute", &["x"]),
        ("EXTRACT(SECOND FROM x)", "second", &["x"]),
        ("EXTRACT(QUARTER FROM x)", "quarter", &["x"]),
        ("EXTRACT(WEEK FROM x)", "week", &["x"]),
        ("EXTRACT(DAY_OF_MONTH FROM x)", "day_of_month", &["x"]),
        ("EXTRACT(DAY_OF_WEEK FROM x)", "day_of_week", &["x"]),
        ("EXTRACT(DOW FROM x)", "day_of_week", &["x"]),
        ("EXTRACT(DAY_OF_YEAR FROM x)", "day_of_year", &["x"]),
        ("EXTRACT(DOY FROM x)", "day_of_year", &["x"]),
        (
            "EXTRACT(YEAR_OF_WEEK FROM x)",
            "__extract_year_of_week",
            &["x"],
        ),
        ("EXTRACT(YOW FROM x)", "__extract_year_of_week", &["x"]),
        (
            "EXTRACT(TIMEZONE_MINUTE FROM x)",
            "__extract_timezone_minute",
            &["x"],
        ),
        (
            "EXTRACT(TIMEZONE_HOUR FROM x)",
            "__extract_timezone_hour",
            &["x"],
        ),
    ];
    for &(source, name, expected_arguments) in cases {
        let expr = parse_expression(source).unwrap();
        let actual = canonical_call(&expr).unwrap().unwrap();
        assert_eq!(actual.name, name, "{source}");
        assert_eq!(actual.span, Span::new(0, source.len()), "{source}");
        let arguments: Vec<_> = actual
            .arguments
            .iter()
            .map(|argument| &source[argument.span().start()..argument.span().end()])
            .collect();
        assert_eq!(arguments, expected_arguments, "{source}");
    }
}

#[test]
fn canonical_call_borrows_original_nested_nodes() {
    let expr = parse_expression("COALESCE(NULLIF(a, b), ROW(1, 2))").unwrap();
    let Expr::Coalesce(node) = &expr else {
        panic!("expected COALESCE");
    };
    let call = canonical_call(&expr).unwrap().unwrap();
    for (original, actual) in node.operands.iter().zip(call.arguments) {
        assert!(std::ptr::eq(original, actual));
    }
}

#[test]
fn syntax_without_a_canonical_call_preserves_the_rejection() {
    for (source, message) in [
        (
            "ROW(a, b AS named_field)",
            "named ROW fields are not supported",
        ),
        ("IF(a, b)", "two-argument IF is not supported"),
        ("\"if\"(a, b)", "two-argument IF is not supported"),
    ] {
        assert_unsupported(source, message);
    }
}

fn assert_unsupported(source: &str, message: &str) {
    let expr = parse_expression(source).unwrap();
    let error = canonical_call(&expr).unwrap_err();
    assert_eq!(error.span, Span::new(0, source.len()), "{source}");
    assert_eq!(error.to_string(), message, "{source}");
}

#[test]
fn ordinary_calls_and_other_expressions_do_not_map() {
    for source in ["abs(a)", "ltrim(a)", "\"rtrim\"(a)", "x", "a + b"] {
        let expr = parse_expression(source).unwrap();
        assert!(canonical_call(&expr).unwrap().is_none(), "{source}");
    }
}

#[test]
fn position_round_trips_source_parameter_order_through_reversed_call_arguments() {
    let source = "POSITION(concat(?, ?) IN concat(?, ?))";
    let expression = parse_expression(source).unwrap();
    let Expr::FunctionCall(position) = &expression else {
        panic!("expected POSITION's canonical call");
    };
    assert_eq!(position.name.parts[0].value, "strpos");
    for (argument, positions) in position.arguments.iter().zip([[2, 3], [0, 1]]) {
        let Expr::FunctionCall(concat) = &argument.value else {
            panic!("expected concat");
        };
        for (child, index) in concat.arguments.iter().zip(positions) {
            assert!(matches!(&child.value, Expr::Parameter(p) if p.position == index));
            assert_eq!(&source[child.span.start()..child.span.end()], "?");
        }
    }
    let printed = print_expression(&expression).unwrap();
    assert!(expression.syntax_eq(&parse_expression(&printed).unwrap()));
}

#[test]
fn normalize_projects_each_form_to_the_original_call_without_folding_case() {
    for (source, form) in [
        ("NORMALIZE(?)", "NFC"),
        ("NORMALIZE(?, NFC)", "NFC"),
        ("NORMALIZE(?, NFD)", "NFD"),
        ("NORMALIZE(?, NFKC)", "NFKC"),
        ("NORMALIZE(?, NFKD)", "NFKD"),
        ("NORMALIZE(?, nfd)", "nfd"),
    ] {
        let expression = parse_expression(source).unwrap();
        let Expr::FunctionCall(call) = &expression else {
            panic!("expected NORMALIZE's canonical call");
        };
        assert!(call.name.parts[0].quoted);
        assert_eq!(call.name.parts[0].value, "normalize");
        assert_eq!(call.span, Span::new(0, source.len()));
        assert!(matches!(&call.arguments[0].value, Expr::Parameter(p) if p.position == 0));
        assert_eq!(
            &source[call.arguments[0].span.start()..call.arguments[0].span.end()],
            "?"
        );
        assert!(matches!(
            &call.arguments[1].value,
            Expr::Literal(literal) if literal.kind == LiteralKind::String(form.into())
        ));
        let printed = print_expression(&expression).unwrap();
        assert!(expression.syntax_eq(&parse_expression(&printed).unwrap()));
    }
}
