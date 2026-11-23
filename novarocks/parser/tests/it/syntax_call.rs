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

use novarocks_parser::{Span, ast::Expr, canonical_call, parser::parse_expression};

#[test]
fn dedicated_syntax_maps_to_canonical_name_and_ordered_source_children() {
    let cases: &[(&str, &str, &[&str])] = &[
        ("ROW(a, b, c)", "row", &["a", "b", "c"]),
        ("(a, b)", "row", &["a", "b"]),
        ("IF(predicate, yes, no)", "if", &["predicate", "yes", "no"]),
        ("NULLIF(a, b)", "nullif", &["a", "b"]),
        ("COALESCE(a, b, c)", "coalesce", &["a", "b", "c"]),
        ("TRIM(text_value)", "trim", &["text_value"]),
        ("TRIM(BOTH FROM text_value)", "trim", &["text_value"]),
        ("TRIM(LEADING FROM text_value)", "ltrim", &["text_value"]),
        ("TRIM(TRAILING FROM text_value)", "rtrim", &["text_value"]),
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
            "TRIM(BOTH 'x' FROM t)",
            "explicit TRIM characters are not supported",
        ),
        ("TRIM(t, 'x')", "explicit TRIM characters are not supported"),
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
