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

use novarocks_parser::ast::*;
use novarocks_parser::parser::parse_expression;
use novarocks_parser::{ParseError, ParserError, SYNTHETIC, Span};

fn call(expression: &Expr) -> &FunctionCall {
    let Expr::FunctionCall(value) = expression else {
        panic!("not a call: {expression:?}")
    };
    value
}
fn parameter(expression: &Expr) -> &Parameter {
    let Expr::Parameter(value) = expression else {
        panic!("not a parameter: {expression:?}")
    };
    value
}
fn literal_kind(expression: &Expr) -> &LiteralKind {
    let Expr::Literal(value) = expression else {
        panic!("not a literal: {expression:?}")
    };
    &value.kind
}
fn trim(expression: &Expr) -> &Trim {
    let Expr::Trim(value) = expression else {
        panic!("not trim: {expression:?}")
    };
    value
}

#[test]
fn position_reverses_values_after_parameter_visits_and_keeps_real_child_spans() {
    let source = "POSITION(? IN ?)";
    let expression = parse_expression(source).unwrap();
    let value = call(&expression);
    assert_eq!(
        value.name.parts,
        vec![Ident {
            value: "strpos".into(),
            quoted: false,
            span: SYNTHETIC
        }]
    );
    assert_eq!(value.name.span, SYNTHETIC);
    assert_eq!(value.arguments.len(), 2);
    assert_eq!(parameter(&value.arguments[0].value).position, 1);
    assert_eq!(parameter(&value.arguments[1].value).position, 0);
    assert_eq!(value.arguments[0].span, Span::new(14, 15));
    assert_eq!(value.arguments[1].span, Span::new(9, 10));
    assert_eq!(value.span, Span::new(0, source.len()));
    for argument in &value.arguments {
        assert!(argument.name.is_none());
        assert_eq!(argument.span, argument.value.span());
    }
    let expression = parse_expression("POSITION(ARRAY[1,2] IN ?)").unwrap();
    let value = call(&expression);
    assert_eq!(parameter(&value.arguments[0].value).position, 0);
    let Expr::Array(array) = &value.arguments[1].value else {
        panic!("lost original array")
    };
    assert_eq!(array.values.len(), 2);
    assert!(matches!(
        literal_kind(&array.values[0]),
        LiteralKind::Long {
            parsed_value: 1,
            ..
        }
    ));
    assert!(matches!(
        literal_kind(&array.values[1]),
        LiteralKind::Long {
            parsed_value: 2,
            ..
        }
    ));
}

#[test]
fn substring_from_and_nonreserved_ordinary_calls_keep_distinct_productions() {
    let source = "SUBSTRING('abc' FROM 2 FOR 1)";
    let expression = parse_expression(source).unwrap();
    let value = call(&expression);
    assert_eq!(value.name.parts[0].value, "substr");
    assert_eq!(value.name.parts[0].span, SYNTHETIC);
    assert_eq!(value.arguments.len(), 3);
    assert_eq!(
        literal_kind(&value.arguments[0].value),
        &LiteralKind::String("abc".into())
    );
    assert!(matches!(
        literal_kind(&value.arguments[1].value),
        LiteralKind::Long {
            parsed_value: 2,
            ..
        }
    ));
    assert!(matches!(
        literal_kind(&value.arguments[2].value),
        LiteralKind::Long {
            parsed_value: 1,
            ..
        }
    ));
    assert_eq!(value.span, Span::new(0, source.len()));
    let expression = parse_expression("SUBSTRING(? FROM ?)").unwrap();
    assert_eq!(call(&expression).arguments.len(), 2);
    for source in [
        "position(?, ?)",
        "substring(?, ?)",
        "overlay(?, ?)",
        "\"POSITION\"(?, ?)",
        "ns.\"normalize\"(?, ?)",
    ] {
        let expression = parse_expression(source).unwrap();
        let value = call(&expression);
        assert_eq!(value.arguments.len(), 2, "{source}");
        assert_eq!(parameter(&value.arguments[0].value).position, 0);
        assert_eq!(parameter(&value.arguments[1].value).position, 1);
        assert_ne!(
            value.name.span, SYNTHETIC,
            "ordinary names must retain their original spans"
        );
    }
}

#[test]
fn normalize_uses_quoted_builtin_name_and_real_raw_form_or_default() {
    for (source, expected) in [("NORMALIZE(?)", "NFC"), ("NORMALIZE(?, nFd)", "nFd")] {
        let expression = parse_expression(source).unwrap();
        let value = call(&expression);
        assert_eq!(
            value.name.parts,
            vec![Ident {
                value: "normalize".into(),
                quoted: true,
                span: SYNTHETIC
            }]
        );
        assert_eq!(value.arguments.len(), 2);
        assert_eq!(parameter(&value.arguments[0].value).position, 0);
        assert_eq!(
            literal_kind(&value.arguments[1].value),
            &LiteralKind::String(expected.into())
        );
        assert_eq!(value.arguments[1].span, Span::new(0, source.len()));
        assert!(!value.distinct);
        assert!(value.order_by.is_none() && value.filter.is_none() && value.window.is_none());
        assert!(value.processing_mode.is_none() && value.null_treatment.is_none());
    }
}

#[test]
fn extract_has_exact_fields_and_never_decodes_quoted_field_names_into_them() {
    for (name, expected) in [
        ("YEAR", ExtractField::Year),
        ("QUARTER", ExtractField::Quarter),
        ("MONTH", ExtractField::Month),
        ("WEEK", ExtractField::Week),
        ("DAY", ExtractField::Day),
        ("DAY_OF_MONTH", ExtractField::DayOfMonth),
        ("DAY_OF_WEEK", ExtractField::DayOfWeek),
        ("DOW", ExtractField::Dow),
        ("DAY_OF_YEAR", ExtractField::DayOfYear),
        ("DOY", ExtractField::Doy),
        ("YEAR_OF_WEEK", ExtractField::YearOfWeek),
        ("YOW", ExtractField::Yow),
        ("HOUR", ExtractField::Hour),
        ("MINUTE", ExtractField::Minute),
        ("SECOND", ExtractField::Second),
        ("TIMEZONE_MINUTE", ExtractField::TimezoneMinute),
        ("TIMEZONE_HOUR", ExtractField::TimezoneHour),
    ] {
        let source = format!("EXTRACT({name} FROM ?)");
        let expression = parse_expression(&source).unwrap();
        let Expr::Extract(value) = expression else {
            panic!("not extract")
        };
        assert_eq!(value.field, expected);
        assert_eq!(parameter(&value.expression).position, 0);
        assert_eq!(value.span, Span::new(0, source.len()));
    }
    for source in ["EXTRACT(\"YEAR\" FROM ?)", "EXTRACT(NOT_A_FIELD FROM ?)"] {
        assert!(matches!(
            parse_expression(source),
            Err(ParserError::Parse(_))
        ));
    }
}

#[test]
fn trim_forms_preserve_default_specification_and_visit_source_before_character() {
    let expression = parse_expression("TRIM(?)").unwrap();
    assert_eq!(trim(&expression).specification, TrimSpecification::Both);
    assert!(trim(&expression).trim_character.is_none());
    assert_eq!(parameter(&trim(&expression).trim_source).position, 0);
    let expression = parse_expression("TRIM(? FROM ?)").unwrap();
    assert_eq!(parameter(&trim(&expression).trim_source).position, 0);
    assert_eq!(
        parameter(trim(&expression).trim_character.as_ref().unwrap()).position,
        1
    );
    assert_eq!(trim(&expression).trim_source.span(), Span::new(12, 13));
    assert_eq!(
        trim(&expression).trim_character.as_ref().unwrap().span(),
        Span::new(5, 6)
    );
    let expression = parse_expression("TRIM(?, ?)").unwrap();
    assert_eq!(parameter(&trim(&expression).trim_source).position, 0);
    assert_eq!(
        parameter(trim(&expression).trim_character.as_ref().unwrap()).position,
        1
    );
    let expression = parse_expression("TRIM(LEADING FROM ?)").unwrap();
    assert_eq!(trim(&expression).specification, TrimSpecification::Leading);
    assert!(trim(&expression).trim_character.is_none());
    let expression = parse_expression("TRIM(TRAILING ? FROM ?)").unwrap();
    assert_eq!(trim(&expression).specification, TrimSpecification::Trailing);
    assert_eq!(
        parameter(trim(&expression).trim_character.as_ref().unwrap()).position,
        1
    );
}

#[test]
fn trim_grammar_retry_rolls_back_uncommitted_parameters_and_preserves_constructor_errors() {
    let expression = parse_expression("TRIM(BOTH * ? FROM ?)").unwrap();
    let Expr::Binary(character) = trim(&expression).trim_character.as_ref().unwrap() else {
        panic!("not the character expression")
    };
    assert_eq!(character.operator, BinaryOperator::Multiply);
    let Expr::Identifier(name) = character.left.as_ref() else {
        panic!("lost BOTH identifier")
    };
    assert_eq!(name.value, "BOTH");
    assert_eq!(parameter(&character.right).position, 1);
    assert_eq!(parameter(&trim(&expression).trim_source).position, 0);
    let expression = parse_expression("TRIM(BOTH + ? FROM ?)").unwrap();
    let Expr::Unary(character) = trim(&expression).trim_character.as_ref().unwrap() else {
        panic!("not explicit BOTH + unary character")
    };
    assert_eq!(character.operator, UnaryOperator::Plus);
    assert_eq!(parameter(&character.expression).position, 1);
    // The speculative specification branch consumes (?) before FILTER proves
    // that BOTH belongs to a function call. Its event must be rolled back.
    let source = "TRIM(BOTH(?) FILTER (WHERE ?) FROM ?)";
    let expression = parse_expression(source).unwrap();
    let value = trim(&expression);
    assert_eq!(parameter(&value.trim_source).position, 0);
    let character = call(value.trim_character.as_ref().unwrap());
    assert_eq!(parameter(character.filter.as_ref().unwrap()).position, 1);
    assert_eq!(parameter(&character.arguments[0].value).position, 2);
    let source = "TRIM(BOTH CAST(? AS DECIMAL(9223372036854775808)) FROM ?)";
    let ParserError::Parse(ParseError::UnexpectedToken { span, .. }) =
        parse_expression(source).unwrap_err()
    else {
        panic!("not numeric constructor rejection")
    };
    let start = source.find("9223372036854775808").unwrap();
    assert_eq!(span, Span::new(start, start + 19));
}

#[test]
fn overlay_preserves_each_value_and_optional_length_without_filling_a_default() {
    let source = "OVERLAY(? PLACING ? FROM ? FOR ?)";
    let expression = parse_expression(source).unwrap();
    let Expr::Overlay(value) = expression else {
        panic!("not overlay")
    };
    assert_eq!(parameter(&value.value).position, 0);
    assert_eq!(parameter(&value.replacement).position, 1);
    assert_eq!(parameter(&value.start).position, 2);
    assert_eq!(parameter(value.length.as_ref().unwrap()).position, 3);
    assert_eq!(value.span, Span::new(0, source.len()));
    let expression = parse_expression("OVERLAY(? PLACING ? FROM ?)").unwrap();
    let Expr::Overlay(value) = expression else {
        panic!("not overlay")
    };
    assert!(value.length.is_none());
}

#[test]
fn grouping_uses_the_existing_qualified_name_canonical_constructor() {
    let source = "GROUPING(T.\"MiX\", \"Two Words\")";
    let expression = parse_expression(source).unwrap();
    let Expr::GroupingOperation(value) = expression else {
        panic!("not grouping")
    };
    assert_eq!(value.grouping_columns.len(), 2);
    let Expr::Dereference(first) = &value.grouping_columns[0] else {
        panic!("not a qualified name chain")
    };
    let Expr::Identifier(base) = &first.base else {
        panic!("not a name base")
    };
    assert_eq!(
        base,
        &Ident {
            value: "t".into(),
            quoted: false,
            span: SYNTHETIC
        }
    );
    assert_eq!(
        first.field,
        Some(Ident {
            value: "mix".into(),
            quoted: false,
            span: SYNTHETIC
        })
    );
    assert_eq!(first.span, SYNTHETIC);
    let Expr::Identifier(second) = &value.grouping_columns[1] else {
        panic!("not an identifier")
    };
    assert_eq!(
        second,
        &Ident {
            value: "two words".into(),
            quoted: true,
            span: SYNTHETIC
        }
    );
    assert_eq!(value.span, Span::new(0, source.len()));
}

#[test]
fn listagg_has_all_five_default_arguments_within_group_and_no_implicit_comma() {
    let source = "LISTAGG('x') WITHIN GROUP (ORDER BY 1)";
    let expression = parse_expression(source).unwrap();
    let value = call(&expression);
    assert_eq!(value.name.parts[0].value, "LISTAGG");
    assert_eq!(value.name.span, SYNTHETIC);
    assert_eq!(value.arguments.len(), 5);
    assert_eq!(
        literal_kind(&value.arguments[0].value),
        &LiteralKind::String("x".into())
    );
    assert_eq!(
        literal_kind(&value.arguments[1].value),
        &LiteralKind::String("".into())
    );
    assert_eq!(
        literal_kind(&value.arguments[2].value),
        &LiteralKind::Boolean(true)
    );
    assert_eq!(
        literal_kind(&value.arguments[3].value),
        &LiteralKind::String("...".into())
    );
    assert_eq!(
        literal_kind(&value.arguments[4].value),
        &LiteralKind::Boolean(false)
    );
    for argument in &value.arguments[1..] {
        assert_eq!(argument.span, Span::new(0, source.len()));
    }
    let order = value.order_by.as_ref().unwrap();
    assert_eq!(order.items.len(), 1);
    assert_eq!(order.items[0].ordering, SortOrdering::Ascending);
    assert_eq!(order.items[0].null_ordering, NullOrdering::Undefined);
    assert!(!value.distinct);
    assert!(value.filter.is_none() && value.window.is_none());
    assert!(value.null_treatment.is_none() && value.processing_mode.is_none());
}

#[test]
fn listagg_keeps_explicit_overflow_facts_and_special_window_order_parameter_visits() {
    let source = "LISTAGG(DISTINCT ?, ';' ON OVERFLOW TRUNCATE 'cut' WITH COUNT) WITHIN GROUP (ORDER BY ? DESC NULLS LAST) FILTER (WHERE ?) OVER (PARTITION BY ? ORDER BY ?)";
    let expression = parse_expression(source).unwrap();
    let value = call(&expression);
    assert!(value.distinct);
    assert_eq!(value.arguments.len(), 5);
    assert_eq!(parameter(&value.arguments[0].value).position, 3);
    assert_eq!(
        literal_kind(&value.arguments[1].value),
        &LiteralKind::String(";".into())
    );
    assert_eq!(
        literal_kind(&value.arguments[2].value),
        &LiteralKind::Boolean(false)
    );
    assert_eq!(
        literal_kind(&value.arguments[3].value),
        &LiteralKind::String("cut".into())
    );
    assert_eq!(
        literal_kind(&value.arguments[4].value),
        &LiteralKind::Boolean(true)
    );
    let separator = source.find("';'").unwrap();
    let filler = source.find("'cut'").unwrap();
    assert_eq!(value.arguments[1].span, Span::new(separator, separator + 3));
    assert_eq!(value.arguments[3].span, Span::new(filler, filler + 5));
    let order = value.order_by.as_ref().unwrap();
    assert_eq!(parameter(&order.items[0].sort_key).position, 2);
    assert_eq!(order.items[0].ordering, SortOrdering::Descending);
    assert_eq!(order.items[0].null_ordering, NullOrdering::Last);
    assert_eq!(parameter(value.filter.as_ref().unwrap()).position, 4);
    let Some(Window::Specification(window)) = &value.window else {
        panic!("not a full window")
    };
    assert_eq!(parameter(&window.partition_by[0]).position, 0);
    assert_eq!(
        parameter(&window.order_by.as_ref().unwrap().items[0].sort_key).position,
        1
    );
    let expression = parse_expression(
        "LISTAGG(ALL ? ON OVERFLOW TRUNCATE WITHOUT COUNT) WITHIN GROUP (ORDER BY ?) OVER w",
    )
    .unwrap();
    let value = call(&expression);
    assert!(!value.distinct);
    assert_eq!(
        literal_kind(&value.arguments[3].value),
        &LiteralKind::String("...".into())
    );
    assert_eq!(
        literal_kind(&value.arguments[4].value),
        &LiteralKind::Boolean(false)
    );
    assert!(matches!(&value.window, Some(Window::Reference(_))));
}

#[test]
fn malformed_special_forms_reject_at_their_grammar_or_constructor_boundary() {
    for source in [
        "POSITION(1 IN)",
        "SUBSTRING('x' FROM)",
        "NORMALIZE('x', 'NFC')",
        "NORMALIZE(?) OVER w",
        "EXTRACT(\"YEAR\" FROM ?)",
        "EXTRACT(NOT_A_FIELD FROM ?)",
        "GROUPING()",
        "GROUPING(a.*)",
        "TRIM(FROM ?)",
        "TRIM(?, ?, ?)",
        "OVERLAY(? PLACING ?)",
        "LISTAGG() WITHIN GROUP (ORDER BY ?)",
        "LISTAGG(?, ?) WITHIN GROUP (ORDER BY ?)",
        "LISTAGG(? ON OVERFLOW TRUNCATE) WITHIN GROUP (ORDER BY ?)",
        "LISTAGG(?)",
    ] {
        assert!(
            matches!(parse_expression(source), Err(ParserError::Parse(_))),
            "{source}"
        );
    }
}

#[test]
fn listagg_all_complete_alternatives_keep_value_identity_and_parameter_visits() {
    let expression = parse_expression("LISTAGG(ALL) WITHIN GROUP (ORDER BY x)").unwrap();
    let value = call(&expression);
    assert!(!value.distinct);
    assert!(matches!(&value.arguments[0].value, Expr::Identifier(value) if value.value == "ALL"));
    let expression = parse_expression("LISTAGG(ALL a) WITHIN GROUP (ORDER BY x)").unwrap();
    assert!(
        matches!(&call(&expression).arguments[0].value, Expr::Identifier(value) if value.value == "a")
    );
    let expression = parse_expression("LISTAGG(DISTINCT ALL) WITHIN GROUP (ORDER BY x)").unwrap();
    assert!(call(&expression).distinct);
    assert!(
        matches!(&call(&expression).arguments[0].value, Expr::Identifier(value) if value.value == "ALL")
    );
    let expression = parse_expression("LISTAGG(ALL(?)) WITHIN GROUP (ORDER BY ?)").unwrap();
    let value = call(&expression);
    assert_eq!(parameter(&value.arguments[0].value).position, 1);
    assert_eq!(
        parameter(&value.order_by.as_ref().unwrap().items[0].sort_key).position,
        0
    );
    let expression =
        parse_expression("LISTAGG(ALL(?) FILTER (WHERE ?)) WITHIN GROUP (ORDER BY ?)").unwrap();
    let value = call(&expression);
    let inner = call(&value.arguments[0].value);
    assert_eq!(inner.name.parts[0].value, "ALL");
    assert_eq!(parameter(&inner.arguments[0].value).position, 2);
    assert_eq!(parameter(inner.filter.as_ref().unwrap()).position, 1);
    assert_eq!(
        parameter(&value.order_by.as_ref().unwrap().items[0].sort_key).position,
        0
    );
    let source = "LISTAGG(ALL IF()) WITHIN GROUP (ORDER BY x)";
    let error = parse_expression(source).unwrap_err();
    let ParserError::Parse(ParseError::UnexpectedToken { span, found, .. }) = error else {
        panic!("not construction diagnostic")
    };
    let start = source.find("IF()").unwrap();
    assert_eq!(span, Span::new(start, start + 4));
    assert_eq!(found, "IF");
}
