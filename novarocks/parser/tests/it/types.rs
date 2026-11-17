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
use novarocks_parser::trino_parser::{parse_expression, parse_type};
use novarocks_parser::{ParseError, ParserError, Span};

fn literal(source: &str) -> Literal {
    match parse_expression(source).unwrap() {
        Expr::Literal(value) => value,
        other => panic!("expected a literal for {source:?}, found {other:?}"),
    }
}

fn generic(node: &TypeName) -> &GenericDataType {
    match &node.kind {
        TypeKind::Generic(value) => value,
        other => panic!("expected a generic type, found {other:?}"),
    }
}

fn type_parameter(parameter: &DataTypeParameter) -> &TypeName {
    match parameter {
        DataTypeParameter::Type(value) => &value.data_type,
        other => panic!("expected a type parameter, found {other:?}"),
    }
}

fn numeric(parameter: &DataTypeParameter, spelling: &str, value: i64) {
    let DataTypeParameter::Numeric(number) = parameter else {
        panic!("not numeric: {parameter:?}")
    };
    assert_eq!(number.spelling, spelling);
    assert_eq!(number.parsed_value, value);
}

#[test]
fn integer_literals_keep_token_text_signed_range_and_bases() {
    for (source, spelling, parsed_value) in [
        ("1_234", "1_234", 1234),
        ("0X_fF", "0X_fF", 255),
        ("0b_1010", "0b_1010", 10),
        ("0O_17", "0O_17", 15),
        (
            "- /* sign */ 0x8000000000000000",
            "-0x8000000000000000",
            i64::MIN,
        ),
        (
            "9_223_372_036_854_775_807",
            "9_223_372_036_854_775_807",
            i64::MAX,
        ),
    ] {
        let actual = literal(source);
        assert_eq!(
            actual.kind,
            LiteralKind::Long {
                spelling: spelling.to_owned(),
                parsed_value
            }
        );
        assert_eq!(actual.span, Span::new(0, source.len()));
    }
    for source in [
        "9223372036854775808",
        "-9223372036854775809",
        "0xffffffffffffffff",
    ] {
        let error = parse_expression(source).unwrap_err();
        assert!(
            matches!(error, ParserError::Parse(_)),
            "{source}: {error:?}"
        );
    }
    // The explicit plus is a unary node; it is not part of `number`.
    let Expr::Unary(positive) = parse_expression("+1").unwrap() else {
        panic!("missing unary plus")
    };
    assert_eq!(positive.operator, UnaryOperator::Plus);
    assert!(matches!(
        positive.expression.as_ref(),
        Expr::Literal(Literal {
            kind: LiteralKind::Long {
                parsed_value: 1,
                ..
            },
            ..
        })
    ));
}

#[test]
fn decimals_and_doubles_preserve_their_distinct_scalar_facts() {
    assert_eq!(
        literal("- /* comment */ 1_2.3_0").kind,
        LiteralKind::Decimal("-1_2.3_0".to_owned())
    );
    assert_eq!(
        literal("DECIMAL '0001.2300'").kind,
        LiteralKind::Decimal("0001.2300".to_owned())
    );
    for (source, bits) in [
        ("1.25e2", 125.0_f64.to_bits()),
        ("-0e0", (-0.0_f64).to_bits()),
        ("1e999", f64::INFINITY.to_bits()),
    ] {
        let LiteralKind::Double(value) = literal(source).kind else {
            panic!("not a double")
        };
        assert_eq!(value.get().to_bits(), bits, "{source}");
    }
    assert_eq!(
        literal("DOUBLE PRECISION 'NaN'").kind,
        LiteralKind::Generic(GenericLiteral {
            type_name: "DOUBLE".to_owned(),
            value: "NaN".to_owned(),
        })
    );
    assert_eq!(
        literal("\"decimal\" '1.25'").kind,
        LiteralKind::Generic(GenericLiteral {
            type_name: "\"decimal\"".to_owned(),
            value: "1.25".to_owned(),
        })
    );
    assert_eq!(
        literal("dAtE '2026-11-18'").kind,
        LiteralKind::Generic(GenericLiteral {
            type_name: "dAtE".to_owned(),
            value: "2026-11-18".to_owned(),
        })
    );
}

#[test]
fn strings_decode_sql_quotes_unicode_and_binary_without_mysql_escapes() {
    for (source, expected) in [
        (r"'a\n''b'", "a\\n'b"),
        (r"U&'a\0062\+01F600\\'", "ab😀\\"),
        ("U&'!0061!!''b' UESCAPE '!'", "a!'b"),
    ] {
        let value = literal(source);
        assert_eq!(value.kind, LiteralKind::String(expected.to_owned()));
        assert_eq!(value.span, Span::new(0, source.len()));
    }
    // Guava's whole-string uppercase also expands the Latin ff ligature.
    assert_eq!(
        literal("X'00\u{2007}ff ﬀ'").kind,
        LiteralKind::Binary(vec![0, 255, 255])
    );
    assert_eq!(literal("X''").kind, LiteralKind::Binary(vec![]));
    for source in [
        r"U&'\D800'",
        r"U&'\+110000'",
        r"U&'\123'",
        "U&'x' UESCAPE 'a'",
        "U&'x' UESCAPE '😀'",
        "X'0'",
        "X'0g'",
        "$$routine$$",
    ] {
        let error = parse_expression(source).unwrap_err();
        assert!(
            matches!(error, ParserError::Parse(_)),
            "{source}: {error:?}"
        );
    }
}

#[test]
fn generic_row_and_array_types_keep_names_order_and_parameter_spans() {
    let source = "ROW(\"A\" DECIMAL(20, 0), BIGINT, \"Case\" ARRAY<ROW(x INTEGER)>)";
    let row = parse_type(source).unwrap();
    assert_eq!(row.span, Span::new(0, source.len()));
    let TypeKind::Row(value) = &row.kind else {
        panic!("not row")
    };
    assert_eq!(value.fields.len(), 3);
    assert_eq!(
        value.fields[0].name,
        Some(Ident {
            value: "A".to_owned(),
            quoted: true,
            span: Span::new(4, 7)
        })
    );
    assert_eq!(value.fields[0].span, Span::new(4, 22));
    let decimal = generic(&value.fields[0].data_type);
    assert_eq!(decimal.name.value, "DECIMAL");
    numeric(&decimal.arguments[0], "20", 20);
    numeric(&decimal.arguments[1], "0", 0);
    let DataTypeParameter::Numeric(parameter) = &decimal.arguments[0] else {
        unreachable!()
    };
    assert_eq!(parameter.span, Span::new(16, 18));
    assert!(value.fields[1].name.is_none());
    assert_eq!(generic(&value.fields[1].data_type).name.value, "BIGINT");
    let array = generic(&value.fields[2].data_type);
    assert_eq!(array.name.value, "ARRAY");
    assert_eq!(array.arguments.len(), 1);
    let TypeKind::Row(inner) = &type_parameter(&array.arguments[0]).kind else {
        panic!("not child row")
    };
    assert_eq!(inner.fields[0].name.as_ref().unwrap().value, "x");
    assert_eq!(generic(&inner.fields[0].data_type).name.value, "INTEGER");
    let map = parse_type("MAP<VARCHAR, ARRAY<BIGINT>>").unwrap();
    assert_eq!(generic(&map).arguments.len(), 2);
    assert_eq!(
        generic(type_parameter(&generic(&map).arguments[0]))
            .name
            .value,
        "VARCHAR"
    );
    assert_eq!(
        generic(type_parameter(&generic(&map).arguments[1]))
            .name
            .value,
        "ARRAY"
    );
}

#[test]
fn row_field_lookahead_uses_grammar_prefixes_not_known_type_names() {
    for (source, named) in [
        ("ROW(time WITH TIME ZONE)", false),
        ("ROW(time WITHOUT TIME ZONE)", false),
        ("ROW(INTERVAL DAY TO SECOND(3))", false),
        ("ROW(DOUBLE PRECISION)", false),
        ("ROW(foo ARRAY)", false),
        ("ROW(ROW BIGINT)", true),
        ("ROW(INTERVAL BIGINT)", true),
        ("ROW(foo ARRAY<INTEGER>)", true),
        ("ROW(foo ARRAY(INTEGER))", true),
        ("ROW(\"time\" custom_name)", true),
    ] {
        let parsed = parse_type(source).unwrap();
        let TypeKind::Row(value) = &parsed.kind else {
            panic!("not row")
        };
        assert_eq!(value.fields.len(), 1);
        assert_eq!(value.fields[0].name.is_some(), named, "{source}");
    }
}

#[test]
fn datetime_and_interval_precision_remain_typed_and_unbounded_by_capability() {
    let source = "TIMESTAMP(0x1f) WITH TIME ZONE";
    let parsed = parse_type(source).unwrap();
    let TypeKind::DateTime(value) = &parsed.kind else {
        panic!("not datetime")
    };
    assert_eq!(value.kind, DateTimeType::Timestamp);
    assert!(value.with_time_zone);
    numeric(value.precision.as_ref().unwrap(), "0x1f", 31);
    assert_eq!(parsed.span, Span::new(0, source.len()));
    let TypeKind::DateTime(plain) = parse_type("time").unwrap().kind else {
        panic!("not datetime")
    };
    assert!(plain.precision.is_none());
    assert!(!plain.with_time_zone);
    let TypeKind::DateTime(explicit) = parse_type("TIME(3) WITHOUT TIME ZONE").unwrap().kind else {
        panic!("not datetime")
    };
    assert!(!explicit.with_time_zone);
    numeric(explicit.precision.as_ref().unwrap(), "3", 3);

    let source = "INTERVAL DAY(02) TO SECOND(09)";
    let parsed = parse_type(source).unwrap();
    let TypeKind::Interval(value) = &parsed.kind else {
        panic!("not interval")
    };
    assert_eq!(value.span, Span::new(0, source.len()));
    let IntervalQualifier::Composite(qualifier) = &value.qualifier else {
        panic!("not composite")
    };
    assert_eq!(qualifier.span, Span::new(9, source.len()));
    assert_eq!(qualifier.from, IntervalQualifierField::Day);
    numeric(qualifier.precision.as_ref().unwrap(), "02", 2);
    let IntervalQualifierField::Second {
        fractional_precision: Some(precision),
    } = &qualifier.to
    else {
        panic!("missing fractional precision")
    };
    numeric(precision, "09", 9);
    let interval_literal = literal("INTERVAL - '2.3' SECOND(02, 09)");
    let LiteralKind::Interval(interval) = &interval_literal.kind else {
        panic!("not an interval literal")
    };
    assert_eq!(interval.value, "2.3");
    assert_eq!(interval.sign, IntervalSign::Negative);
    assert_eq!(interval_literal.span, Span::new(0, 31));
    let IntervalQualifier::Simple(second) = &interval.qualifier else {
        panic!("not simple")
    };
    numeric(second.precision.as_ref().unwrap(), "02", 2);
    let IntervalQualifierField::Second {
        fractional_precision: Some(fraction),
    } = &second.field
    else {
        panic!("missing fraction")
    };
    numeric(fraction, "09", 9);
    assert_eq!(second.span, Span::new(17, 31));
    for source in [
        "INTERVAL HOUR TO HOUR",
        "INTERVAL MINUTE TO HOUR",
        "INTERVAL HOUR TO DAY",
        "INTERVAL MONTH TO YEAR",
        "INTERVAL SECOND TO MINUTE",
        "ROW()",
        "foo()",
        "DECIMAL(1.5)",
        "DECIMAL(-1)",
        "DECIMAL(9223372036854775808)",
    ] {
        assert!(
            matches!(parse_type(source), Err(ParserError::Parse(_))),
            "{source}"
        );
    }
}

#[test]
fn explicit_array_lengths_keep_the_complete_integer_token_without_i64_admission() {
    let source = "INTEGER ARRAY[184467440737095516160] ARRAY";
    let parsed = parse_type(source).unwrap();
    let array = generic(&parsed);
    let TypeKind::ExplicitArray(explicit) = &type_parameter(&array.arguments[0]).kind else {
        panic!("missing explicit array")
    };
    assert_eq!(explicit.length.spelling, "184467440737095516160");
    assert_eq!(explicit.length.span, Span::new(14, 35));
    assert_eq!(generic(&explicit.element).name.value, "INTEGER");
    assert_eq!(parsed.span, Span::new(0, source.len()));
    assert!(parse_type("INTEGER ARRAY[-1]").is_err());
    assert!(parse_type("a.b").is_err());
}

#[test]
fn literal_errors_keep_the_rejected_source_range_and_type_names_stay_syntactic() {
    for source in [
        "9223372036854775808",
        "X'0g'",
        r"U&'\D800'",
        "U&'x' UESCAPE 'a'",
    ] {
        let ParserError::Parse(ParseError::UnexpectedToken { span, .. }) =
            parse_expression(source).unwrap_err()
        else {
            panic!("expected a source-located construction error for {source:?}")
        };
        assert_eq!(span, Span::new(0, source.len()));
    }
    assert_eq!(literal("NULL").kind, LiteralKind::Null);
    assert_eq!(literal("false").kind, LiteralKind::Boolean(false));
    assert_eq!(literal("TRUE").kind, LiteralKind::Boolean(true));
    let value = literal(" /* trivia */ 1 /* tail */");
    assert_eq!(value.span, Span::new(14, 15));
    assert_eq!(
        value.kind,
        LiteralKind::Long {
            spelling: "1".to_owned(),
            parsed_value: 1
        }
    );
    let quoted = parse_type("\"ARRAY\"(bigint)").unwrap();
    assert!(generic(&quoted).name.quoted);
    assert_eq!(generic(&quoted).name.value, "ARRAY");
    assert_eq!(
        generic(type_parameter(&generic(&quoted).arguments[0]))
            .name
            .value,
        "bigint"
    );
    let TypeKind::DateTime(time) = parse_type("TIME(custom_precision)").unwrap().kind else {
        panic!("not a time type")
    };
    assert_eq!(
        generic(type_parameter(time.precision.as_ref().unwrap()))
            .name
            .value,
        "custom_precision"
    );
}

#[test]
fn overlapping_type_rules_choose_grammar_alternatives_without_swallowing_constructor_errors() {
    for source in ["TIME(1,2)", "TIMESTAMP(1,2)"] {
        let parsed = parse_type(source).unwrap();
        let value = generic(&parsed);
        assert_eq!(value.name.value, source.split('(').next().unwrap());
        assert_eq!(value.arguments.len(), 2);
        numeric(&value.arguments[0], "1", 1);
        numeric(&value.arguments[1], "2", 2);
    }
    let parsed = parse_type("ROW(1)").unwrap();
    assert_eq!(generic(&parsed).name.value, "ROW");
    numeric(&generic(&parsed).arguments[0], "1", 1);
    let parsed = parse_type("ROW(time WITHOUT)").unwrap();
    let TypeKind::Row(row) = &parsed.kind else {
        panic!("not a row type")
    };
    assert_eq!(row.fields.len(), 1);
    assert_eq!(row.fields[0].name.as_ref().unwrap().value, "time");
    assert_eq!(generic(&row.fields[0].data_type).name.value, "WITHOUT");
    let parsed = parse_type("ROW(interval DAY(1,2))").unwrap();
    let TypeKind::Row(row) = &parsed.kind else {
        panic!("not a row type")
    };
    assert_eq!(row.fields[0].name.as_ref().unwrap().value, "interval");
    assert_eq!(generic(&row.fields[0].data_type).name.value, "DAY");
    numeric(&generic(&row.fields[0].data_type).arguments[1], "2", 2);
    for (source, expected_span) in [
        (
            "ROW(x ARRAY(DECIMAL(9223372036854775808,2)))",
            Span::new(20, 39),
        ),
        ("TIME(9223372036854775808,1)", Span::new(5, 24)),
        ("ROW(9223372036854775808)", Span::new(4, 23)),
        ("INTERVAL DAY(9223372036854775808)", Span::new(13, 32)),
    ] {
        let ParserError::Parse(ParseError::UnexpectedToken { span, .. }) =
            parse_type(source).unwrap_err()
        else {
            panic!("expected a numeric constructor error for {source}")
        };
        assert_eq!(span, expected_span, "{source}");
    }
}

#[test]
fn interval_constructor_rejection_keeps_qualifier_location_instead_of_generic_fallback() {
    for source in [
        "INTERVAL HOUR TO HOUR",
        "INTERVAL MINUTE TO HOUR",
        "INTERVAL MINUTE TO MINUTE",
    ] {
        let ParserError::Parse(ParseError::UnexpectedToken { span, .. }) =
            parse_type(source).unwrap_err()
        else {
            panic!("constructor error")
        };
        assert_eq!(span, Span::new(9, source.len()), "{source}");
    }
}
