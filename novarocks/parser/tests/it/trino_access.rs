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
use novarocks_parser::parser::{parse_expression, parse_query};
use novarocks_parser::{ParseError, ParserError, Span};

fn method(expression: &Expr) -> &MethodCall {
    let Expr::MethodCall(node) = expression else {
        panic!("not method: {expression:?}")
    };
    node
}
fn static_method(expression: &Expr) -> &StaticMethodCall {
    let Expr::StaticMethodCall(node) = expression else {
        panic!("not static method: {expression:?}")
    };
    node
}
fn interval(expression: &Expr) -> &IntervalValueExpression {
    let Expr::IntervalValueExpression(node) = expression else {
        panic!("not interval value: {expression:?}")
    };
    node
}
fn parameter(expression: &Expr) -> &Parameter {
    let Expr::Parameter(node) = expression else {
        panic!("not parameter: {expression:?}")
    };
    node
}
fn ident(expression: &Expr) -> &Ident {
    let Expr::Identifier(node) = expression else {
        panic!("not identifier: {expression:?}")
    };
    node
}
fn selected(query: &Query) -> &SingleColumn {
    let SetExpr::Select(select) = query.body.as_ref() else {
        panic!()
    };
    assert_eq!(select.projection.len(), 1);
    let SelectItem::SingleColumn(column) = &select.projection[0] else {
        panic!()
    };
    column
}
fn numeric(parameter: &DataTypeParameter) -> (&str, i64) {
    let DataTypeParameter::Numeric(value) = parameter else {
        panic!()
    };
    (&value.spelling, value.parsed_value)
}

#[test]
fn method_and_qualified_function_are_distinct_syntax_and_preserve_real_locations() {
    let ordinary = parse_expression("x.length()").unwrap();
    let Expr::FunctionCall(call) = ordinary else {
        panic!()
    };
    assert_eq!(
        call.name
            .parts
            .iter()
            .map(|part| part.value.as_str())
            .collect::<Vec<_>>(),
        vec!["x", "length"]
    );
    assert!(call.arguments.is_empty());
    let reserved = parse_expression("x.SELECT()").unwrap();
    assert_eq!(ident(&method(&reserved).receiver).value, "x");
    assert_eq!(method(&reserved).method.value, "SELECT");
    let quoted = parse_expression("x.\"length\"()").unwrap();
    assert!(matches!(quoted, Expr::FunctionCall(_)));
    let source = "('hello').length()";
    let expression = parse_expression(source).unwrap();
    let node = method(&expression);
    assert_eq!(node.span, Span::new(0, source.len()));
    let Expr::Literal(Literal {
        kind: LiteralKind::String(value),
        span,
    }) = &node.receiver
    else {
        panic!()
    };
    assert_eq!(value, "hello");
    assert_eq!(*span, Span::new(1, 8));
    let offset = source.find("length").unwrap();
    assert_eq!(
        node.method,
        Ident {
            value: "length".into(),
            quoted: false,
            span: Span::new(offset, offset + 6)
        }
    );
    assert!(node.arguments.is_empty());
    let expression = parse_expression("upper('a').length()").unwrap();
    assert!(matches!(
        &method(&expression).receiver,
        Expr::FunctionCall(_)
    ));
    let expression = parse_expression("((x)).length()").unwrap();
    assert_eq!(ident(&method(&expression).receiver).span, Span::new(2, 3));
    assert_eq!(method(&expression).span.start(), 0);
}

#[test]
fn method_arguments_borrow_one_parameter_author_and_keep_names_and_value_spans() {
    let bare = parse_expression("?.SELECT(?)").unwrap();
    assert_eq!(parameter(&method(&bare).receiver).position, 0);
    assert_eq!(parameter(&method(&bare).arguments[0].value).position, 1);
    assert_eq!(method(&bare).method.value, "SELECT");
    let source = "(?).contains(?, \"element\" => ?)";
    let expression = parse_expression(source).unwrap();
    let node = method(&expression);
    assert_eq!(
        parameter(&node.receiver),
        &Parameter {
            position: 0,
            span: Span::new(1, 2)
        }
    );
    assert_eq!(node.arguments.len(), 2);
    assert!(node.arguments[0].name.is_none());
    assert_eq!(parameter(&node.arguments[0].value).position, 1);
    assert_eq!(node.arguments[0].span, node.arguments[0].value.span());
    let named = &node.arguments[1];
    let name = named.name.as_ref().unwrap();
    assert_eq!(name.value, "element");
    assert!(name.quoted);
    assert_eq!(&source[name.span.start()..name.span.end()], "\"element\"");
    assert_eq!(parameter(&named.value).position, 2);
    assert_eq!(
        &source[named.span.start()..named.span.end()],
        "\"element\" => ?"
    );
    assert_eq!(
        &source[named.value.span().start()..named.value.span().end()],
        "?"
    );
}

#[test]
fn static_calls_use_real_qualified_names_and_method_only_keyword_privilege() {
    let source = "cat.\"sch\".t::SELECT(value => ?, ?)";
    let expression = parse_expression(source).unwrap();
    let node = static_method(&expression);
    assert_eq!(node.span, Span::new(0, source.len()));
    assert_eq!(
        node.type_name
            .parts
            .iter()
            .map(|part| (part.value.as_str(), part.quoted))
            .collect::<Vec<_>>(),
        vec![("cat", false), ("sch", true), ("t", false)]
    );
    assert_eq!(node.method.value, "SELECT");
    assert!(!node.method.quoted);
    assert_eq!(
        &source[node.method.span.start()..node.method.span.end()],
        "SELECT"
    );
    assert_eq!(node.arguments[0].name.as_ref().unwrap().value, "value");
    assert_eq!(parameter(&node.arguments[0].value).position, 0);
    assert!(node.arguments[1].name.is_none());
    assert_eq!(parameter(&node.arguments[1].value).position, 1);
    for source in ["t::values()", "('a').trim()", "(x).\"select\"()"] {
        let expression = parse_expression(source).unwrap();
        let method = match &expression {
            Expr::StaticMethodCall(value) => &value.method,
            _ => &method(&expression).method,
        };
        assert_eq!(method.quoted, source.contains('"'));
    }
    for source in [
        "varchar(5)::parse('42')",
        "ARRAY(BIGINT)::parse(1)",
        "1::parse()",
        "t::123()",
        "(x).SELECT(DISTINCT 1)",
        "(x).m(1 ORDER BY 2)",
    ] {
        assert!(parse_expression(source).is_err(), "{source}");
    }
}

#[test]
fn interval_values_move_subtraction_children_and_keep_separate_qualifier_extent() {
    let source = "(a - b) DAY(0x9) TO SECOND(0b10)";
    let expression = parse_expression(source).unwrap();
    let node = interval(&expression);
    assert_eq!(
        ident(&node.left),
        &Ident {
            value: "a".into(),
            quoted: false,
            span: Span::new(1, 2)
        }
    );
    assert_eq!(
        ident(&node.right),
        &Ident {
            value: "b".into(),
            quoted: false,
            span: Span::new(5, 6)
        }
    );
    assert_eq!(node.span, Span::new(0, source.len()));
    // The type node is located at the IntervalValueExpression context,
    // while the qualifier retains its own actual token range.
    assert_eq!(node.data_type.span, node.span);
    let IntervalQualifier::Composite(qualifier) = &node.data_type.qualifier else {
        panic!()
    };
    assert_eq!(
        qualifier.span,
        Span::new(source.find("DAY").unwrap(), source.len())
    );
    assert_eq!(qualifier.from, IntervalQualifierField::Day);
    assert_eq!(numeric(qualifier.precision.as_deref().unwrap()), ("0x9", 9));
    let IntervalQualifierField::Second {
        fractional_precision,
    } = &qualifier.to
    else {
        panic!()
    };
    assert_eq!(
        numeric(fractional_precision.as_deref().unwrap()),
        ("0b10", 2)
    );
    let expression = parse_expression("(a - b - c) DAY").unwrap();
    let Expr::Binary(left) = &interval(&expression).left else {
        panic!()
    };
    assert_eq!(left.operator, BinaryOperator::Subtract);
    assert_eq!(ident(&left.left).value, "a");
    assert_eq!(ident(&left.right).value, "b");
    assert_eq!(ident(&interval(&expression).right).value, "c");
}

#[test]
fn interval_qualifiers_retain_all_valid_fields_and_type_precision_facts() {
    for (source, expected) in [
        ("(a - b) YEAR", IntervalQualifierField::Year),
        ("(a - b) MONTH", IntervalQualifierField::Month),
        ("(a - b) DAY", IntervalQualifierField::Day),
        ("(a - b) HOUR", IntervalQualifierField::Hour),
        ("(a - b) MINUTE", IntervalQualifierField::Minute),
    ] {
        let expression = parse_expression(source).unwrap();
        let IntervalQualifier::Simple(qualifier) = &interval(&expression).data_type.qualifier
        else {
            panic!()
        };
        assert_eq!(qualifier.field, expected);
        assert!(qualifier.precision.is_none());
    }
    let expression = parse_expression("(a - b) SECOND(2, 3)").unwrap();
    let IntervalQualifier::Simple(qualifier) = &interval(&expression).data_type.qualifier else {
        panic!()
    };
    assert_eq!(numeric(qualifier.precision.as_deref().unwrap()), ("2", 2));
    let IntervalQualifierField::Second {
        fractional_precision,
    } = &qualifier.field
    else {
        panic!()
    };
    assert_eq!(numeric(fractional_precision.as_deref().unwrap()), ("3", 3));
    let expression = parse_expression("(a - b) DAY(BIGINT) TO SECOND(INTEGER)").unwrap();
    let IntervalQualifier::Composite(qualifier) = &interval(&expression).data_type.qualifier else {
        panic!()
    };
    assert!(matches!(
        qualifier.precision.as_deref(),
        Some(DataTypeParameter::Type(_))
    ));
    let IntervalQualifierField::Second {
        fractional_precision,
    } = &qualifier.to
    else {
        panic!()
    };
    assert!(matches!(
        fractional_precision.as_deref(),
        Some(DataTypeParameter::Type(_))
    ));
    for source in [
        "(a - b) YEAR TO MONTH",
        "(a - b) DAY TO HOUR",
        "(a - b) DAY TO MINUTE",
        "(a - b) HOUR TO MINUTE",
        "(a - b) HOUR TO SECOND",
        "(a - b) MINUTE TO SECOND",
    ] {
        assert!(matches!(
            interval(&parse_expression(source).unwrap())
                .data_type
                .qualifier,
            IntervalQualifier::Composite(_)
        ));
    }
}

#[test]
fn actual_alias_context_selects_parentheses_while_delimited_expressions_reset_it() {
    let source = "SELECT (a - b) DAY";
    let query = parse_query(source).unwrap();
    let column = selected(&query);
    assert_eq!(column.alias.as_ref().unwrap().value, "DAY");
    let Expr::Binary(binary) = &column.expression else {
        panic!()
    };
    assert_eq!(binary.operator, BinaryOperator::Subtract);
    assert_eq!(binary.span, Span::new(8, 13));
    assert!(matches!(
        parse_expression("(a - b) DAY").unwrap(),
        Expr::IntervalValueExpression(_)
    ));
    let expression = parse_expression("ROW((a - b) DAY)").unwrap();
    let Expr::Row(row) = expression else { panic!() };
    assert_eq!(row.fields[0].name.as_ref().unwrap().value, "DAY");
    assert!(matches!(row.fields[0].expression, Expr::Binary(_)));
    for source in [
        "SELECT ((a - b) DAY)",
        "SELECT (a - b) DAY TO SECOND",
        "SELECT (a - b) DAY AS difference",
    ] {
        let query = parse_query(source).unwrap();
        assert!(
            matches!(
                selected(&query).expression,
                Expr::IntervalValueExpression(_)
            ),
            "{source}"
        );
    }
    let query = parse_query("SELECT x -> (a - b) DAY").unwrap();
    let column = selected(&query);
    assert_eq!(column.alias.as_ref().unwrap().value, "DAY");
    let Expr::Lambda(lambda) = &column.expression else {
        panic!()
    };
    assert!(matches!(lambda.body, Expr::Binary(_)));
    let expression = parse_expression("x -> (a - b) DAY").unwrap();
    let Expr::Lambda(lambda) = expression else {
        panic!()
    };
    assert!(matches!(lambda.body, Expr::IntervalValueExpression(_)));
    let query = parse_query("SELECT COALESCE((a - b) DAY, 0)").unwrap();
    let Expr::Coalesce(coalesce) = &selected(&query).expression else {
        panic!()
    };
    assert!(matches!(
        coalesce.operands[0],
        Expr::IntervalValueExpression(_)
    ));
}

#[test]
fn interval_subtraction_check_precedes_qualifier_construction_and_preserves_real_error_range() {
    for source in [
        "(a + b) DAY TO SECOND",
        "(a + b) DAY(9223372036854775808)",
        "(a) MONTH",
    ] {
        let ParserError::Parse(ParseError::UnexpectedToken { found, span, .. }) =
            parse_expression(source).unwrap_err()
        else {
            panic!()
        };
        let prefix_end = source.find(')').unwrap() + 1;
        assert_eq!(span, Span::new(0, prefix_end));
        assert_eq!(found, source[..prefix_end]);
    }
    let source = "(a - b) DAY(9223372036854775808)";
    let ParserError::Parse(ParseError::UnexpectedToken { found, span, .. }) =
        parse_expression(source).unwrap_err()
    else {
        panic!()
    };
    assert_eq!(found, "9223372036854775808");
    assert_eq!(&source[span.start()..span.end()], found);
    for source in [
        "(a - b) YEAR TO DAY",
        "(a - b) HOUR TO HOUR",
        "(a - b) MINUTE TO HOUR",
        "(a - b) SECOND TO MINUTE",
    ] {
        assert!(
            matches!(parse_expression(source), Err(ParserError::Parse(_))),
            "{source}"
        );
    }
}

#[test]
fn method_and_interval_nested_children_preserve_actual_parameter_order() {
    let source = "((? - ?) DAY).m(value => ?, other => t::parse(?))";
    let expression = parse_expression(source).unwrap();
    let node = method(&expression);
    let receiver = interval(&node.receiver);
    assert_eq!(parameter(&receiver.left).position, 0);
    assert_eq!(parameter(&receiver.right).position, 1);
    assert_eq!(parameter(&node.arguments[0].value).position, 2);
    let nested = static_method(&node.arguments[1].value);
    assert_eq!(parameter(&nested.arguments[0].value).position, 3);
    let offsets = source
        .match_indices('?')
        .map(|(offset, _)| Span::new(offset, offset + 1))
        .collect::<Vec<_>>();
    assert_eq!(
        vec![
            receiver.left.span(),
            receiver.right.span(),
            node.arguments[0].value.span(),
            nested.arguments[0].value.span()
        ],
        offsets
    );
}

#[test]
fn deeply_chained_method_source_keeps_the_original_receiver_and_node_order() {
    let depth = 2048;
    let mut source = String::from("('seed')");
    for _ in 0..depth {
        source.push_str(".step()");
    }
    let expression = parse_expression(&source).unwrap();
    let mut cursor = &expression;
    let mut end = source.len();
    for _ in 0..depth {
        let node = method(cursor);
        assert_eq!(node.method.value, "step");
        assert!(node.arguments.is_empty());
        assert_eq!(node.span, Span::new(0, end));
        end -= ".step()".len();
        cursor = &node.receiver;
    }
    assert!(
        matches!(cursor, Expr::Literal(Literal { kind: LiteralKind::String(value), span }) if value == "seed" && *span == Span::new(1, 7))
    );
    assert!(expression.syntax_eq(&expression.clone()));
}
