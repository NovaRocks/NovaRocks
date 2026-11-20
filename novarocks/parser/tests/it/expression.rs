// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. See the License for the
// specific language governing permissions and limitations
// under the License.

use novarocks_parser::ast::*;
use novarocks_parser::{ParseError, ParserError, Span, parser::parse_expression};

#[test]
fn arithmetic_and_concat_follow_the_value_grammar_and_keep_true_operand_extent() {
    let Expr::Binary(product) = parse_expression("(1 + 2) * 3").unwrap() else {
        panic!("product")
    };
    assert_eq!(product.operator, BinaryOperator::Multiply);
    assert_eq!(product.left_operand_span, Span::new(0, 7));
    let Expr::Binary(sum) = product.left.as_ref() else {
        panic!("sum")
    };
    assert_eq!(sum.operator, BinaryOperator::Add);
    assert_eq!(sum.span, Span::new(1, 6));
    let Expr::FunctionCall(concat) = parse_expression("1 + 2 || 3 * 4").unwrap() else {
        panic!("concat")
    };
    assert_eq!(concat.name.parts[0].value, "concat");
    assert_eq!(concat.arguments.len(), 2);
    assert!(
        matches!(&concat.arguments[0].value, Expr::Binary(value) if value.operator == BinaryOperator::Add)
    );
    assert!(
        matches!(&concat.arguments[1].value, Expr::Binary(value) if value.operator == BinaryOperator::Multiply)
    );
}

#[test]
fn logical_flattening_respects_parenthesized_grammar_contexts() {
    let Expr::Logical(flat) = parse_expression("a OR b OR c").unwrap() else {
        panic!("OR")
    };
    assert_eq!(flat.operator, LogicalOperator::Or);
    assert_eq!(flat.terms.len(), 3);
    let Expr::Logical(left) = parse_expression("(a OR b) OR c").unwrap() else {
        panic!("OR")
    };
    assert_eq!(left.terms.len(), 2);
    assert!(
        matches!(&left.terms[0], Expr::Logical(inner) if inner.operator == LogicalOperator::Or && inner.terms.len() == 2)
    );
    let Expr::Logical(right) = parse_expression("a OR (b OR c)").unwrap() else {
        panic!("OR")
    };
    assert_eq!(right.terms.len(), 2);
    assert!(
        matches!(&right.terms[1], Expr::Logical(inner) if inner.operator == LogicalOperator::Or && inner.terms.len() == 2)
    );
    let Expr::Logical(mixed) = parse_expression("a OR b AND c").unwrap() else {
        panic!("OR")
    };
    assert_eq!(mixed.terms.len(), 2);
    assert!(
        matches!(&mixed.terms[1], Expr::Logical(inner) if inner.operator == LogicalOperator::And && inner.terms.len() == 2)
    );
}

#[test]
fn number_signs_and_nonreserved_interval_names_keep_their_actual_productions() {
    assert!(
        matches!(parse_expression("-1").unwrap(), Expr::Literal(Literal { kind: LiteralKind::Long { spelling, parsed_value: -1 }, .. }) if spelling == "-1")
    );
    assert!(
        matches!(parse_expression("+1").unwrap(), Expr::Unary(node) if node.operator == UnaryOperator::Plus)
    );
    assert!(
        matches!(parse_expression("-(1)").unwrap(), Expr::Unary(node) if node.operator == UnaryOperator::Minus)
    );
    for (source, expected) in [
        ("INTERVAL + value", BinaryOperator::Add),
        ("INTERVAL - value", BinaryOperator::Subtract),
    ] {
        let Expr::Binary(node) = parse_expression(source).unwrap() else {
            panic!("arithmetic column")
        };
        assert_eq!(node.operator, expected);
        assert!(matches!(node.left.as_ref(), Expr::Identifier(name) if name.value == "INTERVAL"));
    }
}

#[test]
fn predicates_not_and_value_boundaries_remain_distinct() {
    let Expr::Logical(logical) = parse_expression("NOT a = b AND c").unwrap() else {
        panic!("AND")
    };
    assert_eq!(logical.operator, LogicalOperator::And);
    assert_eq!(logical.terms.len(), 2);
    let Expr::Unary(not) = &logical.terms[0] else {
        panic!("NOT")
    };
    assert_eq!(not.operator, UnaryOperator::Not);
    assert!(
        matches!(not.expression.as_ref(), Expr::Predicated(node) if matches!(node.predicate, Predicate::Comparison(_)))
    );
    let Expr::Predicated(value) = parse_expression("a IS NOT DISTINCT FROM b").unwrap() else {
        panic!("predicate")
    };
    assert!(matches!(
        value.predicate,
        Predicate::DistinctFrom(DistinctFromPredicate { negated: true, .. })
    ));
    let Expr::Predicated(value) = parse_expression("a NOT BETWEEN 1 AND 2").unwrap() else {
        panic!("between")
    };
    assert!(matches!(
        value.predicate,
        Predicate::Between(BetweenPredicate { negated: true, .. })
    ));
    for source in [
        "a = b = c",
        "a = NOT b",
        "a + NOT b",
        "a IN ()",
        "a LIKE b ESCAPE",
        "a IS",
    ] {
        assert!(parse_expression(source).is_err(), "{source}");
    }
}

#[test]
fn parameters_are_committed_productions_and_keep_control_call_visit_holes() {
    let Expr::FunctionCall(call) = parse_expression("f('?', ?) /* ? */").unwrap() else {
        panic!("call")
    };
    assert!(
        matches!(&call.arguments[1].value, Expr::Parameter(parameter) if parameter.position == 0 && parameter.span == Span::new(7, 8))
    );
    // The pinned builder visits ORDER BY before arguments, then IF discards it.
    let Expr::If(call) = parse_expression("IF(?, ? ORDER BY ?)").unwrap() else {
        panic!("IF")
    };
    assert!(matches!(&call.condition, Expr::Parameter(parameter) if parameter.position == 1));
    assert!(matches!(&call.true_value, Expr::Parameter(parameter) if parameter.position == 2));
    assert!(call.false_value.is_none());
}

#[test]
fn control_calls_enforce_the_reference_constructor_constraints() {
    assert!(matches!(
        parse_expression("IF(TRUE, 1, 2)").unwrap(),
        Expr::If(_)
    ));
    assert!(matches!(
        parse_expression("NULLIF(1, 2)").unwrap(),
        Expr::NullIf(_)
    ));
    assert!(matches!(
        parse_expression("COALESCE(1, 2)").unwrap(),
        Expr::Coalesce(_)
    ));
    assert!(matches!(parse_expression("TRY(1)").unwrap(), Expr::Try(_)));
    assert!(matches!(
        parse_expression("FORMAT('%s', 1)").unwrap(),
        Expr::Format(_)
    ));
    for source in [
        "IF(1)",
        "NULLIF(1)",
        "COALESCE(1)",
        "TRY(1,2)",
        "FORMAT(1)",
        "IF(a=>1,b=>2)",
        "IF(DISTINCT 1,2)",
        "TRY(1) FILTER(WHERE TRUE)",
        "f(DISTINCT)",
        "f(ALL *)",
        "f(* ORDER BY 1)",
        "f(*) IGNORE NULLS OVER ()",
    ] {
        assert!(parse_expression(source).is_err(), "{source}");
    }
}

#[test]
fn standalone_expressions_require_the_original_end_boundary() {
    for (source, span) in [
        ("1;", Span::new(1, 2)),
        ("1 2", Span::new(2, 3)),
        ("a.*", Span::new(1, 2)),
        ("(1", Span::new(2, 2)),
    ] {
        let ParserError::Parse(ParseError::UnexpectedToken { span: actual, .. }) =
            parse_expression(source).unwrap_err()
        else {
            panic!("syntax boundary")
        };
        assert_eq!(actual, span, "{source}");
    }
}

#[test]
fn deeply_nested_parentheses_and_calls_use_the_default_test_thread_stack() {
    let parentheses = format!("{}1{}", "(".repeat(10_000), ")".repeat(10_000));
    assert!(matches!(
        parse_expression(&parentheses).unwrap(),
        Expr::Literal(_)
    ));
    let calls = format!("{}1{}", "f(".repeat(5_000), ")".repeat(5_000));
    let parsed = parse_expression(&calls).unwrap();
    let printed = novarocks_parser::printer::print_expression(&parsed).unwrap();
    let reparsed = parse_expression(&printed).unwrap();
    assert!(parsed.syntax_eq(&reparsed));
    drop(reparsed);
    drop(parsed);
}

#[test]
fn long_or_chain_is_one_ordered_multiterm_node() {
    let source = std::iter::repeat_n("a", 20_000)
        .collect::<Vec<_>>()
        .join(" OR ");
    let Expr::Logical(node) = parse_expression(&source).unwrap() else {
        panic!("OR")
    };
    assert_eq!(node.operator, LogicalOperator::Or);
    assert_eq!(node.terms.len(), 20_000);
    assert!(
        node.terms
            .iter()
            .all(|term| matches!(term, Expr::Identifier(name) if name.value == "a"))
    );
}

#[test]
fn parenthesized_query_and_expression_alternatives_preserve_the_complete_context() {
    let Expr::Subquery(outer) = parse_expression("((SELECT 1))").unwrap() else {
        panic!()
    };
    assert!(matches!(outer.query.body.as_ref(), SetExpr::Query(_)));
    let Expr::Binary(sum) = parse_expression("((SELECT 1)+1)").unwrap() else {
        panic!()
    };
    assert_eq!(sum.operator, BinaryOperator::Add);
    assert!(
        matches!(sum.left.as_ref(), Expr::Subquery(value) if matches!(value.query.body.as_ref(), SetExpr::Select(_)))
    );
    assert!(matches!(
        sum.right.as_ref(),
        Expr::Literal(Literal {
            kind: LiteralKind::Long {
                parsed_value: 1,
                ..
            },
            ..
        })
    ));
    for (source, list_expected) in [
        ("a IN ((SELECT 1))", true),
        ("a IN ((SELECT 1), 2)", true),
        ("a IN (SELECT 1)", false),
        ("a IN ((SELECT 1) UNION SELECT 2)", false),
    ] {
        let expression = parse_expression(source).unwrap();
        let Expr::Predicated(predicated) = &expression else {
            panic!()
        };
        let Predicate::In(predicate) = &predicated.predicate else {
            panic!()
        };
        if list_expected {
            let Expr::InList(list) = &predicate.value_list else {
                panic!("{source}")
            };
            assert!(
                matches!(&list.values[0], Expr::Subquery(value) if matches!(value.query.body.as_ref(), SetExpr::Select(_)))
            );
        } else {
            assert!(
                matches!(predicate.value_list, Expr::Subquery(_)),
                "{source}"
            );
        }
        let printed = novarocks_parser::printer::print_expression(&expression).unwrap();
        assert!(
            expression.syntax_eq(&parse_expression(&printed).unwrap()),
            "{source} -> {printed}"
        );
    }
    struct Parameters(Vec<usize>);
    impl Visit for Parameters {
        fn visit_parameter(&mut self, parameter: &Parameter) {
            self.0.push(parameter.position);
        }
    }
    for source in ["((SELECT ?)+?)", "a IN ((SELECT ?) UNION SELECT ?)"] {
        let expression = parse_expression(source).unwrap();
        let mut parameters = Parameters(Vec::new());
        parameters.visit_expr(&expression);
        assert_eq!(parameters.0, vec![0, 1], "{source}");
    }
}

#[test]
fn array_constructor_spellings_share_the_same_ast_and_keep_the_real_span() {
    for (bare, explicit, length) in [("[]", "ARRAY[]", 0), ("[1,2]", "ARRAY[1,2]", 2)] {
        let bare_expression = parse_expression(bare).unwrap();
        let explicit_expression = parse_expression(explicit).unwrap();
        let Expr::Array(array) = &bare_expression else {
            panic!("bare array constructor");
        };
        assert_eq!(array.values.len(), length);
        assert_eq!(array.span, Span::new(0, bare.len()));
        assert_eq!(explicit_expression.span(), Span::new(0, explicit.len()));
        assert!(bare_expression.syntax_eq(&explicit_expression));
    }

    let call = parse_expression("f([?])").unwrap();
    let Expr::FunctionCall(arguments) = &call else {
        panic!("function call");
    };
    let Expr::Array(array) = &arguments.arguments[0].value else {
        panic!("bare array argument");
    };
    assert_eq!(array.span, Span::new(2, 5));
    assert!(
        matches!(&array.values[0], Expr::Parameter(value) if value.position == 0 && value.span == Span::new(3, 4))
    );
    assert!(call.syntax_eq(&parse_expression("f(ARRAY[?])").unwrap()));
}

#[test]
fn array_and_subscript_bind_as_distinct_primary_and_postfix_nodes() {
    let source = "[1,2][1] + x[2]";
    let Expr::Binary(sum) = parse_expression(source).unwrap() else {
        panic!("sum");
    };
    assert_eq!(sum.operator, BinaryOperator::Add);
    let (Expr::Subscript(left), Expr::Subscript(right)) = (sum.left.as_ref(), sum.right.as_ref())
    else {
        panic!("two independent subscripts");
    };
    assert!(matches!(&left.base, Expr::Array(value) if value.values.len() == 2));
    assert_eq!(left.base.span(), Span::new(0, 5));
    assert_eq!(left.span, Span::new(0, 8));
    assert!(matches!(&right.base, Expr::Identifier(name) if name.value == "x"));
    assert_eq!(right.span, Span::new(11, 15));
    assert!(parse_expression("[1,]").is_err());
    assert!(parse_expression("x[1,2]").is_err());
}

#[test]
fn parenthesized_values_accepts_a_bare_array_in_its_row() {
    use novarocks_parser::parser::parse_query;
    let actual = parse_query("SELECT x[1] FROM (VALUES ROW([11,12])) AS t(x)").unwrap();
    let explicit = parse_query("SELECT x[1] FROM (VALUES ROW(ARRAY[11,12])) AS t(x)").unwrap();
    assert!(syntax_eq_query(&actual, &explicit));
}

#[test]
fn nonreserved_cast_and_unique_words_are_identifiers_outside_their_productions() {
    for source in ["TRY_CAST", "UNIQUE"] {
        assert!(
            matches!(parse_expression(source).unwrap(), Expr::Identifier(value) if value.value == source)
        );
    }
    assert!(
        matches!(parse_expression("TRY_CAST(x AS INTEGER)").unwrap(), Expr::Cast(value) if value.safe)
    );
    assert!(matches!(
        parse_expression("UNIQUE(SELECT 1)").unwrap(),
        Expr::Unique(_)
    ));
}

#[test]
fn ordinary_function_all_and_order_prefix_preserve_real_arguments_and_header() {
    let Expr::FunctionCall(call) = parse_expression("f(ALL)").unwrap() else {
        panic!("not call")
    };
    assert!(!call.distinct);
    assert!(matches!(&call.arguments[0].value, Expr::Identifier(value) if value.value == "ALL"));
    let Expr::FunctionCall(call) = parse_expression("f(ALL a)").unwrap() else {
        panic!("not call")
    };
    assert!(matches!(&call.arguments[0].value, Expr::Identifier(value) if value.value == "a"));
    let Expr::FunctionCall(call) = parse_expression("f(ALL => ?)").unwrap() else {
        panic!("not call")
    };
    assert_eq!(call.arguments[0].name.as_ref().unwrap().value, "ALL");
    assert!(matches!(&call.arguments[0].value, Expr::Parameter(value) if value.position == 0));
    let Expr::FunctionCall(call) = parse_expression("f(ALL ORDER BY ?)").unwrap() else {
        panic!("not call")
    };
    assert!(matches!(&call.arguments[0].value, Expr::Identifier(value) if value.value == "ALL"));
    assert!(
        matches!(&call.order_by.as_ref().unwrap().items[0].sort_key, Expr::Parameter(value) if value.position == 0)
    );
    let Expr::FunctionCall(call) =
        parse_expression("RUNNING \"cat\".f(ALL ? ORDER BY ?) OVER (PARTITION BY ?)").unwrap()
    else {
        panic!("not call")
    };
    assert_eq!(call.name.parts.len(), 2);
    assert!(call.name.parts[0].quoted);
    assert_eq!(call.name.parts[0].value, "cat");
    assert_eq!(
        call.processing_mode.as_ref().unwrap().mode,
        ProcessingModeKind::Running
    );
    assert!(matches!(&call.arguments[0].value, Expr::Parameter(value) if value.position == 2));
    assert!(
        matches!(&call.order_by.as_ref().unwrap().items[0].sort_key, Expr::Parameter(value) if value.position == 1)
    );
    let Window::Specification(window) = call.window.as_ref().unwrap() else {
        panic!("not spec")
    };
    assert!(matches!(&window.partition_by[0], Expr::Parameter(value) if value.position == 0));
    let Expr::FunctionCall(call) = parse_expression("f(ALL.*)").unwrap() else {
        panic!("not wildcard call")
    };
    assert!(
        matches!(&call.arguments[0].value, Expr::Dereference(value) if value.field.is_none() && matches!(&value.base, Expr::Identifier(value) if value.value == "ALL"))
    );
    let Expr::FunctionCall(call) = parse_expression("f(\"ORDER\")").unwrap() else {
        panic!("not call")
    };
    assert!(
        matches!(&call.arguments[0].value, Expr::Identifier(value) if value.quoted && value.value == "ORDER")
    );
    let Expr::FunctionCall(call) = parse_expression("f(\"ORDER\" => ?)").unwrap() else {
        panic!("not named call")
    };
    assert!(call.arguments[0].name.as_ref().unwrap().quoted);
    assert_eq!(call.arguments[0].name.as_ref().unwrap().value, "ORDER");
    assert!(matches!(&call.arguments[0].value, Expr::Parameter(value) if value.position == 0));
    // ORDER is reserved in the exact pin, unlike ALL. No keyword rule changes.
    assert!(parse_expression("f(ORDER)").is_err());
}

#[test]
fn function_all_retry_rolls_back_consumed_parameters_without_swallowing_construction() {
    let Expr::FunctionCall(outer) = parse_expression("f(ALL(?) FILTER (WHERE ?))").unwrap() else {
        panic!("not outer")
    };
    assert_eq!(outer.arguments.len(), 1);
    let Expr::FunctionCall(inner) = &outer.arguments[0].value else {
        panic!("not actual ALL call")
    };
    assert_eq!(inner.name.parts[0].value, "ALL");
    assert!(matches!(&inner.arguments[0].value, Expr::Parameter(value) if value.position == 1));
    assert!(
        matches!(inner.filter.as_ref().unwrap(), Expr::Parameter(value) if value.position == 0)
    );
    let source = "f(ALL IF())";
    let error = parse_expression(source).unwrap_err();
    let ParserError::Parse(ParseError::UnexpectedToken { span, found, .. }) = error else {
        panic!("not construction diagnostic")
    };
    let start = source.find("IF()").unwrap();
    assert_eq!(span, Span::new(start, start + 4));
    assert_eq!(found, "IF");
}
