// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information regarding copyright
// ownership. The ASF licenses this file to you under the Apache License,
// Version 2.0 (the "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// License for the specific language governing permissions and limitations
// under the License.

use super::*;
use crate::ast::{
    DataTypeParameter, IntervalQualifier, IntervalQualifierField, NumericParameter, SetExpr,
    SimpleIntervalQualifier, Statement, validate_statement,
};
use crate::{SYNTHETIC, build, printer};
fn span() -> Span {
    Span::new(10, 30)
}
fn id(value: &str, quoted: bool) -> Ident {
    build::ident_at(value, quoted, span())
}
fn name(parts: Vec<Ident>) -> ObjectName {
    ObjectName {
        parts,
        span: span(),
    }
}
fn interval_type() -> IntervalDataType {
    IntervalDataType {
        span: span(),
        qualifier: IntervalQualifier::Simple(SimpleIntervalQualifier {
            field: IntervalQualifierField::Day,
            precision: Some(AstBox::new(DataTypeParameter::Numeric(NumericParameter {
                spelling: "17".into(),
                parsed_value: 17,
                span: span(),
            }))),
            span: span(),
        }),
    }
}
fn statement(value: Expr) -> Statement {
    Statement::Query(build::query(SetExpr::Select(AstBox::new(
        build::select(vec![build::item(value, None)], None, None, None).unwrap(),
    ))))
}
fn argument(value: Expr, name: Option<Ident>) -> CallArgument {
    CallArgument {
        span: value.span(),
        name,
        value,
    }
}
fn recursive_fixtures() -> Vec<Expr> {
    vec![
        Expr::AtLocal(AstBox::new(AtLocal {
            value: build::integer(1),
            span: span(),
        })),
        Expr::AtTimeZone(AstBox::new(
            AtTimeZone::new(build::integer(1), build::string("UTC"), span()).unwrap(),
        )),
        Expr::GroupingOperation(AstBox::new(
            GroupingOperation::new(
                vec![
                    name(vec![id("A", true), id("B", false)]),
                    name(vec![id("C", false)]),
                ],
                span(),
            )
            .unwrap(),
        )),
        Expr::Trim(AstBox::new(Trim {
            specification: TrimSpecification::Leading,
            trim_source: build::string(" xy "),
            trim_character: Some(build::string("x")),
            span: span(),
        })),
        Expr::Overlay(AstBox::new(Overlay {
            value: build::string("abc"),
            replacement: build::string("x"),
            start: build::integer(2),
            length: Some(build::integer(1)),
            span: span(),
        })),
        Expr::IntervalValueExpression(AstBox::new(IntervalValueExpression {
            left: build::integer(1),
            right: build::integer(2),
            data_type: interval_type(),
            span: span(),
        })),
        Expr::MethodCall(AstBox::new(MethodCall {
            receiver: build::integer(1),
            method: id("m", true),
            arguments: vec![argument(build::integer(2), Some(id("arg", false)))].into(),
            span: span(),
        })),
        Expr::StaticMethodCall(AstBox::new(StaticMethodCall {
            type_name: name(vec![id("cat", false), id("Type", true)]),
            method: id("run", false),
            arguments: crate::ast::AstVec::new(),
            span: span(),
        })),
    ]
}

#[test]
fn typed_roots_print_real_fields_and_preserve_span_ignored_syntax_facts() {
    let expected = [
        "1 AT LOCAL",
        "1 AT TIME ZONE 'UTC'",
        "GROUPING (a.b, c)",
        "trim(LEADING 'x' FROM ' xy ')",
        "OVERLAY('abc' PLACING 'x' FROM 2 FOR 1)",
        "((1 - 2) DAY(17))",
        "(1).`m`(arg => 2)",
        "cat.`Type`::run()",
    ];
    let fixtures = recursive_fixtures();
    struct Preserve;
    impl Fold for Preserve {}
    for node in &fixtures {
        assert_eq!(Preserve.fold_expr(node.clone()), node.clone());
    }
    for (node, expected) in fixtures.iter().zip(expected) {
        assert_eq!(node.span(), span());
        assert_eq!(printer::print_expr(node), expected);
        assert!(node.syntax_eq(&node.clone()));
        validate_statement(&statement(node.clone())).unwrap();
    }
    for (node, expected) in [
        (
            Expr::CurrentCatalog(CurrentCatalog { span: span() }),
            "CURRENT_CATALOG",
        ),
        (
            Expr::CurrentDate(CurrentDate { span: span() }),
            "current_date",
        ),
        (
            Expr::CurrentPath(CurrentPath { span: span() }),
            "CURRENT_PATH",
        ),
        (
            Expr::CurrentSchema(CurrentSchema { span: span() }),
            "CURRENT_SCHEMA",
        ),
        (
            Expr::CurrentUser(CurrentUser { span: span() }),
            "CURRENT_USER",
        ),
        (
            Expr::CurrentTime(CurrentTime {
                precision: Some(3),
                span: span(),
            }),
            "current_time(3)",
        ),
        (
            Expr::CurrentTimestamp(CurrentTimestamp {
                precision: None,
                span: span(),
            }),
            "current_timestamp",
        ),
        (
            Expr::LocalTime(LocalTime {
                precision: Some(0),
                span: span(),
            }),
            "localtime(0)",
        ),
        (
            Expr::LocalTimestamp(LocalTimestamp {
                precision: Some(9),
                span: span(),
            }),
            "localtimestamp(9)",
        ),
    ] {
        assert_eq!(printer::print_expr(&node), expected);
        assert_eq!(node.span(), span());
        validate_statement(&statement(node)).unwrap();
    }
    for (mut changed, original) in recursive_fixtures().into_iter().zip(fixtures) {
        match &mut changed {
            Expr::AtLocal(node) => node.span = SYNTHETIC,
            Expr::AtTimeZone(node) => node.span = SYNTHETIC,
            Expr::GroupingOperation(node) => node.span = SYNTHETIC,
            Expr::Trim(node) => node.span = SYNTHETIC,
            Expr::Overlay(node) => node.span = SYNTHETIC,
            Expr::IntervalValueExpression(node) => node.span = SYNTHETIC,
            Expr::MethodCall(node) => node.span = SYNTHETIC,
            Expr::StaticMethodCall(node) => node.span = SYNTHETIC,
            _ => unreachable!(),
        }
        assert!(changed.syntax_eq(&original));
        assert_ne!(changed, original);
        match &mut changed {
            Expr::AtLocal(node) => node.value = build::integer(99),
            Expr::AtTimeZone(node) => node.time_zone = build::string("Europe/Paris"),
            Expr::GroupingOperation(node) => node.grouping_columns.swap(0, 1),
            Expr::Trim(node) => node.specification = TrimSpecification::Trailing,
            Expr::Overlay(node) => node.length = None,
            Expr::IntervalValueExpression(node) => {
                node.data_type.qualifier = IntervalQualifier::Simple(SimpleIntervalQualifier {
                    field: IntervalQualifierField::Month,
                    precision: None,
                    span: SYNTHETIC,
                })
            }
            Expr::MethodCall(node) => node.arguments[0].name = None,
            Expr::StaticMethodCall(node) => node.type_name.parts[1].quoted = false,
            _ => unreachable!(),
        }
        assert!(!changed.syntax_eq(&original));
    }
    let sum = build::binary(
        build::integer(1),
        crate::ast::BinaryOperator::Add,
        build::integer(2),
    );
    assert_eq!(
        printer::print_expr(&Expr::AtLocal(AstBox::new(AtLocal {
            value: sum,
            span: span()
        }))),
        "(1 + 2) AT LOCAL"
    );
    let negated = build::unary(
        crate::ast::UnaryOperator::Minus,
        build::identifier(id("x", false)),
    )
    .unwrap();
    assert_eq!(
        printer::print_expr(&Expr::AtTimeZone(AstBox::new(
            AtTimeZone::new(negated, build::string("UTC"), span()).unwrap()
        ))),
        "(-x) AT TIME ZONE 'UTC'"
    );
    let right = build::binary(
        build::integer(2),
        crate::ast::BinaryOperator::Add,
        build::integer(3),
    );
    assert_eq!(
        printer::print_expr(&Expr::IntervalValueExpression(AstBox::new(
            IntervalValueExpression {
                left: build::integer(1),
                right,
                data_type: interval_type(),
                span: span()
            }
        ))),
        "((1 - (2 + 3)) DAY(17))"
    );
    // Ordinary old grammar calls retain the original generic producer.
    let mut parsed = crate::parse("SELECT old_schema.fn(1), trim('x')").unwrap();
    let Statement::Query(query) = parsed.remove(0) else {
        unreachable!()
    };
    let SetExpr::Select(select) = query.body.as_ref() else {
        unreachable!()
    };
    for column in &select.projection {
        let crate::ast::SelectItem::SingleColumn(column) = column else {
            unreachable!()
        };
        assert!(matches!(column.expression, Expr::FunctionCall(_)));
    }
}

#[test]
fn exact_constructor_constraints_and_name_projection_do_not_infer_global_policy() {
    let node = GroupingOperation::new(
        vec![name(vec![
            id("MiXeD", true),
            id("A.B", true),
            id("9X", false),
            id("Ä", true),
        ])],
        span(),
    )
    .unwrap();
    let chain = crate::ast::identifier_chain_observed(&node.grouping_columns[0], |_| {
        Ok::<(), std::convert::Infallible>(())
    })
    .unwrap()
    .unwrap();
    let mut parts = chain.iter_rev().collect::<Vec<_>>();
    parts.reverse();
    assert_eq!(
        parts
            .iter()
            .map(|part| (part.value.as_str(), part.quoted, part.span))
            .collect::<Vec<_>>(),
        [
            ("mixed", false, SYNTHETIC),
            ("a.b", true, SYNTHETIC),
            ("9x", true, SYNTHETIC),
            ("ä", true, SYNTHETIC)
        ]
    );
    assert_eq!(node.span, span());
    let ordinary = name(vec![id("MiXeD", true)]);
    assert_eq!(ordinary.parts[0].value, "MiXeD");
    assert!(ordinary.parts[0].quoted);
    assert_eq!(ordinary.parts[0].span, span());
    assert_eq!(
        GroupingOperation::new(Vec::new(), span()).unwrap_err(),
        SpecialExpressionInvariantError::EmptyGroupingColumns
    );
    assert_eq!(
        GroupingOperation::new(vec![name(Vec::new())], span()).unwrap_err(),
        SpecialExpressionInvariantError::InvalidGroupingColumn
    );
    for column in [
        build::integer(1),
        build::legacy_nested(build::identifier(id("x", false))),
        Expr::Dereference(AstBox::new(crate::ast::DereferenceExpr {
            base: build::identifier(id("label", false)),
            field: None,
            span: span(),
        })),
    ] {
        let node = Expr::GroupingOperation(AstBox::new(GroupingOperation {
            grouping_columns: vec![column].into(),
            span: span(),
        }));
        assert!(
            matches!(validate_statement(&statement(node)),Err(crate::ValidateError::InvalidStructure{violation:crate::StructuralViolation::SpecialExpressionInvariant(SpecialExpressionInvariantError::InvalidGroupingColumn),span:actual})if actual==span())
        );
    }
    for zone in [
        build::integer(1),
        build::legacy_nested(build::string("UTC")),
        Expr::CurrentDate(CurrentDate { span: span() }),
    ] {
        assert_eq!(
            AtTimeZone::new(
                build::identifier(id("unknown_value", false)),
                zone.clone(),
                span()
            )
            .unwrap_err(),
            SpecialExpressionInvariantError::InvalidTimeZoneNode
        );
        let raw = Expr::AtTimeZone(AstBox::new(AtTimeZone {
            value: build::integer(1),
            time_zone: zone,
            span: span(),
        }));
        assert!(validate_statement(&statement(raw)).is_err());
    }
    let interval = Expr::Literal(Literal {
        kind: LiteralKind::Interval(crate::ast::IntervalLiteral {
            value: "01".into(),
            sign: crate::ast::IntervalSign::Negative,
            qualifier: interval_type().qualifier,
        }),
        span: span(),
    });
    validate_statement(&statement(Expr::AtTimeZone(AstBox::new(
        AtTimeZone::new(build::integer(1), interval, span()).unwrap(),
    ))))
    .unwrap();
    for precision in [None, Some(-1), Some(i32::MAX)] {
        for node in [
            Expr::CurrentTime(CurrentTime {
                precision,
                span: span(),
            }),
            Expr::CurrentTimestamp(CurrentTimestamp {
                precision,
                span: span(),
            }),
            Expr::LocalTime(LocalTime {
                precision,
                span: span(),
            }),
            Expr::LocalTimestamp(LocalTimestamp {
                precision,
                span: span(),
            }),
        ] {
            validate_statement(&statement(node.clone())).unwrap();
            if let Some(precision) = precision {
                assert!(printer::print_expr(&node).ends_with(&format!("({precision})")));
            }
        }
    }
    assert!(
        !Expr::CurrentTime(CurrentTime {
            precision: None,
            span: span()
        })
        .syntax_eq(&Expr::CurrentTime(CurrentTime {
            precision: Some(0),
            span: SYNTHETIC
        }))
    );
    assert!(
        Expr::CurrentDate(CurrentDate { span: span() })
            .syntax_eq(&Expr::CurrentDate(CurrentDate { span: SYNTHETIC }))
    );
}

#[test]
fn hooks_borrow_all_original_children_and_fold_names_types_options_in_order() {
    #[derive(Default)]
    struct Facts {
        expressions: Vec<*const Expr>,
        names: Vec<(String, bool)>,
        numbers: Vec<i64>,
        strings: Vec<String>,
        parameters: Vec<i64>,
    }
    impl Visit for Facts {
        fn visit_expr(&mut self, node: &Expr) {
            self.expressions.push(std::ptr::from_ref(node));
            crate::ast::walk_expr(self, node);
        }
        fn visit_ident(&mut self, node: &Ident) {
            self.names.push((node.value.clone(), node.quoted));
        }
        fn visit_literal(&mut self, node: &Literal) {
            match &node.kind {
                LiteralKind::Long { parsed_value, .. } => self.numbers.push(*parsed_value),
                LiteralKind::String(value) => self.strings.push(value.clone()),
                _ => {}
            }
        }
        fn visit_numeric_parameter(&mut self, node: &NumericParameter) {
            self.parameters.push(node.parsed_value);
        }
    }
    let fixtures = recursive_fixtures();
    let mut facts = Facts::default();
    for node in &fixtures {
        facts.visit_expr(node);
    }
    assert_eq!(facts.numbers, [1, 1, 2, 1, 1, 2, 1, 2]);
    assert_eq!(facts.strings, ["UTC", " xy ", "x", "abc", "x"]);
    assert_eq!(facts.parameters, [17]);
    assert_eq!(
        facts.names,
        [
            ("a".into(), false),
            ("b".into(), false),
            ("c".into(), false),
            ("m".into(), true),
            ("arg".into(), false),
            ("cat".into(), false),
            ("Type".into(), true),
            ("run".into(), false)
        ]
    );
    for node in &fixtures {
        let children: Vec<&Expr> = match node {
            Expr::AtLocal(node) => vec![&node.value],
            Expr::AtTimeZone(node) => vec![&node.value, &node.time_zone],
            Expr::GroupingOperation(node) => node.grouping_columns.iter().collect(),
            Expr::Trim(node) => vec![&node.trim_source, node.trim_character.as_ref().unwrap()],
            Expr::Overlay(node) => vec![
                &node.value,
                &node.replacement,
                &node.start,
                node.length.as_ref().unwrap(),
            ],
            Expr::IntervalValueExpression(node) => vec![&node.left, &node.right],
            Expr::MethodCall(node) => vec![&node.receiver, &node.arguments[0].value],
            Expr::StaticMethodCall(_) => vec![],
            _ => unreachable!(),
        };
        for child in children {
            assert!(facts.expressions.contains(&std::ptr::from_ref(child)));
        }
    }
    struct Change;
    impl Fold for Change {
        fn fold_ident(&mut self, mut node: Ident) -> Ident {
            node.value.push_str("_next");
            node
        }
        fn fold_numeric_parameter(&mut self, mut node: NumericParameter) -> NumericParameter {
            node.parsed_value += 1;
            node.spelling = node.parsed_value.to_string();
            node
        }
        fn fold_literal(&mut self, node: Literal) -> Literal {
            if let LiteralKind::Long { parsed_value, .. } = node.kind {
                let Expr::Literal(mut next) = build::integer(parsed_value + 10) else {
                    unreachable!()
                };
                next.span = node.span;
                next
            } else {
                node
            }
        }
    }
    let mut changed = Facts::default();
    for node in fixtures {
        let next = Change.fold_expr(node);
        changed.visit_expr(&next);
    }
    assert_eq!(changed.numbers, [11, 11, 12, 11, 11, 12, 11, 12]);
    assert_eq!(changed.strings, facts.strings);
    assert_eq!(changed.parameters, [18]);
    assert_eq!(
        changed.names,
        facts
            .names
            .iter()
            .map(|(name, quoted)| (format!("{name}_next"), *quoted))
            .collect::<Vec<_>>()
    );
}
