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

use novarocks_parser::{Span, StructuralViolation, ValidateError, ast::*, build, printer};

fn expression(source: &str) -> Expr {
    let tokens = novarocks_parser::lex(source).unwrap();
    novarocks_parser::parser::parse_expression(source, &tokens).unwrap()
}
fn id(value: &str) -> Ident {
    build::ident_at(value, false, Span::new(3, 7))
}
fn value(name: &str) -> Expr {
    Expr::Identifier(id(name))
}
fn frame() -> WindowFrame {
    WindowFrame {
        units: WindowFrameUnits::Rows,
        start_bound: WindowFrameBound::CurrentRow(Span::new(11, 22)),
        end_bound: None,
        exclusion: WindowFrameExclusion::NoOthers,
        measures: vec![MeasureDefinition {
            expression: value("input"),
            name: id("measure"),
            span: Span::new(31, 39),
        }]
        .into(),
        after_match_skip_to: Some(SkipTo::First {
            identifier: id("A"),
            span: Span::new(41, 48),
        }),
        pattern_search_mode: Some(PatternSearchMode {
            mode: PatternSearchModeKind::Seek,
            span: Span::new(51, 55),
        }),
        pattern: Some(RowPattern::Quantified {
            pattern: AstBox::new(RowPattern::Variable {
                name: id("A"),
                span: Span::new(61, 62),
            }),
            quantifier: PatternQuantifier::Range {
                greedy: false,
                at_least: Some(PatternInteger {
                    lexeme: "2".into(),
                    parsed_value: 2,
                    span: Span::new(63, 64),
                }),
                at_most: Some(PatternInteger {
                    lexeme: "4".into(),
                    parsed_value: 4,
                    span: Span::new(65, 66),
                }),
                span: Span::new(62, 68),
            },
            span: Span::new(61, 68),
        }),
        subsets: vec![SubsetDefinition {
            name: id("U"),
            identifiers: vec![id("A"), id("B")],
            span: Span::new(71, 79),
        }]
        .into(),
        variable_definitions: vec![VariableDefinition {
            name: id("A"),
            expression: value("predicate"),
            span: Span::new(81, 89),
        }]
        .into(),
        span: Span::new(1, 90),
    }
}
fn specification(frame: WindowFrame) -> WindowSpecification {
    WindowSpecification {
        existing_window_name: None,
        partition_by: AstVec::new(),
        order_by: None,
        window_frame: Some(frame),
        span: Span::new(0, 91),
    }
}
fn operation(frame: WindowFrame) -> Expr {
    Expr::WindowOperation(AstBox::new(WindowOperation {
        name: id("result"),
        window: Window::Specification(AstBox::new(specification(frame))),
        span: Span::new(0, 100),
    }))
}
fn statement(expr: Expr) -> Statement {
    Statement::Query(build::query(SetExpr::Select(
        novarocks_parser::ast::AstBox::new(
            build::select(vec![build::item(expr, None)], None, None, None).unwrap(),
        ),
    )))
}

#[test]
fn original_over_branches_preserve_reference_vs_specification_and_quoted_spans() {
    for (source, reference, printed) in [
        ("f(v) OVER `w`", true, "f(v) OVER `w`"),
        ("f(v) OVER (`w`)", false, "f(v) OVER (`w`)"),
    ] {
        let expr = expression(source);
        let Expr::FunctionCall(call) = &expr else {
            panic!("call");
        };
        match call.window.as_ref().unwrap() {
            Window::Reference(node) => {
                assert!(reference);
                assert!(node.name.quoted);
                assert_eq!(node.name.value, "w");
                assert_eq!(&source[node.name.span.start()..node.name.span.end()], "`w`");
                assert_eq!(&source[node.span.start()..node.span.end()], "OVER `w`");
            }
            Window::Specification(node) => {
                assert!(!reference);
                let name = node.existing_window_name.as_ref().unwrap();
                assert!(name.quoted);
                assert_eq!(name.value, "w");
                assert_eq!(&source[node.span.start()..node.span.end()], "(`w`)");
            }
        }
        assert_eq!(printer::print_expr(&expr), printed);
        assert!(expr.syntax_eq(&expression(printed)));
    }
    assert!(!expression("f(v) OVER w").syntax_eq(&expression("f(v) OVER (w)")));
    let plain = expression("f(v) OVER (ORDER BY k ROWS CURRENT ROW)");
    let Expr::FunctionCall(call) = plain else {
        panic!("call");
    };
    let Window::Specification(spec) = call.into_inner().window.unwrap() else {
        panic!("spec");
    };
    let frame = spec.window_frame.as_ref().unwrap();
    assert!(
        frame.measures.is_empty()
            && frame.after_match_skip_to.is_none()
            && frame.pattern_search_mode.is_none()
            && frame.pattern.is_none()
            && frame.subsets.is_empty()
            && frame.variable_definitions.is_empty()
    );
}

#[test]
fn complete_frame_children_modes_quantifiers_order_and_spans_survive_shared_traits() {
    let original = frame();
    struct Seen {
        names: Vec<String>,
        integers: Vec<(String, i64, Span)>,
    }
    impl Visit for Seen {
        fn visit_ident(&mut self, id: &Ident) {
            self.names.push(id.value.clone());
        }
        fn visit_pattern_integer(&mut self, integer: &PatternInteger) {
            self.integers
                .push((integer.lexeme.clone(), integer.parsed_value, integer.span));
        }
    }
    let mut seen = Seen {
        names: Vec::new(),
        integers: Vec::new(),
    };
    seen.visit_window_frame(&original);
    assert_eq!(
        seen.names,
        [
            "input",
            "measure",
            "A",
            "A",
            "U",
            "A",
            "B",
            "A",
            "predicate"
        ]
    );
    assert_eq!(
        seen.integers,
        [
            ("2".into(), 2, Span::new(63, 64)),
            ("4".into(), 4, Span::new(65, 66))
        ]
    );
    struct Rename;
    impl Fold for Rename {
        fn fold_ident(&mut self, mut id: Ident) -> Ident {
            id.value.push_str("_new");
            id
        }
    }
    let changed = Rename.fold_window_frame(original.clone());
    let mut changed_seen = Seen {
        names: Vec::new(),
        integers: Vec::new(),
    };
    changed_seen.visit_window_frame(&changed);
    assert_eq!(
        changed_seen.names,
        seen.names
            .iter()
            .map(|name| format!("{name}_new"))
            .collect::<Vec<_>>()
    );
    assert_eq!(changed_seen.integers, seen.integers);
    assert_eq!(changed.span, original.span);
    assert!(!original.syntax_eq(&changed));
    for field in 0..6 {
        let mut missing = original.clone();
        match field {
            0 => missing.measures.clear(),
            1 => missing.after_match_skip_to = None,
            2 => missing.pattern_search_mode = None,
            3 => missing.pattern = None,
            4 => missing.subsets.clear(),
            _ => missing.variable_definitions.clear(),
        }
        assert!(!original.syntax_eq(&missing), "field {field}");
    }
    let mut shifted = original.clone();
    shifted.span = Span::new(1000, 1001);
    assert!(original.syntax_eq(&shifted));
    let mut lexical = PatternInteger {
        lexeme: "02".into(),
        parsed_value: 2,
        span: Span::new(1, 3),
    };
    assert!(!lexical.syntax_eq(&PatternInteger {
        lexeme: "2".into(),
        parsed_value: 2,
        span: Span::new(1, 2)
    }));
    lexical.span = Span::new(100, 102);
    assert!(lexical.syntax_eq(&PatternInteger {
        lexeme: "02".into(),
        parsed_value: 2,
        span: Span::new(1, 3)
    }));
    assert_eq!(
        printer::print_expr(&operation(original.clone())),
        "result OVER (MEASURES input AS measure ROWS CURRENT ROW AFTER MATCH SKIP TO FIRST A SEEK PATTERN((A{2,4}?)) SUBSET U = (A, B) DEFINE A AS predicate)"
    );
    validate_statement(&statement(operation(original))).unwrap();
}

#[test]
fn public_validation_checks_real_pattern_constructor_invariants_inside_window_operation() {
    let mut invalid = frame();
    invalid.pattern = Some(RowPattern::Excluded {
        pattern: AstBox::new(RowPattern::Permutation {
            patterns: AstVec::new(),
            span: Span::new(31, 32),
        }),
        span: Span::new(30, 33),
    });
    assert_eq!(
        validate_statement(&statement(operation(invalid))),
        Err(ValidateError::InvalidStructure {
            violation: StructuralViolation::EmptyPatternList,
            span: Span::new(31, 32)
        })
    );
    let mut invalid = frame();
    invalid.subsets[0].identifiers.clear();
    assert_eq!(
        validate_statement(&statement(operation(invalid))),
        Err(ValidateError::InvalidStructure {
            violation: StructuralViolation::EmptySubsetIdentifierList,
            span: Span::new(71, 79)
        })
    );
}

#[test]
fn inherited_window_name_allows_trivia_before_specification_clauses() {
    for (source, partition_count, order_count, has_frame) in [
        ("f(v) OVER (w ORDER BY k)", 0, 1, false),
        ("f(v) OVER (w ROWS CURRENT ROW)", 0, 0, true),
        (
            "f(v) OVER (`w` /* boundary */ PARTITION BY k ORDER BY v ROWS CURRENT ROW)",
            1,
            1,
            true,
        ),
    ] {
        let expr = expression(source);
        let Expr::FunctionCall(call) = &expr else {
            panic!("call");
        };
        let Window::Specification(spec) = call.window.as_ref().unwrap() else {
            panic!("inherited specification");
        };
        let name = spec.existing_window_name.as_ref().unwrap();
        assert_eq!(name.value, "w");
        assert_eq!(name.quoted, source.contains('`'));
        assert_eq!(
            &source[name.span.start()..name.span.end()],
            if name.quoted { "`w`" } else { "w" }
        );
        assert_eq!(
            &source[spec.span.start()..spec.span.end()],
            source.split_once("OVER ").unwrap().1
        );
        assert_eq!(spec.partition_by.len(), partition_count);
        assert_eq!(
            spec.order_by.as_ref().map_or(0, |order| order.items.len()),
            order_count
        );
        assert_eq!(spec.window_frame.is_some(), has_frame);
        if let Some(frame) = &spec.window_frame {
            assert_eq!(frame.units, WindowFrameUnits::Rows);
            assert!(matches!(frame.start_bound, WindowFrameBound::CurrentRow(_)));
            assert!(frame.end_bound.is_none());
        }
        let printed = printer::print_expr(&expr);
        assert!(
            expr.syntax_eq(&expression(&printed)),
            "{source} -> {printed}"
        );
    }
}
