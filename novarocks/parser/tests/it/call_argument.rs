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
fn id(value: &str, quoted: bool, start: usize) -> Ident {
    build::ident_at(value, quoted, Span::new(start, start + value.len()))
}
fn value(name: &str, start: usize) -> Expr {
    Expr::Identifier(id(name, false, start))
}
fn typed_call() -> Expr {
    let name = build::object_name(vec![id("ns", true, 1), id("f", true, 6)]).unwrap();
    let mut expr = build::function(name, vec![value("v", 31), value("other", 40)]);
    let Expr::FunctionCall(call) = &mut expr else {
        unreachable!()
    };
    call.arguments[0].name = Some(id("arg", true, 20));
    call.arguments[0].span = Span::new(20, 32);
    call.processing_mode = Some(ProcessingMode {
        mode: ProcessingModeKind::Final,
        span: Span::new(0, 5),
    });
    call.filter = Some(value("predicate", 55));
    expr
}
fn statement(expr: Expr) -> Statement {
    Statement::Query(build::query(SetExpr::Select(Box::new(
        build::select(
            vec![build::item(expr, None, false)],
            Vec::new(),
            None,
            GroupBy::None,
        )
        .unwrap(),
    ))))
}

#[test]
fn old_producers_wrap_real_original_values_and_arg_spans_without_new_mode_or_names() {
    let source = "f(`a`, g(2)) FILTER (WHERE flag)";
    let expr = expression(source);
    let Expr::FunctionCall(call) = &expr else {
        panic!("call")
    };
    assert!(call.processing_mode.is_none());
    assert_eq!(call.arguments.len(), 2);
    for (argument, expected) in call.arguments.iter().zip(["`a`", "g(2)"]) {
        assert!(argument.name.is_none());
        assert_eq!(argument.span, argument.value.span());
        assert_eq!(
            &source[argument.span.start()..argument.span.end()],
            expected
        );
    }
    let filter = call.filter.as_ref().unwrap();
    assert_eq!(&source[filter.span().start()..filter.span().end()], "flag");
    let original = expression("items[3]");
    let Expr::Subscript(subscript) = &original else {
        panic!("subscript")
    };
    let original_payload = subscript.as_ref() as *const SubscriptExpr;
    let source_span = original.span();
    let built = build::function(
        build::object_name(vec![build::ident("f", false)]).unwrap(),
        vec![original],
    );
    let Expr::FunctionCall(call) = built else {
        panic!("call")
    };
    let Expr::Subscript(subscript) = &call.arguments[0].value else {
        panic!("original subtree")
    };
    assert_eq!(subscript.as_ref() as *const SubscriptExpr, original_payload);
    assert_eq!(call.arguments[0].span, source_span);
    assert!(call.arguments[0].name.is_none() && call.processing_mode.is_none());
    // Existing table-function named syntax deliberately retains its current
    // Binary owner until the separate producer-normalization slice.
    let parsed =
        novarocks_parser::parse("SELECT * FROM TABLE(generate_series(end => 5, start => 2))")
            .unwrap();
    let Statement::Query(query) = &parsed[0] else {
        panic!("query")
    };
    let SetExpr::Select(select) = query.body.as_ref() else {
        panic!("select")
    };
    let TableFactor::TableFunction {
        expr: Expr::FunctionCall(call),
        ..
    } = &select.from[0].relation
    else {
        panic!("table function")
    };
    for argument in &call.arguments {
        assert!(argument.name.is_none());
        let Expr::Binary(binary) = &argument.value else {
            panic!("original named owner")
        };
        assert_eq!(binary.operator, BinaryOperator::NamedArgument);
        assert!(matches!(binary.left.as_ref(), Expr::Identifier(_)));
        assert!(matches!(binary.right.as_ref(), Expr::Literal(_)));
        assert_eq!(argument.span, binary.span);
    }
}

#[test]
fn typed_named_arguments_and_processing_mode_preserve_hooks_original_refs_and_all_facts() {
    let original = typed_call();
    let Expr::FunctionCall(call) = &original else {
        panic!("call")
    };
    struct Seen {
        names: Vec<String>,
        values: Vec<*const Expr>,
        modes: Vec<(ProcessingModeKind, Span)>,
    }
    impl Visit for Seen {
        fn visit_ident(&mut self, id: &Ident) {
            self.names.push(id.value.clone());
        }
        fn visit_call_argument(&mut self, arg: &CallArgument) {
            self.values.push(&arg.value);
            walk_call_argument(self, arg);
        }
        fn visit_processing_mode(&mut self, mode: &ProcessingMode) {
            self.modes.push((mode.mode, mode.span));
        }
    }
    let mut seen = Seen {
        names: Vec::new(),
        values: Vec::new(),
        modes: Vec::new(),
    };
    seen.visit_expr(&original);
    assert_eq!(seen.names, ["ns", "f", "arg", "v", "other", "predicate"]);
    assert_eq!(
        seen.values,
        vec![
            &call.arguments[0].value as *const Expr,
            &call.arguments[1].value as *const Expr
        ]
    );
    assert_eq!(seen.modes, [(ProcessingModeKind::Final, Span::new(0, 5))]);
    struct Rename;
    impl Fold for Rename {
        fn fold_ident(&mut self, mut id: Ident) -> Ident {
            id.value.push_str("_new");
            id
        }
        fn fold_processing_mode(&mut self, mut mode: ProcessingMode) -> ProcessingMode {
            mode.mode = ProcessingModeKind::Running;
            mode
        }
    }
    let folded = Rename.fold_expr(original.clone());
    assert_eq!(
        printer::print_expr(&folded),
        "RUNNING `ns_new`.`f_new`(`arg_new` => v_new, other_new) FILTER (WHERE predicate_new)"
    );
    assert_eq!(
        printer::print_expr(&original),
        "FINAL `ns`.`f`(`arg` => v, other) FILTER (WHERE predicate)"
    );
    let Expr::FunctionCall(folded) = &folded else {
        panic!("call")
    };
    assert_eq!(folded.arguments[0].span, call.arguments[0].span);
    assert_eq!(
        folded.arguments[0].value.span(),
        call.arguments[0].value.span()
    );
    assert_eq!(
        folded.processing_mode.as_ref().unwrap().span,
        Span::new(0, 5)
    );
    for changed_fact in 0..5 {
        let mut changed = original.clone();
        let Expr::FunctionCall(call) = &mut changed else {
            unreachable!()
        };
        match changed_fact {
            0 => call.arguments[0].name = None,
            1 => call.arguments[0].name.as_mut().unwrap().quoted = false,
            2 => call.arguments.swap(0, 1),
            3 => call.processing_mode = None,
            _ => call.processing_mode.as_mut().unwrap().mode = ProcessingModeKind::Running,
        }
        assert!(!original.syntax_eq(&changed), "fact {changed_fact}");
    }
    let mut shifted = original.clone();
    let Expr::FunctionCall(call) = &mut shifted else {
        unreachable!()
    };
    call.span = Span::new(1000, 1001);
    call.arguments[0].span = Span::new(2000, 2001);
    call.processing_mode.as_mut().unwrap().span = Span::new(3000, 3001);
    assert!(original.syntax_eq(&shifted));
}

#[test]
fn public_validation_preserves_named_mode_facts_and_checks_argument_then_filter_children() {
    validate_statement(&statement(typed_call())).unwrap();
    let named = build::call_argument(value("v", 20), Some(id("key", true, 5)));
    assert_eq!(named.span, Span::new(5, 21));
    assert!(named.name.as_ref().unwrap().quoted);
    for bad_argument in [true, false] {
        let mut expr = typed_call();
        let Expr::FunctionCall(call) = &mut expr else {
            unreachable!()
        };
        let invalid = Expr::InList(AstBox::new(InListExpr {
            values: AstVec::new(),
            span: Span::new(70, 73),
        }));
        if bad_argument {
            call.arguments[0].value = invalid;
        } else {
            call.filter = Some(invalid);
        }
        assert_eq!(
            validate_statement(&statement(expr)),
            Err(ValidateError::InvalidStructure {
                violation: StructuralViolation::EmptyInList,
                span: Span::new(70, 73)
            })
        );
    }
}

#[test]
fn control_call_producers_keep_optional_else_decoded_full_names_and_ordered_terms() {
    let absent = expression("IF(flag, first)");
    let explicit_null = expression("IF(flag, first, NULL)");
    let Expr::If(node) = &absent else {
        panic!("IF");
    };
    assert!(node.false_value.is_none());
    assert_eq!(
        (
            printer::print_expr(&absent),
            printer::print_expr(&explicit_null)
        ),
        (
            "IF(flag, first)".to_owned(),
            "IF(flag, first, NULL)".to_owned()
        )
    );
    assert!(!absent.syntax_eq(&explicit_null));
    assert!(absent.syntax_eq(&expression("`iF`(flag, first)")));
    assert!(absent.syntax_eq(&expression("IF(ALL flag, first)")));
    for source in ["schema.if(flag, first)", "`schema.if`(flag, first)"] {
        assert!(
            matches!(expression(source), Expr::FunctionCall(_)),
            "{source}"
        );
    }
    for (source, canonical) in [
        ("nullif(first, second)", "NULLIF(first, second)"),
        ("coalesce(a, b, c, d)", "COALESCE(a, b, c, d)"),
        ("try(inner_value)", "TRY(inner_value)"),
        ("format('%s', a, b, c)", "FORMAT('%s', a, b, c)"),
    ] {
        let node = expression(source);
        assert_eq!(node.span(), Span::new(0, source.len()));
        assert_eq!(printer::print_expr(&node), canonical);
        assert!(node.syntax_eq(&expression(canonical)));
    }
    let source = "coalesce(a, b, c, d)";
    let node = expression(source);
    let Expr::Coalesce(node) = node else {
        panic!("COALESCE");
    };
    assert_eq!(node.operands.len(), 4);
    assert_eq!(
        node.operands
            .iter()
            .map(|term| &source[term.span().start()..term.span().end()])
            .collect::<Vec<_>>(),
        ["a", "b", "c", "d"]
    );
}

#[test]
fn control_call_parser_enforces_real_arity_metadata_and_pinned_order_by_normalization() {
    use novarocks_parser::ParseError;
    for source in [
        "if()",
        "if(a)",
        "if(a,b,c,d)",
        "nullif(a)",
        "nullif(a,b,c)",
        "coalesce()",
        "coalesce(a)",
        "try()",
        "try(a,b)",
        "format()",
        "format('fmt')",
    ] {
        let tokens = novarocks_parser::lex(source).unwrap();
        assert!(
            matches!(novarocks_parser::parser::parse_expression(source,&tokens),Err(ParseError::UnexpectedToken{span,..}) if span==Span::new(0,source.len())),
            "{source}"
        );
    }
    for (name, args) in [
        ("if", "a, b"),
        ("nullif", "a, b"),
        ("coalesce", "a, b, c, d"),
        ("try", "a"),
        ("format", "'%s', a"),
    ] {
        for source in [
            format!("{name}(DISTINCT {args})"),
            format!("{name}({args}) OVER w"),
            format!("{name}({args}) FILTER (WHERE flag)"),
            format!("{name}({args}) IGNORE NULLS"),
            format!("{name}({args} SEPARATOR ',')"),
        ] {
            let tokens = novarocks_parser::lex(&source).unwrap();
            assert!(
                novarocks_parser::parser::parse_expression(&source, &tokens).is_err(),
                "{source}"
            );
        }
    }
    let source = "if(a) OVER w";
    let tokens = novarocks_parser::lex(source).unwrap();
    let error = novarocks_parser::parser::parse_expression(source, &tokens).unwrap_err();
    assert!(
        matches!(error,ParseError::UnexpectedToken{expected,..} if expected.contains("arguments"))
    );
    assert!(novarocks_parser::parse("SELECT * FROM TABLE(if(named => a, b))").is_err());
    // The exact pinned builder visits ORDER BY and then omits it from these
    // dedicated nodes. This assertion is syntax, not execution capability.
    let with_order = expression("coalesce(a, b, c, d ORDER BY ignored)");
    assert!(with_order.syntax_eq(&expression("coalesce(a, b, c, d)")));
    assert_eq!(printer::print_expr(&with_order), "COALESCE(a, b, c, d)");
}

#[test]
fn control_builders_hooks_syntax_equality_and_public_constructor_guards_keep_real_children() {
    let source = "IF(flag, first, coalesce(a, b, c, d))";
    let original = expression(source);
    let Expr::If(node) = &original else {
        panic!("IF");
    };
    let Expr::Coalesce(fallback) = node.false_value.as_ref().unwrap() else {
        panic!("COALESCE");
    };
    struct Seen(Vec<*const Expr>);
    impl Visit for Seen {
        fn visit_expr(&mut self, node: &Expr) {
            self.0.push(node);
            walk_expr(self, node);
        }
    }
    let mut seen = Seen(Vec::new());
    seen.visit_expr(&original);
    let expected = vec![
        &original as *const Expr,
        &node.condition,
        &node.true_value,
        node.false_value.as_ref().unwrap(),
        &fallback.operands[0],
        &fallback.operands[1],
        &fallback.operands[2],
        &fallback.operands[3],
    ];
    assert_eq!(seen.0, expected);
    struct Rename;
    impl Fold for Rename {
        fn fold_ident(&mut self, mut name: Ident) -> Ident {
            name.value.push_str("_new");
            name
        }
    }
    let changed = Rename.fold_expr(original.clone());
    assert_eq!(
        printer::print_expr(&changed),
        "IF(flag_new, first_new, COALESCE(a_new, b_new, c_new, d_new))"
    );
    assert_eq!(changed.span(), original.span());
    assert!(!original.syntax_eq(&changed));
    let mut shifted = original.clone();
    let Expr::If(node) = &mut shifted else {
        unreachable!()
    };
    node.span = Span::new(1000, 1001);
    assert!(original.syntax_eq(&shifted));
    for (built, source) in [
        (
            build::if_expr(value("flag", 11), value("first", 21), None),
            "IF(flag, first)",
        ),
        (
            build::null_if(value("a", 11), value("b", 21)),
            "NULLIF(a, b)",
        ),
        (
            build::coalesce(vec![
                value("a", 11),
                value("b", 21),
                value("c", 31),
                value("d", 41),
            ])
            .unwrap(),
            "COALESCE(a, b, c, d)",
        ),
        (build::try_expr(value("a", 11)), "TRY(a)"),
        (
            build::format_expr(vec![
                expression("'%s'"),
                value("a", 11),
                value("b", 21),
                value("c", 31),
            ])
            .unwrap(),
            "FORMAT('%s', a, b, c)",
        ),
    ] {
        assert!(built.syntax_eq(&expression(source)));
        validate_statement(&statement(built)).unwrap();
    }
    assert_eq!(
        build::coalesce(vec![value("a", 11)]).unwrap_err(),
        build::BuildError::TooFewCoalesceOperands
    );
    assert_eq!(
        build::format_expr(vec![value("a", 11)]).unwrap_err(),
        build::BuildError::TooFewFormatArguments
    );
    for (invalid, violation) in [
        (
            Expr::Coalesce(AstBox::new(CoalesceExpr {
                operands: vec![value("a", 11)].into(),
                span: Span::new(5, 25),
            })),
            StructuralViolation::TooFewCoalesceOperands,
        ),
        (
            Expr::Format(AstBox::new(FormatExpr {
                arguments: vec![value("a", 11)].into(),
                span: Span::new(5, 25),
            })),
            StructuralViolation::TooFewFormatArguments,
        ),
    ] {
        assert_eq!(
            validate_statement(&statement(invalid)),
            Err(ValidateError::InvalidStructure {
                violation,
                span: Span::new(5, 25)
            })
        );
    }
    let metadata = build::function(
        build::object_name(vec![build::ident("if", false)]).unwrap(),
        Vec::new(),
    );
    assert!(matches!(metadata, Expr::FunctionCall(_)));
}
