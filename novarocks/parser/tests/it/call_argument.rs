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
    Statement::Query(build::query(SetExpr::Select(
        novarocks_parser::ast::AstBox::new(
            build::select(vec![build::item(expr, None)], None, None, None).unwrap(),
        ),
    )))
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
    // Table-function syntax now owns true named argument fields directly.
    let source = "SELECT * FROM TABLE(generate_series(end => 5, start => 2))";
    let parsed = novarocks_parser::parse(source).unwrap();
    let Statement::Query(query) = &parsed[0] else {
        panic!("query")
    };
    let SetExpr::Select(select) = query.body.as_ref() else {
        panic!("select")
    };
    let Some(Relation::TableFunction(call)) = &select.from else {
        panic!("table function")
    };
    for (argument, (name, value)) in call.arguments.iter().zip([("end", 5), ("start", 2)]) {
        let name_node = argument.name.as_ref().unwrap();
        assert_eq!(name_node.value, name);
        assert_eq!(&source[name_node.span.start()..name_node.span.end()], name);
        let TableFunctionArgumentValue::Expression(Expr::Literal(literal)) = &argument.value else {
            panic!("real literal child")
        };
        assert!(matches!(literal.kind,LiteralKind::Long{parsed_value,..} if parsed_value==value));
        assert_eq!(
            &source[argument.span.start()..argument.span.end()],
            format!("{name} => {value}")
        );
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

#[test]
fn extract_real_producer_keeps_all_seventeen_distinct_fields_and_original_operand_spans() {
    for (label, field) in [
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
        let source = format!("EXTRACT({label} FROM source_value)");
        let parsed = expression(&source);
        let Expr::Extract(node) = &parsed else {
            panic!("EXTRACT");
        };
        assert_eq!(node.field, field);
        assert_eq!(field.as_str(), label);
        assert_eq!(node.span, Span::new(0, source.len()));
        assert_eq!(
            &source[node.expression.span().start()..node.expression.span().end()],
            "source_value"
        );
        assert_eq!(printer::print_expr(&parsed), source);
        assert!(parsed.syntax_eq(&expression(&source.to_ascii_lowercase())));
    }
    for source in [
        "EXTRACT(EPOCH FROM x)",
        "EXTRACT(MICROSECOND FROM x)",
        "EXTRACT(`YEAR` FROM x)",
        "EXTRACT('YEAR' FROM x)",
    ] {
        let tokens = novarocks_parser::lex(source).unwrap();
        assert!(
            matches!(
                novarocks_parser::parser::parse_expression(source, &tokens),
                Err(novarocks_parser::ParseError::UnexpectedToken { .. })
            ),
            "{source}"
        );
    }
}

#[test]
fn substring_from_normalizes_directly_to_substr_while_ordinary_call_names_stay_real() {
    for (source, ordinary, expected_values) in [
        (
            "SUBSTRING(`payload` FROM position_value FOR length_value)",
            "substr(`payload`, position_value, length_value)",
            vec!["`payload`", "position_value", "length_value"],
        ),
        (
            "SUBSTRING(`payload` FROM position_value)",
            "substr(`payload`, position_value)",
            vec!["`payload`", "position_value"],
        ),
    ] {
        let parsed = expression(source);
        assert!(parsed.syntax_eq(&expression(ordinary)));
        let Expr::FunctionCall(call) = &parsed else {
            panic!("ordinary normalized call");
        };
        assert_eq!(call.name.parts[0].value, "substr");
        assert_eq!(call.name.span, novarocks_parser::SYNTHETIC);
        assert_eq!(call.name.parts[0].span, novarocks_parser::SYNTHETIC);
        assert_eq!(call.span, Span::new(0, source.len()));
        let actual = call
            .arguments
            .iter()
            .map(|argument| {
                assert!(argument.name.is_none());
                assert_eq!(argument.span, argument.value.span());
                &source[argument.span.start()..argument.span.end()]
            })
            .collect::<Vec<_>>();
        assert_eq!(actual, expected_values);
        assert_eq!(printer::print_expr(&parsed), ordinary);
    }
    let ordinary = expression("substring(payload, position_value, length_value)");
    let Expr::FunctionCall(call) = &ordinary else {
        panic!("ordinary call");
    };
    assert_eq!(call.name.parts[0].value, "substring");
    assert!(!ordinary.syntax_eq(&expression("substr(payload, position_value, length_value)")));
    for source in ["catalog.extract(part, payload)", "`extract`(part, payload)"] {
        let parsed = expression(source);
        assert!(matches!(parsed, Expr::FunctionCall(_)));
        assert_eq!(printer::print_expr(&parsed), source);
    }
    let metadata = build::function(
        build::object_name(vec![build::ident("extract", false)]).unwrap(),
        vec![value("part", 10), value("payload", 20)],
    );
    assert_eq!(printer::print_expr(&metadata), "extract(part, payload)");
}

#[test]
fn extract_shared_hooks_and_builder_borrow_real_subtrees_and_keep_field_in_syntax_equality() {
    let source = "EXTRACT(YEAR FROM items[slot])";
    let original = expression(source);
    let Expr::Extract(node) = &original else {
        panic!("EXTRACT");
    };
    let Expr::Subscript(subscript) = &node.expression else {
        panic!("operand");
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
    assert_eq!(
        seen.0,
        vec![
            &original as *const Expr,
            &node.expression,
            &subscript.base,
            &subscript.index
        ]
    );
    struct Change;
    impl Fold for Change {
        fn fold_ident(&mut self, mut id: Ident) -> Ident {
            id.value.push_str("_new");
            id
        }
        fn fold_extract_expr(&mut self, node: ExtractExpr) -> ExtractExpr {
            let mut node = fold_extract_expr(self, node);
            node.field = ExtractField::Quarter;
            node
        }
    }
    let changed = Change.fold_expr(original.clone());
    assert_eq!(
        printer::print_expr(&changed),
        "EXTRACT(QUARTER FROM items_new[slot_new])"
    );
    assert_eq!(changed.span(), original.span());
    let mut field_changed = original.clone();
    let Expr::Extract(node) = &mut field_changed else {
        unreachable!()
    };
    node.field = ExtractField::Day;
    assert!(!original.syntax_eq(&field_changed));
    let mut shifted = original.clone();
    let Expr::Extract(node) = &mut shifted else {
        unreachable!()
    };
    node.span = Span::new(1000, 1001);
    assert!(original.syntax_eq(&shifted));
    let operand = expression("items[slot]");
    let Expr::Subscript(node) = &operand else {
        panic!("operand");
    };
    let original_payload = node.as_ref() as *const SubscriptExpr;
    let original_span = operand.span();
    let built = build::extract(operand, ExtractField::Year);
    let Expr::Extract(node) = &built else {
        unreachable!()
    };
    let Expr::Subscript(child) = &node.expression else {
        unreachable!()
    };
    assert_eq!(child.as_ref() as *const SubscriptExpr, original_payload);
    assert_eq!(node.expression.span(), original_span);
    validate_statement(&statement(built)).unwrap();
    let invalid = build::extract(
        Expr::InList(AstBox::new(InListExpr {
            values: AstVec::new(),
            span: Span::new(70, 73),
        })),
        ExtractField::TimezoneHour,
    );
    assert_eq!(
        validate_statement(&statement(invalid)),
        Err(ValidateError::InvalidStructure {
            violation: StructuralViolation::EmptyInList,
            span: Span::new(70, 73)
        })
    );
}

#[test]
fn row_constructor_preserves_singleton_identity_and_does_not_specialize_ordinary_names() {
    let source = "ROW(items[2])";
    let row = expression(source);
    let Expr::Row(node) = &row else {
        panic!("ROW constructor");
    };
    assert_eq!(node.span, Span::new(0, source.len()));
    assert_eq!(node.fields.len(), 1);
    let field = &node.fields[0];
    assert!(field.name.is_none());
    assert_eq!(field.span, field.expression.span());
    assert_eq!(&source[field.span.start()..field.span.end()], "items[2]");
    assert!(!row.syntax_eq(&expression("(items[2])")));
    assert_eq!(printer::print_expr(&row), source);
    assert!(row.syntax_eq(&expression(&printer::print_expr(&row))));
    let multi = expression("row(a, 2)");
    assert_eq!(printer::print_expr(&multi), "ROW(a, 2)");
    assert!(multi.syntax_eq(&expression("(a, 2)")));
    for source in ["catalog.row(1)", "`row`(1)", "named_struct('x', 1)"] {
        let ordinary = expression(source);
        let Expr::FunctionCall(call) = ordinary else {
            panic!("ordinary source call: {source}");
        };
        assert_eq!(
            call.name.parts.last().unwrap().value,
            if source.starts_with("named") {
                "named_struct"
            } else {
                "row"
            }
        );
        assert!(call.arguments[0].name.is_none());
    }
    for source in [
        "ROW()",
        "ROW(ALL 1)",
        "ROW(ALL)",
        "ROW(DISTINCT 1)",
        "ROW(1 ORDER BY 2)",
        "ROW(1 SEPARATOR ',')",
        "ROW(1 IGNORE NULLS)",
        "ROW(1) RESPECT NULLS",
        "ROW(1) FILTER (WHERE TRUE)",
        "ROW(1) OVER ()",
    ] {
        let tokens = novarocks_parser::lex(source).unwrap();
        assert!(
            novarocks_parser::parser::parse_expression(source, &tokens).is_err(),
            "constructor syntax must reject modifiers: {source}"
        );
    }
}

#[test]
fn map_literal_produces_plain_variadic_call_with_real_ordered_children_and_spans() {
    let source = "map{items[2]: ROW(3), 'key': other}";
    let expr = expression(source);
    let Expr::FunctionCall(call) = &expr else {
        panic!("ordinary map call");
    };
    assert_eq!(call.span, Span::new(0, source.len()));
    assert_eq!(call.name.span, novarocks_parser::SYNTHETIC);
    assert_eq!(call.name.parts.len(), 1);
    assert_eq!(call.name.parts[0].value, "map");
    assert!(!call.name.parts[0].quoted);
    assert_eq!(call.name.parts[0].span, novarocks_parser::SYNTHETIC);
    assert!(!call.distinct);
    assert!(call.processing_mode.is_none());
    assert!(call.order_by.is_none() && call.filter.is_none());
    assert!(call.null_treatment.is_none() && call.window.is_none());
    assert_eq!(call.arguments.len(), 4);
    for (argument, expected) in call
        .arguments
        .iter()
        .zip(["items[2]", "ROW(3)", "'key'", "other"])
    {
        assert!(argument.name.is_none());
        assert_eq!(argument.span, argument.value.span());
        assert_eq!(
            &source[argument.span.start()..argument.span.end()],
            expected
        );
    }
    struct Args(Vec<*const Expr>);
    impl Visit for Args {
        fn visit_call_argument(&mut self, node: &CallArgument) {
            self.0.push(&node.value as *const Expr);
            novarocks_parser::ast::walk_call_argument(self, node);
        }
    }
    let mut args = Args(Vec::new());
    args.visit_expr(&expr);
    assert_eq!(
        args.0,
        call.arguments
            .iter()
            .map(|argument| &argument.value as *const Expr)
            .collect::<Vec<_>>()
    );
    assert!(expr.syntax_eq(&expression("map(items[2], ROW(3), 'key', other)")));
    assert_eq!(
        printer::print_expr(&expr),
        "map(items[2], ROW(3), 'key', other)"
    );
    assert!(expr.syntax_eq(&expression(&printer::print_expr(&expr))));
    let empty = expression("map{}");
    let Expr::FunctionCall(call) = &empty else {
        panic!("empty map call");
    };
    assert!(call.arguments.is_empty());
    assert_eq!(call.span, Span::new(0, 5));
    assert!(empty.syntax_eq(&expression("map()")));
    assert_eq!(printer::print_expr(&empty), "map()");
}

#[test]
fn row_and_map_builders_retain_original_owning_nodes_without_changing_tuple_contracts() {
    let key = expression("items[2]");
    let Expr::Subscript(original) = &key else {
        panic!("key subtree");
    };
    let original_pointer = original.as_ref() as *const SubscriptExpr;
    let key_span = key.span();
    let val = expression("ROW(3)");
    let val_span = val.span();
    let built = build::map(vec![(key, val)]);
    let Expr::FunctionCall(call) = &built else {
        panic!("plain variadic map");
    };
    let Expr::Subscript(moved) = &call.arguments[0].value else {
        panic!("original key subtree");
    };
    assert_eq!(moved.as_ref() as *const SubscriptExpr, original_pointer);
    assert_eq!(call.arguments[0].span, key_span);
    assert_eq!(call.arguments[1].span, val_span);
    assert!(built.syntax_eq(&expression("map(items[2], ROW(3))")));
    let mut top = built.clone();
    let Expr::FunctionCall(call) = &mut top else {
        unreachable!()
    };
    call.arguments.swap(0, 1);
    assert!(!built.syntax_eq(&top));
    assert_eq!(build::tuple(Vec::new()), Err(build::BuildError::EmptyTuple));
    assert_eq!(
        build::tuple_value(Vec::new()),
        Err(build::BuildError::EmptyTuple)
    );
    assert!(matches!(
        build::tuple(vec![build::integer(1)]).unwrap(),
        Expr::Nested(_)
    ));
    let singleton = build::tuple_value(vec![build::integer(1)]).unwrap();
    assert!(matches!(&singleton, Expr::Row(node) if node.fields.len() == 1));
    assert_eq!(printer::print_expr(&singleton), "ROW(1)");
    assert!(singleton.syntax_eq(&expression("ROW(1)")));
    let empty_row = Expr::Row(RowExpr {
        fields: AstVec::new(),
        span: Span::new(8, 13),
    });
    novarocks_parser::ast::validate_statement(&statement(empty_row)).unwrap();
    novarocks_parser::ast::validate_statement(&statement(built)).unwrap();
    let named = Expr::Row(RowExpr {
        fields: vec![
            RowField {
                name: Some(id("x", true, 5)),
                expression: build::integer(1),
                span: Span::new(5, 11),
            },
            RowField {
                name: None,
                expression: build::integer(2),
                span: Span::new(13, 14),
            },
        ]
        .into(),
        span: Span::new(0, 15),
    });
    let Expr::Row(fields) = &named else {
        unreachable!()
    };
    assert!(fields.fields[0].name.as_ref().unwrap().quoted);
    assert!(fields.fields[1].name.is_none());
    assert_eq!(printer::print_expr(&named), "STRUCT(`x` := 1, 2)");
}

#[test]
fn function_distinct_keeps_the_pinned_none_all_merge_and_real_ordinary_names() {
    for name in [
        "f",
        "group_concat",
        "string_agg",
        "catalog.f",
        "`group_concat`",
    ] {
        let absent = expression(&format!("{name}(a, b)"));
        let all = expression(&format!("{name}(ALL a, b)"));
        let distinct = expression(&format!("{name}(DISTINCT a, b)"));
        let Expr::FunctionCall(call) = &all else {
            panic!("ordinary function");
        };
        assert!(!call.distinct);
        assert_eq!(call.arguments.len(), 2);
        assert!(all.syntax_eq(&absent));
        assert!(!all.syntax_eq(&distinct));
        let Expr::FunctionCall(call) = &distinct else {
            unreachable!()
        };
        assert!(call.distinct);
        assert!(distinct.syntax_eq(&expression(&printer::print_expr(&distinct))));
        assert!(!printer::print_expr(&all).contains("ALL "));
    }
    let built = build::function(
        build::object_name(vec![build::ident("f", false)]).unwrap(),
        vec![build::integer(1)],
    );
    let Expr::FunctionCall(call) = &built else {
        unreachable!()
    };
    assert!(!call.distinct);
    for source in ["group_concat()", "string_agg()"] {
        let Expr::FunctionCall(call) = expression(source) else {
            unreachable!()
        };
        assert!(call.arguments.is_empty());
        assert_eq!(call.name.parts[0].value, source.trim_end_matches("()"));
    }
}

#[test]
fn explicit_separator_moves_real_last_argument_and_keeps_all_other_metadata() {
    let source = "group_concat(DISTINCT a, items[2] ORDER BY sortcol DESC SEPARATOR delimiter[3]) FILTER (WHERE flag) OVER (PARTITION BY partition_key)";
    let expr = expression(source);
    let Expr::FunctionCall(call) = &expr else {
        panic!("canonical string_agg");
    };
    assert_eq!(call.name.parts.len(), 1);
    assert_eq!(call.name.parts[0].value, "string_agg");
    assert!(!call.name.parts[0].quoted);
    assert_eq!(call.name.span, novarocks_parser::SYNTHETIC);
    assert_eq!(call.name.parts[0].span, novarocks_parser::SYNTHETIC);
    assert_eq!(call.span, Span::new(0, source.len()));
    assert!(call.distinct);
    assert!(call.processing_mode.is_none());
    assert!(call.null_treatment.is_none());
    assert_eq!(call.arguments.len(), 3);
    for (argument, expected) in call.arguments.iter().zip(["a", "items[2]", "delimiter[3]"]) {
        assert!(argument.name.is_none());
        assert_eq!(argument.span, argument.value.span());
        assert_eq!(
            &source[argument.span.start()..argument.span.end()],
            expected
        );
    }
    let order = call.order_by.as_ref().unwrap();
    assert_eq!(
        &source[order.span.start()..order.span.end()],
        "ORDER BY sortcol DESC"
    );
    assert_eq!(order.items[0].ordering, SortOrdering::Descending);
    assert_eq!(order.items[0].null_ordering, NullOrdering::Undefined);
    let filter = call.filter.as_ref().unwrap();
    assert_eq!(&source[filter.span().start()..filter.span().end()], "flag");
    let window = call.window.as_ref().unwrap();
    assert_eq!(
        &source[window.span().start()..window.span().end()],
        "(PARTITION BY partition_key)"
    );
    assert!(expr.syntax_eq(&expression("string_agg(DISTINCT a, items[2], delimiter[3] ORDER BY sortcol DESC) FILTER (WHERE flag) OVER (PARTITION BY partition_key)")));
    assert!(expr.syntax_eq(&expression(&printer::print_expr(&expr))));
    struct Args(Vec<*const Expr>);
    impl Visit for Args {
        fn visit_call_argument(&mut self, node: &CallArgument) {
            self.0.push(&node.value as *const Expr);
            novarocks_parser::ast::walk_call_argument(self, node);
        }
    }
    let mut visited = Args(Vec::new());
    visited.visit_expr(&expr);
    assert_eq!(
        visited.0,
        call.arguments
            .iter()
            .map(|node| &node.value as *const Expr)
            .collect::<Vec<_>>()
    );
    let source = "STRING_AGG(value SEPARATOR NULL)";
    let explicit_alias = expression(source);
    let Expr::FunctionCall(call) = &explicit_alias else {
        unreachable!()
    };
    assert_eq!(call.name.parts[0].value, "STRING_AGG");
    assert_eq!(call.name.span, Span::new(0, "STRING_AGG".len()));
    assert_eq!(call.span, Span::new(0, source.len()));
    assert_eq!(call.arguments.len(), 2);
    assert!(matches!(
        &call.arguments[1].value,
        Expr::Literal(Literal {
            kind: LiteralKind::Null,
            ..
        })
    ));
    assert!(explicit_alias.syntax_eq(&expression("STRING_AGG(value, NULL)")));
    novarocks_parser::ast::validate_statement(&statement(explicit_alias)).unwrap();
}

#[test]
fn separator_refuses_illegal_source_shapes_before_the_fact_can_be_erased() {
    for source in [
        "group_concat(SEPARATOR NULL)",
        "group_concat(ORDER BY a SEPARATOR ',')",
        "string_agg(SEPARATOR ',')",
        "array_agg(SEPARATOR NULL)",
        "array_agg(a SEPARATOR ',')",
        "f(a SEPARATOR ',')",
        "catalog.group_concat(a SEPARATOR ',')",
        "`group_concat`(a SEPARATOR ',')",
        "`string_agg`(a SEPARATOR ',')",
    ] {
        let tokens = novarocks_parser::lex(source).unwrap();
        let error = novarocks_parser::parser::parse_expression(source, &tokens).expect_err(source);
        let novarocks_parser::ParseError::UnexpectedToken { span, .. } = error else {
            panic!("typed Parse refusal: {source}");
        };
        assert_eq!(
            span,
            Span::new(0, source.len()),
            "real complete call span: {source}"
        );
    }
    for source in ["group_concat(a, b)", "string_agg(a, b)"] {
        let Expr::FunctionCall(call) = expression(source) else {
            unreachable!()
        };
        assert_eq!(call.name.parts[0].value, source.split('(').next().unwrap());
        assert_eq!(call.arguments.len(), 2);
    }
}
