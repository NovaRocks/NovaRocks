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
    novarocks_parser::parser::parse_expression(source).unwrap()
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
fn statement(expr: Expr) -> TrinoStatement {
    TrinoStatement::Query(build::query(SetExpr::Select(
        novarocks_parser::ast::AstBox::new(
            build::select(vec![build::item(expr, None)], None, None, None).unwrap(),
        ),
    )))
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
        printer::print_expression(&folded).unwrap(),
        "RUNNING \"ns_new\".\"f_new\"(\"arg_new\" => v_new, other_new) FILTER (WHERE predicate_new)"
    );
    assert_eq!(
        printer::print_expression(&original).unwrap(),
        "FINAL \"ns\".\"f\"(\"arg\" => v, other) FILTER (WHERE predicate)"
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
            printer::print_expression(&absent).unwrap(),
            printer::print_expression(&explicit_null).unwrap()
        ),
        (
            "IF(flag, first)".to_owned(),
            "IF(flag, first, NULL)".to_owned()
        )
    );
    assert!(!absent.syntax_eq(&explicit_null));
    assert!(absent.syntax_eq(&expression("\"iF\"(flag, first)")));
    assert!(absent.syntax_eq(&expression("IF(ALL flag, first)")));
    for source in ["schema.if(flag, first)", "\"schema.if\"(flag, first)"] {
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
        assert_eq!(printer::print_expression(&node).unwrap(), canonical);
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
        assert!(
            matches!(novarocks_parser::parser::parse_expression(source),Err(novarocks_parser::ParserError::Parse(ParseError::UnexpectedToken{span,..})) if span==Span::new(0,source.len())),
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
            assert!(
                novarocks_parser::parser::parse_expression(&source).is_err(),
                "{source}"
            );
        }
    }
    let source = "if(a) OVER w";
    let error = novarocks_parser::parser::parse_expression(source).unwrap_err();
    assert!(
        matches!(error,novarocks_parser::ParserError::Parse(ParseError::UnexpectedToken{expected,..}) if expected.contains("arguments"))
    );
    assert!(
        novarocks_parser::parser::parse_statement("SELECT * FROM TABLE(if(named => a, b))").is_ok()
    );
    // The exact pinned builder visits ORDER BY and then omits it from these
    // dedicated nodes. This assertion is syntax, not execution capability.
    let with_order = expression("coalesce(a, b, c, d ORDER BY ignored)");
    assert!(with_order.syntax_eq(&expression("coalesce(a, b, c, d)")));
    assert_eq!(
        printer::print_expression(&with_order).unwrap(),
        "COALESCE(a, b, c, d)"
    );
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
        printer::print_expression(&changed).unwrap(),
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
        assert_eq!(printer::print_expression(&parsed).unwrap(), source);
        assert!(parsed.syntax_eq(&expression(&source.to_ascii_lowercase())));
    }
    for source in [
        "EXTRACT(EPOCH FROM x)",
        "EXTRACT(MICROSECOND FROM x)",
        "EXTRACT(\"YEAR\" FROM x)",
        "EXTRACT('YEAR' FROM x)",
    ] {
        assert!(
            matches!(
                novarocks_parser::parser::parse_expression(source),
                Err(novarocks_parser::ParserError::Parse(
                    novarocks_parser::ParseError::UnexpectedToken { .. }
                ))
            ),
            "{source}"
        );
    }
}

#[test]
fn substring_from_normalizes_directly_to_substr_while_ordinary_call_names_stay_real() {
    for (source, ordinary, expected_values) in [
        (
            "SUBSTRING(\"payload\" FROM position_value FOR length_value)",
            "substr(\"payload\", position_value, length_value)",
            vec!["\"payload\"", "position_value", "length_value"],
        ),
        (
            "SUBSTRING(\"payload\" FROM position_value)",
            "substr(\"payload\", position_value)",
            vec!["\"payload\"", "position_value"],
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
        assert_eq!(printer::print_expression(&parsed).unwrap(), ordinary);
    }
    let ordinary = expression("substring(payload, position_value, length_value)");
    let Expr::FunctionCall(call) = &ordinary else {
        panic!("ordinary call");
    };
    assert_eq!(call.name.parts[0].value, "substring");
    assert!(!ordinary.syntax_eq(&expression("substr(payload, position_value, length_value)")));
    for source in [
        "catalog.extract_field(part, payload)",
        "\"extract\"(part, payload)",
    ] {
        let parsed = expression(source);
        assert!(matches!(parsed, Expr::FunctionCall(_)));
        assert_eq!(printer::print_expression(&parsed).unwrap(), source);
    }
    let metadata = build::function(
        build::object_name(vec![build::ident("extract", false)]).unwrap(),
        vec![value("part", 10), value("payload", 20)],
    );
    assert_eq!(
        printer::print_expression(&metadata).unwrap(),
        "extract(part, payload)"
    );
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
        printer::print_expression(&changed).unwrap(),
        "EXTRACT(QUARTER FROM (items_new)[slot_new])"
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
    assert_eq!(printer::print_expression(&row).unwrap(), "ROW((items)[2])");
    assert!(row.syntax_eq(&expression(&printer::print_expression(&row).unwrap())));
    let multi = expression("row(a, 2)");
    assert_eq!(printer::print_expression(&multi).unwrap(), "ROW(a, 2)");
    assert!(multi.syntax_eq(&expression("(a, 2)")));
    for source in ["catalog.row(1)", "\"row\"(1)", "named_struct('x', 1)"] {
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
        "ROW(DISTINCT 1)",
        "ROW(1 ORDER BY 2)",
        "ROW(1 SEPARATOR ',')",
        "ROW(1 IGNORE NULLS)",
        "ROW(1) RESPECT NULLS",
        "ROW(1) FILTER (WHERE TRUE)",
        "ROW(1) OVER ()",
    ] {
        assert!(
            novarocks_parser::parser::parse_expression(source).is_err(),
            "constructor syntax must reject modifiers: {source}"
        );
    }
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
    let built = build::map(build::array(vec![key]), build::array(vec![val]));
    let Expr::FunctionCall(call) = &built else {
        panic!("two-array map");
    };
    let (Expr::Array(keys), Expr::Array(values)) =
        (&call.arguments[0].value, &call.arguments[1].value)
    else {
        panic!("key and value arrays");
    };
    let Expr::Subscript(moved) = &keys.values[0] else {
        panic!("original key subtree");
    };
    assert_eq!(moved.as_ref() as *const SubscriptExpr, original_pointer);
    assert_eq!(keys.values[0].span(), key_span);
    assert_eq!(values.values[0].span(), val_span);
    assert!(built.syntax_eq(&expression("map(ARRAY[items[2]], ARRAY[ROW(3)])")));
    let mut top = built.clone();
    let Expr::FunctionCall(call) = &mut top else {
        unreachable!()
    };
    call.arguments.swap(0, 1);
    assert!(!built.syntax_eq(&top));
    assert_eq!(
        build::tuple_value(Vec::new()),
        Err(build::BuildError::EmptyTuple)
    );
    let singleton = build::tuple_value(vec![build::integer(1)]).unwrap();
    assert!(matches!(&singleton, Expr::Row(node) if node.fields.len() == 1));
    assert_eq!(printer::print_expression(&singleton).unwrap(), "ROW(1)");
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
    assert_eq!(
        printer::print_expression(&named).unwrap(),
        "ROW(1 AS \"x\", 2)"
    );
}

#[test]
fn function_distinct_keeps_the_pinned_none_all_merge_and_real_ordinary_names() {
    for name in [
        "f",
        "group_concat",
        "string_agg",
        "catalog.f",
        "\"group_concat\"",
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
        assert!(distinct.syntax_eq(&expression(&printer::print_expression(&distinct).unwrap())));
        assert!(!printer::print_expression(&all).unwrap().contains("ALL "));
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
