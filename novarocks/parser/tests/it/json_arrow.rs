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

//! Old-producer semantic bridge; this is not the target JSON_QUERY AstBuilder result.
use novarocks_parser::{Span, ast::*, build, printer};
fn expression(source: &str) -> Expr {
    let tokens = novarocks_parser::lex(source).unwrap();
    novarocks_parser::parser::parse_expression(source, &tokens).unwrap()
}
fn name() -> ObjectName {
    ObjectName {
        parts: vec![build::ident("json_query", false)],
        span: novarocks_parser::SYNTHETIC,
    }
}
#[test]
fn both_old_arrows_move_literal_children_to_one_plain_call_without_claiming_target_node_equality() {
    let mut nodes = Vec::new();
    for operator in ["->", "->>"] {
        let source = format!("doc.payload {operator} '$.a''b'");
        let expr = expression(&source);
        let Expr::FunctionCall(call) = &expr else {
            panic!("old ordinary-call bridge");
        };
        assert_eq!(call.name.parts.len(), 1);
        assert_eq!(call.name.parts[0].value, "json_query");
        assert!(!call.name.parts[0].quoted);
        assert_eq!(call.name.span, novarocks_parser::SYNTHETIC);
        assert_eq!(call.name.parts[0].span, novarocks_parser::SYNTHETIC);
        assert_eq!(call.span, Span::new(0, source.len()));
        assert_eq!(call.arguments.len(), 2);
        assert!(!call.distinct);
        assert!(
            call.order_by.is_none()
                && call.filter.is_none()
                && call.null_treatment.is_none()
                && call.window.is_none()
                && call.processing_mode.is_none()
        );
        for (argument, expected) in call.arguments.iter().zip(["doc.payload", "'$.a''b'"]) {
            assert!(argument.name.is_none());
            assert_eq!(argument.span, argument.value.span());
            assert_eq!(
                &source[argument.span.start()..argument.span.end()],
                expected
            );
        }
        assert!(
            matches!(&call.arguments[1].value,Expr::Literal(Literal {kind:LiteralKind::String(value),..}) if value=="$.a'b")
        );
        assert!(expr.syntax_eq(&expression("json_query(doc.payload, '$.a''b')")));
        assert_eq!(
            printer::print_expr(&expr),
            "json_query(doc.payload, '$.a''b')"
        );
        let dedicated = Expr::JsonQuery(AstBox::new(JsonQuery::plain(
            JsonPathInvocation::plain(
                expression("doc.payload"),
                JsonPathString::new("$.a'b".to_owned(), novarocks_parser::SYNTHETIC),
                novarocks_parser::SYNTHETIC,
            ),
            novarocks_parser::SYNTHETIC,
        )));
        // P01 source-bound literal facts need their own canonicalization for
        // strict P11 comparison. This old bridge is not that final baseline.
        assert!(!expr.syntax_eq(&dedicated));
        nodes.push(expr);
    }
    assert!(nodes[0].syntax_eq(&nodes[1]));
}
#[test]
fn dynamic_null_and_parenthesized_paths_remain_real_expressions_in_the_old_bridge() {
    for (source, path) in [
        ("doc.payload -> path_column", "path_column"),
        ("doc.payload ->> NULL", "NULL"),
        ("JSON '{\"a\":1}' -> concat('$.','a')", "concat('$.','a')"),
        ("doc.payload -> ('$.a')", "('$.a')"),
    ] {
        let expr = expression(source);
        let Expr::FunctionCall(call) = &expr else {
            panic!("ordinary bridge");
        };
        assert_eq!(call.name.parts[0].value, "json_query");
        assert_eq!(call.arguments.len(), 2);
        let start = source.find(path).unwrap();
        assert_eq!(
            call.arguments[1].value.span(),
            Span::new(start, start + path.len())
        );
        assert!(call.arguments[1].value.syntax_eq(&expression(path)));
        assert_eq!(call.span, Span::new(0, source.len()));
    }
    let chained = expression("doc.payload -> '$.a' -> '$.b'");
    let Expr::FunctionCall(outer) = chained else {
        unreachable!()
    };
    assert!(
        matches!(&outer.arguments[1].value,Expr::FunctionCall(call) if call.name.parts[0].value=="json_query")
    );
    // Keep the actual successful old source's admission; result values are a
    // separate native oracle owned by Root, never inferred from parsing.
    let source = "SELECT typeof(CAST(JSON '{\"a\":1}' -> concat('$.','a') AS integer)) AS observed";
    let statements = novarocks_parser::parse(source).unwrap();
    let Statement::Query(query) = &statements[0] else {
        unreachable!()
    };
    let SetExpr::Select(select) = query.body.as_ref() else {
        unreachable!()
    };
    let SelectItem::ExprWithAlias {
        expr: Expr::FunctionCall(typeof_call),
        alias,
        ..
    } = &select.projection[0]
    else {
        panic!("typeof projection");
    };
    assert_eq!(alias.value, "observed");
    let Expr::Cast(cast) = &typeof_call.arguments[0].value else {
        panic!("original Cast declaration");
    };
    let Expr::FunctionCall(json) = &cast.expression else {
        panic!("opaque original scalar call");
    };
    assert_eq!(json.name.parts[0].value, "json_query");
    assert!(
        matches!(&json.arguments[1].value,Expr::FunctionCall(call) if call.name.parts[0].value=="concat")
    );
    assert!(matches!(expression("(a,b) -> a"), Expr::Lambda(_)));
    for source in ["ROW(a,b) -> path", "(ROW(a,b)) -> path"] {
        let Expr::FunctionCall(call) = expression(source) else {
            panic!("ROW must remain json operation: {source}");
        };
        assert_eq!(call.name.parts[0].value, "json_query");
        assert!(matches!(
            &call.arguments[0].value,
            Expr::Row(_) | Expr::Nested(_)
        ));
        assert!(matches!(&call.arguments[1].value,Expr::Identifier(id) if id.value=="path"));
    }
}
#[test]
fn ordinary_bridge_author_retains_original_owning_payloads_and_actual_argument_borrows() {
    let input = expression("items[2]");
    let path = expression("concat('$.', key)");
    let Expr::Subscript(base) = &input else {
        unreachable!()
    };
    let original_base = base.as_ref() as *const SubscriptExpr;
    let Expr::FunctionCall(original_path) = &path else {
        unreachable!()
    };
    let original_path = original_path.as_ref() as *const FunctionCall;
    let input_span = input.span();
    let path_span = path.span();
    let expr = build::function(name(), vec![input, path]);
    let Expr::FunctionCall(call) = &expr else {
        unreachable!()
    };
    let Expr::Subscript(base) = &call.arguments[0].value else {
        unreachable!()
    };
    assert_eq!(base.as_ref() as *const SubscriptExpr, original_base);
    let Expr::FunctionCall(path) = &call.arguments[1].value else {
        unreachable!()
    };
    assert_eq!(path.as_ref() as *const FunctionCall, original_path);
    assert_eq!(call.arguments[0].span, input_span);
    assert_eq!(call.arguments[1].span, path_span);
    struct Arguments(Vec<*const Expr>);
    impl Visit for Arguments {
        fn visit_call_argument(&mut self, node: &CallArgument) {
            self.0.push(&node.value as *const Expr);
            novarocks_parser::ast::walk_call_argument(self, node);
        }
    }
    let mut borrowed = Arguments(Vec::new());
    borrowed.visit_expr(&expr);
    assert_eq!(
        borrowed.0,
        vec![
            &call.arguments[0].value as *const Expr,
            &call.arguments[1].value as *const Expr,
            &path.arguments[0].value as *const Expr,
            &path.arguments[1].value as *const Expr,
        ]
    );
    assert!(expr.syntax_eq(&expression("json_query(items[2], concat('$.', key))")));
}
