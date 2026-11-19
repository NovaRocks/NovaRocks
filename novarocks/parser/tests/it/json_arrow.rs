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

//! Transitional old arrow producer coverage. The target JSON_QUERY grammar is tested separately.
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
fn both_old_arrows_with_direct_string_literals_match_plain_trino_json_query() {
    let mut nodes = Vec::new();
    for operator in ["->", "->>"] {
        let source = format!("doc.payload {operator} '$.a''b'");
        let expr = expression(&source);
        let Expr::JsonQuery(node) = &expr else {
            panic!("literal JSON arrow must produce the dedicated query");
        };
        assert_eq!(node.span, Span::new(0, source.len()));
        assert_eq!(node.path_invocation.span, node.span);
        assert_eq!(
            node.path_invocation.input_expression.span(),
            Span::new(0, 11)
        );
        let path = &node.path_invocation.json_path;
        assert_eq!(&source[path.span.start()..path.span.end()], "'$.a''b'");
        assert_eq!(path.value, "$.a'b");
        let dedicated =
            novarocks_parser::trino_parser::parse_expression("JSON_QUERY(doc.payload, '$.a''b')")
                .unwrap();
        assert!(expr.syntax_eq(&dedicated));
        let legacy_printed = printer::print_expr(&expr);
        assert_eq!(legacy_printed, "doc.payload -> '$.a''b'");
        assert!(expr.syntax_eq(&expression(&legacy_printed)));
        let canonical = novarocks_parser::trino_printer::print_expression(&expr).unwrap();
        let reparsed = novarocks_parser::trino_parser::parse_expression(&canonical).unwrap();
        assert!(expr.syntax_eq(&reparsed));
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
    let SelectItem::SingleColumn(column) = &select.projection[0] else {
        panic!("typeof projection")
    };
    let Expr::FunctionCall(typeof_call) = &column.expression else {
        panic!("typeof expression")
    };
    let alias = column.alias.as_ref().expect("original alias");
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
            Expr::Row(_) | Expr::LegacyParenthesized(_)
        ));
        assert!(matches!(&call.arguments[1].value,Expr::Identifier(id) if id.value=="path"));
    }
}
#[test]
fn multi_hop_arrow_legacy_printer_preserves_authored_groups_and_path_association() {
    for (source, expected) in [
        ("j -> '$.a' -> '$.b'", "json_query(j, '$.a' -> '$.b')"),
        ("(j -> '$.a') -> '$.b'", "(j -> '$.a') -> '$.b'"),
        ("((j -> '$.a')) -> '$.b'", "((j -> '$.a')) -> '$.b'"),
        ("j ->> ('$.a' ->> '$.b')", "json_query(j, ('$.a' -> '$.b'))"),
        ("(j -> '$.a') ->> path", "json_query((j -> '$.a'), path)"),
        (
            "j -> '$.a' ->> path",
            "json_query(j, json_query('$.a', path))",
        ),
        (
            "j ->> concat('$.', key) -> '$.b'",
            "json_query(j, concat('$.', key) -> '$.b')",
        ),
        (
            "(j ->> concat('$.', key)) -> '$.b'",
            "(json_query(j, concat('$.', key))) -> '$.b'",
        ),
        ("(j -> '$.a')[1] ->> '$.b'", "(j -> '$.a')[1] -> '$.b'"),
        (
            "j -> '$.a' -> '$.b' -> '$.c'",
            "json_query(j, json_query('$.a', '$.b' -> '$.c'))",
        ),
        ("j -> path -> '$.b'", "j -> path -> '$.b'"),
        ("(-1) -> '$.a'", "(-1) -> '$.a'"),
        ("CAST((j -> '$.a') AS JSON)", "CAST((j -> '$.a') AS JSON)"),
    ] {
        let original = expression(source);
        let printed = printer::print_expr(&original);
        assert_eq!(printed, expected, "{source}");
        assert!(original.syntax_eq(&expression(&printed)), "{source}");
    }

    let right_nested = expression("j -> '$.a' -> '$.b'");
    let Expr::FunctionCall(outer) = &right_nested else {
        panic!("the right-hand arrow is the outer call's path expression");
    };
    assert!(matches!(
        &outer.arguments[1].value,
        Expr::JsonQuery(query)
            if query.path_invocation.json_path.value == "$.b"
                && query.path_invocation.input_expression.syntax_eq(&expression("'$.a'"))
    ));

    let left_nested = expression("(j -> '$.a') -> '$.b'");
    let Expr::JsonQuery(outer) = &left_nested else {
        panic!("the grouped left-hand arrow yields a literal-path outer query");
    };
    assert!(matches!(
        &outer.path_invocation.input_expression,
        Expr::LegacyParenthesized(group)
            if matches!(group.expression.as_ref(), Expr::JsonQuery(_))
    ));
    assert!(!right_nested.syntax_eq(&left_nested));

    let typed =
        novarocks_parser::trino_parser::parse_expression("JSON_QUERY(JSON_QUERY(j, '$.a'), '$.b')")
            .unwrap();
    // A typed left-nested input has no authored parentheses. Legacy arrow
    // spelling requires one pair to preserve the left composition.
    let printed = printer::print_expr(&typed);
    assert_eq!(printed, "(j -> '$.a') -> '$.b'");
    assert!(left_nested.syntax_eq(&expression(&printed)));
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
