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

use novarocks_parser::{Span, ast::*, printer};
fn expression(source: &str) -> Expr {
    let tokens = novarocks_parser::lex(source).unwrap();
    novarocks_parser::parser::parse_expression(source, &tokens).unwrap()
}
fn call(expr: &Expr) -> &FunctionCall {
    let Expr::FunctionCall(node) = expr else {
        panic!("ordinary normalized call: {expr:?}");
    };
    node
}
fn assert_plain(node: &FunctionCall, name: &str, quoted: bool) {
    assert_eq!(node.name.parts.len(), 1);
    assert_eq!(node.name.parts[0].value, name);
    assert_eq!(node.name.parts[0].quoted, quoted);
    assert_eq!(node.name.span, novarocks_parser::SYNTHETIC);
    assert_eq!(node.name.parts[0].span, novarocks_parser::SYNTHETIC);
    assert!(!node.distinct);
    assert!(
        node.order_by.is_none()
            && node.filter.is_none()
            && node.null_treatment.is_none()
            && node.window.is_none()
            && node.processing_mode.is_none()
    );
    assert!(
        node.arguments
            .iter()
            .all(|argument| argument.name.is_none() && argument.span == argument.value.span())
    );
}
#[test]
fn position_in_reverses_actual_children_but_all_ordinary_position_call_grammars_stay_real() {
    for source in ["POSITION + needle(1 IN (2))", "POSITION + needle([1,2])"] {
        let Expr::Binary(binary) = expression(source) else {
            panic!("POSITION remains an ordinary identifier outside its own call");
        };
        assert_eq!(binary.operator, BinaryOperator::Add);
        assert!(matches!(*binary.left, Expr::Identifier(ref id) if id.value == "POSITION"));
    }
    let source = "POSITION(needle(1) IN haystack(2))";
    let expr = expression(source);
    let node = call(&expr);
    assert_plain(node, "strpos", false);
    assert_eq!(node.span, Span::new(0, source.len()));
    assert_eq!(node.arguments.len(), 2);
    for (arg, expected) in node.arguments.iter().zip(["haystack(2)", "needle(1)"]) {
        assert_eq!(&source[arg.span.start()..arg.span.end()], expected);
    }
    assert!(expr.syntax_eq(&expression("strpos(haystack(2), needle(1))")));
    assert_eq!(printer::print_expr(&expr), "strpos(haystack(2), needle(1))");
    for source in [
        "POSITION([1,2] IN text_value)",
        "POSITION([1,2] IN [3,4])",
        "POSITION(map{1:2,3:4} IN text_value)",
    ] {
        let expr = expression(source);
        assert_plain(call(&expr), "strpos", false);
        assert_eq!(call(&expr).arguments.len(), 2);
        let needle = &call(&expr).arguments[1].value;
        assert!(matches!(needle, Expr::Array(_) | Expr::FunctionCall(_)));
        assert!(
            source[needle.span().start()..needle.span().end()].starts_with("[")
                || source[needle.span().start()..needle.span().end()].starts_with("map{")
        );
    }
    for source in [
        "POSITION(a,b)",
        "POSITION(a IN (b),c)",
        "`POSITION`(a,b)",
        "ns.POSITION(a,b)",
    ] {
        let expr = expression(source);
        let node = call(&expr);
        assert_eq!(node.name.parts.last().unwrap().value, "POSITION");
        assert_eq!(node.arguments.len(), 2);
        if source.starts_with("POSITION(a IN") {
            assert!(matches!(&node.arguments[0].value, Expr::Predicated(_)));
        } else {
            assert!(matches!(&node.arguments[0].value,Expr::Identifier(id) if id.value=="a"));
        }
    }
    let expr = expression("POSITION((a IN (b)) IN text_value)");
    let node = call(&expr);
    assert_eq!(node.name.parts[0].value, "strpos");
    assert!(matches!(
        &node.arguments[1].value,
        Expr::LegacyParenthesized(_)
    ));
}
#[test]
fn normalize_keeps_the_delimited_name_raw_form_text_context_literal_location_and_default() {
    for (source, form) in [
        ("NORMALIZE(value)", "NFC"),
        ("NORMALIZE(value,NFC)", "NFC"),
        ("NORMALIZE(value,NFD)", "NFD"),
        ("NORMALIZE(value,NFKC)", "NFKC"),
        ("NORMALIZE(value,NFKD)", "NFKD"),
        ("normalize(value,nFd)", "nFd"),
    ] {
        let expr = expression(source);
        let node = call(&expr);
        assert_plain(node, "normalize", true);
        assert_eq!(node.span, Span::new(0, source.len()));
        assert_eq!(node.arguments.len(), 2);
        let Expr::Literal(literal) = &node.arguments[1].value else {
            panic!("real form StringLiteral");
        };
        assert_eq!(literal.kind, LiteralKind::String(form.to_owned()));
        assert_eq!(literal.span, node.span);
        let input = &node.arguments[0].value;
        assert_eq!(&source[input.span().start()..input.span().end()], "value");
        assert!(expr.syntax_eq(&expression(&printer::print_expr(&expr))));
    }
    assert!(expression("NORMALIZE(value)").syntax_eq(&expression("NORMALIZE(value,NFC)")));
    assert!(!expression("NORMALIZE(value,NFD)").syntax_eq(&expression("NORMALIZE(value,nfd)")));
    for source in ["`normalize`(value)", "ns.normalize(value,form)"] {
        let expr = expression(source);
        let node = call(&expr);
        if source.starts_with('`') {
            assert_eq!(node.arguments.len(), 1);
            assert!(node.name.parts[0].quoted);
        } else {
            assert_eq!(node.name.parts.len(), 2);
            assert!(matches!(&node.arguments[1].value,Expr::Identifier(id) if id.value=="form"));
        }
    }
    for source in [
        "NORMALIZE()",
        "NORMALIZE(value,'NFC')",
        "NORMALIZE(value,`NFC`)",
        "NORMALIZE(value,UNKNOWN_FORM)",
        "NORMALIZE(value,NFC,NFD)",
    ] {
        let tokens = novarocks_parser::lex(source).unwrap();
        assert!(
            novarocks_parser::parser::parse_expression(source, &tokens).is_err(),
            "invalid normal form syntax: {source}"
        );
    }
}
