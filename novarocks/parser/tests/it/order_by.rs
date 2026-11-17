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

use novarocks_parser::{SYNTHETIC, Span, StructuralViolation, ValidateError, ast::*, build};

fn expression(source: &str) -> Expr {
    let tokens = novarocks_parser::lex(source).unwrap();
    novarocks_parser::parser::parse_expression(source, &tokens).unwrap()
}

fn query(source: &str) -> Query {
    let mut statements = novarocks_parser::parse(source).unwrap();
    let Statement::Query(query) = statements.remove(0) else {
        panic!("query");
    };
    query
}

fn select(query: &Query) -> &Select {
    let SetExpr::Select(select) = query.body.as_ref() else {
        panic!("select");
    };
    select
}

#[test]
fn all_three_order_owners_capture_their_real_keyword_and_original_keys() {
    let source = "SELECT x FROM t ORDER /* clause */ BY x, `Last` DESC NULLS LAST LIMIT 2";
    let query = query(source);
    assert!(query.modifiers.order_by.is_none());
    let order = select(&query).modifiers.order_by.as_ref().unwrap();
    assert_eq!(
        &source[order.span.start()..order.span.end()],
        "ORDER /* clause */ BY x, `Last` DESC NULLS LAST"
    );
    assert_eq!(order.items.len(), 2);
    let (first, last) = (&order.items[0], &order.items[1]);
    assert_eq!(
        (first.ordering, first.null_ordering),
        (SortOrdering::Ascending, NullOrdering::Undefined)
    );
    assert_eq!(
        (last.ordering, last.null_ordering),
        (SortOrdering::Descending, NullOrdering::Last)
    );
    let Expr::Identifier(name) = &last.sort_key else {
        panic!("original quoted key");
    };
    assert_eq!((name.value.as_str(), name.quoted), ("Last", true));
    assert_eq!(&source[name.span.start()..name.span.end()], "`Last`");
    assert_eq!(
        &source[last.span.start()..last.span.end()],
        "`Last` DESC NULLS LAST"
    );

    for (source, expected, window) in [
        (
            "f(v ORDER /* call */ BY k DESC, b)",
            "ORDER /* call */ BY k DESC, b",
            false,
        ),
        (
            "f(v) OVER (PARTITION BY p ORDER /* window */ BY k NULLS FIRST, b DESC ROWS 1 PRECEDING)",
            "ORDER /* window */ BY k NULLS FIRST, b DESC",
            true,
        ),
    ] {
        let expression = expression(source);
        let Expr::FunctionCall(call) = &expression else {
            panic!("call");
        };
        let order = if window {
            match call.window.as_ref().unwrap() {
                Window::Specification(spec) => spec.order_by.as_ref().unwrap(),
                _ => panic!("specification"),
            }
        } else {
            call.order_by.as_ref().unwrap()
        };
        assert_eq!(&source[order.span.start()..order.span.end()], expected);
        assert_eq!(
            &source[order.items[0].sort_key.span().start()..order.items[0].sort_key.span().end()],
            "k"
        );
        assert_eq!(
            &source[order.items[1].sort_key.span().start()..order.items[1].sort_key.span().end()],
            "b"
        );
    }
}

#[test]
fn sort_defaults_and_shared_hooks_preserve_all_non_span_syntax_facts() {
    let implicit = expression("f(v ORDER BY k, b DESC)");
    let explicit = expression("f(v ORDER BY k ASC, b DESC)");
    assert!(implicit.syntax_eq(&explicit));
    for source in [
        "f(v ORDER BY k NULLS FIRST, b DESC)",
        "f(v ORDER BY k NULLS LAST, b DESC)",
        "f(v ORDER BY b, k DESC)",
        "f(v)",
    ] {
        assert!(!implicit.syntax_eq(&expression(source)), "{source}");
    }
    let Expr::FunctionCall(call) = &implicit else {
        panic!("call");
    };
    let order = call.order_by.as_ref().unwrap();
    // The shared hook observes the original key occurrences, not rebuilt ASTs.
    struct Identities(Vec<*const Expr>);
    impl Visit for Identities {
        fn visit_sort_item(&mut self, item: &SortItem) {
            self.0.push(&item.sort_key);
            walk_sort_item(self, item);
        }
    }
    let mut identities = Identities(Vec::new());
    identities.visit_expr(&implicit);
    assert_eq!(
        identities.0,
        vec![
            &order.items[0].sort_key as *const Expr,
            &order.items[1].sort_key as *const Expr
        ]
    );
    struct Rename;
    impl Fold for Rename {
        fn fold_ident(&mut self, mut ident: Ident) -> Ident {
            ident.value.push_str("_new");
            ident
        }
    }
    let folded = Rename.fold_order_by(order.clone());
    for (original, changed) in order.items.iter().zip(folded.items.iter()) {
        let (Expr::Identifier(before), Expr::Identifier(after)) =
            (&original.sort_key, &changed.sort_key)
        else {
            panic!("identifiers");
        };
        assert_eq!(after.value, format!("{}_new", before.value));
        assert_eq!(
            (
                after.quoted,
                after.span,
                changed.span,
                changed.ordering,
                changed.null_ordering
            ),
            (
                before.quoted,
                before.span,
                original.span,
                original.ordering,
                original.null_ordering
            )
        );
    }
    let mut shifted = order.clone();
    shifted.span = Span::new(1000, 1001);
    for item in &mut shifted.items {
        item.span = Span::new(2000, 2001);
    }
    assert!(order.syntax_eq(&shifted));
    assert!(!order.syntax_eq(&folded));
}

#[test]
fn checked_build_and_public_validation_reject_each_empty_order_owner() {
    assert_eq!(
        build::order_by(Vec::new()).unwrap_err(),
        build::BuildError::EmptyOrderBy
    );
    let key = build::identifier(build::ident_at("key", true, Span::new(3, 8)));
    let item = build::sort_item(key.clone(), SortOrdering::Descending, NullOrdering::First);
    let built = build::order_by(vec![item]).unwrap();
    assert_eq!(built.span, SYNTHETIC);
    assert_eq!(built.items[0].sort_key, key);
    assert_eq!(
        (built.items[0].ordering, built.items[0].null_ordering),
        (SortOrdering::Descending, NullOrdering::First)
    );

    for owner in 0..3 {
        let mut query = query("SELECT f(v) OVER (ORDER BY k)");
        let SetExpr::Select(select) = query.body.as_mut() else {
            panic!("select");
        };
        let empty = OrderBy {
            items: AstVec::new(),
            span: Span::new(41, 49),
        };
        if owner == 0 {
            select.modifiers.order_by = Some(empty);
        } else {
            let SelectItem::SingleColumn(column) = &mut select.projection[0] else {
                panic!("unaliased function call");
            };
            let Expr::FunctionCall(call) = &mut column.expression else {
                panic!("unaliased function call");
            };
            if owner == 1 {
                call.order_by = Some(empty);
            } else {
                let Window::Specification(spec) = call.window.as_mut().unwrap() else {
                    panic!("specification");
                };
                spec.order_by = Some(empty);
            }
        }
        assert_eq!(
            validate_statement(&Statement::Query(query)),
            Err(ValidateError::InvalidStructure {
                violation: StructuralViolation::EmptyOrderBy,
                span: Span::new(41, 49)
            })
        );
    }
}
