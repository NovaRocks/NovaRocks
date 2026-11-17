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

fn query(source: &str) -> Query {
    let mut statements = novarocks_parser::parse(source).unwrap();
    let Statement::Query(query) = statements.remove(0) else {
        panic!("query");
    };
    query
}
fn select(query: &Query) -> &Select {
    let SetExpr::Select(select) = query.body.as_ref() else {
        panic!("specification");
    };
    select
}
fn clause(source: &str) -> GroupBy {
    select(&query(source)).group_by.clone().unwrap()
}
fn statement(group_by: GroupBy) -> Statement {
    Statement::Query(build::query(SetExpr::Select(Box::new(
        build::select(vec![build::wildcard()], Vec::new(), None, Some(group_by)).unwrap(),
    ))))
}

#[test]
fn old_group_by_producers_preserve_ordered_expression_sources_and_actual_set_boundaries() {
    assert!(select(&query("SELECT 1")).group_by.is_none());
    let source = "SELECT 1 GROUP BY a, `b`, f(c)";
    let group = clause(source);
    assert!(!group.distinct);
    assert_eq!(
        group.span,
        Span::new(source.find("a,").unwrap(), source.len())
    );
    assert_eq!(group.elements.len(), 3);
    for (element, expected) in group.elements.iter().zip(["a", "`b`", "f(c)"]) {
        let GroupingElement::Simple(simple) = element else {
            panic!("one real Simple per item");
        };
        assert_eq!(simple.columns.len(), 1);
        assert_eq!(simple.span, simple.columns[0].span());
        assert_eq!(&source[simple.span.start()..simple.span.end()], expected);
    }
    for (source, kind) in [
        ("SELECT 1 GROUP BY ROLLUP(a,b)", GroupingSetsKind::Rollup),
        ("SELECT 1 GROUP BY CUBE(a,b)", GroupingSetsKind::Cube),
    ] {
        let parsed = query(source);
        let group = select(&parsed).group_by.as_ref().unwrap();
        let GroupingElement::Sets(sets) = &group.elements[0] else {
            panic!("sets");
        };
        assert_eq!(sets.kind, kind);
        assert_eq!(sets.sets.len(), 2);
        for (set, name) in sets.sets.iter().zip(["a", "b"]) {
            assert_eq!(set.len(), 1);
            assert!(matches!(&set[0], Expr::Identifier(id) if id.value == name));
            assert_eq!(&source[set[0].span().start()..set[0].span().end()], name);
        }
        assert!(parsed.syntax_eq(&query(&printer::print_query(&parsed))));
    }
    for source in ["SELECT 1 GROUP BY ROLLUP()", "SELECT 1 GROUP BY CUBE()"] {
        let parsed = query(source);
        let group = select(&parsed).group_by.as_ref().unwrap();
        let [GroupingElement::Sets(sets)] = group.elements.as_slice() else {
            panic!("one real canonical empty grouping set");
        };
        assert_eq!(sets.kind, GroupingSetsKind::Explicit);
        assert_eq!(sets.sets.len(), 1);
        assert!(sets.sets[0].is_empty());
        assert_eq!(
            &source[sets.span.start()..sets.span.end()],
            source.split("GROUP BY ").nth(1).unwrap()
        );
        assert!(parsed.syntax_eq(&query("SELECT 1 GROUP BY GROUPING SETS (())")));
        assert!(parsed.syntax_eq(&query(&printer::print_query(&parsed))));
    }
    let source = "SELECT 1 GROUP BY GROUPING SETS ((a,b),(),(c))";
    let group = clause(source);
    let GroupingElement::Sets(sets) = &group.elements[0] else {
        panic!("explicit sets");
    };
    assert_eq!(sets.kind, GroupingSetsKind::Explicit);
    assert_eq!(
        sets.sets.iter().map(|set| set.len()).collect::<Vec<_>>(),
        [2, 0, 1]
    );
    assert_eq!(
        &source[sets.span.start()..sets.span.end()],
        "GROUPING SETS ((a,b),(),(c))"
    );
    // A parenthesized old expression remains its actual Row subtree. P05's
    // groupingSet grammar must not be inferred from an already parsed Row.
    let group = clause("SELECT 1 GROUP BY (a,b)");
    let GroupingElement::Simple(simple) = &group.elements[0] else {
        panic!("simple");
    };
    assert_eq!(simple.columns.len(), 1);
    assert!(matches!(&simple.columns[0], Expr::Row(row) if row.fields.len() == 2));
}

#[test]
fn mixed_typed_grouping_facts_keep_quotes_nested_set_order_distinct_hooks_and_span_free_equality() {
    let span = Span::new(10, 30);
    let group = GroupBy {
        distinct: true,
        elements: vec![
            GroupingElement::Simple(SimpleGroupBy {
                columns: vec![
                    build::identifier(build::ident("quoted", true)),
                    build::integer(1),
                ]
                .into(),
                span,
            }),
            GroupingElement::Sets(GroupingSets {
                kind: GroupingSetsKind::Rollup,
                sets: vec![
                    vec![build::integer(2), build::integer(3)].into(),
                    AstVec::new(),
                ]
                .into(),
                span,
            }),
            GroupingElement::Sets(GroupingSets {
                kind: GroupingSetsKind::Cube,
                sets: vec![vec![build::integer(4)].into()].into(),
                span,
            }),
            GroupingElement::Sets(GroupingSets {
                kind: GroupingSetsKind::Explicit,
                sets: vec![AstVec::new(), vec![build::integer(5)].into()].into(),
                span,
            }),
            GroupingElement::Auto(AutoGroupBy { span }),
        ]
        .into(),
        span,
    };
    #[derive(Default)]
    struct Facts {
        numbers: Vec<i64>,
        names: Vec<(String, bool)>,
        simple: usize,
        kinds: Vec<GroupingSetsKind>,
        auto: usize,
    }
    impl Visit for Facts {
        fn visit_literal(&mut self, node: &Literal) {
            if let LiteralKind::Long { parsed_value, .. } = node.kind {
                self.numbers.push(parsed_value);
            }
        }
        fn visit_ident(&mut self, node: &Ident) {
            self.names.push((node.value.clone(), node.quoted));
        }
        fn visit_simple_group_by(&mut self, node: &SimpleGroupBy) {
            self.simple += 1;
            walk_simple_group_by(self, node);
        }
        fn visit_grouping_sets(&mut self, node: &GroupingSets) {
            self.kinds.push(node.kind);
            walk_grouping_sets(self, node);
        }
        fn visit_auto_group_by(&mut self, _node: &AutoGroupBy) {
            self.auto += 1;
        }
    }
    let mut facts = Facts::default();
    facts.visit_group_by(&group);
    assert_eq!(facts.numbers, [1, 2, 3, 4, 5]);
    assert_eq!(facts.names, [("quoted".into(), true)]);
    assert_eq!(facts.simple, 1);
    assert_eq!(
        facts.kinds,
        [
            GroupingSetsKind::Rollup,
            GroupingSetsKind::Cube,
            GroupingSetsKind::Explicit
        ]
    );
    assert_eq!(facts.auto, 1);
    struct Change;
    impl Fold for Change {
        fn fold_group_by(&mut self, mut node: GroupBy) -> GroupBy {
            node.span = Span::new(110, 130);
            fold_group_by(self, node)
        }
        fn fold_grouping_element(&mut self, node: GroupingElement) -> GroupingElement {
            fold_grouping_element(self, node)
        }
        fn fold_literal(&mut self, node: Literal) -> Literal {
            let LiteralKind::Long { parsed_value, .. } = node.kind else {
                panic!("integer");
            };
            let Expr::Literal(node) = build::integer(parsed_value + 10) else {
                unreachable!()
            };
            node
        }
        fn fold_ident(&mut self, mut node: Ident) -> Ident {
            node.value.push_str("_changed");
            node
        }
    }
    let changed = Change.fold_group_by(group.clone());
    let mut facts = Facts::default();
    facts.visit_group_by(&changed);
    assert_eq!(facts.numbers, [11, 12, 13, 14, 15]);
    assert_eq!(facts.names, [("quoted_changed".into(), true)]);
    assert!(!group.syntax_eq(&changed));
    let mut copy = group.clone();
    copy.span = Span::new(110, 130);
    for element in &mut copy.elements {
        match element {
            GroupingElement::Simple(n) => n.span = Span::new(140, 150),
            GroupingElement::Sets(n) => n.span = Span::new(140, 150),
            GroupingElement::Auto(n) => n.span = Span::new(140, 150),
        }
    }
    assert_ne!(group, copy);
    assert!(group.syntax_eq(&copy));
    copy.distinct = false;
    assert!(!group.syntax_eq(&copy));
    copy = group.clone();
    let GroupingElement::Sets(sets) = &mut copy.elements[1] else {
        unreachable!()
    };
    sets.kind = GroupingSetsKind::Cube;
    assert!(!group.syntax_eq(&copy));
    copy = group.clone();
    let GroupingElement::Sets(sets) = &mut copy.elements[1] else {
        unreachable!()
    };
    sets.sets.swap(0, 1);
    assert!(!group.syntax_eq(&copy));
    let statement = statement(group);
    validate_statement(&statement).unwrap();
    assert_eq!(
        printer::print_statement(&statement),
        "SELECT * GROUP BY DISTINCT (`quoted`, 1), ROLLUP ((2, 3), ()), CUBE (4), GROUPING SETS ((), (5)), AUTO"
    );
}

#[test]
fn public_grouping_construction_rejects_only_empty_outer_sets_and_keeps_empty_simple_and_member_sets()
 {
    assert!(build::group_by(Vec::new()).is_none());
    let group = build::group_by(vec![build::integer(1), build::integer(2)]).unwrap();
    assert_eq!(group.elements.len(), 2);
    for kind in [
        GroupingSetsKind::Explicit,
        GroupingSetsKind::Rollup,
        GroupingSetsKind::Cube,
    ] {
        assert!(matches!(
            build::grouping_sets(kind, Vec::new()),
            Err(build::BuildError::EmptyGroupingSets)
        ));
        let span = Span::new(31, 42);
        let invalid = statement(GroupBy {
            distinct: false,
            elements: vec![GroupingElement::Sets(GroupingSets {
                kind,
                sets: AstVec::new(),
                span,
            })]
            .into(),
            span,
        });
        assert_eq!(
            validate_statement(&invalid),
            Err(ValidateError::InvalidStructure {
                violation: StructuralViolation::EmptyGroupingSets,
                span
            })
        );
        let valid = build::grouping_sets(kind, vec![Vec::new()]).unwrap();
        validate_statement(&statement(GroupBy {
            distinct: false,
            elements: vec![GroupingElement::Sets(valid)].into(),
            span,
        }))
        .unwrap();
    }
    validate_statement(&statement(GroupBy {
        distinct: false,
        elements: AstVec::new(),
        span: Span::new(0, 0),
    }))
    .unwrap();
    validate_statement(&statement(GroupBy {
        distinct: false,
        elements: vec![GroupingElement::Simple(SimpleGroupBy {
            columns: AstVec::new(),
            span: Span::new(0, 0),
        })]
        .into(),
        span: Span::new(0, 0),
    }))
    .unwrap();
    // Old ROLLUP()/CUBE() source behavior is an unresolved Root migration
    // obligation, not evidence authorizing a new parse/validation rejection.
}
