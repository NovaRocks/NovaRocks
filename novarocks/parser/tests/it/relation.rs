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
    novarocks_parser::parser::parse_query(source).unwrap()
}
fn statement(relation: Relation) -> TrinoStatement {
    TrinoStatement::Query(build::query(SetExpr::Select(AstBox::new(
        build::select(vec![build::wildcard()], Some(relation), None, None).unwrap(),
    ))))
}
fn table(name: &str) -> Relation {
    build::table(
        build::object_name(vec![build::ident(name, false)]).unwrap(),
        None,
        None,
    )
}

#[test]
fn complete_pattern_relation_reuses_ordering_and_pattern_authors_without_losing_children() {
    let span = Span::new(10, 30);
    let sampled = Relation::Sampled(AstBox::new(SampledRelation {
        relation: AstBox::new(table("source")),
        kind: SamplingType::System,
        sample_percentage: build::integer(10),
        span,
    }));
    let pivot = Relation::Pivot(AstBox::new(Pivot {
        input: AstBox::new(sampled),
        aggregations: vec![PivotAggregation {
            expression: build::integer(30),
            alias: Some(build::ident("aggregation", true)),
            span,
        }]
        .into(),
        pivot_columns: vec![build::integer(40)].into(),
        value_groups: vec![PivotValueGroup {
            values: vec![build::integer(50)].into(),
            alias: Some(build::ident("value", false)),
            span,
        }]
        .into(),
        group_by: None,
        span,
    }));
    let node = PatternRecognitionRelation {
        input: AstBox::new(pivot),
        partition_by: vec![build::integer(60)].into(),
        order_by: Some(
            build::order_by(vec![build::sort_item(
                build::integer(70),
                SortOrdering::Ascending,
                NullOrdering::Undefined,
            )])
            .unwrap(),
        ),
        measures: vec![MeasureDefinition {
            expression: build::integer(80),
            name: build::ident("measure", true),
            span,
        }]
        .into(),
        rows_per_match: None,
        after_match_skip_to: Some(SkipTo::First {
            identifier: build::ident("A", true),
            span,
        }),
        pattern_search_mode: Some(PatternSearchMode {
            mode: PatternSearchModeKind::Seek,
            span,
        }),
        pattern: RowPattern::Variable {
            name: build::ident("A", true),
            span,
        },
        subsets: vec![SubsetDefinition {
            name: build::ident("U", false),
            identifiers: vec![build::ident("A", true), build::ident("B", false)],
            span,
        }]
        .into(),
        variable_definitions: vec![VariableDefinition {
            name: build::ident("A", true),
            expression: build::integer(90),
            span,
        }]
        .into(),
        span,
    };
    #[derive(Default)]
    struct Seen {
        values: Vec<i64>,
        references: Vec<*const Expr>,
        mode: Vec<PatternSearchModeKind>,
    }
    impl Visit for Seen {
        fn visit_expr(&mut self, node: &Expr) {
            if let Expr::Literal(Literal {
                kind: LiteralKind::Long { parsed_value, .. },
                ..
            }) = node
            {
                self.values.push(*parsed_value);
                self.references.push(node as *const Expr);
            }
            walk_expr(self, node);
        }
        fn visit_pattern_search_mode(&mut self, node: &PatternSearchMode) {
            self.mode.push(node.mode);
        }
    }
    let mut original = Seen::default();
    original.visit_pattern_recognition_relation(&node);
    assert_eq!(original.values, [10, 30, 40, 50, 60, 70, 80, 90]);
    assert_eq!(original.mode, [PatternSearchModeKind::Seek]);
    assert_eq!(original.references[4], &node.partition_by[0] as *const Expr);
    assert_eq!(
        original.references[5],
        &node.order_by.as_ref().unwrap().items[0].sort_key as *const Expr
    );
    let mut copy = node.clone();
    copy.span = Span::new(110, 130);
    assert_ne!(node, copy);
    assert!(node.syntax_eq(&copy));
    copy.rows_per_match = Some(RowsPerMatch::One);
    assert!(!node.syntax_eq(&copy));
    copy = node.clone();
    copy.pattern_search_mode.as_mut().unwrap().mode = PatternSearchModeKind::Initial;
    assert!(!node.syntax_eq(&copy));
    struct Change;
    impl Fold for Change {
        fn fold_literal(&mut self, node: Literal) -> Literal {
            let LiteralKind::Long { parsed_value, .. } = node.kind else {
                panic!("integer")
            };
            let Expr::Literal(node) = build::integer(parsed_value + 1) else {
                unreachable!()
            };
            node
        }
    }
    let changed = Change.fold_pattern_recognition_relation(node.clone());
    let mut seen = Seen::default();
    seen.visit_pattern_recognition_relation(&changed);
    assert_eq!(seen.values, [11, 31, 41, 51, 61, 71, 81, 91]);
    assert_eq!(seen.mode, original.mode);
    let statement = statement(Relation::PatternRecognition(AstBox::new(node)));
    validate_statement(&statement).unwrap();
    assert!(matches!(
        printer::print_statement(&statement),
        Err(printer::PrintError::Unrepresentable {
            fact: "computed PIVOT column outside qualifiedName grammar",
        })
    ));
}

#[test]
fn public_relation_invariants_and_optional_cte_columns_preserve_real_constructor_contracts() {
    let span = novarocks_parser::SYNTHETIC;
    let invalid = statement(build::join(
        table("a"),
        JoinType::Cross,
        table("b"),
        Some(JoinCriteria::On(JoinOn {
            expression: build::boolean(true),
            span,
        })),
    ));
    assert!(matches!(
        validate_statement(&invalid),
        Err(ValidateError::InvalidStructure {
            violation: StructuralViolation::InvalidJoinCriteria,
            ..
        })
    ));
    let invalid = statement(Relation::Table(AstBox::new(Table {
        name: build::object_name(vec![build::ident("a", false)]).unwrap(),
        query_period: Some(QueryPeriod {
            range_type: QueryPeriodRangeType::Version,
            start: None,
            end: Some(AstBox::new(build::integer(1))),
            span,
        }),
        branch: Some(build::ident("b", false)),
        span,
    })));
    assert!(matches!(
        validate_statement(&invalid),
        Err(ValidateError::InvalidStructure {
            violation: StructuralViolation::ConflictingTablePeriodAndBranch,
            ..
        })
    ));
    let mut first = query("WITH c AS (SELECT 7) SELECT 1");
    let mut second = first.clone();
    second.with.as_mut().unwrap().ctes[0].columns = Some(Vec::new());
    assert!(!first.syntax_eq(&second));
    first.with.as_mut().unwrap().ctes[0].columns = Some(vec![build::ident("x", true)]);
    let mut renamed = first.clone();
    renamed.with.as_mut().unwrap().ctes[0]
        .columns
        .as_mut()
        .unwrap()[0]
        .quoted = false;
    assert!(!first.syntax_eq(&renamed));
    let table = Table {
        name: build::object_name(vec![build::ident("actual", false)]).unwrap(),
        query_period: None,
        branch: None,
        span,
    };
    let query = build::query(SetExpr::Table(AstBox::new(table)));
    validate_statement(&TrinoStatement::Query(query.clone())).unwrap();
    assert_eq!(printer::print_query(&query).unwrap(), "TABLE actual");
}
