// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file to you under the
// Apache License, Version 2.0 (the "License"); you may not use this file
// except in compliance with the License.  You may obtain a copy of the License
// at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the
// License for the specific language governing permissions and limitations
// under the License.

//! Final row-count and set-operation public structural contracts.

use novarocks_parser::{Span, StructuralViolation, ValidateError, ast::*, build, printer};
fn query(source: &str) -> Query {
    let mut parsed = novarocks_parser::parse(source).unwrap();
    let Statement::Query(query) = parsed.remove(0) else {
        panic!("query");
    };
    query
}
fn modifiers(query: &Query) -> &QueryModifiers {
    match query.body.as_ref() {
        SetExpr::Select(select) => &select.modifiers,
        _ => &query.modifiers,
    }
}
fn statement(modifiers: QueryModifiers) -> Statement {
    let mut query = query("SELECT 1");
    let SetExpr::Select(select) = query.body.as_mut() else {
        unreachable!()
    };
    select.modifiers = modifiers;
    Statement::Query(query)
}
fn count(value: i64) -> Expr {
    build::integer(value)
}

#[test]
fn old_count_producers_move_real_comma_counts_and_keep_suffix_owners_and_keyword_spans() {
    let source = "SELECT 11 ORDER BY 1 LIMIT 2, 5";
    let parsed = query(source);
    assert!(parsed.modifiers.is_empty());
    let suffix = modifiers(&parsed);
    let offset = suffix.offset.as_ref().unwrap();
    let Some(QueryLimit::Limit(limit)) = &suffix.limit else {
        panic!("LIMIT");
    };
    assert_eq!(
        &source[offset.row_count.span().start()..offset.row_count.span().end()],
        "2"
    );
    assert_eq!(
        &source[limit.row_count.span().start()..limit.row_count.span().end()],
        "5"
    );
    assert_eq!(&source[limit.span.start()..limit.span.end()], "LIMIT 2, 5");
    assert_eq!(offset.span, offset.row_count.span());
    assert_eq!(
        printer::print_query(&parsed),
        "SELECT 11 ORDER BY 1 ASC OFFSET 2 ROWS LIMIT 5"
    );
    assert!(parsed.syntax_eq(&query(&printer::print_query(&parsed))));
    let source = "(SELECT 1 LIMIT 3) OFFSET 2 ROWS FETCH FIRST ROW ONLY";
    let parsed = query(source);
    let SetExpr::Query(inner) = parsed.body.as_ref() else {
        panic!("original parentheses");
    };
    assert!(matches!(
        &modifiers(inner).limit,
        Some(QueryLimit::Limit(_))
    ));
    let offset = parsed.modifiers.offset.as_ref().unwrap();
    assert_eq!(
        &source[offset.span.start()..offset.span.end()],
        "OFFSET 2 ROWS"
    );
    let Some(QueryLimit::FetchFirst(fetch)) = &parsed.modifiers.limit else {
        panic!("FETCH");
    };
    assert!(fetch.row_count.is_none());
    assert!(!fetch.with_ties);
    assert_eq!(
        &source[fetch.span.start()..fetch.span.end()],
        "FETCH FIRST ROW ONLY"
    );
    assert!(parsed.syntax_eq(&query(&printer::print_query(&parsed))));
    let source = "SELECT 1 LIMIT ALL";
    let parsed = query(source);
    let Some(QueryLimit::Limit(limit)) = &modifiers(&parsed).limit else {
        unreachable!()
    };
    let Expr::AllRows(all) = &limit.row_count else {
        panic!("explicit unbounded count");
    };
    assert_eq!(&source[all.span.start()..all.span.end()], "ALL");
    assert_ne!(modifiers(&parsed), modifiers(&query("SELECT 1")));
}

#[test]
fn count_constructor_constraints_refuse_real_wrong_shapes_and_do_not_erase_old_illegal_clauses() {
    for source in [
        "SELECT 1 LIMIT 3 OFFSET 2 FETCH FIRST 1 ROW ONLY",
        "SELECT a, count(*) AS n FROM db.t AS t WHERE a >= 1 GROUP BY a HAVING count(*) > 0 ORDER BY a DESC LIMIT 5 OFFSET 2 ROWS FETCH FIRST 1 ROW ONLY",
        "SELECT 1 FETCH FIRST 1 ROW ONLY LIMIT 3",
        "SELECT 1 LIMIT 1.5",
        "SELECT 1 LIMIT 1 + 2",
        "SELECT 1 OFFSET ALL",
        "SELECT 1 FETCH FIRST 2 PERCENT ROWS ONLY",
    ] {
        assert!(
            matches!(
                novarocks_parser::parse(source),
                Err(novarocks_parser::ParserError::Parse(_))
            ),
            "Parse refusal: {source}"
        );
    }
    let span = Span::new(21, 27);
    let invalid = Expr::Identifier(build::ident_at("count", false, span));
    for (modifiers, violation) in [
        (
            QueryModifiers {
                limit: Some(QueryLimit::Limit(Limit {
                    row_count: invalid.clone(),
                    span,
                })),
                ..Default::default()
            },
            StructuralViolation::InvalidLimitRowCount,
        ),
        (
            QueryModifiers {
                offset: Some(Offset {
                    row_count: Expr::AllRows(AllRows { span }),
                    span,
                }),
                ..Default::default()
            },
            StructuralViolation::InvalidOffsetRowCount,
        ),
        (
            QueryModifiers {
                limit: Some(QueryLimit::FetchFirst(FetchFirst {
                    row_count: Some(invalid),
                    with_ties: false,
                    span,
                })),
                ..Default::default()
            },
            StructuralViolation::InvalidFetchFirstRowCount,
        ),
    ] {
        assert_eq!(
            novarocks_parser::ast::validate_statement(&statement(modifiers)),
            Err(ValidateError::InvalidStructure { violation, span })
        );
    }
    // Pinned constructors constrain the class; positivity is a later contract.
    novarocks_parser::ast::validate_statement(&statement(QueryModifiers {
        limit: Some(QueryLimit::Limit(Limit {
            row_count: count(-1),
            span,
        })),
        ..Default::default()
    }))
    .unwrap();
}

#[test]
fn parameter_and_all_rows_keep_leaf_identity_and_fetch_none_stays_distinct_from_one() {
    let parameter = Expr::Parameter(Parameter {
        position: 7,
        span: Span::new(3, 4),
    });
    let relocated = Expr::Parameter(Parameter {
        position: 7,
        span: Span::new(30, 31),
    });
    assert!(parameter.syntax_eq(&relocated));
    assert!(!parameter.syntax_eq(&Expr::Parameter(Parameter {
        position: 8,
        span: Span::new(3, 4)
    })));
    assert_eq!(printer::print_expr(&parameter), "?");
    assert!(
        Expr::AllRows(AllRows {
            span: Span::new(3, 6)
        })
        .syntax_eq(&Expr::AllRows(AllRows {
            span: Span::new(30, 33)
        }))
    );
    let absent = QueryLimit::FetchFirst(FetchFirst {
        row_count: None,
        with_ties: true,
        span: Span::new(1, 8),
    });
    let one = QueryLimit::FetchFirst(FetchFirst {
        row_count: Some(count(1)),
        with_ties: true,
        span: Span::new(1, 8),
    });
    assert!(!absent.syntax_eq(&one));
    let mut all = QueryModifiers {
        offset: Some(Offset {
            row_count: parameter,
            span: Span::new(0, 4),
        }),
        limit: Some(absent),
        ..Default::default()
    };
    let mut stmt = statement(all.clone());
    novarocks_parser::ast::validate_statement(&stmt).unwrap();
    struct Parameters(Vec<usize>);
    impl Visit for Parameters {
        fn visit_parameter(&mut self, node: &Parameter) {
            self.0.push(node.position);
        }
    }
    let mut seen = Parameters(Vec::new());
    seen.visit_statement(&stmt);
    assert_eq!(seen.0, [7]);
    struct Shift;
    impl Fold for Shift {
        fn fold_parameter(&mut self, mut node: Parameter) -> Parameter {
            node.position += 2;
            node
        }
    }
    stmt = Shift.fold_statement(stmt);
    let mut seen = Parameters(Vec::new());
    seen.visit_statement(&stmt);
    assert_eq!(seen.0, [9]);
    assert!(printer::print_statement(&stmt).contains("OFFSET ? ROWS FETCH FIRST ROWS WITH TIES"));
    all.limit = Some(QueryLimit::Limit(Limit {
        row_count: Expr::AllRows(AllRows {
            span: Span::new(9, 12),
        }),
        span: Span::new(5, 12),
    }));
    novarocks_parser::ast::validate_statement(&statement(all)).unwrap();
}

fn values(value: i64) -> SetExpr {
    SetExpr::Values(Values {
        rows: vec![vec![count(value)]],
        explicit_row: false,
        span: Span::new(1, 2),
    })
}
#[test]
fn set_arity_distinct_corresponding_and_union_all_flattening_are_independent_real_facts() {
    for (operator, distinct, corresponding, valid) in [
        (SetOperator::Union, false, None, true),
        (SetOperator::Union, true, None, false),
        (SetOperator::Intersect, false, None, false),
        (SetOperator::Except, false, None, false),
        (
            SetOperator::Union,
            false,
            Some(Corresponding {
                columns: Vec::new(),
                span: Span::new(7, 8),
            }),
            false,
        ),
    ] {
        let span = Span::new(3, 13);
        let body = SetExpr::SetOperation(SetOperation {
            inputs: vec![values(1), values(2), values(3)].into(),
            operator,
            distinct,
            corresponding,
            span,
        });
        let result =
            novarocks_parser::ast::validate_statement(&Statement::Query(build::query(body)));
        if valid {
            result.unwrap();
        } else {
            assert_eq!(
                result,
                Err(ValidateError::InvalidStructure {
                    violation: StructuralViolation::InvalidSetOperationOperandCount,
                    span
                })
            );
        }
    }
    let all = build::union_all(vec![values(1), values(2), values(3)]).unwrap();
    let SetExpr::SetOperation(operation) = &all else {
        panic!("flat source inputs");
    };
    assert_eq!(operation.inputs.len(), 3);
    assert!(!operation.distinct);
    let mut changed = all.clone();
    let SetExpr::SetOperation(operation) = &mut changed else {
        unreachable!()
    };
    operation.corresponding = Some(Corresponding {
        columns: Vec::new(),
        span: Span::new(7, 8),
    });
    assert!(!all.syntax_eq(&changed));
    let nested = SetExpr::Query(Box::new(build::query(all)));
    let composed = build::set_operation(nested, SetOperator::Union, false, values(4), None);
    let SetExpr::SetOperation(operation) = composed else {
        unreachable!()
    };
    assert_eq!(operation.inputs.len(), 2);
    assert!(matches!(&operation.inputs[0], SetExpr::Query(_)));
}

#[test]
fn formerly_combined_fixture_counts_have_independent_complete_limit_and_fetch_contracts() {
    let limit = QueryLimit::Limit(Limit {
        row_count: count(10),
        span: Span::new(4, 12),
    });
    let fetch = QueryLimit::FetchFirst(FetchFirst {
        row_count: Some(count(3)),
        with_ties: false,
        span: Span::new(4, 24),
    });
    for (node, expected) in [
        (limit, "SELECT 1 OFFSET 2 ROWS LIMIT 10"),
        (fetch, "SELECT 1 OFFSET 2 ROWS FETCH FIRST 3 ROWS ONLY"),
    ] {
        let stmt = statement(QueryModifiers {
            order_by: None,
            offset: Some(Offset {
                row_count: count(2),
                span: Span::new(1, 4),
            }),
            limit: Some(node),
        });
        novarocks_parser::ast::validate_statement(&stmt).unwrap();
        assert_eq!(printer::print_statement(&stmt), expected);
        let Statement::Query(typed) = stmt else {
            unreachable!()
        };
        assert!(typed.syntax_eq(&query(expected)));
    }
}
