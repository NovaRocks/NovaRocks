// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file distributed
// with this work for additional information regarding copyright ownership.
// The ASF licenses this file to You under the Apache License, Version 2.0
// (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use super::*;
use crate::compiler::{SqlCompileControl, build_builtin_engine_function_catalog};
use novarocks_functions::{ConstantPolicy, ConstantValue};
use novarocks_type_contract::{CompilePhase, FunctionValueType, ValueLogicalType};
use std::sync::Arc;

fn query(sql: &str) -> ast::Query {
    let query = &novarocks_parser::parser::parse_query(sql).unwrap();
    query.clone()
}
fn value(policy: ConstantPolicy, control: &SqlCompileControl) -> ConstantValue {
    let ty = FunctionValueType::try_with_logical_type(
        arrow::datatypes::DataType::LargeBinary,
        false,
        ValueLogicalType::Variant,
    )
    .unwrap();
    let bytes =
        novarocks_types::value::variant_encode::encode_json_text_to_variant_bytes("null").unwrap();
    ConstantValue::from_binary(
        Arc::new(arrow::datatypes::Field::new(
            "",
            ty.data_type().clone(),
            false,
        )),
        ty,
        &bytes,
        policy,
        CompilePhase::Validate,
        control,
    )
    .unwrap()
}

fn constants(value: ConstantValue) -> SqlExpressionConstants {
    vec![SqlExpressionConstant::new(
        Vec::new(),
        SqlConstantExpressionSite::ValuesCell { row: 0, column: 1 },
        value,
    )]
    .into()
}
#[test]
fn expression_loan_matches_only_the_actual_owned_occurrence() {
    let control = SqlCompileControl::unbounded();
    let value = value(crate::constant::test_constant_policy(), &control);
    let original = query("VALUES (1, NULL)");
    let source =
        MaterializedValues::try_new_expressions(&original, constants(value.clone()), &control)
            .unwrap();
    let owned = source.loan(&original, &control).unwrap();
    let loan = owned.view();
    let ast::SetExpr::Values(values) = original.body.as_ref() else {
        panic!("values")
    };
    assert!(
        loan.constant_at(ast::ValuesRowCells::new(&values.rows[0]).get(0).unwrap())
            .is_none()
    );
    assert!(
        loan.constant_at(ast::ValuesRowCells::new(&values.rows[0]).get(1).unwrap())
            .is_some()
    );
    let independent = query("VALUES (1, NULL)");
    let ast::SetExpr::Values(values) = independent.body.as_ref() else {
        panic!("values")
    };
    assert!(
        loan.constant_at(ast::ValuesRowCells::new(&values.rows[0]).get(1).unwrap())
            .is_none(),
        "equal syntax is not this analysis occurrence"
    );
    assert!(source.loan(&query("VALUES (1)"), &control).is_err());
    assert!(source.loan(&query("SELECT NULL"), &control).is_err());
    let repeated = vec![
        SqlExpressionConstant::new(
            Vec::new(),
            SqlConstantExpressionSite::ValuesCell { row: 0, column: 1 },
            value.clone(),
        ),
        SqlExpressionConstant::new(
            Vec::new(),
            SqlConstantExpressionSite::ValuesCell { row: 0, column: 1 },
            value,
        ),
    ]
    .into();
    assert!(MaterializedValues::try_new_expressions(&original, repeated, &control).is_err());
}
#[test]
fn actual_analyzer_preserves_encoded_json_null_and_original_sql_null() {
    let control = SqlCompileControl::unbounded();
    let policy = crate::constant::test_constant_policy();
    use arrow::array::{Array, StringArray};
    use arrow::datatypes::{DataType, Field};
    use novarocks_type_contract::TrinoCastMode;

    let value = ConstantValue::from_scalar_array(
        Arc::new(Field::new("json", DataType::Utf8, false)),
        FunctionValueType::try_with_logical_type(DataType::Utf8, false, ValueLogicalType::Json)
            .unwrap(),
        StringArray::from(vec!["null"]).to_data(),
        policy,
        CompilePhase::Validate,
        &control,
    )
    .unwrap();
    let identity = value.pool().backing_identity();
    let query = query("VALUES (1 + 2, NULL), (NULL, CAST(NULL AS JSON))");
    let source =
        MaterializedValues::try_new_expressions(&query, constants(value), &control).unwrap();
    let catalog = crate::planning::catalog::PlannerMemoryCatalog::default();
    let functions = build_builtin_engine_function_catalog().unwrap();
    let (resolved, _, _) = crate::analyzer::analyze_with_materialized_values_and_sql_semantics(
        &query,
        &catalog,
        "db",
        &functions,
        &crate::sql_mode::SqlSemanticSettings::default(),
        policy,
        &control,
        Some(&source),
    )
    .unwrap();
    let crate::analysis::QueryBody::Values(values) = resolved.body else {
        panic!("values")
    };
    let crate::analysis::ExprKind::Constant(value) = &values.rows[0][1].kind else {
        panic!("checked constant")
    };
    assert_eq!(value.pool().backing_identity(), identity);
    assert_eq!(value.try_utf8().unwrap(), Some("null"));
    assert_eq!(
        values.rows[0][1].value_type.logical_type(),
        ValueLogicalType::Json
    );
    assert_eq!(
        values.rows[1][1].value_type.logical_type(),
        ValueLogicalType::Json
    );
    assert!(matches!(
        &values.rows[1][1].kind,
        crate::analysis::ExprKind::Cast {
            expr,
            cast_mode: Some(TrinoCastMode::Explicit),
            ..
        } if matches!(expr.kind, crate::analysis::ExprKind::Literal(crate::analysis::LiteralValue::Null))
    ));
    assert!(matches!(
        crate::common::binary_operator::typed_operands(&values.rows[0][0]),
        Some((novarocks_type_contract::OperatorFunction::Add, _))
    ));
}

#[test]
fn logical_term_paths_borrow_only_the_authored_occurrence_and_reject_wrong_edges() {
    use arrow::array::{Array, BooleanArray};
    use novarocks_functions::ConstantPool;
    let control = SqlCompileControl::unbounded();
    let policy = crate::constant::test_constant_policy();
    let ty = FunctionValueType::new(arrow::datatypes::DataType::Boolean, false);
    let field = Arc::new(arrow::datatypes::Field::new(
        "original",
        ty.data_type().clone(),
        false,
    ));
    let pool = ConstantPool::try_new(
        field.clone(),
        ty.clone(),
        BooleanArray::from(vec![false, true]).to_data(),
        policy,
        CompilePhase::Validate,
        &control,
    )
    .unwrap();
    let value = pool.value(1).unwrap();
    let query = query("SELECT TRUE AND only_original_source AND NULL");
    let constant = |path| {
        SqlExpressionConstant::new(
            vec![],
            SqlConstantExpressionSite::SelectProjection { column: 0 },
            value.clone(),
        )
        .with_expression_path(path)
    };
    let source = MaterializedValues::try_new_expressions(
        &query,
        vec![constant(vec![SqlConstantExpressionStep::LogicalTerm(1)])].into(),
        &control,
    )
    .unwrap();
    let ast::SetExpr::Select(select) = query.body.as_ref() else {
        panic!("select")
    };
    let ast::SelectItem::SingleColumn(projection_column) = &select.projection[0] else {
        panic!("logical projection")
    };
    assert!(projection_column.alias.is_none(), "anonymous projection");
    let ast::Expr::Logical(logical) = &projection_column.expression else {
        panic!("logical projection")
    };
    assert_eq!(logical.terms.len(), 3);
    let owned = source.loan(&query, &control).unwrap();
    let loan = owned.view();
    assert!(loan.constant_at(&logical.terms[0]).is_none());
    assert!(loan.constant_at(&logical.terms[2]).is_none());
    assert!(loan.constant_at(&logical.terms[1].clone()).is_none());
    let retained = loan.constant_at(&logical.terms[1]).unwrap();
    assert_eq!(retained.pool().backing_identity(), pool.backing_identity());
    assert_eq!(retained.ordinal(), 1);
    assert_eq!(retained.field(), field.as_ref());
    assert_eq!(retained.value_type(), &ty);
    for path in [
        vec![SqlConstantExpressionStep::LogicalTerm(3)],
        vec![
            SqlConstantExpressionStep::LogicalTerm(1),
            SqlConstantExpressionStep::LogicalTerm(0),
        ],
    ] {
        assert!(
            MaterializedValues::try_new_expressions(&query, vec![constant(path)].into(), &control)
                .is_err()
        );
    }
    let catalog = crate::planning::catalog::PlannerMemoryCatalog::default();
    let functions = build_builtin_engine_function_catalog().unwrap();
    let (resolved, _, _) = crate::analyzer::analyze_with_materialized_values_and_sql_semantics(
        &query,
        &catalog,
        "db",
        &functions,
        &crate::sql_mode::SqlSemanticSettings::default(),
        policy,
        &control,
        Some(&source),
    )
    .unwrap();
    let crate::analysis::QueryBody::Select(select) = resolved.body else {
        panic!("resolved select")
    };
    let expression = &select.projection[0].expr;
    assert_eq!(
        expression.value_type.data_type(),
        &arrow::datatypes::DataType::Boolean
    );
    assert!(expression.value_type.nullable());
    let crate::analysis::ExprKind::BinaryOp {
        left, right, op, ..
    } = &expression.kind
    else {
        panic!("balanced root")
    };
    assert_eq!(*op, crate::analysis::BinOp::And);
    assert!(matches!(
        right.kind,
        crate::analysis::ExprKind::Literal(crate::analysis::LiteralValue::Null)
    ));
    let crate::analysis::ExprKind::BinaryOp { right, .. } = &left.kind else {
        panic!("first adjacent pair")
    };
    let crate::analysis::ExprKind::Constant(retained) = &right.kind else {
        panic!("original constant")
    };
    assert_eq!(retained.ordinal(), 1);
    assert_eq!(retained.pool().backing_identity(), pool.backing_identity());
}

#[test]
fn predicate_source_paths_keep_actual_children_and_refuse_wrong_fields_or_clones() {
    use SqlConstantExpressionStep as Edge;
    let control = SqlCompileControl::unbounded();
    let retained = value(crate::constant::test_constant_policy(), &control);
    type Case = (
        &'static str,
        Vec<Edge>,
        fn(&ast::PredicatedExpr) -> &ast::Expr,
    );
    let cases: Vec<Case> = vec![
        (
            "SELECT source_value = 1",
            vec![Edge::PredicatedValue],
            |p| &p.value,
        ),
        (
            "SELECT 1 = source_value",
            vec![Edge::ComparisonRight],
            |p| {
                let ast::Predicate::Comparison(p) = &p.predicate else {
                    panic!("comparison")
                };
                &p.right
            },
        ),
        (
            "SELECT 1  IS NOT DISTINCT FROM  source_value",
            vec![Edge::ComparisonRight],
            |p| {
                let ast::Predicate::DistinctFrom(p) = &p.predicate else {
                    panic!("null-safe equality")
                };
                &p.right
            },
        ),
        (
            "SELECT 1 BETWEEN source_value AND 3",
            vec![Edge::BetweenMin],
            |p| {
                let ast::Predicate::Between(p) = &p.predicate else {
                    panic!("between")
                };
                &p.min
            },
        ),
        (
            "SELECT 1 BETWEEN 0 AND source_value",
            vec![Edge::BetweenMax],
            |p| {
                let ast::Predicate::Between(p) = &p.predicate else {
                    panic!("between")
                };
                &p.max
            },
        ),
        (
            "SELECT 'a' LIKE source_value",
            vec![Edge::LikePattern],
            |p| {
                let ast::Predicate::Like(p) = &p.predicate else {
                    panic!("like")
                };
                &p.pattern
            },
        ),
        (
            "SELECT 1 IN (2,source_value)",
            vec![Edge::InValueList, Edge::InListValue(1)],
            |p| {
                let ast::Predicate::In(p) = &p.predicate else {
                    panic!("in")
                };
                let ast::Expr::InList(list) = &p.value_list else {
                    panic!("list")
                };
                &list.values[1]
            },
        ),
    ];
    for (sql, path, selected) in cases {
        let query = query(sql);
        let ast::SetExpr::Select(select) = query.body.as_ref() else {
            panic!("select")
        };
        let ast::SelectItem::SingleColumn(projection_column) = &select.projection[0] else {
            panic!("predicated projection")
        };
        assert!(projection_column.alias.is_none(), "anonymous projection");
        let ast::Expr::Predicated(node) = &projection_column.expression else {
            panic!("predicated projection")
        };
        let source = MaterializedValues::try_new_expressions(
            &query,
            vec![
                SqlExpressionConstant::new(
                    vec![],
                    SqlConstantExpressionSite::SelectProjection { column: 0 },
                    retained.clone(),
                )
                .with_expression_path(path),
            ]
            .into(),
            &control,
        )
        .unwrap();
        let owned = source.loan(&query, &control).unwrap();
        let selected = selected(node);
        assert!(owned.view().constant_at(selected).is_some(), "{sql}");
        assert!(
            owned.view().constant_at(&selected.clone()).is_none(),
            "{sql}"
        );
        assert!(
            owned
                .view()
                .constant_at(
                    &select
                        .projection
                        .iter()
                        .find_map(|item| match item {
                            ast::SelectItem::SingleColumn(column) if column.alias.is_none() => {
                                Some(&column.expression)
                            }
                            _ => None,
                        })
                        .unwrap()
                        .clone()
                )
                .is_none()
        );
    }
    let query = query("SELECT 1 IN (2,3)");
    for path in [
        vec![Edge::ComparisonRight],
        vec![Edge::BetweenMin],
        vec![Edge::LikePattern],
        vec![Edge::InListValue(0)],
        vec![Edge::InValueList, Edge::InListValue(2)],
        vec![
            Edge::InValueList,
            Edge::InListValue(0),
            Edge::ComparisonRight,
        ],
    ] {
        assert!(
            MaterializedValues::try_new_expressions(
                &query,
                vec![
                    SqlExpressionConstant::new(
                        vec![],
                        SqlConstantExpressionSite::SelectProjection { column: 0 },
                        retained.clone()
                    )
                    .with_expression_path(path)
                ]
                .into(),
                &control
            )
            .is_err()
        );
    }
}

#[test]
fn actual_in_predicate_preserves_nonzero_constant_sources_on_both_sides() {
    use SqlConstantExpressionStep as Edge;
    use arrow::array::{Array, Int32Array};
    use novarocks_functions::ConstantPool;
    let control = SqlCompileControl::unbounded();
    let policy = crate::constant::test_constant_policy();
    let ty = FunctionValueType::new(arrow::datatypes::DataType::Int32, false);
    let field = Arc::new(arrow::datatypes::Field::new(
        "original_in_source",
        arrow::datatypes::DataType::Int32,
        false,
    ));
    let pool = ConstantPool::try_new(
        field.clone(),
        ty.clone(),
        Int32Array::from(vec![99, 2]).to_data(),
        policy,
        CompilePhase::Validate,
        &control,
    )
    .unwrap();
    let retained = pool.value(1).unwrap();
    let query = query("SELECT original_probe IN (1,original_candidate)");
    let constants = [
        vec![Edge::PredicatedValue],
        vec![Edge::InValueList, Edge::InListValue(1)],
    ]
    .into_iter()
    .map(|path| {
        SqlExpressionConstant::new(
            vec![],
            SqlConstantExpressionSite::SelectProjection { column: 0 },
            retained.clone(),
        )
        .with_expression_path(path)
    })
    .collect::<Vec<_>>()
    .into();
    let source = MaterializedValues::try_new_expressions(&query, constants, &control).unwrap();
    let (resolved, _, _) = crate::analyzer::analyze_with_materialized_values_and_sql_semantics(
        &query,
        &crate::planning::catalog::PlannerMemoryCatalog::default(),
        "db",
        &build_builtin_engine_function_catalog().unwrap(),
        &crate::sql_mode::SqlSemanticSettings::default(),
        policy,
        &control,
        Some(&source),
    )
    .unwrap();
    let crate::analysis::QueryBody::Select(select) = resolved.body else {
        panic!("resolved select")
    };
    assert_eq!(
        select.projection[0].expr.value_type,
        FunctionValueType::new(arrow::datatypes::DataType::Boolean, false)
    );
    let crate::analysis::ExprKind::InList {
        expr,
        list,
        negated,
    } = &select.projection[0].expr.kind
    else {
        panic!("original IN typed owner")
    };
    assert!(!negated);
    assert_eq!(list.len(), 2);
    assert!(matches!(
        list[0].kind,
        crate::analysis::ExprKind::Literal(crate::analysis::LiteralValue::Int(1))
    ));
    for original in [expr.as_ref(), &list[1]] {
        assert_eq!(original.value_type, ty);
        let crate::analysis::ExprKind::Constant(value) = &original.kind else {
            panic!("original source constant")
        };
        assert_eq!(value.ordinal(), 1);
        assert_eq!(value.pool().backing_identity(), pool.backing_identity());
        assert_eq!(value.field(), field.as_ref());
        assert_eq!(value.value_type(), &ty);
    }
}

#[test]
fn control_expression_edges_resolve_only_real_fields_and_reject_partial_or_absent_fields() {
    use SqlConstantExpressionStep as Edge;
    let control = SqlCompileControl::unbounded();
    let retained = value(crate::constant::test_constant_policy(), &control);
    type Case = (&'static str, Vec<Edge>, fn(&ast::Expr) -> &ast::Expr);
    let cases: Vec<Case> = vec![
        (
            "SELECT CAST(source AS INT)",
            vec![Edge::CastExpression],
            |e| {
                let ast::Expr::Cast(c) = e else {
                    panic!("CAST")
                };
                &c.expression
            },
        ),
        (
            "SELECT CASE source WHEN 1 THEN 2 END",
            vec![Edge::CaseOperand],
            |e| {
                let ast::Expr::SimpleCase(c) = e else {
                    panic!("simple CASE")
                };
                &c.operand
            },
        ),
        (
            "SELECT CASE WHEN source THEN 2 END",
            vec![Edge::CaseWhenOperand(0)],
            |e| {
                let ast::Expr::SearchedCase(c) = e else {
                    panic!("searched CASE")
                };
                let ast::WhenMatch::Operand(v) = &c.when_clauses[0].match_value else {
                    panic!("WHEN expression")
                };
                v
            },
        ),
        (
            "SELECT CASE 1 WHEN 2 THEN source END",
            vec![Edge::CaseResult(0)],
            |e| {
                let ast::Expr::SimpleCase(c) = e else {
                    panic!("simple CASE")
                };
                &c.when_clauses[0].result
            },
        ),
        (
            "SELECT CASE WHEN TRUE THEN 2 ELSE source END",
            vec![Edge::CaseDefault],
            |e| {
                let ast::Expr::SearchedCase(c) = e else {
                    panic!("searched CASE")
                };
                c.default_value.as_ref().unwrap()
            },
        ),
        ("SELECT x -> source", vec![Edge::LambdaBody], |e| {
            let ast::Expr::Lambda(c) = e else {
                panic!("lambda")
            };
            &c.body
        }),
        (
            "SELECT EXISTS (SELECT 1)",
            vec![Edge::ExistsSubquery],
            |e| {
                let ast::Expr::Exists(c) = e else {
                    panic!("EXISTS")
                };
                &c.subquery
            },
        ),
    ];
    for (sql, path, selected) in cases {
        let query = query(sql);
        let ast::SetExpr::Select(select) = query.body.as_ref() else {
            panic!("select")
        };
        let ast::SelectItem::SingleColumn(projection_column) = &select.projection[0] else {
            panic!("projection")
        };
        assert!(projection_column.alias.is_none(), "anonymous projection");
        let expression = &projection_column.expression;
        let source = MaterializedValues::try_new_expressions(
            &query,
            vec![
                SqlExpressionConstant::new(
                    vec![],
                    SqlConstantExpressionSite::SelectProjection { column: 0 },
                    retained.clone(),
                )
                .with_expression_path(path),
            ]
            .into(),
            &control,
        )
        .unwrap();
        let owned = source.loan(&query, &control).unwrap();
        assert!(
            owned.view().constant_at(selected(expression)).is_some(),
            "{sql}"
        );
        assert!(
            owned
                .view()
                .constant_at(&selected(expression).clone())
                .is_none(),
            "{sql}"
        );
    }
    let query = query("SELECT CASE WHEN TRUE THEN 2 END");
    for path in [
        vec![Edge::CaseOperand],
        vec![Edge::CastExpression],
        vec![Edge::LambdaBody],
        vec![Edge::ExistsSubquery],
        vec![Edge::CaseWhenOperand(1)],
        vec![Edge::CaseResult(1)],
        vec![Edge::CaseDefault],
        vec![Edge::ComparisonRight],
    ] {
        assert!(
            MaterializedValues::try_new_expressions(
                &query,
                vec![
                    SqlExpressionConstant::new(
                        vec![],
                        SqlConstantExpressionSite::SelectProjection { column: 0 },
                        retained.clone()
                    )
                    .with_expression_path(path)
                ]
                .into(),
                &control
            )
            .is_err()
        );
    }
    let mut partial = query.clone();
    let ast::SetExpr::Select(select) = partial.body.as_mut() else {
        panic!("select")
    };
    let ast::SelectItem::SingleColumn(projection_column) = &mut select.projection[0] else {
        panic!("CASE")
    };
    assert!(projection_column.alias.is_none(), "anonymous projection");
    let ast::Expr::SearchedCase(case) = &mut projection_column.expression else {
        panic!("CASE")
    };
    case.when_clauses[0].match_value =
        ast::WhenMatch::Partial(ast::Predicate::IsNull(ast::IsNullPredicate {
            negated: false,
            span: novarocks_parser::Span::new(0, 1),
        }));
    assert!(
        MaterializedValues::try_new_expressions(
            &partial,
            vec![
                SqlExpressionConstant::new(
                    vec![],
                    SqlConstantExpressionSite::SelectProjection { column: 0 },
                    retained
                )
                .with_expression_path(vec![Edge::CaseWhenOperand(0)])
            ]
            .into(),
            &control
        )
        .is_err()
    );
}

#[test]
fn dereference_borrows_its_original_interior_constant_before_name_resolution() {
    use arrow::array::{Array, Int32Array, StructArray};
    use arrow::datatypes::{DataType, Field, Fields};
    use novarocks_functions::ConstantPool;
    let control = SqlCompileControl::unbounded();
    let policy = crate::constant::test_constant_policy();
    let fields = Fields::from(vec![Arc::new(Field::new(
        "payload",
        DataType::Int32,
        false,
    ))]);
    let ty = FunctionValueType::new(DataType::Struct(fields.clone()), false);
    let field = Arc::new(
        Field::new("original_struct_source", ty.data_type().clone(), false).with_metadata(
            std::collections::HashMap::from([(
                "provider.unknown".to_string(),
                "full-field".to_string(),
            )]),
        ),
    );
    let array =
        StructArray::try_new(fields, vec![Arc::new(Int32Array::from(vec![11, 23]))], None).unwrap();
    let pool = ConstantPool::try_new(
        field.clone(),
        ty.clone(),
        array.to_data(),
        policy,
        CompilePhase::Validate,
        &control,
    )
    .unwrap();
    let retained = pool.value(1).unwrap();
    let query = query("SELECT only_original_source.payload");
    let constant = |path| {
        SqlExpressionConstant::new(
            Vec::new(),
            SqlConstantExpressionSite::SelectProjection { column: 0 },
            retained.clone(),
        )
        .with_expression_path(path)
    };
    let source = MaterializedValues::try_new_expressions(
        &query,
        vec![constant(vec![SqlConstantExpressionStep::DereferenceBase])].into(),
        &control,
    )
    .unwrap();
    let ast::SetExpr::Select(select) = query.body.as_ref() else {
        panic!("select");
    };
    let ast::SelectItem::SingleColumn(projection_column) = &select.projection[0] else {
        panic!("dereference");
    };
    assert!(projection_column.alias.is_none(), "anonymous projection");
    let ast::Expr::Dereference(node) = &projection_column.expression else {
        panic!("dereference");
    };
    let owned = source.loan(&query, &control).unwrap();
    let loan = owned.view();
    let original = loan.constant_at(&node.base).unwrap();
    assert_eq!(original.ordinal(), 1);
    assert_eq!(original.pool().backing_identity(), pool.backing_identity());
    assert_eq!(original.field(), field.as_ref());
    assert!(Arc::ptr_eq(original.pool().field_ref(), &field));
    assert_eq!(
        original
            .field()
            .metadata()
            .get("provider.unknown")
            .map(String::as_str),
        Some("full-field")
    );
    assert_eq!(original.value_type(), &ty);
    assert!(loan.constant_at(&node.base.clone()).is_none());
    for path in [
        vec![SqlConstantExpressionStep::SubscriptBase],
        vec![SqlConstantExpressionStep::SubscriptIndex],
        vec![
            SqlConstantExpressionStep::DereferenceBase,
            SqlConstantExpressionStep::DereferenceBase,
        ],
    ] {
        assert!(
            MaterializedValues::try_new_expressions(&query, vec![constant(path)].into(), &control,)
                .is_err()
        );
    }
    let catalog = crate::planning::catalog::PlannerMemoryCatalog::default();
    let functions = build_builtin_engine_function_catalog().unwrap();
    let (resolved, _, _) = crate::analyzer::analyze_with_materialized_values_and_sql_semantics(
        &query,
        &catalog,
        "db",
        &functions,
        &crate::sql_mode::SqlSemanticSettings::default(),
        policy,
        &control,
        Some(&source),
    )
    .unwrap();
    let crate::analysis::QueryBody::Select(select) = resolved.body else {
        panic!("select");
    };
    let expression = &select.projection[0].expr;
    let crate::analysis::ExprKind::FunctionCall { name, args, .. } = &expression.kind else {
        panic!("original struct subfield binding");
    };
    assert_eq!(name, "__struct_subfield");
    assert_eq!(expression.value_type.data_type(), &DataType::Int32);
    let crate::analysis::ExprKind::Constant(actual) = &args[0].kind else {
        panic!("original interior constant");
    };
    assert_eq!(actual.ordinal(), 1);
    assert_eq!(actual.pool().backing_identity(), pool.backing_identity());
    assert_eq!(actual.field(), field.as_ref());
    assert!(Arc::ptr_eq(actual.pool().field_ref(), &field));
    assert_eq!(
        actual
            .field()
            .metadata()
            .get("provider.unknown")
            .map(String::as_str),
        Some("full-field")
    );
    assert_eq!(actual.value_type(), &ty);
}

#[test]
fn order_key_loans_keep_their_exact_owner_field_ordinal_and_null_rules() {
    use SqlConstantExpressionSite as Site;
    use SqlConstantExpressionStep as Edge;
    use arrow::array::{Array, Int32Array};
    use arrow::datatypes::{DataType, Field};
    use novarocks_functions::ConstantPool;
    let control = SqlCompileControl::unbounded();
    let policy = crate::constant::test_constant_policy();
    let ty = FunctionValueType::new(DataType::Int32, false);
    let field = Arc::new(
        Field::new("original_order_key", DataType::Int32, false).with_metadata(
            std::collections::HashMap::from([(
                "provider.unknown".to_string(),
                "full-field".to_string(),
            )]),
        ),
    );
    let pool = ConstantPool::try_new(
        field.clone(),
        ty.clone(),
        Int32Array::from(vec![7, 11]).to_data(),
        policy,
        CompilePhase::Validate,
        &control,
    )
    .unwrap();
    let retained = pool.value(1).unwrap();
    let functions = build_builtin_engine_function_catalog().unwrap();
    let catalog = crate::planning::catalog::PlannerMemoryCatalog::default();
    for (sql, site, path, ascending, nulls_first) in [
        (
            "SELECT 1 UNION ALL SELECT 2 ORDER BY only_original_sort DESC",
            Site::QueryOrderByKey { index: 0 },
            Vec::new(),
            false,
            false,
        ),
        (
            "SELECT 1 ORDER BY only_original_sort",
            Site::SelectOrderByKey { index: 0 },
            Vec::new(),
            true,
            true,
        ),
        (
            "SELECT array_agg(1 ORDER BY only_original_sort ASC NULLS LAST)",
            Site::SelectProjection { column: 0 },
            vec![Edge::FunctionOrderByKey(0)],
            true,
            false,
        ),
        (
            "SELECT row_number() OVER (ORDER BY only_original_sort DESC NULLS FIRST)",
            Site::SelectProjection { column: 0 },
            vec![Edge::WindowOrderByKey(0)],
            false,
            true,
        ),
    ] {
        let query = query(sql);
        let constant = |site, path| {
            SqlExpressionConstant::new(Vec::new(), site, retained.clone())
                .with_expression_path(path)
        };
        let source = MaterializedValues::try_new_expressions(
            &query,
            vec![constant(site, path.clone())].into(),
            &control,
        )
        .unwrap();
        let mut work =
            novarocks_type_contract::CompileCheckpoints::try_new(&control, CompilePhase::Validate)
                .unwrap();
        let key = resolve_expression_site(&query, &[], site, &path, false, &mut work).unwrap();
        work.finish().unwrap();
        let owned = source.loan(&query, &control).unwrap();
        let loan = owned.view();
        let actual = loan.constant_at(key).unwrap();
        assert_eq!(actual.ordinal(), 1, "{sql}");
        assert_eq!(actual.pool().backing_identity(), pool.backing_identity());
        assert!(Arc::ptr_eq(actual.pool().field_ref(), &field));
        assert_eq!(actual.field(), field.as_ref());
        assert_eq!(actual.value_type(), &ty);
        assert_eq!(
            actual
                .field()
                .metadata()
                .get("provider.unknown")
                .map(String::as_str),
            Some("full-field")
        );
        assert!(loan.constant_at(&key.clone()).is_none());
        let (bad_site, bad_path) = match site {
            Site::QueryOrderByKey { .. } => (Site::QueryOrderByKey { index: 1 }, Vec::new()),
            Site::SelectOrderByKey { .. } => (Site::SelectOrderByKey { index: 1 }, Vec::new()),
            _ => (
                site,
                vec![match path[0] {
                    Edge::FunctionOrderByKey(_) => Edge::FunctionOrderByKey(1),
                    Edge::WindowOrderByKey(_) => Edge::WindowOrderByKey(1),
                    _ => unreachable!("only ORDER BY paths"),
                }],
            ),
        };
        assert!(
            MaterializedValues::try_new_expressions(
                &query,
                vec![constant(bad_site, bad_path)].into(),
                &control,
            )
            .is_err()
        );
        let (wrong_site, wrong_path) = match site {
            Site::QueryOrderByKey { .. } => (Site::SelectOrderByKey { index: 0 }, Vec::new()),
            Site::SelectOrderByKey { .. } => (Site::QueryOrderByKey { index: 0 }, Vec::new()),
            _ => (
                site,
                vec![match path[0] {
                    Edge::FunctionOrderByKey(_) => Edge::WindowOrderByKey(0),
                    Edge::WindowOrderByKey(_) => Edge::FunctionOrderByKey(0),
                    _ => unreachable!("only ORDER BY paths"),
                }],
            ),
        };
        assert!(
            MaterializedValues::try_new_expressions(
                &query,
                vec![constant(wrong_site, wrong_path)].into(),
                &control,
            )
            .is_err()
        );
        if matches!(path.first(), Some(Edge::WindowOrderByKey(_))) {
            let mut reference_query = query.clone();
            let reference_span = reference_query.span;
            let ast::SetExpr::Select(select) = reference_query.body.as_mut() else {
                panic!("select");
            };
            let ast::SelectItem::SingleColumn(projection_column) = &mut select.projection[0] else {
                panic!("call");
            };
            assert!(projection_column.alias.is_none(), "anonymous projection");
            let ast::Expr::FunctionCall(call) = &mut projection_column.expression else {
                panic!("call");
            };
            call.window = Some(ast::Window::Reference(ast::WindowReference {
                name: novarocks_parser::build::ident("unresolved_window", false),
                span: reference_span,
            }));
            assert!(
                MaterializedValues::try_new_expressions(
                    &reference_query,
                    vec![constant(site, path.clone())].into(),
                    &control,
                )
                .is_err()
            );
        }
        if matches!(site, Site::QueryOrderByKey { .. }) {
            // A set operand cannot borrow the enclosing Query's sort key.
            let wrong_owner = SqlExpressionConstant::new(
                vec![SqlConstantQueryStep::SetOperand { index: 0 }],
                site,
                retained.clone(),
            );
            assert!(MaterializedValues::try_new_expressions(
                &query, vec![wrong_owner].into(), &control,
            ).is_err());
        }
        let (resolved, _, _) = crate::analyzer::analyze_with_materialized_values_and_sql_semantics(
            &query,
            &catalog,
            "db",
            &functions,
            &crate::sql_mode::SqlSemanticSettings::default(),
            policy,
            &control,
            Some(&source),
        )
        .unwrap();
        let sort = if let Some(sort) = resolved.order_by.first() {
            sort
        } else {
            let crate::analysis::QueryBody::Select(select) = &resolved.body else {
                panic!("select");
            };
            match &select.projection[0].expr.kind {
                crate::analysis::ExprKind::AggregateCall { order_by, .. }
                | crate::analysis::ExprKind::WindowCall { order_by, .. } => &order_by[0],
                _ => panic!("ordered call"),
            }
        };
        assert_eq!(
            (sort.asc, sort.nulls_first),
            (ascending, nulls_first),
            "{sql}"
        );
        assert_eq!(sort.expr.value_type, ty);
        let crate::analysis::ExprKind::Constant(actual) = &sort.expr.kind else {
            panic!("original key constant");
        };
        assert_eq!(actual.ordinal(), 1);
        assert_eq!(actual.pool().backing_identity(), pool.backing_identity());
        assert!(Arc::ptr_eq(actual.pool().field_ref(), &field));
        assert_eq!(actual.field(), field.as_ref());
        assert_eq!(actual.value_type(), &ty);
    }
}

#[test]
fn function_argument_value_borrows_original_nonzero_full_field_and_refuses_foreign_edges() {
    use arrow::array::{Array, Int32Array};
    use arrow::datatypes::{DataType, Field};
    use novarocks_functions::ConstantPool;
    let control = SqlCompileControl::unbounded();
    let policy = crate::constant::test_constant_policy();
    let ty = FunctionValueType::new(DataType::Int32, false);
    let field = Arc::new(
        Field::new("original_call_value", DataType::Int32, false).with_metadata(
            std::collections::HashMap::from([(
                "provider.unknown".to_string(),
                "full-field".to_string(),
            )]),
        ),
    );
    let pool = ConstantPool::try_new(
        field.clone(),
        ty.clone(),
        Int32Array::from(vec![13, -23]).to_data(),
        policy,
        CompilePhase::Validate,
        &control,
    )
    .unwrap();
    let retained = pool.value(1).unwrap();
    let query = query("SELECT abs(only_original_call_value)");
    let constant = |path| {
        SqlExpressionConstant::new(
            Vec::new(),
            SqlConstantExpressionSite::SelectProjection { column: 0 },
            retained.clone(),
        )
        .with_expression_path(path)
    };
    let source = MaterializedValues::try_new_expressions(
        &query,
        vec![constant(vec![
            SqlConstantExpressionStep::FunctionArgumentValue(0),
        ])]
        .into(),
        &control,
    )
    .unwrap();
    let ast::SetExpr::Select(select) = query.body.as_ref() else {
        panic!("select");
    };
    let ast::SelectItem::SingleColumn(projection_column) = &select.projection[0] else {
        panic!("call");
    };
    assert!(projection_column.alias.is_none(), "anonymous projection");
    let ast::Expr::FunctionCall(call) = &projection_column.expression else {
        panic!("call");
    };
    let original = &call.arguments[0].value;
    let owned = source.loan(&query, &control).unwrap();
    let loan = owned.view();
    let actual = loan.constant_at(original).unwrap();
    assert_eq!(actual.ordinal(), 1);
    assert_eq!(actual.pool().backing_identity(), pool.backing_identity());
    assert!(Arc::ptr_eq(actual.pool().field_ref(), &field));
    assert_eq!(actual.field(), field.as_ref());
    assert_eq!(actual.value_type(), &ty);
    assert_eq!(
        actual
            .field()
            .metadata()
            .get("provider.unknown")
            .map(String::as_str),
        Some("full-field")
    );
    assert!(loan.constant_at(&original.clone()).is_none());
    for path in [
        vec![SqlConstantExpressionStep::FunctionArgumentValue(1)],
        vec![SqlConstantExpressionStep::SubscriptBase],
        vec![
            SqlConstantExpressionStep::FunctionArgumentValue(0),
            SqlConstantExpressionStep::FunctionArgumentValue(0),
        ],
    ] {
        assert!(
            MaterializedValues::try_new_expressions(&query, vec![constant(path)].into(), &control,)
                .is_err()
        );
    }
    let catalog = crate::planning::catalog::PlannerMemoryCatalog::default();
    let functions = build_builtin_engine_function_catalog().unwrap();
    let (resolved, _, _) = crate::analyzer::analyze_with_materialized_values_and_sql_semantics(
        &query,
        &catalog,
        "db",
        &functions,
        &crate::sql_mode::SqlSemanticSettings::default(),
        policy,
        &control,
        Some(&source),
    )
    .unwrap();
    let crate::analysis::QueryBody::Select(select) = &resolved.body else {
        panic!("select");
    };
    let crate::analysis::ExprKind::FunctionCall { name, args, .. } =
        &select.projection[0].expr.kind
    else {
        panic!("call");
    };
    assert_eq!(name, "abs");
    let crate::analysis::ExprKind::Constant(actual) = &args[0].kind else {
        panic!("original value");
    };
    assert_eq!(actual.ordinal(), 1);
    assert_eq!(actual.pool().backing_identity(), pool.backing_identity());
    assert!(Arc::ptr_eq(actual.pool().field_ref(), &field));
    assert_eq!(actual.field(), field.as_ref());
    assert_eq!(actual.value_type(), &ty);
}

#[test]
fn dedicated_control_children_keep_mixed_original_pool_occurrences() {
    use SqlConstantExpressionStep::*;
    use arrow::array::{Array, Int32Array};
    use arrow::datatypes::{DataType, Field};
    use novarocks_functions::ConstantPool;
    let control = SqlCompileControl::unbounded();
    let policy = crate::constant::test_constant_policy();
    let ty = FunctionValueType::new(DataType::Int32, false);
    let field = Arc::new(
        Field::new("original_control_value", DataType::Int32, false).with_metadata(
            std::collections::HashMap::from([(
                "provider.unknown".to_string(),
                "full-field".to_string(),
            )]),
        ),
    );
    let pool = ConstantPool::try_new(
        field.clone(),
        ty.clone(),
        Int32Array::from(vec![13, -23]).to_data(),
        policy,
        CompilePhase::Validate,
        &control,
    )
    .unwrap();
    let retained = pool.value(1).unwrap();
    let query = query("SELECT if(TRUE, coalesce(first_original, 1), nullif(2, second_original))");
    let constant = |path| {
        SqlExpressionConstant::new(
            Vec::new(),
            SqlConstantExpressionSite::SelectProjection { column: 0 },
            retained.clone(),
        )
        .with_expression_path(path)
    };
    let source = MaterializedValues::try_new_expressions(
        &query,
        vec![
            constant(vec![IfTrue, CoalesceOperand(0)]),
            constant(vec![IfFalse, NullIfSecond]),
        ]
        .into(),
        &control,
    )
    .unwrap();
    let ast::SetExpr::Select(select) = query.body.as_ref() else {
        panic!("select");
    };
    let ast::SelectItem::SingleColumn(projection_column) = &select.projection[0] else {
        panic!("if");
    };
    assert!(projection_column.alias.is_none(), "anonymous projection");
    let ast::Expr::If(node) = &projection_column.expression else {
        panic!("if");
    };
    let ast::Expr::Coalesce(coalesce) = &node.true_value else {
        panic!("coalesce");
    };
    let Some(ast::Expr::NullIf(nullif)) = node.false_value.as_ref() else {
        panic!("nullif");
    };
    let owned = source.loan(&query, &control).unwrap();
    let loan = owned.view();
    for expression in [&coalesce.operands[0], &nullif.second] {
        let actual = loan.constant_at(expression).unwrap();
        assert_eq!(actual.ordinal(), 1);
        assert_eq!(actual.pool().backing_identity(), pool.backing_identity());
        assert!(Arc::ptr_eq(actual.pool().field_ref(), &field));
        assert_eq!(actual.field(), field.as_ref());
        assert_eq!(actual.value_type(), &ty);
        assert_eq!(
            actual
                .field()
                .metadata()
                .get("provider.unknown")
                .map(String::as_str),
            Some("full-field")
        );
        assert!(loan.constant_at(&(*expression).clone()).is_none());
    }
    for path in [
        vec![CoalesceOperand(0)],
        vec![IfTrue, CoalesceOperand(2)],
        vec![IfFalse, NullIfSecond, IfTrue],
        vec![IfTrue, FunctionArgumentValue(0)],
    ] {
        assert!(
            MaterializedValues::try_new_expressions(&query, vec![constant(path)].into(), &control)
                .is_err()
        );
    }
    let missing_false = {
        let value = &novarocks_parser::parser::parse_query("SELECT if(TRUE, 1)").unwrap();
        value.clone()
    };
    assert!(
        MaterializedValues::try_new_expressions(
            &missing_false,
            vec![constant(vec![IfFalse])].into(),
            &control
        )
        .is_err()
    );
    let catalog = crate::planning::catalog::PlannerMemoryCatalog::default();
    let functions = build_builtin_engine_function_catalog().unwrap();
    let (resolved, _, _) = crate::analyzer::analyze_with_materialized_values_and_sql_semantics(
        &query,
        &catalog,
        "db",
        &functions,
        &crate::sql_mode::SqlSemanticSettings::default(),
        policy,
        &control,
        Some(&source),
    )
    .unwrap();
    let crate::analysis::QueryBody::Select(select) = &resolved.body else {
        panic!("select");
    };
    let crate::analysis::ExprKind::FunctionCall { name, args, .. } =
        &select.projection[0].expr.kind
    else {
        panic!("if binding");
    };
    assert_eq!(name, "if");
    for (branch, child, expected) in [(1, 0, "coalesce"), (2, 1, "nullif")] {
        let crate::analysis::ExprKind::FunctionCall { name, args, .. } = &args[branch].kind else {
            panic!("branch binding");
        };
        assert_eq!(name, expected);
        let crate::analysis::ExprKind::Constant(actual) = &args[child].kind else {
            panic!("original constant");
        };
        assert_eq!(actual.ordinal(), 1);
        assert_eq!(actual.pool().backing_identity(), pool.backing_identity());
        assert!(Arc::ptr_eq(actual.pool().field_ref(), &field));
        assert_eq!(actual.field(), field.as_ref());
        assert_eq!(actual.value_type(), &ty);
    }
}

#[test]
fn extract_input_borrows_original_nonzero_date_pool_and_refuses_foreign_edges() {
    use arrow::array::{Array, Date32Array};
    use arrow::datatypes::{DataType, Field};
    use novarocks_functions::ConstantPool;
    let control = SqlCompileControl::unbounded();
    let policy = crate::constant::test_constant_policy();
    let ty = FunctionValueType::new(DataType::Date32, false);
    let field = Arc::new(
        Field::new("original_extract_date", DataType::Date32, false).with_metadata(
            std::collections::HashMap::from([(
                "provider.unknown".to_string(),
                "full-field".to_string(),
            )]),
        ),
    );
    let pool = ConstantPool::try_new(
        field.clone(),
        ty.clone(),
        Date32Array::from(vec![0, 19782]).to_data(),
        policy,
        CompilePhase::Validate,
        &control,
    )
    .unwrap();
    let retained = pool.value(1).unwrap();
    let query = query("SELECT EXTRACT(YEAR FROM only_original_date)");
    let constant = |path| {
        SqlExpressionConstant::new(
            Vec::new(),
            SqlConstantExpressionSite::SelectProjection { column: 0 },
            retained.clone(),
        )
        .with_expression_path(path)
    };
    let source = MaterializedValues::try_new_expressions(
        &query,
        vec![constant(vec![SqlConstantExpressionStep::ExtractInput])].into(),
        &control,
    )
    .unwrap();
    let ast::SetExpr::Select(select) = query.body.as_ref() else {
        panic!("select");
    };
    let ast::SelectItem::SingleColumn(projection_column) = &select.projection[0] else {
        panic!("extract");
    };
    assert!(projection_column.alias.is_none(), "anonymous projection");
    let ast::Expr::Extract(node) = &projection_column.expression else {
        panic!("extract");
    };
    let owned = source.loan(&query, &control).unwrap();
    let loan = owned.view();
    let original = &node.expression;
    let actual = loan.constant_at(original).unwrap();
    assert_eq!(actual.ordinal(), 1);
    assert_eq!(actual.pool().backing_identity(), pool.backing_identity());
    assert!(Arc::ptr_eq(actual.pool().field_ref(), &field));
    assert_eq!(actual.field(), field.as_ref());
    assert_eq!(actual.value_type(), &ty);
    assert_eq!(
        actual
            .field()
            .metadata()
            .get("provider.unknown")
            .map(String::as_str),
        Some("full-field")
    );
    assert!(loan.constant_at(&(*original).clone()).is_none());
    for path in [
        vec![SqlConstantExpressionStep::FunctionArgumentValue(1)],
        vec![
            SqlConstantExpressionStep::ExtractInput,
            SqlConstantExpressionStep::ExtractInput,
        ],
        vec![SqlConstantExpressionStep::CoalesceOperand(0)],
    ] {
        assert!(
            MaterializedValues::try_new_expressions(&query, vec![constant(path)].into(), &control)
                .is_err()
        );
    }
    let catalog = crate::planning::catalog::PlannerMemoryCatalog::default();
    let functions = build_builtin_engine_function_catalog().unwrap();
    let (resolved, _, _) = crate::analyzer::analyze_with_materialized_values_and_sql_semantics(
        &query,
        &catalog,
        "db",
        &functions,
        &crate::sql_mode::SqlSemanticSettings::default(),
        policy,
        &control,
        Some(&source),
    )
    .unwrap();
    let crate::analysis::QueryBody::Select(select) = &resolved.body else {
        panic!("select");
    };
    let crate::analysis::ExprKind::FunctionCall { name, args, .. } =
        &select.projection[0].expr.kind
    else {
        panic!("unit binding");
    };
    assert_eq!(name, "year");
    let crate::analysis::ExprKind::Constant(actual) = &args[0].kind else {
        panic!("original date");
    };
    assert_eq!(actual.ordinal(), 1);
    assert_eq!(actual.pool().backing_identity(), pool.backing_identity());
    assert!(Arc::ptr_eq(actual.pool().field_ref(), &field));
    assert_eq!(actual.field(), field.as_ref());
    assert_eq!(actual.value_type(), &ty);
}

#[test]
fn row_field_expression_keeps_original_nonzero_full_field_pool() {
    use arrow::array::{Array, Int32Array};
    use arrow::datatypes::{DataType, Field};
    use novarocks_functions::ConstantPool;
    let control = SqlCompileControl::unbounded();
    let policy = crate::constant::test_constant_policy();
    let ty = FunctionValueType::new(DataType::Int32, false);
    let field = Arc::new(
        Field::new("original_row_child", DataType::Int32, false).with_metadata(
            std::collections::HashMap::from([(
                "provider.unknown".to_string(),
                "full-field".to_string(),
            )]),
        ),
    );
    let pool = ConstantPool::try_new(
        field.clone(),
        ty.clone(),
        Int32Array::from(vec![13, -23]).to_data(),
        policy,
        CompilePhase::Validate,
        &control,
    )
    .unwrap();
    let retained = pool.value(1).unwrap();
    let query = query("SELECT ROW(1, only_original_row_child)");
    let constant = |path| {
        SqlExpressionConstant::new(
            Vec::new(),
            SqlConstantExpressionSite::SelectProjection { column: 0 },
            retained.clone(),
        )
        .with_expression_path(path)
    };
    let source = MaterializedValues::try_new_expressions(
        &query,
        vec![constant(vec![
            SqlConstantExpressionStep::RowFieldExpression(1),
        ])]
        .into(),
        &control,
    )
    .unwrap();
    let ast::SetExpr::Select(select) = query.body.as_ref() else {
        panic!("select");
    };
    let ast::SelectItem::SingleColumn(projection_column) = &select.projection[0] else {
        panic!("row");
    };
    assert!(projection_column.alias.is_none(), "anonymous projection");
    let ast::Expr::Row(row) = &projection_column.expression else {
        panic!("row");
    };
    let original = &row.fields[1].expression;
    let owned = source.loan(&query, &control).unwrap();
    let loan = owned.view();
    let actual = loan.constant_at(original).unwrap();
    assert_eq!(actual.ordinal(), 1);
    assert_eq!(actual.pool().backing_identity(), pool.backing_identity());
    assert!(Arc::ptr_eq(actual.pool().field_ref(), &field));
    assert_eq!(actual.field(), field.as_ref());
    assert_eq!(actual.value_type(), &ty);
    assert!(loan.constant_at(&(*original).clone()).is_none());
    for path in [
        vec![SqlConstantExpressionStep::RowFieldExpression(2)],
        vec![SqlConstantExpressionStep::FunctionArgumentValue(1)],
        vec![
            SqlConstantExpressionStep::RowFieldExpression(1),
            SqlConstantExpressionStep::RowFieldExpression(0),
        ],
    ] {
        assert!(
            MaterializedValues::try_new_expressions(&query, vec![constant(path)].into(), &control)
                .is_err()
        );
    }
    let catalog = crate::planning::catalog::PlannerMemoryCatalog::default();
    let functions = build_builtin_engine_function_catalog().unwrap();
    let (resolved, _, _) = crate::analyzer::analyze_with_materialized_values_and_sql_semantics(
        &query,
        &catalog,
        "db",
        &functions,
        &crate::sql_mode::SqlSemanticSettings::default(),
        policy,
        &control,
        Some(&source),
    )
    .unwrap();
    let crate::analysis::QueryBody::Select(select) = &resolved.body else {
        panic!("select");
    };
    let crate::analysis::ExprKind::FunctionCall { name, args, .. } =
        &select.projection[0].expr.kind
    else {
        panic!("row binding");
    };
    assert_eq!(name, "row");
    let crate::analysis::ExprKind::Constant(actual) = &args[1].kind else {
        panic!("original child");
    };
    assert_eq!(actual.ordinal(), 1);
    assert_eq!(actual.pool().backing_identity(), pool.backing_identity());
    assert!(Arc::ptr_eq(actual.pool().field_ref(), &field));
    assert_eq!(actual.field(), field.as_ref());
    assert_eq!(actual.value_type(), &ty);
    let DataType::Struct(fields) = select.projection[0].expr.value_type.data_type() else {
        panic!("whole Struct");
    };
    assert_eq!(fields.len(), 2);
    assert_eq!(fields[1].data_type(), &DataType::Int32);
}

#[test]
fn concat_normalization_reloans_original_value_separator_and_order_occurrences() {
    use SqlConstantExpressionSite as Site;
    use SqlConstantExpressionStep as Edge;
    use arrow::array::{Array, Int32Array, StringArray};
    use arrow::datatypes::{DataType, Field};
    use novarocks_functions::ConstantPool;
    let control = SqlCompileControl::unbounded();
    let policy = crate::constant::test_constant_policy();
    let text = |name: &str, value: &str| {
        let ty = FunctionValueType::new(DataType::Utf8, false);
        let field = Arc::new(Field::new(name, DataType::Utf8, false).with_metadata(
            std::collections::HashMap::from([("provider.unknown".to_string(), name.to_string())]),
        ));
        ConstantPool::try_new(
            field,
            ty,
            StringArray::from(vec!["unused first row", value]).to_data(),
            policy,
            CompilePhase::Validate,
            &control,
        )
        .unwrap()
        .value(1)
        .unwrap()
    };
    let value = text("original_concat_value", "kept value");
    let separator = text("original_concat_separator", "|");
    let order_type = FunctionValueType::new(DataType::Int32, false);
    let order_field = Arc::new(
        Field::new("original_concat_order", DataType::Int32, false).with_metadata(
            std::collections::HashMap::from([(
                "provider.unknown".to_string(),
                "original_concat_order".to_string(),
            )]),
        ),
    );
    let order = ConstantPool::try_new(
        order_field,
        order_type,
        Int32Array::from(vec![7, 11]).to_data(),
        policy,
        CompilePhase::Validate,
        &control,
    )
    .unwrap()
    .value(1)
    .unwrap();
    fn call(query: &ast::Query) -> &ast::FunctionCall {
        let ast::SetExpr::Select(select) = query.body.as_ref() else {
            panic!("original SELECT");
        };
        let ast::SelectItem::SingleColumn(projection_column) = &select.projection[0] else {
            panic!("original concatenation call");
        };
        assert!(projection_column.alias.is_none(), "anonymous projection");
        let ast::Expr::FunctionCall(call) = &projection_column.expression else {
            panic!("original concatenation call");
        };
        call
    }
    fn retained(actual: &ConstantValue, expected: &ConstantValue) {
        assert_eq!(actual.ordinal(), 1);
        assert_eq!(
            actual.pool().backing_identity(),
            expected.pool().backing_identity()
        );
        assert!(Arc::ptr_eq(
            actual.pool().field_ref(),
            expected.pool().field_ref()
        ));
        assert_eq!(actual.field(), expected.field());
        assert_eq!(actual.value_type(), expected.value_type());
        assert_eq!(
            actual
                .field()
                .metadata()
                .get("provider.unknown")
                .map(String::as_str),
            Some(expected.field().name().as_str())
        );
    }
    let catalog = crate::planning::catalog::PlannerMemoryCatalog::default();
    let functions = build_builtin_engine_function_catalog().unwrap();
    for (sql, legacy, parsed_name) in [
        (
            "SELECT string_agg(only_original_value \n,\n only_original_separator\nORDER BY only_original_order DESC NULLS LAST \n)",
            false,
            "string_agg",
        ),
        (
            "SELECT group_concat(only_original_value, only_original_separator ORDER BY only_original_order DESC NULLS LAST)",
            true,
            "group_concat",
        ),
    ] {
        let parsed = query(sql);
        assert_eq!(
            call(&parsed).name.parts[0].value.to_ascii_lowercase(),
            parsed_name
        );
        let semantics = crate::sql_mode::SqlSemanticSettings::default()
            .with_sql_mode(crate::sql_mode::SqlMode::from_assignment(if legacy {
                "GROUP_CONCAT_LEGACY"
            } else {
                "32"
            }))
            .with_group_concat_max_len(1024);
        let constant = |path, value: &ConstantValue| {
            SqlExpressionConstant::new(
                Vec::new(),
                Site::SelectProjection { column: 0 },
                value.clone(),
            )
            .with_expression_path(path)
        };
        let source = MaterializedValues::try_new_expressions(
            &parsed,
            vec![
                constant(vec![Edge::FunctionArgumentValue(0)], &value),
                constant(vec![Edge::FunctionArgumentValue(1)], &separator),
                constant(vec![Edge::FunctionOrderByKey(0)], &order),
            ]
            .into(),
            &control,
        )
        .unwrap();
        let original_loan = source.loan(&parsed, &control).unwrap();
        let normalized =
            crate::sql_mode::normalize_concat_query(parsed.clone(), &semantics).unwrap();
        assert_eq!(call(&normalized).name.parts[0].value, "string_agg");
        assert_eq!(call(&normalized).arguments.len(), 2);
        let twice =
            crate::sql_mode::normalize_concat_query(normalized.clone(), &semantics).unwrap();
        assert_eq!(normalized, twice);
        let normalized_loan = source.loan(&normalized, &control).unwrap();
        let original_call = call(&parsed);
        let normalized_call = call(&normalized);
        for (before, after, expected) in [
            (
                &original_call.arguments[0].value,
                &normalized_call.arguments[0].value,
                &value,
            ),
            (
                &original_call.arguments[1].value,
                &normalized_call.arguments[1].value,
                &separator,
            ),
            (
                &original_call.order_by.as_ref().unwrap().items[0].sort_key,
                &normalized_call.order_by.as_ref().unwrap().items[0].sort_key,
                &order,
            ),
        ] {
            retained(original_loan.view().constant_at(before).unwrap(), expected);
            retained(normalized_loan.view().constant_at(after).unwrap(), expected);
            assert!(original_loan.view().constant_at(after).is_none());
            assert!(normalized_loan.view().constant_at(before).is_none());
            assert!(
                normalized_loan
                    .view()
                    .constant_at(&(*after).clone())
                    .is_none()
            );
        }
        for path in [
            vec![Edge::FunctionArgumentValue(2)],
            vec![Edge::FunctionOrderByKey(1)],
            vec![Edge::WindowOrderByKey(0)],
        ] {
            assert!(
                MaterializedValues::try_new_expressions(
                    &normalized,
                    vec![constant(path, &value)].into(),
                    &control,
                )
                .is_err()
            );
        }
        // The production entry clones, normalizes and reloans this same source.
        // If binding clones/concatenates argument Exprs, the unknown names cannot resolve.
        let (resolved_query, _, _) =
            crate::analyzer::analyze_with_materialized_values_and_sql_semantics(
                &parsed,
                &catalog,
                "db",
                &functions,
                &semantics,
                policy,
                &control,
                Some(&source),
            )
            .unwrap();
        let crate::analysis::QueryBody::Select(select) = &resolved_query.body else {
            panic!("actual SELECT");
        };
        let crate::analysis::ExprKind::AggregateCall {
            name,
            args,
            distinct,
            order_by,
            resolved,
        } = &select.projection[0].expr.kind
        else {
            panic!("actual selected aggregate");
        };
        assert_eq!(name, "string_agg");
        assert!(!distinct);
        assert_eq!(args.len(), 2);
        assert_eq!(resolved.kind, novarocks_functions::FunctionKind::Aggregate);
        for (argument, expected) in args.iter().zip([&value, &separator]) {
            assert_eq!(&argument.value_type, expected.value_type());
            let crate::analysis::ExprKind::Constant(actual) = &argument.kind else {
                panic!("the actual bound argument retains its original constant");
            };
            retained(actual, expected);
        }
        assert_eq!(order_by.len(), 1);
        assert!(!order_by[0].asc);
        assert!(!order_by[0].nulls_first);
        assert_eq!(&order_by[0].expr.value_type, order.value_type());
        let crate::analysis::ExprKind::Constant(actual) = &order_by[0].expr.kind else {
            panic!("the actual order key retains its original constant");
        };
        retained(actual, &order);
    }
}

#[test]
fn actual_relation_path_keeps_right_derived_nonzero_pool_and_full_field() {
    use arrow::array::{Array, StringArray};
    use arrow::datatypes::{DataType, Field};
    use novarocks_functions::ConstantPool;
    let control = SqlCompileControl::unbounded();
    let policy = crate::constant::test_constant_policy();
    let ty = FunctionValueType::new(DataType::Utf8, false);
    let field = Arc::new(
        Field::new("original_derived_value", DataType::Utf8, false).with_metadata(
            std::collections::HashMap::from([(
                "provider.unknown".to_string(),
                "original-right".to_string(),
            )]),
        ),
    );
    let pool = ConstantPool::try_new(
        field.clone(),
        ty.clone(),
        StringArray::from(vec!["unused", "kept-right"]).to_data(),
        policy,
        CompilePhase::Validate,
        &control,
    )
    .unwrap();
    let retained = pool.value(1).unwrap();
    let query = query("SELECT r.x FROM (SELECT 1 AS k) l JOIN (SELECT NULL AS x) r ON true");
    let path = vec![SqlRelationStep::JoinRight, SqlRelationStep::Aliased];
    let constant = |path| {
        SqlExpressionConstant::new(
            vec![SqlConstantQueryStep::SelectFromDerived {
                relation_path: path,
            }],
            SqlConstantExpressionSite::SelectProjection { column: 0 },
            retained.clone(),
        )
    };
    let source =
        MaterializedValues::try_new_expressions(&query, vec![constant(path)].into(), &control)
            .unwrap();
    let ast::SetExpr::Select(select) = query.body.as_ref() else {
        panic!("outer SELECT");
    };
    let Some(ast::Relation::Join(join)) = &select.from else {
        panic!("actual Join");
    };
    let ast::Relation::Aliased(alias) = join.right.as_ref() else {
        panic!("actual right Alias");
    };
    let ast::Relation::TableSubquery(derived) = alias.relation.as_ref() else {
        panic!("actual derived endpoint");
    };
    let ast::SetExpr::Select(inner) = derived.query.body.as_ref() else {
        panic!("inner SELECT");
    };
    let ast::SelectItem::SingleColumn(projection_column) = &inner.projection[0] else {
        panic!("source value");
    };
    let ast::SingleColumn {
        alias: Some(_),
        expression: expr,
        ..
    } = projection_column.as_ref()
    else {
        panic!("source value");
    };
    let owned = source.loan(&query, &control).unwrap();
    let loan = owned.view();
    let actual = loan.constant_at(expr).unwrap();
    assert_eq!(actual.ordinal(), 1);
    assert_eq!(actual.pool().backing_identity(), pool.backing_identity());
    assert!(Arc::ptr_eq(actual.pool().field_ref(), &field));
    assert_eq!(actual.field(), field.as_ref());
    assert_eq!(actual.value_type(), &ty);
    assert_eq!(
        actual
            .field()
            .metadata()
            .get("provider.unknown")
            .map(String::as_str),
        Some("original-right")
    );
    assert!(loan.constant_at(&expr.clone()).is_none());
    for path in [
        vec![SqlRelationStep::Aliased, SqlRelationStep::JoinRight],
        vec![SqlRelationStep::JoinRight, SqlRelationStep::JoinLeft],
        vec![SqlRelationStep::JoinLeft],
    ] {
        assert!(
            MaterializedValues::try_new_expressions(&query, vec![constant(path)].into(), &control)
                .is_err()
        );
    }
    let catalog = crate::planning::catalog::PlannerMemoryCatalog::default();
    let functions = build_builtin_engine_function_catalog().unwrap();
    let (resolved, _, _) = crate::analyzer::analyze_with_materialized_values_and_sql_semantics(
        &query,
        &catalog,
        "db",
        &functions,
        &Default::default(),
        policy,
        &control,
        Some(&source),
    )
    .unwrap();
    let crate::analysis::QueryBody::Select(outer) = &resolved.body else {
        panic!("outer");
    };
    let Some(crate::analysis::Relation::Join(join)) = &outer.from else {
        panic!("join");
    };
    let crate::analysis::Relation::Subquery { query, .. } = &join.right else {
        panic!("derived");
    };
    let crate::analysis::QueryBody::Select(inner) = &query.body else {
        panic!("inner");
    };
    let crate::analysis::ExprKind::Constant(actual) = &inner.projection[0].expr.kind else {
        panic!("same original constant");
    };
    assert_eq!(actual.ordinal(), 1);
    assert_eq!(actual.pool().backing_identity(), pool.backing_identity());
    assert!(Arc::ptr_eq(actual.pool().field_ref(), &field));
    assert_eq!(actual.field(), field.as_ref());
    assert_eq!(actual.value_type(), &ty);
}

#[test]
fn values_and_single_column_sites_keep_original_nonzero_full_field_sources() {
    use arrow::array::{Array, Int32Array};
    use arrow::datatypes::{DataType, Field};
    use novarocks_functions::ConstantPool;
    let control = SqlCompileControl::unbounded();
    let policy = crate::constant::test_constant_policy();
    let ty = FunctionValueType::new(DataType::Int32, false);
    let field = Arc::new(
        Field::new("original_projection_value", DataType::Int32, false).with_metadata(
            std::collections::HashMap::from([(
                "provider.unknown".to_string(),
                "projection-values".to_string(),
            )]),
        ),
    );
    let pool = ConstantPool::try_new(
        field.clone(),
        ty.clone(),
        Int32Array::from(vec![13, -23]).to_data(),
        policy,
        CompilePhase::Validate,
        &control,
    )
    .unwrap();
    let retained = pool.value(1).unwrap();
    for (sql, site, wrong_kind, wrong_index) in [
        (
            "VALUES (1, only_original_value)",
            SqlConstantExpressionSite::ValuesCell { row: 0, column: 1 },
            SqlConstantExpressionSite::SelectProjection { column: 1 },
            SqlConstantExpressionSite::ValuesCell { row: 0, column: 2 },
        ),
        (
            "SELECT 1, only_original_value AS preserved_alias",
            SqlConstantExpressionSite::SelectProjection { column: 1 },
            SqlConstantExpressionSite::ValuesCell { row: 0, column: 1 },
            SqlConstantExpressionSite::SelectProjection { column: 2 },
        ),
    ] {
        let query = query(sql);
        let constants = |site| {
            vec![SqlExpressionConstant::new(
                Vec::new(),
                site,
                retained.clone(),
            )]
            .into()
        };
        let source =
            MaterializedValues::try_new_expressions(&query, constants(site), &control).unwrap();
        let expression = match query.body.as_ref() {
            ast::SetExpr::Values(values) => {
                ast::ValuesRowCells::new(&values.rows[0]).get(1).unwrap()
            }
            ast::SetExpr::Select(select) => {
                let ast::SelectItem::SingleColumn(column) = &select.projection[1] else {
                    panic!("single column")
                };
                assert_eq!(column.alias.as_ref().unwrap().value, "preserved_alias");
                &column.expression
            }
            _ => panic!("actual source owner"),
        };
        let owned = source.loan(&query, &control).unwrap();
        let loan = owned.view();
        let actual = loan.constant_at(expression).unwrap();
        assert_eq!(actual.ordinal(), 1);
        assert_eq!(actual.pool().backing_identity(), pool.backing_identity());
        assert!(Arc::ptr_eq(actual.pool().field_ref(), &field));
        assert_eq!(actual.field(), field.as_ref());
        assert_eq!(actual.value_type(), &ty);
        assert_eq!(
            actual
                .field()
                .metadata()
                .get("provider.unknown")
                .map(String::as_str),
            Some("projection-values")
        );
        assert!(loan.constant_at(&(*expression).clone()).is_none());
        for wrong in [wrong_kind, wrong_index] {
            assert!(
                MaterializedValues::try_new_expressions(&query, constants(wrong), &control)
                    .is_err()
            );
        }
        let catalog = crate::planning::catalog::PlannerMemoryCatalog::default();
        let functions = build_builtin_engine_function_catalog().unwrap();
        let (resolved, _, _) = crate::analyzer::analyze_with_materialized_values_and_sql_semantics(
            &query,
            &catalog,
            "db",
            &functions,
            &crate::sql_mode::SqlSemanticSettings::default(),
            policy,
            &control,
            Some(&source),
        )
        .unwrap();
        let actual = match &resolved.body {
            crate::analysis::QueryBody::Values(values) => &values.rows[0][1],
            crate::analysis::QueryBody::Select(select) => {
                assert_eq!(
                    select.projection[1].field_name.as_deref(),
                    Some("preserved_alias")
                );
                &select.projection[1].expr
            }
            _ => panic!("resolved source owner"),
        };
        let crate::analysis::ExprKind::Constant(actual) = &actual.kind else {
            panic!("source constant")
        };
        assert_eq!(actual.ordinal(), 1);
        assert_eq!(actual.pool().backing_identity(), pool.backing_identity());
        assert!(Arc::ptr_eq(actual.pool().field_ref(), &field));
        assert_eq!(actual.value_type(), &ty);
    }
    let wildcard = query("SELECT *");
    assert!(
        MaterializedValues::try_new_expressions(
            &wildcard,
            vec![SqlExpressionConstant::new(
                Vec::new(),
                SqlConstantExpressionSite::SelectProjection { column: 0 },
                retained
            )]
            .into(),
            &control
        )
        .is_err()
    );
}

#[test]
fn dedicated_trim_and_grouping_edges_keep_actual_nonzero_source_fields() {
    use arrow::array::{Array, Int32Array, StringArray};
    use arrow::datatypes::{DataType, Field};
    use novarocks_functions::ConstantPool;
    use novarocks_parser::{SYNTHETIC, build};
    let control = SqlCompileControl::unbounded();
    let policy = crate::constant::test_constant_policy();
    for trim in [false, true] {
        let data = if trim {
            StringArray::from(vec!["unused", " x "]).to_data()
        } else {
            Int32Array::from(vec![13, -23]).to_data()
        };
        let ty = FunctionValueType::new(data.data_type().clone(), false);
        let field = Arc::new(
            Field::new("original_special_child", data.data_type().clone(), false).with_metadata(
                std::collections::HashMap::from([(
                    "provider.unknown".to_string(),
                    "special-root".to_string(),
                )]),
            ),
        );
        let pool = ConstantPool::try_new(
            field.clone(),
            ty.clone(),
            data,
            policy,
            CompilePhase::Validate,
            &control,
        )
        .unwrap();
        let retained = pool.value(1).unwrap();
        let mut query = query("SELECT 1");
        let ast::SetExpr::Select(select) = query.body.as_mut() else {
            panic!("select")
        };
        let ast::SelectItem::SingleColumn(column) = &mut select.projection[0] else {
            panic!("column")
        };
        let child = build::identifier(build::ident("only_original_child", false));
        column.expression = if trim {
            ast::Expr::Trim(ast::AstBox::new(ast::Trim {
                specification: ast::TrimSpecification::Both,
                trim_source: child,
                trim_character: None,
                span: SYNTHETIC,
            }))
        } else {
            ast::Expr::GroupingOperation(ast::AstBox::new(ast::GroupingOperation {
                grouping_columns: vec![child].into(),
                span: SYNTHETIC,
            }))
        };
        let edge = if trim {
            SqlConstantExpressionStep::TrimSource
        } else {
            SqlConstantExpressionStep::GroupingColumn(0)
        };
        let constant = |edge| {
            vec![
                SqlExpressionConstant::new(
                    Vec::new(),
                    SqlConstantExpressionSite::SelectProjection { column: 0 },
                    retained.clone(),
                )
                .with_expression_path(vec![edge]),
            ]
            .into()
        };
        let source =
            MaterializedValues::try_new_expressions(&query, constant(edge), &control).unwrap();
        let ast::SetExpr::Select(select) = query.body.as_ref() else {
            panic!("select")
        };
        let ast::SelectItem::SingleColumn(column) = &select.projection[0] else {
            panic!("column")
        };
        let child = match &column.expression {
            ast::Expr::Trim(node) => &node.trim_source,
            ast::Expr::GroupingOperation(node) => &node.grouping_columns[0],
            _ => panic!("actual root"),
        };
        let owned = source.loan(&query, &control).unwrap();
        let loan = owned.view();
        let actual = loan.constant_at(child).unwrap();
        assert_eq!(actual.ordinal(), 1);
        assert!(Arc::ptr_eq(actual.pool().field_ref(), &field));
        assert_eq!(actual.pool().backing_identity(), pool.backing_identity());
        assert_eq!(actual.field(), field.as_ref());
        assert_eq!(actual.value_type(), &ty);
        assert_eq!(
            actual
                .field()
                .metadata()
                .get("provider.unknown")
                .map(String::as_str),
            Some("special-root")
        );
        assert!(loan.constant_at(&(*child).clone()).is_none());
        for wrong in [
            SqlConstantExpressionStep::GroupingColumn(1),
            SqlConstantExpressionStep::FunctionArgumentValue(0),
            if trim {
                SqlConstantExpressionStep::GroupingColumn(0)
            } else {
                SqlConstantExpressionStep::TrimSource
            },
        ] {
            assert!(
                MaterializedValues::try_new_expressions(&query, constant(wrong), &control).is_err()
            );
        }
        if trim {
            let catalog = crate::planning::catalog::PlannerMemoryCatalog::default();
            let functions = build_builtin_engine_function_catalog().unwrap();
            let (resolved, _, _) =
                crate::analyzer::analyze_with_materialized_values_and_sql_semantics(
                    &query,
                    &catalog,
                    "db",
                    &functions,
                    &crate::sql_mode::SqlSemanticSettings::default(),
                    policy,
                    &control,
                    Some(&source),
                )
                .unwrap();
            let crate::analysis::QueryBody::Select(select) = resolved.body else {
                panic!("select")
            };
            let crate::analysis::ExprKind::FunctionCall { name, args, .. } =
                &select.projection[0].expr.kind
            else {
                panic!("real trim binding")
            };
            assert_eq!(name, "trim");
            let crate::analysis::ExprKind::Constant(actual) = &args[0].kind else {
                panic!("original argument")
            };
            assert_eq!(actual.ordinal(), 1);
            assert!(Arc::ptr_eq(actual.pool().field_ref(), &field));
            assert_eq!(
                select.projection[0].expr.value_type.data_type(),
                &DataType::Utf8
            );
        }
    }
}
