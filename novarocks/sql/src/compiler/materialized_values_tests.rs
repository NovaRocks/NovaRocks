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
    let statements = novarocks_parser::parse(sql).unwrap();
    let [ast::Statement::Query(query)] = statements.as_slice() else {
        panic!("query")
    };
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
    assert!(loan.constant_at(&values.rows[0][0]).is_none());
    assert!(loan.constant_at(&values.rows[0][1]).is_some());
    let independent = query("VALUES (1, NULL)");
    let ast::SetExpr::Values(values) = independent.body.as_ref() else {
        panic!("values")
    };
    assert!(
        loan.constant_at(&values.rows[0][1]).is_none(),
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
    let value = value(policy, &control);
    let identity = value.pool().backing_identity();
    let query = query("VALUES (1 + 2, NULL), (NULL, CAST(NULL AS VARIANT))");
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
    assert!(
        value.try_binary().unwrap().is_some(),
        "encoded JSON null is not SQL NULL"
    );
    assert_eq!(
        values.rows[0][1].value_type.logical_type(),
        ValueLogicalType::Variant
    );
    assert_eq!(
        values.rows[1][1].value_type.logical_type(),
        ValueLogicalType::Variant
    );
    assert!(matches!(
        values.rows[1][1].kind,
        crate::analysis::ExprKind::Literal(crate::analysis::LiteralValue::Null)
    ));
    assert!(matches!(
        values.rows[0][0].kind,
        crate::analysis::ExprKind::BinaryOp { .. }
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
    let ast::SelectItem::UnnamedExpr(ast::Expr::Logical(logical)) = &select.projection[0] else {
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
        vec![SqlConstantExpressionStep::Nested],
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
            "SELECT 1 <=> source_value",
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
        let ast::SelectItem::UnnamedExpr(ast::Expr::Predicated(node)) = &select.projection[0]
        else {
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
                            ast::SelectItem::UnnamedExpr(expr) => Some(expr),
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
