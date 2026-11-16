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
