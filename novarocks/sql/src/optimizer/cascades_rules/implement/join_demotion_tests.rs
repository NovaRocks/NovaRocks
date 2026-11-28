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

use super::*;
use crate::analysis::{BinOp, ExprKind, JoinKind, OutputColumn, TypedExpr};
use crate::column_id::ColumnId;
use crate::optimizer::cascades_rules::join_commutativity::JoinCommutativity;
use crate::optimizer::memo::{LogicalProperties, MExpr, Memo};
use crate::optimizer::operator::{LogicalJoinOp, ScanOp};
use crate::optimizer::scalar::ScalarId;
use crate::planner::table::TableDef;

use crate::planner::optimizer_bridge::scalar::{intern_typed, materialize};
use arrow::datatypes::{DataType, Field};
use std::sync::Arc;

fn test_col_id(name: &str) -> ColumnId {
    match name {
        "a_id" => ColumnId::new_for_test(10),
        "a_name" => ColumnId::new_for_test(11),
        "a_arr" => ColumnId::new_for_test(12),
        "b_id" => ColumnId::new_for_test(20),
        "b_arr" => ColumnId::new_for_test(21),
        _ => ColumnId::new_for_test(100),
    }
}

fn col(name: &str) -> TypedExpr {
    col_typed(name, DataType::Int32)
}

fn col_typed(name: &str, data_type: DataType) -> TypedExpr {
    TypedExpr {
        kind: ExprKind::ColumnRef {
            column_id: test_col_id(name),
            qualifier: None,
            column: name.into(),
        },
        value_type: novarocks_type_contract::FunctionValueType::new(data_type, false),
    }
}

fn list_type(item_type: DataType) -> DataType {
    DataType::List(Arc::new(Field::new("item", item_type, true)))
}

/// Create a scan group whose logical_props report the given output columns.
fn mk_scan_group(memo: &mut Memo, col_names: &[&str]) -> usize {
    let output_columns: Vec<OutputColumn> = col_names
        .iter()
        .map(|name| OutputColumn {
            column_id: test_col_id(name),
            name: (*name).into(),
            value_type: novarocks_type_contract::FunctionValueType::new(DataType::Int32, false),

            is_internal: false,
        })
        .collect();
    let scan_mexpr = MExpr {
        id: memo.next_expr_id(),
        op: Operator::LogicalScan(ScanOp {
            database: "db".into(),
            table: TableDef {
                name: "t".into(),
                columns: vec![],
                iceberg_row_lineage_metadata_columns: vec![],
                source: crate::compiler::mv_rewrite::test_scan_source(
                    crate::planner::table::SqlScanKind::ConnectorRead,
                ),
            },
            alias: None,
            stats_ref: None,
            columns: output_columns.clone(),
            predicates: vec![],
            required_columns: None,
            variant_columns: vec![],
            mv_rewritten_from: None,
        }),
        children: vec![],
    };
    let gid = memo.new_group(scan_mexpr);
    // Inject logical_props so side ownership can be derived from ColumnId.
    memo.groups[gid].logical_props = Some(LogicalProperties::new(output_columns, 100.0));
    gid
}

/// Build `left op right` as a TypedExpr.
fn bin(left: TypedExpr, op: BinOp, right: TypedExpr) -> TypedExpr {
    TypedExpr {
        kind: ExprKind::BinaryOp {
            left: novarocks_parser::ast::AstBox::new(left),
            op,
            right: novarocks_parser::ast::AstBox::new(right),
            decimal_overflow_policy: novarocks_type_contract::DecimalOverflowPolicy::OutputNull,
        },
        value_type: novarocks_type_contract::FunctionValueType::new(DataType::Boolean, false),
    }
}

fn logical_join_expr(
    memo: &mut Memo,
    join_type: JoinKind,
    condition: TypedExpr,
    children: Vec<GroupId>,
) -> MExpr {
    let id = memo.next_expr_id();
    let condition = Some(
        intern_typed(
            &mut memo.scalars,
            &condition,
            &crate::compiler::SqlCompileControl::unbounded(),
        )
        .unwrap(),
    );
    MExpr {
        id,
        op: Operator::LogicalJoin(LogicalJoinOp {
            naa_author: None,
            join_type,
            condition,
        }),
        children,
    }
}

fn mat(memo: &Memo, expr: ScalarId) -> TypedExpr {
    materialize(&memo.scalars, expr)
}

fn assert_col_id(expr: &TypedExpr, expected: ColumnId, message: &str) {
    match &expr.kind {
        ExprKind::ColumnRef { column_id, .. } => assert_eq!(*column_id, expected, "{message}"),
        other => panic!("expected ColumnRef for {message}, got {:?}", other),
    }
}

/// The full demotion path: a same-side eq pair must land in other_condition
/// while an orientable pair lands (correctly oriented) in eq_conditions.
#[test]
fn demoted_single_side_pair_ends_in_other_condition() {
    let mut memo = Memo::new();

    // Left side: columns [a_id, a_name].  Right side: column [b_id].
    let left_group = mk_scan_group(&mut memo, &["a_id", "a_name"]);
    let right_group = mk_scan_group(&mut memo, &["b_id"]);

    // Condition: (a_id = b_id) AND (a_id = a_name)
    //   • First pair  (a_id, b_id)  — orientable (a_id left, b_id right).
    //   • Second pair (a_id, a_name) — same-side (both left) → must demote.
    let first_eq = bin(col("a_id"), BinOp::Eq, col("b_id"));
    let second_eq = bin(col("a_id"), BinOp::Eq, col("a_name"));
    let condition = bin(first_eq, BinOp::And, second_eq);

    let join_mexpr = logical_join_expr(
        &mut memo,
        JoinKind::Inner,
        condition,
        vec![left_group, right_group],
    );

    let rule = JoinToHashJoin;
    let alternatives = rule
        .apply(
            &join_mexpr,
            &mut memo,
            &crate::compiler::SqlCompileControl::unbounded(),
        )
        .unwrap();

    assert_eq!(alternatives.len(), 1);

    let alt = &alternatives[0];
    let Operator::PhysicalHashJoin(phys) = &alt.op else {
        panic!("expected PhysicalHashJoin, got {:?}", alt.op);
    };

    // ── eq_conditions: exactly one pair, (a_id, b_id) ──────────────────
    assert_eq!(
        phys.condition.keys().len(),
        1,
        "expected 1 eq pair in eq_conditions, got {:?}",
        phys.condition.keys()
    );
    let eq = &phys.condition.keys()[0];
    assert!(
        !eq.null_safe,
        "regular equality should not be marked null-safe"
    );
    let lhs = mat(&memo, eq.left);
    let rhs = mat(&memo, eq.right);
    assert_col_id(
        &lhs,
        test_col_id("a_id"),
        "left side of eq_condition should be a_id",
    );
    assert_col_id(
        &rhs,
        test_col_id("b_id"),
        "right side of eq_condition should be b_id",
    );

    // ── other_condition: the demoted (a_id = a_name) pair ───────────────
    let other = phys
        .condition
        .residual()
        .as_ref()
        .expect("demoted same-side pair must appear in other_condition");
    let other = mat(&memo, *other);
    match &other.kind {
        ExprKind::BinaryOp {
            left, op, right, ..
        } => {
            assert!(
                matches!(op, BinOp::Eq),
                "demoted condition should be BinaryOp::Eq, got {:?}",
                op
            );
            match (&left.kind, &right.kind) {
                (
                    ExprKind::ColumnRef { column_id: l, .. },
                    ExprKind::ColumnRef { column_id: r, .. },
                ) => {
                    let a_id = test_col_id("a_id");
                    let a_name = test_col_id("a_name");
                    assert!(
                        (*l == a_id && *r == a_name) || (*l == a_name && *r == a_id),
                        "expected (a_id, a_name) in demoted eq, got ({}, {})",
                        l,
                        r
                    );
                }
                other => panic!(
                    "expected two ColumnRef nodes inside demoted eq, got {:?}",
                    other
                ),
            }
        }
        other => panic!("expected BinaryOp::Eq in other_condition, got {:?}", other),
    }
}

#[test]
fn null_safe_join_pair_stays_hash_join_key() {
    let mut memo = Memo::new();
    let left_group = mk_scan_group(&mut memo, &["a_id"]);
    let right_group = mk_scan_group(&mut memo, &["b_id"]);

    let condition = bin(col("a_id"), BinOp::EqForNull, col("b_id"));
    let join_mexpr = logical_join_expr(
        &mut memo,
        JoinKind::Inner,
        condition,
        vec![left_group, right_group],
    );

    let rule = JoinToHashJoin;
    let alternatives = rule
        .apply(
            &join_mexpr,
            &mut memo,
            &crate::compiler::SqlCompileControl::unbounded(),
        )
        .unwrap();
    assert_eq!(alternatives.len(), 1);
    let Operator::PhysicalHashJoin(phys) = &alternatives[0].op else {
        panic!("expected PhysicalHashJoin, got {:?}", alternatives[0].op);
    };
    assert_eq!(phys.condition.keys().len(), 1);
    assert!(
        phys.condition.keys()[0].null_safe,
        "<=> hash join key must retain null-safe semantics"
    );
    assert!(
        phys.condition.residual().is_none(),
        "<=> should not be left as a residual-only predicate"
    );
}

#[test]
fn mixed_integer_eq_pair_keeps_raw_hash_key_for_distribution() {
    let mut memo = Memo::new();
    let left_group = mk_scan_group(&mut memo, &["a_id"]);
    let right_group = mk_scan_group(&mut memo, &["b_id"]);

    let condition = bin(
        col_typed("a_id", DataType::Int64),
        BinOp::Eq,
        col_typed("b_id", DataType::Int32),
    );
    let join_mexpr = logical_join_expr(
        &mut memo,
        JoinKind::Inner,
        condition,
        vec![left_group, right_group],
    );

    let alternatives = JoinToHashJoin
        .apply(
            &join_mexpr,
            &mut memo,
            &crate::compiler::SqlCompileControl::unbounded(),
        )
        .unwrap();
    assert_eq!(
        alternatives.len(),
        1,
        "mixed integer equality should remain hash-joinable"
    );
    let Operator::PhysicalHashJoin(phys) = &alternatives[0].op else {
        panic!("expected PhysicalHashJoin, got {:?}", alternatives[0].op);
    };
    assert_eq!(phys.condition.keys().len(), 1);
    let left_key = mat(&memo, phys.condition.keys()[0].left);
    let right_key = mat(&memo, phys.condition.keys()[0].right);
    assert_eq!(left_key.value_type.data_type().clone(), DataType::Int64);
    assert_eq!(right_key.value_type.data_type().clone(), DataType::Int32);
    assert!(
        matches!(left_key.kind, ExprKind::ColumnRef { .. }),
        "optimizer hash key should keep raw column refs so distribution can enforce both sides"
    );
    assert!(
        matches!(right_key.kind, ExprKind::ColumnRef { .. }),
        "optimizer hash key should keep raw column refs so distribution can enforce both sides"
    );
    assert!(
        phys.condition.residual().is_none(),
        "compatible scalar equality should not be demoted"
    );
}

#[test]
fn cross_type_array_pair_has_no_hash_join_alternative() {
    for comparison in [BinOp::Eq, BinOp::EqForNull] {
        assert_cross_type_array_pair_has_no_hash_join_alternative(comparison);
    }
}

fn assert_cross_type_array_pair_has_no_hash_join_alternative(comparison: BinOp) {
    let mut memo = Memo::new();
    let left_group = mk_scan_group(&mut memo, &["a_arr"]);
    let right_group = mk_scan_group(&mut memo, &["b_arr"]);
    let condition = bin(
        col_typed("a_arr", list_type(DataType::Utf8)),
        comparison,
        col_typed("b_arr", list_type(DataType::Int64)),
    );
    let join_mexpr = logical_join_expr(
        &mut memo,
        JoinKind::Inner,
        condition,
        vec![left_group, right_group],
    );

    let hash_alternatives = JoinToHashJoin
        .apply(
            &join_mexpr,
            &mut memo,
            &crate::compiler::SqlCompileControl::unbounded(),
        )
        .unwrap();
    assert!(
        hash_alternatives.is_empty(),
        "cross-type complex equality must not become a raw hash key"
    );

    let nested_alternatives = JoinToNestLoop
        .apply(
            &join_mexpr,
            &mut memo,
            &crate::compiler::SqlCompileControl::unbounded(),
        )
        .unwrap();
    assert_eq!(
        nested_alternatives.len(),
        1,
        "nested loop must remain available for complex-only equality"
    );
    assert!(matches!(
        nested_alternatives[0].op,
        Operator::PhysicalNestLoopJoin(_)
    ));
}

#[test]
fn complex_pair_demotes_when_scalar_hash_key_remains() {
    for comparison in [BinOp::Eq, BinOp::EqForNull] {
        assert_complex_pair_demotes_when_scalar_hash_key_remains(comparison);
    }
}

fn assert_complex_pair_demotes_when_scalar_hash_key_remains(comparison: BinOp) {
    let mut memo = Memo::new();
    let left_group = mk_scan_group(&mut memo, &["a_id", "a_arr"]);
    let right_group = mk_scan_group(&mut memo, &["b_id", "b_arr"]);
    let scalar_eq = bin(col("a_id"), BinOp::Eq, col("b_id"));
    use novarocks_functions::{FunctionArgument, FunctionBindingRequest};
    use novarocks_type_contract::OperatorFunction;

    // Composite comparison is authored by the installed operator. The raw
    // binary comparison primitive does not support this value domain.
    let operands = [
        col_typed("a_arr", list_type(DataType::Utf8)),
        col_typed("b_arr", list_type(DataType::Utf8)),
    ];
    let control = crate::optimizer::test_optimizer_control();
    let arguments = operands.each_ref().map(|operand| FunctionArgument::Value {
        value_type: operand.value_type.clone(),
        constant: None,
    });
    let operator = match comparison {
        BinOp::Eq => OperatorFunction::Equal,
        BinOp::EqForNull => OperatorFunction::Identical,
        _ => unreachable!(),
    };
    let resolved = crate::functions::builtin_sql_function_catalog()
        .resolve_scalar_operator_binding(
            operator,
            FunctionBindingRequest {
                arguments: &arguments,
                logical_argument_count: arguments.len(),
                expected_result_type: None,
            },
            control,
        )
        .unwrap();
    let complex_eq = crate::common::binary_operator::typed_call(
        operator,
        crate::binding::SqlFunctionBinding::from_test(
            resolved,
            novarocks_type_contract::DecimalOverflowPolicy::OutputNull,
        ),
        operands.into(),
    );
    let condition = bin(scalar_eq, BinOp::And, complex_eq);
    let join_mexpr = logical_join_expr(
        &mut memo,
        JoinKind::Inner,
        condition,
        vec![left_group, right_group],
    );

    let alternatives = JoinToHashJoin
        .apply(
            &join_mexpr,
            &mut memo,
            &crate::compiler::SqlCompileControl::unbounded(),
        )
        .unwrap();
    assert_eq!(alternatives.len(), 1);
    let Operator::PhysicalHashJoin(phys) = &alternatives[0].op else {
        panic!("expected PhysicalHashJoin, got {:?}", alternatives[0].op);
    };
    assert_eq!(
        phys.condition.keys().len(),
        1,
        "only the scalar equality should remain as a hash key"
    );
    let left_key = mat(&memo, phys.condition.keys()[0].left);
    assert_col_id(&left_key, test_col_id("a_id"), "scalar hash key on left");
    assert!(
        phys.condition.residual().is_some(),
        "complex equality must survive as a residual predicate"
    );
}

#[test]
fn join_to_hash_join_emits_one_property_driven_hash_join() {
    let mut memo = Memo::new();
    let left_group = mk_scan_group(&mut memo, &["a_id"]);
    let right_group = mk_scan_group(&mut memo, &["b_id"]);
    let condition = bin(col("a_id"), BinOp::Eq, col("b_id"));
    let expr = logical_join_expr(
        &mut memo,
        JoinKind::Inner,
        condition,
        vec![left_group, right_group],
    );
    let rule = JoinToHashJoin;
    let alternatives = rule
        .apply(
            &expr,
            &mut memo,
            &crate::compiler::SqlCompileControl::unbounded(),
        )
        .unwrap();

    assert_eq!(alternatives.len(), 1);
    let Operator::PhysicalHashJoin(phys) = &alternatives[0].op else {
        panic!("expected PhysicalHashJoin, got {:?}", alternatives[0].op);
    };
    assert!(matches!(phys.distribution, JoinDistribution::Unknown));
}

fn bind_call(name: &str, args: Vec<TypedExpr>) -> TypedExpr {
    let resolved = crate::analysis::select_scalar_function(
        crate::functions::builtin_sql_function_catalog(),
        name,
        &args,
        crate::constant::test_constant_policy(),
        crate::optimizer::test_optimizer_control(),
    )
    .unwrap();
    let novarocks_functions::FunctionResultType::Scalar(value_type) =
        &resolved.selected.result_type
    else {
        panic!("scalar test function");
    };
    TypedExpr {
        value_type: value_type.clone(),
        kind: ExprKind::FunctionCall {
            volatility: resolved.semantics.volatility,
            binding: crate::binding::SqlFunctionBinding::from_test(
                resolved,
                novarocks_type_contract::DecimalOverflowPolicy::OutputNull,
            ),
            name: name.into(),
            args: args.into(),
            distinct: false,
        },
    }
}

fn int(value: i64) -> TypedExpr {
    TypedExpr {
        kind: ExprKind::Literal(crate::analysis::LiteralValue::Int(value)),
        value_type: novarocks_type_contract::FunctionValueType::new(DataType::Int32, false),
    }
}

fn checked_divide(left: TypedExpr, right: TypedExpr) -> TypedExpr {
    use novarocks_type_contract::{ArithmeticOperator, arithmetic_result_value_type_with_op};
    let value_type = arithmetic_result_value_type_with_op(
        &left.value_type,
        &right.value_type,
        ArithmeticOperator::Divide,
    )
    .unwrap()
    .with_nullable(true)
    .unwrap();
    let mut division = bin(left, BinOp::Div, right);
    division.value_type = value_type;
    division
}

#[test]
fn only_source_order_safe_equalities_become_hash_keys() {
    let mut memo = Memo::new();
    let left_group = mk_scan_group(&mut memo, &["a_id", "a_name"]);
    let right_group = mk_scan_group(&mut memo, &["b_id"]);
    let divide = checked_divide(int(12), col("a_name"));
    let mut checked = bin(divide, BinOp::Eq, col("b_id"));
    checked.value_type = checked.value_type.with_nullable(true).unwrap();
    let first_key = bin(col("a_id"), BinOp::Eq, col("b_id"));
    let later_key = bin(col("a_name"), BinOp::Eq, col("b_id"));
    let total_residual = bin(col("a_id"), BinOp::Gt, int(0));
    let join = logical_join_expr(
        &mut memo,
        JoinKind::Inner,
        bin(
            bin(total_residual.clone(), BinOp::And, first_key.clone()),
            BinOp::And,
            bin(checked.clone(), BinOp::And, later_key.clone()),
        ),
        vec![left_group, right_group],
    );
    let control = crate::optimizer::test_optimizer_control();
    let expected = [total_residual, checked.clone(), later_key]
        .map(|part| intern_typed(&mut memo.scalars, &part, control).unwrap());
    assert!(
        memo.scalars
            .effects(expected[0])
            .can_skip_for_earlier_filter()
    );
    let checked_effects = memo.scalars.effects(expected[1]);
    assert!(checked_effects.permits_filter_row_elimination);
    assert!(checked_effects.expression.may_raise_row_error);
    assert!(!checked_effects.expression.may_raise_invocation_data);
    let alt = JoinToHashJoin.apply(&join, &mut memo, control).unwrap();
    let [
        NewExpr {
            op: Operator::PhysicalHashJoin(hash),
            ..
        },
    ] = alt.as_slice()
    else {
        panic!("expected the source-order-safe hash key");
    };
    assert_eq!(hash.condition.keys().len(), 1);
    let mut actual = Vec::new();
    scalar_expr::split_conjuncts(
        &memo.scalars,
        *hash
            .condition
            .residual()
            .as_ref()
            .expect("ordered residual"),
        &mut actual,
    );
    assert_eq!(actual, expected);
    assert!(
        JoinToNestLoop
            .apply(&join, &mut memo, control)
            .unwrap()
            .is_empty()
    );

    let bad_first = logical_join_expr(
        &mut memo,
        JoinKind::Inner,
        bin(checked, BinOp::And, first_key),
        vec![left_group, right_group],
    );
    assert!(
        JoinToHashJoin
            .apply(&bad_first, &mut memo, control)
            .unwrap()
            .is_empty()
    );
    assert_original_nested_loop_condition(&bad_first, &mut memo);

    memo.hash_join_enabled = false;
    assert_original_nested_loop_condition(&join, &mut memo);
}

fn assert_original_nested_loop_condition(join: &MExpr, memo: &mut Memo) {
    let alt = JoinToNestLoop
        .apply(join, memo, crate::optimizer::test_optimizer_control())
        .unwrap();
    let [
        NewExpr {
            op: Operator::PhysicalNestLoopJoin(nlj),
            ..
        },
    ] = alt.as_slice()
    else {
        panic!("expected the original nested loop condition");
    };
    let Operator::LogicalJoin(original) = &join.op else {
        unreachable!()
    };
    assert_eq!(nlj.condition, original.condition);
}

#[test]
fn join_commutativity_preserves_required_pair_evaluation_order() {
    let mut memo = Memo::new();
    let left = mk_scan_group(&mut memo, &["a_id"]);
    let right = mk_scan_group(&mut memo, &["b_id"]);
    let control = crate::optimizer::test_optimizer_control();
    let equality = bin(col("a_id"), BinOp::Eq, col("b_id"));
    let safe = logical_join_expr(
        &mut memo,
        JoinKind::Inner,
        equality.clone(),
        vec![left, right],
    );
    let alternatives = JoinCommutativity.apply(&safe, &mut memo, control).unwrap();
    let [commuted] = alternatives.as_slice() else {
        panic!("a total equality can commute");
    };
    assert_eq!(commuted.children, [right, left]);

    let divide = checked_divide(int(12), col("a_id"));
    let mut checked = bin(divide, BinOp::Gt, int(3));
    checked.value_type = checked.value_type.with_nullable(true).unwrap();
    let checked_id = intern_typed(&mut memo.scalars, &checked, control).unwrap();
    let checked_effects = memo.scalars.effects(checked_id);
    assert!(checked_effects.permits_filter_row_elimination);
    assert!(checked_effects.expression.may_raise_row_error);
    let join = logical_join_expr(
        &mut memo,
        JoinKind::Inner,
        bin(checked, BinOp::And, equality),
        vec![left, right],
    );
    assert!(
        JoinCommutativity
            .apply(&join, &mut memo, control)
            .unwrap()
            .is_empty()
    );
}

#[test]
fn independently_evaluated_rand_terms_keep_the_original_join_condition() {
    let mut memo = Memo::new();
    let left = mk_scan_group(&mut memo, &["a_id"]);
    let right = mk_scan_group(&mut memo, &["b_id"]);
    let random = bind_call("rand", Vec::new());
    let bound = TypedExpr {
        kind: ExprKind::Literal(crate::analysis::LiteralValue::Float(0.5)),
        value_type: novarocks_type_contract::FunctionValueType::new(DataType::Float64, false),
    };
    let random_filter = bin(random, BinOp::Lt, bound);
    let equality = bin(col("a_id"), BinOp::Eq, col("b_id"));
    let condition = bin(
        random_filter.clone(),
        BinOp::And,
        bin(equality, BinOp::And, random_filter),
    );
    let expr = logical_join_expr(&mut memo, JoinKind::Inner, condition, vec![left, right]);
    let control = crate::optimizer::test_optimizer_control();
    let Operator::LogicalJoin(original) = &expr.op else {
        unreachable!()
    };
    let original_condition = original.condition.unwrap();
    assert!(
        memo.scalars
            .effects(original_condition)
            .expression
            .observable_effects
            .rng_sampling,
        "use a genuinely installed RNG overload"
    );
    assert!(
        JoinToHashJoin
            .apply(&expr, &mut memo, control)
            .unwrap()
            .is_empty()
    );
    assert!(
        JoinCommutativity
            .apply(&expr, &mut memo, control)
            .unwrap()
            .is_empty()
    );
    assert_original_nested_loop_condition(&expr, &mut memo);
    let mut parts = Vec::new();
    scalar_expr::split_conjuncts(&memo.scalars, original_condition, &mut parts);
    assert_eq!(parts.len(), 3);
    assert_eq!(
        parts[0], parts[2],
        "interning must not elide either occurrence"
    );
}
