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

//! Necessary-filter properties at the real typed derivation boundary.
use super::*;
use novarocks_type_contract::{
    CompileControlError, CompilePhase, FunctionValueType, PureCompileControl,
};

fn control() -> &'static dyn PureCompileControl {
    crate::optimizer::rewrite::context::unbounded_rewrite_test_control()
}
fn intern(arena: &mut ScalarArena, node: ScalarNode, ty: FunctionValueType) -> ScalarId {
    arena.intern_observed(node, ty, control()).unwrap()
}
fn column(arena: &mut ScalarArena, id: u32, dtype: DataType, nullable: bool) -> ScalarId {
    intern(
        arena,
        ScalarNode::ColumnRef(ColumnId::new_for_test(id)),
        FunctionValueType::new(dtype, nullable),
    )
}
fn cast(arena: &mut ScalarArena, child: ScalarId, target: DataType) -> ScalarId {
    let ty = FunctionValueType::new(target.clone(), arena.nullable(child));
    intern(
        arena,
        ScalarNode::Cast {
            cast_mode: None,
            child,
            target,
            decimal_overflow_policy: novarocks_type_contract::DecimalOverflowPolicy::OutputNull,
        },
        ty,
    )
}
fn bound(arena: &mut ScalarArena, value: i64, dtype: DataType) -> ScalarId {
    intern(
        arena,
        ScalarNode::Literal(HashableLiteral(LiteralValue::Int(value))),
        FunctionValueType::new(dtype, false),
    )
}
fn binary(
    arena: &mut ScalarArena,
    left: ScalarId,
    op: impl Into<crate::common::binary_operator::TestOperator>,
    right: ScalarId,
) -> ScalarId {
    arena.test_intern_operator(
        op,
        left,
        right,
        novarocks_type_contract::DecimalOverflowPolicy::OutputNull,
    )
}
fn between(
    arena: &mut ScalarArena,
    child: ScalarId,
    low: i64,
    high: i64,
    dtype: DataType,
) -> ScalarId {
    let low = bound(arena, low, dtype.clone());
    let high = bound(arena, high, dtype);
    let lower = binary(arena, low, BinOp::Le, child);
    let upper = binary(arena, child, BinOp::Le, high);
    binary(
        arena,
        lower,
        crate::common::BooleanOp::ExpandedConjunction,
        upper,
    )
}
fn branches(
    arena: &mut ScalarArena,
    left: ScalarId,
    right: ScalarId,
    first: ScalarId,
    second: ScalarId,
) -> PredicateGroup {
    let eq = binary(arena, left, BinOp::Eq, right);
    let first = binary(arena, eq, crate::common::BooleanOp::And, first);
    let second = binary(arena, eq, crate::common::BooleanOp::And, second);
    let expr = binary(arena, first, crate::common::BooleanOp::Or, second);
    PredicateGroup::new(
        arena,
        expr,
        PredicateOrigin::Filter,
        PredicateDerivedKind::None,
    )
}
fn derive(
    arena: &mut ScalarArena,
    group: PredicateGroup,
    owner: &dyn PureCompileControl,
) -> Result<Vec<PredicateGroup>, crate::compiler::SqlCompileError> {
    derive_inner_join_predicates(
        crate::functions::builtin_sql_function_catalog(),
        arena,
        &HashSet::from([ColumnId::new_for_test(1)]),
        &HashSet::from([ColumnId::new_for_test(2)]),
        &[],
        &[group],
        owner,
    )
}
fn envelope_axes(
    arena: &ScalarArena,
    derived: &[PredicateGroup],
    source: &DataType,
    target: &DataType,
) -> Vec<ColumnId> {
    let mut axes = Vec::new();
    for group in derived
        .iter()
        .filter(|group| group.derived == PredicateDerivedKind::RangeEnvelope)
    {
        let Some((child, low, high)) =
            crate::common::binary_operator::scalar_inclusive_range(arena, group.expr)
        else {
            panic!("necessary interval")
        };
        assert!(matches!(
            arena.node(low),
            ScalarNode::Literal(HashableLiteral(LiteralValue::Int(10)))
        ));
        assert!(matches!(
            arena.node(high),
            ScalarNode::Literal(HashableLiteral(LiteralValue::Int(40)))
        ));
        let ScalarNode::Cast {
            cast_mode: None,
            child: base,
            target: actual_target,
            decimal_overflow_policy,
        } = arena.node(child)
        else {
            panic!("original widening comparison remains")
        };
        assert_eq!(actual_target, target);
        assert_eq!(
            *decimal_overflow_policy,
            novarocks_type_contract::DecimalOverflowPolicy::OutputNull
        );
        assert_eq!(arena.data_type(*base), source);
        assert_eq!(arena.data_type(child), target);
        assert!(arena.nullable(*base) && arena.nullable(child));
        let ScalarNode::ColumnRef(id) = arena.node(*base) else {
            panic!("exact base column")
        };
        axes.push(*id);
    }
    axes.sort();
    axes
}
#[test]
fn every_total_signed_widening_preserves_original_comparison_and_both_necessary_axes() {
    for (source, target) in [
        (DataType::Int8, DataType::Int16),
        (DataType::Int8, DataType::Int32),
        (DataType::Int8, DataType::Int64),
        (DataType::Int16, DataType::Int32),
        (DataType::Int16, DataType::Int64),
        (DataType::Int32, DataType::Int64),
    ] {
        let mut arena = ScalarArena::new();
        let left = column(&mut arena, 1, source.clone(), true);
        let right = column(&mut arena, 2, source.clone(), true);
        let comparison = cast(&mut arena, left, target.clone());
        let first = between(&mut arena, comparison, 10, 20, target.clone());
        let second = between(&mut arena, comparison, 30, 40, target.clone());
        let original = branches(&mut arena, left, right, first, second);
        let original_id = original.expr;
        let result = derive(&mut arena, original, control()).unwrap();
        assert_eq!(
            envelope_axes(&arena, &result, &source, &target),
            vec![ColumnId::new_for_test(1), ColumnId::new_for_test(2)]
        );
        assert!(matches!(
            arena.node(original_id),
            ScalarNode::BooleanConnective {
                op: crate::common::BooleanOp::Or,
                ..
            }
        ));
    }
}
#[test]
fn unequal_source_nullability_cannot_authorize_the_widened_template_on_the_other_axis() {
    let mut arena = ScalarArena::new();
    let left = column(&mut arena, 1, DataType::Int32, true);
    let right = column(&mut arena, 2, DataType::Int32, false);
    let comparison = cast(&mut arena, left, DataType::Int64);
    let first = between(&mut arena, comparison, 10, 20, DataType::Int64);
    let second = between(&mut arena, comparison, 30, 40, DataType::Int64);
    let original = branches(&mut arena, left, right, first, second);
    let result = derive(&mut arena, original, control()).unwrap();
    assert_eq!(
        envelope_axes(&arena, &result, &DataType::Int32, &DataType::Int64),
        vec![ColumnId::new_for_test(1)],
        "same carrier does not supply the other axis's full FVT"
    );
}
#[test]
fn one_base_column_in_two_valid_comparison_domains_does_not_merge_branch_bounds() {
    let mut arena = ScalarArena::new();
    let left = column(&mut arena, 1, DataType::Int8, true);
    let right = column(&mut arena, 2, DataType::Int8, true);
    let first_domain = cast(&mut arena, left, DataType::Int16);
    let second_domain = cast(&mut arena, left, DataType::Int64);
    let first = between(&mut arena, first_domain, 10, 20, DataType::Int16);
    let second = between(&mut arena, second_domain, 30, 40, DataType::Int64);
    let original = branches(&mut arena, left, right, first, second);
    let result = derive(&mut arena, original, control()).unwrap();
    assert!(
        result
            .iter()
            .all(|group| group.derived != PredicateDerivedKind::RangeEnvelope),
        "neither source ID nor same carrier family unifies original comparison domains"
    );
}
#[test]
fn bounds_outside_the_original_complete_comparison_binding_are_not_retyped() {
    let mut arena = ScalarArena::new();
    let left = column(&mut arena, 1, DataType::Int32, true);
    let comparison = cast(&mut arena, left, DataType::Int64);
    // Both values can be admitted independently, but the bound transfer has
    // no original conversion from those values into this comparison.
    let low = bound(&mut arena, 10, DataType::Int32);
    let high = bound(&mut arena, 20, DataType::Int32);
    let constraint = ColumnConstraint {
        column: comparison,
        kind: ConstraintKind::Between { low, high },
    };
    assert!(!bounds_match_operand(&arena, &constraint, control()).unwrap());
    assert!(
        substitute_constraint_column(
            crate::functions::builtin_sql_function_catalog(),
            &mut arena,
            &constraint,
            comparison,
            PredicateDerivedKind::RangeEnvelope,
            control(),
        )
        .unwrap()
        .is_none()
    );
}
#[test]
fn comparison_control_failure_aborts_instead_of_becoming_an_optimization_decline() {
    struct Stop(CompileControlError);
    impl PureCompileControl for Stop {
        fn checkpoint(&self, _: CompilePhase, _: u32) -> Result<(), CompileControlError> {
            Err(self.0.clone())
        }
    }
    for (cause, expected) in [
        (
            CompileControlError::Cancelled,
            crate::compiler::SqlCompileError::Cancelled,
        ),
        (
            CompileControlError::DeadlineExceeded,
            crate::compiler::SqlCompileError::DeadlineExceeded,
        ),
        (
            CompileControlError::ResourceExhausted,
            crate::compiler::SqlCompileError::ResourceExhausted,
        ),
    ] {
        let mut arena = ScalarArena::new();
        let left = column(&mut arena, 1, DataType::Int32, true);
        let right = column(&mut arena, 2, DataType::Int32, true);
        let comparison = cast(&mut arena, left, DataType::Int64);
        let first = between(&mut arena, comparison, 10, 20, DataType::Int64);
        let second = between(&mut arena, comparison, 30, 40, DataType::Int64);
        let original = branches(&mut arena, left, right, first, second);
        assert_eq!(
            derive(&mut arena, original, &Stop(cause)).unwrap_err(),
            expected
        );
    }
}
