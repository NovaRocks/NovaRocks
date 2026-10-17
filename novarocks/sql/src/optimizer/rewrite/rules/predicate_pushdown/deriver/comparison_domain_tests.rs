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
fn binary(arena: &mut ScalarArena, left: ScalarId, op: BinOp, right: ScalarId) -> ScalarId {
    binary_bool(arena, left, op, right, control()).unwrap()
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
    intern(
        arena,
        ScalarNode::Between {
            child,
            low,
            high,
            negated: false,
        },
        FunctionValueType::new(DataType::Boolean, true),
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
    let first = binary(arena, eq, BinOp::And, first);
    let second = binary(arena, eq, BinOp::And, second);
    let expr = binary(arena, first, BinOp::Or, second);
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
        let ScalarNode::Between {
            child,
            low,
            high,
            negated: false,
        } = arena.node(group.expr)
        else {
            panic!("necessary interval")
        };
        assert!(matches!(
            arena.node(*low),
            ScalarNode::Literal(HashableLiteral(LiteralValue::Int(10)))
        ));
        assert!(matches!(
            arena.node(*high),
            ScalarNode::Literal(HashableLiteral(LiteralValue::Int(40)))
        ));
        let ScalarNode::Cast {
            child: base,
            target: actual_target,
            decimal_overflow_policy,
        } = arena.node(*child)
        else {
            panic!("original widening comparison remains")
        };
        assert_eq!(actual_target, target);
        assert_eq!(
            *decimal_overflow_policy,
            novarocks_type_contract::DecimalOverflowPolicy::OutputNull
        );
        assert_eq!(arena.data_type(*base), source);
        assert_eq!(arena.data_type(*child), target);
        assert!(arena.nullable(*base) && arena.nullable(*child));
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
            ScalarNode::BinaryOp { op: BinOp::Or, .. }
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
    let right = column(&mut arena, 2, DataType::Int32, true);
    let comparison = cast(&mut arena, left, DataType::Int64);
    // Each value has a valid binding, but no comparison author cast these
    // Int32 bounds into the operand's original Int64 comparison domain.
    let first = between(&mut arena, comparison, 10, 20, DataType::Int32);
    let second = between(&mut arena, comparison, 30, 40, DataType::Int32);
    let original = branches(&mut arena, left, right, first, second);
    let result = derive(&mut arena, original, control()).unwrap();
    assert!(
        result
            .iter()
            .all(|group| group.derived != PredicateDerivedKind::RangeEnvelope)
    );
}
#[test]
fn comparison_control_failure_aborts_instead_of_becoming_an_optimization_decline() {
    struct Stop(CompileControlError);
    impl PureCompileControl for Stop {
        fn checkpoint(&self, _: CompilePhase, _: u32) -> Result<(), CompileControlError> {
            Err(self.0)
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
