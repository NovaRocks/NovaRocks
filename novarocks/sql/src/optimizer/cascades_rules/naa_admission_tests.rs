use super::*;
use crate::common::naa::NaaAuthorFacts;
use novarocks_type_contract::{DecimalOverflowPolicy, FunctionValueType};

fn intern(arena: &mut ScalarArena, node: ScalarNode, ty: DataType, nullable: bool) -> ScalarId {
    arena
        .intern_observed(
            node,
            FunctionValueType::new(ty, nullable),
            &crate::compiler::SqlCompileControl::unbounded(),
        )
        .unwrap()
}
fn column(arena: &mut ScalarArena, id: u32, nullable: bool) -> ScalarId {
    intern(
        arena,
        ScalarNode::ColumnRef(ColumnId::new_for_test(id)),
        DataType::Int32,
        nullable,
    )
}
fn binary(
    arena: &mut ScalarArena,
    left: ScalarId,
    op: BinOp,
    right: ScalarId,
    ty: DataType,
    nullable: bool,
) -> ScalarId {
    intern(
        arena,
        ScalarNode::BinaryOp {
            left,
            op,
            right,
            decimal_overflow_policy: DecimalOverflowPolicy::OutputNull,
        },
        ty,
        nullable,
    )
}
fn comparison(arena: &mut ScalarArena) -> ScalarId {
    let left = column(arena, 1, true);
    let right = column(arena, 2, true);
    binary(arena, left, BinOp::Eq, right, DataType::Boolean, true)
}
fn scope(arena: &mut ScalarArena, nullable: bool, null_safe: bool) -> ScalarId {
    let left = column(arena, 3, nullable);
    let right = column(arena, 4, nullable);
    binary(
        arena,
        left,
        if null_safe {
            BinOp::EqForNull
        } else {
            BinOp::Eq
        },
        right,
        DataType::Boolean,
        nullable && !null_safe,
    )
}
fn throwing(arena: &mut ScalarArena) -> ScalarId {
    let left = intern(
        arena,
        ScalarNode::ColumnRef(ColumnId::new_for_test(5)),
        DataType::Decimal128(9, 0),
        false,
    );
    let right = intern(
        arena,
        ScalarNode::ColumnRef(ColumnId::new_for_test(6)),
        DataType::Decimal128(9, 0),
        false,
    );
    let product = binary(
        arena,
        left,
        BinOp::Mul,
        right,
        DataType::Decimal128(9, 0),
        false,
    );
    binary(arena, product, BinOp::Eq, left, DataType::Boolean, false)
}
fn admit(arena: &mut ScalarArena, op: &LogicalJoinOp) -> Option<ScalarJoinCondition> {
    null_aware_hash_condition(
        op,
        &[1, 3, 5].into_iter().map(ColumnId::new_for_test).collect(),
        &[2, 4, 6].into_iter().map(ColumnId::new_for_test).collect(),
        arena,
        &crate::compiler::SqlCompileControl::unbounded(),
    )
    .unwrap()
}
fn authored(
    arena: &mut ScalarArena,
    comparison: ScalarId,
    eligibility: Option<ScalarId>,
) -> LogicalJoinOp {
    let predicate = eligibility
        .map(|e| binary(arena, comparison, BinOp::And, e, DataType::Boolean, true))
        .unwrap_or(comparison);
    LogicalJoinOp {
        join_type: JoinKind::NullAwareLeftAnti,
        condition: Some(predicate),
        naa_author: Some(NaaAuthorFacts {
            comparison,
            eligibility,
        }),
    }
}

#[test]
fn missing_author_is_not_reconstructed_from_orientable_equalities() {
    let mut arena = ScalarArena::new();
    let c = comparison(&mut arena);
    let op = LogicalJoinOp {
        join_type: JoinKind::NullAwareLeftAnti,
        condition: Some(c),
        naa_author: None,
    };
    assert!(admit(&mut arena, &op).is_none());
}

#[test]
fn scope_and_value_keep_distinct_roles_and_equality_modes() {
    for null_safe in [false, true] {
        let mut arena = ScalarArena::new();
        let c = comparison(&mut arena);
        let e = scope(&mut arena, true, null_safe);
        let op = authored(&mut arena, c, Some(e));
        let condition = admit(&mut arena, &op).unwrap();
        let novarocks_physical_plan::HashJoinCondition::NullAwareEqual {
            value_equal_keys,
            scope_keys,
            eligibility,
        } = condition
        else {
            panic!("authored NAA must keep its closed condition")
        };
        assert_eq!(value_equal_keys.len(), 1);
        assert_eq!(scope_keys.len(), 1);
        assert!(!value_equal_keys[0].null_safe);
        assert_eq!(scope_keys[0].null_safe, null_safe);
        assert!(eligibility.is_none());
        assert_ne!(value_equal_keys[0].left, scope_keys[0].left);
    }
}

#[test]
fn nullable_scope_before_required_error_stays_in_value_demand_eligibility() {
    let mut arena = ScalarArena::new();
    let c = comparison(&mut arena);
    let s = scope(&mut arena, true, false);
    let r = throwing(&mut arena);
    assert!(scalar_expr::can_fail(&arena, r));
    let e = binary(&mut arena, s, BinOp::And, r, DataType::Boolean, true);
    let op = authored(&mut arena, c, Some(e));
    let condition = admit(&mut arena, &op).unwrap();
    assert!(condition.scope_keys().unwrap().is_empty());
    assert_eq!(*condition.residual(), Some(e));
}

#[test]
fn nonnullable_safe_scope_prefix_can_exclude_without_skipping_required_error() {
    for null_safe in [false, true] {
        let mut arena = ScalarArena::new();
        let c = comparison(&mut arena);
        let s = scope(&mut arena, null_safe, null_safe);
        let r = throwing(&mut arena);
        let e = binary(&mut arena, s, BinOp::And, r, DataType::Boolean, false);
        let op = authored(&mut arena, c, Some(e));
        let condition = admit(&mut arena, &op).unwrap();
        assert_eq!(condition.scope_keys().unwrap().len(), 1);
        assert_eq!(*condition.residual(), Some(r));
    }
}

#[test]
fn comparison_must_remain_the_original_guard_prefix() {
    let mut arena = ScalarArena::new();
    let c = comparison(&mut arena);
    let e = scope(&mut arena, false, false);
    let predicate = binary(&mut arena, e, BinOp::And, c, DataType::Boolean, true);
    let mut op = authored(&mut arena, c, Some(e));
    op.condition = Some(predicate);
    assert!(admit(&mut arena, &op).is_none());
}

#[test]
fn comparison_operand_cannot_be_hoisted_when_it_can_raise() {
    let mut arena = ScalarArena::new();
    let c = throwing(&mut arena);
    let op = authored(&mut arena, c, None);
    assert!(admit(&mut arena, &op).is_none());
}

#[test]
fn nullable_scope_operands_cannot_be_proved_total_by_boolean_annotation() {
    let mut arena = ScalarArena::new();
    let c = comparison(&mut arena);
    let scope = scope(&mut arena, true, false);
    let node = arena.node(scope).clone();
    // The prefix proof must use the signed operand domains, even when a
    // Boolean result annotation alone cannot establish NULL exclusion.
    let scope = intern(&mut arena, node, DataType::Boolean, false);
    let r = throwing(&mut arena);
    let e = binary(&mut arena, scope, BinOp::And, r, DataType::Boolean, true);
    let op = authored(&mut arena, c, Some(e));
    let admitted = admit(&mut arena, &op).unwrap();
    let novarocks_physical_plan::HashJoinCondition::NullAwareEqual {
        scope_keys,
        eligibility,
        ..
    } = admitted
    else {
        panic!("expected an explicit NAA condition")
    };
    assert!(scope_keys.is_empty());
    assert_eq!(eligibility, Some(e));
}
