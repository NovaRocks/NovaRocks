//! HashJoin: Shuffle / Broadcast / Colocate.

use crate::sql::column_id::ColumnId;
use crate::sql::optimizer::operator::{
    JoinDistribution, PhysicalHashJoinEqCondition, PhysicalHashJoinOp,
};
use crate::sql::optimizer::property::{
    DistributionSpec, HashSource, OrderingSpec, PhysicalPropertySet,
};
use crate::sql::optimizer::scalar::ScalarArena;
use arrow::datatypes::DataType;

use super::{
    ChildRequirementAlternative, DeriveOutput, DeriveRequired, PropertyAlternativeKind,
    scalar_expr_to_column_id,
};

pub(crate) fn join_execution_distribution_for_alternative(
    alt_kind: &PropertyAlternativeKind,
) -> Option<crate::sql::optimizer::physical_plan::JoinExecutionDistribution> {
    match alt_kind {
        PropertyAlternativeKind::BroadcastJoin => {
            Some(crate::sql::optimizer::physical_plan::JoinExecutionDistribution::Broadcast)
        }
        PropertyAlternativeKind::ShuffleJoin => {
            Some(crate::sql::optimizer::physical_plan::JoinExecutionDistribution::Partitioned)
        }
        PropertyAlternativeKind::Default => None,
    }
}

/// Join types whose output rows are streamed from the left side intact —
/// the join only filters/augments by attaching right-side data on a per-
/// row basis. For these, output distribution follows the left child.
///
/// For RightOuter / RightSemi / RightAnti / FullOuter the output is NOT
/// preserved-left (see hash_join.rs derive_output's else branch).
fn preserves_left(jk: &crate::sql::common::JoinKind) -> bool {
    use crate::sql::common::JoinKind::*;
    matches!(jk, Inner | LeftOuter | LeftSemi | LeftAnti | Cross)
}

fn shuffle_join_column_ids(
    arena: &ScalarArena,
    eq_conditions: &[PhysicalHashJoinEqCondition],
) -> Vec<ColumnId> {
    let mut cols = Vec::new();
    for eq in eq_conditions {
        let (Some(left_col), Some(right_col)) = (
            scalar_expr_to_column_id(arena, eq.left),
            scalar_expr_to_column_id(arena, eq.right),
        ) else {
            continue;
        };
        cols.push(left_col);
        cols.push(right_col);
    }
    cols
}

fn shuffle_join_side_column_ids(
    arena: &ScalarArena,
    eq_conditions: &[PhysicalHashJoinEqCondition],
) -> (Vec<ColumnId>, Vec<ColumnId>) {
    let mut left = Vec::new();
    let mut right = Vec::new();
    for eq in eq_conditions {
        let (Some(left_col), Some(right_col)) = (
            scalar_expr_to_column_id(arena, eq.left),
            scalar_expr_to_column_id(arena, eq.right),
        ) else {
            continue;
        };
        left.push(left_col);
        right.push(right_col);
    }
    (left, right)
}

fn is_hash_integer_type(data_type: &DataType) -> bool {
    matches!(
        data_type,
        DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::Int64
    )
}

fn is_hash_float_type(data_type: &DataType) -> bool {
    matches!(data_type, DataType::Float32 | DataType::Float64)
}

fn hash_partition_types_are_compatible(left: &DataType, right: &DataType) -> bool {
    left == right
        || (is_hash_integer_type(left) && is_hash_integer_type(right))
        || (is_hash_float_type(left) && is_hash_float_type(right))
}

fn shuffle_join_eq_condition_is_supported(
    arena: &ScalarArena,
    eq: &PhysicalHashJoinEqCondition,
) -> bool {
    !eq.null_safe
        && scalar_expr_to_column_id(arena, eq.left).is_some()
        && scalar_expr_to_column_id(arena, eq.right).is_some()
        && hash_partition_types_are_compatible(arena.data_type(eq.left), arena.data_type(eq.right))
}

fn shuffle_join_keys_are_supported(
    arena: &ScalarArena,
    eq_conditions: &[PhysicalHashJoinEqCondition],
) -> bool {
    !eq_conditions.is_empty()
        && eq_conditions
            .iter()
            .all(|eq| shuffle_join_eq_condition_is_supported(arena, eq))
}

fn has_duplicates(cols: &[ColumnId]) -> bool {
    let mut seen = std::collections::HashSet::new();
    cols.iter().any(|col| !seen.insert(*col))
}

fn same_key_set(parent_cols: &[ColumnId], keys: &[ColumnId]) -> bool {
    parent_cols.len() == keys.len()
        && !has_duplicates(parent_cols)
        && !has_duplicates(keys)
        && parent_cols.iter().all(|col| keys.contains(col))
}

fn aligned_shuffle_keys(
    arena: &ScalarArena,
    eq_conditions: &[PhysicalHashJoinEqCondition],
    parent_required: &PhysicalPropertySet,
) -> (Vec<ColumnId>, Vec<ColumnId>) {
    if !shuffle_join_keys_are_supported(arena, eq_conditions) {
        return (Vec::new(), Vec::new());
    }

    let fallback = || shuffle_join_side_column_ids(arena, eq_conditions);

    let DistributionSpec::HashPartitioned {
        cols: parent_cols,
        source: HashSource::ShuffleJoin,
    } = &parent_required.distribution
    else {
        return fallback();
    };

    let mut left = Vec::with_capacity(eq_conditions.len());
    let mut right = Vec::with_capacity(eq_conditions.len());
    for eq in eq_conditions {
        let (Some(left_col), Some(right_col)) = (
            scalar_expr_to_column_id(arena, eq.left),
            scalar_expr_to_column_id(arena, eq.right),
        ) else {
            return fallback();
        };
        left.push(left_col);
        right.push(right_col);
    }

    let match_keys = if same_key_set(parent_cols, &left) {
        &left
    } else if same_key_set(parent_cols, &right) {
        &right
    } else {
        return fallback();
    };

    let mut aligned_left = Vec::with_capacity(parent_cols.len());
    let mut aligned_right = Vec::with_capacity(parent_cols.len());
    for parent_col in parent_cols {
        let Some(pos) = match_keys
            .iter()
            .position(|match_col| match_col == parent_col)
        else {
            return fallback();
        };
        aligned_left.push(left[pos]);
        aligned_right.push(right[pos]);
    }
    if has_duplicates(&aligned_left) || has_duplicates(&aligned_right) {
        return fallback();
    }
    (aligned_left, aligned_right)
}

fn hash_join_only_shuffle(join_type: crate::sql::common::JoinKind) -> bool {
    use crate::sql::common::JoinKind::*;
    matches!(join_type, RightOuter | RightSemi | RightAnti | FullOuter)
}

fn hash_join_only_broadcast(join_type: crate::sql::common::JoinKind) -> bool {
    use crate::sql::common::JoinKind::*;
    matches!(join_type, NullAwareLeftAnti)
}

/// Given a set of column ids representing a HashPartitioned key, and the
/// join's `eq_conditions`, return the input extended with the
/// equivalence-class partner from each matching eq pair.
///
/// Rationale: after `JOIN ON L = R`, output rows satisfy `L == R`, so the
/// output can expose both ids as equivalent hash keys for downstream
/// source-aware distribution checks. G4 keeps the source on the enriched
/// distribution; it does not rely on the old global containAll rule.
fn expand_with_eq_equivalents(
    arena: &ScalarArena,
    cols: &[ColumnId],
    eq_conditions: &[PhysicalHashJoinEqCondition],
) -> Vec<ColumnId> {
    let mut out: Vec<ColumnId> = cols.to_vec();
    for eq in eq_conditions {
        let (Some(lc), Some(rc)) = (
            scalar_expr_to_column_id(arena, eq.left),
            scalar_expr_to_column_id(arena, eq.right),
        ) else {
            continue;
        };
        if out.contains(&lc) && !out.contains(&rc) {
            out.push(rc);
        }
        if out.contains(&rc) && !out.contains(&lc) {
            out.push(lc);
        }
    }
    out
}

impl DeriveRequired for PhysicalHashJoinOp {
    fn derive_required(
        &self,
        scalars: &ScalarArena,
        _parent_required: &PhysicalPropertySet,
        _n: usize,
    ) -> Vec<PhysicalPropertySet> {
        match self.distribution {
            JoinDistribution::Unknown => {
                panic!("unknown join distribution should be resolved before property derivation")
            }
            JoinDistribution::Shuffle => {
                self.shuffle_required_alternative(scalars, _parent_required)
                    .child_props
            }
            JoinDistribution::Broadcast => {
                // Do NOT propagate the parent's HashPartitioned requirement
                // into the LEFT child. Pushing HashPart([X]) down can place
                // a Distribution enforcer over a child whose logical scope
                // does not contain X (after JoinCommutativity swaps children
                // or when the parent's required col is the RIGHT side of an
                // eq pair — `orient_eq_pair` cannot distinguish these when
                // both children share a column name like `c0`). Instead,
                // rely on `derive_output`'s eq-equivalent enrichment to let
                // the join's output satisfy the parent natively; if not,
                // the optimizer places a single Distribution enforcer ON
                // TOP of the join, where every column the parent named is
                // in scope.
                vec![PhysicalPropertySet::any(), PhysicalPropertySet::gather()]
            }
            JoinDistribution::Colocate => {
                vec![PhysicalPropertySet::any(), PhysicalPropertySet::any()]
            }
        }
    }
}

impl PhysicalHashJoinOp {
    fn preserve_left_output_distribution(
        &self,
        scalars: &ScalarArena,
        children: &[&PhysicalPropertySet],
    ) -> PhysicalPropertySet {
        let left = children
            .first()
            .copied()
            .cloned()
            .unwrap_or_else(PhysicalPropertySet::any);
        let distribution = match left.distribution {
            DistributionSpec::HashPartitioned {
                cols,
                source: HashSource::ShuffleAgg,
            } => DistributionSpec::hash_partitioned(
                expand_with_eq_equivalents(scalars, &cols, &self.eq_conditions),
                HashSource::ShuffleAgg,
            ),
            DistributionSpec::HashPartitioned {
                source: HashSource::ShuffleJoin,
                ..
            } => DistributionSpec::Any,
            other => other,
        };
        PhysicalPropertySet {
            distribution,
            ordering: OrderingSpec::Any,
        }
    }

    fn derive_shuffle_output(&self, scalars: &ScalarArena) -> PhysicalPropertySet {
        // Symmetric over both sides of each eq pair: a shuffle join partitions
        // both inputs on their respective eq columns, so its output key is an
        // equivalence class containing every eq column from either side.
        let cols = if shuffle_join_keys_are_supported(scalars, &self.eq_conditions) {
            shuffle_join_column_ids(scalars, &self.eq_conditions)
        } else {
            Vec::new()
        };
        PhysicalPropertySet {
            distribution: if cols.is_empty() {
                DistributionSpec::Any
            } else {
                DistributionSpec::shuffle_join(cols)
            },
            ordering: OrderingSpec::Any,
        }
    }

    fn derive_broadcast_output(
        &self,
        scalars: &ScalarArena,
        children: &[&PhysicalPropertySet],
    ) -> PhysicalPropertySet {
        if preserves_left(&self.join_type) {
            self.preserve_left_output_distribution(scalars, children)
        } else {
            PhysicalPropertySet::any()
        }
    }

    fn derive_colocate_output(
        &self,
        scalars: &ScalarArena,
        children: &[&PhysicalPropertySet],
    ) -> PhysicalPropertySet {
        if preserves_left(&self.join_type) {
            self.preserve_left_output_distribution(scalars, children)
        } else {
            PhysicalPropertySet::any()
        }
    }

    pub(crate) fn derive_output_for_alternative(
        &self,
        scalars: &ScalarArena,
        children: &[&PhysicalPropertySet],
        alt_kind: &PropertyAlternativeKind,
    ) -> PhysicalPropertySet {
        match alt_kind {
            PropertyAlternativeKind::BroadcastJoin => {
                self.derive_broadcast_output(scalars, children)
            }
            PropertyAlternativeKind::ShuffleJoin => self.derive_shuffle_output(scalars),
            PropertyAlternativeKind::Default => self.derive_output(scalars, children),
        }
    }

    fn broadcast_required_alternative() -> ChildRequirementAlternative {
        ChildRequirementAlternative {
            kind: PropertyAlternativeKind::BroadcastJoin,
            child_props: vec![PhysicalPropertySet::any(), PhysicalPropertySet::broadcast()],
        }
    }

    fn shuffle_required_alternative(
        &self,
        scalars: &ScalarArena,
        parent_required: &PhysicalPropertySet,
    ) -> ChildRequirementAlternative {
        let (left_keys, right_keys) =
            aligned_shuffle_keys(scalars, &self.eq_conditions, parent_required);
        if left_keys.is_empty() || right_keys.is_empty() {
            return ChildRequirementAlternative {
                kind: PropertyAlternativeKind::ShuffleJoin,
                child_props: vec![PhysicalPropertySet::gather(), PhysicalPropertySet::gather()],
            };
        }
        ChildRequirementAlternative {
            kind: PropertyAlternativeKind::ShuffleJoin,
            child_props: vec![
                PhysicalPropertySet {
                    distribution: DistributionSpec::shuffle_join(left_keys),
                    ordering: OrderingSpec::Any,
                },
                PhysicalPropertySet {
                    distribution: DistributionSpec::shuffle_join(right_keys),
                    ordering: OrderingSpec::Any,
                },
            ],
        }
    }

    pub(crate) fn derive_required_alternatives(
        &self,
        scalars: &ScalarArena,
        parent_required: &PhysicalPropertySet,
        num_children: usize,
    ) -> Vec<ChildRequirementAlternative> {
        if num_children != 2 {
            if self.distribution == JoinDistribution::Unknown {
                return Vec::new();
            }
            return vec![ChildRequirementAlternative::default(vec![
                PhysicalPropertySet::any();
                num_children
            ])];
        }

        let shuffle = || self.shuffle_required_alternative(scalars, parent_required);
        match self.distribution {
            JoinDistribution::Unknown => {
                let mut alternatives = Vec::new();
                if !hash_join_only_shuffle(self.join_type) {
                    alternatives.push(Self::broadcast_required_alternative());
                }
                // DistributionSpec currently models shuffle keys as strict
                // ColumnIds. Expression and null-safe equality keys need a
                // richer shuffle representation before they can safely use
                // this optional partitioned alternative.
                if !hash_join_only_broadcast(self.join_type)
                    && (hash_join_only_shuffle(self.join_type)
                        || shuffle_join_keys_are_supported(scalars, &self.eq_conditions))
                {
                    alternatives.push(shuffle());
                }
                alternatives
            }
            JoinDistribution::Broadcast => {
                if hash_join_only_shuffle(self.join_type) {
                    vec![shuffle()]
                } else {
                    vec![Self::broadcast_required_alternative()]
                }
            }
            JoinDistribution::Shuffle => vec![shuffle()],
            JoinDistribution::Colocate => vec![ChildRequirementAlternative::default(
                self.derive_required(scalars, parent_required, num_children),
            )],
        }
    }
}

impl DeriveOutput for PhysicalHashJoinOp {
    fn derive_output(
        &self,
        scalars: &ScalarArena,
        children: &[&PhysicalPropertySet],
    ) -> PhysicalPropertySet {
        match self.distribution {
            JoinDistribution::Unknown => {
                panic!("unknown join distribution should be resolved before property derivation")
            }
            JoinDistribution::Shuffle => self.derive_shuffle_output(scalars),
            JoinDistribution::Broadcast => self.derive_broadcast_output(scalars, children),
            JoinDistribution::Colocate => self.derive_colocate_output(scalars, children),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sql::analysis::{ExprKind, JoinKind, TypedExpr};
    use crate::sql::column_id::ColumnId;
    use crate::sql::optimizer::derive::PropertyAlternativeKind;
    use crate::sql::optimizer::property::HashSource;
    use crate::sql::optimizer::scalar::ScalarArena;

    use crate::sql::planner::optimizer_bridge::scalar::intern_typed;
    use std::ops::{Deref, DerefMut};

    fn col(id: u32) -> TypedExpr {
        col_with_type(id, arrow::datatypes::DataType::Int64)
    }

    fn col_with_type(id: u32, data_type: arrow::datatypes::DataType) -> TypedExpr {
        TypedExpr {
            kind: ExprKind::ColumnRef {
                column_id: ColumnId(id),
                qualifier: None,
                column: format!("c{id}"),
            },
            data_type,
            nullable: false,
        }
    }

    fn nested_col(id: u32) -> TypedExpr {
        TypedExpr {
            kind: ExprKind::Nested(Box::new(col(id))),
            data_type: arrow::datatypes::DataType::Int64,
            nullable: false,
        }
    }

    fn cast_col(id: u32, target: arrow::datatypes::DataType) -> TypedExpr {
        let input = col(id);
        TypedExpr {
            kind: ExprKind::Cast {
                expr: Box::new(input),
                target: target.clone(),
            },
            data_type: target,
            nullable: false,
        }
    }

    struct TestHashJoinOp {
        scalars: ScalarArena,
        op: PhysicalHashJoinOp,
    }

    impl Deref for TestHashJoinOp {
        type Target = PhysicalHashJoinOp;

        fn deref(&self) -> &Self::Target {
            &self.op
        }
    }

    impl DerefMut for TestHashJoinOp {
        fn deref_mut(&mut self) -> &mut Self::Target {
            &mut self.op
        }
    }

    impl TestHashJoinOp {
        fn derive_output(&self, children: &[&PhysicalPropertySet]) -> PhysicalPropertySet {
            self.op.derive_output(&self.scalars, children)
        }

        fn derive_required(
            &self,
            parent_required: &PhysicalPropertySet,
            num_children: usize,
        ) -> Vec<PhysicalPropertySet> {
            self.op
                .derive_required(&self.scalars, parent_required, num_children)
        }

        fn derive_required_alternatives(
            &self,
            parent_required: &PhysicalPropertySet,
            num_children: usize,
        ) -> Vec<ChildRequirementAlternative> {
            self.op
                .derive_required_alternatives(&self.scalars, parent_required, num_children)
        }
    }

    fn eq(left: TypedExpr, right: TypedExpr) -> (TypedExpr, TypedExpr, bool) {
        (left, right, false)
    }

    fn null_safe_eq(left: TypedExpr, right: TypedExpr) -> (TypedExpr, TypedExpr, bool) {
        (left, right, true)
    }

    fn join_op(
        join_type: JoinKind,
        eq_conditions: Vec<(TypedExpr, TypedExpr, bool)>,
        distribution: JoinDistribution,
    ) -> TestHashJoinOp {
        let mut scalars = ScalarArena::new();
        let eq_conditions = eq_conditions
            .into_iter()
            .map(|(left, right, null_safe)| PhysicalHashJoinEqCondition {
                left: intern_typed(&mut scalars, &left),
                right: intern_typed(&mut scalars, &right),
                null_safe,
            })
            .collect();
        TestHashJoinOp {
            scalars,
            op: PhysicalHashJoinOp {
                join_type,
                eq_conditions,
                other_condition: None,
                distribution,
            },
        }
    }

    fn broadcast_inner(eq_left: u32, eq_right: u32) -> TestHashJoinOp {
        join_op(
            JoinKind::Inner,
            vec![eq(col(eq_left), col(eq_right))],
            JoinDistribution::Broadcast,
        )
    }

    #[test]
    fn hash_join_broadcast_inner_preserves_left_distribution() {
        let op = broadcast_inner(10, 20);
        let left_out = PhysicalPropertySet {
            distribution: DistributionSpec::shuffle_agg([ColumnId(10)]),
            ordering: OrderingSpec::Any,
        };
        let right_out = PhysicalPropertySet::gather();
        let out = op.derive_output(&[&left_out, &right_out]);
        assert_eq!(
            out.distribution,
            DistributionSpec::shuffle_agg([ColumnId(10), ColumnId(20)])
        );
        assert_eq!(out.ordering, OrderingSpec::Any);
    }

    #[test]
    fn hash_join_broadcast_inner_does_not_advertise_shuffle_join_distribution() {
        let op = broadcast_inner(10, 20);
        let left_out = PhysicalPropertySet {
            distribution: DistributionSpec::shuffle_join([ColumnId(10)]),
            ordering: OrderingSpec::Any,
        };
        let right_out = PhysicalPropertySet::gather();
        let out = op.derive_output(&[&left_out, &right_out]);
        assert_eq!(out.distribution, DistributionSpec::Any);
        assert_eq!(out.ordering, OrderingSpec::Any);
    }

    #[test]
    fn hash_join_unknown_distribution_enumerates_implementation_alternatives() {
        let op = join_op(
            JoinKind::Inner,
            vec![eq(col(10), col(20))],
            JoinDistribution::Unknown,
        );

        let alternatives = op.derive_required_alternatives(&PhysicalPropertySet::any(), 2);
        assert_eq!(alternatives.len(), 2);
        assert_eq!(alternatives[0].kind, PropertyAlternativeKind::BroadcastJoin);
        assert_eq!(
            alternatives[0].child_props[0].distribution,
            DistributionSpec::Any
        );
        assert_eq!(
            alternatives[0].child_props[1].distribution,
            DistributionSpec::Broadcast
        );
        assert_eq!(alternatives[1].kind, PropertyAlternativeKind::ShuffleJoin);
        assert_eq!(
            alternatives[1].child_props[0].distribution,
            DistributionSpec::shuffle_join([ColumnId(10)])
        );
        assert_eq!(
            alternatives[1].child_props[1].distribution,
            DistributionSpec::shuffle_join([ColumnId(20)])
        );
    }

    #[test]
    fn hash_join_unknown_distribution_skips_shuffle_for_expression_keys() {
        let op = join_op(
            JoinKind::Inner,
            vec![eq(col(10), col(20)), eq(nested_col(11), nested_col(21))],
            JoinDistribution::Unknown,
        );

        let alternatives = op.derive_required_alternatives(&PhysicalPropertySet::any(), 2);
        assert_eq!(alternatives.len(), 1);
        assert_eq!(alternatives[0].kind, PropertyAlternativeKind::BroadcastJoin);
    }

    #[test]
    fn hash_join_unknown_distribution_skips_shuffle_for_null_safe_keys() {
        let op = join_op(
            JoinKind::Inner,
            vec![null_safe_eq(col(10), col(20)), eq(col(11), col(21))],
            JoinDistribution::Unknown,
        );

        let alternatives = op.derive_required_alternatives(&PhysicalPropertySet::any(), 2);
        assert_eq!(alternatives.len(), 1);
        assert_eq!(alternatives[0].kind, PropertyAlternativeKind::BroadcastJoin);
    }

    #[test]
    fn hash_join_unknown_right_outer_uses_shuffle_only() {
        let op = join_op(
            JoinKind::RightOuter,
            vec![eq(col(10), col(20))],
            JoinDistribution::Unknown,
        );

        let alternatives = op.derive_required_alternatives(&PhysicalPropertySet::any(), 2);
        assert_eq!(alternatives.len(), 1);
        assert_eq!(alternatives[0].kind, PropertyAlternativeKind::ShuffleJoin);
        assert_eq!(
            alternatives[0].child_props[0].distribution,
            DistributionSpec::shuffle_join([ColumnId(10)])
        );
        assert_eq!(
            alternatives[0].child_props[1].distribution,
            DistributionSpec::shuffle_join([ColumnId(20)])
        );
    }

    #[test]
    fn hash_join_legacy_concrete_distribution_limits_alternatives() {
        let mut op = join_op(
            JoinKind::Inner,
            vec![eq(col(10), col(20))],
            JoinDistribution::Broadcast,
        );

        let broadcast = op.derive_required_alternatives(&PhysicalPropertySet::any(), 2);
        assert_eq!(broadcast.len(), 1);
        assert_eq!(broadcast[0].kind, PropertyAlternativeKind::BroadcastJoin);

        op.join_type = crate::sql::analysis::JoinKind::RightOuter;
        let right_outer_broadcast = op.derive_required_alternatives(&PhysicalPropertySet::any(), 2);
        assert_eq!(right_outer_broadcast.len(), 1);
        assert_eq!(
            right_outer_broadcast[0].kind,
            PropertyAlternativeKind::ShuffleJoin
        );

        op.join_type = crate::sql::analysis::JoinKind::Inner;
        op.distribution = JoinDistribution::Shuffle;
        let shuffle = op.derive_required_alternatives(&PhysicalPropertySet::any(), 2);
        assert_eq!(shuffle.len(), 1);
        assert_eq!(shuffle[0].kind, PropertyAlternativeKind::ShuffleJoin);

        op.distribution = JoinDistribution::Colocate;
        let colocate = op.derive_required_alternatives(&PhysicalPropertySet::any(), 2);
        assert_eq!(colocate.len(), 1);
        assert_eq!(colocate[0].kind, PropertyAlternativeKind::Default);
    }

    #[test]
    fn hash_join_shuffle_alternative_aligns_with_parent_required_order() {
        let op = join_op(
            JoinKind::Inner,
            vec![eq(col(10), col(20)), eq(col(11), col(21))],
            JoinDistribution::Unknown,
        );
        let parent = PhysicalPropertySet {
            distribution: DistributionSpec::shuffle_join([ColumnId(11), ColumnId(10)]),
            ordering: OrderingSpec::Any,
        };

        let alternatives = op.derive_required_alternatives(&parent, 2);
        let shuffle = alternatives
            .iter()
            .find(|alt| alt.kind == PropertyAlternativeKind::ShuffleJoin)
            .expect("shuffle alternative");

        assert_eq!(
            shuffle.child_props[0].distribution,
            DistributionSpec::shuffle_join([ColumnId(11), ColumnId(10)])
        );
        assert_eq!(
            shuffle.child_props[1].distribution,
            DistributionSpec::shuffle_join([ColumnId(21), ColumnId(20)])
        );
    }

    #[test]
    fn hash_join_shuffle_alignment_rejects_duplicate_eq_pair_matches() {
        let op = join_op(
            JoinKind::Inner,
            vec![eq(col(10), col(20)), eq(col(11), col(21))],
            JoinDistribution::Unknown,
        );
        let parent = PhysicalPropertySet {
            distribution: DistributionSpec::shuffle_join([ColumnId(10), ColumnId(20)]),
            ordering: OrderingSpec::Any,
        };

        let alternatives = op.derive_required_alternatives(&parent, 2);
        let shuffle = alternatives
            .iter()
            .find(|alt| alt.kind == PropertyAlternativeKind::ShuffleJoin)
            .expect("shuffle alternative");
        assert_eq!(
            shuffle.child_props[0].distribution,
            DistributionSpec::shuffle_join([ColumnId(10), ColumnId(11)])
        );
        assert_eq!(
            shuffle.child_props[1].distribution,
            DistributionSpec::shuffle_join([ColumnId(20), ColumnId(21)])
        );
    }

    #[test]
    fn hash_join_shuffle_alignment_rejects_duplicate_single_side_keys() {
        let op = join_op(
            JoinKind::Inner,
            vec![eq(col(10), col(20)), eq(col(10), col(21))],
            JoinDistribution::Unknown,
        );
        let parent = PhysicalPropertySet {
            distribution: DistributionSpec::shuffle_join([ColumnId(20), ColumnId(21)]),
            ordering: OrderingSpec::Any,
        };

        let alternatives = op.derive_required_alternatives(&parent, 2);
        let shuffle = alternatives
            .iter()
            .find(|alt| alt.kind == PropertyAlternativeKind::ShuffleJoin)
            .expect("shuffle alternative");
        assert_eq!(
            shuffle.child_props[0].distribution,
            DistributionSpec::shuffle_join([ColumnId(10), ColumnId(10)])
        );
        assert_eq!(
            shuffle.child_props[1].distribution,
            DistributionSpec::shuffle_join([ColumnId(20), ColumnId(21)])
        );
    }

    #[test]
    fn hash_join_shuffle_alignment_rejects_mixed_side_parent_keys() {
        let op = join_op(
            JoinKind::Inner,
            vec![eq(col(10), col(20)), eq(col(11), col(21))],
            JoinDistribution::Unknown,
        );
        let parent = PhysicalPropertySet {
            distribution: DistributionSpec::shuffle_join([ColumnId(10), ColumnId(21)]),
            ordering: OrderingSpec::Any,
        };

        let alternatives = op.derive_required_alternatives(&parent, 2);
        let shuffle = alternatives
            .iter()
            .find(|alt| alt.kind == PropertyAlternativeKind::ShuffleJoin)
            .expect("shuffle alternative");
        assert_eq!(
            shuffle.child_props[0].distribution,
            DistributionSpec::shuffle_join([ColumnId(10), ColumnId(11)])
        );
        assert_eq!(
            shuffle.child_props[1].distribution,
            DistributionSpec::shuffle_join([ColumnId(20), ColumnId(21)])
        );
    }

    #[test]
    fn hash_join_right_outer_alternatives_are_shuffle_only() {
        let op = join_op(
            JoinKind::RightOuter,
            vec![eq(col(10), col(20))],
            JoinDistribution::Unknown,
        );

        let alternatives = op.derive_required_alternatives(&PhysicalPropertySet::any(), 2);
        assert_eq!(alternatives.len(), 1);
        assert_eq!(alternatives[0].kind, PropertyAlternativeKind::ShuffleJoin);
    }

    #[test]
    fn hash_join_right_outer_cast_key_does_not_emit_half_shuffle() {
        let op = join_op(
            JoinKind::RightOuter,
            vec![eq(col(10), cast_col(20, arrow::datatypes::DataType::Int64))],
            JoinDistribution::Unknown,
        );

        let alternatives = op.derive_required_alternatives(&PhysicalPropertySet::any(), 2);
        assert_eq!(alternatives.len(), 1);
        assert_eq!(alternatives[0].kind, PropertyAlternativeKind::ShuffleJoin);
        assert_eq!(
            alternatives[0].child_props[0].distribution,
            DistributionSpec::Gather
        );
        assert_eq!(
            alternatives[0].child_props[1].distribution,
            DistributionSpec::Gather
        );
    }

    #[test]
    fn hash_join_unknown_string_int_key_skips_shuffle_alternative() {
        let op = join_op(
            JoinKind::Inner,
            vec![eq(
                col_with_type(10, arrow::datatypes::DataType::Utf8),
                col_with_type(20, arrow::datatypes::DataType::Int32),
            )],
            JoinDistribution::Unknown,
        );

        let alternatives = op.derive_required_alternatives(&PhysicalPropertySet::any(), 2);
        assert_eq!(alternatives.len(), 1);
        assert_eq!(alternatives[0].kind, PropertyAlternativeKind::BroadcastJoin);
    }

    #[test]
    fn hash_join_forced_shuffle_string_int_key_gathers_both_sides() {
        let op = join_op(
            JoinKind::RightOuter,
            vec![eq(
                col_with_type(10, arrow::datatypes::DataType::Utf8),
                col_with_type(20, arrow::datatypes::DataType::Int32),
            )],
            JoinDistribution::Shuffle,
        );

        let reqs = op.derive_required(&PhysicalPropertySet::any(), 2);
        assert_eq!(reqs[0].distribution, DistributionSpec::Gather);
        assert_eq!(reqs[1].distribution, DistributionSpec::Gather);

        let out = op.derive_output(&[&PhysicalPropertySet::any(), &PhysicalPropertySet::any()]);
        assert_eq!(out.distribution, DistributionSpec::Any);
    }

    #[test]
    fn hash_join_mixed_integer_key_keeps_shuffle_distribution() {
        let op = join_op(
            JoinKind::Inner,
            vec![eq(
                col_with_type(10, arrow::datatypes::DataType::Int32),
                col_with_type(20, arrow::datatypes::DataType::Int64),
            )],
            JoinDistribution::Unknown,
        );

        let alternatives = op.derive_required_alternatives(&PhysicalPropertySet::any(), 2);
        let shuffle = alternatives
            .iter()
            .find(|alt| alt.kind == PropertyAlternativeKind::ShuffleJoin)
            .expect("mixed integer keys are hash-compatible");
        assert_eq!(
            shuffle.child_props[0].distribution,
            DistributionSpec::shuffle_join([ColumnId(10)])
        );
        assert_eq!(
            shuffle.child_props[1].distribution,
            DistributionSpec::shuffle_join([ColumnId(20)])
        );
    }

    #[test]
    fn hash_join_right_semi_anti_and_full_alternatives_are_shuffle_only() {
        for join_type in [
            crate::sql::analysis::JoinKind::RightSemi,
            crate::sql::analysis::JoinKind::RightAnti,
            crate::sql::analysis::JoinKind::FullOuter,
        ] {
            let op = join_op(
                join_type,
                vec![eq(col(10), col(20))],
                JoinDistribution::Unknown,
            );

            let alternatives = op.derive_required_alternatives(&PhysicalPropertySet::any(), 2);
            assert_eq!(alternatives.len(), 1, "join_type={join_type:?}");
            assert_eq!(
                alternatives[0].kind,
                PropertyAlternativeKind::ShuffleJoin,
                "join_type={join_type:?}"
            );
        }
    }

    #[test]
    fn required_input_shuffle_join() {
        let left_key = TypedExpr {
            kind: ExprKind::ColumnRef {
                column_id: ColumnId(6),
                qualifier: Some("a".into()),
                column: "id".into(),
            },
            data_type: arrow::datatypes::DataType::Int32,
            nullable: false,
        };
        let right_key = TypedExpr {
            kind: ExprKind::ColumnRef {
                column_id: ColumnId(7),
                qualifier: Some("b".into()),
                column: "id".into(),
            },
            data_type: arrow::datatypes::DataType::Int32,
            nullable: false,
        };
        let op = join_op(
            JoinKind::Inner,
            vec![eq(left_key, right_key)],
            JoinDistribution::Shuffle,
        );
        let reqs = op.derive_required(&PhysicalPropertySet::any(), 2);
        assert_eq!(reqs.len(), 2);

        // Shuffle children must be partitioned by their own side of each eq
        // pair. Mixing both sides into each child is unsafe for qualified
        // self-joins because fragment building may resolve the other side by
        // name and create a different physical hash key.
        for (side_label, req, expected) in [
            ("left", &reqs[0], ColumnId(6)),
            ("right", &reqs[1], ColumnId(7)),
        ] {
            match &req.distribution {
                DistributionSpec::HashPartitioned { cols, source } => {
                    assert_eq!(*source, HashSource::ShuffleJoin);
                    assert_eq!(cols.as_slice(), &[expected], "{side_label} side");
                }
                other => panic!(
                    "expected HashPartitioned for {} side, got {:?}",
                    side_label, other
                ),
            }
        }
    }

    #[test]
    fn shuffle_join_output_and_requirements_use_same_interleaved_key_order() {
        let op = join_op(
            JoinKind::Inner,
            vec![eq(col(10), col(20)), eq(col(11), col(21))],
            JoinDistribution::Shuffle,
        );
        let expected = DistributionSpec::shuffle_join([
            ColumnId(10),
            ColumnId(20),
            ColumnId(11),
            ColumnId(21),
        ]);

        let out = op.derive_output(&[&PhysicalPropertySet::any(), &PhysicalPropertySet::any()]);
        match &out.distribution {
            DistributionSpec::HashPartitioned { cols, source } => {
                assert_eq!(*source, HashSource::ShuffleJoin);
                assert_eq!(
                    cols.as_slice(),
                    &[ColumnId(10), ColumnId(20), ColumnId(11), ColumnId(21)]
                );
            }
            other => panic!("expected ShuffleJoin interleaved output key, got {other:?}"),
        }
        assert_eq!(out.distribution, expected);

        let reqs = op.derive_required(&PhysicalPropertySet::any(), 2);
        assert_eq!(reqs.len(), 2);
        for (req, expected_cols) in [
            (&reqs[0], &[ColumnId(10), ColumnId(11)][..]),
            (&reqs[1], &[ColumnId(20), ColumnId(21)][..]),
        ] {
            match &req.distribution {
                DistributionSpec::HashPartitioned { cols, source } => {
                    assert_eq!(*source, HashSource::ShuffleJoin);
                    assert_eq!(cols.as_slice(), expected_cols);
                }
                other => panic!("expected ShuffleJoin side-specific required key, got {other:?}"),
            }
        }
    }

    // ── Broadcast non-preserves-left → output stays Any ─────────────────────

    fn broadcast_with_type(jk: crate::sql::analysis::JoinKind) -> TestHashJoinOp {
        join_op(jk, vec![eq(col(10), col(20))], JoinDistribution::Broadcast)
    }

    #[test]
    fn hash_join_broadcast_right_outer_returns_any() {
        let op = broadcast_with_type(crate::sql::analysis::JoinKind::RightOuter);
        let left_out = PhysicalPropertySet {
            distribution: DistributionSpec::shuffle_agg([ColumnId(10)]),
            ordering: OrderingSpec::Any,
        };
        let out = op.derive_output(&[&left_out, &PhysicalPropertySet::gather()]);
        assert_eq!(out.distribution, DistributionSpec::Any);
    }

    #[test]
    fn hash_join_broadcast_right_semi_returns_any() {
        let op = broadcast_with_type(crate::sql::analysis::JoinKind::RightSemi);
        let left_out = PhysicalPropertySet {
            distribution: DistributionSpec::shuffle_agg([ColumnId(10)]),
            ordering: OrderingSpec::Any,
        };
        let out = op.derive_output(&[&left_out, &PhysicalPropertySet::gather()]);
        assert_eq!(out.distribution, DistributionSpec::Any);
    }

    #[test]
    fn hash_join_broadcast_right_anti_returns_any() {
        let op = broadcast_with_type(crate::sql::analysis::JoinKind::RightAnti);
        let left_out = PhysicalPropertySet {
            distribution: DistributionSpec::shuffle_agg([ColumnId(10)]),
            ordering: OrderingSpec::Any,
        };
        let out = op.derive_output(&[&left_out, &PhysicalPropertySet::gather()]);
        assert_eq!(out.distribution, DistributionSpec::Any);
    }

    #[test]
    fn hash_join_broadcast_full_outer_returns_any() {
        let op = broadcast_with_type(crate::sql::analysis::JoinKind::FullOuter);
        let left_out = PhysicalPropertySet {
            distribution: DistributionSpec::shuffle_agg([ColumnId(10)]),
            ordering: OrderingSpec::Any,
        };
        let out = op.derive_output(&[&left_out, &PhysicalPropertySet::gather()]);
        assert_eq!(out.distribution, DistributionSpec::Any);
    }

    // ── Task 18: Colocate preserves-left + negative ───────────────────────────

    fn colocate_inner(eq_left: u32, eq_right: u32) -> TestHashJoinOp {
        join_op(
            JoinKind::Inner,
            vec![eq(col(eq_left), col(eq_right))],
            JoinDistribution::Colocate,
        )
    }

    #[test]
    fn hash_join_colocate_inner_preserves_left_distribution() {
        let op = colocate_inner(10, 20);
        let left_out = PhysicalPropertySet {
            distribution: DistributionSpec::shuffle_agg([ColumnId(10)]),
            ordering: OrderingSpec::Any,
        };
        let right_out = PhysicalPropertySet {
            distribution: DistributionSpec::shuffle_agg([ColumnId(20)]),
            ordering: OrderingSpec::Any,
        };
        let out = op.derive_output(&[&left_out, &right_out]);
        // Like Broadcast preserves-left, Colocate enriches the left's
        // HashPartitioned with its eq-equivalence partner so a downstream
        // requirement on either side of the eq pair is satisfied.
        match &out.distribution {
            DistributionSpec::HashPartitioned { cols, source } => {
                assert_eq!(*source, HashSource::ShuffleAgg);
                let ids: std::collections::HashSet<ColumnId> = cols.iter().copied().collect();
                assert!(
                    ids.contains(&ColumnId(10)),
                    "expected ColumnId(10), got {ids:?}"
                );
                assert!(
                    ids.contains(&ColumnId(20)),
                    "expected ColumnId(20), got {ids:?}"
                );
            }
            other => panic!("expected HashPartitioned([10, 20]), got {other:?}"),
        }
    }

    #[test]
    fn hash_join_colocate_right_outer_returns_any() {
        let mut op = colocate_inner(10, 20);
        op.join_type = crate::sql::analysis::JoinKind::RightOuter;
        let left_out = PhysicalPropertySet {
            distribution: DistributionSpec::shuffle_agg([ColumnId(10)]),
            ordering: OrderingSpec::Any,
        };
        let right_out = PhysicalPropertySet {
            distribution: DistributionSpec::shuffle_agg([ColumnId(20)]),
            ordering: OrderingSpec::Any,
        };
        let out = op.derive_output(&[&left_out, &right_out]);
        assert_eq!(out.distribution, DistributionSpec::Any);
    }

    // ── Broadcast required: never propagates HashPart to left ───────────────
    //
    // A Broadcast join's left-required is now always Any (gather-on-right).
    // Pushing parent's HashPart([X]) into the LEFT child was unsafe: when
    // CBO swapped the children (or when the parent's required col is the
    // RIGHT side of the eq under ambiguous orient_eq_pair), the enforcer
    // ended up over a child whose logical scope did not contain X.
    // derive_output's preserves-left eq-equivalence enrichment lets the
    // join's output satisfy the parent natively; if not, the enforcer is
    // placed on the join's output (where every column the parent named is
    // in scope).

    #[test]
    fn hash_join_broadcast_required_never_pushes_hash_to_left() {
        let op = broadcast_inner(10, 20);
        let parent = PhysicalPropertySet {
            distribution: DistributionSpec::shuffle_agg([ColumnId(10)]),
            ordering: OrderingSpec::Any,
        };
        let reqs = op.derive_required(&parent, 2);
        assert_eq!(reqs[0].distribution, DistributionSpec::Any);
        assert_eq!(reqs[1].distribution, DistributionSpec::Gather);
    }

    #[test]
    fn hash_join_broadcast_required_does_not_push_gather() {
        let op = broadcast_inner(10, 20);
        let parent = PhysicalPropertySet::gather();
        let reqs = op.derive_required(&parent, 2);
        assert_eq!(reqs[0].distribution, DistributionSpec::Any);
        assert_eq!(reqs[1].distribution, DistributionSpec::Gather);
    }

    #[test]
    fn hash_join_broadcast_required_right_outer_returns_any_gather() {
        let op = broadcast_with_type(crate::sql::analysis::JoinKind::RightOuter);
        let parent = PhysicalPropertySet {
            distribution: DistributionSpec::shuffle_agg([ColumnId(10)]),
            ordering: OrderingSpec::Any,
        };
        let reqs = op.derive_required(&parent, 2);
        assert_eq!(reqs[0].distribution, DistributionSpec::Any);
        assert_eq!(reqs[1].distribution, DistributionSpec::Gather);
    }

    #[test]
    fn hash_join_colocate_required_returns_any_any() {
        let op = colocate_inner(10, 20);
        let parent = PhysicalPropertySet {
            distribution: DistributionSpec::shuffle_agg([ColumnId(10)]),
            ordering: OrderingSpec::Any,
        };
        let reqs = op.derive_required(&parent, 2);
        assert_eq!(reqs[0].distribution, DistributionSpec::Any);
        assert_eq!(reqs[1].distribution, DistributionSpec::Any);
    }

    // ── eq-equivalence enrichment of derive_output ─────────────────────────

    #[test]
    fn broadcast_output_left_any_stays_any() {
        // No HashPartitioned to enrich → output should pass through unchanged.
        let op = broadcast_inner(10, 20);
        let left_out = PhysicalPropertySet::any();
        let right_out = PhysicalPropertySet::gather();
        let out = op.derive_output(&[&left_out, &right_out]);
        assert_eq!(out.distribution, DistributionSpec::Any);
    }

    #[test]
    fn broadcast_output_left_hash_preserves_eq_enriched_distribution() {
        let op = broadcast_inner(10, 20);
        let left_out = PhysicalPropertySet {
            distribution: DistributionSpec::shuffle_agg([ColumnId(10), ColumnId(20)]),
            ordering: OrderingSpec::Any,
        };
        let right_out = PhysicalPropertySet::gather();
        let out = op.derive_output(&[&left_out, &right_out]);
        assert_eq!(
            out.distribution,
            DistributionSpec::shuffle_agg([ColumnId(10), ColumnId(20)])
        );
    }

    #[test]
    fn broadcast_output_right_eq_hash_preserves_eq_enriched_distribution() {
        let op = broadcast_inner(10, 20);
        let left_out = PhysicalPropertySet {
            distribution: DistributionSpec::shuffle_agg([ColumnId(20)]),
            ordering: OrderingSpec::Any,
        };
        let right_out = PhysicalPropertySet::gather();
        let out = op.derive_output(&[&left_out, &right_out]);
        assert_eq!(
            out.distribution,
            DistributionSpec::shuffle_agg([ColumnId(20), ColumnId(10)])
        );
    }

    #[test]
    fn shuffle_output_is_symmetric_over_eq_cols() {
        // Shuffle's output's HashPartitioned vector must contain BOTH sides
        // of every eq pair — a shuffle partitions both inputs on their
        // respective eq columns, so the output is hash-equivalent in either.
        let op = join_op(
            JoinKind::Inner,
            vec![eq(col(10), col(20))],
            JoinDistribution::Shuffle,
        );
        let out = op.derive_output(&[&PhysicalPropertySet::any(), &PhysicalPropertySet::any()]);
        match &out.distribution {
            DistributionSpec::HashPartitioned { cols, source } => {
                assert_eq!(*source, HashSource::ShuffleJoin);
                let ids: std::collections::HashSet<ColumnId> = cols.iter().copied().collect();
                assert!(ids.contains(&ColumnId(10)));
                assert!(ids.contains(&ColumnId(20)));
            }
            other => panic!("expected HashPartitioned([10, 20]), got {other:?}"),
        }
    }
}
