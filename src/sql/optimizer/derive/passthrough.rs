//! Passthrough operators — operators with a single child whose output mirrors
//! the child's output. Two flavours:
//!
//! 1. **Distribution-blind** (Filter / Project / CTEProduce): these operators
//!    do not constrain their child's distribution.
//!    Their `derive_required` therefore returns `Any` for the child slot,
//!    letting the optimizer freely choose the cheapest distribution for the
//!    subtree below. Any mismatch with the parent's required distribution is
//!    handled by an enforcer placed **above** the passthrough — preserving
//!    distributed execution of the passthrough's computation (StarRocks
//!    canonical: `Gather → Project → Scan` rather than `Project → Gather →
//!    Scan`).
//!
//! 2. **Global barrier** (Limit): a global `LIMIT 10` must produce one query-wide
//!    row stream, not one stream per worker. It therefore requires a Gather child
//!    and reports Gather output even when the parent requirement is Any.
//!
//! 3. **Full-passthrough** (TableFunction): the parent's required distribution
//!    is propagated to the child. TableFunction's semantics are operator-specific
//!    and not generally distribution-blind; we keep the safe pass-through here.
//!
//! For both flavours `derive_output` is the single child's output (via
//! `passthrough_output`).

use crate::sql::optimizer::property::OrderingSpec;
use crate::sql::optimizer::property::{DistributionSpec, PhysicalPropertySet};
use crate::sql::optimizer::scalar::ScalarArena;

/// Output of a passthrough operator equals its single child's output.
pub(crate) fn passthrough_output(children_outputs: &[&PhysicalPropertySet]) -> PhysicalPropertySet {
    children_outputs
        .first()
        .copied()
        .cloned()
        .unwrap_or_else(PhysicalPropertySet::any)
}

/// Required input of a **distribution-blind** passthrough operator: returns
/// `[Any]` so the child is free to pick the cheapest distribution. Any
/// mismatch with the parent's required distribution is resolved by an
/// enforcer placed above the passthrough.
pub(crate) fn passthrough_required_distribution_blind(
    _parent_required: &PhysicalPropertySet,
) -> Vec<PhysicalPropertySet> {
    vec![PhysicalPropertySet::any()]
}

/// Required input of a **full-passthrough** operator: forwards the parent's
/// required distribution and ordering verbatim. Use for operators whose
/// correctness depends on the child satisfying the parent's distribution
/// before they fire (e.g. global `LIMIT` requires Gather).
pub(crate) fn passthrough_required_full(
    parent_required: &PhysicalPropertySet,
) -> Vec<PhysicalPropertySet> {
    vec![parent_required.clone()]
}

use crate::sql::optimizer::operator::{
    CTEProduceOp, DecodeOp, FilterOp, LimitOp, ProjectOp, RepeatOp, TableFunctionOp,
};

use super::{DeriveOutput, DeriveRequired};

/// Distribution-blind passthroughs: `derive_required` returns `[Any]`.
macro_rules! passthrough_distribution_blind_impls {
    ($($op:ty),+ $(,)?) => {
        $(
            impl DeriveOutput for $op {
                fn derive_output(
                    &self,
                    _scalars: &ScalarArena,
                    children: &[&PhysicalPropertySet],
                ) -> PhysicalPropertySet {
                    passthrough_output(children)
                }
            }

            impl DeriveRequired for $op {
                fn derive_required(
                    &self,
                    _scalars: &ScalarArena,
                    parent_required: &PhysicalPropertySet,
                    _n: usize,
                ) -> Vec<PhysicalPropertySet> {
                    passthrough_required_distribution_blind(parent_required)
                }
            }
        )+
    };
}

passthrough_distribution_blind_impls!(FilterOp, ProjectOp, DecodeOp, CTEProduceOp,);

impl DeriveOutput for RepeatOp {
    fn derive_output(
        &self,
        _scalars: &ScalarArena,
        _children: &[&PhysicalPropertySet],
    ) -> PhysicalPropertySet {
        // Repeat rewrites grouping-set keys by NULLing inactive rollup columns
        // and appending grouping-id slots. Any child hash partitioning on the
        // original keys is no longer valid for the repeated rows; a parent
        // aggregate must add a fresh exchange on the post-repeat keys.
        PhysicalPropertySet::any()
    }
}

impl DeriveRequired for RepeatOp {
    fn derive_required(
        &self,
        _scalars: &ScalarArena,
        _parent_required: &PhysicalPropertySet,
        _n: usize,
    ) -> Vec<PhysicalPropertySet> {
        vec![PhysicalPropertySet::any()]
    }
}

/// Full-passthrough operators: `derive_required` forwards `parent_required`.
macro_rules! passthrough_full_impls {
    ($($op:ty),+ $(,)?) => {
        $(
            impl DeriveOutput for $op {
                fn derive_output(
                    &self,
                    _scalars: &ScalarArena,
                    children: &[&PhysicalPropertySet],
                ) -> PhysicalPropertySet {
                    passthrough_output(children)
                }
            }

            impl DeriveRequired for $op {
                fn derive_required(
                    &self,
                    _scalars: &ScalarArena,
                    parent_required: &PhysicalPropertySet,
                    _n: usize,
                ) -> Vec<PhysicalPropertySet> {
                    passthrough_required_full(parent_required)
                }
            }
        )+
    };
}

impl DeriveOutput for LimitOp {
    fn derive_output(
        &self,
        _scalars: &ScalarArena,
        children: &[&PhysicalPropertySet],
    ) -> PhysicalPropertySet {
        PhysicalPropertySet {
            distribution: DistributionSpec::Gather,
            ordering: children
                .first()
                .map(|child| child.ordering.clone())
                .unwrap_or(OrderingSpec::Any),
        }
    }
}

impl DeriveRequired for LimitOp {
    fn derive_required(
        &self,
        _scalars: &ScalarArena,
        parent_required: &PhysicalPropertySet,
        _n: usize,
    ) -> Vec<PhysicalPropertySet> {
        vec![PhysicalPropertySet {
            distribution: DistributionSpec::Gather,
            ordering: parent_required.ordering.clone(),
        }]
    }
}

passthrough_full_impls!(TableFunctionOp);

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sql::analysis::{ExprKind, LiteralValue, TypedExpr};
    use crate::sql::column_id::ColumnId;
    use crate::sql::optimizer::operator::{FilterOp, LimitOp, ProjectOp};
    use crate::sql::optimizer::property::{DistributionSpec, OrderingSpec, SortKey};
    use crate::sql::optimizer::scalar::ScalarArena;

    use crate::sql::planner::optimizer_bridge::scalar::intern_typed;
    use arrow::datatypes::DataType;

    fn bool_filter(scalars: &mut ScalarArena) -> FilterOp {
        let predicate = TypedExpr {
            kind: ExprKind::Literal(LiteralValue::Bool(true)),
            data_type: DataType::Boolean,
            nullable: false,
        };
        FilterOp {
            predicate: intern_typed(scalars, &predicate),
        }
    }

    fn make_minimal_limit_op() -> LimitOp {
        LimitOp {
            limit: Some(10),
            offset: None,
        }
    }

    fn make_minimal_project_op() -> ProjectOp {
        ProjectOp {
            items: vec![],
            output_qualifier: None,
        }
    }

    fn hash_one() -> PhysicalPropertySet {
        PhysicalPropertySet {
            distribution: DistributionSpec::shuffle_agg([ColumnId(1)]),
            ordering: OrderingSpec::Any,
        }
    }

    // --- Required derivation ---

    #[test]
    fn filter_required_is_distribution_blind() {
        // Filter does not constrain its child's distribution: returning [Any]
        // lets the child pick the cheapest plan; any mismatch with the
        // parent's required distribution becomes an enforcer above Filter.
        let mut scalars = ScalarArena::new();
        let op = bool_filter(&mut scalars);
        let parent_req = PhysicalPropertySet::gather();
        let child_reqs = op.derive_required(&scalars, &parent_req, 1);
        assert_eq!(child_reqs.len(), 1);
        assert_eq!(child_reqs[0], PhysicalPropertySet::any());
    }

    #[test]
    fn project_required_is_distribution_blind() {
        let op = make_minimal_project_op();
        let scalars = ScalarArena::new();
        let parent_req = PhysicalPropertySet {
            distribution: DistributionSpec::shuffle_agg([ColumnId(1)]),
            ordering: OrderingSpec::Any,
        };
        let child_reqs = op.derive_required(&scalars, &parent_req, 1);
        assert_eq!(child_reqs.len(), 1);
        assert_eq!(child_reqs[0], PhysicalPropertySet::any());
    }

    #[test]
    fn limit_required_forces_gather_child() {
        // Limit is a global barrier: even when the parent accepts Any
        // distribution, LIMIT must run on one query-wide stream.
        let op = make_minimal_limit_op();
        let scalars = ScalarArena::new();
        let parent = PhysicalPropertySet::any();
        let reqs = op.derive_required(&scalars, &parent, 1);
        assert_eq!(reqs, vec![PhysicalPropertySet::gather()]);
    }

    // --- Output derivation (follows single child) ---

    #[test]
    fn passthrough_filter_output_follows_child() {
        let mut scalars = ScalarArena::new();
        let op = bool_filter(&mut scalars);
        let child = hash_one();
        let out = op.derive_output(&scalars, &[&child]);
        assert_eq!(out, child);
    }

    #[test]
    fn passthrough_project_output_preserves_ordering() {
        let op = make_minimal_project_op();
        let scalars = ScalarArena::new();
        let child = PhysicalPropertySet {
            distribution: DistributionSpec::shuffle_agg([ColumnId(1)]),
            ordering: OrderingSpec::Required(vec![SortKey {
                column: ColumnId(2),
                asc: true,
                nulls_first: false,
            }]),
        };
        let out = op.derive_output(&scalars, &[&child]);
        assert_eq!(out, child);
    }

    #[test]
    fn limit_output_is_gather_barrier() {
        let op = make_minimal_limit_op();
        let scalars = ScalarArena::new();
        let child = hash_one();
        let out = op.derive_output(&scalars, &[&child]);

        assert_eq!(out.distribution, DistributionSpec::Gather);
        assert_eq!(out.ordering, child.ordering);
    }

    #[test]
    fn passthrough_no_children_falls_back_to_any() {
        let mut scalars = ScalarArena::new();
        let op = bool_filter(&mut scalars);
        let out = op.derive_output(&scalars, &[]);
        assert_eq!(out, PhysicalPropertySet::any());
    }

    #[test]
    fn repeat_output_does_not_inherit_child_hash_distribution() {
        let op = RepeatOp {
            repeat_column_ref_list: vec![vec!["k".to_string()], vec![]],
            repeat_column_ref_ids: vec![vec![ColumnId(1)], vec![]],
            grouping_ids: vec![0, 1],
            all_rollup_columns: vec!["k".to_string()],
            all_rollup_column_ids: vec![ColumnId(1)],
            grouping_key_aliases: vec![],
            grouping_fn_args: vec![("__grouping_fn_0".to_string(), vec!["k".to_string()])],
            grouping_fn_arg_ids: vec![vec![ColumnId(1)]],
            grouping_fn_ids: vec![("__grouping_fn_0".to_string(), ColumnId(2))],
        };
        let scalars = ScalarArena::new();
        let child = hash_one();

        let out = op.derive_output(&scalars, &[&child]);

        assert_eq!(out, PhysicalPropertySet::any());
    }
}
