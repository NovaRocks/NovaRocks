//! Passthrough operators — operators with a single child whose output mirrors
//! the child's output. Two flavours:
//!
//! 1. **Distribution-blind** (Filter / Project / SubqueryAlias / CTEProduce /
//!    Repeat): these operators do not constrain their child's distribution.
//!    Their `derive_required` therefore returns `Any` for the child slot,
//!    letting the optimizer freely choose the cheapest distribution for the
//!    subtree below. Any mismatch with the parent's required distribution is
//!    handled by an enforcer placed **above** the passthrough — preserving
//!    distributed execution of the passthrough's computation (StarRocks
//!    canonical: `Gather → Project → Scan` rather than `Project → Gather →
//!    Scan`).
//!
//! 2. **Full-passthrough** (Limit / TableFunction): the parent's required
//!    distribution is propagated to the child. For Limit this is required
//!    for correctness — a global `LIMIT 10` must run on a single instance,
//!    so the child must already be Gather-ed before Limit fires.
//!    TableFunction's semantics are operator-specific and not generally
//!    distribution-blind; we keep the safe pass-through here.
//!
//! For both flavours `derive_output` is the single child's output (via
//! `passthrough_output`).

use crate::sql::optimizer::property::PhysicalPropertySet;

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
    PhysicalCTEProduceOp, PhysicalFilterOp, PhysicalLimitOp, PhysicalProjectOp, PhysicalRepeatOp,
    PhysicalSubqueryAliasOp, PhysicalTableFunctionOp,
};

use super::{DeriveOutput, DeriveRequired};

/// Distribution-blind passthroughs: `derive_required` returns `[Any]`.
macro_rules! passthrough_distribution_blind_impls {
    ($($op:ty),+ $(,)?) => {
        $(
            impl DeriveOutput for $op {
                fn derive_output(
                    &self,
                    children: &[&PhysicalPropertySet],
                ) -> PhysicalPropertySet {
                    passthrough_output(children)
                }
            }

            impl DeriveRequired for $op {
                fn derive_required(
                    &self,
                    parent_required: &PhysicalPropertySet,
                    _n: usize,
                ) -> Vec<PhysicalPropertySet> {
                    passthrough_required_distribution_blind(parent_required)
                }
            }
        )+
    };
}

passthrough_distribution_blind_impls!(
    PhysicalFilterOp,
    PhysicalProjectOp,
    PhysicalSubqueryAliasOp,
    PhysicalCTEProduceOp,
    PhysicalRepeatOp,
);

/// Full-passthrough operators: `derive_required` forwards `parent_required`.
macro_rules! passthrough_full_impls {
    ($($op:ty),+ $(,)?) => {
        $(
            impl DeriveOutput for $op {
                fn derive_output(
                    &self,
                    children: &[&PhysicalPropertySet],
                ) -> PhysicalPropertySet {
                    passthrough_output(children)
                }
            }

            impl DeriveRequired for $op {
                fn derive_required(
                    &self,
                    parent_required: &PhysicalPropertySet,
                    _n: usize,
                ) -> Vec<PhysicalPropertySet> {
                    passthrough_required_full(parent_required)
                }
            }
        )+
    };
}

passthrough_full_impls!(PhysicalLimitOp, PhysicalTableFunctionOp);

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sql::analysis::{ExprKind, LiteralValue, TypedExpr};
    use crate::sql::column_id::ColumnId;
    use crate::sql::optimizer::operator::{PhysicalFilterOp, PhysicalLimitOp, PhysicalProjectOp};
    use crate::sql::optimizer::property::{DistributionSpec, OrderingSpec, SortKey};
    use arrow::datatypes::DataType;

    fn bool_filter() -> PhysicalFilterOp {
        PhysicalFilterOp {
            predicate: TypedExpr {
                kind: ExprKind::Literal(LiteralValue::Bool(true)),
                data_type: DataType::Boolean,
                nullable: false,
            },
        }
    }

    fn make_minimal_limit_op() -> PhysicalLimitOp {
        PhysicalLimitOp {
            limit: Some(10),
            offset: None,
        }
    }

    fn make_minimal_project_op() -> PhysicalProjectOp {
        PhysicalProjectOp { items: vec![] }
    }

    fn hash_one() -> PhysicalPropertySet {
        PhysicalPropertySet {
            distribution: DistributionSpec::HashPartitioned(vec![ColumnId(1)]),
            ordering: OrderingSpec::Any,
        }
    }

    // --- Required derivation ---

    #[test]
    fn filter_required_is_distribution_blind() {
        // Filter does not constrain its child's distribution: returning [Any]
        // lets the child pick the cheapest plan; any mismatch with the
        // parent's required distribution becomes an enforcer above Filter.
        let op = bool_filter();
        let parent_req = PhysicalPropertySet::gather();
        let child_reqs = op.derive_required(&parent_req, 1);
        assert_eq!(child_reqs.len(), 1);
        assert_eq!(child_reqs[0], PhysicalPropertySet::any());
    }

    #[test]
    fn project_required_is_distribution_blind() {
        let op = make_minimal_project_op();
        let parent_req = PhysicalPropertySet {
            distribution: DistributionSpec::HashPartitioned(vec![ColumnId(1)]),
            ordering: OrderingSpec::Any,
        };
        let child_reqs = op.derive_required(&parent_req, 1);
        assert_eq!(child_reqs.len(), 1);
        assert_eq!(child_reqs[0], PhysicalPropertySet::any());
    }

    #[test]
    fn limit_required_forwards_parent() {
        // Limit is a *full-passthrough*: a global LIMIT must run on a single
        // instance, so the child must already satisfy the parent's
        // distribution (typically Gather) before Limit fires.
        let op = make_minimal_limit_op();
        let parent = PhysicalPropertySet::gather();
        let reqs = op.derive_required(&parent, 1);
        assert_eq!(reqs, vec![parent]);
    }

    // --- Output derivation (follows single child) ---

    #[test]
    fn passthrough_filter_output_follows_child() {
        let op = bool_filter();
        let child = hash_one();
        let out = op.derive_output(&[&child]);
        assert_eq!(out, child);
    }

    #[test]
    fn passthrough_project_output_preserves_ordering() {
        let op = make_minimal_project_op();
        let child = PhysicalPropertySet {
            distribution: DistributionSpec::HashPartitioned(vec![ColumnId(1)]),
            ordering: OrderingSpec::Required(vec![SortKey {
                column: ColumnId(2),
                asc: true,
                nulls_first: false,
            }]),
        };
        let out = op.derive_output(&[&child]);
        assert_eq!(out, child);
    }

    #[test]
    fn passthrough_no_children_falls_back_to_any() {
        let op = bool_filter();
        let out = op.derive_output(&[]);
        assert_eq!(out, PhysicalPropertySet::any());
    }
}
