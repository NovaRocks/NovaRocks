//! Shared helpers for "passthrough" operators (Filter, Project, Limit,
//! SubqueryAlias, CTEProduce, Repeat, TableFunction) — operators that
//! preserve their single child's distribution and ordering.

use crate::sql::optimizer::property::PhysicalPropertySet;

/// Output of a passthrough operator equals its single child's output.
pub(crate) fn passthrough_output(children_outputs: &[&PhysicalPropertySet]) -> PhysicalPropertySet {
    children_outputs
        .first()
        .copied()
        .cloned()
        .unwrap_or_else(PhysicalPropertySet::any)
}

/// Required input of a passthrough operator equals its parent's required.
pub(crate) fn passthrough_required(
    parent_required: &PhysicalPropertySet,
) -> Vec<PhysicalPropertySet> {
    vec![parent_required.clone()]
}

use crate::sql::optimizer::operator::{
    PhysicalCTEProduceOp, PhysicalFilterOp, PhysicalLimitOp, PhysicalProjectOp, PhysicalRepeatOp,
    PhysicalSubqueryAliasOp, PhysicalTableFunctionOp,
};

use super::{DeriveOutput, DeriveRequired};

// Output follows the single child via passthrough_output. Required input is
// passed through via passthrough_required.
macro_rules! passthrough_impls {
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
                    passthrough_required(parent_required)
                }
            }
        )+
    };
}

passthrough_impls!(
    PhysicalFilterOp,
    PhysicalProjectOp,
    PhysicalLimitOp,
    PhysicalSubqueryAliasOp,
    PhysicalCTEProduceOp,
    PhysicalRepeatOp,
    PhysicalTableFunctionOp,
);

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

    // --- pre-existing test (kept) ---

    #[test]
    fn filter_passthrough_parent_required() {
        let op = bool_filter();
        let parent_req = PhysicalPropertySet::gather();
        let child_reqs = op.derive_required(&parent_req, 1);
        assert_eq!(child_reqs.len(), 1);
        assert_eq!(child_reqs[0], parent_req);
    }

    // --- new TDD tests for Task 20 ---

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
    fn passthrough_required_returns_parent() {
        let op = make_minimal_limit_op();
        let parent = PhysicalPropertySet::gather();
        let reqs = op.derive_required(&parent, 1);
        assert_eq!(reqs, vec![parent]);
    }

    #[test]
    fn passthrough_no_children_falls_back_to_any() {
        let op = bool_filter();
        let out = op.derive_output(&[]);
        assert_eq!(out, PhysicalPropertySet::any());
    }
}
