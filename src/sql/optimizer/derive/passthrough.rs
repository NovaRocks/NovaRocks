//! Shared helpers for "passthrough" operators (Filter, Project, Limit,
//! SubqueryAlias, CTEProduce, Repeat, TableFunction) — operators that
//! preserve their single child's distribution and ordering.

use crate::sql::optimizer::property::PhysicalPropertySet;

/// Output of a passthrough operator equals its single child's output.
#[allow(dead_code)]
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

// Refactor-phase impls: output is still Any (today's behaviour). Task 20
// flips this to passthrough_output(children_outputs).
macro_rules! passthrough_impls {
    ($($op:ty),+ $(,)?) => {
        $(
            impl DeriveOutput for $op {
                fn derive_output(
                    &self,
                    _children: &[&PhysicalPropertySet],
                ) -> PhysicalPropertySet {
                    PhysicalPropertySet::any()
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

    #[test]
    fn filter_passthrough_parent_required() {
        let op = PhysicalFilterOp {
            predicate: crate::sql::analysis::TypedExpr {
                kind: crate::sql::analysis::ExprKind::Literal(
                    crate::sql::analysis::LiteralValue::Bool(true),
                ),
                data_type: arrow::datatypes::DataType::Boolean,
                nullable: false,
            },
        };
        let parent_req = PhysicalPropertySet::gather();
        let child_reqs = op.derive_required(&parent_req, 1);
        assert_eq!(child_reqs.len(), 1);
        assert_eq!(child_reqs[0], parent_req);
    }
}
