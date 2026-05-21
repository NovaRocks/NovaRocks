//! Shared helpers for "passthrough" operators (Filter, Project, Limit,
//! SubqueryAlias, CTEProduce, Repeat, TableFunction) — operators that
//! preserve their single child's distribution and ordering.

use crate::sql::optimizer::property::PhysicalPropertySet;

/// Output of a passthrough operator equals its single child's output.
#[allow(dead_code)]
pub(crate) fn passthrough_output(
    children_outputs: &[&PhysicalPropertySet],
) -> PhysicalPropertySet {
    children_outputs
        .first()
        .copied()
        .cloned()
        .unwrap_or_else(PhysicalPropertySet::any)
}

/// Required input of a passthrough operator equals its parent's required.
#[allow(dead_code)]
pub(crate) fn passthrough_required(
    parent_required: &PhysicalPropertySet,
) -> Vec<PhysicalPropertySet> {
    vec![parent_required.clone()]
}
