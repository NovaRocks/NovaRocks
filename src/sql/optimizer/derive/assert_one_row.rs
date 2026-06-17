//! AssertOneRow — runtime guard that its input yields at most one row.
//!
//! The row count must be observed globally, so the child is required to be
//! gathered to a single instance before the assert fires (same correctness
//! argument as a global LIMIT). Output mirrors the child's output; ordering
//! requirements pass through.

use crate::sql::optimizer::operator::AssertOneRowOp;
use crate::sql::optimizer::property::{DistributionSpec, PhysicalPropertySet};
use crate::sql::optimizer::scalar::ScalarArena;

use super::passthrough::passthrough_output;
use super::{DeriveOutput, DeriveRequired};

impl DeriveOutput for AssertOneRowOp {
    fn derive_output(
        &self,
        _scalars: &ScalarArena,
        children_outputs: &[&PhysicalPropertySet],
    ) -> PhysicalPropertySet {
        passthrough_output(children_outputs)
    }
}

impl DeriveRequired for AssertOneRowOp {
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
