//! AggregateStateMerge: conservative until execution defines distribution.

use crate::sql::optimizer::operator::PhysicalAggregateStateMergeOp;
use crate::sql::optimizer::property::PhysicalPropertySet;

use super::{DeriveOutput, DeriveRequired};

impl DeriveOutput for PhysicalAggregateStateMergeOp {
    fn derive_output(&self, _children: &[&PhysicalPropertySet]) -> PhysicalPropertySet {
        PhysicalPropertySet::any()
    }
}

impl DeriveRequired for PhysicalAggregateStateMergeOp {
    fn derive_required(&self, _parent: &PhysicalPropertySet, n: usize) -> Vec<PhysicalPropertySet> {
        vec![PhysicalPropertySet::any(); n]
    }
}
