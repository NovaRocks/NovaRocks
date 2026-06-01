//! AggregateStateMerge: conservative until execution defines distribution.

use crate::sql::optimizer::operator::AggregateStateMergeOp;
use crate::sql::optimizer::property::PhysicalPropertySet;

use super::{DeriveOutput, DeriveRequired};

impl DeriveOutput for AggregateStateMergeOp {
    fn derive_output(&self, _children: &[&PhysicalPropertySet]) -> PhysicalPropertySet {
        PhysicalPropertySet::any()
    }
}

impl DeriveRequired for AggregateStateMergeOp {
    fn derive_required(&self, _parent: &PhysicalPropertySet, n: usize) -> Vec<PhysicalPropertySet> {
        vec![PhysicalPropertySet::any(); n]
    }
}
