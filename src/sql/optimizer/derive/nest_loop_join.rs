//! NestLoopJoin: always Gather both inputs; output Gather.

use crate::sql::optimizer::operator::PhysicalNestLoopJoinOp;
use crate::sql::optimizer::property::PhysicalPropertySet;

use super::{DeriveOutput, DeriveRequired};

impl DeriveOutput for PhysicalNestLoopJoinOp {
    fn derive_output(&self, _children: &[&PhysicalPropertySet]) -> PhysicalPropertySet {
        PhysicalPropertySet::gather()
    }
}

impl DeriveRequired for PhysicalNestLoopJoinOp {
    fn derive_required(
        &self,
        _parent: &PhysicalPropertySet,
        _n: usize,
    ) -> Vec<PhysicalPropertySet> {
        vec![PhysicalPropertySet::gather(), PhysicalPropertySet::gather()]
    }
}
