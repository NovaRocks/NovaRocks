//! PhysicalDistribution: the distribution enforcer node.
//! Output: whatever its embedded spec says. Required: one Any child.

use crate::sql::optimizer::operator::PhysicalDistributionOp;
use crate::sql::optimizer::property::{OrderingSpec, PhysicalPropertySet};

use super::{DeriveOutput, DeriveRequired};

impl DeriveOutput for PhysicalDistributionOp {
    fn derive_output(&self, _children: &[&PhysicalPropertySet]) -> PhysicalPropertySet {
        PhysicalPropertySet {
            distribution: self.spec.clone(),
            ordering: OrderingSpec::Any,
        }
    }
}

impl DeriveRequired for PhysicalDistributionOp {
    fn derive_required(
        &self,
        _parent: &PhysicalPropertySet,
        _n: usize,
    ) -> Vec<PhysicalPropertySet> {
        vec![PhysicalPropertySet::any()]
    }
}
