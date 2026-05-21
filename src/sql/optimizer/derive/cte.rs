//! CTEAnchor: structural wiring with two children (produce side, consume side).
//! Today's behaviour: output Any; both children required Any.
//! (CTEConsume lives in scan.rs because it's leaf-like at the property layer.
//!  CTEProduce lives in passthrough.rs because it forwards a single child.)

use crate::sql::optimizer::operator::PhysicalCTEAnchorOp;
use crate::sql::optimizer::property::PhysicalPropertySet;

use super::{DeriveOutput, DeriveRequired};

impl DeriveOutput for PhysicalCTEAnchorOp {
    fn derive_output(&self, _children: &[&PhysicalPropertySet]) -> PhysicalPropertySet {
        PhysicalPropertySet::any()
    }
}

impl DeriveRequired for PhysicalCTEAnchorOp {
    fn derive_required(
        &self,
        _parent: &PhysicalPropertySet,
        _n: usize,
    ) -> Vec<PhysicalPropertySet> {
        vec![PhysicalPropertySet::any(), PhysicalPropertySet::any()]
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cte_anchor_requires_any_for_both_children() {
        let op = PhysicalCTEAnchorOp { cte_id: 7 };
        let parent_req = PhysicalPropertySet::gather();
        let child_reqs = op.derive_required(&parent_req, 2);
        assert_eq!(child_reqs.len(), 2);
        assert_eq!(child_reqs[0], PhysicalPropertySet::any());
        assert_eq!(child_reqs[1], PhysicalPropertySet::any());
    }
}
