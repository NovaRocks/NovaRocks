//! CTEAnchor: structural wiring with two children (produce side, consume side).
//! Today's behaviour: output Any; both children required Any.
//! (CTEConsume lives in scan.rs because it's leaf-like at the property layer.
//!  CTEProduce lives in passthrough.rs because it forwards a single child.)

use crate::sql::optimizer::operator::CTEAnchorOp;
use crate::sql::optimizer::property::PhysicalPropertySet;
use crate::sql::optimizer::scalar::ScalarArena;

use super::{DeriveOutput, DeriveRequired};

impl DeriveOutput for CTEAnchorOp {
    fn derive_output(
        &self,
        _scalars: &ScalarArena,
        _children: &[&PhysicalPropertySet],
    ) -> PhysicalPropertySet {
        PhysicalPropertySet::any()
    }
}

impl DeriveRequired for CTEAnchorOp {
    fn derive_required(
        &self,
        _scalars: &ScalarArena,
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
        let op = CTEAnchorOp { cte_id: 7 };
        let parent_req = PhysicalPropertySet::gather();
        let scalars = ScalarArena::new();
        let child_reqs = op.derive_required(&scalars, &parent_req, 2);
        assert_eq!(child_reqs.len(), 2);
        assert_eq!(child_reqs[0], PhysicalPropertySet::any());
        assert_eq!(child_reqs[1], PhysicalPropertySet::any());
    }
}
