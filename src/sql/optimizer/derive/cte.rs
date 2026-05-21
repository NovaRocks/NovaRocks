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
