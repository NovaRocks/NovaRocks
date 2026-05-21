//! Leaf operators: Scan, Values, GenerateSeries, CTEConsume.
//! All return `Any` output and require no children.

use crate::sql::optimizer::operator::{
    PhysicalCTEConsumeOp, PhysicalGenerateSeriesOp, PhysicalScanOp, PhysicalValuesOp,
};
use crate::sql::optimizer::property::PhysicalPropertySet;

use super::{DeriveOutput, DeriveRequired};

impl DeriveOutput for PhysicalScanOp {
    fn derive_output(&self, _children: &[&PhysicalPropertySet]) -> PhysicalPropertySet {
        PhysicalPropertySet::any()
    }
}

impl DeriveRequired for PhysicalScanOp {
    fn derive_required(
        &self,
        _parent: &PhysicalPropertySet,
        _n: usize,
    ) -> Vec<PhysicalPropertySet> {
        vec![]
    }
}

impl DeriveOutput for PhysicalValuesOp {
    fn derive_output(&self, _children: &[&PhysicalPropertySet]) -> PhysicalPropertySet {
        PhysicalPropertySet::any()
    }
}

impl DeriveRequired for PhysicalValuesOp {
    fn derive_required(
        &self,
        _parent: &PhysicalPropertySet,
        _n: usize,
    ) -> Vec<PhysicalPropertySet> {
        vec![]
    }
}

impl DeriveOutput for PhysicalGenerateSeriesOp {
    fn derive_output(&self, _children: &[&PhysicalPropertySet]) -> PhysicalPropertySet {
        PhysicalPropertySet::any()
    }
}

impl DeriveRequired for PhysicalGenerateSeriesOp {
    fn derive_required(
        &self,
        _parent: &PhysicalPropertySet,
        _n: usize,
    ) -> Vec<PhysicalPropertySet> {
        vec![]
    }
}

impl DeriveOutput for PhysicalCTEConsumeOp {
    fn derive_output(&self, _children: &[&PhysicalPropertySet]) -> PhysicalPropertySet {
        PhysicalPropertySet::any()
    }
}

impl DeriveRequired for PhysicalCTEConsumeOp {
    fn derive_required(
        &self,
        _parent: &PhysicalPropertySet,
        _n: usize,
    ) -> Vec<PhysicalPropertySet> {
        vec![]
    }
}
