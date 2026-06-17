//! Leaf operators: Scan, Values, GenerateSeries, CTEConsume.
//! All return `Any` output and require no children.

use crate::sql::optimizer::operator::{CTEConsumeOp, GenerateSeriesOp, ScanOp, ValuesOp};
use crate::sql::optimizer::property::PhysicalPropertySet;
use crate::sql::optimizer::scalar::ScalarArena;

use super::{DeriveOutput, DeriveRequired};

impl DeriveOutput for ScanOp {
    fn derive_output(
        &self,
        _scalars: &ScalarArena,
        _children: &[&PhysicalPropertySet],
    ) -> PhysicalPropertySet {
        PhysicalPropertySet::any()
    }
}

impl DeriveRequired for ScanOp {
    fn derive_required(
        &self,
        _scalars: &ScalarArena,
        _parent: &PhysicalPropertySet,
        _n: usize,
    ) -> Vec<PhysicalPropertySet> {
        vec![]
    }
}

impl DeriveOutput for ValuesOp {
    fn derive_output(
        &self,
        _scalars: &ScalarArena,
        _children: &[&PhysicalPropertySet],
    ) -> PhysicalPropertySet {
        PhysicalPropertySet::any()
    }
}

impl DeriveRequired for ValuesOp {
    fn derive_required(
        &self,
        _scalars: &ScalarArena,
        _parent: &PhysicalPropertySet,
        _n: usize,
    ) -> Vec<PhysicalPropertySet> {
        vec![]
    }
}

impl DeriveOutput for GenerateSeriesOp {
    fn derive_output(
        &self,
        _scalars: &ScalarArena,
        _children: &[&PhysicalPropertySet],
    ) -> PhysicalPropertySet {
        PhysicalPropertySet::any()
    }
}

impl DeriveRequired for GenerateSeriesOp {
    fn derive_required(
        &self,
        _scalars: &ScalarArena,
        _parent: &PhysicalPropertySet,
        _n: usize,
    ) -> Vec<PhysicalPropertySet> {
        vec![]
    }
}

impl DeriveOutput for CTEConsumeOp {
    fn derive_output(
        &self,
        _scalars: &ScalarArena,
        _children: &[&PhysicalPropertySet],
    ) -> PhysicalPropertySet {
        PhysicalPropertySet::any()
    }
}

impl DeriveRequired for CTEConsumeOp {
    fn derive_required(
        &self,
        _scalars: &ScalarArena,
        _parent: &PhysicalPropertySet,
        _n: usize,
    ) -> Vec<PhysicalPropertySet> {
        vec![]
    }
}
