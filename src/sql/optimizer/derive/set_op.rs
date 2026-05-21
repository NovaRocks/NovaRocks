//! Set operators: Union, Intersect, Except.
//! Today's behaviour: output Any; each child required Any.

use crate::sql::optimizer::operator::{PhysicalExceptOp, PhysicalIntersectOp, PhysicalUnionOp};
use crate::sql::optimizer::property::PhysicalPropertySet;

use super::{DeriveOutput, DeriveRequired};

macro_rules! set_op_impls {
    ($($op:ty),+ $(,)?) => {
        $(
            impl DeriveOutput for $op {
                fn derive_output(
                    &self,
                    _children: &[&PhysicalPropertySet],
                ) -> PhysicalPropertySet {
                    PhysicalPropertySet::any()
                }
            }

            impl DeriveRequired for $op {
                fn derive_required(
                    &self,
                    _parent: &PhysicalPropertySet,
                    n: usize,
                ) -> Vec<PhysicalPropertySet> {
                    vec![PhysicalPropertySet::any(); n]
                }
            }
        )+
    };
}

set_op_impls!(PhysicalUnionOp, PhysicalIntersectOp, PhysicalExceptOp);
