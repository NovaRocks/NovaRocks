// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//! Original Arrow take's diagnostic allocation and full-text last-Drop
//! custody. This is host geometry, not another error renderer or value decoder.
use super::{CopyError, add, buffer_extent, mul};
use crate::KernelFailure;
use crate::opaque_memory::{OpaqueReservation, OpaqueRetainedCharge};
use arrow_schema::ArrowError;
use std::fmt::{self, Write};

pub struct OriginalCopyData {
    // Drop text before releasing the real retained diagnostic charge.
    text: String,
    charge: OpaqueRetainedCharge,
}
impl OriginalCopyData {
    pub fn text(&self) -> &str {
        &self.text
    }
    pub fn retained_bytes(&self) -> usize {
        self.charge.bytes()
    }
}
impl fmt::Debug for OriginalCopyData {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_tuple("OriginalCopyData").field(&self.text).finish()
    }
}
impl fmt::Display for OriginalCopyData {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.text)
    }
}
impl PartialEq<String> for OriginalCopyData {
    fn eq(&self, other: &String) -> bool {
        self.text == *other
    }
}
impl std::ops::Deref for OriginalCopyData {
    type Target = str;
    fn deref(&self) -> &str {
        &self.text
    }
}

/// Allocating take(None) descriptions include Arrow60.1 nullable Union/Run
/// and checked FixedSizeList offset errors beside original validation text.
/// The caller supplies UInt32/UInt64, so the noninteger-index type formatter
/// and check_bounds formatter are not reachable. Unsupported value types panic
/// in the original kernel and keep their original host panic path.
#[derive(Clone, Copy, Debug)]
pub(super) struct OriginalTakeDiagnosticFacts {
    display_upper: usize,
    peak: usize,
}
impl OriginalTakeDiagnosticFacts {
    pub(super) fn from_original_geometry(display_upper: usize, peak: usize) -> Self {
        Self {
            display_upper,
            peak,
        }
    }
    pub(super) fn try_new() -> Result<Self, CopyError> {
        let mut maximum = usize::MAX;
        let mut digits = 1;
        while maximum >= 10 {
            maximum /= 10;
            digits += 1;
        }
        // Original RunArray::get_physical_indices numeric message; two usize
        // fields. PrimitiveArray's try_new uses the same two-length format.
        let run = add(
            "Logical index  is out of bounds for RunArray of length ".len(),
            mul(2, digits)?,
        )?;
        let primitive = add(
            "Incorrect length of null buffer for PrimitiveArray, expected  got ".len(),
            mul(2, digits)?,
        )?;
        let union =
            "Sparse union child arrays must be equal in length to the length of the union".len();
        let fixed = add("Cannot convert size '' to usize".len(), "-2147483648".len())?;
        let nullable_union =
            "Cannot take null indices from a union with no field that can represent nulls".len();
        let nullable_run =
            "Cannot take null indices from a RunEndEncoded array with a non-nullable values field"
                .len();
        let fixed_offset = "FixedSizeList offset overflows u32".len();
        let index_cast = "Cast to usize failed".len();
        let description_upper = run
            .max(primitive)
            .max(union)
            .max(fixed)
            .max(nullable_union)
            .max(nullable_run)
            .max(fixed_offset)
            .max(index_cast);
        let display_upper = add(description_upper, "Invalid argument error: ".len())?;
        // std String/Vec<u8> growth: initial literal hint <=2*formatted length;
        // reserve doubles or uses required length, with minimum nonzero 8.
        // Original description + original Display output coexist. This also
        // covers numeric-only OffsetOverflowError's original Display.
        let peak = add(
            mul(2, description_upper.max(8))?,
            mul(3, display_upper.max(8))?,
        )?;
        buffer_extent(peak, 1)?;
        Ok(Self {
            display_upper,
            peak,
        })
    }
    pub(super) fn try_new_observed<E: From<CopyError>>(
        ty: &arrow_schema::DataType,
        work: &mut impl FnMut(bool) -> Result<(), E>,
    ) -> Result<Self, E> {
        let mut facts = Self::try_new()?;
        if let Some((display, peak)) =
            super::concat_diagnostic::original_take_validation_diagnostic_observed(ty, work)?
        {
            facts.display_upper = facts.display_upper.max(display);
            facts.peak = facts.peak.max(peak);
        }
        Ok(facts)
    }
    pub(super) fn operation_peak_bytes(&self) -> usize {
        self.peak
    }
    pub(super) fn retain(
        self,
        error: ArrowError,
        mut charge: OpaqueRetainedCharge,
        reservation: &mut OpaqueReservation,
    ) -> Result<OriginalCopyData, KernelFailure> {
        // Count the SAME borrowed Display output without allocating. No fallible
        // control footer follows a data error. The earlier real grant already
        // covers both description and String construction, before this call.
        struct Count(usize);
        impl Write for Count {
            fn write_str(&mut self, text: &str) -> fmt::Result {
                self.0 = self.0.checked_add(text.len()).ok_or(fmt::Error)?;
                Ok(())
            }
        }
        let mut count = Count(0);
        fmt::write(&mut count, format_args!("{error}")).map_err(|_| {
            crate::kernel_control::invalid("original copy diagnostic extent is not representable")
        })?;
        if count.0 > self.display_upper {
            return Err(crate::kernel_control::invalid(
                "original copy diagnostic exceeded its pinned author envelope",
            ));
        }
        let text = error.to_string();
        drop(error);
        charge.reconcile_under_reservation(text.capacity(), reservation)?;
        Ok(OriginalCopyData { text, charge })
    }
}
