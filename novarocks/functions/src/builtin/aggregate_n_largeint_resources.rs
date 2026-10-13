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
//! External admission for reached N decimal codec calls. Library invoices are
//! deliberately uncalibrated policy estimates, not private byte upper bounds.
//! Returned String backing has an independent OWN scope. Caller diagnostics,
//! allocator metadata, invocation Data and escaped output remain caller-owned.

use super::aggregate_n_core::NCodecResources;
use crate::KernelFailure;
use crate::aggregate_host_allocator::HostAggregateAllocator;
use crate::aggregate_scalar::{ScalarStateError, ScalarWork};
use crate::kernel_control::{internal, invalid};
use crate::opaque_memory::{OpaqueReservation, OpaqueRetainedCharge};

/// Versioned engineering policy over public calls and reached input bytes.
/// No calibration or internal allocation proof is implied by these invoices.
pub(super) struct NCodecBudgetPolicyV1;
impl NCodecBudgetPolicyV1 {
    const FORMAT_INVOICE: usize = 8192;
    const PARSE_BASE: usize = 4096;
    const PARSE_PER_BYTE: usize = 16;

    fn parse_invoice(text_bytes: usize) -> Result<usize, KernelFailure> {
        text_bytes
            .checked_mul(Self::PARSE_PER_BYTE)
            .and_then(|bytes| bytes.checked_add(Self::PARSE_BASE))
            .ok_or(KernelFailure::ResourceExhausted)
    }
}

// Default signed-i256 ToString writes an optional sign (first capacity8),
// then one magnitude (final capacity<=78): requests/overlap<=86. This OWN
// writer fact is independent of num-bigint's internal initial capacity.
const FORMAT_STRING_PEAK: usize = 86;
const FORMAT_STRING_CAPACITY: usize = 78;

pub(super) struct NCodecLease {
    // Body text is destroyed before this inline lease. Keep the OWN loan until
    // after the library loan; neither field creates new host authority backing.
    _library: OpaqueReservation,
    _returned_string: Option<OpaqueReservation>,
}

pub(super) struct BoundNCodecResources<'a> {
    pub(super) charge: &'a OpaqueRetainedCharge,
    pub(super) allocator: &'a HostAggregateAllocator,
}
impl BoundNCodecResources<'_> {
    fn reserve(&self, bytes: usize) -> Result<OpaqueReservation, KernelFailure> {
        let loan = self.charge.reserve_operation(bytes)?;
        if !loan.belongs_to_allocator(self.allocator) {
            return Err(invalid(
                "min_n/max_n codec reservation uses another host authority",
            ));
        }
        Ok(loan)
    }
}
fn flush(work: &mut ScalarWork<'_, '_>) -> Result<(), KernelFailure> {
    match work.flush() {
        Ok(()) => Ok(()),
        Err(ScalarStateError::Kernel(cause)) => Err(cause),
        Err(_) => Err(internal(
            "min_n/max_n codec flush returned a non-control failure",
        )),
    }
}
impl NCodecResources for BoundNCodecResources<'_> {
    type Lease = NCodecLease;

    fn before_decimal256_format(
        &mut self,
        work: &mut ScalarWork<'_, '_>,
    ) -> Result<Self::Lease, KernelFailure> {
        flush(work)?;
        let returned_string = self.reserve(FORMAT_STRING_PEAK)?;
        let library = self.reserve(NCodecBudgetPolicyV1::FORMAT_INVOICE)?;
        Ok(NCodecLease {
            _library: library,
            _returned_string: Some(returned_string),
        })
    }
    fn after_decimal256_format(
        &mut self,
        _text: &str,
        capacity: usize,
        work: &mut ScalarWork<'_, '_>,
    ) -> Result<(), KernelFailure> {
        // This drift assertion cannot replace admission before the call.
        if capacity > FORMAT_STRING_CAPACITY {
            return Err(internal(
                "min_n/max_n decimal256 formatter exceeded its owned String extent",
            ));
        }
        flush(work)
    }
    fn before_decimal256_parse(
        &mut self,
        text_bytes: usize,
        work: &mut ScalarWork<'_, '_>,
    ) -> Result<Self::Lease, KernelFailure> {
        flush(work)?;
        let bytes = NCodecBudgetPolicyV1::parse_invoice(text_bytes)?;
        let library = self.reserve(bytes)?;
        Ok(NCodecLease {
            _library: library,
            _returned_string: None,
        })
    }
    fn after_decimal256_parse(
        &mut self,
        work: &mut ScalarWork<'_, '_>,
    ) -> Result<(), KernelFailure> {
        flush(work)
    }
}

pub(super) const LEGACY_TEXT_UPPER: usize = 192;
/// Two actual checkpoint/cause diagnostics plus one original error String.
pub(super) fn operation_prefix_bytes() -> Result<usize, KernelFailure> {
    crate::MAX_ROW_ERROR_MESSAGE_BYTES
        .checked_mul(2)
        .and_then(|bytes| bytes.checked_add(768))
        .ok_or(KernelFailure::ResourceExhausted)
}
pub(super) fn add(a: usize, b: usize) -> Result<usize, KernelFailure> {
    a.checked_add(b).ok_or(KernelFailure::ResourceExhausted)
}
fn mul(a: usize, b: usize) -> Result<usize, KernelFailure> {
    a.checked_mul(b).ok_or(KernelFailure::ResourceExhausted)
}
fn bytes<T>(count: usize) -> Result<usize, KernelFailure> {
    std::alloc::Layout::array::<T>(count)
        .map(|layout| layout.size())
        .map_err(|_| KernelFailure::ResourceExhausted)
}
fn arc_bytes<T>() -> Result<usize, KernelFailure> {
    novarocks_type_contract::owned_resources::layout::arc_layout(std::alloc::Layout::new::<T>())
        .map(|layout| layout.size())
        .map_err(|_| KernelFailure::ResourceExhausted)
}

/// Separate named estimates for the original stable sort and Arrow work unit.
/// They are neither measured rates nor private allocation upper bounds.
pub(super) struct NLibraryBudgetPolicyV1;
impl NLibraryBudgetPolicyV1 {
    fn sort(elements: usize) -> Result<usize, KernelFailure> {
        add(
            4096,
            mul(
                2,
                bytes::<crate::aggregate_scalar::TrackedAggScalarValue<HostAggregateAllocator>>(
                    elements,
                )?,
            )?,
        )
    }
    fn arrow(
        rows: usize,
        values: usize,
        payload: usize,
        nodes: usize,
        buffers: usize,
    ) -> Result<usize, KernelFailure> {
        add(
            add(
                add(add(16384, mul(64, add(rows, values)?)?)?, mul(2, payload)?)?,
                mul(2048, nodes)?,
            )?,
            mul(1024, buffers)?,
        )
    }
}
pub(super) struct BoundNSortResources<'a> {
    pub(super) charge: &'a OpaqueRetainedCharge,
    pub(super) allocator: &'a HostAggregateAllocator,
}
impl super::aggregate_n_core::NSortResources for BoundNSortResources<'_> {
    type Lease = OpaqueReservation;
    fn before_sort(
        &mut self,
        len: usize,
        _work: &mut ScalarWork<'_, '_>,
    ) -> Result<Self::Lease, KernelFailure> {
        BoundNCodecResources {
            charge: self.charge,
            allocator: self.allocator,
        }
        .reserve(NLibraryBudgetPolicyV1::sort(len)?)
    }
    fn before_comparison(
        &mut self,
        _work: &mut ScalarWork<'_, '_>,
    ) -> Result<Option<Self::Lease>, KernelFailure> {
        // Both original discarded diagnostic literals are at most39 bytes.
        BoundNCodecResources {
            charge: self.charge,
            allocator: self.allocator,
        }
        .reserve(39)
        .map(Some)
    }
}

#[derive(Default)]
pub(super) struct NEmissionGeometry {
    pub(super) rows: usize,
    pub(super) values: usize,
    utf8: usize,
    encoded: usize,
}
impl NEmissionGeometry {
    pub(super) fn add_state(
        &mut self,
        core: &super::aggregate_n_core::NState<HostAggregateAllocator>,
        work: &mut ScalarWork<'_, '_>,
    ) -> Result<(), ScalarStateError> {
        use crate::aggregate_scalar::TrackedAggScalarValue as V;
        self.rows = add(self.rows, 1)?;
        self.values = add(self.values, core.values.len())?;
        self.encoded = add(self.encoded, 8)?;
        for value in &core.values {
            work.step()?;
            let encoded = match value {
                V::Bool(_) => 2,
                V::Int64(_) | V::Float64(_) | V::Timestamp(_) => 9,
                V::Date32(_) => 5,
                V::Decimal128(_) => 17,
                V::Decimal256(_) => 83,
                V::Utf8(bytes) => {
                    self.utf8 = add(self.utf8, bytes.len())?;
                    add(5, bytes.len())?
                }
                // Private state provenance is Fixed16 update or the original
                // eight-leaf decoder; no public legal codec shape is rejected.
                _ => {
                    return Err(internal(
                        "min_n/max_n emission has an invalid private scalar producer",
                    )
                    .into());
                }
            };
            self.encoded = add(self.encoded, encoded)?;
        }
        Ok(())
    }
    pub(super) fn facts(&self, final_result: bool) -> Result<NEmissionFacts, KernelFailure> {
        use crate::aggregate_scalar::AggScalarValue;
        use crate::arrow_result_custody::custody_node_metadata_upper_bound as node;
        use arrow_array::{BinaryArray, FixedSizeBinaryArray, ListArray};
        let (own_peak, payload, metadata, bodies, nodes, buffers) = if final_result {
            let offsets = bytes::<i32>(add(self.rows, 1)?)?;
            let values = bytes::<u8>(mul(16, self.values)?)?;
            let flat = if self.values == 0 {
                0
            } else {
                mul(
                    2,
                    bytes::<Option<AggScalarValue>>(mul(2, self.values)?.max(4))?,
                )?
            };
            let mut own = add(
                bytes::<Option<AggScalarValue>>(self.rows)?,
                bytes::<Option<AggScalarValue>>(self.values)?,
            )?;
            for term in [
                self.utf8,
                flat,
                bytes::<Option<i128>>(self.values)?,
                offsets,
                values,
                arc_bytes::<ListArray>()?,
                arc_bytes::<FixedSizeBinaryArray>()?,
            ] {
                own = add(own, term)?;
            }
            (
                own,
                add(offsets, values)?,
                add(node(1, false, 1)?, node(1, false, 0)?)?,
                add(size_of::<ListArray>(), size_of::<FixedSizeBinaryArray>())?,
                2,
                2,
            )
        } else {
            let (data_peak, data_capacity) = default_vector::<u8>(1024, self.encoded, 8)?;
            let (offset_peak, offset_capacity) =
                default_vector::<i32>(1025, add(self.rows, 1)?, 4)?;
            // Original finish resets a fresh i32 Vec with its minimum4 elements.
            let own = add(
                add(add(data_peak, offset_peak)?, bytes::<i32>(4)?)?,
                arc_bytes::<BinaryArray>()?,
            )?;
            (
                own,
                add(data_capacity, offset_capacity)?,
                node(2, false, 0)?,
                size_of::<BinaryArray>(),
                1,
                2,
            )
        };
        let library_invoice =
            NLibraryBudgetPolicyV1::arrow(self.rows, self.values, payload, nodes, buffers)?;
        let settlement = add(add(payload, bodies)?, metadata)?;
        // The SAME reservation passed to custody has enough UNUSED backing for
        // its complete stock/metadata transfer, independently of transient peak.
        Ok(NEmissionFacts {
            operation: add(own_peak, library_invoice)?.max(settlement),
            own_peak,
            library_invoice,
            settlement,
        })
    }
}
fn default_vector<T>(
    initial: usize,
    demand: usize,
    minimum: usize,
) -> Result<(usize, usize), KernelFailure> {
    if demand <= initial {
        let bytes = bytes::<T>(initial)?;
        return Ok((bytes, bytes));
    }
    let capacity = add(mul(2, demand)?.max(initial), minimum)?;
    let retained = bytes::<T>(capacity)?;
    Ok((mul(2, retained)?, retained))
}
pub(super) struct NEmissionFacts {
    pub(super) operation: usize,
    pub(super) own_peak: usize,
    pub(super) library_invoice: usize,
    pub(super) settlement: usize,
}

#[cfg(test)]
pub(super) mod tests;
