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

use crate::datasketches_hll_failure::*;
use datasketches::hll::{HllSketch, HllType, HllUnion};

// Keep the legacy module name to minimize call-site churn while removing the C++ dependency.

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum HllTargetType {
    Hll4,
    Hll6,
    Hll8,
}

/// Allocation evidence for one HLL operation.
///
/// `current_bytes` is the retained accumulator footprint before the operation.
/// `operation_peak_bytes` is the conservative absolute peak while the operation runs. Payload
/// storage and any Arrow owner retaining it remain caller-owned and are excluded.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct HllAllocationUpperBounds {
    pub current_bytes: usize,
    pub operation_peak_bytes: usize,
}

impl HllAllocationUpperBounds {
    /// Additional admission headroom above the already-retained current allocation.
    pub const fn additional_headroom_bytes(self) -> usize {
        self.operation_peak_bytes - self.current_bytes
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum AdmissionMode {
    List,
    /// `lg_k = 5` gives LIST and the union's dense HLL8 array the same 32-byte heap shape.
    /// The substrate's public APIs cannot distinguish them without allocating a clone, so admission
    /// models the sparse LIST transition, which is the allocation-producing alternative.
    ListOrHll,
    Set,
    Hll,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct PayloadProfile {
    lg_k: u8,
    lg_arr: u8,
    mode: AdmissionMode,
    target_type: HllTargetType,
    coupon_count: usize,
    aux_count: usize,
    retained_heap_bytes: usize,
    decode_peak_heap_bytes: usize,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct AccumulatorProfile {
    generation: u64,
    lg_k: u8,
    lg_max_k: u8,
    mode: AdmissionMode,
    coupon_count_upper: usize,
    heap_bytes: usize,
    current_bytes: usize,
    empty: bool,
}

/// Allocation-free evidence for creating a accumulator.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct HllNewPreflight {
    log_k: u8,
    target_type: HllTargetType,
    bounds: HllAllocationUpperBounds,
}

impl HllNewPreflight {
    pub const fn bounds(self) -> HllAllocationUpperBounds {
        self.bounds
    }
}

/// Allocation-free evidence for updating an existing accumulator.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct HllUpdatePreflight {
    accumulator_profile: AccumulatorProfile,
    bounds: HllAllocationUpperBounds,
}

impl HllUpdatePreflight {
    pub const fn bounds(self) -> HllAllocationUpperBounds {
        self.bounds
    }
}

/// Allocation-free evidence for decoding a payload, optionally into an existing accumulator.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct HllPayloadPreflight {
    accumulator_profile: Option<AccumulatorProfile>,
    payload: PayloadProfile,
    bounds: HllAllocationUpperBounds,
}

impl HllPayloadPreflight {
    pub const fn bounds(self) -> HllAllocationUpperBounds {
        self.bounds
    }
    /// Additional pinned decoder error backing, distinct from the original
    /// successful-sketch bound. This is consumed by the new explicit host;
    /// legacy preflight/reserve amounts remain unchanged.
    pub fn library_error_headroom_bytes(self) -> usize {
        match self.payload.mode {
            AdmissionMode::Set => "SET mode contains duplicate coupons"
                .len()
                .max("SET mode coupon count does not match occupied slots".len()),
            AdmissionMode::Hll if self.payload.target_type == HllTargetType::Hll4 => {
                // Unique slots and aux_count are both bounded by actual K.
                // Error::context is Vec::new in all pinned HLL paths.
                let k = 1usize << self.payload.lg_k;
                let digits = k.ilog10() as usize + 1;
                let formatted = "HLL4 auxiliary count is ".len() + ", decoded ".len() + 2 * digits;
                // Rust 1.92 format initial capacity<=2N; each actual growth
                // has old<N and new<=2N, so coexisting backing<3N.
                "HLL4 auxiliary entries must be non-empty and unique"
                    .len()
                    .max(3 * formatted)
            }
            AdmissionMode::List | AdmissionMode::Hll => 0,
            AdmissionMode::ListOrHll => {
                unreachable!("serialized payload headers identify an exact mode")
            }
        }
    }
}

const LIST_HEAP_BYTES: usize = 8 * std::mem::size_of::<u32>();
const HLL_PREAMBLE_BYTES: usize = 40;
const COMPACT_FLAG: u8 = 8;
const EMPTY_FLAG: u8 = 4;
pub(crate) const HLL_FAMILY_ID: u8 = 7;
pub(crate) const HLL_SERIAL_VERSION: u8 = 1;
const AUX_INITIAL_LG: [u8; 22] = [
    0, 2, 2, 2, 2, 2, 2, 3, 3, 3, 4, 4, 5, 5, 6, 7, 8, 9, 10, 11, 12, 13,
];

impl HllTargetType {
    fn into_native(self) -> HllType {
        match self {
            Self::Hll4 => HllType::Hll4,
            Self::Hll6 => HllType::Hll6,
            Self::Hll8 => HllType::Hll8,
        }
    }

    fn from_native(value: HllType) -> Self {
        match value {
            HllType::Hll4 => Self::Hll4,
            HllType::Hll6 => Self::Hll6,
            HllType::Hll8 => Self::Hll8,
        }
    }
}

fn read_u32_le_with_failure<F: HllFailureSink>(
    payload: &[u8],
    offset: usize,
    context: HllDiagnosticContext,
    sink: &mut F,
) -> Result<u32, F::Error> {
    let bytes = payload.get(offset..offset + 4).ok_or_else(|| {
        sink.failure(HllFailureRecipe {
            context,
            detail: HllFailureDetail::Truncated(offset),
        })
    })?;
    Ok(u32::from_le_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]))
}

fn require_payload_len_with_failure<F: HllFailureSink>(
    payload: &[u8],
    required: usize,
    context: HllDiagnosticContext,
    sink: &mut F,
) -> Result<(), F::Error> {
    if payload.len() < required {
        return Err(sink.failure(HllFailureRecipe {
            context,
            detail: HllFailureDetail::Length {
                required,
                actual: payload.len(),
            },
        }));
    }
    Ok(())
}

fn hll4_aux_allocation_observed<E>(
    lg_k: u8,
    count: usize,
    observe: &mut impl FnMut() -> Result<(), E>,
) -> Result<(usize, usize), E> {
    if count == 0 {
        return Ok((0, 0));
    }
    let mut capacity = 1usize << AUX_INITIAL_LG[lg_k as usize];
    let mut peak_capacity = capacity;
    for inserted in 1..=count {
        if 4 * inserted > 3 * capacity {
            let old = capacity;
            capacity *= 2;
            peak_capacity = peak_capacity.max(old + capacity);
        }
        observe()?;
    }
    let coupon_bytes = std::mem::size_of::<u32>();
    Ok((capacity * coupon_bytes, peak_capacity * coupon_bytes))
}

// This parser intentionally understands only the allocation header of the exact pinned 0.5.0
// release; no other version's header layout is accepted. The DataSketches decoder below remains
// the semantic authority for estimator values and body data.
fn payload_profile_with_failure<F: HllFailureSink>(
    payload: &[u8],
    context: HllDiagnosticContext,
    sink: &mut F,
) -> Result<PayloadProfile, F::Error> {
    require_payload_len_with_failure(payload, 8, context, sink)?;
    let preamble_ints = payload[0];
    let serial_version = payload[1];
    let family_id = payload[2];
    let lg_k = payload[3];
    let lg_arr = payload[4];
    let flags = payload[5];
    let state = payload[6];
    let mode_byte = payload[7];
    if serial_version != HLL_SERIAL_VERSION {
        return Err(sink.failure(HllFailureRecipe {
            context,
            detail: HllFailureDetail::Version(serial_version),
        }));
    }
    if family_id != HLL_FAMILY_ID {
        return Err(sink.failure(HllFailureRecipe {
            context,
            detail: HllFailureDetail::Family(family_id),
        }));
    }
    if !(4..=21).contains(&lg_k) {
        return Err(sink.failure(HllFailureRecipe {
            context,
            detail: HllFailureDetail::LogK(lg_k),
        }));
    }
    let target_type = match (mode_byte >> 2) & 3 {
        0 => HllTargetType::Hll4,
        1 => HllTargetType::Hll6,
        2 => HllTargetType::Hll8,
        value => {
            return Err(sink.failure(HllFailureRecipe {
                context,
                detail: HllFailureDetail::Target(value),
            }));
        }
    };
    let compact = flags & COMPACT_FLAG != 0;
    let empty = flags & EMPTY_FLAG != 0;
    let k = 1usize << lg_k;
    match mode_byte & 3 {
        0 => {
            if preamble_ints != 2 || lg_arr != 3 {
                return Err(sink.failure(HllFailureRecipe {
                    context,
                    detail: HllFailureDetail::ListHeader {
                        preamble: preamble_ints,
                        lg_arr,
                    },
                }));
            }
            let coupon_count = usize::from(state);
            if coupon_count > 8 || empty != (coupon_count == 0) {
                return Err(sink.failure(HllFailureRecipe {
                    context,
                    detail: HllFailureDetail::ListCount,
                }));
            }
            let read_count = if compact { coupon_count } else { 8 };
            if !empty {
                require_payload_len_with_failure(payload, 8 + read_count * 4, context, sink)?;
            }
            Ok(PayloadProfile {
                lg_k,
                lg_arr,
                mode: AdmissionMode::List,
                target_type,
                coupon_count,
                aux_count: 0,
                retained_heap_bytes: LIST_HEAP_BYTES,
                decode_peak_heap_bytes: LIST_HEAP_BYTES,
            })
        }
        1 => {
            if preamble_ints != 3 {
                return Err(sink.failure(HllFailureRecipe {
                    context,
                    detail: HllFailureDetail::SetPreamble(preamble_ints),
                }));
            }
            let max_lg_arr = lg_k.saturating_sub(3);
            if !(5..=max_lg_arr).contains(&lg_arr) {
                return Err(sink.failure(HllFailureRecipe {
                    context,
                    detail: HllFailureDetail::SetLgArr {
                        max: max_lg_arr,
                        actual: lg_arr,
                    },
                }));
            }
            require_payload_len_with_failure(payload, 12, context, sink)?;
            let coupon_count = read_u32_le_with_failure(payload, 8, context, sink)? as usize;
            let capacity = 1usize << lg_arr;
            if coupon_count >= capacity {
                return Err(sink.failure(HllFailureRecipe {
                    context,
                    detail: HllFailureDetail::SetCount {
                        count: coupon_count,
                        capacity,
                    },
                }));
            }
            let read_count = if compact { coupon_count } else { capacity };
            require_payload_len_with_failure(payload, 12 + read_count * 4, context, sink)?;
            let heap_bytes = capacity * 4;
            Ok(PayloadProfile {
                lg_k,
                lg_arr,
                mode: AdmissionMode::Set,
                target_type,
                coupon_count,
                aux_count: 0,
                retained_heap_bytes: heap_bytes,
                decode_peak_heap_bytes: heap_bytes,
            })
        }
        2 => {
            if preamble_ints != 10 {
                return Err(sink.failure(HllFailureRecipe {
                    context,
                    detail: HllFailureDetail::HllPreamble(preamble_ints),
                }));
            }
            require_payload_len_with_failure(payload, HLL_PREAMBLE_BYTES, context, sink)?;
            let num_at_cur_min = read_u32_le_with_failure(payload, 32, context, sink)? as usize;
            let aux_count = read_u32_le_with_failure(payload, 36, context, sink)? as usize;
            if num_at_cur_min > k || aux_count > k {
                return Err(sink.failure(HllFailureRecipe {
                    context,
                    detail: HllFailureDetail::RegisterCount,
                }));
            }
            let (retained_heap_bytes, decode_peak_heap_bytes, body_bytes) = match target_type {
                HllTargetType::Hll4 => {
                    let packed = k / 2;
                    let aux_slots = if compact {
                        aux_count
                    } else if aux_count == 0 {
                        0
                    } else {
                        1usize.checked_shl(u32::from(lg_arr)).ok_or_else(|| {
                            sink.failure(HllFailureRecipe {
                                context,
                                detail: HllFailureDetail::Hll4LgArr(lg_arr),
                            })
                        })?
                    };
                    let (aux_retained, aux_peak) =
                        hll4_aux_allocation_observed(lg_k, aux_count, &mut || {
                            sink.observe(HllObservation::Step)
                        })?;
                    let aux_body = aux_slots.checked_mul(4).ok_or_else(|| {
                        sink.failure(HllFailureRecipe {
                            context,
                            detail: HllFailureDetail::AuxOverflow { slots: aux_slots },
                        })
                    })?;
                    let body_bytes = packed.checked_add(aux_body).ok_or_else(|| {
                        sink.failure(HllFailureRecipe {
                            context,
                            detail: HllFailureDetail::PayloadOverflow {
                                packed,
                                auxiliary: aux_body,
                            },
                        })
                    })?;
                    (packed + aux_retained, packed + aux_peak, body_bytes)
                }
                HllTargetType::Hll6 => {
                    if aux_count != 0 {
                        return Err(sink.failure(HllFailureRecipe {
                            context,
                            detail: HllFailureDetail::Hll6Aux,
                        }));
                    }
                    let bytes = 3 * k / 4 + 1;
                    (bytes, bytes, bytes)
                }
                HllTargetType::Hll8 => {
                    if aux_count != 0 {
                        return Err(sink.failure(HllFailureRecipe {
                            context,
                            detail: HllFailureDetail::Hll8Aux,
                        }));
                    }
                    (k, k, k)
                }
            };
            require_payload_len_with_failure(
                payload,
                HLL_PREAMBLE_BYTES + body_bytes,
                context,
                sink,
            )?;
            Ok(PayloadProfile {
                lg_k,
                lg_arr,
                mode: AdmissionMode::Hll,
                target_type,
                coupon_count: 0,
                aux_count,
                retained_heap_bytes,
                decode_peak_heap_bytes,
            })
        }
        value => Err(sink.failure(HllFailureRecipe {
            context,
            detail: HllFailureDetail::Mode(value),
        })),
    }
}

fn empty_accumulator_current_bytes() -> usize {
    std::mem::size_of::<HllAccumulator>() + LIST_HEAP_BYTES
}

fn simulate_coupon_destination_with_failure<F: HllFailureSink>(
    profile: AccumulatorProfile,
    additions: usize,
    sink: &mut F,
) -> Result<usize, F::Error> {
    if additions == 0 || profile.mode == AdmissionMode::Hll {
        return Ok(profile.heap_bytes);
    }
    let k = 1usize << profile.lg_k;
    let mut mode = profile.mode;
    let mut count = profile.coupon_count_upper;
    let mut heap = profile.heap_bytes;
    let mut peak_live_heap = heap;
    let final_count = count.saturating_add(additions);
    if matches!(mode, AdmissionMode::List | AdmissionMode::ListOrHll) && final_count >= 8 {
        let new_heap = if profile.lg_k < 8 { k } else { 32 * 4 };
        peak_live_heap = peak_live_heap.max(heap + new_heap);
        heap = new_heap;
        count = 8;
        mode = if profile.lg_k < 8 {
            AdmissionMode::Hll
        } else {
            AdmissionMode::Set
        };
    }
    if mode == AdmissionMode::Set {
        let mut capacity = heap / 4;
        count = count.max(final_count);
        while 4 * count > 3 * capacity {
            sink.observe(HllObservation::Step)?;
            let new_heap = if capacity == k / 8 { k } else { capacity * 8 };
            peak_live_heap = peak_live_heap.max(heap + new_heap);
            heap = new_heap;
            if capacity == k / 8 {
                break;
            }
            capacity *= 2;
        }
    }
    Ok(peak_live_heap)
}

fn union_workspace_bytes_with_failure<F: HllFailureSink>(
    accumulator_profile: AccumulatorProfile,
    payload: PayloadProfile,
    sink: &mut F,
) -> Result<usize, F::Error> {
    Ok(match payload.mode {
        AdmissionMode::List | AdmissionMode::Set => {
            if accumulator_profile.empty && payload.lg_k == accumulator_profile.lg_k {
                payload.retained_heap_bytes
            } else {
                simulate_coupon_destination_with_failure(
                    accumulator_profile,
                    payload.coupon_count,
                    sink,
                )?
                .saturating_sub(accumulator_profile.heap_bytes)
            }
        }
        AdmissionMode::Hll => {
            let result_lg_k = payload.lg_k.min(accumulator_profile.lg_max_k);
            let result_heap = 1usize << result_lg_k;
            match accumulator_profile.mode {
                _ if accumulator_profile.empty => result_heap,
                AdmissionMode::Hll if payload.lg_k < accumulator_profile.lg_k => {
                    result_heap + accumulator_profile.heap_bytes
                }
                AdmissionMode::Hll => 0,
                AdmissionMode::List | AdmissionMode::Set => result_heap,
                AdmissionMode::ListOrHll if payload.lg_k < accumulator_profile.lg_k => {
                    // At lg_k=5 the 32-byte heap may be either LIST or HLL8. A lower-precision
                    // dense source makes the HLL alternative allocate the replacement array while
                    // cloning the old gadget; the LIST alternative only allocates the result.
                    result_heap + accumulator_profile.heap_bytes
                }
                AdmissionMode::ListOrHll => result_heap,
            }
        }
        AdmissionMode::ListOrHll => {
            unreachable!("serialized payload headers always identify one exact HLL mode")
        }
    })
}

fn deserialize_hll_with_failure<F: HllFailureSink>(
    payload: &[u8],
    context: HllDiagnosticContext,
    sink: &mut F,
) -> Result<HllSketch, F::Error> {
    {
        sink.observe(HllObservation::OpaqueBoundary)?;
        let result = HllSketch::deserialize(payload).map_err(|err| {
            sink.failure(HllFailureRecipe {
                context,
                detail: HllFailureDetail::Deserialize(err),
            })
        });
        if result.is_ok() {
            sink.observe(HllObservation::OpaqueBoundary)?;
        }
        result
    }
}

pub fn hll_estimate(payload: &[u8]) -> Result<i64, ClassifiedHllFailure> {
    hll_estimate_with_failure(payload, &mut RawHllFailure)
}

pub fn hll_estimate_with_failure<F: HllFailureSink>(
    payload: &[u8],
    sink: &mut F,
) -> Result<i64, F::Error> {
    Ok(
        deserialize_hll_with_failure(payload, HllDiagnosticContext::Direct, sink)?
            .estimate()
            .round() as i64,
    )
}

/// Bounds for the pinned library's clone/conversion/serialization request graph.
/// The source accumulator is already retained; these additional bytes do not include
/// Arrow builders, source payloads, allocator usable size or a funding grant.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct HllSerializationBounds {
    pub current_bytes: usize,
    pub converted_retained_heap_bytes: usize,
    pub conversion_peak_heap_bytes: usize,
    pub serializer_peak_heap_bytes: usize,
    pub additional_headroom_bytes: usize,
    pub serialized_payload_bytes: usize,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct HllSerializePreflight {
    accumulator_profile: AccumulatorProfile,
    target_type: HllTargetType,
    bounds: HllSerializationBounds,
}
impl HllSerializePreflight {
    pub const fn bounds(&self) -> HllSerializationBounds {
        self.bounds
    }
}

/// Owned mutable HyperLogLog accumulator. The caller retains the original
/// allocation reservation and retained charge; this value owns the sketch state.
pub struct HllAccumulator {
    target_type: HllTargetType,
    sketch_union: HllUnion,
    generation: u64,
}

impl HllAccumulator {
    pub fn new_allocation_preflight(
        log_k: u8,
        target_type: HllTargetType,
    ) -> Result<HllNewPreflight, ClassifiedHllFailure> {
        Self::new_allocation_preflight_with_failure(log_k, target_type, &mut RawHllFailure)
    }

    pub fn new_allocation_preflight_with_failure<F: HllFailureSink>(
        log_k: u8,
        target_type: HllTargetType,
        sink: &mut F,
    ) -> Result<HllNewPreflight, F::Error> {
        if !(4..=21).contains(&log_k) {
            return Err(sink.failure(HllFailureRecipe {
                context: HllDiagnosticContext::Direct,
                detail: HllFailureDetail::NewLogK(log_k),
            }));
        }
        Ok(HllNewPreflight {
            log_k,
            target_type,
            bounds: HllAllocationUpperBounds {
                current_bytes: 0,
                operation_peak_bytes: empty_accumulator_current_bytes(),
            },
        })
    }

    pub fn new_under_reservation<G>(
        preflight: &HllNewPreflight,
        _reservation: &G,
    ) -> Result<(Self, HllAllocationUpperBounds), ClassifiedHllFailure> {
        Self::new_under_reservation_with_failure(preflight, _reservation, &mut RawHllFailure)
    }

    pub fn new_under_reservation_with_failure<G, F: HllFailureSink>(
        preflight: &HllNewPreflight,
        _reservation: &G,
        sink: &mut F,
    ) -> Result<(Self, HllAllocationUpperBounds), F::Error> {
        let expected = Self::new_allocation_preflight_with_failure(
            preflight.log_k,
            preflight.target_type,
            sink,
        )?;
        if expected != *preflight {
            return Err(sink.invariant(HllInvariantRecipe::NewPreflight));
        }
        sink.observe(HllObservation::OpaqueBoundary)?;
        let accumulator = HllUnion::new(preflight.log_k)
            .map_err(|err| {
                sink.failure(HllFailureRecipe {
                    context: HllDiagnosticContext::Direct,
                    detail: HllFailureDetail::Create(err),
                })
            })
            .map(|sketch_union| Self {
                target_type: preflight.target_type,
                sketch_union,
                generation: 0,
            })?;
        let outcome = HllAllocationUpperBounds {
            current_bytes: accumulator.current_allocation_upper_bound(),
            operation_peak_bytes: preflight.bounds.operation_peak_bytes,
        };
        Ok((accumulator, outcome))
    }

    pub fn new_unreserved(
        log_k: u8,
        target_type: HllTargetType,
    ) -> Result<Self, ClassifiedHllFailure> {
        Self::new_unreserved_with_failure(log_k, target_type, &mut RawHllFailure)
    }

    pub fn new_unreserved_with_failure<F: HllFailureSink>(
        log_k: u8,
        target_type: HllTargetType,
        sink: &mut F,
    ) -> Result<Self, F::Error> {
        let preflight = Self::new_allocation_preflight_with_failure(log_k, target_type, sink)?;
        Self::new_under_reservation_with_failure(&preflight, &(), sink)
            .map(|(accumulator, _)| accumulator)
    }

    pub fn from_payload_allocation_preflight(
        payload: &[u8],
    ) -> Result<HllPayloadPreflight, ClassifiedHllFailure> {
        Self::from_payload_allocation_preflight_with_failure(payload, &mut RawHllFailure)
    }

    pub fn from_payload_allocation_preflight_with_failure<F: HllFailureSink>(
        payload: &[u8],
        sink: &mut F,
    ) -> Result<HllPayloadPreflight, F::Error> {
        let payload =
            payload_profile_with_failure(payload, HllDiagnosticContext::InitialPreflight, sink)?;
        let accumulator_profile = AccumulatorProfile {
            generation: 0,
            lg_k: payload.lg_k,
            lg_max_k: payload.lg_k,
            mode: AdmissionMode::List,
            coupon_count_upper: 0,
            heap_bytes: LIST_HEAP_BYTES,
            current_bytes: empty_accumulator_current_bytes(),
            empty: true,
        };
        let decoded_peak = std::mem::size_of::<HllSketch>() + payload.decode_peak_heap_bytes;
        let union_peak = accumulator_profile.current_bytes
            + std::mem::size_of::<HllSketch>()
            + payload.retained_heap_bytes
            + union_workspace_bytes_with_failure(accumulator_profile, payload, sink)?;
        Ok(HllPayloadPreflight {
            accumulator_profile: None,
            payload,
            bounds: HllAllocationUpperBounds {
                current_bytes: 0,
                operation_peak_bytes: decoded_peak.max(union_peak),
            },
        })
    }

    pub fn from_payload_under_reservation<G>(
        payload: &[u8],
        preflight: &HllPayloadPreflight,
        _reservation: &G,
    ) -> Result<(Self, HllAllocationUpperBounds), ClassifiedHllFailure> {
        Self::from_payload_under_reservation_with_failure(
            payload,
            preflight,
            _reservation,
            &mut RawHllFailure,
        )
    }

    pub fn from_payload_under_reservation_with_failure<G, F: HllFailureSink>(
        payload: &[u8],
        preflight: &HllPayloadPreflight,
        _reservation: &G,
        sink: &mut F,
    ) -> Result<(Self, HllAllocationUpperBounds), F::Error> {
        let expected = Self::from_payload_allocation_preflight_with_failure(payload, sink)?;
        if expected != *preflight {
            return Err(sink.invariant(HllInvariantRecipe::PayloadPreflight));
        }
        let sketch = deserialize_hll_with_failure(payload, HllDiagnosticContext::Direct, sink)?;
        let target_type = HllTargetType::from_native(sketch.target_type());
        sink.observe(HllObservation::OpaqueBoundary)?;
        let mut sketch_union = HllUnion::new(sketch.lg_config_k()).map_err(|err| {
            sink.failure(HllFailureRecipe {
                context: HllDiagnosticContext::Direct,
                detail: HllFailureDetail::Create(err),
            })
        })?;
        sketch_union.update(&sketch);
        let accumulator = Self {
            target_type,
            sketch_union,
            generation: 0,
        };
        let outcome = HllAllocationUpperBounds {
            current_bytes: accumulator.current_allocation_upper_bound(),
            operation_peak_bytes: preflight.bounds.operation_peak_bytes,
        };
        Ok((accumulator, outcome))
    }

    pub fn from_payload_unreserved(payload: &[u8]) -> Result<Self, ClassifiedHllFailure> {
        Self::from_payload_unreserved_with_failure(payload, &mut RawHllFailure)
    }

    pub fn from_payload_unreserved_with_failure<F: HllFailureSink>(
        payload: &[u8],
        sink: &mut F,
    ) -> Result<Self, F::Error> {
        let preflight = Self::from_payload_allocation_preflight_with_failure(payload, sink)?;
        Self::from_payload_under_reservation_with_failure(payload, &preflight, &(), sink)
            .map(|(accumulator, _)| accumulator)
    }

    pub fn update_hash_allocation_preflight(
        &self,
    ) -> Result<HllUpdatePreflight, ClassifiedHllFailure> {
        self.update_hash_allocation_preflight_with_failure(&mut RawHllFailure)
    }
    pub fn update_hash_allocation_preflight_with_failure<F: HllFailureSink>(
        &self,
        sink: &mut F,
    ) -> Result<HllUpdatePreflight, F::Error> {
        sink.observe(HllObservation::OpaqueBoundary)?;
        let accumulator_profile = self.profile();
        sink.observe(HllObservation::OpaqueBoundary)?;
        let peak_heap = simulate_coupon_destination_with_failure(accumulator_profile, 1, sink)?;
        Ok(HllUpdatePreflight {
            accumulator_profile,
            bounds: HllAllocationUpperBounds {
                current_bytes: accumulator_profile.current_bytes,
                operation_peak_bytes: accumulator_profile.current_bytes + peak_heap
                    - accumulator_profile.heap_bytes,
            },
        })
    }

    pub fn update_hash_under_reservation<G>(
        &mut self,
        hash: u64,
        preflight: &HllUpdatePreflight,
        _reservation: &G,
    ) -> Result<HllAllocationUpperBounds, ClassifiedHllFailure> {
        self.update_hash_under_reservation_with_failure(
            hash,
            preflight,
            _reservation,
            &mut RawHllFailure,
        )
    }

    pub fn update_hash_under_reservation_with_failure<G, F: HllFailureSink>(
        &mut self,
        hash: u64,
        preflight: &HllUpdatePreflight,
        _reservation: &G,
        sink: &mut F,
    ) -> Result<HllAllocationUpperBounds, F::Error> {
        let expected = self.update_hash_allocation_preflight_with_failure(sink)?;
        if expected != *preflight {
            return Err(sink.invariant(HllInvariantRecipe::UpdatePreflight));
        }
        let next_generation = self
            .generation
            .checked_add(1)
            .ok_or_else(|| sink.invariant(HllInvariantRecipe::Generation))?;
        sink.observe(HllObservation::OpaqueBoundary)?;
        self.sketch_union.update_value(hash);
        self.generation = next_generation;
        Ok(HllAllocationUpperBounds {
            current_bytes: self.current_allocation_upper_bound(),
            operation_peak_bytes: preflight.bounds.operation_peak_bytes,
        })
    }

    pub fn update_hash_unreserved(&mut self, hash: u64) -> Result<(), ClassifiedHllFailure> {
        self.update_hash_unreserved_with_failure(hash, &mut RawHllFailure)
    }

    pub fn update_hash_unreserved_with_failure<F: HllFailureSink>(
        &mut self,
        hash: u64,
        sink: &mut F,
    ) -> Result<(), F::Error> {
        let preflight = self.update_hash_allocation_preflight_with_failure(sink)?;
        self.update_hash_under_reservation_with_failure(hash, &preflight, &(), sink)
            .map(|_| ())
    }

    pub fn merge_payload_allocation_preflight(
        &self,
        payload: &[u8],
    ) -> Result<HllPayloadPreflight, ClassifiedHllFailure> {
        self.merge_payload_allocation_preflight_with_failure(payload, &mut RawHllFailure)
    }

    pub fn merge_payload_allocation_preflight_with_failure<F: HllFailureSink>(
        &self,
        payload: &[u8],
        sink: &mut F,
    ) -> Result<HllPayloadPreflight, F::Error> {
        sink.observe(HllObservation::OpaqueBoundary)?;
        let accumulator_profile = self.profile();
        sink.observe(HllObservation::OpaqueBoundary)?;
        let payload =
            payload_profile_with_failure(payload, HllDiagnosticContext::MergePreflight, sink)?;
        let decoded_peak = accumulator_profile.current_bytes
            + std::mem::size_of::<HllSketch>()
            + payload.decode_peak_heap_bytes;
        let union_peak = accumulator_profile.current_bytes
            + std::mem::size_of::<HllSketch>()
            + payload.retained_heap_bytes
            + union_workspace_bytes_with_failure(accumulator_profile, payload, sink)?;
        Ok(HllPayloadPreflight {
            accumulator_profile: Some(accumulator_profile),
            payload,
            bounds: HllAllocationUpperBounds {
                current_bytes: accumulator_profile.current_bytes,
                operation_peak_bytes: decoded_peak.max(union_peak),
            },
        })
    }

    pub fn merge_payload_under_reservation<G>(
        &mut self,
        payload: &[u8],
        preflight: &HllPayloadPreflight,
        _reservation: &G,
    ) -> Result<HllAllocationUpperBounds, ClassifiedHllFailure> {
        self.merge_payload_under_reservation_with_failure(
            payload,
            preflight,
            _reservation,
            &mut RawHllFailure,
        )
    }

    pub fn merge_payload_under_reservation_with_failure<G, F: HllFailureSink>(
        &mut self,
        payload: &[u8],
        preflight: &HllPayloadPreflight,
        _reservation: &G,
        sink: &mut F,
    ) -> Result<HllAllocationUpperBounds, F::Error> {
        let expected = self.merge_payload_allocation_preflight_with_failure(payload, sink)?;
        if expected != *preflight {
            return Err(sink.invariant(HllInvariantRecipe::MergePreflight));
        }
        let next_generation = self
            .generation
            .checked_add(1)
            .ok_or_else(|| sink.invariant(HllInvariantRecipe::Generation))?;
        let sketch = deserialize_hll_with_failure(payload, HllDiagnosticContext::Direct, sink)?;
        sink.observe(HllObservation::OpaqueBoundary)?;
        self.sketch_union.update(&sketch);
        self.generation = next_generation;
        Ok(HllAllocationUpperBounds {
            current_bytes: self.current_allocation_upper_bound(),
            operation_peak_bytes: preflight.bounds.operation_peak_bytes,
        })
    }

    pub fn merge_payload_unreserved(
        &mut self,
        payload: &[u8],
    ) -> Result<HllAllocationUpperBounds, ClassifiedHllFailure> {
        self.merge_payload_unreserved_with_failure(payload, &mut RawHllFailure)
    }

    pub fn merge_payload_unreserved_with_failure<F: HllFailureSink>(
        &mut self,
        payload: &[u8],
        sink: &mut F,
    ) -> Result<HllAllocationUpperBounds, F::Error> {
        let preflight = self.merge_payload_allocation_preflight_with_failure(payload, sink)?;
        self.merge_payload_under_reservation_with_failure(payload, &preflight, &(), sink)
    }

    /// Caller checkpoints surround opaque profile work. The callback here
    /// observes each iteration of the original auxiliary-capacity author.
    pub fn serialization_allocation_preflight_observed<E>(
        &self,
        observe: &mut impl FnMut() -> Result<(), E>,
    ) -> Result<HllSerializePreflight, E> {
        let accumulator_profile = self.profile();
        let k = 1usize << accumulator_profile.lg_k;
        // Sparse containers clone their actual Box<[Coupon]> backing. Their
        // compact output never contains more coupons than that backing.
        let sparse = |preamble: usize| {
            let payload = preamble + accumulator_profile.heap_bytes;
            HllSerializationBounds {
                current_bytes: accumulator_profile.current_bytes,
                converted_retained_heap_bytes: accumulator_profile.heap_bytes,
                conversion_peak_heap_bytes: accumulator_profile.heap_bytes,
                serializer_peak_heap_bytes: accumulator_profile.heap_bytes + payload,
                additional_headroom_bytes: accumulator_profile.heap_bytes + payload,
                serialized_payload_bytes: payload,
            }
        };
        if matches!(
            accumulator_profile.mode,
            AdmissionMode::List | AdmissionMode::Set
        ) {
            let bounds = sparse(if accumulator_profile.mode == AdmissionMode::List {
                8
            } else {
                12
            });
            return Ok(HllSerializePreflight {
                accumulator_profile,
                target_type: self.target_type,
                bounds,
            });
        }
        let dense = match self.target_type {
            HllTargetType::Hll8 => {
                let payload = HLL_PREAMBLE_BYTES + k;
                HllSerializationBounds {
                    current_bytes: accumulator_profile.current_bytes,
                    converted_retained_heap_bytes: k,
                    conversion_peak_heap_bytes: k,
                    serializer_peak_heap_bytes: k + payload,
                    additional_headroom_bytes: k + payload,
                    serialized_payload_bytes: payload,
                }
            }
            HllTargetType::Hll6 => {
                let packed = 3 * k / 4 + 1;
                let payload = HLL_PREAMBLE_BYTES + packed;
                HllSerializationBounds {
                    current_bytes: accumulator_profile.current_bytes,
                    converted_retained_heap_bytes: packed,
                    conversion_peak_heap_bytes: packed,
                    serializer_peak_heap_bytes: packed + payload,
                    additional_headroom_bytes: packed + payload,
                    serialized_payload_bytes: payload,
                }
            }
            HllTargetType::Hll4 => {
                let packed = k / 2;
                let (aux_retained, aux_growth_peak) =
                    hll4_aux_allocation_observed(accumulator_profile.lg_k, k, observe)?;
                // cur_min rebuilding retains old_aux while constructing and
                // growing new_aux. Include all three actual map backings.
                let conversion_peak = packed + aux_retained + aux_growth_peak;
                let converted_retained = packed + aux_retained;
                let payload = HLL_PREAMBLE_BYTES + packed + k * std::mem::size_of::<u32>();
                // Pinned Rust 1.92: borrowed FilterMap has lower bound zero;
                // collect starts at four and doubles capacity. K>=16 is a
                // power of two, so final capacity<=K and old+new<=2K.
                let entries = k * std::mem::size_of::<(u32, u8)>();
                let collector_growth_peak = 2 * entries;
                let serializer_peak =
                    converted_retained + collector_growth_peak.max(entries + payload);
                HllSerializationBounds {
                    current_bytes: accumulator_profile.current_bytes,
                    converted_retained_heap_bytes: converted_retained,
                    conversion_peak_heap_bytes: conversion_peak,
                    serializer_peak_heap_bytes: serializer_peak,
                    additional_headroom_bytes: conversion_peak.max(serializer_peak),
                    serialized_payload_bytes: payload,
                }
            }
        };
        let bounds = match accumulator_profile.mode {
            AdmissionMode::List => sparse(8),
            AdmissionMode::Set => sparse(12),
            AdmissionMode::Hll => dense,
            AdmissionMode::ListOrHll => {
                let sparse = sparse(8);
                HllSerializationBounds {
                    current_bytes: accumulator_profile.current_bytes,
                    converted_retained_heap_bytes: sparse
                        .converted_retained_heap_bytes
                        .max(dense.converted_retained_heap_bytes),
                    conversion_peak_heap_bytes: sparse
                        .conversion_peak_heap_bytes
                        .max(dense.conversion_peak_heap_bytes),
                    serializer_peak_heap_bytes: sparse
                        .serializer_peak_heap_bytes
                        .max(dense.serializer_peak_heap_bytes),
                    additional_headroom_bytes: sparse
                        .additional_headroom_bytes
                        .max(dense.additional_headroom_bytes),
                    serialized_payload_bytes: sparse
                        .serialized_payload_bytes
                        .max(dense.serialized_payload_bytes),
                }
            }
        };
        Ok(HllSerializePreflight {
            accumulator_profile,
            target_type: self.target_type,
            bounds,
        })
    }

    /// The reservation is actual caller authority, not a stand-in allocation.
    /// Legacy serialize remains unchanged. This method validates identity
    /// without repeating the potentially long bound walk after admission.
    pub fn serialize_under_reservation<G>(
        &self,
        preflight: &HllSerializePreflight,
        _reservation: &G,
    ) -> Result<Vec<u8>, ClassifiedHllFailure> {
        self.serialize_under_reservation_with_failure(preflight, _reservation, &mut RawHllFailure)
    }

    pub fn serialize_under_reservation_with_failure<G, F: HllFailureSink>(
        &self,
        preflight: &HllSerializePreflight,
        _reservation: &G,
        sink: &mut F,
    ) -> Result<Vec<u8>, F::Error> {
        if self.profile() != preflight.accumulator_profile
            || self.target_type != preflight.target_type
        {
            return Err(sink.invariant(HllInvariantRecipe::SerializePreflight));
        }
        self.serialize_with_failure(sink)
    }

    pub fn serialize(&self) -> Result<Vec<u8>, ClassifiedHllFailure> {
        self.serialize_with_failure(&mut RawHllFailure)
    }

    pub fn serialize_with_failure<F: HllFailureSink>(
        &self,
        sink: &mut F,
    ) -> Result<Vec<u8>, F::Error> {
        sink.observe(HllObservation::OpaqueBoundary)?;
        let result = self
            .sketch_union
            .to_sketch(self.target_type.into_native())
            .serialize();
        sink.observe(HllObservation::OpaqueBoundary)?;
        Ok(result)
    }

    pub fn estimate(&self) -> Result<i64, ClassifiedHllFailure> {
        self.estimate_with_failure(&mut RawHllFailure)
    }

    pub fn estimate_with_failure<F: HllFailureSink>(&self, sink: &mut F) -> Result<i64, F::Error> {
        sink.observe(HllObservation::OpaqueBoundary)?;
        let result = self.sketch_union.estimate().round() as i64;
        sink.observe(HllObservation::OpaqueBoundary)?;
        Ok(result)
    }

    /// Returns a conservative upper bound for the live accumulator's current allocation footprint.
    ///
    /// The bound uses DataSketches' capacity-aware `estimated_size()` and includes this wrapper's
    /// inline bytes. It does not use the serialized payload length or derive retained memory from
    /// `lg_k`.
    pub fn current_allocation_upper_bound(&self) -> usize {
        std::mem::size_of::<HllAccumulator>() - std::mem::size_of::<HllUnion>()
            + self.sketch_union.estimated_size()
    }

    fn profile(&self) -> AccumulatorProfile {
        let current_bytes = self.current_allocation_upper_bound();
        let heap_bytes = self.sketch_union.estimated_size() - std::mem::size_of::<HllUnion>();
        let lg_k = self.sketch_union.lg_config_k();
        let k = 1usize << lg_k;
        let empty = self.sketch_union.is_empty();
        // In the exact pinned substrate, LIST/SET Container::estimate() is structurally floored by
        // the exact container length (`len.max(interpolated_estimate)`). Its ceiling is therefore
        // an allocation-free coupon-count upper bound, including externally accepted LIST(8) and
        // near-full SET images cloned into an empty union. Dense states ignore this value except
        // for the conservative lg_k=5 LIST alternative below.
        let sparse_coupon_count_upper = self.sketch_union.estimate().ceil().max(0.0) as usize;
        let (mode, coupon_count_upper) = if heap_bytes == LIST_HEAP_BYTES {
            if lg_k == 5 && !empty {
                // LIST and dense HLL8 both retain 32 bytes at lg_k=5. Treat the state as LIST for
                // allocation purposes; a real dense update allocates less than this alternative.
                (AdmissionMode::ListOrHll, sparse_coupon_count_upper.max(8))
            } else {
                (
                    AdmissionMode::List,
                    if empty { 0 } else { sparse_coupon_count_upper },
                )
            }
        } else if heap_bytes == k {
            (AdmissionMode::Hll, 0)
        } else {
            (AdmissionMode::Set, sparse_coupon_count_upper)
        };
        AccumulatorProfile {
            generation: self.generation,
            lg_k,
            lg_max_k: self.sketch_union.lg_max_k(),
            mode,
            coupon_count_upper,
            heap_bytes,
            current_bytes,
            empty,
        }
    }
}

/*
 * Aggregate callers preflight and reserve each operation, then reconcile the actual retained
 * allocation reported by the accumulator before releasing headroom. Callers without such an admission
 * owner use the explicitly named unreserved methods.
 */

#[cfg(test)]
#[path = "datasketches_hll_original_tests.rs"]
mod original_tests;

#[cfg(test)]
mod typed_generation_failure_tests {
    use super::*;
    use crate::KernelFailure;
    use novarocks_failure_contract::{FailureClass, FailureProjection};
    struct TypedSink;
    impl HllFailureSink for TypedSink {
        type Error = KernelFailure;
        fn failure(&mut self, source: HllFailureRecipe) -> KernelFailure {
            match source.classify() {
                crate::datasketches_hll_failure::ClassifiedHllFailure::Outer(cause) => {
                    KernelFailure::from_domain(cause)
                }
                crate::datasketches_hll_failure::ClassifiedHllFailure::Value(value) => {
                    panic!("unexpected value failure in the valid generation setup: {value}")
                }
            }
        }
        fn invariant(&mut self, source: HllInvariantRecipe) -> KernelFailure {
            KernelFailure::from_domain(source)
        }
        fn observe(&mut self, _: HllObservation) -> Result<(), KernelFailure> {
            Ok(())
        }
    }
    #[test]
    fn actual_generation_exhaustion_is_typed_and_precedes_the_accumulator_mutation() {
        let mut sink = TypedSink;
        let mut accumulator =
            HllAccumulator::new_unreserved_with_failure(4, HllTargetType::Hll6, &mut sink).unwrap();
        accumulator.generation = u64::MAX;
        let before = accumulator.serialize_with_failure(&mut sink).unwrap();
        let preflight = accumulator
            .update_hash_allocation_preflight_with_failure(&mut sink)
            .unwrap();
        // Deliberately no capacity authority: this semantic refusal must happen
        // before the operation can perform its mutation or allocation.
        struct NoReservation;
        let error = accumulator
            .update_hash_under_reservation_with_failure(11, &preflight, &NoReservation, &mut sink)
            .unwrap_err();
        let fact = error.failure_fact();
        assert_eq!(fact.class(), FailureClass::Resource);
        assert_eq!(fact.reason().code(), 183);
        assert_eq!(accumulator.generation, u64::MAX);
        assert_eq!(
            accumulator.serialize_with_failure(&mut sink).unwrap(),
            before
        );
    }
}
