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
/// `current_bytes` is the retained handle footprint before the operation.
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
    /// Public RC1 APIs cannot distinguish them without allocating a clone, so admission models
    /// the sparse LIST transition, which is the allocation-producing alternative.
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
struct HandleProfile {
    generation: u64,
    lg_k: u8,
    lg_max_k: u8,
    mode: AdmissionMode,
    coupon_count_upper: usize,
    heap_bytes: usize,
    current_bytes: usize,
    empty: bool,
}

/// Allocation-free evidence for creating a handle.
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

/// Allocation-free evidence for updating an existing handle.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct HllUpdatePreflight {
    handle: HandleProfile,
    bounds: HllAllocationUpperBounds,
}

impl HllUpdatePreflight {
    pub const fn bounds(self) -> HllAllocationUpperBounds {
        self.bounds
    }
}

/// Allocation-free evidence for decoding a payload, optionally into an existing handle.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct HllPayloadPreflight {
    handle: Option<HandleProfile>,
    payload: PayloadProfile,
    bounds: HllAllocationUpperBounds,
}

impl HllPayloadPreflight {
    pub const fn bounds(self) -> HllAllocationUpperBounds {
        self.bounds
    }
}

const LIST_HEAP_BYTES: usize = 8 * std::mem::size_of::<u32>();
const HLL_PREAMBLE_BYTES: usize = 40;
const COMPACT_FLAG: u8 = 8;
const EMPTY_FLAG: u8 = 4;
const HLL_FAMILY_ID: u8 = 7;
const HLL_SERIAL_VERSION: u8 = 1;
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

fn read_u32_le(payload: &[u8], offset: usize, context: &str) -> Result<u32, String> {
    let bytes = payload
        .get(offset..offset + 4)
        .ok_or_else(|| format!("{context}: HLL header is truncated at byte {offset}"))?;
    Ok(u32::from_le_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]))
}

fn require_payload_len(payload: &[u8], required: usize, context: &str) -> Result<(), String> {
    if payload.len() < required {
        return Err(format!(
            "{context}: HLL payload requires {required} bytes, got {}",
            payload.len()
        ));
    }
    Ok(())
}

fn hll4_aux_allocation(lg_k: u8, count: usize) -> (usize, usize) {
    if count == 0 {
        return (0, 0);
    }
    let mut capacity = 1usize << AUX_INITIAL_LG[lg_k as usize];
    let mut peak_capacity = capacity;
    for inserted in 1..=count {
        if 4 * inserted > 3 * capacity {
            let old = capacity;
            capacity *= 2;
            peak_capacity = peak_capacity.max(old + capacity);
        }
    }
    let coupon_bytes = std::mem::size_of::<u32>();
    (capacity * coupon_bytes, peak_capacity * coupon_bytes)
}

// This parser intentionally understands only the fixed 0.5.0-rc.1 allocation header. The
// DataSketches decoder below remains the semantic authority for estimator values and body data.
fn payload_profile(payload: &[u8], context: &str) -> Result<PayloadProfile, String> {
    require_payload_len(payload, 8, context)?;
    let preamble_ints = payload[0];
    let serial_version = payload[1];
    let family_id = payload[2];
    let lg_k = payload[3];
    let lg_arr = payload[4];
    let flags = payload[5];
    let state = payload[6];
    let mode_byte = payload[7];
    if serial_version != HLL_SERIAL_VERSION {
        return Err(format!(
            "{context}: expected HLL serial version {HLL_SERIAL_VERSION}, got {serial_version}"
        ));
    }
    if family_id != HLL_FAMILY_ID {
        return Err(format!(
            "{context}: expected HLL family {HLL_FAMILY_ID}, got {family_id}"
        ));
    }
    if !(4..=21).contains(&lg_k) {
        return Err(format!(
            "{context}: HLL lg_k must be in [4, 21], got {lg_k}"
        ));
    }
    let target_type = match (mode_byte >> 2) & 3 {
        0 => HllTargetType::Hll4,
        1 => HllTargetType::Hll6,
        2 => HllTargetType::Hll8,
        value => return Err(format!("{context}: invalid HLL target type {value}")),
    };
    let compact = flags & COMPACT_FLAG != 0;
    let empty = flags & EMPTY_FLAG != 0;
    let k = 1usize << lg_k;
    match mode_byte & 3 {
        0 => {
            if preamble_ints != 2 || lg_arr != 3 {
                return Err(format!(
                    "{context}: invalid LIST header preamble={preamble_ints} lg_arr={lg_arr}"
                ));
            }
            let coupon_count = usize::from(state);
            if coupon_count > 8 || empty != (coupon_count == 0) {
                return Err(format!(
                    "{context}: invalid LIST coupon count or empty flag"
                ));
            }
            let read_count = if compact { coupon_count } else { 8 };
            if !empty {
                require_payload_len(payload, 8 + read_count * 4, context)?;
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
                return Err(format!("{context}: invalid SET preamble {preamble_ints}"));
            }
            let max_lg_arr = lg_k.saturating_sub(3);
            if !(5..=max_lg_arr).contains(&lg_arr) {
                return Err(format!(
                    "{context}: SET lg_arr must be in [5, {max_lg_arr}], got {lg_arr}"
                ));
            }
            require_payload_len(payload, 12, context)?;
            let coupon_count = read_u32_le(payload, 8, context)? as usize;
            let capacity = 1usize << lg_arr;
            if coupon_count >= capacity {
                return Err(format!(
                    "{context}: SET coupon count {coupon_count} exceeds capacity {capacity}"
                ));
            }
            let read_count = if compact { coupon_count } else { capacity };
            require_payload_len(payload, 12 + read_count * 4, context)?;
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
                return Err(format!("{context}: invalid HLL preamble {preamble_ints}"));
            }
            require_payload_len(payload, HLL_PREAMBLE_BYTES, context)?;
            let num_at_cur_min = read_u32_le(payload, 32, context)? as usize;
            let aux_count = read_u32_le(payload, 36, context)? as usize;
            if num_at_cur_min > k || aux_count > k {
                return Err(format!(
                    "{context}: HLL register or auxiliary count exceeds k"
                ));
            }
            let (retained_heap_bytes, decode_peak_heap_bytes, body_bytes) = match target_type {
                HllTargetType::Hll4 => {
                    let packed = k / 2;
                    let aux_slots = if compact {
                        aux_count
                    } else if aux_count == 0 {
                        0
                    } else {
                        1usize
                            .checked_shl(u32::from(lg_arr))
                            .ok_or_else(|| format!("{context}: invalid HLL4 lg_arr {lg_arr}"))?
                    };
                    let (aux_retained, aux_peak) = hll4_aux_allocation(lg_k, aux_count);
                    let aux_body = aux_slots.checked_mul(4).ok_or_else(|| {
                        format!("{context}: HLL4 auxiliary payload length overflows")
                    })?;
                    let body_bytes = packed
                        .checked_add(aux_body)
                        .ok_or_else(|| format!("{context}: HLL4 payload length overflows"))?;
                    (packed + aux_retained, packed + aux_peak, body_bytes)
                }
                HllTargetType::Hll6 => {
                    if aux_count != 0 {
                        return Err(format!("{context}: HLL6 auxiliary count must be zero"));
                    }
                    let bytes = 3 * k / 4 + 1;
                    (bytes, bytes, bytes)
                }
                HllTargetType::Hll8 => {
                    if aux_count != 0 {
                        return Err(format!("{context}: HLL8 auxiliary count must be zero"));
                    }
                    (k, k, k)
                }
            };
            require_payload_len(payload, HLL_PREAMBLE_BYTES + body_bytes, context)?;
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
        value => Err(format!("{context}: invalid HLL mode {value}")),
    }
}

fn empty_handle_current_bytes() -> usize {
    std::mem::size_of::<HllHandle>() + LIST_HEAP_BYTES
}

fn simulate_coupon_destination(profile: HandleProfile, additions: usize) -> usize {
    if additions == 0 || profile.mode == AdmissionMode::Hll {
        return profile.heap_bytes;
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
            let new_heap = if capacity == k / 8 { k } else { capacity * 8 };
            peak_live_heap = peak_live_heap.max(heap + new_heap);
            heap = new_heap;
            if capacity == k / 8 {
                break;
            }
            capacity *= 2;
        }
    }
    peak_live_heap
}

fn union_workspace_bytes(handle: HandleProfile, payload: PayloadProfile) -> usize {
    match payload.mode {
        AdmissionMode::List | AdmissionMode::Set => {
            if handle.empty && payload.lg_k == handle.lg_k {
                payload.retained_heap_bytes
            } else {
                simulate_coupon_destination(handle, payload.coupon_count)
                    .saturating_sub(handle.heap_bytes)
            }
        }
        AdmissionMode::Hll => {
            let result_lg_k = payload.lg_k.min(handle.lg_max_k);
            let result_heap = 1usize << result_lg_k;
            match handle.mode {
                _ if handle.empty => result_heap,
                AdmissionMode::Hll if payload.lg_k < handle.lg_k => result_heap + handle.heap_bytes,
                AdmissionMode::Hll => 0,
                AdmissionMode::List | AdmissionMode::Set => result_heap,
                AdmissionMode::ListOrHll if payload.lg_k < handle.lg_k => {
                    // At lg_k=5 the 32-byte heap may be either LIST or HLL8. A lower-precision
                    // dense source makes the HLL alternative allocate the replacement array while
                    // cloning the old gadget; the LIST alternative only allocates the result.
                    result_heap + handle.heap_bytes
                }
                AdmissionMode::ListOrHll => result_heap,
            }
        }
        AdmissionMode::ListOrHll => {
            unreachable!("serialized payload headers always identify one exact HLL mode")
        }
    }
}

fn deserialize_hll(payload: &[u8], context: &str) -> Result<HllSketch, String> {
    HllSketch::deserialize(payload)
        .map_err(|err| format!("{context}: failed to deserialize HLL payload: {err}"))
}

pub fn hll_estimate(payload: &[u8]) -> Result<i64, String> {
    Ok(deserialize_hll(payload, "ds_hll")?.estimate().round() as i64)
}

pub struct HllHandle {
    target_type: HllTargetType,
    sketch_union: HllUnion,
    generation: u64,
}

impl HllHandle {
    pub fn new_allocation_preflight(
        log_k: u8,
        target_type: HllTargetType,
    ) -> Result<HllNewPreflight, String> {
        if !(4..=21).contains(&log_k) {
            return Err(format!("ds_hll log_k must be in [4, 21], got {log_k}"));
        }
        Ok(HllNewPreflight {
            log_k,
            target_type,
            bounds: HllAllocationUpperBounds {
                current_bytes: 0,
                operation_peak_bytes: empty_handle_current_bytes(),
            },
        })
    }

    pub fn new_under_reservation<G>(
        preflight: &HllNewPreflight,
        _reservation: &G,
    ) -> Result<(Self, HllAllocationUpperBounds), String> {
        let expected = Self::new_allocation_preflight(preflight.log_k, preflight.target_type)?;
        if expected != *preflight {
            return Err("ds_hll: new allocation preflight does not match operation".to_string());
        }
        let handle = HllUnion::new(preflight.log_k)
            .map_err(|err| format!("ds_hll: failed to create HLL union: {err}"))
            .map(|sketch_union| Self {
                target_type: preflight.target_type,
                sketch_union,
                generation: 0,
            })?;
        let outcome = HllAllocationUpperBounds {
            current_bytes: handle.current_allocation_upper_bound(),
            operation_peak_bytes: preflight.bounds.operation_peak_bytes,
        };
        Ok((handle, outcome))
    }

    pub fn new_unreserved(log_k: u8, target_type: HllTargetType) -> Result<Self, String> {
        let preflight = Self::new_allocation_preflight(log_k, target_type)?;
        Self::new_under_reservation(&preflight, &()).map(|(handle, _)| handle)
    }

    pub fn from_payload_allocation_preflight(
        payload: &[u8],
    ) -> Result<HllPayloadPreflight, String> {
        let payload = payload_profile(payload, "ds_hll preflight")?;
        let handle = HandleProfile {
            generation: 0,
            lg_k: payload.lg_k,
            lg_max_k: payload.lg_k,
            mode: AdmissionMode::List,
            coupon_count_upper: 0,
            heap_bytes: LIST_HEAP_BYTES,
            current_bytes: empty_handle_current_bytes(),
            empty: true,
        };
        let decoded_peak = std::mem::size_of::<HllSketch>() + payload.decode_peak_heap_bytes;
        let union_peak = handle.current_bytes
            + std::mem::size_of::<HllSketch>()
            + payload.retained_heap_bytes
            + union_workspace_bytes(handle, payload);
        Ok(HllPayloadPreflight {
            handle: None,
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
    ) -> Result<(Self, HllAllocationUpperBounds), String> {
        let expected = Self::from_payload_allocation_preflight(payload)?;
        if expected != *preflight {
            return Err(
                "ds_hll: payload allocation preflight does not match operation".to_string(),
            );
        }
        let sketch = deserialize_hll(payload, "ds_hll")?;
        let target_type = HllTargetType::from_native(sketch.target_type());
        let mut sketch_union = HllUnion::new(sketch.lg_config_k())
            .map_err(|err| format!("ds_hll: failed to create HLL union: {err}"))?;
        sketch_union.update(&sketch);
        let handle = Self {
            target_type,
            sketch_union,
            generation: 0,
        };
        let outcome = HllAllocationUpperBounds {
            current_bytes: handle.current_allocation_upper_bound(),
            operation_peak_bytes: preflight.bounds.operation_peak_bytes,
        };
        Ok((handle, outcome))
    }

    pub fn from_payload_unreserved(payload: &[u8]) -> Result<Self, String> {
        let preflight = Self::from_payload_allocation_preflight(payload)?;
        Self::from_payload_under_reservation(payload, &preflight, &()).map(|(handle, _)| handle)
    }

    pub fn update_hash_allocation_preflight(&self) -> HllUpdatePreflight {
        let handle = self.profile();
        let peak_heap = simulate_coupon_destination(handle, 1);
        HllUpdatePreflight {
            handle,
            bounds: HllAllocationUpperBounds {
                current_bytes: handle.current_bytes,
                operation_peak_bytes: handle.current_bytes + peak_heap - handle.heap_bytes,
            },
        }
    }

    pub fn update_hash_under_reservation<G>(
        &mut self,
        hash: u64,
        preflight: &HllUpdatePreflight,
        _reservation: &G,
    ) -> Result<HllAllocationUpperBounds, String> {
        let expected = self.update_hash_allocation_preflight();
        if expected != *preflight {
            return Err("ds_hll: update allocation preflight is stale or mismatched".to_string());
        }
        let next_generation = self
            .generation
            .checked_add(1)
            .ok_or_else(|| "ds_hll: handle generation overflow".to_string())?;
        self.sketch_union.update_value(hash);
        self.generation = next_generation;
        Ok(HllAllocationUpperBounds {
            current_bytes: self.current_allocation_upper_bound(),
            operation_peak_bytes: preflight.bounds.operation_peak_bytes,
        })
    }

    pub fn update_hash_unreserved(&mut self, hash: u64) -> Result<(), String> {
        let preflight = self.update_hash_allocation_preflight();
        self.update_hash_under_reservation(hash, &preflight, &())
            .map(|_| ())
    }

    pub fn merge_payload_allocation_preflight(
        &self,
        payload: &[u8],
    ) -> Result<HllPayloadPreflight, String> {
        let handle = self.profile();
        let payload = payload_profile(payload, "ds_hll merge preflight")?;
        let decoded_peak = handle.current_bytes
            + std::mem::size_of::<HllSketch>()
            + payload.decode_peak_heap_bytes;
        let union_peak = handle.current_bytes
            + std::mem::size_of::<HllSketch>()
            + payload.retained_heap_bytes
            + union_workspace_bytes(handle, payload);
        Ok(HllPayloadPreflight {
            handle: Some(handle),
            payload,
            bounds: HllAllocationUpperBounds {
                current_bytes: handle.current_bytes,
                operation_peak_bytes: decoded_peak.max(union_peak),
            },
        })
    }

    pub fn merge_payload_under_reservation<G>(
        &mut self,
        payload: &[u8],
        preflight: &HllPayloadPreflight,
        _reservation: &G,
    ) -> Result<HllAllocationUpperBounds, String> {
        let expected = self.merge_payload_allocation_preflight(payload)?;
        if expected != *preflight {
            return Err("ds_hll: merge allocation preflight is stale or mismatched".to_string());
        }
        let next_generation = self
            .generation
            .checked_add(1)
            .ok_or_else(|| "ds_hll: handle generation overflow".to_string())?;
        let sketch = deserialize_hll(payload, "ds_hll")?;
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
    ) -> Result<HllAllocationUpperBounds, String> {
        let preflight = self.merge_payload_allocation_preflight(payload)?;
        self.merge_payload_under_reservation(payload, &preflight, &())
    }

    pub fn serialize(&self) -> Result<Vec<u8>, String> {
        Ok(self
            .sketch_union
            .to_sketch(self.target_type.into_native())
            .serialize())
    }

    pub fn estimate(&self) -> Result<i64, String> {
        Ok(self.sketch_union.estimate().round() as i64)
    }

    /// Returns a conservative upper bound for the live handle's current allocation footprint.
    ///
    /// The bound uses DataSketches' capacity-aware `estimated_size()` and includes this wrapper's
    /// inline bytes. It does not use the serialized payload length or derive retained memory from
    /// `lg_k`.
    pub fn current_allocation_upper_bound(&self) -> usize {
        std::mem::size_of::<HllHandle>() - std::mem::size_of::<HllUnion>()
            + self.sketch_union.estimated_size()
    }

    fn profile(&self) -> HandleProfile {
        let current_bytes = self.current_allocation_upper_bound();
        let heap_bytes = self.sketch_union.estimated_size() - std::mem::size_of::<HllUnion>();
        let lg_k = self.sketch_union.lg_config_k();
        let k = 1usize << lg_k;
        let empty = self.sketch_union.is_empty();
        // In the exact RC1 substrate, LIST/SET Container::estimate() is structurally floored by
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
        HandleProfile {
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
 * allocation reported by the handle before releasing headroom. Callers without such an admission
 * owner use the explicitly named unreserved methods.
 */

#[cfg(test)]
mod tests {
    use base64::Engine;
    use base64::engine::general_purpose::STANDARD;
    use datasketches::hll::{HllSketch, HllType};

    use super::{HllHandle, HllTargetType, hll_estimate};

    #[test]
    fn known_compact_payload_is_still_readable() {
        let payload = STANDARD
            .decode("AgEHEQMIAQQ9nPUc")
            .expect("decode base64 payload");

        assert_eq!(hll_estimate(&payload).expect("estimate"), 1);
        assert_eq!(
            HllHandle::from_payload_unreserved(&payload)
                .expect("handle from payload")
                .estimate()
                .expect("estimate from handle"),
            1
        );
    }

    #[test]
    fn native_hll_roundtrip_merges_without_cpp() {
        let mut left = HllHandle::new_unreserved(10, HllTargetType::Hll6).expect("left handle");
        for value in 0_u64..64 {
            left.update_hash_unreserved(value).expect("update left");
        }
        let left_payload = left.serialize().expect("serialize left");

        let mut right = HllHandle::new_unreserved(10, HllTargetType::Hll6).expect("right handle");
        for value in 64_u64..128 {
            right.update_hash_unreserved(value).expect("update right");
        }
        let right_payload = right.serialize().expect("serialize right");

        let mut merged = HllHandle::from_payload_unreserved(&left_payload).expect("merged handle");
        merged
            .merge_payload_unreserved(&right_payload)
            .expect("merge right");

        let estimate = merged.estimate().expect("estimate merged");
        assert!(
            (110..=150).contains(&estimate),
            "merged estimate out of expected range: {estimate}"
        );
    }

    #[test]
    fn native_hll4_roundtrip_merges_without_cpp() {
        let mut sparse = HllHandle::new_unreserved(10, HllTargetType::Hll4).expect("sparse handle");
        for value in 0_u64..7 {
            sparse.update_hash_unreserved(value).expect("update sparse");
        }
        let sparse_sketch =
            HllSketch::deserialize(&sparse.serialize().expect("serialize sparse HLL4"))
                .expect("deserialize sparse HLL4");

        let mut left = HllHandle::new_unreserved(10, HllTargetType::Hll4).expect("left handle");
        for value in 0_u64..4_096 {
            left.update_hash_unreserved(value).expect("update left");
        }
        let left_payload = left.serialize().expect("serialize left");
        let left_sketch = HllSketch::deserialize(&left_payload).expect("deserialize left");
        assert_eq!(left_sketch.target_type(), HllType::Hll4);
        assert!(left_sketch.estimated_size() > sparse_sketch.estimated_size());

        let mut right = HllHandle::new_unreserved(10, HllTargetType::Hll4).expect("right handle");
        for value in 4_096_u64..8_192 {
            right.update_hash_unreserved(value).expect("update right");
        }
        let right_payload = right.serialize().expect("serialize right");
        let right_sketch = HllSketch::deserialize(&right_payload).expect("deserialize right");
        assert_eq!(right_sketch.target_type(), HllType::Hll4);

        let mut merged = HllHandle::from_payload_unreserved(&left_payload).expect("merged handle");
        merged
            .merge_payload_unreserved(&right_payload)
            .expect("merge right");
        let merged_payload = merged.serialize().expect("serialize merged");
        let merged_sketch = HllSketch::deserialize(&merged_payload).expect("deserialize merged");
        assert_eq!(merged_sketch.target_type(), HllType::Hll4);

        let roundtrip =
            HllHandle::from_payload_unreserved(&merged_payload).expect("roundtrip handle");
        let roundtrip_payload = roundtrip.serialize().expect("serialize roundtrip");
        assert_eq!(
            HllSketch::deserialize(&roundtrip_payload)
                .expect("deserialize roundtrip")
                .target_type(),
            HllType::Hll4
        );

        let estimate = roundtrip.estimate().expect("estimate merged");
        assert!(
            (7_000..=9_500).contains(&estimate),
            "merged estimate out of expected range: {estimate}"
        );
    }
}
