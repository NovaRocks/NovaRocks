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

//! The original aggregate retained-state operation sequence over an explicit host.
use crate::builtin::aggregate_ds_hll_failure::{
    DsHllFailureSink, DsHllInputContext, DsHllInputDetail, DsHllInputRecipe,
};
use crate::datasketches_hll::{HllAccumulator, HllTargetType};
use crate::datasketches_hll_failure::HllObservation;
#[derive(Clone, Copy, Debug)]
pub enum DsHllRetainedOperation {
    Create,
    Payload,
    Update,
    Merge,
}
impl DsHllRetainedOperation {
    pub const fn label(self) -> &'static str {
        match self {
            Self::Create => "reserve ds_hll accumulator creation",
            Self::Payload => "reserve ds_hll payload initialization",
            Self::Update => "reserve ds_hll update",
            Self::Merge => "reserve ds_hll merge",
        }
    }
}
/// A lease comes from the actual host. Its original implementation owns
/// admission, rollback and the transfer to retained bytes.
pub trait DsHllRetainedPort<F: DsHllFailureSink> {
    fn payload_error_headroom(
        &self,
        preflight: crate::datasketches_hll::HllPayloadPreflight,
    ) -> usize;
    type Reservation;
    fn reserve(
        &self,
        bytes: usize,
        operation: DsHllRetainedOperation,
        sink: &mut F,
    ) -> Result<Self::Reservation, F::Error>;
    fn reconcile(
        &mut self,
        bytes: usize,
        reservation: &mut Self::Reservation,
        sink: &mut F,
    ) -> Result<(), F::Error>;
}
pub fn hll_heap_bytes(current: usize) -> usize {
    current.saturating_sub(std::mem::size_of::<HllAccumulator>())
}
pub fn ensure_accumulator<'a, F: DsHllFailureSink, R: DsHllRetainedPort<F>>(
    accumulator: &'a mut Option<HllAccumulator>,
    charge: &mut R,
    log_k: u8,
    target: HllTargetType,
    sink: &mut F,
) -> Result<&'a mut HllAccumulator, F::Error> {
    if accumulator.is_none() {
        let preflight = HllAccumulator::new_allocation_preflight_with_failure(log_k, target, sink)?;
        let mut reservation = charge.reserve(
            preflight.bounds().operation_peak_bytes,
            DsHllRetainedOperation::Create,
            sink,
        )?;
        let (new_accumulator, outcome) =
            HllAccumulator::new_under_reservation_with_failure(&preflight, &reservation, sink)?;
        charge.reconcile(
            hll_heap_bytes(outcome.current_bytes),
            &mut reservation,
            sink,
        )?;
        *accumulator = Some(new_accumulator);
        // Retained transfer precedes the first post-mutation control observation.
        sink.observe(HllObservation::OpaqueBoundary)?;
    }
    Ok(accumulator
        .as_mut()
        .expect("ds_hll accumulator initialized"))
}
pub fn ensure_accumulator_from_payload<'a, F: DsHllFailureSink, R: DsHllRetainedPort<F>>(
    accumulator: &'a mut Option<HllAccumulator>,
    charge: &mut R,
    payload: &[u8],
    sink: &mut F,
) -> Result<&'a mut HllAccumulator, F::Error> {
    if accumulator.is_none() {
        let preflight =
            HllAccumulator::from_payload_allocation_preflight_with_failure(payload, sink)?;
        let mut reservation = charge.reserve(
            preflight.bounds().operation_peak_bytes + charge.payload_error_headroom(preflight),
            DsHllRetainedOperation::Payload,
            sink,
        )?;
        let (new_accumulator, outcome) =
            HllAccumulator::from_payload_under_reservation_with_failure(
                payload,
                &preflight,
                &reservation,
                sink,
            )?;
        charge.reconcile(
            hll_heap_bytes(outcome.current_bytes),
            &mut reservation,
            sink,
        )?;
        *accumulator = Some(new_accumulator);
        sink.observe(HllObservation::OpaqueBoundary)?;
    }
    Ok(accumulator
        .as_mut()
        .expect("ds_hll accumulator initialized"))
}
pub fn update_hash<F: DsHllFailureSink, R: DsHllRetainedPort<F>>(
    accumulator: &mut Option<HllAccumulator>,
    charge: &mut R,
    hash: u64,
    sink: &mut F,
) -> Result<(), F::Error> {
    let accumulator = accumulator.as_mut().ok_or_else(|| {
        sink.input(DsHllInputRecipe {
            context: DsHllInputContext::CountDistinct,
            detail: DsHllInputDetail::Uninitialized,
        })
    })?;
    let preflight = accumulator.update_hash_allocation_preflight_with_failure(sink)?;
    let mut reservation = charge.reserve(
        preflight.bounds().additional_headroom_bytes(),
        DsHllRetainedOperation::Update,
        sink,
    )?;
    let outcome = accumulator.update_hash_under_reservation_with_failure(
        hash,
        &preflight,
        &reservation,
        sink,
    )?;
    charge.reconcile(
        hll_heap_bytes(outcome.current_bytes),
        &mut reservation,
        sink,
    )?;
    sink.observe(HllObservation::OpaqueBoundary)?;
    Ok(())
}
pub fn merge_payload<F: DsHllFailureSink, R: DsHllRetainedPort<F>>(
    accumulator: &mut Option<HllAccumulator>,
    charge: &mut R,
    payload: &[u8],
    sink: &mut F,
) -> Result<(), F::Error> {
    if accumulator.is_none() {
        ensure_accumulator_from_payload(accumulator, charge, payload, sink)?;
        return Ok(());
    }
    let accumulator = accumulator
        .as_mut()
        .expect("ds_hll accumulator initialized");
    let preflight = accumulator.merge_payload_allocation_preflight_with_failure(payload, sink)?;
    let mut reservation = charge.reserve(
        preflight.bounds().additional_headroom_bytes() + charge.payload_error_headroom(preflight),
        DsHllRetainedOperation::Merge,
        sink,
    )?;
    let outcome = accumulator.merge_payload_under_reservation_with_failure(
        payload,
        &preflight,
        &reservation,
        sink,
    )?;
    charge.reconcile(
        hll_heap_bytes(outcome.current_bytes),
        &mut reservation,
        sink,
    )?;
    sink.observe(HllObservation::OpaqueBoundary)?;
    Ok(())
}
