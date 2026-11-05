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

//! The same permanent probes consume both real production tracker adapters.
use crate::runtime::mem_tracker::MemTracker;
use crate::runtime::tracked_state_allocator::TrackedStateAllocator;
use novarocks_functions::opaque_memory::OpaqueRetainedCharge;
use novarocks_functions::{AggregateStateAllocator, KernelFailure};
use std::{alloc::Layout, sync::Arc};
pub(super) fn assert_tracker_refusal(failure: &KernelFailure, limit: i64, requested: Option<u64>) {
    use novarocks_failure_contract::{FailureClass, FailureProjection, domain};
    let KernelFailure::Capacity(source) = failure else {
        panic!("the real tracker must preserve its capacity receipt: {failure:?}");
    };
    let fact = failure.failure_fact();
    assert_eq!(fact, source.failure_fact());
    assert_eq!(fact.class(), FailureClass::Resource);
    assert_eq!(fact.reason().domain(), domain::MEMORY.get());
    assert_eq!(fact.reason().code(), 8);
    assert!(fact.representation().is_none());
    let resource = fact.resource().unwrap();
    assert_eq!(resource.limit, Some(limit as u64));
    assert!(resource.requested.unwrap() > 0);
    if let Some(requested) = requested {
        assert_eq!(resource.requested, Some(requested));
    }
    assert!(resource.committed.unwrap() > limit as u64);
    assert_eq!(resource.request_scope, None);
    assert_eq!(resource.constraint_scope, None);
}
fn host() -> (Arc<MemTracker>, Arc<dyn AggregateStateAllocator>) {
    let tracker = MemTracker::new_root("opaque-real-tracker-test");
    let host: Arc<dyn AggregateStateAllocator> =
        Arc::new(TrackedStateAllocator::new(Arc::clone(&tracker)));
    (tracker, host)
}
#[test]
fn opaque_host_actual_reservation_retained_transfer_and_last_drop() {
    let (tracker, host) = host();
    let mut charge = OpaqueRetainedCharge::try_new(host).unwrap();
    let mut lease = charge.reserve_operation(128).unwrap();
    assert_eq!(tracker.current(), 128);
    charge.reconcile_under_reservation(48, &mut lease).unwrap();
    assert_eq!(charge.bytes(), 48);
    assert_eq!(lease.remaining_bytes(), 80);
    drop(lease);
    assert_eq!(tracker.current(), 48);
    let mut lease = charge.reserve_operation(32).unwrap();
    charge.reconcile_under_reservation(16, &mut lease).unwrap();
    assert_eq!(tracker.current(), 48);
    drop(lease);
    assert_eq!(tracker.current(), 16);
    drop(charge);
    assert_eq!(tracker.current(), 0);
}
#[test]
fn opaque_host_actual_refusal_restores_charge_without_dummy_allocation() {
    let (tracker, host) = host();
    tracker.install_limit_once(32).unwrap();
    let charge = OpaqueRetainedCharge::try_new(host).unwrap();
    let error = charge
        .reserve_operation(33)
        .err()
        .expect("actual host limit refusal");
    assert_tracker_refusal(&error, 32, Some(33));
    assert_eq!(tracker.current(), 0);
    drop(charge);
    assert_eq!(tracker.current(), 0);
}

#[test]
fn typed_and_opaque_host_refusals_roll_back_all_ancestors_but_keep_peak() {
    let root = MemTracker::new_root("host root".repeat(4096));
    root.install_limit_once(31).unwrap();
    let middle = MemTracker::new_child("middle", &root);
    let leaf = MemTracker::new_child("leaf", &middle);
    let host: Arc<dyn AggregateStateAllocator> =
        Arc::new(TrackedStateAllocator::new(Arc::clone(&leaf)));
    let layout = Layout::from_size_align(32, 8).unwrap();
    assert_tracker_refusal(&host.allocate(layout).unwrap_err(), 31, Some(32));
    let charge = OpaqueRetainedCharge::try_new(host).unwrap();
    let refusal = charge.reserve_operation(32).err().unwrap();
    assert_tracker_refusal(&refusal, 31, Some(32));
    for tracker in [&leaf, &middle, &root] {
        assert_eq!(tracker.current(), 0);
        assert_eq!(tracker.allocated(), 64);
        assert_eq!(tracker.deallocated(), 64);
        assert_eq!(tracker.peak(), 32);
    }
    assert_eq!(charge.reserve_operation(0).unwrap().remaining_bytes(), 0);
    drop(charge);
    assert_eq!(root.current(), 0);
}
#[test]
fn opaque_host_actual_underestimate_does_not_publish_retained_growth() {
    let (tracker, host) = host();
    let mut charge = OpaqueRetainedCharge::try_new(host).unwrap();
    let mut lease = charge.reserve_operation(16).unwrap();
    let error = charge
        .reconcile_under_reservation(17, &mut lease)
        .unwrap_err();
    use novarocks_failure_contract::{FailureClass, FailureProjection};
    assert_eq!(error.failure_fact().class(), FailureClass::Invariant);
    assert_eq!(error.failure_fact().reason().code(), 167);
    let KernelFailure::Source(cause) = error else {
        panic!("expected exact preflight violation")
    };
    let source = std::error::Error::source(&cause)
        .unwrap()
        .downcast_ref::<novarocks_functions::opaque_memory::OpaqueRetainedUnderestimate>()
        .unwrap();
    assert_eq!((source.reserved, source.growth), (16, 17));
    assert_eq!(charge.bytes(), 0);
    assert_eq!(lease.remaining_bytes(), 16);
    drop(lease);
    drop(charge);
    assert_eq!(tracker.current(), 0);
}
#[test]
fn opaque_host_actual_block_and_opaque_backing_are_separate_real_obligations() {
    let (tracker, host) = host();
    let layout = Layout::from_size_align(32, 8).unwrap();
    let block = host.allocate(layout).unwrap();
    assert_eq!(tracker.current(), 32);
    let mut charge = OpaqueRetainedCharge::try_new(Arc::clone(&host)).unwrap();
    let mut lease = charge.reserve_operation(100).unwrap();
    assert_eq!(tracker.current(), 132);
    charge.reconcile_under_reservation(80, &mut lease).unwrap();
    drop(lease);
    assert_eq!(tracker.current(), 112);
    drop(charge);
    assert_eq!(tracker.current(), 32);
    // SAFETY: this actual original host allocation remains live with its exact Layout.
    unsafe { host.release(block, layout) };
    assert_eq!(tracker.current(), 0);
}

#[test]
fn opaque_host_actual_pinned_library_new_update_conversion_and_payload_overlap() {
    use novarocks_functions::datasketches_hll::{HllAccumulator, HllTargetType};
    for target in [
        HllTargetType::Hll4,
        HllTargetType::Hll6,
        HllTargetType::Hll8,
    ] {
        let (tracker, host) = host();
        let mut charge = OpaqueRetainedCharge::try_new(Arc::clone(&host)).unwrap();
        let preflight = HllAccumulator::new_allocation_preflight(8, target).unwrap();
        let mut lease = charge
            .reserve_operation(preflight.bounds().operation_peak_bytes)
            .unwrap();
        let (mut accumulator, outcome) =
            HllAccumulator::new_under_reservation(&preflight, &lease).unwrap();
        charge
            .reconcile_under_reservation(
                outcome
                    .current_bytes
                    .saturating_sub(std::mem::size_of::<HllAccumulator>()),
                &mut lease,
            )
            .unwrap();
        drop(lease);
        assert_eq!(tracker.current(), charge.bytes() as i64);
        for hash in 0u64..2048 {
            let preflight = accumulator
                .update_hash_allocation_preflight()
                .expect("valid HLL update preflight");
            let mut lease = charge
                .reserve_operation(preflight.bounds().additional_headroom_bytes())
                .unwrap();
            let outcome = accumulator
                .update_hash_under_reservation(
                    hash.wrapping_mul(0x9e3779b97f4a7c15),
                    &preflight,
                    &lease,
                )
                .unwrap();
            charge
                .reconcile_under_reservation(
                    outcome
                        .current_bytes
                        .saturating_sub(std::mem::size_of::<HllAccumulator>()),
                    &mut lease,
                )
                .unwrap();
            drop(lease);
            assert_eq!(tracker.current(), charge.bytes() as i64);
        }
        let before = tracker.current();
        let preflight = accumulator
            .serialization_allocation_preflight_observed(&mut || Ok::<_, KernelFailure>(()))
            .unwrap();
        let mut payload_charge = OpaqueRetainedCharge::try_new(Arc::clone(&host)).unwrap();
        let mut lease = payload_charge
            .reserve_operation(preflight.bounds().additional_headroom_bytes)
            .unwrap();
        let payload = accumulator
            .serialize_under_reservation(&preflight, &lease)
            .unwrap();
        payload_charge
            .reconcile_under_reservation(payload.capacity(), &mut lease)
            .unwrap();
        drop(lease);
        assert_eq!(tracker.current(), before + payload.capacity() as i64);
        // The retained payload is still live at the append/copy frontier.
        assert_eq!(
            novarocks_functions::datasketches_hll::hll_estimate(&payload).unwrap(),
            accumulator.estimate().unwrap()
        );
        drop(payload);
        drop(payload_charge);
        assert_eq!(tracker.current(), before);
        drop(accumulator);
        drop(charge);
        assert_eq!(tracker.current(), 0);
    }
}
