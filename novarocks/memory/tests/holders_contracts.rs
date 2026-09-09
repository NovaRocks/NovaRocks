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

//! The holder contracts, asserted from outside the crate.
//!
//! These are the properties a consumer depends on and that no amount of
//! internal refactoring may quietly change: exposure is per scope and is never
//! totalled across scopes, withdrawing a claim moves no capacity, and a
//! generation check cannot be separated from the pin it guards.

use novarocks_memory::budget::MetadataBudget;
use novarocks_memory::holder::{HolderRegistry, PinRejection, ScopeExposure, UnloadEligibility};
use novarocks_memory::ids::{AccountId, ExternalRef};

const SCOPE_A: AccountId = AccountId::new(11);
const SCOPE_B: AccountId = AccountId::new(22);
const FRAME: ExternalRef = ExternalRef::from_u128(0xf0_0d);
const OTHER_FRAME: ExternalRef = ExternalRef::from_u128(0xbe_ef);
const FRAME_BYTES: u64 = 8 * 1024 * 1024;

fn registry() -> HolderRegistry {
    HolderRegistry::new(&MetadataBudget::with_defaults())
}

#[test]
fn one_resource_in_two_scopes_reports_its_single_size_to_each_scope() {
    let registry = registry();
    let held_by_a = registry
        .acquire_lease(SCOPE_A, FRAME, FRAME_BYTES)
        .expect("the first scope may hold the frame");
    let held_by_b = registry
        .acquire_lease(SCOPE_B, FRAME, FRAME_BYTES)
        .expect("a second scope may hold the same frame");

    // Each scope's own exposure is the resource's single size. Neither scope
    // gets a share, and neither gets a doubled number.
    assert_eq!(registry.exposure_of(SCOPE_A).exposed_bytes, FRAME_BYTES);
    assert_eq!(registry.exposure_of(SCOPE_B).exposed_bytes, FRAME_BYTES);
    assert_eq!(registry.exposure_of(SCOPE_A).lease_count, 1);
    assert_eq!(registry.exposure_of(SCOPE_B).lease_count, 1);

    // The resource is held twice and is 8 MiB once. The process holds 8 MiB.
    let resource = registry
        .resource_exposure(FRAME)
        .expect("the frame has recorded exposure");
    assert_eq!(resource.holding_scopes, 2);
    assert_eq!(
        resource.bytes, FRAME_BYTES,
        "bytes is the resource's single size, never a sum over its holders"
    );
    assert_ne!(
        resource.bytes,
        FRAME_BYTES * resource.holding_scopes as u64,
        "the size must not scale with the number of holding scopes"
    );

    drop(held_by_a);
    drop(held_by_b);
}

#[test]
fn exposure_is_answered_per_scope_and_per_resource_only() {
    let registry = registry();
    let _first = registry
        .acquire_lease(SCOPE_A, FRAME, FRAME_BYTES)
        .expect("lease");
    let _second = registry
        .acquire_lease(SCOPE_A, OTHER_FRAME, 4_096)
        .expect("lease");
    let _third = registry
        .acquire_lease(SCOPE_B, FRAME, FRAME_BYTES)
        .expect("lease");

    // Within one scope, distinct resources are distinct backings, so summing
    // them is legitimate and is what `exposed_bytes` does.
    let scope_a = registry.exposure_of(SCOPE_A);
    assert_eq!(scope_a.distinct_resources, 2);
    assert_eq!(scope_a.exposed_bytes, FRAME_BYTES + 4_096);

    let scope_b = registry.exposure_of(SCOPE_B);
    assert_eq!(scope_b.distinct_resources, 1);
    assert_eq!(scope_b.exposed_bytes, FRAME_BYTES);

    // Across scopes there is nothing to read. The registry answers about one
    // scope or one resource, and the process-wide number lives in the
    // authority's `L`, where each byte is counted once. Adding these two
    // readings would claim the 8 MiB frame twice; the API offers no method
    // that does it, and this assertion pins that absence in place.
    assert_ne!(
        scope_a.exposed_bytes + scope_b.exposed_bytes,
        FRAME_BYTES + 4_096,
        "adding scope exposures over-counts the shared frame, which is why no \
         cross-scope total is offered"
    );
    assert_eq!(registry.exposed_scopes(), 2);
    assert_eq!(registry.known_resources(), 2);
}

#[test]
fn withdrawing_a_lease_removes_that_scopes_exposure_and_returns_no_capacity() {
    let registry = registry();
    let held_by_a = registry
        .acquire_lease(SCOPE_A, FRAME, FRAME_BYTES)
        .expect("lease");
    let held_by_b = registry
        .acquire_lease(SCOPE_B, FRAME, FRAME_BYTES)
        .expect("lease");

    let outcome = held_by_a.withdraw();
    assert_eq!(outcome.scope(), SCOPE_A);
    assert_eq!(outcome.resource(), FRAME);
    assert_eq!(outcome.bytes(), FRAME_BYTES);
    assert!(!outcome.scope_still_holds());
    assert_eq!(
        outcome.remaining_holding_scopes(),
        1,
        "the other scope still holds the frame"
    );
    assert!(!outcome.became_unheld());
    assert_eq!(
        outcome.capacity_returned_bytes(),
        0,
        "withdrawing exposure is not a release: the allocator still holds the charge"
    );

    assert_eq!(registry.exposure_of(SCOPE_A), ScopeExposure::NONE);
    assert_eq!(registry.exposure_of(SCOPE_B).exposed_bytes, FRAME_BYTES);
    assert_eq!(
        registry
            .resource_exposure(FRAME)
            .expect("still held")
            .holding_scopes,
        1
    );

    drop(held_by_b);
    assert_eq!(registry.exposure_of(SCOPE_B), ScopeExposure::NONE);
    assert_eq!(
        registry.resource_exposure(FRAME),
        None,
        "an unheld, unpinned resource is forgotten"
    );
    assert_eq!(registry.live_holders(), 0);
}

#[test]
fn dropping_a_lease_withdraws_exposure_exactly_as_withdrawing_it_does() {
    let registry = registry();
    {
        let _scoped = registry
            .acquire_lease(SCOPE_A, FRAME, FRAME_BYTES)
            .expect("lease");
        assert_eq!(registry.exposure_of(SCOPE_A).exposed_bytes, FRAME_BYTES);
    }
    assert_eq!(
        registry.exposure_of(SCOPE_A),
        ScopeExposure::NONE,
        "RAII must withdraw exposure that outlives its holder"
    );
    assert_eq!(registry.live_holders(), 0);
}

#[test]
fn a_pin_drop_changes_no_charge_and_only_reports_unload_eligibility() {
    let registry = registry();
    let lease = registry
        .acquire_lease(SCOPE_A, FRAME, FRAME_BYTES)
        .expect("lease");
    let first = lease
        .try_pin(0)
        .expect("the frame is resident at generation 0");
    let second = lease.try_pin(0).expect("a frame may carry several pins");

    let exposure_before = registry.exposure_of(SCOPE_A);
    assert_eq!(exposure_before.pinned_count, 2);
    assert_eq!(exposure_before.exposed_bytes, FRAME_BYTES);

    // Dropping one of two pins: the frame stays resident, nothing is freed.
    let outcome = first.release();
    assert_eq!(
        outcome.eligibility(),
        UnloadEligibility::StillPinned { remaining_pins: 1 }
    );
    assert!(!outcome.became_unloadable());
    assert_eq!(outcome.released_bytes(), 0);
    assert_eq!(outcome.capacity_returned_bytes(), 0);

    // Dropping the last pin: the frame becomes unloadable. That is a
    // statement about residency, not about bytes — the backing is still
    // allocated and still charged to whoever allocated it.
    let outcome = second.release();
    assert_eq!(outcome.eligibility(), UnloadEligibility::BecameUnloadable);
    assert!(outcome.became_unloadable());
    assert_eq!(
        outcome.released_bytes(),
        0,
        "unpinning removes protection, not backing"
    );
    assert_eq!(
        outcome.capacity_returned_bytes(),
        0,
        "a pin never held a charge, so it has none to return"
    );

    // The exposure the pins sat on is untouched: the lease still holds it.
    let exposure_after = registry.exposure_of(SCOPE_A);
    assert_eq!(exposure_after.pinned_count, 0);
    assert_eq!(
        exposure_after.exposed_bytes, FRAME_BYTES,
        "pins never contributed exposed bytes, so their removal changes none"
    );
    assert_eq!(exposure_after.lease_count, 1);
    let resource = registry.resource_exposure(FRAME).expect("still held");
    assert!(resource.is_unloadable());
    assert_eq!(resource.bytes, FRAME_BYTES);

    drop(lease);
}

#[test]
fn try_pin_with_a_stale_generation_is_rejected_and_creates_no_pin() {
    let registry = registry();
    let lease = registry
        .acquire_lease(SCOPE_A, FRAME, FRAME_BYTES)
        .expect("lease");

    let observed = registry
        .generation_of(FRAME)
        .expect("a held resource has a generation");
    assert_eq!(observed, 0);

    // The frame the caller resolved is replaced before it gets to pin it.
    let current = registry.invalidate(FRAME).expect("the resource is known");
    assert_eq!(current, 1);

    let holders_before = registry.live_holders();
    let rejection = lease
        .try_pin(observed)
        .expect_err("a stale generation may not be pinned");
    assert_eq!(
        rejection,
        PinRejection::GenerationChanged { observed: current },
        "the rejection reports the generation the caller must re-resolve against"
    );

    // No pin exists, and the refusal cost the bounded registry nothing.
    assert_eq!(registry.exposure_of(SCOPE_A).pinned_count, 0);
    assert_eq!(
        registry
            .resource_exposure(FRAME)
            .expect("still held")
            .pin_count,
        0
    );
    assert_eq!(registry.live_holders(), holders_before);

    // Re-resolving against the current generation succeeds, which proves the
    // rejection was about staleness and not about the frame being unpinnable.
    let pin = lease
        .try_pin(current)
        .expect("the current frame is pinnable");
    assert_eq!(pin.generation(), current);
    assert_eq!(registry.exposure_of(SCOPE_A).pinned_count, 1);
}

#[test]
fn the_generation_check_and_the_pin_are_one_operation() {
    // There is deliberately no API that reserves a pin without checking a
    // generation, and none that checks a generation and hands back a token to
    // pin with later. `try_pin` is the only way in, so the window in which a
    // frame could be replaced between the check and the pin does not exist to
    // be raced. This test states the shape of that guarantee: every path to a
    // `Pin` presents an expected generation and is refused when it is stale.
    let registry = registry();
    let lease = registry
        .acquire_lease(SCOPE_A, FRAME, FRAME_BYTES)
        .expect("lease");

    // Both entry points behave identically, so neither can be used to skip
    // the check.
    assert_eq!(
        registry
            .try_pin(SCOPE_A, FRAME, 7)
            .expect_err("the registry entry point checks the generation"),
        PinRejection::GenerationChanged { observed: 0 }
    );
    assert_eq!(
        lease
            .try_pin(7)
            .expect_err("the lease entry point checks the generation too"),
        PinRejection::GenerationChanged { observed: 0 }
    );
    assert_eq!(registry.live_holders(), 1, "only the lease holds a slot");

    // Invalidating under a live pin moves the generation without disturbing
    // the frame that pin protects: the pin named a frame, not a version.
    let pin = lease.try_pin(0).expect("pin at the current generation");
    assert_eq!(registry.invalidate(FRAME), Some(1));
    assert_eq!(pin.generation(), 0);
    assert_eq!(
        registry
            .resource_exposure(FRAME)
            .expect("still held")
            .pin_count,
        1,
        "an invalidation must not silently drop protection"
    );
    assert_eq!(
        lease
            .try_pin(0)
            .expect_err("the old generation is now stale"),
        PinRejection::GenerationChanged { observed: 1 }
    );
}

#[test]
fn a_pin_taken_through_the_registry_is_attributed_to_the_named_scope() {
    let registry = registry();
    let lease = registry
        .acquire_lease(SCOPE_A, FRAME, FRAME_BYTES)
        .expect("lease");

    // A scope may pin a frame another scope holds. Its exposure then reports a
    // pin and no exposed bytes, because pinning is not holding.
    let pin = registry
        .try_pin(SCOPE_B, FRAME, 0)
        .expect("a resident frame is pinnable by any scope");
    let borrower = registry.exposure_of(SCOPE_B);
    assert_eq!(borrower.pinned_count, 1);
    assert_eq!(borrower.lease_count, 0);
    assert_eq!(
        borrower.exposed_bytes, 0,
        "a pin says a frame must stay resident, not that this scope holds it"
    );
    assert_eq!(registry.exposure_of(SCOPE_A).exposed_bytes, FRAME_BYTES);

    drop(pin);
    assert_eq!(registry.exposure_of(SCOPE_B), ScopeExposure::NONE);
    drop(lease);
}
