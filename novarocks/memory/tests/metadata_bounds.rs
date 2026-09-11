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

//! The bounded-metadata contracts, asserted from outside the crate.
//!
//! Two properties matter here and both are about what the core refuses to do.
//! A full registry refuses the *new* entry and leaves every existing one
//! intact, because an existing entry stands for memory that is still alive and
//! forgetting it would not free a byte. And reclaim work that did not vouch for
//! its end state contributes nothing grantable, however many bytes it reports.

use std::sync::Arc;

use novarocks_memory::budget::{BoundedSlots, MetadataBudget};
use novarocks_memory::error::{CapacityError, ConfigError, MetadataRegistryLabel};
use novarocks_memory::holder::{HolderRegistry, PinRejection};
use novarocks_memory::ids::{AccountId, ExternalRef, ReclaimTicketId};
use novarocks_memory::reclaim::{
    ReclaimEstimate, ReclaimOutcome, ReclaimRegistry, ReclaimRequestError, Reclaimer,
};

const EVERY_REGISTRY: [MetadataRegistryLabel; 4] = [
    MetadataRegistryLabel::Accounts,
    MetadataRegistryLabel::Holders,
    MetadataRegistryLabel::Reclaimers,
    MetadataRegistryLabel::Events,
];

const SCOPE: AccountId = AccountId::new(1);

/// A reclaimer that answers from a fixed estimate, so these tests assert on
/// the registry's bounds rather than on a subsystem's behaviour.
struct StubReclaimer {
    ticket: ReclaimTicketId,
}

impl StubReclaimer {
    fn new(ticket: u64) -> Arc<Self> {
        Arc::new(Self {
            ticket: ReclaimTicketId::new(ticket),
        })
    }
}

impl Reclaimer for StubReclaimer {
    fn estimate(&self) -> ReclaimEstimate {
        ReclaimEstimate::measured(4_096)
    }

    fn request(&self, _target_bytes: u64) -> ReclaimTicketId {
        self.ticket
    }
}

#[test]
fn every_registry_refuses_at_its_limit_and_names_itself_and_the_limit() {
    for registry in EVERY_REGISTRY {
        let slots = BoundedSlots::new(registry, 3);
        for _ in 0..3 {
            assert_eq!(
                slots.try_acquire(),
                Ok(()),
                "{registry} must admit entries below its limit"
            );
        }
        assert_eq!(
            slots.try_acquire().unwrap_err(),
            CapacityError::MetadataExhausted { registry, limit: 3 },
            "{registry} must refuse with its own label and limit"
        );
        assert_eq!(
            slots.in_use(),
            3,
            "{registry} must not displace an existing entry to admit a new one"
        );
    }
}

#[test]
fn a_metadata_refusal_is_never_resolved_by_waiting() {
    let refusal = CapacityError::MetadataExhausted {
        registry: MetadataRegistryLabel::Holders,
        limit: 1,
    };
    assert!(
        !refusal.may_resolve_by_waiting(),
        "an exhausted registry is not a capacity shortage an arbitrator can queue for"
    );
    assert_eq!(refusal.scope(), None);
}

#[test]
fn releasing_frees_exactly_one_slot_and_repeated_release_does_not_underflow() {
    let slots = BoundedSlots::new(MetadataRegistryLabel::Holders, 2);
    assert!(slots.try_acquire().is_ok());
    assert!(slots.try_acquire().is_ok());
    assert!(slots.try_acquire().is_err());

    slots.release();
    assert_eq!(slots.in_use(), 1, "one release frees exactly one slot");
    assert_eq!(slots.remaining(), 1);
    assert!(slots.try_acquire().is_ok(), "the freed slot is reusable");
    assert!(slots.try_acquire().is_err());

    slots.release();
    slots.release();
    assert_eq!(slots.in_use(), 0);
    // A double release must degrade into a no-op rather than wrapping: these
    // counters are driven by `Drop`, and a panic here would fire while another
    // value is unwinding.
    for _ in 0..8 {
        slots.release();
    }
    assert_eq!(slots.in_use(), 0, "release must not wrap below zero");
    assert_eq!(slots.remaining(), 2);
    assert!(slots.try_acquire().is_ok());
    assert!(slots.try_acquire().is_ok());
    assert!(
        slots.try_acquire().is_err(),
        "underflow must not have inflated the available slots"
    );
}

#[test]
fn budget_validation_rejects_a_zero_limit_for_each_registry_by_name() {
    assert_eq!(MetadataBudget::with_defaults().validate(), Ok(()));

    for registry in EVERY_REGISTRY {
        let budget = match registry {
            MetadataRegistryLabel::Accounts => MetadataBudget::with_defaults().with_accounts(0),
            MetadataRegistryLabel::Holders => MetadataBudget::with_defaults().with_holders(0),
            MetadataRegistryLabel::Reclaimers => MetadataBudget::with_defaults().with_reclaimers(0),
            MetadataRegistryLabel::Events => MetadataBudget::with_defaults().with_events(0),
        };
        assert_eq!(budget.limit_for(registry), 0);
        assert_eq!(
            budget.validate().unwrap_err(),
            ConfigError::MetadataLimitIsZero { registry },
            "a zero {registry} limit must be refused at configuration time"
        );
    }
}

#[test]
fn the_holder_registry_refuses_a_new_lease_at_its_limit() {
    let registry = HolderRegistry::new(&MetadataBudget::with_defaults().with_holders(2));
    assert_eq!(registry.holder_limit(), 2);

    let first = registry
        .acquire_lease(SCOPE, ExternalRef::from_u128(1), 512)
        .expect("below the limit");
    let second = registry
        .acquire_lease(SCOPE, ExternalRef::from_u128(2), 1_024)
        .expect("at the limit");

    assert_eq!(
        registry
            .acquire_lease(SCOPE, ExternalRef::from_u128(3), 2_048)
            .expect_err("the registry is full"),
        CapacityError::MetadataExhausted {
            registry: MetadataRegistryLabel::Holders,
            limit: 2,
        }
    );

    // The refusal changed nothing: both existing leases still report their
    // exposure, and the resource that was refused was never recorded.
    let exposure = registry.exposure_of(SCOPE);
    assert_eq!(exposure.lease_count, 2);
    assert_eq!(exposure.exposed_bytes, 512 + 1_024);
    assert_eq!(registry.known_resources(), 2);
    assert_eq!(registry.resource_exposure(ExternalRef::from_u128(3)), None);
    assert_eq!(first.bytes(), 512);
    assert_eq!(second.bytes(), 1_024);

    // Dropping one lease frees exactly one slot.
    drop(second);
    assert_eq!(registry.live_holders(), 1);
    let replacement = registry
        .acquire_lease(SCOPE, ExternalRef::from_u128(3), 2_048)
        .expect("the freed slot admits a new lease");
    assert_eq!(replacement.bytes(), 2_048);
    assert_eq!(registry.live_holders(), 2);
}

#[test]
fn pins_and_leases_share_one_holder_budget_and_one_exhaustion_story() {
    let registry = HolderRegistry::new(&MetadataBudget::with_defaults().with_holders(2));
    let lease = registry
        .acquire_lease(SCOPE, ExternalRef::from_u128(7), 64)
        .expect("lease");
    let pin = lease.try_pin(0).expect("the second slot goes to a pin");
    assert_eq!(registry.live_holders(), 2);

    let rejection = lease
        .try_pin(0)
        .expect_err("leases and pins draw on the same budget");
    assert_eq!(
        rejection.as_capacity_error(),
        Some(&CapacityError::MetadataExhausted {
            registry: MetadataRegistryLabel::Holders,
            limit: 2,
        }),
        "a pin refusal carries the same typed metadata error a lease refusal does"
    );
    assert!(matches!(rejection, PinRejection::Exhausted { .. }));

    // The existing pin is untouched by the refusal.
    assert_eq!(registry.exposure_of(SCOPE).pinned_count, 1);
    drop(pin);
    assert_eq!(registry.live_holders(), 1);
    assert!(
        lease.try_pin(0).is_ok(),
        "releasing a pin frees exactly one slot"
    );
}

#[test]
fn the_reclaim_registry_refuses_registration_at_its_limit() {
    let registry = ReclaimRegistry::new(&MetadataBudget::with_defaults().with_reclaimers(2));
    assert_eq!(registry.registration_limit(), 2);

    let first = registry.register(StubReclaimer::new(1)).expect("first");
    let second = registry.register(StubReclaimer::new(2)).expect("second");
    assert_eq!(
        registry
            .register(StubReclaimer::new(3))
            .expect_err("the registry is full"),
        CapacityError::MetadataExhausted {
            registry: MetadataRegistryLabel::Reclaimers,
            limit: 2,
        }
    );

    // Both existing registrations are unaffected and still answer.
    let estimates = registry.estimates();
    assert_eq!(estimates.len(), 2);
    assert_eq!(estimates[0].reclaimer, first);
    assert_eq!(estimates[1].reclaimer, second);
    assert_eq!(registry.registered_count(), 2);

    // Deregistering frees exactly one slot, and doing it twice does not free
    // a second one.
    assert!(registry.deregister(second));
    assert!(!registry.deregister(second));
    assert_eq!(registry.registered_count(), 1);
    assert!(
        registry.register(StubReclaimer::new(4)).is_ok(),
        "exactly one slot came back"
    );
    assert_eq!(
        registry
            .register(StubReclaimer::new(5))
            .expect_err("and only one"),
        CapacityError::MetadataExhausted {
            registry: MetadataRegistryLabel::Reclaimers,
            limit: 2,
        }
    );
}

#[test]
fn in_flight_reclaim_requests_are_bounded_too() {
    let registry = ReclaimRegistry::new(&MetadataBudget::with_defaults().with_reclaimers(1));
    let reclaimer = registry
        .register(StubReclaimer::new(9))
        .expect("registered");
    let ticket = registry.request(reclaimer, 4_096).expect("first request");

    let refused = registry
        .request(reclaimer, 4_096)
        .expect_err("the in-flight table is bounded");
    assert!(matches!(refused, ReclaimRequestError::Exhausted { .. }));
    assert_eq!(
        registry.totals().requests_in_flight,
        1,
        "the refusal must not disturb the request already in flight"
    );

    assert!(registry.record_outcome(&ReclaimOutcome::completed(ticket, 4_096, 4_096, 0)));
    assert_eq!(registry.totals().requests_in_flight, 0);
    assert!(
        registry.request(reclaimer, 4_096).is_ok(),
        "reporting an outcome frees exactly one in-flight slot"
    );
}

#[test]
fn only_a_completed_reclaim_contributes_grantable_bytes() {
    let ticket = ReclaimTicketId::new(42);
    let completed = ReclaimOutcome::completed(ticket, 10_000, 7_500, 2_500);
    let failed = ReclaimOutcome::failed(ticket, 10_000, 7_500, 2_500);
    let timed_out = ReclaimOutcome::timed_out(ticket, 10_000, 7_500, 2_500);

    assert_eq!(
        completed.grantable_contribution_bytes(),
        7_500,
        "a completed reclaim contributes exactly what it confirmed"
    );
    assert_eq!(
        failed.grantable_contribution_bytes(),
        0,
        "a failed reclaim has an unknown end state and must never increase \
         grantable capacity"
    );
    assert_eq!(
        timed_out.grantable_contribution_bytes(),
        0,
        "an abandoned reclaim may still be running and must never increase \
         grantable capacity"
    );

    // The reported release is preserved for every status; what changes is
    // whether it may be granted against. Losing the number would hide real
    // work, and trusting it would grant memory the process does not have.
    assert_eq!(failed.released_confirmed_bytes, 7_500);
    assert_eq!(timed_out.released_confirmed_bytes, 7_500);
    assert_eq!(failed.still_retained_bytes, 2_500);
    assert!(!failed.met_target());
    assert!(!timed_out.met_target());
}

#[test]
fn registry_totals_keep_trusted_and_untrusted_releases_apart() {
    let registry = ReclaimRegistry::new(&MetadataBudget::with_defaults());
    let reclaimer = registry
        .register(StubReclaimer::new(50))
        .expect("registered");

    let ticket = registry.request(reclaimer, 1_000).expect("request");
    assert!(
        registry.record_outcome(&ReclaimOutcome::completed(ticket, 1_000, 600, 400)),
        "an outcome for a request in flight is reported as matched"
    );

    // Outcomes for tickets this registry never started: reported as unmatched
    // so the caller can raise a diagnostic, and still recorded, because
    // discarding a reported release would leave the totals understating what
    // came back.
    assert!(
        !registry.record_outcome(&ReclaimOutcome::failed(
            ReclaimTicketId::new(51),
            1_000,
            300,
            700
        )),
        "an outcome with no matching in-flight request is reported as unmatched"
    );
    assert!(!registry.record_outcome(&ReclaimOutcome::timed_out(
        ReclaimTicketId::new(52),
        1_000,
        100,
        900
    )));

    let totals = registry.totals();
    assert_eq!(totals.completed_requests, 1);
    assert_eq!(totals.failed_requests, 1);
    assert_eq!(totals.timed_out_requests, 1);
    assert_eq!(
        totals.confirmed_released_bytes, 1_000,
        "every reported release is observable"
    );
    assert_eq!(
        totals.grantable_contribution_bytes(),
        600,
        "only the completed request's release may inform grantable capacity"
    );
    assert_eq!(
        totals.untrusted_released_bytes(),
        400,
        "the gap is exactly the reclaim work that did not vouch for itself"
    );
}

#[test]
fn an_estimate_never_becomes_grantable_capacity_on_its_own() {
    let registry = ReclaimRegistry::new(&MetadataBudget::with_defaults());
    let reclaimer = registry
        .register(StubReclaimer::new(60))
        .expect("registered");

    let estimates = registry.estimates();
    assert_eq!(estimates.len(), 1);
    assert_eq!(estimates[0].estimate.candidate_bytes, 4_096);
    assert!(estimates[0].estimate.offers_anything());

    // A believed 4 KiB of candidates, and a request in flight for all of it,
    // still leave nothing grantable. Only a reported, completed outcome does.
    let ticket = registry.request(reclaimer, 4_096).expect("request");
    let pending = registry.totals();
    assert_eq!(pending.requested_bytes_in_flight, 4_096);
    assert_eq!(
        pending.grantable_contribution_bytes(),
        0,
        "believing and asking are not releasing"
    );

    assert!(registry.record_outcome(&ReclaimOutcome::completed(ticket, 4_096, 4_096, 0)));
    assert_eq!(registry.totals().grantable_contribution_bytes(), 4_096);
}
