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
// KIND, either express or implied.  session_token
// specific language governing permissions and limitations
// under the License.

//! The frontend's owner of one attempt's vended credential rotation.
//!
//! The frontend stays the only credential principal: it holds the provider's
//! refresh source, it decides when to rotate, and a backend installs what it
//! is handed and mints nothing. That division is ADR-0129's and does not
//! change here. What changes is the carrier and the handshake.
//!
//! # One step instead of two
//!
//! The lifecycle stack rotated a credential with a query-wide prepare followed
//! by a query-wide commit, so that every backend switched epoch together. A
//! query-context domain does not want that: each context's credential domain
//! advances on its own progression, and requiring all of them to move in
//! lockstep would let one slow or unknown-outcome backend hold a healthy one
//! on an expiring secret. So a rotation is a single `AdvanceDomain` per
//! context, and a backend's accepted epoch is its own.
//!
//! # What that costs, and the one thing it still serializes
//!
//! Independent per-context progression is not free. A context's domain accepts
//! only the *exact* next epoch, so if this owner minted epoch three while some
//! context was still on one, that context would see a gap and fail closed.
//!
//! Hence the single invariant this owner enforces: it may mint the next epoch
//! only once every participating context has accepted the current one. That is
//! deliberately much weaker than the old unanimity — delivery is concurrent
//! and unordered, no context waits for another to *apply*, and a context that
//! is already terminal is simply no longer a participant — but it is the one
//! serialization the gap rule makes unavoidable.
//!
//! # The epoch counts rotations of the batch
//!
//! One credential domain carries every vended lease of the query as one batch
//! under one epoch. A batch at epoch *n* may contain leases whose own provider
//! epochs differ, because refreshing one scope does not invalidate another. So
//! the domain epoch counts rotations of the batch, not of any single lease,
//! and a rotation must carry the whole batch rather than only the lease that
//! moved.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use std::time::Duration;

use novarocks_execution::task_execution::{
    AdvanceQueryContextDomain, ConfidentialContent, CredentialEpoch, CredentialLeaseId,
    CredentialUpdate, QueryContextDomainUpdate, QueryContextRef, TaskOperationId,
    UpdateQueryContext,
};
use novarocks_query_application::coordination::MonotonicInstant;
use novarocks_types::QueryExecutionId;

use super::error::TaskExecutionError;

/// How long before expiry a rotation should start, and when it is too late.
///
/// Both margins are proportions of the remaining lifetime rather than fixed
/// durations, so a short-lived vended credential and a long-lived one get the
/// same relative amount of room. The bounds keep either end from becoming
/// absurd: a one-hour lease does not wait 12 minutes' worth of jitter, and a
/// ten-second one still gets a whole second to fail in.
const SOFT_MARGIN_MIN: Duration = Duration::from_secs(5);
const SOFT_MARGIN_MAX: Duration = Duration::from_secs(5 * 60);
const HARD_MARGIN_MIN: Duration = Duration::from_secs(1);
const HARD_MARGIN_MAX: Duration = Duration::from_secs(30);
const JITTER_MAX: Duration = Duration::from_secs(30);

/// When one rotation should be attempted and when it must have succeeded.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub struct RefreshTiming {
    /// How long to wait before starting the rotation.
    soft_delay: Duration,
    /// How long until the credential can no longer be relied on.
    hard_delay: Duration,
}

impl RefreshTiming {
    pub const fn soft_delay(self) -> Duration {
        self.soft_delay
    }

    pub const fn hard_delay(self) -> Duration {
        self.hard_delay
    }
}

/// Derives one rotation's timing from the remaining lifetime.
///
/// The jitter is derived from identity rather than from a random source, so a
/// replay of the same decision reaches the same conclusion. Both halves of
/// that identity are required because each spreads a different collision:
/// the lease id spreads two leases of one attempt that expire together, and
/// the attempt id spreads the far more common case -- many concurrent queries
/// rotating the *same* credential, which would otherwise hit the provider in
/// lockstep and be rate-limited into failing every one of them. A caller that
/// passed only the domain token would get no spread at all, because that token
/// is the same constant for every attempt.
///
/// Neither identity is secret; the material never enters this.
pub fn refresh_timing(
    execution_id: QueryExecutionId,
    lease_id: CredentialLeaseId,
    remaining: Duration,
) -> RefreshTiming {
    let soft_margin = (remaining / 5).clamp(SOFT_MARGIN_MIN, SOFT_MARGIN_MAX);
    let hard_margin = (remaining / 20).clamp(HARD_MARGIN_MIN, HARD_MARGIN_MAX);
    let max_jitter = (remaining / 20).min(JITTER_MAX);
    let jitter_seed = lease_id
        .get()
        .to_be_bytes()
        .iter()
        .chain(execution_id.query_id().high().to_be_bytes().iter())
        .chain(execution_id.query_id().low().to_be_bytes().iter())
        .chain(execution_id.attempt_id().get().to_be_bytes().iter())
        .fold(0_u64, |seed, byte| seed.rotate_left(5) ^ u64::from(*byte));
    let jitter = if max_jitter.is_zero() {
        Duration::ZERO
    } else {
        Duration::from_nanos(
            jitter_seed % (u64::try_from(max_jitter.as_nanos()).unwrap_or(u64::MAX) + 1),
        )
    };
    RefreshTiming {
        soft_delay: remaining.saturating_sub(soft_margin.saturating_add(jitter)),
        hard_delay: remaining.saturating_sub(hard_margin),
    }
}

/// Why a rotation could not be started.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub enum RefreshRefusal {
    /// A rotation is already in flight to at least one context.
    ///
    /// This is the gap rule's serialization point, not contention: minting
    /// past a context that has not caught up would make its next update a
    /// gap, which fails closed.
    RotationInFlight,
    /// Every participating context is gone, so there is nothing to rotate.
    NoParticipants,
    /// The epoch space is exhausted.
    EpochExhausted,
}

impl std::fmt::Display for RefreshRefusal {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(match self {
            Self::RotationInFlight => {
                "a credential rotation is still being accepted by some query context"
            }
            Self::NoParticipants => "no query context participates in this credential domain",
            Self::EpochExhausted => "credential epoch space is exhausted",
        })
    }
}

impl std::error::Error for RefreshRefusal {}

/// The frontend owner of one attempt's credential domain.
///
/// It is a pure state machine: it produces immutable intents and consumes
/// acknowledgements, reads time only through readings it is given, and holds
/// no transport. Nothing here sleeps or spawns.
pub struct CredentialRefreshOwner {
    lease_id: CredentialLeaseId,
    /// The highest epoch this owner has minted.
    minted: CredentialEpoch,
    /// The material of the minted epoch, replayed verbatim to any context
    /// that has not accepted it yet.
    material: Arc<dyn ConfidentialContent>,
    /// Every context that must receive this domain.
    participants: BTreeSet<QueryContextRef>,
    /// The highest epoch each context has acknowledged.
    accepted: BTreeMap<QueryContextRef, CredentialEpoch>,
    /// Contexts holding a released, not-yet-settled request for `minted`.
    in_flight: BTreeSet<QueryContextRef>,
    /// When the minted epoch stops being usable.
    hard_deadline: Option<MonotonicInstant>,
}

impl std::fmt::Debug for CredentialRefreshOwner {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("CredentialRefreshOwner")
            .field("lease_id", &self.lease_id)
            .field("minted", &self.minted)
            .field("participants", &self.participants.len())
            .field("in_flight", &self.in_flight.len())
            .field("material", &"<redacted>")
            .finish()
    }
}

impl CredentialRefreshOwner {
    /// Starts from the initial credential every establish carried.
    ///
    /// Each participating context accepted epoch one by establishing, so this
    /// owner begins with the batch already delivered rather than with a
    /// rotation to send.
    pub fn from_establish(
        initial: &CredentialUpdate,
        participants: impl IntoIterator<Item = QueryContextRef>,
    ) -> Self {
        let participants = participants.into_iter().collect::<BTreeSet<_>>();
        let accepted = participants
            .iter()
            .map(|context| (*context, initial.epoch()))
            .collect();
        Self {
            lease_id: initial.lease_id(),
            minted: initial.epoch(),
            material: Arc::clone(initial.material()),
            participants,
            accepted,
            in_flight: BTreeSet::new(),
            hard_deadline: None,
        }
    }

    pub const fn lease_id(&self) -> CredentialLeaseId {
        self.lease_id
    }

    pub const fn minted_epoch(&self) -> CredentialEpoch {
        self.minted
    }

    pub const fn hard_deadline(&self) -> Option<MonotonicInstant> {
        self.hard_deadline
    }

    /// Whether the minted epoch is still being accepted somewhere.
    pub fn rotation_in_flight(&self) -> bool {
        !self.in_flight.is_empty() || self.contexts_behind().next().is_some()
    }

    /// Contexts that still owe an acknowledgement for the minted epoch.
    fn contexts_behind(&self) -> impl Iterator<Item = QueryContextRef> + '_ {
        self.participants.iter().copied().filter(|context| {
            self.accepted
                .get(context)
                .is_none_or(|accepted| *accepted < self.minted)
        })
    }

    /// Drops a context that is terminal or released.
    ///
    /// A context that has left cannot acknowledge anything, so leaving it in
    /// the participant set would stall every later rotation behind it.
    pub fn retire(&mut self, context: QueryContextRef) {
        self.participants.remove(&context);
        self.accepted.remove(&context);
        self.in_flight.remove(&context);
    }

    /// Mints the next epoch over freshly refreshed material.
    ///
    /// `material` must be the whole batch, including leases whose own provider
    /// epoch did not move; see the module documentation.
    pub fn rotate(
        &mut self,
        material: Arc<dyn ConfidentialContent>,
        hard_deadline: MonotonicInstant,
    ) -> Result<CredentialEpoch, RefreshRefusal> {
        if self.participants.is_empty() {
            return Err(RefreshRefusal::NoParticipants);
        }
        if self.rotation_in_flight() {
            return Err(RefreshRefusal::RotationInFlight);
        }
        let next = self.minted.next().ok_or(RefreshRefusal::EpochExhausted)?;
        self.minted = next;
        self.material = material;
        self.hard_deadline = Some(hard_deadline);
        Ok(next)
    }

    /// The next rotation request for one context, if it owes one.
    ///
    /// A context with a released request that has not settled gets nothing
    /// back: this protocol allows at most one in-flight operation per context
    /// domain, and an unknown outcome is resolved by resending the identical
    /// request through [`Self::retry`], never by queuing a second one.
    pub fn advance_intent(
        &mut self,
        context: QueryContextRef,
    ) -> Result<Option<UpdateQueryContext>, TaskExecutionError> {
        if !self.participants.contains(&context) {
            return Ok(None);
        }
        if self.in_flight.contains(&context) {
            return Ok(None);
        }
        if self
            .accepted
            .get(&context)
            .is_some_and(|accepted| *accepted >= self.minted)
        {
            return Ok(None);
        }
        self.in_flight.insert(context);
        Ok(Some(self.request_for(context)))
    }

    /// The identical request again, after a genuinely unknown outcome.
    ///
    /// It is byte-for-byte the request that was already sent, including the
    /// same material, because a backend recognises a replay only by finding
    /// the same epoch carrying the same content. A newly refreshed secret at
    /// the same epoch would be a conflict, not a retry.
    pub fn retry(&self, context: QueryContextRef) -> Option<UpdateQueryContext> {
        self.in_flight
            .contains(&context)
            .then(|| self.request_for(context))
    }

    fn request_for(&self, context: QueryContextRef) -> UpdateQueryContext {
        UpdateQueryContext::AdvanceDomain(AdvanceQueryContextDomain::new(
            TaskOperationId::new_v7(),
            context,
            QueryContextDomainUpdate::Credential(CredentialUpdate::new(
                self.lease_id,
                self.minted,
                Arc::clone(&self.material),
            )),
        ))
    }

    /// Records what one context reports as its accepted epoch.
    ///
    /// A receipt naming an older epoch is not an error and not a rollback: a
    /// context answers with its current state, and a late receipt for a
    /// superseded epoch is simply behind. It clears the in-flight slot either
    /// way, because the request it answers is settled.
    pub fn accept(&mut self, context: QueryContextRef, accepted_epoch: CredentialEpoch) {
        self.in_flight.remove(&context);
        if !self.participants.contains(&context) {
            return;
        }
        let entry = self.accepted.entry(context).or_insert(accepted_epoch);
        if accepted_epoch > *entry {
            *entry = accepted_epoch;
        }
    }

    /// Whether every participating context has caught up to the minted epoch.
    pub fn fully_accepted(&self) -> bool {
        !self.rotation_in_flight()
    }

    /// Drops the material.
    ///
    /// Called for a cancel, an abort, and a normal finish. After this the
    /// owner can no longer produce a request, which is what makes it safe to
    /// keep the owner alive while an attempt unwinds.
    pub fn wipe(&mut self) {
        self.participants.clear();
        self.accepted.clear();
        self.in_flight.clear();
        self.hard_deadline = None;
        self.material = Arc::new(NoMaterial);
    }
}

/// The absence of material, after a wipe.
///
/// A wiped owner keeps a `ConfidentialContent` rather than an `Option` so no
/// caller has to handle a half-initialized owner. It matches nothing, so a
/// request built from it could never be accepted as a replay.
struct NoMaterial;

impl ConfidentialContent for NoMaterial {
    fn encoded_len(&self) -> usize {
        0
    }

    fn matches(&self, _other: &dyn ConfidentialContent) -> bool {
        false
    }
}

#[cfg(test)]
mod tests {
    use super::{CredentialRefreshOwner, RefreshRefusal, refresh_timing};

    use std::sync::Arc;
    use std::time::Duration;

    use novarocks_execution::task_execution::{
        ConfidentialContent, CredentialEpoch, CredentialLeaseId, CredentialUpdate,
        QueryContextDomainUpdate, QueryContextRef, UpdateQueryContext,
    };
    use novarocks_query_application::coordination::MonotonicInstant;
    use novarocks_types::identity::{
        AttemptId, BackendProcessId, FrontendProcessId, QueryExecutionId, QueryId,
    };

    const SECRET_SENTINEL: &str = "NOVAROCKS_SECRET_SENTINEL";

    struct FakeSecret(&'static str);

    impl ConfidentialContent for FakeSecret {
        fn encoded_len(&self) -> usize {
            self.0.len()
        }

        fn matches(&self, other: &dyn ConfidentialContent) -> bool {
            self.encoded_len() == other.encoded_len()
        }
    }

    fn execution() -> QueryExecutionId {
        QueryExecutionId::new(QueryId::new(7, 9), AttemptId::new(1).expect("nonzero"))
            .expect("legal execution")
    }

    fn context(seed: u8) -> QueryContextRef {
        let _ = seed;
        QueryContextRef::new(
            execution(),
            FrontendProcessId::new_v7(),
            BackendProcessId::new_v7(),
        )
    }

    fn initial() -> CredentialUpdate {
        CredentialUpdate::new(
            CredentialLeaseId::new(1),
            CredentialEpoch::FIRST,
            Arc::new(FakeSecret(SECRET_SENTINEL)),
        )
    }

    fn epoch_of(request: &UpdateQueryContext) -> CredentialEpoch {
        match request {
            UpdateQueryContext::AdvanceDomain(advance) => match advance.domain() {
                QueryContextDomainUpdate::Credential(update) => update.epoch(),
                other => panic!("expected a credential domain, got {other:?}"),
            },
            other => panic!("expected an advance, got {other:?}"),
        }
    }

    #[test]
    fn establishing_already_counts_as_accepting_the_first_epoch() {
        let (first, second) = (context(1), context(2));
        let mut owner = CredentialRefreshOwner::from_establish(&initial(), [first, second]);

        assert_eq!(owner.minted_epoch(), CredentialEpoch::FIRST);
        assert!(owner.fully_accepted());
        assert!(
            owner.advance_intent(first).expect("no error").is_none(),
            "the establish already delivered epoch one"
        );
    }

    #[test]
    fn the_next_epoch_waits_for_every_context_to_catch_up() {
        let (first, second) = (context(1), context(2));
        let mut owner = CredentialRefreshOwner::from_establish(&initial(), [first, second]);
        let deadline = MonotonicInstant::from_origin(Duration::from_secs(10));

        let second_epoch = owner
            .rotate(Arc::new(FakeSecret("rotated")), deadline)
            .expect("the first rotation is free");
        assert_eq!(second_epoch.get(), 2);

        // Only one context has caught up. Minting a third epoch now would make
        // the other context's next update a gap, which fails closed.
        let request = owner
            .advance_intent(first)
            .expect("no error")
            .expect("first context owes epoch two");
        assert_eq!(epoch_of(&request), second_epoch);
        owner.accept(first, second_epoch);
        assert_eq!(
            owner.rotate(Arc::new(FakeSecret("third")), deadline),
            Err(RefreshRefusal::RotationInFlight)
        );

        let request = owner
            .advance_intent(second)
            .expect("no error")
            .expect("second context owes epoch two");
        assert_eq!(epoch_of(&request), second_epoch);
        owner.accept(second, second_epoch);
        assert!(owner.fully_accepted());
        assert_eq!(
            owner
                .rotate(Arc::new(FakeSecret("third")), deadline)
                .expect("both caught up")
                .get(),
            3
        );
    }

    #[test]
    fn a_retired_context_does_not_stall_the_next_rotation() {
        let (first, second) = (context(1), context(2));
        let mut owner = CredentialRefreshOwner::from_establish(&initial(), [first, second]);
        let deadline = MonotonicInstant::from_origin(Duration::from_secs(10));

        let epoch = owner
            .rotate(Arc::new(FakeSecret("rotated")), deadline)
            .expect("rotation");
        owner.advance_intent(first).expect("no error");
        owner.accept(first, epoch);

        // The second context never answers because it is gone. Without
        // retiring it, every later rotation would be blocked behind a context
        // that can no longer accept anything.
        owner.retire(second);
        assert!(owner.fully_accepted());
        assert!(
            owner
                .rotate(Arc::new(FakeSecret("third")), deadline)
                .is_ok()
        );
        assert!(
            owner.advance_intent(second).expect("no error").is_none(),
            "a retired context is no longer addressed"
        );
    }

    #[test]
    fn an_unknown_outcome_resends_the_identical_request_and_never_queues_a_second() {
        let only = context(1);
        let mut owner = CredentialRefreshOwner::from_establish(&initial(), [only]);
        let deadline = MonotonicInstant::from_origin(Duration::from_secs(10));
        let epoch = owner
            .rotate(Arc::new(FakeSecret("rotated")), deadline)
            .expect("rotation");

        let first = owner
            .advance_intent(only)
            .expect("no error")
            .expect("owes the rotation");
        assert!(
            owner.advance_intent(only).expect("no error").is_none(),
            "a released request must not be queued twice"
        );

        let again = owner
            .retry(only)
            .expect("an unknown outcome may be retried");
        assert_eq!(epoch_of(&first), epoch);
        assert_eq!(epoch_of(&again), epoch);

        owner.accept(only, epoch);
        assert!(
            owner.retry(only).is_none(),
            "a settled request is not retryable"
        );
    }

    #[test]
    fn a_receipt_naming_an_older_epoch_does_not_roll_the_owner_back() {
        let only = context(1);
        let mut owner = CredentialRefreshOwner::from_establish(&initial(), [only]);
        let deadline = MonotonicInstant::from_origin(Duration::from_secs(10));
        let second = owner
            .rotate(Arc::new(FakeSecret("rotated")), deadline)
            .expect("rotation");
        owner.advance_intent(only).expect("no error");
        owner.accept(only, second);

        // A late receipt for the superseded epoch must not make the owner
        // believe this context regressed and owes epoch two again.
        owner.accept(only, CredentialEpoch::FIRST);
        assert!(owner.fully_accepted());
        assert!(owner.advance_intent(only).expect("no error").is_none());
    }

    #[test]
    fn a_wiped_owner_can_no_longer_produce_a_request() {
        let only = context(1);
        let mut owner = CredentialRefreshOwner::from_establish(&initial(), [only]);
        let deadline = MonotonicInstant::from_origin(Duration::from_secs(10));
        owner
            .rotate(Arc::new(FakeSecret("rotated")), deadline)
            .expect("rotation");

        owner.wipe();
        assert!(owner.advance_intent(only).expect("no error").is_none());
        assert!(owner.retry(only).is_none());
        assert_eq!(owner.hard_deadline(), None);
        assert_eq!(
            owner.rotate(Arc::new(FakeSecret("third")), deadline),
            Err(RefreshRefusal::NoParticipants)
        );
    }

    #[test]
    fn credential_material_never_appears_in_any_rendering() {
        let only = context(1);
        let mut owner = CredentialRefreshOwner::from_establish(&initial(), [only]);

        let rendered = format!("{owner:?}");
        assert!(
            !rendered.contains(SECRET_SENTINEL),
            "credential material leaked into Debug: {rendered}"
        );
        assert!(rendered.contains("<redacted>"), "{rendered}");
        assert!(rendered.contains("minted"), "{rendered}");

        let deadline = MonotonicInstant::from_origin(Duration::from_secs(10));
        owner
            .rotate(Arc::new(FakeSecret(SECRET_SENTINEL)), deadline)
            .expect("rotation");
        let request = owner
            .advance_intent(only)
            .expect("no error")
            .expect("owes the rotation");
        let rendered = format!("{request:?}");
        assert!(
            !rendered.contains(SECRET_SENTINEL),
            "credential material leaked into an intent: {rendered}"
        );

        let refusal = owner
            .rotate(Arc::new(FakeSecret(SECRET_SENTINEL)), deadline)
            .expect_err("a rotation is in flight")
            .to_string();
        assert!(!refusal.contains(SECRET_SENTINEL), "{refusal}");
    }

    #[test]
    fn refresh_timing_scales_with_the_remaining_lifetime_and_stays_bounded() {
        // A one-hour credential rotates well before expiry but not absurdly
        // early, and the hard margin is capped so a long lease does not
        // reserve minutes it cannot use.
        let hour = refresh_timing(
            execution(),
            CredentialLeaseId::new(1),
            Duration::from_secs(3600),
        );
        assert!(hour.soft_delay() < hour.hard_delay());
        assert_eq!(
            Duration::from_secs(3600) - hour.hard_delay(),
            Duration::from_secs(30),
            "the hard margin is clamped to its maximum"
        );

        // A ten-second credential still gets a full second of hard margin. Its
        // soft margin is raised to the five-second floor, so rotation starts at
        // the halfway point rather than immediately: raising the margin moves
        // the delay earlier, it does not erase it.
        let short = refresh_timing(
            execution(),
            CredentialLeaseId::new(1),
            Duration::from_secs(10),
        );
        assert!(
            short.soft_delay() <= Duration::from_secs(5)
                && short.soft_delay() > Duration::from_secs(4),
            "a five-second floor on a ten-second lease leaves about five seconds, got {:?}",
            short.soft_delay()
        );
        assert!(short.soft_delay() < short.hard_delay());
        assert_eq!(short.hard_delay(), Duration::from_secs(9));

        // Only a lease shorter than the floor itself rotates immediately. This
        // is the branch that matters for a credential whose remaining lifetime
        // is already inside the margin we reserve to fail in.
        let expiring = refresh_timing(
            execution(),
            CredentialLeaseId::new(1),
            Duration::from_secs(4),
        );
        assert_eq!(
            expiring.soft_delay(),
            Duration::ZERO,
            "a lease shorter than the soft margin has no room to wait"
        );
        assert_eq!(expiring.hard_delay(), Duration::from_secs(3));

        // The jitter is derived from identity, so two leases expiring together
        // do not rotate in the same instant, and the same identity always
        // reaches the same decision.
        let first = refresh_timing(
            execution(),
            CredentialLeaseId::new(1),
            Duration::from_secs(3600),
        );
        let second = refresh_timing(
            execution(),
            CredentialLeaseId::new(2),
            Duration::from_secs(3600),
        );
        assert_ne!(first.soft_delay(), second.soft_delay());
        assert_eq!(
            first,
            refresh_timing(
                execution(),
                CredentialLeaseId::new(1),
                Duration::from_secs(3600)
            )
        );

        // And the case that actually matters in production: many concurrent
        // queries rotating the SAME credential. The domain token is the same
        // constant for every attempt, so seeding from it alone would send every
        // query to the provider in the same instant -- rate-limited into
        // failing all of them at once. Two attempts must therefore differ even
        // when their lease id does not.
        let other_attempt =
            QueryExecutionId::new(QueryId::new(7, 9), AttemptId::new(2).expect("nonzero"))
                .expect("legal execution");
        assert_ne!(
            first.soft_delay(),
            refresh_timing(
                other_attempt,
                CredentialLeaseId::new(1),
                Duration::from_secs(3600)
            )
            .soft_delay(),
            "one credential rotated by two attempts must not rotate in lockstep"
        );
    }
}
