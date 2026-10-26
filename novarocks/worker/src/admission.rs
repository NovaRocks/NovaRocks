// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. See the License for the
// specific language governing permissions and limitations
// under the License.

//! Worker-local admission ticket authority.
//!
//! A ticket is a capacity capability, so only this owner may mint one. The
//! shared execution contract can validate and carry its opaque nonce, but it
//! deliberately exposes no constructor that could let a frontend self-issue
//! capacity.

use std::collections::{BTreeMap, VecDeque};
use std::fmt;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use novarocks_execution_contract::{
    AcquireQueryContextAdmissionTicket, AdmissionEpochCapability, AdmissionTicketId, LeaseValidFor,
    QueryContextAdmissionTicketReceipt, QueryContextRef, TaskOperationId,
};
use novarocks_types::NativeCompatibilityId;
use uuid::Uuid;

use crate::MonotonicInstant;

const AUTHORITY_LOCK: &str = "admission ticket authority lock";

pub use novarocks_worker_protocol::admission::{
    AdmissionTicketConfig, MAX_ADMISSION_RESERVATIONS, MAX_ADMISSION_TICKET_VALID_FOR,
};

/// The single owner's state for one ticket nonce.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub enum AdmissionTicketState {
    Issued,
    Redeemed,
    Closed,
    Expired,
}

/// A read-only, context-bound view of one ticket at the worker's monotonic time.
///
/// This is advisory for ingress wait budgeting. Only `redeem` can decide
/// whether a later establish actually consumes the ticket.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub enum AdmissionTicketObservation {
    Issued { remaining: Duration },
    Redeemed,
    Expired,
    Closed,
    Unknown,
    ForeignContext,
}

/// Whether an acquisition issued a new grant or replayed the original one.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub enum AdmissionTicketProgression {
    Issued,
    Replayed,
}

/// A successful acquisition and its immutable receipt.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub struct AdmissionTicketGrant {
    progression: AdmissionTicketProgression,
    receipt: QueryContextAdmissionTicketReceipt,
}

impl AdmissionTicketGrant {
    const fn new(
        progression: AdmissionTicketProgression,
        receipt: QueryContextAdmissionTicketReceipt,
    ) -> Self {
        Self {
            progression,
            receipt,
        }
    }

    pub const fn progression(self) -> AdmissionTicketProgression {
        self.progression
    }

    pub const fn receipt(self) -> QueryContextAdmissionTicketReceipt {
        self.receipt
    }
}

/// Why this worker refused to issue a ticket.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub enum AdmissionTicketAcquisitionRejection {
    ValidityExceedsWorkerLimit,
    ReservationCapacityExhausted,
    ReplayCapacityExhausted,
    ContextAlreadyGranted,
    OperationReplayConflict,
    SealedEpoch,
    Inactive(AdmissionTicketState),
}

impl fmt::Display for AdmissionTicketAcquisitionRejection {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::ValidityExceedsWorkerLimit => {
                "admission ticket validity exceeds the worker limit"
            }
            Self::ReservationCapacityExhausted => {
                "worker query-context admission capacity is exhausted"
            }
            Self::ReplayCapacityExhausted => "worker admission ticket replay capacity is exhausted",
            Self::ContextAlreadyGranted => "query context already has an active admission ticket",
            Self::OperationReplayConflict => {
                "admission ticket operation id was replayed with different content"
            }
            Self::SealedEpoch => "admission epoch is sealed",
            Self::Inactive(AdmissionTicketState::Closed) => "admission ticket is closed",
            Self::Inactive(AdmissionTicketState::Expired) => "admission ticket is expired",
            Self::Inactive(AdmissionTicketState::Issued | AdmissionTicketState::Redeemed) => {
                "admission ticket has an invalid active replay state"
            }
        })
    }
}

impl std::error::Error for AdmissionTicketAcquisitionRejection {}

/// Whether an establish consumed a grant or replayed the same redemption.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub enum AdmissionTicketRedemption {
    Redeemed(QueryContextAdmissionTicketReceipt),
    Replayed(QueryContextAdmissionTicketReceipt),
}

/// Why a ticket cannot authorize an establish.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub enum AdmissionTicketRedemptionRejection {
    Unknown,
    Expired,
    Closed,
    ForeignContext,
}

impl fmt::Display for AdmissionTicketRedemptionRejection {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::Unknown => "establish names an unknown admission ticket",
            Self::Expired => "establish names an expired admission ticket",
            Self::Closed => "establish names a closed admission ticket",
            Self::ForeignContext => "admission ticket belongs to a different query context",
        })
    }
}

impl std::error::Error for AdmissionTicketRedemptionRejection {}

#[derive(Copy, Clone)]
struct TicketRecord {
    operation_id: TaskOperationId,
    receipt: QueryContextAdmissionTicketReceipt,
    expires_at: MonotonicInstant,
    state: AdmissionTicketState,
    terminal_at: Option<MonotonicInstant>,
}

/// What an operation is, for the purpose of deciding whether a second request
/// naming it is the same operation.
///
/// The admission capability is deliberately not part of it. It is not a
/// property of the operation but of when the frontend last heard from this
/// worker, and it moves on its own: a frontend that replays a request after
/// one heartbeat carries a newer capability than it first sent, and calling
/// that a different operation would turn the prescribed recovery into a
/// conflict. What the capability decides is whether an operation this ledger
/// has never seen may be admitted at all, which is a separate question asked
/// after this one.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
struct AdmissionAcquisitionIdentity {
    context: QueryContextRef,
    valid_for: LeaseValidFor,
    native_compatibility_id: NativeCompatibilityId,
}

impl From<AcquireQueryContextAdmissionTicket> for AdmissionAcquisitionIdentity {
    fn from(request: AcquireQueryContextAdmissionTicket) -> Self {
        Self {
            context: request.context(),
            valid_for: request.valid_for(),
            native_compatibility_id: request.native_compatibility_id(),
        }
    }
}

#[derive(Copy, Clone, Debug, Eq, PartialEq)]
enum AdmissionAcquisitionDecision {
    Granted(QueryContextAdmissionTicketReceipt),
    Rejected(AdmissionTicketAcquisitionRejection),
}

#[derive(Copy, Clone, Debug, Eq, PartialEq)]
struct AdmissionAcquisitionRecord {
    identity: AdmissionAcquisitionIdentity,
    decision: AdmissionAcquisitionDecision,
    terminal_at: Option<MonotonicInstant>,
    /// How far the ledger had advanced when this decision was made. Reclaiming
    /// the record moves the frontier to it, which is how a later request is
    /// told that what it might be replaying is no longer here.
    stamp: u64,
}

struct AdmissionTicketStateOwner {
    /// The stamp the next decision will carry. It only ever advances.
    next_stamp: u64,
    /// Every decision stamped at or below this has been reclaimed, so a
    /// request carrying such a stamp may be replaying one this ledger no
    /// longer holds and cannot be admitted as new.
    reclaim_frontier: u64,
    tickets: BTreeMap<AdmissionTicketId, TicketRecord>,
    acquisitions: BTreeMap<TaskOperationId, AdmissionAcquisitionRecord>,
    terminal_acquisition_order: VecDeque<TaskOperationId>,
    terminal_order: VecDeque<AdmissionTicketId>,
    issued: usize,
    reserved: usize,
}

/// Lock-free observation of the Worker-owned reservation count.
///
/// The ticket mutex remains the sole authority. A reader may briefly see the
/// preceding committed count while a mutation still holds that mutex.
pub struct AdmissionReservationObservation {
    used: AtomicUsize,
    last_published_unix_seconds: AtomicU64,
}

impl Default for AdmissionReservationObservation {
    fn default() -> Self {
        Self {
            used: AtomicUsize::new(0),
            last_published_unix_seconds: AtomicU64::new(unix_time_seconds()),
        }
    }
}

impl AdmissionReservationObservation {
    pub fn used(&self) -> usize {
        self.used.load(Ordering::Acquire)
    }

    pub fn last_published_unix_seconds(&self) -> u64 {
        self.last_published_unix_seconds.load(Ordering::Acquire)
    }

    fn publish(&self, used: usize) {
        self.used.store(used, Ordering::Release);
        self.last_published_unix_seconds
            .store(unix_time_seconds(), Ordering::Release);
    }
}

fn unix_time_seconds() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |elapsed| elapsed.as_secs())
}

impl AdmissionTicketStateOwner {
    fn new() -> Self {
        Self {
            // The first decision is stamped 1, so 0 is a frontier that nothing
            // has yet reached and every published stamp clears it.
            next_stamp: 1,
            reclaim_frontier: 0,
            tickets: BTreeMap::new(),
            acquisitions: BTreeMap::new(),
            terminal_acquisition_order: VecDeque::new(),
            terminal_order: VecDeque::new(),
            issued: 0,
            reserved: 0,
        }
    }
}

/// The only worker-local owner allowed to mint, redeem, and close grants.
pub struct AdmissionTicketAuthority {
    config: AdmissionTicketConfig,
    /// Minted once per authority. It is what tells a capability from another
    /// worker process apart from one of this worker's own older capabilities:
    /// the first is meaningless here, the second is merely behind.
    issuer: [u8; 8],
    state: Mutex<AdmissionTicketStateOwner>,
    reservation_observation: Arc<AdmissionReservationObservation>,
}

impl AdmissionTicketAuthority {
    pub fn new(config: AdmissionTicketConfig) -> Self {
        Self {
            config,
            issuer: mint_admission_issuer(),
            state: Mutex::new(AdmissionTicketStateOwner::new()),
            reservation_observation: Arc::new(AdmissionReservationObservation::default()),
        }
    }

    pub const fn config(&self) -> AdmissionTicketConfig {
        self.config
    }

    pub fn reservation_observation(&self) -> Arc<AdmissionReservationObservation> {
        Arc::clone(&self.reservation_observation)
    }

    /// Returns the capability that may authorize new acquisitions now.
    ///
    /// Retention is advanced first so the published stamp is never one this
    /// worker is about to leave behind in the same breath.
    pub fn current_epoch(&self, now: MonotonicInstant) -> AdmissionEpochCapability {
        let mut state = self.state.lock().expect(AUTHORITY_LOCK);
        self.expire_locked(&mut state, now);
        self.reap_locked(&mut state, now);
        self.capability_locked(&state)
    }

    fn capability_locked(&self, state: &AdmissionTicketStateOwner) -> AdmissionEpochCapability {
        AdmissionEpochCapability::try_from_issuer_and_stamp(self.issuer, state.next_stamp)
            .expect("a nonzero issuer makes a nonzero capability")
    }

    /// Issues or exactly replays one immutable acquisition request.
    pub fn acquire(
        &self,
        request: AcquireQueryContextAdmissionTicket,
        now: MonotonicInstant,
    ) -> Result<AdmissionTicketGrant, AdmissionTicketAcquisitionRejection> {
        let mut state = self.state.lock().expect(AUTHORITY_LOCK);
        self.expire_locked(&mut state, now);
        self.reap_locked(&mut state, now);

        let operation_id = request.envelope().operation_id();
        let identity = AdmissionAcquisitionIdentity::from(request);
        if let Some(record) = state.acquisitions.get(&operation_id) {
            if record.identity != identity {
                return Err(AdmissionTicketAcquisitionRejection::OperationReplayConflict);
            }
            return match record.decision {
                AdmissionAcquisitionDecision::Granted(receipt) => Ok(AdmissionTicketGrant::new(
                    AdmissionTicketProgression::Replayed,
                    receipt,
                )),
                AdmissionAcquisitionDecision::Rejected(rejection) => Err(rejection),
            };
        }

        // Design: ADR-0152 (docs/adr/ADR-0152-admission-capability-is-a-ledger-frontier.md)
        // Two different refusals, and they are not the same question.
        //
        // A capability from another worker process says nothing about this
        // ledger, so it cannot authorize anything here. A capability from this
        // process that is at or below the reclaim frontier might be replaying a
        // decision this ledger no longer holds, and admitting it as new would
        // admit the same operation twice.
        //
        // Anything above the frontier is safe: this ledger still holds every
        // decision it could be replaying, and the branch above already found
        // and replayed it if so.
        let capability = request.admission_epoch_capability();
        if capability.issuer() != self.issuer || capability.stamp() <= state.reclaim_frontier {
            return Err(AdmissionTicketAcquisitionRejection::SealedEpoch);
        }
        if !self.ensure_acquisition_capacity_locked(&mut state) {
            return Err(AdmissionTicketAcquisitionRejection::SealedEpoch);
        }

        let rejection = if request.valid_for().get() > self.config.max_valid_for() {
            Some(AdmissionTicketAcquisitionRejection::ValidityExceedsWorkerLimit)
        } else if state.reserved >= self.config.max_reservations() {
            Some(AdmissionTicketAcquisitionRejection::ReservationCapacityExhausted)
        } else if state.tickets.values().any(|record| {
            record.receipt.context() == request.context()
                && matches!(
                    record.state,
                    AdmissionTicketState::Issued | AdmissionTicketState::Redeemed
                )
        }) {
            Some(AdmissionTicketAcquisitionRejection::ContextAlreadyGranted)
        } else {
            None
        };
        if let Some(rejection) = rejection {
            self.record_rejection_locked(&mut state, operation_id, identity, rejection, now);
            return Err(rejection);
        }

        let ticket_id = loop {
            let candidate = AdmissionTicketId::try_from_bytes(Uuid::new_v4().into_bytes())
                .expect("UUIDv4 is a nonzero 16-byte nonce");
            if !state.tickets.contains_key(&candidate) {
                break candidate;
            }
        };
        let receipt = QueryContextAdmissionTicketReceipt::new(
            ticket_id,
            request.context(),
            request.valid_for(),
        );
        state.tickets.insert(
            ticket_id,
            TicketRecord {
                operation_id,
                receipt,
                expires_at: now.saturating_add(request.valid_for().get()),
                state: AdmissionTicketState::Issued,
                terminal_at: None,
            },
        );
        let stamp = next_stamp_locked(&mut state);
        state.acquisitions.insert(
            operation_id,
            AdmissionAcquisitionRecord {
                identity,
                decision: AdmissionAcquisitionDecision::Granted(receipt),
                terminal_at: None,
                stamp,
            },
        );
        state.issued += 1;
        state.reserved += 1;
        self.reservation_observation.publish(state.reserved);
        Ok(AdmissionTicketGrant::new(
            AdmissionTicketProgression::Issued,
            receipt,
        ))
    }

    fn record_rejection_locked(
        &self,
        state: &mut AdmissionTicketStateOwner,
        operation_id: TaskOperationId,
        identity: AdmissionAcquisitionIdentity,
        rejection: AdmissionTicketAcquisitionRejection,
        now: MonotonicInstant,
    ) {
        let stamp = next_stamp_locked(state);
        let previous = state.acquisitions.insert(
            operation_id,
            AdmissionAcquisitionRecord {
                identity,
                decision: AdmissionAcquisitionDecision::Rejected(rejection),
                terminal_at: Some(now),
                stamp,
            },
        );
        debug_assert!(
            previous.is_none(),
            "a new operation id cannot replace a decision"
        );
        state.terminal_acquisition_order.push_back(operation_id);
    }

    fn ensure_acquisition_capacity_locked(&self, state: &mut AdmissionTicketStateOwner) -> bool {
        if state.acquisitions.len() < self.config.max_records() {
            return true;
        }

        // Retained decisions are never evicted early to make room, so a full
        // ledger simply admits nothing new until retention frees a slot. It
        // does not touch the reclaim frontier: nothing has been forgotten, so
        // no capability has become unsafe, and refusing one request is not a
        // reason to refuse every other worker's in-flight work as well.
        false
    }

    /// Consumes one grant for its exact query context.
    pub fn redeem(
        &self,
        ticket_id: AdmissionTicketId,
        context: QueryContextRef,
        now: MonotonicInstant,
    ) -> Result<AdmissionTicketRedemption, AdmissionTicketRedemptionRejection> {
        let mut state = self.state.lock().expect(AUTHORITY_LOCK);
        self.expire_locked(&mut state, now);
        self.reap_locked(&mut state, now);
        let Some(record) = state.tickets.get(&ticket_id) else {
            return Err(AdmissionTicketRedemptionRejection::Unknown);
        };
        if record.receipt.context() != context {
            return Err(AdmissionTicketRedemptionRejection::ForeignContext);
        }
        let ticket_state = record.state;
        let receipt = record.receipt;
        match ticket_state {
            AdmissionTicketState::Issued => {
                state
                    .tickets
                    .get_mut(&ticket_id)
                    .expect("ticket remains present")
                    .state = AdmissionTicketState::Redeemed;
                state.issued = state.issued.saturating_sub(1);
                Ok(AdmissionTicketRedemption::Redeemed(receipt))
            }
            AdmissionTicketState::Redeemed => Ok(AdmissionTicketRedemption::Replayed(receipt)),
            AdmissionTicketState::Closed => Err(AdmissionTicketRedemptionRejection::Closed),
            AdmissionTicketState::Expired => Err(AdmissionTicketRedemptionRejection::Expired),
        }
    }

    /// Revokes grants that have not yet installed a live query context.
    pub fn revoke_unredeemed(&self, context: QueryContextRef, now: MonotonicInstant) -> usize {
        let mut state = self.state.lock().expect(AUTHORITY_LOCK);
        self.expire_locked(&mut state, now);
        let affected = state
            .tickets
            .iter()
            .filter_map(|(ticket_id, record)| {
                (record.receipt.context() == context
                    && record.state == AdmissionTicketState::Issued)
                    .then_some(*ticket_id)
            })
            .collect::<Vec<_>>();
        for ticket_id in &affected {
            state.issued = state.issued.saturating_sub(1);
            state.reserved = state.reserved.saturating_sub(1);
            let record = state
                .tickets
                .get_mut(ticket_id)
                .expect("collected ticket remains present");
            record.state = AdmissionTicketState::Closed;
            record.terminal_at = Some(now);
            let operation_id = record.operation_id;
            state.terminal_order.push_back(*ticket_id);
            self.mark_acquisition_terminal_locked(&mut state, operation_id, now);
        }
        self.reservation_observation.publish(state.reserved);
        self.reap_locked(&mut state, now);
        affected.len()
    }

    /// Releases every grant after the query context has actually stopped.
    pub fn release_context(&self, context: QueryContextRef, now: MonotonicInstant) -> usize {
        let mut state = self.state.lock().expect(AUTHORITY_LOCK);
        self.expire_locked(&mut state, now);
        let affected = state
            .tickets
            .iter()
            .filter_map(|(ticket_id, record)| {
                (record.receipt.context() == context
                    && matches!(
                        record.state,
                        AdmissionTicketState::Issued | AdmissionTicketState::Redeemed
                    ))
                .then_some(*ticket_id)
            })
            .collect::<Vec<_>>();
        for ticket_id in &affected {
            let was_issued = state
                .tickets
                .get(ticket_id)
                .is_some_and(|record| record.state == AdmissionTicketState::Issued);
            if was_issued {
                state.issued = state.issued.saturating_sub(1);
            }
            state.reserved = state.reserved.saturating_sub(1);
            let record = state
                .tickets
                .get_mut(ticket_id)
                .expect("collected ticket remains present");
            record.state = AdmissionTicketState::Closed;
            record.terminal_at = Some(now);
            let operation_id = record.operation_id;
            state.terminal_order.push_back(*ticket_id);
            self.mark_acquisition_terminal_locked(&mut state, operation_id, now);
        }
        self.reservation_observation.publish(state.reserved);
        self.reap_locked(&mut state, now);
        affected.len()
    }

    /// Expires grants and reclaims terminal replay records past the horizon.
    pub fn advance_deadlines(&self, now: MonotonicInstant) -> usize {
        let mut state = self.state.lock().expect(AUTHORITY_LOCK);
        let expired = self.expire_locked(&mut state, now);
        self.reap_locked(&mut state, now);
        expired
    }

    pub fn state(
        &self,
        ticket_id: AdmissionTicketId,
        now: MonotonicInstant,
    ) -> Option<AdmissionTicketState> {
        let mut state = self.state.lock().expect(AUTHORITY_LOCK);
        self.expire_locked(&mut state, now);
        self.reap_locked(&mut state, now);
        state.tickets.get(&ticket_id).map(|record| record.state)
    }

    /// Observes one ticket under the authority lock without changing its state.
    ///
    /// In particular, a ticket whose deadline has passed is reported expired
    /// even if the regular deadline sweep has not run yet. Keeping this read
    /// free of sweeps avoids an O(tickets) critical section on the ingress path.
    pub fn observe(
        &self,
        ticket_id: AdmissionTicketId,
        context: QueryContextRef,
        now: MonotonicInstant,
    ) -> AdmissionTicketObservation {
        let state = self.state.lock().expect(AUTHORITY_LOCK);
        let Some(record) = state.tickets.get(&ticket_id) else {
            return AdmissionTicketObservation::Unknown;
        };
        if record.receipt.context() != context {
            return AdmissionTicketObservation::ForeignContext;
        }
        match record.state {
            AdmissionTicketState::Issued if now.has_reached(record.expires_at) => {
                AdmissionTicketObservation::Expired
            }
            AdmissionTicketState::Issued => AdmissionTicketObservation::Issued {
                remaining: record.expires_at.saturating_duration_since(now),
            },
            AdmissionTicketState::Redeemed => AdmissionTicketObservation::Redeemed,
            AdmissionTicketState::Expired => AdmissionTicketObservation::Expired,
            AdmissionTicketState::Closed => AdmissionTicketObservation::Closed,
        }
    }

    /// Performs the owner's expiry transition for one exact ticket binding.
    ///
    /// This is used only when a shallow ingress observation found an already
    /// expired issuance. Redeemed tickets return false, so an exact Establish
    /// replay can still follow its normal Worker path after issuance expiry.
    pub fn confirm_expired_for_context(
        &self,
        ticket_id: AdmissionTicketId,
        context: QueryContextRef,
        now: MonotonicInstant,
    ) -> bool {
        let mut state = self.state.lock().expect(AUTHORITY_LOCK);
        self.expire_locked(&mut state, now);
        self.reap_locked(&mut state, now);
        state.tickets.get(&ticket_id).is_some_and(|record| {
            record.receipt.context() == context && record.state == AdmissionTicketState::Expired
        })
    }

    pub fn issued_count(&self, now: MonotonicInstant) -> usize {
        let mut state = self.state.lock().expect(AUTHORITY_LOCK);
        self.expire_locked(&mut state, now);
        self.reap_locked(&mut state, now);
        state.issued
    }

    /// Returns capacity held by issued and redeemed tickets.
    pub fn reserved_count(&self, now: MonotonicInstant) -> usize {
        let mut state = self.state.lock().expect(AUTHORITY_LOCK);
        self.expire_locked(&mut state, now);
        self.reap_locked(&mut state, now);
        state.reserved
    }

    fn expire_locked(&self, state: &mut AdmissionTicketStateOwner, now: MonotonicInstant) -> usize {
        let expired = state
            .tickets
            .iter()
            .filter_map(|(ticket_id, record)| {
                (record.state == AdmissionTicketState::Issued && now.has_reached(record.expires_at))
                    .then_some((*ticket_id, record.receipt.context()))
            })
            .collect::<Vec<_>>();
        for (ticket_id, _) in &expired {
            state.issued = state.issued.saturating_sub(1);
            state.reserved = state.reserved.saturating_sub(1);
            let record = state
                .tickets
                .get_mut(ticket_id)
                .expect("collected ticket remains present");
            record.state = AdmissionTicketState::Expired;
            record.terminal_at = Some(now);
            let operation_id = record.operation_id;
            state.terminal_order.push_back(*ticket_id);
            self.mark_acquisition_terminal_locked(state, operation_id, now);
        }
        if !expired.is_empty() {
            self.reservation_observation.publish(state.reserved);
        }
        expired.len()
    }

    fn mark_acquisition_terminal_locked(
        &self,
        state: &mut AdmissionTicketStateOwner,
        operation_id: TaskOperationId,
        now: MonotonicInstant,
    ) {
        let record = state
            .acquisitions
            .get_mut(&operation_id)
            .expect("a retained ticket names its acquisition decision");
        if record.terminal_at.replace(now).is_none() {
            state.terminal_acquisition_order.push_back(operation_id);
        }
    }

    fn reap_locked(&self, state: &mut AdmissionTicketStateOwner, now: MonotonicInstant) {
        while let Some(ticket_id) = state.terminal_order.front().copied() {
            let Some(record) = state.tickets.get(&ticket_id) else {
                state.terminal_order.pop_front();
                continue;
            };
            let Some(terminal_at) = record.terminal_at else {
                state.terminal_order.pop_front();
                continue;
            };
            if !now.has_reached(terminal_at.saturating_add(self.config.replay_retention())) {
                break;
            }
            state.terminal_order.pop_front();
            state.tickets.remove(&ticket_id);
        }
        while let Some(operation_id) = state.terminal_acquisition_order.front().copied() {
            let Some(record) = state.acquisitions.get(&operation_id) else {
                state.terminal_acquisition_order.pop_front();
                continue;
            };
            let Some(terminal_at) = record.terminal_at else {
                state.terminal_acquisition_order.pop_front();
                continue;
            };
            if !now.has_reached(terminal_at.saturating_add(self.config.replay_retention())) {
                break;
            }
            state.terminal_acquisition_order.pop_front();
            if let Some(record) = state.acquisitions.remove(&operation_id) {
                // The frontier is where this ledger's memory now ends. Records
                // leave in the order they reached terminal, which is not the
                // order they were stamped in, so it takes the highest it has
                // seen rather than the last one out.
                state.reclaim_frontier = state.reclaim_frontier.max(record.stamp);
            }
        }
    }
}

/// Hands out the next ledger stamp.
///
/// It saturates rather than wrapping: a wrapped stamp would fall back below
/// the reclaim frontier and start refusing everything, which is a far worse
/// answer than a worker that has admitted 2^64 operations refusing to
/// distinguish its last few.
fn next_stamp_locked(state: &mut AdmissionTicketStateOwner) -> u64 {
    let stamp = state.next_stamp;
    state.next_stamp = state.next_stamp.saturating_add(1);
    stamp
}

fn mint_admission_issuer() -> [u8; 8] {
    loop {
        let bytes = Uuid::new_v4().into_bytes();
        let mut issuer = [0u8; 8];
        issuer.copy_from_slice(&bytes[..8]);
        if issuer != [0; 8] {
            return issuer;
        }
    }
}

impl Default for AdmissionTicketAuthority {
    fn default() -> Self {
        Self::new(AdmissionTicketConfig::DEFAULT)
    }
}

#[cfg(test)]
mod tests {
    use super::{
        AdmissionTicketAcquisitionRejection, AdmissionTicketAuthority, AdmissionTicketConfig,
        AdmissionTicketObservation, AdmissionTicketProgression, AdmissionTicketRedemption,
        AdmissionTicketRedemptionRejection,
    };
    use crate::{MonotonicInstant, RequestHorizon};
    use novarocks_execution_contract::{
        AcquireQueryContextAdmissionTicket, LeaseValidFor, QueryContextRef, TaskOperationId,
    };
    use novarocks_types::{
        NativeCompatibilityId,
        identity::{AttemptId, BackendProcessId, FrontendProcessId, QueryExecutionId, QueryId},
    };
    use std::{sync::Arc, time::Duration};

    fn at(seconds: u64) -> MonotonicInstant {
        MonotonicInstant::from_origin(Duration::from_secs(seconds))
    }

    fn context(seed: i64) -> QueryContextRef {
        QueryContextRef::new(
            QueryExecutionId::new(
                QueryId::new(seed, seed + 1),
                AttemptId::new(1).expect("nonzero attempt"),
            )
            .expect("nonzero query id"),
            FrontendProcessId::new_v7(),
            BackendProcessId::new_v7(),
        )
    }

    fn request(
        authority: &AdmissionTicketAuthority,
        operation_id: TaskOperationId,
        context: QueryContextRef,
        seconds: u64,
    ) -> AcquireQueryContextAdmissionTicket {
        AcquireQueryContextAdmissionTicket::new(
            operation_id,
            context,
            LeaseValidFor::new(Duration::from_secs(seconds)).expect("representable validity"),
            NativeCompatibilityId::new([0x41; 32]),
            authority.current_epoch(at(0)),
        )
    }

    fn small_authority() -> AdmissionTicketAuthority {
        AdmissionTicketAuthority::new(
            AdmissionTicketConfig::new(2, Duration::from_secs(10), Duration::from_secs(2), 4)
                .expect("legal test bounds"),
        )
    }

    #[test]
    fn ticket_observation_is_context_bound_and_does_not_redeem_or_extend() {
        let authority = small_authority();
        let owner = context(1);
        let foreign = context(2);
        let first = authority
            .acquire(
                request(&authority, TaskOperationId::new_v7(), owner, 5),
                at(0),
            )
            .expect("issued");
        let ticket_id = first.receipt().ticket_id();

        assert_eq!(
            authority.observe(ticket_id, owner, at(2)),
            AdmissionTicketObservation::Issued {
                remaining: Duration::from_secs(3),
            }
        );
        assert_eq!(
            authority.observe(ticket_id, foreign, at(2)),
            AdmissionTicketObservation::ForeignContext
        );
        assert_eq!(
            authority.observe(ticket_id, owner, at(5)),
            AdmissionTicketObservation::Expired,
            "observation sees the deadline without mutating authority state"
        );
        assert_eq!(
            authority.redeem(ticket_id, owner, at(5)),
            Err(AdmissionTicketRedemptionRejection::Expired)
        );

        let second = authority
            .acquire(
                request(&authority, TaskOperationId::new_v7(), owner, 5),
                at(5),
            )
            .expect("replacement ticket");
        let second_id = second.receipt().ticket_id();
        assert_eq!(
            authority.redeem(second_id, owner, at(6)),
            Ok(AdmissionTicketRedemption::Redeemed(second.receipt()))
        );
        assert_eq!(
            authority.observe(second_id, owner, at(6)),
            AdmissionTicketObservation::Redeemed
        );
        authority.release_context(owner, at(7));
        assert_eq!(
            authority.observe(second_id, owner, at(7)),
            AdmissionTicketObservation::Closed
        );
        authority.advance_deadlines(at(10));
        assert_eq!(
            authority.observe(second_id, owner, at(10)),
            AdmissionTicketObservation::Unknown,
            "terminal observations are bounded by replay retention"
        );
    }

    #[test]
    fn expired_confirmation_cannot_steal_a_redeemed_ticket() {
        let authority = small_authority();
        let owner = context(1);
        let ticket_id = authority
            .acquire(
                request(&authority, TaskOperationId::new_v7(), owner, 5),
                at(0),
            )
            .expect("issued")
            .receipt()
            .ticket_id();
        assert!(!authority.confirm_expired_for_context(ticket_id, owner, at(4)));
        assert!(!authority.confirm_expired_for_context(ticket_id, context(2), at(5)));
        assert!(authority.confirm_expired_for_context(ticket_id, owner, at(5)));

        let replay_ticket = authority
            .acquire(
                request(&authority, TaskOperationId::new_v7(), owner, 5),
                at(5),
            )
            .expect("new issuance")
            .receipt()
            .ticket_id();
        authority
            .redeem(replay_ticket, owner, at(5))
            .expect("redeemed before expiration");
        assert!(!authority.confirm_expired_for_context(replay_ticket, owner, at(20)));
    }

    #[test]
    fn exact_acquisition_replay_returns_the_same_ticket_without_extending_it() {
        let authority = small_authority();
        let observed = authority.reservation_observation();
        let request = request(&authority, TaskOperationId::new_v7(), context(1), 5);
        let first = authority.acquire(request, at(0)).expect("issued");
        assert_eq!(observed.used(), 1);
        assert_eq!(first.progression(), AdmissionTicketProgression::Issued);
        let replay = authority.acquire(request, at(3)).expect("replayed");
        assert_eq!(replay.progression(), AdmissionTicketProgression::Replayed);
        assert_eq!(replay.receipt(), first.receipt());

        assert_eq!(authority.advance_deadlines(at(5)), 1);
        assert_eq!(observed.used(), 0);
        let expired_replay = authority
            .acquire(request, at(5))
            .expect("the acquisition decision remains an exact replay");
        assert_eq!(
            expired_replay.progression(),
            AdmissionTicketProgression::Replayed
        );
        assert_eq!(expired_replay.receipt(), first.receipt());
        assert_eq!(
            authority.redeem(first.receipt().ticket_id(), request.context(), at(5)),
            Err(AdmissionTicketRedemptionRejection::Expired),
            "replaying the grant must not revive the expired ticket"
        );
        assert_eq!(authority.issued_count(at(5)), 0);
    }

    #[test]
    fn operation_replay_with_different_content_is_a_conflict() {
        let authority = small_authority();
        let operation_id = TaskOperationId::new_v7();
        let first = request(&authority, operation_id, context(1), 5);
        authority.acquire(first, at(0)).expect("issued");
        assert_eq!(
            authority.acquire(request(&authority, operation_id, context(2), 5), at(0)),
            Err(AdmissionTicketAcquisitionRejection::OperationReplayConflict)
        );
        assert_eq!(
            authority.acquire(request(&authority, operation_id, first.context(), 6), at(0)),
            Err(AdmissionTicketAcquisitionRejection::OperationReplayConflict)
        );
        assert_eq!(
            authority.acquire(
                AcquireQueryContextAdmissionTicket::new(
                    operation_id,
                    first.context(),
                    first.valid_for(),
                    NativeCompatibilityId::new([0x42; 32]),
                    first.admission_epoch_capability(),
                ),
                at(0),
            ),
            Err(AdmissionTicketAcquisitionRejection::OperationReplayConflict)
        );
    }

    #[test]
    fn issued_capacity_and_ticket_validity_are_hard_bounds() {
        let authority = small_authority();
        authority
            .acquire(
                request(&authority, TaskOperationId::new_v7(), context(1), 5),
                at(0),
            )
            .expect("first ticket");
        authority
            .acquire(
                request(&authority, TaskOperationId::new_v7(), context(2), 5),
                at(0),
            )
            .expect("second ticket");
        assert_eq!(
            authority.acquire(
                request(&authority, TaskOperationId::new_v7(), context(3), 5),
                at(0)
            ),
            Err(AdmissionTicketAcquisitionRejection::ReservationCapacityExhausted)
        );
        assert_eq!(
            authority.acquire(
                request(&authority, TaskOperationId::new_v7(), context(4), 11),
                at(0)
            ),
            Err(AdmissionTicketAcquisitionRejection::ValidityExceedsWorkerLimit)
        );
    }

    #[test]
    fn redeemed_tickets_hold_capacity_until_context_closure() {
        let authority = small_authority();
        let observed = authority.reservation_observation();
        assert_eq!(observed.used(), 0);
        let first_context = context(1);
        let second_context = context(2);
        let first = authority
            .acquire(
                request(&authority, TaskOperationId::new_v7(), first_context, 5),
                at(0),
            )
            .expect("first ticket");
        let second = authority
            .acquire(
                request(&authority, TaskOperationId::new_v7(), second_context, 5),
                at(0),
            )
            .expect("second ticket");
        authority
            .redeem(first.receipt().ticket_id(), first_context, at(0))
            .expect("first redemption");
        authority
            .redeem(second.receipt().ticket_id(), second_context, at(0))
            .expect("second redemption");

        assert_eq!(authority.issued_count(at(0)), 0);
        assert_eq!(authority.reserved_count(at(0)), 2);
        assert_eq!(observed.used(), 2);
        assert_eq!(
            authority.acquire(
                request(&authority, TaskOperationId::new_v7(), context(3), 5),
                at(0)
            ),
            Err(AdmissionTicketAcquisitionRejection::ReservationCapacityExhausted)
        );

        assert_eq!(authority.release_context(first_context, at(0)), 1);
        assert_eq!(authority.reserved_count(at(0)), 1);
        assert_eq!(observed.used(), 1);
        authority
            .acquire(
                request(&authority, TaskOperationId::new_v7(), context(3), 5),
                at(0),
            )
            .expect("released reservation admits another context");
        assert_eq!(observed.used(), 2);
    }

    #[test]
    fn closing_admission_does_not_release_a_redeemed_context_reservation() {
        let authority = small_authority();
        let owner = context(1);
        let grant = authority
            .acquire(
                request(&authority, TaskOperationId::new_v7(), owner, 5),
                at(0),
            )
            .expect("issued");
        let ticket_id = grant.receipt().ticket_id();
        authority.redeem(ticket_id, owner, at(0)).expect("redeemed");

        assert_eq!(authority.revoke_unredeemed(owner, at(0)), 0);
        assert_eq!(authority.reserved_count(at(0)), 1);
        assert!(matches!(
            authority.redeem(ticket_id, owner, at(0)),
            Ok(AdmissionTicketRedemption::Replayed(_))
        ));

        assert_eq!(authority.release_context(owner, at(0)), 1);
        assert_eq!(authority.reserved_count(at(0)), 0);
        assert_eq!(
            authority.redeem(ticket_id, owner, at(0)),
            Err(AdmissionTicketRedemptionRejection::Closed)
        );
    }

    #[test]
    fn redemption_is_exact_and_context_closure_revokes_every_grant() {
        let authority = small_authority();
        let owner = context(1);
        let foreign = context(2);
        let operation_id = TaskOperationId::new_v7();
        let grant = authority
            .acquire(request(&authority, operation_id, owner, 5), at(0))
            .expect("issued");
        let ticket_id = grant.receipt().ticket_id();
        assert_eq!(
            authority.redeem(ticket_id, foreign, at(0)),
            Err(AdmissionTicketRedemptionRejection::ForeignContext)
        );
        assert!(matches!(
            authority.redeem(ticket_id, owner, at(0)),
            Ok(AdmissionTicketRedemption::Redeemed(_))
        ));
        assert!(matches!(
            authority.redeem(ticket_id, owner, at(9)),
            Ok(AdmissionTicketRedemption::Replayed(_))
        ));
        assert_eq!(authority.release_context(owner, at(9)), 1);
        assert_eq!(
            authority.redeem(ticket_id, owner, at(9)),
            Err(AdmissionTicketRedemptionRejection::Closed)
        );
        assert_eq!(
            authority
                .acquire(request(&authority, operation_id, owner, 5), at(9))
                .expect("the accepted operation remains replayable")
                .receipt(),
            grant.receipt()
        );
    }

    #[test]
    fn rejected_operation_cannot_become_accepted_after_its_precondition_changes() {
        let authority = Arc::new(AdmissionTicketAuthority::new(
            AdmissionTicketConfig::new(2, Duration::from_secs(10), Duration::from_secs(20), 4)
                .expect("legal test bounds"),
        ));
        let owner = context(1);
        let active = authority
            .acquire(
                request(&authority, TaskOperationId::new_v7(), owner, 5),
                at(0),
            )
            .expect("active ticket");
        let rejected = request(&authority, TaskOperationId::new_v7(), owner, 5);
        let concurrent = (0..8)
            .map(|_| {
                let authority = Arc::clone(&authority);
                std::thread::spawn(move || authority.acquire(rejected, at(0)))
            })
            .collect::<Vec<_>>();
        for decision in concurrent {
            assert_eq!(
                decision.join().expect("acquisition thread"),
                Err(AdmissionTicketAcquisitionRejection::ContextAlreadyGranted)
            );
        }

        assert_eq!(authority.advance_deadlines(at(5)), 1);
        assert_eq!(
            authority.acquire(rejected, at(5)),
            Err(AdmissionTicketAcquisitionRejection::ContextAlreadyGranted),
            "the same operation cannot issue after the older ticket expires"
        );
        assert_eq!(
            authority.acquire(
                AcquireQueryContextAdmissionTicket::new(
                    rejected.envelope().operation_id(),
                    context(2),
                    rejected.valid_for(),
                    rejected.native_compatibility_id(),
                    rejected.admission_epoch_capability(),
                ),
                at(5),
            ),
            Err(AdmissionTicketAcquisitionRejection::OperationReplayConflict)
        );

        let replacement = authority
            .acquire(
                request(&authority, TaskOperationId::new_v7(), owner, 5),
                at(5),
            )
            .expect("a distinct operation may issue after expiry");
        assert_ne!(
            replacement.receipt().ticket_id(),
            active.receipt().ticket_id()
        );

        authority.advance_deadlines(at(20));
        assert_eq!(
            authority.acquire(rejected, at(20)),
            Err(AdmissionTicketAcquisitionRejection::SealedEpoch),
            "after bounded replay retention the old epoch must prevent reissuance"
        );
    }

    #[test]
    fn saturated_replay_ledger_fails_closed_without_eviction_or_panic() {
        let authority = AdmissionTicketAuthority::new(
            AdmissionTicketConfig::new(1, Duration::from_secs(10), Duration::from_secs(20), 2)
                .expect("legal test bounds"),
        );
        let owner = context(1);
        authority
            .acquire(
                request(&authority, TaskOperationId::new_v7(), owner, 5),
                at(0),
            )
            .expect("active ticket");
        let retained_rejection = request(&authority, TaskOperationId::new_v7(), owner, 5);
        assert_eq!(
            authority.acquire(retained_rejection, at(0)),
            Err(AdmissionTicketAcquisitionRejection::ReservationCapacityExhausted)
        );

        let saturated = request(&authority, TaskOperationId::new_v7(), context(2), 5);
        assert_eq!(
            authority.acquire(saturated, at(0)),
            Err(AdmissionTicketAcquisitionRejection::SealedEpoch)
        );
        assert_eq!(
            authority.acquire(saturated, at(1)),
            Err(AdmissionTicketAcquisitionRejection::SealedEpoch),
            "the saturated epoch rejection is stable without another ledger record"
        );
        assert_eq!(
            authority.acquire(retained_rejection, at(1)),
            Err(AdmissionTicketAcquisitionRejection::ReservationCapacityExhausted),
            "saturation must not evict a retained terminal decision early"
        );
    }

    /// An operation the ledger has never seen is admitted only if it was named
    /// after everything the ledger has forgotten.
    ///
    /// A capability from before the reclaim frontier could be replaying a
    /// decision that is no longer here, and admitting it as new would decide
    /// the same operation twice. One from after it cannot: every record it
    /// could be replaying is still present, and the replay branch would
    /// already have found it.
    #[test]
    fn an_unknown_operation_is_admitted_only_from_above_the_reclaim_frontier() {
        let authority = AdmissionTicketAuthority::new(
            AdmissionTicketConfig::new(1, Duration::from_secs(10), Duration::from_secs(2), 2)
                .expect("legal test bounds"),
        );
        let owner = context(1);
        let before_anything = authority.current_epoch(at(0));
        authority
            .acquire(
                request(&authority, TaskOperationId::new_v7(), owner, 5),
                at(0),
            )
            .expect("active ticket");
        let retained_rejection = request(&authority, TaskOperationId::new_v7(), context(2), 5);
        assert_eq!(
            authority.acquire(retained_rejection, at(0)),
            Err(AdmissionTicketAcquisitionRejection::ReservationCapacityExhausted)
        );

        // Releasing the owner makes both decisions terminal, and the two second
        // retention has already elapsed, so the reclaim takes them both and the
        // frontier passes every stamp issued so far.
        assert_eq!(authority.release_context(owner, at(2)), 1);

        let from_before_the_reclaim = AcquireQueryContextAdmissionTicket::new(
            TaskOperationId::new_v7(),
            context(3),
            LeaseValidFor::new(Duration::from_secs(5)).expect("representable validity"),
            NativeCompatibilityId::new([0x41; 32]),
            before_anything,
        );
        assert_eq!(
            authority.acquire(from_before_the_reclaim, at(2)),
            Err(AdmissionTicketAcquisitionRejection::SealedEpoch),
            "a capability older than what the ledger forgot cannot name a new operation"
        );

        let current = AcquireQueryContextAdmissionTicket::new(
            TaskOperationId::new_v7(),
            context(3),
            LeaseValidFor::new(Duration::from_secs(5)).expect("representable validity"),
            NativeCompatibilityId::new([0x41; 32]),
            authority.current_epoch(at(2)),
        );
        assert!(
            authority.acquire(current, at(2)).is_ok(),
            "a capability from after the reclaim can use the freed slot"
        );
    }

    /// The point of the whole change: a worker that is reclaiming continuously
    /// stays usable by a frontend that only learns its capability from
    /// heartbeats.
    ///
    /// Under a capability that any reclaim invalidated, the value a heartbeat
    /// carried was already stale when it arrived, and no refresh rate could
    /// fix it -- the worker reclaims on a 100ms sweep and the heartbeat is
    /// 500ms or slower. Here the frontier trails the published stamp by the
    /// whole retention window instead, so the heartbeat's copy clears it.
    #[test]
    fn a_capability_learned_one_heartbeat_ago_survives_continuous_reclaiming() {
        let authority = AdmissionTicketAuthority::new(
            AdmissionTicketConfig::new(64, Duration::from_secs(10), Duration::from_secs(2), 512)
                .expect("legal test bounds"),
        );
        // Fill the ledger with work that will age out, the way a busy worker
        // accumulates it.
        for index in 0..32 {
            let owner = context(index + 10);
            authority
                .acquire(
                    request(&authority, TaskOperationId::new_v7(), owner, 1),
                    at(0),
                )
                .expect("early ticket");
            authority.release_context(owner, at(0));
        }

        // A heartbeat at t=5 hands the frontend this. Between then and the
        // request the worker keeps sweeping, and every sweep reclaims more.
        let learned_at_heartbeat = authority.current_epoch(at(5));
        for tick in 5..20 {
            authority.advance_deadlines(at(tick));
        }

        let request_after_the_sweeps = AcquireQueryContextAdmissionTicket::new(
            TaskOperationId::new_v7(),
            context(1),
            LeaseValidFor::new(Duration::from_secs(5)).expect("representable validity"),
            NativeCompatibilityId::new([0x41; 32]),
            learned_at_heartbeat,
        );
        assert!(
            authority.acquire(request_after_the_sweeps, at(20)).is_ok(),
            "a capability one heartbeat old must still admit new work"
        );
    }

    #[test]
    fn capacity_rejection_replays_after_capacity_is_released() {
        let authority = small_authority();
        let first_context = context(1);
        let first = authority
            .acquire(
                request(&authority, TaskOperationId::new_v7(), first_context, 5),
                at(0),
            )
            .expect("first ticket");
        authority
            .acquire(
                request(&authority, TaskOperationId::new_v7(), context(2), 5),
                at(0),
            )
            .expect("second ticket");
        let rejected = request(&authority, TaskOperationId::new_v7(), context(3), 5);
        assert_eq!(
            authority.acquire(rejected, at(0)),
            Err(AdmissionTicketAcquisitionRejection::ReservationCapacityExhausted)
        );

        assert_eq!(authority.release_context(first_context, at(1)), 1);
        assert_eq!(
            authority.acquire(rejected, at(1)),
            Err(AdmissionTicketAcquisitionRejection::ReservationCapacityExhausted),
            "the first no-grant decision remains final for this operation"
        );
        authority
            .acquire(
                request(&authority, TaskOperationId::new_v7(), context(3), 5),
                at(1),
            )
            .expect("a new operation may consume released capacity");
        assert_eq!(
            authority.redeem(first.receipt().ticket_id(), first_context, at(1)),
            Err(AdmissionTicketRedemptionRejection::Closed)
        );
    }

    #[test]
    fn reclaimed_acquisition_history_seals_its_epoch_and_old_acquire_cannot_reissue() {
        let authority = small_authority();
        let owner = context(1);
        let old_request = request(&authority, TaskOperationId::new_v7(), owner, 1);
        let grant = authority.acquire(old_request, at(0)).expect("issued");
        let ticket_id = grant.receipt().ticket_id();
        assert_eq!(authority.advance_deadlines(at(1)), 1);
        authority.advance_deadlines(at(3));
        assert_eq!(authority.state(ticket_id, at(3)), None);
        assert_eq!(
            authority.redeem(ticket_id, owner, at(3)),
            Err(AdmissionTicketRedemptionRejection::Unknown)
        );

        assert_eq!(
            authority.acquire(old_request, at(3)),
            Err(AdmissionTicketAcquisitionRejection::SealedEpoch),
            "reclaimed history must not turn old acquire bytes into a new issuance"
        );

        let replacement_request = request(&authority, TaskOperationId::new_v7(), owner, 1);
        let replacement = authority
            .acquire(replacement_request, at(3))
            .expect("a fresh operation on the current epoch may acquire");
        assert_ne!(replacement.receipt().ticket_id(), ticket_id);
    }

    #[test]
    fn sealed_epoch_allows_only_retained_exact_replay() {
        let authority = small_authority();
        let expiring = request(&authority, TaskOperationId::new_v7(), context(1), 1);
        let retained = request(&authority, TaskOperationId::new_v7(), context(2), 10);
        authority.acquire(expiring, at(0)).expect("expiring ticket");
        let original = authority.acquire(retained, at(0)).expect("retained ticket");

        authority.advance_deadlines(at(1));
        authority.advance_deadlines(at(3));
        assert_ne!(
            authority.current_epoch(at(3)),
            retained.admission_epoch_capability(),
            "reclaiming any decision seals the issuance epoch"
        );
        let replay = authority
            .acquire(retained, at(3))
            .expect("retained exact replay remains available in a sealed epoch");
        assert_eq!(replay.progression(), AdmissionTicketProgression::Replayed);
        assert_eq!(replay.receipt(), original.receipt());

        let stale_new_operation = AcquireQueryContextAdmissionTicket::new(
            TaskOperationId::new_v7(),
            context(3),
            retained.valid_for(),
            retained.native_compatibility_id(),
            retained.admission_epoch_capability(),
        );
        assert_eq!(
            authority.acquire(stale_new_operation, at(3)),
            Err(AdmissionTicketAcquisitionRejection::SealedEpoch)
        );
    }

    #[test]
    fn process_replacement_has_a_new_epoch_and_rejects_predecessor_acquire() {
        let predecessor = small_authority();
        let old_request = request(&predecessor, TaskOperationId::new_v7(), context(1), 5);
        let replacement = small_authority();

        assert_ne!(
            predecessor.current_epoch(at(0)),
            replacement.current_epoch(at(0)),
            "independent worker processes must mint independent capabilities"
        );
        assert_eq!(
            replacement.acquire(old_request, at(0)),
            Err(AdmissionTicketAcquisitionRejection::SealedEpoch)
        );
        replacement
            .acquire(
                request(&replacement, TaskOperationId::new_v7(), context(2), 5),
                at(0),
            )
            .expect("replacement process accepts its own current epoch");
    }

    #[test]
    fn the_default_1024_limit_counts_redeemed_live_reservations() {
        let authority = AdmissionTicketAuthority::default();
        for seed in 1..=super::MAX_ADMISSION_RESERVATIONS {
            let owner = context(seed as i64);
            let grant = authority
                .acquire(
                    request(&authority, TaskOperationId::new_v7(), owner, 10),
                    at(0),
                )
                .expect("reservation below the product limit");
            authority
                .redeem(grant.receipt().ticket_id(), owner, at(0))
                .expect("redemption retains the reservation");
        }
        assert_eq!(
            authority.acquire(
                request(&authority, TaskOperationId::new_v7(), context(2_000), 10),
                at(0),
            ),
            Err(AdmissionTicketAcquisitionRejection::ReservationCapacityExhausted)
        );
    }

    #[test]
    fn default_bounds_are_the_product_contract() {
        assert_eq!(
            AdmissionTicketConfig::DEFAULT.max_reservations(),
            super::MAX_ADMISSION_RESERVATIONS
        );
        assert_eq!(
            AdmissionTicketConfig::DEFAULT.max_valid_for(),
            Duration::from_secs(10)
        );
        assert_eq!(RequestHorizon::DEFAULT.total(), Duration::from_secs(120));
    }
}
