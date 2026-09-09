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
use std::sync::Mutex;
use std::time::Duration;

use novarocks_execution_contract::{
    AcquireQueryContextAdmissionTicket, AdmissionEpochCapability, AdmissionTicketId,
    QueryContextAdmissionTicketReceipt, QueryContextRef, TaskOperationId,
};
use novarocks_types::NativeCompatibilityId;
use uuid::Uuid;

use crate::MonotonicInstant;

const AUTHORITY_LOCK: &str = "admission ticket authority lock";

/// Maximum number of query-context capacity reservations one worker owns.
pub const MAX_ADMISSION_RESERVATIONS: usize = 1024;

/// Maximum lifetime an unredeemed grant may request.
pub const MAX_ADMISSION_TICKET_VALID_FOR: Duration = Duration::from_secs(10);

/// Worker-local bounds for admission grants and their replay records.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub struct AdmissionTicketConfig {
    max_reservations: usize,
    max_valid_for: Duration,
    replay_retention: Duration,
    max_records: usize,
}

impl AdmissionTicketConfig {
    pub const DEFAULT: Self = Self {
        max_reservations: MAX_ADMISSION_RESERVATIONS,
        max_valid_for: MAX_ADMISSION_TICKET_VALID_FOR,
        replay_retention: Duration::from_secs(120),
        max_records: MAX_ADMISSION_RESERVATIONS * 8,
    };

    pub fn new(
        max_reservations: usize,
        max_valid_for: Duration,
        replay_retention: Duration,
        max_records: usize,
    ) -> Option<Self> {
        if max_reservations == 0
            || max_valid_for.is_zero()
            || replay_retention.is_zero()
            || max_records < max_reservations
        {
            return None;
        }
        Some(Self {
            max_reservations,
            max_valid_for,
            replay_retention,
            max_records,
        })
    }

    pub const fn max_reservations(self) -> usize {
        self.max_reservations
    }

    pub const fn max_valid_for(self) -> Duration {
        self.max_valid_for
    }
}

impl Default for AdmissionTicketConfig {
    fn default() -> Self {
        Self::DEFAULT
    }
}

/// The single owner's state for one ticket nonce.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub enum AdmissionTicketState {
    Issued,
    Redeemed,
    Closed,
    Expired,
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
    admission_epoch_capability: AdmissionEpochCapability,
    native_compatibility_id: NativeCompatibilityId,
    receipt: QueryContextAdmissionTicketReceipt,
    expires_at: MonotonicInstant,
    state: AdmissionTicketState,
    terminal_at: Option<MonotonicInstant>,
}

struct AdmissionTicketStateOwner {
    current_epoch: AdmissionEpochCapability,
    tickets: BTreeMap<AdmissionTicketId, TicketRecord>,
    ticket_by_operation: BTreeMap<TaskOperationId, AdmissionTicketId>,
    terminal_order: VecDeque<AdmissionTicketId>,
    issued: usize,
    reserved: usize,
}

impl AdmissionTicketStateOwner {
    fn new() -> Self {
        Self {
            current_epoch: mint_admission_epoch_capability(),
            tickets: BTreeMap::new(),
            ticket_by_operation: BTreeMap::new(),
            terminal_order: VecDeque::new(),
            issued: 0,
            reserved: 0,
        }
    }
}

/// The only worker-local owner allowed to mint, redeem, and close grants.
pub struct AdmissionTicketAuthority {
    config: AdmissionTicketConfig,
    state: Mutex<AdmissionTicketStateOwner>,
}

impl AdmissionTicketAuthority {
    pub fn new(config: AdmissionTicketConfig) -> Self {
        Self {
            config,
            state: Mutex::new(AdmissionTicketStateOwner::new()),
        }
    }

    pub const fn config(&self) -> AdmissionTicketConfig {
        self.config
    }

    /// Returns the capability that may authorize new acquisitions now.
    ///
    /// Advancing retention here ensures a heartbeat never republishes an
    /// epoch after reclaiming one of its acquisition decisions made that
    /// epoch unsafe for unknown operations.
    pub fn current_epoch(&self, now: MonotonicInstant) -> AdmissionEpochCapability {
        let mut state = self.state.lock().expect(AUTHORITY_LOCK);
        self.expire_locked(&mut state, now);
        self.reap_locked(&mut state, now);
        state.current_epoch
    }

    /// Issues or exactly replays one immutable acquisition request.
    pub fn acquire(
        &self,
        request: AcquireQueryContextAdmissionTicket,
        now: MonotonicInstant,
    ) -> Result<AdmissionTicketGrant, AdmissionTicketAcquisitionRejection> {
        if request.valid_for().get() > self.config.max_valid_for {
            return Err(AdmissionTicketAcquisitionRejection::ValidityExceedsWorkerLimit);
        }

        let mut state = self.state.lock().expect(AUTHORITY_LOCK);
        self.expire_locked(&mut state, now);
        self.reap_locked(&mut state, now);

        let operation_id = request.envelope().operation_id();
        if let Some(ticket_id) = state.ticket_by_operation.get(&operation_id).copied() {
            let record = state
                .tickets
                .get(&ticket_id)
                .expect("operation index names a retained ticket");
            if record.receipt.context() != request.context()
                || record.receipt.valid_for() != request.valid_for()
                || record.native_compatibility_id != request.native_compatibility_id()
                || record.admission_epoch_capability != request.admission_epoch_capability()
            {
                return Err(AdmissionTicketAcquisitionRejection::OperationReplayConflict);
            }
            return match record.state {
                AdmissionTicketState::Issued | AdmissionTicketState::Redeemed => Ok(
                    AdmissionTicketGrant::new(AdmissionTicketProgression::Replayed, record.receipt),
                ),
                state @ (AdmissionTicketState::Closed | AdmissionTicketState::Expired) => {
                    Err(AdmissionTicketAcquisitionRejection::Inactive(state))
                }
            };
        }

        if request.admission_epoch_capability() != state.current_epoch {
            return Err(AdmissionTicketAcquisitionRejection::SealedEpoch);
        }

        if state.reserved >= self.config.max_reservations {
            return Err(AdmissionTicketAcquisitionRejection::ReservationCapacityExhausted);
        }
        if state.tickets.values().any(|record| {
            record.receipt.context() == request.context()
                && matches!(
                    record.state,
                    AdmissionTicketState::Issued | AdmissionTicketState::Redeemed
                )
        }) {
            return Err(AdmissionTicketAcquisitionRejection::ContextAlreadyGranted);
        }
        if state.tickets.len() >= self.config.max_records {
            return Err(AdmissionTicketAcquisitionRejection::ReplayCapacityExhausted);
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
                admission_epoch_capability: request.admission_epoch_capability(),
                native_compatibility_id: request.native_compatibility_id(),
                receipt,
                expires_at: now.saturating_add(request.valid_for().get()),
                state: AdmissionTicketState::Issued,
                terminal_at: None,
            },
        );
        state.ticket_by_operation.insert(operation_id, ticket_id);
        state.issued += 1;
        state.reserved += 1;
        Ok(AdmissionTicketGrant::new(
            AdmissionTicketProgression::Issued,
            receipt,
        ))
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
            state.terminal_order.push_back(*ticket_id);
        }
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
            state.terminal_order.push_back(*ticket_id);
        }
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
            state.terminal_order.push_back(*ticket_id);
        }
        expired.len()
    }

    fn reap_locked(&self, state: &mut AdmissionTicketStateOwner, now: MonotonicInstant) {
        let mut reclaimed = false;
        while let Some(ticket_id) = state.terminal_order.front().copied() {
            let Some(record) = state.tickets.get(&ticket_id) else {
                state.terminal_order.pop_front();
                continue;
            };
            let Some(terminal_at) = record.terminal_at else {
                state.terminal_order.pop_front();
                continue;
            };
            if !now.has_reached(terminal_at.saturating_add(self.config.replay_retention)) {
                break;
            }
            let operation_id = record.operation_id;
            state.terminal_order.pop_front();
            state.tickets.remove(&ticket_id);
            state.ticket_by_operation.remove(&operation_id);
            reclaimed = true;
        }
        if reclaimed {
            state.current_epoch = mint_admission_epoch_capability();
        }
    }
}

fn mint_admission_epoch_capability() -> AdmissionEpochCapability {
    AdmissionEpochCapability::try_from_bytes(Uuid::new_v4().into_bytes())
        .expect("UUIDv4 is a nonzero 16-byte admission epoch capability")
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
        AdmissionTicketProgression, AdmissionTicketRedemption, AdmissionTicketRedemptionRejection,
        AdmissionTicketState,
    };
    use crate::{MonotonicInstant, RequestHorizon};
    use novarocks_execution_contract::{
        AcquireQueryContextAdmissionTicket, LeaseValidFor, QueryContextRef, TaskOperationId,
    };
    use novarocks_types::{
        NativeCompatibilityId,
        identity::{AttemptId, BackendProcessId, FrontendProcessId, QueryExecutionId, QueryId},
    };
    use std::time::Duration;

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
    fn exact_acquisition_replay_returns_the_same_ticket_without_extending_it() {
        let authority = small_authority();
        let request = request(&authority, TaskOperationId::new_v7(), context(1), 5);
        let first = authority.acquire(request, at(0)).expect("issued");
        assert_eq!(first.progression(), AdmissionTicketProgression::Issued);
        let replay = authority.acquire(request, at(3)).expect("replayed");
        assert_eq!(replay.progression(), AdmissionTicketProgression::Replayed);
        assert_eq!(replay.receipt(), first.receipt());

        assert_eq!(authority.advance_deadlines(at(5)), 1);
        assert_eq!(
            authority.acquire(request, at(5)),
            Err(AdmissionTicketAcquisitionRejection::Inactive(
                AdmissionTicketState::Expired
            ))
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
        assert_eq!(
            authority.acquire(
                request(&authority, TaskOperationId::new_v7(), context(3), 5),
                at(0)
            ),
            Err(AdmissionTicketAcquisitionRejection::ReservationCapacityExhausted)
        );

        assert_eq!(authority.release_context(first_context, at(0)), 1);
        assert_eq!(authority.reserved_count(at(0)), 1);
        authority
            .acquire(
                request(&authority, TaskOperationId::new_v7(), context(3), 5),
                at(0),
            )
            .expect("released reservation admits another context");
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
            authority.acquire(request(&authority, operation_id, owner, 5), at(9)),
            Err(AdmissionTicketAcquisitionRejection::Inactive(
                AdmissionTicketState::Closed
            ))
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
