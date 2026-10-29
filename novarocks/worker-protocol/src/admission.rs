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

//! Pure ticket policy. IDs and the issuer come from the driver; capacity
//! transactions return scope-visible reservations, never asynchronous permits.
use crate::{
    creation::{AdmissionNeed, AdmissionPort, AdmissionRefusal},
    lease::MonotonicInstant,
};
use novarocks_control_runtime_core::identity::ReservationId;
use novarocks_execution_contract::{
    AcquireQueryContextAdmissionTicket, AdmissionEpochCapability, AdmissionTicketId, LeaseValidFor,
    OperationOutcome, QueryContextAdmissionTicketReceipt, QueryContextRef, TaskOperationId,
};
use novarocks_types::NativeCompatibilityId;
use std::collections::{BTreeMap, BTreeSet};
use std::{fmt, time::Duration};
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
    pub const fn replay_retention(self) -> Duration {
        self.replay_retention
    }
    pub const fn max_records(self) -> usize {
        self.max_records
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
#[derive(Clone, Debug, Eq, PartialEq)]
#[expect(
    clippy::large_enum_variant,
    reason = "The exact bounded domain refusal stays inline in its prepaid ticket record; capacity denial does not allocate another box."
)]
pub enum AdmissionTicketAcquisitionRejection {
    ValidityExceedsWorkerLimit,
    ReservationCapacityExhausted,
    ReservationRefused(AdmissionRefusal),
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
            Self::ReservationRefused(_) => "worker admission reservation was refused",
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

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct AcquisitionIdentity {
    context: QueryContextRef,
    valid_for: LeaseValidFor,
    compatibility: NativeCompatibilityId,
}
impl From<AcquireQueryContextAdmissionTicket> for AcquisitionIdentity {
    fn from(r: AcquireQueryContextAdmissionTicket) -> Self {
        Self {
            context: r.context(),
            valid_for: r.valid_for(),
            compatibility: r.native_compatibility_id(),
        }
    }
}
#[derive(Clone, Copy, Debug)]
struct TicketRecord {
    operation: TaskOperationId,
    receipt: QueryContextAdmissionTicketReceipt,
    state: AdmissionTicketState,
    reservation: ReservationId,
    expires_at: MonotonicInstant,
}
#[derive(Clone, Debug)]
struct Decision {
    identity: AcquisitionIdentity,
    result: Result<QueryContextAdmissionTicketReceipt, AdmissionTicketAcquisitionRejection>,
    stamp: u64,
}
#[derive(Debug)]
pub struct AdmissionCore {
    config: AdmissionTicketConfig,
    issuer: [u8; 8],
    next_stamp: u64,
    process_draining: bool,
    reclaim_frontier: u64,
    tickets: BTreeMap<AdmissionTicketId, TicketRecord>,
    contexts: BTreeMap<QueryContextRef, AdmissionTicketId>,
    decisions: BTreeMap<TaskOperationId, Decision>,
    expiries: BTreeSet<(MonotonicInstant, AdmissionTicketId)>,
    retention: BTreeSet<(MonotonicInstant, TaskOperationId)>,
}
impl AdmissionCore {
    pub fn new(config: AdmissionTicketConfig, issuer: [u8; 8]) -> Self {
        assert!(issuer != [0; 8], "admission issuer is nonzero");
        Self {
            config,
            issuer,
            next_stamp: 1,
            process_draining: false,
            reclaim_frontier: 0,
            tickets: BTreeMap::new(),
            contexts: BTreeMap::new(),
            decisions: BTreeMap::new(),
            expiries: BTreeSet::new(),
            retention: BTreeSet::new(),
        }
    }
    pub fn capability(&self) -> AdmissionEpochCapability {
        AdmissionEpochCapability::try_from_issuer_and_stamp(self.issuer, self.next_stamp)
            .expect("nonzero issuer and stamp")
    }
    /// Read-only replay existence check used before a new wait deadline may
    /// claim that this operation never acquired ownership.
    pub fn has_decision(&self, operation: TaskOperationId) -> bool {
        self.decisions.contains_key(&operation)
    }
    pub fn acquire(
        &mut self,
        request: AcquireQueryContextAdmissionTicket,
        ticket: AdmissionTicketId,
        now: MonotonicInstant,
        capacity: &mut impl AdmissionPort,
    ) -> Result<AdmissionTicketGrant, AdmissionTicketAcquisitionRejection> {
        let operation = request.envelope().operation_id();
        let identity = AcquisitionIdentity::from(request);
        if let Some(d) = self.decisions.get(&operation) {
            if d.identity != identity {
                return Err(AdmissionTicketAcquisitionRejection::OperationReplayConflict);
            }
            return match d.result.clone() {
                Err(e) => Err(e),
                Ok(receipt) => match self.tickets.get(&receipt.ticket_id()).map(|r| r.state) {
                    Some(AdmissionTicketState::Issued)
                        if self.tickets[&receipt.ticket_id()].expires_at <= now =>
                    {
                        Err(AdmissionTicketAcquisitionRejection::Inactive(
                            AdmissionTicketState::Expired,
                        ))
                    }
                    Some(AdmissionTicketState::Issued | AdmissionTicketState::Redeemed) => Ok(
                        AdmissionTicketGrant::new(AdmissionTicketProgression::Replayed, receipt),
                    ),
                    Some(state) => Err(AdmissionTicketAcquisitionRejection::Inactive(state)),
                    None => Err(AdmissionTicketAcquisitionRejection::SealedEpoch),
                },
            };
        }
        if self.process_draining {
            return Err(AdmissionTicketAcquisitionRejection::SealedEpoch);
        }
        let capability = request.admission_epoch_capability();
        if capability.issuer() != self.issuer
            || capability.stamp() <= self.reclaim_frontier
            || self.decisions.len() >= self.config.max_records
        {
            return Err(AdmissionTicketAcquisitionRejection::SealedEpoch);
        }
        if self.tickets.contains_key(&ticket) {
            return Err(AdmissionTicketAcquisitionRejection::OperationReplayConflict);
        }
        let reject = if request.valid_for().get() > self.config.max_valid_for {
            Some(AdmissionTicketAcquisitionRejection::ValidityExceedsWorkerLimit)
        } else if self.contexts.contains_key(&request.context()) {
            Some(AdmissionTicketAcquisitionRejection::ContextAlreadyGranted)
        } else {
            None
        };
        if let Some(e) = reject {
            self.record(operation, identity, Err(e.clone()), Some(now));
            return Err(e);
        }
        let reservation = match capacity.try_reserve(AdmissionNeed::Ticket {
            context: request.context(),
        }) {
            Ok(r) => r,
            Err(AdmissionRefusal::Verdict(OperationOutcome::ResourceExhausted)) => {
                let e = AdmissionTicketAcquisitionRejection::ReservationCapacityExhausted;
                self.record(operation, identity, Err(e.clone()), Some(now));
                return Err(e);
            }
            Err(refusal) => {
                let e = AdmissionTicketAcquisitionRejection::ReservationRefused(refusal);
                self.record(operation, identity, Err(e.clone()), Some(now));
                return Err(e);
            }
        };
        let receipt =
            QueryContextAdmissionTicketReceipt::new(ticket, request.context(), request.valid_for());
        let expires_at = now.saturating_add(request.valid_for().get());
        self.tickets.insert(
            ticket,
            TicketRecord {
                operation,
                receipt,
                state: AdmissionTicketState::Issued,
                reservation,
                expires_at,
            },
        );
        self.contexts.insert(request.context(), ticket);
        self.expiries.insert((expires_at, ticket));
        self.record(operation, identity, Ok(receipt), None);
        Ok(AdmissionTicketGrant::new(
            AdmissionTicketProgression::Issued,
            receipt,
        ))
    }
    fn record(
        &mut self,
        op: TaskOperationId,
        identity: AcquisitionIdentity,
        result: Result<QueryContextAdmissionTicketReceipt, AdmissionTicketAcquisitionRejection>,
        terminal: Option<MonotonicInstant>,
    ) {
        let stamp = self.next_stamp;
        self.next_stamp = stamp.checked_add(1).expect("admission stamp exhausted");
        assert!(
            self.decisions
                .insert(
                    op,
                    Decision {
                        identity,
                        result,
                        stamp
                    }
                )
                .is_none(),
            "decision recorded once"
        );
        if let Some(now) = terminal {
            self.retention
                .insert((now.saturating_add(self.config.replay_retention), op));
        }
    }
    pub fn redeem(
        &mut self,
        ticket: AdmissionTicketId,
        context: QueryContextRef,
        now: MonotonicInstant,
    ) -> Result<AdmissionTicketRedemption, AdmissionTicketRedemptionRejection> {
        let r = self
            .tickets
            .get_mut(&ticket)
            .ok_or(AdmissionTicketRedemptionRejection::Unknown)?;
        if r.receipt.context() != context {
            return Err(AdmissionTicketRedemptionRejection::ForeignContext);
        }
        if self.process_draining {
            return Err(AdmissionTicketRedemptionRejection::Closed);
        }
        match r.state {
            AdmissionTicketState::Issued if now.has_reached(r.expires_at) => {
                Err(AdmissionTicketRedemptionRejection::Expired)
            }
            AdmissionTicketState::Issued => {
                self.expiries.remove(&(r.expires_at, ticket));
                r.state = AdmissionTicketState::Redeemed;
                Ok(AdmissionTicketRedemption::Redeemed(r.receipt))
            }
            AdmissionTicketState::Redeemed => Ok(AdmissionTicketRedemption::Replayed(r.receipt)),
            AdmissionTicketState::Closed => Err(AdmissionTicketRedemptionRejection::Closed),
            AdmissionTicketState::Expired => Err(AdmissionTicketRedemptionRejection::Expired),
        }
    }
    /// Closing new admission revokes an unused ticket only. A redeemed
    /// reservation remains owned by its actual context until physical release.
    /// Process drain closes new grant/election rights. Existing decisions
    /// remain observable; actual reservations retire only through their owner.
    pub(crate) fn begin_process_drain(&mut self) {
        self.process_draining = true;
    }
    pub(crate) fn has_unredeemed(&self) -> bool {
        !self.expiries.is_empty()
    }
    /// The expiry index contains only actual issued, never redeemed tickets.
    /// Revoke one original without fabricating elapsed time or scanning rows.
    pub(crate) fn revoke_next_unredeemed(
        &mut self,
        now: MonotonicInstant,
    ) -> Option<ReservationId> {
        let (_, ticket) = self.expiries.first().copied()?;
        assert_eq!(
            self.tickets
                .get(&ticket)
                .expect("indexed issued ticket")
                .state,
            AdmissionTicketState::Issued,
            "only issued tickets retain an expiry index"
        );
        self.terminal(ticket, AdmissionTicketState::Closed, now)
    }

    pub fn revoke_unredeemed(
        &mut self,
        context: QueryContextRef,
        now: MonotonicInstant,
    ) -> Option<ReservationId> {
        let ticket = *self.contexts.get(&context)?;
        if self.tickets.get(&ticket)?.state != AdmissionTicketState::Issued {
            return None;
        }
        self.terminal(ticket, AdmissionTicketState::Closed, now)
    }
    pub fn close_context(
        &mut self,
        context: QueryContextRef,
        now: MonotonicInstant,
    ) -> Option<ReservationId> {
        let ticket = self.contexts.remove(&context)?;
        self.terminal(ticket, AdmissionTicketState::Closed, now)
    }
    fn terminal(
        &mut self,
        ticket: AdmissionTicketId,
        state: AdmissionTicketState,
        now: MonotonicInstant,
    ) -> Option<ReservationId> {
        let r = self.tickets.get_mut(&ticket)?;
        if matches!(
            r.state,
            AdmissionTicketState::Closed | AdmissionTicketState::Expired
        ) {
            return None;
        }
        self.contexts.remove(&r.receipt.context());
        self.expiries.remove(&(r.expires_at, ticket));
        r.state = state;
        self.retention.insert((
            now.saturating_add(self.config.replay_retention),
            r.operation,
        ));
        Some(r.reservation)
    }
    /// A bounded cursor over indexes, never a scan of every task/context.
    /// Returned reservations are transferred to runtime cleanup, not released by
    /// this protocol view; the original holder's exit still controls capacity.
    pub fn advance(&mut self, now: MonotonicInstant, budget: usize) -> Vec<ReservationId> {
        let mut retired = Vec::new();
        let mut remaining = budget;
        while remaining > 0 {
            let Some((at, ticket)) = self.expiries.first().copied() else {
                break;
            };
            if now < at {
                break;
            }
            self.expiries.pop_first();
            remaining -= 1;
            if let Some(r) = self.terminal(ticket, AdmissionTicketState::Expired, now) {
                retired.push(r);
            }
        }
        while remaining > 0 {
            let Some((at, op)) = self.retention.first().copied() else {
                break;
            };
            if now < at {
                break;
            }
            self.retention.pop_first();
            remaining -= 1;
            if let Some(d) = self.decisions.remove(&op) {
                self.reclaim_frontier = self.reclaim_frontier.max(d.stamp);
                if let Ok(receipt) = d.result {
                    self.tickets.remove(&receipt.ticket_id());
                }
            }
        }
        retired
    }
    pub fn next_deadline(&self) -> Option<MonotonicInstant> {
        self.expiries
            .first()
            .map(|(at, _)| *at)
            .into_iter()
            .chain(self.retention.first().map(|(at, _)| *at))
            .min()
    }
    pub fn observe(
        &self,
        ticket: AdmissionTicketId,
        context: QueryContextRef,
        now: MonotonicInstant,
    ) -> AdmissionTicketObservation {
        let Some(r) = self.tickets.get(&ticket) else {
            return AdmissionTicketObservation::Unknown;
        };
        if r.receipt.context() != context {
            return AdmissionTicketObservation::ForeignContext;
        }
        match r.state {
            AdmissionTicketState::Issued if now.has_reached(r.expires_at) => {
                AdmissionTicketObservation::Expired
            }
            AdmissionTicketState::Issued => AdmissionTicketObservation::Issued {
                remaining: r.expires_at.saturating_duration_since(now),
            },
            AdmissionTicketState::Redeemed => AdmissionTicketObservation::Redeemed,
            AdmissionTicketState::Closed => AdmissionTicketObservation::Closed,
            AdmissionTicketState::Expired => AdmissionTicketObservation::Expired,
        }
    }
}
