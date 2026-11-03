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

//! Pure ticket metadata policy. IDs and the issuer come from the driver;
//! grants confer protocol rights without reserving execution capacity.
use crate::lease::MonotonicInstant;
use novarocks_execution_contract::{
    AcquireParticipationAdmissionTicket, AdmissionEpochCapability, AdmissionTicketId,
    LeaseValidFor, ParticipationAdmissionTicketReceipt, ParticipationRef, TaskOperationId,
};
use novarocks_types::NativeCompatibilityId;
use std::collections::{BTreeMap, BTreeSet};
use std::{fmt, time::Duration};
/// Maximum lifetime an unredeemed grant may request.
pub const MAX_ADMISSION_TICKET_VALID_FOR: Duration = Duration::from_secs(10);

/// Worker-local lifetimes for admission grants and their replay records.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub struct AdmissionTicketConfig {
    max_valid_for: Duration,
    replay_retention: Duration,
}

impl AdmissionTicketConfig {
    pub const DEFAULT: Self = Self {
        max_valid_for: MAX_ADMISSION_TICKET_VALID_FOR,
        replay_retention: Duration::from_secs(120),
    };

    pub fn new(max_valid_for: Duration, replay_retention: Duration) -> Option<Self> {
        if max_valid_for.is_zero() || replay_retention.is_zero() {
            return None;
        }
        Some(Self {
            max_valid_for,
            replay_retention,
        })
    }

    pub const fn max_valid_for(self) -> Duration {
        self.max_valid_for
    }
    pub const fn replay_retention(self) -> Duration {
        self.replay_retention
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

/// A read-only, participation-bound view of one ticket at the worker's monotonic time.
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
    ForeignParticipation,
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
    receipt: ParticipationAdmissionTicketReceipt,
}

impl AdmissionTicketGrant {
    const fn new(
        progression: AdmissionTicketProgression,
        receipt: ParticipationAdmissionTicketReceipt,
    ) -> Self {
        Self {
            progression,
            receipt,
        }
    }

    pub const fn progression(self) -> AdmissionTicketProgression {
        self.progression
    }

    pub const fn receipt(self) -> ParticipationAdmissionTicketReceipt {
        self.receipt
    }
}

/// Why this worker refused to issue a ticket.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum AdmissionTicketAcquisitionRejection {
    ValidityExceedsWorkerLimit,
    ParticipationAlreadyGranted,
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
            Self::ParticipationAlreadyGranted => {
                "participation already has an active admission ticket"
            }
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
    Redeemed(ParticipationAdmissionTicketReceipt),
    Replayed(ParticipationAdmissionTicketReceipt),
}

/// Why a ticket cannot authorize an establish.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub enum AdmissionTicketRedemptionRejection {
    Unknown,
    Expired,
    Closed,
    ForeignParticipation,
}

impl fmt::Display for AdmissionTicketRedemptionRejection {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::Unknown => "establish names an unknown admission ticket",
            Self::Expired => "establish names an expired admission ticket",
            Self::Closed => "establish names a closed admission ticket",
            Self::ForeignParticipation => "admission ticket belongs to a different participation",
        })
    }
}

impl std::error::Error for AdmissionTicketRedemptionRejection {}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct AcquisitionIdentity {
    participation: ParticipationRef,
    valid_for: LeaseValidFor,
    compatibility: NativeCompatibilityId,
}
impl From<AcquireParticipationAdmissionTicket> for AcquisitionIdentity {
    fn from(r: AcquireParticipationAdmissionTicket) -> Self {
        Self {
            participation: r.participation(),
            valid_for: r.valid_for(),
            compatibility: r.native_compatibility_id(),
        }
    }
}
#[derive(Clone, Copy, Debug)]
struct TicketRecord {
    operation: TaskOperationId,
    receipt: ParticipationAdmissionTicketReceipt,
    state: AdmissionTicketState,
    expires_at: MonotonicInstant,
}
#[derive(Clone, Debug)]
struct Decision {
    identity: AcquisitionIdentity,
    result: Result<ParticipationAdmissionTicketReceipt, AdmissionTicketAcquisitionRejection>,
    stamp: u64,
}
/// Original table inventory. Estimates cover inline keys/values only; tree
/// nodes, allocator overhead and process memory are not measured here.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct AdmissionRetentionInventory {
    pub ticket_records: usize,
    pub acquisition_decisions: usize,
    pub replay_decisions: usize,
    pub expiry_entries: usize,
    pub metadata_bytes_estimate: Option<usize>,
}
#[derive(Debug)]
pub struct AdmissionCore {
    config: AdmissionTicketConfig,
    issuer: [u8; 8],
    next_stamp: u64,
    process_draining: bool,
    reclaim_frontier: u64,
    tickets: BTreeMap<AdmissionTicketId, TicketRecord>,
    participations: BTreeMap<ParticipationRef, AdmissionTicketId>,
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
            participations: BTreeMap::new(),
            decisions: BTreeMap::new(),
            expiries: BTreeSet::new(),
            retention: BTreeSet::new(),
        }
    }
    pub fn capability(&self) -> AdmissionEpochCapability {
        AdmissionEpochCapability::try_from_issuer_and_stamp(self.issuer, self.next_stamp)
            .expect("nonzero issuer and stamp")
    }
    /// Issued or redeemed grants not yet closed by a committed transition.
    /// This is observation only, never a limit or execution capacity grant.
    pub fn logical_live_grants(&self) -> usize {
        self.participations.len()
    }
    pub fn retention_inventory(&self) -> AdmissionRetentionInventory {
        use std::mem::size_of;
        let metadata_bytes_estimate = Some(size_of::<Self>())
            .and_then(|n| {
                n.checked_add(
                    self.tickets
                        .len()
                        .checked_mul(size_of::<(AdmissionTicketId, TicketRecord)>())?,
                )
            })
            .and_then(|n| {
                n.checked_add(
                    self.participations
                        .len()
                        .checked_mul(size_of::<(ParticipationRef, AdmissionTicketId)>())?,
                )
            })
            .and_then(|n| {
                n.checked_add(
                    self.decisions
                        .len()
                        .checked_mul(size_of::<(TaskOperationId, Decision)>())?,
                )
            })
            .and_then(|n| {
                n.checked_add(
                    self.expiries
                        .len()
                        .checked_mul(size_of::<(MonotonicInstant, AdmissionTicketId)>())?,
                )
            })
            .and_then(|n| {
                n.checked_add(
                    self.retention
                        .len()
                        .checked_mul(size_of::<(MonotonicInstant, TaskOperationId)>())?,
                )
            });
        AdmissionRetentionInventory {
            ticket_records: self.tickets.len(),
            acquisition_decisions: self.decisions.len(),
            // Includes retained rejections without a ticket; never a ticket count.
            replay_decisions: self.retention.len(),
            expiry_entries: self.expiries.len(),
            metadata_bytes_estimate,
        }
    }
    /// Read-only replay existence check used before a new wait deadline may
    /// claim that this operation never acquired ownership.
    pub fn has_decision(&self, operation: TaskOperationId) -> bool {
        self.decisions.contains_key(&operation)
    }
    pub fn acquire(
        &mut self,
        request: AcquireParticipationAdmissionTicket,
        ticket: AdmissionTicketId,
        now: MonotonicInstant,
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
        if capability.issuer() != self.issuer || capability.stamp() <= self.reclaim_frontier {
            return Err(AdmissionTicketAcquisitionRejection::SealedEpoch);
        }
        if self.tickets.contains_key(&ticket) {
            return Err(AdmissionTicketAcquisitionRejection::OperationReplayConflict);
        }
        let reject = if request.valid_for().get() > self.config.max_valid_for {
            Some(AdmissionTicketAcquisitionRejection::ValidityExceedsWorkerLimit)
        } else if self.participations.contains_key(&request.participation()) {
            Some(AdmissionTicketAcquisitionRejection::ParticipationAlreadyGranted)
        } else {
            None
        };
        if let Some(e) = reject {
            self.record(operation, identity, Err(e.clone()), Some(now));
            return Err(e);
        }
        let receipt = ParticipationAdmissionTicketReceipt::new(
            ticket,
            request.participation(),
            request.valid_for(),
        );
        let expires_at = now.saturating_add(request.valid_for().get());
        self.tickets.insert(
            ticket,
            TicketRecord {
                operation,
                receipt,
                state: AdmissionTicketState::Issued,
                expires_at,
            },
        );
        self.participations.insert(request.participation(), ticket);
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
        result: Result<ParticipationAdmissionTicketReceipt, AdmissionTicketAcquisitionRejection>,
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
        participation: ParticipationRef,
        now: MonotonicInstant,
    ) -> Result<AdmissionTicketRedemption, AdmissionTicketRedemptionRejection> {
        let r = self
            .tickets
            .get_mut(&ticket)
            .ok_or(AdmissionTicketRedemptionRejection::Unknown)?;
        if r.receipt.participation() != participation {
            return Err(AdmissionTicketRedemptionRejection::ForeignParticipation);
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
    /// Process drain closes new grant/election rights. Existing decisions
    /// remain observable; closing a ticket never proves physical Participation exit.
    pub(crate) fn begin_process_drain(&mut self) {
        self.process_draining = true;
    }
    pub(crate) fn has_unredeemed(&self) -> bool {
        !self.expiries.is_empty()
    }
    /// The expiry index contains only actual issued, never redeemed tickets.
    /// Revoke one original without fabricating elapsed time or scanning rows.
    pub(crate) fn revoke_next_unredeemed(&mut self, now: MonotonicInstant) -> bool {
        let Some((_, ticket)) = self.expiries.first().copied() else {
            return false;
        };
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
        participation: ParticipationRef,
        now: MonotonicInstant,
    ) -> bool {
        let Some(&ticket) = self.participations.get(&participation) else {
            return false;
        };
        if self.tickets[&ticket].state != AdmissionTicketState::Issued {
            return false;
        }
        self.terminal(ticket, AdmissionTicketState::Closed, now)
    }
    pub fn close_participation(
        &mut self,
        participation: ParticipationRef,
        now: MonotonicInstant,
    ) -> bool {
        let Some(ticket) = self.participations.remove(&participation) else {
            return false;
        };
        self.terminal(ticket, AdmissionTicketState::Closed, now)
    }
    fn terminal(
        &mut self,
        ticket: AdmissionTicketId,
        state: AdmissionTicketState,
        now: MonotonicInstant,
    ) -> bool {
        let Some(r) = self.tickets.get_mut(&ticket) else {
            return false;
        };
        if matches!(
            r.state,
            AdmissionTicketState::Closed | AdmissionTicketState::Expired
        ) {
            return false;
        }
        self.participations.remove(&r.receipt.participation());
        self.expiries.remove(&(r.expires_at, ticket));
        r.state = state;
        self.retention.insert((
            now.saturating_add(self.config.replay_retention),
            r.operation,
        ));
        true
    }
    /// A bounded cursor over indexes, never a scan of every task/participation.
    /// Returns the number of metadata records visited for step accounting.
    /// Ticket expiration and replay forgetting never release execution work.
    pub fn advance(&mut self, now: MonotonicInstant, budget: usize) -> usize {
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
            self.terminal(ticket, AdmissionTicketState::Expired, now);
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
        budget - remaining
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
        participation: ParticipationRef,
        now: MonotonicInstant,
    ) -> AdmissionTicketObservation {
        let Some(r) = self.tickets.get(&ticket) else {
            return AdmissionTicketObservation::Unknown;
        };
        if r.receipt.participation() != participation {
            return AdmissionTicketObservation::ForeignParticipation;
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
