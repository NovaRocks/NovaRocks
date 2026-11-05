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

//! Receiver transitions for the ordered Exchange unit domain.
//!
//! The composing receiver owns slots, decoded batches and actual work. A
//! decoding token authorizes one decode; neither a stop nor a close forges its
//! exit. Custody retires sender metadata, while handoff returns receiver slots.

use std::collections::BTreeMap;
use std::fmt;
use std::num::{NonZeroU64, NonZeroUsize};

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct ExchangeUnitSequence(NonZeroU64);

impl ExchangeUnitSequence {
    pub const FIRST: Self = Self(NonZeroU64::MIN);

    pub fn try_new(value: u64) -> Result<Self, ExchangeReceiptError> {
        NonZeroU64::new(value)
            .map(Self)
            .ok_or(ExchangeReceiptError::ZeroSequence)
    }

    pub const fn get(self) -> u64 {
        self.0.get()
    }

    pub fn checked_next(self) -> Result<Self, ExchangeReceiptError> {
        Self::try_new(
            self.get()
                .checked_add(1)
                .ok_or(ExchangeReceiptError::SequenceOverflow)?,
        )
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ExchangeUnitDemand {
    pub sequence: ExchangeUnitSequence,
    pub wire_bytes: u64,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ExchangeUnitItem {
    Unit {
        demand: ExchangeUnitDemand,
        end_after_unit: bool,
    },
    End(ExchangeUnitSequence),
}

impl ExchangeUnitItem {
    pub const fn sequence(self) -> ExchangeUnitSequence {
        match self {
            Self::Unit { demand, .. } => demand.sequence,
            Self::End(sequence) => sequence,
        }
    }
}

/// Authored by actual input exhaustion, not inferred from Task status or ACK.
/// Consumed covers every item through the stream's final End sequence, including
/// data carried by the end-after-unit case.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ExchangeUnitCloseDisposition {
    Consumed {
        final_sequence: ExchangeUnitSequence,
    },
    Withdrawn,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct ExchangeStreamFacts {
    pub custody_through: u64,
    pub granted_through: u64,
    pub sender_stopped: bool,
    pub receiver_closed: Option<ExchangeUnitCloseDisposition>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ExchangeDemandOutcome {
    Registered,
    Idempotent,
    Closed,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ExchangeAdmissionVerdict {
    Applied,
    Idempotent,
}

/// This value is neither Clone nor Copy: the receiver work owns one completion
/// obligation. Its fields cannot be constructed by a transport adapter.
#[derive(Debug, Eq, PartialEq)]
pub struct ExchangeDecodeToken {
    sequence: ExchangeUnitSequence,
    generation: u64,
}

impl ExchangeDecodeToken {
    pub const fn sequence(&self) -> ExchangeUnitSequence {
        self.sequence
    }
}

#[derive(Debug, Eq, PartialEq)]
pub enum ExchangeAdmissionStart {
    Decode(ExchangeDecodeToken),
    Observed(ExchangeAdmissionVerdict),
    Closed,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ExchangeDecodeCommit {
    Committed,
    /// The receiver closed while work was outside its lock. The work's actual
    /// exit, not the close request, returns this unit's slot.
    Discarded,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum UnitPhase {
    Reserved,
    Decoding(u64),
    Admitted,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct UnitRecord {
    demand: ExchangeUnitDemand,
    end_after_unit: Option<bool>,
    phase: UnitPhase,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ExchangeReceiptError {
    ZeroSequence,
    SequenceOverflow,
    InvalidPositionLimit,
    PositionLimit,
    InvalidWireBytes,
    UnitTooLarge { requested: u64, limit: u64 },
    DemandGap,
    DemandConflict,
    MissingGrant,
    UnitConflict,
    EndConflict,
    InvalidDecodeToken,
    HandoffGap,
    NotAdmitted,
    InputNotConsumed,
    CloseConflict,
}

impl fmt::Display for ExchangeReceiptError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ZeroSequence => f.write_str("exchange unit sequence must be nonzero"),
            Self::SequenceOverflow => f.write_str("exchange unit sequence domain is exhausted"),
            Self::InvalidPositionLimit => {
                f.write_str("exchange receipt position limit must be positive")
            }
            Self::PositionLimit => f.write_str("exchange receipt has no free unit position"),
            Self::InvalidWireBytes => f.write_str("exchange data unit must contain wire bytes"),
            Self::UnitTooLarge { requested, limit } => write!(
                f,
                "exchange unit requires {requested} wire bytes; grantable limit is {limit}"
            ),
            Self::DemandGap => f.write_str("exchange demand crosses an unregistered unit"),
            Self::DemandConflict => {
                f.write_str("exchange demand conflicts with its registered unit")
            }
            Self::MissingGrant => f.write_str("exchange data unit has no live reservation"),
            Self::UnitConflict => {
                f.write_str("exchange unit conflicts with its registered evidence")
            }
            Self::EndConflict => {
                f.write_str("exchange End conflicts with the data prefix or existing End")
            }
            Self::InvalidDecodeToken => {
                f.write_str("exchange decode completion has no matching claim")
            }
            Self::HandoffGap => f.write_str("exchange handoff crosses an unconsumed unit"),
            Self::NotAdmitted => f.write_str("exchange unit has not entered the admitted prefix"),
            Self::InputNotConsumed => {
                f.write_str("exchange input has not actually consumed its End")
            }
            Self::CloseConflict => {
                f.write_str("exchange receiver close conflicts with its first close")
            }
        }
    }
}

impl std::error::Error for ExchangeReceiptError {}
impl novarocks_failure_contract::FailureProjection for ExchangeReceiptError {
    fn failure_fact(&self) -> novarocks_failure_contract::FailureFact {
        use novarocks_failure_contract::*;
        // Exchange receipt transitions own 200..=215 in the Exchange domain.
        // A full owner position is control, never authorization to fail a
        // legitimate backpressured query as a resource or capability error.
        let (class, code) = match self {
            Self::ZeroSequence => (FailureClass::Data, 200),
            Self::SequenceOverflow => (FailureClass::Representation, 201),
            Self::InvalidPositionLimit => (FailureClass::Invariant, 202),
            Self::PositionLimit => (FailureClass::Control, 203),
            Self::InvalidWireBytes => (FailureClass::Data, 204),
            Self::UnitTooLarge { .. } => (FailureClass::Resource, 205),
            Self::DemandGap => (FailureClass::Data, 206),
            Self::DemandConflict => (FailureClass::Data, 207),
            Self::MissingGrant => (FailureClass::Data, 208),
            Self::UnitConflict => (FailureClass::Data, 209),
            Self::EndConflict => (FailureClass::Data, 210),
            Self::InvalidDecodeToken => (FailureClass::Invariant, 211),
            Self::HandoffGap => (FailureClass::Invariant, 212),
            Self::NotAdmitted => (FailureClass::Invariant, 213),
            Self::InputNotConsumed => (FailureClass::Invariant, 214),
            Self::CloseConflict => (FailureClass::Invariant, 215),
        };
        let fact = FailureFact::new(
            class,
            FailureReason::new(domain::EXCHANGE, std::num::NonZeroU16::new(code).unwrap()),
        );
        match self {
            Self::UnitTooLarge { requested, limit } => fact.with_resource(ResourceFacts {
                request_scope: Some(ResourceScope::Request),
                constraint_scope: Some(ResourceScope::Request),
                dimension: ResourceDimension::Bytes,
                requested: Some(*requested),
                required_growth: None,
                limit: Some(*limit),
                live: None,
                committed: None,
            }),
            Self::SequenceOverflow => fact.with_representation(RepresentationFacts {
                boundary: RepresentationBoundary::MinimumUnit,
                layout: RepresentationLayout {
                    domain: domain::EXCHANGE,
                    code: std::num::NonZeroU16::new(1003).unwrap(),
                },
                required: None,
                extent_limit: Some(u64::MAX),
                representable_prefix: None,
            }),
            _ => fact,
        }
    }
}

/// Pure, bounded metadata for one exact stream. The caller serializes every
/// transition with the containing instance's slot and queue transition.
#[derive(Debug)]
pub struct ExchangeStreamReceiverState {
    custody: u64,
    handed_off: u64,
    granted: u64,
    units: BTreeMap<u64, UnitRecord>,
    demand: Option<ExchangeUnitDemand>,
    end: Option<ExchangeUnitSequence>,
    sender_stopped: bool,
    receiver_closed: Option<ExchangeUnitCloseDisposition>,
    next_decode_generation: u64,
    position_limit: NonZeroUsize,
}

impl ExchangeStreamReceiverState {
    pub fn try_new(position_limit: usize) -> Result<Self, ExchangeReceiptError> {
        Ok(Self {
            custody: 0,
            handed_off: 0,
            granted: 0,
            units: BTreeMap::new(),
            demand: None,
            end: None,
            sender_stopped: false,
            receiver_closed: None,
            next_decode_generation: 0,
            position_limit: NonZeroUsize::new(position_limit)
                .ok_or(ExchangeReceiptError::InvalidPositionLimit)?,
        })
    }

    pub const fn facts(&self) -> ExchangeStreamFacts {
        ExchangeStreamFacts {
            custody_through: self.custody,
            granted_through: self.granted,
            sender_stopped: self.sender_stopped,
            receiver_closed: self.receiver_closed,
        }
    }

    pub const fn handed_off_through(&self) -> u64 {
        self.handed_off
    }

    pub const fn observed_end(&self) -> Option<ExchangeUnitSequence> {
        self.end
    }

    pub const fn pending_demand(&self) -> Option<ExchangeUnitDemand> {
        self.demand
    }

    pub fn occupied_positions(&self) -> usize {
        self.units.len()
    }

    pub fn decoding_positions(&self) -> usize {
        self.units
            .values()
            .filter(|record| matches!(record.phase, UnitPhase::Decoding(_)))
            .count()
    }

    fn accepts_new_work(&self) -> bool {
        !self.sender_stopped && self.receiver_closed.is_none()
    }

    pub fn register_demand(
        &mut self,
        demand: ExchangeUnitDemand,
        grantable_wire_bytes: u64,
    ) -> Result<ExchangeDemandOutcome, ExchangeReceiptError> {
        if !self.accepts_new_work() {
            return Ok(ExchangeDemandOutcome::Closed);
        }
        self.check_data_boundary(demand.sequence)?;
        if demand.wire_bytes == 0 {
            return Err(ExchangeReceiptError::InvalidWireBytes);
        }
        if demand.wire_bytes > grantable_wire_bytes {
            return Err(ExchangeReceiptError::UnitTooLarge {
                requested: demand.wire_bytes,
                limit: grantable_wire_bytes,
            });
        }
        if let Some(record) = self.units.get(&demand.sequence.get()) {
            return if record.demand == demand {
                Ok(ExchangeDemandOutcome::Idempotent)
            } else {
                Err(ExchangeReceiptError::DemandConflict)
            };
        }
        // Evidence is retired at handoff; the sender still owes immutable replay.
        if demand.sequence.get() <= self.custody {
            return Ok(ExchangeDemandOutcome::Idempotent);
        }
        if let Some(pending) = self.demand {
            return if pending == demand {
                Ok(ExchangeDemandOutcome::Idempotent)
            } else {
                Err(ExchangeReceiptError::DemandConflict)
            };
        }
        let next = self
            .granted
            .checked_add(1)
            .ok_or(ExchangeReceiptError::SequenceOverflow)?;
        if demand.sequence.get() != next {
            return Err(ExchangeReceiptError::DemandGap);
        }
        self.demand = Some(demand);
        Ok(ExchangeDemandOutcome::Registered)
    }

    /// The instance first decides that it can reserve a slot. A successful
    /// transition consumes that exact reservation; None consumes nothing.
    pub fn grant_next(&mut self) -> Result<Option<ExchangeUnitDemand>, ExchangeReceiptError> {
        if !self.accepts_new_work() || self.demand.is_none() {
            return Ok(None);
        }
        if self.units.len() == self.position_limit.get() {
            return Err(ExchangeReceiptError::PositionLimit);
        }
        let demand = self.demand.take().expect("demand checked above");
        self.units.insert(
            demand.sequence.get(),
            UnitRecord {
                demand,
                end_after_unit: None,
                phase: UnitPhase::Reserved,
            },
        );
        self.granted = demand.sequence.get();
        Ok(Some(demand))
    }

    fn check_data_boundary(
        &self,
        sequence: ExchangeUnitSequence,
    ) -> Result<(), ExchangeReceiptError> {
        if self.end.is_some_and(|end| sequence >= end) {
            return Err(ExchangeReceiptError::EndConflict);
        }
        Ok(())
    }

    fn check_end(&self, sequence: ExchangeUnitSequence) -> Result<(), ExchangeReceiptError> {
        if let Some(end) = self.end {
            return if end == sequence {
                Ok(())
            } else {
                Err(ExchangeReceiptError::EndConflict)
            };
        }
        let next = self
            .granted
            .checked_add(1)
            .ok_or(ExchangeReceiptError::SequenceOverflow)?;
        if sequence.get() != next || self.demand.is_some() {
            return Err(ExchangeReceiptError::EndConflict);
        }
        Ok(())
    }

    fn observe_end(&mut self, sequence: ExchangeUnitSequence) -> ExchangeAdmissionVerdict {
        if self.end.is_some() {
            return ExchangeAdmissionVerdict::Idempotent;
        }
        self.end = Some(sequence);
        self.advance_custody();
        ExchangeAdmissionVerdict::Applied
    }

    /// No decode may begin before this transition returns its unique token.
    /// Observing an in-progress original never waits for that original.
    pub fn begin_admission(
        &mut self,
        item: ExchangeUnitItem,
    ) -> Result<ExchangeAdmissionStart, ExchangeReceiptError> {
        if !self.accepts_new_work() {
            return Ok(ExchangeAdmissionStart::Closed);
        }
        let ExchangeUnitItem::Unit {
            demand,
            end_after_unit,
        } = item
        else {
            let sequence = item.sequence();
            self.check_end(sequence)?;
            return Ok(ExchangeAdmissionStart::Observed(self.observe_end(sequence)));
        };
        self.check_data_boundary(demand.sequence)?;
        let n = demand.sequence.get();
        let record = self.units.get(&n).copied();
        if let Some(record) = record {
            if record.demand != demand
                || record
                    .end_after_unit
                    .is_some_and(|end| end != end_after_unit)
            {
                return Err(ExchangeReceiptError::UnitConflict);
            }
        } else if n > self.custody {
            return Err(ExchangeReceiptError::MissingGrant);
        }
        let end = if end_after_unit {
            let end = demand.sequence.checked_next()?;
            self.check_end(end)?;
            Some(end)
        } else {
            None
        };
        // Check token exhaustion before changing End or record state.
        let new_generation = if record.is_some_and(|r| r.phase == UnitPhase::Reserved) {
            Some(
                self.next_decode_generation
                    .checked_add(1)
                    .ok_or(ExchangeReceiptError::SequenceOverflow)?,
            )
        } else {
            None
        };
        if let Some(end) = end {
            self.observe_end(end);
        }
        let Some(generation) = new_generation else {
            return Ok(ExchangeAdmissionStart::Observed(
                ExchangeAdmissionVerdict::Idempotent,
            ));
        };
        self.next_decode_generation = generation;
        let record = self.units.get_mut(&n).expect("reserved unit checked above");
        record.end_after_unit = Some(end_after_unit);
        record.phase = UnitPhase::Decoding(generation);
        Ok(ExchangeAdmissionStart::Decode(ExchangeDecodeToken {
            sequence: demand.sequence,
            generation,
        }))
    }

    fn validate_token(&self, token: &ExchangeDecodeToken) -> Result<(), ExchangeReceiptError> {
        if self
            .units
            .get(&token.sequence.get())
            .is_some_and(|record| record.phase == UnitPhase::Decoding(token.generation))
        {
            Ok(())
        } else {
            Err(ExchangeReceiptError::InvalidDecodeToken)
        }
    }

    /// Call only after this token's decode has actually returned. The receiver
    /// queue commit must be serialized with this transition by the caller.
    pub fn finish_decode(
        &mut self,
        token: ExchangeDecodeToken,
    ) -> Result<ExchangeDecodeCommit, ExchangeReceiptError> {
        self.validate_token(&token)?;
        if self.receiver_closed.is_some() {
            self.units.remove(&token.sequence.get());
            return Ok(ExchangeDecodeCommit::Discarded);
        }
        self.units
            .get_mut(&token.sequence.get())
            .expect("token validated")
            .phase = UnitPhase::Admitted;
        self.advance_custody();
        Ok(ExchangeDecodeCommit::Committed)
    }

    /// Failed/cancelled work returns its slot only on real exit. The composing
    /// owner reports the original failure; this operation invents no EOS.
    pub fn abandon_decode(
        &mut self,
        token: ExchangeDecodeToken,
    ) -> Result<ExchangeUnitDemand, ExchangeReceiptError> {
        self.validate_token(&token)?;
        Ok(self
            .units
            .remove(&token.sequence.get())
            .expect("token validated")
            .demand)
    }

    fn advance_custody(&mut self) {
        while let Some(next) = self.custody.checked_add(1) {
            if self.end.is_some_and(|end| end.get() == next) {
                self.custody = next;
                break;
            }
            if self
                .units
                .get(&next)
                .is_some_and(|record| record.phase == UnitPhase::Admitted)
            {
                self.custody = next;
            } else {
                break;
            }
        }
    }

    pub fn hand_off(
        &mut self,
        sequence: ExchangeUnitSequence,
    ) -> Result<ExchangeUnitDemand, ExchangeReceiptError> {
        let n = sequence.get();
        if self.receiver_closed.is_some() || self.handed_off.checked_add(1) != Some(n) {
            return Err(ExchangeReceiptError::HandoffGap);
        }
        if n > self.custody
            || !self
                .units
                .get(&n)
                .is_some_and(|record| record.phase == UnitPhase::Admitted)
        {
            return Err(ExchangeReceiptError::NotAdmitted);
        }
        self.handed_off = n;
        Ok(self.units.remove(&n).expect("admitted unit checked").demand)
    }

    /// The source calls this only when its input actually returns Finished.
    pub fn complete_input_consumption(&mut self) -> Result<(), ExchangeReceiptError> {
        let end = self.end.ok_or(ExchangeReceiptError::InputNotConsumed)?;
        // Several source drivers can independently observe the same exhausted
        // receiver. Re-observation preserves the first consumption fact.
        if self.receiver_closed.is_none()
            && self.custody == end.get()
            && self.handed_off == end.get()
            && self.units.is_empty()
        {
            return Ok(());
        }
        if self.receiver_closed.is_some()
            || self.custody != end.get()
            || self.handed_off.checked_add(1) != Some(end.get())
            || !self.units.is_empty()
        {
            return Err(ExchangeReceiptError::InputNotConsumed);
        }
        self.handed_off = end.get();
        Ok(())
    }

    /// Return only never-started reservations. Previously issued decode tokens
    /// and admitted units retain responsibility through handoff or real cleanup.
    pub fn stop_sender(&mut self) -> Vec<ExchangeUnitDemand> {
        if self.sender_stopped {
            return Vec::new();
        }
        self.sender_stopped = true;
        self.demand = None;
        let mut released = Vec::new();
        self.units.retain(|_, record| {
            if record.phase == UnitPhase::Reserved {
                released.push(record.demand);
                false
            } else {
                true
            }
        });
        released
    }

    /// The receiver's lifecycle owner supplies its independently authored close.
    /// Decoding work stays pinned until finish_decode/abandon_decode observes exit.
    pub fn close_receiver(
        &mut self,
        disposition: ExchangeUnitCloseDisposition,
    ) -> Result<Vec<ExchangeUnitDemand>, ExchangeReceiptError> {
        if let Some(previous) = self.receiver_closed {
            return if previous == disposition {
                Ok(Vec::new())
            } else {
                Err(ExchangeReceiptError::CloseConflict)
            };
        }
        if let ExchangeUnitCloseDisposition::Consumed { final_sequence } = disposition
            && (self.end != Some(final_sequence) || self.handed_off != final_sequence.get())
        {
            return Err(ExchangeReceiptError::InputNotConsumed);
        }
        self.receiver_closed = Some(disposition);
        self.demand = None;
        let mut released = Vec::new();
        self.units.retain(|_, record| {
            if matches!(record.phase, UnitPhase::Decoding(_)) {
                true
            } else {
                released.push(record.demand);
                false
            }
        });
        Ok(released)
    }
}

#[cfg(test)]
mod tests;
