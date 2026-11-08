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

//! Fixed semantic debt from one frozen Native ACK prefix. Registration prices
//! and allocates these rows from the actual mailbox's existing queued bound.
//! This stores no raw input, permit, rich payload or input-retirement role.
//! Recording precedes domain folding; QA ownership is selected only later at
//! the original post-round settlement phase.

use crate::{
    input::{AckPayload, OperationReply},
    operation::OperationDispatchResult,
};
use novarocks_execution_contract::{
    OperationKind, OperationOutcome, ParticipationAdmissionTicketReceipt, TaskOperationId,
};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ObservedDispatch {
    Unknown,
    PreWorker,
    Worker(OperationOutcome),
}
impl ObservedDispatch {
    pub(crate) fn from_dispatch(result: OperationDispatchResult) -> Self {
        match result {
            OperationDispatchResult::TransportUnknown => Self::Unknown,
            OperationDispatchResult::IngressRejected(_)
            | OperationDispatchResult::NonWorkerRejected => Self::PreWorker,
            OperationDispatchResult::WorkerReceipt(receipt) => Self::Worker(receipt.outcome()),
        }
    }
    pub(crate) fn worker_outcome(self) -> Option<OperationOutcome> {
        match self {
            Self::Worker(outcome) => Some(outcome),
            _ => None,
        }
    }
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct AdmissionObservation {
    pub(crate) operation_id: TaskOperationId,
    pub(crate) result: ObservedDispatch,
    pub(crate) ticket: Option<ParticipationAdmissionTicketReceipt>,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct EstablishObservation {
    pub(crate) operation_id: TaskOperationId,
    pub(crate) result: ObservedDispatch,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ObserverDebt {
    Admission(AdmissionObservation),
    Establish(EstablishObservation),
}
impl ObserverDebt {
    /// Pure extraction only. Do not validate a ticket or sample current/late
    /// ownership here: preceding observed settlements have not run yet.
    pub(crate) fn from_reply(reply: &OperationReply) -> Option<Self> {
        let result = ObservedDispatch::from_dispatch(reply.dispatch_result());
        match reply.kind() {
            OperationKind::AcquireParticipationAdmissionTicket => {
                Some(Self::Admission(AdmissionObservation {
                    operation_id: reply.operation_id(),
                    result,
                    ticket: match reply.payload() {
                        AckPayload::AdmissionTicket(ticket) => Some(*ticket),
                        _ => None,
                    },
                }))
            }
            OperationKind::UpdateParticipation => Some(Self::Establish(EstablishObservation {
                operation_id: reply.operation_id(),
                result,
            })),
            _ => None,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum JournalError {
    GeometryOverflow,
    EmptyGeometry,
    WrongPhase,
    /// An original cut/registration binding was violated, not a resource or
    /// retry refusal. A genuine frozen queued prefix cannot exceed this bound.
    RegistrationMismatch,
    NoCurrentObservation,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Phase {
    Idle,
    Recording,
    Draining,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct ObserverJournalQuote {
    pub(crate) slots: usize,
    pub(crate) bytes: usize,
}
impl ObserverJournalQuote {
    /// Every original mailbox push, including standing fills, checks its
    /// class's queued bound. A Source prefix sampled under that same inbox lock
    /// therefore has at most this sum of rows. These bytes must be included in
    /// the original registration metadata price, in addition to later mailbox
    /// refills; the bound alone grants no storage or capacity.
    pub(crate) fn from_mailbox(mailbox: [usize; 4]) -> Result<Self, JournalError> {
        let slots = mailbox
            .into_iter()
            .try_fold(0usize, usize::checked_add)
            .ok_or(JournalError::GeometryOverflow)?;
        if slots == 0 {
            return Err(JournalError::EmptyGeometry);
        }
        let bytes = slots
            .checked_mul(std::mem::size_of::<ObserverDebt>())
            .and_then(|rows| rows.checked_add(std::mem::size_of::<ObserverJournal>()))
            .ok_or(JournalError::GeometryOverflow)?;
        Ok(Self { slots, bytes })
    }
}
#[derive(Debug)]
pub(crate) struct ObserverJournal {
    rows: Vec<ObserverDebt>,
    bound: usize,
    current: usize,
    phase: Phase,
}
impl ObserverJournal {
    /// Called before actor registration from the same original configuration
    /// used to quote metadata. No operation identity or authorization is minted.
    pub(crate) fn new(mailbox: [usize; 4]) -> Result<Self, JournalError> {
        let quote = ObserverJournalQuote::from_mailbox(mailbox)?;
        Ok(Self {
            rows: Vec::with_capacity(quote.slots),
            bound: quote.slots,
            current: 0,
            phase: Phase::Idle,
        })
    }
    /// The caller fixes the actual pre-pop Native Source tail exactly once and
    /// admits no subsequent Native cut until this journal has fully drained.
    pub(crate) fn begin_cut(&mut self) -> Result<(), JournalError> {
        if self.phase != Phase::Idle {
            return Err(JournalError::WrongPhase);
        }
        self.phase = Phase::Recording;
        Ok(())
    }
    /// The caller already filtered older queued Unknowns. Append at most one
    /// fixed fact per input in the selected original prefix, before domain fold.
    /// Caller charges that bounded primary application; this never starts QA.
    pub(crate) fn observe(&mut self, reply: &OperationReply) -> Result<(), JournalError> {
        if self.phase != Phase::Recording {
            return Err(JournalError::WrongPhase);
        }
        if let Some(debt) = ObserverDebt::from_reply(reply) {
            if self.rows.len() == self.bound {
                return Err(JournalError::RegistrationMismatch);
            }
            self.rows.push(debt);
        }
        Ok(())
    }
    /// The exact frozen tail was fully applied. Root/seal/publication and the
    /// surviving old round tail still run before the caller starts settlement.
    pub(crate) fn finish_recording(&mut self) -> Result<(), JournalError> {
        if self.phase != Phase::Recording {
            return Err(JournalError::WrongPhase);
        }
        self.phase = Phase::Draining;
        Ok(())
    }
    /// Full or an outstanding actual QA reply keeps this same row selected.
    /// The journal does not decide when the post-round phase is allowed to run.
    pub(crate) fn current(&self) -> Result<Option<ObserverDebt>, JournalError> {
        if self.phase != Phase::Draining {
            return Err(JournalError::WrongPhase);
        }
        Ok(self.rows.get(self.current).copied())
    }
    /// Only after the selected row's original semantic settlement has completed.
    /// Handoff, input consumption and actual capture exit cannot call this.
    pub(crate) fn settled_current(&mut self) -> Result<(), JournalError> {
        if self.current()?.is_none() {
            return Err(JournalError::NoCurrentObservation);
        }
        self.current += 1;
        Ok(())
    }
    pub(crate) fn finish_cut(&mut self) -> Result<(), JournalError> {
        if self.current()?.is_some() {
            return Err(JournalError::WrongPhase);
        }
        self.rows.clear();
        self.current = 0;
        self.phase = Phase::Idle;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::input::{LocalDispatchResult, OperationReplyResult};
    fn reply() -> OperationReply {
        let mut ids = crate::ids::OperationIds::new(crate::ids::IdSeed::new(91, 1));
        OperationReply {
            operation_id: ids.issue(),
            kind: OperationKind::AcquireParticipationAdmissionTicket,
            result: OperationReplyResult::Local {
                result: LocalDispatchResult::TransportUnknown,
                failure: None,
                protocol_failure: None,
                detail: None,
            },
        }
    }
    #[test]
    fn finite_cut_reuses_preallocated_storage_and_preserves_row_until_actual_settlement() {
        let mut journal = ObserverJournal::new([1, 2, 3, 4]).unwrap();
        let allocation = journal.rows.as_ptr();
        let capacity = journal.rows.capacity();
        let reply = reply();
        for _ in 0..3 {
            journal.begin_cut().unwrap();
            for _ in 0..10 {
                journal.observe(&reply).unwrap();
            }
            assert_eq!(journal.rows.as_ptr(), allocation);
            assert_eq!(journal.rows.capacity(), capacity);
            assert_eq!(
                journal.observe(&reply),
                Err(JournalError::RegistrationMismatch)
            );
            assert_eq!(journal.current(), Err(JournalError::WrongPhase));
            journal.finish_recording().unwrap();
            assert_eq!(journal.begin_cut(), Err(JournalError::WrongPhase));
            assert_eq!(journal.observe(&reply), Err(JournalError::WrongPhase));
            for _ in 0..10 {
                let selected = journal.current().unwrap().unwrap();
                for _ in 0..20 {
                    assert_eq!(journal.current().unwrap(), Some(selected));
                }
                assert_eq!(journal.finish_cut(), Err(JournalError::WrongPhase));
                journal.settled_current().unwrap();
            }
            assert_eq!(journal.current().unwrap(), None);
            assert_eq!(
                journal.settled_current(),
                Err(JournalError::NoCurrentObservation)
            );
            journal.finish_cut().unwrap();
            assert_eq!(journal.rows.as_ptr(), allocation);
        }
    }
    #[test]
    fn quote_uses_actual_queued_geometry_and_checked_metadata_peak() {
        let quote = ObserverJournalQuote::from_mailbox([1, 2, 3, 4]).unwrap();
        assert_eq!(quote.slots, 10);
        assert_eq!(
            quote.bytes,
            10 * std::mem::size_of::<ObserverDebt>() + std::mem::size_of::<ObserverJournal>()
        );
        assert_eq!(
            ObserverJournalQuote::from_mailbox([0; 4]),
            Err(JournalError::EmptyGeometry)
        );
        assert_eq!(
            ObserverJournalQuote::from_mailbox([usize::MAX, 1, 0, 0]),
            Err(JournalError::GeometryOverflow)
        );
        assert_eq!(
            ObserverJournalQuote::from_mailbox([usize::MAX, 0, 0, 0]),
            Err(JournalError::GeometryOverflow)
        );
    }
}
