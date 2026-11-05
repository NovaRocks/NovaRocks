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

//! Pure root retention and read-seal transitions. Physical payload owners,
//! producer joins, socket receipts and wakeups belong to the host runtime.

use crate::identity::TaskIdentity;
use crate::root_result::RootResultEnd;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RootRetentionClose {
    ParticipationReleased,
    ParticipationAborted,
    LeaseExpired,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RootLifetimeError {
    Closed,
    ConflictingEnd,
    ProducerStillRunning,
    EndNotPublished,
    ParticipationNotHolding,
    ReadNotConverged,
}
impl std::fmt::Display for RootLifetimeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Self::Closed => "root retention is closed",
            Self::ConflictingEnd => "root terminal publication is immutable",
            Self::ProducerStillRunning => "root encoder has not actually exited",
            Self::EndNotPublished => "root End has not been published",
            Self::ParticipationNotHolding => "root result has not been handed to its context",
            Self::ReadNotConverged => {
                "root read cannot seal before local End and root terminal evidence"
            }
        })
    }
}
impl std::error::Error for RootLifetimeError {}

/// One exact root channel survives Task retirement and its tombstone horizon.
/// Only its context's explicit lifetime can close retained payloads.
#[derive(Clone, Debug)]
pub struct RootResultLifetime {
    task: TaskIdentity,
    end: Option<RootResultEnd>,
    encoder_exited: bool,
    participation_holding: bool,
    closed: Option<RootRetentionClose>,
}
impl RootResultLifetime {
    pub const fn new(task: TaskIdentity) -> Self {
        Self {
            task,
            end: None,
            encoder_exited: false,
            participation_holding: false,
            closed: None,
        }
    }
    pub const fn task(&self) -> TaskIdentity {
        self.task
    }
    pub const fn end(&self) -> Option<RootResultEnd> {
        self.end
    }
    pub const fn close_reason(&self) -> Option<RootRetentionClose> {
        self.closed
    }
    pub const fn allows_read(&self) -> bool {
        self.closed.is_none()
    }
    pub const fn root_finished(&self) -> bool {
        self.end.is_some() && self.encoder_exited && self.participation_holding
    }
    /// Called only after the host has published the immutable End item.
    pub fn end_published(&mut self, end: RootResultEnd) -> Result<(), RootLifetimeError> {
        if self.closed.is_some() {
            return Err(RootLifetimeError::Closed);
        }
        if self.end.is_some_and(|published| published != end) {
            return Err(RootLifetimeError::ConflictingEnd);
        }
        self.end = Some(end);
        Ok(())
    }
    /// Actual producer exit is independent from a timeout or Task terminal.
    /// It may arrive after cancellation has already closed reads.
    pub fn encoder_exited(&mut self) {
        self.encoder_exited = true;
    }
    /// The context owner calls this after taking the retained root channel.
    pub fn handoff_to_participation(&mut self) -> Result<(), RootLifetimeError> {
        if self.closed.is_some() {
            return Err(RootLifetimeError::Closed);
        }
        if self.end.is_none() {
            return Err(RootLifetimeError::EndNotPublished);
        }
        if !self.encoder_exited {
            return Err(RootLifetimeError::ProducerStillRunning);
        }
        self.participation_holding = true;
        Ok(())
    }
    pub fn validate_task_retirement(&self) -> Result<(), RootLifetimeError> {
        if self.closed.is_some() || self.root_finished() {
            Ok(())
        } else {
            Err(RootLifetimeError::ParticipationNotHolding)
        }
    }
    /// This is a logical close, not proof that aliases physically released.
    /// The caller seals reads, drops retained owners and wakes blocked work.
    pub fn close(&mut self, reason: RootRetentionClose) {
        if self.closed.is_none() {
            self.closed = Some(reason);
        }
    }
}

/// FE's local receipt frontier and the originating control observation are
/// independent. Optional final ACK is deliberately absent from this gate.
#[derive(Clone, Debug)]
pub struct RootReadConvergence {
    task: TaskIdentity,
    consumed_end: Option<RootResultEnd>,
    root_finished_observed: bool,
    sealed: bool,
}
impl RootReadConvergence {
    pub const fn new(task: TaskIdentity) -> Self {
        Self {
            task,
            consumed_end: None,
            root_finished_observed: false,
            sealed: false,
        }
    }
    pub fn consume_end(&mut self, end: RootResultEnd) -> Result<(), RootLifetimeError> {
        if self.sealed {
            return Err(RootLifetimeError::Closed);
        }
        if self.consumed_end.is_some_and(|consumed| consumed != end) {
            return Err(RootLifetimeError::ConflictingEnd);
        }
        self.consumed_end = Some(end);
        Ok(())
    }
    pub fn observe_root_finished(&mut self) {
        self.root_finished_observed = true;
    }
    pub const fn can_seal_normal(&self) -> bool {
        self.consumed_end.is_some() && self.root_finished_observed
    }
    pub fn seal_normal(&mut self) -> Result<RootReadSealed, RootLifetimeError> {
        if !self.can_seal_normal() {
            return Err(RootLifetimeError::ReadNotConverged);
        }
        self.sealed = true;
        Ok(RootReadSealed { task: self.task })
    }
    /// Only the owner of an originating failure/cancellation observation calls
    /// this path. Its failure evidence remains with that owner, not a root ACK.
    pub fn seal_terminated(&mut self) -> RootReadSealed {
        self.sealed = true;
        RootReadSealed { task: self.task }
    }
    pub const fn sealed(&self) -> bool {
        self.sealed
    }
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RootReadSealed {
    task: TaskIdentity,
}
impl RootReadSealed {
    pub const fn task(self) -> TaskIdentity {
        self.task
    }
}

impl novarocks_failure_contract::FailureProjection for RootLifetimeError {
    fn failure_fact(&self) -> novarocks_failure_contract::FailureFact {
        use novarocks_failure_contract::*;
        let (class, code) = match self {
            Self::Closed => (FailureClass::Control, 220),
            Self::ConflictingEnd => (FailureClass::Invariant, 221),
            Self::ProducerStillRunning => (FailureClass::Invariant, 222),
            Self::EndNotPublished => (FailureClass::Invariant, 223),
            Self::ParticipationNotHolding => (FailureClass::Invariant, 224),
            Self::ReadNotConverged => (FailureClass::Invariant, 225),
        };
        FailureFact::new(
            class,
            FailureReason::new(domain::RESULT, std::num::NonZeroU16::new(code).unwrap()),
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use novarocks_types::{
        AttemptId, BackendProcessId, QueryExecutionId, QueryId, StageId, TaskId,
    };
    use std::num::NonZeroU64;
    fn task() -> TaskIdentity {
        TaskIdentity::new(
            QueryExecutionId::new(QueryId::new(1, 2), AttemptId::new(1).unwrap()).unwrap(),
            StageId::new(1).unwrap(),
            TaskId::new(1).unwrap(),
            BackendProcessId::new_v7(),
        )
    }
    fn end() -> RootResultEnd {
        RootResultEnd {
            sequence: NonZeroU64::new(3).unwrap(),
            output_rows: 7,
        }
    }
    #[test]
    fn finished_requires_publication_actual_exit_and_participation_handoff() {
        let mut root = RootResultLifetime::new(task());
        assert_eq!(
            root.handoff_to_participation(),
            Err(RootLifetimeError::EndNotPublished)
        );
        root.end_published(end()).unwrap();
        assert_eq!(
            root.handoff_to_participation(),
            Err(RootLifetimeError::ProducerStillRunning)
        );
        assert!(!root.root_finished());
        assert!(root.validate_task_retirement().is_err());
        root.encoder_exited();
        assert!(!root.root_finished());
        root.handoff_to_participation().unwrap();
        assert!(root.root_finished());
        root.validate_task_retirement().unwrap();
        assert!(
            root.allows_read(),
            "retiring a Task must not retire its root channel"
        );
        assert!(
            root.end_published(RootResultEnd {
                output_rows: 8,
                ..end()
            })
            .is_err()
        );
        root.close(RootRetentionClose::ParticipationReleased);
        assert!(!root.allows_read());
        assert!(
            root.root_finished(),
            "originating terminal evidence remains observable"
        );
    }
    #[test]
    fn seal_accepts_both_observation_orders_without_a_final_ack() {
        for end_first in [false, true] {
            let mut read = RootReadConvergence::new(task());
            if end_first {
                read.consume_end(end()).unwrap();
            } else {
                read.observe_root_finished();
            }
            assert!(read.seal_normal().is_err());
            if end_first {
                read.observe_root_finished();
            } else {
                read.consume_end(end()).unwrap();
            }
            let seal = read.seal_normal().unwrap();
            assert_eq!(seal.task(), read.task);
            assert!(read.sealed());
            assert!(read.consume_end(end()).is_err());
        }
    }
    #[test]
    fn abort_and_lease_close_reads_before_late_encoder_exit() {
        for reason in [
            RootRetentionClose::ParticipationAborted,
            RootRetentionClose::LeaseExpired,
        ] {
            let mut root = RootResultLifetime::new(task());
            root.close(reason);
            assert!(!root.allows_read());
            root.encoder_exited();
            root.validate_task_retirement().unwrap();
            assert!(root.end_published(end()).is_err());
            assert!(!root.root_finished());
        }
    }
}
