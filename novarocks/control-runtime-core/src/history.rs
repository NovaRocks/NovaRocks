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

//! History records are observations, never inputs to scheduling or policy.

use crate::identity::{
    ActionId, ActorId, ConsumerId, Generation, InputId, ReservationId, ScopeId, ScopedWorkRef,
    SlotId, Tick, WorkId,
};
use std::collections::VecDeque;

/// One original registration within the capture's externally bound runtime
/// namespace. The attachment is the actual original slot, not a debug ID.
/// scope_origin survives detachment and may name an already retired Scope;
/// it never makes the process financial affiliation this actor's owner.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct HistoryActorTarget {
    pub actor: ActorId,
    pub attachment: SlotId,
    pub scope_origin: ScopeId,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum EffectHistoryPhase {
    /// The original effect ticket was handed off. Physical Work starts have
    /// their independent Started fact and are not inferred from this event.
    Dispatched,
    Published,
    Completed,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum FailurePublicationHistoryOrigin {
    SupervisorEffect {
        input: InputId,
        action: ActionId,
    },
    StartedCleanup {
        header: crate::owned_storage::OwnedPositionId,
    },
}
/// Original capacity intent, without IDs for any refused new object.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum CapacityHistoryRequest {
    Reserve {
        scope: ScopeId,
        generation: Generation,
        admission: crate::scope::WorkAdmission,
        kind: crate::scope::WorkKind,
        requested: crate::capacity::ReservationResources,
    },
    ReserveFromParent {
        scope: ScopeId,
        generation: Generation,
        parent: ReservationId,
        kind: crate::scope::WorkKind,
        requested: crate::capacity::ReservationResources,
    },
    ReserveChildRole {
        scope: ScopeId,
        generation: Generation,
        payer: ReservationId,
        payer_is_local: bool,
        parent: ScopedWorkRef,
        parent_is_local: bool,
        kind: crate::scope::WorkKind,
        requested: crate::capacity::ReservationResources,
    },
    OpenChildScope {
        parent: ScopedWorkRef,
        parent_is_local: bool,
        parent_generation: Option<Generation>,
        child_generation: Generation,
        kind: crate::scope::WorkKind,
        requested: crate::capacity::ReservationResources,
    },
    Split {
        scope: ScopeId,
        source: ReservationId,
        generation: Option<Generation>,
        requested: crate::capacity::ReservationResources,
    },
    TransferWhole {
        scope: ScopeId,
        source: ReservationId,
        generation: Option<Generation>,
        target: ScopedWorkRef,
        target_is_local: bool,
        /// None only when the original source no longer exists; the typed
        /// refusal must not invent a resource vector for that missing owner.
        requested: Option<crate::capacity::ReservationResources>,
    },
    ReleaseKeyedQuota {
        scope: ScopeId,
        source: ReservationId,
        generation: Option<Generation>,
        requested: crate::capacity::ResourceVector,
        keyed: crate::capacity::KeyedQuota,
    },
    ReleaseQuota {
        scope: ScopeId,
        source: ReservationId,
        generation: Option<Generation>,
        requested: crate::capacity::ResourceVector,
    },
    Finish {
        scope: ScopeId,
        source: ReservationId,
        generation: Option<Generation>,
    },
}
/// Exact neutral errors. API ownership/state failures are never exhaustion.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CapacityHistoryRefusal {
    Capacity(crate::capacity::CapacityError),
    Scope(crate::scope::ScopeError),
    Completion(crate::completion::CompletionError),
    OwnedStorage(crate::owned_storage::OwnedStorageError),
    CapacityDenied {
        dimension: crate::identity::DimensionId,
        keyed: Option<(crate::identity::FamilyId, crate::identity::CapacityKey)>,
        requested: u64,
        committed: u64,
        limit: u64,
    },
    UnknownScope,
    DuplicateScope,
    UnknownSlot,
    DuplicateSlot,
    ScopeNotRetired,
    UnresolvedWork,
    IdentityExhausted,
}
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum HistoryFact {
    CapacityWaitRegistered {
        registrant: crate::identity::CapacityRegistrantId,
        target: crate::capacity::CapacityWaitTarget,
    },
    CapacityWaitNotified {
        registrant: crate::identity::CapacityRegistrantId,
        target: crate::capacity::CapacityWaitTarget,
    },
    CapacityRefused {
        request: CapacityHistoryRequest,
        reason: CapacityHistoryRefusal,
    },
    Effect {
        target: HistoryActorTarget,
        input: InputId,
        action: ActionId,
        /// The original actor batch's epoch, even for late completion after
        /// actor failure. This does not belong to the financial Work's Scope.
        actor_generation: Generation,
        financial_work: ScopedWorkRef,
        /// The original financial-author epoch frozen in the actual ticket.
        /// A late completion does not refresh it from the current ledger.
        author_generation: Generation,
        phase: EffectHistoryPhase,
    },
    ReservationRetired {
        reservation: ReservationId,
    },
    CapacitySplit {
        from: ReservationId,
        to: ReservationId,
    },
    UnboundReservationRolledBack {
        scope: ScopeId,
        work: WorkId,
        reservation: ReservationId,
    },
    ScopeOpened {
        scope: ScopeId,
        generation: Generation,
        parent: Option<ScopedWorkRef>,
    },
    ScopeRemoved {
        scope: ScopeId,
    },
    WorkUnresolved {
        scope: ScopeId,
        work: WorkId,
    },
    ScopeClosed {
        scope: ScopeId,
    },
    ScopeStopped {
        scope: ScopeId,
    },
    WorkAdmitted {
        scope: ScopeId,
        work: WorkId,
        parent: Option<ScopedWorkRef>,
    },
    WorkRetired {
        scope: ScopeId,
        work: WorkId,
    },
    SlotReserved {
        scope: ScopeId,
        slot: SlotId,
    },
    SlotFilled {
        slot: SlotId,
    },
    SlotAbandoned {
        slot: SlotId,
        consumer: ConsumerId,
    },
    SlotRetired {
        slot: SlotId,
    },
    ReturnClaimed {
        slot: SlotId,
    },
    ReturnFilled {
        slot: SlotId,
    },
    ReturnTaken {
        slot: SlotId,
    },
    ReturnRevokedUnused {
        slot: SlotId,
    },
    ReturnUnknown {
        slot: SlotId,
    },
    Admitted {
        scope: ScopeId,
        work: WorkId,
        reservation: ReservationId,
        parent: Option<ScopedWorkRef>,
    },
    /// The same reservation moves to another existing Work. Neither Work
    /// identity is minted, moved, exited or retired by this transfer.
    ReservationTransferred {
        reservation: ReservationId,
        from: ScopedWorkRef,
        to: ScopedWorkRef,
    },
    Claimed {
        target: HistoryActorTarget,
    },
    InputConsumed {
        target: HistoryActorTarget,
        input: InputId,
    },
    InputAbandoned {
        target: HistoryActorTarget,
        input: InputId,
    },
    BatchCommitted {
        target: HistoryActorTarget,
        actor_generation: Generation,
        input: InputId,
    },
    FailurePublication {
        target: HistoryActorTarget,
        actor_generation: Generation,
        financial_work: ScopedWorkRef,
        financial_generation: Generation,
        action: ActionId,
        origin: FailurePublicationHistoryOrigin,
    },
    Started {
        scope: ScopeId,
        work: WorkId,
    },
    BodyExited {
        scope: ScopeId,
        work: WorkId,
    },
    CapturesExited {
        scope: ScopeId,
        work: WorkId,
    },
    CompletionConsumed {
        slot: SlotId,
        consumer: ConsumerId,
    },
    OwnerFailed {
        scope: ScopeId,
        generation: Generation,
    },
    CapacityReleased {
        reservation: ReservationId,
    },
}
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct HistoryEntry {
    pub ordinal: u64,
    pub now: Tick,
    pub fact: HistoryFact,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum HistoryError {
    Full,
    OrdinalExhausted,
    TimeWentBackwards,
}
#[derive(Debug)]
pub struct HistoryBuffer {
    limit: usize,
    next_ordinal: u64,
    now: Tick,
    entries: VecDeque<HistoryEntry>,
}
impl HistoryBuffer {
    pub fn new(limit: usize) -> Self {
        Self {
            limit,
            next_ordinal: 1,
            now: Tick(0),
            entries: VecDeque::new(),
        }
    }
    /// Full buffers are drained by the observer; history pressure never changes policy.
    /// Drivers may disable collection instead, but cannot silently claim a complete history.
    pub fn record(&mut self, now: Tick, fact: HistoryFact) -> Result<(), HistoryError> {
        if now < self.now {
            return Err(HistoryError::TimeWentBackwards);
        }
        if self.entries.len() >= self.limit {
            return Err(HistoryError::Full);
        }
        let next = self
            .next_ordinal
            .checked_add(1)
            .ok_or(HistoryError::OrdinalExhausted)?;
        self.entries.push_back(HistoryEntry {
            ordinal: self.next_ordinal,
            now,
            fact,
        });
        self.now = now;
        self.next_ordinal = next;
        Ok(())
    }
    pub fn pop(&mut self) -> Option<HistoryEntry> {
        self.entries.pop_front()
    }
    pub fn len(&self) -> usize {
        self.entries.len()
    }
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }
}
