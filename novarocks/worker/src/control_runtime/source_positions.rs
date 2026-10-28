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

//! Actual prepaid source positions. The ingress owns this routing container
//! under its existing cut lock. Claim/install/take only move metadata and the
//! original owners. Binding, unused exit, report consumption and reaping run
//! on the real host after releasing that lock. The caller places its original
//! ready guard before the cut guard, so a successful bind or abandoned claim
//! reaches the live custodian after unlocking; this container never wakes one.

mod storage;
pub(crate) use storage::{
    SOURCE_SLOT_STORAGE, SourceSlotAdmission, SourceSlotAdmissionRefusal, SourceSlotStorageError,
};

use super::source_custody::{SourceCustody, SourcePermit};
use crate::task_port::TaskPendingNotice;
use novarocks_control_runtime::exit_role::ExitRolePhase;
use novarocks_control_runtime::{reported_work::ReportedWork, transaction::RuntimeError};
use novarocks_execution::runtime::{execution_health::ExecutionHealth, fragment::ExecutionFailure};
use novarocks_execution_contract::{RetainedTaskSource, TaskFailurePhase, TaskIdentity};
use novarocks_worker_protocol::proposal_summary::SourceId;
use std::{
    collections::BTreeMap,
    mem::ManuallyDrop,
    sync::{Arc, Mutex},
};
use storage::SourceSlotStorage;

type RetainedSource = RetainedTaskSource<ExecutionFailure>;

/// An address, never work/exit authority. One generation occupies this exact
/// position until its original Work header is actually reaped.
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub(crate) struct SourcePositionId(usize);

/// Addresses one original bank/position generation. Clones observe that same
/// admitted role; they cannot authorize a replacement role at the same index.
#[derive(Clone)]
pub(crate) struct SourcePositionKey {
    position: Arc<Position>,
}

/// A bounded observation of the original slot and role, never another
/// obligation record. Ledger/report operations on `custody` run only after
/// releasing the ingress fence which produced this observation.
pub(crate) struct SourcePositionObservation {
    pub key: SourcePositionKey,
    pub source: Option<SourceId>,
    pub phase: SourcePositionPhase,
    pub custody: SourceCustody,
}
impl SourcePositionKey {
    pub(crate) fn address(&self) -> SourcePositionId {
        self.position.id
    }
    pub(crate) fn same_original(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.position, &other.position)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum SourcePositionPhase {
    Available,
    Pending,
    Ready,
    Taken,
    Unused,
    Abandoned,
    Retired,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum SourcePositionError {
    Full,
    UnknownPosition,
    UnknownSource,
    InvalidPair,
    DuplicateSource,
    ForeignInventory,
    WrongGeneration,
    NotAvailable,
    NotReady,
    SourceAlreadyTaken,
    SourceUnresolved,
    SourceStillHeld,
    IdentityExhausted,
    SlotDetached,
}

struct InventoryIdentity {
    task: TaskIdentity,
}

enum Payload {
    Available(ManuallyDrop<SourcePermit>),
    Pending,
    Ready(ManuallyDrop<RetainedSource>),
    Taken,
    Unused,
    Abandoned {
        permit: Option<ManuallyDrop<SourcePermit>>,
        source: Option<ManuallyDrop<RetainedSource>>,
    },
    Retired,
}
impl Payload {
    fn phase(&self) -> SourcePositionPhase {
        match self {
            Self::Available(_) => SourcePositionPhase::Available,
            Self::Pending => SourcePositionPhase::Pending,
            Self::Ready(_) => SourcePositionPhase::Ready,
            Self::Taken => SourcePositionPhase::Taken,
            Self::Unused => SourcePositionPhase::Unused,
            Self::Abandoned { .. } => SourcePositionPhase::Abandoned,
            Self::Retired => SourcePositionPhase::Retired,
        }
    }
    fn permits_retirement(&self) -> bool {
        matches!(self, Self::Taken | Self::Unused | Self::Retired)
    }
}
struct PositionState {
    source: Option<SourceId>,
    payload: Payload,
    // Old reap witnesses may outlive a later generation's removal. This
    // original row can authorize a replacement only once, even when its
    // numeric address is absent from the resident bank again.
    replacement_issued: bool,
}

/// This is the original payload holder, not a second quota or work ledger.
/// Its self-pin retains the exact opaque value/unused permit on unknown exit.
/// The normal host keeps its inventory and explicitly disposes these values;
/// self-retention is not a replacement recovery registry.
struct Position {
    identity: Arc<InventoryIdentity>,
    id: SourcePositionId,
    generation: u64,
    custody: SourceCustody,
    storage: Mutex<Option<SourceSlotStorage>>,
    state: Mutex<PositionState>,
    retained: Mutex<Option<Arc<Self>>>,
}
impl Position {
    fn from_admitted(
        identity: Arc<InventoryIdentity>,
        id: SourcePositionId,
        generation: u64,
        reported: ReportedWork,
        storage: SourceSlotStorage,
        pending: Arc<dyn TaskPendingNotice>,
        health: Arc<ExecutionHealth>,
    ) -> Arc<Self> {
        let (custody, permit) =
            SourceCustody::from_reported(identity.task, reported, pending, health);
        let position = Arc::new(Self {
            identity,
            id,
            generation,
            custody,
            storage: Mutex::new(Some(storage)),
            state: Mutex::new(PositionState {
                source: None,
                payload: Payload::Available(ManuallyDrop::new(permit)),
                replacement_issued: false,
            }),
            retained: Mutex::new(None),
        });
        *position.retained.lock().expect("source position retention") = Some(position.clone());
        position
    }
    fn note_abandoned(&self) {
        {
            let mut state = self.state.lock().expect("original source position");
            let previous = std::mem::replace(&mut state.payload, Payload::Taken);
            state.payload = match previous {
                Payload::Available(permit) => Payload::Abandoned {
                    permit: Some(permit),
                    source: None,
                },
                Payload::Ready(source) => Payload::Abandoned {
                    permit: None,
                    source: Some(source),
                },
                Payload::Abandoned { permit, source } => Payload::Abandoned { permit, source },
                Payload::Retired => Payload::Retired,
                Payload::Pending | Payload::Taken | Payload::Unused => Payload::Abandoned {
                    permit: None,
                    source: None,
                },
            };
        }
        self.custody.note_inventory_abandoned();
    }
    fn return_abandoned_source(&self, source: RetainedSource) {
        let mut incoming = Some(ManuallyDrop::new(source));
        self.note_abandoned();
        let mut state = self.state.lock().expect("original source position");
        let Payload::Abandoned { source: saved, .. } = &mut state.payload else {
            unreachable!("the unique source returns to its original abandoned position");
        };
        assert!(saved.is_none(), "one original source per position");
        *saved = incoming.take();
    }
    fn return_abandoned_permit(&self, permit: SourcePermit) {
        let mut incoming = Some(ManuallyDrop::new(permit));
        self.note_abandoned();
        let mut state = self.state.lock().expect("original source position");
        let Payload::Abandoned { permit: saved, .. } = &mut state.payload else {
            unreachable!("the unique unused permit returns to its original position");
        };
        assert!(saved.is_none(), "one original permit per position");
        *saved = incoming.take();
    }
    fn release_retention(&self) {
        let retained = self
            .retained
            .lock()
            .expect("source position retention")
            .take();
        drop(retained);
    }
}

/// Only actual supplied roles add positions. `limit` bounds resident rows,
/// independently of monotonic fresh addresses. It issues no funding and
/// does not preallocate a per-Task maximum-cut pool.
#[must_use = "dispose original source/unused holders before retiring their inventory"]
pub(crate) struct SourcePositions {
    identity: Arc<InventoryIdentity>,
    // Only current resident rows are retained here. A Task must not cache
    // its historical peak of shared RPC positions after their true release.
    positions: BTreeMap<SourcePositionId, Arc<Position>>,
    next_position: usize,
    sources: BTreeMap<SourceId, SourcePositionId>,
    limit: usize,
    pending: Arc<dyn TaskPendingNotice>,
    health: Arc<ExecutionHealth>,
}
impl SourcePositions {
    pub(crate) fn new(
        task: TaskIdentity,
        limit: usize,
        pending: Arc<dyn TaskPendingNotice>,
        health: Arc<ExecutionHealth>,
    ) -> Self {
        Self {
            identity: Arc::new(InventoryIdentity { task }),
            positions: BTreeMap::new(),
            next_position: 0,
            sources: BTreeMap::new(),
            limit,
            pending,
            health,
        }
    }
    pub(crate) fn len(&self) -> usize {
        self.positions.len()
    }
    pub(crate) fn append_admitted(
        &mut self,
        role: SourceSlotAdmission,
    ) -> Result<SourcePositionKey, (SourcePositionError, SourceSlotAdmission)> {
        if self.positions.len() >= self.limit {
            return Err((SourcePositionError::Full, role));
        }
        let Some(next) = self.next_position.checked_add(1) else {
            return Err((SourcePositionError::IdentityExhausted, role));
        };
        let id = SourcePositionId(self.next_position);
        let (reported, storage) = role.into_slot_parts();
        let position = Position::from_admitted(
            self.identity.clone(),
            id,
            1,
            reported,
            storage,
            self.pending.clone(),
            self.health.clone(),
        );
        self.positions.insert(id, position.clone());
        self.next_position = next;
        Ok(SourcePositionKey { position })
    }
    pub(crate) fn append_pair(
        &mut self,
        roles: [SourceSlotAdmission; 2],
    ) -> Result<SourcePositionPair, (SourcePositionError, [SourceSlotAdmission; 2])> {
        if self
            .positions
            .len()
            .checked_add(2)
            .is_none_or(|n| n > self.limit)
        {
            return Err((SourcePositionError::Full, roles));
        }
        if self.next_position.checked_add(2).is_none() {
            return Err((SourcePositionError::IdentityExhausted, roles));
        }
        let [first, second] = roles;
        // The complete pair was checked before moving either original role.
        let first = self
            .append_admitted(first)
            .unwrap_or_else(|_| unreachable!());
        let second = self
            .append_admitted(second)
            .unwrap_or_else(|_| unreachable!());
        Ok(SourcePositionPair {
            identity: self.identity.clone(),
            positions: [first, second],
        })
    }
    fn position(&self, id: SourcePositionId) -> Result<&Arc<Position>, SourcePositionError> {
        self.positions
            .get(&id)
            .ok_or(SourcePositionError::UnknownPosition)
    }
    fn validate_key(&self, key: &SourcePositionKey) -> Result<&Arc<Position>, SourcePositionError> {
        if !Arc::ptr_eq(&self.identity, &key.position.identity) {
            return Err(SourcePositionError::ForeignInventory);
        }
        let current = self.position(key.position.id)?;
        if !Arc::ptr_eq(current, &key.position) {
            return Err(SourcePositionError::WrongGeneration);
        }
        Ok(current)
    }
    /// A released original may still have a real external observer or unique
    /// pair owner. Inspecting that retired row does not recreate a resident
    /// slot or grant access to any live replacement generation.
    fn observe_original(
        &self,
        key: &SourcePositionKey,
    ) -> Result<Arc<Position>, SourcePositionError> {
        if !Arc::ptr_eq(&self.identity, &key.position.identity) {
            return Err(SourcePositionError::ForeignInventory);
        }
        if let Some(current) = self.positions.get(&key.position.id) {
            if !Arc::ptr_eq(current, &key.position) {
                return Err(SourcePositionError::WrongGeneration);
            }
            return Ok(current.clone());
        }
        if !matches!(
            key.position
                .state
                .lock()
                .expect("original source position")
                .payload,
            Payload::Retired
        ) {
            return Err(SourcePositionError::UnknownPosition);
        }
        Ok(key.position.clone())
    }
    pub(crate) fn inspect(
        &self,
        key: &SourcePositionKey,
    ) -> Result<SourcePositionObservation, SourcePositionError> {
        let position = self.observe_original(key)?;
        let state = position.state.lock().expect("original source position");
        Ok(SourcePositionObservation {
            key: key.clone(),
            source: state.source,
            phase: state.payload.phase(),
            custody: position.custody.clone(),
        })
    }
    pub(crate) fn source_inventory(
        &self,
        source: SourceId,
    ) -> Result<SourcePositionObservation, SourcePositionError> {
        let id = *self
            .sources
            .get(&source)
            .ok_or(SourcePositionError::UnknownSource)?;
        self.inspect(&SourcePositionKey {
            position: self.position(id)?.clone(),
        })
    }
    /// Moving an original, never-claimed pair transfers its existing permits;
    /// it does not reserve capacity or reset a used position. This predicate
    /// is sampled under the same ingress fence as that ownership transfer.
    pub(crate) fn validate_unclaimed_pair(
        &self,
        pair: &SourcePositionPair,
    ) -> Result<(), SourcePositionError> {
        if !Arc::ptr_eq(&self.identity, &pair.identity) {
            return Err(SourcePositionError::ForeignInventory);
        }
        if pair.positions[0].address() == pair.positions[1].address() {
            return Err(SourcePositionError::InvalidPair);
        }
        for key in &pair.positions {
            let position = self.validate_key(key)?;
            let available = matches!(
                &position
                    .state
                    .lock()
                    .expect("original source position")
                    .payload,
                Payload::Available(_)
            );
            if !available {
                return Err(SourcePositionError::NotAvailable);
            }
            if !position.custody.unused_source_ready() {
                return Err(SourcePositionError::SourceUnresolved);
            }
        }
        Ok(())
    }
    pub(crate) fn phase(
        &self,
        id: SourcePositionId,
    ) -> Result<SourcePositionPhase, SourcePositionError> {
        Ok(self
            .position(id)?
            .state
            .lock()
            .expect("original source position")
            .payload
            .phase())
    }
    /// A clone addresses the existing role. Its ledger/report operations must
    /// run after unlocking the ingress; this getter performs none of them.
    pub(crate) fn custody(
        &self,
        id: SourcePositionId,
    ) -> Result<SourceCustody, SourcePositionError> {
        Ok(self.position(id)?.custody.clone())
    }
    pub(crate) fn claim(
        &mut self,
        key: &SourcePositionKey,
        source: SourceId,
        phase: TaskFailurePhase,
    ) -> Result<SourceClaim, SourcePositionError> {
        if self.sources.contains_key(&source) {
            return Err(SourcePositionError::DuplicateSource);
        }
        let position = self.validate_key(key)?.clone();
        let id = position.id;
        let mut state = position.state.lock().expect("original source position");
        let Payload::Available(permit) = &mut state.payload else {
            return Err(SourcePositionError::NotAvailable);
        };
        // Move to Pending before claiming. An unwind therefore leaves the
        // original position visible and never turns this holder into unused.
        let permit = unsafe { ManuallyDrop::take(permit) };
        state.source = Some(source);
        state.payload = Payload::Pending;
        self.sources.insert(source, id);
        drop(state);
        let mut claim = SourceClaim {
            position,
            source,
            phase,
            permit: Some(permit),
            active: true,
        };
        // Install the guard before a rejected claim can unwind. In particular
        // an already-unknown, not-yet-bound permit must not take its ordinary
        // unused-exit path while the caller still holds the cut lock.
        claim
            .permit
            .as_mut()
            .expect("original source permit")
            .claim(phase);
        Ok(claim)
    }
    pub(crate) fn install(
        &mut self,
        mut bound: BoundSource,
    ) -> Result<(), (SourcePositionError, BoundSource)> {
        if !Arc::ptr_eq(&self.identity, &bound.position.identity) {
            return Err((SourcePositionError::ForeignInventory, bound));
        }
        let Some(position) = self.positions.get(&bound.position.id) else {
            return Err((SourcePositionError::UnknownPosition, bound));
        };
        if !Arc::ptr_eq(position, &bound.position) {
            return Err((SourcePositionError::WrongGeneration, bound));
        }
        let mut state = position.state.lock().expect("original source position");
        if state.source != Some(bound.source) || !matches!(state.payload, Payload::Pending) {
            drop(state);
            return Err((SourcePositionError::NotReady, bound));
        }
        if !position.custody.retained_source_ready() {
            drop(state);
            return Err((SourcePositionError::SourceUnresolved, bound));
        }
        let source = unsafe { ManuallyDrop::take(&mut bound.source_value) };
        bound.active = false;
        state.payload = Payload::Ready(ManuallyDrop::new(source));
        Ok(())
    }
    pub(crate) fn is_ready(&self, source: SourceId) -> bool {
        self.sources.get(&source).is_some_and(|id| {
            let Some(position) = self.positions.get(id) else {
                return false;
            };
            let state = position.state.lock().expect("original source position");
            state.source == Some(source)
                && matches!(state.payload, Payload::Ready(_))
                && position.custody.retained_source_ready()
        })
    }
    pub(crate) fn take_source(
        &mut self,
        source: SourceId,
    ) -> Result<RetainedSource, SourcePositionError> {
        let id = self
            .sources
            .get(&source)
            .copied()
            .ok_or(SourcePositionError::UnknownSource)?;
        let position = self.position(id)?;
        let mut state = position.state.lock().expect("original source position");
        match &mut state.payload {
            Payload::Ready(value) => {
                if !position.custody.retained_source_ready() {
                    return Err(SourcePositionError::SourceUnresolved);
                }
                let source = unsafe { ManuallyDrop::take(value) };
                state.payload = Payload::Taken;
                Ok(source)
            }
            Payload::Taken => Err(SourcePositionError::SourceAlreadyTaken),
            Payload::Abandoned { .. } => Err(SourcePositionError::SourceUnresolved),
            _ => Err(SourcePositionError::NotReady),
        }
    }
    /// The failed owner can still take the exact source returned by a late
    /// binder. Taking it signs no Core consumption, source exit or retirement.
    pub(crate) fn take_for_disposal(
        &mut self,
        key: &SourcePositionKey,
    ) -> Result<RetainedSource, SourcePositionError> {
        let mut state = self
            .validate_key(key)?
            .state
            .lock()
            .expect("original source position");
        match &mut state.payload {
            Payload::Ready(value) => {
                let source = unsafe { ManuallyDrop::take(value) };
                state.payload = Payload::Taken;
                Ok(source)
            }
            Payload::Abandoned { source, .. } => source
                .take()
                .map(ManuallyDrop::into_inner)
                .ok_or(SourcePositionError::NotReady),
            Payload::Taken => Err(SourcePositionError::SourceAlreadyTaken),
            _ => Err(SourcePositionError::NotReady),
        }
    }
    pub(crate) fn take_unused(
        &mut self,
        key: &SourcePositionKey,
    ) -> Result<UnusedPosition, SourcePositionError> {
        let position = self.validate_key(key)?.clone();
        let mut state = position.state.lock().expect("original source position");
        let Payload::Available(permit) = &mut state.payload else {
            return Err(SourcePositionError::NotAvailable);
        };
        let permit = unsafe { ManuallyDrop::take(permit) };
        state.payload = Payload::Unused;
        drop(state);
        Ok(UnusedPosition {
            position,
            permit: ManuallyDrop::new(permit),
            active: true,
        })
    }
    pub(crate) fn retirement_probe(
        &self,
        id: SourcePositionId,
    ) -> Result<RetirementProbe, SourcePositionError> {
        Ok(RetirementProbe {
            position: self.position(id)?.clone(),
        })
    }
    pub(crate) fn retirement_probe_for(
        &self,
        key: &SourcePositionKey,
    ) -> Result<RetirementProbe, SourcePositionError> {
        Ok(RetirementProbe {
            position: self.observe_original(key)?,
        })
    }
    fn validate_reaped(
        &self,
        reaped: &ReapedPosition,
    ) -> Result<Arc<Position>, SourcePositionError> {
        let current = self.observe_original(&SourcePositionKey {
            position: reaped.position.clone(),
        })?;
        let state = current.state.lock().expect("original source position");
        if state.replacement_issued {
            return Err(SourcePositionError::WrongGeneration);
        }
        if !state.payload.permits_retirement() {
            return Err(SourcePositionError::SourceStillHeld);
        }
        drop(state);
        if current
            .storage
            .lock()
            .expect("source slot storage")
            .is_none()
        {
            return Err(SourcePositionError::SlotDetached);
        }
        Ok(current)
    }
    /// Retire only the actual generation. Its row and unique original fee
    /// remain resident so a later validated generation can reuse this slot.
    pub(crate) fn mark_generation_retired(
        &mut self,
        reaped: ReapedPosition,
    ) -> Result<(), (SourcePositionError, ReapedPosition)> {
        let current = match self.validate_reaped(&reaped) {
            Ok(current) => current,
            Err(error) => return Err((error, reaped)),
        };
        let source = {
            let mut state = current.state.lock().expect("original source position");
            state.payload = Payload::Retired;
            state.source.take()
        };
        if let Some(source) = source {
            self.sources.remove(&source);
        }
        Ok(())
    }
    /// The witness was observed against this exact original role outside the
    /// ingress lock. No ledger inspection or opaque destructor occurs here.
    pub(crate) fn release_reaped(
        &mut self,
        reaped: ReapedPosition,
    ) -> Result<DetachedSourceSlotStorage, (SourcePositionError, ReapedPosition)> {
        let current = match self.validate_reaped(&reaped) {
            Ok(current) => current,
            Err(error) => return Err((error, reaped)),
        };
        let source = {
            let mut state = current.state.lock().expect("original source position");
            state.payload = Payload::Retired;
            state.source.take()
        };
        if let Some(source) = source {
            self.sources.remove(&source);
        }
        self.positions.remove(&current.id);
        let storage = current
            .storage
            .lock()
            .expect("source slot storage")
            .take()
            .expect("same validated original slot storage");
        current.release_retention();
        Ok(DetachedSourceSlotStorage { storage })
    }
    /// Supply a new original role only after the old role/report AND
    /// common Work header have retired. The original role is returned intact
    /// on every refusal; this function moves the same original slot fee and
    /// never obtains replacement capacity.
    pub(crate) fn replace_after_reap(
        &mut self,
        reaped: ReapedPosition,
        role: ReportedWork,
    ) -> Result<SourcePositionKey, (SourcePositionError, ReportedWork)> {
        let old = match self.validate_reaped(&reaped) {
            Ok(position) => position,
            Err(error) => return Err((error, role)),
        };
        if !old
            .storage
            .lock()
            .expect("source slot storage")
            .as_ref()
            .expect("same validated original slot storage")
            .matches_scope(role.work().scope())
        {
            return Err((SourcePositionError::ForeignInventory, role));
        }
        let Some(generation) = old.generation.checked_add(1) else {
            return Err((SourcePositionError::IdentityExhausted, role));
        };
        if !self.positions.contains_key(&old.id) && self.positions.len() >= self.limit {
            return Err((SourcePositionError::Full, role));
        }
        let storage = old
            .storage
            .lock()
            .expect("source slot storage")
            .take()
            .expect("same validated original slot storage");
        let replacement = Position::from_admitted(
            self.identity.clone(),
            old.id,
            generation,
            role,
            storage,
            self.pending.clone(),
            self.health.clone(),
        );
        let source = {
            let mut state = old.state.lock().expect("original source position");
            state.replacement_issued = true;
            state.payload = Payload::Retired;
            state.source.take()
        };
        if let Some(source) = source {
            self.sources.remove(&source);
        }
        self.positions.insert(old.id, replacement.clone());
        old.release_retention();
        Ok(SourcePositionKey {
            position: replacement,
        })
    }

    /// Re-arm the same two addresses only after both original roles were
    /// actually reaped. All validation precedes consuming either new role;
    /// failure returns the complete original group without partial mutation.
    /// The host obtains the witnesses and new roles outside the ingress lock.
    pub(crate) fn rearm_pair(
        &mut self,
        pair: SourcePositionPair,
        reaped: [ReapedPosition; 2],
        roles: [ReportedWork; 2],
    ) -> Result<
        SourcePositionPair,
        (
            SourcePositionError,
            SourcePositionPair,
            [ReapedPosition; 2],
            [ReportedWork; 2],
        ),
    > {
        let validation = (|| {
            self.validate_reaped_pair(&pair, &reaped)?;
            for (key, reported) in pair.positions.iter().zip(&roles) {
                if !key
                    .position
                    .storage
                    .lock()
                    .expect("source slot storage")
                    .as_ref()
                    .expect("same validated original pair storage")
                    .matches_scope(reported.work().scope())
                {
                    return Err(SourcePositionError::ForeignInventory);
                }
            }
            let missing = pair
                .positions
                .iter()
                .filter(|key| !self.positions.contains_key(&key.address()))
                .count();
            if self
                .positions
                .len()
                .checked_add(missing)
                .is_none_or(|n| n > self.limit)
            {
                return Err(SourcePositionError::Full);
            }
            let mut generations = [0; 2];
            for (index, generation) in generations.iter_mut().enumerate() {
                let current = &pair.positions[index].position;
                *generation = current
                    .generation
                    .checked_add(1)
                    .ok_or(SourcePositionError::IdentityExhausted)?;
            }
            Ok(generations)
        })();
        let generations = match validation {
            Ok(generations) => generations,
            Err(error) => return Err((error, pair, reaped, roles)),
        };
        let [first, second] = roles;
        let replacements = [
            (first, &pair.positions[0], generations[0]),
            (second, &pair.positions[1], generations[1]),
        ]
        .map(|(role, key, generation)| {
            let storage = key
                .position
                .storage
                .lock()
                .expect("source slot storage")
                .take()
                .expect("same validated original pair storage");
            Position::from_admitted(
                self.identity.clone(),
                key.address(),
                generation,
                role,
                storage,
                self.pending.clone(),
                self.health.clone(),
            )
        });
        for (key, replacement) in pair.positions.iter().zip(&replacements) {
            let old = &key.position;
            let source = {
                let mut state = old.state.lock().expect("original source position");
                state.replacement_issued = true;
                state.payload = Payload::Retired;
                state.source.take()
            };
            if let Some(source) = source {
                self.sources.remove(&source);
            }
            self.positions.insert(old.id, replacement.clone());
        }
        for key in &pair.positions {
            key.position.release_retention();
        }
        Ok(SourcePositionPair {
            identity: self.identity.clone(),
            positions: replacements.map(|position| SourcePositionKey { position }),
        })
    }

    /// Validate both original observations before a caller moves either slot.
    /// Each witness was obtained from the common reaper outside this fence;
    /// this checks only its exact bank, generation, payload and original Arc.
    pub(crate) fn validate_reaped_pair(
        &self,
        pair: &SourcePositionPair,
        reaped: &[ReapedPosition; 2],
    ) -> Result<(), SourcePositionError> {
        if !Arc::ptr_eq(&self.identity, &pair.identity) {
            return Err(SourcePositionError::ForeignInventory);
        }
        if pair.positions[0].address() == pair.positions[1].address() {
            return Err(SourcePositionError::InvalidPair);
        }
        for (key, witness) in pair.positions.iter().zip(reaped) {
            let current = self.observe_original(key)?;
            let old = self.validate_reaped(witness)?;
            if !Arc::ptr_eq(&current, &old) {
                return Err(SourcePositionError::WrongGeneration);
            }
        }
        Ok(())
    }
}
impl Drop for SourcePositions {
    fn drop(&mut self) {
        // Normal cleanup must explicitly move original holders out. A lost
        // inventory is an owner invariant failure, not a shutdown shortcut.
        for position in self.positions.values() {
            if position.custody.phase() == ExitRolePhase::Retired
                && position
                    .storage
                    .lock()
                    .expect("source slot storage")
                    .is_none()
            {
                position.release_retention();
            } else {
                position.note_abandoned();
            }
        }
    }
}

/// Final slot removal transfers this same unique payment out of the bank.
/// Only the caller outside the handoff lock may actually return its units.
#[must_use = "Release detached SourceSlot storage only after leaving the bank lock"]
pub(crate) struct DetachedSourceSlotStorage {
    storage: SourceSlotStorage,
}
impl DetachedSourceSlotStorage {
    pub(crate) fn release(&mut self) -> Result<(), RuntimeError> {
        self.storage.release()
    }
}
impl std::fmt::Debug for DetachedSourceSlotStorage {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DetachedSourceSlotStorage")
            .finish_non_exhaustive()
    }
}

/// Pairing is only routing metadata. Swapping it moves the same two paid
/// positions between open/consuming/cut owners and never grants new roles.
pub(crate) struct SourcePositionPair {
    identity: Arc<InventoryIdentity>,
    positions: [SourcePositionKey; 2],
}
impl SourcePositionPair {
    pub(crate) fn positions(&self) -> [SourcePositionId; 2] {
        self.positions.each_ref().map(SourcePositionKey::address)
    }
    pub(crate) fn keys(&self) -> [&SourcePositionKey; 2] {
        self.positions.each_ref()
    }
    pub(crate) fn swap_with(&mut self, other: &mut Self) -> Result<(), SourcePositionError> {
        if !Arc::ptr_eq(&self.identity, &other.identity) {
            return Err(SourcePositionError::ForeignInventory);
        }
        std::mem::swap(&mut self.positions, &mut other.positions);
        Ok(())
    }
}
impl std::fmt::Debug for SourcePositionPair {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SourcePositionPair")
            .field("positions", &self.positions)
            .finish()
    }
}

#[must_use = "bind outside the cut lock or preserve this original claim as unknown"]
pub(crate) struct SourceClaim {
    position: Arc<Position>,
    source: SourceId,
    phase: TaskFailurePhase,
    permit: Option<SourcePermit>,
    active: bool,
}
impl SourceClaim {
    pub(crate) fn source(&self) -> SourceId {
        self.source
    }
    pub(crate) fn position(&self) -> SourcePositionId {
        self.position.id
    }
    /// The original Work start and occurrence marker binding happen here,
    /// after releasing every ingress/cut lock.
    pub(crate) fn bind(mut self, original: ExecutionFailure) -> BoundSource {
        let retained = self
            .permit
            .take()
            .expect("original source permit")
            .retain(original, self.phase);
        self.active = false;
        BoundSource {
            position: self.position.clone(),
            source: self.source,
            source_value: ManuallyDrop::new(retained),
            active: true,
        }
    }
}
impl Drop for SourceClaim {
    fn drop(&mut self) {
        if self.active {
            if let Some(permit) = self.permit.take() {
                // Preserve even a not-yet-bound permit after a failed claim.
                // Its ordinary Drop can enter the ledger for unused exit.
                self.position.return_abandoned_permit(permit);
            } else {
                self.position.note_abandoned();
            }
        }
    }
}

#[must_use = "install the same source or dispose the entire retained carrier outside locks"]
pub(crate) struct BoundSource {
    position: Arc<Position>,
    source: SourceId,
    source_value: ManuallyDrop<RetainedSource>,
    active: bool,
}
impl BoundSource {
    pub(crate) fn source(&self) -> SourceId {
        self.source
    }
    pub(crate) fn position(&self) -> SourcePositionId {
        self.position.id
    }
    pub(crate) fn retained(&self) -> &RetainedSource {
        &self.source_value
    }
    pub(crate) fn into_retained(mut self) -> RetainedSource {
        {
            let mut state = self
                .position
                .state
                .lock()
                .expect("original source position");
            if matches!(state.payload, Payload::Pending) {
                state.payload = Payload::Taken;
            }
        }
        self.active = false;
        unsafe { ManuallyDrop::take(&mut self.source_value) }
    }
}
impl Drop for BoundSource {
    fn drop(&mut self) {
        if self.active {
            let source = unsafe { ManuallyDrop::take(&mut self.source_value) };
            self.position.return_abandoned_source(source);
        }
    }
}

#[must_use = "the original unused holder exits on its real host stack, outside ingress locks"]
pub(crate) struct UnusedPosition {
    position: Arc<Position>,
    permit: ManuallyDrop<SourcePermit>,
    active: bool,
}
impl UnusedPosition {
    pub(crate) fn exit(mut self) {
        struct ExitAttempt {
            position: Arc<Position>,
            completed: bool,
        }
        impl Drop for ExitAttempt {
            fn drop(&mut self) {
                if !self.completed {
                    self.position.note_abandoned();
                }
            }
        }
        let mut attempt = ExitAttempt {
            position: self.position.clone(),
            completed: false,
        };
        self.active = false;
        let permit = unsafe { ManuallyDrop::take(&mut self.permit) };
        drop(permit);
        attempt.completed = true;
    }
}
impl Drop for UnusedPosition {
    fn drop(&mut self) {
        if self.active {
            let permit = unsafe { ManuallyDrop::take(&mut self.permit) };
            self.position.return_abandoned_permit(permit);
        }
    }
}

/// Obtaining this probe is metadata-only; observing it touches the original
/// common ledger and therefore belongs outside the ingress lock.
pub(crate) struct RetirementProbe {
    position: Arc<Position>,
}
impl RetirementProbe {
    pub(crate) fn observe_reaped(self) -> Result<ReapedPosition, Self> {
        let no_payload = self
            .position
            .state
            .lock()
            .expect("original source position")
            .payload
            .permits_retirement();
        let storage_present = self
            .position
            .storage
            .lock()
            .expect("source slot storage")
            .is_some();
        if no_payload && storage_present && self.position.custody.reusable() {
            Ok(ReapedPosition {
                position: self.position,
            })
        } else {
            Err(self)
        }
    }
}
pub(crate) struct ReapedPosition {
    position: Arc<Position>,
}

macro_rules! metadata_debug {
    ($($name:ident),* $(,)?) => { $(
        impl std::fmt::Debug for $name {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                f.debug_struct(stringify!($name)).field("position", &self.position.id).field("generation", &self.position.generation).finish()
            }
        }
    )* };
}
metadata_debug!(
    SourcePositionKey,
    SourceClaim,
    BoundSource,
    UnusedPosition,
    RetirementProbe,
    ReapedPosition
);

#[cfg(test)]
mod tests {
    mod residency;
    mod slot_storage;
    use super::*;
    use crate::task_completion_roles::AdmittedCompletionRole;
    use novarocks_control_runtime::{
        completion::ReportSlot,
        lease::{CapacityBundle, WorkLease},
        transaction::{RuntimeError, RuntimeLedger, ScopeHandle},
    };
    use novarocks_control_runtime_core::{
        capacity::ResourceVector,
        completion::{CompletionError, ConsumerKind},
        identity::{ConsumerId, DimensionId, Generation},
        scope::{WorkAdmission, WorkKind},
    };
    use novarocks_execution_contract::{
        AcceptedConclusion, AcceptedTaskConclusion, TaskConclusion, TaskFailure, TerminationDetail,
        failure::{FailureClass, FailureFact, FailureProjection},
    };
    use novarocks_types::{
        AttemptId, BackendProcessId, QueryExecutionId, QueryId, StageId, TaskId,
    };
    use std::{
        num::NonZeroU64,
        panic::{AssertUnwindSafe, catch_unwind},
        sync::{
            Weak,
            atomic::{AtomicUsize, Ordering},
        },
    };

    // Empty-vector protocol fixtures explicitly opt into Model payment.
    fn reported(role: AdmittedCompletionRole) -> ReportedWork {
        super::super::admitted_role::adopt_reported_work(role.work, role.capacity, role.report)
    }
    fn model(role: AdmittedCompletionRole) -> SourceSlotAdmission {
        SourceSlotAdmission::protocol_model(reported(role))
    }
    fn raw_reported(role: ReportedWork) -> AdmittedCompletionRole {
        let (work, capacity, report) = role.into_parts();
        AdmittedCompletionRole {
            work,
            capacity,
            report,
        }
    }
    fn raw_admission(admission: SourceSlotAdmission) -> AdmittedCompletionRole {
        let (role, payment) = admission.into_originals();
        assert!(
            payment.is_none(),
            "this fixture explicitly uses Model payment"
        );
        raw_reported(role)
    }
    struct Pending(AtomicUsize);
    impl TaskPendingNotice for Pending {
        fn pending(&self) {
            self.0.fetch_add(1, Ordering::SeqCst);
        }
    }
    fn task() -> TaskIdentity {
        TaskIdentity::new(
            QueryExecutionId::new(QueryId::new(12501, 12502), AttemptId::new(1).unwrap()).unwrap(),
            StageId::new(1).unwrap(),
            TaskId::new(1).unwrap(),
            BackendProcessId::new_v7(),
        )
    }
    fn dimension() -> DimensionId {
        DimensionId::new(1).unwrap()
    }
    fn core() -> ConsumerId {
        ConsumerId::new(1).unwrap()
    }
    fn assert_unused_report(custody: &SourceCustody) {
        for consumer in [core(), host()] {
            assert_eq!(
                custody.report().consume(consumer),
                Err(RuntimeError::Completion(CompletionError::NotFilled)),
                "an unused role revokes its report and invents no consumer receipt"
            );
        }
    }
    fn host() -> ConsumerId {
        ConsumerId::new(2).unwrap()
    }
    #[test]
    fn inventory_observes_the_original_role_without_acknowledging_or_starting_it() {
        let mut f = Fixture::new(2);
        let roles = [f.role(), f.role()];
        let pair = f
            .bank_mut()
            .append_pair(roles.map(model))
            .unwrap_or_else(|_| panic!("original paid pair"));
        let key = pair.keys()[0].clone();
        let version = f.ledger.change_version();
        let initial = f.bank().inspect(&key).unwrap();
        assert_eq!(initial.key.address(), key.address());
        assert_eq!(initial.source, None);
        assert_eq!(initial.phase, SourcePositionPhase::Available);
        assert_eq!(initial.custody.phase(), ExitRolePhase::Admitted);
        f.bank().validate_unclaimed_pair(&pair).unwrap();
        assert_eq!(f.ledger.change_version(), version);

        let (original, dropped) = source(&f, key.address(), false);
        let claim = f
            .bank_mut()
            .claim(&key, SourceId(123), TaskFailurePhase::Execution)
            .unwrap();
        let version = f.ledger.change_version();
        let pending = f.bank().source_inventory(SourceId(123)).unwrap();
        assert_eq!(pending.source, Some(SourceId(123)));
        assert_eq!(pending.phase, SourcePositionPhase::Pending);
        assert_eq!(pending.custody.report().id(), initial.custody.report().id());
        assert_eq!(pending.custody.phase(), ExitRolePhase::Admitted);
        assert_eq!(
            f.bank().validate_unclaimed_pair(&pair),
            Err(SourcePositionError::NotAvailable)
        );
        assert_eq!(f.ledger.change_version(), version);
        let bound = claim.bind(original);
        f.bank_mut()
            .install(bound)
            .unwrap_or_else(|_| panic!("original retained source"));
        let retained = f.bank_mut().take_source(SourceId(123)).unwrap();
        assert_eq!(dropped.load(Ordering::SeqCst), 0);
        drop(retained);
        assert_eq!(dropped.load(Ordering::SeqCst), 1);
        f.finish();
    }
    struct Fixture {
        task: TaskIdentity,
        ledger: RuntimeLedger,
        scope: ScopeHandle,
        parent: WorkLease,
        funding: CapacityBundle,
        pending: Arc<Pending>,
        health: Arc<ExecutionHealth>,
        bank: Option<SourcePositions>,
    }
    impl Fixture {
        fn new(limit: usize) -> Self {
            let task = task();
            let generation = Generation::new(1).unwrap();
            let resources = ResourceVector::new([(dimension(), 1)]).unwrap();
            let ledger = RuntimeLedger::new(resources.clone(), 64, 1);
            let scope = ledger.open_scope_next(None, generation, 64, 64).unwrap();
            let (mut parent, funding) = scope
                .reserve_next(
                    generation,
                    resources,
                    WorkKind::Business,
                    WorkAdmission::External,
                )
                .unwrap()
                .bind(WorkKind::Business)
                .unwrap();
            parent.start().unwrap();
            let pending = Arc::new(Pending(AtomicUsize::new(0)));
            let health = Arc::new(ExecutionHealth::default());
            let bank = SourcePositions::new(task, limit, pending.clone(), health.clone());
            Self {
                task,
                ledger,
                scope,
                parent,
                funding,
                pending,
                health,
                bank: Some(bank),
            }
        }
        fn bank(&self) -> &SourcePositions {
            self.bank.as_ref().unwrap()
        }
        fn bank_mut(&mut self) -> &mut SourcePositions {
            self.bank.as_mut().unwrap()
        }
        fn role(&self) -> AdmittedCompletionRole {
            let generation = self.scope.gates().unwrap().generation;
            let guard = self
                .scope
                .reserve_child_next(
                    generation,
                    self.funding.id(),
                    ResourceVector::default(),
                    WorkKind::Cleanup,
                )
                .unwrap();
            let report = ReportSlot::reserve_next(
                &self.scope,
                generation,
                guard.work_id(),
                2,
                [
                    (
                        core(),
                        ConsumerKind::Owner {
                            scope: self.scope.id(),
                            generation,
                        },
                    ),
                    (host(), ConsumerKind::Observer),
                ],
            )
            .unwrap();
            let (work, capacity) = guard.bind(WorkKind::Cleanup).unwrap();
            AdmittedCompletionRole {
                work,
                capacity,
                report,
            }
        }
        fn append(&mut self) -> SourcePositionId {
            let role = self.role();
            self.bank_mut()
                .append_admitted(model(role))
                .map(|key| key.address())
                .unwrap_or_else(|_| panic!("original paid position"))
        }
        fn key(&self, id: SourcePositionId) -> SourcePositionKey {
            SourcePositionKey {
                position: self.bank().position(id).unwrap().clone(),
            }
        }
        fn claim(
            &mut self,
            id: SourcePositionId,
            source: SourceId,
            phase: TaskFailurePhase,
        ) -> Result<SourceClaim, SourcePositionError> {
            let key = self.key(id);
            self.bank_mut().claim(&key, source, phase)
        }
        fn take_unused(
            &mut self,
            id: SourcePositionId,
        ) -> Result<UnusedPosition, SourcePositionError> {
            let key = self.key(id);
            self.bank_mut().take_unused(&key)
        }
        fn take_for_disposal(
            &mut self,
            id: SourcePositionId,
        ) -> Result<RetainedSource, SourcePositionError> {
            let key = self.key(id);
            self.bank_mut().take_for_disposal(&key)
        }
        fn dispose_role(&self, role: AdmittedCompletionRole) {
            let (custody, permit) = SourceCustody::from_admitted(
                self.task,
                role,
                self.pending.clone(),
                self.health.clone(),
            );
            drop(permit);
            assert_unused_report(&custody);
            assert!(custody.try_retire().unwrap());
            assert_eq!(self.ledger.reap_scope_ready(self.scope.id(), 1).unwrap(), 1);
            assert!(custody.reusable());
        }
        fn reap_generation(&mut self, id: SourcePositionId) -> ReapedPosition {
            let custody = self.bank().custody(id).unwrap();
            if custody.phase() == ExitRolePhase::Retired {
                return self
                    .bank()
                    .retirement_probe(id)
                    .unwrap()
                    .observe_reaped()
                    .unwrap();
            }
            assert_eq!(custody.phase(), ExitRolePhase::Exited);
            if self.bank().phase(id).unwrap() == SourcePositionPhase::Unused {
                assert_unused_report(&custody);
            } else {
                assert!(!custody.try_retire().unwrap());
                custody.report().consume(core()).unwrap();
                assert!(
                    !custody.try_retire().unwrap(),
                    "host is an independent real consumer"
                );
                custody.report().consume(host()).unwrap();
            }
            assert!(custody.try_retire().unwrap());
            assert!(
                !custody.reusable(),
                "the report is retired, not its Work header"
            );
            assert!(
                self.bank()
                    .retirement_probe(id)
                    .unwrap()
                    .observe_reaped()
                    .is_err()
            );
            assert_eq!(self.ledger.reap_scope_ready(self.scope.id(), 1).unwrap(), 1);
            self.bank()
                .retirement_probe(id)
                .unwrap()
                .observe_reaped()
                .unwrap()
        }
        fn retire_generation(&mut self, id: SourcePositionId) {
            let reaped = self.reap_generation(id);
            self.bank_mut().mark_generation_retired(reaped).unwrap();
        }
        fn retire(&mut self, id: SourcePositionId) {
            let reaped = self.reap_generation(id);
            let mut detached = self.bank_mut().release_reaped(reaped).unwrap();
            detached.release().unwrap();
        }
        fn finish(mut self) {
            let resident = self.bank().positions.keys().copied().collect::<Vec<_>>();
            for id in resident {
                match self.bank().phase(id).unwrap() {
                    SourcePositionPhase::Available => self.take_unused(id).unwrap().exit(),
                    SourcePositionPhase::Taken
                    | SourcePositionPhase::Unused
                    | SourcePositionPhase::Retired => {}
                    phase => panic!("unexpected remaining source holder: {phase:?}"),
                }
                if self.bank().phase(id).unwrap() != SourcePositionPhase::Retired {
                    self.retire(id);
                } else {
                    let reaped = self
                        .bank()
                        .retirement_probe(id)
                        .unwrap()
                        .observe_reaped()
                        .unwrap();
                    let mut detached = self.bank_mut().release_reaped(reaped).unwrap();
                    detached.release().unwrap();
                }
            }
            self.parent.body_exited().unwrap();
            self.parent.captures_exited().unwrap();
            self.funding
                .release(&self.funding.resources().unwrap())
                .unwrap();
            assert_eq!(self.ledger.reap_scope_ready(self.scope.id(), 1).unwrap(), 1);
            self.scope.close_external().unwrap();
            self.ledger.remove_scope(self.scope.id()).unwrap();
            assert!(self.ledger.is_empty());
            assert!(self.health.failure().is_none());
        }
    }
    struct Original {
        dropped: Arc<AtomicUsize>,
        position: Weak<Position>,
        panic: bool,
    }
    impl std::fmt::Debug for Original {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.write_str("Original")
        }
    }
    impl std::fmt::Display for Original {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.write_str("original paid source")
        }
    }
    impl std::error::Error for Original {}
    impl FailureProjection for Original {
        fn failure_fact(&self) -> FailureFact {
            FailureFact::new(
                FailureClass::Data,
                crate::failure_reason::WorkerFailureReason::CompletionWorkUnresolved.reason(),
            )
        }
    }
    impl Drop for Original {
        fn drop(&mut self) {
            let position = self
                .position
                .upgrade()
                .expect("original position stays held");
            assert!(
                position.state.try_lock().is_ok(),
                "opaque source destruction is outside position locks"
            );
            self.dropped.fetch_add(1, Ordering::SeqCst);
            assert!(!self.panic, "original source destructor panic");
        }
    }
    fn source(
        f: &Fixture,
        id: SourcePositionId,
        panic: bool,
    ) -> (ExecutionFailure, Arc<AtomicUsize>) {
        let dropped = Arc::new(AtomicUsize::new(0));
        (
            ExecutionFailure::from_domain(Original {
                dropped: dropped.clone(),
                position: Arc::downgrade(f.bank().position(id).unwrap()),
                panic,
            }),
            dropped,
        )
    }
    fn install(
        f: &mut Fixture,
        id: SourcePositionId,
        source_id: SourceId,
        source: ExecutionFailure,
    ) {
        let claim = f.claim(id, source_id, TaskFailurePhase::Execution).unwrap();
        let bound = claim.bind(source);
        f.bank_mut().install(bound).unwrap();
    }
    #[test]
    fn pending_requires_lock_free_original_binding_and_take_returns_the_entire_carrier() {
        let mut f = Fixture::new(1);
        let id = f.append();
        let (original, dropped) = source(&f, id, false);
        let original_ptr = original.cause() as *const _;
        let fact = original.failure_fact();
        let custody = f.bank().custody(id).unwrap();
        let cut = Mutex::new(());
        let version = f.ledger.change_version();
        let claim = {
            let _cut = cut.lock().unwrap();
            let claim = f
                .claim(id, SourceId(1), TaskFailurePhase::Preparation)
                .unwrap();
            assert_eq!(claim.position(), id);
            assert_eq!(claim.source(), SourceId(1));
            assert!(!f.bank().is_ready(SourceId(1)));
            assert!(matches!(
                f.bank_mut().take_source(SourceId(1)),
                Err(SourcePositionError::NotReady)
            ));
            claim
        };
        assert_eq!(f.ledger.change_version(), version);
        assert_eq!(custody.phase(), ExitRolePhase::Admitted);
        let bound = claim.bind(original);
        assert_eq!(bound.retained().cause().cause() as *const _, original_ptr);
        let version = f.ledger.change_version();
        {
            let _cut = cut.lock().unwrap();
            f.bank_mut().install(bound).unwrap();
            assert!(f.bank().is_ready(SourceId(1)));
        }
        assert_eq!(f.ledger.change_version(), version);
        let retained = f.bank_mut().take_source(SourceId(1)).unwrap();
        assert_eq!(retained.cause().cause() as *const _, original_ptr);
        assert_eq!(retained.cause().failure_fact(), fact);
        assert_eq!(f.bank().phase(id).unwrap(), SourcePositionPhase::Taken);
        assert!(matches!(
            f.bank_mut().take_source(SourceId(1)),
            Err(SourcePositionError::SourceAlreadyTaken)
        ));
        assert!(!custody.reusable());
        drop(retained);
        assert_eq!(dropped.load(Ordering::SeqCst), 1);
        f.finish();
    }
    #[test]
    fn foreign_inventory_refusal_preserves_original_source_and_both_capture_legs() {
        let mut f = Fixture::new(2);
        let mut foreign = Fixture::new(1);
        let id = f.append();
        let (original, dropped) = source(&f, id, false);
        let pointer = original.cause() as *const _;
        let bound = f
            .claim(id, SourceId(1), TaskFailurePhase::Execution)
            .unwrap()
            .bind(original);
        let (error, bound) = foreign.bank_mut().install(bound).unwrap_err();
        assert_eq!(error, SourcePositionError::ForeignInventory);
        assert_eq!(bound.retained().cause().cause() as *const _, pointer);
        assert_eq!(bound.position(), id);
        assert_eq!(bound.source(), SourceId(1));
        assert_eq!(dropped.load(Ordering::SeqCst), 0);
        f.bank_mut().install(bound).unwrap();
        drop(f.bank_mut().take_source(SourceId(1)).unwrap());
        assert_eq!(dropped.load(Ordering::SeqCst), 1);
        let second = f.append();
        let (original, second_dropped) = source(&f, second, false);
        let pointer = original.cause() as *const _;
        let bound = f
            .claim(second, SourceId(2), TaskFailurePhase::Execution)
            .unwrap()
            .bind(original);
        let (_, bound) = foreign.bank_mut().install(bound).unwrap_err();
        let retained = bound.into_retained();
        assert_eq!(retained.cause().cause() as *const _, pointer);
        assert_eq!(f.bank().phase(second).unwrap(), SourcePositionPhase::Taken);
        assert_eq!(second_dropped.load(Ordering::SeqCst), 0);
        drop(retained);
        assert_eq!(second_dropped.load(Ordering::SeqCst), 1);
        f.finish();
        foreign.finish();
    }
    #[test]
    fn accepted_holder_and_early_and_late_aliases_retire_independently() {
        let mut f = Fixture::new(1);
        let id = f.append();
        let (original, dropped) = source(&f, id, false);
        let early = original.clone();
        let fact = original.failure_fact();
        let detail = original.safe_detail();
        install(&mut f, id, SourceId(1), original);
        let retained = f.bank_mut().take_source(SourceId(1)).unwrap();
        let accepted = Arc::new(
            AcceptedTaskConclusion::from_retained_source(
                AcceptedConclusion::new(
                    f.task,
                    NonZeroU64::new(1).unwrap(),
                    TaskConclusion::Terminated(TerminationDetail::Failed(
                        TaskFailure::new_in_phase(fact, detail, TaskFailurePhase::Execution),
                    )),
                ),
                retained,
            )
            .unwrap(),
        );
        let late = accepted.originating_failure().unwrap().clone();
        let custody = f.bank().custody(id).unwrap();
        drop(accepted);
        assert_eq!(custody.phase(), ExitRolePhase::Started);
        assert!(!custody.actual_exit_ready().unwrap());
        drop(early);
        assert_eq!(dropped.load(Ordering::SeqCst), 0);
        assert_eq!(custody.phase(), ExitRolePhase::Started);
        drop(late);
        assert_eq!(dropped.load(Ordering::SeqCst), 1);
        assert_eq!(custody.phase(), ExitRolePhase::Exited);
        f.finish();
    }
    #[test]
    fn a_reaped_witness_is_original_generation_bound_and_replacement_returns_refused_roles() {
        let mut f = Fixture::new(1);
        let id = f.append();
        f.take_unused(id).unwrap().exit();
        let custody = f.bank().custody(id).unwrap();
        assert_unused_report(&custody);
        assert!(custody.try_retire().unwrap());
        assert!(
            f.bank()
                .retirement_probe(id)
                .unwrap()
                .observe_reaped()
                .is_err()
        );
        assert_eq!(f.ledger.reap_scope_ready(f.scope.id(), 1).unwrap(), 1);
        let first = f
            .bank()
            .retirement_probe(id)
            .unwrap()
            .observe_reaped()
            .unwrap();
        let stale = f
            .bank()
            .retirement_probe(id)
            .unwrap()
            .observe_reaped()
            .unwrap();
        let role = f.role();
        f.bank_mut()
            .replace_after_reap(first, reported(role))
            .unwrap_or_else(|_| panic!("same original reaped position"));
        assert_eq!(f.bank().phase(id).unwrap(), SourcePositionPhase::Available);
        let role = f.role();
        let original_work = role.work.id();
        let (error, returned) = f
            .bank_mut()
            .replace_after_reap(stale, reported(role))
            .unwrap_err();
        let returned = raw_reported(returned);
        assert_eq!(error, SourcePositionError::WrongGeneration);
        assert_eq!(returned.work.id(), original_work);
        f.dispose_role(returned);
        f.finish();
    }
    #[test]
    fn dropping_a_bound_return_under_a_cut_lock_keeps_the_actual_source_for_disposal() {
        let mut f = ManuallyDrop::new(Fixture::new(1));
        let id = f.append();
        let (original, dropped) = source(&f, id, false);
        let pointer = original.cause() as *const _;
        let bound = f
            .claim(id, SourceId(1), TaskFailurePhase::Preparation)
            .unwrap()
            .bind(original);
        let custody = f.bank().custody(id).unwrap();
        let version = f.ledger.change_version();
        let cut = Mutex::new(());
        {
            let _cut = cut.lock().unwrap();
            drop(bound);
            assert_eq!(f.bank().phase(id).unwrap(), SourcePositionPhase::Abandoned);
            assert!(!f.bank().is_ready(SourceId(1)));
        }
        assert_eq!(f.ledger.change_version(), version);
        assert!(f.health.failure().is_none());
        assert_eq!(f.pending.0.load(Ordering::SeqCst), 0);
        assert_eq!(dropped.load(Ordering::SeqCst), 0);
        let retained = f.take_for_disposal(id).unwrap();
        assert_eq!(retained.cause().cause() as *const _, pointer);
        assert!(custody.observe_pending_failure());
        assert_eq!(custody.phase(), ExitRolePhase::Unresolved);
        drop(retained);
        assert_eq!(dropped.load(Ordering::SeqCst), 1);
        assert!(!custody.try_retire().unwrap());
        assert!(!custody.reusable());
    }
    #[test]
    fn a_claim_dropped_inside_the_cut_only_records_pending_unknown_until_real_host_drain() {
        let mut f = ManuallyDrop::new(Fixture::new(1));
        let id = f.append();
        let custody = f.bank().custody(id).unwrap();
        let cut = Mutex::new(());
        let version = f.ledger.change_version();
        {
            let _cut = cut.lock().unwrap();
            drop(
                f.claim(id, SourceId(1), TaskFailurePhase::Preparation)
                    .unwrap(),
            );
        }
        assert_eq!(f.ledger.change_version(), version);
        assert_eq!(custody.phase(), ExitRolePhase::Admitted);
        assert!(f.ledger.unresolved_work().is_empty());
        assert!(f.health.failure().is_none());
        assert_eq!(f.pending.0.load(Ordering::SeqCst), 0);
        assert!(custody.observe_pending_failure());
        assert_eq!(custody.phase(), ExitRolePhase::Unresolved);
        assert_eq!(f.ledger.unresolved_work().len(), 1);
        assert!(!custody.try_retire().unwrap());
    }
    #[test]
    fn a_claim_rejected_by_prior_metadata_unknown_cannot_run_unused_exit_under_the_cut() {
        let mut f = ManuallyDrop::new(Fixture::new(1));
        let id = f.append();
        let custody = f.bank().custody(id).unwrap();
        custody.note_inventory_abandoned();
        let cut = Mutex::new(());
        let version = f.ledger.change_version();
        let panic = catch_unwind(AssertUnwindSafe(|| {
            let _cut = cut.lock().unwrap();
            let _claim = f.claim(id, SourceId(1), TaskFailurePhase::Preparation);
        }))
        .unwrap_err();
        let _original_panic = ManuallyDrop::new(panic);
        assert!(cut.is_poisoned());
        assert_eq!(f.ledger.change_version(), version);
        assert_eq!(custody.phase(), ExitRolePhase::Admitted);
        assert!(f.ledger.unresolved_work().is_empty());
        assert!(f.health.failure().is_none());
        assert_eq!(f.pending.0.load(Ordering::SeqCst), 0);
        assert_eq!(f.bank().phase(id).unwrap(), SourcePositionPhase::Abandoned);
        assert!(custody.observe_pending_failure());
        assert_eq!(custody.phase(), ExitRolePhase::Unresolved);
        assert!(!custody.try_retire().unwrap());
    }

    #[test]
    fn dropping_an_unused_inventory_under_the_cut_does_not_run_permit_exit_or_ledger_calls() {
        let mut f = ManuallyDrop::new(Fixture::new(1));
        let id = f.append();
        let custody = f.bank().custody(id).unwrap();
        let position = Arc::downgrade(f.bank().position(id).unwrap());
        let version = f.ledger.change_version();
        let cut = Mutex::new(());
        {
            let _cut = cut.lock().unwrap();
            drop(f.bank.take());
        }
        assert_eq!(f.ledger.change_version(), version);
        assert_eq!(custody.phase(), ExitRolePhase::Admitted);
        assert!(f.health.failure().is_none());
        assert_eq!(f.pending.0.load(Ordering::SeqCst), 0);
        let position = position
            .upgrade()
            .expect("the actual original unused permit is retained");
        assert!(matches!(
            position.state.lock().unwrap().payload,
            Payload::Abandoned {
                permit: Some(_),
                source: None
            }
        ));
        assert!(custody.observe_pending_failure());
        assert_eq!(custody.phase(), ExitRolePhase::Unresolved);
        assert!(!custody.reusable());
    }
    #[test]
    fn a_metadata_unknown_before_host_drain_cannot_install_a_ready_source() {
        let mut f = ManuallyDrop::new(Fixture::new(1));
        let id = f.append();
        let (original, dropped) = source(&f, id, false);
        let bound = f
            .claim(id, SourceId(1), TaskFailurePhase::Execution)
            .unwrap()
            .bind(original);
        let custody = f.bank().custody(id).unwrap();
        let version = f.ledger.change_version();
        custody.note_inventory_abandoned();
        assert_eq!(f.ledger.change_version(), version);
        assert_eq!(custody.phase(), ExitRolePhase::Started);
        let (error, bound) = f.bank_mut().install(bound).unwrap_err();
        assert_eq!(error, SourcePositionError::SourceUnresolved);
        assert!(!f.bank().is_ready(SourceId(1)));
        drop(bound.into_retained());
        assert_eq!(dropped.load(Ordering::SeqCst), 1);
        assert!(!custody.try_retire().unwrap());
        assert!(!custody.reusable());
    }
    #[test]
    fn a_source_destructor_unwind_preserves_unknown_and_never_refills_the_position() {
        let mut f = ManuallyDrop::new(Fixture::new(1));
        let id = f.append();
        let (original, dropped) = source(&f, id, true);
        install(&mut f, id, SourceId(1), original);
        let retained = f.bank_mut().take_source(SourceId(1)).unwrap();
        let payload = catch_unwind(AssertUnwindSafe(|| drop(retained))).unwrap_err();
        let _original_panic = ManuallyDrop::new(payload);
        let custody = f.bank().custody(id).unwrap();
        assert_eq!(dropped.load(Ordering::SeqCst), 1);
        assert_eq!(custody.phase(), ExitRolePhase::Unresolved);
        assert!(custody.report().consume(core()).is_err());
        assert!(!custody.try_retire().unwrap());
        assert!(
            f.bank()
                .retirement_probe(id)
                .unwrap()
                .observe_reaped()
                .is_err()
        );
    }
    #[test]
    fn pair_swaps_move_actual_paid_positions_and_full_refusal_returns_original_roles() {
        let mut f = Fixture::new(4);
        let roles = [f.role(), f.role()];
        let mut open = f
            .bank_mut()
            .append_pair(roles.map(model))
            .unwrap_or_else(|_| panic!("paid open pair"));
        let roles = [f.role(), f.role()];
        let mut cut = f
            .bank_mut()
            .append_pair(roles.map(model))
            .unwrap_or_else(|_| panic!("paid cut pair"));
        let original_open = open.positions();
        let original_cut = cut.positions();
        let work_reports = [original_open, original_cut]
            .map(|pair| pair.map(|id| f.bank().custody(id).unwrap().report().id()));
        open.swap_with(&mut cut).unwrap();
        assert_eq!(open.positions(), original_cut);
        assert_eq!(cut.positions(), original_open);
        assert_eq!(
            open.positions()
                .map(|id| f.bank().custody(id).unwrap().report().id()),
            work_reports[1]
        );
        assert_eq!(
            cut.positions()
                .map(|id| f.bank().custody(id).unwrap().report().id()),
            work_reports[0]
        );
        let roles = [f.role(), f.role()];
        let ids = roles.each_ref().map(|role| role.work.id());
        let (error, returned) = f.bank_mut().append_pair(roles.map(model)).unwrap_err();
        let returned = returned.map(raw_admission);
        assert_eq!(error, SourcePositionError::Full);
        assert_eq!(returned.each_ref().map(|role| role.work.id()), ids);
        for role in returned {
            f.dispose_role(role);
        }
        f.finish();
    }
    #[test]
    fn repeated_actual_cuts_rearm_the_same_pair_without_growing_addresses_or_work() {
        let mut f = Fixture::new(6);
        for _ in 0..4 {
            f.append();
        }
        let roles = [f.role(), f.role()];
        let mut pair = f
            .bank_mut()
            .append_pair(roles.map(model))
            .unwrap_or_else(|_| panic!("paid pair admission"));
        let addresses = pair.positions();
        let original_key = pair.keys()[0].clone();
        for round in 0..64_u64 {
            let source_ids = [SourceId(round * 2 + 1), SourceId(round * 2 + 2)];
            let work_before = f.scope.snapshot().unwrap().work;
            assert_eq!(work_before, 7, "Task lifetime plus six original sources");
            for (key, source_id) in pair.keys().into_iter().zip(source_ids) {
                let key = key.clone();
                let (source, dropped) = source(&f, key.address(), false);
                let bound = f
                    .bank_mut()
                    .claim(&key, source_id, TaskFailurePhase::Execution)
                    .unwrap()
                    .bind(source);
                f.bank_mut().install(bound).unwrap();
                drop(f.bank_mut().take_source(source_id).unwrap());
                assert_eq!(dropped.load(Ordering::SeqCst), 1);
                f.retire_generation(key.address());
            }
            assert_eq!(f.scope.snapshot().unwrap().work, 5);
            let witnesses = pair.keys().map(|key| {
                f.bank()
                    .retirement_probe_for(key)
                    .unwrap()
                    .observe_reaped()
                    .unwrap()
            });
            let roles = [f.role(), f.role()];
            let new_reports = roles.each_ref().map(|role| role.report.id());
            let version = f.ledger.change_version();
            pair = f
                .bank_mut()
                .rearm_pair(pair, witnesses, roles.map(reported))
                .unwrap_or_else(|_| panic!("reaped pair replacement"));
            assert_eq!(
                f.ledger.change_version(),
                version,
                "rearm only changes metadata after real outside-lock admission/reaping"
            );
            assert_eq!(pair.positions(), addresses);
            assert_eq!(f.bank().len(), 6);
            assert_eq!(f.scope.snapshot().unwrap().work, work_before);
            assert_eq!(
                pair.keys().map(|key| key.position.custody.report().id()),
                new_reports
            );
            for source in source_ids {
                assert!(matches!(
                    f.bank_mut().take_source(source),
                    Err(SourcePositionError::UnknownSource)
                ));
            }
            assert!(matches!(
                f.bank_mut().claim(
                    &original_key,
                    SourceId(1000 + round),
                    TaskFailurePhase::Execution
                ),
                Err(SourcePositionError::WrongGeneration)
            ));
        }
        f.finish();
    }

    #[test]
    fn pair_rearm_rejects_an_unreaped_second_leg_before_consuming_either_new_role() {
        let mut f = Fixture::new(2);
        let roles = [f.role(), f.role()];
        let pair = f
            .bank_mut()
            .append_pair(roles.map(model))
            .unwrap_or_else(|_| panic!("paid pair admission"));
        let addresses = pair.positions();
        let keys = pair.keys().map(Clone::clone);
        f.bank_mut().take_unused(&keys[0]).unwrap().exit();
        f.retire_generation(addresses[0]);
        assert!(
            f.bank()
                .retirement_probe_for(pair.keys()[1])
                .unwrap()
                .observe_reaped()
                .is_err()
        );
        // A witness can only be obtained for the actually reaped first leg.
        // Presenting it twice cannot authorize replacement of the live second.
        let witnesses = [0, 1].map(|_| {
            f.bank()
                .retirement_probe_for(pair.keys()[0])
                .unwrap()
                .observe_reaped()
                .unwrap()
        });
        let roles = [f.role(), f.role()];
        let ids = roles.each_ref().map(|role| role.work.id());
        let before = f.scope.snapshot().unwrap();
        let (error, pair, witnesses, returned) = f
            .bank_mut()
            .rearm_pair(pair, witnesses, roles.map(reported))
            .unwrap_err();
        let returned = returned.map(raw_reported);
        assert_eq!(error, SourcePositionError::WrongGeneration);
        assert_eq!(returned.each_ref().map(|role| role.work.id()), ids);
        assert_eq!(f.scope.snapshot().unwrap(), before);
        assert!(f.bank().positions.contains_key(&addresses[0]));
        assert_eq!(
            f.bank().inspect(&keys[0]).unwrap().phase,
            SourcePositionPhase::Retired
        );
        assert!(Arc::ptr_eq(
            &f.bank().positions[&addresses[1]],
            &keys[1].position
        ));
        assert_eq!(
            f.bank().phase(addresses[1]).unwrap(),
            SourcePositionPhase::Available
        );
        f.bank_mut().take_unused(&keys[1]).unwrap().exit();
        f.retire_generation(addresses[1]);
        let [first, _duplicate] = witnesses;
        let second = f
            .bank()
            .retirement_probe_for(pair.keys()[1])
            .unwrap()
            .observe_reaped()
            .unwrap();
        let pair = f
            .bank_mut()
            .rearm_pair(pair, [first, second], returned.map(reported))
            .unwrap_or_else(|_| panic!("reaped pair replacement"));
        for key in pair.keys() {
            assert_eq!(key.position.generation, 2);
            assert!(key.position.custody.report().id() != keys[0].position.custody.report().id());
        }
        f.finish();
    }

    #[test]
    fn stale_keys_and_misrouted_late_bound_metadata_preserve_the_new_original_role() {
        let mut f = Fixture::new(1);
        let id = f.append();
        let stale = f.key(id);
        f.bank_mut().take_unused(&stale).unwrap().exit();
        f.retire_generation(id);
        let reaped = f
            .bank()
            .retirement_probe_for(&stale)
            .unwrap()
            .observe_reaped()
            .unwrap();
        let role = f.role();
        let fresh = f
            .bank_mut()
            .replace_after_reap(reaped, reported(role))
            .unwrap_or_else(|_| panic!("reaped replacement"));
        let version = f.ledger.change_version();
        assert!(matches!(
            f.bank_mut()
                .claim(&stale, SourceId(1), TaskFailurePhase::Execution),
            Err(SourcePositionError::WrongGeneration)
        ));
        assert!(matches!(
            f.bank_mut().take_unused(&stale),
            Err(SourcePositionError::WrongGeneration)
        ));
        assert_eq!(f.bank().phase(id).unwrap(), SourcePositionPhase::Available);
        assert_eq!(f.ledger.change_version(), version);
        let (original, dropped) = source(&f, id, false);
        let pointer = original.cause() as *const _;
        let mut bound = f
            .bank_mut()
            .claim(&fresh, SourceId(2), TaskFailurePhase::Execution)
            .unwrap()
            .bind(original);
        // Inject a stale routing envelope only: a legitimately live BoundSource
        // itself prevents its role from reaping. No physical exit is forged.
        bound.position = stale.position;
        let (error, mut bound) = f.bank_mut().install(bound).unwrap_err();
        assert_eq!(error, SourcePositionError::WrongGeneration);
        assert_eq!(bound.retained().cause().cause() as *const _, pointer);
        assert_eq!(dropped.load(Ordering::SeqCst), 0);
        assert_eq!(f.bank().phase(id).unwrap(), SourcePositionPhase::Pending);
        bound.position = fresh.position;
        f.bank_mut().install(bound).unwrap();
        drop(f.bank_mut().take_source(SourceId(2)).unwrap());
        f.finish();
    }

    #[test]
    fn accepted_source_pins_its_original_pair_until_alias_reports_and_common_reap_exit() {
        let mut f = Fixture::new(2);
        let roles = [f.role(), f.role()];
        let pair = f
            .bank_mut()
            .append_pair(roles.map(model))
            .unwrap_or_else(|_| panic!("paid pair admission"));
        let addresses = pair.positions();
        let key = pair.keys()[0].clone();
        let (original, dropped) = source(&f, key.address(), false);
        let early = original.clone();
        let fact = original.failure_fact();
        let detail = original.safe_detail();
        let bound = f
            .bank_mut()
            .claim(&key, SourceId(1), TaskFailurePhase::Execution)
            .unwrap()
            .bind(original);
        f.bank_mut().install(bound).unwrap();
        let retained = f.bank_mut().take_source(SourceId(1)).unwrap();
        let accepted = Arc::new(
            AcceptedTaskConclusion::from_retained_source(
                AcceptedConclusion::new(
                    f.task,
                    NonZeroU64::new(1).unwrap(),
                    TaskConclusion::Terminated(TerminationDetail::Failed(
                        TaskFailure::new_in_phase(fact, detail, TaskFailurePhase::Execution),
                    )),
                ),
                retained,
            )
            .unwrap(),
        );
        let late = accepted.originating_failure().unwrap().clone();
        f.bank_mut().take_unused(pair.keys()[1]).unwrap().exit();
        f.retire_generation(addresses[1]);
        assert!(
            f.bank()
                .retirement_probe_for(pair.keys()[0])
                .unwrap()
                .observe_reaped()
                .is_err()
        );
        assert_eq!(
            f.bank().phase(addresses[0]).unwrap(),
            SourcePositionPhase::Taken
        );
        drop(accepted);
        drop(early);
        assert_eq!(dropped.load(Ordering::SeqCst), 0);
        assert!(
            f.bank()
                .retirement_probe_for(pair.keys()[0])
                .unwrap()
                .observe_reaped()
                .is_err()
        );
        drop(late);
        assert_eq!(dropped.load(Ordering::SeqCst), 1);
        let custody = f.bank().custody(addresses[0]).unwrap();
        custody.report().consume(core()).unwrap();
        custody.report().consume(host()).unwrap();
        assert!(custody.try_retire().unwrap());
        assert!(
            f.bank()
                .retirement_probe_for(pair.keys()[0])
                .unwrap()
                .observe_reaped()
                .is_err()
        );
        assert_eq!(f.ledger.reap_scope_ready(f.scope.id(), 1).unwrap(), 1);
        let witnesses = pair.keys().map(|key| {
            f.bank()
                .retirement_probe_for(key)
                .unwrap()
                .observe_reaped()
                .unwrap()
        });
        let roles = [f.role(), f.role()];
        let fresh = f
            .bank_mut()
            .rearm_pair(pair, witnesses, roles.map(reported))
            .unwrap_or_else(|_| panic!("reaped pair replacement"));
        assert_eq!(fresh.positions(), addresses);
        assert_eq!(f.bank().len(), 2);
        assert!(matches!(
            f.bank_mut().take_source(SourceId(1)),
            Err(SourcePositionError::UnknownSource)
        ));
        f.finish();
    }
    #[test]
    fn a_pair_with_one_replaced_original_key_cannot_consume_replacement_roles() {
        let mut f = Fixture::new(2);
        let roles = [f.role(), f.role()];
        let pair = f
            .bank_mut()
            .append_pair(roles.map(model))
            .unwrap_or_else(|_| panic!("paid pair"));
        let addresses = pair.positions();
        for key in pair.keys() {
            f.bank_mut().take_unused(key).unwrap().exit();
            f.retire_generation(key.address());
        }
        let witnesses = pair.keys().map(|key| {
            f.bank()
                .retirement_probe_for(key)
                .unwrap()
                .observe_reaped()
                .unwrap()
        });
        let replacement_witness = f
            .bank()
            .retirement_probe_for(pair.keys()[0])
            .unwrap()
            .observe_reaped()
            .unwrap();
        let role = f.role();
        let fresh = f
            .bank_mut()
            .replace_after_reap(replacement_witness, reported(role))
            .unwrap_or_else(|_| panic!("one original replacement"));
        let roles = [f.role(), f.role()];
        let original_work = roles.each_ref().map(|role| role.work.id());
        let before = f.scope.snapshot().unwrap();
        let (error, _original_pair, _witnesses, returned) = f
            .bank_mut()
            .rearm_pair(pair, witnesses, roles.map(reported))
            .unwrap_err();
        let returned = returned.map(raw_reported);
        assert_eq!(error, SourcePositionError::WrongGeneration);
        assert_eq!(
            returned.each_ref().map(|role| role.work.id()),
            original_work
        );
        assert_eq!(f.scope.snapshot().unwrap(), before);
        assert!(Arc::ptr_eq(
            &fresh.position,
            &f.bank().positions[&addresses[0]]
        ));
        assert_eq!(
            f.bank().phase(addresses[0]).unwrap(),
            SourcePositionPhase::Available
        );
        for role in returned {
            f.dispose_role(role);
        }
        f.finish();
    }
}
