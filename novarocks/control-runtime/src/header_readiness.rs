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

//! Original Work header readiness, without execution or exit authority.

use crate::{
    actor::Actor,
    actor_turn::{ActorRegistration, ActorRuntime, ActorTurnError},
    notify::Notify,
    obligation::WorkRetirementObserver,
    sync::{Arc, Mutex},
    transaction::{RuntimeError, ScopeHandle},
};
use novarocks_control_runtime_core::identity::{ActorId, ScopedWorkRef, SlotId};
use std::collections::BTreeMap;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum HeaderWaitState {
    Waiting,
    ReadyToReap,
    Retired,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum HeaderReadyError {
    Runtime(RuntimeError),
    ActorTurn(ActorTurnError),
    WrongObserver,
    WrongOwner,
    Cancelled,
}
impl From<RuntimeError> for HeaderReadyError {
    fn from(error: RuntimeError) -> Self {
        Self::Runtime(error)
    }
}
impl From<ActorTurnError> for HeaderReadyError {
    fn from(error: ActorTurnError) -> Self {
        Self::ActorTurn(error)
    }
}
impl std::fmt::Display for HeaderReadyError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "original header readiness failure: {self:?}")
    }
}
impl std::error::Error for HeaderReadyError {}

/// A runtime-qualified neutral destination. It contains no Scope or runtime
/// owner, so retaining it in a ledger cannot create an ownership cycle.
#[derive(Clone)]
pub(crate) struct HeaderOwner {
    pub(crate) actor: ActorId,
    pub(crate) attachment: SlotId,
    pub(crate) origin: Arc<()>,
}
impl HeaderOwner {
    pub(crate) fn same(&self, other: &Self) -> bool {
        self.actor == other.actor
            && self.attachment == other.attachment
            && Arc::ptr_eq(&self.origin, &other.origin)
    }
}

#[derive(Clone, Copy, Eq, PartialEq)]
enum Location {
    Idle,
    Global(u64),
    ActorPending(u64),
}
struct CellState {
    armed: bool,
    location: Location,
}
struct ReadyCell {
    work: ScopedWorkRef,
    owner: HeaderOwner,
    // Always acquired after the hub lock. Cells contain only neutral metadata.
    state: Mutex<CellState>,
}
struct ActorSources {
    owner: HeaderOwner,
    bindings: usize,
    pending: BTreeMap<u64, Arc<ReadyCell>>,
}
#[derive(Default)]
struct HubState {
    next_ready: u64,
    global: BTreeMap<u64, Arc<ReadyCell>>,
    actors: BTreeMap<ActorId, ActorSources>,
}

/// One per actual driver. Neither a cell nor the hub owns a Scope, ledger,
/// RuntimeShared, callback, Waker or opaque destructor. The fixed Notify is
/// the same versioned wake sampled by the existing poller before dispatch.
pub(crate) struct HeaderReadyHub {
    state: Mutex<HubState>,
    changed: Arc<Notify>,
}
impl HeaderReadyHub {
    pub(crate) fn new(changed: Arc<Notify>) -> Self {
        Self {
            state: Mutex::new(HubState::default()),
            changed,
        }
    }
    fn bind(&self, cell: &Arc<ReadyCell>) -> Result<(), HeaderReadyError> {
        let mut state = self.state.lock().expect("header readiness hub lock");
        let sources = state
            .actors
            .entry(cell.owner.actor)
            .or_insert_with(|| ActorSources {
                owner: cell.owner.clone(),
                bindings: 0,
                pending: BTreeMap::new(),
            });
        if !sources.owner.same(&cell.owner) {
            return Err(HeaderReadyError::WrongOwner);
        }
        sources.bindings = sources
            .bindings
            .checked_add(1)
            .expect("admitted header bindings fit usize");
        Ok(())
    }
    fn arm(&self, cell: &Arc<ReadyCell>, ready: bool) {
        let mut state = self.state.lock().expect("header readiness hub lock");
        let mut flags = cell.state.lock().expect("header readiness cell lock");
        flags.armed = true;
        if ready {
            Self::enqueue(&mut state, cell, &mut flags);
            self.changed.notify();
        }
    }
    fn enqueue(state: &mut HubState, cell: &Arc<ReadyCell>, flags: &mut CellState) {
        if flags.armed && flags.location == Location::Idle {
            state.next_ready = state
                .next_ready
                .checked_add(1)
                .expect("header readiness sequence exhausted");
            let sequence = state.next_ready;
            assert!(state.global.insert(sequence, cell.clone()).is_none());
            flags.location = Location::Global(sequence);
        }
    }
    fn ready(&self, cell: &Arc<ReadyCell>) {
        let mut state = self.state.lock().expect("header readiness hub lock");
        let mut flags = cell.state.lock().expect("header readiness cell lock");
        if flags.armed && flags.location == Location::Idle {
            Self::enqueue(&mut state, cell, &mut flags);
            self.changed.notify();
        }
    }
    fn cancel(&self, cell: &Arc<ReadyCell>) {
        let mut state = self.state.lock().expect("header readiness hub lock");
        let mut flags = cell.state.lock().expect("header readiness cell lock");
        match flags.location {
            Location::Idle => {}
            Location::Global(sequence) => {
                let original = state
                    .global
                    .remove(&sequence)
                    .expect("original queued header notice");
                assert!(Arc::ptr_eq(&original, cell));
            }
            Location::ActorPending(sequence) => {
                let original = state
                    .actors
                    .get_mut(&cell.owner.actor)
                    .expect("original header owner")
                    .pending
                    .remove(&sequence)
                    .expect("original pending header notice");
                assert!(Arc::ptr_eq(&original, cell));
            }
        }
        flags.location = Location::Idle;
        flags.armed = false;
        let sources = state
            .actors
            .get_mut(&cell.owner.actor)
            .expect("original header source registration");
        assert!(sources.owner.same(&cell.owner));
        sources.bindings = sources
            .bindings
            .checked_sub(1)
            .expect("header binding underflow");
        if sources.bindings == 0 {
            assert!(sources.pending.is_empty());
            state.actors.remove(&cell.owner.actor);
        }
        self.changed.notify();
    }
    /// Move the same cell once, then release this lock before scheduler access.
    pub(crate) fn promote(&self, budget: usize) -> Vec<HeaderOwner> {
        let mut state = self.state.lock().expect("header readiness hub lock");
        let mut owners = Vec::new();
        for _ in 0..budget {
            let Some((sequence, cell)) = state.global.pop_first() else {
                break;
            };
            let mut flags = cell.state.lock().expect("header readiness cell lock");
            assert!(flags.location == Location::Global(sequence));
            let sources = state
                .actors
                .get_mut(&cell.owner.actor)
                .expect("queued header retains its registered owner");
            assert!(sources.owner.same(&cell.owner));
            assert!(sources.pending.insert(sequence, cell.clone()).is_none());
            flags.location = Location::ActorPending(sequence);
            owners.push(cell.owner.clone());
        }
        if !state.global.is_empty() && budget != 0 {
            self.changed.notify();
        }
        owners
    }
    pub(crate) fn take(&self, owner: &HeaderOwner, budget: usize) -> Vec<HeaderReadyNotice> {
        let mut state = self.state.lock().expect("header readiness hub lock");
        let Some(sources) = state.actors.get_mut(&owner.actor) else {
            return Vec::new();
        };
        if !sources.owner.same(owner) {
            return Vec::new();
        }
        let mut notices = Vec::new();
        for _ in 0..budget {
            let Some((sequence, cell)) = sources.pending.pop_first() else {
                break;
            };
            {
                let mut flags = cell.state.lock().expect("header readiness cell lock");
                assert!(flags.location == Location::ActorPending(sequence));
                flags.location = Location::Idle;
                flags.armed = false;
            }
            notices.push(HeaderReadyNotice { cell });
        }
        notices
    }
    pub(crate) fn pending(&self, owner: &HeaderOwner) -> bool {
        self.state
            .lock()
            .expect("header readiness hub lock")
            .actors
            .get(&owner.actor)
            .is_some_and(|sources| sources.owner.same(owner) && !sources.pending.is_empty())
    }
    pub(crate) fn bound(&self, owner: &HeaderOwner) -> bool {
        self.state
            .lock()
            .expect("header readiness hub lock")
            .actors
            .get(&owner.actor)
            .is_some_and(|sources| sources.owner.same(owner) && sources.bindings != 0)
    }
    pub(crate) fn is_empty(&self) -> bool {
        let state = self.state.lock().expect("header readiness hub lock");
        state.actors.is_empty() && state.global.is_empty()
    }
}

/// A coalesced neutral routing notice. Only the original registration can
/// match its actual cell. Receiving this notice never proves header exit.
pub struct HeaderReadyNotice {
    cell: Arc<ReadyCell>,
}
impl HeaderReadyNotice {
    pub fn work(&self) -> ScopedWorkRef {
        self.cell.work
    }
}
impl std::fmt::Debug for HeaderReadyNotice {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("HeaderReadyNotice")
            .field("work", &self.work())
            .finish()
    }
}

pub(crate) struct HeaderBinding {
    hub: Arc<HeaderReadyHub>,
    cell: Arc<ReadyCell>,
}
impl HeaderBinding {
    pub(crate) fn ready(&self) {
        self.hub.ready(&self.cell);
    }
}

/// Signed once by the original actual WorkObligation. No Clone, header pin, Work,
/// report or execution author is created. First arm allocates routing metadata;
/// cancel/Drop only withdraw that metadata, never observing physical exit.
#[must_use = "Keep the original registration through notice consumption, then cancel before owner metadata refunds"]
pub struct HeaderReadyRegistration {
    scope: ScopeHandle,
    work: ScopedWorkRef,
    binding: Option<HeaderBinding>,
    cancelled: bool,
}
impl std::fmt::Debug for HeaderReadyRegistration {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("HeaderReadyRegistration")
            .field("work", &self.work)
            .field("cancelled", &self.cancelled)
            .finish()
    }
}
impl HeaderReadyRegistration {
    pub(crate) fn new(scope: ScopeHandle, work: ScopedWorkRef) -> Self {
        Self {
            scope,
            work,
            binding: None,
            cancelled: false,
        }
    }
    pub(crate) fn belongs_to_ledger(&self, scope: &ScopeHandle) -> bool {
        self.scope.same_ledger(scope)
    }
    pub fn work(&self) -> ScopedWorkRef {
        self.work
    }
    pub fn matches(&self, notice: &HeaderReadyNotice) -> bool {
        self.binding
            .as_ref()
            .is_some_and(|binding| Arc::ptr_eq(&binding.cell, &notice.cell))
    }
    pub fn arm<A: Actor>(
        &mut self,
        runtime: &ActorRuntime<A>,
        owner: &ActorRegistration,
        observer: &WorkRetirementObserver,
    ) -> Result<HeaderWaitState, HeaderReadyError> {
        runtime.arm_header_readiness(owner, self, observer)
    }
    // Called under the actual inbox gate, after scheduler validation.
    pub(crate) fn arm_for(
        &mut self,
        hub: &Arc<HeaderReadyHub>,
        owner: HeaderOwner,
        observer: &WorkRetirementObserver,
    ) -> Result<HeaderWaitState, HeaderReadyError> {
        if self.cancelled {
            return Err(HeaderReadyError::Cancelled);
        }
        if !observer.matches(&self.scope, self.work) {
            return Err(HeaderReadyError::WrongObserver);
        }
        if let Some(binding) = &self.binding
            && (!Arc::ptr_eq(&binding.hub, hub) || !binding.cell.owner.same(&owner))
        {
            return Err(HeaderReadyError::WrongOwner);
        }
        let mut state = self
            .scope
            .runtime
            .shared
            .state
            .lock()
            .expect("runtime ledger lock");
        let actual = state
            .scopes
            .get(&self.work.scope)
            .and_then(|scope| scope.work(self.work.work));
        let status = match actual {
            None => HeaderWaitState::Retired,
            Some(_) => {
                if state
                    .scopes
                    .get(&self.work.scope)
                    .expect("actual original Scope")
                    .work_header_retirement_ready(self.work.work)
                    .map_err(RuntimeError::from)?
                {
                    HeaderWaitState::ReadyToReap
                } else {
                    HeaderWaitState::Waiting
                }
            }
        };
        if self.binding.is_none() {
            let cell = Arc::new(ReadyCell {
                work: self.work,
                owner,
                state: Mutex::new(CellState {
                    armed: false,
                    location: Location::Idle,
                }),
            });
            hub.bind(&cell)?;
            assert!(
                state
                    .header_readiness
                    .insert(
                        (self.work.scope, self.work.work),
                        HeaderBinding {
                            hub: hub.clone(),
                            cell: cell.clone()
                        }
                    )
                    .is_none()
            );
            self.binding = Some(HeaderBinding {
                hub: hub.clone(),
                cell,
            });
        }
        let binding = self.binding.as_ref().expect("original header binding");
        binding
            .hub
            .arm(&binding.cell, status != HeaderWaitState::Waiting);
        Ok(status)
    }
    pub fn cancel(&mut self) {
        if let Some(binding) = self.binding.take() {
            let mut state = self
                .scope
                .runtime
                .shared
                .state
                .lock()
                .expect("runtime ledger lock");
            let original = state
                .header_readiness
                .remove(&(self.work.scope, self.work.work))
                .expect("original header readiness binding");
            assert!(Arc::ptr_eq(&original.cell, &binding.cell));
            binding.hub.cancel(&binding.cell);
            drop(state);
            self.scope.runtime.shared.changed.notify();
        }
        self.cancelled = true;
    }
}
impl Drop for HeaderReadyRegistration {
    fn drop(&mut self) {
        self.cancel();
    }
}
