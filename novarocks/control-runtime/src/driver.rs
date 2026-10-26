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

//! Production pollers and deterministic drivers execute the same bounded turn.

use crate::{
    actor::{Actor, ActorConfig, StepContext, StepEnvelope},
    notify::Notify,
    sync::{Arc, Mutex},
    transaction::{RuntimeError, ScopeHandle},
};
use novarocks_control_runtime_core::{
    effect::{EffectDescriptor, EffectError, EffectKind, EffectState},
    history::HistoryFact,
    identity::{ActionId, ActorId, Generation, InputId, ReservationId, SlotId, Tick, WorkId},
    mailbox::{InputClass, InputEnvelope, MailboxError, MailboxState},
    scheduler::{ReadyQueue, SchedulerError, TurnBudget},
    scope::{WorkKind, WorkPhase},
    timer::{TimerError, TimerIndex},
};
use std::{
    any::Any,
    collections::BTreeMap,
    mem::ManuallyDrop,
    panic::{AssertUnwindSafe, catch_unwind},
};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DriverError {
    Scheduler(SchedulerError),
    Mailbox(MailboxError),
    Effect(EffectError),
    Timer(TimerError),
    Runtime(RuntimeError),
    UnknownActor,
    Closed,
    ActorFailed,
    InputCapacity,
    UnknownInput,
    InputNotReserved,
    InputStillQueued,
    EffectsRemain,
    ActorNotRetired,
    IdentityExhausted,
    InvalidConfig,
    PublicationTargetMismatch,
    ReplyAlreadyCommitted,
    UnknownRole,
    RoleBusy,
    FailurePublicationRequired,
    FailurePublicationAlreadyClaimed,
    FailureWorkInactive,
    ReservationsRemain,
    UnknownReservation,
    ReservationOwnerMismatch,
}
impl From<SchedulerError> for DriverError {
    fn from(v: SchedulerError) -> Self {
        Self::Scheduler(v)
    }
}
impl From<MailboxError> for DriverError {
    fn from(v: MailboxError) -> Self {
        Self::Mailbox(v)
    }
}
impl From<EffectError> for DriverError {
    fn from(v: EffectError) -> Self {
        Self::Effect(v)
    }
}
impl From<TimerError> for DriverError {
    fn from(v: TimerError) -> Self {
        Self::Timer(v)
    }
}
impl From<RuntimeError> for DriverError {
    fn from(v: RuntimeError) -> Self {
        Self::Runtime(v)
    }
}
impl std::fmt::Display for DriverError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "control driver failure: {self:?}")
    }
}
impl std::error::Error for DriverError {}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum InputPhase {
    Reserved,
    Bound,
    Filled,
    Consuming,
    Committed,
    AbandonedByOwnerFailure,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct InputCustody {
    pub input: InputId,
    pub source: ActorId,
    pub class: InputClass,
    pub phase: InputPhase,
    pub role: Option<InputRoleId>,
}
struct InputRecord {
    source: ActorId,
    class: InputClass,
    phase: InputPhase,
    role: Option<InputRoleId>,
}
/// Stable identity of one admitted role position; this identity does not mint authority.
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct InputRoleId(InputId);
impl InputRoleId {
    pub const fn get(self) -> u64 {
        self.0.get()
    }
}
struct InputRoleRecord {
    source: ActorId,
    class: InputClass,
    in_flight: Option<InputId>,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct InputRoleCustody {
    pub role: InputRoleId,
    pub source: ActorId,
    pub class: InputClass,
    pub in_flight: Option<InputId>,
}
struct FailurePublicationRecord {
    generation: Generation,
    input: InputId,
    action: ActionId,
    work: WorkId,
}
struct Inbox<A: Actor> {
    payloads: BTreeMap<InputId, A::Input>,
    mailbox: MailboxState,
    inputs: BTreeMap<InputId, InputRecord>,
    roles: BTreeMap<InputRoleId, InputRoleRecord>,
    counts: [usize; 4],
    next_input: u64,
    batches: BTreeMap<InputId, StepEnvelope<A::Effect>>,
    generation: Generation,
    failed: bool,
    timer_input: Option<InputId>,
    failure: Option<ManuallyDrop<Box<dyn Any + Send>>>,
    last_progress: Tick,
    failure_publication: Option<FailurePublicationRecord>,
    failure_action_watermark: u64,
}
impl<A: Actor> Drop for Inbox<A> {
    fn drop(&mut self) {
        assert!(
            self.failure.is_none(),
            "actor panic payload must be transferred to its host failure owner"
        );
        assert!(
            self.roles.is_empty(),
            "actor role custody must exit before driver removal"
        );
        assert!(
            self.inputs.is_empty(),
            "actor input custody must retire before driver removal"
        );
    }
}
struct ActorCell<A: Actor> {
    id: ActorId,
    scope: ScopeHandle,
    attachment: SlotId,
    config: ActorConfig,
    failure_work: Option<WorkId>,
    core: Mutex<Option<A>>,
    inbox: Mutex<Inbox<A>>,
}
struct DriverState<A: Actor> {
    actors: BTreeMap<ActorId, Arc<ActorCell<A>>>,
    ready: ReadyQueue,
    effect_ready: ReadyQueue,
    timers: TimerIndex,
    open: bool,
    now: Tick,
}
struct DriverShared<A: Actor> {
    state: Mutex<DriverState<A>>,
    changed: Notify,
    history: Option<crate::history::HistoryCapture>,
}
impl<A: Actor> DriverShared<A> {
    fn record(&self, fact: HistoryFact) {
        if let Some(history) = &self.history {
            history.record(fact);
        }
    }
}
pub struct ActorRuntime<A: Actor> {
    shared: Arc<DriverShared<A>>,
}
impl<A: Actor> Clone for ActorRuntime<A> {
    fn clone(&self) -> Self {
        Self {
            shared: self.shared.clone(),
        }
    }
}

/// This is neutral input storage, not a host WorkLease. Bound positions remain
/// resident until captures, effects and report consumers have actually retired.
#[must_use = "Reserve before binding payloads; fill or explicitly revoke an unused input"]
pub struct InputPermit<A: Actor> {
    runtime: ActorRuntime<A>,
    actor: ActorId,
    input: InputId,
}
impl<A: Actor> std::fmt::Debug for InputPermit<A> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("InputPermit")
            .field("actor", &self.actor)
            .field("input", &self.input)
            .finish()
    }
}
/// One pre-admitted, non-cloneable neutral notification position. It is not
/// work admission and never starts a host body. Drop retains its class credit.
#[must_use = "A repeated input role must close after actual holder/capture and report retirement"]
pub struct InputRole<A: Actor> {
    runtime: ActorRuntime<A>,
    actor: ActorId,
    role: InputRoleId,
    active: bool,
}
impl<A: Actor> std::fmt::Debug for InputRole<A> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("InputRole")
            .field("actor", &self.actor)
            .field("role", &self.role)
            .field("active", &self.active)
            .finish()
    }
}
impl<A: Actor> InputRole<A> {
    pub const fn id(&self) -> InputRoleId {
        self.role
    }
    pub fn in_flight(&self) -> Result<Option<InputId>, DriverError> {
        let node = self.runtime.node(self.actor)?;
        let inbox = node.inbox.lock().expect("actor inbox lock");
        Ok(inbox
            .roles
            .get(&self.role)
            .ok_or(DriverError::UnknownRole)?
            .in_flight)
    }
    /// Fills only an idle, already paid position, including during process drain.
    /// Every occurrence receives a fresh monotonic InputId; a prior effect ticket
    /// can never identify this new batch. Error returns the original neutral value.
    pub fn fill(&self, value: A::Input) -> Result<InputId, (DriverError, A::Input)> {
        if !self.active {
            return Err((DriverError::UnknownRole, value));
        }
        let node = match self.runtime.node(self.actor) {
            Ok(node) => node,
            Err(error) => return Err((error, value)),
        };
        let mut inbox = node.inbox.lock().expect("actor inbox lock");
        let scope_failed = match node.scope.gates() {
            Ok(gates) => gates.failed,
            Err(error) => return Err((error.into(), value)),
        };
        if inbox.failed || scope_failed {
            return Err((DriverError::ActorFailed, value));
        }
        let Some(role) = inbox.roles.get(&self.role) else {
            return Err((DriverError::UnknownRole, value));
        };
        if role.in_flight.is_some() {
            return Err((DriverError::RoleBusy, value));
        }
        let (source, class) = (role.source, role.class);
        let Some(next) = inbox.next_input.checked_add(1) else {
            return Err((DriverError::IdentityExhausted, value));
        };
        let input = InputId::new(next).expect("incremented role input id");
        if let Err(error) = inbox.mailbox.push(class, source, input) {
            return Err((error.into(), value));
        }
        inbox.next_input = next;
        inbox.inputs.insert(
            input,
            InputRecord {
                source,
                class,
                phase: InputPhase::Filled,
                role: Some(self.role),
            },
        );
        inbox.payloads.insert(input, value);
        inbox
            .roles
            .get_mut(&self.role)
            .expect("idle role retains its paid position")
            .in_flight = Some(input);
        self.runtime
            .shared
            .state
            .lock()
            .expect("runtime scheduler lock")
            .ready
            .notify(self.actor)
            .expect("live role owns its actor");
        drop(inbox);
        self.runtime.shared.changed.notify();
        Ok(input)
    }
    /// Positive exit from the role owner. Only idle roles can close, and the
    /// caller has already observed all role holder/capture and report exits.
    pub fn close_after_exit(&mut self) -> Result<(), DriverError> {
        if !self.active {
            return Err(DriverError::UnknownRole);
        }
        self.runtime
            .close_role_after_exit(self.actor, self.role, false)?;
        self.active = false;
        Ok(())
    }
}
impl<A: Actor> InputPermit<A> {
    pub const fn id(&self) -> InputId {
        self.input
    }
}
impl<A: Actor> InputPermit<A> {
    /// Converts a pure unused reservation into one persistent role authority.
    /// The class charge moves intact; the original InputId is permanently burned.
    pub fn into_role(self) -> Result<InputRole<A>, (DriverError, Self)> {
        let node = match self.runtime.node(self.actor) {
            Ok(node) => node,
            Err(error) => return Err((error, self)),
        };
        let mut inbox = node.inbox.lock().expect("actor inbox lock");
        let scope_failed = match node.scope.gates() {
            Ok(gates) => gates.failed,
            Err(error) => return Err((error.into(), self)),
        };
        if inbox.failed || scope_failed {
            return Err((DriverError::ActorFailed, self));
        }
        let Some(input) = inbox.inputs.get(&self.input) else {
            return Err((DriverError::UnknownInput, self));
        };
        if input.phase != InputPhase::Reserved {
            return Err((DriverError::InputNotReserved, self));
        }
        let role = InputRoleId(self.input);
        let (source, class) = (input.source, input.class);
        inbox.roles.insert(
            role,
            InputRoleRecord {
                source,
                class,
                in_flight: None,
            },
        );
        inbox.inputs.remove(&self.input);
        Ok(InputRole {
            runtime: self.runtime.clone(),
            actor: self.actor,
            role,
            active: true,
        })
    }
    /// Call before attaching any payload/capture. Bound positions cannot take unused revocation.
    pub fn bind(&self) -> Result<(), DriverError> {
        let node = self.runtime.node(self.actor)?;
        let mut inbox = node.inbox.lock().expect("actor inbox lock");
        let input = inbox
            .inputs
            .get_mut(&self.input)
            .ok_or(DriverError::UnknownInput)?;
        if input.phase != InputPhase::Reserved {
            return Err(DriverError::InputNotReserved);
        }
        input.phase = InputPhase::Bound;
        Ok(())
    }
    pub fn fill(self, value: A::Input) -> Result<InputId, (DriverError, A::Input)> {
        let node = match self.runtime.node(self.actor) {
            Ok(node) => node,
            Err(e) => return Err((e, value)),
        };
        let mut inbox = node.inbox.lock().expect("actor inbox lock");
        if inbox.failed {
            return Err((DriverError::ActorFailed, value));
        }
        let Some(input) = inbox.inputs.get(&self.input) else {
            return Err((DriverError::UnknownInput, value));
        };
        if !matches!(input.phase, InputPhase::Reserved | InputPhase::Bound) {
            return Err((DriverError::InputNotReserved, value));
        }
        let (class, source) = (input.class, input.source);
        if let Err(e) = inbox.mailbox.push(class, source, self.input) {
            return Err((e.into(), value));
        }
        inbox.payloads.insert(self.input, value);
        inbox
            .inputs
            .get_mut(&self.input)
            .expect("reserved input")
            .phase = InputPhase::Filled;
        self.runtime
            .shared
            .state
            .lock()
            .expect("runtime scheduler lock")
            .ready
            .notify(self.actor)
            .expect("live actor owns its reserved input");
        drop(inbox);
        self.runtime.shared.changed.notify();
        Ok(self.input)
    }
    /// Positive exit from the original holder, after its captures and necessary
    /// report consumers have retired. No command ever reached the actor, so
    /// this is unused-role withdrawal, not fictitious core consumption.
    pub fn revoke_bound_after_exit(self) -> Result<(), DriverError> {
        let node = self.runtime.node(self.actor)?;
        let mut inbox = node.inbox.lock().expect("actor inbox lock");
        let input = inbox
            .inputs
            .get(&self.input)
            .ok_or(DriverError::UnknownInput)?;
        if input.phase != InputPhase::Bound {
            return Err(DriverError::InputNotReserved);
        }
        let class = input.class;
        inbox.inputs.remove(&self.input);
        inbox.counts[class.index()] -= 1;
        Ok(())
    }
    /// The caller proves that no payload or capture has ever been bound.
    pub fn revoke_unused(self) -> Result<(), DriverError> {
        let node = self.runtime.node(self.actor)?;
        let mut inbox = node.inbox.lock().expect("actor inbox lock");
        let input = inbox
            .inputs
            .get(&self.input)
            .ok_or(DriverError::UnknownInput)?;
        if input.phase != InputPhase::Reserved {
            return Err(DriverError::InputNotReserved);
        }
        let class = input.class;
        inbox.inputs.remove(&self.input);
        inbox.counts[class.index()] -= 1;
        Ok(())
    }
}

/// An exclusive effect dispatch ticket. It grants no new work or capacity.
/// `descriptor.work` must name an already admitted WorkLease; a host still
/// calls WorkLease::start immediately before executing its body. The ticket is
/// neither physical-start nor physical-exit evidence.
#[derive(Debug)]
pub struct EffectTicket<E> {
    pub effect: E,
    pub actor: ActorId,
    pub input: InputId,
    pub generation: Generation,
    pub descriptor: EffectDescriptor,
}
/// Exclusive publication authority within one actual failure supervision job.
/// It signs no work and never reads the failed core. Each commit touches one
/// actual target; application failure policy and bounded inventory loops remain
/// in the host. Drop neither exits the cleanup work nor rolls back any value.
#[must_use = "A failure session belongs to its original running cleanup work"]
pub struct FailurePublicationSession<A: Actor> {
    runtime: ActorRuntime<A>,
    actor: ActorId,
    input: InputId,
    generation: Generation,
    action: ActionId,
    work: WorkId,
}
impl<A: Actor> std::fmt::Debug for FailurePublicationSession<A> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FailurePublicationSession")
            .field("actor", &self.actor)
            .field("generation", &self.generation)
            .field("work", &self.work)
            .finish()
    }
}
impl<A: Actor> FailurePublicationSession<A> {
    pub const fn generation(&self) -> Generation {
        self.generation
    }
    fn validate(
        &self,
        node: &ActorCell<A>,
        inbox: &Inbox<A>,
        ledger: &crate::transaction::LedgerState,
        target: &ScopeHandle,
    ) -> Result<ActionId, DriverError> {
        if node.scope.id() != target.id()
            || !Arc::ptr_eq(&node.scope.runtime.shared, &target.runtime.shared)
        {
            return Err(DriverError::PublicationTargetMismatch);
        }
        ActorRuntime::<A>::validate_failure_work(
            node,
            inbox,
            ledger,
            self.input,
            self.generation,
            self.action,
            self.work,
        )?;
        let claim = inbox
            .failure_publication
            .as_ref()
            .ok_or(DriverError::FailurePublicationRequired)?;
        if claim.generation != self.generation
            || claim.input != self.input
            || claim.action != self.action
            || claim.work != self.work
        {
            return Err(EffectError::StaleGeneration.into());
        }
        inbox
            .failure_action_watermark
            .checked_add(1)
            .and_then(ActionId::new)
            .ok_or(DriverError::IdentityExhausted)
    }
    pub fn commit_snapshot<T>(
        &mut self,
        target: &crate::publication::PublishedSnapshot<T>,
        next: Arc<T>,
    ) -> Result<Arc<T>, (DriverError, Arc<T>)> {
        let next = ManuallyDrop::new(next);
        let node = match self.runtime.node(self.actor) {
            Ok(node) => node,
            Err(error) => return Err((error, ManuallyDrop::into_inner(next))),
        };
        let mut inbox = node.inbox.lock().expect("actor inbox lock");
        let ledger = node
            .scope
            .runtime
            .shared
            .state
            .lock()
            .expect("runtime ledger lock");
        let action = match self.validate(&node, &inbox, &ledger, &target.owner) {
            Ok(action) => action,
            Err(error) => return Err((error, ManuallyDrop::into_inner(next))),
        };
        let mut value = target.value.lock().expect("published snapshot lock");
        inbox.failure_action_watermark = action.get();
        self.runtime.shared.record(HistoryFact::Published {
            scope: node.scope.id(),
            generation: self.generation,
            action,
        });
        // All fallible metadata work precedes the no-callback/no-destructor
        // actual swap. Failure and readers are still excluded by these gates.
        let old = ManuallyDrop::new(std::mem::replace(
            &mut *value,
            ManuallyDrop::into_inner(next),
        ));
        drop(value);
        drop(ledger);
        drop(inbox);
        self.runtime.shared.changed.notify();
        Ok(ManuallyDrop::into_inner(old))
    }
    pub fn commit_reply<T>(
        &mut self,
        target: &crate::publication::PublishedReply<T>,
        next: Arc<T>,
    ) -> Result<(), (DriverError, Arc<T>)> {
        let next = ManuallyDrop::new(next);
        let node = match self.runtime.node(self.actor) {
            Ok(node) => node,
            Err(error) => return Err((error, ManuallyDrop::into_inner(next))),
        };
        let mut inbox = node.inbox.lock().expect("actor inbox lock");
        let ledger = node
            .scope
            .runtime
            .shared
            .state
            .lock()
            .expect("runtime ledger lock");
        let action = match self.validate(&node, &inbox, &ledger, &target.owner) {
            Ok(action) => action,
            Err(error) => return Err((error, ManuallyDrop::into_inner(next))),
        };
        let mut reply = target.cell.lock().expect("published reply lock");
        if reply.phase != crate::publication::ReplyPhase::Empty || reply.value.is_some() {
            return Err((
                DriverError::ReplyAlreadyCommitted,
                ManuallyDrop::into_inner(next),
            ));
        }
        inbox.failure_action_watermark = action.get();
        self.runtime.shared.record(HistoryFact::Published {
            scope: node.scope.id(),
            generation: self.generation,
            action,
        });
        reply.phase = crate::publication::ReplyPhase::Committed;
        let previous = ManuallyDrop::new(reply.value.replace(ManuallyDrop::into_inner(next)));
        drop(reply);
        drop(ledger);
        drop(inbox);
        self.runtime.shared.changed.notify();
        drop(ManuallyDrop::into_inner(previous));
        Ok(())
    }
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ActorSnapshot {
    pub generation: Generation,
    pub failed: bool,
    pub inputs: usize,
    pub roles: usize,
    pub batches: usize,
    pub last_progress: Tick,
}

impl<A: Actor> ActorRuntime<A> {
    pub fn new(max_actors: usize) -> Self {
        Self::build(max_actors, None)
    }
    pub fn with_history(max_actors: usize, history: crate::history::HistoryCapture) -> Self {
        Self::build(max_actors, Some(history))
    }
    fn build(max_actors: usize, history: Option<crate::history::HistoryCapture>) -> Self {
        Self {
            shared: Arc::new(DriverShared {
                state: Mutex::new(DriverState {
                    actors: BTreeMap::new(),
                    ready: ReadyQueue::new(max_actors),
                    effect_ready: ReadyQueue::new(max_actors),
                    timers: TimerIndex::new(max_actors),
                    open: true,
                    now: Tick(0),
                }),
                changed: Notify::default(),
                history,
            }),
        }
    }
    fn node(&self, id: ActorId) -> Result<Arc<ActorCell<A>>, DriverError> {
        self.shared
            .state
            .lock()
            .expect("runtime scheduler lock")
            .actors
            .get(&id)
            .cloned()
            .ok_or(DriverError::UnknownActor)
    }
    pub fn register(
        &self,
        id: ActorId,
        scope: ScopeHandle,
        attachment: SlotId,
        actor: A,
        config: ActorConfig,
    ) -> Result<(), (DriverError, A)> {
        self.register_inner(id, scope, (Some(attachment), None), actor, config)
            .map(|_| ())
    }
    /// Production composition chooses and binds actor storage in one ledger
    /// transaction; no externally allocated slot can be overtaken before bind.
    pub fn register_next(
        &self,
        id: ActorId,
        scope: ScopeHandle,
        actor: A,
        config: ActorConfig,
    ) -> Result<SlotId, (DriverError, A)> {
        self.register_inner(id, scope, (None, None), actor, config)
    }
    /// Bind the original failure-cleanup role outside the actor's mutable
    /// state. Its pure failure hook receives only the role identity, even if
    /// handle panics halfway through mutating every protocol field.
    pub fn register_supervised_next(
        &self,
        id: ActorId,
        scope: ScopeHandle,
        actor: A,
        config: ActorConfig,
        failure_work: &crate::lease::WorkLease,
    ) -> Result<SlotId, (DriverError, A)> {
        if scope.id() != failure_work.scope().id()
            || !Arc::ptr_eq(&scope.runtime.shared, &failure_work.scope().runtime.shared)
        {
            return Err((DriverError::PublicationTargetMismatch, actor));
        }
        let valid = {
            let ledger = scope
                .runtime
                .shared
                .state
                .lock()
                .expect("runtime ledger lock");
            ledger
                .scopes
                .get(&scope.id())
                .and_then(|state| state.work(failure_work.id()))
                .is_some_and(|work| {
                    work.kind == WorkKind::Cleanup
                        && !work.unbound_guard
                        && matches!(
                            work.phase,
                            WorkPhase::Admitted | WorkPhase::Queued | WorkPhase::Started
                        )
                })
        };
        if !valid {
            return Err((DriverError::FailureWorkInactive, actor));
        }
        self.register_inner(id, scope, (None, Some(failure_work.id())), actor, config)
    }
    fn register_inner(
        &self,
        id: ActorId,
        scope: ScopeHandle,
        binding: (Option<SlotId>, Option<WorkId>),
        actor: A,
        config: ActorConfig,
    ) -> Result<SlotId, (DriverError, A)> {
        let config = match config.validate() {
            Ok(c) => c,
            Err(_) => return Err((DriverError::InvalidConfig, actor)),
        };
        let generation = match scope.gates() {
            Ok(s) => s.generation,
            Err(e) => return Err((e.into(), actor)),
        };
        let deadline = actor.next_deadline();
        let (attachment, failure_work) = binding;
        let attachment = match attachment {
            Some(slot) => match scope.attach_actor(slot) {
                Ok(()) => slot,
                Err(error) => return Err((error.into(), actor)),
            },
            None => match scope.attach_actor_next() {
                Ok(slot) => slot,
                Err(error) => return Err((error.into(), actor)),
            },
        };
        let mut state = self.shared.state.lock().expect("runtime scheduler lock");
        if !state.open {
            drop(state);
            scope
                .detach_actor(attachment)
                .expect("unbound actor attachment rolls back");
            return Err((DriverError::Closed, actor));
        }
        if let Err(error) = state.ready.register(id) {
            drop(state);
            scope
                .detach_actor(attachment)
                .expect("unbound actor attachment rolls back");
            return Err((error.into(), actor));
        }
        state
            .effect_ready
            .register(id)
            .expect("effect readiness has the same actor bound");
        if let Some(deadline) = deadline {
            state
                .timers
                .set(id, deadline)
                .expect("one timer per registered actor");
        }
        state.actors.insert(
            id,
            Arc::new(ActorCell {
                id,
                scope,
                attachment,
                config,
                failure_work,
                core: Mutex::new(Some(actor)),
                inbox: Mutex::new(Inbox {
                    payloads: BTreeMap::new(),
                    mailbox: MailboxState::new(config.mailbox),
                    inputs: BTreeMap::new(),
                    roles: BTreeMap::new(),
                    counts: [0; 4],
                    next_input: 0,
                    batches: BTreeMap::new(),
                    generation,
                    failed: false,
                    timer_input: None,
                    failure: None,
                    last_progress: Tick(0),
                    failure_publication: None,
                    failure_action_watermark: 0,
                }),
            }),
        );
        Ok(attachment)
    }
    /// Existing permits may still fill after external admission closes.
    pub fn reserve_input(
        &self,
        actor: ActorId,
        source: ActorId,
        class: InputClass,
    ) -> Result<InputPermit<A>, DriverError> {
        if class == InputClass::Timer {
            return Err(DriverError::InvalidConfig);
        }
        let node = self.node(actor)?;
        let mut inbox = node.inbox.lock().expect("actor inbox lock");
        if !self
            .shared
            .state
            .lock()
            .expect("runtime scheduler lock")
            .open
        {
            return Err(DriverError::Closed);
        }
        if inbox.failed {
            return Err(DriverError::ActorFailed);
        }
        if inbox.counts[class.index()] >= node.config.mailbox[class.index()] {
            return Err(DriverError::InputCapacity);
        }
        inbox.next_input = inbox
            .next_input
            .checked_add(1)
            .ok_or(DriverError::IdentityExhausted)?;
        let id = InputId::new(inbox.next_input).expect("incremented input id");
        inbox.inputs.insert(
            id,
            InputRecord {
                source,
                class,
                phase: InputPhase::Reserved,
                role: None,
            },
        );
        inbox.counts[class.index()] += 1;
        Ok(InputPermit {
            runtime: self.clone(),
            actor,
            input: id,
        })
    }
    pub fn snapshot(&self, actor: ActorId) -> Result<ActorSnapshot, DriverError> {
        let node = self.node(actor)?;
        let inbox = node.inbox.lock().expect("actor inbox lock");
        Ok(ActorSnapshot {
            generation: inbox.generation,
            failed: inbox.failed,
            inputs: inbox.inputs.len(),
            roles: inbox.roles.len(),
            batches: inbox.batches.len(),
            last_progress: inbox.last_progress,
        })
    }
    pub fn close_admission(&self) {
        self.shared
            .state
            .lock()
            .expect("runtime scheduler lock")
            .open = false;
        self.shared.changed.notify();
    }
    pub fn next_deadline(&self) -> Option<Tick> {
        self.shared
            .state
            .lock()
            .expect("runtime scheduler lock")
            .timers
            .next_deadline()
    }
    /// Both drivers call the same timer conversion. A delayed clock read cannot move process time backwards.
    pub fn advance_time(&self, observed: Tick, budget: usize) -> Result<usize, DriverError> {
        let (actors, now) = {
            let mut state = self.shared.state.lock().expect("runtime scheduler lock");
            let now = observed.max(state.now);
            state.now = now;
            if let Some(history) = &self.shared.history {
                history.observe_time(now);
            }
            (state.timers.advance(now, budget)?, now)
        };
        let mut filled = 0;
        for actor in actors {
            let node = self.node(actor)?;
            let timer_value = A::timer_input(now);
            let mut inbox = node.inbox.lock().expect("actor inbox lock");
            if inbox.failed {
                self.shared
                    .state
                    .lock()
                    .expect("runtime scheduler lock")
                    .timers
                    .acknowledge_tick(actor);
                continue;
            }
            if inbox.timer_input.is_some() {
                continue;
            }
            // The timer has its own admitted position, never an ordinary request slot.
            if inbox.counts[InputClass::Timer.index()]
                >= node.config.mailbox[InputClass::Timer.index()]
            {
                return Err(DriverError::InputCapacity);
            }
            inbox.next_input = inbox
                .next_input
                .checked_add(1)
                .ok_or(DriverError::IdentityExhausted)?;
            let input = InputId::new(inbox.next_input).expect("incremented timer input id");
            inbox.mailbox.push(InputClass::Timer, actor, input)?;
            inbox.inputs.insert(
                input,
                InputRecord {
                    source: actor,
                    class: InputClass::Timer,
                    phase: InputPhase::Filled,
                    role: None,
                },
            );
            inbox.payloads.insert(input, timer_value);
            inbox.counts[InputClass::Timer.index()] += 1;
            inbox.timer_input = Some(input);
            self.shared
                .state
                .lock()
                .expect("runtime scheduler lock")
                .ready
                .notify(actor)?;
            filled += 1;
        }
        if filled != 0 {
            self.shared.changed.notify();
        }
        Ok(filled)
    }
    /// Executes one finite actor turn using an explicit simulation time. Every
    /// input still clamps this value against the shared scheduler's latest time.
    pub fn poll_one(&self, observed: Tick) -> Result<bool, DriverError> {
        self.poll_one_inner(observed, None)
    }
    /// Production input steps observe time independently. Clock sampling occurs
    /// after removing the input and outside inbox, ledger, scheduler and core
    /// locks; only the clamped Tick value enters the pure actor.
    pub fn poll_one_with_clock(&self, clock: &dyn Clock) -> Result<bool, DriverError> {
        self.poll_one_inner(clock.now(), Some(clock))
    }
    fn poll_one_inner(
        &self,
        observed: Tick,
        clock: Option<&dyn Clock>,
    ) -> Result<bool, DriverError> {
        let node = {
            let mut state = self.shared.state.lock().expect("runtime scheduler lock");
            state.now = observed.max(state.now);
            if let Some(history) = &self.shared.history {
                history.observe_time(state.now);
            }
            let Some(actor) = state.ready.claim() else {
                return Ok(false);
            };
            self.shared.record(HistoryFact::Claimed { actor });
            state
                .actors
                .get(&actor)
                .cloned()
                .expect("registered ready actor")
        };
        let mut budget = TurnBudget::new(node.config.turn_budget)?;
        loop {
            let next = {
                let mut inbox = node.inbox.lock().expect("actor inbox lock");
                if inbox.failed || inbox.batches.len() >= node.config.max_batches {
                    None
                } else {
                    let ready =
                        std::array::from_fn(|i| inbox.mailbox.has_ready(InputClass::ALL[i]));
                    let mut candidate = budget.clone();
                    let class = candidate.next(ready);
                    class
                        .filter(|_| candidate.charge(node.config.max_step_work))
                        .and_then(|class| {
                            budget = candidate;
                            inbox.mailbox.pop(class)
                        })
                        .map(|input| {
                            inbox
                                .inputs
                                .get_mut(&input.input)
                                .expect("filled input")
                                .phase = InputPhase::Consuming;
                            let value = inbox
                                .payloads
                                .remove(&input.input)
                                .expect("filled input owns its neutral payload");
                            (input, value)
                        })
                }
            };
            let Some((input, value)) = next else {
                break;
            };
            let observed = clock.map_or(observed, Clock::now);
            let now = {
                let mut state = self.shared.state.lock().expect("runtime scheduler lock");
                state.now = observed.max(state.now);
                if let Some(history) = &self.shared.history {
                    history.observe_time(state.now);
                }
                state.now
            };
            let generation = node.inbox.lock().expect("actor inbox lock").generation;
            let mut step = StepContext::new(
                &mut budget,
                &node.scope,
                generation,
                input.input,
                node.config.max_effects_per_step,
                node.config.max_dependencies_per_effect,
                node.config.max_step_work,
            );
            step.set_failure_work(node.failure_work);
            let result = {
                let mut core = node.core.lock().expect("actor core lock");
                let core = core.as_mut().expect("registered pure actor");
                catch_unwind(AssertUnwindSafe(|| {
                    core.handle(now, input, value, &mut step);
                    core.next_deadline()
                }))
            };
            let deadline = match result {
                Ok(deadline) => {
                    let batch = step.finish();
                    let mut inbox = node.inbox.lock().expect("actor inbox lock");
                    inbox.batches.insert(input.input, batch);
                    inbox
                        .inputs
                        .get_mut(&input.input)
                        .expect("consumed input")
                        .phase = InputPhase::Committed;
                    inbox.last_progress = now;
                    self.shared.record(HistoryFact::InputConsumed {
                        actor: node.id,
                        input: input.input,
                    });
                    self.shared.record(HistoryFact::BatchCommitted {
                        scope: node.scope.id(),
                        input: input.input,
                    });
                    deadline
                }
                Err(payload) => {
                    // The failed pure state stays in its cell until the host removes it.
                    let mut inbox = node.inbox.lock().expect("actor inbox lock");
                    let generation = node.scope.fail()?;
                    inbox.generation = generation;
                    inbox.failed = true;
                    inbox.failure = Some(ManuallyDrop::new(payload));
                    drop(inbox);
                    drop(step);
                    let mut failure_budget = TurnBudget::new(node.config.turn_budget)?;
                    let _ = failure_budget.next([true, false, false, false]);
                    assert!(
                        failure_budget.charge(node.config.max_step_work),
                        "failure projection has its reserved finite step budget"
                    );
                    let mut failure_step = StepContext::new(
                        &mut failure_budget,
                        &node.scope,
                        generation,
                        input.input,
                        node.config.max_effects_per_step,
                        node.config.max_dependencies_per_effect,
                        node.config.max_step_work,
                    );
                    failure_step.set_failure_work(node.failure_work);
                    // A failure projection panic is a shared runtime invariant failure.
                    A::owner_failed(node.scope.id(), generation, input, &mut failure_step);
                    let mut inbox = node.inbox.lock().expect("actor inbox lock");
                    let batch = failure_step.finish();
                    inbox.failure_action_watermark =
                        batch.state.last_action_id().map_or(0, ActionId::get);
                    inbox.batches.insert(input.input, batch);
                    inbox
                        .inputs
                        .get_mut(&input.input)
                        .expect("failed input custody")
                        .phase = InputPhase::AbandonedByOwnerFailure;
                    self.shared.record(HistoryFact::InputAbandoned {
                        actor: node.id,
                        input: input.input,
                    });
                    inbox.last_progress = now;
                    self.shared.record(HistoryFact::BatchCommitted {
                        scope: node.scope.id(),
                        input: input.input,
                    });
                    None
                }
            };
            let mut state = self.shared.state.lock().expect("runtime scheduler lock");
            state.effect_ready.notify(node.id)?;
            if let Some(deadline) = deadline {
                state.timers.set(node.id, deadline)?;
            } else {
                state.timers.cancel(node.id);
            }
        }
        {
            let inbox = node.inbox.lock().expect("actor inbox lock");
            let pending = !inbox.failed
                && !inbox.mailbox.is_empty()
                && inbox.batches.len() < node.config.max_batches;
            self.shared
                .state
                .lock()
                .expect("runtime scheduler lock")
                .ready
                .release(node.id, pending)?;
        }
        self.shared.changed.notify();
        Ok(true)
    }
    pub fn ready_effects(
        &self,
        actor: ActorId,
        input: InputId,
        budget: usize,
    ) -> Result<Vec<ActionId>, DriverError> {
        let node = self.node(actor)?;
        let mut inbox = node.inbox.lock().expect("actor inbox lock");
        let generation = inbox.generation;
        Ok(inbox
            .batches
            .get_mut(&input)
            .ok_or(DriverError::UnknownInput)?
            .state
            .poll_ready(budget, generation)?)
    }
    /// Actual bounded dependency metadata, also available to failed-owner
    /// cleanup. Payload custody stays exclusively in the original batch.
    pub fn effect_inventory(
        &self,
        actor: ActorId,
        input: InputId,
        after: Option<ActionId>,
        budget: usize,
    ) -> Result<Vec<(EffectDescriptor, EffectState)>, DriverError> {
        let node = self.node(actor)?;
        let inbox = node.inbox.lock().expect("actor inbox lock");
        Ok(inbox
            .batches
            .get(&input)
            .ok_or(DriverError::UnknownInput)?
            .state
            .records(after, budget))
    }
    /// Start is synchronized with the same failure fence as batch commitment.
    pub fn take_effect(
        &self,
        actor: ActorId,
        input: InputId,
        action: ActionId,
    ) -> Result<EffectTicket<A::Effect>, DriverError> {
        let node = self.node(actor)?;
        let mut inbox = node.inbox.lock().expect("actor inbox lock");
        let scope = node.scope.gates()?;
        let generation = scope.generation;
        let batch = inbox
            .batches
            .get_mut(&input)
            .ok_or(DriverError::UnknownInput)?;
        if scope.stopped
            && batch.state.record(action).is_some_and(|effect| {
                effect.descriptor().kind
                    == novarocks_control_runtime_core::effect::EffectKind::Business
            })
        {
            return Err(EffectError::NotReady.into());
        }
        batch.state.start(action, generation)?;
        let effect = batch
            .payloads
            .remove(&action)
            .expect("pending batch action owns its neutral payload");
        Ok(EffectTicket {
            effect,
            actor,
            input,
            generation,
            descriptor: batch
                .state
                .record(action)
                .expect("started action")
                .descriptor()
                .clone(),
        })
    }
    /// Move the original still-unbound reservation into its actual host job.
    /// Starting the exact committed effect and taking its guard share the
    /// generation gate; no host side counter or reconstructed permit is used.
    pub fn take_step_reservation(
        &self,
        ticket: &EffectTicket<A::Effect>,
        reservation: ReservationId,
    ) -> Result<crate::lease::StepGuard, DriverError> {
        let node = self.node(ticket.actor)?;
        let mut inbox = node.inbox.lock().expect("actor inbox lock");
        let ledger = node
            .scope
            .runtime
            .shared
            .state
            .lock()
            .expect("runtime ledger lock");
        self.validate_publication(&node, &inbox, &ledger, ticket, &node.scope)?;
        let batch = inbox
            .batches
            .get_mut(&ticket.input)
            .ok_or(DriverError::UnknownInput)?;
        let guard = batch
            .reservations
            .get(&reservation)
            .ok_or(DriverError::UnknownReservation)?;
        if guard.work_id() != ticket.descriptor.work {
            return Err(DriverError::ReservationOwnerMismatch);
        }
        Ok(batch
            .reservations
            .remove(&reservation)
            .expect("validated original reservation"))
    }
    /// Bounded inventory of the actual guards retained by one committed step.
    /// This remains readable after owner failure without reading its core.
    pub fn step_reservations(
        &self,
        actor: ActorId,
        input: InputId,
        after: Option<ReservationId>,
        budget: usize,
    ) -> Result<Vec<(ReservationId, WorkId)>, DriverError> {
        let node = self.node(actor)?;
        let inbox = node.inbox.lock().expect("actor inbox lock");
        let batch = inbox.batches.get(&input).ok_or(DriverError::UnknownInput)?;
        let lower = after.map_or(std::ops::Bound::Unbounded, std::ops::Bound::Excluded);
        Ok(batch
            .reservations
            .range((lower, std::ops::Bound::Unbounded))
            .take(budget)
            .map(|(id, guard)| (*id, guard.work_id()))
            .collect())
    }
    /// An unstarted reservation has no payload or captures. The failure owner
    /// takes the original guard and rolls it back outside the inbox lock.
    pub fn take_failed_reservation(
        &self,
        actor: ActorId,
        input: InputId,
        reservation: ReservationId,
    ) -> Result<crate::lease::StepGuard, DriverError> {
        let node = self.node(actor)?;
        let mut inbox = node.inbox.lock().expect("actor inbox lock");
        if !inbox.failed {
            return Err(DriverError::ActorNotRetired);
        }
        inbox
            .batches
            .get_mut(&input)
            .ok_or(DriverError::UnknownInput)?
            .reservations
            .remove(&reservation)
            .ok_or(DriverError::UnknownReservation)
    }
    fn validate_failure_work(
        node: &ActorCell<A>,
        inbox: &Inbox<A>,
        ledger: &crate::transaction::LedgerState,
        input: InputId,
        generation: Generation,
        action: ActionId,
        work: WorkId,
    ) -> Result<(), DriverError> {
        let scope = ledger
            .scopes
            .get(&node.scope.id())
            .ok_or(RuntimeError::UnknownScope)?;
        if !inbox.failed
            || !scope.failed()
            || inbox.generation != generation
            || scope.generation() != generation
        {
            return Err(EffectError::StaleGeneration.into());
        }
        let batch = inbox.batches.get(&input).ok_or(DriverError::UnknownInput)?;
        if batch.state.generation() != generation {
            return Err(EffectError::StaleGeneration.into());
        }
        let effect = batch
            .state
            .record(action)
            .ok_or(EffectError::UnknownAction)?;
        if effect.state() != EffectState::Started
            || effect.descriptor().kind != EffectKind::Cleanup
            || effect.descriptor().work != work
        {
            return Err(EffectError::InvalidTransition.into());
        }
        let work = scope.work(work).ok_or(DriverError::FailureWorkInactive)?;
        if work.kind != WorkKind::Cleanup || work.phase != WorkPhase::Started || work.body_exited {
            return Err(DriverError::FailureWorkInactive);
        }
        Ok(())
    }
    /// Claims one failure-generation publication authority from the actual
    /// started cleanup effect and its prepaid, still-running supervision work.
    /// The original ticket remains Started until the host completes that work.
    pub fn claim_failure_publication(
        &self,
        ticket: &EffectTicket<A::Effect>,
    ) -> Result<FailurePublicationSession<A>, DriverError> {
        let node = self.node(ticket.actor)?;
        let mut inbox = node.inbox.lock().expect("actor inbox lock");
        let ledger = node
            .scope
            .runtime
            .shared
            .state
            .lock()
            .expect("runtime ledger lock");
        Self::validate_failure_work(
            &node,
            &inbox,
            &ledger,
            ticket.input,
            ticket.generation,
            ticket.descriptor.id,
            ticket.descriptor.work,
        )?;
        let effect = inbox
            .batches
            .get(&ticket.input)
            .expect("validated failure batch")
            .state
            .record(ticket.descriptor.id)
            .expect("validated failure effect");
        if effect.descriptor() != &ticket.descriptor {
            return Err(EffectError::InvalidTransition.into());
        }
        if inbox.failure_publication.is_some() {
            return Err(DriverError::FailurePublicationAlreadyClaimed);
        }
        inbox.failure_publication = Some(FailurePublicationRecord {
            generation: ticket.generation,
            input: ticket.input,
            action: ticket.descriptor.id,
            work: ticket.descriptor.work,
        });
        Ok(FailurePublicationSession {
            runtime: self.clone(),
            actor: ticket.actor,
            input: ticket.input,
            generation: ticket.generation,
            action: ticket.descriptor.id,
            work: ticket.descriptor.work,
        })
    }
    fn validate_publication(
        &self,
        node: &ActorCell<A>,
        inbox: &Inbox<A>,
        ledger: &crate::transaction::LedgerState,
        ticket: &EffectTicket<A::Effect>,
        target: &ScopeHandle,
    ) -> Result<(), DriverError> {
        if node.scope.id() != target.id()
            || !Arc::ptr_eq(&node.scope.runtime.shared, &target.runtime.shared)
        {
            return Err(DriverError::PublicationTargetMismatch);
        }
        let scope = ledger
            .scopes
            .get(&node.scope.id())
            .ok_or(RuntimeError::UnknownScope)?;
        let batch = inbox
            .batches
            .get(&ticket.input)
            .ok_or(DriverError::UnknownInput)?;
        if inbox.failed && ticket.generation == inbox.generation {
            return Err(DriverError::FailurePublicationRequired);
        }
        if scope.generation() != ticket.generation || batch.state.generation() != ticket.generation
        {
            return Err(EffectError::StaleGeneration.into());
        }
        let action = batch
            .state
            .record(ticket.descriptor.id)
            .ok_or(EffectError::UnknownAction)?;
        if action.descriptor() != &ticket.descriptor || action.state() != EffectState::Started {
            return Err(EffectError::InvalidTransition.into());
        }
        Ok(())
    }
    /// Commits the actual snapshot value and publication fact under the same
    /// short scope-failure gate. Both rejected new values and replaced old
    /// values are returned to the host for destruction after all locks release.
    pub fn commit_snapshot<T>(
        &self,
        ticket: &EffectTicket<A::Effect>,
        target: &crate::publication::PublishedSnapshot<T>,
        next: Arc<T>,
    ) -> Result<Arc<T>, (DriverError, Arc<T>)> {
        let next = ManuallyDrop::new(next);
        let node = match self.node(ticket.actor) {
            Ok(node) => node,
            Err(error) => return Err((error, ManuallyDrop::into_inner(next))),
        };
        let mut inbox = node.inbox.lock().expect("actor inbox lock");
        let ledger = node
            .scope
            .runtime
            .shared
            .state
            .lock()
            .expect("runtime ledger lock");
        if let Err(error) = self.validate_publication(&node, &inbox, &ledger, ticket, &target.owner)
        {
            return Err((error, ManuallyDrop::into_inner(next)));
        }
        let mut value = target.value.lock().expect("published snapshot lock");
        let published = inbox
            .batches
            .get_mut(&ticket.input)
            .expect("validated publication batch")
            .state
            .published(ticket.descriptor.id);
        if let Err(error) = published {
            return Err((error.into(), ManuallyDrop::into_inner(next)));
        }
        self.shared.record(HistoryFact::Published {
            scope: node.scope.id(),
            generation: ticket.generation,
            action: ticket.descriptor.id,
        });
        // Every validation and potentially failing observation precedes this
        // no-callback, no-destructor commit tail. Readers remain behind value's lock.
        let old = ManuallyDrop::new(std::mem::replace(
            &mut *value,
            ManuallyDrop::into_inner(next),
        ));
        drop(value);
        drop(ledger);
        self.shared
            .state
            .lock()
            .expect("runtime scheduler lock")
            .effect_ready
            .notify(ticket.actor)
            .expect("live publication actor");
        drop(inbox);
        self.shared.changed.notify();
        Ok(ManuallyDrop::into_inner(old))
    }
    /// Publishes the actual once-only reply outbox. Network sending follows
    /// take_once outside this gate; failure cannot revoke an already committed reply.
    pub fn commit_reply<T>(
        &self,
        ticket: &EffectTicket<A::Effect>,
        target: &crate::publication::PublishedReply<T>,
        next: Arc<T>,
    ) -> Result<(), (DriverError, Arc<T>)> {
        let next = ManuallyDrop::new(next);
        let node = match self.node(ticket.actor) {
            Ok(node) => node,
            Err(error) => return Err((error, ManuallyDrop::into_inner(next))),
        };
        let mut inbox = node.inbox.lock().expect("actor inbox lock");
        let ledger = node
            .scope
            .runtime
            .shared
            .state
            .lock()
            .expect("runtime ledger lock");
        if let Err(error) = self.validate_publication(&node, &inbox, &ledger, ticket, &target.owner)
        {
            return Err((error, ManuallyDrop::into_inner(next)));
        }
        let mut reply = target.cell.lock().expect("published reply lock");
        if reply.phase != crate::publication::ReplyPhase::Empty || reply.value.is_some() {
            return Err((
                DriverError::ReplyAlreadyCommitted,
                ManuallyDrop::into_inner(next),
            ));
        }
        let published = inbox
            .batches
            .get_mut(&ticket.input)
            .expect("validated publication batch")
            .state
            .published(ticket.descriptor.id);
        if let Err(error) = published {
            return Err((error.into(), ManuallyDrop::into_inner(next)));
        }
        self.shared.record(HistoryFact::Published {
            scope: node.scope.id(),
            generation: ticket.generation,
            action: ticket.descriptor.id,
        });
        reply.phase = crate::publication::ReplyPhase::Committed;
        let previous = ManuallyDrop::new(reply.value.replace(ManuallyDrop::into_inner(next)));
        drop(reply);
        drop(ledger);
        self.shared
            .state
            .lock()
            .expect("runtime scheduler lock")
            .effect_ready
            .notify(ticket.actor)
            .expect("live publication actor");
        drop(inbox);
        self.shared.changed.notify();
        drop(ManuallyDrop::into_inner(previous));
        Ok(())
    }
    /// Records an externally established I/O fact, which may be observed late.
    /// This grants no permission to mutate visible state and cannot establish a
    /// generation fence. Runtime projections/replies use commit_snapshot/commit_reply.
    pub fn observe_external_publication(
        &self,
        ticket: &EffectTicket<A::Effect>,
    ) -> Result<(), DriverError> {
        let node = self.node(ticket.actor)?;
        let mut inbox = node.inbox.lock().expect("actor inbox lock");
        inbox
            .batches
            .get_mut(&ticket.input)
            .ok_or(DriverError::UnknownInput)?
            .state
            .published(ticket.descriptor.id)?;
        self.shared.record(HistoryFact::Published {
            scope: node.scope.id(),
            generation: ticket.generation,
            action: ticket.descriptor.id,
        });
        self.shared
            .state
            .lock()
            .expect("runtime scheduler lock")
            .effect_ready
            .notify(ticket.actor)?;
        drop(inbox);
        self.shared.changed.notify();
        Ok(())
    }
    pub fn effect_completed(&self, ticket: EffectTicket<A::Effect>) -> Result<(), DriverError> {
        self.node(ticket.actor)?
            .inbox
            .lock()
            .expect("actor inbox lock")
            .batches
            .get_mut(&ticket.input)
            .ok_or(DriverError::UnknownInput)?
            .state
            .complete(ticket.descriptor.id)?;
        self.shared
            .state
            .lock()
            .expect("runtime scheduler lock")
            .effect_ready
            .notify(ticket.actor)?;
        self.shared.changed.notify();
        Ok(())
    }
    /// The caller acknowledges the exact declared predecessor condition, after its real publication or completion fact.
    pub fn acknowledge_dependency(
        &self,
        actor: ActorId,
        input: InputId,
        action: ActionId,
        predecessor: ActionId,
    ) -> Result<(), DriverError> {
        self.node(actor)?
            .inbox
            .lock()
            .expect("actor inbox lock")
            .batches
            .get_mut(&input)
            .ok_or(DriverError::UnknownInput)?
            .state
            .dependency_published(action, predecessor)?;
        self.shared
            .state
            .lock()
            .expect("runtime scheduler lock")
            .effect_ready
            .notify(actor)?;
        self.shared.changed.notify();
        Ok(())
    }
    pub fn abandon_unstarted_effect(
        &self,
        actor: ActorId,
        input: InputId,
        action: ActionId,
    ) -> Result<(), DriverError> {
        let node = self.node(actor)?;
        let mut inbox = node.inbox.lock().expect("actor inbox lock");
        let generation = inbox.generation;
        inbox
            .batches
            .get_mut(&input)
            .ok_or(DriverError::UnknownInput)?
            .state
            .abandon_unstarted(action, generation)?;
        Ok(())
    }
    /// Called by the host failure owner in bounded slices; never pretends the failed core consumed an input.
    pub fn abandon_failed_input(&self, actor: ActorId, input: InputId) -> Result<(), DriverError> {
        let node = self.node(actor)?;
        let mut inbox = node.inbox.lock().expect("actor inbox lock");
        if !inbox.failed {
            return Err(DriverError::ActorNotRetired);
        }
        let record = inbox
            .inputs
            .get_mut(&input)
            .ok_or(DriverError::UnknownInput)?;
        if record.phase == InputPhase::Filled {
            return Err(DriverError::InputStillQueued);
        }
        if record.phase == InputPhase::Committed {
            return Err(DriverError::EffectsRemain);
        }
        if record.phase != InputPhase::AbandonedByOwnerFailure {
            record.phase = InputPhase::AbandonedByOwnerFailure;
            self.shared
                .record(HistoryFact::InputAbandoned { actor, input });
        }
        Ok(())
    }
    pub fn take_failed_input(
        &self,
        actor: ActorId,
        class: InputClass,
    ) -> Result<Option<(InputEnvelope, A::Input)>, DriverError> {
        let node = self.node(actor)?;
        let mut inbox = node.inbox.lock().expect("actor inbox lock");
        if !inbox.failed {
            return Err(DriverError::ActorNotRetired);
        }
        let input = inbox.mailbox.pop(class);
        Ok(input.map(|input| {
            inbox
                .inputs
                .get_mut(&input.input)
                .expect("failed filled input")
                .phase = InputPhase::AbandonedByOwnerFailure;
            self.shared.record(HistoryFact::InputAbandoned {
                actor,
                input: input.input,
            });
            let value = inbox
                .payloads
                .remove(&input.input)
                .expect("failed filled input owns payload");
            (input, value)
        }))
    }
    pub fn take_failure_payload(
        &self,
        actor: ActorId,
    ) -> Result<Option<Box<dyn Any + Send>>, DriverError> {
        let node = self.node(actor)?;
        let payload = node.inbox.lock().expect("actor inbox lock").failure.take();
        Ok(payload.map(ManuallyDrop::into_inner))
    }
    /// External captures and all report consumers must already have retired before this release.
    pub fn retire_input(&self, actor: ActorId, input: InputId) -> Result<(), DriverError> {
        let node = self.node(actor)?;
        let mut inbox = node.inbox.lock().expect("actor inbox lock");
        let record = inbox.inputs.get(&input).ok_or(DriverError::UnknownInput)?;
        if !matches!(
            record.phase,
            InputPhase::Committed | InputPhase::AbandonedByOwnerFailure
        ) {
            return Err(DriverError::InputStillQueued);
        }
        if inbox
            .batches
            .get(&input)
            .is_some_and(|batch| !batch.state.all_terminal())
        {
            return Err(DriverError::EffectsRemain);
        }
        if inbox
            .batches
            .get(&input)
            .is_some_and(|batch| !batch.reservations.is_empty())
        {
            return Err(DriverError::ReservationsRemain);
        }
        let class = record.class;
        let role = record.role;
        if let Some(role) = role {
            let owner = inbox.roles.get(&role).ok_or(DriverError::UnknownRole)?;
            if owner.in_flight != Some(input) {
                return Err(DriverError::InputNotReserved);
            }
        }
        inbox.batches.remove(&input);
        inbox.inputs.remove(&input);
        if let Some(role) = role {
            inbox
                .roles
                .get_mut(&role)
                .expect("in-flight role retains its position")
                .in_flight = None;
        } else {
            inbox.counts[class.index()] -= 1;
        }
        if inbox.timer_input == Some(input) {
            inbox.timer_input = None;
            self.shared
                .state
                .lock()
                .expect("runtime scheduler lock")
                .timers
                .acknowledge_tick(actor);
        }
        if !inbox.failed && !inbox.mailbox.is_empty() {
            self.shared
                .state
                .lock()
                .expect("runtime scheduler lock")
                .ready
                .notify(actor)?;
        }
        drop(inbox);
        self.shared.changed.notify();
        Ok(())
    }
    /// One host dispatcher claims an actor; no request thread scans the actor directory.
    pub fn claim_effect_actor(&self) -> Option<ActorId> {
        self.shared
            .state
            .lock()
            .expect("runtime scheduler lock")
            .effect_ready
            .claim()
    }
    pub fn release_effect_actor(
        &self,
        actor: ActorId,
        has_runnable: bool,
    ) -> Result<(), DriverError> {
        self.shared
            .state
            .lock()
            .expect("runtime scheduler lock")
            .effect_ready
            .release(actor, has_runnable)?;
        self.shared.changed.notify();
        Ok(())
    }
    /// Bounded projection of actual runtime batches, including timer-generated batches.
    pub fn pending_batches(
        &self,
        actor: ActorId,
        after: Option<InputId>,
        budget: usize,
    ) -> Result<Vec<InputId>, DriverError> {
        let node = self.node(actor)?;
        let inbox = node.inbox.lock().expect("actor inbox lock");
        let lower = after.map_or(std::ops::Bound::Unbounded, std::ops::Bound::Excluded);
        Ok(inbox
            .batches
            .range((lower, std::ops::Bound::Unbounded))
            .take(budget)
            .map(|(input, _)| *input)
            .collect())
    }
    /// Actual input custody is available to bounded scope-failure teardown, never from core private tables.
    pub fn input_inventory(
        &self,
        actor: ActorId,
        after: Option<InputId>,
        budget: usize,
    ) -> Result<Vec<InputCustody>, DriverError> {
        let node = self.node(actor)?;
        let inbox = node.inbox.lock().expect("actor inbox lock");
        let lower = after.map_or(std::ops::Bound::Unbounded, std::ops::Bound::Excluded);
        Ok(inbox
            .inputs
            .range((lower, std::ops::Bound::Unbounded))
            .take(budget)
            .map(|(input, record)| InputCustody {
                input: *input,
                source: record.source,
                class: record.class,
                phase: record.phase,
                role: record.role,
            })
            .collect())
    }
    /// Enumerates actual role custody in bounded slices, including idle roles
    /// whose authority was dropped. No payload or protocol-private state is read.
    pub fn role_inventory(
        &self,
        actor: ActorId,
        after: Option<InputRoleId>,
        budget: usize,
    ) -> Result<Vec<InputRoleCustody>, DriverError> {
        let node = self.node(actor)?;
        let inbox = node.inbox.lock().expect("actor inbox lock");
        let lower = after.map_or(std::ops::Bound::Unbounded, std::ops::Bound::Excluded);
        Ok(inbox
            .roles
            .range((lower, std::ops::Bound::Unbounded))
            .take(budget)
            .map(|(role, record)| InputRoleCustody {
                role: *role,
                source: record.source,
                class: record.class,
                in_flight: record.in_flight,
            })
            .collect())
    }
    fn close_role_after_exit(
        &self,
        actor: ActorId,
        role: InputRoleId,
        require_failed: bool,
    ) -> Result<(), DriverError> {
        let node = self.node(actor)?;
        let mut inbox = node.inbox.lock().expect("actor inbox lock");
        if require_failed && !inbox.failed && !node.scope.gates()?.failed {
            return Err(DriverError::ActorNotRetired);
        }
        let record = inbox.roles.get(&role).ok_or(DriverError::UnknownRole)?;
        if record.in_flight.is_some() {
            return Err(DriverError::RoleBusy);
        }
        let class = record.class;
        inbox.roles.remove(&role);
        inbox.counts[class.index()] -= 1;
        drop(inbox);
        self.shared.changed.notify();
        Ok(())
    }
    /// Scope-failure cleanup can retire a lost role authority after observing
    /// its actual owner exit. Failure alone never releases an in-flight input.
    pub fn close_failed_role_after_exit(
        &self,
        actor: ActorId,
        role: InputRoleId,
    ) -> Result<(), DriverError> {
        self.close_role_after_exit(actor, role, true)
    }
    /// Returns pure state to its host owner; this method does not drop it on a poller.
    /// Work join is only one prerequisite. The actor attachment itself stays
    /// in scope storage until all actual input/effect custody has retired here.
    pub fn remove_actor(&self, actor: ActorId) -> Result<A, DriverError> {
        let node = self.node(actor)?;
        let inbox = node.inbox.lock().expect("actor inbox lock");
        if !inbox.inputs.is_empty()
            || !inbox.roles.is_empty()
            || !inbox.batches.is_empty()
            || inbox.failure.is_some()
            || !node.scope.join_ready()?
        {
            return Err(DriverError::ActorNotRetired);
        }
        let mut state = self.shared.state.lock().expect("runtime scheduler lock");
        if !state.ready.is_idle(actor)? || !state.effect_ready.is_idle(actor)? {
            return Err(SchedulerError::Busy.into());
        }
        state.ready.unregister(actor)?;
        state.effect_ready.unregister(actor)?;
        state.timers.cancel(actor);
        state.timers.acknowledge_tick(actor);
        state.actors.remove(&actor);
        drop(state);
        node.scope.detach_actor(node.attachment)?;
        Ok(node
            .core
            .lock()
            .expect("actor core lock")
            .take()
            .expect("one core owner"))
    }
    pub fn is_empty(&self) -> bool {
        self.shared
            .state
            .lock()
            .expect("runtime scheduler lock")
            .actors
            .is_empty()
    }
    pub fn change_version(&self) -> u64 {
        self.shared.changed.version()
    }
    pub fn wait_after(&self, observed: u64) {
        self.shared.changed.wait_after(observed);
    }
    pub fn notify_pollers(&self) {
        self.shared.changed.notify();
    }
}

/// The only environment operation used by a production poller is its supplied monotonic clock.
pub trait Clock: Send + Sync + 'static {
    fn now(&self) -> Tick;
}

#[cfg(not(loom))]
pub struct ProcessClock {
    origin: std::time::Instant,
}
#[cfg(not(loom))]
impl Default for ProcessClock {
    fn default() -> Self {
        Self {
            origin: std::time::Instant::now(),
        }
    }
}
#[cfg(not(loom))]
impl Clock for ProcessClock {
    fn now(&self) -> Tick {
        Tick(u64::try_from(self.origin.elapsed().as_nanos()).unwrap_or(u64::MAX))
    }
}

/// Poller ownership is retained until explicit, fully drained shutdown and real thread joins.
#[cfg(not(loom))]
pub struct PollerPool<A: Actor> {
    runtime: ActorRuntime<A>,
    stop: Arc<crate::sync::AtomicBool>,
    workers: Vec<std::thread::JoinHandle<()>>,
    stopped: bool,
}
#[cfg(not(loom))]
impl<A: Actor> PollerPool<A> {
    pub fn start(
        runtime: ActorRuntime<A>,
        clock: Arc<dyn Clock>,
        workers: usize,
    ) -> Result<Self, std::io::Error> {
        if workers == 0 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "control poller count must be positive",
            ));
        }
        let stop = Arc::new(crate::sync::AtomicBool::new(false));
        let mut threads = Vec::new();
        for index in 0..workers {
            let worker_runtime = runtime.clone();
            let worker_stop = stop.clone();
            let worker_clock = clock.clone();
            let worker = std::thread::Builder::new()
                .name(format!("control-poller-{index}"))
                .spawn(move || {
                    let result = catch_unwind(AssertUnwindSafe(|| {
                        while !worker_stop.load(crate::sync::Ordering::Acquire) {
                            let version = worker_runtime.change_version();
                            let now = worker_clock.now();
                            worker_runtime
                                .advance_time(now, 64)
                                .unwrap_or_else(|_| std::process::abort());
                            if worker_runtime
                                .poll_one_with_clock(worker_clock.as_ref())
                                .unwrap_or_else(|_| std::process::abort())
                            {
                                continue;
                            }
                            if worker_stop.load(crate::sync::Ordering::Acquire) {
                                break;
                            }
                            if let Some(deadline) = worker_runtime.next_deadline() {
                                let delay = std::time::Duration::from_nanos(
                                    deadline.0.saturating_sub(worker_clock.now().0),
                                );
                                worker_runtime
                                    .shared
                                    .changed
                                    .wait_after_timeout(version, delay);
                            } else {
                                worker_runtime.wait_after(version);
                            }
                        }
                    }));
                    // Protocol-core panic is isolated inside poll_one. Reaching this boundary
                    // means the shared driver or its fixed failure projection lost an invariant.
                    if result.is_err() {
                        std::process::abort();
                    }
                });
            match worker {
                Ok(worker) => threads.push(worker),
                Err(error) => {
                    stop.store(true, crate::sync::Ordering::Release);
                    runtime.notify_pollers();
                    for worker in threads {
                        worker.join().expect("started control poller exits");
                    }
                    return Err(error);
                }
            }
        }
        Ok(Self {
            runtime,
            stop,
            workers: threads,
            stopped: false,
        })
    }
    pub fn runtime(&self) -> &ActorRuntime<A> {
        &self.runtime
    }
    pub fn shutdown(mut self) -> Result<(), Self> {
        if !self.runtime.is_empty() {
            return Err(self);
        }
        self.stop.store(true, crate::sync::Ordering::Release);
        self.runtime.notify_pollers();
        for worker in self.workers.drain(..) {
            worker.join().expect("control poller actually exits");
        }
        self.stopped = true;
        Ok(())
    }
}
#[cfg(not(loom))]
impl<A: Actor> Drop for PollerPool<A> {
    fn drop(&mut self) {
        assert!(
            self.stopped,
            "control poller pool requires explicit drained shutdown and joins"
        );
    }
}
