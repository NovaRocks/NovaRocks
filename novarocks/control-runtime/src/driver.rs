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
    actor::{Actor, ActorConfig, ActorFailurePolicy, StepContext, StepEnvelope},
    notify::Notify,
    sync::{Arc, Mutex},
    transaction::{RuntimeError, ScopeHandle},
};
use novarocks_control_runtime_core::{
    effect::{EffectDescriptor, EffectError, EffectKind, EffectState},
    history::HistoryFact,
    identity::{
        ActionId, ActorId, BatchId, Generation, InputId, ReservationId, ScopeId, SlotId, Tick,
        WorkId,
    },
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
    EffectTicketOriginMismatch,
    ReplyAlreadyCommitted,
    UnknownRole,
    RoleBusy,
    InputAuthorMismatch,
    InputAuthorInactive,
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

/// Read-only identity of one original actor registration. Cloning this witness
/// grants no Work, publication or effect-ticket authority.
#[derive(Clone)]
pub struct ActorRegistration {
    actor: ActorId,
    attachment: SlotId,
    scope: ScopeHandle,
    origin: Arc<()>,
}
impl ActorRegistration {
    pub fn actor(&self) -> ActorId {
        self.actor
    }
    pub fn attachment(&self) -> SlotId {
        self.attachment
    }
    pub fn scope_id(&self) -> ScopeId {
        self.scope.id()
    }
    /// Equal public numbers do not identify the same registration. This is a
    /// read-only origin comparison and grants no scheduling or Work authority.
    pub fn same_registration(&self, other: &Self) -> bool {
        self.actor == other.actor
            && self.attachment == other.attachment
            && self.scope.same_scope(&other.scope)
            && Arc::ptr_eq(&self.origin, &other.origin)
    }
    /// Original scope identity only; this grants no Work or publication right.
    pub fn matches_scope(&self, scope: &ScopeHandle) -> bool {
        self.scope.same_scope(scope)
    }
}
impl std::fmt::Debug for ActorRegistration {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ActorRegistration")
            .field("actor", &self.actor)
            .field("attachment", &self.attachment)
            .field("scope", &self.scope.id())
            .finish_non_exhaustive()
    }
}

/// Qualified state of one actor's original builtin timer source. Closing the
/// source never consumes an already filled Tick or observes any Work exit.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TimerSourceState {
    Open,
    ClosedPending(InputId),
    ClosedDrained,
}

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
    external_open: bool,
    sealed: bool,
    payloads: BTreeMap<InputId, A::Input>,
    mailbox: MailboxState,
    inputs: BTreeMap<InputId, InputRecord>,
    roles: BTreeMap<InputRoleId, InputRoleRecord>,
    counts: [usize; 4],
    next_input: u64,
    batches: BTreeMap<InputId, StepEnvelope<A::Effect>>,
    batch_order: BTreeMap<BatchId, InputId>,
    next_batch: u64,
    generation: Generation,
    failed: bool,
    timer_input: Option<InputId>,
    // Monotonic source closure; an already filled Tick retains its input.
    timer_closed: bool,
    failure: Option<ManuallyDrop<Box<dyn Any + Send>>>,
    last_progress: Tick,
    failure_publication: Option<FailurePublicationRecord>,
    failure_action_watermark: u64,
}
impl<A: Actor> Inbox<A> {
    /// The single paid-role fill algorithm shared by direct and effect-backed
    /// handoff. The caller holds this inbox and qualifies the scope failure gate.
    fn fill_role(
        &mut self,
        role_id: InputRoleId,
        scope_failed: bool,
        value: A::Input,
    ) -> Result<InputId, (DriverError, A::Input)> {
        if self.sealed {
            return Err((DriverError::Closed, value));
        }
        if self.failed || scope_failed {
            return Err((DriverError::ActorFailed, value));
        }
        let Some(role) = self.roles.get(&role_id) else {
            return Err((DriverError::UnknownRole, value));
        };
        if role.in_flight.is_some() {
            return Err((DriverError::RoleBusy, value));
        }
        let (source, class) = (role.source, role.class);
        let Some(next) = self.next_input.checked_add(1) else {
            return Err((DriverError::IdentityExhausted, value));
        };
        let input = InputId::new(next).expect("incremented role input id");
        if let Err(error) = self.mailbox.push(class, source, input) {
            return Err((error.into(), value));
        }
        self.next_input = next;
        self.inputs.insert(
            input,
            InputRecord {
                source,
                class,
                phase: InputPhase::Filled,
                role: Some(role_id),
            },
        );
        self.payloads.insert(input, value);
        self.roles
            .get_mut(&role_id)
            .expect("idle role retains its paid position")
            .in_flight = Some(input);
        Ok(input)
    }
    fn next_batch_ordinal(&self) -> Result<BatchId, DriverError> {
        let next = self
            .next_batch
            .checked_add(1)
            .ok_or(DriverError::IdentityExhausted)?;
        Ok(BatchId::new(next).expect("incremented committed batch ordinal"))
    }
    fn commit_batch(
        &mut self,
        input: InputId,
        mut batch: StepEnvelope<A::Effect>,
    ) -> Result<(), DriverError> {
        let ordinal = self.next_batch_ordinal()?;
        let next = ordinal.get();
        assert!(batch.ordinal.is_none(), "one atomic batch commitment");
        batch.ordinal = Some(ordinal);
        assert!(
            self.batches.insert(input, batch).is_none(),
            "one batch per input occurrence"
        );
        assert!(self.batch_order.insert(ordinal, input).is_none());
        self.next_batch = next;
        Ok(())
    }
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
        assert!(
            self.batch_order.is_empty(),
            "actual committed-batch index retires with its batch"
        );
    }
}
struct ActorCell<A: Actor> {
    // Fresh for every registration, including reuse of the same public ActorId.
    origin: Arc<()>,
    id: ActorId,
    scope: ScopeHandle,
    attachment: SlotId,
    config: ActorConfig,
    failure_work: Option<WorkId>,
    failure_policy: ActorFailurePolicy,
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

/// Exclusive dispatch ownership for one original actor's host metadata turn.
/// This is not a WorkLease, input, publication or capacity permit. Dropping an
/// unfinished turn merely requeues its original address; it observes no exit.
#[must_use = "Release or retire the original host turn explicitly"]
pub struct ActorHostTurn<A: Actor> {
    runtime: ActorRuntime<A>,
    node: Arc<ActorCell<A>>,
    active: bool,
}
impl<A: Actor> std::fmt::Debug for ActorHostTurn<A> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ActorHostTurn")
            .field("actor", &self.node.id)
            .field("scope", &self.node.scope.id())
            .field("active", &self.active)
            .finish()
    }
}
impl<A: Actor> ActorHostTurn<A> {
    pub fn actor(&self) -> ActorId {
        self.node.id
    }
    pub fn registration(&self) -> ActorRegistration {
        ActorRegistration {
            actor: self.node.id,
            attachment: self.node.attachment,
            scope: self.node.scope.clone(),
            origin: self.node.origin.clone(),
        }
    }
    pub fn matches_registration(&self, registration: &ActorRegistration) -> bool {
        self.node.id == registration.actor
            && self.node.attachment == registration.attachment
            && self.node.scope.same_scope(&registration.scope)
            && Arc::ptr_eq(&self.node.origin, &registration.origin)
    }
    /// A mark racing this release is retained by the same ready queue, even
    /// when the caller has no currently runnable work of its own.
    pub fn release(mut self, has_runnable: bool) -> Result<(), (DriverError, Self)> {
        if let Err(error) = self.runtime.release_host_turn(&self.node, has_runnable) {
            return Err((error, self));
        }
        self.active = false;
        Ok(())
    }
    /// Retire only after actual Scope/input/effect custody has drained. The
    /// current claim and pending metadata marks are withdrawn atomically with
    /// this original actor's removal. A refusal returns the same active turn.
    pub fn retire_actor(mut self) -> Result<A, (DriverError, Self)> {
        match self.runtime.remove_actor_node(&self.node, true) {
            Ok(actor) => {
                self.active = false;
                Ok(actor)
            }
            Err(error) => Err((error, self)),
        }
    }
}
impl<A: Actor> Drop for ActorHostTurn<A> {
    fn drop(&mut self) {
        if self.active && self.runtime.release_host_turn(&self.node, true).is_err() {
            // A duplicated/lost scheduler claim is shared runtime corruption;
            // neither actor replacement nor implicit Work exit can repair it.
            std::process::abort();
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
        if inbox.sealed {
            return Err((DriverError::Closed, value));
        }
        let scope_failed = match node.scope.gates() {
            Ok(gates) => gates.failed,
            Err(error) => return Err((error.into(), value)),
        };
        let result = inbox.fill_role(self.role, scope_failed, value);
        if result.is_ok() {
            self.runtime
                .shared
                .state
                .lock()
                .expect("runtime scheduler lock")
                .ready
                .notify(self.actor)
                .expect("live role owns its actor");
        }
        drop(inbox);
        if result.is_ok() {
            self.runtime.shared.changed.notify();
        }
        result
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
        if inbox.sealed {
            return Err((DriverError::Closed, value));
        }
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

/// Read-only neutral metadata carried by one original dispatch ticket.
/// Constructing this view does not grant dispatch or publication authority.
#[derive(Debug)]
pub struct EffectTicketView<E> {
    pub effect: E,
    pub actor: ActorId,
    pub input: InputId,
    pub generation: Generation,
    pub descriptor: EffectDescriptor,
}
/// An exclusive effect dispatch ticket from one actual actor incarnation.
/// It grants no new work or capacity. `descriptor.work` names an already
/// admitted WorkLease; the host still starts that lease before running its body.
/// The ticket is neither physical-start nor physical-exit evidence.
///
/// Authority fields and the neutral effect are readable, but cannot be replaced:
/// ```compile_fail
/// use novarocks_control_runtime::driver::EffectTicket;
/// fn replace_effect(mut ticket: EffectTicket<u64>) {
///     ticket.effect = 99;
/// }
/// ```
/// ```compile_fail
/// use novarocks_control_runtime::driver::EffectTicket;
/// use novarocks_control_runtime_core::identity::ActorId;
/// fn redirect(mut ticket: EffectTicket<u64>, actor: ActorId) {
///     ticket.actor = actor;
/// }
/// ```
/// ```compile_fail
/// use novarocks_control_runtime::driver::EffectTicket;
/// use novarocks_control_runtime_core::identity::ActionId;
/// fn redirect_action(mut ticket: EffectTicket<u64>, action: ActionId) {
///     ticket.descriptor.id = action;
/// }
/// ```
/// ```compile_fail
/// use novarocks_control_runtime::driver::EffectTicket;
/// fn duplicate(ticket: &EffectTicket<u64>) -> EffectTicket<u64> {
///     ticket.clone()
/// }
/// ```
/// A metadata view cannot be converted into a new ticket:
/// ```compile_fail
/// use novarocks_control_runtime::driver::{EffectTicket, EffectTicketView};
/// fn forge(view: EffectTicketView<u64>) -> EffectTicket<u64> {
///     EffectTicket { view, origin: std::sync::Arc::new(()) }
/// }
/// ```
#[derive(Debug)]
pub struct EffectTicket<E> {
    view: EffectTicketView<E>,
    origin: Arc<()>,
}
impl<E> std::ops::Deref for EffectTicket<E> {
    type Target = EffectTicketView<E>;
    fn deref(&self) -> &Self::Target {
        &self.view
    }
}
/// A refused completion preserves the original exclusive dispatch authority.
/// Debug reports only runtime metadata, never the neutral effect payload.
pub struct EffectCompletionRefusal<E> {
    pub error: DriverError,
    pub ticket: EffectTicket<E>,
}
impl<E> std::fmt::Debug for EffectCompletionRefusal<E> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("EffectCompletionRefusal")
            .field("error", &self.error)
            .field("actor", &self.ticket.actor)
            .field("input", &self.ticket.input)
            .field("generation", &self.ticket.generation)
            .field("action", &self.ticket.descriptor.id)
            .finish()
    }
}
/// Exclusive publication authority within one actual failure supervision job.
/// It signs no work and never reads the failed core. Each commit touches one
/// actual target; application failure policy and bounded inventory loops remain
/// in the host. Drop neither exits the cleanup work nor rolls back any value.
#[must_use = "A failure session belongs to its original running cleanup work"]
pub struct FailurePublicationSession<A: Actor> {
    runtime: ActorRuntime<A>,
    actor: ActorId,
    // ActorIds may be registered again after actual removal, and WorkIds are
    // scope-local. Preserve the original scope and ledger identity even after
    // its work exits; a later incarnation cannot inherit this authority.
    scope: ScopeHandle,
    input: InputId,
    generation: Generation,
    action: ActionId,
    work: WorkId,
}
impl<A: Actor> std::fmt::Debug for FailurePublicationSession<A> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FailurePublicationSession")
            .field("actor", &self.actor)
            .field("scope", &self.scope.id())
            .field("generation", &self.generation)
            .field("work", &self.work)
            .finish()
    }
}
impl<A: Actor> FailurePublicationSession<A> {
    pub const fn generation(&self) -> Generation {
        self.generation
    }
    /// Read-only qualification of this original, still-running failure job.
    /// It signs no publication action and changes no watermark or custody.
    pub fn matches_actor_runtime(
        &self,
        runtime: &ActorRuntime<A>,
        actor: ActorId,
    ) -> Result<bool, DriverError> {
        if actor != self.actor || !Arc::ptr_eq(&runtime.shared, &self.runtime.shared) {
            return Ok(false);
        }
        let node = runtime.node(actor)?;
        let inbox = node.inbox.lock().expect("actor inbox lock");
        let ledger = node
            .scope
            .runtime
            .shared
            .state
            .lock()
            .expect("runtime ledger lock");
        self.validate(&node, &inbox, &ledger, &self.scope)?;
        Ok(true)
    }
    fn validate(
        &self,
        node: &ActorCell<A>,
        inbox: &Inbox<A>,
        ledger: &crate::transaction::LedgerState,
        target: &ScopeHandle,
    ) -> Result<(), DriverError> {
        if node.scope.id() != self.scope.id()
            || !Arc::ptr_eq(&node.scope.runtime.shared, &self.scope.runtime.shared)
            || node.scope.id() != target.id()
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
        Ok(())
    }
    fn next_publication_action(inbox: &Inbox<A>) -> Result<ActionId, DriverError> {
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
        let action = match self
            .validate(&node, &inbox, &ledger, &target.owner)
            .and_then(|()| Self::next_publication_action(&inbox))
        {
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
        let action = match self
            .validate(&node, &inbox, &ledger, &target.owner)
            .and_then(|()| Self::next_publication_action(&inbox))
        {
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
    pub external_open: bool,
    pub sealed: bool,
    pub inputs: usize,
    pub roles: usize,
    pub batches: usize,
    pub last_progress: Tick,
}

impl<A: Actor> ActorRuntime<A> {
    /// Compares the actual runtime instance only. This grants no work,
    /// capacity, input, or permission to pass a current admission gate.
    pub fn same_runtime(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.shared, &other.shared)
    }
    /// Read-only original scope identity, not admission, current generation,
    /// publication permission or evidence that any Work has exited.
    pub fn owns_scope(&self, actor: ActorId, scope: &ScopeHandle) -> Result<bool, DriverError> {
        Ok(self.node(actor)?.scope.same_scope(scope))
    }
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
        self.register_inner(
            id,
            scope,
            (Some(attachment), None),
            actor,
            config,
            ActorFailurePolicy::IsolateScope,
        )
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
        self.register_inner(
            id,
            scope,
            (None, None),
            actor,
            config,
            ActorFailurePolicy::IsolateScope,
        )
        .map(|registration| registration.attachment)
    }
    /// Bind the original failure-cleanup role outside the actor's mutable
    /// state. Its pure failure hook receives only the role identity, even if
    /// handle panics halfway through mutating every protocol field.
    /// This already admitted authority can attach during external drain;
    /// failed scopes, exited supervisors and a closed actor runtime refuse it.
    pub fn register_supervised_next(
        &self,
        id: ActorId,
        scope: ScopeHandle,
        actor: A,
        config: ActorConfig,
        failure_work: &crate::lease::WorkLease,
    ) -> Result<SlotId, (DriverError, A)> {
        self.register_supervised_next_with_policy(
            id,
            scope,
            actor,
            config,
            failure_work,
            ActorFailurePolicy::IsolateScope,
        )
    }
    /// The same original supervisor validation applies to both policies. The
    /// policy is retained in immutable runtime metadata, never read from a
    /// Core after panic. Ordinary input results do not invoke this policy.
    pub fn register_supervised_next_with_policy(
        &self,
        id: ActorId,
        scope: ScopeHandle,
        actor: A,
        config: ActorConfig,
        failure_work: &crate::lease::WorkLease,
        failure_policy: ActorFailurePolicy,
    ) -> Result<SlotId, (DriverError, A)> {
        self.register_supervised_bound_with_policy(
            id,
            scope,
            actor,
            config,
            failure_work,
            failure_policy,
        )
        .map(|registration| registration.attachment)
    }
    /// Return the original registration witness in the same transaction that
    /// installs the actor. No later lookup can bind this owner to a replacement.
    pub fn register_supervised_bound(
        &self,
        id: ActorId,
        scope: ScopeHandle,
        actor: A,
        config: ActorConfig,
        failure_work: &crate::lease::WorkLease,
    ) -> Result<ActorRegistration, (DriverError, A)> {
        self.register_supervised_bound_with_policy(
            id,
            scope,
            actor,
            config,
            failure_work,
            ActorFailurePolicy::IsolateScope,
        )
    }
    /// Return the original witness without changing the supervisor's immutable
    /// failure policy. Process owners retain ProcessFailStop through this path.
    pub fn register_supervised_bound_with_policy(
        &self,
        id: ActorId,
        scope: ScopeHandle,
        actor: A,
        config: ActorConfig,
        failure_work: &crate::lease::WorkLease,
        failure_policy: ActorFailurePolicy,
    ) -> Result<ActorRegistration, (DriverError, A)> {
        self.register_supervised_inner(id, scope, actor, config, failure_work, failure_policy)
    }
    fn register_supervised_inner(
        &self,
        id: ActorId,
        scope: ScopeHandle,
        actor: A,
        config: ActorConfig,
        failure_work: &crate::lease::WorkLease,
        failure_policy: ActorFailurePolicy,
    ) -> Result<ActorRegistration, (DriverError, A)> {
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
                        && !work.body_exited
                        && !work.captures_exited
                        && matches!(
                            work.phase,
                            WorkPhase::Admitted | WorkPhase::Queued | WorkPhase::Started
                        )
                })
        };
        if !valid {
            return Err((DriverError::FailureWorkInactive, actor));
        }
        self.register_inner(
            id,
            scope,
            (None, Some(failure_work.id())),
            actor,
            config,
            failure_policy,
        )
    }
    fn register_inner(
        &self,
        id: ActorId,
        scope: ScopeHandle,
        binding: (Option<SlotId>, Option<WorkId>),
        actor: A,
        config: ActorConfig,
        failure_policy: ActorFailurePolicy,
    ) -> Result<ActorRegistration, (DriverError, A)> {
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
            None => match failure_work.map_or_else(
                || scope.attach_actor_next(),
                |work| scope.attach_admitted_actor_next(work),
            ) {
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
        let registration = ActorRegistration {
            actor: id,
            attachment,
            scope: scope.clone(),
            origin: Arc::new(()),
        };
        state.actors.insert(
            id,
            Arc::new(ActorCell {
                origin: registration.origin.clone(),
                id,
                scope,
                attachment,
                config,
                failure_work,
                failure_policy,
                core: Mutex::new(Some(actor)),
                inbox: Mutex::new(Inbox {
                    external_open: true,
                    sealed: false,
                    payloads: BTreeMap::new(),
                    mailbox: MailboxState::new(config.mailbox),
                    inputs: BTreeMap::new(),
                    roles: BTreeMap::new(),
                    counts: [0; 4],
                    next_input: 0,
                    batches: BTreeMap::new(),
                    batch_order: BTreeMap::new(),
                    next_batch: 0,
                    generation,
                    failed: false,
                    timer_input: None,
                    timer_closed: false,
                    failure: None,
                    last_progress: Tick(0),
                    failure_publication: None,
                    failure_action_watermark: 0,
                }),
            }),
        );
        Ok(registration)
    }
    /// Existing permits may still fill after external admission closes.
    pub fn reserve_input(
        &self,
        actor: ActorId,
        source: ActorId,
        class: InputClass,
    ) -> Result<InputPermit<A>, DriverError> {
        self.reserve_input_position(actor, source, class, None)
    }
    /// Reserve a neutral input for an already admitted holder in this actor's
    /// exact scope. External closure cannot discard that holder's drain work.
    /// The caller has funded its input/batch/capture inventory before binding
    /// the holder. This does not admit Work, acquire capacity or start a body.
    /// Failed/sealed actors and exited or unresolved authors still refuse it.
    pub fn reserve_admitted_input(
        &self,
        actor: ActorId,
        source: ActorId,
        class: InputClass,
        author: &crate::lease::WorkLease,
    ) -> Result<InputPermit<A>, DriverError> {
        self.reserve_input_position(actor, source, class, Some(author))
    }
    fn reserve_input_position(
        &self,
        actor: ActorId,
        source: ActorId,
        class: InputClass,
        author: Option<&crate::lease::WorkLease>,
    ) -> Result<InputPermit<A>, DriverError> {
        if class == InputClass::Timer {
            return Err(DriverError::InvalidConfig);
        }
        let node = self.node(actor)?;
        let mut inbox = node.inbox.lock().expect("actor inbox lock");
        // Keep the actual author check and position insertion in one order
        // with scope failure. Lock order is the existing inbox -> ledger order;
        // no callback, payload construction or Work admission runs here.
        let author_state = if let Some(author) = author {
            if author.scope().id() != node.scope.id()
                || !Arc::ptr_eq(&author.scope().runtime.shared, &node.scope.runtime.shared)
            {
                return Err(DriverError::InputAuthorMismatch);
            }
            let state = node
                .scope
                .runtime
                .shared
                .state
                .lock()
                .expect("runtime ledger lock");
            let scope = state
                .scopes
                .get(&node.scope.id())
                .ok_or(DriverError::InputAuthorInactive)?;
            if scope.failed() {
                return Err(DriverError::ActorFailed);
            }
            let work = scope
                .work(author.id())
                .ok_or(DriverError::InputAuthorInactive)?;
            if work.unbound_guard
                || work.body_exited
                || work.captures_exited
                || state.unresolved.contains(&(node.scope.id(), author.id()))
            {
                return Err(DriverError::InputAuthorInactive);
            }
            Some(state)
        } else {
            if !self
                .shared
                .state
                .lock()
                .expect("runtime scheduler lock")
                .open
            {
                return Err(DriverError::Closed);
            }
            None
        };
        if inbox.failed {
            return Err(DriverError::ActorFailed);
        }
        if inbox.sealed || author.is_none() && !inbox.external_open {
            return Err(DriverError::Closed);
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
        drop(author_state);
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
            external_open: inbox.external_open,
            sealed: inbox.sealed,
            inputs: inbox.inputs.len(),
            roles: inbox.roles.len(),
            batches: inbox.batches.len(),
            last_progress: inbox.last_progress,
        })
    }
    /// Stop issuing new positions for one actor. Existing permits and prepaid
    /// roles still drain, including after process-wide external admission closes.
    /// This is not a stop request, failure, work exit or retirement proof.
    pub fn close_actor_admission(&self, actor: ActorId) -> Result<(), DriverError> {
        let node = self.node(actor)?;
        node.inbox.lock().expect("actor inbox lock").external_open = false;
        self.shared.changed.notify();
        Ok(())
    }
    /// Monotonically withdraw future builtin timer production for this actor.
    /// Existing filled Tick custody remains until actual `retire_input`.
    /// Persistent prepaid input roles and ordinary input drain are unaffected.
    pub fn close_timer_source(&self, actor: ActorId) -> Result<TimerSourceState, DriverError> {
        let node = self.node(actor)?;
        let mut inbox = node.inbox.lock().expect("actor inbox lock");
        inbox.timer_closed = true;
        let mut state = self.shared.state.lock().expect("runtime scheduler lock");
        if state
            .actors
            .get(&actor)
            .is_some_and(|current| Arc::ptr_eq(current, &node))
        {
            state.timers.cancel(actor);
            if inbox.timer_input.is_none() {
                // A selected but not yet filled position can be withdrawn:
                // the same inbox gate prevents its later delivery. A real
                // filled Tick is never acknowledged by source closure.
                state.timers.acknowledge_tick(actor);
            }
        }
        let observed = Self::timer_state(&inbox);
        drop(state);
        drop(inbox);
        self.shared.changed.notify();
        Ok(observed)
    }
    /// A current source-qualified read, not an actor/Work retirement proof.
    /// ClosedDrained cannot be followed by another builtin fill or rearm on
    /// this same actor incarnation. Other input sources still own their legs.
    pub fn timer_source_state(&self, actor: ActorId) -> Result<TimerSourceState, DriverError> {
        let node = self.node(actor)?;
        let inbox = node.inbox.lock().expect("actor inbox lock");
        Ok(Self::timer_state(&inbox))
    }
    /// Observe the unique builtin Tick's original input address. This grants
    /// no retirement authority; its real batch still gates retire_input.
    pub fn pending_timer_input(&self, actor: ActorId) -> Result<Option<InputId>, DriverError> {
        let node = self.node(actor)?;
        let inbox = node.inbox.lock().expect("actor inbox lock");
        Ok(inbox.timer_input)
    }
    fn timer_state(inbox: &Inbox<A>) -> TimerSourceState {
        if !inbox.timer_closed {
            TimerSourceState::Open
        } else if let Some(input) = inbox.timer_input {
            TimerSourceState::ClosedPending(input)
        } else {
            TimerSourceState::ClosedDrained
        }
    }
    /// After all actual input/role/effect holders have retired, seal timer and
    /// mailbox ingress before the host exits its last housekeeping leases.
    /// Work remains counted and remove_actor still independently requires join.
    pub fn seal_actor(&self, actor: ActorId) -> Result<(), DriverError> {
        let node = self.node(actor)?;
        let mut inbox = node.inbox.lock().expect("actor inbox lock");
        if inbox.external_open
            || !inbox.inputs.is_empty()
            || !inbox.roles.is_empty()
            || !inbox.batches.is_empty()
            || inbox.failure.is_some()
        {
            return Err(DriverError::ActorNotRetired);
        }
        let mut state = self.shared.state.lock().expect("runtime scheduler lock");
        if !state.ready.is_idle(actor)? || !state.effect_ready.is_idle(actor)? {
            return Err(SchedulerError::Busy.into());
        }
        inbox.sealed = true;
        inbox.timer_closed = true;
        state.timers.cancel(actor);
        state.timers.acknowledge_tick(actor);
        drop(state);
        drop(inbox);
        self.shared.changed.notify();
        Ok(())
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
            // Retain the exact selected node before releasing the scheduler
            // lock. Concurrent retirement may remove its directory entry.
            let actors = state
                .timers
                .advance(now, budget)?
                .into_iter()
                .map(|actor| {
                    state
                        .actors
                        .get(&actor)
                        .cloned()
                        .expect("timer retains a registered actor")
                })
                .collect::<Vec<_>>();
            (actors, now)
        };
        let mut filled = 0;
        for node in actors {
            let actor = node.id;
            let timer_value = A::timer_input(now);
            let mut inbox = node.inbox.lock().expect("actor inbox lock");
            if inbox.failed || inbox.sealed || inbox.timer_closed {
                let mut state = self.shared.state.lock().expect("runtime scheduler lock");
                if state
                    .actors
                    .get(&actor)
                    .is_some_and(|current| Arc::ptr_eq(current, &node))
                {
                    state.timers.acknowledge_tick(actor);
                }
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
                if inbox.failed || inbox.sealed || inbox.batches.len() >= node.config.max_batches {
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
                    let stop_producing = batch.stop_producing;
                    let mut inbox = node.inbox.lock().expect("actor inbox lock");
                    // Preflight every fallible batch boundary before changing
                    // the scope gate. The same inbox stays locked through commit.
                    let ordinal = inbox.next_batch_ordinal()?;
                    assert!(batch.ordinal.is_none(), "one atomic batch commitment");
                    assert!(
                        !inbox.batches.contains_key(&input.input),
                        "one batch per input occurrence"
                    );
                    assert!(
                        !inbox.batch_order.contains_key(&ordinal),
                        "one committed batch ordinal"
                    );
                    inbox.inputs.get(&input.input).expect("consumed input");
                    // Only an explicit directive needs this ledger transaction.
                    // No host lock, callback or rich object enters the gate.
                    let mut ledger = stop_producing.then(|| {
                        node.scope
                            .runtime
                            .shared
                            .state
                            .lock()
                            .expect("runtime ledger lock")
                    });
                    if let Some(ledger) = &ledger {
                        ledger
                            .scopes
                            .get(&node.scope.id())
                            .expect("registered actor retains its original scope");
                    }
                    inbox
                        .commit_batch(input.input, batch)
                        .expect("preflighted atomic batch commitment");
                    inbox
                        .inputs
                        .get_mut(&input.input)
                        .expect("consumed input")
                        .phase = InputPhase::Committed;
                    if let Some(ledger) = &mut ledger {
                        ledger
                            .scopes
                            .get_mut(&node.scope.id())
                            .expect("registered actor retains its original scope")
                            .request_stop();
                        node.scope.runtime.shared.record(HistoryFact::ScopeStopped {
                            scope: node.scope.id(),
                        });
                    }
                    inbox.last_progress = now;
                    self.shared.record(HistoryFact::InputConsumed {
                        actor: node.id,
                        input: input.input,
                    });
                    self.shared.record(HistoryFact::BatchCommitted {
                        scope: node.scope.id(),
                        input: input.input,
                    });
                    drop(ledger);
                    drop(inbox);
                    if stop_producing {
                        node.scope.runtime.shared.changed.notify();
                    }
                    deadline
                }
                Err(payload) => {
                    if node.failure_policy == ActorFailurePolicy::ProcessFailStop {
                        // Preserve the original panic, all uncommitted step
                        // reservations/effects and the original Core in its
                        // cell. Never inspect that Core, unwind its captures,
                        // or publish a normal scope-failure receipt.
                        std::mem::forget(payload);
                        std::mem::forget(step);
                        std::process::abort();
                    }
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
                    inbox.commit_batch(input.input, batch)?;
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
            // A Core turn may have started before source closure. Observe
            // its returned deadline under the same inbox -> scheduler gate,
            // so neither this turn nor a later one can reopen the source.
            let inbox = node.inbox.lock().expect("actor inbox lock");
            let mut state = self.shared.state.lock().expect("runtime scheduler lock");
            state.effect_ready.notify(node.id)?;
            if !inbox.timer_closed && !inbox.failed && !inbox.sealed {
                if let Some(deadline) = deadline {
                    state.timers.set(node.id, deadline)?;
                } else {
                    state.timers.cancel(node.id);
                }
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
    /// Copy plain routing metadata before an effect becomes eligible to start.
    /// This grants no execution/publication authority and leaves the original
    /// payload in the committed batch. An application's host dispatcher uses
    /// it to index per-owner FIFO lanes in actual batch commitment order.
    /// As required by Actor, this value and its Clone contain no foreign code,
    /// runnable, rich source or opaque destructor.
    pub fn pending_effect_metadata(
        &self,
        actor: ActorId,
        input: InputId,
        action: ActionId,
    ) -> Result<Option<A::Effect>, DriverError>
    where
        A::Effect: Clone,
    {
        let node = self.node(actor)?;
        let inbox = node.inbox.lock().expect("actor inbox lock");
        let batch = inbox.batches.get(&input).ok_or(DriverError::UnknownInput)?;
        if batch.state.record(action).is_none() {
            return Err(EffectError::UnknownAction.into());
        }
        Ok(batch.payloads.get(&action).cloned())
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
            view: EffectTicketView {
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
            },
            origin: node.origin.clone(),
        })
    }
    /// Read-only validation of this original ticket's current publication
    /// authority. It neither runs the effect nor creates work or capacity.
    /// Late external facts and completion use their separate origin-only gate.
    pub fn validate_effect_ticket(
        &self,
        ticket: &EffectTicket<A::Effect>,
    ) -> Result<(), DriverError> {
        let node = self.node(ticket.actor)?;
        let inbox = node.inbox.lock().expect("actor inbox lock");
        let ledger = node
            .scope
            .runtime
            .shared
            .state
            .lock()
            .expect("runtime ledger lock");
        self.validate_publication(&node, &inbox, &ledger, ticket, &node.scope)
    }
    /// Atomically qualifies the original effect's current publication authority
    /// and fills an already paid role in this same runtime and ledger. No Work,
    /// capacity or target admission is created; the ticket remains with its caller.
    /// Source failure and target fill share the original ledger transaction.
    pub fn fill_from_effect(
        &self,
        ticket: &EffectTicket<A::Effect>,
        target: &InputRole<A>,
        value: A::Input,
    ) -> Result<InputId, (DriverError, A::Input)> {
        if !self.same_runtime(&target.runtime) {
            return Err((DriverError::PublicationTargetMismatch, value));
        }
        if !target.active {
            return Err((DriverError::UnknownRole, value));
        }
        let source_node = match self.node(ticket.actor) {
            Ok(node) => node,
            Err(error) => return Err((error, value)),
        };
        let target_node = if ticket.actor == target.actor {
            source_node.clone()
        } else {
            match self.node(target.actor) {
                Ok(node) => node,
                Err(error) => return Err((error, value)),
            }
        };
        if !Arc::ptr_eq(
            &source_node.scope.runtime.shared,
            &target_node.scope.runtime.shared,
        ) {
            return Err((DriverError::PublicationTargetMismatch, value));
        }
        // Every two-inbox operation uses ActorId order. The same-actor case
        // borrows its one inbox twice immutably only for validation.
        let result = if ticket.actor == target.actor {
            let mut inbox = source_node.inbox.lock().expect("actor inbox lock");
            let ledger = source_node
                .scope
                .runtime
                .shared
                .state
                .lock()
                .expect("runtime ledger lock");
            match self.validate_effect_forwarding(
                ticket,
                (&source_node, &inbox),
                (&target_node, &inbox, target.role),
                &ledger,
            ) {
                Ok(failed) => inbox.fill_role(target.role, failed, value),
                Err(error) => Err((error, value)),
            }
        } else if ticket.actor < target.actor {
            let source = source_node.inbox.lock().expect("source actor inbox lock");
            let mut destination = target_node.inbox.lock().expect("target actor inbox lock");
            self.fill_effect_target_locked(
                ticket,
                (&source_node, &source),
                (&target_node, &mut destination, target.role),
                value,
            )
        } else {
            let mut destination = target_node.inbox.lock().expect("target actor inbox lock");
            let source = source_node.inbox.lock().expect("source actor inbox lock");
            self.fill_effect_target_locked(
                ticket,
                (&source_node, &source),
                (&target_node, &mut destination, target.role),
                value,
            )
        };
        if result.is_ok() {
            self.shared
                .state
                .lock()
                .expect("runtime scheduler lock")
                .ready
                .notify(target.actor)
                .expect("live role owns its actor");
            self.shared.changed.notify();
        }
        result
    }
    fn fill_effect_target_locked(
        &self,
        ticket: &EffectTicket<A::Effect>,
        source: (&ActorCell<A>, &Inbox<A>),
        target: (&ActorCell<A>, &mut Inbox<A>, InputRoleId),
        value: A::Input,
    ) -> Result<InputId, (DriverError, A::Input)> {
        let ledger = source
            .0
            .scope
            .runtime
            .shared
            .state
            .lock()
            .expect("runtime ledger lock");
        match self.validate_effect_forwarding(
            ticket,
            source,
            (target.0, target.1, target.2),
            &ledger,
        ) {
            Ok(failed) => target.1.fill_role(target.2, failed, value),
            Err(error) => Err((error, value)),
        }
    }
    fn validate_effect_forwarding(
        &self,
        ticket: &EffectTicket<A::Effect>,
        source: (&ActorCell<A>, &Inbox<A>),
        target: (&ActorCell<A>, &Inbox<A>, InputRoleId),
        ledger: &crate::transaction::LedgerState,
    ) -> Result<bool, DriverError> {
        self.validate_publication(source.0, source.1, ledger, ticket, &source.0.scope)?;
        let role = target
            .1
            .roles
            .get(&target.2)
            .ok_or(DriverError::UnknownRole)?;
        if role.source != ticket.actor {
            return Err(DriverError::InputAuthorMismatch);
        }
        Ok(ledger
            .scopes
            .get(&target.0.scope.id())
            .ok_or(RuntimeError::UnknownScope)?
            .failed())
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
        Self::validate_ticket_origin(&node, ticket)?;
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
        Self::validate_failure_record(
            node,
            inbox,
            ledger,
            (input, generation, action, work),
            false,
        )
    }
    fn validate_failure_record(
        node: &ActorCell<A>,
        inbox: &Inbox<A>,
        ledger: &crate::transaction::LedgerState,
        (input, generation, action, work): (InputId, Generation, ActionId, WorkId),
        allow_admitted: bool,
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
        if work.kind != WorkKind::Cleanup
            || !(work.phase == WorkPhase::Started
                || allow_admitted && matches!(work.phase, WorkPhase::Admitted | WorkPhase::Queued))
            || work.body_exited
        {
            return Err(DriverError::FailureWorkInactive);
        }
        Ok(())
    }
    /// Validate an original failure action before its owner starts the prepaid
    /// cleanup Work. This read-only check grants no publication or exit and
    /// never accepts an ordinary publication ticket from a failed generation.
    pub fn validate_failure_effect_ticket(
        &self,
        ticket: &EffectTicket<A::Effect>,
    ) -> Result<(), DriverError> {
        let node = self.node(ticket.actor)?;
        Self::validate_ticket_origin(&node, ticket)?;
        let inbox = node.inbox.lock().expect("actor inbox lock");
        let ledger = node
            .scope
            .runtime
            .shared
            .state
            .lock()
            .expect("runtime ledger lock");
        Self::validate_failure_record(
            &node,
            &inbox,
            &ledger,
            (
                ticket.input,
                ticket.generation,
                ticket.descriptor.id,
                ticket.descriptor.work,
            ),
            true,
        )?;
        let record = inbox
            .batches
            .get(&ticket.input)
            .expect("validated failure batch")
            .state
            .record(ticket.descriptor.id)
            .expect("validated failure action");
        if record.descriptor() != &ticket.descriptor {
            return Err(EffectError::InvalidTransition.into());
        }
        if inbox.failure_publication.is_some() {
            return Err(DriverError::FailurePublicationAlreadyClaimed);
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
        Self::validate_ticket_origin(&node, ticket)?;
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
            scope: node.scope.clone(),
            input: ticket.input,
            generation: ticket.generation,
            action: ticket.descriptor.id,
            work: ticket.descriptor.work,
        })
    }
    fn validate_ticket_origin(
        node: &ActorCell<A>,
        ticket: &EffectTicket<A::Effect>,
    ) -> Result<(), DriverError> {
        if !Arc::ptr_eq(&node.origin, &ticket.origin) {
            return Err(DriverError::EffectTicketOriginMismatch);
        }
        Ok(())
    }
    fn validate_publication(
        &self,
        node: &ActorCell<A>,
        inbox: &Inbox<A>,
        ledger: &crate::transaction::LedgerState,
        ticket: &EffectTicket<A::Effect>,
        target: &ScopeHandle,
    ) -> Result<(), DriverError> {
        Self::validate_ticket_origin(node, ticket)?;
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
        Self::validate_ticket_origin(&node, ticket)?;
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
    /// Complete a physical effect only for its original registered host owner.
    /// Failure generations do not invalidate an observed physical completion.
    /// Refusal preserves the exclusive original ticket for its actual owner.
    pub fn effect_completed_for(
        &self,
        registration: &ActorRegistration,
        ticket: EffectTicket<A::Effect>,
    ) -> Result<(), EffectCompletionRefusal<A::Effect>> {
        if registration.actor != ticket.actor || !Arc::ptr_eq(&registration.origin, &ticket.origin)
        {
            return Err(EffectCompletionRefusal {
                error: DriverError::EffectTicketOriginMismatch,
                ticket,
            });
        }
        self.effect_completed(ticket)
    }
    pub fn effect_completed(
        &self,
        ticket: EffectTicket<A::Effect>,
    ) -> Result<(), EffectCompletionRefusal<A::Effect>> {
        let complete = || -> Result<(), DriverError> {
            let node = self.node(ticket.actor)?;
            Self::validate_ticket_origin(&node, &ticket)?;
            let mut inbox = node.inbox.lock().expect("actor inbox lock");
            inbox
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
            drop(inbox);
            self.shared.changed.notify();
            Ok(())
        };
        match complete() {
            Ok(()) => Ok(()),
            Err(error) => Err(EffectCompletionRefusal { error, ticket }),
        }
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
        if let Some(batch) = inbox.batches.remove(&input) {
            let ordinal = batch
                .ordinal
                .expect("a resident batch was atomically committed");
            assert_eq!(inbox.batch_order.remove(&ordinal), Some(input));
        }
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
    fn validate_registration_in_state(
        state: &DriverState<A>,
        registration: &ActorRegistration,
    ) -> Result<(), DriverError> {
        let node = state
            .actors
            .get(&registration.actor)
            .ok_or(DriverError::UnknownActor)?;
        if node.attachment != registration.attachment
            || !node.scope.same_scope(&registration.scope)
            || !Arc::ptr_eq(&node.origin, &registration.origin)
        {
            return Err(DriverError::EffectTicketOriginMismatch);
        }
        Ok(())
    }
    /// Qualify the actual current registration without scheduling or granting
    /// admission. Equal public IDs in another runtime cannot pass this check.
    pub fn validate_registration(
        &self,
        registration: &ActorRegistration,
    ) -> Result<(), DriverError> {
        let state = self.shared.state.lock().expect("runtime scheduler lock");
        Self::validate_registration_in_state(&state, registration)
    }
    /// A host completion/retry mark, including during failure or process drain.
    /// Qualify origin and enqueue under the same scheduler transaction, so an
    /// old witness can never wake an actor registered later at the same ID.
    pub fn notify_host_actor(&self, registration: &ActorRegistration) -> Result<(), DriverError> {
        let mut state = self.shared.state.lock().expect("runtime scheduler lock");
        Self::validate_registration_in_state(&state, registration)?;
        state.effect_ready.notify(registration.actor)?;
        drop(state);
        self.shared.changed.notify();
        Ok(())
    }
    /// One poller owns this original actor's host turn. Pure Core dispatch has
    /// its independent queue and can proceed while this metadata turn exists.
    pub fn claim_host_actor(&self) -> Option<ActorHostTurn<A>> {
        let mut state = self.shared.state.lock().expect("runtime scheduler lock");
        let actor = state.effect_ready.claim()?;
        let node = state
            .actors
            .get(&actor)
            .expect("ready actor retains its original owner")
            .clone();
        Some(ActorHostTurn {
            runtime: self.clone(),
            node,
            active: true,
        })
    }
    fn release_host_turn(
        &self,
        node: &Arc<ActorCell<A>>,
        has_runnable: bool,
    ) -> Result<(), DriverError> {
        let mut state = self.shared.state.lock().expect("runtime scheduler lock");
        if !state
            .actors
            .get(&node.id)
            .is_some_and(|actual| Arc::ptr_eq(actual, node))
        {
            return Err(DriverError::EffectTicketOriginMismatch);
        }
        state.effect_ready.release(node.id, has_runnable)?;
        drop(state);
        self.shared.changed.notify();
        Ok(())
    }
    /// Bounded projection of actual runtime batches, including timer-generated batches.
    /// Input IDs describe issuance, not consumption order. Host publication
    /// dispatch uses committed_batches, preserving the actual core decisions.
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
    /// Bounded actual commitment order within this actor. Fair scheduling may
    /// consume a numerically later input first; a host cannot sort InputIds to
    /// recover publication FIFO. The cursor survives earlier batch retirement.
    pub fn committed_batches(
        &self,
        actor: ActorId,
        after: Option<BatchId>,
        budget: usize,
    ) -> Result<Vec<(BatchId, InputId)>, DriverError> {
        let node = self.node(actor)?;
        let inbox = node.inbox.lock().expect("actor inbox lock");
        let lower = after.map_or(std::ops::Bound::Unbounded, std::ops::Bound::Excluded);
        Ok(inbox
            .batch_order
            .range((lower, std::ops::Bound::Unbounded))
            .take(budget)
            .map(|(ordinal, input)| (*ordinal, *input))
            .collect())
    }
    /// Actual input custody is available to bounded scope-failure teardown, never from core private tables.
    pub fn input_custody(
        &self,
        actor: ActorId,
        input: InputId,
    ) -> Result<Option<InputCustody>, DriverError> {
        let node = self.node(actor)?;
        let inbox = node.inbox.lock().expect("actor inbox lock");
        Ok(inbox.inputs.get(&input).map(|record| InputCustody {
            input,
            source: record.source,
            class: record.class,
            phase: record.phase,
            role: record.role,
        }))
    }
    /// Enumerates the same actual input records; a point read grants no start
    /// or publication authority and changes no consumption or exit fact.
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
        self.remove_actor_node(&node, false)
    }
    fn remove_actor_node(&self, node: &Arc<ActorCell<A>>, claimed: bool) -> Result<A, DriverError> {
        let actor = node.id;
        let mut inbox = node.inbox.lock().expect("actor inbox lock");
        if !inbox.inputs.is_empty()
            || !inbox.roles.is_empty()
            || !inbox.batches.is_empty()
            || inbox.failure.is_some()
            || !node.scope.join_ready()?
        {
            return Err(DriverError::ActorNotRetired);
        }
        let mut state = self.shared.state.lock().expect("runtime scheduler lock");
        if !state
            .actors
            .get(&actor)
            .is_some_and(|actual| Arc::ptr_eq(actual, node))
        {
            return Err(DriverError::EffectTicketOriginMismatch);
        }
        if !state.ready.is_idle(actor)? {
            return Err(SchedulerError::Busy.into());
        }
        if claimed {
            if !state.effect_ready.is_claimed(actor)? {
                return Err(SchedulerError::NotClaimed.into());
            }
        } else if !state.effect_ready.is_idle(actor)? {
            return Err(SchedulerError::Busy.into());
        }
        // A producer may already have looked up this exact Arc and be waiting
        // for the inbox lock. It must observe the seal after removal as well.
        inbox.external_open = false;
        inbox.sealed = true;
        inbox.timer_closed = true;
        state.ready.unregister(actor)?;
        if claimed {
            state.effect_ready.unregister_claimed(actor)?;
        } else {
            state.effect_ready.unregister(actor)?;
        }
        state.timers.cancel(actor);
        state.timers.acknowledge_tick(actor);
        state.actors.remove(&actor);
        drop(state);
        node.scope.detach_actor(node.attachment)?;
        let actor = node
            .core
            .lock()
            .expect("actor core lock")
            .take()
            .expect("one core owner");
        drop(inbox);
        self.shared.changed.notify();
        Ok(actor)
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
    /// Wait for a possible host/scheduler change or the caller's real deadline.
    /// Preserve the version sampled before checking the caller's predicate;
    /// a wake supplies no completion or deadline fact by itself.
    #[cfg(not(loom))]
    pub fn wait_after_timeout(&self, observed: u64, timeout: std::time::Duration) {
        self.shared.changed.wait_after_timeout(observed, timeout);
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
        Self::start_with_dispatch(runtime, clock, workers, |_, _| false)
    }
    /// Runs finite host metadata turns on the same pollers as protocol turns.
    /// The callback must hand foreign calls and destructors to their admitted
    /// executors, and release or retire the original exclusive host turn.
    pub fn start_with_host<H>(
        runtime: ActorRuntime<A>,
        clock: Arc<dyn Clock>,
        workers: usize,
        host: H,
    ) -> Result<Self, std::io::Error>
    where
        H: Fn(Tick, ActorHostTurn<A>) + Send + Sync + 'static,
    {
        Self::start_with_dispatch(runtime, clock, workers, move |runtime, now| {
            let Some(turn) = runtime.claim_host_actor() else {
                return false;
            };
            host(now, turn);
            true
        })
    }
    fn start_with_dispatch<H>(
        runtime: ActorRuntime<A>,
        clock: Arc<dyn Clock>,
        workers: usize,
        host: H,
    ) -> Result<Self, std::io::Error>
    where
        H: Fn(&ActorRuntime<A>, Tick) -> bool + Send + Sync + 'static,
    {
        if workers == 0 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "control poller count must be positive",
            ));
        }
        let stop = Arc::new(crate::sync::AtomicBool::new(false));
        let host = Arc::new(host);
        let mut threads = Vec::new();
        for index in 0..workers {
            let worker_runtime = runtime.clone();
            let worker_stop = stop.clone();
            let worker_clock = clock.clone();
            let worker_host = host.clone();
            let worker = std::thread::Builder::new()
                .name(format!("control-poller-{index}"))
                .spawn(move || {
                    let result = catch_unwind(AssertUnwindSafe(|| {
                        while !worker_stop.load(crate::sync::Ordering::Acquire) {
                            let version = worker_runtime.change_version();
                            let now = worker_clock.now();
                            worker_runtime
                                .advance_time(now, 64)
                                .unwrap_or_else(|error| {
                                    eprintln!("Control poller timer advance failed: {error:?}");
                                    std::process::abort();
                                });
                            let core_progressed = worker_runtime
                                .poll_one_with_clock(worker_clock.as_ref())
                                .unwrap_or_else(|error| {
                                    eprintln!("Control poller Core turn failed: {error:?}");
                                    std::process::abort();
                                });
                            // A continuously ready protocol actor must not starve
                            // host handoffs or retirement. Each queue gets one
                            // finite opportunity before another turn begins.
                            let host_progressed = worker_host(&worker_runtime, worker_clock.now());
                            if core_progressed || host_progressed {
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
                        eprintln!("Control poller shared driver panicked");
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

#[cfg(all(test, not(loom)))]
#[path = "driver/registration_tests.rs"]
mod registration_tests;

#[cfg(all(test, not(loom)))]
mod effect_ticket_incarnation_tests {
    use super::*;
    use crate::transaction::RuntimeLedger;
    use novarocks_control_runtime_core::{
        capacity::ResourceVector, identity::ScopeId, scope::WorkAdmission,
    };

    #[derive(Debug)]
    struct OneEffect(WorkId);
    impl Actor for OneEffect {
        type Input = ();
        type Effect = ();
        fn timer_input(_now: Tick) {}
        fn handle(
            &mut self,
            _now: Tick,
            _input: InputEnvelope,
            _value: (),
            step: &mut StepContext<'_, ()>,
        ) {
            step.effect(
                EffectDescriptor {
                    id: ActionId::new(1).unwrap(),
                    work: self.0,
                    kind: EffectKind::Publication,
                    dependencies: vec![],
                },
                (),
            )
            .unwrap();
        }
        fn next_deadline(&self) -> Option<Tick> {
            None
        }
        fn owner_failed(
            _scope: ScopeId,
            _generation: Generation,
            _input: InputEnvelope,
            _step: &mut StepContext<'_, ()>,
        ) {
        }
    }
    #[test]
    fn actual_actor_re_registration_mints_a_fresh_ticket_incarnation() {
        let ledger = RuntimeLedger::new(ResourceVector::new([]).unwrap(), 8, 8);
        let runtime = ActorRuntime::new(1);
        let actor = ActorId::new(1).unwrap();
        let generation = Generation::new(1).unwrap();
        let mut previous: Option<Arc<()>> = None;
        let mut fresh = true;
        for _ in 0..2 {
            let scope = ledger.open_scope_next(None, generation, 4, 4).unwrap();
            let (mut work, mut capacity) = scope
                .reserve_next(
                    generation,
                    ResourceVector::new([]).unwrap(),
                    WorkKind::Business,
                    WorkAdmission::External,
                )
                .unwrap()
                .bind(WorkKind::Business)
                .unwrap();
            work.start().unwrap();
            runtime
                .register_next(
                    actor,
                    scope.clone(),
                    OneEffect(work.id()),
                    ActorConfig {
                        mailbox: [1; 4],
                        max_batches: 1,
                        max_effects_per_step: 1,
                        max_dependencies_per_effect: 1,
                        turn_budget: 8,
                        max_step_work: 1,
                    },
                )
                .unwrap();
            let input = runtime
                .reserve_input(actor, actor, InputClass::Ordinary)
                .unwrap()
                .fill(())
                .unwrap();
            runtime.poll_one(Tick(1)).unwrap();
            let ticket = runtime
                .take_effect(actor, input, ActionId::new(1).unwrap())
                .unwrap();
            if let Some(previous) = &previous {
                fresh &= !Arc::ptr_eq(previous, &ticket.origin);
            }
            // This retained marker is only metadata. The exclusive ticket is
            // consumed before input retirement or actor removal can succeed.
            previous = Some(ticket.origin.clone());
            runtime.validate_effect_ticket(&ticket).unwrap();
            runtime.effect_completed(ticket).unwrap();
            runtime.retire_input(actor, input).unwrap();
            scope.close_external().unwrap();
            work.body_exited().unwrap();
            work.captures_exited().unwrap();
            ledger.reap_scope_ready(scope.id(), 1).unwrap();
            while let Some(original_host_turn) = runtime.claim_host_actor() {
                original_host_turn.release(false).unwrap();
            }
            runtime.remove_actor(actor).unwrap();
            capacity.release(&capacity.resources().unwrap()).unwrap();
            ledger.remove_scope(scope.id()).unwrap();
        }
        assert!(runtime.is_empty());
        assert!(ledger.is_empty());
        assert!(
            fresh,
            "the same public ActorId cannot inherit an old ticket origin"
        );
    }
}
