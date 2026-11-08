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

mod actor_failure_publication;
mod execution_detach;
mod failed_custodian_timer;
mod poller_observation;
#[cfg(not(loom))]
pub use poller_observation::{
    PollerObservation, PollerObservationSnapshot, PollerPhase, PollerTimerSnapshot,
    PollerWorkerSnapshot,
};

use crate::{
    actor::{Actor, ActorConfig, ActorFailurePolicy, ActorStep, StepEnvelope},
    notify::Notify,
    sync::{Arc, Mutex},
    transaction::{RuntimeError, ScopeHandle},
};
use failed_custodian_timer::FailedCustodianTimer;
use novarocks_control_runtime_core::{
    effect::{EffectDescriptor, EffectError, EffectKind, EffectState},
    history::HistoryFact,
    identity::{
        ActionId, ActorId, BatchId, Generation, InputId, ReservationId, ScopeId, ScopedWorkRef,
        SlotId, Tick, WorkId,
    },
    mailbox::{InputClass, InputEnvelope, MailboxError, MailboxState},
    scheduler::{ReadyQueue, SchedulerError, TurnBudget},
    scope::{WorkKind, WorkPhase},
    timer::{TimerError, TimerIndex},
};
use std::{
    any::Any,
    collections::{BTreeMap, BTreeSet},
    mem::ManuallyDrop,
    panic::{AssertUnwindSafe, catch_unwind},
};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ActorTurnError {
    Scheduler(SchedulerError),
    Mailbox(MailboxError),
    Effect(EffectError),
    Timer(TimerError),
    CustodianDeadlineMismatch,
    CustodianTimerReceiptMissing,
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
    UnknownStandingInput,
    StandingInputBusy,
    InputAuthorMismatch,
    InputAuthorInactive,
    FailurePublicationRequired,
    FailurePublicationAlreadyClaimed,
    FailureWorkInactive,
    ExecutionAlreadyDetached,
    RetainedInputRequired,
    MetadataStillPending,
    MetadataAlreadyForwarded,
    MetadataDeliveryAbandoned,
    ReservationsRemain,
    UnknownReservation,
    ReservationOwnerMismatch,
}
impl From<SchedulerError> for ActorTurnError {
    fn from(v: SchedulerError) -> Self {
        Self::Scheduler(v)
    }
}
impl From<MailboxError> for ActorTurnError {
    fn from(v: MailboxError) -> Self {
        Self::Mailbox(v)
    }
}
impl From<EffectError> for ActorTurnError {
    fn from(v: EffectError) -> Self {
        Self::Effect(v)
    }
}
impl From<TimerError> for ActorTurnError {
    fn from(v: TimerError) -> Self {
        Self::Timer(v)
    }
}
impl From<RuntimeError> for ActorTurnError {
    fn from(v: RuntimeError) -> Self {
        Self::Runtime(v)
    }
}
impl std::fmt::Display for ActorTurnError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "control driver failure: {self:?}")
    }
}
impl std::error::Error for ActorTurnError {}

/// Read-only identity of one original actor registration. Cloning this witness
/// grants no Work, publication or effect-ticket authority.
#[derive(Clone)]
pub struct ActorRegistration {
    actor: ActorId,
    attachment: SlotId,
    scope: Arc<RegistrationScope>,
    origin: Arc<()>,
}
impl ActorRegistration {
    pub fn actor(&self) -> ActorId {
        self.actor
    }
    pub fn attachment(&self) -> SlotId {
        self.attachment
    }
    pub(crate) fn publication_scope(&self) -> ScopeHandle {
        self.scope.current_scope()
    }
    pub fn scope_id(&self) -> ScopeId {
        self.scope.id()
    }
    /// The original actor epoch survives execution-scope detachment unchanged.
    /// It is an identity fact, not current admission or failure authority.
    pub fn initial_epoch(&self) -> Generation {
        self.scope.original_epoch
    }
    /// Equal public numbers do not identify the same registration. This is a
    /// read-only origin comparison and grants no scheduling or Work authority.
    pub fn same_registration(&self, other: &Self) -> bool {
        self.actor == other.actor
            && self.attachment == other.attachment
            && Arc::ptr_eq(&self.scope, &other.scope)
            && Arc::ptr_eq(&self.origin, &other.origin)
    }
    /// Match only a still-attached execution scope. After detachment the
    /// historical scope_id remains, but no execution scope is owned here.
    pub fn matches_scope(&self, scope: &ScopeHandle) -> bool {
        self.scope.matches_live_scope(scope)
    }
    /// A read-only observation of this same registration's execution binding.
    /// False grants no request, Work or publication right.
    pub fn execution_attached(&self) -> bool {
        !self.scope.retained()
    }
    /// Observe only this original registration's actual retained affiliation.
    /// This grants no Work, request admission or publication authority.
    pub fn retained_in_process(&self, process: &ScopeHandle) -> bool {
        let execution = self
            .scope
            .execution
            .lock()
            .expect("actor execution binding lock");
        matches!(&*execution, ActorExecution::Retained { process: actual } if actual.same_scope(process))
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

/// An immutable binding issued only from an actual admitted input position.
/// Cloning it copies no input credit, WorkObligation, report leg or consumption state.
#[derive(Clone)]
pub struct InputAuthor {
    actor: ActorId,
    origin: Arc<()>,
    input: InputId,
    actor_generation: Generation,
    pub(crate) scope: ScopeHandle,
    work: ScopedWorkRef,
    generation: Generation,
}
impl std::fmt::Debug for InputAuthor {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("InputAuthor")
            .field("actor", &self.actor)
            .field("input", &self.input)
            .field("work", &self.work)
            .field("actor_generation", &self.actor_generation)
            .field("generation", &self.generation)
            .finish_non_exhaustive()
    }
}
#[derive(Clone)]
pub(crate) struct PendingInputAuthor {
    pub(crate) registration: ActorRegistration,
    pub(crate) scope: ScopeHandle,
    work: ScopedWorkRef,
    actor_generation: Generation,
    generation: Generation,
}
impl PendingInputAuthor {
    pub(crate) fn matches(&self, author: &InputAuthor) -> bool {
        self.registration.actor == author.actor
            && Arc::ptr_eq(&self.registration.origin, &author.origin)
            && self.scope.same_scope(&author.scope)
            && self.work == author.work
            && self.actor_generation == author.actor_generation
            && self.generation == author.generation
    }
}
impl InputAuthor {
    pub const fn actor(&self) -> ActorId {
        self.actor
    }
    /// Original admitted position, retained across persistent-role occurrences.
    /// The current command's token still comes from its actual InputEnvelope.
    pub const fn input(&self) -> InputId {
        self.input
    }
    pub const fn work(&self) -> ScopedWorkRef {
        self.work
    }
    pub const fn generation(&self) -> Generation {
        self.generation
    }
    /// Compare only the same original read-only binding; this grants no credit.
    pub fn same_binding(&self, other: &Self) -> bool {
        self.actor == other.actor
            && Arc::ptr_eq(&self.origin, &other.origin)
            && self.input == other.input
            && self.work == other.work
            && self.actor_generation == other.actor_generation
            && self.generation == other.generation
            && self.scope.same_scope(&other.scope)
    }
}

/// Proof of one actual original actor's fenced failure. Its financial scope
/// may remain healthy or its old execution scope may already have been removed.
/// Cloning grants no Work, publication authority or additional report leg.
#[derive(Clone)]
pub struct ActorFailureProof {
    registration: ActorRegistration,
    failed_epoch: Generation,
    ledger: crate::transaction::RuntimeLedger,
}
impl ActorFailureProof {
    pub fn registration(&self) -> &ActorRegistration {
        &self.registration
    }
    pub fn initial_epoch(&self) -> Generation {
        self.registration.initial_epoch()
    }
    pub const fn failed_epoch(&self) -> Generation {
        self.failed_epoch
    }
    pub(crate) fn validate_consumer(
        &self,
        registration: &ActorRegistration,
        ledger: &crate::transaction::RuntimeLedger,
    ) -> Result<(), ActorTurnError> {
        if !self.registration.same_registration(registration)
            || self.initial_epoch() != registration.initial_epoch()
            || self.failed_epoch <= self.initial_epoch()
            || !Arc::ptr_eq(&self.ledger.shared, &ledger.shared)
        {
            return Err(ActorTurnError::PublicationTargetMismatch);
        }
        Ok(())
    }
}
impl std::fmt::Debug for ActorFailureProof {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ActorFailureProof")
            .field("registration", &self.registration)
            .field("initial_epoch", &self.initial_epoch())
            .field("failed_epoch", &self.failed_epoch)
            .finish_non_exhaustive()
    }
}

// Every witness shares one binding. Detachment unloads the old ScopeHandle
// without changing the original registration's identity.
struct RegistrationScope {
    original: ScopeId,
    original_epoch: Generation,
    execution: Mutex<ActorExecution>,
}
enum ActorExecution {
    Live {
        scope: ScopeHandle,
        failure_work: Option<WorkId>,
    },
    Retained {
        process: ScopeHandle,
    },
}
impl RegistrationScope {
    fn id(&self) -> ScopeId {
        self.original
    }
    fn current_scope(&self) -> ScopeHandle {
        match &*self.execution.lock().expect("actor execution binding lock") {
            ActorExecution::Live { scope, .. } => scope.clone(),
            ActorExecution::Retained { process } => process.clone(),
        }
    }
    fn matches_live_scope(&self, candidate: &ScopeHandle) -> bool {
        matches!(&*self.execution.lock().expect("actor execution binding lock"),
            ActorExecution::Live { scope, .. } if scope.same_scope(candidate))
    }
    fn retained(&self) -> bool {
        matches!(
            &*self.execution.lock().expect("actor execution binding lock"),
            ActorExecution::Retained { .. }
        )
    }
    fn failure_work(&self) -> Option<ScopedWorkRef> {
        match &*self.execution.lock().expect("actor execution binding lock") {
            ActorExecution::Live {
                scope,
                failure_work,
            } => failure_work.map(|work| ScopedWorkRef {
                scope: scope.id(),
                work,
            }),
            ActorExecution::Retained { .. } => None,
        }
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

/// One independently funded supervisor position per isolated actor. It never
/// borrows a triggering request/source receipt or enters the normal mailbox.
pub const ISOLATED_FAILURE_INPUT_POSITIONS: usize = 1;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum FailureSourceState {
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
    pub standing_input_id: Option<StandingInputId>,
    /// This original position belongs to the runtime's Workless forwarding port.
    pub runtime_metadata: bool,
}
struct InputRecord {
    source: ActorId,
    class: InputClass,
    phase: InputPhase,
    standing_input_id: Option<StandingInputId>,
    author: Option<InputAuthor>,
    producer: Option<InputAuthor>,
    metadata_delivery: Option<MetadataDelivery>,
    header_pins: Vec<crate::obligation::WorkHeaderObligation>,
    handoff_at: Option<Tick>,
}
impl InputRecord {
    fn release_header_custody(&mut self, ledger: &mut crate::transaction::LedgerState) {
        for pin in self.header_pins.drain(..) {
            pin.release_locked(ledger);
        }
    }
}
/// Stable identity of one admitted role position; this identity does not mint authority.
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct StandingInputId(InputId);
impl StandingInputId {
    pub const fn get(self) -> u64 {
        self.0.get()
    }
}
struct StandingInputRecord {
    source: ActorId,
    class: InputClass,
    producer: Option<InputAuthor>,
    producer_header: Option<crate::obligation::WorkHeaderObligation>,
    in_flight: Option<InputId>,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct StandingInputCustody {
    pub standing_input_id: StandingInputId,
    pub source: ActorId,
    pub class: InputClass,
    pub in_flight: Option<InputId>,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum FailurePublicationOrigin {
    SupervisorEffect {
        input: InputId,
        action: ActionId,
    },
    StartedCleanup {
        financial_generation: Generation,
        header: novarocks_control_runtime_core::owned_storage::OwnedPositionId,
    },
}
enum FailurePublicationRecord {
    Active {
        generation: Generation,
        origin: FailurePublicationOrigin,
        work: ScopedWorkRef,
    },
    // The publication phase ended explicitly. No active claim or Work is
    // retained here, and this failure incarnation cannot be claimed again.
    Closed,
}
struct Inbox<A: Actor> {
    observation: crate::observation::RuntimeObservation,
    external_open: bool,
    sealed: bool,
    payloads: BTreeMap<InputId, A::Input>,
    mailbox: MailboxState,
    input_source: Option<ActorId>,
    prefer_actor_source: bool,
    inputs: BTreeMap<InputId, InputRecord>,
    standing_inputs: BTreeMap<StandingInputId, StandingInputRecord>,
    counts: [usize; 4],
    input_limits: [usize; 4],
    metadata_credit: [MetadataCreditQueue; 4],
    metadata_closing: bool,
    next_input: u64,
    batches: BTreeMap<InputId, StepEnvelope<A::Effect>>,
    batch_order: BTreeMap<BatchId, InputId>,
    next_batch: u64,
    generation: Generation,
    failed: bool,
    timer_input: Option<InputId>,
    // Monotonic source closure; an already filled Tick retains its input.
    timer_closed: bool,
    failed_custodian_timer: Option<FailedCustodianTimer>,
    // This identity is reserved at registration. Normal inputs/batches cannot
    // spend the separate supervisor position. No message payload is invented.
    failure_position: Option<InputId>,
    failure_input: Option<InputId>,
    failure: Option<ManuallyDrop<Box<dyn Any + Send>>>,
    last_progress: Tick,
    failure_publication: Option<FailurePublicationRecord>,
    failure_action_watermark: u64,
}
impl<A: Actor> Inbox<A> {
    fn ready_classes(&self, actor: ActorId) -> [bool; 4] {
        std::array::from_fn(|index| {
            let class = InputClass::ALL[index];
            self.input_source.map_or_else(
                || self.mailbox.has_ready(class),
                |source| {
                    self.mailbox.has_ready_from(class, source)
                        || self.mailbox.has_ready_from(class, actor)
                },
            )
        })
    }
    fn has_selectable_input(&self, actor: ActorId) -> bool {
        self.ready_classes(actor).into_iter().any(|ready| ready)
    }
    fn pop_selected(&mut self, actor: ActorId, class: InputClass) -> Option<InputEnvelope> {
        let Some(source) = self.input_source else {
            return self.mailbox.pop(class);
        };
        let continuation = self.mailbox.has_ready_from(class, source);
        let own = self.mailbox.has_ready_from(class, actor);
        let chosen = match (continuation, own) {
            (true, true) if source != actor => {
                let chosen = if self.prefer_actor_source {
                    actor
                } else {
                    source
                };
                self.prefer_actor_source = !self.prefer_actor_source;
                chosen
            }
            (true, _) => source,
            (_, true) => actor,
            _ => return None,
        };
        self.mailbox.pop_from(class, chosen)
    }
    /// The single paid standing-input fill algorithm shared by direct and effect-backed
    /// handoff. The caller holds this inbox and qualifies the scope failure gate.
    fn fill_standing_input(
        &mut self,
        input_address: StandingInputId,
        scope_failed: bool,
        financial_author: Option<&InputAuthor>,
        ledger: &mut crate::transaction::LedgerState,
        observed: Option<Tick>,
        value: A::Input,
    ) -> Result<InputId, (ActorTurnError, A::Input)> {
        if self.sealed {
            return Err((ActorTurnError::Closed, value));
        }
        if self.failed || scope_failed {
            return Err((ActorTurnError::ActorFailed, value));
        }
        let Some(standing_input_record) = self.standing_inputs.get(&input_address) else {
            return Err((ActorTurnError::UnknownStandingInput, value));
        };
        if standing_input_record.in_flight.is_some() {
            return Err((ActorTurnError::StandingInputBusy, value));
        }
        let (source, class) = (standing_input_record.source, standing_input_record.class);
        let producer = standing_input_record.producer.clone();
        let author = financial_author.cloned().or_else(|| producer.clone());
        let Some(next) = self.next_input.checked_add(1) else {
            return Err((ActorTurnError::IdentityExhausted, value));
        };
        let input = InputId::new(next).expect("incremented standing input id");
        let mut pins = Vec::new();
        if let Some(pin) = standing_input_record.producer_header.as_ref() {
            match pin.retain_locked(ledger) {
                Ok(original) => pins.push(original),
                Err(error) => return Err((error.into(), value)),
            }
        }
        if let Some(author) = financial_author
            && !pins
                .iter()
                .any(|pin| pin.reference() == author.work && pin.belongs_to(&author.scope))
        {
            match crate::obligation::WorkHeaderObligation::for_input_author_locked(author, ledger) {
                Ok(original) => pins.push(original),
                Err(error) => {
                    for pin in pins {
                        pin.release_locked(ledger);
                    }
                    return Err((error.into(), value));
                }
            }
        }
        if let Err(error) = self.mailbox.push(class, source, input) {
            for pin in pins {
                pin.release_locked(ledger);
            }
            return Err((error.into(), value));
        }
        self.next_input = next;
        self.inputs.insert(
            input,
            InputRecord {
                source,
                class,
                phase: InputPhase::Filled,
                standing_input_id: Some(input_address),
                author,
                producer,
                metadata_delivery: None,
                header_pins: pins,
                handoff_at: observed,
            },
        );
        self.payloads.insert(input, value);
        self.standing_inputs
            .get_mut(&input_address)
            .expect("idle role retains its paid position")
            .in_flight = Some(input);
        Ok(input)
    }
    fn available_input_credit(&self, class: InputClass) -> usize {
        self.input_limits[class.index()]
            .checked_sub(
                self.counts[class.index()]
                    .checked_add(self.metadata_credit[class.index()].granted)
                    .expect("bounded input credit"),
            )
            .expect("actual input use and grants stay inside the original window")
    }
    fn grant_metadata_credit(&mut self, class: InputClass) -> Vec<ActorRegistration> {
        if self.failed || self.metadata_closing || self.sealed {
            return Vec::new();
        }
        let available = self
            .available_input_credit(class)
            .min(METADATA_CREDIT_WAKE_BUDGET);
        self.metadata_credit[class.index()].grant(available)
    }
    fn return_input_credit(&mut self, class: InputClass) -> Vec<ActorRegistration> {
        self.observation.input_credit_returned(class);
        self.counts[class.index()] = self.counts[class.index()]
            .checked_sub(1)
            .expect("original input credit");
        self.grant_metadata_credit(class)
    }
    fn close_metadata_waiters(&mut self, budget: usize) -> (Vec<ActorRegistration>, bool) {
        self.metadata_closing = true;
        let mut wakes = Vec::new();
        for class in InputClass::ALL {
            let remaining = budget.saturating_sub(wakes.len());
            if remaining == 0 {
                break;
            }
            wakes.extend(self.metadata_credit[class.index()].close(remaining));
        }
        let remains = self
            .metadata_credit
            .iter()
            .any(|queue| !queue.entries.is_empty());
        (wakes, remains)
    }
    fn retire_metadata_input_if_ready(
        &mut self,
        input: InputId,
        ledger: &mut crate::transaction::LedgerState,
    ) -> Vec<ActorRegistration> {
        let Some(record) = self.inputs.get(&input) else {
            return Vec::new();
        };
        if self.failed
            || record.metadata_delivery.is_none()
            || !matches!(
                record.phase,
                InputPhase::Committed | InputPhase::AbandonedByOwnerFailure
            )
            || self.payloads.contains_key(&input)
        {
            return Vec::new();
        }
        if self.batches.get(&input).is_some_and(|batch| {
            !batch.state.all_terminal()
                || !batch.reservations.is_empty()
                || batch.metadata.iter().any(|item| !item.completed)
        }) {
            return Vec::new();
        }
        assert!(
            record.standing_input_id.is_none()
                && record.author.is_none()
                && record.producer.is_none(),
            "forwarded metadata never owns a Work or producer role"
        );
        let class = record.class;
        if let Some(batch) = self.batches.remove(&input) {
            let ordinal = batch.ordinal.expect("committed metadata input batch");
            assert_eq!(self.batch_order.remove(&ordinal), Some(input));
        }
        self.inputs
            .remove(&input)
            .expect("same original metadata input")
            .release_header_custody(ledger);
        self.return_input_credit(class)
    }
    fn next_batch_ordinal(&self) -> Result<BatchId, ActorTurnError> {
        let next = self
            .next_batch
            .checked_add(1)
            .ok_or(ActorTurnError::IdentityExhausted)?;
        Ok(BatchId::new(next).expect("incremented committed batch ordinal"))
    }
    fn commit_batch(
        &mut self,
        input: InputId,
        mut batch: StepEnvelope<A::Effect>,
    ) -> Result<(), ActorTurnError> {
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
            self.failure_position.is_none() && self.failure_input.is_none(),
            "the builtin failure source must close before driver removal"
        );
        assert!(
            self.failure.is_none(),
            "actor panic payload must be transferred to its host failure owner"
        );
        assert!(
            self.metadata_credit
                .iter()
                .all(|queue| queue.entries.is_empty()
                    && queue.waiting.is_empty()
                    && queue.granted == 0),
            "all original metadata scheduler waits must close before actor removal"
        );
        assert!(
            self.standing_inputs.is_empty(),
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
    scope: Arc<RegistrationScope>,
    attachment: SlotId,
    config: ActorConfig,
    failure_policy: ActorFailurePolicy,
    core: Mutex<Option<A>>,
    inbox: Mutex<Inbox<A>>,
}
impl<A: Actor> ActorCell<A> {
    fn history_target(&self) -> novarocks_control_runtime_core::history::HistoryActorTarget {
        novarocks_control_runtime_core::history::HistoryActorTarget {
            actor: self.id,
            attachment: self.attachment,
            scope_origin: self.scope.id(),
        }
    }
}
struct ActorRuntimeState<A: Actor> {
    actors: BTreeMap<ActorId, Arc<ActorCell<A>>>,
    ready: ReadyQueue,
    effect_ready: ReadyQueue,
    timers: TimerIndex,
    open: bool,
    now: Tick,
}
struct ActorRuntimeShared<A: Actor> {
    state: Mutex<ActorRuntimeState<A>>,
    changed: Arc<Notify>,
    header_readiness: Arc<crate::header_readiness::HeaderReadyHub>,
    capacity_wakes: Mutex<Vec<Arc<crate::capacity_notification::PendingCapacityWakes>>>,
    capacity_notifier: std::sync::Arc<crate::capacity_notification::CapacityWakeNotifier>,
    history: Option<crate::history::HistoryCapture>,
    observation: crate::observation::RuntimeObservation,
}
impl<A: Actor> ActorRuntimeShared<A> {
    fn record(&self, fact: impl FnOnce() -> HistoryFact) {
        if let Some(history) = &self.history {
            history.record(fact());
        }
    }
}
pub struct ActorRuntime<A: Actor> {
    shared: Arc<ActorRuntimeShared<A>>,
}
impl<A: Actor> Clone for ActorRuntime<A> {
    fn clone(&self) -> Self {
        Self {
            shared: self.shared.clone(),
        }
    }
}

/// Exclusive dispatch ownership for one original actor's host metadata turn.
/// This is not a WorkObligation, input, publication or capacity permit. Dropping an
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
    /// Drain only neutral readiness from this original actor incarnation.
    /// The caller must match each actual cell to its original Work owner and
    /// probe real header state; a notice supplies no physical exit authority.
    pub fn take_header_readiness(
        &self,
        budget: usize,
    ) -> Vec<crate::header_readiness::HeaderReadyNotice> {
        self.runtime
            .shared
            .header_readiness
            .take(&Self::header_owner(&self.node), budget)
    }
    fn header_owner(node: &ActorCell<A>) -> crate::header_readiness::HeaderOwner {
        crate::header_readiness::HeaderOwner {
            actor: node.id,
            attachment: node.attachment,
            origin: node.origin.clone(),
        }
    }
    pub fn matches_registration(&self, registration: &ActorRegistration) -> bool {
        self.node.id == registration.actor
            && self.node.attachment == registration.attachment
            && Arc::ptr_eq(&self.node.scope, &registration.scope)
            && Arc::ptr_eq(&self.node.origin, &registration.origin)
    }
    /// Unload execution only after its actual custody retires. The same
    /// actor registration, scheduler address and builtin timer survive.
    pub fn detach_execution(&mut self, process: &ScopeHandle) -> Result<(), ActorTurnError> {
        self.runtime.detach_execution_node(&self.node, process)
    }
    /// Taking a notice does not acknowledge its destination's consumption.
    pub fn take_metadata(
        &mut self,
        input: InputId,
    ) -> Result<Option<MetadataTicket<A::Effect>>, ActorTurnError> {
        let mut inbox = self.node.inbox.lock().expect("actor inbox lock");
        let batch = inbox
            .batches
            .get_mut(&input)
            .ok_or(ActorTurnError::UnknownInput)?;
        let Some((ordinal, record)) = batch
            .metadata
            .iter_mut()
            .enumerate()
            .find(|(_, item)| item.value.is_some())
        else {
            return Ok(None);
        };
        Ok(Some(MetadataTicket {
            actor: self.node.id,
            input,
            ordinal,
            origin: self.node.origin.clone(),
            value: record.value.take().expect("selected original metadata"),
        }))
    }
    /// A mark racing this release is retained by the same ready queue, even
    /// when the caller has no currently runnable work of its own.
    pub fn release(mut self, has_runnable: bool) -> Result<(), (ActorTurnError, Self)> {
        if let Err(error) = self.runtime.release_host_turn(&self.node, has_runnable) {
            return Err((error, self));
        }
        self.active = false;
        Ok(())
    }
    /// Retire only after actual Scope/input/effect custody has drained. The
    /// current claim and pending metadata marks are withdrawn atomically with
    /// this original actor's removal. A refusal returns the same active turn.
    pub fn retire_actor(mut self) -> Result<A, (ActorTurnError, Self)> {
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

const METADATA_CREDIT_WAKE_BUDGET: usize = 16;
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
struct MetadataWaitKey {
    actor: ActorId,
    input: InputId,
    ordinal: usize,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum MetadataCreditState {
    Waiting,
    Ready,
    Forwarded,
    Cancelled,
    Closed,
}
/// Scheduling metadata for the same pending original fact, never another fact,
/// input position, Work or acknowledgement of destination consumption.
#[derive(Clone)]
pub(crate) struct MetadataCreditWait {
    key: MetadataWaitKey,
    source: ActorRegistration,
    target: ActorRegistration,
    class: InputClass,
    state: Arc<Mutex<MetadataCreditState>>,
}
impl MetadataCreditWait {
    fn status(&self) -> MetadataCreditState {
        *self.state.lock().expect("original metadata credit wait")
    }
    fn same_wait(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.state, &other.state)
    }
    fn set(&self, next: MetadataCreditState) {
        *self.state.lock().expect("original metadata credit wait") = next;
    }
}
struct MetadataCreditQueue {
    entries: BTreeMap<MetadataWaitKey, MetadataCreditWait>,
    waiting: BTreeMap<ActorId, BTreeSet<MetadataWaitKey>>,
    ready: ReadyQueue,
    granted: usize,
}
impl MetadataCreditQueue {
    fn new() -> Self {
        Self {
            entries: BTreeMap::new(),
            waiting: BTreeMap::new(),
            ready: ReadyQueue::new(),
            granted: 0,
        }
    }
    fn register(&mut self, waiter: MetadataCreditWait) {
        assert!(
            !self.entries.contains_key(&waiter.key),
            "one waiter per original metadata fact"
        );
        if !self.waiting.contains_key(&waiter.key.actor) {
            self.ready
                .register(waiter.key.actor)
                .expect("fresh metadata waiting source");
        }
        self.waiting
            .entry(waiter.key.actor)
            .or_default()
            .insert(waiter.key);
        self.ready
            .notify(waiter.key.actor)
            .expect("registered metadata waiting source");
        self.entries.insert(waiter.key, waiter);
    }
    fn grant(&mut self, budget: usize) -> Vec<ActorRegistration> {
        let mut wakes = Vec::new();
        for _ in 0..budget {
            let Some(actor) = self.ready.claim() else {
                break;
            };
            let group = self
                .waiting
                .get_mut(&actor)
                .expect("queued metadata source group");
            let key = group.pop_first().expect("queued metadata fact");
            let more = !group.is_empty();
            self.ready
                .release(actor, more)
                .expect("same metadata ready claim");
            if !more {
                self.waiting.remove(&actor);
                self.ready
                    .unregister(actor)
                    .expect("idle metadata source group");
            }
            let waiter = self
                .entries
                .get(&key)
                .expect("original metadata scheduling entry");
            assert_eq!(waiter.status(), MetadataCreditState::Waiting);
            waiter.set(MetadataCreditState::Ready);
            self.granted = self
                .granted
                .checked_add(1)
                .expect("bounded actual input credit");
            wakes.push(waiter.source.clone());
        }
        wakes
    }
    fn remove(&mut self, waiter: &MetadataCreditWait, next: MetadataCreditState) -> bool {
        let Some(original) = self.entries.get(&waiter.key) else {
            return false;
        };
        assert!(
            original.same_wait(waiter),
            "one original metadata scheduling link"
        );
        let original = self
            .entries
            .remove(&waiter.key)
            .expect("validated metadata scheduling entry");
        match original.status() {
            MetadataCreditState::Ready => {
                self.granted = self
                    .granted
                    .checked_sub(1)
                    .expect("original granted input credit");
            }
            MetadataCreditState::Waiting => {
                let group = self
                    .waiting
                    .get_mut(&waiter.key.actor)
                    .expect("original waiting source group");
                assert!(group.remove(&waiter.key));
                if group.is_empty() {
                    self.waiting.remove(&waiter.key.actor);
                    self.ready
                        .cancel_pending(waiter.key.actor)
                        .expect("unclaimed original credit waiter");
                    self.ready
                        .unregister(waiter.key.actor)
                        .expect("idle original credit waiter");
                }
            }
            _ => panic!("only pending or granted scheduling entries remain indexed"),
        }
        original.set(next);
        true
    }
    fn close(&mut self, budget: usize) -> Vec<ActorRegistration> {
        let mut wakes = Vec::new();
        for _ in 0..budget {
            let Some(waiter) = self
                .entries
                .first_key_value()
                .map(|(_, waiter)| waiter.clone())
            else {
                break;
            };
            assert!(self.remove(&waiter, MetadataCreditState::Closed));
            wakes.push(waiter.source.clone());
        }
        wakes
    }
}

/// Only the actual destination Core can establish consumption of a forwarded
/// metadata fact. Effect completion and physical exit are independent facts.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum MetadataConsumption {
    Pending,
    Consumed,
    AbandonedByOwnerFailure,
}

/// Read-only shared receipt for one original source metadata / target input.
/// Clones issue no input, Work, metadata ticket, or acknowledgement authority.
#[derive(Clone)]
pub struct MetadataDelivery {
    source: ActorRegistration,
    target: ActorRegistration,
    input: InputId,
    state: Arc<Mutex<MetadataConsumption>>,
}
impl MetadataDelivery {
    pub fn consumption(&self) -> MetadataConsumption {
        *self
            .state
            .lock()
            .expect("original metadata delivery receipt")
    }
    pub const fn input(&self) -> InputId {
        self.input
    }
    pub fn target_registration(&self) -> &ActorRegistration {
        &self.target
    }
    fn resolve(&self, next: MetadataConsumption) -> bool {
        let mut state = self
            .state
            .lock()
            .expect("original metadata delivery receipt");
        if *state != MetadataConsumption::Pending {
            return false;
        }
        *state = next;
        true
    }
}
impl std::fmt::Debug for MetadataDelivery {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MetadataDelivery")
            .field("source", &self.source.actor)
            .field("target", &self.target.actor)
            .field("input", &self.input)
            .field("consumption", &self.consumption())
            .finish_non_exhaustive()
    }
}

/// Original neutral retained-state notice. It is no Work, I/O or publication
/// authority. Drop never acknowledges consumption or releases its custody.
pub struct MetadataTicket<E> {
    actor: ActorId,
    input: InputId,
    ordinal: usize,
    origin: Arc<()>,
    value: E,
}
impl<E> MetadataTicket<E> {
    pub fn value(&self) -> &E {
        &self.value
    }
}
impl<E> std::fmt::Debug for MetadataTicket<E> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MetadataTicket")
            .field("actor", &self.actor)
            .field("input", &self.input)
            .field("ordinal", &self.ordinal)
            .finish_non_exhaustive()
    }
}
/// This is neutral input storage, not a host WorkObligation. Bound positions remain
/// resident until captures, effects and report consumers have actually retired.
#[must_use = "Reserve before binding payloads; fill or explicitly revoke an unused input"]
pub struct ReservedInputObligation<A: Actor> {
    runtime: ActorRuntime<A>,
    actor: ActorId,
    origin: Arc<()>,
    input: InputId,
}
impl<A: Actor> std::fmt::Debug for ReservedInputObligation<A> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ReservedInputObligation")
            .field("actor", &self.actor)
            .field("input", &self.input)
            .finish()
    }
}
/// One pre-admitted, non-cloneable neutral notification position. It is not
/// work admission and never starts a host body. Drop retains its class credit.
#[must_use = "A repeated input role must close after actual holder/capture and report retirement"]
pub struct StandingInputObligation<A: Actor> {
    runtime: ActorRuntime<A>,
    actor: ActorId,
    origin: Arc<()>,
    standing_input_id: StandingInputId,
    active: bool,
}
impl<A: Actor> std::fmt::Debug for StandingInputObligation<A> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("StandingInputObligation")
            .field("actor", &self.actor)
            .field("role", &self.standing_input_id)
            .field("active", &self.active)
            .finish()
    }
}
impl<A: Actor> StandingInputObligation<A> {
    fn original_node(&self) -> Result<Arc<ActorCell<A>>, ActorTurnError> {
        let node = self.runtime.node(self.actor)?;
        if !Arc::ptr_eq(&self.origin, &node.origin) {
            return Err(ActorTurnError::EffectTicketOriginMismatch);
        }
        Ok(node)
    }
    pub const fn id(&self) -> StandingInputId {
        self.standing_input_id
    }
    pub fn author(&self) -> Result<InputAuthor, ActorTurnError> {
        let node = self.original_node()?;
        let inbox = node.inbox.lock().expect("actor inbox lock");
        inbox
            .standing_inputs
            .get(&self.standing_input_id)
            .ok_or(ActorTurnError::UnknownStandingInput)?
            .producer
            .clone()
            .ok_or(ActorTurnError::InputAuthorMismatch)
    }
    pub fn input_author(&self, input: InputId) -> Result<InputAuthor, ActorTurnError> {
        let node = self.original_node()?;
        let inbox = node.inbox.lock().expect("actor inbox lock");
        let record = inbox
            .inputs
            .get(&input)
            .ok_or(ActorTurnError::UnknownInput)?;
        if record.standing_input_id != Some(self.standing_input_id) {
            return Err(ActorTurnError::InputAuthorMismatch);
        }
        record
            .author
            .clone()
            .ok_or(ActorTurnError::InputAuthorMismatch)
    }
    pub fn in_flight(&self) -> Result<Option<InputId>, ActorTurnError> {
        let node = self.original_node()?;
        let inbox = node.inbox.lock().expect("actor inbox lock");
        Ok(inbox
            .standing_inputs
            .get(&self.standing_input_id)
            .ok_or(ActorTurnError::UnknownStandingInput)?
            .in_flight)
    }
    /// Fills only an idle, already paid position, including during process drain.
    /// Every occurrence receives a fresh monotonic InputId; a prior effect ticket
    /// can never identify this new batch. Error returns the original neutral value.
    pub fn fill(&self, value: A::Input) -> Result<InputId, (ActorTurnError, A::Input)> {
        self.fill_inner(None, value)
    }
    /// The original producer position and financial author remain distinct.
    pub fn fill_from_author(
        &self,
        author: &InputAuthor,
        value: A::Input,
    ) -> Result<InputId, (ActorTurnError, A::Input)> {
        self.fill_inner(Some(author), value)
    }
    fn fill_inner(
        &self,
        author: Option<&InputAuthor>,
        value: A::Input,
    ) -> Result<InputId, (ActorTurnError, A::Input)> {
        let observed = self.runtime.shared.observation.now();
        if !self.active {
            return Err((ActorTurnError::UnknownStandingInput, value));
        }
        let node = match self.original_node() {
            Ok(node) => node,
            Err(error) => return Err((error, value)),
        };
        let mut inbox = node.inbox.lock().expect("actor inbox lock");
        let execution_scope = node.scope.current_scope();
        if inbox.sealed {
            return Err((ActorTurnError::Closed, value));
        }
        let mut ledger = execution_scope
            .runtime
            .shared
            .state
            .lock()
            .expect("runtime ledger lock");
        let scope_failed = match ledger.scopes.get(&execution_scope.id()) {
            Some(scope) => !node.scope.retained() && scope.failed(),
            None => return Err((RuntimeError::UnknownScope.into(), value)),
        };
        if let Some(author) = author
            && let Err(error) = ActorRuntime::<A>::validate_input_author(
                &node, &inbox, &ledger, author, false, true,
            )
        {
            return Err((error, value));
        }
        let result = inbox.fill_standing_input(
            self.standing_input_id,
            scope_failed,
            author,
            &mut ledger,
            observed,
            value,
        );
        drop(ledger);
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
    pub fn close_after_exit(&mut self) -> Result<(), ActorTurnError> {
        if !self.active {
            return Err(ActorTurnError::UnknownStandingInput);
        }
        self.runtime.close_standing_input_after_exit(
            self.actor,
            self.standing_input_id,
            false,
            Some(&self.origin),
        )?;
        self.active = false;
        Ok(())
    }
}
impl<A: Actor> ReservedInputObligation<A> {
    pub const fn id(&self) -> InputId {
        self.input
    }
}
impl<A: Actor> ReservedInputObligation<A> {
    fn original_node(&self) -> Result<Arc<ActorCell<A>>, ActorTurnError> {
        let node = self.runtime.node(self.actor)?;
        if !Arc::ptr_eq(&self.origin, &node.origin) {
            return Err(ActorTurnError::EffectTicketOriginMismatch);
        }
        Ok(node)
    }
    pub fn author(&self) -> Result<InputAuthor, ActorTurnError> {
        let node = self.original_node()?;
        let inbox = node.inbox.lock().expect("actor inbox lock");
        inbox
            .inputs
            .get(&self.input)
            .ok_or(ActorTurnError::UnknownInput)?
            .author
            .clone()
            .ok_or(ActorTurnError::InputAuthorMismatch)
    }
    /// Converts a pure unused reservation into one standing input obligation.
    /// The class charge moves intact; the original InputId is permanently burned.
    pub fn into_standing_input(self) -> Result<StandingInputObligation<A>, (ActorTurnError, Self)> {
        let node = match self.original_node() {
            Ok(node) => node,
            Err(error) => return Err((error, self)),
        };
        let mut inbox = node.inbox.lock().expect("actor inbox lock");
        let execution_scope = node.scope.current_scope();
        let scope_failed = match execution_scope.gates() {
            Ok(gates) => gates.failed,
            Err(error) => return Err((error.into(), self)),
        };
        if inbox.failed || scope_failed {
            return Err((ActorTurnError::ActorFailed, self));
        }
        let Some(input) = inbox.inputs.get(&self.input) else {
            return Err((ActorTurnError::UnknownInput, self));
        };
        if input.phase != InputPhase::Reserved {
            return Err((ActorTurnError::InputNotReserved, self));
        }
        let standing_input_id = StandingInputId(self.input);
        let (source, class) = (input.source, input.class);
        let producer = input.producer.clone();
        let mut original = inbox
            .inputs
            .remove(&self.input)
            .expect("same unused original input");
        let producer_header = original.header_pins.pop();
        assert!(
            original.header_pins.is_empty(),
            "an unused permit owns only its original producer header"
        );
        inbox.standing_inputs.insert(
            standing_input_id,
            StandingInputRecord {
                source,
                class,
                producer,
                producer_header,
                in_flight: None,
            },
        );
        Ok(StandingInputObligation {
            runtime: self.runtime.clone(),
            actor: self.actor,
            origin: node.origin.clone(),
            standing_input_id,
            active: true,
        })
    }
    /// Call before attaching any payload/capture. Bound positions cannot take unused revocation.
    pub fn bind(&self) -> Result<(), ActorTurnError> {
        let node = self.original_node()?;
        let mut inbox = node.inbox.lock().expect("actor inbox lock");
        let input = inbox
            .inputs
            .get_mut(&self.input)
            .ok_or(ActorTurnError::UnknownInput)?;
        if input.phase != InputPhase::Reserved {
            return Err(ActorTurnError::InputNotReserved);
        }
        input.phase = InputPhase::Bound;
        Ok(())
    }
    pub fn fill(self, value: A::Input) -> Result<InputId, (ActorTurnError, A::Input)> {
        let observed = self.runtime.shared.observation.now();
        let node = match self.original_node() {
            Ok(node) => node,
            Err(e) => return Err((e, value)),
        };
        let mut inbox = node.inbox.lock().expect("actor inbox lock");
        if inbox.sealed {
            return Err((ActorTurnError::Closed, value));
        }
        if inbox.failed {
            return Err((ActorTurnError::ActorFailed, value));
        }
        let Some(input) = inbox.inputs.get(&self.input) else {
            return Err((ActorTurnError::UnknownInput, value));
        };
        if !matches!(input.phase, InputPhase::Reserved | InputPhase::Bound) {
            return Err((ActorTurnError::InputNotReserved, value));
        }
        let (class, source) = (input.class, input.source);
        if let Err(e) = inbox.mailbox.push(class, source, self.input) {
            return Err((e.into(), value));
        }
        inbox.payloads.insert(self.input, value);
        let input = inbox.inputs.get_mut(&self.input).expect("reserved input");
        input.phase = InputPhase::Filled;
        input.handoff_at = observed;
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
    pub fn revoke_bound_after_exit(self) -> Result<(), ActorTurnError> {
        let node = self.original_node()?;
        let mut inbox = node.inbox.lock().expect("actor inbox lock");
        let input = inbox
            .inputs
            .get(&self.input)
            .ok_or(ActorTurnError::UnknownInput)?;
        if input.phase != InputPhase::Bound {
            return Err(ActorTurnError::InputNotReserved);
        }
        let class = input.class;
        let mut original = inbox
            .inputs
            .remove(&self.input)
            .expect("same original revoked input");
        let scope = node.scope.current_scope();
        let mut ledger = scope
            .runtime
            .shared
            .state
            .lock()
            .expect("original input header release");
        original.release_header_custody(&mut ledger);
        drop(ledger);
        self.runtime
            .notify_metadata_credit_sources(inbox.return_input_credit(class));
        Ok(())
    }
    /// The caller proves that no payload or capture has ever been bound.
    pub fn revoke_unused(self) -> Result<(), ActorTurnError> {
        let node = self.original_node()?;
        let mut inbox = node.inbox.lock().expect("actor inbox lock");
        let input = inbox
            .inputs
            .get(&self.input)
            .ok_or(ActorTurnError::UnknownInput)?;
        if input.phase != InputPhase::Reserved {
            return Err(ActorTurnError::InputNotReserved);
        }
        let class = input.class;
        let mut original = inbox
            .inputs
            .remove(&self.input)
            .expect("same original revoked input");
        let scope = node.scope.current_scope();
        let mut ledger = scope
            .runtime
            .shared
            .state
            .lock()
            .expect("original input header release");
        original.release_header_custody(&mut ledger);
        drop(ledger);
        self.runtime
            .notify_metadata_credit_sources(inbox.return_input_credit(class));
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
    pub author_generation: Generation,
    pub descriptor: EffectDescriptor,
}
/// An exclusive effect dispatch ticket from one actual actor incarnation.
/// It grants no new work or capacity. `descriptor.work` names an already
/// admitted WorkObligation; the host still starts that lease before running its body.
/// The ticket is neither physical-start nor physical-exit evidence.
///
/// Authority fields and the neutral effect are readable, but cannot be replaced:
/// ```compile_fail
/// use novarocks_control_runtime::actor_turn::EffectTicket;
/// fn replace_effect(mut ticket: EffectTicket<u64>) {
///     ticket.effect = 99;
/// }
/// ```
/// ```compile_fail
/// use novarocks_control_runtime::actor_turn::EffectTicket;
/// use novarocks_control_runtime_core::identity::ActorId;
/// fn redirect(mut ticket: EffectTicket<u64>, actor: ActorId) {
///     ticket.actor = actor;
/// }
/// ```
/// ```compile_fail
/// use novarocks_control_runtime::actor_turn::EffectTicket;
/// use novarocks_control_runtime_core::identity::ActionId;
/// fn redirect_action(mut ticket: EffectTicket<u64>, action: ActionId) {
///     ticket.descriptor.id = action;
/// }
/// ```
/// ```compile_fail
/// use novarocks_control_runtime::actor_turn::EffectTicket;
/// fn duplicate(ticket: &EffectTicket<u64>) -> EffectTicket<u64> {
///     ticket.clone()
/// }
/// ```
/// A metadata view cannot be converted into a new ticket:
/// ```compile_fail
/// use novarocks_control_runtime::actor_turn::{EffectTicket, EffectTicketView};
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
    pub error: ActorTurnError,
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
/// An original effect's actual Completed transition, independent of how long
/// its input/batch remains resident. This immutable receipt grants no Work,
/// dispatch, publication or exit authority and holds no effect payload.
#[must_use = "Forward the actual completion fact to its original ordering owner"]
pub struct CompletedEffect<A: Actor> {
    runtime: ActorRuntime<A>,
    registration: ActorRegistration,
    input: InputId,
    ordinal: BatchId,
    generation: Generation,
    descriptor: EffectDescriptor,
}
impl<A: Actor> CompletedEffect<A> {
    pub fn registration(&self) -> &ActorRegistration {
        &self.registration
    }
    pub const fn input(&self) -> InputId {
        self.input
    }
    pub fn descriptor(&self) -> &EffectDescriptor {
        &self.descriptor
    }
    pub const fn generation(&self) -> Generation {
        self.generation
    }
}
impl<A: Actor> std::fmt::Debug for CompletedEffect<A> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CompletedEffect")
            .field("registration", &self.registration)
            .field("input", &self.input)
            .field("ordinal", &self.ordinal)
            .field("generation", &self.generation)
            .field("descriptor", &self.descriptor)
            .finish()
    }
}
/// Read-only completion evidence from one original Started/Published ticket.
/// It retains no effect payload, Work permission or actor core. Construction
/// qualifies the original record; arbitrary missing IDs cannot create it.
pub struct EffectCompletionObserver<A: Actor> {
    runtime: ActorRuntime<A>,
    origin: Arc<()>,
    actor: ActorId,
    input: InputId,
    action: ActionId,
}
impl<A: Actor> std::fmt::Debug for EffectCompletionObserver<A> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("EffectCompletionObserver")
            .field("actor", &self.actor)
            .field("input", &self.input)
            .field("action", &self.action)
            .finish()
    }
}
impl<A: Actor> EffectCompletionObserver<A> {
    pub fn is_completed(&self) -> Result<bool, ActorTurnError> {
        let node = match self.runtime.node(self.actor) {
            Ok(node) => node,
            // This observer was issued while its actual action was Started.
            // Removing that registration requires every input/batch empty;
            // a Started/Published action cannot be abandoned on that path.
            Err(ActorTurnError::UnknownActor) => return Ok(true),
            Err(error) => return Err(error),
        };
        if !Arc::ptr_eq(&node.origin, &self.origin) {
            // A fresh registration can reuse the public actor/input numbers,
            // but only after the original actor passed the removal gate.
            return Ok(true);
        }
        let inbox = node.inbox.lock().expect("actor inbox lock");
        let Some(batch) = inbox.batches.get(&self.input) else {
            if inbox.inputs.contains_key(&self.input) {
                return Err(ActorTurnError::UnknownInput);
            }
            // The same registration never reuses InputIds. Its committed
            // input can disappear only via retire_input, which requires all
            // effects terminal; this original Started action had to complete.
            return Ok(true);
        };
        let record = batch
            .state
            .record(self.action)
            .ok_or(EffectError::UnknownAction)?;
        match record.state() {
            EffectState::Completed => Ok(true),
            EffectState::Started | EffectState::Published => Ok(false),
            _ => Err(EffectError::InvalidTransition.into()),
        }
    }
}
/// Exclusive publication authority within one actual failure supervision job.
/// It signs no work and never reads the failed core. Each commit touches one
/// actual target; application failure policy and bounded inventory loops remain
/// in the host. Drop neither exits the cleanup work nor rolls back any value.
#[must_use = "A failure session belongs to its original running cleanup work"]
pub struct FailurePublicationSession<A: Actor> {
    runtime: ActorRuntime<A>,
    registration: ActorRegistration,
    actor: ActorId,
    origin: Arc<()>,
    // Preserve the actual cleanup financial Scope independently of the failed
    // actor origin. A later actor incarnation or scope-local WorkId cannot
    // inherit this running job's authority.
    scope: ScopeHandle,
    generation: Generation,
    claim: FailurePublicationOrigin,
    work: ScopedWorkRef,
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
    /// Original failed target binding, independent of the cleanup financial Scope.
    /// Reading it grants no additional Work, report leg or publication action.
    pub fn registration(&self) -> &ActorRegistration {
        &self.registration
    }
    pub const fn generation(&self) -> Generation {
        self.generation
    }
    /// Read-only qualification of this original, still-running failure job.
    /// It signs no publication action and changes no watermark or custody.
    pub fn matches_actor_runtime(
        &self,
        runtime: &ActorRuntime<A>,
        actor: ActorId,
    ) -> Result<bool, ActorTurnError> {
        if actor != self.actor || !Arc::ptr_eq(&runtime.shared, &self.runtime.shared) {
            return Ok(false);
        }
        let node = runtime.node(actor)?;
        let inbox = node.inbox.lock().expect("actor inbox lock");
        let execution_scope = node.scope.current_scope();
        let ledger = execution_scope
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
    ) -> Result<(), ActorTurnError> {
        let affiliation = node.scope.current_scope();
        if node.id != self.actor || !Arc::ptr_eq(&node.origin, &self.origin) {
            return Err(ActorTurnError::EffectTicketOriginMismatch);
        }
        if !Arc::ptr_eq(&affiliation.runtime.shared, &self.scope.runtime.shared)
            || !Arc::ptr_eq(&self.scope.runtime.shared, &target.runtime.shared)
        {
            return Err(ActorTurnError::PublicationTargetMismatch);
        }
        match self.claim {
            FailurePublicationOrigin::SupervisorEffect { input, action } => {
                if !affiliation.same_scope(&self.scope) {
                    return Err(ActorTurnError::PublicationTargetMismatch);
                }
                ActorRuntime::<A>::validate_failure_work(
                    node,
                    inbox,
                    ledger,
                    input,
                    self.generation,
                    action,
                    self.work,
                )?;
            }
            FailurePublicationOrigin::StartedCleanup {
                financial_generation,
                header,
            } => {
                if !inbox.failed
                    || inbox.generation != self.generation
                    || self.generation <= node.scope.original_epoch
                {
                    return Err(EffectError::StaleGeneration.into());
                }
                let actual =
                    ActorRuntime::<A>::started_failure_cleanup(ledger, self.scope.id(), self.work)?;
                if actual != (financial_generation, header) {
                    return Err(ActorTurnError::FailureWorkInactive);
                }
            }
        }
        match inbox.failure_publication.as_ref() {
            Some(FailurePublicationRecord::Active {
                generation,
                origin,
                work,
            }) if *generation == self.generation && *origin == self.claim && *work == self.work => {
                Ok(())
            }
            Some(FailurePublicationRecord::Active { .. }) => {
                Err(EffectError::StaleGeneration.into())
            }
            Some(FailurePublicationRecord::Closed) | None => {
                Err(ActorTurnError::FailurePublicationRequired)
            }
        }
    }
    fn next_publication_action(inbox: &Inbox<A>) -> Result<ActionId, ActorTurnError> {
        inbox
            .failure_action_watermark
            .checked_add(1)
            .and_then(ActionId::new)
            .ok_or(ActorTurnError::IdentityExhausted)
    }
    /// Take the same old, pending cleanup obligation after the actual failure
    /// publication owner has replaced its normal predecessor ordering. This
    /// grants no business start, publication, Work or physical-exit fact.
    /// Refusal leaves both the action and its neutral payload in the batch.
    pub fn take_cleanup(
        &mut self,
        input: InputId,
        action: ActionId,
    ) -> Result<EffectTicket<A::Effect>, ActorTurnError> {
        let node = self.runtime.node(self.actor)?;
        let mut inbox = node.inbox.lock().expect("actor inbox lock");
        let execution_scope = node.scope.current_scope();
        let ledger = execution_scope
            .runtime
            .shared
            .state
            .lock()
            .expect("runtime ledger lock");
        self.validate(&node, &inbox, &ledger, &self.scope)?;
        if matches!(self.claim, FailurePublicationOrigin::SupervisorEffect { input: own, .. } if input == own)
        {
            return Err(EffectError::InvalidTransition.into());
        }
        let author_ref = inbox
            .batches
            .get(&input)
            .ok_or(ActorTurnError::UnknownInput)?
            .state
            .record(action)
            .ok_or(EffectError::UnknownAction)?
            .descriptor()
            .work;
        let author_generation = ledger
            .scopes
            .get(&author_ref.scope)
            .ok_or(RuntimeError::UnknownScope)?
            .generation();
        let batch = inbox
            .batches
            .get_mut(&input)
            .ok_or(ActorTurnError::UnknownInput)?;
        if matches!(
            self.claim,
            FailurePublicationOrigin::SupervisorEffect { .. }
        ) && batch.state.scope() != self.scope.id()
        {
            return Err(ActorTurnError::PublicationTargetMismatch);
        }
        batch
            .state
            .start_after_owner_failure(action, self.generation)?;
        self.runtime.shared.record(|| HistoryFact::Effect {
            target: node.history_target(),
            input,
            action,
            actor_generation: batch.state.generation(),
            financial_work: author_ref,
            author_generation,
            phase: novarocks_control_runtime_core::history::EffectHistoryPhase::Dispatched,
        });
        let effect = batch
            .payloads
            .remove(&action)
            .expect("pending cleanup owns its neutral payload");
        Ok(EffectTicket {
            view: EffectTicketView {
                effect,
                actor: self.actor,
                input,
                generation: batch.state.generation(),
                author_generation,
                descriptor: batch
                    .state
                    .record(action)
                    .expect("started original cleanup")
                    .descriptor()
                    .clone(),
            },
            origin: self.origin.clone(),
        })
    }
    pub fn commit_snapshot<T>(
        &mut self,
        target: &crate::publication::PublishedSnapshot<T>,
        next: Arc<T>,
    ) -> Result<Arc<T>, (ActorTurnError, Arc<T>)> {
        let next = ManuallyDrop::new(next);
        let node = match self.runtime.node(self.actor) {
            Ok(node) => node,
            Err(error) => return Err((error, ManuallyDrop::into_inner(next))),
        };
        let mut inbox = node.inbox.lock().expect("actor inbox lock");
        let execution_scope = node.scope.current_scope();
        let ledger = execution_scope
            .runtime
            .shared
            .state
            .lock()
            .expect("runtime ledger lock");
        let action = match self
            .validate(&node, &inbox, &ledger, &target.owner)
            .and_then(|()| {
                ActorRuntime::<A>::validate_target_binding(
                    &node,
                    &inbox,
                    &ledger,
                    None,
                    &target.owner,
                    &target.binding,
                    true,
                )
            })
            .and_then(|()| Self::next_publication_action(&inbox))
        {
            Ok(action) => action,
            Err(error) => return Err((error, ManuallyDrop::into_inner(next))),
        };
        let mut value = target.value.lock().expect("published snapshot lock");
        inbox.failure_action_watermark = action.get();
        self.runtime.shared.record(|| HistoryFact::FailurePublication {
            target: node.history_target(),
            actor_generation: self.generation,
            financial_work: self.work,
            financial_generation: match self.claim {
                FailurePublicationOrigin::StartedCleanup { financial_generation, .. } => financial_generation,
                FailurePublicationOrigin::SupervisorEffect { .. } => ledger.scopes
                    .get(&self.work.scope).expect("the validated original failure Work Scope").generation(),
            },
            action,
            origin: match self.claim {
                FailurePublicationOrigin::SupervisorEffect { input, action } =>
                    novarocks_control_runtime_core::history::FailurePublicationHistoryOrigin::SupervisorEffect {
                        input, action,
                    },
                FailurePublicationOrigin::StartedCleanup { header, .. } =>
                    novarocks_control_runtime_core::history::FailurePublicationHistoryOrigin::StartedCleanup {
                        header,
                    },
            },
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
    ) -> Result<(), (ActorTurnError, Arc<T>)> {
        let next = ManuallyDrop::new(next);
        let node = match self.runtime.node(self.actor) {
            Ok(node) => node,
            Err(error) => return Err((error, ManuallyDrop::into_inner(next))),
        };
        let mut inbox = node.inbox.lock().expect("actor inbox lock");
        let execution_scope = node.scope.current_scope();
        let ledger = execution_scope
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
        if let Err(error) = ActorRuntime::<A>::validate_target_binding(
            &node,
            &inbox,
            &ledger,
            None,
            &target.owner,
            &reply.binding,
            true,
        ) {
            return Err((error, ManuallyDrop::into_inner(next)));
        }
        if reply.phase != crate::publication::ReplyPhase::Empty || reply.value.is_some() {
            return Err((
                ActorTurnError::ReplyAlreadyCommitted,
                ManuallyDrop::into_inner(next),
            ));
        }
        inbox.failure_action_watermark = action.get();
        self.runtime.shared.record(|| HistoryFact::FailurePublication {
            target: node.history_target(),
            actor_generation: self.generation,
            financial_work: self.work,
            financial_generation: match self.claim {
                FailurePublicationOrigin::StartedCleanup { financial_generation, .. } => financial_generation,
                FailurePublicationOrigin::SupervisorEffect { .. } => ledger.scopes
                    .get(&self.work.scope).expect("the validated original failure Work Scope").generation(),
            },
            action,
            origin: match self.claim {
                FailurePublicationOrigin::SupervisorEffect { input, action } =>
                    novarocks_control_runtime_core::history::FailurePublicationHistoryOrigin::SupervisorEffect {
                        input, action,
                    },
                FailurePublicationOrigin::StartedCleanup { header, .. } =>
                    novarocks_control_runtime_core::history::FailurePublicationHistoryOrigin::StartedCleanup {
                        header,
                    },
            },
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
    pub standing_inputs: usize,
    pub batches: usize,
    pub last_progress: Tick,
}

impl<A: Actor> Default for ActorRuntime<A> {
    fn default() -> Self {
        Self::new()
    }
}

impl<A: Actor> ActorRuntime<A> {
    /// Compares the actual runtime instance only. This grants no work,
    /// capacity, input, or permission to pass a current admission gate.
    pub fn same_runtime(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.shared, &other.shared)
    }
    /// Read-only original scope identity, not admission, current generation,
    /// publication permission or evidence that any Work has exited.
    pub fn owns_scope(&self, actor: ActorId, scope: &ScopeHandle) -> Result<bool, ActorTurnError> {
        Ok(self.node(actor)?.scope.matches_live_scope(scope))
    }
    pub fn new() -> Self {
        Self::build(None, Default::default())
    }
    pub fn with_history(history: crate::history::HistoryCapture) -> Self {
        Self::build(Some(history), Default::default())
    }
    pub fn with_observation(
        history: Option<crate::history::HistoryCapture>,
        observation: crate::observation::RuntimeObservation,
    ) -> Self {
        Self::build(history, observation)
    }
    pub fn observation_snapshot(&self) -> Option<crate::observation::RuntimeObservationSnapshot> {
        self.shared.observation.snapshot()
    }
    fn build(
        history: Option<crate::history::HistoryCapture>,
        observation: crate::observation::RuntimeObservation,
    ) -> Self {
        let changed = Arc::new(Notify::default());
        let header_readiness = Arc::new(crate::header_readiness::HeaderReadyHub::new(
            changed.clone(),
        ));
        Self {
            shared: Arc::new(ActorRuntimeShared {
                state: Mutex::new(ActorRuntimeState {
                    actors: BTreeMap::new(),
                    ready: ReadyQueue::new(),
                    effect_ready: ReadyQueue::new(),
                    timers: TimerIndex::new(),
                    open: true,
                    now: Tick(0),
                }),
                capacity_notifier: std::sync::Arc::new(
                    crate::capacity_notification::CapacityWakeNotifier {
                        changed: changed.clone(),
                    },
                ),
                capacity_wakes: Mutex::new(Vec::new()),
                changed,
                header_readiness,
                history,
                observation,
            }),
        }
    }
    fn attach_capacity_ledger(&self, ledger: &crate::transaction::RuntimeLedger) {
        let queue = &ledger.shared.pending_capacity_wakes;
        let mut queues = self
            .shared
            .capacity_wakes
            .lock()
            .expect("actor capacity notification queues lock");
        if !queues.iter().any(|old| Arc::ptr_eq(old, queue)) {
            queue.attach(&self.shared.capacity_notifier);
            queues.push(queue.clone());
        }
    }
    /// Take queue snapshots without retaining any runtime lock across a Waker.
    /// A queue is shared by all attached runtimes and outlives removed Scopes.
    fn flush_capacity_notifications(&self) {
        let queues = self
            .shared
            .capacity_wakes
            .lock()
            .expect("actor capacity notification queues lock")
            .clone();
        for queue in queues {
            queue.flush();
        }
    }
    fn node(&self, id: ActorId) -> Result<Arc<ActorCell<A>>, ActorTurnError> {
        self.shared
            .state
            .lock()
            .expect("runtime scheduler lock")
            .actors
            .get(&id)
            .cloned()
            .ok_or(ActorTurnError::UnknownActor)
    }
    pub fn register(
        &self,
        id: ActorId,
        scope: ScopeHandle,
        attachment: SlotId,
        actor: A,
        config: ActorConfig,
    ) -> Result<(), (ActorTurnError, A)> {
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
    ) -> Result<SlotId, (ActorTurnError, A)> {
        self.register_bound(id, scope, actor, config)
            .map(|registration| registration.attachment)
    }
    /// Install an unsupervised actor and return its original admission witness
    /// atomically. Retaining this witness cannot bind an owner to a replacement.
    pub fn register_bound(
        &self,
        id: ActorId,
        scope: ScopeHandle,
        actor: A,
        config: ActorConfig,
    ) -> Result<ActorRegistration, (ActorTurnError, A)> {
        self.register_inner(
            id,
            scope,
            (None, None),
            actor,
            config,
            ActorFailurePolicy::IsolateScope,
        )
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
        failure_work: &crate::obligation::WorkObligation,
    ) -> Result<SlotId, (ActorTurnError, A)> {
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
        failure_work: &crate::obligation::WorkObligation,
        failure_policy: ActorFailurePolicy,
    ) -> Result<SlotId, (ActorTurnError, A)> {
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
        failure_work: &crate::obligation::WorkObligation,
    ) -> Result<ActorRegistration, (ActorTurnError, A)> {
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
        failure_work: &crate::obligation::WorkObligation,
        failure_policy: ActorFailurePolicy,
    ) -> Result<ActorRegistration, (ActorTurnError, A)> {
        self.register_supervised_inner(id, scope, actor, config, failure_work, failure_policy)
    }
    fn register_supervised_inner(
        &self,
        id: ActorId,
        scope: ScopeHandle,
        actor: A,
        config: ActorConfig,
        failure_work: &crate::obligation::WorkObligation,
        failure_policy: ActorFailurePolicy,
    ) -> Result<ActorRegistration, (ActorTurnError, A)> {
        if scope.id() != failure_work.scope().id()
            || !Arc::ptr_eq(&scope.runtime.shared, &failure_work.scope().runtime.shared)
        {
            return Err((ActorTurnError::PublicationTargetMismatch, actor));
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
            return Err((ActorTurnError::FailureWorkInactive, actor));
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
    ) -> Result<ActorRegistration, (ActorTurnError, A)> {
        let config = match config.validate() {
            Ok(c) => c,
            Err(_) => return Err((ActorTurnError::InvalidConfig, actor)),
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
            return Err((ActorTurnError::Closed, actor));
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
        self.attach_capacity_ledger(&scope.runtime);
        let registration = ActorRegistration {
            actor: id,
            attachment,
            scope: Arc::new(RegistrationScope {
                original: scope.id(),
                original_epoch: generation,
                execution: Mutex::new(ActorExecution::Live {
                    scope,
                    failure_work,
                }),
            }),
            origin: Arc::new(()),
        };
        state.actors.insert(
            id,
            Arc::new(ActorCell {
                origin: registration.origin.clone(),
                id,
                scope: registration.scope.clone(),
                attachment,
                config,
                failure_policy,
                core: Mutex::new(Some(actor)),
                inbox: Mutex::new(Inbox {
                    observation: self.shared.observation.clone(),
                    external_open: true,
                    sealed: false,
                    payloads: BTreeMap::new(),
                    mailbox: MailboxState::new(config.mailbox),
                    input_source: None,
                    prefer_actor_source: false,
                    inputs: BTreeMap::new(),
                    standing_inputs: BTreeMap::new(),
                    counts: [0; 4],
                    input_limits: config.mailbox,
                    metadata_credit: std::array::from_fn(|_| MetadataCreditQueue::new()),
                    metadata_closing: false,
                    next_input: u64::from(failure_policy == ActorFailurePolicy::IsolateScope),
                    batches: BTreeMap::new(),
                    batch_order: BTreeMap::new(),
                    next_batch: 0,
                    generation,
                    failed: false,
                    timer_input: None,
                    timer_closed: false,
                    failed_custodian_timer: None,
                    failure_position: (failure_policy == ActorFailurePolicy::IsolateScope)
                        .then(|| InputId::new(1).expect("fixed initial supervisor identity")),
                    failure_input: None,
                    failure: None,
                    last_progress: Tick(0),
                    failure_publication: None,
                    failure_action_watermark: 0,
                }),
            }),
        );
        Ok(registration)
    }
    /// A committed input owns every explicit financial author used by its
    /// effects, including a delayed R reply on a workless Timer turn. Pins
    /// retain headers only; ordinary effect authorization is unchanged.
    fn pin_batch_authors(
        node: &ActorCell<A>,
        inbox: &mut Inbox<A>,
        input: InputId,
        batch: &StepEnvelope<A::Effect>,
        ledger: &mut crate::transaction::LedgerState,
    ) -> Result<(), ActorTurnError> {
        let record = inbox
            .inputs
            .get(&input)
            .ok_or(ActorTurnError::UnknownInput)?;
        let affiliation = node.scope.current_scope();
        let mut added: Vec<crate::obligation::WorkHeaderObligation> = Vec::new();
        for author in batch.authors.values() {
            if author.actor != node.id
                || !Arc::ptr_eq(&author.origin, &node.origin)
                || !Arc::ptr_eq(&author.scope.runtime.shared, &affiliation.runtime.shared)
            {
                for pin in added {
                    pin.release_locked(ledger);
                }
                return Err(ActorTurnError::InputAuthorMismatch);
            }
            if record
                .header_pins
                .iter()
                .chain(added.iter())
                .any(|pin| pin.reference() == author.work && pin.belongs_to(&author.scope))
            {
                continue;
            }
            match crate::obligation::WorkHeaderObligation::for_input_author_locked(author, ledger) {
                Ok(pin) => added.push(pin),
                Err(error) => {
                    for pin in added {
                        pin.release_locked(ledger);
                    }
                    return Err(error.into());
                }
            }
        }
        // New business effects can name an original unbound reservation
        // instead of the incoming producer. The guard itself proves that
        // header; its batch keeps it even after the guard moves and binds.
        for guard in batch.reservations.values() {
            let reference = ScopedWorkRef {
                scope: guard.scope().id(),
                work: guard.work_id(),
            };
            if record
                .header_pins
                .iter()
                .chain(added.iter())
                .any(|pin| pin.reference() == reference && pin.belongs_to(guard.scope()))
            {
                continue;
            }
            if !Arc::ptr_eq(&guard.scope().runtime.shared, &affiliation.runtime.shared) {
                for pin in added {
                    pin.release_locked(ledger);
                }
                return Err(ActorTurnError::ReservationOwnerMismatch);
            }
            match crate::obligation::WorkHeaderObligation::for_guard_locked(guard, ledger) {
                Ok(pin) => added.push(pin),
                Err(error) => {
                    for pin in added {
                        pin.release_locked(ledger);
                    }
                    return Err(error.into());
                }
            }
        }
        inbox
            .inputs
            .get_mut(&input)
            .expect("same committing original input")
            .header_pins
            .extend(added);
        Ok(())
    }
    /// Existing permits may still fill after external admission closes.
    pub fn reserve_input(
        &self,
        actor: ActorId,
        source: ActorId,
        class: InputClass,
    ) -> Result<ReservedInputObligation<A>, ActorTurnError> {
        self.reserve_input_position(actor, source, class, None, None)
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
        author: &crate::obligation::WorkObligation,
    ) -> Result<ReservedInputObligation<A>, ActorTurnError> {
        self.reserve_input_position(actor, source, class, Some(author), None)
    }
    /// Cross-owner input admission requires the actual destination registration,
    /// not a numeric actor address which a later incarnation could reuse.
    pub fn reserve_admitted_input_for(
        &self,
        registration: &ActorRegistration,
        source: ActorId,
        class: InputClass,
        author: &crate::obligation::WorkObligation,
    ) -> Result<ReservedInputObligation<A>, ActorTurnError> {
        self.reserve_input_position(
            registration.actor,
            source,
            class,
            Some(author),
            Some(registration),
        )
    }
    /// A retained request consumes its actual current process-scope Work.
    /// No identity alone grants admission and no old execution Work is reused.
    pub fn reserve_retained_input(
        &self,
        registration: &ActorRegistration,
        source: ActorId,
        class: InputClass,
        author: &crate::obligation::WorkObligation,
    ) -> Result<ReservedInputObligation<A>, ActorTurnError> {
        if !self.node(registration.actor)?.scope.retained() {
            return Err(ActorTurnError::RetainedInputRequired);
        }
        self.reserve_input_position(
            registration.actor,
            source,
            class,
            Some(author),
            Some(registration),
        )
    }
    fn reserve_input_position(
        &self,
        actor: ActorId,
        source: ActorId,
        class: InputClass,
        author: Option<&crate::obligation::WorkObligation>,
        retained_registration: Option<&ActorRegistration>,
    ) -> Result<ReservedInputObligation<A>, ActorTurnError> {
        if class == InputClass::Timer {
            return Err(ActorTurnError::InvalidConfig);
        }
        let node = self.node(actor)?;
        let mut inbox = node.inbox.lock().expect("actor inbox lock");
        let execution_scope = node.scope.current_scope();
        if let Some(registration) = retained_registration {
            if registration.actor != actor
                || !Arc::ptr_eq(&registration.origin, &node.origin)
                || !Arc::ptr_eq(&registration.scope, &node.scope)
            {
                return Err(ActorTurnError::EffectTicketOriginMismatch);
            }
        } else if node.scope.retained() {
            return Err(ActorTurnError::RetainedInputRequired);
        }
        if author.is_some_and(|work| work.scope().id() != execution_scope.id())
            && retained_registration.is_none()
        {
            return Err(ActorTurnError::InputAuthorMismatch);
        }
        if node.scope.retained() && author.is_none() {
            return Err(ActorTurnError::RetainedInputRequired);
        }
        // Keep the actual author check and position insertion in one order
        // with scope failure. Lock order is the existing inbox -> ledger order;
        // no callback, payload construction or Work admission runs here.
        let mut author_state = if let Some(author) = author {
            if !Arc::ptr_eq(
                &author.scope().runtime.shared,
                &execution_scope.runtime.shared,
            ) {
                return Err(ActorTurnError::InputAuthorMismatch);
            }
            let state = execution_scope
                .runtime
                .shared
                .state
                .lock()
                .expect("runtime ledger lock");
            if !node.scope.retained()
                && state
                    .scopes
                    .get(&execution_scope.id())
                    .ok_or(RuntimeError::UnknownScope)?
                    .failed()
            {
                return Err(ActorTurnError::ActorFailed);
            }
            let scope = state
                .scopes
                .get(&author.scope().id())
                .ok_or(ActorTurnError::InputAuthorInactive)?;
            if scope.failed() {
                return Err(ActorTurnError::ActorFailed);
            }
            let work = scope
                .work(author.id())
                .ok_or(ActorTurnError::InputAuthorInactive)?;
            if work.unbound_guard
                || work.body_exited
                || work.captures_exited
                || state
                    .unresolved
                    .contains(&(author.scope().id(), author.id()))
            {
                return Err(ActorTurnError::InputAuthorInactive);
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
                return Err(ActorTurnError::Closed);
            }
            None
        };
        if inbox.failed {
            return Err(ActorTurnError::ActorFailed);
        }
        if inbox.sealed || author.is_none() && !inbox.external_open {
            return Err(ActorTurnError::Closed);
        }
        if inbox.available_input_credit(class) == 0 {
            return Err(ActorTurnError::InputCapacity);
        }
        inbox.next_input = inbox
            .next_input
            .checked_add(1)
            .ok_or(ActorTurnError::IdentityExhausted)?;
        let id = InputId::new(inbox.next_input).expect("incremented input id");
        let binding = author.map(|work| InputAuthor {
            actor,
            origin: node.origin.clone(),
            input: id,
            actor_generation: inbox.generation,
            scope: work.scope().clone(),
            work: work.reference(),
            generation: author_state
                .as_ref()
                .expect("qualified original author ledger")
                .scopes
                .get(&work.scope().id())
                .expect("qualified original author scope")
                .generation(),
        });
        let header_pins = match author {
            Some(author) => vec![crate::obligation::WorkHeaderObligation::for_work_locked(
                author,
                author_state
                    .as_mut()
                    .expect("actual original author ledger"),
            )?],
            None => Vec::new(),
        };
        inbox.inputs.insert(
            id,
            InputRecord {
                source,
                class,
                phase: InputPhase::Reserved,
                standing_input_id: None,
                author: binding.clone(),
                producer: binding,
                metadata_delivery: None,
                header_pins,
                handoff_at: None,
            },
        );
        inbox.counts[class.index()] += 1;
        inbox.observation.input_credit_taken(class);
        drop(author_state);
        Ok(ReservedInputObligation {
            runtime: self.clone(),
            actor,
            origin: node.origin.clone(),
            input: id,
        })
    }
    pub fn snapshot(&self, actor: ActorId) -> Result<ActorSnapshot, ActorTurnError> {
        let node = self.node(actor)?;
        let inbox = node.inbox.lock().expect("actor inbox lock");
        Ok(ActorSnapshot {
            generation: inbox.generation,
            failed: inbox.failed,
            external_open: inbox.external_open,
            sealed: inbox.sealed,
            inputs: inbox.inputs.len(),
            standing_inputs: inbox.standing_inputs.len(),
            batches: inbox.batches.len(),
            last_progress: inbox.last_progress,
        })
    }
    /// Stop issuing new positions for one actor. Existing permits and prepaid
    /// roles still drain, including after process-wide external admission closes.
    /// This is not a stop request, failure, work exit or retirement proof.
    pub fn close_actor_admission(&self, actor: ActorId) -> Result<(), ActorTurnError> {
        let node = self.node(actor)?;
        node.inbox.lock().expect("actor inbox lock").external_open = false;
        self.shared.changed.notify();
        Ok(())
    }
    /// Monotonically withdraw future builtin timer production for this actor.
    /// Existing filled Tick custody remains until actual `retire_input`.
    /// Persistent prepaid input roles and ordinary input drain are unaffected.
    pub fn close_timer_source(&self, actor: ActorId) -> Result<TimerSourceState, ActorTurnError> {
        let node = self.node(actor)?;
        let mut inbox = node.inbox.lock().expect("actor inbox lock");
        inbox.timer_closed = true;
        // This withdraws only neutral host-deadline delivery. An original
        // filled Core Tick still owns its actual input until retire_input.
        inbox.failed_custodian_timer = None;
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
    pub fn timer_source_state(&self, actor: ActorId) -> Result<TimerSourceState, ActorTurnError> {
        let node = self.node(actor)?;
        let inbox = node.inbox.lock().expect("actor inbox lock");
        Ok(Self::timer_state(&inbox))
    }
    /// Observe the unique builtin Tick's original input address. This grants
    /// no retirement authority; its real batch still gates retire_input.
    pub fn pending_timer_input(&self, actor: ActorId) -> Result<Option<InputId>, ActorTurnError> {
        let node = self.node(actor)?;
        let inbox = node.inbox.lock().expect("actor inbox lock");
        Ok(inbox.timer_input)
    }

    pub fn failure_source_state(
        &self,
        actor: ActorId,
    ) -> Result<FailureSourceState, ActorTurnError> {
        let node = self.node(actor)?;
        let inbox = node.inbox.lock().expect("actor inbox lock");
        Ok(if let Some(input) = inbox.failure_input {
            FailureSourceState::ClosedPending(input)
        } else if inbox.failure_position.is_some() {
            FailureSourceState::Open
        } else {
            FailureSourceState::ClosedDrained
        })
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
    pub fn seal_actor(&self, actor: ActorId) -> Result<(), ActorTurnError> {
        let node = self.node(actor)?;
        let mut inbox = node.inbox.lock().expect("actor inbox lock");
        if inbox.external_open
            || !inbox.inputs.is_empty()
            || !inbox.standing_inputs.is_empty()
            || !inbox.batches.is_empty()
            || inbox.failure.is_some()
        {
            return Err(ActorTurnError::ActorNotRetired);
        }
        if self.close_metadata_waiters_locked(&node, &mut inbox) {
            return Err(ActorTurnError::ActorNotRetired);
        }
        let mut state = self.shared.state.lock().expect("runtime scheduler lock");
        if !state.ready.is_idle(actor)? || !state.effect_ready.is_idle(actor)? {
            return Err(SchedulerError::Busy.into());
        }
        inbox.sealed = true;
        inbox.timer_closed = true;
        inbox.failed_custodian_timer = None;
        // All actual inputs and scheduler claims are empty under these same
        // locks. Withdrawing the unused builtin cannot race a later Core turn.
        inbox.failure_position = None;
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
        self.flush_capacity_notifications();
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
    pub fn advance_time(&self, observed: Tick, budget: usize) -> Result<usize, ActorTurnError> {
        self.advance_time_sampled(observed, budget, &mut None)
    }
    fn advance_time_sampled(
        &self,
        observed: Tick,
        budget: usize,
        sample: &mut Option<poller_observation::TimerScan>,
    ) -> Result<usize, ActorTurnError> {
        let handoff_observed = self.shared.observation.now();
        let (actors, now) = {
            let mut state = self.shared.state.lock().expect("runtime scheduler lock");
            let now = observed.max(state.now);
            state.now = now;
            *sample = Some(poller_observation::TimerScan {
                observed_at: now,
                earliest_indexed_deadline: state.timers.next_deadline(),
                pending_ticks: state.timers.pending_count(),
                registered_deadlines: state.timers.deadline_count(),
            });
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
            {
                let mut inbox = node.inbox.lock().expect("actor timer delivery lock");
                if inbox.failed {
                    filled +=
                        usize::from(self.deliver_failed_custodian_timer(&node, &mut inbox, now)?);
                    continue;
                }
            }
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
                return Err(ActorTurnError::InputCapacity);
            }
            inbox.next_input = inbox
                .next_input
                .checked_add(1)
                .ok_or(ActorTurnError::IdentityExhausted)?;
            let input = InputId::new(inbox.next_input).expect("incremented timer input id");
            inbox.mailbox.push(InputClass::Timer, actor, input)?;
            inbox.inputs.insert(
                input,
                InputRecord {
                    source: actor,
                    class: InputClass::Timer,
                    phase: InputPhase::Filled,
                    standing_input_id: None,
                    author: None,
                    producer: None,
                    metadata_delivery: None,
                    header_pins: Vec::new(),
                    handoff_at: handoff_observed,
                },
            );
            inbox.payloads.insert(input, timer_value);
            inbox.counts[InputClass::Timer.index()] += 1;
            inbox.observation.input_credit_taken(InputClass::Timer);
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
    pub fn poll_one(&self, observed: Tick) -> Result<bool, ActorTurnError> {
        self.poll_one_deferred(observed, None)
    }
    /// Production input steps observe time independently. Clock sampling occurs
    /// after removing the input and outside inbox, ledger, scheduler and core
    /// locks; only the clamped Tick value enters the pure actor.
    pub fn poll_one_with_clock(&self, clock: &dyn Clock) -> Result<bool, ActorTurnError> {
        self.poll_one_deferred(clock.now(), Some(clock))
    }
    fn poll_one_deferred(
        &self,
        observed: Tick,
        clock: Option<&dyn Clock>,
    ) -> Result<bool, ActorTurnError> {
        self.flush_capacity_notifications();
        let result = {
            let _deferral = crate::capacity_notification::CapacityWakeDeferral::enter();
            self.poll_one_inner(observed, clock)
        };
        self.flush_capacity_notifications();
        result
    }
    fn poll_one_inner(
        &self,
        observed: Tick,
        clock: Option<&dyn Clock>,
    ) -> Result<bool, ActorTurnError> {
        let node = {
            let mut state = self.shared.state.lock().expect("runtime scheduler lock");
            state.now = observed.max(state.now);
            if let Some(history) = &self.shared.history {
                history.observe_time(state.now);
            }
            let Some(actor) = state.ready.claim() else {
                return Ok(false);
            };
            let node = state
                .actors
                .get(&actor)
                .cloned()
                .expect("registered ready actor");
            self.shared.record(|| HistoryFact::Claimed {
                target: node.history_target(),
            });
            node
        };
        let execution_scope = node.scope.current_scope();
        self.attach_capacity_ledger(&execution_scope.runtime);
        let mut budget = TurnBudget::new(node.config.turn_budget)?;
        loop {
            let next = {
                let mut inbox = node.inbox.lock().expect("actor inbox lock");
                if inbox.failed || inbox.sealed || inbox.batches.len() >= node.config.max_batches {
                    None
                } else {
                    let ready = inbox.ready_classes(node.id);
                    let mut candidate = budget.clone();
                    let class = candidate.next(ready);
                    class
                        .filter(|_| candidate.charge(node.config.max_step_work))
                        .and_then(|class| {
                            budget = candidate;
                            inbox.pop_selected(node.id, class)
                        })
                        .map(|input| {
                            let original =
                                inbox.inputs.get_mut(&input.input).expect("filled input");
                            original.phase = InputPhase::Consuming;
                            let handoff_at = original.handoff_at;
                            let value = inbox
                                .payloads
                                .remove(&input.input)
                                .expect("filled input owns its neutral payload");
                            (input, value, handoff_at)
                        })
                }
            };
            let Some((input, value, handoff_at)) = next else {
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
            let mut step = ActorStep::new(
                &mut budget,
                &execution_scope,
                generation,
                input.input,
                node.config.max_effects_per_step,
                node.config.max_dependencies_per_effect,
                node.config.max_step_work,
            );
            step.set_failure_work(node.scope.failure_work());
            let input_author = node
                .inbox
                .lock()
                .expect("actor inbox lock")
                .inputs
                .get(&input.input)
                .expect("consuming original input")
                .author
                .clone();
            step.set_input_author(input_author);
            let runtime_metadata = node
                .inbox
                .lock()
                .expect("actor inbox lock")
                .inputs
                .get(&input.input)
                .expect("actual consumed input")
                .metadata_delivery
                .is_some();
            step.set_runtime_metadata(runtime_metadata);
            if node.scope.retained() {
                step.retained();
            }
            let began = self.shared.observation.now();
            let result = {
                let mut core = node.core.lock().expect("actor core lock");
                let core = core.as_mut().expect("registered pure actor");
                catch_unwind(AssertUnwindSafe(|| {
                    core.handle(now, input, value, &mut step);
                    core.next_deadline()
                }))
            };
            let ended = self.shared.observation.now();
            self.shared.observation.step(
                input.class,
                handoff_at,
                began,
                ended,
                node.config.max_step_work - step.remaining(),
                result.is_err(),
            );
            let deadline = match result {
                Ok(deadline) => {
                    let batch = step.finish();
                    let stop_producing = batch.stop_producing;
                    let input_source = batch.input_source;
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
                    // Header custody and atomic commitment share the original
                    // short ledger gate; no callback or rich Drop runs here.
                    let mut ledger = execution_scope
                        .runtime
                        .shared
                        .state
                        .lock()
                        .expect("runtime ledger lock");
                    if Self::pin_batch_authors(&node, &mut inbox, input.input, &batch, &mut ledger)
                        .is_err()
                    {
                        // A trusted commitment cannot discard E or an original
                        // guard on this poller. Preserve this entire refused
                        // batch and fail-stop before unwinding any captures.
                        std::mem::forget(batch);
                        std::process::abort();
                    }
                    inbox
                        .commit_batch(input.input, batch)
                        .expect("preflighted atomic batch commitment");
                    if let Some(source) = input_source
                        && inbox.input_source != source
                    {
                        inbox.input_source = source;
                        inbox.prefer_actor_source = false;
                    }
                    inbox
                        .inputs
                        .get_mut(&input.input)
                        .expect("consumed input")
                        .phase = InputPhase::Committed;
                    self.resolve_metadata_delivery(
                        &inbox,
                        input.input,
                        MetadataConsumption::Consumed,
                    );
                    self.retire_metadata_input_and_wake(
                        &node,
                        &mut inbox,
                        input.input,
                        &mut ledger,
                    );
                    if stop_producing {
                        ledger
                            .scopes
                            .get_mut(&execution_scope.id())
                            .expect("registered actor retains its original scope")
                            .request_stop();
                        execution_scope
                            .runtime
                            .shared
                            .record(|| HistoryFact::ScopeStopped {
                                scope: execution_scope.id(),
                            });
                    }
                    inbox.last_progress = now;
                    self.shared.record(|| HistoryFact::InputConsumed {
                        target: node.history_target(),
                        input: input.input,
                    });
                    self.shared.record(|| HistoryFact::BatchCommitted {
                        target: node.history_target(),
                        actor_generation: generation,
                        input: input.input,
                    });
                    drop(ledger);
                    drop(inbox);
                    if stop_producing {
                        execution_scope.runtime.shared.changed.notify();
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
                    if node.scope.retained() {
                        inbox.generation = inbox
                            .generation
                            .next()
                            .ok_or(ActorTurnError::IdentityExhausted)?;
                        inbox.failed = true;
                        self.close_metadata_waiters_locked(&node, &mut inbox);
                        inbox.failure = Some(ManuallyDrop::new(payload));
                        inbox
                            .inputs
                            .get_mut(&input.input)
                            .expect("failed original retained input")
                            .phase = InputPhase::AbandonedByOwnerFailure;
                        self.resolve_metadata_delivery(
                            &inbox,
                            input.input,
                            MetadataConsumption::AbandonedByOwnerFailure,
                        );
                        inbox.last_progress = now;
                        self.shared.record(|| HistoryFact::InputAbandoned {
                            target: node.history_target(),
                            input: input.input,
                        });
                        let mut state = self.shared.state.lock().expect("runtime scheduler lock");
                        state.timers.cancel(node.id);
                        state.effect_ready.notify(node.id)?;
                        drop(state);
                        drop(inbox);
                        drop(step);
                        break;
                    }
                    let generation = execution_scope.fail()?;
                    inbox.generation = generation;
                    inbox.failed = true;
                    self.close_metadata_waiters_locked(&node, &mut inbox);
                    inbox.failure = Some(ManuallyDrop::new(payload));
                    let supervisor_input = inbox
                        .failure_position
                        .take()
                        .expect("isolated actor owns its prepaid supervisor position");
                    assert!(inbox.failure_input.is_none());
                    inbox.failure_input = Some(supervisor_input);
                    inbox.inputs.insert(
                        supervisor_input,
                        InputRecord {
                            source: node.id,
                            class: InputClass::Lifecycle,
                            phase: InputPhase::Committed,
                            standing_input_id: None,
                            author: None,
                            producer: None,
                            metadata_delivery: None,
                            header_pins: Vec::new(),
                            handoff_at: None,
                        },
                    );
                    drop(inbox);
                    drop(step);
                    let mut failure_budget = TurnBudget::new(node.config.turn_budget)?;
                    let _ = failure_budget.next([true, false, false, false]);
                    assert!(
                        failure_budget.charge(node.config.max_step_work),
                        "failure projection has its reserved finite step budget"
                    );
                    let mut failure_step = ActorStep::new(
                        &mut failure_budget,
                        &execution_scope,
                        generation,
                        supervisor_input,
                        node.config.max_effects_per_step,
                        node.config.max_dependencies_per_effect,
                        node.config.max_step_work,
                    );
                    failure_step.set_failure_work(node.scope.failure_work());
                    // A failure projection panic is a shared runtime invariant failure.
                    let began = self.shared.observation.now();
                    A::owner_failed(execution_scope.id(), generation, input, &mut failure_step);
                    let ended = self.shared.observation.now();
                    self.shared.observation.step(
                        InputClass::Lifecycle,
                        None,
                        began,
                        ended,
                        node.config.max_step_work - failure_step.remaining(),
                        false,
                    );
                    let mut inbox = node.inbox.lock().expect("actor inbox lock");
                    let batch = failure_step.finish();
                    inbox.failure_action_watermark =
                        batch.state.last_action_id().map_or(0, ActionId::get);
                    {
                        let mut ledger = execution_scope
                            .runtime
                            .shared
                            .state
                            .lock()
                            .expect("failed original batch header custody");
                        if Self::pin_batch_authors(
                            &node,
                            &mut inbox,
                            supervisor_input,
                            &batch,
                            &mut ledger,
                        )
                        .is_err()
                        {
                            std::mem::forget(batch);
                            std::process::abort();
                        }
                    }
                    inbox.commit_batch(supervisor_input, batch)?;
                    inbox
                        .inputs
                        .get_mut(&input.input)
                        .expect("failed input custody")
                        .phase = InputPhase::AbandonedByOwnerFailure;
                    self.resolve_metadata_delivery(
                        &inbox,
                        input.input,
                        MetadataConsumption::AbandonedByOwnerFailure,
                    );
                    self.shared.record(|| HistoryFact::InputAbandoned {
                        target: node.history_target(),
                        input: input.input,
                    });
                    inbox.last_progress = now;
                    self.shared.record(|| HistoryFact::BatchCommitted {
                        target: node.history_target(),
                        actor_generation: generation,
                        input: supervisor_input,
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
                && inbox.has_selectable_input(node.id)
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
    ) -> Result<Vec<ActionId>, ActorTurnError> {
        let node = self.node(actor)?;
        let mut inbox = node.inbox.lock().expect("actor inbox lock");
        let generation = inbox.generation;
        Ok(inbox
            .batches
            .get_mut(&input)
            .ok_or(ActorTurnError::UnknownInput)?
            .state
            .poll_ready(budget, generation)?)
    }
    /// Nonblocking observation of original committed batches. The caller's
    /// original registration is qualified under the scheduler gate; the inbox
    /// snapshot includes lane tails and batches after the supplied real cursor.
    /// No ready/dispatch cursor is advanced and no payload is cloned.
    pub fn try_effect_inventory_observation(
        &self,
        registration: &ActorRegistration,
        indexed_through: Option<BatchId>,
    ) -> Result<
        Option<crate::observation::EffectInventoryObservation>,
        crate::observation::EffectObservationError,
    > {
        use crate::observation::{EffectInventoryObservation, EffectObservationError};
        let node = {
            let state = match self.shared.state.try_lock() {
                Ok(state) => state,
                Err(std::sync::TryLockError::WouldBlock) => return Ok(None),
                Err(std::sync::TryLockError::Poisoned(_)) => {
                    return Err(EffectObservationError::Poisoned);
                }
            };
            Self::validate_registration_in_state(&state, registration)
                .map_err(EffectObservationError::Runtime)?;
            state
                .actors
                .get(&registration.actor())
                .expect("qualified original actor registration")
                .clone()
        };
        let inbox = match node.inbox.try_lock() {
            Ok(inbox) => inbox,
            Err(std::sync::TryLockError::WouldBlock) => return Ok(None),
            Err(std::sync::TryLockError::Poisoned(_)) => {
                return Err(EffectObservationError::Poisoned);
            }
        };
        let mut sample = EffectInventoryObservation::default();
        for (ordinal, input) in &inbox.batch_order {
            let states = inbox
                .batches
                .get(input)
                .expect("original committed batch order index")
                .state
                .observed_states();
            sample.states = sample
                .states
                .checked_add(states)
                .ok_or(EffectObservationError::InventoryOverflow)?;
            sample.batches = sample
                .batches
                .checked_add(1)
                .ok_or(EffectObservationError::InventoryOverflow)?;
            if indexed_through.is_none_or(|cursor| *ordinal > cursor) {
                sample.unindexed_states = sample
                    .unindexed_states
                    .checked_add(states)
                    .ok_or(EffectObservationError::InventoryOverflow)?;
                sample.unindexed_batches = sample
                    .unindexed_batches
                    .checked_add(1)
                    .ok_or(EffectObservationError::InventoryOverflow)?;
            }
        }
        Ok(Some(sample))
    }
    /// Actual bounded dependency metadata, also available to failed-owner
    /// cleanup. Payload custody stays exclusively in the original batch.
    pub fn effect_inventory(
        &self,
        actor: ActorId,
        input: InputId,
        after: Option<ActionId>,
        budget: usize,
    ) -> Result<Vec<(EffectDescriptor, EffectState)>, ActorTurnError> {
        let node = self.node(actor)?;
        let inbox = node.inbox.lock().expect("actor inbox lock");
        Ok(inbox
            .batches
            .get(&input)
            .ok_or(ActorTurnError::UnknownInput)?
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
    ) -> Result<Option<A::Effect>, ActorTurnError>
    where
        A::Effect: Clone,
    {
        let node = self.node(actor)?;
        let inbox = node.inbox.lock().expect("actor inbox lock");
        let batch = inbox
            .batches
            .get(&input)
            .ok_or(ActorTurnError::UnknownInput)?;
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
    ) -> Result<EffectTicket<A::Effect>, ActorTurnError> {
        let node = self.node(actor)?;
        let mut inbox = node.inbox.lock().expect("actor inbox lock");
        let execution_scope = node.scope.current_scope();
        let ledger = execution_scope
            .runtime
            .shared
            .state
            .lock()
            .expect("runtime ledger lock");
        let descriptor = inbox
            .batches
            .get(&input)
            .ok_or(ActorTurnError::UnknownInput)?
            .state
            .record(action)
            .ok_or(EffectError::UnknownAction)?
            .descriptor()
            .clone();
        let author_generation =
            Self::action_author_generation(&node, &inbox, &ledger, input, &descriptor)?;
        let generation = inbox.generation;
        if !node.scope.retained()
            && ledger
                .scopes
                .get(&execution_scope.id())
                .ok_or(RuntimeError::UnknownScope)?
                .failed()
            && inbox.failure_input != Some(input)
        {
            return Err(ActorTurnError::ActorFailed);
        }
        let stopped = (!node.scope.retained()
            && ledger
                .scopes
                .get(&execution_scope.id())
                .ok_or(RuntimeError::UnknownScope)?
                .stopped())
            || ledger
                .scopes
                .get(&descriptor.work.scope)
                .ok_or(RuntimeError::UnknownScope)?
                .stopped();
        if inbox.failed && inbox.failure_input != Some(input) {
            return Err(ActorTurnError::ActorFailed);
        }
        let batch = inbox
            .batches
            .get_mut(&input)
            .ok_or(ActorTurnError::UnknownInput)?;
        if stopped
            && batch.state.record(action).is_some_and(|effect| {
                effect.descriptor().kind
                    == novarocks_control_runtime_core::effect::EffectKind::Business
            })
        {
            return Err(EffectError::NotReady.into());
        }
        batch.state.start(action, generation)?;
        self.shared.record(|| HistoryFact::Effect {
            target: node.history_target(),
            input,
            action,
            actor_generation: generation,
            financial_work: descriptor.work,
            author_generation,
            phase: novarocks_control_runtime_core::history::EffectHistoryPhase::Dispatched,
        });
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
                author_generation,
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
    ) -> Result<(), ActorTurnError> {
        let node = self.node(ticket.actor)?;
        let inbox = node.inbox.lock().expect("actor inbox lock");
        let execution_scope = node.scope.current_scope();
        let ledger = execution_scope
            .runtime
            .shared
            .state
            .lock()
            .expect("runtime ledger lock");
        self.validate_publication(&node, &inbox, &ledger, ticket, &execution_scope)
    }
    /// Atomically qualifies the original effect's current publication authority
    /// and fills an already paid role in this same runtime and ledger. No Work,
    /// capacity or target admission is created; the ticket remains with its caller.
    /// Source failure and target fill share the original ledger transaction.
    pub fn fill_from_effect(
        &self,
        ticket: &EffectTicket<A::Effect>,
        target: &StandingInputObligation<A>,
        value: A::Input,
    ) -> Result<InputId, (ActorTurnError, A::Input)> {
        self.fill_from_effect_inner(ticket, target, None, value)
    }
    /// Forward the same source effect while retaining the original request's
    /// financial author independently of the destination role's producer.
    pub fn fill_from_effect_with_author(
        &self,
        ticket: &EffectTicket<A::Effect>,
        target: &StandingInputObligation<A>,
        author: &InputAuthor,
        value: A::Input,
    ) -> Result<InputId, (ActorTurnError, A::Input)> {
        self.fill_from_effect_inner(ticket, target, Some(author), value)
    }
    fn fill_from_effect_inner(
        &self,
        ticket: &EffectTicket<A::Effect>,
        target: &StandingInputObligation<A>,
        author: Option<&InputAuthor>,
        value: A::Input,
    ) -> Result<InputId, (ActorTurnError, A::Input)> {
        let observed = self.shared.observation.now();
        if !self.same_runtime(&target.runtime) {
            return Err((ActorTurnError::PublicationTargetMismatch, value));
        }
        if !target.active {
            return Err((ActorTurnError::UnknownStandingInput, value));
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
        if !Arc::ptr_eq(&target.origin, &target_node.origin) {
            return Err((ActorTurnError::EffectTicketOriginMismatch, value));
        }
        let source_scope = source_node.scope.current_scope();
        let target_scope = target_node.scope.current_scope();
        if !Arc::ptr_eq(&source_scope.runtime.shared, &target_scope.runtime.shared) {
            return Err((ActorTurnError::PublicationTargetMismatch, value));
        }
        // Every two-inbox operation uses ActorId order. The same-actor case
        // borrows its one inbox twice immutably only for validation.
        let result = if ticket.actor == target.actor {
            let mut inbox = source_node.inbox.lock().expect("actor inbox lock");
            let mut ledger = source_scope
                .runtime
                .shared
                .state
                .lock()
                .expect("runtime ledger lock");
            match self.validate_effect_forwarding(
                ticket,
                (&source_node, &inbox),
                (&target_node, &inbox, target.standing_input_id),
                &ledger,
            ) {
                Ok(failed) => {
                    if let Some(author) = author
                        && let Err(error) = Self::validate_input_author(
                            &target_node,
                            &inbox,
                            &ledger,
                            author,
                            false,
                            true,
                        )
                    {
                        return Err((error, value));
                    }
                    inbox.fill_standing_input(
                        target.standing_input_id,
                        failed,
                        author,
                        &mut ledger,
                        observed,
                        value,
                    )
                }
                Err(error) => Err((error, value)),
            }
        } else if ticket.actor < target.actor {
            let source = source_node.inbox.lock().expect("source actor inbox lock");
            let mut destination = target_node.inbox.lock().expect("target actor inbox lock");
            self.fill_effect_target_locked(
                ticket,
                (&source_node, &source),
                (&target_node, &mut destination, target.standing_input_id),
                author,
                observed,
                value,
            )
        } else {
            let mut destination = target_node.inbox.lock().expect("target actor inbox lock");
            let source = source_node.inbox.lock().expect("source actor inbox lock");
            self.fill_effect_target_locked(
                ticket,
                (&source_node, &source),
                (&target_node, &mut destination, target.standing_input_id),
                author,
                observed,
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
        target: (&ActorCell<A>, &mut Inbox<A>, StandingInputId),
        author: Option<&InputAuthor>,
        observed: Option<Tick>,
        value: A::Input,
    ) -> Result<InputId, (ActorTurnError, A::Input)> {
        let source_scope = source.0.scope.current_scope();
        let mut ledger = source_scope
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
            Ok(failed) => {
                if let Some(author) = author
                    && let Err(error) = Self::validate_input_author(
                        target.0, target.1, &ledger, author, false, true,
                    )
                {
                    return Err((error, value));
                }
                target
                    .1
                    .fill_standing_input(target.2, failed, author, &mut ledger, observed, value)
            }
            Err(error) => Err((error, value)),
        }
    }
    fn validate_effect_forwarding(
        &self,
        ticket: &EffectTicket<A::Effect>,
        source: (&ActorCell<A>, &Inbox<A>),
        target: (&ActorCell<A>, &Inbox<A>, StandingInputId),
        ledger: &crate::transaction::LedgerState,
    ) -> Result<bool, ActorTurnError> {
        self.validate_publication(
            source.0,
            source.1,
            ledger,
            ticket,
            &source.0.scope.current_scope(),
        )?;
        let standing_input_record = target
            .1
            .standing_inputs
            .get(&target.2)
            .ok_or(ActorTurnError::UnknownStandingInput)?;
        if standing_input_record.source != ticket.actor {
            return Err(ActorTurnError::InputAuthorMismatch);
        }
        Ok(ledger
            .scopes
            .get(&target.0.scope.current_scope().id())
            .ok_or(RuntimeError::UnknownScope)?
            .failed())
    }
    /// Start the exact original host lease while holding both actor and author gates.
    /// No callback or foreign destructor runs in this inbox -> ledger transaction.
    pub fn start_effect_work(
        &self,
        ticket: &EffectTicket<A::Effect>,
        work: &mut crate::obligation::WorkObligation,
    ) -> Result<(), ActorTurnError> {
        let node = self.node(ticket.actor)?;
        let inbox = node.inbox.lock().expect("actor inbox lock");
        let affiliation = node.scope.current_scope();
        if work.reference() != ticket.descriptor.work
            || !Arc::ptr_eq(&work.scope().runtime.shared, &affiliation.runtime.shared)
        {
            return Err(ActorTurnError::InputAuthorMismatch);
        }
        let mut ledger = affiliation
            .runtime
            .shared
            .state
            .lock()
            .expect("runtime ledger lock");
        self.validate_publication(&node, &inbox, &ledger, ticket, &affiliation)?;
        if !node.scope.retained() {
            let owner = ledger
                .scopes
                .get(&affiliation.id())
                .ok_or(RuntimeError::UnknownScope)?;
            if owner.stopped() && ticket.descriptor.kind == EffectKind::Business {
                return Err(EffectError::NotReady.into());
            }
        }
        let scope = ledger
            .scopes
            .get_mut(&ticket.descriptor.work.scope)
            .ok_or(RuntimeError::UnknownScope)?;
        scope
            .start(ticket.descriptor.work.work)
            .map_err(RuntimeError::from)?;
        work.scope().runtime.shared.record(|| HistoryFact::Started {
            scope: ticket.descriptor.work.scope,
            work: ticket.descriptor.work.work,
        });
        Ok(())
    }
    /// Move the original still-unbound reservation into its actual host job.
    /// Starting the exact committed effect and taking its guard share the
    /// generation gate; no host side counter or reconstructed permit is used.
    pub fn take_step_reservation(
        &self,
        ticket: &EffectTicket<A::Effect>,
        reservation: ReservationId,
    ) -> Result<crate::obligation::StepRegistration, ActorTurnError> {
        let node = self.node(ticket.actor)?;
        Self::validate_ticket_origin(&node, ticket)?;
        let mut inbox = node.inbox.lock().expect("actor inbox lock");
        let execution_scope = node.scope.current_scope();
        let mut ledger = execution_scope
            .runtime
            .shared
            .state
            .lock()
            .expect("runtime ledger lock");
        self.validate_publication(&node, &inbox, &ledger, ticket, &execution_scope)?;
        let batch = inbox
            .batches
            .get_mut(&ticket.input)
            .ok_or(ActorTurnError::UnknownInput)?;
        let guard = batch
            .reservations
            .get(&reservation)
            .ok_or(ActorTurnError::UnknownReservation)?;
        if ticket.descriptor.work
            != (ScopedWorkRef {
                scope: execution_scope.id(),
                work: guard.work_id(),
            })
        {
            return Err(ActorTurnError::ReservationOwnerMismatch);
        }
        let guard = batch
            .reservations
            .remove(&reservation)
            .expect("validated original reservation");
        self.retire_metadata_input_and_wake(&node, &mut inbox, ticket.input, &mut ledger);
        drop(ledger);
        drop(inbox);
        self.shared.changed.notify();
        Ok(guard)
    }
    /// Bounded inventory of the actual guards retained by one committed step.
    /// This remains readable after owner failure without reading its core.
    pub fn step_reservations(
        &self,
        actor: ActorId,
        input: InputId,
        after: Option<ReservationId>,
        budget: usize,
    ) -> Result<Vec<(ReservationId, WorkId)>, ActorTurnError> {
        let node = self.node(actor)?;
        let inbox = node.inbox.lock().expect("actor inbox lock");
        let batch = inbox
            .batches
            .get(&input)
            .ok_or(ActorTurnError::UnknownInput)?;
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
    ) -> Result<crate::obligation::StepRegistration, ActorTurnError> {
        let node = self.node(actor)?;
        let mut inbox = node.inbox.lock().expect("actor inbox lock");
        if !inbox.failed {
            return Err(ActorTurnError::ActorNotRetired);
        }
        inbox
            .batches
            .get_mut(&input)
            .ok_or(ActorTurnError::UnknownInput)?
            .reservations
            .remove(&reservation)
            .ok_or(ActorTurnError::UnknownReservation)
    }
    fn validate_failure_work(
        node: &ActorCell<A>,
        inbox: &Inbox<A>,
        ledger: &crate::transaction::LedgerState,
        input: InputId,
        generation: Generation,
        action: ActionId,
        work: ScopedWorkRef,
    ) -> Result<(), ActorTurnError> {
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
        (input, generation, action, work): (InputId, Generation, ActionId, ScopedWorkRef),
        allow_admitted: bool,
    ) -> Result<(), ActorTurnError> {
        let execution_scope = node.scope.current_scope();
        let scope = ledger
            .scopes
            .get(&execution_scope.id())
            .ok_or(RuntimeError::UnknownScope)?;
        if !inbox.failed
            || !scope.failed()
            || inbox.failure_input != Some(input)
            || inbox.generation != generation
            || scope.generation() != generation
        {
            return Err(EffectError::StaleGeneration.into());
        }
        let batch = inbox
            .batches
            .get(&input)
            .ok_or(ActorTurnError::UnknownInput)?;
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
        if work.scope != execution_scope.id() {
            return Err(ActorTurnError::FailureWorkInactive);
        }
        let work = scope
            .work(work.work)
            .ok_or(ActorTurnError::FailureWorkInactive)?;
        if work.kind != WorkKind::Cleanup
            || !(work.phase == WorkPhase::Started
                || allow_admitted && matches!(work.phase, WorkPhase::Admitted | WorkPhase::Queued))
            || work.body_exited
        {
            return Err(ActorTurnError::FailureWorkInactive);
        }
        Ok(())
    }
    /// Validate an original failure action before its owner starts the prepaid
    /// cleanup Work. This read-only check grants no publication or exit and
    /// never accepts an ordinary publication ticket from a failed generation.
    pub fn validate_failure_effect_ticket(
        &self,
        ticket: &EffectTicket<A::Effect>,
    ) -> Result<(), ActorTurnError> {
        let node = self.node(ticket.actor)?;
        Self::validate_ticket_origin(&node, ticket)?;
        let inbox = node.inbox.lock().expect("actor inbox lock");
        let execution_scope = node.scope.current_scope();
        let ledger = execution_scope
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
            return Err(ActorTurnError::FailurePublicationAlreadyClaimed);
        }
        Ok(())
    }
    /// Claims one failure-generation publication authority from the actual
    /// started cleanup effect and its prepaid, still-running supervision work.
    /// The original ticket remains Started until the host completes that work.
    pub fn claim_failure_publication(
        &self,
        ticket: &EffectTicket<A::Effect>,
    ) -> Result<FailurePublicationSession<A>, ActorTurnError> {
        let node = self.node(ticket.actor)?;
        Self::validate_ticket_origin(&node, ticket)?;
        let mut inbox = node.inbox.lock().expect("actor inbox lock");
        let execution_scope = node.scope.current_scope();
        let ledger = execution_scope
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
            return Err(ActorTurnError::FailurePublicationAlreadyClaimed);
        }
        let claim = FailurePublicationOrigin::SupervisorEffect {
            input: ticket.input,
            action: ticket.descriptor.id,
        };
        inbox.failure_publication = Some(FailurePublicationRecord::Active {
            generation: ticket.generation,
            origin: claim,
            work: ticket.descriptor.work,
        });
        Ok(FailurePublicationSession {
            runtime: self.clone(),
            registration: ActorRegistration {
                actor: node.id,
                attachment: node.attachment,
                scope: node.scope.clone(),
                origin: node.origin.clone(),
            },
            actor: ticket.actor,
            origin: ticket.origin.clone(),
            scope: execution_scope.clone(),
            generation: ticket.generation,
            claim,
            work: ticket.descriptor.work,
        })
    }
    fn validate_ticket_origin(
        node: &ActorCell<A>,
        ticket: &EffectTicket<A::Effect>,
    ) -> Result<(), ActorTurnError> {
        if !Arc::ptr_eq(&node.origin, &ticket.origin) {
            return Err(ActorTurnError::EffectTicketOriginMismatch);
        }
        Ok(())
    }
    /// Validate an actual bound author without admitting input or publishing.
    /// Cross-Scope metadata observers require this original registration and
    /// the same real WorkObligation; numeric scope/work identities grant no right.
    pub fn validate_work_author(
        &self,
        registration: &ActorRegistration,
        work: &crate::obligation::WorkObligation,
    ) -> Result<(), ActorTurnError> {
        self.pending_input_author_for(registration, work)
            .map(|_| ())
    }
    pub(crate) fn pending_input_author_for(
        &self,
        registration: &ActorRegistration,
        work: &crate::obligation::WorkObligation,
    ) -> Result<PendingInputAuthor, ActorTurnError> {
        let node = self.node(registration.actor)?;
        if !Arc::ptr_eq(&node.origin, &registration.origin)
            || !Arc::ptr_eq(&node.scope, &registration.scope)
        {
            return Err(ActorTurnError::EffectTicketOriginMismatch);
        }
        let inbox = node.inbox.lock().expect("actor inbox lock");
        let affiliation = node.scope.current_scope();
        if !Arc::ptr_eq(&affiliation.runtime.shared, &work.scope().runtime.shared) {
            return Err(ActorTurnError::InputAuthorMismatch);
        }
        let ledger = affiliation
            .runtime
            .shared
            .state
            .lock()
            .expect("runtime ledger lock");
        if inbox.failed
            || (!node.scope.retained()
                && ledger
                    .scopes
                    .get(&affiliation.id())
                    .ok_or(RuntimeError::UnknownScope)?
                    .failed())
        {
            return Err(ActorTurnError::ActorFailed);
        }
        let scope = ledger
            .scopes
            .get(&work.scope().id())
            .ok_or(ActorTurnError::InputAuthorInactive)?;
        let header = scope
            .work(work.id())
            .ok_or(ActorTurnError::InputAuthorInactive)?;
        if scope.failed()
            || header.unbound_guard
            || header.body_exited
            || header.captures_exited
            || ledger.unresolved.contains(&(work.scope().id(), work.id()))
        {
            return Err(ActorTurnError::InputAuthorInactive);
        }
        Ok(PendingInputAuthor {
            registration: registration.clone(),
            scope: work.scope().clone(),
            work: work.reference(),
            actor_generation: inbox.generation,
            generation: scope.generation(),
        })
    }
    pub(crate) fn validate_original_input_author(
        &self,
        author: &InputAuthor,
    ) -> Result<(), ActorTurnError> {
        let node = self.node(author.actor)?;
        let inbox = node.inbox.lock().expect("actor inbox lock");
        let affiliation = node.scope.current_scope();
        let ledger = affiliation
            .runtime
            .shared
            .state
            .lock()
            .expect("runtime ledger lock");
        if inbox.failed {
            return Err(ActorTurnError::ActorFailed);
        }
        Self::validate_input_author(&node, &inbox, &ledger, author, false, true)
    }
    fn validate_input_author(
        node: &ActorCell<A>,
        inbox: &Inbox<A>,
        ledger: &crate::transaction::LedgerState,
        author: &InputAuthor,
        after_failure: bool,
        require_active: bool,
    ) -> Result<(), ActorTurnError> {
        let affiliation = node.scope.current_scope();
        if author.actor != node.id
            || !Arc::ptr_eq(&author.origin, &node.origin)
            || !Arc::ptr_eq(&author.scope.runtime.shared, &affiliation.runtime.shared)
            || (!after_failure && author.actor_generation != inbox.generation)
        {
            return Err(ActorTurnError::InputAuthorMismatch);
        }
        let scope = ledger
            .scopes
            .get(&author.work.scope)
            .ok_or(ActorTurnError::InputAuthorInactive)?;
        let work = scope
            .work(author.work.work)
            .ok_or(ActorTurnError::InputAuthorInactive)?;
        if scope.failed()
            || scope.generation() != author.generation
            || work.unbound_guard
            || require_active && (work.body_exited || work.captures_exited)
            || ledger
                .unresolved
                .contains(&(author.work.scope, author.work.work))
        {
            return Err(ActorTurnError::InputAuthorInactive);
        }
        Ok(())
    }
    fn action_author_generation(
        node: &ActorCell<A>,
        inbox: &Inbox<A>,
        ledger: &crate::transaction::LedgerState,
        input: InputId,
        descriptor: &EffectDescriptor,
    ) -> Result<Generation, ActorTurnError> {
        if node.scope.retained() && descriptor.kind == EffectKind::Business {
            return Err(EffectError::InvalidTransition.into());
        }
        let batch = inbox
            .batches
            .get(&input)
            .ok_or(ActorTurnError::UnknownInput)?;
        if let Some(proof) = batch.authors.get(&descriptor.id) {
            if proof.work != descriptor.work {
                return Err(ActorTurnError::InputAuthorMismatch);
            }
            Self::validate_input_author(
                node,
                inbox,
                ledger,
                proof,
                false,
                descriptor.kind == EffectKind::Business,
            )?;
        } else if descriptor.work.scope != node.scope.id() || node.scope.retained() {
            return Err(ActorTurnError::InputAuthorMismatch);
        }
        let scope = ledger
            .scopes
            .get(&descriptor.work.scope)
            .ok_or(ActorTurnError::InputAuthorInactive)?;
        let work = scope
            .work(descriptor.work.work)
            .ok_or(ActorTurnError::InputAuthorInactive)?;
        if (scope.failed()
            && !(inbox.failed
                && inbox.failure_input == Some(input)
                && descriptor.kind == EffectKind::Cleanup))
            || descriptor.kind == EffectKind::Business && (work.body_exited || work.captures_exited)
            || ledger
                .unresolved
                .contains(&(descriptor.work.scope, descriptor.work.work))
        {
            return Err(ActorTurnError::InputAuthorInactive);
        }
        Ok(scope.generation())
    }
    fn validate_publication(
        &self,
        node: &ActorCell<A>,
        inbox: &Inbox<A>,
        ledger: &crate::transaction::LedgerState,
        ticket: &EffectTicket<A::Effect>,
        target: &ScopeHandle,
    ) -> Result<(), ActorTurnError> {
        let execution_scope = node.scope.current_scope();
        Self::validate_ticket_origin(node, ticket)?;
        if !Arc::ptr_eq(&execution_scope.runtime.shared, &target.runtime.shared) {
            return Err(ActorTurnError::PublicationTargetMismatch);
        }
        let batch = inbox
            .batches
            .get(&ticket.input)
            .ok_or(ActorTurnError::UnknownInput)?;
        if !node.scope.retained()
            && ledger
                .scopes
                .get(&execution_scope.id())
                .ok_or(RuntimeError::UnknownScope)?
                .failed()
        {
            return Err(ActorTurnError::FailurePublicationRequired);
        }
        if inbox.failed {
            return Err(ActorTurnError::FailurePublicationRequired);
        }
        if inbox.generation != ticket.generation || batch.state.generation() != ticket.generation {
            return Err(EffectError::StaleGeneration.into());
        }
        let author_generation =
            Self::action_author_generation(node, inbox, ledger, ticket.input, &ticket.descriptor)?;
        if author_generation != ticket.author_generation {
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
    fn validate_target_binding(
        node: &ActorCell<A>,
        inbox: &Inbox<A>,
        ledger: &crate::transaction::LedgerState,
        ticket: Option<&EffectTicket<A::Effect>>,
        owner: &ScopeHandle,
        binding: &crate::publication::PublicationBinding,
        after_failure: bool,
    ) -> Result<(), ActorTurnError> {
        use crate::publication::PublicationBinding;
        let affiliation = node.scope.current_scope();
        if !Arc::ptr_eq(&affiliation.runtime.shared, &owner.runtime.shared) {
            return Err(ActorTurnError::PublicationTargetMismatch);
        }
        match binding {
            PublicationBinding::SameScope => {
                if node.scope.retained()
                    || owner.id() != node.scope.id()
                    || ticket.is_some_and(|ticket| ticket.descriptor.work.scope != owner.id())
                {
                    return Err(ActorTurnError::PublicationTargetMismatch);
                }
            }
            PublicationBinding::Actor(registration) => {
                if registration.actor != node.id
                    || !Arc::ptr_eq(&registration.origin, &node.origin)
                    || !Arc::ptr_eq(&registration.scope, &node.scope)
                {
                    return Err(ActorTurnError::PublicationTargetMismatch);
                }
            }
            PublicationBinding::PendingInput(author) => {
                if !after_failure
                    || !inbox.failed
                    || author.registration.actor != node.id
                    || author.registration.attachment != node.attachment
                    || !Arc::ptr_eq(&author.registration.origin, &node.origin)
                    || !Arc::ptr_eq(&author.registration.scope, &node.scope)
                    || !Arc::ptr_eq(&author.scope.runtime.shared, &affiliation.runtime.shared)
                    || !owner.same_scope(&author.scope)
                    || author.work.scope != author.scope.id()
                    || author.actor_generation >= inbox.generation
                    || author.actor_generation < node.scope.original_epoch
                {
                    return Err(ActorTurnError::InputAuthorMismatch);
                }
                let financial = ledger
                    .scopes
                    .get(&author.work.scope)
                    .ok_or(ActorTurnError::InputAuthorInactive)?;
                let work = financial
                    .work(author.work.work)
                    .ok_or(ActorTurnError::InputAuthorInactive)?;
                if financial.failed()
                    || financial.generation() != author.generation
                    || work.unbound_guard
                    || work.body_exited
                    || work.captures_exited
                    || ledger
                        .unresolved
                        .contains(&(author.work.scope, author.work.work))
                {
                    return Err(ActorTurnError::InputAuthorInactive);
                }
            }
            PublicationBinding::Input(author) => {
                Self::validate_input_author(node, inbox, ledger, author, after_failure, false)?;
                if owner.id() != author.work.scope {
                    return Err(ActorTurnError::PublicationTargetMismatch);
                }
                if let Some(ticket) = ticket {
                    let proof = inbox
                        .batches
                        .get(&ticket.input)
                        .ok_or(ActorTurnError::UnknownInput)?
                        .authors
                        .get(&ticket.descriptor.id)
                        .ok_or(ActorTurnError::InputAuthorMismatch)?;
                    if proof.work != author.work
                        || proof.input != author.input
                        || !Arc::ptr_eq(&proof.origin, &author.origin)
                    {
                        return Err(ActorTurnError::PublicationTargetMismatch);
                    }
                }
            }
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
    ) -> Result<Arc<T>, (ActorTurnError, Arc<T>)> {
        let next = ManuallyDrop::new(next);
        let node = match self.node(ticket.actor) {
            Ok(node) => node,
            Err(error) => return Err((error, ManuallyDrop::into_inner(next))),
        };
        let mut inbox = node.inbox.lock().expect("actor inbox lock");
        let execution_scope = node.scope.current_scope();
        let ledger = execution_scope
            .runtime
            .shared
            .state
            .lock()
            .expect("runtime ledger lock");
        if let Err(error) = self
            .validate_publication(&node, &inbox, &ledger, ticket, &target.owner)
            .and_then(|()| {
                Self::validate_target_binding(
                    &node,
                    &inbox,
                    &ledger,
                    Some(ticket),
                    &target.owner,
                    &target.binding,
                    false,
                )
            })
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
        self.shared.record(|| HistoryFact::Effect {
            target: node.history_target(),
            input: ticket.input,
            action: ticket.descriptor.id,
            actor_generation: ticket.generation,
            financial_work: ticket.descriptor.work,
            author_generation: ticket.author_generation,
            phase: novarocks_control_runtime_core::history::EffectHistoryPhase::Published,
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
    ) -> Result<(), (ActorTurnError, Arc<T>)> {
        let next = ManuallyDrop::new(next);
        let node = match self.node(ticket.actor) {
            Ok(node) => node,
            Err(error) => return Err((error, ManuallyDrop::into_inner(next))),
        };
        let mut inbox = node.inbox.lock().expect("actor inbox lock");
        let execution_scope = node.scope.current_scope();
        let ledger = execution_scope
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
        if let Err(error) = Self::validate_target_binding(
            &node,
            &inbox,
            &ledger,
            Some(ticket),
            &target.owner,
            &reply.binding,
            false,
        ) {
            return Err((error, ManuallyDrop::into_inner(next)));
        }
        if reply.phase != crate::publication::ReplyPhase::Empty || reply.value.is_some() {
            return Err((
                ActorTurnError::ReplyAlreadyCommitted,
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
        self.shared.record(|| HistoryFact::Effect {
            target: node.history_target(),
            input: ticket.input,
            action: ticket.descriptor.id,
            actor_generation: ticket.generation,
            financial_work: ticket.descriptor.work,
            author_generation: ticket.author_generation,
            phase: novarocks_control_runtime_core::history::EffectHistoryPhase::Published,
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
    ) -> Result<(), ActorTurnError> {
        let node = self.node(ticket.actor)?;
        Self::validate_ticket_origin(&node, ticket)?;
        let mut inbox = node.inbox.lock().expect("actor inbox lock");
        inbox
            .batches
            .get_mut(&ticket.input)
            .ok_or(ActorTurnError::UnknownInput)?
            .state
            .published(ticket.descriptor.id)?;
        self.shared.record(|| HistoryFact::Effect {
            target: node.history_target(),
            input: ticket.input,
            action: ticket.descriptor.id,
            actor_generation: ticket.generation,
            financial_work: ticket.descriptor.work,
            author_generation: ticket.author_generation,
            phase: novarocks_control_runtime_core::history::EffectHistoryPhase::Published,
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
                error: ActorTurnError::EffectTicketOriginMismatch,
                ticket,
            });
        }
        self.effect_completed(ticket)
    }
    /// Observe an actual original dispatch without consuming or cloning its
    /// ticket. Failure generations do not revoke physical completion evidence.
    pub fn effect_completion_observer(
        &self,
        ticket: &EffectTicket<A::Effect>,
    ) -> Result<EffectCompletionObserver<A>, ActorTurnError> {
        let node = self.node(ticket.actor)?;
        Self::validate_ticket_origin(&node, ticket)?;
        let inbox = node.inbox.lock().expect("actor inbox lock");
        let record = inbox
            .batches
            .get(&ticket.input)
            .ok_or(ActorTurnError::UnknownInput)?
            .state
            .record(ticket.descriptor.id)
            .ok_or(EffectError::UnknownAction)?;
        if record.descriptor() != &ticket.descriptor
            || !matches!(
                record.state(),
                EffectState::Started | EffectState::Published
            )
        {
            return Err(EffectError::InvalidTransition.into());
        }
        Ok(EffectCompletionObserver {
            runtime: self.clone(),
            origin: ticket.origin.clone(),
            actor: ticket.actor,
            input: ticket.input,
            action: ticket.descriptor.id,
        })
    }
    pub fn effect_completed(
        &self,
        ticket: EffectTicket<A::Effect>,
    ) -> Result<(), EffectCompletionRefusal<A::Effect>> {
        self.complete_effect(ticket, false).map(|_| ())
    }
    /// Transfer this actual completion fact out of its original batch before
    /// automatic metadata retirement can remove that batch. No fake input or
    /// second completed-action registry is required by a later ordering owner.
    pub fn effect_completed_with_receipt(
        &self,
        ticket: EffectTicket<A::Effect>,
    ) -> Result<CompletedEffect<A>, EffectCompletionRefusal<A::Effect>> {
        self.complete_effect(ticket, true)
            .map(|receipt| receipt.expect("the original completion requested its receipt"))
    }
    pub fn effect_completed_for_with_receipt(
        &self,
        registration: &ActorRegistration,
        ticket: EffectTicket<A::Effect>,
    ) -> Result<CompletedEffect<A>, EffectCompletionRefusal<A::Effect>> {
        if registration.actor != ticket.actor || !Arc::ptr_eq(&registration.origin, &ticket.origin)
        {
            return Err(EffectCompletionRefusal {
                error: ActorTurnError::EffectTicketOriginMismatch,
                ticket,
            });
        }
        self.effect_completed_with_receipt(ticket)
    }
    fn complete_effect(
        &self,
        ticket: EffectTicket<A::Effect>,
        with_receipt: bool,
    ) -> Result<Option<CompletedEffect<A>>, EffectCompletionRefusal<A::Effect>> {
        let complete = || -> Result<Option<CompletedEffect<A>>, ActorTurnError> {
            let node = self.node(ticket.actor)?;
            Self::validate_ticket_origin(&node, &ticket)?;
            let mut inbox = node.inbox.lock().expect("actor inbox lock");
            let batch = inbox
                .batches
                .get_mut(&ticket.input)
                .ok_or(ActorTurnError::UnknownInput)?;
            if batch
                .state
                .record(ticket.descriptor.id)
                .ok_or(EffectError::UnknownAction)?
                .descriptor()
                != &ticket.descriptor
            {
                return Err(EffectError::InvalidTransition.into());
            }
            batch.state.complete(ticket.descriptor.id)?;
            self.shared.record(|| HistoryFact::Effect {
                target: node.history_target(),
                input: ticket.input,
                action: ticket.descriptor.id,
                actor_generation: ticket.generation,
                financial_work: ticket.descriptor.work,
                author_generation: ticket.author_generation,
                phase: novarocks_control_runtime_core::history::EffectHistoryPhase::Completed,
            });
            let completed = with_receipt.then(|| CompletedEffect {
                runtime: self.clone(),
                registration: ActorRegistration {
                    actor: node.id,
                    attachment: node.attachment,
                    scope: node.scope.clone(),
                    origin: node.origin.clone(),
                },
                input: ticket.input,
                ordinal: batch
                    .ordinal
                    .expect("actual completion belongs to the original committed batch"),
                generation: batch.state.generation(),
                descriptor: ticket.descriptor.clone(),
            });
            {
                let scope = node.scope.current_scope();
                let mut ledger = scope
                    .runtime
                    .shared
                    .state
                    .lock()
                    .expect("original metadata tail header custody");
                self.retire_metadata_input_and_wake(&node, &mut inbox, ticket.input, &mut ledger);
            }
            self.shared
                .state
                .lock()
                .expect("runtime scheduler lock")
                .effect_ready
                .notify(ticket.actor)?;
            drop(inbox);
            self.shared.changed.notify();
            Ok(completed)
        };
        match complete() {
            Ok(receipt) => Ok(receipt),
            Err(error) => Err(EffectCompletionRefusal { error, ticket }),
        }
    }
    /// Apply only the original completed predecessor's declared edges. The
    /// receipt proves completion without retaining the original input header;
    /// if that batch remains, its real records still decide every pending edge.
    pub fn acknowledge_completed_effect(
        &self,
        receipt: &CompletedEffect<A>,
    ) -> Result<(), ActorTurnError> {
        if !self.same_runtime(&receipt.runtime) {
            return Err(ActorTurnError::EffectTicketOriginMismatch);
        }
        let node = self.node(receipt.registration.actor)?;
        if node.attachment != receipt.registration.attachment
            || !Arc::ptr_eq(&node.origin, &receipt.registration.origin)
            || !Arc::ptr_eq(&node.scope, &receipt.registration.scope)
        {
            return Err(ActorTurnError::EffectTicketOriginMismatch);
        }
        let mut inbox = node
            .inbox
            .lock()
            .expect("original completed effect dependency gate");
        let Some(batch) = inbox.batches.get_mut(&receipt.input) else {
            // Only the private successful Completed transition can mint this
            // receipt. A missing numeric input alone never reaches this gate.
            // Original batch retirement already requires every edge's tail
            // terminal, so there is no surviving dependency to acknowledge.
            return Ok(());
        };
        if batch.ordinal != Some(receipt.ordinal) || batch.state.generation() != receipt.generation
        {
            return Err(ActorTurnError::EffectTicketOriginMismatch);
        }
        let original = batch
            .state
            .record(receipt.descriptor.id)
            .ok_or(EffectError::UnknownAction)?;
        if original.descriptor() != &receipt.descriptor
            || original.state() != EffectState::Completed
        {
            return Err(EffectError::InvalidTransition.into());
        }
        for (descriptor, state) in batch.state.records(None, node.config.max_effects_per_step) {
            if state == EffectState::Pending
                && descriptor.dependencies.contains(&receipt.descriptor.id)
            {
                batch
                    .state
                    .dependency_published(descriptor.id, receipt.descriptor.id)?;
            }
        }
        Ok(())
    }
    /// The caller acknowledges the exact declared predecessor condition, after its real publication or completion fact.
    pub fn acknowledge_dependency(
        &self,
        actor: ActorId,
        input: InputId,
        action: ActionId,
        predecessor: ActionId,
    ) -> Result<(), ActorTurnError> {
        self.node(actor)?
            .inbox
            .lock()
            .expect("actor inbox lock")
            .batches
            .get_mut(&input)
            .ok_or(ActorTurnError::UnknownInput)?
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
    ) -> Result<(), ActorTurnError> {
        let node = self.node(actor)?;
        let mut inbox = node.inbox.lock().expect("actor inbox lock");
        let generation = inbox.generation;
        inbox
            .batches
            .get_mut(&input)
            .ok_or(ActorTurnError::UnknownInput)?
            .state
            .abandon_unstarted(action, generation)?;
        Ok(())
    }
    /// Called by the host failure owner in bounded slices; never pretends the failed core consumed an input.
    pub fn abandon_failed_input(
        &self,
        actor: ActorId,
        input: InputId,
    ) -> Result<(), ActorTurnError> {
        let node = self.node(actor)?;
        let mut inbox = node.inbox.lock().expect("actor inbox lock");
        if !inbox.failed {
            return Err(ActorTurnError::ActorNotRetired);
        }
        let record = inbox
            .inputs
            .get_mut(&input)
            .ok_or(ActorTurnError::UnknownInput)?;
        if record.phase == InputPhase::Filled {
            return Err(ActorTurnError::InputStillQueued);
        }
        if record.phase == InputPhase::Committed {
            return Err(ActorTurnError::EffectsRemain);
        }
        if record.phase != InputPhase::AbandonedByOwnerFailure {
            record.phase = InputPhase::AbandonedByOwnerFailure;
            self.shared.record(|| HistoryFact::InputAbandoned {
                target: node.history_target(),
                input,
            });
        }
        self.resolve_metadata_delivery(&inbox, input, MetadataConsumption::AbandonedByOwnerFailure);
        Ok(())
    }
    pub fn take_failed_input(
        &self,
        actor: ActorId,
        class: InputClass,
    ) -> Result<Option<(InputEnvelope, A::Input)>, ActorTurnError> {
        let node = self.node(actor)?;
        let mut inbox = node.inbox.lock().expect("actor inbox lock");
        if !inbox.failed {
            return Err(ActorTurnError::ActorNotRetired);
        }
        let input = inbox.mailbox.pop(class);
        Ok(input.map(|input| {
            inbox
                .inputs
                .get_mut(&input.input)
                .expect("failed filled input")
                .phase = InputPhase::AbandonedByOwnerFailure;
            self.resolve_metadata_delivery(
                &inbox,
                input.input,
                MetadataConsumption::AbandonedByOwnerFailure,
            );
            self.shared.record(|| HistoryFact::InputAbandoned {
                target: node.history_target(),
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
    ) -> Result<Option<Box<dyn Any + Send>>, ActorTurnError> {
        let node = self.node(actor)?;
        let payload = node.inbox.lock().expect("actor inbox lock").failure.take();
        Ok(payload.map(ManuallyDrop::into_inner))
    }
    /// External captures and all report consumers must already have retired before this release.
    fn input_retirement_preflight(
        inbox: &Inbox<A>,
        input: InputId,
    ) -> Result<(InputClass, Option<StandingInputId>), ActorTurnError> {
        let record = inbox
            .inputs
            .get(&input)
            .ok_or(ActorTurnError::UnknownInput)?;
        if !matches!(
            record.phase,
            InputPhase::Committed | InputPhase::AbandonedByOwnerFailure
        ) {
            return Err(ActorTurnError::InputStillQueued);
        }
        if inbox
            .batches
            .get(&input)
            .is_some_and(|batch| !batch.state.all_terminal())
        {
            return Err(ActorTurnError::EffectsRemain);
        }
        if inbox
            .batches
            .get(&input)
            .is_some_and(|batch| !batch.reservations.is_empty())
        {
            return Err(ActorTurnError::ReservationsRemain);
        }
        if inbox
            .batches
            .get(&input)
            .is_some_and(|batch| batch.metadata.iter().any(|item| !item.completed))
        {
            return Err(ActorTurnError::MetadataStillPending);
        }
        let class = record.class;
        let input_address = record.standing_input_id;
        if let Some(input_address) = input_address {
            let owner = inbox
                .standing_inputs
                .get(&input_address)
                .ok_or(ActorTurnError::UnknownStandingInput)?;
            if owner.in_flight != Some(input) {
                return Err(ActorTurnError::InputNotReserved);
            }
        }
        Ok((class, input_address))
    }
    /// The same preflight as real retirement, without releasing any holder.
    /// Ready is a snapshot; actual retirement must still validate its input.
    pub fn input_retirement_status(
        &self,
        actor: ActorId,
        input: InputId,
    ) -> Result<(), ActorTurnError> {
        let node = self.node(actor)?;
        let inbox = node.inbox.lock().expect("input retirement observation");
        Self::input_retirement_preflight(&inbox, input).map(|_| ())
    }
    /// Bounded original header-pin identities, with no release authority.
    pub fn input_header_authors(
        &self,
        actor: ActorId,
        input: InputId,
        limit: usize,
    ) -> Result<Vec<ScopedWorkRef>, ActorTurnError> {
        let node = self.node(actor)?;
        let inbox = node.inbox.lock().expect("input header custody observation");
        let record = inbox
            .inputs
            .get(&input)
            .ok_or(ActorTurnError::UnknownInput)?;
        Ok(record
            .header_pins
            .iter()
            .take(limit)
            .map(|pin| pin.reference())
            .collect())
    }
    pub fn retire_input(&self, actor: ActorId, input: InputId) -> Result<(), ActorTurnError> {
        let node = self.node(actor)?;
        let mut inbox = node.inbox.lock().expect("actor inbox lock");
        let (class, input_address) = Self::input_retirement_preflight(&inbox, input)?;
        if let Some(batch) = inbox.batches.remove(&input) {
            let ordinal = batch
                .ordinal
                .expect("a resident batch was atomically committed");
            assert_eq!(inbox.batch_order.remove(&ordinal), Some(input));
        }
        let mut original = inbox
            .inputs
            .remove(&input)
            .expect("same terminal original input");
        let scope = node.scope.current_scope();
        let mut ledger = scope
            .runtime
            .shared
            .state
            .lock()
            .expect("terminal input header release");
        original.release_header_custody(&mut ledger);
        drop(ledger);
        if let Some(input_address) = input_address {
            inbox
                .standing_inputs
                .get_mut(&input_address)
                .expect("in-flight role retains its position")
                .in_flight = None;
        } else if inbox.failure_input != Some(input) {
            self.notify_metadata_credit_sources(inbox.return_input_credit(class));
        }
        if inbox.failure_input == Some(input) {
            inbox.failure_input = None;
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
        if !inbox.failed && inbox.has_selectable_input(actor) {
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
        state: &ActorRuntimeState<A>,
        registration: &ActorRegistration,
    ) -> Result<(), ActorTurnError> {
        let node = state
            .actors
            .get(&registration.actor)
            .ok_or(ActorTurnError::UnknownActor)?;
        if node.attachment != registration.attachment
            || !Arc::ptr_eq(&node.scope, &registration.scope)
            || !Arc::ptr_eq(&node.origin, &registration.origin)
        {
            return Err(ActorTurnError::EffectTicketOriginMismatch);
        }
        Ok(())
    }
    /// Read the actual fenced failure of this same actor incarnation. This
    /// does not fail a financial scope or inspect the failed protocol state.
    pub fn failure_proof_for(
        &self,
        registration: &ActorRegistration,
    ) -> Result<ActorFailureProof, ActorTurnError> {
        let node = self.node(registration.actor)?;
        if node.attachment != registration.attachment
            || !Arc::ptr_eq(&node.scope, &registration.scope)
            || !Arc::ptr_eq(&node.origin, &registration.origin)
        {
            return Err(ActorTurnError::EffectTicketOriginMismatch);
        }
        let inbox = node.inbox.lock().expect("actor failure proof inbox lock");
        if !inbox.failed || inbox.generation <= registration.initial_epoch() {
            return Err(ActorTurnError::ActorNotRetired);
        }
        Ok(ActorFailureProof {
            registration: registration.clone(),
            failed_epoch: inbox.generation,
            ledger: node.scope.current_scope().runtime.clone(),
        })
    }
    /// Qualify the actual current registration without scheduling or granting
    /// admission. Equal public IDs in another runtime cannot pass this check.
    pub fn validate_registration(
        &self,
        registration: &ActorRegistration,
    ) -> Result<(), ActorTurnError> {
        let state = self.shared.state.lock().expect("runtime scheduler lock");
        Self::validate_registration_in_state(&state, registration)
    }
    /// Read the original registration from a currently valid exclusive effect
    /// ticket. This witness grants no execution, input or publication authority.
    pub fn registration_from_effect(
        &self,
        ticket: &EffectTicket<A::Effect>,
    ) -> Result<ActorRegistration, ActorTurnError> {
        self.validate_effect_ticket(ticket)?;
        let node = self.node(ticket.actor)?;
        // Validation and this lookup need not share one scheduler lock. Never
        // return a later incarnation registered at the same public actor ID.
        Self::validate_ticket_origin(&node, ticket)?;
        Ok(ActorRegistration {
            actor: node.id,
            attachment: node.attachment,
            scope: node.scope.clone(),
            origin: node.origin.clone(),
        })
    }

    /// A host completion/retry mark, including during failure or process drain.
    /// Qualify origin and enqueue under the same scheduler transaction, so an
    /// old witness can never wake an actor registered later at the same ID.
    pub fn notify_host_actor(
        &self,
        registration: &ActorRegistration,
    ) -> Result<(), ActorTurnError> {
        let mut state = self.shared.state.lock().expect("runtime scheduler lock");
        Self::validate_registration_in_state(&state, registration)?;
        state.effect_ready.notify(registration.actor)?;
        drop(state);
        self.shared.changed.notify();
        Ok(())
    }
    pub(crate) fn arm_header_readiness(
        &self,
        owner: &ActorRegistration,
        registration: &mut crate::header_readiness::HeaderReadyRegistration,
        observer: &crate::obligation::WorkRetirementObserver,
    ) -> Result<crate::header_readiness::HeaderWaitState, crate::header_readiness::HeaderReadyError>
    {
        let node = self.node(owner.actor)?;
        // Removal and execution detachment use this same actual inbox gate.
        // Validate under scheduler, then release it before ledger acquisition:
        // detachment already has the canonical ledger -> scheduler order.
        let _inbox = node.inbox.lock().expect("actor inbox lock");
        {
            let state = self.shared.state.lock().expect("runtime scheduler lock");
            Self::validate_registration_in_state(&state, owner)?;
        }
        let scope = node.scope.current_scope();
        if !registration.belongs_to_ledger(&scope) {
            return Err(crate::header_readiness::HeaderReadyError::WrongOwner);
        }
        // The inbox gate retains this actual registration through first bind;
        // no scheduler lock is held while arm probes the original ledger.
        registration.arm_for(
            &self.shared.header_readiness,
            crate::header_readiness::HeaderOwner {
                actor: owner.actor,
                attachment: owner.attachment,
                origin: owner.origin.clone(),
            },
            observer,
        )
    }
    /// One poller owns this original actor's host turn. Pure Core dispatch has
    /// its independent queue and can proceed while this metadata turn exists.
    pub fn claim_host_actor(&self) -> Option<ActorHostTurn<A>> {
        self.claim_host_actor_with_budget(1)
    }
    /// One header-ready handoff consumes one budget unit. This is a dispatch
    /// budget, never an admission limit. Ordinary host turns need no hub row.
    pub fn claim_host_actor_with_budget(
        &self,
        readiness_budget: usize,
    ) -> Option<ActorHostTurn<A>> {
        // Never hold the readiness hub while acquiring the scheduler. Actual
        // release may hold inbox/ledger and can only touch hub + fixed Notify.
        let owners = self.shared.header_readiness.promote(readiness_budget);
        let mut state = self.shared.state.lock().expect("runtime scheduler lock");
        for owner in owners {
            if state.actors.get(&owner.actor).is_some_and(|node| {
                node.attachment == owner.attachment && Arc::ptr_eq(&node.origin, &owner.origin)
            }) {
                state
                    .effect_ready
                    .notify(owner.actor)
                    .expect("actual registered header owner");
            }
        }
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
    ) -> Result<(), ActorTurnError> {
        let closing_pending = {
            let mut inbox = node.inbox.lock().expect("actor inbox lock");
            if inbox.failed || inbox.sealed || inbox.metadata_closing {
                self.close_metadata_waiters_locked(node, &mut inbox)
            } else {
                false
            }
        };
        let has_runnable = has_runnable
            || closing_pending
            || self
                .shared
                .header_readiness
                .pending(&ActorHostTurn::<A>::header_owner(node));
        let mut state = self.shared.state.lock().expect("runtime scheduler lock");
        if !state
            .actors
            .get(&node.id)
            .is_some_and(|actual| Arc::ptr_eq(actual, node))
        {
            return Err(ActorTurnError::EffectTicketOriginMismatch);
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
    ) -> Result<Vec<InputId>, ActorTurnError> {
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
    ) -> Result<Vec<(BatchId, InputId)>, ActorTurnError> {
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
    ) -> Result<Option<InputCustody>, ActorTurnError> {
        let node = self.node(actor)?;
        let inbox = node.inbox.lock().expect("actor inbox lock");
        Ok(inbox.inputs.get(&input).map(|record| InputCustody {
            input,
            source: record.source,
            class: record.class,
            phase: record.phase,
            standing_input_id: record.standing_input_id,
            runtime_metadata: record.metadata_delivery.is_some(),
        }))
    }
    /// Enumerates the same actual input records; a point read grants no start
    /// or publication authority and changes no consumption or exit fact.
    pub fn input_inventory(
        &self,
        actor: ActorId,
        after: Option<InputId>,
        budget: usize,
    ) -> Result<Vec<InputCustody>, ActorTurnError> {
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
                standing_input_id: record.standing_input_id,
                runtime_metadata: record.metadata_delivery.is_some(),
            })
            .collect())
    }
    /// Enumerates standing input custody in bounded slices, including idle inputs
    /// whose authority was dropped. No payload or protocol-private state is read.
    pub fn standing_input_inventory(
        &self,
        actor: ActorId,
        after: Option<StandingInputId>,
        budget: usize,
    ) -> Result<Vec<StandingInputCustody>, ActorTurnError> {
        let node = self.node(actor)?;
        let inbox = node.inbox.lock().expect("actor inbox lock");
        let lower = after.map_or(std::ops::Bound::Unbounded, std::ops::Bound::Excluded);
        Ok(inbox
            .standing_inputs
            .range((lower, std::ops::Bound::Unbounded))
            .take(budget)
            .map(|(input_address, record)| StandingInputCustody {
                standing_input_id: *input_address,
                source: record.source,
                class: record.class,
                in_flight: record.in_flight,
            })
            .collect())
    }
    fn close_standing_input_after_exit(
        &self,
        actor: ActorId,
        standing_input_id: StandingInputId,
        require_failed: bool,
        origin: Option<&Arc<()>>,
    ) -> Result<(), ActorTurnError> {
        let node = self.node(actor)?;
        if origin.is_some_and(|original| !Arc::ptr_eq(original, &node.origin)) {
            return Err(ActorTurnError::EffectTicketOriginMismatch);
        }
        let mut inbox = node.inbox.lock().expect("actor inbox lock");
        let execution_scope = node.scope.current_scope();
        if require_failed && !inbox.failed && !execution_scope.gates()?.failed {
            return Err(ActorTurnError::ActorNotRetired);
        }
        let record = inbox
            .standing_inputs
            .get(&standing_input_id)
            .ok_or(ActorTurnError::UnknownStandingInput)?;
        if record.in_flight.is_some() {
            return Err(ActorTurnError::StandingInputBusy);
        }
        let class = record.class;
        let original = inbox
            .standing_inputs
            .remove(&standing_input_id)
            .expect("same idle original standing input");
        if let Some(pin) = original.producer_header {
            let mut ledger = execution_scope
                .runtime
                .shared
                .state
                .lock()
                .expect("original standing input header release");
            pin.release_locked(&mut ledger);
        }
        self.notify_metadata_credit_sources(inbox.return_input_credit(class));
        drop(inbox);
        self.shared.changed.notify();
        Ok(())
    }
    /// Scope-failure cleanup can retire a lost standing input authority after observing
    /// its actual owner exit. Failure alone never releases an in-flight input.
    pub fn close_failed_standing_input_after_exit(
        &self,
        actor: ActorId,
        standing_input_id: StandingInputId,
    ) -> Result<(), ActorTurnError> {
        self.close_standing_input_after_exit(actor, standing_input_id, true, None)
    }
    /// Returns pure state to its host owner; this method does not drop it on a poller.
    /// Work join is only one prerequisite. The actor attachment itself stays
    /// in scope storage until all actual input/effect custody has retired here.
    pub fn remove_actor(&self, actor: ActorId) -> Result<A, ActorTurnError> {
        let node = self.node(actor)?;
        self.remove_actor_node(&node, false)
    }
    fn remove_actor_node(
        &self,
        node: &Arc<ActorCell<A>>,
        claimed: bool,
    ) -> Result<A, ActorTurnError> {
        let execution_scope = node.scope.current_scope();
        let actor = node.id;
        let mut inbox = node.inbox.lock().expect("actor inbox lock");
        if !inbox.inputs.is_empty()
            || !inbox.standing_inputs.is_empty()
            || !inbox.batches.is_empty()
            || inbox.failure.is_some()
            || inbox
                .failed_custodian_timer
                .as_ref()
                .is_some_and(FailedCustodianTimer::is_pending)
            || matches!(
                inbox.failure_publication.as_ref(),
                Some(FailurePublicationRecord::Active {
                    origin: FailurePublicationOrigin::StartedCleanup { .. },
                    ..
                })
            )
            || !node.scope.retained() && !execution_scope.join_ready()?
        {
            return Err(ActorTurnError::ActorNotRetired);
        }
        if self.close_metadata_waiters_locked(node, &mut inbox) {
            return Err(ActorTurnError::ActorNotRetired);
        }
        let mut state = self.shared.state.lock().expect("runtime scheduler lock");
        if !state
            .actors
            .get(&actor)
            .is_some_and(|actual| Arc::ptr_eq(actual, node))
        {
            return Err(ActorTurnError::EffectTicketOriginMismatch);
        }
        if self
            .shared
            .header_readiness
            .bound(&ActorHostTurn::<A>::header_owner(node))
        {
            return Err(ActorTurnError::ActorNotRetired);
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
        inbox.failed_custodian_timer = None;
        inbox.failure_position = None;
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
        if !node.scope.retained() {
            execution_scope.detach_actor(node.attachment)?;
        }
        let actor = node
            .core
            .lock()
            .expect("actor core lock")
            .take()
            .expect("one core owner");
        drop(inbox);
        self.shared.changed.notify();
        self.flush_capacity_notifications();
        Ok(actor)
    }
    fn detach_execution_node(
        &self,
        node: &Arc<ActorCell<A>>,
        process: &ScopeHandle,
    ) -> Result<(), ActorTurnError> {
        let mut inbox = node.inbox.lock().expect("actor inbox lock");
        let mut binding = node
            .scope
            .execution
            .lock()
            .expect("actor execution binding lock");
        let ActorExecution::Live { scope, .. } = &*binding else {
            return Err(ActorTurnError::ExecutionAlreadyDetached);
        };
        if scope.same_scope(process) || !Arc::ptr_eq(&scope.runtime.shared, &process.runtime.shared)
        {
            return Err(ActorTurnError::PublicationTargetMismatch);
        }
        if inbox.failed
            || inbox.sealed
            || inbox.inputs.values().any(|input| {
                [input.author.as_ref(), input.producer.as_ref()]
                    .into_iter()
                    .flatten()
                    .any(|binding| binding.work.scope == scope.id())
                    || input.author.is_none()
            })
            || inbox.standing_inputs.values().any(|role| {
                role.producer
                    .as_ref()
                    .is_none_or(|binding| binding.work.scope == scope.id())
            })
            || inbox.batches.values().any(|batch| {
                batch.state.has_unfinished_author_scope(scope.id())
                    || batch.state.has_unfinished_business()
                    || !batch.reservations.is_empty()
            })
            || inbox.failure.is_some()
            || inbox.failure_input.is_some()
            || inbox.failure_publication.is_some()
            || inbox.timer_input.is_some()
        {
            return Err(ActorTurnError::ActorNotRetired);
        }
        let mut ledger = scope
            .runtime
            .shared
            .state
            .lock()
            .expect("runtime ledger lock");
        let original = ledger
            .scopes
            .get(&scope.id())
            .ok_or(RuntimeError::UnknownScope)?;
        if !original.join_ready()
            || original.storage_count() != 1
            || ledger.ledger.reservation_count_for(scope.id()) != 0
            || ledger
                .unresolved
                .range(
                    (scope.id(), WorkId::new(1).expect("minimum Work identity"))
                        ..=(
                            scope.id(),
                            WorkId::new(u64::MAX).expect("maximum Work identity"),
                        ),
                )
                .next()
                .is_some()
        {
            return Err(ActorTurnError::ActorNotRetired);
        }
        ledger
            .scopes
            .get(&process.id())
            .ok_or(RuntimeError::UnknownScope)?;
        let state = self.shared.state.lock().expect("runtime scheduler lock");
        if !state
            .actors
            .get(&node.id)
            .is_some_and(|actual| Arc::ptr_eq(actual, node))
            || !state.ready.is_idle(node.id)?
            || !state.effect_ready.is_claimed(node.id)?
        {
            return Err(ActorTurnError::ActorNotRetired);
        }
        ledger
            .scopes
            .get_mut(&scope.id())
            .expect("validated original scope")
            .retire_slot(node.attachment)
            .map_err(RuntimeError::from)?;
        scope.runtime.shared.record(|| HistoryFact::SlotRetired {
            slot: node.attachment,
        });
        // Original failure Work, holders, captures and reports are all gone.
        // Withdraw its unused builtin position without creating another Work.
        inbox.failure_position = None;
        // Actor epoch is independent of the payer Scope and survives detachment.
        drop(state);
        drop(ledger);
        *binding = ActorExecution::Retained {
            process: process.clone(),
        };
        drop(binding);
        drop(inbox);
        process.runtime.shared.changed.notify();
        self.shared.changed.notify();
        Ok(())
    }
    /// Internal facts wait on their original metadata object's class credit.
    /// Full registers one waiter atomically with the actual window check.
    pub fn forward_metadata_for(
        &self,
        source: &ActorRegistration,
        target: &ActorRegistration,
        ticket: &MetadataTicket<A::Effect>,
        class: InputClass,
        value: A::Input,
    ) -> Result<MetadataDelivery, (ActorTurnError, A::Input)> {
        let observed = self.shared.observation.now();
        if !matches!(class, InputClass::Control | InputClass::Lifecycle) {
            return Err((ActorTurnError::InvalidConfig, value));
        }
        let source_node = match self.node(source.actor) {
            Ok(node) => node,
            Err(error) => return Err((error, value)),
        };
        let target_node = match self.node(target.actor) {
            Ok(node) => node,
            Err(error) => return Err((error, value)),
        };
        let delivery = if source.actor == target.actor {
            let mut inbox = source_node.inbox.lock().expect("metadata actor inbox lock");
            if let Err(error) = Self::validate_metadata_source(&source_node, &inbox, source, ticket)
            {
                return Err((error, value));
            }
            let current = inbox.batches[&ticket.input].metadata[ticket.ordinal]
                .credit_waiter
                .clone();
            let waiter = match self.prepare_metadata_credit(
                (&target_node, &mut inbox, target),
                (source, ticket),
                class,
                current,
            ) {
                Ok(waiter) => waiter,
                Err(error) => return Err((error, value)),
            };
            inbox
                .batches
                .get_mut(&ticket.input)
                .expect("original metadata batch")
                .metadata[ticket.ordinal]
                .credit_waiter = Some(waiter.clone());
            if waiter.status() != MetadataCreditState::Ready {
                return Err((ActorTurnError::InputCapacity, value));
            }
            let delivery = match Self::insert_metadata_input(
                &target_node,
                &mut inbox,
                source,
                target,
                &waiter,
                observed,
                value,
            ) {
                Ok(delivery) => delivery,
                Err(error) => {
                    self.close_metadata_credit(&mut inbox, &waiter);
                    return Err(error);
                }
            };
            let notice = &mut inbox
                .batches
                .get_mut(&ticket.input)
                .expect("original metadata batch")
                .metadata[ticket.ordinal];
            notice.credit_waiter = None;
            notice.forwarded = Some(delivery.clone());
            delivery
        } else if source.actor < target.actor {
            let mut from = source_node
                .inbox
                .lock()
                .expect("source metadata inbox lock");
            let mut into = target_node
                .inbox
                .lock()
                .expect("target metadata inbox lock");
            self.forward_metadata_locked(
                (&source_node, &mut from, source),
                (&target_node, &mut into, target),
                ticket,
                class,
                observed,
                value,
            )?
        } else {
            let mut into = target_node
                .inbox
                .lock()
                .expect("target metadata inbox lock");
            let mut from = source_node
                .inbox
                .lock()
                .expect("source metadata inbox lock");
            self.forward_metadata_locked(
                (&source_node, &mut from, source),
                (&target_node, &mut into, target),
                ticket,
                class,
                observed,
                value,
            )?
        };
        self.shared
            .state
            .lock()
            .expect("runtime scheduler lock")
            .ready
            .notify(target.actor)
            .expect("forwarded input retains its original destination");
        self.shared.changed.notify();
        Ok(delivery)
    }
    fn validate_metadata_source(
        node: &ActorCell<A>,
        inbox: &Inbox<A>,
        source: &ActorRegistration,
        ticket: &MetadataTicket<A::Effect>,
    ) -> Result<(), ActorTurnError> {
        Self::validate_metadata_ticket_owner(node, inbox, source, ticket)?;
        if inbox.sealed {
            return Err(ActorTurnError::Closed);
        }
        // Failure cannot revoke an already committed original fact. Its real
        // host/failure custodian may finish this exact handoff without running
        // the failed Core, admitting Work or producing another metadata fact.
        let notice = &inbox.batches[&ticket.input].metadata[ticket.ordinal];
        if notice.forwarded.is_some() {
            return Err(ActorTurnError::MetadataAlreadyForwarded);
        }
        Ok(())
    }
    fn prepare_metadata_credit(
        &self,
        (node, inbox, target): (&ActorCell<A>, &mut Inbox<A>, &ActorRegistration),
        (source, ticket): (&ActorRegistration, &MetadataTicket<A::Effect>),
        class: InputClass,
        current: Option<MetadataCreditWait>,
    ) -> Result<MetadataCreditWait, ActorTurnError> {
        if node.id != target.actor
            || !Arc::ptr_eq(&node.origin, &target.origin)
            || !Arc::ptr_eq(&node.scope, &target.scope)
        {
            return Err(ActorTurnError::EffectTicketOriginMismatch);
        }
        let gate = node.scope.current_scope().gates()?;
        if inbox.failed || gate.failed || inbox.metadata_closing || inbox.sealed {
            self.close_metadata_waiters_locked(node, inbox);
            return Err(if inbox.failed || gate.failed {
                ActorTurnError::ActorFailed
            } else {
                ActorTurnError::Closed
            });
        }
        let waiter = if let Some(waiter) = current {
            if !waiter.target.same_registration(target) || waiter.class != class {
                return Err(ActorTurnError::PublicationTargetMismatch);
            }
            if !matches!(
                waiter.status(),
                MetadataCreditState::Waiting | MetadataCreditState::Ready
            ) {
                return Err(ActorTurnError::Closed);
            }
            waiter
        } else {
            let waiter = MetadataCreditWait {
                key: MetadataWaitKey {
                    actor: source.actor,
                    input: ticket.input,
                    ordinal: ticket.ordinal,
                },
                source: source.clone(),
                target: target.clone(),
                class,
                state: Arc::new(Mutex::new(MetadataCreditState::Waiting)),
            };
            inbox.metadata_credit[class.index()].register(waiter.clone());
            waiter
        };
        self.notify_metadata_credit_sources(inbox.grant_metadata_credit(class));
        Ok(waiter)
    }
    fn forward_metadata_locked(
        &self,
        (source_node, source_inbox, source): (&ActorCell<A>, &mut Inbox<A>, &ActorRegistration),
        (target_node, target_inbox, target): (&ActorCell<A>, &mut Inbox<A>, &ActorRegistration),
        ticket: &MetadataTicket<A::Effect>,
        class: InputClass,
        observed: Option<Tick>,
        value: A::Input,
    ) -> Result<MetadataDelivery, (ActorTurnError, A::Input)> {
        if let Err(error) =
            Self::validate_metadata_source(source_node, source_inbox, source, ticket)
        {
            return Err((error, value));
        }
        let current = source_inbox.batches[&ticket.input].metadata[ticket.ordinal]
            .credit_waiter
            .clone();
        let waiter = match self.prepare_metadata_credit(
            (target_node, target_inbox, target),
            (source, ticket),
            class,
            current,
        ) {
            Ok(waiter) => waiter,
            Err(error) => return Err((error, value)),
        };
        source_inbox
            .batches
            .get_mut(&ticket.input)
            .expect("original metadata batch")
            .metadata[ticket.ordinal]
            .credit_waiter = Some(waiter.clone());
        if waiter.status() != MetadataCreditState::Ready {
            return Err((ActorTurnError::InputCapacity, value));
        }
        let delivery = match Self::insert_metadata_input(
            target_node,
            target_inbox,
            source,
            target,
            &waiter,
            observed,
            value,
        ) {
            Ok(delivery) => delivery,
            Err(error) => {
                self.close_metadata_credit(target_inbox, &waiter);
                return Err(error);
            }
        };
        let notice = &mut source_inbox
            .batches
            .get_mut(&ticket.input)
            .expect("original metadata batch")
            .metadata[ticket.ordinal];
        notice.credit_waiter = None;
        notice.forwarded = Some(delivery.clone());
        Ok(delivery)
    }
    fn insert_metadata_input(
        node: &ActorCell<A>,
        inbox: &mut Inbox<A>,
        source: &ActorRegistration,
        target: &ActorRegistration,
        waiter: &MetadataCreditWait,
        observed: Option<Tick>,
        value: A::Input,
    ) -> Result<MetadataDelivery, (ActorTurnError, A::Input)> {
        if node.id != target.actor
            || !Arc::ptr_eq(&node.origin, &target.origin)
            || !Arc::ptr_eq(&node.scope, &target.scope)
        {
            return Err((ActorTurnError::EffectTicketOriginMismatch, value));
        }
        let scope = node.scope.current_scope();
        let ledger = scope
            .runtime
            .shared
            .state
            .lock()
            .expect("metadata target ledger lock");
        let owner = match ledger.scopes.get(&scope.id()) {
            Some(owner) => owner,
            None => return Err((RuntimeError::UnknownScope.into(), value)),
        };
        if inbox.failed || owner.failed() {
            return Err((ActorTurnError::ActorFailed, value));
        }
        if inbox.sealed || inbox.metadata_closing {
            return Err((ActorTurnError::Closed, value));
        }
        let class = waiter.class;
        let original = inbox.metadata_credit[class.index()]
            .entries
            .get(&waiter.key);
        if !original.is_some_and(|original| original.same_wait(waiter))
            || waiter.status() != MetadataCreditState::Ready
        {
            return Err((ActorTurnError::InputCapacity, value));
        }
        let Some(next) = inbox.next_input.checked_add(1) else {
            return Err((ActorTurnError::IdentityExhausted, value));
        };
        let input = InputId::new(next).expect("incremented actual metadata input");
        if let Err(error) = inbox.mailbox.push(class, source.actor, input) {
            return Err((error.into(), value));
        }
        assert!(
            inbox.metadata_credit[class.index()].remove(waiter, MetadataCreditState::Forwarded)
        );
        let delivery = MetadataDelivery {
            source: source.clone(),
            target: target.clone(),
            input,
            state: Arc::new(Mutex::new(MetadataConsumption::Pending)),
        };
        inbox.next_input = next;
        inbox.inputs.insert(
            input,
            InputRecord {
                source: source.actor,
                class,
                phase: InputPhase::Filled,
                standing_input_id: None,
                author: None,
                producer: None,
                metadata_delivery: Some(delivery.clone()),
                header_pins: Vec::new(),
                handoff_at: observed,
            },
        );
        inbox.payloads.insert(input, value);
        inbox.counts[class.index()] += 1;
        inbox.observation.input_credit_taken(class);
        Ok(delivery)
    }
    /// The original input's last tail may free max_batches without a new
    /// message. Restore eligible Core readiness as well as producer credit;
    /// the caller wakes pollers only after dropping inbox/ledger locks.
    fn retire_metadata_input_and_wake(
        &self,
        node: &ActorCell<A>,
        inbox: &mut Inbox<A>,
        input: InputId,
        ledger: &mut crate::transaction::LedgerState,
    ) {
        let existed = inbox.inputs.contains_key(&input);
        let credit = inbox.retire_metadata_input_if_ready(input, ledger);
        self.notify_metadata_credit_sources(credit);
        if existed
            && !inbox.inputs.contains_key(&input)
            && !inbox.failed
            && !inbox.sealed
            && inbox.batches.len() < node.config.max_batches
            && inbox.has_selectable_input(node.id)
        {
            self.shared
                .state
                .lock()
                .expect("runtime scheduler lock")
                .ready
                .notify(node.id)
                .expect("the locked original actor remains registered");
        }
    }
    fn notify_metadata_credit_sources(&self, sources: Vec<ActorRegistration>) {
        for source in sources {
            self.notify_metadata_source(&source);
        }
    }
    /// Consumption and closed-credit state are published before their wake.
    /// An actual custodian can observe that state, settle the original fact
    /// and retire its actor before this notification reaches the scheduler.
    /// The state remains authoritative; this hint must never wake a later
    /// incarnation or keep an already retired actor artificially alive.
    fn notify_metadata_source(&self, source: &ActorRegistration) {
        let mut state = self.shared.state.lock().expect("runtime scheduler lock");
        match Self::validate_registration_in_state(&state, source) {
            Ok(()) => {}
            Err(ActorTurnError::UnknownActor | ActorTurnError::EffectTicketOriginMismatch) => {
                return;
            }
            Err(error) => panic!("metadata source registration invariant: {error:?}"),
        }
        state
            .effect_ready
            .notify(source.actor)
            .expect("the same registered metadata source retains its ready position");
        drop(state);
        self.shared.changed.notify();
    }
    fn close_metadata_credit(&self, inbox: &mut Inbox<A>, waiter: &MetadataCreditWait) {
        if inbox.metadata_credit[waiter.class.index()].remove(waiter, MetadataCreditState::Closed) {
            self.notify_metadata_credit_sources(vec![waiter.source.clone()]);
        }
        inbox.metadata_closing = true;
    }
    fn close_metadata_waiters_locked(&self, node: &ActorCell<A>, inbox: &mut Inbox<A>) -> bool {
        let (wakes, remains) = inbox.close_metadata_waiters(METADATA_CREDIT_WAKE_BUDGET);
        self.notify_metadata_credit_sources(wakes);
        if remains {
            self.shared
                .state
                .lock()
                .expect("runtime scheduler lock")
                .effect_ready
                .notify(node.id)
                .expect("closing metadata destination remains registered");
        }
        remains
    }
    pub fn metadata_waiting_for(
        &self,
        source: &ActorRegistration,
        ticket: &MetadataTicket<A::Effect>,
    ) -> Result<bool, ActorTurnError> {
        let node = self.node(source.actor)?;
        let inbox = node.inbox.lock().expect("source metadata inbox lock");
        Self::validate_metadata_ticket_owner(&node, &inbox, source, ticket)?;
        Ok(inbox.batches[&ticket.input].metadata[ticket.ordinal]
            .credit_waiter
            .as_ref()
            .is_some_and(|waiter| waiter.status() == MetadataCreditState::Waiting))
    }
    fn validate_metadata_ticket_owner(
        node: &ActorCell<A>,
        inbox: &Inbox<A>,
        source: &ActorRegistration,
        ticket: &MetadataTicket<A::Effect>,
    ) -> Result<(), ActorTurnError> {
        if ticket.actor != source.actor || !Arc::ptr_eq(&ticket.origin, &node.origin) {
            return Err(ActorTurnError::EffectTicketOriginMismatch);
        }
        Self::validate_metadata_key_owner(node, inbox, source, (ticket.input, ticket.ordinal))
    }
    fn validate_metadata_key_owner(
        node: &ActorCell<A>,
        inbox: &Inbox<A>,
        source: &ActorRegistration,
        (input, ordinal): (InputId, usize),
    ) -> Result<(), ActorTurnError> {
        if node.id != source.actor
            || !Arc::ptr_eq(&source.origin, &node.origin)
            || !Arc::ptr_eq(&source.scope, &node.scope)
        {
            return Err(ActorTurnError::EffectTicketOriginMismatch);
        }
        let original = inbox
            .inputs
            .get(&input)
            .ok_or(ActorTurnError::UnknownInput)?;
        let batch = inbox
            .batches
            .get(&input)
            .ok_or(ActorTurnError::UnknownInput)?;
        if !matches!(
            original.phase,
            InputPhase::Committed | InputPhase::AbandonedByOwnerFailure
        ) || batch.state.phase() != novarocks_control_runtime_core::effect::BatchPhase::Committed
        {
            return Err(EffectError::NotCommitted.into());
        }
        let notice = batch
            .metadata
            .get(ordinal)
            .ok_or(ActorTurnError::UnknownInput)?;
        if notice.completed || notice.value.is_some() {
            return Err(EffectError::InvalidTransition.into());
        }
        Ok(())
    }
    /// Cancels only the scheduler wait. The original fact/ticket is neither
    /// consumed nor acknowledged; failure/shutdown must still settle its owner.
    pub fn cancel_metadata_wait_for(
        &self,
        source: &ActorRegistration,
        ticket: &MetadataTicket<A::Effect>,
    ) -> Result<bool, ActorTurnError> {
        let node = self.node(source.actor)?;
        let waiter = {
            let mut inbox = node.inbox.lock().expect("source metadata inbox lock");
            Self::validate_metadata_ticket_owner(&node, &inbox, source, ticket)?;
            let notice = &mut inbox
                .batches
                .get_mut(&ticket.input)
                .expect("original metadata batch")
                .metadata[ticket.ordinal];
            let Some(waiter) = notice.credit_waiter.clone() else {
                return Ok(false);
            };
            if !matches!(
                waiter.status(),
                MetadataCreditState::Waiting | MetadataCreditState::Ready
            ) {
                notice.credit_waiter = None;
                return Ok(true);
            }
            waiter
        };
        let target = match self.node(waiter.target.actor) {
            Ok(target) => target,
            Err(error) => {
                // Actual destination removal closes each original scheduling
                // link before removing its actor. Only that positive state can
                // justify clearing a link after the address disappeared.
                if waiter.status() != MetadataCreditState::Closed {
                    return Err(error);
                }
                let mut inbox = node.inbox.lock().expect("source metadata inbox lock");
                Self::validate_metadata_ticket_owner(&node, &inbox, source, ticket)?;
                let notice = &mut inbox
                    .batches
                    .get_mut(&ticket.input)
                    .expect("original metadata batch")
                    .metadata[ticket.ordinal];
                if notice
                    .credit_waiter
                    .as_ref()
                    .is_some_and(|original| original.same_wait(&waiter))
                {
                    notice.credit_waiter = None;
                }
                return Ok(true);
            }
        };
        if source.actor == target.id {
            let mut inbox = node.inbox.lock().expect("metadata actor inbox lock");
            Self::validate_metadata_ticket_owner(&node, &inbox, source, ticket)?;
            let matches = inbox.batches[&ticket.input].metadata[ticket.ordinal]
                .credit_waiter
                .as_ref()
                .is_some_and(|original| original.same_wait(&waiter));
            if !matches {
                return Ok(false);
            }
            if waiter.status() != MetadataCreditState::Closed {
                if !Arc::ptr_eq(&target.origin, &waiter.target.origin) {
                    return Err(ActorTurnError::EffectTicketOriginMismatch);
                }
                inbox.metadata_credit[waiter.class.index()]
                    .remove(&waiter, MetadataCreditState::Cancelled);
            }
            inbox
                .batches
                .get_mut(&ticket.input)
                .expect("original metadata batch")
                .metadata[ticket.ordinal]
                .credit_waiter = None;
            self.notify_metadata_credit_sources(inbox.grant_metadata_credit(waiter.class));
            Ok(true)
        } else if source.actor < target.id {
            let mut from = node.inbox.lock().expect("source metadata inbox lock");
            let mut into = target.inbox.lock().expect("target metadata inbox lock");
            self.cancel_metadata_wait_locked(
                (&node, &mut from, source),
                (&target, &mut into),
                (ticket.input, ticket.ordinal),
                &waiter,
            )
        } else {
            let mut into = target.inbox.lock().expect("target metadata inbox lock");
            let mut from = node.inbox.lock().expect("source metadata inbox lock");
            self.cancel_metadata_wait_locked(
                (&node, &mut from, source),
                (&target, &mut into),
                (ticket.input, ticket.ordinal),
                &waiter,
            )
        }
    }
    fn cancel_metadata_wait_locked(
        &self,
        (source_node, source_inbox, source): (&ActorCell<A>, &mut Inbox<A>, &ActorRegistration),
        (target_node, target_inbox): (&ActorCell<A>, &mut Inbox<A>),
        (input, ordinal): (InputId, usize),
        waiter: &MetadataCreditWait,
    ) -> Result<bool, ActorTurnError> {
        Self::validate_metadata_key_owner(source_node, source_inbox, source, (input, ordinal))?;
        let notice = &mut source_inbox
            .batches
            .get_mut(&input)
            .expect("original metadata batch")
            .metadata[ordinal];
        if !notice
            .credit_waiter
            .as_ref()
            .is_some_and(|original| original.same_wait(waiter))
        {
            return Ok(false);
        }
        // Destination close already removed this exact original wait. Its
        // numeric actor may now name a replacement; never inspect or mutate
        // that replacement's queue to finish the source's closed link.
        if waiter.status() == MetadataCreditState::Closed {
            notice.credit_waiter = None;
            return Ok(true);
        }
        if !Arc::ptr_eq(&target_node.origin, &waiter.target.origin)
            || !Arc::ptr_eq(&target_node.scope, &waiter.target.scope)
        {
            return Err(ActorTurnError::EffectTicketOriginMismatch);
        }
        target_inbox.metadata_credit[waiter.class.index()]
            .remove(waiter, MetadataCreditState::Cancelled);
        notice.credit_waiter = None;
        self.notify_metadata_credit_sources(target_inbox.grant_metadata_credit(waiter.class));
        Ok(true)
    }
    /// Recover the same read-only delivery after an interrupted host turn;
    /// this issues neither another forwarding nor another input position.
    pub fn metadata_delivery_for(
        &self,
        source: &ActorRegistration,
        ticket: &MetadataTicket<A::Effect>,
    ) -> Result<Option<MetadataDelivery>, ActorTurnError> {
        self.validate_registration(source)?;
        let node = self.node(source.actor)?;
        if ticket.actor != source.actor || !Arc::ptr_eq(&ticket.origin, &node.origin) {
            return Err(ActorTurnError::EffectTicketOriginMismatch);
        }
        let inbox = node.inbox.lock().expect("source metadata inbox lock");
        let notice = inbox
            .batches
            .get(&ticket.input)
            .ok_or(ActorTurnError::UnknownInput)?
            .metadata
            .get(ticket.ordinal)
            .ok_or(ActorTurnError::UnknownInput)?;
        Ok(notice.forwarded.clone())
    }
    fn resolve_metadata_delivery(
        &self,
        inbox: &Inbox<A>,
        input: InputId,
        next: MetadataConsumption,
    ) {
        if let Some(delivery) = inbox
            .inputs
            .get(&input)
            .and_then(|record| record.metadata_delivery.as_ref())
            && delivery.resolve(next)
        {
            self.notify_metadata_source(&delivery.source);
        }
    }
    /// Transfer this same original fact to its actual failure custodian. The
    /// receiver must retain and settle the returned value under the existing
    /// failure policy; this is neither successful Core consumption nor physical
    /// exit. A live wait must first be cancelled through its original queue.
    /// Forwarded facts require the actual target's Abandoned receipt. An
    /// unforwarded fact requires this original source to have actually failed.
    pub fn take_failed_metadata_for(
        &self,
        registration: &ActorRegistration,
        ticket: MetadataTicket<A::Effect>,
    ) -> Result<A::Effect, (ActorTurnError, MetadataTicket<A::Effect>)> {
        let transfer = || -> Result<(), ActorTurnError> {
            let node = self.node(registration.actor)?;
            let mut inbox = node
                .inbox
                .lock()
                .expect("failed metadata custody inbox lock");
            Self::validate_metadata_ticket_owner(&node, &inbox, registration, &ticket)?;
            let source_failed = inbox.failed;
            let notice = &mut inbox
                .batches
                .get_mut(&ticket.input)
                .expect("qualified original committed metadata")
                .metadata[ticket.ordinal];
            if notice.credit_waiter.is_some() {
                return Err(ActorTurnError::MetadataStillPending);
            }
            match notice.forwarded.as_ref().map(MetadataDelivery::consumption) {
                Some(MetadataConsumption::AbandonedByOwnerFailure) => {}
                Some(MetadataConsumption::Pending) => {
                    return Err(ActorTurnError::MetadataStillPending);
                }
                Some(MetadataConsumption::Consumed) => {
                    return Err(EffectError::InvalidTransition.into());
                }
                None if source_failed => {}
                None => return Err(ActorTurnError::ActorNotRetired),
            }
            // The unique ticket already owns the actual value. Only terminal
            // source custody changes here; no target receipt becomes Consumed.
            notice.completed = true;
            {
                let scope = node.scope.current_scope();
                let mut ledger = scope
                    .runtime
                    .shared
                    .state
                    .lock()
                    .expect("original metadata tail header custody");
                self.retire_metadata_input_and_wake(&node, &mut inbox, ticket.input, &mut ledger);
            }
            self.shared
                .state
                .lock()
                .expect("runtime scheduler lock")
                .effect_ready
                .notify(node.id)?;
            Ok(())
        };
        match transfer() {
            Ok(()) => {
                self.shared.changed.notify();
                Ok(ticket.value)
            }
            Err(error) => Err((error, ticket)),
        }
    }
    /// The actual destination consumed this original neutral notice. Refusal
    /// returns the same ticket; taking or dropping it is never completion.
    pub fn metadata_completed_for(
        &self,
        registration: &ActorRegistration,
        ticket: MetadataTicket<A::Effect>,
    ) -> Result<(), (ActorTurnError, MetadataTicket<A::Effect>)> {
        let complete = || -> Result<(), ActorTurnError> {
            self.validate_registration(registration)?;
            let node = self.node(registration.actor)?;
            if ticket.actor != node.id || !Arc::ptr_eq(&ticket.origin, &node.origin) {
                return Err(ActorTurnError::EffectTicketOriginMismatch);
            }
            let mut inbox = node.inbox.lock().expect("actor inbox lock");
            let notice = inbox
                .batches
                .get_mut(&ticket.input)
                .ok_or(ActorTurnError::UnknownInput)?
                .metadata
                .get_mut(ticket.ordinal)
                .ok_or(ActorTurnError::UnknownInput)?;
            if notice.completed || notice.value.is_some() {
                return Err(EffectError::InvalidTransition.into());
            }
            if notice.credit_waiter.is_some() {
                return Err(ActorTurnError::MetadataStillPending);
            }
            if let Some(delivery) = &notice.forwarded {
                match delivery.consumption() {
                    MetadataConsumption::Pending => {
                        return Err(ActorTurnError::MetadataStillPending);
                    }
                    MetadataConsumption::AbandonedByOwnerFailure => {
                        return Err(ActorTurnError::MetadataDeliveryAbandoned);
                    }
                    MetadataConsumption::Consumed => {}
                }
            }
            notice.completed = true;
            {
                let scope = node.scope.current_scope();
                let mut ledger = scope
                    .runtime
                    .shared
                    .state
                    .lock()
                    .expect("original metadata tail header custody");
                self.retire_metadata_input_and_wake(&node, &mut inbox, ticket.input, &mut ledger);
            }
            self.shared
                .state
                .lock()
                .expect("runtime scheduler lock")
                .effect_ready
                .notify(node.id)?;
            Ok(())
        };
        match complete() {
            Ok(()) => {
                self.shared.changed.notify();
                Ok(())
            }
            Err(error) => Err((error, ticket)),
        }
    }
    pub fn is_empty(&self) -> bool {
        let empty = self
            .shared
            .state
            .lock()
            .expect("runtime scheduler lock")
            .actors
            .is_empty();
        empty && self.shared.header_readiness.is_empty()
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
    observation: PollerObservation,
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
    #[expect(
        clippy::disallowed_methods,
        reason = "The control-runtime poller service owns and joins its original worker threads; see ADR-0218."
    )]
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
        let observation = PollerObservation::new(workers)?;
        let stop = Arc::new(crate::sync::AtomicBool::new(false));
        let host = Arc::new(host);
        let mut threads = Vec::new();
        for index in 0..workers {
            let worker_runtime = runtime.clone();
            let worker_stop = stop.clone();
            let worker_clock = clock.clone();
            let worker_host = host.clone();
            let worker_observation = observation.writer(index);
            let worker = std::thread::Builder::new()
                .name(format!("control-poller-{index}"))
                .spawn(move || {
                    let result = catch_unwind(AssertUnwindSafe(|| {
                        while !worker_stop.load(crate::sync::Ordering::Acquire) {
                            worker_observation.enter(PollerPhase::Timer, worker_clock.now());
                            let version = worker_runtime.change_version();
                            let now = worker_clock.now();
                            let mut timer_scan = None;
                            worker_runtime
                                .advance_time_sampled(now, 64, &mut timer_scan)
                                .unwrap_or_else(|error| {
                                    eprintln!("Control poller timer advance failed: {error:?}");
                                    std::process::abort();
                                });
                            if let Some(scan) = timer_scan {
                                worker_observation.timer(scan);
                            }
                            worker_observation.complete(worker_clock.now());
                            worker_observation.enter(PollerPhase::Core, worker_clock.now());
                            let core_progressed = worker_runtime
                                .poll_one_with_clock(worker_clock.as_ref())
                                .unwrap_or_else(|error| {
                                    eprintln!("Control poller Core turn failed: {error:?}");
                                    std::process::abort();
                                });
                            worker_observation.complete(worker_clock.now());
                            worker_observation.enter(PollerPhase::Host, worker_clock.now());
                            // A continuously ready protocol actor must not starve
                            // host handoffs or retirement. Each queue gets one
                            // finite opportunity before another turn begins.
                            let host_progressed = worker_host(&worker_runtime, worker_clock.now());
                            worker_observation.complete(worker_clock.now());
                            worker_observation.iteration(core_progressed, host_progressed);
                            if core_progressed || host_progressed {
                                continue;
                            }
                            if worker_stop.load(crate::sync::Ordering::Acquire) {
                                break;
                            }
                            worker_observation.enter(PollerPhase::Timer, worker_clock.now());
                            let deadline = worker_runtime.next_deadline();
                            worker_observation.complete(worker_clock.now());
                            worker_observation.enter(PollerPhase::Idle, worker_clock.now());
                            if let Some(deadline) = deadline {
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
                        worker_observation.enter(PollerPhase::Exited, worker_clock.now());
                        worker_observation.complete(worker_clock.now());
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
            observation,
            stop,
            workers: threads,
            stopped: false,
        })
    }
    /// The original pool's neutral metadata only, without stop/join authority.
    pub fn observation(&self) -> PollerObservation {
        self.observation.clone()
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
#[path = "actor_turn/registration_tests.rs"]
mod registration_tests;

#[cfg(all(test, not(loom)))]
mod effect_ticket_incarnation_tests {
    use super::*;
    use crate::transaction::RuntimeLedger;
    use novarocks_control_runtime_core::{
        capacity::{ReservationResources, ResourceVector},
        identity::ScopeId,
        owned_storage::OwnedSlotNeed,
        scope::WorkAdmission,
    };

    #[derive(Debug)]
    struct OneEffect(ScopedWorkRef);
    impl Actor for OneEffect {
        type Input = ();
        type Effect = ();
        fn timer_input(_now: Tick) {}
        fn handle(
            &mut self,
            _now: Tick,
            _input: InputEnvelope,
            _value: (),
            step: &mut ActorStep<'_, ()>,
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
            _step: &mut ActorStep<'_, ()>,
        ) {
        }
    }
    #[test]
    fn actual_actor_re_registration_mints_a_fresh_ticket_incarnation() {
        let ledger = RuntimeLedger::new(ResourceVector::new([]).unwrap());
        let runtime = ActorRuntime::new();
        let actor = ActorId::new(1).unwrap();
        let generation = Generation::new(1).unwrap();
        let mut previous: Option<Arc<()>> = None;
        let mut fresh = true;
        for _ in 0..2 {
            let scope = ledger.open_scope_next(None, generation).unwrap();
            let (mut work, mut capacity) = scope
                .reserve_next(
                    generation,
                    ReservationResources {
                        keyed: None,
                        quota: ResourceVector::new([]).unwrap(),
                        owned_slots: OwnedSlotNeed {
                            work_headers: 1,
                            ..OwnedSlotNeed::default()
                        },
                    },
                    WorkKind::Business,
                    WorkAdmission::External,
                )
                .unwrap()
                .bind(WorkKind::Business)
                .unwrap();
            let mut header_ready = work.header_readiness_registration().unwrap();
            assert!(work.header_readiness_registration().is_err());
            let header = work.retirement_observer();
            work.start().unwrap();
            runtime
                .register_next(
                    actor,
                    scope.clone(),
                    OneEffect(work.reference()),
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
                .reserve_admitted_input(actor, actor, InputClass::Ordinary, &work)
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
            let registration = runtime.registration_from_effect(&ticket).unwrap();
            runtime.effect_completed(ticket).unwrap();
            scope.close_external().unwrap();
            work.body_exited().unwrap();
            work.captures_exited().unwrap();
            // The real admitted input still owns its original producer pin.
            assert!(header.header_holders().unwrap().0 > 0);
            assert_eq!(
                header_ready.arm(&runtime, &registration, &header).unwrap(),
                crate::header_readiness::HeaderWaitState::Waiting,
            );
            assert!(!ledger.reap_observed_header(&header).unwrap());
            let turn = runtime.claim_host_actor().unwrap();
            assert!(turn.take_header_readiness(1).is_empty());
            turn.release(false).unwrap();
            assert!(runtime.claim_host_actor().is_none());
            runtime.retire_input(actor, input).unwrap();
            let turn = runtime.claim_host_actor().unwrap();
            let notices = turn.take_header_readiness(1);
            assert_eq!(notices.len(), 1);
            assert!(header_ready.matches(&notices[0]));
            assert_eq!(notices[0].work(), work.reference());
            assert!(!header.is_retired());
            assert!(turn.take_header_readiness(1).is_empty());
            assert_eq!(
                header_ready.arm(&runtime, &registration, &header).unwrap(),
                crate::header_readiness::HeaderWaitState::ReadyToReap,
            );
            assert!(ledger.reap_observed_header(&header).unwrap());
            assert!(header.is_retired());
            assert!(matches!(
                runtime.remove_actor(actor),
                Err(ActorTurnError::ActorNotRetired)
            ));
            // Cancel withdraws the same newly queued ready/retired notice;
            // it does not stand in for input exit or header retirement.
            header_ready.cancel();
            assert!(!header_ready.matches(&notices[0]));
            turn.release(false).unwrap();
            while let Some(original_host_turn) = runtime.claim_host_actor() {
                original_host_turn.release(false).unwrap();
            }
            runtime.remove_actor(actor).unwrap();
            capacity.finish().unwrap();
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

#[cfg(all(test, not(loom)))]
mod effect_observation_tests {
    use super::*;
    use crate::observation::EffectObservationError;
    use crate::transaction::RuntimeLedger;
    use novarocks_control_runtime_core::capacity::ResourceVector;
    struct Empty;
    impl Actor for Empty {
        type Input = ();
        type Effect = ();
        fn timer_input(_: Tick) {}
        fn next_deadline(&self) -> Option<Tick> {
            None
        }
        fn handle(&mut self, _: Tick, _: InputEnvelope, _: (), _: &mut ActorStep<'_, ()>) {}
        fn owner_failed(_: ScopeId, _: Generation, _: InputEnvelope, _: &mut ActorStep<'_, ()>) {}
    }
    #[test]
    fn pending_observation_is_unavailable_on_scheduler_or_inbox_contention() {
        let ledger = RuntimeLedger::new(ResourceVector::default());
        let scope = ledger
            .open_scope_next(None, Generation::new(1).unwrap())
            .unwrap();
        let runtime = ActorRuntime::new();
        let registration = runtime
            .register_bound(
                ActorId::new(1).unwrap(),
                scope.clone(),
                Empty,
                ActorConfig {
                    mailbox: [1; 4],
                    max_batches: 1,
                    max_effects_per_step: 1,
                    max_dependencies_per_effect: 0,
                    turn_budget: 8,
                    max_step_work: 1,
                },
            )
            .unwrap_or_else(|_| panic!("original actor admission"));
        let before = scope.snapshot().unwrap();
        let refused = runtime.register_bound(
            registration.actor(),
            scope.clone(),
            Empty,
            ActorConfig {
                mailbox: [1; 4],
                max_batches: 1,
                max_effects_per_step: 1,
                max_dependencies_per_effect: 0,
                turn_budget: 8,
                max_step_work: 1,
            },
        );
        assert!(
            refused.is_err(),
            "duplicate admission returns only its original actor"
        );
        assert_eq!(
            scope.snapshot().unwrap().storage,
            before.storage,
            "failed admission rolls back its actual attachment"
        );
        let lock = runtime.shared.state.lock().unwrap();
        assert_eq!(
            runtime
                .try_effect_inventory_observation(&registration, None)
                .unwrap(),
            None
        );
        drop(lock);
        let node = runtime.node(registration.actor()).unwrap();
        let lock = node.inbox.lock().unwrap();
        assert_eq!(
            runtime
                .try_effect_inventory_observation(&registration, None)
                .unwrap(),
            None
        );
        drop(lock);
        let sample = runtime
            .try_effect_inventory_observation(&registration, None)
            .unwrap()
            .unwrap();
        assert_eq!(sample.batches, 0);
        assert_eq!(sample.states.pending, 0);
        scope.close_external().unwrap();
        assert!(runtime.remove_actor(registration.actor()).is_ok());
        assert_eq!(scope.snapshot().unwrap().storage, 0);
        runtime.close_admission();
        let other = ledger
            .open_scope_next(None, Generation::new(1).unwrap())
            .unwrap();
        assert!(
            runtime
                .register_bound(
                    registration.actor(),
                    other.clone(),
                    Empty,
                    ActorConfig {
                        mailbox: [1; 4],
                        max_batches: 1,
                        max_effects_per_step: 1,
                        max_dependencies_per_effect: 0,
                        turn_budget: 8,
                        max_step_work: 1
                    }
                )
                .is_err()
        );
        assert_eq!(other.snapshot().unwrap().storage, 0);
        other.close_external().unwrap();
        ledger.remove_scope(other.id()).unwrap();
        ledger.remove_scope(scope.id()).unwrap();
        assert!(matches!(
            runtime.try_effect_inventory_observation(&registration, None),
            Err(EffectObservationError::Runtime(
                ActorTurnError::UnknownActor
            ))
        ));
    }
}
