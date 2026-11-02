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

//! A concrete once-only Task host call. It carries the original reported
//! cleanup Work and its actual bound Header/report/capture positions. Foreign
//! code runs only on its physical host runner; queue return never signs exit.

use super::{
    actors::{WorkerAction, WorkerActor},
    adopted_cleanup::AdoptedCleanupEffect,
    failure::ScopePublicationsDrained,
    preparation::QueuedPreparation,
    request_endpoint::RequestNotice,
};
use crate::{
    control_runtime_policy::StorageNeed,
    task_port::{TaskPendingNotice, TaskRuntimePort},
};
use novarocks_control_runtime::{
    completion::ReportSlot,
    driver::{ActorRuntime, DriverError, EffectTicket},
    exit_role::{ExitAuthority, ExitRole, ExitRoleError, ExitRolePhase},
    lease::WorkRetirementObserver,
    owned_return::{OwnedReturnReader, OwnedReturnSender},
    reported_work::{ReportedWork, ReportedWorkBindingError},
    transaction::{RuntimeError, RuntimeLedger, ScopeHandle},
};
use novarocks_control_runtime_core::{
    identity::{ActorId, ConsumerId, Generation, ScopeId, WorkId},
    owned_storage::OwnedSlotNeed,
    scope::ScopeError,
};
use novarocks_execution::runtime::execution_health::{ExecutionExitOrigin, ExecutionHealth};
use novarocks_execution_contract::{TaskFailurePhase, TaskIdentity};
use novarocks_worker_protocol::context::ContextEffect;
use std::{
    any::Any,
    mem::ManuallyDrop,
    panic::{AssertUnwindSafe, catch_unwind},
    sync::{Arc, Mutex, Weak},
};

pub(crate) const HOST_CALL_STORAGE: StorageNeed = StorageNeed {
    requests: 0,
    inputs: 0,
    batches: 0,
    effects: 0,
    cuts: 0,
    replies: 0,
    completions: 1,
    captures: 1,
    work: 1,
};
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum TaskHostCallKind {
    Stop,
    RetireBorrowing,
}
pub(crate) const HOST_CALL_KINDS: [TaskHostCallKind; 2] =
    [TaskHostCallKind::Stop, TaskHostCallKind::RetireBorrowing];
#[derive(Clone, Debug)]
pub(crate) enum TaskHostCallOutcome {
    Completed,
    Panicked,
    /// The checked cleanup returned its typed refusal; body/captures exited.
    CleanupRefused,
    /// A cleanup invocation or capture failed to establish actual exit.
    CleanupUnresolved,
    StartRefused(ExitRoleError),
}

impl std::fmt::Display for TaskHostCallOutcome {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Completed => f.write_str("Task host call completed"),
            Self::Panicked => f.write_str("Task host call panicked"),
            Self::CleanupRefused => f.write_str("Task host cleanup returned a typed refusal"),
            Self::CleanupUnresolved => f.write_str("Task host cleanup actual exit is unresolved"),
            Self::StartRefused(error) => write!(f, "Task host call start was refused: {error}"),
        }
    }
}

#[derive(Debug)]
pub(crate) enum TaskHostCallError {
    Runtime(RuntimeError),
    Driver(DriverError),
    Exit(ExitRoleError),
    WrongScope,
    WrongTask,
    WrongStorage,
    StorageBinding(ReportedWorkBindingError),
    AlreadyIssued,
    Withdrawn,
    AwaitingFailurePublication,
    BorrowerExitRequired,
    AlreadyCaptured,
    PortAlreadyAttached,
    MissingPort,
    MissingConclusion,
}
impl std::fmt::Display for TaskHostCallError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Runtime(error) => write!(f, "Task host call runtime failure: {error}"),
            Self::Driver(error) => write!(f, "Task host call driver failure: {error}"),
            Self::Exit(error) => write!(f, "Task host call exit failure: {error}"),
            Self::WrongScope => f.write_str("Task host call has a different original scope"),
            Self::WrongTask => f.write_str("Task host call names a different Task"),
            Self::WrongStorage => {
                f.write_str("Task host call has a different original storage profile")
            }
            Self::StorageBinding(error) => {
                write!(f, "Task host call storage binding failed: {error}")
            }
            Self::AlreadyIssued => f.write_str("Task host call was already issued"),
            Self::Withdrawn => f.write_str("Task host call was withdrawn before issuance"),
            Self::AwaitingFailurePublication => {
                f.write_str("Task host call requires the actual owner failure publication")
            }
            Self::BorrowerExitRequired => {
                f.write_str("Task host call requires actual execution borrower exit")
            }
            Self::AlreadyCaptured => {
                f.write_str("Task host call already retains its original capture")
            }
            Self::PortAlreadyAttached => {
                f.write_str("Task host call already has its original port")
            }
            Self::MissingPort => f.write_str("Task host call has no attached original port"),
            Self::MissingConclusion => f.write_str("Task host stop has no published conclusion"),
        }
    }
}
impl std::error::Error for TaskHostCallError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Runtime(error) => Some(error),
            Self::Driver(error) => Some(error),
            Self::Exit(error) => Some(error),
            Self::StorageBinding(error) => Some(error),
            _ => None,
        }
    }
}

impl From<RuntimeError> for TaskHostCallError {
    fn from(error: RuntimeError) -> Self {
        Self::Runtime(error)
    }
}
impl From<DriverError> for TaskHostCallError {
    fn from(error: DriverError) -> Self {
        Self::Driver(error)
    }
}
impl From<ExitRoleError> for TaskHostCallError {
    fn from(error: ExitRoleError) -> Self {
        Self::Exit(error)
    }
}

/// The already elected Task owns these original host roles before its port
/// exists. This is custody, never permission to execute an unbound callback.
pub(crate) struct TaskHostCallOwner {
    pub kind: TaskHostCallKind,
    pub runtime: ActorRuntime<WorkerActor>,
    pub actor: ActorId,
    pub task: TaskIdentity,
    pub pending: Arc<dyn TaskPendingNotice>,
    pub health: Arc<ExecutionHealth>,
}
#[must_use = "The same unbound reported role and owner require disposition"]
pub(crate) struct TaskHostCallUnboundRefusal {
    pub error: TaskHostCallError,
    pub _original_owner: TaskHostCallOwner,
    pub original: ReportedWork,
}
impl std::fmt::Debug for TaskHostCallUnboundRefusal {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TaskHostCallUnboundRefusal")
            .field("error", &self.error)
            .field("original", &self.original)
            .finish_non_exhaustive()
    }
}
type PanicPayload = ManuallyDrop<Box<dyn Any + Send>>;
#[derive(Clone, Copy, Eq, PartialEq)]
enum CallHandoff {
    Available,
    Issued,
    AwaitingFailurePublication,
    Withdrawn,
}
/// This is an additional field of the same original call capture, not a
/// second Work/return author. It remains with that custody on failure.
struct ExecutionRetirementCall {
    // None is issued only from the actual never-exposed RetireTask branch.
    owner: Option<Arc<super::task::ProductionPreparation>>,
    host: Arc<dyn crate::TaskExecutionHost>,
    ingress: crate::TaskIngressRetirement,
}
struct State {
    port: Option<Weak<TaskRuntimePort>>,
    authority: Option<ExitAuthority>,
    sender: Option<OwnedReturnSender<TaskHostCallOutcome>>,
    reader: OwnedReturnReader<TaskHostCallOutcome>,
    returned: Option<TaskHostCallOutcome>,
    capture: Option<ManuallyDrop<Arc<TaskRuntimePort>>>,
    retirement: Option<ManuallyDrop<ExecutionRetirementCall>>,
    panic: Option<PanicPayload>,
    handoff: CallHandoff,
    observer_consumed: bool,
    retired: bool,
    effect: Option<AdoptedCleanupEffect>,
}
struct Custody {
    kind: TaskHostCallKind,
    runtime: ActorRuntime<WorkerActor>,
    actor: ActorId,
    task: TaskIdentity,
    scope: ScopeHandle,
    work: WorkId,
    pending: Arc<dyn TaskPendingNotice>,
    health: Arc<ExecutionHealth>,
    role: ExitRole,
    report: ReportSlot,
    retirement: WorkRetirementObserver,
    state: Mutex<State>,
    retained: Mutex<Option<Arc<Self>>>,
}
#[derive(Clone)]
#[must_use = "Retain the original host-call inventory until its true return and Work reaping"]
pub(crate) struct TaskHostCall(Arc<Custody>);
#[must_use = "A rejected or canceled physical queue returns this exact original job"]
pub(crate) struct TaskHostCallJob {
    owner: TaskHostCall,
    start: StartPermission,
    authority: Option<ExitAuthority>,
    sender: Option<OwnedReturnSender<TaskHostCallOutcome>>,
}
#[derive(Clone, Copy)]
enum StartPermission {
    Normal(Generation),
    FailurePublicationsDrained,
}
impl TaskHostCall {
    pub(crate) fn from_unbound_admitted(
        owner: TaskHostCallOwner,
        original: ReportedWork,
    ) -> Result<Self, TaskHostCallUnboundRefusal> {
        let checked = (|| -> Result<(), TaskHostCallError> {
            let profile = original
                .bound_storage_profile()
                .map_err(TaskHostCallError::StorageBinding)?;
            let expected = OwnedSlotNeed {
                work_headers: 1,
                completions: 1,
                captures: 1,
                ..OwnedSlotNeed::default()
            };
            if profile != expected {
                return Err(TaskHostCallError::WrongStorage);
            }
            if !owner
                .runtime
                .owns_scope(owner.actor, original.work().scope())?
            {
                return Err(TaskHostCallError::WrongScope);
            }
            Ok(())
        })();
        if let Err(error) = checked {
            return Err(TaskHostCallUnboundRefusal {
                error,
                _original_owner: owner,
                original,
            });
        }
        let scope = original.work().scope().clone();
        let work = original.work().id();
        let retirement = original.work().retirement_observer();
        let report = original.report().clone();
        let (sender, reader) = match report.claim_owned_return() {
            Ok(pair) => pair,
            Err(error) => {
                return Err(TaskHostCallUnboundRefusal {
                    error: TaskHostCallError::Runtime(error),
                    _original_owner: owner,
                    original,
                });
            }
        };
        let (role, authority) = ExitRole::from_admitted(original);
        let shared = Arc::new(Custody {
            kind: owner.kind,
            runtime: owner.runtime,
            actor: owner.actor,
            task: owner.task,
            scope,
            work,
            pending: owner.pending,
            health: owner.health,
            role,
            report,
            retirement,
            state: Mutex::new(State {
                port: None,
                authority: Some(authority),
                sender: Some(sender),
                reader,
                returned: None,
                capture: None,
                retirement: None,
                panic: None,
                handoff: CallHandoff::Available,
                observer_consumed: false,
                retired: false,
                effect: None,
            }),
            retained: Mutex::new(None),
        });
        *shared
            .retained
            .lock()
            .expect("original Task host-call retention") = Some(shared.clone());
        Ok(Self(shared))
    }
    /// Bind the actual private Task port once before exposing any view. The
    /// weak address owns no execution capture; only a real host job takes one.
    pub(crate) fn attach_port(&self, port: &Arc<TaskRuntimePort>) -> Result<(), TaskHostCallError> {
        if port.identity() != self.0.task {
            return Err(TaskHostCallError::WrongTask);
        }
        if !port.publication().owned_by(&self.0.scope) {
            return Err(TaskHostCallError::WrongScope);
        }
        let mut state = self
            .0
            .state
            .lock()
            .expect("original host-call port binding");
        match state.handoff {
            CallHandoff::Available => {}
            CallHandoff::Withdrawn => return Err(TaskHostCallError::Withdrawn),
            CallHandoff::Issued => return Err(TaskHostCallError::AlreadyIssued),
            CallHandoff::AwaitingFailurePublication => {
                return Err(TaskHostCallError::AlreadyCaptured);
            }
        }
        if state.port.is_some() {
            return Err(TaskHostCallError::PortAlreadyAttached);
        }
        state.port = Some(Arc::downgrade(port));
        Ok(())
    }
    pub(crate) fn key(&self) -> (ScopeId, WorkId) {
        (self.0.scope.id(), self.0.work)
    }
    pub(crate) fn kind(&self) -> TaskHostCallKind {
        self.0.kind
    }
    #[cfg(test)]
    pub(crate) fn phase(&self) -> ExitRolePhase {
        self.0.role.phase()
    }
    /// Positive observation of this original synchronous host call and its
    /// port capture, independent of its return reader/report consumers/reaper.
    pub(crate) fn actual_call_exited(&self) -> bool {
        matches!(
            self.0.role.phase(),
            ExitRolePhase::Exited | ExitRolePhase::Retired
        ) && self.0.role.body_exited()
            && self.0.role.captures_exited()
    }
    pub(crate) fn is_retired(&self) -> bool {
        self.0.state.lock().expect("host-call retirement").retired
    }
    #[cfg(test)]
    pub(crate) fn has_panic(&self) -> bool {
        self.0
            .state
            .lock()
            .expect("host-call panic")
            .panic
            .is_some()
    }
    pub(crate) fn take_failure_job(
        &self,
        drained: &ScopePublicationsDrained<'_>,
    ) -> Result<TaskHostCallJob, TaskHostCallError> {
        if self.0.kind != TaskHostCallKind::Stop {
            return Err(TaskHostCallError::BorrowerExitRequired);
        }
        drained.validate(&self.0.runtime, self.0.actor)?;
        self.take_job(StartPermission::FailurePublicationsDrained)
    }
    /// Only the actual Task owner can construct this borrowed proof after
    /// examining its original fixed producer inventory. A drained publication
    /// stage alone cannot authorize execution-borrow retirement.
    pub(super) fn take_retire_failure_job(
        &self,
        drained: &ScopePublicationsDrained<'_>,
        exited: &super::task::TaskExecutionBorrowersExited<'_>,
    ) -> Result<TaskHostCallJob, TaskHostCallError> {
        if self.0.kind != TaskHostCallKind::RetireBorrowing {
            return Err(TaskHostCallError::WrongTask);
        }
        if !exited.matches(self.0.task, &self.0.scope) {
            return Err(TaskHostCallError::WrongScope);
        }
        drained.validate(&self.0.runtime, self.0.actor)?;
        self.take_job(StartPermission::FailurePublicationsDrained)
    }
    /// The original normal retirement effect and the original Task's positive
    /// borrower inventory authorize this once-only execution-port close. The
    /// effect remains with the Task owner until its physical cleanup commits.
    pub(super) fn take_retire_effect_job(
        &self,
        ticket: &EffectTicket<WorkerAction>,
        exited: &super::task::TaskExecutionBorrowersExited<'_>,
    ) -> Result<TaskHostCallJob, TaskHostCallError> {
        if self.0.kind != TaskHostCallKind::RetireBorrowing
            || ticket.actor != self.0.actor
            || !matches!(&ticket.effect, WorkerAction::Context(ContextEffect::RetireTask {task,..} | ContextEffect::FinalizeTaskExecution {task,..}) if *task==self.0.task)
            || !exited.matches(self.0.task, &self.0.scope)
        {
            return Err(TaskHostCallError::WrongTask);
        }
        self.0.runtime.validate_effect_ticket(ticket)?;
        self.take_job(StartPermission::Normal(ticket.generation))
    }
    /// The actual once-only call owns the original effect as well as its
    /// preadmitted callback/report. Refusal returns the same exclusive ticket.
    pub(in crate::control_runtime) fn handoff_stop_effect(
        &self,
        ticket: EffectTicket<WorkerAction>,
        notice: Option<RequestNotice>,
    ) -> Result<TaskHostCallJob, (TaskHostCallError, EffectTicket<WorkerAction>)> {
        let effect = match AdoptedCleanupEffect::from_normal(&self.0.runtime, ticket, notice) {
            Ok(effect) => effect,
            Err((error, ticket)) => return Err((TaskHostCallError::Driver(error), ticket)),
        };
        let job = match self.take_stop_effect_job(effect.ticket()) {
            Ok(job) => job,
            Err(error) => return Err((error, effect.into_ticket())),
        };
        let mut state = self.0.state.lock().expect("original Task host effect");
        assert!(state.effect.is_none(), "one original Stop effect");
        state.effect = Some(effect);
        Ok(job)
    }
    /// Move the old cleanup ticket into this same original Stop holder. The
    /// actual drained stage authorizes takeover, not the callback's exit.
    pub(crate) fn adopt_failed_stop_effect(
        &self,
        stage: &ScopePublicationsDrained<'_>,
        mut effect: AdoptedCleanupEffect,
    ) -> Result<(), (TaskHostCallError, AdoptedCleanupEffect)> {
        if self.0.kind != TaskHostCallKind::Stop
            || effect.is_completed()
            || !matches!(effect.effect(), WorkerAction::Context(ContextEffect::StopTask { task, .. }) if *task == self.0.task)
        {
            return Err((TaskHostCallError::WrongTask, effect));
        }
        if let Err(error) = effect.validate_scope(stage, &self.0.scope) {
            return Err((TaskHostCallError::Driver(error), effect));
        }
        {
            let state = self.0.state.lock().expect("original failed Stop ticket");
            if state.effect.is_some() {
                return Err((TaskHostCallError::AlreadyCaptured, effect));
            }
            if state.handoff == CallHandoff::Withdrawn {
                return Err((TaskHostCallError::Withdrawn, effect));
            }
        }
        if self.actual_call_exited() {
            if let Err(error) = effect.complete() {
                return Err((TaskHostCallError::Driver(error), effect));
            }
            return Ok(());
        }
        let mut state = self.0.state.lock().expect("original adopted Stop ticket");
        assert!(state.effect.is_none(), "one original adopted Stop effect");
        state.effect = Some(effect);
        drop(state);
        // The physical return may have raced the move into this field.
        // A later owner visit retries completion from actual call exit only.
        self.0.pending.pending();
        Ok(())
    }
    fn complete_stop_effect(&self) -> Result<bool, TaskHostCallError> {
        if !self.actual_call_exited() {
            return Ok(false);
        }
        let effect = self
            .0
            .state
            .lock()
            .expect("original Stop completion")
            .effect
            .take();
        let Some(mut effect) = effect else {
            return Ok(false);
        };
        // Runtime completion and the exact original R notice occur outside
        // the custody lock. On refusal the same ticket returns to this field.
        match effect.complete() {
            Ok(completed) => Ok(completed),
            Err(error) => {
                let mut state = self
                    .0
                    .state
                    .lock()
                    .expect("refused original Stop completion");
                assert!(state.effect.is_none(), "one original Stop completion owner");
                state.effect = Some(effect);
                Err(TaskHostCallError::Driver(error))
            }
        }
    }
    pub(crate) fn take_stop_effect_job(
        &self,
        ticket: &EffectTicket<WorkerAction>,
    ) -> Result<TaskHostCallJob, TaskHostCallError> {
        if self.0.kind != TaskHostCallKind::Stop
            || ticket.actor != self.0.actor
            || !matches!(&ticket.effect, WorkerAction::Context(ContextEffect::StopTask { task, .. }) if *task == self.0.task)
        {
            return Err(TaskHostCallError::WrongTask);
        }
        self.0.runtime.validate_effect_ticket(ticket)?;
        self.take_job(StartPermission::Normal(ticket.generation))
    }
    fn take_job(&self, start: StartPermission) -> Result<TaskHostCallJob, TaskHostCallError> {
        let original_port = self
            .0
            .state
            .lock()
            .expect("original Task host-call port")
            .port
            .clone()
            .ok_or(TaskHostCallError::MissingPort)?;
        let port = original_port
            .upgrade()
            .ok_or(TaskHostCallError::MissingPort)?;
        // Stop publishes an accepted conclusion to execution observers.
        // RetireBorrowing only closes borrowing, under its separately checked
        // Core retirement effect and actual borrower-exit inventory. A refused
        // creation has no accepted Task conclusion to publish.
        if self.0.kind == TaskHostCallKind::Stop
            && port.publication().borrow().neutral_accepted().is_none()
        {
            return Err(TaskHostCallError::MissingConclusion);
        }
        let mut state = self
            .0
            .state
            .lock()
            .expect("original Task host-call handoff");
        match state.handoff {
            CallHandoff::Issued => return Err(TaskHostCallError::AlreadyIssued),
            CallHandoff::Withdrawn => return Err(TaskHostCallError::Withdrawn),
            CallHandoff::AwaitingFailurePublication
                if matches!(start, StartPermission::Normal(_)) =>
            {
                return Err(TaskHostCallError::AwaitingFailurePublication);
            }
            CallHandoff::Available | CallHandoff::AwaitingFailurePublication => {}
        }
        if state.capture.is_some() && state.handoff != CallHandoff::AwaitingFailurePublication {
            return Err(TaskHostCallError::AlreadyCaptured);
        }
        let authority = state
            .authority
            .take()
            .expect("original unissued host authority");
        let sender = state.sender.take().expect("original unissued host return");
        state.handoff = CallHandoff::Issued;
        if state.capture.is_none() {
            state.capture = Some(ManuallyDrop::new(port));
        }
        Ok(TaskHostCallJob {
            owner: self.clone(),
            start,
            authority: Some(authority),
            sender: Some(sender),
        })
    }
    pub(crate) fn poll_return(&self) -> Result<Option<TaskHostCallOutcome>, TaskHostCallError> {
        let mut state = self.0.state.lock().expect("original Task host return");
        if state.returned.is_none() {
            state.returned = state.reader.try_take()?;
        }
        Ok(state.returned.clone())
    }
    /// No job or opaque port capture was exposed. Withdrawal revokes its
    /// original return/report rather than manufacturing observer consumption.
    pub(crate) fn retire_unused(&self, ledger: &RuntimeLedger) -> Result<bool, TaskHostCallError> {
        {
            let mut state = self.0.state.lock().expect("unused Task host call");
            if state.handoff == CallHandoff::Issued {
                return Err(TaskHostCallError::AlreadyIssued);
            }
            if state.capture.is_some() {
                return Err(TaskHostCallError::AlreadyCaptured);
            }
            if let Some(sender) = state.sender.take()
                && let Err(refusal) = sender.revoke_unused()
            {
                state.sender = Some(refusal.sender);
                return Err(TaskHostCallError::Runtime(refusal.error));
            }
            // Withdrawal closes issuance under the same handoff lock. Its
            // later bounded retirement may need several turns, but a late
            // effect can never obtain these already revoked capabilities.
            state.handoff = CallHandoff::Withdrawn;
            if let Some(authority) = state.authority.take() {
                authority.revoke_unused_and_exit()?;
            }
        }
        self.finish_retirement(ledger)
    }
    /// Advance the original call's actual disposition. An unexposed,
    /// withdrawn call has revoked its return slot; it must never be polled
    /// or acknowledged as an invoked call while its physical header retires.
    pub(crate) fn advance_retirement(
        &self,
        ledger: &RuntimeLedger,
    ) -> Result<bool, TaskHostCallError> {
        let handoff = {
            let state = self.0.state.lock().expect("original host-call disposition");
            if state.retired {
                return Ok(true);
            }
            state.handoff
        };
        match handoff {
            CallHandoff::Withdrawn => return self.finish_retirement(ledger),
            CallHandoff::Available | CallHandoff::AwaitingFailurePublication => {
                return Ok(false);
            }
            CallHandoff::Issued => {}
        }
        self.complete_stop_effect()?;
        let returned = self.poll_return()?;
        if !matches!(returned, Some(TaskHostCallOutcome::Completed)) {
            return Ok(false);
        }
        if self.0.role.phase() != ExitRolePhase::Exited
            && self.0.role.phase() != ExitRolePhase::Retired
        {
            return Ok(false);
        }
        {
            let mut state = self.0.state.lock().expect("host-call actual observer");
            if !state.observer_consumed {
                self.0.report.consume(ConsumerId::new(2).unwrap())?;
                state.observer_consumed = true;
            }
        }
        self.finish_retirement(ledger)
    }
    fn finish_retirement(&self, ledger: &RuntimeLedger) -> Result<bool, TaskHostCallError> {
        if self.is_retired() {
            return Ok(true);
        }
        if self.0.role.phase() != ExitRolePhase::Retired && !self.0.role.try_retire()? {
            return Ok(false);
        }
        if !self.0.retirement.is_retired() {
            ledger.reap_scope_ready(self.0.scope.id(), 1)?;
        }
        if !self.0.retirement.is_retired() {
            self.0.pending.pending();
            return Ok(false);
        }
        {
            let mut state = self.0.state.lock().expect("actual host-call retirement");
            state.retired = true;
        }
        let retained = self
            .0
            .retained
            .lock()
            .expect("Task host-call final retention")
            .take();
        drop(retained);
        Ok(true)
    }
    fn unresolved(&self) {
        self.0.health.report_unresolved(
            ExecutionExitOrigin::Task {
                identity: self.0.task,
                phase: TaskFailurePhase::Execution,
            },
            novarocks_execution_contract::failure::FailureFact::new(
                novarocks_execution_contract::failure::FailureClass::Invariant,
                crate::failure_reason::WorkerFailureReason::CompletionWorkUnresolved.reason(),
            ),
        );
        self.0.pending.pending();
    }
}
impl QueuedPreparation for TaskHostCallJob {
    fn queue_key(&self) -> (ScopeId, WorkId) {
        self.owner.key()
    }
}
impl TaskHostCallJob {
    /// The same originally admitted role covers the checked receiver call and
    /// every actual host capture Drop before its authority can sign exit.
    pub(crate) fn run_retirement(
        self,
        owner: Option<Arc<super::task::ProductionPreparation>>,
        host: Arc<dyn crate::TaskExecutionHost>,
        ingress: crate::TaskIngressRetirement,
    ) {
        assert_eq!(self.owner.0.kind, TaskHostCallKind::RetireBorrowing);
        {
            let mut state = self
                .owner
                .0
                .state
                .lock()
                .expect("original execution cleanup capture");
            assert!(
                state.retirement.is_none(),
                "one original receiver retirement capture"
            );
            state.retirement = Some(ManuallyDrop::new(ExecutionRetirementCall {
                owner,
                host,
                ingress,
            }));
        }
        self.run();
    }
    /// A normal queued capture may have crossed failure before its body
    /// started. Reuse that original capture under the actual failure stage;
    /// never overwrite it with another host/port loan.
    pub(crate) fn run_failure_retirement(
        self,
        owner: Option<Arc<super::task::ProductionPreparation>>,
        host: Arc<dyn crate::TaskExecutionHost>,
    ) {
        assert!(matches!(
            self.start,
            StartPermission::FailurePublicationsDrained
        ));
        let captured = self
            .owner
            .0
            .state
            .lock()
            .expect("original failed receiver capture")
            .retirement
            .is_some();
        if captured {
            // These additional metadata loans were not installed in the
            // original capture. Dispose them on this same physical worker.
            drop((owner, host));
            self.run();
        } else {
            self.run_retirement(owner, host, crate::TaskIngressRetirement::Remove);
        }
    }
    fn run_retirement_capture(&self) -> TaskHostCallOutcome {
        use super::task::ExecutionRetirementOutcome;
        let capture = self
            .owner
            .0
            .state
            .lock()
            .expect("original receiver cleanup capture")
            .retirement
            .take();
        let Some(capture) = capture else {
            return TaskHostCallOutcome::Completed;
        };
        let ingress = match self.start {
            StartPermission::Normal(_) => capture.ingress,
            // A real failure-publication session grants cleanup only, never
            // the earlier queued decision's normal-close permission.
            StartPermission::FailurePublicationsDrained => crate::TaskIngressRetirement::Remove,
        };
        let outcome = capture
            .owner
            .as_ref()
            .map_or(ExecutionRetirementOutcome::Completed, |owner| {
                owner.retire_execution_on_host(capture.host.as_ref(), ingress)
            });
        if outcome == ExecutionRetirementOutcome::Unresolved {
            self.owner
                .0
                .state
                .lock()
                .expect("unresolved original receiver capture")
                .retirement = Some(capture);
            return TaskHostCallOutcome::CleanupUnresolved;
        }
        // Neither the host nor its original returned source is destroyed
        // under a metadata lock. A capture destructor panic is Unknown even
        // when the checked operation itself returned a known typed refusal.
        if let Err(payload) =
            catch_unwind(AssertUnwindSafe(|| drop(ManuallyDrop::into_inner(capture))))
        {
            self.owner
                .0
                .state
                .lock()
                .expect("receiver capture panic retention")
                .panic = Some(ManuallyDrop::new(payload));
            return TaskHostCallOutcome::CleanupUnresolved;
        }
        match outcome {
            ExecutionRetirementOutcome::Completed => TaskHostCallOutcome::Completed,
            ExecutionRetirementOutcome::Rejected => TaskHostCallOutcome::CleanupRefused,
            ExecutionRetirementOutcome::Unresolved => {
                unreachable!("original unresolved capture retained above")
            }
        }
    }
    pub(crate) fn run(mut self) {
        let mut authority = self.authority.take().expect("one original host authority");
        let port = self
            .owner
            .0
            .state
            .lock()
            .expect("original host port capture")
            .capture
            .take()
            .expect("one original host port capture");
        let started = match self.start {
            StartPermission::Normal(generation) => authority.start_in_generation(generation),
            StartPermission::FailurePublicationsDrained => authority.start(),
        };
        if matches!(self.start, StartPermission::Normal(_))
            && matches!(
                started,
                Err(ExitRoleError::Runtime(RuntimeError::Scope(
                    ScopeError::GenerationMismatch
                )))
            )
        {
            // The original normal effect lost to scope failure before any
            // foreign code started. Keep every original capability and capture
            // for the real publication stage; this is neither exit nor Unknown.
            let mut state = self.owner.0.state.lock().expect("fenced Task host call");
            state.authority = Some(authority);
            state.sender = self.sender.take();
            state.capture = Some(port);
            state.handoff = CallHandoff::AwaitingFailurePublication;
            drop(state);
            self.owner.0.pending.pending();
            return;
        }
        let (outcome, exited) = match started {
            Err(error) => {
                self.owner
                    .0
                    .state
                    .lock()
                    .expect("unstarted host capture")
                    .capture = Some(port);
                (TaskHostCallOutcome::StartRefused(error), false)
            }
            Ok(()) => {
                let port = ManuallyDrop::into_inner(port);
                let result = catch_unwind(AssertUnwindSafe(|| match self.owner.0.kind {
                    TaskHostCallKind::Stop => port.notify_stopped(),
                    TaskHostCallKind::RetireBorrowing => port.retire_execution_port(),
                }));
                match result {
                    Err(payload) => {
                        let mut state = self
                            .owner
                            .0
                            .state
                            .lock()
                            .expect("host callback panic retention");
                        state.capture = Some(ManuallyDrop::new(port));
                        assert!(state.panic.is_none(), "one original Task host-call panic");
                        state.panic = Some(ManuallyDrop::new(payload));
                        (TaskHostCallOutcome::Panicked, false)
                    }
                    Ok(()) => {
                        let cleanup = self.run_retirement_capture();
                        if matches!(cleanup, TaskHostCallOutcome::CleanupUnresolved) {
                            self.owner
                                .0
                                .state
                                .lock()
                                .expect("unresolved original host port")
                                .capture = Some(ManuallyDrop::new(port));
                            (cleanup, false)
                        } else {
                            match catch_unwind(AssertUnwindSafe(|| drop(port))) {
                                Ok(()) => (cleanup, true),
                                Err(payload) => {
                                    let mut state = self
                                        .owner
                                        .0
                                        .state
                                        .lock()
                                        .expect("host capture panic retention");
                                    assert!(
                                        state.panic.is_none(),
                                        "one original Task host-call panic"
                                    );
                                    state.panic = Some(ManuallyDrop::new(payload));
                                    (TaskHostCallOutcome::Panicked, false)
                                }
                            }
                        }
                    }
                }
            }
        };
        if exited {
            if let Err(error) = authority.exited() {
                tracing::error!(
                    pid = std::process::id(),
                    ?error,
                    "Original host call exit failed"
                );
                std::mem::forget((error, self.sender.take()));
                std::process::abort();
            }
        } else {
            drop(authority);
            self.owner.unresolved();
        }
        let sender = self.sender.take().expect("original host return sender");
        if let Err(refusal) = sender.publish(outcome) {
            tracing::error!(pid = std::process::id(), error = ?refusal.error,
                "Original host call return publication failed");
            std::mem::forget(refusal);
            std::process::abort();
        }
        if exited && let Err(error) = self.owner.complete_stop_effect() {
            tracing::error!(?error, "Original Stop effect completion failed");
            self.owner.unresolved();
        }
        self.owner.0.pending.pending();
    }
}
impl Drop for TaskHostCallJob {
    fn drop(&mut self) {
        if self.authority.is_some() || self.sender.is_some() {
            // The original capture stays in its finite custodian. Losing an
            // issued job cannot become unused revocation or a poller Drop.
            self.owner.unresolved();
        }
    }
}
