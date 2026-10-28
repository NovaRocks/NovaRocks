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
//! cleanup Work and a separately owned, already-paid slot fee. Foreign code
//! runs only on its physical host runner; queue return never signs Work exit.

use super::{
    actors::{WorkerAction, WorkerActor},
    admitted_role::adopt_reported_work,
    failure::ScopePublicationsDrained,
    preparation::QueuedPreparation,
};
use crate::{
    control_runtime_policy::StorageNeed,
    task_port::{TaskPendingNotice, TaskRuntimePort},
};
use novarocks_control_runtime::{
    completion::ReportSlot,
    driver::{ActorRuntime, DriverError, EffectTicket},
    exit_role::{ExitAuthority, ExitRole, ExitRoleError, ExitRolePhase},
    lease::{CapacityBundle, WorkRetirementObserver},
    owned_return::{OwnedReturnReader, OwnedReturnSender},
    reported_work::ReportedWork,
    transaction::{RuntimeError, RuntimeLedger, ScopeHandle},
};
use novarocks_control_runtime_core::{
    capacity::ResourceVector,
    identity::{ActorId, ConsumerId, Generation, ScopeId, WorkId},
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
#[derive(Clone, Debug)]
pub(crate) enum TaskHostCallOutcome {
    Completed,
    Panicked,
    StartRefused(ExitRoleError),
}
#[derive(Debug)]
pub(crate) enum TaskHostCallError {
    Runtime(RuntimeError),
    Driver(DriverError),
    Exit(ExitRoleError),
    WrongScope,
    WrongTask,
    WrongStorage,
    AlreadyIssued,
    Withdrawn,
    AwaitingFailurePublication,
    AlreadyCaptured,
    MissingPort,
    MissingConclusion,
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

pub(crate) struct TaskHostCallBinding {
    pub kind: TaskHostCallKind,
    pub runtime: ActorRuntime<WorkerActor>,
    pub actor: ActorId,
    pub task: TaskIdentity,
    pub port: Arc<TaskRuntimePort>,
    pub pending: Arc<dyn TaskPendingNotice>,
    pub health: Arc<ExecutionHealth>,
}
/// Protocol fixtures explicitly admit finite zero-resource headers. Absence
/// of a payment is never inferred to be production admission.
pub(crate) enum TaskHostCallPayment {
    Paid(CapacityBundle),
    #[cfg(test)]
    ProtocolModel,
}
#[must_use = "Admission refusal retains the complete original binding, Work and payment"]
pub(crate) struct TaskHostCallRefusal {
    pub error: TaskHostCallError,
    pub binding: TaskHostCallBinding,
    pub original: ReportedWork,
    pub payment: TaskHostCallPayment,
}
impl std::fmt::Debug for TaskHostCallRefusal {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TaskHostCallRefusal")
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
struct State {
    authority: Option<ExitAuthority>,
    sender: Option<OwnedReturnSender<TaskHostCallOutcome>>,
    reader: OwnedReturnReader<TaskHostCallOutcome>,
    returned: Option<TaskHostCallOutcome>,
    capture: Option<ManuallyDrop<Arc<TaskRuntimePort>>>,
    panic: Option<PanicPayload>,
    handoff: CallHandoff,
    observer_consumed: bool,
    payment: Option<CapacityBundle>,
    retired: bool,
}
struct Custody {
    kind: TaskHostCallKind,
    runtime: ActorRuntime<WorkerActor>,
    actor: ActorId,
    task: TaskIdentity,
    scope: ScopeHandle,
    work: WorkId,
    port: Weak<TaskRuntimePort>,
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
    pub(crate) fn from_admitted(
        binding: TaskHostCallBinding,
        original: ReportedWork,
        mut payment: TaskHostCallPayment,
    ) -> Result<Self, TaskHostCallRefusal> {
        let (work, capacity, report) = original.into_parts();
        let header = capacity.resources();
        let original = adopt_reported_work(work, capacity, report);
        let checked = (|| -> Result<(), TaskHostCallError> {
            if header? != ResourceVector::default() {
                return Err(TaskHostCallError::WrongStorage);
            }

            if binding.port.identity() != binding.task {
                return Err(TaskHostCallError::WrongTask);
            }
            if !binding
                .runtime
                .owns_scope(binding.actor, original.work().scope())?
                || !binding.port.publication().owned_by(original.work().scope())
            {
                return Err(TaskHostCallError::WrongScope);
            }
            match &mut payment {
                TaskHostCallPayment::Paid(paid) => {
                    if paid.resources()?
                        != HOST_CALL_STORAGE
                            .resources()
                            .expect("fixed host-call storage")
                    {
                        return Err(TaskHostCallError::WrongStorage);
                    }
                    paid.transfer(original.work())?;
                }
                #[cfg(test)]
                TaskHostCallPayment::ProtocolModel => {}
            }
            Ok(())
        })();
        if let Err(error) = checked {
            return Err(TaskHostCallRefusal {
                error,
                binding,
                original,
                payment,
            });
        }
        let scope = original.work().scope().clone();
        let work = original.work().id();
        let retirement = original.work().retirement_observer();
        let report = original.report().clone();
        let (sender, reader) = match report.claim_owned_return() {
            Ok(pair) => pair,
            Err(error) => {
                return Err(TaskHostCallRefusal {
                    error: TaskHostCallError::Runtime(error),
                    binding,
                    original,
                    payment,
                });
            }
        };
        let (role, authority) = ExitRole::from_admitted(original);
        let payment = match payment {
            TaskHostCallPayment::Paid(payment) => Some(payment),
            #[cfg(test)]
            TaskHostCallPayment::ProtocolModel => None,
        };
        let shared = Arc::new(Custody {
            kind: binding.kind,
            runtime: binding.runtime,
            actor: binding.actor,
            task: binding.task,
            scope,
            work,
            port: Arc::downgrade(&binding.port),
            pending: binding.pending,
            health: binding.health,
            role,
            report,
            retirement,
            state: Mutex::new(State {
                authority: Some(authority),
                sender: Some(sender),
                reader,
                returned: None,
                capture: None,
                panic: None,
                handoff: CallHandoff::Available,
                observer_consumed: false,
                payment,
                retired: false,
            }),
            retained: Mutex::new(None),
        });
        *shared
            .retained
            .lock()
            .expect("original Task host-call retention") = Some(shared.clone());
        Ok(Self(shared))
    }
    pub(crate) fn key(&self) -> (ScopeId, WorkId) {
        (self.0.scope.id(), self.0.work)
    }
    pub(crate) fn kind(&self) -> TaskHostCallKind {
        self.0.kind
    }
    pub(crate) fn phase(&self) -> ExitRolePhase {
        self.0.role.phase()
    }
    pub(crate) fn is_retired(&self) -> bool {
        self.0.state.lock().expect("host-call retirement").retired
    }
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
        drained.validate(&self.0.runtime, self.0.actor)?;
        self.take_job(StartPermission::FailurePublicationsDrained)
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
        let port = self
            .0
            .port
            .upgrade()
            .ok_or(TaskHostCallError::MissingPort)?;
        if port.publication().borrow().neutral_accepted().is_none() {
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
    pub(crate) fn retire_returned(
        &self,
        ledger: &RuntimeLedger,
    ) -> Result<bool, TaskHostCallError> {
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
            let mut state = self.0.state.lock().expect("actual host-call payment");
            if let Some(payment) = &mut state.payment
                && !payment.is_released()
            {
                let resources = payment.resources()?;
                payment.release(&resources)?;
            }
            state.payment = None;
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
                    Ok(()) => match catch_unwind(AssertUnwindSafe(|| drop(port))) {
                        Ok(()) => (TaskHostCallOutcome::Completed, true),
                        Err(payload) => {
                            let mut state = self
                                .owner
                                .0
                                .state
                                .lock()
                                .expect("host capture panic retention");
                            assert!(state.panic.is_none(), "one original Task host-call panic");
                            state.panic = Some(ManuallyDrop::new(payload));
                            (TaskHostCallOutcome::Panicked, false)
                        }
                    },
                }
            }
        };
        if exited {
            if let Err(error) = authority.exited() {
                std::mem::forget((error, self.sender.take()));
                std::process::abort();
            }
        } else {
            drop(authority);
            self.owner.unresolved();
        }
        let sender = self.sender.take().expect("original host return sender");
        if let Err(refusal) = sender.publish(outcome) {
            std::mem::forget(refusal);
            std::process::abort();
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
