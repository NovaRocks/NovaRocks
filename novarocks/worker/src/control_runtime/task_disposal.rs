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

//! One preadmitted Participation-child Cleanup role covers the original Task's
//! physical payload destruction and observer invocation. It is a sibling of
//! TaskLifetime, so its unstarted body never obstructs the Task's read seal.

use super::{
    actors::{WorkerAction, WorkerActor},
    failure::ScopePublicationsDrained,
};
use crate::{control_runtime_policy::StorageNeed, task_port::TaskPendingNotice};
use novarocks_control_runtime::{
    actor_turn::{ActorRuntime, ActorTurnError, EffectTicket},
    exit_custody::{ExitCustody, ExitCustodyError, ExitCustodyPhase, ExitObligation},
    obligation::WorkRetirementObserver,
    reported_work::{ReportedWork, ReportedWorkBindingError},
    transaction::{RuntimeError, RuntimeLedger, ScopeHandle},
};
use novarocks_control_runtime_core::{
    completion::Consumption,
    identity::{ActorId, ConsumerId, Generation, ScopeId, WorkId},
    owned_storage::OwnedSlotNeed,
    scope::ScopeError,
};
use novarocks_execution::runtime::execution_health::{ExecutionExitOrigin, ExecutionHealth};
use novarocks_execution_contract::{
    TaskFailurePhase, TaskIdentity, TaskState,
    failure::{FailureClass, FailureFact},
};
use novarocks_worker_protocol::{creation::PayloadId, participation::ParticipationEffect};
use std::sync::{Arc, Mutex};

pub(crate) const TASK_DISPOSAL_STORAGE: StorageNeed = StorageNeed {
    work: 1,
    completions: 1,
    captures: 1,
    inputs: 0,
    batches: 0,
    effects: 0,
    cuts: 0,
    replies: 0,
    requests: 0,
};
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum TaskDisposalOrigin {
    AcceptedTerminal {
        state: TaskState,
    },
    RejectedCreation {
        state: TaskState,
        payload: PayloadId,
    },
    OwnerFailed,
}
#[derive(Debug)]
pub(crate) enum TaskDisposalError {
    Runtime(RuntimeError),
    ActorTurn(ActorTurnError),
    Exit(ExitCustodyError),
    WrongOwner,
    WrongStorage,
    StorageBinding(ReportedWorkBindingError),
    AlreadyIssued,
}
impl std::fmt::Display for TaskDisposalError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Runtime(error) => write!(f, "Task disposal runtime failure: {error}"),
            Self::ActorTurn(error) => write!(f, "Task disposal driver failure: {error}"),
            Self::Exit(error) => write!(f, "Task disposal exit failure: {error}"),
            Self::WrongOwner => f.write_str("Task disposal has a different original owner"),
            Self::WrongStorage => {
                f.write_str("Task disposal has a different original storage profile")
            }
            Self::StorageBinding(error) => {
                write!(f, "Task disposal storage binding failed: {error}")
            }
            Self::AlreadyIssued => f.write_str("Task disposal was already issued"),
        }
    }
}
impl std::error::Error for TaskDisposalError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Runtime(error) => Some(error),
            Self::ActorTurn(error) => Some(error),
            Self::Exit(error) => Some(error),
            Self::StorageBinding(error) => Some(error),
            _ => None,
        }
    }
}

impl From<RuntimeError> for TaskDisposalError {
    fn from(e: RuntimeError) -> Self {
        Self::Runtime(e)
    }
}
impl From<ActorTurnError> for TaskDisposalError {
    fn from(e: ActorTurnError) -> Self {
        Self::ActorTurn(e)
    }
}
impl From<ExitCustodyError> for TaskDisposalError {
    fn from(e: ExitCustodyError) -> Self {
        Self::Exit(e)
    }
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Permission {
    Normal(Generation),
    Failure,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Handoff {
    Available,
    AwaitingFailurePublication,
    Authorized(Permission),
    Issued,
    Withdrawn,
    Exited,
    Unresolved,
}
struct State {
    handoff: Handoff,
    authority: Option<ExitObligation>,
}
struct Custody {
    task: TaskIdentity,
    actor: ActorId,
    runtime: ActorRuntime<WorkerActor>,
    scope: ScopeHandle,
    work: WorkId,
    exit_custody: ExitCustody,
    report: novarocks_control_runtime::completion::ReportSlot,
    retirement: WorkRetirementObserver,
    state: Mutex<State>,
    pending: Arc<dyn TaskPendingNotice>,
    health: Arc<ExecutionHealth>,
}
pub(crate) struct TaskDisposalBinding {
    pub runtime: ActorRuntime<WorkerActor>,
    pub actor: ActorId,
    pub task: TaskIdentity,
    pub pending: Arc<dyn TaskPendingNotice>,
    pub health: Arc<ExecutionHealth>,
}
pub(crate) struct TaskDisposalCustody(Arc<Custody>);
#[must_use]
pub(crate) struct TaskDisposalAuthorization {
    owner: Arc<Custody>,
    origin: TaskDisposalOrigin,
    permission: Permission,
}
impl TaskDisposalAuthorization {
    pub(crate) fn origin(&self) -> TaskDisposalOrigin {
        self.origin
    }
}
#[must_use]
pub(crate) struct TaskDisposalJob {
    owner: TaskDisposalCustody,
    origin: TaskDisposalOrigin,
    permission: Permission,
    authority: Option<ExitObligation>,
}
pub(crate) struct TaskDisposalAdmissionRefusal {
    pub _original_error: TaskDisposalError,
    pub _original_work: ReportedWork,
}
impl TaskDisposalCustody {
    pub(crate) fn from_admitted(
        binding: TaskDisposalBinding,
        original: ReportedWork,
    ) -> Result<Self, Box<TaskDisposalAdmissionRefusal>> {
        let TaskDisposalBinding {
            runtime,
            actor,
            task,
            pending,
            health,
        } = binding;
        let checked = (|| -> Result<(), TaskDisposalError> {
            let profile = original
                .bound_storage_profile()
                .map_err(TaskDisposalError::StorageBinding)?;
            let expected = OwnedSlotNeed {
                work_headers: 1,
                completions: 1,
                captures: 1,
                ..OwnedSlotNeed::default()
            };
            if profile != expected {
                return Err(TaskDisposalError::WrongStorage);
            }
            if !runtime.owns_scope(actor, original.work().scope())? {
                return Err(TaskDisposalError::WrongOwner);
            }
            Ok(())
        })();
        if let Err(error) = checked {
            return Err(Box::new(TaskDisposalAdmissionRefusal {
                _original_error: error,
                _original_work: original,
            }));
        }
        let scope = original.work().scope().clone();
        let work = original.work().id();
        let retirement = original.work().retirement_observer();
        let report = original.report().clone();
        let (exit_custody, authority) = ExitCustody::from_admitted(original);
        Ok(Self(Arc::new(Custody {
            task,
            actor,
            runtime,
            scope,
            work,
            exit_custody,
            report,
            retirement,
            state: Mutex::new(State {
                handoff: Handoff::Available,
                authority: Some(authority),
            }),
            pending,
            health,
        })))
    }
    pub(crate) fn key(&self) -> (ScopeId, WorkId) {
        (self.0.scope.id(), self.0.work)
    }
    pub(crate) fn authorize_normal(
        &self,
        ticket: &EffectTicket<WorkerAction>,
    ) -> Result<TaskDisposalAuthorization, TaskDisposalError> {
        self.0.runtime.validate_effect_ticket(ticket)?;
        let origin = match &ticket.effect {
            WorkerAction::Participation(ParticipationEffect::RetireTask {
                task,
                state,
                payload,
                never_accepted,
                ..
            }) if ticket.actor == self.0.actor && *task == self.0.task => {
                if *never_accepted {
                    TaskDisposalOrigin::RejectedCreation {
                        state: *state,
                        payload: *payload,
                    }
                } else {
                    TaskDisposalOrigin::AcceptedTerminal { state: *state }
                }
            }
            _ => return Err(TaskDisposalError::WrongOwner),
        };
        let permission = Permission::Normal(ticket.generation);
        let mut state = self
            .0
            .state
            .lock()
            .expect("original Task disposal authorization");
        if state.handoff != Handoff::Available {
            return Err(TaskDisposalError::AlreadyIssued);
        }
        state.handoff = Handoff::Authorized(permission);
        Ok(TaskDisposalAuthorization {
            owner: self.0.clone(),
            origin,
            permission,
        })
    }
    pub(crate) fn authorize_failure(
        &self,
        stage: &ScopePublicationsDrained<'_>,
    ) -> Result<TaskDisposalAuthorization, TaskDisposalError> {
        stage.validate(&self.0.runtime, self.0.actor)?;
        let mut state = self
            .0
            .state
            .lock()
            .expect("original failed Task disposal authorization");
        if !matches!(
            state.handoff,
            Handoff::Available | Handoff::Authorized(_) | Handoff::AwaitingFailurePublication
        ) || state.authority.is_none()
        {
            return Err(TaskDisposalError::AlreadyIssued);
        }
        state.handoff = Handoff::Authorized(Permission::Failure);
        Ok(TaskDisposalAuthorization {
            owner: self.0.clone(),
            origin: TaskDisposalOrigin::OwnerFailed,
            permission: Permission::Failure,
        })
    }
    pub(crate) fn take_job(
        &self,
        authorization: TaskDisposalAuthorization,
    ) -> Result<TaskDisposalJob, (TaskDisposalError, TaskDisposalAuthorization)> {
        if !Arc::ptr_eq(&self.0, &authorization.owner) {
            return Err((TaskDisposalError::WrongOwner, authorization));
        }
        let mut state = self
            .0
            .state
            .lock()
            .expect("unique original Task disposal handoff");
        if state.handoff != Handoff::Authorized(authorization.permission)
            || state.authority.is_none()
        {
            return Err((TaskDisposalError::AlreadyIssued, authorization));
        }
        let authority = state.authority.take();
        state.handoff = Handoff::Issued;
        Ok(TaskDisposalJob {
            owner: TaskDisposalCustody(self.0.clone()),
            origin: authorization.origin,
            permission: authorization.permission,
            authority,
        })
    }
    pub(crate) fn actual_call_exited(&self) -> bool {
        matches!(
            self.0.exit_custody.phase(),
            ExitCustodyPhase::Exited | ExitCustodyPhase::Retired
        ) && self.0.exit_custody.body_exited()
            && self.0.exit_custody.captures_exited()
    }
    pub(crate) fn is_retired(&self) -> bool {
        self.0.exit_custody.phase() == ExitCustodyPhase::Retired && self.0.retirement.is_retired()
    }
    pub(crate) fn advance_retirement(
        &self,
        ledger: &RuntimeLedger,
    ) -> Result<bool, TaskDisposalError> {
        let mut progress = false;
        if !self.actual_call_exited() {
            return Ok(false);
        }
        if self.0.exit_custody.phase() != ExitCustodyPhase::Retired {
            let observer = ConsumerId::new(2).unwrap();
            let withdrawn = self
                .0
                .state
                .lock()
                .expect("original unused disposal observation")
                .handoff
                == Handoff::Withdrawn;
            if !withdrawn && self.0.report.consumption(observer)? == Consumption::Pending {
                self.0.report.consume(observer)?;
                progress = true;
            }
            let before = self.0.exit_custody.phase();
            self.0.exit_custody.try_retire()?;
            progress |= before != self.0.exit_custody.phase();
        }
        progress |= ledger.reap_scope_ready(self.0.scope.id(), 1)? != 0;
        Ok(progress)
    }
    pub(crate) fn retire_unused(&self, ledger: &RuntimeLedger) -> Result<bool, TaskDisposalError> {
        {
            let mut state = self
                .0
                .state
                .lock()
                .expect("unexposed original Task disposal");
            if !matches!(state.handoff, Handoff::Available | Handoff::Withdrawn) {
                return Err(TaskDisposalError::AlreadyIssued);
            }
            if let Some(authority) = state.authority.take() {
                authority.revoke_unused_and_exit()?;
            }
            state.handoff = Handoff::Withdrawn;
        }
        self.advance_retirement(ledger)?;
        Ok(self.is_retired())
    }
    fn unresolved(&self) {
        self.0
            .state
            .lock()
            .expect("unresolved original Task disposal")
            .handoff = Handoff::Unresolved;
        self.0.health.report_unresolved(
            ExecutionExitOrigin::Task {
                identity: self.0.task,
                phase: TaskFailurePhase::Execution,
            },
            FailureFact::new(
                FailureClass::Unexpected,
                crate::failure_reason::WorkerFailureReason::TaskDisposalCallback.reason(),
            ),
        );
        self.0.pending.pending();
    }
}
impl TaskDisposalJob {
    pub(crate) fn key(&self) -> (ScopeId, WorkId) {
        self.owner.key()
    }
    pub(crate) fn run(mut self, payload: &super::task::TaskDisposal) {
        let mut authority = self
            .authority
            .take()
            .expect("same original Task disposal authority");
        let started = match self.permission {
            Permission::Normal(g) => authority.start_in_generation(g),
            Permission::Failure => authority.start(),
        };
        if matches!(self.permission, Permission::Normal(_))
            && matches!(
                started,
                Err(ExitCustodyError::Runtime(RuntimeError::Scope(
                    ScopeError::GenerationMismatch
                )))
            )
        {
            let mut state = self
                .owner
                .0
                .state
                .lock()
                .expect("queued Task disposal lost to failure");
            state.authority = Some(authority);
            state.handoff = Handoff::AwaitingFailurePublication;
            drop(state);
            self.owner.0.pending.pending();
            return;
        }
        if started.is_err() {
            drop(authority);
            self.owner.unresolved();
            return;
        }
        if !payload.dispose_on_host(self.origin) {
            drop(authority);
            self.owner.unresolved();
            return;
        }
        if authority.exited().is_err() {
            self.owner.unresolved();
            return;
        }
        self.owner
            .0
            .state
            .lock()
            .expect("actual Task disposal returned")
            .handoff = Handoff::Exited;
        self.owner.0.pending.pending();
    }
}
