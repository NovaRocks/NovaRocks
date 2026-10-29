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

//! Finite host lifecycle handoff. These ports transfer facts onto the original
//! Task causal roles; they select no conclusion, invoke no observer and wait
//! for neither the Core nor another queue's capacity.

use novarocks_execution_contract::{
    AbortCause, CancelReason, OperationOutcome, TaskIdentity, TaskStatusVersion,
};
use std::{
    future::Future,
    pin::Pin,
    sync::{Arc, Mutex},
};

/// The dormant start's unique, already admitted Running input. Read handles
/// and reporter clones cannot mint or duplicate this original authority.
#[must_use = "Transfer Running once, or withdraw this original unused start role"]
pub struct TaskStartPermit {
    identity: TaskIdentity,
    author: Option<crate::control_runtime::causal_input::InputAuthor>,
    runtime_generation: novarocks_control_runtime_core::identity::Generation,
}
impl std::fmt::Debug for TaskStartPermit {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TaskStartPermit")
            .field("identity", &self.identity)
            .finish_non_exhaustive()
    }
}
#[derive(Debug)]
pub struct TaskLifecycleHandoffError {
    role: &'static str,
}
impl std::fmt::Display for TaskLifecycleHandoffError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "the original Task {} input could not be handed off",
            self.role
        )
    }
}
impl std::error::Error for TaskLifecycleHandoffError {}
impl novarocks_execution_contract::failure::FailureProjection for TaskLifecycleHandoffError {
    fn failure_fact(&self) -> novarocks_execution_contract::failure::FailureFact {
        use novarocks_execution_contract::failure::{FailureClass, FailureFact};
        FailureFact::new(
            FailureClass::Invariant,
            crate::failure_reason::WorkerFailureReason::CausalInputUnresolved.reason(),
        )
    }
}
impl TaskStartPermit {
    pub(crate) fn from_admitted_author(
        author: crate::control_runtime::causal_input::InputAuthor,
        runtime_generation: novarocks_control_runtime_core::identity::Generation,
    ) -> Self {
        Self {
            identity: author.task(),
            author: Some(author),
            runtime_generation,
        }
    }
    pub const fn identity(&self) -> TaskIdentity {
        self.identity
    }
    /// Order this actual production start against the original scope's stop
    /// and failure fence. This Business role cannot borrow cleanup's ability
    /// to start after stop. A refusal returns the same never-started permit.
    pub fn authorize_running(mut self) -> Result<(), TaskStartRefusal> {
        if let Err(error) = self
            .author
            .as_mut()
            .expect("one original start authority")
            .start_in_generation(self.runtime_generation)
        {
            if matches!(
                error,
                novarocks_control_runtime::exit_role::ExitRoleError::Runtime(
                    novarocks_control_runtime::transaction::RuntimeError::Scope(
                        novarocks_control_runtime_core::scope::ScopeError::Stopped
                    )
                )
            ) {
                return Err(TaskStartRefusal::Stopped { permit: self });
            }
            return Err(TaskStartRefusal::Start {
                error,
                permit: self,
            });
        }
        self.handoff_running().map_err(TaskStartRefusal::Handoff)
    }
    /// Success transfers the input at the Task's original causal fence. It
    /// does not claim that the Core has consumed it or published RUNNING.
    pub fn handoff_running(mut self) -> Result<(), TaskLifecycleHandoffError> {
        self.author
            .take()
            .expect("one original dormant start input")
            .publish(novarocks_worker_protocol::context::ContextInput::Running {
                task: self.identity,
            })
            .map_err(|_retained| TaskLifecycleHandoffError { role: "Running" })
    }
}
#[must_use = "A refused start retains its original authority for explicit withdrawal"]
#[derive(Debug)]
pub enum TaskStartRefusal {
    Stopped {
        permit: TaskStartPermit,
    },
    Start {
        error: novarocks_control_runtime::exit_role::ExitRoleError,
        permit: TaskStartPermit,
    },
    Handoff(TaskLifecycleHandoffError),
}
impl Drop for TaskStartPermit {
    fn drop(&mut self) {
        if let Some(author) = self.author.take() {
            let _reported = author.withdraw();
        }
    }
}

/// The terminal callback's original observation role. Attachment and actual
/// callback exit are separate: a bound callback that loses its observation
/// keeps Unknown custody, while a never-invoked creation may withdraw it.
#[must_use = "The original terminal observation must publish or retain its responsibility"]
pub struct TaskTerminalPermit {
    identity: TaskIdentity,
    author: Option<crate::control_runtime::causal_input::InputAuthor>,
    bound: bool,
    preparation_return: Option<Arc<PreparationTerminalReturn>>,
}
impl std::fmt::Debug for TaskTerminalPermit {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TaskTerminalPermit")
            .field("identity", &self.identity)
            .field("bound", &self.bound)
            .finish_non_exhaustive()
    }
}
impl TaskTerminalPermit {
    pub(crate) fn custody(&self) -> crate::control_runtime::causal_input::CausalInputCustody {
        self.author
            .as_ref()
            .expect("original terminal observation author")
            .custody()
    }
    pub(crate) fn from_admitted_author(
        author: crate::control_runtime::causal_input::InputAuthor,
    ) -> Self {
        Self {
            identity: author.task(),
            author: Some(author),
            bound: false,
            preparation_return: None,
        }
    }
    pub const fn identity(&self) -> TaskIdentity {
        self.identity
    }
    pub(crate) fn bind(&mut self) {
        assert!(!self.bound, "one original terminal observation attachment");
        self.bound = true;
    }
    /// Transfer facts co-observed by the original callback at one causal
    /// prefix fence. Failure proposals still transfer their actual source
    /// before this call; this input cannot originate a failure or claim exit.
    pub fn handoff(
        mut self,
        observation: crate::TaskTerminalObservation,
    ) -> Result<(), TaskLifecycleHandoffError> {
        let author = self
            .author
            .take()
            .expect("one original terminal observation");
        let input = novarocks_worker_protocol::context::ContextInput::TerminalObservation {
            task: self.identity,
            observation,
        };
        if !self.bound {
            author.reject(input);
            return Err(TaskLifecycleHandoffError {
                role: "TerminalObservation",
            });
        }
        author
            .publish(input)
            .map_err(|_retained| TaskLifecycleHandoffError {
                role: "TerminalObservation",
            })
    }
    /// Only the original completion owner, after proving it never invoked
    /// this callback, can revoke a bound observation without fabricating it.
    pub(crate) fn withdraw_uninvoked(mut self) {
        if self.return_to_preparation() {
            return;
        }
        if let Some(author) = self.author.take() {
            let _reported = author.withdraw();
        }
    }
    fn return_to_preparation(&mut self) -> bool {
        let Some(owner) = self.preparation_return.take() else {
            return false;
        };
        if let Some(author) = self.author.take() {
            owner.return_terminal(Self {
                identity: self.identity,
                author: Some(author),
                bound: self.bound,
                preparation_return: None,
            });
        }
        true
    }
}
impl Drop for TaskTerminalPermit {
    fn drop(&mut self) {
        if !self.bound && self.return_to_preparation() {
            return;
        }
        if let Some(author) = self.author.take() {
            if self.bound {
                author.unresolved();
            } else {
                let _reported = author.withdraw();
            }
        }
    }
}

/// A control intent has no originating failure source. A real failure enters
/// through TaskConclusionPort::submit with its original ExecutionFailure.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TaskStopIntent {
    Canceled(CancelReason),
    Aborted(AbortCause),
}

/// The scope issuer supplies this once before attaching the original terminal
/// action. It owns both its finite causal input and the original Work/report
/// needed to preserve a late exit after failure or external admission close.
pub trait CompletionExitCapability: Send {
    /// The action, action captures, original terminal fact and reporter all
    /// exited after the actual runtime's retired-resource proof. Publication
    /// still goes through this role's Task FIFO and actual runtime fence.
    fn exited(self: Box<Self>);
    /// A never-committed action and all its captures really exited. This does
    /// not publish CompletionExited or invent a runtime stopped proof.
    fn withdrawn(self: Box<Self>);
    /// No callback/capture ever attached to this original unused authority.
    fn revoke_unused(self: Box<Self>);
    /// Unknown original exit remains retained; this grants no positive fact.
    fn unresolved(self: Box<Self>);
}

#[must_use = "The original completion action retains its finite exit authority"]
pub struct CompletionExitNotice {
    identity: TaskIdentity,
    capability: Option<Box<dyn CompletionExitCapability>>,
    bound: bool,
    preparation_return: Option<Arc<PreparationTerminalReturn>>,
}
impl std::fmt::Debug for CompletionExitNotice {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CompletionExitNotice")
            .field("identity", &self.identity)
            .field("bound", &self.bound)
            .finish_non_exhaustive()
    }
}
impl CompletionExitNotice {
    /// Capacity and the original role already exist. This factory grants no
    /// ability to reserve more positions from a Task control/reporter clone.
    pub fn from_admitted_role(
        identity: TaskIdentity,
        capability: Box<dyn CompletionExitCapability>,
    ) -> Self {
        Self {
            identity,
            capability: Some(capability),
            bound: false,
            preparation_return: None,
        }
    }
    pub const fn identity(&self) -> TaskIdentity {
        self.identity
    }
    pub(crate) fn bind(&mut self) {
        assert!(!self.bound, "one original completion action binding");
        self.bound = true;
    }
    pub(crate) fn completed(mut self) {
        assert!(self.bound, "a runtime completion has its original action");
        self.capability
            .take()
            .expect("one original completion exit")
            .exited();
    }
    pub(crate) fn withdrawn(mut self) {
        if self.return_to_preparation() {
            return;
        }
        let capability = self
            .capability
            .take()
            .expect("one original completion withdrawal");
        if self.bound {
            capability.withdrawn();
        } else {
            capability.revoke_unused();
        }
    }
    pub(crate) fn unresolved(mut self) {
        self.capability
            .take()
            .expect("one original unresolved completion")
            .unresolved();
    }
    fn return_to_preparation(&mut self) -> bool {
        let Some(owner) = self.preparation_return.take() else {
            return false;
        };
        if let Some(capability) = self.capability.take() {
            owner.return_exit(Self {
                identity: self.identity,
                capability: Some(capability),
                bound: self.bound,
                preparation_return: None,
            });
        }
        true
    }
}
impl Drop for CompletionExitNotice {
    fn drop(&mut self) {
        if !self.bound && self.return_to_preparation() {
            return;
        }
        if let Some(capability) = self.capability.take() {
            if self.bound {
                capability.unresolved();
            } else {
                capability.revoke_unused();
            }
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum PreparationTerminalReturnError {
    WrongIdentity,
    WrongScope,
    AlreadyBound,
    AlreadyTaken,
    Unresolved,
}
#[derive(Default)]
struct PreparationTerminalReturnState {
    bound: bool,
    taken: bool,
    unresolved: bool,
    terminal: Option<TaskTerminalPermit>,
    exit: Option<CompletionExitNotice>,
}
/// The original preparation owns exactly one return position for each of its
/// two terminal originals. Returned holders contain no link back to this owner.
/// Nothing here invokes a callback, signs exit or creates another authority.
pub(crate) struct PreparationTerminalReturn {
    scope: Arc<crate::task_preparation::TaskPreparationScope>,
    pending: Arc<dyn crate::task_port::TaskPendingNotice>,
    state: Mutex<PreparationTerminalReturnState>,
}
impl PreparationTerminalReturn {
    pub(crate) fn new(
        scope: Arc<crate::task_preparation::TaskPreparationScope>,
        pending: Arc<dyn crate::task_port::TaskPendingNotice>,
    ) -> Arc<Self> {
        Arc::new(Self {
            scope,
            pending,
            state: Mutex::new(PreparationTerminalReturnState::default()),
        })
    }
    /// The original issuer attaches this before either holder reaches a host.
    /// Refusal leaves both exact originals with that same caller.
    pub(crate) fn bind_original(
        self: &Arc<Self>,
        terminal: &mut TaskTerminalPermit,
        exit: &mut CompletionExitNotice,
    ) -> Result<(), PreparationTerminalReturnError> {
        if terminal.identity != self.scope.identity() || exit.identity != self.scope.identity() {
            return Err(PreparationTerminalReturnError::WrongIdentity);
        }
        if terminal.bound
            || exit.bound
            || terminal.author.is_none()
            || exit.capability.is_none()
            || terminal.preparation_return.is_some()
            || exit.preparation_return.is_some()
        {
            return Err(PreparationTerminalReturnError::AlreadyBound);
        }
        let mut state = self
            .state
            .lock()
            .expect("original preparation terminal return");
        if state.bound {
            return Err(PreparationTerminalReturnError::AlreadyBound);
        }
        terminal.preparation_return = Some(self.clone());
        exit.preparation_return = Some(self.clone());
        state.bound = true;
        Ok(())
    }
    /// Only the same scope's positive rollback proof can release the whole
    /// original pair. Rebinding is now preparation-owned, not a second runtime
    /// callback or a replacement completion authority.
    pub(crate) fn take_after_rollback(
        self: &Arc<Self>,
        proof: &crate::task_preparation::NeverAdoptedPreparationExit<'_>,
    ) -> Result<Option<(TaskTerminalPermit, CompletionExitNotice)>, PreparationTerminalReturnError>
    {
        if !proof.same_scope(self.scope.as_ref()) {
            return Err(PreparationTerminalReturnError::WrongScope);
        }
        if proof.identity() != self.scope.identity() {
            return Err(PreparationTerminalReturnError::WrongIdentity);
        }
        let mut state = self
            .state
            .lock()
            .expect("original preparation terminal return");
        if state.unresolved {
            return Err(PreparationTerminalReturnError::Unresolved);
        }
        if state.taken {
            return Err(PreparationTerminalReturnError::AlreadyTaken);
        }
        if state.terminal.is_none() || state.exit.is_none() {
            return Ok(None);
        }
        let mut terminal = state
            .terminal
            .take()
            .expect("same returned terminal original");
        let mut exit = state
            .exit
            .take()
            .expect("same returned completion original");
        state.taken = true;
        drop(state);
        terminal.bound = false;
        exit.bound = false;
        // Neither original remains in this owner's slots. These external links
        // therefore form no cycle, but reject a lost recovered original as
        // Unknown instead of reverting to unused withdrawal after Taken.
        terminal.preparation_return = Some(self.clone());
        exit.preparation_return = Some(self.clone());
        Ok(Some((terminal, exit)))
    }
    fn return_terminal(&self, original: TaskTerminalPermit) {
        let mut original = Some(original);
        {
            let mut state = self
                .state
                .lock()
                .expect("original preparation terminal return");
            if state.bound && !state.taken && state.terminal.is_none() {
                state.terminal = original.take();
            } else {
                state.unresolved = true;
            }
        }
        if let Some(mut original) = original {
            // A duplicate/invalid return is unknown, never unused withdrawal.
            original.bound = true;
            drop(original);
        }
        self.pending.pending();
    }
    fn return_exit(&self, original: CompletionExitNotice) {
        let mut original = Some(original);
        {
            let mut state = self
                .state
                .lock()
                .expect("original preparation terminal return");
            if state.bound && !state.taken && state.exit.is_none() {
                state.exit = original.take();
            } else {
                state.unresolved = true;
            }
        }
        if let Some(mut original) = original {
            original.bound = true;
            drop(original);
        }
        self.pending.pending();
    }
}
impl Drop for PreparationTerminalReturn {
    fn drop(&mut self) {
        let state = self
            .state
            .get_mut()
            .expect("original preparation terminal return");
        // No mutex guard spans either original's unknown-exit handling. Loss of
        // the original preparation owner cannot turn stored roles into refunds.
        let terminal = state.terminal.take();
        let exit = state.exit.take();
        if let Some(mut terminal) = terminal {
            terminal.bound = true;
            drop(terminal);
        }
        if let Some(mut exit) = exit {
            exit.bound = true;
            drop(exit);
        }
    }
}

/// An EOF answer is available only after the actual owner has adjudicated the
/// original drain and committed its required result/status publications.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum TaskRootDrainOutcome {
    EndPublished {
        version: TaskStatusVersion,
    },
    AlreadyTerminal {
        state: novarocks_execution_contract::TaskState,
    },
    AwaitTerminalControl,
    OwnerFailed {
        fact: novarocks_execution_contract::failure::FailureFact,
    },
    Refused(OperationOutcome),
}
pub type TaskRootDrainFuture = Pin<Box<dyn Future<Output = TaskRootDrainOutcome> + Send + 'static>>;
pub trait TaskRootDrainCapability: Send {
    /// Observe the same R's committed ACK fact. The channel already handed
    /// the fact to that R before returning this capability. Calling submit,
    /// dropping it unused, or canceling the returned observer cannot revoke
    /// the ACK or release its Work/capture/other-consumer obligations.
    fn submit(self: Box<Self>) -> TaskRootDrainFuture;
    fn revoke_unused(self: Box<Self>);
}
#[must_use = "A root drain consumes its own already admitted request role"]
pub struct TaskRootDrainPermit {
    identity: TaskIdentity,
    capability: Option<Box<dyn TaskRootDrainCapability>>,
}
impl std::fmt::Debug for TaskRootDrainPermit {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TaskRootDrainPermit")
            .field("identity", &self.identity)
            .finish_non_exhaustive()
    }
}
impl TaskRootDrainPermit {
    pub(crate) fn from_admitted_role(
        identity: TaskIdentity,
        capability: Box<dyn TaskRootDrainCapability>,
    ) -> Self {
        Self {
            identity,
            capability: Some(capability),
        }
    }
    pub const fn identity(&self) -> TaskIdentity {
        self.identity
    }
    pub fn submit(mut self) -> TaskRootDrainFuture {
        self.capability
            .take()
            .expect("one original root drain request")
            .submit()
    }
}
impl Drop for TaskRootDrainPermit {
    fn drop(&mut self) {
        if let Some(capability) = self.capability.take() {
            capability.revoke_unused();
        }
    }
}
