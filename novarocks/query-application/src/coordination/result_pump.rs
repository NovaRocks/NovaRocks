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

//! Actor-owned root-result pumping.
//!
//! Role adapters supply bounded Backend-encoded root items. This module owns
//! ordered relay, acknowledgement, status progression and terminal handoff.
//! It has no Arrow decoder or execution-kernel dependency.

use std::{future::Future, num::NonZeroU64, pin::Pin, sync::Arc};

use novarocks_execution_contract::root_result::RootResultRead;
use novarocks_execution_contract::{
    QueryContextRef, ResultPacketSequence, TaskIdentity, TaskState, TaskStatus,
};
use novarocks_types::QueryExecutionId;
use novarocks_workload_control::{CancellationReason, CancellationView, WorkError, WorkScope};
use tokio::sync::{oneshot, watch};

use crate::api::{BoundedRootReadPort, ComputationReadyToken, RetainedRootReply};
use crate::api::{NativeAttemptTerminal, NativeAttemptTopologyRequirement};
use crate::api::{QueryExecutionError, QueryExecutionErrorKind};

use super::{
    LogicalConclusion, LogicalExecutionActor, LogicalExecutionActorError, ReplacementQualification,
    RootResultObserver, RootTerminalFailure, RunningAttemptHandoffError, RunningAttemptPermit,
};
use novarocks_coordination_contract::{AttemptFailureClass, ResultContextWithdrawal};

pub(crate) struct NativeAttemptTerminalSender {
    sender: Option<oneshot::Sender<NativeAttemptTerminal>>,
}

pub(crate) struct NativeAttemptTerminalSource {
    receiver: Option<oneshot::Receiver<NativeAttemptTerminal>>,
}

pub(crate) fn native_attempt_terminal_channel()
-> (NativeAttemptTerminalSender, NativeAttemptTerminalSource) {
    let (sender, receiver) = oneshot::channel();
    (
        NativeAttemptTerminalSender {
            sender: Some(sender),
        },
        NativeAttemptTerminalSource {
            receiver: Some(receiver),
        },
    )
}

impl NativeAttemptTerminalSender {
    pub(crate) fn publish(mut self, terminal: NativeAttemptTerminal) {
        if let Some(sender) = self.sender.take() {
            let _ = sender.send(terminal);
        }
    }
}

impl NativeAttemptTerminalSource {
    async fn next(&mut self) -> Result<NativeAttemptTerminal, QueryExecutionError> {
        let receiver = self
            .receiver
            .as_mut()
            .ok_or_else(|| contract_error("Native attempt terminal was consumed more than once"))?;
        let terminal = receiver.await.map_err(|_| {
            contract_error("Native attempt terminal owner dropped without a verdict")
        })?;
        self.receiver = None;
        Ok(terminal)
    }

    fn try_next(&mut self) -> Result<Option<NativeAttemptTerminal>, QueryExecutionError> {
        let receiver = self
            .receiver
            .as_mut()
            .ok_or_else(|| contract_error("Native attempt terminal was consumed more than once"))?;
        match receiver.try_recv() {
            Ok(terminal) => {
                self.receiver = None;
                Ok(Some(terminal))
            }
            Err(oneshot::error::TryRecvError::Empty) => Ok(None),
            Err(oneshot::error::TryRecvError::Closed) => Err(contract_error(
                "Native attempt terminal owner dropped without a verdict",
            )),
        }
    }
}

/// Typed adapter failure and its attempt-level recovery class.
#[doc(hidden)]
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RootResultFetchFailure {
    error: QueryExecutionError,
    pre_ready_replacement: Option<crate::api::PreReadyProcessReplacementWitness>,
}
impl RootResultFetchFailure {
    pub fn from_input(input: crate::execution_error::ExecutionFailureInput) -> Self {
        Self {
            error: QueryExecutionError::from_execution(input),
            pre_ready_replacement: None,
        }
    }
    pub fn input(&self) -> &crate::execution_error::ExecutionFailureInput {
        self.error
            .execution_failure()
            .expect("typed constructor retains its original failure input")
    }
    fn from_preparation_failure(failure: crate::api::NativeAttemptPreparationFailure) -> Self {
        Self {
            error: failure.error().clone(),
            pre_ready_replacement: failure.pre_ready_replacement().cloned(),
        }
    }
    pub const fn pre_ready_replacement(
        &self,
    ) -> Option<&crate::api::PreReadyProcessReplacementWitness> {
        self.pre_ready_replacement.as_ref()
    }
    pub fn topology_requirement(&self) -> NativeAttemptTopologyRequirement {
        self.pre_ready_replacement.as_ref().map_or_else(
            || self.input().topology_requirement(),
            |witness| {
                NativeAttemptTopologyRequirement::ExcludeProcess(
                    witness.context().backend_process_id(),
                )
            },
        )
    }
    pub fn class(&self) -> AttemptFailureClass {
        self.input().attempt_class()
    }
    pub const fn error(&self) -> &QueryExecutionError {
        &self.error
    }
}

/// Single accepted-status projection. Frontend publishes the complete
/// snapshot in the same serial turn that its TaskRound accepts it.
#[doc(hidden)]
pub struct AcceptedRootStatusSender {
    root: TaskIdentity,
    sender: watch::Sender<Option<AcceptedRootProjection>>,
}

#[doc(hidden)]
pub struct AcceptedRootStatusSource {
    root: TaskIdentity,
    receiver: watch::Receiver<Option<AcceptedRootProjection>>,
    observed: Option<AcceptedRootProjection>,
    root_control_port: Option<Arc<dyn AcceptedRootControlPort>>,
}

/// Frontend's single serialized path for ordering result-control requirements
/// against accepted Task status. Implementations enqueue the request beside status events and
/// must not decide it on the caller's async task.
#[doc(hidden)]
pub trait AcceptedRootControlPort: std::fmt::Debug + Send + Sync {
    fn enqueue_root_control(
        &self,
        request: AcceptedRootControlRequest,
    ) -> Result<(), AcceptedRootControlRequest>;
}

/// Move-only result-control request. Success sealing consumes its status
/// publisher; terminal-control registration records a nonrenewable evidence
/// requirement in that same serialized owner.
#[doc(hidden)]
#[derive(Debug)]
pub struct AcceptedRootControlRequest {
    kind: AcceptedRootControlRequestKind,
    reply: oneshot::Sender<Result<(), QueryExecutionError>>,
}

#[derive(Clone, Copy, Debug)]
enum AcceptedRootControlRequestKind {
    SuccessSeal,
    AwaitTerminalControl(TaskIdentity),
}

impl AcceptedRootControlRequest {
    pub fn terminal_control_root(&self) -> Option<TaskIdentity> {
        match self.kind {
            AcceptedRootControlRequestKind::AwaitTerminalControl(root) => Some(root),
            AcceptedRootControlRequestKind::SuccessSeal => None,
        }
    }

    pub fn accept_terminal_control(self) -> Result<(), QueryExecutionError> {
        if self.terminal_control_root().is_none() {
            return Err(contract_error(
                "success request cannot register terminal control",
            ));
        }
        let _ = self.reply.send(Ok(()));
        Ok(())
    }

    pub fn accept(
        self,
        sender: AcceptedRootStatusSender,
        withdrawals: Vec<ResultContextWithdrawal>,
    ) -> Result<(), QueryExecutionError> {
        if self.terminal_control_root().is_some() {
            let error = contract_error("terminal control request cannot seal success");
            let _ = self.reply.send(Err(error.clone()));
            return Err(error);
        }
        let result = sender.seal_success(withdrawals);
        let _ = self.reply.send(result.clone());
        result
    }

    pub fn reject(self, error: QueryExecutionError) {
        let _ = self.reply.send(Err(error));
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum AcceptedRootProjection {
    Observation(Box<AcceptedRootObservation>),
    SuccessSealed {
        status: Box<TaskStatus>,
        withdrawals: Vec<ResultContextWithdrawal>,
    },
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct AcceptedRootObservation {
    status: TaskStatus,
    attempt_failure: AcceptedAttemptFailure,
}

#[doc(hidden)]
#[derive(Clone, Debug, Eq, PartialEq)]
#[expect(
    clippy::large_enum_variant,
    reason = "the public projection keeps the selected attempt termination inline with its accepted root status"
)]
pub enum AcceptedAttemptFailure {
    None,
    DerivedPending,
    Authoritative(novarocks_coordination_contract::SelectedTaskTermination),
}

#[cfg(test)]
fn accepted_root_status_projection(
    root: TaskIdentity,
) -> (AcceptedRootStatusSender, AcceptedRootStatusSource) {
    accepted_root_status_projection_inner(root, None)
}

#[doc(hidden)]
pub fn accepted_root_status_projection_with_control_port(
    root: TaskIdentity,
    root_control_port: Arc<dyn AcceptedRootControlPort>,
) -> (AcceptedRootStatusSender, AcceptedRootStatusSource) {
    accepted_root_status_projection_inner(root, Some(root_control_port))
}

fn accepted_root_status_projection_inner(
    root: TaskIdentity,
    root_control_port: Option<Arc<dyn AcceptedRootControlPort>>,
) -> (AcceptedRootStatusSender, AcceptedRootStatusSource) {
    let (sender, receiver) = watch::channel(None);
    (
        AcceptedRootStatusSender { root, sender },
        AcceptedRootStatusSource {
            root,
            receiver,
            observed: None,
            root_control_port,
        },
    )
}

impl AcceptedRootStatusSender {
    #[cfg(test)]
    fn publish(&self, status: TaskStatus) -> Result<(), QueryExecutionError> {
        let attempt_failure = if status
            .termination()
            .is_some_and(|detail| !detail.is_success_compatible())
        {
            AcceptedAttemptFailure::DerivedPending
        } else {
            AcceptedAttemptFailure::None
        };
        self.publish_attempt_observation(status, attempt_failure)
    }

    /// Atomically publishes the exact root status and the serialized
    /// attempt-level failure latch observed in the same owner turn.
    pub fn publish_attempt_observation(
        &self,
        status: TaskStatus,
        attempt_failure: AcceptedAttemptFailure,
    ) -> Result<(), QueryExecutionError> {
        if let AcceptedAttemptFailure::Authoritative(authoritative) = &attempt_failure
            && (authoritative.detail().is_derived()
                || authoritative.detail().is_success_compatible())
        {
            return Err(contract_error(
                "authoritative attempt failure must be a non-derived failure cause",
            ));
        }
        self.publish_observation(AcceptedRootObservation {
            status,
            attempt_failure,
        })
    }

    /// Publishes the current root status together with the non-derived cause
    /// selected by the same serialized attempt status owner.
    ///
    /// The authoritative attempt failure may originate from another Task, so
    /// the root itself need not be terminal or failed.
    pub fn publish_with_attempt_failure(
        &self,
        status: TaskStatus,
        authoritative_attempt_failure: novarocks_coordination_contract::SelectedTaskTermination,
    ) -> Result<(), QueryExecutionError> {
        self.publish_attempt_observation(
            status,
            AcceptedAttemptFailure::Authoritative(authoritative_attempt_failure),
        )
    }

    fn publish_observation(
        &self,
        observation: AcceptedRootObservation,
    ) -> Result<(), QueryExecutionError> {
        let status = &observation.status;
        if status.identity() != self.root {
            return Err(contract_error("accepted root status names another Task"));
        }
        if let AcceptedAttemptFailure::Authoritative(selected) = &observation.attempt_failure
            && selected.identity().query_execution_id() != self.root.query_execution_id()
        {
            return Err(contract_error(
                "accepted attempt failure names another query execution",
            ));
        }
        if let Some(AcceptedRootProjection::Observation(held)) = self.sender.borrow().as_ref() {
            let cause_refinement = held.status == observation.status
                && matches!(
                    (&held.attempt_failure, &observation.attempt_failure),
                    (
                        AcceptedAttemptFailure::None,
                        AcceptedAttemptFailure::DerivedPending
                            | AcceptedAttemptFailure::Authoritative(_)
                    ) | (
                        AcceptedAttemptFailure::DerivedPending,
                        AcceptedAttemptFailure::Authoritative(_)
                    )
                );
            if status.version() < held.status.version()
                || (status.version() == held.status.version()
                    && observation != **held
                    && !cause_refinement)
                || (held.status.is_terminal() && observation.status != held.status)
            {
                return Err(contract_error(
                    "accepted root status regressed or overwrote a published version",
                ));
            }
            if observation == **held {
                return Ok(());
            }
        } else if self.sender.borrow().is_some() {
            return Err(contract_error(
                "accepted root status cannot be published after success was sealed",
            ));
        }
        self.sender
            .send_replace(Some(AcceptedRootProjection::Observation(Box::new(
                observation,
            ))));
        Ok(())
    }

    /// Consumes the only publisher and seals that the serialized attempt
    /// owner proved success. The carried Finished snapshot lets a watch
    /// receiver observe status and seal atomically even if it skipped the
    /// immediately preceding observation.
    fn seal_success(
        self,
        withdrawals: Vec<ResultContextWithdrawal>,
    ) -> Result<(), QueryExecutionError> {
        let status = match self.sender.borrow().as_ref() {
            Some(AcceptedRootProjection::Observation(observation))
                if observation.status.state() == TaskState::Finished
                    && matches!(observation.attempt_failure, AcceptedAttemptFailure::None) =>
            {
                observation.status.clone()
            }
            _ => {
                return Err(contract_error(
                    "success seal requires a Finished root and no attempt failure",
                ));
            }
        };
        if withdrawals.iter().any(|fact| {
            fact.root() != self.root
                || fact.activation().execution() != self.root.query_execution_id()
                || fact.context().query_execution_id() != self.root.query_execution_id()
                || fact.context().backend_process_id() == self.root.backend_process_id()
        }) {
            return Err(contract_error(
                "root seal contains a foreign result-demand withdrawal",
            ));
        }
        self.sender
            .send_replace(Some(AcceptedRootProjection::SuccessSealed {
                status: Box::new(status),
                withdrawals,
            }));
        Ok(())
    }
}

impl AcceptedRootStatusSource {
    fn try_next(&mut self) -> Option<AcceptedRootProjection> {
        let current = self.receiver.borrow_and_update().clone();
        if let Some(status) = current
            && self.observed.as_ref() != Some(&status)
        {
            self.observed = Some(status.clone());
            Some(status)
        } else {
            None
        }
    }

    async fn next(&mut self) -> Result<AcceptedRootProjection, QueryExecutionError> {
        loop {
            let current = self.receiver.borrow_and_update().clone();
            if let Some(status) = current
                && self.observed.as_ref() != Some(&status)
            {
                self.observed = Some(status.clone());
                return Ok(status);
            }
            self.receiver.changed().await.map_err(|_| {
                contract_error(
                    "accepted root status source closed without an explicit success seal",
                )
            })?;
        }
    }

    pub fn begin_terminal_control_request(
        &self,
    ) -> Result<oneshot::Receiver<Result<(), QueryExecutionError>>, QueryExecutionError> {
        let Some(port) = self.root_control_port.as_ref() else {
            return Err(contract_error(
                "accepted root source has no serialized terminal control port",
            ));
        };
        let (reply, receiver) = oneshot::channel();
        port.enqueue_root_control(AcceptedRootControlRequest {
            kind: AcceptedRootControlRequestKind::AwaitTerminalControl(self.root),
            reply,
        })
        .map_err(|_| contract_error("terminal control request intake is closed"))?;
        Ok(receiver)
    }

    /// Requests the serialized attempt owner's decision after the result owner
    /// consumed exact EOS. This request carries no authority to seal success.
    #[doc(hidden)]
    pub fn begin_success_seal_request(
        &self,
    ) -> Result<oneshot::Receiver<Result<(), QueryExecutionError>>, QueryExecutionError> {
        let Some(port) = self.root_control_port.as_ref() else {
            return Err(contract_error(
                "accepted root status source has no serialized success-seal port",
            ));
        };
        let (reply, outcome) = oneshot::channel();
        port.enqueue_root_control(AcceptedRootControlRequest {
            kind: AcceptedRootControlRequestKind::SuccessSeal,
            reply,
        })
        .map_err(|_| contract_error("success-seal request intake is closed"))?;
        Ok(outcome)
    }
}

#[doc(hidden)]
#[derive(Debug)]
pub enum ResultPumpFailure {
    DecisionPending(ResultPumpDecision),
    Concluded(ConcludedResultPumpFailure),
    ActorOutcomeUnknown(ActorOutcomeUnknownResultPumpFailure),
}

/// An attempt failure whose final retry-or-fail decision still belongs to the
/// running permit owner.
#[doc(hidden)]
#[derive(Debug)]
pub struct ResultPumpDecision {
    permit: RunningAttemptPermit,
    observer: Option<RootResultObserver>,
    failure: RootResultFetchFailure,
}

impl ResultPumpDecision {
    pub const fn pre_ready_replacement(
        &self,
    ) -> Option<&crate::api::PreReadyProcessReplacementWitness> {
        self.failure.pre_ready_replacement()
    }
    pub fn topology_requirement(&self) -> NativeAttemptTopologyRequirement {
        self.failure.topology_requirement()
    }

    pub fn class(&self) -> AttemptFailureClass {
        self.failure.class()
    }
    pub const fn error(&self) -> &QueryExecutionError {
        self.failure.error()
    }

    /// Installs the final typed failure before releasing the last root
    /// observer, so observer loss cannot race and replace the chosen result.
    pub async fn fail_logical(
        self,
        actor: &LogicalExecutionActor,
    ) -> Result<LogicalConclusion, LogicalExecutionActorError> {
        let error = self.failure.error().clone();
        self.fail_logical_with_error(actor, error).await
    }

    /// A successor preparation failed before it produced a new running
    /// permit. The supervisor retains this exact attempt authority and
    /// submits that actual preparation verdict as the logical terminal cause.
    pub(crate) async fn fail_logical_with_error(
        self,
        actor: &LogicalExecutionActor,
        error: QueryExecutionError,
    ) -> Result<LogicalConclusion, LogicalExecutionActorError> {
        let permit = self.permit;
        let result = actor.fail_attempt_with_error(permit, error).await;
        drop(self.observer);
        result
    }

    /// Transfers the retained attempt authority into replacement while the
    /// root observer remains alive through the actor's decision turn.
    pub async fn begin_replacement(
        self,
        actor: &LogicalExecutionActor,
        replacement: QueryExecutionId,
        replacement_contexts: Vec<QueryContextRef>,
    ) -> Result<(ReplacementQualification, QueryExecutionError), LogicalExecutionActorError> {
        let class = self.failure.class();
        let permit = self.permit;
        let qualification = actor
            .begin_replacement_with_error(
                permit,
                class,
                self.failure.error().clone(),
                self.failure.pre_ready_replacement().cloned(),
                replacement,
                replacement_contexts,
            )
            .await?;
        drop(self.observer);
        Ok((qualification, self.failure.error))
    }
}

/// The actor already fixed the logical terminal state; no attempt authority is
/// returned and no caller can ask this value to begin replacement.
#[doc(hidden)]
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ConcludedResultPumpFailure {
    conclusion: LogicalConclusion,
    failure: RootResultFetchFailure,
}

impl ConcludedResultPumpFailure {
    pub const fn conclusion(&self) -> LogicalConclusion {
        self.conclusion
    }

    pub fn class(&self) -> AttemptFailureClass {
        self.failure.class()
    }

    pub const fn error(&self) -> &QueryExecutionError {
        self.failure.error()
    }
}

/// The actor accepted the running-attempt authority, but no actor reply proved
/// which logical conclusion, if any, was fixed.
#[doc(hidden)]
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ActorOutcomeUnknownResultPumpFailure {
    error: QueryExecutionError,
}

impl ActorOutcomeUnknownResultPumpFailure {
    pub const fn error(&self) -> &QueryExecutionError {
        &self.error
    }
}

enum PumpInterruption {
    Decision(RootResultFetchFailure),
    Cancellation(RootResultFetchFailure),
    Concluded(LogicalConclusion, RootResultFetchFailure),
}

struct PumpRuntime {
    observer: RootResultObserver,
    statuses: AcceptedRootStatusSource,
    native_terminal: NativeAttemptTerminalSource,
    cancellation: CancellationView,
    root_finished: bool,
    success_sealed: bool,
    result_withdrawals: Vec<ResultContextWithdrawal>,
    native_completed: bool,
    pending_root_failure: Option<TaskStatus>,
}

impl PumpRuntime {
    fn observe_ready_native_terminal(&mut self) -> Result<(), PumpInterruption> {
        if self.native_completed {
            return Ok(());
        }
        if let Some(terminal) = self
            .native_terminal
            .try_next()
            .map_err(|error| PumpInterruption::Decision(contract_failure(error)))?
        {
            self.observe_native_terminal(Ok(terminal))?;
        }
        Ok(())
    }

    fn observe_native_terminal(
        &mut self,
        terminal: Result<NativeAttemptTerminal, QueryExecutionError>,
    ) -> Result<(), PumpInterruption> {
        match terminal {
            Ok(NativeAttemptTerminal::Completed) => {
                self.native_completed = true;
                Ok(())
            }
            Ok(NativeAttemptTerminal::Failed(failure)) => Err(PumpInterruption::Decision(
                RootResultFetchFailure::from_preparation_failure(failure),
            )),
            Err(error) => Err(PumpInterruption::Decision(contract_failure(error))),
        }
    }

    async fn observe_projection(
        &mut self,
        projection: AcceptedRootProjection,
    ) -> Result<(), PumpInterruption> {
        match projection {
            AcceptedRootProjection::Observation(observation) => {
                self.observe_status(*observation).await
            }
            AcceptedRootProjection::SuccessSealed {
                status,
                withdrawals,
            } => {
                if self.pending_root_failure.is_some() || status.state() != TaskState::Finished {
                    return Err(PumpInterruption::Decision(contract_failure(
                        contract_error("invalid success seal for the accepted root status"),
                    )));
                }
                self.observe_status(AcceptedRootObservation {
                    status: *status,
                    attempt_failure: AcceptedAttemptFailure::None,
                })
                .await?;
                if !self.root_finished {
                    return Err(PumpInterruption::Decision(contract_failure(
                        contract_error("success seal did not carry a Finished root status"),
                    )));
                }
                self.result_withdrawals = withdrawals;
                self.success_sealed = true;
                Ok(())
            }
        }
    }

    async fn observe_status(
        &mut self,
        observation: AcceptedRootObservation,
    ) -> Result<(), PumpInterruption> {
        let AcceptedRootObservation {
            status,
            attempt_failure,
        } = observation;
        if self.pending_root_failure.is_some() {
            if matches!(attempt_failure, AcceptedAttemptFailure::DerivedPending) {
                // Pending causal authority does not freeze the root's Task version.
                // The actor's existing cursor/transition owner validates each
                // exact-root progression and still forbids terminal overwrite.
                return match self
                    .observer
                    .observe_status_with_failure(status.clone(), RootTerminalFailure::Pending)
                    .await
                {
                    Err(LogicalExecutionActorError::RootAttemptTerminal) => {
                        self.pending_root_failure = Some(status);
                        Ok(())
                    }
                    Err(error) => Err(PumpInterruption::Decision(actor_failure(
                        "advance pending root Task status",
                        error,
                    ))),
                    Ok(()) => Err(PumpInterruption::Decision(contract_failure(
                        contract_error(
                            "pending root failure progression did not retain a terminal disposition",
                        ),
                    ))),
                };
            }
            let AcceptedAttemptFailure::Authoritative(authoritative) = attempt_failure else {
                return Err(PumpInterruption::Decision(contract_failure(
                    contract_error(
                        "pending attempt failure cannot lose its causal authority requirement",
                    ),
                )));
            };
            let failure =
                classify_root_termination(&status, Some(&authoritative)).ok_or_else(|| {
                    PumpInterruption::Decision(contract_failure(contract_error(
                        "authoritative attempt failure did not classify the pending root terminal",
                    )))
                })?;
            return match self
                .observer
                .observe_status_with_failure(
                    status,
                    RootTerminalFailure::Authoritative(Box::new(failure.error.clone())),
                )
                .await
            {
                Err(LogicalExecutionActorError::RootAttemptTerminal) => {
                    Err(PumpInterruption::Decision(failure))
                }
                Err(LogicalExecutionActorError::ExecutionConcluded(conclusion)) => {
                    Err(PumpInterruption::Concluded(conclusion, failure))
                }
                Err(error) => Err(PumpInterruption::Decision(actor_failure(
                    "refine root Task failure",
                    error,
                ))),
                Ok(()) => Err(PumpInterruption::Decision(contract_failure(
                    contract_error(
                        "authoritative root failure refinement did not retain a terminal disposition",
                    ),
                ))),
            };
        }
        let finished = status.state() == TaskState::Finished;
        let terminal_failure = match &attempt_failure {
            AcceptedAttemptFailure::None => classify_root_termination(&status, None),
            AcceptedAttemptFailure::DerivedPending => None,
            AcceptedAttemptFailure::Authoritative(authoritative) => {
                classify_root_termination(&status, Some(authoritative))
            }
        };
        let terminal_actor_fact = match (&attempt_failure, terminal_failure.as_ref()) {
            (AcceptedAttemptFailure::DerivedPending, _) => RootTerminalFailure::Pending,
            (_, Some(failure)) => {
                RootTerminalFailure::Authoritative(Box::new(failure.error.clone()))
            }
            _ => RootTerminalFailure::Unspecified,
        };
        match self
            .observer
            .observe_status_with_failure(status.clone(), terminal_actor_fact)
            .await
        {
            Ok(()) => {
                self.root_finished |= finished;
                Ok(())
            }
            Err(LogicalExecutionActorError::RootAttemptTerminal) if terminal_failure.is_some() => {
                Err(PumpInterruption::Decision(terminal_failure.expect(
                    "terminal failure was checked before actor observation",
                )))
            }
            Err(LogicalExecutionActorError::RootAttemptTerminal)
                if matches!(attempt_failure, AcceptedAttemptFailure::DerivedPending) =>
            {
                self.pending_root_failure = Some(status);
                Ok(())
            }
            Err(LogicalExecutionActorError::ExecutionConcluded(conclusion))
                if terminal_failure.is_some() =>
            {
                Err(PumpInterruption::Concluded(
                    conclusion,
                    terminal_failure
                        .expect("terminal failure was checked before actor observation"),
                ))
            }
            Err(error @ LogicalExecutionActorError::ExecutionConcluded(conclusion)) => {
                Err(PumpInterruption::Concluded(
                    conclusion,
                    actor_failure("observe root Task status", error),
                ))
            }
            Err(error) => Err(PumpInterruption::Decision(actor_failure(
                "observe root Task status",
                error,
            ))),
        }
    }

    /// Waits until the serialized Task owner has accepted at least one status
    /// for the exact root. The Frontend publishes this projection only after
    /// Installed proves result capability, or a terminal preparation failure
    /// makes a fetch unnecessary. A result fetch therefore cannot overtake
    /// root installation on its owning Worker.
    async fn await_initial_root_status(&mut self) -> Result<(), PumpInterruption> {
        loop {
            tokio::select! {
                biased;
                reason = self.cancellation.cancelled() => {
                    return Err(PumpInterruption::Cancellation(cancellation_failure(reason)));
                }
                terminal = self.native_terminal.next(), if !self.native_completed => {
                    self.observe_native_terminal(terminal)?;
                }
                status = self.statuses.next() => {
                    let projection = status.map_err(|error| PumpInterruption::Decision(contract_failure(error)))?;
                    self.observe_projection(projection).await?;
                    return Ok(());
                }
            }
        }
    }

    async fn await_step<F, T>(&mut self, future: F) -> Result<T, PumpInterruption>
    where
        F: Future<Output = T>,
    {
        tokio::pin!(future);
        loop {
            if self.pending_root_failure.is_some() {
                tokio::select! {
                    biased;
                    reason = self.cancellation.cancelled() => {
                        return Err(PumpInterruption::Cancellation(cancellation_failure(reason)));
                    }
                    terminal = self.native_terminal.next(), if !self.native_completed => {
                        self.observe_native_terminal(terminal)?;
                    }
                    status = self.statuses.next() => {
                        let projection = status.map_err(|error| PumpInterruption::Decision(contract_failure(error)))?;
                        self.observe_projection(projection).await?;
                    }
                }
                continue;
            }
            if self.success_sealed {
                return tokio::select! {
                    biased;
                    reason = self.cancellation.cancelled() => Err(PumpInterruption::Cancellation(cancellation_failure(reason))),
                    terminal = self.native_terminal.next(), if !self.native_completed => {
                        self.observe_native_terminal(terminal)?;
                        continue;
                    }
                    output = &mut future => {
                        self.observe_ready_native_terminal()?;
                        Ok(output)
                    },
                };
            }
            tokio::select! {
                biased;
                reason = self.cancellation.cancelled() => {
                    return Err(PumpInterruption::Cancellation(cancellation_failure(reason)));
                }
                terminal = self.native_terminal.next(), if !self.native_completed => {
                    self.observe_native_terminal(terminal)?;
                }
                status = self.statuses.next() => {
                    let projection = status.map_err(|error| PumpInterruption::Decision(contract_failure(error)))?;
                    self.observe_projection(projection).await?;
                }
                output = &mut future => {
                    self.observe_ready_native_terminal()?;
                    return Ok(output);
                },
            }
        }
    }

    async fn await_terminal_control(&mut self) -> PumpInterruption {
        if let Some(projection) = self.statuses.try_next()
            && let Err(interruption) = self.observe_projection(projection).await
        {
            return interruption;
        }
        if self.native_completed || self.success_sealed {
            return PumpInterruption::Decision(contract_failure(contract_error(
                "revoked root output has no pending native terminal authority",
            )));
        }
        let reply = match self.statuses.begin_terminal_control_request() {
            Ok(reply) => reply,
            Err(error) => return PumpInterruption::Decision(contract_failure(error)),
        };
        match self
            .await_step(async move {
                reply
                    .await
                    .map_err(|_| contract_error("terminal control registration owner dropped"))?
            })
            .await
        {
            Ok(Ok(())) => {}
            Ok(Err(error)) => return PumpInterruption::Decision(contract_failure(error)),
            Err(interruption) => return interruption,
        }
        if let Some(projection) = self.statuses.try_next()
            && let Err(interruption) = self.observe_projection(projection).await
        {
            return interruption;
        }
        if self.native_completed || self.success_sealed {
            return PumpInterruption::Decision(contract_failure(contract_error(
                "revoked root output has no pending native terminal authority",
            )));
        }
        // No further result fetch or ACK is issued. The existing Task owner
        // applies its bounded observation/transport recovery and publishes
        // either an originating failure or an explicit recovery failure.
        loop {
            tokio::select! {
                biased;
                reason = self.cancellation.cancelled() => {
                    return PumpInterruption::Cancellation(cancellation_failure(reason));
                }
                status = self.statuses.next() => {
                    let projection = match status {
                        Ok(projection) => projection,
                        Err(error) => return PumpInterruption::Decision(contract_failure(error)),
                    };
                    if let Err(interruption) = self.observe_projection(projection).await {
                        return interruption;
                    }
                    if self.success_sealed {
                        return PumpInterruption::Decision(contract_failure(contract_error(
                            "revoked root output received a success seal",
                        )));
                    }
                }
                terminal = self.native_terminal.next(), if !self.native_completed => {
                    if let Err(interruption) = self.observe_native_terminal(terminal) {
                        return interruption;
                    }
                    return PumpInterruption::Decision(contract_failure(contract_error(
                        "revoked root output completed without an originating Task failure",
                    )));
                }
            }
        }
    }

    async fn await_success_seal(&mut self) -> Result<(), PumpInterruption> {
        // The serialized Task owner publishes SuccessSealed only after the
        // exact root is Finished, no authoritative attempt failure exists,
        // and every required Task creation was accepted. That is the logical
        // result boundary. Native terminal convergence may still be waiting
        // for transport ownership or residual Worker cleanup and must remain
        // observable without delaying the actor-owned success EOF.
        while !self.success_sealed {
            tokio::select! {
                biased;
                reason = self.cancellation.cancelled() => {
                    return Err(PumpInterruption::Cancellation(cancellation_failure(reason)));
                }
                terminal = self.native_terminal.next(), if !self.native_completed => {
                    self.observe_native_terminal(terminal)?;
                }
                status = self.statuses.next() => {
                    let projection = status.map_err(|error| PumpInterruption::Decision(contract_failure(error)))?;
                    self.observe_projection(projection).await?;
                }
            }
        }
        Ok(())
    }
}

/// One root stream relayed to its consumer without decode: the read port,
/// the frontier that decides each read, and the window alias every reply
/// retains until its delivery exits.
pub struct RootRelayBinding {
    pub port: Arc<dyn BoundedRootReadPort>,
    pub frontier: super::RootRelayFrontier,
    pub window: novarocks_workload_control::ResultWindowAlias,
    pub max_wait: std::time::Duration,
}

enum RelayEvent {
    Delivered(Result<(), LogicalExecutionActorError>),
    Read(Result<RetainedRootReply, RootResultFetchFailure>),
}

enum RelayExit {
    EndConsumed(
        super::RootRelayRead,
        novarocks_execution_contract::root_result::RootResultEnd,
    ),
    Interruption(PumpInterruption),
    Failure(RootResultFetchFailure),
    Actor(&'static str, LogicalExecutionActorError),
    AwaitTerminalControl,
}

type DeliveryFuture<'a> =
    Pin<Box<dyn Future<Output = Result<(), LogicalExecutionActorError>> + Send + 'a>>;
type ReadFuture =
    Pin<Box<dyn Future<Output = Result<RetainedRootReply, RootResultFetchFailure>> + Send>>;

/// Minted only after this original pump consumed Root End, drained its
/// real delivery/read slots and obtained its final actor success or exact Internal Ready. Moving this
/// token out is not P retirement: the supervisor consumes it after the pump
/// and its driver frame have returned normally.
pub(crate) struct StreamedRowsComputedCut {
    source: crate::admitted_query_context::StreamedComputationCutHandle,
    _activation: novarocks_coordination_contract::AttemptActivationIdentity,
    _root: TaskIdentity,
    sealing: Result<(), WorkError>,
}
impl StreamedRowsComputedCut {
    pub(crate) fn is_for(
        &self,
        source: &crate::admitted_query_context::StreamedComputationCutHandle,
    ) -> bool {
        self.source.same_source(source)
    }
    pub(super) async fn retire_after_pump_exit(self) -> Result<(), WorkError> {
        self.sealing.clone()?;
        let source = self.source.clone();
        source.retire_after_pump_exit(self).await
    }
}
pub(super) struct RootPumpDecision {
    pub(super) conclusion: LogicalConclusion,
    pub(super) streamed_cut: Option<StreamedRowsComputedCut>,
}
pub(super) async fn run_streamed_root_relay(
    permit: RunningAttemptPermit,
    root: TaskIdentity,
    scope: WorkScope,
    binding: RootRelayBinding,
    statuses: AcceptedRootStatusSource,
    native_terminal: NativeAttemptTerminalSource,
    root_ack: super::RootAckTailReservation,
    streamed: Option<crate::admitted_query_context::StreamedComputationCutHandle>,
) -> Result<RootPumpDecision, ResultPumpFailure> {
    run_root_relay_core(
        permit,
        root,
        scope,
        binding,
        statuses,
        native_terminal,
        root_ack,
        None,
        streamed,
    )
    .await
}

/// Relays one exact attempt's Backend-encoded root stream in order. The next
/// item is read while the previous one is written; the cumulative ACK on
/// each read is the prefix whose delivery receipts completed. Success seal
/// follows the local End consumption proof and the root's terminal facts; it
/// never waits for the Backend to acknowledge the final item.
#[doc(hidden)]
#[cfg(test)]
pub(crate) async fn run_root_relay(
    permit: RunningAttemptPermit,
    root: TaskIdentity,
    scope: WorkScope,
    binding: RootRelayBinding,
    statuses: AcceptedRootStatusSource,
    native_terminal: NativeAttemptTerminalSource,
    root_ack: super::RootAckTailReservation,
) -> Result<LogicalConclusion, ResultPumpFailure> {
    run_root_relay_core(
        permit,
        root,
        scope,
        binding,
        statuses,
        native_terminal,
        root_ack,
        None,
        None,
    )
    .await
    .map(|decision| decision.conclusion)
}

/// Relays provisional internal output while retaining the original attempt
/// authority until the registered domain proof establishes its computation cut.
pub(super) async fn run_internal_root_relay(
    permit: RunningAttemptPermit,
    root: TaskIdentity,
    scope: WorkScope,
    binding: RootRelayBinding,
    statuses: AcceptedRootStatusSource,
    native_terminal: NativeAttemptTerminalSource,
    root_ack: super::RootAckTailReservation,
    ready: watch::Receiver<Option<Result<ComputationReadyToken, QueryExecutionError>>>,
    streamed: Option<crate::admitted_query_context::StreamedComputationCutHandle>,
) -> Result<RootPumpDecision, ResultPumpFailure> {
    run_root_relay_core(
        permit,
        root,
        scope,
        binding,
        statuses,
        native_terminal,
        root_ack,
        Some(ready),
        streamed,
    )
    .await
}

async fn run_root_relay_core(
    permit: RunningAttemptPermit,
    root: TaskIdentity,
    scope: WorkScope,
    binding: RootRelayBinding,
    statuses: AcceptedRootStatusSource,
    native_terminal: NativeAttemptTerminalSource,
    root_ack: super::RootAckTailReservation,
    ready: Option<watch::Receiver<Option<Result<ComputationReadyToken, QueryExecutionError>>>>,
    streamed: Option<crate::admitted_query_context::StreamedComputationCutHandle>,
) -> Result<RootPumpDecision, ResultPumpFailure> {
    let RootRelayBinding {
        port,
        mut frontier,
        window,
        max_wait,
    } = binding;
    if streamed
        .as_ref()
        .is_some_and(|streamed| !streamed.is_for_scope(&scope))
        || statuses.root != root
        || frontier.root() != root
        || permit.identity().execution() != root.query_execution_id()
        || !window.is_for_scope(&scope)
        || window.class()
            != if ready.is_some() {
                novarocks_workload_control::ResultWindowClass::Internal
            } else {
                match frontier.kind() {
                    novarocks_result_contract::RootOutputKind::ClientRows => {
                        novarocks_workload_control::ResultWindowClass::Client
                    }
                    _ => novarocks_workload_control::ResultWindowClass::Internal,
                }
            }
    {
        return Err(pump_failure(
            permit,
            contract_failure(contract_error(
                "root relay identity does not match its attempt, status source, frontier or admitted window",
            )),
        ));
    }
    let cancellation = match scope.cancellation() {
        Ok(cancellation) => cancellation,
        Err(error) => return Err(pump_failure(permit, work_failure(error))),
    };
    if let Some(reason) = cancellation.reason() {
        return Err(conclude_cancellation(None, permit, cancellation_failure(reason)).await);
    }
    let observer = match permit.bind_root_result(root).await {
        Ok(observer) => observer,
        Err(error) => {
            if let Some(reason) = cancellation.reason() {
                return Err(
                    conclude_cancellation(None, permit, cancellation_failure(reason)).await,
                );
            }
            return Err(pump_actor_failure(permit, "bind root result", error));
        }
    };
    let observer_owner = observer;
    let mut runtime = PumpRuntime {
        observer: observer_owner.clone(),
        statuses,
        native_terminal,
        cancellation,
        root_finished: false,
        success_sealed: false,
        result_withdrawals: Vec::new(),
        native_completed: false,
        pending_root_failure: None,
    };
    if let Err(interruption) = runtime.await_initial_root_status().await {
        return Err(bound_pump_interruption(&observer_owner, permit, interruption).await);
    }

    let exit = {
        // This slot survives a pump cancellation while its delivered item
        // still owns the window; only the protocol cut may take it for closing.
        let resident = crate::api::RootRelayResidentWindow::default();
        let mut delivering: Option<(NonZeroU64, DeliveryFuture<'_>)> = None;
        let mut reading: Option<ReadFuture> = None;
        loop {
            if delivering.is_none()
                && let Some(crate::api::ResidentRootSegment {
                    sequence,
                    reply,
                    client_rows,
                    rows,
                }) = resident.take()
            {
                let packet = ResultPacketSequence::new(sequence.get() - 1);
                delivering = Some((
                    sequence,
                    Box::pin(permit.deliver_root_segment(
                        packet,
                        reply,
                        client_rows,
                        rows,
                        resident.clone(),
                    )),
                ));
            }
            if let Some(end) = frontier.end_consumed()
                && delivering.is_none()
                && resident.is_empty()
                && reading.is_none()
            {
                // The final ACK-only read is optional and never awaited here.
                let read = frontier.next_read().unwrap_or(super::RootRelayRead {
                    wanted: None,
                    consumed: frontier.consumed_through(),
                });
                break RelayExit::EndConsumed(read, end);
            }
            if reading.is_none()
                && let Some(read) = frontier.next_read()
                && read.wanted.is_some()
            {
                let request = match RootResultRead::try_new(
                    root,
                    frontier.profile(),
                    frontier.kind(),
                    read.wanted,
                    read.consumed,
                    max_wait,
                ) {
                    Ok(request) => request,
                    Err(error) => {
                        use novarocks_execution_contract::failure::FailureProjection;
                        break RelayExit::Failure(RootResultFetchFailure::from_input(
                            crate::execution_error::ExecutionFailureInput::new(
                                error.failure_fact(),
                                novarocks_execution_contract::SafeDetail::truncating(
                                    "root relay read does not satisfy its frozen request contract",
                                ),
                                crate::execution_error::ExecutionFailureSite::RootRead { root },
                            ),
                        ));
                    }
                };
                frontier.sent(read);
                reading = Some(paced_root_read(port.as_ref(), request, window.clone()));
            }
            if delivering.is_none() && reading.is_none() {
                break RelayExit::Failure(contract_failure(contract_error(
                    "root relay has neither a delivery nor a read in progress",
                )));
            }
            let event = runtime
                .await_step(std::future::poll_fn(|cx| {
                    if let Some((_, future)) = delivering.as_mut()
                        && let std::task::Poll::Ready(result) = future.as_mut().poll(cx)
                    {
                        return std::task::Poll::Ready(RelayEvent::Delivered(result));
                    }
                    if let Some(future) = reading.as_mut()
                        && let std::task::Poll::Ready(result) = future.as_mut().poll(cx)
                    {
                        return std::task::Poll::Ready(RelayEvent::Read(result));
                    }
                    std::task::Poll::Pending
                }))
                .await;
            match event {
                Err(interruption) => break RelayExit::Interruption(interruption),
                Ok(RelayEvent::Delivered(result)) => {
                    let (sequence, _) = delivering.take().expect("a delivery completed");
                    if let Err(error) = result {
                        break RelayExit::Actor("deliver root segment", error);
                    }
                    resident.retire(sequence);
                    if let Err(error) = frontier.receipt(sequence) {
                        break RelayExit::Failure(relay_failure(root, error));
                    }
                }
                Ok(RelayEvent::Read(result)) => {
                    reading = None;
                    let reply = match result {
                        Ok(reply) => reply,
                        Err(failure) => break RelayExit::Failure(failure),
                    };
                    let client_rows = frontier.client_rows();
                    let body = match reply.outcome() {
                        crate::api::RootReplyView::Data { body, .. } => Some(body),
                        _ => None,
                    };
                    let step = frontier.accept(reply.response(), body);
                    match step {
                        Err(error) => {
                            break RelayExit::Failure(relay_failure(root, error));
                        }
                        Ok(super::RootRelayStep::Deliver { sequence, rows, .. }) => {
                            if !resident.publish(crate::api::ResidentRootSegment {
                                sequence,
                                reply: Arc::new(reply),
                                client_rows,
                                rows,
                            }) {
                                break RelayExit::AwaitTerminalControl;
                            }
                        }
                        Ok(super::RootRelayStep::AwaitTerminalControl) => {
                            break RelayExit::AwaitTerminalControl;
                        }
                        Ok(super::RootRelayStep::NotReady | super::RootRelayStep::Deferred(_)) => {}
                        Ok(super::RootRelayStep::Refused(refusal)) => {
                            break RelayExit::Failure(RootResultFetchFailure::from_input(
                                crate::execution_error::ExecutionFailureInput::new(
                                    refusal.fact(),
                                    refusal.detail().clone(),
                                    crate::execution_error::ExecutionFailureSite::RootRead { root },
                                ),
                            ));
                        }
                        Ok(
                            super::RootRelayStep::EndKnown(_) | super::RootRelayStep::Acknowledged,
                        ) => {}
                    }
                }
            }
        }
    };
    let (final_read, end) = match exit {
        RelayExit::EndConsumed(read, end) => (read, end),
        RelayExit::Interruption(interruption) => {
            return Err(bound_pump_interruption(&observer_owner, permit, interruption).await);
        }
        RelayExit::Failure(failure) => {
            return Err(bound_pump_failure(&observer_owner, permit, failure).await);
        }
        RelayExit::Actor(operation, error) => {
            return Err(bound_actor_failure(&observer_owner, permit, operation, error).await);
        }
        RelayExit::AwaitTerminalControl => {
            let interruption = runtime.await_terminal_control().await;
            return Err(bound_pump_interruption(&observer_owner, permit, interruption).await);
        }
    };
    // Optional early retirement of the consumed prefix on the Backend. It is
    // neither a seal precondition nor awaited for success.
    if final_read.wanted.is_none()
        && let Ok(request) = RootResultRead::try_new(
            root,
            frontier.profile(),
            frontier.kind(),
            None,
            final_read.consumed,
            max_wait,
        )
    {
        let ack = port.read(request, window.clone());
        root_ack.start(ack);
    }
    // The local End consumption proof: every data item's delivery completed.
    if let Err(error) = runtime.observer.observe_local_root_end(root, end).await {
        return Err(bound_actor_failure(
            &observer_owner,
            permit,
            "observe local root End consumption",
            error,
        )
        .await);
    }
    if let Some(ready) = ready {
        let activation = permit.identity();
        return finish_internal_relay(&mut runtime, &observer_owner, permit, root, end, ready)
            .await
            .map(|conclusion| {
                let streamed_cut = if conclusion == LogicalConclusion::ComputationReady {
                    streamed.map(|source| {
                        let sealing = source.seal_producer();
                        StreamedRowsComputedCut {
                            source,
                            _activation: activation,
                            _root: root,
                            sealing,
                        }
                    })
                } else {
                    None
                };
                RootPumpDecision {
                    conclusion,
                    streamed_cut,
                }
            });
    }
    if !runtime.success_sealed {
        let seal_reply = match runtime.statuses.begin_success_seal_request() {
            Ok(reply) => reply,
            Err(error) => {
                return Err(
                    bound_pump_failure(&observer_owner, permit, contract_failure(error)).await,
                );
            }
        };
        let seal_result = runtime
            .await_step(async move {
                seal_reply.await.map_err(|_| {
                    contract_error("success-seal request owner dropped without a verdict")
                })?
            })
            .await;
        match seal_result {
            Ok(Ok(())) => {}
            Ok(Err(error)) => {
                return Err(
                    bound_pump_failure(&observer_owner, permit, contract_failure(error)).await,
                );
            }
            Err(interruption) => {
                return Err(bound_pump_interruption(&observer_owner, permit, interruption).await);
            }
        }
    }
    if let Err(interruption) = runtime.await_success_seal().await {
        return Err(bound_pump_interruption(&observer_owner, permit, interruption).await);
    }
    let activation = permit.identity();
    match permit
        .finish_result_stream(std::mem::take(&mut runtime.result_withdrawals))
        .await
    {
        Ok(conclusion) => {
            let streamed_cut = if conclusion == LogicalConclusion::Succeeded {
                if let Some(source) = streamed {
                    let sealing = source.seal_producer();
                    Some(StreamedRowsComputedCut {
                        source,
                        _activation: activation,
                        _root: root,
                        sealing,
                    })
                } else {
                    None
                }
            } else {
                None
            };
            Ok(RootPumpDecision {
                conclusion,
                streamed_cut,
            })
        }
        Err(error) => Err(finish_handoff_failure(observer_owner, error).await),
    }
}

fn internal_ready_accepted(
    ready: &watch::Receiver<Option<Result<ComputationReadyToken, QueryExecutionError>>>,
    activation: novarocks_coordination_contract::AttemptActivationIdentity,
    root: TaskIdentity,
    end: novarocks_execution_contract::root_result::RootResultEnd,
) -> Result<bool, RootResultFetchFailure> {
    let Some(result) = ready.borrow().clone() else {
        return Ok(false);
    };
    let token = result.map_err(contract_failure)?;
    if token.activation() != activation || token.root() != root || token.end() != end {
        return Err(contract_failure(contract_error(
            "internal readiness does not match the relay activation, root or consumed End",
        )));
    }
    Ok(true)
}

async fn finish_internal_relay(
    runtime: &mut PumpRuntime,
    observer: &RootResultObserver,
    permit: RunningAttemptPermit,
    root: TaskIdentity,
    end: novarocks_execution_contract::root_result::RootResultEnd,
    mut ready: watch::Receiver<Option<Result<ComputationReadyToken, QueryExecutionError>>>,
) -> Result<LogicalConclusion, ResultPumpFailure> {
    // The End only borrows running authority. Its real CPU receipt may fail,
    // and the same permit must remain available to the pre-ready recovery owner.
    let delivery = runtime
        .await_step(permit.deliver_internal_end(root, end))
        .await;
    let interruption = match delivery {
        Ok(Ok(())) => None,
        Ok(Err(error)) => {
            let failure = actor_failure("deliver internal root End", error);
            Some(match actor_error_conclusion(error) {
                Some(conclusion) => PumpInterruption::Concluded(conclusion, failure),
                None => PumpInterruption::Decision(failure),
            })
        }
        Err(interruption) => Some(interruption),
    };
    if let Some(interruption) = interruption {
        return settle_internal_interruption(observer, permit, &ready, root, end, interruption)
            .await;
    }

    loop {
        match internal_ready_accepted(&ready, permit.identity(), root, end) {
            Ok(true) => return Ok(LogicalConclusion::ComputationReady),
            Err(failure) => {
                return Err(bound_pump_failure(observer, permit, failure).await);
            }
            Ok(false) => {}
        }
        // Ready is the actor's accepted cut, so it precedes observation of a
        // simultaneous cancellation or a status that arrived after that cut.
        let interruption = tokio::select! {
            biased;
            changed = ready.changed() => {
                if changed.is_err() {
                    Some(PumpInterruption::Decision(contract_failure(contract_error(
                        "internal readiness owner closed without its accepted cut",
                    ))))
                } else {
                    None
                }
            }
            reason = runtime.cancellation.cancelled() => {
                Some(PumpInterruption::Cancellation(cancellation_failure(reason)))
            }
            terminal = runtime.native_terminal.next(), if !runtime.native_completed => {
                runtime.observe_native_terminal(terminal).err()
            }
            status = runtime.statuses.next() => {
                match status {
                    Ok(projection) => runtime.observe_projection(projection).await.err(),
                    Err(error) => Some(PumpInterruption::Decision(contract_failure(error))),
                }
            }
        };
        if let Some(interruption) = interruption {
            return settle_internal_interruption(observer, permit, &ready, root, end, interruption)
                .await;
        }
    }
}

async fn settle_internal_interruption(
    observer: &RootResultObserver,
    permit: RunningAttemptPermit,
    ready: &watch::Receiver<Option<Result<ComputationReadyToken, QueryExecutionError>>>,
    root: TaskIdentity,
    end: novarocks_execution_contract::root_result::RootResultEnd,
    interruption: PumpInterruption,
) -> Result<LogicalConclusion, ResultPumpFailure> {
    let activation = permit.identity();
    match internal_ready_accepted(ready, activation, root, end) {
        Ok(true) => return Ok(LogicalConclusion::ComputationReady),
        Err(failure) => return Err(bound_pump_failure(observer, permit, failure).await),
        Ok(false) => {}
    }
    let failure = bound_pump_interruption(observer, permit, interruption).await;
    // The actor can accept Ready while the pump is awaiting its failure or
    // cancellation disposition. That exact accepted cut remains authoritative.
    if matches!(
        &failure,
        ResultPumpFailure::Concluded(concluded)
            if concluded.conclusion() == LogicalConclusion::ComputationReady
    ) && matches!(
        internal_ready_accepted(ready, activation, root, end),
        Ok(true)
    ) {
        Ok(LogicalConclusion::ComputationReady)
    } else {
        Err(failure)
    }
}

/// Preserve one read and its original window while an early normal deferral
/// consumes the wait already requested from the backend. It grants no retry
/// authority and never advances the cumulative delivery frontier.
#[doc(hidden)]
pub fn paced_root_read(
    port: &dyn BoundedRootReadPort,
    request: RootResultRead,
    physical_guard: novarocks_workload_control::ResultWindowAlias,
) -> Pin<Box<dyn Future<Output = Result<RetainedRootReply, RootResultFetchFailure>> + Send>> {
    let not_before = tokio::time::Instant::now() + request.max_wait();
    let read = port.read(request, physical_guard);
    Box::pin(async move {
        let response = read.await?;
        if matches!(
            response.outcome(),
            crate::api::RootReplyView::Deferred(_) | crate::api::RootReplyView::NotReady
        ) {
            tokio::time::sleep_until(not_before).await;
        }
        Ok(response)
    })
}

pub(crate) fn pump_failure(
    permit: RunningAttemptPermit,
    failure: RootResultFetchFailure,
) -> ResultPumpFailure {
    ResultPumpFailure::DecisionPending(ResultPumpDecision {
        permit,
        observer: None,
        failure,
    })
}

pub(super) fn pump_actor_failure(
    permit: RunningAttemptPermit,
    operation: &'static str,
    error: LogicalExecutionActorError,
) -> ResultPumpFailure {
    let conclusion = actor_error_conclusion(error);
    let failure = actor_failure(operation, error);
    if let Some(conclusion) = conclusion {
        drop(permit);
        concluded_pump_failure(conclusion, failure)
    } else {
        pump_failure(permit, failure)
    }
}

async fn bound_pump_failure(
    observer: &RootResultObserver,
    permit: RunningAttemptPermit,
    failure: RootResultFetchFailure,
) -> ResultPumpFailure {
    match permit
        .freeze_result_attempt_failure(failure.error.clone())
        .await
    {
        Ok(()) => ResultPumpFailure::DecisionPending(ResultPumpDecision {
            permit,
            observer: Some(observer.clone()),
            failure,
        }),
        Err(LogicalExecutionActorError::ExecutionConcluded(conclusion)) => {
            drop(permit);
            ResultPumpFailure::Concluded(ConcludedResultPumpFailure {
                conclusion,
                failure,
            })
        }
        Err(error) => {
            drop(permit);
            ResultPumpFailure::ActorOutcomeUnknown(ActorOutcomeUnknownResultPumpFailure {
                error: actor_failure("freeze result attempt failure", error).error,
            })
        }
    }
}

async fn bound_actor_failure(
    observer: &RootResultObserver,
    permit: RunningAttemptPermit,
    operation: &'static str,
    error: LogicalExecutionActorError,
) -> ResultPumpFailure {
    let conclusion = actor_error_conclusion(error);
    let failure = actor_failure(operation, error);
    if let Some(conclusion) = conclusion {
        drop(permit);
        concluded_pump_failure(conclusion, failure)
    } else {
        bound_pump_failure(observer, permit, failure).await
    }
}

async fn bound_pump_interruption(
    observer: &RootResultObserver,
    permit: RunningAttemptPermit,
    interruption: PumpInterruption,
) -> ResultPumpFailure {
    match interruption {
        PumpInterruption::Decision(failure) => bound_pump_failure(observer, permit, failure).await,
        PumpInterruption::Cancellation(failure) => {
            conclude_cancellation(Some(observer.clone()), permit, failure).await
        }
        PumpInterruption::Concluded(conclusion, failure) => {
            drop(permit);
            ResultPumpFailure::Concluded(ConcludedResultPumpFailure {
                conclusion,
                failure,
            })
        }
    }
}

async fn conclude_cancellation(
    observer: Option<RootResultObserver>,
    permit: RunningAttemptPermit,
    failure: RootResultFetchFailure,
) -> ResultPumpFailure {
    match permit.await_work_cancellation().await {
        Ok(conclusion) | Err(RunningAttemptHandoffError::ExecutionConcluded(conclusion)) => {
            drop(observer);
            concluded_pump_failure(conclusion, failure)
        }
        Err(RunningAttemptHandoffError::NotSubmitted { permit, .. }) => {
            if let Some(observer) = observer {
                bound_pump_failure(&observer, permit, failure).await
            } else {
                pump_failure(permit, failure)
            }
        }
        Err(RunningAttemptHandoffError::ActorOutcomeUnknown(error)) => {
            drop(observer);
            actor_outcome_unknown("await work cancellation", error)
        }
    }
}

fn concluded_pump_failure(
    conclusion: LogicalConclusion,
    failure: RootResultFetchFailure,
) -> ResultPumpFailure {
    ResultPumpFailure::Concluded(ConcludedResultPumpFailure {
        conclusion,
        failure,
    })
}

async fn finish_handoff_failure(
    observer: RootResultObserver,
    error: RunningAttemptHandoffError,
) -> ResultPumpFailure {
    match error {
        RunningAttemptHandoffError::ExecutionConcluded(conclusion) => {
            drop(observer);
            concluded_pump_failure(
                conclusion,
                actor_failure(
                    "finish root result stream",
                    LogicalExecutionActorError::ExecutionConcluded(conclusion),
                ),
            )
        }
        RunningAttemptHandoffError::NotSubmitted { permit, error } => {
            bound_actor_failure(&observer, permit, "finish root result stream", error).await
        }
        RunningAttemptHandoffError::ActorOutcomeUnknown(error) => {
            drop(observer);
            actor_outcome_unknown("finish root result stream", error)
        }
    }
}

fn actor_error_conclusion(error: LogicalExecutionActorError) -> Option<LogicalConclusion> {
    match error {
        LogicalExecutionActorError::ExecutionConcluded(conclusion) => Some(conclusion),
        _ => None,
    }
}

fn actor_outcome_unknown(
    operation: &'static str,
    error: LogicalExecutionActorError,
) -> ResultPumpFailure {
    ResultPumpFailure::ActorOutcomeUnknown(ActorOutcomeUnknownResultPumpFailure {
        error: actor_failure(operation, error).error,
    })
}

fn contract_error(message: impl Into<Arc<str>>) -> QueryExecutionError {
    QueryExecutionError::new(QueryExecutionErrorKind::InvalidRequest, message)
}

fn query_failure(
    reason: crate::execution_error::QueryFailureReason,
    detail: &'static str,
) -> RootResultFetchFailure {
    use novarocks_execution_contract::failure::FailureProjection;
    RootResultFetchFailure::from_input(crate::execution_error::ExecutionFailureInput::new(
        reason.failure_fact(),
        novarocks_execution_contract::SafeDetail::truncating(detail),
        crate::execution_error::ExecutionFailureSite::Coordinator,
    ))
}
fn relay_failure(root: TaskIdentity, error: super::RootRelayError) -> RootResultFetchFailure {
    use novarocks_execution_contract::failure::FailureProjection;
    RootResultFetchFailure::from_input(crate::execution_error::ExecutionFailureInput::new(
        error.failure_fact(),
        error.safe_detail(),
        crate::execution_error::ExecutionFailureSite::RootRead { root },
    ))
}

pub(crate) fn contract_failure(error: QueryExecutionError) -> RootResultFetchFailure {
    if let Some(input) = error.execution_failure() {
        return RootResultFetchFailure::from_input(input.clone());
    }
    tracing::debug!(error = %error, "local root result contract failure");
    query_failure(
        crate::execution_error::QueryFailureReason::ResultProtocol,
        "root result protocol contract failed",
    )
}
fn actor_failure(
    operation: &'static str,
    error: LogicalExecutionActorError,
) -> RootResultFetchFailure {
    tracing::debug!(operation, error = %error, "root result actor operation failed");
    use novarocks_execution_contract::failure::FailureProjection;
    RootResultFetchFailure::from_input(crate::execution_error::ExecutionFailureInput::new(
        error.failure_fact(),
        error.safe_detail(),
        crate::execution_error::ExecutionFailureSite::Coordinator,
    ))
}
fn work_failure(error: WorkError) -> RootResultFetchFailure {
    // The workload domain owns this projection; capacity wait expiry does not
    // claim that the logical query deadline or a backend process expired.
    use novarocks_execution_contract::failure::FailureProjection;
    RootResultFetchFailure::from_input(crate::execution_error::ExecutionFailureInput::new(
        error.failure_fact(),
        novarocks_execution_contract::SafeDetail::truncating(
            "result workload control refused the operation",
        ),
        crate::execution_error::ExecutionFailureSite::Coordinator,
    ))
}
fn cancellation_failure(reason: CancellationReason) -> RootResultFetchFailure {
    use crate::execution_error::QueryFailureReason;
    match reason {
        CancellationReason::DeadlineExceeded
        | CancellationReason::FrontendDrainDeadlineExceeded => query_failure(
            QueryFailureReason::DeadlineExceeded,
            "logical execution deadline expired while pumping results",
        ),
        _ => query_failure(
            QueryFailureReason::Cancelled,
            "logical execution was cancelled while pumping results",
        ),
    }
}
fn classify_root_termination(
    status: &TaskStatus,
    authoritative_attempt_failure: Option<
        &novarocks_coordination_contract::SelectedTaskTermination,
    >,
) -> Option<RootResultFetchFailure> {
    let selected;
    let original = match authoritative_attempt_failure {
        Some(original) => original,
        None => {
            selected = novarocks_coordination_contract::SelectedTaskTermination::new(
                status.identity(),
                status.termination()?.clone(),
            );
            &selected
        }
    };
    if original.detail().is_derived() {
        return Some(contract_failure(contract_error(
            "derived root termination lacks its authoritative Task cause",
        )));
    }
    crate::execution_error::ExecutionFailureInput::from_selected_task(original)
        .map(RootResultFetchFailure::from_input)
}

#[cfg(test)]
mod tests {
    #[test]
    fn actor_boundary_preserves_nested_fact_and_stopped_observation() {
        use novarocks_execution_contract::failure::{FailureEvidence, FailureProjection};
        let source = crate::coordination::EstablishIssueError::TicketExpired;
        let error = LogicalExecutionActorError::Establish(source);
        let projected = actor_failure("authorize Establish", error);
        assert_eq!(projected.input().fact(), source.failure_fact());
        assert_eq!(projected.input().detail(), &source.safe_detail());
        assert_eq!(projected.class(), AttemptFailureClass::ExecutionFailure);
        assert_eq!(
            projected.topology_requirement(),
            NativeAttemptTopologyRequirement::LiveSnapshot
        );

        let projected = actor_failure(
            "observe actor",
            LogicalExecutionActorError::AlreadyConcluded,
        );
        assert_eq!(
            projected.input().fact().evidence(),
            FailureEvidence::StopObservation
        );
        assert_eq!(projected.error().kind(), QueryExecutionErrorKind::Failed);
        assert_eq!(projected.class(), AttemptFailureClass::ExecutionFailure);
    }

    use std::{
        collections::VecDeque,
        num::NonZeroUsize,
        sync::{Arc, Mutex},
        time::Duration,
    };

    use arrow::datatypes::DataType;
    use novarocks_execution_contract::{
        AbortCause, SafeDetail, TaskFailure, TaskOutputFacts, TaskState, TaskStatusVersion,
        TerminationDetail,
    };
    use novarocks_types::{
        AttemptId, BackendProcessId, QueryId, StageId, TaskId, identity::QueryExecutionId,
    };
    use novarocks_workload_control::{
        ResourceConfig, Stage, WorkClass, WorkRequest, WorkloadConfig, WorkloadControl,
    };
    use tokio::{runtime::Handle, sync::Notify};

    use crate::api::{ExecutionOutput, ResultDelivery, ResultField, ResultSchema};
    use crate::coordination::{
        LogicalExecutionActor, LogicalExecutionActorConfig, spawn_logical_execution_actor,
    };
    use novarocks_coordination_contract::ExecutionEffect;

    use super::*;

    #[derive(Debug, Default)]
    struct TestSuccessSealPort {
        requests: Mutex<VecDeque<AcceptedRootControlRequest>>,
        ready: Notify,
        terminal_roots: Mutex<VecDeque<TaskIdentity>>,
        terminal_ready: Notify,
    }

    impl AcceptedRootControlPort for TestSuccessSealPort {
        fn enqueue_root_control(
            &self,
            request: AcceptedRootControlRequest,
        ) -> Result<(), AcceptedRootControlRequest> {
            if let Some(root) = request.terminal_control_root() {
                request.accept_terminal_control().unwrap();
                self.terminal_roots.lock().unwrap().push_back(root);
                self.terminal_ready.notify_one();
                return Ok(());
            }
            self.requests.lock().unwrap().push_back(request);
            self.ready.notify_one();
            Ok(())
        }
    }

    impl TestSuccessSealPort {
        async fn next_terminal_control(&self) -> TaskIdentity {
            loop {
                if let Some(root) = self.terminal_roots.lock().unwrap().pop_front() {
                    return root;
                }
                self.terminal_ready.notified().await;
            }
        }

        async fn next(&self) -> AcceptedRootControlRequest {
            loop {
                if let Some(request) = self.requests.lock().unwrap().pop_front() {
                    return request;
                }
                self.ready.notified().await;
            }
        }
    }

    async fn poll_once<F: Future + ?Sized>(mut future: Pin<&mut F>) -> std::task::Poll<F::Output> {
        std::future::poll_fn(|cx| std::task::Poll::Ready(future.as_mut().poll(cx))).await
    }

    async fn wait_for_terminal_control_registration<F: Future + ?Sized>(
        pump: Pin<&mut F>,
        port: &TestSuccessSealPort,
        root: TaskIdentity,
    ) {
        tokio::time::timeout(Duration::from_secs(10), async {
            tokio::select! {
                _ = pump => panic!("the revoked result finished before control registration"),
                registered = port.next_terminal_control() => assert_eq!(registered, root),
            }
        })
        .await
        .expect("the exact revoked root must register terminal control");
    }

    async fn poll_pending_after_actor_turn<F: Future + ?Sized>(
        mut future: Pin<&mut F>,
        actor: &LogicalExecutionActor,
    ) {
        assert!(matches!(
            poll_once(future.as_mut()).await,
            std::task::Poll::Pending
        ));
        // This exact actor mailbox barrier acknowledges the previously sent
        // root observation; advancing the test does not depend on scheduling.
        actor.snapshot().await.unwrap();
        assert!(matches!(
            poll_once(future.as_mut()).await,
            std::task::Poll::Pending
        ));
    }

    fn execution(tag: i64) -> QueryExecutionId {
        QueryExecutionId::new(QueryId::new(77, tag), AttemptId::new(1).unwrap()).unwrap()
    }

    fn root_task(execution: QueryExecutionId) -> TaskIdentity {
        TaskIdentity::new(
            execution,
            StageId::new(1).unwrap(),
            TaskId::new(1).unwrap(),
            BackendProcessId::new_v7(),
        )
    }

    #[test]
    fn unsupported_root_or_originating_failure_has_a_stable_nonretryable_identity() {
        use crate::execution_error::{ExecutionFailureSite, QueryFailureReason};
        use novarocks_execution_contract::{
            TaskFailurePhase,
            failure::{FailureClass, FailureFact, FailureProjection, FailureReason, domain},
        };
        use std::num::NonZeroU16;
        let root = root_task(execution(118));
        let origin = TaskIdentity::new(
            root.query_execution_id(),
            StageId::new(2).unwrap(),
            TaskId::new(2).unwrap(),
            BackendProcessId::new_v7(),
        );
        let fact = FailureFact::new(
            FailureClass::Unsupported,
            FailureReason::new(domain::LOCAL_COMPILER, NonZeroU16::new(1).unwrap()),
        );
        let failure = TerminationDetail::Failed(TaskFailure::new_in_phase(
            fact,
            SafeDetail::truncating("FULL OUTER hash join"),
            TaskFailurePhase::Preparation,
        ));
        let failed = TaskStatus::try_new_with_installed(
            root,
            TaskStatusVersion::new(3).unwrap(),
            TaskState::Failed,
            Some(failure.clone()),
            TaskOutputFacts::default(),
            false,
        )
        .unwrap();
        let derived = TaskStatus::try_new(
            root,
            TaskStatusVersion::new(3).unwrap(),
            TaskState::Aborted,
            Some(TerminationDetail::Aborted(AbortCause::PeerTaskFailed)),
            TaskOutputFacts::default(),
        )
        .unwrap();
        let authoritative =
            novarocks_coordination_contract::SelectedTaskTermination::new(origin, failure);
        for (decision, identity) in [
            (classify_root_termination(&failed, None).unwrap(), root),
            (
                classify_root_termination(&derived, Some(&authoritative)).unwrap(),
                origin,
            ),
        ] {
            assert_eq!(decision.class(), AttemptFailureClass::Unsupported);
            assert_eq!(decision.input().fact(), fact);
            assert_eq!(
                decision.input().site(),
                ExecutionFailureSite::Task {
                    identity,
                    phase: TaskFailurePhase::Preparation,
                },
            );
            let user = decision.input().user_error();
            assert_eq!(user.code().as_str(), "UnsupportedCompiledShape");
            assert_eq!(user.phase(), novarocks_user_error::ErrorPhase::Execution);
            assert_eq!(user.retry(), novarocks_user_error::RetryClass::Never);
            assert_eq!(user.location(), None);
            assert_eq!(
                crate::session_error::execution_public_message(decision.error()),
                "[UnsupportedCompiledShape] FULL OUTER hash join",
            );
        }
        let invalid = TaskStatus::try_new(
            root,
            TaskStatusVersion::new(3).unwrap(),
            TaskState::Failed,
            Some(TerminationDetail::Failed(TaskFailure::new(
                QueryFailureReason::ResultProtocol.failure_fact(),
                SafeDetail::truncating("unsupported diagnostic is not a capability"),
            ))),
            TaskOutputFacts::default(),
        )
        .unwrap();
        let decision = classify_root_termination(&invalid, None).unwrap();
        assert_eq!(decision.class(), AttemptFailureClass::ContractViolation);
        assert_ne!(
            decision.input().user_error().code().as_str(),
            "UnsupportedCompiledShape"
        );
        assert!(
            !crate::session_error::execution_public_message(decision.error())
                .starts_with("[UnsupportedCompiledShape]")
        );
    }

    fn result_schema() -> ResultSchema {
        ResultSchema::new(vec![ResultField::new(
            "value",
            novarocks_type_contract::FunctionValueType::new(DataType::Int64, false),
        )])
    }

    fn finished(root: TaskIdentity) -> TaskStatus {
        TaskStatus::try_new(
            root,
            TaskStatusVersion::new(2).unwrap(),
            TaskState::Finished,
            None,
            TaskOutputFacts::new(true),
        )
        .unwrap()
    }

    fn running(root: TaskIdentity) -> TaskStatus {
        TaskStatus::try_new(
            root,
            TaskStatusVersion::new(1).unwrap(),
            TaskState::Running,
            None,
            TaskOutputFacts::new(false),
        )
        .unwrap()
    }

    struct Harness {
        control: WorkloadControl,
        scope: WorkScope,
        actor: LogicalExecutionActor,
        owner: super::super::LogicalExecutionActorOwner,
        permit: RunningAttemptPermit,
        stream: crate::api::QueryResultStream,
        root: TaskIdentity,
    }

    #[tokio::test]
    async fn relay_refuses_foreign_scope_or_wrong_window_class_before_reading() {
        for foreign_scope in [false, true] {
            let (
                Harness {
                    control: _control,
                    scope,
                    actor,
                    owner,
                    permit,
                    mut stream,
                    root,
                },
                window,
            ) = relay_harness(56).await;
            let (foreign, foreign_window) = relay_harness(57).await;
            let wrong_class = scope
                .result_capacity()
                .unwrap()
                .try_acquire(&scope, ResultWindowClass::Local)
                .unwrap();
            let (sender, statuses) = accepted_root_status_projection(root);
            sender.publish(running(root)).unwrap();
            let port = ScriptedRootPort::new(Vec::new());
            let (_terminal_sender, terminal) = native_attempt_terminal_channel();
            let mut binding = relay_binding(root, port.clone(), &window);
            binding.window = if foreign_scope {
                foreign_window.retain_alias()
            } else {
                wrong_class.retain_alias()
            };
            let ResultPumpFailure::DecisionPending(decision) =
                run_root_relay(permit, root, scope, binding, statuses, terminal)
                    .await
                    .unwrap_err()
            else {
                panic!("invalid window must be refused before root binding");
            };
            assert_eq!(decision.class(), AttemptFailureClass::ContractViolation);
            assert_eq!(
                decision.fail_logical(&actor).await.unwrap(),
                LogicalConclusion::Failed
            );
            assert!(stream.next().await.is_err());
            assert!(port.requests().is_empty());
            drop((
                sender,
                stream,
                actor,
                owner,
                window,
                wrong_class,
                foreign,
                foreign_window,
            ));
        }
    }

    #[tokio::test]
    async fn relay_revoked_output_registers_exact_control_and_never_polls_or_acks_again() {
        let (
            Harness {
                control: _control,
                scope,
                actor,
                owner,
                permit,
                mut stream,
                root,
            },
            window,
        ) = relay_harness(54).await;
        let control = Arc::new(TestSuccessSealPort::default());
        let (sender, statuses) = accepted_root_status_projection_with_control_port(
            root,
            control.clone() as Arc<dyn AcceptedRootControlPort>,
        );
        sender.publish(running(root)).unwrap();
        let port = ScriptedRootPort::new(vec![RootReadOutcome::AwaitTerminalControl]);
        let (_terminal_sender, terminal) = native_attempt_terminal_channel();
        let mut relay = Box::pin(run_root_relay(
            permit,
            root,
            scope,
            relay_binding(root, port.clone(), &window),
            statuses,
            terminal,
        ));
        wait_for_terminal_control_registration(relay.as_mut(), &control, root).await;
        let status = TaskStatus::try_new(
            root,
            TaskStatusVersion::new(3).unwrap(),
            TaskState::Aborted,
            Some(TerminationDetail::Aborted(AbortCause::PeerTaskFailed)),
            TaskOutputFacts::new(false),
        )
        .unwrap();
        sender
            .publish_with_attempt_failure(
                status,
                novarocks_coordination_contract::SelectedTaskTermination::new(
                    root,
                    TerminationDetail::Failed(TaskFailure::new(
                        novarocks_execution_contract::failure::FailureFact::new(
                            novarocks_execution_contract::failure::FailureClass::Dependency,
                            novarocks_execution_contract::failure::FailureReason::new(
                                std::num::NonZeroU16::new(u16::MAX).unwrap(),
                                std::num::NonZeroU16::new(2).unwrap(),
                            ),
                        ),
                        SafeDetail::new("revoked root original cause").unwrap(),
                    )),
                ),
            )
            .unwrap();
        let error = tokio::time::timeout(Duration::from_secs(1), relay)
            .await
            .unwrap()
            .unwrap_err();
        let ResultPumpFailure::DecisionPending(decision) = error else {
            panic!("revoked output must retain its original attempt authority");
        };
        assert_eq!(decision.class(), AttemptFailureClass::ExecutionFailure);
        assert!(
            decision
                .error()
                .message()
                .contains("revoked root original cause")
        );
        assert_eq!(
            decision.fail_logical(&actor).await.unwrap(),
            LogicalConclusion::Failed
        );
        assert!(stream.next().await.is_err());
        assert_eq!(port.requests(), vec![(Some(1), 0)]);
        drop((sender, stream, actor, owner, window));
    }

    #[tokio::test]
    async fn relay_finished_and_native_completed_cannot_replace_an_explicit_success_seal() {
        let (
            Harness {
                control: _control,
                scope,
                actor,
                owner,
                permit,
                mut stream,
                root,
            },
            window,
        ) = relay_harness(55).await;
        let control = Arc::new(TestSuccessSealPort::default());
        let (sender, statuses) = accepted_root_status_projection_with_control_port(
            root,
            control.clone() as Arc<dyn AcceptedRootControlPort>,
        );
        sender.publish(finished(root)).unwrap();
        let port = ScriptedRootPort::new(vec![rows_data(1, &[1, 0, 0, 0, b'a'], Some((2, 1)))]);
        let (terminal_sender, terminal) = native_attempt_terminal_channel();
        terminal_sender.publish(NativeAttemptTerminal::Completed);
        let relay = tokio::spawn(run_root_relay(
            permit,
            root,
            scope,
            relay_binding(root, port, &window),
            statuses,
            terminal,
        ));
        let delivery = next_segment(&mut stream).await;
        assert!(
            !relay.is_finished(),
            "Finished and Native Completed do not consume local output"
        );
        assert_eq!(actor.snapshot().await.unwrap().conclusion, None);
        delivery.complete();
        let seal_request = tokio::time::timeout(Duration::from_secs(1), control.next())
            .await
            .unwrap();
        assert!(
            !relay.is_finished(),
            "local End still needs the original success seal"
        );
        assert_eq!(actor.snapshot().await.unwrap().conclusion, None);
        drop(sender);
        drop(seal_request);
        let error = tokio::time::timeout(Duration::from_secs(1), relay)
            .await
            .unwrap()
            .unwrap()
            .unwrap_err();
        match error {
            ResultPumpFailure::DecisionPending(decision) => {
                assert_eq!(decision.class(), AttemptFailureClass::ContractViolation);
                assert_eq!(
                    decision.fail_logical(&actor).await.unwrap(),
                    LogicalConclusion::Failed
                );
            }
            ResultPumpFailure::Concluded(concluded) => {
                assert_eq!(concluded.conclusion(), LogicalConclusion::Failed);
                assert_eq!(concluded.class(), AttemptFailureClass::ContractViolation);
            }
            _ => panic!("missing success seal must settle a typed logical failure"),
        }
        assert!(stream.next().await.is_err());
        drop((stream, actor, owner, window));
    }

    #[test]
    fn a_cancelled_root_and_a_latched_failure_both_refuse_a_read_success_seal() {
        let root = root_task(execution(100_101));
        let failure = TaskFailure::new(
            novarocks_execution_contract::failure::FailureFact::new(
                novarocks_execution_contract::failure::FailureClass::Data,
                novarocks_execution_contract::failure::FailureReason::new(
                    std::num::NonZeroU16::new(u16::MAX).unwrap(),
                    std::num::NonZeroU16::new(1).unwrap(),
                ),
            ),
            SafeDetail::new("original read failure").unwrap(),
        );
        for state in [
            TaskState::Running,
            TaskState::Flushing,
            TaskState::Canceling,
            TaskState::Canceled,
            TaskState::Aborted,
            TaskState::Failed,
        ] {
            let termination = match state {
                TaskState::Canceling | TaskState::Canceled => Some(TerminationDetail::Canceled(
                    novarocks_execution_contract::CancelReason::UpstreamNoLongerNeeded,
                )),
                TaskState::Aborted => Some(TerminationDetail::Aborted(AbortCause::QueryFailed)),
                TaskState::Failed => Some(TerminationDetail::Failed(failure.clone())),
                _ => None,
            };
            let status = TaskStatus::try_new(
                root,
                TaskStatusVersion::new(1).unwrap(),
                state,
                termination,
                TaskOutputFacts::new(false),
            )
            .unwrap();
            let (sender, mut source) = accepted_root_status_projection(root);
            sender
                .publish_attempt_observation(status, AcceptedAttemptFailure::None)
                .unwrap();
            let error = sender.seal_success(Vec::new()).unwrap_err();
            assert_eq!(
                error.kind(),
                QueryExecutionErrorKind::InvalidRequest,
                "{state}"
            );
            assert_eq!(
                error.message(),
                "success seal requires a Finished root and no attempt failure"
            );
            assert!(
                matches!(
                    source.try_next(),
                    Some(AcceptedRootProjection::Observation(_))
                ),
                "a refused terminal never publishes a success seal"
            );
            assert!(
                source.try_next().is_none(),
                "no later success seal may follow the refusal"
            );
        }
        let writer = TaskIdentity::new(
            root.query_execution_id(),
            StageId::new(2).unwrap(),
            TaskId::new(2).unwrap(),
            root.backend_process_id(),
        );
        for attempt_failure in [
            AcceptedAttemptFailure::DerivedPending,
            AcceptedAttemptFailure::Authoritative(
                novarocks_coordination_contract::SelectedTaskTermination::new(
                    writer,
                    TerminationDetail::Failed(failure.clone()),
                ),
            ),
        ] {
            let (sender, mut source) = accepted_root_status_projection(root);
            sender
                .publish_attempt_observation(finished(root), attempt_failure)
                .unwrap();
            let error = sender.seal_success(Vec::new()).unwrap_err();
            assert_eq!(error.kind(), QueryExecutionErrorKind::InvalidRequest);
            assert_eq!(
                error.message(),
                "success seal requires a Finished root and no attempt failure"
            );
            assert!(matches!(
                source.try_next(),
                Some(AcceptedRootProjection::Observation(_))
            ));
            assert!(
                source.try_next().is_none(),
                "no later success seal may follow the latched failure"
            );
        }
    }

    #[test]
    fn a_foreign_root_projection_cannot_supply_this_reads_success_seal() {
        let root = root_task(execution(100_102));
        let foreign = TaskIdentity::new(
            root.query_execution_id(),
            root.stage_id(),
            root.task_id(),
            BackendProcessId::new_v7(),
        );
        let (sender, mut source) = accepted_root_status_projection(root);
        let error = sender
            .publish_attempt_observation(finished(foreign), AcceptedAttemptFailure::None)
            .unwrap_err();
        assert_eq!(error.kind(), QueryExecutionErrorKind::InvalidRequest);
        assert_eq!(error.message(), "accepted root status names another Task");
        assert!(
            source.try_next().is_none(),
            "the foreign process must not advance the accepted Root"
        );
        assert!(sender.seal_success(Vec::new()).is_err());
    }

    #[tokio::test]
    async fn relay_authoritative_failure_interrupts_a_read_or_held_delivery_without_ack() {
        for hold_delivery in [false, true] {
            let (
                Harness {
                    control: _control,
                    scope,
                    actor,
                    owner,
                    permit,
                    mut stream,
                    root,
                },
                window,
            ) = relay_harness(50 + i64::from(hold_delivery)).await;
            let (sender, statuses) = accepted_root_status_projection(root);
            sender.publish(running(root)).unwrap();
            let port = ScriptedRootPort::new(if hold_delivery {
                vec![rows_data(1, &[1, 0, 0, 0, b'a'], None)]
            } else {
                Vec::new()
            });
            let (_terminal_sender, terminal) = native_attempt_terminal_channel();
            let mut relay = tokio::spawn(run_root_relay(
                permit,
                root,
                scope,
                relay_binding(root, port.clone(), &window),
                statuses,
                terminal,
            ));
            let held = if hold_delivery {
                Some(next_segment(&mut stream).await)
            } else {
                tokio::time::timeout(Duration::from_secs(1), async {
                    while port.requests().is_empty() {
                        tokio::task::yield_now().await;
                    }
                })
                .await
                .unwrap();
                None
            };
            let status = TaskStatus::try_new(
                root,
                TaskStatusVersion::new(3).unwrap(),
                TaskState::Failed,
                Some(TerminationDetail::Failed(TaskFailure::new(
                    novarocks_execution_contract::failure::FailureFact::new(
                        novarocks_execution_contract::failure::FailureClass::Data,
                        novarocks_execution_contract::failure::FailureReason::new(
                            std::num::NonZeroU16::new(u16::MAX).unwrap(),
                            std::num::NonZeroU16::new(1).unwrap(),
                        ),
                    ),
                    SafeDetail::new("originating root failure").unwrap(),
                ))),
                TaskOutputFacts::new(false),
            )
            .unwrap();
            sender
                .publish_with_attempt_failure(
                    status.clone(),
                    novarocks_coordination_contract::SelectedTaskTermination::new(
                        root,
                        status.termination().unwrap().clone(),
                    ),
                )
                .unwrap();
            let error = match tokio::time::timeout(Duration::from_secs(1), &mut relay).await {
                Ok(result) => result.unwrap().unwrap_err(),
                Err(_) => {
                    let snapshot = actor.snapshot().await.unwrap();
                    _control.cancel_active_roots(CancellationReason::Requested);
                    let _ = tokio::time::timeout(Duration::from_secs(1), relay).await;
                    panic!(
                        "root failure stuck: held={hold_delivery}, phase={:?}, conclusion={:?}, visible={}, reads={}",
                        snapshot.phase,
                        snapshot.conclusion,
                        snapshot.output_visible,
                        port.requests().len()
                    );
                }
            };
            match error {
                ResultPumpFailure::DecisionPending(decision) if !hold_delivery => {
                    assert_eq!(decision.class(), AttemptFailureClass::ExecutionFailure);
                    assert!(
                        decision
                            .error()
                            .message()
                            .contains("originating root failure")
                    );
                    assert_eq!(
                        decision.fail_logical(&actor).await.unwrap(),
                        LogicalConclusion::Failed
                    );
                }
                ResultPumpFailure::Concluded(concluded) if hold_delivery => {
                    assert_eq!(concluded.conclusion(), LogicalConclusion::Failed);
                    assert_eq!(concluded.class(), AttemptFailureClass::ExecutionFailure);
                    assert!(
                        concluded
                            .error()
                            .message()
                            .contains("originating root failure")
                    );
                }
                _ => panic!("visibility must decide whether failure returns attempt authority"),
            }
            if let Some(held) = held {
                held.complete();
            }
            assert!(stream.next().await.is_err());
            assert!(port.requests().iter().all(|(_, consumed)| *consumed == 0));
            drop((sender, stream, actor, owner, window));
        }
    }

    #[tokio::test]
    async fn relay_derived_failure_requires_originating_refinement_before_settling() {
        let (
            Harness {
                control: _control,
                scope,
                actor,
                owner,
                permit,
                mut stream,
                root,
            },
            window,
        ) = relay_harness(52).await;
        let control = Arc::new(TestSuccessSealPort::default());
        let (sender, statuses) = accepted_root_status_projection_with_control_port(
            root,
            control.clone() as Arc<dyn AcceptedRootControlPort>,
        );
        sender.publish(running(root)).unwrap();
        let port = ScriptedRootPort::new(Vec::new());
        let (_terminal_sender, terminal) = native_attempt_terminal_channel();
        let mut relay = Box::pin(run_root_relay(
            permit,
            root,
            scope,
            relay_binding(root, port.clone(), &window),
            statuses,
            terminal,
        ));
        tokio::time::timeout(Duration::from_secs(1), async {
            while port.requests().is_empty() {
                poll_pending_after_actor_turn(relay.as_mut(), &actor).await;
            }
        })
        .await
        .unwrap();
        let pending = TaskStatus::try_new(
            root,
            TaskStatusVersion::new(3).unwrap(),
            TaskState::Aborted,
            Some(TerminationDetail::Aborted(AbortCause::PeerTaskFailed)),
            TaskOutputFacts::new(false),
        )
        .unwrap();
        sender
            .publish_attempt_observation(pending.clone(), AcceptedAttemptFailure::DerivedPending)
            .unwrap();
        poll_pending_after_actor_turn(relay.as_mut(), &actor).await;
        sender
            .publish_with_attempt_failure(
                pending,
                novarocks_coordination_contract::SelectedTaskTermination::new(
                    root,
                    TerminationDetail::Failed(TaskFailure::new(
                        novarocks_execution_contract::failure::FailureFact::new(
                            novarocks_execution_contract::failure::FailureClass::Dependency,
                            novarocks_execution_contract::failure::FailureReason::new(
                                std::num::NonZeroU16::new(u16::MAX).unwrap(),
                                std::num::NonZeroU16::new(2).unwrap(),
                            ),
                        ),
                        SafeDetail::new("originating peer exchange failure").unwrap(),
                    )),
                ),
            )
            .unwrap();
        let error = tokio::time::timeout(Duration::from_secs(1), relay)
            .await
            .unwrap()
            .unwrap_err();
        let ResultPumpFailure::DecisionPending(decision) = error else {
            panic!("refinement must retain its attempt decision");
        };
        assert_eq!(decision.class(), AttemptFailureClass::ExecutionFailure);
        assert!(
            decision
                .error()
                .message()
                .contains("originating peer exchange failure")
        );
        assert_eq!(
            decision.fail_logical(&actor).await.unwrap(),
            LogicalConclusion::Failed
        );
        assert!(stream.next().await.is_err());
        assert!(port.requests().iter().all(|(_, consumed)| *consumed == 0));
        drop((sender, stream, actor, owner, window));
    }

    #[tokio::test]
    async fn relay_native_failure_without_root_status_retains_recovery_authority() {
        let (
            Harness {
                control: _control,
                scope,
                actor,
                owner,
                permit,
                mut stream,
                root,
            },
            window,
        ) = relay_harness(53).await;
        let (_sender, statuses) = accepted_root_status_projection(root);
        let port = ScriptedRootPort::new(Vec::new());
        let (terminal_sender, terminal) = native_attempt_terminal_channel();
        terminal_sender.publish(NativeAttemptTerminal::Failed(
            crate::api::NativeAttemptPreparationFailure::from_input(
                crate::execution_error::ExecutionFailureInput::new(
                    novarocks_execution_contract::failure::FailureProjection::failure_fact(
                        &crate::execution_error::QueryFailureReason::BackendLost,
                    ),
                    SafeDetail::truncating("exact backend process replaced"),
                    crate::execution_error::ExecutionFailureSite::Backend {
                        process: root.backend_process_id(),
                    },
                ),
            ),
        ));
        let error = tokio::time::timeout(
            Duration::from_secs(1),
            run_root_relay(
                permit,
                root,
                scope,
                relay_binding(root, port.clone(), &window),
                statuses,
                terminal,
            ),
        )
        .await
        .unwrap()
        .unwrap_err();
        let ResultPumpFailure::DecisionPending(decision) = error else {
            panic!("native failure must retain its attempt decision");
        };
        assert_eq!(
            decision.class(),
            AttemptFailureClass::RecoverableInfrastructure
        );
        assert_eq!(decision.error().message(), "exact backend process replaced");
        assert_eq!(
            decision.fail_logical(&actor).await.unwrap(),
            LogicalConclusion::Failed
        );
        assert!(stream.next().await.is_err());
        assert!(port.requests().is_empty());
        drop((stream, actor, owner, window));
    }

    async fn run_root_relay(
        permit: RunningAttemptPermit,
        root: TaskIdentity,
        scope: WorkScope,
        binding: RootRelayBinding,
        statuses: AcceptedRootStatusSource,
        terminal: NativeAttemptTerminalSource,
    ) -> Result<LogicalConclusion, ResultPumpFailure> {
        let (tail_owner, tail) =
            super::super::native_active_custody::root_ack_test_fixture(permit.identity(), root);
        let result =
            super::run_root_relay(permit, root, scope, binding, statuses, terminal, tail).await;
        tail_owner.close_native();
        tail_owner.drain().await;
        result
    }

    // ---- Backend-encoded root relay ----

    use novarocks_execution_contract::root_result::{
        RootReadOutcome, RootResultData, RootResultEnd, RootResultReply,
    };
    use novarocks_result_contract::{
        ClientRowProfile, RootOutputKind, RootProfileId, RootProfileV1,
    };
    use novarocks_workload_control::{ResultCapacityConfig, ResultWindowClass, ResultWindowGrant};

    /// Answers each read from a script and records it. An ACK-only read with
    /// no scripted answer is acknowledged.
    struct ScriptedRootPort {
        requests: Mutex<Vec<RootResultRead>>,
        outcomes: Mutex<VecDeque<RootReadOutcome>>,
        final_ack_gate: Mutex<Option<tokio::sync::oneshot::Receiver<()>>>,
    }
    impl ScriptedRootPort {
        fn new(outcomes: Vec<RootReadOutcome>) -> Arc<Self> {
            Arc::new(Self {
                requests: Mutex::new(Vec::new()),
                outcomes: Mutex::new(outcomes.into()),
                final_ack_gate: Mutex::new(None),
            })
        }
        fn requests(&self) -> Vec<(Option<u64>, u64)> {
            self.requests
                .lock()
                .unwrap()
                .iter()
                .map(|read| (read.wanted().map(|wanted| wanted.get()), read.consumed()))
                .collect()
        }
    }
    impl crate::api::BoundedRootReadPort for ScriptedRootPort {
        fn read(
            &self,
            request: RootResultRead,
            physical_guard: novarocks_workload_control::ResultWindowAlias,
        ) -> Pin<Box<dyn Future<Output = Result<RetainedRootReply, RootResultFetchFailure>> + Send>>
        {
            self.requests.lock().unwrap().push(request.clone());
            let final_ack_gate = if request.wanted().is_none() {
                self.final_ack_gate.lock().unwrap().take()
            } else {
                None
            };
            let outcome = self.outcomes.lock().unwrap().pop_front().unwrap_or(
                if request.wanted().is_none() {
                    RootReadOutcome::AckOnly
                } else {
                    RootReadOutcome::NotReady
                },
            );
            let reply = RootResultReply {
                root_task: request.root_task(),
                profile: request.profile(),
                kind: request.kind(),
                accepted_consumed: request.consumed(),
                outcome,
            };
            let long_poll = matches!(reply.outcome, RootReadOutcome::NotReady);
            Box::pin(async move {
                if let Some(gate) = final_ack_gate {
                    gate.await.unwrap();
                }
                if long_poll {
                    tokio::time::sleep(Duration::from_millis(2)).await;
                }
                RetainedRootReply::try_new(
                    novarocks_execution_contract::root_result::RootReadResponse::Reply(reply),
                    physical_guard,
                    64 * 1024,
                )
                .map_err(|error| {
                    contract_failure(contract_error(format!("retain root reply: {error}")))
                })
            })
        }
        fn seal(
            &self,
            _sealed: novarocks_execution_contract::root_lifetime::RootReadSealed,
        ) -> Result<(), QueryExecutionError> {
            Ok(())
        }
    }

    fn rows_data(sequence: u64, body: &'static [u8], end: Option<(u64, u64)>) -> RootReadOutcome {
        RootReadOutcome::Data(
            RootResultData::try_new(
                RootOutputKind::ClientRows,
                NonZeroU64::new(sequence).unwrap(),
                bytes::Bytes::from_static(body),
                end.map(|(sequence, rows)| RootResultEnd {
                    sequence: NonZeroU64::new(sequence).unwrap(),
                    output_rows: rows,
                }),
            )
            .unwrap(),
        )
    }

    /// The ordinary harness plus a configured result window for the root.
    async fn relay_harness(tag: i64) -> (Harness, ResultWindowGrant) {
        relay_harness_with(tag, relay_carrier()).await
    }

    async fn relay_harness_with(
        tag: i64,
        carrier: crate::api::ResultRowCarrier,
    ) -> (Harness, ResultWindowGrant) {
        let control = WorkloadControl::try_new(
            WorkloadConfig::default(),
            ResourceConfig {
                total_bytes: 1 << 20,
                control_bytes: 1 << 12,
                per_scope_bytes: 1 << 18,
            },
        )
        .unwrap();
        let capacity = control
            .configure_result_capacity(ResultCapacityConfig::V1)
            .unwrap();
        control.mark_ready().unwrap();
        let work = control
            .try_begin_root(WorkRequest::new(WorkClass::Query))
            .unwrap();
        let scope = work.owner.scope();
        let class = match carrier {
            crate::api::ResultRowCarrier::Relayed {
                kind: RootOutputKind::InternalFacts(_) | RootOutputKind::CountOnly,
                ..
            } => ResultWindowClass::Internal,
            _ => ResultWindowClass::Client,
        };
        let window = capacity.try_acquire(&scope, class).unwrap();
        let stage = scope.try_acquire(Stage::Execution).unwrap();
        let execution = execution(tag);
        let config = LogicalExecutionActorConfig::single_attempt_completion(
            execution,
            ExecutionEffect::None,
            NonZeroUsize::new(8).unwrap(),
            Vec::new(),
            NonZeroUsize::new(1).unwrap(),
            NonZeroUsize::new(1).unwrap(),
            work.owner,
            stage,
        )
        .unwrap()
        .with_result_stream(result_schema(), NonZeroUsize::new(1).unwrap())
        .with_result_row_carrier(carrier);
        let (owner, initial, output) = spawn_logical_execution_actor(&Handle::current(), config)
            .unwrap()
            .into_parts();
        let ExecutionOutput::Rows(mut stream) = output.into_output() else {
            panic!("result actor must expose a stream");
        };
        let schema = stream.begin_schema().unwrap();
        assert_eq!(schema.row_carrier(), carrier);
        schema.complete();
        let actor = owner.actor().clone();
        let permit = actor.activate(initial.ready()).await.unwrap();
        (
            Harness {
                control,
                scope,
                actor,
                owner,
                permit,
                stream,
                root: root_task(execution),
            },
            window,
        )
    }

    fn relay_profile() -> ClientRowProfile {
        ClientRowProfile::try_new(RootProfileV1::SEGMENT_BYTES, 1 << 20).unwrap()
    }

    fn relay_carrier() -> crate::api::ResultRowCarrier {
        crate::api::ResultRowCarrier::relayed(RootOutputKind::ClientRows, Some(relay_profile()))
            .unwrap()
    }

    fn relay_binding(
        root: TaskIdentity,
        port: Arc<ScriptedRootPort>,
        window: &ResultWindowGrant,
    ) -> RootRelayBinding {
        RootRelayBinding {
            port,
            frontier: super::super::RootRelayFrontier::new(
                root,
                RootProfileId::V1,
                RootOutputKind::ClientRows,
                Some(relay_profile()),
            )
            .unwrap(),
            window: window.retain_alias(),
            max_wait: Duration::from_millis(5),
        }
    }

    async fn next_segment(
        stream: &mut crate::api::QueryResultStream,
    ) -> crate::api::RootSegmentDelivery {
        match stream.next().await.unwrap().unwrap() {
            ResultDelivery::Segment(segment) => segment,
            _ => panic!("expected a relayed root segment"),
        }
    }

    #[tokio::test]
    async fn relay_cancel_cut_retains_validated_coverage_across_actor_handoff_without_ack() {
        for complete_first in [false, true] {
            let (
                Harness {
                    control,
                    scope,
                    actor,
                    owner,
                    permit,
                    mut stream,
                    root,
                },
                window,
            ) = relay_harness(40 + i64::from(complete_first)).await;
            let (status_sender, statuses) = accepted_root_status_projection_with_control_port(
                root,
                Arc::new(TestSuccessSealPort::default()),
            );
            status_sender.publish(finished(root)).unwrap();
            let port = ScriptedRootPort::new(vec![
                rows_data(1, &[5, 0, 0, 0, b'a', b'b'], None),
                rows_data(2, &[b'c', b'd', b'e', 1, 0, 0, 0, b'f'], Some((3, 2))),
            ]);
            let (_terminal_sender, terminal) = native_attempt_terminal_channel();
            let relay = tokio::spawn(run_root_relay(
                permit,
                root,
                scope.clone(),
                relay_binding(root, port.clone(), &window),
                statuses,
                terminal,
            ));
            let mut first = Some(next_segment(&mut stream).await);
            let coverage = first.as_ref().unwrap().resident_window().unwrap();
            // Barrier: the second response must have passed frontier validation,
            // not merely have been dispatched by the read port.
            while coverage.is_empty() {
                tokio::task::yield_now().await;
            }
            if complete_first {
                first.take().unwrap().complete();
                // Force the second item out of ready and through the actor's
                // queue before the protocol has acquired its next delivery.
                while !coverage.is_empty() {
                    tokio::task::yield_now().await;
                }
            }
            assert_eq!(
                control.cancel_active_roots(CancellationReason::Requested),
                1
            );
            let items = coverage.freeze();
            let payloads = items
                .iter()
                .flatten()
                .flat_map(|item| {
                    item.client_rows()
                        .unwrap()
                        .payload_spans()
                        .map(|span| span.bytes.to_vec())
                        .collect::<Vec<_>>()
                })
                .collect::<Vec<_>>();
            assert!(payloads.iter().any(|part| part == b"cde"));
            assert!(coverage.freeze().into_iter().all(|item| item.is_none()));
            if let Some(first) = first {
                first.fail(contract_error("cancelled during a partial write"));
            }
            assert!(relay.await.unwrap().is_err());
            assert_eq!(port.requests(), vec![(Some(1), 0), (Some(2), 0)]);
            drop(items);
            drop(stream);
            drop(actor);
            drop(owner);
        }
    }

    #[tokio::test]
    async fn relay_prefetches_delivers_in_order_and_seals_without_a_final_backend_ack() {
        let (
            Harness {
                control: _control,
                scope,
                actor,
                owner,
                permit,
                mut stream,
                root,
            },
            window,
        ) = relay_harness(31).await;
        let seal_port = Arc::new(TestSuccessSealPort::default());
        let (status_sender, statuses) = accepted_root_status_projection_with_control_port(
            root,
            Arc::clone(&seal_port) as Arc<dyn AcceptedRootControlPort>,
        );
        status_sender.publish(running(root)).unwrap();
        // A two-segment row, then a one-row segment carrying End.
        let port = ScriptedRootPort::new(vec![
            rows_data(1, &[5, 0, 0, 0, b'a', b'b'], None),
            rows_data(2, &[b'c', b'd', b'e', 1, 0, 0, 0, b'f'], Some((3, 2))),
        ]);
        let (release_ack, ack_gate) = tokio::sync::oneshot::channel();
        *port.final_ack_gate.lock().unwrap() = Some(ack_gate);
        let capacity = scope.result_capacity().unwrap();
        let (tail_owner, tail) =
            super::super::native_active_custody::root_ack_test_fixture(permit.identity(), root);
        let (_terminal_sender, terminal_source) = native_attempt_terminal_channel();
        let relay = tokio::spawn(super::run_root_relay(
            permit,
            root,
            scope,
            relay_binding(root, Arc::clone(&port), &window),
            statuses,
            terminal_source,
            tail,
        ));
        drop(window);
        let first = next_segment(&mut stream).await;
        assert_eq!(first.sequence(), ResultPacketSequence::new(0));
        assert_eq!(first.rows(), 0);
        // Seq 2 is read while seq 1 is still being written; both carry ACK 0.
        while port.requests().len() < 2 {
            tokio::task::yield_now().await;
        }
        assert_eq!(port.requests(), vec![(Some(1), 0), (Some(2), 0)]);
        first.complete();
        let second = next_segment(&mut stream).await;
        assert_eq!(second.rows(), 2);
        let spans = second
            .client_rows()
            .unwrap()
            .payload_spans()
            .map(|span| (span.bytes.to_vec(), span.completes_row))
            .collect::<Vec<_>>();
        assert_eq!(spans, vec![(b"cde".to_vec(), true), (b"f".to_vec(), true)]);
        second.complete();
        // Real local End alone cannot complete a still-running Root.
        let seal_request = seal_port.next().await;
        assert!(!relay.is_finished());
        assert_eq!(actor.snapshot().await.unwrap().conclusion, None);
        status_sender.publish(finished(root)).unwrap();
        // Success needs the original seal, not the pending Backend final ACK.
        seal_request.accept(status_sender, Vec::new()).unwrap();
        let ResultDelivery::End(end) = stream.next().await.unwrap().unwrap() else {
            panic!("local End consumption and root Finished produce EOF");
        };
        assert_eq!(end.root_output_rows(), Some(2));
        end.complete();
        assert_eq!(
            tokio::time::timeout(Duration::from_secs(1), relay)
                .await
                .unwrap()
                .unwrap()
                .unwrap(),
            LogicalConclusion::Succeeded
        );
        assert_eq!(&port.requests()[..2], &[(Some(1), 0), (Some(2), 0)]);
        tail_owner.close_native();
        assert!(
            tail_owner.retained(),
            "client EOF is independent of the pending original ACK"
        );
        assert_eq!(capacity.snapshot().held_positions, [1, 0, 0, 0]);
        release_ack.send(()).unwrap();
        tokio::time::timeout(Duration::from_secs(2), tail_owner.drain())
            .await
            .unwrap();
        assert!(!tail_owner.retained());
        assert_eq!(capacity.snapshot().held_positions, [0; 4]);
        drop((stream, actor, owner));
    }

    #[tokio::test]
    async fn scalar_relay_delivers_value_and_no_rows_counts_before_success_seal() {
        use novarocks_result_contract::{
            BorrowedScalarLeaf, InternalResultDomain, ScalarField, ScalarLeafCursor, ScalarRecord,
            ScalarSchema, ScalarValueType,
        };
        let schema = ScalarSchema::try_new(ScalarField {
            nullable: true,
            value_type: ScalarValueType::SignedInteger(64),
        })
        .unwrap();
        let kind = RootOutputKind::InternalFacts(InternalResultDomain::ScalarValueV1);
        for (tag, value) in [
            (71, BorrowedScalarLeaf::NoRows),
            (72, BorrowedScalarLeaf::Null),
            (73, BorrowedScalarLeaf::SignedInteger { bits: 64, value: 7 }),
        ] {
            let (
                Harness {
                    control: _control,
                    scope,
                    actor,
                    owner,
                    permit,
                    mut stream,
                    root,
                },
                window,
            ) = relay_harness_with(
                tag,
                crate::api::ResultRowCarrier::relayed(kind, None).unwrap(),
            )
            .await;
            let cursor = ScalarLeafCursor::try_new(&schema, value).unwrap();
            let rows = cursor.rows();
            let mut body = vec![0; cursor.encoded_len()];
            cursor.copy_range(0, &mut body).unwrap();
            let port = ScriptedRootPort::new(vec![RootReadOutcome::Data(
                RootResultData::try_new(
                    kind,
                    NonZeroU64::MIN,
                    bytes::Bytes::from(body),
                    Some(RootResultEnd {
                        sequence: NonZeroU64::new(2).unwrap(),
                        output_rows: rows,
                    }),
                )
                .unwrap(),
            )]);
            let seal_port = Arc::new(TestSuccessSealPort::default());
            let (status_sender, statuses) = accepted_root_status_projection_with_control_port(
                root,
                Arc::clone(&seal_port) as Arc<dyn AcceptedRootControlPort>,
            );
            status_sender.publish(finished(root)).unwrap();
            let (_terminal_sender, terminal) = native_attempt_terminal_channel();
            let binding = RootRelayBinding {
                port: port.clone(),
                window: window.retain_alias(),
                frontier: super::super::RootRelayFrontier::new(root, RootProfileId::V1, kind, None)
                    .unwrap(),
                max_wait: Duration::from_millis(5),
            };
            let relay = tokio::spawn(run_root_relay(
                permit, root, scope, binding, statuses, terminal,
            ));
            let segment = next_segment(&mut stream).await;
            assert_eq!(segment.rows(), rows);
            let record = ScalarRecord::decode_owned(&schema, segment.body()).unwrap();
            assert_eq!(
                u64::from(matches!(record, ScalarRecord::Value(_))),
                segment.rows()
            );
            drop(record);
            assert!(seal_port.requests.lock().unwrap().is_empty());
            segment.complete();
            seal_port
                .next()
                .await
                .accept(status_sender, Vec::new())
                .unwrap();
            let ResultDelivery::End(end) = stream.next().await.unwrap().unwrap() else {
                panic!("end")
            };
            assert_eq!(end.root_output_rows(), Some(rows));
            end.complete();
            assert_eq!(
                tokio::time::timeout(Duration::from_secs(1), relay)
                    .await
                    .unwrap()
                    .unwrap()
                    .unwrap(),
                LogicalConclusion::Succeeded
            );
            drop((stream, actor, owner, window));
        }
    }

    #[tokio::test]
    async fn count_only_relay_preserves_end_rows_after_success_seal() {
        let kind = RootOutputKind::CountOnly;
        let (
            Harness {
                control: _control,
                scope,
                actor,
                owner,
                permit,
                mut stream,
                root,
            },
            window,
        ) = relay_harness_with(
            74,
            crate::api::ResultRowCarrier::relayed(kind, None).unwrap(),
        )
        .await;
        let port = ScriptedRootPort::new(vec![RootReadOutcome::End(RootResultEnd {
            sequence: NonZeroU64::MIN,
            output_rows: 42,
        })]);
        let seal_port = Arc::new(TestSuccessSealPort::default());
        let (status_sender, statuses) = accepted_root_status_projection_with_control_port(
            root,
            Arc::clone(&seal_port) as Arc<dyn AcceptedRootControlPort>,
        );
        status_sender.publish(finished(root)).unwrap();
        let (_terminal_sender, terminal) = native_attempt_terminal_channel();
        let binding = RootRelayBinding {
            port: port.clone(),
            window: window.retain_alias(),
            frontier: super::super::RootRelayFrontier::new(root, RootProfileId::V1, kind, None)
                .unwrap(),
            max_wait: Duration::from_millis(5),
        };
        let relay = tokio::spawn(run_root_relay(
            permit, root, scope, binding, statuses, terminal,
        ));
        let seal = seal_port.next().await;
        assert!(
            tokio::time::timeout(Duration::from_millis(20), stream.next())
                .await
                .is_err()
        );
        seal.accept(status_sender, Vec::new()).unwrap();
        let ResultDelivery::End(end) = stream.next().await.unwrap().unwrap() else {
            panic!("CountOnly must produce End without a data segment");
        };
        assert_eq!(end.root_output_rows(), Some(42));
        end.complete();
        assert_eq!(
            tokio::time::timeout(Duration::from_secs(1), relay)
                .await
                .unwrap()
                .unwrap()
                .unwrap(),
            LogicalConclusion::Succeeded
        );
        let requests = port.requests();
        assert_eq!(requests[0], (Some(1), 0));
        assert!(requests[1..].iter().all(|request| *request == (None, 0)));
        drop((stream, actor, owner, window));
    }

    fn assert_relay_failure_input(
        error: &QueryExecutionError,
        root: TaskIdentity,
        expected: super::super::RootRelayError,
    ) {
        use crate::execution_error::{ExecutionFailureSite, QueryFailureReason};
        use novarocks_execution_contract::failure::FailureProjection;
        let input = error
            .execution_failure()
            .expect("relay preserves a canonical input");
        assert_eq!(input.fact(), expected.failure_fact());
        assert_eq!(input.detail(), &expected.safe_detail());
        assert_eq!(input.site(), ExecutionFailureSite::RootRead { root });
        assert_eq!(
            input.topology_requirement(),
            NativeAttemptTopologyRequirement::LiveSnapshot
        );
        assert_ne!(
            input.fact().reason(),
            QueryFailureReason::BackendLost.failure_fact().reason()
        );
        assert_eq!(input.fact().resource(), None);
        assert_eq!(input.fact().representation(), None);
        let class = match expected {
            super::super::RootRelayError::Body(_) => AttemptFailureClass::ExecutionFailure,
            _ => AttemptFailureClass::ContractViolation,
        };
        assert_eq!(input.attempt_class(), class);
    }

    #[tokio::test]
    async fn relay_refuses_exact_prefix_only_and_complete_row_prefix_suffix_without_ack() {
        for (body, label) in [
            (&[1, 0, 0, 0][..], "exact four-byte prefix-only body"),
            (
                &[1, 0, 0, 0, b'a', 1, 0, 0, 0][..],
                "complete valid row followed by a four-byte prefix-only suffix",
            ),
        ] {
            let (
                Harness {
                    control: _control,
                    scope,
                    actor,
                    owner,
                    permit,
                    mut stream,
                    root,
                },
                window,
            ) = relay_harness(75).await;
            let capacity = scope.result_capacity().unwrap();
            assert_eq!(capacity.snapshot().held_positions, [1, 0, 0, 0]);
            let (status_sender, statuses) = accepted_root_status_projection(root);
            status_sender.publish(running(root)).unwrap();
            let port = ScriptedRootPort::new(vec![rows_data(1, body, None)]);
            let (_terminal_sender, terminal_source) = native_attempt_terminal_channel();
            let error = tokio::time::timeout(
                Duration::from_secs(1),
                run_root_relay(
                    permit,
                    root,
                    scope,
                    relay_binding(root, port.clone(), &window),
                    statuses,
                    terminal_source,
                ),
            )
            .await
            .unwrap()
            .unwrap_err();
            let ResultPumpFailure::DecisionPending(decision) = error else {
                panic!("{label} must retain its failure decision");
            };
            assert_eq!(decision.class(), AttemptFailureClass::ExecutionFailure);
            assert_eq!(
                decision.topology_requirement(),
                NativeAttemptTopologyRequirement::LiveSnapshot
            );
            let expected_error = decision.error().clone();
            assert_relay_failure_input(
                &expected_error,
                root,
                super::super::RootRelayError::Body(
                    novarocks_result_contract::ClientBodyError::PrefixWithoutPayload,
                ),
            );
            assert_eq!(
                tokio::time::timeout(Duration::from_secs(1), decision.fail_logical(&actor))
                    .await
                    .unwrap()
                    .unwrap(),
                LogicalConclusion::Failed
            );
            // Whole-body validation must reject the valid row too; the first
            // consumer observation is the failure, never a Segment delivery.
            let delivered_error = match tokio::time::timeout(Duration::from_secs(1), stream.next())
                .await
                .unwrap()
            {
                Err(error) => error,
                Ok(_) => panic!("whole-body failure must precede every Segment"),
            };
            assert_eq!(delivered_error, expected_error, "{label}");
            assert!(stream.next().await.unwrap().is_none());
            let requests = port.requests();
            assert_eq!(requests, vec![(Some(1), 0)], "{label}");
            assert!(
                requests
                    .iter()
                    .all(|(wanted, consumed)| wanted.is_some() && *consumed == 0),
                "{label} must neither ACK-only nor acknowledge consumption"
            );
            // The test's original grant still holds its one position. After
            // dropping it, the existing capacity oracle detects leaked relay
            // window aliases; this is not a physical-allocation exit oracle.
            assert_eq!(capacity.snapshot().held_positions, [1, 0, 0, 0]);
            drop((stream, actor, window, status_sender));
            owner
                .into_residual_stand_down_supervisor()
                .join()
                .await
                .unwrap();
            assert_eq!(capacity.snapshot().held_positions, [0; 4]);
        }
    }

    #[tokio::test]
    async fn relay_refuses_malformed_rows_and_end_mismatch_before_delivery() {
        use super::super::RootRelayError;
        use novarocks_result_contract::ClientBodyError;
        for (outcomes, expected, label) in [
            (
                vec![rows_data(1, &[1, 0, 0, 0, b'a', 1, 0], None)],
                RootRelayError::Body(ClientBodyError::PrefixWithoutPayload),
                "prefix without payload",
            ),
            (
                vec![rows_data(1, &[2, 0, 0, 0, b'a'], Some((2, 1)))],
                RootRelayError::Body(ClientBodyError::EndInsideRow),
                "End inside an unfinished row",
            ),
            (
                vec![rows_data(1, &[1, 0, 0, 0, b'a'], Some((2, 3)))],
                RootRelayError::EndMismatch,
                "End row count",
            ),
            (
                vec![rows_data(2, &[1, 0, 0, 0, b'a'], None)],
                RootRelayError::Sequence,
                "sequence gap",
            ),
            (
                vec![rows_data(2, &[1, 0, 0, 0, b'a'], Some((3, 1)))],
                RootRelayError::Sequence,
                "sequence gap carrying End",
            ),
        ] {
            let (
                Harness {
                    control: _control,
                    scope,
                    actor,
                    owner,
                    permit,
                    mut stream,
                    root,
                },
                window,
            ) = relay_harness(32).await;
            let capacity = scope.result_capacity().unwrap();
            let (status_sender, statuses) = accepted_root_status_projection(root);
            status_sender.publish(running(root)).unwrap();
            let port = ScriptedRootPort::new(outcomes);
            let (_terminal_sender, terminal_source) = native_attempt_terminal_channel();
            let result = tokio::time::timeout(
                Duration::from_secs(1),
                run_root_relay(
                    permit,
                    root,
                    scope,
                    relay_binding(root, port.clone(), &window),
                    statuses,
                    terminal_source,
                ),
            )
            .await
            .unwrap()
            .unwrap_err();
            let ResultPumpFailure::DecisionPending(decision) = result else {
                panic!("{label} must retain its original failure decision");
            };
            let expected_error = decision.error().clone();
            assert_relay_failure_input(&expected_error, root, expected);
            assert_eq!(
                decision.fail_logical(&actor).await.unwrap(),
                LogicalConclusion::Failed,
            );
            // Validation precedes frontier mutation, delivery, and ACK.
            let delivered_error = match tokio::time::timeout(Duration::from_secs(1), stream.next())
                .await
                .unwrap()
            {
                Err(error) => error,
                Ok(_) => {
                    panic!("malformed replies must deliver their original failure, never a Segment")
                }
            };
            assert_eq!(delivered_error, expected_error, "{label}");
            assert!(stream.next().await.unwrap().is_none());
            assert_eq!(port.requests(), vec![(Some(1), 0)], "{label}");
            assert_eq!(capacity.snapshot().held_positions, [1, 0, 0, 0]);
            drop((stream, actor, window, status_sender));
            owner
                .into_residual_stand_down_supervisor()
                .join()
                .await
                .unwrap();
            assert_eq!(capacity.snapshot().held_positions, [0; 4]);
        }
    }

    #[tokio::test]
    async fn a_client_segment_on_a_count_only_result_is_refused() {
        let (
            Harness {
                control: _control,
                scope,
                actor,
                owner,
                permit,
                mut stream,
                root,
            },
            window,
        ) = relay_harness_with(
            33,
            crate::api::ResultRowCarrier::relayed(
                novarocks_result_contract::RootOutputKind::CountOnly,
                None,
            )
            .unwrap(),
        )
        .await;
        let capacity = scope.result_capacity().unwrap();
        // Keep the native root and admitted window valid; only the stream's
        // frozen consumer purpose conflicts with the ClientRows delivery.
        drop(window);
        let window = scope
            .result_capacity()
            .unwrap()
            .try_acquire(&scope, ResultWindowClass::Client)
            .unwrap();
        let (status_sender, statuses) = accepted_root_status_projection(root);
        status_sender.publish(running(root)).unwrap();
        let port = ScriptedRootPort::new(vec![rows_data(1, &[1, 0, 0, 0, b'a'], None)]);
        let (_terminal_sender, terminal_source) = native_attempt_terminal_channel();
        let error = tokio::time::timeout(
            Duration::from_secs(1),
            run_root_relay(
                permit,
                root,
                scope,
                relay_binding(root, port.clone(), &window),
                statuses,
                terminal_source,
            ),
        )
        .await
        .unwrap()
        .unwrap_err();
        assert!(
            matches!(error, ResultPumpFailure::Concluded(_)),
            "unexpected error: {error:?}"
        );
        // A ClientRows segment cannot reach a CountOnly consumer.
        let Err(consumer) = stream.next().await else {
            panic!("the CountOnly consumer must see the refusal");
        };
        assert!(
            consumer.to_string().contains("row carrier"),
            "unexpected consumer error: {consumer}"
        );
        use novarocks_execution_contract::failure::FailureProjection;
        let input = consumer.execution_failure().unwrap();
        assert_eq!(
            input.fact(),
            LogicalExecutionActorError::ResultProtocolViolation.failure_fact(),
        );
        assert_eq!(
            input.site(),
            crate::execution_error::ExecutionFailureSite::Coordinator,
        );
        assert_eq!(
            (input.descriptor().errno, input.descriptor().sqlstate),
            (1105, "HY000")
        );
        assert!(stream.next().await.unwrap().is_none());
        assert!(!port.requests().is_empty());
        assert!(port.requests().iter().all(|(_, consumed)| *consumed == 0));
        drop((stream, actor, window, status_sender));
        owner
            .into_residual_stand_down_supervisor()
            .join()
            .await
            .unwrap();
        assert_eq!(capacity.snapshot().held_positions, [0; 4]);
    }

    #[tokio::test]
    async fn relay_failed_delivery_fails_the_attempt_without_acknowledging_it() {
        let (
            Harness {
                control: _control,
                scope,
                actor,
                owner,
                permit,
                mut stream,
                root,
            },
            window,
        ) = relay_harness(33).await;
        let (status_sender, statuses) = accepted_root_status_projection(root);
        status_sender.publish(running(root)).unwrap();
        let port = ScriptedRootPort::new(vec![rows_data(1, &[1, 0, 0, 0, b'a'], None)]);
        let (_terminal_sender, terminal_source) = native_attempt_terminal_channel();
        let relay = tokio::spawn(run_root_relay(
            permit,
            root,
            scope,
            relay_binding(root, Arc::clone(&port), &window),
            statuses,
            terminal_source,
        ));
        let segment = next_segment(&mut stream).await;
        segment.fail(QueryExecutionError::new(
            QueryExecutionErrorKind::Failed,
            "client went away",
        ));
        assert!(
            tokio::time::timeout(Duration::from_secs(1), relay)
                .await
                .unwrap()
                .unwrap()
                .is_err()
        );
        // The failed item was never acknowledged as consumed.
        assert!(port.requests().iter().all(|(_, consumed)| *consumed == 0));
        drop((stream, actor, owner, window, status_sender));
    }
    #[tokio::test]
    async fn native_terminal_keeps_exact_replacement_witness_through_real_root_pump() {
        for kind in [
            novarocks_execution_contract::OperationKind::UpdateQueryContext,
            novarocks_execution_contract::OperationKind::CreateTask,
        ] {
            let (
                Harness {
                    control: _control,
                    scope,
                    actor,
                    owner,
                    permit,
                    mut stream,
                    root,
                },
                window,
            ) = relay_harness(79).await;
            let (_sender, statuses) = accepted_root_status_projection(root);
            let port = ScriptedRootPort::new(Vec::new());
            let context = QueryContextRef::new(
                root.query_execution_id(),
                novarocks_types::FrontendProcessId::new_v7(),
                root.backend_process_id(),
            );
            let (input, witness, native) =
                crate::api::pre_ready_identity_refusal_test_fixture_for_kind(context, kind);
            let (terminal_sender, terminal) = native_attempt_terminal_channel();
            terminal_sender.publish(NativeAttemptTerminal::Failed(native));
            let error = tokio::time::timeout(
                Duration::from_secs(1),
                run_root_relay(
                    permit,
                    root,
                    scope,
                    relay_binding(root, port.clone(), &window),
                    statuses,
                    terminal,
                ),
            )
            .await
            .unwrap()
            .unwrap_err();
            let ResultPumpFailure::DecisionPending(decision) = error else {
                panic!("the original attempt authority must remain available");
            };
            assert_eq!(decision.class(), AttemptFailureClass::ContractViolation);
            assert_eq!(decision.error().execution_failure(), Some(&input));
            assert_eq!(decision.pre_ready_replacement(), Some(&witness));
            assert_eq!(
                decision.topology_requirement(),
                NativeAttemptTopologyRequirement::ExcludeProcess(root.backend_process_id())
            );
            assert_eq!(
                decision.fail_logical(&actor).await.unwrap(),
                LogicalConclusion::Failed
            );
            assert!(stream.next().await.is_err());
            assert!(port.requests().is_empty());
            drop((stream, actor, owner, window));
        }
    }
}
