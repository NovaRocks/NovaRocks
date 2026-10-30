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

use novarocks_coordination_contract::{
    AttemptActivationIdentity, LogicalExecutionActorId, MonotonicClock, ResultContextWithdrawal,
};

mod internal_ready;
mod result_transport;
pub(crate) use internal_ready::InternalReadyActorPort;
use internal_ready::{
    BindConsumer, ConsumerExit, EndInFlight, EndRequest, InternalRuntime, PendingEnd, ReadyProposal,
};
use result_transport::{ResultSlot, ResultTransport};

use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::num::{NonZeroU32, NonZeroUsize};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use novarocks_execution_contract::{
    AcquireQueryContextAdmissionTicket, EstablishQueryContext, OperationOutcome, QueryContextRef,
    ResultPacketSequence, TaskIdentity, TaskStatus, TaskStatusCursor,
};
use novarocks_types::NativeCompatibilityId;
use novarocks_types::identity::QueryExecutionId;
use novarocks_workload_control::{
    CancellationReason, CancellationView, Obligation, ObligationKey, ObligationKind, Stage,
    StagePermit, WorkOwner, WorkScope,
};
use tokio::runtime::Handle;
use tokio::sync::{mpsc, oneshot, watch};
use tokio::task::JoinHandle;

use crate::api::{
    EndDelivery, ExecutionOutput, InternalConsumerActivation, InternalConsumerSeal,
    QueryExecutionError, QueryResultStream, ResultDeliveryDisposition, ResultDeliveryReceipt,
    ResultSchema, RetainedRootReply, RootSegmentDelivery,
};
use novarocks_result_contract::{ClientRowProfile, ClientRowStreamCursor};

use super::actor_state::{
    ActorStateError, AttemptCapability, LogicalExecutionState, ReplacementFact, ReplacementToken,
};
use super::receipt_channel;
use super::{
    AbortQueryContextEffectPort, AbortQueryContextIssuePermit, AbortQueryContextIssueSubmit,
    ActiveReplacementResources, AdmissionIssueDisposition, AdmissionIssueReceipt,
    AdmissionIssueSettlement, BeginSchemaDelivery, ContextStandDownCause, ContextStandDownError,
    ContextStandDownLedger, ContextStandDownSnapshot, DeliveryPermit, EstablishIssueError,
    EstablishIssueLedger, EstablishIssuePermit, EstablishIssueSnapshot, ExecutionPhase,
    LogicalConclusion, LogicalOutputMode, RecoveryRefusal, RegistryContextConvergence,
    ReplacementQualificationEffectAdmission, ReplacementQualificationEffectPort,
    ReplacementQualificationEffectReceipt, ReplacementQualificationEffectSubmission,
    ReplacementQualificationFailure, ReplacementQualificationIdentity,
    ReplacementQualificationRequest, ReplacementQualificationSettlement,
    ReplacementWorkerAdmissionEvidence, ResultPacket, SchemaDeliveryPermit,
};
use novarocks_coordination_contract::{
    AttemptFailureClass, ExecutionEffect, MonotonicInstant, ObservedTaskTransition, RecoveryMode,
    StatusObservation, classify_observation, classify_observed_task_transition,
};

#[derive(Debug)]
pub struct ProcessMonotonicClock {
    origin: Instant,
}

impl ProcessMonotonicClock {
    pub fn new() -> Self {
        Self {
            origin: Instant::now(),
        }
    }
}

impl Default for ProcessMonotonicClock {
    fn default() -> Self {
        Self::new()
    }
}

impl MonotonicClock for ProcessMonotonicClock {
    fn now(&self) -> MonotonicInstant {
        MonotonicInstant::from_origin(self.origin.elapsed())
    }
}

/// Actor-minted observation capability for one exact attempt root Task.
///
/// Future role adapters report complete Worker facts through this capability;
/// they cannot construct a success receipt or directly authorize client EOF.
/// Clones share one bounded terminal channel. Dropping the last clone before
/// both stable-success facts arrive closes that channel and fails the attempt.
#[derive(Clone, Debug)]
pub(crate) struct RootResultObserver {
    activation: AttemptActivationIdentity,
    root: TaskIdentity,
    mailbox: mpsc::Sender<ActorCommand>,
    terminal_mailbox: mpsc::Sender<RootTerminalObservation>,
}

impl RootResultObserver {
    #[cfg(test)]
    pub(crate) const fn activation(&self) -> AttemptActivationIdentity {
        self.activation
    }

    #[cfg(test)]
    pub(crate) async fn observe_status(
        &self,
        status: TaskStatus,
    ) -> Result<(), LogicalExecutionActorError> {
        self.observe_status_with_failure(status, RootTerminalFailure::Unspecified)
            .await
    }

    pub(crate) async fn observe_status_with_failure(
        &self,
        status: TaskStatus,
        terminal_failure: RootTerminalFailure,
    ) -> Result<(), LogicalExecutionActorError> {
        if status.state().is_failure()
            || status.state().is_cancellation()
            || status.state().is_abort()
            || !matches!(&terminal_failure, RootTerminalFailure::Unspecified)
        {
            let (reply, response) = oneshot::channel();
            self.terminal_mailbox
                .send(RootTerminalObservation {
                    activation: self.activation,
                    root: self.root,
                    status,
                    terminal_failure,
                    reply,
                })
                .await
                .map_err(|_| LogicalExecutionActorError::MailboxClosed)?;
            return response
                .await
                .map_err(|_| LogicalExecutionActorError::MailboxClosed)?;
        }
        request(&self.mailbox, |reply| ActorCommand::ObserveRootStatus {
            activation: self.activation,
            root: self.root,
            status,
            reply,
        })
        .await
    }

    pub(crate) async fn observe_local_root_end(
        &self,
        root: TaskIdentity,
        end: novarocks_execution_contract::root_result::RootResultEnd,
    ) -> Result<(), LogicalExecutionActorError> {
        self.observe_root_end(
            root,
            ResultPacketSequence::new(end.sequence.get() - 1),
            Some(end.output_rows),
        )
        .await
    }

    async fn observe_root_end(
        &self,
        root: TaskIdentity,
        sequence: ResultPacketSequence,
        root_output_rows: Option<u64>,
    ) -> Result<(), LogicalExecutionActorError> {
        request(&self.mailbox, |reply| ActorCommand::ObserveRootEosAck {
            activation: self.activation,
            root,
            expected_root: self.root,
            sequence,
            root_output_rows,
            reply,
        })
        .await
    }
}

/// Coalesced, actor-local abandonment signal shared by every typestate form of
/// one attempt authority. It uses constant memory and never waits for mailbox
/// capacity from `Drop`.
#[derive(Debug)]
struct PermitLifetime {
    abandoned: watch::Sender<bool>,
    settled: AtomicBool,
}

impl PermitLifetime {
    fn abandon(&self) {
        if !self.settled.load(Ordering::Acquire) {
            self.abandoned.send_replace(true);
        }
    }

    fn settle(&self) {
        self.settled.store(true, Ordering::Release);
    }
}

fn abandon_on_drop(lifetime: &mut Option<Arc<PermitLifetime>>) {
    if let Some(lifetime) = lifetime.take() {
        lifetime.abandon();
    }
}

/// Move-only authority to instantiate one exact attempt.
#[derive(Debug)]
#[must_use = "attempt instantiation must report readiness, failure, or cancellation"]
pub struct AttemptInstantiationPermit {
    capability: Option<AttemptCapability>,
    lifetime: Option<Arc<PermitLifetime>>,
    mailbox_liveness: Option<mpsc::Sender<ActorCommand>>,
}

impl AttemptInstantiationPermit {
    pub fn identity(&self) -> AttemptActivationIdentity {
        identity_of(
            self.capability
                .as_ref()
                .expect("live instantiation permit retains its capability"),
        )
    }

    /// Seals successful initialization into the only receipt accepted by the
    /// actor's activation transition.
    pub fn ready(mut self) -> AttemptReadinessReceipt {
        AttemptReadinessReceipt {
            capability: self.capability.take(),
            lifetime: self.lifetime.take(),
            mailbox_liveness: self.mailbox_liveness.take(),
        }
    }

    /// Consumes initialization evidence for grants already recorded by the
    /// actor. The opaque active owner keeps exclusive cleanup responsibility;
    /// this call transfers neither the Acquire operation nor ticket ownership.
    pub async fn take_replacement_admission_evidence(
        &self,
    ) -> Result<Option<Box<[ReplacementWorkerAdmissionEvidence]>>, LogicalExecutionActorError> {
        request(
            self.mailbox_liveness
                .as_ref()
                .expect("live instantiation permit retains actor liveness"),
            |reply| ActorCommand::TakeReplacementAdmissionEvidence {
                activation: self.identity(),
                reply,
            },
        )
        .await
    }

    fn into_parts(
        mut self,
    ) -> (
        AttemptCapability,
        Arc<PermitLifetime>,
        mpsc::Sender<ActorCommand>,
    ) {
        let capability = self
            .capability
            .take()
            .expect("live instantiation permit retains its capability");
        let lifetime = self
            .lifetime
            .take()
            .expect("live instantiation permit retains its lifetime");
        let mailbox_liveness = self
            .mailbox_liveness
            .take()
            .expect("live instantiation permit retains actor liveness");
        (capability, lifetime, mailbox_liveness)
    }
}

impl Drop for AttemptInstantiationPermit {
    fn drop(&mut self) {
        abandon_on_drop(&mut self.lifetime);
    }
}

/// Move-only proof that initialization completed for one exact attempt.
#[derive(Debug)]
#[must_use = "attempt readiness must be consumed by its owning actor"]
pub struct AttemptReadinessReceipt {
    capability: Option<AttemptCapability>,
    lifetime: Option<Arc<PermitLifetime>>,
    mailbox_liveness: Option<mpsc::Sender<ActorCommand>>,
}

impl AttemptReadinessReceipt {
    pub fn identity(&self) -> AttemptActivationIdentity {
        identity_of(
            self.capability
                .as_ref()
                .expect("live readiness receipt retains its capability"),
        )
    }

    fn into_parts(
        mut self,
    ) -> (
        AttemptCapability,
        Arc<PermitLifetime>,
        mpsc::Sender<ActorCommand>,
    ) {
        let capability = self
            .capability
            .take()
            .expect("live readiness receipt retains its capability");
        let lifetime = self
            .lifetime
            .take()
            .expect("live readiness receipt retains its lifetime");
        let mailbox_liveness = self
            .mailbox_liveness
            .take()
            .expect("live readiness receipt retains actor liveness");
        (capability, lifetime, mailbox_liveness)
    }
}

impl Drop for AttemptReadinessReceipt {
    fn drop(&mut self) {
        abandon_on_drop(&mut self.lifetime);
    }
}

/// Move-only authority for commands concerning one running attempt.
#[derive(Debug)]
#[must_use = "a running attempt must report completion or failure"]
pub struct RunningAttemptPermit {
    capability: Option<AttemptCapability>,
    lifetime: Option<Arc<PermitLifetime>>,
    mailbox_liveness: Option<mpsc::Sender<ActorCommand>>,
}

/// Non-terminal actor capability used by the Native attempt driver while the
/// supervisor retains the sole running-attempt decision permit.
///
/// This value can issue only admission and Establish commands for one exact
/// activation. It cannot complete, fail, replace, or deliver results, and a
/// stale activation is rejected by the actor after the supervisor consumes the
/// running permit.
pub(crate) struct RunningAttemptDriveAuthority {
    activation: AttemptActivationIdentity,
    mailbox: mpsc::Sender<ActorCommand>,
}

impl fmt::Debug for RunningAttemptDriveAuthority {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RunningAttemptDriveAuthority")
            .field("activation", &self.activation)
            .finish_non_exhaustive()
    }
}

impl RunningAttemptDriveAuthority {
    pub(crate) const fn identity(&self) -> AttemptActivationIdentity {
        self.activation
    }

    pub(crate) async fn may_observe_pre_ready_replacement(
        &self,
        refusal: &crate::execution_error::ExecutionFailureInput,
    ) -> Result<bool, LogicalExecutionActorError> {
        request(&self.mailbox, |reply| {
            ActorCommand::MayObservePreReadyReplacement {
                activation: self.activation,
                refusal: refusal.clone(),
                reply,
            }
        })
        .await
    }

    pub(crate) async fn begin_admission_issue(
        &self,
        request_to_issue: AcquireQueryContextAdmissionTicket,
    ) -> Result<AdmissionIssueReceipt, LogicalExecutionActorError> {
        request(&self.mailbox, |reply| ActorCommand::BeginAdmissionIssue {
            activation: self.activation,
            request: request_to_issue,
            reply,
        })
        .await
    }

    pub(crate) async fn settle_admission_issue(
        &self,
        admission_issue: AdmissionIssueReceipt,
        settlement: AdmissionIssueSettlement,
    ) -> Result<AdmissionIssueDisposition, LogicalExecutionActorError> {
        request(&self.mailbox, |reply| ActorCommand::SettleAdmissionIssue {
            activation: self.activation,
            admission_issue,
            settlement,
            reply,
        })
        .await
    }

    pub(crate) async fn authorize_establish(
        &self,
        request_to_issue: Arc<EstablishQueryContext>,
        native_compatibility_id: NativeCompatibilityId,
    ) -> Result<EstablishIssuePermit, LogicalExecutionActorError> {
        request(&self.mailbox, |reply| ActorCommand::AuthorizeEstablish {
            activation: self.activation,
            request: request_to_issue,
            native_compatibility_id,
            reply,
        })
        .await
    }

    pub(crate) async fn reauthorize_establish(
        &self,
        context: QueryContextRef,
    ) -> Result<EstablishIssuePermit, LogicalExecutionActorError> {
        request(&self.mailbox, |reply| ActorCommand::ReauthorizeEstablish {
            activation: self.activation,
            context,
            reply,
        })
        .await
    }
}

impl RunningAttemptPermit {
    pub fn identity(&self) -> AttemptActivationIdentity {
        identity_of(
            self.capability
                .as_ref()
                .expect("live running permit retains its capability"),
        )
    }

    fn new(
        capability: AttemptCapability,
        lifetime: Arc<PermitLifetime>,
        mailbox_liveness: mpsc::Sender<ActorCommand>,
    ) -> Self {
        Self {
            capability: Some(capability),
            lifetime: Some(lifetime),
            mailbox_liveness: Some(mailbox_liveness),
        }
    }

    pub(crate) fn native_drive_authority(&self) -> RunningAttemptDriveAuthority {
        RunningAttemptDriveAuthority {
            activation: self.identity(),
            mailbox: self.mailbox().clone(),
        }
    }

    /// Freezes the actor's own local time immediately before the admission
    /// transport is issued. Exact transport replay keeps this receipt.
    pub async fn begin_admission_issue(
        &self,
        request_to_issue: AcquireQueryContextAdmissionTicket,
    ) -> Result<AdmissionIssueReceipt, LogicalExecutionActorError> {
        request(self.mailbox(), |reply| ActorCommand::BeginAdmissionIssue {
            activation: self.identity(),
            request: request_to_issue,
            reply,
        })
        .await
    }

    /// Records one exact Worker admission grant in the actor-owned Establish
    /// ledger before any request can be authorized from it.
    pub async fn settle_admission_issue(
        &self,
        admission_issue: AdmissionIssueReceipt,
        settlement: AdmissionIssueSettlement,
    ) -> Result<AdmissionIssueDisposition, LogicalExecutionActorError> {
        request(self.mailbox(), |reply| ActorCommand::SettleAdmissionIssue {
            activation: self.identity(),
            admission_issue,
            settlement,
            reply,
        })
        .await
    }

    /// Authorizes one exact Establish and returns its move-only pre-transport
    /// permit. Replays must retain the same request allocation.
    pub async fn authorize_establish(
        &self,
        request_to_issue: Arc<EstablishQueryContext>,
        native_compatibility_id: NativeCompatibilityId,
    ) -> Result<EstablishIssuePermit, LogicalExecutionActorError> {
        request(self.mailbox(), |reply| ActorCommand::AuthorizeEstablish {
            activation: self.identity(),
            request: request_to_issue,
            native_compatibility_id,
            reply,
        })
        .await
    }

    /// Re-authorizes the actor-retained exact request after a definitely-unsent
    /// or transport-unknown outcome. The caller cannot replace its payload.
    pub async fn reauthorize_establish(
        &self,
        context: QueryContextRef,
    ) -> Result<EstablishIssuePermit, LogicalExecutionActorError> {
        request(self.mailbox(), |reply| ActorCommand::ReauthorizeEstablish {
            activation: self.identity(),
            context,
            reply,
        })
        .await
    }

    /// Transfers one validated Backend-encoded root item to the actor-owned
    /// delivery boundary. The future resolves only after
    /// the protocol consumer completes or rejects it; that completion is the
    /// in-order receipt the relay acknowledges to the Backend.
    pub(crate) async fn deliver_root_segment(
        &self,
        sequence: ResultPacketSequence,
        segment: Arc<RetainedRootReply>,
        client_rows: Option<(ClientRowProfile, ClientRowStreamCursor)>,
        rows: u64,
        resident_window: crate::api::RootRelayResidentWindow,
    ) -> Result<(), LogicalExecutionActorError> {
        request(self.mailbox(), |reply| ActorCommand::DeliverRootSegment {
            activation: self.identity(),
            sequence,
            segment,
            client_rows,
            rows,
            resident_window,
            reply,
        })
        .await
    }

    /// Binds the scheduler's exact root Task exactly once for this activation
    /// and returns the only typed observation surface accepted by the actor.
    pub(crate) async fn bind_root_result(
        &self,
        root: TaskIdentity,
    ) -> Result<RootResultObserver, LogicalExecutionActorError> {
        let activation = self.identity();
        let (terminal_mailbox, terminal_receiver) = mpsc::channel(1);
        request(self.mailbox(), |reply| ActorCommand::BindRootResult {
            activation,
            root,
            terminal_receiver,
            reply,
        })
        .await?;
        Ok(RootResultObserver {
            activation,
            root,
            mailbox: self.mailbox().clone(),
            terminal_mailbox,
        })
    }

    pub(crate) async fn bind_internal_consumer(
        &self,
        root: TaskIdentity,
    ) -> Result<InternalConsumerActivation, LogicalExecutionActorError> {
        request(self.mailbox(), |reply| {
            ActorCommand::BindInternalConsumer(BindConsumer {
                activation: self.identity(),
                root,
                lifetime: Arc::clone(
                    self.lifetime
                        .as_ref()
                        .expect("running authority retains its lifetime"),
                ),
                mailbox: self.mailbox().clone(),
                reply,
            })
        })
        .await
    }

    /// End consumption borrows authority. A pre-ready failure must still be
    /// able to return this exact permit to the recovery owner.
    pub(crate) async fn deliver_internal_end(
        &self,
        root: TaskIdentity,
        end: novarocks_execution_contract::root_result::RootResultEnd,
    ) -> Result<(), LogicalExecutionActorError> {
        request(self.mailbox(), |reply| {
            ActorCommand::DeliverInternalEnd(EndRequest {
                activation: self.identity(),
                root,
                end,
                reply,
            })
        })
        .await
    }

    /// Serializes a pump-owned attempt failure against the result delivery
    /// boundary. A successful reply proves that no batch is visible or in a
    /// protocol-owned delivery, and freezes future delivery until the permit
    /// owner chooses replacement or logical failure.
    pub(crate) async fn freeze_result_attempt_failure(
        &self,
        error: QueryExecutionError,
    ) -> Result<(), LogicalExecutionActorError> {
        request(self.mailbox(), |reply| {
            ActorCommand::FreezeResultAttemptFailure {
                activation: self.identity(),
                error,
                reply,
            }
        })
        .await
    }

    /// Transfers the running authority after the coordinator has submitted
    /// all root-result packets. This does not assert success: the actor still
    /// requires exact root Finished and final Worker EOS ACK observations.
    pub(crate) async fn finish_result_stream(
        self,
        withdrawals: Vec<ResultContextWithdrawal>,
    ) -> Result<LogicalConclusion, RunningAttemptHandoffError> {
        self.handoff(|permit, reply| ActorCommand::FinishResultStream {
            permit,
            withdrawals,
            reply,
        })
        .await
    }

    pub(crate) async fn await_work_cancellation(
        self,
    ) -> Result<LogicalConclusion, RunningAttemptHandoffError> {
        self.handoff(|permit, reply| ActorCommand::AwaitWorkCancellation { permit, reply })
            .await
    }

    async fn handoff(
        self,
        command: impl FnOnce(RunningAttemptPermit, ActorReply<LogicalConclusion>) -> ActorCommand,
    ) -> Result<LogicalConclusion, RunningAttemptHandoffError> {
        let Some(sender) = self.mailbox_liveness.as_ref().cloned() else {
            return Err(RunningAttemptHandoffError::NotSubmitted {
                permit: self,
                error: LogicalExecutionActorError::StaleAuthority,
            });
        };
        let slot = match sender.reserve().await {
            Ok(slot) => slot,
            Err(_) => {
                return Err(RunningAttemptHandoffError::NotSubmitted {
                    permit: self,
                    error: LogicalExecutionActorError::MailboxClosed,
                });
            }
        };
        let (reply, response) = oneshot::channel();
        slot.send(command(self, reply));
        match response.await {
            Ok(Ok(conclusion)) => Ok(conclusion),
            Ok(Err(LogicalExecutionActorError::ExecutionConcluded(conclusion))) => {
                Err(RunningAttemptHandoffError::ExecutionConcluded(conclusion))
            }
            Ok(Err(error)) => Err(RunningAttemptHandoffError::ActorOutcomeUnknown(error)),
            Err(_) => Err(RunningAttemptHandoffError::ActorOutcomeUnknown(
                LogicalExecutionActorError::MailboxClosed,
            )),
        }
    }

    fn mailbox(&self) -> &mpsc::Sender<ActorCommand> {
        self.mailbox_liveness
            .as_ref()
            .expect("live running permit retains actor liveness")
    }

    fn into_parts(
        mut self,
    ) -> (
        AttemptCapability,
        Arc<PermitLifetime>,
        mpsc::Sender<ActorCommand>,
    ) {
        let capability = self
            .capability
            .take()
            .expect("live running permit retains its capability");
        let lifetime = self
            .lifetime
            .take()
            .expect("live running permit retains its lifetime");
        let mailbox_liveness = self
            .mailbox_liveness
            .take()
            .expect("live running permit retains actor liveness");
        (capability, lifetime, mailbox_liveness)
    }
}

/// Result of handing the sole running authority to a terminal actor command.
#[derive(Debug)]
pub(crate) enum RunningAttemptHandoffError {
    /// Mailbox ownership was never transferred, so the attempt decision remains
    /// available to the caller.
    NotSubmitted {
        permit: RunningAttemptPermit,
        error: LogicalExecutionActorError,
    },
    /// The actor fixed this exact conclusion while processing the handoff.
    ExecutionConcluded(LogicalConclusion),
    /// The actor accepted the permit, but its reply did not prove a conclusion.
    ActorOutcomeUnknown(LogicalExecutionActorError),
}

impl Drop for RunningAttemptPermit {
    fn drop(&mut self) {
        abandon_on_drop(&mut self.lifetime);
    }
}

/// Move-only owner of the interval between a failed attempt and its proposed
/// successor. Dropping it cancels the logical execution and leaves every old
/// attempt ledger under the actor's residual supervision.
#[derive(Debug)]
#[must_use = "replacement qualification must activate its successor or conclude"]
pub struct ReplacementQualification {
    identity: ReplacementQualificationIdentity,
    lifetime: Option<Arc<PermitLifetime>>,
    mailbox_liveness: Option<mpsc::Sender<ActorCommand>>,
}

impl ReplacementQualification {
    pub const fn identity(&self) -> ReplacementQualificationIdentity {
        self.identity
    }

    fn into_parts(
        mut self,
    ) -> (
        ReplacementQualificationIdentity,
        Arc<PermitLifetime>,
        mpsc::Sender<ActorCommand>,
    ) {
        let lifetime = self
            .lifetime
            .take()
            .expect("live replacement qualification retains its lifetime");
        let mailbox_liveness = self
            .mailbox_liveness
            .take()
            .expect("live replacement qualification retains actor liveness");
        (self.identity, lifetime, mailbox_liveness)
    }
}

impl Drop for ReplacementQualification {
    fn drop(&mut self) {
        abandon_on_drop(&mut self.lifetime);
    }
}

fn identity_of(capability: &AttemptCapability) -> AttemptActivationIdentity {
    AttemptActivationIdentity::from_parts(
        capability.actor_instance_id(),
        capability.execution(),
        capability.generation(),
    )
}

fn attempt_obligation_key(
    actor: LogicalExecutionActorId,
    execution: QueryExecutionId,
) -> ObligationKey {
    let query = execution.query_id();
    let mut bytes = [0_u8; 32];
    bytes[0..8].copy_from_slice(&actor.get().to_le_bytes());
    bytes[8..16].copy_from_slice(&query.high().to_le_bytes());
    bytes[16..24].copy_from_slice(&query.low().to_le_bytes());
    bytes[24..32].copy_from_slice(&execution.attempt_id().get().to_le_bytes());
    ObligationKey(bytes)
}

/// Honest construction inputs for the currently connected actor slice.
///
/// Recovery, result delivery, and attempt effects are selected together so
/// callers cannot assemble a mode that lacks its actor-owned effect gates.
pub struct LogicalExecutionActorConfig {
    initial_execution: QueryExecutionId,
    recovery_mode: RecoveryMode,
    effect: ExecutionEffect,
    output_mode: LogicalOutputMode,
    max_attempts: u32,
    mailbox_capacity: NonZeroUsize,
    required_establish_contexts: Vec<QueryContextRef>,
    max_admission_issues_per_context: NonZeroUsize,
    max_establish_authorizations_per_context: NonZeroUsize,
    abort_effect_port: Option<Arc<dyn AbortQueryContextEffectPort>>,
    replacement_effect_port: Option<Arc<dyn ReplacementQualificationEffectPort>>,
    replacement_reservation_valid_for: Option<Duration>,
    max_abort_authorizations_per_context: NonZeroUsize,
    clock: Arc<dyn MonotonicClock>,
    work_owner: Option<WorkOwner>,
    execution_stage: Option<StagePermit>,
    result_schema: Option<ResultSchema>,
    result_delivery_capacity: Option<NonZeroUsize>,
    result_row_carrier: Option<crate::api::ResultRowCarrier>,
    internal_budgets: crate::api::ComputationObservationBudgets,
    internal_plan: Option<crate::api::PlanSeal>,
    native_actions: Option<novarocks_workload_control::joint_admission::NativeAsyncActionAuthority>,
}

impl fmt::Debug for LogicalExecutionActorConfig {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("LogicalExecutionActorConfig")
            .field("initial_execution", &self.initial_execution)
            .field("recovery_mode", &self.recovery_mode)
            .field("effect", &self.effect)
            .field("output_mode", &self.output_mode)
            .field("max_attempts", &self.max_attempts)
            .field(
                "work",
                &self.work_owner.as_ref().map(|owner| owner.scope().id()),
            )
            .finish_non_exhaustive()
    }
}

impl Drop for LogicalExecutionActorConfig {
    fn drop(&mut self) {
        self.execution_stage.take();
        if let Some(owner) = self.work_owner.take() {
            owner.complete_after_terminal_cancel_settled();
        }
    }
}

fn close_unstarted_governance(work_owner: WorkOwner, execution_stage: StagePermit) {
    drop(execution_stage);
    // No actor was installed, so no attempt or output owner can deliver a
    // cancellation notification after this constructor returns.
    work_owner.complete_after_terminal_cancel_settled();
}

impl LogicalExecutionActorConfig {
    pub(crate) fn with_native_action_authority(
        mut self,
        source: Option<novarocks_workload_control::joint_admission::NativeAsyncActionAuthority>,
    ) -> Self {
        self.native_actions = source;
        self
    }

    pub(crate) const fn initial_execution(&self) -> QueryExecutionId {
        self.initial_execution
    }

    pub(crate) fn required_establish_contexts(&self) -> &[QueryContextRef] {
        &self.required_establish_contexts
    }

    /// Constructs an execution whose business contract permits no attempt
    /// replacement. External-effect executions can only use this constructor.
    pub fn no_recovery_completion(
        initial_execution: QueryExecutionId,
        effect: ExecutionEffect,
        mailbox_capacity: NonZeroUsize,
        required_establish_contexts: Vec<QueryContextRef>,
        max_admission_issues_per_context: NonZeroUsize,
        max_establish_authorizations_per_context: NonZeroUsize,
        work_owner: WorkOwner,
        initial_execution_stage: StagePermit,
    ) -> Result<Self, LogicalExecutionActorError> {
        let work = work_owner.scope();
        if initial_execution_stage
            .check(&work, Stage::Execution)
            .is_err()
        {
            close_unstarted_governance(work_owner, initial_execution_stage);
            return Err(LogicalExecutionActorError::InvariantViolation);
        }
        Ok(Self {
            initial_execution,
            recovery_mode: RecoveryMode::NoRecovery,
            effect,
            output_mode: LogicalOutputMode::CompletionOnly,
            max_attempts: 1,
            mailbox_capacity,
            required_establish_contexts,
            max_admission_issues_per_context,
            max_establish_authorizations_per_context,
            abort_effect_port: None,
            replacement_effect_port: None,
            replacement_reservation_valid_for: None,
            max_abort_authorizations_per_context: max_establish_authorizations_per_context,
            clock: Arc::new(ProcessMonotonicClock::new()),
            work_owner: Some(work_owner),
            execution_stage: Some(initial_execution_stage),
            result_schema: None,
            result_delivery_capacity: None,
            result_row_carrier: None,
            internal_budgets: crate::api::ComputationObservationBudgets::default(),
            internal_plan: None,
            native_actions: None,
        })
    }

    pub fn single_attempt_completion(
        initial_execution: QueryExecutionId,
        effect: ExecutionEffect,
        mailbox_capacity: NonZeroUsize,
        required_establish_contexts: Vec<QueryContextRef>,
        max_admission_issues_per_context: NonZeroUsize,
        max_establish_authorizations_per_context: NonZeroUsize,
        work_owner: WorkOwner,
        initial_execution_stage: StagePermit,
    ) -> Result<Self, LogicalExecutionActorError> {
        Self::no_recovery_completion(
            initial_execution,
            effect,
            mailbox_capacity,
            required_establish_contexts,
            max_admission_issues_per_context,
            max_establish_authorizations_per_context,
            work_owner,
            initial_execution_stage,
        )
    }

    /// Constructs one effect-free read attempt with a bounded row stream.
    ///
    /// The effect and recovery policy are fixed by this constructor. External
    /// callers cannot attach a row stream to an effectful completion config.
    ///
    /// ```compile_fail
    /// use std::num::NonZeroUsize;
    /// use novarocks_query_application::{api::ResultSchema, coordination::LogicalExecutionActorConfig};
    /// fn attach_arbitrary_rows(config: LogicalExecutionActorConfig) {
    ///     let _ = config.with_result_stream(
    ///         ResultSchema::new(Vec::new()),
    ///         NonZeroUsize::new(1).unwrap(),
    ///     );
    /// }
    /// ```
    pub fn single_attempt_read_rows(
        initial_execution: QueryExecutionId,
        mailbox_capacity: NonZeroUsize,
        required_establish_contexts: Vec<QueryContextRef>,
        max_admission_issues_per_context: NonZeroUsize,
        max_establish_authorizations_per_context: NonZeroUsize,
        work_owner: WorkOwner,
        initial_execution_stage: StagePermit,
        schema: ResultSchema,
        delivery_capacity: NonZeroUsize,
        carrier: crate::api::ResultRowCarrier,
    ) -> Result<Self, LogicalExecutionActorError> {
        Ok(Self::single_attempt_completion(
            initial_execution,
            ExecutionEffect::None,
            mailbox_capacity,
            required_establish_contexts,
            max_admission_issues_per_context,
            max_establish_authorizations_per_context,
            work_owner,
            initial_execution_stage,
        )?
        .with_result_stream(schema, delivery_capacity)
        .with_result_row_carrier(carrier))
    }

    /// Constructs effect-free read execution recovery before any data packet
    /// becomes visible. The budget must authorize at least one successor.
    pub(crate) fn read_only_pre_visibility_recovery(
        initial_execution: QueryExecutionId,
        mailbox_capacity: NonZeroUsize,
        required_establish_contexts: Vec<QueryContextRef>,
        max_admission_issues_per_context: NonZeroUsize,
        max_establish_authorizations_per_context: NonZeroUsize,
        max_attempts: NonZeroU32,
        replacement_effect_port: Arc<dyn ReplacementQualificationEffectPort>,
        work_owner: WorkOwner,
        initial_execution_stage: StagePermit,
        replacement_reservation_valid_for: Duration,
    ) -> Result<Self, LogicalExecutionActorError> {
        if max_attempts.get() <= 1 {
            close_unstarted_governance(work_owner, initial_execution_stage);
            return Err(LogicalExecutionActorError::RecoveryRefused(
                RecoveryRefusal::AttemptBudget,
            ));
        }
        if replacement_reservation_valid_for.is_zero() {
            close_unstarted_governance(work_owner, initial_execution_stage);
            return Err(LogicalExecutionActorError::InvariantViolation);
        }
        let work = work_owner.scope();
        if initial_execution_stage
            .check(&work, Stage::Execution)
            .is_err()
        {
            close_unstarted_governance(work_owner, initial_execution_stage);
            return Err(LogicalExecutionActorError::InvariantViolation);
        }
        Ok(Self {
            initial_execution,
            recovery_mode: RecoveryMode::RestartAttemptBeforeVisibility,
            effect: ExecutionEffect::None,
            output_mode: LogicalOutputMode::ResultStream,
            max_attempts: max_attempts.get(),
            mailbox_capacity,
            required_establish_contexts,
            max_admission_issues_per_context,
            max_establish_authorizations_per_context,
            abort_effect_port: None,
            replacement_effect_port: Some(replacement_effect_port),
            replacement_reservation_valid_for: Some(replacement_reservation_valid_for),
            max_abort_authorizations_per_context: max_establish_authorizations_per_context,
            clock: Arc::new(ProcessMonotonicClock::new()),
            work_owner: Some(work_owner),
            execution_stage: Some(initial_execution_stage),
            result_schema: None,
            result_delivery_capacity: None,
            result_row_carrier: None,
            internal_budgets: crate::api::ComputationObservationBudgets::default(),
            internal_plan: None,
            native_actions: None,
        })
    }

    /// Constructs an effect-free read whose whole attempt may restart only
    /// before any result packet becomes visible.
    #[expect(
        clippy::too_many_arguments,
        reason = "this public constructor freezes each independent admission, replacement, work-owner, and row-carrier fact"
    )]
    pub fn read_only_pre_visibility_recovery_rows(
        initial_execution: QueryExecutionId,
        mailbox_capacity: NonZeroUsize,
        required_establish_contexts: Vec<QueryContextRef>,
        max_admission_issues_per_context: NonZeroUsize,
        max_establish_authorizations_per_context: NonZeroUsize,
        max_attempts: NonZeroU32,
        replacement_effect_port: Arc<dyn ReplacementQualificationEffectPort>,
        work_owner: WorkOwner,
        initial_execution_stage: StagePermit,
        replacement_reservation_valid_for: Duration,
        schema: ResultSchema,
        delivery_capacity: NonZeroUsize,
        carrier: crate::api::ResultRowCarrier,
    ) -> Result<Self, LogicalExecutionActorError> {
        Ok(Self::read_only_pre_visibility_recovery(
            initial_execution,
            mailbox_capacity,
            required_establish_contexts,
            max_admission_issues_per_context,
            max_establish_authorizations_per_context,
            max_attempts,
            replacement_effect_port,
            work_owner,
            initial_execution_stage,
            replacement_reservation_valid_for,
        )?
        .with_result_stream(schema, delivery_capacity)
        .with_result_row_carrier(carrier))
    }

    /// Attaches the fixed logical schema and bounded protocol queue used by
    /// the actor-owned result stream.
    pub(crate) fn with_result_stream(
        mut self,
        schema: ResultSchema,
        delivery_capacity: NonZeroUsize,
    ) -> Self {
        self.output_mode = LogicalOutputMode::ResultStream;
        self.result_schema = Some(schema);
        self.result_delivery_capacity = Some(delivery_capacity);
        self
    }

    /// Fixes how this execution's result rows arrive. A relayed carrier admits
    /// only Backend-encoded root items of its exact kind and row profile.
    /// Row-stream construction requires this explicit frozen contract.
    pub(crate) fn with_result_row_carrier(mut self, carrier: crate::api::ResultRowCarrier) -> Self {
        self.result_row_carrier = Some(carrier);
        self
    }

    pub(crate) fn with_internal_observation_budgets(
        mut self,
        budgets: crate::api::ComputationObservationBudgets,
    ) -> Self {
        self.internal_budgets = budgets;
        self
    }

    pub(crate) fn with_internal_computation(
        mut self,
        description: &crate::preparation::FrozenExecutionDescription,
        capacity: NonZeroUsize,
    ) -> Result<Self, LogicalExecutionActorError> {
        if description.finish_boundary()
            != crate::preparation::ExecutionFinishBoundary::ComputationReady
            || self.effect != description.effect()
            || self.recovery_mode != description.recovery()
        {
            return Err(LogicalExecutionActorError::InvariantViolation);
        }
        self.result_row_carrier = Some(
            description
                .row_carrier()
                .map_err(|_| LogicalExecutionActorError::InvariantViolation)?,
        );
        self.internal_plan = Some(description.plan_identity());
        self.result_schema = None;
        self.result_delivery_capacity = Some(capacity);
        self.output_mode = LogicalOutputMode::InternalResultStream;
        Ok(self)
    }

    /// Connects the role-composed Abort effect port. The query application
    /// retains no Native operation carrier or codec dependency.
    pub fn with_abort_query_context_effect_port(
        mut self,
        port: Arc<dyn AbortQueryContextEffectPort>,
        max_authorizations_per_context: NonZeroUsize,
    ) -> Self {
        self.abort_effect_port = Some(port);
        self.max_abort_authorizations_per_context = max_authorizations_per_context;
        self
    }

    pub(crate) fn with_clock(mut self, clock: Arc<dyn MonotonicClock>) -> Self {
        self.clock = clock;
        self
    }
}

/// Stable public failures at the actor boundary. Reducer internals remain
/// private to the query application.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum LogicalExecutionActorError {
    MailboxClosed,
    StaleAuthority,
    WrongExecution,
    WrongPhase,
    AlreadyConcluded,
    RecoveryRefused(RecoveryRefusal),
    ReplacementNotReady,
    ReplacementQualificationFailed(ReplacementQualificationFailure),
    InvariantViolation,
    Establish(EstablishIssueError),
    StandDown(ContextStandDownError),
    /// The exact root Task reached a non-success terminal state. The caller
    /// still owns the running-attempt permit and must classify this attempt
    /// for replacement or logical conclusion.
    RootAttemptTerminal,
    /// The actor already fixed the logical execution's terminal state. No
    /// attempt decision authority remains with the caller.
    ExecutionConcluded(LogicalConclusion),
    ResultDeliveryFailed,
    ResultProtocolViolation,
}

impl fmt::Display for LogicalExecutionActorError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        if let Self::Establish(error) = self {
            return write!(formatter, "Establish issue protocol failed: {error}");
        }
        if let Self::StandDown(error) = self {
            return write!(formatter, "query-context stand-down failed: {error}");
        }
        if let Self::RecoveryRefused(reason) = self {
            return write!(
                formatter,
                "logical execution recovery was refused: {reason:?}"
            );
        }
        if let Self::ReplacementQualificationFailed(reason) = self {
            return write!(
                formatter,
                "attempt replacement qualification failed: {reason:?}"
            );
        }
        formatter.write_str(match self {
            Self::MailboxClosed => "logical execution actor mailbox is closed",
            Self::StaleAuthority => "attempt authority belongs to another actor or generation",
            Self::WrongExecution => "attempt authority does not name the current execution",
            Self::WrongPhase => "logical execution is in the wrong phase",
            Self::AlreadyConcluded => "logical execution has already concluded",
            Self::ReplacementNotReady => "attempt replacement qualification is incomplete",
            Self::InvariantViolation => "logical execution actor invariant was violated",
            Self::RootAttemptTerminal => "root task reached a non-success terminal state",
            Self::ExecutionConcluded(conclusion) => {
                return write!(
                    formatter,
                    "logical execution already concluded as {conclusion:?}"
                );
            }
            Self::ResultDeliveryFailed => "logical result delivery failed",
            Self::ResultProtocolViolation => "logical result delivery protocol was violated",
            Self::Establish(_)
            | Self::StandDown(_)
            | Self::RecoveryRefused(_)
            | Self::ReplacementQualificationFailed(_) => unreachable!("handled above"),
        })
    }
}

impl std::error::Error for LogicalExecutionActorError {}

impl novarocks_execution_contract::failure::FailureProjection for LogicalExecutionActorError {
    fn failure_fact(&self) -> novarocks_execution_contract::failure::FailureFact {
        use novarocks_execution_contract::failure::{
            FailureClass, FailureFact, FailureProjection, FailureReason, domain,
        };
        let (class, code, observed_stop) = match self {
            Self::MailboxClosed => (FailureClass::Control, 50, true),
            Self::StaleAuthority => (FailureClass::Control, 51, true),
            Self::WrongExecution => (FailureClass::Invariant, 52, false),
            Self::WrongPhase => (FailureClass::Control, 53, true),
            Self::AlreadyConcluded => (FailureClass::Control, 54, true),
            Self::RecoveryRefused(_) => (FailureClass::Control, 55, false),
            Self::ReplacementNotReady => (FailureClass::Control, 56, false),
            Self::InvariantViolation => (FailureClass::Invariant, 57, false),
            Self::RootAttemptTerminal => (FailureClass::Control, 58, true),
            Self::ExecutionConcluded(_) => (FailureClass::Control, 59, true),
            Self::ResultDeliveryFailed => (FailureClass::Unexpected, 60, false),
            Self::ResultProtocolViolation => (FailureClass::Invariant, 99, false),
            Self::Establish(error) => return FailureProjection::failure_fact(error),
            Self::StandDown(error) => return FailureProjection::failure_fact(error),
            Self::ReplacementQualificationFailed(error) => {
                return FailureProjection::failure_fact(error);
            }
        };
        let reason = FailureReason::new(domain::QUERY, std::num::NonZeroU16::new(code).unwrap());
        if observed_stop {
            FailureFact::stop_observation(reason)
        } else {
            FailureFact::new(class, reason)
        }
    }
}

impl LogicalExecutionActorError {
    /// Bounded diagnostic authored here; no foreign Display becomes safe detail.
    pub fn safe_detail(&self) -> novarocks_execution_contract::SafeDetail {
        let message = match self {
            Self::MailboxClosed => "logical execution actor mailbox is closed",
            Self::StaleAuthority => "attempt authority is no longer current",
            Self::WrongExecution => "attempt authority names a different logical execution",
            Self::WrongPhase => "logical execution operation is not allowed in its current phase",
            Self::AlreadyConcluded => "logical execution has already concluded",
            Self::RecoveryRefused(_) => "logical execution recovery was refused by query policy",
            Self::ReplacementNotReady => "attempt replacement qualification is not ready",
            Self::InvariantViolation => "logical execution actor invariant was violated",
            Self::RootAttemptTerminal => "root task already reached a non-success terminal state",
            Self::ExecutionConcluded(_) => "logical execution already has a terminal conclusion",
            Self::ResultDeliveryFailed => {
                "logical result delivery failed without a more specific recorded cause"
            }
            Self::ResultProtocolViolation => {
                "logical result sequence or delivery authority violated its protocol"
            }
            Self::Establish(error) => return error.safe_detail(),
            Self::StandDown(error) => return error.safe_detail(),
            Self::ReplacementQualificationFailed(error) => return error.safe_detail(),
        };
        novarocks_execution_contract::SafeDetail::truncating(message)
    }
}

impl From<ActorStateError> for LogicalExecutionActorError {
    fn from(value: ActorStateError) -> Self {
        match value {
            ActorStateError::StaleAttemptCapability | ActorStateError::StaleEffectReceipt => {
                Self::StaleAuthority
            }
            ActorStateError::ForeignQuery
            | ActorStateError::NotCurrent
            | ActorStateError::ReusedAttempt
            | ActorStateError::NonMonotonicAttempt => Self::WrongExecution,
            ActorStateError::WrongPhase
            | ActorStateError::SuccessRequiresEndOfStream
            | ActorStateError::SuccessAlreadyAuthorized => Self::WrongPhase,
            ActorStateError::AlreadyConcluded => Self::AlreadyConcluded,
            ActorStateError::RecoveryRefused(reason) => Self::RecoveryRefused(reason),
            ActorStateError::ReplacementNotReady(_) => Self::ReplacementNotReady,
            ActorStateError::StaleReplacement
            | ActorStateError::EffectAlreadyPending
            | ActorStateError::EffectAlreadySatisfied
            | ActorStateError::EffectIdentityExhausted
            | ActorStateError::Delivery(_) => Self::InvariantViolation,
        }
    }
}

impl From<EstablishIssueError> for LogicalExecutionActorError {
    fn from(value: EstablishIssueError) -> Self {
        Self::Establish(value)
    }
}

impl From<ContextStandDownError> for LogicalExecutionActorError {
    fn from(value: ContextStandDownError) -> Self {
        Self::StandDown(value)
    }
}

/// Immutable observation of the actor-owned state. It carries no mutation
/// authority and is suitable for diagnostics and deterministic supervision.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct LogicalExecutionActorSnapshot {
    pub phase: ExecutionPhase,
    pub conclusion: Option<LogicalConclusion>,
    pub schema_emitted: bool,
    pub output_visible: bool,
    pub accepted_result_packets: u64,
    pub delivered_result_through: Option<ResultPacketSequence>,
    pub establish_error: Option<EstablishIssueError>,
    pub stand_down_error: Option<ContextStandDownError>,
    pub replacement_error: Option<ReplacementQualificationFailure>,
}

struct AttemptLedgers {
    activation: AttemptActivationIdentity,
    establish: EstablishIssueLedger,
    establish_error: Option<EstablishIssueError>,
    establish_protocol_error: Option<EstablishIssueError>,
    stand_down: ContextStandDownLedger,
    stand_down_error: Option<ContextStandDownError>,
    pending_aborts: BTreeMap<QueryContextRef, AbortQueryContextIssuePermit>,
    abort_backpressured: bool,
    attempt_obligation: Option<Obligation>,
    active_resources: Option<ActiveReplacementResources>,
}

impl AttemptLedgers {
    /// Keep the first diagnostic without letting an ordinary remote refusal
    /// hide a later protocol fault whose rejected event has no durable row.
    fn record_establish_error(&mut self, error: EstablishIssueError) {
        self.establish_error.get_or_insert(error);
        if !matches!(
            error,
            EstablishIssueError::AdmissionRejected | EstablishIssueError::EstablishRejected
        ) {
            self.establish_protocol_error.get_or_insert(error);
        }
    }

    /// Every success entrance uses the same fence. Only an exact sealed
    /// result withdrawal can remove an ordinary context requirement; protocol
    /// faults remain fatal even when their original context was withdrawn.
    fn ensure_success_ready(&mut self) -> Result<(), EstablishIssueError> {
        if let Some(error) = self.establish_protocol_error {
            return Err(error);
        }
        self.establish.ensure_success_ready()
    }
}

impl fmt::Debug for AttemptLedgers {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("AttemptLedgers")
            .field("activation", &self.activation)
            .field("has_attempt_obligation", &self.attempt_obligation.is_some())
            .field("has_active_resources", &self.active_resources.is_some())
            .finish_non_exhaustive()
    }
}

struct ReplacementRuntime {
    token: ReplacementToken,
    identity: ReplacementQualificationIdentity,
    request: Arc<ReplacementQualificationRequest>,
    effect_cancellation: watch::Sender<bool>,
    effect_dispatched: bool,
    effect_settled: bool,
    effect_backpressured: bool,
    activation: Option<PendingReplacementActivation>,
    reservation: Option<super::QualifiedReplacementReservation>,
    effect_dispatched_at: Option<MonotonicInstant>,
    conservative_expiry: Option<MonotonicInstant>,
    reservation_valid_for: Duration,
    stale_usage_accounted: bool,
    /// Only the qualifier failure accepted by this replacement's own token
    /// can decide a late activation. Later effect errors remain diagnostics.
    accepted_failure: Option<ReplacementQualificationFailure>,
}

impl fmt::Debug for ReplacementRuntime {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ReplacementRuntime")
            .field("identity", &self.identity)
            .field("effect_dispatched", &self.effect_dispatched)
            .field("effect_settled", &self.effect_settled)
            .field("has_reservation", &self.reservation.is_some())
            .finish_non_exhaustive()
    }
}

#[derive(Debug)]
struct PendingReplacementActivation {
    lifetime: Arc<PermitLifetime>,
    mailbox_liveness: mpsc::Sender<ActorCommand>,
    reply: ActorReply<AttemptInstantiationPermit>,
}

/// Cancels a replacement whose caller stopped awaiting the actor reply after
/// transferring the move-only qualification into the mailbox.
#[derive(Debug)]
struct PendingAuthorityRequest {
    lifetime: Option<Arc<PermitLifetime>>,
}

impl PendingAuthorityRequest {
    fn new(lifetime: Arc<PermitLifetime>) -> Self {
        Self {
            lifetime: Some(lifetime),
        }
    }

    fn disarm(&mut self) {
        self.lifetime.take();
    }
}

impl Drop for PendingAuthorityRequest {
    fn drop(&mut self) {
        abandon_on_drop(&mut self.lifetime);
    }
}

type ActorReply<T> = oneshot::Sender<Result<T, LogicalExecutionActorError>>;

struct PendingResultBatch {
    activation: AttemptActivationIdentity,
    sequence: ResultPacketSequence,
    /// A decoded batch or a Backend-encoded root segment; both share the
    /// same ordered delivery, receipt and success gate.
    delivery: RootSegmentDelivery,
    receipt: ResultDeliveryReceipt,
    reply: ActorReply<()>,
}

struct PendingResultEnd {
    permit: RunningAttemptPermit,
    sequence: ResultPacketSequence,
    reply: ActorReply<LogicalConclusion>,
}

struct PendingResultFinish {
    permit: RunningAttemptPermit,
    reply: ActorReply<LogicalConclusion>,
}

struct RootSuccessGate {
    activation: AttemptActivationIdentity,
    root: TaskIdentity,
    status: Option<TaskStatus>,
    terminal_failure: RootTerminalFailure,
    final_eos_ack: Option<ResultPacketSequence>,
    root_output_rows: Option<u64>,
}

struct RootTerminalObservation {
    activation: AttemptActivationIdentity,
    root: TaskIdentity,
    status: TaskStatus,
    terminal_failure: RootTerminalFailure,
    reply: ActorReply<()>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum RootTerminalFailure {
    Unspecified,
    Pending,
    Authoritative(Box<QueryExecutionError>),
}

enum PendingResult {
    Batch(PendingResultBatch),
    End(PendingResultEnd),
    InternalEnd(PendingEnd),
}

enum InFlightResult {
    Schema {
        permit: SchemaDeliveryPermit,
        receipt: ResultDeliveryReceipt,
    },
    Batch {
        permit: DeliveryPermit<()>,
        receipt: ResultDeliveryReceipt,
        reply: ActorReply<()>,
    },
    End {
        receipt: ResultDeliveryReceipt,
    },
    InternalEnd(EndInFlight),
}

// Transport disappearance cannot select a supervised consumer's domain cause.
// Move the exact outstanding delivery here so no closed reservation or settled
// receipt is polled again. Terminal control or retirement settles it once.
enum DeferredInternalDelivery {
    Pending(PendingResult),
    InFlight(InFlightResult),
    EndAckLost(ActorReply<()>),
}

impl DeferredInternalDelivery {
    fn settle(self, state: &mut LogicalExecutionState, error: Option<QueryExecutionError>) {
        match self {
            Self::Pending(pending) => reject_pending_result(state, pending),
            Self::InFlight(in_flight) => settle_failed_in_flight_result(state, in_flight, error),
            Self::EndAckLost(reply) => reply_result_failure(state, reply),
        }
    }

    fn retire(self) {
        match self {
            Self::Pending(pending) => reject_pending_result_for_retirement(pending),
            Self::InFlight(in_flight) => retire_in_flight_result(in_flight),
            Self::EndAckLost(reply) => {
                let _ = reply.send(Err(LogicalExecutionActorError::StaleAuthority));
            }
        }
    }
}

// An observed attempt failure is pending the logical recovery decision. It
// belongs to that exact activation and cannot become a successor's cause.
struct AttemptResultFailure {
    activation: AttemptActivationIdentity,
    error: QueryExecutionError,
}

struct ResultRuntime {
    carrier: crate::api::ResultRowCarrier,
    schema_receipt: Option<ResultDeliveryReceipt>,
    schema_writer_completed: bool,
    failure_sender: Option<watch::Sender<Option<QueryExecutionError>>>,
    accepted_error: Option<QueryExecutionError>,
    attempt_error: Option<AttemptResultFailure>,
    transport: ResultTransport,
    internal: Option<InternalRuntime>,
    internal_outcome: Option<
        watch::Sender<Option<Result<crate::api::ComputationReadyToken, QueryExecutionError>>>,
    >,
    root_success: Option<RootSuccessGate>,
    attempt_decision_pending: Option<AttemptActivationIdentity>,
    finish_waiter: Option<PendingResultFinish>,
    pending: Option<PendingResult>,
    in_flight: Option<InFlightResult>,
    deferred_internal: Option<DeferredInternalDelivery>,
}

enum ResultReceiptOutcome {
    Schema(Result<ResultDeliveryDisposition, ()>),
    Delivery(Result<ResultDeliveryDisposition, ()>),
    SourceExited(ConsumerExit),
}

impl ResultRuntime {
    fn internal_exit_supervised(&self) -> bool {
        self.internal
            .as_ref()
            .is_some_and(InternalRuntime::exit_supervised)
    }

    fn retire_attempt_failure(&mut self, activation: AttemptActivationIdentity) {
        if self
            .attempt_error
            .as_ref()
            .is_some_and(|failure| failure.activation == activation)
        {
            self.attempt_error.take();
        }
    }

    fn idle(&self) -> bool {
        self.finish_waiter.is_none()
            && self.pending.is_none()
            && self.in_flight.is_none()
            && self.deferred_internal.is_none()
            && self.internal.as_ref().is_none_or(InternalRuntime::idle)
    }
}

enum ActorCommand {
    BindInternalConsumer(BindConsumer),
    SuperviseInternalConsumer {
        source: Arc<InternalConsumerSeal>,
        receiver: oneshot::Receiver<Result<(), QueryExecutionError>>,
        reply: ActorReply<()>,
    },
    DeliverInternalEnd(EndRequest),
    AcceptComputationReady(ReadyProposal),
    FailInternalComputation {
        source: Arc<InternalConsumerSeal>,
        error: QueryExecutionError,
        reply: ActorReply<()>,
    },
    TakeReplacementAdmissionEvidence {
        activation: AttemptActivationIdentity,
        reply: ActorReply<Option<Box<[ReplacementWorkerAdmissionEvidence]>>>,
    },
    Activate {
        readiness: AttemptReadinessReceipt,
        reply: ActorReply<RunningAttemptPermit>,
    },
    InitializationFailed {
        permit: AttemptInstantiationPermit,
        error: Option<QueryExecutionError>,
        reply: ActorReply<LogicalConclusion>,
    },
    InitializationCancelled {
        permit: AttemptInstantiationPermit,
        reply: ActorReply<LogicalConclusion>,
    },
    Completed {
        permit: RunningAttemptPermit,
        reply: ActorReply<LogicalConclusion>,
    },
    Failed {
        permit: RunningAttemptPermit,
        error: Option<QueryExecutionError>,
        reply: ActorReply<LogicalConclusion>,
    },
    DeliverRootSegment {
        activation: AttemptActivationIdentity,
        sequence: ResultPacketSequence,
        segment: Arc<RetainedRootReply>,
        client_rows: Option<(ClientRowProfile, ClientRowStreamCursor)>,
        rows: u64,
        resident_window: crate::api::RootRelayResidentWindow,
        reply: ActorReply<()>,
    },
    BindRootResult {
        activation: AttemptActivationIdentity,
        root: TaskIdentity,
        terminal_receiver: mpsc::Receiver<RootTerminalObservation>,
        reply: ActorReply<()>,
    },
    FreezeResultAttemptFailure {
        activation: AttemptActivationIdentity,
        error: QueryExecutionError,
        reply: ActorReply<()>,
    },
    ObserveRootStatus {
        activation: AttemptActivationIdentity,
        root: TaskIdentity,
        status: TaskStatus,
        reply: ActorReply<()>,
    },
    ObserveRootEosAck {
        activation: AttemptActivationIdentity,
        root: TaskIdentity,
        expected_root: TaskIdentity,
        sequence: ResultPacketSequence,
        root_output_rows: Option<u64>,
        reply: ActorReply<()>,
    },
    FinishResultStream {
        permit: RunningAttemptPermit,
        withdrawals: Vec<ResultContextWithdrawal>,
        reply: ActorReply<LogicalConclusion>,
    },
    AwaitWorkCancellation {
        permit: RunningAttemptPermit,
        reply: ActorReply<LogicalConclusion>,
    },
    BeginReplacement {
        permit: RunningAttemptPermit,
        failure: AttemptFailureClass,
        pre_ready_replacement: Option<crate::api::PreReadyProcessReplacementWitness>,
        terminal_error: Option<QueryExecutionError>,
        replacement: QueryExecutionId,
        replacement_contexts: Vec<QueryContextRef>,
        reply: ActorReply<ReplacementQualification>,
    },
    ActivateReplacement {
        qualification: ReplacementQualification,
        reply: ActorReply<AttemptInstantiationPermit>,
    },
    MayObservePreReadyReplacement {
        activation: AttemptActivationIdentity,
        refusal: crate::execution_error::ExecutionFailureInput,
        reply: ActorReply<bool>,
    },
    BeginAdmissionIssue {
        activation: AttemptActivationIdentity,
        request: AcquireQueryContextAdmissionTicket,
        reply: ActorReply<AdmissionIssueReceipt>,
    },
    SettleAdmissionIssue {
        activation: AttemptActivationIdentity,
        admission_issue: AdmissionIssueReceipt,
        settlement: AdmissionIssueSettlement,
        reply: ActorReply<AdmissionIssueDisposition>,
    },
    AuthorizeEstablish {
        activation: AttemptActivationIdentity,
        request: Arc<EstablishQueryContext>,
        native_compatibility_id: NativeCompatibilityId,
        reply: ActorReply<EstablishIssuePermit>,
    },
    ReauthorizeEstablish {
        activation: AttemptActivationIdentity,
        context: QueryContextRef,
        reply: ActorReply<EstablishIssuePermit>,
    },
    EstablishSnapshot {
        context: QueryContextRef,
        reply: ActorReply<Option<EstablishIssueSnapshot>>,
    },
    StandDownSnapshot {
        context: QueryContextRef,
        reply: ActorReply<Option<ContextStandDownSnapshot>>,
    },
    RegistryContextConverged {
        context: QueryContextRef,
        convergence: RegistryContextConvergence,
        reply: ActorReply<()>,
    },
    RegistryJoinReadiness {
        close_ingress: bool,
        reply: ActorReply<bool>,
    },
    Snapshot {
        reply: ActorReply<LogicalExecutionActorSnapshot>,
    },
}

/// Why the Registry closes an actor's ingress.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum RegistryClose {
    /// The supervisor retires an actor after its logical conclusion. The
    /// conclusion is the client-visible outcome, so this close never cancels
    /// work that has concluded; it only lets the actor finish delivering and
    /// exit.
    Retire,
    /// The process is shutting down: work still in flight is cancelled.
    Shutdown,
}

/// Cloneable bounded mailbox handle. Authority-bearing commands wait
/// asynchronously for capacity; abandoning their future drops the exact
/// permit and wakes the actor through a separate coalesced signal.
#[derive(Clone, Debug)]
pub struct LogicalExecutionActor {
    id: LogicalExecutionActorId,
    sender: mpsc::Sender<ActorCommand>,
    registry_close_requested: watch::Sender<Option<RegistryClose>>,
    registry_ingress_closed: watch::Receiver<bool>,
    internal_outcome: Option<
        watch::Receiver<Option<Result<crate::api::ComputationReadyToken, QueryExecutionError>>>,
    >,
}

impl LogicalExecutionActor {
    /// Observes the execution-wide internal result selected by this actor.
    /// Attempt retirement leaves this observer open and undecided.
    pub(crate) fn internal_outcome(
        &self,
    ) -> Option<
        watch::Receiver<Option<Result<crate::api::ComputationReadyToken, QueryExecutionError>>>,
    > {
        self.internal_outcome.clone()
    }
    pub const fn id(&self) -> LogicalExecutionActorId {
        self.id
    }

    pub(crate) async fn observe_registry_context_convergence(
        &self,
        context: QueryContextRef,
        convergence: RegistryContextConvergence,
    ) -> Result<(), LogicalExecutionActorError> {
        request(&self.sender, |reply| {
            ActorCommand::RegistryContextConverged {
                context,
                convergence,
                reply,
            }
        })
        .await
    }

    pub(crate) async fn registry_join_readiness(
        &self,
        close_ingress: bool,
    ) -> Result<bool, LogicalExecutionActorError> {
        request(&self.sender, |reply| ActorCommand::RegistryJoinReadiness {
            close_ingress,
            reply,
        })
        .await
    }

    pub(crate) fn request_registry_close_for_join(
        &self,
        cause: RegistryClose,
    ) -> watch::Receiver<bool> {
        // This signal is independent of ordinary mailbox capacity. Once set,
        // dropping the returned waiter never retracts cancellation or ingress
        // closure, and a shutdown is never downgraded to a retirement.
        let closed = self.registry_ingress_closed.clone();
        if *closed.borrow() {
            return closed;
        }
        self.registry_close_requested.send_if_modified(|requested| {
            if *requested == Some(RegistryClose::Shutdown) || *requested == Some(cause) {
                return false;
            }
            *requested = Some(cause);
            true
        });
        closed
    }

    #[cfg(test)]
    pub(crate) fn registry_ingress_closed(&self) -> watch::Receiver<bool> {
        self.registry_ingress_closed.clone()
    }

    #[cfg(test)]
    pub(crate) fn registry_close_was_requested(&self) -> bool {
        self.registry_close_requested.borrow().is_some()
    }

    pub async fn activate(
        &self,
        readiness: AttemptReadinessReceipt,
    ) -> Result<RunningAttemptPermit, LogicalExecutionActorError> {
        request(&self.sender, |reply| ActorCommand::Activate {
            readiness,
            reply,
        })
        .await
    }

    pub async fn initialization_failed(
        &self,
        permit: AttemptInstantiationPermit,
    ) -> Result<LogicalConclusion, LogicalExecutionActorError> {
        request(&self.sender, |reply| ActorCommand::InitializationFailed {
            permit,
            error: None,
            reply,
        })
        .await
    }

    pub(crate) async fn initialization_failed_with_error(
        &self,
        permit: AttemptInstantiationPermit,
        error: QueryExecutionError,
    ) -> Result<LogicalConclusion, LogicalExecutionActorError> {
        request(&self.sender, |reply| ActorCommand::InitializationFailed {
            permit,
            error: Some(error),
            reply,
        })
        .await
    }

    pub async fn cancel_initialization(
        &self,
        permit: AttemptInstantiationPermit,
    ) -> Result<LogicalConclusion, LogicalExecutionActorError> {
        request(&self.sender, |reply| {
            ActorCommand::InitializationCancelled { permit, reply }
        })
        .await
    }

    pub async fn complete_attempt(
        &self,
        permit: RunningAttemptPermit,
    ) -> Result<LogicalConclusion, LogicalExecutionActorError> {
        request(&self.sender, |reply| ActorCommand::Completed {
            permit,
            reply,
        })
        .await
    }

    pub async fn fail_attempt(
        &self,
        permit: RunningAttemptPermit,
    ) -> Result<LogicalConclusion, LogicalExecutionActorError> {
        request(&self.sender, |reply| ActorCommand::Failed {
            permit,
            error: None,
            reply,
        })
        .await
    }

    /// Fixes a final logical failure while preserving its public error.
    /// Recoverable attempt failures use `begin_replacement` and must not
    /// publish a logical failure before that decision.
    pub async fn fail_attempt_with_error(
        &self,
        permit: RunningAttemptPermit,
        error: QueryExecutionError,
    ) -> Result<LogicalConclusion, LogicalExecutionActorError> {
        request(&self.sender, |reply| ActorCommand::Failed {
            permit,
            error: Some(error),
            reply,
        })
        .await
    }

    /// Consumes the current attempt authority and begins exact successor
    /// qualification. The old attempt becomes residual before any successor
    /// capacity can be activated.
    pub async fn begin_replacement(
        &self,
        permit: RunningAttemptPermit,
        failure: AttemptFailureClass,
        replacement: QueryExecutionId,
        replacement_contexts: Vec<QueryContextRef>,
    ) -> Result<ReplacementQualification, LogicalExecutionActorError> {
        request(&self.sender, |reply| ActorCommand::BeginReplacement {
            permit,
            failure,
            pre_ready_replacement: None,
            terminal_error: None,
            replacement,
            replacement_contexts,
            reply,
        })
        .await
    }

    pub(crate) async fn begin_replacement_with_error(
        &self,
        permit: RunningAttemptPermit,
        failure: AttemptFailureClass,
        terminal_error: QueryExecutionError,
        pre_ready_replacement: Option<crate::api::PreReadyProcessReplacementWitness>,
        replacement: QueryExecutionId,
        replacement_contexts: Vec<QueryContextRef>,
    ) -> Result<ReplacementQualification, LogicalExecutionActorError> {
        request(&self.sender, |reply| ActorCommand::BeginReplacement {
            permit,
            failure,
            pre_ready_replacement,
            terminal_error: Some(terminal_error),
            replacement,
            replacement_contexts,
            reply,
        })
        .await
    }

    /// Activates a fully qualified successor and returns its exact
    /// instantiation authority. The combined qualification owner proves the
    /// replacement gate; the old attempt's stand-down ledger remains an
    /// independently supervised residual after activation.
    pub async fn activate_replacement(
        &self,
        qualification: ReplacementQualification,
    ) -> Result<AttemptInstantiationPermit, LogicalExecutionActorError> {
        let lifetime = qualification
            .lifetime
            .as_ref()
            .cloned()
            .ok_or(LogicalExecutionActorError::StaleAuthority)?;
        let mut pending_request = PendingAuthorityRequest::new(lifetime);
        let (reply, response) = oneshot::channel();
        self.sender
            .send(ActorCommand::ActivateReplacement {
                qualification,
                reply,
            })
            .await
            .map_err(|_| LogicalExecutionActorError::MailboxClosed)?;
        let result = response
            .await
            .map_err(|_| LogicalExecutionActorError::MailboxClosed)?;
        pending_request.disarm();
        result
    }

    pub async fn snapshot(
        &self,
    ) -> Result<LogicalExecutionActorSnapshot, LogicalExecutionActorError> {
        request(&self.sender, |reply| ActorCommand::Snapshot { reply }).await
    }

    /// Delivers an exact admission receipt that completed after the logical
    /// conclusion was fixed. It can only settle an operation previously
    /// issued by this actor; it cannot create fresh admission authority.
    pub async fn settle_late_admission_issue(
        &self,
        activation: AttemptActivationIdentity,
        admission_issue: AdmissionIssueReceipt,
        settlement: AdmissionIssueSettlement,
    ) -> Result<AdmissionIssueDisposition, LogicalExecutionActorError> {
        request(&self.sender, |reply| ActorCommand::SettleAdmissionIssue {
            activation,
            admission_issue,
            settlement,
            reply,
        })
        .await
    }

    pub async fn establish_snapshot(
        &self,
        context: QueryContextRef,
    ) -> Result<Option<EstablishIssueSnapshot>, LogicalExecutionActorError> {
        request(&self.sender, |reply| ActorCommand::EstablishSnapshot {
            context,
            reply,
        })
        .await
    }

    pub async fn stand_down_snapshot(
        &self,
        context: QueryContextRef,
    ) -> Result<Option<ContextStandDownSnapshot>, LogicalExecutionActorError> {
        request(&self.sender, |reply| ActorCommand::StandDownSnapshot {
            context,
            reply,
        })
        .await
    }
}

async fn request<T>(
    sender: &mpsc::Sender<ActorCommand>,
    command: impl FnOnce(ActorReply<T>) -> ActorCommand,
) -> Result<T, LogicalExecutionActorError> {
    let (reply, response) = oneshot::channel();
    sender
        .send(command(reply))
        .await
        .map_err(|_| LogicalExecutionActorError::MailboxClosed)?;
    response
        .await
        .map_err(|_| LogicalExecutionActorError::MailboxClosed)?
}

/// Indivisible owner of an actor mailbox and its in-memory task.
///
/// Dropping the owner detaches the Tokio task instead of aborting it. Every
/// live attempt permit retains one private mailbox sender, so the actor remains
/// alive long enough to observe permit abandonment and conclude the logical
/// execution. The production registry must retain this owner until exact
/// attempt retirement is connected.
#[must_use = "the logical execution actor owner must remain supervised"]
pub struct LogicalExecutionActorOwner {
    actor: LogicalExecutionActor,
    join: JoinHandle<()>,
}

impl fmt::Debug for LogicalExecutionActorOwner {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("LogicalExecutionActorOwner")
            .field("actor", &self.actor)
            .field("join_finished", &self.join.is_finished())
            .finish()
    }
}

impl LogicalExecutionActorOwner {
    pub const fn actor(&self) -> &LogicalExecutionActor {
        &self.actor
    }

    pub fn is_finished(&self) -> bool {
        self.join.is_finished()
    }

    /// Transfers ownership of an unfinished cancellation tail to an explicit
    /// residual supervisor without detaching its join handle.
    pub fn into_residual_stand_down_supervisor(self) -> ResidualStandDownSupervisor {
        let Self { actor, join } = self;
        ResidualStandDownSupervisor { actor, join }
    }

    #[cfg(test)]
    fn into_test_parts(self) -> (LogicalExecutionActor, JoinHandle<()>) {
        let Self { actor, join } = self;
        (actor, join)
    }

    pub(crate) fn into_registry_parts(self) -> (LogicalExecutionActor, JoinHandle<()>) {
        let Self { actor, join } = self;
        (actor, join)
    }
}

/// Move-only transfer of the logical execution's single output consumer.
///
/// Output belongs to the logical execution spawn result rather than to an
/// attempt or runtime supervisor. Consuming this transfer is the only way to
/// obtain that output.
///
/// ```compile_fail
/// use novarocks_query_application::coordination::LogicalExecutionOutputTransfer;
/// fn consume_twice(transfer: LogicalExecutionOutputTransfer) {
///     let _first = transfer.into_output();
///     let _second = transfer.into_output();
/// }
/// ```
#[must_use = "the logical execution output must be handed to its application consumer"]
pub(crate) struct LogicalExecutionOutputTransfer {
    output: Option<ExecutionOutput>,
}

impl fmt::Debug for LogicalExecutionOutputTransfer {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("LogicalExecutionOutputTransfer")
            .finish_non_exhaustive()
    }
}

impl LogicalExecutionOutputTransfer {
    pub(crate) fn into_output(self) -> ExecutionOutput {
        self.output
            .expect("client output transfer requires a client execution")
    }
}

/// Indivisible result of creating one logical execution runtime.
///
/// The private fields prevent callers from minting a runtime owner, initial
/// attempt authority, or output transfer independently. The query application
/// may split this bundle only while atomically installing all three owners in
/// its runtime registry.
///
/// ```compile_fail
/// use novarocks_query_application::coordination::SpawnedLogicalExecution;
/// fn split_before_registry_install(spawned: SpawnedLogicalExecution) {
///     let _ = spawned.into_parts();
/// }
/// ```
///
/// ```compile_fail
/// use novarocks_query_application::coordination::AttemptInstantiationPermit;
/// fn attempt_cannot_take_output(attempt: AttemptInstantiationPermit) {
///     let _ = attempt.into_output();
/// }
/// ```
///
/// ```compile_fail
/// use novarocks_query_application::coordination::ResidualStandDownSupervisor;
/// fn residual_cannot_take_output(residual: ResidualStandDownSupervisor) {
///     let _ = residual.into_output();
/// }
/// ```
///
/// ```compile_fail
/// use novarocks_query_application::coordination::LogicalExecutionActorOwner;
/// fn runtime_owner_cannot_take_output(owner: LogicalExecutionActorOwner) {
///     let _ = owner.take_output();
/// }
/// ```
#[must_use = "the spawned logical execution must remain supervised"]
pub(crate) struct SpawnedLogicalExecution {
    runtime_owner: LogicalExecutionActorOwner,
    initial_attempt: AttemptInstantiationPermit,
    output: LogicalExecutionOutputTransfer,
}

impl fmt::Debug for SpawnedLogicalExecution {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("SpawnedLogicalExecution")
            .field("runtime_owner", &self.runtime_owner)
            .field("initial_attempt", &self.initial_attempt.identity())
            .field("output", &self.output)
            .finish()
    }
}

impl SpawnedLogicalExecution {
    pub(crate) fn into_parts(
        self,
    ) -> (
        LogicalExecutionActorOwner,
        AttemptInstantiationPermit,
        LogicalExecutionOutputTransfer,
    ) {
        let Self {
            runtime_owner,
            initial_attempt,
            output,
        } = self;
        (runtime_owner, initial_attempt, output)
    }
}

#[derive(Debug)]
#[must_use = "residual query-context stand-down must remain supervised"]
pub struct ResidualStandDownSupervisor {
    actor: LogicalExecutionActor,
    join: JoinHandle<()>,
}

impl ResidualStandDownSupervisor {
    pub const fn actor(&self) -> &LogicalExecutionActor {
        &self.actor
    }

    pub fn is_finished(&self) -> bool {
        self.join.is_finished()
    }

    /// Applies the Registry's composite proof that the exact Worker stopped,
    /// normal context admission was fenced, and its old admission authority
    /// was revoked. Holding this move-only supervisor is the source authority;
    /// ordinary actor handles cannot submit convergence evidence.
    pub async fn observe_worker_stopped_and_context_fenced(
        &self,
        context: QueryContextRef,
    ) -> Result<(), LogicalExecutionActorError> {
        request(&self.actor.sender, |reply| {
            ActorCommand::RegistryContextConverged {
                context,
                convergence: RegistryContextConvergence::WorkerStoppedAndContextFenced,
                reply,
            }
        })
        .await
    }

    /// Applies exact process-replacement evidence for the Worker process
    /// embedded in this context identity. It fences residual Abort
    /// responsibility without claiming clean resource release.
    pub async fn observe_worker_process_replaced(
        &self,
        context: QueryContextRef,
    ) -> Result<(), LogicalExecutionActorError> {
        request(&self.actor.sender, |reply| {
            ActorCommand::RegistryContextConverged {
                context,
                convergence: RegistryContextConvergence::WorkerProcessReplaced,
                reply,
            }
        })
        .await
    }

    pub async fn join(self) -> Result<(), tokio::task::JoinError> {
        let Self { actor, join } = self;
        drop(actor);
        join.await
    }
}

/// Starts one event-driven logical execution actor on an explicit process
/// runtime and mints the initial exact instantiation authority.
pub(crate) fn spawn_logical_execution_actor(
    runtime: &Handle,
    mut config: LogicalExecutionActorConfig,
) -> Result<SpawnedLogicalExecution, LogicalExecutionActorError> {
    if matches!(config.output_mode, LogicalOutputMode::ResultStream)
        && !matches!(config.effect, ExecutionEffect::None)
    {
        return Err(LogicalExecutionActorError::InvariantViolation);
    }
    let mut state = LogicalExecutionState::new(
        config.initial_execution,
        config.recovery_mode,
        config.effect,
        config.output_mode,
        config.max_attempts,
    )?;
    let capability = state.attempt_capability(config.initial_execution)?;
    let capability_identity = identity_of(&capability);
    let actor_id = LogicalExecutionActorId::from_raw(capability.actor_instance_id());
    let (abandoned, abandoned_rx) = watch::channel(false);
    let lifetime = Arc::new(PermitLifetime {
        abandoned,
        settled: AtomicBool::new(false),
    });
    let (sender, receiver) = mpsc::channel(config.mailbox_capacity.get());
    let (registry_close_requested, registry_close_requested_rx) = watch::channel(None);
    let (registry_ingress_closed, registry_ingress_closed_rx) = watch::channel(false);
    let required_contexts: BTreeSet<_> =
        config.required_establish_contexts.iter().copied().collect();
    if required_contexts.len() != config.required_establish_contexts.len() {
        return Err(LogicalExecutionActorError::InvariantViolation);
    }
    if required_contexts
        .iter()
        .any(|context| context.query_execution_id() != config.initial_execution)
    {
        return Err(LogicalExecutionActorError::WrongExecution);
    }
    if !required_contexts.is_empty() && config.abort_effect_port.is_none() {
        return Err(LogicalExecutionActorError::InvariantViolation);
    }
    let establish = EstablishIssueLedger::new(
        required_contexts.clone(),
        config.max_admission_issues_per_context,
        config.max_establish_authorizations_per_context,
    );
    let stand_down = ContextStandDownLedger::new(
        required_contexts,
        config.max_abort_authorizations_per_context,
    )?;
    let abort_capacity = config
        .abort_effect_port
        .as_ref()
        .map(|port| port.subscribe_capacity());
    let replacement_capacity = config
        .replacement_effect_port
        .as_ref()
        .map(|port| port.subscribe_capacity());
    let (replacement_receipts, replacement_receipt_rx) = receipt_channel();
    let work_cancellation = config
        .work_owner
        .as_ref()
        .ok_or(LogicalExecutionActorError::InvariantViolation)?
        .scope()
        .cancellation()
        .map_err(|_| LogicalExecutionActorError::InvariantViolation)?;
    let execution_stage = config
        .execution_stage
        .take()
        .ok_or(LogicalExecutionActorError::InvariantViolation)?;
    let (internal_outcome_sender, internal_outcome) =
        if config.output_mode == LogicalOutputMode::InternalResultStream {
            let (sender, receiver) = watch::channel(None);
            (Some(sender), Some(receiver))
        } else {
            (None, None)
        };
    let (result_runtime, output) = match config.output_mode {
        LogicalOutputMode::CompletionOnly => (None, Some(ExecutionOutput::Completion)),
        LogicalOutputMode::ResultStream => {
            let schema = config
                .result_schema
                .take()
                .ok_or(LogicalExecutionActorError::InvariantViolation)?;
            let capacity = config
                .result_delivery_capacity
                .ok_or(LogicalExecutionActorError::InvariantViolation)?;
            let carrier = config
                .result_row_carrier
                .ok_or(LogicalExecutionActorError::InvariantViolation)?;
            let (transport, schema_receipt, failure_sender, stream) =
                QueryResultStream::try_channel(
                    config.initial_execution.query_id(),
                    schema,
                    carrier,
                    capacity.get(),
                )
                .map_err(|_| LogicalExecutionActorError::InvariantViolation)?;
            (
                Some(ResultRuntime {
                    carrier,
                    schema_receipt: Some(schema_receipt),
                    schema_writer_completed: false,
                    failure_sender: Some(failure_sender),
                    accepted_error: None,
                    attempt_error: None,
                    transport: ResultTransport::Client(transport),
                    internal: None,
                    internal_outcome: None,
                    root_success: None,
                    attempt_decision_pending: None,
                    finish_waiter: None,
                    pending: None,
                    in_flight: None,
                    deferred_internal: None,
                }),
                Some(ExecutionOutput::Rows(stream)),
            )
        }
        LogicalOutputMode::InternalResultStream => {
            let plan = config
                .internal_plan
                .ok_or(LogicalExecutionActorError::InvariantViolation)?;
            let capacity = config
                .result_delivery_capacity
                .ok_or(LogicalExecutionActorError::InvariantViolation)?;
            let carrier = config
                .result_row_carrier
                .ok_or(LogicalExecutionActorError::InvariantViolation)?;
            (
                Some(ResultRuntime {
                    carrier,
                    schema_receipt: None,
                    schema_writer_completed: true,
                    failure_sender: None,
                    accepted_error: None,
                    attempt_error: None,
                    transport: ResultTransport::UnboundInternal,
                    internal: Some(InternalRuntime::new(
                        plan,
                        capacity,
                        config.internal_budgets,
                    )),
                    internal_outcome: internal_outcome_sender,
                    root_success: None,
                    attempt_decision_pending: None,
                    finish_waiter: None,
                    pending: None,
                    in_flight: None,
                    deferred_internal: None,
                }),
                None,
            )
        }
    };
    let work_owner = config
        .work_owner
        .take()
        .ok_or(LogicalExecutionActorError::InvariantViolation)?;
    let attempt_obligation = match work_owner.scope().register_obligation(
        attempt_obligation_key(actor_id, config.initial_execution),
        ObligationKind::RunningWork,
    ) {
        Ok(obligation) => obligation,
        Err(_) => {
            close_unstarted_governance(work_owner, execution_stage);
            return Err(LogicalExecutionActorError::InvariantViolation);
        }
    };
    let attempts = BTreeMap::from([(
        config.initial_execution,
        AttemptLedgers {
            activation: capability_identity,
            establish,
            establish_error: None,
            establish_protocol_error: None,
            stand_down,
            stand_down_error: None,
            pending_aborts: BTreeMap::new(),
            abort_backpressured: false,
            attempt_obligation: Some(attempt_obligation),
            active_resources: None,
        },
    )]);
    let clock = Arc::clone(&config.clock);
    let actor_lifetime = Arc::clone(&lifetime);
    let abort_effect_port = config.abort_effect_port.take();
    let replacement_effect_port = config.replacement_effect_port.take();
    let join = runtime.spawn(async move {
        run_actor(
            &mut state,
            receiver,
            abandoned_rx,
            actor_lifetime,
            attempts,
            abort_effect_port,
            abort_capacity,
            replacement_effect_port,
            work_owner,
            execution_stage,
            config.replacement_reservation_valid_for,
            replacement_capacity,
            replacement_receipts,
            replacement_receipt_rx,
            work_cancellation,
            config.max_admission_issues_per_context,
            config.max_establish_authorizations_per_context,
            config.max_abort_authorizations_per_context,
            result_runtime,
            config.native_actions.take(),
            clock,
            registry_close_requested_rx,
            registry_ingress_closed,
        )
        .await;
    });
    Ok(SpawnedLogicalExecution {
        runtime_owner: LogicalExecutionActorOwner {
            actor: LogicalExecutionActor {
                id: actor_id,
                sender: sender.clone(),
                registry_close_requested,
                registry_ingress_closed: registry_ingress_closed_rx,
                internal_outcome,
            },
            join,
        },
        initial_attempt: AttemptInstantiationPermit {
            capability: Some(capability),
            lifetime: Some(lifetime),
            mailbox_liveness: Some(sender),
        },
        output: LogicalExecutionOutputTransfer { output },
    })
}

#[expect(
    clippy::too_many_arguments,
    reason = "the actor loop receives independently owned mailboxes, permits, effect ports, and ledger state from one spawn boundary"
)]
async fn run_actor(
    state: &mut LogicalExecutionState,
    mut receiver: mpsc::Receiver<ActorCommand>,
    mut abandoned: watch::Receiver<bool>,
    lifetime: Arc<PermitLifetime>,
    mut attempts: BTreeMap<QueryExecutionId, AttemptLedgers>,
    abort_effect_port: Option<Arc<dyn AbortQueryContextEffectPort>>,
    mut abort_capacity: Option<watch::Receiver<u64>>,
    replacement_effect_port: Option<Arc<dyn ReplacementQualificationEffectPort>>,
    work_owner: WorkOwner,
    execution_stage: StagePermit,
    replacement_reservation_valid_for: Option<Duration>,
    mut replacement_capacity: Option<watch::Receiver<u64>>,
    replacement_receipts: mpsc::UnboundedSender<ReplacementQualificationEffectReceipt>,
    mut replacement_receipt_rx: mpsc::UnboundedReceiver<ReplacementQualificationEffectReceipt>,
    work_cancellation: CancellationView,
    max_admission_issues_per_context: NonZeroUsize,
    max_establish_authorizations_per_context: NonZeroUsize,
    max_abort_authorizations_per_context: NonZeroUsize,
    mut result_runtime: Option<ResultRuntime>,
    native_actions: Option<novarocks_workload_control::joint_admission::NativeAsyncActionAuthority>,
    clock: Arc<dyn MonotonicClock>,
    mut registry_close_requested_rx: watch::Receiver<Option<RegistryClose>>,
    registry_ingress_closed: watch::Sender<bool>,
) {
    let mut establish_error = None;
    let mut stand_down_error = None;
    let mut replacement_error = None;
    let mut replacement = None;
    let mut execution_stage = Some(execution_stage);
    let mut receiver_open = true;
    let mut registry_close_requested = false;
    let mut registry_close_cancelled = false;
    let mut registry_close_signal_open = true;
    let mut root_terminal_receiver: Option<mpsc::Receiver<RootTerminalObservation>> = None;
    loop {
        let now = clock.now();
        if state.conclusion().is_none()
            && let Some(reason) = work_cancellation.reason()
        {
            revoke_all_establish_authority(&mut attempts);
            conclude_for_work_cancellation(
                state,
                replacement.as_ref(),
                result_runtime.as_mut(),
                &reason,
            );
            lifetime.settle();
        }
        let requested_close = *registry_close_requested_rx.borrow();
        observe_registry_close(
            requested_close,
            state.conclusion().is_some(),
            &mut registry_close_requested,
            &mut registry_close_cancelled,
            &work_owner,
        );
        expire_replacement_reservation(state, replacement.as_mut(), &mut replacement_error, now);
        if *abandoned.borrow() {
            revoke_all_establish_authority(&mut attempts);
            conclude_or_interrupt_success(
                state,
                replacement.as_ref(),
                result_runtime.as_mut(),
                LogicalConclusion::Cancelled,
            );
            lifetime.settle();
        }
        release_concluded_actor_resources(state, &mut execution_stage, replacement.as_mut());
        synchronize_all_stand_down(
            state,
            &mut attempts,
            &mut stand_down_error,
            now,
            abort_effect_port.as_deref(),
        );
        if state.conclusion().is_some()
            && let Some(replacement) = replacement.as_mut()
            && !replacement.effect_dispatched
        {
            replacement.effect_backpressured = false;
        }
        if state.conclusion().is_none() {
            drive_replacement_effect(
                replacement.as_mut(),
                replacement_effect_port.as_deref(),
                &replacement_receipts,
                now,
            );
        }
        try_activate_qualified_replacement(
            state,
            &mut attempts,
            &mut replacement,
            &work_owner.scope(),
            max_admission_issues_per_context,
            max_establish_authorizations_per_context,
            max_abort_authorizations_per_context,
            clock.as_ref(),
            result_runtime.as_mut(),
        );
        // A terminal observation already enqueued on the reserved ingress
        // precedes this turn's computation cut, even when the proof arrived
        // through the normal command mailbox.
        if result_runtime
            .as_ref()
            .is_some_and(|runtime| runtime.internal.is_some())
            && let Some(receiver) = root_terminal_receiver.as_mut()
        {
            let queued = receiver.len();
            for _ in 0..queued {
                if let Ok(observation) = receiver.try_recv() {
                    apply_root_terminal_observation(
                        state,
                        &mut attempts,
                        result_runtime.as_mut(),
                        observation,
                    );
                }
            }
        }
        // Registry close can record cancellation in this same actor turn.
        // Reduce it before taking the computation cut, rather than waiting
        // for another mailbox wakeup.
        if state.conclusion().is_none()
            && let Some(reason) = work_cancellation.reason()
        {
            revoke_all_establish_authority(&mut attempts);
            conclude_for_work_cancellation(
                state,
                replacement.as_ref(),
                result_runtime.as_mut(),
                &reason,
            );
            lifetime.settle();
        }
        if let Some(error) = accepted_replacement_error(state, replacement.as_ref())
            && let Some(runtime) = result_runtime.as_mut()
            && runtime.internal.is_some()
        {
            runtime.accepted_error = Some(error);
        }
        internal_ready::try_accept(state, &mut attempts, result_runtime.as_mut(), clock.now());
        release_concluded_actor_resources(state, &mut execution_stage, replacement.as_mut());
        let committed_end_awaiting_transport = state.conclusion()
            == Some(LogicalConclusion::Succeeded)
            && result_runtime.as_ref().is_some_and(|runtime| {
                matches!(runtime.in_flight, Some(InFlightResult::End { .. }))
            });
        let finish_establish_ready =
            settle_finish_establish_readiness(state, &mut attempts, result_runtime.as_mut());
        if state.conclusion().is_some() && !committed_end_awaiting_transport {
            fail_result_runtime(state, result_runtime.as_mut());
        } else if let Err(error) =
            drive_root_success(state, result_runtime.as_mut(), finish_establish_ready)
        {
            let runtime = result_runtime
                .as_mut()
                .expect("a failed root success drive has its result owner");
            if runtime.internal.is_some() {
                revoke_all_establish_authority(&mut attempts);
            }
            conclude_result_failure(
                state,
                runtime,
                "the root attempt could not be driven to success",
                result_actor_failure(error),
            );
            fail_result_runtime(state, Some(runtime));
        }
        if state.conclusion().is_some() {
            // Close the original transport-issue fences before publishing a
            // terminal outcome that authorizes independent local shutdown.
            revoke_all_establish_authority(&mut attempts);
        }
        internal_ready::publish_outcome(state, result_runtime.as_ref());
        if registry_close_requested
            && actor_cleanup_complete(state, &attempts, replacement.as_ref())
            && result_runtime.as_ref().is_none_or(ResultRuntime::idle)
        {
            receiver.close();
            receiver_open = false;
            registry_ingress_closed.send_replace(true);
        }
        if !receiver_open
            && actor_cleanup_complete(state, &attempts, replacement.as_ref())
            && result_runtime.as_ref().is_none_or(ResultRuntime::idle)
        {
            work_owner.complete_after_terminal_cancel_settled();
            return;
        }
        let abort_retry_at = next_abort_retry_at(&attempts);
        let abort_backpressured = attempts.values().any(|attempt| attempt.abort_backpressured);
        let replacement_backpressured = replacement.as_ref().is_some_and(|replacement| {
            state.conclusion().is_none() && replacement.effect_backpressured
        });
        let replacement_effect_pending = replacement.as_ref().is_some_and(|replacement| {
            replacement.effect_dispatched && !replacement.effect_settled
        });
        let replacement_expires_at = replacement.as_ref().and_then(|replacement| {
            state
                .conclusion()
                .is_none()
                .then_some(replacement.conservative_expiry)
                .flatten()
        });
        let result_capacity_transport = result_runtime.as_ref().and_then(|runtime| {
            (runtime.pending.is_some()
                && runtime.in_flight.is_none()
                && runtime.deferred_internal.is_none()
                && runtime.attempt_decision_pending.is_none())
            .then(|| runtime.transport.clone())
        });
        let result_receipt_pending = result_runtime.as_ref().is_some_and(|runtime| {
            runtime.in_flight.is_some()
                || runtime.schema_receipt.is_some()
                || runtime
                    .internal
                    .as_ref()
                    .is_some_and(InternalRuntime::source_pending)
        });
        let result_consumer_transport = result_runtime.as_ref().and_then(|runtime| {
            state
                .conclusion()
                .is_none()
                .then_some(())
                .filter(|_| !runtime.internal_exit_supervised())
                .map(|_| runtime.transport.clone())
        });
        let root_terminal_pending = root_terminal_receiver.is_some();
        tokio::select! {
            biased;
            changed = registry_close_requested_rx.changed(), if !registry_close_cancelled && registry_close_signal_open => {
                if changed.is_ok() {
                    let requested_close = *registry_close_requested_rx.borrow();
                    observe_registry_close(
                        requested_close,
                        state.conclusion().is_some(),
                        &mut registry_close_requested,
                        &mut registry_close_cancelled,
                        &work_owner,
                    );
                } else {
                    registry_close_signal_open = false;
                }
            }
            reason = work_cancellation.cancelled(), if state.conclusion().is_none() => {
                revoke_all_establish_authority(&mut attempts);
                conclude_for_work_cancellation(
                    state,
                    replacement.as_ref(),
                    result_runtime.as_mut(),
                    &reason,
                );
                lifetime.settle();
            }
            changed = abandoned.changed(), if !*abandoned.borrow() => {
                if changed.is_ok() && *abandoned.borrow() {
                    revoke_all_establish_authority(&mut attempts);
                    conclude_or_interrupt_success(
                        state,
                        replacement.as_ref(),
                        result_runtime.as_mut(),
                        LogicalConclusion::Cancelled,
                    );
                    lifetime.settle();
                }
            }
            observation = wait_for_root_terminal(root_terminal_receiver.as_mut()), if root_terminal_pending => {
                match observation {
                    Some(observation) => apply_root_terminal_observation(
                        state,
                        &mut attempts,
                        result_runtime.as_mut(),
                        observation,
                    ),
                    None => {
                        root_terminal_receiver.take();
                        if !root_success_facts_complete(result_runtime.as_ref()) {
                            if result_runtime.as_ref().is_some_and(|runtime| runtime.internal.is_some()) {
                                revoke_all_establish_authority(&mut attempts);
                            }
                            conclude_or_interrupt_success(
                                state,
                                replacement.as_ref(),
                                result_runtime.as_mut(),
                                LogicalConclusion::Failed,
                            );
                        }
                    }
                }
            }
            event = wait_for_attempt_ledger_event(&mut attempts, clock.as_ref()) => {
                apply_attempt_ledger_event(
                    &mut attempts,
                    event,
                    &mut establish_error,
                    &mut stand_down_error,
                );
            }
            receipt = replacement_receipt_rx.recv(), if replacement_effect_pending => {
                apply_replacement_receipt(
                    state,
                    replacement.as_mut(),
                    receipt,
                    &mut replacement_error,
                );
            }
            _ = wait_for_replacement_expiry(clock.as_ref(), replacement_expires_at), if replacement_expires_at.is_some() => {}
            _ = wait_for_abort_retry(clock.as_ref(), abort_retry_at), if abort_retry_at.is_some() => {}
            permit = wait_for_result_capacity(result_capacity_transport), if result_capacity_transport.is_some() => {
                apply_result_capacity(state, &mut attempts, result_runtime.as_mut(), permit);
            }
            disposition = wait_for_result_receipt(result_runtime.as_mut()), if result_receipt_pending => {
                apply_result_receipt(state, &mut attempts, replacement.as_ref(), result_runtime.as_mut(), disposition);
            }
            _ = wait_for_result_consumer_closed(result_consumer_transport), if result_consumer_transport.is_some() => {
                if result_runtime.as_ref().is_some_and(|runtime| runtime.internal.is_some()) {
                    revoke_all_establish_authority(&mut attempts);
                }
                conclude_or_interrupt_success(
                    state,
                    replacement.as_ref(),
                    result_runtime.as_mut(),
                    LogicalConclusion::Failed,
                );
            }
            command = receiver.recv(), if receiver_open => {
                let Some(command) = command else {
                    receiver_open = false;
                    continue;
                };
                if let ActorCommand::RegistryJoinReadiness {
                    close_ingress,
                    reply,
                } = command
                {
                    let ready = actor_cleanup_complete(state, &attempts, replacement.as_ref())
                        && result_runtime.as_ref().is_none_or(ResultRuntime::idle);
                    if ready && close_ingress {
                        receiver.close();
                        receiver_open = false;
                    }
                    let _ = reply.send(Ok(ready));
                    continue;
                }
                handle_command(
                    state,
                    &mut attempts,
                    &mut establish_error,
                    &mut stand_down_error,
                    &mut replacement,
                    &mut replacement_error,
                    clock.as_ref(),
                    &work_cancellation,
                    replacement_reservation_valid_for,
                    result_runtime.as_mut(),
                    &mut root_terminal_receiver,
                    native_actions.as_ref(),
                    command,
                );
            }
            result = wait_for_abort_capacity(&mut abort_capacity), if abort_backpressured => {
                match result {
                    Ok(()) => {
                        for attempt in attempts.values_mut() {
                            attempt.abort_backpressured = false;
                        }
                    }
                    Err(error) => {
                        stand_down_error.get_or_insert(error);
                        for attempt in attempts.values_mut() {
                            if attempt.abort_backpressured {
                                attempt.stand_down_error.get_or_insert(error);
                                attempt.abort_backpressured = false;
                            }
                        }
                    }
                }
            }
            result = wait_for_replacement_capacity(&mut replacement_capacity), if replacement_backpressured => {
                match result {
                    Ok(()) => {
                        if let Some(replacement) = replacement.as_mut() {
                            replacement.effect_backpressured = false;
                        }
                    }
                    Err(failure) => {
                        if let Some(replacement) = replacement.as_mut() {
                            replacement.effect_backpressured = false;
                        }
                        replacement_error.get_or_insert(failure);
                        conclude_replacement_failed(state, replacement.as_mut(), failure);
                    }
                }
            }
        }
    }
}

fn release_concluded_actor_resources(
    state: &LogicalExecutionState,
    execution_stage: &mut Option<StagePermit>,
    replacement: Option<&mut ReplacementRuntime>,
) {
    let Some(conclusion) = state.conclusion() else {
        return;
    };
    execution_stage.take();
    let Some(replacement) = replacement else {
        return;
    };
    if replacement.effect_dispatched && !replacement.effect_settled {
        replacement.effect_cancellation.send_replace(true);
    }
    replacement.reservation.take();
    if let Some(pending) = replacement.activation.take() {
        pending.lifetime.settle();
        let _ = pending.reply.send(Err(replacement_terminal_disposition(
            replacement,
            conclusion,
        )));
    }
}

fn expire_replacement_reservation(
    state: &mut LogicalExecutionState,
    replacement: Option<&mut ReplacementRuntime>,
    replacement_error: &mut Option<ReplacementQualificationFailure>,
    now: MonotonicInstant,
) {
    let Some(replacement) = replacement else {
        return;
    };
    let expired = state.conclusion().is_none()
        && replacement
            .conservative_expiry
            .is_some_and(|expiry| now.has_reached(expiry));
    if !expired {
        return;
    }
    replacement.reservation.take();
    replacement.effect_cancellation.send_replace(true);
    replacement_error.get_or_insert(ReplacementQualificationFailure::Expired);
    if let Some(pending) = replacement.activation.take() {
        pending.lifetime.settle();
        let _ = pending.reply.send(Err(
            LogicalExecutionActorError::ReplacementQualificationFailed(
                ReplacementQualificationFailure::Expired,
            ),
        ));
    }
    conclude_replacement_failed(
        state,
        Some(replacement),
        ReplacementQualificationFailure::Expired,
    );
}

enum AttemptLedgerEvent {
    Establish(QueryExecutionId, Result<(), EstablishIssueError>),
    StandDown(QueryExecutionId, Result<(), ContextStandDownError>),
}

async fn wait_for_attempt_ledger_event(
    attempts: &mut BTreeMap<QueryExecutionId, AttemptLedgers>,
    clock: &dyn MonotonicClock,
) -> AttemptLedgerEvent {
    std::future::poll_fn(|context| {
        for (execution, attempt) in attempts.iter_mut() {
            match attempt.establish.poll_next_event(context) {
                std::task::Poll::Ready(result) => {
                    return std::task::Poll::Ready(AttemptLedgerEvent::Establish(
                        *execution, result,
                    ));
                }
                std::task::Poll::Pending => {}
            }
            if attempt.stand_down.started() && !attempt.stand_down.responsibility_settled() {
                match attempt.stand_down.poll_next_event(context, || clock.now()) {
                    std::task::Poll::Ready(result) => {
                        return std::task::Poll::Ready(AttemptLedgerEvent::StandDown(
                            *execution, result,
                        ));
                    }
                    std::task::Poll::Pending => {}
                }
            }
        }
        std::task::Poll::Pending
    })
    .await
}

fn apply_attempt_ledger_event(
    attempts: &mut BTreeMap<QueryExecutionId, AttemptLedgers>,
    event: AttemptLedgerEvent,
    establish_error: &mut Option<EstablishIssueError>,
    stand_down_error: &mut Option<ContextStandDownError>,
) {
    match event {
        AttemptLedgerEvent::Establish(execution, result) => {
            let Some(attempt) = attempts.get_mut(&execution) else {
                return;
            };
            let result = result.and_then(|()| {
                if attempt.establish.has_establish_rejection() {
                    Err(EstablishIssueError::EstablishRejected)
                } else {
                    Ok(())
                }
            });
            if let Err(error) = result {
                establish_error.get_or_insert(error);
                attempt.record_establish_error(error);
                attempt.establish.revoke_issue_authority();
                // A failed Establish ledger makes this exact attempt unusable,
                // but its Task-protocol owner is still responsible for
                // publishing the authoritative terminal class. Ending the
                // logical execution here would race that owner and turn a
                // pre-visibility process replacement into an unconditional
                // failure before the supervisor can apply recovery policy.
                //
                // Every success entrance checks the same ledger. A protocol
                // fault always vetoes success. An ordinary refusal vetoes a
                // still-required context; only the Native owner's exact sealed
                // result withdrawal can make it diagnostic/residual instead.
                // The original request, verdict and stand-down responsibility
                // remain in the ledger in either case.
            }
        }
        AttemptLedgerEvent::StandDown(execution, Err(error)) => {
            stand_down_error.get_or_insert(error);
            if let Some(attempt) = attempts.get_mut(&execution) {
                attempt.stand_down_error.get_or_insert(error);
            }
        }
        AttemptLedgerEvent::StandDown(_, Ok(())) => {}
    }
}

/// Applies a Registry close request. A shutdown cancels work still in flight;
/// a retirement cancels only work that has not concluded, because the
/// conclusion is the client-visible outcome and the actor still owns its
/// delivery until the result runtime is idle.
fn observe_registry_close(
    requested: Option<RegistryClose>,
    concluded: bool,
    close_requested: &mut bool,
    close_cancelled: &mut bool,
    work_owner: &WorkOwner,
) {
    let Some(cause) = requested else {
        return;
    };
    *close_requested = true;
    if !*close_cancelled && (cause == RegistryClose::Shutdown || !concluded) {
        *close_cancelled = true;
        work_owner.cancel(CancellationReason::ServerShutdown);
    }
}

fn revoke_all_establish_authority(attempts: &mut BTreeMap<QueryExecutionId, AttemptLedgers>) {
    for attempt in attempts.values_mut() {
        attempt.establish.revoke_issue_authority();
    }
}

fn synchronize_all_stand_down(
    state: &LogicalExecutionState,
    attempts: &mut BTreeMap<QueryExecutionId, AttemptLedgers>,
    stand_down_error: &mut Option<ContextStandDownError>,
    now: MonotonicInstant,
    abort_effect_port: Option<&dyn AbortQueryContextEffectPort>,
) {
    for attempt in attempts.values_mut() {
        if !attempt.stand_down.started()
            && matches!(
                state.conclusion(),
                Some(LogicalConclusion::Succeeded | LogicalConclusion::ComputationReady)
            )
            && let Err(error) = begin_or_refresh_successful_cleanup(attempt)
        {
            stand_down_error.get_or_insert(error);
            attempt.stand_down_error.get_or_insert(error);
        }
        let replacement_failed = matches!(
            state.phase(),
            ExecutionPhase::Replacing { failed, .. } if failed == attempt.activation.execution()
        );
        let cause = if attempt.stand_down.started() || replacement_failed {
            Some(ContextStandDownCause::LogicalExecutionFailed)
        } else {
            state.conclusion().and_then(|conclusion| match conclusion {
                LogicalConclusion::Cancelled => {
                    Some(ContextStandDownCause::LogicalExecutionCancelled)
                }
                LogicalConclusion::Failed => Some(ContextStandDownCause::LogicalExecutionFailed),
                LogicalConclusion::Succeeded | LogicalConclusion::ComputationReady => None,
            })
        };
        if let Some(cause) = cause
            && let Err(error) = begin_or_refresh_stand_down(attempt, cause)
        {
            stand_down_error.get_or_insert(error);
            attempt.stand_down_error.get_or_insert(error);
        }
        if attempt.stand_down.started() && attempt.stand_down.responsibility_settled() {
            if let Err(error) = cancel_pending_abort_issues(
                &mut attempt.stand_down,
                &mut attempt.pending_aborts,
                now,
            ) {
                stand_down_error.get_or_insert(error);
                attempt.stand_down_error.get_or_insert(error);
            }
            attempt.abort_backpressured = !attempt.pending_aborts.is_empty();
        }
        if attempt.stand_down.remote_tracking_ended()
            && !attempt.stand_down.residual_resource_settled()
        {
            let obligation_ended = attempt
                .attempt_obligation
                .as_ref()
                .is_none_or(Obligation::end_remote_tracking_unknown);
            if obligation_ended {
                attempt.attempt_obligation.take();
                // This invokes the resource owner's abnormal exit path. It
                // must not be replaced with `finish()`: finite FE tracking is
                // not evidence that the remote attempt physically stopped.
                attempt.active_resources.take();
            } else {
                stand_down_error
                    .get_or_insert(ContextStandDownError::ResponsibilitySettlementFailed);
                attempt
                    .stand_down_error
                    .get_or_insert(ContextStandDownError::ResponsibilitySettlementFailed);
                continue;
            }
        } else if attempt.stand_down.residual_resource_settled() {
            let obligation_resolved = attempt
                .attempt_obligation
                .as_ref()
                .is_none_or(Obligation::resolve);
            if obligation_resolved {
                attempt.attempt_obligation.take();
            } else {
                stand_down_error
                    .get_or_insert(ContextStandDownError::ResponsibilitySettlementFailed);
                attempt
                    .stand_down_error
                    .get_or_insert(ContextStandDownError::ResponsibilitySettlementFailed);
                continue;
            }
            if let Some(resources) = attempt.active_resources.take() {
                resources.finish();
            }
        }
        if !attempt.stand_down.is_successful_cleanup()
            && attempt.stand_down_error.is_none()
            && let Err(error) = drive_abort_effect(
                &mut attempt.stand_down,
                abort_effect_port,
                &mut attempt.pending_aborts,
                &mut attempt.abort_backpressured,
                now,
            )
        {
            stand_down_error.get_or_insert(error);
            attempt.stand_down_error.get_or_insert(error);
        }
    }
}

fn begin_or_refresh_stand_down(
    attempt: &mut AttemptLedgers,
    cause: ContextStandDownCause,
) -> Result<(), ContextStandDownError> {
    let facts = attempt
        .establish
        .stand_down_facts()
        .map_err(|_| ContextStandDownError::WrongState)?;
    if !attempt.stand_down.started() {
        return attempt.stand_down.begin(attempt.activation, cause, facts);
    }
    for (context, fact) in facts {
        attempt.stand_down.refresh_establish_fact(context, fact)?;
    }
    Ok(())
}

fn begin_or_refresh_successful_cleanup(
    attempt: &mut AttemptLedgers,
) -> Result<(), ContextStandDownError> {
    let facts = attempt
        .establish
        .stand_down_facts()
        .map_err(|_| ContextStandDownError::WrongState)?;
    if !attempt.stand_down.started() {
        return attempt
            .stand_down
            .begin_successful_cleanup(attempt.activation, facts);
    }
    for (context, fact) in facts {
        attempt.stand_down.refresh_establish_fact(context, fact)?;
    }
    Ok(())
}

fn next_abort_retry_at(
    attempts: &BTreeMap<QueryExecutionId, AttemptLedgers>,
) -> Option<MonotonicInstant> {
    attempts
        .values()
        .filter(|attempt| attempt.stand_down_error.is_none())
        .filter_map(|attempt| attempt.stand_down.next_retry_at())
        .min()
}

fn drive_replacement_effect(
    replacement: Option<&mut ReplacementRuntime>,
    port: Option<&dyn ReplacementQualificationEffectPort>,
    receipts: &mpsc::UnboundedSender<ReplacementQualificationEffectReceipt>,
    now: MonotonicInstant,
) {
    let Some(replacement) = replacement else {
        return;
    };
    if replacement.effect_dispatched || replacement.effect_backpressured {
        return;
    }
    let Some(port) = port else {
        return;
    };
    match port.try_reserve(replacement.request.as_ref()) {
        ReplacementQualificationEffectAdmission::Admitted(reservation) => {
            replacement.effect_dispatched = true;
            replacement.effect_dispatched_at = Some(now);
            reservation.submit(ReplacementQualificationEffectSubmission::new(
                Arc::clone(&replacement.request),
                receipts.clone(),
                replacement.effect_cancellation.subscribe(),
            ));
        }
        ReplacementQualificationEffectAdmission::Backpressured => {
            replacement.effect_backpressured = true;
        }
    }
}

fn apply_replacement_receipt(
    state: &mut LogicalExecutionState,
    replacement: Option<&mut ReplacementRuntime>,
    receipt: Option<ReplacementQualificationEffectReceipt>,
    replacement_error: &mut Option<ReplacementQualificationFailure>,
) {
    let Some(replacement) = replacement else {
        return;
    };
    let Some(receipt) = receipt else {
        replacement.effect_settled = true;
        replacement_error.get_or_insert(ReplacementQualificationFailure::EffectOwnerClosed);
        if state.conclusion().is_none() {
            conclude_replacement_failed(
                state,
                Some(replacement),
                ReplacementQualificationFailure::EffectOwnerClosed,
            );
        }
        return;
    };
    replacement.effect_settled = true;
    let (operation_id, identity, settlement) = receipt.into_parts();
    if identity != replacement.identity || operation_id != replacement.request.operation_id() {
        replacement_error.get_or_insert(ReplacementQualificationFailure::MismatchedReceipt);
        if state.conclusion().is_none() {
            conclude_replacement_failed(
                state,
                Some(replacement),
                ReplacementQualificationFailure::MismatchedReceipt,
            );
        }
        return;
    }
    match settlement {
        ReplacementQualificationSettlement::Qualified(reservation) => {
            let Some(dispatched_at) = replacement.effect_dispatched_at else {
                replacement_error
                    .get_or_insert(ReplacementQualificationFailure::InvalidReservation);
                conclude_replacement_failed(
                    state,
                    Some(replacement),
                    ReplacementQualificationFailure::InvalidReservation,
                );
                return;
            };
            let ticket_expiry =
                reservation.conservative_expiry(dispatched_at, replacement.reservation_valid_for);
            replacement.conservative_expiry = Some(
                replacement
                    .conservative_expiry
                    .map_or(ticket_expiry, |current| current.min(ticket_expiry)),
            );
            if let Some(conclusion) = state.conclusion() {
                if let Some(pending) = replacement.activation.take() {
                    pending.lifetime.settle();
                    let _ = pending.reply.send(Err(replacement_terminal_disposition(
                        replacement,
                        conclusion,
                    )));
                }
                return;
            }
            replacement.reservation = Some(reservation);
        }
        ReplacementQualificationSettlement::Failed(failure) => {
            replacement_error.get_or_insert(failure);
            if state.conclusion().is_none() {
                conclude_replacement_failed(state, Some(replacement), failure);
            }
        }
    }
}

fn conclude_replacement_failed(
    state: &mut LogicalExecutionState,
    replacement: Option<&mut ReplacementRuntime>,
    failure: ReplacementQualificationFailure,
) {
    if let Some(replacement) = replacement
        && state
            .conclude_replacement(&replacement.token, LogicalConclusion::Failed)
            .is_ok()
    {
        replacement.accepted_failure = Some(failure);
    }
}

fn accepted_replacement_error(
    state: &LogicalExecutionState,
    replacement: Option<&ReplacementRuntime>,
) -> Option<QueryExecutionError> {
    if state.conclusion() != Some(LogicalConclusion::Failed) {
        return None;
    }
    replacement
        .and_then(|replacement| replacement.accepted_failure)
        .map(|failure| {
            result_actor_failure(LogicalExecutionActorError::ReplacementQualificationFailed(
                failure,
            ))
        })
}

fn replacement_terminal_disposition(
    replacement: &ReplacementRuntime,
    conclusion: LogicalConclusion,
) -> LogicalExecutionActorError {
    replacement.accepted_failure.map_or(
        LogicalExecutionActorError::ExecutionConcluded(conclusion),
        LogicalExecutionActorError::ReplacementQualificationFailed,
    )
}

fn try_activate_qualified_replacement(
    state: &mut LogicalExecutionState,
    attempts: &mut BTreeMap<QueryExecutionId, AttemptLedgers>,
    replacement: &mut Option<ReplacementRuntime>,
    work: &WorkScope,
    max_admission_issues_per_context: NonZeroUsize,
    max_establish_authorizations_per_context: NonZeroUsize,
    max_abort_authorizations_per_context: NonZeroUsize,
    clock: &dyn MonotonicClock,
    result_runtime: Option<&mut ResultRuntime>,
) {
    let Some(active) = replacement.as_mut() else {
        return;
    };
    let Some(pending_activation) = active.activation.take() else {
        return;
    };
    let Some(reservation) = active.reservation.take() else {
        active.activation = Some(pending_activation);
        return;
    };
    let Some(conservative_expiry) = active.conservative_expiry else {
        pending_activation.lifetime.settle();
        let _ = pending_activation.reply.send(Err(
            LogicalExecutionActorError::ReplacementQualificationFailed(
                ReplacementQualificationFailure::InvalidReservation,
            ),
        ));
        conclude_replacement_failed(
            state,
            Some(active),
            ReplacementQualificationFailure::InvalidReservation,
        );
        return;
    };
    if clock.now().has_reached(conservative_expiry) {
        pending_activation.lifetime.settle();
        let _ = pending_activation.reply.send(Err(
            LogicalExecutionActorError::ReplacementQualificationFailed(
                ReplacementQualificationFailure::Expired,
            ),
        ));
        conclude_replacement_failed(
            state,
            Some(active),
            ReplacementQualificationFailure::Expired,
        );
        return;
    }
    // The opaque reservation proves reachability, isolation, successor
    // admission, and that the registry's old usage record remains under
    // last-known/current-unknown ownership. The exact failed attempt retains
    // its actor-owned RunningWork observation record until positive residual
    // convergence; an already converged no-remote attempt may have resolved
    // it before successor activation.
    if !active.stale_usage_accounted
        || !attempts
            .get(&active.identity.failed())
            .is_some_and(|attempt| {
                attempt.attempt_obligation.is_some()
                    || attempt.stand_down.residual_resource_settled()
            })
    {
        pending_activation.lifetime.settle();
        let _ = pending_activation.reply.send(Err(
            LogicalExecutionActorError::ReplacementQualificationFailed(
                ReplacementQualificationFailure::InvalidReservation,
            ),
        ));
        conclude_replacement_failed(
            state,
            Some(active),
            ReplacementQualificationFailure::InvalidReservation,
        );
        return;
    }
    for fact in [
        ReplacementFact::ReachableContextsClosed,
        ReplacementFact::AttemptIsolationProven,
        ReplacementFact::StaleUsageAccounted,
        ReplacementFact::NewCapacityAdmitted,
    ] {
        let result = state
            .issue_replacement_effect(&active.token, fact)
            .and_then(|pending| state.complete_replacement_effect(&active.token, pending));
        if result.is_err() {
            pending_activation.lifetime.settle();
            let _ = pending_activation.reply.send(Err(
                LogicalExecutionActorError::ReplacementQualificationFailed(
                    ReplacementQualificationFailure::OutcomeUnknown,
                ),
            ));
            conclude_replacement_failed(
                state,
                Some(active),
                ReplacementQualificationFailure::OutcomeUnknown,
            );
            return;
        }
    }
    let activated = match reservation.activate() {
        Ok(resources) => resources,
        Err(error) => {
            pending_activation.lifetime.settle();
            let _ = pending_activation.reply.send(Err(
                LogicalExecutionActorError::ReplacementQualificationFailed(error),
            ));
            conclude_replacement_failed(state, Some(active), error);
            return;
        }
    };
    let Some(first_sent_at) = active.effect_dispatched_at else {
        pending_activation.lifetime.settle();
        let _ = pending_activation.reply.send(Err(
            LogicalExecutionActorError::ReplacementQualificationFailed(
                ReplacementQualificationFailure::InvalidReservation,
            ),
        ));
        conclude_replacement_failed(
            state,
            Some(active),
            ReplacementQualificationFailure::InvalidReservation,
        );
        return;
    };
    match state.activate_replacement(&active.token) {
        Ok(capability) => {
            let activation = identity_of(&capability);
            let contexts = active
                .request
                .replacement_contexts()
                .iter()
                .copied()
                .collect();
            let result = if attempts.contains_key(&active.identity.replacement()) {
                Err(LogicalExecutionActorError::WrongExecution)
            } else {
                new_attempt_ledgers(
                    activation,
                    contexts,
                    max_admission_issues_per_context,
                    max_establish_authorizations_per_context,
                    max_abort_authorizations_per_context,
                    None,
                    work,
                )
                .and_then(|mut ledgers| {
                    for admission in activated.admissions() {
                        let issue = ledgers.establish.begin_admission_issue(
                            activation,
                            admission.request(),
                            first_sent_at,
                        )?;
                        let disposition = ledgers.establish.settle_admission_issue(
                            activation,
                            issue,
                            AdmissionIssueSettlement::applied(
                                issue.operation_id(),
                                OperationOutcome::Accepted,
                                admission.receipt(),
                            )?,
                            clock.now(),
                        )?;
                        if disposition != AdmissionIssueDisposition::Granted {
                            return Err(
                                LogicalExecutionActorError::ReplacementQualificationFailed(
                                    ReplacementQualificationFailure::Expired,
                                ),
                            );
                        }
                    }
                    ledgers.active_resources =
                        Some(ActiveReplacementResources::replacement(activated));
                    Ok(ledgers)
                })
            }
            .map(|ledgers| {
                attempts.insert(active.identity.replacement(), ledgers);
                Ok(AttemptInstantiationPermit {
                    capability: Some(capability),
                    lifetime: Some(Arc::clone(&pending_activation.lifetime)),
                    mailbox_liveness: Some(pending_activation.mailbox_liveness.clone()),
                })
            })
            .and_then(|permit| permit);
            match result {
                Ok(permit) => {
                    if let Some(runtime) = result_runtime
                        && let Some(failed) = attempts.get(&active.identity.failed())
                    {
                        runtime.retire_attempt_failure(failed.activation);
                    }
                    if let Err(response) = pending_activation.reply.send(Ok(permit)) {
                        drop(response);
                    }
                    *replacement = None;
                }
                Err(error) => {
                    if let Ok(capability) = state.attempt_capability(active.identity.replacement())
                        && state
                            .conclude(&capability, LogicalConclusion::Failed)
                            .is_ok()
                        && let Some(runtime) = result_runtime
                        && runtime.internal.is_some()
                    {
                        runtime.accepted_error = Some(result_actor_failure(error));
                    }
                    pending_activation.lifetime.abandon();
                    let _ = pending_activation.reply.send(Err(error));
                }
            }
        }
        Err(ActorStateError::ReplacementNotReady(_)) => {
            // All prerequisites were sealed by the same qualified reservation.
            // Reaching this branch would lose active isolation ownership, so
            // fail closed instead of attempting a second qualification.
            drop(activated);
            pending_activation.lifetime.settle();
            let _ = pending_activation.reply.send(Err(
                LogicalExecutionActorError::ReplacementQualificationFailed(
                    ReplacementQualificationFailure::InvalidReservation,
                ),
            ));
            conclude_replacement_failed(
                state,
                Some(active),
                ReplacementQualificationFailure::InvalidReservation,
            );
        }
        Err(error) => {
            drop(activated);
            pending_activation.lifetime.settle();
            let _ = pending_activation.reply.send(Err(error.into()));
        }
    }
}

/// Conclude this execution as failed, naming the transition that failed.
///
/// This records local transition context. A result failure also installs its
/// producer-authored fact through `conclude_result_failure`; this text alone
/// cannot classify a failure or claim that a logical conclusion was accepted.
fn conclude_failed(state: &mut LogicalExecutionState, reason: impl Into<String>) {
    state.note_failure_reason(reason);
    if state.conclusion().is_some() {
        return;
    }
    let execution = match state.phase() {
        ExecutionPhase::Instantiating { execution, .. }
        | ExecutionPhase::Running { execution, .. } => execution,
        _ => return,
    };
    if let Ok(capability) = state.attempt_capability(execution) {
        let _ = state.conclude(&capability, LogicalConclusion::Failed);
    }
}

/// Preserve a result producer's cause only if this same serialized transition
/// actually accepts the failure. Attempt observations and prior conclusions
/// cannot be replaced by a later protocol rejection.
fn conclude_result_failure(
    state: &mut LogicalExecutionState,
    runtime: &mut ResultRuntime,
    context: &'static str,
    error: QueryExecutionError,
) {
    if state.conclusion().is_some() {
        return;
    }
    conclude_failed(state, context);
    if state.conclusion() == Some(LogicalConclusion::Failed) {
        runtime.accepted_error = Some(error);
    }
}

fn result_protocol_failure(detail: &'static str) -> QueryExecutionError {
    use novarocks_execution_contract::failure::FailureProjection;
    QueryExecutionError::from_execution(crate::execution_error::ExecutionFailureInput::new(
        LogicalExecutionActorError::ResultProtocolViolation.failure_fact(),
        novarocks_execution_contract::SafeDetail::truncating(detail),
        crate::execution_error::ExecutionFailureSite::Coordinator,
    ))
}

fn result_actor_failure(error: LogicalExecutionActorError) -> QueryExecutionError {
    use novarocks_execution_contract::failure::FailureProjection;
    QueryExecutionError::from_execution(crate::execution_error::ExecutionFailureInput::new(
        error.failure_fact(),
        error.safe_detail(),
        crate::execution_error::ExecutionFailureSite::Coordinator,
    ))
}

/// A consumer's explicit failure is already authored. Losing the receipt
/// owner only observes a stop; it cannot manufacture Query cancellation.
fn result_receipt_failure(
    disposition: Result<ResultDeliveryDisposition, ()>,
) -> Option<QueryExecutionError> {
    match disposition {
        Ok(ResultDeliveryDisposition::Completed) => None,
        Ok(ResultDeliveryDisposition::Failed(error)) => Some(*error),
        Ok(ResultDeliveryDisposition::Dropped) | Err(()) => Some(logical_owner_failure(
            crate::execution_error::QueryFailureReason::LogicalOwnerStopped,
            "result delivery receipt ended without a consumer completion",
        )),
    }
}

fn start_schema_delivery(
    state: &mut LogicalExecutionState,
    runtime: &mut ResultRuntime,
    activation: AttemptActivationIdentity,
) -> Result<(), LogicalExecutionActorError> {
    if state.schema_emitted() || matches!(runtime.in_flight, Some(InFlightResult::Schema { .. })) {
        return Ok(());
    }
    if runtime.in_flight.is_some() {
        return Err(LogicalExecutionActorError::ResultProtocolViolation);
    }
    let capability = state.attempt_capability(activation.execution())?;
    if identity_of(&capability) != activation {
        return Err(LogicalExecutionActorError::StaleAuthority);
    }
    let BeginSchemaDelivery::Permit(permit) = state.begin_schema_delivery(&capability)? else {
        return Ok(());
    };
    if runtime.schema_writer_completed {
        state.complete_schema_delivery(permit)?;
        return Ok(());
    }
    let receipt = runtime
        .schema_receipt
        .take()
        .ok_or(LogicalExecutionActorError::InvariantViolation)?;
    runtime.in_flight = Some(InFlightResult::Schema { permit, receipt });
    Ok(())
}

async fn wait_for_result_capacity(
    transport: Option<ResultTransport>,
) -> Result<ResultSlot, QueryExecutionError> {
    transport
        .expect("result capacity wait is gated by one pending delivery")
        .reserve_owned()
        .await
}

async fn wait_for_result_receipt(runtime: Option<&mut ResultRuntime>) -> ResultReceiptOutcome {
    let runtime = runtime.expect("result receipt wait is gated by a result runtime");
    if runtime
        .internal
        .as_ref()
        .is_some_and(InternalRuntime::source_pending)
    {
        let receipt_pending = runtime.in_flight.is_some() || runtime.schema_receipt.is_some();
        tokio::select! {
            biased;
            receipt = wait_for_regular_result_receipt(runtime.in_flight.as_mut(), runtime.schema_receipt.as_mut()), if receipt_pending => receipt,
            source = internal_ready::wait_for_source_exit(runtime.internal.as_mut()) => ResultReceiptOutcome::SourceExited(source),
        }
    } else {
        wait_for_regular_result_receipt(runtime.in_flight.as_mut(), runtime.schema_receipt.as_mut())
            .await
    }
}

async fn wait_for_regular_result_receipt(
    in_flight: Option<&mut InFlightResult>,
    schema: Option<&mut ResultDeliveryReceipt>,
) -> ResultReceiptOutcome {
    if let Some(in_flight) = in_flight {
        return ResultReceiptOutcome::Delivery(match in_flight {
            InFlightResult::Schema { receipt, .. }
            | InFlightResult::Batch { receipt, .. }
            | InFlightResult::End { receipt, .. } => receipt.await.map_err(|_| ()),
            InFlightResult::InternalEnd(end) => (&mut end.receipt).await.map_err(|_| ()),
        });
    }
    let receipt = schema.expect("result receipt wait requires a schema or delivery receipt");
    ResultReceiptOutcome::Schema(receipt.await.map_err(|_| ()))
}

async fn wait_for_result_consumer_closed(transport: Option<ResultTransport>) {
    transport
        .expect("result consumer wait is gated by a result runtime")
        .closed()
        .await;
}

async fn wait_for_root_terminal(
    receiver: Option<&mut mpsc::Receiver<RootTerminalObservation>>,
) -> Option<RootTerminalObservation> {
    receiver
        .expect("root terminal wait is gated by a bound observer")
        .recv()
        .await
}

fn root_success_facts_complete(runtime: Option<&ResultRuntime>) -> bool {
    runtime
        .and_then(|runtime| runtime.root_success.as_ref())
        .is_some_and(|gate| {
            gate.status.as_ref().is_some_and(|status| {
                status.state() == novarocks_execution_contract::TaskState::Finished
            }) && gate.final_eos_ack.is_some()
        })
}

fn apply_root_terminal_observation(
    state: &mut LogicalExecutionState,
    attempts: &mut BTreeMap<QueryExecutionId, AttemptLedgers>,
    runtime: Option<&mut ResultRuntime>,
    observation: RootTerminalObservation,
) {
    let Some(runtime) = runtime else {
        let _ = observation
            .reply
            .send(Err(LogicalExecutionActorError::WrongPhase));
        return;
    };
    apply_root_status_observation(
        state,
        attempts,
        runtime,
        observation.activation,
        observation.root,
        observation.status,
        observation.terminal_failure,
        observation.reply,
    );
}

/// The native Task round may observe its root success seal before the actor
/// has consumed the matching Establish settlement. Success EOF is therefore
/// gated on the actor ledger, not on incidental scheduling order between the
/// Task round and this mailbox.
fn settle_finish_establish_readiness(
    state: &mut LogicalExecutionState,
    attempts: &mut BTreeMap<QueryExecutionId, AttemptLedgers>,
    runtime: Option<&mut ResultRuntime>,
) -> bool {
    if state.conclusion().is_some() {
        return true;
    }
    let Some(runtime) = runtime else {
        return false;
    };
    let Some(finish) = runtime.finish_waiter.as_ref() else {
        return true;
    };
    let execution = finish.permit.identity().execution();
    let result = attempts
        .get_mut(&execution)
        .ok_or(EstablishIssueError::UnknownContext)
        .and_then(AttemptLedgers::ensure_success_ready);
    match result {
        Ok(()) => true,
        Err(EstablishIssueError::EstablishNotSettled) => false,
        Err(error) => {
            if runtime.internal.is_some() {
                revoke_all_establish_authority(attempts);
            }
            tracing::debug!(error = %error, "Establish settlement failed before success EOF");
            conclude_failed(state, "Establish settlement failed before success EOF");
            if state.conclusion() == Some(LogicalConclusion::Failed) {
                use novarocks_execution_contract::failure::FailureProjection;
                runtime.accepted_error = Some(QueryExecutionError::from_execution(
                    crate::execution_error::ExecutionFailureInput::new(
                        error.failure_fact(),
                        error.safe_detail(),
                        crate::execution_error::ExecutionFailureSite::Coordinator,
                    ),
                ));
            }
            false
        }
    }
}

fn drive_root_success(
    state: &LogicalExecutionState,
    runtime: Option<&mut ResultRuntime>,
    finish_establish_ready: bool,
) -> Result<(), LogicalExecutionActorError> {
    let Some(runtime) = runtime else {
        return Ok(());
    };
    if runtime.finish_waiter.is_none() || runtime.pending.is_some() || runtime.in_flight.is_some() {
        return Ok(());
    }
    if !finish_establish_ready {
        return Ok(());
    }
    let Some(gate) = runtime.root_success.as_ref() else {
        return Ok(());
    };
    if !gate
        .status
        .as_ref()
        .is_some_and(|status| status.state() == novarocks_execution_contract::TaskState::Finished)
    {
        return Ok(());
    }
    let Some(sequence) = gate.final_eos_ack else {
        return Ok(());
    };
    let expected = ResultPacketSequence::new(state.accepted_result_packets());
    if sequence != expected {
        return Err(LogicalExecutionActorError::ResultProtocolViolation);
    }
    if sequence.get() > 0
        && state.delivered_result_through() != Some(sequence.get().saturating_sub(1))
    {
        return Err(LogicalExecutionActorError::ResultProtocolViolation);
    }
    let finish = runtime
        .finish_waiter
        .as_ref()
        .ok_or(LogicalExecutionActorError::InvariantViolation)?;
    if finish.permit.identity() != gate.activation {
        return Err(LogicalExecutionActorError::ResultProtocolViolation);
    }
    let finish = runtime
        .finish_waiter
        .take()
        .expect("validated finish waiter remains owned by the result runtime");
    runtime.pending = Some(PendingResult::End(PendingResultEnd {
        permit: finish.permit,
        sequence,
        reply: finish.reply,
    }));
    Ok(())
}

fn apply_result_capacity(
    state: &mut LogicalExecutionState,
    attempts: &mut BTreeMap<QueryExecutionId, AttemptLedgers>,
    runtime: Option<&mut ResultRuntime>,
    reserved: Result<ResultSlot, QueryExecutionError>,
) {
    let Some(runtime) = runtime else {
        return;
    };
    let Some(pending) = runtime.pending.take() else {
        return;
    };
    let slot = match reserved {
        Ok(slot) => slot,
        Err(_)
            if state.conclusion().is_none()
                && runtime.internal_exit_supervised()
                && matches!(runtime.transport, ResultTransport::Internal(_))
                && matches!(
                    pending,
                    PendingResult::Batch(_) | PendingResult::InternalEnd(_)
                ) =>
        {
            debug_assert!(runtime.deferred_internal.is_none());
            runtime.deferred_internal = Some(DeferredInternalDelivery::Pending(pending));
            return;
        }
        Err(error) => {
            if runtime.internal.is_some() {
                revoke_all_establish_authority(attempts);
            }
            conclude_result_failure(
                state,
                runtime,
                "no result delivery slot could be reserved",
                error,
            );
            reject_pending_result(state, pending);
            return;
        }
    };
    match pending {
        PendingResult::Batch(pending) => {
            let capability = match state.attempt_capability(pending.activation.execution()) {
                Ok(capability) if identity_of(&capability) == pending.activation => capability,
                _ => {
                    drop(slot);
                    drop(pending.delivery);
                    let _ = pending
                        .reply
                        .send(Err(LogicalExecutionActorError::StaleAuthority));
                    return;
                }
            };
            let permit = match state.begin_result_delivery(
                &capability,
                pending.sequence.get(),
                ResultPacket::Data(()),
            ) {
                Ok(permit) => permit,
                Err(error) => {
                    if runtime.internal.is_some() {
                        revoke_all_establish_authority(attempts);
                    }
                    drop(slot);
                    drop(pending.delivery);
                    conclude_result_failure(
                        state,
                        runtime,
                        "a reserved result delivery slot could not be started",
                        result_actor_failure(ActorStateError::Delivery(error.error()).into()),
                    );
                    reply_result_failure(state, pending.reply);
                    return;
                }
            };
            if runtime.internal.is_some()
                && !matches!(
                    (&runtime.transport, &slot),
                    (ResultTransport::Internal(_), ResultSlot::Internal(_))
                )
            {
                // The original enqueue will reject this mismatched slot.
                // Revoke before its by-value refusal drops the delivery.
                revoke_all_establish_authority(attempts);
            }
            if let Err(error) = runtime.transport.enqueue_segment(slot, pending.delivery) {
                if runtime.internal.is_some() {
                    revoke_all_establish_authority(attempts);
                }
                let _ = state.fail_result_delivery(permit);
                runtime.accepted_error = Some(error);
                reply_result_failure(state, pending.reply);
                return;
            }
            runtime.in_flight = Some(InFlightResult::Batch {
                permit,
                receipt: pending.receipt,
                reply: pending.reply,
            });
        }
        PendingResult::End(pending) => {
            if !matches!(
                (&runtime.transport, &slot),
                (ResultTransport::Client(_), ResultSlot::Client(_))
            ) {
                drop(slot);
                conclude_consumed_handoff_as_failed(state, pending.permit, pending.reply);
                return;
            }
            let activation = pending.permit.identity();
            let capability = match state.attempt_capability(activation.execution()) {
                Ok(capability) if identity_of(&capability) == activation => capability,
                _ => {
                    drop(slot);
                    conclude_consumed_handoff_as_failed(state, pending.permit, pending.reply);
                    return;
                }
            };
            let success = state
                .issue_attempt_success_effect(&capability)
                .and_then(|effect| state.complete_attempt_success_effect(&capability, effect));
            let Ok(success) = success else {
                drop(slot);
                conclude_consumed_handoff_as_failed(state, pending.permit, pending.reply);
                return;
            };
            let permit = match state.begin_success_end_of_stream(
                &capability,
                &success,
                pending.sequence.get(),
            ) {
                Ok(permit) => permit,
                Err(_) => {
                    drop(slot);
                    conclude_consumed_handoff_as_failed(state, pending.permit, pending.reply);
                    return;
                }
            };
            // Commit logical success before the End token becomes observable.
            // Once a protocol/application consumer can see this token, a
            // later root or cancellation observation cannot safely revoke
            // success. The token disposition settles only result transport.
            if state.complete_success_end_of_stream(permit).is_err() {
                drop(slot);
                conclude_consumed_handoff_as_failed(state, pending.permit, pending.reply);
                return;
            }
            let (delivery, receipt) = EndDelivery::success_eof_with_root_rows(
                activation.execution(),
                pending.sequence,
                runtime
                    .root_success
                    .as_ref()
                    .and_then(|gate| gate.root_output_rows),
            );
            runtime
                .transport
                .enqueue_client_end(slot, delivery)
                .expect("the frozen client channel was checked before success authorization");
            let (_capability, lifetime, mailbox_liveness) = pending.permit.into_parts();
            drop(mailbox_liveness);
            lifetime.settle();
            let _ = pending.reply.send(Ok(LogicalConclusion::Succeeded));
            runtime.in_flight = Some(InFlightResult::End { receipt });
        }
        PendingResult::InternalEnd(pending) => {
            internal_ready::enqueue_end(state, attempts, runtime, slot, pending)
        }
    }
}

fn apply_result_receipt(
    state: &mut LogicalExecutionState,
    attempts: &mut BTreeMap<QueryExecutionId, AttemptLedgers>,
    replacement: Option<&ReplacementRuntime>,
    runtime: Option<&mut ResultRuntime>,
    outcome: ResultReceiptOutcome,
) {
    let Some(runtime) = runtime else {
        return;
    };
    let was_unconcluded = state.conclusion().is_none();
    let disposition = match outcome {
        ResultReceiptOutcome::SourceExited(source) => {
            internal_ready::source_exited(state, attempts, Some(runtime), source);
            return;
        }
        ResultReceiptOutcome::Schema(disposition) => {
            runtime.schema_receipt.take();
            if let Some(error) = result_receipt_failure(disposition) {
                if runtime.internal.is_some() {
                    revoke_all_establish_authority(attempts);
                }
                conclude_with(state, replacement, LogicalConclusion::Failed);
                if was_unconcluded && state.conclusion() == Some(LogicalConclusion::Failed) {
                    runtime.accepted_error = Some(error);
                }
            } else {
                runtime.schema_writer_completed = true;
            }
            return;
        }
        ResultReceiptOutcome::Delivery(disposition) => disposition,
    };
    if state.conclusion().is_none()
        && runtime.internal_exit_supervised()
        && matches!(
            disposition,
            Ok(ResultDeliveryDisposition::Dropped) | Err(())
        )
        && matches!(
            runtime.in_flight,
            Some(InFlightResult::Batch { .. } | InFlightResult::InternalEnd(_))
        )
    {
        // Unwinding can drop the input before the outer supervisor reports
        // the actual typed Err/panic. Do not repoll this completed receipt.
        debug_assert!(runtime.deferred_internal.is_none());
        runtime.deferred_internal = runtime
            .in_flight
            .take()
            .map(DeferredInternalDelivery::InFlight);
        return;
    }
    let Some(in_flight) = runtime.in_flight.take() else {
        return;
    };
    let mut failure = result_receipt_failure(disposition.clone());
    let completed = failure.is_none();
    // Settle the original state transition before installing a failure or
    // replying. A prior cancellation/success still owns its exact conclusion.
    let reply = match in_flight {
        InFlightResult::InternalEnd(end) => {
            internal_ready::settle_end(state, attempts, runtime, end, disposition);
            return;
        }
        InFlightResult::Schema { permit, .. } => {
            if completed {
                if let Err(error) = state.complete_schema_delivery(permit) {
                    failure = Some(result_actor_failure(error.into()));
                    if runtime.internal.is_some() {
                        revoke_all_establish_authority(attempts);
                    }
                    conclude_failed(state, "schema delivery could not be completed");
                }
            } else {
                if runtime.internal.is_some() {
                    revoke_all_establish_authority(attempts);
                }
                if let Err(error) = state.fail_schema_delivery(permit) {
                    failure = Some(result_actor_failure(error.into()));
                }
                conclude_failed(state, "schema delivery was rejected by the consumer");
            }
            None
        }
        InFlightResult::Batch { permit, reply, .. } => {
            let delivered = if completed {
                match state.complete_result_delivery(permit) {
                    Ok(_) => true,
                    Err(error) => {
                        failure = Some(result_actor_failure(
                            ActorStateError::Delivery(error.error()).into(),
                        ));
                        if runtime.internal.is_some() {
                            revoke_all_establish_authority(attempts);
                        }
                        conclude_failed(state, "result delivery could not be completed");
                        false
                    }
                }
            } else {
                if runtime.internal.is_some() {
                    revoke_all_establish_authority(attempts);
                }
                if let Err(error) = state.fail_result_delivery(permit) {
                    failure = Some(result_actor_failure(
                        ActorStateError::Delivery(error.error()).into(),
                    ));
                }
                conclude_failed(state, "result delivery was rejected by the consumer");
                false
            };
            Some((reply, delivered))
        }
        InFlightResult::End { .. } => {
            // This EOF was authorized only after committed logical success.
            // Its transport failure cannot install another logical cause.
            None
        }
    };
    if was_unconcluded
        && state.conclusion() == Some(LogicalConclusion::Failed)
        && let Some(error) = failure
    {
        runtime.accepted_error = Some(error);
    }
    if let Some((reply, delivered)) = reply {
        if delivered {
            let _ = reply.send(Ok(()));
        } else {
            reply_result_failure(state, reply);
        }
    }
}

fn reject_pending_result(state: &mut LogicalExecutionState, pending: PendingResult) {
    match pending {
        PendingResult::Batch(pending) => {
            drop(pending.delivery);
            reply_result_failure(state, pending.reply);
        }
        PendingResult::End(pending) => {
            if state.conclusion().is_none() {
                conclude_consumed_handoff_as_failed(state, pending.permit, pending.reply);
            } else {
                reply_consumed_handoff_actual(state, pending.permit, pending.reply, true);
            }
        }
        PendingResult::InternalEnd(pending) => {
            let _ = pending.reply.send(Err(state.conclusion().map_or(
                LogicalExecutionActorError::ResultDeliveryFailed,
                LogicalExecutionActorError::ExecutionConcluded,
            )));
        }
    }
}

fn reject_pending_result_for_retirement(pending: PendingResult) {
    match pending {
        PendingResult::Batch(pending) => {
            let _ = pending
                .reply
                .send(Err(LogicalExecutionActorError::StaleAuthority));
        }
        PendingResult::InternalEnd(pending) => {
            let _ = pending
                .reply
                .send(Err(LogicalExecutionActorError::StaleAuthority));
        }
        PendingResult::End(pending) => {
            let (_, lifetime, mailbox) = pending.permit.into_parts();
            drop(mailbox);
            lifetime.settle();
            let _ = pending
                .reply
                .send(Err(LogicalExecutionActorError::StaleAuthority));
        }
    }
}

fn retire_in_flight_result(in_flight: InFlightResult) {
    match in_flight {
        InFlightResult::Batch { reply, .. } => {
            let _ = reply.send(Err(LogicalExecutionActorError::StaleAuthority));
        }
        InFlightResult::InternalEnd(end) => {
            let _ = end.checked.send(Err(result_actor_failure(
                LogicalExecutionActorError::StaleAuthority,
            )));
            let _ = end
                .reply
                .send(Err(LogicalExecutionActorError::StaleAuthority));
        }
        InFlightResult::Schema { .. } | InFlightResult::End { .. } => {}
    }
}

fn logical_owner_failure(
    reason: crate::execution_error::QueryFailureReason,
    detail: &str,
) -> QueryExecutionError {
    use novarocks_execution_contract::failure::FailureProjection;
    QueryExecutionError::from_execution(crate::execution_error::ExecutionFailureInput::new(
        reason.failure_fact(),
        novarocks_execution_contract::SafeDetail::truncating(detail),
        crate::execution_error::ExecutionFailureSite::Coordinator,
    ))
}

fn fail_result_runtime(state: &mut LogicalExecutionState, runtime: Option<&mut ResultRuntime>) {
    let Some(runtime) = runtime else {
        return;
    };
    internal_ready::notify_terminal(state, runtime);
    let internal_error = runtime.accepted_error.clone().or_else(|| {
        runtime
            .attempt_error
            .as_ref()
            .map(|failure| failure.error.clone())
    });
    if let Some(sender) = runtime.failure_sender.take() {
        if matches!(
            state.conclusion(),
            Some(LogicalConclusion::Succeeded | LogicalConclusion::ComputationReady)
        ) {
            runtime.accepted_error.take();
            runtime.attempt_error.take();
            drop(sender);
        } else if let Some(error) = runtime.accepted_error.clone().or_else(|| {
            (state.conclusion() == Some(LogicalConclusion::Failed))
                .then(|| {
                    runtime
                        .attempt_error
                        .as_ref()
                        .map(|failure| failure.error.clone())
                })
                .flatten()
        }) {
            runtime.accepted_error.get_or_insert_with(|| error.clone());
            sender.send_replace(Some(error));
        } else {
            match state.conclusion() {
                Some(LogicalConclusion::Succeeded | LogicalConclusion::ComputationReady) => {
                    drop(sender)
                }
                Some(LogicalConclusion::Cancelled) => {
                    sender.send_replace(Some(logical_owner_failure(
                        crate::execution_error::QueryFailureReason::LogicalOwnerStopped,
                        "the logical result owner stopped without an accepted cancellation",
                    )));
                }
                Some(LogicalConclusion::Failed) => {
                    tracing::debug!(reason = ?state.failure_reason(), "logical execution failed without a selected domain cause");
                    sender.send_replace(Some(logical_owner_failure(
                        crate::execution_error::QueryFailureReason::LogicalFailureUnspecified,
                        "logical execution failed before success EOF",
                    )));
                }
                None => {}
            }
        }
    }
    runtime.root_success = None;
    if let Some(deferred) = runtime.deferred_internal.take() {
        deferred.settle(state, internal_error.clone());
    }
    if let Some(finish) = runtime.finish_waiter.take() {
        if state.conclusion().is_none() {
            conclude_consumed_handoff_as_failed(state, finish.permit, finish.reply);
        } else {
            reply_consumed_handoff_actual(state, finish.permit, finish.reply, true);
        }
    }
    if let Some(pending) = runtime.pending.take() {
        reject_pending_result(state, pending);
    }
    if let Some(in_flight) = runtime.in_flight.take() {
        settle_failed_in_flight_result(state, in_flight, internal_error);
    }
}

fn settle_failed_in_flight_result(
    state: &LogicalExecutionState,
    in_flight: InFlightResult,
    internal_error: Option<QueryExecutionError>,
) {
    match in_flight {
        InFlightResult::Schema { .. } => {}
        InFlightResult::Batch { reply, .. } => {
            reply_result_failure(state, reply);
        }
        InFlightResult::End { .. } => {}
        InFlightResult::InternalEnd(end) => {
            let error = internal_error.unwrap_or_else(|| {
                result_actor_failure(state.conclusion().map_or(
                    LogicalExecutionActorError::ResultDeliveryFailed,
                    LogicalExecutionActorError::ExecutionConcluded,
                ))
            });
            let _ = end.checked.send(Err(error));
            let _ = end.reply.send(Err(state.conclusion().map_or(
                LogicalExecutionActorError::ResultDeliveryFailed,
                LogicalExecutionActorError::ExecutionConcluded,
            )));
        }
    }
}

fn conclude_for_work_cancellation(
    state: &mut LogicalExecutionState,
    replacement: Option<&ReplacementRuntime>,
    runtime: Option<&mut ResultRuntime>,
    reason: &CancellationReason,
) {
    let was_unconcluded = state.conclusion().is_none();
    let conclusion = match reason {
        CancellationReason::DeadlineExceeded
        | CancellationReason::FrontendDrainDeadlineExceeded => LogicalConclusion::Failed,
        _ => LogicalConclusion::Cancelled,
    };
    conclude_or_interrupt_success(state, replacement, None, conclusion);
    if was_unconcluded
        && state.conclusion() == Some(conclusion)
        && let Some(runtime) = runtime
    {
        let (kind, message) = match reason {
            CancellationReason::DeadlineExceeded
            | CancellationReason::FrontendDrainDeadlineExceeded => (
                crate::execution_error::QueryFailureReason::DeadlineExceeded,
                "logical execution deadline expired before success EOF",
            ),
            CancellationReason::Requested
            | CancellationReason::ExplicitKill { .. }
            | CancellationReason::ExplicitKillConnection { .. }
            | CancellationReason::ClientCancelRequested
            | CancellationReason::ClientDisconnected
            | CancellationReason::ServerShutdown
            | CancellationReason::OwnerDropped => (
                crate::execution_error::QueryFailureReason::Cancelled,
                "logical execution was cancelled before success EOF",
            ),
        };
        runtime.accepted_error = Some(logical_owner_failure(kind, message));
    }
}

fn conclude_or_interrupt_success(
    state: &mut LogicalExecutionState,
    replacement: Option<&ReplacementRuntime>,
    runtime: Option<&mut ResultRuntime>,
    conclusion: LogicalConclusion,
) {
    let _ = runtime;
    conclude_with(state, replacement, conclusion);
}

fn conclude_with(
    state: &mut LogicalExecutionState,
    replacement: Option<&ReplacementRuntime>,
    conclusion: LogicalConclusion,
) {
    if state.conclusion().is_some() {
        return;
    }
    let execution = match state.phase() {
        ExecutionPhase::Instantiating { execution, .. }
        | ExecutionPhase::Running { execution, .. } => execution,
        ExecutionPhase::Replacing { .. } => {
            if let Some(replacement) = replacement {
                let _ = state.conclude_replacement(&replacement.token, conclusion);
            }
            return;
        }
        _ => return,
    };
    if let Ok(capability) = state.attempt_capability(execution) {
        let _ = state.conclude(&capability, conclusion);
    }
}

#[expect(
    clippy::too_many_arguments,
    reason = "each mutable reference is a distinct actor ledger and must stay separately borrowed while one command is applied"
)]
fn handle_command(
    state: &mut LogicalExecutionState,
    attempts: &mut BTreeMap<QueryExecutionId, AttemptLedgers>,
    establish_error: &mut Option<EstablishIssueError>,
    stand_down_error: &mut Option<ContextStandDownError>,
    replacement: &mut Option<ReplacementRuntime>,
    replacement_error: &mut Option<ReplacementQualificationFailure>,
    clock: &dyn MonotonicClock,
    work_cancellation: &CancellationView,
    replacement_reservation_valid_for: Option<Duration>,
    mut result_runtime: Option<&mut ResultRuntime>,
    root_terminal_receiver: &mut Option<mpsc::Receiver<RootTerminalObservation>>,
    native_actions: Option<
        &novarocks_workload_control::joint_admission::NativeAsyncActionAuthority,
    >,
    command: ActorCommand,
) {
    match command {
        ActorCommand::SuperviseInternalConsumer {
            source,
            receiver,
            reply,
        } => {
            let Some(runtime) = result_runtime else {
                let _ = reply.send(Err(LogicalExecutionActorError::WrongPhase));
                return;
            };
            internal_ready::supervise_exit(state, runtime, source, receiver, reply);
        }
        ActorCommand::BindInternalConsumer(binding) => {
            let Some(runtime) = result_runtime else {
                let _ = binding
                    .reply
                    .send(Err(LogicalExecutionActorError::WrongPhase));
                return;
            };
            internal_ready::bind(state, runtime, binding);
        }
        ActorCommand::DeliverInternalEnd(end) => {
            let Some(runtime) = result_runtime else {
                let _ = end.reply.send(Err(LogicalExecutionActorError::WrongPhase));
                return;
            };
            internal_ready::queue_end(state, runtime, end);
        }
        ActorCommand::AcceptComputationReady(proposal) => {
            let Some(runtime) = result_runtime else {
                let _ = proposal
                    .reply
                    .send(Err(LogicalExecutionActorError::WrongPhase));
                return;
            };
            internal_ready::propose(state, attempts, runtime, proposal);
        }
        ActorCommand::FailInternalComputation {
            source,
            error,
            reply,
        } => {
            let Some(runtime) = result_runtime else {
                let _ = reply.send(Err(LogicalExecutionActorError::WrongPhase));
                return;
            };
            internal_ready::fail(state, attempts, runtime, source, error, reply);
        }
        ActorCommand::TakeReplacementAdmissionEvidence { activation, reply } => {
            let result = (|| {
                let current = state.attempt_capability(activation.execution())?;
                if identity_of(&current) != activation {
                    return Err(LogicalExecutionActorError::StaleAuthority);
                }
                validate_attempt_command(state, &current)?;
                if !matches!(
                    state.phase(),
                    ExecutionPhase::Instantiating { execution, .. }
                        if execution == activation.execution()
                ) {
                    return Err(LogicalExecutionActorError::WrongPhase);
                }
                let attempt = attempts
                    .get_mut(&activation.execution())
                    .ok_or(LogicalExecutionActorError::WrongExecution)?;
                Ok(attempt
                    .active_resources
                    .as_mut()
                    .and_then(ActiveReplacementResources::take_admissions))
            })();
            if let Err(Ok(Some(admissions))) = reply.send(result) {
                let restored = attempts
                    .get_mut(&activation.execution())
                    .and_then(|attempt| attempt.active_resources.as_mut())
                    .is_some_and(|resources| resources.restore_admissions(admissions).is_ok());
                if !restored {
                    conclude_failed(state, "the attempt's admissions could not be restored");
                }
            }
        }
        ActorCommand::Activate { readiness, reply } => {
            let (capability, lifetime, mailbox_liveness) = readiness.into_parts();
            match validate_attempt_command(state, &capability)
                .and_then(|()| state.mark_running(&capability).map_err(Into::into))
            {
                Ok(()) => {
                    let running = RunningAttemptPermit::new(capability, lifetime, mailbox_liveness);
                    if let Err(response) = reply.send(Ok(running)) {
                        drop(response);
                    }
                }
                Err(error) => {
                    settle_refused_attempt(&lifetime, error);
                    let _ = reply.send(Err(error));
                }
            }
        }
        ActorCommand::InitializationFailed {
            permit,
            error,
            reply,
        } => {
            settle_terminal(
                state,
                permit.into_parts(),
                LogicalConclusion::Failed,
                || {
                    if let (Some(runtime), Some(error)) = (result_runtime, error) {
                        runtime.accepted_error = Some(error);
                    }
                },
                reply,
            );
        }
        ActorCommand::InitializationCancelled { permit, reply } => {
            settle_terminal(
                state,
                permit.into_parts(),
                LogicalConclusion::Cancelled,
                || {},
                reply,
            );
        }
        ActorCommand::Completed { permit, reply } => {
            let execution = permit.identity().execution();
            let parts = permit.into_parts();
            if let Err(error) = validate_attempt_command(state, &parts.0) {
                settle_refused_attempt(&parts.1, error);
                let _ = reply.send(Err(error));
                return;
            }
            let Some(attempt) = attempts.get_mut(&execution) else {
                let _ = reply.send(Err(LogicalExecutionActorError::WrongExecution));
                return;
            };
            match attempt.ensure_success_ready() {
                Ok(()) => {
                    attempt.establish.revoke_issue_authority();
                    settle_terminal(state, parts, LogicalConclusion::Succeeded, || {}, reply);
                }
                Err(error) => {
                    establish_error.get_or_insert(error);
                    attempt.record_establish_error(error);
                    attempt.establish.revoke_issue_authority();
                    settle_terminal_with_error(state, parts, error.into(), reply);
                }
            }
        }
        ActorCommand::Failed {
            permit,
            error,
            reply,
        } => {
            let execution = permit.identity().execution();
            let parts = permit.into_parts();
            if result_runtime
                .as_ref()
                .is_some_and(|runtime| runtime.internal.is_some())
                && validate_attempt_command(state, &parts.0).is_ok()
            {
                revoke_all_establish_authority(attempts);
            }
            settle_terminal(
                state,
                parts,
                LogicalConclusion::Failed,
                || {
                    if let Some(attempt) = attempts.get_mut(&execution) {
                        attempt.establish.revoke_issue_authority();
                    }
                    if let (Some(runtime), Some(error)) = (result_runtime.as_deref_mut(), error) {
                        runtime.accepted_error = Some(error);
                    }
                },
                reply,
            );
        }
        ActorCommand::DeliverRootSegment {
            activation,
            sequence,
            segment,
            client_rows,
            rows,
            resident_window,
            reply,
        } => {
            // Admit one ordered item at a time, never outside the attempt's delivery gate.
            if let Some(conclusion) = state.conclusion() {
                drop(segment);
                let _ = reply.send(Err(LogicalExecutionActorError::ExecutionConcluded(
                    conclusion,
                )));
                return;
            }
            let Some(runtime) = result_runtime else {
                drop(segment);
                let _ = reply.send(Err(LogicalExecutionActorError::WrongPhase));
                return;
            };
            if runtime.attempt_decision_pending == Some(activation) {
                drop(segment);
                let _ = reply.send(Err(LogicalExecutionActorError::RootAttemptTerminal));
                return;
            }
            if runtime
                .root_success
                .as_ref()
                .is_some_and(|gate| gate.activation == activation && gate.final_eos_ack.is_some())
            {
                if runtime.internal.is_some() {
                    revoke_all_establish_authority(attempts);
                }
                drop(segment);
                conclude_result_failure(
                    state,
                    runtime,
                    "a root segment arrived outside its delivery gate",
                    result_protocol_failure("a root segment arrived outside its delivery gate"),
                );
                reply_result_failure(state, reply);
                return;
            }
            let activation_error = verify_running_activation(state, activation).err();
            if activation_error.is_some()
                || runtime.pending.is_some()
                || runtime.deferred_internal.is_some()
                || matches!(
                    runtime.in_flight,
                    Some(InFlightResult::Batch { .. } | InFlightResult::End { .. })
                )
            {
                drop(segment);
                let error =
                    activation_error.unwrap_or(LogicalExecutionActorError::ResultProtocolViolation);
                let _ = reply.send(Err(error));
                return;
            }
            if sequence.next().is_none() {
                if runtime.internal.is_some() {
                    revoke_all_establish_authority(attempts);
                }
                drop(segment);
                conclude_result_failure(
                    state,
                    runtime,
                    "a root segment sequence overflowed",
                    result_protocol_failure("a root segment sequence overflowed"),
                );
                reply_result_failure(state, reply);
                return;
            }
            let expected = crate::api::ResultRowCarrier::Relayed {
                kind: segment.kind(),
                client_rows: client_rows.map(|(profile, _)| profile),
            };
            if runtime.carrier != expected {
                if runtime.internal.is_some() {
                    revoke_all_establish_authority(attempts);
                }
                drop(segment);
                conclude_result_failure(
                    state,
                    runtime,
                    "a root segment does not match the row carrier this result declared",
                    result_protocol_failure(
                        "a root segment does not match the row carrier this result declared",
                    ),
                );
                reply_result_failure(state, reply);
                return;
            }
            let (delivery, receipt) = match RootSegmentDelivery::try_new(
                activation.execution(),
                sequence,
                segment,
                client_rows,
                rows,
            ) {
                Ok(delivery) => delivery,
                Err(error) => {
                    if runtime.internal.is_some() {
                        revoke_all_establish_authority(attempts);
                    }
                    conclude_result_failure(
                        state,
                        runtime,
                        "a root segment could not be prepared for delivery",
                        error,
                    );
                    reply_result_failure(state, reply);
                    return;
                }
            };
            let delivery = delivery.with_resident_window(resident_window);
            if let Err(error) = start_schema_delivery(state, runtime, activation) {
                if runtime.internal.is_some() {
                    revoke_all_establish_authority(attempts);
                }
                drop(delivery);
                conclude_result_failure(
                    state,
                    runtime,
                    "schema delivery could not be started",
                    result_actor_failure(error),
                );
                reply_result_failure(state, reply);
                return;
            }
            runtime.pending = Some(PendingResult::Batch(PendingResultBatch {
                activation,
                sequence,
                delivery,
                receipt,
                reply,
            }));
        }
        ActorCommand::BindRootResult {
            activation,
            root,
            terminal_receiver,
            reply,
        } => {
            let Some(runtime) = result_runtime else {
                let _ = reply.send(Err(LogicalExecutionActorError::WrongPhase));
                return;
            };
            if let Some(conclusion) = state.conclusion() {
                let _ = reply.send(Err(LogicalExecutionActorError::ExecutionConcluded(
                    conclusion,
                )));
                return;
            }
            if verify_running_activation(state, activation).is_err()
                || root.query_execution_id() != activation.execution()
            {
                let _ = reply.send(Err(LogicalExecutionActorError::StaleAuthority));
                return;
            }
            if runtime.internal.as_ref().is_some_and(|internal| {
                !internal.consumer.as_ref().is_some_and(|record| {
                    record.source.activation == activation && record.source.root == root
                })
            }) {
                let _ = reply.send(Err(LogicalExecutionActorError::StaleAuthority));
                return;
            }
            match runtime.root_success.as_ref() {
                Some(gate) if gate.activation == activation && gate.root == root => {
                    let _ = reply.send(Err(LogicalExecutionActorError::WrongPhase));
                }
                Some(gate) if gate.activation == activation => {
                    if runtime.internal.is_some() {
                        revoke_all_establish_authority(attempts);
                    }
                    conclude_result_failure(
                        state,
                        runtime,
                        "the delivery gate was closed while its activation was still current",
                        result_protocol_failure(
                            "the delivery gate was closed while its activation was still current",
                        ),
                    );
                    reply_result_failure(state, reply);
                }
                _ => {
                    runtime.root_success = Some(RootSuccessGate {
                        activation,
                        root,
                        status: None,
                        terminal_failure: RootTerminalFailure::Unspecified,
                        final_eos_ack: None,
                        root_output_rows: None,
                    });
                    *root_terminal_receiver = Some(terminal_receiver);
                    let _ = reply.send(Ok(()));
                }
            }
        }
        ActorCommand::FreezeResultAttemptFailure {
            activation,
            error,
            reply,
        } => {
            let Some(runtime) = result_runtime else {
                let _ = reply.send(Err(LogicalExecutionActorError::WrongPhase));
                return;
            };
            if let Some(conclusion) = state.conclusion() {
                let _ = reply.send(Err(LogicalExecutionActorError::ExecutionConcluded(
                    conclusion,
                )));
                return;
            }
            if verify_running_activation(state, activation).is_err() {
                let _ = reply.send(Err(LogicalExecutionActorError::StaleAuthority));
                return;
            }
            runtime.attempt_error = Some(AttemptResultFailure { activation, error });
            if runtime.internal.is_some() {
                runtime.attempt_decision_pending = Some(activation);
                internal_ready::retire(runtime, activation);
                let _ = reply.send(Ok(()));
                return;
            }
            let protocol_may_own_rows = matches!(
                runtime.in_flight,
                Some(InFlightResult::Batch { .. } | InFlightResult::End { .. })
            );
            if state.output_visible() || protocol_may_own_rows {
                fail_result_observation(state, attempts, runtime);
                if state.conclusion().is_none() {
                    conclude_failed(
                        state,
                        "result observation failed after rows were already visible",
                    );
                }
                let _ = reply.send(Err(LogicalExecutionActorError::ExecutionConcluded(
                    LogicalConclusion::Failed,
                )));
            } else {
                runtime.attempt_decision_pending = Some(activation);
                if let Some(pending) = runtime.pending.take() {
                    reject_pending_result(state, pending);
                }
                let _ = reply.send(Ok(()));
            }
        }
        ActorCommand::ObserveRootStatus {
            activation,
            root,
            status,
            reply,
        } => {
            let Some(runtime) = result_runtime else {
                let _ = reply.send(Err(LogicalExecutionActorError::WrongPhase));
                return;
            };
            apply_root_status_observation(
                state,
                attempts,
                runtime,
                activation,
                root,
                status,
                RootTerminalFailure::Unspecified,
                reply,
            );
        }
        ActorCommand::ObserveRootEosAck {
            activation,
            root,
            expected_root,
            sequence,
            root_output_rows,
            reply,
        } => {
            let Some(runtime) = result_runtime else {
                let _ = reply.send(Err(LogicalExecutionActorError::WrongPhase));
                return;
            };
            if let Some(conclusion) = state.conclusion() {
                let _ = reply.send(Err(LogicalExecutionActorError::ExecutionConcluded(
                    conclusion,
                )));
                return;
            }
            if verify_result_observation_activation(state, activation).is_err() {
                let _ = reply.send(Err(LogicalExecutionActorError::StaleAuthority));
                return;
            }
            if matches!(
                runtime.carrier,
                crate::api::ResultRowCarrier::Relayed { .. }
            ) != root_output_rows.is_some()
            {
                fail_result_observation(state, attempts, runtime);
                reply_result_failure(state, reply);
                return;
            }
            let expected = ResultPacketSequence::new(state.accepted_result_packets());
            if sequence != expected
                || (sequence.get() > 0
                    && state.delivered_result_through() != Some(sequence.get().saturating_sub(1)))
            {
                fail_result_observation(state, attempts, runtime);
                reply_result_failure(state, reply);
                return;
            }
            let Some(gate) = runtime.root_success.as_mut() else {
                let _ = reply.send(Err(LogicalExecutionActorError::WrongPhase));
                return;
            };
            if gate.activation != activation || gate.root != expected_root || root != expected_root
            {
                if runtime.internal.is_some() {
                    revoke_all_establish_authority(attempts);
                }
                conclude_failed(
                    state,
                    "a result poll named an activation or root this execution does not own",
                );
                reply_result_failure(state, reply);
                return;
            }
            match gate.final_eos_ack {
                None => {
                    gate.final_eos_ack = Some(sequence);
                    gate.root_output_rows = root_output_rows;
                    let _ = reply.send(Ok(()));
                }
                Some(held) if held == sequence && gate.root_output_rows == root_output_rows => {
                    let _ = reply.send(Ok(()));
                }
                Some(_) => {
                    fail_result_observation(state, attempts, runtime);
                    reply_result_failure(state, reply);
                }
            }
        }
        ActorCommand::FinishResultStream {
            permit,
            withdrawals,
            reply,
        } => {
            let activation = permit.identity();
            if result_runtime
                .as_ref()
                .is_some_and(|runtime| runtime.internal.is_some())
                && permit
                    .capability
                    .as_ref()
                    .is_some_and(|capability| validate_attempt_command(state, capability).is_ok())
            {
                // This consumed handoff cannot return to an Internal source.
                // Fence it before any refusal concludes or drops its custody.
                revoke_all_establish_authority(attempts);
            }
            let Some(runtime) = result_runtime else {
                conclude_consumed_handoff_as_failed(state, permit, reply);
                return;
            };
            if verify_running_activation(state, activation).is_err()
                || !attempts.contains_key(&activation.execution())
                || !matches!(
                    runtime.root_success.as_ref(),
                    Some(gate) if gate.activation == activation
                )
                || runtime.finish_waiter.is_some()
                || matches!(runtime.pending, Some(PendingResult::End(_)))
                || matches!(runtime.in_flight, Some(InFlightResult::End { .. }))
            {
                conclude_consumed_handoff_as_failed(state, permit, reply);
                return;
            }
            let root = runtime
                .root_success
                .as_ref()
                .expect("checked root gate")
                .root;
            if withdrawals.iter().any(|fact| {
                fact.activation() != activation
                    || fact.root() != root
                    || fact.context().query_execution_id() != activation.execution()
                    || fact.context().backend_process_id() == root.backend_process_id()
            }) {
                conclude_consumed_handoff_as_failed(state, permit, reply);
                return;
            }
            let contexts = withdrawals
                .iter()
                .map(|fact| fact.context())
                .collect::<BTreeSet<_>>();
            let attempt = attempts
                .get_mut(&activation.execution())
                .expect("checked attempt");
            if attempt
                .establish
                .record_result_withdrawals(&contexts)
                .is_err()
            {
                conclude_consumed_handoff_as_failed(state, permit, reply);
                return;
            }
            if let Some(attempt) = attempts.get_mut(&activation.execution()) {
                attempt.establish.revoke_issue_authority();
            }
            if start_schema_delivery(state, runtime, activation).is_err() {
                conclude_consumed_handoff_as_failed(state, permit, reply);
                return;
            }
            runtime.finish_waiter = Some(PendingResultFinish { permit, reply });
        }
        ActorCommand::AwaitWorkCancellation { permit, reply } => {
            if state.conclusion().is_none() {
                if let Some(reason) = work_cancellation.reason() {
                    revoke_all_establish_authority(attempts);
                    conclude_for_work_cancellation(
                        state,
                        replacement.as_ref(),
                        result_runtime.as_deref_mut(),
                        &reason,
                    );
                } else {
                    if result_runtime
                        .as_ref()
                        .is_some_and(|runtime| runtime.internal.is_some())
                    {
                        revoke_all_establish_authority(attempts);
                    }
                    conclude_failed(
                        state,
                        "the attempt terminated without a reason this execution could use",
                    );
                }
            }
            reply_consumed_handoff_actual(state, permit, reply, false);
        }
        ActorCommand::BeginReplacement {
            permit,
            failure,
            pre_ready_replacement,
            terminal_error,
            replacement: replacement_execution,
            replacement_contexts,
            reply,
        } => {
            let activation = permit.identity();
            let (capability, lifetime, mailbox_liveness) = permit.into_parts();
            if pre_ready_replacement.as_ref().is_some_and(|witness| {
                witness.context().query_execution_id() != activation.execution()
                    || !terminal_error
                        .as_ref()
                        .and_then(QueryExecutionError::execution_failure)
                        .is_some_and(|input| witness.matches_input(input))
            }) {
                settle_refused_attempt(&lifetime, LogicalExecutionActorError::WrongExecution);
                let _ = reply.send(Err(LogicalExecutionActorError::WrongExecution));
                return;
            }
            if let Err(error) = validate_attempt_command(state, &capability)
                .and_then(|()| verify_running_activation(state, activation))
            {
                settle_refused_attempt(&lifetime, error);
                let _ = reply.send(Err(error));
                return;
            }
            if let Some(runtime) = result_runtime.as_deref_mut()
                && matches!(
                    runtime.pending.as_ref(),
                    Some(PendingResult::Batch(batch)) if batch.activation.execution() == activation.execution()
                )
                && let Some(pending) = runtime.pending.take()
            {
                reject_pending_result(state, pending);
            }
            if let Some(reason) = work_cancellation.reason() {
                revoke_all_establish_authority(attempts);
                conclude_for_work_cancellation(
                    state,
                    replacement.as_ref(),
                    result_runtime.as_deref_mut(),
                    &reason,
                );
                let error = state.conclusion().map_or(
                    LogicalExecutionActorError::WrongPhase,
                    LogicalExecutionActorError::ExecutionConcluded,
                );
                settle_refused_attempt(&lifetime, error);
                let _ = reply.send(Err(error));
                return;
            }
            let preparation = validate_contexts(replacement_execution, &replacement_contexts)
                .and_then(|contexts| {
                    let failed = attempts
                        .get_mut(&activation.execution())
                        .ok_or(LogicalExecutionActorError::WrongExecution)?;
                    failed.establish.revoke_issue_authority();
                    let failed_contexts = failed.establish.required_contexts();
                    begin_or_refresh_stand_down(
                        failed,
                        ContextStandDownCause::LogicalExecutionFailed,
                    )?;
                    Ok((contexts, failed_contexts))
                });
            let (contexts, failed_contexts) = match preparation {
                Ok(preparation) => preparation,
                Err(error) => {
                    if result_runtime
                        .as_ref()
                        .is_some_and(|runtime| runtime.internal.is_some())
                    {
                        revoke_all_establish_authority(attempts);
                    }
                    let _ = state.conclude(&capability, LogicalConclusion::Failed);
                    lifetime.settle();
                    let _ = reply.send(Err(error));
                    return;
                }
            };
            // Unknown parent outcomes can settle after the observation check.
            // Stand-down preparation drains this same original ledger. Read
            // it afterwards, at final acceptance, so a late unrelated refusal
            // cannot borrow the earlier open observation window.
            let correlated_pre_ready_replacement = pre_ready_replacement.as_ref().filter(|_| {
                terminal_error
                    .as_ref()
                    .and_then(QueryExecutionError::execution_failure)
                    .is_some_and(|input| {
                        pre_ready_refusal_matches_establish(attempts, activation, input)
                    })
            });
            let token = match state.begin_replacement(
                &capability,
                replacement_execution,
                failure,
                correlated_pre_ready_replacement,
                false,
            ) {
                Ok(token) => token,
                Err(error) => {
                    if state
                        .conclude(&capability, LogicalConclusion::Failed)
                        .is_ok()
                        && let (Some(runtime), Some(error)) = (result_runtime, terminal_error)
                    {
                        runtime.accepted_error = Some(error);
                    }
                    lifetime.settle();
                    let _ = reply.send(Err(error.into()));
                    return;
                }
            };
            if let Some(runtime) = result_runtime.as_deref_mut() {
                internal_ready::retire(runtime, activation);
                if let Some(error) = terminal_error {
                    runtime.attempt_error = Some(AttemptResultFailure { activation, error });
                }
                runtime.root_success = None;
                runtime.attempt_decision_pending = None;
            }
            root_terminal_receiver.take();
            let identity = ReplacementQualificationIdentity::new(
                activation.actor(),
                token.failed(),
                token.replacement(),
                token.eligibility_generation(),
            );
            let Some(reservation_valid_for) = replacement_reservation_valid_for else {
                let _ = state.conclude_replacement(&token, LogicalConclusion::Failed);
                lifetime.settle();
                let _ = reply.send(Err(LogicalExecutionActorError::InvariantViolation));
                return;
            };
            let issued_at = clock.now();
            let conservative_expiry = issued_at.saturating_add(reservation_valid_for);
            let (effect_cancellation, _) = watch::channel(false);
            let request = Arc::new(ReplacementQualificationRequest::new(
                identity,
                failed_contexts,
                contexts,
                issued_at,
                conservative_expiry,
                native_actions.cloned(),
            ));
            *replacement = Some(ReplacementRuntime {
                token,
                identity,
                request,
                effect_cancellation,
                effect_dispatched: false,
                effect_settled: false,
                effect_backpressured: false,
                activation: None,
                reservation: None,
                effect_dispatched_at: None,
                conservative_expiry: Some(conservative_expiry),
                reservation_valid_for,
                stale_usage_accounted: true,
                accepted_failure: None,
            });
            let qualification = ReplacementQualification {
                identity,
                lifetime: Some(Arc::clone(&lifetime)),
                mailbox_liveness: Some(mailbox_liveness),
            };
            if let Err(response) = reply.send(Ok(qualification)) {
                drop(response);
            }
        }
        ActorCommand::ActivateReplacement {
            qualification,
            reply,
        } => {
            let (identity, lifetime, mailbox_liveness) = qualification.into_parts();
            let Some(active) = replacement.as_mut() else {
                lifetime.abandon();
                let _ = reply.send(Err(LogicalExecutionActorError::WrongPhase));
                return;
            };
            if identity != active.identity {
                lifetime.abandon();
                let _ = reply.send(Err(LogicalExecutionActorError::StaleAuthority));
                return;
            }
            if let Some(conclusion) = state.conclusion() {
                lifetime.settle();
                let _ = reply.send(Err(replacement_terminal_disposition(active, conclusion)));
                return;
            }
            if let Some(error) = *replacement_error {
                lifetime.settle();
                let _ = reply.send(Err(
                    LogicalExecutionActorError::ReplacementQualificationFailed(error),
                ));
                return;
            }
            if active.activation.is_some() {
                lifetime.abandon();
                let _ = reply.send(Err(LogicalExecutionActorError::StaleAuthority));
                return;
            }
            active.activation = Some(PendingReplacementActivation {
                lifetime,
                mailbox_liveness,
                reply,
            });
        }
        ActorCommand::MayObservePreReadyReplacement {
            activation,
            refusal,
            reply,
        } => {
            let result = verify_running_activation(state, activation).map(|()| {
                pre_ready_refusal_matches_establish(attempts, activation, &refusal)
                    && work_cancellation.reason().is_none()
                    && state.may_observe_pre_ready_replacement(false)
            });
            let _ = reply.send(result);
        }
        ActorCommand::BeginAdmissionIssue {
            activation,
            request,
            reply,
        } => {
            let result = verify_running_activation(state, activation)
                .and_then(|()| {
                    attempts
                        .get(&activation.execution())
                        .ok_or(LogicalExecutionActorError::WrongExecution)?
                        .establish_error
                        .map_or(Ok(()), |error| Err(error.into()))
                })
                .and_then(|()| {
                    attempts
                        .get_mut(&activation.execution())
                        .ok_or(LogicalExecutionActorError::WrongExecution)?
                        .establish
                        .begin_admission_issue(activation, request, clock.now())
                        .map_err(Into::into)
                });
            let _ = reply.send(result);
        }
        ActorCommand::SettleAdmissionIssue {
            activation,
            admission_issue,
            settlement,
            reply,
        } => {
            let result = attempts
                .get(&activation.execution())
                .ok_or(LogicalExecutionActorError::WrongExecution)
                .and_then(|attempt| verify_actor_activation(attempt.activation, activation))
                .and_then(|()| {
                    attempts
                        .get_mut(&activation.execution())
                        .ok_or(LogicalExecutionActorError::WrongExecution)?
                        .establish
                        .settle_admission_issue(
                            activation,
                            admission_issue,
                            settlement,
                            clock.now(),
                        )
                        .map_err(Into::into)
                });
            if let Err(LogicalExecutionActorError::Establish(error)) = result {
                establish_error.get_or_insert(error);
                if let Some(attempt) = attempts.get_mut(&activation.execution()) {
                    attempt.record_establish_error(error);
                    attempt.establish.revoke_issue_authority();
                }
                // As with an ordinary Establish refusal, the Native owner
                // receives the original typed error and determines whether it
                // preceded its accepted root cut. Required contexts are still
                // checked at every success entrance. Protocol faults cannot
                // acquire this result-only exemption.
                if error != EstablishIssueError::AdmissionRejected
                    && matches!(state.phase(), ExecutionPhase::Running { execution, .. } if execution == activation.execution())
                {
                    if let Some(runtime) = result_runtime {
                        conclude_result_failure(
                            state,
                            runtime,
                            "settling this attempt's admission issue failed",
                            result_actor_failure(error.into()),
                        );
                    } else {
                        // Completion callers receive the same typed error in
                        // the original reply below; there is no row channel.
                        conclude_failed(
                            state,
                            format!("settling this attempt's admission issue failed: {error}"),
                        );
                    }
                }
            }
            let _ = reply.send(result);
        }
        ActorCommand::AuthorizeEstablish {
            activation,
            request,
            native_compatibility_id,
            reply,
        } => {
            let result = verify_running_activation(state, activation)
                .and_then(|()| {
                    attempts
                        .get(&activation.execution())
                        .ok_or(LogicalExecutionActorError::WrongExecution)?
                        .establish_error
                        .map_or(Ok(()), |error| Err(error.into()))
                })
                .and_then(|()| {
                    attempts
                        .get_mut(&activation.execution())
                        .ok_or(LogicalExecutionActorError::WrongExecution)?
                        .establish
                        .authorize_issue(activation, request, native_compatibility_id, clock.now())
                        .map_err(Into::into)
                });
            let _ = reply.send(result);
        }
        ActorCommand::ReauthorizeEstablish {
            activation,
            context,
            reply,
        } => {
            let result = verify_running_activation(state, activation)
                .and_then(|()| {
                    attempts
                        .get(&activation.execution())
                        .ok_or(LogicalExecutionActorError::WrongExecution)?
                        .establish_error
                        .map_or(Ok(()), |error| Err(error.into()))
                })
                .and_then(|()| {
                    attempts
                        .get_mut(&activation.execution())
                        .ok_or(LogicalExecutionActorError::WrongExecution)?
                        .establish
                        .reauthorize_issue(activation, context, clock.now())
                        .map_err(Into::into)
                });
            let _ = reply.send(result);
        }
        ActorCommand::EstablishSnapshot { context, reply } => {
            let snapshot = attempts
                .get(&context.query_execution_id())
                .and_then(|attempt| attempt.establish.snapshot(context));
            let _ = reply.send(Ok(snapshot));
        }
        ActorCommand::StandDownSnapshot { context, reply } => {
            let snapshot = attempts
                .get(&context.query_execution_id())
                .and_then(|attempt| attempt.stand_down.snapshot(context));
            let _ = reply.send(Ok(snapshot));
        }
        ActorCommand::RegistryContextConverged {
            context,
            convergence,
            reply,
        } => {
            let result = match attempts.get_mut(&context.query_execution_id()) {
                Some(attempt) => {
                    let result = attempt
                        .stand_down
                        .observe_registry_convergence(context, convergence)
                        .map_err(Into::into);
                    if let Err(LogicalExecutionActorError::StandDown(error)) = &result {
                        attempt.stand_down_error.get_or_insert(*error);
                    }
                    result
                }
                None => Err(LogicalExecutionActorError::WrongExecution),
            };
            if let Err(LogicalExecutionActorError::StandDown(error)) = &result {
                stand_down_error.get_or_insert(*error);
            }
            let _ = reply.send(result);
        }
        ActorCommand::RegistryJoinReadiness { .. } => {
            unreachable!("registry join readiness is handled by the actor loop")
        }
        ActorCommand::Snapshot { reply } => {
            let _ = reply.send(Ok(LogicalExecutionActorSnapshot {
                phase: state.phase(),
                conclusion: state.conclusion(),
                schema_emitted: state.schema_emitted(),
                output_visible: state.output_visible(),
                accepted_result_packets: state.accepted_result_packets(),
                delivered_result_through: state
                    .delivered_result_through()
                    .map(ResultPacketSequence::new),
                establish_error: *establish_error,
                stand_down_error: *stand_down_error,
                replacement_error: *replacement_error,
            }));
        }
    }
}

fn verify_actor_activation(
    expected: AttemptActivationIdentity,
    supplied: AttemptActivationIdentity,
) -> Result<(), LogicalExecutionActorError> {
    if supplied.execution() != expected.execution() {
        return Err(LogicalExecutionActorError::WrongExecution);
    }
    if supplied != expected {
        return Err(LogicalExecutionActorError::StaleAuthority);
    }
    Ok(())
}

fn validate_contexts(
    execution: QueryExecutionId,
    contexts: &[QueryContextRef],
) -> Result<Arc<[QueryContextRef]>, LogicalExecutionActorError> {
    let distinct: BTreeSet<_> = contexts.iter().copied().collect();
    if distinct.len() != contexts.len()
        || distinct
            .iter()
            .any(|context| context.query_execution_id() != execution)
    {
        return Err(LogicalExecutionActorError::WrongExecution);
    }
    Ok(distinct.into_iter().collect::<Vec<_>>().into())
}

fn new_attempt_ledgers(
    activation: AttemptActivationIdentity,
    required_contexts: BTreeSet<QueryContextRef>,
    max_admission_issues_per_context: NonZeroUsize,
    max_establish_authorizations_per_context: NonZeroUsize,
    max_abort_authorizations_per_context: NonZeroUsize,
    active_resources: Option<ActiveReplacementResources>,
    work: &WorkScope,
) -> Result<AttemptLedgers, LogicalExecutionActorError> {
    if required_contexts
        .iter()
        .any(|context| context.query_execution_id() != activation.execution())
    {
        return Err(LogicalExecutionActorError::WrongExecution);
    }
    let establish = EstablishIssueLedger::new(
        required_contexts.clone(),
        max_admission_issues_per_context,
        max_establish_authorizations_per_context,
    );
    let stand_down =
        ContextStandDownLedger::new(required_contexts, max_abort_authorizations_per_context)?;
    let attempt_obligation = work
        .register_obligation(
            attempt_obligation_key(activation.actor(), activation.execution()),
            ObligationKind::RunningWork,
        )
        .map_err(|_| LogicalExecutionActorError::InvariantViolation)?;
    Ok(AttemptLedgers {
        activation,
        establish,
        establish_error: None,
        establish_protocol_error: None,
        stand_down,
        stand_down_error: None,
        pending_aborts: BTreeMap::new(),
        abort_backpressured: false,
        attempt_obligation: Some(attempt_obligation),
        active_resources,
    })
}

fn drive_abort_effect(
    stand_down: &mut ContextStandDownLedger,
    port: Option<&dyn AbortQueryContextEffectPort>,
    pending: &mut BTreeMap<QueryContextRef, AbortQueryContextIssuePermit>,
    backpressured: &mut bool,
    now: MonotonicInstant,
) -> Result<(), ContextStandDownError> {
    let Some(port) = port else {
        return Ok(());
    };
    let pending_contexts: Vec<_> = pending.keys().copied().collect();
    for context in pending_contexts {
        let permit = pending
            .remove(&context)
            .expect("pending Abort context was collected from the same map");
        match permit.try_submit(port)? {
            AbortQueryContextIssueSubmit::Accepted => {}
            AbortQueryContextIssueSubmit::Backpressured(permit) => {
                pending.insert(context, permit);
            }
        }
    }

    while let Some(permit) = stand_down.authorize_next_at(now)? {
        let context = permit.identity().context();
        match permit.try_submit(port)? {
            AbortQueryContextIssueSubmit::Accepted => {}
            AbortQueryContextIssueSubmit::Backpressured(permit) => {
                if pending.insert(context, permit).is_some() {
                    return Err(ContextStandDownError::EventBackpressureInvariant);
                }
            }
        }
    }
    *backpressured = !pending.is_empty();
    Ok(())
}

fn cancel_pending_abort_issues(
    stand_down: &mut ContextStandDownLedger,
    pending: &mut BTreeMap<QueryContextRef, AbortQueryContextIssuePermit>,
    now: MonotonicInstant,
) -> Result<(), ContextStandDownError> {
    let pending = std::mem::take(pending);
    for (_, permit) in pending {
        permit.settle_definitely_unsent()?;
    }
    stand_down.drain_events_at(now)?;
    Ok(())
}

async fn wait_for_abort_retry(clock: &dyn MonotonicClock, retry_at: Option<MonotonicInstant>) {
    if let Some(retry_at) = retry_at {
        tokio::time::sleep(retry_at.saturating_duration_since(clock.now())).await;
    } else {
        std::future::pending::<()>().await;
    }
}

async fn wait_for_abort_capacity(
    capacity: &mut Option<watch::Receiver<u64>>,
) -> Result<(), ContextStandDownError> {
    if let Some(capacity) = capacity {
        capacity
            .changed()
            .await
            .map_err(|_| ContextStandDownError::EffectCapacityClosed)
    } else {
        std::future::pending::<()>().await;
        Ok(())
    }
}

async fn wait_for_replacement_capacity(
    capacity: &mut Option<watch::Receiver<u64>>,
) -> Result<(), ReplacementQualificationFailure> {
    if let Some(capacity) = capacity {
        capacity
            .changed()
            .await
            .map_err(|_| ReplacementQualificationFailure::EffectOwnerClosed)
    } else {
        std::future::pending::<()>().await;
        Ok(())
    }
}

async fn wait_for_replacement_expiry(clock: &dyn MonotonicClock, expiry: Option<MonotonicInstant>) {
    if let Some(expiry) = expiry {
        tokio::time::sleep(expiry.saturating_duration_since(clock.now())).await;
    } else {
        std::future::pending::<()>().await;
    }
}

fn actor_cleanup_complete(
    state: &LogicalExecutionState,
    attempts: &BTreeMap<QueryExecutionId, AttemptLedgers>,
    replacement: Option<&ReplacementRuntime>,
) -> bool {
    let attempt_settled = |attempt: &AttemptLedgers| {
        attempt.stand_down.started()
            && (attempt.stand_down.residual_resource_settled()
                || attempt.stand_down.remote_tracking_ended())
            && attempt.attempt_obligation.is_none()
            && attempt.active_resources.is_none()
    };
    let replacement_effect_settled = replacement
        .map(|replacement| {
            replacement.reservation.is_none()
                && (!replacement.effect_dispatched || replacement.effect_settled)
        })
        .unwrap_or(true);
    match state.conclusion() {
        Some(LogicalConclusion::Succeeded) => {
            attempts.values().all(attempt_settled) && replacement_effect_settled
        }
        Some(_) => attempts.values().all(attempt_settled) && replacement_effect_settled,
        None => false,
    }
}

/// Correlates facts only; recovery policy and the original Native witness
/// remain with their existing owners. Both opportunity and final acceptance
/// read this ledger so a late unknown-parent settlement cannot bypass it.
fn pre_ready_refusal_matches_establish(
    attempts: &BTreeMap<QueryExecutionId, AttemptLedgers>,
    activation: AttemptActivationIdentity,
    refusal: &crate::execution_error::ExecutionFailureInput,
) -> bool {
    let Some((context, operation_id, kind)) =
        crate::api::PreReadyProcessReplacementWitness::refusal_operation(refusal)
    else {
        return false;
    };
    if context.query_execution_id() != activation.execution() {
        return false;
    }
    let Some(attempt) = attempts.get(&activation.execution()) else {
        return false;
    };
    attempt.establish.snapshot(context).is_some_and(|snapshot| {
        snapshot.identity().is_some_and(|identity| {
            identity.activation() == activation
                && identity.context() == context
                && match kind {
                    novarocks_execution_contract::OperationKind::UpdateQueryContext => {
                        identity.operation_id() == operation_id
                    }
                    novarocks_execution_contract::OperationKind::CreateTask => matches!(
                        snapshot.state(),
                        super::EstablishIssueState::TransportOwned
                            | super::EstablishIssueState::TransportUnknown
                            | super::EstablishIssueState::WorkerSettled(
                                super::EstablishWorkerSettlement::Applied
                                    | super::EstablishWorkerSettlement::Rejected(
                                        novarocks_execution_contract::OperationOutcome::IdentityMismatch
                                    )
                            )
                    ),
                    _ => false,
                }
        })
    })
}

fn verify_running_activation(
    state: &LogicalExecutionState,
    activation: AttemptActivationIdentity,
) -> Result<(), LogicalExecutionActorError> {
    if !matches!(
        state.phase(),
        ExecutionPhase::Running { execution, .. } if execution == activation.execution()
    ) {
        return Err(LogicalExecutionActorError::WrongPhase);
    }
    let current = state.attempt_capability(activation.execution())?;
    if identity_of(&current) != activation {
        return Err(LogicalExecutionActorError::StaleAuthority);
    }
    Ok(())
}

fn apply_root_status_observation(
    state: &mut LogicalExecutionState,
    attempts: &mut BTreeMap<QueryExecutionId, AttemptLedgers>,
    runtime: &mut ResultRuntime,
    activation: AttemptActivationIdentity,
    root: TaskIdentity,
    status: TaskStatus,
    terminal_failure: RootTerminalFailure,
    reply: ActorReply<()>,
) {
    if let Some(conclusion) = state.conclusion() {
        let _ = reply.send(Err(LogicalExecutionActorError::ExecutionConcluded(
            conclusion,
        )));
        return;
    }
    if verify_result_observation_activation(state, activation).is_err() {
        let _ = reply.send(Err(LogicalExecutionActorError::StaleAuthority));
        return;
    }
    let Some(gate) = runtime.root_success.as_mut() else {
        let _ = reply.send(Err(LogicalExecutionActorError::WrongPhase));
        return;
    };
    if gate.activation != activation || gate.root != root || status.identity() != gate.root {
        if runtime.internal.is_some() {
            revoke_all_establish_authority(attempts);
        }
        conclude_or_interrupt_success(state, None, Some(runtime), LogicalConclusion::Failed);
        reply_result_failure(state, reply);
        return;
    }
    if gate.status.as_ref() == Some(&status) {
        let accepted_refinement = matches!(
            (&gate.terminal_failure, &terminal_failure),
            (
                RootTerminalFailure::Unspecified,
                RootTerminalFailure::Pending
            ) | (
                RootTerminalFailure::Unspecified | RootTerminalFailure::Pending,
                RootTerminalFailure::Authoritative(_)
            )
        );
        if !accepted_refinement {
            if gate.terminal_failure == terminal_failure {
                let response = if matches!(terminal_failure, RootTerminalFailure::Unspecified) {
                    Ok(())
                } else {
                    Err(LogicalExecutionActorError::RootAttemptTerminal)
                };
                let _ = reply.send(response);
                return;
            }
            fail_result_observation(state, attempts, runtime);
            reply_result_failure(state, reply);
            return;
        }
        gate.terminal_failure = terminal_failure.clone();
        if matches!(terminal_failure, RootTerminalFailure::Pending) {
            // A derived cause is not a final failure classification. Freeze
            // delivery and ACK progression even after visibility, then let
            // the authoritative refinement decide whether this becomes a
            // logical stream failure or a pre-visibility attempt decision.
            runtime.attempt_decision_pending = Some(activation);
            if let Some(pending) = runtime.pending.take() {
                reject_pending_result(state, pending);
            }
            let _ = reply.send(Err(LogicalExecutionActorError::RootAttemptTerminal));
            return;
        }
        if let RootTerminalFailure::Authoritative(error) = terminal_failure {
            runtime.attempt_error = Some(AttemptResultFailure {
                activation,
                error: *error,
            });
        }
        if state.output_visible() {
            fail_result_observation(state, attempts, runtime);
            let _ = reply.send(Err(LogicalExecutionActorError::ExecutionConcluded(
                LogicalConclusion::Failed,
            )));
        } else {
            runtime.attempt_decision_pending = Some(activation);
            if let Some(pending) = runtime.pending.take() {
                reject_pending_result(state, pending);
            }
            let _ = reply.send(Err(LogicalExecutionActorError::RootAttemptTerminal));
        }
        return;
    }
    let cursor = gate.status.as_ref().map_or_else(
        || TaskStatusCursor::unobserved(gate.root),
        |held| TaskStatusCursor::at(gate.root, held.version()),
    );
    match classify_observation(cursor, gate.status.as_ref(), &status) {
        StatusObservation::Ignore | StatusObservation::Idempotent => {
            let _ = reply.send(Ok(()));
            return;
        }
        StatusObservation::Accept => {}
        StatusObservation::VersionConflict
        | StatusObservation::IdentityMismatch(_)
        | StatusObservation::TerminalOverwrite
        | StatusObservation::InstallationRegression
        | StatusObservation::ConclusionOverwrite => {
            fail_result_observation(state, attempts, runtime);
            reply_result_failure(state, reply);
            return;
        }
    }
    if let Some(held) = gate.status.as_ref()
        && held.version().next() == Some(status.version())
        && !matches!(
            classify_observed_task_transition(held.state(), status.state()),
            ObservedTaskTransition::Advance | ObservedTaskTransition::SameState
        )
    {
        fail_result_observation(state, attempts, runtime);
        reply_result_failure(state, reply);
        return;
    }
    let terminal_state = status.state().is_failure()
        || status.state().is_cancellation()
        || status.state().is_abort()
        || !matches!(&terminal_failure, RootTerminalFailure::Unspecified);
    gate.status = Some(status);
    gate.terminal_failure = terminal_failure.clone();
    if terminal_state {
        let cause_pending = matches!(&terminal_failure, RootTerminalFailure::Pending);
        if let RootTerminalFailure::Authoritative(error) = terminal_failure {
            runtime.attempt_error = Some(AttemptResultFailure {
                activation,
                error: *error,
            });
        }
        if cause_pending {
            runtime.attempt_decision_pending = Some(activation);
            if let Some(pending) = runtime.pending.take() {
                reject_pending_result(state, pending);
            }
            let _ = reply.send(Err(LogicalExecutionActorError::RootAttemptTerminal));
            return;
        }
        if state.output_visible() {
            // Once data crossed the visibility boundary this attempt cannot
            // be replaced without duplicating a prefix. Fail the logical
            // stream and interrupt any protocol delivery immediately.
            fail_result_observation(state, attempts, runtime);
            reply_result_failure(state, reply);
        } else {
            // Freeze all pending and future data in the same actor turn that
            // accepts the terminal fact. The running-permit owner can now
            // choose replacement or a final logical failure without a queued
            // batch racing across the visibility boundary.
            runtime.attempt_decision_pending = Some(activation);
            if let Some(pending) = runtime.pending.take() {
                reject_pending_result(state, pending);
            }
            let _ = reply.send(Err(LogicalExecutionActorError::RootAttemptTerminal));
        }
    } else {
        let _ = reply.send(Ok(()));
    }
}

fn verify_result_observation_activation(
    state: &LogicalExecutionState,
    activation: AttemptActivationIdentity,
) -> Result<(), LogicalExecutionActorError> {
    if !matches!(
        state.phase(),
        ExecutionPhase::Running { execution, .. }
            | ExecutionPhase::FinishingSuccess { execution, .. }
                if execution == activation.execution()
    ) {
        return Err(LogicalExecutionActorError::WrongPhase);
    }
    let current = state.attempt_capability(activation.execution())?;
    if identity_of(&current) != activation {
        return Err(LogicalExecutionActorError::StaleAuthority);
    }
    Ok(())
}

fn fail_result_observation(
    state: &mut LogicalExecutionState,
    attempts: &mut BTreeMap<QueryExecutionId, AttemptLedgers>,
    runtime: &mut ResultRuntime,
) {
    if runtime.internal.is_some() {
        revoke_all_establish_authority(attempts);
    }
    if state.conclusion().is_none() {
        conclude_failed(state, "result observation failed");
    }
}

/// A terminal reply is a disposition of this exact actor-owned capability,
/// never a conclusion inferred later by the supervisor from a snapshot.
fn validate_attempt_command(
    state: &LogicalExecutionState,
    capability: &AttemptCapability,
) -> Result<(), LogicalExecutionActorError> {
    let activation = identity_of(capability);
    let known = state.attempt_capability(activation.execution())?;
    if identity_of(&known) != activation {
        return Err(LogicalExecutionActorError::StaleAuthority);
    }
    match state.conclusion() {
        Some(conclusion) => Err(LogicalExecutionActorError::ExecutionConcluded(conclusion)),
        None => Ok(()),
    }
}

fn settle_refused_attempt(lifetime: &PermitLifetime, error: LogicalExecutionActorError) {
    if matches!(error, LogicalExecutionActorError::ExecutionConcluded(_)) {
        lifetime.settle();
    } else {
        lifetime.abandon();
    }
}

fn settle_terminal(
    state: &mut LogicalExecutionState,
    (capability, lifetime, _mailbox_liveness): (
        AttemptCapability,
        Arc<PermitLifetime>,
        mpsc::Sender<ActorCommand>,
    ),
    conclusion: LogicalConclusion,
    on_accepted: impl FnOnce(),
    reply: ActorReply<LogicalConclusion>,
) {
    let result = validate_attempt_command(state, &capability)
        .and_then(|()| state.conclude(&capability, conclusion).map_err(Into::into))
        .map(|()| conclusion);
    if result.is_ok() {
        on_accepted();
        lifetime.settle();
    } else if let Err(error) = result {
        settle_refused_attempt(&lifetime, error);
    }
    let _ = reply.send(result);
}

fn settle_terminal_with_error(
    state: &mut LogicalExecutionState,
    (capability, lifetime, _mailbox_liveness): (
        AttemptCapability,
        Arc<PermitLifetime>,
        mpsc::Sender<ActorCommand>,
    ),
    error: LogicalExecutionActorError,
    reply: ActorReply<LogicalConclusion>,
) {
    match validate_attempt_command(state, &capability).and_then(|()| {
        state
            .conclude(&capability, LogicalConclusion::Failed)
            .map_err(Into::into)
    }) {
        Ok(()) => {
            lifetime.settle();
            let _ = reply.send(Err(error));
        }
        Err(state_error) => {
            settle_refused_attempt(&lifetime, state_error);
            let _ = reply.send(Err(state_error));
        }
    }
}

fn conclude_consumed_handoff_as_failed(
    state: &mut LogicalExecutionState,
    permit: RunningAttemptPermit,
    reply: ActorReply<LogicalConclusion>,
) {
    if state.conclusion().is_none() {
        let capability = permit
            .capability
            .as_ref()
            .expect("live running handoff retains its capability");
        if state
            .conclude(capability, LogicalConclusion::Failed)
            .is_err()
        {
            conclude_failed(state, "a consumed handoff could not be concluded as failed");
        }
    }
    reply_consumed_handoff_actual(state, permit, reply, true);
}

fn reply_consumed_handoff_actual(
    state: &LogicalExecutionState,
    permit: RunningAttemptPermit,
    reply: ActorReply<LogicalConclusion>,
    as_error: bool,
) {
    let (_capability, lifetime, _mailbox_liveness) = permit.into_parts();
    let Some(conclusion) = state.conclusion() else {
        lifetime.abandon();
        let _ = reply.send(Err(LogicalExecutionActorError::InvariantViolation));
        return;
    };
    lifetime.settle();
    let response = if as_error {
        Err(LogicalExecutionActorError::ExecutionConcluded(conclusion))
    } else {
        Ok(conclusion)
    };
    let _ = reply.send(response);
}

fn reply_result_failure(state: &LogicalExecutionState, reply: ActorReply<()>) {
    let error = state.conclusion().map_or(
        LogicalExecutionActorError::ResultDeliveryFailed,
        LogicalExecutionActorError::ExecutionConcluded,
    );
    let _ = reply.send(Err(error));
}

#[cfg(test)]
mod tests {
    use crate::api::ResultDelivery;
    mod internal_ready_tests {
        include!("actor/internal_ready_tests.rs");
    }

    use std::future::Future;
    use std::num::NonZeroU32;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::{Mutex, OnceLock};
    use std::task::Poll;
    use std::time::Duration;

    use super::super::{
        ActiveAttemptIsolationOwner, AttemptIsolationActivationFailure,
        AttemptIsolationReservation, EstablishIssueIdentity, EstablishIssueSubmit,
        EstablishTransportAdmission, EstablishTransportReservation, EstablishTransportSink,
        EstablishTransportSubmission, QualifiedReplacementReservation, QualifiedWorkerAdmission,
        ReplacementQualificationEffectReservation,
    };
    use super::*;
    use arrow::datatypes::DataType;
    use novarocks_execution_contract::{
        AbortCause, AdmissionEpochCapability, AdmissionTicketId, CodecOwnedContent,
        ConfidentialContent, ContentFingerprint, CredentialEpoch, CredentialLeaseId,
        CredentialUpdate, LeaseValidFor, OperationOutcome, QueryContextAdmissionTicketReceipt,
        QueryContextReceipt, QueryContextState, TaskOperationId, TaskOutputFacts, TaskState,
        TaskStatusVersion, TerminationDetail,
    };
    use novarocks_types::NativeCompatibilityId;
    use novarocks_types::identity::{
        AttemptId, BackendProcessId, FrontendProcessId, QueryId, StageId, TaskId,
    };
    use novarocks_workload_control::{
        ResourceConfig, WorkClass, WorkRequest, WorkloadConfig, WorkloadControl,
    };

    use crate::api::{QueryExecutionErrorKind, ResultField};

    #[derive(Debug)]
    struct SelectiveAbortPort {
        blocked: QueryContextRef,
        accepted: Arc<Mutex<Vec<QueryContextRef>>>,
        capacity: watch::Sender<u64>,
    }

    #[derive(Debug)]
    struct AcceptedAbortReservation {
        accepted: Arc<Mutex<Vec<QueryContextRef>>>,
    }

    impl super::super::AbortQueryContextEffectReservation for AcceptedAbortReservation {
        fn submit(self: Box<Self>, submission: super::super::AbortQueryContextEffectSubmission) {
            let context = submission.identity().context();
            self.accepted.lock().unwrap().push(context);
            submission
                .worker_settled(QueryContextReceipt::new(
                    context,
                    QueryContextState::Aborting,
                ))
                .unwrap();
        }
    }

    impl AbortQueryContextEffectPort for SelectiveAbortPort {
        fn subscribe_capacity(&self) -> watch::Receiver<u64> {
            self.capacity.subscribe()
        }

        fn try_reserve(
            &self,
            identity: super::super::AbortQueryContextIssueIdentity,
        ) -> super::super::AbortQueryContextEffectAdmission {
            if identity.context() == self.blocked {
                super::super::AbortQueryContextEffectAdmission::Backpressured
            } else {
                super::super::AbortQueryContextEffectAdmission::Admitted(Box::new(
                    AcceptedAbortReservation {
                        accepted: Arc::clone(&self.accepted),
                    },
                ))
            }
        }
    }

    #[derive(Debug)]
    struct ClosingMixedAbortPort {
        accepted: QueryContextRef,
        blocked: QueryContextRef,
        submissions: Arc<Mutex<Vec<super::super::AbortQueryContextEffectSubmission>>>,
        capacity: Mutex<Option<watch::Sender<u64>>>,
        reserve_calls: AtomicUsize,
    }

    impl ClosingMixedAbortPort {
        fn new(accepted: QueryContextRef, blocked: QueryContextRef) -> Self {
            let (capacity, _) = watch::channel(0);
            Self {
                accepted,
                blocked,
                submissions: Arc::default(),
                capacity: Mutex::new(Some(capacity)),
                reserve_calls: AtomicUsize::new(0),
            }
        }

        fn close_capacity(&self) {
            self.capacity.lock().unwrap().take();
        }
    }

    #[derive(Debug)]
    struct HoldingAbortReservation {
        submissions: Arc<Mutex<Vec<super::super::AbortQueryContextEffectSubmission>>>,
    }

    impl super::super::AbortQueryContextEffectReservation for HoldingAbortReservation {
        fn submit(self: Box<Self>, submission: super::super::AbortQueryContextEffectSubmission) {
            self.submissions.lock().unwrap().push(submission);
        }
    }

    impl AbortQueryContextEffectPort for ClosingMixedAbortPort {
        fn subscribe_capacity(&self) -> watch::Receiver<u64> {
            self.capacity
                .lock()
                .unwrap()
                .as_ref()
                .expect("test capacity is open during actor construction")
                .subscribe()
        }

        fn try_reserve(
            &self,
            identity: super::super::AbortQueryContextIssueIdentity,
        ) -> super::super::AbortQueryContextEffectAdmission {
            self.reserve_calls.fetch_add(1, Ordering::SeqCst);
            if identity.context() == self.accepted {
                super::super::AbortQueryContextEffectAdmission::Admitted(Box::new(
                    HoldingAbortReservation {
                        submissions: Arc::clone(&self.submissions),
                    },
                ))
            } else if identity.context() == self.blocked {
                super::super::AbortQueryContextEffectAdmission::Backpressured
            } else {
                super::super::AbortQueryContextEffectAdmission::Closed
            }
        }
    }

    #[derive(Debug)]
    struct ManualActorClock {
        now: Mutex<MonotonicInstant>,
    }

    impl ManualActorClock {
        fn new(now: MonotonicInstant) -> Self {
            Self {
                now: Mutex::new(now),
            }
        }

        fn set(&self, now: MonotonicInstant) {
            *self.now.lock().unwrap() = now;
        }
    }

    impl MonotonicClock for ManualActorClock {
        fn now(&self) -> MonotonicInstant {
            *self.now.lock().unwrap()
        }
    }

    #[derive(Debug)]
    struct DelayedQualificationPort {
        submissions: Arc<Mutex<Vec<ReplacementQualificationEffectSubmission>>>,
        capacity: watch::Sender<u64>,
    }

    impl Default for DelayedQualificationPort {
        fn default() -> Self {
            let (capacity, _) = watch::channel(0);
            Self {
                submissions: Arc::default(),
                capacity,
            }
        }
    }

    #[derive(Debug)]
    struct DelayedQualificationReservation {
        submissions: Arc<Mutex<Vec<ReplacementQualificationEffectSubmission>>>,
    }

    impl ReplacementQualificationEffectReservation for DelayedQualificationReservation {
        fn submit(self: Box<Self>, submission: ReplacementQualificationEffectSubmission) {
            self.submissions.lock().unwrap().push(submission);
        }
    }

    impl ReplacementQualificationEffectPort for DelayedQualificationPort {
        fn subscribe_capacity(&self) -> watch::Receiver<u64> {
            self.capacity.subscribe()
        }

        fn try_reserve(
            &self,
            _request: &ReplacementQualificationRequest,
        ) -> ReplacementQualificationEffectAdmission {
            ReplacementQualificationEffectAdmission::Admitted(Box::new(
                DelayedQualificationReservation {
                    submissions: Arc::clone(&self.submissions),
                },
            ))
        }
    }

    #[derive(Debug)]
    struct ClosingBackpressureQualificationPort {
        capacity: Mutex<Option<watch::Sender<u64>>>,
        reserve_calls: AtomicUsize,
    }

    impl Default for ClosingBackpressureQualificationPort {
        fn default() -> Self {
            let (capacity, _) = watch::channel(0);
            Self {
                capacity: Mutex::new(Some(capacity)),
                reserve_calls: AtomicUsize::new(0),
            }
        }
    }

    impl ClosingBackpressureQualificationPort {
        fn close(&self) {
            self.capacity.lock().unwrap().take();
        }
    }

    impl ReplacementQualificationEffectPort for ClosingBackpressureQualificationPort {
        fn subscribe_capacity(&self) -> watch::Receiver<u64> {
            self.capacity
                .lock()
                .unwrap()
                .as_ref()
                .expect("test capacity is open during actor construction")
                .subscribe()
        }

        fn try_reserve(
            &self,
            _request: &ReplacementQualificationRequest,
        ) -> ReplacementQualificationEffectAdmission {
            self.reserve_calls.fetch_add(1, Ordering::SeqCst);
            ReplacementQualificationEffectAdmission::Backpressured
        }
    }

    #[derive(Debug)]
    struct RecoverableBackpressureQualificationPort {
        available: AtomicBool,
        reserve_calls: AtomicUsize,
        submissions: Arc<AtomicUsize>,
        capacity: watch::Sender<u64>,
    }

    impl Default for RecoverableBackpressureQualificationPort {
        fn default() -> Self {
            let (capacity, _) = watch::channel(0);
            Self {
                available: AtomicBool::new(false),
                reserve_calls: AtomicUsize::new(0),
                submissions: Arc::new(AtomicUsize::new(0)),
                capacity,
            }
        }
    }

    impl RecoverableBackpressureQualificationPort {
        fn recover_capacity(&self) {
            self.available.store(true, Ordering::SeqCst);
            self.capacity.send_modify(|generation| *generation += 1);
        }
    }

    #[derive(Debug)]
    struct CountingQualificationReservation {
        submissions: Arc<AtomicUsize>,
    }

    impl ReplacementQualificationEffectReservation for CountingQualificationReservation {
        fn submit(self: Box<Self>, _submission: ReplacementQualificationEffectSubmission) {
            self.submissions.fetch_add(1, Ordering::SeqCst);
        }
    }

    impl ReplacementQualificationEffectPort for RecoverableBackpressureQualificationPort {
        fn subscribe_capacity(&self) -> watch::Receiver<u64> {
            self.capacity.subscribe()
        }

        fn try_reserve(
            &self,
            _request: &ReplacementQualificationRequest,
        ) -> ReplacementQualificationEffectAdmission {
            self.reserve_calls.fetch_add(1, Ordering::SeqCst);
            if self.available.load(Ordering::SeqCst) {
                ReplacementQualificationEffectAdmission::Admitted(Box::new(
                    CountingQualificationReservation {
                        submissions: Arc::clone(&self.submissions),
                    },
                ))
            } else {
                ReplacementQualificationEffectAdmission::Backpressured
            }
        }
    }

    #[derive(Debug, Default)]
    struct HoldingEstablishTransport {
        submission: Arc<Mutex<Option<EstablishTransportSubmission>>>,
    }

    #[derive(Debug)]
    struct HoldingEstablishReservation {
        submission: Arc<Mutex<Option<EstablishTransportSubmission>>>,
    }

    impl EstablishTransportReservation for HoldingEstablishReservation {
        fn submit(self: Box<Self>, submission: EstablishTransportSubmission) {
            let replaced = self.submission.lock().unwrap().replace(submission);
            assert!(replaced.is_none());
        }
    }

    impl EstablishTransportSink for HoldingEstablishTransport {
        fn try_reserve(&self, _identity: EstablishIssueIdentity) -> EstablishTransportAdmission {
            EstablishTransportAdmission::Admitted(Box::new(HoldingEstablishReservation {
                submission: Arc::clone(&self.submission),
            }))
        }
    }

    impl HoldingEstablishTransport {
        fn take(&self) -> EstablishTransportSubmission {
            self.submission.lock().unwrap().take().unwrap()
        }
    }

    #[derive(Debug)]
    struct FakeContent(ContentFingerprint);

    impl CodecOwnedContent for FakeContent {
        fn fingerprint(&self) -> ContentFingerprint {
            self.0
        }

        fn encoded_len(&self) -> usize {
            1
        }
    }

    struct FakeSecret;

    impl ConfidentialContent for FakeSecret {
        fn encoded_len(&self) -> usize {
            1
        }

        fn matches(&self, other: &dyn ConfidentialContent) -> bool {
            other.encoded_len() == 1
        }
    }

    fn execution(query: i64) -> QueryExecutionId {
        QueryExecutionId::new(QueryId::new(31, query), AttemptId::new(1).unwrap()).unwrap()
    }

    fn replacement(execution: QueryExecutionId, attempt: u64) -> QueryExecutionId {
        QueryExecutionId::new(execution.query_id(), AttemptId::new(attempt).unwrap()).unwrap()
    }

    fn root_task(execution: QueryExecutionId, tag: u32) -> TaskIdentity {
        TaskIdentity::new(
            execution,
            StageId::new(tag).unwrap(),
            TaskId::new(tag).unwrap(),
            BackendProcessId::new_v7(),
        )
    }

    fn root_status(root: TaskIdentity, version: u64, state: TaskState) -> TaskStatus {
        let termination = match state {
            TaskState::Aborting | TaskState::Aborted => {
                Some(TerminationDetail::Aborted(AbortCause::QueryFailed))
            }
            _ => None,
        };
        TaskStatus::try_new(
            root,
            TaskStatusVersion::new(version).unwrap(),
            state,
            termination,
            TaskOutputFacts::new(state == TaskState::Finished),
        )
        .unwrap()
    }

    fn test_governed_work() -> (WorkOwner, StagePermit) {
        static CONTROL: OnceLock<WorkloadControl> = OnceLock::new();
        let control = CONTROL.get_or_init(|| {
            let control = WorkloadControl::try_new(
                WorkloadConfig::default(),
                ResourceConfig {
                    total_bytes: 1 << 30,
                    control_bytes: 1 << 20,
                    per_scope_bytes: 1 << 28,
                },
            )
            .unwrap();
            control.mark_ready().unwrap();
            control
        });
        let root = control
            .try_begin_root(WorkRequest::new(WorkClass::Query))
            .unwrap();
        let scope = root.owner.scope();
        let stage = scope.try_acquire(Stage::Execution).unwrap();
        (root.owner, stage)
    }

    fn isolated_workload_control() -> WorkloadControl {
        let control = WorkloadControl::try_new(
            WorkloadConfig::default(),
            ResourceConfig {
                total_bytes: 1 << 20,
                control_bytes: 1 << 12,
                per_scope_bytes: (1 << 20) - (1 << 12),
            },
        )
        .unwrap();
        control.mark_ready().unwrap();
        control
    }

    #[test]
    fn cancellation_between_execution_admission_and_actor_install_settles_control() {
        let control = isolated_workload_control();
        let root = control
            .try_begin_root(WorkRequest::new(WorkClass::Query))
            .unwrap();
        let stage = root.owner.scope().try_acquire(Stage::Execution).unwrap();
        root.owner.cancel(CancellationReason::DeadlineExceeded);

        let result = LogicalExecutionActorConfig::single_attempt_read_rows(
            execution(10_003),
            NonZeroUsize::new(1).unwrap(),
            Vec::new(),
            NonZeroUsize::new(1).unwrap(),
            NonZeroUsize::new(1).unwrap(),
            root.owner,
            stage,
            result_schema(),
            NonZeroUsize::new(1).unwrap(),
            result_carrier(),
        );
        assert!(matches!(
            result,
            Err(LogicalExecutionActorError::InvariantViolation)
        ));
        root.business.release();

        assert_eq!(control.snapshot().root_responsibilities, 0);
        assert_eq!(control.snapshot().control_ready, 0);
    }

    fn test_governed_child_work(
        parent_deadline: Option<tokio::time::Instant>,
    ) -> (WorkOwner, WorkOwner, StagePermit) {
        let control = WorkloadControl::try_new(
            WorkloadConfig::default(),
            ResourceConfig {
                total_bytes: 1 << 30,
                control_bytes: 1 << 20,
                per_scope_bytes: 1 << 28,
            },
        )
        .unwrap();
        control.mark_ready().unwrap();
        let root = control
            .try_begin_root(WorkRequest {
                class: WorkClass::Query,
                deadline: parent_deadline,
            })
            .unwrap();
        let child = root
            .owner
            .scope()
            .child(WorkRequest::new(WorkClass::Query))
            .unwrap();
        let stage = child.scope().try_acquire(Stage::Execution).unwrap();
        (root.owner, child, stage)
    }

    #[derive(Debug)]
    struct TestIsolationReservation {
        identity: ReplacementQualificationIdentity,
        failed_contexts: BTreeSet<QueryContextRef>,
        contexts: BTreeSet<QueryContextRef>,
        admissions: Box<[QualifiedWorkerAdmission]>,
    }

    #[derive(Debug)]
    struct TestActiveIsolation(QueryExecutionId);

    impl AttemptIsolationReservation for TestIsolationReservation {
        fn identity(&self) -> ReplacementQualificationIdentity {
            self.identity
        }
        fn topology_revision(&self) -> u64 {
            1
        }
        fn admissions(&self) -> &[QualifiedWorkerAdmission] {
            &self.admissions
        }
        fn validate_binding(
            &self,
            failed_contexts: &[QueryContextRef],
        ) -> Result<(), ReplacementQualificationFailure> {
            let failed: BTreeSet<_> = failed_contexts.iter().copied().collect();
            let contexts: BTreeSet<_> = self
                .admissions
                .iter()
                .map(QualifiedWorkerAdmission::context)
                .collect();
            (failed == self.failed_contexts && contexts == self.contexts)
                .then_some(())
                .ok_or(ReplacementQualificationFailure::InvalidReservation)
        }
        fn activate(
            self: Box<Self>,
        ) -> Result<Box<dyn ActiveAttemptIsolationOwner>, AttemptIsolationActivationFailure>
        {
            Ok(Box::new(TestActiveIsolation(self.identity.replacement())))
        }
        fn abandon(self: Box<Self>) {}
    }

    impl ActiveAttemptIsolationOwner for TestActiveIsolation {
        fn execution(&self) -> QueryExecutionId {
            self.0
        }
        fn finish(self: Box<Self>) {}
        fn abandon(self: Box<Self>) {}
    }

    #[derive(Debug)]
    struct CountingIsolationReservation {
        identity: ReplacementQualificationIdentity,
        abandoned: Arc<AtomicUsize>,
        admissions: Box<[QualifiedWorkerAdmission]>,
    }

    impl AttemptIsolationReservation for CountingIsolationReservation {
        fn identity(&self) -> ReplacementQualificationIdentity {
            self.identity
        }
        fn topology_revision(&self) -> u64 {
            1
        }
        fn admissions(&self) -> &[QualifiedWorkerAdmission] {
            &self.admissions
        }
        fn validate_binding(
            &self,
            _failed_contexts: &[QueryContextRef],
        ) -> Result<(), ReplacementQualificationFailure> {
            Ok(())
        }
        fn activate(
            self: Box<Self>,
        ) -> Result<Box<dyn ActiveAttemptIsolationOwner>, AttemptIsolationActivationFailure>
        {
            Ok(Box::new(CountingActiveIsolation {
                execution: self.identity.replacement(),
                abandoned: Arc::clone(&self.abandoned),
            }))
        }
        fn abandon(self: Box<Self>) {
            self.abandoned.fetch_add(1, Ordering::SeqCst);
        }
    }

    #[derive(Debug)]
    struct CountingActiveIsolation {
        execution: QueryExecutionId,
        abandoned: Arc<AtomicUsize>,
    }

    impl ActiveAttemptIsolationOwner for CountingActiveIsolation {
        fn execution(&self) -> QueryExecutionId {
            self.execution
        }
        fn finish(self: Box<Self>) {
            self.abandoned.fetch_add(1, Ordering::SeqCst);
        }
        fn abandon(self: Box<Self>) {
            self.abandoned.fetch_add(1, Ordering::SeqCst);
        }
    }

    #[derive(Debug)]
    struct FailingIsolationReservation {
        identity: ReplacementQualificationIdentity,
        abandoned: Arc<AtomicUsize>,
        admissions: Box<[QualifiedWorkerAdmission]>,
    }

    impl AttemptIsolationReservation for FailingIsolationReservation {
        fn identity(&self) -> ReplacementQualificationIdentity {
            self.identity
        }

        fn topology_revision(&self) -> u64 {
            1
        }

        fn admissions(&self) -> &[QualifiedWorkerAdmission] {
            &self.admissions
        }

        fn validate_binding(
            &self,
            _failed_contexts: &[QueryContextRef],
        ) -> Result<(), ReplacementQualificationFailure> {
            Ok(())
        }

        fn activate(
            self: Box<Self>,
        ) -> Result<Box<dyn ActiveAttemptIsolationOwner>, AttemptIsolationActivationFailure>
        {
            Err(AttemptIsolationActivationFailure::new(
                ReplacementQualificationFailure::Rejected,
                self,
            ))
        }

        fn abandon(self: Box<Self>) {
            self.abandoned.fetch_add(1, Ordering::SeqCst);
        }
    }

    #[derive(Debug)]
    struct LifecycleIsolationReservation {
        identity: ReplacementQualificationIdentity,
        failed_contexts: BTreeSet<QueryContextRef>,
        contexts: BTreeSet<QueryContextRef>,
        admissions: Box<[QualifiedWorkerAdmission]>,
        finished: Arc<AtomicUsize>,
        abandoned: Arc<AtomicUsize>,
    }

    impl AttemptIsolationReservation for LifecycleIsolationReservation {
        fn identity(&self) -> ReplacementQualificationIdentity {
            self.identity
        }

        fn topology_revision(&self) -> u64 {
            1
        }

        fn admissions(&self) -> &[QualifiedWorkerAdmission] {
            &self.admissions
        }

        fn validate_binding(
            &self,
            failed_contexts: &[QueryContextRef],
        ) -> Result<(), ReplacementQualificationFailure> {
            let failed: BTreeSet<_> = failed_contexts.iter().copied().collect();
            let contexts: BTreeSet<_> = self
                .admissions
                .iter()
                .map(QualifiedWorkerAdmission::context)
                .collect();
            (failed == self.failed_contexts && contexts == self.contexts)
                .then_some(())
                .ok_or(ReplacementQualificationFailure::InvalidReservation)
        }

        fn activate(
            self: Box<Self>,
        ) -> Result<Box<dyn ActiveAttemptIsolationOwner>, AttemptIsolationActivationFailure>
        {
            Ok(Box::new(LifecycleActiveIsolation {
                execution: self.identity.replacement(),
                finished: Arc::clone(&self.finished),
                abandoned: Arc::clone(&self.abandoned),
            }))
        }

        fn abandon(self: Box<Self>) {
            self.abandoned.fetch_add(1, Ordering::SeqCst);
        }
    }

    #[derive(Debug)]
    struct LifecycleActiveIsolation {
        execution: QueryExecutionId,
        finished: Arc<AtomicUsize>,
        abandoned: Arc<AtomicUsize>,
    }

    impl ActiveAttemptIsolationOwner for LifecycleActiveIsolation {
        fn execution(&self) -> QueryExecutionId {
            self.execution
        }

        fn finish(self: Box<Self>) {
            self.finished.fetch_add(1, Ordering::SeqCst);
        }

        fn abandon(self: Box<Self>) {
            self.abandoned.fetch_add(1, Ordering::SeqCst);
        }
    }

    fn qualified_reservation(
        submission: &ReplacementQualificationEffectSubmission,
    ) -> QualifiedReplacementReservation {
        let request = submission.request();
        let admissions: Box<[_]> = request
            .replacement_contexts()
            .iter()
            .enumerate()
            .map(|(index, context)| {
                let tag = u8::try_from(index + 1).unwrap();
                QualifiedWorkerAdmission::try_new(
                    request.identity().replacement(),
                    admission_request(*context, tag),
                    admission_ticket(*context, tag),
                )
                .unwrap()
            })
            .collect::<Vec<_>>()
            .into_boxed_slice();
        let contexts = admissions
            .iter()
            .map(QualifiedWorkerAdmission::context)
            .collect();
        QualifiedReplacementReservation::try_new(
            request,
            Box::new(TestIsolationReservation {
                identity: request.identity(),
                failed_contexts: request.failed_contexts().iter().copied().collect(),
                contexts,
                admissions,
            }),
        )
        .unwrap()
    }

    fn qualify(submission: ReplacementQualificationEffectSubmission) {
        let reservation = qualified_reservation(&submission);
        submission.qualified(reservation).unwrap();
    }

    fn admission_request(
        context: QueryContextRef,
        tag: u8,
    ) -> novarocks_execution_contract::AcquireQueryContextAdmissionTicket {
        novarocks_execution_contract::AcquireQueryContextAdmissionTicket::new(
            TaskOperationId::new_v7(),
            context,
            LeaseValidFor::new(Duration::from_secs(10)).unwrap(),
            NativeCompatibilityId::new([tag; 32]),
            AdmissionEpochCapability::try_from_bytes([tag; 16]).unwrap(),
        )
    }

    fn admission_ticket(context: QueryContextRef, tag: u8) -> QueryContextAdmissionTicketReceipt {
        QueryContextAdmissionTicketReceipt::new(
            AdmissionTicketId::try_from_bytes([tag; 16]).unwrap(),
            context,
            LeaseValidFor::new(Duration::from_secs(10)).unwrap(),
        )
    }

    fn establish_request(
        context: QueryContextRef,
        ticket: QueryContextAdmissionTicketReceipt,
        tag: u8,
    ) -> Arc<novarocks_execution_contract::EstablishQueryContext> {
        establish_request_with_operation(context, ticket, tag, TaskOperationId::new_v7())
    }

    fn establish_request_with_operation(
        context: QueryContextRef,
        ticket: QueryContextAdmissionTicketReceipt,
        tag: u8,
        operation_id: TaskOperationId,
    ) -> Arc<novarocks_execution_contract::EstablishQueryContext> {
        let content = |offset| {
            Arc::new(FakeContent(ContentFingerprint::from_bytes(
                [tag + offset; 16],
            ))) as Arc<dyn CodecOwnedContent>
        };
        Arc::new(novarocks_execution_contract::EstablishQueryContext::new(
            operation_id,
            context,
            ticket.ticket_id(),
            content(0),
            content(1),
            content(2),
            CredentialUpdate::new(
                CredentialLeaseId::new(u64::from(tag)),
                CredentialEpoch::new(1).unwrap(),
                Arc::new(FakeSecret),
            ),
            LeaseValidFor::new(Duration::from_secs(30)).unwrap(),
        ))
    }

    fn recovery_config(
        initial_execution: QueryExecutionId,
        port: Arc<dyn ReplacementQualificationEffectPort>,
        max_attempts: u32,
    ) -> LogicalExecutionActorConfig {
        let (work_owner, stage) = test_governed_work();
        LogicalExecutionActorConfig::read_only_pre_visibility_recovery_rows(
            initial_execution,
            NonZeroUsize::new(4).unwrap(),
            Vec::new(),
            NonZeroUsize::new(2).unwrap(),
            NonZeroUsize::new(2).unwrap(),
            NonZeroU32::new(max_attempts).unwrap(),
            port,
            work_owner,
            stage,
            Duration::from_secs(30),
            ResultSchema::new(Vec::<crate::api::ResultField>::new()),
            NonZeroUsize::new(1).unwrap(),
            result_carrier(),
        )
        .unwrap()
    }

    fn recovery_config_with_contexts(
        initial_execution: QueryExecutionId,
        contexts: Vec<QueryContextRef>,
        port: Arc<dyn ReplacementQualificationEffectPort>,
        max_attempts: u32,
    ) -> LogicalExecutionActorConfig {
        let (work_owner, stage) = test_governed_work();
        LogicalExecutionActorConfig::read_only_pre_visibility_recovery_rows(
            initial_execution,
            NonZeroUsize::new(4).unwrap(),
            contexts,
            NonZeroUsize::new(2).unwrap(),
            NonZeroUsize::new(2).unwrap(),
            NonZeroU32::new(max_attempts).unwrap(),
            port,
            work_owner,
            stage,
            Duration::from_secs(30),
            ResultSchema::new(Vec::<crate::api::ResultField>::new()),
            NonZeroUsize::new(1).unwrap(),
            result_carrier(),
        )
        .unwrap()
    }

    fn config(initial_execution: QueryExecutionId) -> LogicalExecutionActorConfig {
        let (work_owner, stage) = test_governed_work();
        LogicalExecutionActorConfig::single_attempt_completion(
            initial_execution,
            ExecutionEffect::None,
            NonZeroUsize::new(1).unwrap(),
            Vec::new(),
            NonZeroUsize::new(2).unwrap(),
            NonZeroUsize::new(2).unwrap(),
            work_owner,
            stage,
        )
        .unwrap()
    }

    fn result_schema() -> ResultSchema {
        ResultSchema::new(vec![ResultField::new(
            "value",
            novarocks_type_contract::FunctionValueType::new(DataType::Int64, false),
        )])
    }

    fn result_carrier() -> crate::api::ResultRowCarrier {
        crate::api::ResultRowCarrier::relayed(
            novarocks_result_contract::RootOutputKind::ClientRows,
            Some(result_profile()),
        )
        .unwrap()
    }

    fn result_end(position: u64) -> novarocks_execution_contract::root_result::RootResultEnd {
        novarocks_execution_contract::root_result::RootResultEnd {
            sequence: std::num::NonZeroU64::new(position + 1).unwrap(),
            output_rows: position * 2,
        }
    }

    fn result_profile() -> ClientRowProfile {
        ClientRowProfile::try_new(
            novarocks_result_contract::RootProfileV1::SEGMENT_BYTES,
            novarocks_result_contract::RootProfileV1::ROW_PAYLOAD_BYTES,
        )
        .unwrap()
    }

    fn result_segment(
        execution: QueryExecutionId,
        sequence: u64,
    ) -> (
        WorkloadControl,
        WorkOwner,
        novarocks_workload_control::ResultCapacityHandle,
        Arc<RetainedRootReply>,
    ) {
        use novarocks_execution_contract::root_result::{
            RootReadOutcome, RootResultData, RootResultReply,
        };
        use novarocks_result_contract::{RootOutputKind, RootProfileId};
        use novarocks_workload_control::{ResultCapacityConfig, ResultWindowClass};
        let control = WorkloadControl::try_new(
            WorkloadConfig::default(),
            ResourceConfig {
                total_bytes: 1024 * 1024,
                control_bytes: 1024,
                per_scope_bytes: 1024 * 1024 - 1024,
            },
        )
        .unwrap();
        let capacity = control
            .configure_result_capacity(ResultCapacityConfig::V1)
            .unwrap();
        control.mark_ready().unwrap();
        let root = control
            .try_begin_root(WorkRequest::new(WorkClass::Query))
            .unwrap();
        let window = capacity
            .try_acquire(&root.owner.scope(), ResultWindowClass::Client)
            .unwrap();
        let segment = RetainedRootReply::try_new(
            novarocks_execution_contract::root_result::RootReadResponse::Reply(RootResultReply {
                root_task: root_task(execution, 1),
                profile: RootProfileId::V1,
                kind: RootOutputKind::ClientRows,
                accepted_consumed: 0,
                outcome: RootReadOutcome::Data(
                    RootResultData::try_new(
                        RootOutputKind::ClientRows,
                        std::num::NonZeroU64::new(sequence).unwrap(),
                        bytes::Bytes::from_static(&[
                            2, 0, 0, 0, 1, b'7', 3, 0, 0, 0, 2, b'1', b'1',
                        ]),
                        None,
                    )
                    .unwrap(),
                ),
            }),
            window.retain_alias(),
            4096,
        )
        .unwrap();
        drop(window);
        root.business.release();
        (control, root.owner, capacity, Arc::new(segment))
    }

    async fn wait_for_conclusion(
        actor: &LogicalExecutionActor,
        expected: LogicalConclusion,
    ) -> LogicalExecutionActorSnapshot {
        for _ in 0..16 {
            let snapshot = actor.snapshot().await.unwrap();
            if snapshot.conclusion == Some(expected) {
                return snapshot;
            }
            tokio::task::yield_now().await;
        }
        panic!("logical execution did not reach {expected:?}");
    }

    #[tokio::test]
    async fn exact_readiness_activates_and_completes_once() {
        let runtime = Handle::current();
        let execution = execution(1);
        let (owner, permit, _output) = spawn_logical_execution_actor(&runtime, config(execution))
            .unwrap()
            .into_parts();
        let actor = owner.actor();
        let identity = permit.identity();
        assert_eq!(actor.id(), identity.actor());
        let running = actor.activate(permit.ready()).await.unwrap();
        assert_eq!(running.identity(), identity);
        assert_eq!(
            actor.complete_attempt(running).await.unwrap(),
            LogicalConclusion::Succeeded
        );
    }

    #[tokio::test]
    async fn spawned_bundle_explicitly_holds_untransferred_output() {
        let runtime = Handle::current();
        let execution = execution(10_001);
        let spawned = spawn_logical_execution_actor(&runtime, config(execution)).unwrap();

        let (owner, permit, output) = spawned.into_parts();
        assert!(matches!(output.into_output(), ExecutionOutput::Completion));
        let actor = owner.actor();
        let running = actor.activate(permit.ready()).await.unwrap();
        assert_eq!(
            actor.complete_attempt(running).await.unwrap(),
            LogicalConclusion::Succeeded
        );
    }

    #[tokio::test]
    async fn residual_transfer_does_not_close_separately_handed_result_stream() {
        let runtime = Handle::current();
        let execution = execution(10_002);
        let (work_owner, stage) = test_governed_work();
        let actor_config = LogicalExecutionActorConfig::single_attempt_read_rows(
            execution,
            NonZeroUsize::new(1).unwrap(),
            Vec::new(),
            NonZeroUsize::new(1).unwrap(),
            NonZeroUsize::new(1).unwrap(),
            work_owner,
            stage,
            result_schema(),
            NonZeroUsize::new(1).unwrap(),
            result_carrier(),
        )
        .unwrap()
        .with_result_row_carrier(result_carrier());
        let spawned = spawn_logical_execution_actor(&runtime, actor_config).unwrap();
        let (owner, initial, output) = spawned.into_parts();
        let actor = owner.actor().clone();
        let ExecutionOutput::Rows(mut stream) = output.into_output() else {
            panic!("result execution must expose its row stream");
        };

        let residual = owner.into_residual_stand_down_supervisor();
        stream.begin_schema().unwrap().complete();

        drop(initial);
        wait_for_conclusion(&actor, LogicalConclusion::Cancelled).await;
        assert!(stream.next().await.is_err());
        drop(stream);
        drop(actor);
        residual.join().await.unwrap();
    }

    #[tokio::test]
    async fn effectful_result_stream_configuration_fails_closed() {
        let control = isolated_workload_control();
        let root = control
            .try_begin_root(WorkRequest::new(WorkClass::Query))
            .unwrap();
        let stage = root.owner.scope().try_acquire(Stage::Execution).unwrap();
        let work_owner = root.owner;
        drop(root.business);
        let mut config = LogicalExecutionActorConfig::single_attempt_completion(
            execution(10_005),
            ExecutionEffect::External,
            NonZeroUsize::new(1).unwrap(),
            Vec::new(),
            NonZeroUsize::new(1).unwrap(),
            NonZeroUsize::new(1).unwrap(),
            work_owner,
            stage,
        )
        .unwrap();
        config.output_mode = LogicalOutputMode::ResultStream;
        config.result_schema = Some(result_schema());
        config.result_delivery_capacity = NonZeroUsize::new(1);

        assert!(matches!(
            spawn_logical_execution_actor(&Handle::current(), config),
            Err(LogicalExecutionActorError::InvariantViolation)
        ));
        assert_eq!(control.snapshot().root_responsibilities, 0);
    }

    #[tokio::test]
    async fn successful_initial_attempt_waits_for_positive_resource_convergence() {
        let runtime = Handle::current();
        let first = execution(877);
        let frontend = FrontendProcessId::new_v7();
        let context = QueryContextRef::new(first, frontend, BackendProcessId::new_v7());
        let unrelated = QueryContextRef::new(first, frontend, BackendProcessId::new_v7());
        let abort_port = Arc::new(ClosingMixedAbortPort::new(context, unrelated));
        let abort_submissions = Arc::clone(&abort_port.submissions);
        let (work_owner, stage) = test_governed_work();
        let config = LogicalExecutionActorConfig::single_attempt_completion(
            first,
            ExecutionEffect::None,
            NonZeroUsize::new(4).unwrap(),
            vec![context],
            NonZeroUsize::new(2).unwrap(),
            NonZeroUsize::new(2).unwrap(),
            work_owner,
            stage,
        )
        .unwrap()
        .with_abort_query_context_effect_port(abort_port, NonZeroUsize::new(2).unwrap());
        let (owner, initial, _output) = spawn_logical_execution_actor(&runtime, config)
            .unwrap()
            .into_parts();
        let actor = owner.actor().clone();
        let running = actor.activate(initial.ready()).await.unwrap();
        let ticket = admission_ticket(context, 71);
        let admission = running
            .begin_admission_issue(admission_request(context, 71))
            .await
            .unwrap();
        running
            .settle_admission_issue(
                admission,
                AdmissionIssueSettlement::applied(
                    admission.operation_id(),
                    OperationOutcome::Accepted,
                    ticket,
                )
                .unwrap(),
            )
            .await
            .unwrap();
        let establish = running
            .authorize_establish(
                establish_request(context, ticket, 71),
                NativeCompatibilityId::new([71; 32]),
            )
            .await
            .unwrap();
        let transport = HoldingEstablishTransport::default();
        assert_eq!(
            establish.try_submit(&transport).unwrap(),
            EstablishIssueSubmit::Accepted
        );
        transport
            .take()
            .worker_settled(OperationOutcome::Accepted)
            .unwrap();
        assert_eq!(
            actor.complete_attempt(running).await.unwrap(),
            LogicalConclusion::Succeeded
        );
        assert!(
            abort_submissions.lock().unwrap().is_empty(),
            "successful cleanup must not be sent through the Abort effect"
        );

        let supervisor = owner.into_residual_stand_down_supervisor();
        drop(actor);
        for _ in 0..16 {
            tokio::task::yield_now().await;
        }
        assert!(
            !supervisor.is_finished(),
            "query success is not a Worker resource-convergence fact"
        );
        supervisor
            .observe_worker_stopped_and_context_fenced(context)
            .await
            .unwrap();
        tokio::time::timeout(Duration::from_secs(1), supervisor.join())
            .await
            .expect("positive Registry convergence must finish successful cleanup")
            .unwrap();
    }

    #[tokio::test]
    async fn dropping_initial_permit_concludes_cancelled() {
        let runtime = Handle::current();
        let (owner, permit, _output) =
            spawn_logical_execution_actor(&runtime, config(execution(2)))
                .unwrap()
                .into_parts();
        let actor = owner.actor();
        drop(permit);
        let snapshot = wait_for_conclusion(actor, LogicalConclusion::Cancelled).await;
        assert!(matches!(snapshot.phase, ExecutionPhase::Converging));
    }

    #[tokio::test]
    async fn cancelling_activation_before_enqueue_does_not_strand_instantiating() {
        let runtime = Handle::current();
        let (owner, permit, _output) =
            spawn_logical_execution_actor(&runtime, config(execution(3)))
                .unwrap()
                .into_parts();
        let actor = owner.actor();
        let activation = actor.activate(permit.ready());
        drop(activation);
        wait_for_conclusion(actor, LogicalConclusion::Cancelled).await;
    }

    #[tokio::test]
    async fn cancelling_activation_after_enqueue_does_not_strand_running() {
        let runtime = Handle::current();
        let (owner, permit, _output) =
            spawn_logical_execution_actor(&runtime, config(execution(4)))
                .unwrap()
                .into_parts();
        let actor = owner.actor();
        let mut activation = Box::pin(actor.activate(permit.ready()));
        assert!(matches!(
            std::future::poll_fn(|context| Poll::Ready(activation.as_mut().poll(context))).await,
            Poll::Pending
        ));
        drop(activation);
        wait_for_conclusion(actor, LogicalConclusion::Cancelled).await;
    }

    #[tokio::test]
    async fn dropping_delivered_running_permit_concludes_cancelled() {
        let runtime = Handle::current();
        let (owner, permit, _output) =
            spawn_logical_execution_actor(&runtime, config(execution(5)))
                .unwrap()
                .into_parts();
        let actor = owner.actor();
        let (reply, response) = oneshot::channel();
        actor
            .sender
            .try_send(ActorCommand::Activate {
                readiness: permit.ready(),
                reply,
            })
            .unwrap();
        tokio::task::yield_now().await;
        drop(response);
        wait_for_conclusion(actor, LogicalConclusion::Cancelled).await;
    }

    #[tokio::test]
    async fn initialization_can_fail_or_cancel_before_readiness() {
        let runtime = Handle::current();
        let (failed_owner, failed, _failed_output) =
            spawn_logical_execution_actor(&runtime, config(execution(6)))
                .unwrap()
                .into_parts();
        assert_eq!(
            failed_owner
                .actor()
                .initialization_failed(failed)
                .await
                .unwrap(),
            LogicalConclusion::Failed
        );

        let (cancelled_owner, cancelled, _cancelled_output) =
            spawn_logical_execution_actor(&runtime, config(execution(7)))
                .unwrap()
                .into_parts();
        assert_eq!(
            cancelled_owner
                .actor()
                .cancel_initialization(cancelled)
                .await
                .unwrap(),
            LogicalConclusion::Cancelled
        );
    }

    #[tokio::test]
    async fn running_attempt_can_fail() {
        let runtime = Handle::current();
        let (owner, permit, _output) =
            spawn_logical_execution_actor(&runtime, config(execution(8)))
                .unwrap()
                .into_parts();
        let actor = owner.actor();
        let running = actor.activate(permit.ready()).await.unwrap();
        assert_eq!(
            actor.fail_attempt(running).await.unwrap(),
            LogicalConclusion::Failed
        );
    }

    #[tokio::test]
    async fn replacement_waits_for_the_combined_external_qualification() {
        let runtime = Handle::current();
        let first = execution(81);
        let second = replacement(first, 2);
        let port = Arc::new(DelayedQualificationPort::default());
        let submissions = Arc::clone(&port.submissions);
        let (owner, permit, _output) =
            spawn_logical_execution_actor(&runtime, recovery_config(first, port, 2))
                .unwrap()
                .into_parts();
        let actor = owner.actor();
        let running = actor.activate(permit.ready()).await.unwrap();
        let qualification = actor
            .begin_replacement(
                running,
                AttemptFailureClass::RecoverableInfrastructure,
                second,
                Vec::new(),
            )
            .await
            .unwrap();
        assert_eq!(qualification.identity().failed(), first);
        assert_eq!(qualification.identity().replacement(), second);

        let mut activation = Box::pin(actor.activate_replacement(qualification));
        assert!(matches!(
            std::future::poll_fn(|context| Poll::Ready(activation.as_mut().poll(context))).await,
            Poll::Pending
        ));
        let submission = submissions.lock().unwrap().pop().unwrap();
        assert_eq!(submission.request().identity().replacement(), second);
        assert!(submission.request().failed_contexts().is_empty());
        qualify(submission);

        let permit = activation.await.unwrap();
        assert_eq!(permit.identity().execution(), second);
        let running = actor.activate(permit.ready()).await.unwrap();
        assert_eq!(
            actor.fail_attempt(running).await.unwrap(),
            LogicalConclusion::Failed
        );
    }

    #[tokio::test]
    async fn late_establish_settlement_refreshes_the_failed_attempt_residual() {
        let runtime = Handle::current();
        let first = execution(811);
        let second = replacement(first, 2);
        let frontend = FrontendProcessId::new_v7();
        let old_context = QueryContextRef::new(first, frontend, BackendProcessId::new_v7());
        let (capacity, _) = watch::channel(0);
        let accepted = Arc::new(Mutex::new(Vec::new()));
        let abort_port = Arc::new(SelectiveAbortPort {
            blocked: QueryContextRef::new(first, frontend, BackendProcessId::new_v7()),
            accepted: Arc::clone(&accepted),
            capacity,
        });
        let port = Arc::new(DelayedQualificationPort::default());
        let submissions = Arc::clone(&port.submissions);
        let config = recovery_config_with_contexts(first, vec![old_context], port, 2)
            .with_abort_query_context_effect_port(abort_port, NonZeroUsize::new(2).unwrap());
        let (owner, initial, _output) = spawn_logical_execution_actor(&runtime, config)
            .unwrap()
            .into_parts();
        let actor = owner.actor();
        let running = actor.activate(initial.ready()).await.unwrap();
        let ticket = admission_ticket(old_context, 21);
        let admission = running
            .begin_admission_issue(admission_request(old_context, 21))
            .await
            .unwrap();
        running
            .settle_admission_issue(
                admission,
                AdmissionIssueSettlement::applied(
                    admission.operation_id(),
                    OperationOutcome::Accepted,
                    ticket,
                )
                .unwrap(),
            )
            .await
            .unwrap();
        let establish = running
            .authorize_establish(
                establish_request(old_context, ticket, 21),
                NativeCompatibilityId::new([21; 32]),
            )
            .await
            .unwrap();
        let qualification = actor
            .begin_replacement(
                running,
                AttemptFailureClass::RecoverableInfrastructure,
                second,
                Vec::new(),
            )
            .await
            .unwrap();
        assert_eq!(
            submissions.lock().unwrap()[0].request().failed_contexts(),
            &[old_context]
        );
        qualify(submissions.lock().unwrap().pop().unwrap());
        let successor = actor.activate_replacement(qualification).await.unwrap();
        let successor = actor.activate(successor.ready()).await.unwrap();
        assert_eq!(
            actor
                .stand_down_snapshot(old_context)
                .await
                .unwrap()
                .unwrap()
                .closure(),
            super::super::ContextClosureState::AwaitingEstablishIssue
        );

        drop(establish);
        for _ in 0..16 {
            let closure = actor
                .stand_down_snapshot(old_context)
                .await
                .unwrap()
                .unwrap()
                .closure();
            if closure == super::super::ContextClosureState::Aborting {
                break;
            }
            tokio::task::yield_now().await;
        }
        assert_eq!(&*accepted.lock().unwrap(), &[old_context]);
        assert_eq!(
            actor
                .stand_down_snapshot(old_context)
                .await
                .unwrap()
                .unwrap()
                .closure(),
            super::super::ContextClosureState::Aborting
        );
        assert_eq!(
            actor.fail_attempt(successor).await.unwrap(),
            LogicalConclusion::Failed
        );
    }

    #[tokio::test]
    async fn successful_replacement_retains_unsettled_failed_attempt_residual() {
        let runtime = Handle::current();
        let first = execution(812);
        let second = replacement(first, 2);
        let frontend = FrontendProcessId::new_v7();
        let old_context = QueryContextRef::new(first, frontend, BackendProcessId::new_v7());
        let blocked_context = QueryContextRef::new(first, frontend, BackendProcessId::new_v7());
        let port = Arc::new(DelayedQualificationPort::default());
        let submissions = Arc::clone(&port.submissions);
        let abort_port = Arc::new(ClosingMixedAbortPort::new(old_context, blocked_context));
        let abort_submissions = Arc::clone(&abort_port.submissions);
        let mut config = recovery_config_with_contexts(first, vec![old_context], port, 2);
        config =
            config.with_abort_query_context_effect_port(abort_port, NonZeroUsize::new(2).unwrap());
        // Result-stream success is not exposed by this actor slice yet. The
        // completion-only test mode reaches the same actor cleanup loop after
        // exercising the real replacement and residual ledgers.
        config.output_mode = LogicalOutputMode::CompletionOnly;
        let (owner, initial, _output) = spawn_logical_execution_actor(&runtime, config)
            .unwrap()
            .into_parts();
        let actor = owner.actor().clone();
        let running = actor.activate(initial.ready()).await.unwrap();
        let ticket = admission_ticket(old_context, 22);
        let admission = running
            .begin_admission_issue(admission_request(old_context, 22))
            .await
            .unwrap();
        running
            .settle_admission_issue(
                admission,
                AdmissionIssueSettlement::applied(
                    admission.operation_id(),
                    OperationOutcome::Accepted,
                    ticket,
                )
                .unwrap(),
            )
            .await
            .unwrap();
        let establish = running
            .authorize_establish(
                establish_request(old_context, ticket, 22),
                NativeCompatibilityId::new([22; 32]),
            )
            .await
            .unwrap();
        let qualification = actor
            .begin_replacement(
                running,
                AttemptFailureClass::RecoverableInfrastructure,
                second,
                Vec::new(),
            )
            .await
            .unwrap();
        qualify(submissions.lock().unwrap().pop().unwrap());
        let successor = actor.activate_replacement(qualification).await.unwrap();
        let successor = actor.activate(successor.ready()).await.unwrap();
        assert_eq!(
            actor.complete_attempt(successor).await.unwrap(),
            LogicalConclusion::Succeeded
        );
        assert_eq!(
            actor
                .stand_down_snapshot(old_context)
                .await
                .unwrap()
                .unwrap()
                .closure(),
            super::super::ContextClosureState::AwaitingEstablishIssue
        );

        let supervisor = owner.into_residual_stand_down_supervisor();
        drop(actor);
        for _ in 0..16 {
            tokio::task::yield_now().await;
        }
        assert!(
            !supervisor.is_finished(),
            "successful current attempt must not discard an old residual responsibility"
        );

        drop(establish);
        let abort = tokio::time::timeout(Duration::from_secs(1), async {
            loop {
                if let Some(submission) = abort_submissions.lock().unwrap().pop() {
                    break submission;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("late Establish settlement must start residual Abort");
        assert!(!supervisor.is_finished());
        abort
            .worker_settled(QueryContextReceipt::new(
                old_context,
                QueryContextState::Aborting,
            ))
            .unwrap();
        for _ in 0..16 {
            tokio::task::yield_now().await;
        }
        assert!(
            !supervisor.is_finished(),
            "Abort acknowledgement is not an actual-stop or process-fence fact"
        );
        supervisor
            .observe_worker_stopped_and_context_fenced(old_context)
            .await
            .unwrap();
        tokio::time::timeout(Duration::from_secs(1), supervisor.join())
            .await
            .expect("registry convergence must settle the old residual")
            .unwrap();
    }

    async fn assert_initial_attempt_residual_requires_positive_convergence(
        query: i64,
        worker_state: QueryContextState,
    ) {
        let runtime = Handle::current();
        let first = execution(query);
        let frontend = FrontendProcessId::new_v7();
        let context = QueryContextRef::new(first, frontend, BackendProcessId::new_v7());
        let unrelated = QueryContextRef::new(first, frontend, BackendProcessId::new_v7());
        let abort_port = Arc::new(ClosingMixedAbortPort::new(context, unrelated));
        let abort_submissions = Arc::clone(&abort_port.submissions);
        let (work_owner, stage) = test_governed_work();
        let config = LogicalExecutionActorConfig::single_attempt_completion(
            first,
            ExecutionEffect::None,
            NonZeroUsize::new(4).unwrap(),
            vec![context],
            NonZeroUsize::new(2).unwrap(),
            NonZeroUsize::new(2).unwrap(),
            work_owner,
            stage,
        )
        .unwrap()
        .with_abort_query_context_effect_port(abort_port, NonZeroUsize::new(2).unwrap());
        let (owner, initial, _output) = spawn_logical_execution_actor(&runtime, config)
            .unwrap()
            .into_parts();
        let actor = owner.actor().clone();
        let running = actor.activate(initial.ready()).await.unwrap();
        let ticket = admission_ticket(context, 51);
        let admission = running
            .begin_admission_issue(admission_request(context, 51))
            .await
            .unwrap();
        running
            .settle_admission_issue(
                admission,
                AdmissionIssueSettlement::applied(
                    admission.operation_id(),
                    OperationOutcome::Accepted,
                    ticket,
                )
                .unwrap(),
            )
            .await
            .unwrap();
        let establish = running
            .authorize_establish(
                establish_request(context, ticket, 51),
                NativeCompatibilityId::new([51; 32]),
            )
            .await
            .unwrap();
        assert_eq!(
            actor.fail_attempt(running).await.unwrap(),
            LogicalConclusion::Failed
        );
        drop(establish);
        let abort = tokio::time::timeout(Duration::from_secs(1), async {
            loop {
                if let Some(submission) = abort_submissions.lock().unwrap().pop() {
                    break submission;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("failed initial attempt must reach the Abort effect owner");
        abort
            .worker_settled(QueryContextReceipt::new(context, worker_state))
            .unwrap();

        let supervisor = owner.into_residual_stand_down_supervisor();
        drop(actor);
        for _ in 0..16 {
            tokio::task::yield_now().await;
        }
        assert!(
            !supervisor.is_finished(),
            "a Worker status without positive stop or process replacement must retain supervision"
        );
        supervisor
            .observe_worker_stopped_and_context_fenced(context)
            .await
            .unwrap();
        tokio::time::timeout(Duration::from_secs(1), supervisor.join())
            .await
            .expect("positive Registry convergence must settle the initial residual")
            .unwrap();
    }

    #[tokio::test]
    async fn initial_terminal_retained_attempt_remains_residually_supervised() {
        assert_initial_attempt_residual_requires_positive_convergence(
            872,
            QueryContextState::TerminalRetained,
        )
        .await;
    }

    #[tokio::test]
    async fn initial_gone_attempt_remains_residually_supervised() {
        assert_initial_attempt_residual_requires_positive_convergence(873, QueryContextState::Gone)
            .await;
    }

    #[tokio::test]
    async fn old_attempt_convergence_cannot_mutate_the_running_replacement() {
        let runtime = Handle::current();
        let first = execution(82);
        let second = replacement(first, 2);
        let frontend = FrontendProcessId::new_v7();
        let old_context = QueryContextRef::new(first, frontend, BackendProcessId::new_v7());
        let port = Arc::new(DelayedQualificationPort::default());
        let submissions = Arc::clone(&port.submissions);
        let config = recovery_config_with_contexts(first, vec![old_context], port, 2)
            .with_abort_query_context_effect_port(
                super::super::PermanentlyBackpressuredAbortEffectPort::shared(),
                NonZeroUsize::new(2).unwrap(),
            );
        let (owner, permit, _output) = spawn_logical_execution_actor(&runtime, config)
            .unwrap()
            .into_parts();
        let actor = owner.actor();
        let running = actor.activate(permit.ready()).await.unwrap();
        let qualification = actor
            .begin_replacement(
                running,
                AttemptFailureClass::RecoverableInfrastructure,
                second,
                Vec::new(),
            )
            .await
            .unwrap();
        qualify(submissions.lock().unwrap().pop().unwrap());
        let permit = actor.activate_replacement(qualification).await.unwrap();
        let running = actor.activate(permit.ready()).await.unwrap();

        let (reply, response) = oneshot::channel();
        actor
            .sender
            .send(ActorCommand::RegistryContextConverged {
                context: old_context,
                convergence: RegistryContextConvergence::WorkerProcessReplaced,
                reply,
            })
            .await
            .unwrap();
        response.await.unwrap().unwrap();
        assert!(matches!(
            actor.snapshot().await.unwrap().phase,
            ExecutionPhase::Running { execution, .. } if execution == second
        ));
        assert_eq!(
            actor.fail_attempt(running).await.unwrap(),
            LogicalConclusion::Failed
        );
    }

    #[tokio::test]
    async fn old_attempt_establish_rejection_does_not_gate_the_successor() {
        let runtime = Handle::current();
        let first = execution(821);
        let second = replacement(first, 2);
        let frontend = FrontendProcessId::new_v7();
        let old_context = QueryContextRef::new(first, frontend, BackendProcessId::new_v7());
        let new_context = QueryContextRef::new(second, frontend, BackendProcessId::new_v7());
        let qualification_port = Arc::new(DelayedQualificationPort::default());
        let submissions = Arc::clone(&qualification_port.submissions);
        let config = recovery_config_with_contexts(first, vec![old_context], qualification_port, 2)
            .with_abort_query_context_effect_port(
                super::super::PermanentlyBackpressuredAbortEffectPort::shared(),
                NonZeroUsize::new(2).unwrap(),
            );
        let (owner, initial, _output) = spawn_logical_execution_actor(&runtime, config)
            .unwrap()
            .into_parts();
        let actor = owner.actor();
        let running = actor.activate(initial.ready()).await.unwrap();
        let ticket = admission_ticket(old_context, 22);
        let admission = running
            .begin_admission_issue(admission_request(old_context, 22))
            .await
            .unwrap();
        running
            .settle_admission_issue(
                admission,
                AdmissionIssueSettlement::applied(
                    admission.operation_id(),
                    OperationOutcome::Accepted,
                    ticket,
                )
                .unwrap(),
            )
            .await
            .unwrap();
        let establish = running
            .authorize_establish(
                establish_request(old_context, ticket, 22),
                NativeCompatibilityId::new([22; 32]),
            )
            .await
            .unwrap();
        let transport = HoldingEstablishTransport::default();
        assert_eq!(
            establish.try_submit(&transport).unwrap(),
            EstablishIssueSubmit::Accepted
        );
        let qualification = actor
            .begin_replacement(
                running,
                AttemptFailureClass::RecoverableInfrastructure,
                second,
                vec![new_context],
            )
            .await
            .unwrap();
        qualify(submissions.lock().unwrap().pop().unwrap());
        let successor = actor.activate_replacement(qualification).await.unwrap();
        let admissions = successor
            .take_replacement_admission_evidence()
            .await
            .unwrap()
            .unwrap();
        assert_eq!(admissions.len(), 1);
        assert_eq!(admissions[0].context(), new_context);
        let successor = actor.activate(successor.ready()).await.unwrap();

        transport
            .take()
            .worker_settled(OperationOutcome::ContextConflict)
            .unwrap();
        for _ in 0..16 {
            if actor.snapshot().await.unwrap().establish_error
                == Some(EstablishIssueError::EstablishRejected)
            {
                break;
            }
            tokio::task::yield_now().await;
        }
        assert_eq!(
            actor.snapshot().await.unwrap().establish_error,
            Some(EstablishIssueError::EstablishRejected)
        );
        let _establish = successor
            .authorize_establish(
                establish_request(new_context, admissions[0].receipt(), 23),
                NativeCompatibilityId::new([23; 32]),
            )
            .await
            .expect("an old attempt diagnostic must not gate successor Establish");
        assert_eq!(
            actor.fail_attempt(successor).await.unwrap(),
            LogicalConclusion::Failed
        );
    }

    #[tokio::test]
    async fn current_establish_rejection_defers_to_the_running_permit_owner() {
        let runtime = Handle::current();
        let execution = execution(822);
        let frontend = FrontendProcessId::new_v7();
        let context = QueryContextRef::new(execution, frontend, BackendProcessId::new_v7());
        let (work_owner, stage) = test_governed_work();
        let config = LogicalExecutionActorConfig::single_attempt_read_rows(
            execution,
            NonZeroUsize::new(4).unwrap(),
            vec![context],
            NonZeroUsize::new(2).unwrap(),
            NonZeroUsize::new(2).unwrap(),
            work_owner,
            stage,
            result_schema(),
            NonZeroUsize::new(1).unwrap(),
            result_carrier(),
        )
        .unwrap()
        .with_result_row_carrier(result_carrier())
        .with_abort_query_context_effect_port(
            super::super::PermanentlyBackpressuredAbortEffectPort::shared(),
            NonZeroUsize::new(2).unwrap(),
        );
        let (owner, initial, output) = spawn_logical_execution_actor(&runtime, config)
            .unwrap()
            .into_parts();
        let ExecutionOutput::Rows(mut stream) = output.into_output() else {
            panic!("row execution must expose its result stream");
        };
        stream.begin_schema().unwrap().complete();
        let actor = owner.actor().clone();
        let running = actor.activate(initial.ready()).await.unwrap();
        let ticket = admission_ticket(context, 22);
        let admission = running
            .begin_admission_issue(admission_request(context, 22))
            .await
            .unwrap();
        running
            .settle_admission_issue(
                admission,
                AdmissionIssueSettlement::applied(
                    admission.operation_id(),
                    OperationOutcome::Accepted,
                    ticket,
                )
                .unwrap(),
            )
            .await
            .unwrap();
        let establish = running
            .authorize_establish(
                establish_request(context, ticket, 22),
                NativeCompatibilityId::new([22; 32]),
            )
            .await
            .unwrap();
        let transport = HoldingEstablishTransport::default();
        assert_eq!(
            establish.try_submit(&transport).unwrap(),
            EstablishIssueSubmit::Accepted
        );
        transport
            .take()
            .worker_settled(OperationOutcome::ContextConflict)
            .unwrap();

        for _ in 0..16 {
            if actor.snapshot().await.unwrap().establish_error
                == Some(EstablishIssueError::EstablishRejected)
            {
                break;
            }
            tokio::task::yield_now().await;
        }
        assert_eq!(
            actor.snapshot().await.unwrap().establish_error,
            Some(EstablishIssueError::EstablishRejected)
        );
        assert_eq!(actor.snapshot().await.unwrap().conclusion, None);

        let native_terminal = QueryExecutionError::new(
            QueryExecutionErrorKind::Failed,
            "authoritative Native attempt terminal",
        );
        assert_eq!(
            actor
                .fail_attempt_with_error(running, native_terminal.clone())
                .await
                .unwrap(),
            LogicalConclusion::Failed
        );
        let error = match stream.next().await {
            Err(error) => error,
            Ok(_) => panic!("failed Native attempt must terminate the result stream"),
        };
        assert_eq!(error, native_terminal);
        drop(stream);
        drop(actor);
        drop(owner);
    }

    #[tokio::test]
    async fn exact_result_withdrawal_delivers_end_without_erasing_unsent_or_unknown_establish() {
        use super::super::EstablishIssueState;
        for mode in 0..3 {
            let runtime = Handle::current();
            let execution = execution(824 + mode);
            let frontend = FrontendProcessId::new_v7();
            let root_context =
                QueryContextRef::new(execution, frontend, BackendProcessId::new_v7());
            let unused = QueryContextRef::new(execution, frontend, BackendProcessId::new_v7());
            let (work_owner, stage) = test_governed_work();
            let config = LogicalExecutionActorConfig::single_attempt_read_rows(
                execution,
                NonZeroUsize::new(4).unwrap(),
                vec![root_context, unused],
                NonZeroUsize::new(2).unwrap(),
                NonZeroUsize::new(2).unwrap(),
                work_owner,
                stage,
                result_schema(),
                NonZeroUsize::new(1).unwrap(),
                result_carrier(),
            )
            .unwrap()
            .with_result_row_carrier(result_carrier())
            .with_abort_query_context_effect_port(
                super::super::PermanentlyBackpressuredAbortEffectPort::shared(),
                NonZeroUsize::new(2).unwrap(),
            );
            let (owner, initial, output) = spawn_logical_execution_actor(&runtime, config)
                .unwrap()
                .into_parts();
            let ExecutionOutput::Rows(mut stream) = output.into_output() else {
                panic!("Client rows required")
            };
            stream.begin_schema().unwrap().complete();
            let actor = owner.actor().clone();
            let running = actor.activate(initial.ready()).await.unwrap();
            let activation = running.identity();
            let mut root_submission = None;
            let mut late = None;
            for (context, tag) in [(root_context, 24), (unused, 25)] {
                if context == unused && mode == 0 {
                    continue;
                }
                let ticket = admission_ticket(context, tag);
                let admission = running
                    .begin_admission_issue(admission_request(context, tag))
                    .await
                    .unwrap();
                running
                    .settle_admission_issue(
                        admission,
                        AdmissionIssueSettlement::applied(
                            admission.operation_id(),
                            OperationOutcome::Accepted,
                            ticket,
                        )
                        .unwrap(),
                    )
                    .await
                    .unwrap();
                let establish = running
                    .authorize_establish(
                        establish_request(context, ticket, tag),
                        NativeCompatibilityId::new([tag; 32]),
                    )
                    .await
                    .unwrap();
                let transport = HoldingEstablishTransport::default();
                assert_eq!(
                    establish.try_submit(&transport).unwrap(),
                    EstablishIssueSubmit::Accepted
                );
                let submission = transport.take();
                if context == root_context {
                    root_submission = Some(submission);
                } else if mode == 1 {
                    submission.definitely_unsent().unwrap();
                } else {
                    late = Some(submission.transport_unknown().unwrap());
                }
            }
            let root = TaskIdentity::new(
                execution,
                StageId::new(24).unwrap(),
                TaskId::new(24).unwrap(),
                root_context.backend_process_id(),
            );
            let observer = running.bind_root_result(root).await.unwrap();
            observer
                .observe_status(root_status(root, 1, TaskState::Finished))
                .await
                .unwrap();
            observer
                .observe_local_root_end(root, result_end(0))
                .await
                .unwrap();
            let fact = ResultContextWithdrawal::new(activation, root, unused);
            let finish =
                tokio::spawn(async move { running.finish_result_stream(vec![fact]).await });
            tokio::task::yield_now().await;
            assert!(
                !finish.is_finished(),
                "the root context must still have its own Applied receipt"
            );
            root_submission
                .take()
                .unwrap()
                .worker_settled(OperationOutcome::Accepted)
                .unwrap();
            let ResultDelivery::End(end) = stream.next().await.unwrap().unwrap() else {
                panic!("exact result withdrawal must permit Client End")
            };
            end.complete();
            assert_eq!(finish.await.unwrap().unwrap(), LogicalConclusion::Succeeded);
            let held = actor.establish_snapshot(unused).await.unwrap();
            match mode {
                0 => assert!(
                    held.is_none(),
                    "result withdrawal is not a fabricated grant or Worker receipt"
                ),
                1 => assert_eq!(held.unwrap().state(), EstablishIssueState::DefinitelyUnsent),
                _ => assert_eq!(held.unwrap().state(), EstablishIssueState::TransportUnknown),
            }
            assert!(
                !owner.is_finished(),
                "Client End is not the original physical/context exit"
            );
            if let Some(late) = late {
                late.worker_settled(OperationOutcome::Accepted).unwrap();
                for _ in 0..16 {
                    if actor
                        .establish_snapshot(unused)
                        .await
                        .unwrap()
                        .is_some_and(|held| {
                            held.state()
                                == EstablishIssueState::WorkerSettled(
                                    super::super::EstablishWorkerSettlement::Applied,
                                )
                        })
                    {
                        break;
                    }
                    tokio::task::yield_now().await;
                }
                assert_eq!(
                    actor
                        .establish_snapshot(unused)
                        .await
                        .unwrap()
                        .unwrap()
                        .state(),
                    EstablishIssueState::WorkerSettled(
                        super::super::EstablishWorkerSettlement::Applied
                    )
                );
            }
            // Actor-unit teardown uses the existing Registry composite author;
            // it is separate from End and does not claim a Native transport exit.
            drop((stream, observer, actor));
            let residual = owner.into_residual_stand_down_supervisor();
            residual
                .observe_worker_stopped_and_context_fenced(root_context)
                .await
                .unwrap();
            if mode != 0 {
                residual
                    .observe_worker_stopped_and_context_fenced(unused)
                    .await
                    .unwrap();
            }
            tokio::time::timeout(Duration::from_secs(2), residual.join())
                .await
                .unwrap()
                .unwrap();
        }
    }

    #[tokio::test]
    async fn sealed_withdrawal_keeps_late_refusals_but_not_required_or_protocol_vetoes() {
        use super::super::{EstablishIssueState, EstablishWorkerSettlement};
        // Five ordinary unused-context refusals, then a contradictory original
        // receipt and two required-root admission refusals without Establish.
        for mode in 0..8 {
            let execution = execution(840 + mode);
            let frontend = FrontendProcessId::new_v7();
            let root_context =
                QueryContextRef::new(execution, frontend, BackendProcessId::new_v7());
            let unused = QueryContextRef::new(execution, frontend, BackendProcessId::new_v7());
            let (work_owner, stage) = test_governed_work();
            let config = LogicalExecutionActorConfig::single_attempt_read_rows(
                execution,
                NonZeroUsize::new(4).unwrap(),
                vec![root_context, unused],
                NonZeroUsize::new(2).unwrap(),
                NonZeroUsize::new(2).unwrap(),
                work_owner,
                stage,
                result_schema(),
                NonZeroUsize::new(1).unwrap(),
                result_carrier(),
            )
            .unwrap()
            .with_result_row_carrier(result_carrier())
            .with_abort_query_context_effect_port(
                super::super::PermanentlyBackpressuredAbortEffectPort::shared(),
                NonZeroUsize::new(2).unwrap(),
            );
            let (owner, initial, output) =
                spawn_logical_execution_actor(&Handle::current(), config)
                    .unwrap()
                    .into_parts();
            let ExecutionOutput::Rows(mut stream) = output.into_output() else {
                panic!("Client rows required")
            };
            stream.begin_schema().unwrap().complete();
            let actor = owner.actor().clone();
            let running = actor.activate(initial.ready()).await.unwrap();
            let activation = running.identity();
            let mut root_submission = None;
            let mut root_late = None;
            let mut unused_late = None;
            let mut root_admission = None;
            let mut unused_admission = None;
            for (context, tag) in [(root_context, 34), (unused, 35)] {
                let admission = running
                    .begin_admission_issue(admission_request(context, tag))
                    .await
                    .unwrap();
                if context == root_context && mode >= 6 {
                    root_admission = Some(admission);
                    continue;
                }
                if context == unused && (2..=4).contains(&mode) {
                    unused_admission = Some(admission);
                    continue;
                }
                let ticket = admission_ticket(context, tag);
                running
                    .settle_admission_issue(
                        admission,
                        AdmissionIssueSettlement::applied(
                            admission.operation_id(),
                            OperationOutcome::Accepted,
                            ticket,
                        )
                        .unwrap(),
                    )
                    .await
                    .unwrap();
                let request = establish_request(context, ticket, tag);
                let establish = running
                    .authorize_establish(
                        Arc::clone(&request),
                        NativeCompatibilityId::new([tag; 32]),
                    )
                    .await
                    .unwrap();
                let transport = HoldingEstablishTransport::default();
                assert_eq!(
                    establish.try_submit(&transport).unwrap(),
                    EstablishIssueSubmit::Accepted
                );
                let submission = transport.take();
                if context == root_context {
                    if mode == 5 {
                        root_late = Some(submission.transport_unknown().unwrap());
                        let replay = running.reauthorize_establish(context).await.unwrap();
                        assert_eq!(
                            replay.try_submit(&transport).unwrap(),
                            EstablishIssueSubmit::Accepted
                        );
                        transport
                            .take()
                            .worker_settled(OperationOutcome::Accepted)
                            .unwrap();
                    } else {
                        root_submission = Some(submission);
                    }
                } else {
                    // The original transport guard, not a selected diagnostic,
                    // owns the only late receipt used below.
                    unused_late = Some(submission.transport_unknown().unwrap());
                }
            }
            let root = TaskIdentity::new(
                execution,
                StageId::new(34).unwrap(),
                TaskId::new(34).unwrap(),
                root_context.backend_process_id(),
            );
            let observer = running.bind_root_result(root).await.unwrap();
            observer
                .observe_status(root_status(root, 1, TaskState::Finished))
                .await
                .unwrap();
            observer
                .observe_local_root_end(root, result_end(0))
                .await
                .unwrap();
            let fact = ResultContextWithdrawal::new(activation, root, unused);

            let expected_diagnostic = if let Some(admission) = root_admission.or(unused_admission) {
                let settlement = if mode == 2 || mode == 6 {
                    AdmissionIssueSettlement::rejected(
                        admission.operation_id(),
                        OperationOutcome::ContextConflict,
                    )
                    .unwrap()
                } else {
                    AdmissionIssueSettlement::pre_worker_rejected_with_prior_unknown(
                        admission.operation_id(),
                        mode == 4,
                    )
                };
                assert_eq!(
                    running.settle_admission_issue(admission, settlement).await,
                    Err(LogicalExecutionActorError::Establish(
                        EstablishIssueError::AdmissionRejected
                    ))
                );
                EstablishIssueError::AdmissionRejected
            } else {
                let late = unused_late.take().unwrap();
                if mode == 1 {
                    late.pre_worker_rejected().unwrap();
                } else {
                    late.worker_settled(OperationOutcome::ContextConflict)
                        .unwrap();
                }
                EstablishIssueError::EstablishRejected
            };
            // This refusal is accounted before the pump's consumed handoff.
            // Arrival here cannot decide whether the Native owner sealed it.
            for _ in 0..16 {
                if actor.snapshot().await.unwrap().establish_error == Some(expected_diagnostic) {
                    break;
                }
                tokio::task::yield_now().await;
            }
            assert_eq!(
                actor.snapshot().await.unwrap().establish_error,
                Some(expected_diagnostic)
            );
            assert_eq!(actor.snapshot().await.unwrap().conclusion, None);
            if let Some(late) = root_late {
                // The first root receipt already said Applied. The actual
                // original late carrier contradicts it: a result withdrawal
                // must never hide this protocol fault behind the first refusal.
                late.worker_settled(OperationOutcome::ContextConflict)
                    .unwrap();
            }
            let finish =
                tokio::spawn(async move { running.finish_result_stream(vec![fact]).await });
            if mode < 5 {
                tokio::task::yield_now().await;
                assert!(!finish.is_finished(), "root Applied is still required");
                root_submission
                    .take()
                    .unwrap()
                    .worker_settled(OperationOutcome::Accepted)
                    .unwrap();
                let ResultDelivery::End(end) = stream.next().await.unwrap().unwrap() else {
                    panic!("the exact unused context must not veto End")
                };
                end.complete();
                assert_eq!(finish.await.unwrap().unwrap(), LogicalConclusion::Succeeded);
            } else {
                let error = match stream.next().await {
                    Err(error) => error,
                    Ok(_) => panic!("required admission/protocol failure must not deliver End"),
                };
                use novarocks_execution_contract::failure::FailureProjection;
                let expected = if mode == 5 {
                    EstablishIssueError::ConflictingWorkerSettlement
                } else {
                    EstablishIssueError::AdmissionRejected
                };
                assert_eq!(
                    error.execution_failure().unwrap().fact(),
                    expected.failure_fact()
                );
                assert!(matches!(
                    finish.await.unwrap(),
                    Err(RunningAttemptHandoffError::ExecutionConcluded(
                        LogicalConclusion::Failed
                    ))
                ));
            }
            assert_eq!(
                actor.snapshot().await.unwrap().establish_error,
                Some(expected_diagnostic),
                "first diagnostic is retained even after a protocol fault"
            );
            if mode == 0 || mode == 5 {
                assert_eq!(
                    actor
                        .establish_snapshot(unused)
                        .await
                        .unwrap()
                        .unwrap()
                        .state(),
                    EstablishIssueState::WorkerSettled(EstablishWorkerSettlement::Rejected(
                        OperationOutcome::ContextConflict
                    ))
                );
            } else if mode == 1 {
                assert_eq!(
                    actor
                        .establish_snapshot(unused)
                        .await
                        .unwrap()
                        .unwrap()
                        .state(),
                    EstablishIssueState::PreWorkerRejected
                );
            } else if mode < 5 {
                assert!(
                    actor.establish_snapshot(unused).await.unwrap().is_none(),
                    "admission refusal must not fabricate Establish"
                );
            }
            assert!(
                !owner.is_finished(),
                "logical End/failure is not physical context exit"
            );
            drop((stream, observer, actor, root_submission, unused_late));
            let residual = owner.into_residual_stand_down_supervisor();
            // This is the original Registry composite unit-test teardown, not
            // evidence of a Native Worker physical exit.
            for context in [root_context, unused] {
                residual
                    .observe_worker_stopped_and_context_fenced(context)
                    .await
                    .unwrap();
            }
            tokio::time::timeout(Duration::from_secs(2), residual.join())
                .await
                .unwrap()
                .unwrap();
        }
    }

    #[tokio::test]
    async fn result_withdrawal_cannot_exempt_root_foreign_context_or_foreign_activation() {
        for mode in 0..3 {
            let runtime = Handle::current();
            let execution = execution(830 + mode);
            let frontend = FrontendProcessId::new_v7();
            let root_context =
                QueryContextRef::new(execution, frontend, BackendProcessId::new_v7());
            let unused = QueryContextRef::new(execution, frontend, BackendProcessId::new_v7());
            let (work_owner, stage) = test_governed_work();
            let config = LogicalExecutionActorConfig::single_attempt_read_rows(
                execution,
                NonZeroUsize::new(4).unwrap(),
                vec![root_context, unused],
                NonZeroUsize::new(2).unwrap(),
                NonZeroUsize::new(2).unwrap(),
                work_owner,
                stage,
                result_schema(),
                NonZeroUsize::new(1).unwrap(),
                result_carrier(),
            )
            .unwrap()
            .with_result_row_carrier(result_carrier())
            .with_abort_query_context_effect_port(
                super::super::PermanentlyBackpressuredAbortEffectPort::shared(),
                NonZeroUsize::new(2).unwrap(),
            );
            let (owner, initial, output) = spawn_logical_execution_actor(&runtime, config)
                .unwrap()
                .into_parts();
            let ExecutionOutput::Rows(mut stream) = output.into_output() else {
                panic!("Client rows required")
            };
            stream.begin_schema().unwrap().complete();
            let actor = owner.actor().clone();
            let running = actor.activate(initial.ready()).await.unwrap();
            let activation = running.identity();
            let root = TaskIdentity::new(
                execution,
                StageId::new(30).unwrap(),
                TaskId::new(30).unwrap(),
                root_context.backend_process_id(),
            );
            let observer = running.bind_root_result(root).await.unwrap();
            observer
                .observe_status(root_status(root, 1, TaskState::Finished))
                .await
                .unwrap();
            observer
                .observe_local_root_end(root, result_end(0))
                .await
                .unwrap();
            let fact = match mode {
                0 => ResultContextWithdrawal::new(activation, root, root_context),
                1 => ResultContextWithdrawal::new(
                    activation,
                    root,
                    QueryContextRef::new(execution, frontend, BackendProcessId::new_v7()),
                ),
                _ => ResultContextWithdrawal::new(
                    AttemptActivationIdentity::from_parts(
                        activation.actor().get(),
                        execution,
                        activation.generation() + 1,
                    ),
                    root,
                    unused,
                ),
            };
            assert!(matches!(
                running.finish_result_stream(vec![fact]).await,
                Err(RunningAttemptHandoffError::ExecutionConcluded(
                    LogicalConclusion::Failed
                ))
            ));
            assert!(
                stream.next().await.is_err(),
                "invalid evidence cannot enqueue success End"
            );
            assert!(
                actor
                    .establish_snapshot(root_context)
                    .await
                    .unwrap()
                    .is_none()
            );
            assert!(actor.establish_snapshot(unused).await.unwrap().is_none());
            drop((stream, observer, actor));
            tokio::time::timeout(
                Duration::from_secs(2),
                owner.into_residual_stand_down_supervisor().join(),
            )
            .await
            .unwrap()
            .unwrap();
        }
    }

    #[tokio::test]
    async fn finish_waits_for_the_establish_settlement_that_precedes_success_eof() {
        let runtime = Handle::current();
        let execution = execution(823);
        let frontend = FrontendProcessId::new_v7();
        let context = QueryContextRef::new(execution, frontend, BackendProcessId::new_v7());
        let (work_owner, stage) = test_governed_work();
        let config = LogicalExecutionActorConfig::single_attempt_read_rows(
            execution,
            NonZeroUsize::new(4).unwrap(),
            vec![context],
            NonZeroUsize::new(2).unwrap(),
            NonZeroUsize::new(2).unwrap(),
            work_owner,
            stage,
            result_schema(),
            NonZeroUsize::new(1).unwrap(),
            result_carrier(),
        )
        .unwrap()
        .with_result_row_carrier(result_carrier())
        .with_abort_query_context_effect_port(
            super::super::PermanentlyBackpressuredAbortEffectPort::shared(),
            NonZeroUsize::new(2).unwrap(),
        );
        let (owner, initial, output) = spawn_logical_execution_actor(&runtime, config)
            .unwrap()
            .into_parts();
        let ExecutionOutput::Rows(mut stream) = output.into_output() else {
            panic!("row execution must expose its result stream");
        };
        stream.begin_schema().unwrap().complete();
        let actor = owner.actor().clone();
        let running = actor.activate(initial.ready()).await.unwrap();
        let ticket = admission_ticket(context, 23);
        let admission = running
            .begin_admission_issue(admission_request(context, 23))
            .await
            .unwrap();
        running
            .settle_admission_issue(
                admission,
                AdmissionIssueSettlement::applied(
                    admission.operation_id(),
                    OperationOutcome::Accepted,
                    ticket,
                )
                .unwrap(),
            )
            .await
            .unwrap();
        let establish = running
            .authorize_establish(
                establish_request(context, ticket, 23),
                NativeCompatibilityId::new([23; 32]),
            )
            .await
            .unwrap();
        let transport = HoldingEstablishTransport::default();
        assert_eq!(
            establish.try_submit(&transport).unwrap(),
            EstablishIssueSubmit::Accepted
        );

        let root = root_task(execution, 23);
        let observer = running.bind_root_result(root).await.unwrap();
        observer
            .observe_status(root_status(root, 1, TaskState::Finished))
            .await
            .unwrap();
        observer
            .observe_local_root_end(root, result_end(0))
            .await
            .unwrap();
        let finish = tokio::spawn(async move { running.finish_result_stream(Vec::new()).await });
        tokio::task::yield_now().await;
        assert!(
            !finish.is_finished(),
            "success EOF must wait for the exact Establish Worker settlement"
        );

        transport
            .take()
            .worker_settled(OperationOutcome::Accepted)
            .unwrap();
        let ResultDelivery::End(end) = stream.next().await.unwrap().unwrap() else {
            panic!("settled Establish must permit success EOF");
        };
        end.complete();
        assert_eq!(finish.await.unwrap().unwrap(), LogicalConclusion::Succeeded);
        drop(stream);
        drop(actor);
        drop(owner);
    }

    #[tokio::test]
    async fn replacement_refusals_preserve_closed_policy() {
        let runtime = Handle::current();
        let first = execution(83);
        let second = replacement(first, 2);
        let (owner, permit, _output) = spawn_logical_execution_actor(&runtime, config(first))
            .unwrap()
            .into_parts();
        let actor = owner.actor();
        let running = actor.activate(permit.ready()).await.unwrap();
        assert_eq!(
            actor
                .begin_replacement(
                    running,
                    AttemptFailureClass::RecoverableInfrastructure,
                    second,
                    Vec::new(),
                )
                .await
                .unwrap_err(),
            LogicalExecutionActorError::RecoveryRefused(RecoveryRefusal::Mode)
        );
        wait_for_conclusion(actor, LogicalConclusion::Failed).await;

        let port = Arc::new(DelayedQualificationPort::default());
        let (owner, permit, _output) =
            spawn_logical_execution_actor(&runtime, recovery_config(execution(84), port, 2))
                .unwrap()
                .into_parts();
        let actor = owner.actor();
        let running = actor.activate(permit.ready()).await.unwrap();
        assert_eq!(
            actor
                .begin_replacement(
                    running,
                    AttemptFailureClass::RecoverableInfrastructure,
                    execution(999),
                    Vec::new(),
                )
                .await
                .unwrap_err(),
            LogicalExecutionActorError::WrongExecution
        );
        wait_for_conclusion(actor, LogicalConclusion::Failed).await;

        let first = execution(842);
        let port = Arc::new(DelayedQualificationPort::default());
        let (owner, permit, _output) =
            spawn_logical_execution_actor(&runtime, recovery_config(first, port, 2))
                .unwrap()
                .into_parts();
        let actor = owner.actor();
        let running = actor.activate(permit.ready()).await.unwrap();
        assert_eq!(
            actor
                .begin_replacement(
                    running,
                    AttemptFailureClass::ContractViolation,
                    replacement(first, 2),
                    Vec::new(),
                )
                .await
                .unwrap_err(),
            LogicalExecutionActorError::RecoveryRefused(RecoveryRefusal::FailureClass)
        );
        wait_for_conclusion(actor, LogicalConclusion::Failed).await;

        let port = Arc::new(DelayedQualificationPort::default());
        let (work_owner, stage) = test_governed_work();
        assert_eq!(
            LogicalExecutionActorConfig::read_only_pre_visibility_recovery(
                execution(843),
                NonZeroUsize::new(1).unwrap(),
                Vec::new(),
                NonZeroUsize::new(1).unwrap(),
                NonZeroUsize::new(1).unwrap(),
                NonZeroU32::new(1).unwrap(),
                port,
                work_owner,
                stage,
                Duration::from_secs(30),
            )
            .unwrap_err(),
            LogicalExecutionActorError::RecoveryRefused(RecoveryRefusal::AttemptBudget)
        );
    }

    #[tokio::test]
    async fn accepted_cancellation_returns_its_conclusion_to_late_initial_commands() {
        for reason in [
            CancellationReason::Requested,
            CancellationReason::DeadlineExceeded,
        ] {
            for fail_initialization in [false, true] {
                let (parent, work_owner, stage) = test_governed_child_work(None);
                let config = LogicalExecutionActorConfig::single_attempt_completion(
                    execution(88001),
                    ExecutionEffect::None,
                    NonZeroUsize::new(4).unwrap(),
                    Vec::new(),
                    NonZeroUsize::new(2).unwrap(),
                    NonZeroUsize::new(2).unwrap(),
                    work_owner,
                    stage,
                )
                .unwrap();
                let (owner, initial, _output) =
                    spawn_logical_execution_actor(&Handle::current(), config)
                        .unwrap()
                        .into_parts();
                let actor = owner.actor().clone();
                let conclusion = if matches!(reason, CancellationReason::DeadlineExceeded) {
                    LogicalConclusion::Failed
                } else {
                    LogicalConclusion::Cancelled
                };
                parent.cancel(reason.clone());
                wait_for_conclusion(&actor, conclusion).await;
                let result = if fail_initialization {
                    actor.initialization_failed(initial).await.map(|_| ())
                } else {
                    actor.activate(initial.ready()).await.map(|_| ())
                };
                assert_eq!(
                    result,
                    Err(LogicalExecutionActorError::ExecutionConcluded(conclusion))
                );
                assert_eq!(actor.snapshot().await.unwrap().conclusion, Some(conclusion));
                drop(actor);
                owner
                    .into_residual_stand_down_supervisor()
                    .join()
                    .await
                    .unwrap();
                parent.complete();
            }
        }
    }

    #[tokio::test]
    async fn accepted_cancellation_does_not_turn_late_failure_or_replacement_into_a_new_verdict() {
        for reason in [
            CancellationReason::Requested,
            CancellationReason::DeadlineExceeded,
        ] {
            for command in 0..3 {
                let (parent, work_owner, stage) = test_governed_child_work(None);
                let port = Arc::new(DelayedQualificationPort::default());
                let submissions = Arc::clone(&port.submissions);
                let first = execution(88002);
                let config = LogicalExecutionActorConfig::read_only_pre_visibility_recovery(
                    first,
                    NonZeroUsize::new(4).unwrap(),
                    Vec::new(),
                    NonZeroUsize::new(2).unwrap(),
                    NonZeroUsize::new(2).unwrap(),
                    NonZeroU32::new(2).unwrap(),
                    port,
                    work_owner,
                    stage,
                    Duration::from_secs(30),
                )
                .unwrap()
                .with_result_stream(result_schema(), NonZeroUsize::new(1).unwrap())
                .with_result_row_carrier(result_carrier());
                let (owner, initial, output) =
                    spawn_logical_execution_actor(&Handle::current(), config)
                        .unwrap()
                        .into_parts();
                let ExecutionOutput::Rows(mut stream) = output.into_output() else {
                    panic!("the recovery fixture owns a result stream");
                };
                stream.begin_schema().unwrap().complete();
                let actor = owner.actor().clone();
                let running = actor.activate(initial.ready()).await.unwrap();
                let conclusion = if matches!(reason, CancellationReason::DeadlineExceeded) {
                    LogicalConclusion::Failed
                } else {
                    LogicalConclusion::Cancelled
                };
                parent.cancel(reason.clone());
                wait_for_conclusion(&actor, conclusion).await;
                let result = match command {
                    0 => actor
                        .fail_attempt_with_error(
                            running,
                            result_protocol_failure("late producer failure must remain diagnostic"),
                        )
                        .await
                        .map(|_| ()),
                    1 => actor.complete_attempt(running).await.map(|_| ()),
                    _ => actor
                        .begin_replacement(
                            running,
                            AttemptFailureClass::RecoverableInfrastructure,
                            replacement(first, 2),
                            Vec::new(),
                        )
                        .await
                        .map(|_| ()),
                };
                assert_eq!(
                    result,
                    Err(LogicalExecutionActorError::ExecutionConcluded(conclusion))
                );
                assert_eq!(actor.snapshot().await.unwrap().conclusion, Some(conclusion));
                assert!(submissions.lock().unwrap().is_empty());
                let error = match stream.next().await {
                    Err(error) => error,
                    Ok(_) => panic!("accepted cancellation must retain its public failure"),
                };
                assert_eq!(
                    error.kind(),
                    if matches!(reason, CancellationReason::DeadlineExceeded) {
                        QueryExecutionErrorKind::DeadlineExceeded
                    } else {
                        QueryExecutionErrorKind::Cancelled
                    }
                );
                drop(stream);
                drop(actor);
                owner
                    .into_residual_stand_down_supervisor()
                    .join()
                    .await
                    .unwrap();
                parent.complete();
            }
        }
    }

    #[tokio::test]
    async fn accepted_cancellation_settles_held_and_pending_replacement_activation_with_its_conclusion()
     {
        for reason in [
            CancellationReason::Requested,
            CancellationReason::DeadlineExceeded,
        ] {
            for already_enqueued in [false, true] {
                let (parent, work_owner, stage) = test_governed_child_work(None);
                let port = Arc::new(DelayedQualificationPort::default());
                let submissions = Arc::clone(&port.submissions);
                let first = execution(88003);
                let config = LogicalExecutionActorConfig::read_only_pre_visibility_recovery(
                    first,
                    NonZeroUsize::new(4).unwrap(),
                    Vec::new(),
                    NonZeroUsize::new(2).unwrap(),
                    NonZeroUsize::new(2).unwrap(),
                    NonZeroU32::new(2).unwrap(),
                    port,
                    work_owner,
                    stage,
                    Duration::from_secs(30),
                )
                .unwrap()
                .with_result_stream(result_schema(), NonZeroUsize::new(1).unwrap())
                .with_result_row_carrier(result_carrier());
                let (owner, initial, _output) =
                    spawn_logical_execution_actor(&Handle::current(), config)
                        .unwrap()
                        .into_parts();
                let actor = owner.actor().clone();
                let running = actor.activate(initial.ready()).await.unwrap();
                let qualification = actor
                    .begin_replacement(
                        running,
                        AttemptFailureClass::RecoverableInfrastructure,
                        replacement(first, 2),
                        Vec::new(),
                    )
                    .await
                    .unwrap();
                let mut submission = Some(submissions.lock().unwrap().pop().unwrap());
                let conclusion = if matches!(reason, CancellationReason::DeadlineExceeded) {
                    LogicalConclusion::Failed
                } else {
                    LogicalConclusion::Cancelled
                };
                let result = if already_enqueued {
                    let mut activation = Box::pin(actor.activate_replacement(qualification));
                    assert!(matches!(
                        std::future::poll_fn(|cx| Poll::Ready(activation.as_mut().poll(cx))).await,
                        Poll::Pending
                    ));
                    parent.cancel(reason.clone());
                    activation.await
                } else {
                    parent.cancel(reason.clone());
                    wait_for_conclusion(&actor, conclusion).await;
                    submission
                        .take()
                        .unwrap()
                        .refused(ReplacementQualificationFailure::Expired);
                    loop {
                        if actor.snapshot().await.unwrap().replacement_error.is_some() {
                            break;
                        }
                        tokio::task::yield_now().await;
                    }
                    actor.activate_replacement(qualification).await
                };
                assert!(matches!(
                    result,
                    Err(LogicalExecutionActorError::ExecutionConcluded(actual)) if actual == conclusion
                ));
                assert_eq!(actor.snapshot().await.unwrap().conclusion, Some(conclusion));
                // A pending activation still owns its dispatched effect; the
                // held case already consumed the late refusal above.
                if let Some(submission) = submission.take() {
                    qualify(submission);
                }
                drop(actor);
                owner
                    .into_residual_stand_down_supervisor()
                    .join()
                    .await
                    .unwrap();
                parent.complete();
            }
        }
    }

    #[tokio::test]
    async fn inherited_work_cancellation_concludes_a_running_execution() {
        let runtime = Handle::current();
        let first = execution(880);
        let (parent, work_owner, stage) = test_governed_child_work(None);
        let config = LogicalExecutionActorConfig::single_attempt_completion(
            first,
            ExecutionEffect::None,
            NonZeroUsize::new(4).unwrap(),
            Vec::new(),
            NonZeroUsize::new(2).unwrap(),
            NonZeroUsize::new(2).unwrap(),
            work_owner,
            stage,
        )
        .unwrap();
        let (owner, initial, _output) = spawn_logical_execution_actor(&runtime, config)
            .unwrap()
            .into_parts();
        let actor = owner.actor().clone();
        let running = actor.activate(initial.ready()).await.unwrap();

        parent.cancel(CancellationReason::Requested);
        wait_for_conclusion(&actor, LogicalConclusion::Cancelled).await;
        drop(running);
        drop(actor);
        owner
            .into_residual_stand_down_supervisor()
            .join()
            .await
            .unwrap();
        parent.complete();
    }

    #[tokio::test]
    async fn inherited_deadline_is_preserved_on_the_result_stream() {
        let runtime = Handle::current();
        let first = execution(8801);
        let (parent, work_owner, stage) = test_governed_child_work(None);
        let config = LogicalExecutionActorConfig::read_only_pre_visibility_recovery(
            first,
            NonZeroUsize::new(4).unwrap(),
            Vec::new(),
            NonZeroUsize::new(2).unwrap(),
            NonZeroUsize::new(2).unwrap(),
            NonZeroU32::new(2).unwrap(),
            Arc::new(DelayedQualificationPort::default()),
            work_owner,
            stage,
            Duration::from_secs(30),
        )
        .unwrap()
        .with_result_stream(result_schema(), NonZeroUsize::new(1).unwrap())
        .with_result_row_carrier(result_carrier());
        let (owner, initial, output) = spawn_logical_execution_actor(&runtime, config)
            .unwrap()
            .into_parts();
        let ExecutionOutput::Rows(mut stream) = output.into_output() else {
            panic!("result execution must expose its row stream");
        };
        stream.begin_schema().unwrap().complete();
        let actor = owner.actor().clone();
        let running = actor.activate(initial.ready()).await.unwrap();

        parent.cancel(CancellationReason::DeadlineExceeded);
        wait_for_conclusion(&actor, LogicalConclusion::Failed).await;
        let error = match stream.next().await {
            Err(error) => error,
            Ok(_) => panic!("deadline must terminate the result stream"),
        };
        assert_eq!(error.kind(), QueryExecutionErrorKind::DeadlineExceeded);
        assert_eq!(
            error.message(),
            "logical execution deadline expired before success EOF"
        );

        drop(running);
        drop(stream);
        drop(actor);
        owner
            .into_residual_stand_down_supervisor()
            .join()
            .await
            .unwrap();
        parent.complete();
    }

    #[tokio::test]
    async fn pump_cancellation_handoff_waits_for_actor_deadline_conclusion() {
        let runtime = Handle::current();
        let first = execution(8802);
        let (parent, work_owner, stage) = test_governed_child_work(None);
        let config = LogicalExecutionActorConfig::single_attempt_completion(
            first,
            ExecutionEffect::None,
            NonZeroUsize::new(4).unwrap(),
            Vec::new(),
            NonZeroUsize::new(2).unwrap(),
            NonZeroUsize::new(2).unwrap(),
            work_owner,
            stage,
        )
        .unwrap()
        .with_result_stream(result_schema(), NonZeroUsize::new(1).unwrap())
        .with_result_row_carrier(result_carrier());
        let (owner, initial, output) = spawn_logical_execution_actor(&runtime, config)
            .unwrap()
            .into_parts();
        let ExecutionOutput::Rows(mut stream) = output.into_output() else {
            panic!("result execution must expose its row stream");
        };
        stream.begin_schema().unwrap().complete();
        let actor = owner.actor().clone();
        let running = actor.activate(initial.ready()).await.unwrap();

        parent.cancel(CancellationReason::DeadlineExceeded);
        assert_eq!(
            running.await_work_cancellation().await.unwrap(),
            LogicalConclusion::Failed
        );
        let error = match stream.next().await {
            Err(error) => error,
            Ok(_) => panic!("deadline must terminate the result stream"),
        };
        assert_eq!(error.kind(), QueryExecutionErrorKind::DeadlineExceeded);

        drop(stream);
        drop(actor);
        owner
            .into_residual_stand_down_supervisor()
            .join()
            .await
            .unwrap();
        parent.complete();
    }

    #[tokio::test]
    async fn work_deadline_wins_when_running_permit_is_abandoned_in_the_same_tick() {
        let runtime = Handle::current();
        let first = execution(8803);
        let (parent, work_owner, stage) = test_governed_child_work(None);
        let config = LogicalExecutionActorConfig::single_attempt_completion(
            first,
            ExecutionEffect::None,
            NonZeroUsize::new(4).unwrap(),
            Vec::new(),
            NonZeroUsize::new(2).unwrap(),
            NonZeroUsize::new(2).unwrap(),
            work_owner,
            stage,
        )
        .unwrap()
        .with_result_stream(result_schema(), NonZeroUsize::new(1).unwrap())
        .with_result_row_carrier(result_carrier());
        let (owner, initial, output) = spawn_logical_execution_actor(&runtime, config)
            .unwrap()
            .into_parts();
        let ExecutionOutput::Rows(mut stream) = output.into_output() else {
            panic!("result execution must expose its row stream");
        };
        stream.begin_schema().unwrap().complete();
        let actor = owner.actor().clone();
        let running = actor.activate(initial.ready()).await.unwrap();

        parent.cancel(CancellationReason::DeadlineExceeded);
        drop(running);
        let error = match stream.next().await {
            Err(error) => error,
            Ok(_) => panic!("deadline must terminate the result stream"),
        };
        assert_eq!(error.kind(), QueryExecutionErrorKind::DeadlineExceeded);

        drop(stream);
        drop(actor);
        owner
            .into_residual_stand_down_supervisor()
            .join()
            .await
            .unwrap();
        parent.complete();
    }

    #[tokio::test(start_paused = true)]
    async fn inherited_work_deadline_cancels_backpressured_replacement() {
        let runtime = Handle::current();
        let first = execution(881);
        let second = replacement(first, 2);
        let port = Arc::new(RecoverableBackpressureQualificationPort::default());
        let submissions = Arc::clone(&port.submissions);
        let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
        let (parent, work_owner, stage) = test_governed_child_work(Some(deadline));
        let config = LogicalExecutionActorConfig::read_only_pre_visibility_recovery(
            first,
            NonZeroUsize::new(4).unwrap(),
            Vec::new(),
            NonZeroUsize::new(2).unwrap(),
            NonZeroUsize::new(2).unwrap(),
            NonZeroU32::new(2).unwrap(),
            port,
            work_owner,
            stage,
            Duration::from_secs(60),
        )
        .unwrap()
        .with_result_stream(
            ResultSchema::new(Vec::<crate::api::ResultField>::new()),
            NonZeroUsize::new(1).unwrap(),
        )
        .with_result_row_carrier(result_carrier());
        let (owner, initial, _output) = spawn_logical_execution_actor(&runtime, config)
            .unwrap()
            .into_parts();
        let actor = owner.actor().clone();
        let running = actor.activate(initial.ready()).await.unwrap();
        let qualification = actor
            .begin_replacement(
                running,
                AttemptFailureClass::RecoverableInfrastructure,
                second,
                Vec::new(),
            )
            .await
            .unwrap();

        tokio::time::advance(Duration::from_secs(31)).await;
        wait_for_conclusion(&actor, LogicalConclusion::Failed).await;
        assert_eq!(submissions.load(Ordering::SeqCst), 0);
        drop(qualification);
        drop(actor);
        owner
            .into_residual_stand_down_supervisor()
            .join()
            .await
            .unwrap();
        parent.complete();
    }

    #[tokio::test]
    async fn work_cancellation_abandons_qualified_successor_while_authority_is_held() {
        let runtime = Handle::current();
        let first = execution(882);
        let second = replacement(first, 2);
        let failed_context = QueryContextRef::new(
            first,
            FrontendProcessId::new_v7(),
            BackendProcessId::new_v7(),
        );
        let port = Arc::new(DelayedQualificationPort::default());
        let submissions = Arc::clone(&port.submissions);
        let (parent, work_owner, stage) = test_governed_child_work(None);
        let config = LogicalExecutionActorConfig::read_only_pre_visibility_recovery(
            first,
            NonZeroUsize::new(4).unwrap(),
            vec![failed_context],
            NonZeroUsize::new(2).unwrap(),
            NonZeroUsize::new(2).unwrap(),
            NonZeroU32::new(2).unwrap(),
            port,
            work_owner,
            stage,
            Duration::from_secs(60),
        )
        .unwrap()
        .with_result_stream(
            ResultSchema::new(Vec::<crate::api::ResultField>::new()),
            NonZeroUsize::new(1).unwrap(),
        )
        .with_result_row_carrier(result_carrier())
        .with_abort_query_context_effect_port(
            super::super::PermanentlyBackpressuredAbortEffectPort::shared(),
            NonZeroUsize::new(2).unwrap(),
        );
        let (owner, initial, _output) = spawn_logical_execution_actor(&runtime, config)
            .unwrap()
            .into_parts();
        let actor = owner.actor().clone();
        let running = actor.activate(initial.ready()).await.unwrap();
        let qualification = actor
            .begin_replacement(
                running,
                AttemptFailureClass::RecoverableInfrastructure,
                second,
                Vec::new(),
            )
            .await
            .unwrap();
        let submission = submissions.lock().unwrap().pop().unwrap();
        let request = submission.request();
        let abandoned = Arc::new(AtomicUsize::new(0));
        let reservation = QualifiedReplacementReservation::try_new(
            request,
            Box::new(CountingIsolationReservation {
                identity: request.identity(),
                abandoned: Arc::clone(&abandoned),
                admissions: Vec::new().into_boxed_slice(),
            }),
        )
        .unwrap();
        submission.qualified(reservation).unwrap();
        actor.snapshot().await.unwrap();

        parent.cancel(CancellationReason::Requested);
        wait_for_conclusion(&actor, LogicalConclusion::Cancelled).await;
        assert_eq!(abandoned.load(Ordering::SeqCst), 1);
        drop(qualification);
        drop(actor);
        let supervisor = owner.into_residual_stand_down_supervisor();
        for _ in 0..16 {
            tokio::task::yield_now().await;
        }
        assert!(
            !supervisor.is_finished(),
            "work ownership must outlive successor cleanup until the failed Worker converges"
        );
        supervisor
            .observe_worker_stopped_and_context_fenced(failed_context)
            .await
            .unwrap();
        supervisor.join().await.unwrap();
        parent.complete();
    }

    #[tokio::test]
    async fn replacement_receipt_rejects_exact_header_mismatch_and_preserves_domain_refusal() {
        use novarocks_execution_contract::failure::{FailureClass, FailureProjection};
        for fault in 0..3 {
            let first = execution(10_100 + fault);
            let port = Arc::new(DelayedQualificationPort::default());
            let submissions = Arc::clone(&port.submissions);
            let config = recovery_config(first, port, 2);
            let scope = config.work_owner.as_ref().unwrap().scope().clone();
            let (owner, initial, output) =
                spawn_logical_execution_actor(&Handle::current(), config)
                    .unwrap()
                    .into_parts();
            let (actor, join) = owner.into_test_parts();
            let running = actor.activate(initial.ready()).await.unwrap();
            let qualification = actor
                .begin_replacement(
                    running,
                    AttemptFailureClass::RecoverableInfrastructure,
                    replacement(first, 2),
                    Vec::new(),
                )
                .await
                .unwrap();
            let submission = submissions.lock().unwrap().pop().unwrap();
            let identity = submission.request().identity();
            let original_operation = submission.request().operation_id();
            let original =
                novarocks_workload_control::WorkError::CapacityWaitTimeout.failure_fact();
            if fault == 0 {
                submission.reject_with_header_for_test(TaskOperationId::new_v7(), identity);
            } else if fault == 1 {
                let foreign = ReplacementQualificationIdentity::new(
                    identity.actor(),
                    identity.failed(),
                    identity.replacement(),
                    identity.eligibility_generation().checked_add(1).unwrap(),
                );
                submission.reject_with_header_for_test(original_operation, foreign);
            } else {
                submission.refused(ReplacementQualificationFailure::DomainRefused(original));
            }
            let snapshot = wait_for_conclusion(&actor, LogicalConclusion::Failed).await;
            let refusal = snapshot.replacement_error.unwrap();
            if fault < 2 {
                assert_eq!(refusal, ReplacementQualificationFailure::MismatchedReceipt);
                assert_eq!(refusal.failure_fact().reason().code(), 100);
                assert_eq!(refusal.failure_fact().class(), FailureClass::Invariant);
            } else {
                assert_eq!(
                    refusal,
                    ReplacementQualificationFailure::DomainRefused(original)
                );
                assert_eq!(refusal.failure_fact(), original);
            }
            drop(qualification);
            drop(output);
            drop(actor);
            tokio::time::timeout(Duration::from_secs(1), join)
                .await
                .expect("the original actor joins after its effect verdict")
                .unwrap();
            tokio::time::timeout(Duration::from_secs(1), scope.wait_released())
                .await
                .expect("the original work scope releases after the actual actor join");
        }
    }

    #[tokio::test]
    async fn dropped_qualification_and_unknown_effect_cannot_lose_responsibility() {
        let runtime = Handle::current();
        let first = execution(85);
        let port = Arc::new(DelayedQualificationPort::default());
        let (owner, permit, _output) = spawn_logical_execution_actor(
            &runtime,
            recovery_config(first, Arc::clone(&port) as Arc<_>, 2),
        )
        .unwrap()
        .into_parts();
        let actor = owner.actor();
        let running = actor.activate(permit.ready()).await.unwrap();
        let qualification = actor
            .begin_replacement(
                running,
                AttemptFailureClass::RecoverableInfrastructure,
                replacement(first, 2),
                Vec::new(),
            )
            .await
            .unwrap();
        drop(qualification);
        wait_for_conclusion(actor, LogicalConclusion::Cancelled).await;

        let first = execution(86);
        let port = Arc::new(DelayedQualificationPort::default());
        let submissions = Arc::clone(&port.submissions);
        let (owner, permit, _output) =
            spawn_logical_execution_actor(&runtime, recovery_config(first, port, 2))
                .unwrap()
                .into_parts();
        let actor = owner.actor();
        let running = actor.activate(permit.ready()).await.unwrap();
        let _qualification = actor
            .begin_replacement(
                running,
                AttemptFailureClass::RecoverableInfrastructure,
                replacement(first, 2),
                Vec::new(),
            )
            .await
            .unwrap();
        drop(submissions.lock().unwrap().pop().unwrap());
        let snapshot = wait_for_conclusion(actor, LogicalConclusion::Failed).await;
        assert_eq!(
            snapshot.replacement_error,
            Some(ReplacementQualificationFailure::OutcomeUnknown)
        );
    }

    #[tokio::test]
    async fn dropping_pending_replacement_activation_abandons_the_logical_execution() {
        let runtime = Handle::current();
        let first = execution(861);
        let port = Arc::new(DelayedQualificationPort::default());
        let submissions = Arc::clone(&port.submissions);
        let (owner, initial, _output) =
            spawn_logical_execution_actor(&runtime, recovery_config(first, port, 2))
                .unwrap()
                .into_parts();
        let (actor, join) = owner.into_test_parts();
        let running = actor.activate(initial.ready()).await.unwrap();
        let qualification = actor
            .begin_replacement(
                running,
                AttemptFailureClass::RecoverableInfrastructure,
                replacement(first, 2),
                Vec::new(),
            )
            .await
            .unwrap();
        let mut activation = Box::pin(actor.activate_replacement(qualification));
        assert!(matches!(
            std::future::poll_fn(|context| Poll::Ready(activation.as_mut().poll(context))).await,
            Poll::Pending
        ));
        assert_eq!(submissions.lock().unwrap().len(), 1);

        drop(activation);
        wait_for_conclusion(&actor, LogicalConclusion::Cancelled).await;
        qualify(submissions.lock().unwrap().pop().unwrap());
        drop(actor);
        tokio::time::timeout(Duration::from_secs(1), join)
            .await
            .expect("submitted qualification must remain supervised after cancellation")
            .unwrap();
    }

    #[tokio::test]
    async fn conclusion_abandons_qualified_successor_before_old_residual_converges() {
        let runtime = Handle::current();
        let first = execution(874);
        let second = replacement(first, 2);
        let frontend = FrontendProcessId::new_v7();
        let old_context = QueryContextRef::new(first, frontend, BackendProcessId::new_v7());
        let unrelated = QueryContextRef::new(first, frontend, BackendProcessId::new_v7());
        let port = Arc::new(DelayedQualificationPort::default());
        let submissions = Arc::clone(&port.submissions);
        let abort_port = Arc::new(ClosingMixedAbortPort::new(old_context, unrelated));
        let abort_submissions = Arc::clone(&abort_port.submissions);
        let config = recovery_config_with_contexts(first, vec![old_context], port, 2)
            .with_abort_query_context_effect_port(abort_port, NonZeroUsize::new(2).unwrap());
        let (owner, initial, _output) = spawn_logical_execution_actor(&runtime, config)
            .unwrap()
            .into_parts();
        let actor = owner.actor().clone();
        let running = actor.activate(initial.ready()).await.unwrap();
        let ticket = admission_ticket(old_context, 61);
        let admission = running
            .begin_admission_issue(admission_request(old_context, 61))
            .await
            .unwrap();
        running
            .settle_admission_issue(
                admission,
                AdmissionIssueSettlement::applied(
                    admission.operation_id(),
                    OperationOutcome::Accepted,
                    ticket,
                )
                .unwrap(),
            )
            .await
            .unwrap();
        let establish = running
            .authorize_establish(
                establish_request(old_context, ticket, 61),
                NativeCompatibilityId::new([61; 32]),
            )
            .await
            .unwrap();
        let qualification = actor
            .begin_replacement(
                running,
                AttemptFailureClass::RecoverableInfrastructure,
                second,
                Vec::new(),
            )
            .await
            .unwrap();
        let submission = submissions.lock().unwrap().pop().unwrap();
        let request = submission.request();
        let abandoned = Arc::new(AtomicUsize::new(0));
        let reservation = QualifiedReplacementReservation::try_new(
            request,
            Box::new(CountingIsolationReservation {
                identity: request.identity(),
                abandoned: Arc::clone(&abandoned),
                admissions: Vec::new().into_boxed_slice(),
            }),
        )
        .unwrap();
        submission.qualified(reservation).unwrap();
        // The receipt branch is biased ahead of mailbox commands, so this
        // snapshot proves the actor has taken ownership of the reservation.
        actor.snapshot().await.unwrap();
        assert_eq!(abandoned.load(Ordering::SeqCst), 0);

        drop(qualification);
        wait_for_conclusion(&actor, LogicalConclusion::Cancelled).await;
        assert_eq!(
            abandoned.load(Ordering::SeqCst),
            1,
            "successor grants must be abandoned immediately at logical conclusion"
        );

        drop(establish);
        let abort = tokio::time::timeout(Duration::from_secs(1), async {
            loop {
                if let Some(submission) = abort_submissions.lock().unwrap().pop() {
                    break submission;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("the old attempt must remain under Abort supervision");
        abort
            .worker_settled(QueryContextReceipt::new(
                old_context,
                QueryContextState::TerminalRetained,
            ))
            .unwrap();
        let supervisor = owner.into_residual_stand_down_supervisor();
        drop(actor);
        for _ in 0..16 {
            tokio::task::yield_now().await;
        }
        assert!(!supervisor.is_finished());
        supervisor
            .observe_worker_stopped_and_context_fenced(old_context)
            .await
            .unwrap();
        tokio::time::timeout(Duration::from_secs(1), supervisor.join())
            .await
            .expect("old residual convergence must finish actor cleanup")
            .unwrap();
    }

    #[test]
    fn qualified_worker_admission_rejects_a_foreign_receipt_context() {
        let first = execution(864);
        let second = replacement(first, 2);
        let frontend = FrontendProcessId::new_v7();
        let expected = QueryContextRef::new(second, frontend, BackendProcessId::new_v7());
        let foreign = QueryContextRef::new(second, frontend, BackendProcessId::new_v7());
        assert_eq!(
            QualifiedWorkerAdmission::try_new(
                second,
                admission_request(expected, 41),
                admission_ticket(foreign, 41),
            )
            .unwrap_err(),
            ReplacementQualificationFailure::InvalidReservation
        );
    }

    #[tokio::test]
    async fn actor_clock_expires_qualified_capacity_before_activation() {
        let runtime = Handle::current();
        let first = execution(866);
        let port = Arc::new(DelayedQualificationPort::default());
        let submissions = Arc::clone(&port.submissions);
        let clock = Arc::new(ManualActorClock::new(MonotonicInstant::ORIGIN));
        let config = recovery_config(first, port, 2).with_clock(clock.clone());
        let (owner, initial, _output) = spawn_logical_execution_actor(&runtime, config)
            .unwrap()
            .into_parts();
        let actor = owner.actor();
        let running = actor.activate(initial.ready()).await.unwrap();
        let qualification = actor
            .begin_replacement(
                running,
                AttemptFailureClass::RecoverableInfrastructure,
                replacement(first, 2),
                Vec::new(),
            )
            .await
            .unwrap();
        let submission = submissions.lock().unwrap().pop().unwrap();
        clock.set(MonotonicInstant::from_origin(Duration::from_secs(31)));
        qualify(submission);
        assert_eq!(
            actor.activate_replacement(qualification).await.unwrap_err(),
            LogicalExecutionActorError::ReplacementQualificationFailed(
                ReplacementQualificationFailure::Expired,
            )
        );
        drop(owner);
    }

    #[tokio::test]
    async fn replacement_deadline_cancels_a_hung_effect_and_releases_the_execution_stage() {
        let runtime = Handle::current();
        let first = execution(869);
        let port = Arc::new(DelayedQualificationPort::default());
        let submissions = Arc::clone(&port.submissions);
        let clock = Arc::new(ManualActorClock::new(MonotonicInstant::ORIGIN));
        let (work_owner, stage) = test_governed_work();
        let scope = work_owner.scope();
        let config = LogicalExecutionActorConfig::read_only_pre_visibility_recovery(
            first,
            NonZeroUsize::new(4).unwrap(),
            Vec::new(),
            NonZeroUsize::new(2).unwrap(),
            NonZeroUsize::new(2).unwrap(),
            NonZeroU32::new(2).unwrap(),
            port,
            work_owner,
            stage,
            Duration::from_secs(30),
        )
        .unwrap()
        .with_result_stream(
            ResultSchema::new(Vec::<crate::api::ResultField>::new()),
            NonZeroUsize::new(1).unwrap(),
        )
        .with_result_row_carrier(result_carrier())
        .with_clock(clock.clone());
        let (owner, initial, _output) = spawn_logical_execution_actor(&runtime, config)
            .unwrap()
            .into_parts();
        let actor = owner.actor();
        let running = actor.activate(initial.ready()).await.unwrap();
        let qualification = actor
            .begin_replacement(
                running,
                AttemptFailureClass::RecoverableInfrastructure,
                replacement(first, 2),
                Vec::new(),
            )
            .await
            .unwrap();
        let submission = submissions.lock().unwrap().pop().unwrap();
        assert_eq!(
            submission.request().conservative_expiry(),
            MonotonicInstant::from_origin(Duration::from_secs(30))
        );
        let cancellation = submission.subscribe_cancellation();

        clock.set(MonotonicInstant::from_origin(Duration::from_secs(31)));
        wait_for_conclusion(actor, LogicalConclusion::Failed).await;
        assert!(*cancellation.borrow());
        let replacement_stage = scope.try_acquire(Stage::Execution).unwrap();
        drop(replacement_stage);

        drop(qualification);
        drop(submission);
        drop(owner);
    }

    #[tokio::test]
    async fn qualified_replacement_expires_while_the_caller_holds_activation_authority() {
        let runtime = Handle::current();
        let first = execution(870);
        let port = Arc::new(DelayedQualificationPort::default());
        let submissions = Arc::clone(&port.submissions);
        let clock = Arc::new(ManualActorClock::new(MonotonicInstant::ORIGIN));
        let (work_owner, stage) = test_governed_work();
        let scope = work_owner.scope();
        let config = LogicalExecutionActorConfig::read_only_pre_visibility_recovery(
            first,
            NonZeroUsize::new(4).unwrap(),
            Vec::new(),
            NonZeroUsize::new(2).unwrap(),
            NonZeroUsize::new(2).unwrap(),
            NonZeroU32::new(2).unwrap(),
            port,
            work_owner,
            stage,
            Duration::from_secs(30),
        )
        .unwrap()
        .with_result_stream(
            ResultSchema::new(Vec::<crate::api::ResultField>::new()),
            NonZeroUsize::new(1).unwrap(),
        )
        .with_result_row_carrier(result_carrier())
        .with_clock(clock.clone());
        let (owner, initial, _output) = spawn_logical_execution_actor(&runtime, config)
            .unwrap()
            .into_parts();
        let actor = owner.actor();
        let running = actor.activate(initial.ready()).await.unwrap();
        let qualification = actor
            .begin_replacement(
                running,
                AttemptFailureClass::RecoverableInfrastructure,
                replacement(first, 2),
                Vec::new(),
            )
            .await
            .unwrap();
        qualify(submissions.lock().unwrap().pop().unwrap());

        clock.set(MonotonicInstant::from_origin(Duration::from_secs(31)));
        wait_for_conclusion(actor, LogicalConclusion::Failed).await;
        let replacement_stage = scope.try_acquire(Stage::Execution).unwrap();
        drop(replacement_stage);

        drop(qualification);
        drop(owner);
    }

    #[tokio::test]
    async fn replacement_reservation_rejects_incorrect_failed_context_proof() {
        let runtime = Handle::current();
        let first = execution(871);
        let second = replacement(first, 2);
        let frontend = FrontendProcessId::new_v7();
        let failed_context = QueryContextRef::new(first, frontend, BackendProcessId::new_v7());
        let foreign_failed_context =
            QueryContextRef::new(first, frontend, BackendProcessId::new_v7());
        let port = Arc::new(DelayedQualificationPort::default());
        let submissions = Arc::clone(&port.submissions);
        let config = recovery_config_with_contexts(first, vec![failed_context], port, 2)
            .with_abort_query_context_effect_port(
                super::super::PermanentlyBackpressuredAbortEffectPort::shared(),
                NonZeroUsize::new(2).unwrap(),
            );
        let (owner, initial, _output) = spawn_logical_execution_actor(&runtime, config)
            .unwrap()
            .into_parts();
        let actor = owner.actor();
        let running = actor.activate(initial.ready()).await.unwrap();
        let _qualification = actor
            .begin_replacement(
                running,
                AttemptFailureClass::RecoverableInfrastructure,
                second,
                Vec::new(),
            )
            .await
            .unwrap();
        let submission = submissions.lock().unwrap().pop().unwrap();
        let request = submission.request();
        assert_eq!(
            QualifiedReplacementReservation::try_new(
                request,
                Box::new(TestIsolationReservation {
                    identity: request.identity(),
                    failed_contexts: BTreeSet::from([foreign_failed_context]),
                    contexts: BTreeSet::new(),
                    admissions: Vec::new().into_boxed_slice(),
                }),
            )
            .unwrap_err(),
            ReplacementQualificationFailure::InvalidReservation
        );
        drop(submission);
        drop(owner);
    }

    #[tokio::test]
    async fn duplicate_replacement_contexts_fail_and_conclude_the_consumed_attempt() {
        let runtime = Handle::current();
        let first = execution(876);
        let second = replacement(first, 2);
        let frontend = FrontendProcessId::new_v7();
        let duplicate = QueryContextRef::new(second, frontend, BackendProcessId::new_v7());
        let port = Arc::new(DelayedQualificationPort::default());
        let (owner, initial, _output) =
            spawn_logical_execution_actor(&runtime, recovery_config(first, port, 2))
                .unwrap()
                .into_parts();
        let actor = owner.actor();
        let running = actor.activate(initial.ready()).await.unwrap();
        assert_eq!(
            actor
                .begin_replacement(
                    running,
                    AttemptFailureClass::RecoverableInfrastructure,
                    second,
                    vec![duplicate, duplicate],
                )
                .await
                .unwrap_err(),
            LogicalExecutionActorError::WrongExecution
        );
        wait_for_conclusion(actor, LogicalConclusion::Failed).await;
        drop(owner);
    }

    #[tokio::test]
    async fn replacement_reservation_rejects_a_foreign_isolation_identity() {
        let runtime = Handle::current();
        let first = execution(867);
        let second = replacement(first, 2);
        let port = Arc::new(DelayedQualificationPort::default());
        let submissions = Arc::clone(&port.submissions);
        let (owner, initial, _output) =
            spawn_logical_execution_actor(&runtime, recovery_config(first, port, 2))
                .unwrap()
                .into_parts();
        let actor = owner.actor();
        let running = actor.activate(initial.ready()).await.unwrap();
        let _qualification = actor
            .begin_replacement(
                running,
                AttemptFailureClass::RecoverableInfrastructure,
                second,
                Vec::new(),
            )
            .await
            .unwrap();
        let submission = submissions.lock().unwrap().pop().unwrap();
        let request = submission.request();
        let abandoned = Arc::new(AtomicUsize::new(0));
        let foreign = ReplacementQualificationIdentity::new(
            request.identity().actor(),
            first,
            replacement(first, 3),
            request.identity().eligibility_generation(),
        );
        assert_eq!(
            QualifiedReplacementReservation::try_new(
                request,
                Box::new(CountingIsolationReservation {
                    identity: foreign,
                    abandoned: Arc::clone(&abandoned),
                    admissions: Vec::new().into_boxed_slice(),
                }),
            )
            .unwrap_err(),
            ReplacementQualificationFailure::InvalidReservation
        );
        assert_eq!(abandoned.load(Ordering::SeqCst), 1);
        drop(submission);
        drop(owner);
    }

    #[tokio::test]
    async fn replacement_resources_are_consumed_once_and_drop_closes_active_ownership() {
        let runtime = Handle::current();
        let first = execution(868);
        let port = Arc::new(DelayedQualificationPort::default());
        let submissions = Arc::clone(&port.submissions);
        let (owner, initial, _output) =
            spawn_logical_execution_actor(&runtime, recovery_config(first, port, 2))
                .unwrap()
                .into_parts();
        let actor = owner.actor();
        let running = actor.activate(initial.ready()).await.unwrap();
        let qualification = actor
            .begin_replacement(
                running,
                AttemptFailureClass::RecoverableInfrastructure,
                replacement(first, 2),
                Vec::new(),
            )
            .await
            .unwrap();
        let submission = submissions.lock().unwrap().pop().unwrap();
        let request = submission.request();
        let abandoned = Arc::new(AtomicUsize::new(0));
        let reservation = QualifiedReplacementReservation::try_new(
            request,
            Box::new(CountingIsolationReservation {
                identity: request.identity(),
                abandoned: Arc::clone(&abandoned),
                admissions: Vec::new().into_boxed_slice(),
            }),
        )
        .unwrap();
        submission.qualified(reservation).unwrap();
        let successor = actor.activate_replacement(qualification).await.unwrap();
        assert!(
            successor
                .take_replacement_admission_evidence()
                .await
                .unwrap()
                .is_some()
        );
        assert!(
            successor
                .take_replacement_admission_evidence()
                .await
                .unwrap()
                .is_none()
        );
        drop(successor);
        wait_for_conclusion(actor, LogicalConclusion::Cancelled).await;
        assert_eq!(abandoned.load(Ordering::SeqCst), 1);
        drop(owner);
    }

    #[tokio::test]
    async fn failed_isolation_activation_returns_ownership_for_abandonment() {
        let runtime = Handle::current();
        let first = execution(883);
        let second = replacement(first, 2);
        let port = Arc::new(DelayedQualificationPort::default());
        let submissions = Arc::clone(&port.submissions);
        let (owner, initial, _output) =
            spawn_logical_execution_actor(&runtime, recovery_config(first, port, 2))
                .unwrap()
                .into_parts();
        let actor = owner.actor();
        let running = actor.activate(initial.ready()).await.unwrap();
        let qualification = actor
            .begin_replacement(
                running,
                AttemptFailureClass::RecoverableInfrastructure,
                second,
                Vec::new(),
            )
            .await
            .unwrap();
        let submission = submissions.lock().unwrap().pop().unwrap();
        let request = submission.request();
        let abandoned = Arc::new(AtomicUsize::new(0));
        let reservation = QualifiedReplacementReservation::try_new(
            request,
            Box::new(FailingIsolationReservation {
                identity: request.identity(),
                abandoned: Arc::clone(&abandoned),
                admissions: Vec::new().into_boxed_slice(),
            }),
        )
        .unwrap();
        submission.qualified(reservation).unwrap();

        assert_eq!(
            actor.activate_replacement(qualification).await.unwrap_err(),
            LogicalExecutionActorError::ReplacementQualificationFailed(
                ReplacementQualificationFailure::Rejected,
            )
        );
        assert_eq!(abandoned.load(Ordering::SeqCst), 1);
        drop(owner);
    }

    #[tokio::test]
    async fn cancelled_admission_take_restores_the_exact_successor_bundle() {
        let runtime = Handle::current();
        let first = execution(875);
        let second = replacement(first, 2);
        let frontend = FrontendProcessId::new_v7();
        let new_context = QueryContextRef::new(second, frontend, BackendProcessId::new_v7());
        let port = Arc::new(DelayedQualificationPort::default());
        let submissions = Arc::clone(&port.submissions);
        let (owner, initial, _output) =
            spawn_logical_execution_actor(&runtime, recovery_config(first, port, 2))
                .unwrap()
                .into_parts();
        let actor = owner.actor();
        let running = actor.activate(initial.ready()).await.unwrap();
        let qualification = actor
            .begin_replacement(
                running,
                AttemptFailureClass::RecoverableInfrastructure,
                second,
                vec![new_context],
            )
            .await
            .unwrap();
        qualify(submissions.lock().unwrap().pop().unwrap());
        let successor = actor.activate_replacement(qualification).await.unwrap();
        let (reply, response) = oneshot::channel();
        actor
            .sender
            .send(ActorCommand::TakeReplacementAdmissionEvidence {
                activation: successor.identity(),
                reply,
            })
            .await
            .unwrap();
        drop(response);
        actor.snapshot().await.unwrap();

        let admissions = successor
            .take_replacement_admission_evidence()
            .await
            .unwrap()
            .expect("a cancelled receiver must not consume the move-only admission bundle");
        assert_eq!(admissions.len(), 1);
        assert_eq!(admissions[0].context(), new_context);
        drop(successor);
        wait_for_conclusion(actor, LogicalConclusion::Cancelled).await;
        drop(owner);
    }

    #[tokio::test]
    async fn successful_replacement_finishes_isolation_only_after_resource_convergence() {
        let runtime = Handle::current();
        let first = execution(878);
        let second = replacement(first, 2);
        let frontend = FrontendProcessId::new_v7();
        let new_context = QueryContextRef::new(second, frontend, BackendProcessId::new_v7());
        let port = Arc::new(DelayedQualificationPort::default());
        let submissions = Arc::clone(&port.submissions);
        let mut config = recovery_config(first, port, 2).with_abort_query_context_effect_port(
            super::super::PermanentlyBackpressuredAbortEffectPort::shared(),
            NonZeroUsize::new(2).unwrap(),
        );
        config.output_mode = LogicalOutputMode::CompletionOnly;
        let (owner, initial, _output) = spawn_logical_execution_actor(&runtime, config)
            .unwrap()
            .into_parts();
        let actor = owner.actor().clone();
        let running = actor.activate(initial.ready()).await.unwrap();
        let qualification = actor
            .begin_replacement(
                running,
                AttemptFailureClass::RecoverableInfrastructure,
                second,
                vec![new_context],
            )
            .await
            .unwrap();
        let submission = submissions.lock().unwrap().pop().unwrap();
        let request = submission.request();
        let admission = QualifiedWorkerAdmission::try_new(
            second,
            admission_request(new_context, 81),
            admission_ticket(new_context, 81),
        )
        .unwrap();
        let finished = Arc::new(AtomicUsize::new(0));
        let abandoned = Arc::new(AtomicUsize::new(0));
        let reservation = QualifiedReplacementReservation::try_new(
            request,
            Box::new(LifecycleIsolationReservation {
                identity: request.identity(),
                failed_contexts: BTreeSet::new(),
                contexts: BTreeSet::from([new_context]),
                admissions: vec![admission].into_boxed_slice(),
                finished: Arc::clone(&finished),
                abandoned: Arc::clone(&abandoned),
            }),
        )
        .unwrap();
        submission.qualified(reservation).unwrap();
        let successor = actor.activate_replacement(qualification).await.unwrap();
        let admissions = successor
            .take_replacement_admission_evidence()
            .await
            .unwrap()
            .unwrap();
        let successor = actor.activate(successor.ready()).await.unwrap();
        let establish = successor
            .authorize_establish(
                establish_request(new_context, admissions[0].receipt(), 81),
                NativeCompatibilityId::new([81; 32]),
            )
            .await
            .unwrap();
        let transport = HoldingEstablishTransport::default();
        assert_eq!(
            establish.try_submit(&transport).unwrap(),
            EstablishIssueSubmit::Accepted
        );
        transport
            .take()
            .worker_settled(OperationOutcome::Accepted)
            .unwrap();
        let before_completion = actor.snapshot().await.unwrap();
        assert!(matches!(
            before_completion.phase,
            ExecutionPhase::Running { execution, .. } if execution == second
        ));
        assert_eq!(before_completion.conclusion, None);
        assert_eq!(
            actor.complete_attempt(successor).await.unwrap(),
            LogicalConclusion::Succeeded
        );
        assert_eq!(finished.load(Ordering::SeqCst), 0);
        assert_eq!(abandoned.load(Ordering::SeqCst), 0);

        let supervisor = owner.into_residual_stand_down_supervisor();
        drop(actor);
        for _ in 0..16 {
            tokio::task::yield_now().await;
        }
        assert!(!supervisor.is_finished());
        assert_eq!(finished.load(Ordering::SeqCst), 0);
        supervisor
            .observe_worker_stopped_and_context_fenced(new_context)
            .await
            .unwrap();
        tokio::time::timeout(Duration::from_secs(1), supervisor.join())
            .await
            .expect("positive Registry convergence must finish successor isolation")
            .unwrap();
        assert_eq!(finished.load(Ordering::SeqCst), 1);
        assert_eq!(abandoned.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn closed_replacement_capacity_does_not_starve_the_mailbox() {
        let runtime = Handle::current();
        let first = execution(862);
        let port = Arc::new(ClosingBackpressureQualificationPort::default());
        let (owner, initial, _output) = spawn_logical_execution_actor(
            &runtime,
            recovery_config(first, Arc::clone(&port) as Arc<_>, 2),
        )
        .unwrap()
        .into_parts();
        let actor = owner.actor();
        let running = actor.activate(initial.ready()).await.unwrap();
        let _qualification = actor
            .begin_replacement(
                running,
                AttemptFailureClass::RecoverableInfrastructure,
                replacement(first, 2),
                Vec::new(),
            )
            .await
            .unwrap();
        for _ in 0..16 {
            if port.reserve_calls.load(Ordering::SeqCst) > 0 {
                break;
            }
            tokio::task::yield_now().await;
        }
        assert_eq!(port.reserve_calls.load(Ordering::SeqCst), 1);
        port.close();

        let snapshot = tokio::time::timeout(Duration::from_secs(1), async {
            loop {
                let snapshot = actor.snapshot().await.unwrap();
                if snapshot.conclusion == Some(LogicalConclusion::Failed) {
                    break snapshot;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("closed capacity must not create a biased-select busy loop");
        assert_eq!(
            snapshot.replacement_error,
            Some(ReplacementQualificationFailure::EffectOwnerClosed)
        );
        assert_eq!(port.reserve_calls.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn cancelled_backpressured_replacement_never_submits_after_capacity_recovers() {
        let runtime = Handle::current();
        let first = execution(863);
        let port = Arc::new(RecoverableBackpressureQualificationPort::default());
        let submissions = Arc::clone(&port.submissions);
        let (owner, initial, _output) = spawn_logical_execution_actor(
            &runtime,
            recovery_config(first, Arc::clone(&port) as Arc<_>, 2),
        )
        .unwrap()
        .into_parts();
        let actor = owner.actor();
        let running = actor.activate(initial.ready()).await.unwrap();
        let qualification = actor
            .begin_replacement(
                running,
                AttemptFailureClass::RecoverableInfrastructure,
                replacement(first, 2),
                Vec::new(),
            )
            .await
            .unwrap();
        for _ in 0..16 {
            if port.reserve_calls.load(Ordering::SeqCst) > 0 {
                break;
            }
            tokio::task::yield_now().await;
        }
        assert_eq!(port.reserve_calls.load(Ordering::SeqCst), 1);

        let mut activation = Box::pin(actor.activate_replacement(qualification));
        assert!(matches!(
            std::future::poll_fn(|context| Poll::Ready(activation.as_mut().poll(context))).await,
            Poll::Pending
        ));
        drop(activation);
        wait_for_conclusion(actor, LogicalConclusion::Cancelled).await;

        port.recover_capacity();
        for _ in 0..16 {
            actor.snapshot().await.unwrap();
            tokio::task::yield_now().await;
        }
        assert_eq!(submissions.load(Ordering::SeqCst), 0);
        assert_eq!(port.reserve_calls.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn permit_keeps_actor_alive_after_owner_is_dropped() {
        let runtime = Handle::current();
        let (owner, permit, _output) =
            spawn_logical_execution_actor(&runtime, config(execution(9)))
                .unwrap()
                .into_parts();
        let (actor, join) = owner.into_test_parts();
        drop(actor);
        tokio::task::yield_now().await;
        assert!(!join.is_finished());
        drop(permit);
        for _ in 0..16 {
            if join.is_finished() {
                break;
            }
            tokio::task::yield_now().await;
        }
        assert!(join.is_finished());
        join.await.unwrap();
    }

    #[tokio::test]
    async fn actor_commits_logical_success_before_end_transport_receipt() {
        let runtime = Handle::current();
        let first = execution(103);
        let config = recovery_config(first, Arc::new(DelayedQualificationPort::default()), 2)
            .with_result_stream(result_schema(), NonZeroUsize::new(1).unwrap())
            .with_result_row_carrier(result_carrier());
        let (owner, initial, output) = spawn_logical_execution_actor(&runtime, config)
            .unwrap()
            .into_parts();
        let ExecutionOutput::Rows(mut stream) = output.into_output() else {
            panic!("result execution must expose its row stream");
        };
        stream.begin_schema().unwrap().complete();

        let actor = owner.actor().clone();
        let running = Arc::new(actor.activate(initial.ready()).await.unwrap());
        let root = root_task(first, 1);
        let observer = running.bind_root_result(root).await.unwrap();
        let (control, window_owner, capacity, segment) = result_segment(first, 1);
        let delivery_running = Arc::clone(&running);
        let delivery_task = tokio::spawn(async move {
            delivery_running
                .deliver_root_segment(
                    ResultPacketSequence::new(0),
                    segment,
                    Some((result_profile(), ClientRowStreamCursor::default())),
                    2,
                    crate::api::RootRelayResidentWindow::default(),
                )
                .await
        });

        let ResultDelivery::Segment(delivery) = stream.next().await.unwrap().unwrap() else {
            panic!("actor must enqueue one batch");
        };
        assert!(!delivery_task.is_finished());
        assert_eq!(capacity.snapshot().held_positions, [1, 0, 0, 0]);
        delivery.complete();
        delivery_task.await.unwrap().unwrap();
        assert_eq!(capacity.snapshot().held_positions, [0; 4]);

        let running = Arc::try_unwrap(running).expect("batch sender released its permit");
        observer
            .observe_status(root_status(root, 1, TaskState::Finished))
            .await
            .unwrap();
        observer
            .observe_local_root_end(root, result_end(1))
            .await
            .unwrap();
        let finish_task =
            tokio::spawn(async move { running.finish_result_stream(Vec::new()).await });
        let ResultDelivery::End(end) = stream.next().await.unwrap().unwrap() else {
            panic!("actor must enqueue success EOF");
        };
        assert_eq!(
            finish_task.await.unwrap().unwrap(),
            LogicalConclusion::Succeeded
        );
        assert_eq!(
            actor.snapshot().await.unwrap().conclusion,
            Some(LogicalConclusion::Succeeded)
        );
        end.complete();
        wait_for_conclusion(&actor, LogicalConclusion::Succeeded).await;
        window_owner.complete();
        drop(control);
        drop(stream);
        drop(actor);
        drop(owner);
    }

    #[tokio::test]
    async fn cancelled_delivery_waiter_can_recover_the_durable_ack_watermark() {
        let runtime = Handle::current();
        let first = execution(109);
        let config = recovery_config(first, Arc::new(DelayedQualificationPort::default()), 2)
            .with_result_stream(result_schema(), NonZeroUsize::new(1).unwrap())
            .with_result_row_carrier(result_carrier());
        let (owner, initial, output) = spawn_logical_execution_actor(&runtime, config)
            .unwrap()
            .into_parts();
        let ExecutionOutput::Rows(mut stream) = output.into_output() else {
            panic!("result execution must expose its row stream");
        };
        stream.begin_schema().unwrap().complete();
        let actor = owner.actor().clone();
        let running = Arc::new(actor.activate(initial.ready()).await.unwrap());
        let (control, window_owner, capacity, segment) = result_segment(first, 1);
        let delivery_running = Arc::clone(&running);
        let delivery_task = tokio::spawn(async move {
            delivery_running
                .deliver_root_segment(
                    ResultPacketSequence::new(0),
                    segment,
                    Some((result_profile(), ClientRowStreamCursor::default())),
                    2,
                    crate::api::RootRelayResidentWindow::default(),
                )
                .await
        });
        let ResultDelivery::Segment(delivery) = stream.next().await.unwrap().unwrap() else {
            panic!("actor must enqueue one batch");
        };

        delivery_task.abort();
        assert!(delivery_task.await.unwrap_err().is_cancelled());
        delivery.complete();
        for _ in 0..16 {
            let snapshot = actor.snapshot().await.unwrap();
            if snapshot.delivered_result_through == Some(ResultPacketSequence::new(0)) {
                break;
            }
            tokio::task::yield_now().await;
        }
        assert_eq!(
            actor.snapshot().await.unwrap().delivered_result_through,
            Some(ResultPacketSequence::new(0))
        );
        assert_eq!(capacity.snapshot().held_positions, [0; 4]);

        let running = Arc::try_unwrap(running).expect("cancelled waiter released its permit");
        assert_eq!(
            actor.fail_attempt(running).await.unwrap(),
            LogicalConclusion::Failed
        );
        window_owner.complete();
        drop(control);
        drop(stream);
        drop(actor);
        drop(owner);
    }

    #[tokio::test]
    async fn foreign_failed_permit_cannot_install_a_canonical_cause_in_another_actor() {
        use crate::execution_error::{ExecutionFailureInput, ExecutionFailureSite};
        use novarocks_execution_contract::failure::FailureProjection;
        let config_a = recovery_config(
            execution(10_200),
            Arc::new(DelayedQualificationPort::default()),
            2,
        );
        let scope_a = config_a.work_owner.as_ref().unwrap().scope().clone();
        let config_b = recovery_config(
            execution(10_201),
            Arc::new(DelayedQualificationPort::default()),
            2,
        );
        let scope_b = config_b.work_owner.as_ref().unwrap().scope().clone();
        let (owner_a, initial_a, output_a) =
            spawn_logical_execution_actor(&Handle::current(), config_a)
                .unwrap()
                .into_parts();
        let (owner_b, initial_b, output_b) =
            spawn_logical_execution_actor(&Handle::current(), config_b)
                .unwrap()
                .into_parts();
        let (actor_a, join_a) = owner_a.into_test_parts();
        let (actor_b, join_b) = owner_b.into_test_parts();
        let running_a = actor_a.activate(initial_a.ready()).await.unwrap();
        let running_b = actor_b.activate(initial_b.ready()).await.unwrap();
        let ExecutionOutput::Rows(mut stream_b) = output_b.into_output() else {
            panic!("the receiving actor must own its original result stream");
        };
        stream_b.begin_schema().unwrap().complete();
        let failure_b = stream_b.failure_view().unwrap();
        let source = EstablishIssueError::AuthorizationBudgetExhausted;
        let foreign_error = QueryExecutionError::from_execution(ExecutionFailureInput::new(
            source.failure_fact(),
            source.safe_detail(),
            ExecutionFailureSite::Coordinator,
        ));

        // This is a real move-only permit owned by a different governed actor.
        let refusal = actor_b
            .fail_attempt_with_error(running_a, foreign_error.clone())
            .await;
        let snapshot_after_refusal = actor_b.snapshot().await.unwrap();
        let failure_after_refusal = failure_b.current();
        let accepted = actor_b.fail_attempt(running_b).await;
        let delivered = stream_b.next().await;
        let published = failure_b.current();

        // Join both original actor bodies and release their work before asserting the oracle.
        drop(output_a);
        drop(stream_b);
        drop(actor_a);
        drop(actor_b);
        let (joined_a, joined_b) = tokio::join!(
            tokio::time::timeout(Duration::from_secs(1), join_a),
            tokio::time::timeout(Duration::from_secs(1), join_b),
        );
        joined_a
            .expect("the foreign permit's original actor must actually join")
            .unwrap();
        joined_b
            .expect("the receiving actor must actually join")
            .unwrap();
        tokio::time::timeout(Duration::from_secs(1), scope_a.wait_released())
            .await
            .expect("the foreign actor original Work scope releases after join");
        tokio::time::timeout(Duration::from_secs(1), scope_b.wait_released())
            .await
            .expect("the receiving actor original Work scope releases after join");

        assert_eq!(refusal, Err(LogicalExecutionActorError::StaleAuthority));
        assert_eq!(snapshot_after_refusal.conclusion, None);
        assert_eq!(failure_after_refusal, None);
        assert_eq!(accepted, Ok(LogicalConclusion::Failed));
        let error = match delivered {
            Err(error) => error,
            Ok(_) => panic!("the receiving actor's own failure must terminate its stream"),
        };
        assert_eq!(error.kind(), QueryExecutionErrorKind::Failed);
        assert_eq!(
            error.message(),
            "logical execution failed before success EOF"
        );
        let input = error
            .execution_failure()
            .expect("the receiving actor owns its fallback reason");
        assert_eq!(
            input.fact(),
            crate::execution_error::QueryFailureReason::LogicalFailureUnspecified.failure_fact()
        );
        assert_eq!(input.site(), ExecutionFailureSite::Coordinator);
        assert_ne!(error, foreign_error);
        assert_eq!(published, Some(error));
    }

    #[tokio::test]
    async fn logical_failure_wakes_an_idle_result_stream_without_queue_capacity() {
        let runtime = Handle::current();
        let first = execution(110);
        let config = recovery_config(first, Arc::new(DelayedQualificationPort::default()), 2)
            .with_result_stream(result_schema(), NonZeroUsize::new(1).unwrap())
            .with_result_row_carrier(result_carrier());
        let (owner, initial, output) = spawn_logical_execution_actor(&runtime, config)
            .unwrap()
            .into_parts();
        let ExecutionOutput::Rows(mut stream) = output.into_output() else {
            panic!("result execution must expose its row stream");
        };
        stream.begin_schema().unwrap().complete();
        let actor = owner.actor().clone();
        let running = actor.activate(initial.ready()).await.unwrap();
        assert_eq!(
            actor.fail_attempt(running).await.unwrap(),
            LogicalConclusion::Failed
        );

        let result = tokio::time::timeout(Duration::from_secs(1), stream.next())
            .await
            .expect("logical failure must wake the result stream");
        let error = match result {
            Err(error) => error,
            Ok(_) => panic!("logical failure must terminate the result stream"),
        };
        assert_eq!(error.kind(), QueryExecutionErrorKind::Failed);
        drop(stream);
        drop(actor);
        drop(owner);
    }

    #[tokio::test]
    async fn cancellation_beats_a_queued_batch_before_protocol_delivery() {
        let runtime = Handle::current();
        let first = execution(113);
        let (parent, work_owner, stage) = test_governed_child_work(None);
        let config = LogicalExecutionActorConfig::read_only_pre_visibility_recovery(
            first,
            NonZeroUsize::new(4).unwrap(),
            Vec::new(),
            NonZeroUsize::new(2).unwrap(),
            NonZeroUsize::new(2).unwrap(),
            NonZeroU32::new(2).unwrap(),
            Arc::new(DelayedQualificationPort::default()),
            work_owner,
            stage,
            Duration::from_secs(30),
        )
        .unwrap()
        .with_result_stream(result_schema(), NonZeroUsize::new(1).unwrap())
        .with_result_row_carrier(result_carrier());
        let (owner, initial, output) = spawn_logical_execution_actor(&runtime, config)
            .unwrap()
            .into_parts();
        let ExecutionOutput::Rows(mut stream) = output.into_output() else {
            panic!("result execution must expose its row stream");
        };
        stream.begin_schema().unwrap().complete();
        let actor = owner.actor().clone();
        let running = Arc::new(actor.activate(initial.ready()).await.unwrap());
        let (control, window_owner, capacity, segment) = result_segment(first, 1);
        let delivery_running = Arc::clone(&running);
        let delivery_task = tokio::spawn(async move {
            delivery_running
                .deliver_root_segment(
                    ResultPacketSequence::new(0),
                    segment,
                    Some((result_profile(), ClientRowStreamCursor::default())),
                    2,
                    crate::api::RootRelayResidentWindow::default(),
                )
                .await
        });
        for _ in 0..16 {
            if actor.snapshot().await.unwrap().accepted_result_packets == 1 {
                break;
            }
            tokio::task::yield_now().await;
        }
        assert_eq!(actor.snapshot().await.unwrap().accepted_result_packets, 1);
        assert_eq!(
            actor.snapshot().await.unwrap().delivered_result_through,
            None
        );

        parent.cancel(CancellationReason::Requested);
        wait_for_conclusion(&actor, LogicalConclusion::Cancelled).await;
        let result = tokio::time::timeout(Duration::from_secs(1), stream.next())
            .await
            .expect("cancellation must preempt the queued batch");
        let error = match result {
            Err(error) => error,
            Ok(_) => panic!("a queued batch must not pass a prior cancellation"),
        };
        assert_eq!(error.kind(), QueryExecutionErrorKind::Cancelled);
        assert_eq!(
            delivery_task.await.unwrap().unwrap_err(),
            LogicalExecutionActorError::ExecutionConcluded(LogicalConclusion::Cancelled)
        );
        assert_eq!(capacity.snapshot().held_positions, [0; 4]);
        drop(stream);

        drop(running);
        window_owner.complete();
        drop(control);
        drop(actor);
        drop(owner);
        parent.complete();
    }

    #[tokio::test]
    async fn cancellation_view_interrupts_an_in_flight_protocol_delivery() {
        let runtime = Handle::current();
        let first = execution(114);
        let (parent, work_owner, stage) = test_governed_child_work(None);
        let config = LogicalExecutionActorConfig::read_only_pre_visibility_recovery(
            first,
            NonZeroUsize::new(4).unwrap(),
            Vec::new(),
            NonZeroUsize::new(2).unwrap(),
            NonZeroUsize::new(2).unwrap(),
            NonZeroU32::new(2).unwrap(),
            Arc::new(DelayedQualificationPort::default()),
            work_owner,
            stage,
            Duration::from_secs(30),
        )
        .unwrap()
        .with_result_stream(result_schema(), NonZeroUsize::new(1).unwrap())
        .with_result_row_carrier(result_carrier());
        let (owner, initial, output) = spawn_logical_execution_actor(&runtime, config)
            .unwrap()
            .into_parts();
        let ExecutionOutput::Rows(mut stream) = output.into_output() else {
            panic!("result execution must expose its row stream");
        };
        let mut failure = stream.failure_view().unwrap();
        stream.begin_schema().unwrap().complete();
        let actor = owner.actor().clone();
        let running = Arc::new(actor.activate(initial.ready()).await.unwrap());
        let (control, window_owner, capacity, segment) = result_segment(first, 1);
        let delivery_running = Arc::clone(&running);
        let delivery_task = tokio::spawn(async move {
            delivery_running
                .deliver_root_segment(
                    ResultPacketSequence::new(0),
                    segment,
                    Some((result_profile(), ClientRowStreamCursor::default())),
                    2,
                    crate::api::RootRelayResidentWindow::default(),
                )
                .await
        });
        let ResultDelivery::Segment(delivery) = stream.next().await.unwrap().unwrap() else {
            panic!("actor must enqueue one batch");
        };

        parent.cancel(CancellationReason::Requested);
        let error = tokio::time::timeout(Duration::from_secs(1), failure.wait())
            .await
            .expect("the in-flight protocol writer must observe cancellation");
        assert_eq!(error.kind(), QueryExecutionErrorKind::Cancelled);
        delivery.fail(actual_consumer_body_failure());
        assert_eq!(
            delivery_task.await.unwrap().unwrap_err(),
            LogicalExecutionActorError::ExecutionConcluded(LogicalConclusion::Cancelled)
        );
        wait_for_conclusion(&actor, LogicalConclusion::Cancelled).await;
        assert_eq!(capacity.snapshot().held_positions, [0; 4]);

        drop(running);
        window_owner.complete();
        drop(control);
        drop(stream);
        drop(actor);
        owner
            .into_residual_stand_down_supervisor()
            .join()
            .await
            .unwrap();
        assert_eq!(failure.current(), Some(error));
        parent.complete();
    }

    fn actual_consumer_body_failure() -> QueryExecutionError {
        use novarocks_execution_contract::failure::FailureProjection;
        let after = ClientRowStreamCursor::default()
            .validate_body(result_profile(), &[2, 0, 0, 0, b'a'])
            .unwrap()
            .after();
        let cause = after.validate_end().unwrap_err();
        assert_eq!(
            cause,
            novarocks_result_contract::ClientBodyError::EndInsideRow
        );
        QueryExecutionError::from_execution(crate::execution_error::ExecutionFailureInput::new(
            cause.failure_fact(),
            novarocks_execution_contract::SafeDetail::truncating(cause.safe_detail()),
            crate::execution_error::ExecutionFailureSite::Coordinator,
        ))
    }

    #[tokio::test]
    async fn schema_consumer_failure_keeps_its_original_domain_input() {
        let first = execution(12_201);
        let config = recovery_config(first, Arc::new(DelayedQualificationPort::default()), 2)
            .with_result_stream(result_schema(), NonZeroUsize::new(1).unwrap())
            .with_result_row_carrier(result_carrier());
        let scope = config.work_owner.as_ref().unwrap().scope().clone();
        let (owner, initial, output) = spawn_logical_execution_actor(&Handle::current(), config)
            .unwrap()
            .into_parts();
        let ExecutionOutput::Rows(mut stream) = output.into_output() else {
            panic!("result execution must expose its row stream");
        };
        let actor = owner.actor().clone();
        let running = actor.activate(initial.ready()).await.unwrap();
        let original = actual_consumer_body_failure();
        let failure = stream.failure_view().unwrap();
        stream.begin_schema().unwrap().fail(original.clone());
        let received = match tokio::time::timeout(Duration::from_secs(1), stream.next())
            .await
            .unwrap()
        {
            Err(error) => error,
            Ok(_) => panic!("schema failure must prevent every batch and success EOF"),
        };
        assert_eq!(received, original);
        assert!(stream.next().await.unwrap().is_none());
        wait_for_conclusion(&actor, LogicalConclusion::Failed).await;
        drop((running, stream, actor));
        owner
            .into_residual_stand_down_supervisor()
            .join()
            .await
            .unwrap();
        assert_eq!(failure.current(), Some(original));
        tokio::time::timeout(Duration::from_secs(1), scope.wait_released())
            .await
            .expect("the original schema failure owner releases its work");
    }

    #[tokio::test]
    async fn writer_failure_fails_delivery_and_releases_original_window() {
        let runtime = Handle::current();
        let first = execution(104);
        let config = recovery_config(first, Arc::new(DelayedQualificationPort::default()), 2)
            .with_result_stream(result_schema(), NonZeroUsize::new(1).unwrap())
            .with_result_row_carrier(result_carrier());
        let (owner, initial, output) = spawn_logical_execution_actor(&runtime, config)
            .unwrap()
            .into_parts();
        let ExecutionOutput::Rows(mut stream) = output.into_output() else {
            panic!("result execution must expose its row stream");
        };
        stream.begin_schema().unwrap().complete();
        let actor = owner.actor().clone();
        let running = Arc::new(actor.activate(initial.ready()).await.unwrap());
        let (control, window_owner, capacity, segment) = result_segment(first, 1);
        let delivery_running = Arc::clone(&running);
        let delivery_task = tokio::spawn(async move {
            delivery_running
                .deliver_root_segment(
                    ResultPacketSequence::new(0),
                    segment,
                    Some((result_profile(), ClientRowStreamCursor::default())),
                    2,
                    crate::api::RootRelayResidentWindow::default(),
                )
                .await
        });
        let ResultDelivery::Segment(delivery) = stream.next().await.unwrap().unwrap() else {
            panic!("actor must enqueue one batch");
        };
        let original = actual_consumer_body_failure();
        let failure = stream.failure_view().unwrap();
        delivery.fail(original.clone());
        assert_eq!(
            delivery_task.await.unwrap().unwrap_err(),
            LogicalExecutionActorError::ExecutionConcluded(LogicalConclusion::Failed)
        );
        wait_for_conclusion(&actor, LogicalConclusion::Failed).await;
        let received = match tokio::time::timeout(Duration::from_secs(1), stream.next())
            .await
            .unwrap()
        {
            Err(error) => error,
            Ok(_) => panic!("failed consumer receipt must prevent successful EOF"),
        };
        assert_eq!(received, original);
        assert!(stream.next().await.unwrap().is_none());
        assert_eq!(capacity.snapshot().held_positions, [0; 4]);
        drop(running);
        window_owner.complete();
        drop(control);
        drop(stream);
        drop(actor);
        owner
            .into_residual_stand_down_supervisor()
            .join()
            .await
            .unwrap();
        assert_eq!(failure.current(), Some(original));
    }

    #[tokio::test]
    async fn dropped_consumer_fails_closed_without_waiting_for_a_batch() {
        let runtime = Handle::current();
        let first = execution(105);
        let config = recovery_config(first, Arc::new(DelayedQualificationPort::default()), 2)
            .with_result_stream(result_schema(), NonZeroUsize::new(1).unwrap())
            .with_result_row_carrier(result_carrier());
        let (owner, initial, output) = spawn_logical_execution_actor(&runtime, config)
            .unwrap()
            .into_parts();
        let ExecutionOutput::Rows(stream) = output.into_output() else {
            panic!("result execution must expose its row stream");
        };
        drop(stream);
        let actor = owner.actor().clone();
        wait_for_conclusion(&actor, LogicalConclusion::Failed).await;
        assert!(matches!(
            actor.activate(initial.ready()).await,
            Err(LogicalExecutionActorError::ExecutionConcluded(
                LogicalConclusion::Failed
            ))
        ));
        drop(actor);
        drop(owner);
    }

    #[tokio::test]
    async fn dropped_consumer_after_schema_completion_still_fails_closed() {
        let runtime = Handle::current();
        let first = execution(108);
        let config = recovery_config(first, Arc::new(DelayedQualificationPort::default()), 2)
            .with_result_stream(result_schema(), NonZeroUsize::new(1).unwrap())
            .with_result_row_carrier(result_carrier());
        let (owner, initial, output) = spawn_logical_execution_actor(&runtime, config)
            .unwrap()
            .into_parts();
        let ExecutionOutput::Rows(mut stream) = output.into_output() else {
            panic!("result execution must expose its row stream");
        };
        stream.begin_schema().unwrap().complete();
        let actor = owner.actor().clone();
        actor.snapshot().await.unwrap();
        let _running = actor.activate(initial.ready()).await.unwrap();

        drop(stream);
        wait_for_conclusion(&actor, LogicalConclusion::Failed).await;
        drop(actor);
        drop(owner);
    }

    #[tokio::test]
    async fn actor_rejects_foreign_and_gapped_result_packets_fail_closed() {
        let runtime = Handle::current();
        let first = execution(106);
        let config = recovery_config(first, Arc::new(DelayedQualificationPort::default()), 2)
            .with_result_stream(result_schema(), NonZeroUsize::new(1).unwrap())
            .with_result_row_carrier(result_carrier());
        let (owner, initial, output) = spawn_logical_execution_actor(&runtime, config)
            .unwrap()
            .into_parts();
        let ExecutionOutput::Rows(mut stream) = output.into_output() else {
            panic!("result execution must expose its row stream");
        };
        stream.begin_schema().unwrap().complete();
        let actor = owner.actor().clone();
        let running = actor.activate(initial.ready()).await.unwrap();
        let identity = running.identity();

        let (foreign_control, foreign_owner, foreign_capacity, foreign_segment) =
            result_segment(first, 1);
        let foreign = AttemptActivationIdentity::from_parts(
            identity.actor().get(),
            replacement(first, 2),
            identity.generation(),
        );
        assert_eq!(
            request(&actor.sender, |reply| ActorCommand::DeliverRootSegment {
                activation: foreign,
                sequence: ResultPacketSequence::new(0),
                segment: foreign_segment,
                client_rows: Some((result_profile(), ClientRowStreamCursor::default())),
                rows: 2,
                resident_window: crate::api::RootRelayResidentWindow::default(),
                reply,
            })
            .await
            .unwrap_err(),
            LogicalExecutionActorError::WrongPhase
        );
        assert_eq!(foreign_capacity.snapshot().held_positions, [0; 4]);
        assert_eq!(actor.snapshot().await.unwrap().conclusion, None);
        foreign_owner.complete();
        drop(foreign_control);

        let (gap_control, gap_owner, gap_capacity, gap_segment) = result_segment(first, 2);
        assert_eq!(
            running
                .deliver_root_segment(
                    ResultPacketSequence::new(1),
                    gap_segment,
                    Some((result_profile(), ClientRowStreamCursor::default())),
                    2,
                    crate::api::RootRelayResidentWindow::default()
                )
                .await
                .unwrap_err(),
            LogicalExecutionActorError::ExecutionConcluded(LogicalConclusion::Failed)
        );
        wait_for_conclusion(&actor, LogicalConclusion::Failed).await;
        assert_eq!(gap_capacity.snapshot().held_positions, [0; 4]);
        gap_owner.complete();
        drop(gap_control);
        drop(running);
        drop(stream);
        drop(actor);
        drop(owner);
    }

    #[tokio::test]
    async fn exhausted_data_sequence_and_foreign_root_fail_closed() {
        let runtime = Handle::current();
        let first = execution(111);
        let config = recovery_config(first, Arc::new(DelayedQualificationPort::default()), 2)
            .with_result_stream(result_schema(), NonZeroUsize::new(1).unwrap())
            .with_result_row_carrier(result_carrier());
        let (owner, initial, output) = spawn_logical_execution_actor(&runtime, config)
            .unwrap()
            .into_parts();
        let ExecutionOutput::Rows(mut stream) = output.into_output() else {
            panic!("result execution must expose its row stream");
        };
        stream.begin_schema().unwrap().complete();
        let actor = owner.actor().clone();
        let running = actor.activate(initial.ready()).await.unwrap();
        let (control, window_owner, capacity, segment) = result_segment(first, 1);
        assert_eq!(
            running
                .deliver_root_segment(
                    ResultPacketSequence::new(u64::MAX),
                    segment,
                    Some((result_profile(), ClientRowStreamCursor::default())),
                    2,
                    crate::api::RootRelayResidentWindow::default()
                )
                .await
                .unwrap_err(),
            LogicalExecutionActorError::ExecutionConcluded(LogicalConclusion::Failed)
        );
        wait_for_conclusion(&actor, LogicalConclusion::Failed).await;
        let error = match stream.next().await {
            Err(error) => error,
            Ok(_) => panic!("failed result stream must not expose a delivery"),
        };
        assert_eq!(error.kind(), QueryExecutionErrorKind::Failed);
        assert_eq!(capacity.snapshot().held_positions, [0; 4]);
        drop(running);
        window_owner.complete();
        drop(control);
        drop(stream);
        drop(actor);
        drop(owner);

        let second = execution(112);
        let config = recovery_config(second, Arc::new(DelayedQualificationPort::default()), 2)
            .with_result_stream(result_schema(), NonZeroUsize::new(1).unwrap())
            .with_result_row_carrier(result_carrier());
        let (owner, initial, output) = spawn_logical_execution_actor(&runtime, config)
            .unwrap()
            .into_parts();
        let ExecutionOutput::Rows(mut stream) = output.into_output() else {
            panic!("result execution must expose its row stream");
        };
        stream.begin_schema().unwrap().complete();
        let actor = owner.actor().clone();
        let running = actor.activate(initial.ready()).await.unwrap();
        let root = root_task(second, 2);
        let observer = running.bind_root_result(root).await.unwrap();
        let foreign = root_task(second, 3);
        assert_eq!(
            observer
                .observe_local_root_end(foreign, result_end(0))
                .await
                .unwrap_err(),
            LogicalExecutionActorError::ExecutionConcluded(LogicalConclusion::Failed)
        );
        wait_for_conclusion(&actor, LogicalConclusion::Failed).await;
        drop(stream);
        drop(actor);
        drop(owner);
    }

    #[tokio::test]
    async fn schema_survives_replacement_and_empty_result_succeeds_after_eof_receipt() {
        let runtime = Handle::current();
        let first = execution(107);
        let second = replacement(first, 2);
        let port = Arc::new(DelayedQualificationPort::default());
        let submissions = Arc::clone(&port.submissions);
        let config = recovery_config(first, port, 2)
            .with_result_stream(result_schema(), NonZeroUsize::new(1).unwrap())
            .with_result_row_carrier(result_carrier());
        let (owner, initial, output) = spawn_logical_execution_actor(&runtime, config)
            .unwrap()
            .into_parts();
        let ExecutionOutput::Rows(mut stream) = output.into_output() else {
            panic!("result execution must expose its row stream");
        };
        stream.begin_schema().unwrap().complete();
        let actor = owner.actor().clone();
        actor.snapshot().await.unwrap();

        let running = actor.activate(initial.ready()).await.unwrap();
        let old_root = root_task(first, 5);
        let old_observer = running.bind_root_result(old_root).await.unwrap();
        let qualification = actor
            .begin_replacement(
                running,
                AttemptFailureClass::RecoverableInfrastructure,
                second,
                Vec::new(),
            )
            .await
            .unwrap();
        drop(old_observer);
        assert_eq!(actor.snapshot().await.unwrap().conclusion, None);
        let mut activation = Box::pin(actor.activate_replacement(qualification));
        assert!(matches!(
            std::future::poll_fn(|context| Poll::Ready(activation.as_mut().poll(context))).await,
            Poll::Pending
        ));
        qualify(submissions.lock().unwrap().pop().unwrap());
        let successor = actor
            .activate(activation.await.unwrap().ready())
            .await
            .unwrap();
        assert_eq!(actor.snapshot().await.unwrap().conclusion, None);

        let root = root_task(second, 4);
        let observer = successor.bind_root_result(root).await.unwrap();
        observer
            .observe_local_root_end(root, result_end(0))
            .await
            .unwrap();
        let finish_task =
            tokio::spawn(async move { successor.finish_result_stream(Vec::new()).await });
        assert!(!finish_task.is_finished());
        observer
            .observe_status(root_status(root, 1, TaskState::Finished))
            .await
            .unwrap();
        let ResultDelivery::End(end) = stream.next().await.unwrap().unwrap() else {
            panic!("empty result must still deliver EOF");
        };
        assert_eq!(end.execution_id(), second);
        assert_eq!(end.sequence(), ResultPacketSequence::new(0));
        assert_eq!(
            finish_task.await.unwrap().unwrap(),
            LogicalConclusion::Succeeded
        );
        assert_eq!(
            actor.snapshot().await.unwrap().conclusion,
            Some(LogicalConclusion::Succeeded)
        );
        end.complete();
        assert!(stream.begin_schema().is_none());
        wait_for_conclusion(&actor, LogicalConclusion::Succeeded).await;
        drop(stream);
        drop(actor);
        drop(owner);
    }

    #[tokio::test]
    async fn result_constructor_failure_keeps_its_domain_fact_through_the_actor() {
        use novarocks_execution_contract::failure::FailureProjection;
        let first = execution(12_200);
        let config = recovery_config(first, Arc::new(DelayedQualificationPort::default()), 2)
            .with_result_stream(result_schema(), NonZeroUsize::new(1).unwrap())
            .with_result_row_carrier(result_carrier());
        let scope = config.work_owner.as_ref().unwrap().scope().clone();
        let (owner, initial, output) = spawn_logical_execution_actor(&Handle::current(), config)
            .unwrap()
            .into_parts();
        let (actor, join) = owner.into_test_parts();
        let ExecutionOutput::Rows(mut stream) = output.into_output() else {
            panic!("result execution must expose its row stream");
        };
        stream.begin_schema().unwrap().complete();
        let running = actor.activate(initial.ready()).await.unwrap();
        let observer = running.bind_root_result(root_task(first, 1)).await.unwrap();
        let (control, window_owner, capacity, segment) = result_segment(first, 1);
        // A real unfinished row leaves two payload bytes. The supplied segment
        // then presents an out-of-profile prefix, so its original body domain
        // rejects it before any delivery can own the window.
        let before = ClientRowStreamCursor::default()
            .validate_body(result_profile(), &[3, 0, 0, 0, b'x'])
            .unwrap()
            .after();
        let delivery_result = running
            .deliver_root_segment(
                ResultPacketSequence::new(0),
                segment,
                Some((result_profile(), before)),
                2,
                crate::api::RootRelayResidentWindow::default(),
            )
            .await;
        let observed = match stream.next().await {
            Err(error) => Some(error),
            Ok(delivery) => {
                drop(delivery);
                None
            }
        };
        drop(observer);
        drop(running);
        drop(stream);
        drop(actor);
        tokio::time::timeout(Duration::from_secs(1), join)
            .await
            .expect("the original actor actually exits")
            .unwrap();
        tokio::time::timeout(Duration::from_secs(1), scope.wait_released())
            .await
            .expect("the original actor work releases after exit");
        let held = capacity.snapshot().held_positions;
        window_owner.complete();
        drop(control);
        assert!(delivery_result.is_err());
        assert_eq!(held, [0; 4]);
        let error = observed.expect("the rejected body must terminate its original stream");
        let input = error
            .execution_failure()
            .expect("body facts must remain typed");
        assert_eq!(
            input.fact(),
            novarocks_result_contract::ClientBodyError::InvalidRowLength.failure_fact()
        );
        assert_eq!(
            input.site(),
            crate::execution_error::ExecutionFailureSite::Coordinator
        );
    }

    #[tokio::test]
    async fn recovered_attempt_failure_does_not_escape_its_activation() {
        use crate::execution_error::{
            ExecutionFailureInput, ExecutionFailureSite, QueryFailureReason,
        };
        use novarocks_execution_contract::{SafeDetail, failure::FailureProjection};
        for outcome in 0..3 {
            let first = execution(12_100 + outcome);
            let second = replacement(first, 2);
            let port = Arc::new(DelayedQualificationPort::default());
            let submissions = Arc::clone(&port.submissions);
            let config = recovery_config(first, port, 2)
                .with_result_stream(result_schema(), NonZeroUsize::new(1).unwrap())
                .with_result_row_carrier(result_carrier());
            let scope = config.work_owner.as_ref().unwrap().scope().clone();
            let (owner, initial, output) =
                spawn_logical_execution_actor(&Handle::current(), config)
                    .unwrap()
                    .into_parts();
            let (actor, join) = owner.into_test_parts();
            let ExecutionOutput::Rows(mut stream) = output.into_output() else {
                panic!("result execution must expose its row stream");
            };
            let failure_view = stream.failure_view().unwrap();
            stream.begin_schema().unwrap().complete();
            let running = actor.activate(initial.ready()).await.unwrap();
            let original = QueryExecutionError::from_execution(ExecutionFailureInput::new(
                QueryFailureReason::BackendLost.failure_fact(),
                SafeDetail::truncating("the original attempt lost its exact backend"),
                ExecutionFailureSite::Backend {
                    process: BackendProcessId::new_v7(),
                },
            ));
            running
                .freeze_result_attempt_failure(original.clone())
                .await
                .unwrap();
            let qualification = actor
                .begin_replacement_with_error(
                    running,
                    AttemptFailureClass::RecoverableInfrastructure,
                    original.clone(),
                    None,
                    second,
                    Vec::new(),
                )
                .await
                .unwrap();
            let submission = submissions.lock().unwrap().pop().unwrap();
            let mut observed_failure = None;
            if outcome == 0 {
                submission.rejected();
                wait_for_conclusion(&actor, LogicalConclusion::Failed).await;
                observed_failure = match stream.next().await {
                    Err(error) => Some(error),
                    Ok(delivery) => {
                        drop(delivery);
                        None
                    }
                };
                drop(qualification);
            } else {
                let mut activation = Box::pin(actor.activate_replacement(qualification));
                assert!(matches!(
                    std::future::poll_fn(|cx| Poll::Ready(activation.as_mut().poll(cx))).await,
                    Poll::Pending
                ));
                qualify(submission);
                let successor = actor
                    .activate(activation.await.unwrap().ready())
                    .await
                    .unwrap();
                if outcome == 1 {
                    actor.fail_attempt(successor).await.unwrap();
                    observed_failure = match stream.next().await {
                        Err(error) => Some(error),
                        Ok(delivery) => {
                            drop(delivery);
                            None
                        }
                    };
                } else {
                    let root = root_task(second, 4);
                    let observer = successor.bind_root_result(root).await.unwrap();
                    observer
                        .observe_local_root_end(root, result_end(0))
                        .await
                        .unwrap();
                    observer
                        .observe_status(root_status(root, 1, TaskState::Finished))
                        .await
                        .unwrap();
                    let finish =
                        tokio::spawn(
                            async move { successor.finish_result_stream(Vec::new()).await },
                        );
                    let ResultDelivery::End(end) = stream.next().await.unwrap().unwrap() else {
                        panic!("the successor must deliver its own success End");
                    };
                    assert_eq!(end.execution_id(), second);
                    assert_eq!(finish.await.unwrap().unwrap(), LogicalConclusion::Succeeded);
                    end.complete();
                    drop(observer);
                }
            }
            drop(stream);
            drop(actor);
            tokio::time::timeout(Duration::from_secs(1), join)
                .await
                .expect("the original actor actually exits")
                .unwrap();
            tokio::time::timeout(Duration::from_secs(1), scope.wait_released())
                .await
                .expect("the original work scope releases after actor exit");
            if outcome == 0 {
                assert_eq!(observed_failure, Some(original));
            } else if outcome == 1 {
                let current = observed_failure.expect("the successor must publish its own failure");
                assert_eq!(current.kind(), QueryExecutionErrorKind::Failed);
                assert_eq!(
                    current.message(),
                    "logical execution failed before success EOF"
                );
                assert_ne!(current, original);
                assert_ne!(current.execution_failure(), original.execution_failure());
            } else {
                assert_eq!(
                    failure_view.current(),
                    None,
                    "a successful successor must never publish its predecessor's failure"
                );
            }
        }
    }

    #[tokio::test]
    async fn eof_waits_for_pending_batch_protocol_completion() {
        let runtime = Handle::current();
        let first = execution(113);
        let config = recovery_config(first, Arc::new(DelayedQualificationPort::default()), 2)
            .with_result_stream(result_schema(), NonZeroUsize::new(1).unwrap())
            .with_result_row_carrier(result_carrier());
        let (owner, initial, output) = spawn_logical_execution_actor(&runtime, config)
            .unwrap()
            .into_parts();
        let ExecutionOutput::Rows(mut stream) = output.into_output() else {
            panic!("result execution must expose its row stream");
        };
        stream.begin_schema().unwrap().complete();
        let actor = owner.actor().clone();
        let running = actor.activate(initial.ready()).await.unwrap();
        let root = root_task(first, 6);
        let observer = running.bind_root_result(root).await.unwrap();
        let (control, window_owner, capacity, segment) = result_segment(first, 1);
        let (reply, response) = oneshot::channel();
        actor
            .sender
            .send(ActorCommand::DeliverRootSegment {
                activation: running.identity(),
                sequence: ResultPacketSequence::new(0),
                segment,
                client_rows: Some((result_profile(), ClientRowStreamCursor::default())),
                rows: 2,
                resident_window: crate::api::RootRelayResidentWindow::default(),
                reply,
            })
            .await
            .unwrap();
        let ResultDelivery::Segment(delivery) = stream.next().await.unwrap().unwrap() else {
            panic!("actor must enqueue the pending batch");
        };
        observer
            .observe_status(root_status(root, 1, TaskState::Finished))
            .await
            .unwrap();
        let finish_task =
            tokio::spawn(async move { running.finish_result_stream(Vec::new()).await });
        actor.snapshot().await.unwrap();
        assert!(!finish_task.is_finished());

        assert_eq!(capacity.snapshot().held_positions, [1, 0, 0, 0]);
        delivery.complete();
        assert_eq!(capacity.snapshot().held_positions, [0; 4]);
        response.await.unwrap().unwrap();
        observer
            .observe_local_root_end(root, result_end(1))
            .await
            .unwrap();
        let ResultDelivery::End(end) = stream.next().await.unwrap().unwrap() else {
            panic!("EOF must follow the completed batch");
        };
        end.complete();
        assert_eq!(
            finish_task.await.unwrap().unwrap(),
            LogicalConclusion::Succeeded
        );
        window_owner.complete();
        drop(control);
        drop(stream);
        drop(actor);
        drop(owner);
    }

    #[tokio::test]
    async fn root_terminal_failure_returns_attempt_decision_to_the_permit_owner() {
        let runtime = Handle::current();
        let first = execution(114);
        let config = recovery_config(first, Arc::new(DelayedQualificationPort::default()), 2)
            .with_result_stream(result_schema(), NonZeroUsize::new(1).unwrap())
            .with_result_row_carrier(result_carrier());
        let (owner, initial, output) = spawn_logical_execution_actor(&runtime, config)
            .unwrap()
            .into_parts();
        let ExecutionOutput::Rows(mut stream) = output.into_output() else {
            panic!("result execution must expose its row stream");
        };
        stream.begin_schema().unwrap().complete();
        let actor = owner.actor().clone();
        let running = actor.activate(initial.ready()).await.unwrap();
        let root = root_task(first, 7);
        let observer = running.bind_root_result(root).await.unwrap();
        assert_eq!(
            observer
                .observe_status(root_status(root, 1, TaskState::Aborted))
                .await
                .unwrap_err(),
            LogicalExecutionActorError::RootAttemptTerminal
        );
        let snapshot = actor.snapshot().await.unwrap();
        assert_eq!(snapshot.conclusion, None);
        assert!(matches!(
            snapshot.phase,
            ExecutionPhase::Running { execution, .. } if execution == first
        ));
        let (control, window_owner, capacity, segment) = result_segment(first, 1);
        assert_eq!(
            running
                .deliver_root_segment(
                    ResultPacketSequence::new(0),
                    segment,
                    Some((result_profile(), ClientRowStreamCursor::default())),
                    2,
                    crate::api::RootRelayResidentWindow::default()
                )
                .await
                .unwrap_err(),
            LogicalExecutionActorError::RootAttemptTerminal
        );
        assert!(!actor.snapshot().await.unwrap().output_visible);
        assert_eq!(capacity.snapshot().held_positions, [0; 4]);
        let terminal_error = QueryExecutionError::new(
            QueryExecutionErrorKind::Failed,
            "root task failed after attempt classification",
        );
        assert_eq!(
            actor
                .fail_attempt_with_error(running, terminal_error.clone())
                .await
                .unwrap(),
            LogicalConclusion::Failed
        );
        let error = match stream.next().await {
            Err(error) => error,
            Ok(_) => panic!("a final attempt failure must terminate the result stream"),
        };
        assert_eq!(error, terminal_error);
        window_owner.complete();
        drop(control);
        drop(stream);
        drop(actor);
        drop(owner);
    }

    #[tokio::test]
    async fn coalesced_root_status_may_skip_nonterminal_versions() {
        let runtime = Handle::current();
        let first = execution(1141);
        let config = recovery_config(first, Arc::new(DelayedQualificationPort::default()), 2)
            .with_result_stream(result_schema(), NonZeroUsize::new(1).unwrap())
            .with_result_row_carrier(result_carrier());
        let (owner, initial, output) = spawn_logical_execution_actor(&runtime, config)
            .unwrap()
            .into_parts();
        let ExecutionOutput::Rows(mut stream) = output.into_output() else {
            panic!("result execution must expose its row stream");
        };
        stream.begin_schema().unwrap().complete();
        let actor = owner.actor().clone();
        let running = actor.activate(initial.ready()).await.unwrap();
        let root = root_task(first, 71);
        let observer = running.bind_root_result(root).await.unwrap();
        observer
            .observe_status(root_status(root, 1, TaskState::Planned))
            .await
            .unwrap();
        observer
            .observe_status(root_status(root, 4, TaskState::Finished))
            .await
            .unwrap();
        assert_eq!(actor.snapshot().await.unwrap().conclusion, None);
        observer
            .observe_local_root_end(root, result_end(0))
            .await
            .unwrap();
        let finish = tokio::spawn(async move { running.finish_result_stream(Vec::new()).await });
        let ResultDelivery::End(end) = stream.next().await.unwrap().unwrap() else {
            panic!("coalesced stable success must still deliver EOF");
        };
        end.complete();
        assert_eq!(finish.await.unwrap().unwrap(), LogicalConclusion::Succeeded);
        drop(stream);
        drop(actor);
        drop(owner);
    }

    #[tokio::test]
    async fn wrong_final_ack_sequence_fails_at_observation() {
        let runtime = Handle::current();
        let first = execution(115);
        let config = recovery_config(first, Arc::new(DelayedQualificationPort::default()), 2)
            .with_result_stream(result_schema(), NonZeroUsize::new(1).unwrap())
            .with_result_row_carrier(result_carrier());
        let (owner, initial, output) = spawn_logical_execution_actor(&runtime, config)
            .unwrap()
            .into_parts();
        let ExecutionOutput::Rows(mut stream) = output.into_output() else {
            panic!("result execution must expose its row stream");
        };
        stream.begin_schema().unwrap().complete();
        let actor = owner.actor().clone();
        let running = actor.activate(initial.ready()).await.unwrap();
        let root = root_task(first, 8);
        let observer = running.bind_root_result(root).await.unwrap();
        assert_eq!(
            observer
                .observe_local_root_end(root, result_end(1))
                .await
                .unwrap_err(),
            LogicalExecutionActorError::ExecutionConcluded(LogicalConclusion::Failed)
        );
        wait_for_conclusion(&actor, LogicalConclusion::Failed).await;
        drop(running);
        drop(stream);
        drop(actor);
        drop(owner);
    }

    #[tokio::test]
    async fn data_after_final_worker_ack_is_rejected_before_visibility() {
        let runtime = Handle::current();
        let first = execution(122);
        let config = recovery_config(first, Arc::new(DelayedQualificationPort::default()), 2)
            .with_result_stream(result_schema(), NonZeroUsize::new(1).unwrap())
            .with_result_row_carrier(result_carrier());
        let (owner, initial, output) = spawn_logical_execution_actor(&runtime, config)
            .unwrap()
            .into_parts();
        let ExecutionOutput::Rows(mut stream) = output.into_output() else {
            panic!("result execution must expose its row stream");
        };
        stream.begin_schema().unwrap().complete();
        let actor = owner.actor().clone();
        let running = actor.activate(initial.ready()).await.unwrap();
        let root = root_task(first, 15);
        let observer = running.bind_root_result(root).await.unwrap();
        observer
            .observe_local_root_end(root, result_end(0))
            .await
            .unwrap();
        let (control, window_owner, capacity, segment) = result_segment(first, 1);

        assert_eq!(
            running
                .deliver_root_segment(
                    ResultPacketSequence::new(0),
                    segment,
                    Some((result_profile(), ClientRowStreamCursor::default())),
                    2,
                    crate::api::RootRelayResidentWindow::default()
                )
                .await
                .unwrap_err(),
            LogicalExecutionActorError::ExecutionConcluded(LogicalConclusion::Failed)
        );
        wait_for_conclusion(&actor, LogicalConclusion::Failed).await;
        let error = match stream.next().await {
            Err(error) => error,
            Ok(_) => panic!("failed result stream must not expose a delivery"),
        };
        assert_eq!(error.kind(), QueryExecutionErrorKind::Failed);
        assert_eq!(capacity.snapshot().held_positions, [0; 4]);
        window_owner.complete();
        drop(control);
        drop(running);
        drop(observer);
        drop(stream);
        drop(actor);
        drop(owner);
    }

    #[tokio::test]
    async fn attempt_failure_wins_after_root_facts_but_before_finish_handoff() {
        let runtime = Handle::current();
        let first = execution(116);
        let config = recovery_config(first, Arc::new(DelayedQualificationPort::default()), 2)
            .with_result_stream(result_schema(), NonZeroUsize::new(1).unwrap())
            .with_result_row_carrier(result_carrier());
        let (owner, initial, output) = spawn_logical_execution_actor(&runtime, config)
            .unwrap()
            .into_parts();
        let ExecutionOutput::Rows(mut stream) = output.into_output() else {
            panic!("result execution must expose its row stream");
        };
        stream.begin_schema().unwrap().complete();
        let actor = owner.actor().clone();
        let running = actor.activate(initial.ready()).await.unwrap();
        let root = root_task(first, 9);
        let observer = running.bind_root_result(root).await.unwrap();
        observer
            .observe_status(root_status(root, 1, TaskState::Finished))
            .await
            .unwrap();
        observer
            .observe_local_root_end(root, result_end(0))
            .await
            .unwrap();
        assert_eq!(
            actor.fail_attempt(running).await.unwrap(),
            LogicalConclusion::Failed
        );
        wait_for_conclusion(&actor, LogicalConclusion::Failed).await;
        drop(stream);
        drop(actor);
        drop(owner);
    }

    #[tokio::test]
    async fn committed_end_authorization_beats_a_late_root_failure_in_the_same_tick() {
        let runtime = Handle::current();
        let first = execution(117);
        let config = recovery_config(first, Arc::new(DelayedQualificationPort::default()), 2)
            .with_result_stream(result_schema(), NonZeroUsize::new(1).unwrap())
            .with_result_row_carrier(result_carrier());
        let (owner, initial, output) = spawn_logical_execution_actor(&runtime, config)
            .unwrap()
            .into_parts();
        let ExecutionOutput::Rows(mut stream) = output.into_output() else {
            panic!("result execution must expose its row stream");
        };
        stream.begin_schema().unwrap().complete();
        let actor = owner.actor().clone();
        let running = actor.activate(initial.ready()).await.unwrap();
        let root = root_task(first, 10);
        let observer = running.bind_root_result(root).await.unwrap();
        observer
            .observe_status(root_status(root, 1, TaskState::Finished))
            .await
            .unwrap();
        observer
            .observe_local_root_end(root, result_end(0))
            .await
            .unwrap();
        let finish_task =
            tokio::spawn(async move { running.finish_result_stream(Vec::new()).await });
        let ResultDelivery::End(end) = stream.next().await.unwrap().unwrap() else {
            panic!("actor must enqueue success EOF");
        };

        let (reply, response) = oneshot::channel();
        observer
            .terminal_mailbox
            .try_send(RootTerminalObservation {
                activation: observer.activation(),
                root,
                status: root_status(root, 2, TaskState::Aborted),
                terminal_failure: RootTerminalFailure::Unspecified,
                reply,
            })
            .unwrap();
        end.complete();

        assert_eq!(
            response.await.unwrap().unwrap_err(),
            LogicalExecutionActorError::ExecutionConcluded(LogicalConclusion::Succeeded)
        );
        assert_eq!(
            finish_task.await.unwrap().unwrap(),
            LogicalConclusion::Succeeded
        );
        wait_for_conclusion(&actor, LogicalConclusion::Succeeded).await;
        drop(stream);
        drop(actor);
        drop(owner);
    }

    #[tokio::test]
    async fn eof_writer_failure_cannot_revoke_committed_logical_success() {
        let runtime = Handle::current();
        let first = execution(118);
        let config = recovery_config(first, Arc::new(DelayedQualificationPort::default()), 2)
            .with_result_stream(result_schema(), NonZeroUsize::new(1).unwrap())
            .with_result_row_carrier(result_carrier());
        let (owner, initial, output) = spawn_logical_execution_actor(&runtime, config)
            .unwrap()
            .into_parts();
        let ExecutionOutput::Rows(mut stream) = output.into_output() else {
            panic!("result execution must expose its row stream");
        };
        let failure = stream.failure_view().unwrap();
        stream.begin_schema().unwrap().complete();
        let actor = owner.actor().clone();
        let running = actor.activate(initial.ready()).await.unwrap();
        let root = root_task(first, 11);
        let observer = running.bind_root_result(root).await.unwrap();
        observer
            .observe_status(root_status(root, 1, TaskState::Finished))
            .await
            .unwrap();
        observer
            .observe_local_root_end(root, result_end(0))
            .await
            .unwrap();
        let finish_task =
            tokio::spawn(async move { running.finish_result_stream(Vec::new()).await });
        let ResultDelivery::End(end) = stream.next().await.unwrap().unwrap() else {
            panic!("actor must enqueue success EOF");
        };
        end.fail(actual_consumer_body_failure());

        assert_eq!(
            finish_task.await.unwrap().unwrap(),
            LogicalConclusion::Succeeded
        );
        wait_for_conclusion(&actor, LogicalConclusion::Succeeded).await;
        drop(observer);
        drop(stream);
        drop(actor);
        owner
            .into_residual_stand_down_supervisor()
            .join()
            .await
            .unwrap();
        assert!(failure.current().is_none());
    }

    #[tokio::test]
    async fn cancellation_after_end_authorization_cannot_revoke_logical_success() {
        let runtime = Handle::current();
        let first = execution(119);
        let (parent, work_owner, stage) = test_governed_child_work(None);
        let config = LogicalExecutionActorConfig::read_only_pre_visibility_recovery(
            first,
            NonZeroUsize::new(4).unwrap(),
            Vec::new(),
            NonZeroUsize::new(2).unwrap(),
            NonZeroUsize::new(2).unwrap(),
            NonZeroU32::new(2).unwrap(),
            Arc::new(DelayedQualificationPort::default()),
            work_owner,
            stage,
            Duration::from_secs(30),
        )
        .unwrap()
        .with_result_stream(result_schema(), NonZeroUsize::new(1).unwrap())
        .with_result_row_carrier(result_carrier());
        let (owner, initial, output) = spawn_logical_execution_actor(&runtime, config)
            .unwrap()
            .into_parts();
        let ExecutionOutput::Rows(mut stream) = output.into_output() else {
            panic!("result execution must expose its row stream");
        };
        stream.begin_schema().unwrap().complete();
        let actor = owner.actor().clone();
        let running = actor.activate(initial.ready()).await.unwrap();
        let root = root_task(first, 12);
        let observer = running.bind_root_result(root).await.unwrap();
        observer
            .observe_status(root_status(root, 1, TaskState::Finished))
            .await
            .unwrap();
        observer
            .observe_local_root_end(root, result_end(0))
            .await
            .unwrap();
        let finish_task =
            tokio::spawn(async move { running.finish_result_stream(Vec::new()).await });
        let ResultDelivery::End(end) = stream.next().await.unwrap().unwrap() else {
            panic!("actor must enqueue committed success EOF");
        };

        parent.cancel(CancellationReason::Requested);
        assert_eq!(
            finish_task.await.unwrap().unwrap(),
            LogicalConclusion::Succeeded
        );
        wait_for_conclusion(&actor, LogicalConclusion::Succeeded).await;
        drop(end);
        drop(observer);
        drop(stream);
        drop(actor);
        drop(owner);
        drop(parent);
    }

    #[tokio::test]
    async fn cancellation_before_end_authorization_prevents_success_visibility() {
        let runtime = Handle::current();
        let first = execution(1191);
        let (parent, work_owner, stage) = test_governed_child_work(None);
        let config = LogicalExecutionActorConfig::read_only_pre_visibility_recovery(
            first,
            NonZeroUsize::new(4).unwrap(),
            Vec::new(),
            NonZeroUsize::new(2).unwrap(),
            NonZeroUsize::new(2).unwrap(),
            NonZeroU32::new(2).unwrap(),
            Arc::new(DelayedQualificationPort::default()),
            work_owner,
            stage,
            Duration::from_secs(30),
        )
        .unwrap()
        .with_result_stream(result_schema(), NonZeroUsize::new(1).unwrap())
        .with_result_row_carrier(result_carrier());
        let (owner, initial, output) = spawn_logical_execution_actor(&runtime, config)
            .unwrap()
            .into_parts();
        let ExecutionOutput::Rows(mut stream) = output.into_output() else {
            panic!("result execution must expose its row stream");
        };
        stream.begin_schema().unwrap().complete();
        let actor = owner.actor().clone();
        let running = actor.activate(initial.ready()).await.unwrap();
        let root = root_task(first, 121);
        let observer = running.bind_root_result(root).await.unwrap();
        observer
            .observe_status(root_status(root, 1, TaskState::Finished))
            .await
            .unwrap();
        observer
            .observe_local_root_end(root, result_end(0))
            .await
            .unwrap();
        let finish_task =
            tokio::spawn(async move { running.finish_result_stream(Vec::new()).await });

        parent.cancel(CancellationReason::Requested);
        let error = match stream.next().await {
            Err(error) => error,
            Ok(_) => panic!("prior cancellation must prevent success EOF visibility"),
        };
        assert_eq!(error.kind(), QueryExecutionErrorKind::Cancelled);
        assert!(matches!(
            finish_task.await.unwrap().unwrap_err(),
            RunningAttemptHandoffError::ExecutionConcluded(LogicalConclusion::Cancelled)
        ));
        wait_for_conclusion(&actor, LogicalConclusion::Cancelled).await;
        drop(observer);
        drop(stream);
        drop(actor);
        drop(owner);
        parent.complete();
    }

    #[tokio::test]
    async fn losing_the_root_observer_fails_a_pending_finish() {
        let runtime = Handle::current();
        let first = execution(120);
        let config = recovery_config(first, Arc::new(DelayedQualificationPort::default()), 2)
            .with_result_stream(result_schema(), NonZeroUsize::new(1).unwrap())
            .with_result_row_carrier(result_carrier());
        let (owner, initial, output) = spawn_logical_execution_actor(&runtime, config)
            .unwrap()
            .into_parts();
        let ExecutionOutput::Rows(mut stream) = output.into_output() else {
            panic!("result execution must expose its row stream");
        };
        stream.begin_schema().unwrap().complete();
        let actor = owner.actor().clone();
        let running = actor.activate(initial.ready()).await.unwrap();
        let root = root_task(first, 13);
        let observer = running.bind_root_result(root).await.unwrap();
        observer
            .observe_status(root_status(root, 1, TaskState::Finished))
            .await
            .unwrap();
        let finish_task =
            tokio::spawn(async move { running.finish_result_stream(Vec::new()).await });
        drop(observer);

        assert!(matches!(
            finish_task.await.unwrap().unwrap_err(),
            RunningAttemptHandoffError::ExecutionConcluded(LogicalConclusion::Failed)
        ));
        wait_for_conclusion(&actor, LogicalConclusion::Failed).await;
        drop(stream);
        drop(actor);
        drop(owner);
    }

    #[tokio::test]
    async fn ordinary_mailbox_work_does_not_preempt_a_ready_eof_receipt() {
        let runtime = Handle::current();
        let first = execution(121);
        let config = recovery_config(first, Arc::new(DelayedQualificationPort::default()), 2)
            .with_result_stream(result_schema(), NonZeroUsize::new(1).unwrap())
            .with_result_row_carrier(result_carrier());
        let (owner, initial, output) = spawn_logical_execution_actor(&runtime, config)
            .unwrap()
            .into_parts();
        let ExecutionOutput::Rows(mut stream) = output.into_output() else {
            panic!("result execution must expose its row stream");
        };
        stream.begin_schema().unwrap().complete();
        let actor = owner.actor().clone();
        let running = actor.activate(initial.ready()).await.unwrap();
        let root = root_task(first, 14);
        let observer = running.bind_root_result(root).await.unwrap();
        observer
            .observe_status(root_status(root, 1, TaskState::Finished))
            .await
            .unwrap();
        observer
            .observe_local_root_end(root, result_end(0))
            .await
            .unwrap();
        let finish_task =
            tokio::spawn(async move { running.finish_result_stream(Vec::new()).await });
        let ResultDelivery::End(end) = stream.next().await.unwrap().unwrap() else {
            panic!("actor must enqueue success EOF");
        };
        let (reply, response) = oneshot::channel();
        actor
            .sender
            .try_send(ActorCommand::Snapshot { reply })
            .unwrap();
        end.complete();

        assert_eq!(
            finish_task.await.unwrap().unwrap(),
            LogicalConclusion::Succeeded
        );
        assert_eq!(
            response.await.unwrap().unwrap().conclusion,
            Some(LogicalConclusion::Succeeded)
        );
        drop(observer);
        drop(stream);
        drop(actor);
        drop(owner);
    }

    #[test]
    fn one_backpressured_context_does_not_block_other_abort_issues() {
        let execution = execution(10);
        let frontend = FrontendProcessId::new_v7();
        let blocked = QueryContextRef::new(execution, frontend, BackendProcessId::new_v7());
        let reachable = QueryContextRef::new(execution, frontend, BackendProcessId::new_v7());
        let contexts = BTreeSet::from([blocked, reachable]);
        let mut stand_down =
            ContextStandDownLedger::new(contexts, NonZeroUsize::new(2).unwrap()).unwrap();
        stand_down
            .begin(
                AttemptActivationIdentity::from_parts(17, execution, 1),
                ContextStandDownCause::LogicalExecutionCancelled,
                [
                    (blocked, super::super::EstablishStandDownFact::AbortRequired),
                    (
                        reachable,
                        super::super::EstablishStandDownFact::AbortRequired,
                    ),
                ],
            )
            .unwrap();
        let (capacity, _) = watch::channel(0);
        let accepted = Arc::new(Mutex::new(Vec::new()));
        let port = SelectiveAbortPort {
            blocked,
            accepted: Arc::clone(&accepted),
            capacity,
        };
        let mut pending = BTreeMap::new();
        let mut backpressured = false;

        drive_abort_effect(
            &mut stand_down,
            Some(&port),
            &mut pending,
            &mut backpressured,
            MonotonicInstant::ORIGIN,
        )
        .unwrap();
        stand_down.drain_events().unwrap();

        assert_eq!(&*accepted.lock().unwrap(), &[reachable]);
        assert!(pending.contains_key(&blocked));
        assert!(backpressured);
        assert_eq!(
            stand_down.snapshot(reachable).unwrap().closure(),
            super::super::ContextClosureState::Aborting
        );
    }

    #[test]
    fn one_attempts_stand_down_error_does_not_block_another_attempt() {
        let first = execution(101);
        let second = replacement(first, 2);
        let frontend = FrontendProcessId::new_v7();
        let blocked = QueryContextRef::new(first, frontend, BackendProcessId::new_v7());
        let reachable = QueryContextRef::new(second, frontend, BackendProcessId::new_v7());
        let first_activation = AttemptActivationIdentity::from_parts(17, first, 1);
        let second_activation = AttemptActivationIdentity::from_parts(17, second, 2);
        let make_attempt = |activation, context| {
            let contexts = BTreeSet::from([context]);
            let establish = EstablishIssueLedger::new(
                contexts.clone(),
                NonZeroUsize::new(2).unwrap(),
                NonZeroUsize::new(2).unwrap(),
            );
            let mut stand_down =
                ContextStandDownLedger::new(contexts, NonZeroUsize::new(2).unwrap()).unwrap();
            stand_down
                .begin(
                    activation,
                    ContextStandDownCause::LogicalExecutionFailed,
                    [(context, super::super::EstablishStandDownFact::AbortRequired)],
                )
                .unwrap();
            AttemptLedgers {
                activation,
                establish,
                establish_error: None,
                establish_protocol_error: None,
                stand_down,
                stand_down_error: None,
                pending_aborts: BTreeMap::new(),
                abort_backpressured: false,
                attempt_obligation: None,
                active_resources: None,
            }
        };
        let mut attempts = BTreeMap::from([
            (first, make_attempt(first_activation, blocked)),
            (second, make_attempt(second_activation, reachable)),
        ]);
        attempts.get_mut(&first).unwrap().stand_down_error =
            Some(ContextStandDownError::WrongState);
        let (capacity, _) = watch::channel(0);
        let accepted = Arc::new(Mutex::new(Vec::new()));
        let port = SelectiveAbortPort {
            blocked,
            accepted: Arc::clone(&accepted),
            capacity,
        };
        let mut state = LogicalExecutionState::new(
            first,
            RecoveryMode::NoRecovery,
            ExecutionEffect::None,
            LogicalOutputMode::CompletionOnly,
            1,
        )
        .unwrap();
        let first_capability = state.attempt_capability(first).unwrap();
        state.mark_running(&first_capability).unwrap();
        let mut global_error = Some(ContextStandDownError::WrongState);

        synchronize_all_stand_down(
            &state,
            &mut attempts,
            &mut global_error,
            MonotonicInstant::ORIGIN,
            Some(&port),
        );

        assert_eq!(&*accepted.lock().unwrap(), &[reachable]);
    }

    #[tokio::test]
    async fn failed_stand_down_attempt_does_not_busy_wake_on_an_old_retry_deadline() {
        let runtime = Handle::current();
        let execution = execution(102);
        let frontend = FrontendProcessId::new_v7();
        let unknown = QueryContextRef::new(execution, frontend, BackendProcessId::new_v7());
        let blocked = QueryContextRef::new(execution, frontend, BackendProcessId::new_v7());
        let clock = Arc::new(ManualActorClock::new(MonotonicInstant::ORIGIN));
        let port = Arc::new(ClosingMixedAbortPort::new(unknown, blocked));
        let (work_owner, stage) = test_governed_work();
        let config = LogicalExecutionActorConfig::single_attempt_completion(
            execution,
            ExecutionEffect::None,
            NonZeroUsize::new(4).unwrap(),
            vec![unknown, blocked],
            NonZeroUsize::new(2).unwrap(),
            NonZeroUsize::new(2).unwrap(),
            work_owner,
            stage,
        )
        .unwrap()
        .with_abort_query_context_effect_port(port.clone(), NonZeroUsize::new(2).unwrap())
        .with_clock(clock.clone());
        let (owner, initial, _output) = spawn_logical_execution_actor(&runtime, config)
            .unwrap()
            .into_parts();
        let actor = owner.actor();
        let running = actor.activate(initial.ready()).await.unwrap();
        for (context, tag) in [(unknown, 31), (blocked, 32)] {
            let issue = running
                .begin_admission_issue(admission_request(context, tag))
                .await
                .unwrap();
            running
                .settle_admission_issue(
                    issue,
                    AdmissionIssueSettlement::applied(
                        issue.operation_id(),
                        OperationOutcome::Accepted,
                        admission_ticket(context, tag),
                    )
                    .unwrap(),
                )
                .await
                .unwrap();
        }
        assert_eq!(
            actor.fail_attempt(running).await.unwrap(),
            LogicalConclusion::Failed
        );

        let submission = tokio::time::timeout(Duration::from_secs(1), async {
            loop {
                if let Some(submission) = port.submissions.lock().unwrap().pop() {
                    break submission;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("the accepted context must reach the Abort effect owner");
        assert_eq!(submission.identity().context(), unknown);
        let _late = submission.transport_unknown().unwrap();
        tokio::time::timeout(Duration::from_secs(1), async {
            loop {
                let snapshot = actor.stand_down_snapshot(unknown).await.unwrap().unwrap();
                if snapshot.issue_state()
                    == Some(super::super::AbortQueryContextIssueState::TransportUnknown)
                {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("transport-unknown retry must be retained by the stand-down ledger");

        port.close_capacity();
        tokio::time::timeout(Duration::from_secs(1), async {
            loop {
                if actor.snapshot().await.unwrap().stand_down_error
                    == Some(ContextStandDownError::EffectCapacityClosed)
                {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("closing Abort capacity must fail the owning attempt");
        let reserve_calls_after_failure = port.reserve_calls.load(Ordering::SeqCst);

        clock.set(MonotonicInstant::from_origin(Duration::from_millis(100)));
        tokio::time::sleep(Duration::from_millis(75)).await;
        let snapshot = tokio::time::timeout(Duration::from_millis(250), actor.snapshot())
            .await
            .expect("an expired retry from a failed attempt must not starve the mailbox")
            .unwrap();
        assert_eq!(
            snapshot.stand_down_error,
            Some(ContextStandDownError::EffectCapacityClosed)
        );
        let residual = tokio::time::timeout(
            Duration::from_millis(250),
            actor.stand_down_snapshot(unknown),
        )
        .await
        .expect("the failed attempt's residual ledger must remain observable")
        .unwrap()
        .unwrap();
        assert_eq!(
            residual.issue_state(),
            Some(super::super::AbortQueryContextIssueState::TransportUnknown)
        );
        assert_eq!(
            port.reserve_calls.load(Ordering::SeqCst),
            reserve_calls_after_failure,
            "a failed attempt must retain its ledger without issuing another Abort effect"
        );
    }
    #[tokio::test]
    async fn actor_replacement_accepts_exact_process_witness_without_reclassifying_original_error()
    {
        let first = execution(12_170);
        let second = replacement(first, 2);
        let context = QueryContextRef::new(
            first,
            FrontendProcessId::new_v7(),
            BackendProcessId::new_v7(),
        );
        let (input, witness, failure) =
            crate::api::pre_ready_identity_refusal_test_fixture(context);
        let original = failure.error().clone();
        let port = Arc::new(DelayedQualificationPort::default());
        let submissions = Arc::clone(&port.submissions);
        let config = recovery_config_with_contexts(first, vec![context], port, 2)
            .with_abort_query_context_effect_port(
                super::super::PermanentlyBackpressuredAbortEffectPort::shared(),
                NonZeroUsize::new(2).unwrap(),
            );
        let scope = config.work_owner.as_ref().unwrap().scope();
        let (owner, initial, output) = spawn_logical_execution_actor(&Handle::current(), config)
            .unwrap()
            .into_parts();
        let actor = owner.actor().clone();
        let residual = owner.into_residual_stand_down_supervisor();
        let ExecutionOutput::Rows(mut stream) = output.into_output() else {
            panic!("recovery rows must retain their original result stream");
        };
        stream.begin_schema().unwrap().complete();
        let running = actor.activate(initial.ready()).await.unwrap();
        let ticket = admission_ticket(context, 73);
        let admission = running
            .begin_admission_issue(admission_request(context, 73))
            .await
            .unwrap();
        running
            .settle_admission_issue(
                admission,
                AdmissionIssueSettlement::applied(
                    admission.operation_id(),
                    OperationOutcome::Accepted,
                    ticket,
                )
                .unwrap(),
            )
            .await
            .unwrap();
        let establish = running
            .authorize_establish(
                establish_request_with_operation(context, ticket, 73, witness.operation_id()),
                NativeCompatibilityId::new([73; 32]),
            )
            .await
            .unwrap();
        let transport = HoldingEstablishTransport::default();
        assert_eq!(
            establish.try_submit(&transport).unwrap(),
            EstablishIssueSubmit::Accepted
        );
        let submission = transport.take();
        assert_eq!(submission.identity().operation_id(), witness.operation_id());
        assert_eq!(submission.identity().context(), context);
        submission
            .worker_settled(OperationOutcome::IdentityMismatch)
            .unwrap();
        running
            .freeze_result_attempt_failure(original.clone())
            .await
            .unwrap();
        let qualification = actor
            .begin_replacement_with_error(
                running,
                failure.class(),
                original.clone(),
                Some(witness),
                second,
                Vec::new(),
            )
            .await
            .unwrap();
        assert_eq!(failure.class(), AttemptFailureClass::ContractViolation);
        assert!(matches!(
            actor.snapshot().await.unwrap().phase,
            ExecutionPhase::Replacing { .. }
        ));
        submissions
            .lock()
            .unwrap()
            .pop()
            .expect("same actor issued qualification")
            .rejected();
        wait_for_conclusion(&actor, LogicalConclusion::Failed).await;
        let observed = match stream.next().await {
            Err(error) => error,
            Ok(_) => panic!("the original failure remains selected"),
        };
        assert_eq!(observed, original);
        assert_eq!(observed.execution_failure(), Some(&input));
        // This fixture's exact Worker is stopped and its admission/context
        // fenced. Logical failure alone is not original Root convergence.
        residual
            .observe_worker_stopped_and_context_fenced(context)
            .await
            .unwrap();
        drop((qualification, stream, actor));
        tokio::time::timeout(Duration::from_secs(1), residual.join())
            .await
            .expect("the original actor actually exits")
            .unwrap();
        tokio::time::timeout(Duration::from_secs(1), scope.wait_released())
            .await
            .expect("original Root releases after actor exit");
    }
    #[tokio::test]
    async fn native_drive_observation_uses_exact_issue_and_current_owner_policy() {
        use crate::execution_error::{ExecutionFailureInput, ExecutionFailureSite};
        for recovery in [false, true] {
            for outcome in [
                OperationOutcome::ContextConflict,
                OperationOutcome::Accepted,
                OperationOutcome::IdentityMismatch,
            ] {
                let applied = outcome == OperationOutcome::Accepted;
                let first = execution(if recovery { 12_181 } else { 12_180 });
                let context = QueryContextRef::new(
                    first,
                    FrontendProcessId::new_v7(),
                    BackendProcessId::new_v7(),
                );
                let config = if recovery {
                    recovery_config_with_contexts(
                        first,
                        vec![context],
                        Arc::new(DelayedQualificationPort::default()),
                        2,
                    )
                } else {
                    let (work_owner, stage) = test_governed_work();
                    LogicalExecutionActorConfig::single_attempt_read_rows(
                        first,
                        NonZeroUsize::new(4).unwrap(),
                        vec![context],
                        NonZeroUsize::new(2).unwrap(),
                        NonZeroUsize::new(2).unwrap(),
                        work_owner,
                        stage,
                        result_schema(),
                        NonZeroUsize::new(1).unwrap(),
                        result_carrier(),
                    )
                    .unwrap()
                }
                .with_abort_query_context_effect_port(
                    super::super::PermanentlyBackpressuredAbortEffectPort::shared(),
                    NonZeroUsize::new(2).unwrap(),
                );
                let (owner, initial, output) =
                    spawn_logical_execution_actor(&Handle::current(), config)
                        .unwrap()
                        .into_parts();
                let actor = owner.actor().clone();
                let running = actor.activate(initial.ready()).await.unwrap();
                let drive = super::super::NativeAttemptDrive::new(&running);
                let (unissued, _, _) = crate::api::pre_ready_identity_refusal_test_fixture(context);
                assert!(
                    !drive
                        .may_observe_pre_ready_replacement(&unissued)
                        .await
                        .unwrap()
                );
                let ticket = admission_ticket(context, 72);
                let admission = drive
                    .begin_admission_issue(admission_request(context, 72))
                    .await
                    .unwrap();
                drive
                    .settle_admission_issue(
                        admission,
                        AdmissionIssueSettlement::applied(
                            admission.operation_id(),
                            OperationOutcome::Accepted,
                            ticket,
                        )
                        .unwrap(),
                    )
                    .await
                    .unwrap();
                let request = establish_request(context, ticket, 72);
                let input = ExecutionFailureInput::new(
                    unissued.fact(),
                    unissued.detail().clone(),
                    ExecutionFailureSite::Operation {
                        context,
                        operation_id: request.envelope().operation_id(),
                        kind: novarocks_execution_contract::OperationKind::UpdateQueryContext,
                    },
                );
                let create_input = ExecutionFailureInput::new(
                    unissued.fact(),
                    unissued.detail().clone(),
                    ExecutionFailureSite::Operation {
                        context,
                        operation_id: novarocks_execution_contract::TaskOperationId::new_v7(),
                        kind: novarocks_execution_contract::OperationKind::CreateTask,
                    },
                );
                // An admission ticket alone does not prove the parent context.
                assert!(
                    !drive
                        .may_observe_pre_ready_replacement(&create_input)
                        .await
                        .unwrap()
                );
                let issue = drive
                    .authorize_establish(request, NativeCompatibilityId::new([72; 32]))
                    .await
                    .unwrap();
                let transport = HoldingEstablishTransport::default();
                assert_eq!(
                    issue.try_submit(&transport).unwrap(),
                    EstablishIssueSubmit::Accepted
                );
                let submission = transport.take();
                let issued_identity = submission.identity();
                assert_eq!(issued_identity.context(), context);
                assert_eq!(
                    issued_identity.operation_id(),
                    match input.site() {
                        ExecutionFailureSite::Operation { operation_id, .. } => operation_id,
                        _ => panic!("the Update refusal retains the actual issued operation"),
                    }
                );
                assert_eq!(
                    actor
                        .establish_snapshot(context)
                        .await
                        .unwrap()
                        .unwrap()
                        .state(),
                    super::super::EstablishIssueState::TransportOwned,
                );
                assert_eq!(
                    drive
                        .may_observe_pre_ready_replacement(&create_input)
                        .await
                        .unwrap(),
                    recovery,
                    "the exact transport-owned parent preserves the pre-ready window",
                );
                let late = submission.transport_unknown().unwrap();
                assert_eq!(late.identity(), issued_identity);
                assert_eq!(
                    actor
                        .establish_snapshot(context)
                        .await
                        .unwrap()
                        .unwrap()
                        .state(),
                    super::super::EstablishIssueState::TransportUnknown,
                );
                assert_eq!(
                    drive
                        .may_observe_pre_ready_replacement(&create_input)
                        .await
                        .unwrap(),
                    recovery,
                    "the same unknown parent remains owned while its exact receipt is late",
                );
                late.worker_settled(outcome).unwrap();
                // The real actor retains the exact issued operation even after its
                // rejection. QA tests eligibility; remote QUERY15 provenance and
                // finite topology observation are exercised by the Native owner.
                assert_eq!(
                    drive
                        .may_observe_pre_ready_replacement(&input)
                        .await
                        .unwrap(),
                    recovery
                );
                assert!(
                    !drive
                        .may_observe_pre_ready_replacement(&unissued)
                        .await
                        .unwrap()
                );
                assert_eq!(
                    drive
                        .may_observe_pre_ready_replacement(&create_input)
                        .await
                        .unwrap(),
                    recovery && (applied || outcome == OperationOutcome::IdentityMismatch),
                );
                let foreign_context = QueryContextRef::new(
                    first,
                    FrontendProcessId::new_v7(),
                    context.backend_process_id(),
                );
                let foreign_create = ExecutionFailureInput::new(
                    create_input.fact(),
                    create_input.detail().clone(),
                    ExecutionFailureSite::Operation {
                        context: foreign_context,
                        operation_id: novarocks_execution_contract::TaskOperationId::new_v7(),
                        kind: novarocks_execution_contract::OperationKind::CreateTask,
                    },
                );
                assert!(
                    !drive
                        .may_observe_pre_ready_replacement(&foreign_create)
                        .await
                        .unwrap()
                );
                assert_eq!(actor.snapshot().await.unwrap().conclusion, None);
                let error = QueryExecutionError::from_execution(input.clone());
                actor.fail_attempt_with_error(running, error).await.unwrap();
                assert!(
                    drive
                        .may_observe_pre_ready_replacement(&input)
                        .await
                        .is_err()
                );
                drop((drive, actor, output));
                let residual = owner.into_residual_stand_down_supervisor();
                residual
                    .observe_worker_stopped_and_context_fenced(context)
                    .await
                    .unwrap();
                tokio::time::timeout(Duration::from_secs(1), residual.join())
                    .await
                    .expect("the actual actor and original context converge")
                    .unwrap();
            }
        }
    }

    #[tokio::test]
    async fn begin_replacement_drains_the_same_late_parent_refusal_before_final_acceptance() {
        let first = execution(12_182);
        let context = QueryContextRef::new(
            first,
            FrontendProcessId::new_v7(),
            BackendProcessId::new_v7(),
        );
        let (input, witness, failure) =
            crate::api::pre_ready_identity_refusal_test_fixture_for_kind(
                context,
                novarocks_execution_contract::OperationKind::CreateTask,
            );
        let original = failure.error().clone();
        let port = Arc::new(DelayedQualificationPort::default());
        let mut config = recovery_config_with_contexts(first, vec![context], port.clone(), 2);
        let scope = config.work_owner.as_ref().unwrap().scope();
        let cancellation = scope.cancellation().unwrap();
        let clock = ManualActorClock::new(MonotonicInstant::ORIGIN);
        let mut state = LogicalExecutionState::new(
            config.initial_execution,
            config.recovery_mode,
            config.effect,
            config.output_mode,
            config.max_attempts,
        )
        .unwrap();
        let capability = state.attempt_capability(first).unwrap();
        state.mark_running(&capability).unwrap();
        let activation = identity_of(&capability);
        // One original actor ledger owns this real admission/Establish. No
        // actor loop runs in this synchronous handler fixture, so it cannot
        // consume the queued late receipt before BeginReplacement's drain.
        let mut attempt = new_attempt_ledgers(
            activation,
            BTreeSet::from([context]),
            config.max_admission_issues_per_context,
            config.max_establish_authorizations_per_context,
            config.max_abort_authorizations_per_context,
            None,
            &scope,
        )
        .unwrap();
        let ticket = admission_ticket(context, 74);
        let admission = attempt
            .establish
            .begin_admission_issue(activation, admission_request(context, 74), clock.now())
            .unwrap();
        attempt
            .establish
            .settle_admission_issue(
                activation,
                admission,
                AdmissionIssueSettlement::applied(
                    admission.operation_id(),
                    OperationOutcome::Accepted,
                    ticket,
                )
                .unwrap(),
                clock.now(),
            )
            .unwrap();
        let request = establish_request(context, ticket, 74);
        let operation_id = request.envelope().operation_id();
        let issue = attempt
            .establish
            .authorize_issue(
                activation,
                request,
                NativeCompatibilityId::new([74; 32]),
                clock.now(),
            )
            .unwrap();
        let transport = HoldingEstablishTransport::default();
        assert_eq!(
            issue.try_submit(&transport).unwrap(),
            EstablishIssueSubmit::Accepted
        );
        let submission = transport.take();
        let parent_identity = submission.identity();
        assert_eq!(parent_identity.context(), context);
        assert_eq!(parent_identity.operation_id(), operation_id);
        let late = submission.transport_unknown().unwrap();
        assert_eq!(late.identity(), parent_identity);
        attempt.establish.drain_events().unwrap();
        assert_eq!(
            attempt.establish.snapshot(context).unwrap().state(),
            super::super::EstablishIssueState::TransportUnknown,
        );
        let mut attempts = BTreeMap::from([(first, attempt)]);
        assert!(pre_ready_refusal_matches_establish(
            &attempts, activation, &input
        ));
        assert!(state.may_observe_pre_ready_replacement(false));

        let carrier = config.result_row_carrier.unwrap();
        let (transport, schema_receipt, failure_sender, mut stream) =
            QueryResultStream::try_channel(
                first.query_id(),
                config.result_schema.take().unwrap(),
                carrier,
                config.result_delivery_capacity.unwrap().get(),
            )
            .unwrap();
        stream.begin_schema().unwrap().complete();
        let mut result = ResultRuntime {
            carrier,
            schema_receipt: Some(schema_receipt),
            schema_writer_completed: false,
            failure_sender: Some(failure_sender),
            accepted_error: None,
            attempt_error: None,
            transport: ResultTransport::Client(transport),
            internal: None,
            internal_outcome: None,
            root_success: None,
            attempt_decision_pending: None,
            finish_waiter: None,
            pending: None,
            in_flight: None,
            deferred_internal: None,
        };
        let (abandoned, _abandoned_rx) = watch::channel(false);
        let lifetime = Arc::new(PermitLifetime {
            abandoned,
            settled: AtomicBool::new(false),
        });
        let (mailbox, _receiver) = mpsc::channel(4);
        let running = RunningAttemptPermit::new(capability, Arc::clone(&lifetime), mailbox);
        let mut establish_error = None;
        let mut stand_down_error = None;
        let mut replacement_runtime = None;
        let mut replacement_error = None;
        let mut root_terminal_receiver = None;
        let (reply, response) = oneshot::channel();
        // The exact transport's late result is queued after opportunity was
        // observed. The snapshot is still Unknown until the original handler
        // drains it while preparing this same attempt's stand-down.
        late.worker_settled(OperationOutcome::ContextConflict)
            .unwrap();
        assert!(pre_ready_refusal_matches_establish(
            &attempts, activation, &input
        ));
        handle_command(
            &mut state,
            &mut attempts,
            &mut establish_error,
            &mut stand_down_error,
            &mut replacement_runtime,
            &mut replacement_error,
            &clock,
            &cancellation,
            config.replacement_reservation_valid_for,
            Some(&mut result),
            &mut root_terminal_receiver,
            None,
            ActorCommand::BeginReplacement {
                permit: running,
                failure: failure.class(),
                pre_ready_replacement: Some(witness),
                terminal_error: Some(original.clone()),
                replacement: replacement(first, 2),
                replacement_contexts: Vec::new(),
                reply,
            },
        );
        assert!(matches!(
            response.await.unwrap(),
            Err(LogicalExecutionActorError::RecoveryRefused(
                RecoveryRefusal::FailureClass
            ))
        ));
        assert_eq!(state.conclusion(), Some(LogicalConclusion::Failed));
        let snapshot = attempts[&first].establish.snapshot(context).unwrap();
        assert_eq!(snapshot.identity(), Some(parent_identity));
        assert_eq!(
            snapshot.state(),
            super::super::EstablishIssueState::WorkerSettled(
                super::super::EstablishWorkerSettlement::Rejected(
                    OperationOutcome::ContextConflict
                ),
            )
        );
        assert!(attempts[&first].stand_down.started());
        assert!(!pre_ready_refusal_matches_establish(
            &attempts, activation, &input
        ));
        assert_eq!(result.accepted_error.as_ref(), Some(&original));
        assert!(replacement_runtime.is_none());
        let (receipts, _receipts_rx) = receipt_channel();
        drive_replacement_effect(
            replacement_runtime.as_mut(),
            Some(port.as_ref()),
            &receipts,
            clock.now(),
        );
        assert!(port.submissions.lock().unwrap().is_empty());
        assert!(lifetime.settled.load(Ordering::Acquire));
        fail_result_runtime(&mut state, Some(&mut result));
        let observed = match stream.next().await {
            Err(error) => error,
            Ok(_) => panic!("late non-identity parent refusal must keep the original error"),
        };
        assert_eq!(observed, original);
        assert_eq!(observed.execution_failure(), Some(&input));

        // No real Worker is spawned by this fixture. Its original Registry
        // owner supplies the exact stopped/context-fenced fact for cleanup.
        attempts
            .get_mut(&first)
            .unwrap()
            .stand_down
            .observe_registry_convergence(
                context,
                RegistryContextConvergence::WorkerStoppedAndContextFenced,
            )
            .unwrap();
        synchronize_all_stand_down(
            &state,
            &mut attempts,
            &mut stand_down_error,
            clock.now(),
            None,
        );
        assert_eq!(stand_down_error, None);
        assert!(actor_cleanup_complete(
            &state,
            &attempts,
            replacement_runtime.as_ref()
        ));
        release_concluded_actor_resources(
            &state,
            &mut config.execution_stage,
            replacement_runtime.as_mut(),
        );
    }
}
