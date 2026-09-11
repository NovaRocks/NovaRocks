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
    CancellationReason, CancellationView, Obligation, ObligationKey, ObligationKind, ResultCredit,
    Stage, StagePermit, WorkOwner, WorkScope,
};
use tokio::runtime::Handle;
use tokio::sync::{mpsc, oneshot, watch};
use tokio::task::JoinHandle;

use crate::api::{
    BatchDelivery, DecodedResultBatch, EndDelivery, ExecutionOutput, QueryExecutionError,
    QueryResultStream, QueryResultTransport, ResultDelivery, ResultDeliveryDisposition,
    ResultDeliveryReceipt, ResultQueuePermit, ResultSchema,
};

use super::actor_state::{
    ActorStateError, AttemptCapability, LogicalExecutionState, ReplacementFact, ReplacementToken,
};
use super::{
    AbortQueryContextEffectPort, AbortQueryContextIssuePermit, AbortQueryContextIssueSubmit,
    ActiveReplacementResources, AdmissionIssueDisposition, AdmissionIssueReceipt,
    AdmissionIssueSettlement, AttemptFailureClass, BeginSchemaDelivery, ContextStandDownCause,
    ContextStandDownError, ContextStandDownLedger, ContextStandDownSnapshot, DeliveryPermit,
    EstablishIssueError, EstablishIssueLedger, EstablishIssuePermit, EstablishIssueSnapshot,
    ExecutionEffect, ExecutionPhase, LogicalConclusion, LogicalOutputMode, MonotonicInstant,
    ObservedTaskTransition, RecoveryMode, RecoveryRefusal, RegistryContextConvergence,
    ReplacementQualificationEffectAdmission, ReplacementQualificationEffectPort,
    ReplacementQualificationEffectReceipt, ReplacementQualificationEffectSubmission,
    ReplacementQualificationFailure, ReplacementQualificationIdentity,
    ReplacementQualificationRequest, ReplacementQualificationSettlement,
    ReplacementWorkerAdmissionEvidence, ResultPacket, SchemaDeliveryPermit, StatusObservation,
    SuccessEndOfStreamPermit, classify_observation, classify_observed_task_transition,
    receipt_channel,
};

/// Immutable identity of one actor-owned attempt activation generation.
///
/// This value is diagnostic only. State transitions require the corresponding
/// move-only permit, so copying an identity cannot create authority.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct AttemptActivationIdentity {
    actor_instance_id: u64,
    execution: QueryExecutionId,
    generation: u64,
}

/// Opaque process-local identity of one logical execution actor instance.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct LogicalExecutionActorId(u64);

impl LogicalExecutionActorId {
    pub const fn get(self) -> u64 {
        self.0
    }
}

/// Actor-owned monotonic time authority. Callers never provide a current time
/// or derive an admission-ticket expiry themselves.
pub trait LogicalExecutionClock: fmt::Debug + Send + Sync {
    fn now(&self) -> MonotonicInstant;
}

#[derive(Debug)]
pub struct ProcessLogicalExecutionClock {
    origin: Instant,
}

impl ProcessLogicalExecutionClock {
    pub fn new() -> Self {
        Self {
            origin: Instant::now(),
        }
    }
}

impl Default for ProcessLogicalExecutionClock {
    fn default() -> Self {
        Self::new()
    }
}

impl LogicalExecutionClock for ProcessLogicalExecutionClock {
    fn now(&self) -> MonotonicInstant {
        MonotonicInstant::from_origin(self.origin.elapsed())
    }
}

impl AttemptActivationIdentity {
    pub(crate) const fn from_parts(
        actor_instance_id: u64,
        execution: QueryExecutionId,
        generation: u64,
    ) -> Self {
        Self {
            actor_instance_id,
            execution,
            generation,
        }
    }

    pub const fn actor(self) -> LogicalExecutionActorId {
        LogicalExecutionActorId(self.actor_instance_id)
    }

    pub const fn execution(self) -> QueryExecutionId {
        self.execution
    }

    pub const fn generation(self) -> u64 {
        self.generation
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
    pub(crate) const fn activation(&self) -> AttemptActivationIdentity {
        self.activation
    }

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

    pub(crate) async fn observe_final_worker_eos_ack(
        &self,
        root: TaskIdentity,
        sequence: ResultPacketSequence,
    ) -> Result<(), LogicalExecutionActorError> {
        request(&self.mailbox, |reply| ActorCommand::ObserveRootEosAck {
            activation: self.activation,
            root,
            expected_root: self.root,
            sequence,
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

    /// Transfers one decoded batch to the actor-owned delivery boundary. The
    /// future resolves only after the protocol consumer completes or rejects
    /// the batch, so a Native adapter can delay its Worker ACK until success.
    /// If the waiter is cancelled after submission, `snapshot` retains the
    /// delivered watermark and is the retryable ACK authority.
    pub(crate) async fn deliver_result_batch(
        &self,
        sequence: ResultPacketSequence,
        batch: DecodedResultBatch,
        credit: ResultCredit,
    ) -> Result<(), LogicalExecutionActorError> {
        request(self.mailbox(), |reply| ActorCommand::DeliverResultBatch {
            activation: self.identity(),
            sequence,
            batch,
            credit,
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

    /// Transfers the running authority after the coordinator has submitted
    /// all root-result packets. This does not assert success: the actor still
    /// requires exact root Finished and final Worker EOS ACK observations.
    pub(crate) async fn finish_result_stream(
        self,
    ) -> Result<LogicalConclusion, RunningAttemptHandoffError> {
        self.handoff(|permit, reply| ActorCommand::FinishResultStream { permit, reply })
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
    AttemptActivationIdentity {
        actor_instance_id: capability.actor_instance_id(),
        execution: capability.execution(),
        generation: capability.generation(),
    }
}

fn retired_obligation_key(identity: ReplacementQualificationIdentity) -> ObligationKey {
    let query = identity.failed().query_id();
    let mut bytes = [0_u8; 32];
    bytes[0..8].copy_from_slice(&identity.actor().get().to_le_bytes());
    bytes[8..16].copy_from_slice(&query.high().to_le_bytes());
    bytes[16..24].copy_from_slice(&query.low().to_le_bytes());
    bytes[24..32].copy_from_slice(&identity.failed().attempt_id().get().to_le_bytes());
    ObligationKey(bytes)
}

/// Honest construction inputs for the currently connected actor slice.
///
/// T08 first connects a single, completion-only attempt. Recovery and result
/// delivery remain reducer capabilities until their actor-owned effect gates
/// are connected; callers cannot select those modes prematurely.
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
    clock: Arc<dyn LogicalExecutionClock>,
    work_owner: Option<WorkOwner>,
    execution_stage: Option<StagePermit>,
    result_schema: Option<ResultSchema>,
    result_delivery_capacity: Option<NonZeroUsize>,
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
            owner.complete();
        }
    }
}

fn close_unstarted_governance(work_owner: WorkOwner, execution_stage: StagePermit) {
    drop(execution_stage);
    work_owner.complete();
}

impl LogicalExecutionActorConfig {
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
            clock: Arc::new(ProcessLogicalExecutionClock::new()),
            work_owner: Some(work_owner),
            execution_stage: Some(initial_execution_stage),
            result_schema: None,
            result_delivery_capacity: None,
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
    #[allow(clippy::too_many_arguments)]
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
        .with_result_stream(schema, delivery_capacity))
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
            clock: Arc::new(ProcessLogicalExecutionClock::new()),
            work_owner: Some(work_owner),
            execution_stage: Some(initial_execution_stage),
            result_schema: None,
            result_delivery_capacity: None,
        })
    }

    /// Constructs an effect-free read whose whole attempt may restart only
    /// before any result packet becomes visible.
    #[allow(clippy::too_many_arguments)]
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
        .with_result_stream(schema, delivery_capacity))
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

    #[cfg(test)]
    pub(crate) fn with_clock(mut self, clock: Arc<dyn LogicalExecutionClock>) -> Self {
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
            Self::Establish(_)
            | Self::StandDown(_)
            | Self::RecoveryRefused(_)
            | Self::ReplacementQualificationFailed(_) => unreachable!("handled above"),
        })
    }
}

impl std::error::Error for LogicalExecutionActorError {}

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
    stand_down: ContextStandDownLedger,
    stand_down_error: Option<ContextStandDownError>,
    pending_aborts: BTreeMap<QueryContextRef, AbortQueryContextIssuePermit>,
    abort_backpressured: bool,
    retired_obligation: Option<Obligation>,
    active_resources: Option<ActiveReplacementResources>,
}

impl fmt::Debug for AttemptLedgers {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("AttemptLedgers")
            .field("activation", &self.activation)
            .field("has_retired_obligation", &self.retired_obligation.is_some())
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
    delivery: BatchDelivery,
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
    Authoritative(QueryExecutionError),
}

enum PendingResult {
    Batch(PendingResultBatch),
    End(PendingResultEnd),
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
        permit: SuccessEndOfStreamPermit,
        receipt: ResultDeliveryReceipt,
        lifetime: Arc<PermitLifetime>,
        reply: ActorReply<LogicalConclusion>,
    },
}

struct ResultRuntime {
    schema: ResultSchema,
    schema_receipt: Option<ResultDeliveryReceipt>,
    schema_writer_completed: bool,
    failure_sender: Option<watch::Sender<Option<QueryExecutionError>>>,
    terminal_error: Option<QueryExecutionError>,
    transport: QueryResultTransport,
    root_success: Option<RootSuccessGate>,
    attempt_decision_pending: Option<AttemptActivationIdentity>,
    finish_waiter: Option<PendingResultFinish>,
    pending: Option<PendingResult>,
    in_flight: Option<InFlightResult>,
}

enum ResultReceiptOutcome {
    Schema(Result<ResultDeliveryDisposition, ()>),
    Delivery(Result<ResultDeliveryDisposition, ()>),
}

impl ResultRuntime {
    fn idle(&self) -> bool {
        self.finish_waiter.is_none() && self.pending.is_none() && self.in_flight.is_none()
    }
}

enum ActorCommand {
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
    DeliverResultBatch {
        activation: AttemptActivationIdentity,
        sequence: ResultPacketSequence,
        batch: DecodedResultBatch,
        credit: ResultCredit,
        reply: ActorReply<()>,
    },
    BindRootResult {
        activation: AttemptActivationIdentity,
        root: TaskIdentity,
        terminal_receiver: mpsc::Receiver<RootTerminalObservation>,
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
        reply: ActorReply<()>,
    },
    FinishResultStream {
        permit: RunningAttemptPermit,
        reply: ActorReply<LogicalConclusion>,
    },
    AwaitWorkCancellation {
        permit: RunningAttemptPermit,
        reply: ActorReply<LogicalConclusion>,
    },
    BeginReplacement {
        permit: RunningAttemptPermit,
        failure: AttemptFailureClass,
        terminal_error: Option<QueryExecutionError>,
        replacement: QueryExecutionId,
        replacement_contexts: Vec<QueryContextRef>,
        reply: ActorReply<ReplacementQualification>,
    },
    ActivateReplacement {
        qualification: ReplacementQualification,
        reply: ActorReply<AttemptInstantiationPermit>,
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

/// Cloneable bounded mailbox handle. Authority-bearing commands wait
/// asynchronously for capacity; abandoning their future drops the exact
/// permit and wakes the actor through a separate coalesced signal.
#[derive(Clone, Debug)]
pub struct LogicalExecutionActor {
    id: LogicalExecutionActorId,
    sender: mpsc::Sender<ActorCommand>,
    registry_close_requested: watch::Sender<bool>,
    registry_ingress_closed: watch::Receiver<bool>,
}

impl LogicalExecutionActor {
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

    pub(crate) fn request_registry_close_for_join(&self) -> watch::Receiver<bool> {
        // This signal is independent of ordinary mailbox capacity. Once set,
        // dropping the returned waiter never retracts cancellation or ingress
        // closure.
        let closed = self.registry_ingress_closed.clone();
        if *closed.borrow() {
            return closed;
        }
        self.registry_close_requested.send_replace(true);
        closed
    }

    #[cfg(test)]
    pub(crate) fn registry_ingress_closed(&self) -> watch::Receiver<bool> {
        self.registry_ingress_closed.clone()
    }

    #[cfg(test)]
    pub(crate) fn registry_close_was_requested(&self) -> bool {
        *self.registry_close_requested.borrow()
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
        replacement: QueryExecutionId,
        replacement_contexts: Vec<QueryContextRef>,
    ) -> Result<ReplacementQualification, LogicalExecutionActorError> {
        request(&self.sender, |reply| ActorCommand::BeginReplacement {
            permit,
            failure,
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
    output: ExecutionOutput,
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
    let actor_id = LogicalExecutionActorId(capability.actor_instance_id());
    let (abandoned, abandoned_rx) = watch::channel(false);
    let lifetime = Arc::new(PermitLifetime {
        abandoned,
        settled: AtomicBool::new(false),
    });
    let (sender, receiver) = mpsc::channel(config.mailbox_capacity.get());
    let (registry_close_requested, registry_close_requested_rx) = watch::channel(false);
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
    let work_owner = config
        .work_owner
        .take()
        .ok_or(LogicalExecutionActorError::InvariantViolation)?;
    let attempts = BTreeMap::from([(
        config.initial_execution,
        AttemptLedgers {
            activation: capability_identity,
            establish,
            establish_error: None,
            stand_down,
            stand_down_error: None,
            pending_aborts: BTreeMap::new(),
            abort_backpressured: false,
            retired_obligation: None,
            active_resources: None,
        },
    )]);
    let clock = Arc::clone(&config.clock);
    let actor_lifetime = Arc::clone(&lifetime);
    let abort_effect_port = config.abort_effect_port.take();
    let replacement_effect_port = config.replacement_effect_port.take();
    let (result_runtime, output) = match config.output_mode {
        LogicalOutputMode::CompletionOnly => (None, ExecutionOutput::Completion),
        LogicalOutputMode::ResultStream => {
            let schema = config
                .result_schema
                .take()
                .ok_or(LogicalExecutionActorError::InvariantViolation)?;
            let capacity = config
                .result_delivery_capacity
                .ok_or(LogicalExecutionActorError::InvariantViolation)?;
            let (transport, schema_receipt, failure_sender, stream) =
                QueryResultStream::try_channel(
                    config.initial_execution.query_id(),
                    schema.clone(),
                    capacity.get(),
                )
                .map_err(|_| LogicalExecutionActorError::InvariantViolation)?;
            (
                Some(ResultRuntime {
                    schema,
                    schema_receipt: Some(schema_receipt),
                    schema_writer_completed: false,
                    failure_sender: Some(failure_sender),
                    terminal_error: None,
                    transport,
                    root_success: None,
                    attempt_decision_pending: None,
                    finish_waiter: None,
                    pending: None,
                    in_flight: None,
                }),
                ExecutionOutput::Rows(stream),
            )
        }
    };
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
    clock: Arc<dyn LogicalExecutionClock>,
    mut registry_close_requested_rx: watch::Receiver<bool>,
    registry_ingress_closed: watch::Sender<bool>,
) {
    let mut establish_error = None;
    let mut stand_down_error = None;
    let mut replacement_error = None;
    let mut replacement = None;
    let mut execution_stage = Some(execution_stage);
    let mut receiver_open = true;
    let mut registry_close_requested = false;
    let mut registry_close_signal_open = true;
    let mut root_terminal_receiver = None;
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
        if !registry_close_requested && *registry_close_requested_rx.borrow() {
            registry_close_requested = true;
            work_owner.cancel(CancellationReason::ServerShutdown);
        }
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
            max_admission_issues_per_context,
            max_establish_authorizations_per_context,
            max_abort_authorizations_per_context,
            clock.as_ref(),
        );
        release_concluded_actor_resources(state, &mut execution_stage, replacement.as_mut());
        if state.conclusion().is_some() {
            fail_result_runtime(state, result_runtime.as_mut());
        } else if drive_root_success(state, result_runtime.as_mut()).is_err() {
            conclude_failed(state);
            fail_result_runtime(state, result_runtime.as_mut());
        }
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
            work_owner.complete();
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
                && runtime.attempt_decision_pending.is_none())
            .then(|| runtime.transport.clone())
        });
        let result_receipt_pending = result_runtime
            .as_ref()
            .is_some_and(|runtime| runtime.in_flight.is_some() || runtime.schema_receipt.is_some());
        let result_consumer_transport = result_runtime.as_ref().and_then(|runtime| {
            state
                .conclusion()
                .is_none()
                .then(|| runtime.transport.clone())
        });
        let root_terminal_pending = root_terminal_receiver.is_some();
        tokio::select! {
            biased;
            changed = registry_close_requested_rx.changed(), if !registry_close_requested && registry_close_signal_open => {
                if changed.is_ok() && *registry_close_requested_rx.borrow() {
                    registry_close_requested = true;
                    work_owner.cancel(CancellationReason::ServerShutdown);
                } else if changed.is_err() {
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
                        result_runtime.as_mut(),
                        observation,
                    ),
                    None => {
                        root_terminal_receiver.take();
                        if !root_success_facts_complete(result_runtime.as_ref()) {
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
                apply_attempt_ledger_event(state, &mut attempts, event, &mut establish_error, &mut stand_down_error);
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
                apply_result_capacity(state, result_runtime.as_mut(), permit);
            }
            disposition = wait_for_result_receipt(result_runtime.as_mut()), if result_receipt_pending => {
                apply_result_receipt(state, replacement.as_ref(), result_runtime.as_mut(), disposition);
            }
            _ = wait_for_result_consumer_closed(result_consumer_transport), if result_consumer_transport.is_some() => {
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
                    &work_owner.scope(),
                    &work_cancellation,
                    replacement_reservation_valid_for,
                    result_runtime.as_mut(),
                    &mut root_terminal_receiver,
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
                        conclude_replacement_failed(state, replacement.as_mut());
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
    if state.conclusion().is_none() {
        return;
    }
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
        let _ = pending
            .reply
            .send(Err(LogicalExecutionActorError::WrongPhase));
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
    conclude_replacement_failed(state, Some(replacement));
}

enum AttemptLedgerEvent {
    Establish(QueryExecutionId, Result<(), EstablishIssueError>),
    StandDown(QueryExecutionId, Result<(), ContextStandDownError>),
}

async fn wait_for_attempt_ledger_event(
    attempts: &mut BTreeMap<QueryExecutionId, AttemptLedgers>,
    clock: &dyn LogicalExecutionClock,
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
    state: &mut LogicalExecutionState,
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
                if attempt.establish.has_worker_rejection() {
                    Err(EstablishIssueError::EstablishRejected)
                } else {
                    Ok(())
                }
            });
            if let Err(error) = result {
                establish_error.get_or_insert(error);
                attempt.establish_error.get_or_insert(error);
                attempt.establish.revoke_issue_authority();
                if matches!(state.phase(), ExecutionPhase::Running { execution: current, .. } if current == execution)
                {
                    conclude_failed(state);
                }
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
        if !attempt.stand_down.started() && state.conclusion() == Some(LogicalConclusion::Succeeded)
        {
            if let Err(error) = begin_or_refresh_successful_cleanup(attempt) {
                stand_down_error.get_or_insert(error);
                attempt.stand_down_error.get_or_insert(error);
            }
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
                LogicalConclusion::Failed | LogicalConclusion::BusinessDecisionRequired => {
                    Some(ContextStandDownCause::LogicalExecutionFailed)
                }
                LogicalConclusion::Succeeded => None,
            })
        };
        if let Some(cause) = cause {
            if let Err(error) = begin_or_refresh_stand_down(attempt, cause) {
                stand_down_error.get_or_insert(error);
                attempt.stand_down_error.get_or_insert(error);
            }
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
        if attempt.stand_down.residual_resource_settled() {
            let obligation_resolved = attempt
                .retired_obligation
                .as_ref()
                .is_none_or(Obligation::resolve);
            if obligation_resolved {
                attempt.retired_obligation.take();
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
            conclude_replacement_failed(state, Some(replacement));
        }
        return;
    };
    replacement.effect_settled = true;
    let (operation_id, identity, settlement) = receipt.into_parts();
    if identity != replacement.identity || operation_id != replacement.request.operation_id() {
        replacement_error.get_or_insert(ReplacementQualificationFailure::OutcomeUnknown);
        if state.conclusion().is_none() {
            conclude_replacement_failed(state, Some(replacement));
        }
        return;
    }
    match settlement {
        ReplacementQualificationSettlement::Qualified(reservation) => {
            let Some(dispatched_at) = replacement.effect_dispatched_at else {
                replacement_error
                    .get_or_insert(ReplacementQualificationFailure::InvalidReservation);
                conclude_replacement_failed(state, Some(replacement));
                return;
            };
            let ticket_expiry =
                reservation.conservative_expiry(dispatched_at, replacement.reservation_valid_for);
            replacement.conservative_expiry = Some(
                replacement
                    .conservative_expiry
                    .map_or(ticket_expiry, |current| current.min(ticket_expiry)),
            );
            if state.conclusion().is_some() {
                if let Some(pending) = replacement.activation.take() {
                    pending.lifetime.settle();
                    let _ = pending
                        .reply
                        .send(Err(LogicalExecutionActorError::WrongPhase));
                }
                return;
            }
            replacement.reservation = Some(reservation);
        }
        ReplacementQualificationSettlement::Failed(failure) => {
            replacement_error.get_or_insert(failure);
            if state.conclusion().is_none() {
                conclude_replacement_failed(state, Some(replacement));
            }
        }
    }
}

fn conclude_replacement_failed(
    state: &mut LogicalExecutionState,
    replacement: Option<&mut ReplacementRuntime>,
) {
    if let Some(replacement) = replacement {
        let _ = state.conclude_replacement(&replacement.token, LogicalConclusion::Failed);
    }
}

fn try_activate_qualified_replacement(
    state: &mut LogicalExecutionState,
    attempts: &mut BTreeMap<QueryExecutionId, AttemptLedgers>,
    replacement: &mut Option<ReplacementRuntime>,
    max_admission_issues_per_context: NonZeroUsize,
    max_establish_authorizations_per_context: NonZeroUsize,
    max_abort_authorizations_per_context: NonZeroUsize,
    clock: &dyn LogicalExecutionClock,
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
        conclude_replacement_failed(state, Some(active));
        return;
    };
    if clock.now().has_reached(conservative_expiry) {
        pending_activation.lifetime.settle();
        let _ = pending_activation.reply.send(Err(
            LogicalExecutionActorError::ReplacementQualificationFailed(
                ReplacementQualificationFailure::Expired,
            ),
        ));
        conclude_replacement_failed(state, Some(active));
        return;
    }
    // The opaque reservation proves reachability, isolation, successor
    // admission, and that the registry's old usage record remains under
    // last-known/current-unknown ownership. The exact failed attempt must also
    // have registered the actor-owned same-scope RetiredAttempt obligation,
    // whose registration consumes old-attempt capacity even when no byte
    // observation is available. The handle remains present until positive
    // residual convergence; an already converged no-remote attempt may have
    // resolved it before successor activation.
    if !active.stale_usage_accounted
        || !attempts
            .get(&active.identity.failed())
            .is_some_and(|attempt| {
                attempt.retired_obligation.is_some()
                    || attempt.stand_down.residual_resource_settled()
            })
    {
        pending_activation.lifetime.settle();
        let _ = pending_activation.reply.send(Err(
            LogicalExecutionActorError::ReplacementQualificationFailed(
                ReplacementQualificationFailure::InvalidReservation,
            ),
        ));
        conclude_replacement_failed(state, Some(active));
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
            conclude_replacement_failed(state, Some(active));
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
            conclude_replacement_failed(state, Some(active));
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
        conclude_replacement_failed(state, Some(active));
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
                    if let Err(response) = pending_activation.reply.send(Ok(permit)) {
                        drop(response);
                    }
                    *replacement = None;
                }
                Err(error) => {
                    if let Ok(capability) = state.attempt_capability(active.identity.replacement())
                    {
                        let _ = state.conclude(&capability, LogicalConclusion::Failed);
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
            conclude_replacement_failed(state, Some(active));
        }
        Err(error) => {
            drop(activated);
            pending_activation.lifetime.settle();
            let _ = pending_activation.reply.send(Err(error.into()));
        }
    }
}

fn conclude_failed(state: &mut LogicalExecutionState) {
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

fn start_schema_delivery(
    state: &mut LogicalExecutionState,
    runtime: &mut ResultRuntime,
    activation: AttemptActivationIdentity,
) -> Result<(), LogicalExecutionActorError> {
    if state.schema_emitted() || matches!(runtime.in_flight, Some(InFlightResult::Schema { .. })) {
        return Ok(());
    }
    if runtime.in_flight.is_some() {
        return Err(LogicalExecutionActorError::ResultDeliveryFailed);
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
    transport: Option<QueryResultTransport>,
) -> Result<ResultQueuePermit, QueryExecutionError> {
    transport
        .expect("result capacity wait is gated by one pending delivery")
        .reserve_owned()
        .await
}

async fn wait_for_result_receipt(runtime: Option<&mut ResultRuntime>) -> ResultReceiptOutcome {
    let runtime = runtime.expect("result receipt wait is gated by a result runtime");
    if let Some(in_flight) = runtime.in_flight.as_mut() {
        return ResultReceiptOutcome::Delivery(match in_flight {
            InFlightResult::Schema { receipt, .. }
            | InFlightResult::Batch { receipt, .. }
            | InFlightResult::End { receipt, .. } => receipt.await.map_err(|_| ()),
        });
    }
    let receipt = runtime
        .schema_receipt
        .as_mut()
        .expect("result receipt wait requires a schema or delivery receipt");
    ResultReceiptOutcome::Schema(receipt.await.map_err(|_| ()))
}

async fn wait_for_result_consumer_closed(transport: Option<QueryResultTransport>) {
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
        runtime,
        observation.activation,
        observation.root,
        observation.status,
        observation.terminal_failure,
        observation.reply,
    );
}

fn drive_root_success(
    state: &LogicalExecutionState,
    runtime: Option<&mut ResultRuntime>,
) -> Result<(), LogicalExecutionActorError> {
    let Some(runtime) = runtime else {
        return Ok(());
    };
    if runtime.finish_waiter.is_none() || runtime.pending.is_some() || runtime.in_flight.is_some() {
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
        return Err(LogicalExecutionActorError::ResultDeliveryFailed);
    }
    if sequence.get() > 0
        && state.delivered_result_through() != Some(sequence.get().saturating_sub(1))
    {
        return Err(LogicalExecutionActorError::ResultDeliveryFailed);
    }
    let finish = runtime
        .finish_waiter
        .as_ref()
        .ok_or(LogicalExecutionActorError::InvariantViolation)?;
    if finish.permit.identity() != gate.activation {
        return Err(LogicalExecutionActorError::ResultDeliveryFailed);
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
    runtime: Option<&mut ResultRuntime>,
    reserved: Result<ResultQueuePermit, QueryExecutionError>,
) {
    let Some(runtime) = runtime else {
        return;
    };
    let Some(pending) = runtime.pending.take() else {
        return;
    };
    let slot = match reserved {
        Ok(slot) => slot,
        Err(_) => {
            conclude_failed(state);
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
                        .send(Err(LogicalExecutionActorError::ResultDeliveryFailed));
                    return;
                }
            };
            let permit = match state.begin_result_delivery(
                &capability,
                pending.sequence.get(),
                ResultPacket::Data(()),
            ) {
                Ok(permit) => permit,
                Err(_) => {
                    drop(slot);
                    drop(pending.delivery);
                    conclude_failed(state);
                    reply_result_failure(state, pending.reply);
                    return;
                }
            };
            runtime
                .transport
                .enqueue(slot, ResultDelivery::Batch(pending.delivery));
            runtime.in_flight = Some(InFlightResult::Batch {
                permit,
                receipt: pending.receipt,
                reply: pending.reply,
            });
        }
        PendingResult::End(pending) => {
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
            let (delivery, receipt) =
                EndDelivery::success_eof(activation.execution(), pending.sequence);
            runtime
                .transport
                .enqueue(slot, ResultDelivery::End(delivery));
            let (_capability, lifetime, mailbox_liveness) = pending.permit.into_parts();
            drop(mailbox_liveness);
            runtime.in_flight = Some(InFlightResult::End {
                permit,
                receipt,
                lifetime,
                reply: pending.reply,
            });
        }
    }
}

fn apply_result_receipt(
    state: &mut LogicalExecutionState,
    replacement: Option<&ReplacementRuntime>,
    runtime: Option<&mut ResultRuntime>,
    outcome: ResultReceiptOutcome,
) {
    let Some(runtime) = runtime else {
        return;
    };
    let disposition = match outcome {
        ResultReceiptOutcome::Schema(disposition) => {
            runtime.schema_receipt.take();
            if matches!(disposition, Ok(ResultDeliveryDisposition::Completed)) {
                runtime.schema_writer_completed = true;
            } else {
                conclude_with(state, replacement, LogicalConclusion::Failed);
            }
            return;
        }
        ResultReceiptOutcome::Delivery(disposition) => disposition,
    };
    let Some(in_flight) = runtime.in_flight.take() else {
        return;
    };
    let completed = matches!(disposition, Ok(ResultDeliveryDisposition::Completed));
    match in_flight {
        InFlightResult::Schema { permit, .. } => {
            if completed {
                if state.complete_schema_delivery(permit).is_err() {
                    conclude_failed(state);
                }
            } else {
                let _ = state.fail_schema_delivery(permit);
                conclude_failed(state);
            }
        }
        InFlightResult::Batch { permit, reply, .. } => {
            if completed {
                match state.complete_result_delivery(permit) {
                    Ok(_) => {
                        let _ = reply.send(Ok(()));
                    }
                    Err(_) => {
                        conclude_failed(state);
                        reply_result_failure(state, reply);
                    }
                }
            } else {
                let _ = state.fail_result_delivery(permit);
                conclude_failed(state);
                reply_result_failure(state, reply);
            }
        }
        InFlightResult::End {
            permit,
            lifetime,
            reply,
            ..
        } => {
            if completed {
                match state.complete_success_end_of_stream(permit) {
                    Ok(_) => {
                        lifetime.settle();
                        let _ = reply.send(Ok(LogicalConclusion::Succeeded));
                    }
                    Err(_) => {
                        conclude_failed(state);
                        reply_fixed_handoff_conclusion(state, &lifetime, reply);
                    }
                }
            } else {
                let _ = state.fail_success_end_of_stream(permit);
                conclude_failed(state);
                reply_fixed_handoff_conclusion(state, &lifetime, reply);
            }
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
    }
}

fn fail_result_runtime(state: &mut LogicalExecutionState, runtime: Option<&mut ResultRuntime>) {
    let Some(runtime) = runtime else {
        return;
    };
    if let Some(sender) = runtime.failure_sender.take() {
        if let Some(error) = runtime.terminal_error.take() {
            sender.send_replace(Some(error));
        } else {
            match state.conclusion() {
                Some(LogicalConclusion::Succeeded) => drop(sender),
                Some(LogicalConclusion::Cancelled) => {
                    sender.send_replace(Some(QueryExecutionError::new(
                        crate::api::QueryExecutionErrorKind::Cancelled,
                        "logical execution was cancelled before success EOF",
                    )));
                }
                Some(LogicalConclusion::Failed) => {
                    sender.send_replace(Some(QueryExecutionError::new(
                        crate::api::QueryExecutionErrorKind::Failed,
                        "logical execution failed before success EOF",
                    )));
                }
                Some(LogicalConclusion::BusinessDecisionRequired) => {
                    sender.send_replace(Some(QueryExecutionError::new(
                        crate::api::QueryExecutionErrorKind::Failed,
                        "logical execution requires a business decision before success EOF",
                    )));
                }
                None => {}
            }
        }
    }
    runtime.root_success = None;
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
    let Some(in_flight) = runtime.in_flight.take() else {
        return;
    };
    match in_flight {
        InFlightResult::Schema { .. } => {}
        InFlightResult::Batch { reply, .. } => {
            reply_result_failure(state, reply);
        }
        InFlightResult::End {
            lifetime, reply, ..
        } => {
            reply_fixed_handoff_conclusion(state, &lifetime, reply);
        }
    }
}

fn conclude_for_work_cancellation(
    state: &mut LogicalExecutionState,
    replacement: Option<&ReplacementRuntime>,
    mut runtime: Option<&mut ResultRuntime>,
    reason: &CancellationReason,
) {
    if let Some(runtime) = runtime.as_deref_mut() {
        let (kind, message) = match reason {
            CancellationReason::DeadlineExceeded
            | CancellationReason::FrontendDrainDeadlineExceeded => (
                crate::api::QueryExecutionErrorKind::DeadlineExceeded,
                "logical execution deadline expired before success EOF",
            ),
            CancellationReason::Requested
            | CancellationReason::ExplicitKill { .. }
            | CancellationReason::ExplicitKillConnection { .. }
            | CancellationReason::ClientDisconnected
            | CancellationReason::ServerShutdown
            | CancellationReason::OwnerDropped => (
                crate::api::QueryExecutionErrorKind::Cancelled,
                "logical execution was cancelled before success EOF",
            ),
        };
        runtime.terminal_error = Some(QueryExecutionError::new(kind, message));
    }
    let conclusion = match reason {
        CancellationReason::DeadlineExceeded
        | CancellationReason::FrontendDrainDeadlineExceeded => LogicalConclusion::Failed,
        CancellationReason::Requested
        | CancellationReason::ExplicitKill { .. }
        | CancellationReason::ExplicitKillConnection { .. }
        | CancellationReason::ClientDisconnected
        | CancellationReason::ServerShutdown
        | CancellationReason::OwnerDropped => LogicalConclusion::Cancelled,
    };
    conclude_or_interrupt_success(state, replacement, runtime, conclusion);
}

fn conclude_or_interrupt_success(
    state: &mut LogicalExecutionState,
    replacement: Option<&ReplacementRuntime>,
    runtime: Option<&mut ResultRuntime>,
    conclusion: LogicalConclusion,
) {
    if matches!(state.phase(), ExecutionPhase::FinishingSuccess { .. }) {
        let Some(runtime) = runtime else {
            return;
        };
        let Some(InFlightResult::End {
            permit,
            lifetime,
            reply,
            ..
        }) = runtime.in_flight.take()
        else {
            return;
        };
        let _ = state.conclude_success_end_of_stream(permit, conclusion);
        reply_fixed_handoff_conclusion(state, &lifetime, reply);
        return;
    }
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

fn handle_command(
    state: &mut LogicalExecutionState,
    attempts: &mut BTreeMap<QueryExecutionId, AttemptLedgers>,
    establish_error: &mut Option<EstablishIssueError>,
    stand_down_error: &mut Option<ContextStandDownError>,
    replacement: &mut Option<ReplacementRuntime>,
    replacement_error: &mut Option<ReplacementQualificationFailure>,
    clock: &dyn LogicalExecutionClock,
    work: &WorkScope,
    work_cancellation: &CancellationView,
    replacement_reservation_valid_for: Option<Duration>,
    mut result_runtime: Option<&mut ResultRuntime>,
    root_terminal_receiver: &mut Option<mpsc::Receiver<RootTerminalObservation>>,
    command: ActorCommand,
) {
    match command {
        ActorCommand::TakeReplacementAdmissionEvidence { activation, reply } => {
            let result = (|| {
                if !matches!(
                    state.phase(),
                    ExecutionPhase::Instantiating { execution, .. }
                        if execution == activation.execution()
                ) {
                    return Err(LogicalExecutionActorError::WrongPhase);
                }
                let current = state.attempt_capability(activation.execution())?;
                if identity_of(&current) != activation {
                    return Err(LogicalExecutionActorError::StaleAuthority);
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
                    conclude_failed(state);
                }
            }
        }
        ActorCommand::Activate { readiness, reply } => {
            let (capability, lifetime, mailbox_liveness) = readiness.into_parts();
            match state.mark_running(&capability) {
                Ok(()) => {
                    let running = RunningAttemptPermit::new(capability, lifetime, mailbox_liveness);
                    if let Err(response) = reply.send(Ok(running)) {
                        drop(response);
                    }
                }
                Err(error) => {
                    lifetime.abandon();
                    let _ = reply.send(Err(error.into()));
                }
            }
        }
        ActorCommand::InitializationFailed { permit, reply } => {
            settle_terminal(state, permit.into_parts(), LogicalConclusion::Failed, reply);
        }
        ActorCommand::InitializationCancelled { permit, reply } => {
            settle_terminal(
                state,
                permit.into_parts(),
                LogicalConclusion::Cancelled,
                reply,
            );
        }
        ActorCommand::Completed { permit, reply } => {
            let execution = permit.identity().execution();
            let Some(attempt) = attempts.get_mut(&execution) else {
                let _ = reply.send(Err(LogicalExecutionActorError::WrongExecution));
                return;
            };
            match attempt.establish.ensure_success_ready() {
                Ok(()) => {
                    attempt.establish.revoke_issue_authority();
                    settle_terminal(
                        state,
                        permit.into_parts(),
                        LogicalConclusion::Succeeded,
                        reply,
                    );
                }
                Err(error) => {
                    establish_error.get_or_insert(error);
                    attempt.establish_error.get_or_insert(error);
                    attempt.establish.revoke_issue_authority();
                    settle_terminal_with_error(state, permit.into_parts(), error.into(), reply);
                }
            }
        }
        ActorCommand::Failed {
            permit,
            error,
            reply,
        } => {
            if let Some(attempt) = attempts.get_mut(&permit.identity().execution()) {
                attempt.establish.revoke_issue_authority();
            }
            if let (Some(runtime), Some(error)) = (result_runtime.as_deref_mut(), error) {
                runtime.terminal_error = Some(error);
            }
            settle_terminal(state, permit.into_parts(), LogicalConclusion::Failed, reply);
        }
        ActorCommand::DeliverResultBatch {
            activation,
            sequence,
            batch,
            credit,
            reply,
        } => {
            if let Some(conclusion) = state.conclusion() {
                drop(batch);
                drop(credit);
                let _ = reply.send(Err(LogicalExecutionActorError::ExecutionConcluded(
                    conclusion,
                )));
                return;
            }
            let Some(runtime) = result_runtime else {
                drop(batch);
                drop(credit);
                let _ = reply.send(Err(LogicalExecutionActorError::WrongPhase));
                return;
            };
            if runtime.attempt_decision_pending == Some(activation) {
                drop(batch);
                drop(credit);
                let _ = reply.send(Err(LogicalExecutionActorError::RootAttemptTerminal));
                return;
            }
            if runtime
                .root_success
                .as_ref()
                .is_some_and(|gate| gate.activation == activation && gate.final_eos_ack.is_some())
            {
                drop(batch);
                drop(credit);
                conclude_failed(state);
                reply_result_failure(state, reply);
                return;
            }
            if verify_running_activation(state, activation).is_err()
                || runtime.pending.is_some()
                || matches!(
                    runtime.in_flight,
                    Some(InFlightResult::Batch { .. } | InFlightResult::End { .. })
                )
            {
                drop(batch);
                drop(credit);
                let _ = reply.send(Err(LogicalExecutionActorError::ResultDeliveryFailed));
                return;
            }
            if sequence.next().is_none() || !runtime.schema.accepts(batch.batch()) {
                drop(batch);
                drop(credit);
                conclude_failed(state);
                reply_result_failure(state, reply);
                return;
            }
            let delivery = BatchDelivery::try_new(activation.execution(), sequence, batch, credit);
            let Ok((delivery, receipt)) = delivery else {
                conclude_failed(state);
                reply_result_failure(state, reply);
                return;
            };
            if start_schema_delivery(state, runtime, activation).is_err() {
                drop(delivery);
                conclude_failed(state);
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
            match runtime.root_success.as_ref() {
                Some(gate) if gate.activation == activation && gate.root == root => {
                    let _ = reply.send(Err(LogicalExecutionActorError::WrongPhase));
                }
                Some(gate) if gate.activation == activation => {
                    conclude_failed(state);
                    reply_result_failure(state, reply);
                }
                _ => {
                    runtime.root_success = Some(RootSuccessGate {
                        activation,
                        root,
                        status: None,
                        terminal_failure: RootTerminalFailure::Unspecified,
                        final_eos_ack: None,
                    });
                    *root_terminal_receiver = Some(terminal_receiver);
                    let _ = reply.send(Ok(()));
                }
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
            let expected = ResultPacketSequence::new(state.accepted_result_packets());
            if sequence != expected
                || (sequence.get() > 0
                    && state.delivered_result_through() != Some(sequence.get().saturating_sub(1)))
            {
                fail_result_observation(state, runtime);
                reply_result_failure(state, reply);
                return;
            }
            let Some(gate) = runtime.root_success.as_mut() else {
                let _ = reply.send(Err(LogicalExecutionActorError::WrongPhase));
                return;
            };
            if gate.activation != activation || gate.root != expected_root || root != expected_root
            {
                conclude_failed(state);
                reply_result_failure(state, reply);
                return;
            }
            match gate.final_eos_ack {
                None => {
                    gate.final_eos_ack = Some(sequence);
                    let _ = reply.send(Ok(()));
                }
                Some(held) if held == sequence => {
                    let _ = reply.send(Ok(()));
                }
                Some(_) => {
                    fail_result_observation(state, runtime);
                    reply_result_failure(state, reply);
                }
            }
        }
        ActorCommand::FinishResultStream { permit, reply } => {
            let activation = permit.identity();
            let Some(runtime) = result_runtime else {
                conclude_consumed_handoff_as_failed(state, permit, reply);
                return;
            };
            let success_ready = attempts
                .get_mut(&activation.execution())
                .ok_or(LogicalExecutionActorError::WrongExecution)
                .and_then(|attempt| attempt.establish.ensure_success_ready().map_err(Into::into));
            if verify_running_activation(state, activation).is_err()
                || success_ready.is_err()
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
                    conclude_failed(state);
                }
            }
            reply_consumed_handoff_actual(state, permit, reply, false);
        }
        ActorCommand::BeginReplacement {
            permit,
            failure,
            terminal_error,
            replacement: replacement_execution,
            replacement_contexts,
            reply,
        } => {
            let activation = permit.identity();
            let (capability, lifetime, mailbox_liveness) = permit.into_parts();
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
                conclude_for_work_cancellation(state, replacement.as_ref(), None, &reason);
                lifetime.settle();
                let _ = reply.send(Err(LogicalExecutionActorError::WrongPhase));
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
                    let _ = state.conclude(&capability, LogicalConclusion::Failed);
                    lifetime.settle();
                    let _ = reply.send(Err(error));
                    return;
                }
            };
            let token =
                match state.begin_replacement(&capability, replacement_execution, failure, false) {
                    Ok(token) => token,
                    Err(error) => {
                        if let (Some(runtime), Some(terminal_error)) =
                            (result_runtime, terminal_error)
                        {
                            runtime.terminal_error = Some(terminal_error);
                        }
                        let _ = state.conclude(&capability, LogicalConclusion::Failed);
                        lifetime.settle();
                        let _ = reply.send(Err(error.into()));
                        return;
                    }
                };
            if let Some(runtime) = result_runtime.as_deref_mut() {
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
            let obligation = match work.register_obligation(
                retired_obligation_key(identity),
                ObligationKind::RetiredAttempt,
            ) {
                Ok(obligation) => obligation,
                Err(_) => {
                    let _ = state.conclude_replacement(&token, LogicalConclusion::Failed);
                    lifetime.settle();
                    let _ = reply.send(Err(LogicalExecutionActorError::InvariantViolation));
                    return;
                }
            };
            let failed = attempts
                .get_mut(&activation.execution())
                .expect("failed attempt was validated before replacement transition");
            failed.retired_obligation = Some(obligation);
            let issued_at = clock.now();
            let conservative_expiry = issued_at.saturating_add(reservation_valid_for);
            let (effect_cancellation, _) = watch::channel(false);
            let request = Arc::new(ReplacementQualificationRequest::new(
                identity,
                failed_contexts,
                contexts,
                issued_at,
                conservative_expiry,
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
                    attempt.establish_error.get_or_insert(error);
                    attempt.establish.revoke_issue_authority();
                }
                if matches!(state.phase(), ExecutionPhase::Running { execution, .. } if execution == activation.execution())
                {
                    conclude_failed(state);
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
    Ok(AttemptLedgers {
        activation,
        establish,
        establish_error: None,
        stand_down,
        stand_down_error: None,
        pending_aborts: BTreeMap::new(),
        abort_backpressured: false,
        retired_obligation: None,
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

async fn wait_for_abort_retry(
    clock: &dyn LogicalExecutionClock,
    retry_at: Option<MonotonicInstant>,
) {
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

async fn wait_for_replacement_expiry(
    clock: &dyn LogicalExecutionClock,
    expiry: Option<MonotonicInstant>,
) {
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
            && attempt.stand_down.residual_resource_settled()
            && attempt.retired_obligation.is_none()
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
            fail_result_observation(state, runtime);
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
            runtime.terminal_error = Some(error);
        }
        if state.output_visible() {
            fail_result_observation(state, runtime);
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
        | StatusObservation::TerminalOverwrite => {
            fail_result_observation(state, runtime);
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
        fail_result_observation(state, runtime);
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
            runtime.terminal_error = Some(error);
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
            fail_result_observation(state, runtime);
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

fn fail_result_observation(state: &mut LogicalExecutionState, runtime: &mut ResultRuntime) {
    if !matches!(state.phase(), ExecutionPhase::FinishingSuccess { .. }) {
        conclude_failed(state);
        return;
    }
    let Some(InFlightResult::End {
        permit,
        lifetime,
        reply,
        ..
    }) = runtime.in_flight.take()
    else {
        return;
    };
    let _ = state.fail_success_end_of_stream(permit);
    reply_fixed_handoff_conclusion(state, &lifetime, reply);
}

fn settle_terminal(
    state: &mut LogicalExecutionState,
    (capability, lifetime, _mailbox_liveness): (
        AttemptCapability,
        Arc<PermitLifetime>,
        mpsc::Sender<ActorCommand>,
    ),
    conclusion: LogicalConclusion,
    reply: ActorReply<LogicalConclusion>,
) {
    let result = state
        .conclude(&capability, conclusion)
        .map(|()| conclusion)
        .map_err(LogicalExecutionActorError::from);
    if result.is_ok() {
        lifetime.settle();
    } else {
        lifetime.abandon();
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
    match state.conclude(&capability, LogicalConclusion::Failed) {
        Ok(()) => {
            lifetime.settle();
            let _ = reply.send(Err(error));
        }
        Err(state_error) => {
            lifetime.abandon();
            let _ = reply.send(Err(state_error.into()));
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
            conclude_failed(state);
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

fn reply_fixed_handoff_conclusion(
    state: &LogicalExecutionState,
    lifetime: &Arc<PermitLifetime>,
    reply: ActorReply<LogicalConclusion>,
) {
    let Some(conclusion) = state.conclusion() else {
        lifetime.abandon();
        let _ = reply.send(Err(LogicalExecutionActorError::InvariantViolation));
        return;
    };
    lifetime.settle();
    let _ = reply.send(Err(LogicalExecutionActorError::ExecutionConcluded(
        conclusion,
    )));
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
    use arrow::array::Int64Array;
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
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
        LocalResourceAuthority, ResourceConfig, WorkClass, WorkRequest, WorkloadConfig,
        WorkloadControl,
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

    impl LogicalExecutionClock for ManualActorClock {
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
        let content = |offset| {
            Arc::new(FakeContent(ContentFingerprint::from_bytes(
                [tag + offset; 16],
            ))) as Arc<dyn CodecOwnedContent>
        };
        Arc::new(novarocks_execution_contract::EstablishQueryContext::new(
            TaskOperationId::new_v7(),
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
            DataType::Int64,
            false,
            None,
        )])
    }

    fn result_batch() -> DecodedResultBatch {
        DecodedResultBatch::try_new(
            RecordBatch::try_new(
                Arc::new(Schema::new(vec![Field::new(
                    "value",
                    DataType::Int64,
                    false,
                )])),
                vec![Arc::new(Int64Array::from(vec![7_i64, 11]))],
            )
            .unwrap(),
        )
        .unwrap()
    }

    fn result_credit(
        batch: &DecodedResultBatch,
    ) -> (
        WorkloadControl,
        WorkOwner,
        LocalResourceAuthority,
        ResultCredit,
    ) {
        let control = WorkloadControl::try_new(
            WorkloadConfig::default(),
            ResourceConfig {
                total_bytes: 1024 * 1024,
                control_bytes: 1024,
                per_scope_bytes: 1024 * 1024 - 1024,
            },
        )
        .unwrap();
        control.mark_ready().unwrap();
        let root = control
            .try_begin_root(WorkRequest::new(WorkClass::Query))
            .unwrap();
        let bytes = batch.governance_charge_bytes();
        let authority = control.resources();
        let credit = authority
            .reserve_result_credit(&root.owner.scope(), bytes)
            .unwrap()
            .begin_fetch()
            .unwrap()
            .retain_raw(bytes)
            .unwrap()
            .reserve_decode(&authority, bytes)
            .unwrap()
            .queue_decoded(bytes)
            .unwrap();
        (control, root.owner, authority, credit)
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
        )
        .unwrap();
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
        .with_result_stream(result_schema(), NonZeroUsize::new(1).unwrap());
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
        .with_result_stream(result_schema(), NonZeroUsize::new(1).unwrap());
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
        .with_result_stream(result_schema(), NonZeroUsize::new(1).unwrap());
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
    async fn actor_holds_batch_ack_and_logical_success_until_writer_receipts() {
        let runtime = Handle::current();
        let first = execution(103);
        let config = recovery_config(first, Arc::new(DelayedQualificationPort::default()), 2)
            .with_result_stream(result_schema(), NonZeroUsize::new(1).unwrap());
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
        let batch = result_batch();
        let bytes = batch.governance_charge_bytes();
        let (control, credit_owner, authority, credit) = result_credit(&batch);
        let delivery_running = Arc::clone(&running);
        let delivery_task = tokio::spawn(async move {
            delivery_running
                .deliver_result_batch(ResultPacketSequence::new(0), batch, credit)
                .await
        });

        let ResultDelivery::Batch(delivery) = stream.next().await.unwrap().unwrap() else {
            panic!("actor must enqueue one batch");
        };
        assert!(!delivery_task.is_finished());
        assert_eq!(authority.snapshot().result_credit.held_bytes(), bytes);
        delivery
            .reserve_protocol(&authority, bytes)
            .unwrap()
            .begin_protocol_write(bytes)
            .unwrap()
            .complete()
            .unwrap();
        delivery_task.await.unwrap().unwrap();
        assert_eq!(authority.snapshot().result_credit.held_bytes(), 0);

        let running = Arc::try_unwrap(running).expect("batch sender released its permit");
        observer
            .observe_status(root_status(root, 1, TaskState::Finished))
            .await
            .unwrap();
        observer
            .observe_final_worker_eos_ack(root, ResultPacketSequence::new(1))
            .await
            .unwrap();
        let finish_task = tokio::spawn(async move { running.finish_result_stream().await });
        let ResultDelivery::End(end) = stream.next().await.unwrap().unwrap() else {
            panic!("actor must enqueue success EOF");
        };
        assert!(!finish_task.is_finished());
        assert_eq!(actor.snapshot().await.unwrap().conclusion, None);
        end.complete();
        assert_eq!(
            finish_task.await.unwrap().unwrap(),
            LogicalConclusion::Succeeded
        );
        wait_for_conclusion(&actor, LogicalConclusion::Succeeded).await;
        credit_owner.complete();
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
            .with_result_stream(result_schema(), NonZeroUsize::new(1).unwrap());
        let (owner, initial, output) = spawn_logical_execution_actor(&runtime, config)
            .unwrap()
            .into_parts();
        let ExecutionOutput::Rows(mut stream) = output.into_output() else {
            panic!("result execution must expose its row stream");
        };
        stream.begin_schema().unwrap().complete();
        let actor = owner.actor().clone();
        let running = Arc::new(actor.activate(initial.ready()).await.unwrap());
        let batch = result_batch();
        let (control, credit_owner, authority, credit) = result_credit(&batch);
        let delivery_running = Arc::clone(&running);
        let delivery_task = tokio::spawn(async move {
            delivery_running
                .deliver_result_batch(ResultPacketSequence::new(0), batch, credit)
                .await
        });
        let ResultDelivery::Batch(delivery) = stream.next().await.unwrap().unwrap() else {
            panic!("actor must enqueue one batch");
        };

        delivery_task.abort();
        assert!(delivery_task.await.unwrap_err().is_cancelled());
        let bytes = delivery.decoded_bytes();
        delivery
            .reserve_protocol(&authority, bytes)
            .unwrap()
            .begin_protocol_write(bytes)
            .unwrap()
            .complete()
            .unwrap();
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
        assert_eq!(authority.snapshot().result_credit.held_bytes(), 0);

        let running = Arc::try_unwrap(running).expect("cancelled waiter released its permit");
        assert_eq!(
            actor.fail_attempt(running).await.unwrap(),
            LogicalConclusion::Failed
        );
        credit_owner.complete();
        drop(control);
        drop(stream);
        drop(actor);
        drop(owner);
    }

    #[tokio::test]
    async fn logical_failure_wakes_an_idle_result_stream_without_queue_capacity() {
        let runtime = Handle::current();
        let first = execution(110);
        let config = recovery_config(first, Arc::new(DelayedQualificationPort::default()), 2)
            .with_result_stream(result_schema(), NonZeroUsize::new(1).unwrap());
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
        .with_result_stream(result_schema(), NonZeroUsize::new(1).unwrap());
        let (owner, initial, output) = spawn_logical_execution_actor(&runtime, config)
            .unwrap()
            .into_parts();
        let ExecutionOutput::Rows(mut stream) = output.into_output() else {
            panic!("result execution must expose its row stream");
        };
        stream.begin_schema().unwrap().complete();
        let actor = owner.actor().clone();
        let running = Arc::new(actor.activate(initial.ready()).await.unwrap());
        let batch = result_batch();
        let (control, credit_owner, authority, credit) = result_credit(&batch);
        let delivery_running = Arc::clone(&running);
        let delivery_task = tokio::spawn(async move {
            delivery_running
                .deliver_result_batch(ResultPacketSequence::new(0), batch, credit)
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
        assert_eq!(authority.snapshot().result_credit.held_bytes(), 0);
        drop(stream);

        drop(running);
        credit_owner.complete();
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
        .with_result_stream(result_schema(), NonZeroUsize::new(1).unwrap());
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
        let batch = result_batch();
        let (control, credit_owner, authority, credit) = result_credit(&batch);
        let delivery_running = Arc::clone(&running);
        let delivery_task = tokio::spawn(async move {
            delivery_running
                .deliver_result_batch(ResultPacketSequence::new(0), batch, credit)
                .await
        });
        let ResultDelivery::Batch(delivery) = stream.next().await.unwrap().unwrap() else {
            panic!("actor must enqueue one batch");
        };

        parent.cancel(CancellationReason::Requested);
        let error = tokio::time::timeout(Duration::from_secs(1), failure.wait())
            .await
            .expect("the in-flight protocol writer must observe cancellation");
        assert_eq!(error.kind(), QueryExecutionErrorKind::Cancelled);
        delivery.fail(error);
        assert_eq!(
            delivery_task.await.unwrap().unwrap_err(),
            LogicalExecutionActorError::ExecutionConcluded(LogicalConclusion::Cancelled)
        );
        wait_for_conclusion(&actor, LogicalConclusion::Cancelled).await;
        assert_eq!(authority.snapshot().result_credit.held_bytes(), 0);

        drop(running);
        credit_owner.complete();
        drop(control);
        drop(stream);
        drop(actor);
        drop(owner);
        parent.complete();
    }

    #[tokio::test]
    async fn writer_failure_fails_delivery_and_releases_result_credit() {
        let runtime = Handle::current();
        let first = execution(104);
        let config = recovery_config(first, Arc::new(DelayedQualificationPort::default()), 2)
            .with_result_stream(result_schema(), NonZeroUsize::new(1).unwrap());
        let (owner, initial, output) = spawn_logical_execution_actor(&runtime, config)
            .unwrap()
            .into_parts();
        let ExecutionOutput::Rows(mut stream) = output.into_output() else {
            panic!("result execution must expose its row stream");
        };
        stream.begin_schema().unwrap().complete();
        let actor = owner.actor().clone();
        let running = Arc::new(actor.activate(initial.ready()).await.unwrap());
        let batch = result_batch();
        let (control, credit_owner, authority, credit) = result_credit(&batch);
        let delivery_running = Arc::clone(&running);
        let delivery_task = tokio::spawn(async move {
            delivery_running
                .deliver_result_batch(ResultPacketSequence::new(0), batch, credit)
                .await
        });
        let ResultDelivery::Batch(delivery) = stream.next().await.unwrap().unwrap() else {
            panic!("actor must enqueue one batch");
        };
        delivery.fail(QueryExecutionError::new(
            QueryExecutionErrorKind::Failed,
            "protocol writer failed",
        ));
        assert_eq!(
            delivery_task.await.unwrap().unwrap_err(),
            LogicalExecutionActorError::ExecutionConcluded(LogicalConclusion::Failed)
        );
        wait_for_conclusion(&actor, LogicalConclusion::Failed).await;
        assert_eq!(authority.snapshot().result_credit.held_bytes(), 0);
        drop(running);
        credit_owner.complete();
        drop(control);
        drop(stream);
        drop(actor);
        drop(owner);
    }

    #[tokio::test]
    async fn dropped_consumer_fails_closed_without_waiting_for_a_batch() {
        let runtime = Handle::current();
        let first = execution(105);
        let config = recovery_config(first, Arc::new(DelayedQualificationPort::default()), 2)
            .with_result_stream(result_schema(), NonZeroUsize::new(1).unwrap());
        let (owner, initial, output) = spawn_logical_execution_actor(&runtime, config)
            .unwrap()
            .into_parts();
        let ExecutionOutput::Rows(stream) = output.into_output() else {
            panic!("result execution must expose its row stream");
        };
        drop(stream);
        let actor = owner.actor().clone();
        wait_for_conclusion(&actor, LogicalConclusion::Failed).await;
        assert!(actor.activate(initial.ready()).await.is_err());
        drop(actor);
        drop(owner);
    }

    #[tokio::test]
    async fn dropped_consumer_after_schema_completion_still_fails_closed() {
        let runtime = Handle::current();
        let first = execution(108);
        let config = recovery_config(first, Arc::new(DelayedQualificationPort::default()), 2)
            .with_result_stream(result_schema(), NonZeroUsize::new(1).unwrap());
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
            .with_result_stream(result_schema(), NonZeroUsize::new(1).unwrap());
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

        let foreign_batch = result_batch();
        let (foreign_control, foreign_owner, foreign_authority, foreign_credit) =
            result_credit(&foreign_batch);
        let foreign = AttemptActivationIdentity::from_parts(
            identity.actor().get(),
            replacement(first, 2),
            identity.generation(),
        );
        assert_eq!(
            request(&actor.sender, |reply| ActorCommand::DeliverResultBatch {
                activation: foreign,
                sequence: ResultPacketSequence::new(0),
                batch: foreign_batch,
                credit: foreign_credit,
                reply,
            })
            .await
            .unwrap_err(),
            LogicalExecutionActorError::ResultDeliveryFailed
        );
        assert_eq!(foreign_authority.snapshot().result_credit.held_bytes(), 0);
        assert_eq!(actor.snapshot().await.unwrap().conclusion, None);
        foreign_owner.complete();
        drop(foreign_control);

        let gap_batch = result_batch();
        let (gap_control, gap_owner, gap_authority, gap_credit) = result_credit(&gap_batch);
        assert_eq!(
            running
                .deliver_result_batch(ResultPacketSequence::new(1), gap_batch, gap_credit)
                .await
                .unwrap_err(),
            LogicalExecutionActorError::ExecutionConcluded(LogicalConclusion::Failed)
        );
        wait_for_conclusion(&actor, LogicalConclusion::Failed).await;
        assert_eq!(gap_authority.snapshot().result_credit.held_bytes(), 0);
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
            .with_result_stream(result_schema(), NonZeroUsize::new(1).unwrap());
        let (owner, initial, output) = spawn_logical_execution_actor(&runtime, config)
            .unwrap()
            .into_parts();
        let ExecutionOutput::Rows(mut stream) = output.into_output() else {
            panic!("result execution must expose its row stream");
        };
        stream.begin_schema().unwrap().complete();
        let actor = owner.actor().clone();
        let running = actor.activate(initial.ready()).await.unwrap();
        let batch = result_batch();
        let (control, credit_owner, authority, credit) = result_credit(&batch);
        assert_eq!(
            running
                .deliver_result_batch(ResultPacketSequence::new(u64::MAX), batch, credit)
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
        assert_eq!(authority.snapshot().result_credit.held_bytes(), 0);
        drop(running);
        credit_owner.complete();
        drop(control);
        drop(stream);
        drop(actor);
        drop(owner);

        let second = execution(112);
        let config = recovery_config(second, Arc::new(DelayedQualificationPort::default()), 2)
            .with_result_stream(result_schema(), NonZeroUsize::new(1).unwrap());
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
                .observe_final_worker_eos_ack(foreign, ResultPacketSequence::new(0))
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
            .with_result_stream(result_schema(), NonZeroUsize::new(1).unwrap());
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
            .observe_final_worker_eos_ack(root, ResultPacketSequence::new(0))
            .await
            .unwrap();
        let finish_task = tokio::spawn(async move { successor.finish_result_stream().await });
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
        assert!(!finish_task.is_finished());
        end.complete();
        assert_eq!(
            finish_task.await.unwrap().unwrap(),
            LogicalConclusion::Succeeded
        );
        assert!(stream.begin_schema().is_none());
        wait_for_conclusion(&actor, LogicalConclusion::Succeeded).await;
        drop(stream);
        drop(actor);
        drop(owner);
    }

    #[tokio::test]
    async fn eof_waits_for_pending_batch_protocol_completion() {
        let runtime = Handle::current();
        let first = execution(113);
        let config = recovery_config(first, Arc::new(DelayedQualificationPort::default()), 2)
            .with_result_stream(result_schema(), NonZeroUsize::new(1).unwrap());
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
        let batch = result_batch();
        let bytes = batch.governance_charge_bytes();
        let (control, credit_owner, authority, credit) = result_credit(&batch);
        let (reply, response) = oneshot::channel();
        actor
            .sender
            .send(ActorCommand::DeliverResultBatch {
                activation: running.identity(),
                sequence: ResultPacketSequence::new(0),
                batch,
                credit,
                reply,
            })
            .await
            .unwrap();
        let ResultDelivery::Batch(delivery) = stream.next().await.unwrap().unwrap() else {
            panic!("actor must enqueue the pending batch");
        };
        observer
            .observe_status(root_status(root, 1, TaskState::Finished))
            .await
            .unwrap();
        let finish_task = tokio::spawn(async move { running.finish_result_stream().await });
        actor.snapshot().await.unwrap();
        assert!(!finish_task.is_finished());

        delivery
            .reserve_protocol(&authority, bytes)
            .unwrap()
            .begin_protocol_write(bytes)
            .unwrap()
            .complete()
            .unwrap();
        response.await.unwrap().unwrap();
        observer
            .observe_final_worker_eos_ack(root, ResultPacketSequence::new(1))
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
        credit_owner.complete();
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
            .with_result_stream(result_schema(), NonZeroUsize::new(1).unwrap());
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
        let batch = result_batch();
        let (control, credit_owner, authority, credit) = result_credit(&batch);
        assert_eq!(
            running
                .deliver_result_batch(ResultPacketSequence::new(0), batch, credit)
                .await
                .unwrap_err(),
            LogicalExecutionActorError::RootAttemptTerminal
        );
        assert!(!actor.snapshot().await.unwrap().output_visible);
        assert_eq!(authority.snapshot().result_credit.held_bytes(), 0);
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
        credit_owner.complete();
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
            .with_result_stream(result_schema(), NonZeroUsize::new(1).unwrap());
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
            .observe_final_worker_eos_ack(root, ResultPacketSequence::new(0))
            .await
            .unwrap();
        let finish = tokio::spawn(async move { running.finish_result_stream().await });
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
            .with_result_stream(result_schema(), NonZeroUsize::new(1).unwrap());
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
                .observe_final_worker_eos_ack(root, ResultPacketSequence::new(1))
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
            .with_result_stream(result_schema(), NonZeroUsize::new(1).unwrap());
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
            .observe_final_worker_eos_ack(root, ResultPacketSequence::new(0))
            .await
            .unwrap();
        let batch = result_batch();
        let (control, credit_owner, authority, credit) = result_credit(&batch);

        assert_eq!(
            running
                .deliver_result_batch(ResultPacketSequence::new(0), batch, credit)
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
        assert_eq!(authority.snapshot().result_credit.held_bytes(), 0);
        credit_owner.complete();
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
            .with_result_stream(result_schema(), NonZeroUsize::new(1).unwrap());
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
            .observe_final_worker_eos_ack(root, ResultPacketSequence::new(0))
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
    async fn root_failure_command_beats_ready_eof_receipt_in_the_same_tick() {
        let runtime = Handle::current();
        let first = execution(117);
        let config = recovery_config(first, Arc::new(DelayedQualificationPort::default()), 2)
            .with_result_stream(result_schema(), NonZeroUsize::new(1).unwrap());
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
            .observe_final_worker_eos_ack(root, ResultPacketSequence::new(0))
            .await
            .unwrap();
        let finish_task = tokio::spawn(async move { running.finish_result_stream().await });
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
            LogicalExecutionActorError::ExecutionConcluded(LogicalConclusion::Failed)
        );
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
    async fn eof_writer_failure_fails_the_logical_execution() {
        let runtime = Handle::current();
        let first = execution(118);
        let config = recovery_config(first, Arc::new(DelayedQualificationPort::default()), 2)
            .with_result_stream(result_schema(), NonZeroUsize::new(1).unwrap());
        let (owner, initial, output) = spawn_logical_execution_actor(&runtime, config)
            .unwrap()
            .into_parts();
        let ExecutionOutput::Rows(mut stream) = output.into_output() else {
            panic!("result execution must expose its row stream");
        };
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
            .observe_final_worker_eos_ack(root, ResultPacketSequence::new(0))
            .await
            .unwrap();
        let finish_task = tokio::spawn(async move { running.finish_result_stream().await });
        let ResultDelivery::End(end) = stream.next().await.unwrap().unwrap() else {
            panic!("actor must enqueue success EOF");
        };
        end.fail(QueryExecutionError::new(
            QueryExecutionErrorKind::Failed,
            "protocol writer failed to encode EOF",
        ));

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
    async fn cancellation_before_eof_acceptance_consumes_provisional_success() {
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
        .with_result_stream(result_schema(), NonZeroUsize::new(1).unwrap());
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
            .observe_final_worker_eos_ack(root, ResultPacketSequence::new(0))
            .await
            .unwrap();
        let finish_task = tokio::spawn(async move { running.finish_result_stream().await });
        let ResultDelivery::End(end) = stream.next().await.unwrap().unwrap() else {
            panic!("actor must enqueue provisional success EOF");
        };

        parent.cancel(CancellationReason::Requested);
        assert!(matches!(
            finish_task.await.unwrap().unwrap_err(),
            RunningAttemptHandoffError::ExecutionConcluded(LogicalConclusion::Cancelled)
        ));
        wait_for_conclusion(&actor, LogicalConclusion::Cancelled).await;
        drop(end);
        drop(observer);
        drop(stream);
        drop(actor);
        drop(owner);
        drop(parent);
    }

    #[tokio::test]
    async fn losing_the_root_observer_fails_a_pending_finish() {
        let runtime = Handle::current();
        let first = execution(120);
        let config = recovery_config(first, Arc::new(DelayedQualificationPort::default()), 2)
            .with_result_stream(result_schema(), NonZeroUsize::new(1).unwrap());
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
        let finish_task = tokio::spawn(async move { running.finish_result_stream().await });
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
            .with_result_stream(result_schema(), NonZeroUsize::new(1).unwrap());
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
            .observe_final_worker_eos_ack(root, ResultPacketSequence::new(0))
            .await
            .unwrap();
        let finish_task = tokio::spawn(async move { running.finish_result_stream().await });
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
                stand_down,
                stand_down_error: None,
                pending_aborts: BTreeMap::new(),
                abort_backpressured: false,
                retired_obligation: None,
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
}
