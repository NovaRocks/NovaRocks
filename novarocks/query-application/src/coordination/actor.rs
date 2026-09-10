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
use std::num::NonZeroUsize;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Instant;

use novarocks_execution_contract::{
    AcquireQueryContextAdmissionTicket, EstablishQueryContext, QueryContextRef,
};
use novarocks_types::NativeCompatibilityId;
use novarocks_types::identity::QueryExecutionId;
use tokio::runtime::Handle;
use tokio::sync::{mpsc, oneshot, watch};
use tokio::task::JoinHandle;

use super::actor_state::{ActorStateError, AttemptCapability, LogicalExecutionState};
use super::{
    AbortQueryContextEffectPort, AbortQueryContextIssuePermit, AbortQueryContextIssueSubmit,
    AdmissionIssueDisposition, AdmissionIssueReceipt, AdmissionIssueSettlement,
    ContextStandDownCause, ContextStandDownError, ContextStandDownLedger, ContextStandDownSnapshot,
    EstablishIssueError, EstablishIssueLedger, EstablishIssuePermit, EstablishIssueSnapshot,
    ExecutionEffect, ExecutionPhase, LogicalConclusion, LogicalOutputMode, MonotonicInstant,
    RecoveryMode, RegistryContextConvergence,
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

impl Drop for RunningAttemptPermit {
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

/// Honest construction inputs for the currently connected actor slice.
///
/// T08 first connects a single, completion-only attempt. Recovery and result
/// delivery remain reducer capabilities until their actor-owned effect gates
/// are connected; callers cannot select those modes prematurely.
#[derive(Clone, Debug)]
pub struct LogicalExecutionActorConfig {
    initial_execution: QueryExecutionId,
    effect: ExecutionEffect,
    mailbox_capacity: NonZeroUsize,
    required_establish_contexts: Vec<QueryContextRef>,
    max_admission_issues_per_context: NonZeroUsize,
    max_establish_authorizations_per_context: NonZeroUsize,
    abort_effect_port: Option<Arc<dyn AbortQueryContextEffectPort>>,
    max_abort_authorizations_per_context: NonZeroUsize,
    clock: Arc<dyn LogicalExecutionClock>,
}

impl LogicalExecutionActorConfig {
    pub fn single_attempt_completion(
        initial_execution: QueryExecutionId,
        effect: ExecutionEffect,
        mailbox_capacity: NonZeroUsize,
        required_establish_contexts: Vec<QueryContextRef>,
        max_admission_issues_per_context: NonZeroUsize,
        max_establish_authorizations_per_context: NonZeroUsize,
    ) -> Self {
        Self {
            initial_execution,
            effect,
            mailbox_capacity,
            required_establish_contexts,
            max_admission_issues_per_context,
            max_establish_authorizations_per_context,
            abort_effect_port: None,
            max_abort_authorizations_per_context: max_establish_authorizations_per_context,
            clock: Arc::new(ProcessLogicalExecutionClock::new()),
        }
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
    InvariantViolation,
    Establish(EstablishIssueError),
    StandDown(ContextStandDownError),
}

impl fmt::Display for LogicalExecutionActorError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        if let Self::Establish(error) = self {
            return write!(formatter, "Establish issue protocol failed: {error}");
        }
        if let Self::StandDown(error) = self {
            return write!(formatter, "query-context stand-down failed: {error}");
        }
        formatter.write_str(match self {
            Self::MailboxClosed => "logical execution actor mailbox is closed",
            Self::StaleAuthority => "attempt authority belongs to another actor or generation",
            Self::WrongExecution => "attempt authority does not name the current execution",
            Self::WrongPhase => "logical execution is in the wrong phase",
            Self::AlreadyConcluded => "logical execution has already concluded",
            Self::InvariantViolation => "logical execution actor invariant was violated",
            Self::Establish(_) => unreachable!("handled above"),
            Self::StandDown(_) => unreachable!("handled above"),
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
            ActorStateError::RecoveryRefused(_)
            | ActorStateError::ReplacementNotReady(_)
            | ActorStateError::StaleReplacement
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
    pub establish_error: Option<EstablishIssueError>,
    pub stand_down_error: Option<ContextStandDownError>,
}

type ActorReply<T> = oneshot::Sender<Result<T, LogicalExecutionActorError>>;

enum ActorCommand {
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
        reply: ActorReply<LogicalConclusion>,
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
}

impl LogicalExecutionActor {
    pub const fn id(&self) -> LogicalExecutionActorId {
        self.id
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
        request(&self.sender, |reply| ActorCommand::Failed { permit, reply }).await
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
#[derive(Debug)]
#[must_use = "the logical execution actor owner must remain supervised"]
pub struct LogicalExecutionActorOwner {
    actor: LogicalExecutionActor,
    join: JoinHandle<()>,
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
pub fn spawn_logical_execution_actor(
    runtime: &Handle,
    config: LogicalExecutionActorConfig,
) -> Result<(LogicalExecutionActorOwner, AttemptInstantiationPermit), LogicalExecutionActorError> {
    let mut state = LogicalExecutionState::new(
        config.initial_execution,
        RecoveryMode::NoRecovery,
        config.effect,
        LogicalOutputMode::CompletionOnly,
        1,
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
    let clock = Arc::clone(&config.clock);
    let actor_lifetime = Arc::clone(&lifetime);
    let join = runtime.spawn(async move {
        run_actor(
            &mut state,
            receiver,
            abandoned_rx,
            actor_lifetime,
            establish,
            stand_down,
            capability_identity,
            config.abort_effect_port,
            abort_capacity,
            clock,
        )
        .await;
    });
    Ok((
        LogicalExecutionActorOwner {
            actor: LogicalExecutionActor {
                id: actor_id,
                sender: sender.clone(),
            },
            join,
        },
        AttemptInstantiationPermit {
            capability: Some(capability),
            lifetime: Some(lifetime),
            mailbox_liveness: Some(sender),
        },
    ))
}

async fn run_actor(
    state: &mut LogicalExecutionState,
    mut receiver: mpsc::Receiver<ActorCommand>,
    mut abandoned: watch::Receiver<bool>,
    lifetime: Arc<PermitLifetime>,
    mut establish: EstablishIssueLedger,
    mut stand_down: ContextStandDownLedger,
    activation: AttemptActivationIdentity,
    abort_effect_port: Option<Arc<dyn AbortQueryContextEffectPort>>,
    mut abort_capacity: Option<watch::Receiver<u64>>,
    clock: Arc<dyn LogicalExecutionClock>,
) {
    let mut establish_error = None;
    let mut stand_down_error = None;
    let mut pending_aborts = BTreeMap::new();
    let mut abort_backpressured = false;
    let mut receiver_open = true;
    loop {
        let now = clock.now();
        if *abandoned.borrow() {
            establish.revoke_issue_authority();
            conclude_abandoned(state);
            lifetime.settle();
        }
        if let Err(error) =
            synchronize_stand_down(state, activation, &mut establish, &mut stand_down)
        {
            stand_down_error.get_or_insert(error);
        }
        if stand_down.responsibility_settled() {
            if let Err(error) =
                cancel_pending_abort_issues(&mut stand_down, &mut pending_aborts, now)
            {
                stand_down_error.get_or_insert(error);
            }
            abort_backpressured = !pending_aborts.is_empty();
        }
        if stand_down_error.is_none() {
            if let Err(error) = drive_abort_effect(
                &mut stand_down,
                abort_effect_port.as_deref(),
                &mut pending_aborts,
                &mut abort_backpressured,
                now,
            ) {
                stand_down_error.get_or_insert(error);
            }
        }
        if !receiver_open && actor_cleanup_complete(state, &stand_down) {
            return;
        }
        let abort_retry_at = stand_down_error
            .is_none()
            .then(|| stand_down.next_retry_at())
            .flatten();
        tokio::select! {
            biased;
            changed = abandoned.changed(), if !*abandoned.borrow() => {
                if changed.is_ok() && *abandoned.borrow() {
                    establish.revoke_issue_authority();
                    conclude_abandoned(state);
                    lifetime.settle();
                }
            }
            result = establish.apply_next_event() => {
                match result {
                    Ok(()) if establish.has_worker_rejection() => {
                        establish_error.get_or_insert(EstablishIssueError::EstablishRejected);
                        establish.revoke_issue_authority();
                        conclude_failed(state);
                    }
                    Ok(()) => {}
                    Err(error) => {
                        establish_error.get_or_insert(error);
                        establish.revoke_issue_authority();
                        conclude_failed(state);
                    }
                }
            }
            result = stand_down.apply_next_event(|| clock.now()), if stand_down.started() && !stand_down.responsibility_settled() => {
                if let Err(error) = result {
                    stand_down_error.get_or_insert(error);
                }
            }
            _ = wait_for_abort_retry(clock.as_ref(), abort_retry_at), if abort_retry_at.is_some() => {}
            result = wait_for_abort_capacity(&mut abort_capacity), if abort_backpressured => {
                match result {
                    Ok(()) => abort_backpressured = false,
                    Err(error) => {
                        stand_down_error.get_or_insert(error);
                    }
                }
            }
            command = receiver.recv(), if receiver_open => {
                let Some(command) = command else {
                    receiver_open = false;
                    continue;
                };
                handle_command(
                    state,
                    &mut establish,
                    &mut establish_error,
                    &mut stand_down,
                    &mut stand_down_error,
                    activation,
                    clock.as_ref(),
                    command,
                );
            }
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

fn conclude_abandoned(state: &mut LogicalExecutionState) {
    if state.conclusion().is_some() {
        return;
    }
    let execution = match state.phase() {
        ExecutionPhase::Instantiating { execution, .. }
        | ExecutionPhase::Running { execution, .. } => execution,
        _ => return,
    };
    if let Ok(capability) = state.attempt_capability(execution) {
        let _ = state.conclude(&capability, LogicalConclusion::Cancelled);
    }
}

fn handle_command(
    state: &mut LogicalExecutionState,
    establish: &mut EstablishIssueLedger,
    establish_error: &mut Option<EstablishIssueError>,
    stand_down: &mut ContextStandDownLedger,
    stand_down_error: &mut Option<ContextStandDownError>,
    actor_activation: AttemptActivationIdentity,
    clock: &dyn LogicalExecutionClock,
    command: ActorCommand,
) {
    match command {
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
        ActorCommand::Completed { permit, reply } => match establish.ensure_success_ready() {
            Ok(()) => {
                establish.revoke_issue_authority();
                settle_terminal(
                    state,
                    permit.into_parts(),
                    LogicalConclusion::Succeeded,
                    reply,
                );
            }
            Err(error) => {
                establish_error.get_or_insert(error);
                establish.revoke_issue_authority();
                settle_terminal_with_error(state, permit.into_parts(), error.into(), reply);
            }
        },
        ActorCommand::Failed { permit, reply } => {
            establish.revoke_issue_authority();
            settle_terminal(state, permit.into_parts(), LogicalConclusion::Failed, reply);
        }
        ActorCommand::BeginAdmissionIssue {
            activation,
            request,
            reply,
        } => {
            let result = verify_running_activation(state, activation)
                .and_then(|()| (*establish_error).map_or(Ok(()), |error| Err(error.into())))
                .and_then(|()| {
                    establish
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
            let result = verify_actor_activation(actor_activation, activation)
                .and_then(|()| {
                    if state.conclusion().is_some() {
                        Ok(())
                    } else {
                        (*establish_error).map_or(Ok(()), |error| Err(error.into()))
                    }
                })
                .and_then(|()| {
                    establish
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
                establish.revoke_issue_authority();
                conclude_failed(state);
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
                .and_then(|()| (*establish_error).map_or(Ok(()), |error| Err(error.into())))
                .and_then(|()| {
                    establish
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
                .and_then(|()| (*establish_error).map_or(Ok(()), |error| Err(error.into())))
                .and_then(|()| {
                    establish
                        .reauthorize_issue(activation, context, clock.now())
                        .map_err(Into::into)
                });
            let _ = reply.send(result);
        }
        ActorCommand::EstablishSnapshot { context, reply } => {
            let _ = reply.send(Ok(establish.snapshot(context)));
        }
        ActorCommand::StandDownSnapshot { context, reply } => {
            let _ = reply.send(Ok(stand_down.snapshot(context)));
        }
        ActorCommand::RegistryContextConverged {
            context,
            convergence,
            reply,
        } => {
            let result = stand_down
                .observe_registry_convergence(context, convergence)
                .map_err(Into::into);
            if let Err(LogicalExecutionActorError::StandDown(error)) = &result {
                stand_down_error.get_or_insert(*error);
            }
            let _ = reply.send(result);
        }
        ActorCommand::Snapshot { reply } => {
            let _ = reply.send(Ok(LogicalExecutionActorSnapshot {
                phase: state.phase(),
                conclusion: state.conclusion(),
                establish_error: *establish_error,
                stand_down_error: *stand_down_error,
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

fn synchronize_stand_down(
    state: &LogicalExecutionState,
    activation: AttemptActivationIdentity,
    establish: &mut EstablishIssueLedger,
    stand_down: &mut ContextStandDownLedger,
) -> Result<(), ContextStandDownError> {
    let Some(conclusion) = state.conclusion() else {
        return Ok(());
    };
    let cause = match conclusion {
        LogicalConclusion::Succeeded => return Ok(()),
        LogicalConclusion::Cancelled => ContextStandDownCause::LogicalExecutionCancelled,
        LogicalConclusion::Failed | LogicalConclusion::BusinessDecisionRequired => {
            ContextStandDownCause::LogicalExecutionFailed
        }
    };
    let facts = establish
        .stand_down_facts()
        .map_err(|_| ContextStandDownError::WrongState)?;
    if !stand_down.started() {
        return stand_down.begin(activation, cause, facts);
    }
    for (context, fact) in facts {
        stand_down.refresh_establish_fact(context, fact)?;
    }
    Ok(())
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

fn actor_cleanup_complete(
    state: &LogicalExecutionState,
    stand_down: &ContextStandDownLedger,
) -> bool {
    match state.conclusion() {
        Some(LogicalConclusion::Succeeded) => true,
        Some(_) => stand_down.responsibility_settled(),
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

#[cfg(test)]
mod tests {
    use std::future::Future;
    use std::sync::Mutex;
    use std::task::Poll;

    use super::*;
    use novarocks_execution_contract::{QueryContextReceipt, QueryContextState};
    use novarocks_types::identity::{AttemptId, BackendProcessId, FrontendProcessId, QueryId};

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

    fn execution(query: i64) -> QueryExecutionId {
        QueryExecutionId::new(QueryId::new(31, query), AttemptId::new(1).unwrap()).unwrap()
    }

    fn config(initial_execution: QueryExecutionId) -> LogicalExecutionActorConfig {
        LogicalExecutionActorConfig::single_attempt_completion(
            initial_execution,
            ExecutionEffect::None,
            NonZeroUsize::new(1).unwrap(),
            Vec::new(),
            NonZeroUsize::new(2).unwrap(),
            NonZeroUsize::new(2).unwrap(),
        )
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
        let (owner, permit) = spawn_logical_execution_actor(&runtime, config(execution)).unwrap();
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
    async fn dropping_initial_permit_concludes_cancelled() {
        let runtime = Handle::current();
        let (owner, permit) =
            spawn_logical_execution_actor(&runtime, config(execution(2))).unwrap();
        let actor = owner.actor();
        drop(permit);
        let snapshot = wait_for_conclusion(actor, LogicalConclusion::Cancelled).await;
        assert!(matches!(snapshot.phase, ExecutionPhase::Converging));
    }

    #[tokio::test]
    async fn cancelling_activation_before_enqueue_does_not_strand_instantiating() {
        let runtime = Handle::current();
        let (owner, permit) =
            spawn_logical_execution_actor(&runtime, config(execution(3))).unwrap();
        let actor = owner.actor();
        let activation = actor.activate(permit.ready());
        drop(activation);
        wait_for_conclusion(actor, LogicalConclusion::Cancelled).await;
    }

    #[tokio::test]
    async fn cancelling_activation_after_enqueue_does_not_strand_running() {
        let runtime = Handle::current();
        let (owner, permit) =
            spawn_logical_execution_actor(&runtime, config(execution(4))).unwrap();
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
        let (owner, permit) =
            spawn_logical_execution_actor(&runtime, config(execution(5))).unwrap();
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
        let (failed_owner, failed) =
            spawn_logical_execution_actor(&runtime, config(execution(6))).unwrap();
        assert_eq!(
            failed_owner
                .actor()
                .initialization_failed(failed)
                .await
                .unwrap(),
            LogicalConclusion::Failed
        );

        let (cancelled_owner, cancelled) =
            spawn_logical_execution_actor(&runtime, config(execution(7))).unwrap();
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
        let (owner, permit) =
            spawn_logical_execution_actor(&runtime, config(execution(8))).unwrap();
        let actor = owner.actor();
        let running = actor.activate(permit.ready()).await.unwrap();
        assert_eq!(
            actor.fail_attempt(running).await.unwrap(),
            LogicalConclusion::Failed
        );
    }

    #[tokio::test]
    async fn permit_keeps_actor_alive_after_owner_is_dropped() {
        let runtime = Handle::current();
        let (owner, permit) =
            spawn_logical_execution_actor(&runtime, config(execution(9))).unwrap();
        let LogicalExecutionActorOwner { actor, join } = owner;
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
}
