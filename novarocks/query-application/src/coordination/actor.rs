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

use std::fmt;
use std::num::NonZeroUsize;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use novarocks_types::identity::QueryExecutionId;
use tokio::runtime::Handle;
use tokio::sync::{mpsc, oneshot, watch};
use tokio::task::JoinHandle;

use super::actor_state::{ActorStateError, AttemptCapability, LogicalExecutionState};
use super::{ExecutionEffect, ExecutionPhase, LogicalConclusion, LogicalOutputMode, RecoveryMode};

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

impl AttemptActivationIdentity {
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
#[derive(Clone, Copy, Debug)]
pub struct LogicalExecutionActorConfig {
    initial_execution: QueryExecutionId,
    effect: ExecutionEffect,
    mailbox_capacity: NonZeroUsize,
}

impl LogicalExecutionActorConfig {
    pub const fn single_attempt_completion(
        initial_execution: QueryExecutionId,
        effect: ExecutionEffect,
        mailbox_capacity: NonZeroUsize,
    ) -> Self {
        Self {
            initial_execution,
            effect,
            mailbox_capacity,
        }
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
}

impl fmt::Display for LogicalExecutionActorError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::MailboxClosed => "logical execution actor mailbox is closed",
            Self::StaleAuthority => "attempt authority belongs to another actor or generation",
            Self::WrongExecution => "attempt authority does not name the current execution",
            Self::WrongPhase => "logical execution is in the wrong phase",
            Self::AlreadyConcluded => "logical execution has already concluded",
            Self::InvariantViolation => "logical execution actor invariant was violated",
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

/// Immutable observation of the actor-owned state. It carries no mutation
/// authority and is suitable for diagnostics and deterministic supervision.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct LogicalExecutionActorSnapshot {
    pub phase: ExecutionPhase,
    pub conclusion: Option<LogicalConclusion>,
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
    let actor_id = LogicalExecutionActorId(capability.actor_instance_id());
    let (abandoned, abandoned_rx) = watch::channel(false);
    let lifetime = Arc::new(PermitLifetime {
        abandoned,
        settled: AtomicBool::new(false),
    });
    let (sender, receiver) = mpsc::channel(config.mailbox_capacity.get());
    let actor_lifetime = Arc::clone(&lifetime);
    let join = runtime.spawn(async move {
        run_actor(&mut state, receiver, abandoned_rx, actor_lifetime).await;
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
) {
    loop {
        if *abandoned.borrow() {
            conclude_abandoned(state);
            lifetime.settle();
        }
        tokio::select! {
            biased;
            changed = abandoned.changed(), if !*abandoned.borrow() => {
                if changed.is_ok() && *abandoned.borrow() {
                    conclude_abandoned(state);
                    lifetime.settle();
                }
            }
            command = receiver.recv() => {
                let Some(command) = command else {
                    return;
                };
                handle_command(state, command);
            }
        }
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

fn handle_command(state: &mut LogicalExecutionState, command: ActorCommand) {
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
        ActorCommand::Completed { permit, reply } => {
            settle_terminal(
                state,
                permit.into_parts(),
                LogicalConclusion::Succeeded,
                reply,
            );
        }
        ActorCommand::Failed { permit, reply } => {
            settle_terminal(state, permit.into_parts(), LogicalConclusion::Failed, reply);
        }
        ActorCommand::Snapshot { reply } => {
            let _ = reply.send(Ok(LogicalExecutionActorSnapshot {
                phase: state.phase(),
                conclusion: state.conclusion(),
            }));
        }
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

#[cfg(test)]
mod tests {
    use std::future::Future;
    use std::task::Poll;

    use super::*;
    use novarocks_types::identity::{AttemptId, QueryId};

    fn execution(query: i64) -> QueryExecutionId {
        QueryExecutionId::new(QueryId::new(31, query), AttemptId::new(1).unwrap()).unwrap()
    }

    fn config(initial_execution: QueryExecutionId) -> LogicalExecutionActorConfig {
        LogicalExecutionActorConfig::single_attempt_completion(
            initial_execution,
            ExecutionEffect::None,
            NonZeroUsize::new(1).unwrap(),
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
}
