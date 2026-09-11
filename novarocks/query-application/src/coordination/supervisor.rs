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

//! Process-owned logical execution supervision.
//!
//! The supervisor is the sole constructor of [`QueryExecutionClient`]. A
//! bounded start mailbox transfers one governed work owner before returning a
//! future to the caller. Accepted requests remain in the supervisor's JoinSet
//! even when that caller drops its reply future.

use std::fmt;
use std::future::Future;
use std::num::NonZeroUsize;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::task::Poll;
use std::time::{Duration, Instant};

use novarocks_types::identity::{
    AttemptId, FrontendProcessId, LocalQuerySequence, QueryExecutionId, QueryIdAttribution,
    QueryProcessNamespace,
};
use novarocks_workload_control::{CancellationReason, Stage, StageRequest, WorkError, WorkOwner};
use tokio::runtime::Handle;
use tokio::sync::{mpsc, oneshot, watch};
use tokio::task::{JoinHandle, JoinSet};

use crate::api::{
    ActiveNativeAttemptOwner, DormantNativeAttemptOwner, ExecutionHandle,
    LogicalExecutionNativePort, LogicalNativeOpenError, LogicalNativeOpenRequest,
    NativeAttemptPreparationError, NativeAttemptTerminal, QueryExecutionClient,
    QueryExecutionDriver, QueryExecutionError, QueryExecutionErrorKind, QueryExecutionFuture,
    QueryExecutionRequest,
};
use crate::preparation::OutputContract;

use super::{
    LogicalExecutionActorConfig, LogicalExecutionRuntimeRegistry,
    LogicalExecutionRuntimeRegistryError, LogicalExecutionRuntimeRegistryHandle,
    LogicalExecutionRuntimeShutdownError, NativeAttemptDrive, RecoveryMode, build_attempt_schedule,
};

static NEXT_PROCESS_QUERY_SEQUENCE: AtomicU64 = AtomicU64::new(0);
const NATIVE_CONVERGENCE_PANIC_BACKOFF: Duration = Duration::from_millis(10);

/// Explicit process bounds used by the first supervisor slice.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct LogicalExecutionSupervisorConfig {
    start_capacity: NonZeroUsize,
    actor_mailbox_capacity: NonZeroUsize,
    max_admission_issues_per_context: NonZeroUsize,
    max_establish_authorizations_per_context: NonZeroUsize,
}

impl LogicalExecutionSupervisorConfig {
    pub const fn new(
        start_capacity: NonZeroUsize,
        actor_mailbox_capacity: NonZeroUsize,
        max_admission_issues_per_context: NonZeroUsize,
        max_establish_authorizations_per_context: NonZeroUsize,
    ) -> Self {
        Self {
            start_capacity,
            actor_mailbox_capacity,
            max_admission_issues_per_context,
            max_establish_authorizations_per_context,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum LogicalExecutionSupervisorShutdownError {
    DeadlineExceeded,
    Registry(LogicalExecutionRuntimeRegistryError),
    SupervisorFailed(QueryExecutionError),
    SupervisorPanicked,
}

impl fmt::Display for LogicalExecutionSupervisorShutdownError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::DeadlineExceeded => {
                formatter.write_str("logical execution supervisor shutdown deadline exceeded")
            }
            Self::Registry(error) => write!(
                formatter,
                "logical execution supervisor Registry shutdown failed: {error}"
            ),
            Self::SupervisorFailed(error) => {
                write!(formatter, "logical execution supervisor failed: {error}")
            }
            Self::SupervisorPanicked => {
                formatter.write_str("logical execution supervisor task panicked")
            }
        }
    }
}

impl std::error::Error for LogicalExecutionSupervisorShutdownError {}

/// Unique process owner for logical execution admission, tasks, and Registry.
///
/// Shutdown borrows this owner. A timed-out or cancelled wait leaves the exact
/// join handle and Registry owner here so a later call resumes the same
/// convergence.
pub struct LogicalExecutionSupervisor {
    shutdown: watch::Sender<bool>,
    join: Option<JoinHandle<Result<(), QueryExecutionError>>>,
    run_outcome: Option<SupervisorRunOutcome>,
    registry: LogicalExecutionRuntimeRegistry,
    shutdown_complete: bool,
}

enum SupervisorRunOutcome {
    Completed,
    Failed(QueryExecutionError),
    Panicked,
}

impl fmt::Debug for LogicalExecutionSupervisor {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("LogicalExecutionSupervisor")
            .field("shutdown_requested", &*self.shutdown.borrow())
            .field("shutdown_complete", &self.shutdown_complete)
            .finish_non_exhaustive()
    }
}

impl Drop for LogicalExecutionSupervisor {
    fn drop(&mut self) {
        assert!(
            self.shutdown_complete,
            "logical execution supervisor dropped without explicit shutdown"
        );
    }
}

impl LogicalExecutionSupervisor {
    /// Constructs the sole bounded client together with its process owner.
    pub fn new(
        runtime: Handle,
        native: Arc<dyn LogicalExecutionNativePort>,
        namespace: QueryProcessNamespace,
        frontend_process_id: FrontendProcessId,
        config: LogicalExecutionSupervisorConfig,
    ) -> (Self, QueryExecutionClient) {
        let (starts, start_rx) = mpsc::channel(config.start_capacity.get());
        let (shutdown, shutdown_rx) = watch::channel(false);
        let registry = LogicalExecutionRuntimeRegistry::new(runtime.clone());
        let registry_handle = registry.handle();
        let driver = BoundedQueryExecutionDriver {
            starts,
            shutdown: shutdown_rx.clone(),
        };
        let join = runtime.spawn(run_supervisor(
            registry_handle,
            native,
            namespace,
            frontend_process_id,
            config,
            start_rx,
            shutdown_rx,
        ));
        (
            Self {
                shutdown,
                join: Some(join),
                run_outcome: None,
                registry,
                shutdown_complete: false,
            },
            QueryExecutionClient::new(driver),
        )
    }

    pub async fn shutdown_until(
        &mut self,
        deadline: Instant,
    ) -> Result<(), LogicalExecutionSupervisorShutdownError> {
        self.shutdown.send_replace(true);
        if self.shutdown_complete {
            return Ok(());
        }
        if self.run_outcome.is_none() {
            let join = self
                .join
                .as_mut()
                .expect("unfinished supervisor retains its join handle");
            self.run_outcome = match tokio::time::timeout_at(deadline.into(), join).await {
                Err(_) => {
                    return Err(LogicalExecutionSupervisorShutdownError::DeadlineExceeded);
                }
                Ok(Err(_)) => Some(SupervisorRunOutcome::Panicked),
                Ok(Ok(Err(error))) => Some(SupervisorRunOutcome::Failed(error)),
                Ok(Ok(Ok(()))) => Some(SupervisorRunOutcome::Completed),
            };
            self.join.take();
        }

        match self.registry.shutdown_until(deadline).await {
            Ok(()) => {}
            Err(LogicalExecutionRuntimeShutdownError::DeadlineExceeded { .. }) => {
                return Err(LogicalExecutionSupervisorShutdownError::DeadlineExceeded);
            }
            Err(LogicalExecutionRuntimeShutdownError::Registry(error)) => {
                return Err(LogicalExecutionSupervisorShutdownError::Registry(error));
            }
        }

        self.shutdown_complete = true;
        match self
            .run_outcome
            .take()
            .expect("registry shutdown follows a collected supervisor outcome")
        {
            SupervisorRunOutcome::Completed => Ok(()),
            SupervisorRunOutcome::Failed(error) => Err(
                LogicalExecutionSupervisorShutdownError::SupervisorFailed(error),
            ),
            SupervisorRunOutcome::Panicked => {
                Err(LogicalExecutionSupervisorShutdownError::SupervisorPanicked)
            }
        }
    }

    #[cfg(test)]
    fn inject_registry_shutdown_error_once(&mut self, error: LogicalExecutionRuntimeRegistryError) {
        self.registry.inject_shutdown_error_once(error);
    }
}

#[derive(Debug)]
struct BoundedQueryExecutionDriver {
    starts: mpsc::Sender<StartCommand>,
    shutdown: watch::Receiver<bool>,
}

impl QueryExecutionDriver for BoundedQueryExecutionDriver {
    fn start(&self, request: QueryExecutionRequest, owner: WorkOwner) -> QueryExecutionFuture {
        if *self.shutdown.borrow() {
            return rejected_start(owner, "logical execution supervisor is shutting down");
        }
        let (reply, response) = oneshot::channel();
        let command = StartCommand {
            request,
            owner,
            reply,
        };
        match self.starts.try_send(command) {
            Ok(()) => Box::pin(async move {
                response.await.unwrap_or_else(|_| {
                    Err(QueryExecutionError::new(
                        QueryExecutionErrorKind::Failed,
                        "logical execution supervisor closed without a start verdict",
                    ))
                })
            }),
            Err(mpsc::error::TrySendError::Full(command)) => rejected_start(
                command.owner,
                "logical execution start capacity is exhausted",
            ),
            Err(mpsc::error::TrySendError::Closed(command)) => {
                rejected_start(command.owner, "logical execution supervisor is closed")
            }
        }
    }
}

fn rejected_start(owner: WorkOwner, message: &'static str) -> QueryExecutionFuture {
    owner.complete();
    Box::pin(async move {
        Err(QueryExecutionError::new(
            QueryExecutionErrorKind::Rejected,
            message,
        ))
    })
}

struct StartCommand {
    request: QueryExecutionRequest,
    owner: WorkOwner,
    reply: oneshot::Sender<Result<ExecutionHandle, QueryExecutionError>>,
}

struct ProcessQueryIdAllocator {
    namespace: QueryProcessNamespace,
}

impl ProcessQueryIdAllocator {
    const fn new(namespace: QueryProcessNamespace) -> Self {
        Self { namespace }
    }

    fn first_execution(&self) -> Result<QueryExecutionId, QueryExecutionError> {
        let previous = NEXT_PROCESS_QUERY_SEQUENCE
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |current| {
                current
                    .checked_add(1)
                    .filter(|next| *next <= i64::MAX as u64)
            })
            .map_err(|_| {
                QueryExecutionError::new(
                    QueryExecutionErrorKind::Failed,
                    "logical execution query id sequence is exhausted",
                )
            })?;
        let sequence = LocalQuerySequence::new(previous + 1).expect("validated nonzero sequence");
        QueryExecutionId::new(
            QueryIdAttribution::new(self.namespace, sequence).into_query_id(),
            AttemptId::new(1).expect("initial attempt id is nonzero"),
        )
        .map_err(|error| {
            QueryExecutionError::new(QueryExecutionErrorKind::Failed, error.to_string())
        })
    }
}

async fn run_supervisor(
    registry: LogicalExecutionRuntimeRegistryHandle,
    native: Arc<dyn LogicalExecutionNativePort>,
    namespace: QueryProcessNamespace,
    frontend_process_id: FrontendProcessId,
    config: LogicalExecutionSupervisorConfig,
    mut starts: mpsc::Receiver<StartCommand>,
    mut shutdown: watch::Receiver<bool>,
) -> Result<(), QueryExecutionError> {
    let query_ids = Arc::new(ProcessQueryIdAllocator::new(namespace));
    let mut logical_tasks = JoinSet::new();
    let mut task_failure = None;
    loop {
        if *shutdown.borrow() {
            break;
        }
        tokio::select! {
            biased;
            changed = shutdown.changed() => {
                if changed.is_err() || *shutdown.borrow() {
                    break;
                }
            }
            command = starts.recv() => {
                let Some(command) = command else { break };
                logical_tasks.spawn(run_logical_execution(
                    command,
                    Arc::clone(&native),
                    Arc::clone(&query_ids),
                    frontend_process_id,
                    config,
                    registry.clone(),
                    shutdown.clone(),
                ));
            }
            result = logical_tasks.join_next(), if !logical_tasks.is_empty() => {
                match result {
                    Some(Ok(Ok(()))) | None => {}
                    Some(Ok(Err(error))) => {
                        task_failure.get_or_insert(error);
                    }
                    Some(Err(_)) => {
                        task_failure.get_or_insert_with(|| QueryExecutionError::new(
                            QueryExecutionErrorKind::Failed,
                            "logical execution supervision task panicked",
                        ));
                    }
                }
            }
        }
    }

    starts.close();
    while let Ok(command) = starts.try_recv() {
        command.owner.complete();
        let _ = command.reply.send(Err(QueryExecutionError::new(
            QueryExecutionErrorKind::Rejected,
            "logical execution supervisor shut down before starting the accepted request",
        )));
    }
    while let Some(result) = logical_tasks.join_next().await {
        match result {
            Ok(Ok(())) => {}
            Ok(Err(error)) => {
                task_failure.get_or_insert(error);
            }
            Err(_) => {
                task_failure.get_or_insert_with(|| {
                    QueryExecutionError::new(
                        QueryExecutionErrorKind::Failed,
                        "logical execution supervision task panicked",
                    )
                });
            }
        }
    }
    task_failure.map_or(Ok(()), Err)
}

async fn run_logical_execution(
    command: StartCommand,
    native: Arc<dyn LogicalExecutionNativePort>,
    query_ids: Arc<ProcessQueryIdAllocator>,
    frontend_process_id: FrontendProcessId,
    config: LogicalExecutionSupervisorConfig,
    registry: super::LogicalExecutionRuntimeRegistryHandle,
    mut shutdown: watch::Receiver<bool>,
) -> Result<(), QueryExecutionError> {
    let StartCommand {
        request,
        owner,
        reply,
    } = command;
    let mut pending_owner = PendingWorkOwner(Some(owner));
    let requester = pending_owner.owner().cancellation_requester();
    let scope = pending_owner.owner().scope();
    let initial_execution = match query_ids.first_execution() {
        Ok(execution) => execution,
        Err(error) => return fail_uninstalled_start(pending_owner, reply, error),
    };
    let description = Arc::new(request.into_description());
    if !matches!(description.output(), OutputContract::CompletionOnly)
        || description.recovery() != RecoveryMode::NoRecovery
    {
        return fail_uninstalled_start(
            pending_owner,
            reply,
            QueryExecutionError::new(
                QueryExecutionErrorKind::InvalidRequest,
                "the first logical supervisor slice accepts only NoRecovery completion executions",
            ),
        );
    }
    let cancellation = match scope.cancellation() {
        Ok(cancellation) => cancellation,
        Err(error) => return fail_uninstalled_start(pending_owner, reply, work_error(error)),
    };
    let (open_request, open_acceptance) = LogicalNativeOpenRequest::issue(
        initial_execution,
        Arc::clone(&description),
        scope.id(),
        cancellation.clone(),
    );
    let opened = await_with_shutdown(native.open(open_request), &mut shutdown, &requester).await;
    let mut session =
        match opened.and_then(|session| open_acceptance.accept(session).map_err(Into::into)) {
            Ok(session) => session,
            Err(error) => {
                return fail_uninstalled_start(pending_owner, reply, native_open_error(error));
            }
        };
    let (attempt_request, attempt_acceptance) = match session.issue_attempt(initial_execution) {
        Ok(pair) => pair,
        Err(error) => {
            return fail_uninstalled_start(pending_owner, reply, contract_error(error));
        }
    };
    let prepared =
        await_with_shutdown(session.prepare(attempt_request), &mut shutdown, &requester).await;
    let prepared = match prepared
        .and_then(|prepared| attempt_acceptance.accept(prepared).map_err(Into::into))
    {
        Ok(prepared) => prepared,
        Err(error) => {
            return fail_uninstalled_start(pending_owner, reply, native_prepare_error(error));
        }
    };
    debug_assert_eq!(prepared.execution, initial_execution);
    let schedule = match build_attempt_schedule(
        initial_execution,
        frontend_process_id,
        &prepared.eligible_backends,
        &prepared.scan_work,
        description.scheduling(),
    ) {
        Ok(schedule) => schedule,
        Err(error) => {
            return fail_uninstalled_start(
                pending_owner,
                reply,
                QueryExecutionError::new(QueryExecutionErrorKind::Failed, error.to_string()),
            );
        }
    };
    let stage_admission = match scope.acquire(StageRequest {
        stage: Stage::Execution,
        retained_bytes: 0,
    }) {
        Ok(admission) => admission,
        Err(error) => return fail_uninstalled_start(pending_owner, reply, work_error(error)),
    };
    let stage = match await_with_shutdown(stage_admission, &mut shutdown, &requester).await {
        Ok(stage) => stage,
        Err(error) => return fail_uninstalled_start(pending_owner, reply, work_error(error)),
    };
    let contexts = schedule.contexts().to_vec();
    let reservation = match registry.reserve(initial_execution, contexts.clone()) {
        Ok(reservation) => reservation,
        Err(error) => {
            drop(stage);
            return fail_uninstalled_start(pending_owner, reply, registry_error(error));
        }
    };
    let max_establish_authorizations = config.max_establish_authorizations_per_context;
    let actor_config = match LogicalExecutionActorConfig::no_recovery_completion(
        initial_execution,
        description.effect(),
        config.actor_mailbox_capacity,
        contexts,
        config.max_admission_issues_per_context,
        max_establish_authorizations,
        pending_owner.take(),
        stage,
    ) {
        Ok(actor_config) => actor_config.with_abort_query_context_effect_port(
            session.abort_effect_port(),
            max_establish_authorizations,
        ),
        Err(error) => {
            let _ = reply.send(Err(actor_error(error)));
            return Ok(());
        }
    };
    let installed = match reservation.spawn_and_install(actor_config) {
        Ok(installed) => installed,
        Err(error) => {
            let error = registry_error(error);
            let _ = reply.send(Err(error.clone()));
            return Err(error);
        }
    };
    let (registration, initial, output) = installed.into_parts();
    let actor = match registry.actor(&registration) {
        Ok(actor) => actor,
        Err(error) => {
            let error = registry_error(error);
            let _ = reply.send(Err(error.clone()));
            return Err(error);
        }
    };
    let mut dormant = prepared.owner;
    let activation = await_with_shutdown(
        catch_future_panic(dormant.activate(&schedule, cancellation.clone())),
        &mut shutdown,
        &requester,
    )
    .await;
    let active = match activation {
        Ok(Ok(active)) => {
            drop(dormant);
            active
        }
        Ok(Err(failure)) => {
            let error = failure.into_failure().error().clone();
            let actor_result = actor
                .initialization_failed(initial)
                .await
                .map(|_| ())
                .map_err(actor_error);
            let _ = reply.send(Err(error));
            converge_dormant(dormant.as_mut(), cancellation, &mut shutdown, &requester).await;
            drop(dormant);
            let retire_result = retire_logical(&registry, registration).await;
            return first_supervision_error(actor_result, retire_result);
        }
        Err(()) => {
            let error = native_future_panicked("Native attempt activation panicked");
            let actor_result = actor
                .initialization_failed(initial)
                .await
                .map(|_| ())
                .map_err(actor_error);
            let _ = reply.send(Err(error.clone()));
            converge_dormant(dormant.as_mut(), cancellation, &mut shutdown, &requester).await;
            drop(dormant);
            let retire_result = retire_logical(&registry, registration).await;
            return first_supervision_error(
                Err(error),
                first_supervision_error(actor_result, retire_result),
            );
        }
    };
    let mut active = active.into_owner();
    let running = match actor.activate(initial.ready()).await {
        Ok(running) => running,
        Err(error) => {
            let error = actor_error(error);
            let _ = requester.request(CancellationReason::Requested);
            converge_active(active.as_mut(), cancellation, &mut shutdown, &requester).await;
            drop(active);
            let convergence_result =
                record_active_convergence(&registry, &registration, schedule.contexts()).await;
            let _ = reply.send(Err(error.clone()));
            let retire_result = retire_logical(&registry, registration).await;
            return first_supervision_error(
                Err(error),
                first_supervision_error(convergence_result, retire_result),
            );
        }
    };
    let drive = NativeAttemptDrive::new(running);
    let terminal = await_with_shutdown(
        catch_future_panic(active.run(&drive, cancellation.clone())),
        &mut shutdown,
        &requester,
    )
    .await;
    let actor_result = match terminal {
        Ok(NativeAttemptTerminal::Completed) => {
            converge_active(active.as_mut(), cancellation, &mut shutdown, &requester).await;
            drop(active);
            let running = drive.into_permit();
            match actor.complete_attempt(running).await {
                Ok(super::LogicalConclusion::Succeeded) => {
                    let handle = ExecutionHandle::new(requester, output.into_output());
                    let _ = reply.send(Ok(handle));
                    Ok(())
                }
                Ok(conclusion) => {
                    let error = QueryExecutionError::new(
                        QueryExecutionErrorKind::Failed,
                        format!("completion execution concluded as {conclusion:?}"),
                    );
                    let _ = reply.send(Err(error.clone()));
                    Err(error)
                }
                Err(error) => {
                    let error = actor_error(error);
                    let _ = reply.send(Err(error.clone()));
                    Err(error)
                }
            }
        }
        Ok(NativeAttemptTerminal::Failed(failure)) => {
            let client_error = failure.error().clone();
            let running = drive.into_permit();
            let actor_result = actor
                .fail_attempt_with_error(running, client_error.clone())
                .await
                .map(|_| ())
                .map_err(actor_error);
            let _ = reply.send(Err(client_error));
            converge_active(active.as_mut(), cancellation, &mut shutdown, &requester).await;
            drop(active);
            actor_result
        }
        Err(()) => {
            let error = native_future_panicked("Native attempt run panicked");
            let running = drive.into_permit();
            let actor_result = actor
                .fail_attempt_with_error(running, error.clone())
                .await
                .map(|_| ())
                .map_err(actor_error);
            let _ = reply.send(Err(error.clone()));
            converge_active(active.as_mut(), cancellation, &mut shutdown, &requester).await;
            drop(active);
            first_supervision_error(Err(error), actor_result)
        }
    };
    let convergence_result =
        record_active_convergence(&registry, &registration, schedule.contexts()).await;
    let retire_result = retire_logical(&registry, registration).await;
    first_supervision_error(
        actor_result,
        first_supervision_error(convergence_result, retire_result),
    )
}

async fn record_active_convergence(
    registry: &LogicalExecutionRuntimeRegistryHandle,
    registration: &super::LogicalExecutionRegistration,
    contexts: &[novarocks_execution_contract::QueryContextRef],
) -> Result<(), QueryExecutionError> {
    let mut first_error = None;
    for &context in contexts {
        if let Err(error) = registry
            .observe_worker_stopped_and_context_fenced(registration, context)
            .await
        {
            first_error.get_or_insert_with(|| registry_error(error));
        }
    }
    first_error.map_or(Ok(()), Err)
}

async fn converge_dormant(
    owner: &mut dyn DormantNativeAttemptOwner,
    cancellation: novarocks_workload_control::CancellationView,
    shutdown: &mut watch::Receiver<bool>,
    requester: &novarocks_workload_control::WorkCancellationRequester,
) {
    loop {
        let convergence = async { owner.converge(cancellation.clone()).await };
        if await_with_shutdown(catch_future_panic(convergence), shutdown, requester)
            .await
            .is_ok()
        {
            return;
        }
        let _ = requester.request(CancellationReason::Requested);
        tokio::time::sleep(NATIVE_CONVERGENCE_PANIC_BACKOFF).await;
    }
}

async fn converge_active(
    owner: &mut dyn ActiveNativeAttemptOwner,
    cancellation: novarocks_workload_control::CancellationView,
    shutdown: &mut watch::Receiver<bool>,
    requester: &novarocks_workload_control::WorkCancellationRequester,
) {
    loop {
        let convergence = async { owner.converge(cancellation.clone()).await };
        if await_with_shutdown(catch_future_panic(convergence), shutdown, requester)
            .await
            .is_ok()
        {
            return;
        }
        let _ = requester.request(CancellationReason::Requested);
        tokio::time::sleep(NATIVE_CONVERGENCE_PANIC_BACKOFF).await;
    }
}

async fn catch_future_panic<F>(future: F) -> Result<F::Output, ()>
where
    F: Future,
{
    let mut future = Box::pin(future);
    std::future::poll_fn(move |context| {
        match catch_unwind(AssertUnwindSafe(|| future.as_mut().poll(context))) {
            Ok(Poll::Ready(output)) => Poll::Ready(Ok(output)),
            Ok(Poll::Pending) => Poll::Pending,
            Err(_payload) => Poll::Ready(Err(())),
        }
    })
    .await
}

async fn await_with_shutdown<F>(
    future: F,
    shutdown: &mut watch::Receiver<bool>,
    requester: &novarocks_workload_control::WorkCancellationRequester,
) -> F::Output
where
    F: Future,
{
    tokio::pin!(future);
    let mut shutdown_open = true;
    loop {
        if *shutdown.borrow() {
            let _ = requester.request(CancellationReason::ServerShutdown);
            return future.await;
        }
        tokio::select! {
            output = &mut future => return output,
            changed = shutdown.changed(), if shutdown_open => {
                shutdown_open = changed.is_ok();
                if !shutdown_open || *shutdown.borrow() {
                    let _ = requester.request(CancellationReason::ServerShutdown);
                }
            }
        }
    }
}

struct PendingWorkOwner(Option<WorkOwner>);

impl PendingWorkOwner {
    fn owner(&self) -> &WorkOwner {
        self.0
            .as_ref()
            .expect("pending start retains its work owner")
    }

    fn take(&mut self) -> WorkOwner {
        self.0
            .take()
            .expect("pending start transfers its work owner once")
    }

    fn complete(mut self) {
        self.take().complete();
    }
}

impl Drop for PendingWorkOwner {
    fn drop(&mut self) {
        if let Some(owner) = self.0.take() {
            owner.complete();
        }
    }
}

fn fail_uninstalled_start(
    owner: PendingWorkOwner,
    reply: oneshot::Sender<Result<ExecutionHandle, QueryExecutionError>>,
    error: QueryExecutionError,
) -> Result<(), QueryExecutionError> {
    owner.complete();
    let _ = reply.send(Err(error));
    Ok(())
}

async fn retire_logical(
    registry: &super::LogicalExecutionRuntimeRegistryHandle,
    registration: super::LogicalExecutionRegistration,
) -> Result<(), QueryExecutionError> {
    registry
        .join_and_retire(registration)
        .await
        .map_err(registry_error)
}

fn first_supervision_error(
    first: Result<(), QueryExecutionError>,
    convergence: Result<(), QueryExecutionError>,
) -> Result<(), QueryExecutionError> {
    first.and(convergence)
}

fn contract_error(error: crate::api::NativeExecutionContractError) -> QueryExecutionError {
    QueryExecutionError::new(QueryExecutionErrorKind::Failed, error.to_string())
}

fn native_open_error(error: LogicalNativeOpenError) -> QueryExecutionError {
    match error {
        LogicalNativeOpenError::Contract(error) => contract_error(error),
        LogicalNativeOpenError::Runtime(failure) => failure.error().clone(),
    }
}

fn native_prepare_error(error: NativeAttemptPreparationError) -> QueryExecutionError {
    match error {
        NativeAttemptPreparationError::Contract(error) => contract_error(error),
        NativeAttemptPreparationError::Runtime(failure) => failure.error().clone(),
    }
}

fn actor_error(error: super::LogicalExecutionActorError) -> QueryExecutionError {
    QueryExecutionError::new(QueryExecutionErrorKind::Failed, error.to_string())
}

fn registry_error(error: LogicalExecutionRuntimeRegistryError) -> QueryExecutionError {
    QueryExecutionError::new(QueryExecutionErrorKind::Failed, error.to_string())
}

fn work_error(error: WorkError) -> QueryExecutionError {
    let kind = match error {
        WorkError::Cancelled(_) => QueryExecutionErrorKind::Cancelled,
        WorkError::CapacityWaitTimeout => QueryExecutionErrorKind::DeadlineExceeded,
        WorkError::Capacity(_) | WorkError::NotReady | WorkError::Closed => {
            QueryExecutionErrorKind::Rejected
        }
        _ => QueryExecutionErrorKind::Failed,
    };
    QueryExecutionError::new(kind, error.to_string())
}

fn native_future_panicked(message: &'static str) -> QueryExecutionError {
    QueryExecutionError::new(QueryExecutionErrorKind::Failed, message)
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
    use std::time::Duration;

    use novarocks_execution_contract::{
        AdmissionEpochCapability, AdmissionTicketId, CodecOwnedContent, ConfidentialContent,
        ContentFingerprint, CredentialEpoch, CredentialLeaseId, CredentialUpdate,
        EstablishQueryContext, LeaseValidFor, OperationOutcome, QueryContextAdmissionTicketReceipt,
        QueryContextRef, TaskOperationId,
    };
    use novarocks_sql::planning::query_execution::SealedPreparationPlan;
    use novarocks_sql::test_support::{NativePreparationFixture, native_preparation_plan};
    use novarocks_types::NativeCompatibilityId;
    use novarocks_types::identity::{BackendProcessId, FrontendProcessId};
    use novarocks_workload_control::{
        ResourceConfig, RootWork, WorkClass, WorkRequest, WorkloadConfig, WorkloadControl,
    };

    use crate::api::{
        ActivatedNativeAttempt, ActiveNativeAttemptOwner, DormantNativeAttemptOwner,
        LogicalNativeOpenFailure, LogicalNativeOpenFuture, NativeAttemptActivationFailure,
        NativeAttemptActivationFuture, NativeAttemptConvergenceFuture,
        NativeAttemptPreparationFailure, NativeAttemptPreparationFuture,
        NativeAttemptPreparationPort, NativeAttemptPreparationRequest, NativeAttemptRunFuture,
    };
    use crate::coordination::AttemptSchedule;
    use crate::coordination::{
        AdmissionIssueSettlement, EstablishIssueIdentity, EstablishIssueSubmit,
        EstablishTransportAdmission, EstablishTransportReservation, EstablishTransportSink,
        EstablishTransportSubmission,
    };
    use crate::coordination::{AttemptFailureClass, PermanentlyBackpressuredAbortEffectPort};
    use crate::preparation::{
        ExecutionResourceRequirements, FrozenCostEstimate, FrozenEstimateUnknownReason,
        FrozenExecutionDescription, FrozenExecutionDescriptionDraft,
    };

    use super::*;

    fn supervisor_config(start_capacity: usize) -> LogicalExecutionSupervisorConfig {
        LogicalExecutionSupervisorConfig::new(
            NonZeroUsize::new(start_capacity).unwrap(),
            NonZeroUsize::new(8).unwrap(),
            NonZeroUsize::new(2).unwrap(),
            NonZeroUsize::new(2).unwrap(),
        )
    }

    fn completion_request() -> QueryExecutionRequest {
        let plan = native_preparation_plan(NativePreparationFixture::MissingResultOutput).unwrap();
        let description =
            FrozenExecutionDescription::try_freeze(FrozenExecutionDescriptionDraft::new(
                crate::api::QueryExecutionKind::Maintenance,
                SealedPreparationPlan::seal(plan),
                None,
                super::super::ExecutionEffect::None,
                RecoveryMode::NoRecovery,
                Vec::new(),
                FrozenCostEstimate::unknown(FrozenEstimateUnknownReason::NotProjected),
                ExecutionResourceRequirements::unknown(FrozenEstimateUnknownReason::NotProjected),
            ))
            .unwrap();
        QueryExecutionRequest::from_frozen_description(description)
    }

    fn governance() -> (WorkloadControl, RootWork) {
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
        let root = control
            .try_begin_root(WorkRequest::new(WorkClass::Query))
            .unwrap();
        (control, root)
    }

    fn acknowledge_all_control(control: &WorkloadControl) {
        while let Some(permit) = control.next_control() {
            permit.acknowledge();
        }
    }

    #[derive(Debug)]
    struct FailingActivationNativePort {
        opened_execution_low: Arc<AtomicU64>,
        activated: Arc<AtomicBool>,
        residual_converged: Arc<AtomicBool>,
    }

    impl LogicalExecutionNativePort for FailingActivationNativePort {
        fn open(&self, request: LogicalNativeOpenRequest) -> LogicalNativeOpenFuture {
            let execution = request.initial_execution();
            self.opened_execution_low
                .store(execution.query_id().low() as u64, Ordering::SeqCst);
            let backend = BackendProcessId::new_v7();
            let attempts = FailingActivationPreparationPort {
                backend,
                activated: Arc::clone(&self.activated),
                residual_converged: Arc::clone(&self.residual_converged),
            };
            Box::pin(async move {
                request
                    .bind(
                        PermanentlyBackpressuredAbortEffectPort::shared(),
                        None,
                        attempts,
                    )
                    .map_err(Into::into)
            })
        }
    }

    #[derive(Debug)]
    struct FailingActivationPreparationPort {
        backend: BackendProcessId,
        activated: Arc<AtomicBool>,
        residual_converged: Arc<AtomicBool>,
    }

    impl NativeAttemptPreparationPort for FailingActivationPreparationPort {
        fn prepare(
            &mut self,
            request: NativeAttemptPreparationRequest,
        ) -> NativeAttemptPreparationFuture {
            let backend = self.backend;
            let dormant = FailingDormantOwner {
                activated: Arc::clone(&self.activated),
                residual_converged: Arc::clone(&self.residual_converged),
            };
            Box::pin(async move {
                request
                    .bind(vec![backend], Vec::new(), dormant)
                    .map_err(Into::into)
            })
        }
    }

    #[derive(Debug)]
    struct FailingDormantOwner {
        activated: Arc<AtomicBool>,
        residual_converged: Arc<AtomicBool>,
    }

    impl DormantNativeAttemptOwner for FailingDormantOwner {
        fn activate<'a>(
            &'a mut self,
            _schedule: &'a AttemptSchedule,
            _cancellation: novarocks_workload_control::CancellationView,
        ) -> NativeAttemptActivationFuture<'a> {
            self.activated.store(true, Ordering::SeqCst);
            Box::pin(async move {
                Err(NativeAttemptActivationFailure::new(
                    NativeAttemptPreparationFailure::new(
                        AttemptFailureClass::ExecutionFailure,
                        QueryExecutionError::new(
                            QueryExecutionErrorKind::Failed,
                            "injected Native activation failure",
                        ),
                    ),
                ))
            })
        }

        fn converge<'a>(
            &'a mut self,
            _cancellation: novarocks_workload_control::CancellationView,
        ) -> NativeAttemptConvergenceFuture<'a> {
            Box::pin(async move {
                self.residual_converged.store(true, Ordering::SeqCst);
            })
        }
    }

    fn failing_native() -> (
        Arc<dyn LogicalExecutionNativePort>,
        Arc<AtomicU64>,
        Arc<AtomicBool>,
        Arc<AtomicBool>,
    ) {
        let opened = Arc::new(AtomicU64::new(0));
        let activated = Arc::new(AtomicBool::new(false));
        let residual = Arc::new(AtomicBool::new(false));
        (
            Arc::new(FailingActivationNativePort {
                opened_execution_low: Arc::clone(&opened),
                activated: Arc::clone(&activated),
                residual_converged: Arc::clone(&residual),
            }),
            opened,
            activated,
            residual,
        )
    }

    #[tokio::test]
    async fn activation_failure_retains_residual_and_retires_exact_registry_entry() {
        let (native, opened, activated, residual) = failing_native();
        let (mut supervisor, client) = LogicalExecutionSupervisor::new(
            Handle::current(),
            native,
            QueryProcessNamespace::new(0x45),
            FrontendProcessId::new_v7(),
            supervisor_config(4),
        );
        let (control, root) = governance();
        let scope = root.owner.scope();

        let error = match client.start(completion_request(), root.owner).await {
            Ok(_) => panic!("activation failure must not return an execution handle"),
            Err(error) => error,
        };
        assert_eq!(error.message(), "injected Native activation failure");
        root.business.release();
        supervisor
            .shutdown_until(Instant::now() + Duration::from_secs(1))
            .await
            .unwrap();
        acknowledge_all_control(&control);
        scope.wait_released().await;
        assert_ne!(opened.load(Ordering::SeqCst), 0);
        assert!(activated.load(Ordering::SeqCst));
        assert!(residual.load(Ordering::SeqCst));
        assert_eq!(control.snapshot().root_responsibilities, 0);
    }

    #[tokio::test]
    async fn dropping_an_accepted_reply_does_not_drop_its_work_owner() {
        let (native, _opened, activated, residual) = failing_native();
        let (mut supervisor, client) = LogicalExecutionSupervisor::new(
            Handle::current(),
            native,
            QueryProcessNamespace::new(0x46),
            FrontendProcessId::new_v7(),
            supervisor_config(4),
        );
        let (control, root) = governance();
        let scope = root.owner.scope();
        let future = client.start(completion_request(), root.owner);
        drop(future);
        root.business.release();

        tokio::time::timeout(Duration::from_secs(1), async {
            while !activated.load(Ordering::SeqCst) {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("accepted request continues after its reply receiver is dropped");

        supervisor
            .shutdown_until(Instant::now() + Duration::from_secs(1))
            .await
            .unwrap();
        acknowledge_all_control(&control);
        scope.wait_released().await;
        assert!(activated.load(Ordering::SeqCst));
        assert!(residual.load(Ordering::SeqCst));
    }

    #[derive(Clone, Copy, Debug, Eq, PartialEq)]
    enum AttemptPanicPhase {
        Activate,
        Run,
        Converge,
    }

    #[derive(Debug)]
    struct PanickingAttemptNativePort {
        phase: AttemptPanicPhase,
        convergence_calls: Arc<AtomicU64>,
    }

    impl LogicalExecutionNativePort for PanickingAttemptNativePort {
        fn open(&self, request: LogicalNativeOpenRequest) -> LogicalNativeOpenFuture {
            let backend = BackendProcessId::new_v7();
            let attempts = PanickingAttemptPreparationPort {
                backend,
                phase: self.phase,
                convergence_calls: Arc::clone(&self.convergence_calls),
            };
            Box::pin(async move {
                request
                    .bind(
                        PermanentlyBackpressuredAbortEffectPort::shared(),
                        None,
                        attempts,
                    )
                    .map_err(Into::into)
            })
        }
    }

    #[derive(Debug)]
    struct PanickingAttemptPreparationPort {
        backend: BackendProcessId,
        phase: AttemptPanicPhase,
        convergence_calls: Arc<AtomicU64>,
    }

    impl NativeAttemptPreparationPort for PanickingAttemptPreparationPort {
        fn prepare(
            &mut self,
            request: NativeAttemptPreparationRequest,
        ) -> NativeAttemptPreparationFuture {
            let backend = self.backend;
            let owner = PanickingDormantOwner {
                phase: self.phase,
                convergence_calls: Arc::clone(&self.convergence_calls),
            };
            Box::pin(async move {
                request
                    .bind(vec![backend], Vec::new(), owner)
                    .map_err(Into::into)
            })
        }
    }

    #[derive(Debug)]
    struct PanickingDormantOwner {
        phase: AttemptPanicPhase,
        convergence_calls: Arc<AtomicU64>,
    }

    impl DormantNativeAttemptOwner for PanickingDormantOwner {
        fn activate<'a>(
            &'a mut self,
            _schedule: &'a AttemptSchedule,
            _cancellation: novarocks_workload_control::CancellationView,
        ) -> NativeAttemptActivationFuture<'a> {
            match self.phase {
                AttemptPanicPhase::Activate => {
                    Box::pin(async { panic!("injected Native activation panic") })
                }
                AttemptPanicPhase::Run | AttemptPanicPhase::Converge => {
                    let convergence_calls = Arc::clone(&self.convergence_calls);
                    let phase = self.phase;
                    Box::pin(async move {
                        Ok(ActivatedNativeAttempt::new(PanickingActiveOwner {
                            phase,
                            convergence_calls,
                        }))
                    })
                }
            }
        }

        fn converge<'a>(
            &'a mut self,
            _cancellation: novarocks_workload_control::CancellationView,
        ) -> NativeAttemptConvergenceFuture<'a> {
            Box::pin(async move {
                self.convergence_calls.fetch_add(1, Ordering::SeqCst);
            })
        }
    }

    #[derive(Debug)]
    struct PanickingActiveOwner {
        phase: AttemptPanicPhase,
        convergence_calls: Arc<AtomicU64>,
    }

    impl ActiveNativeAttemptOwner for PanickingActiveOwner {
        fn run<'a>(
            &'a mut self,
            _drive: &'a NativeAttemptDrive,
            _cancellation: novarocks_workload_control::CancellationView,
        ) -> NativeAttemptRunFuture<'a> {
            match self.phase {
                AttemptPanicPhase::Run => Box::pin(async { panic!("injected Native run panic") }),
                AttemptPanicPhase::Converge => Box::pin(async {
                    NativeAttemptTerminal::Failed(NativeAttemptPreparationFailure::new(
                        AttemptFailureClass::ExecutionFailure,
                        QueryExecutionError::new(
                            QueryExecutionErrorKind::Failed,
                            "injected Native attempt failure",
                        ),
                    ))
                }),
                AttemptPanicPhase::Activate => unreachable!("activation panic has no active owner"),
            }
        }

        fn converge<'a>(
            &'a mut self,
            _cancellation: novarocks_workload_control::CancellationView,
        ) -> NativeAttemptConvergenceFuture<'a> {
            Box::pin(async move {
                let previous = self.convergence_calls.fetch_add(1, Ordering::SeqCst);
                if self.phase == AttemptPanicPhase::Converge && previous == 0 {
                    panic!("injected Native convergence panic");
                }
            })
        }
    }

    async fn assert_attempt_panic_converges(
        phase: AttemptPanicPhase,
        namespace: u64,
        expected_message: &'static str,
    ) {
        let convergence_calls = Arc::new(AtomicU64::new(0));
        let (mut supervisor, client) = LogicalExecutionSupervisor::new(
            Handle::current(),
            Arc::new(PanickingAttemptNativePort {
                phase,
                convergence_calls: Arc::clone(&convergence_calls),
            }),
            QueryProcessNamespace::new(namespace),
            FrontendProcessId::new_v7(),
            supervisor_config(4),
        );
        let (control, root) = governance();
        let scope = root.owner.scope();

        let error = match client.start(completion_request(), root.owner).await {
            Ok(_) => panic!("a panicking Native attempt must not return an execution handle"),
            Err(error) => error,
        };
        assert_eq!(error.message(), expected_message);
        root.business.release();
        let shutdown = supervisor
            .shutdown_until(Instant::now() + Duration::from_secs(1))
            .await;
        assert!(matches!(
            shutdown,
            Err(LogicalExecutionSupervisorShutdownError::SupervisorFailed(_))
        ));
        assert_eq!(convergence_calls.load(Ordering::SeqCst), 1);
        acknowledge_all_control(&control);
        scope.wait_released().await;
        assert_eq!(control.snapshot().root_responsibilities, 0);
    }

    #[tokio::test]
    async fn activation_future_panic_retains_owner_for_convergence() {
        assert_attempt_panic_converges(
            AttemptPanicPhase::Activate,
            0x4b,
            "Native attempt activation panicked",
        )
        .await;
    }

    #[tokio::test]
    async fn run_future_panic_retains_owner_for_convergence() {
        assert_attempt_panic_converges(AttemptPanicPhase::Run, 0x4c, "Native attempt run panicked")
            .await;
    }

    #[tokio::test]
    async fn convergence_poll_panic_retries_without_losing_the_active_owner() {
        let convergence_calls = Arc::new(AtomicU64::new(0));
        let (mut supervisor, client) = LogicalExecutionSupervisor::new(
            Handle::current(),
            Arc::new(PanickingAttemptNativePort {
                phase: AttemptPanicPhase::Converge,
                convergence_calls: Arc::clone(&convergence_calls),
            }),
            QueryProcessNamespace::new(0x50),
            FrontendProcessId::new_v7(),
            supervisor_config(4),
        );
        let (control, root) = governance();
        let scope = root.owner.scope();

        let error = match client.start(completion_request(), root.owner).await {
            Ok(_) => panic!("a failed Native attempt must not return an execution handle"),
            Err(error) => error,
        };
        assert_eq!(error.message(), "injected Native attempt failure");
        root.business.release();
        supervisor
            .shutdown_until(Instant::now() + Duration::from_secs(1))
            .await
            .unwrap();
        assert_eq!(convergence_calls.load(Ordering::SeqCst), 2);
        acknowledge_all_control(&control);
        scope.wait_released().await;
    }

    #[derive(Debug)]
    struct TestContent(u8);

    impl CodecOwnedContent for TestContent {
        fn fingerprint(&self) -> ContentFingerprint {
            ContentFingerprint::from_bytes([self.0; 16])
        }

        fn encoded_len(&self) -> usize {
            1
        }
    }

    #[derive(Debug)]
    struct TestSecret;

    impl ConfidentialContent for TestSecret {
        fn encoded_len(&self) -> usize {
            1
        }

        fn matches(&self, other: &dyn ConfidentialContent) -> bool {
            other.encoded_len() == 1
        }
    }

    #[derive(Debug)]
    struct ImmediateEstablishTransport;

    impl EstablishTransportSink for ImmediateEstablishTransport {
        fn try_reserve(&self, _identity: EstablishIssueIdentity) -> EstablishTransportAdmission {
            EstablishTransportAdmission::Admitted(Box::new(ImmediateEstablishReservation))
        }
    }

    #[derive(Debug)]
    struct ImmediateEstablishReservation;

    impl EstablishTransportReservation for ImmediateEstablishReservation {
        fn submit(self: Box<Self>, submission: EstablishTransportSubmission) {
            submission
                .worker_settled(OperationOutcome::Accepted)
                .expect("test transport publishes exact Establish success");
        }
    }

    #[derive(Debug)]
    struct SuccessfulCompletionNativePort {
        backend: BackendProcessId,
        expected_frontend: FrontendProcessId,
    }

    impl LogicalExecutionNativePort for SuccessfulCompletionNativePort {
        fn open(&self, request: LogicalNativeOpenRequest) -> LogicalNativeOpenFuture {
            let attempts = SuccessfulCompletionPreparationPort {
                backend: self.backend,
                expected_frontend: self.expected_frontend,
            };
            Box::pin(async move {
                request
                    .bind(
                        PermanentlyBackpressuredAbortEffectPort::shared(),
                        None,
                        attempts,
                    )
                    .map_err(Into::into)
            })
        }
    }

    #[derive(Debug)]
    struct SuccessfulCompletionPreparationPort {
        backend: BackendProcessId,
        expected_frontend: FrontendProcessId,
    }

    impl NativeAttemptPreparationPort for SuccessfulCompletionPreparationPort {
        fn prepare(
            &mut self,
            request: NativeAttemptPreparationRequest,
        ) -> NativeAttemptPreparationFuture {
            let backend = self.backend;
            let expected_frontend = self.expected_frontend;
            Box::pin(async move {
                request
                    .bind(
                        vec![backend],
                        Vec::new(),
                        SuccessfulCompletionDormantOwner { expected_frontend },
                    )
                    .map_err(Into::into)
            })
        }
    }

    #[derive(Debug)]
    struct SuccessfulCompletionDormantOwner {
        expected_frontend: FrontendProcessId,
    }

    impl DormantNativeAttemptOwner for SuccessfulCompletionDormantOwner {
        fn activate<'a>(
            &'a mut self,
            schedule: &'a AttemptSchedule,
            _cancellation: novarocks_workload_control::CancellationView,
        ) -> NativeAttemptActivationFuture<'a> {
            assert!(
                schedule
                    .contexts()
                    .iter()
                    .all(|context| context.frontend_process_id() == self.expected_frontend)
            );
            let contexts = schedule.contexts().to_vec();
            Box::pin(async move {
                Ok(ActivatedNativeAttempt::new(
                    SuccessfulCompletionActiveOwner { contexts },
                ))
            })
        }

        fn converge<'a>(
            &'a mut self,
            _cancellation: novarocks_workload_control::CancellationView,
        ) -> NativeAttemptConvergenceFuture<'a> {
            Box::pin(async {})
        }
    }

    #[derive(Debug)]
    struct SuccessfulCompletionActiveOwner {
        contexts: Vec<QueryContextRef>,
    }

    impl ActiveNativeAttemptOwner for SuccessfulCompletionActiveOwner {
        fn run<'a>(
            &'a mut self,
            drive: &'a NativeAttemptDrive,
            _cancellation: novarocks_workload_control::CancellationView,
        ) -> NativeAttemptRunFuture<'a> {
            Box::pin(async move {
                let valid_for = LeaseValidFor::new(Duration::from_secs(30)).unwrap();
                for (index, &context) in self.contexts.iter().enumerate() {
                    let tag = u8::try_from(index + 1).unwrap();
                    let compatibility = NativeCompatibilityId::new([tag; 32]);
                    let issue = drive
                        .begin_admission_issue(
                            novarocks_execution_contract::AcquireQueryContextAdmissionTicket::new(
                                TaskOperationId::new_v7(),
                                context,
                                valid_for,
                                compatibility,
                                AdmissionEpochCapability::try_from_bytes([tag; 16]).unwrap(),
                            ),
                        )
                        .await
                        .unwrap();
                    let ticket = QueryContextAdmissionTicketReceipt::new(
                        AdmissionTicketId::try_from_bytes([tag; 16]).unwrap(),
                        context,
                        valid_for,
                    );
                    drive
                        .settle_admission_issue(
                            issue,
                            AdmissionIssueSettlement::applied(
                                issue.operation_id(),
                                OperationOutcome::Accepted,
                                ticket,
                            )
                            .unwrap(),
                        )
                        .await
                        .unwrap();
                    let content = |offset: u8| {
                        Arc::new(TestContent(tag + offset)) as Arc<dyn CodecOwnedContent>
                    };
                    let establish = Arc::new(EstablishQueryContext::new(
                        TaskOperationId::new_v7(),
                        context,
                        ticket.ticket_id(),
                        content(0),
                        content(1),
                        content(2),
                        CredentialUpdate::new(
                            CredentialLeaseId::new(u64::from(tag)),
                            CredentialEpoch::new(1).unwrap(),
                            Arc::new(TestSecret),
                        ),
                        valid_for,
                    ));
                    let permit = drive
                        .authorize_establish(establish, compatibility)
                        .await
                        .unwrap();
                    assert_eq!(
                        permit.try_submit(&ImmediateEstablishTransport).unwrap(),
                        EstablishIssueSubmit::Accepted
                    );
                }
                tokio::task::yield_now().await;
                NativeAttemptTerminal::Completed
            })
        }

        fn converge<'a>(
            &'a mut self,
            _cancellation: novarocks_workload_control::CancellationView,
        ) -> NativeAttemptConvergenceFuture<'a> {
            Box::pin(async {})
        }
    }

    #[tokio::test]
    async fn completion_becomes_visible_only_after_exact_establish_success() {
        let frontend = FrontendProcessId::new_v7();
        let (mut supervisor, client) = LogicalExecutionSupervisor::new(
            Handle::current(),
            Arc::new(SuccessfulCompletionNativePort {
                backend: BackendProcessId::new_v7(),
                expected_frontend: frontend,
            }),
            QueryProcessNamespace::new(0x50),
            frontend,
            supervisor_config(4),
        );
        let (control, root) = governance();
        let scope = root.owner.scope();
        let mut handle = client
            .start(completion_request(), root.owner)
            .await
            .expect("exact Establish success permits completion visibility");
        assert!(matches!(
            handle.take_output(),
            Some(crate::api::ExecutionOutput::Completion)
        ));
        root.business.release();
        supervisor
            .shutdown_until(Instant::now() + Duration::from_secs(1))
            .await
            .unwrap();
        acknowledge_all_control(&control);
        scope.wait_released().await;
    }

    #[derive(Debug)]
    struct PrematureCompletionNativePort;

    impl LogicalExecutionNativePort for PrematureCompletionNativePort {
        fn open(&self, request: LogicalNativeOpenRequest) -> LogicalNativeOpenFuture {
            let backend = BackendProcessId::new_v7();
            Box::pin(async move {
                request
                    .bind(
                        PermanentlyBackpressuredAbortEffectPort::shared(),
                        None,
                        PrematureCompletionPreparationPort { backend },
                    )
                    .map_err(Into::into)
            })
        }
    }

    #[derive(Debug)]
    struct PrematureCompletionPreparationPort {
        backend: BackendProcessId,
    }

    impl NativeAttemptPreparationPort for PrematureCompletionPreparationPort {
        fn prepare(
            &mut self,
            request: NativeAttemptPreparationRequest,
        ) -> NativeAttemptPreparationFuture {
            let backend = self.backend;
            Box::pin(async move {
                request
                    .bind(vec![backend], Vec::new(), PrematureCompletionDormant)
                    .map_err(Into::into)
            })
        }
    }

    #[derive(Debug)]
    struct PrematureCompletionDormant;

    impl DormantNativeAttemptOwner for PrematureCompletionDormant {
        fn activate<'a>(
            &'a mut self,
            _schedule: &'a AttemptSchedule,
            _cancellation: novarocks_workload_control::CancellationView,
        ) -> NativeAttemptActivationFuture<'a> {
            Box::pin(async { Ok(ActivatedNativeAttempt::new(PrematureCompletionActive)) })
        }

        fn converge<'a>(
            &'a mut self,
            _cancellation: novarocks_workload_control::CancellationView,
        ) -> NativeAttemptConvergenceFuture<'a> {
            Box::pin(async {})
        }
    }

    #[derive(Debug)]
    struct PrematureCompletionActive;

    impl ActiveNativeAttemptOwner for PrematureCompletionActive {
        fn run<'a>(
            &'a mut self,
            _drive: &'a NativeAttemptDrive,
            _cancellation: novarocks_workload_control::CancellationView,
        ) -> NativeAttemptRunFuture<'a> {
            Box::pin(async { NativeAttemptTerminal::Completed })
        }

        fn converge<'a>(
            &'a mut self,
            _cancellation: novarocks_workload_control::CancellationView,
        ) -> NativeAttemptConvergenceFuture<'a> {
            Box::pin(async {})
        }
    }

    #[tokio::test]
    async fn completion_is_not_visible_without_exact_establish_success() {
        let (mut supervisor, client) = LogicalExecutionSupervisor::new(
            Handle::current(),
            Arc::new(PrematureCompletionNativePort),
            QueryProcessNamespace::new(0x4a),
            FrontendProcessId::new_v7(),
            supervisor_config(4),
        );
        let (control, root) = governance();
        let scope = root.owner.scope();
        let error = match client.start(completion_request(), root.owner).await {
            Ok(_) => panic!("Native completion without Establish proof must fail closed"),
            Err(error) => error,
        };
        assert_eq!(error.kind(), QueryExecutionErrorKind::Failed);
        root.business.release();
        let shutdown = supervisor
            .shutdown_until(Instant::now() + Duration::from_secs(1))
            .await;
        assert!(matches!(
            shutdown,
            Err(LogicalExecutionSupervisorShutdownError::SupervisorFailed(_))
        ));
        acknowledge_all_control(&control);
        scope.wait_released().await;
    }

    #[tokio::test]
    async fn full_and_closed_start_admission_complete_rejected_work() {
        let (native, ..) = failing_native();
        let (mut supervisor, client) = LogicalExecutionSupervisor::new(
            Handle::current(),
            native,
            QueryProcessNamespace::new(0x48),
            FrontendProcessId::new_v7(),
            supervisor_config(1),
        );
        let (first_control, first) = governance();
        let first_scope = first.owner.scope();
        let accepted = client.start(completion_request(), first.owner);
        let (second_control, second) = governance();
        let second_scope = second.owner.scope();
        let second_cancellation = second_scope.cancellation().unwrap();
        let rejected = client.start(completion_request(), second.owner);
        let error = match rejected.await {
            Ok(_) => panic!("a full start mailbox must reject synchronously"),
            Err(error) => error,
        };
        assert_eq!(error.kind(), QueryExecutionErrorKind::Rejected);
        assert!(
            second_control.next_control().is_none(),
            "full admission rejection must not manufacture cancellation work"
        );
        assert_eq!(second_cancellation.reason(), None);
        second.business.release();
        second_scope.wait_released().await;

        supervisor
            .shutdown_until(Instant::now() + Duration::from_secs(1))
            .await
            .unwrap();
        let first_error = match accepted.await {
            Ok(_) => panic!("queued request must receive an explicit shutdown verdict"),
            Err(error) => error,
        };
        assert_eq!(first_error.kind(), QueryExecutionErrorKind::Rejected);
        assert!(
            first_control.next_control().is_none(),
            "accepted but unstarted shutdown rejection must not manufacture cancellation work"
        );
        assert_eq!(first_scope.cancellation().unwrap().reason(), None);
        first.business.release();
        first_scope.wait_released().await;

        let (closed_control, closed) = governance();
        let closed_scope = closed.owner.scope();
        let closed_cancellation = closed_scope.cancellation().unwrap();
        let closed_error = match client.start(completion_request(), closed.owner).await {
            Ok(_) => panic!("a closed supervisor must reject new work"),
            Err(error) => error,
        };
        assert_eq!(closed_error.kind(), QueryExecutionErrorKind::Rejected);
        assert!(
            closed_control.next_control().is_none(),
            "closed admission rejection must not manufacture cancellation work"
        );
        assert_eq!(closed_cancellation.reason(), None);
        closed.business.release();
        closed_scope.wait_released().await;
    }

    #[derive(Debug)]
    struct PanickingOpenNativePort;

    impl LogicalExecutionNativePort for PanickingOpenNativePort {
        fn open(&self, _request: LogicalNativeOpenRequest) -> LogicalNativeOpenFuture {
            Box::pin(async { panic!("injected Native open panic") })
        }
    }

    #[derive(Debug)]
    struct IsolatedTaskPanicNativePort {
        opens: AtomicU64,
        first_opened: Arc<AtomicBool>,
        second_opened: Arc<AtomicBool>,
        second_gate: Arc<tokio::sync::Notify>,
        backend: BackendProcessId,
        expected_frontend: FrontendProcessId,
    }

    impl LogicalExecutionNativePort for IsolatedTaskPanicNativePort {
        fn open(&self, request: LogicalNativeOpenRequest) -> LogicalNativeOpenFuture {
            if self.opens.fetch_add(1, Ordering::SeqCst) == 0 {
                self.first_opened.store(true, Ordering::SeqCst);
                return Box::pin(async { panic!("injected isolated logical task panic") });
            }
            let attempts = SuccessfulCompletionPreparationPort {
                backend: self.backend,
                expected_frontend: self.expected_frontend,
            };
            self.second_opened.store(true, Ordering::SeqCst);
            let gate = Arc::clone(&self.second_gate);
            Box::pin(async move {
                gate.notified().await;
                request
                    .bind(
                        PermanentlyBackpressuredAbortEffectPort::shared(),
                        None,
                        attempts,
                    )
                    .map_err(Into::into)
            })
        }
    }

    #[tokio::test]
    async fn one_logical_task_panic_does_not_cancel_or_stop_another_execution() {
        let frontend = FrontendProcessId::new_v7();
        let first_opened = Arc::new(AtomicBool::new(false));
        let second_opened = Arc::new(AtomicBool::new(false));
        let second_gate = Arc::new(tokio::sync::Notify::new());
        let native = Arc::new(IsolatedTaskPanicNativePort {
            opens: AtomicU64::new(0),
            first_opened: Arc::clone(&first_opened),
            second_opened: Arc::clone(&second_opened),
            second_gate: Arc::clone(&second_gate),
            backend: BackendProcessId::new_v7(),
            expected_frontend: frontend,
        });
        let (mut supervisor, client) = LogicalExecutionSupervisor::new(
            Handle::current(),
            native,
            QueryProcessNamespace::new(0x51),
            frontend,
            supervisor_config(4),
        );
        let (first_control, first) = governance();
        let first_scope = first.owner.scope();
        let first_cancellation = first_scope.cancellation().unwrap();
        let first_result = client.start(completion_request(), first.owner);
        tokio::time::timeout(Duration::from_secs(1), async {
            while !first_opened.load(Ordering::SeqCst) {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("the first logical task reached its isolated panic");

        let (second_control, second) = governance();
        let second_scope = second.owner.scope();
        let second_cancellation = second_scope.cancellation().unwrap();
        let second_result = client.start(completion_request(), second.owner);

        let first_error = match first_result.await {
            Ok(_) => panic!("the panicking logical task must fail its own request"),
            Err(error) => error,
        };
        assert_eq!(first_error.kind(), QueryExecutionErrorKind::Failed);
        tokio::time::timeout(Duration::from_secs(1), async {
            while !second_opened.load(Ordering::SeqCst) {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("the sibling logical task remains admitted after the panic");
        assert_eq!(first_cancellation.reason(), None);
        assert_eq!(second_cancellation.reason(), None);
        assert!(first_control.next_control().is_none());
        assert!(second_control.next_control().is_none());

        second_gate.notify_one();
        let mut second_handle = second_result
            .await
            .expect("a sibling execution remains supervised after the panic");
        assert!(matches!(
            second_handle.take_output(),
            Some(crate::api::ExecutionOutput::Completion)
        ));
        assert_eq!(first_cancellation.reason(), None);
        assert!(first_control.next_control().is_none());

        first.business.release();
        second.business.release();
        assert!(matches!(
            supervisor
                .shutdown_until(Instant::now() + Duration::from_secs(1))
                .await,
            Err(LogicalExecutionSupervisorShutdownError::SupervisorFailed(_))
        ));
        assert_eq!(first_cancellation.reason(), None);
        acknowledge_all_control(&first_control);
        acknowledge_all_control(&second_control);
        first_scope.wait_released().await;
        second_scope.wait_released().await;
    }

    #[tokio::test]
    async fn logical_task_panic_completes_uninstalled_work_and_drains_supervisor() {
        let (mut supervisor, client) = LogicalExecutionSupervisor::new(
            Handle::current(),
            Arc::new(PanickingOpenNativePort),
            QueryProcessNamespace::new(0x49),
            FrontendProcessId::new_v7(),
            supervisor_config(4),
        );
        let (control, root) = governance();
        let scope = root.owner.scope();
        let start = client.start(completion_request(), root.owner);
        let error = match start.await {
            Ok(_) => panic!("panicking Native open must not return an execution handle"),
            Err(error) => error,
        };
        assert_eq!(error.kind(), QueryExecutionErrorKind::Failed);
        root.business.release();
        let shutdown = supervisor
            .shutdown_until(Instant::now() + Duration::from_secs(1))
            .await;
        assert!(matches!(
            shutdown,
            Err(LogicalExecutionSupervisorShutdownError::SupervisorFailed(_))
        ));
        acknowledge_all_control(&control);
        scope.wait_released().await;
        assert_eq!(control.snapshot().root_responsibilities, 0);
    }

    #[derive(Debug)]
    struct DelayedShutdownNativePort;

    impl LogicalExecutionNativePort for DelayedShutdownNativePort {
        fn open(&self, request: LogicalNativeOpenRequest) -> LogicalNativeOpenFuture {
            let cancellation = request.cancellation();
            Box::pin(async move {
                cancellation.cancelled().await;
                tokio::time::sleep(Duration::from_millis(25)).await;
                Err(
                    LogicalNativeOpenFailure::cancelled("server shutdown interrupted Native open")
                        .into(),
                )
            })
        }
    }

    #[tokio::test]
    async fn shutdown_deadline_keeps_the_same_supervisor_join_for_retry() {
        let (mut supervisor, client) = LogicalExecutionSupervisor::new(
            Handle::current(),
            Arc::new(DelayedShutdownNativePort),
            QueryProcessNamespace::new(0x47),
            FrontendProcessId::new_v7(),
            supervisor_config(4),
        );
        let (control, root) = governance();
        let scope = root.owner.scope();
        let start = client.start(completion_request(), root.owner);
        tokio::task::yield_now().await;

        assert_eq!(
            supervisor.shutdown_until(Instant::now()).await,
            Err(LogicalExecutionSupervisorShutdownError::DeadlineExceeded)
        );
        let error = match start.await {
            Ok(_) => panic!("shutdown during Native open must not return an execution handle"),
            Err(error) => error,
        };
        assert_eq!(error.kind(), QueryExecutionErrorKind::Cancelled);
        root.business.release();
        supervisor
            .shutdown_until(Instant::now() + Duration::from_secs(1))
            .await
            .unwrap();
        acknowledge_all_control(&control);
        scope.wait_released().await;
    }

    #[test]
    fn query_sequence_is_not_reused_by_a_rebuilt_supervisor_allocator() {
        let namespace = QueryProcessNamespace::new(0x4e);
        let first = ProcessQueryIdAllocator::new(namespace)
            .first_execution()
            .unwrap();
        let rebuilt = ProcessQueryIdAllocator::new(namespace)
            .first_execution()
            .unwrap();

        assert_ne!(first, rebuilt);
    }

    #[tokio::test]
    async fn registry_shutdown_error_retains_owner_and_run_failure_for_retry() {
        let (mut supervisor, client) = LogicalExecutionSupervisor::new(
            Handle::current(),
            Arc::new(PanickingOpenNativePort),
            QueryProcessNamespace::new(0x4f),
            FrontendProcessId::new_v7(),
            supervisor_config(1),
        );
        let (_control, root) = governance();
        let scope = root.owner.scope();
        let error = match client.start(completion_request(), root.owner).await {
            Ok(_) => panic!("panicking Native open must fail the logical task"),
            Err(error) => error,
        };
        assert_eq!(error.kind(), QueryExecutionErrorKind::Failed);
        root.business.release();
        supervisor.inject_registry_shutdown_error_once(
            LogicalExecutionRuntimeRegistryError::JoinNotReady,
        );

        assert_eq!(
            supervisor
                .shutdown_until(Instant::now() + Duration::from_secs(1))
                .await,
            Err(LogicalExecutionSupervisorShutdownError::Registry(
                LogicalExecutionRuntimeRegistryError::JoinNotReady,
            ))
        );
        assert!(!supervisor.shutdown_complete);

        assert!(matches!(
            supervisor
                .shutdown_until(Instant::now() + Duration::from_secs(1))
                .await,
            Err(LogicalExecutionSupervisorShutdownError::SupervisorFailed(_))
        ));
        assert!(supervisor.shutdown_complete);
        scope.wait_released().await;
    }
}
