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

use std::collections::BTreeSet;
use std::fmt;
use std::future::Future;
use std::num::{NonZeroU32, NonZeroUsize};
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::task::Poll;
use std::time::{Duration, Instant};

use novarocks_execution_contract::{MaxWait, ResultByteLimit};
use novarocks_types::identity::{
    AttemptId, FrontendProcessId, LocalQuerySequence, QueryExecutionId, QueryIdAttribution,
    QueryProcessNamespace,
};
use novarocks_workload_control::{
    CancellationReason, LocalResourceAuthority, Stage, StageRequest, WorkError, WorkOwner,
};
use tokio::runtime::Handle;
use tokio::sync::{mpsc, oneshot, watch};
use tokio::task::{JoinHandle, JoinSet};

use crate::api::{
    ActiveNativeAttemptOwner, DormantNativeAttemptOwner, ExecutionHandle,
    LogicalExecutionNativePort, LogicalNativeOpenError, LogicalNativeOpenRequest,
    LogicalNativeSession, NativeAttemptConvergence, NativeAttemptPreparationError,
    NativeAttemptPreparationFailure, NativeAttemptTerminal, NativeContextConvergenceKind,
    NativeRowsAttemptRuntime, QueryExecutionClient, QueryExecutionDriver, QueryExecutionError,
    QueryExecutionErrorKind, QueryExecutionFuture, QueryExecutionRequest, ResultField,
    ResultSchema,
};
use crate::preparation::OutputContract;

use super::{
    LogicalExecutionActorConfig, LogicalExecutionRuntimeRegistry,
    LogicalExecutionRuntimeRegistryError, LogicalExecutionRuntimeRegistryHandle,
    LogicalExecutionRuntimeShutdownError, NativeAttemptDrive, RecoveryDecision, RecoveryInput,
    RecoveryMode, ResultPumpFailure, build_attempt_schedule, evaluate_recovery,
    run_root_result_pump,
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
    rows: LogicalExecutionRowsConfig,
}

/// Process-fixed bounds for every row-producing logical execution.
///
/// These limits are role composition facts. A frozen query description cannot
/// raise them, and the supervisor supplies the same bounds to each replacement
/// attempt.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct LogicalExecutionRowsConfig {
    delivery_capacity: NonZeroUsize,
    max_attempts: NonZeroU32,
    replacement_reservation_valid_for: Duration,
    fetch_max_wait: MaxWait,
    fetch_byte_limit: ResultByteLimit,
}

impl LogicalExecutionRowsConfig {
    pub const fn new(
        delivery_capacity: NonZeroUsize,
        max_attempts: NonZeroU32,
        replacement_reservation_valid_for: Duration,
        fetch_max_wait: MaxWait,
        fetch_byte_limit: ResultByteLimit,
    ) -> Self {
        Self {
            delivery_capacity,
            max_attempts,
            replacement_reservation_valid_for,
            fetch_max_wait,
            fetch_byte_limit,
        }
    }
}

impl LogicalExecutionSupervisorConfig {
    pub const fn new(
        start_capacity: NonZeroUsize,
        actor_mailbox_capacity: NonZeroUsize,
        max_admission_issues_per_context: NonZeroUsize,
        max_establish_authorizations_per_context: NonZeroUsize,
        rows: LogicalExecutionRowsConfig,
    ) -> Self {
        Self {
            start_capacity,
            actor_mailbox_capacity,
            max_admission_issues_per_context,
            max_establish_authorizations_per_context,
            rows,
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
        resources: LocalResourceAuthority,
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
            resources,
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

    /// Ends process-local ownership after bounded graceful convergence failed
    /// and the FE runner has irrevocably committed to process exit.
    ///
    /// This does not abort Tokio tasks. Their join handles are released only at
    /// the process boundary, where the runtime and every process-local task are
    /// about to cease together. Ordinary callers must keep using
    /// [`Self::shutdown_until`] and may not use this as a detach path.
    pub fn abandon_for_process_exit(&mut self) {
        self.shutdown.send_replace(true);
        self.join.take();
        self.run_outcome.take();
        self.registry.abandon_for_process_exit();
        self.shutdown_complete = true;
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
    resources: LocalResourceAuthority,
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
                    resources.clone(),
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
    resources: LocalResourceAuthority,
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
    let (description, native_seed) = request.into_parts();
    let description = Arc::new(description);
    let result_schema = match description.output() {
        OutputContract::Rows(columns) => Some(ResultSchema::new(
            columns
                .iter()
                .map(|column| {
                    ResultField::new(
                        column.name.clone(),
                        column.data_type.clone(),
                        column.nullable,
                        None,
                    )
                })
                .collect::<Vec<_>>(),
        )),
        OutputContract::CompletionOnly => None,
    };
    if result_schema.is_none() && description.recovery() != RecoveryMode::NoRecovery {
        return fail_uninstalled_start(
            pending_owner,
            reply,
            QueryExecutionError::new(
                QueryExecutionErrorKind::InvalidRequest,
                "completion-only logical execution cannot restart as a row attempt",
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
        native_seed,
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
    let actor_config = match (&result_schema, description.recovery()) {
        (None, RecoveryMode::NoRecovery) => LogicalExecutionActorConfig::no_recovery_completion(
            initial_execution,
            description.effect(),
            config.actor_mailbox_capacity,
            contexts,
            config.max_admission_issues_per_context,
            max_establish_authorizations,
            pending_owner.take(),
            stage,
        ),
        (Some(schema), RecoveryMode::NoRecovery) => {
            LogicalExecutionActorConfig::single_attempt_read_rows(
                initial_execution,
                config.actor_mailbox_capacity,
                contexts,
                config.max_admission_issues_per_context,
                max_establish_authorizations,
                pending_owner.take(),
                stage,
                schema.clone(),
                config.rows.delivery_capacity,
            )
        }
        (Some(schema), RecoveryMode::RestartAttemptBeforeVisibility) => {
            let replacement = session
                .replacement_effect_port()
                .ok_or_else(|| super::LogicalExecutionActorError::InvariantViolation);
            match replacement {
                Ok(replacement) => {
                    LogicalExecutionActorConfig::read_only_pre_visibility_recovery_rows(
                        initial_execution,
                        config.actor_mailbox_capacity,
                        contexts,
                        config.max_admission_issues_per_context,
                        max_establish_authorizations,
                        config.rows.max_attempts,
                        replacement,
                        pending_owner.take(),
                        stage,
                        config.rows.replacement_reservation_valid_for,
                        schema.clone(),
                        config.rows.delivery_capacity,
                    )
                }
                Err(error) => Err(error),
            }
        }
        (None, RecoveryMode::RestartAttemptBeforeVisibility) => {
            unreachable!("completion-only recovery was rejected before actor construction")
        }
    };
    let actor_config = match actor_config {
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
    let (mut active, rows_runtime) = active.into_parts();
    match (&result_schema, rows_runtime) {
        (Some(schema), Some(rows_runtime)) => {
            let running = match actor.activate(initial.ready()).await {
                Ok(running) => running,
                Err(error) => {
                    let error = actor_error(error);
                    let _ = requester.request(CancellationReason::Requested);
                    let convergence =
                        converge_active(active.as_mut(), cancellation, &mut shutdown, &requester)
                            .await;
                    drop(active);
                    let convergence_result = record_reported_active_convergence(
                        &registry,
                        &registration,
                        schedule.contexts(),
                        convergence,
                    )
                    .await;
                    let _ = reply.send(Err(error.clone()));
                    let retire_result = retire_logical(&registry, registration).await;
                    return first_supervision_error(
                        Err(error),
                        first_supervision_error(convergence_result, retire_result),
                    );
                }
            };
            let handle = ExecutionHandle::new(requester.clone(), output.into_output());
            let _ = reply.send(Ok(handle));
            let actor_result = supervise_rows(
                &actor,
                &mut session,
                Arc::clone(&description),
                resources,
                scope,
                frontend_process_id,
                config,
                &registry,
                &registration,
                &mut shutdown,
                &requester,
                RowsAttempt {
                    schedule,
                    active,
                    running,
                    runtime: rows_runtime,
                },
                schema.clone(),
            )
            .await;
            let retire_result = retire_logical(&registry, registration).await;
            return first_supervision_error(actor_result, retire_result);
        }
        (Some(_), None) => {
            let error = QueryExecutionError::new(
                QueryExecutionErrorKind::InvalidRequest,
                "Native activated a row execution without its root result runtime",
            );
            let actor_result = actor
                .initialization_failed(initial)
                .await
                .map(|_| ())
                .map_err(actor_error);
            let _ = requester.request(CancellationReason::Requested);
            let convergence =
                converge_active(active.as_mut(), cancellation, &mut shutdown, &requester).await;
            drop(active);
            let convergence_result = record_reported_active_convergence(
                &registry,
                &registration,
                schedule.contexts(),
                convergence,
            )
            .await;
            let _ = reply.send(Err(error.clone()));
            let retire_result = retire_logical(&registry, registration).await;
            return first_supervision_error(
                Err(error),
                first_supervision_error(
                    actor_result,
                    first_supervision_error(convergence_result, retire_result),
                ),
            );
        }
        (None, Some(_)) => {
            let error = QueryExecutionError::new(
                QueryExecutionErrorKind::InvalidRequest,
                "Native attached a root result runtime to a completion-only execution",
            );
            let actor_result = actor
                .initialization_failed(initial)
                .await
                .map(|_| ())
                .map_err(actor_error);
            let _ = requester.request(CancellationReason::Requested);
            let convergence =
                converge_active(active.as_mut(), cancellation, &mut shutdown, &requester).await;
            drop(active);
            let convergence_result = record_reported_active_convergence(
                &registry,
                &registration,
                schedule.contexts(),
                convergence,
            )
            .await;
            let _ = reply.send(Err(error.clone()));
            let retire_result = retire_logical(&registry, registration).await;
            return first_supervision_error(
                Err(error),
                first_supervision_error(
                    actor_result,
                    first_supervision_error(convergence_result, retire_result),
                ),
            );
        }
        (None, None) => {}
    }
    let running = match actor.activate(initial.ready()).await {
        Ok(running) => running,
        Err(error) => {
            let error = actor_error(error);
            let _ = requester.request(CancellationReason::Requested);
            let convergence =
                converge_active(active.as_mut(), cancellation, &mut shutdown, &requester).await;
            drop(active);
            let convergence_result = record_reported_active_convergence(
                &registry,
                &registration,
                schedule.contexts(),
                convergence,
            )
            .await;
            let _ = reply.send(Err(error.clone()));
            let retire_result = retire_logical(&registry, registration).await;
            return first_supervision_error(
                Err(error),
                first_supervision_error(convergence_result, retire_result),
            );
        }
    };
    let drive = NativeAttemptDrive::new(&running);
    let terminal = await_with_shutdown(
        catch_future_panic(active.run(&drive, cancellation.clone())),
        &mut shutdown,
        &requester,
    )
    .await;
    let (actor_result, convergence) = match terminal {
        Ok(NativeAttemptTerminal::Completed) => {
            let convergence =
                converge_active(active.as_mut(), cancellation, &mut shutdown, &requester).await;
            drop(active);
            let actor_result = match actor.complete_attempt(running).await {
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
            };
            (actor_result, convergence)
        }
        Ok(NativeAttemptTerminal::Failed(failure)) => {
            let client_error = failure.error().clone();
            let actor_result = actor
                .fail_attempt_with_error(running, client_error.clone())
                .await
                .map(|_| ())
                .map_err(actor_error);
            let _ = reply.send(Err(client_error));
            let convergence =
                converge_active(active.as_mut(), cancellation, &mut shutdown, &requester).await;
            drop(active);
            (actor_result, convergence)
        }
        Err(()) => {
            let error = native_future_panicked("Native attempt run panicked");
            let actor_result = actor
                .fail_attempt_with_error(running, error.clone())
                .await
                .map(|_| ())
                .map_err(actor_error);
            let _ = reply.send(Err(error.clone()));
            let convergence =
                converge_active(active.as_mut(), cancellation, &mut shutdown, &requester).await;
            drop(active);
            (
                first_supervision_error(Err(error), actor_result),
                convergence,
            )
        }
    };
    let convergence_result = record_reported_active_convergence(
        &registry,
        &registration,
        schedule.contexts(),
        convergence,
    )
    .await;
    let retire_result = retire_logical(&registry, registration).await;
    first_supervision_error(
        actor_result,
        first_supervision_error(convergence_result, retire_result),
    )
}

struct RowsAttempt {
    schedule: super::AttemptSchedule,
    active: Box<dyn ActiveNativeAttemptOwner>,
    running: super::RunningAttemptPermit,
    runtime: NativeRowsAttemptRuntime,
}

type ResidualRowsConvergence = JoinSet<(
    Box<[novarocks_execution_contract::QueryContextRef]>,
    NativeAttemptConvergence,
)>;

fn supervise_residual_rows_attempt(
    residuals: &mut ResidualRowsConvergence,
    mut active: Box<dyn ActiveNativeAttemptOwner>,
    contexts: Box<[novarocks_execution_contract::QueryContextRef]>,
    cancellation: novarocks_workload_control::CancellationView,
    mut shutdown: watch::Receiver<bool>,
    requester: novarocks_workload_control::WorkCancellationRequester,
) {
    residuals.spawn(async move {
        let convergence =
            converge_active(active.as_mut(), cancellation, &mut shutdown, &requester).await;
        (contexts, convergence)
    });
}

#[allow(clippy::too_many_arguments)]
async fn converge_rows_attempts(
    active: &mut dyn ActiveNativeAttemptOwner,
    active_contexts: &[novarocks_execution_contract::QueryContextRef],
    residuals: &mut ResidualRowsConvergence,
    cancellation: novarocks_workload_control::CancellationView,
    registry: &LogicalExecutionRuntimeRegistryHandle,
    registration: &super::LogicalExecutionRegistration,
    shutdown: &mut watch::Receiver<bool>,
    requester: &novarocks_workload_control::WorkCancellationRequester,
) -> Result<(), QueryExecutionError> {
    let active_convergence = converge_active(active, cancellation, shutdown, requester).await;
    let mut result = record_reported_active_convergence(
        registry,
        registration,
        active_contexts,
        active_convergence,
    )
    .await;
    while let Some(joined) = residuals.join_next().await {
        let convergence = match joined {
            Ok((contexts, convergence)) => {
                record_reported_active_convergence(registry, registration, &contexts, convergence)
                    .await
            }
            Err(error) => Err(QueryExecutionError::new(
                QueryExecutionErrorKind::Failed,
                format!("residual Native attempt convergence task failed: {error}"),
            )),
        };
        result = first_supervision_error(result, convergence);
    }
    result
}

#[allow(clippy::too_many_arguments)]
async fn drive_rows_attempt_until_pump_decision(
    active: &mut dyn ActiveNativeAttemptOwner,
    drive: &NativeAttemptDrive,
    cancellation: novarocks_workload_control::CancellationView,
    running: super::RunningAttemptPermit,
    root: novarocks_execution_contract::TaskIdentity,
    scope: novarocks_workload_control::WorkScope,
    resources: LocalResourceAuthority,
    schema: ResultSchema,
    runtime: NativeRowsAttemptRuntime,
    max_wait: MaxWait,
    fetch_byte_limit: ResultByteLimit,
    shutdown: &mut watch::Receiver<bool>,
    requester: &novarocks_workload_control::WorkCancellationRequester,
) -> (Result<super::LogicalConclusion, ResultPumpFailure>,) {
    let (terminal_sender, terminal_source) = super::native_attempt_terminal_channel();
    let mut terminal_sender = Some(terminal_sender);
    let native_run = catch_future_panic(active.run(drive, cancellation));
    let pump = run_root_result_pump(
        running,
        root,
        scope,
        resources,
        schema,
        runtime.binding,
        runtime.statuses,
        terminal_source,
        max_wait,
        fetch_byte_limit,
    );
    tokio::pin!(native_run);
    tokio::pin!(pump);
    let pump_result = loop {
        tokio::select! {
            biased;
            changed = shutdown.changed() => {
                if changed.is_err() || *shutdown.borrow() {
                    let _ = requester.request(CancellationReason::ServerShutdown);
                }
            }
            outcome = &mut native_run, if terminal_sender.is_some() => {
                let terminal = outcome.unwrap_or_else(|()| {
                    NativeAttemptTerminal::Failed(NativeAttemptPreparationFailure::new(
                        super::AttemptFailureClass::ExecutionFailure,
                        native_future_panicked("Native row attempt run panicked"),
                    ))
                });
                terminal_sender
                    .take()
                    .expect("Native terminal sender is present while run is polled")
                    .publish(terminal);
            }
            outcome = &mut pump => break outcome,
        }
    };
    (pump_result,)
}

#[allow(clippy::too_many_arguments)]
async fn supervise_rows(
    actor: &super::LogicalExecutionActor,
    session: &mut LogicalNativeSession,
    description: Arc<crate::preparation::FrozenExecutionDescription>,
    resources: LocalResourceAuthority,
    scope: novarocks_workload_control::WorkScope,
    frontend_process_id: FrontendProcessId,
    config: LogicalExecutionSupervisorConfig,
    registry: &LogicalExecutionRuntimeRegistryHandle,
    registration: &super::LogicalExecutionRegistration,
    shutdown: &mut watch::Receiver<bool>,
    requester: &novarocks_workload_control::WorkCancellationRequester,
    mut attempt: RowsAttempt,
    schema: ResultSchema,
) -> Result<(), QueryExecutionError> {
    let mut residuals = JoinSet::new();
    loop {
        let drive = NativeAttemptDrive::new(&attempt.running);
        let (pump_result,) = drive_rows_attempt_until_pump_decision(
            attempt.active.as_mut(),
            &drive,
            scope.cancellation().map_err(work_error)?,
            attempt.running,
            attempt.schedule.root(),
            scope.clone(),
            resources.clone(),
            schema.clone(),
            attempt.runtime,
            config.rows.fetch_max_wait,
            config.rows.fetch_byte_limit,
            shutdown,
            requester,
        )
        .await;
        drop(drive);

        match pump_result {
            Ok(super::LogicalConclusion::Succeeded) => {
                return converge_rows_attempts(
                    attempt.active.as_mut(),
                    attempt.schedule.contexts(),
                    &mut residuals,
                    scope.cancellation().map_err(work_error)?,
                    registry,
                    registration,
                    shutdown,
                    requester,
                )
                .await;
            }
            Ok(conclusion) => {
                let convergence = converge_rows_attempts(
                    attempt.active.as_mut(),
                    attempt.schedule.contexts(),
                    &mut residuals,
                    scope.cancellation().map_err(work_error)?,
                    registry,
                    registration,
                    shutdown,
                    requester,
                )
                .await;
                return first_supervision_error(
                    Err(QueryExecutionError::new(
                        QueryExecutionErrorKind::Failed,
                        format!("row execution concluded as {conclusion:?}"),
                    )),
                    convergence,
                );
            }
            Err(ResultPumpFailure::Concluded(failure)) => {
                let convergence = converge_rows_attempts(
                    attempt.active.as_mut(),
                    attempt.schedule.contexts(),
                    &mut residuals,
                    scope.cancellation().map_err(work_error)?,
                    registry,
                    registration,
                    shutdown,
                    requester,
                )
                .await;
                return if matches!(
                    failure.conclusion(),
                    super::LogicalConclusion::Cancelled | super::LogicalConclusion::Failed
                ) {
                    convergence
                } else {
                    first_supervision_error(Err(failure.error().clone()), convergence)
                };
            }
            Err(ResultPumpFailure::ActorOutcomeUnknown(failure)) => {
                let convergence = converge_rows_attempts(
                    attempt.active.as_mut(),
                    attempt.schedule.contexts(),
                    &mut residuals,
                    scope.cancellation().map_err(work_error)?,
                    registry,
                    registration,
                    shutdown,
                    requester,
                )
                .await;
                return first_supervision_error(Err(failure.error().clone()), convergence);
            }
            Err(ResultPumpFailure::DecisionPending(decision)) => {
                let cancellation = scope.cancellation().map_err(work_error)?;
                let attempts_started =
                    u32::try_from(attempt.schedule.execution().attempt_id().get())
                        .unwrap_or(u32::MAX);
                let recovery = evaluate_recovery(RecoveryInput {
                    mode: description.recovery(),
                    effect: description.effect(),
                    failure: decision.class(),
                    output_visible: false,
                    attempts_started,
                    max_attempts: config.rows.max_attempts.get(),
                    deadline_reached: matches!(
                        cancellation.reason(),
                        Some(CancellationReason::DeadlineExceeded)
                    ),
                });
                if recovery != RecoveryDecision::BeginReplacement {
                    let actor_result = decision
                        .fail_logical(actor)
                        .await
                        .map(|_| ())
                        .map_err(actor_error);
                    let convergence = converge_rows_attempts(
                        attempt.active.as_mut(),
                        attempt.schedule.contexts(),
                        &mut residuals,
                        cancellation,
                        registry,
                        registration,
                        shutdown,
                        requester,
                    )
                    .await;
                    return first_supervision_error(actor_result, convergence);
                }

                let replacement = match next_attempt(attempt.schedule.execution()) {
                    Ok(replacement) => replacement,
                    Err(error) => {
                        let actor_result = decision
                            .fail_logical(actor)
                            .await
                            .map(|_| ())
                            .map_err(actor_error);
                        let convergence = converge_rows_attempts(
                            attempt.active.as_mut(),
                            attempt.schedule.contexts(),
                            &mut residuals,
                            cancellation,
                            registry,
                            registration,
                            shutdown,
                            requester,
                        )
                        .await;
                        return first_supervision_error(
                            Err(error),
                            first_supervision_error(actor_result, convergence),
                        );
                    }
                };
                let prepared = prepare_native_attempt(
                    session,
                    replacement,
                    frontend_process_id,
                    description.scheduling(),
                    shutdown,
                    requester,
                )
                .await;
                let (replacement_schedule, mut dormant) = match prepared {
                    Ok(prepared) => prepared,
                    Err(error) => {
                        let actor_result = decision
                            .fail_logical(actor)
                            .await
                            .map(|_| ())
                            .map_err(actor_error);
                        let convergence = converge_rows_attempts(
                            attempt.active.as_mut(),
                            attempt.schedule.contexts(),
                            &mut residuals,
                            cancellation,
                            registry,
                            registration,
                            shutdown,
                            requester,
                        )
                        .await;
                        return first_supervision_error(
                            Err(error),
                            first_supervision_error(actor_result, convergence),
                        );
                    }
                };
                let qualification = match decision
                    .begin_replacement(actor, replacement, replacement_schedule.contexts().to_vec())
                    .await
                {
                    Ok((qualification, _)) => qualification,
                    Err(error) => {
                        converge_dormant(dormant.as_mut(), cancellation, shutdown, requester).await;
                        let replacement_convergence = record_active_convergence(
                            registry,
                            registration,
                            replacement_schedule.contexts(),
                        )
                        .await;
                        let old_convergence = converge_rows_attempts(
                            attempt.active.as_mut(),
                            attempt.schedule.contexts(),
                            &mut residuals,
                            scope.cancellation().map_err(work_error)?,
                            registry,
                            registration,
                            shutdown,
                            requester,
                        )
                        .await;
                        return first_supervision_error(
                            Err(actor_error(error)),
                            first_supervision_error(replacement_convergence, old_convergence),
                        );
                    }
                };

                let instantiation = match await_with_shutdown(
                    actor.activate_replacement(qualification),
                    shutdown,
                    requester,
                )
                .await
                {
                    Ok(instantiation) => instantiation,
                    Err(error) => {
                        converge_dormant(dormant.as_mut(), cancellation, shutdown, requester).await;
                        let replacement_convergence = record_active_convergence(
                            registry,
                            registration,
                            replacement_schedule.contexts(),
                        )
                        .await;
                        let old_convergence = converge_rows_attempts(
                            attempt.active.as_mut(),
                            attempt.schedule.contexts(),
                            &mut residuals,
                            scope.cancellation().map_err(work_error)?,
                            registry,
                            registration,
                            shutdown,
                            requester,
                        )
                        .await;
                        return first_supervision_error(
                            Err(actor_error(error)),
                            first_supervision_error(replacement_convergence, old_convergence),
                        );
                    }
                };
                let activated = await_with_shutdown(
                    catch_future_panic(
                        dormant.activate(&replacement_schedule, cancellation.clone()),
                    ),
                    shutdown,
                    requester,
                )
                .await;
                let activated = match activated {
                    Ok(Ok(activated)) => activated,
                    Ok(Err(failure)) => {
                        let error = failure.into_failure().error().clone();
                        let actor_result = actor
                            .initialization_failed(instantiation)
                            .await
                            .map(|_| ())
                            .map_err(actor_error);
                        converge_dormant(dormant.as_mut(), cancellation, shutdown, requester).await;
                        let replacement_convergence = record_active_convergence(
                            registry,
                            registration,
                            replacement_schedule.contexts(),
                        )
                        .await;
                        let old_convergence = converge_rows_attempts(
                            attempt.active.as_mut(),
                            attempt.schedule.contexts(),
                            &mut residuals,
                            scope.cancellation().map_err(work_error)?,
                            registry,
                            registration,
                            shutdown,
                            requester,
                        )
                        .await;
                        return first_supervision_error(
                            Err(error),
                            first_supervision_error(
                                actor_result,
                                first_supervision_error(replacement_convergence, old_convergence),
                            ),
                        );
                    }
                    Err(()) => {
                        let error = native_future_panicked(
                            "Native replacement attempt activation panicked",
                        );
                        let actor_result = actor
                            .initialization_failed(instantiation)
                            .await
                            .map(|_| ())
                            .map_err(actor_error);
                        converge_dormant(dormant.as_mut(), cancellation, shutdown, requester).await;
                        let replacement_convergence = record_active_convergence(
                            registry,
                            registration,
                            replacement_schedule.contexts(),
                        )
                        .await;
                        let old_convergence = converge_rows_attempts(
                            attempt.active.as_mut(),
                            attempt.schedule.contexts(),
                            &mut residuals,
                            scope.cancellation().map_err(work_error)?,
                            registry,
                            registration,
                            shutdown,
                            requester,
                        )
                        .await;
                        return first_supervision_error(
                            Err(error),
                            first_supervision_error(
                                actor_result,
                                first_supervision_error(replacement_convergence, old_convergence),
                            ),
                        );
                    }
                };
                drop(dormant);
                let (active, rows_runtime) = activated.into_parts();
                let Some(rows_runtime) = rows_runtime else {
                    let error = QueryExecutionError::new(
                        QueryExecutionErrorKind::InvalidRequest,
                        "Native replacement omitted its root result runtime",
                    );
                    let actor_result = actor
                        .initialization_failed(instantiation)
                        .await
                        .map(|_| ())
                        .map_err(actor_error);
                    let mut active = active;
                    let convergence =
                        converge_active(active.as_mut(), cancellation, shutdown, requester).await;
                    drop(active);
                    let replacement_convergence = record_reported_active_convergence(
                        registry,
                        registration,
                        replacement_schedule.contexts(),
                        convergence,
                    )
                    .await;
                    let old_convergence = converge_rows_attempts(
                        attempt.active.as_mut(),
                        attempt.schedule.contexts(),
                        &mut residuals,
                        scope.cancellation().map_err(work_error)?,
                        registry,
                        registration,
                        shutdown,
                        requester,
                    )
                    .await;
                    return first_supervision_error(
                        Err(error),
                        first_supervision_error(
                            actor_result,
                            first_supervision_error(replacement_convergence, old_convergence),
                        ),
                    );
                };
                let running = match actor.activate(instantiation.ready()).await {
                    Ok(running) => running,
                    Err(error) => {
                        let mut active = active;
                        let convergence =
                            converge_active(active.as_mut(), cancellation, shutdown, requester)
                                .await;
                        let replacement_convergence = record_reported_active_convergence(
                            registry,
                            registration,
                            replacement_schedule.contexts(),
                            convergence,
                        )
                        .await;
                        let old_convergence = converge_rows_attempts(
                            attempt.active.as_mut(),
                            attempt.schedule.contexts(),
                            &mut residuals,
                            scope.cancellation().map_err(work_error)?,
                            registry,
                            registration,
                            shutdown,
                            requester,
                        )
                        .await;
                        return first_supervision_error(
                            Err(actor_error(error)),
                            first_supervision_error(replacement_convergence, old_convergence),
                        );
                    }
                };
                supervise_residual_rows_attempt(
                    &mut residuals,
                    attempt.active,
                    attempt.schedule.contexts().to_vec().into_boxed_slice(),
                    cancellation,
                    shutdown.clone(),
                    requester.clone(),
                );
                attempt = RowsAttempt {
                    schedule: replacement_schedule,
                    active,
                    running,
                    runtime: rows_runtime,
                };
            }
        }
    }
}

async fn prepare_native_attempt(
    session: &mut LogicalNativeSession,
    execution: QueryExecutionId,
    frontend_process_id: FrontendProcessId,
    scheduling: &novarocks_sql::planning::query_execution::SqlExecutionSchedulingFacts,
    shutdown: &mut watch::Receiver<bool>,
    requester: &novarocks_workload_control::WorkCancellationRequester,
) -> Result<(super::AttemptSchedule, Box<dyn DormantNativeAttemptOwner>), QueryExecutionError> {
    let (request, acceptance) = session.issue_attempt(execution).map_err(contract_error)?;
    let prepared = await_with_shutdown(session.prepare(request), shutdown, requester)
        .await
        .and_then(|prepared| acceptance.accept(prepared).map_err(Into::into))
        .map_err(native_prepare_error)?;
    let schedule = build_attempt_schedule(
        execution,
        frontend_process_id,
        &prepared.eligible_backends,
        &prepared.scan_work,
        scheduling,
    )
    .map_err(|error| {
        QueryExecutionError::new(QueryExecutionErrorKind::Failed, error.to_string())
    })?;
    Ok((schedule, prepared.owner))
}

fn next_attempt(execution: QueryExecutionId) -> Result<QueryExecutionId, QueryExecutionError> {
    let next = execution.attempt_id().get().checked_add(1).ok_or_else(|| {
        QueryExecutionError::new(
            QueryExecutionErrorKind::Failed,
            "logical execution attempt id space is exhausted",
        )
    })?;
    QueryExecutionId::new(
        execution.query_id(),
        AttemptId::new(next).expect("checked successor attempt id is nonzero"),
    )
    .map_err(|error| QueryExecutionError::new(QueryExecutionErrorKind::Failed, error.to_string()))
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

async fn record_reported_active_convergence(
    registry: &LogicalExecutionRuntimeRegistryHandle,
    registration: &super::LogicalExecutionRegistration,
    expected_contexts: &[novarocks_execution_contract::QueryContextRef],
    convergence: NativeAttemptConvergence,
) -> Result<(), QueryExecutionError> {
    let NativeAttemptConvergence::Contexts(facts) = convergence else {
        return record_active_convergence(registry, registration, expected_contexts).await;
    };
    let expected = expected_contexts.iter().copied().collect::<BTreeSet<_>>();
    let mut observed = BTreeSet::new();
    for fact in &facts {
        if !expected.contains(&fact.context()) || !observed.insert(fact.context()) {
            return Err(QueryExecutionError::new(
                QueryExecutionErrorKind::InvalidRequest,
                "Native active convergence returned duplicate or foreign context evidence",
            ));
        }
    }
    if observed != expected {
        return Err(QueryExecutionError::new(
            QueryExecutionErrorKind::InvalidRequest,
            "Native active convergence omitted scheduled context evidence",
        ));
    }
    let mut first_error = None;
    for fact in facts {
        let result = match fact.kind() {
            NativeContextConvergenceKind::WorkerStoppedAndContextFenced => {
                registry
                    .observe_worker_stopped_and_context_fenced(registration, fact.context())
                    .await
            }
            NativeContextConvergenceKind::WorkerProcessReplaced => {
                registry
                    .observe_worker_process_replaced(registration, fact.context())
                    .await
            }
        };
        if let Err(error) = result {
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
) -> NativeAttemptConvergence {
    loop {
        let convergence = async { owner.converge(cancellation.clone()).await };
        if let Ok(convergence) =
            await_with_shutdown(catch_future_panic(convergence), shutdown, requester).await
        {
            return convergence;
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
    use std::collections::BTreeSet;
    use std::sync::OnceLock;
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
        LogicalNativeOpenFailure, LogicalNativeOpenFuture, NativeActiveAttemptConvergenceFuture,
        NativeAttemptActivationFailure, NativeAttemptActivationFuture, NativeAttemptConvergence,
        NativeAttemptConvergenceFuture, NativeAttemptPreparationFailure,
        NativeAttemptPreparationFuture, NativeAttemptPreparationPort,
        NativeAttemptPreparationRequest, NativeAttemptRunFuture,
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
            LogicalExecutionRowsConfig::new(
                NonZeroUsize::new(2).unwrap(),
                NonZeroU32::new(2).unwrap(),
                Duration::from_secs(30),
                MaxWait::new(Duration::from_millis(10)).unwrap(),
                ResultByteLimit::new(1 << 12).unwrap(),
            ),
        )
    }

    fn supervisor_resources() -> LocalResourceAuthority {
        let control = WorkloadControl::try_new(
            WorkloadConfig::default(),
            ResourceConfig {
                total_bytes: 1 << 20,
                control_bytes: 1 << 12,
                per_scope_bytes: (1 << 20) - (1 << 12),
            },
        )
        .unwrap();
        control.resources()
    }

    fn completion_request(attempts: impl NativeAttemptPreparationPort) -> QueryExecutionRequest {
        let plan = native_preparation_plan(NativePreparationFixture::MissingResultOutput).unwrap();
        let plan = SealedPreparationPlan::seal(plan);
        let description =
            FrozenExecutionDescription::try_freeze(FrozenExecutionDescriptionDraft::new(
                crate::api::QueryExecutionKind::Maintenance,
                plan,
                None,
                super::super::ExecutionEffect::None,
                RecoveryMode::NoRecovery,
                Vec::new(),
                FrozenCostEstimate::unknown(FrozenEstimateUnknownReason::NotProjected),
                ExecutionResourceRequirements::unknown(FrozenEstimateUnknownReason::NotProjected),
            ))
            .unwrap();
        QueryExecutionRequest::bind_native(
            description,
            PermanentlyBackpressuredAbortEffectPort::shared(),
            None,
            attempts,
        )
    }

    fn rows_request(
        recovery: RecoveryMode,
        replacements: Option<Arc<dyn super::super::ReplacementQualificationEffectPort>>,
        attempts: impl NativeAttemptPreparationPort,
    ) -> QueryExecutionRequest {
        let plan = native_preparation_plan(NativePreparationFixture::ResultOutput).unwrap();
        let plan = SealedPreparationPlan::seal(plan);
        let description =
            FrozenExecutionDescription::try_freeze(FrozenExecutionDescriptionDraft::new(
                crate::api::QueryExecutionKind::Read,
                plan,
                None,
                super::super::ExecutionEffect::None,
                recovery,
                Vec::new(),
                FrozenCostEstimate::unknown(FrozenEstimateUnknownReason::NotProjected),
                ExecutionResourceRequirements::unknown(FrozenEstimateUnknownReason::NotProjected),
            ))
            .unwrap();
        QueryExecutionRequest::bind_native(
            description,
            PermanentlyBackpressuredAbortEffectPort::shared(),
            replacements,
            attempts,
        )
    }

    fn rows_decode_runtime() -> super::super::RootResultDecodeRuntime {
        static OWNER: OnceLock<super::super::RootResultDecodeRuntimeOwner> = OnceLock::new();
        OWNER
            .get_or_init(|| {
                super::super::RootResultDecodeRuntimeOwner::try_new(
                    NonZeroUsize::new(1).unwrap(),
                    NonZeroUsize::new(4).unwrap(),
                )
                .unwrap()
            })
            .runtime()
    }

    #[derive(Debug)]
    struct UnreachablePreparationPort;

    impl NativeAttemptPreparationPort for UnreachablePreparationPort {
        fn prepare(
            &mut self,
            _request: NativeAttemptPreparationRequest,
        ) -> NativeAttemptPreparationFuture {
            Box::pin(async { panic!("unreachable Native attempt preparation") })
        }
    }

    #[derive(Debug)]
    struct DropTrackedPreparationPort {
        drops: Arc<AtomicU64>,
    }

    impl Drop for DropTrackedPreparationPort {
        fn drop(&mut self) {
            self.drops.fetch_add(1, Ordering::SeqCst);
        }
    }

    impl NativeAttemptPreparationPort for DropTrackedPreparationPort {
        fn prepare(
            &mut self,
            _request: NativeAttemptPreparationRequest,
        ) -> NativeAttemptPreparationFuture {
            Box::pin(async { panic!("rejected Native seed must never prepare an attempt") })
        }
    }

    fn drop_tracked_request(drops: &Arc<AtomicU64>) -> QueryExecutionRequest {
        completion_request(DropTrackedPreparationPort {
            drops: Arc::clone(drops),
        })
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
    }

    impl LogicalExecutionNativePort for FailingActivationNativePort {
        fn open(&self, request: LogicalNativeOpenRequest) -> LogicalNativeOpenFuture {
            let execution = request.initial_execution();
            self.opened_execution_low
                .store(execution.query_id().low() as u64, Ordering::SeqCst);
            Box::pin(async move { request.bind().map_err(Into::into) })
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
                backend,
                activated: Arc::clone(&self.activated),
                residual_converged: Arc::clone(&self.residual_converged),
            };
            Box::pin(async move { request.bind(Vec::new(), dormant).map_err(Into::into) })
        }
    }

    #[derive(Debug)]
    struct FailingDormantOwner {
        backend: BackendProcessId,
        activated: Arc<AtomicBool>,
        residual_converged: Arc<AtomicBool>,
    }

    impl DormantNativeAttemptOwner for FailingDormantOwner {
        fn eligible_backends(&self) -> &[BackendProcessId] {
            std::slice::from_ref(&self.backend)
        }

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
        FailingActivationPreparationPort,
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
            }),
            FailingActivationPreparationPort {
                backend: BackendProcessId::new_v7(),
                activated: Arc::clone(&activated),
                residual_converged: Arc::clone(&residual),
            },
            opened,
            activated,
            residual,
        )
    }

    #[tokio::test]
    async fn activation_failure_retains_residual_and_retires_exact_registry_entry() {
        let (native, attempts, opened, activated, residual) = failing_native();
        let (mut supervisor, client) = LogicalExecutionSupervisor::new(
            Handle::current(),
            native,
            supervisor_resources(),
            QueryProcessNamespace::new(0x45),
            FrontendProcessId::new_v7(),
            supervisor_config(4),
        );
        let (control, root) = governance();
        let scope = root.owner.scope();

        let error = match client.start(completion_request(attempts), root.owner).await {
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
        let (native, attempts, _opened, activated, residual) = failing_native();
        let (mut supervisor, client) = LogicalExecutionSupervisor::new(
            Handle::current(),
            native,
            supervisor_resources(),
            QueryProcessNamespace::new(0x46),
            FrontendProcessId::new_v7(),
            supervisor_config(4),
        );
        let (control, root) = governance();
        let scope = root.owner.scope();
        let future = client.start(completion_request(attempts), root.owner);
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
    struct BindingNativePort;

    impl LogicalExecutionNativePort for BindingNativePort {
        fn open(&self, request: LogicalNativeOpenRequest) -> LogicalNativeOpenFuture {
            Box::pin(async move { request.bind().map_err(Into::into) })
        }
    }

    #[derive(Debug, Default)]
    struct ClosedSuccessSealPort;

    impl super::super::AcceptedRootSuccessSealPort for ClosedSuccessSealPort {
        fn enqueue_success_seal(
            &self,
            request: super::super::AcceptedRootSuccessSealRequest,
        ) -> Result<(), super::super::AcceptedRootSuccessSealRequest> {
            Err(request)
        }
    }

    #[derive(Debug)]
    struct RowsAttemptPreparationPort {
        backend: BackendProcessId,
        converged: Arc<AtomicBool>,
    }

    impl NativeAttemptPreparationPort for RowsAttemptPreparationPort {
        fn prepare(
            &mut self,
            request: NativeAttemptPreparationRequest,
        ) -> NativeAttemptPreparationFuture {
            let owner = RowsDormantOwner {
                backend: self.backend,
                converged: Arc::clone(&self.converged),
            };
            Box::pin(async move { request.bind(Vec::new(), owner).map_err(Into::into) })
        }
    }

    #[derive(Debug)]
    struct RowsDormantOwner {
        backend: BackendProcessId,
        converged: Arc<AtomicBool>,
    }

    impl DormantNativeAttemptOwner for RowsDormantOwner {
        fn eligible_backends(&self) -> &[BackendProcessId] {
            std::slice::from_ref(&self.backend)
        }

        fn activate<'a>(
            &'a mut self,
            schedule: &'a AttemptSchedule,
            _cancellation: novarocks_workload_control::CancellationView,
        ) -> NativeAttemptActivationFuture<'a> {
            let root = schedule.root();
            let converged = Arc::clone(&self.converged);
            Box::pin(async move {
                let (status_sender, statuses) =
                    super::super::accepted_root_status_projection_with_seal_port(
                        root,
                        Arc::new(ClosedSuccessSealPort),
                    );
                let binding =
                    super::super::RootResultPumpBinding::new(rows_decode_runtime(), |_| async {
                        std::future::pending::<
                            Result<
                                super::super::RootResultFetchOutcome,
                                super::super::RootResultFetchFailure,
                            >,
                        >()
                        .await
                    });
                Ok(ActivatedNativeAttempt::rows(
                    RowsActiveOwner {
                        converged,
                        _status_sender: status_sender,
                    },
                    binding,
                    statuses,
                ))
            })
        }

        fn converge<'a>(
            &'a mut self,
            _cancellation: novarocks_workload_control::CancellationView,
        ) -> NativeAttemptConvergenceFuture<'a> {
            Box::pin(async move {
                self.converged.store(true, Ordering::SeqCst);
            })
        }
    }

    struct RowsActiveOwner {
        converged: Arc<AtomicBool>,
        _status_sender: super::super::AcceptedRootStatusSender,
    }

    impl fmt::Debug for RowsActiveOwner {
        fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
            formatter.write_str("RowsActiveOwner")
        }
    }

    impl ActiveNativeAttemptOwner for RowsActiveOwner {
        fn run<'a>(
            &'a mut self,
            _drive: &'a NativeAttemptDrive,
            cancellation: novarocks_workload_control::CancellationView,
        ) -> NativeAttemptRunFuture<'a> {
            Box::pin(async move {
                let _ = cancellation.cancelled().await;
                NativeAttemptTerminal::Failed(NativeAttemptPreparationFailure::new(
                    AttemptFailureClass::ExecutionFailure,
                    QueryExecutionError::new(
                        QueryExecutionErrorKind::Cancelled,
                        "Rows test attempt was cancelled",
                    ),
                ))
            })
        }

        fn converge<'a>(
            &'a mut self,
            _cancellation: novarocks_workload_control::CancellationView,
        ) -> NativeActiveAttemptConvergenceFuture<'a> {
            Box::pin(async move {
                self.converged.store(true, Ordering::SeqCst);
                NativeAttemptConvergence::all_workers_stopped_and_contexts_fenced()
            })
        }
    }

    #[tokio::test]
    async fn rows_handle_is_handed_off_before_the_attempt_converges() {
        let (control, root) = governance();
        let scope = root.owner.scope();
        let converged = Arc::new(AtomicBool::new(false));
        let attempts = RowsAttemptPreparationPort {
            backend: BackendProcessId::new_v7(),
            converged: Arc::clone(&converged),
        };
        let (mut supervisor, client) = LogicalExecutionSupervisor::new(
            Handle::current(),
            Arc::new(BindingNativePort),
            control.resources(),
            QueryProcessNamespace::new(0x52),
            FrontendProcessId::new_v7(),
            supervisor_config(4),
        );

        let mut handle = tokio::time::timeout(
            Duration::from_secs(1),
            client.start(
                rows_request(RecoveryMode::NoRecovery, None, attempts),
                root.owner,
            ),
        )
        .await
        .expect("Rows execution must hand off its bounded result consumer")
        .unwrap();
        let Some(crate::api::ExecutionOutput::Rows(mut stream)) = handle.take_output() else {
            panic!("Rows execution must return its result stream");
        };
        assert!(!converged.load(Ordering::SeqCst));
        handle.request_cancel().unwrap();
        let Err(error) = tokio::time::timeout(Duration::from_secs(1), stream.next())
            .await
            .expect("scope cancellation must terminate the result stream")
        else {
            panic!("cancelled Rows stream must end with a typed error");
        };
        assert!(!error.message().is_empty());
        drop(stream);
        root.business.release();
        supervisor
            .shutdown_until(Instant::now() + Duration::from_secs(1))
            .await
            .unwrap();
        acknowledge_all_control(&control);
        scope.wait_released().await;
        assert!(converged.load(Ordering::SeqCst));
    }

    #[derive(Debug)]
    struct ImmediateReplacementPort {
        capacity: watch::Sender<u64>,
    }

    impl Default for ImmediateReplacementPort {
        fn default() -> Self {
            let (capacity, _) = watch::channel(1);
            Self { capacity }
        }
    }

    #[derive(Debug)]
    struct ImmediateReplacementEffectReservation;

    impl super::super::ReplacementQualificationEffectReservation
        for ImmediateReplacementEffectReservation
    {
        fn submit(
            self: Box<Self>,
            submission: super::super::ReplacementQualificationEffectSubmission,
        ) {
            let request = submission.request();
            let admissions: Box<[_]> = request
                .replacement_contexts()
                .iter()
                .enumerate()
                .map(|(index, context)| {
                    let tag = u8::try_from(index + 1).unwrap();
                    let valid_for = LeaseValidFor::new(Duration::from_secs(30)).unwrap();
                    let admission_request =
                        novarocks_execution_contract::AcquireQueryContextAdmissionTicket::new(
                            TaskOperationId::new_v7(),
                            *context,
                            valid_for,
                            NativeCompatibilityId::new([tag; 32]),
                            AdmissionEpochCapability::try_from_bytes([tag; 16]).unwrap(),
                        );
                    let ticket = QueryContextAdmissionTicketReceipt::new(
                        AdmissionTicketId::try_from_bytes([tag; 16]).unwrap(),
                        *context,
                        valid_for,
                    );
                    super::super::QualifiedWorkerAdmission::try_new(
                        request.identity().replacement(),
                        admission_request,
                        ticket,
                    )
                    .unwrap()
                })
                .collect::<Vec<_>>()
                .into_boxed_slice();
            let reservation = ImmediateIsolationReservation {
                identity: request.identity(),
                failed_contexts: request.failed_contexts().iter().copied().collect(),
                replacement_contexts: request.replacement_contexts().iter().copied().collect(),
                admissions,
            };
            let qualified = super::super::QualifiedReplacementReservation::try_new(
                request,
                Box::new(reservation),
            )
            .unwrap();
            submission.qualified(qualified).unwrap();
        }
    }

    impl super::super::ReplacementQualificationEffectPort for ImmediateReplacementPort {
        fn subscribe_capacity(&self) -> watch::Receiver<u64> {
            self.capacity.subscribe()
        }

        fn try_reserve(
            &self,
            _request: &super::super::ReplacementQualificationRequest,
        ) -> super::super::ReplacementQualificationEffectAdmission {
            super::super::ReplacementQualificationEffectAdmission::Admitted(Box::new(
                ImmediateReplacementEffectReservation,
            ))
        }
    }

    #[derive(Debug)]
    struct ImmediateIsolationReservation {
        identity: super::super::ReplacementQualificationIdentity,
        failed_contexts: BTreeSet<QueryContextRef>,
        replacement_contexts: BTreeSet<QueryContextRef>,
        admissions: Box<[super::super::QualifiedWorkerAdmission]>,
    }

    impl super::super::AttemptIsolationReservation for ImmediateIsolationReservation {
        fn identity(&self) -> super::super::ReplacementQualificationIdentity {
            self.identity
        }

        fn topology_revision(&self) -> u64 {
            1
        }

        fn admissions(&self) -> &[super::super::QualifiedWorkerAdmission] {
            &self.admissions
        }

        fn validate_binding(
            &self,
            failed_contexts: &[QueryContextRef],
        ) -> Result<(), super::super::ReplacementQualificationFailure> {
            let failed: BTreeSet<_> = failed_contexts.iter().copied().collect();
            let replacements: BTreeSet<_> = self
                .admissions
                .iter()
                .map(super::super::QualifiedWorkerAdmission::context)
                .collect();
            (failed == self.failed_contexts && replacements == self.replacement_contexts)
                .then_some(())
                .ok_or(super::super::ReplacementQualificationFailure::InvalidReservation)
        }

        fn activate(
            self: Box<Self>,
        ) -> Result<
            Box<dyn super::super::ActiveAttemptIsolationOwner>,
            super::super::AttemptIsolationActivationFailure,
        > {
            Ok(Box::new(ImmediateActiveIsolation {
                execution: self.identity.replacement(),
            }))
        }

        fn abandon(self: Box<Self>) {}
    }

    #[derive(Debug)]
    struct ImmediateActiveIsolation {
        execution: QueryExecutionId,
    }

    impl super::super::ActiveAttemptIsolationOwner for ImmediateActiveIsolation {
        fn execution(&self) -> QueryExecutionId {
            self.execution
        }

        fn finish(self: Box<Self>) {}

        fn abandon(self: Box<Self>) {}
    }

    #[derive(Debug)]
    struct RecoveringRowsPreparationPort {
        backend: BackendProcessId,
        prepares: Arc<AtomicU64>,
        runs: Arc<AtomicU64>,
        convergences: Arc<AtomicU64>,
        allow_initial_convergence: Arc<tokio::sync::Notify>,
        block_initial_convergence: bool,
        replacement_missing_rows: bool,
    }

    impl NativeAttemptPreparationPort for RecoveringRowsPreparationPort {
        fn prepare(
            &mut self,
            request: NativeAttemptPreparationRequest,
        ) -> NativeAttemptPreparationFuture {
            let ordinal = self.prepares.fetch_add(1, Ordering::SeqCst) + 1;
            let owner = RecoveringRowsDormantOwner {
                backend: self.backend,
                ordinal,
                runs: Arc::clone(&self.runs),
                convergences: Arc::clone(&self.convergences),
                allow_initial_convergence: Arc::clone(&self.allow_initial_convergence),
                block_initial_convergence: self.block_initial_convergence,
                replacement_missing_rows: self.replacement_missing_rows,
            };
            Box::pin(async move { request.bind(Vec::new(), owner).map_err(Into::into) })
        }
    }

    #[derive(Debug)]
    struct RecoveringRowsDormantOwner {
        backend: BackendProcessId,
        ordinal: u64,
        runs: Arc<AtomicU64>,
        convergences: Arc<AtomicU64>,
        allow_initial_convergence: Arc<tokio::sync::Notify>,
        block_initial_convergence: bool,
        replacement_missing_rows: bool,
    }

    impl DormantNativeAttemptOwner for RecoveringRowsDormantOwner {
        fn eligible_backends(&self) -> &[BackendProcessId] {
            std::slice::from_ref(&self.backend)
        }

        fn activate<'a>(
            &'a mut self,
            schedule: &'a AttemptSchedule,
            _cancellation: novarocks_workload_control::CancellationView,
        ) -> NativeAttemptActivationFuture<'a> {
            let root = schedule.root();
            let context = schedule.contexts()[0];
            let ordinal = self.ordinal;
            let runs = Arc::clone(&self.runs);
            let convergences = Arc::clone(&self.convergences);
            let allow_initial_convergence = Arc::clone(&self.allow_initial_convergence);
            let block_initial_convergence = self.block_initial_convergence;
            let replacement_missing_rows = self.replacement_missing_rows;
            Box::pin(async move {
                if replacement_missing_rows && ordinal > 1 {
                    return Ok(ActivatedNativeAttempt::completion(PanickingActiveOwner {
                        phase: AttemptPanicPhase::Run,
                        convergence_calls: convergences,
                    }));
                }
                let (status_sender, statuses) =
                    super::super::accepted_root_status_projection_with_seal_port(
                        root,
                        Arc::new(ClosedSuccessSealPort),
                    );
                let fetches = Arc::new(AtomicU64::new(0));
                let binding =
                    super::super::RootResultPumpBinding::new(rows_decode_runtime(), move |_| {
                        let fetch = fetches.fetch_add(1, Ordering::SeqCst);
                        async move {
                            if ordinal > 1 && fetch == 0 {
                                let packet = super::super::PreflightedRootResultPacket::new(
                                    novarocks_execution_contract::ResultPacketSequence::new(0),
                                    64,
                                    super::super::RootResultDecodeBounds::new(4_096, 4_096)
                                        .unwrap(),
                                    || {
                                        arrow::record_batch::RecordBatch::try_new(
                                            Arc::new(arrow::datatypes::Schema::new(vec![
                                                arrow::datatypes::Field::new(
                                                    "a",
                                                    arrow::datatypes::DataType::Int64,
                                                    false,
                                                ),
                                                arrow::datatypes::Field::new(
                                                    "b",
                                                    arrow::datatypes::DataType::Utf8,
                                                    true,
                                                ),
                                            ])),
                                            vec![
                                                Arc::new(arrow::array::Int64Array::from(vec![
                                                    2_i64,
                                                ])),
                                                Arc::new(arrow::array::StringArray::from(vec![
                                                    Some("replacement"),
                                                ])),
                                            ],
                                        )
                                        .map_err(|error| {
                                            QueryExecutionError::new(
                                                QueryExecutionErrorKind::Failed,
                                                error.to_string(),
                                            )
                                        })
                                    },
                                )
                                .unwrap();
                                Ok(super::super::RootResultFetchOutcome::Ready(packet))
                            } else {
                                std::future::pending::<
                                    Result<
                                        super::super::RootResultFetchOutcome,
                                        super::super::RootResultFetchFailure,
                                    >,
                                >()
                                .await
                            }
                        }
                    });
                Ok(ActivatedNativeAttempt::rows(
                    RecoveringRowsActiveOwner {
                        ordinal,
                        context,
                        runs,
                        convergences,
                        allow_initial_convergence,
                        block_initial_convergence,
                        _status_sender: status_sender,
                    },
                    binding,
                    statuses,
                ))
            })
        }

        fn converge<'a>(
            &'a mut self,
            _cancellation: novarocks_workload_control::CancellationView,
        ) -> NativeAttemptConvergenceFuture<'a> {
            Box::pin(async move {
                self.convergences.fetch_add(1, Ordering::SeqCst);
            })
        }
    }

    struct RecoveringRowsActiveOwner {
        ordinal: u64,
        context: QueryContextRef,
        runs: Arc<AtomicU64>,
        convergences: Arc<AtomicU64>,
        allow_initial_convergence: Arc<tokio::sync::Notify>,
        block_initial_convergence: bool,
        _status_sender: super::super::AcceptedRootStatusSender,
    }

    impl fmt::Debug for RecoveringRowsActiveOwner {
        fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
            formatter
                .debug_struct("RecoveringRowsActiveOwner")
                .field("ordinal", &self.ordinal)
                .finish_non_exhaustive()
        }
    }

    impl ActiveNativeAttemptOwner for RecoveringRowsActiveOwner {
        fn run<'a>(
            &'a mut self,
            _drive: &'a NativeAttemptDrive,
            cancellation: novarocks_workload_control::CancellationView,
        ) -> NativeAttemptRunFuture<'a> {
            Box::pin(async move {
                self.runs.fetch_add(1, Ordering::SeqCst);
                if self.ordinal == 1 {
                    NativeAttemptTerminal::Failed(NativeAttemptPreparationFailure::new(
                        AttemptFailureClass::RecoverableInfrastructure,
                        QueryExecutionError::new(
                            QueryExecutionErrorKind::Failed,
                            "injected pre-visibility infrastructure failure",
                        ),
                    ))
                } else {
                    let _ = cancellation.cancelled().await;
                    NativeAttemptTerminal::Failed(NativeAttemptPreparationFailure::new(
                        AttemptFailureClass::ExecutionFailure,
                        QueryExecutionError::new(
                            QueryExecutionErrorKind::Cancelled,
                            "replacement attempt was cancelled",
                        ),
                    ))
                }
            })
        }

        fn converge<'a>(
            &'a mut self,
            _cancellation: novarocks_workload_control::CancellationView,
        ) -> NativeActiveAttemptConvergenceFuture<'a> {
            Box::pin(async move {
                if self.ordinal == 1 && self.block_initial_convergence {
                    self.allow_initial_convergence.notified().await;
                }
                self.convergences.fetch_add(1, Ordering::SeqCst);
                if self.ordinal == 1 {
                    NativeAttemptConvergence::contexts(vec![
                        crate::api::NativeContextConvergence::worker_process_replaced(self.context),
                    ])
                } else {
                    NativeAttemptConvergence::all_workers_stopped_and_contexts_fenced()
                }
            })
        }
    }

    #[tokio::test]
    async fn recoverable_pre_visibility_failure_activates_a_rows_replacement() {
        let prepares = Arc::new(AtomicU64::new(0));
        let runs = Arc::new(AtomicU64::new(0));
        let convergences = Arc::new(AtomicU64::new(0));
        let allow_initial_convergence = Arc::new(tokio::sync::Notify::new());
        let attempts = RecoveringRowsPreparationPort {
            backend: BackendProcessId::new_v7(),
            prepares: Arc::clone(&prepares),
            runs: Arc::clone(&runs),
            convergences: Arc::clone(&convergences),
            allow_initial_convergence: Arc::clone(&allow_initial_convergence),
            block_initial_convergence: true,
            replacement_missing_rows: false,
        };
        let replacements = Arc::new(ImmediateReplacementPort::default());
        let (control, root) = governance();
        let scope = root.owner.scope();
        let (mut supervisor, client) = LogicalExecutionSupervisor::new(
            Handle::current(),
            Arc::new(BindingNativePort),
            control.resources(),
            QueryProcessNamespace::new(0x54),
            FrontendProcessId::new_v7(),
            supervisor_config(4),
        );
        let mut handle = client
            .start(
                rows_request(
                    RecoveryMode::RestartAttemptBeforeVisibility,
                    Some(replacements),
                    attempts,
                ),
                root.owner,
            )
            .await
            .unwrap();
        let Some(crate::api::ExecutionOutput::Rows(mut stream)) = handle.take_output() else {
            panic!("recoverable Rows execution must keep one result consumer");
        };
        tokio::time::timeout(Duration::from_secs(1), async {
            while runs.load(Ordering::SeqCst) < 2 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("successor activation must not wait for old attempt convergence");
        assert_eq!(prepares.load(Ordering::SeqCst), 2);
        assert_eq!(runs.load(Ordering::SeqCst), 2);
        assert_eq!(convergences.load(Ordering::SeqCst), 0);
        stream
            .begin_schema()
            .expect("replacement Rows execution must retain the frozen schema")
            .complete();
        let crate::api::ResultDelivery::Batch(delivery) =
            tokio::time::timeout(Duration::from_secs(1), stream.next())
                .await
                .expect("replacement must deliver while old convergence is blocked")
                .unwrap()
                .unwrap()
        else {
            panic!("replacement must deliver a result batch");
        };
        let bytes = delivery.decoded_bytes();
        delivery
            .reserve_protocol(&control.resources(), bytes)
            .unwrap()
            .begin_protocol_write(bytes)
            .unwrap()
            .complete()
            .unwrap();
        assert_eq!(convergences.load(Ordering::SeqCst), 0);
        allow_initial_convergence.notify_one();
        handle.request_cancel().unwrap();
        let _ = tokio::time::timeout(Duration::from_secs(1), stream.next())
            .await
            .expect("replacement cancellation must terminate the result stream");
        drop(stream);
        root.business.release();
        supervisor
            .shutdown_until(Instant::now() + Duration::from_secs(1))
            .await
            .unwrap();
        acknowledge_all_control(&control);
        scope.wait_released().await;
        assert_eq!(convergences.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn process_exit_abandonment_is_finite_with_a_lost_residual_attempt() {
        let prepares = Arc::new(AtomicU64::new(0));
        let runs = Arc::new(AtomicU64::new(0));
        let convergences = Arc::new(AtomicU64::new(0));
        let allow_initial_convergence = Arc::new(tokio::sync::Notify::new());
        let attempts = RecoveringRowsPreparationPort {
            backend: BackendProcessId::new_v7(),
            prepares,
            runs: Arc::clone(&runs),
            convergences: Arc::clone(&convergences),
            allow_initial_convergence: Arc::clone(&allow_initial_convergence),
            block_initial_convergence: true,
            replacement_missing_rows: false,
        };
        let (control, root) = governance();
        let scope = root.owner.scope();
        let (mut supervisor, client) = LogicalExecutionSupervisor::new(
            Handle::current(),
            Arc::new(BindingNativePort),
            control.resources(),
            QueryProcessNamespace::new(0x55),
            FrontendProcessId::new_v7(),
            supervisor_config(4),
        );
        let mut handle = client
            .start(
                rows_request(
                    RecoveryMode::RestartAttemptBeforeVisibility,
                    Some(Arc::new(ImmediateReplacementPort::default())),
                    attempts,
                ),
                root.owner,
            )
            .await
            .unwrap();
        let Some(crate::api::ExecutionOutput::Rows(mut stream)) = handle.take_output() else {
            panic!("recoverable Rows execution must keep one result consumer");
        };
        tokio::time::timeout(Duration::from_secs(1), async {
            while runs.load(Ordering::SeqCst) < 2 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("successor activation must not wait for the lost residual attempt");
        stream.begin_schema().unwrap().complete();
        let crate::api::ResultDelivery::Batch(delivery) =
            tokio::time::timeout(Duration::from_secs(1), stream.next())
                .await
                .expect("successor result must remain available")
                .unwrap()
                .unwrap()
        else {
            panic!("successor must deliver a result batch");
        };
        let bytes = delivery.decoded_bytes();
        delivery
            .reserve_protocol(&control.resources(), bytes)
            .unwrap()
            .begin_protocol_write(bytes)
            .unwrap()
            .complete()
            .unwrap();

        handle.request_cancel().unwrap();
        let _ = tokio::time::timeout(Duration::from_secs(1), stream.next())
            .await
            .expect("successor cancellation must terminate the result stream");
        drop(stream);
        root.business.release();

        let shutdown = supervisor
            .shutdown_until(Instant::now() + Duration::from_millis(10))
            .await;
        assert!(matches!(
            shutdown,
            Err(LogicalExecutionSupervisorShutdownError::DeadlineExceeded)
        ));
        assert_eq!(convergences.load(Ordering::SeqCst), 1);
        supervisor.abandon_for_process_exit();
        drop(supervisor);

        // The real process would exit here. Let the synthetic lost worker
        // report afterwards so the test runtime can drain its detached task.
        allow_initial_convergence.notify_one();
        tokio::time::timeout(Duration::from_secs(1), async {
            loop {
                acknowledge_all_control(&control);
                tokio::select! {
                    () = scope.wait_released() => break,
                    () = tokio::time::sleep(Duration::from_millis(1)) => {}
                }
            }
        })
        .await
        .expect("detached logical work must release after the late residual fact");
    }

    #[tokio::test]
    async fn replacement_activation_rejects_a_missing_rows_runtime() {
        let prepares = Arc::new(AtomicU64::new(0));
        let runs = Arc::new(AtomicU64::new(0));
        let convergences = Arc::new(AtomicU64::new(0));
        let attempts = RecoveringRowsPreparationPort {
            backend: BackendProcessId::new_v7(),
            prepares: Arc::clone(&prepares),
            runs,
            convergences: Arc::clone(&convergences),
            allow_initial_convergence: Arc::new(tokio::sync::Notify::new()),
            block_initial_convergence: false,
            replacement_missing_rows: true,
        };
        let (control, root) = governance();
        let scope = root.owner.scope();
        let (mut supervisor, client) = LogicalExecutionSupervisor::new(
            Handle::current(),
            Arc::new(BindingNativePort),
            control.resources(),
            QueryProcessNamespace::new(0x55),
            FrontendProcessId::new_v7(),
            supervisor_config(4),
        );
        let mut handle = client
            .start(
                rows_request(
                    RecoveryMode::RestartAttemptBeforeVisibility,
                    Some(Arc::new(ImmediateReplacementPort::default())),
                    attempts,
                ),
                root.owner,
            )
            .await
            .unwrap();
        let Some(crate::api::ExecutionOutput::Rows(mut stream)) = handle.take_output() else {
            panic!("recoverable Rows execution must keep one result consumer");
        };
        let Err(error) = tokio::time::timeout(Duration::from_secs(1), stream.next())
            .await
            .expect("invalid replacement runtime must terminate the stream")
        else {
            panic!("invalid replacement runtime must fail the stream");
        };
        assert_eq!(error.kind(), QueryExecutionErrorKind::InvalidRequest);
        assert_eq!(prepares.load(Ordering::SeqCst), 2);
        drop(stream);
        root.business.release();
        assert!(matches!(
            supervisor
                .shutdown_until(Instant::now() + Duration::from_secs(1))
                .await,
            Err(LogicalExecutionSupervisorShutdownError::SupervisorFailed(_))
        ));
        acknowledge_all_control(&control);
        scope.wait_released().await;
        assert_eq!(convergences.load(Ordering::SeqCst), 2);
    }

    #[derive(Debug)]
    struct VisibleFailurePreparationPort {
        backend: BackendProcessId,
        prepares: Arc<AtomicU64>,
        fail: Arc<tokio::sync::Notify>,
    }

    impl NativeAttemptPreparationPort for VisibleFailurePreparationPort {
        fn prepare(
            &mut self,
            request: NativeAttemptPreparationRequest,
        ) -> NativeAttemptPreparationFuture {
            self.prepares.fetch_add(1, Ordering::SeqCst);
            let owner = VisibleFailureDormantOwner {
                backend: self.backend,
                fail: Arc::clone(&self.fail),
            };
            Box::pin(async move { request.bind(Vec::new(), owner).map_err(Into::into) })
        }
    }

    #[derive(Debug)]
    struct VisibleFailureDormantOwner {
        backend: BackendProcessId,
        fail: Arc<tokio::sync::Notify>,
    }

    impl DormantNativeAttemptOwner for VisibleFailureDormantOwner {
        fn eligible_backends(&self) -> &[BackendProcessId] {
            std::slice::from_ref(&self.backend)
        }

        fn activate<'a>(
            &'a mut self,
            schedule: &'a AttemptSchedule,
            _cancellation: novarocks_workload_control::CancellationView,
        ) -> NativeAttemptActivationFuture<'a> {
            let root = schedule.root();
            let fail = Arc::clone(&self.fail);
            Box::pin(async move {
                let (status_sender, statuses) =
                    super::super::accepted_root_status_projection_with_seal_port(
                        root,
                        Arc::new(ClosedSuccessSealPort),
                    );
                let fetches = Arc::new(AtomicU64::new(0));
                let binding =
                    super::super::RootResultPumpBinding::new(rows_decode_runtime(), move |_| {
                        let fetch = fetches.fetch_add(1, Ordering::SeqCst);
                        async move {
                            if fetch == 0 {
                                let packet = super::super::PreflightedRootResultPacket::new(
                                    novarocks_execution_contract::ResultPacketSequence::new(0),
                                    64,
                                    super::super::RootResultDecodeBounds::new(4_096, 4_096)
                                        .unwrap(),
                                    || {
                                        arrow::record_batch::RecordBatch::try_new(
                                            Arc::new(arrow::datatypes::Schema::new(vec![
                                                arrow::datatypes::Field::new(
                                                    "a",
                                                    arrow::datatypes::DataType::Int64,
                                                    false,
                                                ),
                                                arrow::datatypes::Field::new(
                                                    "b",
                                                    arrow::datatypes::DataType::Utf8,
                                                    true,
                                                ),
                                            ])),
                                            vec![
                                                Arc::new(arrow::array::Int64Array::from(vec![
                                                    1_i64,
                                                ])),
                                                Arc::new(arrow::array::StringArray::from(vec![
                                                    Some("visible"),
                                                ])),
                                            ],
                                        )
                                        .map_err(|error| {
                                            QueryExecutionError::new(
                                                QueryExecutionErrorKind::Failed,
                                                error.to_string(),
                                            )
                                        })
                                    },
                                )
                                .unwrap();
                                Ok(super::super::RootResultFetchOutcome::Ready(packet))
                            } else {
                                std::future::pending::<
                                    Result<
                                        super::super::RootResultFetchOutcome,
                                        super::super::RootResultFetchFailure,
                                    >,
                                >()
                                .await
                            }
                        }
                    });
                Ok(ActivatedNativeAttempt::rows(
                    VisibleFailureActiveOwner {
                        fail,
                        _status_sender: status_sender,
                    },
                    binding,
                    statuses,
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

    struct VisibleFailureActiveOwner {
        fail: Arc<tokio::sync::Notify>,
        _status_sender: super::super::AcceptedRootStatusSender,
    }

    impl fmt::Debug for VisibleFailureActiveOwner {
        fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
            formatter.write_str("VisibleFailureActiveOwner")
        }
    }

    impl ActiveNativeAttemptOwner for VisibleFailureActiveOwner {
        fn run<'a>(
            &'a mut self,
            _drive: &'a NativeAttemptDrive,
            _cancellation: novarocks_workload_control::CancellationView,
        ) -> NativeAttemptRunFuture<'a> {
            Box::pin(async move {
                self.fail.notified().await;
                NativeAttemptTerminal::Failed(NativeAttemptPreparationFailure::new(
                    AttemptFailureClass::RecoverableInfrastructure,
                    QueryExecutionError::new(
                        QueryExecutionErrorKind::Failed,
                        "injected failure after visible output",
                    ),
                ))
            })
        }

        fn converge<'a>(
            &'a mut self,
            _cancellation: novarocks_workload_control::CancellationView,
        ) -> NativeActiveAttemptConvergenceFuture<'a> {
            Box::pin(async { NativeAttemptConvergence::all_workers_stopped_and_contexts_fenced() })
        }
    }

    #[tokio::test]
    async fn visible_rows_failure_does_not_prepare_a_replacement() {
        let prepares = Arc::new(AtomicU64::new(0));
        let fail = Arc::new(tokio::sync::Notify::new());
        let attempts = VisibleFailurePreparationPort {
            backend: BackendProcessId::new_v7(),
            prepares: Arc::clone(&prepares),
            fail: Arc::clone(&fail),
        };
        let (control, root) = governance();
        let scope = root.owner.scope();
        let (mut supervisor, client) = LogicalExecutionSupervisor::new(
            Handle::current(),
            Arc::new(BindingNativePort),
            control.resources(),
            QueryProcessNamespace::new(0x56),
            FrontendProcessId::new_v7(),
            supervisor_config(4),
        );
        let mut handle = client
            .start(
                rows_request(
                    RecoveryMode::RestartAttemptBeforeVisibility,
                    Some(Arc::new(ImmediateReplacementPort::default())),
                    attempts,
                ),
                root.owner,
            )
            .await
            .unwrap();
        let Some(crate::api::ExecutionOutput::Rows(mut stream)) = handle.take_output() else {
            panic!("Rows execution must return one result stream");
        };
        stream
            .begin_schema()
            .expect("Rows execution must begin with its frozen result schema")
            .complete();
        let crate::api::ResultDelivery::Batch(delivery) = stream.next().await.unwrap().unwrap()
        else {
            panic!("the first delivery must be a result batch");
        };
        let bytes = delivery.decoded_bytes();
        delivery
            .reserve_protocol(&control.resources(), bytes)
            .unwrap()
            .begin_protocol_write(bytes)
            .unwrap()
            .complete()
            .unwrap();
        fail.notify_one();
        let Err(error) = tokio::time::timeout(Duration::from_secs(1), stream.next())
            .await
            .expect("visible failure must terminate the stream")
        else {
            panic!("visible failure must be caller-visible");
        };
        assert_eq!(error.message(), "injected failure after visible output");
        assert_eq!(prepares.load(Ordering::SeqCst), 1);
        drop(stream);
        root.business.release();
        supervisor
            .shutdown_until(Instant::now() + Duration::from_secs(1))
            .await
            .unwrap();
        acknowledge_all_control(&control);
        scope.wait_released().await;
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
                backend,
                phase: self.phase,
                convergence_calls: Arc::clone(&self.convergence_calls),
            };
            Box::pin(async move { request.bind(Vec::new(), owner).map_err(Into::into) })
        }
    }

    #[derive(Debug)]
    struct PanickingDormantOwner {
        backend: BackendProcessId,
        phase: AttemptPanicPhase,
        convergence_calls: Arc<AtomicU64>,
    }

    impl DormantNativeAttemptOwner for PanickingDormantOwner {
        fn eligible_backends(&self) -> &[BackendProcessId] {
            std::slice::from_ref(&self.backend)
        }

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
                        Ok(ActivatedNativeAttempt::completion(PanickingActiveOwner {
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
        ) -> NativeActiveAttemptConvergenceFuture<'a> {
            Box::pin(async move {
                let previous = self.convergence_calls.fetch_add(1, Ordering::SeqCst);
                if self.phase == AttemptPanicPhase::Converge && previous == 0 {
                    panic!("injected Native convergence panic");
                }
                NativeAttemptConvergence::all_workers_stopped_and_contexts_fenced()
            })
        }
    }

    async fn assert_attempt_panic_converges(
        phase: AttemptPanicPhase,
        namespace: u64,
        expected_message: &'static str,
    ) {
        let convergence_calls = Arc::new(AtomicU64::new(0));
        let attempts = PanickingAttemptPreparationPort {
            backend: BackendProcessId::new_v7(),
            phase,
            convergence_calls: Arc::clone(&convergence_calls),
        };
        let (mut supervisor, client) = LogicalExecutionSupervisor::new(
            Handle::current(),
            Arc::new(BindingNativePort),
            supervisor_resources(),
            QueryProcessNamespace::new(namespace),
            FrontendProcessId::new_v7(),
            supervisor_config(4),
        );
        let (control, root) = governance();
        let scope = root.owner.scope();

        let error = match client.start(completion_request(attempts), root.owner).await {
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
    async fn rows_activation_rejects_a_missing_root_result_runtime() {
        let convergence_calls = Arc::new(AtomicU64::new(0));
        let attempts = PanickingAttemptPreparationPort {
            backend: BackendProcessId::new_v7(),
            phase: AttemptPanicPhase::Run,
            convergence_calls: Arc::clone(&convergence_calls),
        };
        let (control, root) = governance();
        let scope = root.owner.scope();
        let (mut supervisor, client) = LogicalExecutionSupervisor::new(
            Handle::current(),
            Arc::new(BindingNativePort),
            control.resources(),
            QueryProcessNamespace::new(0x53),
            FrontendProcessId::new_v7(),
            supervisor_config(4),
        );

        let error = match client
            .start(
                rows_request(RecoveryMode::NoRecovery, None, attempts),
                root.owner,
            )
            .await
        {
            Ok(_) => panic!("Rows activation must transfer its exact pump runtime"),
            Err(error) => error,
        };
        assert_eq!(error.kind(), QueryExecutionErrorKind::InvalidRequest);
        assert_eq!(
            error.message(),
            "Native activated a row execution without its root result runtime"
        );
        root.business.release();
        assert!(matches!(
            supervisor
                .shutdown_until(Instant::now() + Duration::from_secs(1))
                .await,
            Err(LogicalExecutionSupervisorShutdownError::SupervisorFailed(_))
        ));
        acknowledge_all_control(&control);
        scope.wait_released().await;
        assert_eq!(convergence_calls.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn convergence_poll_panic_retries_without_losing_the_active_owner() {
        let convergence_calls = Arc::new(AtomicU64::new(0));
        let attempts = PanickingAttemptPreparationPort {
            backend: BackendProcessId::new_v7(),
            phase: AttemptPanicPhase::Converge,
            convergence_calls: Arc::clone(&convergence_calls),
        };
        let (mut supervisor, client) = LogicalExecutionSupervisor::new(
            Handle::current(),
            Arc::new(BindingNativePort),
            supervisor_resources(),
            QueryProcessNamespace::new(0x50),
            FrontendProcessId::new_v7(),
            supervisor_config(4),
        );
        let (control, root) = governance();
        let scope = root.owner.scope();

        let error = match client.start(completion_request(attempts), root.owner).await {
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
                        Vec::new(),
                        SuccessfulCompletionDormantOwner {
                            backend,
                            expected_frontend,
                        },
                    )
                    .map_err(Into::into)
            })
        }
    }

    #[derive(Debug)]
    struct SuccessfulCompletionDormantOwner {
        backend: BackendProcessId,
        expected_frontend: FrontendProcessId,
    }

    impl DormantNativeAttemptOwner for SuccessfulCompletionDormantOwner {
        fn eligible_backends(&self) -> &[BackendProcessId] {
            std::slice::from_ref(&self.backend)
        }

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
                Ok(ActivatedNativeAttempt::completion(
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
        ) -> NativeActiveAttemptConvergenceFuture<'a> {
            Box::pin(async { NativeAttemptConvergence::all_workers_stopped_and_contexts_fenced() })
        }
    }

    #[tokio::test]
    async fn completion_becomes_visible_only_after_exact_establish_success() {
        let frontend = FrontendProcessId::new_v7();
        let attempts = SuccessfulCompletionPreparationPort {
            backend: BackendProcessId::new_v7(),
            expected_frontend: frontend,
        };
        let (mut supervisor, client) = LogicalExecutionSupervisor::new(
            Handle::current(),
            Arc::new(BindingNativePort),
            supervisor_resources(),
            QueryProcessNamespace::new(0x50),
            frontend,
            supervisor_config(4),
        );
        let (control, root) = governance();
        let scope = root.owner.scope();
        let mut handle = client
            .start(completion_request(attempts), root.owner)
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
                    .bind(Vec::new(), PrematureCompletionDormant { backend })
                    .map_err(Into::into)
            })
        }
    }

    #[derive(Debug)]
    struct PrematureCompletionDormant {
        backend: BackendProcessId,
    }

    impl DormantNativeAttemptOwner for PrematureCompletionDormant {
        fn eligible_backends(&self) -> &[BackendProcessId] {
            std::slice::from_ref(&self.backend)
        }

        fn activate<'a>(
            &'a mut self,
            _schedule: &'a AttemptSchedule,
            _cancellation: novarocks_workload_control::CancellationView,
        ) -> NativeAttemptActivationFuture<'a> {
            Box::pin(async {
                Ok(ActivatedNativeAttempt::completion(
                    PrematureCompletionActive,
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
        ) -> NativeActiveAttemptConvergenceFuture<'a> {
            Box::pin(async { NativeAttemptConvergence::all_workers_stopped_and_contexts_fenced() })
        }
    }

    #[tokio::test]
    async fn completion_is_not_visible_without_exact_establish_success() {
        let attempts = PrematureCompletionPreparationPort {
            backend: BackendProcessId::new_v7(),
        };
        let (mut supervisor, client) = LogicalExecutionSupervisor::new(
            Handle::current(),
            Arc::new(BindingNativePort),
            supervisor_resources(),
            QueryProcessNamespace::new(0x4a),
            FrontendProcessId::new_v7(),
            supervisor_config(4),
        );
        let (control, root) = governance();
        let scope = root.owner.scope();
        let error = match client.start(completion_request(attempts), root.owner).await {
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
        let seed_drops = Arc::new(AtomicU64::new(0));
        let (mut supervisor, client) = LogicalExecutionSupervisor::new(
            Handle::current(),
            Arc::new(BindingNativePort),
            supervisor_resources(),
            QueryProcessNamespace::new(0x48),
            FrontendProcessId::new_v7(),
            supervisor_config(1),
        );
        let (first_control, first) = governance();
        let first_scope = first.owner.scope();
        let accepted = client.start(drop_tracked_request(&seed_drops), first.owner);
        let (second_control, second) = governance();
        let second_scope = second.owner.scope();
        let second_cancellation = second_scope.cancellation().unwrap();
        let rejected = client.start(drop_tracked_request(&seed_drops), second.owner);
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
        assert_eq!(seed_drops.load(Ordering::SeqCst), 1);
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
        assert_eq!(seed_drops.load(Ordering::SeqCst), 2);
        first.business.release();
        first_scope.wait_released().await;

        let (closed_control, closed) = governance();
        let closed_scope = closed.owner.scope();
        let closed_cancellation = closed_scope.cancellation().unwrap();
        let closed_error = match client
            .start(drop_tracked_request(&seed_drops), closed.owner)
            .await
        {
            Ok(_) => panic!("a closed supervisor must reject new work"),
            Err(error) => error,
        };
        assert_eq!(closed_error.kind(), QueryExecutionErrorKind::Rejected);
        assert!(
            closed_control.next_control().is_none(),
            "closed admission rejection must not manufacture cancellation work"
        );
        assert_eq!(closed_cancellation.reason(), None);
        assert_eq!(seed_drops.load(Ordering::SeqCst), 3);
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
    }

    impl LogicalExecutionNativePort for IsolatedTaskPanicNativePort {
        fn open(&self, request: LogicalNativeOpenRequest) -> LogicalNativeOpenFuture {
            if self.opens.fetch_add(1, Ordering::SeqCst) == 0 {
                self.first_opened.store(true, Ordering::SeqCst);
                return Box::pin(async { panic!("injected isolated logical task panic") });
            }
            self.second_opened.store(true, Ordering::SeqCst);
            let gate = Arc::clone(&self.second_gate);
            Box::pin(async move {
                gate.notified().await;
                request.bind().map_err(Into::into)
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
        });
        let (mut supervisor, client) = LogicalExecutionSupervisor::new(
            Handle::current(),
            native,
            supervisor_resources(),
            QueryProcessNamespace::new(0x51),
            frontend,
            supervisor_config(4),
        );
        let (first_control, first) = governance();
        let first_scope = first.owner.scope();
        let first_cancellation = first_scope.cancellation().unwrap();
        let first_result =
            client.start(completion_request(UnreachablePreparationPort), first.owner);
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
        let second_result = client.start(
            completion_request(SuccessfulCompletionPreparationPort {
                backend: BackendProcessId::new_v7(),
                expected_frontend: frontend,
            }),
            second.owner,
        );

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
            supervisor_resources(),
            QueryProcessNamespace::new(0x49),
            FrontendProcessId::new_v7(),
            supervisor_config(4),
        );
        let (control, root) = governance();
        let scope = root.owner.scope();
        let start = client.start(completion_request(UnreachablePreparationPort), root.owner);
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
            supervisor_resources(),
            QueryProcessNamespace::new(0x47),
            FrontendProcessId::new_v7(),
            supervisor_config(4),
        );
        let (control, root) = governance();
        let scope = root.owner.scope();
        let start = client.start(completion_request(UnreachablePreparationPort), root.owner);
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
            supervisor_resources(),
            QueryProcessNamespace::new(0x4f),
            FrontendProcessId::new_v7(),
            supervisor_config(1),
        );
        let (_control, root) = governance();
        let scope = root.owner.scope();
        let error = match client
            .start(completion_request(UnreachablePreparationPort), root.owner)
            .await
        {
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
