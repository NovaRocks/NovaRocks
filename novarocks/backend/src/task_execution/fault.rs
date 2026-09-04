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

//! Runner-owned perturbation of the task protocol's RPC boundary.
//!
//! Most faults here drop an acknowledgement the backend already earned: the
//! operation linearized, its receipt exists, and only the answer is lost. That
//! is the whole point of that group. A fault that skipped the operation would
//! leave the backend and the frontend agreeing that nothing happened, which no
//! replay rule has to survive; losing the answer to work that *did* happen is
//! the unknown outcome the protocol's exact-request replay exists for.
//!
//! Loss is reported as `DEADLINE_EXCEEDED`, which the frontend's
//! `classify_apply_status` maps to `RetryableTransportUnknown` — the outcome
//! whose prescribed action is `RetryExactRequest`. Anything else would tell
//! the frontend the backend had answered.
//!
//! Two faults deliberately are not acknowledgement drops, because for them the
//! answer arriving is not what the case is about.
//! [`task_execution_failure_injected`] fails a task that was admitted and
//! started, which is the only way to observe the attempt-wide stand-down one
//! participant's failure must cause; and
//! [`restart_after_establish_context`] holds an applied establish open so the
//! harness can replace that exact backend process, which is a process loss
//! rather than a lost message. Neither fabricates a success and neither skips
//! the operation it follows.
//!
//! Each fault fires once per arming: the trigger file is consumed by the
//! claim, so the replay that follows reaches an untouched boundary and settles
//! on the owner's idempotent verdict.
#![expect(
    clippy::result_large_err,
    reason = "The tonic service boundary must preserve Status without changing its generated signature."
)]

use novarocks_execution::task_execution::identity::{QueryContextRef, TaskIdentity};
use novarocks_execution::task_execution::operation::OperationOutcome;
use novarocks_failpoint::QueryLifecycleFaultKind;
use novarocks_types::identity::{BackendProcessId, QueryExecutionId};

/// Drops the acknowledgement of one applied `EstablishQueryContext`.
pub(super) fn establish_context_ack_dropped(
    context: QueryContextRef,
    outcome: OperationOutcome,
) -> Result<(), tonic::Status> {
    if outcome != OperationOutcome::Accepted {
        return Ok(());
    }
    drop_context_ack(
        QueryLifecycleFaultKind::EstablishContextAckDrop,
        "NOVAROCKS_TASK_ESTABLISH_CONTEXT_ACK_DROPPED",
        "runner-owned EstablishQueryContext acknowledgement dropped after the context was installed",
        context,
    )
}

/// What a task whose execution this fault failed reports as its cause.
///
/// It names the fault rather than an engine condition on purpose: this text
/// reaches the client through the attempt's termination cause, and a message
/// that read like a real execution error would make an injected failure
/// indistinguishable from a genuine one in a test log.
pub(super) const TASK_EXECUTION_FAILURE_DETAIL: &str =
    "runner-owned task execution failure injected after the task started";

/// Fails one admitted task's local execution.
///
/// Claimed on the creation path, where a malformed arming can still be
/// reported as a typed rejection, and applied by the worker thread once the
/// task has published RUNNING. The task is therefore genuinely admitted and
/// genuinely started before it fails, which is what makes the resulting
/// stand-down the attempt's answer to a participant failure rather than to a
/// refused create.
///
/// It fires once per arming, so a second task of the same attempt runs
/// untouched and its termination is caused by the first one's failure rather
/// than by another injection.
pub(super) fn task_execution_failure_injected(identity: TaskIdentity) -> Result<bool, String> {
    let execution = identity.query_execution_id();
    let Some(scope) = claim_by_detail(
        QueryLifecycleFaultKind::TaskExecutionFailure,
        execution,
        identity.backend_process_id(),
    )?
    else {
        return Ok(false);
    };
    eprintln!(
        "NOVAROCKS_TASK_EXECUTION_FAILURE_INJECTED execution_id={}:{}:{} stage={} task={} backend_index={} token={}",
        execution.query_id().high(),
        execution.query_id().low(),
        execution.attempt_id().get(),
        identity.stage_id().get(),
        identity.task_id().get(),
        scope.backend_index,
        scope.token,
    );
    Ok(true)
}

/// Holds one applied `EstablishQueryContext` open until this process is gone.
///
/// The context is installed before the wait begins, so the process the harness
/// replaces is one that really did admit this attempt. Without the wait a
/// short query can finish between the marker and the harness's kill, which
/// proves nothing about loss after admission -- the same reason the retired
/// protocol's restart rendezvous waited at its own applied Init.
///
/// It returns normally if the wait runs out. The harness is the only thing
/// that ends this wait deliberately, so a timeout means the harness never
/// acted, and reporting that as a backend error would blame this process for
/// the harness's own missed deadline.
pub(super) fn restart_after_establish_context(
    context: QueryContextRef,
    outcome: OperationOutcome,
) -> Result<(), tonic::Status> {
    if outcome != OperationOutcome::Accepted {
        return Ok(());
    }
    let execution = context.query_execution_id();
    let Some(scope) = claim(
        QueryLifecycleFaultKind::RestartAfterEstablishContext,
        execution,
        context.backend_process_id(),
    )?
    else {
        return Ok(());
    };
    eprintln!(
        "NOVAROCKS_TASK_ESTABLISH_CONTEXT_OBSERVED execution_id={}:{}:{} backend_index={} process_id={} token={}",
        execution.query_id().high(),
        execution.query_id().low(),
        execution.attempt_id().get(),
        scope.backend_index,
        scope.process_id,
        scope.token,
    );
    wait_for_runner_owned_restart(&scope);
    Ok(())
}

/// Blocks until this process is replaced, this exact arming is released, or
/// the wait runs out.
///
/// Being killed is the normal end: the harness replaces the process while
/// this thread is parked, so the wait usually ends with the process rather
/// than with a release. The release path exists so a harness that only wants
/// the rendezvous, without a replacement, can let the establish answer.
#[cfg(debug_assertions)]
fn wait_for_runner_owned_restart(scope: &novarocks_failpoint::QueryLifecycleFaultScope) {
    const RESTART_WAIT: std::time::Duration = std::time::Duration::from_secs(30);
    const RESTART_POLL: std::time::Duration = std::time::Duration::from_millis(1);
    let Some(root) = novarocks_failpoint::configured_root() else {
        return;
    };
    let release = novarocks_failpoint::trigger_path(
        &root,
        scope.backend_index,
        QueryLifecycleFaultKind::RestartAfterEstablishContext,
    )
    .with_extension("release");
    let deadline = std::time::Instant::now() + RESTART_WAIT;
    while std::time::Instant::now() < deadline {
        match std::fs::read_to_string(&release) {
            Ok(token) if token.trim() == scope.token => {
                let _ = std::fs::remove_file(&release);
                return;
            }
            Ok(_) => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(_) => return,
        }
        std::thread::sleep(RESTART_POLL);
    }
}

#[cfg(not(debug_assertions))]
fn wait_for_runner_owned_restart(_scope: &novarocks_failpoint::QueryLifecycleFaultScope) {}

/// Drops the acknowledgement of one applied lease renewal.
pub(super) fn lease_renewal_ack_dropped(
    context: QueryContextRef,
    outcome: OperationOutcome,
) -> Result<(), tonic::Status> {
    if outcome != OperationOutcome::Accepted {
        return Ok(());
    }
    drop_context_ack(
        QueryLifecycleFaultKind::LeaseRenewalAckDrop,
        "NOVAROCKS_TASK_LEASE_RENEWAL_ACK_DROPPED",
        "runner-owned lease renewal acknowledgement dropped after the lease was extended",
        context,
    )
}

/// Refuses every lease renewal of this attempt for as long as the fault stays
/// armed, so the lease genuinely expires.
///
/// This is the failure path a single dropped acknowledgement cannot reach: the
/// frontend resends a lost renewal and the lease survives, which is exactly
/// what `lease-renewal-ack-drop` asserts. Letting the lease run out instead is
/// what makes the backend stand its tasks down, and the arming is therefore
/// matched without being consumed -- consuming it would let the second renewal
/// through and keep the lease alive.
pub(super) fn lease_renewal_stopped(context: QueryContextRef) -> Result<bool, tonic::Status> {
    let Some(scope) = match_persistent(
        QueryLifecycleFaultKind::LeaseRenewalStop,
        context.query_execution_id(),
        context.backend_process_id(),
    )?
    else {
        return Ok(false);
    };
    let execution = context.query_execution_id();
    eprintln!(
        "NOVAROCKS_TASK_LEASE_RENEWAL_STOPPED execution_id={}:{}:{} backend_index={} token={}",
        execution.query_id().high(),
        execution.query_id().low(),
        execution.attempt_id().get(),
        scope.backend_index,
        scope.token,
    );
    Ok(true)
}

/// Drops the acknowledgement of one admitted `CreateTask`.
///
/// It claims only on an accepted create, so a converging duplicate or a
/// refused descriptor leaves the arming for the create that really admitted a
/// task.
/// Drops the acknowledgement of a task update that both carried splits and
/// closed its plan node.
///
/// The retired `TaskUpdate` RPC claimed this fault in its own handler. The
/// cutover moved split delivery onto `ApplyTaskOperations`, so the claim has
/// to move with it -- otherwise the fault is armed, nothing consumes it, the
/// query succeeds untouched, and the case asserting the drop waits out its
/// whole budget for a marker that no longer has an emitter.
///
/// Only a terminal, non-empty delivery claims it. A malformed, refused or
/// non-terminal one must leave the token for the real terminal
/// acknowledgement, which is the case's actual subject.
pub(super) fn task_update_terminal_ack_dropped(
    identity: TaskIdentity,
    outcome: OperationOutcome,
    terminal_nonempty: bool,
) -> Result<(), tonic::Status> {
    if !terminal_nonempty || outcome != OperationOutcome::Accepted {
        return Ok(());
    }
    let execution = identity.query_execution_id();
    let Some(scope) = claim(
        QueryLifecycleFaultKind::TaskUpdateTerminalAckDrop,
        execution,
        identity.backend_process_id(),
    )?
    else {
        return Ok(());
    };
    eprintln!(
        "NOVAROCKS_TASK_UPDATE_TERMINAL_ACK_DROPPED execution_id={}:{}:{} backend_index={} token={}",
        execution.query_id().high(),
        execution.query_id().low(),
        execution.attempt_id().get(),
        scope.backend_index,
        scope.token,
    );
    Err(tonic::Status::deadline_exceeded(
        "runner-owned TaskUpdate terminal acknowledgement dropped after acceptance",
    ))
}

pub(super) fn create_task_ack_dropped(
    identity: TaskIdentity,
    outcome: OperationOutcome,
) -> Result<(), tonic::Status> {
    if outcome != OperationOutcome::Accepted {
        return Ok(());
    }
    let execution = identity.query_execution_id();
    let Some(scope) = claim(
        QueryLifecycleFaultKind::CreateTaskAckDrop,
        execution,
        identity.backend_process_id(),
    )?
    else {
        return Ok(());
    };
    eprintln!(
        "NOVAROCKS_TASK_CREATE_ACK_DROPPED execution_id={}:{}:{} stage={} task={} backend_index={} token={}",
        execution.query_id().high(),
        execution.query_id().low(),
        execution.attempt_id().get(),
        identity.stage_id().get(),
        identity.task_id().get(),
        scope.backend_index,
        scope.token,
    );
    Err(tonic::Status::deadline_exceeded(
        "runner-owned CreateTask acknowledgement dropped after the task was admitted",
    ))
}

/// Drops one established `SubscribeTaskStatus` stream.
///
/// Reported as `Ok(true)`, not as an error: the subscription really was
/// established, so the drop belongs to the stream body rather than to the call
/// that opened it. Nothing was consumed — the per-task cursors are read-only —
/// so the frontend's resubscription sees exactly what this stream would have
/// carried.
pub(super) fn task_status_subscription_dropped(
    context: QueryContextRef,
) -> Result<bool, tonic::Status> {
    let execution = context.query_execution_id();
    let Some(scope) = claim(
        QueryLifecycleFaultKind::TaskStatusSubscriptionDrop,
        execution,
        context.backend_process_id(),
    )?
    else {
        return Ok(false);
    };
    eprintln!(
        "NOVAROCKS_TASK_STATUS_SUBSCRIPTION_DROPPED execution_id={}:{}:{} backend_index={} token={}",
        execution.query_id().high(),
        execution.query_id().low(),
        execution.attempt_id().get(),
        scope.backend_index,
        scope.token,
    );
    Ok(true)
}

fn drop_context_ack(
    kind: QueryLifecycleFaultKind,
    marker: &str,
    detail: &'static str,
    context: QueryContextRef,
) -> Result<(), tonic::Status> {
    let execution = context.query_execution_id();
    let Some(scope) = claim(kind, execution, context.backend_process_id())? else {
        return Ok(());
    };
    eprintln!(
        "{marker} execution_id={}:{}:{} backend_index={} token={}",
        execution.query_id().high(),
        execution.query_id().low(),
        execution.attempt_id().get(),
        scope.backend_index,
        scope.token,
    );
    Err(tonic::Status::deadline_exceeded(detail))
}

#[cfg(debug_assertions)]
fn match_persistent(
    kind: QueryLifecycleFaultKind,
    execution_id: QueryExecutionId,
    process_id: BackendProcessId,
) -> Result<Option<novarocks_failpoint::QueryLifecycleFaultScope>, tonic::Status> {
    let Some(root) = novarocks_failpoint::configured_root() else {
        return Ok(None);
    };
    let backend_index = std::env::var("NOVAROCKS_SQL_TEST_QUERY_LIFECYCLE_BACKEND_INDEX")
        .map_err(|_| tonic::Status::failed_precondition("lifecycle fault backend index is unset"))?
        .parse::<usize>()
        .map_err(|error| {
            tonic::Status::failed_precondition(format!(
                "invalid lifecycle fault backend index: {error}"
            ))
        })?;
    novarocks_failpoint::match_persistent_fault(
        &root,
        kind,
        execution_id,
        backend_index,
        process_id,
    )
    .map_err(tonic::Status::failed_precondition)
}

#[cfg(not(debug_assertions))]
fn match_persistent(
    _kind: QueryLifecycleFaultKind,
    _execution_id: QueryExecutionId,
    _process_id: BackendProcessId,
) -> Result<Option<novarocks_failpoint::QueryLifecycleFaultScope>, tonic::Status> {
    Ok(None)
}

#[cfg(debug_assertions)]
fn claim(
    kind: QueryLifecycleFaultKind,
    execution_id: QueryExecutionId,
    process_id: BackendProcessId,
) -> Result<Option<novarocks_failpoint::QueryLifecycleFaultScope>, tonic::Status> {
    let Some(root) = novarocks_failpoint::configured_root() else {
        return Ok(None);
    };
    let backend_index = std::env::var("NOVAROCKS_SQL_TEST_QUERY_LIFECYCLE_BACKEND_INDEX")
        .map_err(|_| tonic::Status::failed_precondition("lifecycle fault backend index is unset"))?
        .parse::<usize>()
        .map_err(|error| {
            tonic::Status::failed_precondition(format!(
                "invalid lifecycle fault backend index: {error}"
            ))
        })?;
    novarocks_failpoint::claim_matching_fault(&root, kind, execution_id, backend_index, process_id)
        .map_err(tonic::Status::failed_precondition)
}

#[cfg(not(debug_assertions))]
fn claim(
    _kind: QueryLifecycleFaultKind,
    _execution_id: QueryExecutionId,
    _process_id: BackendProcessId,
) -> Result<Option<novarocks_failpoint::QueryLifecycleFaultScope>, tonic::Status> {
    Ok(None)
}

/// The same claim, for a caller that is not on the tonic boundary.
///
/// The execution-side host reports a rejection as a typed `HostRejection`, so
/// it cannot receive a `tonic::Status`. The detail text is handed back
/// unchanged and the caller decides which category it belongs to; nothing here
/// invents an outcome.
#[cfg(debug_assertions)]
fn claim_by_detail(
    kind: QueryLifecycleFaultKind,
    execution_id: QueryExecutionId,
    process_id: BackendProcessId,
) -> Result<Option<novarocks_failpoint::QueryLifecycleFaultScope>, String> {
    let Some(root) = novarocks_failpoint::configured_root() else {
        return Ok(None);
    };
    let backend_index = std::env::var("NOVAROCKS_SQL_TEST_QUERY_LIFECYCLE_BACKEND_INDEX")
        .map_err(|_| "lifecycle fault backend index is unset".to_owned())?
        .parse::<usize>()
        .map_err(|error| format!("invalid lifecycle fault backend index: {error}"))?;
    novarocks_failpoint::claim_matching_fault(&root, kind, execution_id, backend_index, process_id)
}

#[cfg(not(debug_assertions))]
fn claim_by_detail(
    _kind: QueryLifecycleFaultKind,
    _execution_id: QueryExecutionId,
    _process_id: BackendProcessId,
) -> Result<Option<novarocks_failpoint::QueryLifecycleFaultScope>, String> {
    Ok(None)
}
