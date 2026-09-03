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

//! Runner-owned acknowledgement loss on the task protocol's RPC boundary.
//!
//! Every fault here drops an acknowledgement the backend already earned: the
//! operation linearized, its receipt exists, and only the answer is lost. That
//! is the whole point. A fault that skipped the operation would leave the
//! backend and the frontend agreeing that nothing happened, which no replay
//! rule has to survive; losing the answer to work that *did* happen is the
//! unknown outcome the protocol's exact-request replay exists for.
//!
//! Loss is reported as `DEADLINE_EXCEEDED`, which the frontend's
//! `classify_apply_status` maps to `RetryableTransportUnknown` — the outcome
//! whose prescribed action is `RetryExactRequest`. Anything else would tell
//! the frontend the backend had answered.
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
