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

//! Runner-owned perturbation at the Native task-protocol boundary.
//!
//! Most faults here drop an acknowledgement the backend already earned: the
//! operation linearized, its receipt exists, and only the answer is lost. That
//! is the whole point of that group. A fault that skipped the operation would
//! leave the backend and the frontend agreeing that nothing happened, which no
//! replay rule has to survive; losing the answer to work that *did* happen is
//! the unknown outcome the protocol's exact-request replay exists for.
//!
//! Loss is reported as `DEADLINE_EXCEEDED`, which the frontend's
//! `classify_apply_status` maps to a transport-unknown dispatch result, whose
//! prescribed frontend action is exact-request replay. Anything else would
//! tell the frontend the backend had answered.
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
//! A third group perturbs the dynamic-filter feedback this backend publishes
//! ([`corrupt_feedback_contract_digest`], [`force_feedback_unavailable`],
//! [`forge_feedback_foreign_attempt`]). They are not acknowledgement drops
//! either: feedback is a retained payload the frontend polls, so there is no
//! answer to lose. They perturb the payload's own facts and are claimed at the
//! publication rather than where the sink is installed, because a fault
//! claimed at install time would say only that a carrier exists.
//!
//! A fourth group misstates one fact on the wire about an operation that
//! genuinely applied ([`create_task_rejected_after_apply`],
//! [`create_task_receipt_foreign_task`], [`task_status_foreign_process`]).
//! They are the task protocol's identity fences seen from the outside: the
//! backend's own state is untouched and correct, and only the value the
//! frontend is told is wrong -- a verdict, an acknowledgement's task, or an
//! event's backend process. Requiring the operation to have applied first is
//! what makes them provable rather than merely red: a frontend that failed to
//! fence any of the three would find real, working state behind the lie and
//! the query would succeed.
//!
//! Most faults fire once per arming: the trigger file is consumed by the
//! claim, so the replay that follows reaches an untouched boundary and settles
//! on the owner's idempotent verdict.
//!
//! Two deliberately do not, and both are cases where the frontend's *own*
//! replay is what the fault has to survive. [`lease_renewal_stopped`] must
//! refuse every renewal or the lease never expires, and
//! [`restart_after_establish_context`] must withhold every establish of its
//! attempt or the replay's idempotent answer closes the pre-ready window the
//! case exists to observe. Both therefore match the arming without consuming
//! it, and both are scoped to one exact attempt and one exact backend process,
//! so a replanned attempt is untouched.
#![expect(
    clippy::result_large_err,
    reason = "The tonic service boundary must preserve Status without changing its generated signature."
)]

use std::{
    io::{Read, Write},
    net::Shutdown,
    os::unix::net::UnixStream,
};

use crate::test_controls::{NativeFaultClaim, NativeTestControls};
use novarocks_execution_contract::task_execution::identity::{QueryContextRef, TaskIdentity};
use novarocks_execution_contract::task_execution::operation::OperationOutcome;
use novarocks_failpoint::QueryLifecycleFaultKind;
use novarocks_proto_models::novarocks as proto;
use novarocks_types::identity::{BackendProcessId, QueryExecutionId};

/// Selects a finite direct hold only for the exact nominated admission context.
pub fn claim_native_ingress_hold(
    controls: &NativeTestControls,
    request: &tonic::Request<proto::ApplyTaskOperationsRequest>,
) -> Result<Option<crate::test_controls::NativeHoldClaim>, tonic::Status> {
    let Some(token) = request.metadata().get("x-novarocks-ingress-hold-token") else {
        return Ok(None);
    };
    let token = token
        .to_str()
        .map_err(|_| tonic::Status::invalid_argument("native ingress hold token is not ASCII"))?;
    claim_batch_hold(controls, request.get_ref(), "ingress-hold", Some(token))
}
pub fn claim_native_registry_hold(
    controls: &NativeTestControls,
    request: &proto::ApplyTaskOperationsRequest,
) -> Result<Option<crate::test_controls::NativeHoldClaim>, tonic::Status> {
    claim_batch_hold(controls, request, "registry-hold", None)
}
fn claim_batch_hold(
    controls: &NativeTestControls,
    request: &proto::ApplyTaskOperationsRequest,
    action: &str,
    token: Option<&str>,
) -> Result<Option<crate::test_controls::NativeHoldClaim>, tonic::Status> {
    let operations = novarocks_task_codec::operation::decode_ordinary_operation_batch(
        request,
        novarocks_task_codec::TransportBudget::DEFAULT,
        novarocks_proto_codec::FieldPath::root("hold_target"),
    )
    .map_err(|error| tonic::Status::invalid_argument(error.to_string()))?;
    for operation in operations {
        if let novarocks_task_codec::operation::DecodedOperation::AcquireQueryContextAdmissionTicket(
            request,
        ) = operation
            && let Some(claim) = controls
                .claim_context_hold(action, request.context(), token)
                .map_err(tonic::Status::failed_precondition)?
        {
            return Ok(Some(claim));
        }
    }
    Ok(None)
}
pub fn hold_native_registry_lock(
    mut claim: crate::test_controls::NativeHoldClaim,
    request_deadline: std::time::Instant,
) -> Result<(), tonic::Status> {
    let path = novarocks_failpoint::native_registry_hold_socket_path(&claim.token)
        .map_err(tonic::Status::invalid_argument)?;
    let marker = format!(
        "NIR1 {} {} {}\n",
        claim.backend_index, claim.process_id, claim.token
    );
    hold_socket(&path, marker.as_bytes(), &mut claim, request_deadline)
        .map_err(tonic::Status::failed_precondition)
}
pub fn hold_native_ingress_ordinary_closure(
    mut claim: crate::test_controls::NativeHoldClaim,
    request_deadline: std::time::Instant,
) -> Result<(), tonic::Status> {
    let path = novarocks_failpoint::native_ingress_hold_socket_path(&claim.token)
        .map_err(tonic::Status::invalid_argument)?;
    let marker = format!(
        "NIH1 {} {} {}\n",
        claim.backend_index, claim.process_id, claim.token
    );
    hold_socket(&path, marker.as_bytes(), &mut claim, request_deadline)
        .map_err(tonic::Status::failed_precondition)
}
fn hold_socket(
    path: &std::path::Path,
    marker: &[u8],
    claim: &mut crate::test_controls::NativeHoldClaim,
    request_deadline: std::time::Instant,
) -> Result<(), String> {
    let cancellation = claim.cancellation();
    let deadline = request_deadline
        .min(cancellation.deadline())
        .min(std::time::Instant::now() + std::time::Duration::from_secs(30));
    if !cancellation.is_active() || std::time::Instant::now() >= deadline {
        return Ok(());
    }
    let mut stream = match novarocks_failpoint::liveness::connect_until(path, deadline, || {
        !cancellation.is_active()
    }) {
        Ok(stream) => stream,
        Err(_) if !cancellation.is_active() || std::time::Instant::now() >= deadline => {
            return Ok(());
        }
        Err(error) => return Err(format!("connect Native hold runner: {error}")),
    };
    if !cancellation.is_active() || std::time::Instant::now() >= deadline {
        return Ok(());
    }
    stream
        .set_write_timeout(Some(
            deadline.saturating_duration_since(std::time::Instant::now()),
        ))
        .map_err(|error| error.to_string())?;
    stream
        .write_all(marker)
        .map_err(|error| format!("write Native hold identity: {error}"))?;
    stream
        .shutdown(Shutdown::Write)
        .map_err(|error| error.to_string())?;
    claim.record_hit()?;
    read_release(&mut stream, &cancellation, deadline, false)
}
fn read_release(
    stream: &mut UnixStream,
    cancellation: &novarocks_failpoint::scope::ControlCancellation,
    deadline: std::time::Instant,
    allow_eof: bool,
) -> Result<(), String> {
    read_release_with_end(stream, cancellation, deadline, allow_eof, || Ok(false))
}
fn read_release_with_end(
    stream: &mut UnixStream,
    cancellation: &novarocks_failpoint::scope::ControlCancellation,
    deadline: std::time::Instant,
    allow_eof: bool,
    ended: impl Fn() -> Result<bool, String>,
) -> Result<(), String> {
    let mut release = [0_u8; 1];
    while cancellation.is_active() && std::time::Instant::now() < deadline {
        if ended()? {
            return Ok(());
        }
        let remaining = deadline.saturating_duration_since(std::time::Instant::now());
        if remaining.is_zero() {
            break;
        }
        stream
            .set_read_timeout(Some(remaining.min(std::time::Duration::from_millis(20))))
            .map_err(|error| error.to_string())?;
        let read = stream.read(&mut release);
        if ended()? {
            return Ok(());
        }
        match read {
            Ok(0) if allow_eof => return Ok(()),
            Ok(1) if release == *b"R" && !allow_eof => return Ok(()),
            Ok(_) => {
                return Err("Native hold runner disconnected or sent an invalid release".into());
            }
            Err(error)
                if matches!(
                    error.kind(),
                    std::io::ErrorKind::TimedOut | std::io::ErrorKind::WouldBlock
                ) => {}
            Err(error) => return Err(format!("Native hold runner read failed: {error}")),
        }
    }
    if ended()? {
        return Ok(());
    }
    Err("Native hold session closed or original deadline elapsed".into())
}

/// Drops the acknowledgement of one applied `EstablishQueryContext`.
pub fn establish_context_ack_dropped(
    controls: &NativeTestControls,
    context: QueryContextRef,
    outcome: OperationOutcome,
) -> Result<(), tonic::Status> {
    if outcome != OperationOutcome::Accepted {
        return Ok(());
    }
    drop_context_ack(
        controls,
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
pub const TASK_EXECUTION_FAILURE_DETAIL: &str =
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
pub fn task_execution_failure_injected(
    controls: &NativeTestControls,
    identity: TaskIdentity,
    root_result: bool,
) -> Result<Option<NativeFaultClaim>, String> {
    let ordinary = controls.claim_task(
        QueryLifecycleFaultKind::TaskExecutionFailure,
        identity,
        false,
    )?;
    if ordinary.is_some() || !root_result {
        return Ok(ordinary);
    }
    // The caller derives this capability from its actual installed Root channel.
    // An ordinary Task must leave the Root-only action unclaimed.
    claim_by_detail(
        controls,
        QueryLifecycleFaultKind::RootTaskExecutionFailureBeforeData,
        identity.query_execution_id(),
        identity.backend_process_id(),
    )
}
#[derive(Debug)]
enum TaskExecutionFaultFailure {
    Injected,
    PreparedExitInjected,
    Evidence(novarocks_execution_contract::failure::BoundedDetail),
}
impl std::fmt::Display for TaskExecutionFaultFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Injected => f.write_str(TASK_EXECUTION_FAILURE_DETAIL),
            Self::PreparedExitInjected => f.write_str(
                "runner-owned task preparation failure injected after kernel preparation",
            ),
            Self::Evidence(detail) => {
                write!(f, "Native task execution fault evidence failed: {detail}")
            }
        }
    }
}
impl std::error::Error for TaskExecutionFaultFailure {}
impl novarocks_execution_contract::failure::FailureProjection for TaskExecutionFaultFailure {
    fn failure_fact(&self) -> novarocks_execution_contract::failure::FailureFact {
        use novarocks_execution_contract::failure::*;
        FailureFact::new(
            FailureClass::Unexpected,
            FailureReason::new(
                domain::NATIVE,
                std::num::NonZeroU16::new(match self {
                    Self::Injected | Self::PreparedExitInjected => 20,
                    Self::Evidence(_) => 21,
                })
                .unwrap(),
            ),
        )
    }
}
pub fn injected_task_execution_failure() -> novarocks_execution::runtime::fragment::ExecutionFailure
{
    novarocks_execution::runtime::fragment::ExecutionFailure::from_domain(
        TaskExecutionFaultFailure::Injected,
    )
}
pub(crate) fn injected_prepared_task_failure()
-> novarocks_execution::runtime::fragment::ExecutionFailure {
    novarocks_execution::runtime::fragment::ExecutionFailure::from_domain(
        TaskExecutionFaultFailure::PreparedExitInjected,
    )
}
pub fn record_task_execution_failure(
    identity: TaskIdentity,
    scope: &NativeFaultClaim,
) -> novarocks_execution::runtime::fragment::ExecutionResult<()> {
    scope.record_hit().map_err(|detail| {
        novarocks_execution::runtime::fragment::ExecutionFailure::from_domain(
            TaskExecutionFaultFailure::Evidence(
                novarocks_execution_contract::failure::BoundedDetail::truncating(&detail),
            ),
        )
    })?;
    let execution = identity.query_execution_id();
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
    Ok(())
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
///
/// # Why the arming is matched rather than consumed
///
/// The hold has to cover every establish of this attempt, not just the one it
/// started on, and the protocol guarantees there will be more. The frontend
/// requests `MaxWait::DEFAULT_CREATE` (15 s) for an `UpdateQueryContext` while
/// this rendezvous waits up to 30 s, so a hold that outlasts the frontend's own
/// wait is the normal case: the operation times out client-side and
/// `QueryContextOwner` replays the exact same request.
///
/// That replay reaches a process whose context is already installed, so the
/// registry answers `Idempotent` -- and `is_applied()` counts `Idempotent`, so
/// the frontend would mark the establish acknowledged, `contexts_established()`
/// would flip, and `close_after_control_ready()` would shut the pre-ready retry
/// window. The kill that follows would then be a post-ControlReady loss, which
/// is correctly not replannable. The case asserting the replan would be
/// asserting the opposite of what it set up, and only when the harness happens
/// to be slower than 15 s -- a flake, not a failure.
///
/// So a one-shot claim is the wrong shape here. The arming is matched without
/// being consumed, and every establish delivery is withheld until the process
/// is replaced. Returning an immediate deadline for an idempotent replay would
/// let the frontend mint another establish authorization before the runner can
/// complete the replacement, turning this process-loss rendezvous into an
/// artificial authorization-budget failure.
///
/// The hold itself is armed immediately before the registry applies the
/// establish. A create can already be waiting on the registry's creation gate;
/// recording the hold only after the establish returns would let that waiter
/// reserve a task in the narrow gate-open interval before this handler publishes
/// its marker.
pub fn arm_restart_after_establish_context(
    controls: &NativeTestControls,
    context: QueryContextRef,
) -> Result<(), tonic::Status> {
    let execution = context.query_execution_id();
    let Some(scope) = match_persistent(
        controls,
        QueryLifecycleFaultKind::RestartAfterEstablishContext,
        execution,
        context.backend_process_id(),
    )?
    else {
        return Ok(());
    };
    controls.arm_restart(context, scope);
    Ok(())
}

pub fn restart_after_establish_context(
    controls: &NativeTestControls,
    context: QueryContextRef,
    outcome: OperationOutcome,
) -> Result<(), tonic::Status> {
    if !matches!(
        outcome,
        OperationOutcome::Accepted | OperationOutcome::Idempotent
    ) {
        controls.disarm_restart(context);
        return Ok(());
    }
    let execution = context.query_execution_id();
    let Some(scope) = controls.restart_claim(context) else {
        return Ok(());
    };
    if outcome == OperationOutcome::Accepted {
        eprintln!(
            "NOVAROCKS_TASK_ESTABLISH_CONTEXT_OBSERVED execution_id={}:{}:{} backend_index={} process_id={} token={}",
            execution.query_id().high(),
            execution.query_id().low(),
            execution.attempt_id().get(),
            scope.backend_index,
            scope.process_id,
            scope.token,
        );
    }
    notify_runner_of_restart_after_establish(&scope)?;
    wait_for_runner_owned_restart(&scope);
    if outcome == OperationOutcome::Accepted {
        controls.disarm_restart(context);
    }
    Ok(())
}

/// Holds task creation for an establish rendezvous that has already published
/// its marker.
///
/// The establish RPC and task-create RPCs have distinct gRPC handlers. Parking
/// only the former leaves a scheduler race in which a parallel create can be
/// admitted after the marker but before the runner kills the process. That is
/// a different failure boundary from the one this fault claims to construct:
/// the context would become ControlReady before the process loss. The hold is
/// scoped to the exact context and exists only while the accepted establish is
/// parked, so it neither changes normal admission nor broadens the fault to
/// other attempts.
pub fn restart_after_establish_context_holds_task_creation(
    controls: &NativeTestControls,
    context: QueryContextRef,
) -> bool {
    controls.restart_claim(context).is_some()
}

/// Waits for the exact runner-owned restart that holds this context's task
/// creation. The scope was validated when the accepted establish armed the
/// hold, so the registry can call this at its linearization point without
/// re-reading fault configuration while it owns its lock.
pub fn wait_for_restart_after_establish_context(
    controls: &NativeTestControls,
    context: QueryContextRef,
) {
    if let Some(scope) = controls.restart_claim(context) {
        wait_for_runner_owned_restart(&scope);
    }
}

/// Native fault adapter for the Worker task-creation gate.
pub struct RestartAfterEstablishTaskCreationGate(pub std::sync::Arc<NativeTestControls>);

impl novarocks_worker::TaskCreationGate for RestartAfterEstablishTaskCreationGate {
    fn holds_task_creation(&self, context: QueryContextRef) -> bool {
        restart_after_establish_context_holds_task_creation(&self.0, context)
    }

    fn wait_for_task_creation_release(&self, context: QueryContextRef) {
        wait_for_restart_after_establish_context(&self.0, context);
    }
}

/// Blocks until this process is replaced, this exact arming is released, or
/// the wait runs out.
///
/// Being killed is the normal end: the harness replaces the process while
/// this thread is parked, so the wait usually ends with the process rather
/// than with a release. The release path exists so a harness that only wants
/// the rendezvous, without a replacement, can let the establish answer.
fn wait_for_runner_owned_restart(scope: &NativeFaultClaim) {
    const RESTART_WAIT: std::time::Duration = std::time::Duration::from_secs(30);
    const RESTART_POLL: std::time::Duration = std::time::Duration::from_millis(1);
    let root = scope.root();
    let release = novarocks_failpoint::trigger_path(
        root,
        scope.backend_index,
        QueryLifecycleFaultKind::RestartAfterEstablishContext,
    )
    .with_extension("release");
    let deadline = std::time::Instant::now() + RESTART_WAIT;
    while scope.cancellation().is_active() && std::time::Instant::now() < deadline {
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

/// Hands the accepted establish directly to the runner-owned process-loss
/// action before leaving the RPC handler.
///
/// A log marker is proof, not synchronization: a polling harness can observe
/// it after the frontend already spent its exact request's authorization
/// budget. The socket is bound before the query begins. The BE writes its
/// token and waits for EOF; the runner deliberately keeps the connection open
/// until it has completed the restart action, whose first operation kills this
/// process. EOF is therefore the runner's acknowledgement, without admitting
/// another establish while the action is still pending.
fn notify_runner_of_restart_after_establish(scope: &NativeFaultClaim) -> Result<(), tonic::Status> {
    let path = novarocks_failpoint::restart_after_establish_rendezvous_socket_path(&scope.token)
        .map_err(|error| tonic::Status::failed_precondition(error.to_string()))?;
    let deadline = (std::time::Instant::now() + std::time::Duration::from_secs(30))
        .min(scope.cancellation().deadline());
    let mut stream = match novarocks_failpoint::liveness::connect_until(&path, deadline, || {
        !scope.cancellation().is_active()
    }) {
        Ok(stream) => stream,
        Err(_) if !scope.cancellation().is_active() => return Ok(()),
        Err(error) => {
            return Err(tonic::Status::failed_precondition(format!(
                "restart-after-establish runner rendezvous is unavailable: {error}"
            )));
        }
    };
    if !scope.cancellation().is_active() || std::time::Instant::now() >= deadline {
        return Ok(());
    }
    stream
        .set_write_timeout(Some(
            deadline
                .saturating_duration_since(std::time::Instant::now())
                .max(std::time::Duration::from_nanos(1)),
        ))
        .map_err(|error| {
            tonic::Status::internal(format!("set restart rendezvous write deadline: {error}"))
        })?;
    stream.write_all(scope.token.as_bytes()).map_err(|error| {
        tonic::Status::internal(format!(
            "write restart-after-establish rendezvous token: {error}"
        ))
    })?;
    stream.shutdown(Shutdown::Write).map_err(|error| {
        tonic::Status::internal(format!(
            "close restart-after-establish rendezvous write side: {error}"
        ))
    })?;
    scope
        .record_hit()
        .map_err(tonic::Status::failed_precondition)?;
    read_release(&mut stream, &scope.cancellation(), deadline, true)
        .map_err(tonic::Status::failed_precondition)
}

/// Drops the acknowledgement of one applied lease renewal.
pub fn lease_renewal_ack_dropped(
    controls: &NativeTestControls,
    context: QueryContextRef,
    outcome: OperationOutcome,
) -> Result<(), tonic::Status> {
    if outcome != OperationOutcome::Accepted {
        return Ok(());
    }
    drop_context_ack(
        controls,
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
pub fn lease_renewal_stopped(
    controls: &NativeTestControls,
    context: QueryContextRef,
) -> Result<bool, tonic::Status> {
    let Some(scope) = match_persistent(
        controls,
        QueryLifecycleFaultKind::LeaseRenewalStop,
        context.query_execution_id(),
        context.backend_process_id(),
    )?
    else {
        return Ok(false);
    };
    let execution = context.query_execution_id();
    scope
        .record_hit()
        .map_err(tonic::Status::failed_precondition)?;
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
pub fn task_update_terminal_ack_dropped(
    controls: &NativeTestControls,
    identity: TaskIdentity,
    outcome: OperationOutcome,
    terminal_nonempty: bool,
    activate: impl FnOnce() -> Result<(), String>,
) -> Result<(), tonic::Status> {
    if !terminal_nonempty || outcome != OperationOutcome::Accepted {
        return Ok(());
    }
    let execution = identity.query_execution_id();
    let Some(scope) = claim(
        controls,
        QueryLifecycleFaultKind::TaskUpdateTerminalAckDrop,
        execution,
        identity.backend_process_id(),
    )?
    else {
        return Ok(());
    };
    activate().map_err(tonic::Status::failed_precondition)?;
    scope
        .record_hit()
        .map_err(tonic::Status::failed_precondition)?;
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

/// Holds only a runner-armed exact Create before the Worker accepts it.
/// Establishment and status subscription remain ordinary independent paths.
pub fn create_task_before_worker_hold(
    controls: &NativeTestControls,
    identity: TaskIdentity,
    outbound_edges: usize,
) -> Result<(), tonic::Status> {
    {
        let execution = identity.query_execution_id();
        let Some(scope) = controls
            .claim_task(
                QueryLifecycleFaultKind::CreateTaskBeforeWorkerHold,
                identity,
                true,
            )
            .map_err(tonic::Status::failed_precondition)?
        else {
            return Ok(());
        };
        let payload = format!(
            "token={} execution_id={}:{}:{} stage={} task={} backend_index={} process_id={} outbound_edges={}\n",
            scope.token,
            execution.query_id().high(),
            execution.query_id().low(),
            execution.attempt_id().get(),
            identity.stage_id().get(),
            identity.task_id().get(),
            scope.backend_index,
            identity.backend_process_id(),
            outbound_edges,
        );
        let path = novarocks_failpoint::create_before_worker_rendezvous_socket_path(&scope.token)
            .map_err(tonic::Status::failed_precondition)?;
        let deadline = (std::time::Instant::now() + std::time::Duration::from_secs(30))
            .min(scope.cancellation().deadline());
        let mut stream = match novarocks_failpoint::liveness::connect_until(&path, deadline, || {
            !scope.cancellation().is_active() || scope.action_ended().unwrap_or(true)
        }) {
            Ok(stream) => stream,
            Err(error) => {
                if scope
                    .action_ended()
                    .map_err(tonic::Status::failed_precondition)?
                    || !scope.cancellation().is_active()
                {
                    return Ok(());
                }
                return Err(tonic::Status::failed_precondition(format!(
                    "connect pre-Worker Create rendezvous: {error}"
                )));
            }
        };
        if scope
            .action_ended()
            .map_err(tonic::Status::failed_precondition)?
            || !scope.cancellation().is_active()
            || std::time::Instant::now() >= deadline
        {
            return Ok(());
        }
        stream
            .set_write_timeout(Some(
                deadline
                    .saturating_duration_since(std::time::Instant::now())
                    .max(std::time::Duration::from_nanos(1)),
            ))
            .map_err(|error| {
                tonic::Status::internal(format!("set pre-Worker Create write deadline: {error}"))
            })?;
        let delivered = stream
            .write_all(payload.as_bytes())
            .and_then(|()| stream.shutdown(Shutdown::Write));
        if scope
            .action_ended()
            .map_err(tonic::Status::failed_precondition)?
        {
            return Ok(());
        }
        delivered.map_err(|error| {
            tonic::Status::internal(format!("deliver pre-Worker Create identity: {error}"))
        })?;
        scope
            .record_hit()
            .map_err(tonic::Status::failed_precondition)?;
        eprintln!(
            "NOVAROCKS_TASK_CREATE_BEFORE_WORKER_HELD {}",
            payload.trim_end()
        );
        read_release_with_end(&mut stream, &scope.cancellation(), deadline, false, || {
            scope.action_ended()
        })
        .map_err(tonic::Status::failed_precondition)
    }
}

pub fn create_task_ack_dropped(
    controls: &NativeTestControls,
    identity: TaskIdentity,
    outcome: OperationOutcome,
) -> Result<(), tonic::Status> {
    if outcome != OperationOutcome::Accepted {
        return Ok(());
    }
    let execution = identity.query_execution_id();
    let Some(scope) = claim(
        controls,
        QueryLifecycleFaultKind::CreateTaskAckDrop,
        execution,
        identity.backend_process_id(),
    )?
    else {
        return Ok(());
    };
    scope
        .record_hit()
        .map_err(tonic::Status::failed_precondition)?;
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

/// What a create answered with a runner-owned rejection reports as its reason.
///
/// It names the fault rather than a protocol condition, for the same reason
/// [`TASK_EXECUTION_FAILURE_DETAIL`] does: this text reaches the client through
/// the attempt's failure, and a message that read like a real refusal would
/// make an injected one indistinguishable from a genuinely refused first
/// creation in a cluster log.
const CREATE_REJECTED_AFTER_APPLY_DETAIL: &str =
    "runner-owned CreateTask rejection answered after the task was admitted";

/// Answers one admitted `CreateTask` with the protocol's first-creation
/// refusal, `InvalidStateOrRequest`.
///
/// The retired protocol's `stage-conflict-after-apply` claimed its fault in
/// `handle_stage_fragments` and rewrote a staged participant's wire outcome to
/// `StageFragmentsRejectedConflict`. `CreateTask` is the task protocol's single
/// per-task admission point, so the claim moves here, and the perturbation is
/// the same one: the answer, not the operation. A create replay is decided by
/// task identity and has no content-conflict verdict to forge, so the verdict
/// forged here is the one a genuine first creation still receives when the
/// backend refuses its body.
///
/// # Why the operation must really have applied
///
/// This is what makes the case it serves impossible to satisfy by accident. The
/// task is admitted and running on this backend, so a frontend that retried the
/// refusal, or ignored it, would find a working task and the query would
/// return rows. Only a frontend that treats a refused create as fatal can fail
/// the statement -- which is the fence being asserted.
///
/// The receipt is rewritten into the exact shape a genuine refusal has: a
/// rejection carries no acknowledgement body, so leaving the applied one
/// attached would be a wire value no owner can produce, and the frontend would
/// refuse it for its shape instead of for its verdict.
pub fn create_task_rejected_after_apply(
    controls: &NativeTestControls,
    identity: TaskIdentity,
    outcome: OperationOutcome,
    encoded: &mut proto::TaskOperationReceipt,
) -> Result<(), tonic::Status> {
    if outcome != OperationOutcome::Accepted {
        return Ok(());
    }
    let execution = identity.query_execution_id();
    let Some(scope) = claim(
        controls,
        QueryLifecycleFaultKind::CreateTaskRejectedAfterApply,
        execution,
        identity.backend_process_id(),
    )?
    else {
        return Ok(());
    };
    scope
        .record_hit()
        .map_err(tonic::Status::failed_precondition)?;
    eprintln!(
        "NOVAROCKS_TASK_CREATE_REJECTED_AFTER_APPLY execution_id={}:{}:{} stage={} task={} backend_index={} token={}",
        execution.query_id().high(),
        execution.query_id().low(),
        execution.attempt_id().get(),
        identity.stage_id().get(),
        identity.task_id().get(),
        scope.backend_index,
        scope.token,
    );
    encoded.result = Some(proto::task_operation_receipt::Result::Verdict(
        proto::TaskOperationVerdict {
            outcome: proto::TaskOperationOutcome::InvalidStateOrRequest as i32,
            safe_detail: CREATE_REJECTED_AFTER_APPLY_DETAIL.to_owned(),
            safe_field_path: None,
            ack: None,
        },
    ));
    Ok(())
}

/// Makes one admitted `CreateTask` acknowledgement name a different task.
///
/// The successor of the retired `start-digest-corrupt`, which flipped a bit of
/// the `stage_digest` a `StartPreparedQuery` carried so it disagreed with the
/// plan the backend had staged. That fault has no direct expression here: the
/// task protocol has no second operation that commits an already staged plan,
/// and a create request's static plan is interpreted only by the backend that
/// wins that identity's creation, so no request field can be corrupted into
/// disagreeing with a plan the receiver already holds.
///
/// What survives is the identity half of the same fence. `TaskIdentity` is
/// indivisible, and an answer that names another task is refused rather than
/// adopted -- by `decode_create_task_ack` against the request's own identity,
/// and again by `RemoteTask::on_create_ack`. So the forged value is the
/// acknowledgement's task, and only that field: forging the carried status
/// identity as well would move the refusal onto the status cross-check and the
/// case would no longer be about the identity the frontend asked for.
///
/// Claimed only once the applied acknowledgement is present, so a malformed or
/// refused create leaves the arming for the create that really admitted a task.
pub fn create_task_receipt_foreign_task(
    controls: &NativeTestControls,
    identity: TaskIdentity,
    outcome: OperationOutcome,
    encoded: &mut proto::TaskOperationReceipt,
) -> Result<(), tonic::Status> {
    if outcome != OperationOutcome::Accepted {
        return Ok(());
    }
    let Some(proto::task_operation_receipt::Result::Verdict(verdict)) = encoded.result.as_mut()
    else {
        return Ok(());
    };
    let Some(proto::task_operation_verdict::Ack::CreateTask(ack)) = verdict.ack.as_mut() else {
        return Ok(());
    };
    let Some(wire_identity) = ack.identity.as_mut() else {
        return Ok(());
    };
    let execution = identity.query_execution_id();
    let Some(scope) = claim(
        controls,
        QueryLifecycleFaultKind::CreateTaskReceiptForeignTask,
        execution,
        identity.backend_process_id(),
    )?
    else {
        return Ok(());
    };
    // A task id is nonzero on the wire, so the forgery has to stay a legal
    // identity: a zero would be refused as a malformed field and the case would
    // be asserting the decoder's range check instead of its identity fence.
    let foreign_task_id = wire_identity.task_id.checked_add(1).unwrap_or(1);
    scope
        .record_hit()
        .map_err(tonic::Status::failed_precondition)?;
    eprintln!(
        "NOVAROCKS_TASK_CREATE_RECEIPT_FOREIGN_TASK execution_id={}:{}:{} stage={} task={} foreign_task={foreign_task_id} backend_index={} token={}",
        execution.query_id().high(),
        execution.query_id().low(),
        execution.attempt_id().get(),
        identity.stage_id().get(),
        identity.task_id().get(),
        scope.backend_index,
        scope.token,
    );
    wire_identity.task_id = foreign_task_id;
    Ok(())
}

/// Makes one delivered task status event name a foreign backend process.
///
/// The successor of the retired `observation-foreign-participant`, which
/// replaced the `ParticipantAttemptRef` of a fragment observation published on
/// the control stream. The surviving observation channel is
/// `SubscribeTaskStatus`; it names no participant of its own, so the forgeable
/// fact is the backend process inside the event's own `TaskIdentity`.
///
/// Claimed at the encode boundary rather than in
/// [`novarocks_worker::TaskStatusSource`], which is a process-local snapshot
/// holder with no wire and no identity of its own to misstate: a status
/// published there is the truth this backend holds, and the lie belongs where
/// the frame leaves the process. Both the catch-up frames and the live ones
/// pass through here, so no delivery path escapes the forgery.
///
/// # Why a malformed arming is warned about rather than returned
///
/// The caller is a stream body, so the only error it could report would tear
/// the subscription down -- which is a different perturbation entirely, and one
/// `task_status_subscription_dropped` already owns. A fault that cannot be
/// claimed must not silently become that other fault.
pub fn task_status_foreign_process(
    controls: &NativeTestControls,
    identity: TaskIdentity,
    encoded: &mut proto::TaskStatusStreamEvent,
) {
    // The frame's own identity is located before the arming is claimed, so a
    // frame this fault could not have forged leaves the token for the next one
    // instead of consuming it invisibly.
    if wire_status_identity(encoded).is_none() {
        return;
    }
    let execution = identity.query_execution_id();
    let scope = match claim_by_detail(
        controls,
        QueryLifecycleFaultKind::TaskStatusForeignProcess,
        execution,
        identity.backend_process_id(),
    ) {
        Ok(Some(scope)) => scope,
        Ok(None) => return,
        Err(detail) => {
            tracing::warn!(
                task = %identity,
                detail,
                "runner-owned foreign task status process fault could not be claimed"
            );
            return;
        }
    };
    let foreign = BackendProcessId::new_v7();
    if let Err(error) = scope.record_hit() {
        tracing::warn!(%error,"record Native fault hit failed");
        return;
    }
    eprintln!(
        "NOVAROCKS_TASK_STATUS_FOREIGN_PROCESS execution_id={}:{}:{} stage={} task={} backend={} foreign_backend={foreign} backend_index={} token={}",
        execution.query_id().high(),
        execution.query_id().low(),
        execution.attempt_id().get(),
        identity.stage_id().get(),
        identity.task_id().get(),
        identity.backend_process_id(),
        scope.backend_index,
        scope.token,
    );
    if let Some(wire_identity) = wire_status_identity(encoded) {
        wire_identity.backend_process_id = Some(proto::BackendProcessId {
            value: foreign.to_bytes().to_vec(),
        });
    }
}

/// The identity of whichever status event body this frame carries.
fn wire_status_identity(
    encoded: &mut proto::TaskStatusStreamEvent,
) -> Option<&mut proto::TaskIdentity> {
    match encoded.event.as_mut()? {
        proto::task_status_stream_event::Event::TaskStatus(status) => status.identity.as_mut(),
        proto::task_status_stream_event::Event::TaskGone(gone) => gone.identity.as_mut(),
        proto::task_status_stream_event::Event::TaskConvergence(receipt) => {
            receipt.identity.as_mut()
        }
        proto::task_status_stream_event::Event::TaskStatusUnchanged(answer) => {
            answer.identity.as_mut()
        }
        proto::task_status_stream_event::Event::TaskConvergenceUnchanged(answer) => {
            answer.identity.as_mut()
        }
        proto::task_status_stream_event::Event::TaskUnknown(answer) => answer.identity.as_mut(),
        proto::task_status_stream_event::Event::ContextConvergence(_)
        | proto::task_status_stream_event::Event::Quiesce(_)
        | proto::task_status_stream_event::Event::CatchUpComplete(_)
        | proto::task_status_stream_event::Event::Bookmark(_) => None,
    }
}

/// Corrupts the contract digest of one terminal logical feedback publication.
///
/// The retired protocol claimed this fault inside its control-stream
/// `BackendFrontendFeedbackSink`. The cutover moved the feedback carrier onto
/// the task substrate, so the claim has to move with it -- otherwise the fault
/// is armed, nothing consumes it, the query succeeds untouched, and the case
/// asserting the fail-closed rejection waits out its whole budget for a
/// perturbation that no longer has a producer.
///
/// The digest is the frontend's fence: `admit_terminal` refuses a publication
/// whose digest is not the contract it declared. Corrupting it therefore has
/// to fail the query closed rather than prune on a domain nobody declared.
pub fn corrupt_feedback_contract_digest(
    controls: &NativeTestControls,
    carrier: TaskIdentity,
) -> bool {
    claim_feedback_perturbation(
        controls,
        QueryLifecycleFaultKind::RuntimeFilterFeedbackContractDigestCorrupt,
        "NOVAROCKS_TASK_RUNTIME_FILTER_FEEDBACK_CONTRACT_DIGEST_CORRUPT",
        carrier,
    )
}

/// Forces one channel's terminal outcome to report no usable domain.
///
/// Moved from the retired sink for the same reason as the digest fault. This
/// one is the fail-*open* half of the pair: an unavailable channel is a
/// statement the frontend admits, and the split source then enumerates
/// unpruned. Correctness may not move; only the pruning optimization may.
pub fn force_feedback_unavailable(controls: &NativeTestControls, carrier: TaskIdentity) -> bool {
    claim_feedback_perturbation(
        controls,
        QueryLifecycleFaultKind::RuntimeFilterFeedbackUnavailable,
        "NOVAROCKS_TASK_RUNTIME_FILTER_FEEDBACK_UNAVAILABLE",
        carrier,
    )
}

/// Makes one publication claim it belongs to another attempt of this query.
///
/// This is the task carrier's whole forgeable share of the retired
/// `runtime-filter-feedback-foreign-participant` fault, and the rename is not
/// cosmetic. That fault replaced the publisher's `ParticipantAttemptRef` on
/// the wire, because the control stream *named* its publisher and the frontend
/// then checked the name against the authenticated process. The task carrier
/// names no publisher at all: `RuntimeFilterEnvelope` has no participant
/// field the frontend reads, and `DynamicFilterFeedbackPump` derives the
/// publisher from the `TaskIdentity` it fetched from. A backend cannot claim
/// another process's publisher slot because the claim never travels, so that
/// injection has no expression here and inventing one would assert a threat
/// the carrier structurally does not have.
///
/// What a backend *can* still misstate is which attempt the publication
/// belongs to, and the deployment epoch is exactly that fact. The fence it
/// meets is the first check in `admit_terminal`, ahead of the pruning winner,
/// so the query must fail closed with the winner untouched.
pub fn forge_feedback_foreign_attempt(
    controls: &NativeTestControls,
    carrier: TaskIdentity,
) -> bool {
    claim_feedback_perturbation(
        controls,
        QueryLifecycleFaultKind::RuntimeFilterFeedbackForeignAttempt,
        "NOVAROCKS_TASK_RUNTIME_FILTER_FEEDBACK_FOREIGN_ATTEMPT",
        carrier,
    )
}

/// The claim the three feedback perturbations share.
///
/// It is process-scoped like every other task-protocol fault: the carrier that
/// publishes is the backend the frontend reads this domain from, so there is
/// no third party whose arming this could consume. A malformed arming is
/// reported rather than silently read as "not armed" -- a publication may not
/// fail because a test file was wrong, but a fault that never fires must not
/// look like one that did not reproduce.
fn claim_feedback_perturbation(
    controls: &NativeTestControls,
    kind: QueryLifecycleFaultKind,
    marker: &str,
    carrier: TaskIdentity,
) -> bool {
    let execution = carrier.query_execution_id();
    let scope = match claim_by_detail(controls, kind, execution, carrier.backend_process_id()) {
        Ok(Some(scope)) => scope,
        Ok(None) => return false,
        Err(detail) => {
            tracing::warn!(
                kind = kind.file_stem(),
                task = %carrier,
                detail,
                "runner-owned runtime filter feedback fault could not be claimed"
            );
            return false;
        }
    };
    if let Err(error) = scope.record_hit() {
        tracing::warn!(%error,"record Native fault hit failed");
        return false;
    }
    eprintln!(
        "{marker} execution_id={}:{}:{} stage={} task={} backend_index={} token={}",
        execution.query_id().high(),
        execution.query_id().low(),
        execution.attempt_id().get(),
        carrier.stage_id().get(),
        carrier.task_id().get(),
        scope.backend_index,
        scope.token,
    );
    true
}

/// Drops one established `SubscribeTaskStatus` stream.
///
/// Reported as `Ok(true)`, not as an error: the subscription really was
/// established, so the drop belongs to the stream body rather than to the call
/// that opened it. Nothing was consumed — the per-task cursors are read-only —
/// so the frontend's resubscription sees exactly what this stream would have
/// carried.
pub fn task_status_subscription_dropped(
    controls: &NativeTestControls,
    context: QueryContextRef,
) -> Result<bool, tonic::Status> {
    let execution = context.query_execution_id();
    let Some(scope) = claim(
        controls,
        QueryLifecycleFaultKind::TaskStatusSubscriptionDrop,
        execution,
        context.backend_process_id(),
    )?
    else {
        return Ok(false);
    };
    scope
        .record_hit()
        .map_err(tonic::Status::failed_precondition)?;
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
    controls: &NativeTestControls,
    kind: QueryLifecycleFaultKind,
    marker: &str,
    detail: &'static str,
    context: QueryContextRef,
) -> Result<(), tonic::Status> {
    let execution = context.query_execution_id();
    let Some(scope) = claim(controls, kind, execution, context.backend_process_id())? else {
        return Ok(());
    };
    scope
        .record_hit()
        .map_err(tonic::Status::failed_precondition)?;
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

fn match_persistent(
    controls: &NativeTestControls,
    kind: QueryLifecycleFaultKind,
    execution_id: QueryExecutionId,
    process_id: BackendProcessId,
) -> Result<Option<NativeFaultClaim>, tonic::Status> {
    controls
        .claim(kind, execution_id, process_id, true)
        .map_err(tonic::Status::failed_precondition)
}
fn claim(
    controls: &NativeTestControls,
    kind: QueryLifecycleFaultKind,
    execution_id: QueryExecutionId,
    process_id: BackendProcessId,
) -> Result<Option<NativeFaultClaim>, tonic::Status> {
    controls
        .claim(kind, execution_id, process_id, false)
        .map_err(tonic::Status::failed_precondition)
}
fn claim_by_detail(
    controls: &NativeTestControls,
    kind: QueryLifecycleFaultKind,
    execution_id: QueryExecutionId,
    process_id: BackendProcessId,
) -> Result<Option<NativeFaultClaim>, String> {
    controls.claim(kind, execution_id, process_id, false)
}

/// Parks one exact accepted preparation at its Native host boundary. The
/// Worker already owns the queued/running position and backing charge; no
/// registry lock or RPC ingress permit is held by this rendezvous.
pub fn hold_native_task_preparation(
    controls: &NativeTestControls,
    identity: TaskIdentity,
) -> Result<(), String> {
    let Some(mut claim) = controls.claim_preparation_hold(identity)? else {
        return Ok(());
    };
    let path = novarocks_failpoint::native_registry_hold_socket_path(&claim.token)?;
    let execution = identity.query_execution_id();
    let marker = format!(
        "NTP1 {} {} {} {} {} {} {}\n",
        identity.backend_process_id(),
        execution.query_id().high(),
        execution.query_id().low(),
        execution.attempt_id().get(),
        identity.stage_id().get(),
        identity.task_id().get(),
        claim.token
    );
    let deadline = claim.cancellation().deadline();
    hold_socket(&path, marker.as_bytes(), &mut claim, deadline)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_controls::tests::{bind_fixture, context, controlled, end_fixture, task};
    use std::os::unix::net::UnixListener;
    use std::sync::Arc;
    use std::time::{Duration, Instant};

    #[test]
    fn root_start_action_is_left_unclaimed_by_ordinary_tasks_and_claimed_once_by_root() {
        let kind = QueryLifecycleFaultKind::RootTaskExecutionFailureBeforeData;
        let fixture = controlled(kind.as_str());
        bind_fixture(&fixture, kind);
        let identity = task(&fixture, 1);
        assert!(
            task_execution_failure_injected(&fixture.controls, identity, false)
                .unwrap()
                .is_none()
        );
        assert!(
            task_execution_failure_injected(&fixture.controls, identity, true)
                .unwrap()
                .is_some()
        );
        assert!(
            task_execution_failure_injected(&fixture.controls, identity, true)
                .unwrap()
                .is_none()
        );
        // Claiming is not Task acceptance, nor evidence that the failure ran.
        assert!(
            fixture
                .role
                .scope()
                .observations()
                .iter()
                .all(|event| event.event != "hit")
        );
    }

    fn accept_marker(listener: &UnixListener) -> UnixStream {
        listener.set_nonblocking(true).unwrap();
        let deadline = Instant::now() + Duration::from_secs(3);
        loop {
            match listener.accept() {
                Ok((stream, _)) => {
                    stream
                        .set_read_timeout(Some(Duration::from_secs(3)))
                        .unwrap();
                    return stream;
                }
                Err(error)
                    if error.kind() == std::io::ErrorKind::WouldBlock
                        && Instant::now() < deadline =>
                {
                    std::thread::sleep(Duration::from_millis(2))
                }
                Err(error) => panic!("Native hold did not enter: {error}"),
            }
        }
    }
    #[test]
    fn late_create_bypasses_only_exact_ended_injection_with_role_still_active() {
        let kind = QueryLifecycleFaultKind::CreateTaskBeforeWorkerHold;
        let fixture = controlled(kind.as_str());
        let binding = bind_fixture(&fixture, kind);
        // No listener: an ordinary, unended controlled hold must fail.
        assert!(create_task_before_worker_hold(&fixture.controls, task(&fixture, 1), 1).is_err());
        end_fixture(&fixture, kind, &binding);
        assert!(create_task_before_worker_hold(&fixture.controls, task(&fixture, 2), 1).is_ok());
        assert!(fixture.role.scope().is_active());
        assert!(
            fixture
                .role
                .scope()
                .observations()
                .iter()
                .all(|event| event.event != "hit")
        );
    }
    #[test]
    fn exact_end_unparks_read_but_eof_alone_preserves_failure() {
        let kind = QueryLifecycleFaultKind::CreateTaskBeforeWorkerHold;
        let fixture = controlled(kind.as_str());
        let binding = bind_fixture(&fixture, kind);
        let claim = fixture
            .controls
            .claim(kind, fixture.execution, fixture.process, true)
            .unwrap()
            .unwrap();
        let (mut client, peer) = UnixStream::pair().unwrap();
        drop(peer);
        assert!(
            read_release_with_end(
                &mut client,
                &claim.cancellation(),
                claim.cancellation().deadline(),
                false,
                || claim.action_ended()
            )
            .is_err()
        );
        let (mut client, _peer) = UnixStream::pair().unwrap();
        let worker = std::thread::spawn(move || {
            read_release_with_end(
                &mut client,
                &claim.cancellation(),
                claim.cancellation().deadline(),
                false,
                || claim.action_ended(),
            )
        });
        end_fixture(&fixture, kind, &binding);
        worker.join().unwrap().unwrap();
        assert!(fixture.role.scope().is_active());
    }
    #[test]
    fn native_ordinary_hold_unblocks_when_runner_disconnects() {
        let mut fixture = controlled("ingress-hold");
        // Keep the declared finite target token: socket namespace is private
        // to the attempted action rather than a process-global registry.
        let claim = fixture
            .controls
            .claim_context_hold("ingress-hold", context(&fixture), Some(&fixture.token))
            .unwrap()
            .unwrap();
        let path = novarocks_failpoint::native_ingress_hold_socket_path(&claim.token).unwrap();
        let listener = UnixListener::bind(&path).unwrap();
        let worker = std::thread::spawn(move || {
            hold_native_ingress_ordinary_closure(claim, Instant::now() + Duration::from_secs(5))
        });
        let mut stream = accept_marker(&listener);
        let mut marker = String::new();
        stream.read_to_string(&mut marker).unwrap();
        assert!(marker.contains(&fixture.process.to_string()));
        fixture.runner.end_and_join().unwrap();
        assert!(
            worker.join().unwrap().is_err(),
            "domain hold really returned after liveness EOF"
        );
        assert_eq!(
            fixture
                .role
                .scope()
                .observations()
                .iter()
                .filter(|event| event.event == "hit")
                .count(),
            1
        );
        drop(stream);
        drop(listener);
        std::fs::remove_file(path).unwrap();
    }
    #[test]
    fn preparation_hold_unblocks_on_session_end_and_the_actual_worker_is_joined() {
        let mut fixture = controlled("preparation-hold");
        let path = novarocks_failpoint::native_registry_hold_socket_path(&fixture.token).unwrap();
        let listener = UnixListener::bind(&path).unwrap();
        let identity = task(&fixture, 2);
        let controls = Arc::clone(&fixture.controls);
        let worker = std::thread::spawn(move || hold_native_task_preparation(&controls, identity));
        let mut stream = accept_marker(&listener);
        let mut marker = String::new();
        stream.read_to_string(&mut marker).unwrap();
        assert!(marker.starts_with("NTP1 "));
        fixture.runner.end_and_join().unwrap();
        assert!(worker.join().unwrap().is_err());
        drop(stream);
        drop(listener);
        std::fs::remove_file(path).unwrap();
    }
    #[test]
    fn restart_after_establish_hold_is_exact_and_released_with_its_establish() {
        let fixture = controlled("restart-after-establish-context");
        let kind = QueryLifecycleFaultKind::RestartAfterEstablishContext;
        novarocks_failpoint::target_binding::StatementExecutionBinding::new(
            "native-tests".into(),
            fixture.token.clone(),
            0,
            fixture.process,
            fixture.execution,
        )
        .unwrap()
        .write_private(&crate::test_controls::fault_binding_path(
            fixture.directory.path(),
            kind,
            0,
            &fixture.token,
        ))
        .unwrap();
        let exact = context(&fixture);
        let other = QueryContextRef::new(
            fixture.execution,
            fixture.frontend,
            BackendProcessId::new_v7(),
        );
        arm_restart_after_establish_context(&fixture.controls, exact).unwrap();
        assert!(restart_after_establish_context_holds_task_creation(
            &fixture.controls,
            exact
        ));
        assert!(!restart_after_establish_context_holds_task_creation(
            &fixture.controls,
            other
        ));
        fixture.controls.disarm_restart(exact);
        assert!(!restart_after_establish_context_holds_task_creation(
            &fixture.controls,
            exact
        ));
    }
}
