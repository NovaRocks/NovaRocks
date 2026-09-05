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

use std::sync::Arc;

use tokio_stream::{Stream, StreamExt, wrappers::ReceiverStream};

use novarocks_proto_codec::lifecycle::{
    QueryAbortRequest, QueryControlAttach, QueryControlCommand as ProtocolQueryControlCommand,
    QueryControlEvent, QueryInitOutcome, QueryInitRequest, QueryStageRequest, QueryStartRequest,
    QueryTerminalAck, QueryTerminationReason,
};
use novarocks_proto_codec::{ProtocolError, ProtocolErrorKind};
use novarocks_proto_models::novarocks as proto;
use novarocks_types::QueryExecutionId;

use crate::query_lifecycle::{
    BackendQueryControl, QueryLifecycleError, QueryLifecycleErrorCode, QueryLifecycleIngress,
};

const CONTROL_STREAM_CAPACITY: usize = 16;

// Design: ADR-0123 (docs/adr/ADR-0123-task-update-watermark-retry-delivery.md)
pub(crate) type QueryControlResponseStream =
    ReceiverStream<Result<proto::QueryControlResponse, tonic::Status>>;

#[expect(
    clippy::result_large_err,
    reason = "The tonic service boundary must preserve Status without changing its generated signature."
)]
pub(crate) fn handle_init_query(
    ingress: &dyn QueryLifecycleIngress,
    request: proto::InitQueryRequest,
    tls_verified: bool,
) -> Result<proto::InitQueryResponse, tonic::Status> {
    let request = if tls_verified {
        QueryInitRequest::parse_tls(request)
    } else {
        QueryInitRequest::parse(request)
    }
    .map_err(status_from_contract_error)?;
    let execution_id = request
        .manifest()
        .map_err(status_from_contract_error)?
        .execution_id()
        .map_err(status_from_contract_error)?;
    let ack = if tls_verified {
        ingress.init_query_tls(request)
    } else {
        ingress.init_query(request)
    };
    if matches!(ack.outcome(), Ok(QueryInitOutcome::QueryInitApplied)) {
        if let Some(scope) = claim_backend_fault(
            QueryLifecycleFaultKind::RestartAfterInitAck,
            execution_id,
            ingress.backend_process_id(),
        )? {
            eprintln!(
                "NOVAROCKS_QUERY_INIT_ACK_OBSERVED execution_id={}:{}:{} backend_index={} process_id={} token={}",
                execution_id.query_id().high(),
                execution_id.query_id().low(),
                execution_id.attempt_id().get(),
                scope.backend_index,
                scope.process_id,
                scope.token
            );
            wait_for_runner_owned_restart(&scope);
        }
        if let Some(scope) = claim_backend_fault(
            QueryLifecycleFaultKind::InitAckDrop,
            execution_id,
            ingress.backend_process_id(),
        )? {
            eprintln!(
                "NOVAROCKS_QUERY_INIT_ACK_DROPPED execution_id={}:{}:{} backend_index={} process_id={} token={}",
                execution_id.query_id().high(),
                execution_id.query_id().low(),
                execution_id.attempt_id().get(),
                scope.backend_index,
                scope.process_id,
                scope.token
            );
            return Err(tonic::Status::deadline_exceeded(
                "runner-owned InitAck response dropped after Applied",
            ));
        }
    }
    Ok(ack.as_proto().clone())
}

#[expect(
    clippy::result_large_err,
    reason = "The tonic service boundary must preserve Status without changing its generated signature."
)]
pub(crate) fn handle_abort_query(
    ingress: &dyn QueryLifecycleIngress,
    request: proto::AbortQueryRequest,
) -> Result<proto::AbortQueryResponse, tonic::Status> {
    let request = QueryAbortRequest::parse(request).map_err(status_from_contract_error)?;
    let response = ingress
        .abort_query(request)
        .map_err(status_from_lifecycle_error)?;
    emit_query_lifecycle_abort_marker();
    Ok(*response.as_proto())
}

fn emit_query_lifecycle_abort_marker() {
    if crate::config::debug_emit_cancel_marker() {
        println!("NOVAROCKS_QUERY_LIFECYCLE_ABORT");
        let _ = std::io::Write::flush(&mut std::io::stdout());
    }
}

#[expect(
    clippy::result_large_err,
    reason = "The tonic service boundary must preserve Status without changing its generated signature."
)]
pub(crate) fn handle_stage_fragments(
    ingress: &dyn QueryLifecycleIngress,
    request: proto::StageFragmentsRequest,
) -> Result<proto::StageFragmentsResponse, tonic::Status> {
    let request = QueryStageRequest::parse(request).map_err(status_from_contract_error)?;
    let execution_id = request.execution_id();
    let response = ingress.stage_fragments(request);
    if response.outcome().is_staged()
        && let Some(scope) = claim_backend_fault(
            QueryLifecycleFaultKind::StageConflictAfterApply,
            execution_id,
            ingress.backend_process_id(),
        )?
    {
        eprintln!(
            "NOVAROCKS_STAGE_CONFLICT_AFTER_APPLY execution_id={}:{}:{} backend_index={} token={}",
            execution_id.query_id().high(),
            execution_id.query_id().low(),
            execution_id.attempt_id().get(),
            scope.backend_index,
            scope.token
        );
        let mut response = response.as_proto().clone();
        response.outcome = proto::StageFragmentsOutcome::StageFragmentsRejectedConflict as i32;
        response.detail = "runner-owned Stage conflict after apply".to_owned();
        return Ok(response);
    }
    Ok(response.as_proto().clone())
}

/// A malformed or refused task update is a typed rejection on the response,
/// not a transport error: the sender needs the reason to decide whether to
/// retransmit, stop, or fail the query.
#[expect(
    clippy::result_large_err,
    reason = "The tonic service boundary must preserve Status without changing its generated signature."
)]
pub(crate) fn handle_task_update(
    ingress: &dyn QueryLifecycleIngress,
    request: proto::TaskUpdateRequest,
) -> Result<proto::TaskUpdateResponse, tonic::Status> {
    match super::task_update::TaskUpdateRequest::parse(request) {
        Ok(request) => {
            // Claim only after the backend has durably accepted an update
            // which both carries a split and closes that plan node. A malformed,
            // refused, or nonterminal delivery must leave the runner token for
            // the real terminal acknowledgement.
            let terminal_nonempty = request
                .assignments()
                .iter()
                .any(|assignment| assignment.no_more_splits() && !assignment.splits().is_empty());
            let execution_id = request.execution_id();
            let response = ingress.task_update(request);
            if terminal_nonempty
                && matches!(&response, super::task_update::TaskUpdateAck::Accepted(_))
                && let Some(scope) = claim_backend_fault(
                    QueryLifecycleFaultKind::TaskUpdateTerminalAckDrop,
                    execution_id,
                    ingress.backend_process_id(),
                )?
            {
                eprintln!(
                    "NOVAROCKS_TASK_UPDATE_TERMINAL_ACK_DROPPED execution_id={}:{}:{} backend_index={} token={}",
                    execution_id.query_id().high(),
                    execution_id.query_id().low(),
                    execution_id.attempt_id().get(),
                    scope.backend_index,
                    scope.token
                );
                return Err(tonic::Status::deadline_exceeded(
                    "runner-owned TaskUpdate terminal acknowledgement dropped after acceptance",
                ));
            }
            Ok(response.to_proto())
        }
        Err(error) => Ok(super::task_update::rejection_from_contract_error(&error).to_proto()),
    }
}

#[expect(
    clippy::result_large_err,
    reason = "The tonic service boundary must preserve Status without changing its generated signature."
)]
pub(crate) fn handle_start_prepared_query(
    ingress: &dyn QueryLifecycleIngress,
    request: proto::StartPreparedQueryRequest,
) -> Result<proto::StartPreparedQueryResponse, tonic::Status> {
    let request = QueryStartRequest::parse(request).map_err(status_from_contract_error)?;
    let execution_id = request.execution_id();
    let request = if let Some(scope) = claim_backend_fault(
        QueryLifecycleFaultKind::StartDigestCorrupt,
        execution_id,
        ingress.backend_process_id(),
    )? {
        eprintln!(
            "NOVAROCKS_START_DIGEST_CORRUPTED execution_id={}:{}:{} backend_index={} token={}",
            execution_id.query_id().high(),
            execution_id.query_id().low(),
            execution_id.attempt_id().get(),
            scope.backend_index,
            scope.token
        );
        let mut raw = request.as_proto().clone();
        raw.stage_digest[0] ^= 1;
        QueryStartRequest::parse(raw).map_err(status_from_contract_error)?
    } else {
        request
    };
    let response = ingress.start_prepared_query(request);
    if response.outcome().is_running()
        && let Some(scope) = observe_backend_fault(
            QueryLifecycleFaultKind::StartAckSuppress,
            execution_id,
            ingress.backend_process_id(),
        )?
    {
        eprintln!(
            "NOVAROCKS_START_ACK_SUPPRESSED execution_id={}:{}:{} backend_index={} token={}",
            execution_id.query_id().high(),
            execution_id.query_id().low(),
            execution_id.attempt_id().get(),
            scope.backend_index,
            scope.token
        );
        return Err(tonic::Status::deadline_exceeded(
            "runner-owned StartAck response suppressed after release",
        ));
    }
    Ok(response.as_proto().clone())
}

pub(crate) async fn handle_query_control_stream(
    ingress: Arc<dyn QueryLifecycleIngress>,
    mut inbound: tonic::Streaming<proto::QueryControlRequest>,
    mut shutdown: Option<tokio::sync::watch::Receiver<bool>>,
    tls_verified: bool,
) -> Result<QueryControlResponseStream, tonic::Status> {
    let first = tokio::select! {
        biased;
        _ = wait_for_query_control_shutdown(&mut shutdown) => {
            return Err(tonic::Status::unavailable("query control server is shutting down"));
        }
        first = inbound.message() => first,
    }
    .map_err(|error| tonic::Status::invalid_argument(format!("read attach frame: {error}")))?
    .ok_or_else(|| tonic::Status::failed_precondition("first frame must be Attach"))?;
    if !matches!(
        first.command,
        Some(proto::query_control_request::Command::Attach(_))
    ) {
        return Err(tonic::Status::failed_precondition(
            "first frame must be Attach",
        ));
    }
    let attach = first
        .command
        .and_then(|command| match command {
            proto::query_control_request::Command::Attach(attach) => Some(attach),
            _ => None,
        })
        .expect("checked Attach frame must contain attach payload");
    let attach = QueryControlAttach::parse(attach).map_err(status_from_contract_error)?;
    let execution_id = attach.execution_id().map_err(status_from_contract_error)?;
    for kind in [
        QueryLifecycleFaultKind::TerminalP0RetainedSlotExhausted,
        QueryLifecycleFaultKind::TerminalP0BytesExhausted,
        QueryLifecycleFaultKind::TerminalP0DeliveryPermitExhausted,
    ] {
        if let Some(scope) = claim_backend_fault(kind, execution_id, ingress.backend_process_id())?
        {
            eprintln!(
                "NOVAROCKS_QUERY_TERMINAL_ATTACH_REJECTED kind={} execution_id={}:{}:{} backend_index={} process_id={} token={}",
                kind.file_stem(),
                scope.execution_id.query_id().high(),
                scope.execution_id.query_id().low(),
                scope.execution_id.attempt_id().get(),
                scope.backend_index,
                scope.process_id,
                scope.token,
            );
            return Err(tonic::Status::resource_exhausted(format!(
                "injected query lifecycle fault {} before ControlReady",
                kind.file_stem()
            )));
        }
    }
    let terminal_proof_stream_drop = claim_backend_fault(
        QueryLifecycleFaultKind::TerminalProofStreamDrop,
        execution_id,
        ingress.backend_process_id(),
    )?;
    let terminal_attestation_stream_drop = claim_backend_fault(
        QueryLifecycleFaultKind::TerminalAttestationStreamDrop,
        execution_id,
        ingress.backend_process_id(),
    )?;
    let attachment = ingress
        .attach_control(attach)
        .map_err(status_from_lifecycle_error)?;
    let (outbound_tx, outbound_rx) = tokio::sync::mpsc::channel(CONTROL_STREAM_CAPACITY);
    let lease = CoordinatorLease::new(attachment.control);
    tokio::spawn(run_attached_control_stream(
        inbound,
        lease,
        attachment.events,
        attachment.runtime_filter_feedback,
        outbound_tx,
        shutdown,
        terminal_proof_stream_drop,
        terminal_attestation_stream_drop,
        tls_verified,
    ));
    Ok(ReceiverStream::new(outbound_rx))
}

#[expect(
    clippy::too_many_arguments,
    reason = "The control-stream task receives one explicitly named resource for each lifecycle responsibility."
)]
async fn run_attached_control_stream(
    mut inbound: impl Stream<Item = Result<proto::QueryControlRequest, tonic::Status>>
    + Send
    + Unpin
    + 'static,
    mut lease: CoordinatorLease,
    mut events: tokio::sync::mpsc::Receiver<QueryControlEvent>,
    mut runtime_filter_feedback: tokio::sync::mpsc::Receiver<QueryControlEvent>,
    outbound: tokio::sync::mpsc::Sender<Result<proto::QueryControlResponse, tonic::Status>>,
    mut shutdown: Option<tokio::sync::watch::Receiver<bool>>,
    terminal_proof_stream_drop: Option<QueryLifecycleFaultScope>,
    terminal_attestation_stream_drop: Option<QueryLifecycleFaultScope>,
    tls_verified: bool,
) {
    let first_event = tokio::select! {
        biased;
        _ = wait_for_query_control_shutdown(&mut shutdown) => return,
        event = events.recv() => event,
    };
    let Some(first_event) = first_event else {
        let _ = send_control_response(
            &outbound,
            Err(tonic::Status::internal(
                "query control event stream closed before ControlReady",
            )),
            &mut shutdown,
        )
        .await;
        return;
    };
    if !matches!(
        first_event.as_proto().event,
        Some(proto::query_control_response::Event::ControlReady(_))
    ) {
        let _ = send_control_response(
            &outbound,
            Err(tonic::Status::internal(
                "query control event stream did not begin with ControlReady",
            )),
            &mut shutdown,
        )
        .await;
        return;
    }
    if !send_control_response(&outbound, Ok(first_event.as_proto().clone()), &mut shutdown).await {
        return;
    }

    let mut awaiting_graceful_termination = false;
    let mut runtime_filter_feedback_open = true;
    loop {
        tokio::select! {
            biased;
            _ = wait_for_query_control_shutdown(&mut shutdown) => {
                break;
            }
            // Correctness events outrank liveness traffic. In particular,
            // LocalFailure may be queued while a heartbeat sent before the
            // failure is still readable on the inbound stream.
            event = events.recv() => {
                let Some(event) = event else {
                    break;
                };
                let terminal_stream_drop = match event.as_proto().event.as_ref() {
                    Some(proto::query_control_response::Event::TerminalOutcome(outcome)) => {
                        match outcome.outcome.as_ref() {
                            Some(proto::participant_terminal_outcome::Outcome::Proof(_)) => {
                                terminal_proof_stream_drop.as_ref()
                            }
                            Some(proto::participant_terminal_outcome::Outcome::NegativeAttestation(_)) => {
                                terminal_attestation_stream_drop.as_ref()
                            }
                            None => None,
                        }
                    }
                    _ => None,
                };
                if let Some(scope) = terminal_stream_drop {
                    eprintln!(
                        "NOVAROCKS_QUERY_TERMINAL_STREAM_DROPPED execution_id={}:{}:{} backend_index={} process_id={} token={}",
                        scope.execution_id.query_id().high(),
                        scope.execution_id.query_id().low(),
                        scope.execution_id.attempt_id().get(),
                        scope.backend_index,
                        scope.process_id,
                        scope.token,
                    );
                    break;
                }
                let termination_accepted = matches!(
                    event.as_proto().event,
                    Some(proto::query_control_response::Event::TerminationAccepted(_))
                );
                if !send_control_response(
                    &outbound,
                    Ok(event.as_proto().clone()),
                    &mut shutdown,
                )
                .await
                {
                    break;
                }
                if termination_accepted {
                    if awaiting_graceful_termination {
                        // Abort may publish its legacy acknowledgement before
                        // the asynchronous immutable TerminalSnapshot. Both
                        // terminal paths retain that record until the
                        // frontend acknowledges it, so this latch never
                        // closes the command side by itself.
                        continue;
                    }
                    lease.mark_graceful();
                    break;
                }
            }
            inbound_message = inbound.next() => {
                let request = match inbound_message {
                    Some(Ok(request)) => request,
                    None => break,
                    Some(Err(error)) => {
                        let _ = send_control_response(
                            &outbound,
                            Err(tonic::Status::invalid_argument(format!(
                                "read query control command: {error}"
                            ))),
                            &mut shutdown,
                        )
                        .await;
                        break;
                    }
                };
                if matches!(
                    request.command,
                    Some(proto::query_control_request::Command::Attach(_))
                ) {
                    let _ = send_control_response(
                        &outbound,
                        Err(tonic::Status::already_exists(
                            "Attach may appear exactly once",
                        )),
                        &mut shutdown,
                    )
                    .await;
                    break;
                }
                let command = match (if tls_verified {
                    ProtocolQueryControlCommand::parse_tls(request)
                } else {
                    ProtocolQueryControlCommand::parse(request)
                })
                    .map_err(status_from_contract_error)
                {
                    Ok(command) => command,
                    Err(error) => {
                        let _ = send_control_response(
                            &outbound,
                            Err(error),
                            &mut shutdown,
                        )
                        .await;
                        break;
                    }
                };
                let terminal_ack = matches!(
                    command.as_proto().command,
                    Some(proto::query_control_request::Command::TerminalAck(_))
                );
                let result = match command.as_proto().command.as_ref() {
                    Some(proto::query_control_request::Command::Heartbeat(heartbeat)) => {
                        lease.control().heartbeat(heartbeat.sequence).map(|_| ())
                    }
                    Some(proto::query_control_request::Command::Abort(abort)) => {
                        awaiting_graceful_termination = true;
                        let result = lease.control().abort(abort.reason.clone());
                        if result.is_ok() {
                            emit_query_lifecycle_abort_marker();
                        }
                        result
                    }
                    Some(proto::query_control_request::Command::Finalize(_)) => {
                        awaiting_graceful_termination = true;
                        lease.control().finalize()
                    }
                    Some(proto::query_control_request::Command::TerminalAck(ack)) => {
                        let ack = QueryTerminalAck::parse(ack.clone())
                            .expect("validated Protocol command contains a terminal acknowledgement");
                        let result = lease.control().terminal_ack(ack);
                        if result.is_ok() {
                            lease.mark_graceful();
                        }
                        result
                    }
                    Some(proto::query_control_request::Command::CredentialLeasePrepare(_)) => {
                        let envelope = command
                            .credential_lease_prepare()
                            .expect("validated Protocol command parses lease prepare")
                            .expect("lease prepare command carries an envelope");
                        lease.control().credential_lease_prepare(envelope)
                    }
                    Some(proto::query_control_request::Command::CredentialLeaseCommit(_)) => {
                        let (lease_id, epoch) = command
                            .credential_lease_commit()
                            .expect("validated Protocol command parses lease commit")
                            .expect("lease commit command carries an epoch");
                        lease.control().credential_lease_commit(lease_id, epoch)
                    }
                    Some(proto::query_control_request::Command::Attach(_)) | None => unreachable!(
                        "validated Protocol command excludes Attach and empty control frames"
                    ),
                };
                if let Err(error) = result {
                    let _ = send_control_response(
                        &outbound,
                        Err(status_from_lifecycle_error(error)),
                        &mut shutdown,
                    )
                    .await;
                    break;
                }
                if terminal_ack {
                    // TerminalSnapshot is store-before-ACK.  Keep the
                    // bidirectional stream open after Finalize until that
                    // ACK has crossed the command side; otherwise the
                    // compatibility TerminationAccepted event can race the
                    // frontend's ACK and lose the retained record.
                    break;
                }
            }
            // Feedback is deliberately last in this biased mux. It is an
            // optimization hint, never a lifecycle correctness dependency.
            event = runtime_filter_feedback.recv(), if runtime_filter_feedback_open => {
                let Some(event) = event else {
                    // A feedback sender disappears on normal cancellation or
                    // retirement; keep serving the required control stream.
                    runtime_filter_feedback_open = false;
                    continue;
                };
                if !send_control_response(
                    &outbound,
                    Ok(event.as_proto().clone()),
                    &mut shutdown,
                )
                .await
                {
                    break;
                }
            }
        }
    }
}

#[cfg(debug_assertions)]
use novarocks_failpoint::observe_matching_fault;
use novarocks_failpoint::{QueryLifecycleFaultKind, QueryLifecycleFaultScope};
#[cfg(debug_assertions)]
use novarocks_failpoint::{claim_matching_fault, trigger_path};

#[cfg(debug_assertions)]
#[expect(
    clippy::result_large_err,
    reason = "The debug-only fault seam returns tonic Status at its gRPC-facing boundary."
)]
fn claim_backend_fault(
    kind: QueryLifecycleFaultKind,
    execution_id: QueryExecutionId,
    process_id: novarocks_types::BackendProcessId,
) -> Result<Option<QueryLifecycleFaultScope>, tonic::Status> {
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
    claim_matching_fault(&root, kind, execution_id, backend_index, process_id)
        .map_err(tonic::Status::failed_precondition)
}

/// `RestartAfterInitAck` is a runner-owned rendezvous: the BE emits its
/// token-scoped marker, then waits for the runner to terminate that exact
/// process.  Without the wait, a small query can finish between the marker
/// write and the parent's kill, which does not prove loss after admission.
#[cfg(debug_assertions)]
fn wait_for_runner_owned_restart(scope: &QueryLifecycleFaultScope) {
    let Some(root) = novarocks_failpoint::configured_root() else {
        return;
    };
    let release = trigger_path(
        &root,
        scope.backend_index,
        QueryLifecycleFaultKind::RestartAfterInitAck,
    )
    .with_extension("release");
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(30);
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
        std::thread::sleep(std::time::Duration::from_millis(1));
    }
}

#[cfg(not(debug_assertions))]
fn wait_for_runner_owned_restart(_scope: &QueryLifecycleFaultScope) {}

#[cfg(debug_assertions)]
#[expect(
    clippy::result_large_err,
    reason = "The debug-only fault seam returns tonic Status at its gRPC-facing boundary."
)]
fn observe_backend_fault(
    kind: QueryLifecycleFaultKind,
    execution_id: QueryExecutionId,
    process_id: novarocks_types::BackendProcessId,
) -> Result<Option<QueryLifecycleFaultScope>, tonic::Status> {
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
    observe_matching_fault(&root, kind, execution_id, backend_index, process_id)
        .map_err(tonic::Status::failed_precondition)
}

#[cfg(not(debug_assertions))]
fn claim_backend_fault(
    _kind: QueryLifecycleFaultKind,
    _execution_id: QueryExecutionId,
    _process_id: novarocks_types::BackendProcessId,
) -> Result<Option<QueryLifecycleFaultScope>, tonic::Status> {
    Ok(None)
}

#[cfg(not(debug_assertions))]
fn observe_backend_fault(
    _kind: QueryLifecycleFaultKind,
    _execution_id: QueryExecutionId,
    _process_id: novarocks_types::BackendProcessId,
) -> Result<Option<QueryLifecycleFaultScope>, tonic::Status> {
    Ok(None)
}

async fn send_control_response(
    outbound: &tokio::sync::mpsc::Sender<Result<proto::QueryControlResponse, tonic::Status>>,
    response: Result<proto::QueryControlResponse, tonic::Status>,
    shutdown: &mut Option<tokio::sync::watch::Receiver<bool>>,
) -> bool {
    tokio::select! {
        biased;
        _ = wait_for_query_control_shutdown(shutdown) => false,
        result = outbound.send(response) => result.is_ok(),
    }
}

async fn wait_for_query_control_shutdown(
    shutdown: &mut Option<tokio::sync::watch::Receiver<bool>>,
) {
    let Some(shutdown) = shutdown.as_mut() else {
        std::future::pending::<()>().await;
        return;
    };
    loop {
        if *shutdown.borrow_and_update() {
            return;
        }
        if shutdown.changed().await.is_err() {
            std::future::pending::<()>().await;
        }
    }
}

struct CoordinatorLease {
    control: Arc<dyn BackendQueryControl>,
    graceful: bool,
}

impl CoordinatorLease {
    fn new(control: Arc<dyn BackendQueryControl>) -> Self {
        Self {
            control,
            graceful: false,
        }
    }

    fn control(&self) -> &dyn BackendQueryControl {
        self.control.as_ref()
    }

    fn mark_graceful(&mut self) {
        self.graceful = true;
    }
}

impl Drop for CoordinatorLease {
    fn drop(&mut self) {
        if !self.graceful {
            let _ = self
                .control
                .coordinator_lost(QueryTerminationReason::QueryTerminationCoordinatorStreamLost);
        }
    }
}

pub(crate) fn status_from_lifecycle_error(error: QueryLifecycleError) -> tonic::Status {
    let detail = error.detail().to_string();
    match error.code() {
        QueryLifecycleErrorCode::InvalidManifest => tonic::Status::invalid_argument(detail),
        QueryLifecycleErrorCode::Conflict => tonic::Status::already_exists(detail),
        QueryLifecycleErrorCode::StaleBackend | QueryLifecycleErrorCode::Terminated => {
            tonic::Status::failed_precondition(detail)
        }
        QueryLifecycleErrorCode::Capacity => tonic::Status::resource_exhausted(detail),
        QueryLifecycleErrorCode::Transport => tonic::Status::unavailable(detail),
        QueryLifecycleErrorCode::Internal => tonic::Status::internal(detail),
    }
}

pub(crate) fn status_from_contract_error(error: ProtocolError) -> tonic::Status {
    let detail = error.detail().to_string();
    match error.kind() {
        ProtocolErrorKind::MissingField
        | ProtocolErrorKind::InvalidEnum
        | ProtocolErrorKind::InvalidValue
        | ProtocolErrorKind::OutOfRange
        | ProtocolErrorKind::DuplicateField
        | ProtocolErrorKind::InconsistentFields
        | ProtocolErrorKind::Unsupported
        | ProtocolErrorKind::VersionMismatch => tonic::Status::invalid_argument(detail),
        ProtocolErrorKind::Conflict => tonic::Status::already_exists(detail),
        ProtocolErrorKind::Capacity => tonic::Status::resource_exhausted(detail),
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use novarocks_proto_codec::lifecycle::QueryControlEvent;
    use novarocks_proto_models::{catalog, novarocks as proto};

    use super::{CoordinatorLease, run_attached_control_stream};
    use crate::query_lifecycle::{
        BackendQueryControl, QueryHeartbeatDisposition, QueryLifecycleError,
    };

    struct TerminalPendingControl {
        heartbeat_seen: Arc<tokio::sync::Notify>,
    }

    impl BackendQueryControl for TerminalPendingControl {
        fn heartbeat(
            &self,
            _sequence: u64,
        ) -> Result<QueryHeartbeatDisposition, QueryLifecycleError> {
            self.heartbeat_seen.notify_one();
            Ok(QueryHeartbeatDisposition::TerminalDeliveryPending)
        }

        fn abort(&self, _reason: String) -> Result<(), QueryLifecycleError> {
            unreachable!("test sends only heartbeat")
        }

        fn finalize(&self) -> Result<(), QueryLifecycleError> {
            unreachable!("test sends only heartbeat")
        }

        fn coordinator_lost(
            &self,
            _reason: novarocks_proto_codec::lifecycle::QueryTerminationReason,
        ) -> Result<(), QueryLifecycleError> {
            Ok(())
        }
    }

    fn control_event(event: proto::query_control_response::Event) -> QueryControlEvent {
        QueryControlEvent::parse(proto::QueryControlResponse { event: Some(event) })
            .expect("test control event is valid")
    }

    #[tokio::test]
    async fn queued_local_failure_outranks_simultaneously_ready_heartbeat() {
        let (inbound_tx, inbound_rx) = tokio::sync::mpsc::channel(2);
        let (events_tx, events_rx) = tokio::sync::mpsc::channel(2);
        let (_feedback_tx, feedback_rx) = tokio::sync::mpsc::channel(1);
        let (outbound_tx, mut outbound_rx) = tokio::sync::mpsc::channel(3);
        let heartbeat_seen = Arc::new(tokio::sync::Notify::new());
        let control = Arc::new(TerminalPendingControl {
            heartbeat_seen: Arc::clone(&heartbeat_seen),
        });

        events_tx
            .send(control_event(
                proto::query_control_response::Event::ControlReady(proto::QueryControlReady {
                    catalog_load_state: Some(catalog::CatalogLoadState {
                        state: Some(catalog::catalog_load_state::State::Ready(
                            catalog::CatalogReady {},
                        )),
                    }),
                }),
            ))
            .await
            .expect("queue ControlReady");
        events_tx
            .send(control_event(
                proto::query_control_response::Event::LocalFailure(
                    proto::QueryControlLocalFailure {
                        code: "FRAGMENT_EXECUTION_FAILED".to_owned(),
                        detail: "deterministic terminal race".to_owned(),
                    },
                ),
            ))
            .await
            .expect("queue LocalFailure");
        inbound_tx
            .send(Ok(proto::QueryControlRequest {
                command: Some(proto::query_control_request::Command::Heartbeat(
                    proto::QueryControlHeartbeat {
                        sequence: 1,
                        sent_mono_ns: 1,
                    },
                )),
            }))
            .await
            .expect("queue heartbeat");

        let task = tokio::spawn(run_attached_control_stream(
            tokio_stream::wrappers::ReceiverStream::new(inbound_rx),
            CoordinatorLease::new(control),
            events_rx,
            feedback_rx,
            outbound_tx,
            None,
            None,
            None,
            false,
        ));

        let ready = outbound_rx
            .recv()
            .await
            .expect("ControlReady response")
            .expect("ControlReady is not a status error");
        assert!(matches!(
            ready.event,
            Some(proto::query_control_response::Event::ControlReady(_))
        ));
        let failure = outbound_rx
            .recv()
            .await
            .expect("LocalFailure response")
            .expect("LocalFailure is not a status error");
        assert!(matches!(
            failure.event,
            Some(proto::query_control_response::Event::LocalFailure(_))
        ));

        heartbeat_seen.notified().await;
        drop(inbound_tx);
        task.await.expect("control stream task exits cleanly");
        assert!(
            outbound_rx.recv().await.is_none(),
            "terminal-pending heartbeat must not emit FailedPrecondition"
        );
    }
}
