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

#[cfg(debug_assertions)]
use std::collections::BTreeMap;
use std::sync::Arc;
#[cfg(debug_assertions)]
use std::sync::{Mutex, OnceLock};

use tokio_stream::wrappers::ReceiverStream;

use novarocks::query_lifecycle::{QueryLifecycleError, QueryLifecycleErrorCode};
use novarocks_protocol::lifecycle::{
    ContractError, ContractErrorCode, QueryAbortRequest, QueryControlAttach,
    QueryControlCommand as ProtocolQueryControlCommand, QueryControlEvent, QueryExecutionId,
    QueryInitOutcome, QueryInitRequest, QueryStageRequest, QueryStartRequest, QueryTerminalAck,
    QueryTerminationReason,
};
use novarocks_protocol::novarocks as proto;

use crate::query_lifecycle::{BackendQueryControl, QueryLifecycleIngress};

const CONTROL_STREAM_CAPACITY: usize = 16;

pub type QueryControlResponseStream =
    ReceiverStream<Result<proto::QueryControlResponse, tonic::Status>>;

pub fn handle_init_query(
    ingress: &dyn QueryLifecycleIngress,
    request: proto::InitQueryRequest,
) -> Result<proto::InitQueryResponse, tonic::Status> {
    let request = QueryInitRequest::parse(request).map_err(status_from_contract_error)?;
    let execution_id = request
        .manifest()
        .map_err(status_from_contract_error)?
        .execution_id()
        .map_err(status_from_contract_error)?;
    let ack = ingress.init_query(request);
    if matches!(ack.outcome(), Ok(QueryInitOutcome::QueryInitApplied)) {
        if let Some(scope) =
            claim_backend_fault(QueryLifecycleFaultKind::RestartAfterInitAck, execution_id)?
        {
            eprintln!(
                "NOVAROCKS_QUERY_INIT_ACK_OBSERVED execution_id={}:{}:{} backend_index={} backend_id={} start_epoch={} token={}",
                execution_id.query_id().high(),
                execution_id.query_id().low(),
                execution_id.attempt_id().get(),
                scope.backend_index,
                scope.backend_id,
                scope.start_epoch,
                scope.token
            );
            wait_for_runner_owned_restart(&scope);
        }
        if let Some(scope) =
            claim_backend_fault(QueryLifecycleFaultKind::InitAckDrop, execution_id)?
        {
            eprintln!(
                "NOVAROCKS_QUERY_INIT_ACK_DROPPED execution_id={}:{}:{} backend_index={} backend_id={} start_epoch={} token={}",
                execution_id.query_id().high(),
                execution_id.query_id().low(),
                execution_id.attempt_id().get(),
                scope.backend_index,
                scope.backend_id,
                scope.start_epoch,
                scope.token
            );
            return Err(tonic::Status::deadline_exceeded(
                "runner-owned InitAck response dropped after Applied",
            ));
        }
    }
    Ok(ack.as_proto().clone())
}

pub fn handle_abort_query(
    ingress: &dyn QueryLifecycleIngress,
    request: proto::AbortQueryRequest,
) -> Result<proto::AbortQueryResponse, tonic::Status> {
    let request = QueryAbortRequest::parse(request).map_err(status_from_contract_error)?;
    let response = ingress
        .abort_query(request)
        .map_err(status_from_lifecycle_error)?;
    emit_query_lifecycle_abort_marker();
    Ok(response.as_proto().clone())
}

fn emit_query_lifecycle_abort_marker() {
    if novarocks::common::config::debug_emit_cancel_marker() {
        println!("NOVAROCKS_QUERY_LIFECYCLE_ABORT");
        let _ = std::io::Write::flush(&mut std::io::stdout());
    }
}

pub fn handle_stage_fragments(
    ingress: &dyn QueryLifecycleIngress,
    request: proto::StageFragmentsRequest,
) -> Result<proto::StageFragmentsResponse, tonic::Status> {
    let request = QueryStageRequest::parse(request).map_err(status_from_contract_error)?;
    let execution_id = request.execution_id();
    let response = ingress.stage_fragments(request);
    if response.outcome().is_staged() {
        if let Some(scope) = claim_backend_fault(
            QueryLifecycleFaultKind::HeartbeatStopAfterStage,
            execution_id,
        )? {
            register_staged_heartbeat_stop(scope);
        }
    }
    if response.outcome().is_staged()
        && let Some(scope) =
            claim_backend_fault(QueryLifecycleFaultKind::StageAckDrop, execution_id)?
    {
        eprintln!(
            "NOVAROCKS_STAGE_ACK_DROPPED execution_id={}:{}:{} backend_index={} token={}",
            execution_id.query_id().high(),
            execution_id.query_id().low(),
            execution_id.attempt_id().get(),
            scope.backend_index,
            scope.token
        );
        return Err(tonic::Status::deadline_exceeded(
            "runner-owned StageAck response dropped after staging",
        ));
    }
    Ok(response.as_proto().clone())
}

pub fn handle_start_prepared_query(
    ingress: &dyn QueryLifecycleIngress,
    request: proto::StartPreparedQueryRequest,
) -> Result<proto::StartPreparedQueryResponse, tonic::Status> {
    let request = QueryStartRequest::parse(request).map_err(status_from_contract_error)?;
    let execution_id = request.execution_id();
    let response = ingress.start_prepared_query(request);
    if response.outcome().is_running()
        && let Some(scope) =
            claim_backend_fault(QueryLifecycleFaultKind::StartAckDrop, execution_id)?
    {
        eprintln!(
            "NOVAROCKS_START_ACK_DROPPED execution_id={}:{}:{} backend_index={} token={}",
            execution_id.query_id().high(),
            execution_id.query_id().low(),
            execution_id.attempt_id().get(),
            scope.backend_index,
            scope.token
        );
        return Err(tonic::Status::deadline_exceeded(
            "runner-owned StartAck response dropped after release",
        ));
    }
    if response.outcome().is_running()
        && let Some(scope) =
            observe_backend_fault(QueryLifecycleFaultKind::StartAckSuppress, execution_id)?
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

pub async fn handle_query_control_stream(
    ingress: Arc<dyn QueryLifecycleIngress>,
    mut inbound: tonic::Streaming<proto::QueryControlRequest>,
    mut shutdown: Option<tokio::sync::watch::Receiver<bool>>,
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
        if let Some(scope) = claim_backend_fault(kind, execution_id)? {
            eprintln!(
                "NOVAROCKS_QUERY_TERMINAL_ATTACH_REJECTED kind={} execution_id={}:{}:{} backend_index={} backend_id={} start_epoch={} token={}",
                kind.file_stem(),
                scope.execution_id.query_id().high(),
                scope.execution_id.query_id().low(),
                scope.execution_id.attempt_id().get(),
                scope.backend_index,
                scope.backend_id,
                scope.start_epoch,
                scope.token,
            );
            return Err(tonic::Status::resource_exhausted(format!(
                "injected query lifecycle fault {} before ControlReady",
                kind.file_stem()
            )));
        }
    }
    let heartbeat_stop = claim_backend_fault(QueryLifecycleFaultKind::HeartbeatStop, execution_id)?;
    let terminal_snapshot_stream_drop = claim_backend_fault(
        QueryLifecycleFaultKind::TerminalSnapshotStreamDrop,
        execution_id,
    )?;
    let terminal_proof_stream_drop = claim_backend_fault(
        QueryLifecycleFaultKind::TerminalProofStreamDrop,
        execution_id,
    )?;
    let terminal_attestation_stream_drop = claim_backend_fault(
        QueryLifecycleFaultKind::TerminalAttestationStreamDrop,
        execution_id,
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
        outbound_tx,
        shutdown,
        heartbeat_stop,
        terminal_snapshot_stream_drop,
        terminal_proof_stream_drop,
        terminal_attestation_stream_drop,
        execution_id,
    ));
    Ok(ReceiverStream::new(outbound_rx))
}

async fn run_attached_control_stream(
    mut inbound: tonic::Streaming<proto::QueryControlRequest>,
    mut lease: CoordinatorLease,
    mut events: tokio::sync::mpsc::Receiver<QueryControlEvent>,
    outbound: tokio::sync::mpsc::Sender<Result<proto::QueryControlResponse, tonic::Status>>,
    mut shutdown: Option<tokio::sync::watch::Receiver<bool>>,
    heartbeat_stop: Option<QueryLifecycleFaultScope>,
    terminal_snapshot_stream_drop: Option<QueryLifecycleFaultScope>,
    terminal_proof_stream_drop: Option<QueryLifecycleFaultScope>,
    terminal_attestation_stream_drop: Option<QueryLifecycleFaultScope>,
    execution_id: QueryExecutionId,
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
    let mut heartbeat_stop_logged = false;
    loop {
        tokio::select! {
            biased;
            _ = wait_for_query_control_shutdown(&mut shutdown) => {
                break;
            }
            inbound_message = inbound.message() => {
                let request = match inbound_message {
                    Ok(Some(request)) => request,
                    Ok(None) => break,
                    Err(error) => {
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
                let command = match ProtocolQueryControlCommand::parse(request)
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
                        if let Some(scope) = heartbeat_stop
                            .as_ref()
                            .cloned()
                            .or_else(|| staged_heartbeat_stop(execution_id))
                        {
                            if !heartbeat_stop_logged {
                                eprintln!(
                                    "NOVAROCKS_QUERY_CONTROL_HEARTBEAT_STOPPED execution_id={}:{}:{} backend_index={} backend_id={} start_epoch={} token={}",
                                    scope.execution_id.query_id().high(),
                                    scope.execution_id.query_id().low(),
                                    scope.execution_id.attempt_id().get(),
                                    scope.backend_index,
                                    scope.backend_id,
                                    scope.start_epoch,
                                    scope.token
                                );
                                heartbeat_stop_logged = true;
                            }
                            Ok(())
                        } else {
                            lease.control().heartbeat(heartbeat.sequence)
                        }
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
            event = events.recv() => {
                let Some(event) = event else {
                    break;
                };
                let terminal_stream_drop = match event.as_proto().event.as_ref() {
                    Some(proto::query_control_response::Event::TerminalOutcome(outcome)) => {
                        match outcome.outcome.as_ref() {
                            Some(proto::participant_terminal_outcome::Outcome::Proof(_)) => {
                                terminal_proof_stream_drop
                                    .as_ref()
                                    .or(terminal_snapshot_stream_drop.as_ref())
                            }
                            Some(proto::participant_terminal_outcome::Outcome::NegativeAttestation(_)) => {
                                terminal_attestation_stream_drop
                                    .as_ref()
                                    .or(terminal_snapshot_stream_drop.as_ref())
                            }
                            None => None,
                        }
                    }
                    _ => None,
                };
                if let Some(scope) = terminal_stream_drop {
                    eprintln!(
                        "NOVAROCKS_QUERY_TERMINAL_STREAM_DROPPED execution_id={}:{}:{} backend_index={} backend_id={} start_epoch={} token={}",
                        scope.execution_id.query_id().high(),
                        scope.execution_id.query_id().low(),
                        scope.execution_id.attempt_id().get(),
                        scope.backend_index,
                        scope.backend_id,
                        scope.start_epoch,
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
        }
    }
}

#[cfg(debug_assertions)]
use novarocks::common::query_lifecycle_fault::observe_matching_fault;
use novarocks::common::query_lifecycle_fault::{QueryLifecycleFaultKind, QueryLifecycleFaultScope};
#[cfg(debug_assertions)]
use novarocks::common::query_lifecycle_fault::{claim_matching_fault, trigger_path};

#[cfg(debug_assertions)]
fn staged_heartbeat_stops() -> &'static Mutex<
    BTreeMap<novarocks_protocol::lifecycle::QueryExecutionId, QueryLifecycleFaultScope>,
> {
    static STOPS: OnceLock<
        Mutex<BTreeMap<novarocks_protocol::lifecycle::QueryExecutionId, QueryLifecycleFaultScope>>,
    > = OnceLock::new();
    STOPS.get_or_init(|| Mutex::new(BTreeMap::new()))
}

#[cfg(debug_assertions)]
fn register_staged_heartbeat_stop(scope: QueryLifecycleFaultScope) {
    let mut stops = staged_heartbeat_stops()
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    stops.insert(scope.execution_id, scope);
}

#[cfg(not(debug_assertions))]
fn register_staged_heartbeat_stop(_scope: QueryLifecycleFaultScope) {}

#[cfg(debug_assertions)]
fn staged_heartbeat_stop(execution_id: QueryExecutionId) -> Option<QueryLifecycleFaultScope> {
    staged_heartbeat_stops()
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .get(&execution_id)
        .cloned()
}

#[cfg(not(debug_assertions))]
fn staged_heartbeat_stop(_execution_id: QueryExecutionId) -> Option<QueryLifecycleFaultScope> {
    None
}

#[cfg(debug_assertions)]
fn claim_backend_fault(
    kind: QueryLifecycleFaultKind,
    execution_id: QueryExecutionId,
) -> Result<Option<QueryLifecycleFaultScope>, tonic::Status> {
    let Some(root) = novarocks::common::query_lifecycle_fault::configured_root() else {
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
    let backend_id = novarocks::runtime::backend_id::backend_id()
        .and_then(|id| u64::try_from(id).ok())
        .ok_or_else(|| tonic::Status::failed_precondition("backend identity is not bound"))?;
    claim_matching_fault(
        &root,
        kind,
        execution_id,
        backend_index,
        backend_id,
        novarocks::runtime::start_epoch::start_epoch(),
    )
    .map_err(tonic::Status::failed_precondition)
}

/// `RestartAfterInitAck` is a runner-owned rendezvous: the BE emits its
/// token-scoped marker, then waits for the runner to terminate that exact
/// process.  Without the wait, a small query can finish between the marker
/// write and the parent's kill, which does not prove loss after admission.
#[cfg(debug_assertions)]
fn wait_for_runner_owned_restart(scope: &QueryLifecycleFaultScope) {
    let Some(root) = novarocks::common::query_lifecycle_fault::configured_root() else {
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
fn observe_backend_fault(
    kind: QueryLifecycleFaultKind,
    execution_id: QueryExecutionId,
) -> Result<Option<QueryLifecycleFaultScope>, tonic::Status> {
    let Some(root) = novarocks::common::query_lifecycle_fault::configured_root() else {
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
    let backend_id = novarocks::runtime::backend_id::backend_id()
        .and_then(|id| u64::try_from(id).ok())
        .ok_or_else(|| tonic::Status::failed_precondition("backend identity is not bound"))?;
    observe_matching_fault(
        &root,
        kind,
        execution_id,
        backend_index,
        backend_id,
        novarocks::runtime::start_epoch::start_epoch(),
    )
    .map_err(tonic::Status::failed_precondition)
}

#[cfg(not(debug_assertions))]
fn claim_backend_fault(
    _kind: QueryLifecycleFaultKind,
    _execution_id: QueryExecutionId,
) -> Result<Option<QueryLifecycleFaultScope>, tonic::Status> {
    Ok(None)
}

#[cfg(not(debug_assertions))]
fn observe_backend_fault(
    _kind: QueryLifecycleFaultKind,
    _execution_id: QueryExecutionId,
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

pub fn status_from_lifecycle_error(error: QueryLifecycleError) -> tonic::Status {
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

pub(crate) fn status_from_contract_error(error: ContractError) -> tonic::Status {
    let detail = error.detail().to_string();
    match error.code() {
        ContractErrorCode::InvalidValue | ContractErrorCode::VersionMismatch => {
            tonic::Status::invalid_argument(detail)
        }
        ContractErrorCode::Conflict | ContractErrorCode::DigestMismatch => {
            tonic::Status::already_exists(detail)
        }
        ContractErrorCode::Capacity => tonic::Status::resource_exhausted(detail),
    }
}
