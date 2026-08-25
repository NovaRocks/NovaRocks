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

use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::pin::Pin;
use std::sync::{Arc, Condvar, Mutex};
use std::time::{Duration, Instant};

use crate::common::backend_topology::LiveBackendTarget;
use crate::common::query_cancellation::{QueryCancellationReason, QueryCancellationSource};
use crate::native::data_runtime::FrontendDataRuntime;
use crate::native::fragment_transport::{
    ExpectedOutputSchemaView, FetchOutcome, FragmentDispatcher,
};
use crate::native::transport::new_query_lifecycle_transport;
use crate::query_execution::contract::DistributedQueryIntent;
use crate::query_execution::lifecycle_plan::{QueryInitBarrier, QueryInitPlan};
use crate::{QueryLifecycleError, QueryLifecycleErrorCode};
use novarocks_proto::lifecycle as protocol_lifecycle;
use novarocks_proto::lifecycle::{
    AttemptId, FragmentLiveObservation, ParticipantBackendIdentity, ParticipantManifest,
    ParticipantRole, QueryControlCommand as ProtocolQueryControlCommand, QueryControlEndpoint,
    QueryControlEvent as ProtocolQueryControlEvent, QueryExecutionId, QueryInitAck,
    QueryInitOutcome, QueryInitRequest, QueryOptions, QueryTerminationAck, QueryTerminationReason,
    RuntimeFilterContribution,
};
use novarocks_proto::{common as proto_common, filter, novarocks as proto};
use novarocks_types::QueryId;
use novarocks_types::UniqueId;
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status};

fn frontend_data_runtime_for_test() -> FrontendDataRuntime {
    FrontendDataRuntime::new(tokio::runtime::Handle::current())
}

use super::barrier::{
    FrontendQueryLifecycleBarrier, FrontendQueryLifecycleConfig, PreReadyAttemptGuard,
};
use super::lease::{
    ActiveSession, AttemptControl, FragmentObservationStoreOutcome, FrontendLifecycleMetrics,
    TerminalOutcomeStoreOutcome,
};
use super::{
    QueryControlSession, QueryLifecycleTarget, QueryLifecycleTransport,
    QueryLifecycleTransportError, QueryLifecycleTransportErrorKind,
};
use crate::coordinator::query_registry::{
    ActiveQueryAttemptControl, FrontendQueryRegistry, QueryLifecycleConvergenceErrorSource,
    RuntimeFilterTerminalRollupSnapshot, RuntimeFilterTerminalRollupUnavailable,
};

fn terminal_outcome(
    snapshot: protocol_lifecycle::QueryTerminalSnapshot,
) -> protocol_lifecycle::ParticipantTerminalOutcome {
    let execution_id = snapshot.execution_id();
    let backend = snapshot.backend();
    let init_digest = snapshot.init_digest().as_bytes().to_vec();
    let proof = protocol_lifecycle::TerminalizationProof::seal(proto::TerminalizationProof {
        version: 1,
        execution_id: Some(execution_id.into()),
        backend: Some(backend.as_proto().clone()),
        init_digest,
        digest: Vec::new(),
        fragments: Vec::new(),
    })
    .expect("protocol terminal proof");
    protocol_lifecycle::ParticipantTerminalOutcome::parse(proto::ParticipantTerminalOutcome {
        snapshot: Some(snapshot.as_proto().clone()),
        outcome: Some(proto::participant_terminal_outcome::Outcome::Proof(
            proof.as_proto().clone(),
        )),
    })
    .expect("protocol terminal proof outcome")
}

fn terminal_snapshot(
    execution_id: QueryExecutionId,
    backend: ParticipantBackendIdentity,
    digest: protocol_lifecycle::ParticipantManifestDigest,
) -> protocol_lifecycle::QueryTerminalSnapshot {
    protocol_lifecycle::QueryTerminalSnapshot::seal(proto::QueryTerminalSnapshot {
        version: 1,
        execution_id: Some(execution_id.into()),
        backend: Some(backend.as_proto().clone()),
        init_digest: digest.as_bytes().to_vec(),
        digest: Vec::new(),
        fragments: Vec::new(),
        profile_contribution: Some(proto::QueryTerminalProfileContributionTelemetry {
            telemetry: Some(
                proto::query_terminal_profile_contribution_telemetry::Telemetry::Unavailable(
                    proto::TerminalTelemetryUnavailable {
                        stage: "test".to_string(),
                        code: "TEST".to_string(),
                    },
                ),
            ),
        }),
    })
    .expect("protocol terminal snapshot")
}

fn negative_attestation_outcome(
    execution_id: QueryExecutionId,
    backend: ParticipantBackendIdentity,
    digest: protocol_lifecycle::ParticipantManifestDigest,
) -> protocol_lifecycle::ParticipantTerminalOutcome {
    let attestation = protocol_lifecycle::NegativeAttestation::seal(proto::NegativeAttestation {
        execution_id: Some(execution_id.into()),
        backend: Some(backend.as_proto().clone()),
        init_digest: digest.as_bytes().to_vec(),
        reason: proto::NegativeAttestationReason::CorrectnessEvidenceRetentionExhausted as i32,
        detail: "terminal evidence retention exhausted".to_string(),
        detail_truncated: false,
        digest: Vec::new(),
    })
    .expect("protocol negative attestation");
    protocol_lifecycle::ParticipantTerminalOutcome::parse(proto::ParticipantTerminalOutcome {
        snapshot: None,
        outcome: Some(
            proto::participant_terminal_outcome::Outcome::NegativeAttestation(
                attestation.as_proto().clone(),
            ),
        ),
    })
    .expect("protocol negative attestation outcome")
}

fn protocol_event(
    event: proto::query_control_response::Event,
) -> protocol_lifecycle::QueryControlEvent {
    protocol_lifecycle::QueryControlEvent::parse(proto::QueryControlResponse { event: Some(event) })
        .expect("protocol control event")
}

// These small test-only views make assertions legible. Transport fixtures
// immediately lower them to generated frames and re-parse Protocol wrappers;
// production code never sees, stores, or encodes these views.
#[derive(Clone, Debug, PartialEq)]
enum QueryControlCommand {
    Heartbeat { sequence: u64 },
    Abort { reason: String },
    Finalize,
    TerminalAck,
}

#[derive(Clone, Debug, PartialEq)]
enum QueryControlEvent {
    ControlReady,
    HeartbeatAck { sequence: u64 },
    LocalDrained,
    LocalFailure { code: String, detail: String },
    TerminationAccepted { reason: QueryTerminationReason },
}

fn query_control_command_view(
    command: &ProtocolQueryControlCommand,
) -> Result<QueryControlCommand, String> {
    match command.as_proto().command.as_ref() {
        Some(proto::query_control_request::Command::Heartbeat(heartbeat)) => {
            Ok(QueryControlCommand::Heartbeat {
                sequence: heartbeat.sequence,
            })
        }
        Some(proto::query_control_request::Command::Abort(abort)) => {
            Ok(QueryControlCommand::Abort {
                reason: abort.reason.clone(),
            })
        }
        Some(proto::query_control_request::Command::Finalize(_)) => {
            Ok(QueryControlCommand::Finalize)
        }
        Some(proto::query_control_request::Command::TerminalAck(_)) => {
            Ok(QueryControlCommand::TerminalAck)
        }
        Some(proto::query_control_request::Command::Attach(_)) | None => {
            Err("active command must not be attach or empty".to_string())
        }
    }
}

fn protocol_control_event(event: QueryControlEvent) -> ProtocolQueryControlEvent {
    match event {
        QueryControlEvent::ControlReady => protocol_event_control_ready(),
        QueryControlEvent::HeartbeatAck { sequence } => protocol_event_heartbeat_ack(sequence),
        QueryControlEvent::LocalDrained => protocol_event_local_drained(),
        QueryControlEvent::LocalFailure { code, detail } => {
            protocol_event_local_failure(code, detail)
        }
        QueryControlEvent::TerminationAccepted { reason } => protocol_event_termination(reason),
    }
}

fn protocol_command(
    command: proto::query_control_request::Command,
) -> protocol_lifecycle::QueryControlCommand {
    protocol_lifecycle::QueryControlCommand::parse(proto::QueryControlRequest {
        command: Some(command),
    })
    .expect("protocol control command")
}

fn protocol_heartbeat(sequence: u64) -> protocol_lifecycle::QueryControlCommand {
    protocol_command(proto::query_control_request::Command::Heartbeat(
        proto::QueryControlHeartbeat {
            sequence,
            sent_mono_ns: sequence,
        },
    ))
}

fn protocol_finalize() -> protocol_lifecycle::QueryControlCommand {
    protocol_command(proto::query_control_request::Command::Finalize(
        proto::QueryControlFinalize {},
    ))
}

fn protocol_abort_command(reason: impl Into<String>) -> protocol_lifecycle::QueryControlCommand {
    protocol_command(proto::query_control_request::Command::Abort(
        proto::QueryControlAbort {
            reason: reason.into(),
        },
    ))
}

fn protocol_terminal_ack_command(
    outcome: &protocol_lifecycle::ParticipantTerminalOutcome,
) -> protocol_lifecycle::QueryControlCommand {
    let snapshot = outcome.snapshot().expect("fixture terminal snapshot");
    let ack = protocol_lifecycle::QueryTerminalAck::parse(proto::QueryControlTerminalAck {
        execution_id: Some(outcome.execution_id().into()),
        init_digest: outcome.init_digest().as_bytes().to_vec(),
        snapshot_version: snapshot.version(),
        snapshot_digest: outcome.digest().to_vec(),
    })
    .expect("protocol terminal acknowledgement");
    protocol_command(proto::query_control_request::Command::TerminalAck(
        ack.as_proto().clone(),
    ))
}

fn protocol_attach_for(
    execution_id: protocol_lifecycle::QueryExecutionId,
    digest: protocol_lifecycle::ParticipantManifestDigest,
    epoch: u64,
) -> protocol_lifecycle::QueryControlAttach {
    protocol_lifecycle::QueryControlAttach::parse(proto::QueryControlAttach {
        execution_id: Some(execution_id.into()),
        init_digest: digest.as_bytes().to_vec(),
        frontend_owner_epoch: epoch,
    })
    .expect("protocol control attach")
}

fn protocol_abort_for(
    execution_id: protocol_lifecycle::QueryExecutionId,
    digest: protocol_lifecycle::ParticipantManifestDigest,
    reason: impl Into<String>,
) -> protocol_lifecycle::QueryAbortRequest {
    protocol_lifecycle::QueryAbortRequest::parse(proto::AbortQueryRequest {
        execution_id: Some(execution_id.into()),
        init_digest: digest.as_bytes().to_vec(),
        reason: reason.into(),
    })
    .expect("protocol abort request")
}

fn protocol_event_control_ready() -> protocol_lifecycle::QueryControlEvent {
    protocol_event(proto::query_control_response::Event::ControlReady(
        proto::QueryControlReady {},
    ))
}

fn protocol_event_local_drained() -> protocol_lifecycle::QueryControlEvent {
    protocol_event(proto::query_control_response::Event::LocalDrained(
        proto::QueryControlLocalDrained {},
    ))
}

fn protocol_event_local_failure(
    code: impl Into<String>,
    detail: impl Into<String>,
) -> protocol_lifecycle::QueryControlEvent {
    protocol_event(proto::query_control_response::Event::LocalFailure(
        proto::QueryControlLocalFailure {
            code: code.into(),
            detail: detail.into(),
        },
    ))
}

fn protocol_event_heartbeat_ack(sequence: u64) -> protocol_lifecycle::QueryControlEvent {
    protocol_event(proto::query_control_response::Event::HeartbeatAck(
        proto::QueryControlHeartbeatAck { sequence },
    ))
}

fn protocol_event_termination(
    reason: proto::QueryTerminationReason,
) -> protocol_lifecycle::QueryControlEvent {
    protocol_event(proto::query_control_response::Event::TerminationAccepted(
        proto::QueryControlTerminationAccepted {
            reason: reason as i32,
        },
    ))
}

fn protocol_terminal_outcome_event(
    outcome: protocol_lifecycle::ParticipantTerminalOutcome,
) -> protocol_lifecycle::QueryControlEvent {
    protocol_event(proto::query_control_response::Event::TerminalOutcome(
        outcome.as_proto().clone(),
    ))
}

#[expect(
    clippy::too_many_arguments,
    reason = "The wire fixture keeps each fragment observation field explicit."
)]
fn protocol_fragment_observation(
    execution_id: QueryExecutionId,
    digest: protocol_lifecycle::ParticipantManifestDigest,
    backend: ParticipantBackendIdentity,
    fragment_instance_id: proto_common::UniqueId,
    sequence: u64,
    input_rows: u64,
    output_rows: u64,
    elapsed_ms: u64,
) -> protocol_lifecycle::FragmentLiveObservation {
    protocol_lifecycle::FragmentLiveObservation::parse(proto::FragmentLiveObservation {
        execution_id: Some(execution_id.into()),
        init_digest: digest.as_bytes().to_vec(),
        backend: Some(backend.as_proto().clone()),
        fragment_instance_id: Some(fragment_instance_id),
        sequence,
        input_rows,
        output_rows,
        elapsed_ms,
        profile: None,
    })
    .expect("protocol fragment observation")
}

/// Test-only BE-shaped contract for the generated FE client wire tests.
/// Production ownership is in `novarocks-backend`; this peer deliberately
/// avoids adding a Frontend-to-Backend test dependency.
trait QueryLifecycleIngress: Send + Sync + 'static {
    #[allow(
        dead_code,
        reason = "Retained for generated FE client lifecycle contract coverage."
    )]
    fn bind_backend_identity(&self, backend_id: u64) -> Result<(), QueryLifecycleError>;

    fn init_query(
        &self,
        request: protocol_lifecycle::QueryInitRequest,
    ) -> protocol_lifecycle::QueryInitAck;

    fn stage_fragments(
        &self,
        request: protocol_lifecycle::QueryStageRequest,
    ) -> protocol_lifecycle::QueryStageAck {
        protocol_lifecycle::QueryStageAck::new(
            request.execution_id(),
            request.digest_version(),
            request.digest(),
            protocol_lifecycle::QueryStageOutcome::RejectedInvalidState,
            "StageFragments is not supported by this lifecycle ingress",
        )
        .expect("protocol stage rejection")
    }

    fn start_prepared_query(
        &self,
        request: protocol_lifecycle::QueryStartRequest,
    ) -> protocol_lifecycle::QueryStartAck {
        protocol_lifecycle::QueryStartAck::new(
            request.execution_id(),
            request.digest_version(),
            request.digest(),
            protocol_lifecycle::QueryStartOutcome::RejectedNotStaged,
            "StartPreparedQuery is not supported by this lifecycle ingress",
        )
        .expect("protocol start rejection")
    }

    fn abort_query(
        &self,
        request: protocol_lifecycle::QueryAbortRequest,
    ) -> Result<protocol_lifecycle::QueryTerminationAck, QueryLifecycleError>;

    fn attach_control(
        &self,
        attach: protocol_lifecycle::QueryControlAttach,
    ) -> Result<QueryControlAttachment, QueryLifecycleError>;
}

trait BackendQueryControl: Send + Sync + 'static {
    fn heartbeat(&self, sequence: u64) -> Result<(), QueryLifecycleError>;

    fn abort(&self, reason: String) -> Result<(), QueryLifecycleError>;

    fn finalize(&self) -> Result<(), QueryLifecycleError>;

    fn terminal_ack(
        &self,
        _ack: protocol_lifecycle::QueryTerminalAck,
    ) -> Result<(), QueryLifecycleError> {
        Err(QueryLifecycleError::new(
            QueryLifecycleErrorCode::Terminated,
            "query terminal acknowledgement is not supported by this lifecycle owner",
        ))
    }

    #[allow(
        dead_code,
        reason = "Retained for generated FE client lifecycle contract coverage."
    )]
    fn coordinator_lost(&self, reason: QueryTerminationReason) -> Result<(), QueryLifecycleError>;
}

struct QueryControlAttachment {
    control: Arc<dyn BackendQueryControl>,
    events: tokio::sync::mpsc::Receiver<protocol_lifecycle::QueryControlEvent>,
    #[allow(
        dead_code,
        reason = "Retained to model the complete control-stream attachment fixture."
    )]
    observations: tokio::sync::watch::Receiver<Option<FragmentLiveObservation>>,
}

#[derive(Default)]
struct NoopFragmentDispatcher;

impl FragmentDispatcher for NoopFragmentDispatcher {
    fn fetch_result(
        &self,
        _backend_idx: usize,
        _finst_id: UniqueId,
        _max_wait_ms: i64,
        _expected_output_schema: Option<ExpectedOutputSchemaView<'_>>,
    ) -> Result<FetchOutcome, String> {
        unreachable!("query lifecycle unit tests do not fetch results")
    }

    fn backend_count(&self) -> usize {
        3
    }
}

#[derive(Clone)]
struct RecordingSession {
    state: Arc<(Mutex<RecordingSessionState>, Condvar)>,
}

#[derive(Default)]
struct RecordingSessionState {
    commands: Vec<QueryControlCommand>,
    events: VecDeque<Result<protocol_lifecycle::QueryControlEvent, QueryLifecycleTransportError>>,
    send_errors: VecDeque<QueryLifecycleTransportError>,
    terminal_snapshot: Option<protocol_lifecycle::QueryTerminalSnapshot>,
    emit_terminal_snapshot_on_finalize: bool,
}

impl RecordingSession {
    fn with_events(
        events: impl IntoIterator<
            Item = Result<protocol_lifecycle::QueryControlEvent, QueryLifecycleTransportError>,
        >,
    ) -> Self {
        let state = RecordingSessionState {
            events: events.into_iter().collect(),
            ..RecordingSessionState::default()
        };
        Self {
            state: Arc::new((Mutex::new(state), Condvar::new())),
        }
    }

    fn with_terminal_snapshot(
        events: impl IntoIterator<
            Item = Result<protocol_lifecycle::QueryControlEvent, QueryLifecycleTransportError>,
        >,
        terminal_snapshot: protocol_lifecycle::QueryTerminalSnapshot,
    ) -> Self {
        let state = RecordingSessionState {
            events: events.into_iter().collect(),
            terminal_snapshot: Some(terminal_snapshot),
            emit_terminal_snapshot_on_finalize: true,
            ..RecordingSessionState::default()
        };
        Self {
            state: Arc::new((Mutex::new(state), Condvar::new())),
        }
    }

    fn commands(&self) -> Vec<QueryControlCommand> {
        self.state
            .0
            .lock()
            .expect("recording session lock")
            .commands
            .clone()
    }

    fn fail_next_send(&self, error: QueryLifecycleTransportError) {
        self.state
            .0
            .lock()
            .expect("recording session lock")
            .send_errors
            .push_back(error);
    }

    fn clear_commands(&self) {
        self.state
            .0
            .lock()
            .expect("recording session lock")
            .commands
            .clear();
    }

    fn suppress_terminal_snapshot_on_finalize(&self) {
        self.state
            .0
            .lock()
            .expect("recording session lock")
            .emit_terminal_snapshot_on_finalize = false;
    }

    fn terminal_snapshot(&self) -> protocol_lifecycle::QueryTerminalSnapshot {
        self.state
            .0
            .lock()
            .expect("recording session lock")
            .terminal_snapshot
            .clone()
            .expect("recording terminal snapshot")
    }

    fn push_event(&self, event: protocol_lifecycle::QueryControlEvent) {
        self.state
            .0
            .lock()
            .expect("recording session lock")
            .events
            .push_back(Ok(event));
        self.state.1.notify_all();
    }

    fn push_protocol_event(&self, event: protocol_lifecycle::QueryControlEvent) {
        self.state
            .0
            .lock()
            .expect("recording session lock")
            .events
            .push_back(Ok(event));
        self.state.1.notify_all();
    }

    fn state_ref_count(&self) -> usize {
        Arc::strong_count(&self.state)
    }
}

impl QueryControlSession for RecordingSession {
    fn send(
        &self,
        command: protocol_lifecycle::QueryControlCommand,
    ) -> Result<(), QueryLifecycleTransportError> {
        let command_view = query_control_command_view(&command)
            .expect("test transport accepts only validated protocol commands");
        let mut state = self.state.0.lock().expect("recording session lock");
        if let Some(error) = state.send_errors.pop_front() {
            state.commands.push(command_view);
            return Err(error);
        }
        let terminal = match command.as_proto().command.as_ref() {
            Some(proto::query_control_request::Command::Abort(_)) => {
                Some(QueryTerminationReason::CoordinatorAbort)
            }
            Some(proto::query_control_request::Command::Finalize(_)) => {
                Some(QueryTerminationReason::CoordinatorFinalize)
            }
            Some(proto::query_control_request::Command::Heartbeat(_))
            | Some(proto::query_control_request::Command::TerminalAck(_)) => None,
            Some(proto::query_control_request::Command::Attach(_)) | None => {
                unreachable!("validated active control command")
            }
        };
        state.commands.push(command_view);
        if state.emit_terminal_snapshot_on_finalize
            && matches!(terminal, Some(QueryTerminationReason::CoordinatorFinalize))
            && let Some(snapshot) = state.terminal_snapshot.clone()
        {
            state
                .events
                .push_back(Ok(protocol_terminal_outcome_event(terminal_outcome(
                    snapshot,
                ))));
        }
        if let Some(reason) = terminal {
            state
                .events
                .push_back(Ok(protocol_event_termination(reason)));
        }
        drop(state);
        self.state.1.notify_all();
        Ok(())
    }

    fn recv_timeout(
        &self,
        timeout: Duration,
    ) -> Result<protocol_lifecycle::QueryControlEvent, QueryLifecycleTransportError> {
        let (lock, ready) = &*self.state;
        let mut state = lock.lock().expect("recording session lock");
        if state.events.is_empty() {
            let (next, _) = ready
                .wait_timeout(state, timeout)
                .expect("recording session wait");
            state = next;
        }
        state.events.pop_front().unwrap_or_else(|| {
            Err(QueryLifecycleTransportError::new(
                QueryLifecycleTransportErrorKind::DeadlineExceeded,
                "recording session receive timed out",
            ))
        })
    }
}

#[derive(Clone)]
struct RecordingTransport {
    state: Arc<Mutex<RecordingTransportState>>,
}

#[derive(Default)]
#[expect(
    clippy::type_complexity,
    reason = "The transport fixture retains typed per-backend scripted outcomes."
)]
struct RecordingTransportState {
    init_results: BTreeMap<
        usize,
        VecDeque<Result<protocol_lifecycle::QueryInitAck, QueryLifecycleTransportError>>,
    >,
    attach_results: BTreeMap<
        usize,
        VecDeque<Result<Arc<dyn QueryControlSession>, QueryLifecycleTransportError>>,
    >,
    init_calls: Vec<(QueryLifecycleTarget, protocol_lifecycle::QueryInitRequest)>,
    attach_calls: Vec<(QueryLifecycleTarget, protocol_lifecycle::QueryControlAttach)>,
    abort_calls: Vec<(QueryLifecycleTarget, protocol_lifecycle::QueryAbortRequest)>,
    abort_results: BTreeMap<
        usize,
        VecDeque<Result<protocol_lifecycle::QueryTerminationAck, QueryLifecycleTransportError>>,
    >,
    cancel_on_init: Option<QueryCancellationSource>,
}

impl RecordingTransport {
    fn ready(plan: &QueryInitPlan) -> (Self, BTreeMap<usize, RecordingSession>) {
        Self::ready_with_drain(plan, true)
    }

    fn ready_without_local_drain(
        plan: &QueryInitPlan,
    ) -> (Self, BTreeMap<usize, RecordingSession>) {
        Self::ready_with_drain(plan, false)
    }

    fn ready_with_drain(
        plan: &QueryInitPlan,
        include_local_drain: bool,
    ) -> (Self, BTreeMap<usize, RecordingSession>) {
        let mut state = RecordingTransportState::default();
        let mut sessions = BTreeMap::new();
        for backend_idx in plan.backend_ids() {
            let participant = plan
                .participant(backend_idx)
                .expect("fixture participant exists");
            state.init_results.insert(
                backend_idx,
                VecDeque::from([Ok(QueryInitAck::new(
                    plan.execution_id(),
                    participant.digest(),
                    QueryInitOutcome::Applied,
                ))]),
            );
            let backend = participant.backend().clone();
            let snapshot = terminal_snapshot(plan.execution_id(), backend, participant.digest());
            let mut events = vec![Ok(protocol_control_event(QueryControlEvent::ControlReady))];
            if include_local_drain {
                events.push(Ok(protocol_control_event(QueryControlEvent::LocalDrained)));
            }
            let session = RecordingSession::with_terminal_snapshot(events, snapshot);
            state.attach_results.insert(
                backend_idx,
                VecDeque::from([Ok(Arc::new(session.clone()) as Arc<dyn QueryControlSession>)]),
            );
            sessions.insert(backend_idx, session);
        }
        (
            Self {
                state: Arc::new(Mutex::new(state)),
            },
            sessions,
        )
    }

    fn init_calls(&self) -> Vec<(QueryLifecycleTarget, protocol_lifecycle::QueryInitRequest)> {
        self.state
            .lock()
            .expect("recording transport lock")
            .init_calls
            .clone()
    }

    fn attach_targets(&self) -> Vec<usize> {
        self.state
            .lock()
            .expect("recording transport lock")
            .attach_calls
            .iter()
            .map(|(target, _)| target.backend_idx())
            .collect()
    }

    fn abort_targets(&self) -> Vec<usize> {
        self.state
            .lock()
            .expect("recording transport lock")
            .abort_calls
            .iter()
            .map(|(target, _)| target.backend_idx())
            .collect()
    }
}

impl QueryLifecycleTransport for RecordingTransport {
    fn init_query(
        &self,
        target: QueryLifecycleTarget,
        request: protocol_lifecycle::QueryInitRequest,
        _timeout: Duration,
    ) -> Result<protocol_lifecycle::QueryInitAck, QueryLifecycleTransportError> {
        let mut state = self.state.lock().expect("recording transport lock");
        state.init_calls.push((target, request));
        let result = state
            .init_results
            .get_mut(&target.backend_idx())
            .and_then(VecDeque::pop_front)
            .unwrap_or_else(|| {
                Err(QueryLifecycleTransportError::new(
                    QueryLifecycleTransportErrorKind::InvalidResponse,
                    "unexpected InitQuery call",
                ))
            });
        let cancellation = state.cancel_on_init.take();
        drop(state);
        if let Some(cancellation) = cancellation {
            cancellation.request(QueryCancellationReason::ClientDisconnected);
        }
        result
    }

    fn attach_control(
        &self,
        target: QueryLifecycleTarget,
        attach: protocol_lifecycle::QueryControlAttach,
        _timeout: Duration,
    ) -> Result<Arc<dyn QueryControlSession>, QueryLifecycleTransportError> {
        let mut state = self.state.lock().expect("recording transport lock");
        state.attach_calls.push((target, attach));
        state
            .attach_results
            .get_mut(&target.backend_idx())
            .and_then(VecDeque::pop_front)
            .unwrap_or_else(|| {
                Err(QueryLifecycleTransportError::new(
                    QueryLifecycleTransportErrorKind::InvalidResponse,
                    "unexpected control attach call",
                ))
            })
    }

    fn abort_query(
        &self,
        target: QueryLifecycleTarget,
        request: protocol_lifecycle::QueryAbortRequest,
        _timeout: Duration,
    ) -> Result<protocol_lifecycle::QueryTerminationAck, QueryLifecycleTransportError> {
        let mut state = self.state.lock().expect("recording transport lock");
        state.abort_calls.push((target, request.clone()));
        state
            .abort_results
            .get_mut(&target.backend_idx())
            .and_then(VecDeque::pop_front)
            .unwrap_or_else(|| {
                Ok(QueryTerminationAck::new(
                    request.execution_id().expect("validated request id"),
                    QueryTerminationReason::CoordinatorAbort,
                ))
            })
    }
}

fn transport_error(
    kind: QueryLifecycleTransportErrorKind,
    detail: &str,
) -> QueryLifecycleTransportError {
    QueryLifecycleTransportError::new(kind, detail)
}

fn query_execution_id() -> QueryExecutionId {
    QueryExecutionId::new(
        QueryId::new(71, 72),
        AttemptId::new(1).expect("fixture attempt id"),
    )
    .expect("fixture execution id")
}

fn proto_id(id: UniqueId) -> proto_common::UniqueId {
    proto_common::UniqueId {
        hi: id.high(),
        lo: id.low(),
    }
}

fn protocol_backend_from_live(backend: LiveBackendTarget) -> ParticipantBackendIdentity {
    let endpoint = backend.endpoint();
    ParticipantBackendIdentity::new(
        backend.backend_idx() as u64,
        QueryControlEndpoint::new(endpoint.ip().to_string(), endpoint.port())
            .expect("live backend endpoint"),
        backend.start_epoch(),
    )
    .expect("live backend identity")
}

fn manifest(
    execution_id: QueryExecutionId,
    backend_idx: usize,
    service_only: bool,
) -> ParticipantManifest {
    let endpoint = QueryControlEndpoint::new("127.0.0.1", 18_000 + backend_idx as u16)
        .expect("fixture backend endpoint");
    let backend =
        ParticipantBackendIdentity::new(backend_idx as u64, endpoint, 90 + backend_idx as u64)
            .expect("fixture backend identity");
    let (roles, fragments, runtime_filter) = if service_only {
        (
            BTreeSet::from([ParticipantRole::RuntimeFilterService]),
            Vec::<proto_common::UniqueId>::new(),
            Some(
                RuntimeFilterContribution::parse(proto::RuntimeFilterContribution {
                    participant_id: backend_idx as u32 + 1,
                    contribution_digest: vec![0; 32],
                    ..Default::default()
                })
                .expect("fixture runtime-filter contribution"),
            ),
        )
    } else {
        (
            BTreeSet::from([ParticipantRole::FragmentExecutor]),
            vec![proto_id(UniqueId::new(100, backend_idx as i64 + 1))],
            None,
        )
    };
    ParticipantManifest::new(
        execution_id,
        backend,
        roles,
        fragments,
        QueryOptions::parse(proto::QueryOptions::default()).expect("fixture query options"),
        1_900_000_000_000,
        [],
        runtime_filter,
        Duration::from_secs(30),
        QueryControlEndpoint::new("127.0.0.1", 19_000).expect("fixture report endpoint"),
    )
    .expect("fixture participant manifest")
}

fn query_init_plan(service_only_backend: Option<usize>) -> QueryInitPlan {
    let execution_id = query_execution_id();
    QueryInitPlan::from_manifests_for_contract_test(
        execution_id,
        (0..3).map(|backend_idx| {
            (
                backend_idx,
                manifest(
                    execution_id,
                    backend_idx,
                    service_only_backend == Some(backend_idx),
                ),
            )
        }),
    )
    .expect("fixture init plan")
}

fn registry_for(
    plan: &QueryInitPlan,
) -> (
    Arc<FrontendQueryRegistry>,
    super::super::query_registry::ActiveQueryGuard,
) {
    let registry = Arc::new(FrontendQueryRegistry::new(
        plan.execution_id()
            .query_id()
            .process_attribution()
            .expect("fixture query id has process attribution")
            .namespace(),
    ));
    let guard = registry
        .register(
            plan.execution_id().query_id(),
            DistributedQueryIntent::Result,
            Arc::new(NoopFragmentDispatcher),
        )
        .expect("register fixture query");
    (registry, guard)
}

fn config() -> FrontendQueryLifecycleConfig {
    FrontendQueryLifecycleConfig::new(
        Duration::from_millis(50),
        Duration::from_millis(150),
        Duration::from_millis(20),
        Duration::from_millis(20),
    )
    .expect("fixture lifecycle config")
}

fn observation_control(
    backend_idx: usize,
) -> (
    Arc<AttemptControl>,
    ActiveSession,
    super::manifest::MaterializedParticipant,
) {
    let plan = query_init_plan(None);
    let execution_id = plan.execution_id();
    let (transport, _) = RecordingTransport::ready(&plan);
    let (registry, _query) = registry_for(&plan);
    let materialized = super::manifest::materialize(plan).expect("materialize fixture plan");
    let participant = materialized.participants[backend_idx].clone();
    let control = AttemptControl::new(
        execution_id,
        Arc::new(transport),
        Arc::downgrade(&registry),
        config(),
        Arc::new(FrontendLifecycleMetrics::default()),
    );
    control.set_planned(&materialized.participants);
    control.set_init_attempted(&materialized.participants);
    for participant in &materialized.participants {
        control.mark_control_ready(participant.target.backend_idx());
    }
    control
        .freeze_admitted()
        .expect("fixture admits every ControlReady participant");
    let session = ActiveSession::new(
        participant.target,
        participant.digest,
        Arc::new(RecordingSession::with_events([])),
    );
    (control, session, participant)
}

fn fragment_observation(
    participant: &super::manifest::MaterializedParticipant,
    sequence: u64,
    input_rows: u64,
) -> protocol_lifecycle::FragmentLiveObservation {
    let manifest = participant.request.manifest().expect("fixture manifest");
    let execution_id = manifest.execution_id().expect("fixture execution id");
    let backend = manifest.backend().expect("fixture backend");
    let fragment = manifest
        .expected_fragment_instance_ids()
        .first()
        .copied()
        .expect("fragment participant has an expected instance");
    protocol_fragment_observation(
        execution_id,
        participant.digest,
        backend,
        fragment,
        sequence,
        input_rows,
        input_rows + 1,
        input_rows + 2,
    )
}

#[test]
fn frontend_fragment_observation_keeps_latest_sequence_and_counts_replays() {
    let (control, session, participant) = observation_control(0);
    let first = fragment_observation(&participant, 1, 10);
    let newer = fragment_observation(&participant, 2, 20);

    assert_eq!(
        control.store_fragment_observation(&session, first.clone()),
        FragmentObservationStoreOutcome::Accepted
    );
    assert_eq!(
        control.store_fragment_observation(&session, first),
        FragmentObservationStoreOutcome::Idempotent
    );
    assert_eq!(
        control.store_fragment_observation(&session, newer.clone()),
        FragmentObservationStoreOutcome::Accepted
    );
    assert_eq!(
        control.store_fragment_observation(&session, fragment_observation(&participant, 1, 10)),
        FragmentObservationStoreOutcome::Stale
    );

    let snapshot = control.fragment_observation_snapshot();
    assert_eq!(snapshot.accepted, 2);
    assert_eq!(snapshot.idempotent, 1);
    assert_eq!(snapshot.stale, 1);
    assert_eq!(snapshot.conflict, 0);
    assert_eq!(snapshot.rejected, 0);
    assert_eq!(
        snapshot.latest.get(&(0, {
            let id = newer.fragment_instance_id().expect("fixture fragment id");
            UniqueId::new(id.hi, id.lo)
        })),
        Some(&newer)
    );
}

#[test]
fn frontend_fragment_observation_rejects_conflicts_and_wrong_participants() {
    let (control, session, participant) = observation_control(0);
    let first = fragment_observation(&participant, 1, 10);
    assert_eq!(
        control.store_fragment_observation(&session, first.clone()),
        FragmentObservationStoreOutcome::Accepted
    );
    assert_eq!(
        control.store_fragment_observation(&session, fragment_observation(&participant, 1, 99)),
        FragmentObservationStoreOutcome::Conflict
    );

    let manifest = participant.request.manifest().expect("fixture manifest");
    let manifest_execution_id = manifest.execution_id().expect("fixture execution id");
    let manifest_backend = manifest.backend().expect("fixture backend");
    let unknown = protocol_fragment_observation(
        manifest_execution_id,
        participant.digest,
        manifest_backend.clone(),
        proto_id(UniqueId::new(999, 999)),
        2,
        0,
        0,
        0,
    );
    assert_eq!(
        control.store_fragment_observation(&session, unknown),
        FragmentObservationStoreOutcome::Rejected
    );
    let old_attempt = protocol_fragment_observation(
        QueryExecutionId::new(
            manifest_execution_id.query_id(),
            AttemptId::new(2).expect("fixture attempt id"),
        )
        .expect("fixture old attempt execution id"),
        participant.digest,
        manifest_backend,
        first.fragment_instance_id().expect("fixture fragment id"),
        2,
        0,
        0,
        0,
    );
    assert_eq!(
        control.store_fragment_observation(&session, old_attempt),
        FragmentObservationStoreOutcome::Rejected
    );

    let snapshot = control.fragment_observation_snapshot();
    assert_eq!(snapshot.conflict, 1);
    assert_eq!(snapshot.rejected, 2);
    assert_eq!(
        snapshot.latest.get(&(0, {
            let id = first.fragment_instance_id().expect("fixture fragment id");
            UniqueId::new(id.hi, id.lo)
        })),
        Some(&first),
        "same-sequence conflicts must retain the original sample"
    );
}

#[test]
fn frontend_terminal_snapshot_fences_later_fragment_observations() {
    let (control, session, participant) = observation_control(0);
    let before_terminal = fragment_observation(&participant, 1, 10);
    assert_eq!(
        control.store_fragment_observation(&session, before_terminal.clone()),
        FragmentObservationStoreOutcome::Accepted
    );
    let manifest = participant.request.manifest().expect("fixture manifest");
    let terminal = terminal_snapshot(
        manifest.execution_id().expect("fixture execution id"),
        manifest.backend().expect("fixture backend"),
        participant.digest,
    );
    control
        .store_terminal_outcome(terminal_outcome(terminal))
        .expect("terminal snapshot is accepted");

    assert_eq!(
        control.store_fragment_observation(&session, fragment_observation(&participant, 2, 20)),
        FragmentObservationStoreOutcome::Rejected
    );
    let snapshot = control.fragment_observation_snapshot();
    assert_eq!(snapshot.rejected, 1);
    assert_eq!(
        snapshot.latest.get(&(0, {
            let id = before_terminal
                .fragment_instance_id()
                .expect("fixture fragment id");
            UniqueId::new(id.hi, id.lo)
        })),
        Some(&before_terminal),
        "terminal state must not be overwritten by late telemetry"
    );
}

#[test]
fn frontend_negative_attestation_is_deduplicated_and_surfaces_as_terminal_input() {
    let (control, _session, participant) = observation_control(0);
    let manifest = participant.request.manifest().expect("fixture manifest");
    let outcome = negative_attestation_outcome(
        manifest.execution_id().expect("fixture execution id"),
        manifest.backend().expect("fixture backend"),
        participant.digest,
    );

    assert_eq!(
        control
            .store_terminal_outcome(outcome.clone())
            .expect("attestation is stored"),
        TerminalOutcomeStoreOutcome::Accepted
    );
    assert_eq!(
        control
            .store_terminal_outcome(outcome)
            .expect("same attestation retry is idempotent"),
        TerminalOutcomeStoreOutcome::AlreadyAccepted
    );
    let outcomes = control.terminal_outcomes_for_test();
    assert_eq!(outcomes.len(), 1);
    assert_eq!(
        outcomes[0]
            .negative_attestation()
            .expect("fixture stores negative attestation")
            .reason(),
        proto::NegativeAttestationReason::CorrectnessEvidenceRetentionExhausted,
    );
    let snapshot = ActiveQueryAttemptControl::convergence_snapshot(control.as_ref())
        .expect("stored terminal outcome retains convergence evidence");
    assert_eq!(
        snapshot.error_source,
        Some(QueryLifecycleConvergenceErrorSource::BackendAttestation)
    );
    assert_eq!(
        snapshot.runtime_filter,
        RuntimeFilterTerminalRollupSnapshot::Unavailable(
            RuntimeFilterTerminalRollupUnavailable::NegativeAttestation,
        )
    );
}

#[test]
fn frontend_query_lifecycle_config_requires_three_heartbeat_intervals() {
    let invalid = FrontendQueryLifecycleConfig::new(
        Duration::from_millis(50),
        Duration::from_millis(100),
        Duration::from_millis(20),
        Duration::from_millis(20),
    );
    assert!(invalid.is_err(), "50/100 must violate the 3x bound");

    FrontendQueryLifecycleConfig::new(
        Duration::from_millis(50),
        Duration::from_millis(150),
        Duration::from_millis(20),
        Duration::from_millis(20),
    )
    .expect("50/150 must satisfy the 3x bound");
}

#[test]
fn query_control_barrier_initializes_every_participant() {
    let plan = query_init_plan(Some(2));
    let (transport, _) = RecordingTransport::ready(&plan);
    let (registry, _query) = registry_for(&plan);
    let barrier =
        FrontendQueryLifecycleBarrier::new(Arc::new(transport.clone()), registry, config());

    let lease = barrier
        .initialize_all(plan)
        .expect("all Init and ControlReady acknowledgements precede submission eligibility");

    assert_eq!(sorted(transport.attach_targets()), vec![0, 1, 2]);
    assert_eq!(transport.init_calls().len(), 3);
    lease.finalize().expect("finalize lifecycle fixture");
}

#[test]
fn request_cancellation_before_barrier_prevents_init_fanout() {
    let plan = query_init_plan(None);
    let (transport, _) = RecordingTransport::ready(&plan);
    let (registry, _query) = registry_for(&plan);
    let cancellation = QueryCancellationSource::new();
    cancellation.request(QueryCancellationReason::ClientDisconnected);
    let barrier =
        FrontendQueryLifecycleBarrier::new(Arc::new(transport.clone()), registry, config())
            .with_cancellation(cancellation.view());

    let error = barrier
        .initialize_all(plan)
        .err()
        .expect("request cancellation must stop the lifecycle barrier");

    assert!(error.message().contains("ClientDisconnected"), "{error}");
    assert!(transport.init_calls().is_empty());
}

#[test]
fn request_cancellation_during_init_aborts_before_control_attach() {
    let plan = query_init_plan(None);
    let (transport, _) = RecordingTransport::ready(&plan);
    let (registry, _query) = registry_for(&plan);
    let cancellation = QueryCancellationSource::new();
    transport
        .state
        .lock()
        .expect("recording transport lock")
        .cancel_on_init = Some(cancellation.clone());
    let barrier =
        FrontendQueryLifecycleBarrier::new(Arc::new(transport.clone()), registry, config())
            .with_cancellation(cancellation.view());

    let error = barrier
        .initialize_all(plan)
        .err()
        .expect("cancellation observed after Init must abort the attempt");

    assert!(error.message().contains("ClientDisconnected"), "{error}");
    assert!(
        transport.attach_targets().is_empty(),
        "a cancelled request must not enter control attach"
    );
    assert_eq!(sorted(transport.abort_targets()), vec![0, 1, 2]);
}

#[derive(Clone)]
struct HeartbeatGateSession {
    gate: Arc<(Mutex<bool>, Condvar)>,
    wait_for_heartbeat_before_ready: bool,
    state: Arc<Mutex<HeartbeatGateSessionState>>,
}

#[derive(Default)]
struct HeartbeatGateSessionState {
    ready_sent: bool,
    events: VecDeque<protocol_lifecycle::QueryControlEvent>,
    commands: Vec<QueryControlCommand>,
    terminal_snapshot: Option<protocol_lifecycle::QueryTerminalSnapshot>,
}

impl HeartbeatGateSession {
    fn early(
        gate: Arc<(Mutex<bool>, Condvar)>,
        terminal_snapshot: protocol_lifecycle::QueryTerminalSnapshot,
    ) -> Self {
        Self {
            gate,
            wait_for_heartbeat_before_ready: false,
            state: Arc::new(Mutex::new(HeartbeatGateSessionState {
                events: VecDeque::new(),
                terminal_snapshot: Some(terminal_snapshot),
                ..HeartbeatGateSessionState::default()
            })),
        }
    }

    fn slow(
        gate: Arc<(Mutex<bool>, Condvar)>,
        terminal_snapshot: protocol_lifecycle::QueryTerminalSnapshot,
    ) -> Self {
        Self {
            gate,
            wait_for_heartbeat_before_ready: true,
            state: Arc::new(Mutex::new(HeartbeatGateSessionState {
                events: VecDeque::new(),
                terminal_snapshot: Some(terminal_snapshot),
                ..HeartbeatGateSessionState::default()
            })),
        }
    }

    fn heartbeat_count(&self) -> usize {
        self.state
            .lock()
            .expect("heartbeat gate state")
            .commands
            .iter()
            .filter(|command| matches!(command, QueryControlCommand::Heartbeat { .. }))
            .count()
    }
}

impl QueryControlSession for HeartbeatGateSession {
    fn send(
        &self,
        command: protocol_lifecycle::QueryControlCommand,
    ) -> Result<(), QueryLifecycleTransportError> {
        let command = query_control_command_view(&command)
            .expect("test transport accepts only validated protocol commands");
        let mut state = self.state.lock().expect("heartbeat gate state");
        match &command {
            QueryControlCommand::Heartbeat { sequence, .. } => {
                state
                    .events
                    .push_back(protocol_control_event(QueryControlEvent::HeartbeatAck {
                        sequence: *sequence,
                    }));
                let mut released = self.gate.0.lock().expect("heartbeat gate");
                *released = true;
                self.gate.1.notify_all();
            }
            QueryControlCommand::Abort { .. } => {
                state.events.push_back(protocol_control_event(
                    QueryControlEvent::TerminationAccepted {
                        reason: QueryTerminationReason::CoordinatorAbort,
                    },
                ));
            }
            QueryControlCommand::Finalize => {
                let snapshot = state
                    .terminal_snapshot
                    .clone()
                    .expect("heartbeat fixture terminal snapshot");
                state
                    .events
                    .push_back(protocol_terminal_outcome_event(terminal_outcome(snapshot)));
                state.events.push_back(protocol_control_event(
                    QueryControlEvent::TerminationAccepted {
                        reason: QueryTerminationReason::CoordinatorFinalize,
                    },
                ));
            }
            QueryControlCommand::TerminalAck => {}
        }
        state.commands.push(command);
        Ok(())
    }

    fn recv_timeout(
        &self,
        timeout: Duration,
    ) -> Result<protocol_lifecycle::QueryControlEvent, QueryLifecycleTransportError> {
        {
            let mut state = self.state.lock().expect("heartbeat gate state");
            if !state.ready_sent && !self.wait_for_heartbeat_before_ready {
                state.ready_sent = true;
                state
                    .events
                    .push_back(protocol_control_event(QueryControlEvent::LocalDrained));
                return Ok(protocol_control_event(QueryControlEvent::ControlReady));
            }
            if let Some(event) = state.events.pop_front() {
                return Ok(event);
            }
        }
        if self.wait_for_heartbeat_before_ready {
            let released = self.gate.0.lock().expect("heartbeat gate");
            let (released, _) = self
                .gate
                .1
                .wait_timeout_while(released, timeout, |released| !*released)
                .expect("heartbeat gate wait");
            if *released {
                let mut state = self.state.lock().expect("heartbeat gate state");
                if !state.ready_sent {
                    state.ready_sent = true;
                    state
                        .events
                        .push_back(protocol_control_event(QueryControlEvent::LocalDrained));
                    return Ok(protocol_control_event(QueryControlEvent::ControlReady));
                }
                if let Some(event) = state.events.pop_front() {
                    return Ok(event);
                }
            }
        }
        Err(transport_error(
            QueryLifecycleTransportErrorKind::DeadlineExceeded,
            "heartbeat gate receive timed out",
        ))
    }
}

#[test]
fn early_control_ready_session_is_heartbeated_while_other_attach_is_slow() {
    let plan = query_init_plan(None);
    let (transport, _) = RecordingTransport::ready(&plan);
    let gate = Arc::new((Mutex::new(false), Condvar::new()));
    let terminal_snapshot = |backend_idx| {
        let participant = plan.participant(backend_idx).expect("fixture participant");
        let backend = participant.backend().clone();
        terminal_snapshot(plan.execution_id(), backend, participant.digest())
    };
    let early = HeartbeatGateSession::early(Arc::clone(&gate), terminal_snapshot(0));
    let slow = HeartbeatGateSession::slow(Arc::clone(&gate), terminal_snapshot(1));
    let peer = HeartbeatGateSession::early(Arc::clone(&gate), terminal_snapshot(2));
    {
        let mut state = transport.state.lock().expect("recording transport lock");
        state.attach_results.insert(
            0,
            VecDeque::from([Ok(Arc::new(early.clone()) as Arc<dyn QueryControlSession>)]),
        );
        state.attach_results.insert(
            1,
            VecDeque::from([Ok(Arc::new(slow.clone()) as Arc<dyn QueryControlSession>)]),
        );
        state.attach_results.insert(
            2,
            VecDeque::from([Ok(Arc::new(peer) as Arc<dyn QueryControlSession>)]),
        );
    }
    let (registry, _query) = registry_for(&plan);
    let live_config = FrontendQueryLifecycleConfig::new(
        Duration::from_millis(5),
        Duration::from_millis(15),
        Duration::from_millis(20),
        Duration::from_millis(100),
    )
    .expect("heartbeat gate config");
    let barrier = FrontendQueryLifecycleBarrier::new(Arc::new(transport), registry, live_config);

    let lease = barrier
        .initialize_all(plan)
        .expect("an early ready session must retain its heartbeat lease");

    assert!(
        early.heartbeat_count() > 0,
        "the early ControlReady session was not heartbeated during the slow attach"
    );
    lease.finalize().expect("finalize heartbeat gate fixture");
}

#[test]
fn process_metrics_keep_other_active_query_when_one_terminates() {
    let first = FrontendLifecycleMetrics::process_shared();
    let second = FrontendLifecycleMetrics::process_shared();
    let baseline = first.snapshot().active_attempts;

    first.attempt_created();
    second.attempt_created();
    first.attempt_terminated();

    assert_eq!(second.snapshot().active_attempts, baseline + 1);
    second.attempt_terminated();
}

#[test]
fn frontend_query_lifecycle_pre_ready_guard_unwind_aborts_and_unbinds() {
    let plan = query_init_plan(None);
    let execution_id = plan.execution_id();
    let (transport, _) = RecordingTransport::ready(&plan);
    let (registry, _query) = registry_for(&plan);
    let materialized = super::manifest::materialize(plan).expect("materialize fixture plan");
    let metrics = Arc::new(FrontendLifecycleMetrics::default());
    let control = AttemptControl::new(
        execution_id,
        Arc::new(transport.clone()),
        Arc::downgrade(&registry),
        config(),
        Arc::clone(&metrics),
    );
    control.set_planned(&materialized.participants);
    control.set_init_attempted(&materialized.participants);
    let active: Arc<dyn ActiveQueryAttemptControl> = control.clone();
    let binding = registry
        .bind_active_attempt(execution_id, active)
        .expect("bind fixture attempt");
    let guard = PreReadyAttemptGuard::new(control, binding);
    let initialized = materialized.participants[0].clone();
    let init_transport = transport.clone();

    let unwind = std::panic::catch_unwind(std::panic::AssertUnwindSafe(move || {
        let _guard = guard;
        init_transport
            .init_query(
                initialized.target,
                initialized.request,
                Duration::from_millis(20),
            )
            .expect("first participant Init succeeds before interruption");
        panic!("deterministic interruption after a successful Init");
    }));
    assert!(unwind.is_err(), "fixture must unwind");
    assert_eq!(transport.init_calls().len(), 1);
    assert_eq!(sorted(transport.abort_targets()), vec![0, 1, 2]);

    let replacement = AttemptControl::new(
        execution_id,
        Arc::new(transport),
        Arc::downgrade(&registry),
        config(),
        metrics,
    );
    let replacement_control: Arc<dyn ActiveQueryAttemptControl> = replacement.clone();
    let replacement_binding = registry
        .bind_active_attempt(execution_id, replacement_control)
        .expect("unwind guard must clear the registry binding");
    replacement.abort_before_ready("fixture cleanup".to_string());
    drop(replacement_binding);
}

fn sorted(mut values: Vec<usize>) -> Vec<usize> {
    values.sort_unstable();
    values
}

fn wait_until(timeout: Duration, predicate: impl Fn() -> bool) {
    let deadline = Instant::now() + timeout;
    while !predicate() {
        assert!(Instant::now() < deadline, "condition did not become true");
        std::thread::sleep(Duration::from_millis(1));
    }
}

#[test]
fn frontend_query_lifecycle_all_participant_barrier_aborts_attempted_targets() {
    let plan = query_init_plan(None);
    let (transport, _) = RecordingTransport::ready(&plan);
    transport.state.lock().unwrap().attach_results.insert(
        2,
        VecDeque::from([Err(transport_error(
            QueryLifecycleTransportErrorKind::Unavailable,
            "backend 2 attach failed",
        ))]),
    );
    let (registry, _query) = registry_for(&plan);
    let barrier =
        FrontendQueryLifecycleBarrier::new(Arc::new(transport.clone()), registry, config());

    let error = match barrier.initialize_all(plan) {
        Ok(_) => panic!("one failed attach must not produce a lifecycle lease"),
        Err(error) => error,
    };

    assert!(
        error.message().contains("backend 2 attach failed"),
        "{error}"
    );
    assert_eq!(sorted(transport.abort_targets()), vec![0, 1, 2]);
}

#[test]
fn control_ready_then_peer_attach_failure_never_freezes_admitted_participants() {
    let plan = query_init_plan(None);
    let execution_id = plan.execution_id();
    let (transport, _) = RecordingTransport::ready(&plan);
    transport.state.lock().unwrap().attach_results.insert(
        1,
        VecDeque::from([Err(transport_error(
            QueryLifecycleTransportErrorKind::Unavailable,
            "backend 1 attach failed",
        ))]),
    );
    let (registry, _query) = registry_for(&plan);
    let materialized = super::manifest::materialize(plan).expect("materialize fixture plan");
    let control = AttemptControl::new(
        execution_id,
        Arc::new(transport.clone()),
        Arc::downgrade(&registry),
        config(),
        Arc::new(FrontendLifecycleMetrics::default()),
    );
    control.set_planned(&materialized.participants);
    control.set_init_attempted(&materialized.participants);

    let errors = super::barrier::attach_all(
        &transport,
        &materialized.participants,
        execution_id.attempt_id().get(),
        config(),
        &FrontendLifecycleMetrics::default(),
        &control,
    );

    assert!(
        errors
            .iter()
            .any(|error| error.contains("backend 1 attach failed")),
        "expected backend 1 attach failure: {errors:?}"
    );
    assert_eq!(control.admitted_for_test(), None);

    control.abort_before_ready("fixture cleanup".to_string());
}

#[test]
fn frontend_query_lifecycle_unknown_init_ack_retries_same_request_once() {
    let plan = query_init_plan(None);
    let retry_digest = plan.participant(1).unwrap().digest();
    let execution_id = plan.execution_id();
    let (transport, _) = RecordingTransport::ready(&plan);
    transport.state.lock().unwrap().init_results.insert(
        1,
        VecDeque::from([
            Err(transport_error(
                QueryLifecycleTransportErrorKind::DeadlineExceeded,
                "InitAck was lost",
            )),
            Ok(QueryInitAck::new(
                execution_id,
                retry_digest,
                QueryInitOutcome::AlreadyApplied,
            )),
        ]),
    );
    let (registry, _query) = registry_for(&plan);
    let barrier =
        FrontendQueryLifecycleBarrier::new(Arc::new(transport.clone()), registry, config());

    barrier
        .initialize_all(plan)
        .expect("same-digest retry must recover")
        .finalize()
        .expect("fixture finalize");

    let calls = transport.init_calls();
    let backend_one = calls
        .iter()
        .filter(|(target, _)| target.backend_idx() == 1)
        .collect::<Vec<_>>();
    assert_eq!(backend_one.len(), 2);
    assert_eq!(
        backend_one[0]
            .1
            .manifest()
            .expect("Protocol manifest")
            .execution_id(),
        backend_one[1]
            .1
            .manifest()
            .expect("Protocol manifest")
            .execution_id()
    );
    assert_eq!(backend_one[0].1.digest(), backend_one[1].1.digest());
    assert_eq!(
        calls
            .iter()
            .filter(|(target, _)| target.backend_idx() != 1)
            .count(),
        2
    );
}

#[test]
fn frontend_query_lifecycle_business_rejection_is_not_retried() {
    let plan = query_init_plan(None);
    let rejected_digest = plan.participant(1).unwrap().digest();
    let execution_id = plan.execution_id();
    let (transport, _) = RecordingTransport::ready(&plan);
    transport.state.lock().unwrap().init_results.insert(
        1,
        VecDeque::from([Ok(QueryInitAck::new(
            execution_id,
            rejected_digest,
            QueryInitOutcome::RejectedCapacity,
        ))]),
    );
    let (registry, _query) = registry_for(&plan);
    let barrier =
        FrontendQueryLifecycleBarrier::new(Arc::new(transport.clone()), registry, config());

    let error = match barrier.initialize_all(plan) {
        Ok(_) => panic!("business rejection must fail the barrier"),
        Err(error) => error,
    };

    assert!(error.message().contains("RejectedCapacity"), "{error}");
    assert_eq!(
        transport
            .init_calls()
            .iter()
            .filter(|(target, _)| target.backend_idx() == 1)
            .count(),
        1
    );
    assert_eq!(barrier.metrics_snapshot().manifest_conflicts, 0);
}

#[test]
fn frontend_query_lifecycle_manifest_conflict_is_classified() {
    let plan = query_init_plan(None);
    let digest = plan.participant(1).unwrap().digest();
    let execution_id = plan.execution_id();
    let (transport, _) = RecordingTransport::ready(&plan);
    transport.state.lock().unwrap().init_results.insert(
        1,
        VecDeque::from([Ok(QueryInitAck::new(
            execution_id,
            digest,
            QueryInitOutcome::RejectedConflict,
        ))]),
    );
    let (registry, _query) = registry_for(&plan);
    let barrier = FrontendQueryLifecycleBarrier::new(Arc::new(transport), registry, config());

    match barrier.initialize_all(plan) {
        Ok(_) => panic!("manifest conflict must fail the barrier"),
        Err(error) => assert!(error.message().contains("RejectedConflict"), "{error}"),
    }

    assert_eq!(barrier.metrics_snapshot().manifest_conflicts, 1);
}

#[test]
fn frontend_query_lifecycle_rollback_preserves_primary_error() {
    let plan = query_init_plan(None);
    let (transport, _) = RecordingTransport::ready(&plan);
    {
        let mut state = transport.state.lock().unwrap();
        state.attach_results.insert(
            2,
            VecDeque::from([Err(transport_error(
                QueryLifecycleTransportErrorKind::Unavailable,
                "primary attach failure",
            ))]),
        );
        state.abort_results.insert(
            1,
            VecDeque::from([Err(transport_error(
                QueryLifecycleTransportErrorKind::Unavailable,
                "rollback transport failure",
            ))]),
        );
    }
    let (registry, _query) = registry_for(&plan);
    let barrier =
        FrontendQueryLifecycleBarrier::new(Arc::new(transport.clone()), registry, config());

    let error = match barrier.initialize_all(plan) {
        Ok(_) => panic!("attach failure must fail the barrier"),
        Err(error) => error,
    };

    assert!(
        error
            .message()
            .starts_with("backend 2 control attach failed"),
        "{error}"
    );
    assert!(
        error.message().contains("rollback transport failure"),
        "{error}"
    );
    assert_eq!(sorted(transport.abort_targets()), vec![0, 1, 2]);
    assert_eq!(barrier.metrics_snapshot().attach_failed, 1);
}

#[test]
fn frontend_query_lifecycle_unary_fallback_accepts_first_wins_terminal_reasons() {
    let plan = query_init_plan(None);
    let execution_id = plan.execution_id();
    let (transport, sessions) = RecordingTransport::ready(&plan);
    {
        let mut state = transport.state.lock().unwrap();
        for (backend_idx, reason) in [
            (0, QueryTerminationReason::CoordinatorStreamLost),
            (1, QueryTerminationReason::CoordinatorHeartbeatTimeout),
            (2, QueryTerminationReason::LocalFailure),
        ] {
            sessions[&backend_idx].fail_next_send(transport_error(
                QueryLifecycleTransportErrorKind::StreamClosed,
                "control stream already closed after backend termination",
            ));
            state.abort_results.insert(
                backend_idx,
                VecDeque::from([Ok(QueryTerminationAck::new(execution_id, reason))]),
            );
        }
    }
    let (registry, _query) = registry_for(&plan);
    let barrier =
        FrontendQueryLifecycleBarrier::new(Arc::new(transport.clone()), registry, config());
    let lease = barrier
        .initialize_all(plan)
        .expect("all participants ready");

    let message = lease.abort_preserving("primary execution failure".to_string());

    assert_eq!(message, "primary execution failure");
    assert_eq!(sorted(transport.abort_targets()), vec![0, 1, 2]);
}

#[test]
fn frontend_query_lifecycle_unknown_init_cleanup_is_classified() {
    let plan = query_init_plan(None);
    let (transport, _) = RecordingTransport::ready(&plan);
    transport.state.lock().unwrap().init_results.insert(
        1,
        VecDeque::from([
            Err(transport_error(
                QueryLifecycleTransportErrorKind::DeadlineExceeded,
                "first InitAck outcome unknown",
            )),
            Err(transport_error(
                QueryLifecycleTransportErrorKind::StreamClosed,
                "retry InitAck outcome unknown",
            )),
        ]),
    );
    let (registry, _query) = registry_for(&plan);
    let barrier = FrontendQueryLifecycleBarrier::new(Arc::new(transport), registry, config());

    match barrier.initialize_all(plan) {
        Ok(_) => panic!("unresolved Init outcome must fail the barrier"),
        Err(error) => assert!(error.message().contains("unknown outcome"), "{error}"),
    }

    let snapshot = barrier.metrics_snapshot();
    assert_eq!(snapshot.init_failed, 1);
    assert_eq!(snapshot.init_uncertain_cleanup, 1);
}

#[test]
fn frontend_query_lifecycle_epoch_mismatch_is_classified_before_init() {
    let plan = query_init_plan(None);
    let (transport, _) = RecordingTransport::ready(&plan);
    let (registry, _query) = registry_for(&plan);
    registry.replace_live_backends(
        1,
        &[
            LiveBackendTarget::new(0, "127.0.0.1:18000".parse().unwrap(), 90),
            LiveBackendTarget::new(1, "127.0.0.1:18001".parse().unwrap(), 999),
            LiveBackendTarget::new(2, "127.0.0.1:18002".parse().unwrap(), 92),
        ],
    );
    let barrier =
        FrontendQueryLifecycleBarrier::new(Arc::new(transport.clone()), registry, config());

    match barrier.initialize_all(plan) {
        Ok(_) => panic!("stale backend generation must fail the barrier"),
        Err(error) => assert!(error.message().contains("stale"), "{error}"),
    }

    assert!(transport.init_calls().is_empty());
    assert_eq!(barrier.metrics_snapshot().backend_epoch_mismatches, 1);
}

#[test]
fn frontend_query_lifecycle_drop_cleanup_failure_is_observable() {
    let plan = query_init_plan(None);
    let (transport, sessions) = RecordingTransport::ready(&plan);
    {
        let mut state = transport.state.lock().unwrap();
        for backend_idx in 0..3 {
            sessions[&backend_idx].fail_next_send(transport_error(
                QueryLifecycleTransportErrorKind::StreamClosed,
                "drop cleanup stream unavailable",
            ));
            state.abort_results.insert(
                backend_idx,
                VecDeque::from([Err(transport_error(
                    QueryLifecycleTransportErrorKind::Unavailable,
                    "drop cleanup unary fallback unavailable",
                ))]),
            );
        }
    }
    let (registry, _query) = registry_for(&plan);
    let barrier =
        FrontendQueryLifecycleBarrier::new(Arc::new(transport.clone()), registry, config());
    let lease = barrier
        .initialize_all(plan)
        .expect("all participants ready");

    drop(lease);

    assert_eq!(sorted(transport.abort_targets()), vec![0, 1, 2]);
    assert_eq!(barrier.metrics_snapshot().cleanup_failures, 3);
}

#[test]
fn frontend_query_lifecycle_lease_drop_without_finalize_aborts_all() {
    let plan = query_init_plan(None);
    let (transport, sessions) = RecordingTransport::ready(&plan);
    let (registry, _query) = registry_for(&plan);
    let barrier =
        FrontendQueryLifecycleBarrier::new(Arc::new(transport.clone()), registry, config());

    let lease = barrier
        .initialize_all(plan)
        .expect("all participants ready");
    drop(lease);

    for session in sessions.values() {
        assert!(
            session
                .commands()
                .iter()
                .any(|command| matches!(command, QueryControlCommand::Abort { .. }))
        );
    }
}

#[test]
fn frontend_query_lifecycle_lease_finalize_sends_once() {
    let plan = query_init_plan(None);
    let (transport, sessions) = RecordingTransport::ready(&plan);
    let (registry, _query) = registry_for(&plan);
    let barrier = FrontendQueryLifecycleBarrier::new(Arc::new(transport), registry, config());

    barrier
        .initialize_all(plan)
        .expect("all participants ready")
        .finalize()
        .expect("finalize all participants");

    for session in sessions.values() {
        assert_eq!(
            session
                .commands()
                .iter()
                .filter(|command| matches!(command, QueryControlCommand::Finalize))
                .count(),
            1
        );
    }
    wait_until(Duration::from_secs(1), || {
        sessions
            .values()
            .all(|session| session.state_ref_count() == 1)
    });
}

#[test]
fn frontend_query_lifecycle_finalize_keeps_heartbeats_until_all_participants_drain() {
    let plan = query_init_plan(None);
    let (transport, sessions) = RecordingTransport::ready_without_local_drain(&plan);
    let (registry, _query) = registry_for(&plan);
    let finalize_config = config()
        .with_terminal_timeouts(Duration::from_secs(2), Duration::from_secs(2))
        .expect("terminal timeouts");
    let barrier =
        FrontendQueryLifecycleBarrier::new(Arc::new(transport), registry, finalize_config);
    let lease = barrier
        .initialize_all(plan)
        .expect("all participants ready");
    for session in sessions.values() {
        session.clear_commands();
        session.suppress_terminal_snapshot_on_finalize();
    }

    let finalize = std::thread::spawn(move || lease.finalize());
    wait_until(Duration::from_secs(1), || {
        sessions.values().all(|session| {
            session
                .commands()
                .iter()
                .any(|command| matches!(command, QueryControlCommand::Heartbeat { .. }))
        })
    });
    for session in sessions.values() {
        session.push_event(protocol_event_local_drained());
    }
    wait_until(Duration::from_secs(1), || {
        sessions.values().all(|session| {
            session
                .commands()
                .iter()
                .any(|command| matches!(command, QueryControlCommand::Finalize))
        })
    });
    for session in sessions.values() {
        session.push_protocol_event(protocol_terminal_outcome_event(terminal_outcome(
            session.terminal_snapshot(),
        )));
    }
    finalize
        .join()
        .expect("join finalize")
        .expect("finalize once every snapshot arrives");
}

#[test]
fn frontend_query_lifecycle_finalization_reports_stable_no_outcome_participants() {
    let plan = query_init_plan(None);
    let (transport, sessions) = RecordingTransport::ready(&plan);
    let execution_id = plan.execution_id();
    let (registry, _query) = registry_for(&plan);
    let finalize_config = config()
        .with_terminal_timeouts(Duration::from_millis(20), Duration::from_millis(20))
        .expect("terminal timeouts");
    let barrier = FrontendQueryLifecycleBarrier::new(
        Arc::new(transport),
        Arc::clone(&registry),
        finalize_config,
    );
    let lease = barrier
        .initialize_all(plan)
        .expect("all participants ready");
    for session in sessions.values() {
        session.suppress_terminal_snapshot_on_finalize();
    }

    let error = lease
        .finalize()
        .expect_err("missing terminal outcomes must fail");
    let message = error.message();
    assert!(
        message.contains("NoOutcome missing admitted participants"),
        "{message}"
    );
    for backend_idx in [0, 1, 2] {
        assert!(
            message.contains(&format!("backend={backend_idx}")),
            "{message}"
        );
    }
    assert!(
        message.find("backend=0") < message.find("backend=1")
            && message.find("backend=1") < message.find("backend=2"),
        "missing participants must be stable-sorted: {message}"
    );
    let snapshot = registry
        .retained_convergence_snapshot(execution_id)
        .expect("finalization failure must retain its committed decision");
    assert_eq!(
        snapshot.error_source,
        Some(QueryLifecycleConvergenceErrorSource::NoOutcome)
    );
    assert_eq!(snapshot.primary_error.as_deref(), Some(message));
}

#[test]
fn frontend_query_lifecycle_lease_duplicate_abort_is_idempotent() {
    let plan = query_init_plan(None);
    let query_id = plan.execution_id().query_id();
    let (transport, sessions) = RecordingTransport::ready(&plan);
    let (registry, _query) = registry_for(&plan);
    let barrier = FrontendQueryLifecycleBarrier::new(
        Arc::new(transport.clone()),
        Arc::clone(&registry),
        config(),
    );
    let lease = barrier
        .initialize_all(plan)
        .expect("all participants ready");

    registry
        .request_active_attempt_abort(query_id, "first abort".to_string())
        .expect("first abort request");
    registry
        .request_active_attempt_abort(query_id, "duplicate abort".to_string())
        .expect("duplicate abort request");
    drop(lease);

    for session in sessions.values() {
        assert_eq!(
            session
                .commands()
                .iter()
                .filter(|command| matches!(command, QueryControlCommand::Abort { .. }))
                .count(),
            1
        );
    }
}

#[test]
fn frontend_query_lifecycle_lease_local_failure_aborts_other_participants() {
    let plan = query_init_plan(None);
    let query_id = plan.execution_id().query_id();
    let (transport, sessions) = RecordingTransport::ready(&plan);
    sessions.get(&0).unwrap().state.0.lock().unwrap().events = VecDeque::from([
        Ok(protocol_control_event(QueryControlEvent::ControlReady)),
        Ok(protocol_control_event(QueryControlEvent::LocalFailure {
            code: "LOCAL_SCAN_FAILURE".to_string(),
            detail: "backend 0 scan failed".to_string(),
        })),
    ]);
    for backend_idx in [1, 2] {
        sessions
            .get(&backend_idx)
            .unwrap()
            .state
            .0
            .lock()
            .unwrap()
            .events = VecDeque::from([
            Ok(protocol_control_event(QueryControlEvent::ControlReady)),
            Ok(protocol_control_event(QueryControlEvent::HeartbeatAck {
                sequence: 1,
            })),
        ]);
    }
    let (registry, _query) = registry_for(&plan);
    let local_failure_config = FrontendQueryLifecycleConfig::new(
        Duration::from_millis(1),
        Duration::from_millis(20),
        Duration::from_millis(20),
        Duration::from_millis(20),
    )
    .unwrap();
    let barrier = FrontendQueryLifecycleBarrier::new(
        Arc::new(transport),
        Arc::clone(&registry),
        local_failure_config,
    );
    let lease = barrier
        .initialize_all(plan)
        .expect("all participants ready");

    wait_until(Duration::from_secs(1), || {
        registry
            .first_failure(query_id)
            .is_some_and(|failure| failure.contains("backend 0 scan failed"))
            && [1, 2].into_iter().all(|backend_idx| {
                sessions[&backend_idx]
                    .commands()
                    .iter()
                    .any(|command| matches!(command, QueryControlCommand::Abort { .. }))
            })
    });
    for backend_idx in [1, 2] {
        assert!(
            sessions[&backend_idx]
                .commands()
                .iter()
                .any(|command| matches!(command, QueryControlCommand::Abort { .. }))
        );
    }
    let snapshot = barrier.metrics_snapshot();
    assert_eq!(snapshot.local_failures, 1);
    assert_eq!(snapshot.coordinator_lost, 0);
    assert_eq!(snapshot.heartbeat_timeouts, 0);
    let error = lease
        .finalize()
        .expect_err("local failure must interrupt terminal drain");
    assert!(error.message().contains("backend 0 scan failed"), "{error}");
    assert!(
        !error
            .message()
            .contains("timed out waiting for all participants to drain"),
        "{error}"
    );
}

#[test]
fn frontend_query_lifecycle_lease_service_only_participant_joins_barrier() {
    let plan = query_init_plan(Some(2));
    let (transport, _) = RecordingTransport::ready(&plan);
    let (registry, _query) = registry_for(&plan);
    let barrier =
        FrontendQueryLifecycleBarrier::new(Arc::new(transport.clone()), registry, config());

    barrier
        .initialize_all(plan)
        .expect("service-only participant must become control ready")
        .finalize()
        .expect("fixture finalize");

    assert_eq!(sorted(transport.attach_targets()), vec![0, 1, 2]);
    let service_request = transport
        .init_calls()
        .into_iter()
        .find(|(target, _)| target.backend_idx() == 2)
        .expect("service-only InitQuery");
    assert_eq!(
        service_request
            .1
            .manifest()
            .expect("Protocol manifest")
            .roles()
            .expect("Protocol roles"),
        vec![proto::QueryParticipantRole::RuntimeFilterService]
    );
    assert!(
        service_request
            .1
            .manifest()
            .expect("Protocol manifest")
            .expected_fragment_instance_ids()
            .is_empty()
    );
}

#[test]
fn frontend_query_lifecycle_heartbeat_timeout_fails_closed() {
    let plan = query_init_plan(None);
    let query_id = plan.execution_id().query_id();
    let (transport, sessions) = RecordingTransport::ready(&plan);
    for session in sessions.values() {
        session.state.0.lock().unwrap().events =
            VecDeque::from([Ok(protocol_event_control_ready())]);
    }
    let (registry, _query) = registry_for(&plan);
    let heartbeat_config = FrontendQueryLifecycleConfig::new(
        Duration::from_millis(1),
        Duration::from_millis(5),
        Duration::from_millis(20),
        Duration::from_millis(20),
    )
    .unwrap();
    let barrier = FrontendQueryLifecycleBarrier::new(
        Arc::new(transport),
        Arc::clone(&registry),
        heartbeat_config,
    );
    let lease = barrier
        .initialize_all(plan)
        .expect("all participants ready");

    wait_until(Duration::from_secs(1), || {
        registry
            .first_failure(query_id)
            .is_some_and(|failure| failure.contains("heartbeat"))
    });
    let snapshot = barrier.metrics_snapshot();
    assert_eq!(snapshot.heartbeat_timeouts, 1);
    assert_eq!(snapshot.local_failures, 0);
    assert_eq!(snapshot.coordinator_lost, 0);
    drop(lease);
}

#[test]
fn frontend_query_lifecycle_backend_stream_loss_is_classified_as_heartbeat_timeout() {
    let plan = query_init_plan(None);
    let query_id = plan.execution_id().query_id();
    let (transport, sessions) = RecordingTransport::ready(&plan);
    sessions.get(&0).unwrap().state.0.lock().unwrap().events = VecDeque::from([
        Ok(protocol_control_event(QueryControlEvent::ControlReady)),
        Err(transport_error(
            QueryLifecycleTransportErrorKind::StreamClosed,
            "backend 0 stream closed",
        )),
    ]);
    for backend_idx in [1, 2] {
        sessions
            .get(&backend_idx)
            .unwrap()
            .state
            .0
            .lock()
            .unwrap()
            .events = VecDeque::from([
            Ok(protocol_control_event(QueryControlEvent::ControlReady)),
            Ok(protocol_control_event(QueryControlEvent::HeartbeatAck {
                sequence: 1,
            })),
        ]);
    }
    let (registry, _query) = registry_for(&plan);
    let stream_loss_config = FrontendQueryLifecycleConfig::new(
        Duration::from_millis(1),
        Duration::from_millis(5),
        Duration::from_millis(20),
        Duration::from_millis(20),
    )
    .unwrap();
    let barrier = FrontendQueryLifecycleBarrier::new(
        Arc::new(transport),
        Arc::clone(&registry),
        stream_loss_config,
    );
    let lease = barrier
        .initialize_all(plan)
        .expect("all participants ready");

    wait_until(Duration::from_secs(1), || {
        registry
            .first_failure(query_id)
            .is_some_and(|failure| failure.contains("backend 0 lost after heartbeat timeout"))
    });
    let snapshot = barrier.metrics_snapshot();
    assert_eq!(snapshot.coordinator_lost, 0);
    assert_eq!(snapshot.local_failures, 0);
    assert_eq!(snapshot.heartbeat_timeouts, 1);
    drop(lease);
}

#[test]
fn frontend_query_lifecycle_query_registry_pre_init_cancellation_blocks_fanout() {
    let plan = query_init_plan(None);
    let query_id = plan.execution_id().query_id();
    let (transport, _) = RecordingTransport::ready(&plan);
    let (registry, _query) = registry_for(&plan);
    registry
        .latch_failure_and_cancel(query_id, "client cancelled before InitQuery")
        .expect("latch pre-init cancellation");
    let barrier =
        FrontendQueryLifecycleBarrier::new(Arc::new(transport.clone()), registry, config());

    let error = match barrier.initialize_all(plan) {
        Ok(_) => panic!("pre-init cancellation must not produce a lifecycle lease"),
        Err(error) => error,
    };

    assert!(error.message().contains("client cancelled"), "{error}");
    assert!(transport.init_calls().is_empty());
}

#[test]
fn frontend_query_lifecycle_query_registry_service_only_backend_loss_aborts_attempt() {
    let plan = query_init_plan(Some(2));
    let query_id = plan.execution_id().query_id();
    let (transport, sessions) = RecordingTransport::ready(&plan);
    let (registry, _query) = registry_for(&plan);
    let barrier =
        FrontendQueryLifecycleBarrier::new(Arc::new(transport), Arc::clone(&registry), config());
    let lease = barrier
        .initialize_all(plan)
        .expect("all participants ready");

    assert_eq!(
        registry.backend_failed(2, "service-only backend unavailable".to_string()),
        vec![query_id]
    );
    for session in sessions.values() {
        assert!(
            session
                .commands()
                .iter()
                .any(|command| matches!(command, QueryControlCommand::Abort { .. }))
        );
    }
    drop(lease);
}

#[derive(Default)]
#[allow(
    dead_code,
    reason = "Retained for legacy cancellation dispatch lifecycle coverage."
)]
struct RecordingLegacyCancellationDispatcher {
    cancellations: std::sync::atomic::AtomicUsize,
}

impl FragmentDispatcher for RecordingLegacyCancellationDispatcher {
    fn fetch_result(
        &self,
        _backend_idx: usize,
        _finst_id: UniqueId,
        _max_wait_ms: i64,
        _expected_output_schema: Option<ExpectedOutputSchemaView<'_>>,
    ) -> Result<FetchOutcome, String> {
        unreachable!("cancellation test does not fetch fragments")
    }

    fn backend_count(&self) -> usize {
        3
    }
}

#[test]
#[cfg(any())]
fn query_cancel_aborts_all_participants() {
    let plan = query_init_plan(Some(2));
    let execution_id = plan.execution_id();
    let query_id = execution_id.query_id();
    let (transport, sessions) = RecordingTransport::ready(&plan);
    let dispatcher = Arc::new(RecordingLegacyCancellationDispatcher::default());
    let registry = Arc::new(FrontendQueryRegistry::new(
        query_id
            .process_attribution()
            .expect("fixture query id has process attribution")
            .namespace(),
    ));
    let _query = registry
        .register(query_id, DistributedQueryIntent::Result, dispatcher.clone())
        .expect("register cancellation fixture");
    let barrier =
        FrontendQueryLifecycleBarrier::new(Arc::new(transport), Arc::clone(&registry), config());
    let lease = barrier
        .initialize_all(plan)
        .expect("all participants become control-ready");
    let submitted = manifest(execution_id, 0, false)
        .expected_fragment_instance_ids()
        .iter()
        .next()
        .copied()
        .expect("fragment participant has one instance");
    registry
        .record_attempt(query_id, 0, submitted)
        .expect("record the only submitted fragment");
    registry
        .finish_attempt(query_id)
        .expect("finish the only submission");

    registry
        .latch_failure_and_cancel(query_id, "client requested statement cancellation")
        .expect("first cancellation wins");

    for session in sessions.values() {
        assert_eq!(
            session
                .commands()
                .iter()
                .filter(|command| matches!(command, QueryControlCommand::Abort { .. }))
                .count(),
            1,
            "every initialized participant, including service-only, receives one stream Abort"
        );
    }
    assert_eq!(
        dispatcher
            .cancellations
            .load(std::sync::atomic::Ordering::SeqCst),
        0,
        "QLC-1A cancellation must not fall back to attempted fragment cancellation"
    );
    drop(lease);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn frontend_query_lifecycle_live_transport_crosses_generated_grpc_service() {
    let ingress = Arc::new(LiveLifecycleIngress::default());
    let (endpoint, shutdown_tx, server) = spawn_frontend_live_server(ingress.clone()).await;

    let backend = LiveBackendTarget::new(7, endpoint, 77);
    let request = live_init_request(backend, 801);
    let execution_id = request
        .manifest()
        .expect("live manifest")
        .execution_id()
        .expect("id");
    let digest = request.digest().expect("digest");
    let live_manifest = request.manifest().expect("live manifest");
    let plan = QueryInitPlan::from_manifests_for_contract_test(execution_id, [(7, live_manifest)])
        .expect("live plan");
    let (registry, _query) = registry_for(&plan);
    let transport = new_query_lifecycle_transport(&[backend], frontend_data_runtime_for_test())
        .expect("production lifecycle transport");
    let live_config = FrontendQueryLifecycleConfig::new(
        Duration::from_millis(100),
        Duration::from_millis(300),
        Duration::from_secs(2),
        Duration::from_secs(2),
    )
    .expect("live lifecycle config");
    let barrier = FrontendQueryLifecycleBarrier::new(Arc::clone(&transport), registry, live_config);

    let lease = barrier
        .initialize_all(plan)
        .expect("Init and ControlReady cross the generated gRPC service");
    ingress.send_control_event(protocol_control_event(QueryControlEvent::LocalDrained));
    lease
        .finalize()
        .expect("Finalize crosses the same control stream");
    let abort_ack = transport
        .abort_query(
            QueryLifecycleTarget::new(7, endpoint, 77),
            protocol_abort_for(execution_id, digest, "idempotent cleanup"),
            Duration::from_secs(2),
        )
        .expect("AbortQuery crosses the generated gRPC service");
    assert_eq!(
        abort_ack.execution_id().expect("abort identity"),
        execution_id
    );

    assert_eq!(
        ingress
            .initialized_backend
            .lock()
            .expect("initialized backend")
            .clone(),
        Some(protocol_backend_from_live(backend))
    );
    assert!(ingress.finalized.load(std::sync::atomic::Ordering::Acquire));

    let _ = shutdown_tx.send(());
    server.await.expect("join live lifecycle server");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn frontend_query_lifecycle_live_transport_backpressures_and_surfaces_stream_reset() {
    let gate = Arc::new(LiveHeartbeatGate::default());
    let ingress = Arc::new(LiveLifecycleIngress {
        gate: Some(Arc::clone(&gate)),
        ..Default::default()
    });
    let (endpoint, shutdown_tx, server) = spawn_frontend_live_server(ingress).await;
    let backend = LiveBackendTarget::new(7, endpoint, 88);
    let target = QueryLifecycleTarget::new(7, endpoint, 88);
    let request = live_init_request(backend, 802);
    let execution_id = request
        .manifest()
        .expect("manifest")
        .execution_id()
        .expect("id");
    let digest = request.digest().expect("digest");
    let transport = new_query_lifecycle_transport(&[backend], frontend_data_runtime_for_test())
        .expect("production lifecycle transport");
    transport
        .init_query(target, request, Duration::from_secs(2))
        .expect("InitQuery");
    let session = transport
        .attach_control(
            target,
            protocol_attach_for(execution_id, digest, 9),
            Duration::from_secs(2),
        )
        .expect("attach");
    assert_eq!(
        session
            .recv_timeout(Duration::from_secs(2))
            .expect("ControlReady"),
        protocol_event_control_ready()
    );

    for sequence in 0..32 {
        session
            .send(protocol_heartbeat(sequence))
            .expect("bounded command");
    }
    let error = session
        .send(protocol_heartbeat(33))
        .expect_err("the 33rd unacknowledged command must backpressure");
    assert_eq!(error.kind(), QueryLifecycleTransportErrorKind::Backpressure);
    wait_until(Duration::from_secs(2), || {
        gate.entered.load(std::sync::atomic::Ordering::Acquire)
    });
    gate.release
        .store(true, std::sync::atomic::Ordering::Release);
    let error = session
        .recv_timeout(Duration::from_secs(2))
        .expect_err("server reset must close the stream");
    assert_eq!(error.kind(), QueryLifecycleTransportErrorKind::StreamClosed);

    let _ = shutdown_tx.send(());
    server.await.expect("join live lifecycle server");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn frontend_query_lifecycle_live_transport_closes_commands_before_terminal_is_observed() {
    let ingress = Arc::new(LiveLifecycleIngress::default());
    let (endpoint, shutdown_tx, server) = spawn_frontend_live_server(ingress.clone()).await;
    let backend = LiveBackendTarget::new(7, endpoint, 89);
    let target = QueryLifecycleTarget::new(7, endpoint, 89);
    let request = live_init_request(backend, 803);
    let execution_id = request
        .manifest()
        .expect("manifest")
        .execution_id()
        .expect("id");
    let digest = request.digest().expect("digest");
    let transport = new_query_lifecycle_transport(&[backend], frontend_data_runtime_for_test())
        .expect("production lifecycle transport");
    transport
        .init_query(target, request, Duration::from_secs(2))
        .expect("InitQuery");
    let session = transport
        .attach_control(
            target,
            protocol_attach_for(execution_id, digest, 11),
            Duration::from_secs(2),
        )
        .expect("attach");
    assert_eq!(
        session
            .recv_timeout(Duration::from_secs(2))
            .expect("ControlReady"),
        protocol_event_control_ready()
    );

    session.send(protocol_finalize()).expect("send finalize");
    let event = session
        .recv_timeout(Duration::from_secs(2))
        .expect("TerminalOutcome");
    let Some(proto::query_control_response::Event::TerminalOutcome(outcome)) =
        event.as_proto().event.as_ref()
    else {
        panic!("expected TerminalOutcome, got {event:?}");
    };
    let outcome = protocol_lifecycle::ParticipantTerminalOutcome::parse(outcome.clone())
        .expect("terminal outcome");
    assert_eq!(
        session
            .recv_timeout(Duration::from_secs(2))
            .expect("TerminationAccepted"),
        protocol_event_termination(
            proto::QueryTerminationReason::QueryTerminationCoordinatorFinalize
        )
    );
    session
        .send(protocol_terminal_ack_command(&outcome))
        .expect("TerminalAck");
    let close = session
        .recv_timeout(Duration::from_secs(2))
        .expect_err("TerminalAck must close the fake backend control stream");
    assert_eq!(close.kind(), QueryLifecycleTransportErrorKind::StreamClosed);
    let error = session
        .send(protocol_heartbeat(1))
        .expect_err("terminal observation must imply a closed command side");
    assert_eq!(error.kind(), QueryLifecycleTransportErrorKind::StreamClosed);

    let _ = shutdown_tx.send(());
    server.await.expect("join live lifecycle server");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn frontend_query_lifecycle_live_transport_ack_releases_only_its_pending_command() {
    let ingress = Arc::new(LiveLifecycleIngress {
        manual_heartbeat_acks: true,
        ..Default::default()
    });
    let (endpoint, shutdown_tx, server) = spawn_frontend_live_server(ingress.clone()).await;
    let backend = LiveBackendTarget::new(7, endpoint, 90);
    let target = QueryLifecycleTarget::new(7, endpoint, 90);
    let request = live_init_request(backend, 804);
    let execution_id = request
        .manifest()
        .expect("manifest")
        .execution_id()
        .expect("id");
    let digest = request.digest().expect("digest");
    let transport = new_query_lifecycle_transport(&[backend], frontend_data_runtime_for_test())
        .expect("production lifecycle transport");
    transport
        .init_query(target, request, Duration::from_secs(2))
        .expect("InitQuery");
    let session = transport
        .attach_control(
            target,
            protocol_attach_for(execution_id, digest, 12),
            Duration::from_secs(2),
        )
        .expect("attach");
    assert_eq!(
        session
            .recv_timeout(Duration::from_secs(2))
            .expect("ControlReady"),
        protocol_event_control_ready()
    );

    for sequence in 0..32 {
        session
            .send(protocol_heartbeat(sequence))
            .expect("fill pending command capacity");
    }
    assert_eq!(
        session
            .send(protocol_heartbeat(32))
            .expect_err("33rd pending command must backpressure")
            .kind(),
        QueryLifecycleTransportErrorKind::Backpressure
    );

    ingress.send_control_event(protocol_event_heartbeat_ack(0));
    assert_eq!(
        session
            .recv_timeout(Duration::from_secs(2))
            .expect("matching heartbeat acknowledgement"),
        protocol_event_heartbeat_ack(0)
    );
    session
        .send(protocol_heartbeat(32))
        .expect("one matching acknowledgement releases exactly one slot");
    assert_eq!(
        session
            .send(protocol_heartbeat(33))
            .expect_err("only one slot was released")
            .kind(),
        QueryLifecycleTransportErrorKind::Backpressure
    );

    ingress.send_control_event(protocol_event_heartbeat_ack(0));
    let error = session
        .recv_timeout(Duration::from_secs(2))
        .expect_err("duplicate acknowledgement must terminate the invalid stream");
    assert_eq!(
        error.kind(),
        QueryLifecycleTransportErrorKind::InvalidResponse
    );
    assert_eq!(
        session
            .send(protocol_heartbeat(33))
            .expect_err("duplicate acknowledgement must not release capacity")
            .kind(),
        QueryLifecycleTransportErrorKind::InvalidResponse
    );

    let _ = shutdown_tx.send(());
    server.await.expect("join live lifecycle server");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn frontend_query_lifecycle_live_transport_rejects_mismatched_terminal_ack() {
    let ingress = Arc::new(LiveLifecycleIngress {
        manual_terminal_acks: true,
        ..Default::default()
    });
    let (endpoint, shutdown_tx, server) = spawn_frontend_live_server(ingress.clone()).await;
    let backend = LiveBackendTarget::new(7, endpoint, 91);
    let target = QueryLifecycleTarget::new(7, endpoint, 91);
    let request = live_init_request(backend, 805);
    let execution_id = request
        .manifest()
        .expect("manifest")
        .execution_id()
        .expect("id");
    let digest = request.digest().expect("digest");
    let transport = new_query_lifecycle_transport(&[backend], frontend_data_runtime_for_test())
        .expect("production lifecycle transport");
    transport
        .init_query(target, request, Duration::from_secs(2))
        .expect("InitQuery");
    let session = transport
        .attach_control(
            target,
            protocol_attach_for(execution_id, digest, 13),
            Duration::from_secs(2),
        )
        .expect("attach");
    assert_eq!(
        session
            .recv_timeout(Duration::from_secs(2))
            .expect("ControlReady"),
        protocol_event_control_ready()
    );

    session.send(protocol_finalize()).expect("send finalize");
    ingress.send_control_event(protocol_event_termination(
        proto::QueryTerminationReason::QueryTerminationCoordinatorAbort,
    ));
    let error = session
        .recv_timeout(Duration::from_secs(2))
        .expect_err("Finalize must not accept an Abort acknowledgement");
    assert_eq!(
        error.kind(),
        QueryLifecycleTransportErrorKind::InvalidResponse
    );

    let _ = shutdown_tx.send(());
    server.await.expect("join live lifecycle server");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn frontend_query_lifecycle_live_transport_accepts_finalized_abort_replay_only() {
    let ingress = Arc::new(LiveLifecycleIngress {
        manual_terminal_acks: true,
        ..Default::default()
    });
    let (endpoint, shutdown_tx, server) = spawn_frontend_live_server(ingress.clone()).await;
    let backend = LiveBackendTarget::new(7, endpoint, 93);
    let target = QueryLifecycleTarget::new(7, endpoint, 93);
    let request = live_init_request(backend, 807);
    let execution_id = request
        .manifest()
        .expect("manifest")
        .execution_id()
        .expect("id");
    let digest = request.digest().expect("digest");
    let transport = new_query_lifecycle_transport(&[backend], frontend_data_runtime_for_test())
        .expect("production lifecycle transport");
    transport
        .init_query(target, request, Duration::from_secs(2))
        .expect("InitQuery");
    let session = transport
        .attach_control(
            target,
            protocol_attach_for(execution_id, digest, 14),
            Duration::from_secs(2),
        )
        .expect("attach");
    assert_eq!(
        session
            .recv_timeout(Duration::from_secs(2))
            .expect("ControlReady"),
        protocol_event_control_ready()
    );

    session.send(protocol_finalize()).expect("send finalize");
    ingress.send_control_event(protocol_event_termination(
        proto::QueryTerminationReason::QueryTerminationCoordinatorFinalize,
    ));
    assert_eq!(
        session
            .recv_timeout(Duration::from_secs(2))
            .expect("Finalize acknowledgement"),
        protocol_event_termination(
            proto::QueryTerminationReason::QueryTerminationCoordinatorFinalize
        )
    );

    session
        .send(protocol_abort_command("terminal fallback cleanup"))
        .expect("send abort after finalized participant");
    ingress.send_control_event(protocol_event_termination(
        proto::QueryTerminationReason::QueryTerminationCoordinatorFinalize,
    ));
    assert_eq!(
        session
            .recv_timeout(Duration::from_secs(2))
            .expect("replayed Finalize acknowledgement satisfies abort cleanup"),
        protocol_event_termination(
            proto::QueryTerminationReason::QueryTerminationCoordinatorFinalize
        )
    );

    drop(session);
    let _ = shutdown_tx.send(());
    server.await.expect("join live lifecycle server");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn frontend_query_lifecycle_live_transport_pre_submission_timeout_is_definite() {
    let ingress = Arc::new(LiveLifecycleIngress::default());
    let (endpoint, shutdown_tx, server) = spawn_frontend_live_server(ingress).await;
    let backend = LiveBackendTarget::new(7, endpoint, 92);
    let target = QueryLifecycleTarget::new(7, endpoint, 92);
    let request = live_init_request(backend, 806);
    let transport = new_query_lifecycle_transport(&[backend], frontend_data_runtime_for_test())
        .expect("production lifecycle transport");

    let error = transport
        .init_query(target, request, Duration::ZERO)
        .expect_err("channel acquisition deadline is a definite pre-submission failure");
    assert_eq!(error.kind(), QueryLifecycleTransportErrorKind::Unavailable);
    assert!(!error.is_unknown_init_outcome());

    let _ = shutdown_tx.send(());
    server.await.expect("join live lifecycle server");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn frontend_query_lifecycle_live_transport_post_submission_timeout_is_unknown() {
    let ingress = Arc::new(LiveLifecycleIngress::default());
    let (endpoint, shutdown_tx, server) = spawn_frontend_live_server(ingress.clone()).await;
    let backend = LiveBackendTarget::new(7, endpoint, 93);
    let target = QueryLifecycleTarget::new(7, endpoint, 93);
    let transport = new_query_lifecycle_transport(&[backend], frontend_data_runtime_for_test())
        .expect("production lifecycle transport");

    transport
        .init_query(
            target,
            live_init_request(backend, 807),
            Duration::from_secs(2),
        )
        .expect("warm the channel before the delayed request");
    *ingress.init_delay.lock().expect("init delay") = Some(Duration::from_millis(100));
    let error = transport
        .init_query(
            target,
            live_init_request(backend, 808),
            Duration::from_millis(20),
        )
        .expect_err("submitted InitQuery must time out while the server is handling it");
    assert!(matches!(
        error.kind(),
        QueryLifecycleTransportErrorKind::DeadlineExceeded
            | QueryLifecycleTransportErrorKind::StreamClosed
    ));
    assert!(error.is_unknown_init_outcome());

    let _ = shutdown_tx.send(());
    server.await.expect("join live lifecycle server");
}

fn live_init_request(
    backend: LiveBackendTarget,
    finst_high: i64,
) -> protocol_lifecycle::QueryInitRequest {
    let execution_id = query_execution_id();
    QueryInitRequest::from_manifest(
        ParticipantManifest::new(
            execution_id,
            protocol_backend_from_live(backend),
            [ParticipantRole::FragmentExecutor],
            [proto_id(UniqueId::new(finst_high, 1))],
            QueryOptions::parse(proto::QueryOptions::default()).expect("fixture query options"),
            1_900_000_000_000,
            [],
            None,
            Duration::from_secs(30),
            QueryControlEndpoint::new("127.0.0.1", 19_000).expect("report endpoint"),
        )
        .expect("live manifest"),
    )
}

/// Test-only BE-shaped wire peer implemented with Frontend's generated stub.
/// It exercises the same client codec, paths, and bidirectional lifecycle
/// framing without recovering Core's former generic gRPC service.
struct FrontendLifecycleWireService {
    ingress: Arc<dyn QueryLifecycleIngress>,
}

type EmptyExchangeStream =
    Pin<Box<dyn tokio_stream::Stream<Item = Result<proto::ExchangeResponse, Status>> + Send>>;
type LifecycleResponseStream = ReceiverStream<Result<proto::QueryControlResponse, Status>>;

impl FrontendLifecycleWireService {
    fn rejected(rpc: &str) -> Status {
        Status::failed_precondition(format!("lifecycle wire test peer rejects {rpc}"))
    }

    fn status(error: QueryLifecycleError) -> Status {
        match error.code() {
            QueryLifecycleErrorCode::InvalidManifest => Status::invalid_argument(error.detail()),
            QueryLifecycleErrorCode::Conflict => Status::already_exists(error.detail()),
            QueryLifecycleErrorCode::StaleBackend | QueryLifecycleErrorCode::Terminated => {
                Status::failed_precondition(error.detail())
            }
            QueryLifecycleErrorCode::Capacity => Status::resource_exhausted(error.detail()),
            QueryLifecycleErrorCode::Transport => Status::unavailable(error.detail()),
            QueryLifecycleErrorCode::Internal => Status::internal(error.detail()),
        }
    }
}

#[tonic::async_trait]
impl crate::native::generated::nova_rocks_grpc_server::NovaRocksGrpc
    for FrontendLifecycleWireService
{
    type ExchangeStream = EmptyExchangeStream;
    type QueryControlStreamStream = LifecycleResponseStream;

    async fn exchange(
        &self,
        _request: Request<tonic::Streaming<proto::ExchangeRequest>>,
    ) -> Result<Response<Self::ExchangeStream>, Status> {
        Err(Self::rejected("Exchange"))
    }

    async fn exchange_unary(
        &self,
        _request: Request<proto::ExchangeRequest>,
    ) -> Result<Response<proto::ExchangeResponse>, Status> {
        Err(Self::rejected("ExchangeUnary"))
    }

    async fn transmit_runtime_filter_envelope(
        &self,
        _request: Request<filter::RuntimeFilterEnvelope>,
    ) -> Result<Response<filter::RuntimeFilterEnvelopeResponse>, Status> {
        Err(Self::rejected("TransmitRuntimeFilterEnvelope"))
    }

    async fn lookup(
        &self,
        _request: Request<filter::LookupRequest>,
    ) -> Result<Response<filter::LookupResponse>, Status> {
        Err(Self::rejected("Lookup"))
    }

    async fn fetch_result(
        &self,
        _request: Request<proto::FetchResultRequest>,
    ) -> Result<Response<proto::FetchResultResponse>, Status> {
        Err(Self::rejected("FetchResult"))
    }

    async fn init_query(
        &self,
        request: Request<proto::InitQueryRequest>,
    ) -> Result<Response<proto::InitQueryResponse>, Status> {
        let request = protocol_lifecycle::QueryInitRequest::parse(request.into_inner())
            .map_err(|error| Status::invalid_argument(error.to_string()))?;
        Ok(Response::new(
            self.ingress.init_query(request).as_proto().clone(),
        ))
    }

    async fn stage_fragments(
        &self,
        request: Request<proto::StageFragmentsRequest>,
    ) -> Result<Response<proto::StageFragmentsResponse>, Status> {
        let request = protocol_lifecycle::QueryStageRequest::parse(request.into_inner())
            .map_err(|error| Status::invalid_argument(error.to_string()))?;
        Ok(Response::new(
            self.ingress.stage_fragments(request).as_proto().clone(),
        ))
    }

    async fn start_prepared_query(
        &self,
        request: Request<proto::StartPreparedQueryRequest>,
    ) -> Result<Response<proto::StartPreparedQueryResponse>, Status> {
        let request = protocol_lifecycle::QueryStartRequest::parse(request.into_inner())
            .map_err(|error| Status::invalid_argument(error.to_string()))?;
        Ok(Response::new(
            self.ingress
                .start_prepared_query(request)
                .as_proto()
                .clone(),
        ))
    }

    async fn abort_query(
        &self,
        request: Request<proto::AbortQueryRequest>,
    ) -> Result<Response<proto::AbortQueryResponse>, Status> {
        let request = protocol_lifecycle::QueryAbortRequest::parse(request.into_inner())
            .map_err(|error| Status::invalid_argument(error.to_string()))?;
        let response = self.ingress.abort_query(request).map_err(Self::status)?;
        Ok(Response::new(*response.as_proto()))
    }

    async fn query_control_stream(
        &self,
        request: Request<tonic::Streaming<proto::QueryControlRequest>>,
    ) -> Result<Response<Self::QueryControlStreamStream>, Status> {
        let mut inbound = request.into_inner();
        let first = inbound
            .message()
            .await
            .map_err(|error| Status::invalid_argument(format!("read attach frame: {error}")))?
            .ok_or_else(|| Status::failed_precondition("first frame must be Attach"))?;
        let Some(proto::query_control_request::Command::Attach(attach)) = first.command else {
            return Err(Status::failed_precondition("first frame must be Attach"));
        };
        let attach = protocol_lifecycle::QueryControlAttach::parse(attach)
            .map_err(|error| Status::invalid_argument(error.to_string()))?;
        let attachment = self.ingress.attach_control(attach).map_err(Self::status)?;
        let (outbound, receiver) = tokio::sync::mpsc::channel(32);
        tokio::spawn(async move {
            let control = attachment.control;
            let mut events = attachment.events;
            loop {
                tokio::select! {
                    inbound_message = inbound.message() => {
                        let request = match inbound_message {
                            Ok(Some(request)) => request,
                            Ok(None) => break,
                            Err(error) => { let _ = outbound.send(Err(Status::invalid_argument(format!("read query control command: {error}")))).await; break; }
                        };
                        let command = match protocol_lifecycle::QueryControlCommand::parse(request) {
                            Ok(command) => command,
                            Err(error) => { let _ = outbound.send(Err(Status::invalid_argument(error.to_string()))).await; break; }
                        };
                        let result = match command.as_proto().command.as_ref() {
                            Some(proto::query_control_request::Command::Heartbeat(heartbeat)) => control.heartbeat(heartbeat.sequence),
                            Some(proto::query_control_request::Command::Abort(abort)) => control.abort(abort.reason.clone()),
                            Some(proto::query_control_request::Command::Finalize(_)) => control.finalize(),
                            Some(proto::query_control_request::Command::TerminalAck(ack)) => match protocol_lifecycle::QueryTerminalAck::parse(ack.clone()) {
                                Ok(ack) => control.terminal_ack(ack),
                                Err(error) => Err(QueryLifecycleError::new(QueryLifecycleErrorCode::InvalidManifest, error.to_string())),
                            },
                            Some(proto::query_control_request::Command::Attach(_)) | None => Err(QueryLifecycleError::new(QueryLifecycleErrorCode::InvalidManifest, "invalid active control command")),
                        };
                        if let Err(error) = result { let _ = outbound.send(Err(Self::status(error))).await; break; }
                    }
                    event = events.recv() => match event {
                        Some(event) if outbound.send(Ok(event.as_proto().clone())).await.is_ok() => {}
                        _ => break,
                    },
                }
            }
        });
        Ok(Response::new(ReceiverStream::new(receiver)))
    }

    async fn report_query_terminal(
        &self,
        _request: Request<proto::ReportQueryTerminalRequest>,
    ) -> Result<Response<proto::ReportQueryTerminalResponse>, Status> {
        Err(Self::rejected("ReportQueryTerminal"))
    }

    async fn ensure_connector_execution_binding(
        &self,
        _request: Request<proto::EnsureConnectorExecutionBindingRequest>,
    ) -> Result<Response<proto::EnsureConnectorExecutionBindingResponse>, Status> {
        Err(Self::rejected("EnsureConnectorExecutionBinding"))
    }

    async fn retire_connector_execution_binding(
        &self,
        _request: Request<proto::RetireConnectorExecutionBindingRequest>,
    ) -> Result<Response<proto::RetireConnectorExecutionBindingResponse>, Status> {
        Err(Self::rejected("RetireConnectorExecutionBinding"))
    }

    async fn heartbeat(
        &self,
        _request: Request<proto::HeartbeatRequest>,
    ) -> Result<Response<proto::HeartbeatResponse>, Status> {
        Err(Self::rejected("Heartbeat"))
    }
}

async fn spawn_frontend_live_server(
    ingress: Arc<dyn QueryLifecycleIngress>,
) -> (
    std::net::SocketAddr,
    tokio::sync::oneshot::Sender<()>,
    tokio::task::JoinHandle<()>,
) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind live lifecycle server");
    let endpoint = listener.local_addr().expect("live lifecycle endpoint");
    let incoming = futures::stream::unfold(listener, |listener| async {
        let item = listener.accept().await.map(|(stream, _)| stream);
        Some((item, listener))
    });
    let service = FrontendLifecycleWireService { ingress };
    let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel();
    let server = tokio::spawn(async move {
        tonic::transport::Server::builder()
            .add_service(
                crate::native::generated::nova_rocks_grpc_server::NovaRocksGrpcServer::new(service),
            )
            .serve_with_incoming_shutdown(incoming, async {
                let _ = shutdown_rx.await;
            })
            .await
            .expect("serve live lifecycle server");
    });
    (endpoint, shutdown_tx, server)
}

#[derive(Default)]
struct LiveLifecycleIngress {
    initialized: Mutex<
        Option<(
            QueryExecutionId,
            protocol_lifecycle::ParticipantManifestDigest,
        )>,
    >,
    initialized_backend: Mutex<Option<ParticipantBackendIdentity>>,
    finalized: Arc<std::sync::atomic::AtomicBool>,
    gate: Option<Arc<LiveHeartbeatGate>>,
    manual_heartbeat_acks: bool,
    manual_terminal_acks: bool,
    init_delay: Mutex<Option<Duration>>,
    control_events: Mutex<Option<tokio::sync::mpsc::Sender<protocol_lifecycle::QueryControlEvent>>>,
}

impl LiveLifecycleIngress {
    fn send_control_event(&self, event: protocol_lifecycle::QueryControlEvent) {
        self.control_events
            .lock()
            .expect("control events")
            .as_ref()
            .expect("attached control stream")
            .try_send(event)
            .expect("inject control event");
    }
}

impl QueryLifecycleIngress for LiveLifecycleIngress {
    fn bind_backend_identity(&self, _backend_id: u64) -> Result<(), QueryLifecycleError> {
        Ok(())
    }

    fn init_query(
        &self,
        request: protocol_lifecycle::QueryInitRequest,
    ) -> protocol_lifecycle::QueryInitAck {
        if let Some(delay) = *self.init_delay.lock().expect("init delay") {
            std::thread::sleep(delay);
        }
        let manifest = request
            .manifest()
            .map_err(|error| {
                QueryLifecycleError::new(
                    QueryLifecycleErrorCode::InvalidManifest,
                    error.to_string(),
                )
            })
            .expect("validated Protocol InitQuery carries manifest");
        let execution_id = manifest.execution_id().expect("validated execution id");
        let digest = request.digest().expect("validated digest");
        *self
            .initialized_backend
            .lock()
            .expect("initialized backend") = Some(manifest.backend().expect("validated backend"));
        *self.initialized.lock().expect("initialized") = Some((execution_id, digest));
        QueryInitAck::new(execution_id, digest, QueryInitOutcome::Applied)
    }

    fn abort_query(
        &self,
        request: protocol_lifecycle::QueryAbortRequest,
    ) -> Result<protocol_lifecycle::QueryTerminationAck, QueryLifecycleError> {
        Ok(QueryTerminationAck::new(
            request.execution_id().expect("validated abort request id"),
            QueryTerminationReason::CoordinatorAbort,
        ))
    }

    fn attach_control(
        &self,
        attach: protocol_lifecycle::QueryControlAttach,
    ) -> Result<QueryControlAttachment, QueryLifecycleError> {
        let execution_id = attach.execution_id().map_err(|error| {
            QueryLifecycleError::new(QueryLifecycleErrorCode::InvalidManifest, error.to_string())
        })?;
        let digest = attach.digest().map_err(|error| {
            QueryLifecycleError::new(QueryLifecycleErrorCode::InvalidManifest, error.to_string())
        })?;
        if *self.initialized.lock().expect("initialized") != Some((execution_id, digest)) {
            return Err(QueryLifecycleError::new(
                QueryLifecycleErrorCode::Conflict,
                "attach identity or digest mismatch",
            ));
        }
        let (events, receiver) = tokio::sync::mpsc::channel(32);
        let (_observation_events, observations) = tokio::sync::watch::channel(None);
        events
            .try_send(protocol_event_control_ready())
            .expect("ControlReady");
        *self.control_events.lock().expect("control events") = Some(events.clone());
        Ok(QueryControlAttachment {
            control: Arc::new(LiveBackendControl {
                events,
                finalized: Arc::clone(&self.finalized),
                gate: self.gate.clone(),
                manual_heartbeat_acks: self.manual_heartbeat_acks,
                manual_terminal_acks: self.manual_terminal_acks,
                execution_id,
                backend: self
                    .initialized_backend
                    .lock()
                    .expect("initialized backend")
                    .clone()
                    .expect("InitQuery precedes attach"),
                digest,
            }),
            events: receiver,
            observations,
        })
    }
}

struct LiveBackendControl {
    events: tokio::sync::mpsc::Sender<protocol_lifecycle::QueryControlEvent>,
    finalized: Arc<std::sync::atomic::AtomicBool>,
    gate: Option<Arc<LiveHeartbeatGate>>,
    manual_heartbeat_acks: bool,
    manual_terminal_acks: bool,
    execution_id: QueryExecutionId,
    backend: ParticipantBackendIdentity,
    digest: protocol_lifecycle::ParticipantManifestDigest,
}

impl BackendQueryControl for LiveBackendControl {
    fn heartbeat(&self, sequence: u64) -> Result<(), QueryLifecycleError> {
        if let Some(gate) = &self.gate {
            gate.entered
                .store(true, std::sync::atomic::Ordering::Release);
            while !gate.release.load(std::sync::atomic::Ordering::Acquire) {
                std::thread::yield_now();
            }
            return Err(QueryLifecycleError::new(
                QueryLifecycleErrorCode::Transport,
                "reset live test stream",
            ));
        }
        if self.manual_heartbeat_acks {
            return Ok(());
        }
        self.events
            .try_send(protocol_event_heartbeat_ack(sequence))
            .map_err(live_control_error)
    }

    fn abort(&self, _reason: String) -> Result<(), QueryLifecycleError> {
        if self.manual_terminal_acks {
            return Ok(());
        }
        self.events
            .try_send(protocol_terminal_outcome_event(terminal_outcome(
                terminal_snapshot(self.execution_id, self.backend.clone(), self.digest),
            )))
            .map_err(live_control_error)?;
        self.events
            .try_send(protocol_event_termination(
                QueryTerminationReason::CoordinatorAbort,
            ))
            .map_err(live_control_error)
    }

    fn finalize(&self) -> Result<(), QueryLifecycleError> {
        self.finalized
            .store(true, std::sync::atomic::Ordering::Release);
        if self.manual_terminal_acks {
            return Ok(());
        }
        self.events
            .try_send(protocol_terminal_outcome_event(terminal_outcome(
                terminal_snapshot(self.execution_id, self.backend.clone(), self.digest),
            )))
            .map_err(live_control_error)?;
        self.events
            .try_send(protocol_event_termination(
                QueryTerminationReason::CoordinatorFinalize,
            ))
            .map_err(live_control_error)
    }

    fn coordinator_lost(&self, _reason: QueryTerminationReason) -> Result<(), QueryLifecycleError> {
        Ok(())
    }
}

#[derive(Default)]
struct LiveHeartbeatGate {
    entered: std::sync::atomic::AtomicBool,
    release: std::sync::atomic::AtomicBool,
}

fn live_control_error(
    error: tokio::sync::mpsc::error::TrySendError<protocol_lifecycle::QueryControlEvent>,
) -> QueryLifecycleError {
    QueryLifecycleError::new(QueryLifecycleErrorCode::Internal, error.to_string())
}
