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

#[cfg(test)]
use std::collections::VecDeque;
use std::collections::{BTreeMap, BTreeSet};
#[cfg(test)]
use std::net::SocketAddr;
use std::num::NonZeroUsize;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, AtomicU16, AtomicU64, Ordering};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use crate::common::backend_topology::{
    BackendTopologyPort, BackendTopologySnapshot, BackendTopologyValidationError, LiveBackendTarget,
};
use crate::native::fragment_transport::{
    FetchOutcome, FinalTaskInfoRead, FragmentDispatcher, NativeTaskResultTransport,
    RootResultOutcome, TaskResultTransport,
};
use crate::query_execution::artifact::{
    PreparedDistributedQuery, RunningNativeExecutionParts,
    RuntimeFilterDeploymentReadyDistributedQuery, ValidatedFragmentSchedule,
    ValidatedNativeSubmission,
};
use crate::query_execution::completion::{PreReadyRetryBoundary, QueryAttemptReservation};
use crate::query_execution::contract::{
    DistributedQueryCoordinator, DistributedQueryError, DistributedQueryErrorKind,
    DistributedQueryIntent, DistributedQueryOutcome, DistributedQueryRequest,
    PreReadyTopologyOutcome, ProfileTerminalBuilder,
};
#[cfg(test)]
use crate::query_execution::lifecycle_plan::QueryLifecycleTarget;
use crate::query_execution::lifecycle_plan::{
    QueryCredentialLeases, QueryInitOptions, QueryLifecycleLease,
};
#[cfg(test)]
use crate::query_execution::split_assignment::DEFAULT_INITIAL_DYNAMIC_FILTER_WAIT_CAP;
use crate::query_execution::split_assignment::{RoundSplitSource, TaskUpdateTransport};
use crate::runtime::statement_result::StatementResult;
use crate::task_execution::sources::AttemptEstablishFacts;
use novarocks_proto_codec::lifecycle::QueryOptions as ProtocolQueryOptions;
use novarocks_types::identity::{BackendProcessId, FrontendProcessId, TaskId};
use novarocks_types::{
    AttemptId, LocalQuerySequence, NativeCompatibilityId, QueryExecutionId, QueryId,
    QueryIdAttribution, QueryProcessNamespace,
};

use super::query_lifecycle::{
    FrontendQueryLifecycleBarrier, FrontendQueryLifecycleConfig, QueryLifecycleTransport,
};
#[cfg(test)]
use super::query_lifecycle::{
    QueryControlSession, QueryLifecycleTransportError, QueryLifecycleTransportErrorKind,
};
use super::query_registry::{FrontendQueryRegistry, QueryLifecycleConvergenceReader};
use super::report::FrontendCoordinatorTerminalIngress;
use super::scheduler::{FrontendBackendSnapshot, FrontendFragmentScheduler};
use super::split_assignment_round::{
    RoundSplitAssignmentPlan, SplitAssignmentRoundGuard, assignment_endpoints, assignment_targets,
};
use super::task_round::{
    AssembledRound, AttemptPumps, AttemptTransport, assemble_round, install_attempt_pumps,
};
use crate::metrics::{
    observe_pre_ready_replan, observe_waiting_for_backend, record_pre_ready_effect_gate,
    record_pre_ready_replan,
};
use crate::native::data_runtime::FrontendDataRuntime;
use crate::native::fragment_encoder::instance::encode_query_options;
use crate::native::fragment_encoder::submission::encode_native_submission;
use crate::native::task_transport::AttemptWireFacts;
use crate::native::transport::{
    GrpcTaskUpdateTransport, new_fragment_dispatcher, new_query_lifecycle_transport,
};
use crate::runtime_filter::compiler::{
    FrontendRuntimeFilterDeploymentCompilerConfig, compile_scheduled_runtime_filter_deployment,
};
use crate::runtime_filter::feedback::RuntimeFilterFeedbackState;
use crate::runtime_filter::plan_encoder::encode_binding_attachment;
use crate::task_execution::completion::{WriteCompletionTracker, accept_final_info};
use crate::task_execution::error::TaskExecutionError;
use crate::task_execution::feedback_pump::TaskDynamicFilterReads;
use crate::task_execution::graph::TaskNode;
use crate::task_execution::remote_task::RemoteTaskState;
use crate::task_execution::round::{TaskRound, TurnReport};
use crate::task_execution::split_transport::SplitDeliveryBridge;
use crate::task_execution::status_intake::{CondvarWake, StatusIntakeWake};
use novarocks_execution::task_execution::{
    AbortCause, FinalTaskInfo, MaxWait, OperationKind, TaskIdentity,
};
#[cfg(test)]
use novarocks_proto_codec::lifecycle::{
    QueryAbortRequest, QueryControlAttach, QueryControlCommand, QueryControlEvent, QueryInitAck,
    QueryInitOutcome, QueryInitRequest, QueryStageAck, QueryStageOutcome, QueryStageRequest,
    QueryStartAck, QueryStartOutcome, QueryStartRequest, QueryTerminationAck,
    QueryTerminationReason, StageDigest,
};
#[cfg(test)]
use novarocks_proto_models::novarocks as protocol;

trait QueryIdSource: Send + Sync + 'static {
    fn next_query_id(&self) -> Result<QueryId, DistributedQueryError>;
}

struct UniqueQueryIdSource {
    namespace: QueryProcessNamespace,
    last_issued_sequence: AtomicU64,
}

impl Default for UniqueQueryIdSource {
    fn default() -> Self {
        let (namespace, _) = uuid::Uuid::new_v4().as_u64_pair();
        Self::new(QueryProcessNamespace::new(namespace))
    }
}

impl UniqueQueryIdSource {
    fn new(namespace: QueryProcessNamespace) -> Self {
        Self {
            namespace,
            last_issued_sequence: AtomicU64::new(0),
        }
    }

    #[cfg(test)]
    fn with_last_issued_sequence(namespace: QueryProcessNamespace, last_issued: u64) -> Self {
        Self {
            namespace,
            last_issued_sequence: AtomicU64::new(last_issued),
        }
    }

    fn namespace(&self) -> QueryProcessNamespace {
        self.namespace
    }
}

impl QueryIdSource for UniqueQueryIdSource {
    fn next_query_id(&self) -> Result<QueryId, DistributedQueryError> {
        let last_issued = self
            .last_issued_sequence
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |current| {
                current
                    .checked_add(1)
                    .filter(|next| *next <= i64::MAX as u64)
            })
            .map_err(|_| {
                DistributedQueryError::new(
                    DistributedQueryErrorKind::Failed,
                    "frontend query id local sequence is exhausted",
                )
            })?;
        let sequence = LocalQuerySequence::new(
            last_issued
                .checked_add(1)
                .expect("successful query id allocation increments the sequence"),
        )
        .expect("successful query id allocation produces a nonzero sequence");
        Ok(QueryIdAttribution::new(self.namespace, sequence).into_query_id())
    }
}

#[cfg(test)]
#[allow(
    dead_code,
    reason = "Shared coordinator test fixture provides a deterministic query id source."
)]
struct FixedQueryIdSource(QueryId);

#[cfg(test)]
impl QueryIdSource for FixedQueryIdSource {
    fn next_query_id(&self) -> Result<QueryId, DistributedQueryError> {
        Ok(self.0)
    }
}

#[allow(
    dead_code,
    reason = "Retained as the coordinator-owned live topology test fixture."
)]
pub(crate) struct FrontendLiveBackendTopology {
    state: Mutex<FrontendLiveBackendTopologyState>,
}

#[allow(
    dead_code,
    reason = "State is retained solely by the coordinator-owned live topology test fixture."
)]
struct FrontendLiveBackendTopologyState {
    revision: u64,
    live: Vec<LiveBackendTarget>,
}

#[allow(
    dead_code,
    reason = "Retained as the coordinator-owned live topology test fixture API."
)]
impl FrontendLiveBackendTopology {
    pub(crate) fn new() -> Self {
        Self {
            state: Mutex::new(FrontendLiveBackendTopologyState {
                revision: 0,
                live: Vec::new(),
            }),
        }
    }

    fn snapshot(&self) -> Vec<LiveBackendTarget> {
        self.state
            .lock()
            .expect("frontend live backend topology lock")
            .live
            .clone()
    }

    pub(crate) fn replace(&self, revision: u64, live: Vec<LiveBackendTarget>) {
        let mut state = self
            .state
            .lock()
            .expect("frontend live backend topology lock");
        if revision >= state.revision {
            state.revision = revision;
            state.live = live;
        }
    }
}

struct FrontendReportEndpointBinding {
    advertised_host: String,
    configured_port: u16,
    bound_port: AtomicU16,
}

impl FrontendReportEndpointBinding {
    fn new(advertised_host: String, configured_port: u16) -> Self {
        Self {
            advertised_host,
            configured_port,
            bound_port: AtomicU16::new(0),
        }
    }

    #[cfg(test)]
    #[allow(
        dead_code,
        reason = "Coordinator test fixture builds the report endpoint from a socket address."
    )]
    fn from_socket_addr(endpoint: SocketAddr) -> Self {
        Self::new(endpoint.ip().to_string(), endpoint.port())
    }

    fn resolve(
        &self,
    ) -> Result<crate::common::backend_topology::CoordinatorReportEndpoint, DistributedQueryError>
    {
        let port = if self.configured_port == 0 {
            let bound = self.bound_port.load(Ordering::Acquire);
            if bound == 0 {
                return Err(failed(
                    "frontend coordinator report endpoint is not bound yet",
                ));
            }
            bound
        } else {
            self.configured_port
        };
        crate::common::backend_topology::CoordinatorReportEndpoint::new(
            self.advertised_host.clone(),
            port,
        )
        .map_err(failed)
    }
}

impl crate::common::backend_topology::CoordinatorReportEndpointSink
    for FrontendReportEndpointBinding
{
    fn set_bound_port(&self, port: u16) {
        self.bound_port.store(port, Ordering::Release);
    }
}

#[cfg(test)]
#[allow(
    dead_code,
    reason = "Coordinator test fixture models fixed and sequenced backend services."
)]
enum BackendServicesSource {
    Fixed {
        scheduler: FrontendFragmentScheduler,
        dispatcher: Arc<dyn FragmentDispatcher>,
        lifecycle_transport: Arc<dyn QueryLifecycleTransport>,
    },
    #[cfg(test)]
    Sequence {
        schedulers: Mutex<VecDeque<FrontendFragmentScheduler>>,
        dispatcher: Arc<dyn FragmentDispatcher>,
        lifecycle_transport: Arc<dyn QueryLifecycleTransport>,
    },
}

struct QueryBackendServices {
    scheduler: FrontendFragmentScheduler,
    dispatcher: Arc<dyn FragmentDispatcher>,
    lifecycle_transport: Arc<dyn QueryLifecycleTransport>,
    live_backends: Vec<LiveBackendTarget>,
}

#[cfg(test)]
#[allow(
    dead_code,
    reason = "Coordinator test fixture provides an immediately ready lifecycle transport."
)]
pub(crate) fn ready_lifecycle_transport_for_test() -> Arc<dyn QueryLifecycleTransport> {
    Arc::new(ReadyLifecycleTransportForTest)
}

#[cfg(test)]
#[allow(
    dead_code,
    reason = "Coordinator test fixture provides the ready lifecycle transport implementation."
)]
struct ReadyLifecycleTransportForTest;

#[cfg(test)]
#[allow(
    dead_code,
    reason = "Coordinator test fixture stores ready control events for lifecycle assertions."
)]
struct ReadyLifecycleSessionForTest {
    events: Mutex<VecDeque<QueryControlEvent>>,
}

#[cfg(test)]
impl QueryControlSession for ReadyLifecycleSessionForTest {
    fn send(&self, command: QueryControlCommand) -> Result<(), QueryLifecycleTransportError> {
        use protocol::query_control_request::Command;
        use protocol::query_control_response::Event;

        let event = match command.as_proto().command.as_ref() {
            Some(Command::Heartbeat(heartbeat)) => {
                QueryControlEvent::parse(protocol::QueryControlResponse {
                    event: Some(Event::HeartbeatAck(protocol::QueryControlHeartbeatAck {
                        sequence: heartbeat.sequence,
                    })),
                })
            }
            Some(Command::Abort(_)) => QueryControlEvent::parse(protocol::QueryControlResponse {
                event: Some(Event::TerminationAccepted(
                    protocol::QueryControlTerminationAccepted {
                        reason: QueryTerminationReason::QueryTerminationCoordinatorAbort as i32,
                    },
                )),
            }),
            Some(Command::Finalize(_)) => {
                QueryControlEvent::parse(protocol::QueryControlResponse {
                    event: Some(Event::TerminationAccepted(
                        protocol::QueryControlTerminationAccepted {
                            reason: QueryTerminationReason::QueryTerminationCoordinatorFinalize
                                as i32,
                        },
                    )),
                })
            }
            Some(Command::TerminalAck(_)) => return Ok(()),
            Some(Command::CredentialLeasePrepare(prepare)) => {
                let envelope = prepare
                    .envelope
                    .as_ref()
                    .expect("validated credential lease prepare envelope");
                QueryControlEvent::parse(protocol::QueryControlResponse {
                    event: Some(Event::CredentialLeasePrepared(
                        protocol::CredentialLeasePrepared {
                            lease_id: envelope.lease_id.clone(),
                            epoch: envelope.epoch,
                        },
                    )),
                })
            }
            Some(Command::CredentialLeaseCommit(commit)) => {
                QueryControlEvent::parse(protocol::QueryControlResponse {
                    event: Some(Event::CredentialLeaseCommitted(
                        protocol::CredentialLeaseCommitted {
                            lease_id: commit.lease_id.clone(),
                            epoch: commit.epoch,
                        },
                    )),
                })
            }
            Some(Command::Attach(_)) | None => unreachable!("validated control command"),
        };
        let event = event.map_err(protocol_contract_error)?;
        self.events
            .lock()
            .expect("ready lifecycle session")
            .push_back(event);
        Ok(())
    }

    fn recv_timeout(
        &self,
        _timeout: Duration,
    ) -> Result<QueryControlEvent, QueryLifecycleTransportError> {
        self.events
            .lock()
            .expect("ready lifecycle session")
            .pop_front()
            .ok_or_else(|| {
                QueryLifecycleTransportError::new(
                    QueryLifecycleTransportErrorKind::DeadlineExceeded,
                    "ready lifecycle session has no pending event",
                )
            })
    }
}

#[cfg(test)]
impl QueryLifecycleTransport for ReadyLifecycleTransportForTest {
    fn init_query(
        &self,
        _target: QueryLifecycleTarget,
        request: QueryInitRequest,
        _timeout: Duration,
    ) -> Result<QueryInitAck, QueryLifecycleTransportError> {
        let manifest = request.manifest().map_err(protocol_contract_error)?;
        let execution_id = manifest.execution_id().map_err(protocol_contract_error)?;
        let digest = manifest.digest().map_err(protocol_contract_error)?;
        QueryInitAck::parse(protocol::InitQueryResponse {
            execution_id: Some(novarocks_proto_codec::lifecycle::encode_query_execution_id(
                execution_id,
            )),
            init_digest: digest.as_bytes().to_vec(),
            outcome: QueryInitOutcome::QueryInitApplied as i32,
        })
        .map_err(protocol_contract_error)
    }

    fn attach_control(
        &self,
        _target: QueryLifecycleTarget,
        _attach: QueryControlAttach,
        _timeout: Duration,
    ) -> Result<Arc<dyn QueryControlSession>, QueryLifecycleTransportError> {
        Ok(Arc::new(ReadyLifecycleSessionForTest {
            events: Mutex::new(VecDeque::from([QueryControlEvent::parse(
                protocol::QueryControlResponse {
                    event: Some(protocol::query_control_response::Event::ControlReady(
                        protocol::QueryControlReady {
                            catalog_load_state: Some(
                                novarocks_proto_models::catalog::CatalogLoadState {
                                    state: Some(
                                        novarocks_proto_models::catalog::catalog_load_state::State::Ready(
                                            novarocks_proto_models::catalog::CatalogReady {},
                                        ),
                                    ),
                                },
                            ),
                        },
                    )),
                },
            )
            .expect("ready lifecycle control-ready event is valid")])),
        }))
    }

    fn stage_fragments(
        &self,
        _target: QueryLifecycleTarget,
        request: &QueryStageRequest,
        _timeout: Duration,
    ) -> Result<QueryStageAck, QueryLifecycleTransportError> {
        QueryStageAck::new(
            request
                .participant()
                .execution_id()
                .map_err(protocol_contract_error)?,
            StageDigest::compute(request.participant(), &request.fragments())
                .map_err(protocol_contract_error)?,
            QueryStageOutcome::Applied,
            "test participant staged",
        )
        .map_err(protocol_contract_error)
    }

    fn start_prepared_query(
        &self,
        _target: QueryLifecycleTarget,
        request: &QueryStartRequest,
        _timeout: Duration,
    ) -> Result<QueryStartAck, QueryLifecycleTransportError> {
        QueryStartAck::new(
            request.execution_id(),
            request.digest(),
            QueryStartOutcome::Applied,
            "test participant started",
        )
        .map_err(protocol_contract_error)
    }

    fn abort_query(
        &self,
        _target: QueryLifecycleTarget,
        request: QueryAbortRequest,
        _timeout: Duration,
    ) -> Result<QueryTerminationAck, QueryLifecycleTransportError> {
        QueryTerminationAck::parse(protocol::AbortQueryResponse {
            execution_id: Some(novarocks_proto_codec::lifecycle::encode_query_execution_id(
                request.execution_id().map_err(protocol_contract_error)?,
            )),
            accepted_reason: QueryTerminationReason::QueryTerminationCoordinatorAbort as i32,
        })
        .map_err(protocol_contract_error)
    }
}

#[cfg(test)]
fn protocol_contract_error(
    error: novarocks_proto_codec::ProtocolError,
) -> QueryLifecycleTransportError {
    QueryLifecycleTransportError::new(
        QueryLifecycleTransportErrorKind::InvalidResponse,
        error.to_string(),
    )
}

#[cfg(test)]
impl BackendServicesSource {
    fn resolve(
        &self,
        topology: &[LiveBackendTarget],
    ) -> Result<QueryBackendServices, DistributedQueryError> {
        match self {
            Self::Fixed {
                scheduler,
                dispatcher,
                lifecycle_transport,
            } => Ok(QueryBackendServices {
                scheduler: scheduler.clone(),
                dispatcher: Arc::clone(dispatcher),
                lifecycle_transport: Arc::clone(lifecycle_transport),
                live_backends: topology.to_vec(),
            }),
            #[cfg(test)]
            Self::Sequence {
                schedulers,
                dispatcher,
                lifecycle_transport,
            } => {
                let scheduler = schedulers
                    .lock()
                    .expect("frontend test backend sequence lock")
                    .pop_front()
                    .expect("frontend test backend sequence exhausted");
                Ok(QueryBackendServices {
                    scheduler,
                    dispatcher: Arc::clone(dispatcher),
                    lifecycle_transport: Arc::clone(lifecycle_transport),
                    live_backends: topology.to_vec(),
                })
            }
        }
    }
}

fn production_backend_services(
    topology: &[LiveBackendTarget],
    data_runtime: FrontendDataRuntime,
) -> Result<QueryBackendServices, DistributedQueryError> {
    let entries = topology
        .iter()
        .map(|target| {
            target
                .endpoint()
                .map(|endpoint| (target.backend_idx(), endpoint))
                .map_err(|error| failed(error.to_string()))
        })
        .collect::<Result<Vec<_>, _>>()?;
    let snapshot = FrontendBackendSnapshot::from_live_targets(topology.to_vec())?;
    Ok(QueryBackendServices {
        scheduler: FrontendFragmentScheduler::new(snapshot),
        dispatcher: new_fragment_dispatcher(&entries, data_runtime.clone()).map_err(failed)?,
        lifecycle_transport: new_query_lifecycle_transport(topology, data_runtime.clone())
            .map_err(failed)?,
        live_backends: topology.to_vec(),
    })
}

pub struct FrontendDistributedQueryCoordinator {
    report_endpoint: Arc<FrontendReportEndpointBinding>,
    backend_topology: crate::common::backend_topology::BackendTopologyService,
    #[cfg(test)]
    backend_services: Option<BackendServicesSource>,
    runtime_filter_worker_count: NonZeroUsize,
    query_ids: Arc<dyn QueryIdSource>,
    registry: Arc<FrontendQueryRegistry>,
    data_runtime: FrontendDataRuntime,
    /// Validated once at startup from the timeouts the composition root froze;
    /// query admission consumes it rather than re-reading configuration.
    lifecycle_config: FrontendQueryLifecycleConfig,
    /// Every bound the task protocol runs one attempt with, frozen at startup.
    ///
    /// Held rather than read per attempt so a deployment's bounds cannot change
    /// while the process runs.
    task_execution_budgets: novarocks_execution::task_execution::TaskExecutionBudgets,
    /// This frontend process's own identity, minted once per process.
    ///
    /// It is half of every query context reference, so a backend can tell one
    /// frontend's contexts from a restarted frontend's. Minting it per process
    /// rather than per query is what makes that distinction meaningful.
    frontend_process_id: FrontendProcessId,
    pre_start_timeout: Duration,
    task_update_retry_policy: crate::query_execution::split_assignment::TaskUpdateRetryPolicy,
    connector_split_initial_dynamic_filter_wait_cap: Duration,
    native_compatibility_id: NativeCompatibilityId,
}

/// Credential material stays under its planning collector until the round has
/// opened every connector split source. Opening a source may read Iceberg
/// manifests through the request-bound resolver and may observe the vended
/// response that must be sealed into the same Init manifest.
enum RoundCredentialLeaseSource {
    Frozen(QueryCredentialLeases),
    Reservation {
        reservation: QueryAttemptReservation,
        observed_collected: Option<Arc<AtomicBool>>,
    },
}

impl RoundCredentialLeaseSource {
    fn into_credential_leases(self) -> Result<QueryCredentialLeases, DistributedQueryError> {
        match self {
            Self::Frozen(leases) => Ok(leases),
            Self::Reservation {
                reservation,
                observed_collected,
            } => {
                if let Some(observed_collected) = observed_collected {
                    observed_collected.store(
                        reservation.has_collected_credential_leases(),
                        Ordering::Release,
                    );
                }
                reservation.into_credential_leases()
            }
        }
    }
}

fn build_lifecycle_config(
    timeouts: crate::application::FrontendQueryControlTimeouts,
) -> Result<FrontendQueryLifecycleConfig, DistributedQueryError> {
    FrontendQueryLifecycleConfig::new(
        Duration::from_millis(timeouts.heartbeat_interval_ms),
        Duration::from_millis(timeouts.heartbeat_timeout_ms),
        Duration::from_millis(timeouts.init_rpc_timeout_ms),
        Duration::from_millis(timeouts.attach_timeout_ms),
    )?
    .with_stage_start_timeouts(
        Duration::from_millis(timeouts.stage_rpc_timeout_ms),
        Duration::from_millis(timeouts.start_rpc_timeout_ms),
    )?
    .with_terminal_timeouts(
        Duration::from_millis(timeouts.terminal_drain_timeout_ms),
        Duration::from_millis(timeouts.terminal_ack_timeout_ms),
    )?
    .with_participant_fanout_max_inflight(timeouts.participant_fanout_max_inflight)
}

impl FrontendDistributedQueryCoordinator {
    #[expect(
        private_interfaces,
        reason = "The public composition entrypoint receives the frontend-owned native runtime."
    )]
    pub fn new(
        advertised_report_host: String,
        configured_report_port: u16,
        runtime_filter_worker_count: NonZeroUsize,
        native_compatibility_id: NativeCompatibilityId,
        query_control_timeouts: crate::application::FrontendQueryControlTimeouts,
        task_update_retry_policy: crate::query_execution::split_assignment::TaskUpdateRetryPolicy,
        connector_split_initial_dynamic_filter_wait_cap: Duration,
        task_execution_budgets: novarocks_execution::task_execution::TaskExecutionBudgets,
        backend_topology: crate::common::backend_topology::BackendTopologyService,
        data_runtime: FrontendDataRuntime,
    ) -> Result<Self, DistributedQueryError> {
        // Reject an unusable `[runtime]` query-control section at startup rather
        // than on the first query that tries to use it.
        let lifecycle_config = build_lifecycle_config(query_control_timeouts)?;
        let query_id_source = UniqueQueryIdSource::default();
        let query_namespace = query_id_source.namespace();
        tracing::info!(
            query_process_namespace = %query_namespace,
            "frontend query process namespace initialized"
        );
        if cfg!(debug_assertions)
            && std::env::var_os(novarocks_failpoint::QUERY_LIFECYCLE_FAULT_DIR_ENV).is_some()
        {
            eprintln!(
                "NOVAROCKS_QUERY_PROCESS_NAMESPACE query_process_namespace={query_namespace}"
            );
        }
        Ok(Self {
            report_endpoint: Arc::new(FrontendReportEndpointBinding::new(
                advertised_report_host,
                configured_report_port,
            )),
            backend_topology,
            #[cfg(test)]
            backend_services: None,
            runtime_filter_worker_count,
            query_ids: Arc::new(query_id_source),
            registry: Arc::new(FrontendQueryRegistry::new(query_namespace)),
            data_runtime,
            lifecycle_config,
            task_execution_budgets,
            frontend_process_id: FrontendProcessId::new_v7(),
            pre_start_timeout: Duration::from_millis(query_control_timeouts.pre_start_timeout_ms),
            task_update_retry_policy,
            connector_split_initial_dynamic_filter_wait_cap,
            native_compatibility_id,
        })
    }

    #[cfg(test)]
    #[allow(
        dead_code,
        reason = "Coordinator test constructor retains explicit dependency injection for lifecycle coverage."
    )]
    pub(crate) fn new_for_test(
        query_id: QueryId,
        report_endpoint: SocketAddr,
        scheduler: FrontendFragmentScheduler,
        dispatcher: Arc<dyn FragmentDispatcher>,
        runtime_filter_worker_count: NonZeroUsize,
        _test_fixture: Arc<dyn std::any::Any + Send + Sync>,
        lifecycle_transport: Arc<dyn QueryLifecycleTransport>,
    ) -> Self {
        let topology = crate::topology::ClusterBackendService::from_captured_targets_for_test(
            &scheduler.live_targets(),
        );
        Self::new_for_test_with_topology(
            query_id,
            report_endpoint,
            scheduler,
            dispatcher,
            runtime_filter_worker_count,
            _test_fixture,
            lifecycle_transport,
            Arc::new(topology),
        )
    }

    #[cfg(test)]
    #[allow(
        dead_code,
        reason = "Coordinator test constructor retains an injected topology for lifecycle coverage."
    )]
    #[expect(
        clippy::too_many_arguments,
        reason = "The test constructor keeps independent topology and lifecycle fixtures explicit."
    )]
    pub(crate) fn new_for_test_with_topology(
        query_id: QueryId,
        report_endpoint: SocketAddr,
        scheduler: FrontendFragmentScheduler,
        dispatcher: Arc<dyn FragmentDispatcher>,
        runtime_filter_worker_count: NonZeroUsize,
        _test_fixture: Arc<dyn std::any::Any + Send + Sync>,
        lifecycle_transport: Arc<dyn QueryLifecycleTransport>,
        backend_topology: crate::common::backend_topology::BackendTopologyService,
    ) -> Self {
        let test_timeouts = crate::application::FrontendQueryControlTimeouts::default();
        Self {
            task_execution_budgets:
                novarocks_execution::task_execution::TaskExecutionBudgets::DEFAULT,
            frontend_process_id: FrontendProcessId::new_v7(),
            report_endpoint: Arc::new(FrontendReportEndpointBinding::from_socket_addr(
                report_endpoint,
            )),
            backend_topology,
            backend_services: Some(BackendServicesSource::Fixed {
                scheduler,
                dispatcher,
                lifecycle_transport,
            }),
            runtime_filter_worker_count,
            query_ids: Arc::new(FixedQueryIdSource(query_id)),
            registry: Arc::new(FrontendQueryRegistry::new(QueryProcessNamespace::new(
                query_id.high() as u64,
            ))),
            data_runtime: FrontendDataRuntime::new(tokio::runtime::Handle::current()),
            lifecycle_config: build_lifecycle_config(test_timeouts)
                .expect("default query-control timeouts validate"),
            pre_start_timeout: Duration::from_millis(test_timeouts.pre_start_timeout_ms),
            task_update_retry_policy:
                crate::query_execution::split_assignment::TaskUpdateRetryPolicy::default(),
            connector_split_initial_dynamic_filter_wait_cap:
                DEFAULT_INITIAL_DYNAMIC_FILTER_WAIT_CAP,
            native_compatibility_id: NativeCompatibilityId::new([0x71; 32]),
        }
    }

    #[cfg(test)]
    #[allow(
        dead_code,
        reason = "Coordinator test constructor retains sequenced backends for retry coverage."
    )]
    pub(crate) fn new_for_test_with_backend_sequence(
        query_id: QueryId,
        report_endpoint: SocketAddr,
        schedulers: Vec<FrontendFragmentScheduler>,
        dispatcher: Arc<dyn FragmentDispatcher>,
        runtime_filter_worker_count: NonZeroUsize,
        _test_fixture: Arc<dyn std::any::Any + Send + Sync>,
        lifecycle_transport: Arc<dyn QueryLifecycleTransport>,
    ) -> Self {
        let targets = schedulers
            .iter()
            .flat_map(|scheduler| scheduler.live_targets())
            .collect::<Vec<_>>();
        let topology =
            crate::topology::ClusterBackendService::from_captured_targets_for_test(&targets);
        Self::new_for_test_with_backend_sequence_and_topology(
            query_id,
            report_endpoint,
            schedulers,
            dispatcher,
            runtime_filter_worker_count,
            _test_fixture,
            lifecycle_transport,
            Arc::new(topology),
        )
    }

    #[cfg(test)]
    #[expect(
        clippy::too_many_arguments,
        reason = "The retry fixture must inject independently evolving topology and scheduler sequences."
    )]
    pub(crate) fn new_for_test_with_backend_sequence_and_topology(
        query_id: QueryId,
        report_endpoint: SocketAddr,
        schedulers: Vec<FrontendFragmentScheduler>,
        dispatcher: Arc<dyn FragmentDispatcher>,
        runtime_filter_worker_count: NonZeroUsize,
        _test_fixture: Arc<dyn std::any::Any + Send + Sync>,
        lifecycle_transport: Arc<dyn QueryLifecycleTransport>,
        backend_topology: crate::common::backend_topology::BackendTopologyService,
    ) -> Self {
        let test_timeouts = crate::application::FrontendQueryControlTimeouts::default();
        Self {
            task_execution_budgets:
                novarocks_execution::task_execution::TaskExecutionBudgets::DEFAULT,
            frontend_process_id: FrontendProcessId::new_v7(),
            report_endpoint: Arc::new(FrontendReportEndpointBinding::from_socket_addr(
                report_endpoint,
            )),
            backend_topology,
            backend_services: Some(BackendServicesSource::Sequence {
                schedulers: Mutex::new(schedulers.into()),
                dispatcher,
                lifecycle_transport,
            }),
            runtime_filter_worker_count,
            query_ids: Arc::new(FixedQueryIdSource(query_id)),
            registry: Arc::new(FrontendQueryRegistry::new(QueryProcessNamespace::new(
                query_id.high() as u64,
            ))),
            data_runtime: FrontendDataRuntime::new(tokio::runtime::Handle::current()),
            lifecycle_config: build_lifecycle_config(test_timeouts)
                .expect("default query-control timeouts validate"),
            pre_start_timeout: Duration::from_millis(test_timeouts.pre_start_timeout_ms),
            task_update_retry_policy:
                crate::query_execution::split_assignment::TaskUpdateRetryPolicy::default(),
            connector_split_initial_dynamic_filter_wait_cap:
                DEFAULT_INITIAL_DYNAMIC_FILTER_WAIT_CAP,
            native_compatibility_id: NativeCompatibilityId::new([0x71; 32]),
        }
    }

    pub fn terminal_ingress(&self) -> FrontendCoordinatorTerminalIngress {
        FrontendCoordinatorTerminalIngress::new(Arc::clone(&self.registry))
    }

    pub(crate) fn convergence_reader(&self) -> Arc<dyn QueryLifecycleConvergenceReader> {
        Arc::clone(&self.registry) as Arc<dyn QueryLifecycleConvergenceReader>
    }

    pub fn report_endpoint_sink(
        &self,
    ) -> Arc<dyn crate::common::backend_topology::CoordinatorReportEndpointSink> {
        self.report_endpoint.clone()
    }

    pub fn execute(
        &self,
        request: DistributedQueryRequest,
    ) -> Result<DistributedQueryOutcome, DistributedQueryError> {
        self.execute_request(request)
    }

    fn execute_request(
        &self,
        request: DistributedQueryRequest,
    ) -> Result<DistributedQueryOutcome, DistributedQueryError> {
        let statement_deadline = statement_deadline_for_request(&request)?;
        let intent = request.intent();
        let query_id = self.query_ids.next_query_id()?;
        let execution_id = execution_id_for_round(query_id, 1)?;
        self.execute_round(
            query_id,
            execution_id,
            statement_deadline,
            request,
            None,
            RoundCredentialLeaseSource::Frozen(QueryCredentialLeases::empty()),
        )
        .map_err(|error| fail_closed_one_shot_topology_retry(intent, error))
    }

    fn execute_reserved_request(
        &self,
        request: DistributedQueryRequest,
        reservation: QueryAttemptReservation,
    ) -> Result<DistributedQueryOutcome, DistributedQueryError> {
        let statement_deadline = statement_deadline_for_request(&request)?;
        let intent = request.intent();
        let query_id = reservation.query_id();
        let execution_id = reservation.execution_id();
        self.execute_round(
            query_id,
            execution_id,
            statement_deadline,
            request,
            None,
            RoundCredentialLeaseSource::Reservation {
                reservation,
                observed_collected: None,
            },
        )
        .map_err(|error| fail_closed_one_shot_topology_retry(intent, error))
    }

    /// Execute exactly one move-only distributed round. A future statement
    /// controller retains the logical `query_id`, advances only the attempt
    /// id, and supplies a newly planned request here after the old pre-ready
    /// attempt has been aborted. This method never patches or reuses an old
    /// round's artifacts.
    fn execute_round(
        &self,
        query_id: QueryId,
        execution_id: QueryExecutionId,
        statement_deadline: Instant,
        request: DistributedQueryRequest,
        retry_boundary: Option<&dyn PreReadyRetryBoundary>,
        credential_lease_source: RoundCredentialLeaseSource,
    ) -> Result<DistributedQueryOutcome, DistributedQueryError> {
        let parts = request.into_parts();
        let write_stack_session = parts.write_stack_session.clone();
        let intent = parts.completion.intent();
        // Statistics collection enters only with its Core-owned typed program.
        // It never falls through to client-result construction.
        if intent == DistributedQueryIntent::Statistics && parts.statistics_program.is_none() {
            return Err(DistributedQueryError::new(
                DistributedQueryErrorKind::ContractViolation,
                "statistics execution requires a typed StatisticsCollectionProgram",
            ));
        }
        self.backend_topology
            .validate_snapshot(&parts.topology)
            .map_err(pre_ready_topology_validation_error)?;
        #[cfg(test)]
        let backend_services = match &self.backend_services {
            Some(services) => services.resolve(parts.topology.targets())?,
            None => {
                production_backend_services(parts.topology.targets(), self.data_runtime.clone())?
            }
        };
        #[cfg(not(test))]
        let backend_services =
            production_backend_services(parts.topology.targets(), self.data_runtime.clone())?;
        let dispatcher = Arc::clone(&backend_services.dispatcher);
        let _query = self
            .registry
            .register(query_id, intent, Arc::clone(&dispatcher))?;
        let schedule = backend_services
            .scheduler
            .schedule(parts.artifacts.scheduling_view(), execution_id)?;
        let scheduled_backend_ownership = backend_services
            .scheduler
            .scheduled_backend_ownership(&schedule.backend_ids())?;
        self.backend_topology
            .validate_snapshot(&parts.topology)
            .map_err(pre_ready_topology_validation_error)?;
        self.registry
            .set_scheduled_backend_ownership(query_id, &scheduled_backend_ownership)?;
        // Split sources and the lifecycle barrier share this one stable,
        // attempt-local feedback object.  It is populated from the sealed
        // deployment below, before either control readers or the pump starts.
        let feedback_state = Arc::new(
            RuntimeFilterFeedbackState::new(execution_id, Default::default())
                .expect("empty runtime filter feedback declaration is valid"),
        );
        let split_assignment_plan = prepare_round_split_assignment(
            &parts.artifacts,
            &schedule,
            self.task_update_retry_policy,
            Arc::clone(&feedback_state),
            self.connector_split_initial_dynamic_filter_wait_cap,
        )?;
        // A source open may load manifest metadata under the request-local
        // resolver and collect one exact vended response. Seal that collector
        // only after every source is open, then hand its leases to Init.
        let credential_leases = credential_lease_source.into_credential_leases()?;
        let binding_attachment =
            encode_binding_attachment(parts.artifacts.runtime_filter_binding_view())?;
        let scheduled = parts
            .artifacts
            .attach_runtime_filter_bindings(binding_attachment)?
            .bind_schedule(schedule)?;
        let deployment = compile_scheduled_runtime_filter_deployment(
            scheduled.runtime_filter_scheduled_view()?,
            FrontendRuntimeFilterDeploymentCompilerConfig::from_query_lifecycle(
                parts.options.runtime_filter_lifecycle(),
                self.runtime_filter_worker_count.get(),
            )?,
        )?;
        let feedback_declaration = deployment.feedback_declaration().clone();
        feedback_state
            .configure(feedback_declaration.clone())
            .map_err(|error| {
                DistributedQueryError::new(DistributedQueryErrorKind::ContractViolation, error)
            })?;
        let runtime_filter_attachment =
            scheduled.seal_runtime_filter_deployment(deployment.contributions())?;
        let runtime_filter_ready =
            scheduled.attach_runtime_filter_deployment(runtime_filter_attachment)?;
        let timeout_ms = parts
            .statistics_program
            .as_ref()
            .map(|program| {
                program
                    .policy()
                    .attempt_timeout()
                    .as_millis()
                    .max(1)
                    .min(i64::MAX as u128) as i64
            })
            .unwrap_or_else(|| parts.options.timeout_ms().max(0));
        let remaining_budget = statement_deadline.saturating_duration_since(Instant::now());
        if remaining_budget.is_zero() {
            return Err(failed(
                "query deadline elapsed before native lifecycle initialization",
            ));
        }
        let query_deadline_unix_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|error| failed(format!("system clock precedes Unix epoch: {error}")))?
            .as_millis()
            .saturating_add(remaining_budget.as_millis())
            .try_into()
            .map_err(|_| failed("query deadline exceeds u64 milliseconds"))?;
        let init_options = QueryInitOptions::new(
            execution_id,
            self.native_compatibility_id,
            backend_services.live_backends.clone(),
            &parts.options,
            ProtocolQueryOptions::parse(encode_query_options(parts.options.runtime_options()))
                .map_err(|error| {
                    failed(format!(
                        "query options protocol projection is invalid: {error}"
                    ))
                })?,
            query_deadline_unix_ms,
            self.pre_start_timeout,
            self.report_endpoint.resolve()?,
        )?
        .with_credential_leases(credential_leases);
        // A write session's catalog is a materialization input like any typed
        // scan's. Nothing else contributes it: a writer node names its catalog
        // by handle, and the backend leases a catalog from its properties, so a
        // query whose only use of a catalog is writing to it would reach a
        // backend that never materialized it and could not resolve a write
        // runtime for the handle its own plan carries.
        let init_options = match write_stack_session.as_ref() {
            Some(session) => {
                let catalog_set = novarocks_proto_codec::catalog::CatalogSet::new([session
                    .catalog_properties()
                    .clone()])
                .map_err(|error| {
                    failed(format!("write session catalog set is invalid: {error}"))
                })?;
                init_options.with_catalog_set(catalog_set)
            }
            None => init_options,
        };
        // `DistributedQueryIntent::Statistics` is the ONE intent that does not
        // move onto the task protocol. This branch exists for that single
        // reason; it is not ordinary intent dispatch, and nothing else may
        // take the path it selects.
        //
        // ANALYZE's payload is produced by the fragment terminal fact and
        // carried by the old lifecycle's terminal report. The task protocol
        // has no field for it, deliberately: NCP-8 turns statistics into
        // ordinary aggregates read over the root result plane, and its own
        // tasks own both halves of the removal -- NCP-8 T06 moves the payload
        // onto that plane, NCP-8 T08 deletes `statistics_payload` and this
        // branch with it. Building a carrier here would be torn out by that
        // work; deleting the old path before it lands would leave ANALYZE
        // unavailable in between.
        let handoff = RoundHandoff {
            query_id,
            execution_id,
            statement_deadline,
            timeout_ms,
            intent,
            cancellation: parts.cancellation,
            completion: parts.completion,
            topology: parts.topology,
            statistics_program: parts.statistics_program,
            write_stack_session,
            backend_services,
            dispatcher,
            retry_boundary,
            runtime_filter_ready,
            init_options,
            feedback_declaration,
            feedback_state,
            split_assignment_plan,
            scheduled_backend_ownership,
        };
        if intent == DistributedQueryIntent::Statistics {
            return self.execute_statistics_round_on_query_lifecycle(handoff);
        }
        self.execute_round_on_task_protocol(handoff)
    }

    /// The pre-task-protocol execution path, retained only for ANALYZE.
    ///
    /// See the branch that selects it: the statistics payload has no carrier
    /// on the task protocol, and NCP-8 owns both moving it and deleting this.
    /// Everything here is the code that used to run for every intent.
    #[expect(
        clippy::too_many_lines,
        reason = "the retained lifecycle path is moved verbatim so its removal stays a deletion"
    )]
    fn execute_statistics_round_on_query_lifecycle(
        &self,
        handoff: RoundHandoff<'_>,
    ) -> Result<DistributedQueryOutcome, DistributedQueryError> {
        let RoundHandoff {
            query_id,
            execution_id,
            statement_deadline,
            timeout_ms,
            intent,
            cancellation,
            completion,
            topology,
            statistics_program,
            write_stack_session,
            backend_services,
            dispatcher,
            retry_boundary,
            runtime_filter_ready,
            init_options,
            feedback_declaration,
            feedback_state,
            split_assignment_plan,
            scheduled_backend_ownership: _,
        } = handoff;
        let lifecycle_barrier = FrontendQueryLifecycleBarrier::new(
            Arc::clone(&backend_services.lifecycle_transport),
            Arc::clone(&self.registry),
            self.lifecycle_config,
        )
        .with_cancellation(cancellation.clone())
        .with_backend_topology(Arc::clone(&self.backend_topology), topology.revision())
        .with_runtime_filter_feedback(feedback_declaration)
        .with_runtime_filter_feedback_state(feedback_state);
        let connector_binding_ready = runtime_filter_ready
            .initialize_query(init_options, &lifecycle_barrier)
            .map_err(|error| {
                reclassify_pre_ready_lifecycle_failure(
                    self.backend_topology.as_ref(),
                    &topology,
                    error,
                    statement_deadline.min(
                        Instant::now()
                            .checked_add(self.lifecycle_config.init_rpc_timeout())
                            .unwrap_or(statement_deadline),
                    ),
                )
            })?
            .catalog_ready();
        if let Some(retry_boundary) = retry_boundary {
            retry_boundary.close_after_control_ready();
        }
        let submission_view = connector_binding_ready.native_submission_view()?;
        let submission_attachment = encode_native_submission(&submission_view).map_err(failed)?;
        let stage_prepared = connector_binding_ready.finish_stage(submission_attachment)?;
        let staged = stage_prepared.stage(&lifecycle_barrier)?;
        if let Some(retry_boundary) = retry_boundary {
            retry_boundary.close_after_stage_or_start();
        }
        for batch in staged.batches() {
            self.backend_topology.record_successful_stage(
                batch.binding().target().backend_idx(),
                batch.request().fragments().len(),
            );
        }
        let execution = staged.start(&lifecycle_barrier)?;
        // Started only after Start: a backend admits a task update only while
        // its attempt is staged or running. The guard owns the pump thread, so
        // every exit path below closes the sources by dropping it.
        let mut split_assignment = split_assignment_plan
            .map(|plan| {
                GrpcTaskUpdateTransport::new(plan.endpoints(), self.data_runtime.clone())
                    .map(|transport| (plan, Arc::new(transport)))
                    .map_err(|error| failed(format!("task update transport: {error}")))
            })
            .transpose()?
            .and_then(|(plan, transport)| {
                SplitAssignmentRoundGuard::start(
                    execution_id,
                    plan,
                    transport as Arc<dyn TaskUpdateTransport>,
                )
            });
        let RunningNativeExecutionParts {
            root_fetch,
            expected_output,
            query_lifecycle_lease,
        } = execution.into_parts();
        let mut query_lifecycle_lease = Some(query_lifecycle_lease);
        if let Some(message) = self.registry.first_failure(query_id)
            && intent != DistributedQueryIntent::Write
        {
            let message = abort_query_lifecycle(&mut query_lifecycle_lease, message);
            return Err(failed(message));
        }

        let deadline = statement_deadline;
        let mut batches = Vec::new();
        // Recorded rather than inferred. Every other exit from the loop below
        // returns an error, so "we got here" would imply end of stream -- but a
        // write commits on the strength of this fact, and a fact that important
        // should not rest on a future edit preserving a control-flow accident.
        let mut observed_result_eof = false;
        if root_fetch.uses_result_buffer() {
            loop {
                if cancellation.is_cancelled() {
                    return Err(self.fail_cancel_then_abort_query_lifecycle(
                        query_id,
                        &mut query_lifecycle_lease,
                        "query cancelled while fetching result",
                    ));
                }
                if let Some(message) = self.registry.first_failure(query_id) {
                    let message = abort_query_lifecycle(&mut query_lifecycle_lease, message);
                    return Err(failed(message));
                }
                let now = Instant::now();
                if now >= deadline {
                    return Err(self.fail_cancel_then_abort_query_lifecycle(
                        query_id,
                        &mut query_lifecycle_lease,
                        format!("query timed out after {timeout_ms} ms"),
                    ));
                }
                let fetch_wait_ms = deadline
                    .saturating_duration_since(now)
                    .as_millis()
                    .clamp(1, 300) as i64;
                let fetch = match dispatcher.fetch_result(
                    root_fetch.backend_idx(),
                    root_fetch.fragment_instance_id(),
                    fetch_wait_ms,
                    Some(expected_output.fetch_view()),
                ) {
                    Ok(fetch) => fetch,
                    Err(error) => {
                        return Err(self.fail_cancel_then_abort_query_lifecycle(
                            query_id,
                            &mut query_lifecycle_lease,
                            error,
                        ));
                    }
                };
                match fetch {
                    FetchOutcome::Ready(batch) => batches.push(batch),
                    FetchOutcome::NotReady => continue,
                    FetchOutcome::Eof => {
                        observed_result_eof = true;
                        break;
                    }
                    FetchOutcome::Err(error) => {
                        return Err(self.fail_cancel_then_abort_query_lifecycle(
                            query_id,
                            &mut query_lifecycle_lease,
                            error,
                        ));
                    }
                }
            }
        }

        if cancellation.is_cancelled() {
            return Err(self.fail_cancel_then_abort_query_lifecycle(
                query_id,
                &mut query_lifecycle_lease,
                "query cancelled before terminal finalization",
            ));
        }
        if let Some(message) = self.registry.first_failure(query_id) {
            let message = abort_query_lifecycle(&mut query_lifecycle_lease, message);
            return Err(failed(message));
        }

        // A successful query must not construct its terminal/profile outcome
        // while this attempt still owns an unconfirmed TaskUpdate.  On error
        // the guard's Drop path stops and closes the sources; on success we
        // join explicitly so every immutable split assignment is either
        // confirmed or reported as the query failure.
        let split_assignment_profile = if let Some(assignment) = split_assignment.take() {
            match assignment.finish() {
                Ok(profile) => profile,
                Err(error) => {
                    return Err(self.fail_cancel_then_abort_query_lifecycle(
                        query_id,
                        &mut query_lifecycle_lease,
                        format!("split assignment did not finish: {error}"),
                    ));
                }
            }
        } else {
            novarocks_spi::connector::read_stack::SplitSourceProfile::default()
        };

        // A connector write reaches its external commit only after this
        // attempt finalizes: the frontend reloads table metadata and writes
        // manifests through object storage once every participant has
        // converged. Retain the attempt's terminal-only storage capability
        // while the lease still exists and hand it to the session that owns
        // the commit decision. Taking it is also what tells finalization that
        // a write still needs the attempt's credential leases; on a
        // vended-credential deployment they would otherwise be cleared before
        // the commit could read a single object. A deployment with no vended
        // lease has no capability to retain and is unaffected.
        if let Some(session) = write_stack_session.as_ref()
            && let Some(resolver) = query_lifecycle_lease
                .as_ref()
                .and_then(QueryLifecycleLease::retain_terminal_storage_resolver)
        {
            session.retain_terminal_storage_resolver(resolver);
        }
        let terminal_set = query_lifecycle_lease
            .take()
            .expect("query lifecycle lease is present through query completion")
            .finalize()?;
        if !terminal_set.is_success() {
            return Err(failed(
                "query terminal snapshot set contains a failed, cancelled, or incomplete fragment",
            ));
        }

        // A failure recorded after this point cannot invalidate the query. Every
        // participant already converged on a Succeeded terminal, so the work
        // finished and a write's commit fragments are all in hand; the
        // pre-finalize check above is what fails a query that actually failed. Heartbeat
        // observations keep latching into active queries regardless -- a backend
        // briefly marked unavailable under load latches into every query
        // scheduled on it -- and consuming that here turned a completed
        // statement into `connector write execution ended without a complete
        // staged-report commit`, a message describing neither the latch nor its
        // reason, because the latch made the builder emit an abort that this
        // call site then discarded.
        if let Some(message) = self.registry.first_failure(query_id) {
            tracing::warn!(
                query_id = ?query_id,
                latched = %message,
                "query failure recorded after a successful terminal set; the completed \
                 query is not failed by it",
            );
        }
        let outcome = (|| match intent {
            DistributedQueryIntent::Result => {
                completion.result(expected_output.into_query_result(batches)?)
            }
            DistributedQueryIntent::Write => {
                let session = write_stack_session.ok_or_else(|| {
                    DistributedQueryError::new(
                        DistributedQueryErrorKind::ContractViolation,
                        "distributed write execution has no connector write session",
                    )
                })?;
                // The write relation is engine machinery: the client's result
                // is empty, and the rows are decoded by position against the
                // frozen relation instead.
                let mut decoder =
                    crate::query_execution::write_result::RootWriteResultDecoder::new(
                        &session.expected_targets(),
                    )
                    .map_err(|error| {
                        DistributedQueryError::new(
                            DistributedQueryErrorKind::ContractViolation,
                            error,
                        )
                    })?;
                for batch in batches {
                    decoder.apply_chunk(&batch.into_chunk()).map_err(|error| {
                        DistributedQueryError::new(
                            DistributedQueryErrorKind::ContractViolation,
                            error,
                        )
                    })?;
                }

                let mut barrier = crate::query_execution::write_barrier::WriteCommitBarrier::new();
                // Only an observed end of stream can produce a complete set. A
                // prefix is not most of a write; it is no write at all.
                if observed_result_eof {
                    barrier.observe_prepared_write_set(decoder.finish_at_eof().map_err(
                        |error| {
                            DistributedQueryError::new(
                                DistributedQueryErrorKind::ContractViolation,
                                error,
                            )
                        },
                    )?);
                }
                barrier.observe_execution_terminals(terminal_set.is_success());
                if cancellation.is_cancelled() {
                    barrier.observe_cancelled();
                }

                let prepared = barrier.into_committable().map_err(|blocked| {
                    DistributedQueryError::new(
                        DistributedQueryErrorKind::ContractViolation,
                        blocked.as_str(),
                    )
                })?;
                completion.write_session_outcome(session, prepared)
            }
            DistributedQueryIntent::Profile => {
                let result = expected_output.into_query_result(batches)?;
                let mut builder = ProfileTerminalBuilder::new();
                for snapshot in terminal_set.snapshots() {
                    builder.apply_profile_contribution(snapshot)?;
                    for fragment in snapshot.fragments() {
                        builder.apply_terminal(fragment.as_proto())?;
                    }
                }
                builder.apply_split_assignment_profile(split_assignment_profile);
                completion.profile(result, builder.finish())
            }
            DistributedQueryIntent::Statistics => {
                let program = statistics_program.as_ref().ok_or_else(|| {
                    DistributedQueryError::new(
                        DistributedQueryErrorKind::ContractViolation,
                        "statistics execution lost its typed collection program",
                    )
                })?;
                let result = program.finish_fragment_payloads(
                    terminal_set
                        .fragments()
                        .map(|fragment| fragment.statistics_payload.as_slice()),
                )?;
                completion.statistics(program, result)
            }
        })();
        if let Err(error) = &outcome {
            let _ = self
                .registry
                .latch_failure_and_cancel(query_id, error.message().to_string());
            return Err(DistributedQueryError::new(error.kind(), error.message()));
        }
        // A split that never reached its task means this query returned fewer
        // rows than it should, so an assignment failure fails the query even
        // though the fetch loop already produced an outcome.
        if let Some(split_assignment) = split_assignment
            && let Err(error) = split_assignment.finish()
        {
            let message = self.fail_and_cancel(
                query_id,
                format!("runtime split assignment failed: {error}"),
            );
            return Err(message);
        }
        outcome
    }

    /// The production execution path: one attempt's tasks on the task protocol.
    ///
    /// It replaces `Init -> ControlReady -> Stage -> Start` with the substrate's
    /// own sequence -- establish one query context per scheduled backend,
    /// create every task, open each exchange edge when its destinations exist
    /// -- and then reads the client's rows from the root task's own result
    /// plane. Nothing here decides completion: the state machine computes the
    /// verdicts and this loop reads them.
    #[expect(
        clippy::too_many_lines,
        reason = "one attempt's assembly, drive loop and drain are one linear story; splitting them would hide the order they must happen in"
    )]
    fn execute_round_on_task_protocol(
        &self,
        handoff: RoundHandoff<'_>,
    ) -> Result<DistributedQueryOutcome, DistributedQueryError> {
        let RoundHandoff {
            query_id,
            execution_id,
            statement_deadline,
            timeout_ms,
            intent,
            cancellation,
            completion,
            // The captured snapshot is kept: it is what a pre-establish
            // failure is judged against. A statistics program never reaches
            // this path at all.
            topology: captured_topology,
            statistics_program: _,
            write_stack_session,
            backend_services,
            dispatcher: _,
            retry_boundary,
            runtime_filter_ready,
            init_options,
            feedback_declaration,
            feedback_state,
            split_assignment_plan,
            scheduled_backend_ownership,
        } = handoff;

        // The catalog lease this produces is held for the whole attempt: the FE
        // control leases that resolved this query's typed reads must outlive
        // the backends that are still reading through them.
        let mut prepared = runtime_filter_ready.prepare_task_execution(init_options)?;
        let submission_attachment = {
            let view = prepared.native_submission_view()?;
            encode_native_submission(&view).map_err(failed)?
        };
        let (submissions, root_fetch, expected_output) = prepared
            .seal_task_submission(submission_attachment)?
            .into_parts();
        if !root_fetch.uses_result_buffer() {
            // The root result plane serves only the task whose sink is the
            // query's result sink, and a read completes only once this
            // frontend has consumed the end of that stream. A root that owns
            // no result buffer could satisfy neither, so it is refused here
            // rather than waited on forever.
            return Err(DistributedQueryError::new(
                DistributedQueryErrorKind::ContractViolation,
                "task execution requires a root fragment that owns the query result sink",
            ));
        }

        let backend_process_ids = scheduled_backend_ownership
            .iter()
            .copied()
            .collect::<BTreeMap<usize, BackendProcessId>>();
        let mut backends = Vec::with_capacity(backend_process_ids.len());
        for target in &backend_services.live_backends {
            let Some(&process_id) = backend_process_ids.get(&target.backend_idx()) else {
                // Live but not scheduled. Freezing it would let an operation
                // reach a process this attempt never placed a task on.
                continue;
            };
            let endpoint = target
                .endpoint()
                .map_err(|error| failed(error.to_string()))?;
            backends.push((process_id, endpoint));
        }
        if backends.len() != backend_process_ids.len() {
            return Err(failed(
                "a scheduled backend is absent from this attempt's live snapshot",
            ));
        }

        // Each backend establishes the role bindings its own tasks play, so the
        // map is built over the scheduled backends rather than over whatever
        // the deployment happens to name: the deployment covers every live
        // backend, and a live one this attempt did not schedule has no context
        // to establish anything on.
        //
        // A deployment with no contributions at all is a query with no runtime
        // filters, and every context establishes the empty contribution. That
        // is the neutral form of the absent contribution the old participant
        // manifest carried, not a guessed default: the sealing step admits
        // only "one per live backend" or "none", so a missing entry while
        // others exist is a real disagreement between the filter compiler and
        // the scheduler and is refused.
        let compiled_filters = prepared.runtime_filter_contributions();
        let mut runtime_filters = Vec::with_capacity(backend_process_ids.len());
        for (&backend_idx, &process_id) in &backend_process_ids {
            let contribution = match compiled_filters.get(&backend_idx) {
                Some(contribution) => contribution.clone(),
                None if compiled_filters.is_empty() => {
                    novarocks_proto_models::novarocks::RuntimeFilterContribution::default()
                }
                None => {
                    return Err(failed(format!(
                        "runtime filter deployment has no contribution for scheduled backend \
                         {backend_idx}"
                    )));
                }
            };
            runtime_filters.push((process_id, contribution));
        }
        let establish = AttemptEstablishFacts::freeze(
            prepared.catalog_set().as_proto().clone(),
            runtime_filters,
            prepared.init_options().credential_leases(),
        )
        .map_err(|error| failed(error.to_string()))?;
        // Taken before the facts are handed to the runner: the rotation owner
        // has to start from the exact domain every establish installs, or its
        // first rotation would be a gap every context refuses.
        let initial_credential = establish.credential().clone();
        // Taken now that the establish holds its own wire copy: the attempt
        // keeps one owner of the vended material, and the connectors' planning
        // route is re-pointed at it. Held to the end of this call so neither
        // that route nor a write session's commit finds it already dropped.
        let attempt_storage = prepared.take_terminal_storage_resolver();

        // Declared from the encoded plans, before they are consumed. A write's
        // completion turns on every writer having finished, and "this query is
        // a write" does not say which of its fragments write.
        let writer_fragments = if intent == DistributedQueryIntent::Write {
            submissions
                .iter()
                .filter(|submission| submission.declares_table_writer())
                .map(ValidatedNativeSubmission::fragment_id)
                .collect::<BTreeSet<_>>()
        } else {
            BTreeSet::new()
        };

        let wake = Arc::new(CondvarWake::default());
        let attempt = AttemptWireFacts {
            query_options: *prepared.init_options().query_options().as_proto(),
            native_compatibility_id: Some(
                novarocks_proto_models::novarocks::NativeCompatibilityId {
                    value: self.native_compatibility_id.as_bytes().to_vec(),
                },
            ),
        };
        let AssembledRound {
            mut round,
            split_delivery,
        } = assemble_round(
            execution_id,
            self.frontend_process_id,
            prepared.scheduling_plan(),
            prepared.fragment_edges(),
            &backend_process_ids,
            &backends,
            submissions,
            establish,
            Arc::clone(&wake) as Arc<dyn StatusIntakeWake>,
            AttemptTransport {
                budget: self.task_execution_budgets.dispatch,
                transport: self.task_execution_budgets.transport,
                status_subscription_error_budget: self
                    .task_execution_budgets
                    .status_subscription_error_budget,
                attempt,
                data_runtime: self.data_runtime.clone(),
            },
        )
        .map_err(|error| failed(error.to_string()))?;
        let result_transport = Arc::new(
            NativeTaskResultTransport::new(&backends, self.data_runtime.clone()).map_err(failed)?,
        );
        let root_task = round.root_task();

        // The two per-attempt feedback loops. Both hang on the runner's one
        // pump seam rather than on call sites in the drive loop below: see
        // `TaskRound::turn` for why they land between the status fold and
        // submission. The runner refuses to turn until this has run, so
        // omitting it is a query failure rather than a silently missing loop.
        let credential_rotation = install_attempt_pumps(
            &mut round,
            AttemptPumps {
                execution_id,
                feedback_state: Arc::clone(&feedback_state),
                declared_feedback_channels: feedback_declaration.channels().len(),
                reads: Arc::clone(&result_transport) as Arc<dyn TaskDynamicFilterReads>,
                initial_credential: &initial_credential,
                credential_storage: attempt_storage.clone(),
            },
        );

        let mut write_completion = if intent == DistributedQueryIntent::Write {
            let writers = round
                .execution()
                .graph()
                .tasks()
                .filter(|task| writer_fragments.contains(&task.fragment_id()))
                .map(TaskNode::identity)
                .collect::<Vec<_>>();
            Some(
                WriteCompletionTracker::try_new(root_task, writers)
                    .map_err(|error| failed(error.to_string()))?,
            )
        } else {
            None
        };

        // Started as soon as the substrate exists rather than after a staging
        // barrier this path does not have: a delivery for a task that is still
        // creating is queued on that task and drains when its create is
        // acknowledged.
        let mut split_assignment = split_assignment_plan.and_then(|plan| {
            SplitAssignmentRoundGuard::start(
                execution_id,
                plan,
                Arc::clone(&split_delivery) as Arc<dyn TaskUpdateTransport>,
            )
        });

        let mut final_task_info = FinalTaskInfoCollector::new(
            result_transport.as_ref(),
            intent == DistributedQueryIntent::Profile,
        );
        // The task protocol's analogue of the two lifecycle gates: every query
        // context established is ControlReady, every task created is Stage and
        // Start. Both close exactly once, and until the first one closes a
        // failure may still be a replaced backend rather than this query's.
        let mut contexts_established = false;
        let mut tasks_created = false;
        // The membership owner gets the same window the old Init RPC had to
        // prove a replacement, taken from the task protocol's own establish
        // cap rather than a second number invented here.
        let establish_wait = self.task_execution_budgets.wait_caps.clamp(
            OperationKind::UpdateQueryContext,
            MaxWait::default_for(OperationKind::UpdateQueryContext),
        );
        let mut batches = Vec::new();
        // Recorded rather than inferred, exactly as the old path recorded it: a
        // write commits on the strength of this fact.
        let mut observed_result_eof = false;
        let outcome = loop {
            // Recomputed each turn so the two gates and the classification
            // window are read from this turn's state rather than last turn's.
            let classification = TaskRoundFailureClassification {
                before_contexts_established: !contexts_established,
                captured: &captured_topology,
                observation_deadline: statement_deadline.min(
                    Instant::now()
                        .checked_add(establish_wait)
                        .unwrap_or(statement_deadline),
                ),
            };
            if cancellation.is_cancelled() {
                break Err(self.fail_task_round(
                    query_id,
                    &mut round,
                    &split_delivery,
                    classification,
                    "query cancelled while fetching result",
                ));
            }
            if let Some(message) = self.registry.first_failure(query_id) {
                break Err(self.fail_task_round(
                    query_id,
                    &mut round,
                    &split_delivery,
                    classification,
                    message,
                ));
            }
            let now = Instant::now();
            if now >= statement_deadline {
                break Err(self.fail_task_round(
                    query_id,
                    &mut round,
                    &split_delivery,
                    classification,
                    format!("query timed out after {timeout_ms} ms"),
                ));
            }

            let mut moved = match advance_task_round(&mut round, &split_delivery) {
                Ok(report) => !report.is_idle(),
                Err(error) => {
                    break Err(self.fail_task_round(
                        query_id,
                        &mut round,
                        &split_delivery,
                        classification,
                        format!("task execution did not advance: {error}"),
                    ));
                }
            };
            final_task_info.observe(&round);
            if !contexts_established && round.contexts_established() {
                contexts_established = true;
                if let Some(retry_boundary) = retry_boundary {
                    retry_boundary.close_after_control_ready();
                }
            }
            if contexts_established && !tasks_created && round.tasks_created() {
                tasks_created = true;
                if let Some(retry_boundary) = retry_boundary {
                    retry_boundary.close_after_stage_or_start();
                }
            }
            // Re-read after the gates moved: a failure seen from here on is
            // judged against the window this turn actually reached.
            let classification = TaskRoundFailureClassification {
                before_contexts_established: !contexts_established,
                captured: &captured_topology,
                observation_deadline: statement_deadline.min(
                    Instant::now()
                        .checked_add(establish_wait)
                        .unwrap_or(statement_deadline),
                ),
            };
            if let Some(detail) = round.failure_cause() {
                let detail = format!("task execution terminated: {detail:?}");
                break Err(self.fail_task_round(
                    query_id,
                    &mut round,
                    &split_delivery,
                    classification,
                    detail,
                ));
            }

            if !observed_result_eof && task_is_created(&round, root_task) {
                let wait = max_root_result_wait(now, statement_deadline);
                match result_transport.fetch_root_result(
                    root_task,
                    wait,
                    Some(expected_output.fetch_view()),
                ) {
                    Ok(RootResultOutcome::Ready {
                        packet_sequence,
                        batch,
                    }) => {
                        if let Err(error) = round.consume_root_result_packet(packet_sequence, false)
                        {
                            break Err(self.fail_task_round(
                                query_id,
                                &mut round,
                                &split_delivery,
                                classification,
                                format!("root result packet was refused: {error}"),
                            ));
                        }
                        batches.push(batch);
                        moved = true;
                    }
                    Ok(RootResultOutcome::EndOfStream { packet_sequence }) => {
                        if let Err(error) = round.consume_root_result_packet(packet_sequence, true)
                        {
                            break Err(self.fail_task_round(
                                query_id,
                                &mut round,
                                &split_delivery,
                                classification,
                                format!("root result end of stream was refused: {error}"),
                            ));
                        }
                        observed_result_eof = true;
                        moved = true;
                    }
                    Ok(RootResultOutcome::NotReady) => moved = true,
                    Ok(RootResultOutcome::Failed(detail)) => {
                        break Err(self.fail_task_round(
                            query_id,
                            &mut round,
                            &split_delivery,
                            classification,
                            detail,
                        ));
                    }
                    Err(error) => {
                        break Err(self.fail_task_round(
                            query_id,
                            &mut round,
                            &split_delivery,
                            classification,
                            error,
                        ));
                    }
                }
            }

            if round.client_visible_completion() {
                match write_completion.as_mut() {
                    // A write's completion is not the read's. Every declared
                    // writer and the root finish task must have published
                    // FINISHED, so this keeps turning while one has not.
                    Some(tracker) => {
                        observe_write_statuses(&round, tracker);
                        if tracker
                            .execution_verdict(round.failure_cause().is_some())
                            .is_complete()
                        {
                            break Ok(());
                        }
                    }
                    None => break Ok(()),
                }
            }
            if !moved {
                wake.wait(TASK_ROUND_IDLE_WAIT);
            }
        };
        // Nothing rotates or prunes for a query whose answer is already
        // decided, success or failure. Both loops are stopped before the
        // outcome is propagated: the drain below keeps turning, and a rotation
        // started there would be judged against a hard deadline for material
        // no task still reads -- which would turn a linearized completion into
        // a failure. Closing the feedback state also wakes any split source
        // still inside its initial wait.
        if let Some(rotation) = &credential_rotation {
            rotation.wipe();
        }
        feedback_state.close();
        outcome?;

        // The client-visible completion is linearized. Draining closes this
        // attempt's internal resources and releases every query context; it
        // must never be able to take that completion back, so a drain that
        // does not finish inside the statement's own budget is reported rather
        // than turned into a query failure.
        drain_task_round(
            &mut round,
            &split_delivery,
            &wake,
            split_assignment.as_ref(),
            statement_deadline,
            self.task_execution_budgets
                .transport
                .frontend_queue_residence(),
            execution_id,
            &mut final_task_info,
        );

        // The split worker blocks on acknowledgements the drain above settles,
        // so joining it is safe only now that it has stopped. A worker still
        // waiting is woken with an unknown outcome, and the guard's own stop
        // then keeps it from resending.
        let split_assignment_profile = match split_assignment.take() {
            Some(assignment) => {
                if !assignment.is_finished() {
                    split_delivery.abandon("split assignment round ended with the attempt");
                }
                match assignment.finish() {
                    Ok(profile) => profile,
                    Err(error) => {
                        return Err(self.fail_and_cancel(
                            query_id,
                            format!("split assignment did not finish: {error}"),
                        ));
                    }
                }
            }
            None => novarocks_spi::connector::read_stack::SplitSourceProfile::default(),
        };

        if let Some(session) = write_stack_session.as_ref()
            && let Some(resolver) = attempt_storage.as_ref()
        {
            // A connector write reaches its external commit after this attempt
            // finalizes, and on a vended-credential deployment that commit
            // reads object storage through the attempt's own leases.
            session
                .retain_terminal_storage_resolver(Arc::clone(resolver)
                    as Arc<dyn novarocks_spi::connector::ConnectorStorageResolver>);
        }

        let outcome = (|| match intent {
            DistributedQueryIntent::Result => {
                completion.result(expected_output.into_query_result(batches)?)
            }
            DistributedQueryIntent::Write => {
                let session = write_stack_session.ok_or_else(|| {
                    DistributedQueryError::new(
                        DistributedQueryErrorKind::ContractViolation,
                        "distributed write execution has no connector write session",
                    )
                })?;
                let tracker = write_completion.as_mut().ok_or_else(|| {
                    DistributedQueryError::new(
                        DistributedQueryErrorKind::ContractViolation,
                        "distributed write execution has no write completion tracker",
                    )
                })?;
                // The write relation is engine machinery: the client's result
                // is empty, and the rows are decoded by position against the
                // frozen relation instead.
                let mut decoder =
                    crate::query_execution::write_result::RootWriteResultDecoder::new(
                        &session.expected_targets(),
                    )
                    .map_err(|error| {
                        DistributedQueryError::new(
                            DistributedQueryErrorKind::ContractViolation,
                            error,
                        )
                    })?;
                for batch in batches {
                    decoder.apply_chunk(&batch.into_chunk()).map_err(|error| {
                        DistributedQueryError::new(
                            DistributedQueryErrorKind::ContractViolation,
                            error,
                        )
                    })?;
                }

                let mut barrier = crate::query_execution::write_barrier::WriteCommitBarrier::new();
                // Only an observed end of stream can produce a complete set. A
                // prefix is not most of a write; it is no write at all.
                if observed_result_eof {
                    barrier.observe_prepared_write_set(decoder.finish_at_eof().map_err(
                        |error| {
                            DistributedQueryError::new(
                                DistributedQueryErrorKind::ContractViolation,
                                error,
                            )
                        },
                    )?);
                    tracker.note_prepared_write_set_complete();
                }
                observe_write_statuses(&round, tracker);
                // The execution half only. The barrier keeps "the prepared
                // write set is complete" as its own independent fact, and
                // handing it a verdict that already folded that in would let
                // one signal stand for both again.
                barrier.observe_task_execution(
                    tracker.execution_verdict(round.failure_cause().is_some()),
                );
                if cancellation.is_cancelled() {
                    barrier.observe_cancelled();
                }

                let prepared_set = barrier.into_committable().map_err(|blocked| {
                    DistributedQueryError::new(
                        DistributedQueryErrorKind::ContractViolation,
                        blocked.as_str(),
                    )
                })?;
                completion.write_session_outcome(session, prepared_set)
            }
            DistributedQueryIntent::Profile => {
                let result = expected_output.into_query_result(batches)?;
                let mut builder = ProfileTerminalBuilder::new();
                for info in &final_task_info.collected {
                    builder.apply_task_operator_statistics(info)?;
                }
                builder.apply_split_assignment_profile(split_assignment_profile);
                completion.profile(result, builder.finish())
            }
            // Statistics never reaches this path; the branch in `execute_round`
            // that keeps it on the old lifecycle is the only route it has.
            DistributedQueryIntent::Statistics => Err(DistributedQueryError::new(
                DistributedQueryErrorKind::ContractViolation,
                "statistics execution does not run on the task protocol",
            )),
        })();
        if let Err(error) = &outcome {
            let _ = self
                .registry
                .latch_failure_and_cancel(query_id, error.message().to_string());
            return Err(DistributedQueryError::new(error.kind(), error.message()));
        }
        outcome
    }

    /// Stands every query context of this attempt down, then classifies why it
    /// failed.
    ///
    /// A dropped round would leave the backends holding tasks until their
    /// lease expired; an explicit abort is what makes a failed attempt release
    /// its resources at the moment the frontend gave up on it.
    ///
    /// A failure that arrived before every query context was established gets
    /// the same treatment the old barrier gave a pre-ControlReady one: it is
    /// marked as needing a topology observation, and the membership owner --
    /// not this failure's text -- decides whether an exact captured process
    /// was replaced. Nothing is latched in that case, because a replanned
    /// round registers its own attempt and the failure belongs to the one
    /// being abandoned.
    fn fail_task_round(
        &self,
        query_id: QueryId,
        round: &mut TaskRound,
        split_delivery: &SplitDeliveryBridge,
        classification: TaskRoundFailureClassification<'_>,
        message: impl Into<String>,
    ) -> DistributedQueryError {
        let message = message.into();
        split_delivery.abandon(message.clone());
        abort_task_round(round, &message);
        if !classification.before_contexts_established {
            return self.fail_and_cancel(query_id, message);
        }
        let classified = reclassify_pre_ready_lifecycle_failure(
            self.backend_topology.as_ref(),
            classification.captured,
            DistributedQueryError::pre_ready_topology_observation(message),
            classification.observation_deadline,
        );
        if classified.pre_ready_topology_outcome().is_some() {
            return classified;
        }
        self.fail_and_cancel(query_id, classified.message().to_owned())
    }

    fn fail_and_cancel(
        &self,
        query_id: QueryId,
        message: impl Into<String>,
    ) -> DistributedQueryError {
        match self.registry.latch_failure_and_cancel(query_id, message) {
            Ok(message) => failed(message),
            Err(error) => error,
        }
    }

    fn fail_cancel_then_abort_query_lifecycle(
        &self,
        query_id: QueryId,
        lease: &mut Option<QueryLifecycleLease>,
        message: impl Into<String>,
    ) -> DistributedQueryError {
        let primary = self.fail_and_cancel(query_id, message);
        let enriched = abort_query_lifecycle(lease, primary.message().to_string());
        let _ = self
            .registry
            .preserve_failure_context(query_id, enriched.clone());
        failed(self.registry.first_failure(query_id).unwrap_or(enriched))
    }
}

impl DistributedQueryCoordinator for FrontendDistributedQueryCoordinator {
    fn reserve_initial_attempt(
        &self,
    ) -> Result<crate::query_execution::completion::QueryAttemptReservation, DistributedQueryError>
    {
        crate::query_execution::completion::QueryAttemptReservation::first(
            self.query_ids.next_query_id()?,
        )
    }

    fn execute(
        &self,
        request: DistributedQueryRequest,
    ) -> Result<DistributedQueryOutcome, DistributedQueryError> {
        self.execute_request(request)
    }

    fn execute_reserved(
        &self,
        request: DistributedQueryRequest,
        reservation: QueryAttemptReservation,
    ) -> Result<DistributedQueryOutcome, DistributedQueryError> {
        self.execute_reserved_request(request, reservation)
    }

    fn execute_prepared(
        &self,
        operation: crate::query_execution::completion::PreparedDistributedQuery,
    ) -> Result<StatementResult, DistributedQueryError> {
        let (first_request, first_completion, mut round_factory, reservation) =
            operation.into_parts();
        let reservation = match reservation {
            Some(reservation) => reservation,
            None => crate::query_execution::completion::QueryAttemptReservation::first(
                self.query_ids.next_query_id()?,
            )?,
        };
        let query_id = reservation.query_id();
        let first_execution_id = reservation.execution_id();
        let first_revision = first_request.topology().revision();
        let retry_deadline = statement_deadline_for_request(&first_request)?;
        let first_retry_boundary = round_factory
            .as_deref()
            .map(|factory| factory as &dyn PreReadyRetryBoundary);
        match self.execute_round(
            query_id,
            first_execution_id,
            retry_deadline,
            first_request,
            first_retry_boundary,
            RoundCredentialLeaseSource::Reservation {
                reservation,
                observed_collected: None,
            },
        ) {
            Ok(outcome) => first_completion.complete(outcome).map_err(failed),
            Err(first_error) => {
                // Never classify ordinary failure text as a topology retry.
                // The lifecycle barrier and pre-Init validation are the sole
                // typed evidence sources.
                if first_error.pre_ready_topology_outcome().is_none() {
                    return Err(first_error);
                }
                let Some(factory) = round_factory.as_deref_mut() else {
                    return Err(first_error);
                };
                let reason = pre_ready_topology_reason(
                    first_error
                        .pre_ready_topology_outcome()
                        .expect("pre-ready topology outcome checked above"),
                );
                if let Err(error) = factory.permit_pre_ready_retry() {
                    record_pre_ready_effect_gate("rejected");
                    return Err(error);
                }
                record_pre_ready_effect_gate("permitted");
                record_pre_ready_replan(reason);
                let waiting_started_at = Instant::now();
                let fresh_topology = self
                    .backend_topology
                    .wait_for_eligible_after(first_revision, retry_deadline)
                    .map_err(|error| {
                        observe_waiting_for_backend(waiting_started_at.elapsed());
                        DistributedQueryError::new(
                            DistributedQueryErrorKind::Failed,
                            error.to_string(),
                        )
                    })?;
                observe_waiting_for_backend(waiting_started_at.elapsed());
                let replan_started_at = Instant::now();
                let replacement_reservation =
                    crate::query_execution::completion::QueryAttemptReservation::retry(
                        query_id, 2,
                    )?;
                let replacement = factory.replan(fresh_topology, replacement_reservation);
                observe_pre_ready_replan(replan_started_at.elapsed());
                let replacement = replacement?;
                let (
                    replacement_request,
                    replacement_completion,
                    replacement_factory,
                    replacement_reservation,
                ) = replacement.into_parts();
                if replacement_factory.is_some() {
                    return Err(DistributedQueryError::new(
                        DistributedQueryErrorKind::ContractViolation,
                        "replacement distributed round must not retain another automatic retry factory",
                    ));
                }
                let replacement_reservation = replacement_reservation.ok_or_else(|| {
                    DistributedQueryError::new(
                        DistributedQueryErrorKind::ContractViolation,
                        "replacement distributed round lost its reserved attempt identity",
                    )
                })?;
                if replacement_reservation.query_id() != query_id
                    || replacement_reservation.execution_id().attempt_id()
                        != execution_id_for_round(query_id, 2)?.attempt_id()
                {
                    return Err(DistributedQueryError::new(
                        DistributedQueryErrorKind::ContractViolation,
                        "replacement distributed round changed its reserved attempt identity",
                    ));
                }
                let replacement_execution_id = replacement_reservation.execution_id();
                self.execute_round(
                    query_id,
                    replacement_execution_id,
                    retry_deadline,
                    replacement_request,
                    None,
                    RoundCredentialLeaseSource::Reservation {
                        reservation: replacement_reservation,
                        observed_collected: None,
                    },
                )
                .and_then(|outcome| replacement_completion.complete(outcome).map_err(failed))
            }
        }
    }

    fn execute_prepared_raw(
        &self,
        operation: crate::query_execution::completion::PreparedRetriableDistributedRequest,
    ) -> Result<DistributedQueryOutcome, DistributedQueryError> {
        let (first_request, mut round_factory, reservation) = operation.into_parts();
        let reservation = match reservation {
            Some(reservation) => reservation,
            None => crate::query_execution::completion::QueryAttemptReservation::first(
                self.query_ids.next_query_id()?,
            )?,
        };
        let query_id = reservation.query_id();
        let first_execution_id = reservation.execution_id();
        let retry_collected_vended_credentials = Arc::new(AtomicBool::new(
            reservation.has_collected_credential_leases(),
        ));
        let first_revision = first_request.topology().revision();
        let retry_deadline = statement_deadline_for_request(&first_request)?;
        match self.execute_round(
            query_id,
            first_execution_id,
            retry_deadline,
            first_request,
            Some(round_factory.as_ref() as &dyn PreReadyRetryBoundary),
            RoundCredentialLeaseSource::Reservation {
                reservation,
                observed_collected: Some(Arc::clone(&retry_collected_vended_credentials)),
            },
        ) {
            Ok(outcome) => Ok(outcome),
            Err(first_error) => {
                if first_error.pre_ready_topology_outcome().is_none() {
                    return Err(first_error);
                }
                if retry_collected_vended_credentials.load(Ordering::Acquire) {
                    return Err(DistributedQueryError::topology_retry_unsupported(
                        first_error
                            .pre_ready_topology_outcome()
                            .expect("pre-ready topology outcome checked above"),
                        "distributed write collected vended credentials and requires fresh whole-round materialization for a topology retry",
                    ));
                }
                let reason = pre_ready_topology_reason(
                    first_error
                        .pre_ready_topology_outcome()
                        .expect("pre-ready topology outcome checked above"),
                );
                if let Err(error) = round_factory.permit_pre_ready_retry() {
                    record_pre_ready_effect_gate("rejected");
                    return Err(error);
                }
                record_pre_ready_effect_gate("permitted");
                record_pre_ready_replan(reason);
                let waiting_started_at = Instant::now();
                let fresh_topology = self
                    .backend_topology
                    .wait_for_eligible_after(first_revision, retry_deadline)
                    .map_err(|error| {
                        observe_waiting_for_backend(waiting_started_at.elapsed());
                        DistributedQueryError::new(
                            DistributedQueryErrorKind::Failed,
                            error.to_string(),
                        )
                    })?;
                observe_waiting_for_backend(waiting_started_at.elapsed());
                let replan_started_at = Instant::now();
                let replacement = round_factory.replan(fresh_topology);
                observe_pre_ready_replan(replan_started_at.elapsed());
                let replacement = replacement?;
                let replacement_execution_id = execution_id_for_round(query_id, 2)?;
                self.execute_round(
                    query_id,
                    replacement_execution_id,
                    retry_deadline,
                    replacement,
                    None,
                    RoundCredentialLeaseSource::Frozen(QueryCredentialLeases::empty()),
                )
            }
        }
    }
}

fn pre_ready_topology_reason(outcome: PreReadyTopologyOutcome) -> &'static str {
    match outcome {
        PreReadyTopologyOutcome::BackendDraining { .. } => "backend_draining",
        PreReadyTopologyOutcome::BackendProcessMismatch { .. } => "backend_process_mismatch",
        PreReadyTopologyOutcome::BackendNotEligible { .. } => "backend_not_eligible",
        PreReadyTopologyOutcome::CompatibilityMismatch { .. } => "compatibility_mismatch",
    }
}

fn fail_closed_one_shot_topology_retry(
    intent: DistributedQueryIntent,
    error: DistributedQueryError,
) -> DistributedQueryError {
    if intent == DistributedQueryIntent::Write
        && let Some(outcome) = error.pre_ready_topology_outcome()
    {
        // A write request is one-shot. Until its DML owner can preserve the
        // exact target/base/publication binding and rebuild the complete write
        // layout under a positive zero-effect permit, it must not reuse this
        // request or silently enter the read-query replan controller.
        return DistributedQueryError::topology_retry_unsupported(
            outcome,
            format!(
                "distributed write cannot retry pre-ready topology change without a whole-round semantic binding and effect-free permit: {}",
                error.message()
            ),
        );
    }
    error
}

/// Establish one absolute execution deadline before the first distributed
/// round.  Every retry consumes this same monotonic budget; it must never
/// inherit a fresh `query_timeout` window merely because the layout changed.
fn statement_deadline_for_request(
    request: &DistributedQueryRequest,
) -> Result<Instant, DistributedQueryError> {
    if let Some(deadline) = request.deadline() {
        return Ok(deadline);
    }
    let timeout_ms = u64::try_from(request.options().timeout_ms().max(1)).map_err(|_| {
        DistributedQueryError::new(
            DistributedQueryErrorKind::ContractViolation,
            "resolved query timeout does not fit an unsigned duration",
        )
    })?;
    Instant::now()
        .checked_add(Duration::from_millis(timeout_ms))
        .ok_or_else(|| {
            DistributedQueryError::new(
                DistributedQueryErrorKind::Failed,
                "query deadline exceeds monotonic clock range",
            )
        })
}

fn failed(message: impl Into<String>) -> DistributedQueryError {
    DistributedQueryError::new(DistributedQueryErrorKind::Failed, message)
}

fn execution_id_for_round(
    query_id: QueryId,
    attempt: u32,
) -> Result<QueryExecutionId, DistributedQueryError> {
    QueryExecutionId::new(
        query_id,
        AttemptId::new(u64::from(attempt)).map_err(|error| {
            DistributedQueryError::new(
                DistributedQueryErrorKind::ContractViolation,
                error.to_string(),
            )
        })?,
    )
    .map_err(|error| {
        DistributedQueryError::new(
            DistributedQueryErrorKind::ContractViolation,
            error.to_string(),
        )
    })
}

/// Snapshot validation runs before the Init + ControlReady boundary.  Only a
/// concrete missing/replaced captured process becomes typed topology evidence;
/// revision-only and availability errors are not guessed into retryability.
fn pre_ready_topology_validation_error(
    error: BackendTopologyValidationError,
) -> DistributedQueryError {
    match error {
        BackendTopologyValidationError::GenerationChanged {
            backend_idx,
            captured_generation,
            ..
        }
        | BackendTopologyValidationError::TargetMissing {
            backend_idx,
            captured_generation,
            ..
        } => DistributedQueryError::pre_ready_topology(
            PreReadyTopologyOutcome::BackendNotEligible {
                backend_idx,
                process_id: captured_generation,
            },
            error.to_string(),
        ),
        other => failed(other.to_string()),
    }
}

/// A lifecycle transport failure remains terminal unless the FE-owned
/// membership authority can independently prove that a captured participant
/// process was replaced or is no longer eligible. This runs before
/// `ControlReady`, after the guarded old round has been aborted, and never
/// derives retryability from transport text.
fn reclassify_pre_ready_lifecycle_failure(
    topology: &dyn BackendTopologyPort,
    captured: &BackendTopologySnapshot,
    original: DistributedQueryError,
    observation_deadline: Instant,
) -> DistributedQueryError {
    if original.pre_ready_topology_outcome().is_some() {
        return original;
    }
    if !original.requires_pre_ready_topology_observation() {
        return original;
    }

    // The failed Init RPC itself is not retry evidence.  It can, however,
    // race the registered replacement's announce/heartbeat publication. Wait
    // for at most the already-budgeted Init RPC window, and only elevate the
    // failure if the membership owner independently proves one of the
    // captured processes became unavailable.
    let mut observed_revision = captured.revision();
    loop {
        let current = match topology.snapshot() {
            Ok(snapshot) => snapshot,
            Err(_) => return original,
        };
        observed_revision = observed_revision.max(current.revision());
        // A transport loss has no BE-provided draining disposition.  Do not
        // reinterpret the transient N-1 snapshot as an intentional scale-down
        // and replan onto it. Explicit `BackendDraining` evidence takes the
        // separate typed path above; this observer waits only for a replacement
        // that restores the captured participation capacity.
        if current.targets().len() < captured.targets().len() {
            if Instant::now() >= observation_deadline {
                return original;
            }
            let snapshot =
                match topology.wait_for_eligible_after(observed_revision, observation_deadline) {
                    Ok(snapshot) => snapshot,
                    Err(_) => return original,
                };
            observed_revision = snapshot.revision();
            continue;
        }
        match topology.validate_snapshot(captured) {
            Err(
                error @ (BackendTopologyValidationError::GenerationChanged { .. }
                | BackendTopologyValidationError::TargetMissing { .. }),
            ) => {
                let observed = pre_ready_topology_validation_error(error);
                let outcome = observed
                    .pre_ready_topology_outcome()
                    .expect("exact topology replacement is typed pre-ready evidence");
                tracing::info!(
                    original_error = %original,
                    observed_topology = %observed,
                    ?outcome,
                    "frontend reclassified a pre-ready lifecycle failure from exact backend topology evidence"
                );
                return DistributedQueryError::pre_ready_topology(
                    outcome,
                    format!(
                        "{original}; observed captured backend topology invalid before ControlReady: {observed}"
                    ),
                );
            }
            Err(BackendTopologyValidationError::ContentChangedWithoutRevision { .. })
            | Err(BackendTopologyValidationError::Unavailable(_)) => return original,
            Err(BackendTopologyValidationError::RevisionChanged {
                current_revision, ..
            }) => observed_revision = current_revision,
            Ok(()) => {}
        }
        if Instant::now() >= observation_deadline {
            return original;
        }
        let snapshot =
            match topology.wait_for_eligible_after(observed_revision, observation_deadline) {
                Ok(snapshot) => snapshot,
                Err(_) => return original,
            };
        observed_revision = snapshot.revision();
    }
}

fn abort_query_lifecycle(
    lease: &mut Option<QueryLifecycleLease>,
    message: impl Into<String>,
) -> String {
    let message = message.into();
    lease
        .take()
        .map_or(message.clone(), |lease| lease.abort_preserving(message))
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;
    use std::net::SocketAddr;
    use std::num::NonZeroUsize;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::{Arc, Mutex};
    use std::time::{Duration, Instant};

    use super::{
        FrontendBackendSnapshot, FrontendDistributedQueryCoordinator, FrontendFragmentScheduler,
        FrontendReportEndpointBinding, QueryIdSource, ReadyLifecycleTransportForTest,
        UniqueQueryIdSource, fail_closed_one_shot_topology_retry,
        pre_ready_topology_validation_error,
    };
    use crate::common::backend_topology::CoordinatorReportEndpointSink;
    use crate::common::backend_topology::{
        BackendTopologyPort, BackendTopologyValidationError, LiveBackendTarget,
    };
    use crate::common::query_cancellation::QueryCancellationSource;
    use crate::connector::{
        FixtureConnectorRegistry, FixtureControlResolver, test_request_context,
    };
    use crate::native::fragment_transport::{
        ExpectedOutputSchemaView, FetchOutcome, FragmentDispatcher,
    };
    use crate::query_execution::completion::{
        PreReadyRetryBoundary, PreparedDistributedQuery, PreparedDistributedRequestFactory,
        PreparedDistributedRoundFactory, PreparedQueryCompletion,
        PreparedRetriableDistributedRequest,
    };
    use crate::query_execution::contract::{
        DistributedQueryCoordinator, DistributedQueryError, DistributedQueryErrorKind,
        DistributedQueryIntent, DistributedQueryRequest, PreReadyTopologyOutcome,
        build_distributed_query_request_with_execution,
    };
    use crate::query_execution::preparation::{ScanPreparationOptions, prepare_fragments};
    use crate::topology::ClusterBackendService;
    use novarocks_proto_codec::lifecycle::QueryControlEndpoint;
    use novarocks_proto_codec::membership::{BackendProcessDescriptor, BackendReportedState};
    use novarocks_sql::test_support::{NativePreparationFixture, native_preparation_plan};
    use novarocks_types::{
        BackendProcessId, ClusterRole, QueryId, QueryProcessNamespace, UniqueId,
    };
    use novarocks_version::native_build_identity;

    #[test]
    fn missing_captured_process_is_typed_pre_ready_not_eligible() {
        let process_id = BackendProcessId::new_v7();
        let error =
            pre_ready_topology_validation_error(BackendTopologyValidationError::TargetMissing {
                backend_idx: 2,
                captured_generation: process_id,
                captured_revision: 7,
                current_revision: 8,
            });
        assert_eq!(
            error.pre_ready_topology_outcome(),
            Some(PreReadyTopologyOutcome::BackendNotEligible {
                backend_idx: 2,
                process_id,
            })
        );
    }

    #[test]
    fn one_shot_write_rejects_pre_ready_topology_without_a_replan_owner() {
        let process_id = BackendProcessId::new_v7();
        let original = DistributedQueryError::pre_ready_topology(
            PreReadyTopologyOutcome::BackendDraining {
                backend_idx: 1,
                process_id,
            },
            "backend is draining",
        );
        let outcome = original
            .pre_ready_topology_outcome()
            .expect("typed outcome is retained");
        let error = fail_closed_one_shot_topology_retry(DistributedQueryIntent::Write, original);

        assert_eq!(
            error.kind(),
            DistributedQueryErrorKind::TopologyRetryUnsupported
        );
        assert_eq!(error.pre_ready_topology_outcome(), Some(outcome));
    }

    #[test]
    fn unique_query_id_source_uses_one_namespace_with_continuous_positive_sequences() {
        let namespace = QueryProcessNamespace::new(0xfedc_ba98_7654_3210);
        let source = UniqueQueryIdSource::new(namespace);

        let first = source.next_query_id().expect("first allocation");
        let second = source.next_query_id().expect("second allocation");

        assert_eq!(
            first
                .process_attribution()
                .expect("first attribution")
                .namespace(),
            namespace
        );
        assert_eq!(
            first
                .process_attribution()
                .expect("first attribution")
                .sequence()
                .get(),
            1
        );
        assert_eq!(
            second
                .process_attribution()
                .expect("second attribution")
                .namespace(),
            namespace
        );
        assert_eq!(
            second
                .process_attribution()
                .expect("second attribution")
                .sequence()
                .get(),
            2
        );
    }

    #[test]
    fn unique_query_id_sources_keep_injected_process_namespaces_distinct() {
        let first = UniqueQueryIdSource::new(QueryProcessNamespace::new(11));
        let second = UniqueQueryIdSource::new(QueryProcessNamespace::new(12));

        let first_id = first.next_query_id().expect("first namespace allocation");
        let second_id = second.next_query_id().expect("second namespace allocation");

        assert_eq!(first_id.low(), second_id.low());
        assert_ne!(first_id.high(), second_id.high());
        assert_ne!(first_id, second_id);
    }

    #[test]
    fn unique_query_id_source_fails_closed_after_sequence_exhaustion() {
        let source = UniqueQueryIdSource::with_last_issued_sequence(
            QueryProcessNamespace::new(13),
            i64::MAX as u64 - 1,
        );

        let final_id = source.next_query_id().expect("final sequence allocation");
        assert_eq!(
            final_id
                .process_attribution()
                .expect("final attribution")
                .sequence()
                .get(),
            i64::MAX as u64
        );
        let error = source.next_query_id().expect_err("exhaustion must fail");
        assert_eq!(error.kind(), DistributedQueryErrorKind::Failed);
        assert_eq!(
            error.message(),
            "frontend query id local sequence is exhausted"
        );
    }

    #[test]
    fn ephemeral_report_endpoint_is_unavailable_until_the_bound_port_is_published() {
        let binding = FrontendReportEndpointBinding::new("frontend.internal".to_string(), 0);

        let error = binding
            .resolve()
            .err()
            .expect("port zero must gate query submission until listener bind");
        assert!(error.message().contains("not bound yet"), "{error}");

        binding.set_bound_port(19070);

        binding
            .resolve()
            .expect("bound port publication makes the DNS endpoint available");
    }

    struct FailingAfterStartDispatcher;

    impl FragmentDispatcher for FailingAfterStartDispatcher {
        fn fetch_result(
            &self,
            _backend_idx: usize,
            _finst_id: UniqueId,
            _max_wait_ms: i64,
            _expected_output_schema: Option<ExpectedOutputSchemaView<'_>>,
        ) -> Result<FetchOutcome, String> {
            Err("test fetch failure after retry stage/start".to_string())
        }

        fn backend_count(&self) -> usize {
            1
        }
    }

    struct RecordingRetryFactory {
        permits: Arc<AtomicUsize>,
        control_ready_closures: Arc<AtomicUsize>,
        stage_or_start_closures: Arc<AtomicUsize>,
        replanned_topologies:
            Arc<Mutex<Vec<crate::common::backend_topology::BackendTopologySnapshot>>>,
    }

    impl PreparedDistributedRoundFactory for RecordingRetryFactory {
        fn replan(
            &mut self,
            topology: crate::common::backend_topology::BackendTopologySnapshot,
            reservation: crate::query_execution::completion::QueryAttemptReservation,
        ) -> Result<PreparedDistributedQuery, DistributedQueryError> {
            self.replanned_topologies
                .lock()
                .expect("replanned topologies")
                .push(topology.clone());
            Ok(PreparedDistributedQuery::new(
                fresh_result_request(topology)?,
                PreparedQueryCompletion::result(),
            )
            .with_attempt_reservation(reservation))
        }
    }

    impl PreparedDistributedRequestFactory for RecordingRetryFactory {
        fn replan(
            &mut self,
            topology: crate::common::backend_topology::BackendTopologySnapshot,
        ) -> Result<DistributedQueryRequest, DistributedQueryError> {
            self.replanned_topologies
                .lock()
                .expect("replanned topologies")
                .push(topology.clone());
            fresh_result_request(topology)
        }
    }

    impl PreReadyRetryBoundary for RecordingRetryFactory {
        fn permit_pre_ready_retry(&self) -> Result<(), DistributedQueryError> {
            self.permits.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }

        fn close_after_control_ready(&self) {
            self.control_ready_closures.fetch_add(1, Ordering::SeqCst);
        }

        fn close_after_stage_or_start(&self) {
            self.stage_or_start_closures.fetch_add(1, Ordering::SeqCst);
        }
    }
    fn descriptor(process_id: BackendProcessId, endpoint: SocketAddr) -> BackendProcessDescriptor {
        BackendProcessDescriptor::new(
            process_id,
            QueryControlEndpoint::new(endpoint.ip().to_string(), endpoint.port())
                .expect("test endpoint"),
            "test-deployment",
            native_build_identity(),
            novarocks_types::NativeCompatibilityId::new([0x71; 32]),
        )
        .expect("test descriptor")
    }

    fn verify(
        topology: &ClusterBackendService,
        descriptor: &BackendProcessDescriptor,
        now_ms: i64,
    ) {
        topology.record_heartbeat_success(
            descriptor.process_id().expect("descriptor process id"),
            descriptor.clone(),
            BackendReportedState::Running,
            2,
            now_ms,
        );
    }

    fn fresh_result_request(
        topology: crate::common::backend_topology::BackendTopologySnapshot,
    ) -> Result<DistributedQueryRequest, DistributedQueryError> {
        let plan = native_preparation_plan(NativePreparationFixture::ResultOutput)
            .expect("sealed result fixture");
        let registry = FixtureConnectorRegistry::new();
        let controls = FixtureControlResolver::new(registry.clone());
        let prepared = prepare_fragments(
            &plan,
            &controls,
            &test_request_context(),
            None,
            None,
            ScanPreparationOptions::single_backend_fixture(),
        )
        .expect("prepared result fixture");
        let native = crate::query_execution::native_fragment::native_fragment_attachment_for_test(
            [novarocks_proto_models::plan::PlanFragment {
                fragment_id: 7,
                // The task protocol refuses a fragment plan with no sink, so a
                // fixture without one would fail at graph assembly and never
                // reach the behaviour these tests are about.
                sink: Some(novarocks_proto_models::plan::DataSink {
                    kind: Some(novarocks_proto_models::plan::data_sink::Kind::Result(true)),
                }),
                ..Default::default()
            }],
            &BTreeSet::from([7]),
            None,
        )
        .expect("native fragment fixture");
        let cancellation = QueryCancellationSource::new();
        let execution = crate::common::admitted_query_context::QueryExecutionContext::new(
            ClusterRole::Fe,
            topology,
            // Short on purpose. These fixtures point at endpoints nothing
            // listens on, so an attempt that reaches the task substrate ends
            // at its own deadline; the budget only has to outlast preparation.
            Some(Instant::now() + Duration::from_secs(1)),
            cancellation.view(),
            novarocks_sql::compiler::SessionOptimizerSettings::default(),
        );
        build_distributed_query_request_with_execution(
            prepared,
            native,
            None,
            DistributedQueryIntent::Result,
            &execution,
        )
    }

    /// The pre-establish replan survived the cutover, with a different trigger.
    ///
    /// The old lifecycle refused Init on a draining backend and that refusal
    /// was what a replan rested on. The task protocol has no such refusal, so
    /// what remains -- and what this asserts -- is the evidence
    /// `reclassify_pre_ready_lifecycle_failure` always required: the
    /// membership owner independently proving that an exact captured process
    /// was replaced. Only the failure that evidence is applied to moved.
    #[test]
    fn a_drain_never_inherits_the_remainder_of_a_long_statement_timeout() {
        // The client-visible answer is linearized before the drain starts, so
        // every moment the drain spends is a moment the caller waits for a
        // result already in hand. A query that finished in a second against
        // one stuck backend must return in about the drain budget, not in five
        // minutes.
        let now = Instant::now();
        let statement_deadline = now + Duration::from_secs(300);
        let budget = Duration::from_secs(15);

        let deadline = super::drain_deadline(statement_deadline, budget, now);
        assert_eq!(
            deadline - now,
            budget,
            "the drain's own budget bounds it, not the statement's remaining time"
        );

        // A statement already near its end has no time left to lend: the drain
        // cannot extend a query past the deadline its caller was promised.
        let nearly_over = now + Duration::from_millis(50);
        assert_eq!(
            super::drain_deadline(nearly_over, budget, now),
            nearly_over,
            "the statement deadline still caps the drain"
        );

        // And an already-expired statement yields no drain time at all rather
        // than wrapping into a long wait.
        let expired = now - Duration::from_secs(1);
        assert!(
            super::drain_deadline(expired, budget, now) <= now,
            "an expired statement leaves the drain no budget"
        );
    }

    #[test]
    fn a_replaced_captured_process_replans_the_round_once_before_establish() {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("test runtime");
        let _runtime_guard = runtime.enter();
        let endpoint: SocketAddr = "127.0.0.1:19041".parse().expect("test endpoint");
        let old = descriptor(BackendProcessId::new_v7(), endpoint);
        let replacement = descriptor(BackendProcessId::new_v7(), endpoint);
        let topology = Arc::new(ClusterBackendService::new_transient_for_test(1));
        topology
            .record_announce(old.clone(), BackendReportedState::Running)
            .expect("initial announce");
        verify(topology.as_ref(), &old, 1);
        let first_snapshot = topology.snapshot().expect("initial topology");
        let old_scheduler = FrontendFragmentScheduler::new(
            FrontendBackendSnapshot::from_live_targets(first_snapshot.targets().to_vec())
                .expect("old scheduler"),
        );

        let replacement_for_scheduler = replacement.clone();
        let replacement_scheduler = FrontendFragmentScheduler::new(
            FrontendBackendSnapshot::from_live_targets(vec![LiveBackendTarget::new(
                0,
                replacement_for_scheduler,
            )])
            .expect("replacement scheduler"),
        );
        let coordinator =
            FrontendDistributedQueryCoordinator::new_for_test_with_backend_sequence_and_topology(
                QueryId::new(7, 11),
                "127.0.0.1:19070".parse().expect("report endpoint"),
                vec![old_scheduler, replacement_scheduler],
                Arc::new(FailingAfterStartDispatcher),
                NonZeroUsize::new(1).expect("nonzero workers"),
                Arc::new(()),
                Arc::new(ReadyLifecycleTransportForTest),
                Arc::clone(&topology) as crate::common::backend_topology::BackendTopologyService,
            );
        // The membership owner replaces the captured process. Published before
        // the round starts because that is the only ordering a test can pin;
        // what matters is that the replan rests on this fact rather than on a
        // transport's own report of it.
        topology
            .record_announce(replacement.clone(), BackendReportedState::Running)
            .expect("replacement announce");
        verify(topology.as_ref(), &replacement, 2);
        let permits = Arc::new(AtomicUsize::new(0));
        let control_ready_closures = Arc::new(AtomicUsize::new(0));
        let stage_or_start_closures = Arc::new(AtomicUsize::new(0));
        let replanned_topologies = Arc::new(Mutex::new(Vec::new()));
        let operation = PreparedDistributedQuery::new(
            fresh_result_request(first_snapshot.clone()).expect("first request"),
            PreparedQueryCompletion::result(),
        )
        .with_round_factory(Box::new(RecordingRetryFactory {
            permits: Arc::clone(&permits),
            control_ready_closures: Arc::clone(&control_ready_closures),
            stage_or_start_closures: Arc::clone(&stage_or_start_closures),
            replanned_topologies: Arc::clone(&replanned_topologies),
        }));

        let error = coordinator
            .execute_prepared(operation)
            .expect_err("the replanned round has no backend to reach");
        // The second round runs on the task substrate against an endpoint
        // nothing listens on, so it ends at its own deadline. What this test
        // is about happened before that: one permit, one replan, onto the
        // process the membership owner named.
        assert!(
            error.message().contains("query timed out after"),
            "actual: {}",
            error.message()
        );
        assert_eq!(permits.load(Ordering::SeqCst), 1);
        // Neither gate may close: no query context was ever established, so
        // the window in which a replan is still legal never ended.
        assert_eq!(control_ready_closures.load(Ordering::SeqCst), 0);
        assert_eq!(stage_or_start_closures.load(Ordering::SeqCst), 0);
        let replanned = replanned_topologies.lock().expect("replanned topologies");
        assert_eq!(replanned.len(), 1);
        assert!(replanned[0].revision() > first_snapshot.revision());
        assert_eq!(
            replanned[0].targets()[0]
                .process_id()
                .expect("replacement process id"),
            replacement.process_id().expect("replacement process id"),
        );
    }

    /// The same replan through the raw entrypoint, which owns the write
    /// outcome boundary.
    ///
    /// Its subject is that a replan does not lose that boundary, not how the
    /// replan was triggered, so it follows the surviving trigger for the same
    /// reason the test above does.
    #[test]
    fn raw_replan_on_a_replaced_process_keeps_the_write_outcome_boundary() {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("test runtime");
        let _runtime_guard = runtime.enter();
        let endpoint: SocketAddr = "127.0.0.1:19043".parse().expect("test endpoint");
        let old = descriptor(BackendProcessId::new_v7(), endpoint);
        let replacement = descriptor(BackendProcessId::new_v7(), endpoint);
        let topology = Arc::new(ClusterBackendService::new_transient_for_test(1));
        topology
            .record_announce(old.clone(), BackendReportedState::Running)
            .expect("initial announce");
        verify(topology.as_ref(), &old, 1);
        let first_snapshot = topology.snapshot().expect("initial topology");
        let old_scheduler = FrontendFragmentScheduler::new(
            FrontendBackendSnapshot::from_live_targets(first_snapshot.targets().to_vec())
                .expect("old scheduler"),
        );
        let replacement_scheduler = FrontendFragmentScheduler::new(
            FrontendBackendSnapshot::from_live_targets(vec![LiveBackendTarget::new(
                0,
                replacement.clone(),
            )])
            .expect("replacement scheduler"),
        );
        let coordinator =
            FrontendDistributedQueryCoordinator::new_for_test_with_backend_sequence_and_topology(
                QueryId::new(7, 13),
                "127.0.0.1:19070".parse().expect("report endpoint"),
                vec![old_scheduler, replacement_scheduler],
                Arc::new(FailingAfterStartDispatcher),
                NonZeroUsize::new(1).expect("nonzero workers"),
                Arc::new(()),
                Arc::new(ReadyLifecycleTransportForTest),
                Arc::clone(&topology) as crate::common::backend_topology::BackendTopologyService,
            );
        topology
            .record_announce(replacement.clone(), BackendReportedState::Running)
            .expect("replacement announce");
        verify(topology.as_ref(), &replacement, 2);
        let permits = Arc::new(AtomicUsize::new(0));
        let control_ready_closures = Arc::new(AtomicUsize::new(0));
        let stage_or_start_closures = Arc::new(AtomicUsize::new(0));
        let replanned_topologies = Arc::new(Mutex::new(Vec::new()));
        let operation = PreparedRetriableDistributedRequest::new(
            fresh_result_request(first_snapshot.clone()).expect("first request"),
            Box::new(RecordingRetryFactory {
                permits: Arc::clone(&permits),
                control_ready_closures: Arc::clone(&control_ready_closures),
                stage_or_start_closures: Arc::clone(&stage_or_start_closures),
                replanned_topologies: Arc::clone(&replanned_topologies),
            }),
        );

        let error = match coordinator.execute_prepared_raw(operation) {
            Ok(_) => panic!("the replanned raw round has no backend to reach"),
            Err(error) => error,
        };
        assert!(
            error.message().contains("query timed out after"),
            "actual: {}",
            error.message()
        );
        assert_eq!(permits.load(Ordering::SeqCst), 1);
        let replanned = replanned_topologies.lock().expect("replanned topologies");
        assert_eq!(replanned.len(), 1);
        assert!(replanned[0].revision() > first_snapshot.revision());
        assert_eq!(
            replanned[0].targets()[0]
                .process_id()
                .expect("replacement process id"),
            replacement.process_id().expect("replacement process id"),
        );
    }

    /// Both gates are evidence, not milestones a code path passes.
    ///
    /// This round reaches the task substrate and never gets an answer from it,
    /// so no query context is established and no task is created. Neither gate
    /// may close: closing one here would end the window in which a replaced
    /// backend can still be replanned onto, on the strength of having asked
    /// rather than having been answered.
    ///
    /// The other half -- that both gates do close once the answers arrive --
    /// is asserted against the state machine itself in
    /// `task_execution::tests::the_two_start_gates_are_observations_of_acknowledgements_not_of_sending`,
    /// because a fixture-injected transport cannot answer the task protocol:
    /// it speaks over a real connection.
    #[test]
    fn the_round_retry_boundary_stays_open_while_no_backend_has_answered() {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("test runtime");
        let _runtime_guard = runtime.enter();
        let endpoint: SocketAddr = "127.0.0.1:19042".parse().expect("test endpoint");
        let descriptor = descriptor(BackendProcessId::new_v7(), endpoint);
        let topology = Arc::new(ClusterBackendService::new_transient_for_test(1));
        topology
            .record_announce(descriptor.clone(), BackendReportedState::Running)
            .expect("initial announce");
        verify(topology.as_ref(), &descriptor, 1);
        let snapshot = topology.snapshot().expect("eligible topology");
        let scheduler = FrontendFragmentScheduler::new(
            FrontendBackendSnapshot::from_live_targets(snapshot.targets().to_vec())
                .expect("scheduler"),
        );
        let coordinator = FrontendDistributedQueryCoordinator::new_for_test_with_topology(
            QueryId::new(7, 12),
            "127.0.0.1:19070".parse().expect("report endpoint"),
            scheduler,
            Arc::new(FailingAfterStartDispatcher),
            NonZeroUsize::new(1).expect("nonzero workers"),
            Arc::new(()),
            Arc::new(ReadyLifecycleTransportForTest),
            Arc::clone(&topology) as crate::common::backend_topology::BackendTopologyService,
        );
        let control_ready_closures = Arc::new(AtomicUsize::new(0));
        let stage_or_start_closures = Arc::new(AtomicUsize::new(0));
        let operation = PreparedDistributedQuery::new(
            fresh_result_request(snapshot).expect("first request"),
            PreparedQueryCompletion::result(),
        )
        .with_round_factory(Box::new(RecordingRetryFactory {
            permits: Arc::new(AtomicUsize::new(0)),
            control_ready_closures: Arc::clone(&control_ready_closures),
            stage_or_start_closures: Arc::clone(&stage_or_start_closures),
            replanned_topologies: Arc::new(Mutex::new(Vec::new())),
        }));

        let error = coordinator
            .execute_prepared(operation)
            .expect_err("the round has no backend to reach");
        assert!(
            error.message().contains("query timed out after"),
            "actual: {}",
            error.message()
        );
        assert_eq!(control_ready_closures.load(Ordering::SeqCst), 0);
        assert_eq!(stage_or_start_closures.load(Ordering::SeqCst), 0);
    }
}

/// How long the coordinator parks when one turn moved nothing at all.
///
/// Only a pacing bound: every wait ends early when the transport wakes the
/// runner, and every deadline is checked before the next turn.
const TASK_ROUND_IDLE_WAIT: Duration = Duration::from_millis(5);

/// The longest one root result poll may block a backend.
///
/// Kept short so the loop keeps turning the state machine while it waits: the
/// same thread owns both, and a poll that parked for the whole statement
/// budget would stop settling acknowledgements and opening edges.
const MAX_ROOT_RESULT_WAIT: Duration = Duration::from_millis(200);

/// Everything both execution paths receive from the shared preamble.
///
/// It exists so the two paths take the same frozen inputs rather than
/// re-deriving any of them: one attempt is scheduled, sealed and budgeted
/// exactly once, whichever path runs it.
struct RoundHandoff<'a> {
    query_id: QueryId,
    execution_id: QueryExecutionId,
    statement_deadline: Instant,
    timeout_ms: i64,
    intent: DistributedQueryIntent,
    /// Only the request fields both paths still need: `artifacts` is consumed
    /// by the preamble that produced `runtime_filter_ready`, so the request
    /// cannot travel whole.
    cancellation: crate::common::query_cancellation::QueryCancellationView,
    completion: crate::query_execution::contract::QueryOutcomeFactory,
    topology: BackendTopologySnapshot,
    statistics_program: Option<crate::query_execution::statistics::StatisticsCollectionProgram>,
    write_stack_session: Option<Arc<crate::query_execution::write_session::ConnectorWriteSession>>,
    backend_services: QueryBackendServices,
    dispatcher: Arc<dyn FragmentDispatcher>,
    retry_boundary: Option<&'a dyn PreReadyRetryBoundary>,
    runtime_filter_ready: RuntimeFilterDeploymentReadyDistributedQuery,
    init_options: QueryInitOptions,
    feedback_declaration:
        crate::runtime_filter::install_encoder::FrontendRuntimeFilterFeedbackDeclaration,
    feedback_state: Arc<RuntimeFilterFeedbackState>,
    split_assignment_plan: Option<RoundSplitAssignmentPlan>,
    scheduled_backend_ownership: Vec<(usize, BackendProcessId)>,
}

/// Hands the substrate whatever split delivery produced, then steps the runner.
///
/// The order is the reason this is one function: a submission recorded before
/// the turn is released by that same turn, while one recorded after it waits a
/// whole turn for no reason. Every submission taken is reported back, because
/// a submission the substrate refused would otherwise keep its sender blocked
/// until the driver's own timeout and then be resent -- a retry of a decision.
fn advance_task_round(
    round: &mut TaskRound,
    split_delivery: &SplitDeliveryBridge,
) -> Result<TurnReport, TaskExecutionError> {
    for pending in split_delivery.take_pending() {
        let delivery = pending.delivery();
        let task = pending.task();
        let admitted = round
            .execution_mut()
            .enqueue_task_update(task, pending.into_update());
        let reported = match &admitted {
            Ok(admission) => Ok(*admission),
            Err(error) => Err(error),
        };
        split_delivery
            .admit(delivery, reported)
            .map_err(|error| TaskExecutionError::Schedule(error.to_string()))?;
        // A refused fact is the frontend's own disagreement about what this
        // task may still be told, so it fails the attempt rather than being
        // dropped after the sender has been told about it.
        admitted?;
    }
    round.turn()
}

/// How one failure of an attempt that has not finished starting is judged.
///
/// Before every query context is established, a failure may be the shadow of a
/// backend that was replaced or went unavailable between the schedule and the
/// establish. The failure's own text never decides that; this only records
/// that the membership owner should be asked, and for how long.
#[derive(Clone, Copy)]
struct TaskRoundFailureClassification<'a> {
    before_contexts_established: bool,
    captured: &'a BackendTopologySnapshot,
    observation_deadline: Instant,
}

/// Whether this task's creation has been acknowledged.
///
/// The root result plane refuses a poll for a task whose creation transaction
/// has not committed, so polling before this is true would turn a normal
/// startup race into a query failure.
fn task_is_created(round: &TaskRound, identity: TaskIdentity) -> bool {
    round
        .execution()
        .task(identity.task_id())
        .is_some_and(|task| matches!(task.state(), RemoteTaskState::Created))
}

/// The wait one root result poll asks for, bounded by the statement deadline.
fn max_root_result_wait(now: Instant, deadline: Instant) -> MaxWait {
    let remaining = deadline.saturating_duration_since(now);
    let wait = remaining
        .min(MAX_ROOT_RESULT_WAIT)
        .max(Duration::from_millis(1));
    // `wait` is already clamped into (0, MAX_ROOT_RESULT_WAIT], which is far
    // inside what `MaxWait` represents, so the fallback is unreachable rather
    // than a silent widening of a wait the deadline had bounded.
    MaxWait::new(wait).unwrap_or_else(|_| MaxWait::default_for(OperationKind::GetFinalTaskInfo))
}

/// Feeds every task's latest status to the write completion tracker.
///
/// Every task, not only the declared writers: a task that reported writer
/// facts without being declared a writer means the declared set is not the
/// real one, and that is a fact only the whole set can show.
fn observe_write_statuses(round: &TaskRound, tracker: &mut WriteCompletionTracker) {
    for stage in round.execution().graph().stages() {
        let Some(execution_stage) = round.execution().stage(stage.stage_id()) else {
            continue;
        };
        for (_, task) in execution_stage.tasks() {
            if let Some(status) = task.status() {
                tracker.observe_status(status);
            }
        }
    }
}

/// Reads the bounded final info of every terminal task of this attempt.
///
/// Final info is observation only: success and failure are decided by
/// `TaskStatus` alone, so a task that cannot answer costs diagnostics and
/// nothing else. That is why a failed read is logged and skipped rather than
/// failing a query that already completed.
struct FinalTaskInfoCollector<'a> {
    transport: &'a dyn TaskResultTransport,
    collected: Vec<FinalTaskInfo>,
    read: BTreeSet<TaskId>,
    enabled: bool,
}

impl<'a> FinalTaskInfoCollector<'a> {
    /// Collects nothing unless this attempt's intent asked for a profile.
    ///
    /// Final info is one extra round trip per task, and only `EXPLAIN ANALYZE`
    /// has anything to do with it.
    const fn new(transport: &'a dyn TaskResultTransport, enabled: bool) -> Self {
        Self {
            transport,
            collected: Vec::new(),
            read: BTreeSet::new(),
            enabled,
        }
    }

    /// Reads the final info of every task that has just become terminal.
    ///
    /// Called on each turn rather than once at the end because releasing a
    /// query context is what lets a backend reclaim its retained terminal
    /// records: a sweep after the drain would find exactly the tasks whose
    /// info it wanted already gone.
    ///
    /// Losing one costs diagnostics and nothing else -- success and failure
    /// are decided by `TaskStatus` alone -- so a failed read is reported and
    /// skipped rather than failing a query that already ran.
    fn observe(&mut self, round: &TaskRound) {
        if !self.enabled {
            return;
        }
        for stage in round.execution().graph().stages() {
            let Some(execution_stage) = round.execution().stage(stage.stage_id()) else {
                continue;
            };
            for (task_id, task) in execution_stage.tasks() {
                if !task.is_terminal() || self.read.contains(task_id) {
                    continue;
                }
                let Some(observed) = task.status() else {
                    continue;
                };
                self.read.insert(*task_id);
                match self.transport.final_task_info(task.identity()) {
                    Ok(FinalTaskInfoRead::Available(info)) => {
                        // A final info that contradicts the terminal already
                        // observed is a protocol conflict, not a profile: two
                        // answers would exist to a question with one answer.
                        match accept_final_info(observed, &info) {
                            Ok(()) => self.collected.push(info),
                            Err(error) => tracing::warn!(
                                task = %task.identity(),
                                error = %error,
                                "final task info disagrees with the observed terminal; \
                                 it is dropped from the profile"
                            ),
                        }
                    }
                    Ok(FinalTaskInfoRead::Unavailable(outcome)) => tracing::debug!(
                        task = %task.identity(),
                        outcome = ?outcome,
                        "final task info is unavailable"
                    ),
                    Err(error) => tracing::warn!(
                        task = %task.identity(),
                        error = %error,
                        "final task info could not be read"
                    ),
                }
            }
        }
    }
}

/// Keeps turning until every task terminated and every context was released.
///
/// Bounded by the statement's own deadline. A drain that does not finish
/// inside it is reported and left to the query execution lease, which is the
/// mechanism that exists for exactly this: it never fails a completion that
/// has already been linearized.
/// When a drain gives up, whichever of its own budget and the statement's
/// deadline runs out first.
///
/// Split out from the drain so the rule is assertable: the client-visible
/// answer is already linearized when this is computed, so a drain that
/// silently inherited the statement deadline would hold the caller for the
/// remainder of a long statement timeout with a finished result in hand.
fn drain_deadline(statement_deadline: Instant, drain_budget: Duration, now: Instant) -> Instant {
    statement_deadline.min(now + drain_budget)
}

fn drain_task_round(
    round: &mut TaskRound,
    split_delivery: &SplitDeliveryBridge,
    wake: &CondvarWake,
    split_assignment: Option<&SplitAssignmentRoundGuard>,
    statement_deadline: Instant,
    drain_budget: Duration,
    execution_id: QueryExecutionId,
    final_task_info: &mut FinalTaskInfoCollector<'_>,
) {
    // The drain gets a budget of its own rather than the statement's. The
    // client-visible answer is already linearized, so every moment spent here
    // is a moment the client waits for a result it could already have had: a
    // query that finished in a second against one stuck backend must not hold
    // its caller for the rest of a five-minute statement timeout.
    //
    // The bound is the transport's own queue-residence budget, not a number
    // invented here. That is how long a released operation may sit before the
    // transport itself calls it lost, so a release still unanswered after it
    // is not going to be answered. The statement deadline still caps it: a
    // statement already past its deadline has no time left to lend.
    let deadline = drain_deadline(statement_deadline, drain_budget, Instant::now());
    loop {
        let split_worker_stopped =
            split_assignment.is_none_or(SplitAssignmentRoundGuard::is_finished);
        if round.attempt_drained() && split_worker_stopped {
            return;
        }
        if Instant::now() >= deadline {
            tracing::warn!(
                execution_id = ?execution_id,
                drained = round.attempt_drained(),
                split_worker_stopped,
                "attempt did not finish draining inside its drain budget; \
                 the query execution lease closes what is left"
            );
            return;
        }
        if let Err(error) = advance_task_round(round, split_delivery) {
            tracing::warn!(
                execution_id = ?execution_id,
                error = %error,
                "attempt could not be drained after its client-visible completion"
            );
            return;
        }
        // Each task's final info is read here, while its context still exists:
        // releasing the context is what lets its backend reclaim the retained
        // terminal record this reads.
        final_task_info.observe(round);
        wake.wait(TASK_ROUND_IDLE_WAIT);
    }
}

/// Stands every query context of a failed attempt down.
///
/// Best effort by construction: the attempt has already failed, so an abort
/// that cannot be sent changes nothing about the query's answer -- it only
/// leaves the backends to their lease. What it must not do is return before
/// the aborts have been released to the transport.
fn abort_task_round(round: &mut TaskRound, reason: &str) {
    let contexts = round
        .execution()
        .graph()
        .contexts()
        .copied()
        .collect::<Vec<_>>();
    for context in contexts {
        if let Err(error) = round
            .execution_mut()
            .abort_context(context, AbortCause::QueryFailed)
        {
            tracing::warn!(
                context = %context,
                error = %error,
                reason,
                "query context could not be aborted after the attempt failed"
            );
        }
    }
}
/// Open one lazy split source per typed connector scan of this round.
///
/// Enumeration itself does not happen here: `get_splits` hands back a source
/// the round pumps. Returning `None` means this query reads nothing through a
/// connector, so no pump thread is started at all.
///
/// The session is minted per round rather than reused from preparation:
/// preparation runs before the execution id exists, so there is no session to
/// inherit, and enumeration must not borrow an identity that named a different
/// attempt.
fn prepare_round_split_assignment(
    artifacts: &PreparedDistributedQuery,
    schedule: &ValidatedFragmentSchedule,
    retry_policy: crate::query_execution::split_assignment::TaskUpdateRetryPolicy,
    feedback: Arc<RuntimeFilterFeedbackState>,
    initial_dynamic_filter_wait_cap: Duration,
) -> Result<Option<RoundSplitAssignmentPlan>, DistributedQueryError> {
    let scan_nodes = artifacts
        .typed_scans()
        .map(|(fragment_id, plan_node_id, _)| (fragment_id, plan_node_id))
        .collect::<Vec<_>>();
    if scan_nodes.is_empty() {
        return Ok(None);
    }
    let session = crate::query_execution::compiler::typed_connector_session().map_err(failed)?;
    let mut sources = Vec::with_capacity(scan_nodes.len());
    for (_, plan_node_id, scan) in artifacts.typed_scans() {
        let table_scan = &scan.prepared.table_scan;
        let source = scan
            .prepared
            .split_manager
            .get_splits(
                &session,
                table_scan.table().relation().table(),
                table_scan.assignments(),
                &table_scan.dynamic_filter_columns(),
                &scan.prepared.constraint,
            )
            .map_err(|error| {
                failed(format!(
                    "typed connector scan node_id={plan_node_id} cannot open its split source: {error}"
                ))
            })?;
        sources.push(RoundSplitSource {
            plan_node_id,
            source,
            encoder: Arc::clone(&scan.prepared.encoder),
            feedback: Arc::clone(&feedback),
            feedback_bindings: feedback_bindings(table_scan),
            initial_wait_deadline: None,
        });
    }
    let targets = assignment_targets(schedule, &scan_nodes);
    // Every scan node must have somewhere to send its work. An empty task set
    // would silently drop every split of that scan.
    for plan_node_id in sources.iter().map(|source| source.plan_node_id) {
        if targets
            .get(&plan_node_id)
            .is_none_or(|targets| targets.is_empty())
        {
            return Err(failed(format!(
                "typed connector scan node_id={plan_node_id} has no admitted task in this schedule"
            )));
        }
    }
    Ok(Some(RoundSplitAssignmentPlan::new(
        targets,
        sources,
        retry_policy,
        initial_dynamic_filter_wait_cap,
        assignment_endpoints(schedule),
    )))
}

fn feedback_bindings(
    table_scan: &crate::query_execution::connector_domain::TableScanNode,
) -> Vec<(
    u32,
    novarocks_spi::connector::read_stack::ConnectorReadColumnHandle,
)> {
    table_scan
        .dynamic_filters()
        .iter()
        .filter_map(|binding| {
            table_scan
                .assignments()
                .iter()
                .find(|assignment| assignment.variable() == binding.variable())
                .map(|assignment| (binding.filter_id(), assignment.column().clone()))
        })
        .collect()
}
