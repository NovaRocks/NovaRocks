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
use std::net::SocketAddr;
use std::num::NonZeroUsize;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::{AtomicI64, AtomicU16, Ordering};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use novarocks::query_execution::ConnectorWriteCompletion;
use novarocks::query_execution::artifact::{
    ConnectorBindingDispatcher, ConnectorBindingInstallObserver,
    DispatchingConnectorBindingBarrier, RunningNativeExecutionParts,
    new_grpc_connector_binding_dispatcher,
};
use novarocks::query_execution::backend::LiveBackendTarget;
use novarocks::query_execution::contract::{
    ConnectorWriteOperationRegistration, DistributedQueryCoordinator, DistributedQueryError,
    DistributedQueryErrorKind, DistributedQueryIntent, DistributedQueryOutcome,
    DistributedQueryRequest, ProfileTerminalBuilder,
};
use novarocks::query_execution::fragment_transport::{
    FetchOutcome, FragmentDispatcher, new_grpc_fragment_dispatcher,
};
use novarocks::query_execution::lifecycle::{
    AttemptId, QueryExecutionId, QueryInitOptions, QueryLifecycleTransport,
};
use novarocks::query_execution::write::WriteTerminalBuilder;
use novarocks::query_execution::write_operation::ConnectorWriteOperationSession;
use novarocks::service::grpc_query_lifecycle_client::new_grpc_query_lifecycle_transport;
use novarocks_spi::connector::ConnectorWriteResolver;
use novarocks_types::QueryId;

use super::backend_events::BackendQueryActivity;
use super::query_lifecycle::{FrontendQueryLifecycleBarrier, FrontendQueryLifecycleConfig};
use super::query_registry::FrontendQueryRegistry;
use super::report::FrontendCoordinatorTerminalIngress;
use super::scheduler::{FrontendBackendSnapshot, FrontendFragmentScheduler};
use crate::connector::{
    ConnectorControlHost, ConnectorControlRetirement, ConnectorControlRetirementSink,
};

trait QueryIdSource: Send + Sync + 'static {
    fn next_query_id(&self) -> QueryId;
}

struct UniqueQueryIdSource {
    next_low: AtomicI64,
}

impl Default for UniqueQueryIdSource {
    fn default() -> Self {
        Self {
            next_low: AtomicI64::new(100),
        }
    }
}

impl QueryIdSource for UniqueQueryIdSource {
    fn next_query_id(&self) -> QueryId {
        let (high, _) = uuid::Uuid::new_v4().as_u64_pair();
        QueryId::new(
            high as i64,
            self.next_low.fetch_add(1_000, Ordering::Relaxed),
        )
    }
}

#[cfg(test)]
struct FixedQueryIdSource(QueryId);

#[cfg(test)]
impl QueryIdSource for FixedQueryIdSource {
    fn next_query_id(&self) -> QueryId {
        self.0
    }
}

#[cfg(test)]
struct TestConnectorBindingDispatcher;

#[cfg(test)]
impl ConnectorBindingDispatcher for TestConnectorBindingDispatcher {
    fn install(
        &self,
        _execution_id: QueryExecutionId,
        _backend_idx: usize,
        _endpoint: SocketAddr,
        _declaration: &novarocks_spi::connector::ConnectorExecutionDeclaration,
    ) -> Result<(), String> {
        Ok(())
    }

    fn retire(
        &self,
        _endpoint: SocketAddr,
        _key: &novarocks_spi::connector::ConnectorExecutionBindingKey,
    ) -> Result<(), String> {
        Ok(())
    }
}

struct FrontendConnectorBindingInstallObserver {
    control: Arc<ConnectorControlHost>,
}

struct GrpcConnectorControlRetirementSink;

impl ConnectorControlRetirementSink for GrpcConnectorControlRetirementSink {
    fn retire(&self, retirement: ConnectorControlRetirement) {
        let endpoints = retirement
            .installed_backends
            .iter()
            .enumerate()
            .filter_map(|(index, endpoint)| match endpoint.parse::<SocketAddr>() {
                Ok(endpoint) => Some((index, endpoint)),
                Err(error) => {
                    tracing::warn!(
                        instance_id = %retirement.key.instance_id.as_str(),
                        incarnation = ?retirement.key.incarnation,
                        endpoint = %endpoint,
                        %error,
                        "connector control retirement skipped an invalid recorded backend endpoint"
                    );
                    None
                }
            })
            .collect::<Vec<_>>();
        let dispatcher = match new_grpc_connector_binding_dispatcher(&endpoints) {
            Ok(dispatcher) => dispatcher,
            Err(error) => {
                tracing::warn!(
                    instance_id = %retirement.key.instance_id.as_str(),
                    incarnation = ?retirement.key.incarnation,
                    %error,
                    "connector execution retirement dispatcher could not be composed"
                );
                return;
            }
        };
        for (_, endpoint) in endpoints {
            if let Err(error) = dispatcher.retire(endpoint, &retirement.key) {
                tracing::warn!(
                    instance_id = %retirement.key.instance_id.as_str(),
                    incarnation = ?retirement.key.incarnation,
                    %endpoint,
                    %error,
                    "connector execution binding retirement was not acknowledged"
                );
            }
        }
    }
}

impl ConnectorBindingInstallObserver for FrontendConnectorBindingInstallObserver {
    fn installed(
        &self,
        endpoint: std::net::SocketAddr,
        declaration: &novarocks_spi::connector::ConnectorExecutionDeclaration,
    ) -> Result<(), String> {
        self.control
            .record_installed_backend(&declaration.binding_key(), endpoint.to_string())
            .map_err(|error| error.to_string())
    }
}

pub(crate) struct FrontendLiveBackendTopology {
    state: Mutex<FrontendLiveBackendTopologyState>,
}

struct FrontendLiveBackendTopologyState {
    revision: u64,
    live: Vec<LiveBackendTarget>,
}

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
    fn from_socket_addr(endpoint: SocketAddr) -> Self {
        Self::new(endpoint.ip().to_string(), endpoint.port())
    }

    fn resolve(
        &self,
    ) -> Result<novarocks::query_execution::backend::CoordinatorReportEndpoint, DistributedQueryError>
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
        novarocks::query_execution::backend::CoordinatorReportEndpoint::new(
            self.advertised_host.clone(),
            port,
        )
        .map_err(failed)
    }
}

impl novarocks::query_execution::backend::CoordinatorReportEndpointSink
    for FrontendReportEndpointBinding
{
    fn set_bound_port(&self, port: u16) {
        self.bound_port.store(port, Ordering::Release);
    }
}

#[cfg(test)]
enum BackendServicesSource {
    Fixed {
        scheduler: FrontendFragmentScheduler,
        dispatcher: Arc<dyn FragmentDispatcher>,
        lifecycle_transport: Arc<dyn QueryLifecycleTransport>,
        connector_binding_dispatcher: Arc<dyn ConnectorBindingDispatcher>,
    },
    #[cfg(test)]
    Sequence {
        schedulers: Mutex<VecDeque<FrontendFragmentScheduler>>,
        dispatcher: Arc<dyn FragmentDispatcher>,
        lifecycle_transport: Arc<dyn QueryLifecycleTransport>,
        connector_binding_dispatcher: Arc<dyn ConnectorBindingDispatcher>,
    },
}

struct QueryBackendServices {
    scheduler: FrontendFragmentScheduler,
    dispatcher: Arc<dyn FragmentDispatcher>,
    lifecycle_transport: Arc<dyn QueryLifecycleTransport>,
    live_backends: Vec<LiveBackendTarget>,
    connector_binding_dispatcher: Arc<dyn ConnectorBindingDispatcher>,
}

#[cfg(test)]
pub(crate) fn ready_lifecycle_transport_for_test() -> Arc<dyn QueryLifecycleTransport> {
    Arc::new(ReadyLifecycleTransportForTest)
}

#[cfg(test)]
struct ReadyLifecycleTransportForTest;

#[cfg(test)]
struct ReadyLifecycleSessionForTest {
    events: Mutex<VecDeque<novarocks::query_execution::lifecycle::QueryControlEvent>>,
}

#[cfg(test)]
impl novarocks::query_execution::lifecycle::QueryControlSession for ReadyLifecycleSessionForTest {
    fn send(
        &self,
        command: novarocks::query_execution::lifecycle::QueryControlCommand,
    ) -> Result<(), novarocks::query_execution::lifecycle::QueryLifecycleTransportError> {
        use novarocks::query_execution::lifecycle::{
            QueryControlCommand, QueryControlEvent, QueryTerminationReason,
        };
        let event = match command {
            QueryControlCommand::Heartbeat { sequence, .. } => {
                QueryControlEvent::HeartbeatAck { sequence }
            }
            QueryControlCommand::Abort { .. } => QueryControlEvent::TerminationAccepted {
                reason: QueryTerminationReason::CoordinatorAbort,
            },
            QueryControlCommand::Finalize => QueryControlEvent::TerminationAccepted {
                reason: QueryTerminationReason::CoordinatorFinalize,
            },
            QueryControlCommand::TerminalAck { .. } => {
                return Ok(());
            }
        };
        self.events
            .lock()
            .expect("ready lifecycle session")
            .push_back(event);
        Ok(())
    }

    fn recv_timeout(
        &self,
        _timeout: Duration,
    ) -> Result<
        novarocks::query_execution::lifecycle::QueryControlEvent,
        novarocks::query_execution::lifecycle::QueryLifecycleTransportError,
    > {
        self.events
            .lock()
            .expect("ready lifecycle session")
            .pop_front()
            .ok_or_else(|| {
                novarocks::query_execution::lifecycle::QueryLifecycleTransportError::new(
                    novarocks::query_execution::lifecycle::QueryLifecycleTransportErrorKind::DeadlineExceeded,
                    "ready lifecycle session has no pending event",
                )
            })
    }
}

#[cfg(test)]
impl QueryLifecycleTransport for ReadyLifecycleTransportForTest {
    fn init_query(
        &self,
        _target: novarocks::query_execution::lifecycle::QueryLifecycleTarget,
        request: novarocks::query_execution::lifecycle::QueryInitRequest,
        _timeout: Duration,
    ) -> Result<
        novarocks::query_execution::lifecycle::QueryInitAck,
        novarocks::query_execution::lifecycle::QueryLifecycleTransportError,
    > {
        Ok(novarocks::query_execution::lifecycle::QueryInitAck::new(
            request.manifest().execution_id(),
            request.digest(),
            novarocks::query_execution::lifecycle::QueryInitOutcome::Applied,
        ))
    }

    fn attach_control(
        &self,
        _target: novarocks::query_execution::lifecycle::QueryLifecycleTarget,
        _attach: novarocks::query_execution::lifecycle::QueryControlAttach,
        _timeout: Duration,
    ) -> Result<
        Arc<dyn novarocks::query_execution::lifecycle::QueryControlSession>,
        novarocks::query_execution::lifecycle::QueryLifecycleTransportError,
    > {
        Ok(Arc::new(ReadyLifecycleSessionForTest {
            events: Mutex::new(VecDeque::from([
                novarocks::query_execution::lifecycle::QueryControlEvent::ControlReady,
            ])),
        }))
    }

    fn stage_fragments(
        &self,
        _target: novarocks::query_execution::lifecycle::QueryLifecycleTarget,
        request: &novarocks::query_execution::lifecycle::QueryStageRequest,
        _timeout: Duration,
    ) -> Result<
        novarocks::query_execution::lifecycle::QueryStageAck,
        novarocks::query_execution::lifecycle::QueryLifecycleTransportError,
    > {
        Ok(novarocks::query_execution::lifecycle::QueryStageAck::new(
            request.execution_id(),
            request.digest_version(),
            request.digest(),
            novarocks::query_execution::lifecycle::QueryStageOutcome::Applied,
            "test participant staged",
        ))
    }

    fn start_prepared_query(
        &self,
        _target: novarocks::query_execution::lifecycle::QueryLifecycleTarget,
        request: &novarocks::query_execution::lifecycle::QueryStartRequest,
        _timeout: Duration,
    ) -> Result<
        novarocks::query_execution::lifecycle::QueryStartAck,
        novarocks::query_execution::lifecycle::QueryLifecycleTransportError,
    > {
        Ok(novarocks::query_execution::lifecycle::QueryStartAck::new(
            request.execution_id(),
            request.digest_version(),
            request.digest(),
            novarocks::query_execution::lifecycle::QueryStartOutcome::Applied,
            "test participant started",
        ))
    }

    fn abort_query(
        &self,
        _target: novarocks::query_execution::lifecycle::QueryLifecycleTarget,
        request: novarocks::query_execution::lifecycle::QueryAbortRequest,
        _timeout: Duration,
    ) -> Result<
        novarocks::query_execution::lifecycle::QueryTerminationAck,
        novarocks::query_execution::lifecycle::QueryLifecycleTransportError,
    > {
        Ok(
            novarocks::query_execution::lifecycle::QueryTerminationAck::new(
                request.execution_id(),
                novarocks::query_execution::lifecycle::QueryTerminationReason::CoordinatorAbort,
            ),
        )
    }
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
                connector_binding_dispatcher,
            } => Ok(QueryBackendServices {
                scheduler: scheduler.clone(),
                dispatcher: Arc::clone(dispatcher),
                lifecycle_transport: Arc::clone(lifecycle_transport),
                live_backends: topology.to_vec(),
                connector_binding_dispatcher: Arc::clone(connector_binding_dispatcher),
            }),
            #[cfg(test)]
            Self::Sequence {
                schedulers,
                dispatcher,
                lifecycle_transport,
                connector_binding_dispatcher,
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
                    connector_binding_dispatcher: Arc::clone(connector_binding_dispatcher),
                })
            }
        }
    }
}

fn production_backend_services(
    topology: &[LiveBackendTarget],
) -> Result<QueryBackendServices, DistributedQueryError> {
    let entries = topology
        .iter()
        .map(|target| (target.backend_idx(), target.endpoint()))
        .collect::<Vec<_>>();
    let snapshot = FrontendBackendSnapshot::from_live_targets(topology.to_vec())?;
    Ok(QueryBackendServices {
        scheduler: FrontendFragmentScheduler::new(snapshot),
        dispatcher: new_grpc_fragment_dispatcher(&entries).map_err(failed)?,
        lifecycle_transport: new_grpc_query_lifecycle_transport(topology).map_err(failed)?,
        live_backends: topology.to_vec(),
        connector_binding_dispatcher: new_grpc_connector_binding_dispatcher(&entries)
            .map_err(failed)?,
    })
}

pub struct FrontendDistributedQueryCoordinator {
    report_endpoint: Arc<FrontendReportEndpointBinding>,
    backend_topology: novarocks::query_execution::backend::BackendTopologyService,
    #[cfg(test)]
    backend_services: Option<BackendServicesSource>,
    runtime_filter_worker_count: NonZeroUsize,
    query_ids: Arc<dyn QueryIdSource>,
    registry: Arc<FrontendQueryRegistry>,
    connector_control: Arc<ConnectorControlHost>,
}

impl FrontendDistributedQueryCoordinator {
    pub fn new(
        advertised_report_host: String,
        configured_report_port: u16,
        runtime_filter_worker_count: NonZeroUsize,
        backend_topology: novarocks::query_execution::backend::BackendTopologyService,
        connector_control: Arc<ConnectorControlHost>,
    ) -> Self {
        connector_control.set_retirement_sink(Arc::new(GrpcConnectorControlRetirementSink));
        Self {
            report_endpoint: Arc::new(FrontendReportEndpointBinding::new(
                advertised_report_host,
                configured_report_port,
            )),
            backend_topology,
            #[cfg(test)]
            backend_services: None,
            runtime_filter_worker_count,
            query_ids: Arc::new(UniqueQueryIdSource::default()),
            registry: Arc::new(FrontendQueryRegistry::default()),
            connector_control,
        }
    }

    #[cfg(test)]
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
    pub(crate) fn new_for_test_with_topology(
        query_id: QueryId,
        report_endpoint: SocketAddr,
        scheduler: FrontendFragmentScheduler,
        dispatcher: Arc<dyn FragmentDispatcher>,
        runtime_filter_worker_count: NonZeroUsize,
        _test_fixture: Arc<dyn std::any::Any + Send + Sync>,
        lifecycle_transport: Arc<dyn QueryLifecycleTransport>,
        backend_topology: novarocks::query_execution::backend::BackendTopologyService,
    ) -> Self {
        Self {
            report_endpoint: Arc::new(FrontendReportEndpointBinding::from_socket_addr(
                report_endpoint,
            )),
            backend_topology,
            backend_services: Some(BackendServicesSource::Fixed {
                scheduler,
                dispatcher,
                lifecycle_transport,
                connector_binding_dispatcher: Arc::new(TestConnectorBindingDispatcher),
            }),
            runtime_filter_worker_count,
            query_ids: Arc::new(FixedQueryIdSource(query_id)),
            registry: Arc::new(FrontendQueryRegistry::default()),
            connector_control: Arc::new(ConnectorControlHost::new()),
        }
    }

    #[cfg(test)]
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
        Self {
            report_endpoint: Arc::new(FrontendReportEndpointBinding::from_socket_addr(
                report_endpoint,
            )),
            backend_topology: Arc::new(
                crate::topology::ClusterBackendService::from_captured_targets_for_test(&targets),
            ),
            backend_services: Some(BackendServicesSource::Sequence {
                schedulers: Mutex::new(schedulers.into()),
                dispatcher,
                lifecycle_transport,
                connector_binding_dispatcher: Arc::new(TestConnectorBindingDispatcher),
            }),
            runtime_filter_worker_count,
            query_ids: Arc::new(FixedQueryIdSource(query_id)),
            registry: Arc::new(FrontendQueryRegistry::default()),
            connector_control: Arc::new(ConnectorControlHost::new()),
        }
    }

    pub fn terminal_ingress(&self) -> FrontendCoordinatorTerminalIngress {
        FrontendCoordinatorTerminalIngress::new(Arc::clone(&self.registry))
    }

    pub fn backend_query_activity(&self) -> BackendQueryActivity {
        BackendQueryActivity::new(Arc::clone(&self.registry))
    }

    pub fn report_endpoint_sink(
        &self,
    ) -> Arc<dyn novarocks::query_execution::backend::CoordinatorReportEndpointSink> {
        self.report_endpoint.clone()
    }

    pub fn execute(
        &self,
        request: DistributedQueryRequest,
    ) -> Result<DistributedQueryOutcome, DistributedQueryError> {
        self.execute_request(request)
    }

    fn dispatch_ready_connector_retires(&self, dispatcher: &dyn ConnectorBindingDispatcher) {
        let ready = match self.connector_control.take_ready_retires() {
            Ok(ready) => ready,
            Err(error) => {
                tracing::warn!(error = %error, "connector control retirement queue is unavailable");
                return;
            }
        };
        for retirement in ready {
            for endpoint in retirement.installed_backends {
                let endpoint = match endpoint.parse::<SocketAddr>() {
                    Ok(endpoint) => endpoint,
                    Err(error) => {
                        tracing::warn!(
                            instance_id = %retirement.key.instance_id.as_str(),
                            incarnation = ?retirement.key.incarnation,
                            endpoint = %endpoint,
                            %error,
                            "connector control retirement skipped an invalid recorded backend endpoint"
                        );
                        continue;
                    }
                };
                if let Err(error) = dispatcher.retire(endpoint, &retirement.key) {
                    tracing::warn!(
                        instance_id = %retirement.key.instance_id.as_str(),
                        incarnation = ?retirement.key.incarnation,
                        %endpoint,
                        %error,
                        "connector execution binding retirement was not acknowledged"
                    );
                }
            }
        }
    }

    fn execute_request(
        &self,
        request: DistributedQueryRequest,
    ) -> Result<DistributedQueryOutcome, DistributedQueryError> {
        let query_id = self.query_ids.next_query_id();
        let execution_id = QueryExecutionId::new(
            query_id,
            AttemptId::new(1).expect("the initial query attempt is nonzero"),
        )
        .map_err(|error| {
            DistributedQueryError::new(
                DistributedQueryErrorKind::ContractViolation,
                error.to_string(),
            )
        })?;
        let parts = request.into_parts();
        let connector_write_session = parts
            .connector_write
            .as_ref()
            .map(|registration| registration.session().clone());
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
            .map_err(|error| failed(error.to_string()))?;
        #[cfg(test)]
        let backend_services = match &self.backend_services {
            Some(services) => services.resolve(parts.topology.targets())?,
            None => production_backend_services(parts.topology.targets())?,
        };
        #[cfg(not(test))]
        let backend_services = production_backend_services(parts.topology.targets())?;
        self.dispatch_ready_connector_retires(
            backend_services.connector_binding_dispatcher.as_ref(),
        );
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
            .map_err(|error| failed(error.to_string()))?;
        self.registry
            .set_scheduled_backend_ownership(query_id, &scheduled_backend_ownership)?;
        let scheduled = parts.artifacts.bind_schedule(schedule)?;
        let scheduled = match parts.connector_write {
            Some(registration) => {
                let session = registration.session();
                let manifest = scheduled.freeze_connector_write_manifest(
                    &scheduled.terminal_write_fragment_ids(),
                    session.operation_id(),
                    registration.cohort_id(),
                    session.owner().clone(),
                )?;
                let attachment = session
                    .plan_manifest(&manifest)
                    .map_err(|error| failed(format!("plan connector writer manifest: {error}")))?;
                scheduled.attach_connector_write_plan(attachment)?
            }
            None => scheduled,
        };
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
        let query_deadline_unix_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|error| failed(format!("system clock precedes Unix epoch: {error}")))?
            .as_millis()
            .saturating_add(u128::from(timeout_ms.max(1) as u64))
            .try_into()
            .map_err(|_| failed("query deadline exceeds u64 milliseconds"))?;
        let runtime = &novarocks::common::app_config::config()
            .map_err(|error| failed(format!("load query lifecycle config: {error}")))?
            .runtime;
        let lifecycle_config = FrontendQueryLifecycleConfig::new(
            Duration::from_millis(runtime.query_control_heartbeat_interval_ms),
            Duration::from_millis(runtime.query_control_heartbeat_timeout_ms),
            Duration::from_millis(runtime.query_control_init_rpc_timeout_ms),
            Duration::from_millis(runtime.query_control_attach_timeout_ms),
        )?
        .with_stage_start_timeouts(
            Duration::from_millis(runtime.query_control_stage_rpc_timeout_ms),
            Duration::from_millis(runtime.query_control_start_rpc_timeout_ms),
        )?
        .with_terminal_timeouts(
            Duration::from_millis(runtime.query_control_terminal_drain_timeout_ms),
            Duration::from_millis(runtime.query_control_terminal_ack_timeout_ms),
        )?;
        let lifecycle_barrier = FrontendQueryLifecycleBarrier::new(
            Arc::clone(&backend_services.lifecycle_transport),
            Arc::clone(&self.registry),
            lifecycle_config,
        )
        .with_cancellation(parts.cancellation.clone());
        let init_options = QueryInitOptions::new(
            execution_id,
            backend_services.live_backends,
            self.runtime_filter_worker_count.get(),
            parts.options.runtime_filter_lifecycle(),
            &parts.options,
            query_deadline_unix_ms,
            Duration::from_millis(runtime.query_control_pre_start_timeout_ms),
            self.report_endpoint.resolve()?,
        )?;
        let connector_binding_dispatcher =
            Arc::clone(&backend_services.connector_binding_dispatcher);
        let connector_bindings = DispatchingConnectorBindingBarrier::with_observer(
            Arc::clone(&connector_binding_dispatcher),
            Arc::new(FrontendConnectorBindingInstallObserver {
                control: Arc::clone(&self.connector_control),
            }),
        );
        let stage_prepared = scheduled
            .initialize_query(init_options, &lifecycle_barrier)?
            .prepare_connector_bindings(&connector_bindings)?
            .prepare_stage()?;
        self.dispatch_ready_connector_retires(connector_binding_dispatcher.as_ref());
        let staged = stage_prepared.stage(&lifecycle_barrier)?;
        for batch in staged.batches() {
            self.backend_topology.record_successful_stage(
                batch.binding().target().backend_idx(),
                batch.request().fragments().len(),
            );
        }
        let execution = staged.start(&lifecycle_barrier)?;
        let RunningNativeExecutionParts {
            root_fetch,
            writer_registrations,
            expected_output,
            query_lifecycle_lease,
            connector_binding_lease: _connector_binding_lease,
            connector_write_plan,
        } = execution.into_parts();
        let mut query_lifecycle_lease = Some(query_lifecycle_lease);
        if let Some(message) = self.registry.first_failure(query_id)
            && intent != DistributedQueryIntent::Write
        {
            let message = abort_query_lifecycle(&mut query_lifecycle_lease, message);
            return Err(failed(message));
        }

        let deadline = Instant::now() + Duration::from_millis(timeout_ms as u64);
        let mut batches = Vec::new();
        if root_fetch.uses_result_buffer() {
            loop {
                if parts.cancellation.is_cancelled() {
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
                    FetchOutcome::Eof => break,
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

        if parts.cancellation.is_cancelled() {
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

        let terminal_set = match query_lifecycle_lease
            .take()
            .expect("query lifecycle lease is present through query completion")
            .finalize()
        {
            Ok(terminal_set) => terminal_set,
            Err(error) => return Err(error),
        };
        if !terminal_set.is_success() {
            return Err(failed(
                "query terminal snapshot set contains a failed, cancelled, or incomplete fragment",
            ));
        }

        let query_failure = self.registry.first_failure(query_id);
        let outcome = (|| match intent {
            DistributedQueryIntent::Result => parts
                .completion
                .result(expected_output.into_query_result(batches)?),
            DistributedQueryIntent::Write => {
                let result = expected_output.into_query_result(batches)?;
                let mut builder = WriteTerminalBuilder::new(writer_registrations)?;
                if let Some(message) = query_failure {
                    builder.latch_failure(message);
                }
                for fragment in terminal_set.fragments() {
                    builder.apply_terminal(fragment)?;
                }
                let report_outcome = builder.finish()?;
                let (commit, abort) = report_outcome.into_payloads();
                let connector_completion = match (
                    connector_write_session,
                    connector_write_plan,
                    commit.as_ref(),
                ) {
                    (Some(session), Some(attachment), Some(commit)) => Some(
                        ConnectorWriteCompletion::from_write_commit(session, attachment, commit)?,
                    ),
                    (Some(_), Some(_), None) => {
                        return Err(DistributedQueryError::new(
                            DistributedQueryErrorKind::ContractViolation,
                            "connector write execution ended without a complete staged-report commit",
                        ));
                    }
                    (None, None, _) => None,
                    _ => {
                        return Err(DistributedQueryError::new(
                            DistributedQueryErrorKind::ContractViolation,
                            "connector write operation session and planned attachment disagree",
                        ));
                    }
                };
                parts
                    .completion
                    .write_with_connector(result, commit, abort, connector_completion)
            }
            DistributedQueryIntent::Profile => {
                let result = expected_output.into_query_result(batches)?;
                let mut builder = ProfileTerminalBuilder::new();
                for fragment in terminal_set.fragments() {
                    builder.apply_terminal(fragment)?;
                }
                parts.completion.profile(result, builder.finish())
            }
            DistributedQueryIntent::Statistics => {
                let program = parts.statistics_program.as_ref().ok_or_else(|| {
                    DistributedQueryError::new(
                        DistributedQueryErrorKind::ContractViolation,
                        "statistics execution lost its typed collection program",
                    )
                })?;
                let result = program.finish_fragment_payloads(
                    terminal_set
                        .fragments()
                        .map(|fragment| fragment.statistics_payload()),
                )?;
                parts.completion.statistics(program, result)
            }
        })();
        if let Err(error) = &outcome {
            let _ = self
                .registry
                .latch_failure_and_cancel(query_id, error.message().to_string());
            return Err(DistributedQueryError::new(error.kind(), error.message()));
        }
        outcome
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
        lease: &mut Option<novarocks::query_execution::lifecycle::QueryLifecycleLease>,
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
    fn begin_write_operation(
        &self,
        registration: ConnectorWriteOperationRegistration,
    ) -> Result<ConnectorWriteOperationSession, DistributedQueryError> {
        let lease = self
            .connector_control
            .acquire_current_write(registration.connector_instance_id())
            .map_err(|error| failed(format!("acquire connector write operation lease: {error}")))?;
        ConnectorWriteOperationSession::try_begin(registration, lease)
            .map_err(|error| failed(format!("seal connector write operation cohorts: {error}")))
    }

    fn execute(
        &self,
        request: DistributedQueryRequest,
    ) -> Result<DistributedQueryOutcome, DistributedQueryError> {
        self.execute_request(request)
    }
}

fn failed(message: impl Into<String>) -> DistributedQueryError {
    DistributedQueryError::new(DistributedQueryErrorKind::Failed, message)
}

fn abort_query_lifecycle(
    lease: &mut Option<novarocks::query_execution::lifecycle::QueryLifecycleLease>,
    message: impl Into<String>,
) -> String {
    let message = message.into();
    lease
        .take()
        .map_or(message.clone(), |lease| lease.abort_preserving(message))
}

#[cfg(test)]
mod tests {
    use super::FrontendReportEndpointBinding;
    use novarocks::query_execution::backend::CoordinatorReportEndpointSink;

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
}
