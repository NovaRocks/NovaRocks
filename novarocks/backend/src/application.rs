use std::fmt;
use std::future::Future;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, mpsc};
use std::time::Duration;

use tokio::sync::watch;

use crate::drain::BackendDrainState;
use novarocks_connector_binding::ConnectorExecutionRoleBindingFactory;
use novarocks_execution::runtime::execution_runtime::{ExecutionRuntime, ExecutionRuntimeConfig};
use novarocks_native_trust::NativeTrust;
use novarocks_proto_codec::lifecycle::QueryControlEndpoint;
use novarocks_proto_codec::membership::BackendProcessDescriptor;
use novarocks_proto_codec::membership::{
    BackendAnnounceRequest, BackendAnnounceResult, BackendReportedState,
};
use novarocks_types::{AdvertiseEndpoint, BackendProcessId, NativeCompatibilityId, NativeEndpoint};

use crate::BackendDataRuntime;
use crate::exchange_receiver::BackendExchangeReceiverPort;
use crate::fragment::{grpc_exchange_transmitter, native_result_writer};
use crate::metrics::{BackendMetricsRegistry, MetricsHttpServer};
use crate::rpc::client::BackendRpcClient;
use crate::rpc::runtime::BackendNativeTransport;
use crate::rpc::server::{BackendRpcServerHandle, BackendRpcService};
use crate::rpc::task_execution::TaskExecutionIngress;
use crate::runtime_filter::ingress::native_runtime_filter_envelope_ingress;
use crate::runtime_filter::rpc::BackendRuntimeFilterEnvelopeIngress;
use crate::task_execution::{
    RegistryTaskExecutionIngress, TaskExecutionRegistry, TaskExecutionRegistryConfig,
};
// Only the refusing hosts below name these, and they exist for one test.
#[cfg(test)]
use crate::task_execution::{
    HostRejection, QueryContextHost, ReleasedContextEvidence, RunnableTask, SharedFactsRequest,
    TaskExecutionHost, TaskStatusReporter,
};
use novarocks_execution::exec::expr::agg::SealedExecutionFunctionSet;
use novarocks_execution::runtime::fragment::io::ExchangeReceiverPort;
#[cfg(test)]
use novarocks_execution::task_execution::descriptor::TaskDescriptor;
#[cfg(test)]
use novarocks_execution::task_execution::identity::QueryContextRef;
#[cfg(test)]
use novarocks_execution::task_execution::operation::{QueryContextDomainUpdate, TaskDomainUpdate};
#[cfg(test)]
use novarocks_execution::task_execution::status::TaskFailureCategory;
use novarocks_spi::connector::WriteCommitEvidenceLimits;

const READINESS_TIMEOUT: Duration = Duration::from_secs(5);
const SUPERVISION_POLL_INTERVAL: Duration = Duration::from_millis(50);
const ANNOUNCE_RPC_TIMEOUT: Duration = Duration::from_secs(3);

/// How often the task protocol owner re-evaluates its own deadlines.
///
/// Nothing in that protocol expires by itself: a lease expiry, a creation-gate
/// timeout, a retention sweep, and a throttled metric report all become
/// decisions only inside `advance_deadlines`, so this interval is what bounds
/// how late each of them can be. It has to stay well inside the shortest thing
/// it decides — the one-second minimum lease of `LeaseBounds::DEFAULT` — and no
/// coarser than the 250 ms status metric throttle it flushes, so a delayed
/// report waits for one tick rather than for a whole tick period on top of the
/// throttle. One sweep of an idle owner is a pair of ordered-map walks under
/// one mutex, so paying it ten times a second costs nothing.
const TASK_DEADLINE_TICK_INTERVAL: Duration = Duration::from_millis(100);

pub struct BackendServerConfig {
    pub bind_host: String,
    pub grpc_port: u16,
    pub metrics_http_port: u16,
    pub advertise_endpoint: AdvertiseEndpoint,
    /// Server-resolved Native caller authentication and transport material.
    /// Backend receives this immutable capability and never reads trust source
    /// configuration or credentials itself.
    pub native_trust: Arc<NativeTrust>,
    /// Server-resolved compatibility identity frozen before role composition.
    pub native_compatibility_id: NativeCompatibilityId,
    /// Process-wide immutable engine function metadata and implementations.
    pub function_set: Arc<SealedExecutionFunctionSet>,
    pub native_transport: BackendNativeTransport,
    /// Exact FE native ingress used exclusively for authenticated membership announce.
    pub frontend_endpoint: NativeEndpoint,
    pub announce_interval: Duration,
    pub announce_initial_backoff: Duration,
    pub announce_max_backoff: Duration,
    /// Server-resolved per-fragment terminal write evidence budget.
    pub write_commit_evidence_limits: WriteCommitEvidenceLimits,
    pub execution_runtime_config: ExecutionRuntimeConfig,
    /// Server-frozen bounded failure and provider-bind policy for the BE
    /// catalog manager.
    pub catalog_manager_config: crate::connector::catalog_manager::CatalogManagerConfig,
    /// Provider-owned complete BE role factories. The backend seals exactly
    /// one factory per provider kind before query lifecycle admission.
    pub execution_role_binding_factories: Vec<Arc<dyn ConnectorExecutionRoleBindingFactory>>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum BackendApplicationErrorKind {
    Configuration,
    Start,
    Readiness,
    Supervision,
    Shutdown,
    Signal,
}

#[derive(Debug)]
pub struct BackendApplicationError {
    kind: BackendApplicationErrorKind,
    message: String,
}

impl BackendApplicationError {
    fn new(kind: BackendApplicationErrorKind, error: impl fmt::Display) -> Self {
        Self {
            kind,
            message: error.to_string(),
        }
    }

    fn with_cleanup_context(mut self, cleanup_error: impl fmt::Display) -> Self {
        self.message
            .push_str(&format!("; cleanup failed: {cleanup_error}"));
        self
    }

    pub const fn kind(&self) -> BackendApplicationErrorKind {
        self.kind
    }
}

impl fmt::Display for BackendApplicationError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{:?}: {}", self.kind, self.message)
    }
}

impl std::error::Error for BackendApplicationError {}

pub struct BackendApplicationHost {
    ready_marker: String,
    grpc_server: BackendRpcServerHandle,
    _execution_runtime: Arc<ExecutionRuntime>,
    task_deadline_tick: TaskDeadlineTickTask,
    metrics_http_server: MetricsHttpServer,
    process_descriptor: BackendProcessDescriptor,
    drain: Arc<BackendDrainState>,
    announce_task: BackendAnnounceTask,
}

struct BackendAnnounceTask {
    stop: Arc<AtomicBool>,
    wake: Arc<(std::sync::Mutex<bool>, std::sync::Condvar)>,
    join: Option<std::thread::JoinHandle<()>>,
    data_runtime: BackendDataRuntime,
    frontend_endpoint: NativeEndpoint,
    descriptor: BackendProcessDescriptor,
}

impl BackendAnnounceTask {
    fn start(
        data_runtime: BackendDataRuntime,
        frontend_endpoint: NativeEndpoint,
        descriptor: BackendProcessDescriptor,
        drain: Arc<BackendDrainState>,
        interval: Duration,
        initial_backoff: Duration,
        max_backoff: Duration,
    ) -> Self {
        let stop = Arc::new(AtomicBool::new(false));
        let wake = Arc::new((std::sync::Mutex::new(false), std::sync::Condvar::new()));
        let thread_stop = Arc::clone(&stop);
        let thread_drain = drain;
        let thread_wake = Arc::clone(&wake);
        let thread_runtime = data_runtime.clone();
        let thread_frontend_endpoint = frontend_endpoint.clone();
        let thread_descriptor = descriptor.clone();
        let join = std::thread::Builder::new()
            .name("backend-announce".to_string())
            .spawn(move || {
                let client = BackendRpcClient::new_native_endpoint(
                    thread_runtime,
                    thread_frontend_endpoint,
                );
                let initial_backoff = initial_backoff.max(Duration::from_millis(1));
                let max_backoff = max_backoff.max(initial_backoff);
                let mut retry_delay = initial_backoff;
                while !thread_stop.load(Ordering::Acquire) {
                    let reported_state = if thread_drain.is_draining() {
                        BackendReportedState::Draining
                    } else {
                        BackendReportedState::Running
                    };
                    let request = BackendAnnounceRequest::new(
                        thread_descriptor.clone(),
                        reported_state,
                    )
                        .expect("backend process descriptor remains validated");
                    let next_delay = match client.blocking_announce_backend_with_timeout(
                        request.as_proto().clone(),
                        ANNOUNCE_RPC_TIMEOUT,
                    ) {
                        Ok(BackendAnnounceResult::Accepted { lease_ttl_ms }) => {
                            retry_delay = initial_backoff;
                            interval.min(Duration::from_millis(lease_ttl_ms.saturating_div(3).max(1)))
                        }
                        Ok(BackendAnnounceResult::Rejected { reason, safe_detail }) => {
                            tracing::error!(?reason, %safe_detail, "backend announce rejected by frontend");
                            let delay = retry_delay;
                            retry_delay = retry_delay.saturating_mul(2).min(max_backoff);
                            delay
                        }
                        Err(error) => {
                            tracing::warn!(%error, "backend announce attempt failed");
                            let delay = retry_delay;
                            retry_delay = retry_delay.saturating_mul(2).min(max_backoff);
                            delay
                        }
                    };
                    let (pending, signal) = &*thread_wake;
                    let mut pending = pending.lock().expect("backend announce wake lock");
                    if !*pending && !thread_stop.load(Ordering::Acquire) {
                        let (next, _) = signal
                            .wait_timeout(pending, next_delay)
                            .expect("backend announce wake wait");
                        pending = next;
                    }
                    *pending = false;
                }
            })
            .expect("spawn backend announce task");
        Self {
            stop,
            wake,
            join: Some(join),
            data_runtime,
            frontend_endpoint,
            descriptor,
        }
    }

    /// Reports the drain the process has already entered.
    ///
    /// The flag itself belongs to `BackendDrainState`; the composition root
    /// sets it before calling this, so the announce below and the heartbeat
    /// this BE answers cannot disagree about the same process.
    fn announce_drain(&self) {
        let client = BackendRpcClient::new_native_endpoint(
            self.data_runtime.clone(),
            self.frontend_endpoint.clone(),
        );
        let request =
            BackendAnnounceRequest::new(self.descriptor.clone(), BackendReportedState::Draining)
                .expect("backend process descriptor remains validated");
        match client.blocking_announce_backend_with_timeout(
            request.as_proto().clone(),
            ANNOUNCE_RPC_TIMEOUT,
        ) {
            Ok(BackendAnnounceResult::Accepted { .. }) => {}
            Ok(BackendAnnounceResult::Rejected {
                reason,
                safe_detail,
            }) => {
                tracing::error!(?reason, %safe_detail, "backend drain announce rejected by frontend");
            }
            Err(error) => {
                tracing::warn!(%error, "backend drain announce attempt failed");
            }
        }
        let (pending, signal) = &*self.wake;
        *pending.lock().expect("backend announce wake lock") = true;
        signal.notify_one();
    }

    fn stop(&mut self) {
        self.stop.store(true, Ordering::Release);
        let (_, signal) = &*self.wake;
        signal.notify_one();
        if let Some(join) = self.join.take() {
            let _ = join.join();
        }
    }
}

impl fmt::Debug for BackendApplicationHost {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("BackendApplicationHost")
            .field("ready_marker", &self.ready_marker)
            .finish_non_exhaustive()
    }
}

struct BackendApplicationServices {
    /// This process's immutable identity, minted once by the composition root
    /// below. Every owner that stamps or checks it reads this one value.
    backend_process_id: BackendProcessId,
    drain: Arc<BackendDrainState>,
    execution_runtime: Arc<ExecutionRuntime>,
    exchange_receiver_port: Arc<dyn ExchangeReceiverPort>,
    task_execution_registry: Arc<TaskExecutionRegistry>,
    task_execution_ingress: Arc<dyn TaskExecutionIngress>,
    /// The task substrate's exchange-destination authority. The RPC data
    /// plane needs it directly: without it no created task can receive an
    /// exchange frame, because the frozen descriptor is the only place a
    /// task's inbound topology exists.
    task_inbound_capabilities: Arc<crate::task_execution::TaskInboundCapabilities>,
    /// The task substrate's runtime-filter participant owner. The RPC ingress
    /// needs it directly, for the same reason: an `EstablishQueryContext`
    /// install is the only place a task-protocol query's participant exists.
    query_context_host: Arc<crate::task_execution::NativeQueryContextHost>,
}

/// What the two unrouted hosts below report.
#[cfg(test)]
const UNROUTED_DETAIL: &str = "the native task protocol has no execution binding in this process";

/// The query-context half of the execution binding the task protocol owner
/// does not have yet.
///
/// The protocol is reachable over the wire, but the fragment-based lifecycle
/// stack still owns every query, so nothing in this process can materialize
/// shared facts, install a receiver, or start a worker on the owner's behalf.
/// Refusing each of those explicitly is what keeps the gap visible: an
/// establish or a create is answered with a typed refusal, while every
/// decision the owner makes on its own — termination, observation, retention —
/// keeps working. Binding this to real execution is a separate step.
/// A host that installs nothing, for the one test that drives the deadline
/// tick without binding a fragment.
#[cfg(test)]
struct UnroutedQueryContextHost;

#[cfg(test)]
impl QueryContextHost for UnroutedQueryContextHost {
    fn materialize(&self, _request: SharedFactsRequest<'_>) -> Result<(), HostRejection> {
        Err(HostRejection::new(
            TaskFailureCategory::Internal,
            UNROUTED_DETAIL,
        ))
    }

    fn release(&self, _context: QueryContextRef) -> ReleasedContextEvidence {
        ReleasedContextEvidence::none()
    }

    fn advance_shared_domain(
        &self,
        _context: QueryContextRef,
        _domain: &QueryContextDomainUpdate,
    ) -> Result<(), HostRejection> {
        Err(HostRejection::new(
            TaskFailureCategory::Internal,
            UNROUTED_DETAIL,
        ))
    }
}

/// The task half of the same missing binding.
#[cfg(test)]
struct UnroutedTaskExecutionHost;

#[cfg(test)]
impl TaskExecutionHost for UnroutedTaskExecutionHost {
    fn install_receiver(&self, _descriptor: &TaskDescriptor) -> Result<(), HostRejection> {
        Err(HostRejection::new(
            TaskFailureCategory::Internal,
            UNROUTED_DETAIL,
        ))
    }

    fn remove_receiver(&self, _descriptor: &TaskDescriptor) {}

    fn install_inbound_capability(
        &self,
        _descriptor: &TaskDescriptor,
    ) -> Result<(), HostRejection> {
        Err(HostRejection::new(
            TaskFailureCategory::Internal,
            UNROUTED_DETAIL,
        ))
    }

    fn remove_inbound_capability(&self, _descriptor: &TaskDescriptor) {}

    fn submit_runnable(
        &self,
        _descriptor: &TaskDescriptor,
        _reporter: TaskStatusReporter,
    ) -> Result<Arc<dyn RunnableTask>, HostRejection> {
        Err(HostRejection::new(
            TaskFailureCategory::Internal,
            UNROUTED_DETAIL,
        ))
    }

    fn apply_task_domain(
        &self,
        _descriptor: &TaskDescriptor,
        _domain: &TaskDomainUpdate,
    ) -> Result<Option<u64>, HostRejection> {
        Err(HostRejection::new(
            TaskFailureCategory::Internal,
            UNROUTED_DETAIL,
        ))
    }
}

/// The maintenance tick of the task protocol owner.
///
/// The owner never sleeps against a wall clock, so elapsed time becomes a
/// decision only when something calls `advance_deadlines`. This is that
/// something: without it no lease ever expires, no creation gate ever times
/// out, and no retained record is ever reclaimed.
struct TaskDeadlineTickTask {
    stop: watch::Sender<bool>,
    failure_rx: mpsc::Receiver<String>,
    join: Option<tokio::task::JoinHandle<()>>,
}

impl TaskDeadlineTickTask {
    fn start(
        runtime: &BackendDataRuntime,
        registry: Arc<TaskExecutionRegistry>,
        interval: Duration,
    ) -> Self {
        let (stop, mut stopped) = watch::channel(false);
        let (failure_tx, failure_rx) = mpsc::channel();
        let join = runtime.handle().spawn(async move {
            let mut ticker = tokio::time::interval(interval);
            ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            // The first tick of a Tokio interval completes immediately and
            // there is nothing to sweep at composition time.
            ticker.tick().await;
            loop {
                tokio::select! {
                    _ = stopped.changed() => return,
                    _ = ticker.tick() => {}
                }
                let registry = Arc::clone(&registry);
                // One sweep takes the owner's mutex, so it runs on the
                // blocking pool rather than on a runtime worker.
                if let Err(error) =
                    tokio::task::spawn_blocking(move || registry.advance_deadlines()).await
                {
                    // Nothing else re-evaluates a deadline, so a dead sweep is
                    // a supervision failure rather than a missed tick.
                    let _ = failure_tx.send(format!(
                        "task execution deadline sweep stopped running: {error}"
                    ));
                    return;
                }
            }
        });
        Self {
            stop,
            failure_rx,
            join: Some(join),
        }
    }

    fn poll_failure(&mut self) -> Option<String> {
        self.failure_rx.try_recv().ok()
    }

    /// Asks the tick to return, then drops it.
    ///
    /// The abort is a backstop for a runtime that is already winding down: the
    /// loop's only await points are the tick, the stop signal, and the join of
    /// one sweep, and a sweep that has already started runs to completion on
    /// the blocking pool, so nothing is left half applied.
    fn stop(&mut self) {
        let _ = self.stop.send(true);
        if let Some(join) = self.join.take() {
            join.abort();
        }
    }
}

impl Drop for TaskDeadlineTickTask {
    fn drop(&mut self) {
        self.stop();
    }
}

struct BackendExecutionRuntimeInput {
    config: ExecutionRuntimeConfig,
    function_set: Arc<SealedExecutionFunctionSet>,
}

impl BackendExecutionRuntimeInput {
    fn new(config: ExecutionRuntimeConfig, function_set: Arc<SealedExecutionFunctionSet>) -> Self {
        Self {
            config,
            function_set,
        }
    }
}

fn compose_backend_application_services(
    data_runtime: BackendDataRuntime,
    execution: BackendExecutionRuntimeInput,
    write_commit_evidence_limits: WriteCommitEvidenceLimits,
    catalog_manager_config: crate::connector::catalog_manager::CatalogManagerConfig,
    execution_role_binding_factories: &[Arc<dyn ConnectorExecutionRoleBindingFactory>],
) -> Result<BackendApplicationServices, BackendApplicationError> {
    let BackendExecutionRuntimeInput {
        config: execution_runtime_config,
        function_set,
    } = execution;
    let execution_runtime = Arc::new(
        ExecutionRuntime::new(execution_runtime_config, Arc::clone(&function_set)).map_err(
            |error| BackendApplicationError::new(BackendApplicationErrorKind::Configuration, error),
        )?,
    );
    // One process identity, minted here. It is what the announce carries,
    // what a heartbeat is checked against, and what both execution owners
    // stamp their work with, so it is minted by the composition root rather
    // than by whichever owner happens to be constructed first.
    let backend_process_id = BackendProcessId::new_v7();
    let drain = Arc::new(BackendDrainState::new());
    let exchange_receiver_port: Arc<dyn ExchangeReceiverPort> = Arc::new(
        BackendExchangeReceiverPort::new(Arc::clone(&execution_runtime)),
    );
    let execution_role_binding_factories = Arc::new(
        crate::connector::catalog_manager::ConnectorExecutionRoleBindingFactorySet::try_new(
            execution_role_binding_factories.iter().cloned(),
        )
        .map_err(|error| {
            BackendApplicationError::new(
                BackendApplicationErrorKind::Configuration,
                format!("seal connector execution role binding factories: {error}"),
            )
        })?,
    );
    // One catalog manager per process: a catalog lease belongs to the process,
    // and two managers would be two authorities over the same leases.
    let catalog_manager = Arc::new(
        crate::connector::catalog_manager::CatalogManager::try_new(catalog_manager_config)
            .map_err(|error| {
                BackendApplicationError::new(
                    BackendApplicationErrorKind::Configuration,
                    format!("compose backend catalog manager: {error}"),
                )
            })?,
    );
    crate::runtime::native_fragment_query::NativeFragmentQueryRuntime::global()
        .publish_resource_snapshot();
    // One task protocol owner per process, on this process's own identity and
    // its monotonic clock, routed to the real execution owners.
    let context_host = Arc::new(crate::task_execution::NativeQueryContextHost::new(
        Arc::clone(&catalog_manager),
        Arc::clone(&execution_role_binding_factories),
        Arc::new(
            crate::runtime_filter::participant::BackendRuntimeFilterParticipantFactory::new(
                data_runtime.clone(),
            ),
        ),
        data_runtime.clone(),
    ));
    let inbound_capabilities = crate::task_execution::TaskInboundCapabilities::new();
    let execution_host = Arc::new(crate::task_execution::NativeTaskExecutionHost::new(
        crate::runtime::native_fragment_query::NativeFragmentQueryRuntime::global(),
        Arc::clone(&context_host) as Arc<dyn crate::task_execution::TaskQueryContextFacts>,
        Arc::clone(&inbound_capabilities),
        grpc_exchange_transmitter(data_runtime.clone()),
        native_result_writer(),
        Arc::clone(&exchange_receiver_port),
        Arc::new(
            crate::runtime::sink_commit::ConfiguredBackendSinkCommitPort::new(
                write_commit_evidence_limits,
            ),
        ),
        Arc::clone(&execution_runtime),
    ));
    let task_execution_registry = TaskExecutionRegistry::with_process_clock(
        TaskExecutionRegistryConfig::for_process(backend_process_id),
        Arc::clone(&context_host) as Arc<dyn crate::task_execution::QueryContextHost>,
        execution_host,
    );
    let task_execution_ingress: Arc<dyn TaskExecutionIngress> =
        RegistryTaskExecutionIngress::new(Arc::clone(&task_execution_registry));
    Ok(BackendApplicationServices {
        backend_process_id,
        drain,
        execution_runtime,
        exchange_receiver_port,
        task_execution_registry,
        task_execution_ingress,
        task_inbound_capabilities: inbound_capabilities,
        query_context_host: context_host,
    })
}

impl BackendApplicationHost {
    pub fn open(
        config: BackendServerConfig,
        data_runtime: BackendDataRuntime,
    ) -> Result<Self, BackendApplicationError> {
        Self::open_with_readiness_timeout(config, data_runtime, READINESS_TIMEOUT)
    }

    pub fn ready_marker(&self) -> &str {
        &self.ready_marker
    }

    /// Return the actual listener endpoint in a form a same-process frontend
    /// can dial.  A wildcard bind remains a listener concern; composition must
    /// use loopback rather than attempting to connect to `0.0.0.0` or `::`.
    pub fn connectable_native_endpoint(&self) -> SocketAddr {
        let bound = self.grpc_server.bound_addr();
        let ip = if bound.ip().is_unspecified() {
            match bound.ip() {
                std::net::IpAddr::V4(_) => std::net::IpAddr::V4(std::net::Ipv4Addr::LOCALHOST),
                std::net::IpAddr::V6(_) => std::net::IpAddr::V6(std::net::Ipv6Addr::LOCALHOST),
            }
        } else {
            bound.ip()
        };
        SocketAddr::new(ip, bound.port())
    }

    pub fn process_descriptor(&self) -> &BackendProcessDescriptor {
        &self.process_descriptor
    }

    /// SIGTERM makes this BE ineligible for new Init while existing admitted
    /// lifecycle entries remain reachable until their normal terminal state.
    pub fn begin_drain(&self) {
        // One flag, set once, before anything reports it: the heartbeat this
        // BE answers and the announce it sends then read the same value.
        self.drain.begin_drain();
        self.announce_task.announce_drain();
    }

    pub fn poll_failure(
        &mut self,
    ) -> Result<Option<BackendApplicationError>, BackendApplicationError> {
        for failure in [
            self.grpc_server.poll_failure(),
            self.metrics_http_server.poll_failure(),
            Ok(self.task_deadline_tick.poll_failure()),
        ] {
            match failure {
                Ok(Some(error)) => {
                    return Ok(Some(BackendApplicationError::new(
                        BackendApplicationErrorKind::Supervision,
                        error,
                    )));
                }
                Ok(None) => {}
                Err(error) => {
                    return Err(BackendApplicationError::new(
                        BackendApplicationErrorKind::Supervision,
                        error,
                    ));
                }
            }
        }
        Ok(None)
    }

    pub fn shutdown(mut self) -> Result<(), BackendApplicationError> {
        self.announce_task.stop();
        self.task_deadline_tick.stop();
        let listener_shutdown = self.grpc_server.stop();
        let metrics_result = self.metrics_http_server.stop();
        combine_shutdown_results(listener_shutdown, metrics_result).map_err(|error| {
            BackendApplicationError::new(BackendApplicationErrorKind::Shutdown, error)
        })
    }

    fn open_with_readiness_timeout(
        config: BackendServerConfig,
        data_runtime: BackendDataRuntime,
        readiness_timeout: Duration,
    ) -> Result<Self, BackendApplicationError> {
        let BackendServerConfig {
            bind_host,
            grpc_port,
            metrics_http_port,
            advertise_endpoint,
            native_trust,
            native_compatibility_id,
            function_set,
            native_transport,
            frontend_endpoint,
            announce_interval,
            announce_initial_backoff,
            announce_max_backoff,
            write_commit_evidence_limits,
            execution_runtime_config,
            catalog_manager_config,
            execution_role_binding_factories,
        } = config;
        let readiness_endpoint =
            NativeEndpoint::from_host_port(&advertise_endpoint.host, advertise_endpoint.port)
                .map_err(|error| {
                    BackendApplicationError::new(
                        BackendApplicationErrorKind::Configuration,
                        format!("invalid advertised Native readiness endpoint: {error}"),
                    )
                })?;
        let readiness_runtime = data_runtime.clone();
        let services = compose_backend_application_services(
            data_runtime,
            BackendExecutionRuntimeInput::new(execution_runtime_config, function_set),
            write_commit_evidence_limits,
            catalog_manager_config,
            &execution_role_binding_factories,
        )?;
        let process_descriptor = BackendProcessDescriptor::new(
            services.backend_process_id,
            QueryControlEndpoint::new(advertise_endpoint.host.clone(), advertise_endpoint.port)
                .map_err(|error| {
                    BackendApplicationError::new(
                        BackendApplicationErrorKind::Configuration,
                        format!("resolve backend process endpoint: {error}"),
                    )
                })?,
            native_trust.deployment_id().as_str(),
            novarocks_version::native_build_identity(),
            native_compatibility_id,
        )
        .map_err(|error| {
            BackendApplicationError::new(
                BackendApplicationErrorKind::Configuration,
                format!("construct backend process descriptor: {error}"),
            )
        })?;
        let metrics_registry = Arc::new(BackendMetricsRegistry::new().map_err(|error| {
            BackendApplicationError::new(BackendApplicationErrorKind::Configuration, error)
        })?);
        let metrics_http_server =
            MetricsHttpServer::start(&bind_host, metrics_http_port, metrics_registry).map_err(
                |error| BackendApplicationError::new(BackendApplicationErrorKind::Start, error),
            )?;
        // Started before the listener: the owner is reachable the moment its
        // RPCs are, and a deadline that elapses must already be decidable.
        let task_deadline_tick = TaskDeadlineTickTask::start(
            &readiness_runtime,
            Arc::clone(&services.task_execution_registry),
            TASK_DEADLINE_TICK_INTERVAL,
        );

        // The participant owner, because an attempt is reachable only
        // through the one that installed it, and every intent's participant is
        // installed by the query-context host.
        let runtime_filter_ingress: Arc<dyn BackendRuntimeFilterEnvelopeIngress> =
            native_runtime_filter_envelope_ingress(Arc::clone(&services.query_context_host));
        let mut grpc_server = match BackendRpcServerHandle::start(
            &bind_host,
            grpc_port,
            BackendRpcService::new(
                Arc::clone(&services.task_execution_ingress),
                Arc::clone(&services.query_context_host)
                    as Arc<dyn crate::rpc::server::CatalogReachabilityAuthority>,
                runtime_filter_ingress,
                Arc::clone(&services.exchange_receiver_port),
                Arc::clone(&services.task_inbound_capabilities),
                crate::rpc::server::BackendProcessFacts {
                    process_id: services.backend_process_id,
                    descriptor: process_descriptor.clone(),
                    drain: Arc::clone(&services.drain),
                },
            ),
            native_trust,
            native_transport,
        ) {
            Ok(server) => server,
            Err(error) => {
                let metrics_result = metrics_http_server.stop();
                let primary = BackendApplicationError::new(
                    BackendApplicationErrorKind::Start,
                    format!("start native backend gRPC server on {bind_host}:{grpc_port}: {error}"),
                );
                return Err(match metrics_result {
                    Ok(()) => primary,
                    Err(cleanup_error) => primary.with_cleanup_context(cleanup_error),
                });
            }
        };

        if let Err(error) =
            wait_for_native_ready(&readiness_runtime, readiness_endpoint, readiness_timeout)
        {
            let listener_result = grpc_server.stop();
            let metrics_result = metrics_http_server.stop();
            let primary = BackendApplicationError::new(
                BackendApplicationErrorKind::Readiness,
                format!("advertised endpoint readiness failed: {error}"),
            );
            return Err(append_cleanup_results(
                primary,
                [listener_result, metrics_result],
            ));
        }

        let announce_task = BackendAnnounceTask::start(
            readiness_runtime,
            frontend_endpoint,
            process_descriptor.clone(),
            Arc::clone(&services.drain),
            announce_interval.max(Duration::from_millis(100)),
            announce_initial_backoff,
            announce_max_backoff,
        );

        Ok(Self {
            ready_marker: format!(
                "NOVAROCKS_READY role=be grpc_port={grpc_port} advertise_host={} pid={}",
                advertise_endpoint.host,
                std::process::id()
            ),
            grpc_server,
            _execution_runtime: services.execution_runtime,
            task_deadline_tick,
            metrics_http_server,
            process_descriptor,
            drain: services.drain,
            announce_task,
        })
    }
}

#[allow(
    dead_code,
    reason = "This library entrypoint is invoked by the backend server binary, not backend lib tests."
)]
pub fn run_backend_server(config: BackendServerConfig) -> Result<(), BackendApplicationError> {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .thread_stack_size(novarocks_types::WORKER_STACK_SIZE_BYTES)
        .build()
        .map_err(|error| {
            BackendApplicationError::new(
                BackendApplicationErrorKind::Start,
                format!("build backend Tokio runtime failed: {error}"),
            )
        })?;
    let data_runtime = BackendDataRuntime::new(
        runtime.handle().clone(),
        Arc::clone(&config.native_trust),
        config.native_transport.clone(),
    );
    runtime.block_on(run_backend_server_until_signal(config, data_runtime))
}
pub async fn run_backend_server_until_shutdown<F>(
    config: BackendServerConfig,
    data_runtime: BackendDataRuntime,
    shutdown: F,
) -> Result<(), BackendApplicationError>
where
    F: Future<Output = ()> + Send,
{
    run_backend_server_until(config, data_runtime, async move {
        shutdown.await;
        Ok(())
    })
    .await
}

pub async fn run_backend_server_until_signal(
    config: BackendServerConfig,
    data_runtime: BackendDataRuntime,
) -> Result<(), BackendApplicationError> {
    #[cfg(unix)]
    let mut interrupt = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::interrupt())
        .map_err(|error| {
        BackendApplicationError::new(
            BackendApplicationErrorKind::Signal,
            format!("install SIGINT listener failed: {error}"),
        )
    })?;

    run_backend_server_until(config, data_runtime, async {
        #[cfg(unix)]
        {
            // Register the OS handler before the host emits its ready marker.
            // A supervisor can otherwise deliver SIGINT in the narrow window
            // between readiness and the first poll of `tokio::signal::ctrl_c`.
            interrupt.recv().await;
            Ok(())
        }
        #[cfg(not(unix))]
        tokio::signal::ctrl_c().await.map_err(|error| {
            BackendApplicationError::new(
                BackendApplicationErrorKind::Signal,
                format!("Ctrl-C listener failed: {error}"),
            )
        })
    })
    .await
}

async fn run_backend_server_until<F>(
    config: BackendServerConfig,
    data_runtime: BackendDataRuntime,
    shutdown: F,
) -> Result<(), BackendApplicationError>
where
    F: Future<Output = Result<(), BackendApplicationError>> + Send,
{
    let mut host = BackendApplicationHost::open(config, data_runtime)?;
    println!("{}", host.ready_marker());
    tokio::pin!(shutdown);

    let primary = loop {
        tokio::select! {
            signal_result = &mut shutdown => break signal_result,
            _ = tokio::time::sleep(SUPERVISION_POLL_INTERVAL) => match host.poll_failure() {
                Ok(Some(error)) | Err(error) => break Err(error),
                Ok(None) => {}
            },
        }
    };

    let primary = match primary {
        Ok(()) => match host.poll_failure() {
            Ok(Some(error)) | Err(error) => Err(error),
            Ok(None) => Ok(()),
        },
        Err(error) => Err(error),
    };
    host.begin_drain();
    combine_primary_and_shutdown(primary, host.shutdown())
}

fn combine_primary_and_shutdown(
    primary: Result<(), BackendApplicationError>,
    shutdown: Result<(), BackendApplicationError>,
) -> Result<(), BackendApplicationError> {
    match (primary, shutdown) {
        (Ok(()), Ok(())) => Ok(()),
        (Err(primary), Ok(())) => Err(primary),
        (Ok(()), Err(shutdown)) => Err(shutdown),
        (Err(primary), Err(shutdown)) => Err(primary.with_cleanup_context(shutdown)),
    }
}

fn append_cleanup_results(
    mut primary: BackendApplicationError,
    cleanup_results: impl IntoIterator<Item = Result<(), String>>,
) -> BackendApplicationError {
    for cleanup_result in cleanup_results {
        if let Err(cleanup_error) = cleanup_result {
            primary = primary.with_cleanup_context(cleanup_error);
        }
    }
    primary
}

fn combine_shutdown_results(
    listener: Result<(), String>,
    sweep: Result<(), String>,
) -> Result<(), String> {
    match (listener, sweep) {
        (Ok(()), Ok(())) => Ok(()),
        (Err(error), Ok(())) | (Ok(()), Err(error)) => Err(error),
        (Err(sweep), Err(resources)) => Err(format!("{sweep}; {resources}")),
    }
}

fn wait_for_native_ready(
    runtime: &BackendDataRuntime,
    endpoint: NativeEndpoint,
    timeout: Duration,
) -> Result<(), String> {
    let connector = runtime.native_transport().connector_for(endpoint.clone())?;
    runtime.block_on(async move {
        tokio::time::timeout(timeout, connector.connect())
            .await
            .map_err(|_| {
                format!(
                    "advertised Native endpoint {endpoint} did not become ready within {}ms",
                    timeout.as_millis()
                )
            })?
            .map(|_| ())
            .map_err(|error| {
                format!("advertised Native endpoint {endpoint} readiness failed: {error}")
            })
    })
}

#[cfg(test)]
mod tests {
    use std::io::{Read, Write};
    use std::net::TcpListener;
    use std::sync::{Arc, LazyLock, Mutex};
    use std::time::Duration;

    use super::{
        BackendApplicationError, BackendApplicationErrorKind, BackendApplicationHost,
        BackendExecutionRuntimeInput, BackendServerConfig, QueryContextRef, TaskDeadlineTickTask,
        TaskExecutionRegistryConfig, UnroutedQueryContextHost, UnroutedTaskExecutionHost,
        combine_primary_and_shutdown, compose_backend_application_services,
    };
    use crate::rpc::runtime::test_backend_native_trust;
    use crate::rpc::transport::nova_rocks_grpc_client::NovaRocksGrpcClient;
    use novarocks_execution::exec::expr::agg::SealedExecutionFunctionSet;
    use novarocks_execution::runtime::execution_runtime::{
        ExecutionRuntimeConfig, ExecutionSpillStorageConfig,
    };
    use novarocks_proto_models::novarocks as protocol;
    use novarocks_proto_models::novarocks::{HeartbeatRequest, HeartbeatResponse};
    use novarocks_spi::connector::WriteCommitEvidenceLimits;
    use novarocks_types::{AdvertiseEndpoint, BackendProcessId, NativeEndpoint};

    static LIVE_HOST_TEST: LazyLock<Mutex<()>> = LazyLock::new(|| Mutex::new(()));

    fn unused_port() -> u16 {
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind ephemeral port");
        let port = listener
            .local_addr()
            .expect("read ephemeral address")
            .port();
        drop(listener);
        port
    }

    fn execution_runtime_config() -> ExecutionRuntimeConfig {
        ExecutionRuntimeConfig {
            driver_threads: 1,
            scan_threads: 1,
            scan_queue_capacity: 1,
            spill_io_threads: 1,
            spill_io_queue_capacity: 1,
            spill_storage: ExecutionSpillStorageConfig::default(),
            exchange_wait_ms: 1,
            exchange_io_threads: 1,
            exchange_io_max_inflight_bytes: 1,
            exchange_max_transmit_batched_bytes: 1,
            operator_buffer_chunks: 1,
            local_exchange_buffer_mem_limit_per_driver: 1,
            local_exchange_max_buffered_rows: -1,
            connector_io_tasks_per_scan_operator: 1,
            scan_submit_fail_max: 1,
            scan_submit_fail_timeout_ms: 1,
            runtime_filter_scan_wait_time_ms_override: None,
            runtime_filter_wait_timeout_ms_override: None,
            sink_io_worker_threads: 1,
            sink_io_max_blocking_threads: 1,
        }
    }

    fn test_execution_function_set() -> Arc<SealedExecutionFunctionSet> {
        let mut builder = novarocks_execution::exec::expr::agg::ExecutionFunctionSetBuilder::new();
        novarocks_sql::compiler::contribute_builtin_functions(builder.catalog_builder_mut())
            .expect("builtin function metadata");
        novarocks_execution::exec::expr::agg::contribute_builtin_aggregate_implementations(
            &mut builder,
        )
        .expect("builtin aggregate implementations");
        Arc::new(builder.seal().expect("builtin execution function set"))
    }

    fn test_data_runtime() -> crate::BackendDataRuntime {
        crate::rpc::runtime::test_backend_data_runtime()
    }

    /// The tick is the only thing that turns elapsed time into a decision, so
    /// this asserts an actual reclamation rather than that a task was spawned.
    ///
    /// An abort of a context this backend never established leaves a retained
    /// terminal fence, which needs no execution binding at all. Nothing else
    /// reclaims it: the clock moves, and only a sweep can notice.
    #[test]
    fn the_deadline_tick_drives_the_task_owner_forward() {
        use novarocks_execution::task_execution::identity::TaskOperationId;
        use novarocks_execution::task_execution::operation::AbortQueryContext;
        use novarocks_execution::task_execution::status::AbortCause;
        use novarocks_execution::task_execution::transition::QueryContextState;
        use novarocks_types::identity::{FrontendProcessId, QueryExecutionId, QueryId};

        let backend = novarocks_types::BackendProcessId::new_v7();
        let clock = Arc::new(crate::task_execution::ManualClock::new());
        let registry = crate::task_execution::TaskExecutionRegistry::new(
            TaskExecutionRegistryConfig::for_process(backend),
            Arc::clone(&clock) as Arc<dyn crate::task_execution::BackendMonotonicClock>,
            Arc::new(UnroutedQueryContextHost),
            Arc::new(UnroutedTaskExecutionHost),
        );
        let context = QueryContextRef::new(
            QueryExecutionId::new(
                QueryId::new(5, 6),
                novarocks_types::identity::AttemptId::new(1).expect("nonzero attempt"),
            )
            .expect("nonzero query"),
            FrontendProcessId::new_v7(),
            backend,
        );
        registry.abort_query_context(&AbortQueryContext::new(
            TaskOperationId::new_v7(),
            context,
            AbortCause::QueryFailed,
        ));
        assert_eq!(
            registry.context_state(context),
            QueryContextState::TerminalRetained
        );

        let mut tick = TaskDeadlineTickTask::start(
            &test_data_runtime(),
            Arc::clone(&registry),
            Duration::from_millis(5),
        );
        clock.advance(Duration::from_secs(600));
        let mut reclaimed = false;
        for _ in 0..500 {
            if registry.context_state(context) == QueryContextState::Gone {
                reclaimed = true;
                break;
            }
            std::thread::sleep(Duration::from_millis(10));
        }
        tick.stop();
        assert!(
            reclaimed,
            "the maintenance tick never re-evaluated the retention horizon"
        );
        assert_eq!(tick.poll_failure(), None);
    }

    fn http_get(port: u16, path: &str) -> std::io::Result<String> {
        let mut stream =
            std::net::TcpStream::connect(("127.0.0.1", port)).expect("connect HTTP listener");
        stream
            .set_read_timeout(Some(Duration::from_secs(1)))
            .expect("set HTTP read timeout");
        write!(
            stream,
            "GET {path} HTTP/1.1\r\nHost: 127.0.0.1\r\nConnection: close\r\n\r\n"
        )
        .expect("write HTTP request");
        let mut response = String::new();
        stream.read_to_string(&mut response)?;
        Ok(response)
    }

    fn backend_config(grpc_port: u16, advertise_port: u16) -> BackendServerConfig {
        BackendServerConfig {
            bind_host: "127.0.0.1".to_string(),
            grpc_port,
            metrics_http_port: unused_port(),
            advertise_endpoint: AdvertiseEndpoint {
                host: "127.0.0.1".to_string(),
                port: advertise_port,
            },
            native_trust: crate::rpc::runtime::test_backend_native_trust(),
            native_compatibility_id: novarocks_types::NativeCompatibilityId::new([0x71; 32]),
            function_set: test_execution_function_set(),
            native_transport: crate::rpc::runtime::BackendNativeTransport::Plaintext,
            frontend_endpoint: NativeEndpoint::from_host_port("127.0.0.1", unused_port())
                .expect("valid frontend endpoint"),
            announce_interval: Duration::from_secs(60),
            announce_initial_backoff: Duration::from_millis(100),
            announce_max_backoff: Duration::from_secs(2),
            write_commit_evidence_limits: WriteCommitEvidenceLimits::default(),
            execution_runtime_config: execution_runtime_config(),
            catalog_manager_config:
                crate::connector::catalog_manager::CatalogManagerConfig::default(),
            execution_role_binding_factories: Vec::new(),
        }
    }

    async fn connect_live_channel(grpc_port: u16) -> tonic::transport::Channel {
        tonic::transport::Channel::from_shared(format!("http://127.0.0.1:{grpc_port}"))
            .expect("construct native backend test endpoint")
            .connect()
            .await
            .expect("connect native backend gRPC")
    }

    /// Every cross-backend runtime-filter envelope arrives through the ingress
    /// this composes, and a participant is reachable only through the owner
    /// that installed it.
    ///
    /// The task protocol installs its participant with
    /// `EstablishQueryContext` and creates no `InitQuery` manifest, so the
    /// fragment query lifecycle holds nothing for such an attempt. With only
    /// that owner wired, every producer contribution and every materialized
    /// artifact is refused at the peer as query-unavailable. Nothing fails --
    /// a runtime filter is a conservative pre-filter -- so each consumer
    /// simply waits out its whole wait cap and then scans unfiltered.
    #[test]
    fn the_composed_runtime_filter_ingress_reaches_a_task_protocol_participant() {
        use super::native_runtime_filter_envelope_ingress;
        use crate::runtime_filter::domain::BackendEnvelopeKind;
        use crate::runtime_filter::test_support::delivery_envelope_for_test;
        use crate::task_execution::{QueryContextHost, SharedFactsRequest};
        use novarocks_execution::task_execution::CredentialUpdate;
        use novarocks_execution::task_execution::domain::{
            CodecOwnedContent, CredentialEpoch, CredentialLeaseId,
        };
        use novarocks_proto_codec::FieldPath;
        use novarocks_proto_codec::catalog::CatalogSet;
        use novarocks_proto_codec::task_execution::domain::{WireContent, WireCredential};
        use novarocks_proto_models::filter;
        use novarocks_types::identity::FrontendProcessId;

        let services = compose_backend_application_services(
            test_data_runtime(),
            BackendExecutionRuntimeInput::new(
                execution_runtime_config(),
                test_execution_function_set(),
            ),
            WriteCommitEvidenceLimits::default(),
            crate::connector::catalog_manager::CatalogManagerConfig::default(),
            &[],
        )
        .expect("compose backend application services");

        // The exact attempt the fixture envelope is addressed to, established
        // the way the task protocol establishes one.
        let envelope = delivery_envelope_for_test(BackendEnvelopeKind::CompletedWithoutArtifact);
        let context = QueryContextRef::new(
            crate::runtime_filter::test_support::participant_execution_id(),
            FrontendProcessId::new_v7(),
            BackendProcessId::new_v7(),
        );
        let catalogs: Arc<dyn CodecOwnedContent> = Arc::new(WireContent::new(
            b"catalog",
            CatalogSet::new(Vec::new())
                .expect("an empty catalog set is legal")
                .as_proto()
                .clone(),
        ));
        let filter: Arc<dyn CodecOwnedContent> = Arc::new(WireContent::new(
            b"contribution",
            protocol::RuntimeFilterContribution {
                participant_id: 1,
                lifecycle: Some(filter::RuntimeFilterQueryLifecycleOptions {
                    delivery_expire_ms: 1,
                    query_expire_ms: 1,
                    transport_retry_interval_ms: 1,
                    transport_max_attempts: 1,
                    transport_deadline_ms: 1,
                    transport_max_pending_entries: 1,
                    transport_max_pending_bytes: 1,
                }),
                install: Some(filter::RuntimeFilterParticipantInstall::default()),
            },
        ));
        let options: Arc<dyn CodecOwnedContent> = Arc::new(WireContent::new(
            b"query-options",
            protocol::QueryOptions::default(),
        ));
        let credential = CredentialUpdate::new(
            CredentialLeaseId::new(1),
            CredentialEpoch::FIRST,
            Arc::new(
                WireCredential::decode(&[], &[], FieldPath::root("credential"))
                    .expect("an empty rotation is legal"),
            ),
        );
        services
            .query_context_host
            .materialize(SharedFactsRequest::new(
                context,
                &catalogs,
                &filter,
                &options,
                &credential,
            ))
            .expect("establishing a query context with a participant is legal");

        let ingress =
            native_runtime_filter_envelope_ingress(Arc::clone(&services.query_context_host));
        let reason = ingress
            .accept(envelope)
            .rejection_reason()
            .map(str::to_string)
            .expect("a channel-less participant refuses this delivery itself");
        // Reaching the participant is the whole claim: it refuses the delivery
        // on its own route authority, which no composition refusal can say.
        assert!(
            reason.contains("[artifact-delivery]"),
            "the envelope must be decided by the participant the query context \
             installed, not refused for having no owner: {reason}"
        );
    }

    #[test]
    fn readiness_failure_stops_and_joins_started_listener() {
        let _live_host = LIVE_HOST_TEST.lock().expect("live host test lock");
        let grpc_port = unused_port();
        let mut config = backend_config(grpc_port, grpc_port);
        config.advertise_endpoint.host = "127.0.0.2".to_string();
        let error = BackendApplicationHost::open_with_readiness_timeout(
            config,
            test_data_runtime(),
            std::time::Duration::from_millis(25),
        )
        .expect_err("unreachable advertised endpoint must fail readiness");

        assert_eq!(error.kind(), BackendApplicationErrorKind::Readiness);
        TcpListener::bind(("127.0.0.1", grpc_port))
            .expect("readiness cleanup must release the started listener");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    #[expect(
        clippy::await_holding_lock,
        reason = "The mutex serializes loopback backend tests that bind listeners and must remain held for the full test."
    )]
    async fn application_authenticates_complete_native_route_set_before_domain_or_fallback() {
        let _live_host = LIVE_HOST_TEST.lock().expect("live host test lock");
        let grpc_port = unused_port();
        let metrics_port = unused_port();
        let mut config = backend_config(grpc_port, grpc_port);
        config.metrics_http_port = metrics_port;
        let host = BackendApplicationHost::open(config, test_data_runtime())
            .expect("native backend host starts");

        let mut missing_auth = NovaRocksGrpcClient::new(connect_live_channel(grpc_port).await);
        let error = missing_auth
            .heartbeat(HeartbeatRequest {
                expected_process_id: host.process_descriptor().as_proto().process_id.clone(),
            })
            .await
            .expect_err("Native RPC without JWT must fail before domain validation");
        assert_eq!(error.code(), tonic::Code::Unauthenticated);
        assert_eq!(error.message(), "native caller authentication failed");
        let metrics = http_get(metrics_port, "/metrics").expect("read backend metrics");
        assert!(metrics.contains(
            "novarocks_native_authentication_failures_total{reason=\"authentication\"} 1"
        ));

        let channel = connect_live_channel(grpc_port).await;
        let service = tonic::service::interceptor::InterceptedService::new(
            channel,
            test_backend_native_trust().client_interceptor(),
        );
        let mut grpc = tonic::client::Grpc::new(service);
        grpc.ready()
            .await
            .expect("authenticated test client is ready");
        let result: Result<tonic::Response<HeartbeatResponse>, tonic::Status> = grpc
            .unary(
                tonic::Request::new(HeartbeatRequest {
                    expected_process_id: host.process_descriptor().as_proto().process_id.clone(),
                }),
                "/novarocks.NovaRocksGrpc/Unknown"
                    .parse()
                    .expect("valid unknown native RPC path"),
                tonic::codec::ProstCodec::default(),
            )
            .await;
        let error = result.expect_err("valid JWT must reach the Native fallback");
        assert_eq!(error.code(), tonic::Code::Unimplemented);

        host.shutdown().expect("native backend shutdown");
    }

    /// The probe is a family only the BE role registry holds, and one that
    /// renders before any query runs: `..._tasks_created_total` is a counter,
    /// so a freshly opened host already publishes it at zero. That makes an
    /// empty result on the management port a real failure rather than an idle
    /// process, and its presence on the native port a real leak.
    #[test]
    fn application_exposes_metrics_only_on_the_management_listener() {
        const BACKEND_OWNED_FAMILY: &str = "novarocks_backend_task_execution_tasks_created_total";

        let _live_host = LIVE_HOST_TEST.lock().expect("live host test lock");
        let grpc_port = unused_port();
        let metrics_port = unused_port();
        let mut config = backend_config(grpc_port, grpc_port);
        config.metrics_http_port = metrics_port;
        let host = BackendApplicationHost::open(config, test_data_runtime())
            .expect("native backend host starts");

        if let Ok(native_response) = http_get(grpc_port, "/metrics") {
            assert!(!native_response.contains(BACKEND_OWNED_FAMILY));
        }

        let management_response =
            http_get(metrics_port, "/metrics").expect("read management metrics");
        assert!(management_response.starts_with("HTTP/1.1 200"));
        assert!(management_response.contains(BACKEND_OWNED_FAMILY));

        host.shutdown().expect("native backend shutdown");
    }

    #[test]
    fn supervision_error_remains_primary_when_shutdown_also_fails() {
        let error = combine_primary_and_shutdown(
            Err(BackendApplicationError::new(
                BackendApplicationErrorKind::Supervision,
                "gRPC server exited",
            )),
            Err(BackendApplicationError::new(
                BackendApplicationErrorKind::Shutdown,
                "gRPC join failed",
            )),
        )
        .expect_err("supervision failure must be returned");

        assert_eq!(error.kind(), BackendApplicationErrorKind::Supervision);
        assert!(
            error
                .to_string()
                .contains("cleanup failed: Shutdown: gRPC join failed")
        );
    }
}
