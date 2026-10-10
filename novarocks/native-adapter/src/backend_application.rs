use std::fmt;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use novarocks_connector_contract::PureProviderProgramCatalog;
use novarocks_execution::runtime::execution_runtime::{ExecutionRuntime, ExecutionRuntimeConfig};
use novarocks_execution_contract::{BackendProcessDescriptor, RuntimeEndpoint};
use novarocks_functions::{ConstantPolicy, PureEngineFunctionCatalog};
use novarocks_memory::MemoryAuthority;
use novarocks_native_trust::{NativeProcessIdentity, NativeTrust};
use novarocks_plan_codec::physical_package_v2::PackageDecodeLimits;
use novarocks_plan_codec::resource_preflight_v2::FragmentDecodeResourceModel;
use novarocks_spi::connector::ConnectorExecutionRoleBindingFactory;
use novarocks_type_contract::{CompileControlError, CompilePhase, PureCompileControl};
use novarocks_types::{AdvertiseEndpoint, BackendProcessId, NativeCompatibilityId, NativeEndpoint};
use novarocks_worker::sink_commit::ConfiguredWorkerSinkCommitPort;
use novarocks_worker::{
    CatalogManager, CatalogManagerConfig, ConnectorExecutionRoleBindingFactorySet,
    WorkerAdmissionEpochAuthority, WorkerDeadlineSupervisor, WorkerDrainState,
    WorkerResultRetainedLimits,
};

// Native backend application composition and lifecycle owner.

use crate::backend_metrics::BackendMetricsRegistry;
use crate::backend_rpc_service::BackendRpcService;
use crate::backend_task_execution::{CompiledPackageCompiler, CompiledPackageInterpreter};
use crate::fragment_result_writer::native_result_writer;
use crate::management_http::MetricsHttpServer;
use crate::native_transport_admission::{NativeTransportAdmission, TransportClass};
use crate::root_result_session::{RootProducerLimits, RootProducerPool};
use crate::runtime_filter_ingress::native_runtime_filter_envelope_ingress;
use crate::runtime_filter_participant::NativeRuntimeFilterParticipantFactory;
use crate::static_package_admission::StaticPackageAdmission;
use crate::task_execution_observation::backend_task_execution_ports;
use crate::task_protocol_ingress::RegistryTaskExecutionIngress;
use crate::{
    BackendDataRuntime, BackendNativeTransport, NativeRpcServerHandle,
    backend_announce::BackendAnnounceSupervisor, backend_heartbeat::BackendHeartbeatResponder,
    backend_readiness::wait_for_backend_native_endpoint_ready,
    runtime_filter_rpc::BackendRuntimeFilterEnvelopeIngress, task_protocol::TaskExecutionIngress,
    task_protocol_fault::RestartAfterEstablishTaskCreationGate,
};
use novarocks_execution::exec::expr::agg::SealedExecutionFunctionSet;
use novarocks_execution::runtime::fragment::io::{
    ExchangeReceiverPort, ExecutionRuntimeExchangeReceiverPort,
};
#[cfg(test)]
use novarocks_execution_contract::task_execution::creation::TaskCreationInput;
#[cfg(test)]
use novarocks_execution_contract::task_execution::descriptor::TaskDescriptor;
#[cfg(test)]
use novarocks_execution_contract::task_execution::identity::QueryContextRef;
#[cfg(test)]
use novarocks_execution_contract::task_execution::operation::{
    QueryContextDomainUpdate, TaskDomainUpdate,
};
#[cfg(test)]
use novarocks_execution_contract::task_execution::status::TaskFailureCategory;
use novarocks_spi::connector::WriteCommitEvidenceLimits;
#[cfg(test)]
use novarocks_worker::ReleasedContextEvidence;
#[cfg(test)]
use novarocks_worker::TaskStatusReporter;
#[cfg(test)]
use novarocks_worker::{HostRejection, RunnableTask, SharedFactsRequest, TaskExecutionHost};
use novarocks_worker::{QueryContextHost, TaskExecutionRegistry, TaskExecutionRegistryConfig};

const READINESS_TIMEOUT: Duration = Duration::from_secs(5);
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
    pub control_grpc_port: u16,
    pub metrics_http_port: u16,
    pub native_ingress: crate::native_server::NativeIngressConfig,
    pub advertise_endpoint: AdvertiseEndpoint,
    pub advertise_control_endpoint: AdvertiseEndpoint,
    /// Server-resolved Native caller authentication and transport material.
    /// Backend receives this immutable capability and never reads trust source
    /// configuration or credentials itself.
    pub native_trust: Arc<NativeTrust>,
    /// Server-resolved compatibility identity frozen before role composition.
    pub native_compatibility_id: NativeCompatibilityId,
    /// Process-wide immutable engine function metadata and implementations.
    pub function_set: Arc<SealedExecutionFunctionSet>,
    /// The one memory capacity authority this OS process was given.
    ///
    /// The backend receives a handle, never the right to install a second
    /// authority: a capacity bound belongs to the process, and a role that
    /// could mint its own would be governing a budget nobody else can see.
    pub memory_authority: Arc<MemoryAuthority>,
    pub native_transport: BackendNativeTransport,
    /// Exact FE native ingress used exclusively for authenticated membership announce.
    pub frontend_endpoint: NativeEndpoint,
    pub announce_interval: Duration,
    pub announce_initial_backoff: Duration,
    pub announce_max_backoff: Duration,
    /// Server-resolved per-fragment terminal write evidence budget.
    pub write_commit_evidence_limits: WriteCommitEvidenceLimits,
    /// Server-validated hierarchy for retained native query results.
    pub result_retained_limits: WorkerResultRetainedLimits,
    /// Independent finite CPU/installation/stack resources for root encoding.
    pub root_producer_limits: RootProducerLimits,
    /// Active and retained exact late-frame records use one Worker budget.
    pub inbound_capability_limits: novarocks_worker::TaskInboundCapabilityLimits,
    pub preparation_limits: novarocks_worker::TaskPreparationLimits,
    pub execution_runtime_config: ExecutionRuntimeConfig,
    pub scan_preparation_config: novarocks_worker::ScanPreparationConfig,
    /// The Server's scan I/O runtime. Typed scan streams are polled and
    /// closed inside its context, where their I/O waits and timers live.
    pub scan_stream_runtime: tokio::runtime::Handle,
    /// Server-frozen bounded failure and provider-bind policy for the BE
    /// catalog manager.
    pub catalog_manager_config: CatalogManagerConfig,
    /// Provider-owned complete BE role factories. The backend seals exactly
    /// one factory per provider kind before query lifecycle admission.
    pub execution_role_binding_factories: Vec<Arc<dyn ConnectorExecutionRoleBindingFactory>>,
    /// The one static plan interpreter Server composed for this process.
    pub static_plan_interpreter: BackendStaticPlanInterpreter,
    /// This process's memory readings for the `/metrics` endpoint, supplied
    /// by the process that owns the allocator and the probes.
    pub process_memory: crate::backend_metrics::ProcessMemoryObservation,
}

/// The one static plan interpreter a backend process composes.
///
/// Server selects it once, at composition. A task's carrier never selects it,
/// and no process composes both: the plan-tree decoder refuses a package
/// carrier and the compiled-package interpreter refuses a plan tree.
pub enum BackendStaticPlanInterpreter {
    /// The production plan-tree decoder. No package carrier is gated.
    PlanTree,
    /// The compiled-package interpreter and its ingress package gate.
    CompiledPackage(CompiledStaticPlan),
}

impl BackendStaticPlanInterpreter {
    const fn label(&self) -> &'static str {
        match self {
            Self::PlanTree => "plan_tree",
            Self::CompiledPackage(_) => "compiled_package",
        }
    }
}

/// The compiled-package interpreter and the ingress gate, built over the
/// process's one package decode model.
pub struct CompiledStaticPlan {
    compiler: Arc<dyn CompiledPackageCompiler>,
    admission: Arc<StaticPackageAdmission>,
}

impl CompiledStaticPlan {
    /// Builds the one decode model and shares it between the package receiver
    /// and the ingress gate. The gate admits with the receiver's own wire
    /// limits, so both refuse the same bytes. Every input is host-authored;
    /// nothing here supplies a default.
    pub fn try_new<E>(
        decode_limits: PackageDecodeLimits,
        functions: Arc<PureEngineFunctionCatalog>,
        providers: Arc<PureProviderProgramCatalog<E>>,
        constants: ConstantPolicy,
    ) -> Result<Self, BackendApplicationError>
    where
        E: std::error::Error + Send + Sync + 'static,
        PureProviderProgramCatalog<E>: Send + Sync,
    {
        let model = Arc::new(
            FragmentDecodeResourceModel::try_new(&CompositionControl).map_err(|error| {
                BackendApplicationError::new(
                    BackendApplicationErrorKind::Configuration,
                    format!("build the fragment package decode model: {error}"),
                )
            })?,
        );
        let admission = Arc::new(StaticPackageAdmission::new(
            Arc::clone(&model),
            decode_limits.wire,
        ));
        let compiler: Arc<dyn CompiledPackageCompiler> = Arc::new(CompiledPackageInterpreter::new(
            model,
            decode_limits,
            functions,
            providers,
            constants,
        ));
        Ok(Self {
            compiler,
            admission,
        })
    }
}

/// Composition runs once, before any listener opens, over a statically sized
/// generated schema; it has no caller that could cancel it.
struct CompositionControl;

impl PureCompileControl for CompositionControl {
    fn checkpoint(&self, _phase: CompilePhase, _units: u32) -> Result<(), CompileControlError> {
        Ok(())
    }
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

    pub fn with_cleanup_context(mut self, cleanup_error: impl fmt::Display) -> Self {
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
    grpc_server: NativeRpcServerHandle,
    control_grpc_server: NativeRpcServerHandle,
    execution_runtime: Arc<ExecutionRuntime>,
    root_producer_pool: Arc<RootProducerPool>,
    task_completion_supervisor: Arc<novarocks_worker::TaskCompletionSupervisor>,
    task_deadline_tick: WorkerDeadlineSupervisor,
    metrics_http_server: MetricsHttpServer,
    process_descriptor: BackendProcessDescriptor,
    drain: Arc<WorkerDrainState>,
    announce_task: BackendAnnounceSupervisor,
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
    drain: Arc<WorkerDrainState>,
    execution_runtime: Arc<ExecutionRuntime>,
    root_producer_pool: Arc<RootProducerPool>,
    exchange_receiver_port: Arc<dyn ExchangeReceiverPort>,
    task_execution_registry: Arc<TaskExecutionRegistry>,
    task_completion_supervisor: Arc<novarocks_worker::TaskCompletionSupervisor>,
    task_execution_ingress: Arc<dyn TaskExecutionIngress>,
    /// The task substrate's exchange-destination authority. The RPC data
    /// plane needs it directly: without it no created task can receive an
    /// exchange frame, because the frozen descriptor is the only place a
    /// task's inbound topology exists.
    task_inbound_capabilities: Arc<novarocks_worker::TaskInboundCapabilities>,
    /// The task substrate's runtime-filter participant owner. The RPC ingress
    /// needs it directly, for the same reason: an `EstablishQueryContext`
    /// install is the only place a task-protocol query's participant exists.
    query_context_host: Arc<crate::backend_task_execution::NativeQueryContextHost>,
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
    fn close_context_admission(&self, _context: QueryContextRef) {}

    fn retire_context_execution(&self, _context: QueryContextRef) {}

    fn forget_context_admission(&self, _context: QueryContextRef) {}

    fn install_receiver(
        &self,
        _descriptor: &TaskDescriptor,
        _input: TaskCreationInput,
        _preparation: &novarocks_worker::PreparationControlLoan<'_>,
    ) -> Result<novarocks_worker::PreparedTaskInstallation, HostRejection> {
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

struct BackendExecutionRuntimeInput {
    config: ExecutionRuntimeConfig,
    function_set: Arc<SealedExecutionFunctionSet>,
    memory_authority: Arc<MemoryAuthority>,
}

impl BackendExecutionRuntimeInput {
    fn new(
        config: ExecutionRuntimeConfig,
        function_set: Arc<SealedExecutionFunctionSet>,
        memory_authority: Arc<MemoryAuthority>,
    ) -> Self {
        Self {
            config,
            function_set,
            memory_authority,
        }
    }
}

#[expect(
    clippy::too_many_arguments,
    reason = "Role composition keeps the original retained budget and independent runtime inputs explicit."
)]
fn compose_backend_application_services(
    data_runtime: BackendDataRuntime,
    backend_process_id: BackendProcessId,
    execution: BackendExecutionRuntimeInput,
    native_compatibility_id: NativeCompatibilityId,
    write_commit_evidence_limits: WriteCommitEvidenceLimits,
    result_retained_limits: WorkerResultRetainedLimits,
    result_retained_budget: Arc<novarocks_worker::result_buffer::ResultRetainedBudget>,
    root_producer_limits: RootProducerLimits,
    inbound_capability_limits: novarocks_worker::TaskInboundCapabilityLimits,
    preparation_limits: novarocks_worker::TaskPreparationLimits,
    scan_stream_host: novarocks_worker::ScanStreamHost,
    catalog_manager_config: CatalogManagerConfig,
    execution_role_binding_factories: &[Arc<dyn ConnectorExecutionRoleBindingFactory>],
    static_plan_interpreter: BackendStaticPlanInterpreter,
) -> Result<BackendApplicationServices, BackendApplicationError> {
    let BackendExecutionRuntimeInput {
        config: execution_runtime_config,
        function_set,
        memory_authority,
    } = execution;
    let execution_runtime = Arc::new(
        ExecutionRuntime::new(
            execution_runtime_config,
            Arc::clone(&function_set),
            Arc::clone(&memory_authority),
        )
        .map_err(|error| {
            BackendApplicationError::new(BackendApplicationErrorKind::Configuration, error)
        })?,
    );
    let drain = Arc::new(WorkerDrainState::new());
    let exchange_receiver_port: Arc<dyn ExchangeReceiverPort> = Arc::new(
        ExecutionRuntimeExchangeReceiverPort::new(Arc::clone(&execution_runtime)),
    );
    let execution_role_binding_factories = Arc::new(
        ConnectorExecutionRoleBindingFactorySet::try_new(
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
    let catalog_manager = Arc::new(CatalogManager::try_new(catalog_manager_config).map_err(
        |error| {
            BackendApplicationError::new(
                BackendApplicationErrorKind::Configuration,
                format!("compose backend catalog manager: {error}"),
            )
        },
    )?);
    // One task protocol owner per process, on this process's own identity and
    // its monotonic clock, routed to the real execution owners.
    let runtime_filter_factory = NativeRuntimeFilterParticipantFactory::new(data_runtime.clone());
    let context_host = Arc::new(crate::backend_task_execution::NativeQueryContextHost::new(
        Arc::clone(&catalog_manager),
        Arc::clone(&execution_role_binding_factories),
        Arc::new(move |execution_id, contribution| {
            runtime_filter_factory.install(execution_id, contribution)
        }),
        data_runtime.clone(),
    ));
    let inbound_capabilities =
        novarocks_worker::TaskInboundCapabilities::with_capacity_limits(inbound_capability_limits);
    let root_producer_pool = RootProducerPool::try_new(
        root_producer_limits.threads(),
        root_producer_limits.positions(),
        root_producer_limits.stack_bytes(),
        Arc::clone(&result_retained_budget),
    )
    .map_err(|error| {
        BackendApplicationError::new(
            BackendApplicationErrorKind::Configuration,
            format!("compose bounded root producer pool: {error}"),
        )
    })?;
    let task_execution_registry_config = TaskExecutionRegistryConfig::for_process(
        backend_process_id,
        novarocks_task_codec::TransportBudget::DEFAULT.max_tasks_per_context(),
        novarocks_task_codec::TransportBudget::DEFAULT.max_active_tasks_per_backend(),
    );
    let task_execution_registry_config =
        task_execution_registry_config.with_preparation_limits(preparation_limits);
    let completion_capacity =
        novarocks_task_codec::TransportBudget::DEFAULT.max_active_tasks_per_backend();
    task_execution_registry_config
        .validate_completion_capacity(completion_capacity)
        .map_err(|error| {
            BackendApplicationError::new(
                BackendApplicationErrorKind::Configuration,
                format!("validate task capacity limits: {error}"),
            )
        })?;
    let task_completion_supervisor = novarocks_worker::TaskCompletionSupervisor::start(
        data_runtime.handle().clone(),
        completion_capacity,
    );
    tracing::info!(
        static_plan_interpreter = static_plan_interpreter.label(),
        "composed the backend static plan interpreter"
    );
    let (compiled_package_compiler, static_package_admission) = match static_plan_interpreter {
        BackendStaticPlanInterpreter::PlanTree => (None, None),
        BackendStaticPlanInterpreter::CompiledPackage(CompiledStaticPlan {
            compiler,
            admission,
        }) => (Some(compiler), Some(admission)),
    };
    let execution_host = crate::backend_task_execution::NativeTaskExecutionHost::new(
        novarocks_native_adapter::native_fragment_query::NativeFragmentQueryRuntime::global(
            Arc::clone(&memory_authority),
        ),
        Arc::clone(&context_host) as Arc<dyn crate::backend_task_execution::TaskQueryContextFacts>,
        Arc::clone(&inbound_capabilities),
        novarocks_native_adapter::exchange_transmitter::grpc_exchange_transmitter(
            data_runtime.clone(),
            Duration::from_millis(execution_runtime.config().exchange_wait_ms),
        ),
        native_result_writer(
            Arc::clone(&result_retained_budget),
            result_retained_limits.per_root(),
        ),
        result_retained_budget,
        result_retained_limits,
        Arc::clone(&root_producer_pool),
        Arc::clone(&exchange_receiver_port),
        Arc::new(ConfiguredWorkerSinkCommitPort::new(
            write_commit_evidence_limits,
        )),
        Arc::clone(&execution_runtime),
        scan_stream_host,
        Arc::clone(&task_completion_supervisor),
    );
    let execution_host = Arc::new(match compiled_package_compiler {
        None => execution_host,
        Some(compiler) => execution_host.with_compiled_package_compiler(compiler),
    });
    let task_execution_registry = TaskExecutionRegistry::with_process_clock_and_task_creation_gate(
        task_execution_registry_config,
        Arc::clone(&context_host) as Arc<dyn QueryContextHost>,
        execution_host,
        backend_task_execution_ports(),
        Arc::new(RestartAfterEstablishTaskCreationGate),
    );
    let task_execution_ingress: Arc<dyn TaskExecutionIngress> = match static_package_admission {
        None => RegistryTaskExecutionIngress::new(
            Arc::clone(&task_execution_registry),
            native_compatibility_id,
        ),
        Some(admission) => RegistryTaskExecutionIngress::with_static_package_admission(
            Arc::clone(&task_execution_registry),
            native_compatibility_id,
            admission,
        ),
    };
    Ok(BackendApplicationServices {
        backend_process_id,
        drain,
        execution_runtime,
        root_producer_pool,
        exchange_receiver_port,
        task_execution_registry,
        task_completion_supervisor,
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
        Self::connectable_bound_address(self.grpc_server.bound_addr())
    }

    pub fn connectable_control_native_endpoint(&self) -> SocketAddr {
        Self::connectable_bound_address(self.control_grpc_server.bound_addr())
    }

    fn connectable_bound_address(bound: SocketAddr) -> SocketAddr {
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
        for (component, failure) in [
            ("grpc_listener", self.grpc_server.poll_failure()),
            (
                "control_grpc_listener",
                self.control_grpc_server.poll_failure(),
            ),
            ("metrics_http", self.metrics_http_server.poll_failure()),
            (
                "task_deadline_tick",
                Ok(self.task_deadline_tick.poll_failure()),
            ),
            (
                "task_completion_supervisor",
                Ok(self.task_completion_supervisor.poll_failure()),
            ),
        ] {
            match failure {
                Ok(Some(error)) => {
                    tracing::error!(component, %error, "supervised backend component failed");
                    return Ok(Some(BackendApplicationError::new(
                        BackendApplicationErrorKind::Supervision,
                        format!("{component}: {error}"),
                    )));
                }
                Ok(None) => {}
                Err(error) => {
                    tracing::error!(component, %error, "supervised backend component failed");
                    return Err(BackendApplicationError::new(
                        BackendApplicationErrorKind::Supervision,
                        format!("{component}: {error}"),
                    ));
                }
            }
        }
        Ok(None)
    }

    pub fn shutdown(mut self) -> Result<(), BackendApplicationError> {
        self.announce_task.stop();
        self.task_deadline_tick.stop();
        // Close ingress before draining drivers, so no CreateTask can
        // install a new completion slot behind the shutdown boundary.
        self.grpc_server.begin_stop();
        self.control_grpc_server.begin_stop();
        let listener_shutdown =
            combine_shutdown_results(self.grpc_server.stop(), self.control_grpc_server.stop());
        // Cancel and join root producers before driver pending_finish waits.
        // Completion of this join does not free pool backings; their owner is
        // still retained here until Host destruction.
        let producer_result = self.root_producer_pool.shutdown();
        let execution_result = self.execution_runtime.shutdown_driver_execution();
        // Driver shutdown publishes every actual-stop fact. Only after that
        // may the fixed completion owner drain its exact slots and return.
        let completion_result = self.task_completion_supervisor.shutdown();
        let metrics_result = self.metrics_http_server.stop();
        combine_shutdown_results(
            combine_shutdown_results(
                combine_shutdown_results(
                    combine_shutdown_results(listener_shutdown, producer_result),
                    execution_result,
                ),
                completion_result,
            ),
            metrics_result,
        )
        .map_err(|error| BackendApplicationError::new(BackendApplicationErrorKind::Shutdown, error))
    }

    fn open_with_readiness_timeout(
        config: BackendServerConfig,
        data_runtime: BackendDataRuntime,
        readiness_timeout: Duration,
    ) -> Result<Self, BackendApplicationError> {
        let BackendServerConfig {
            bind_host,
            grpc_port,
            control_grpc_port,
            metrics_http_port,
            native_ingress,
            advertise_endpoint,
            advertise_control_endpoint,
            native_trust,
            native_compatibility_id,
            function_set,
            memory_authority,
            native_transport,
            frontend_endpoint,
            announce_interval,
            announce_initial_backoff,
            announce_max_backoff,
            write_commit_evidence_limits,
            result_retained_limits,
            root_producer_limits,
            inbound_capability_limits,
            preparation_limits,
            execution_runtime_config,
            scan_preparation_config,
            scan_stream_runtime,
            catalog_manager_config,
            execution_role_binding_factories,
            static_plan_interpreter,
            process_memory,
        } = config;
        if control_grpc_port == 0
            || control_grpc_port == grpc_port
            || control_grpc_port == metrics_http_port
        {
            return Err(BackendApplicationError::new(
                BackendApplicationErrorKind::Configuration,
                "backend Native control port must be nonzero and distinct from data and management ports",
            ));
        }
        let readiness_endpoint = novarocks_types::NativeEndpoint::from_host_port(
            &advertise_endpoint.host,
            advertise_endpoint.port,
        )
        .map_err(|error| {
            BackendApplicationError::new(
                BackendApplicationErrorKind::Configuration,
                format!("invalid advertised Native readiness endpoint: {error}"),
            )
        })?;
        let control_readiness_endpoint = NativeEndpoint::from_host_port(
            &advertise_control_endpoint.host,
            advertise_control_endpoint.port,
        )
        .map_err(|error| {
            BackendApplicationError::new(
                BackendApplicationErrorKind::Configuration,
                format!("invalid advertised Native control readiness endpoint: {error}"),
            )
        })?;
        if readiness_endpoint == control_readiness_endpoint {
            return Err(BackendApplicationError::new(
                BackendApplicationErrorKind::Configuration,
                "backend advertised Native control and data endpoints must be distinct",
            ));
        }
        crate::native_fd_capacity::verify_native_file_descriptor_capacity(
            novarocks_proto_codec::native_rpc::NativeEndpointDomain::BackendData,
        )
        .map_err(|error| {
            BackendApplicationError::new(
                BackendApplicationErrorKind::Configuration,
                format!("verify backend Native file descriptor baseline: {error}"),
            )
        })?;
        let bounded_root_process_bytes = usize::try_from(
            novarocks_execution_contract::native_result_support::NativeResultSupportGeometry::V1
                .root_joint_retained_bytes_per_process,
        )
        .map_err(|_| {
            BackendApplicationError::new(
                BackendApplicationErrorKind::Configuration,
                "bounded root process capacity exceeds the target",
            )
        })?;
        if result_retained_limits.per_process().get() < bounded_root_process_bytes {
            return Err(BackendApplicationError::new(
                BackendApplicationErrorKind::Configuration,
                "backend result retained process capacity cannot cover the frozen bounded root profile",
            ));
        }
        let result_retained_budget = novarocks_worker::result_buffer::ResultRetainedBudget::new(
            result_retained_limits.per_process(),
        );
        // The frozen transport geometry is refused here if inconsistent; the
        // count part of its envelope is logged. Per-object coefficients are
        // frozen by P00b, so no byte bound is claimed before then.
        let transport_geometry =
            crate::native_transport_geometry::validate_native_transport_geometry(
                &novarocks_execution_contract::native_result_support::NativeResultSupportGeometry::V1,
            )
            .map_err(|error| {
                BackendApplicationError::new(
                    BackendApplicationErrorKind::Configuration,
                    format!("validate Native transport geometry: {error}"),
                )
            })?;
        // One process-wide Native connection admission: physical and handshake
        // positions per transport class and served stream positions per lane,
        // held outside the HTTP/2 stack. Metrics only observe it.
        let transport_admission = NativeTransportAdmission::backend(Some(
            crate::backend_metrics::backend_native_transport_observer(),
        ))
        .map_err(|error| {
            BackendApplicationError::new(
                BackendApplicationErrorKind::Configuration,
                format!("compose Native transport admission: {error}"),
            )
        })?;
        tracing::info!(
            data_positions = transport_admission.positions(TransportClass::Data),
            control_positions = transport_admission.positions(TransportClass::Control),
            data_handshakes = transport_admission.handshake_positions(TransportClass::Data),
            control_handshakes = transport_admission.handshake_positions(TransportClass::Control),
            connections = transport_geometry.backend.connections,
            streams = transport_geometry.backend.streams,
            structural_bytes = transport_geometry.backend.structural_bytes,
            native_sockets = transport_geometry.backend_socket_positions,
            coefficients_frozen = transport_geometry.backend.coefficients.is_some(),
            "Native transport admission composed; envelope count part only until P00b \
             freezes per-object coefficients"
        );
        // The same process identity signs Native calls and appears in the
        // announce, heartbeat and local execution owners. Bind before channels
        // or role workers can issue an outbound RPC.
        let backend_process_id = BackendProcessId::new_v7();
        let native_identity = NativeProcessIdentity::Backend(backend_process_id);
        native_trust
            .bind_process_identity(native_identity)
            .map_err(|error| {
                BackendApplicationError::new(BackendApplicationErrorKind::Configuration, error)
            })?;
        data_runtime
            .native_trust()
            .bind_process_identity(native_identity)
            .map_err(|error| {
                BackendApplicationError::new(BackendApplicationErrorKind::Configuration, error)
            })?;
        let data_runtime = data_runtime
            .with_transport_admission(transport_admission.clone())
            .map_err(|error| {
                BackendApplicationError::new(
                    BackendApplicationErrorKind::Configuration,
                    format!("compose Native channel cache: {error}"),
                )
            })?;
        let readiness_runtime = data_runtime.clone();
        let services = compose_backend_application_services(
            data_runtime,
            backend_process_id,
            BackendExecutionRuntimeInput::new(
                execution_runtime_config,
                function_set,
                memory_authority,
            ),
            native_compatibility_id,
            write_commit_evidence_limits,
            result_retained_limits,
            result_retained_budget,
            root_producer_limits,
            inbound_capability_limits,
            preparation_limits,
            novarocks_worker::ScanStreamHost::new(scan_preparation_config, scan_stream_runtime),
            catalog_manager_config,
            &execution_role_binding_factories,
            static_plan_interpreter,
        )?;
        let process_descriptor = BackendProcessDescriptor::try_new(
            services.backend_process_id,
            RuntimeEndpoint::new(
                advertise_endpoint.host.clone(),
                i32::from(advertise_endpoint.port),
            )
            .map_err(|error| {
                BackendApplicationError::new(
                    BackendApplicationErrorKind::Configuration,
                    format!("resolve backend process endpoint: {error}"),
                )
            })?,
            RuntimeEndpoint::new(
                advertise_control_endpoint.host.clone(),
                i32::from(advertise_control_endpoint.port),
            )
            .map_err(|error| {
                BackendApplicationError::new(
                    BackendApplicationErrorKind::Configuration,
                    format!("resolve backend process control endpoint: {error}"),
                )
            })?,
            native_trust.deployment_id().as_str(),
            novarocks_version::native_build_identity(),
            native_compatibility_id,
            preparation_limits.per_context(),
        )
        .map_err(|error| {
            BackendApplicationError::new(
                BackendApplicationErrorKind::Configuration,
                format!("construct backend process descriptor: {error}"),
            )
        })?;
        // The composed host has installed all V1 domain producers and their
        // exact exit guards before this immutable descriptor can be announced.
        let root_support =
            novarocks_execution_contract::native_result_support::BoundedRootSupport::new(
                process_descriptor.control_endpoint().clone(),
                novarocks_result_contract::RootProfileId::V1,
            );
        let process_descriptor = process_descriptor
            .with_bounded_root_support(root_support)
            .map_err(|error| {
                BackendApplicationError::new(
                    BackendApplicationErrorKind::Configuration,
                    format!("advertise backend bounded root support: {error}"),
                )
            })?;
        let metrics_registry = Arc::new(
            BackendMetricsRegistry::new()
                .and_then(|registry| registry.with_process_memory(process_memory))
                .map_err(|error| {
                    BackendApplicationError::new(BackendApplicationErrorKind::Configuration, error)
                })?
                .with_native_query_resources(Arc::new(|| {
                    novarocks_worker::query_context::query_context_manager()
                        .native_execution_resource_snapshot()
                }))
                .with_worker_reservations(
                    services
                        .task_execution_registry
                        .admission_reservation_observation(),
                    services
                        .task_execution_registry
                        .config()
                        .admission_tickets
                        .max_reservations(),
                )
                .with_worker_registry_lock(
                    services.task_execution_registry.registry_lock_observation(),
                )
                .with_task_preparation(Arc::clone(&services.task_execution_registry)),
        );
        let metrics_http_server =
            MetricsHttpServer::start(&bind_host, metrics_http_port, metrics_registry).map_err(
                |error| BackendApplicationError::new(BackendApplicationErrorKind::Start, error),
            )?;
        // Started before the listener: the owner is reachable the moment its
        // RPCs are, and a deadline that elapses must already be decidable.
        let task_deadline_tick = WorkerDeadlineSupervisor::start(
            readiness_runtime.handle(),
            Arc::clone(&services.task_execution_registry)
                as Arc<dyn novarocks_worker::WorkerDeadlineAuthority>,
            TASK_DEADLINE_TICK_INTERVAL,
        );

        // The participant owner, because an attempt is reachable only
        // through the one that installed it, and every intent's participant is
        // installed by the query-context host.
        let runtime_filter_authority: Arc<
            dyn crate::runtime_filter_ingress::BackendRuntimeFilterParticipantAuthority,
        > = services.query_context_host.clone();
        let runtime_filter_ingress: Arc<dyn BackendRuntimeFilterEnvelopeIngress> =
            native_runtime_filter_envelope_ingress(runtime_filter_authority);
        let admission_epoch: Arc<dyn WorkerAdmissionEpochAuthority> =
            services.task_execution_registry.clone();
        let control_queue_capacity = native_ingress
            .control_running
            .checked_add(native_ingress.control_waiting)
            .ok_or_else(|| {
                BackendApplicationError::new(
                    BackendApplicationErrorKind::Configuration,
                    "native control execution capacity overflow",
                )
            })?;
        let control_executor = crate::native_control_executor::NativeControlExecutor::start(
            native_ingress.control_worker_threads,
            control_queue_capacity,
        )
        .map_err(|error| BackendApplicationError::new(BackendApplicationErrorKind::Start, error))?;
        let service = BackendRpcService::new(
            Arc::clone(&services.task_execution_ingress),
            Arc::clone(&services.query_context_host)
                as Arc<
                    dyn novarocks_native_adapter::catalog_prune_rpc::CatalogReachabilityAuthority,
                >,
            runtime_filter_ingress,
            Arc::clone(&services.exchange_receiver_port),
            Arc::clone(&services.task_inbound_capabilities),
            BackendHeartbeatResponder::new(
                process_descriptor.clone(),
                Arc::clone(&services.drain),
                admission_epoch,
            ),
            control_executor,
        );
        let mut grpc_server = match NativeRpcServerHandle::start_with_admission(
            &bind_host,
            grpc_port,
            service.clone(),
            Arc::clone(&native_trust),
            native_transport.incoming_adapter(),
            "backend",
            novarocks_proto_codec::native_rpc::NativeEndpointDomain::BackendData,
            "native-backend-grpc",
            novarocks_native_adapter::backend_metrics::record_backend_native_authentication_failure,
            novarocks_native_adapter::backend_metrics::record_backend_native_tls_handshake_failure,
            native_ingress,
            transport_admission.clone(),
            TransportClass::Data,
            Some(Arc::new(
                novarocks_native_adapter::root_result_reader::NativeRootResultReader::new(
                    Arc::clone(&services.task_execution_registry),
                ),
            )),
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

        let mut control_grpc_server = match NativeRpcServerHandle::start_with_admission(
            &bind_host,
            control_grpc_port,
            service,
            native_trust,
            native_transport.incoming_adapter(),
            "backend-control",
            novarocks_proto_codec::native_rpc::NativeEndpointDomain::BackendControl,
            "native-backend-control-grpc",
            novarocks_native_adapter::backend_metrics::record_backend_native_authentication_failure,
            novarocks_native_adapter::backend_metrics::record_backend_native_tls_handshake_failure,
            native_ingress,
            transport_admission,
            TransportClass::Control,
            None,
        ) {
            Ok(server) => server,
            Err(error) => {
                grpc_server.begin_stop();
                let primary = BackendApplicationError::new(
                    BackendApplicationErrorKind::Start,
                    format!(
                        "start native backend control gRPC server on {bind_host}:{control_grpc_port}: {error}"
                    ),
                );
                return Err(append_cleanup_results(
                    primary,
                    [grpc_server.stop(), metrics_http_server.stop()],
                ));
            }
        };

        for (endpoint, class) in [
            (readiness_endpoint, TransportClass::Data),
            (control_readiness_endpoint, TransportClass::Control),
        ] {
            if let Err(error) = wait_for_backend_native_endpoint_ready(
                &readiness_runtime,
                endpoint,
                class,
                readiness_timeout,
            ) {
                grpc_server.begin_stop();
                control_grpc_server.begin_stop();
                let listener_result = grpc_server.stop();
                let control_listener_result = control_grpc_server.stop();
                let metrics_result = metrics_http_server.stop();
                let primary = BackendApplicationError::new(
                    BackendApplicationErrorKind::Readiness,
                    format!("advertised endpoint readiness failed: {error}"),
                );
                return Err(append_cleanup_results(
                    primary,
                    [listener_result, control_listener_result, metrics_result],
                ));
            }
        }

        let announce_task = BackendAnnounceSupervisor::start(
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
                "NOVAROCKS_READY role=be grpc_port={grpc_port} control_grpc_port={control_grpc_port} advertise_host={} pid={}",
                advertise_endpoint.host,
                std::process::id()
            ),
            grpc_server,
            control_grpc_server,
            execution_runtime: services.execution_runtime,
            root_producer_pool: services.root_producer_pool,
            task_completion_supervisor: services.task_completion_supervisor,
            task_deadline_tick,
            metrics_http_server,
            process_descriptor,
            drain: services.drain,
            announce_task,
        })
    }
}

#[cfg(test)]
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

#[cfg(test)]
#[path = "backend_application_host_tests.rs"]
mod application_host_tests;

#[cfg(test)]
mod tests {
    use std::io::{Read, Write};
    use std::net::TcpListener;
    use std::sync::{Arc, LazyLock, Mutex};
    use std::time::Duration;

    use super::{
        BackendApplicationError, BackendApplicationErrorKind, BackendApplicationHost,
        BackendExecutionRuntimeInput, BackendServerConfig, BackendStaticPlanInterpreter,
        CompiledStaticPlan, QueryContextRef, UnroutedQueryContextHost, UnroutedTaskExecutionHost,
        combine_primary_and_shutdown, compose_backend_application_services,
    };
    use novarocks_execution::exec::expr::agg::SealedExecutionFunctionSet;
    use novarocks_execution::runtime::execution_runtime::ExecutionRuntimeConfig;
    use novarocks_native_adapter::backend_test_support::test_backend_native_trust;
    use novarocks_native_adapter::generated::nova_rocks_grpc_client::NovaRocksGrpcClient;
    use novarocks_native_adapter::{BackendDataRuntime, BackendNativeTransport};
    use novarocks_proto_models::novarocks as protocol;
    use novarocks_proto_models::novarocks::{HeartbeatRequest, HeartbeatResponse};
    use novarocks_spi::connector::WriteCommitEvidenceLimits;
    use novarocks_types::{AdvertiseEndpoint, BackendProcessId, NativeEndpoint};
    use novarocks_worker::{
        CatalogManagerConfig, TaskExecutionRegistry, TaskExecutionRegistryConfig,
        WorkerResultRetainedLimits,
    };
    use novarocks_worker::{ManualClock, WorkerDeadlineSupervisor, WorkerMonotonicClock};

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
            exchange_wait_ms: 1,
            exchange_io_threads: 1,
            exchange_io_max_inflight_bytes: 1,
            exchange_max_transmit_batched_bytes: 1,
            operator_buffer_chunks: 1,
            local_exchange_buffer_mem_limit_per_driver: 1,
            local_exchange_max_buffered_rows: -1,
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

    fn test_data_runtime() -> BackendDataRuntime {
        novarocks_native_adapter::backend_test_support::test_backend_data_runtime()
    }

    /// The tick is the only thing that turns elapsed time into a decision, so
    /// this asserts an actual reclamation rather than that a task was spawned.
    ///
    /// An abort of a context this backend never established leaves a retained
    /// terminal fence, which needs no execution binding at all. Nothing else
    /// reclaims it: the clock moves, and only a sweep can notice.
    #[test]
    fn the_deadline_tick_drives_the_task_owner_forward() {
        use novarocks_execution_contract::task_execution::identity::TaskOperationId;
        use novarocks_execution_contract::task_execution::operation::AbortQueryContext;
        use novarocks_execution_contract::task_execution::status::AbortCause;
        use novarocks_execution_contract::task_execution::transition::QueryContextState;
        use novarocks_types::identity::{FrontendProcessId, QueryExecutionId, QueryId};

        let backend = novarocks_types::BackendProcessId::new_v7();
        let clock = Arc::new(ManualClock::new());
        let registry = TaskExecutionRegistry::new(
            TaskExecutionRegistryConfig::for_process(
                backend,
                novarocks_task_codec::TransportBudget::DEFAULT.max_tasks_per_context(),
                novarocks_task_codec::TransportBudget::DEFAULT.max_active_tasks_per_backend(),
            ),
            Arc::clone(&clock) as Arc<dyn WorkerMonotonicClock>,
            Arc::new(UnroutedQueryContextHost),
            Arc::new(UnroutedTaskExecutionHost),
            novarocks_native_adapter::task_execution_observation::backend_task_execution_ports(),
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

        let mut tick = WorkerDeadlineSupervisor::start(
            test_data_runtime().handle(),
            Arc::clone(&registry) as Arc<dyn novarocks_worker::WorkerDeadlineAuthority>,
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
        let mut stream = std::net::TcpStream::connect(("127.0.0.1", port))?;
        stream.set_read_timeout(Some(Duration::from_secs(1)))?;
        write!(
            stream,
            "GET {path} HTTP/1.1\r\nHost: 127.0.0.1\r\nConnection: close\r\n\r\n"
        )?;
        let mut response = String::new();
        stream.read_to_string(&mut response)?;
        Ok(response)
    }

    fn backend_config(grpc_port: u16, advertise_port: u16) -> BackendServerConfig {
        let control_grpc_port = unused_port();
        BackendServerConfig {
            memory_authority: novarocks_native_adapter::backend_test_support::test_memory_authority(
            ),
            bind_host: "127.0.0.1".to_string(),
            grpc_port,
            control_grpc_port,
            metrics_http_port: unused_port(),
            native_ingress: crate::native_server::NativeIngressConfig::default(),
            advertise_endpoint: AdvertiseEndpoint {
                host: "127.0.0.1".to_string(),
                port: advertise_port,
            },
            advertise_control_endpoint: AdvertiseEndpoint {
                host: "127.0.0.1".to_string(),
                port: control_grpc_port,
            },
            native_trust: novarocks_native_adapter::backend_test_support::test_backend_native_trust(
            ),
            native_compatibility_id: novarocks_types::NativeCompatibilityId::new([0x71; 32]),
            function_set: test_execution_function_set(),
            native_transport: BackendNativeTransport::Plaintext,
            frontend_endpoint: NativeEndpoint::from_host_port("127.0.0.1", unused_port())
                .expect("valid frontend endpoint"),
            announce_interval: Duration::from_secs(60),
            announce_initial_backoff: Duration::from_millis(100),
            announce_max_backoff: Duration::from_secs(2),
            write_commit_evidence_limits: WriteCommitEvidenceLimits::default(),
            result_retained_limits: WorkerResultRetainedLimits::try_new(
                16 * 1024 * 1024,
                novarocks_execution_contract::native_result_support::NativeResultSupportGeometry::V1
                    .root_joint_retained_bytes_per_process as usize,
            )
            .expect("valid test result retained-byte limits"),
            root_producer_limits: super::RootProducerLimits::try_new(1, 64, 1024 * 1024)
                .expect("finite root producer limits"),
            inbound_capability_limits: novarocks_worker::TaskInboundCapabilityLimits::default(),
            preparation_limits: novarocks_worker::TaskPreparationLimits::default(),
            execution_runtime_config: execution_runtime_config(),
            scan_preparation_config: novarocks_worker::ScanPreparationConfig::try_new(
                64 * 1024 * 1024,
                4,
                Duration::from_millis(500),
                Duration::from_millis(500),
                Duration::from_millis(100),
            )
            .expect("valid scan preparation configuration"),
            scan_stream_runtime:
                novarocks_native_adapter::backend_test_support::test_scan_stream_runtime(),
            catalog_manager_config: CatalogManagerConfig::default(),
            execution_role_binding_factories: Vec::new(),
            static_plan_interpreter: BackendStaticPlanInterpreter::PlanTree,
            process_memory: novarocks_native_adapter::backend_test_support::test_process_memory(),
        }
    }

    #[test]
    fn installing_transport_admission_creates_a_new_channel_cache_generation() {
        let original = test_data_runtime();
        let admission = crate::native_transport_admission::NativeTransportAdmission::new().unwrap();
        let admitted = original.with_transport_admission(admission).unwrap();
        assert!(original.transport_admission().is_none());
        assert!(admitted.transport_admission().is_some());
        assert!(!original.channels().same_cache(admitted.channels()));
        let endpoint = NativeEndpoint::from_host_port("127.0.0.1", unused_port()).unwrap();
        let key = crate::native_client::NativeChannelKey::membership(endpoint);
        admitted.block_on(async {
            assert!(matches!(
                admitted
                    .channels()
                    .acquire(key.inline_identity().unwrap())
                    .await,
                Ok(crate::native_channel_cache::Election::Leader(_))
            ));
        });
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
        use crate::runtime_filter_ingress::native_runtime_filter_envelope_ingress;
        use novarocks_execution_contract::CredentialUpdate;
        use novarocks_execution_contract::task_execution::domain::{
            CodecOwnedContent, CredentialEpoch, CredentialLeaseId,
        };
        use novarocks_native_adapter::runtime_filter_test_support::{
            delivery_envelope_for_test, participant_execution_id,
        };
        use novarocks_proto_codec::FieldPath;
        use novarocks_proto_codec::catalog::CatalogSet;
        use novarocks_proto_models::filter;
        use novarocks_task_codec::domain::{WireContent, WireCredential};
        use novarocks_types::identity::FrontendProcessId;
        use novarocks_worker::QueryContextHost;
        use novarocks_worker::SharedFactsRequest;
        use novarocks_worker::runtime_filter::domain::BackendEnvelopeKind;

        let services = compose_backend_application_services(
            test_data_runtime(),
            BackendProcessId::new_v7(),
            BackendExecutionRuntimeInput::new(
                execution_runtime_config(),
                test_execution_function_set(),
                novarocks_native_adapter::backend_test_support::test_memory_authority(),
            ),
            novarocks_types::NativeCompatibilityId::new([0x71; 32]),
            WriteCommitEvidenceLimits::default(),
            WorkerResultRetainedLimits::try_new(16 * 1024 * 1024, 32 * 1024 * 1024)
                .expect("valid test result retained-byte limits"),
            novarocks_worker::result_buffer::ResultRetainedBudget::new(
                std::num::NonZeroUsize::new(32 * 1024 * 1024).unwrap(),
            ),
            super::RootProducerLimits::try_new(1, 64, 1024 * 1024)
                .expect("finite root producer limits"),
            novarocks_worker::TaskInboundCapabilityLimits::default(),
            novarocks_worker::TaskPreparationLimits::default(),
            novarocks_worker::ScanStreamHost::new(
                novarocks_worker::ScanPreparationConfig::try_new(
                    64 * 1024 * 1024,
                    4,
                    Duration::from_millis(500),
                    Duration::from_millis(500),
                    Duration::from_millis(100),
                )
                .expect("valid scan preparation configuration"),
                novarocks_native_adapter::backend_test_support::test_scan_stream_runtime(),
            ),
            CatalogManagerConfig::default(),
            &[],
            BackendStaticPlanInterpreter::PlanTree,
        )
        .expect("compose backend application services");

        // The exact attempt the fixture envelope is addressed to, established
        // the way the task protocol establishes one.
        let envelope = delivery_envelope_for_test(BackendEnvelopeKind::CompletedWithoutArtifact);
        let context = QueryContextRef::new(
            participant_execution_id(),
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
                WireCredential::decode(&[], FieldPath::root("credential"))
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

        let authority: Arc<
            dyn crate::runtime_filter_ingress::BackendRuntimeFilterParticipantAuthority,
        > = services.query_context_host.clone();
        let ingress = native_runtime_filter_envelope_ingress(authority);
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
    fn insufficient_joint_root_process_capacity_refuses_before_listener_binds() {
        let _live_host = LIVE_HOST_TEST.lock().expect("live host test lock");
        let data_port = unused_port();
        let mut config = backend_config(data_port, data_port);
        let control_port = config.control_grpc_port;
        let metrics_port = config.metrics_http_port;
        let required =
            novarocks_execution_contract::native_result_support::NativeResultSupportGeometry::V1
                .root_joint_retained_bytes_per_process as usize;
        config.result_retained_limits =
            WorkerResultRetainedLimits::try_new(16 * 1024 * 1024, required - 1).unwrap();
        let error = BackendApplicationHost::open(config, test_data_runtime()).unwrap_err();
        assert_eq!(error.kind(), BackendApplicationErrorKind::Configuration);
        assert!(
            error
                .to_string()
                .contains("cannot cover the frozen bounded root profile")
        );
        let _data = TcpListener::bind(("127.0.0.1", data_port)).unwrap();
        let _control = TcpListener::bind(("127.0.0.1", control_port)).unwrap();
        let _metrics = TcpListener::bind(("127.0.0.1", metrics_port)).unwrap();
    }

    #[test]
    fn control_port_validation_precedes_native_budget_and_all_listener_binds() {
        let _live_host = LIVE_HOST_TEST.lock().expect("live host test lock");
        for collision in [0, 1, 2] {
            let data_port = unused_port();
            let mut config = backend_config(data_port, data_port);
            let metrics_port = config.metrics_http_port;
            config.control_grpc_port = match collision {
                0 => 0,
                1 => data_port,
                _ => metrics_port,
            };
            // If validation moves behind stock construction, this different
            // original-budget error wins and the oracle fails.
            config.result_retained_limits =
                WorkerResultRetainedLimits::try_new(16 * 1024 * 1024, 32 * 1024 * 1024).unwrap();
            let error = BackendApplicationHost::open(config, test_data_runtime()).unwrap_err();
            assert_eq!(error.kind(), BackendApplicationErrorKind::Configuration);
            assert!(
                error
                    .to_string()
                    .contains("control port must be nonzero and distinct")
            );
            let _data = TcpListener::bind(("127.0.0.1", data_port)).unwrap();
            let _metrics = TcpListener::bind(("127.0.0.1", metrics_port)).unwrap();
        }
    }

    #[test]
    fn second_control_listener_bind_failure_joins_data_and_management_listeners() {
        let _live_host = LIVE_HOST_TEST.lock().expect("live host test lock");
        let occupied = TcpListener::bind("127.0.0.1:0").unwrap();
        let control_port = occupied.local_addr().unwrap().port();
        let data_port = unused_port();
        let mut config = backend_config(data_port, data_port);
        let metrics_port = config.metrics_http_port;
        config.control_grpc_port = control_port;
        config.advertise_control_endpoint.port = control_port;
        let error = BackendApplicationHost::open(config, test_data_runtime()).unwrap_err();
        assert_eq!(error.kind(), BackendApplicationErrorKind::Start);
        assert!(
            error
                .to_string()
                .contains("start native backend control gRPC server")
        );
        let _data = TcpListener::bind(("127.0.0.1", data_port)).unwrap();
        let _metrics = TcpListener::bind(("127.0.0.1", metrics_port)).unwrap();
        // The failed second bind never owns the other process's descriptor.
        assert_eq!(occupied.local_addr().unwrap().port(), control_port);
        drop(occupied);
        let _control = TcpListener::bind(("127.0.0.1", control_port)).unwrap();
    }

    #[test]
    fn control_readiness_failure_joins_both_native_and_management_listeners() {
        let _live_host = LIVE_HOST_TEST.lock().expect("live host test lock");
        let data_port = unused_port();
        let mut config = backend_config(data_port, data_port);
        let control_port = config.control_grpc_port;
        let metrics_port = config.metrics_http_port;
        config.advertise_control_endpoint.host = "127.0.0.2".to_string();
        let error = BackendApplicationHost::open_with_readiness_timeout(
            config,
            test_data_runtime(),
            Duration::from_millis(25),
        )
        .unwrap_err();
        assert_eq!(error.kind(), BackendApplicationErrorKind::Readiness);
        let _data = TcpListener::bind(("127.0.0.1", data_port)).unwrap();
        let _control = TcpListener::bind(("127.0.0.1", control_port)).unwrap();
        let _metrics = TcpListener::bind(("127.0.0.1", metrics_port)).unwrap();
    }

    #[test]
    fn readiness_failure_stops_and_joins_started_listener() {
        let _live_host = LIVE_HOST_TEST.lock().expect("live host test lock");
        let grpc_port = unused_port();
        let mut config = backend_config(grpc_port, grpc_port);
        let control_port = config.control_grpc_port;
        let metrics_port = config.metrics_http_port;
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
        let _control = TcpListener::bind(("127.0.0.1", control_port)).unwrap();
        let _metrics = TcpListener::bind(("127.0.0.1", metrics_port)).unwrap();
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
                expected_process_id: Some(
                    novarocks_proto_codec::membership::BackendProcessId::from_domain(
                        host.process_descriptor().process_id(),
                    )
                    .as_proto()
                    .clone(),
                ),
            })
            .await
            .expect_err("Native RPC without JWT must fail before domain validation");
        assert_eq!(error.code(), tonic::Code::Unauthenticated);
        assert_eq!(error.message(), "native caller authentication failed");
        let mut wrong_data_domain = NovaRocksGrpcClient::with_interceptor(
            connect_live_channel(grpc_port).await,
            test_backend_native_trust().client_interceptor(),
        );
        let error = wrong_data_domain
            .heartbeat(HeartbeatRequest::default())
            .await
            .unwrap_err();
        assert_eq!(error.code(), tonic::Code::Unimplemented);
        let mut wrong_control_domain = NovaRocksGrpcClient::with_interceptor(
            connect_live_channel(host.connectable_control_native_endpoint().port()).await,
            test_backend_native_trust().client_interceptor(),
        );
        let error = wrong_control_domain
            .apply_task_operations(protocol::ApplyTaskOperationsRequest::default())
            .await
            .unwrap_err();
        assert_eq!(error.code(), tonic::Code::Unimplemented);
        let metrics = http_get(metrics_port, "/metrics").expect("read backend metrics");
        assert!(metrics.contains(
            "novarocks_native_authentication_failures_total{reason=\"authentication\"} 1"
        ));

        let frontend_caller = test_backend_native_trust();
        frontend_caller
            .bind_process_identity(novarocks_native_trust::NativeProcessIdentity::Frontend(
                novarocks_types::FrontendProcessId::new_v7(),
            ))
            .unwrap();
        let mut authenticated = NovaRocksGrpcClient::with_interceptor(
            connect_live_channel(host.connectable_control_native_endpoint().port()).await,
            frontend_caller.client_interceptor(),
        );
        let heartbeat = authenticated
            .heartbeat(HeartbeatRequest {
                expected_process_id: Some(
                    novarocks_proto_codec::membership::BackendProcessId::from_domain(
                        host.process_descriptor().process_id(),
                    )
                    .as_proto()
                    .clone(),
                ),
            })
            .await
            .expect("authenticated heartbeat succeeds")
            .into_inner();
        let capability = heartbeat
            .admission_epoch_capability
            .expect("heartbeat publishes the current admission epoch capability");
        assert_eq!(capability.value.len(), 16);
        assert!(capability.value.iter().any(|byte| *byte != 0));

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
                    expected_process_id: Some(
                        novarocks_proto_codec::membership::BackendProcessId::from_domain(
                            host.process_descriptor().process_id(),
                        )
                        .as_proto()
                        .clone(),
                    ),
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
        assert!(
            management_response
                .contains("novarocks_backend_process_allocator_info{allocator=\"test\"} 1")
        );
        assert!(
            management_response
                .contains("novarocks_backend_process_counted_live_bytes{band=\"small\"} 0")
        );
        assert!(
            management_response
                .contains("novarocks_backend_process_counted_live_bytes{band=\"tagged\"} 0")
        );
        assert!(management_response.contains(
            "novarocks_backend_task_preparation{dimension=\"used\",resource=\"positions\"} 0"
        ));

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

    /// Compiled-package composition: the host interpreter and the ingress
    /// package gate come from one `CompiledStaticPlan`, so a create whose
    /// package exceeds the receiver's own wire admission is refused at the
    /// composed ingress before any owner classifies it.
    #[test]
    fn a_compiled_package_backend_gates_creates_with_its_receiver_admission() {
        use novarocks_connector_contract::PureProviderProgramCatalog;
        use novarocks_execution_contract::task_execution::identity::{
            TaskIdentity, TaskOperationId,
        };
        use novarocks_functions::{
            EngineFunctionCatalogBuilder, FunctionId, FunctionKind, FunctionOverloadId,
            InstalledPureKernel, PureImplementationDeclaration, PureImplementationId,
            PureKernelAbi,
        };
        use novarocks_task_codec::identity::{
            encode_query_context_ref, encode_task_identity, encode_task_operation_id,
        };
        use novarocks_type_contract::{CompileControlError, CompilePhase, PureCompileControl};
        use novarocks_types::identity::{
            AttemptId, FrontendProcessId, QueryExecutionId, QueryId, StageId, TaskId,
        };
        use prost::Message;

        struct Unbounded;
        impl PureCompileControl for Unbounded {
            fn checkpoint(&self, _: CompilePhase, _: u32) -> Result<(), CompileControlError> {
                Ok(())
            }
        }

        // Fixture inputs, not production sizing: the receiver's generous test
        // admission with a small package bound, and a sealed RAND-only subset.
        let mut limits = novarocks_plan_codec::physical_package_v2::test_support::decode_limits();
        limits.wire.max_input_bytes = 64;
        let constants = limits.constant_policy;
        let metadata = test_execution_function_set();
        let mut builder = EngineFunctionCatalogBuilder::new();
        builder
            .register(
                metadata
                    .catalog()
                    .definition("rand", FunctionKind::Scalar)
                    .expect("rand")
                    .clone(),
            )
            .expect("register rand");
        let functions = builder
            .seal_pure(
                [
                    "builtin.scalar/rand/()->f64;strict;legacy",
                    "builtin.scalar/rand/(i64)->f64;strict;legacy",
                ]
                .into_iter()
                .map(|overload| InstalledPureKernel {
                    function: FunctionId::try_new("builtin.scalar/rand/v1").unwrap(),
                    kind: FunctionKind::Scalar,
                    implementation: PureImplementationDeclaration {
                        overload: FunctionOverloadId::try_new(overload).unwrap(),
                        implementation: PureImplementationId::try_new(
                            "builtin.scalar/rand/selected-v1",
                        )
                        .unwrap(),
                        abi: PureKernelAbi::ScalarV1,
                    },
                    aggregate_state_format: None,
                }),
            )
            .expect("sealed rand subset");
        let providers =
            PureProviderProgramCatalog::<std::io::Error>::try_new(&[], vec![], &Unbounded)
                .expect("empty provider catalog");
        let plan = CompiledStaticPlan::try_new(
            limits,
            Arc::new(functions),
            Arc::new(providers),
            constants,
        )
        .expect("compiled static plan");

        let services = compose_backend_application_services(
            test_data_runtime(),
            BackendProcessId::new_v7(),
            BackendExecutionRuntimeInput::new(
                execution_runtime_config(),
                metadata,
                novarocks_native_adapter::backend_test_support::test_memory_authority(),
            ),
            novarocks_types::NativeCompatibilityId::new([0x71; 32]),
            WriteCommitEvidenceLimits::default(),
            WorkerResultRetainedLimits::try_new(16 * 1024 * 1024, 32 * 1024 * 1024)
                .expect("valid test result retained-byte limits"),
            novarocks_worker::result_buffer::ResultRetainedBudget::new(
                std::num::NonZeroUsize::new(32 * 1024 * 1024).unwrap(),
            ),
            super::RootProducerLimits::try_new(1, 64, 1024 * 1024)
                .expect("finite root producer limits"),
            novarocks_worker::TaskInboundCapabilityLimits::default(),
            novarocks_worker::TaskPreparationLimits::default(),
            novarocks_worker::ScanStreamHost::new(
                novarocks_worker::ScanPreparationConfig::try_new(
                    64 * 1024 * 1024,
                    4,
                    Duration::from_millis(500),
                    Duration::from_millis(500),
                    Duration::from_millis(100),
                )
                .expect("valid scan preparation configuration"),
                novarocks_native_adapter::backend_test_support::test_scan_stream_runtime(),
            ),
            CatalogManagerConfig::default(),
            &[],
            BackendStaticPlanInterpreter::CompiledPackage(plan),
        )
        .expect("compose compiled-package backend services");

        let execution = QueryExecutionId::new(
            QueryId::new(31, 37),
            AttemptId::new(1).expect("nonzero attempt"),
        )
        .expect("nonzero query");
        let context = QueryContextRef::new(
            execution,
            FrontendProcessId::new_v7(),
            services.backend_process_id,
        );
        let identity = TaskIdentity::new(
            execution,
            StageId::new(1).expect("nonzero stage"),
            TaskId::new(1).expect("nonzero task"),
            services.backend_process_id,
        );
        let metadata = protocol::CreationMetadata {
            query_context: Some(encode_query_context_ref(context)),
            descriptor: Some(protocol::TaskDescriptor {
                identity: Some(encode_task_identity(identity)),
                fragment_instance_id: Some(novarocks_proto_models::common::UniqueId {
                    hi: 41,
                    lo: 42,
                }),
                pipeline_dop: 1,
                split_plan_nodes: Vec::new(),
                topology: Some(protocol::TaskExchangeTopology::default()),
            }),
            initial_domains: Vec::new(),
            assignment: Some(protocol::TaskAssignment::default()),
        };
        let carrier = protocol::FrozenFragment {
            package: vec![0; 65].into(),
            ..Default::default()
        };
        let error = services
            .task_execution_ingress
            .apply_task_operations(protocol::ApplyTaskOperationsRequest {
                operations: vec![protocol::TaskOperation {
                    envelope: Some(protocol::TaskOperationEnvelope {
                        operation_id: Some(encode_task_operation_id(TaskOperationId::new_v7())),
                        max_wait_millis: 5_000,
                    }),
                    operation: Some(protocol::task_operation::Operation::CreateTask(
                        protocol::CreateTaskRequest {
                            frozen_fragment: carrier.encode_to_vec().into(),
                            creation_metadata: metadata.encode_to_vec().into(),
                        },
                    )),
                }],
            })
            .expect_err("the composed ingress gates the package carrier");
        assert_eq!(error.code(), tonic::Code::InvalidArgument);
        assert!(
            error
                .message()
                .contains("create_task.frozen_fragment.package (out of range)"),
            "unexpected message: {}",
            error.message()
        );
        assert!(
            !services.task_execution_registry.has_live_task(identity),
            "a refused create leaves no task behind"
        );
    }
}
