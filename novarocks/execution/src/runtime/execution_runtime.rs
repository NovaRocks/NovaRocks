use std::fmt;
use std::sync::Arc;
use std::time::Duration;

use crate::exec::pipeline::global_driver_executor::GlobalDriverExecutor;
use crate::runtime::exchange::ExecutionExchangeRegistry;
use crate::runtime::execution_services::ExecutionServices;
use crate::runtime::function_set::SealedExecutionFunctionSet;
use crate::runtime::mem_tracker::{MemTracker, process_mem_tracker};
use crate::runtime::task::io::exchange_stream_sender::ExchangeUnitSendScheduler;
use novarocks_memory::MemoryAuthority;

/// Frozen process-local settings used to construct one execution runtime.
///
/// Application composition resolves configuration defaults before creating this
/// value. Execution code never performs application configuration lookup.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ExecutionRuntimeConfig {
    pub driver_threads: usize,
    pub exchange_io_max_inflight_bytes: usize,
    pub exchange_max_transmit_batched_bytes: usize,
    pub operator_buffer_chunks: usize,
    pub local_exchange_buffer_mem_limit_per_driver: usize,
    pub local_exchange_max_buffered_rows: i64,
    pub runtime_filter_scan_wait_time_ms_override: Option<i64>,
    pub runtime_filter_wait_timeout_ms_override: Option<i64>,
    pub sink_io_worker_threads: usize,
    pub sink_io_max_blocking_threads: usize,
}

impl ExecutionRuntimeConfig {
    pub fn validate(&self) -> Result<(), ExecutionRuntimeConfigError> {
        for (name, value) in [
            ("driver_threads", self.driver_threads),
            (
                "exchange_io_max_inflight_bytes",
                self.exchange_io_max_inflight_bytes,
            ),
            (
                "exchange_max_transmit_batched_bytes",
                self.exchange_max_transmit_batched_bytes,
            ),
            ("operator_buffer_chunks", self.operator_buffer_chunks),
            (
                "local_exchange_buffer_mem_limit_per_driver",
                self.local_exchange_buffer_mem_limit_per_driver,
            ),
            ("sink_io_worker_threads", self.sink_io_worker_threads),
            (
                "sink_io_max_blocking_threads",
                self.sink_io_max_blocking_threads,
            ),
        ] {
            if value == 0 {
                return Err(ExecutionRuntimeConfigError::invalid_field(name));
            }
        }
        if self.local_exchange_max_buffered_rows == 0 {
            return Err(ExecutionRuntimeConfigError::invalid_field(
                "local_exchange_max_buffered_rows",
            ));
        }
        Ok(())
    }
}

/// Validated structural exchange limits supplied by application composition.
///
/// Slots count units, not bytes of resident memory. Each installed instance has
/// its own progress floor; registration does not consume a second process quota.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ExchangeFlowLimits {
    instance_floor_slots: usize,
    instance_max_slots: usize,
    floating_slots: usize,
    stream_window: usize,
    transport_timeout: Duration,
}

impl ExchangeFlowLimits {
    pub fn try_new(
        instance_floor_slots: usize,
        instance_max_slots: usize,
        floating_slots: usize,
        stream_window: usize,
        transport_timeout: Duration,
    ) -> Result<Self, ExecutionRuntimeConfigError> {
        for (field, value) in [
            ("exchange_instance_floor_slots", instance_floor_slots),
            ("exchange_stream_window", stream_window),
        ] {
            if value == 0 {
                return Err(ExecutionRuntimeConfigError::invalid_field(field));
            }
        }
        if instance_max_slots < instance_floor_slots {
            return Err(ExecutionRuntimeConfigError::runtime(
                "exchange_instance_max_slots must be at least exchange_instance_floor_slots"
                    .to_owned(),
            ));
        }
        if transport_timeout.is_zero() {
            return Err(ExecutionRuntimeConfigError::invalid_field(
                "exchange_transport_timeout_ms",
            ));
        }
        Ok(Self {
            instance_floor_slots,
            instance_max_slots,
            floating_slots,
            stream_window,
            transport_timeout,
        })
    }

    pub const fn instance_floor_slots(self) -> usize {
        self.instance_floor_slots
    }

    pub const fn instance_max_slots(self) -> usize {
        self.instance_max_slots
    }

    pub const fn floating_slots(self) -> usize {
        self.floating_slots
    }

    pub const fn stream_window(self) -> usize {
        self.stream_window
    }

    pub const fn transport_timeout(self) -> Duration {
        self.transport_timeout
    }
}

impl Default for ExchangeFlowLimits {
    fn default() -> Self {
        // Candidate defaults are calibrated before the production cutover.
        Self::try_new(1, 32, 8192, 2, Duration::from_secs(30))
            .expect("candidate exchange flow defaults must be valid")
    }
}

/// Process-local runtime supplied and owned by application composition.
///
/// Scheduler and I/O executor implementations are installed in the runtime in
/// the dependency-ceiling wave. Keeping construction here makes the ownership
/// boundary explicit before those implementations move into this crate.
#[derive(Clone)]
pub struct ExecutionRuntime {
    // One health owner follows this application runtime and all of its clones.
    execution_health: Arc<crate::runtime::execution_health::ExecutionHealth>,
    config: ExecutionRuntimeConfig,
    exchange_flow_limits: ExchangeFlowLimits,
    function_set: Arc<SealedExecutionFunctionSet>,
    /// The one memory capacity authority this OS process was given.
    ///
    /// The execution runtime is where a charge is actually made, so it must
    /// be able to reach the authority; it receives a handle rather than
    /// creating one, because a second authority over the same address space
    /// would govern a budget nobody else can see.
    memory_authority: Arc<MemoryAuthority>,
    services: Arc<ExecutionServices>,
    mem_root: Arc<MemTracker>,
    exchange_registry: Arc<ExecutionExchangeRegistry>,
    driver_executor: Arc<GlobalDriverExecutor>,
    exchange_unit_scheduler: Arc<ExchangeUnitSendScheduler>,
}

/// A real, small memory authority for tests that need to build an
/// [`ExecutionRuntime`].
///
/// Tests get the production type rather than a stub: the authority is the
/// thing under test in W2A, and a stub would let a wiring mistake pass.
#[cfg(test)]
pub(crate) fn test_memory_authority() -> Arc<MemoryAuthority> {
    const BOUND: u64 = 64 * 1024 * 1024;
    Arc::new(
        MemoryAuthority::new(novarocks_memory::AuthorityConfig::new(
            BOUND,
            BOUND - BOUND / 4,
            BOUND / 4,
        ))
        .expect("the test partition must be valid"),
    )
}

impl ExecutionRuntime {
    pub fn new(
        config: ExecutionRuntimeConfig,
        function_set: Arc<SealedExecutionFunctionSet>,
        memory_authority: Arc<MemoryAuthority>,
    ) -> Result<Self, ExecutionRuntimeConfigError> {
        Self::new_with_exchange_flow_limits(
            config,
            function_set,
            memory_authority,
            ExchangeFlowLimits::default(),
        )
    }

    pub fn new_with_exchange_flow_limits(
        config: ExecutionRuntimeConfig,
        function_set: Arc<SealedExecutionFunctionSet>,
        memory_authority: Arc<MemoryAuthority>,
        exchange_flow_limits: ExchangeFlowLimits,
    ) -> Result<Self, ExecutionRuntimeConfigError> {
        config.validate()?;
        let services =
            ExecutionServices::new(&config).map_err(ExecutionRuntimeConfigError::runtime)?;
        let driver_executor = Arc::new(GlobalDriverExecutor::new(config.driver_threads));
        let peer_calls = usize::try_from(
            novarocks_execution_contract::native_result_support::NativeResultSupportGeometry::V1
                .transport_exchange_inflight_per_peer,
        )
        .map_err(|error| ExecutionRuntimeConfigError::runtime(error.to_string()))?;
        let exchange_unit_scheduler = ExchangeUnitSendScheduler::try_new(
            config.exchange_io_max_inflight_bytes,
            peer_calls,
            novarocks_execution_contract::native_result_support::NATIVE_GRPC_DECODED_MESSAGE_MAX_BYTES,
        )
        .map_err(|error| ExecutionRuntimeConfigError::runtime(error.to_string()))?;
        Ok(Self {
            execution_health: Arc::new(crate::runtime::execution_health::ExecutionHealth::default()),
            config,
            exchange_flow_limits,
            function_set,
            memory_authority,
            services: Arc::new(services),
            // The execution runtime owns a named branch of the process
            // hierarchy, never a second root: a disconnected root makes every
            // charge under it invisible to the process memory boundary.
            mem_root: MemTracker::new_child("execution", &process_mem_tracker()),
            exchange_registry: Arc::new(ExecutionExchangeRegistry::with_flow_limits(
                exchange_flow_limits,
            )),
            driver_executor,
            exchange_unit_scheduler,
        })
    }

    pub fn execution_health(&self) -> Arc<crate::runtime::execution_health::ExecutionHealth> {
        Arc::clone(&self.execution_health)
    }

    pub const fn config(&self) -> &ExecutionRuntimeConfig {
        &self.config
    }

    pub const fn exchange_flow_limits(&self) -> ExchangeFlowLimits {
        self.exchange_flow_limits
    }

    pub fn function_set(&self) -> &Arc<SealedExecutionFunctionSet> {
        &self.function_set
    }

    pub fn function_catalog(&self) -> &Arc<novarocks_functions::EngineFunctionCatalog> {
        self.function_set.catalog()
    }

    pub fn services(&self) -> &ExecutionServices {
        &self.services
    }

    pub fn mem_root(&self) -> Arc<MemTracker> {
        Arc::clone(&self.mem_root)
    }

    /// Returns this process's memory capacity authority.
    pub fn memory_authority(&self) -> &Arc<MemoryAuthority> {
        &self.memory_authority
    }

    pub fn exchange_registry(&self) -> Arc<ExecutionExchangeRegistry> {
        Arc::clone(&self.exchange_registry)
    }

    pub(crate) fn driver_executor(&self) -> Arc<GlobalDriverExecutor> {
        Arc::clone(&self.driver_executor)
    }

    /// Stop accepting driver work and wait a bounded time for driver scheduling
    /// threads to join. A reaper retains join ownership after a timeout.
    /// Application shutdown invokes this before releasing the runtime owner.
    pub fn shutdown_driver_execution(&self) -> crate::runtime::task::ExecutionResult<()> {
        self.driver_executor.shutdown()
    }

    /// Seal sink actor admission before waiting for actual future/capture exit.
    /// A deadline only stops this wait; live actors retain their own responsibility.
    pub fn shutdown_sink_io(&self) -> crate::runtime::task::ExecutionResult<()> {
        let io = self.services.sink_io();
        io.close_admission();
        io.wait_live_exit(crate::exec::pipeline::global_driver_executor::DEFAULT_SHUTDOWN_TIMEOUT)
            .map_err(Into::into)
    }

    pub fn exchange_unit_scheduler(&self) -> Arc<ExchangeUnitSendScheduler> {
        Arc::clone(&self.exchange_unit_scheduler)
    }
}

#[cfg(test)]
pub(crate) fn test_execution_function_set() -> Arc<SealedExecutionFunctionSet> {
    crate::runtime::function_set::test_builtin_execution_function_set()
}

#[cfg(test)]
pub(crate) fn test_execution_runtime() -> Arc<ExecutionRuntime> {
    Arc::new(
        ExecutionRuntime::new(
            ExecutionRuntimeConfig {
                driver_threads: 1,
                exchange_io_max_inflight_bytes: 1024,
                exchange_max_transmit_batched_bytes: 1024,
                operator_buffer_chunks: 1,
                local_exchange_buffer_mem_limit_per_driver: 1024,
                local_exchange_max_buffered_rows: 1024,
                runtime_filter_scan_wait_time_ms_override: None,
                runtime_filter_wait_timeout_ms_override: None,
                sink_io_worker_threads: 1,
                sink_io_max_blocking_threads: 1,
            },
            test_execution_function_set(),
            test_memory_authority(),
        )
        .expect("test execution runtime"),
    )
}

impl fmt::Debug for ExecutionRuntime {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ExecutionRuntime")
            .field("config", &self.config)
            .field("exchange_flow_limits", &self.exchange_flow_limits)
            .finish_non_exhaustive()
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ExecutionRuntimeConfigError {
    message: String,
}

impl ExecutionRuntimeConfigError {
    fn invalid_field(field: &'static str) -> Self {
        Self {
            message: format!("{field} must be non-zero"),
        }
    }

    fn runtime(message: String) -> Self {
        Self { message }
    }
}

impl fmt::Display for ExecutionRuntimeConfigError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "execution runtime configuration error: {}",
            self.message
        )
    }
}

impl std::error::Error for ExecutionRuntimeConfigError {}

#[cfg(test)]
mod tests {
    use std::thread;
    use std::time::{Duration, Instant};

    use super::{
        ExchangeFlowLimits, ExecutionRuntime, ExecutionRuntimeConfig, test_execution_function_set,
    };

    fn wait_until(timeout: Duration, predicate: impl Fn() -> bool) -> bool {
        let deadline = Instant::now() + timeout;
        while Instant::now() < deadline {
            if predicate() {
                return true;
            }
            thread::yield_now();
        }
        predicate()
    }

    fn config() -> ExecutionRuntimeConfig {
        ExecutionRuntimeConfig {
            driver_threads: 1,
            exchange_io_max_inflight_bytes: 1,
            exchange_max_transmit_batched_bytes: 1,
            operator_buffer_chunks: 1,
            local_exchange_buffer_mem_limit_per_driver: 1,
            local_exchange_max_buffered_rows: 1,
            runtime_filter_scan_wait_time_ms_override: None,
            runtime_filter_wait_timeout_ms_override: None,
            sink_io_worker_threads: 1,
            sink_io_max_blocking_threads: 1,
        }
    }

    #[test]
    fn exchange_flow_limits_accept_zero_floating_and_preserve_explicit_values() {
        let limits = ExchangeFlowLimits::try_new(2, 5, 0, 3, Duration::from_millis(71))
            .expect("instance progress does not require floating slots");
        assert_eq!(limits.instance_floor_slots(), 2);
        assert_eq!(limits.instance_max_slots(), 5);
        assert_eq!(limits.floating_slots(), 0);
        assert_eq!(limits.stream_window(), 3);
        assert_eq!(limits.transport_timeout(), Duration::from_millis(71));

        let runtime = ExecutionRuntime::new_with_exchange_flow_limits(
            config(),
            test_execution_function_set(),
            crate::runtime::execution_runtime::test_memory_authority(),
            limits,
        )
        .expect("explicit exchange limits construct a runtime");
        assert_eq!(runtime.exchange_flow_limits(), limits);
    }

    #[test]
    fn exchange_flow_limits_refuse_inverted_bounds_and_zero_progress_resources() {
        for (floor, cap, window, timeout_ms, field) in [
            (0, 5, 3, 71, "exchange_instance_floor_slots"),
            (2, 1, 3, 71, "exchange_instance_max_slots"),
            (2, 5, 0, 71, "exchange_stream_window"),
            (2, 5, 3, 0, "exchange_transport_timeout_ms"),
        ] {
            let error = ExchangeFlowLimits::try_new(
                floor,
                cap,
                0,
                window,
                Duration::from_millis(timeout_ms),
            )
            .expect_err("an impossible progress resource must be refused");
            assert!(error.to_string().contains(field), "{error}");
        }
    }

    #[test]
    fn rejects_zero_capacity_before_runtime_construction() {
        let mut config = config();
        config.exchange_io_max_inflight_bytes = 0;
        let error = ExecutionRuntime::new(
            config,
            test_execution_function_set(),
            crate::runtime::execution_runtime::test_memory_authority(),
        )
        .expect_err("zero queue must be rejected");
        assert_eq!(
            error.to_string(),
            "execution runtime configuration error: exchange_io_max_inflight_bytes must be non-zero"
        );
    }

    #[test]
    fn retains_frozen_composition_settings() {
        let config = config();
        let runtime = ExecutionRuntime::new(
            config.clone(),
            test_execution_function_set(),
            crate::runtime::execution_runtime::test_memory_authority(),
        )
        .expect("valid runtime config");
        assert_eq!(runtime.config(), &config);
        assert_eq!(
            runtime.exchange_flow_limits(),
            ExchangeFlowLimits::default()
        );
    }

    #[test]
    fn accepts_negative_one_for_unbounded_local_exchange_rows() {
        let mut config = config();
        config.local_exchange_max_buffered_rows = -1;
        ExecutionRuntime::new(
            config,
            test_execution_function_set(),
            crate::runtime::execution_runtime::test_memory_authority(),
        )
        .expect("-1 preserves the unlimited local exchange contract");
    }

    #[test]
    fn runtime_drop_and_rebuild_join_driver_scheduler_threads() {
        for _ in 0..4 {
            let runtime = ExecutionRuntime::new(
                config(),
                test_execution_function_set(),
                crate::runtime::execution_runtime::test_memory_authority(),
            )
            .expect("valid runtime config");
            let probes = runtime.driver_executor.exit_probes();

            drop(runtime);

            assert!(wait_until(Duration::from_secs(1), || probes.all_exited()));
        }
    }

    #[test]
    fn runtime_driver_shutdown_is_explicit_and_idempotent() {
        let runtime = ExecutionRuntime::new(
            config(),
            test_execution_function_set(),
            crate::runtime::execution_runtime::test_memory_authority(),
        )
        .expect("valid runtime config");
        let probes = runtime.driver_executor.exit_probes();

        runtime
            .shutdown_driver_execution()
            .expect("first driver shutdown");
        runtime
            .shutdown_driver_execution()
            .expect("repeated driver shutdown");

        assert!(probes.all_exited());
    }

    /// The execution runtime owns a branch, not a second root. A root here
    /// would make every byte charged under it invisible to the one process
    /// memory boundary.
    #[test]
    fn execution_mem_root_is_a_child_of_the_process_root() {
        let runtime = ExecutionRuntime::new(
            config(),
            test_execution_function_set(),
            crate::runtime::execution_runtime::test_memory_authority(),
        )
        .expect("valid runtime config");
        let mem_root = runtime.mem_root();
        assert_eq!(mem_root.label(), "execution");
        assert!(
            crate::runtime::mem_tracker::process_mem_tracker()
                .children()
                .iter()
                .any(|child| std::sync::Arc::ptr_eq(child, &mem_root)),
            "the execution mem root must hang off the process root"
        );
    }
}
