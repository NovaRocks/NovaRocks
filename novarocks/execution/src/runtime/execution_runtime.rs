use std::fmt;
use std::sync::Arc;

use crate::exec::pipeline::global_driver_executor::GlobalDriverExecutor;
use crate::runtime::exchange::ExecutionExchangeRegistry;
use crate::runtime::execution_services::ExecutionServices;
use crate::runtime::fragment::io::exchange_queue::ExchangeSendQueue;
use crate::runtime::io::IoExecutor;
use crate::runtime::mem_tracker::MemTracker;
use crate::runtime::scan_executor::ScanExecutor;

/// Frozen process-local settings used to construct one execution runtime.
///
/// Application composition resolves configuration defaults before creating this
/// value. Execution code never performs application configuration lookup.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ExecutionRuntimeConfig {
    pub driver_threads: usize,
    pub scan_threads: usize,
    pub scan_queue_capacity: usize,
    pub spill_io_threads: usize,
    pub spill_io_queue_capacity: usize,
    pub spill_storage: ExecutionSpillStorageConfig,
    pub exchange_io_threads: usize,
    pub exchange_io_max_inflight_bytes: usize,
    pub exchange_max_transmit_batched_bytes: usize,
    pub operator_buffer_chunks: usize,
    pub local_exchange_buffer_mem_limit_per_driver: usize,
    pub local_exchange_max_buffered_rows: i64,
    pub connector_io_tasks_per_scan_operator: i32,
    pub scan_submit_fail_max: usize,
    pub scan_submit_fail_timeout_ms: u64,
    pub runtime_filter_scan_wait_time_ms_override: Option<i64>,
    pub runtime_filter_wait_timeout_ms_override: Option<i64>,
    pub sink_io_worker_threads: usize,
    pub sink_io_max_blocking_threads: usize,
}

/// Frozen storage facts used by execution-side spilling.
///
/// Application composition resolves directories and codec defaults before
/// constructing this value; the kernel never reads application configuration.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ExecutionSpillStorageConfig {
    pub enabled: bool,
    pub local_dirs: Vec<String>,
    pub dir_max_bytes: u64,
    pub block_size_bytes: u64,
    pub ipc_compression: String,
}

impl Default for ExecutionSpillStorageConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            local_dirs: Vec::new(),
            dir_max_bytes: 0,
            block_size_bytes: 1,
            ipc_compression: "lz4".to_string(),
        }
    }
}

impl ExecutionRuntimeConfig {
    pub fn validate(&self) -> Result<(), ExecutionRuntimeConfigError> {
        for (name, value) in [
            ("driver_threads", self.driver_threads),
            ("scan_threads", self.scan_threads),
            ("scan_queue_capacity", self.scan_queue_capacity),
            ("spill_io_threads", self.spill_io_threads),
            ("spill_io_queue_capacity", self.spill_io_queue_capacity),
            ("exchange_io_threads", self.exchange_io_threads),
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
        if self.connector_io_tasks_per_scan_operator <= 0 {
            return Err(ExecutionRuntimeConfigError::invalid_field(
                "connector_io_tasks_per_scan_operator",
            ));
        }
        if self.spill_storage.enabled {
            if self.spill_storage.local_dirs.is_empty() {
                return Err(ExecutionRuntimeConfigError::invalid_field(
                    "spill_storage.local_dirs",
                ));
            }
            if self.spill_storage.block_size_bytes == 0 {
                return Err(ExecutionRuntimeConfigError::invalid_field(
                    "spill_storage.block_size_bytes",
                ));
            }
        }
        Ok(())
    }
}

/// Process-local runtime supplied and owned by application composition.
///
/// Scheduler and I/O executor implementations are installed in the runtime in
/// the dependency-ceiling wave. Keeping construction here makes the ownership
/// boundary explicit before those implementations move into this crate.
#[derive(Clone)]
pub struct ExecutionRuntime {
    config: ExecutionRuntimeConfig,
    services: Arc<ExecutionServices>,
    mem_root: Arc<MemTracker>,
    exchange_registry: Arc<ExecutionExchangeRegistry>,
    driver_executor: Arc<GlobalDriverExecutor>,
    scan_executor: Arc<ScanExecutor>,
    exchange_send_queue: Arc<ExchangeSendQueue>,
}

impl ExecutionRuntime {
    pub fn new(config: ExecutionRuntimeConfig) -> Result<Self, ExecutionRuntimeConfigError> {
        config.validate()?;
        let services = ExecutionServices::new(&config)
            .map_err(|error| ExecutionRuntimeConfigError::runtime(error))?;
        let driver_executor = Arc::new(GlobalDriverExecutor::new(config.driver_threads));
        let scan_executor = Arc::new(ScanExecutor::new(
            config.scan_threads,
            config.scan_queue_capacity,
        ));
        let exchange_io_executor = Arc::new(IoExecutor::new(config.exchange_io_threads));
        let exchange_send_queue = Arc::new(ExchangeSendQueue::new(
            config.exchange_io_max_inflight_bytes,
            exchange_io_executor,
        ));
        Ok(Self {
            config,
            services: Arc::new(services),
            mem_root: MemTracker::new_root("execution"),
            exchange_registry: Arc::new(ExecutionExchangeRegistry::default()),
            driver_executor,
            scan_executor,
            exchange_send_queue,
        })
    }

    pub const fn config(&self) -> &ExecutionRuntimeConfig {
        &self.config
    }

    pub fn services(&self) -> &ExecutionServices {
        &self.services
    }

    pub fn mem_root(&self) -> Arc<MemTracker> {
        Arc::clone(&self.mem_root)
    }

    pub fn exchange_registry(&self) -> Arc<ExecutionExchangeRegistry> {
        Arc::clone(&self.exchange_registry)
    }

    pub fn driver_executor(&self) -> Arc<GlobalDriverExecutor> {
        Arc::clone(&self.driver_executor)
    }

    pub fn scan_executor(&self) -> Arc<ScanExecutor> {
        Arc::clone(&self.scan_executor)
    }

    pub fn exchange_send_queue(&self) -> Arc<ExchangeSendQueue> {
        Arc::clone(&self.exchange_send_queue)
    }
}

impl fmt::Debug for ExecutionRuntime {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ExecutionRuntime")
            .field("config", &self.config)
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
    use super::{ExecutionRuntime, ExecutionRuntimeConfig, ExecutionSpillStorageConfig};

    fn config() -> ExecutionRuntimeConfig {
        ExecutionRuntimeConfig {
            driver_threads: 1,
            scan_threads: 1,
            scan_queue_capacity: 1,
            spill_io_threads: 1,
            spill_io_queue_capacity: 1,
            spill_storage: ExecutionSpillStorageConfig::default(),
            exchange_io_threads: 1,
            exchange_io_max_inflight_bytes: 1,
            exchange_max_transmit_batched_bytes: 1,
            operator_buffer_chunks: 1,
            local_exchange_buffer_mem_limit_per_driver: 1,
            local_exchange_max_buffered_rows: 1,
            connector_io_tasks_per_scan_operator: 1,
            scan_submit_fail_max: 1,
            scan_submit_fail_timeout_ms: 1,
            runtime_filter_scan_wait_time_ms_override: None,
            runtime_filter_wait_timeout_ms_override: None,
            sink_io_worker_threads: 1,
            sink_io_max_blocking_threads: 1,
        }
    }

    #[test]
    fn rejects_zero_capacity_before_runtime_construction() {
        let mut config = config();
        config.scan_queue_capacity = 0;
        let error = ExecutionRuntime::new(config).expect_err("zero queue must be rejected");
        assert_eq!(
            error.to_string(),
            "execution runtime configuration error: scan_queue_capacity must be non-zero"
        );
    }

    #[test]
    fn retains_frozen_composition_settings() {
        let config = config();
        let runtime = ExecutionRuntime::new(config.clone()).expect("valid runtime config");
        assert_eq!(runtime.config(), &config);
    }

    #[test]
    fn accepts_negative_one_for_unbounded_local_exchange_rows() {
        let mut config = config();
        config.local_exchange_max_buffered_rows = -1;
        ExecutionRuntime::new(config).expect("-1 preserves the unlimited local exchange contract");
    }
}
