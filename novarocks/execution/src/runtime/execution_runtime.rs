use std::fmt;
use std::sync::Arc;

use crate::runtime::execution_services::ExecutionServices;
use crate::runtime::mem_tracker::MemTracker;

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
    pub exchange_io_threads: usize,
    pub exchange_io_max_inflight_bytes: usize,
    pub sink_io_worker_threads: usize,
    pub sink_io_max_blocking_threads: usize,
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
        Ok(())
    }
}

/// Process-local runtime supplied and owned by application composition.
///
/// Scheduler and I/O executor implementations are installed in the runtime in
/// the dependency-ceiling wave. Keeping construction here makes the ownership
/// boundary explicit before those implementations move into this crate.
#[derive(Clone, Debug)]
pub struct ExecutionRuntime {
    config: ExecutionRuntimeConfig,
    services: Arc<ExecutionServices>,
    mem_root: Arc<MemTracker>,
}

impl ExecutionRuntime {
    pub fn new(config: ExecutionRuntimeConfig) -> Result<Self, ExecutionRuntimeConfigError> {
        config.validate()?;
        let services = ExecutionServices::new(&config)
            .map_err(|error| ExecutionRuntimeConfigError::runtime(error))?;
        Ok(Self {
            config,
            services: Arc::new(services),
            mem_root: MemTracker::new_root("execution"),
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
    use super::{ExecutionRuntime, ExecutionRuntimeConfig};

    fn config() -> ExecutionRuntimeConfig {
        ExecutionRuntimeConfig {
            driver_threads: 1,
            scan_threads: 1,
            scan_queue_capacity: 1,
            spill_io_threads: 1,
            spill_io_queue_capacity: 1,
            exchange_io_threads: 1,
            exchange_io_max_inflight_bytes: 1,
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
}
