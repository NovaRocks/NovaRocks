use std::fmt;

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
                return Err(ExecutionRuntimeConfigError { field: name });
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
}

impl ExecutionRuntime {
    pub fn new(config: ExecutionRuntimeConfig) -> Result<Self, ExecutionRuntimeConfigError> {
        config.validate()?;
        Ok(Self { config })
    }

    pub const fn config(&self) -> &ExecutionRuntimeConfig {
        &self.config
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ExecutionRuntimeConfigError {
    field: &'static str,
}

impl ExecutionRuntimeConfigError {
    pub const fn field(&self) -> &'static str {
        self.field
    }
}

impl fmt::Display for ExecutionRuntimeConfigError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "execution runtime config {} must be non-zero",
            self.field
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
        assert_eq!(error.field(), "scan_queue_capacity");
    }

    #[test]
    fn retains_frozen_composition_settings() {
        let config = config();
        let runtime = ExecutionRuntime::new(config.clone()).expect("valid runtime config");
        assert_eq!(runtime.config(), &config);
    }
}
