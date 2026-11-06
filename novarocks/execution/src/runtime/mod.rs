//! Process-neutral local execution runtime primitives.

pub mod cache;
pub mod dispatch_metrics;
pub mod endpoint;
pub mod exchange;
pub mod exchange_slots;
pub mod exec_env;
pub mod execution_health;
pub mod execution_runtime;
pub mod execution_services;
pub mod kernel_memory;
#[cfg(test)]
pub(crate) mod local_completion;
pub mod mem_tracker;
pub mod observable;
pub mod operator_statistics;
pub mod preparation_memory;
pub mod preparation_metadata;
pub mod profile;
pub mod query_context;
pub mod query_memory;
pub mod query_options;
pub mod runtime_state;
pub mod scalar_memory;
pub mod scan_stream_metrics;
pub mod table_writer_metrics;
pub mod task;
pub mod task_control;
pub(crate) mod tracked_state_allocator;
#[cfg(novarocks_dev_controls)]
pub(crate) mod writer_diagnostics;

pub use execution_runtime::{
    ExecutionRuntime, ExecutionRuntimeConfig, ExecutionRuntimeConfigError,
};

#[cfg(test)]
mod kernel_memory_observation_tests;

pub mod function_set;

#[cfg(novarocks_dev_controls)]
pub mod join_test_control;
