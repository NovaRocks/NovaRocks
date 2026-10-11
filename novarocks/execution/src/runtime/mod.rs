//! Process-neutral local execution runtime primitives.

pub mod cache;
pub mod dispatch_metrics;
pub mod endpoint;
pub mod exchange;
pub mod exec_env;
pub mod execution_runtime;
pub mod execution_services;
pub mod fragment;
pub mod io;
pub mod kernel_memory;
pub mod mem_tracker;
pub mod observable;
pub mod operator_statistics;
pub mod preparation_memory;
pub mod preparation_metadata;
pub mod profile;
pub mod query_memory;
pub mod query_options;
pub mod runtime_state;
pub mod scan_stream_metrics;
pub mod table_writer_metrics;

pub use execution_runtime::{
    ExecutionRuntime, ExecutionRuntimeConfig, ExecutionRuntimeConfigError,
};

#[cfg(test)]
mod kernel_memory_observation_tests;
