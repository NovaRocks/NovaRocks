//! Process-neutral local execution runtime primitives.

pub mod cache;
pub mod connector_write_report;
pub mod endpoint;
pub mod exchange;
pub mod exec_env;
pub mod execution_runtime;
pub mod execution_services;
pub mod fragment;
pub mod io;
pub mod mem_tracker;
pub mod observable;
pub mod profile;
pub mod query_options;
pub mod runtime_state;
pub mod scan_executor;
pub mod spill_config;

pub use execution_runtime::{
    ExecutionRuntime, ExecutionRuntimeConfig, ExecutionRuntimeConfigError,
};
