//! Process-neutral local execution runtime primitives.

pub mod execution_runtime;
pub mod mem_tracker;
pub mod profile;
pub mod query_options;
pub mod spill_config;

pub use execution_runtime::{
    ExecutionRuntime, ExecutionRuntimeConfig, ExecutionRuntimeConfigError,
};
