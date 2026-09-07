// Design: ADR-0091 (docs/adr/ADR-0091-backend-domain-owned-module-layout.md)
mod application;
mod config;
pub mod connector;
mod exchange_receiver;
mod fragment;
mod metrics;
mod query_lifecycle;
pub(crate) mod rpc;
mod runtime;
pub(crate) mod runtime_filter;
mod service;
pub mod task_execution;

pub use application::{
    BackendApplicationError, BackendApplicationErrorKind, BackendApplicationHost,
    BackendServerConfig, run_backend_server_until_shutdown, run_backend_server_until_signal,
};
pub use fragment::NativeFragmentService;
pub use query_lifecycle::QueryLifecycleRegistryConfig;
pub use rpc::runtime::{BackendDataRuntime, BackendNativeTransport};
