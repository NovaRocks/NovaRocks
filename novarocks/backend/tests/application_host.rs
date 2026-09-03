use std::net::TcpListener;
use std::sync::Arc;

use std::time::Duration;

use novarocks_backend::{
    BackendApplicationHost, BackendDataRuntime, BackendNativeTransport, BackendServerConfig,
    QueryLifecycleRegistryConfig,
};
use novarocks_execution::runtime::execution_runtime::{
    ExecutionRuntimeConfig, ExecutionSpillStorageConfig,
};
use novarocks_native_trust::{
    DeploymentId, NativeCallerSubject, NativeTransportMode, NativeTrust, ValidatedSharedSecret,
};
use novarocks_secret::SecretValue;
use novarocks_types::AdvertiseEndpoint;

fn test_native_trust() -> Arc<NativeTrust> {
    Arc::new(NativeTrust::new(
        DeploymentId::parse("backend-test").expect("valid deployment"),
        ValidatedSharedSecret::new(SecretValue::new("0123456789abcdef0123456789abcdef"))
            .expect("valid secret"),
        NativeCallerSubject::parse("be@127.0.0.1:9070").expect("valid subject"),
        NativeTransportMode::Disabled,
    ))
}

fn unused_port() -> u16 {
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind ephemeral port");
    let port = listener
        .local_addr()
        .expect("read ephemeral address")
        .port();
    drop(listener);
    port
}

fn backend_config(grpc_port: u16, advertise_port: u16) -> BackendServerConfig {
    BackendServerConfig {
        bind_host: "127.0.0.1".to_string(),
        grpc_port,
        metrics_http_port: unused_port(),
        advertise_endpoint: AdvertiseEndpoint {
            host: "127.0.0.1".to_string(),
            port: advertise_port,
        },
        native_trust: test_native_trust(),
        native_compatibility_id: novarocks_types::NativeCompatibilityId::new([0x71; 32]),
        function_catalog: std::sync::Arc::new(
            novarocks_sql::compiler::build_builtin_engine_function_catalog()
                .expect("builtin function catalog"),
        ),
        native_transport: BackendNativeTransport::Plaintext,
        frontend_endpoint: novarocks_types::NativeEndpoint::from_host_port(
            "127.0.0.1",
            unused_port(),
        )
        .expect("valid frontend endpoint"),
        announce_interval: Duration::from_secs(60),
        announce_initial_backoff: Duration::from_millis(100),
        announce_max_backoff: Duration::from_secs(2),
        query_lifecycle_sweep_interval: Duration::from_millis(1_000),
        query_lifecycle_config: QueryLifecycleRegistryConfig::new(
            4_096,
            16_384,
            Duration::from_millis(120_000),
            Duration::from_millis(5_000),
            Duration::from_millis(30_000),
            256,
            32,
            48 * 1024 * 1024,
            256 * 1024 * 1024,
            512,
            48 * 1024 * 1024,
            Duration::from_millis(30_000),
            Duration::from_millis(5_000),
            Duration::from_millis(5_000),
            5,
            Duration::from_millis(100),
            Duration::from_millis(1_000),
            Duration::from_millis(120_000),
            4_096,
            256 * 1024 * 1024,
        ),
        write_commit_evidence_limits: novarocks_spi::connector::WriteCommitEvidenceLimits::default(
        ),
        execution_runtime_config: ExecutionRuntimeConfig {
            driver_threads: 1,
            scan_threads: 1,
            scan_queue_capacity: 1,
            spill_io_threads: 1,
            spill_io_queue_capacity: 1,
            spill_storage: ExecutionSpillStorageConfig::default(),
            exchange_wait_ms: 1,
            exchange_io_threads: 1,
            exchange_io_max_inflight_bytes: 1,
            exchange_max_transmit_batched_bytes: 1,
            operator_buffer_chunks: 1,
            local_exchange_buffer_mem_limit_per_driver: 1,
            local_exchange_max_buffered_rows: -1,
            connector_io_tasks_per_scan_operator: 1,
            scan_submit_fail_max: 1,
            scan_submit_fail_timeout_ms: 1,
            runtime_filter_scan_wait_time_ms_override: None,
            runtime_filter_wait_timeout_ms_override: None,
            sink_io_worker_threads: 1,
            sink_io_max_blocking_threads: 1,
        },
        catalog_manager_config:
            novarocks_backend::connector::catalog_manager::CatalogManagerConfig::default(),
        execution_role_binding_factories: Vec::new(),
    }
}

#[test]
fn host_accepts_an_empty_sealed_execution_role_binding_factory_set() {
    let grpc_port = unused_port();
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .worker_threads(1)
        .build()
        .expect("build Backend application host runtime");
    let host = BackendApplicationHost::open(
        backend_config(grpc_port, grpc_port),
        BackendDataRuntime::new(
            runtime.handle().clone(),
            test_native_trust(),
            BackendNativeTransport::Plaintext,
        ),
    )
    .expect("empty sealed catalog runtime set must start");
    host.shutdown()
        .expect("empty catalog runtime host shuts down");
}
