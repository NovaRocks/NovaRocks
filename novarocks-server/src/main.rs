// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

use std::env;
use std::process;
use std::sync::Arc;

use novarocks_execution::exec::expr::agg::{
    ExecutionFunctionSetBuilder, SealedExecutionFunctionSet,
    contribute_builtin_aggregate_implementations,
};
use novarocks_server::app_config::NovaRocksConfig;
use novarocks_server::{
    composition, launch, logging, native_compatibility, provider_manifest::ServerProviderManifest,
};
use novarocks_types::NativeCompatibilityId;

fn usage() {
    eprintln!("Usage:");
    eprintln!("  novarocks standalone --role fe --config <fe.toml>");
    eprintln!("  novarocks standalone --role be --config <be.toml>");
    eprintln!(
        "  novarocks standalone --role all-in-one --fe-config <fe.toml> --be-config <be.toml>"
    );
}

/// The tracing filter this process runs with.
///
/// `NOVAROCKS_LOG_FILTER` takes precedence over both config keys, matching how
/// `NOVAROCKS_LOG_DIR` already overrides the configured log directory. A
/// deployment states its intent in config; an operator diagnosing a live
/// process cannot always edit that config, and in a launched test cluster the
/// configs are generated per run.
fn resolve_log_filter(config: &NovaRocksConfig) -> String {
    if let Ok(filter) = std::env::var("NOVAROCKS_LOG_FILTER") {
        let filter = filter.trim();
        if !filter.is_empty() {
            return filter.to_owned();
        }
    }
    config
        .log_filter
        .clone()
        .unwrap_or_else(|| match config.log_level.as_str() {
            "debug" => "info,novarocks=debug".to_string(),
            "trace" => "info,novarocks=trace".to_string(),
            other => other.to_string(),
        })
}

fn init_process(config: &NovaRocksConfig) -> anyhow::Result<tokio::runtime::Runtime> {
    logging::init_with_level(
        &resolve_log_filter(config),
        &logging::LogFileSettings {
            dir: config.sys_log_dir.clone(),
            roll_mode: config.sys_log_roll_mode.clone(),
            roll_num: config.sys_log_roll_num,
        },
    );
    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .worker_threads(config.runtime.actual_data_runtime_threads().max(1))
        .max_blocking_threads(config.runtime.data_runtime_max_blocking_threads.max(1))
        .thread_name("novarocks-data-runtime")
        .thread_stack_size(novarocks_types::WORKER_STACK_SIZE_BYTES)
        .build()
        .map_err(|error| anyhow::anyhow!("build data Tokio runtime: {error}"))
}

fn compose_process_function_set() -> anyhow::Result<Arc<SealedExecutionFunctionSet>> {
    let mut builder = ExecutionFunctionSetBuilder::new();
    novarocks_sql::compiler::contribute_builtin_functions(builder.catalog_builder_mut())
        .map_err(|error| anyhow::anyhow!("contribute builtin function metadata: {error}"))?;
    contribute_builtin_aggregate_implementations(&mut builder).map_err(|error| {
        anyhow::anyhow!("contribute builtin aggregate implementations: {error}")
    })?;
    builder
        .register_typed_aggregate(
            novarocks_connector_iceberg_functions::iceberg_theta_registration()
                .map_err(|error| anyhow::anyhow!("build Iceberg function bundle: {error}"))?,
        )
        .map_err(|error| anyhow::anyhow!("contribute Iceberg function bundle: {error}"))?;
    Ok(Arc::new(builder.seal().map_err(|error| {
        anyhow::anyhow!("seal process engine function set: {error}")
    })?))
}

/// SIGTERM is the production authority for the one-way FE drain. SIGINT uses
/// the same path for local operation; neither signal is interpreted as an
/// immediate process-wide connection cancellation.
async fn termination_signal() {
    #[cfg(unix)]
    {
        use tokio::signal::unix::{SignalKind, signal};

        let mut terminate = signal(SignalKind::terminate())
            .expect("install SIGTERM handler for NovaRocks server process");
        tokio::select! {
            _ = tokio::signal::ctrl_c() => {}
            _ = terminate.recv() => {}
        }
    }
    #[cfg(not(unix))]
    {
        let _ = tokio::signal::ctrl_c().await;
    }
}

fn run_frontend(
    role: launch::RoleConfig,
    native_compatibility_id: NativeCompatibilityId,
    function_catalog: std::sync::Arc<novarocks_functions::EngineFunctionCatalog>,
    provider_manifest: Arc<ServerProviderManifest>,
    runtime: &tokio::runtime::Runtime,
) -> anyhow::Result<()> {
    let frontend = composition::compose_frontend_server_config(
        &role.config,
        &role.native_trust,
        None,
        native_compatibility_id,
        function_catalog,
        provider_manifest,
        runtime.handle().clone(),
    )?;
    runtime
        .block_on(novarocks_frontend::run_frontend_server_until_shutdown(
            frontend,
            runtime.handle().clone(),
            termination_signal(),
        ))
        .map_err(|error| anyhow::anyhow!("role=fe: {error}"))
}

fn run_backend(
    role: launch::RoleConfig,
    native_compatibility_id: NativeCompatibilityId,
    function_set: std::sync::Arc<novarocks_execution::exec::expr::agg::SealedExecutionFunctionSet>,
    provider_manifest: Arc<ServerProviderManifest>,
    runtime: &tokio::runtime::Runtime,
) -> anyhow::Result<()> {
    initialize_backend_file_caches(&role.config);
    let backend = composition::compose_backend_server_config(
        &role.config,
        &role.native_trust,
        native_compatibility_id,
        function_set,
        provider_manifest,
        runtime.handle().clone(),
    )?;
    let data_runtime = novarocks_backend::BackendDataRuntime::new(
        runtime.handle().clone(),
        std::sync::Arc::clone(&backend.native_trust),
        backend.native_transport.clone(),
    );
    runtime
        .block_on(novarocks_backend::run_backend_server_until_shutdown(
            backend,
            data_runtime,
            termination_signal(),
        ))
        .map_err(|error| anyhow::anyhow!("role=be: {error}"))
}

async fn wait_for_stop(mut receiver: tokio::sync::watch::Receiver<bool>) {
    while !*receiver.borrow() {
        if receiver.changed().await.is_err() {
            break;
        }
    }
}

async fn run_all_in_one(
    fe: launch::RoleConfig,
    be: launch::RoleConfig,
    native_compatibility_id: NativeCompatibilityId,
    function_set: std::sync::Arc<novarocks_execution::exec::expr::agg::SealedExecutionFunctionSet>,
    provider_manifest: Arc<ServerProviderManifest>,
    runtime: tokio::runtime::Handle,
) -> anyhow::Result<()> {
    initialize_backend_file_caches(&be.config);
    let frontend = composition::compose_frontend_server_config(
        &fe.config,
        &fe.native_trust,
        None,
        native_compatibility_id,
        std::sync::Arc::clone(function_set.catalog()),
        Arc::clone(&provider_manifest),
        runtime.clone(),
    )?;
    let backend = composition::compose_backend_server_config(
        &be.config,
        &be.native_trust,
        native_compatibility_id,
        function_set,
        provider_manifest,
        runtime.clone(),
    )?;
    let backend_runtime = novarocks_backend::BackendDataRuntime::new(
        runtime.clone(),
        std::sync::Arc::clone(&backend.native_trust),
        backend.native_transport.clone(),
    );
    let (frontend_stop_tx, frontend_stop_rx) = tokio::sync::watch::channel(false);
    let (backend_stop_tx, backend_stop_rx) = tokio::sync::watch::channel(false);
    let frontend_runtime = runtime.clone();
    let frontend_run = async move {
        novarocks_frontend::run_frontend_server_until_shutdown(
            frontend,
            frontend_runtime,
            wait_for_stop(frontend_stop_rx),
        )
        .await
        .map_err(|error| anyhow::anyhow!("{error}"))
    };
    let backend_run = async move {
        novarocks_backend::run_backend_server_until_shutdown(
            backend,
            backend_runtime,
            wait_for_stop(backend_stop_rx),
        )
        .await
        .map_err(|error| anyhow::anyhow!("{error}"))
    };
    novarocks_server::supervisor::supervise_all_in_one(
        frontend_run,
        backend_run,
        frontend_stop_tx,
        backend_stop_tx,
        termination_signal(),
    )
    .await
}

/// Initialize the BE-local file caches before the first connector reader can
/// create a query-scoped cache context. FE does not own these process-local
/// execution resources.
fn initialize_backend_file_caches(config: &NovaRocksConfig) {
    let cache = &config.runtime.cache;
    if cache.page_cache_enable {
        let _ = novarocks_fs::DataCacheManager::instance().init_page_cache(
            novarocks_fs::DataCachePageCacheOptions {
                capacity: cache.page_cache_capacity,
                evict_probability: cache.page_cache_evict_probability,
            },
        );
    }
    let _ = novarocks_fs::init_parquet_cache(novarocks_fs::ParquetCacheOptions {
        enable_metadata: cache.parquet_meta_cache_enable,
        metadata_ttl: std::time::Duration::from_secs(cache.parquet_meta_cache_ttl_seconds),
        enable_page: cache.parquet_page_cache_enable,
    });
}

fn run(args: launch::StandaloneLaunchArgs) -> anyhow::Result<()> {
    let resolved = launch::resolve_server_launch(args)?;
    let process_config = match &resolved {
        launch::ResolvedServerLaunch::Fe(role) | launch::ResolvedServerLaunch::Be(role) => {
            &role.config
        }
        launch::ResolvedServerLaunch::AllInOne { fe, .. } => &fe.config,
    };
    let provider_manifest = Arc::new(ServerProviderManifest::seal()?);
    let runtime = init_process(process_config)?;
    let function_set = compose_process_function_set()?;
    let functions = std::sync::Arc::clone(function_set.catalog());
    let native_compatibility = native_compatibility::resolve_native_compatibility_material(
        provider_manifest.contracts(),
        functions.digest(),
        function_set.implementation_manifest_digest(),
    )?;
    tracing::info!(
        native_compatibility_id = %native_compatibility.id(),
        function_catalog_digest = %hex::encode(functions.digest()),
        execution_implementation_manifest_digest = %hex::encode(function_set.implementation_manifest_digest()),
        build_identity = novarocks_version::native_build_identity(),
        "resolved native compatibility material"
    );
    match resolved {
        launch::ResolvedServerLaunch::Fe(role) => run_frontend(
            role,
            native_compatibility.id(),
            functions,
            provider_manifest,
            &runtime,
        ),
        launch::ResolvedServerLaunch::Be(role) => run_backend(
            role,
            native_compatibility.id(),
            function_set,
            provider_manifest,
            &runtime,
        ),
        launch::ResolvedServerLaunch::AllInOne { fe, be } => runtime.block_on(run_all_in_one(
            fe,
            be,
            native_compatibility.id(),
            function_set,
            provider_manifest,
            runtime.handle().clone(),
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::compose_process_function_set;
    use novarocks_functions::{FunctionKind, FunctionVisibility};

    #[test]
    fn native_compatibility_uses_one_sealed_process_function_set() {
        let function_set = compose_process_function_set().expect("sealed process function set");
        let catalog = function_set.catalog();
        let definition = catalog
            .definition(
                novarocks_connector_iceberg_functions::ICEBERG_THETA_AGGREGATE_NAME,
                FunctionKind::Aggregate,
            )
            .expect("Iceberg hidden aggregate metadata");

        assert_eq!(definition.visibility(), FunctionVisibility::Hidden);
        assert_ne!(catalog.digest(), [0; 32]);
        assert_ne!(function_set.implementation_manifest_digest(), [0; 32]);
    }
}

fn main() {
    let args = env::args().skip(1).collect::<Vec<_>>();
    if args
        .first()
        .is_none_or(|command| command == "--help" || command == "-h")
    {
        usage();
        return;
    }
    if args.first().is_none_or(|command| command != "standalone") {
        eprintln!("the only server command is `standalone`");
        usage();
        process::exit(1);
    }
    let parsed = match launch::parse_standalone_launch_args(&args[1..]) {
        Ok(Some(parsed)) => parsed,
        Ok(None) => {
            usage();
            return;
        }
        Err(error) => {
            eprintln!("{error}");
            usage();
            process::exit(1);
        }
    };
    if let Err(error) = run(parsed) {
        eprintln!("{error:#}");
        process::exit(1);
    }
}
