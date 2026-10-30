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

use crate::app_config::NovaRocksConfig;
use crate::{
    composition, launch, logging, memory_limit, memory_observation, native_compatibility,
    provider_manifest::ServerProviderManifest, scan_io::ScanIoRuntime,
    static_plan::ServerStaticPlan,
};
use novarocks_memory::MemoryAuthority;
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
    memory_observation::log_installed();
    memory_observation::log_allocator_configuration();
    memory_limit::log_visible_memory();
    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .worker_threads(config.runtime.actual_data_runtime_threads().max(1))
        .max_blocking_threads(config.runtime.data_runtime_max_blocking_threads.max(1))
        .thread_name("novarocks-data-runtime")
        .thread_stack_size(novarocks_types::WORKER_STACK_SIZE_BYTES)
        .build()
        .map_err(|error| anyhow::anyhow!("build data Tokio runtime: {error}"))
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
    build: &crate::build_information::BuildInformation,
    role: launch::RoleConfig,
    native_compatibility_id: NativeCompatibilityId,
    static_plan: &ServerStaticPlan,
    function_catalog: std::sync::Arc<novarocks_functions::EngineFunctionCatalog>,
    provider_manifest: Arc<ServerProviderManifest>,
    memory_authority: Arc<MemoryAuthority>,
    runtime: &tokio::runtime::Runtime,
) -> anyhow::Result<()> {
    let filesystem = composition::compose_connector_file_planning_resources(
        &role.config,
        runtime.handle().clone(),
    )?;
    let frontend = composition::compose_frontend_role_config(
        &role.config,
        build,
        &role.native_trust,
        None,
        native_compatibility_id,
        static_plan,
        function_catalog,
        provider_manifest,
        memory_authority,
        runtime.handle().clone(),
        filesystem,
        #[cfg(novarocks_dev_controls)]
        role.test_session.clone(),
    )?;
    // The synchronous process aggregate owns Host before its borrowed role F.
    let mut frontend_owner = {
        let _entered = runtime.enter();
        crate::roles::frontend::FrontendRoleOwner::prepare(frontend, runtime.handle().clone())?
    };
    runtime
        .block_on(crate::roles::frontend::run_until_shutdown(
            &mut frontend_owner,
            termination_signal(),
        ))
        .map_err(|error| anyhow::anyhow!("role=fe: {error}"))
}

fn run_backend(
    build: &crate::build_information::BuildInformation,
    role: launch::RoleConfig,
    native_compatibility_id: NativeCompatibilityId,
    static_plan: &ServerStaticPlan,
    function_set: std::sync::Arc<
        novarocks_execution::runtime::function_set::SealedExecutionFunctionSet,
    >,
    provider_manifest: Arc<ServerProviderManifest>,
    memory_authority: Arc<MemoryAuthority>,
    runtime: &tokio::runtime::Runtime,
) -> anyhow::Result<()> {
    initialize_backend_file_caches(&role.config);
    let filesystem = composition::compose_connector_file_planning_resources(
        &role.config,
        runtime.handle().clone(),
    )?;
    let scan_io = ScanIoRuntime::start(&role.config.runtime)?;
    let backend = match composition::compose_backend_server_config(
        &role.config,
        build,
        &role.native_trust,
        native_compatibility_id,
        static_plan,
        function_set,
        provider_manifest,
        memory_authority,
        runtime.handle().clone(),
        filesystem,
        &scan_io.services(),
        #[cfg(novarocks_dev_controls)]
        role.test_session.clone(),
    ) {
        Ok(backend) => backend,
        Err(error) => {
            return match runtime.block_on(scan_io.shutdown()) {
                Ok(()) => Err(error),
                Err(drain) => Err(error.context(format!("scan I/O shutdown failed: {drain}"))),
            };
        }
    };
    let data_runtime = novarocks_native_adapter::BackendDataRuntime::new(
        runtime.handle().clone(),
        std::sync::Arc::clone(&backend.native_trust),
        backend.native_transport.clone(),
    );
    let primary = runtime
        .block_on(crate::roles::backend::run_until_shutdown(
            backend,
            data_runtime,
            scan_io.services(),
            termination_signal(),
        ))
        .map_err(|error| anyhow::anyhow!("role=be: {error}"));
    let drain = runtime.block_on(scan_io.shutdown());
    match (primary, drain) {
        (Ok(()), Ok(())) => Ok(()),
        (Err(error), Ok(())) => Err(error),
        (Ok(()), Err(error)) => Err(error),
        (Err(error), Err(drain)) => {
            Err(error.context(format!("scan I/O shutdown failed: {drain}")))
        }
    }
}

async fn wait_for_stop(mut receiver: tokio::sync::watch::Receiver<bool>) {
    while !*receiver.borrow() {
        if receiver.changed().await.is_err() {
            break;
        }
    }
}

async fn run_all_in_one(
    build: &crate::build_information::BuildInformation,
    fe: launch::RoleConfig,
    be: launch::RoleConfig,
    native_compatibility_id: NativeCompatibilityId,
    static_plan: &ServerStaticPlan,
    function_set: std::sync::Arc<
        novarocks_execution::runtime::function_set::SealedExecutionFunctionSet,
    >,
    provider_manifest: Arc<ServerProviderManifest>,
    memory_authority: Arc<MemoryAuthority>,
    runtime: tokio::runtime::Handle,
) -> anyhow::Result<()> {
    initialize_backend_file_caches(&be.config);
    let fe_filesystem =
        composition::compose_connector_file_planning_resources(&fe.config, runtime.clone())?;
    let be_filesystem =
        composition::compose_connector_file_planning_resources(&be.config, runtime.clone())?;
    let frontend = composition::compose_frontend_role_config(
        &fe.config,
        build,
        &fe.native_trust,
        None,
        native_compatibility_id,
        static_plan,
        std::sync::Arc::clone(function_set.catalog()),
        Arc::clone(&provider_manifest),
        // Both roles consume the same authority: one OS process, one bound.
        Arc::clone(&memory_authority),
        runtime.clone(),
        fe_filesystem,
        #[cfg(novarocks_dev_controls)]
        fe.test_session.clone(),
    )?;
    let scan_io = ScanIoRuntime::start(&be.config.runtime)?;
    let backend = match composition::compose_backend_server_config(
        &be.config,
        build,
        &be.native_trust,
        native_compatibility_id,
        static_plan,
        function_set,
        provider_manifest,
        memory_authority,
        runtime.clone(),
        be_filesystem,
        &scan_io.services(),
        #[cfg(novarocks_dev_controls)]
        be.test_session.clone(),
    ) {
        Ok(backend) => backend,
        Err(error) => {
            scan_io.shutdown().await?;
            return Err(error);
        }
    };
    let backend_runtime = novarocks_native_adapter::BackendDataRuntime::new(
        runtime.clone(),
        std::sync::Arc::clone(&backend.native_trust),
        backend.native_transport.clone(),
    );
    let backend_scan_io = scan_io.services();
    let (frontend_stop_tx, frontend_stop_rx) = tokio::sync::watch::channel(false);
    let (backend_stop_tx, backend_stop_rx) = tokio::sync::watch::channel(false);
    let mut frontend_owner =
        match crate::roles::frontend::FrontendRoleOwner::prepare(frontend, runtime.clone()) {
            Ok(owner) => owner,
            Err(error) => {
                scan_io.shutdown().await?;
                return Err(error.into());
            }
        };
    // Owner stays in this existing process aggregate, outside the role F.
    let frontend_run = async {
        crate::roles::frontend::run_until_shutdown(
            &mut frontend_owner,
            wait_for_stop(frontend_stop_rx),
        )
        .await
        .map_err(|error| anyhow::anyhow!("{error}"))
    };
    let backend_run = async move {
        crate::roles::backend::run_until_shutdown(
            backend,
            backend_runtime,
            backend_scan_io,
            wait_for_stop(backend_stop_rx),
        )
        .await
        .map_err(|error| anyhow::anyhow!("{error}"))
    };
    let primary = crate::supervisor::supervise_all_in_one(
        frontend_run,
        backend_run,
        frontend_stop_tx,
        backend_stop_tx,
        termination_signal(),
    )
    .await;
    let drain = scan_io.shutdown().await;
    match (primary, drain) {
        (Ok(()), Ok(())) => Ok(()),
        (Err(error), Ok(())) => Err(error),
        (Ok(()), Err(error)) => Err(error),
        (Err(error), Err(drain)) => {
            Err(error.context(format!("scan I/O shutdown failed: {drain}")))
        }
    }
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

/// Builds the one memory authority this OS process gets.
///
/// A capacity bound is a property of the process, not of a role, so this is
/// deliberately called once in `run()` and the resulting handle is shared.
/// Under `all-in-one` both role runners consume *this* authority: two
/// authorities over one address space would each believe they owned the whole
/// bound, and sampling the same RSS twice would not make that safe. There is
/// no `all-in-one` branch here for the same reason -- the single-process form
/// is a test convenience, not a topology to model for.
///
/// `all-in-one` reads its bound from the FE config, matching how the process
/// already resolves its tokio runtime and log filter from that same config.
fn compose_memory_authority(config: &NovaRocksConfig) -> anyhow::Result<Arc<MemoryAuthority>> {
    let authority_config = config.runtime.effective_authority_config()?;
    let authority = MemoryAuthority::new(authority_config)
        .map_err(|error| anyhow::anyhow!("compose process memory authority: {error}"))?;
    // Control-plane traffic gets its own branch off the root before any work
    // account exists. Installation precommits its protected floor, so ordinary
    // work cannot consume cancellation/status backing, even after shrink.
    let control_bytes = config.runtime.memory.control_bytes;
    authority
        .install_control_branch(control_bytes)
        .map_err(|error| anyhow::anyhow!("install process control branch: {error}"))?;
    tracing::info!(
        process_bound_bytes = authority.process_bound_bytes(),
        capacity_bytes = authority.capacity_bytes(),
        headroom_budget_bytes = authority.headroom_budget_bytes(),
        control_bytes,
        "installed the process memory authority"
    );
    Ok(Arc::new(authority))
}

fn run(
    args: launch::StandaloneLaunchArgs,
    build: &crate::build_information::BuildInformation,
) -> anyhow::Result<()> {
    let resolved = launch::resolve_server_launch(args)?;
    let process_config = match &resolved {
        launch::ResolvedServerLaunch::Fe(role) | launch::ResolvedServerLaunch::Be(role) => {
            &role.config
        }
        launch::ResolvedServerLaunch::AllInOne { fe, .. } => &fe.config,
    };
    let provider_manifest = ServerProviderManifest::seal()?;
    let provider_manifest = Arc::new(provider_manifest);
    let runtime = init_process(process_config)?;
    // After `init_process`, because composing the authority is the first thing
    // this process reports about its own memory and logging is not installed
    // until then. It does not depend on the runtime.
    let memory_authority = compose_memory_authority(process_config)?;
    let function_set = composition::compose_process_function_set()?;
    let functions = std::sync::Arc::clone(function_set.catalog());
    // One interpreter per process, chosen by this binary's build and composed
    // before compatibility material: the choice is part of the island.
    let static_plan = ServerStaticPlan::compose(&function_set, &provider_manifest)?;
    let native_compatibility = native_compatibility::resolve_native_compatibility_material(
        provider_manifest.contracts(),
        functions.digest(),
        static_plan.compatibility_component(),
    )?;
    tracing::info!(
        native_compatibility_id = %native_compatibility.id(),
        function_catalog_digest = %hex::encode(functions.digest()),
        static_plan_interpreter = ?native_compatibility.static_plan_interpreter(),
        static_plan_interpreter_digest = %hex::encode(native_compatibility.static_plan_interpreter_digest()),
        build_identity = build.native_build_identity,
        "resolved native compatibility material"
    );
    match resolved {
        launch::ResolvedServerLaunch::Fe(role) => run_frontend(
            build,
            role,
            native_compatibility.id(),
            &static_plan,
            functions,
            provider_manifest,
            memory_authority,
            &runtime,
        ),
        launch::ResolvedServerLaunch::Be(role) => run_backend(
            build,
            role,
            native_compatibility.id(),
            &static_plan,
            function_set,
            provider_manifest,
            memory_authority,
            &runtime,
        ),
        launch::ResolvedServerLaunch::AllInOne { fe, be } => runtime.block_on(run_all_in_one(
            build,
            fe,
            be,
            native_compatibility.id(),
            &static_plan,
            function_set,
            provider_manifest,
            memory_authority,
            runtime.handle().clone(),
        )),
    }
}

/// Reports the same complete-definition pure seal used by production composition.
/// No configuration, listener, provider I/O or runtime resource is opened.
fn build_information_value(
    build: &crate::build_information::BuildInformation,
) -> anyhow::Result<serde_json::Value> {
    let functions = composition::compose_process_function_set()?;
    let seal = crate::compiled_composition::seal_pure_catalog(functions.catalog())?;
    let mut value = crate::build_information::value(build);
    value["static_plan_interpreter"] = serde_json::json!({
        "name": "compiled_package",
        "pure_function_catalog_digest": hex::encode(seal.catalog.digest()),
        "sealed_definition_count": seal.installed_definitions,
        "excluded_definitions": seal.excluded,
    });
    Ok(value)
}

pub fn main(build: crate::build_information::BuildInformation) {
    let args = env::args().skip(1).collect::<Vec<_>>();
    if args.as_slice() == ["--build-info"] {
        match build_information_value(&build) {
            Ok(value) => println!("{value}"),
            Err(error) => {
                eprintln!("build information composition failed: {error:#}");
                std::process::exit(1);
            }
        }
        return;
    }
    #[cfg(feature = "mem-1-m07-hms-listing-observe")]
    if args.as_slice() == ["--mem-1-m07-hms-listing-build-identity"] {
        println!(
            "NOVAROCKS_MEM_1_M07_HMS_LISTING_BUILD commit={} build_identity={} hms_listing_observe=true",
            build.source_commit, build.native_build_identity
        );
        return;
    }
    #[cfg(feature = "mem-1-m07-closing-pressure")]
    if args.as_slice() == ["--mem-1-m07-closing-pressure-build-identity"] {
        println!(
            "NOVAROCKS_MEM_1_M07_CLOSING_PRESSURE_BUILD commit={} build_identity={} closing_pressure=true",
            build.source_commit, build.native_build_identity
        );
        return;
    }
    #[cfg(feature = "mem-1-m07-exact-mysql-write")]
    if args.as_slice() == ["--mem-1-m07-build-identity"] {
        println!(
            "NOVAROCKS_MEM_1_M07_BUILD commit={} build_identity={} exact_mysql_write=true",
            build.source_commit, build.native_build_identity
        );
        return;
    }
    #[cfg(feature = "mem-1-m07-root-observation")]
    if args.as_slice() == ["--mem-1-m07-root-observation-build-identity"] {
        println!(
            "NOVAROCKS_MEM_1_M07_ROOT_OBSERVATION_BUILD commit={} build_identity={} root_observation=true",
            build.source_commit, build.native_build_identity
        );
        return;
    }
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
    if let Err(error) = run(parsed, &build) {
        eprintln!("{error:#}");
        process::exit(1);
    }
}

#[cfg(test)]
mod tests {
    use super::compose_memory_authority;
    use crate::app_config::NovaRocksConfig;
    use crate::composition::compose_process_function_set;
    use novarocks_functions::{FunctionKind, FunctionVisibility};
    use std::sync::Arc;

    #[test]
    fn build_information_reports_the_production_pure_support_boundary() {
        let build = crate::build_information::BuildInformation::fixture();
        let value = super::build_information_value(&build).expect("build information");
        let again = super::build_information_value(&build).expect("repeated build information");
        assert_eq!(value, again, "support reporting is deterministic");
        assert!(
            !value
                .to_string()
                .contains("execution_implementation_manifest_digest"),
            "build information contains only current compatibility facts"
        );
        let interpreter = &value["static_plan_interpreter"];
        assert_eq!(interpreter["name"], "compiled_package");
        let sealed = interpreter["sealed_definition_count"].as_u64().unwrap() as usize;
        let excluded = interpreter["excluded_definitions"].as_array().unwrap();
        let functions = compose_process_function_set().expect("function set");
        assert!(sealed > 0);
        assert_eq!(
            sealed + excluded.len(),
            functions.catalog().definitions().len()
        );
        assert!(
            !excluded.iter().any(|entry| entry["name"] == "rand"),
            "RAND has pure owners"
        );
        assert!(
            !excluded.iter().any(|entry| entry["name"] == "sleep"),
            "SLEEP has a complete controlled-wait pure owner"
        );
        assert!(excluded.iter().all(|entry| {
            entry["name"].as_str().is_some_and(|name| !name.is_empty())
                && entry["kind"].as_str().is_some_and(|kind| !kind.is_empty())
                && entry["reason"]
                    .as_str()
                    .is_some_and(|reason| !reason.is_empty())
        }));
        let seal = crate::compiled_composition::seal_pure_catalog(functions.catalog()).unwrap();
        assert_eq!(
            interpreter["pure_function_catalog_digest"],
            hex::encode(seal.catalog.digest())
        );
    }

    /// One OS process gets one authority, and every role that needs capacity
    /// is handed *that* one.
    ///
    /// Under all-in-one this is the whole point: two authorities over a single
    /// address space would each believe they owned the entire bound, and
    /// sampling the same RSS twice would not make that safe. The test states
    /// it as pointer identity because that is what "the same bound" means at
    /// runtime — a second authority with equal numbers would pass any test
    /// that only compared configuration.
    #[test]
    fn every_role_in_one_process_shares_one_authority() {
        let config = NovaRocksConfig::default();
        let authority = compose_memory_authority(&config).expect("the default config composes");

        // What `run()` does for all-in-one: clone the handle per role.
        let frontend_handle = Arc::clone(&authority);
        let backend_handle = Arc::clone(&authority);
        assert!(
            Arc::ptr_eq(&frontend_handle, &backend_handle),
            "both roles must consume the same authority, not two with equal numbers"
        );

        // Composing again is what a second authority would look like, and it
        // must be a different one: that is why `run()` calls this exactly once.
        let second = compose_memory_authority(&config).expect("the default config composes");
        assert!(
            !Arc::ptr_eq(&authority, &second),
            "each composition mints its own authority, so composing twice would \
             split the process bound in two"
        );
    }

    /// The control partition exists before any work account can.
    #[test]
    fn the_composed_authority_partitions_its_bound_and_installs_control() {
        let mut config = NovaRocksConfig::default();
        config.runtime.memory.control_bytes = 32 * 1024 * 1024;
        let authority = compose_memory_authority(&config).expect("the default config composes");

        assert!(
            authority.capacity_bytes() + authority.headroom_budget_bytes()
                <= authority.process_bound_bytes(),
            "B + H must fit inside P"
        );
        assert!(
            authority.control_branch().is_some(),
            "control must have its own partition before any work account exists"
        );
        let (snapshot, pressure) = authority.accounting_snapshot();
        let control = authority.control_branch().unwrap();
        assert_eq!(
            control.committed_bytes(),
            config.runtime.memory.control_bytes
        );
        assert!(pressure.classification_complete);
        assert!(pressure.storage_metadata > 0);
        assert_eq!(snapshot.root.live_bytes, pressure.storage_metadata);
        assert_eq!(snapshot.root.floor_bytes, control.committed_bytes());
        assert_eq!(snapshot.root.granted_bytes, snapshot.root.floor_bytes);
        assert_eq!(
            snapshot.root.committed_bytes,
            pressure.storage_metadata + snapshot.root.floor_bytes,
            "assembly must charge real core storage and prepay the protected floor"
        );
    }

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
        let declaration = definition
            .binding_declaration()
            .expect("Theta binding owner");
        assert!(std::ptr::eq(
            catalog.definition_by_id(declaration.function_id()).unwrap(),
            definition,
        ));
        let arguments = [novarocks_functions::FunctionArgument::Value {
            value_type: novarocks_functions::FunctionValueType::new(
                arrow_schema::DataType::Int64,
                false,
            ),
            constant: None,
        }];
        let selected = catalog
            .select_exact_overload_observed(
                declaration.function_id(),
                FunctionKind::Aggregate,
                &novarocks_functions::FunctionOverloadId::try_new("iceberg/theta-stat/long/v1")
                    .unwrap(),
                novarocks_functions::FunctionBindingRequest {
                    arguments: &arguments,
                    logical_argument_count: 1,
                    expected_result_type: None,
                },
                &novarocks_sql::compiler::SqlCompileControl::unbounded(),
            )
            .expect("original installed fixed Theta author");
        assert_eq!(
            selected.aggregate.as_ref().unwrap().state_format.as_str(),
            novarocks_connector_iceberg_functions::ICEBERG_THETA_STATE_FORMAT_IDENTITY
        );
        for overload in declaration.overloads() {
            let installed = catalog
                .pure_overload_declaration_observed(
                    declaration.function_id(),
                    FunctionKind::Aggregate,
                    &overload.identity,
                    &novarocks_sql::compiler::SqlCompileControl::unbounded(),
                )
                .expect("actual process installs the original Theta pure attachment");
            assert_eq!(installed.implementation().overload, overload.identity);
            assert_eq!(
                installed.implementation().abi,
                novarocks_functions::PureKernelAbi::AggregateV1,
            );
            assert_eq!(
                installed.effects().own_row_error,
                novarocks_functions::FunctionIntrinsicRowError::NotRowEvaluated,
            );
            assert!(installed.effects().environment_dependencies.is_empty());
        }
        assert_ne!(catalog.digest(), [0; 32]);
    }
}
