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

use anyhow::{Context, anyhow};
use std::num::NonZeroUsize;
use std::time::Duration;

use crate::app_config::NovaRocksConfig;
use crate::native_trust::{NativeTrustSnapshot, NativeTrustTransport};
use crate::roles::frontend::FrontendRoleConfig;
use crate::state_store_config::SQLITE_STATE_STORE_PROVIDER_ID;
use crate::state_store_limits::resolve_state_store_limits;
use novarocks_connector_iceberg::access_binding::IcebergReadBinding;
use novarocks_connector_iceberg::resources::IcebergExecutionResources;
use novarocks_connector_iceberg::storage_inspector::{
    IcebergStorageInspector, IcebergStoragePartitionTransform,
};
use novarocks_execution::runtime::execution_runtime::ExecutionRuntimeConfig;
use novarocks_frontend_application::task_protocol_config::{
    CoordinationBudgets, DispatchBudget, TaskUpdateRetryPolicy,
};
use novarocks_frontend_application::{
    ClusterBackendOpenConfig, FrontendApplicationOpenConfig, FrontendExecutionConfig,
    FrontendLogicalExecutionRuntimeConfig, FrontendManagementConfig, FrontendNativeCapacityInputs,
    FrontendQueryControlTimeouts, FrontendResultCapacityProfile, FrontendServingConfig,
    FrontendStatementIngress, FrontendWorkloadCapacityConfig, FrontendWorkloadCapacityProfile,
};
use novarocks_fs::{
    FsAccessResolver, FsAccessResources, ObjectStoreProviderPool, ObjectStoreProviderPoolOptions,
    TokioFileIoRuntime, TokioFileTaskSpawner,
};
use novarocks_mv_application::maintenance::MaintenanceCoordinatorConfig;
use novarocks_mv_application::scheduler::MvSchedulerConfig;
use novarocks_native_adapter::FrontendTaskTransportBudget;
use novarocks_native_adapter::backend_application::BackendServerConfig;
use novarocks_query_application::coordination::{
    LogicalExecutionRowsConfig, LogicalExecutionSupervisorConfig,
};
use novarocks_query_application::cpu::{QueryBlockingExecutorConfig, QueryCpuExecutorConfig};
use novarocks_query_application::publication::LakePublicationRuntimePolicy;
use novarocks_spi::connector::{
    ConnectorControlPlanningLease, ConnectorError, ConnectorErrorKind, ConnectorRequestContext,
    ConnectorTableMetadata, MvCreateSourceObservation, MvCreatedTargetObservation,
    MvMaintenanceMetadataObservation, MvObservedField, MvObservedMaintenancePolicy,
    MvObservedPartitionField, MvObservedPartitionSpec, MvObservedPartitionTransform,
    MvObservedRefreshMarker, MvObservedSnapshot, MvObservedSourceField, MvRefreshBaseObservation,
    MvRefreshTargetObservation, MvSchemaValidationObservation, MvStorageObservationPort,
    WriteCommitEvidenceLimits,
};
use novarocks_state_store_api::{MAX_KEY_BYTES, StateStoreProviderDescriptor};
use novarocks_state_store_runtime::{
    StateStoreHostInput, StateStoreProviderRegistration, StateStoreProviderRegistry,
};
use novarocks_state_store_sqlite::SqliteStateStoreContribution;
use novarocks_types::{ClusterRole, NativeCompatibilityId};
use novarocks_worker::{LeaseBounds, OperationWaitCaps};
use novarocks_workload_control::WorkloadConfig;

use crate::paimon_access::ServerPaimonRoleFileIoFactory;
use crate::provider_manifest::ServerProviderManifest;
use crate::scan_io::ScanIoServices;
use crate::static_plan::ServerStaticPlan;
use novarocks_execution::runtime::function_set::{
    ExecutionFunctionSetBuilder, SealedExecutionFunctionSet,
};

/// Seals the process metadata and its installed pure owners from the builtin
/// declarations and the original Iceberg function bundle.
pub fn compose_process_function_set() -> anyhow::Result<std::sync::Arc<SealedExecutionFunctionSet>>
{
    let mut builder = ExecutionFunctionSetBuilder::new();
    novarocks_sql::compiler::contribute_builtin_functions(builder.catalog_builder_mut())
        .map_err(|error| anyhow!("contribute builtin function metadata: {error}"))?;
    novarocks_functions::FunctionBundleContributor::contribute(
        &novarocks_connector_iceberg_functions::IcebergFunctionBundle,
        builder.catalog_builder_mut(),
    )
    .map_err(|error| anyhow!("contribute Iceberg function metadata: {error}"))?;
    Ok(std::sync::Arc::new(builder.seal().map_err(|error| {
        anyhow!("seal process engine function set: {error}")
    })?))
}

#[derive(Clone, Copy, Debug, Default)]
pub struct IcebergMvStorageObservationAdapter {
    inspector: IcebergStorageInspector,
}

/// Map the inspector's partition facts onto the SPI observation projection.
///
/// Shared by the created-target, schema-validation, and refresh-target
/// observations so these sealed provider facts cannot drift.
fn mv_partition_observation(
    observed: novarocks_connector_iceberg::storage_inspector::IcebergStoragePartitionContract,
) -> MvObservedPartitionSpec {
    MvObservedPartitionSpec::new(
        observed.target_spec_id,
        observed
            .fields
            .into_iter()
            .map(|field| {
                MvObservedPartitionField::new(
                    field.partition_field_id,
                    field.partition_field_name,
                    field.source_target_field_id,
                    field.source_column_name,
                    match field.transform {
                        IcebergStoragePartitionTransform::Identity => {
                            MvObservedPartitionTransform::Identity
                        }
                        IcebergStoragePartitionTransform::Year => {
                            MvObservedPartitionTransform::Year
                        }
                        IcebergStoragePartitionTransform::Month => {
                            MvObservedPartitionTransform::Month
                        }
                        IcebergStoragePartitionTransform::Day => MvObservedPartitionTransform::Day,
                        IcebergStoragePartitionTransform::Hour => {
                            MvObservedPartitionTransform::Hour
                        }
                        IcebergStoragePartitionTransform::Bucket { num_buckets } => {
                            MvObservedPartitionTransform::Bucket { num_buckets }
                        }
                        IcebergStoragePartitionTransform::Truncate { width } => {
                            MvObservedPartitionTransform::Truncate { width }
                        }
                        IcebergStoragePartitionTransform::Void => {
                            MvObservedPartitionTransform::Void
                        }
                    },
                )
            })
            .collect(),
    )
}

impl MvStorageObservationPort for IcebergMvStorageObservationAdapter {
    fn observe_create_source(
        &self,
        exact_lease: &ConnectorControlPlanningLease,
        metadata: &ConnectorTableMetadata,
        context: ConnectorRequestContext,
    ) -> Result<MvCreateSourceObservation, ConnectorError> {
        let observed =
            self.inspector
                .observe_create_source(exact_lease, metadata, context.clone())?;
        if metadata.version.as_ref() != Some(&observed.schema_version) {
            return Err(ConnectorError::new(
                ConnectorErrorKind::CorruptData,
                "Iceberg CREATE source observation does not match its sealed metadata schema version",
            ));
        }
        let fields = observed
            .fields
            .into_iter()
            .map(|field| {
                MvObservedSourceField::try_new(
                    field.provider_field_id,
                    field.name,
                    field.type_signature,
                    field.nullable,
                )
            })
            .collect::<Result<Vec<_>, _>>()?;
        MvCreateSourceObservation::try_new(
            metadata.identity.clone(),
            observed.object_id,
            observed.schema_version,
            fields,
            &context,
        )
    }

    fn observe_created_target(
        &self,
        exact_lease: &ConnectorControlPlanningLease,
        metadata: &ConnectorTableMetadata,
        context: ConnectorRequestContext,
    ) -> Result<MvCreatedTargetObservation, ConnectorError> {
        let observed =
            self.inspector
                .observe_created_target(exact_lease, metadata, context.clone())?;
        let fields = observed
            .fields
            .into_iter()
            .map(|field| {
                MvObservedField::new(
                    field.field_id,
                    field.name,
                    field.type_signature,
                    field.nullable,
                )
            })
            .collect();
        let partition = mv_partition_observation(observed.partition);
        MvCreatedTargetObservation::try_new(
            metadata.identity.clone(),
            observed.table_uuid,
            observed.schema_id,
            fields,
            partition,
            &context,
        )
    }

    fn observe_schema_validation(
        &self,
        exact_lease: &ConnectorControlPlanningLease,
        metadata: &ConnectorTableMetadata,
        context: ConnectorRequestContext,
    ) -> Result<MvSchemaValidationObservation, ConnectorError> {
        let observed =
            self.inspector
                .observe_schema_validation(exact_lease, metadata, context.clone())?;
        let fields = observed
            .fields
            .into_iter()
            .map(|(ordinal, field)| {
                Ok((
                    ordinal,
                    MvObservedSourceField::try_new(
                        field.provider_field_id,
                        field.name,
                        field.type_signature,
                        field.nullable,
                    )?,
                ))
            })
            .collect::<Result<Vec<_>, ConnectorError>>()?;
        MvSchemaValidationObservation::try_new(
            metadata.identity.clone(),
            observed.object_id,
            observed.metadata_version,
            observed.schema_version,
            observed.partition_spec_version,
            observed.format_v3,
            observed.explicit_row_lineage_enabled,
            fields,
            observed.partition_fields,
            &context,
        )
    }

    fn observe_refresh_base(
        &self,
        exact_lease: &ConnectorControlPlanningLease,
        metadata: &ConnectorTableMetadata,
        context: ConnectorRequestContext,
    ) -> Result<MvRefreshBaseObservation, ConnectorError> {
        let observed =
            self.inspector
                .observe_refresh_base(exact_lease, metadata, context.clone())?;
        MvRefreshBaseObservation::try_new(
            metadata.identity.clone(),
            observed.object_id,
            observed.current_snapshot_id,
            &context,
        )
    }

    fn observe_refresh_target(
        &self,
        exact_lease: &ConnectorControlPlanningLease,
        metadata: &ConnectorTableMetadata,
        context: ConnectorRequestContext,
    ) -> Result<MvRefreshTargetObservation, ConnectorError> {
        let observed =
            self.inspector
                .observe_refresh_target(exact_lease, metadata, context.clone())?;
        MvRefreshTargetObservation::try_new(
            metadata.identity.clone(),
            observed.table_uuid,
            observed.schema_id,
            mv_partition_observation(observed.partition),
            observed.current_snapshot_id,
            observed.ref_snapshot_ids,
            observed.field_ids,
            observed.main_ancestor_snapshot_ids,
            observed
                .snapshot_markers
                .into_iter()
                .map(|(snapshot_id, marker)| {
                    (
                        snapshot_id,
                        MvObservedRefreshMarker {
                            publication_id: marker.publication_id,
                        },
                    )
                })
                .collect(),
            &context,
        )
    }

    fn observe_maintenance_metadata(
        &self,
        exact_lease: &ConnectorControlPlanningLease,
        metadata: &ConnectorTableMetadata,
        context: ConnectorRequestContext,
    ) -> Result<MvMaintenanceMetadataObservation, ConnectorError> {
        let observed =
            self.inspector
                .observe_maintenance_metadata(exact_lease, metadata, context.clone())?;
        MvMaintenanceMetadataObservation::try_new(
            observed.current_snapshot_id,
            observed
                .snapshots
                .into_iter()
                .map(|snapshot| MvObservedSnapshot {
                    snapshot_id: snapshot.snapshot_id,
                    timestamp_ms: snapshot.timestamp_ms,
                })
                .collect(),
            observed.non_default_reference_count,
            observed.total_data_files,
            observed.total_delete_files,
            observed.total_files_size_bytes,
            MvObservedMaintenancePolicy {
                maintenance_enabled: observed.policy.maintenance_enabled,
                expire_max_snapshot_age_ms: observed.policy.expire_max_snapshot_age_ms,
                expire_min_snapshots_to_keep: observed.policy.expire_min_snapshots_to_keep,
                target_file_size_bytes: observed.policy.target_file_size_bytes,
            },
            &context,
        )
    }
}

/// Resolve the BE-owned startup facts from the application wire configuration.
///
/// This is intentionally the only Server-to-Backend projection: Backend
/// receives no root configuration and therefore cannot observe unrelated
/// Frontend, StateStore, or connector wire sections.
#[expect(
    clippy::too_many_arguments,
    reason = "This public BE composition boundary validates independently owned startup inputs; an aggregate request would add a second composition schema without a shared invariant."
)]
pub fn compose_backend_server_config(
    config: &NovaRocksConfig,
    build: &crate::build_information::BuildInformation,
    native_trust: &NativeTrustSnapshot,
    native_compatibility_id: NativeCompatibilityId,
    static_plan: &ServerStaticPlan,
    function_set: std::sync::Arc<
        novarocks_execution::runtime::function_set::SealedExecutionFunctionSet,
    >,
    provider_manifest: std::sync::Arc<ServerProviderManifest>,
    memory_authority: std::sync::Arc<novarocks_memory::MemoryAuthority>,
    runtime: tokio::runtime::Handle,
    filesystem: FsAccessResources,
    scan_io: &ScanIoServices,
    #[cfg(novarocks_dev_controls)] test_session: Option<
        novarocks_failpoint::liveness::PreparedRoleSession,
    >,
) -> anyhow::Result<BackendServerConfig> {
    crate::sdk_listing_profile::validate_current()?;
    config
        .server
        .validate_for_role(novarocks_types::ClusterRole::Be)
        .map_err(anyhow::Error::msg)?;
    let control_grpc_port = config
        .server
        .control_grpc_port
        .ok_or_else(|| anyhow!("role=be requires explicit [server].control_grpc_port"))?;
    let control_endpoint = native_trust.control_advertised_endpoint().ok_or_else(|| {
        anyhow!("BE native trust snapshot requires a Control advertised endpoint")
    })?;
    let advertise_control_endpoint = novarocks_types::AdvertiseEndpoint {
        host: control_endpoint.host().to_string(),
        port: control_endpoint.port(),
    };
    let runtime_config = &config.runtime;
    let frame_envelope = runtime_config.native_ingress.validate()?;
    tracing::info!(
        ordinary_frame_envelope_bytes = frame_envelope.ordinary_bytes,
        control_frame_envelope_bytes = frame_envelope.control_bytes,
        combined_frame_envelope_bytes = frame_envelope.combined_bytes,
        "validated BE Native ingress frame envelope"
    );
    let advertise_endpoint = novarocks_types::AdvertiseEndpoint {
        host: native_trust.advertised_endpoint().host().to_string(),
        port: native_trust.advertised_endpoint().port(),
    };
    let frontend_endpoint = config
        .cluster
        .frontend_endpoint
        .as_deref()
        .ok_or_else(|| anyhow::anyhow!("role=be requires [cluster].frontend_endpoint"))?
        .parse::<novarocks_types::NativeEndpoint>()
        .map_err(|error| anyhow::anyhow!("parse [cluster].frontend_endpoint: {error}"))?;
    #[cfg(novarocks_dev_controls)]
    let test_slots = crate::provider_test_slots::ProviderTestSlots::default();
    Ok(BackendServerConfig {
        build_identity: build.native_build_identity.to_owned(),
        #[cfg(novarocks_dev_controls)]
        test_session: test_session.map(|prepared| {
            let slots = test_slots.clone();
            novarocks_native_adapter::backend_test_session::BackendTestSessionConfig::new(prepared)
                .with_provider_bind(std::sync::Arc::new(move |scope| slots.bind_backend(scope)))
        }),
        bind_host: config.server.host.clone(),
        grpc_port: config.server.grpc_port,
        control_grpc_port,
        metrics_http_port: config.server.http_port,
        advertise_endpoint,
        advertise_control_endpoint,
        native_trust: std::sync::Arc::clone(native_trust.trust()),
        native_compatibility_id,
        function_set,
        memory_authority: std::sync::Arc::clone(&memory_authority),
        native_transport: backend_native_transport(native_trust.transport()),
        native_ingress: runtime_config.native_ingress.to_adapter_config(),
        frontend_endpoint,
        announce_interval: Duration::from_millis(config.cluster.backend_announce_interval_ms()),
        announce_initial_backoff: Duration::from_millis(
            config.cluster.backend_announce_initial_backoff_ms(),
        ),
        announce_max_backoff: Duration::from_millis(
            config.cluster.backend_announce_max_backoff_ms(),
        ),
        write_commit_evidence_limits: WriteCommitEvidenceLimits::try_new(
            runtime_config.write_commit_evidence_max_bytes,
            runtime_config.write_commit_evidence_max_entries,
        )
        .map_err(|error| anyhow::anyhow!("resolve write commit evidence limits: {error}"))?,
        result_retained_limits: novarocks_worker::WorkerResultRetainedLimits::try_new(
            runtime_config.result_retained_bytes_per_root,
            runtime_config.result_retained_bytes_per_process,
        )
        .map_err(|error| anyhow::anyhow!("resolve native result retained-byte limits: {error}"))?,
        root_producer_limits:
            novarocks_native_adapter::root_result_session::RootProducerLimits::try_new(
                runtime_config.root_result_producer_threads,
                runtime_config.root_result_producer_positions,
                runtime_config.root_result_producer_stack_bytes,
            )
            .map_err(|error| anyhow::anyhow!("resolve root result producer limits: {error}"))?,
        preparation_limits: novarocks_worker::TaskPreparationLimits::try_new(
            config.runtime.task_preparation_max_tasks_per_context,
            config.runtime.task_preparation_max_tasks,
            config.runtime.task_preparation_max_bytes,
            config.runtime.task_preparation_max_workers,
        )
        .map_err(|error| anyhow::anyhow!("resolve preparation limits: {error}"))?,
        inbound_capability_limits: novarocks_worker::TaskInboundCapabilityLimits::try_new(
            runtime_config.task_normal_close_max_records,
            runtime_config.task_normal_close_max_bytes,
        )
        .map_err(|error| anyhow::anyhow!("resolve normal-close capacity limits: {error}"))?,
        execution_runtime_config: backend_execution_runtime_config(config),
        scan_preparation_config: novarocks_worker::ScanPreparationConfig::try_new(
            runtime_config.prefetch_input_bytes_per_stream,
            runtime_config.prefetch_max_candidates,
            Duration::from_millis(runtime_config.prefetch_pause_release_ms),
            Duration::from_millis(runtime_config.prefetch_rearm_ms),
            Duration::from_millis(runtime_config.prefetch_progress_bucket_ms),
        )
        .map_err(|error| anyhow::anyhow!("resolve scan preparation configuration: {error}"))?,
        scan_stream_runtime: scan_io.runtime_handle(),
        catalog_manager_config: novarocks_worker::CatalogManagerConfig {
            max_retained_catalogs: novarocks_worker::DEFAULT_MAX_RETAINED_CATALOGS,
            max_failed_catalogs: runtime_config.catalog_bind_max_failed,
            failed_retention: Duration::from_millis(
                runtime_config.catalog_bind_failed_retention_ms,
            ),
            transient_retry_cooldown: Duration::from_millis(
                runtime_config.catalog_bind_transient_retry_cooldown_ms,
            ),
            provider_max_concurrent_binds: runtime_config.catalog_bind_provider_max_concurrent,
            provider_min_bind_interval: Duration::from_millis(
                runtime_config.catalog_bind_provider_min_interval_ms,
            ),
        },
        execution_role_binding_factories: provider_manifest.compose_execution_factories(
            config,
            runtime,
            &filesystem,
            scan_io,
            #[cfg(novarocks_dev_controls)]
            &test_slots,
        )?,
        static_plan_interpreter: static_plan.backend_interpreter()?,
        process_memory: backend_process_memory_observation(memory_authority),
    })
}

/// What the Backend `/metrics` endpoint reports about this process's memory:
/// the allocator and the settings that took effect, the visible memory, and a
/// sampler for the physical readings.
fn backend_process_memory_observation(
    memory_authority: std::sync::Arc<novarocks_memory::MemoryAuthority>,
) -> novarocks_native_adapter::backend_metrics::ProcessMemoryObservation {
    // An unreadable jemalloc configuration was already logged at start-up;
    // its settings are then not exported rather than guessed.
    let allocator_settings = match crate::memory_observation::jemalloc_configuration() {
        Ok(Some(configuration)) => vec![
            (
                "background_thread",
                i64::from(configuration.background_thread),
            ),
            ("dirty_decay_ms", configuration.dirty_decay_ms),
            ("muzzy_decay_ms", configuration.muzzy_decay_ms),
        ],
        Ok(None) | Err(_) => Vec::new(),
    };
    novarocks_native_adapter::backend_metrics::ProcessMemoryObservation {
        allocator: crate::memory_observation::process_allocator().label(),
        allocator_settings,
        visible_memory: crate::memory_limit::visible_memory()
            .map(|visible| (visible.bytes, visible.bound_label())),
        sample: std::sync::Arc::new(move || {
            (
                crate::memory_observation::attribution_snapshot()
                    .with_observation_metadata_bytes(memory_authority.observation_metadata_bytes()),
                crate::memory_observation::sample_physical(),
            )
        }),
    }
}

/// What the Frontend `/metrics` endpoint reports about this process's memory:
/// the allocator and its physical readings. The Frontend has no memory ledger;
/// these readings serve the measured bound of its third-party internals.
fn frontend_process_memory_observation()
-> novarocks_frontend_application::FrontendProcessMemoryObservation {
    novarocks_frontend_application::FrontendProcessMemoryObservation {
        allocator: crate::memory_observation::process_allocator().label(),
        sample: std::sync::Arc::new(crate::memory_observation::sample_physical),
    }
}

type MvManagementContinuation = (
    novarocks_mv_application::management::RemoteEffectPolicy,
    Option<std::sync::Arc<dyn novarocks_mv_application::management::ManagementAuditSink>>,
    Option<novarocks_frontend_application::StartupIsolationSource>,
);

/// Resolve every Frontend startup input from the application wire configuration.
#[expect(
    clippy::too_many_arguments,
    reason = "This public FE composition boundary validates independently owned startup inputs; an aggregate request would add a second composition schema without a shared invariant."
)]
pub fn compose_frontend_role_config(
    config: &NovaRocksConfig,
    build: &crate::build_information::BuildInformation,
    native_trust: &NativeTrustSnapshot,
    port_override: Option<u16>,
    native_compatibility_id: NativeCompatibilityId,
    static_plan: &ServerStaticPlan,
    function_catalog: std::sync::Arc<novarocks_functions::EngineFunctionCatalog>,
    provider_manifest: std::sync::Arc<ServerProviderManifest>,
    memory_authority: std::sync::Arc<novarocks_memory::MemoryAuthority>,
    runtime: tokio::runtime::Handle,
    filesystem: FsAccessResources,
    #[cfg(novarocks_dev_controls)] test_session: Option<
        novarocks_failpoint::liveness::PreparedRoleSession,
    >,
) -> anyhow::Result<FrontendRoleConfig> {
    crate::sdk_listing_profile::validate_current()?;
    let runtime_config = &config.runtime;
    let constant_policy = crate::compiled_composition::frontend_constant_policy(
        runtime_config
            .frontend_constant_policy
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("runtime.frontend_constant_policy is required for FE"))?
            .policy(),
    )?;
    let runtime_filter_worker_count = NonZeroUsize::new(runtime_config.actual_exec_threads())
        .ok_or_else(|| anyhow::anyhow!("frontend runtime-filter worker count must be nonzero"))?;
    let failure_backoff_ms = config
        .standalone_server
        .as_ref()
        .map(|standalone| standalone.mv_refresh_scheduler_failure_backoff_ms.max(1));
    let catalog_source = config
        .catalog_source
        .as_ref()
        .ok_or_else(|| anyhow::anyhow!("compose frontend server without catalog source preflight"))?
        .input()?;
    let task_execution_budgets = compose_task_execution_budgets(config)?;
    let (logical_supervisor, workload, abort_capacity, result_profile, capacity_inputs) =
        compose_frontend_workload_runtime(runtime_config)?;
    // Runtime settings come from the same original ingress and Native owners.
    let ingress = compose_frontend_statement_ingress()?;
    let capacity_profile = FrontendWorkloadCapacityProfile::try_for_native(
        capacity_inputs,
        &workload,
        result_profile,
        task_execution_budgets.transport,
        ingress,
    )
    .context("compose frontend original-source workload capacity profile")?;
    // The fifth input and the actual pool consume one frozen configuration.
    let query_blocking_config = capacity_profile.inputs().query_blocking_capacity();
    let logical_runtime = FrontendLogicalExecutionRuntimeConfig::new(
        logical_supervisor,
        workload,
        abort_capacity,
        result_profile,
        capacity_profile,
    );
    let mut execution = FrontendExecutionConfig::new(
        runtime_filter_worker_count,
        native_compatibility_id,
        function_catalog,
        logical_runtime,
        constant_policy,
    )
    .with_catalog_desired_state_source(catalog_source)
    .try_with_catalog_prune_config(
        Duration::from_millis(runtime_config.catalog_prune_interval_ms),
        Duration::from_millis(runtime_config.catalog_prune_rpc_timeout_ms),
        runtime_config.catalog_prune_max_inflight,
    )
    .map_err(|error| anyhow::anyhow!("construct catalog prune configuration: {error}"))?
    .with_lake_publication_runtime_policy(
        LakePublicationRuntimePolicy::try_new(
            Duration::from_millis(runtime_config.lake_publication_max_attempt_duration_ms),
            Duration::from_millis(runtime_config.lake_publication_safe_gc_age_ms),
            Duration::from_millis(runtime_config.lake_publication_max_clock_skew_ms),
            Duration::from_millis(runtime_config.lake_publication_listing_visibility_delay_ms),
            Duration::from_millis(runtime_config.lake_publication_scheduler_margin_ms),
        )
        .map_err(|error| anyhow::anyhow!("construct lake publication runtime policy: {error}"))?,
    )
    .with_optimizer_query_mem_limit_bytes(runtime_config.optimizer_query_mem_limit_bytes)
    .with_connector_split_initial_dynamic_filter_wait_cap(Duration::from_millis(
        runtime_config.connector_split_initial_dynamic_filter_wait_cap_ms,
    ))
    .with_catalog_materialization_config(
        novarocks_catalog_application::CatalogMaterializationConfig::try_new(
            Duration::from_millis(runtime_config.catalog_materialization_attempt_timeout_ms),
            Duration::from_millis(runtime_config.catalog_materialization_retry_initial_backoff_ms),
            Duration::from_millis(runtime_config.catalog_materialization_retry_max_backoff_ms),
            runtime_config.catalog_materialization_max_inflight,
        )
        .map_err(|error| {
            anyhow::anyhow!("construct catalog materialization configuration: {error}")
        })?,
    )
    .with_query_control_timeouts(FrontendQueryControlTimeouts {
        pre_start_timeout_ms: runtime_config.query_control_pre_start_timeout_ms,
    })
    .with_task_update_retry_policy(
        TaskUpdateRetryPolicy::try_new(
            Duration::from_millis(runtime_config.query_control_task_update_rpc_timeout_ms),
            Duration::from_millis(runtime_config.query_control_task_update_retry_error_duration_ms),
            Duration::from_millis(
                runtime_config.query_control_task_update_retry_initial_backoff_ms,
            ),
            Duration::from_millis(runtime_config.query_control_task_update_retry_max_backoff_ms),
        )
        .map_err(|error| anyhow::anyhow!("construct task update retry policy: {error}"))?,
    )
    // Composed and validated here, then frozen: the coordinator never reads a
    // process-global configuration per attempt.
    .with_task_execution_budgets(
        task_execution_budgets.coordination,
        task_execution_budgets.transport,
    )
    .with_query_cpu_executor_config(QueryCpuExecutorConfig::with_idle_keepalive(
        Duration::from_millis(runtime_config.frontend_workload.planning_idle_keepalive_ms),
    ))
    .with_query_blocking_executor_config(query_blocking_config);
    // The frontend freezes the one carrier its island's backends interpret,
    // under the same structural package admission as their receiver.
    execution = execution.with_static_plan_carrier(static_plan.frontend_carrier());
    let (remote_effect_policy, management_audit, startup_isolation) =
        mv_management_continuation(config)?;
    execution =
        execution.with_mv_management(remote_effect_policy, management_audit, startup_isolation);
    if let Some(standalone) = config.standalone_server.as_ref() {
        let failure_backoff_ms = failure_backoff_ms.expect("standalone config supplies backoff");
        execution = execution.with_mv_scheduler_config(MvSchedulerConfig::new(
            standalone.mv_refresh_scheduler_enabled,
            standalone.mv_refresh_scheduler_interval_ms.max(1),
            standalone.mv_refresh_scheduler_max_concurrent.max(1),
            failure_backoff_ms,
            standalone
                .mv_refresh_scheduler_max_failure_backoff_ms
                .max(failure_backoff_ms),
        ));
        execution = execution.with_mv_maintenance_config(MaintenanceCoordinatorConfig::new(
            standalone.iceberg_maintenance_enabled,
            standalone.iceberg_maintenance_tick_interval_ms.max(1),
            standalone.iceberg_maintenance_max_concurrent.max(1),
            standalone
                .iceberg_maintenance_compaction_min_data_files
                .try_into()
                .unwrap_or(i64::MAX),
            standalone
                .iceberg_maintenance_dv_min_delete_files
                .try_into()
                .unwrap_or(i64::MAX),
            standalone.iceberg_maintenance_action_cooldown_ms,
            standalone.iceberg_maintenance_max_consecutive_failures,
        ));
    }
    let backend_open = ClusterBackendOpenConfig::new(
        config.cluster.role,
        native_compatibility_id,
        Duration::from_millis(config.cluster.heartbeat_interval_ms()),
        config.cluster.heartbeat_timeout_retries(),
        Duration::from_millis(config.cluster.backend_announce_lease_ttl_ms()),
    )
    .map_err(|error| anyhow::anyhow!("open frontend backend cluster configuration: {error}"))?;
    let mysql_listener = novarocks_mysql_adapter::resolve_mysql_listener_settings(
        config
            .standalone_server
            .as_ref()
            .map(|server| server.mysql_port),
        config
            .standalone_server
            .as_ref()
            .map(|server| server.user.as_str()),
        port_override,
    )
    .map_err(|error| anyhow::anyhow!("resolve MySQL listener settings: {error}"))?;
    let state_store_provider_registry = state_store_provider_registry(config)?;
    let state_store_input = state_store_input(config)?;
    #[cfg(novarocks_dev_controls)]
    let test_slots = crate::provider_test_slots::ProviderTestSlots::default();
    #[cfg(novarocks_dev_controls)]
    if let Some(prepared) = test_session {
        let slots = test_slots.clone();
        execution = execution.with_test_session(
            novarocks_frontend_application::test_session::FrontendTestSessionConfig::new(prepared)
                .with_provider_bind(std::sync::Arc::new(move |scope| slots.bind_frontend(scope))),
        );
    }
    Ok(FrontendRoleConfig {
        memory_authority: std::sync::Arc::clone(&memory_authority),
        application: FrontendApplicationOpenConfig {
            execution,
            backend_open,
            connector_control_role_factories: provider_manifest.compose_control_factories(
                config,
                runtime,
                &filesystem,
                #[cfg(novarocks_dev_controls)]
                &test_slots,
            )?,
            state_store_input,
            state_store_provider_registry,
            native_trust: std::sync::Arc::clone(native_trust.trust()),
            native_transport: frontend_native_transport(native_trust.transport()),
        },
        management: FrontendManagementConfig {
            bind_host: config.server.host.clone(),
            http_port: config.server.http_port,
            native_compatibility_id,
            memory_authority: std::sync::Arc::clone(&memory_authority),
            process_memory: Some(frontend_process_memory_observation()),
            #[cfg(feature = "mem-1-m07-hms-listing-observe")]
            hms_listing_observation: None,
        },
        serving: FrontendServingConfig {
            server_version: build.short_version.to_owned(),
            report_bind_host: config.server.host.clone(),
            report_grpc_port: config.server.grpc_port,
            frontend_drain_timeout: Duration::from_millis(config.server.frontend_drain_timeout_ms),
            frontend_cleanup_timeout: Duration::from_millis(
                config.server.frontend_cleanup_timeout_ms,
            ),
            mysql_listener,
            native_trust: std::sync::Arc::clone(native_trust.trust()),
            native_transport: frontend_native_transport(native_trust.transport()),
            native_ingress: runtime_config.native_ingress.to_adapter_config(),
        },
        mv_storage_observation: std::sync::Arc::new(IcebergMvStorageObservationAdapter::default()),
    })
}

/// Materializes the task protocol's budgets from one validated role config.
///
/// Deserialization has already rejected a zero or inverted bound, so each
/// neutral constructor here can only fail if that validation and these types
/// disagree, which is worth failing startup over rather than clamping.
#[allow(
    dead_code,
    reason = "Worker-owned bounds are validated with the complete startup budget before backend composition consumes them."
)]
#[derive(Clone, Copy, Debug)]
struct ComposedTaskExecutionBudgets {
    coordination: CoordinationBudgets,
    wait_caps: OperationWaitCaps,
    lease_bounds: LeaseBounds,
    transport: FrontendTaskTransportBudget,
}

fn compose_frontend_capacity_config(
    runtime: &crate::app_config::RuntimeConfig,
) -> anyhow::Result<FrontendWorkloadCapacityConfig> {
    let input = &runtime.frontend_workload;
    FrontendWorkloadCapacityConfig::try_new(
        input.scope_records_limit,
        NonZeroUsize::new(input.joint_progress_turn_budget)
            .ok_or_else(|| anyhow!("frontend responsibility turn budget must be nonzero"))?,
        Duration::from_millis(input.retired_backing_recheck_ms),
    )
    .context("construct frontend workload runtime configuration")
}

fn compose_frontend_workload_policy(
    runtime: &crate::app_config::RuntimeConfig,
) -> anyhow::Result<WorkloadConfig> {
    let input = &runtime.frontend_workload;
    let scope_limit = compose_frontend_capacity_config(runtime)?.scope_limit();
    let workload = WorkloadConfig {
        // These generic bookkeeping ceilings protect finite in-process state.
        // Frontend compute roots use `query_concurrency_limit` below as their
        // single operational queue; these do not form a second FE workload
        // admission policy.
        root_limit: scope_limit,
        business_limit: scope_limit,
        query_concurrency_limit: input.concurrency_limit,
        preparation_limit: scope_limit,
        execution_limit: scope_limit,
        executions_per_root: scope_limit,
        waiting_limit: input.waiting_limit,
        // Query admission has no retained-byte declaration. Keep the generic
        // stage-accounting invariant valid without exposing a second FE query
        // capacity knob.
        waiting_bytes: 1,
        capacity_wait_timeout: Duration::from_millis(input.capacity_wait_timeout_ms),
        restarts_per_work: input.restarts_per_work,
        old_attempts_per_work: input.old_attempts_per_work,
        old_attempts_limit: input.old_attempts_limit,
        unknown_creates_limit: input.unknown_creates_limit,
        scope_records_limit: scope_limit,
        obligation_records_limit: input.obligation_records_limit,
        control_inflight_limit: input.control_inflight_limit,
        control_ready_limit: input.control_ready_limit,
    };
    workload
        .validate()
        .context("construct frontend workload policy")?;
    Ok(workload)
}

fn compose_frontend_result_profile(
    runtime: &crate::app_config::RuntimeConfig,
) -> anyhow::Result<FrontendResultCapacityProfile> {
    let mut capacity = novarocks_workload_control::ResultCapacityConfig::V1;
    capacity.positions[2] = runtime.frontend_workload.internal_result_positions;
    capacity.all_objects_bytes[2] = runtime.frontend_workload.internal_result_all_objects_bytes;
    FrontendResultCapacityProfile::try_new(capacity).context("construct frontend result capacity")
}

/// Pure validation of the actual runtime configuration used by composition.
pub(crate) fn validate_frontend_joint_startup(
    runtime: &crate::app_config::RuntimeConfig,
) -> anyhow::Result<()> {
    let workload = compose_frontend_workload_policy(runtime)?;
    let result = compose_frontend_result_profile(runtime)?;
    if workload.query_concurrency_limit > result.declared().client_compute_positions {
        anyhow::bail!("frontend query concurrency exceeds its actual Client result positions");
    }
    Ok(())
}

fn compose_frontend_statement_ingress() -> anyhow::Result<FrontendStatementIngress> {
    let mysql = novarocks_mysql_adapter::frozen_mysql_startup_parameters()
        .context("read original frontend MySQL ingress bounds")?;
    Ok(FrontendStatementIngress {
        ordinary_connections: NonZeroUsize::new(mysql.ordinary_positions)
            .ok_or_else(|| anyhow!("ordinary MySQL positions must be nonzero"))?,
        control_connections: NonZeroUsize::new(mysql.control_positions)
            .ok_or_else(|| anyhow!("control MySQL positions must be nonzero"))?,
        ordinary_command_bytes: NonZeroUsize::new(mysql.ordinary.limits.command_bytes)
            .ok_or_else(|| anyhow!("ordinary MySQL command bound must be nonzero"))?,
        control_command_bytes: NonZeroUsize::new(mysql.control.limits.command_bytes)
            .ok_or_else(|| anyhow!("control MySQL command bound must be nonzero"))?,
    })
}

fn compose_frontend_workload_runtime(
    runtime: &crate::app_config::RuntimeConfig,
) -> anyhow::Result<(
    LogicalExecutionSupervisorConfig,
    WorkloadConfig,
    NonZeroUsize,
    FrontendResultCapacityProfile,
    FrontendNativeCapacityInputs,
)> {
    let input = &runtime.frontend_workload;
    let workload = compose_frontend_workload_policy(runtime)?;
    let result_profile = compose_frontend_result_profile(runtime)?;
    validate_frontend_joint_startup(runtime)?;
    let nonzero = |field: &'static str, value: usize| {
        NonZeroUsize::new(value)
            .ok_or_else(|| anyhow::anyhow!("runtime.frontend_workload.{field} must be nonzero"))
    };
    let max_attempts = input
        .restarts_per_work
        .checked_add(1)
        .and_then(|value| u32::try_from(value).ok())
        .and_then(std::num::NonZeroU32::new)
        .ok_or_else(|| {
            anyhow::anyhow!(
                "runtime.frontend_workload.restarts_per_work cannot form a nonzero u32 attempt bound"
            )
        })?;
    let abort_capacity = nonzero(
        "logical_abort_effect_capacity",
        input.logical_abort_effect_capacity,
    )?;
    if input.logical_replacement_reservation_ms == 0 {
        anyhow::bail!(
            "runtime.frontend_workload.logical_replacement_reservation_ms must be nonzero"
        );
    }
    if input.logical_remote_cleanup_timeout_ms == 0 {
        anyhow::bail!(
            "runtime.frontend_workload.logical_remote_cleanup_timeout_ms must be nonzero"
        );
    }
    if input.planning_idle_keepalive_ms == 0 {
        anyhow::bail!("runtime.frontend_workload.planning_idle_keepalive_ms must be nonzero");
    }
    let rows = LogicalExecutionRowsConfig::new(
        nonzero(
            "logical_rows_delivery_capacity",
            input.logical_rows_delivery_capacity,
        )?,
        max_attempts,
        Duration::from_millis(input.logical_replacement_reservation_ms),
    );
    let supervisor = LogicalExecutionSupervisorConfig::new(
        nonzero(
            "logical_actor_mailbox_capacity",
            input.logical_actor_mailbox_capacity,
        )?,
        nonzero(
            "logical_context_admission_issue_capacity",
            input.logical_context_admission_issue_capacity,
        )?,
        nonzero(
            "logical_context_establish_capacity",
            input.logical_context_establish_capacity,
        )?,
        rows,
    )
    .with_remote_cleanup_timeout(Duration::from_millis(
        input.logical_remote_cleanup_timeout_ms,
    ));
    let query_blocking = QueryBlockingExecutorConfig::new(
        NonZeroUsize::new(runtime.actual_query_blocking_workers())
            .ok_or_else(|| anyhow!("runtime.query_blocking_worker_threads must be nonzero"))?,
        NonZeroUsize::new(runtime.query_blocking_queue_capacity)
            .ok_or_else(|| anyhow!("runtime.query_blocking_queue_capacity must be nonzero"))?,
    );
    let capacity_inputs = FrontendNativeCapacityInputs::try_new(
        compose_frontend_capacity_config(runtime)?,
        query_blocking,
    )
    .context("construct unvalidated frontend Native capacity inputs")?;
    Ok((
        supervisor,
        workload,
        abort_capacity,
        result_profile,
        capacity_inputs,
    ))
}

fn compose_task_execution_budgets(
    config: &NovaRocksConfig,
) -> anyhow::Result<ComposedTaskExecutionBudgets> {
    let runtime = &config.runtime;
    let dispatch = DispatchBudget::new(
        runtime.task_dispatch_create_permits,
        runtime.task_dispatch_update_permits,
        runtime.task_dispatch_lifecycle_permits,
        runtime.task_dispatch_control_permits,
    )
    .ok_or_else(|| anyhow::anyhow!("construct task dispatch budget: permits must be nonzero"))?;
    let wait_caps = OperationWaitCaps::new(
        Duration::from_millis(runtime.task_operation_create_wait_cap_ms),
        Duration::from_millis(runtime.task_operation_update_wait_cap_ms),
    )
    .ok_or_else(|| {
        anyhow::anyhow!("construct task operation wait caps: both caps must be nonzero")
    })?;
    let lease_bounds = LeaseBounds::new(
        Duration::from_millis(runtime.task_lease_min_ms),
        Duration::from_millis(runtime.task_lease_max_ms),
    )
    .ok_or_else(|| {
        anyhow::anyhow!("construct task lease bounds: the accepted range must not be inverted")
    })?;
    let transport = FrontendTaskTransportBudget::try_new(
        runtime.task_operation_max_batch_items,
        runtime.task_operation_max_batch_encoded_bytes,
        runtime.task_descriptor_max_encoded_bytes,
        runtime.task_query_backend_max_queued_operations,
        runtime.task_query_backend_max_queued_bytes,
        runtime.task_backend_max_queued_operations,
        runtime.task_backend_max_queued_bytes,
        runtime.task_max_tasks_per_context,
        runtime.task_max_active_tasks_per_backend,
        Duration::from_millis(runtime.task_operation_queue_residence_ms),
    )
    .ok_or_else(|| {
        anyhow::anyhow!(
            "construct task transport budget: a descriptor must fit in a batch, a batch in one \
             query's queue, and the process must retain one maximum ordinary and control batch"
        )
    })?;
    Ok(ComposedTaskExecutionBudgets {
        coordination: CoordinationBudgets {
            dispatch,
            status_subscription_error_budget: runtime.task_status_subscription_error_budget,
        },
        wait_caps,
        lease_bounds,
        transport,
    })
}

fn backend_native_transport(
    transport: &NativeTrustTransport,
) -> novarocks_native_adapter::BackendNativeTransport {
    match transport {
        NativeTrustTransport::Plaintext => {
            novarocks_native_adapter::BackendNativeTransport::Plaintext
        }
        NativeTrustTransport::Automatic(material) => {
            novarocks_native_adapter::BackendNativeTransport::Automatic(material.clone())
        }
        NativeTrustTransport::Pem(material) => {
            novarocks_native_adapter::BackendNativeTransport::Pem(material.clone())
        }
    }
}

fn frontend_native_transport(
    transport: &NativeTrustTransport,
) -> novarocks_native_adapter::FrontendNativeTransport {
    match transport {
        NativeTrustTransport::Plaintext => {
            novarocks_native_adapter::FrontendNativeTransport::plaintext()
        }
        NativeTrustTransport::Automatic(material) => {
            novarocks_native_adapter::FrontendNativeTransport::automatic(material.clone())
        }
        NativeTrustTransport::Pem(material) => {
            novarocks_native_adapter::FrontendNativeTransport::pem(material.clone())
        }
    }
}

fn backend_execution_runtime_config(config: &NovaRocksConfig) -> ExecutionRuntimeConfig {
    let runtime = &config.runtime;
    ExecutionRuntimeConfig {
        driver_threads: runtime.actual_exec_threads(),
        exchange_wait_ms: runtime.exchange_wait_ms,
        exchange_io_threads: runtime.exchange_io_threads.max(1),
        exchange_io_max_inflight_bytes: runtime.exchange_io_max_inflight_bytes.max(1),
        exchange_max_transmit_batched_bytes: runtime.exchange_max_transmit_batched_bytes.max(1),
        operator_buffer_chunks: runtime.operator_buffer_chunks.max(1),
        local_exchange_buffer_mem_limit_per_driver: runtime
            .local_exchange_buffer_mem_limit_per_driver
            .max(1),
        local_exchange_max_buffered_rows: runtime.local_exchange_max_buffered_rows,
        runtime_filter_scan_wait_time_ms_override: runtime
            .runtime_filter_scan_wait_time_ms_override,
        runtime_filter_wait_timeout_ms_override: runtime.runtime_filter_wait_timeout_ms_override,
        sink_io_worker_threads: runtime.execution_services.actual_sink_io_worker_threads(),
        sink_io_max_blocking_threads: runtime
            .execution_services
            .sink_io_max_blocking_threads
            .max(1),
    }
}

pub fn compose_iceberg_execution_resources(
    config: &NovaRocksConfig,
    runtime: tokio::runtime::Handle,
    write_resources: FsAccessResources,
    scan_io: &ScanIoServices,
) -> anyhow::Result<IcebergExecutionResources> {
    // Read and write are I/O views of the same BE storage authority. Moving
    // reads onto scan I/O must not mint another credential registry or pool.
    let read_resources =
        write_resources.with_file_io(scan_io.file_runtime(), scan_io.file_task_spawner());
    let read_binding = compose_iceberg_access_template_from_resources(config, read_resources)?
        .with_range_service(scan_io.range_service());
    let write_binding = compose_iceberg_access_template_from_resources(config, write_resources)?;
    Ok(IcebergExecutionResources::new(
        read_binding,
        write_binding,
        novarocks_connector_iceberg::resources::IcebergCatalogRuntime::new(runtime),
    ))
}

fn compose_iceberg_access_template_from_resources(
    config: &NovaRocksConfig,
    resources: FsAccessResources,
) -> anyhow::Result<IcebergReadBinding> {
    let resolver: std::sync::Arc<
        dyn novarocks_connector_iceberg::access_binding::IcebergCredentialResolver,
    > = std::sync::Arc::new(
        config
            .connector
            .credential_registry(ClusterRole::Be)
            .map_err(|error| anyhow::anyhow!("resolve BE catalog credentials: {error}"))?,
    );
    Ok(IcebergReadBinding::with_credential_resolver(
        resources, resolver,
    ))
}

/// Build the FE-only Iceberg metadata access template. Its resolver selects
/// only `ObjectStoreMetadata`; execution-data bindings remain unavailable to
/// planning FileIO.
pub(crate) fn compose_iceberg_metadata_access_template(
    config: &NovaRocksConfig,
    resources: FsAccessResources,
) -> anyhow::Result<IcebergReadBinding> {
    let resolver: std::sync::Arc<
        dyn novarocks_connector_iceberg::access_binding::IcebergCredentialResolver,
    > = std::sync::Arc::new(
        config
            .connector
            .credential_registry(ClusterRole::Fe)
            .map_err(|error| anyhow::anyhow!("resolve FE metadata credentials: {error}"))?,
    );
    Ok(IcebergReadBinding::with_metadata_credential_resolver(
        resources, resolver,
    ))
}

/// The coordinator's Paimon file access: metadata reads on the process
/// runtime, outside the scan I/O services.
pub(crate) fn compose_paimon_control_access_factory(
    config: &NovaRocksConfig,
    resources: FsAccessResources,
) -> anyhow::Result<std::sync::Arc<ServerPaimonRoleFileIoFactory>> {
    Ok(std::sync::Arc::new(
        compose_paimon_access_factory_with_resources(config, ClusterRole::Fe, resources)?,
    ))
}

/// A backend's Paimon file access: the same scan I/O services as every other
/// provider's execution reads, so its HEADs and GETs share their windows.
pub(crate) fn compose_paimon_execution_access_factory(
    config: &NovaRocksConfig,
    resources: FsAccessResources,
    scan_io: &ScanIoServices,
) -> anyhow::Result<std::sync::Arc<ServerPaimonRoleFileIoFactory>> {
    let resources = resources.with_file_io(scan_io.file_runtime(), scan_io.file_task_spawner());
    Ok(std::sync::Arc::new(
        compose_paimon_access_factory_with_resources(config, ClusterRole::Be, resources)?
            .with_range_service(scan_io.range_service()),
    ))
}

pub(crate) fn compose_paimon_access_factory_with_resources(
    config: &NovaRocksConfig,
    role: ClusterRole,
    resources: FsAccessResources,
) -> anyhow::Result<ServerPaimonRoleFileIoFactory> {
    let credentials = config
        .connector
        .credential_registry(role)
        .map_err(|error| anyhow::anyhow!("resolve role-local catalog credentials: {error}"))?;
    Ok(ServerPaimonRoleFileIoFactory::new(resources, credentials))
}

pub fn compose_connector_file_planning_resources(
    _config: &NovaRocksConfig,
    runtime: tokio::runtime::Handle,
) -> anyhow::Result<FsAccessResources> {
    let pool = std::sync::Arc::new(
        ObjectStoreProviderPool::new(ObjectStoreProviderPoolOptions::default())
            .map_err(|error| anyhow::anyhow!("construct object-store provider pool: {error}"))?,
    );
    Ok(FsAccessResources::new(
        pool,
        FsAccessResolver::with_runtime(runtime.clone()),
        std::sync::Arc::new(TokioFileIoRuntime::new(runtime.clone())),
        std::sync::Arc::new(TokioFileTaskSpawner::new(runtime)),
    ))
}

pub fn state_store_input(config: &NovaRocksConfig) -> anyhow::Result<Option<StateStoreHostInput>> {
    let Some(state_store) = &config.state_store else {
        return Ok(None);
    };
    let limits = resolve_state_store_limits(&state_store.store.limits, MAX_KEY_BYTES)?;
    // Two independently validated groups from two owners: what the provider
    // enforces, and what this application does about failures.
    let run_policy = config.application.state_store_policy.resolve()?;
    Ok(Some(StateStoreHostInput {
        cluster_id: state_store.store.cluster_id.clone(),
        provider_id: SQLITE_STATE_STORE_PROVIDER_ID,
        limits,
        run_policy,
    }))
}

pub fn state_store_provider_registry(
    config: &NovaRocksConfig,
) -> anyhow::Result<StateStoreProviderRegistry> {
    let mut registry = StateStoreProviderRegistry::new();
    let Some(state_store) = &config.state_store else {
        return Ok(registry);
    };
    let contribution = SqliteStateStoreContribution::new(state_store.store.path.clone());
    let descriptor =
        StateStoreProviderDescriptor::new(SQLITE_STATE_STORE_PROVIDER_ID, MAX_KEY_BYTES);
    registry.register(StateStoreProviderRegistration::new(descriptor, move |_| {
        Ok(Box::new(contribution.clone().into_factory()))
    }))?;
    Ok(registry)
}

#[cfg(test)]
mod tests {
    use super::{
        FrontendWorkloadCapacityProfile, compose_frontend_statement_ingress,
        compose_frontend_workload_runtime, compose_task_execution_budgets,
    };

    use novarocks_execution_contract::{MaxWait, OperationKind};
    use novarocks_frontend_application::task_protocol_config::DispatchBudget;
    use novarocks_native_adapter::FrontendTaskTransportBudget;
    use novarocks_worker::LeaseBounds;
    use std::time::Duration;

    fn startup_isolation_config(
        evidence_file: &str,
        launch_nonce: &str,
    ) -> crate::app_config::NovaRocksConfig {
        let mut config = crate::app_config::NovaRocksConfig::default();
        config.mv_management.startup_isolation = Some(crate::app_config::StartupIsolationConfig {
            evidence_file: evidence_file.to_string(),
            launch_nonce: launch_nonce.to_string(),
        });
        config
    }

    #[test]
    fn a_deployment_without_a_startup_statement_reads_none() {
        let config = crate::app_config::NovaRocksConfig::default();

        assert!(
            super::startup_isolation(&config)
                .expect("no statement is the default")
                .is_none()
        );
    }

    #[test]
    fn a_relative_evidence_path_is_refused() {
        let error = super::startup_isolation(&startup_isolation_config(
            "mv-startup-isolation.toml",
            "launch-1",
        ))
        .expect_err("the same configuration must not mean different files");

        assert!(error.to_string().contains("absolute path"), "{error}");
    }

    #[test]
    fn a_launch_nonce_that_is_not_a_token_is_refused() {
        let error = super::startup_isolation(&startup_isolation_config(
            "/var/lib/novarocks/mv-startup-isolation.toml",
            "two tokens",
        ))
        .expect_err("a nonce is one token");

        assert!(error.to_string().contains("launch_nonce"), "{error}");
    }

    #[test]
    fn a_complete_startup_statement_source_is_accepted() {
        let source = super::startup_isolation(&startup_isolation_config(
            "/var/lib/novarocks/mv-startup-isolation.toml",
            "launch-7f3c",
        ))
        .expect("a complete configuration")
        .expect("a configured source");

        assert_eq!(
            source.path(),
            std::path::Path::new("/var/lib/novarocks/mv-startup-isolation.toml")
        );
        assert_eq!(source.launch_nonce().as_str(), "launch-7f3c");
    }

    #[test]
    fn a_configured_budget_may_tighten_the_bounds_but_not_invert_them() {
        // The bounds are a hierarchy, not ten unrelated numbers. Composition
        // is where a deployment's numbers become the type that enforces them,
        // so a configuration that could never send anything must fail here
        // rather than at the first oversized descriptor.
        let mut config = crate::app_config::NovaRocksConfig::default();
        config.runtime.task_operation_max_batch_items = 4;
        config.runtime.task_operation_max_batch_encoded_bytes = 1 << 20;
        config.runtime.task_descriptor_max_encoded_bytes = 1 << 19;
        let budgets = compose_task_execution_budgets(&config).expect("a tightened budget composes");
        assert_eq!(budgets.transport.max_batch_items(), 4);
        assert_eq!(budgets.transport.max_descriptor_encoded_bytes(), 1 << 19);

        // A descriptor larger than a batch could never be sent at all.
        config.runtime.task_descriptor_max_encoded_bytes = (1 << 20) + 1;
        let error = compose_task_execution_budgets(&config)
            .expect_err("an inverted hierarchy is refused")
            .to_string();
        assert!(
            error.contains("task transport budget"),
            "the refusal must name the budget it rejected, got: {error}"
        );

        let mut zeroed = crate::app_config::NovaRocksConfig::default();
        zeroed.runtime.task_operation_max_batch_items = 0;
        assert!(
            compose_task_execution_budgets(&zeroed).is_err(),
            "a zero bound is not a disabled bound"
        );

        let mut byte_starved = crate::app_config::NovaRocksConfig::default();
        let batch_bytes = byte_starved.runtime.task_operation_max_batch_encoded_bytes;
        byte_starved.runtime.task_query_backend_max_queued_bytes = batch_bytes;
        byte_starved.runtime.task_backend_max_queued_bytes = batch_bytes * 4 - 1;
        assert!(
            compose_task_execution_budgets(&byte_starved).is_err(),
            "the process byte bound must retain queued and encoded forms of ordinary and control batches"
        );

        let mut item_starved = crate::app_config::NovaRocksConfig::default();
        let batch_items = item_starved.runtime.task_operation_max_batch_items;
        item_starved
            .runtime
            .task_query_backend_max_queued_operations = batch_items;
        item_starved.runtime.task_backend_max_queued_operations = batch_items * 2 - 1;
        assert!(
            compose_task_execution_budgets(&item_starved).is_err(),
            "the process item bound must retain one ordinary and one control batch"
        );
    }

    #[test]
    fn task_execution_budgets_use_frozen_wire_and_server_queue_policy() {
        let config = crate::app_config::NovaRocksConfig::default();
        let budgets = compose_task_execution_budgets(&config).expect("default budgets");

        assert_eq!(budgets.coordination.dispatch, DispatchBudget::DEFAULT);
        assert_eq!(budgets.lease_bounds, LeaseBounds::DEFAULT);
        let frozen = FrontendTaskTransportBudget::DEFAULT;
        assert_eq!(
            budgets.transport.max_batch_items(),
            frozen.max_batch_items()
        );
        assert_eq!(
            budgets.transport.max_batch_encoded_bytes(),
            frozen.max_batch_encoded_bytes()
        );
        assert_eq!(
            budgets.transport.max_descriptor_encoded_bytes(),
            frozen.max_descriptor_encoded_bytes()
        );
        assert_eq!(
            budgets.transport.max_query_backend_queued_operations(),
            frozen.max_query_backend_queued_operations()
        );
        assert_eq!(
            budgets.transport.max_query_backend_queued_bytes(),
            frozen.max_query_backend_queued_bytes()
        );
        assert_eq!(
            budgets.transport.max_backend_queued_operations(),
            frozen.max_backend_queued_operations()
        );
        assert_eq!(
            budgets.transport.max_backend_queued_bytes(),
            config.runtime.task_backend_max_queued_bytes
        );
        assert_eq!(
            budgets.transport.max_tasks_per_context(),
            frozen.max_tasks_per_context()
        );
        assert_eq!(
            budgets.transport.max_active_tasks_per_backend(),
            frozen.max_active_tasks_per_backend()
        );
        assert_eq!(
            budgets.transport.frontend_queue_residence(),
            frozen.frontend_queue_residence()
        );
        // `OperationWaitCaps` has no accessors, so the caps are compared
        // through the clamp they exist for.
        assert_eq!(
            budgets.wait_caps.clamp(
                OperationKind::CreateTask,
                MaxWait::new(Duration::from_secs(300)).expect("representable"),
            ),
            MaxWait::DEFAULT_CREATE
        );
        assert_eq!(
            budgets.wait_caps.clamp(
                OperationKind::UpdateTask,
                MaxWait::new(Duration::from_secs(300)).expect("representable"),
            ),
            MaxWait::DEFAULT_UPDATE
        );
        assert!(budgets.coordination.status_subscription_error_budget > 0);
    }

    #[test]
    fn frontend_configuration_with_one_worker_does_not_derive_a_topology_limit() {
        let mut config = crate::app_config::NovaRocksConfig::default();
        config.runtime.query_blocking_worker_threads = 1;
        config.runtime.query_blocking_queue_capacity = 1;
        let (_, workload, _, result, inputs) =
            compose_frontend_workload_runtime(&config.runtime).unwrap();
        let transport = compose_task_execution_budgets(&config).unwrap().transport;
        let profile = FrontendWorkloadCapacityProfile::try_for_native(
            inputs,
            &workload,
            result,
            transport,
            compose_frontend_statement_ingress().unwrap(),
        )
        .unwrap();
        assert_eq!(
            profile
                .inputs()
                .query_blocking_capacity()
                .worker_threads()
                .get(),
            1
        );
        assert_eq!(profile.policy().scope_limit(), 8192);
        assert_eq!(transport.max_backend_queued_bytes(), 512 * 1024 * 1024);
        assert_eq!(
            profile.statement_ingress(),
            Some(compose_frontend_statement_ingress().unwrap())
        );
    }

    #[test]
    fn frontend_workload_runtime_is_built_from_validated_server_fields() {
        let mut config = crate::app_config::NovaRocksConfig::default();
        config.runtime.mem_limit = "1G".to_string();
        let (_, workload, abort_capacity, _, inputs) =
            compose_frontend_workload_runtime(&config.runtime)
                .expect("explicit frontend workload fields compose");
        assert_eq!(workload.query_concurrency_limit, 256);
        assert_eq!(workload.execution_limit, workload.scope_records_limit);
        assert_eq!(abort_capacity.get(), 16);
        assert_eq!(inputs.policy().scope_limit(), 8192);
        assert_eq!(inputs.policy().scope_limit(), workload.scope_records_limit);
        assert_eq!(
            inputs.query_blocking_capacity().worker_threads().get(),
            config.runtime.actual_query_blocking_workers()
        );

        config
            .runtime
            .frontend_workload
            .logical_replacement_reservation_ms = 0;
        assert!(
            compose_frontend_workload_runtime(&config.runtime)
                .expect_err("zero replacement reservation must fail preflight")
                .to_string()
                .contains("logical_replacement_reservation_ms")
        );

        config
            .runtime
            .frontend_workload
            .logical_replacement_reservation_ms = 30_000;
        config
            .runtime
            .frontend_workload
            .logical_abort_effect_capacity = 0;
        assert!(
            compose_frontend_workload_runtime(&config.runtime)
                .expect_err("zero logical Abort effect capacity must fail preflight")
                .to_string()
                .contains("logical_abort_effect_capacity")
        );

        config
            .runtime
            .frontend_workload
            .logical_abort_effect_capacity = 16;
        // FE admission no longer derives an allocation budget from mem_limit.
        config.runtime.mem_limit = "64M".to_string();
        assert!(compose_frontend_workload_runtime(&config.runtime).is_ok());
    }
}

/// Turn this deployment's MV management claims into the domain values the
/// frontend takes.
///
/// Both halves refuse rather than degrade. A guarantee whose basis does not
/// establish a remote lifetime is a configuration error, not a weaker
/// guarantee, and an audit path that cannot be written is a configuration
/// error, not a reason to act unrecorded.
fn mv_management_continuation(
    config: &NovaRocksConfig,
) -> anyhow::Result<MvManagementContinuation> {
    let policy = novarocks_mv_application::management::RemoteEffectPolicy::try_new(
        remote_effect_guarantee(
            config.mv_management.catalog_commit_guarantee.as_ref(),
            novarocks_mv_application::management::EffectScope::CATALOG_COMMIT,
            "catalog_commit_guarantee",
        )?,
        remote_effect_guarantee(
            config.mv_management.object_deletion_guarantee.as_ref(),
            novarocks_mv_application::management::EffectScope::OBJECT_DELETION,
            "object_deletion_guarantee",
        )?,
    )
    .map_err(|error| anyhow!("InvalidMvManagementConfig: {error}"))?;
    let audit = match config.mv_management.audit_log.as_deref() {
        None => None,
        Some(path) => {
            let path = std::path::Path::new(path);
            let path = if path.is_absolute() {
                path.to_path_buf()
            } else {
                std::path::Path::new(&config.sys_log_dir).join(path)
            };
            let sink = novarocks_frontend_application::FileManagementAuditSink::open(path)
                .map_err(|error| anyhow!("InvalidMvManagementConfig: {error}"))?;
            Some(std::sync::Arc::new(sink)
                as std::sync::Arc<
                    dyn novarocks_mv_application::management::ManagementAuditSink,
                >)
        }
    };
    Ok((policy, audit, startup_isolation(config)?))
}

/// Where this deployment's startup isolation statement is read from.
///
/// The path is required to be absolute: a statement is deployment state
/// written by whatever stopped the previous process, and resolving it against
/// a directory this process happens to run in would make the same
/// configuration mean different files.
fn startup_isolation(
    config: &NovaRocksConfig,
) -> anyhow::Result<Option<novarocks_frontend_application::StartupIsolationSource>> {
    let Some(startup) = config.mv_management.startup_isolation.as_ref() else {
        return Ok(None);
    };
    let path = std::path::PathBuf::from(&startup.evidence_file);
    if !path.is_absolute() {
        return Err(anyhow!(
            "InvalidMvManagementConfig: [mv_management.startup_isolation].evidence_file must be \
             an absolute path"
        ));
    }
    let nonce = novarocks_mv_application::management::StartupNonce::parse(&startup.launch_nonce)
        .map_err(|error| {
            anyhow!(
                "InvalidMvManagementConfig: [mv_management.startup_isolation].launch_nonce: \
                 {error}"
            )
        })?;
    Ok(Some(
        novarocks_frontend_application::StartupIsolationSource::new(path, nonce),
    ))
}

fn remote_effect_guarantee(
    config: Option<&crate::app_config::RemoteEffectGuaranteeConfig>,
    scope: novarocks_mv_application::management::EffectScope,
    field: &str,
) -> anyhow::Result<Option<novarocks_mv_application::management::RemoteEffectLifetimeGuarantee>> {
    use novarocks_mv_application::management::RemoteEffectGuaranteeBasis;

    let Some(config) = config else {
        return Ok(None);
    };
    let basis = match config.basis.as_str() {
        "provider-service-contract" => RemoteEffectGuaranteeBasis::ProviderServiceContract,
        "deployment-enforced-bound" => RemoteEffectGuaranteeBasis::DeploymentEnforcedBound,
        other => {
            return Err(anyhow!(
                "InvalidMvManagementConfig: [mv_management].{field}.basis `{other}` does not \
                 establish a remote effect lifetime; only `provider-service-contract` and \
                 `deployment-enforced-bound` do"
            ));
        }
    };
    novarocks_mv_application::management::RemoteEffectLifetimeGuarantee::try_new(
        scope,
        Duration::from_millis(config.lifetime_ms),
        Duration::from_millis(config.safety_margin_ms),
        basis,
        &config.source,
    )
    .map(Some)
    .map_err(|error| anyhow!("InvalidMvManagementConfig: [mv_management].{field}: {error}"))
}

#[cfg(test)]
mod frontend_runtime_policy_tests {
    use super::*;
    #[test]
    fn configured_internal_capacity_reaches_the_actual_runtime_projection() {
        let mut runtime = crate::app_config::RuntimeConfig::default();
        runtime.frontend_workload.internal_result_positions = 1;
        runtime.frontend_workload.internal_result_all_objects_bytes = 2 * 1024 * 1024 * 1024;
        validate_frontend_joint_startup(&runtime).unwrap();
        let (_, _, _, installed, _) = compose_frontend_workload_runtime(&runtime).unwrap();
        assert_eq!(installed.declared().positions[2], 1);
        assert_eq!(
            installed.declared().all_objects_bytes[2],
            2 * 1024 * 1024 * 1024
        );
        runtime.frontend_workload.internal_result_positions = 0;
        assert!(validate_frontend_joint_startup(&runtime).is_err());
        assert!(compose_frontend_workload_runtime(&runtime).is_err());
    }
    #[test]
    fn original_invalid_workload_configuration_preserves_the_owner_error() {
        let mut runtime = crate::app_config::RuntimeConfig::default();
        runtime.frontend_workload.waiting_limit = 0;
        let error = validate_frontend_joint_startup(&runtime).unwrap_err();
        assert!(
            error
                .downcast_ref::<novarocks_workload_control::WorkError>()
                .is_some()
        );
    }
    #[test]
    fn actual_client_result_positions_still_bound_query_concurrency() {
        let mut runtime = crate::app_config::RuntimeConfig::default();
        runtime.frontend_workload.concurrency_limit = 257;
        assert!(validate_frontend_joint_startup(&runtime).is_err());
        assert!(compose_frontend_workload_runtime(&runtime).is_err());
    }
    #[test]
    fn global_scope_inventory_reaches_the_original_workload() {
        let mut runtime = crate::app_config::RuntimeConfig::default();
        runtime.frontend_workload.scope_records_limit = 5000;
        let (_, workload, _, _, inputs) = compose_frontend_workload_runtime(&runtime).unwrap();
        assert_eq!(workload.scope_records_limit, 5000);
        assert_eq!(workload.root_limit, 5000);
        assert_eq!(inputs.policy().scope_limit(), 5000);
        runtime.frontend_workload.scope_records_limit = 0;
        assert!(compose_frontend_workload_runtime(&runtime).is_err());
    }
}
