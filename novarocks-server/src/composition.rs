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

use std::num::NonZeroUsize;
use std::time::Duration;

use crate::app_config::NovaRocksConfig;
use crate::native_trust::{NativeTrustSnapshot, NativeTrustTransport};
use crate::state_store_config::SQLITE_STATE_STORE_PROVIDER_ID;
use crate::state_store_limits::resolve_state_store_limits;
use novarocks_backend::{BackendServerConfig, QueryLifecycleRegistryConfig};
use novarocks_connector_iceberg::access_binding::IcebergReadBinding;
use novarocks_connector_iceberg::connector_factory::IcebergConnectorFactory;
use novarocks_connector_iceberg::file_reader::execution_installer::IcebergConnectorInstaller;
use novarocks_connector_iceberg::resources::{IcebergExecutionResources, IcebergMetadataResources};
use novarocks_connector_iceberg::storage_inspector::{
    IcebergStorageInspector, IcebergStorageLakePublication,
    IcebergStorageLakeTargetSnapshotObservation, IcebergStoragePartitionTransform,
    IcebergStorageRefreshTechnique,
};
use novarocks_connector_starrocks::StarRocksExecutionInstaller;
use novarocks_execution::runtime::execution_runtime::{
    ExecutionRuntimeConfig, ExecutionSpillStorageConfig,
};
use novarocks_frontend::{
    ClusterBackendOpenConfig, FrontendExecutionConfig, FrontendQueryControlTimeouts,
    FrontendServerConfig, LakePublicationRuntimePolicy,
    state_store::{
        StateStoreHostInput, StateStoreProviderRegistration, StateStoreProviderRegistry,
    },
};
use novarocks_fs::{FsAccessResolver, FsAccessResources, TokioFileIoRuntime, TokioFileTaskSpawner};
use novarocks_spi::connector::{
    ConnectorControlFactory, ConnectorControlPlanningLease, ConnectorError, ConnectorErrorKind,
    ConnectorExecutionInstaller, ConnectorExecutionProviderKind, ConnectorRequestContext,
    ConnectorTableMetadata, MvCreatedTargetObservation, MvLakeDescriptorProjection,
    MvLakePackageObservation, MvLakePublicationObservation, MvLakeTargetSnapshotObservation,
    MvMaintenanceMetadataObservation, MvObservedField, MvObservedMaintenancePolicy,
    MvObservedPartitionField, MvObservedPartitionSpec, MvObservedPartitionTransform,
    MvObservedRefreshMarker, MvObservedSnapshot, MvPublishedBaseObservation,
    MvPublishedRefreshObservation, MvPublishedRefreshTechnique, MvRefreshBaseObservation,
    MvRefreshTargetObservation, MvSchemaValidationObservation, MvStorageObservationPort,
    WriteCommitEvidenceLimits,
};
use novarocks_spi::state_store::{MAX_KEY_BYTES, StateStoreProviderDescriptor};
use novarocks_state_store_sqlite::SqliteStateStoreContribution;

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

/// Preserve the exact snapshot identity carried by the provider package
/// observation. Publication consistency is frontend policy; this adapter only
/// translates the sealed provider value without loading metadata again.
fn mv_lake_target_snapshot_observation(
    observed: Option<IcebergStorageLakeTargetSnapshotObservation>,
) -> Result<Option<MvLakeTargetSnapshotObservation>, ConnectorError> {
    observed
        .map(|snapshot| {
            MvLakeTargetSnapshotObservation::try_new(snapshot.snapshot_id, snapshot.timestamp_ms)
        })
        .transpose()
}

impl MvStorageObservationPort for IcebergMvStorageObservationAdapter {
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
        MvSchemaValidationObservation::try_new(
            observed.table_uuid,
            observed.schema_id,
            observed.format_v3,
            observed.explicit_row_lineage_enabled,
            fields,
            partition,
            &context,
        )
    }

    fn observe_lake_package(
        &self,
        exact_lease: &ConnectorControlPlanningLease,
        metadata: &ConnectorTableMetadata,
        context: ConnectorRequestContext,
    ) -> Result<Option<MvLakePackageObservation>, ConnectorError> {
        let Some(observed) =
            self.inspector
                .observe_lake_package(exact_lease, metadata, context.clone())?
        else {
            return Ok(None);
        };
        let package_id = observed
            .descriptor_properties
            .get("novarocks.mv.descriptor.package-id")
            .cloned()
            .ok_or_else(|| {
                ConnectorError::new(
                    ConnectorErrorKind::CorruptData,
                    "Iceberg MV package is missing its descriptor package ID",
                )
            })?;
        let inline_descriptor = observed
            .descriptor_properties
            .get("novarocks.mv.descriptor.inline")
            .cloned()
            .ok_or_else(|| {
                ConnectorError::new(
                    ConnectorErrorKind::CorruptData,
                    "Iceberg MV package is missing its inline descriptor property",
                )
            })?;
        let descriptor = MvLakeDescriptorProjection::try_new(
            package_id,
            inline_descriptor,
            observed
                .descriptor_properties
                .get("novarocks.mv.descriptor.hash")
                .cloned(),
            &context,
        )?;
        let current_target_snapshot =
            mv_lake_target_snapshot_observation(observed.current_target_snapshot)?;
        let publication = match observed.publication {
            IcebergStorageLakePublication::NeverPublished => {
                MvLakePublicationObservation::NeverPublished
            }
            IcebergStorageLakePublication::Published(facts) => {
                let technique = match facts.technique {
                    IcebergStorageRefreshTechnique::Incremental => {
                        MvPublishedRefreshTechnique::Incremental
                    }
                    IcebergStorageRefreshTechnique::Full => MvPublishedRefreshTechnique::Full,
                    IcebergStorageRefreshTechnique::MetadataOnly => {
                        MvPublishedRefreshTechnique::MetadataOnly
                    }
                };
                let bases = facts
                    .bases
                    .into_iter()
                    .map(|base| MvPublishedBaseObservation {
                        table_fqn: base.table_fqn,
                        object_id: base.object_id,
                        from_snapshot: base.from_snapshot,
                        to_snapshot: base.to_snapshot,
                    })
                    .collect();
                MvLakePublicationObservation::Published(MvPublishedRefreshObservation::try_new(
                    facts.target_snapshot_id,
                    facts.publication_id,
                    technique,
                    bases,
                    facts.definition_fingerprint,
                    facts.rows,
                    facts.provenance_hash,
                    facts.waterline_hash,
                    &context,
                )?)
            }
        };
        MvLakePackageObservation::try_new(
            metadata.identity.clone(),
            observed.target_object_id,
            descriptor,
            current_target_snapshot,
            publication,
        )
        .map(Some)
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
            observed.current_snapshot_is_empty_bootstrap,
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

pub fn compose_backend_execution_installers(
    config: &NovaRocksConfig,
    runtime: tokio::runtime::Handle,
) -> anyhow::Result<Vec<std::sync::Arc<dyn ConnectorExecutionInstaller>>> {
    let iceberg_resources = compose_iceberg_execution_resources(config, runtime)?;
    let iceberg_installers: Vec<std::sync::Arc<dyn ConnectorExecutionInstaller>> =
        vec![std::sync::Arc::new(IcebergConnectorInstaller::new(
            iceberg_resources,
        ))];
    let expected = ConnectorExecutionProviderKind::Iceberg;
    let mut installers: Vec<std::sync::Arc<dyn ConnectorExecutionInstaller>> =
        vec![std::sync::Arc::new(StarRocksExecutionInstaller::new())];
    for installer in &iceberg_installers {
        if installer.provider_kind() != expected {
            anyhow::bail!(
                "composed connector execution installer has kind {:?}; expected {:?}",
                installer.provider_kind(),
                expected
            );
        }
    }
    installers.extend(iceberg_installers);
    Ok(installers)
}

/// Resolve the BE-owned startup facts from the application wire configuration.
///
/// This is intentionally the only Server-to-Backend projection: Backend
/// receives no root configuration and therefore cannot observe unrelated
/// Frontend, StateStore, or connector wire sections.
pub fn compose_backend_server_config(
    config: &NovaRocksConfig,
    native_trust: &NativeTrustSnapshot,
    runtime: tokio::runtime::Handle,
) -> anyhow::Result<BackendServerConfig> {
    let runtime_config = &config.runtime;
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
    Ok(BackendServerConfig {
        bind_host: config.server.host.clone(),
        grpc_port: config.server.grpc_port,
        metrics_http_port: config.server.http_port,
        advertise_endpoint,
        native_trust: std::sync::Arc::clone(native_trust.trust()),
        native_transport: backend_native_transport(native_trust.transport()),
        frontend_endpoint,
        announce_interval: Duration::from_millis(config.cluster.backend_announce_interval_ms()),
        announce_initial_backoff: Duration::from_millis(
            config.cluster.backend_announce_initial_backoff_ms(),
        ),
        announce_max_backoff: Duration::from_millis(
            config.cluster.backend_announce_max_backoff_ms(),
        ),
        query_lifecycle_sweep_interval: Duration::from_millis(
            runtime_config.query_control_heartbeat_interval_ms,
        ),
        query_lifecycle_config: QueryLifecycleRegistryConfig::new(
            runtime_config.query_control_max_active_entries,
            runtime_config.query_control_tombstone_capacity,
            Duration::from_millis(runtime_config.query_control_tombstone_retention_ms),
            Duration::from_millis(runtime_config.query_control_heartbeat_timeout_ms),
            Duration::from_millis(runtime_config.query_control_pre_start_timeout_ms),
            runtime_config.query_control_stage_max_fragments,
            runtime_config.query_control_max_active_staging,
            runtime_config.query_control_stage_max_encoded_bytes,
            runtime_config.query_control_stage_max_inflight_encoded_bytes,
            runtime_config.query_control_stage_max_dormant_workers,
            runtime_config.query_control_terminal_max_encoded_bytes,
            Duration::from_millis(runtime_config.query_control_terminal_drain_timeout_ms),
            Duration::from_millis(runtime_config.query_control_terminal_ack_timeout_ms),
            Duration::from_millis(runtime_config.query_control_terminal_fallback_rpc_timeout_ms),
            runtime_config.query_control_terminal_fallback_max_attempts,
            Duration::from_millis(
                runtime_config.query_control_terminal_fallback_initial_backoff_ms,
            ),
            Duration::from_millis(runtime_config.query_control_terminal_fallback_max_backoff_ms),
            Duration::from_millis(runtime_config.query_control_terminal_retention_ms),
            runtime_config.query_control_terminal_retained_capacity,
            runtime_config.query_control_terminal_max_retained_bytes,
        ),
        write_commit_evidence_limits: WriteCommitEvidenceLimits::try_new(
            runtime_config.write_commit_evidence_max_bytes,
            runtime_config.write_commit_evidence_max_entries,
        )
        .map_err(|error| anyhow::anyhow!("resolve write commit evidence limits: {error}"))?,
        execution_runtime_config: backend_execution_runtime_config(config),
        execution_installers: compose_backend_execution_installers(config, runtime.clone())?,
        read_execution_bundle_factories: compose_backend_read_execution_bundle_factories(
            config, runtime,
        )?,
    })
}

/// Resolve every Frontend startup input from the application wire configuration.
pub fn compose_frontend_server_config(
    config: &NovaRocksConfig,
    native_trust: &NativeTrustSnapshot,
    port_override: Option<u16>,
    runtime: tokio::runtime::Handle,
) -> anyhow::Result<FrontendServerConfig> {
    let runtime_config = &config.runtime;
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
    let mut execution = FrontendExecutionConfig::new(
        native_trust.advertised_endpoint().host().to_string(),
        native_trust.advertised_endpoint().port(),
        runtime_filter_worker_count,
    )
    .with_catalog_desired_state_source(catalog_source)
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
    .with_query_control_timeouts(FrontendQueryControlTimeouts {
        heartbeat_interval_ms: runtime_config.query_control_heartbeat_interval_ms,
        heartbeat_timeout_ms: runtime_config.query_control_heartbeat_timeout_ms,
        init_rpc_timeout_ms: runtime_config.query_control_init_rpc_timeout_ms,
        attach_timeout_ms: runtime_config.query_control_attach_timeout_ms,
        stage_rpc_timeout_ms: runtime_config.query_control_stage_rpc_timeout_ms,
        start_rpc_timeout_ms: runtime_config.query_control_start_rpc_timeout_ms,
        terminal_drain_timeout_ms: runtime_config.query_control_terminal_drain_timeout_ms,
        terminal_ack_timeout_ms: runtime_config.query_control_terminal_ack_timeout_ms,
        pre_start_timeout_ms: runtime_config.query_control_pre_start_timeout_ms,
    });
    if let Some(standalone) = config.standalone_server.as_ref() {
        let failure_backoff_ms = failure_backoff_ms.expect("standalone config supplies backoff");
        execution =
            execution.with_mv_scheduler_config(novarocks_frontend::FrontendMvSchedulerConfig::new(
                standalone.mv_refresh_scheduler_enabled,
                standalone.mv_refresh_scheduler_interval_ms.max(1),
                standalone.mv_refresh_scheduler_max_concurrent.max(1),
                failure_backoff_ms,
                standalone
                    .mv_refresh_scheduler_max_failure_backoff_ms
                    .max(failure_backoff_ms),
            ));
        execution = execution.with_mv_maintenance_config(
            novarocks_frontend::MaintenanceCoordinatorConfig::new(
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
            ),
        );
    }
    let backend_open = ClusterBackendOpenConfig::new(
        config.cluster.role,
        Duration::from_millis(config.cluster.heartbeat_interval_ms()),
        config.cluster.heartbeat_timeout_retries(),
        Duration::from_millis(config.cluster.backend_announce_lease_ttl_ms()),
    )
    .map_err(|error| anyhow::anyhow!("open frontend backend cluster configuration: {error}"))?;
    let mysql_listener = novarocks_frontend::resolve_mysql_listener_settings(
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
    // One registry: the control factory installs into it and the planner
    // resolves from it, so a generation is either reachable to both or neither.
    let typed_connector_control =
        std::sync::Arc::new(novarocks_frontend::ConnectorReadControlRegistry::new());
    Ok(FrontendServerConfig {
        execution,
        backend_open,
        report_bind_host: config.server.host.clone(),
        report_grpc_port: config.server.grpc_port,
        metrics_http_port: config.server.http_port,
        frontend_drain_timeout: Duration::from_millis(config.server.frontend_drain_timeout_ms),
        frontend_cleanup_timeout: Duration::from_millis(config.server.frontend_cleanup_timeout_ms),
        mysql_listener,
        connector_control_factories: compose_frontend_control_factories(
            config,
            runtime,
            std::sync::Arc::clone(&typed_connector_control),
        )?,
        typed_connector_control,
        mv_storage_observation: std::sync::Arc::new(IcebergMvStorageObservationAdapter::default()),
        state_store_input,
        state_store_provider_registry,
        native_trust: std::sync::Arc::clone(native_trust.trust()),
        native_transport: frontend_native_transport(native_trust.transport()),
    })
}

fn backend_native_transport(
    transport: &NativeTrustTransport,
) -> novarocks_backend::BackendNativeTransport {
    match transport {
        NativeTrustTransport::Plaintext => novarocks_backend::BackendNativeTransport::Plaintext,
        NativeTrustTransport::Automatic(material) => {
            novarocks_backend::BackendNativeTransport::Automatic(material.clone())
        }
        NativeTrustTransport::Pem(material) => {
            novarocks_backend::BackendNativeTransport::Pem(material.clone())
        }
    }
}

fn frontend_native_transport(
    transport: &NativeTrustTransport,
) -> novarocks_frontend::FrontendNativeTransport {
    match transport {
        NativeTrustTransport::Plaintext => novarocks_frontend::FrontendNativeTransport::plaintext(),
        NativeTrustTransport::Automatic(material) => {
            novarocks_frontend::FrontendNativeTransport::automatic(material.clone())
        }
        NativeTrustTransport::Pem(material) => {
            novarocks_frontend::FrontendNativeTransport::pem(material.clone())
        }
    }
}

fn backend_execution_runtime_config(config: &NovaRocksConfig) -> ExecutionRuntimeConfig {
    let runtime = &config.runtime;
    let spill_io_threads = if runtime.spill_io_threads == 0 {
        runtime.actual_exec_threads()
    } else {
        runtime.spill_io_threads
    };
    ExecutionRuntimeConfig {
        driver_threads: runtime.actual_exec_threads(),
        scan_threads: runtime.actual_scan_threads(),
        scan_queue_capacity: runtime.pipeline_scan_thread_pool_queue_size.max(1),
        spill_io_threads,
        spill_io_queue_capacity: runtime.spill_io_queue_size.max(1),
        spill_storage: ExecutionSpillStorageConfig {
            enabled: config.spill.enable,
            local_dirs: if config.spill.local_dirs.is_empty() {
                vec![
                    std::env::temp_dir()
                        .join("novarocks-spill")
                        .to_string_lossy()
                        .into_owned(),
                ]
            } else {
                config.spill.local_dirs.clone()
            },
            dir_max_bytes: config.spill.dir_max_bytes,
            block_size_bytes: config.spill.block_size_bytes.max(1),
            ipc_compression: config.spill.ipc_compression.clone(),
        },
        exchange_wait_ms: runtime.exchange_wait_ms,
        exchange_io_threads: runtime.exchange_io_threads.max(1),
        exchange_io_max_inflight_bytes: runtime.exchange_io_max_inflight_bytes.max(1),
        exchange_max_transmit_batched_bytes: runtime.exchange_max_transmit_batched_bytes.max(1),
        operator_buffer_chunks: runtime.operator_buffer_chunks.max(1),
        local_exchange_buffer_mem_limit_per_driver: runtime
            .local_exchange_buffer_mem_limit_per_driver
            .max(1),
        local_exchange_max_buffered_rows: runtime.local_exchange_max_buffered_rows,
        connector_io_tasks_per_scan_operator: runtime.connector_io_tasks_per_scan_operator.max(1),
        scan_submit_fail_max: runtime.scan_submit_fail_max.max(1),
        scan_submit_fail_timeout_ms: runtime.scan_submit_fail_timeout_ms.max(1),
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

pub fn compose_frontend_control_factories(
    config: &NovaRocksConfig,
    runtime: tokio::runtime::Handle,
    typed_control: std::sync::Arc<novarocks_frontend::ConnectorReadControlRegistry>,
) -> anyhow::Result<Vec<std::sync::Arc<dyn ConnectorControlFactory>>> {
    let planning_resources = compose_connector_file_planning_resources(config, runtime.clone())?;
    // The installer runs only after the provider has built and validated its
    // complete control binding.  A registration failure therefore prevents a
    // binding from escaping without its matching read services and codec.
    let sink = std::sync::Arc::clone(&typed_control);
    let factory = IcebergConnectorFactory::new(IcebergMetadataResources::new(
        IcebergReadBinding::from_resources(planning_resources),
        runtime,
    ))
    .with_read_control_installer(std::sync::Arc::new(move |key, metadata, splits, codec| {
        sink.install_read_control(
            key,
            novarocks_frontend::connector::typed_control_registry::InstalledReadControl::new(
                metadata, splits, codec,
            ),
        )
    }));
    Ok(vec![std::sync::Arc::new(factory)])
}

/// The worker-side typed provider factory of every built-in provider.
///
/// StarRocks has no typed read contract yet, so it deliberately contributes no
/// factory: a typed StarRocks scan must fail to resolve rather than reach a
/// placeholder that would look like support.
pub fn compose_backend_read_execution_bundle_factories(
    config: &NovaRocksConfig,
    runtime: tokio::runtime::Handle,
) -> anyhow::Result<
    Vec<(
        novarocks_spi::connector::ConnectorExecutionProviderKind,
        std::sync::Arc<dyn novarocks_backend::ConnectorReadExecutionBundleFactory>,
    )>,
> {
    let binding = IcebergReadBinding::from_resources(compose_connector_file_planning_resources(
        config, runtime,
    )?);
    let factory = std::sync::Arc::new(
        novarocks_connector_iceberg::typed_provider_factory::IcebergTypedProviderFactory::new(
            binding,
            novarocks_connector_iceberg::typed_read::page_source_provider::IcebergPageSourceProviderOptions::with_default_budget(),
        ),
    );
    Ok(vec![(
        novarocks_spi::connector::ConnectorExecutionProviderKind::Iceberg,
        factory,
    )])
}

pub fn compose_iceberg_execution_resources(
    config: &NovaRocksConfig,
    runtime: tokio::runtime::Handle,
) -> anyhow::Result<IcebergExecutionResources> {
    Ok(IcebergExecutionResources::new(
        IcebergReadBinding::from_resources(compose_connector_file_planning_resources(
            config,
            runtime.clone(),
        )?),
        runtime,
    ))
}

pub fn compose_connector_file_planning_resources(
    config: &NovaRocksConfig,
    runtime: tokio::runtime::Handle,
) -> anyhow::Result<FsAccessResources> {
    let object_store = config
        .connector
        .object_store_config(&config.runtime.object_storage.retry_settings())
        .map_err(|error| {
            anyhow::anyhow!("resolve connector startup object-store binding: {error}")
        })?;
    Ok(FsAccessResources::new(
        object_store,
        FsAccessResolver::new(),
        std::sync::Arc::new(TokioFileIoRuntime::new(runtime.clone())),
        std::sync::Arc::new(TokioFileTaskSpawner::new(runtime)),
    ))
}

pub fn state_store_input(config: &NovaRocksConfig) -> anyhow::Result<Option<StateStoreHostInput>> {
    let Some(state_store) = &config.state_store else {
        return Ok(None);
    };
    let limits = resolve_state_store_limits(&state_store.store.limits, MAX_KEY_BYTES)?;
    Ok(Some(StateStoreHostInput {
        cluster_id: state_store.store.cluster_id.clone(),
        provider_id: SQLITE_STATE_STORE_PROVIDER_ID,
        limits,
    }))
}

pub fn state_store_provider_registry(
    config: &NovaRocksConfig,
) -> anyhow::Result<StateStoreProviderRegistry> {
    let mut registry = StateStoreProviderRegistry::new();
    let Some(state_store) = &config.state_store else {
        return Ok(registry);
    };
    let contribution = SqliteStateStoreContribution::new(
        state_store.store.path.clone(),
        state_store.store.history_retention.clone(),
    );
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
        IcebergStorageLakeTargetSnapshotObservation, compose_backend_execution_installers,
        compose_frontend_control_factories, mv_lake_target_snapshot_observation,
    };
    use novarocks_spi::connector::ConnectorExecutionProviderKind;

    #[test]
    fn lake_target_snapshot_adapter_preserves_provider_metadata() {
        let observed = mv_lake_target_snapshot_observation(Some(
            IcebergStorageLakeTargetSnapshotObservation {
                snapshot_id: 42,
                timestamp_ms: 1_700_000_042_000,
            },
        ))
        .expect("SPI snapshot observation")
        .expect("snapshot");

        assert_eq!(observed.snapshot_id(), 42);
        assert_eq!(observed.timestamp_ms(), 1_700_000_042_000);
    }

    #[test]
    fn frontend_and_backend_compose_distinct_iceberg_role_capabilities() {
        let runtime = tokio::runtime::Runtime::new().expect("runtime");
        let config = crate::app_config::NovaRocksConfig::default();
        let factories = compose_frontend_control_factories(
            &config,
            runtime.handle().clone(),
            std::sync::Arc::new(novarocks_frontend::ConnectorReadControlRegistry::new()),
        )
        .expect("frontend factories");
        let installers = compose_backend_execution_installers(&config, runtime.handle().clone())
            .expect("backend installers");
        let iceberg = novarocks_spi::connector::ConnectorProviderId::parse(
            novarocks_connector_iceberg::PROVIDER_ID,
        )
        .expect("provider ID");

        assert_eq!(factories.len(), 1);
        assert_eq!(factories[0].provider_id(), &iceberg);
        assert_eq!(
            installers
                .iter()
                .filter(|installer| installer.provider_kind()
                    == ConnectorExecutionProviderKind::Iceberg)
                .count(),
            1
        );
    }

    #[test]
    fn frontend_factory_resource_failure_is_reported_before_role_startup() {
        let runtime = tokio::runtime::Runtime::new().expect("runtime");
        let mut config = crate::app_config::NovaRocksConfig::default();
        config.connector.object_store = Some(crate::app_config::ConnectorObjectStoreConfig {
            endpoint: Some("http://minio:9000".to_string()),
            access_key_id: None,
            access_key_secret: None,
            region: None,
            enable_path_style_access: Some(true),
        });

        let error = match compose_frontend_control_factories(
            &config,
            runtime.handle().clone(),
            std::sync::Arc::new(novarocks_frontend::ConnectorReadControlRegistry::new()),
        ) {
            Ok(_) => panic!("incomplete frontend resources must fail before role startup"),
            Err(error) => error,
        };
        assert!(
            error
                .to_string()
                .contains("object-store credentials missing aws.s3.access_key"),
            "{error}"
        );
    }
}
