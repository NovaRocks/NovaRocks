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

use std::future::Future;
use std::num::NonZeroUsize;
use std::time::Duration;

use crate::app_config::NovaRocksConfig;
use anyhow::Context;
use novarocks::common::backend_topology::BackendTopologyPort;
use novarocks::common::network;
use novarocks_backend::{
    BackendApplicationHost, BackendServerConfig, BackendStoreSettings, QueryLifecycleRegistryConfig,
};
use novarocks_connector_iceberg::access_binding::IcebergReadBinding;
use novarocks_connector_iceberg::control_factory::IcebergControlFactory;
use novarocks_connector_iceberg::file_reader::execution_installer::IcebergConnectorInstaller;
use novarocks_connector_iceberg::resources::{IcebergControlResources, IcebergExecutionResources};
use novarocks_connector_iceberg::storage_inspector::{
    IcebergStorageInspector, IcebergStorageLakePublication, IcebergStoragePartitionTransform,
    IcebergStorageRefreshTechnique,
};
use novarocks_connector_starrocks::{StarRocksExecutionBindings, StarRocksExecutionInstaller};
use novarocks_execution::runtime::execution_runtime::{
    ExecutionRuntimeConfig, ExecutionSpillStorageConfig,
};
use novarocks_frontend::{
    ClusterBackendOpenConfig, FrontendExecutionConfig, FrontendQueryControlTimeouts,
    FrontendServerConfig,
};
use novarocks_fs::{FsAccessResolver, FsAccessResources, TokioFileIoRuntime, TokioFileTaskSpawner};
use novarocks_spi::connector::{
    ConnectorControlFactory, ConnectorControlPlanningLease, ConnectorError, ConnectorErrorKind,
    ConnectorExecutionInstaller, ConnectorRequestContext, ConnectorTableMetadata,
    MvCreatedTargetObservation, MvLakeDescriptorProjection, MvLakePackageObservation,
    MvLakePublicationObservation, MvMaintenanceMetadataObservation, MvObservedField,
    MvObservedMaintenancePolicy, MvObservedPartitionField, MvObservedPartitionSpec,
    MvObservedPartitionTransform, MvObservedRefreshMarker, MvObservedSnapshot,
    MvPublishedBaseObservation, MvPublishedRefreshObservation, MvPublishedRefreshTechnique,
    MvRefreshBaseObservation, MvRefreshTargetObservation, MvSchemaValidationObservation,
    MvStorageObservationPort, WriteCommitEvidenceLimits,
};

const BACKEND_SUPERVISION_POLL_INTERVAL: Duration = Duration::from_millis(50);

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
                        table_uuid: base.table_uuid,
                        from_snapshot: base.from_snapshot,
                        to_snapshot: base.to_snapshot,
                    })
                    .collect();
                MvLakePublicationObservation::Published(MvPublishedRefreshObservation::try_new(
                    facts.target_snapshot_id,
                    facts.refresh_id,
                    facts.mv_id,
                    facts.token,
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
        MvLakePackageObservation::try_new(metadata.identity.clone(), descriptor, publication)
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
            observed.table_uuid,
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
                            refresh_id: marker.refresh_id,
                            mv_id: marker.mv_id,
                            token: marker.token,
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
    let expected = novarocks_spi::connector::ConnectorProviderId::parse(
        novarocks_connector_iceberg::PROVIDER_ID,
    )
    .map_err(|error| anyhow::anyhow!("invalid composed provider ID: {error}"))?;
    let mut installers: Vec<std::sync::Arc<dyn ConnectorExecutionInstaller>> =
        vec![std::sync::Arc::new(StarRocksExecutionInstaller::new(
            StarRocksExecutionBindings::new(),
        ))];
    for installer in &iceberg_installers {
        if installer.provider_id() != &expected {
            anyhow::bail!(
                "composed connector execution installer has provider `{}`; expected `{}`",
                installer.provider_id().as_str(),
                expected.as_str()
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
    runtime: tokio::runtime::Handle,
) -> anyhow::Result<BackendServerConfig> {
    let runtime_config = &config.runtime;
    let advertise_endpoint = network::standalone_advertise_endpoint(
        &config.server.host,
        &config.server.priority_networks,
        &config.cluster.advertise_host,
        config.server.grpc_port,
    )
    .map_err(|error| anyhow::anyhow!("resolve backend advertise endpoint: {error}"))?;
    Ok(BackendServerConfig {
        bind_host: config.server.host.clone(),
        grpc_port: config.server.grpc_port,
        metrics_http_port: config.server.http_port,
        advertise_endpoint,
        store_settings: BackendStoreSettings::new(
            runtime_config.enable_tablet_write_log,
            runtime_config.tablet_write_log_buffer_size,
            runtime_config.be_txn_info_history_size,
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
        execution_installers: compose_backend_execution_installers(config, runtime)?,
    })
}

/// Resolve every Frontend startup input from the application wire configuration.
pub fn compose_frontend_server_config(
    config: &NovaRocksConfig,
    port_override: Option<u16>,
    runtime: tokio::runtime::Handle,
) -> anyhow::Result<FrontendServerConfig> {
    let runtime_config = &config.runtime;
    let advertised = network::standalone_advertise_endpoint(
        &config.server.host,
        &config.server.priority_networks,
        &config.cluster.advertise_host,
        config.server.grpc_port,
    )
    .map_err(|error| anyhow::anyhow!("resolve frontend advertise endpoint: {error}"))?;
    let runtime_filter_worker_count = NonZeroUsize::new(runtime_config.actual_exec_threads())
        .ok_or_else(|| anyhow::anyhow!("frontend runtime-filter worker count must be nonzero"))?;
    let failure_backoff_ms = config
        .standalone_server
        .as_ref()
        .map(|standalone| standalone.mv_refresh_scheduler_failure_backoff_ms.max(1));
    let mut execution = FrontendExecutionConfig::new(
        advertised.host,
        advertised.port,
        runtime_filter_worker_count,
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
    let backend_seeds = config
        .cluster
        .backends
        .iter()
        .map(|endpoint| {
            endpoint.parse().map_err(|error| {
                anyhow::anyhow!("parse configured backend endpoint '{endpoint}' failed: {error}")
            })
        })
        .collect::<Result<Vec<_>, _>>()?;
    let backend_open = ClusterBackendOpenConfig::new(
        config.cluster.role,
        backend_seeds,
        Duration::from_millis(config.cluster.heartbeat_interval_ms),
        config.cluster.heartbeat_timeout_retries,
        Duration::from_secs(config.cluster.decommission_timeout_secs),
    )
    .map_err(|error| anyhow::anyhow!("open frontend backend cluster configuration: {error}"))?;
    let mysql_listener = novarocks::server::resolve_mysql_listener_settings(
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
    Ok(FrontendServerConfig {
        execution,
        backend_open,
        report_bind_host: config.server.host.clone(),
        report_grpc_port: config.server.grpc_port,
        mysql_listener,
        connector_control_factories: compose_frontend_control_factories(config, runtime)?,
        mv_storage_observation: std::sync::Arc::new(IcebergMvStorageObservationAdapter::default()),
        state_store_host_config: state_store_host_config(config),
    })
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
) -> anyhow::Result<Vec<std::sync::Arc<dyn ConnectorControlFactory>>> {
    let planning_resources = compose_connector_file_planning_resources(config, runtime.clone())?;
    let factory = IcebergControlFactory::new(IcebergControlResources::new(
        IcebergReadBinding::from_resources(planning_resources),
        runtime,
    ));
    Ok(vec![std::sync::Arc::new(factory)])
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

pub fn state_store_host_config(
    config: &NovaRocksConfig,
) -> Option<novarocks_state_store::StateStoreHostConfig> {
    config
        .state_store
        .clone()
        .map(|state_store| novarocks_state_store::StateStoreHostConfig {
            state_store,
            foundationdb_client: config.foundationdb_client.clone(),
        })
}

pub fn run_all_in_one(config: NovaRocksConfig, port_override: Option<u16>) -> anyhow::Result<()> {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .thread_stack_size(novarocks::runtime::global_async_runtime::WORKER_STACK_SIZE_BYTES)
        .build()
        .context("build all-in-one Tokio runtime")?;

    runtime.block_on(run_all_in_one_until(
        config,
        port_override,
        runtime.handle().clone(),
        async {
            tokio::signal::ctrl_c()
                .await
                .map_err(|error| format!("Ctrl-C listener failed: {error}"))
        },
    ))
}

async fn run_all_in_one_until<F>(
    config: NovaRocksConfig,
    port_override: Option<u16>,
    runtime: tokio::runtime::Handle,
    shutdown: F,
) -> anyhow::Result<()>
where
    F: Future<Output = Result<(), String>> + Send,
{
    let frontend_config = compose_frontend_server_config(&config, port_override, runtime.clone())?;
    let frontend = novarocks_frontend::open_frontend_application_for_server(&frontend_config)
        .await
        .map_err(|error| anyhow::anyhow!("open all-in-one frontend application failed: {error}"))?;
    let backend_config = compose_backend_server_config(&config, runtime)?;
    let mut backend = match BackendApplicationHost::open_with_terminal_ingress(
        backend_config,
        Some(frontend.terminal_ingress()),
    ) {
        Ok(backend) => backend,
        Err(error) => {
            let frontend_cleanup = frontend.shutdown().await;
            return Err(anyhow::anyhow!(
                "open all-in-one backend application failed: {error}; frontend cleanup: {:?}",
                frontend_cleanup.err()
            ));
        }
    };
    let endpoint = backend.connectable_native_endpoint();
    let topology = frontend.backend_topology_port();
    topology
        .add_backend(endpoint)
        .map_err(|error| anyhow::anyhow!("register all-in-one backend {endpoint}: {error}"))?;
    wait_for_live_backend(topology.as_ref(), endpoint).await?;
    frontend
        .coordinator_report_endpoint_sink()
        .set_bound_port(endpoint.port());
    let session_factory = novarocks_frontend::build_frontend_query_session_factory(
        &frontend,
        std::sync::Arc::new(novarocks_frontend::SystemCatalogService::with_defaults()),
        endpoint.port(),
        std::sync::Arc::clone(&frontend_config.mv_storage_observation),
    )
    .map_err(|error| {
        anyhow::anyhow!("build all-in-one frontend SQL capabilities failed: {error}")
    })?;

    let (server_shutdown_tx, server_shutdown_rx) = tokio::sync::oneshot::channel();
    let listener = novarocks::server::resolve_mysql_listener_settings(
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
    .map_err(anyhow::Error::msg)?;
    let server =
        novarocks::server::run_mysql_server_until_shutdown(listener, session_factory, async move {
            let _ = server_shutdown_rx.await;
        });
    tokio::pin!(server);
    tokio::pin!(shutdown);

    let mut server_completed = false;
    let primary = loop {
        tokio::select! {
            server_result = &mut server => {
                server_completed = true;
                break server_result;
            }
            shutdown_result = &mut shutdown => break shutdown_result,
            _ = tokio::time::sleep(BACKEND_SUPERVISION_POLL_INTERVAL) => {
                match backend.poll_failure() {
                    Ok(Some(error)) | Err(error) => break Err(error.to_string()),
                    Ok(None) => {}
                }
            }
        }
    };

    let server_cleanup = if server_completed {
        Ok(())
    } else {
        let _ = server_shutdown_tx.send(());
        server.await
    };
    let backend_cleanup = backend.shutdown().map_err(|error| error.to_string());
    let frontend_cleanup = frontend.shutdown().await.map_err(|error| error.to_string());
    combine_primary_and_cleanup(primary, server_cleanup, backend_cleanup, frontend_cleanup)
        .map_err(anyhow::Error::msg)
}

async fn wait_for_live_backend(
    topology: &dyn BackendTopologyPort,
    endpoint: std::net::SocketAddr,
) -> anyhow::Result<()> {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    loop {
        if topology
            .snapshot()
            .map_err(|error| anyhow::anyhow!("read all-in-one backend topology: {error}"))?
            .targets()
            .iter()
            .copied()
            .any(|backend| backend.endpoint() == endpoint)
        {
            return Ok(());
        }
        if tokio::time::Instant::now() >= deadline {
            anyhow::bail!(
                "all-in-one backend {endpoint} did not become Live before startup timeout"
            );
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

fn combine_primary_and_cleanup(
    primary: Result<(), String>,
    server_cleanup: Result<(), String>,
    backend_cleanup: Result<(), String>,
    frontend_cleanup: Result<(), String>,
) -> Result<(), String> {
    let cleanup_errors = [
        server_cleanup.err(),
        backend_cleanup.err(),
        frontend_cleanup.err(),
    ]
    .into_iter()
    .flatten()
    .collect::<Vec<_>>();

    match (primary, cleanup_errors.is_empty()) {
        (Ok(()), true) => Ok(()),
        (Ok(()), false) => Err(format!("cleanup failed: {}", cleanup_errors.join("; "))),
        (Err(primary), true) => Err(primary),
        (Err(primary), false) => Err(format!(
            "{primary}; cleanup failed: {}",
            cleanup_errors.join("; ")
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::{
        combine_primary_and_cleanup, compose_backend_execution_installers,
        compose_frontend_control_factories,
    };

    #[test]
    fn primary_failure_remains_primary_when_all_cleanup_steps_fail() {
        let error = combine_primary_and_cleanup(
            Err("backend failed".to_string()),
            Err("server cleanup failed".to_string()),
            Err("backend cleanup failed".to_string()),
            Err("frontend cleanup failed".to_string()),
        )
        .expect_err("backend failure must be returned");

        assert!(error.contains("backend failed"), "{error}");
        assert!(error.contains("server cleanup failed"), "{error}");
        assert!(error.contains("frontend cleanup failed"), "{error}");
        assert!(error.contains("backend cleanup failed"), "{error}");
    }

    #[test]
    fn frontend_and_backend_compose_distinct_iceberg_role_capabilities() {
        let runtime = tokio::runtime::Runtime::new().expect("runtime");
        let config = crate::app_config::NovaRocksConfig::default();
        let factories = compose_frontend_control_factories(&config, runtime.handle().clone())
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
                .filter(|installer| installer.provider_id() == &iceberg)
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

        let error = match compose_frontend_control_factories(&config, runtime.handle().clone()) {
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
