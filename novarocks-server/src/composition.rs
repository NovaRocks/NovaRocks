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
use std::path::PathBuf;
use std::time::Duration;

use anyhow::Context;
use novarocks::common::app_config::NovaRocksConfig;
use novarocks::mv::persistence::descriptor::MvDescriptorV1;
use novarocks::mv::persistence::schema::{
    MvPartitionContract, MvPartitionFieldContract, MvPartitionTransformContract,
};
use novarocks::mv::storage_observation::{
    MvLakePackageObservation, MvLakePublication, MvMaintenanceMetadataObservation,
    MvObservedMaintenancePolicy, MvObservedRefreshMarker, MvObservedSnapshot,
    MvObservedTargetField, MvPublishedBaseFact, MvPublishedLakeFacts, MvPublishedRefreshTechnique,
    MvRefreshBaseObservation, MvRefreshTargetObservation, MvSchemaValidationObservation,
    MvSchemaValidationPartitionContract, MvSchemaValidationPartitionField,
    MvSchemaValidationPartitionTransform, MvStorageObservationPort, MvTargetCreationObservation,
};
use novarocks::query_execution::backend::BackendTopologyPort;
use novarocks_backend::{BackendApplicationHost, BackendServerConfig};
use novarocks_connector_iceberg::access_binding::IcebergReadBinding;
use novarocks_connector_iceberg::file_reader::execution_installer::IcebergConnectorInstaller;
use novarocks_connector_iceberg::resources::IcebergExecutionResources;
use novarocks_connector_iceberg::storage_inspector::{
    IcebergStorageInspector, IcebergStorageLakePublication, IcebergStoragePartitionTransform,
    IcebergStorageRefreshTechnique,
};
use novarocks_connector_starrocks::{StarRocksExecutionBindings, StarRocksExecutionInstaller};
use novarocks_frontend::FrontendServerConfig;
use novarocks_fs::{FsAccessResolver, FsAccessResources, TokioFileIoRuntime, TokioFileTaskSpawner};
use novarocks_spi::connector::{
    ConnectorControlFactory, ConnectorControlPlanningLease, ConnectorError, ConnectorErrorKind,
    ConnectorExecutionInstaller, ConnectorRequestContext, ConnectorTableMetadata,
};

const BACKEND_SUPERVISION_POLL_INTERVAL: Duration = Duration::from_millis(50);

#[derive(Clone, Copy, Debug, Default)]
pub struct IcebergMvStorageObservationAdapter {
    inspector: IcebergStorageInspector,
}

/// Map the inspector's partition contract onto the neutral MV contract.
///
/// Shared by the created-target and refresh-target observations so the two
/// cannot drift into different transform mappings.
fn mv_partition_contract(
    observed: novarocks_connector_iceberg::storage_inspector::IcebergStoragePartitionContract,
) -> MvPartitionContract {
    MvPartitionContract {
        target_spec_id: observed.target_spec_id,
        fields: observed
            .fields
            .into_iter()
            .map(|field| MvPartitionFieldContract {
                partition_field_id: field.partition_field_id,
                partition_field_name: field.partition_field_name,
                source_target_field_id: field.source_target_field_id,
                source_column_name: field.source_column_name,
                transform: match field.transform {
                    IcebergStoragePartitionTransform::Identity => {
                        MvPartitionTransformContract::Identity
                    }
                    IcebergStoragePartitionTransform::Year => MvPartitionTransformContract::Year,
                    IcebergStoragePartitionTransform::Month => MvPartitionTransformContract::Month,
                    IcebergStoragePartitionTransform::Day => MvPartitionTransformContract::Day,
                    IcebergStoragePartitionTransform::Hour => MvPartitionTransformContract::Hour,
                    IcebergStoragePartitionTransform::Bucket { num_buckets } => {
                        MvPartitionTransformContract::Bucket { num_buckets }
                    }
                    IcebergStoragePartitionTransform::Truncate { width } => {
                        MvPartitionTransformContract::Truncate { width }
                    }
                    IcebergStoragePartitionTransform::Void => MvPartitionTransformContract::Void,
                },
            })
            .collect(),
    }
}

impl MvStorageObservationPort for IcebergMvStorageObservationAdapter {
    fn observe_created_target(
        &self,
        exact_lease: &ConnectorControlPlanningLease,
        metadata: &ConnectorTableMetadata,
        context: ConnectorRequestContext,
    ) -> Result<MvTargetCreationObservation, ConnectorError> {
        let observed = self
            .inspector
            .observe_created_target(exact_lease, metadata, context)?;
        let fields = observed
            .fields
            .into_iter()
            .map(|field| MvObservedTargetField {
                field_id: field.field_id,
                name: field.name,
                type_signature: field.type_signature,
                nullable: field.nullable,
            })
            .collect();
        let partition = mv_partition_contract(observed.partition);
        MvTargetCreationObservation::try_new(
            metadata.identity.clone(),
            observed.table_uuid,
            observed.schema_id,
            fields,
            partition,
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
            .map(|field| MvObservedTargetField {
                field_id: field.field_id,
                name: field.name,
                type_signature: field.type_signature,
                nullable: field.nullable,
            })
            .collect();
        let partition = MvSchemaValidationPartitionContract::new(
            observed.partition.target_spec_id,
            observed
                .partition
                .fields
                .into_iter()
                .map(|field| {
                    MvSchemaValidationPartitionField::new(
                        field.partition_field_id,
                        field.partition_field_name,
                        field.source_target_field_id,
                        field.source_column_name,
                        match field.transform {
                            IcebergStoragePartitionTransform::Identity => {
                                MvSchemaValidationPartitionTransform::Identity
                            }
                            IcebergStoragePartitionTransform::Year => {
                                MvSchemaValidationPartitionTransform::Year
                            }
                            IcebergStoragePartitionTransform::Month => {
                                MvSchemaValidationPartitionTransform::Month
                            }
                            IcebergStoragePartitionTransform::Day => {
                                MvSchemaValidationPartitionTransform::Day
                            }
                            IcebergStoragePartitionTransform::Hour => {
                                MvSchemaValidationPartitionTransform::Hour
                            }
                            IcebergStoragePartitionTransform::Bucket { num_buckets } => {
                                MvSchemaValidationPartitionTransform::Bucket { num_buckets }
                            }
                            IcebergStoragePartitionTransform::Truncate { width } => {
                                MvSchemaValidationPartitionTransform::Truncate { width }
                            }
                            IcebergStoragePartitionTransform::Void => {
                                MvSchemaValidationPartitionTransform::Void
                            }
                        },
                    )
                })
                .collect(),
        );
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
        let Some(observed) = self
            .inspector
            .observe_lake_package(exact_lease, metadata, context)?
        else {
            return Ok(None);
        };
        let properties = observed
            .descriptor_properties
            .into_iter()
            .collect::<std::collections::HashMap<_, _>>();
        let descriptor = MvDescriptorV1::from_storage_properties(&properties).map_err(|error| {
            ConnectorError::new(
                ConnectorErrorKind::CorruptData,
                format!("decode Iceberg MV storage descriptor: {error}"),
            )
        })?;
        let publication = match observed.publication {
            IcebergStorageLakePublication::NeverPublished => MvLakePublication::NeverPublished,
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
                    .map(|base| MvPublishedBaseFact {
                        table_fqn: base.table_fqn,
                        table_uuid: base.table_uuid,
                        from_snapshot: base.from_snapshot,
                        to_snapshot: base.to_snapshot,
                    })
                    .collect();
                MvLakePublication::Published(MvPublishedLakeFacts::try_new(
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
            mv_partition_contract(observed.partition),
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

pub fn compose_frontend_control_factories() -> Vec<std::sync::Arc<dyn ConnectorControlFactory>> {
    // SPI-5EF Phase 1 keeps the legacy Core Iceberg control binding as the
    // sole production FE authority. The provider factory remains available
    // for provider-local conformance tests and will be installed only by the
    // follow-up atomic factory/owner cut.
    Vec::new()
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

pub fn run_all_in_one(
    config: NovaRocksConfig,
    config_path: Option<PathBuf>,
    port_override: Option<u16>,
) -> anyhow::Result<()> {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .thread_stack_size(novarocks::runtime::global_async_runtime::WORKER_STACK_SIZE_BYTES)
        .build()
        .context("build all-in-one Tokio runtime")?;

    runtime.block_on(run_all_in_one_until(
        config,
        config_path,
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
    config_path: Option<PathBuf>,
    port_override: Option<u16>,
    runtime: tokio::runtime::Handle,
    shutdown: F,
) -> anyhow::Result<()>
where
    F: Future<Output = Result<(), String>> + Send,
{
    let optimizer_query_mem_limit_bytes = config.runtime.optimizer_query_mem_limit_bytes;
    let connector_control_factories = compose_frontend_control_factories();
    let connector_file_planning_resources = Some(compose_connector_file_planning_resources(
        &config,
        runtime.clone(),
    )?);
    let frontend_config = FrontendServerConfig {
        config: config.clone(),
        config_path: config_path.clone(),
        port_override,
        connector_control_factories,
        connector_file_planning_resources,
        mv_storage_observation: std::sync::Arc::new(IcebergMvStorageObservationAdapter::default()),
        state_store_host_config: state_store_host_config(&config),
    };
    let frontend = novarocks_frontend::open_frontend_application_for_server(&frontend_config)
        .await
        .map_err(|error| anyhow::anyhow!("open all-in-one frontend application failed: {error}"))?;
    let execution_installers = compose_backend_execution_installers(&config, runtime)?;
    let mut backend = match BackendApplicationHost::open_with_terminal_ingress(
        BackendServerConfig {
            config: config.clone(),
            execution_installers,
        },
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
    let dml = frontend.dml_service();
    let mut services = novarocks_frontend::standalone_open_services_for_server(
        &frontend,
        std::sync::Arc::clone(&frontend_config.mv_storage_observation),
        frontend_config.connector_file_planning_resources.clone(),
    );
    services
        .backend_topology
        .add_backend(endpoint)
        .map_err(|error| anyhow::anyhow!("register all-in-one backend {endpoint}: {error}"))?;
    wait_for_live_backend(services.backend_topology.as_ref(), endpoint).await?;
    services.exchange_port = endpoint.port();

    let (server_shutdown_tx, server_shutdown_rx) = tokio::sync::oneshot::channel();
    let query_control = services.query_control.clone();
    let query_execution = services.query_execution.clone();
    let topology = services.backend_topology.clone();
    let role = services.execution_role;
    let server =
        novarocks::server::run_standalone_server_with_config_until_shutdown_with_session_factory(
            config,
            config_path,
            port_override,
            services,
            move |engine| {
                let insert_engine = engine.insert_engine();
                let delete_engine = engine.delete_engine();
                let mutation_engine = engine.mutation_engine();
                let add_files_engine = engine.add_files_engine();
                let ctas_engine = engine.ctas_engine();
                let truncate_engine = engine.truncate_engine();
                Ok(std::sync::Arc::new(
                    novarocks_frontend::FrontendQueryService::new(
                        engine,
                        query_control,
                        query_execution,
                        role,
                        topology,
                        dml,
                        insert_engine,
                        delete_engine,
                        mutation_engine,
                        add_files_engine,
                        ctas_engine,
                        truncate_engine,
                        optimizer_query_mem_limit_bytes,
                    ),
                ))
            },
            async move {
                let _ = server_shutdown_rx.await;
            },
        );
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
        let config = novarocks::common::app_config::NovaRocksConfig::default();
        let factories = compose_frontend_control_factories();
        let installers = compose_backend_execution_installers(&config, runtime.handle().clone())
            .expect("backend installers");
        let iceberg = novarocks_spi::connector::ConnectorProviderId::parse(
            novarocks_connector_iceberg::PROVIDER_ID,
        )
        .expect("provider ID");

        assert!(
            factories.is_empty(),
            "Phase 1 must keep legacy Core control as the sole FE authority"
        );
        assert_eq!(
            installers
                .iter()
                .filter(|installer| installer.provider_id() == &iceberg)
                .count(),
            1
        );
    }
}
