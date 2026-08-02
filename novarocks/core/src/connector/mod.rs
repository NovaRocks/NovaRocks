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
pub(crate) mod backend;
pub(crate) mod data_mutation;
pub mod file_execution;
pub mod iceberg;
pub mod metadata_maintenance;
pub mod mutation;
pub mod runtime;
pub(crate) mod scan_model;
pub mod schema;
pub(crate) mod stats;
pub(crate) mod unified_statistics;

pub(crate) use backend::MvBackend;
#[cfg(test)]
pub(crate) use iceberg::catalog::load_table as load_iceberg_table;
pub(crate) use iceberg::catalog::{
    IcebergCatalogRegistry, namespace_exists as iceberg_namespace_exists,
};
#[cfg(test)]
pub(crate) use iceberg::changes::plan_changes as plan_iceberg_changes;
#[cfg(test)]
use std::collections::BTreeMap;
use std::collections::HashMap;
use std::sync::Arc;
#[cfg(test)]
use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Instant;

use novarocks_spi::connector::{
    ConnectorCancellation, ConnectorInstanceId, ConnectorRequestContext, ConnectorTableIdentity,
    ConnectorTableRequest, ConnectorTableResolution,
};

struct RequestConnectorCancellation {
    signal: Arc<AtomicBool>,
}

impl ConnectorCancellation for RequestConnectorCancellation {
    fn is_cancelled(&self) -> bool {
        self.signal.load(Ordering::SeqCst)
    }
}

struct QueryConnectorCancellation {
    cancellation: crate::query_execution::cancellation::QueryCancellationView,
}

impl ConnectorCancellation for QueryConnectorCancellation {
    fn is_cancelled(&self) -> bool {
        self.cancellation.is_cancelled()
    }
}

fn build_connector_request_context(
    query_options: Option<&crate::runtime::query_options::QueryOptions>,
    cancellation: Arc<dyn ConnectorCancellation>,
) -> Result<ConnectorRequestContext, String> {
    let (_, query_expire) = crate::runtime::query_options::query_expire_durations(query_options);
    ConnectorRequestContext::try_new(
        Instant::now() + query_expire,
        cancellation,
        novarocks_spi::connector::MAX_CONNECTOR_HANDLE_PAYLOAD_BYTES,
        novarocks_spi::connector::MAX_CONNECTOR_TOTAL_PAYLOAD_BYTES,
    )
    .map_err(|error| error.to_string())
}

pub(crate) fn connector_request_context(
    query_options: Option<&crate::runtime::query_options::QueryOptions>,
    cancellation_signal: Arc<AtomicBool>,
) -> Result<ConnectorRequestContext, String> {
    build_connector_request_context(
        query_options,
        Arc::new(RequestConnectorCancellation {
            signal: cancellation_signal,
        }),
    )
}

pub(crate) fn connector_request_context_for_query(
    query_options: Option<&crate::runtime::query_options::QueryOptions>,
    cancellation: crate::query_execution::cancellation::QueryCancellationView,
) -> Result<ConnectorRequestContext, String> {
    build_connector_request_context(
        query_options,
        Arc::new(QueryConnectorCancellation { cancellation }),
    )
}

/// Derive connector admission from the immutable query execution captured by
/// the frontend. A request deadline is authoritative; only requests without an
/// admission deadline use the bounded connector fallback.
pub(crate) fn connector_request_context_for_execution(
    query_options: Option<&crate::runtime::query_options::QueryOptions>,
    execution: &crate::query_execution::request_context::QueryExecutionContext,
) -> Result<ConnectorRequestContext, String> {
    let cancellation: Arc<dyn ConnectorCancellation> = Arc::new(QueryConnectorCancellation {
        cancellation: execution.cancellation().clone(),
    });
    match execution.deadline() {
        Some(deadline) => ConnectorRequestContext::try_new(
            deadline,
            cancellation,
            novarocks_spi::connector::MAX_CONNECTOR_HANDLE_PAYLOAD_BYTES,
            novarocks_spi::connector::MAX_CONNECTOR_TOTAL_PAYLOAD_BYTES,
        )
        .map_err(|error| error.to_string()),
        None => build_connector_request_context(query_options, cancellation),
    }
}

pub(crate) fn validate_request_context(context: &ConnectorRequestContext) -> Result<(), String> {
    if context.cancellation().is_cancelled() {
        return Err("connector request was cancelled".to_string());
    }
    if Instant::now() >= context.deadline() {
        return Err("connector request deadline elapsed".to_string());
    }
    Ok(())
}

#[cfg(test)]
pub(crate) fn test_request_context() -> ConnectorRequestContext {
    connector_request_context(None, Arc::new(AtomicBool::new(false)))
        .expect("test connector request context")
}

#[cfg(test)]
mod request_context_tests {
    use std::time::{Duration, Instant};

    use super::connector_request_context_for_execution;
    use crate::common::app_config::ClusterRole;
    use crate::query_execution::backend::BackendTopologySnapshot;
    use crate::query_execution::cancellation::{QueryCancellationReason, QueryCancellationSource};
    use crate::query_execution::request_context::{RequestAdmission, RequestContext};
    use crate::sql::optimizer::options::SessionOptimizerSettings;

    #[test]
    fn connector_context_preserves_admitted_deadline_and_cancellation() {
        let cancellation = QueryCancellationSource::new();
        let deadline = Instant::now() + Duration::from_secs(17);
        let request = RequestContext::admit(RequestAdmission::new(
            None,
            "db".to_string(),
            ClusterRole::Fe,
            BackendTopologySnapshot::empty(41),
            Some(deadline),
            cancellation.view(),
            SessionOptimizerSettings::default(),
        ));

        let connector = connector_request_context_for_execution(None, request.execution()).unwrap();
        assert_eq!(connector.deadline(), deadline);
        assert!(!connector.cancellation().is_cancelled());

        cancellation.request(QueryCancellationReason::ClientDisconnected);
        assert!(request.execution().cancellation().is_cancelled());
        assert!(connector.cancellation().is_cancelled());
    }

    #[test]
    fn connector_context_without_admitted_deadline_uses_bounded_fallback() {
        let request = RequestContext::admit(RequestAdmission::new(
            None,
            "db".to_string(),
            ClusterRole::Fe,
            BackendTopologySnapshot::empty(43),
            None,
            QueryCancellationSource::new().view(),
            SessionOptimizerSettings::default(),
        ));
        let before = Instant::now();
        let connector = connector_request_context_for_execution(None, request.execution()).unwrap();
        assert!(connector.deadline() > before);
    }
}

fn metadata_binding(
    controls: &dyn novarocks_spi::connector::ConnectorControlResolver,
    catalog: &str,
) -> Result<novarocks_spi::connector::ConnectorControlPlanningLease, String> {
    let instance_id = ConnectorInstanceId::parse(catalog).map_err(|error| error.to_string())?;
    controls
        .acquire_current(&instance_id)
        .map_err(|error| error.to_string())
}

pub(crate) fn metadata_namespace_exists(
    controls: &dyn novarocks_spi::connector::ConnectorControlResolver,
    context: ConnectorRequestContext,
    catalog: &str,
    namespace: &str,
) -> Result<bool, String> {
    let binding = metadata_binding(controls, catalog)?;
    let instance_id = binding.binding().descriptor().instance_id.clone();
    binding
        .binding()
        .metadata()
        .namespace_exists(novarocks_spi::connector::ConnectorNamespaceRequest {
            namespace: novarocks_spi::connector::ConnectorNamespaceIdentity {
                instance_id,
                namespace: Arc::from(namespace),
            },
            context,
        })
        .map_err(|error| error.to_string())
}

pub(crate) fn metadata_table_exists(
    controls: &dyn novarocks_spi::connector::ConnectorControlResolver,
    context: ConnectorRequestContext,
    catalog: &str,
    namespace: &str,
    table: &str,
) -> Result<bool, String> {
    let binding = metadata_binding(controls, catalog)?;
    let instance_id = binding.binding().descriptor().instance_id.clone();
    binding
        .binding()
        .metadata()
        .table_exists(ConnectorTableRequest {
            table: ConnectorTableIdentity {
                instance_id,
                namespace: Arc::from(namespace),
                table: Arc::from(table),
            },
            resolution: ConnectorTableResolution::StrictBaseTable,
            context,
        })
        .map_err(|error| error.to_string())
}

pub(crate) fn metadata_load_table(
    controls: &dyn novarocks_spi::connector::ConnectorControlResolver,
    context: ConnectorRequestContext,
    catalog: &str,
    namespace: &str,
    table: &str,
    resolution: ConnectorTableResolution,
) -> Result<(backend::ResolvedTable, Option<i32>), String> {
    let binding = metadata_binding(controls, catalog)?;
    let instance_id = binding.binding().descriptor().instance_id.clone();
    let metadata = binding
        .binding()
        .metadata()
        .load_table(ConnectorTableRequest {
            table: ConnectorTableIdentity {
                instance_id,
                namespace: Arc::from(namespace),
                table: Arc::from(table),
            },
            resolution,
            context,
        })
        .map_err(|error| error.to_string())?;
    let columns = metadata
        .schema
        .fields()
        .iter()
        .map(|field| novarocks_catalog::schema::ColumnDef {
            name: field.name().clone(),
            data_type: field.data_type().clone(),
            nullable: field.is_nullable(),
            write_default: None,
            logical_type: None,
        })
        .collect();
    let schema_id = metadata.version.as_ref().and_then(|version| {
        <[u8; 4]>::try_from(version.as_ref())
            .ok()
            .map(i32::from_le_bytes)
    });
    Ok((
        backend::ResolvedTable {
            catalog: metadata.identity.instance_id.as_str().to_string(),
            namespace: metadata.identity.namespace.to_string(),
            table: metadata.identity.table.to_string(),
            columns,
            statistics_pin: metadata
                .statistics_data_version
                .clone()
                .map(|data_version| backend::ResolvedTableStatisticsPin {
                    table: metadata.table.clone(),
                    data_version,
                }),
        },
        schema_id,
    ))
}

pub use crate::common::min_max_predicate::{MinMaxPredicate, MinMaxPredicateValue};

pub use crate::connector::file_execution::FileScanRange;
pub use crate::formats::FileFormatConfig;
pub use crate::formats::orc::OrcScanConfig;
pub use crate::formats::parquet::ParquetScanConfig;

#[cfg(test)]
mod iceberg_provider_test;
#[cfg(test)]
mod runtime_test;

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    #[test]
    fn standalone_catalog_service_keeps_internal_entry_after_backend_registration() {
        let state = Arc::new(crate::engine::StandaloneState::default());
        super::register_standalone_backends(&state);

        let registry = state
            .catalog_service
            .registry()
            .read()
            .expect("catalog service registry");
        assert!(registry.get_catalog("default_catalog").is_ok());
    }
}

#[derive(Clone)]
pub struct ConnectorRegistry {
    mv_backends: HashMap<&'static str, Arc<dyn MvBackend>>,
    #[cfg(test)]
    fixture_controls: Arc<
        Mutex<
            BTreeMap<ConnectorInstanceId, Arc<novarocks_spi::connector::ConnectorControlBinding>>,
        >,
    >,
}

impl ConnectorRegistry {
    pub fn new() -> Self {
        Self {
            mv_backends: HashMap::new(),
            #[cfg(test)]
            fixture_controls: Arc::new(Mutex::new(BTreeMap::new())),
        }
    }

    #[cfg(test)]
    pub(crate) fn register_fixture_control(
        &self,
        binding: novarocks_spi::connector::ConnectorControlBinding,
    ) {
        self.fixture_controls
            .lock()
            .expect("fixture connector control lock")
            .insert(binding.descriptor().instance_id.clone(), Arc::new(binding));
    }

    #[cfg(test)]
    fn acquire_fixture_control(
        &self,
        instance_id: &ConnectorInstanceId,
    ) -> Result<
        novarocks_spi::connector::ConnectorControlPlanningLease,
        novarocks_spi::connector::ConnectorError,
    > {
        let binding = self
            .fixture_controls
            .lock()
            .map_err(|_| {
                novarocks_spi::connector::ConnectorError::new(
                    novarocks_spi::connector::ConnectorErrorKind::Internal,
                    "fixture connector control lock poisoned",
                )
            })?
            .get(instance_id)
            .cloned()
            .ok_or_else(|| {
                novarocks_spi::connector::ConnectorError::new(
                    novarocks_spi::connector::ConnectorErrorKind::NotFound,
                    "test fixture did not register a connector control binding",
                )
            })?;
        Ok(novarocks_spi::connector::ConnectorControlPlanningLease::new(binding, || {}))
    }

    pub(crate) fn register_mv_backend(&mut self, backend: Arc<dyn MvBackend>) {
        self.mv_backends.insert(backend.name(), backend);
    }

    pub(crate) fn mv_backend(&self, name: &str) -> Result<Arc<dyn MvBackend>, String> {
        self.mv_backends
            .get(name)
            .cloned()
            .ok_or_else(|| format!("unknown MV backend: {name}"))
    }

    pub(crate) fn mv_backends(&self) -> Vec<Arc<dyn MvBackend>> {
        let mut entries: Vec<_> = self.mv_backends.iter().collect();
        entries.sort_by(|(left, _), (right, _)| left.cmp(right));
        entries
            .into_iter()
            .map(|(_, backend)| Arc::clone(backend))
            .collect()
    }
}

/// Test-only resolver for fixtures that explicitly register a control binding.
#[cfg(test)]
pub(crate) struct FixtureControlResolver {
    registry: ConnectorRegistry,
}

#[cfg(test)]
impl FixtureControlResolver {
    pub(crate) fn new(registry: ConnectorRegistry) -> Self {
        Self { registry }
    }
}

#[cfg(test)]
impl novarocks_spi::connector::ConnectorControlResolver for FixtureControlResolver {
    fn observe_current_binding(
        &self,
        instance_id: &ConnectorInstanceId,
    ) -> Result<
        novarocks_spi::connector::ConnectorExecutionBindingKey,
        novarocks_spi::connector::ConnectorError,
    > {
        let binding = self
            .registry
            .fixture_controls
            .lock()
            .map_err(|_| {
                novarocks_spi::connector::ConnectorError::new(
                    novarocks_spi::connector::ConnectorErrorKind::Internal,
                    "fixture connector control lock poisoned",
                )
            })?
            .get(instance_id)
            .cloned()
            .ok_or_else(|| {
                novarocks_spi::connector::ConnectorError::new(
                    novarocks_spi::connector::ConnectorErrorKind::NotFound,
                    "test fixture did not register a connector control binding",
                )
            })?;
        Ok(novarocks_spi::connector::ConnectorExecutionBindingKey {
            instance_id: binding.descriptor().instance_id.clone(),
            incarnation: binding.incarnation(),
        })
    }

    fn acquire_current(
        &self,
        instance_id: &ConnectorInstanceId,
    ) -> Result<
        novarocks_spi::connector::ConnectorControlPlanningLease,
        novarocks_spi::connector::ConnectorError,
    > {
        self.registry.acquire_fixture_control(instance_id)
    }
}

/// Compose the BE-only installers used by the execution host. The resulting
/// installers are bound entirely from process startup configuration; an
/// execution declaration can select a named binding but cannot carry a client
/// or credential into the BE.
pub fn compose_backend_connector_execution_installers(
    default_object_store: Option<novarocks_fs::ObjectStoreConfig>,
) -> Result<Vec<Arc<dyn novarocks_spi::connector::ConnectorExecutionInstaller>>, String> {
    let file_runtime = crate::runtime::global_async_runtime::data_runtime_handle()?;
    let binding = iceberg::provider::IcebergReadBinding::new(
        default_object_store,
        Arc::new(novarocks_fs::TokioFileIoRuntime::new(file_runtime.clone())),
        Arc::new(novarocks_fs::TokioFileTaskSpawner::new(file_runtime)),
    );
    Ok(vec![Arc::new(
        iceberg::provider::IcebergConnectorInstaller::new(binding),
    )])
}

pub(crate) fn register_standalone_backends(state: &Arc<crate::engine::StandaloneState>) {
    {
        let mut connectors = state
            .connectors
            .write()
            .expect("standalone connector registry write lock");
        connectors.register_mv_backend(Arc::new(
            crate::engine::mv::iceberg_backend::IcebergMvBackend::new(state),
        ));
    }
}

impl Default for ConnectorRegistry {
    fn default() -> Self {
        ConnectorRegistry::new()
    }
}

impl std::fmt::Debug for ConnectorRegistry {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut mv_backends: Vec<_> = self.mv_backends.keys().copied().collect();
        mv_backends.sort();
        f.debug_struct("ConnectorRegistry")
            .field("mv_backends", &mv_backends)
            .finish()
    }
}
