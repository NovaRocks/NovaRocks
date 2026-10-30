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

use super::backend;
use crate::connector::schema_creation::FrontendSchemaLoad;
use novarocks_proto_codec::lifecycle::QueryOptions;
use std::sync::Arc;
use std::time::{Duration, Instant};

use novarocks_spi::connector::{
    ConnectorError, ConnectorErrorKind, ConnectorInstanceId, ConnectorListNamespacesRequest,
    ConnectorNamespaceIdentity, ConnectorPlanningContext, ConnectorReadReferenceFacts,
    ConnectorReadReferenceFactsRequest, ConnectorRequestContext, ConnectorStopOwner,
    ConnectorTableIdentity, ConnectorTableRequest, ConnectorTableResolution,
};

struct QueryStopRelay {
    relay: Option<tokio::task::AbortHandle>,
}

impl Drop for QueryStopRelay {
    fn drop(&mut self) {
        if let Some(relay) = self.relay.take() {
            relay.abort();
        }
    }
}

pub(crate) fn query_connector_request_context_on_runtime(
    runtime: &tokio::runtime::Handle,
    deadline: Instant,
    cancellation: novarocks_query_application::cancellation::QueryCancellationView,
) -> Result<ConnectorRequestContext, String> {
    let stop = ConnectorStopOwner::new();
    let relay = if cancellation.is_cancelled() {
        stop.request_stop();
        None
    } else {
        let source = cancellation.clone();
        let owner = stop.clone();
        Some(
            runtime
                .spawn(async move {
                    source.cancelled().await;
                    owner.request_stop();
                })
                .abort_handle(),
        )
    };
    ConnectorRequestContext::try_new(
        deadline,
        stop.view(),
        novarocks_spi::connector::MAX_CONNECTOR_HANDLE_PAYLOAD_BYTES,
        novarocks_spi::connector::MAX_CONNECTOR_TOTAL_PAYLOAD_BYTES,
    )
    .map(|request| request.with_stop_lifetime(Arc::new(QueryStopRelay { relay })))
    .map_err(|error| error.to_string())
}

fn build_connector_request_context(
    query_options: Option<&QueryOptions>,
    stop: novarocks_spi::connector::ConnectorStopView,
) -> Result<ConnectorRequestContext, String> {
    let query_expire = query_expire_duration(query_options);
    ConnectorRequestContext::try_new(
        Instant::now() + query_expire,
        stop,
        novarocks_spi::connector::MAX_CONNECTOR_HANDLE_PAYLOAD_BYTES,
        novarocks_spi::connector::MAX_CONNECTOR_TOTAL_PAYLOAD_BYTES,
    )
    .map_err(|error| error.to_string())
}

pub fn connector_request_context(
    query_options: Option<&QueryOptions>,
    stop: novarocks_spi::connector::ConnectorStopView,
) -> Result<ConnectorRequestContext, String> {
    build_connector_request_context(query_options, stop)
}

/// Freeze connector request facts from the admitted frontend statement.
///
/// Frontend-owned typed command capabilities use this same constructor so
/// provider requests share the statement cancellation identity and options.
#[cfg(test)]
pub fn connector_request_context_for_query(
    query_options: Option<&QueryOptions>,
    cancellation: novarocks_query_application::cancellation::QueryCancellationView,
) -> Result<ConnectorRequestContext, String> {
    let runtime = tokio::runtime::Handle::try_current()
        .map_err(|_| "connector query cancellation requires the FE runtime".to_string())?;
    connector_request_context_for_query_on_runtime(&runtime, query_options, cancellation)
}

pub(crate) fn connector_request_context_for_query_on_runtime(
    runtime: &tokio::runtime::Handle,
    query_options: Option<&QueryOptions>,
    cancellation: novarocks_query_application::cancellation::QueryCancellationView,
) -> Result<ConnectorRequestContext, String> {
    query_connector_request_context_on_runtime(
        runtime,
        Instant::now() + query_expire_duration(query_options),
        cancellation,
    )
}

pub(crate) fn connector_planning_context_for_query_on_runtime(
    runtime: &tokio::runtime::Handle,
    query_options: Option<&QueryOptions>,
    cancellation: novarocks_query_application::cancellation::QueryCancellationView,
) -> Result<ConnectorPlanningContext, String> {
    ConnectorPlanningContext::try_from_request(query_connector_request_context_on_runtime(
        runtime,
        Instant::now() + query_expire_duration(query_options),
        cancellation,
    )?)
    .map_err(|error| error.to_string())
}

/// Derive connector admission from the immutable query execution captured by
/// the frontend. A request deadline is authoritative; only requests without an
/// admission deadline use the bounded connector fallback.
pub fn connector_request_context_for_execution(
    query_options: Option<&QueryOptions>,
    execution: &novarocks_query_application::admitted_query_context::QueryExecutionContext,
) -> Result<ConnectorRequestContext, String> {
    let runtime = tokio::runtime::Handle::try_current()
        .map_err(|_| "connector query cancellation requires the FE runtime".to_string())?;
    connector_request_context_for_execution_on_runtime(query_options, execution, &runtime)
}

pub(crate) fn connector_request_context_for_execution_on_runtime(
    query_options: Option<&QueryOptions>,
    execution: &novarocks_query_application::admitted_query_context::QueryExecutionContext,
    runtime: &tokio::runtime::Handle,
) -> Result<ConnectorRequestContext, String> {
    let context = match execution.deadline() {
        Some(deadline) => query_connector_request_context_on_runtime(
            runtime,
            deadline,
            execution.cancellation().clone(),
        ),
        None => query_connector_request_context_on_runtime(
            runtime,
            Instant::now() + query_expire_duration(query_options),
            execution.cancellation().clone(),
        ),
    }?;
    #[cfg(novarocks_dev_controls)]
    if let Some(nomination) = execution.test_statement_nomination() {
        let actual = context.request_scope_extension_or_insert_with(|| nomination.as_ref().clone());
        if actual != *nomination {
            return Err("connector execution scope has a foreign statement nomination".into());
        }
    }
    Ok(context)
}

fn query_expire_duration(query_options: Option<&QueryOptions>) -> Duration {
    let default_timeout = 300i32;
    let query_timeout = query_options
        .and_then(|options| {
            (options.as_proto().query_timeout > 0).then_some(options.as_proto().query_timeout)
        })
        .unwrap_or(default_timeout)
        .max(1);
    Duration::from_secs(query_timeout as u64)
}

pub fn validate_request_context(context: &ConnectorRequestContext) -> Result<(), String> {
    if context.is_cancelled() {
        return Err("connector request was cancelled".to_string());
    }
    if Instant::now() >= context.deadline() {
        return Err("connector request deadline elapsed".to_string());
    }
    Ok(())
}

#[cfg(test)]
pub fn test_request_context() -> ConnectorRequestContext {
    connector_request_context(None, ConnectorStopOwner::new().view())
        .expect("test connector request context")
}

#[cfg(test)]
mod request_context_tests {
    use std::time::{Duration, Instant};

    use super::{connector_request_context_for_execution, query_expire_duration};
    use novarocks_query_application::admitted_query_context::{RequestAdmission, RequestContext};
    use novarocks_query_application::api::BackendTopologySnapshot;
    use novarocks_query_application::cancellation::{
        QueryCancellationReason, QueryCancellationSource,
    };
    use novarocks_sql::compiler::SessionOptimizerSettings;
    use novarocks_types::ClusterRole;

    #[tokio::test]
    async fn connector_context_preserves_admitted_deadline_and_cancellation() {
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
            novarocks_sql::sql_mode::SqlSemanticSettings::default(),
            novarocks_result_contract::ClientPresentationFamily::MysqlText,
        ));

        let connector = connector_request_context_for_execution(None, request.execution()).unwrap();
        assert_eq!(connector.deadline(), deadline);
        assert!(!connector.is_cancelled());
        let stop = connector.stop().clone();
        let stopped = stop.stopped();

        cancellation.request(QueryCancellationReason::ClientDisconnected);
        tokio::time::timeout(Duration::from_secs(1), stopped)
            .await
            .expect("query cancellation wakes connector stop");
        assert!(request.execution().cancellation().is_cancelled());
        assert!(connector.is_cancelled());
    }

    #[tokio::test]
    async fn connector_context_without_admitted_deadline_uses_bounded_fallback() {
        let request = RequestContext::admit(RequestAdmission::new(
            None,
            "db".to_string(),
            ClusterRole::Fe,
            BackendTopologySnapshot::empty(43),
            None,
            QueryCancellationSource::new().view(),
            SessionOptimizerSettings::default(),
            novarocks_sql::sql_mode::SqlSemanticSettings::default(),
            novarocks_result_contract::ClientPresentationFamily::MysqlText,
        ));
        let before = Instant::now();
        let connector = connector_request_context_for_execution(None, request.execution()).unwrap();
        assert!(connector.deadline() > before);
        assert!(!connector.stop().is_stopped());
    }

    #[tokio::test]
    async fn post_effect_context_keeps_query_stop_relay_alive() {
        let cancellation = QueryCancellationSource::new();
        let request = super::connector_request_context_for_query(None, cancellation.view())
            .expect("query connector context");
        let post_effect = request.after_external_effect();
        let stop = post_effect.stop().clone();

        cancellation.request(QueryCancellationReason::ClientDisconnected);
        tokio::time::timeout(Duration::from_secs(1), stop.stopped())
            .await
            .expect("post-effect request keeps cancellation relay alive");
        assert!(post_effect.is_cancelled());
    }

    #[tokio::test]
    async fn cloned_context_keeps_query_stop_relay_alive() {
        let cancellation = QueryCancellationSource::new();
        let request = super::connector_request_context_for_query(None, cancellation.view())
            .expect("query connector context");
        let retained = request.clone();
        drop(request);

        cancellation.request(QueryCancellationReason::ClientDisconnected);
        tokio::time::timeout(Duration::from_secs(1), retained.stop().stopped())
            .await
            .expect("cloned request keeps cancellation relay alive");
        assert!(retained.is_cancelled());
    }

    #[tokio::test]
    async fn pre_cancelled_query_installs_stopped_context_immediately() {
        let cancellation = QueryCancellationSource::new();
        cancellation.request(QueryCancellationReason::ClientDisconnected);

        let request = super::connector_request_context_for_query(None, cancellation.view())
            .expect("query connector context");
        assert!(request.is_cancelled());
        tokio::time::timeout(Duration::from_secs(1), request.stop().stopped())
            .await
            .expect("pre-cancelled query stop is immediately observable");
    }

    #[test]
    fn query_context_without_runtime_fails_explicitly() {
        let cancellation = QueryCancellationSource::new();
        let error = super::connector_request_context_for_query(None, cancellation.view())
            .err()
            .expect("query context must reject absent runtime");
        assert!(error.contains("requires the FE runtime"));
    }

    #[test]
    fn protocol_query_timeout_preserves_connector_deadline_defaults() {
        let unset = novarocks_proto_codec::lifecycle::QueryOptions::from_proto(
            novarocks_proto_models::novarocks::QueryOptions::default(),
        );
        let configured = novarocks_proto_codec::lifecycle::QueryOptions::from_proto(
            novarocks_proto_models::novarocks::QueryOptions {
                query_timeout: 17,
                ..Default::default()
            },
        );

        assert_eq!(query_expire_duration(None), Duration::from_secs(300));
        assert_eq!(
            query_expire_duration(Some(&unset)),
            Duration::from_secs(300)
        );
        assert_eq!(
            query_expire_duration(Some(&configured)),
            Duration::from_secs(17)
        );
    }
}

fn metadata_binding(
    controls: &dyn novarocks_spi::connector::ConnectorControlResolver,
    catalog: &str,
) -> Result<novarocks_spi::connector::ConnectorControlPlanningLease, String> {
    metadata_binding_typed(controls, catalog).map_err(|error| error.to_string())
}

/// Acquires one exact planning lease without erasing the connector's failure
/// category. Refresh scheduling uses this to distinguish a temporary catalog
/// outage from a definition that cannot be refreshed.
pub(crate) fn metadata_binding_typed(
    controls: &dyn novarocks_spi::connector::ConnectorControlResolver,
    catalog: &str,
) -> Result<novarocks_spi::connector::ConnectorControlPlanningLease, ConnectorError> {
    let instance_id = ConnectorInstanceId::parse(catalog).map_err(|error| {
        ConnectorError::new(ConnectorErrorKind::InvalidRequest, error.to_string())
    })?;
    controls.acquire_current(&instance_id)
}

/// Await namespace existence using the same exact planning generation.
pub(crate) async fn metadata_namespace_exists_async(
    controls: &dyn novarocks_spi::connector::ConnectorControlResolver,
    context: ConnectorRequestContext,
    catalog: &str,
    namespace: &str,
) -> Result<bool, String> {
    validate_request_context(&context)?;
    let binding = metadata_binding(controls, catalog)?;
    let instance_id = binding.binding().descriptor().instance_id.clone();
    binding
        .binding()
        .metadata()
        .namespace_exists_async(novarocks_spi::connector::ConnectorNamespaceRequest {
            namespace: novarocks_spi::connector::ConnectorNamespaceIdentity {
                instance_id,
                namespace: Arc::from(namespace),
            },
            context,
        })
        .await
        .map_err(|error| error.to_string())
}

/// Attach a query-attempt vended-credential collector to one exact catalog
/// generation. Every provider metadata entry point that can transitively load
/// a table must use this adapter; callers may not recreate an unbound context
/// after query admission.
pub(crate) fn context_for_planning_lease(
    binding: &novarocks_spi::connector::ConnectorControlPlanningLease,
    context: ConnectorRequestContext,
) -> Result<ConnectorRequestContext, String> {
    context_for_planning_lease_typed(binding, context).map_err(|error| error.to_string())
}

/// Attaches attempt-scoped credential collection while preserving connector
/// error classification for lifecycle callers.
pub(crate) fn context_for_planning_lease_typed(
    binding: &novarocks_spi::connector::ConnectorControlPlanningLease,
    context: ConnectorRequestContext,
) -> Result<ConnectorRequestContext, ConnectorError> {
    if context.vended_credential_lease_sink().is_some() {
        context
            .with_vended_credential_lease_collection(
                binding.binding().catalog_properties()?.clone(),
            )
            .map_err(|error| {
                ConnectorError::new(ConnectorErrorKind::InvalidRequest, error.to_string())
            })
    } else {
        Ok(context)
    }
}

/// Resolve table existence through an admission-frozen planning lease.  A
/// caller that performs a table-or-view decision must retain this lease for
/// every metadata lookup in that decision.
pub fn metadata_table_exists_with_planning_lease(
    binding: novarocks_spi::connector::ConnectorControlPlanningLease,
    context: ConnectorRequestContext,
    namespace: &str,
    table: &str,
) -> Result<bool, String> {
    let instance_id = binding.binding().descriptor().instance_id.clone();
    let context = context_for_planning_lease(&binding, context)?;
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

pub(crate) async fn metadata_table_exists_with_planning_lease_async(
    binding: novarocks_spi::connector::ConnectorControlPlanningLease,
    context: ConnectorRequestContext,
    namespace: &str,
    table: &str,
) -> Result<bool, String> {
    let instance_id = binding.binding().descriptor().instance_id.clone();
    let context = context_for_planning_lease(&binding, context)?;
    binding
        .binding()
        .metadata()
        .table_exists_async(ConnectorTableRequest {
            table: ConnectorTableIdentity {
                instance_id,
                namespace: Arc::from(namespace),
                table: Arc::from(table),
            },
            resolution: ConnectorTableResolution::StrictBaseTable,
            context,
        })
        .await
        .map_err(|error| error.to_string())
}

/// Enumerate namespaces through an admission-frozen connector control lease.
/// Ordering and duplicate handling stay application-owned so providers only
/// expose their authoritative catalog facts. The provider refuses a listing
/// that would exceed `bound`.
pub fn metadata_list_namespaces_with_planning_lease(
    binding: novarocks_spi::connector::ConnectorControlPlanningLease,
    context: ConnectorRequestContext,
    bound: novarocks_spi::connector::ConnectorListingBound,
) -> Result<Vec<ConnectorNamespaceIdentity>, String> {
    let instance_id = binding.binding().descriptor().instance_id.clone();
    let context = context_for_planning_lease(&binding, context)?;
    binding
        .binding()
        .metadata()
        .list_namespaces(ConnectorListNamespacesRequest {
            instance_id,
            bound,
            context,
        })
        .map_err(|error| error.to_string())
}

pub(crate) async fn metadata_list_namespaces_with_planning_lease_async(
    binding: novarocks_spi::connector::ConnectorControlPlanningLease,
    context: ConnectorRequestContext,
    bound: novarocks_spi::connector::ConnectorListingBound,
) -> Result<Vec<ConnectorNamespaceIdentity>, String> {
    let instance_id = binding.binding().descriptor().instance_id.clone();
    let context = context_for_planning_lease(&binding, context)?;
    binding
        .binding()
        .metadata()
        .list_namespaces_async(ConnectorListNamespacesRequest {
            instance_id,
            bound,
            context,
        })
        .await
        .map_err(|error| error.to_string())
}

/// Enumerate a namespace's tables through an admission-frozen connector control
/// lease.
///
/// This is what makes a namespace's contents knowable to SQL. Without it a
/// caller can only drop children it can already name, which is no help to
/// anyone who did not create them. The provider refuses a listing that would
/// exceed `bound`.
pub fn metadata_list_tables_with_planning_lease(
    binding: &novarocks_spi::connector::ConnectorControlPlanningLease,
    context: ConnectorRequestContext,
    namespace: &str,
    bound: novarocks_spi::connector::ConnectorListingBound,
) -> Result<Vec<String>, String> {
    let instance_id = binding.binding().descriptor().instance_id.clone();
    let context = context_for_planning_lease(binding, context)?;
    let mut tables = binding
        .binding()
        .metadata()
        .list_tables(novarocks_spi::connector::ConnectorListTablesRequest {
            namespace: novarocks_spi::connector::ConnectorNamespaceIdentity {
                instance_id,
                namespace: Arc::from(namespace),
            },
            bound,
            context,
        })
        .map_err(|error| error.to_string())?
        .into_iter()
        .map(|identity| identity.table.to_string())
        .collect::<Vec<_>>();
    tables.sort();
    tables.dedup();
    Ok(tables)
}

pub(crate) async fn metadata_list_tables_with_planning_lease_async(
    binding: &novarocks_spi::connector::ConnectorControlPlanningLease,
    context: ConnectorRequestContext,
    namespace: &str,
    bound: novarocks_spi::connector::ConnectorListingBound,
) -> Result<Vec<String>, String> {
    let instance_id = binding.binding().descriptor().instance_id.clone();
    let context = context_for_planning_lease(binding, context)?;
    let mut tables = binding
        .binding()
        .metadata()
        .list_tables_async(novarocks_spi::connector::ConnectorListTablesRequest {
            namespace: novarocks_spi::connector::ConnectorNamespaceIdentity {
                instance_id,
                namespace: Arc::from(namespace),
            },
            bound,
            context,
        })
        .await
        .map_err(|error| error.to_string())?
        .into_iter()
        .map(|identity| identity.table.to_string())
        .collect::<Vec<_>>();
    tables.sort();
    tables.dedup();
    Ok(tables)
}

/// Read immutable branch/tag/snapshot facts through the same exact lease that
/// admitted the table.  SQL owns the projection of these neutral facts.
pub fn metadata_read_reference_facts_with_planning_lease(
    binding: novarocks_spi::connector::ConnectorControlPlanningLease,
    context: ConnectorRequestContext,
    namespace: &str,
    table: &str,
) -> Result<ConnectorReadReferenceFacts, String> {
    let instance_id = binding.binding().descriptor().instance_id.clone();
    let context = context_for_planning_lease(&binding, context)?;
    binding
        .binding()
        .metadata()
        .read_reference_facts(ConnectorReadReferenceFactsRequest {
            table: ConnectorTableIdentity {
                instance_id,
                namespace: Arc::from(namespace),
                table: Arc::from(table),
            },
            context,
        })
        .map_err(|error| error.to_string())
}

pub(crate) async fn metadata_read_reference_facts_with_planning_lease_async(
    binding: novarocks_spi::connector::ConnectorControlPlanningLease,
    context: ConnectorRequestContext,
    namespace: &str,
    table: &str,
) -> Result<ConnectorReadReferenceFacts, String> {
    let instance_id = binding.binding().descriptor().instance_id.clone();
    let context = context_for_planning_lease(&binding, context)?;
    binding
        .binding()
        .metadata()
        .read_reference_facts_async(ConnectorReadReferenceFactsRequest {
            table: ConnectorTableIdentity {
                instance_id,
                namespace: Arc::from(namespace),
                table: Arc::from(table),
            },
            context,
        })
        .await
        .map_err(|error| error.to_string())
}

pub fn metadata_load_table(
    controls: &dyn novarocks_spi::connector::ConnectorControlResolver,
    context: ConnectorRequestContext,
    catalog: &str,
    namespace: &str,
    table: &str,
    resolution: ConnectorTableResolution,
) -> Result<(backend::ResolvedTable, Option<i32>), String> {
    let binding = metadata_binding(controls, catalog)?;
    metadata_load_table_with_planning_lease(binding, context, namespace, table, resolution)
}

/// Resolve metadata through an admission-frozen planning lease.  Write
/// callers use this instead of reopening `acquire_current` after target
/// admission.
pub fn metadata_load_table_with_planning_lease(
    binding: novarocks_spi::connector::ConnectorControlPlanningLease,
    context: ConnectorRequestContext,
    namespace: &str,
    table: &str,
    resolution: ConnectorTableResolution,
) -> Result<(backend::ResolvedTable, Option<i32>), String> {
    let instance_id = binding.binding().descriptor().instance_id.clone();
    let context = context_for_planning_lease(&binding, context)?;
    let metadata = binding
        .binding()
        .metadata()
        .load_frontend_table(ConnectorTableRequest {
            table: ConnectorTableIdentity {
                instance_id,
                namespace: Arc::from(namespace),
                table: Arc::from(table),
            },
            resolution,
            context,
        })
        .map_err(|error| error.to_string())?;
    let columns = sql_columns_from_connector_metadata(&metadata)?;
    let schema_id = metadata.version.as_ref().and_then(|version| {
        <[u8; 4]>::try_from(version.as_ref())
            .ok()
            .map(i32::from_le_bytes)
    });
    Ok((backend::ResolvedTable { columns }, schema_id))
}

/// Load the exact connector metadata admitted by a retained planning lease.
///
/// Consumers that need provider-owned interpretation must pass this value to
/// a composition-injected application port.  They must not decode the opaque
/// table handle or reopen the current connector generation themselves.
pub fn metadata_load_connector_table_with_planning_lease(
    binding: &novarocks_spi::connector::ConnectorControlPlanningLease,
    context: ConnectorRequestContext,
    namespace: &str,
    table: &str,
    resolution: ConnectorTableResolution,
) -> Result<novarocks_spi::connector::ConnectorTableMetadata, String> {
    metadata_load_connector_table_with_planning_lease_typed(
        binding, context, namespace, table, resolution,
    )
    .map_err(|error| error.to_string())
}

/// Typed counterpart of [`metadata_load_connector_table_with_planning_lease`]
/// for lifecycle code that must retain retry semantics.
pub(crate) fn metadata_load_connector_table_with_planning_lease_typed(
    binding: &novarocks_spi::connector::ConnectorControlPlanningLease,
    context: ConnectorRequestContext,
    namespace: &str,
    table: &str,
    resolution: ConnectorTableResolution,
) -> Result<novarocks_spi::connector::ConnectorTableMetadata, ConnectorError> {
    let instance_id = binding.binding().descriptor().instance_id.clone();
    let context = context_for_planning_lease_typed(binding, context)?;
    binding
        .binding()
        .metadata()
        .load_frontend_table(ConnectorTableRequest {
            table: ConnectorTableIdentity {
                instance_id,
                namespace: Arc::from(namespace),
                table: Arc::from(table),
            },
            resolution,
            context,
        })
}

pub(crate) fn sql_columns_from_connector_metadata(
    metadata: &novarocks_spi::connector::ConnectorTableMetadata,
) -> Result<Vec<novarocks_types::schema::ColumnDef>, String> {
    use novarocks_spi::connector::ConnectorTableColumnVisibility;
    metadata
        .schema()
        .fields()
        .iter()
        .enumerate()
        .filter(|(ordinal, _)| {
            !matches!(
                metadata
                    .planning_facts
                    .column_facts()
                    .get(*ordinal)
                    .map(|fact| fact.visibility()),
                Some(ConnectorTableColumnVisibility::Hidden)
            )
        })
        .map(|(ordinal, field)| {
            let binding = metadata
                .execution_schema()
                .binding(ordinal)
                .ok_or_else(|| {
                    "catalog execution schema is missing an authored column".to_owned()
                })?;
            sql_column_from_connector_binding(
                field.name(),
                binding,
                &metadata.planning_facts,
                ordinal,
            )
        })
        .collect()
}

/// Project the provider's complete value contract to its declared write layout.
/// A physical target override changes representation and preserves every
/// semantic child; it cannot infer or erase a domain from format annotations.
pub(crate) fn connector_write_column(
    metadata: &novarocks_spi::connector::ConnectorTableMetadata,
    ordinal: usize,
    write_default: Option<novarocks_types::schema::ColumnDefault>,
) -> Result<novarocks_types::schema::ColumnDef, String> {
    let field = metadata
        .schema()
        .fields()
        .get(ordinal)
        .ok_or_else(|| "connector write column is outside its source schema".to_string())?;
    let binding = metadata
        .execution_schema()
        .binding(ordinal)
        .ok_or_else(|| "connector write column has no complete execution binding".to_string())?;
    let value_type = if let Some(target) = metadata
        .planning_facts
        .column_facts()
        .get(ordinal)
        .and_then(|facts| facts.write_target_type())
    {
        let layout = novarocks_type_contract::ValueLayout::try_new(
            target,
            novarocks_type_contract::ValueTypeStorageLifetime::pure(),
        )
        .map_err(|error| format!("invalid connector write layout: {error}"))?;
        novarocks_type_contract::FunctionValueType::try_from_value(
            binding.value_type().clone(),
            layout,
        )
        .map_err(|error| {
            format!("connector write layout conflicts with its value contract: {error}")
        })?
    } else {
        binding.clone()
    };
    novarocks_types::schema::ColumnDef::from_value_type(
        field.name().clone(),
        value_type,
        write_default,
    )
    .map_err(|error| format!("invalid connector write column `{}`: {error}", field.name()))
}

/// Materialize SQL facts from the provider's complete value declaration. The
/// source Arrow schema is never a logical-domain author at this boundary.
pub(crate) fn sql_column_from_connector_binding(
    name: &str,
    binding: &novarocks_type_contract::FunctionValueType,
    planning_facts: &novarocks_spi::connector::ConnectorTablePlanningFacts,
    ordinal: usize,
) -> Result<novarocks_types::schema::ColumnDef, String> {
    use novarocks_spi::connector::ConnectorTableColumnSemanticKind;
    use novarocks_type_contract::ValueLogicalType;
    let declared = match planning_facts
        .column_facts()
        .get(ordinal)
        .map(|fact| fact.semantic_kind())
    {
        Some(ConnectorTableColumnSemanticKind::Bitmap) => Some(ValueLogicalType::Bitmap),
        Some(ConnectorTableColumnSemanticKind::Hll) => Some(ValueLogicalType::Hll),
        None | Some(ConnectorTableColumnSemanticKind::None) => None,
        Some(_) => return Err("unsupported connector column semantic fact".into()),
    };
    if let Some(domain) = declared
        && binding.logical_type() != domain
    {
        return Err(format!(
            "connector column `{name}` value declaration conflicts with its semantic fact"
        ));
    }
    novarocks_types::schema::ColumnDef::from_value_type(
        name.to_owned(),
        binding.clone(),
        connector_write_default_at(planning_facts, ordinal),
    )
    .map_err(|error| error.to_string())
}

/// Read the write default a provider published for one frozen schema ordinal.
///
/// Facts are optional by contract: a provider with no column defaults returns
/// empty facts, and the column then behaves exactly as it did before defaults
/// were expressible.
pub fn connector_write_default_at(
    planning_facts: &novarocks_spi::connector::ConnectorTablePlanningFacts,
    ordinal: usize,
) -> Option<novarocks_types::schema::ColumnDefault> {
    planning_facts
        .column_facts()
        .get(ordinal)
        .and_then(|fact| fact.write_default())
        .map(connector_default_to_column_default)
}

/// Project the sealed SPI default value onto the neutral catalog value.
///
/// The two vocabularies are variant-for-variant identical; they are separate
/// types only because the SPI dependency ceiling admits no application value
/// crate.
pub fn connector_default_to_column_default(
    value: &novarocks_spi::connector::ConnectorColumnDefault,
) -> novarocks_types::schema::ColumnDefault {
    use novarocks_spi::connector::ConnectorColumnDefault as Spi;
    use novarocks_types::schema::ColumnDefault;

    match value {
        Spi::Null => ColumnDefault::Null,
        Spi::Boolean(value) => ColumnDefault::Boolean(*value),
        Spi::Int32(value) => ColumnDefault::Int32(*value),
        Spi::Int64(value) => ColumnDefault::Int64(*value),
        Spi::Float32 { bits } => ColumnDefault::Float32 { bits: *bits },
        Spi::Float64 { bits } => ColumnDefault::Float64 { bits: *bits },
        Spi::Decimal {
            unscaled,
            precision,
            scale,
        } => ColumnDefault::Decimal {
            unscaled: *unscaled,
            precision: *precision,
            scale: *scale,
        },
        Spi::String(text) => ColumnDefault::String(text.to_string()),
        Spi::Binary(bytes) => ColumnDefault::Binary(bytes.to_vec()),
        Spi::Date { days_since_epoch } => ColumnDefault::Date {
            days_since_epoch: *days_since_epoch,
        },
        Spi::TimeMicros {
            micros_since_midnight,
        } => ColumnDefault::TimeMicros {
            micros_since_midnight: *micros_since_midnight,
        },
        Spi::TimestampMicros { micros_since_epoch } => ColumnDefault::TimestampMicros {
            micros_since_epoch: *micros_since_epoch,
        },
        Spi::TimestamptzMicros { micros_since_epoch } => ColumnDefault::TimestamptzMicros {
            micros_since_epoch: *micros_since_epoch,
        },
        Spi::TimestampNanos { nanos_since_epoch } => ColumnDefault::TimestampNanos {
            nanos_since_epoch: *nanos_since_epoch,
        },
        Spi::TimestamptzNanos { nanos_since_epoch } => ColumnDefault::TimestamptzNanos {
            nanos_since_epoch: *nanos_since_epoch,
        },
        Spi::Uuid(bytes) => ColumnDefault::Uuid(*bytes),
        Spi::Fixed { size, bytes } => ColumnDefault::Fixed {
            size: *size,
            bytes: bytes.to_vec(),
        },
        Spi::Struct(fields) => ColumnDefault::Struct(
            fields
                .iter()
                .map(|(name, field_value)| {
                    (
                        name.to_string(),
                        connector_default_to_column_default(field_value),
                    )
                })
                .collect(),
        ),
        Spi::Array(elements) => ColumnDefault::Array(
            elements
                .iter()
                .map(connector_default_to_column_default)
                .collect(),
        ),
        Spi::Map(entries) => ColumnDefault::Map(
            entries
                .iter()
                .map(|(key, entry_value)| {
                    (
                        connector_default_to_column_default(key),
                        connector_default_to_column_default(entry_value),
                    )
                })
                .collect(),
        ),
    }
}

pub fn acquire_metadata_planning_lease(
    controls: &dyn novarocks_spi::connector::ConnectorControlResolver,
    catalog: &str,
) -> Result<novarocks_spi::connector::ConnectorControlPlanningLease, String> {
    metadata_binding(controls, catalog)
}

#[cfg(test)]
mod source_value_type_tests {
    use super::{
        sql_column_from_connector_binding, sql_columns_from_connector_metadata,
        test_request_context,
    };
    use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
    use novarocks_spi::connector::{
        ConnectorTableColumnPlanningFact, ConnectorTableColumnRole,
        ConnectorTableColumnSemanticKind as K, ConnectorTableColumnVisibility as V,
        ConnectorTablePlanningFacts,
    };
    use novarocks_type_contract::{
        FunctionValueType, ValueField, ValueKind, ValueLayout, ValueLogicalType as L, ValueTypeRef,
        ValueTypeStorageLifetime as S,
    };
    use novarocks_types::schema::{ColumnDefault, SqlType};
    use std::sync::Arc;

    fn facts(schema: &SchemaRef, kinds: &[K]) -> ConnectorTablePlanningFacts {
        ConnectorTablePlanningFacts::try_new(
            schema,
            kinds
                .iter()
                .enumerate()
                .map(|(ordinal, kind)| {
                    ConnectorTableColumnPlanningFact::new(
                        ordinal as u32,
                        V::Sql,
                        *kind,
                        ConnectorTableColumnRole::Ordinary,
                    )
                })
                .collect(),
            vec![],
            vec![],
            vec![],
            &test_request_context(),
        )
        .unwrap()
    }

    #[test]
    fn authored_uuid_and_variant_field_roots_reach_exact_catalog_value_types() {
        for (carrier, domain, declaration) in [
            (DataType::FixedSizeBinary(16), L::Uuid, SqlType::Uuid),
            (DataType::LargeBinary, L::Variant, SqlType::Variant),
            (DataType::Binary, L::Physical, SqlType::Binary),
        ] {
            let binding = FunctionValueType::try_with_logical_type(carrier, true, domain).unwrap();
            let column = sql_column_from_connector_binding(
                "value",
                &binding,
                &ConnectorTablePlanningFacts::empty(),
                0,
            )
            .unwrap();
            assert_eq!(column.declared_value_type().unwrap(), binding);
            assert_eq!(column.logical_type, Some(declaration));
        }
    }

    #[test]
    fn complete_nested_provider_declaration_preserves_domains_and_normalized_dictionary_layout() {
        let uuid = ValueTypeRef::try_new(ValueKind::Nominal(L::Uuid), false, S::pure()).unwrap();
        let variant =
            ValueTypeRef::try_new(ValueKind::Nominal(L::Variant), true, S::pure()).unwrap();
        let list = ValueTypeRef::try_new(ValueKind::List(variant), false, S::pure()).unwrap();
        let string = ValueTypeRef::try_new(ValueKind::String, true, S::pure()).unwrap();
        let declaration = ValueTypeRef::try_new(
            ValueKind::Struct(
                vec![
                    ValueField::new("u", uuid, S::pure()),
                    ValueField::new("items", list, S::pure()),
                    ValueField::new("codes", string, S::pure()),
                ]
                .into_boxed_slice(),
            ),
            true,
            S::pure(),
        )
        .unwrap();
        let source = DataType::Struct(
            vec![
                Field::new("u", DataType::FixedSizeBinary(16), false).with_metadata(
                    std::collections::HashMap::<String, String>::from([(
                        "nr_logical_type".into(),
                        "unknown-source-attribute".into(),
                    )]),
                ),
                Field::new(
                    "items",
                    DataType::List(Arc::new(Field::new(
                        "source_item",
                        DataType::LargeBinary,
                        true,
                    ))),
                    false,
                ),
                Field::new(
                    "codes",
                    DataType::Dictionary(Box::new(DataType::Int16), Box::new(DataType::Utf8)),
                    true,
                ),
            ]
            .into(),
        );
        let ty = FunctionValueType::try_from_value(
            declaration,
            ValueLayout::try_new(&source, S::pure()).unwrap(),
        )
        .unwrap();
        let column = sql_column_from_connector_binding(
            "record",
            &ty,
            &ConnectorTablePlanningFacts::empty(),
            0,
        )
        .unwrap();
        assert_eq!(column.declared_value_type().unwrap(), ty);
        assert_eq!(ty.child_at(0).unwrap().logical_type(), L::Uuid);
        assert_eq!(
            ty.child_at(1).unwrap().child_at(0).unwrap().logical_type(),
            L::Variant
        );
        assert_eq!(ty.child_at(2).unwrap().data_type(), &DataType::Utf8);
    }

    #[test]
    fn checked_provider_declaration_refuses_wrong_carriers() {
        for (carrier, domain) in [
            (DataType::FixedSizeBinary(15), L::Uuid),
            (DataType::Utf8, L::Uuid),
            (DataType::Binary, L::Variant),
        ] {
            assert!(FunctionValueType::try_with_logical_type(carrier, true, domain).is_err());
        }
    }

    #[test]
    fn independent_bitmap_hll_facts_require_the_complete_declaration_to_agree() {
        for (kind, logical, other) in [(K::Bitmap, L::Bitmap, L::Hll), (K::Hll, L::Hll, L::Bitmap)]
        {
            let source = Arc::new(Schema::new(vec![Field::new(
                "blob",
                DataType::Binary,
                true,
            )]));
            let signed = facts(&source, &[kind]);
            let matching =
                FunctionValueType::try_with_logical_type(DataType::Binary, true, logical).unwrap();
            assert_eq!(
                sql_column_from_connector_binding("blob", &matching, &signed, 0)
                    .unwrap()
                    .declared_value_type()
                    .unwrap(),
                matching
            );
            let mismatch =
                FunctionValueType::try_with_logical_type(DataType::Binary, true, other).unwrap();
            assert!(sql_column_from_connector_binding("blob", &mismatch, &signed, 0).is_err());
            assert!(
                sql_column_from_connector_binding(
                    "blob",
                    &FunctionValueType::new(DataType::Binary, true),
                    &signed,
                    0
                )
                .is_err()
            );
        }
    }

    #[test]
    fn hidden_fields_leave_default_facts_at_their_original_schema_ordinals() {
        use novarocks_spi::connector::*;
        let source = Arc::new(Schema::new(vec![
            Field::new("left", DataType::Int64, false),
            Field::new("hidden", DataType::Int64, false),
            Field::new("right", DataType::FixedSizeBinary(16), true),
        ]));
        let uuid = [0x5a; 16];
        let declarations = [
            (V::Sql, ConnectorColumnDefault::Int64(11)),
            (V::Hidden, ConnectorColumnDefault::Int64(99)),
            (V::Sql, ConnectorColumnDefault::Uuid(uuid)),
        ]
        .into_iter()
        .enumerate()
        .map(|(ordinal, (visibility, default))| {
            ConnectorTableColumnPlanningFact::new(
                ordinal as u32,
                visibility,
                K::None,
                ConnectorTableColumnRole::Ordinary,
            )
            .with_write_default(Some(default))
        })
        .collect();
        let planning = ConnectorTablePlanningFacts::try_new(
            &source,
            declarations,
            vec![],
            vec![],
            vec![],
            &test_request_context(),
        )
        .unwrap();
        let instance = ConnectorInstanceId::parse("test").unwrap();
        let metadata = crate::connector::schema_creation::declared_test_metadata(
            ConnectorTableMetadataHeader {
                identity: ConnectorTableIdentity {
                    instance_id: instance.clone(),
                    namespace: Arc::from("db"),
                    table: Arc::from("source"),
                },
                planning_facts: planning,
                definition_facts: ConnectorTableDefinitionFacts::empty(),
                version: None,
                statistics_data_version: None,
                table: ConnectorTableHandle::try_new(instance, bytes::Bytes::from_static(b"table"))
                    .unwrap(),
            },
            source,
            vec![
                FunctionValueType::new(DataType::Int64, false),
                FunctionValueType::new(DataType::Int64, false),
                FunctionValueType::try_with_logical_type(
                    DataType::FixedSizeBinary(16),
                    true,
                    L::Uuid,
                )
                .unwrap(),
            ],
        );
        let columns = sql_columns_from_connector_metadata(&metadata).unwrap();
        assert_eq!(columns.len(), 2);
        assert_eq!(columns[0].write_default, Some(ColumnDefault::Int64(11)));
        assert_eq!(columns[1].write_default, Some(ColumnDefault::Uuid(uuid)));
        assert_eq!(
            columns[1].declared_value_type().unwrap().logical_type(),
            L::Uuid
        );
    }

    #[test]
    fn column_names_never_author_domains() {
        for (name, carrier) in [
            ("variant", DataType::LargeBinary),
            ("hll", DataType::Binary),
            ("uuid", DataType::FixedSizeBinary(16)),
        ] {
            let physical = FunctionValueType::new(carrier, true);
            let column = sql_column_from_connector_binding(
                name,
                &physical,
                &ConnectorTablePlanningFacts::empty(),
                0,
            )
            .unwrap();
            let declared = column.declared_value_type().unwrap();
            assert_eq!(declared, physical);
            assert_eq!(declared.logical_type(), L::Physical);
            assert_eq!(declared.data_type(), physical.data_type());
            assert!(declared.nullable());
            assert_eq!(column.logical_type, Some(SqlType::Binary));
        }
    }
}
