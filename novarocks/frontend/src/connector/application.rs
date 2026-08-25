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
use novarocks_proto::lifecycle::QueryOptions;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use novarocks_spi::connector::{
    ConnectorCancellation, ConnectorInstanceId, ConnectorListNamespacesRequest,
    ConnectorNamespaceIdentity, ConnectorReadReferenceFacts, ConnectorReadReferenceFactsRequest,
    ConnectorRequestContext, ConnectorTableIdentity, ConnectorTableRequest,
    ConnectorTableResolution,
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
    cancellation: crate::common::query_cancellation::QueryCancellationView,
}

impl ConnectorCancellation for QueryConnectorCancellation {
    fn is_cancelled(&self) -> bool {
        self.cancellation.is_cancelled()
    }
}

fn build_connector_request_context(
    query_options: Option<&QueryOptions>,
    cancellation: Arc<dyn ConnectorCancellation>,
) -> Result<ConnectorRequestContext, String> {
    let query_expire = query_expire_duration(query_options);
    ConnectorRequestContext::try_new(
        Instant::now() + query_expire,
        cancellation,
        novarocks_spi::connector::MAX_CONNECTOR_HANDLE_PAYLOAD_BYTES,
        novarocks_spi::connector::MAX_CONNECTOR_TOTAL_PAYLOAD_BYTES,
    )
    .map_err(|error| error.to_string())
}

pub fn connector_request_context(
    query_options: Option<&QueryOptions>,
    cancellation_signal: Arc<AtomicBool>,
) -> Result<ConnectorRequestContext, String> {
    build_connector_request_context(
        query_options,
        Arc::new(RequestConnectorCancellation {
            signal: cancellation_signal,
        }),
    )
}

/// Freeze connector request facts from the admitted frontend statement.
///
/// Frontend-owned typed command capabilities use this same constructor so
/// provider requests share the statement cancellation identity and options.
pub fn connector_request_context_for_query(
    query_options: Option<&QueryOptions>,
    cancellation: crate::common::query_cancellation::QueryCancellationView,
) -> Result<ConnectorRequestContext, String> {
    build_connector_request_context(
        query_options,
        Arc::new(QueryConnectorCancellation { cancellation }),
    )
}

/// Derive connector admission from the immutable query execution captured by
/// the frontend. A request deadline is authoritative; only requests without an
/// admission deadline use the bounded connector fallback.
pub fn connector_request_context_for_execution(
    query_options: Option<&QueryOptions>,
    execution: &crate::common::admitted_query_context::QueryExecutionContext,
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
    if context.cancellation().is_cancelled() {
        return Err("connector request was cancelled".to_string());
    }
    if Instant::now() >= context.deadline() {
        return Err("connector request deadline elapsed".to_string());
    }
    Ok(())
}

#[cfg(test)]
pub fn test_request_context() -> ConnectorRequestContext {
    connector_request_context(None, Arc::new(AtomicBool::new(false)))
        .expect("test connector request context")
}

#[cfg(test)]
#[expect(
    clippy::items_after_test_module,
    reason = "The request-context tests remain adjacent to their test-only factory."
)]
mod request_context_tests {
    use std::time::{Duration, Instant};

    use super::{connector_request_context_for_execution, query_expire_duration};
    use crate::common::admitted_query_context::{RequestAdmission, RequestContext};
    use crate::common::backend_topology::BackendTopologySnapshot;
    use crate::common::query_cancellation::{QueryCancellationReason, QueryCancellationSource};
    use novarocks_sql::compiler::SessionOptimizerSettings;
    use novarocks_types::ClusterRole;

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

    #[test]
    fn protocol_query_timeout_preserves_connector_deadline_defaults() {
        let unset = novarocks_proto::lifecycle::QueryOptions::parse(
            novarocks_proto::novarocks::QueryOptions::default(),
        )
        .expect("default protocol query options are valid");
        let configured = novarocks_proto::lifecycle::QueryOptions::parse(
            novarocks_proto::novarocks::QueryOptions {
                query_timeout: 17,
                ..Default::default()
            },
        )
        .expect("configured protocol query options are valid");

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
    let instance_id = ConnectorInstanceId::parse(catalog).map_err(|error| error.to_string())?;
    controls
        .acquire_current(&instance_id)
        .map_err(|error| error.to_string())
}

pub fn metadata_namespace_exists(
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

/// Enumerate namespaces through an admission-frozen connector control lease.
/// Ordering and duplicate handling stay application-owned so providers only
/// expose their authoritative catalog facts.
pub fn metadata_list_namespaces_with_planning_lease(
    binding: novarocks_spi::connector::ConnectorControlPlanningLease,
    context: ConnectorRequestContext,
) -> Result<Vec<ConnectorNamespaceIdentity>, String> {
    let instance_id = binding.binding().descriptor().instance_id.clone();
    binding
        .binding()
        .metadata()
        .list_namespaces(ConnectorListNamespacesRequest {
            instance_id,
            context,
        })
        .map_err(|error| error.to_string())
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
    let columns = sql_columns_from_connector_schema(&metadata.schema, &metadata.planning_facts);
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
    let instance_id = binding.binding().descriptor().instance_id.clone();
    binding
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
        .map_err(|error| error.to_string())
}

pub(crate) fn sql_columns_from_connector_schema(
    schema: &arrow::datatypes::Schema,
    planning_facts: &novarocks_spi::connector::ConnectorTablePlanningFacts,
) -> Vec<novarocks_types::schema::ColumnDef> {
    schema
        .fields()
        .iter()
        .enumerate()
        // The ordinal must index the frozen schema, not this filtered view:
        // planning facts are aligned to the schema Core received, and hidden
        // fields keep their position in it.
        .filter(|(_, field)| {
            field
                .metadata()
                .get(novarocks_spi::connector::CONNECTOR_FIELD_HIDDEN_FROM_SQL)
                .is_none_or(|value| !value.eq_ignore_ascii_case("true"))
        })
        .map(|(ordinal, field)| novarocks_types::schema::ColumnDef {
            name: field.name().clone(),
            data_type: field.data_type().clone(),
            nullable: field.is_nullable(),
            write_default: connector_write_default_at(planning_facts, ordinal),
            logical_type: None,
        })
        .collect()
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
