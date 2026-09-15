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

//! DDL/DML statement handlers for the standalone engine.
//!
//! Top-level dispatchers route statement families that remain in the core
//! command kernel to connector-owned catalogs based on the parsed name and
//! current catalog/database session context.

use std::sync::Arc;

use crate::catalog_application::model::{CatalogCreateTableKind, CatalogCreateTableRequest};
use crate::catalog_application::query_catalog::drop_local_table_registration_if_exists;
use bytes::Bytes;
use novarocks_query_application::protocol_delivery::QuerySessionOutput as StatementResult;
use novarocks_spi::connector::ConnectorControlRegistry;
use novarocks_spi::connector::{
    ConnectorCatalogMutationOperation, ConnectorColumnAggregation, ConnectorColumnDefinition,
    ConnectorColumnPath, ConnectorColumnPosition, ConnectorDataType, ConnectorDefaultValue,
    ConnectorDropTableDataDisposition, ConnectorErrorKind, ConnectorInstanceId,
    ConnectorNamespaceIdentity, ConnectorPartitionTransform, ConnectorTableIdentity,
    ConnectorTableKey, ConnectorTableKeyKind, ConnectorViewIdentity, ConnectorViewRequest,
    CreatePolicy, DropPolicy,
};
use novarocks_sql::literal::{parse_date_string_to_days, parse_datetime_string_to_micros};
use novarocks_sql::semantic::command::{
    ColumnPositionSql, CommandLiteral, CreateTableSqlCommand, IcebergColumnSqlAction,
    IcebergPartitionSqlChange, IcebergPropertiesSqlAction, IcebergSchemaSqlChange,
    TablePartitionSqlCommand,
};
use novarocks_sql::semantic::{
    ColumnAggregation, DefaultLiteral, IcebergPartitionFieldExpr, ObjectName, TableColumnDef,
    TableKeyDesc, TableKeyKind,
};
use novarocks_types::naming::{normalize_identifier, resolve_local_table_name};
use novarocks_types::schema::SqlType;

/// Exact dependencies needed by catalog-drop statements.
///
/// This deliberately does not expose the standalone application aggregate:
/// catalog DDL needs only catalog admission, exact-generation connector
/// control, local catalog invalidation, MV guards, and view metadata lookup.
pub trait CatalogDropContext:
    crate::catalog_application::resolver::CatalogAdmission
    + crate::catalog_application::query_catalog::CatalogServiceSource
{
    fn connector_control(&self) -> &dyn ConnectorControlRegistry;
    fn mv_readiness(&self) -> &crate::mv::domain::readiness::MvReadinessPort;
    fn mv_storage_observation(&self) -> &dyn novarocks_spi::connector::MvStorageObservationPort;
}

// ---------------------------------------------------------------------------
// DDL handlers
// ---------------------------------------------------------------------------

/// The narrow catalog mutation surface shared by the legacy engine and the
/// explicit catalog command kernel.  Keep statement helpers on this port so
/// command routing cannot recover an application facade just to resolve a
/// catalog target or issue a provider-owned mutation.
pub trait CatalogMutationContext: crate::catalog_application::resolver::CatalogAdmission {
    fn connector_control(&self) -> &dyn ConnectorControlRegistry;
}

pub(crate) fn execute_create_database_statement(
    context: &impl CatalogMutationContext,
    name: &ObjectName,
    if_not_exists: bool,
    current_catalog: Option<&str>,
    connector_context: &novarocks_spi::connector::ConnectorRequestContext,
) -> Result<StatementResult, String> {
    let target = crate::catalog_application::resolver::resolve_namespace_target(
        context,
        name,
        current_catalog,
    )?;
    let instance_id = mutation_instance_id(&target.catalog)?;
    crate::connector::mutation::execute_catalog_mutation(
        context.connector_control(),
        &instance_id,
        ConnectorCatalogMutationOperation::CreateNamespace {
            namespace: ConnectorNamespaceIdentity {
                instance_id: instance_id.clone(),
                namespace: Arc::from(target.namespace),
            },
            policy: if if_not_exists {
                CreatePolicy::NoOpIfExists
            } else {
                CreatePolicy::FailIfExists
            },
        },
        connector_context.clone(),
    )?;
    Ok(StatementResult::Ok)
}

pub(crate) fn execute_create_table_statement(
    context: &impl CatalogMutationContext,
    stmt: CatalogCreateTableRequest,
    current_catalog: Option<&str>,
    current_database: &str,
    connector_context: &novarocks_spi::connector::ConnectorRequestContext,
) -> Result<StatementResult, String> {
    match stmt.kind {
        CatalogCreateTableKind::Iceberg {
            columns,
            key_desc,
            bucket_count,
            distribution_columns,
            partition_fields,
            properties,
        } => {
            // BITMAP / HLL columns cannot be used as distribution keys —
            // they are opaque blobs with no hash semantics that match a
            // scalar column. Reject the CREATE TABLE before any catalog
            // mutation. Column names are case-insensitive in StarRocks.
            for dist_col in &distribution_columns {
                let dist_lower = dist_col.to_ascii_lowercase();
                if let Some(column) = columns
                    .iter()
                    .find(|c| c.name.eq_ignore_ascii_case(&dist_lower))
                    && matches!(
                        column.data_type,
                        novarocks_types::schema::SqlType::Bitmap
                            | novarocks_types::schema::SqlType::Hll
                    )
                {
                    return Err(format!(
                        "BITMAP/HLL columns cannot be used as distribution key (column `{}` has type {:?})",
                        column.name, column.data_type
                    ));
                }
            }
            // This validation must precede the connector mutation dispatcher:
            // its reconciliation path may inspect a not-yet-created table,
            // whereas an invalid partition source is a deterministic statement
            // error independent of catalog state.
            for partition_field in &partition_fields {
                let source_column = match partition_field {
                    IcebergPartitionFieldExpr::Identity { column }
                    | IcebergPartitionFieldExpr::Year { column }
                    | IcebergPartitionFieldExpr::Month { column }
                    | IcebergPartitionFieldExpr::Day { column }
                    | IcebergPartitionFieldExpr::Hour { column }
                    | IcebergPartitionFieldExpr::Bucket { column, .. }
                    | IcebergPartitionFieldExpr::Truncate { column, .. }
                    | IcebergPartitionFieldExpr::Void { column } => column,
                };
                if let Some(column) = columns
                    .iter()
                    .find(|column| column.name.eq_ignore_ascii_case(source_column))
                    && matches!(column.data_type, novarocks_types::schema::SqlType::Variant)
                {
                    return Err(format!(
                        "iceberg table column `{}` is variant; variant columns cannot appear in the partition spec. Use a non-variant source column for partition transforms.",
                        column.name
                    ));
                }
            }

            let target = crate::catalog_application::resolver::resolve_table_target(
                context,
                &stmt.name,
                current_catalog,
                current_database,
            )?;
            let instance_id = mutation_instance_id(&target.catalog)?;
            let _ = bucket_count;
            crate::connector::mutation::execute_catalog_mutation(
                context.connector_control(),
                &instance_id,
                ConnectorCatalogMutationOperation::CreateTable {
                    table: ConnectorTableIdentity {
                        instance_id: instance_id.clone(),
                        namespace: Arc::from(target.namespace),
                        table: Arc::from(target.table),
                    },
                    columns: columns
                        .iter()
                        .map(connector_column)
                        .collect::<Result<_, _>>()?,
                    key: key_desc.as_ref().map(connector_table_key),
                    partitioning: partition_fields
                        .iter()
                        .map(connector_partition_transform)
                        .collect(),
                    properties: properties
                        .into_iter()
                        .map(|(key, value)| (Arc::from(key), Arc::from(value)))
                        .collect(),
                    policy: if stmt.if_not_exists {
                        CreatePolicy::NoOpIfExists
                    } else {
                        CreatePolicy::FailIfExists
                    },
                },
                connector_context.clone(),
            )?;
            Ok(StatementResult::Ok)
        }
    }
}

/// Lowers a Query-Application `CREATE TABLE` command to the catalog product
/// request. The input is a closed semantic value, so source syntax cannot
/// cross this application boundary or be reconstructed here.
pub(crate) fn lower_semantic_create_table_statement(
    statement: &CreateTableSqlCommand,
) -> Result<CatalogCreateTableRequest, String> {
    if statement.temporary || statement.external {
        return Err("CREATE TABLE does not support TEMPORARY or EXTERNAL tables".to_string());
    }
    if let Some(engine) = &statement.engine
        && !engine.eq_ignore_ascii_case("iceberg")
    {
        return Err(format!("CREATE TABLE does not support ENGINE = {engine}"));
    }
    if statement.like.is_some() {
        return Err("CREATE TABLE LIKE must use the semantic LIKE executor".to_string());
    }
    if !statement.order_by.is_empty() {
        return Err("CREATE TABLE does not support ORDER BY".to_string());
    }
    let partition_fields = match &statement.partition {
        None => Vec::new(),
        Some(TablePartitionSqlCommand::Transform(partition)) => partition.clone(),
        Some(TablePartitionSqlCommand::UnsupportedLegacyRange { .. }) => {
            return Err(
                "CREATE TABLE does not support legacy RANGE partition definitions".to_string(),
            );
        }
    };
    let mut properties = statement
        .properties
        .iter()
        .map(|property| {
            Ok((
                semantic_table_property_text(&property.key)?,
                semantic_table_property_text(&property.value)?,
            ))
        })
        .collect::<Result<Vec<_>, String>>()?;
    if let Some(comment) = &statement.comment {
        properties.push((
            "comment".to_string(),
            semantic_table_property_text(comment)?,
        ));
    }
    Ok(CatalogCreateTableRequest {
        name: statement.name.clone(),
        kind: CatalogCreateTableKind::Iceberg {
            columns: statement
                .columns
                .iter()
                .map(|column| {
                    Ok(TableColumnDef {
                        name: column.name.clone(),
                        nullable: column.nullable.unwrap_or(true),
                        aggregation: column.aggregation,
                        default: column
                            .default
                            .as_ref()
                            .map(|value| lower_semantic_default_literal(value, &column.data_type))
                            .transpose()?,
                        data_type: column.data_type.clone(),
                    })
                })
                .collect::<Result<Vec<_>, String>>()?,
            key_desc: statement.key.clone(),
            bucket_count: statement
                .distribution
                .as_ref()
                .and_then(|value| value.buckets)
                .map(|value| {
                    u32::try_from(value)
                        .map_err(|_| "distribution bucket count exceeds u32".to_string())
                })
                .transpose()?,
            distribution_columns: statement
                .distribution
                .as_ref()
                .map(|value| value.columns.clone())
                .unwrap_or_default(),
            partition_fields,
            properties,
        },
        if_not_exists: statement.if_not_exists,
    })
}

fn semantic_table_property_text(literal: &CommandLiteral) -> Result<String, String> {
    match literal {
        CommandLiteral::Null => Err("table properties do not support NULL values".to_string()),
        CommandLiteral::Bool(value) => Ok(value.to_string()),
        CommandLiteral::Number(value) | CommandLiteral::String(value) => Ok(value.clone()),
        CommandLiteral::Binary(value) => Ok(format!("0x{}", hex::encode(value))),
    }
}

fn lower_semantic_default_literal(
    literal: &CommandLiteral,
    data_type: &SqlType,
) -> Result<DefaultLiteral, String> {
    let lowered = match literal {
        CommandLiteral::Null => DefaultLiteral::Null,
        CommandLiteral::Bool(value) => DefaultLiteral::Bool(*value),
        CommandLiteral::Number(value) => lower_typed_numeric_default(value, data_type)?,
        CommandLiteral::String(value) => lower_typed_string_default(value, data_type)?,
        CommandLiteral::Binary(value) => {
            if !matches!(data_type, SqlType::Binary) {
                return Err(format!(
                    "hex DEFAULT not supported for column type {data_type:?}"
                ));
            }
            DefaultLiteral::Binary(value.clone())
        }
    };
    validate_typed_default_literal(&lowered, data_type)?;
    Ok(lowered)
}

fn mutation_instance_id(catalog: &str) -> Result<ConnectorInstanceId, String> {
    ConnectorInstanceId::parse(catalog).map_err(|error| error.to_string())
}

pub fn connector_column(column: &TableColumnDef) -> Result<ConnectorColumnDefinition, String> {
    Ok(ConnectorColumnDefinition {
        name: Arc::from(column.name.as_str()),
        data_type: connector_data_type(&column.data_type)?,
        nullable: column.nullable,
        aggregation: column.aggregation.map(connector_column_aggregation),
        default: column.default.as_ref().map(connector_default).transpose()?,
    })
}

pub(crate) fn connector_data_type(data_type: &SqlType) -> Result<ConnectorDataType, String> {
    Ok(match data_type {
        SqlType::Boolean => ConnectorDataType::Boolean,
        SqlType::TinyInt => ConnectorDataType::TinyInt,
        SqlType::SmallInt => ConnectorDataType::SmallInt,
        SqlType::Int => ConnectorDataType::Int,
        SqlType::BigInt => ConnectorDataType::BigInt,
        SqlType::LargeInt => ConnectorDataType::LargeInt,
        SqlType::Float => ConnectorDataType::Float,
        SqlType::Double => ConnectorDataType::Double,
        SqlType::Decimal { precision, scale } => ConnectorDataType::Decimal {
            precision: *precision,
            scale: *scale,
        },
        SqlType::String => ConnectorDataType::String,
        SqlType::Json => ConnectorDataType::Json,
        SqlType::Binary => ConnectorDataType::Binary,
        SqlType::Bitmap => ConnectorDataType::Bitmap,
        SqlType::Hll => ConnectorDataType::Hll,
        SqlType::Date => ConnectorDataType::Date,
        SqlType::DateTime => ConnectorDataType::DateTime,
        SqlType::DateTimeNs => ConnectorDataType::DateTimeNs,
        SqlType::Time => ConnectorDataType::Time,
        SqlType::Array(element) => {
            ConnectorDataType::Array(Box::new(connector_data_type(element)?))
        }
        SqlType::Map(key, value) => ConnectorDataType::Map(
            Box::new(connector_data_type(key)?),
            Box::new(connector_data_type(value)?),
        ),
        SqlType::Struct(fields) => ConnectorDataType::Struct(
            fields
                .iter()
                .map(|(name, data_type)| {
                    Ok(novarocks_spi::connector::ConnectorStructField {
                        name: Arc::from(name.as_str()),
                        data_type: connector_data_type(data_type)?,
                        // SQL's current struct AST has no child-nullability bit.
                        nullable: true,
                    })
                })
                .collect::<Result<_, String>>()?,
        ),
        SqlType::Variant => ConnectorDataType::Variant,
    })
}

fn connector_default(value: &DefaultLiteral) -> Result<ConnectorDefaultValue, String> {
    Ok(match value {
        DefaultLiteral::Null => ConnectorDefaultValue::Null,
        DefaultLiteral::Bool(value) => ConnectorDefaultValue::Bool(*value),
        DefaultLiteral::Int(value) => ConnectorDefaultValue::Int(*value),
        DefaultLiteral::Float(value) => ConnectorDefaultValue::Float(*value),
        DefaultLiteral::Decimal { unscaled, scale } => ConnectorDefaultValue::Decimal {
            unscaled: *unscaled,
            scale: *scale,
        },
        DefaultLiteral::String(value) => ConnectorDefaultValue::String(Arc::from(value.as_str())),
        DefaultLiteral::Date(value) => ConnectorDefaultValue::Date(*value),
        DefaultLiteral::DateTime(value) => ConnectorDefaultValue::DateTime(*value),
        DefaultLiteral::Binary(value) => {
            ConnectorDefaultValue::Binary(Bytes::copy_from_slice(value))
        }
    })
}

fn connector_column_aggregation(aggregation: ColumnAggregation) -> ConnectorColumnAggregation {
    match aggregation {
        ColumnAggregation::Sum => ConnectorColumnAggregation::Sum,
        ColumnAggregation::Min => ConnectorColumnAggregation::Min,
        ColumnAggregation::Max => ConnectorColumnAggregation::Max,
        ColumnAggregation::Replace => ConnectorColumnAggregation::Replace,
        ColumnAggregation::ReplaceIfNotNull => ConnectorColumnAggregation::ReplaceIfNotNull,
        ColumnAggregation::BitmapUnion => ConnectorColumnAggregation::BitmapUnion,
        ColumnAggregation::HllUnion => ConnectorColumnAggregation::HllUnion,
    }
}

pub(crate) fn connector_table_key(key: &TableKeyDesc) -> ConnectorTableKey {
    ConnectorTableKey {
        kind: match key.kind {
            TableKeyKind::Duplicate => ConnectorTableKeyKind::Duplicate,
            TableKeyKind::Unique => ConnectorTableKeyKind::Unique,
            TableKeyKind::Aggregate => ConnectorTableKeyKind::Aggregate,
            TableKeyKind::Primary => ConnectorTableKeyKind::Primary,
        },
        columns: key
            .columns
            .iter()
            .map(|column| Arc::from(column.as_str()))
            .collect(),
    }
}

pub fn connector_partition_transform(
    field: &IcebergPartitionFieldExpr,
) -> ConnectorPartitionTransform {
    match field {
        IcebergPartitionFieldExpr::Identity { column } => ConnectorPartitionTransform::Identity {
            column: Arc::from(column.as_str()),
        },
        IcebergPartitionFieldExpr::Year { column } => ConnectorPartitionTransform::Year {
            column: Arc::from(column.as_str()),
        },
        IcebergPartitionFieldExpr::Month { column } => ConnectorPartitionTransform::Month {
            column: Arc::from(column.as_str()),
        },
        IcebergPartitionFieldExpr::Day { column } => ConnectorPartitionTransform::Day {
            column: Arc::from(column.as_str()),
        },
        IcebergPartitionFieldExpr::Hour { column } => ConnectorPartitionTransform::Hour {
            column: Arc::from(column.as_str()),
        },
        IcebergPartitionFieldExpr::Bucket {
            column,
            num_buckets,
        } => ConnectorPartitionTransform::Bucket {
            column: Arc::from(column.as_str()),
            num_buckets: *num_buckets,
        },
        IcebergPartitionFieldExpr::Truncate { column, width } => {
            ConnectorPartitionTransform::Truncate {
                column: Arc::from(column.as_str()),
                width: *width,
            }
        }
        IcebergPartitionFieldExpr::Void { column } => ConnectorPartitionTransform::Void {
            column: Arc::from(column.as_str()),
        },
    }
}

// Ownership: `ColumnPath` and `AddPosition` are this module's own parsed
// schema-change AST types, so lowering them onto the connector SPI is catalog
// statement work, not query assembly. These two join the sibling converters
// above (`connector_partition_transform`, `connector_table_key`,
// `connector_column_aggregation`) that already own that lowering.
pub(crate) fn connector_schema_path(path: ColumnPath) -> ConnectorColumnPath {
    ConnectorColumnPath {
        segments: path
            .segments()
            .iter()
            .map(|segment| Arc::from(segment.as_str()))
            .collect(),
    }
}

pub(crate) fn connector_schema_position(position: AddPosition) -> ConnectorColumnPosition {
    match position {
        AddPosition::Default => ConnectorColumnPosition::Default,
        AddPosition::First => ConnectorColumnPosition::First,
        AddPosition::After(column) => ConnectorColumnPosition::After {
            column: Arc::from(column),
        },
        AddPosition::Before(column) => ConnectorColumnPosition::Before {
            column: Arc::from(column),
        },
    }
}

pub(crate) fn execute_drop_catalog_statement(
    context: &impl CatalogDropContext,
    catalog_name: &str,
    if_exists: bool,
) -> Result<StatementResult, String> {
    let normalized_catalog = normalize_identifier(catalog_name)?;
    let application = context.catalog_application().ok_or_else(|| {
        "catalog statements require a configured frontend catalog application".to_string()
    })?;
    let instance_id = ConnectorInstanceId::parse(&normalized_catalog)
        .map_err(|error| format!("invalid catalog connector instance ID: {error}"))?;
    // The Frontend application owns the exact-version delete and the MV
    // dependency scan that fences it, both inside one serializable StateStore
    // transaction. Core must not pre-check dependencies outside that fence.
    application
        .drop_catalog(novarocks_catalog_application::CatalogDropCommand {
            instance_id,
            if_exists,
        })
        .map_err(|error| error.to_string())?;
    Ok(StatementResult::Ok)
}

pub(crate) fn execute_drop_database_statement(
    context: &impl CatalogDropContext,
    name: &ObjectName,
    current_catalog: Option<&str>,
    if_exists: bool,
    force: bool,
    connector_context: &novarocks_spi::connector::ConnectorRequestContext,
) -> Result<StatementResult, String> {
    let target = crate::catalog_application::resolver::resolve_namespace_target(
        context,
        name,
        current_catalog,
    )?;
    if target.provider_id.as_str() == "iceberg" {
        ensure_no_iceberg_mv_targets_in_scope(context, &target.catalog, Some(&target.namespace))?;
        ensure_no_external_iceberg_dependents(context, &target.catalog, Some(&target.namespace))?;
    }
    let instance_id = mutation_instance_id(&target.catalog)?;
    if force {
        let lease = context
            .connector_control()
            .acquire_current(&instance_id)
            .map_err(|error| error.to_string())?;
        // `IF EXISTS` applies to the complete FORCE decomposition.  In
        // particular, do not ask a remote catalog to enumerate a namespace
        // which the final DropNamespace mutation would correctly treat as a
        // no-op.
        let namespace_identity = ConnectorNamespaceIdentity {
            instance_id: instance_id.clone(),
            namespace: Arc::from(target.namespace.as_str()),
        };
        let namespace_exists = lease
            .binding()
            .metadata()
            .namespace_exists(novarocks_spi::connector::ConnectorNamespaceRequest {
                namespace: namespace_identity.clone(),
                context: connector_context.clone(),
            })
            .map_err(|error| error.to_string())?;
        if !namespace_exists {
            if if_exists {
                return Ok(StatementResult::Ok);
            }
            return Err(format!("namespace `{}` does not exist", target.namespace));
        }
        let mut tables = lease
            .binding()
            .metadata()
            .list_tables(novarocks_spi::connector::ConnectorListTablesRequest {
                namespace: namespace_identity.clone(),
                context: connector_context.clone(),
            })
            .map_err(|error| error.to_string())?
            .into_iter()
            .map(|identity| identity.table.to_string())
            .collect::<Vec<_>>();
        tables.sort();
        let mut views = lease
            .binding()
            .view_metadata()
            .map(|view_metadata| {
                view_metadata.list_views(novarocks_spi::connector::ConnectorListViewsRequest {
                    namespace: namespace_identity,
                    context: connector_context.clone(),
                })
            })
            .transpose()
            .map_err(|error| error.to_string())?
            .unwrap_or_default()
            .into_iter()
            .map(|identity| identity.view.to_string())
            .collect::<Vec<_>>();
        views.sort();
        for table in tables {
            crate::connector::mutation::execute_catalog_mutation(
                context.connector_control(),
                &instance_id,
                ConnectorCatalogMutationOperation::DropTable {
                    table: ConnectorTableIdentity {
                        instance_id: instance_id.clone(),
                        namespace: Arc::from(target.namespace.as_str()),
                        table: Arc::from(table.as_str()),
                    },
                    // FORCE expands a namespace delete from a non-transactional
                    // listing. A child may disappear before its mutation starts,
                    // so every child delete is idempotent; the final namespace
                    // mutation retains the statement-level IF EXISTS contract.
                    policy: DropPolicy::NoOpIfMissing,
                    data_disposition: ConnectorDropTableDataDisposition::Purge,
                },
                connector_context.clone(),
            )?;
            context.catalog_service().invalidate_table(
                &target.catalog,
                &target.namespace,
                &table,
            )?;
            drop_local_table_registration_if_exists(context, &target.namespace, &table)?;
        }
        for view in views {
            crate::connector::mutation::execute_catalog_mutation(
                context.connector_control(),
                &instance_id,
                ConnectorCatalogMutationOperation::DropView {
                    view: ConnectorViewIdentity {
                        instance_id: instance_id.clone(),
                        namespace: Arc::from(target.namespace.as_str()),
                        view: Arc::from(view.as_str()),
                    },
                    policy: DropPolicy::FailIfMissing,
                },
                connector_context.clone(),
            )?;
        }
    }
    crate::connector::mutation::execute_catalog_mutation(
        context.connector_control(),
        &instance_id,
        ConnectorCatalogMutationOperation::DropNamespace {
            namespace: ConnectorNamespaceIdentity {
                instance_id: instance_id.clone(),
                namespace: Arc::from(target.namespace),
            },
            policy: if if_exists {
                DropPolicy::NoOpIfMissing
            } else {
                DropPolicy::FailIfMissing
            },
        },
        connector_context.clone(),
    )?;
    Ok(StatementResult::Ok)
}

pub(crate) fn execute_drop_table_statement(
    context: &impl CatalogDropContext,
    name: &ObjectName,
    current_catalog: Option<&str>,
    current_database: &str,
    if_exists: bool,
    _force: bool,
    connector_context: &novarocks_spi::connector::ConnectorRequestContext,
) -> Result<StatementResult, String> {
    let target = match crate::catalog_application::resolver::resolve_existing_table_target(
        context,
        name,
        current_catalog,
        current_database,
    ) {
        Ok(target) => target,
        Err(_) if current_catalog.is_none() && name.parts.len() <= 2 => {
            // External parquet tables registered through the embedding API are
            // still catalog-only entries. Dropping them does not involve a
            // connector backend.
            return drop_local_catalog_table(context, name, current_database, if_exists);
        }
        Err(err) => return Err(err),
    };
    let dependency_ref = if target.provider_id.as_str() == "iceberg" {
        novarocks_mv_application::dependency::iceberg_table_object_ref(
            &target.catalog,
            &target.namespace,
            &target.table,
        )
    } else {
        novarocks_mv_application::dependency::external_table_object_ref(
            &target.catalog,
            &target.namespace,
            &target.table,
        )
    };
    match crate::mv::domain::iceberg_guard::reject_if_iceberg_mv_table_with_ports(
        context.connector_control(),
        context.mv_storage_observation(),
        &target,
        crate::mv::domain::iceberg_guard::IcebergMvUserMutation::DropTable,
    ) {
        Ok(()) => {}
        Err(err)
            if if_exists
                && target.provider_id.as_str() == "iceberg"
                && is_missing_table_guard_error(&err) =>
        {
            cleanup_iceberg_drop_table_registration_if_exists(context, &target)?;
            return Ok(StatementResult::Ok);
        }
        Err(err) => return Err(err),
    }
    context
        .mv_readiness()
        .ensure_no_ready_downstream_dependencies(&dependency_ref)
        .map_err(|error| error.to_string())?;
    let instance_id = mutation_instance_id(&target.catalog)?;
    match crate::connector::mutation::execute_catalog_mutation(
        context.connector_control(),
        &instance_id,
        ConnectorCatalogMutationOperation::DropTable {
            table: ConnectorTableIdentity {
                instance_id: instance_id.clone(),
                namespace: Arc::from(target.namespace.as_str()),
                table: Arc::from(target.table.as_str()),
            },
            policy: if if_exists {
                DropPolicy::NoOpIfMissing
            } else {
                DropPolicy::FailIfMissing
            },
            data_disposition: ConnectorDropTableDataDisposition::Purge,
        },
        connector_context.clone(),
    ) {
        Ok(_) => {
            if target.provider_id.as_str() == "iceberg" {
                context.catalog_service().invalidate_table(
                    &target.catalog,
                    &target.namespace,
                    &target.table,
                )?;
                drop_local_table_registration_if_exists(context, &target.namespace, &target.table)?;
            }
            Ok(StatementResult::Ok)
        }
        Err(err) if if_exists && err.contains("NotFound") => {
            if target.provider_id.as_str() == "iceberg" {
                cleanup_iceberg_drop_table_registration_if_exists(context, &target)?;
            }
            Ok(StatementResult::Ok)
        }
        Err(err) => {
            // A DROP TABLE aimed at a view must say so instead of "unknown
            // table" — views and tables are separate REST resources.
            if target.provider_id.as_str() == "iceberg"
                && external_view_exists(
                    context,
                    &target.catalog,
                    &target.namespace,
                    &target.table,
                    connector_context,
                )?
            {
                return Err(format!(
                    "{}.{}.{} is a view, use DROP VIEW",
                    target.catalog, target.namespace, target.table
                ));
            }
            Err(err)
        }
    }
}

fn is_missing_table_guard_error(err: &str) -> bool {
    let lower = err.to_ascii_lowercase();
    lower.contains("unknown table:")
        || lower.contains("table not found")
        || lower.contains("no metadata files")
        // Catalog backends normalize absence differently; the REST client
        // reports that the table does not exist.
        || lower.contains("does not exist")
}

fn cleanup_iceberg_drop_table_registration_if_exists(
    context: &impl CatalogDropContext,
    target: &crate::catalog_application::resolver::TargetBackend,
) -> Result<(), String> {
    context.catalog_service().invalidate_table(
        &target.catalog,
        &target.namespace,
        &target.table,
    )?;
    drop_local_table_registration_if_exists(context, &target.namespace, &target.table)
}

fn drop_local_catalog_table(
    context: &impl CatalogDropContext,
    name: &ObjectName,
    current_database: &str,
    if_exists: bool,
) -> Result<StatementResult, String> {
    let resolved = resolve_local_table_name(name.parts.as_slice(), current_database)?;
    let mut guard = context
        .catalog_service()
        .local()
        .write()
        .expect("standalone catalog write lock");
    match guard.drop_table(&resolved.database, &resolved.table) {
        Ok(()) => Ok(StatementResult::Ok),
        Err(err) if if_exists && err.contains("unknown") => Ok(StatementResult::Ok),
        Err(err) => Err(err),
    }
}

fn ensure_no_iceberg_mv_targets_in_scope(
    context: &impl CatalogDropContext,
    scope_catalog: &str,
    scope_namespace: Option<&str>,
) -> Result<(), String> {
    let projections = context
        .mv_readiness()
        .list_ready_projections()
        .map_err(|error| {
            format!("load MV definitions for drop target scope check failed: {error}")
        })?;
    let targets = projections
        .iter()
        .map(|projection| &projection.definition)
        .filter(|definition| definition.storage_engine.eq_ignore_ascii_case("iceberg"))
        .map(|definition| {
            novarocks_mv_application::persistence::dependency::stored_definition_dependency_ref(
                definition, None,
            )
        })
        .collect::<Result<Vec<_>, _>>()?;
    crate::mv::domain::dependency::scope::validate_no_iceberg_mv_targets_in_scope(
        scope_catalog,
        scope_namespace,
        &targets,
    )
}

fn ensure_no_external_iceberg_dependents(
    context: &impl CatalogDropContext,
    scope_catalog: &str,
    scope_namespace: Option<&str>,
) -> Result<(), String> {
    let projections = context
        .mv_readiness()
        .list_ready_projections()
        .map_err(|error| format!("load MV definitions for drop scope check failed: {error}"))?;
    let mut edges = Vec::with_capacity(projections.len());
    for projection in projections {
        let definition = projection.definition.clone();
        let target =
            novarocks_mv_application::persistence::dependency::stored_definition_dependency_ref(
                &definition,
                None,
            )?;
        let upstreams = context
            .mv_readiness()
            .list_ready_dependencies_by_downstream(&projection)
            .map_err(|error| format!("load MV dependencies for drop scope check failed: {error}"))?
            .into_iter()
            .map(|dependency| dependency.upstream)
            .collect();
        edges.push((target, upstreams));
    }
    crate::mv::domain::dependency::scope::validate_no_external_dependents_for_scope(
        scope_catalog,
        scope_namespace,
        &edges,
    )
}

fn external_view_exists(
    context: &impl CatalogDropContext,
    catalog: &str,
    namespace: &str,
    view: &str,
    connector_context: &novarocks_spi::connector::ConnectorRequestContext,
) -> Result<bool, String> {
    let lease =
        crate::connector::acquire_metadata_planning_lease(context.connector_control(), catalog)?;
    let binding = lease.binding();
    let Some(view_metadata) = binding.view_metadata() else {
        return Ok(false);
    };
    let instance_id = binding.descriptor().instance_id.clone();
    match view_metadata.load_view(ConnectorViewRequest {
        view: ConnectorViewIdentity {
            instance_id,
            namespace: Arc::from(namespace),
            view: Arc::from(view),
        },
        context: connector_context.clone(),
    }) {
        Ok(_) => Ok(true),
        Err(error)
            if matches!(
                error.kind(),
                ConnectorErrorKind::NotFound | ConnectorErrorKind::Unsupported
            ) =>
        {
            Ok(false)
        }
        Err(error) => Err(error.to_string()),
    }
}

// ---------------------------------------------------------------------------
// DML handlers
// ---------------------------------------------------------------------------

// ---------------------------------------------------------------------------
// ADD FILES SQL parsing
// ---------------------------------------------------------------------------

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct AlterIcebergSchemaStmt {
    pub(crate) table: ObjectName,
    pub(crate) change: IcebergSchemaChange,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct AlterIcebergPropertiesStmt {
    pub(crate) table: ObjectName,
    pub(crate) op: PropertiesOp,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum PropertiesOp {
    Set { entries: Vec<(String, String)> },
    Unset { keys: Vec<String>, if_exists: bool },
}

/// One typed partition-spec mutation after syntax has been lowered to the
/// connector-owned representation. Table resolution remains with command
/// execution, which owns the target catalog and mutation admission.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum IcebergPartitionSpecChange {
    Add(ConnectorPartitionTransform),
    Drop(ConnectorPartitionTransform),
}

/// Lowers a semantic Iceberg property command without accepting parser AST.
pub(crate) fn lower_semantic_iceberg_properties_action(
    action: &IcebergPropertiesSqlAction,
) -> Result<PropertiesOp, String> {
    match action {
        IcebergPropertiesSqlAction::Set { entries } => {
            if entries.is_empty() {
                return Err("SET TBLPROPERTIES requires at least one key=value pair".to_string());
            }
            let mut seen = std::collections::HashSet::new();
            let mut lowered = Vec::with_capacity(entries.len());
            for (key, value) in entries {
                if !seen.insert(key.clone()) {
                    return Err(format!("duplicate key '{key}' in SET TBLPROPERTIES"));
                }
                lowered.push((key.clone(), semantic_property_string(value)?));
            }
            Ok(PropertiesOp::Set { entries: lowered })
        }
        IcebergPropertiesSqlAction::Unset { keys, if_exists } => {
            if keys.is_empty() {
                return Err("UNSET TBLPROPERTIES requires at least one key".to_string());
            }
            let mut seen = std::collections::HashSet::new();
            let mut lowered = Vec::with_capacity(keys.len());
            for key in keys {
                if !seen.insert(key.clone()) {
                    return Err(format!("duplicate key '{key}' in UNSET TBLPROPERTIES"));
                }
                lowered.push(key.clone());
            }
            Ok(PropertiesOp::Unset {
                keys: lowered,
                if_exists: *if_exists,
            })
        }
        IcebergPropertiesSqlAction::Comment { value } => Ok(PropertiesOp::Set {
            entries: vec![("comment".to_string(), semantic_property_string(value)?)],
        }),
    }
}

/// Lowers a semantic partition change to the connector representation.
pub(crate) fn lower_semantic_iceberg_partition_change(
    change: &IcebergPartitionSqlChange,
) -> Result<IcebergPartitionSpecChange, String> {
    let lower = |field: &IcebergPartitionFieldExpr| -> Result<ConnectorPartitionTransform, String> {
        let normalized = normalize_semantic_partition_transform(field)?;
        Ok(connector_partition_transform(&normalized))
    };
    match change {
        IcebergPartitionSqlChange::Add(field) => Ok(IcebergPartitionSpecChange::Add(lower(field)?)),
        IcebergPartitionSqlChange::Drop(field) => {
            Ok(IcebergPartitionSpecChange::Drop(lower(field)?))
        }
    }
}

/// Lowers a semantic schema change to the catalog product request.
pub(crate) fn lower_semantic_iceberg_schema_change(
    change: &IcebergSchemaSqlChange,
) -> Result<IcebergSchemaChange, String> {
    match change {
        IcebergSchemaSqlChange::AddColumn {
            path,
            data_type,
            nullable,
            default,
            position,
        } => {
            if matches!(nullable, Some(false)) {
                return Err(
                    "ADD COLUMN NOT NULL is not supported for Iceberg schema evolution".to_string(),
                );
            }
            let (parent, name) = semantic_add_column_path(path)?;
            Ok(IcebergSchemaChange::AddColumn {
                parent,
                name,
                data_type: data_type.clone(),
                default: default
                    .as_ref()
                    .map(|value| lower_semantic_default_literal(value, data_type))
                    .transpose()?,
                position: lower_semantic_add_position(position, true)?,
            })
        }
        IcebergSchemaSqlChange::DropColumn { path } => Ok(IcebergSchemaChange::DropColumn {
            path: semantic_column_path(path)?,
        }),
        IcebergSchemaSqlChange::RenameColumn { from, to } => {
            let path = semantic_column_path(from)?;
            let target = semantic_column_path(to)?;
            let source_parent = path.parent();
            let target_parent = target.parent();
            if !target_parent.is_empty() && target_parent != source_parent {
                return Err(
                    "RENAME COLUMN target must share the same parent path as the source"
                        .to_string(),
                );
            }
            Ok(IcebergSchemaChange::RenameColumn {
                path,
                new_name: target
                    .last()
                    .expect("semantic column path is non-empty")
                    .to_owned(),
            })
        }
        IcebergSchemaSqlChange::ModifyColumn { path, data_type } => {
            Ok(IcebergSchemaChange::ModifyColumn {
                path: semantic_column_path(path)?,
                new_type: data_type.clone(),
            })
        }
        IcebergSchemaSqlChange::AlterColumn { path, action } => {
            let path = semantic_column_path(path)?;
            match action {
                IcebergColumnSqlAction::Reorder(position) => Ok(IcebergSchemaChange::Reorder {
                    path,
                    position: lower_semantic_add_position(position, false)?,
                }),
                IcebergColumnSqlAction::SetNullable(nullable) => {
                    Ok(IcebergSchemaChange::SetNullable {
                        path,
                        nullable: *nullable,
                    })
                }
                IcebergColumnSqlAction::Comment(comment) => {
                    Ok(IcebergSchemaChange::UpdateComment {
                        path,
                        comment: semantic_property_string(comment).map_err(|_| {
                            "ALTER COLUMN COMMENT requires a string literal".to_string()
                        })?,
                    })
                }
            }
        }
    }
}

fn semantic_property_string(literal: &CommandLiteral) -> Result<String, String> {
    match literal {
        CommandLiteral::String(value) => Ok(value.clone()),
        _ => Err("TBLPROPERTIES key/value must be a string literal".to_string()),
    }
}

fn semantic_column_path(parts: &[String]) -> Result<ColumnPath, String> {
    if parts.is_empty() {
        return Err("column path is empty".to_string());
    }
    Ok(ColumnPath::from_segments(parts.to_vec()))
}

fn semantic_add_column_path(parts: &[String]) -> Result<(ColumnPath, String), String> {
    let mut path = semantic_column_path(parts)?;
    let name = path
        .segments
        .pop()
        .ok_or_else(|| "ADD COLUMN requires a column path".to_string())?;
    Ok((path, name))
}

fn lower_semantic_add_position(
    position: &ColumnPositionSql,
    add_column: bool,
) -> Result<AddPosition, String> {
    let target = |path: &[String]| {
        let path = semantic_column_path(path)?;
        if add_column && path.segments.len() != 1 {
            return Err(
                "ADD COLUMN position target must be a single column identifier".to_string(),
            );
        }
        path.last()
            .map(str::to_owned)
            .ok_or_else(|| "column position target is empty".to_string())
    };
    match position {
        ColumnPositionSql::Default => Ok(AddPosition::Default),
        ColumnPositionSql::First => Ok(AddPosition::First),
        ColumnPositionSql::After(path) => Ok(AddPosition::After(target(path)?)),
        ColumnPositionSql::Before(path) => Ok(AddPosition::Before(target(path)?)),
    }
}

fn normalize_semantic_partition_transform(
    field: &IcebergPartitionFieldExpr,
) -> Result<IcebergPartitionFieldExpr, String> {
    let normalize = |column: &str| normalize_identifier(column);
    Ok(match field {
        IcebergPartitionFieldExpr::Identity { column } => IcebergPartitionFieldExpr::Identity {
            column: normalize(column)?,
        },
        IcebergPartitionFieldExpr::Year { column } => IcebergPartitionFieldExpr::Year {
            column: normalize(column)?,
        },
        IcebergPartitionFieldExpr::Month { column } => IcebergPartitionFieldExpr::Month {
            column: normalize(column)?,
        },
        IcebergPartitionFieldExpr::Day { column } => IcebergPartitionFieldExpr::Day {
            column: normalize(column)?,
        },
        IcebergPartitionFieldExpr::Hour { column } => IcebergPartitionFieldExpr::Hour {
            column: normalize(column)?,
        },
        IcebergPartitionFieldExpr::Void { column } => IcebergPartitionFieldExpr::Void {
            column: normalize(column)?,
        },
        IcebergPartitionFieldExpr::Bucket {
            column,
            num_buckets,
        } => {
            if *num_buckets == 0 {
                return Err("bucket count must be positive".to_string());
            }
            IcebergPartitionFieldExpr::Bucket {
                column: normalize(column)?,
                num_buckets: *num_buckets,
            }
        }
        IcebergPartitionFieldExpr::Truncate { column, width } => {
            if *width == 0 {
                return Err("truncate width must be positive".to_string());
            }
            IcebergPartitionFieldExpr::Truncate {
                column: normalize(column)?,
                width: *width,
            }
        }
    })
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ColumnPath {
    segments: Vec<String>,
}

impl ColumnPath {
    pub(crate) fn root() -> Self {
        Self {
            segments: Vec::new(),
        }
    }

    #[cfg(test)]
    pub(crate) fn parse(input: &str) -> Result<Self, String> {
        if input.is_empty() {
            return Err("column path is empty".to_string());
        }
        let mut segments = Vec::new();
        for raw in input.split('.') {
            if raw.is_empty() {
                return Err(format!("invalid column path '{input}': empty segment"));
            }
            segments.push(raw.to_ascii_lowercase());
        }
        Ok(Self { segments })
    }

    pub(crate) fn from_segments(segments: Vec<String>) -> Self {
        Self {
            segments: segments
                .into_iter()
                .map(|s| s.to_ascii_lowercase())
                .collect(),
        }
    }

    pub(crate) fn segments(&self) -> &[String] {
        &self.segments
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.segments.is_empty()
    }

    pub(crate) fn last(&self) -> Option<&str> {
        self.segments.last().map(String::as_str)
    }

    pub(crate) fn parent(&self) -> ColumnPath {
        if self.segments.is_empty() {
            return ColumnPath::root();
        }
        Self {
            segments: self.segments[..self.segments.len() - 1].to_vec(),
        }
    }

    pub(crate) fn dotted(&self) -> String {
        self.segments.join(".")
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum AddPosition {
    Default,
    First,
    After(String),
    Before(String),
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) enum IcebergSchemaChange {
    AddColumn {
        parent: ColumnPath,
        name: String,
        data_type: SqlType,
        default: Option<DefaultLiteral>,
        position: AddPosition,
    },
    DropColumn {
        path: ColumnPath,
    },
    RenameColumn {
        path: ColumnPath,
        new_name: String,
    },
    ModifyColumn {
        path: ColumnPath,
        new_type: SqlType,
    },
    SetNullable {
        path: ColumnPath,
        nullable: bool,
    },
    Reorder {
        path: ColumnPath,
        position: AddPosition,
    },
    UpdateComment {
        path: ColumnPath,
        comment: String,
    },
}

/// Lower a parser-owned syntax type directly to the catalog's semantic type.
/// This is intentionally recursive so ARRAY/MAP/STRUCT never fall back to a
/// SQL text round-trip.
pub(crate) fn lower_typed_sql_type(
    type_name: &novarocks_parser::ast::TypeName,
) -> Result<SqlType, String> {
    use novarocks_parser::ast::TypeNameArgument;

    let name = type_name
        .name
        .parts
        .last()
        .ok_or_else(|| "type name is empty".to_string())?
        .value
        .to_ascii_lowercase();
    if type_name.name.parts.len() != 1 {
        return Err(format!("qualified type name `{name}` is not supported"));
    }

    match name.as_str() {
        "array" => Ok(SqlType::Array(Box::new(lower_array_element_type(
            type_name,
        )?))),
        "map" => {
            let (key, value) = lower_map_types(type_name)?;
            Ok(SqlType::Map(Box::new(key), Box::new(value)))
        }
        "struct" => Ok(SqlType::Struct(
            type_name
                .arguments
                .iter()
                .map(|argument| match argument {
                    TypeNameArgument::Field(field) => Ok((
                        field.name.value.clone(),
                        lower_typed_sql_type(&field.data_type)?,
                    )),
                    _ => Err("STRUCT type requires named fields".to_string()),
                })
                .collect::<Result<Vec<_>, String>>()?,
        )),
        "decimal" | "dec" | "numeric" | "decimal32" | "decimal64" | "decimal128" => {
            let (precision, scale) = lower_decimal_arguments(type_name)?;
            Ok(SqlType::Decimal { precision, scale })
        }
        "tinyint" | "int8" => Ok(SqlType::TinyInt),
        "smallint" | "int16" => Ok(SqlType::SmallInt),
        "int" | "integer" | "int32" => Ok(SqlType::Int),
        "bigint" | "int64" => Ok(SqlType::BigInt),
        "largeint" | "int128" => Ok(SqlType::LargeInt),
        "float" | "float32" => Ok(SqlType::Float),
        "double" | "float64" | "double precision" => Ok(SqlType::Double),
        "boolean" | "bool" => Ok(SqlType::Boolean),
        "string" | "varchar" | "char" | "character" | "text" => Ok(SqlType::String),
        "date" => Ok(SqlType::Date),
        "datetime" | "timestamp" => Ok(SqlType::DateTime),
        "timestamp_ns" | "timestamptz_ns" | "datetime_ns" => Ok(SqlType::DateTimeNs),
        "time" => Ok(SqlType::Time),
        "binary" | "varbinary" => Ok(SqlType::Binary),
        "json" | "jsonb" => Ok(SqlType::Json),
        "bitmap" => Ok(SqlType::Bitmap),
        "hll" => Ok(SqlType::Hll),
        "variant" => Ok(SqlType::Variant),
        _ => Err(format!("unsupported Iceberg schema type `{name}`")),
    }
}

fn lower_array_element_type(
    type_name: &novarocks_parser::ast::TypeName,
) -> Result<SqlType, String> {
    use novarocks_parser::ast::TypeNameArgument;

    let [TypeNameArgument::Type(element)] = type_name.arguments.as_slice() else {
        return Err("ARRAY type requires one element type".to_string());
    };
    lower_typed_sql_type(element)
}

fn lower_map_types(
    type_name: &novarocks_parser::ast::TypeName,
) -> Result<(SqlType, SqlType), String> {
    use novarocks_parser::ast::TypeNameArgument;

    let [TypeNameArgument::Type(key), TypeNameArgument::Type(value)] =
        type_name.arguments.as_slice()
    else {
        return Err("MAP type requires key and value types".to_string());
    };
    Ok((lower_typed_sql_type(key)?, lower_typed_sql_type(value)?))
}

fn lower_decimal_arguments(
    type_name: &novarocks_parser::ast::TypeName,
) -> Result<(u8, i8), String> {
    use novarocks_parser::ast::{LiteralKind, TypeNameArgument};

    if type_name.arguments.len() > 2 {
        return Err("DECIMAL type accepts at most precision and scale".to_string());
    }
    let mut values = type_name.arguments.iter().map(|argument| match argument {
        TypeNameArgument::Literal(literal) => match &literal.kind {
            LiteralKind::Number(value) => Ok(value.as_str()),
            _ => Err("DECIMAL precision and scale must be numeric literals".to_string()),
        },
        _ => Err("DECIMAL precision and scale must be numeric literals".to_string()),
    });
    let precision = match values.next() {
        Some(value) => value?
            .parse::<u8>()
            .map_err(|_| "DECIMAL precision must fit u8".to_string())?,
        None => 38,
    };
    let scale = match values.next() {
        Some(value) => value?
            .parse::<i8>()
            .map_err(|_| "DECIMAL scale must fit i8".to_string())?,
        None => 0,
    };
    Ok((precision, scale))
}

fn lower_typed_numeric_default(text: &str, data_type: &SqlType) -> Result<DefaultLiteral, String> {
    match data_type {
        SqlType::TinyInt | SqlType::SmallInt | SqlType::Int | SqlType::BigInt => {
            let value = text
                .parse::<i64>()
                .map_err(|error| format!("invalid integer DEFAULT `{text}`: {error}"))?;
            Ok(DefaultLiteral::Int(value))
        }
        SqlType::Float | SqlType::Double => {
            let value = text
                .parse::<f64>()
                .map_err(|error| format!("invalid float DEFAULT `{text}`: {error}"))?;
            Ok(DefaultLiteral::Float(value))
        }
        SqlType::Decimal { scale, .. } => {
            let (unscaled, literal_scale) = typed_decimal_from_str(text)?;
            if literal_scale != *scale {
                return Err(format!(
                    "DEFAULT value scale {literal_scale} does not match column scale {scale}"
                ));
            }
            Ok(DefaultLiteral::Decimal {
                unscaled,
                scale: *scale,
            })
        }
        other => Err(format!(
            "numeric DEFAULT not supported for column type {other:?}"
        )),
    }
}

fn lower_typed_string_default(value: &str, data_type: &SqlType) -> Result<DefaultLiteral, String> {
    match data_type {
        SqlType::String => Ok(DefaultLiteral::String(value.to_string())),
        SqlType::TinyInt
        | SqlType::SmallInt
        | SqlType::Int
        | SqlType::BigInt
        | SqlType::Float
        | SqlType::Double
        | SqlType::Decimal { .. } => lower_typed_numeric_default(value.trim(), data_type),
        SqlType::Boolean => match value.trim().to_ascii_lowercase().as_str() {
            "true" | "1" => Ok(DefaultLiteral::Bool(true)),
            "false" | "0" => Ok(DefaultLiteral::Bool(false)),
            other => Err(format!(
                "invalid boolean DEFAULT `{other}` (expected true/false/0/1)"
            )),
        },
        SqlType::Json => {
            serde_json::from_str::<serde_json::Value>(value)
                .map_err(|error| format!("invalid JSON DEFAULT literal: {error}"))?;
            Ok(DefaultLiteral::String(value.to_string()))
        }
        SqlType::Date => Ok(DefaultLiteral::Date(parse_date_string_to_days(value)?)),
        SqlType::DateTime => Ok(DefaultLiteral::DateTime(parse_datetime_string_to_micros(
            value,
        )?)),
        SqlType::DateTimeNs => Ok(DefaultLiteral::DateTime(typed_datetime_string_to_nanos(
            value,
        )?)),
        SqlType::Binary | SqlType::Bitmap | SqlType::Hll => {
            Ok(DefaultLiteral::Binary(value.as_bytes().to_vec()))
        }
        SqlType::Array(_) => {
            let json: serde_json::Value = serde_json::from_str(value)
                .map_err(|error| format!("invalid ARRAY DEFAULT literal: {error}"))?;
            if !json.is_array() {
                return Err(format!(
                    "ARRAY DEFAULT must be a JSON array literal (e.g. '[]'), got: {value:?}"
                ));
            }
            Ok(DefaultLiteral::String(value.to_string()))
        }
        SqlType::Map(_, _) => {
            let json: serde_json::Value = serde_json::from_str(value)
                .map_err(|error| format!("invalid MAP DEFAULT literal: {error}"))?;
            if !json.is_object() {
                return Err(format!(
                    "MAP DEFAULT must be a JSON object literal (e.g. '{{}}'), got: {value:?}"
                ));
            }
            Ok(DefaultLiteral::String(value.to_string()))
        }
        other => Err(format!(
            "string DEFAULT not supported for column type {other:?}"
        )),
    }
}

fn validate_typed_default_literal(
    literal: &DefaultLiteral,
    data_type: &SqlType,
) -> Result<(), String> {
    if matches!(literal, DefaultLiteral::Null) {
        return Ok(());
    }
    if let DefaultLiteral::Decimal { scale, .. } = literal
        && *scale < 0
    {
        return Err(format!("negative DECIMAL scale {scale} is not supported"));
    }
    if let SqlType::Decimal { scale, .. } = data_type
        && *scale < 0
    {
        return Err(format!("negative DECIMAL scale {scale} is not supported"));
    }

    match (literal, data_type) {
        (DefaultLiteral::String(value), SqlType::Array(_)) => {
            let elements = serde_json::from_str::<serde_json::Value>(value)
                .map_err(|error| format!("invalid ARRAY DEFAULT JSON: {error}"))?
                .as_array()
                .ok_or_else(|| format!("ARRAY DEFAULT must be a JSON array, got: {value:?}"))?
                .clone();
            if !elements.is_empty() {
                return Err(
                    "non-empty ARRAY DEFAULT literals are not yet supported; use '[]'".to_string(),
                );
            }
        }
        (DefaultLiteral::String(value), SqlType::Map(_, _)) => {
            let entries = serde_json::from_str::<serde_json::Value>(value)
                .map_err(|error| format!("invalid MAP DEFAULT JSON: {error}"))?
                .as_object()
                .ok_or_else(|| format!("MAP DEFAULT must be a JSON object, got: {value:?}"))?
                .clone();
            if !entries.is_empty() {
                return Err(
                    "non-empty MAP DEFAULT literals are not yet supported; use '{}'".to_string(),
                );
            }
        }
        (DefaultLiteral::Bool(_), SqlType::Boolean)
        | (DefaultLiteral::Int(_), SqlType::BigInt)
        | (DefaultLiteral::Float(_), SqlType::Float | SqlType::Double)
        | (DefaultLiteral::String(_), SqlType::String | SqlType::Json)
        | (DefaultLiteral::Binary(_), SqlType::Binary | SqlType::Bitmap | SqlType::Hll)
        | (DefaultLiteral::Date(_), SqlType::Date)
        | (DefaultLiteral::DateTime(_), SqlType::DateTime | SqlType::DateTimeNs) => {}
        (DefaultLiteral::Int(value), SqlType::TinyInt) => {
            i8::try_from(*value).map_err(|_| default_out_of_range("TINYINT", *value))?;
        }
        (DefaultLiteral::Int(value), SqlType::SmallInt) => {
            i16::try_from(*value).map_err(|_| default_out_of_range("SMALLINT", *value))?;
        }
        (DefaultLiteral::Int(value), SqlType::Int) => {
            i32::try_from(*value).map_err(|_| default_out_of_range("INT", *value))?;
        }
        (
            DefaultLiteral::Decimal { scale, .. },
            SqlType::Decimal {
                scale: column_scale,
                ..
            },
        ) if scale == column_scale => {}
        (
            DefaultLiteral::Decimal { scale, .. },
            SqlType::Decimal {
                scale: column_scale,
                ..
            },
        ) => {
            return Err(format!(
                "DEFAULT value scale {scale} does not match column scale {column_scale}"
            ));
        }
        (literal, column_type) => {
            return Err(format!(
                "DEFAULT value type does not match column type: literal={literal:?} column={column_type:?}"
            ));
        }
    }
    Ok(())
}

fn typed_decimal_from_str(text: &str) -> Result<(i128, i8), String> {
    let trimmed = text.trim();
    let (sign, body) = if let Some(rest) = trimmed.strip_prefix('-') {
        (-1_i128, rest)
    } else {
        (1_i128, trimmed)
    };
    let (whole, fraction) = match body.split_once('.') {
        Some((whole, fraction)) => (whole, fraction),
        None => (body, ""),
    };
    let combined: String = whole.chars().chain(fraction.chars()).collect();
    let unscaled = combined
        .parse::<i128>()
        .map_err(|error| format!("invalid decimal DEFAULT `{text}`: {error}"))?;
    let scale = i8::try_from(fraction.len()).map_err(|_| "decimal scale too large".to_string())?;
    Ok((sign * unscaled, scale))
}

fn typed_datetime_string_to_nanos(value: &str) -> Result<i64, String> {
    use chrono::{NaiveDate, NaiveDateTime};

    let value = value.trim();
    let date_time = NaiveDateTime::parse_from_str(value, "%Y-%m-%d %H:%M:%S")
        .or_else(|_| NaiveDateTime::parse_from_str(value, "%Y-%m-%d %H:%M:%S%.f"))
        .or_else(|_| {
            NaiveDate::parse_from_str(value, "%Y-%m-%d")
                .map(|date| date.and_hms_opt(0, 0, 0).expect("midnight"))
        })
        .map_err(|_| format!("invalid datetime literal `{value}`"))?;
    date_time
        .and_utc()
        .timestamp_nanos_opt()
        .ok_or_else(|| format!("DATETIME literal '{value}' out of nanosecond representable range"))
}

fn default_out_of_range(type_name: &str, value: i64) -> String {
    format!("DEFAULT value {value} out of range for {type_name}")
}

#[cfg(test)]
mod drop_table_if_exists_tests {
    #[test]
    fn guard_missing_table_error_is_soft_drop_candidate_but_mv_error_is_not() {
        assert!(super::is_missing_table_guard_error(
            "unknown table: db.missing"
        ));
        assert!(super::is_missing_table_guard_error(
            "load iceberg table db.missing: table not found: warehouse/db/missing"
        ));
        assert!(super::is_missing_table_guard_error(
            "no metadata files for db.missing"
        ));
        assert!(!super::is_missing_table_guard_error(
            "table ice.db.mv_orders is a materialized view; use DROP MATERIALIZED VIEW"
        ));
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn semantic_create_table_lowering_materializes_catalog_request() {
        let sql = "CREATE TABLE IF NOT EXISTS ice.db.orders (id BIGINT DEFAULT 3, amount DECIMAL(10,2) DEFAULT '12.30', payload BINARY DEFAULT X'CAFE') DUPLICATE KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 8 PARTITION BY (month(id)) PROPERTIES ('format-version' = '2') COMMENT 'orders'";
        let statement = novarocks_query_application::sql::parse_single_statement(sql)
            .expect("parse semantic CREATE TABLE");
        let Some(novarocks_query_application::sql::ProductSqlCommand::Catalog(
            novarocks_sql::semantic::CatalogSqlCommand::CreateTable(command),
        )) = novarocks_query_application::sql::lower_product_sql_command(&statement)
            .expect("lower semantic CREATE TABLE")
        else {
            panic!("expected semantic CREATE TABLE");
        };

        let request = super::lower_semantic_create_table_statement(&command)
            .expect("lower semantic catalog request");
        let super::CatalogCreateTableKind::Iceberg {
            columns,
            key_desc,
            bucket_count,
            distribution_columns,
            partition_fields,
            properties,
        } = request.kind;
        assert_eq!(request.name.parts, ["ice", "db", "orders"]);
        assert!(request.if_not_exists);
        assert_eq!(columns[0].default, Some(super::DefaultLiteral::Int(3)));
        assert_eq!(
            columns[1].default,
            Some(super::DefaultLiteral::Decimal {
                unscaled: 1230,
                scale: 2,
            })
        );
        assert_eq!(
            columns[2].default,
            Some(super::DefaultLiteral::Binary(vec![0xCA, 0xFE]))
        );
        assert_eq!(
            key_desc.expect("duplicate key").kind,
            super::TableKeyKind::Duplicate
        );
        assert_eq!(bucket_count, Some(8));
        assert_eq!(distribution_columns, ["id"]);
        assert_eq!(
            partition_fields,
            [super::IcebergPartitionFieldExpr::Month {
                column: "id".into()
            }]
        );
        assert_eq!(
            properties,
            [
                ("format-version".into(), "2".into()),
                ("comment".into(), "orders".into()),
            ]
        );
    }

    fn semantic_create_table(sql: &str) -> Vec<(String, String)> {
        let statement = novarocks_query_application::sql::parse_single_statement(sql)
            .expect("parse semantic CREATE TABLE");
        let Some(novarocks_query_application::sql::ProductSqlCommand::Catalog(
            novarocks_sql::semantic::CatalogSqlCommand::CreateTable(command),
        )) = novarocks_query_application::sql::lower_product_sql_command(&statement)
            .expect("lower semantic CREATE TABLE")
        else {
            panic!("expected semantic CREATE TABLE");
        };
        let super::CatalogCreateTableKind::Iceberg { properties, .. } =
            super::lower_semantic_create_table_statement(&command)
                .expect("lower semantic catalog request")
                .kind;
        properties
    }

    #[test]
    fn semantic_create_table_preserves_comment_property_duplicate() {
        let table = semantic_create_table(
            "CREATE TABLE ice.db.orders (id INT) PROPERTIES ('comment' = 'property comment') COMMENT 'table comment'",
        );

        assert_eq!(
            table,
            vec![
                ("comment".to_string(), "property comment".to_string()),
                ("comment".to_string(), "table comment".to_string()),
            ]
        );
    }

    #[test]
    fn semantic_iceberg_schema_lowering_preserves_defaults_and_paths() {
        let novarocks_sql::semantic::command::IcebergTableSqlAction::Schema(change) =
            semantic_iceberg_action(
                "ALTER TABLE ice.db.orders ADD COLUMN total DECIMAL(10,2) DEFAULT '1.20' AFTER id",
            )
        else {
            panic!("expected semantic schema action");
        };
        assert_eq!(
            super::lower_semantic_iceberg_schema_change(&change).expect("lower semantic schema"),
            super::IcebergSchemaChange::AddColumn {
                parent: super::ColumnPath::root(),
                name: "total".to_string(),
                data_type: super::SqlType::Decimal {
                    precision: 10,
                    scale: 2,
                },
                default: Some(super::DefaultLiteral::Decimal {
                    unscaled: 120,
                    scale: 2,
                }),
                position: super::AddPosition::After("id".to_string()),
            }
        );
    }

    #[test]
    fn semantic_iceberg_schema_lowering_rejects_invalid_defaults() {
        let novarocks_sql::semantic::command::IcebergTableSqlAction::Schema(overflow) =
            semantic_iceberg_action("ALTER TABLE ice.db.orders ADD COLUMN d TINYINT DEFAULT 200")
        else {
            panic!("expected semantic schema action");
        };
        assert!(
            super::lower_semantic_iceberg_schema_change(&overflow)
                .expect_err("tinyint overflow")
                .contains("out of range for TINYINT")
        );

        let novarocks_sql::semantic::command::IcebergTableSqlAction::Schema(wrong_type) =
            semantic_iceberg_action(
                "ALTER TABLE ice.db.orders ADD COLUMN d BOOLEAN DEFAULT 'maybe'",
            )
        else {
            panic!("expected semantic schema action");
        };
        assert!(
            super::lower_semantic_iceberg_schema_change(&wrong_type)
                .expect_err("boolean coercion must validate")
                .contains("invalid boolean DEFAULT")
        );
    }

    #[test]
    fn semantic_iceberg_property_and_partition_lowering_preserves_validation() {
        let novarocks_sql::semantic::command::IcebergTableSqlAction::Properties(properties) =
            semantic_iceberg_action(
                "ALTER TABLE ice.db.orders SET TBLPROPERTIES ('format' = 'parquet', 'owner' = 'ops')",
            )
        else {
            panic!("expected semantic properties action");
        };
        assert_eq!(
            super::lower_semantic_iceberg_properties_action(&properties)
                .expect("lower semantic properties"),
            super::PropertiesOp::Set {
                entries: vec![
                    ("format".to_string(), "parquet".to_string()),
                    ("owner".to_string(), "ops".to_string()),
                ],
            }
        );

        let novarocks_sql::semantic::command::IcebergTableSqlAction::Partition(partition) =
            semantic_iceberg_action(
                "ALTER TABLE ice.db.orders ADD PARTITION COLUMN bucket(User_Id, 32)",
            )
        else {
            panic!("expected semantic partition action");
        };
        assert_eq!(
            super::lower_semantic_iceberg_partition_change(&partition)
                .expect("lower semantic partition"),
            super::IcebergPartitionSpecChange::Add(super::ConnectorPartitionTransform::Bucket {
                column: std::sync::Arc::from("user_id"),
                num_buckets: 32,
            })
        );

        let novarocks_sql::semantic::command::IcebergTableSqlAction::Properties(non_string) =
            semantic_iceberg_action(
                "ALTER TABLE ice.db.orders SET TBLPROPERTIES ('retention' = 7)",
            )
        else {
            panic!("expected semantic properties action");
        };
        assert!(
            super::lower_semantic_iceberg_properties_action(&non_string)
                .expect_err("non-string property")
                .contains("string literal")
        );

        let novarocks_sql::semantic::command::IcebergTableSqlAction::Partition(zero) =
            semantic_iceberg_action(
                "ALTER TABLE ice.db.orders ADD PARTITION COLUMN bucket(user_id, 0)",
            )
        else {
            panic!("expected semantic partition action");
        };
        assert!(
            super::lower_semantic_iceberg_partition_change(&zero)
                .expect_err("zero bucket count")
                .contains("must be positive")
        );
    }

    fn semantic_iceberg_action(
        sql: &str,
    ) -> novarocks_sql::semantic::command::IcebergTableSqlAction {
        let statement = novarocks_query_application::sql::parse_single_statement(sql)
            .expect("parse semantic Iceberg command");
        let Some(novarocks_query_application::sql::ProductSqlCommand::Catalog(
            novarocks_sql::semantic::CatalogSqlCommand::AlterIcebergTable(statement),
        )) = novarocks_query_application::sql::lower_product_sql_command(&statement)
            .expect("lower semantic Iceberg command")
        else {
            panic!("expected semantic Iceberg ALTER TABLE statement");
        };
        statement.action
    }
}

#[cfg(test)]
mod column_path_tests {
    use super::ColumnPath;

    #[test]
    fn column_path_parses_single_segment() {
        let p = ColumnPath::parse("address").unwrap();
        assert_eq!(p.segments(), &["address".to_string()]);
        assert!(!p.is_empty());
    }

    #[test]
    fn column_path_parses_dotted() {
        let p = ColumnPath::parse("address.street").unwrap();
        assert_eq!(p.segments(), &["address".to_string(), "street".to_string()]);
    }

    #[test]
    fn column_path_normalizes_case() {
        let p = ColumnPath::parse("Address.Street").unwrap();
        assert_eq!(p.segments(), &["address".to_string(), "street".to_string()]);
    }

    #[test]
    fn column_path_rejects_empty_segment() {
        assert!(ColumnPath::parse("address.").is_err());
        assert!(ColumnPath::parse(".street").is_err());
        assert!(ColumnPath::parse("").is_err());
        assert!(ColumnPath::parse("a..b").is_err());
    }

    #[test]
    fn column_path_root_is_empty() {
        assert!(ColumnPath::root().is_empty());
        assert!(ColumnPath::root().segments().is_empty());
    }

    #[test]
    fn add_position_default_constructed() {
        use super::AddPosition;
        let pos = AddPosition::Default;
        assert!(matches!(pos, AddPosition::Default));
    }

    #[test]
    fn add_position_variants_construct() {
        use super::AddPosition;
        let _ = AddPosition::First;
        let _ = AddPosition::After("col_a".to_string());
        let _ = AddPosition::Before("col_b".to_string());
    }
}
