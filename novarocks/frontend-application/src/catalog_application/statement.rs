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

use crate::catalog_application::query_catalog::drop_local_table_registration_if_exists;
use bytes::Bytes;
use novarocks_query_application::{
    api::CommandError, protocol_delivery::QuerySessionOutput as StatementResult,
};
use novarocks_spi::connector::ConnectorControlRegistry;
use novarocks_spi::connector::{
    ConnectorCatalogMutationOperation, ConnectorColumnDefinition, ConnectorColumnPath,
    ConnectorColumnPosition, ConnectorDataType, ConnectorDefaultValue,
    ConnectorDropTableDataDisposition, ConnectorErrorKind, ConnectorInstanceId,
    ConnectorNamespaceIdentity, ConnectorObjectKind, ConnectorTableIdentity, ConnectorViewIdentity,
    ConnectorViewRequest, CreatePolicy, DropPolicy,
};
use novarocks_sql::literal::{parse_date_string_to_days, parse_datetime_string_to_micros};
use novarocks_sql::semantic::command::{
    ColumnPositionSql, CommandLiteral, IcebergColumnSqlAction, IcebergSchemaSqlChange,
};
use novarocks_sql::semantic::{DefaultLiteral, ObjectName, TableColumnDef};
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
    properties: &[(String, novarocks_spi::connector::DdlPropertyValue)],
    current_catalog: Option<&str>,
    connector_context: &novarocks_spi::connector::ConnectorRequestContext,
) -> Result<StatementResult, CommandError> {
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
            properties: properties
                .iter()
                .map(|(name, value)| (Arc::from(name.as_str()), value.clone()))
                .collect(),
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

/// Lower complete column defaults through the original typed literal author.
pub(crate) fn lower_semantic_create_table_columns(
    columns: &[novarocks_sql::semantic::command::TableColumnSqlCommand],
) -> Result<Vec<TableColumnDef>, String> {
    columns
        .iter()
        .map(|column| {
            Ok(TableColumnDef {
                name: column.name.clone(),
                nullable: column.nullable.unwrap_or(true),
                default: lower_semantic_default_literal(
                    column.default.as_ref(),
                    &column.data_type,
                )?,
                data_type: column.data_type.clone(),
            })
        })
        .collect::<Result<Vec<_>, String>>()
}

fn lower_semantic_default_literal(
    literal: Option<&CommandLiteral>,
    data_type: &SqlType,
) -> Result<Option<DefaultLiteral>, String> {
    let Some(literal) = literal.filter(|_| !contains_json_leaf(data_type)) else {
        // Preserve the declaration for the Provider's actual-target admission.
        // A JSON target cannot store a default, so there is no default to lower.
        return Ok(None);
    };
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
    Ok(Some(lowered))
}

fn contains_json_leaf(data_type: &SqlType) -> bool {
    match data_type {
        SqlType::Json => true,
        SqlType::Array(element) => contains_json_leaf(element),
        SqlType::Map(key, value) => contains_json_leaf(key) || contains_json_leaf(value),
        SqlType::Struct(fields) => fields.iter().any(|(_, field)| contains_json_leaf(field)),
        _ => false,
    }
}

fn mutation_instance_id(catalog: &str) -> Result<ConnectorInstanceId, String> {
    ConnectorInstanceId::parse(catalog).map_err(|error| error.to_string())
}

pub fn connector_column(column: &TableColumnDef) -> Result<ConnectorColumnDefinition, String> {
    Ok(ConnectorColumnDefinition {
        name: Arc::from(column.name.as_str()),
        data_type: connector_data_type(&column.data_type)?,
        nullable: column.nullable,
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
        SqlType::BoundedCharacter(character) => ConnectorDataType::BoundedCharacter(*character),
        SqlType::Json => ConnectorDataType::Json,
        SqlType::Binary => ConnectorDataType::Binary,
        SqlType::Bitmap => ConnectorDataType::Bitmap,
        SqlType::Hll => ConnectorDataType::Hll,
        SqlType::Object | SqlType::Percentile => {
            return Err(
                "internal opaque value domains are not supported connector column types".into(),
            );
        }
        SqlType::Date => ConnectorDataType::Date,
        SqlType::DateTime => ConnectorDataType::DateTime,
        SqlType::DateTimeNs => ConnectorDataType::DateTimeNs,
        SqlType::Time => ConnectorDataType::Time,
        SqlType::Temporal(declaration) => ConnectorDataType::Temporal(*declaration),
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
        SqlType::Uuid => return Err("UUID is an external source domain, not a SQL DDL type".into()),
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
        DefaultLiteral::Temporal { data_type, value } => ConnectorDefaultValue::Temporal {
            data_type: *data_type,
            value: *value,
        },
        DefaultLiteral::Binary(value) => {
            ConnectorDefaultValue::Binary(Bytes::copy_from_slice(value))
        }
    })
}

// Ownership: `ColumnPath` and `AddPosition` are this module's own parsed
// schema-change AST types, so lowering them onto the connector SPI is catalog
// statement work, not query assembly. Partition transform facts use the
// shared Query Application projection author.
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
) -> Result<StatementResult, CommandError> {
    let normalized_catalog = normalize_identifier(catalog_name)?;
    let instance_id = ConnectorInstanceId::parse(&normalized_catalog)
        .map_err(|error| format!("invalid catalog connector instance ID: {error}"))?;
    novarocks_catalog_application::validate_catalog_drop_name(&instance_id).map_err(|error| {
        CommandError::from_failure_cause(novarocks_failure_contract::SharedFailureCause::new(error))
    })?;
    let application = context.catalog_application().ok_or_else(|| {
        "catalog statements require a configured frontend catalog application".to_string()
    })?;
    // The Frontend application owns the exact-version delete and the MV
    // dependency scan that fences it, both inside one serializable StateStore
    // transaction. Core must not pre-check dependencies outside that fence.
    application
        .drop_catalog(novarocks_catalog_application::CatalogDropCommand {
            instance_id,
            if_exists,
        })
        .map_err(|error| {
            CommandError::from_failure_cause(novarocks_failure_contract::SharedFailureCause::new(
                error,
            ))
        })?;
    Ok(StatementResult::Ok)
}

pub(crate) fn execute_drop_database_statement(
    context: &impl CatalogDropContext,
    name: &ObjectName,
    current_catalog: Option<&str>,
    if_exists: bool,
    force: bool,
    connector_context: &novarocks_spi::connector::ConnectorRequestContext,
) -> Result<StatementResult, CommandError> {
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
        let Some((tables, views)) = enumerate_admitted_force_drop_children(
            &lease,
            ConnectorNamespaceIdentity {
                instance_id: instance_id.clone(),
                namespace: Arc::from(target.namespace.as_str()),
            },
            if_exists,
            connector_context,
        )?
        else {
            return Ok(StatementResult::Ok);
        };
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
                    managed: None,
                },
                connector_context.clone(),
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

type AdmittedForceDropChildren = (Vec<String>, Vec<String>);

fn enumerate_admitted_force_drop_children(
    lease: &novarocks_spi::connector::ConnectorControlPlanningPin,
    namespace_identity: ConnectorNamespaceIdentity,
    if_exists: bool,
    connector_context: &novarocks_spi::connector::ConnectorRequestContext,
) -> Result<Option<AdmittedForceDropChildren>, String> {
    // FORCE is one namespace mutation. Ask the exact owner before its
    // decomposition can encounter a weaker read or child-operation error.
    lease
        .derive_mutation_lease()
        .and_then(|mutation| {
            mutation.admit_operation(
                novarocks_spi::connector::ConnectorMutationOperationId::new(),
                ConnectorCatalogMutationOperation::DropNamespace {
                    namespace: namespace_identity.clone(),
                    policy: if if_exists {
                        DropPolicy::NoOpIfMissing
                    } else {
                        DropPolicy::FailIfMissing
                    },
                },
                connector_context.clone(),
            )
        })
        .map_err(|error| error.to_string())?;
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
            return Ok(None);
        }
        return Err(format!(
            "namespace `{}` does not exist",
            namespace_identity.namespace
        ));
    }
    let mut tables = lease
        .binding()
        .metadata()
        .list_tables(novarocks_spi::connector::ConnectorListTablesRequest {
            namespace: namespace_identity.clone(),
            page_size: novarocks_spi::connector::ConnectorListingPageSize::DEFAULT,
            context: connector_context.clone(),
        })
        .map_err(|error| error.to_string())?
        .into_iter()
        .map(|identity| identity.table.to_string())
        .collect::<Vec<_>>();
    tables.sort();
    // Complete every child enumeration before the first destructive mutation.
    // Unsupported view metadata cannot establish authoritative absence.
    let view_metadata = lease
        .binding()
        .view_metadata()
        .ok_or_else(|| "view enumeration is not supported by this catalog".to_string())?;
    let classified_views = view_metadata
        .list_classified_views(novarocks_spi::connector::ConnectorListViewsRequest {
            namespace: namespace_identity.clone(),
            page_size: novarocks_spi::connector::ConnectorListingPageSize::DEFAULT,
            context: connector_context.clone(),
        })
        .map_err(|error| error.to_string())?;
    let mut views = Vec::new();
    let mut materialized_views = Vec::new();
    for (identity, kind) in classified_views {
        match kind {
            ConnectorObjectKind::View => views.push(identity.view.to_string()),
            ConnectorObjectKind::MaterializedView { .. } => {
                materialized_views.push(identity.view.to_string());
            }
            ConnectorObjectKind::Table => return Err("Unexpected table in view listing".into()),
        }
    }
    if !materialized_views.is_empty() {
        materialized_views.sort();
        return Err(format!(
            "Cannot drop schema {}.{} with CASCADE: materialized views exist: {}",
            namespace_identity.instance_id.as_str(),
            namespace_identity.namespace,
            materialized_views.join(", "),
        ));
    }
    views.sort();
    Ok(Some((tables, views)))
}

pub(crate) fn execute_drop_table_statement(
    context: &impl CatalogDropContext,
    name: &ObjectName,
    current_catalog: Option<&str>,
    current_database: &str,
    if_exists: bool,
    connector_context: &novarocks_spi::connector::ConnectorRequestContext,
) -> Result<StatementResult, CommandError> {
    let target = match crate::catalog_application::resolver::resolve_existing_table_target(
        context,
        name,
        current_catalog,
        current_database,
    ) {
        Ok(target) => target,
        Err(crate::catalog_application::resolver::TargetResolutionError::Message(_))
            if current_catalog.is_none() && name.parts.len() <= 2 =>
        {
            // External parquet tables registered through the embedding API are
            // still catalog-only entries. Dropping them does not involve a
            // connector backend.
            return drop_local_catalog_table(context, name, current_database, if_exists);
        }
        Err(err) => return Err((err).into()),
    };
    let exact_lease = crate::connector::acquire_metadata_planning_pin(
        context.connector_control(),
        &target.catalog,
    )?;
    let missing_table = |error: CommandError| {
        if target.provider_id.as_str() == "iceberg" {
            let kind = external_view_kind(&exact_lease, &target, connector_context)?;
            if let Some(hint) = drop_table_view_hint(&target, kind) {
                return Err(hint.into());
            }
            if if_exists {
                cleanup_iceberg_drop_table_registration_if_exists(context, &target)?;
                return Ok(StatementResult::Ok);
            }
        } else if if_exists {
            return Ok(StatementResult::Ok);
        }
        Err(error)
    };
    match crate::mv::domain::iceberg_guard::check_iceberg_mv_table_mutation(
        &exact_lease,
        context.mv_readiness(),
        &target,
        crate::mv::domain::iceberg_guard::IcebergMvUserMutation::DropTable,
        connector_context.clone(),
    ) {
        Ok(()) => {}
        Err(error) if error.target_missing() => {
            return missing_table(error.to_string().into());
        }
        Err(error) => return Err(error.to_string().into()),
    }
    let metadata = match crate::connector::metadata_load_connector_table_with_planning_pin_typed(
        &exact_lease,
        connector_context.clone(),
        &target.namespace,
        &target.table,
        novarocks_spi::connector::ConnectorTableResolution::StrictBaseTable,
    ) {
        Ok(metadata) => metadata,
        Err(error)
            if target.provider_id.as_str() == "iceberg"
                && error.kind() == ConnectorErrorKind::NotFound =>
        {
            return missing_table(error.to_string().into());
        }
        Err(error) => return Err(error.to_string().into()),
    };
    let revision = exact_lease
        .binding()
        .metadata()
        .exact_semantic_revision(
            &metadata.table,
            novarocks_spi::connector::ConnectorReadSelector::Current,
        )
        .map_err(|error| {
            format!(
                "observe exact dependency identity for {}.{}.{}: {error}",
                target.catalog, target.namespace, target.table
            )
        })?;
    let object_id = novarocks_spi::connector::ConnectorTableObjectId::try_new(
        revision.object_identity().value().clone(),
    )
    .map_err(|error| error.to_string())?;
    let dependency_identity = novarocks_mv_application::dependency::MvDependencyObjectIdentity::new(
        target.catalog.clone(),
        object_id,
    );
    context
        .mv_readiness()
        .ensure_no_ready_downstream_dependencies(&dependency_identity)
        .map_err(|error| error.to_string())?;
    let instance_id = mutation_instance_id(&target.catalog)?;
    match crate::connector::mutation::resolve_catalog_mutation(
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
            managed: None,
        },
        connector_context.clone(),
    ) {
        crate::connector::mutation::ResolvedCatalogMutation::KnownCommitted(completed) => {
            if let novarocks_spi::connector::ExternalMutationFinalization::Failed(failure) =
                completed.finalization
            {
                return Err((novarocks_query_application::engine_error::EngineError::commit_known_committed_finalize_failed(failure.to_string()).to_string()).into());
            }
            if target.provider_id.as_str() == "iceberg" {
                drop_local_table_registration_if_exists(context, &target.namespace, &target.table)?;
            }
            Ok(StatementResult::Ok)
        }
        crate::connector::mutation::ResolvedCatalogMutation::KnownUncommitted {
            failure,
            cleanup,
        } => drop_table_failure_with_view_check(failure, &cleanup, missing_table),
        crate::connector::mutation::ResolvedCatalogMutation::CommitUnknown { failure, .. } => Err(
            (novarocks_query_application::engine_error::EngineError::commit_unknown(
                failure.to_string(),
            )
            .to_bracketed_user_message())
            .into(),
        ),
        crate::connector::mutation::ResolvedCatalogMutation::ContractFailure { error, .. } => {
            Err((error.to_string()).into())
        }
    }
}

fn drop_table_failure_with_view_check(
    failure: novarocks_spi::connector::ConnectorMutationFailure,
    cleanup: &novarocks_spi::connector::ExternalMutationFinalization,
    missing_table: impl FnOnce(CommandError) -> Result<StatementResult, CommandError>,
) -> Result<StatementResult, CommandError> {
    let error: CommandError =
        novarocks_query_application::engine_error::EngineError::commit_known_uncommitted(
            crate::connector::mutation::known_uncommitted_message(&failure, cleanup),
        )
        .to_string()
        .into();
    if failure.kind() == novarocks_spi::connector::ConnectorMutationFailureKind::NotFound
        && matches!(
            cleanup,
            novarocks_spi::connector::ExternalMutationFinalization::Complete
        )
    {
        return missing_table(error);
    }
    Err(error)
}

fn cleanup_iceberg_drop_table_registration_if_exists(
    context: &impl CatalogDropContext,
    target: &crate::catalog_application::resolver::TargetBackend,
) -> Result<(), String> {
    drop_local_table_registration_if_exists(context, &target.namespace, &target.table)
}

fn drop_local_catalog_table(
    context: &impl CatalogDropContext,
    name: &ObjectName,
    current_database: &str,
    if_exists: bool,
) -> Result<StatementResult, CommandError> {
    let resolved = resolve_local_table_name(name.parts.as_slice(), current_database)?;
    let mut guard = context
        .catalog_service()
        .local()
        .write()
        .expect("standalone catalog write lock");
    match guard.drop_table(&resolved.database, &resolved.table) {
        Ok(()) => Ok(StatementResult::Ok),
        Err(err) if if_exists && err.contains("unknown") => Ok(StatementResult::Ok),
        Err(err) => Err((err).into()),
    }
}

fn ensure_no_iceberg_mv_targets_in_scope(
    context: &impl CatalogDropContext,
    scope_catalog: &str,
    scope_namespace: Option<&str>,
) -> Result<(), String> {
    crate::mv::domain::dependency_resolver::ensure_no_iceberg_mv_targets_in_scope_with_readiness(
        context.mv_readiness(),
        scope_catalog,
        scope_namespace,
    )
}

fn ensure_no_external_iceberg_dependents(
    context: &impl CatalogDropContext,
    scope_catalog: &str,
    scope_namespace: Option<&str>,
) -> Result<(), String> {
    crate::mv::domain::dependency_resolver::ensure_no_external_iceberg_dependents_with_readiness(
        context.mv_readiness(),
        scope_catalog,
        scope_namespace,
    )
}

fn external_view_kind(
    lease: &novarocks_spi::connector::ConnectorControlPlanningPin,
    target: &crate::catalog_application::resolver::TargetBackend,
    connector_context: &novarocks_spi::connector::ConnectorRequestContext,
) -> Result<Option<ConnectorObjectKind>, String> {
    let binding = lease.binding();
    let Some(view_metadata) = binding.view_metadata() else {
        return Ok(None);
    };
    let instance_id = binding.descriptor().instance_id.clone();
    match view_metadata.classify_view(ConnectorViewRequest {
        view: ConnectorViewIdentity {
            instance_id,
            namespace: Arc::from(target.namespace.as_str()),
            view: Arc::from(target.table.as_str()),
        },
        context: crate::connector::context_for_planning_pin(lease, connector_context.clone())?,
    }) {
        Err(error)
            if matches!(
                error.kind(),
                ConnectorErrorKind::NotFound | ConnectorErrorKind::Unsupported
            ) =>
        {
            Ok(None)
        }
        Err(error) => Err(error.to_string()),
        Ok(kind) => Ok(kind),
    }
}

fn drop_table_view_hint(
    target: &crate::catalog_application::resolver::TargetBackend,
    kind: Option<ConnectorObjectKind>,
) -> Option<String> {
    let kind = kind?;
    let name = format!("{}.{}.{}", target.catalog, target.namespace, target.table);
    match kind {
        ConnectorObjectKind::View => Some(format!("{name} is a view, use DROP VIEW")),
        ConnectorObjectKind::MaterializedView { .. } => Some(format!(
            "Table '{name}' does not exist, but a materialized view with that name exists. Did you mean DROP MATERIALIZED VIEW {name}?"
        )),
        ConnectorObjectKind::Table => None,
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
                default: lower_semantic_default_literal(default.as_ref(), data_type)?,
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
        SqlType::String | SqlType::BoundedCharacter(_) => {
            Ok(DefaultLiteral::String(value.to_string()))
        }
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
        SqlType::Date => Ok(DefaultLiteral::Date(parse_date_string_to_days(value)?)),
        SqlType::DateTime => Ok(DefaultLiteral::DateTime(parse_datetime_string_to_micros(
            value,
        )?)),
        SqlType::DateTimeNs => Ok(DefaultLiteral::DateTime(typed_datetime_string_to_nanos(
            value,
        )?)),
        SqlType::Temporal(ty) => {
            use novarocks_functions::temporal_value::{
                parse_time_literal, parse_timestamp_literal,
            };
            use novarocks_type_contract::TemporalType;
            let parsed = match ty {
                TemporalType::Time(_) | TemporalType::TimeWithTimeZone(_) => {
                    parse_time_literal(value)
                }
                TemporalType::Timestamp(_) | TemporalType::TimestampWithTimeZone(_) => {
                    parse_timestamp_literal(value)
                }
                TemporalType::Interval(_) => {
                    return Err("Iceberg cannot store an interval DEFAULT".into());
                }
            }
            .map_err(|error| error.to_string())?;
            Ok(DefaultLiteral::Temporal {
                data_type: parsed.ty,
                value: parsed.value,
            })
        }
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
        | (DefaultLiteral::String(_), SqlType::String | SqlType::BoundedCharacter(_))
        | (DefaultLiteral::Binary(_), SqlType::Binary | SqlType::Bitmap | SqlType::Hll)
        | (DefaultLiteral::Date(_), SqlType::Date)
        | (DefaultLiteral::DateTime(_), SqlType::DateTime | SqlType::DateTimeNs) => {}
        (DefaultLiteral::Temporal { data_type, value }, SqlType::Temporal(target))
            if std::mem::discriminant(data_type) == std::mem::discriminant(target) =>
        {
            value
                .validate(*data_type)
                .map_err(|error| error.to_string())?;
        }
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
mod force_drop_admission_tests {
    use super::*;
    use novarocks_spi::connector::ConnectorObjectKind;
    use novarocks_spi::connector::*;
    use std::sync::Mutex;

    struct Capability {
        descriptor: ConnectorInstanceDescriptor,
        admitted: bool,
        exists: bool,
        views_supported: bool,
        table_count: usize,
        calls: Mutex<Vec<&'static str>>,
    }
    impl ConnectorMetadata for Capability {
        fn instance_id(&self) -> &ConnectorInstanceId {
            &self.descriptor.instance_id
        }
        fn namespace_exists(&self, _: ConnectorNamespaceRequest) -> Result<bool, ConnectorError> {
            self.calls.lock().unwrap().push("exists");
            Ok(self.exists)
        }
        fn table_exists(&self, _: ConnectorTableRequest) -> Result<bool, ConnectorError> {
            panic!("unexpected table lookup")
        }
        fn list_tables(
            &self,
            request: ConnectorListTablesRequest,
        ) -> Result<Vec<ConnectorTableIdentity>, ConnectorError> {
            self.calls.lock().unwrap().push("tables");
            let tables = if self.table_count == 2 {
                vec!["z".to_owned(), "a".to_owned()]
            } else {
                (0..self.table_count).map(|i| format!("t{i:08}")).collect()
            };
            Ok(tables
                .into_iter()
                .map(|table| ConnectorTableIdentity {
                    instance_id: self.descriptor.instance_id.clone(),
                    namespace: request.namespace.namespace.clone(),
                    table: table.into(),
                })
                .collect())
        }
        fn load_table(
            &self,
            _: ConnectorTableRequest,
            _creation: &mut novarocks_spi::connector::read_stack::ConnectorReadSchemaCreation<'_>,
        ) -> Result<
            ConnectorTableMetadata,
            novarocks_connector_contract::PureProviderCompileError<ConnectorError>,
        > {
            panic!("unexpected table lookup")
        }
    }
    impl ConnectorScanPlanning for Capability {
        fn instance_id(&self) -> &ConnectorInstanceId {
            &self.descriptor.instance_id
        }
        fn begin_scan(
            &self,
            _: &ConnectorTableHandle,
            _: ConnectorBeginScanRequest,
        ) -> Result<ConnectorScan, ConnectorError> {
            panic!("unexpected scan")
        }
        fn plan_splits(
            &self,
            _: &ConnectorScanHandle,
            _: ConnectorSplitPlanningRequest,
        ) -> Result<ConnectorSplitPlanningResult, ConnectorError> {
            panic!("unexpected scan")
        }
    }
    impl ConnectorExecutionDistribution for Capability {
        fn declaration(
            &self,
            _: &ConnectorRequestContext,
        ) -> Result<ConnectorProviderBinding, ConnectorError> {
            panic!("unexpected distribution")
        }
    }
    impl ConnectorCatalogMutation for Capability {
        fn descriptor(&self) -> &ConnectorInstanceDescriptor {
            &self.descriptor
        }
        fn incarnation(&self) -> ProviderBindingEpoch {
            ProviderBindingEpoch::from_bytes([1; 16])
        }
        fn admit(&self, request: &ConnectorCatalogMutationRequest) -> Result<(), ConnectorError> {
            self.calls.lock().unwrap().push("admit");
            assert!(
                matches!(&request.operation, ConnectorCatalogMutationOperation::DropNamespace { namespace, .. } if namespace.namespace.as_ref() == "db")
            );
            if self.admitted {
                Ok(())
            } else {
                Err(ConnectorError::new(
                    ConnectorErrorKind::Unsupported,
                    "read-only compatibility entry",
                ))
            }
        }
        fn execute(
            &self,
            _: ConnectorCatalogMutationRequest,
        ) -> Result<ExternalMutationOutcome<ConnectorCatalogMutationReceipt>, ConnectorError>
        {
            panic!("enumeration must finish before any child mutation")
        }
        fn reconcile(
            &self,
            _: ConnectorCatalogMutationReconcileRequest,
        ) -> Result<ExternalMutationOutcome<ConnectorCatalogMutationReceipt>, ConnectorError>
        {
            panic!("unexpected reconcile")
        }
    }
    impl ConnectorViewMetadata for Capability {
        fn classify_view(
            &self,
            _: ConnectorViewRequest,
        ) -> Result<Option<ConnectorObjectKind>, ConnectorError> {
            panic!("unexpected view classification")
        }

        fn list_classified_views(
            &self,
            request: ConnectorListViewsRequest,
        ) -> Result<Vec<(ConnectorViewIdentity, ConnectorObjectKind)>, ConnectorError> {
            self.list_views(request).map(|views| {
                views
                    .into_iter()
                    .map(|view| (view, ConnectorObjectKind::View))
                    .collect()
            })
        }

        fn descriptor(&self) -> &ConnectorInstanceDescriptor {
            &self.descriptor
        }
        fn incarnation(&self) -> ProviderBindingEpoch {
            ProviderBindingEpoch::from_bytes([1; 16])
        }
        fn view_exists(&self, _: ConnectorViewRequest) -> Result<bool, ConnectorError> {
            panic!("unexpected view lookup")
        }
        fn load_view(
            &self,
            _: ConnectorViewRequest,
            _: &mut novarocks_spi::connector::read_stack::ConnectorReadSchemaCreation<'_>,
        ) -> Result<
            ConnectorViewMetadataValue,
            novarocks_connector_contract::PureProviderCompileError<ConnectorError>,
        > {
            panic!("unexpected view lookup")
        }
        fn list_views(
            &self,
            request: ConnectorListViewsRequest,
        ) -> Result<Vec<ConnectorViewIdentity>, ConnectorError> {
            self.calls.lock().unwrap().push("views");
            if !self.views_supported {
                return Err(ConnectorError::new(
                    ConnectorErrorKind::Unsupported,
                    "view enumeration is unsupported",
                ));
            }
            Ok(["v2", "v1"]
                .into_iter()
                .map(|view| ConnectorViewIdentity {
                    instance_id: self.descriptor.instance_id.clone(),
                    namespace: request.namespace.namespace.clone(),
                    view: view.into(),
                })
                .collect())
        }
    }

    #[test]
    fn force_drop_admits_before_reads_and_keeps_complete_enumeration() {
        for (admitted, exists, views_supported, table_count) in [
            (false, true, false, 2),
            (true, false, true, 2),
            (true, true, false, 2),
            (true, true, true, 2),
            (true, true, true, 65_537),
        ] {
            let descriptor = ConnectorInstanceDescriptor {
                provider_id: ConnectorProviderId::parse("iceberg").unwrap(),
                instance_id: ConnectorInstanceId::parse("catalog").unwrap(),
            };
            let capability = Arc::new(Capability {
                descriptor: descriptor.clone(),
                admitted,
                exists,
                views_supported,
                table_count,
                calls: Mutex::new(Vec::new()),
            });
            let binding = ConnectorControlBinding::try_new(
                descriptor.clone(),
                ProviderBindingEpoch::from_bytes([1; 16]),
                capability.clone(),
                capability.clone(),
                capability.clone(),
                Some(capability.clone()),
            )
            .unwrap()
            .try_with_view_metadata(Some(capability.clone()))
            .unwrap();
            let lease = ConnectorControlPlanningPin::new(Arc::new(binding), || {});
            let context =
                crate::connector::connector_request_context(None, ConnectorStopOwner::new().view())
                    .unwrap();
            let result = enumerate_admitted_force_drop_children(
                &lease,
                ConnectorNamespaceIdentity {
                    instance_id: descriptor.instance_id,
                    namespace: "db".into(),
                },
                true,
                &context,
            );
            if !admitted {
                assert!(
                    result
                        .unwrap_err()
                        .contains("read-only compatibility entry")
                );
                assert_eq!(*capability.calls.lock().unwrap(), ["admit"]);
            } else if !exists {
                assert_eq!(result.unwrap(), None);
                assert_eq!(*capability.calls.lock().unwrap(), ["admit", "exists"]);
            } else if !views_supported {
                assert!(
                    result
                        .unwrap_err()
                        .contains("view enumeration is unsupported")
                );
                assert_eq!(
                    *capability.calls.lock().unwrap(),
                    ["admit", "exists", "tables", "views"]
                );
            } else {
                assert_eq!(
                    result.unwrap(),
                    Some((
                        if table_count == 2 {
                            vec!["a".to_string(), "z".to_string()]
                        } else {
                            (0..table_count).map(|i| format!("t{i:08}")).collect()
                        },
                        vec!["v1".to_string(), "v2".to_string()]
                    ))
                );
                assert_eq!(
                    *capability.calls.lock().unwrap(),
                    ["admit", "exists", "tables", "views"]
                );
            }
        }
    }
}

#[cfg(test)]
mod drop_table_if_exists_tests {
    struct RefuseBeforeExternalAdmission;
    impl crate::catalog_application::resolver::CatalogAdmission for RefuseBeforeExternalAdmission {
        fn catalog_application(
            &self,
        ) -> Option<&dyn novarocks_catalog_application::CatalogApplicationPort> {
            panic!("virtual DROP must not request external catalog admission")
        }
    }
    impl crate::catalog_application::query_catalog::CatalogServiceSource
        for RefuseBeforeExternalAdmission
    {
        fn catalog_service(
            &self,
        ) -> &std::sync::Arc<crate::catalog_application::query_catalog::QueryCatalogService>
        {
            panic!("virtual DROP must not enter the local table fallback")
        }
    }
    impl super::CatalogDropContext for RefuseBeforeExternalAdmission {
        fn connector_control(&self) -> &dyn novarocks_spi::connector::ConnectorControlRegistry {
            panic!("virtual DROP must not enter a connector")
        }
        fn mv_readiness(&self) -> &crate::mv::domain::readiness::MvReadinessPort {
            panic!("virtual DROP must not enter MV readiness")
        }
    }
    #[test]
    fn conditional_drop_cannot_hide_virtual_targets_in_the_local_fallback() {
        use novarocks_failure_contract::{FailureClass, FailureProjection};
        for if_exists in [false, true] {
            for parts in [
                vec!["information_schema", "tables"],
                vec!["system", "metadata", "materialized_views"],
            ] {
                let name = novarocks_sql::semantic::ObjectName {
                    parts: parts.into_iter().map(str::to_owned).collect(),
                };
                let error = super::execute_drop_table_statement(
                    &RefuseBeforeExternalAdmission,
                    &name,
                    None,
                    "db",
                    if_exists,
                    &crate::connector::test_request_context(),
                )
                .unwrap_err();
                assert_eq!(
                    error.failure_cause().unwrap().failure_fact().class(),
                    FailureClass::Unsupported
                );
            }
            let error = super::execute_drop_catalog_statement(
                &RefuseBeforeExternalAdmission,
                "SYSTEM",
                if_exists,
            )
            .unwrap_err();
            assert_eq!(
                error.failure_cause().unwrap().failure_fact().class(),
                FailureClass::Unsupported
            );
        }
    }

    #[test]
    fn non_missing_mutation_failures_preserve_the_error_without_view_io() {
        use novarocks_spi::connector::{
            ConnectorMutationFailure, ConnectorMutationFailureKind, ExternalMutationFinalization,
        };
        for kind in [
            ConnectorMutationFailureKind::Unsupported,
            ConnectorMutationFailureKind::Unavailable,
            ConnectorMutationFailureKind::PermissionDenied,
        ] {
            let message = "read-only compatibility entry";
            let result = super::drop_table_failure_with_view_check(
                ConnectorMutationFailure::new(kind, message),
                &ExternalMutationFinalization::Complete,
                |_| panic!("non-NotFound failure must not query a view"),
            );
            assert!(result.unwrap_err().to_string().contains(message));
        }
    }

    #[test]
    fn missing_table_mutation_with_incomplete_cleanup_preserves_the_failure() {
        use novarocks_spi::connector::{
            ConnectorMutationFailure, ConnectorMutationFailureKind, ExternalMutationFinalization,
        };
        let error = super::drop_table_failure_with_view_check(
            ConnectorMutationFailure::new(ConnectorMutationFailureKind::NotFound, "table missing"),
            &ExternalMutationFinalization::Failed(ConnectorMutationFailure::new(
                ConnectorMutationFailureKind::Unavailable,
                "cleanup failed",
            )),
            |_| panic!("a hint must not mask the cleanup failure"),
        )
        .unwrap_err()
        .to_string();
        assert!(error.contains("table missing"));
        assert!(error.contains("cleanup failed"));
    }

    #[test]
    fn missing_table_mutation_uses_classified_view_hints() {
        use novarocks_spi::connector::{
            ConnectorMutationFailure, ConnectorMutationFailureKind, ConnectorObjectKind,
            ConnectorProviderId, ConnectorStorageTableIdentifier, ExternalMutationFinalization,
        };
        let target = crate::catalog_application::resolver::TargetBackend {
            provider_id: ConnectorProviderId::parse("iceberg").unwrap(),
            catalog: "ice".into(),
            namespace: "db".into(),
            table: "v".into(),
        };
        let materialized = ConnectorObjectKind::MaterializedView {
            storage_table: ConnectorStorageTableIdentifier {
                namespace: vec!["db".into()].into(),
                name: "storage".into(),
                catalog: None,
            },
        };
        for (kind, hint) in [
            (
                Some(ConnectorObjectKind::View),
                Some("ice.db.v is a view, use DROP VIEW"),
            ),
            (
                Some(materialized),
                Some(
                    "Table 'ice.db.v' does not exist, but a materialized view with that name exists. Did you mean DROP MATERIALIZED VIEW ice.db.v?",
                ),
            ),
            (None, None),
        ] {
            let error = super::drop_table_failure_with_view_check(
                ConnectorMutationFailure::new(
                    ConnectorMutationFailureKind::NotFound,
                    "table missing",
                ),
                &ExternalMutationFinalization::Complete,
                |original| {
                    Err(super::drop_table_view_hint(&target, kind).map_or(original, Into::into))
                },
            )
            .unwrap_err()
            .to_string();
            if let Some(hint) = hint {
                assert_eq!(error, hint);
            } else {
                assert!(error.contains("table missing"));
            }
        }
    }

    #[test]
    fn only_exact_table_binding_absence_is_a_missing_drop_target() {
        use crate::mv::domain::iceberg_guard::IcebergMvMutationGuardError;
        use novarocks_spi::connector::{ConnectorError, ConnectorErrorKind};
        assert!(
            IcebergMvMutationGuardError::TableBinding(ConnectorError::new(
                ConnectorErrorKind::NotFound,
                "opaque connector detail",
            ))
            .target_missing()
        );
        assert!(
            !IcebergMvMutationGuardError::TableBinding(ConnectorError::new(
                ConnectorErrorKind::Unavailable,
                "table not found",
            ))
            .target_missing()
        );
        assert!(
            !IcebergMvMutationGuardError::Other("unknown table: db.missing".into())
                .target_missing()
        );
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn m07_internal_opaque_domains_are_refused_before_connector_schema_admission() {
        use novarocks_types::schema::SqlType as T;
        for domain in [T::Object, T::Percentile] {
            for ty in [
                domain.clone(),
                T::Array(Box::new(domain.clone())),
                T::Map(Box::new(T::String), Box::new(domain.clone())),
                T::Struct(vec![("value".into(), domain)]),
            ] {
                assert!(super::connector_data_type(&ty).is_err());
            }
        }
        assert!(super::connector_data_type(&T::Binary).is_ok());
    }
    #[test]
    fn semantic_create_table_columns_preserve_typed_defaults() {
        use novarocks_sql::semantic::command::{CommandLiteral, TableColumnSqlCommand};
        let columns = super::lower_semantic_create_table_columns(&[
            TableColumnSqlCommand {
                name: "id".into(),
                data_type: super::SqlType::BigInt,
                nullable: None,
                default: Some(CommandLiteral::Number("3".into())),
                comment: None,
            },
            TableColumnSqlCommand {
                name: "amount".into(),
                data_type: super::SqlType::Decimal {
                    precision: 10,
                    scale: 2,
                },
                nullable: None,
                default: Some(CommandLiteral::String("12.30".into())),
                comment: None,
            },
            TableColumnSqlCommand {
                name: "payload".into(),
                data_type: super::SqlType::Binary,
                nullable: None,
                default: Some(CommandLiteral::Binary(vec![0xCA, 0xFE])),
                comment: None,
            },
        ])
        .expect("lower original typed column declarations");
        assert_eq!(columns[0].default, Some(super::DefaultLiteral::Int(3)));
        assert_eq!(
            columns[1].default,
            Some(super::DefaultLiteral::Decimal {
                unscaled: 1230,
                scale: 2
            })
        );
        assert_eq!(
            columns[2].default,
            Some(super::DefaultLiteral::Binary(vec![0xCA, 0xFE]))
        );
        let projected = columns
            .iter()
            .map(super::connector_column)
            .collect::<Result<Vec<_>, _>>()
            .unwrap();
        assert_eq!(
            projected[0].default,
            Some(super::ConnectorDefaultValue::Int(3))
        );
        assert_eq!(
            projected[1].default,
            Some(super::ConnectorDefaultValue::Decimal {
                unscaled: 1230,
                scale: 2
            })
        );
        assert_eq!(
            projected[2].default,
            Some(super::ConnectorDefaultValue::Binary(
                bytes::Bytes::from_static(&[0xCA, 0xFE])
            ))
        );
    }

    #[test]
    fn json_create_and_alter_defaults_leave_the_provider_declaration_intact() {
        use super::{IcebergSchemaChange, SqlType};
        use novarocks_spi::connector::ConnectorDataType;
        use novarocks_sql::semantic::command::{
            ColumnPositionSql, CommandLiteral, IcebergSchemaSqlChange, TableColumnSqlCommand,
        };

        for (data_type, expected) in [
            (SqlType::Json, ConnectorDataType::Json),
            (
                SqlType::Array(Box::new(SqlType::Json)),
                ConnectorDataType::Array(Box::new(ConnectorDataType::Json)),
            ),
        ] {
            let raw = Some(CommandLiteral::String("not-a-json".into()));
            let columns = super::lower_semantic_create_table_columns(&[TableColumnSqlCommand {
                name: "payload".into(),
                data_type: data_type.clone(),
                nullable: None,
                default: raw.clone(),
                comment: None,
            }])
            .expect("the provider settles whether the declaration is used");
            let column = super::connector_column(&columns[0]).unwrap();
            assert_eq!(column.data_type, expected);
            assert_eq!(column.default, None);

            let add =
                super::lower_semantic_iceberg_schema_change(&IcebergSchemaSqlChange::AddColumn {
                    path: vec!["payload".into()],
                    data_type: data_type.clone(),
                    nullable: None,
                    default: raw,
                    position: ColumnPositionSql::Default,
                })
                .unwrap();
            assert!(matches!(
                add,
                IcebergSchemaChange::AddColumn { data_type: actual, default: None, .. }
                    if actual == data_type
            ));
        }
    }

    #[test]
    fn unprepared_compound_create_requires_evaluated_property_preparation() {
        let sql = "CREATE TABLE IF NOT EXISTS ice.db.orders (id BIGINT DEFAULT 3, amount DECIMAL(10,2) DEFAULT '12.30', payload BINARY DEFAULT X'CAFE') COMMENT 'orders' WITH (partitioning = ARRAY['month(id)'], format_version = 2)";
        let statement = novarocks_query_application::sql::parse_single_statement(sql)
            .expect("parse original compound CREATE source");
        use novarocks_query_application::sql::trino_admission::{
            TrinoStatementAction, TrinoStatementPreparation, admit_trino_statement,
        };
        assert!(matches!(
            admit_trino_statement(sql, statement).unwrap(),
            TrinoStatementAction::Prepare(TrinoStatementPreparation::CreateTable(_)),
        ));
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

    fn semantic_iceberg_action(
        sql: &str,
    ) -> novarocks_sql::semantic::command::IcebergTableSqlAction {
        let statement = novarocks_query_application::sql::parse_single_statement(sql)
            .expect("parse semantic Iceberg command");
        let novarocks_parser::ast::TrinoStatement::AddColumn(statement) = statement else {
            panic!("expected typed ADD COLUMN");
        };
        let command =
            novarocks_query_application::sql::trino_table_command::lower_trino_add_column(
                sql,
                &statement,
                &novarocks_sql::compiler::SqlCompileControl::unbounded(),
            )
            .unwrap();
        let novarocks_sql::semantic::CatalogSqlCommand::AlterIcebergTable(statement) = command
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

#[cfg(test)]
pub(crate) mod external_listing_tests {
    use std::sync::{Arc, Mutex};

    use novarocks_catalog_application::{
        CatalogAdmission, CatalogApplicationError, CatalogApplicationPort, CatalogCreateCommand,
        CatalogDropCommand, CatalogRuntimeObservation,
    };
    use novarocks_spi::connector::*;

    use super::{CatalogDropContext, execute_drop_database_statement};
    use crate::catalog_application::query_catalog::{
        CatalogServiceSource, QueryCatalogService, new_query_catalog_service,
    };
    use crate::query_execution::compiler::TestConnectorControlRegistry;

    #[derive(Clone, Copy, Debug, PartialEq, Eq)]
    pub(crate) enum FailurePoint {
        NamespaceExists,
        Namespaces,
        Tables,
        SecondNamespaceTables,
        Views,
        MaterializedViews,
    }

    pub(crate) fn error_kinds() -> [ConnectorErrorKind; 3] {
        [
            ConnectorErrorKind::DeadlineExceeded,
            ConnectorErrorKind::Cancelled,
            ConnectorErrorKind::ResourceExhausted,
        ]
    }

    #[derive(Default)]
    pub(crate) struct ListingMetadataGate {
        started: std::sync::atomic::AtomicBool,
        ready: tokio::sync::Notify,
        released: Mutex<bool>,
        changed: std::sync::Condvar,
    }
    impl ListingMetadataGate {
        fn run(&self) {
            self.started
                .store(true, std::sync::atomic::Ordering::Release);
            self.ready.notify_waiters();
            let mut released = self.released.lock().unwrap();
            while !*released {
                released = self.changed.wait(released).unwrap();
            }
        }
        pub(crate) async fn wait_started(&self) {
            let ready = self.ready.notified();
            tokio::pin!(ready);
            ready.as_mut().enable();
            if !self.started.load(std::sync::atomic::Ordering::Acquire) {
                ready.await;
            }
        }
        pub(crate) fn release(&self) {
            *self.released.lock().unwrap() = true;
            self.changed.notify_all();
        }
    }

    pub(crate) struct ListingFixture {
        pub(crate) registry: Arc<TestConnectorControlRegistry>,
        pub(crate) catalog_service: Arc<QueryCatalogService>,
        provider: Arc<ListingProvider>,
    }

    impl ListingFixture {
        pub(crate) fn new(failure: FailurePoint, kind: ConnectorErrorKind) -> Self {
            let provider = Arc::new(ListingProvider {
                descriptor: ConnectorInstanceDescriptor {
                    provider_id: ConnectorProviderId::parse("paimon").unwrap(),
                    instance_id: ConnectorInstanceId::parse("catalog").unwrap(),
                },
                incarnation: ProviderBindingEpoch::new(),
                failure,
                kind,
                calls: Arc::new(Mutex::new(Vec::new())),
                metadata_in_views: std::sync::atomic::AtomicBool::new(false),
                metadata_gate: Mutex::new(None),
                held_view: Mutex::new(None),
                stop_in_view_poll: Mutex::new(None),
            });
            let binding = ConnectorControlBinding::try_new(
                provider.descriptor.clone(),
                provider.incarnation,
                provider.clone(),
                provider.clone(),
                provider.clone(),
                Some(provider.clone()),
            )
            .unwrap()
            .try_with_view_metadata(Some(provider.clone()))
            .unwrap();
            let registry = Arc::new(TestConnectorControlRegistry::default());
            registry.register(binding).unwrap();
            Self {
                registry,
                catalog_service: Arc::new(new_query_catalog_service()),
                provider,
            }
        }

        pub(crate) fn stop_inside_view_poll(
            &self,
            original: novarocks_workload_control::WorkCancellationRequester,
        ) {
            *self.provider.stop_in_view_poll.lock().unwrap() = Some(original);
        }

        pub(crate) fn hold_metadata_cpu(&self, gate: Arc<ListingMetadataGate>) {
            *self.provider.metadata_gate.lock().unwrap() = Some(gate);
            self.form_metadata_in_views();
        }
        pub(crate) fn form_metadata_in_views(&self) {
            self.provider
                .metadata_in_views
                .store(true, std::sync::atomic::Ordering::Release);
        }

        pub(crate) fn hold_views(&self, gate: Arc<tokio::sync::Notify>) {
            *self.provider.held_view.lock().unwrap() = Some(gate);
        }

        pub(crate) fn expected_error(&self) -> String {
            self.provider.error().to_string()
        }

        pub(crate) fn calls(&self) -> Vec<String> {
            self.provider.calls.lock().unwrap().clone()
        }
    }

    impl CatalogApplicationPort for ListingFixture {
        fn create_catalog(
            &self,
            _: CatalogCreateCommand,
        ) -> Result<CatalogRuntimeObservation, CatalogApplicationError> {
            unreachable!("listing must not create a catalog")
        }

        fn drop_catalog(&self, _: CatalogDropCommand) -> Result<(), CatalogApplicationError> {
            unreachable!("listing must not drop a catalog")
        }

        fn admit_catalog(&self, instance_id: &ConnectorInstanceId) -> CatalogAdmission {
            assert_eq!(instance_id, &self.provider.descriptor.instance_id);
            CatalogAdmission::Ready(CatalogRuntimeObservation {
                attachment_id: uuid::Uuid::nil(),
                instance_id: instance_id.clone(),
                provider_id: self.provider.descriptor.provider_id.clone(),
                generation: 1,
            })
        }
    }

    impl crate::catalog_application::resolver::CatalogAdmission for ListingFixture {
        fn catalog_application(&self) -> Option<&dyn CatalogApplicationPort> {
            Some(self)
        }
    }

    impl CatalogServiceSource for ListingFixture {
        fn catalog_service(&self) -> &Arc<QueryCatalogService> {
            &self.catalog_service
        }
    }

    impl CatalogDropContext for ListingFixture {
        fn connector_control(&self) -> &dyn ConnectorControlRegistry {
            self.registry.as_ref()
        }

        fn mv_readiness(&self) -> &crate::mv::domain::readiness::MvReadinessPort {
            unreachable!("the non-Iceberg fixture does not consult MV readiness")
        }
    }

    struct ListingProvider {
        descriptor: ConnectorInstanceDescriptor,
        incarnation: ProviderBindingEpoch,
        failure: FailurePoint,
        kind: ConnectorErrorKind,
        calls: Arc<Mutex<Vec<String>>>,
        metadata_in_views: std::sync::atomic::AtomicBool,
        metadata_gate: Mutex<Option<Arc<ListingMetadataGate>>>,
        held_view: Mutex<Option<Arc<tokio::sync::Notify>>>,
        stop_in_view_poll: Mutex<Option<novarocks_workload_control::WorkCancellationRequester>>,
    }

    impl ListingProvider {
        fn error(&self) -> ConnectorError {
            ConnectorError::new(
                self.kind,
                format!("injected {:?} listing failure", self.failure),
            )
        }

        fn call(&self, name: impl Into<String>) {
            self.calls.lock().unwrap().push(name.into());
        }
    }

    impl ConnectorMetadata for ListingProvider {
        fn provider_metadata_cpu_author(
            &self,
        ) -> Option<(ConnectorProviderBindingKey, std::any::TypeId)> {
            Some((
                ConnectorProviderBindingKey {
                    instance_id: self.descriptor.instance_id.clone(),
                    incarnation: self.incarnation,
                },
                std::any::TypeId::of::<Self>(),
            ))
        }
        fn instance_id(&self) -> &ConnectorInstanceId {
            &self.descriptor.instance_id
        }

        fn namespace_exists(&self, _: ConnectorNamespaceRequest) -> Result<bool, ConnectorError> {
            self.call("namespace_exists");
            if self.failure == FailurePoint::NamespaceExists {
                Err(self.error())
            } else {
                Ok(true)
            }
        }

        fn list_namespaces(
            &self,
            request: ConnectorListNamespacesRequest,
        ) -> Result<Vec<ConnectorNamespaceIdentity>, ConnectorError> {
            self.call("namespaces");
            if self.failure == FailurePoint::Namespaces {
                return Err(self.error());
            }
            Ok(["a", "b"]
                .into_iter()
                .map(|namespace| ConnectorNamespaceIdentity {
                    instance_id: request.instance_id.clone(),
                    namespace: Arc::from(namespace),
                })
                .collect())
        }

        fn list_namespaces_async(
            &self,
            request: ConnectorListNamespacesRequest,
        ) -> futures::future::BoxFuture<'_, Result<Vec<ConnectorNamespaceIdentity>, ConnectorError>>
        {
            Box::pin(async move { self.list_namespaces(request) })
        }

        fn list_tables(
            &self,
            request: ConnectorListTablesRequest,
        ) -> Result<Vec<ConnectorTableIdentity>, ConnectorError> {
            self.call(format!("tables:{}", request.namespace.namespace));
            if self.failure == FailurePoint::Tables
                || (self.failure == FailurePoint::SecondNamespaceTables
                    && request.namespace.namespace.as_ref() == "b")
            {
                return Err(self.error());
            }
            Ok(vec![ConnectorTableIdentity {
                instance_id: request.namespace.instance_id,
                namespace: request.namespace.namespace,
                table: Arc::from("existing_table"),
            }])
        }

        fn list_tables_async(
            &self,
            request: ConnectorListTablesRequest,
        ) -> futures::future::BoxFuture<'_, Result<Vec<ConnectorTableIdentity>, ConnectorError>>
        {
            Box::pin(async move { self.list_tables(request) })
        }

        fn table_exists(&self, _: ConnectorTableRequest) -> Result<bool, ConnectorError> {
            unreachable!("listing must not resolve a table")
        }

        fn load_table(
            &self,
            _: ConnectorTableRequest,
            _creation: &mut novarocks_spi::connector::read_stack::ConnectorReadSchemaCreation<'_>,
        ) -> Result<
            ConnectorTableMetadata,
            novarocks_connector_contract::PureProviderCompileError<ConnectorError>,
        > {
            unreachable!("listing must not load a table")
        }
    }

    // The same closed fixture author can execute one finite metadata phase.
    // Ordinary fault tests refuse before that phase; explicit CPU tests use
    // the actual qualified factory/packet/worker and its physical footer.
    impl ProviderMetadataCpuSource for ListingProvider {
        fn descriptor(&self) -> &ConnectorInstanceDescriptor {
            &self.descriptor
        }
        fn incarnation(&self) -> ProviderBindingEpoch {
            self.incarnation
        }
        fn run_metadata(&self) {
            let gate = self.metadata_gate.lock().unwrap().clone();
            if let Some(gate) = gate {
                self.call("metadata-start");
                gate.run();
            }
            self.call("metadata-cpu");
        }
    }

    impl ConnectorViewMetadata for ListingProvider {
        fn classify_view(
            &self,
            _: ConnectorViewRequest,
        ) -> Result<Option<ConnectorObjectKind>, ConnectorError> {
            unreachable!("the FORCE preflight must fail before a successful view classification")
        }

        fn list_classified_views(
            &self,
            request: ConnectorListViewsRequest,
        ) -> Result<Vec<(ConnectorViewIdentity, ConnectorObjectKind)>, ConnectorError> {
            if self.failure == FailurePoint::MaterializedViews {
                self.call("classified-views");
                return Ok(["mv_z", "mv_a"]
                    .into_iter()
                    .map(|name| {
                        (
                            ConnectorViewIdentity {
                                instance_id: self.descriptor.instance_id.clone(),
                                namespace: request.namespace.namespace.clone(),
                                view: name.into(),
                            },
                            ConnectorObjectKind::MaterializedView {
                                storage_table: ConnectorStorageTableIdentifier {
                                    namespace: vec!["db".into()].into(),
                                    name: format!("{name}_storage").into(),
                                    catalog: None,
                                },
                            },
                        )
                    })
                    .collect());
            }
            self.list_views(request).map(|views| {
                views
                    .into_iter()
                    .map(|view| (view, ConnectorObjectKind::View))
                    .collect()
            })
        }

        fn descriptor(&self) -> &ConnectorInstanceDescriptor {
            &self.descriptor
        }

        fn incarnation(&self) -> ProviderBindingEpoch {
            self.incarnation
        }

        fn list_views(
            &self,
            _: ConnectorListViewsRequest,
        ) -> Result<Vec<ConnectorViewIdentity>, ConnectorError> {
            self.call("views");
            if self.failure == FailurePoint::Views {
                Err(self.error())
            } else {
                unreachable!("the FORCE preflight must fail before a successful view listing")
            }
        }

        fn list_views_async(
            &self,
            request: ConnectorListViewsRequest,
            cpu: Arc<dyn ProviderMetadataCpuPort>,
        ) -> futures::future::BoxFuture<'_, Result<Vec<ConnectorViewIdentity>, ConnectorError>>
        {
            Box::pin(async move {
                let gate = self.held_view.lock().unwrap().clone();
                if let Some(gate) = gate {
                    self.call("views-start");
                    gate.notified().await;
                }
                let stop = self.stop_in_view_poll.lock().unwrap().take();
                if let Some(original) = stop {
                    original
                        .request(novarocks_workload_control::CancellationReason::DeadlineExceeded)
                        .unwrap();
                }
                if self
                    .metadata_in_views
                    .load(std::sync::atomic::Ordering::Acquire)
                {
                    let original = Arc::new(Self {
                        descriptor: self.descriptor.clone(),
                        incarnation: self.incarnation,
                        failure: self.failure,
                        kind: self.kind,
                        calls: self.calls.clone(),
                        held_view: Mutex::new(None),
                        stop_in_view_poll: Mutex::new(None),
                        metadata_in_views: std::sync::atomic::AtomicBool::new(false),
                        metadata_gate: Mutex::new(self.metadata_gate.lock().unwrap().clone()),
                    });
                    let mut input = Some(ProviderMetadataCpuInput(original));
                    cpu.submit_metadata_in(&mut input)?;
                    assert!(
                        input.is_none(),
                        "same closed metadata input transferred once"
                    );
                    self.call("metadata-submit");
                    let held = self.metadata_gate.lock().unwrap().clone();
                    if let Some(held) = held {
                        // Deliberately return the original error after the
                        // SAME actual worker started, before its footer. The
                        // outer Action must retain and close this exact port.
                        tokio::select! {
                            _ = held.wait_started() => {},
                            result = std::future::poll_fn(|cx| cpu.poll_finished(cx)) => result?,
                        }
                    } else {
                        std::future::poll_fn(|cx| cpu.poll_finished(cx)).await?;
                    }
                }
                self.list_views(request)
            })
        }

        fn view_exists(&self, _: ConnectorViewRequest) -> Result<bool, ConnectorError> {
            unreachable!("listing must not resolve a view")
        }

        fn load_view(
            &self,
            _: ConnectorViewRequest,
            _: &mut novarocks_spi::connector::read_stack::ConnectorReadSchemaCreation<'_>,
        ) -> Result<
            ConnectorViewMetadataValue,
            novarocks_connector_contract::PureProviderCompileError<ConnectorError>,
        > {
            unreachable!("listing must not load a view")
        }
    }

    impl ConnectorScanPlanning for ListingProvider {
        fn instance_id(&self) -> &ConnectorInstanceId {
            &self.descriptor.instance_id
        }

        fn begin_scan(
            &self,
            _: &ConnectorTableHandle,
            _: ConnectorBeginScanRequest,
        ) -> Result<ConnectorScan, ConnectorError> {
            unreachable!("listing must not begin a scan")
        }

        fn plan_splits(
            &self,
            _: &ConnectorScanHandle,
            _: ConnectorSplitPlanningRequest,
        ) -> Result<ConnectorSplitPlanningResult, ConnectorError> {
            unreachable!("listing must not plan splits")
        }
    }

    impl ConnectorExecutionDistribution for ListingProvider {
        fn declaration(
            &self,
            _: &ConnectorRequestContext,
        ) -> Result<ConnectorProviderBinding, ConnectorError> {
            unreachable!("listing must not distribute a connector")
        }
    }

    impl ConnectorCatalogMutation for ListingProvider {
        fn descriptor(&self) -> &ConnectorInstanceDescriptor {
            &self.descriptor
        }

        fn incarnation(&self) -> ProviderBindingEpoch {
            self.incarnation
        }

        fn admit(&self, request: &ConnectorCatalogMutationRequest) -> Result<(), ConnectorError> {
            request.context.check_active()?;
            assert!(
                matches!(&request.operation, ConnectorCatalogMutationOperation::DropNamespace { namespace, .. }
                    if namespace.instance_id == self.descriptor.instance_id && namespace.namespace.as_ref() == "db")
            );
            self.call("admit");
            Ok(())
        }

        fn execute(
            &self,
            _: ConnectorCatalogMutationRequest,
        ) -> Result<ExternalMutationOutcome<ConnectorCatalogMutationReceipt>, ConnectorError>
        {
            self.call("mutation");
            Err(ConnectorError::new(
                ConnectorErrorKind::Internal,
                "unexpected destructive mutation",
            ))
        }

        fn reconcile(
            &self,
            _: ConnectorCatalogMutationReconcileRequest,
        ) -> Result<ExternalMutationOutcome<ConnectorCatalogMutationReceipt>, ConnectorError>
        {
            unreachable!("listing must not reconcile a mutation")
        }
    }

    #[test]
    fn drop_database_force_lists_materialized_views_before_any_mutation() {
        let fixture = ListingFixture::new(
            FailurePoint::MaterializedViews,
            ConnectorErrorKind::Internal,
        );
        let error = execute_drop_database_statement(
            &fixture,
            &novarocks_sql::semantic::ObjectName {
                parts: vec!["catalog".into(), "db".into()],
            },
            None,
            false,
            true,
            &crate::connector::test_request_context(),
        )
        .unwrap_err();
        assert_eq!(
            error.to_string(),
            "Cannot drop schema catalog.db with CASCADE: materialized views exist: mv_a, mv_z"
        );
        assert_eq!(
            fixture.calls(),
            ["admit", "namespace_exists", "tables:db", "classified-views"]
        );
    }

    #[test]
    fn drop_database_force_listing_errors_precede_every_destructive_mutation() {
        for kind in error_kinds() {
            for (point, calls) in [
                (
                    FailurePoint::NamespaceExists,
                    vec!["admit", "namespace_exists"],
                ),
                (
                    FailurePoint::Tables,
                    vec!["admit", "namespace_exists", "tables:db"],
                ),
                (
                    FailurePoint::Views,
                    vec!["admit", "namespace_exists", "tables:db", "views"],
                ),
            ] {
                let fixture = ListingFixture::new(point, kind);
                let result = execute_drop_database_statement(
                    &fixture,
                    &novarocks_sql::semantic::ObjectName {
                        parts: vec!["catalog".into(), "db".into()],
                    },
                    None,
                    false,
                    true,
                    &crate::connector::test_request_context(),
                );
                assert_eq!(
                    result.err().map(|error| error.to_string()),
                    Some(fixture.expected_error()),
                    "{point:?}, {kind:?}"
                );
                assert_eq!(
                    fixture.calls(),
                    calls,
                    "no child or namespace deletion may precede completed listings"
                );
            }
        }
    }
}
