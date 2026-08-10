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

//! Core-owned Iceberg write preparation and execution primitives.
//!
//! Frontend DML services own production statement routing and transaction
//! orchestration over these primitives.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

use arrow::datatypes::{Field, Schema};
use bytes::Bytes;
use novarocks_connector_iceberg::iceberg::Catalog;
use novarocks_connector_iceberg::iceberg::spec::DataFile;
use novarocks_connector_iceberg::iceberg::{NamespaceIdent, TableIdent};

use crate::connector::backend::ResolvedTable;
use crate::connector::iceberg::catalog::registry::{
    IcebergCatalogEntry, block_on_iceberg, build_iceberg_catalog,
};
use crate::connector::iceberg::commit::{
    CleanupPathMapper, CommitServiceError, EqualityDeleteColumn, IcebergCommitCollector,
    ensure_iceberg_write_supported, ensure_no_equality_deletes,
    ensure_overwrite_single_partition_spec,
};
use crate::connector::iceberg::commit::{CommitOpKind, CommitOutcome, WrittenFile};
use crate::connector::iceberg::write_commit::IcebergWriteCommitExecutor;
use crate::connector::iceberg::write_control::IcebergWritePlanPayloadV1;
use crate::connector::iceberg::write_service::{
    IcebergWriteControlService, IcebergWriteControlServiceContext, IcebergWriteReportCommitter,
};
use crate::engine::StandaloneState;
use crate::engine::backend_resolver::TargetBackend;
use crate::engine::mv::refresh_io::query_result_to_chunks;
use crate::engine::query_planning::write_sink::{
    admit_prepared_connector_write_target, sql_write_plan_input_for_admitted_target,
};
use crate::engine::write_transaction::{
    IcebergWriteCommitPolicy, IcebergWriteSource, IcebergWriteTransactionSpec,
    IcebergWriteValidationPolicy,
};
use crate::meta::repository::iceberg_operation::{IcebergOperationKind, IcebergOperationTarget};
use crate::query_execution::outcome::QueryExecutionResult;
use crate::query_execution::request_context::QueryExecutionContext;
use crate::sql::parser::ast::Literal;
use novarocks_catalog::schema::ColumnDef;
use novarocks_catalog::schema::SqlType;
use novarocks_execution::exec::chunk::Chunk;
use novarocks_spi::connector::{
    ConnectorInstanceId, ConnectorTableHandle, ConnectorTableIdentity, ConnectorTableRequest,
    ConnectorTableResolution, ConnectorWriteAdmissionPurpose, ConnectorWriteFieldRequest,
    ConnectorWriteInputRequest, ConnectorWriteIntent, ConnectorWriteLease,
    ConnectorWriteOperationId, ConnectorWritePreparation, ConnectorWritePreparationOutcome,
    ConnectorWritePreparationRequest,
};

#[derive(Clone, Debug, PartialEq)]
pub(crate) enum IcebergWriteInput {
    Rows(Vec<Vec<Literal>>),
    Query(Box<sqlparser::ast::Query>),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum IcebergWriteMode {
    Append,
    FullTableOverwrite,
    DynamicPartitionOverwrite,
}

/// Provider-owned identity and snapshot facts for a prepared Iceberg write.
///
/// The application layer may preallocate the opaque operation identity, but it
/// never interprets or constructs Iceberg commit state. In particular, MV
/// refresh uses this to make the staged writer, snapshot marker, and durable
/// frontend ledger refer to one attempt before any external action starts.
#[derive(Clone, Debug)]
pub(crate) struct IcebergWritePreparationOptions {
    pub(crate) operation_id: ConnectorWriteOperationId,
    pub(crate) snapshot_properties: BTreeMap<String, String>,
}

impl IcebergWritePreparationOptions {
    pub(crate) fn new(operation_id: ConnectorWriteOperationId) -> Self {
        Self {
            operation_id,
            snapshot_properties: BTreeMap::new(),
        }
    }

    pub(crate) fn with_snapshot_properties(
        mut self,
        snapshot_properties: BTreeMap<String, String>,
    ) -> Self {
        self.snapshot_properties = snapshot_properties;
        self
    }
}

/// Core Iceberg write preparation shared by the frontend INSERT adapter,
/// CTAS, and mutation flows. Construction validates and plans the write but
/// never starts a distributed writer or external metadata commit.
#[allow(clippy::too_many_arguments)]
pub(crate) fn prepare_iceberg_write(
    state: &Arc<StandaloneState>,
    target: &TargetBackend,
    resolved: &ResolvedTable,
    insert_columns: &[String],
    source: &IcebergWriteInput,
    overwrite_mode: IcebergWriteMode,
    target_ref: &str,
    execution: Option<QueryExecutionContext>,
    connector_context: &novarocks_spi::connector::ConnectorRequestContext,
    planning_lease: novarocks_spi::connector::ConnectorControlPlanningLease,
) -> Result<PreparedIcebergWrite, String> {
    prepare_iceberg_write_with_options(
        state,
        target,
        resolved,
        insert_columns,
        source,
        overwrite_mode,
        target_ref,
        execution,
        connector_context,
        IcebergWritePreparationOptions::new(ConnectorWriteOperationId::new()),
        planning_lease,
    )
}

/// Prepare an Iceberg write with an application-preallocated operation
/// identity. This still performs no writer execution or catalog mutation.
#[allow(clippy::too_many_arguments)]
pub(crate) fn prepare_iceberg_write_with_options(
    state: &Arc<StandaloneState>,
    target: &TargetBackend,
    resolved: &ResolvedTable,
    insert_columns: &[String],
    source: &IcebergWriteInput,
    overwrite_mode: IcebergWriteMode,
    target_ref: &str,
    execution: Option<QueryExecutionContext>,
    connector_context: &novarocks_spi::connector::ConnectorRequestContext,
    options: IcebergWritePreparationOptions,
    planning_lease: novarocks_spi::connector::ConnectorControlPlanningLease,
) -> Result<PreparedIcebergWrite, String> {
    debug_assert_eq!(target.backend_name, "iceberg");

    let overwrite_full_table = matches!(overwrite_mode, IcebergWriteMode::FullTableOverwrite);
    let overwrite_partitions =
        matches!(overwrite_mode, IcebergWriteMode::DynamicPartitionOverwrite);

    // 1. Resolve catalog entry + build iceberg-rust Catalog handle.
    let entry = {
        let registry = state
            .iceberg_catalogs
            .read()
            .map_err(|e| format!("iceberg catalog registry read lock: {e}"))?;
        registry.get(&target.catalog)?
    };
    let catalog: Arc<dyn Catalog> = build_iceberg_catalog(&entry)?;
    let table_ident = TableIdent::new(
        NamespaceIdent::new(target.namespace.clone()),
        target.table.clone(),
    );
    let table =
        block_on_iceberg(async { catalog.load_table(&table_ident).await })?.map_err(|e| {
            format!(
                "load iceberg table {target_str}: {e}",
                target_str = target_string(target)
            )
        })?;

    // 2. Pre-lowering validators.
    let _write_mode = ensure_iceberg_write_supported(&table)?;
    if overwrite_full_table {
        ensure_overwrite_single_partition_spec(&table)?;
        ensure_no_equality_deletes(&table)?;
    }
    if overwrite_partitions {
        // v3 row-lineage + cross-historical-spec checks happen in
        // OverwritePartitionsCommit.
        if table.metadata().default_partition_spec().is_unpartitioned() {
            return Err(format!(
                "INSERT OVERWRITE PARTITIONS requires a partitioned table; \
                 table {} is unpartitioned (use OVERWRITE without PARTITIONS)",
                target_string(target),
            ));
        }
    }
    // Branch writes require Iceberg v3 (row-lineage semantics).
    if target_ref != "main" {
        let fmt = table.metadata().format_version();
        if fmt != novarocks_connector_iceberg::iceberg::spec::FormatVersion::V3 {
            return Err(format!(
                "iceberg ref: branch writes require Iceberg v3 tables (table {} is v{})",
                target_string(target),
                fmt as u8,
            ));
        }
    }

    prepare_iceberg_distributed_write(
        state,
        target,
        resolved,
        insert_columns,
        source,
        overwrite_mode,
        target_ref,
        catalog,
        table,
        &entry,
        table_ident,
        execution,
        connector_context,
        options,
        planning_lease,
    )
}

#[allow(clippy::too_many_arguments)]
fn prepare_iceberg_distributed_write(
    state: &Arc<StandaloneState>,
    target: &TargetBackend,
    resolved: &ResolvedTable,
    insert_columns: &[String],
    source: &IcebergWriteInput,
    overwrite_mode: IcebergWriteMode,
    target_ref: &str,
    catalog: Arc<dyn Catalog>,
    table: novarocks_connector_iceberg::iceberg::table::Table,
    entry: &IcebergCatalogEntry,
    table_ident: TableIdent,
    execution: Option<QueryExecutionContext>,
    connector_context: &novarocks_spi::connector::ConnectorRequestContext,
    options: IcebergWritePreparationOptions,
    planning_lease: novarocks_spi::connector::ConnectorControlPlanningLease,
) -> Result<PreparedIcebergWrite, String> {
    let write_lease = planning_lease
        .derive_write_lease()
        .map_err(|error| format!("derive Iceberg write admission lease: {error}"))?;
    let metadata = table.metadata();
    let (query, write_columns) =
        build_iceberg_write_plan(target, resolved, insert_columns, source, &table, entry)?;
    let intent = match overwrite_mode {
        IcebergWriteMode::Append => ConnectorWriteIntent::Append,
        IcebergWriteMode::FullTableOverwrite => ConnectorWriteIntent::Overwrite,
        IcebergWriteMode::DynamicPartitionOverwrite => ConnectorWriteIntent::PartitionOverwrite,
    };
    let preparation = prepare_iceberg_connector_write(
        &write_lease,
        target,
        target_ref,
        intent,
        ConnectorWriteInputRequest::Data {
            fields: write_columns
                .iter()
                .map(|column| {
                    ConnectorWriteFieldRequest::new(Field::new(
                        &column.name,
                        column.data_type.clone(),
                        column.nullable,
                    ))
                })
                .collect(),
        },
        ConnectorWriteAdmissionPurpose::OrdinaryDml,
        connector_context.clone(),
    )?;
    let table_bindings =
        Arc::new(crate::engine::query_planning::bindings::QueryTableBindingStore::try_new()?);
    let target_binding = admit_prepared_connector_write_target(
        table_bindings.as_ref(),
        crate::sql::planner::table::SqlTableIdentity {
            catalog: target.catalog.clone(),
            namespace: target.namespace.clone(),
            table: target.table.clone(),
        },
        preparation.clone(),
        planning_lease,
    )?;
    let sql_write_input = sql_write_plan_input_for_admitted_target(
        table_bindings.as_ref(),
        target_binding,
        crate::sql::planner::distributed::write::contract::SqlWriteSinkMode::Data,
        crate::sql::planner::distributed::write::contract::ConnectorWriteInputBinding::RootOutputByOrdinal,
        None,
    )?;

    let commit_op_kind = commit_op_kind_for_overwrite_mode(overwrite_mode);
    let base_snapshot_id = write_base_snapshot_id(metadata, target_ref)?;
    let base_sequence_number = metadata.last_sequence_number();
    let current_schema = metadata.current_schema().clone();
    let default_partition_spec = metadata.default_partition_spec().clone();
    let staging_dir = format!(
        "{}/data/_staging/{}",
        metadata.location(),
        uuid::Uuid::new_v4()
    );
    let collector = Arc::new(
        IcebergCommitCollector::new(
            commit_op_kind,
            table_ident,
            base_snapshot_id,
            base_sequence_number,
            current_schema,
            default_partition_spec,
            staging_dir,
            novarocks_types::UniqueId::new(0, 0),
        )
        .with_table_metadata(metadata.clone()),
    );

    let abort_cleanup = build_abort_cleanup_for_catalog_entry(entry)?;
    let commit_executor = Arc::new(IcebergWriteCommitExecutor {
        catalog,
        table,
        collector,
        fs: abort_cleanup.fs,
        cleanup_path_mapper: abort_cleanup.path_mapper,
        cow_update_rewrite: None,
        target_ref: target_ref.to_string(),
        snapshot_properties: options.snapshot_properties.clone(),
    });
    let connector_operation_id = options.operation_id;
    let connector_write = register_insert_connector_write(
        state,
        target_ref,
        preparation,
        entry,
        Arc::clone(&commit_executor),
        connector_operation_id,
        connector_context.clone(),
        &write_lease,
    )?;
    let executor = PreparedIcebergWriteExecutor {
        state: Arc::clone(state),
        target: target.clone(),
        query,
        sql_write_input,
        table_bindings,
        commit_executor,
        execution,
        connector_context: connector_context.clone(),
        connector_write,
    };
    let spec = IcebergWriteTransactionSpec {
        target: IcebergOperationTarget {
            catalog: target.catalog.clone(),
            namespace: target.namespace.clone(),
            table: target.table.clone(),
            ref_name: (target_ref != "main").then(|| target_ref.to_string()),
        },
        operation_kind: operation_kind_for_commit_op_kind(commit_op_kind),
        attempt_id: connector_operation_id.to_string(),
        commit: IcebergWriteCommitPolicy {
            commit_op_kind,
            base_snapshot_id,
            base_snapshot_map: BTreeMap::new(),
            target_ref: target_ref.to_string(),
            snapshot_properties: options.snapshot_properties,
        },
        validation: IcebergWriteValidationPolicy {
            require_v3_for_branch: target_ref != "main",
        },
        source: IcebergWriteSource::CoordinatedPlan,
    };
    Ok(PreparedIcebergWrite { executor, spec })
}

#[allow(clippy::too_many_arguments)]
fn register_insert_connector_write(
    state: &Arc<StandaloneState>,
    target_ref: &str,
    preparation: ConnectorWritePreparation,
    entry: &IcebergCatalogEntry,
    commit_executor: Arc<IcebergWriteCommitExecutor>,
    operation_id: ConnectorWriteOperationId,
    context: novarocks_spi::connector::ConnectorRequestContext,
    exact_lease: &ConnectorWriteLease,
) -> Result<crate::query_execution::contract::ConnectorWritePlanningTemplate, String> {
    let services = state
        .iceberg_catalogs
        .read()
        .map_err(|error| format!("Iceberg catalog registry read lock: {error}"))?
        .write_services();
    let committer: Arc<dyn IcebergWriteReportCommitter> = commit_executor;
    crate::connector::iceberg::provider::register_iceberg_data_write_service_from_preparation(
        services,
        operation_id,
        &preparation,
        target_ref,
        entry,
        committer,
    )
    .map_err(|error| format!("activate Iceberg data writer from preparation: {error}"))?;
    Ok(
        crate::query_execution::contract::ConnectorWritePlanningTemplate::new(
            operation_id,
            preparation,
            context,
            exact_lease.clone(),
        ),
    )
}

pub(crate) fn register_iceberg_change_stream_provider_binding(
    state: &Arc<StandaloneState>,
    _target: &TargetBackend,
    binding: &crate::connector::iceberg::change_stream_write::IcebergChangeStreamProviderBinding,
    preparation: ConnectorWritePreparation,
    operation_id: ConnectorWriteOperationId,
    context: novarocks_spi::connector::ConnectorRequestContext,
    exact_lease: &ConnectorWriteLease,
) -> Result<crate::query_execution::contract::ConnectorWritePlanningTemplate, String> {
    preparation
        .validate()
        .map_err(|error| format!("validate Iceberg change-stream preparation: {error}"))?;
    if preparation.owner() != exact_lease.binding_key()
        || preparation.target_ref().as_str() != binding.target_ref()
    {
        return Err("Iceberg change-stream preparation drifted from its exact binding".to_string());
    }
    let services = state
        .iceberg_catalogs
        .read()
        .map_err(|error| format!("Iceberg catalog registry read lock: {error}"))?
        .write_services();
    services
        .register(
            operation_id,
            binding
                .control_service()
                .map_err(|error| format!("build Iceberg change-stream write service: {error}"))?,
        )
        .map_err(|error| format!("register Iceberg change-stream write service: {error}"))?;
    Ok(
        crate::query_execution::contract::ConnectorWritePlanningTemplate::new(
            operation_id,
            preparation,
            context,
            exact_lease.clone(),
        ),
    )
}

/// Build the inert registration sealed before exact-session admission. The
/// provider service is intentionally not registered here; DML activates the
/// binding only after it retains the exact session that will stage it.
pub(crate) fn iceberg_change_stream_provider_binding_template(
    _state: &Arc<StandaloneState>,
    _target: &TargetBackend,
    binding: &crate::connector::iceberg::change_stream_write::IcebergChangeStreamProviderBinding,
    operation_id: ConnectorWriteOperationId,
    context: novarocks_spi::connector::ConnectorRequestContext,
    exact_lease: &ConnectorWriteLease,
    preparation: &novarocks_spi::connector::ConnectorWritePreparation,
) -> Result<crate::query_execution::contract::ConnectorWritePlanningTemplate, String> {
    preparation
        .validate()
        .map_err(|error| format!("validate change-stream provider preparation: {error}"))?;
    if preparation.owner() != exact_lease.binding_key() {
        return Err(
            "change-stream provider preparation does not match its exact write lease".to_string(),
        );
    }
    if preparation.target_ref().as_str() != binding.target_ref() {
        return Err(format!(
            "change-stream provider preparation targets ref `{}`, but binding targets `{}`",
            preparation.target_ref().as_str(),
            binding.target_ref()
        ));
    }
    Ok(
        crate::query_execution::contract::ConnectorWritePlanningTemplate::new(
            operation_id,
            preparation.clone(),
            context,
            exact_lease.clone(),
        ),
    )
}

/// Register the provider service only after the exact operation session is
/// sealed. Any failure therefore remains abortable through that same session.
pub(crate) fn activate_iceberg_change_stream_provider_binding_after_session(
    state: &Arc<StandaloneState>,
    target: &TargetBackend,
    binding: &crate::connector::iceberg::change_stream_write::IcebergChangeStreamProviderBinding,
    operation_id: ConnectorWriteOperationId,
    session: &crate::query_execution::write_operation::ConnectorWriteOperationSession,
) -> Result<(), String> {
    if session.operation_id() != operation_id {
        return Err("Iceberg change-stream session has a foreign operation ID".to_string());
    }
    let instance_id = ConnectorInstanceId::parse(&target.catalog)
        .map_err(|error| format!("invalid Iceberg connector instance ID: {error}"))?;
    if session.owner().instance_id != instance_id {
        return Err(
            "Iceberg change-stream session does not match the target connector instance"
                .to_string(),
        );
    }
    let services = state
        .iceberg_catalogs
        .read()
        .map_err(|error| format!("Iceberg catalog registry read lock: {error}"))?
        .write_services();
    services
        .register_lazy(
            operation_id,
            binding.activation_digest(),
            binding.control_service_factory(),
        )
        .map_err(|error| format!("reserve Iceberg change-stream write service: {error}"))
}

/// Reserve the same provider binding only after exact-lease admission. The
/// registry owns lazy activation; the connector owns the frozen binding and
/// its digest.
#[allow(clippy::too_many_arguments)]
pub(crate) fn activate_iceberg_change_stream_connector_write(
    state: &Arc<StandaloneState>,
    target: &TargetBackend,
    topology: &crate::sql::planner::distributed::write::change_stream::SqlChangeStreamWriteTopology,
    table_bindings: &crate::engine::query_planning::bindings::QueryTableBindingStore,
    commit_executor: Arc<IcebergWriteCommitExecutor>,
    entry: &IcebergCatalogEntry,
    base_snapshot_id: Option<i64>,
    operation_id: ConnectorWriteOperationId,
    context: novarocks_spi::connector::ConnectorRequestContext,
    exact_lease: &ConnectorWriteLease,
    preparation: ConnectorWritePreparation,
) -> Result<crate::query_execution::contract::ConnectorWritePlanningTemplate, String> {
    let target_ref = commit_executor.target_ref.clone();
    let binding =
        crate::connector::iceberg::change_stream_write::bind_iceberg_change_stream_provider(
            crate::connector::iceberg::change_stream_write::IcebergChangeStreamProviderRequest {
                target: &format!("{}.{}.{}", target.catalog, target.namespace, target.table),
                target_ref: &target_ref,
                table: &commit_executor.table,
                entry,
                base_snapshot_id,
                operation_id,
                topology,
                table_bindings,
                commit_executor: Arc::clone(&commit_executor),
            },
        )?;
    let template = iceberg_change_stream_provider_binding_template(
        state,
        target,
        &binding,
        operation_id,
        context,
        exact_lease,
        &preparation,
    )?;
    let services = state
        .iceberg_catalogs
        .read()
        .map_err(|error| format!("Iceberg catalog registry read lock: {error}"))?
        .write_services();
    services
        .register_lazy(
            operation_id,
            binding.activation_digest(),
            binding.control_service_factory(),
        )
        .map_err(|error| format!("reserve Iceberg change-stream write service: {error}"))?;
    Ok(template)
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn register_iceberg_connector_write_service<S>(
    state: &Arc<StandaloneState>,
    target: &TargetBackend,
    target_ref: &str,
    intent: ConnectorWriteIntent,
    input_schema: Arc<Schema>,
    plan_payload: IcebergWritePlanPayloadV1,
    service: S,
    operation_id: ConnectorWriteOperationId,
    context: novarocks_spi::connector::ConnectorRequestContext,
    exact_lease: &ConnectorWriteLease,
) -> Result<crate::query_execution::contract::ConnectorWritePlanningTemplate, String>
where
    S: crate::connector::iceberg::write_control::IcebergWriteControlBackend + 'static,
{
    let cohort_id = novarocks_spi::connector::ConnectorWriteCohortId::primary(operation_id);
    let payload = plan_payload
        .encode()
        .map_err(|error| format!("encode Iceberg connector plan payload: {error}"))?;
    let mut templates = register_iceberg_connector_write_cohort_service(
        state,
        target,
        target_ref,
        service,
        operation_id,
        exact_lease,
        vec![(cohort_id, intent, input_schema, payload, context)],
    )?;
    Ok(templates
        .pop()
        .expect("single Iceberg cohort registration returns one template"))
}

#[allow(clippy::type_complexity)]
pub(crate) fn register_iceberg_connector_write_cohort_service<S>(
    state: &Arc<StandaloneState>,
    target: &TargetBackend,
    target_ref: &str,
    service: S,
    operation_id: ConnectorWriteOperationId,
    exact_lease: &ConnectorWriteLease,
    cohorts: Vec<(
        novarocks_spi::connector::ConnectorWriteCohortId,
        ConnectorWriteIntent,
        Arc<Schema>,
        Bytes,
        novarocks_spi::connector::ConnectorRequestContext,
    )>,
) -> Result<Vec<crate::query_execution::contract::ConnectorWritePlanningTemplate>, String>
where
    S: crate::connector::iceberg::write_control::IcebergWriteControlBackend + 'static,
{
    if cohorts.is_empty() {
        return Err("Iceberg connector write operation has no cohorts".to_string());
    }
    // Provider admission is deliberately before the local service registry
    // mutation. A typed denial (notably managed-MV ordinary DML) therefore
    // leaves no operation entry that could later be activated accidentally.
    let templates = build_iceberg_connector_write_templates(
        target,
        target_ref,
        operation_id,
        exact_lease,
        cohorts,
    )?;
    let instance_id = ConnectorInstanceId::parse(&target.catalog)
        .map_err(|error| format!("invalid Iceberg connector instance ID: {error}"))?;
    let services = state
        .iceberg_catalogs
        .read()
        .map_err(|error| format!("Iceberg catalog registry read lock: {error}"))?
        .write_services();
    services
        .register(operation_id, service)
        .map_err(|error| format!("register Iceberg connector write service: {error}"))?;

    Ok(templates)
}

/// Reserve an Iceberg write service only after the application has retained
/// the exact write lease. The factory is evaluated lazily by the first SPI
/// planning request, so SQL preparation cannot mutate the provider registry
/// or accidentally bind a newer connector generation.
#[allow(clippy::type_complexity)]
pub(crate) fn reserve_iceberg_connector_write_cohort_service_with_exact_lease<F>(
    state: &Arc<StandaloneState>,
    target: &TargetBackend,
    target_ref: &str,
    operation_id: ConnectorWriteOperationId,
    cohorts: Vec<(
        novarocks_spi::connector::ConnectorWriteCohortId,
        ConnectorWriteIntent,
        Arc<Schema>,
        Bytes,
        novarocks_spi::connector::ConnectorRequestContext,
    )>,
    activation_digest: [u8; 32],
    exact_lease: &ConnectorWriteLease,
    factory: F,
) -> Result<Vec<crate::query_execution::contract::ConnectorWritePlanningTemplate>, String>
where
    F: Fn() -> Result<
            Arc<dyn crate::connector::iceberg::write_control::IcebergWriteControlBackend>,
            novarocks_spi::connector::ConnectorError,
        > + Send
        + Sync
        + 'static,
{
    if cohorts.is_empty() {
        return Err("Iceberg connector write operation has no cohorts".to_string());
    }
    let instance_id = ConnectorInstanceId::parse(&target.catalog)
        .map_err(|error| format!("invalid Iceberg connector instance ID: {error}"))?;
    if exact_lease.binding_key().instance_id != instance_id {
        return Err("Iceberg write lease does not match the target connector instance".to_string());
    }
    // As above, prepare against the exact retained generation before local
    // lazy activation is visible to a later SPI planning request.
    let templates = build_iceberg_connector_write_templates(
        target,
        target_ref,
        operation_id,
        exact_lease,
        cohorts,
    )?;
    let services = state
        .iceberg_catalogs
        .read()
        .map_err(|error| format!("Iceberg catalog registry read lock: {error}"))?
        .write_services();
    services
        .register_lazy(operation_id, activation_digest, factory)
        .map_err(|error| format!("reserve Iceberg connector write service: {error}"))?;
    Ok(templates)
}

#[allow(clippy::type_complexity)]
fn build_iceberg_connector_write_templates(
    target: &TargetBackend,
    target_ref: &str,
    operation_id: ConnectorWriteOperationId,
    exact_lease: &ConnectorWriteLease,
    cohorts: Vec<(
        novarocks_spi::connector::ConnectorWriteCohortId,
        ConnectorWriteIntent,
        Arc<Schema>,
        Bytes,
        novarocks_spi::connector::ConnectorRequestContext,
    )>,
) -> Result<Vec<crate::query_execution::contract::ConnectorWritePlanningTemplate>, String> {
    build_iceberg_connector_write_templates_with_purpose(
        target,
        target_ref,
        operation_id,
        exact_lease,
        ConnectorWriteAdmissionPurpose::OrdinaryDml,
        cohorts,
    )
}

#[allow(clippy::type_complexity)]
fn build_iceberg_connector_write_templates_with_purpose(
    target: &TargetBackend,
    target_ref: &str,
    operation_id: ConnectorWriteOperationId,
    exact_lease: &ConnectorWriteLease,
    purpose: ConnectorWriteAdmissionPurpose,
    cohorts: Vec<(
        novarocks_spi::connector::ConnectorWriteCohortId,
        ConnectorWriteIntent,
        Arc<Schema>,
        Bytes,
        novarocks_spi::connector::ConnectorRequestContext,
    )>,
) -> Result<Vec<crate::query_execution::contract::ConnectorWritePlanningTemplate>, String> {
    let context = cohorts
        .first()
        .ok_or_else(|| "Iceberg connector write operation has no cohorts".to_string())?
        .4
        .clone();
    cohorts
        .into_iter()
        .map(
            |(cohort_id, intent, input_schema, provider_payload, context)| {
                // The registered service still owns its provider-private
                // payload during this C1 cutover, but the operation contract
                // is now sealed exclusively from Provider-signed admission.
                // In particular, no caller can put that payload into the
                // generic planning template.
                let preparation = prepare_iceberg_connector_write(
                    exact_lease,
                    target,
                    target_ref,
                    intent,
                    ConnectorWriteInputRequest::Data {
                        fields: input_schema
                            .fields()
                            .iter()
                            .map(|field| ConnectorWriteFieldRequest::new((**field).clone()))
                            .collect(),
                    },
                    purpose,
                    context.clone(),
                )?;
                let _provider_payload = provider_payload;
                Ok(
                    crate::query_execution::contract::ConnectorWritePlanningTemplate::new_in_cohort(
                        operation_id,
                        cohort_id,
                        preparation,
                        context,
                        exact_lease.clone(),
                    ),
                )
            },
        )
        .collect()
}

/// Request a sealed preparation from the write-control generation retained by
/// the original planning lease.  This helper is the only generic-template
/// construction seam: callers provide Arrow fields, never a table-format
/// field ID, writer payload, or a freshly acquired connector generation.
pub(crate) fn prepare_iceberg_connector_write(
    exact_lease: &ConnectorWriteLease,
    target: &TargetBackend,
    target_ref: &str,
    intent: ConnectorWriteIntent,
    input: ConnectorWriteInputRequest,
    purpose: ConnectorWriteAdmissionPurpose,
    context: novarocks_spi::connector::ConnectorRequestContext,
) -> Result<ConnectorWritePreparation, String> {
    let table = iceberg_connector_table_handle(exact_lease, target, context.clone())?;
    let outcome = exact_lease
        .control()
        .prepare_write(ConnectorWritePreparationRequest {
            table,
            target_ref: novarocks_spi::connector::ConnectorWriteTargetRef::parse(target_ref)
                .map_err(|error| format!("validate Iceberg write target ref: {error}"))?,
            intent,
            purpose,
            input,
            context,
        })
        .map_err(|error| format!("prepare Iceberg connector write: {error}"))?;
    match outcome {
        ConnectorWritePreparationOutcome::Prepared(preparation) => Ok(preparation),
        ConnectorWritePreparationOutcome::Denied(error) => {
            Err(format!("Iceberg write admission denied: {error}"))
        }
    }
}

/// Activate a row-level writer from a Provider-signed preparation.  The
/// application retains only the exact lease and opaque preparation; the
/// Iceberg provider derives its private writer handle and service payload.
#[allow(clippy::too_many_arguments)]
pub(crate) fn register_iceberg_row_connector_write(
    state: &Arc<StandaloneState>,
    target_ref: &str,
    preparation: ConnectorWritePreparation,
    entry: &IcebergCatalogEntry,
    table: &novarocks_connector_iceberg::iceberg::table::Table,
    commit_executor: Arc<IcebergWriteCommitExecutor>,
    operation_id: ConnectorWriteOperationId,
    context: novarocks_spi::connector::ConnectorRequestContext,
    exact_lease: &ConnectorWriteLease,
) -> Result<crate::query_execution::contract::ConnectorWritePlanningTemplate, String> {
    let services = state
        .iceberg_catalogs
        .read()
        .map_err(|error| format!("Iceberg catalog registry read lock: {error}"))?
        .write_services();
    let committer: Arc<dyn IcebergWriteReportCommitter> = commit_executor;
    crate::connector::iceberg::provider::register_iceberg_row_write_service_from_preparation(
        services,
        operation_id,
        &preparation,
        target_ref,
        entry,
        table,
        committer,
    )
    .map_err(|error| format!("activate Iceberg row writer from preparation: {error}"))?;
    Ok(
        crate::query_execution::contract::ConnectorWritePlanningTemplate::new(
            operation_id,
            preparation,
            context,
            exact_lease.clone(),
        ),
    )
}

/// Resolve an opaque Iceberg write target through the connector metadata
/// capability owned by the exact generation observed at write admission.
/// Core only forwards the target identity; it never builds a handle payload.
pub(crate) fn iceberg_connector_table_handle(
    exact_lease: &ConnectorWriteLease,
    target: &TargetBackend,
    context: novarocks_spi::connector::ConnectorRequestContext,
) -> Result<ConnectorTableHandle, String> {
    let instance_id = ConnectorInstanceId::parse(&target.catalog)
        .map_err(|error| format!("invalid Iceberg connector instance ID: {error}"))?;
    if exact_lease.binding_key().instance_id != instance_id {
        return Err("Iceberg write lease does not match the target connector instance".to_string());
    }
    let metadata = exact_lease
        .load_table(ConnectorTableRequest {
            table: ConnectorTableIdentity {
                instance_id,
                namespace: Arc::from(target.namespace.as_str()),
                table: Arc::from(target.table.as_str()),
            },
            resolution: ConnectorTableResolution::StrictBaseTable,
            context,
        })
        .map_err(|error| {
            format!("load Iceberg write target through connector metadata: {error}")
        })?;
    Ok(metadata.table)
}

pub(crate) struct PreparedIcebergWrite {
    executor: PreparedIcebergWriteExecutor,
    spec: IcebergWriteTransactionSpec,
}

impl PreparedIcebergWrite {
    pub(crate) fn target(&self) -> &TargetBackend {
        &self.executor.target
    }

    pub(crate) fn attempt_id(&self) -> &str {
        &self.spec.attempt_id
    }

    pub(crate) fn commit_op_kind(&self) -> CommitOpKind {
        self.spec.commit.commit_op_kind
    }

    pub(crate) fn base_snapshot_id(&self) -> Option<i64> {
        self.spec.commit.base_snapshot_id
    }

    pub(crate) fn run_coordinated_write(&self) -> Result<QueryExecutionResult, String> {
        crate::engine::execute_query_as_iceberg_write_with_connector_context(
            &self.executor.state,
            Some(&self.executor.target.catalog),
            &self.executor.target.namespace,
            &self.executor.query,
            self.executor.sql_write_input.clone(),
            Arc::clone(&self.executor.table_bindings),
            None,
            crate::sql::compiler::RootDistributionRequirement::Any,
            self.executor.execution.as_ref(),
            &self.executor.connector_context,
            Some(self.executor.connector_write.clone()),
        )
    }

    /// Convert a validated Iceberg write into SQL's inert distributed-write
    /// handoff. This registers no writer attempt and executes no query; the
    /// connector control service has already retained the provider-private
    /// committer under the operation identity carried by the resulting plan.
    ///
    /// Frontend application owners use this form when they must persist their
    /// intent and retain an exact connector lease before submitting native
    /// fragments.
    pub(crate) fn into_prepared_distributed_write(
        self,
    ) -> Result<crate::query_execution::prepared_write::PreparedDistributedWriteRequest, String>
    {
        let Self { executor, .. } = self;
        let execution = executor.execution.as_ref().ok_or_else(|| {
            "prepared distributed Iceberg write requires an admitted execution context".to_string()
        })?;
        crate::engine::prepare_query_as_iceberg_write_with_connector_binding(
            &executor.state,
            Some(&executor.target.catalog),
            &executor.target.namespace,
            &executor.query,
            executor.sql_write_input,
            executor.table_bindings,
            None,
            None,
            execution,
            &executor.connector_context,
            executor.connector_write,
        )
    }

    pub(crate) fn commit(
        &self,
        completion: &crate::query_execution::ConnectorWriteCompletion,
    ) -> Result<CommitOutcome, CommitServiceError> {
        crate::connector::iceberg::write_commit::commit_iceberg_connector_write(
            &self.executor.commit_executor,
            completion,
        )
    }

    pub(crate) fn commit_terminal(
        &self,
        completion: &crate::query_execution::ConnectorWriteCompletion,
    ) -> Result<
        novarocks_spi::connector::ExternalMutationOutcome<
            novarocks_spi::connector::ConnectorWriteReceipt,
        >,
        String,
    > {
        crate::connector::iceberg::write_control::terminal_outcome_from_iceberg_commit(
            self.executor.connector_write.preparation().owner(),
            self.executor.connector_write.operation_id(),
            self.commit(completion),
        )
    }

    pub(crate) fn finalize(&self) -> Result<(), String> {
        invalidate_iceberg_caches(&self.executor.state, &self.executor.target)
    }

    /// Convert an inert prepared append into the mutation reverse-port
    /// execution.  The returned object retains the exact connector session
    /// created during request binding so a post-bind failure has a typed abort
    /// capability instead of being silently abandoned.
    pub(crate) fn into_mutation_execution(
        self,
    ) -> Result<Arc<dyn crate::engine::mutation_flow::MutationExecution>, String> {
        let state = Arc::clone(&self.executor.state);
        let target = self.executor.target.clone();
        let commit_executor = Arc::clone(&self.executor.commit_executor);
        let connector_context = self.executor.connector_context.clone();
        let execution = self.executor.execution.clone().ok_or_else(|| {
            "prepared Iceberg mutation write requires an admitted execution context".to_string()
        })?;
        let prepared_request = self.into_prepared_distributed_write()?;
        Ok(Arc::new(PreparedIcebergWriteMutationExecution {
            state,
            target,
            execution,
            prepared_request: Mutex::new(Some(prepared_request)),
            commit_executor,
            connector_context,
            operation_session: Mutex::new(None),
        }))
    }
}

struct PreparedIcebergWriteMutationExecution {
    state: Arc<StandaloneState>,
    target: TargetBackend,
    execution: QueryExecutionContext,
    prepared_request:
        Mutex<Option<crate::query_execution::prepared_write::PreparedDistributedWriteRequest>>,
    commit_executor: Arc<IcebergWriteCommitExecutor>,
    connector_context: novarocks_spi::connector::ConnectorRequestContext,
    operation_session:
        Mutex<Option<crate::query_execution::write_operation::ConnectorWriteOperationSession>>,
}

impl crate::engine::mutation_flow::MutationExecution for PreparedIcebergWriteMutationExecution {
    fn stage(&self) -> Result<QueryExecutionResult, String> {
        let prepared_request = self
            .prepared_request
            .lock()
            .expect("prepared Iceberg mutation request lock poisoned")
            .take()
            .ok_or_else(|| "prepared Iceberg mutation request was already consumed".to_string())?;
        let bound = crate::engine::bind_prepared_distributed_write_request(
            &self.state.query_execution,
            &self.execution,
            prepared_request,
        )?;
        let bound = match bound {
            crate::engine::BoundDistributedWriteBinding::Bound(bound) => bound,
            crate::engine::BoundDistributedWriteBinding::AbortRequired { session, reason } => {
                *self
                    .operation_session
                    .lock()
                    .expect("prepared Iceberg mutation session lock poisoned") = Some(session);
                return Err(reason);
            }
        };
        *self
            .operation_session
            .lock()
            .expect("prepared Iceberg mutation session lock poisoned") = Some(bound.session);
        crate::engine::execute_bound_distributed_write_request(
            &self.state.query_execution,
            bound.request,
        )
    }

    fn needs_abort_on_stage_error(&self) -> bool {
        self.operation_session
            .lock()
            .expect("prepared Iceberg mutation session lock poisoned")
            .is_some()
    }

    fn abort(&self, reason: String) -> Result<CommitOutcome, CommitServiceError> {
        let session = self
            .operation_session
            .lock()
            .expect("prepared Iceberg mutation session lock poisoned")
            .clone()
            .expect("prepared Iceberg mutation abort requires a retained operation session");
        crate::connector::iceberg::write_commit::abort_iceberg_connector_write(
            &self.commit_executor,
            &session,
            self.connector_context.clone(),
            reason,
        )
    }

    fn abort_terminal(
        &self,
    ) -> Result<novarocks_spi::connector::ConnectorWriteAbortOutcome, String> {
        let session = self
            .operation_session
            .lock()
            .expect("prepared Iceberg mutation session lock poisoned")
            .clone()
            .ok_or_else(|| {
                "prepared Iceberg mutation terminal abort requires a retained operation session"
                    .to_string()
            })?;
        session
            .abort(self.connector_context.clone())
            .map_err(|error| error.to_string())
    }

    fn commit(
        &self,
        completion: &crate::query_execution::ConnectorWriteCompletion,
    ) -> Result<CommitOutcome, CommitServiceError> {
        crate::connector::iceberg::write_commit::commit_iceberg_connector_write(
            &self.commit_executor,
            completion,
        )
    }

    fn finalize(&self) -> Result<(), String> {
        invalidate_iceberg_caches(&self.state, &self.target)
    }
}

/// Prepared execution payload consumed by frontend DML adapters.
///
/// This type owns no SQL routing or application transaction policy. The
/// frontend DML services drive production statement lifecycles.
struct PreparedIcebergWriteExecutor {
    state: Arc<StandaloneState>,
    target: TargetBackend,
    query: sqlparser::ast::Query,
    sql_write_input: crate::sql::planner::distributed::write::contract::SqlWritePlanInput,
    table_bindings: Arc<crate::engine::query_planning::bindings::QueryTableBindingStore>,
    commit_executor: Arc<IcebergWriteCommitExecutor>,
    execution: Option<QueryExecutionContext>,
    connector_context: novarocks_spi::connector::ConnectorRequestContext,
    connector_write: crate::query_execution::contract::ConnectorWritePlanningTemplate,
}

/// Build the `(query, Arrow write layout)` pair for an iceberg INSERT/OVERWRITE write
/// without driving a transaction. The frontend INSERT adapter consumes this
/// plan through its DML-owned runner; the folded MERGE not-matched INSERT
/// branch runs the same pair into a shared collector so the INSERT commits in
/// the same snapshot as the matched branch. Both callers share one query/sink
/// construction to avoid semantic drift.
pub(crate) fn build_iceberg_write_plan(
    target: &TargetBackend,
    resolved: &ResolvedTable,
    insert_columns: &[String],
    source: &IcebergWriteInput,
    table: &novarocks_connector_iceberg::iceberg::table::Table,
    entry: &IcebergCatalogEntry,
) -> Result<(sqlparser::ast::Query, Vec<ColumnDef>), String> {
    let write_columns = iceberg_insert_columns_from_schema(table.metadata().current_schema())?;
    let source_columns = sql_write_source_columns(&resolved.columns, &write_columns);
    let query =
        append_source_to_query_for_write(source, insert_columns, &source_columns, &write_columns)?;
    let _ = (target, entry);
    Ok((query, write_columns))
}

/// A connector read schema can carry execution-only fields (for example
/// row-lineage fields) alongside SQL target columns.  INSERT shaping is owned
/// by the SQL write target contract, so retain only the columns that exist in
/// that contract before assigning derived-query aliases.
fn sql_write_source_columns(
    source_columns: &[ColumnDef],
    write_columns: &[ColumnDef],
) -> Vec<ColumnDef> {
    source_columns
        .iter()
        .filter(|source| {
            write_columns
                .iter()
                .any(|target| target.name.eq_ignore_ascii_case(&source.name))
        })
        .cloned()
        .collect()
}

fn append_source_to_query(
    source: &IcebergWriteInput,
    insert_columns: &[String],
    target_columns: &[ColumnDef],
) -> Result<sqlparser::ast::Query, String> {
    append_source_to_query_for_write(source, insert_columns, target_columns, target_columns)
}

fn append_source_to_query_for_write(
    source: &IcebergWriteInput,
    insert_columns: &[String],
    source_columns: &[ColumnDef],
    write_columns: &[ColumnDef],
) -> Result<sqlparser::ast::Query, String> {
    match source {
        IcebergWriteInput::Query(query)
            if insert_columns.is_empty() && same_column_sequence(source_columns, write_columns) =>
        {
            Ok((**query).clone())
        }
        IcebergWriteInput::Query(query) => wrap_insert_query_with_write_projection(
            query,
            insert_columns,
            source_columns,
            write_columns,
        ),
        IcebergWriteInput::Rows(rows) => values_append_source_to_query_for_write(
            rows,
            insert_columns,
            source_columns,
            write_columns,
        ),
    }
}

fn wrap_insert_query_with_write_projection(
    query: &sqlparser::ast::Query,
    insert_columns: &[String],
    source_columns: &[ColumnDef],
    write_columns: &[ColumnDef],
) -> Result<sqlparser::ast::Query, String> {
    let insert_idx_by_target = if insert_columns.is_empty() {
        std::collections::HashMap::new()
    } else {
        insert_column_index_by_target_name(insert_columns, write_columns)?
    };
    let source_alias = "__nr_insert_src";
    let mut projection = Vec::with_capacity(write_columns.len());
    for (write_idx, column) in write_columns.iter().enumerate() {
        let target_name = novarocks_catalog::identifier::normalize_identifier(&column.name)?;
        let expr = if let Some(source_idx) = insert_idx_by_target.get(&target_name) {
            let source_expr = format!(
                "{}.{}",
                sql_identifier(source_alias),
                sql_identifier(&insert_columns[*source_idx])
            );
            target_cast_expr_sql(&source_expr, column)?
        } else if insert_columns.is_empty() {
            if let Some(source_idx) =
                source_index_for_write_column(column, write_idx, source_columns, write_columns)
            {
                let source_expr = format!(
                    "{}.{}",
                    sql_identifier(source_alias),
                    sql_identifier(&source_columns[source_idx].name)
                );
                target_cast_expr_sql(&source_expr, column)?
            } else {
                target_cast_expr_sql(&omitted_column_expr_sql(column)?, column)?
            }
        } else {
            target_cast_expr_sql(&omitted_column_expr_sql(column)?, column)?
        };
        projection.push(format!("{expr} AS {}", sql_identifier(&column.name)));
    }
    let alias_source_columns = if insert_columns.is_empty() {
        source_columns
            .iter()
            .map(|column| column.name.as_str())
            .collect::<Vec<_>>()
    } else {
        insert_columns
            .iter()
            .map(String::as_str)
            .collect::<Vec<_>>()
    };
    let alias_columns = alias_source_columns
        .into_iter()
        .map(|column| sql_identifier(column))
        .collect::<Vec<_>>()
        .join(", ");
    let sql = format!(
        "SELECT {} FROM ({}) AS {} ({})",
        projection.join(", "),
        query,
        sql_identifier(source_alias),
        alias_columns
    );
    parse_generated_query(&sql, "append INSERT SELECT projection")
}

fn values_append_source_to_query_for_write(
    rows: &[Vec<Literal>],
    insert_columns: &[String],
    source_columns: &[ColumnDef],
    write_columns: &[ColumnDef],
) -> Result<sqlparser::ast::Query, String> {
    let insert_idx_by_target = if insert_columns.is_empty() {
        std::collections::HashMap::new()
    } else {
        insert_column_index_by_target_name(insert_columns, write_columns)?
    };
    let rendered_rows = rows
        .iter()
        .map(|row| {
            if insert_columns.is_empty() {
                if row.len() != source_columns.len() {
                    return Err(format!(
                        "insert column count mismatch: expected {} values, got {}",
                        source_columns.len(),
                        row.len()
                    ));
                }
            } else if row.len() != insert_columns.len() {
                return Err(format!(
                    "insert column count mismatch: expected {} values for column list, got {}",
                    insert_columns.len(),
                    row.len()
                ));
            }
            let values = write_columns
                .iter()
                .enumerate()
                .map(|(write_idx, column)| {
                    if insert_columns.is_empty() {
                        if let Some(literal) = source_index_for_write_column(
                            column,
                            write_idx,
                            source_columns,
                            write_columns,
                        )
                        .and_then(|source_idx| row.get(source_idx))
                        {
                            target_literal_expr_sql(literal, column)
                        } else {
                            target_cast_expr_sql(&omitted_column_expr_sql(column)?, column)
                        }
                    } else {
                        let target_name =
                            novarocks_catalog::identifier::normalize_identifier(&column.name)?;
                        if let Some(literal) = insert_idx_by_target
                            .get(&target_name)
                            .and_then(|source_idx| row.get(*source_idx))
                        {
                            target_literal_expr_sql(literal, column)
                        } else {
                            target_cast_expr_sql(&omitted_column_expr_sql(column)?, column)
                        }
                    }
                })
                .collect::<Result<Vec<_>, _>>()?
                .join(", ");
            Ok(format!("({values})"))
        })
        .collect::<Result<Vec<_>, String>>()?;
    let sql = format!("VALUES {}", rendered_rows.join(", "));
    parse_generated_query(&sql, "append INSERT VALUES")
}

fn same_column_sequence(left: &[ColumnDef], right: &[ColumnDef]) -> bool {
    left.len() == right.len()
        && left
            .iter()
            .zip(right.iter())
            .all(|(l, r)| l.name.eq_ignore_ascii_case(&r.name) && l.data_type == r.data_type)
}

fn source_index_for_write_column(
    write_column: &ColumnDef,
    write_idx: usize,
    source_columns: &[ColumnDef],
    write_columns: &[ColumnDef],
) -> Option<usize> {
    source_columns
        .iter()
        .position(|source| source.name.eq_ignore_ascii_case(&write_column.name))
        .or_else(|| {
            (source_columns.len() == write_columns.len() && write_idx < source_columns.len())
                .then_some(write_idx)
        })
}

pub(crate) fn iceberg_insert_columns_from_schema(
    schema: &novarocks_connector_iceberg::iceberg::spec::Schema,
) -> Result<Vec<ColumnDef>, String> {
    let arrow_schema = novarocks_connector_iceberg::iceberg::arrow::schema_to_arrow_schema(schema)
        .map_err(|e| format!("convert iceberg insert schema to arrow schema failed: {e}"))?;
    arrow_schema
        .fields()
        .iter()
        .map(|field| {
            let nested = schema
                .field_by_name(field.name())
                .ok_or_else(|| format!("iceberg column `{}` missing from schema", field.name()))?;
            let data_type = match nested.field_type.as_ref() {
                novarocks_connector_iceberg::iceberg::spec::Type::Primitive(
                    novarocks_connector_iceberg::iceberg::spec::PrimitiveType::Variant,
                ) => arrow::datatypes::DataType::LargeBinary,
                novarocks_connector_iceberg::iceberg::spec::Type::Primitive(
                    novarocks_connector_iceberg::iceberg::spec::PrimitiveType::Binary,
                ) => arrow::datatypes::DataType::Binary,
                _ => field.data_type().clone(),
            };
            Ok(ColumnDef {
                name: field.name().clone(),
                data_type,
                nullable: field.is_nullable(),
                write_default: nested
                    .write_default
                    .as_ref()
                    .map(|literal| {
                        novarocks_connector_iceberg::default_value::iceberg_literal_to_column_default(
                            literal,
                            nested.field_type.as_ref(),
                        )
                        .map_err(|e| {
                            format!(
                                "convert Iceberg insert write-default for column `{}` failed: {e}",
                                field.name()
                            )
                        })
                    })
                    .transpose()?,
                logical_type: None,
            })
        })
        .collect()
}

fn insert_column_index_by_target_name(
    insert_columns: &[String],
    target_columns: &[ColumnDef],
) -> Result<std::collections::HashMap<String, usize>, String> {
    let mut target_names = std::collections::HashSet::with_capacity(target_columns.len());
    for column in target_columns {
        target_names.insert(novarocks_catalog::identifier::normalize_identifier(
            &column.name,
        )?);
    }

    let mut mapping = std::collections::HashMap::with_capacity(insert_columns.len());
    for (idx, column) in insert_columns.iter().enumerate() {
        let normalized = novarocks_catalog::identifier::normalize_identifier(column)?;
        if !target_names.contains(&normalized) {
            return Err(format!("unknown INSERT column `{column}`"));
        }
        if mapping.insert(normalized.clone(), idx).is_some() {
            return Err(format!("duplicate INSERT column `{column}`"));
        }
    }
    Ok(mapping)
}

fn omitted_column_expr_sql(column: &ColumnDef) -> Result<String, String> {
    let Some(write_default) = &column.write_default else {
        return Ok("NULL".to_string());
    };
    let sql_type = arrow_data_type_to_sql_type(&column.data_type)?;
    let literal = crate::sql::literal::column_default_to_ast_literal(write_default, &sql_type)?;
    literal_to_sql_for_arrow_type(&literal, &column.data_type)
}

fn target_literal_expr_sql(literal: &Literal, column: &ColumnDef) -> Result<String, String> {
    target_cast_expr_sql(
        &literal_to_sql_for_arrow_type(literal, &column.data_type)?,
        column,
    )
}

pub(crate) fn target_cast_expr_sql(expr_sql: &str, column: &ColumnDef) -> Result<String, String> {
    Ok(format!(
        "CAST({expr_sql} AS {})",
        arrow_data_type_to_sql_type_name(&column.data_type)?
    ))
}

fn parse_generated_query(sql: &str, context: &str) -> Result<sqlparser::ast::Query, String> {
    match crate::sql::parser::parse_sql_raw(sql)? {
        sqlparser::ast::Statement::Query(query) => Ok(*query),
        other => Err(format!("{context}: generated non-query statement: {other}")),
    }
}

fn sql_identifier(name: &str) -> String {
    format!("`{}`", name.replace('`', "``"))
}

fn literal_to_sql(literal: &Literal) -> Result<String, String> {
    Ok(match literal {
        Literal::Null => "NULL".to_string(),
        Literal::Bool(value) => {
            if *value {
                "TRUE".to_string()
            } else {
                "FALSE".to_string()
            }
        }
        Literal::Int(value) => value.to_string(),
        Literal::Float(value) => {
            if !value.is_finite() {
                return Err(format!(
                    "non-finite floating literal is not supported: {value}"
                ));
            }
            value.to_string()
        }
        Literal::String(value) | Literal::Date(value) => single_quoted_sql(value),
        Literal::Array(items) => format!(
            "[{}]",
            items
                .iter()
                .map(literal_to_sql)
                .collect::<Result<Vec<_>, _>>()?
                .join(", ")
        ),
        Literal::Map(entries) => {
            let mut args = Vec::with_capacity(entries.len() * 2);
            for (key, value) in entries {
                args.push(literal_to_sql(key)?);
                args.push(literal_to_sql(value)?);
            }
            format!("map({})", args.join(", "))
        }
        Literal::Struct(values) => format!(
            "row({})",
            values
                .iter()
                .map(literal_to_sql)
                .collect::<Result<Vec<_>, _>>()?
                .join(", ")
        ),
    })
}

pub(crate) fn literal_to_sql_for_arrow_type(
    literal: &Literal,
    data_type: &arrow::datatypes::DataType,
) -> Result<String, String> {
    use arrow::datatypes::DataType;

    match (literal, data_type) {
        (
            Literal::String(value) | Literal::Date(value),
            DataType::Binary | DataType::LargeBinary,
        ) => {
            let bytes = crate::sql::literal::latin1_string_to_bytes(value)?;
            Ok(format!("X'{}'", hex::encode_upper(bytes)))
        }
        (Literal::Array(items), DataType::List(item_field)) => {
            let values = items
                .iter()
                .map(|item| literal_to_sql_for_arrow_type(item, item_field.data_type()))
                .collect::<Result<Vec<_>, _>>()?;
            Ok(format!("[{}]", values.join(", ")))
        }
        (Literal::Map(entries), DataType::Map(entries_field, _)) => {
            let DataType::Struct(fields) = entries_field.data_type() else {
                return literal_to_sql(literal);
            };
            if fields.len() != 2 {
                return literal_to_sql(literal);
            }
            let mut args = Vec::with_capacity(entries.len() * 2);
            for (key, value) in entries {
                args.push(literal_to_sql_for_arrow_type(key, fields[0].data_type())?);
                args.push(literal_to_sql_for_arrow_type(value, fields[1].data_type())?);
            }
            Ok(format!("map({})", args.join(", ")))
        }
        (Literal::Struct(values), DataType::Struct(fields)) if values.len() == fields.len() => {
            let values = values
                .iter()
                .zip(fields.iter())
                .map(|(value, field)| literal_to_sql_for_arrow_type(value, field.data_type()))
                .collect::<Result<Vec<_>, _>>()?;
            Ok(format!("row({})", values.join(", ")))
        }
        _ => literal_to_sql(literal),
    }
}

fn single_quoted_sql(value: &str) -> String {
    let mut escaped = String::with_capacity(value.len() + 2);
    for ch in value.chars() {
        match ch {
            '\'' => escaped.push_str("''"),
            '\\' => escaped.push_str(r"\\"),
            _ => escaped.push(ch),
        }
    }
    format!("'{escaped}'")
}

fn arrow_data_type_to_sql_type(dt: &arrow::datatypes::DataType) -> Result<SqlType, String> {
    use arrow::datatypes::{DataType, TimeUnit};
    Ok(match dt {
        DataType::Boolean => SqlType::Boolean,
        DataType::Int8 => SqlType::TinyInt,
        DataType::Int16 => SqlType::SmallInt,
        DataType::Int32 => SqlType::Int,
        DataType::Int64 => SqlType::BigInt,
        DataType::FixedSizeBinary(width)
            if *width == novarocks_types::largeint::LARGEINT_BYTE_WIDTH =>
        {
            SqlType::LargeInt
        }
        DataType::Float32 => SqlType::Float,
        DataType::Float64 => SqlType::Double,
        DataType::Decimal128(precision, scale) => SqlType::Decimal {
            precision: *precision,
            scale: *scale,
        },
        DataType::Utf8 | DataType::LargeUtf8 => SqlType::String,
        DataType::Date32 => SqlType::Date,
        DataType::Timestamp(TimeUnit::Nanosecond, _) => SqlType::DateTimeNs,
        DataType::Timestamp(TimeUnit::Microsecond, _) => SqlType::DateTime,
        DataType::Time64(TimeUnit::Microsecond | TimeUnit::Nanosecond) => SqlType::Time,
        DataType::Binary => SqlType::Binary,
        DataType::LargeBinary => SqlType::Variant,
        DataType::List(element_field) => SqlType::Array(Box::new(arrow_data_type_to_sql_type(
            element_field.data_type(),
        )?)),
        DataType::Map(entries_field, _) => {
            let DataType::Struct(fields) = entries_field.data_type() else {
                return Err(format!("unsupported Arrow map entries type: {dt:?}"));
            };
            if fields.len() != 2 {
                return Err(format!("unsupported Arrow map entries field count: {dt:?}"));
            }
            SqlType::Map(
                Box::new(arrow_data_type_to_sql_type(fields[0].data_type())?),
                Box::new(arrow_data_type_to_sql_type(fields[1].data_type())?),
            )
        }
        DataType::Struct(fields) => SqlType::Struct(
            fields
                .iter()
                .map(|field| {
                    Ok((
                        field.name().clone(),
                        arrow_data_type_to_sql_type(field.data_type())?,
                    ))
                })
                .collect::<Result<Vec<_>, String>>()?,
        ),
        other => {
            return Err(format!(
                "unsupported Arrow type for INSERT default conversion: {other:?}"
            ));
        }
    })
}

fn arrow_data_type_to_sql_type_name(dt: &arrow::datatypes::DataType) -> Result<String, String> {
    sql_type_name(&arrow_data_type_to_sql_type(dt)?)
}

fn sql_type_name(sql_type: &SqlType) -> Result<String, String> {
    Ok(match sql_type {
        SqlType::TinyInt => "TINYINT".to_string(),
        SqlType::SmallInt => "SMALLINT".to_string(),
        SqlType::Int => "INT".to_string(),
        SqlType::BigInt => "BIGINT".to_string(),
        SqlType::LargeInt => "LARGEINT".to_string(),
        SqlType::Float => "FLOAT".to_string(),
        SqlType::Double => "DOUBLE".to_string(),
        SqlType::Decimal { precision, scale } => format!("DECIMAL({precision}, {scale})"),
        SqlType::String => "STRING".to_string(),
        SqlType::Json => "JSON".to_string(),
        SqlType::Binary => "VARBINARY".to_string(),
        SqlType::Bitmap => "BITMAP".to_string(),
        SqlType::Hll => "HLL".to_string(),
        SqlType::Boolean => "BOOLEAN".to_string(),
        SqlType::Date => "DATE".to_string(),
        SqlType::DateTime => "DATETIME".to_string(),
        SqlType::DateTimeNs => "DATETIME_NS".to_string(),
        SqlType::Time => "TIME".to_string(),
        SqlType::Array(inner) => format!("ARRAY<{}>", sql_type_name(inner)?),
        SqlType::Map(key, value) => {
            format!("MAP<{}, {}>", sql_type_name(key)?, sql_type_name(value)?)
        }
        SqlType::Struct(fields) => format!(
            "STRUCT<{}>",
            fields
                .iter()
                .map(|(name, ty)| Ok(format!("{} {}", sql_identifier(name), sql_type_name(ty)?)))
                .collect::<Result<Vec<_>, String>>()?
                .join(", ")
        ),
        SqlType::Variant => "VARIANT".to_string(),
    })
}

fn commit_op_kind_for_overwrite_mode(overwrite_mode: IcebergWriteMode) -> CommitOpKind {
    match overwrite_mode {
        IcebergWriteMode::DynamicPartitionOverwrite => CommitOpKind::OverwritePartitions,
        IcebergWriteMode::FullTableOverwrite => CommitOpKind::Overwrite,
        IcebergWriteMode::Append => CommitOpKind::FastAppend,
    }
}

fn operation_kind_for_commit_op_kind(kind: CommitOpKind) -> IcebergOperationKind {
    match kind {
        CommitOpKind::FastAppend => IcebergOperationKind::InsertAppend,
        CommitOpKind::Overwrite | CommitOpKind::OverwritePartitions => {
            IcebergOperationKind::InsertOverwrite
        }
        _ => IcebergOperationKind::Maintenance,
    }
}

fn write_base_snapshot_id(
    metadata: &novarocks_connector_iceberg::iceberg::spec::TableMetadata,
    target_ref: &str,
) -> Result<Option<i64>, String> {
    if target_ref == "main" {
        return Ok(metadata.current_snapshot().map(|s| s.snapshot_id()));
    }
    metadata
        .refs()
        .get(target_ref)
        .map(|snapshot_ref| Some(snapshot_ref.snapshot_id))
        .ok_or_else(|| format!("iceberg ref: branch '{target_ref}' not found in table metadata"))
}

pub(crate) fn invalidate_iceberg_caches(
    state: &Arc<StandaloneState>,
    target: &TargetBackend,
) -> Result<(), String> {
    {
        let registry = state
            .iceberg_catalogs
            .read()
            .map_err(|e| format!("iceberg catalog registry read lock: {e}"))?;
        let entry = registry.get(&target.catalog)?;
        entry.invalidate_table_cache(&target.namespace, &target.table);
    }
    state
        .catalog_service
        .invalidate_table(&target.catalog, &target.namespace, &target.table)
}

fn target_string(t: &TargetBackend) -> String {
    format!("{}.{}.{}", t.catalog, t.namespace, t.table)
}

pub(crate) fn data_file_to_written_file(
    df: &DataFile,
    partition_spec_id: i32,
) -> Result<WrittenFile, String> {
    Ok(WrittenFile {
        path: df.file_path().to_string(),
        format: df.file_format(),
        content: df.content_type(),
        partition_values: df.partition().clone(),
        partition_spec_id,
        record_count: df.record_count(),
        file_size_in_bytes: df.file_size_in_bytes(),
        split_offsets: df.split_offsets().map(|s| s.to_vec()).unwrap_or_default(),
        column_sizes: df.column_sizes().clone(),
        value_counts: df.value_counts().clone(),
        null_value_counts: df.null_value_counts().clone(),
        nan_value_counts: df.nan_value_counts().clone(),
        lower_bounds: df.lower_bounds().clone(),
        upper_bounds: df.upper_bounds().clone(),
        key_metadata: df.key_metadata().map(|s| s.to_vec()),
        referenced_data_file: df.referenced_data_file().map(|s| s.to_string()),
        equality_ids: df.equality_ids(),
        first_row_id: df.first_row_id(),
        content_offset: None,
        content_size_in_bytes: None,
        cardinality: None,
    })
}

pub(crate) fn run_select_to_chunks(
    state: &Arc<StandaloneState>,
    target: &TargetBackend,
    query: &sqlparser::ast::Query,
) -> Result<Vec<Chunk>, String> {
    // Pass `current_catalog` when the target is an iceberg table so that
    // 1-part and 2-part table references in the SELECT (e.g. `db.table`)
    // resolve against the active catalog.
    let current_catalog = if target.backend_name == "iceberg" && !target.catalog.is_empty() {
        Some(target.catalog.as_str())
    } else {
        None
    };

    let result = crate::engine::execute_query_with_catalog_service(
        state,
        current_catalog,
        &target.namespace,
        query,
        None,
    )?;
    query_result_to_chunks(result)
}

pub(crate) struct AbortCleanupOperator {
    pub(crate) fs: novarocks_connector_iceberg::opendal::Operator,
    pub(crate) path_mapper: Option<CleanupPathMapper>,
}

pub(crate) fn build_abort_cleanup_for_catalog_entry(
    entry: &crate::connector::iceberg::catalog::IcebergCatalogEntry,
) -> Result<AbortCleanupOperator, String> {
    if let Some(s3_config) = entry.object_store_config() {
        let access = crate::connector::iceberg::fs_io::resolve_access_for_location(
            &entry.warehouse_uri,
            Some(s3_config),
        )
        .map_err(|e| format!("resolve warehouse URI for iceberg abort cleanup: {e}"))?;
        let bucket = access
            .handle()
            .authority()
            .ok_or_else(|| {
                format!(
                    "resolve warehouse URI for iceberg abort cleanup missing bucket: {}",
                    entry.warehouse_uri
                )
            })?
            .to_string();
        let fs = access.operator();
        let mapper: CleanupPathMapper = Arc::new(move |path| {
            novarocks_fs::parse_object_store_path_parse_only(path)
                .ok()
                .and_then(|(actual_bucket, key)| {
                    if actual_bucket == bucket {
                        Some(key)
                    } else {
                        None
                    }
                })
                .unwrap_or_else(|| path.to_string())
        });
        return Ok(AbortCleanupOperator {
            fs,
            path_mapper: Some(mapper),
        });
    }

    let fs = novarocks_fs::FsAccessResolver::new()
        .resolve_location("/__novarocks_local_root__", None)
        .map_err(|error| format!("build local-FS operator failed: {error}"))?
        .operator();
    let mapper: CleanupPathMapper =
        Arc::new(|path: &str| path.strip_prefix("file://").unwrap_or(path).to_string());
    Ok(AbortCleanupOperator {
        fs,
        path_mapper: Some(mapper),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::datatypes::{DataType, Field, Fields, TimeUnit};
    use novarocks_connector_iceberg::iceberg::spec::{SnapshotReference, SnapshotRetention};
    use sqlparser::ast as sqlast;

    use novarocks_catalog::schema::ColumnDefault;

    fn test_column(
        name: &str,
        data_type: DataType,
        write_default: Option<ColumnDefault>,
    ) -> novarocks_catalog::schema::ColumnDef {
        novarocks_catalog::schema::ColumnDef {
            name: name.to_string(),
            data_type,
            nullable: true,
            write_default,
            logical_type: None,
        }
    }

    fn parse_query(sql: &str) -> sqlast::Query {
        let stmt = crate::sql::parser::parse_sql_raw(sql).expect("parse query");
        let sqlast::Statement::Query(query) = stmt else {
            panic!("expected query statement");
        };
        *query
    }

    fn test_map_type(key: DataType, value: DataType) -> DataType {
        DataType::Map(
            Arc::new(Field::new(
                "entries",
                DataType::Struct(Fields::from(vec![
                    Arc::new(Field::new("key", key, false)),
                    Arc::new(Field::new("value", value, true)),
                ])),
                false,
            )),
            false,
        )
    }

    fn test_struct_type(fields: Vec<(&str, DataType)>) -> DataType {
        DataType::Struct(Fields::from(
            fields
                .into_iter()
                .map(|(name, data_type)| Arc::new(Field::new(name, data_type, true)))
                .collect::<Vec<_>>(),
        ))
    }

    #[test]
    fn branch_write_uses_the_branch_head_as_its_base_snapshot() {
        let metadata = crate::connector::iceberg::test_metadata::metadata_with_two_snapshots()
            .into_builder(None)
            .set_ref(
                "dev",
                SnapshotReference::new(1, SnapshotRetention::branch(None, None, None)),
            )
            .expect("add dev branch")
            .build()
            .expect("build metadata with dev branch")
            .metadata;

        assert_eq!(write_base_snapshot_id(&metadata, "main").unwrap(), Some(2));
        assert_eq!(write_base_snapshot_id(&metadata, "dev").unwrap(), Some(1));
    }

    fn test_iceberg_metadata_with_identity_partition(
        source_column_name: &str,
        source_field_id: i32,
        partition_spec_id: i32,
    ) -> novarocks_connector_iceberg::iceberg::spec::TableMetadata {
        test_iceberg_metadata_with_partition(
            source_column_name,
            source_column_name,
            novarocks_connector_iceberg::iceberg::spec::Transform::Identity,
            source_field_id,
            partition_spec_id,
        )
    }

    fn test_iceberg_metadata_with_partition(
        source_column_name: &str,
        partition_field_name: &str,
        transform: novarocks_connector_iceberg::iceberg::spec::Transform,
        source_field_id: i32,
        partition_spec_id: i32,
    ) -> novarocks_connector_iceberg::iceberg::spec::TableMetadata {
        let schema = novarocks_connector_iceberg::iceberg::spec::Schema::builder()
            .with_fields(vec![
                Arc::new(
                    novarocks_connector_iceberg::iceberg::spec::NestedField::required(
                        source_field_id,
                        source_column_name,
                        novarocks_connector_iceberg::iceberg::spec::Type::Primitive(
                            novarocks_connector_iceberg::iceberg::spec::PrimitiveType::Int,
                        ),
                    ),
                ),
                Arc::new(
                    novarocks_connector_iceberg::iceberg::spec::NestedField::optional(
                        source_field_id + 1,
                        "v",
                        novarocks_connector_iceberg::iceberg::spec::Type::Primitive(
                            novarocks_connector_iceberg::iceberg::spec::PrimitiveType::String,
                        ),
                    ),
                ),
            ])
            .build()
            .expect("schema");
        let partition_spec = novarocks_connector_iceberg::iceberg::spec::PartitionSpec::builder(
            Arc::new(schema.clone()),
        )
        .with_spec_id(partition_spec_id)
        .add_partition_field(source_column_name, partition_field_name, transform)
        .expect("partition field")
        .build()
        .expect("partition spec");
        let creation = novarocks_connector_iceberg::iceberg::TableCreation::builder()
            .name("t".to_string())
            .location("file:///warehouse/db/t".to_string())
            .schema(schema)
            .partition_spec(partition_spec.into_unbound())
            .format_version(novarocks_connector_iceberg::iceberg::spec::FormatVersion::V3)
            .build();
        let metadata =
            novarocks_connector_iceberg::iceberg::spec::TableMetadataBuilder::from_table_creation(
                creation,
            )
            .expect("metadata builder")
            .build()
            .expect("metadata")
            .metadata;
        retag_test_metadata_partition_source(metadata, source_field_id, partition_spec_id)
    }

    fn retag_test_metadata_partition_source(
        metadata: novarocks_connector_iceberg::iceberg::spec::TableMetadata,
        source_field_id: i32,
        partition_spec_id: i32,
    ) -> novarocks_connector_iceberg::iceberg::spec::TableMetadata {
        // TableMetadataBuilder::from_table_creation intentionally reassigns
        // field and spec ids; this fixture retags serialized metadata so tests
        // can assert planner descriptors carry target-table ids verbatim.
        let mut value = serde_json::to_value(metadata).expect("metadata json");
        let object = value.as_object_mut().expect("metadata object");
        object.insert(
            "default-spec-id".to_string(),
            serde_json::Value::from(partition_spec_id),
        );
        object.insert(
            "last-column-id".to_string(),
            serde_json::Value::from(source_field_id + 1),
        );

        let schemas = object
            .get_mut("schemas")
            .and_then(serde_json::Value::as_array_mut)
            .expect("schemas array");
        let schema_fields = schemas[0]
            .as_object_mut()
            .and_then(|schema| schema.get_mut("fields"))
            .and_then(serde_json::Value::as_array_mut)
            .expect("schema fields");
        schema_fields[0]
            .as_object_mut()
            .expect("first field")
            .insert("id".to_string(), serde_json::Value::from(source_field_id));
        schema_fields[1]
            .as_object_mut()
            .expect("second field")
            .insert(
                "id".to_string(),
                serde_json::Value::from(source_field_id + 1),
            );

        let specs = object
            .get_mut("partition-specs")
            .and_then(serde_json::Value::as_array_mut)
            .expect("partition specs");
        let spec = specs[0].as_object_mut().expect("partition spec object");
        spec.insert(
            "spec-id".to_string(),
            serde_json::Value::from(partition_spec_id),
        );
        let partition_fields = spec
            .get_mut("fields")
            .and_then(serde_json::Value::as_array_mut)
            .expect("partition fields");
        partition_fields[0]
            .as_object_mut()
            .expect("partition field")
            .insert(
                "source-id".to_string(),
                serde_json::Value::from(source_field_id),
            );

        serde_json::from_value(value).expect("retagged metadata")
    }

    fn test_iceberg_target() -> TargetBackend {
        TargetBackend {
            backend_name: "iceberg",
            catalog: "test_catalog".to_string(),
            namespace: "test_db".to_string(),
            table: "target_orders".to_string(),
        }
    }

    fn test_resolved_table(columns: Vec<ColumnDef>) -> ResolvedTable {
        ResolvedTable {
            catalog: "test_catalog".to_string(),
            namespace: "test_db".to_string(),
            table: "target_orders".to_string(),
            columns,
            statistics_pin: None,
        }
    }

    fn test_iceberg_table(
        metadata: novarocks_connector_iceberg::iceberg::spec::TableMetadata,
    ) -> novarocks_connector_iceberg::iceberg::table::Table {
        novarocks_connector_iceberg::iceberg::table::Table::builder()
            .identifier(TableIdent::new(
                NamespaceIdent::new("test_db".to_string()),
                "target_orders".to_string(),
            ))
            .file_io(novarocks_connector_iceberg::iceberg::io::FileIO::new_with_fs())
            .metadata(metadata)
            .build()
            .expect("iceberg table")
    }

    fn test_iceberg_catalog_entry() -> IcebergCatalogEntry {
        let warehouse = tempfile::TempDir::new().expect("warehouse tempdir");
        crate::connector::iceberg::catalog::registry::build_catalog_entry(
            "test_catalog",
            &[
                ("type".to_string(), "iceberg".to_string()),
                (
                    "iceberg.catalog.warehouse".to_string(),
                    warehouse.path().display().to_string(),
                ),
            ],
        )
        .expect("iceberg catalog entry")
    }

    fn assert_position_delete_output_field(
        field: &crate::connector::iceberg::position_delete_descriptor::PositionDeleteOutputField,
        output_expr_index: i32,
        name: &str,
        data_type: &DataType,
        field_id: i32,
    ) {
        assert_eq!(field.output_expr_index, output_expr_index as usize);
        assert_eq!(field.name, name);
        assert_eq!(&field.data_type, data_type);
        assert_eq!(field.field_id, field_id);
    }

    fn assert_position_delete_descriptor_contract(
        desc: &crate::connector::iceberg::position_delete_descriptor::PositionDeleteDescriptorInput,
    ) {
        use crate::connector::iceberg::position_delete_descriptor::{
            ICEBERG_POSITION_DELETE_FILE_PATH_COLUMN, ICEBERG_POSITION_DELETE_FILE_PATH_FIELD_ID,
            ICEBERG_POSITION_DELETE_POS_COLUMN, ICEBERG_POSITION_DELETE_POS_FIELD_ID,
        };

        assert_eq!(desc.target_partition_spec_id, 7);
        assert_position_delete_output_field(
            &desc.file_path,
            0,
            ICEBERG_POSITION_DELETE_FILE_PATH_COLUMN,
            &DataType::Utf8,
            ICEBERG_POSITION_DELETE_FILE_PATH_FIELD_ID,
        );
        assert_position_delete_output_field(
            &desc.pos,
            1,
            ICEBERG_POSITION_DELETE_POS_COLUMN,
            &DataType::Int64,
            ICEBERG_POSITION_DELETE_POS_FIELD_ID,
        );
        let partition_field = desc
            .partition_source_fields
            .first()
            .expect("partition source field");
        assert_eq!(partition_field.output_expr_index, 2);
        assert_eq!(partition_field.source_column_name, "id");
        assert_eq!(partition_field.partition_field_name, "id_bucket");
        assert_eq!(partition_field.transform_expr, "bucket[8]");
        assert_eq!(partition_field.source_field_id, 42);
        assert_eq!(partition_field.data_type, DataType::Int32);
    }

    #[test]
    fn arrow_data_type_to_sql_type_accepts_time64_for_insert_defaults() {
        assert_eq!(
            arrow_data_type_to_sql_type(&DataType::Time64(TimeUnit::Microsecond)).expect("type"),
            novarocks_catalog::schema::SqlType::Time
        );
    }

    #[test]
    fn append_source_to_query_values_reorders_columns_and_fills_defaults() {
        let target_columns = vec![
            test_column("a", DataType::Int32, None),
            test_column("b", DataType::Int32, Some(ColumnDefault::Int32(5))),
            test_column("c", DataType::Int32, None),
        ];
        let source = IcebergWriteInput::Rows(vec![vec![
            crate::sql::parser::ast::Literal::Int(30),
            crate::sql::parser::ast::Literal::Int(10),
        ]]);

        let query = append_source_to_query(
            &source,
            &["c".to_string(), "a".to_string()],
            &target_columns,
        )
        .expect("append source query");

        let sqlast::SetExpr::Values(values) = query.body.as_ref() else {
            panic!("expected VALUES query, got: {query}");
        };
        let row = values.rows.first().expect("one row");
        let rendered: Vec<String> = row.iter().map(ToString::to_string).collect();
        assert_eq!(
            rendered,
            vec!["CAST(10 AS INT)", "CAST(5 AS INT)", "CAST(30 AS INT)"]
        );
    }

    #[test]
    fn omitted_column_expr_characterizes_neutral_write_defaults() {
        let full_binary = (0_u16..=255).map(|byte| byte as u8).collect::<Vec<_>>();
        let full_binary_sql = format!(
            "X'{}'",
            (0_u16..=255)
                .map(|byte| format!("{byte:02X}"))
                .collect::<String>()
        );
        let cases = vec![
            (
                "integer",
                DataType::Int32,
                Some(ColumnDefault::Int32(5)),
                "5".to_string(),
            ),
            (
                "string",
                DataType::Utf8,
                Some(ColumnDefault::String("value".to_string())),
                "'value'".to_string(),
            ),
            (
                "decimal",
                DataType::Decimal128(10, 2),
                Some(ColumnDefault::Decimal {
                    unscaled: 12_345,
                    precision: 10,
                    scale: 2,
                }),
                "'123.45'".to_string(),
            ),
            (
                "date",
                DataType::Date32,
                Some(ColumnDefault::Date {
                    days_since_epoch: -1,
                }),
                "'1969-12-31'".to_string(),
            ),
            (
                "datetime",
                DataType::Timestamp(TimeUnit::Microsecond, None),
                Some(ColumnDefault::TimestampMicros {
                    micros_since_epoch: 1_704_110_400_123_456,
                }),
                "'2024-01-01 12:00:00'".to_string(),
            ),
            (
                "datetime-ns",
                DataType::Timestamp(TimeUnit::Nanosecond, None),
                Some(ColumnDefault::TimestampNanos {
                    nanos_since_epoch: 1_704_164_645_123_456_789,
                }),
                "'2024-01-02 03:04:05.123456789'".to_string(),
            ),
            (
                "binary",
                DataType::Binary,
                Some(ColumnDefault::Binary(full_binary)),
                full_binary_sql,
            ),
            (
                "empty-array",
                DataType::List(Arc::new(Field::new("item", DataType::Int32, true))),
                Some(ColumnDefault::Array(Vec::new())),
                "[]".to_string(),
            ),
            (
                "empty-map",
                test_map_type(DataType::Int32, DataType::Utf8),
                Some(ColumnDefault::Map(Vec::new())),
                "map()".to_string(),
            ),
            ("missing", DataType::Int32, None, "NULL".to_string()),
        ];

        for (name, data_type, write_default, expected) in cases {
            let column = test_column(name, data_type, write_default);
            assert_eq!(
                omitted_column_expr_sql(&column),
                Ok(expected),
                "case={name}"
            );
        }
    }

    #[test]
    fn fixed_size_binary_largeint_maps_to_largeint_sql_type() {
        assert_eq!(
            arrow_data_type_to_sql_type(&DataType::FixedSizeBinary(
                novarocks_types::largeint::LARGEINT_BYTE_WIDTH
            )),
            Ok(SqlType::LargeInt)
        );
    }

    #[test]
    fn omitted_column_expr_characterizes_non_empty_collection_default_errors() {
        let list_column = test_column(
            "items",
            DataType::List(Arc::new(Field::new("item", DataType::Int32, true))),
            Some(ColumnDefault::Array(vec![ColumnDefault::Int32(1)])),
        );
        assert_eq!(
            omitted_column_expr_sql(&list_column).unwrap_err(),
            "non-empty ARRAY write-default is not yet supported (1 elements)"
        );

        let map_column = test_column(
            "attributes",
            test_map_type(DataType::Int32, DataType::Utf8),
            Some(ColumnDefault::Map(vec![(
                ColumnDefault::Int32(1),
                ColumnDefault::String("value".to_string()),
            )])),
        );
        assert_eq!(
            omitted_column_expr_sql(&map_column).unwrap_err(),
            "non-empty MAP write-default is not yet supported (1 entries)"
        );
    }

    #[test]
    fn append_source_to_query_values_casts_literals_to_target_types() {
        let target_columns = vec![
            test_column("id", DataType::Int64, None),
            test_column("region", DataType::Utf8, None),
            test_column("amount", DataType::Float64, None),
        ];
        let source = IcebergWriteInput::Rows(vec![
            vec![
                crate::sql::parser::ast::Literal::Int(1),
                crate::sql::parser::ast::Literal::String("us".to_string()),
                crate::sql::parser::ast::Literal::Float(10.5),
            ],
            vec![
                crate::sql::parser::ast::Literal::Int(2),
                crate::sql::parser::ast::Literal::String("eu".to_string()),
                crate::sql::parser::ast::Literal::Float(20.0),
            ],
        ]);

        let query =
            append_source_to_query(&source, &[], &target_columns).expect("append source query");

        let sqlast::SetExpr::Values(values) = query.body.as_ref() else {
            panic!("expected VALUES query, got: {query}");
        };
        let first_row: Vec<String> = values.rows[0].iter().map(ToString::to_string).collect();
        let second_row: Vec<String> = values.rows[1].iter().map(ToString::to_string).collect();
        assert_eq!(
            first_row,
            vec![
                "CAST(1 AS BIGINT)",
                "CAST('us' AS STRING)",
                "CAST(10.5 AS DOUBLE)"
            ]
        );
        assert_eq!(
            second_row,
            vec![
                "CAST(2 AS BIGINT)",
                "CAST('eu' AS STRING)",
                "CAST(20 AS DOUBLE)"
            ]
        );
    }

    #[test]
    fn append_source_to_query_values_does_not_position_fill_added_middle_column() {
        let source_columns = vec![
            test_column("id", DataType::Int32, None),
            test_column("amount", DataType::Int32, None),
        ];
        let write_columns = vec![
            test_column("id", DataType::Int32, None),
            test_column("category", DataType::Utf8, None),
            test_column("amount", DataType::Int32, None),
        ];
        let source = IcebergWriteInput::Rows(vec![vec![
            crate::sql::parser::ast::Literal::Int(1),
            crate::sql::parser::ast::Literal::Int(10),
        ]]);

        let query = append_source_to_query_for_write(&source, &[], &source_columns, &write_columns)
            .expect("append source query");

        let sqlast::SetExpr::Values(values) = query.body.as_ref() else {
            panic!("expected VALUES query, got: {query}");
        };
        let row: Vec<String> = values.rows[0].iter().map(ToString::to_string).collect();
        assert_eq!(
            row,
            vec!["CAST(1 AS INT)", "CAST(NULL AS STRING)", "CAST(10 AS INT)"]
        );
    }

    #[test]
    fn spi5b_write_projection_excludes_execution_only_read_fields() {
        let source_columns = vec![
            test_column("id", DataType::Int32, None),
            test_column("value", DataType::Utf8, None),
            test_column("_file", DataType::Utf8, None),
            test_column("_pos", DataType::Int64, None),
        ];
        let write_columns = vec![
            test_column("id", DataType::Int32, None),
            test_column("value", DataType::Utf8, None),
        ];

        let source = sql_write_source_columns(&source_columns, &write_columns);
        assert_eq!(
            source
                .iter()
                .map(|column| column.name.as_str())
                .collect::<Vec<_>>(),
            vec!["id", "value"]
        );
    }

    #[test]
    fn append_source_to_query_values_preserves_backslash_string_literals() {
        let target_columns = vec![test_column("region", DataType::Utf8, None)];
        let source = IcebergWriteInput::Rows(vec![vec![crate::sql::parser::ast::Literal::String(
            r"e\f".to_string(),
        )]]);

        let query =
            append_source_to_query(&source, &[], &target_columns).expect("append source query");

        let sqlast::SetExpr::Values(values) = query.body.as_ref() else {
            panic!("expected VALUES query, got: {query}");
        };
        let sqlast::Expr::Cast { expr, .. } = &values.rows[0][0] else {
            panic!("expected CAST expression");
        };
        let sqlast::Expr::Value(value) = expr.as_ref() else {
            panic!("expected string literal inside CAST");
        };
        let sqlast::Value::SingleQuotedString(s) = &value.value else {
            panic!("expected single-quoted string");
        };
        assert_eq!(s, r"e\f");
    }

    #[test]
    fn append_source_to_query_values_renders_binary_literals_as_hex() {
        let target_columns = vec![test_column("payload", DataType::Binary, None)];
        let packed = crate::sql::literal::bytes_to_latin1_string(&[0xab, 0x01]);
        let source =
            IcebergWriteInput::Rows(vec![vec![crate::sql::parser::ast::Literal::String(packed)]]);

        let query =
            append_source_to_query(&source, &[], &target_columns).expect("append source query");

        let sqlast::SetExpr::Values(values) = query.body.as_ref() else {
            panic!("expected VALUES query, got: {query}");
        };
        let sqlast::Expr::Cast { expr, .. } = &values.rows[0][0] else {
            panic!("expected CAST expression");
        };
        let sqlast::Expr::Value(value) = expr.as_ref() else {
            panic!("expected hex literal inside CAST");
        };
        let sqlast::Value::HexStringLiteral(s) = &value.value else {
            panic!("expected hex literal");
        };
        assert_eq!(s, "AB01");
    }

    #[test]
    fn target_cast_expr_sql_renders_large_binary_as_variant() {
        let column = test_column("v", DataType::LargeBinary, None);

        let sql = target_cast_expr_sql("X'AB01'", &column).expect("cast sql");

        assert_eq!(sql, "CAST(X'AB01' AS VARIANT)");
    }

    #[test]
    fn append_source_to_query_values_rejects_column_list_width_mismatch() {
        let target_columns = vec![
            test_column("a", DataType::Int32, None),
            test_column("b", DataType::Int32, None),
        ];
        let source = IcebergWriteInput::Rows(vec![vec![
            crate::sql::parser::ast::Literal::Int(1),
            crate::sql::parser::ast::Literal::Int(2),
        ]]);

        let err = append_source_to_query(&source, &["a".to_string()], &target_columns)
            .expect_err("extra value must be rejected");
        assert!(
            err.contains("expected 1 values for column list, got 2"),
            "got: {err}"
        );
    }

    #[test]
    fn append_source_to_query_from_query_column_list_wraps_projection() {
        let target_columns = vec![
            test_column("a", DataType::Int32, None),
            test_column("b", DataType::Int32, Some(ColumnDefault::Int32(7))),
            test_column("c", DataType::Int32, None),
        ];
        let source = IcebergWriteInput::Query(Box::new(parse_query("SELECT x, y FROM src")));

        let query = append_source_to_query(
            &source,
            &["c".to_string(), "a".to_string()],
            &target_columns,
        )
        .expect("append source query");

        let rendered = query.to_string();
        assert!(
            rendered.contains("FROM (SELECT x, y FROM src) AS `__nr_insert_src` (`c`, `a`)"),
            "derived query should carry source column aliases, got: {rendered}"
        );
        assert!(
            rendered.starts_with(
                "SELECT CAST(`__nr_insert_src`.`a` AS INT) AS `a`, CAST(7 AS INT) AS `b`, CAST(`__nr_insert_src`.`c` AS INT) AS `c`"
            ),
            "projection should target table column order, got: {rendered}"
        );
    }

    #[test]
    fn append_source_to_query_from_query_omitted_complex_columns_parse() {
        let target_columns = vec![
            test_column("k1", DataType::Int64, None),
            test_column(
                "c_map",
                test_map_type(DataType::Int32, DataType::Int32),
                None,
            ),
            test_column(
                "c_struct",
                test_struct_type(vec![("k1", DataType::Int32), ("k2", DataType::Int32)]),
                None,
            ),
        ];
        let source = IcebergWriteInput::Query(Box::new(parse_query(
            "SELECT idx FROM row_util ORDER BY idx LIMIT 1000",
        )));

        let query = append_source_to_query(&source, &["k1".to_string()], &target_columns)
            .expect("append source query");
        let rendered = query.to_string();

        assert!(
            rendered.contains("CAST(NULL AS MAP"),
            "omitted map column should be cast from NULL once, got: {rendered}"
        );
        assert!(
            rendered.contains("CAST(NULL AS STRUCT"),
            "omitted struct column should be cast from NULL once, got: {rendered}"
        );
        assert!(
            !rendered.contains("CAST(CAST(NULL"),
            "omitted complex columns must not produce nested casts, got: {rendered}"
        );
    }

    #[test]
    fn insert_writer_columns_follow_fresh_iceberg_schema_after_external_evolution() {
        let schema = novarocks_connector_iceberg::iceberg::spec::Schema::builder()
            .with_fields(vec![
                std::sync::Arc::new(
                    novarocks_connector_iceberg::iceberg::spec::NestedField::required(
                        1,
                        "amount",
                        novarocks_connector_iceberg::iceberg::spec::Type::Primitive(
                            novarocks_connector_iceberg::iceberg::spec::PrimitiveType::Long,
                        ),
                    ),
                ),
                std::sync::Arc::new(
                    novarocks_connector_iceberg::iceberg::spec::NestedField::required(
                        2,
                        "id",
                        novarocks_connector_iceberg::iceberg::spec::Type::Primitive(
                            novarocks_connector_iceberg::iceberg::spec::PrimitiveType::Int,
                        ),
                    ),
                ),
                std::sync::Arc::new(
                    novarocks_connector_iceberg::iceberg::spec::NestedField::optional(
                        3,
                        "category",
                        novarocks_connector_iceberg::iceberg::spec::Type::Primitive(
                            novarocks_connector_iceberg::iceberg::spec::PrimitiveType::String,
                        ),
                    ),
                ),
            ])
            .build()
            .expect("schema");

        let columns = iceberg_insert_columns_from_schema(&schema).expect("columns");
        let names = columns
            .iter()
            .map(|column| column.name.as_str())
            .collect::<Vec<_>>();
        let types = columns
            .iter()
            .map(|column| column.data_type.clone())
            .collect::<Vec<_>>();
        let nullable = columns
            .iter()
            .map(|column| column.nullable)
            .collect::<Vec<_>>();

        assert_eq!(names, vec!["amount", "id", "category"]);
        assert_eq!(
            types,
            vec![DataType::Int64, DataType::Int32, DataType::Utf8]
        );
        assert_eq!(nullable, vec![false, false, true]);
    }
}
