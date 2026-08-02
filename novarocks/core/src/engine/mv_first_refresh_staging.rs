// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements. See the NOTICE file distributed with this
// work for additional information regarding copyright ownership.

//! Native, result-free consumer for a prepared MV first-refresh append.
//!
//! This module is deliberately not wired into the production REFRESH route.
//! MVX-2W exercises it through the native fixture; MVX-2 will make the route
//! switch only after that fixture proves the data plane.

use std::sync::{Arc, Weak};

use iceberg::{NamespaceIdent, TableIdent};
use novarocks_spi::connector::{
    ConnectorCatalogMutationOperation, ConnectorControlResolver, ConnectorExecutionBindingKey,
    ConnectorInstanceId, ConnectorMutationOperationId, ConnectorRefAction, ConnectorRefKind,
    ConnectorTableIdentity, ConnectorWriteLease, ConnectorWriteOperationId, CreateOrReplacePolicy,
};

use crate::connector::iceberg::commit::CommitOpKind;
use crate::connector::iceberg::write_control::IcebergFirstRefreshWritePlanPayloadV2;
use crate::engine::{
    StandaloneState, execute_logical_plan_as_iceberg_staging_in_operation_with_connector_context,
    execute_query_as_iceberg_staging_in_operation_with_connector_context,
    iceberg_write_shuffle_by_output_name,
};
use crate::query_execution::contract::ConnectorWriteExecutionRegistration;
use crate::query_execution::contract::ConnectorWriteOperationRegistration;
use crate::query_execution::request_context::QueryExecutionContext;
use crate::query_execution::{ConnectorWriteCompletion, ConnectorWriteStagingSummary};
use crate::sql::mv_refresh::PreparedDistributedWriteRequest;
use crate::sql::mv_refresh::first_refresh::{
    MvFirstRefreshExecutionArtifact, MvFirstRefreshPhysicalSql, MvFirstRefreshShape,
    MvFirstRefreshTargetContract, MvFirstRefreshWritePreparer, MvFirstRefreshWriteRequest,
    PreparedMvFirstRefreshWrite,
};
use crate::sql::mv_refresh::incremental::PreparedMvIncrementalWrite;
use crate::sql::planner::distributed::write::sink::IcebergWriteSinkSpec;

/// Core-side implementation installed into the frontend composition through
/// the typed MV activation port. It retains only a weak engine reference, so
/// it cannot keep an engine or an all-in-one runtime alive past shutdown.
pub(crate) struct StandaloneMvFirstRefreshWriteActivator {
    state: Weak<StandaloneState>,
}

impl StandaloneMvFirstRefreshWriteActivator {
    pub(crate) fn new(state: Weak<StandaloneState>) -> Self {
        Self { state }
    }
}

impl crate::mv::application::MvFirstRefreshWriteActivator
    for StandaloneMvFirstRefreshWriteActivator
{
    fn bind_first_refresh_write(
        &self,
        prepared: PreparedMvFirstRefreshWrite,
        exact_lease: &ConnectorWriteLease,
        execution: &QueryExecutionContext,
    ) -> Result<PreparedDistributedWriteRequest, String> {
        let state = self.state.upgrade().ok_or_else(|| {
            "MV first-refresh write activator is unavailable during engine shutdown".to_string()
        })?;
        bind_prepared_mv_first_refresh_staging(&state, prepared, exact_lease, execution)
    }

    fn bind_incremental_refresh_write(
        &self,
        prepared: PreparedMvIncrementalWrite,
        exact_lease: &ConnectorWriteLease,
        execution: &QueryExecutionContext,
    ) -> Result<PreparedDistributedWriteRequest, String> {
        let state = self.state.upgrade().ok_or_else(|| {
            "MV incremental write activator is unavailable during engine shutdown".to_string()
        })?;
        crate::engine::mv::iceberg_refresh::bind_prepared_mv_incremental_staging(
            &state,
            prepared,
            exact_lease,
            execution,
        )
    }
}

/// Bounded facts emitted by the feature-gated native fixture.  This deliberately
/// contains no report frame, provider receipt, Arrow batch, or query result.
#[cfg(feature = "mv-first-refresh-staging-test-support")]
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MvFirstRefreshStagingTestOutcome {
    pub staging_branch: String,
    pub input_rows: u64,
    pub staged_bytes: u64,
    pub artifact_count: u64,
    pub writer_count: u32,
}

/// Execute one already prepared SQL-shaped first refresh through the real
/// frontend coordinator and commit only its isolated staging branch.  This is
/// compiled exclusively for the native cross-process fixture; the default
/// REFRESH application route does not call it.
#[cfg(feature = "mv-first-refresh-staging-test-support")]
#[allow(clippy::too_many_arguments)]
pub(crate) fn execute_mv_first_refresh_staging_for_test(
    state: &Arc<StandaloneState>,
    ctx: &crate::mv::refresh::execution_context::IcebergMvRefreshContext,
    shape: MvFirstRefreshShape,
    physical_sql: MvFirstRefreshPhysicalSql,
    staging_branch: String,
    execution: &QueryExecutionContext,
) -> Result<MvFirstRefreshStagingTestOutcome, String> {
    execute_mv_first_refresh_staging_with_preparer_for_test(
        state,
        ctx,
        staging_branch.clone(),
        execution,
        move |operation_id, observed_binding, connector_context| {
            prepare_mv_first_refresh_sql_write(
                ctx,
                shape,
                physical_sql,
                &staging_branch,
                operation_id,
                observed_binding,
                connector_context,
            )
        },
    )
}

#[cfg(feature = "mv-first-refresh-staging-test-support")]
#[allow(clippy::too_many_arguments)]
pub(crate) fn execute_mv_first_refresh_join_staging_for_test(
    state: &Arc<StandaloneState>,
    ctx: &crate::mv::refresh::execution_context::IcebergMvRefreshContext,
    shape: MvFirstRefreshShape,
    append: crate::mv::refresh::join_first_refresh::JoinFirstRefreshAppendLogicalPlan,
    staging_branch: String,
    execution: &QueryExecutionContext,
) -> Result<MvFirstRefreshStagingTestOutcome, String> {
    execute_mv_first_refresh_staging_with_preparer_for_test(
        state,
        ctx,
        staging_branch.clone(),
        execution,
        move |operation_id, observed_binding, connector_context| {
            prepare_mv_first_refresh_join_write(
                ctx,
                shape,
                append,
                &staging_branch,
                operation_id,
                observed_binding,
                connector_context,
            )
        },
    )
}

#[cfg(feature = "mv-first-refresh-staging-test-support")]
fn execute_mv_first_refresh_staging_with_preparer_for_test<F>(
    state: &Arc<StandaloneState>,
    ctx: &crate::mv::refresh::execution_context::IcebergMvRefreshContext,
    staging_branch: String,
    execution: &QueryExecutionContext,
    prepare: F,
) -> Result<MvFirstRefreshStagingTestOutcome, String>
where
    F: FnOnce(
        ConnectorWriteOperationId,
        ConnectorExecutionBindingKey,
        novarocks_spi::connector::ConnectorRequestContext,
    ) -> Result<PreparedMvFirstRefreshWrite, String>,
{
    let catalog_instance = ConnectorInstanceId::parse(&ctx.rewrite.target.catalog)
        .map_err(|error| format!("MV first-refresh test catalog identity: {error}"))?;
    let planning_lease = state
        .connector_control
        .acquire_current(&catalog_instance)
        .map_err(|error| format!("acquire MV first-refresh test control lease: {error}"))?;
    let observed_binding = ConnectorExecutionBindingKey {
        instance_id: catalog_instance.clone(),
        incarnation: planning_lease.binding().incarnation(),
    };
    let operation_id = ConnectorWriteOperationId::new();
    let connector_context = ctx
        .connector_context
        .as_ref()
        .ok_or_else(|| {
            "MV first-refresh test is missing its connector request context".to_string()
        })?
        .clone();

    // Preparation is intentionally complete before the staging-ref mutation.
    let prepared = prepare(operation_id, observed_binding, connector_context.clone())?;
    let mutation_lease = planning_lease
        .derive_mutation_lease()
        .map_err(|error| format!("derive MV first-refresh test mutation lease: {error}"))?;
    let expected_snapshot_id = ctx.rewrite.target_snapshot_id.ok_or_else(|| {
        "MV first-refresh test requires a bootstrap snapshot before staging".to_string()
    })?;
    let staging_ref = crate::connector::mutation::resolve_catalog_mutation_with_lease(
        &mutation_lease,
        ConnectorMutationOperationId::new(),
        ConnectorCatalogMutationOperation::AlterRef {
            table: ConnectorTableIdentity {
                instance_id: catalog_instance,
                namespace: Arc::from(ctx.rewrite.target.namespace.as_str()),
                table: Arc::from(ctx.rewrite.target.table.as_str()),
            },
            action: ConnectorRefAction::Create {
                kind: ConnectorRefKind::Branch,
                name: Arc::from(staging_branch.as_str()),
                snapshot_id: Some(expected_snapshot_id),
                policy: CreateOrReplacePolicy::FailIfExists,
            },
        },
        connector_context.clone(),
    );
    match staging_ref {
        crate::connector::mutation::ResolvedCatalogMutation::KnownCommitted(completed)
            if matches!(
                completed.finalization,
                novarocks_spi::connector::ExternalMutationFinalization::Complete
            ) => {}
        crate::connector::mutation::ResolvedCatalogMutation::KnownCommitted(completed) => {
            return Err(format!(
                "MV first-refresh test staging-ref finalization failed: {:?}",
                completed.finalization
            ));
        }
        crate::connector::mutation::ResolvedCatalogMutation::KnownUncommitted { failure } => {
            return Err(format!(
                "MV first-refresh test staging ref is known uncommitted: {failure}"
            ));
        }
        crate::connector::mutation::ResolvedCatalogMutation::CommitUnknown { failure, .. } => {
            return Err(format!(
                "MV first-refresh test staging ref remains commit unknown: {failure}"
            ));
        }
        crate::connector::mutation::ResolvedCatalogMutation::ContractFailure { error, .. } => {
            return Err(format!(
                "MV first-refresh test staging-ref contract failure: {error}"
            ));
        }
    }

    let write_lease = planning_lease
        .derive_write_lease()
        .map_err(|error| format!("derive MV first-refresh test write lease: {error}"))?;
    let (sink_spec, template) = activate_mv_first_refresh_connector_write(
        state,
        &prepared,
        std::collections::BTreeMap::from([
            (
                "novarocks.mv.first-refresh-test".to_string(),
                "true".to_string(),
            ),
            (
                "novarocks.mv.first-refresh-operation".to_string(),
                operation_id.to_string(),
            ),
        ]),
        &write_lease,
    )?;
    let registration = ConnectorWriteOperationRegistration::single(template);
    let session = state
        .query_execution
        .begin_write_operation_with_lease(registration, write_lease)
        .map_err(|error| format!("seal MV first-refresh test write operation: {error}"))?;
    let write_registration =
        ConnectorWriteExecutionRegistration::try_new(session, prepared.primary_cohort())
            .map_err(|error| format!("register MV first-refresh test cohort: {error}"))?;
    let (completion, summary) = execute_prepared_mv_first_refresh_staging(
        state,
        prepared,
        sink_spec,
        execution,
        write_registration,
        Some(ctx),
    )?;
    if summary.artifact_count() == 0 {
        completion
            .session()
            .abort(connector_context)
            .map_err(|error| format!("abort empty MV first-refresh test staging: {error}"))?;
        return Ok(MvFirstRefreshStagingTestOutcome {
            staging_branch,
            input_rows: summary.input_rows(),
            staged_bytes: summary.staged_bytes(),
            artifact_count: 0,
            writer_count: summary.writer_count(),
        });
    }
    match crate::query_execution::connector_write_transaction::commit(&completion)
        .map_err(|error| format!("commit MV first-refresh test staging: {error}"))?
    {
        novarocks_spi::connector::ExternalMutationOutcome::KnownCommitted {
            finalization: novarocks_spi::connector::ExternalMutationFinalization::Complete,
            ..
        } => Ok(MvFirstRefreshStagingTestOutcome {
            staging_branch,
            input_rows: summary.input_rows(),
            staged_bytes: summary.staged_bytes(),
            artifact_count: summary.artifact_count(),
            writer_count: summary.writer_count(),
        }),
        novarocks_spi::connector::ExternalMutationOutcome::KnownCommitted {
            finalization, ..
        } => Err(format!(
            "MV first-refresh test staging commit finalized with failure: {finalization:?}"
        )),
        novarocks_spi::connector::ExternalMutationOutcome::KnownUncommitted { failure } => Err(
            format!("MV first-refresh test staging commit is known uncommitted: {failure}"),
        ),
        novarocks_spi::connector::ExternalMutationOutcome::CommitUnknown { failure, .. } => Err(
            format!("MV first-refresh test staging commit remains unknown: {failure}"),
        ),
    }
}

/// Build the ordinary data-writer sink from the target frozen by the MV
/// refresh context.  This adapter owns concrete Iceberg table metadata; the
/// SQL prepared artifact itself remains provider-neutral and contains none of
/// these catalog handles.
pub(crate) fn build_mv_first_refresh_sink_spec(
    ctx: &crate::mv::refresh::execution_context::IcebergMvRefreshContext,
) -> Result<IcebergWriteSinkSpec, String> {
    let target = crate::engine::backend_resolver::TargetBackend {
        backend_name: "iceberg",
        catalog: ctx.rewrite.target.catalog.clone(),
        namespace: ctx.rewrite.target.namespace.clone(),
        table: ctx.rewrite.target.table.clone(),
    };
    let target_table = ctx.target_bindings.runtime().target_table();
    let columns = crate::engine::iceberg_writer::iceberg_insert_columns_from_schema(
        target_table.metadata().current_schema(),
    )?;
    let resolved = crate::connector::backend::ResolvedTable {
        catalog: target.catalog.clone(),
        namespace: target.namespace.clone(),
        table: target.table.clone(),
        columns: columns.clone(),
        statistics_pin: None,
    };
    crate::engine::iceberg_writer::build_insert_write_sink_spec(
        &target,
        &resolved,
        target_table,
        ctx.target_bindings.runtime().target_entry(),
        &columns,
    )
}

/// Freeze an SQL-shaped first refresh against the already-validated target
/// context. This is pure preparation: it allocates no catalog branch, control
/// lease, writer service, fragment or backend work.
#[allow(clippy::too_many_arguments)]
pub(crate) fn prepare_mv_first_refresh_sql_write(
    ctx: &crate::mv::refresh::execution_context::IcebergMvRefreshContext,
    shape: MvFirstRefreshShape,
    physical_sql: MvFirstRefreshPhysicalSql,
    staging_branch: &str,
    operation_id: ConnectorWriteOperationId,
    observed_binding: novarocks_spi::connector::ConnectorExecutionBindingKey,
    connector_context: novarocks_spi::connector::ConnectorRequestContext,
) -> Result<PreparedMvFirstRefreshWrite, String> {
    if matches!(
        shape,
        MvFirstRefreshShape::Join | MvFirstRefreshShape::JoinAggregate
    ) {
        return Err("join first-refresh must use the typed append logical artifact".to_string());
    }
    let request = mv_first_refresh_request(
        ctx,
        shape,
        staging_branch,
        operation_id,
        observed_binding,
        connector_context,
    )?;
    MvFirstRefreshWritePreparer::prepare(request, physical_sql)
}

/// Freeze a canonical join append projection behind the same request facts as
/// SQL-shaped first refreshes. The typed logical plan is still not prepared
/// into native fragments until the exact write lease is active.
#[allow(clippy::too_many_arguments)]
pub(crate) fn prepare_mv_first_refresh_join_write(
    ctx: &crate::mv::refresh::execution_context::IcebergMvRefreshContext,
    shape: MvFirstRefreshShape,
    append: crate::mv::refresh::join_first_refresh::JoinFirstRefreshAppendLogicalPlan,
    staging_branch: &str,
    operation_id: ConnectorWriteOperationId,
    observed_binding: novarocks_spi::connector::ConnectorExecutionBindingKey,
    connector_context: novarocks_spi::connector::ConnectorRequestContext,
) -> Result<PreparedMvFirstRefreshWrite, String> {
    if !matches!(
        shape,
        MvFirstRefreshShape::Join | MvFirstRefreshShape::JoinAggregate
    ) {
        return Err("typed first-refresh append artifact requires a join shape".to_string());
    }
    let request = mv_first_refresh_request(
        ctx,
        shape,
        staging_branch,
        operation_id,
        observed_binding,
        connector_context,
    )?;
    MvFirstRefreshWritePreparer::prepare_join_logical(request, append, frozen_logical_context(ctx))
}

pub(crate) fn frozen_logical_context(
    ctx: &crate::mv::refresh::execution_context::IcebergMvRefreshContext,
) -> crate::sql::mv_refresh::first_refresh::MvFirstRefreshLogicalContext {
    crate::sql::mv_refresh::first_refresh::MvFirstRefreshLogicalContext {
        mv_definition: (*ctx.rewrite.mv_definition).clone(),
        canonical_select_query: (*ctx.rewrite.canonical_select_query).clone(),
        base_refs: ctx.rewrite.base_refs.to_vec(),
        pin: (*ctx.rewrite.pin).clone(),
        previous_snapshot_ids: ctx.rewrite.previous_snapshot_ids.clone(),
        previous_table_uuids: ctx.rewrite.previous_table_uuids.clone(),
        target_table_uuid: ctx.rewrite.target_table_uuid.clone(),
        affected_partitions: ctx.affected_partitions.clone(),
    }
}

#[allow(clippy::too_many_arguments)]
fn mv_first_refresh_request(
    ctx: &crate::mv::refresh::execution_context::IcebergMvRefreshContext,
    shape: MvFirstRefreshShape,
    staging_branch: &str,
    operation_id: ConnectorWriteOperationId,
    observed_binding: novarocks_spi::connector::ConnectorExecutionBindingKey,
    connector_context: novarocks_spi::connector::ConnectorRequestContext,
) -> Result<MvFirstRefreshWriteRequest, String> {
    if staging_branch.is_empty() {
        return Err("MV first-refresh staging branch is empty".to_string());
    }
    let target = crate::engine::backend_resolver::TargetBackend {
        backend_name: "iceberg",
        catalog: ctx.rewrite.target.catalog.clone(),
        namespace: ctx.rewrite.target.namespace.clone(),
        table: ctx.rewrite.target.table.clone(),
    };
    let runtime = ctx.target_bindings.runtime();
    let persisted_partition_spec_id = ctx
        .rewrite
        .schema_contract
        .target
        .partition
        .as_ref()
        .map(|partition| partition.target_spec_id)
        .unwrap_or_else(|| {
            runtime
                .target_table()
                .metadata()
                .default_partition_spec_id()
        });
    if runtime
        .target_table()
        .metadata()
        .default_partition_spec_id()
        != persisted_partition_spec_id
    {
        return Err(
            "MV first-refresh target partition spec drifted from its persisted contract"
                .to_string(),
        );
    }
    let hidden_hash_key = ctx
        .rewrite
        .schema_contract
        .target
        .hidden_apply_key
        .column_name
        .clone();
    let target_schema = ctx.rewrite.target_schema.as_ref();
    let target_arrow_schema = iceberg::arrow::schema_to_arrow_schema(target_schema)
        .map_err(|error| format!("convert MV first-refresh target schema to Arrow: {error}"))?;
    let target_field_ids = target_schema
        .as_struct()
        .fields()
        .iter()
        .map(|field| field.id)
        .collect();
    if target_schema.field_by_name(&hidden_hash_key).is_none() {
        return Err(format!(
            "MV first-refresh target schema is missing hidden hash key {hidden_hash_key}"
        ));
    }
    let target_contract = MvFirstRefreshTargetContract::try_new(
        std::sync::Arc::new(target_arrow_schema),
        target_field_ids,
        persisted_partition_spec_id,
        hidden_hash_key,
    )?;
    let table =
        crate::engine::iceberg_writer::iceberg_connector_table_handle(&target, staging_branch)?;
    MvFirstRefreshWriteRequest::try_new(
        ctx.rewrite.canonical_select_query.to_string(),
        shape,
        target.catalog,
        target.namespace,
        target.table,
        staging_branch.to_string(),
        ctx.rewrite.current_catalog.clone(),
        ctx.rewrite.current_database.clone(),
        ctx.rewrite.target_snapshot_id,
        table,
        target_contract,
        observed_binding,
        operation_id,
        connector_context,
    )
}

/// Reserve the one primary first-refresh write cohort from facts frozen in an
/// MV refresh context. The staging branch must already exist and `exact_lease`
/// must have been derived from the retained target control binding. This is
/// the first point that mutates the provider write-service registry; SQL
/// artifact preparation remains side-effect free.
pub(crate) fn activate_mv_first_refresh_connector_write(
    state: &Arc<StandaloneState>,
    prepared: &PreparedMvFirstRefreshWrite,
    mut provenance_properties: std::collections::BTreeMap<String, String>,
    exact_lease: &ConnectorWriteLease,
) -> Result<
    (
        IcebergWriteSinkSpec,
        crate::query_execution::contract::ConnectorWritePlanningTemplate,
    ),
    String,
> {
    if prepared.observed_binding() != exact_lease.binding_key() {
        return Err("MV first-refresh write lease drifted from prepared binding".to_string());
    }
    for (key, value) in prepared.provenance_properties() {
        if provenance_properties
            .insert(key.clone(), value.clone())
            .is_some()
        {
            return Err("MV first-refresh provenance was supplied twice".to_string());
        }
    }
    let operation_id: ConnectorWriteOperationId = prepared.operation_id();
    let target = crate::engine::backend_resolver::TargetBackend {
        backend_name: "iceberg",
        catalog: prepared.target_catalog().to_string(),
        namespace: prepared.target_namespace().to_string(),
        table: prepared.target_name().to_string(),
    };
    // The staging branch was just created through the provider mutation
    // contract.  The refresh context intentionally remains immutable, so its
    // table handle predates that mutation.  Reload only in this provider
    // activation adapter: this is the authoritative read used to validate the
    // ref CAS facts and construct the writer, never a SQL-preparation side
    // effect.
    let entry = state
        .iceberg_catalogs
        .read()
        .map_err(|error| {
            format!("read Iceberg catalog registry for first-refresh activation: {error}")
        })?
        .get(&target.catalog)?;
    entry.invalidate_table_cache(&target.namespace, &target.table);
    let target_table =
        crate::connector::iceberg::catalog::load_table(&entry, &target.namespace, &target.table)
            .map_err(|error| format!("reload MV first-refresh staging target: {error}"))?
            .table;
    validate_first_refresh_target_contract(&target_table, prepared.target_contract())?;
    let columns = crate::engine::iceberg_writer::iceberg_insert_columns_from_schema(
        target_table.metadata().current_schema(),
    )?;
    let resolved = crate::connector::backend::ResolvedTable {
        catalog: target.catalog.clone(),
        namespace: target.namespace.clone(),
        table: target.table.clone(),
        columns,
        statistics_pin: None,
    };
    let sink_spec = crate::engine::iceberg_writer::build_insert_write_sink_spec(
        &target,
        &resolved,
        &target_table,
        &entry,
        &resolved.columns,
    )?;
    let ident = TableIdent::new(
        NamespaceIdent::new(target.namespace.clone()),
        target.table.clone(),
    );
    let collector = crate::mv::refresh::change_stream_write::new_iceberg_mv_commit_collector(
        &target_table,
        &ident,
        prepared.staging_branch(),
        match prepared.write_mode() {
            crate::sql::mv_refresh::first_refresh::MvStagedRefreshWriteMode::Append => {
                CommitOpKind::FastAppend
            }
            crate::sql::mv_refresh::first_refresh::MvStagedRefreshWriteMode::FullOverwrite => {
                CommitOpKind::Overwrite
            }
        },
    );
    let catalog = crate::connector::iceberg::catalog::registry::build_iceberg_catalog(&entry)?;
    let abort_cleanup =
        crate::engine::iceberg_writer::build_abort_cleanup_for_catalog_entry(&entry)?;
    let commit_executor = Arc::new(crate::engine::IcebergWriteCommitExecutor {
        state: Arc::downgrade(state),
        target: target.clone(),
        catalog,
        table: target_table.clone(),
        collector: Arc::clone(&collector),
        fs: abort_cleanup.fs,
        cleanup_path_mapper: abort_cleanup.path_mapper,
        cow_update_rewrite: None,
        target_ref: prepared.staging_branch().to_string(),
        snapshot_properties: std::collections::BTreeMap::new(),
    });
    let payload = IcebergFirstRefreshWritePlanPayloadV2 {
        version: 2,
        target: format!("{}.{}.{}", target.catalog, target.namespace, target.table),
        target_ref: prepared.staging_branch().to_string(),
        expected_snapshot_id: prepared.expected_target_snapshot_id(),
        staging_path: collector.staging_dir.clone(),
        provenance_properties,
    };
    let writer_handle_payload =
        crate::connector::iceberg::write_contract::encode_data_sink_spec_handle_payload(
            &sink_spec,
        )?;
    let template = crate::engine::iceberg_writer::activate_iceberg_first_refresh_connector_write(
        state,
        &target,
        prepared.staging_branch(),
        Arc::clone(prepared.target_contract().schema()),
        writer_handle_payload,
        payload,
        commit_executor,
        match prepared.write_mode() {
            crate::sql::mv_refresh::first_refresh::MvStagedRefreshWriteMode::Append => {
                novarocks_spi::connector::ConnectorWriteIntent::Append
            }
            crate::sql::mv_refresh::first_refresh::MvStagedRefreshWriteMode::FullOverwrite => {
                novarocks_spi::connector::ConnectorWriteIntent::Overwrite
            }
        },
        match prepared.write_mode() {
            crate::sql::mv_refresh::first_refresh::MvStagedRefreshWriteMode::Append => {
                crate::connector::iceberg::write_service::IcebergMvPrimaryEmptyInputPolicy::AbortWithoutSnapshot
            }
            crate::sql::mv_refresh::first_refresh::MvStagedRefreshWriteMode::FullOverwrite => {
                crate::connector::iceberg::write_service::IcebergMvPrimaryEmptyInputPolicy::CommitEmptyOverwrite
            }
        },
        operation_id,
        prepared.connector_context().clone(),
        exact_lease,
    )?;
    Ok((sink_spec, template))
}

fn validate_first_refresh_target_contract(
    target_table: &iceberg::table::Table,
    contract: &MvFirstRefreshTargetContract,
) -> Result<(), String> {
    let actual_schema = target_table.metadata().current_schema();
    let actual_arrow_schema = iceberg::arrow::schema_to_arrow_schema(actual_schema)
        .map_err(|error| format!("convert MV first-refresh activation schema to Arrow: {error}"))?;
    let actual_field_ids = actual_schema
        .as_struct()
        .fields()
        .iter()
        .map(|field| field.id)
        .collect::<Vec<_>>();
    contract.validate_observed(
        &actual_arrow_schema,
        &actual_field_ids,
        target_table.metadata().default_partition_spec_id(),
    )
}

/// Bind an SQL-shaped first-refresh artifact only after the frontend has
/// retained its exact write lease and admitted an immutable query execution.
/// The result is the same generic result-free writer request used by all
/// other frontend-owned write lifecycles; it deliberately does not submit a
/// query, commit a provider mutation, or expose row payloads.
pub(crate) fn bind_prepared_mv_first_refresh_staging(
    state: &Arc<StandaloneState>,
    prepared: PreparedMvFirstRefreshWrite,
    exact_lease: &ConnectorWriteLease,
    execution: &QueryExecutionContext,
) -> Result<PreparedDistributedWriteRequest, String> {
    let operation_id = prepared.operation_id();
    let cohort_id = prepared.primary_cohort();
    let expected_target_snapshot_id = prepared.expected_target_snapshot_id();
    let target_catalog = prepared.target_catalog().to_string();
    let target_namespace = prepared.target_namespace().to_string();
    let target_name = prepared.target_name().to_string();
    let current_catalog = prepared.current_catalog().map(str::to_string);
    let current_database = prepared.current_database().to_string();
    let connector_context = prepared.connector_context().clone();
    let root_distribution = iceberg_write_shuffle_by_output_name(prepared.root_hash_column());
    let (sink_spec, template) = activate_mv_first_refresh_connector_write(
        state,
        &prepared,
        std::collections::BTreeMap::new(),
        exact_lease,
    )?;
    let distributed = match prepared.into_execution_artifact() {
        MvFirstRefreshExecutionArtifact::Sql(physical_sql) => {
            let query = parse_query_from_sql(physical_sql.sql())?;
            crate::engine::prepare_query_as_iceberg_write_with_connector_binding(
                state,
                current_catalog.as_deref(),
                &current_database,
                &query,
                sink_spec,
                None,
                Some(root_distribution),
                execution,
                &connector_context,
                template,
            )?
        }
        MvFirstRefreshExecutionArtifact::Logical(logical) => {
            let (logical_plan, factory, facts) = logical.into_parts();
            let refresh_context = rebuild_frozen_mv_refresh_context(
                state,
                current_catalog.as_deref(),
                &current_database,
                expected_target_snapshot_id,
                &target_catalog,
                &target_namespace,
                &target_name,
                &facts,
            )?;
            crate::engine::prepare_logical_plan_as_iceberg_write_with_connector_binding(
                state,
                logical_plan,
                factory,
                sink_spec,
                root_distribution,
                execution,
                &connector_context,
                &refresh_context,
                template,
            )?
        }
    };
    if distributed.write_operation_id() != operation_id
        || distributed.write_cohort_id() != cohort_id
    {
        return Err("MV first-refresh distributed artifact identity mismatch".to_string());
    }
    Ok(distributed)
}

pub(crate) fn rebuild_frozen_mv_refresh_context(
    state: &Arc<StandaloneState>,
    current_catalog: Option<&str>,
    current_database: &str,
    expected_target_snapshot_id: Option<i64>,
    target_catalog: &str,
    target_namespace: &str,
    target_name: &str,
    facts: &crate::sql::mv_refresh::first_refresh::MvFirstRefreshLogicalContext,
) -> Result<crate::mv::refresh::execution_context::IcebergMvRefreshContext, String> {
    let target_identity =
        novarocks_catalog::identifier::TableIdentity {
            catalog: facts.mv_definition.target_catalog.clone().ok_or_else(|| {
                "MV first-refresh logical artifact target has no connector catalog".to_string()
            })?,
            namespace: facts
                .mv_definition
                .target_namespace
                .clone()
                .ok_or_else(|| {
                    "MV first-refresh logical artifact target has no namespace".to_string()
                })?,
            table: facts.mv_definition.target_table.clone().ok_or_else(|| {
                "MV first-refresh logical artifact target has no table".to_string()
            })?,
        };
    if target_identity.catalog != target_catalog
        || target_identity.namespace != target_namespace
        || target_identity.table != target_name
    {
        return Err(
            "MV refresh logical artifact target does not match its frozen write request"
                .to_string(),
        );
    }
    validate_frozen_join_base_facts(state, facts)?;
    let entry = state
        .iceberg_catalogs
        .read()
        .map_err(|error| format!("read Iceberg catalog registry for join activation: {error}"))?
        .get(&target_identity.catalog)?;
    entry.invalidate_table_cache(&target_identity.namespace, &target_identity.table);
    let target_table = crate::connector::iceberg::catalog::load_table(
        &entry,
        &target_identity.namespace,
        &target_identity.table,
    )
    .map_err(|error| format!("reload MV join staging target: {error}"))?
    .table;
    if target_table.metadata().uuid().to_string() != facts.target_table_uuid {
        return Err(
            "MV refresh logical artifact target UUID drifted after preparation".to_string(),
        );
    }
    let actual_target_snapshot_id = target_table
        .metadata()
        .current_snapshot()
        .map(|snapshot| snapshot.snapshot_id());
    if actual_target_snapshot_id != expected_target_snapshot_id {
        return Err(
            "MV refresh logical artifact target snapshot drifted after preparation".to_string(),
        );
    }
    let catalog = crate::connector::iceberg::catalog::registry::build_iceberg_catalog(&entry)?;
    let catalogs = state
        .iceberg_catalogs
        .read()
        .map_err(|error| format!("read Iceberg catalog registry for join context: {error}"))?;
    crate::mv::refresh::execution_context::IcebergMvRefreshContext::new_with_validated_inputs_and_pruning_limits(
        target_identity,
        facts.mv_definition.mv_id,
        current_catalog,
        current_database,
        Arc::new(facts.mv_definition.clone()),
        Arc::new(facts.canonical_select_query.clone()),
        Arc::from(facts.base_refs.clone()),
        Arc::new(facts.pin.clone()),
        facts.previous_snapshot_ids.clone(),
        facts.previous_table_uuids.clone(),
        expected_target_snapshot_id,
        facts.target_table_uuid.clone(),
        &catalogs,
        Arc::new(entry),
        catalog,
        target_table,
        facts.affected_partitions.clone(),
        state.mv_refresh_pruning_limits,
    )
}

fn validate_frozen_join_base_facts(
    state: &Arc<StandaloneState>,
    facts: &crate::sql::mv_refresh::first_refresh::MvFirstRefreshLogicalContext,
) -> Result<(), String> {
    if facts.base_refs.is_empty() || facts.pin.len() != facts.base_refs.len() {
        return Err(
            "MV first-refresh logical artifact has incomplete base snapshot pins".to_string(),
        );
    }
    for base in &facts.base_refs {
        let pinned_uuid = facts.pin.uuid(base).ok_or_else(|| {
            format!(
                "MV first-refresh logical artifact has no UUID pin for {}",
                base.fqn()
            )
        })?;
        let loaded = crate::engine::mv::refresh_io::load_current_iceberg_base_table(state, base)?;
        if loaded.table.metadata().uuid().to_string() != pinned_uuid {
            return Err(format!(
                "MV first-refresh join base table identity drifted after preparation for {}",
                base.fqn()
            ));
        }
    }
    Ok(())
}

pub(crate) fn execute_prepared_mv_first_refresh_staging(
    state: &Arc<StandaloneState>,
    prepared: PreparedMvFirstRefreshWrite,
    sink_spec: IcebergWriteSinkSpec,
    execution: &QueryExecutionContext,
    registration: ConnectorWriteExecutionRegistration,
    mv_refresh_ctx: Option<&crate::mv::refresh::execution_context::IcebergMvRefreshContext>,
) -> Result<(ConnectorWriteCompletion, ConnectorWriteStagingSummary), String> {
    if registration.session().operation_id() != prepared.operation_id()
        || registration.cohort_id() != prepared.primary_cohort()
    {
        return Err("MV first-refresh staging registration identity mismatch".to_string());
    }
    if sink_spec.target_columns.len() != prepared.target_contract().schema().fields().len() {
        return Err(
            "MV first-refresh staging sink schema does not match target contract".to_string(),
        );
    }
    let connector_context = prepared.connector_context().clone();
    let root_hash_column = prepared.root_hash_column().to_string();
    let current_catalog = prepared.current_catalog().map(str::to_string);
    let current_database = prepared.current_database().to_string();
    match prepared.into_execution_artifact() {
        MvFirstRefreshExecutionArtifact::Sql(physical_sql) => {
            let query = parse_query_from_sql(physical_sql.sql())?;
            let root_distribution = iceberg_write_shuffle_by_output_name(root_hash_column);
            execute_query_as_iceberg_staging_in_operation_with_connector_context(
                state,
                current_catalog.as_deref(),
                &current_database,
                &query,
                sink_spec,
                None,
                root_distribution,
                Some(execution),
                &connector_context,
                registration,
            )
        }
        MvFirstRefreshExecutionArtifact::Logical(logical) => {
            let mv_refresh_ctx = mv_refresh_ctx.ok_or_else(|| {
                "MV first-refresh logical staging requires its frozen refresh context".to_string()
            })?;
            let (logical_plan, factory, _frozen_context) = logical.into_parts();
            execute_logical_plan_as_iceberg_staging_in_operation_with_connector_context(
                state,
                logical_plan,
                factory,
                sink_spec,
                iceberg_write_shuffle_by_output_name(root_hash_column),
                execution,
                &connector_context,
                mv_refresh_ctx,
                registration,
            )
        }
    }
}

fn parse_query_from_sql(sql: &str) -> Result<sqlparser::ast::Query, String> {
    let normalized = crate::sql::parser::dialect::normalize_for_raw_parse(sql)?;
    let statement = crate::sql::parser::parse_normalized_sql_raw(&normalized)?;
    let sqlparser::ast::Statement::Query(query) = statement else {
        return Err("MV first-refresh physical artifact is not a SELECT query".to_string());
    };
    Ok(*query)
}
