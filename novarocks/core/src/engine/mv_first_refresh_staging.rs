// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements. See the NOTICE file distributed with this
// work for additional information regarding copyright ownership.

//! Native, result-free consumer for a prepared MV first-refresh append.
//!
//! This module is deliberately not wired into the production REFRESH route.
//! MVX-2W exercises it through the native fixture; MVX-2 will make the route
//! switch only after that fixture proves the data plane.

use std::sync::Arc;
use std::sync::atomic::AtomicBool;

use novarocks_spi::connector::{
    ConnectorCatalogMutationOperation, ConnectorControlPlanningLease, ConnectorControlResolver,
    ConnectorExecutionBindingKey, ConnectorInstanceId, ConnectorMutationOperationId,
    ConnectorRefAction, ConnectorRefKind, ConnectorTableIdentity, ConnectorWriteLease,
    ConnectorWriteOperationId, CreateOrReplacePolicy,
};

use crate::engine::query_planning::bindings::QueryTableBindingStore;
use crate::engine::query_planning::write_sink::{
    admit_prepared_connector_write_target, sql_write_plan_input_for_admitted_target,
};
use crate::engine::{
    StandaloneState, execute_query_as_iceberg_staging_in_operation_with_connector_context,
    iceberg_write_shuffle_by_output_name,
};
use crate::mv::application::{
    MvFirstRefreshExecutionArtifact, MvFirstRefreshLogicalContext, MvFirstRefreshWritePreparer,
    MvFirstRefreshWriteRequest, MvRefreshPublicationBase, MvRefreshPublicationIntent,
    MvRefreshPublicationTechnique, MvStagedRefreshWriteMode, PreparedMvFirstRefreshWrite,
};
use crate::query_execution::contract::ConnectorWriteExecutionRegistration;
use crate::query_execution::contract::ConnectorWriteOperationRegistration;
use crate::query_execution::prepared_write::PreparedDistributedWriteRequest;
use crate::query_execution::request_context::QueryExecutionContext;
use crate::query_execution::{ConnectorWriteCompletion, ConnectorWriteStagingSummary};
use crate::sql::mv_refresh::first_refresh::{
    MvFirstRefreshPhysicalSql, MvFirstRefreshShape, MvFirstRefreshTargetContract,
    SqlMvFirstRefreshArtifact, SqlMvFirstRefreshArtifactInput, SqlMvFirstRefreshPlanner,
    SqlMvFirstRefreshPlannerInput,
};

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
        move |operation_id, observed_binding| {
            prepare_mv_first_refresh_sql_write(
                state,
                ctx,
                shape,
                physical_sql,
                &staging_branch,
                operation_id,
                observed_binding,
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
    _append: crate::mv::refresh::join_first_refresh::JoinFirstRefreshAppendLogicalPlan,
    staging_branch: String,
    execution: &QueryExecutionContext,
) -> Result<MvFirstRefreshStagingTestOutcome, String> {
    execute_mv_first_refresh_staging_with_preparer_for_test(
        state,
        ctx,
        staging_branch.clone(),
        execution,
        move |operation_id, observed_binding| {
            prepare_mv_first_refresh_join_write(
                state,
                ctx,
                shape,
                &staging_branch,
                operation_id,
                observed_binding,
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
    let prepared = prepare(operation_id, observed_binding)?;
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
    let template = crate::engine::mv::iceberg_activation::activate_first_refresh_connector_write(
        state,
        &prepared,
        connector_context.clone(),
        &write_lease,
    )?;
    let registration = ConnectorWriteOperationRegistration::single(template);
    let session = state
        .query_execution
        .begin_write_operation(registration, write_lease)
        .map_err(|error| format!("seal MV first-refresh test write operation: {error}"))?;
    let write_registration =
        ConnectorWriteExecutionRegistration::try_new(session, prepared.primary_cohort())
            .map_err(|error| format!("register MV first-refresh test cohort: {error}"))?;
    let (completion, summary) = execute_prepared_mv_first_refresh_staging(
        state,
        prepared,
        planning_lease,
        execution,
        connector_context.clone(),
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

/// Freeze an SQL-shaped first refresh against the already-validated target
/// context. This is pure preparation: it allocates no catalog branch, control
/// lease, writer service, fragment or backend work.
#[allow(clippy::too_many_arguments)]
pub(crate) fn prepare_mv_first_refresh_sql_write(
    state: &Arc<StandaloneState>,
    ctx: &crate::mv::refresh::execution_context::IcebergMvRefreshContext,
    shape: MvFirstRefreshShape,
    physical_sql: MvFirstRefreshPhysicalSql,
    staging_branch: &str,
    operation_id: ConnectorWriteOperationId,
    observed_binding: novarocks_spi::connector::ConnectorExecutionBindingKey,
) -> Result<PreparedMvFirstRefreshWrite, String> {
    if matches!(
        shape,
        MvFirstRefreshShape::Join | MvFirstRefreshShape::JoinAggregate
    ) {
        return Err("join first-refresh must use the typed append logical artifact".to_string());
    }
    let request = mv_first_refresh_request(
        state,
        ctx,
        shape,
        staging_branch,
        operation_id,
        observed_binding,
    )?;
    MvFirstRefreshWritePreparer::prepare(
        request,
        physical_sql,
        legacy_fixture_publication_intent(ctx, staging_branch)?,
    )
}

/// Freeze a join first-refresh request behind immutable refresh facts. The
/// canonical join SELECT is compiled only during activation, after the
/// frontend has retained its exact planning lease.
#[allow(clippy::too_many_arguments)]
pub(crate) fn prepare_mv_first_refresh_join_write(
    state: &Arc<StandaloneState>,
    ctx: &crate::mv::refresh::execution_context::IcebergMvRefreshContext,
    shape: MvFirstRefreshShape,
    staging_branch: &str,
    operation_id: ConnectorWriteOperationId,
    observed_binding: novarocks_spi::connector::ConnectorExecutionBindingKey,
) -> Result<PreparedMvFirstRefreshWrite, String> {
    if !matches!(
        shape,
        MvFirstRefreshShape::Join | MvFirstRefreshShape::JoinAggregate
    ) {
        return Err("typed first-refresh append artifact requires a join shape".to_string());
    }
    let request = mv_first_refresh_request(
        state,
        ctx,
        shape,
        staging_branch,
        operation_id,
        observed_binding,
    )?;
    MvFirstRefreshWritePreparer::prepare_join_logical(
        request,
        frozen_logical_context_with_base_overlays(state, ctx)?,
        legacy_fixture_publication_intent(ctx, staging_branch)?,
    )
}

/// The native fixture never enters the current frontend-owned refresh route.
/// Give it an explicit test-only publication value rather than reintroducing
/// a raw provider property map into the application artifact.
fn legacy_fixture_publication_intent(
    ctx: &crate::mv::refresh::execution_context::IcebergMvRefreshContext,
    staging_branch: &str,
) -> Result<MvRefreshPublicationIntent, String> {
    let bases = ctx
        .rewrite
        .pin
        .to_snapshot_map()
        .into_iter()
        .map(|(table_fqn, to_snapshot)| {
            MvRefreshPublicationBase::try_new(
                table_fqn.clone(),
                ctx.rewrite
                    .pin
                    .to_table_uuid_map()
                    .get(&table_fqn)
                    .cloned()
                    .ok_or_else(|| {
                        format!("MV first-refresh fixture has no UUID fact for {table_fqn}")
                    })?,
                ctx.rewrite.previous_snapshot_ids.get(&table_fqn).copied(),
                to_snapshot,
            )
        })
        .collect::<Result<Vec<_>, _>>()?;
    MvRefreshPublicationIntent::try_new(
        1,
        ctx.rewrite.mv_definition.mv_id,
        "fixture".to_string(),
        MvRefreshPublicationTechnique::Full,
        bases,
        "fixture".to_string(),
        ctx.rewrite.target.catalog.clone(),
        ctx.rewrite.target.namespace.clone(),
        ctx.rewrite.target.table.clone(),
        staging_branch.to_string(),
    )
}

pub(crate) fn frozen_logical_context(
    ctx: &crate::mv::refresh::execution_context::IcebergMvRefreshContext,
) -> Result<MvFirstRefreshLogicalContext, String> {
    frozen_logical_context_from_rewrite(&ctx.rewrite, ctx.affected_partitions.clone(), None)
}

pub(crate) fn frozen_logical_context_from_rewrite(
    rewrite: &crate::mv::rewrite::context::IcebergMvRewriteContext,
    affected_partitions: crate::mv::model::AffectedTargetPartitions,
    frozen_base_overlays: Option<
        Vec<crate::engine::query_planning::catalog_materializer::QueryLocalTableOverlay>,
    >,
) -> Result<MvFirstRefreshLogicalContext, String> {
    Ok(MvFirstRefreshLogicalContext {
        mv_definition: (*rewrite.mv_definition).clone(),
        canonical_select_query: (*rewrite.canonical_select_query).clone(),
        base_refs: rewrite.base_refs.to_vec(),
        pin: crate::sql::mv_refresh::first_refresh::SqlMvSnapshotPin::try_from_maps(
            rewrite.pin.to_snapshot_map(),
            rewrite.pin.to_table_uuid_map(),
        )?,
        previous_snapshot_ids: rewrite.previous_snapshot_ids.clone(),
        previous_table_uuids: rewrite.previous_table_uuids.clone(),
        target_table_uuid: rewrite.target_table_uuid.clone(),
        affected_partitions,
        frozen_base_overlays,
    })
}

/// Retain the already-admitted base materializations in the application
/// artifact.  Logical join activation must consume these overlays rather than
/// resolving a newer connector generation after the write operation has been
/// admitted.
pub(crate) fn frozen_logical_context_with_base_overlays(
    state: &Arc<StandaloneState>,
    ctx: &crate::mv::refresh::execution_context::IcebergMvRefreshContext,
) -> Result<MvFirstRefreshLogicalContext, String> {
    let mut facts = frozen_logical_context(ctx)?;
    facts.frozen_base_overlays =
        Some(crate::engine::mv::iceberg_refresh::freeze_imv_base_query_local_overlays(state, ctx)?);
    Ok(facts)
}

#[allow(clippy::too_many_arguments)]
fn mv_first_refresh_request(
    state: &Arc<StandaloneState>,
    ctx: &crate::mv::refresh::execution_context::IcebergMvRefreshContext,
    shape: MvFirstRefreshShape,
    staging_branch: &str,
    operation_id: ConnectorWriteOperationId,
    observed_binding: novarocks_spi::connector::ConnectorExecutionBindingKey,
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
    let planning_lease = crate::connector::acquire_metadata_planning_lease(
        state.connector_control.as_ref(),
        &target.catalog,
    )?;
    let write_lease = planning_lease
        .derive_write_lease()
        .map_err(|error| format!("derive MV first-refresh request write lease: {error}"))?;
    if write_lease.binding_key() != &observed_binding {
        return Err(
            "MV first-refresh target connector generation changed during admission".to_string(),
        );
    }
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
    let target_arrow_schema = ctx.rewrite.target_arrow_schema.as_ref().clone();
    let target_field_ids: Vec<i32> = ctx.rewrite.target_field_ids.to_vec();
    if target_arrow_schema
        .field_with_name(&hidden_hash_key)
        .is_err()
    {
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
    let table = crate::engine::iceberg_writer::iceberg_connector_table_handle(
        &write_lease,
        &target,
        crate::connector::connector_request_context(None, Arc::new(AtomicBool::new(false)))?,
    )?;
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
    )
}

/// Reserve the one primary first-refresh write cohort from facts frozen in an
/// MV refresh context. The staging branch must already exist and `exact_lease`
/// must have been derived from the retained target control binding. This is
/// the first point that mutates the provider write-service registry; SQL
/// artifact preparation remains side-effect free.
/// Bind an SQL-shaped first-refresh artifact only after the frontend has
/// retained its exact write lease and admitted an immutable query execution.
/// The result is the same generic result-free writer request used by all
/// other frontend-owned write lifecycles; it deliberately does not submit a
/// query, commit a provider mutation, or expose row payloads.
pub(crate) fn bind_prepared_mv_first_refresh_staging(
    state: &Arc<StandaloneState>,
    prepared: PreparedMvFirstRefreshWrite,
    planning_lease: &ConnectorControlPlanningLease,
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
    let shape = prepared.shape();
    let target_contract = prepared.target_contract().clone();
    let connector_context =
        crate::connector::connector_request_context_for_execution(None, execution)?;
    let root_hash_column = prepared.root_hash_column().to_string();
    let root_distribution = iceberg_write_shuffle_by_output_name(root_hash_column.clone());
    let template = crate::engine::mv::iceberg_activation::activate_first_refresh_connector_write(
        state,
        &prepared,
        connector_context.clone(),
        exact_lease,
    )?;
    let distributed = match prepared.into_execution_artifact() {
        MvFirstRefreshExecutionArtifact::Sql(physical_sql) => {
            let bindings = Arc::new(QueryTableBindingStore::try_new()?);
            let target_binding = admit_prepared_connector_write_target(
                bindings.as_ref(),
                crate::sql::planner::table::SqlTableIdentity {
                    catalog: target_catalog.clone(),
                    namespace: target_namespace.clone(),
                    table: target_name.clone(),
                },
                template.preparation().clone(),
                planning_lease.clone(),
            )?;
            let sink = sql_write_plan_input_for_admitted_target(
                bindings.as_ref(),
                target_binding,
                crate::sql::planner::distributed::write::contract::SqlWriteSinkMode::Data,
                crate::sql::planner::distributed::write::contract::ConnectorWriteInputBinding::RootOutputByOrdinal,
                None,
            )?;
            let first_refresh = SqlMvFirstRefreshPlanner::plan(SqlMvFirstRefreshPlannerInput {
                shape,
                target_contract,
                target_binding,
                root_distribution,
                artifact: SqlMvFirstRefreshArtifactInput::Sql(physical_sql),
            })?;
            if first_refresh.target_binding() != target_binding {
                return Err(
                    "MV first-refresh SQL plan target binding drifted during activation"
                        .to_string(),
                );
            }
            let root_distribution = first_refresh.root_distribution().clone();
            let SqlMvFirstRefreshArtifact::Sql(physical_sql) = first_refresh.into_artifact() else {
                return Err("MV first-refresh SQL activation expected a SQL artifact".to_string());
            };
            let query = parse_query_from_sql(physical_sql.sql())?;
            crate::engine::prepare_query_as_iceberg_write_with_connector_binding(
                state,
                current_catalog.as_deref(),
                &current_database,
                &query,
                sink,
                bindings,
                None,
                Some(root_distribution),
                execution,
                &connector_context,
                template,
            )?
        }
        MvFirstRefreshExecutionArtifact::Logical(logical) => {
            let facts = logical.into_context();
            let frozen_base_overlays = facts.frozen_base_overlays.clone().ok_or_else(|| {
                "MV first-refresh logical artifact is missing its admitted base bindings"
                    .to_string()
            })?;
            let refresh_rewrite = rebuild_frozen_mv_rewrite_context(
                state,
                current_catalog.as_deref(),
                &current_database,
                expected_target_snapshot_id,
                &target_catalog,
                &target_namespace,
                &target_name,
                &facts,
                planning_lease,
                &connector_context,
            )?;
            let bindings = Arc::new(QueryTableBindingStore::try_new()?);
            let _target_binding =
                crate::engine::mv::iceberg_refresh::bind_imv_target_query_table_in_store_from_rewrite(
                    &refresh_rewrite,
                    &bindings,
                    planning_lease,
                    &connector_context,
                )?;
            let write_target_binding = admit_prepared_connector_write_target(
                bindings.as_ref(),
                crate::sql::planner::table::SqlTableIdentity {
                    catalog: target_catalog.clone(),
                    namespace: target_namespace.clone(),
                    table: target_name.clone(),
                },
                template.preparation().clone(),
                planning_lease.clone(),
            )?;
            let sink = sql_write_plan_input_for_admitted_target(
                bindings.as_ref(),
                write_target_binding,
                crate::sql::planner::distributed::write::contract::SqlWriteSinkMode::Data,
                crate::sql::planner::distributed::write::contract::ConnectorWriteInputBinding::RootOutputByOrdinal,
                None,
            )?;
            let (plan, factory) =
                crate::engine::mv::iceberg_refresh::compile_canonical_select_for_imv_with_frozen_rewrite(
                    state,
                    &refresh_rewrite,
                    &connector_context,
                    Arc::clone(&bindings),
                    execution,
                    frozen_base_overlays,
                )
                .map_err(|error| error.message)?;
            let schema_contract = refresh_rewrite.schema_contract.as_ref();
            let (left_ref, right_ref) =
                crate::engine::mv::iceberg_refresh::join_base_refs_for_schema_contract(
                    schema_contract,
                    &refresh_rewrite.base_refs,
                )?;
            let append = crate::mv::refresh::join_first_refresh::build_join_first_refresh_append_logical_plan(
                &refresh_rewrite,
                left_ref,
                right_ref,
                crate::mv::refresh::join_first_refresh::JoinFirstRefreshLogicalInput {
                    plan,
                    factory,
                },
            )?;
            let first_refresh = SqlMvFirstRefreshPlanner::plan(SqlMvFirstRefreshPlannerInput {
                shape,
                target_contract,
                target_binding: write_target_binding,
                root_distribution,
                artifact: SqlMvFirstRefreshArtifactInput::Logical {
                    plan: append.plan,
                    factory: append.factory,
                    root_hash_column: root_hash_column.clone(),
                },
            })?;
            if first_refresh.target_binding() != write_target_binding {
                return Err(
                    "MV first-refresh SQL plan target binding drifted during activation"
                        .to_string(),
                );
            }
            let root_distribution = first_refresh.root_distribution().clone();
            let SqlMvFirstRefreshArtifact::Logical { plan, factory } =
                first_refresh.into_artifact()
            else {
                return Err(
                    "MV first-refresh join activation expected a logical SQL artifact".to_string(),
                );
            };
            crate::engine::prepare_logical_plan_as_iceberg_write_with_connector_binding(
                state,
                current_catalog.as_deref(),
                &current_database,
                plan,
                factory,
                sink,
                root_distribution,
                execution,
                &connector_context,
                bindings,
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

pub(crate) fn rebuild_frozen_mv_rewrite_context(
    state: &Arc<StandaloneState>,
    current_catalog: Option<&str>,
    current_database: &str,
    expected_target_snapshot_id: Option<i64>,
    target_catalog: &str,
    target_namespace: &str,
    target_name: &str,
    facts: &MvFirstRefreshLogicalContext,
    planning_lease: &ConnectorControlPlanningLease,
    connector_context: &novarocks_spi::connector::ConnectorRequestContext,
) -> Result<Arc<crate::mv::rewrite::context::IcebergMvRewriteContext>, String> {
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
    let target_binding = crate::mv::refresh::target_binding::load_mv_target_binding_with_lease(
        state,
        &target_identity,
        planning_lease.clone(),
        connector_context,
    )?;
    if target_binding.table_uuid() != facts.target_table_uuid {
        return Err(
            "MV refresh logical artifact target UUID drifted after preparation".to_string(),
        );
    }
    if target_binding.current_snapshot_id() != expected_target_snapshot_id {
        return Err(
            "MV refresh logical artifact target snapshot drifted after preparation".to_string(),
        );
    }
    let application_pin = crate::mv::refresh::pin::RefreshSnapshotPin::from_captured_entries(
        facts
            .base_refs
            .iter()
            .map(|base| {
                let snapshot_id = facts.pin.get(base).ok_or_else(|| {
                    format!(
                        "MV first-refresh logical artifact has no snapshot pin for {}",
                        base.fqn()
                    )
                })?;
                let table_uuid = facts.pin.uuid(base).ok_or_else(|| {
                    format!(
                        "MV first-refresh logical artifact has no UUID pin for {}",
                        base.fqn()
                    )
                })?;
                Ok((base.clone(), snapshot_id, table_uuid.to_string()))
            })
            .collect::<Result<Vec<_>, String>>()?,
    );
    let schema_contract = facts.mv_definition.schema_contract.clone().map(Arc::new);
    crate::mv::rewrite::context::IcebergMvRewriteContext::from_parts(
        target_identity,
        facts.mv_definition.mv_id,
        current_catalog.map(str::to_string),
        current_database.to_string(),
        Arc::new(facts.mv_definition.clone()),
        Arc::new(facts.canonical_select_query.clone()),
        Arc::from(facts.base_refs.clone()),
        Arc::new(application_pin),
        facts.previous_snapshot_ids.clone(),
        facts.previous_table_uuids.clone(),
        expected_target_snapshot_id,
        facts.target_table_uuid.clone(),
        target_binding.arrow_schema().clone(),
        Arc::from(target_binding.observation().field_ids().to_vec()),
        schema_contract,
    )
    .map(Arc::new)
}

/// Legacy physical refresh context retained only until the incremental
/// activation migration completes. New first-refresh activation uses
/// `rebuild_frozen_mv_rewrite_context` and never reconstructs a provider table.
pub(crate) fn rebuild_frozen_mv_refresh_context(
    state: &Arc<StandaloneState>,
    current_catalog: Option<&str>,
    current_database: &str,
    expected_target_snapshot_id: Option<i64>,
    target_catalog: &str,
    target_namespace: &str,
    target_name: &str,
    facts: &MvFirstRefreshLogicalContext,
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
    .into_table();
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
    let application_pin = crate::mv::refresh::pin::RefreshSnapshotPin::from_captured_entries(
        facts
            .base_refs
            .iter()
            .map(|base| {
                let snapshot_id = facts.pin.get(base).ok_or_else(|| {
                    format!(
                        "MV first-refresh logical artifact has no snapshot pin for {}",
                        base.fqn()
                    )
                })?;
                let table_uuid = facts.pin.uuid(base).ok_or_else(|| {
                    format!(
                        "MV first-refresh logical artifact has no UUID pin for {}",
                        base.fqn()
                    )
                })?;
                Ok((base.clone(), snapshot_id, table_uuid.to_string()))
            })
            .collect::<Result<Vec<_>, String>>()?,
    );
    crate::mv::refresh::execution_context::IcebergMvRefreshContext::new_with_validated_inputs_and_pruning_limits(
        target_identity,
        facts.mv_definition.mv_id,
        current_catalog,
        current_database,
        Arc::new(facts.mv_definition.clone()),
        Arc::new(facts.canonical_select_query.clone()),
        Arc::from(facts.base_refs.clone()),
        Arc::new(application_pin),
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
    facts: &MvFirstRefreshLogicalContext,
) -> Result<(), String> {
    if facts.base_refs.is_empty() || facts.pin.len() != facts.base_refs.len() {
        return Err(
            "MV first-refresh logical artifact has incomplete base snapshot pins".to_string(),
        );
    }
    // Production logical first-refresh artifacts retain the materializations
    // admitted during preparation.  Those overlays carry the exact lease,
    // table identity, and pinned input set that activation must use; asking
    // the catalog for the current base here would silently reintroduce a
    // latest-generation acquire.
    if facts.frozen_base_overlays.is_some() {
        return Ok(());
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
    planning_lease: ConnectorControlPlanningLease,
    execution: &QueryExecutionContext,
    connector_context: novarocks_spi::connector::ConnectorRequestContext,
    registration: ConnectorWriteExecutionRegistration,
    mv_refresh_ctx: Option<&crate::mv::refresh::execution_context::IcebergMvRefreshContext>,
) -> Result<(ConnectorWriteCompletion, ConnectorWriteStagingSummary), String> {
    if registration.session().operation_id() != prepared.operation_id()
        || registration.cohort_id() != prepared.primary_cohort()
    {
        return Err("MV first-refresh staging registration identity mismatch".to_string());
    }
    let root_hash_column = prepared.root_hash_column().to_string();
    let current_catalog = prepared.current_catalog().map(str::to_string);
    let current_database = prepared.current_database().to_string();
    let target_identity = crate::sql::planner::table::SqlTableIdentity {
        catalog: prepared.target_catalog().to_string(),
        namespace: prepared.target_namespace().to_string(),
        table: prepared.target_name().to_string(),
    };
    let preparation = registration
        .session()
        .preparation(registration.cohort_id())
        .map_err(|error| format!("read MV first-refresh write preparation: {error}"))?;
    match prepared.into_execution_artifact() {
        MvFirstRefreshExecutionArtifact::Sql(physical_sql) => {
            let query = parse_query_from_sql(physical_sql.sql())?;
            let root_distribution = iceberg_write_shuffle_by_output_name(root_hash_column);
            let bindings = Arc::new(QueryTableBindingStore::try_new()?);
            let target_binding = admit_prepared_connector_write_target(
                bindings.as_ref(),
                target_identity,
                preparation,
                planning_lease,
            )?;
            let sink = sql_write_plan_input_for_admitted_target(
                bindings.as_ref(),
                target_binding,
                crate::sql::planner::distributed::write::contract::SqlWriteSinkMode::Data,
                crate::sql::planner::distributed::write::contract::ConnectorWriteInputBinding::RootOutputByOrdinal,
                None,
            )?;
            execute_query_as_iceberg_staging_in_operation_with_connector_context(
                state,
                current_catalog.as_deref(),
                &current_database,
                &query,
                sink,
                bindings,
                None,
                root_distribution,
                Some(execution),
                &connector_context,
                registration,
            )
        }
        MvFirstRefreshExecutionArtifact::Logical(_) => Err(
            "legacy direct join first-refresh execution is unavailable; use the frontend retained-lease activation path"
                .to_string(),
        ),
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
