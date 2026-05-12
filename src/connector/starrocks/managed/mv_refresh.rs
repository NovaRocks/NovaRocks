use std::collections::BTreeMap;
use std::sync::{Arc, Mutex, MutexGuard, OnceLock};

use arrow::array::{Array, ArrayRef, BooleanArray, Int8Array};
use arrow::compute::filter_record_batch;
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;

use crate::connector::iceberg::catalog::load_table;
use crate::connector::starrocks::ObjectStoreProfile;
use crate::connector::starrocks::lake::context::remove_tablet_runtime;
use crate::connector::starrocks::managed::ivm_change_stream::plan_iceberg_change_batch_for_ivm;
use crate::connector::starrocks::managed::ivm_delta_source::{
    IvmDeltaSourceInput, build_delta_source_files, execute_delta_source_query,
    projection_select_with_change_op,
};
use crate::connector::starrocks::managed::mv_apply_policy::{
    MvApplyPolicy, apply_policy_for_change,
};
use crate::connector::starrocks::managed::mv_refresh_strategy::{
    FullRefreshReason, MvRefreshPolicy, choose_snapshot_refresh_policy, policy_from_change_error,
};
use crate::engine::mv_flow::{analyze_visible_output_types, execute_query_for_mv_refresh};
use crate::engine::{QueryResult, StandaloneState, StatementResult, record_batch_to_chunk};
use crate::exec::change_op::{CHANGE_OP_COLUMN, CHANGE_OP_DELETE, CHANGE_OP_INSERT};
use crate::exec::chunk::Chunk;
use crate::sql::parser::ast::{ObjectName, RefreshMaterializedViewStmt};

use crate::connector::starrocks::managed::catalog::{
    ManagedLakeCatalog, ManagedTableRuntime, register_managed_tables_in_catalog,
};
use crate::connector::starrocks::managed::config::ManagedLakeConfig;
use crate::connector::starrocks::managed::ddl::{
    bootstrap_empty_partition_for_tablets, build_create_tablet_request, request_schema_from_runtime,
};
use crate::connector::starrocks::managed::model::{
    IcebergTableRef, ManagedMvStorageEngine, ManagedPartitionState, ManagedTableKind,
};
use crate::connector::starrocks::managed::txn::{
    MvRefreshWriteMetadata, PartitionTarget, load_insert_plan, load_physical_insert_plan,
    write_chunks_into_managed_partition,
    write_chunks_into_managed_partition_for_aggregate_mv_upsert,
    write_chunks_into_managed_partition_for_mv_refresh_with_row_delta,
};
use crate::meta::repository::job::CreateEraseJobRequest;
use crate::meta::repository::managed_lake::{StageManagedMvRefreshRequest, StagedManagedMvRefresh};
use crate::meta::repository::mv::{StoredMvDefinition, UpdateManagedMvRefreshSummaryRequest};

pub(crate) fn refresh_mv(
    state: &Arc<StandaloneState>,
    current_catalog: Option<&str>,
    current_database: &str,
    stmt: &RefreshMaterializedViewStmt,
) -> Result<StatementResult, String> {
    let (db_name, mv_name) = resolve_mv_name(&stmt.name, current_database)?;
    let _refresh_guard = acquire_mv_refresh_lock()?;

    if current_catalog.is_some() {
        let target = crate::engine::mv::iceberg_refresh::resolve_refresh_target(
            current_catalog,
            current_database,
            &stmt.name,
        )?;
        if load_mv_definition_by_target(state, &target.catalog, &target.namespace, &target.table)?
            .as_ref()
            .is_some_and(|mv| {
                mv.storage_engine
                    .eq_ignore_ascii_case(ManagedMvStorageEngine::Iceberg.as_sql_str())
            })
        {
            drop(_refresh_guard);
            return crate::engine::mv::iceberg_refresh::refresh_iceberg_mv(
                state,
                current_catalog,
                current_database,
                stmt,
            );
        }
    }

    let runtime = {
        let managed = state
            .managed_lake
            .read()
            .expect("standalone managed lake read lock");
        managed.table(&db_name, &mv_name)?.clone()
    };
    if runtime.table.kind != ManagedTableKind::MaterializedView {
        return Err(format!("`{db_name}.{mv_name}` is not a materialized view"));
    }

    let mut mv_definition = load_mv_definition_by_id(state, runtime.table.table_id)?
        .ok_or_else(|| format!("materialized view {db_name}.{mv_name} has no MV definition"))?;
    if mv_definition.refresh_in_progress || mv_definition.active_refresh_id.is_some() {
        tracing::warn!(
            "materialized view {db_name}.{mv_name}: clearing stale refresh progress before retry; target_snapshots={:?}",
            mv_definition.refresh_target_snapshots
        );
        clear_mv_refresh_progress(state, runtime.table.table_id)?;
        mv_definition.refresh_in_progress = false;
        mv_definition.active_refresh_id = None;
        mv_definition.refresh_target_snapshots.clear();
    }

    let mv_shape = validate_incremental_mv_select(&mv_definition.select_sql)?;
    let base_refs = parse_iceberg_table_refs(&mv_definition.base_table_refs)?;
    let [base_ref] = base_refs.as_slice() else {
        return Err(
            "incremental materialized view refresh requires a single Iceberg base table"
                .to_string(),
        );
    };
    validate_incremental_mv_base_ref(mv_shape.base_table(), base_ref)?;

    let loaded = load_current_iceberg_base_table(state, base_ref)?;
    let current_snapshot_id = loaded
        .table
        .metadata()
        .current_snapshot()
        .map(|snapshot| snapshot.snapshot_id());
    let current_table_uuid = loaded.table.metadata().uuid().to_string();
    let previous_snapshot_id = mv_definition
        .last_refresh_snapshots
        .get(&base_ref.fqn())
        .copied();
    let mut policy = choose_snapshot_refresh_policy(previous_snapshot_id, current_snapshot_id)?;
    if let Some(previous_uuid) = mv_definition.last_refresh_table_uuids.get(&base_ref.fqn())
        && previous_uuid != &current_table_uuid
    {
        policy = MvRefreshPolicy::FullRefresh {
            target_snapshot_id: current_snapshot_id,
            reason: FullRefreshReason::BaseTableRecreated {
                previous_uuid: previous_uuid.clone(),
                current_uuid: current_table_uuid.clone(),
            },
        };
    }
    tracing::info!(
        target: "mv_refresh",
        mv = %format!("{}.{}", db_name, mv_name),
        base = %base_ref.fqn(),
        previous_snapshot_id = ?previous_snapshot_id,
        current_snapshot_id = ?current_snapshot_id,
        policy = ?policy,
        "selected materialized view refresh policy"
    );
    if matches!(
        policy,
        MvRefreshPolicy::FullRefresh { .. } | MvRefreshPolicy::Incremental { .. }
    ) {
        let target_snapshots = current_snapshot_id
            .map(|snapshot_id| single_snapshot_map(base_ref, snapshot_id))
            .unwrap_or_default();
        begin_mv_refresh_intent(state, runtime.table.table_id, target_snapshots)?;
    }

    let projection_apply_shape = mv_shape.clone();
    let projection_full_primary_key_columns = mv_definition.primary_key_columns.clone();
    dispatch_mv_refresh_strategy(
        &mv_shape,
        policy,
        || {
            refresh_mv_full_with_executor(state, &db_name, &mv_name, move |ctx| {
                run_projection_mv_select_and_chunks(ctx, &projection_full_primary_key_columns)
            })
        },
        |shape| refresh_aggregate_mv_full(state, &db_name, &mv_name, shape),
        |current_snapshot_id| {
            let snapshots = single_snapshot_map(base_ref, current_snapshot_id);
            let table_uuids = single_table_uuid_map(base_ref, &current_table_uuid);
            update_managed_mv_refresh_summary(
                state,
                runtime.table.table_id,
                mv_definition.last_refresh_rows.unwrap_or(0),
                snapshots,
                table_uuids,
            )?;
            refresh_managed_catalog(state)?;
            Ok(StatementResult::Ok)
        },
        |previous_snapshot_id, current_snapshot_id| {
            let batch = match plan_iceberg_change_batch_for_ivm(
                &loaded.table,
                previous_snapshot_id,
                current_snapshot_id,
                &mv_definition.primary_key_columns,
            ) {
                Ok(batch) => batch,
                Err(err) => match policy_from_change_error(err) {
                    MvRefreshPolicy::FullRefresh { reason, .. } => {
                        tracing::info!(
                            target: "mv_refresh",
                            mv = %format!("{}.{}", db_name, mv_name),
                            base = %base_ref.fqn(),
                            snapshot_from = previous_snapshot_id,
                            snapshot_to = current_snapshot_id,
                            reason = %reason,
                            "mv_refresh fall-back to Full from projection incremental planner"
                        );
                        let primary_key_columns = mv_definition.primary_key_columns.clone();
                        return refresh_mv_full_with_executor(
                            state,
                            &db_name,
                            &mv_name,
                            move |ctx| {
                                run_projection_mv_select_and_chunks(ctx, &primary_key_columns)
                            },
                        );
                    }
                    MvRefreshPolicy::Unsupported { reason } => {
                        return Err(format!(
                            "iceberg materialized view refresh unsupported: {reason}"
                        ));
                    }
                    other => {
                        return Err(format!(
                            "iceberg materialized view refresh produced invalid policy from change planner: {other:?}"
                        ));
                    }
                },
            };
            let has_inserts = !batch.inserts.is_empty();
            let has_deletes = change_batch_has_deletes(&batch);
            let apply_policy = apply_policy_for_change(
                &projection_apply_shape,
                has_inserts,
                has_deletes,
                !mv_definition.primary_key_columns.is_empty(),
            );
            match apply_policy {
                MvApplyPolicy::Incremental => {}
                MvApplyPolicy::FullRefresh { reason } => {
                    tracing::info!(
                        target: "mv_refresh",
                        mv = %format!("{}.{}", db_name, mv_name),
                        base = %base_ref.fqn(),
                        snapshot_from = previous_snapshot_id,
                        snapshot_to = current_snapshot_id,
                        reason = %reason,
                        "mv_refresh fall-back to Full from projection apply policy"
                    );
                    let primary_key_columns = mv_definition.primary_key_columns.clone();
                    return refresh_mv_full_with_executor(state, &db_name, &mv_name, move |ctx| {
                        run_projection_mv_select_and_chunks(ctx, &primary_key_columns)
                    });
                }
            }

            let source_files = build_delta_source_files(
                IvmDeltaSourceInput {
                    state,
                    current_database: &db_name,
                    base_ref,
                    loaded: &loaded,
                },
                batch,
            )?;
            if source_files.previous_snapshot_id != previous_snapshot_id
                || source_files.current_snapshot_id != current_snapshot_id
            {
                return Err(format!(
                    "projection/filter MV incremental refresh delta source snapshot window mismatch: expected {} -> {}, got {} -> {}",
                    previous_snapshot_id,
                    current_snapshot_id,
                    source_files.previous_snapshot_id,
                    source_files.current_snapshot_id
                ));
            }
            if source_files.files.is_empty() {
                advance_mv_refresh_metadata_without_writes(
                    state,
                    runtime.table.table_id,
                    base_ref,
                    current_snapshot_id,
                    &current_table_uuid,
                    mv_definition.last_refresh_rows.unwrap_or(0),
                )?;
                refresh_managed_catalog(state)?;
                return Ok(StatementResult::Ok);
            }
            let physical_select_sql = projection_mv_physical_select_sql(
                &mv_definition.select_sql,
                &mv_definition.primary_key_columns,
            )?;
            let tagged_select_sql = projection_select_with_change_op(&physical_select_sql)?;
            let delta_result = execute_delta_source_query(
                IvmDeltaSourceInput {
                    state,
                    current_database: &db_name,
                    base_ref,
                    loaded: &loaded,
                },
                &tagged_select_sql,
                source_files,
            )?;
            let (chunks, row_delta) = tagged_projection_change_chunks(delta_result)?;
            let resolved_mv = crate::engine::ResolvedLocalTableName {
                database: db_name.clone(),
                table: mv_name.clone(),
            };
            let plan = if mv_definition.primary_key_columns.is_empty() {
                load_insert_plan(state, &resolved_mv, PartitionTarget::Active)
            } else {
                load_physical_insert_plan(state, &resolved_mv, PartitionTarget::Active)
            }?;
            let previous_rows = mv_definition.last_refresh_rows.unwrap_or(0);
            let snapshots = single_snapshot_map(base_ref, current_snapshot_id);
            let table_uuids = single_table_uuid_map(base_ref, &current_table_uuid);
            write_chunks_into_managed_partition_for_mv_refresh_with_row_delta(
                state,
                plan,
                &chunks,
                MvRefreshWriteMetadata {
                    table_id: runtime.table.table_id,
                    previous_refresh_rows: previous_rows,
                    snapshots,
                    table_uuids,
                },
                row_delta,
            )?;
            refresh_managed_catalog(state)?;
            Ok(StatementResult::Ok)
        },
        |shape, previous_snapshot_id, current_snapshot_id| {
            let change_batch = match plan_iceberg_change_batch_for_ivm(
                &loaded.table,
                previous_snapshot_id,
                current_snapshot_id,
                &mv_definition.primary_key_columns,
            ) {
                Ok(batch) => batch,
                Err(err) => match policy_from_change_error(err) {
                    MvRefreshPolicy::FullRefresh { reason, .. } => {
                        tracing::info!(
                            target: "mv_refresh",
                            mv = %format!("{}.{}", db_name, mv_name),
                            base = %base_ref.fqn(),
                            snapshot_from = previous_snapshot_id,
                            snapshot_to = current_snapshot_id,
                            reason = %reason,
                            "mv_refresh fall-back to Full from aggregate incremental planner"
                        );
                        return refresh_aggregate_mv_full(state, &db_name, &mv_name, shape);
                    }
                    MvRefreshPolicy::Unsupported { reason } => {
                        return Err(format!(
                            "iceberg materialized view refresh unsupported: {reason}"
                        ));
                    }
                    other => {
                        return Err(format!(
                            "iceberg materialized view refresh produced invalid policy from change planner: {other:?}"
                        ));
                    }
                },
            };
            refresh_aggregate_mv_incremental(AggregateMvIncrementalRefreshContext {
                state,
                database: &db_name,
                mv_name: &mv_name,
                table_id: runtime.table.table_id,
                select_sql: &mv_definition.select_sql,
                base_ref,
                shape,
                change_batch,
                previous_refresh_rows: mv_definition.last_refresh_rows.unwrap_or(0),
                previous_snapshot_id,
                current_snapshot_id,
                current_table_uuid: current_table_uuid.clone(),
                loaded: &loaded,
            })
        },
    )
}

fn dispatch_mv_refresh_strategy<
    ProjectionFull,
    AggregateFull,
    NoOp,
    ProjectionIncremental,
    AggregateIncremental,
>(
    mv_shape: &super::mv_shape::IncrementalMvShape,
    strategy: MvRefreshPolicy,
    projection_full: ProjectionFull,
    aggregate_full: AggregateFull,
    no_op: NoOp,
    projection_incremental: ProjectionIncremental,
    aggregate_incremental: AggregateIncremental,
) -> Result<StatementResult, String>
where
    ProjectionFull: FnOnce() -> Result<StatementResult, String>,
    AggregateFull: FnOnce(&super::mv_shape::AggregateMvShape) -> Result<StatementResult, String>,
    NoOp: FnOnce(i64) -> Result<StatementResult, String>,
    ProjectionIncremental: FnOnce(i64, i64) -> Result<StatementResult, String>,
    AggregateIncremental:
        FnOnce(&super::mv_shape::AggregateMvShape, i64, i64) -> Result<StatementResult, String>,
{
    match (mv_shape, strategy) {
        (
            super::mv_shape::IncrementalMvShape::ProjectionFilter(_),
            MvRefreshPolicy::FullRefresh { .. },
        ) => projection_full(),
        (
            super::mv_shape::IncrementalMvShape::Aggregate(shape),
            MvRefreshPolicy::FullRefresh { .. },
        ) => aggregate_full(shape),
        (
            _,
            MvRefreshPolicy::NoOp {
                current_snapshot_id,
            },
        ) => no_op(current_snapshot_id),
        (
            super::mv_shape::IncrementalMvShape::ProjectionFilter(_),
            MvRefreshPolicy::Incremental {
                previous_snapshot_id,
                current_snapshot_id,
            },
        ) => projection_incremental(previous_snapshot_id, current_snapshot_id),
        (
            super::mv_shape::IncrementalMvShape::Aggregate(shape),
            MvRefreshPolicy::Incremental {
                previous_snapshot_id,
                current_snapshot_id,
            },
        ) => aggregate_incremental(shape, previous_snapshot_id, current_snapshot_id),
        (_, MvRefreshPolicy::Unsupported { reason }) => Err(format!(
            "iceberg materialized view refresh unsupported: {reason}"
        )),
    }
}

fn refresh_aggregate_mv_full(
    state: &Arc<StandaloneState>,
    database: &str,
    mv_name: &str,
    shape: &super::mv_shape::AggregateMvShape,
) -> Result<StatementResult, String> {
    let shape = shape.clone();
    refresh_mv_full_with_executor(state, database, mv_name, move |ctx| {
        // Step 1: obtain visible-shaped output types by analyzing the ORIGINAL select_sql
        // without executing it. `build_aggregate_mv_layout` expects visible-shaped types
        // (one column per visible_output), not state-shaped types (which expand AVG into
        // two columns: SUM + COUNT). Running the analyzer is cheap — no execution occurs.
        let visible_output_columns =
            analyze_visible_output_types(&ctx.state, &ctx.database, &ctx.select_sql)?;

        // Step 2: build the layout from visible types.
        let layout =
            super::mv_agg_state::build_aggregate_mv_layout(&shape, &visible_output_columns)?;

        // Step 3: rewrite the SELECT to emit state columns (AVG → SUM + COUNT) and execute
        // it to obtain the actual state-shaped data.
        let state_sql = super::mv_shape::rewrite_select_sql_for_state(&ctx.select_sql, &shape)?;
        let result = execute_query_for_mv_refresh(&ctx.state, &ctx.database, &state_sql)?;

        // Step 4: materialize state-shaped executor result using the visible-type layout.
        super::mv_agg_state::materialize_aggregate_result_chunks(result, &layout, &shape)
    })
}

fn change_batch_has_deletes(
    batch: &crate::connector::iceberg::changes::IcebergChangeBatch,
) -> bool {
    !batch.deletes.is_empty()
        || !batch.equality_deletes.is_empty()
        || !batch.deleted_data_files.is_empty()
}

struct AggregateMvIncrementalRefreshContext<'a> {
    state: &'a Arc<StandaloneState>,
    database: &'a str,
    mv_name: &'a str,
    table_id: i64,
    select_sql: &'a str,
    base_ref: &'a IcebergTableRef,
    shape: &'a super::mv_shape::AggregateMvShape,
    change_batch: crate::connector::iceberg::changes::IcebergChangeBatch,
    previous_refresh_rows: i64,
    previous_snapshot_id: i64,
    current_snapshot_id: i64,
    current_table_uuid: String,
    loaded: &'a crate::connector::iceberg::catalog::IcebergLoadedTable,
}

fn refresh_aggregate_mv_incremental(
    ctx: AggregateMvIncrementalRefreshContext<'_>,
) -> Result<StatementResult, String> {
    let has_inserts = !ctx.change_batch.inserts.is_empty();
    let has_deletes = change_batch_has_deletes(&ctx.change_batch);
    let apply_policy = apply_policy_for_change(
        &super::mv_shape::IncrementalMvShape::Aggregate(ctx.shape.clone()),
        has_inserts,
        has_deletes,
        false,
    );
    match apply_policy {
        MvApplyPolicy::Incremental => {}
        MvApplyPolicy::FullRefresh { reason } => {
            tracing::info!(
                target: "mv_refresh",
                mv = %format!("{}.{}", ctx.database, ctx.mv_name),
                base = %ctx.base_ref.fqn(),
                snapshot_from = ctx.previous_snapshot_id,
                snapshot_to = ctx.current_snapshot_id,
                has_deletes,
                reason = %reason,
                "mv_refresh fall-back to Full from apply policy"
            );
            return refresh_aggregate_mv_full(ctx.state, ctx.database, ctx.mv_name, ctx.shape);
        }
    }

    let source_files = build_delta_source_files(
        IvmDeltaSourceInput {
            state: ctx.state,
            current_database: ctx.database,
            base_ref: ctx.base_ref,
            loaded: ctx.loaded,
        },
        ctx.change_batch,
    )?;

    if source_files.previous_snapshot_id != ctx.previous_snapshot_id
        || source_files.current_snapshot_id != ctx.current_snapshot_id
    {
        return Err(format!(
            "aggregate MV incremental refresh delta source snapshot window mismatch: expected {} -> {}, got {} -> {}",
            ctx.previous_snapshot_id,
            ctx.current_snapshot_id,
            source_files.previous_snapshot_id,
            source_files.current_snapshot_id
        ));
    }

    // Empty-input early return: nothing to merge, just advance lineage.
    if source_files.files.is_empty() {
        advance_mv_refresh_metadata_without_writes(
            ctx.state,
            ctx.table_id,
            ctx.base_ref,
            ctx.current_snapshot_id,
            &ctx.current_table_uuid,
            ctx.previous_refresh_rows,
        )?;
        refresh_managed_catalog(ctx.state)?;
        return Ok(StatementResult::Ok);
    }

    // The rewritten state SQL (AVG -> SUM + COUNT) produces state-shaped columns whose
    // count does not match shape.visible_outputs. Sourcing types from the analyzer
    // avoids this mismatch before materializing state chunks.
    let visible_output_columns =
        analyze_visible_output_types(ctx.state, ctx.database, ctx.select_sql)?;
    let layout =
        super::mv_agg_state::build_aggregate_mv_layout(ctx.shape, &visible_output_columns)?;

    let signed_state_sql =
        match super::ivm_delta_aggregate::rewrite_select_sql_for_signed_delta_state(
            ctx.select_sql,
            ctx.shape,
        ) {
            Ok(sql) => sql,
            Err(err)
                if err.contains("MIN/MAX") && err.contains("delete-bearing signed delta state") =>
            {
                tracing::info!(
                    target: "mv_refresh",
                    mv = %format!("{}.{}", ctx.database, ctx.mv_name),
                    base = %ctx.base_ref.fqn(),
                    snapshot_from = ctx.previous_snapshot_id,
                    snapshot_to = ctx.current_snapshot_id,
                    error = %err,
                    "mv_refresh fall-back to Full from signed delta aggregate rewrite"
                );
                return refresh_aggregate_mv_full(ctx.state, ctx.database, ctx.mv_name, ctx.shape);
            }
            Err(err) => return Err(err),
        };
    let delta_result = execute_delta_source_query(
        IvmDeltaSourceInput {
            state: ctx.state,
            current_database: ctx.database,
            base_ref: ctx.base_ref,
            loaded: ctx.loaded,
        },
        &signed_state_sql,
        source_files,
    )?;
    let delta_chunks =
        super::mv_agg_state::materialize_aggregate_result_chunks(delta_result, &layout, ctx.shape)?;

    let plan = load_physical_insert_plan(
        ctx.state,
        &crate::engine::ResolvedLocalTableName {
            database: ctx.database.to_string(),
            table: ctx.mv_name.to_string(),
        },
        PartitionTarget::Active,
    )?;
    let snapshots = single_snapshot_map(ctx.base_ref, ctx.current_snapshot_id);
    let table_uuids = single_table_uuid_map(ctx.base_ref, &ctx.current_table_uuid);
    write_chunks_into_managed_partition_for_aggregate_mv_upsert(
        ctx.state,
        plan,
        &delta_chunks,
        &layout,
        MvRefreshWriteMetadata {
            table_id: ctx.table_id,
            previous_refresh_rows: ctx.previous_refresh_rows,
            snapshots,
            table_uuids,
        },
    )?;
    refresh_managed_catalog(ctx.state)?;
    Ok(StatementResult::Ok)
}

fn advance_mv_refresh_metadata_without_writes(
    state: &Arc<StandaloneState>,
    table_id: i64,
    base_ref: &IcebergTableRef,
    current_snapshot_id: i64,
    current_table_uuid: &str,
    last_refresh_rows: i64,
) -> Result<(), String> {
    update_managed_mv_refresh_summary(
        state,
        table_id,
        last_refresh_rows,
        single_snapshot_map(base_ref, current_snapshot_id),
        single_table_uuid_map(base_ref, current_table_uuid),
    )
}

pub(crate) fn refresh_mv_full_with_executor<F>(
    state: &Arc<StandaloneState>,
    database: &str,
    mv_name: &str,
    executor: F,
) -> Result<StatementResult, String>
where
    F: FnOnce(MvRefreshContext) -> Result<Vec<Chunk>, String>,
{
    let managed_config = state
        .managed_lake_config
        .clone()
        .ok_or_else(|| "standalone managed lake config is missing".to_string())?;

    let runtime = {
        let managed = state
            .managed_lake
            .read()
            .expect("standalone managed lake read lock");
        managed.table(database, mv_name)?.clone()
    };
    if runtime.table.kind != ManagedTableKind::MaterializedView {
        return Err(format!("`{database}.{mv_name}` is not a materialized view"));
    }

    let mv_definition = load_mv_definition_by_id(state, runtime.table.table_id)?
        .ok_or_else(|| format!("materialized view {database}.{mv_name} has no MV definition"))?;
    let active_partition = runtime
        .partitions
        .iter()
        .find(|partition| partition.state == ManagedPartitionState::Active)
        .cloned()
        .ok_or_else(|| format!("materialized view {database}.{mv_name} has no active partition"))?;
    let retired_root_path = managed_config.tablet_root_path(
        runtime.table.db_id,
        runtime.table.table_id,
        active_partition.partition_id,
    );

    let staged = stage_managed_mv_refresh_partition(
        state,
        &runtime,
        &active_partition.name,
        &managed_config.warehouse_uri,
    )?;

    if let Err(err) = refresh_managed_catalog(state) {
        cleanup_staged_partition(state, runtime.table.table_id, &staged, false)?;
        return Err(format!("mv refresh catalog refresh failed: {err}"));
    }

    if let Err(err) = bootstrap_mv_refresh_partition_for_tablets(
        &runtime,
        &managed_config,
        staged.partition_id,
        &staged.tablet_ids,
    ) {
        cleanup_staged_partition(state, runtime.table.table_id, &staged, false)?;
        return Err(format!("mv refresh bootstrap failed: {err}"));
    }

    let chunks = match executor(MvRefreshContext {
        state: Arc::clone(state),
        database: database.to_string(),
        select_sql: mv_definition.select_sql.clone(),
    }) {
        Ok(chunks) => chunks,
        Err(err) => {
            cleanup_staged_partition(state, runtime.table.table_id, &staged, true)?;
            return Err(format!("mv refresh execute failed: {err}"));
        }
    };

    let plan = match load_physical_insert_plan(
        state,
        &crate::engine::ResolvedLocalTableName {
            database: database.to_string(),
            table: mv_name.to_string(),
        },
        PartitionTarget::Staged {
            partition_id: staged.partition_id,
            index_id: staged.index_id,
            tablet_ids: staged.tablet_ids.clone(),
        },
    ) {
        Ok(plan) => plan,
        Err(err) => {
            cleanup_staged_partition(state, runtime.table.table_id, &staged, true)?;
            return Err(format!("mv refresh plan load failed: {err}"));
        }
    };

    let rows_written = match write_chunks_into_managed_partition(state, plan, &chunks) {
        Ok(rows_written) => rows_written,
        Err(err) => {
            cleanup_staged_partition(state, runtime.table.table_id, &staged, true)?;
            return Err(format!("mv refresh write failed: {err}"));
        }
    };

    let base_refs = parse_iceberg_table_refs(&mv_definition.base_table_refs)?;
    let base_metadata = collect_current_base_metadata_or_cleanup_staged_partition(
        state,
        runtime.table.table_id,
        &staged,
        &base_refs,
    )?;
    if let Err(err) = activate_managed_mv_refresh_partition(
        state,
        runtime.table.table_id,
        active_partition.partition_id,
        &retired_root_path,
        &staged,
        rows_written,
        base_metadata,
    ) {
        cleanup_staged_partition(state, runtime.table.table_id, &staged, true)?;
        return Err(format!("mv refresh activate failed: {err}"));
    }

    refresh_managed_catalog(state)?;
    Ok(StatementResult::Ok)
}

#[derive(Clone)]
pub(crate) struct MvRefreshContext {
    pub(crate) state: Arc<StandaloneState>,
    pub(crate) database: String,
    pub(crate) select_sql: String,
}

fn run_mv_select_and_chunks(ctx: MvRefreshContext) -> Result<Vec<Chunk>, String> {
    let result: QueryResult =
        execute_query_for_mv_refresh(&ctx.state, &ctx.database, &ctx.select_sql)?;
    query_result_to_chunks(result)
}

fn run_projection_mv_select_and_chunks(
    ctx: MvRefreshContext,
    primary_key_columns: &[String],
) -> Result<Vec<Chunk>, String> {
    let select_sql = projection_mv_physical_select_sql(&ctx.select_sql, primary_key_columns)?;
    let result: QueryResult = execute_query_for_mv_refresh(&ctx.state, &ctx.database, &select_sql)?;
    query_result_to_chunks(result)
}

fn projection_mv_physical_select_sql(
    select_sql: &str,
    primary_key_columns: &[String],
) -> Result<String, String> {
    if primary_key_columns.is_empty() {
        return Ok(select_sql.to_string());
    }

    let normalized = crate::sql::parser::dialect::normalize_for_raw_parse(select_sql)
        .map_err(|e| format!("projection MV physical SELECT normalize error: {e}"))?;
    let mut stmt = crate::sql::parser::parse_normalized_sql_raw(&normalized)
        .map_err(|e| format!("projection MV physical SELECT parse error: {e}"))?;
    let sqlparser::ast::Statement::Query(query) = &mut stmt else {
        return Err("projection MV physical SELECT expects a SELECT query".to_string());
    };
    let sqlparser::ast::SetExpr::Select(select) = query.body.as_mut() else {
        return Err("projection MV physical SELECT expects a SELECT body".to_string());
    };

    let mut projection = Vec::with_capacity(
        primary_key_columns
            .len()
            .saturating_add(select.projection.len()),
    );
    for key in primary_key_columns {
        projection.push(hidden_primary_key_select_item(key)?);
    }
    projection.extend(std::mem::take(&mut select.projection));
    select.projection = projection;
    Ok(stmt.to_string())
}

fn hidden_primary_key_select_item(key: &str) -> Result<sqlparser::ast::SelectItem, String> {
    use sqlparser::ast::{Expr, Ident, SelectItem};
    let hidden_name = super::mv_ddl::projection_mv_hidden_pk_column_name(key)?;
    Ok(SelectItem::ExprWithAlias {
        expr: Expr::Identifier(Ident::new(key)),
        alias: Ident::new(hidden_name),
    })
}

/// Run the MV SELECT against the base table and return the resulting chunks.
/// Wrapper around `run_mv_select_and_chunks` for use by the iceberg refresh path.
pub(crate) fn run_mv_full_select_chunks(
    state: &Arc<StandaloneState>,
    database: &str,
    select_sql: &str,
) -> Result<Vec<Chunk>, String> {
    run_mv_select_and_chunks(MvRefreshContext {
        state: Arc::clone(state),
        database: database.to_string(),
        select_sql: select_sql.to_string(),
    })
}

pub(crate) fn query_result_to_chunks(result: QueryResult) -> Result<Vec<Chunk>, String> {
    result
        .chunks
        .into_iter()
        .map(|chunk| record_batch_to_chunk(chunk.batch))
        .collect()
}

const MV_OP_UPSERT: i8 = 0;
const MV_OP_DELETE: i8 = 1;
const MV_OP_COLUMN: &str = "__op";

fn tagged_projection_change_chunks(result: QueryResult) -> Result<(Vec<Chunk>, i64), String> {
    let mut delete_chunks = Vec::new();
    let mut insert_chunks = Vec::new();
    let mut delete_rows = 0_i64;
    let mut insert_rows = 0_i64;

    for chunk in result.chunks {
        let batch = chunk.batch;
        let change_op_index = find_change_op_column(&batch)?;
        let change_ops = batch
            .column(change_op_index)
            .as_any()
            .downcast_ref::<Int8Array>()
            .ok_or_else(|| {
                format!(
                    "projection/filter MV delta source column `{CHANGE_OP_COLUMN}` must be Int8"
                )
            })?;

        let mut delete_mask = Vec::with_capacity(batch.num_rows());
        let mut insert_mask = Vec::with_capacity(batch.num_rows());
        for row in 0..batch.num_rows() {
            if change_ops.is_null(row) {
                return Err(format!(
                    "projection/filter MV delta source column `{CHANGE_OP_COLUMN}` contains NULL"
                ));
            }
            match change_ops.value(row) {
                CHANGE_OP_DELETE => {
                    delete_mask.push(true);
                    insert_mask.push(false);
                }
                CHANGE_OP_INSERT => {
                    delete_mask.push(false);
                    insert_mask.push(true);
                }
                op => {
                    return Err(format!(
                        "projection/filter MV delta source column `{CHANGE_OP_COLUMN}` contains invalid value {op}; expected {CHANGE_OP_INSERT} or {CHANGE_OP_DELETE}"
                    ));
                }
            }
        }

        let delete_count = delete_mask.iter().filter(|keep| **keep).count();
        if delete_count > 0 {
            delete_rows = add_row_count(delete_rows, delete_count)?;
            let filtered = filter_record_batch(&batch, &BooleanArray::from(delete_mask))
                .map_err(|e| format!("filter projection MV deletes failed: {e}"))?;
            let without_change_op = record_batch_without_column(filtered, change_op_index)?;
            delete_chunks.push(record_batch_to_chunk(append_mv_op_column(
                without_change_op,
                MV_OP_DELETE,
            )?)?);
        }

        let insert_count = insert_mask.iter().filter(|keep| **keep).count();
        if insert_count > 0 {
            insert_rows = add_row_count(insert_rows, insert_count)?;
            let filtered = filter_record_batch(&batch, &BooleanArray::from(insert_mask))
                .map_err(|e| format!("filter projection MV upserts failed: {e}"))?;
            let without_change_op = record_batch_without_column(filtered, change_op_index)?;
            insert_chunks.push(record_batch_to_chunk(append_mv_op_column(
                without_change_op,
                MV_OP_UPSERT,
            )?)?);
        }
    }

    let mut chunks = Vec::with_capacity(delete_chunks.len() + insert_chunks.len());
    chunks.extend(delete_chunks);
    chunks.extend(insert_chunks);
    let row_delta = insert_rows.checked_sub(delete_rows).ok_or_else(|| {
        format!(
            "projection/filter MV row-count delta overflow: inserts={insert_rows} deletes={delete_rows}"
        )
    })?;
    Ok((chunks, row_delta))
}

fn find_change_op_column(batch: &RecordBatch) -> Result<usize, String> {
    let mut found = None;
    for (index, field) in batch.schema().fields().iter().enumerate() {
        if field.name().eq_ignore_ascii_case(CHANGE_OP_COLUMN) {
            if found.is_some() {
                return Err(format!(
                    "projection/filter MV delta source contains duplicate `{CHANGE_OP_COLUMN}` columns"
                ));
            }
            if field.data_type() != &DataType::Int8 {
                return Err(format!(
                    "projection/filter MV delta source column `{CHANGE_OP_COLUMN}` must be Int8, got {:?}",
                    field.data_type()
                ));
            }
            found = Some(index);
        }
    }
    found.ok_or_else(|| {
        format!("projection/filter MV delta source must include `{CHANGE_OP_COLUMN}` column")
    })
}

fn record_batch_without_column(
    batch: RecordBatch,
    column_index: usize,
) -> Result<RecordBatch, String> {
    let fields = batch
        .schema()
        .fields()
        .iter()
        .enumerate()
        .filter_map(|(index, field)| (index != column_index).then(|| field.as_ref().clone()))
        .collect::<Vec<_>>();
    let columns = batch
        .columns()
        .iter()
        .enumerate()
        .filter_map(|(index, column)| (index != column_index).then(|| Arc::clone(column)))
        .collect::<Vec<_>>();
    RecordBatch::try_new(Arc::new(Schema::new(fields)), columns)
        .map_err(|e| format!("remove projection MV change-op column failed: {e}"))
}

fn add_row_count(acc: i64, rows: usize) -> Result<i64, String> {
    let rows = i64::try_from(rows)
        .map_err(|_| "materialized view refresh row count overflow".to_string())?;
    acc.checked_add(rows)
        .ok_or_else(|| "materialized view refresh row count overflow".to_string())
}

fn append_mv_op_column(batch: RecordBatch, op: i8) -> Result<RecordBatch, String> {
    let row_count = batch.num_rows();
    let mut fields = batch
        .schema()
        .fields()
        .iter()
        .map(|field| field.as_ref().clone())
        .collect::<Vec<_>>();
    if fields
        .iter()
        .any(|field| field.name().eq_ignore_ascii_case(MV_OP_COLUMN))
    {
        return Err(format!(
            "materialized view incremental write result contains reserved column `{MV_OP_COLUMN}`"
        ));
    }
    fields.push(Field::new(MV_OP_COLUMN, DataType::Int8, false));
    let mut columns = batch.columns().to_vec();
    columns.push(Arc::new(Int8Array::from(vec![op; row_count])) as ArrayRef);
    RecordBatch::try_new(Arc::new(Schema::new(fields)), columns)
        .map_err(|e| format!("append MV op column failed: {e}"))
}

#[cfg(test)]
fn query_result_column_to_output_column(
    column: &crate::engine::QueryResultColumn,
) -> Result<crate::sql::analysis::OutputColumn, String> {
    Ok(crate::sql::analysis::OutputColumn {
        name: column.name.clone(),
        data_type: column.data_type.clone(),
        nullable: column.nullable,
    })
}

fn bootstrap_mv_refresh_partition_for_tablets(
    runtime: &ManagedTableRuntime,
    managed_config: &ManagedLakeConfig,
    partition_id: i64,
    tablet_ids: &[i64],
) -> Result<(), String> {
    if runtime.columns.iter().all(|column| column.visible) {
        return bootstrap_empty_partition_for_tablets(
            runtime,
            managed_config,
            partition_id,
            tablet_ids,
        );
    }

    let request_schema = request_schema_from_runtime(runtime)?;
    let object_store_profile = ObjectStoreProfile::from_s3_store_config(&managed_config.s3)?;
    let tablet_root_path =
        managed_config.tablet_root_path(runtime.table.db_id, runtime.table.table_id, partition_id);
    for tablet_id in tablet_ids {
        let request = build_create_tablet_request(
            *tablet_id,
            runtime.table.table_id,
            partition_id,
            request_schema.clone(),
        );
        crate::connector::starrocks::lake::schema::create_lake_tablet_from_req_with_schema_patch(
            &request,
            &tablet_root_path,
            Some(managed_config.s3.clone()),
            |schema| {
                *schema = runtime.tablet_schema.clone();
                Ok(())
            },
        )?;
        let loaded = crate::formats::starrocks::metadata::load_tablet_snapshot(
            *tablet_id,
            1,
            &tablet_root_path,
            Some(&object_store_profile),
        )?;
        if loaded.tablet_schema != runtime.tablet_schema {
            return Err(format!(
                "managed bootstrap schema mismatch after bootstrap: tablet_id={tablet_id}"
            ));
        }
    }
    Ok(())
}

fn validate_incremental_mv_select(
    select_sql: &str,
) -> Result<super::mv_shape::IncrementalMvShape, String> {
    let normalized = crate::sql::parser::dialect::normalize_for_raw_parse(select_sql)?;
    let statement = crate::sql::parser::parse_normalized_sql_raw(&normalized)
        .map_err(|e| format!("sql parser error: {e}"))?;
    let sqlparser::ast::Statement::Query(query) = statement else {
        return Err("REFRESH MATERIALIZED VIEW stored SQL must be a SELECT query".to_string());
    };
    super::mv_shape::classify_incremental_mv_query(&query)
}

fn validate_incremental_mv_base_ref(
    base_table: &sqlparser::ast::ObjectName,
    base_ref: &IcebergTableRef,
) -> Result<(), String> {
    let actual = normalize_three_part_base_table(base_table)?;
    let expected = (
        crate::engine::catalog::normalize_identifier(&base_ref.catalog).map_err(|e| {
            format!("incremental MV refresh stored metadata has invalid catalog reference: {e}")
        })?,
        crate::engine::catalog::normalize_identifier(&base_ref.namespace).map_err(|e| {
            format!("incremental MV refresh stored metadata has invalid namespace reference: {e}")
        })?,
        crate::engine::catalog::normalize_identifier(&base_ref.table).map_err(|e| {
            format!("incremental MV refresh stored metadata has invalid table reference: {e}")
        })?,
    );
    if actual != expected {
        return Err(format!(
            "incremental MV refresh stored SQL base table mismatch: expected {}.{}.{}, got {}.{}.{}",
            expected.0, expected.1, expected.2, actual.0, actual.1, actual.2
        ));
    }
    Ok(())
}

fn normalize_three_part_base_table(
    base_table: &sqlparser::ast::ObjectName,
) -> Result<(String, String, String), String> {
    let parts = base_table
        .0
        .iter()
        .map(|part| match part {
            sqlparser::ast::ObjectNamePart::Identifier(ident) => {
                crate::engine::catalog::normalize_identifier(&ident.value).map_err(|e| {
                    format!(
                        "incremental MV refresh stored SQL has invalid base table reference: {e}"
                    )
                })
            }
            _ => {
                Err("incremental MV refresh stored SQL base table must use identifiers".to_string())
            }
        })
        .collect::<Result<Vec<_>, _>>()?;
    let [catalog, namespace, table] = parts.as_slice() else {
        return Err(
            "incremental MV refresh stored SQL must reference a 3-part Iceberg table".to_string(),
        );
    };
    Ok((catalog.clone(), namespace.clone(), table.clone()))
}

pub(crate) fn load_current_iceberg_base_table(
    state: &Arc<StandaloneState>,
    table_ref: &IcebergTableRef,
) -> Result<crate::connector::iceberg::catalog::IcebergLoadedTable, String> {
    let entry = {
        let registry = state
            .iceberg_catalogs
            .read()
            .expect("iceberg registry read lock");
        registry.get(&table_ref.catalog)?
    };
    entry.invalidate_table_cache(&table_ref.namespace, &table_ref.table);
    load_table(&entry, &table_ref.namespace, &table_ref.table)
}

pub(crate) fn single_snapshot_map(
    table_ref: &IcebergTableRef,
    snapshot_id: i64,
) -> BTreeMap<String, i64> {
    let mut snapshots = BTreeMap::new();
    snapshots.insert(table_ref.fqn(), snapshot_id);
    snapshots
}

pub(crate) fn single_table_uuid_map(
    table_ref: &IcebergTableRef,
    table_uuid: &str,
) -> BTreeMap<String, String> {
    let mut uuids = BTreeMap::new();
    uuids.insert(table_ref.fqn(), table_uuid.to_string());
    uuids
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
struct CurrentBaseMetadata {
    snapshots: BTreeMap<String, i64>,
    table_uuids: BTreeMap<String, String>,
}

fn collect_current_base_metadata(
    state: &Arc<StandaloneState>,
    refs: &[IcebergTableRef],
) -> Result<CurrentBaseMetadata, String> {
    let registry = state
        .iceberg_catalogs
        .read()
        .expect("iceberg registry read lock");
    let mut metadata = CurrentBaseMetadata::default();
    for table_ref in refs {
        let entry = registry.get(&table_ref.catalog)?;
        let loaded = load_table(&entry, &table_ref.namespace, &table_ref.table)?;
        metadata
            .table_uuids
            .insert(table_ref.fqn(), loaded.table.metadata().uuid().to_string());
        if let Some(snapshot) = loaded.table.metadata().current_snapshot() {
            metadata
                .snapshots
                .insert(table_ref.fqn(), snapshot.snapshot_id());
        }
    }
    Ok(metadata)
}

fn collect_current_base_metadata_or_cleanup_staged_partition(
    state: &Arc<StandaloneState>,
    table_id: i64,
    staged: &StagedManagedMvRefresh,
    refs: &[IcebergTableRef],
) -> Result<CurrentBaseMetadata, String> {
    match collect_current_base_metadata(state, refs) {
        Ok(metadata) => Ok(metadata),
        Err(err) => {
            if let Err(cleanup_err) = cleanup_staged_partition(state, table_id, staged, true) {
                return Err(format!(
                    "mv refresh snapshot collection failed: {err}; cleanup failed: {cleanup_err}"
                ));
            }
            Err(format!("mv refresh snapshot collection failed: {err}"))
        }
    }
}

pub(crate) fn acquire_mv_refresh_lock() -> Result<MutexGuard<'static, ()>, String> {
    static MV_REFRESH_LOCK: OnceLock<Mutex<()>> = OnceLock::new();
    lock_mv_refresh_mutex(MV_REFRESH_LOCK.get_or_init(|| Mutex::new(())))
}

fn lock_mv_refresh_mutex(lock: &Mutex<()>) -> Result<MutexGuard<'_, ()>, String> {
    lock.lock()
        .map_err(|_| "materialized view refresh lock poisoned".to_string())
}

fn cleanup_staged_partition(
    state: &Arc<StandaloneState>,
    table_id: i64,
    staged: &StagedManagedMvRefresh,
    enqueue_erase_job: bool,
) -> Result<(), String> {
    for tablet_id in &staged.tablet_ids {
        let _ = remove_tablet_runtime(*tablet_id);
    }
    let provider = state
        .metadata_provider
        .as_ref()
        .ok_or_else(|| "managed lake MV refresh cleanup requires metadata provider".to_string())?;
    let mut txn = provider
        .begin_write("cleanup managed lake mv refresh partition")
        .map_err(|e| format!("open managed mv refresh cleanup transaction failed: {e}"))?;
    state
        .managed_repo
        .delete_creating_partition(txn.as_mut(), staged.partition_id)
        .map_err(|e| format!("delete staged mv refresh partition failed: {e}"))?;
    if enqueue_erase_job {
        state
            .job_repo
            .create_erase_job(
                txn.as_mut(),
                CreateEraseJobRequest {
                    table_id,
                    partition_id: Some(staged.partition_id),
                    root_path: staged.partition_root_path.clone(),
                    now_ms: super::mv_ddl::now_ms(),
                },
            )
            .map_err(|e| format!("enqueue staged mv refresh erase job failed: {e}"))?;
    }
    txn.commit()
        .map_err(|e| format!("commit managed mv refresh cleanup failed: {e}"))?;
    refresh_managed_catalog(state)?;
    Ok(())
}

fn load_mv_definition_by_id(
    state: &Arc<StandaloneState>,
    mv_id: i64,
) -> Result<Option<StoredMvDefinition>, String> {
    let provider = state
        .metadata_provider
        .as_ref()
        .ok_or_else(|| "materialized view metadata provider is not configured".to_string())?;
    let read = provider
        .begin_read()
        .map_err(|e| format!("open mv definition read transaction failed: {e}"))?;
    state
        .mv_repo
        .load_by_id(read.as_ref(), mv_id)
        .map_err(|e| format!("load mv definition failed: {e}"))
}

fn load_mv_definition_by_target(
    state: &Arc<StandaloneState>,
    catalog: &str,
    namespace: &str,
    table: &str,
) -> Result<Option<StoredMvDefinition>, String> {
    let provider = state
        .metadata_provider
        .as_ref()
        .ok_or_else(|| "materialized view metadata provider is not configured".to_string())?;
    let read = provider
        .begin_read()
        .map_err(|e| format!("open mv target read transaction failed: {e}"))?;
    state
        .mv_repo
        .find_by_target(read.as_ref(), catalog, namespace, table)
        .map_err(|e| format!("load mv target definition failed: {e}"))
}

fn begin_mv_refresh_intent(
    state: &Arc<StandaloneState>,
    mv_id: i64,
    target_snapshots: BTreeMap<String, i64>,
) -> Result<(), String> {
    let provider = state
        .metadata_provider
        .as_ref()
        .ok_or_else(|| "materialized view refresh requires metadata provider".to_string())?;
    let mut txn = provider
        .begin_write("begin materialized view refresh")
        .map_err(|e| format!("open mv refresh transaction failed: {e}"))?;
    state
        .mv_repo
        .begin_refresh_intent(txn.as_mut(), mv_id, target_snapshots)
        .map_err(|e| format!("begin mv refresh intent failed: {e}"))?;
    txn.commit()
        .map_err(|e| format!("commit mv refresh intent failed: {e}"))?;
    Ok(())
}

fn clear_mv_refresh_progress(state: &Arc<StandaloneState>, mv_id: i64) -> Result<(), String> {
    let provider = state
        .metadata_provider
        .as_ref()
        .ok_or_else(|| "materialized view refresh requires metadata provider".to_string())?;
    let mut txn = provider
        .begin_write("clear materialized view refresh progress")
        .map_err(|e| format!("open mv refresh cleanup transaction failed: {e}"))?;
    state
        .mv_repo
        .clear_refresh_progress(txn.as_mut(), mv_id)
        .map_err(|e| format!("clear mv refresh progress failed: {e}"))?;
    txn.commit()
        .map_err(|e| format!("commit mv refresh cleanup failed: {e}"))?;
    Ok(())
}

fn update_managed_mv_refresh_summary(
    state: &Arc<StandaloneState>,
    mv_id: i64,
    last_refresh_rows: i64,
    base_snapshots: BTreeMap<String, i64>,
    base_table_uuids: BTreeMap<String, String>,
) -> Result<(), String> {
    let provider = state
        .metadata_provider
        .as_ref()
        .ok_or_else(|| "materialized view refresh requires metadata provider".to_string())?;
    let mut txn = provider
        .begin_write("update managed materialized view refresh summary")
        .map_err(|e| format!("open mv refresh summary transaction failed: {e}"))?;
    state
        .mv_repo
        .update_managed_refresh_summary_if_present(
            txn.as_mut(),
            UpdateManagedMvRefreshSummaryRequest {
                mv_id,
                last_refresh_ms: super::mv_ddl::now_ms(),
                last_refresh_rows,
                base_snapshots,
                base_table_uuids,
            },
        )
        .map_err(|e| format!("update mv refresh summary failed: {e}"))?;
    txn.commit()
        .map_err(|e| format!("commit mv refresh summary failed: {e}"))?;
    Ok(())
}

fn stage_managed_mv_refresh_partition(
    state: &Arc<StandaloneState>,
    runtime: &ManagedTableRuntime,
    partition_name: &str,
    warehouse_uri: &str,
) -> Result<StagedManagedMvRefresh, String> {
    let provider = state
        .metadata_provider
        .as_ref()
        .ok_or_else(|| "managed lake MV refresh requires metadata provider".to_string())?;
    let mut txn = provider
        .begin_write("stage managed lake mv refresh partition")
        .map_err(|e| format!("open managed mv refresh stage transaction failed: {e}"))?;
    state
        .managed_txn_repo
        .ensure_no_inflight_for_table(txn.as_ref(), runtime.table.table_id)
        .map_err(|e| format!("validate managed mv refresh failed: {e}"))?;
    let staged = state
        .managed_repo
        .stage_mv_refresh_partition(
            txn.as_mut(),
            StageManagedMvRefreshRequest {
                table_id: runtime.table.table_id,
                db_id: runtime.table.db_id,
                bucket_num: runtime.table.bucket_num,
                partition_name: partition_name.to_string(),
                warehouse_uri: warehouse_uri.to_string(),
            },
        )
        .map_err(|e| format!("stage managed mv refresh metadata failed: {e}"))?;
    txn.commit()
        .map_err(|e| format!("commit managed mv refresh stage metadata failed: {e}"))?;
    Ok(staged)
}

fn activate_managed_mv_refresh_partition(
    state: &Arc<StandaloneState>,
    table_id: i64,
    old_partition_id: i64,
    retired_root_path: &str,
    staged: &StagedManagedMvRefresh,
    rows_written: i64,
    base_metadata: CurrentBaseMetadata,
) -> Result<(), String> {
    let provider = state
        .metadata_provider
        .as_ref()
        .ok_or_else(|| "managed lake MV refresh requires metadata provider".to_string())?;
    let mut txn = provider
        .begin_write("activate managed lake mv refresh partition")
        .map_err(|e| format!("open managed mv refresh activate transaction failed: {e}"))?;
    state
        .managed_repo
        .activate_mv_refresh_partition(
            txn.as_mut(),
            table_id,
            old_partition_id,
            staged.partition_id,
            staged.index_id,
        )
        .map_err(|e| format!("activate managed mv refresh metadata failed: {e}"))?;
    state
        .job_repo
        .create_erase_job(
            txn.as_mut(),
            CreateEraseJobRequest {
                table_id,
                partition_id: Some(old_partition_id),
                root_path: retired_root_path.to_string(),
                now_ms: super::mv_ddl::now_ms(),
            },
        )
        .map_err(|e| format!("enqueue managed mv refresh erase job failed: {e}"))?;
    state
        .mv_repo
        .update_managed_refresh_summary_if_present(
            txn.as_mut(),
            UpdateManagedMvRefreshSummaryRequest {
                mv_id: table_id,
                last_refresh_ms: super::mv_ddl::now_ms(),
                last_refresh_rows: rows_written,
                base_snapshots: base_metadata.snapshots,
                base_table_uuids: base_metadata.table_uuids,
            },
        )
        .map_err(|e| format!("update managed mv refresh summary failed: {e}"))?;
    txn.commit()
        .map_err(|e| format!("commit managed mv refresh activate metadata failed: {e}"))?;
    Ok(())
}

pub(crate) fn parse_iceberg_table_refs(refs: &[String]) -> Result<Vec<IcebergTableRef>, String> {
    refs.iter()
        .map(|fqn| {
            let parts = fqn.split('.').collect::<Vec<_>>();
            let [catalog, namespace, table] = parts.as_slice() else {
                return Err(format!(
                    "materialized view base table reference must be catalog.namespace.table, got `{fqn}`"
                ));
            };
            Ok(IcebergTableRef {
                catalog: crate::engine::catalog::normalize_identifier(catalog)?,
                namespace: crate::engine::catalog::normalize_identifier(namespace)?,
                table: crate::engine::catalog::normalize_identifier(table)?,
            })
        })
        .collect()
}

fn refresh_managed_catalog(state: &Arc<StandaloneState>) -> Result<(), String> {
    let provider = state
        .metadata_provider
        .as_ref()
        .ok_or_else(|| "managed lake catalog refresh requires metadata provider".to_string())?;
    let read = provider
        .begin_read()
        .map_err(|e| format!("open managed catalog refresh transaction failed: {e}"))?;
    let snapshot = state
        .managed_repo
        .load_snapshot(read.as_ref())
        .map_err(|e| format!("load managed catalog metadata failed: {e}"))?;
    let rebuilt = ManagedLakeCatalog::rebuild_from_repository(
        state.managed_lake_config.clone(),
        snapshot.clone(),
    )?;
    {
        let mut catalog = state
            .catalog
            .write()
            .expect("standalone catalog write lock");
        for database in &snapshot.databases {
            catalog.create_database(&database.name)?;
        }
        register_managed_tables_in_catalog(&mut catalog, &rebuilt)?;
    }
    rebuilt.re_register_active_tablet_runtimes()?;
    let mut managed = state
        .managed_lake
        .write()
        .expect("standalone managed lake write lock");
    *managed = rebuilt;
    Ok(())
}

fn resolve_mv_name(name: &ObjectName, current_database: &str) -> Result<(String, String), String> {
    match name.parts.as_slice() {
        [table] => Ok((
            crate::engine::catalog::normalize_identifier(current_database)?,
            crate::engine::catalog::normalize_identifier(table)?,
        )),
        [database, table] => Ok((
            crate::engine::catalog::normalize_identifier(database)?,
            crate::engine::catalog::normalize_identifier(table)?,
        )),
        [catalog, database, table] => {
            let catalog = crate::engine::catalog::normalize_identifier(catalog)?;
            if catalog != "default_catalog" {
                return Err(format!(
                    "materialized view name catalog must be `default_catalog`, got `{catalog}`"
                ));
            }
            Ok((
                crate::engine::catalog::normalize_identifier(database)?,
                crate::engine::catalog::normalize_identifier(table)?,
            ))
        }
        _ => Err(format!(
            "materialized view name must be `<name>`, `<db>.<name>`, or `default_catalog.<db>.<name>`; got `{}`",
            name.parts.join(".")
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::connector::iceberg::catalog::IcebergCatalogRegistry;
    use crate::connector::starrocks::ObjectStoreProfile;
    use crate::connector::starrocks::lake::context::lock_runtime_test_state;
    use crate::connector::starrocks::lake::schema::{
        build_tablet_schema_pb_from_thrift, create_lake_tablet_from_req_with_schema_patch,
    };
    use crate::connector::starrocks::managed::ManagedLakeConfig;
    use crate::connector::starrocks::managed::ddl::{
        build_create_tablet_request, build_tablet_schema, keys_type_name,
        patch_tablet_schema_column_flags, stored_columns_from_physical_columns,
        table_columns_from_physical_columns,
    };
    use crate::connector::starrocks::managed::model::{
        ManagedGlobalMeta, ManagedIndexState, ManagedMvRefreshMode, ManagedMvStorageEngine,
        ManagedSnapshot, ManagedTableKind, ManagedTableState, StoredManagedDatabase,
        StoredManagedIndex, StoredManagedPartition, StoredManagedSchema, StoredManagedTable,
        StoredManagedTablet, StoredMaterializedView,
    };
    use crate::connector::starrocks::managed::mv_refresh_strategy::FullRefreshReason;
    use crate::engine::catalog::InMemoryCatalog;
    use crate::engine::{QueryResult, QueryResultColumn, record_batch_to_chunk};
    use crate::formats::starrocks::metadata::load_tablet_snapshot;
    use crate::meta::MetaStoreProvider;
    use crate::runtime::starlet_shard_registry::S3StoreConfig;
    use crate::sql::analysis::OutputColumn;
    use crate::sql::parser::ast::{TableKeyDesc, TableKeyKind};
    use arrow::array::{Array, Int64Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use prost::Message;
    use std::cell::Cell;
    use std::net::ToSocketAddrs;
    use std::sync::RwLock;
    use std::time::Duration;

    #[test]
    fn choose_refresh_strategy_without_previous_snapshot_uses_full_refresh() {
        let strategy = choose_snapshot_refresh_policy(None, Some(10)).expect("strategy");
        assert_eq!(
            strategy,
            MvRefreshPolicy::FullRefresh {
                target_snapshot_id: Some(10),
                reason: FullRefreshReason::InitialRefresh,
            }
        );
    }

    #[test]
    fn choose_refresh_strategy_for_same_snapshot_is_no_op() {
        let strategy = choose_snapshot_refresh_policy(Some(10), Some(10)).expect("strategy");
        assert_eq!(
            strategy,
            MvRefreshPolicy::NoOp {
                current_snapshot_id: 10
            }
        );
    }

    #[test]
    fn choose_refresh_strategy_for_advanced_snapshot_is_incremental() {
        let strategy = choose_snapshot_refresh_policy(Some(10), Some(12)).expect("strategy");
        assert_eq!(
            strategy,
            MvRefreshPolicy::Incremental {
                previous_snapshot_id: 10,
                current_snapshot_id: 12,
            }
        );
    }

    #[test]
    fn choose_refresh_strategy_rejects_unreachable_previous_snapshot() {
        let err = choose_snapshot_refresh_policy(Some(10), None).expect_err("strategy should fail");
        assert!(
            err.contains("10"),
            "expected error to contain previous snapshot id, got `{err}`"
        );
        assert!(
            err.contains("no current snapshot"),
            "expected error to describe missing current snapshot, got `{err}`"
        );
    }

    #[test]
    fn replace_validation_change_error_routes_projection_mv_to_full_refresh_policy() {
        let policy = policy_from_change_error(
            crate::connector::iceberg::changes::ChangeError::ReplaceValidationFailed {
                snapshot_id: 99,
                reason: "records changed".to_string(),
            },
        );
        assert!(matches!(policy, MvRefreshPolicy::FullRefresh { .. }));
    }

    #[test]
    fn advance_empty_change_stream_updates_metadata_without_writing() {
        let (_dir, state, table_id) = seed_mv_refresh_state();
        let base_ref = IcebergTableRef {
            catalog: "missing_catalog".to_string(),
            namespace: "ns".to_string(),
            table: "orders".to_string(),
        };
        let snapshots = single_snapshot_map(&base_ref, 42);
        begin_mv_refresh_intent(&state, table_id, snapshots.clone()).expect("begin refresh");

        advance_mv_refresh_metadata_without_writes(&state, table_id, &base_ref, 42, "uuid-1", 17)
            .expect("advance metadata");

        let provider = state.metadata_provider.as_ref().expect("metadata provider");
        let read = provider.begin_read().expect("read");
        let mv = state
            .mv_repo
            .load_by_id(read.as_ref(), table_id)
            .expect("load mv")
            .expect("mv definition");
        assert_eq!(mv.last_refresh_rows, Some(17));
        assert_eq!(mv.last_refresh_snapshots, snapshots);
        assert!(!mv.refresh_in_progress);
        assert!(mv.refresh_target_snapshots.is_empty());
    }

    #[test]
    fn refresh_mv_full_cleans_staged_partition_when_executor_fails() {
        // Covered by integration-style engine/mysql tests once mv refresh is wired.
        // Keep this module-level smoke test minimal so the file always participates
        // in compilation even when object-store-backed test infra is unavailable.
        let _ = std::any::type_name::<MvRefreshContext>();
    }

    #[test]
    fn refresh_mv_dispatches_aggregate_full_to_aggregate_executor() {
        let mv_shape = super::super::mv_shape::IncrementalMvShape::Aggregate(
            aggregate_mv_shape().expect("aggregate shape"),
        );
        let projection_full_called = Cell::new(false);
        let aggregate_full_called = Cell::new(false);

        let result = dispatch_mv_refresh_strategy(
            &mv_shape,
            MvRefreshPolicy::FullRefresh {
                target_snapshot_id: Some(12),
                reason: FullRefreshReason::InitialRefresh,
            },
            || {
                projection_full_called.set(true);
                Err("projection full executor should not run".to_string())
            },
            |shape| {
                aggregate_full_called.set(true);
                assert_eq!(shape.aggregates.len(), 2);
                Ok(StatementResult::Ok)
            },
            |_| Err("no-op path should not run".to_string()),
            |_, _| Err("projection incremental path should not run".to_string()),
            |_, _, _| Err("aggregate incremental path should not run".to_string()),
        );

        assert!(result.is_ok(), "result={result:?}");
        assert!(aggregate_full_called.get());
        assert!(!projection_full_called.get());
    }

    #[test]
    fn aggregate_full_refresh_executor_writes_physical_columns() {
        let Some(config) = maybe_object_store_config_for_mv_refresh() else {
            return;
        };
        let _guard = lock_runtime_test_state();
        let (_dir, state, shape) = match seed_aggregate_mv_refresh_state(config) {
            Ok(fixture) => fixture,
            Err(err) if is_unavailable_object_store_error(&err) => {
                eprintln!(
                    "skipping aggregate MV full refresh writer test: object store is unavailable: {err}"
                );
                return;
            }
            Err(err) => panic!("aggregate mv fixture: {err}"),
        };

        let refresh_result =
            refresh_mv_full_with_executor(&state, "analytics", "orders_mv", move |_ctx| {
                let result = aggregate_visible_query_result()?;
                let output_columns = result
                    .columns
                    .iter()
                    .map(query_result_column_to_output_column)
                    .collect::<Result<Vec<_>, String>>()?;
                let layout =
                    super::super::mv_agg_state::build_aggregate_mv_layout(&shape, &output_columns)?;
                let chunks = super::super::mv_agg_state::materialize_aggregate_result_chunks(
                    result, &layout, &shape,
                )?;
                assert_eq!(chunks.len(), 1);
                assert_eq!(chunks[0].batch.num_columns(), layout.physical_columns.len());
                assert_eq!(
                    chunks[0]
                        .batch
                        .schema()
                        .fields()
                        .iter()
                        .map(|field| field.name().as_str())
                        .collect::<Vec<_>>(),
                    vec![
                        "__row_id__",
                        "k1",
                        "c",
                        "s",
                        "__agg_state_c",
                        "__agg_state_s"
                    ]
                );
                Ok(chunks)
            });
        if let Err(err) = refresh_result {
            if is_unavailable_object_store_error(&err) {
                eprintln!(
                    "skipping aggregate MV full refresh writer test: object store is unavailable: {err}"
                );
                return;
            }
            panic!("full refresh: {err}");
        }

        let provider = state.metadata_provider.as_ref().expect("metadata provider");
        let read = provider.begin_read().expect("metadata read");
        let snapshot = state
            .managed_repo
            .load_snapshot(read.as_ref())
            .expect("repository snapshot");
        let active_partition = snapshot
            .partitions
            .iter()
            .find(|partition| {
                partition.table_id == 10
                    && partition.state
                        == crate::meta::repository::managed_lake::ManagedPartitionState::Active
            })
            .expect("active partition");
        assert_ne!(active_partition.partition_id, 20);
        let active_tablets = snapshot
            .tablets
            .iter()
            .filter(|tablet| tablet.partition_id == active_partition.partition_id)
            .collect::<Vec<_>>();
        assert_eq!(active_tablets.len(), 2);

        let profile = ObjectStoreProfile::from_s3_store_config(
            &state.managed_lake_config.as_ref().expect("config").s3,
        )
        .expect("object store profile");
        let mut total_rows = 0_u64;
        for tablet in active_tablets {
            let loaded = load_tablet_snapshot(
                tablet.tablet_id,
                active_partition.visible_version,
                &tablet.tablet_root_path,
                Some(&profile),
            )
            .expect("active tablet snapshot");
            let column_names = loaded
                .tablet_schema
                .column
                .iter()
                .map(|column| column.name.as_deref().unwrap_or(""))
                .collect::<Vec<_>>();
            assert_eq!(
                column_names,
                vec![
                    "__row_id__",
                    "k1",
                    "c",
                    "s",
                    "__agg_state_c",
                    "__agg_state_s"
                ]
            );
            assert_eq!(loaded.tablet_schema.column[0].visible, Some(false));
            assert_eq!(loaded.tablet_schema.column[0].is_key, Some(true));
            assert_eq!(loaded.tablet_schema.column[4].visible, Some(false));
            assert_eq!(loaded.tablet_schema.column[5].visible, Some(false));
            total_rows += loaded.total_num_rows;
        }
        assert_eq!(total_rows, 2);
    }

    #[test]
    fn aggregate_mv_incremental_refresh_handles_base_delete() {
        // End-to-end delete-bearing aggregate IVM test. Builds a real iceberg base
        // table (3 rows over (id, customer, amount)), creates a
        // managed-lake aggregate MV (count + sum grouped by customer),
        // populates the MV via a full REFRESH, then DELETEs one base row
        // and triggers an incremental REFRESH. Asserts the MV's count
        // and sum decrease for the affected group while the other group
        // is untouched.
        //
        // Skipped when the minio object-store endpoint is unreachable
        // (matches the pattern in `aggregate_full_refresh_executor_writes_physical_columns`).
        //
        // We hold `lock_runtime_test_state()` for the test's full
        // duration so that no parallel test holding the same runtime-state
        // lock can clear the global tablet/shard registries underneath
        // us once `StandaloneNovaRocks::open` has registered our tablets.
        let _runtime_guard = lock_runtime_test_state();
        let Some((_config_dir, config_path)) = maybe_managed_lake_config_path() else {
            return;
        };
        let iceberg_dir = tempfile::tempdir().expect("iceberg warehouse tempdir");
        let iceberg_warehouse = format!("file://{}", iceberg_dir.path().display());

        let engine = match crate::engine::StandaloneNovaRocks::open(
            crate::engine::StandaloneOptions {
                config_path: Some(config_path),
            },
        ) {
            Ok(engine) => engine,
            Err(err) => {
                if is_unavailable_object_store_error(&err) {
                    eprintln!(
                        "skipping aggregate MV incremental DELETE test: object store unavailable: {err}"
                    );
                    return;
                }
                panic!("open standalone engine: {err}");
            }
        };
        let session = engine.session();
        let create_catalog_sql = format!(
            r#"create external catalog ice properties("type"="iceberg","iceberg.catalog.type"="hadoop","iceberg.catalog.warehouse"="{iceberg_warehouse}")"#
        );
        session
            .execute_in_database(&create_catalog_sql, "default")
            .expect("create iceberg catalog");
        session
            .execute_in_database("create database ice.ns", "default")
            .expect("create iceberg namespace");
        session
            .execute_in_database(
                r#"create table ice.ns.orders (id bigint not null, customer string, amount bigint) tblproperties("format-version"="3")"#,
                "default",
            )
            .expect("create iceberg orders table");
        session
            .execute_in_database(
                "insert into ice.ns.orders values (1, 'A', 10), (2, 'A', 20), (3, 'B', 30)",
                "default",
            )
            .expect("seed iceberg base rows");

        session
            .execute_in_database("create database analytics", "default")
            .expect("create analytics database");
        if let Err(err) = session.execute_in_database(
            "create materialized view agg_mv \
             distributed by hash(customer) buckets 2 \
             as select customer, count(*) as c, sum(amount) as s \
             from ice.ns.orders group by customer",
            "analytics",
        ) {
            if is_unavailable_object_store_error(&err) {
                eprintln!(
                    "skipping aggregate MV incremental DELETE test: object store unavailable on create: {err}"
                );
                return;
            }
            panic!("create materialized view: {err}");
        }

        if let Err(err) =
            session.execute_in_database("refresh materialized view agg_mv", "analytics")
        {
            if is_unavailable_object_store_error(&err) {
                eprintln!(
                    "skipping aggregate MV incremental DELETE test: object store unavailable on full refresh: {err}"
                );
                return;
            }
            panic!("first (full) refresh materialized view: {err}");
        }

        let pre_state = match collect_agg_mv_state(&session) {
            Ok(rows) => rows,
            Err(err) => {
                if is_unavailable_object_store_error(&err) {
                    eprintln!(
                        "skipping aggregate MV incremental DELETE test: object store unavailable on pre-delete select: {err}"
                    );
                    return;
                }
                panic!("select pre-delete agg_mv state: {err}");
            }
        };
        assert_eq!(
            pre_state,
            vec![
                ("A".to_string(), 2_i64, 30_i64),
                ("B".to_string(), 1_i64, 30_i64),
            ],
            "MV state after full refresh must reflect the 3 seeded base rows"
        );
        assert_eq!(
            show_mv_last_refresh_rows(&session, "agg_mv").expect("show full refresh rows"),
            Some(2),
            "full refresh metadata row count tracks MV group count"
        );

        // Trigger a DELETE-bearing snapshot on the base.
        session
            .execute_in_database("delete from ice.ns.orders where id = 1", "default")
            .expect("delete base row id=1");

        if let Err(err) =
            session.execute_in_database("refresh materialized view agg_mv", "analytics")
        {
            if is_unavailable_object_store_error(&err) {
                eprintln!(
                    "skipping aggregate MV incremental DELETE test: object store unavailable on incremental refresh: {err}"
                );
                return;
            }
            panic!("second (incremental, delete-bearing) refresh materialized view: {err}");
        }

        let post_state = match collect_agg_mv_state(&session) {
            Ok(rows) => rows,
            Err(err) => {
                if is_unavailable_object_store_error(&err) {
                    eprintln!(
                        "skipping aggregate MV incremental DELETE test: object store unavailable on post-delete select: {err}"
                    );
                    return;
                }
                panic!("select post-delete agg_mv state: {err}");
            }
        };
        assert_eq!(
            post_state,
            vec![
                ("A".to_string(), 1_i64, 20_i64),
                ("B".to_string(), 1_i64, 30_i64),
            ],
            "MV state after delete-bearing incremental refresh must drop \
             count and sum for the affected group; other groups untouched"
        );
        assert_eq!(
            show_mv_last_refresh_rows(&session, "agg_mv").expect("show incremental refresh rows"),
            Some(2),
            "incremental refresh metadata row count tracks active MV rows, not the row delta"
        );

        drop(engine);
    }

    #[test]
    fn aggregate_mv_incremental_refresh_treats_deleted_data_files_as_delete_bearing() {
        let batch = crate::connector::iceberg::changes::IcebergChangeBatch {
            previous_snapshot_id: 1,
            current_snapshot_id: 2,
            inserts: Vec::new(),
            deletes: Vec::new(),
            equality_deletes: Vec::new(),
            deleted_data_files: vec![crate::connector::iceberg::changes::DeletedDataFileRef {
                path: "file:///tmp/old.parquet".to_string(),
                size: 128,
                record_count: Some(1),
                partition_spec_id: Some(0),
                partition_key: None,
                first_row_id: Some(0),
                data_sequence_number: Some(1),
            }],
        };

        assert!(change_batch_has_deletes(&batch));
    }

    #[test]
    fn projection_delete_bearing_change_applies_deletes_before_upserts() {
        use arrow::array::{Int8Array, Int64Array};

        fn tagged_result(values: Vec<i64>, ops: Vec<i8>) -> QueryResult {
            let batch = RecordBatch::try_new(
                Arc::new(Schema::new(vec![
                    Field::new("id", DataType::Int64, false),
                    Field::new(
                        crate::exec::change_op::CHANGE_OP_COLUMN,
                        DataType::Int8,
                        false,
                    ),
                ])),
                vec![
                    Arc::new(Int64Array::from(values)) as ArrayRef,
                    Arc::new(Int8Array::from(ops)) as ArrayRef,
                ],
            )
            .expect("record batch");
            QueryResult {
                columns: Vec::new(),
                chunks: vec![record_batch_to_chunk(batch).expect("chunk")],
            }
        }

        fn op_value(chunk: &Chunk) -> i8 {
            let op_column = chunk
                .batch
                .column(chunk.batch.num_columns() - 1)
                .as_any()
                .downcast_ref::<Int8Array>()
                .expect("op column");
            op_column.value(0)
        }

        let (chunks, row_delta) =
            tagged_projection_change_chunks(tagged_result(vec![10, 20, 30], vec![1, -1, 1]))
                .expect("projection chunks");

        assert_eq!(row_delta, 1);
        assert_eq!(chunks.len(), 2);
        assert_eq!(chunks[0].batch.num_rows(), 1);
        assert_eq!(chunks[1].batch.num_rows(), 2);
        assert_eq!(
            chunks[0].batch.schema().field(0).name(),
            chunks[1].batch.schema().field(0).name()
        );
        assert_eq!(chunks[0].batch.num_columns(), 2);
        assert_eq!(op_value(&chunks[0]), MV_OP_DELETE);
        assert_eq!(op_value(&chunks[1]), MV_OP_UPSERT);
    }

    #[test]
    fn projection_change_op_can_be_non_last_and_is_removed_before_mv_op() {
        use arrow::array::{Int8Array, StringArray};

        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("id", DataType::Int64, false),
                Field::new(
                    crate::exec::change_op::CHANGE_OP_COLUMN,
                    DataType::Int8,
                    false,
                ),
                Field::new("name", DataType::Utf8, false),
            ])),
            vec![
                Arc::new(Int64Array::from(vec![10, 20])) as ArrayRef,
                Arc::new(Int8Array::from(vec![1, -1])) as ArrayRef,
                Arc::new(StringArray::from(vec!["a", "b"])) as ArrayRef,
            ],
        )
        .expect("record batch");
        let result = QueryResult {
            columns: Vec::new(),
            chunks: vec![record_batch_to_chunk(batch).expect("chunk")],
        };

        let (chunks, row_delta) = tagged_projection_change_chunks(result).expect("chunks");

        assert_eq!(row_delta, 0);
        assert_eq!(chunks.len(), 2);
        for chunk in chunks {
            let names = chunk
                .batch
                .schema()
                .fields()
                .iter()
                .map(|field| field.name().clone())
                .collect::<Vec<_>>();
            assert_eq!(names, vec!["id", "name", MV_OP_COLUMN]);
        }
    }

    #[test]
    fn projection_change_op_rejects_null_value() {
        use arrow::array::Int8Array;

        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("id", DataType::Int64, false),
                Field::new(
                    crate::exec::change_op::CHANGE_OP_COLUMN,
                    DataType::Int8,
                    true,
                ),
            ])),
            vec![
                Arc::new(Int64Array::from(vec![10, 20])) as ArrayRef,
                Arc::new(Int8Array::from(vec![Some(1), None])) as ArrayRef,
            ],
        )
        .expect("record batch");
        let result = QueryResult {
            columns: Vec::new(),
            chunks: vec![record_batch_to_chunk(batch).expect("chunk")],
        };

        let err = tagged_projection_change_chunks(result).expect_err("null op must fail");

        assert!(err.contains("contains NULL"));
    }

    #[test]
    fn aggregate_mv_incremental_refresh_handles_equality_delete() {
        let _runtime_guard = lock_runtime_test_state();
        let Some((_config_dir, config_path)) = maybe_managed_lake_config_path() else {
            return;
        };
        let iceberg_dir = tempfile::tempdir().expect("iceberg warehouse tempdir");
        let iceberg_warehouse = format!("file://{}", iceberg_dir.path().display());

        let engine = match crate::engine::StandaloneNovaRocks::open(
            crate::engine::StandaloneOptions {
                config_path: Some(config_path),
            },
        ) {
            Ok(engine) => engine,
            Err(err) => {
                if is_unavailable_object_store_error(&err) {
                    eprintln!(
                        "skipping aggregate MV equality-delete test: object store unavailable: {err}"
                    );
                    return;
                }
                panic!("open standalone engine: {err}");
            }
        };
        let session = engine.session();
        let create_catalog_sql = format!(
            r#"create external catalog ice properties("type"="iceberg","iceberg.catalog.type"="hadoop","iceberg.catalog.warehouse"="{iceberg_warehouse}")"#
        );
        session
            .execute_in_database(&create_catalog_sql, "default")
            .expect("create iceberg catalog");
        session
            .execute_in_database("create database ice.ns", "default")
            .expect("create iceberg namespace");
        session
            .execute_in_database(
                r#"create table ice.ns.orders (id bigint not null, customer string, amount bigint) tblproperties("format-version"="2")"#,
                "default",
            )
            .expect("create iceberg orders table");
        session
            .execute_in_database(
                "insert into ice.ns.orders values (1, 'A', 10), (2, 'A', 20), (3, 'B', 30)",
                "default",
            )
            .expect("seed iceberg base rows");

        session
            .execute_in_database("create database analytics", "default")
            .expect("create analytics database");
        if let Err(err) = session.execute_in_database(
            "create materialized view agg_mv \
             distributed by hash(customer) buckets 2 \
             as select customer, count(*) as c, sum(amount) as s \
             from ice.ns.orders group by customer",
            "analytics",
        ) {
            if is_unavailable_object_store_error(&err) {
                eprintln!(
                    "skipping aggregate MV equality-delete test: object store unavailable on create: {err}"
                );
                return;
            }
            panic!("create materialized view: {err}");
        }

        if let Err(err) =
            session.execute_in_database("refresh materialized view agg_mv", "analytics")
        {
            if is_unavailable_object_store_error(&err) {
                eprintln!(
                    "skipping aggregate MV equality-delete test: object store unavailable on full refresh: {err}"
                );
                return;
            }
            panic!("first (full) refresh materialized view: {err}");
        }

        let pre_state = match collect_agg_mv_state(&session) {
            Ok(rows) => rows,
            Err(err) => {
                if is_unavailable_object_store_error(&err) {
                    eprintln!(
                        "skipping aggregate MV equality-delete test: object store unavailable on pre-delete select: {err}"
                    );
                    return;
                }
                panic!("select pre-delete agg_mv state: {err}");
            }
        };
        assert_eq!(
            pre_state,
            vec![
                ("A".to_string(), 2_i64, 30_i64),
                ("B".to_string(), 1_i64, 30_i64),
            ],
            "MV state after full refresh must reflect the seeded base rows"
        );

        session
            .execute_in_database(
                "alter table ice.ns.orders add equality delete (id) values (1)",
                "default",
            )
            .expect("add equality delete for base row id=1");

        if let Err(err) =
            session.execute_in_database("refresh materialized view agg_mv", "analytics")
        {
            if is_unavailable_object_store_error(&err) {
                eprintln!(
                    "skipping aggregate MV equality-delete test: object store unavailable on incremental refresh: {err}"
                );
                return;
            }
            panic!(
                "second (incremental, equality-delete-bearing) refresh materialized view: {err}"
            );
        }

        let post_state = match collect_agg_mv_state(&session) {
            Ok(rows) => rows,
            Err(err) => {
                if is_unavailable_object_store_error(&err) {
                    eprintln!(
                        "skipping aggregate MV equality-delete test: object store unavailable on post-delete select: {err}"
                    );
                    return;
                }
                panic!("select post-delete agg_mv state: {err}");
            }
        };
        assert_eq!(
            post_state,
            vec![
                ("A".to_string(), 1_i64, 20_i64),
                ("B".to_string(), 1_i64, 30_i64),
            ],
            "MV state after equality-delete incremental refresh must retract the deleted row"
        );

        drop(engine);
    }

    #[test]
    fn aggregate_mv_incremental_refresh_handles_v3_row_lineage_base_delete() {
        // TODO(ivm-phase-2 follow-up): this test is a near-copy of
        // `aggregate_mv_incremental_refresh_handles_base_delete` differing only in
        // `tblproperties`. Extract a shared `run_aggregate_mv_incremental_delete_refresh`
        // harness once we touch the reference test for another reason; the duplication
        // is intentional here to keep this PR's diff minimal and reviewable.
        //
        // End-to-end variant of `aggregate_mv_incremental_refresh_handles_base_delete`
        // that puts the base on Iceberg v3 with `write.row-lineage=true`. The DELETE
        // therefore writes a Puffin deletion-vector file (Phase 2a RowDeltaDvCommit)
        // instead of a v2 position-delete Parquet. The IVM incremental refresh path
        // exercised here is `plan_changes` -> `materialize_changes` -> `scan_deletes`,
        // with scan_deletes routing the Puffin DV through the new
        // `read_dv_positions_per_data_file` helper.
        //
        // Skipped when the minio object-store endpoint is unreachable, matching
        // `aggregate_mv_incremental_refresh_handles_base_delete`.
        //
        // We hold `lock_runtime_test_state()` for the test's full duration so
        // that no parallel test holding the same runtime-state lock can clear
        // the global tablet/shard registries underneath us once
        // `StandaloneNovaRocks::open` has registered our tablets.
        let _runtime_guard = lock_runtime_test_state();
        let Some((_config_dir, config_path)) = maybe_managed_lake_config_path() else {
            return;
        };
        let iceberg_dir = tempfile::tempdir().expect("iceberg warehouse tempdir");
        let iceberg_warehouse = format!("file://{}", iceberg_dir.path().display());

        let engine = match crate::engine::StandaloneNovaRocks::open(
            crate::engine::StandaloneOptions {
                config_path: Some(config_path),
            },
        ) {
            Ok(engine) => engine,
            Err(err) => {
                if is_unavailable_object_store_error(&err) {
                    eprintln!(
                        "skipping v3 row-lineage MV incremental DELETE test: object store unavailable: {err}"
                    );
                    return;
                }
                panic!("open standalone engine: {err}");
            }
        };
        let session = engine.session();
        let create_catalog_sql = format!(
            r#"create external catalog ice properties("type"="iceberg","iceberg.catalog.type"="hadoop","iceberg.catalog.warehouse"="{iceberg_warehouse}")"#
        );
        session
            .execute_in_database(&create_catalog_sql, "default")
            .expect("create iceberg catalog");
        session
            .execute_in_database("create database ice.ns", "default")
            .expect("create iceberg namespace");
        // The only delta vs `_handles_base_delete`: tblproperties enables row-lineage.
        session
            .execute_in_database(
                r#"create table ice.ns.orders (id bigint not null, customer string, amount bigint) tblproperties("format-version"="3","write.row-lineage"="true")"#,
                "default",
            )
            .expect("create row-lineage iceberg orders table");
        session
            .execute_in_database(
                "insert into ice.ns.orders values (1, 'A', 10), (2, 'A', 20), (3, 'B', 30)",
                "default",
            )
            .expect("seed iceberg base rows");

        session
            .execute_in_database("create database analytics", "default")
            .expect("create analytics database");
        if let Err(err) = session.execute_in_database(
            "create materialized view agg_mv \
             distributed by hash(customer) buckets 2 \
             as select customer, count(*) as c, sum(amount) as s \
             from ice.ns.orders group by customer",
            "analytics",
        ) {
            if is_unavailable_object_store_error(&err) {
                eprintln!(
                    "skipping v3 row-lineage MV incremental DELETE test: object store unavailable on create: {err}"
                );
                return;
            }
            panic!("create materialized view: {err}");
        }

        if let Err(err) =
            session.execute_in_database("refresh materialized view agg_mv", "analytics")
        {
            if is_unavailable_object_store_error(&err) {
                eprintln!(
                    "skipping v3 row-lineage MV incremental DELETE test: object store unavailable on full refresh: {err}"
                );
                return;
            }
            panic!("first (full) refresh materialized view: {err}");
        }

        let pre_state = match collect_agg_mv_state(&session) {
            Ok(rows) => rows,
            Err(err) => {
                if is_unavailable_object_store_error(&err) {
                    eprintln!(
                        "skipping v3 row-lineage MV incremental DELETE test: object store unavailable on pre-delete select: {err}"
                    );
                    return;
                }
                panic!("select pre-delete agg_mv state: {err}");
            }
        };
        assert_eq!(
            pre_state,
            vec![
                ("A".to_string(), 2_i64, 30_i64),
                ("B".to_string(), 1_i64, 30_i64),
            ],
            "row-lineage MV state after full refresh must reflect the 3 seeded base rows"
        );

        // Trigger a DELETE — this writes a Puffin DV (Phase 2a RowDeltaDvCommit).
        session
            .execute_in_database("delete from ice.ns.orders where id = 1", "default")
            .expect("delete base row id=1 (writes puffin dv)");

        if let Err(err) =
            session.execute_in_database("refresh materialized view agg_mv", "analytics")
        {
            if is_unavailable_object_store_error(&err) {
                eprintln!(
                    "skipping v3 row-lineage MV incremental DELETE test: object store unavailable on incremental refresh: {err}"
                );
                return;
            }
            panic!("second (incremental, DV-bearing) refresh materialized view: {err}");
        }

        let post_state = match collect_agg_mv_state(&session) {
            Ok(rows) => rows,
            Err(err) => {
                if is_unavailable_object_store_error(&err) {
                    eprintln!(
                        "skipping v3 row-lineage MV incremental DELETE test: object store unavailable on post-delete select: {err}"
                    );
                    return;
                }
                panic!("select post-delete agg_mv state: {err}");
            }
        };
        assert_eq!(
            post_state,
            vec![
                ("A".to_string(), 1_i64, 20_i64),
                ("B".to_string(), 1_i64, 30_i64),
            ],
            "row-lineage MV state after DV-bearing incremental refresh must drop \
             count and sum for the affected group; other groups untouched"
        );

        drop(engine);
    }

    #[test]
    fn aggregate_mv_incremental_refresh_handles_s3_v3_row_lineage_base_delete() {
        // Same delete-bearing aggregate IVM path as
        // `aggregate_mv_incremental_refresh_handles_v3_row_lineage_base_delete`,
        // but the base Iceberg table itself lives in the MinIO-backed S3 catalog.
        // This covers object-store path normalization in delete reverse projection.
        let _runtime_guard = lock_runtime_test_state();
        let Some((_config_dir, config_path)) = maybe_managed_lake_config_path() else {
            return;
        };
        let iceberg_warehouse = unique_s3_iceberg_warehouse("mv_s3_v3_delete");

        let engine = match crate::engine::StandaloneNovaRocks::open(
            crate::engine::StandaloneOptions {
                config_path: Some(config_path),
            },
        ) {
            Ok(engine) => engine,
            Err(err) => {
                if is_unavailable_object_store_error(&err) {
                    eprintln!(
                        "skipping s3 v3 row-lineage MV incremental DELETE test: object store unavailable: {err}"
                    );
                    return;
                }
                panic!("open standalone engine: {err}");
            }
        };
        let session = engine.session();
        session
            .execute_in_database(
                &create_s3_iceberg_catalog_sql("ice", &iceberg_warehouse),
                "default",
            )
            .expect("create s3 iceberg catalog");
        session
            .execute_in_database("create database ice.ns", "default")
            .expect("create iceberg namespace");
        session
            .execute_in_database(
                r#"create table ice.ns.orders (id bigint not null, customer string, amount bigint) tblproperties("format-version"="3","write.row-lineage"="true")"#,
                "default",
            )
            .expect("create s3 row-lineage iceberg orders table");
        session
            .execute_in_database(
                "insert into ice.ns.orders values (1, 'A', 10), (2, 'A', 20), (3, 'B', 30)",
                "default",
            )
            .expect("seed s3 iceberg base rows");

        session
            .execute_in_database("create database analytics", "default")
            .expect("create analytics database");
        if let Err(err) = session.execute_in_database(
            "create materialized view agg_mv \
             distributed by hash(customer) buckets 2 \
             as select customer, count(*) as c, sum(amount) as s \
             from ice.ns.orders group by customer",
            "analytics",
        ) {
            if is_unavailable_object_store_error(&err) {
                eprintln!(
                    "skipping s3 v3 row-lineage MV incremental DELETE test: object store unavailable on create: {err}"
                );
                return;
            }
            panic!("create materialized view: {err}");
        }

        if let Err(err) =
            session.execute_in_database("refresh materialized view agg_mv", "analytics")
        {
            if is_unavailable_object_store_error(&err) {
                eprintln!(
                    "skipping s3 v3 row-lineage MV incremental DELETE test: object store unavailable on full refresh: {err}"
                );
                return;
            }
            panic!("first (full) refresh materialized view: {err}");
        }

        let pre_state = match collect_agg_mv_state(&session) {
            Ok(rows) => rows,
            Err(err) => {
                if is_unavailable_object_store_error(&err) {
                    eprintln!(
                        "skipping s3 v3 row-lineage MV incremental DELETE test: object store unavailable on pre-delete select: {err}"
                    );
                    return;
                }
                panic!("select pre-delete agg_mv state: {err}");
            }
        };
        assert_eq!(
            pre_state,
            vec![
                ("A".to_string(), 2_i64, 30_i64),
                ("B".to_string(), 1_i64, 30_i64),
            ],
            "s3 row-lineage MV state after full refresh must reflect the seeded base rows"
        );

        session
            .execute_in_database("delete from ice.ns.orders where id = 1", "default")
            .expect("delete s3 base row id=1 (writes puffin dv)");

        if let Err(err) =
            session.execute_in_database("refresh materialized view agg_mv", "analytics")
        {
            if is_unavailable_object_store_error(&err) {
                eprintln!(
                    "skipping s3 v3 row-lineage MV incremental DELETE test: object store unavailable on incremental refresh: {err}"
                );
                return;
            }
            panic!("second (incremental, s3 DV-bearing) refresh materialized view: {err}");
        }

        let post_state = match collect_agg_mv_state(&session) {
            Ok(rows) => rows,
            Err(err) => {
                if is_unavailable_object_store_error(&err) {
                    eprintln!(
                        "skipping s3 v3 row-lineage MV incremental DELETE test: object store unavailable on post-delete select: {err}"
                    );
                    return;
                }
                panic!("select post-delete agg_mv state: {err}");
            }
        };
        assert_eq!(
            post_state,
            vec![
                ("A".to_string(), 1_i64, 20_i64),
                ("B".to_string(), 1_i64, 30_i64),
            ],
            "s3 row-lineage MV state after DV-bearing incremental refresh must retract the deleted row"
        );

        drop(engine);
    }

    /// Read the managed-lake aggregate MV's visible state as
    /// `(customer, c, s)` rows sorted by customer. Used by the
    /// end-to-end `aggregate_mv_incremental_refresh_handles_base_delete`
    /// test to verify state before and after each refresh.
    fn collect_agg_mv_state(
        session: &crate::engine::StandaloneSession,
    ) -> Result<Vec<(String, i64, i64)>, String> {
        let result = session.execute_in_context(
            "select customer, c, s from agg_mv order by customer",
            None,
            "analytics",
            None,
        )?;
        let crate::engine::StatementResult::Query(query_result) = result else {
            return Err("select from agg_mv must return rows".to_string());
        };
        let mut out = Vec::new();
        for chunk in &query_result.chunks {
            let customers = chunk
                .batch
                .column(0)
                .as_any()
                .downcast_ref::<arrow::array::StringArray>()
                .ok_or_else(|| "agg_mv customer column not Utf8".to_string())?;
            let counts = chunk
                .batch
                .column(1)
                .as_any()
                .downcast_ref::<Int64Array>()
                .ok_or_else(|| "agg_mv count column not Int64".to_string())?;
            let sums = chunk
                .batch
                .column(2)
                .as_any()
                .downcast_ref::<Int64Array>()
                .ok_or_else(|| "agg_mv sum column not Int64".to_string())?;
            for i in 0..chunk.batch.num_rows() {
                out.push((
                    customers.value(i).to_string(),
                    counts.value(i),
                    sums.value(i),
                ));
            }
        }
        Ok(out)
    }

    fn show_mv_last_refresh_rows(
        session: &crate::engine::StandaloneSession,
        mv_name: &str,
    ) -> Result<Option<i64>, String> {
        let result = session.execute_in_context(
            "show materialized views from analytics",
            None,
            "analytics",
            None,
        )?;
        let crate::engine::StatementResult::Query(query_result) = result else {
            return Err("show materialized views must return rows".to_string());
        };
        for chunk in &query_result.chunks {
            let names = chunk
                .batch
                .column(0)
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| "SHOW MATERIALIZED VIEWS Name column not Utf8".to_string())?;
            let refresh_rows = chunk
                .batch
                .column(5)
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| {
                    "SHOW MATERIALIZED VIEWS LastRefreshRows column not Utf8".to_string()
                })?;
            for row in 0..chunk.batch.num_rows() {
                if names.value(row).eq_ignore_ascii_case(mv_name) {
                    if refresh_rows.is_null(row) {
                        return Ok(None);
                    }
                    return refresh_rows.value(row).parse::<i64>().map(Some).map_err(|e| {
                        format!(
                            "SHOW MATERIALIZED VIEWS LastRefreshRows value `{}` is not i64: {e}",
                            refresh_rows.value(row)
                        )
                    });
                }
            }
        }
        Err(format!(
            "SHOW MATERIALIZED VIEWS did not return `{mv_name}`"
        ))
    }

    /// Build a TOML config file that points the standalone server at
    /// the minio endpoint identified by `maybe_object_store_config_for_mv_refresh`,
    /// plus a per-test sqlite metadata-db path. Returns `None` when minio
    /// is unreachable (the caller skips). Mirrors `engine::tests::maybe_managed_lake_config`.
    fn maybe_managed_lake_config_path() -> Option<(tempfile::TempDir, std::path::PathBuf)> {
        let endpoint = std::env::var("AWS_S3_ENDPOINT")
            .unwrap_or_else(|_| "http://127.0.0.1:9000".to_string());
        if !managed_lake_endpoint_reachable(&endpoint) {
            eprintln!(
                "skipping MV integration test: object store endpoint is unreachable: {endpoint}"
            );
            return None;
        }
        let access_key_id = std::env::var("AWS_S3_ACCESS_KEY_ID")
            .or_else(|_| std::env::var("MINIO_ROOT_USER"))
            .unwrap_or_else(|_| "admin".to_string());
        let access_key_secret = std::env::var("AWS_S3_SECRET_ACCESS_KEY")
            .or_else(|_| std::env::var("MINIO_ROOT_PASSWORD"))
            .unwrap_or_else(|_| "admin123".to_string());
        let bucket = std::env::var("AWS_S3_BUCKET").unwrap_or_else(|_| "novarocks".to_string());
        let run_id = format!(
            "mv_refresh_delete_{}_{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|duration| duration.as_nanos())
                .unwrap_or(0)
        );
        let warehouse_uri = format!("s3://{bucket}/{run_id}");

        let dir = tempfile::TempDir::new().expect("create managed lake config dir");
        let metadata_dir = dir.path().join("meta");
        std::fs::create_dir_all(&metadata_dir).expect("create metadata dir");
        let config_path = dir.path().join("novarocks.toml");
        std::fs::write(
            &config_path,
            format!(
                r#"[metadata]
provider = "sqlite"
path = "meta/standalone.sqlite"

[standalone_server]
user = "root"
warehouse_uri = "{warehouse_uri}"

[standalone_server.object_store]
endpoint = "{endpoint}"
access_key_id = "{access_key_id}"
access_key_secret = "{access_key_secret}"
enable_path_style_access = true
"#
            ),
        )
        .expect("write standalone config");
        Some((dir, config_path))
    }

    fn s3_test_value(primary: &str, fallback: &str, default: &str) -> String {
        std::env::var(primary)
            .or_else(|_| std::env::var(fallback))
            .unwrap_or_else(|_| default.to_string())
    }

    fn unique_s3_iceberg_warehouse(prefix: &str) -> String {
        let bucket = std::env::var("AWS_S3_BUCKET").unwrap_or_else(|_| "novarocks".to_string());
        let root =
            std::env::var("AWS_S3_ROOT").unwrap_or_else(|_| "codex-managed-lake-tests".to_string());
        let run_id = format!(
            "{prefix}_{}_{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|duration| duration.as_nanos())
                .unwrap_or(0)
        );
        if root.trim_matches('/').is_empty() {
            format!("s3://{bucket}/{run_id}")
        } else {
            format!("s3://{}/{}/{}", bucket, root.trim_matches('/'), run_id)
        }
    }

    fn create_s3_iceberg_catalog_sql(catalog_name: &str, warehouse_uri: &str) -> String {
        let endpoint = std::env::var("AWS_S3_ENDPOINT")
            .unwrap_or_else(|_| "http://127.0.0.1:9000".to_string());
        let access_key_id = s3_test_value("AWS_S3_ACCESS_KEY_ID", "MINIO_ROOT_USER", "admin");
        let access_key_secret = s3_test_value(
            "AWS_S3_SECRET_ACCESS_KEY",
            "MINIO_ROOT_PASSWORD",
            "admin123",
        );
        format!(
            r#"create external catalog {catalog_name} properties("type"="iceberg","iceberg.catalog.type"="hadoop","iceberg.catalog.warehouse"="{warehouse_uri}","aws.s3.endpoint"="{endpoint}","aws.s3.access_key"="{access_key_id}","aws.s3.secret_key"="{access_key_secret}","aws.s3.enable_path_style_access"="true","aws.s3.region"="us-east-1")"#
        )
    }

    #[test]
    fn collect_current_snapshots_cleans_staged_partition_on_failure() {
        let (_dir, state, table_id) = seed_mv_refresh_state();
        let runtime = {
            let managed = state.managed_lake.read().expect("managed lake");
            managed
                .table("analytics", "orders_mv")
                .expect("runtime")
                .clone()
        };
        let staged = stage_managed_mv_refresh_partition(
            &state,
            &runtime,
            "p0",
            &test_managed_config().warehouse_uri,
        )
        .expect("stage");
        let refs = vec![IcebergTableRef {
            catalog: "missing_catalog".to_string(),
            namespace: "ns".to_string(),
            table: "orders".to_string(),
        }];

        let err = collect_current_base_metadata_or_cleanup_staged_partition(
            &state, table_id, &staged, &refs,
        )
        .expect_err("snapshot collection should fail");

        assert!(
            err.contains("mv refresh snapshot collection failed"),
            "err={err}"
        );
        let provider = state.metadata_provider.as_ref().expect("metadata provider");
        let read = provider.begin_read().expect("read");
        let loaded = state
            .managed_repo
            .load_snapshot(read.as_ref())
            .expect("reload");
        assert!(
            !loaded
                .partitions
                .iter()
                .any(|partition| partition.partition_id == staged.partition_id)
        );
        let erase_job = state
            .job_repo
            .list_runnable_erase_jobs(read.as_ref(), super::super::mv_ddl::now_ms())
            .expect("erase jobs")
            .into_iter()
            .find(|job| job.partition_id == Some(staged.partition_id))
            .expect("staged partition erase job");
        assert_eq!(erase_job.table_id, table_id);
    }

    #[test]
    fn refresh_mv_clears_stale_progress_before_retry() {
        let (_dir, state, table_id) = seed_mv_refresh_state();
        let mut target = BTreeMap::new();
        target.insert("missing_catalog.ns.orders".to_string(), 99);
        begin_mv_refresh_intent(&state, table_id, target).expect("begin stale refresh");

        let err = refresh_mv(
            &state,
            None,
            "analytics",
            &RefreshMaterializedViewStmt {
                name: ObjectName {
                    parts: vec!["orders_mv".to_string()],
                },
            },
        )
        .expect_err("missing iceberg catalog should fail after stale progress cleanup");
        assert!(err.contains("missing_catalog"), "err={err}");

        let provider = state.metadata_provider.as_ref().expect("metadata provider");
        let read = provider.begin_read().expect("read");
        let mv = state
            .mv_repo
            .load_by_id(read.as_ref(), table_id)
            .expect("load mv")
            .expect("mv definition");
        assert!(!mv.refresh_in_progress);
        assert!(mv.refresh_target_snapshots.is_empty());
    }

    #[test]
    fn lock_mv_refresh_mutex_reports_poisoned_lock() {
        let lock: &'static std::sync::Mutex<()> = Box::leak(Box::new(std::sync::Mutex::new(())));
        static PANIC_HOOK_LOCK: OnceLock<Mutex<()>> = OnceLock::new();
        let _hook_guard = PANIC_HOOK_LOCK
            .get_or_init(|| Mutex::new(()))
            .lock()
            .expect("panic hook lock");
        let old_hook = std::panic::take_hook();
        std::panic::set_hook(Box::new(|_| {}));
        let poison_result = std::panic::catch_unwind(|| {
            let _guard = lock.lock().expect("lock");
            panic!("poison test lock");
        });
        std::panic::set_hook(old_hook);
        assert!(poison_result.is_err());

        let err = lock_mv_refresh_mutex(lock).expect_err("poisoned lock should fail");
        assert!(err.contains("poisoned"), "err={err}");
    }

    fn seed_mv_refresh_state() -> (tempfile::TempDir, Arc<StandaloneState>, i64) {
        let dir = tempfile::tempdir().expect("tempdir");
        let metadata_path = dir.path().join("standalone.sqlite");
        let provider =
            crate::meta::SqliteMetaStoreProvider::open(&metadata_path).expect("open meta provider");
        let config = test_managed_config();
        let managed_repo =
            crate::meta::repository::managed_lake::ManagedLakeMetaRepository::default();
        let mv_repo = crate::meta::repository::mv::MvMetaRepository::default();
        let table_id = {
            let mut txn = provider
                .begin_write("seed mv refresh state")
                .expect("write");
            let database = managed_repo
                .get_or_create_database(txn.as_mut(), "analytics")
                .expect("database");
            let created = managed_repo
                .create_table_layout(
                    txn.as_mut(),
                    crate::meta::repository::managed_lake::CreateManagedTableLayoutRequest {
                        db_id: database.db_id,
                        table_name: "orders_mv".to_string(),
                        keys_type: "DUP_KEYS".to_string(),
                        bucket_num: 2,
                        kind: crate::meta::repository::managed_lake::ManagedTableKind::MaterializedView,
                        schema_version: 0,
                        tablet_schema_pb: crate::service::grpc_client::proto::starrocks::TabletSchemaPb::default()
                            .encode_to_vec(),
                        columns: vec![crate::meta::repository::managed_lake::CreateManagedColumnRequest {
                            column_name: "id".to_string(),
                            logical_type: "INT".to_string(),
                            nullable: false,
                            visible: true,
                            is_key: true,
                        }],
                        partition_name: "p0".to_string(),
                        warehouse_uri: config.warehouse_uri.clone(),
                    },
                )
                .expect("managed mv layout");
            mv_repo
                .create_definition_with_id(
                    txn.as_mut(),
                    created.table.table_id,
                    crate::meta::repository::mv::CreateMvDefinitionRequest {
                        select_sql: "SELECT id FROM missing_catalog.ns.orders".to_string(),
                        base_table_refs: vec!["missing_catalog.ns.orders".to_string()],
                        primary_key_columns: Vec::new(),
                        storage_engine: ManagedMvStorageEngine::ManagedLake
                            .as_sql_str()
                            .to_string(),
                        target_catalog: None,
                        target_namespace: None,
                        target_table: None,
                        created_at_ms: super::super::mv_ddl::now_ms(),
                    },
                )
                .expect("mv definition");
            txn.commit().expect("commit seed");
            created.table.table_id
        };
        let read = provider.begin_read().expect("read");
        let snapshot = managed_repo
            .load_snapshot(read.as_ref())
            .expect("load managed snapshot");
        let managed = ManagedLakeCatalog::rebuild_from_repository(Some(config.clone()), snapshot)
            .expect("rebuild managed catalog");
        let state = Arc::new(StandaloneState {
            catalog: RwLock::new(InMemoryCatalog::default()),
            iceberg_catalogs: Arc::new(RwLock::new(IcebergCatalogRegistry::default())),
            managed_lake: RwLock::new(managed),
            statistics: RwLock::new(crate::engine::statistics::StandaloneStatistics::default()),
            connectors: Arc::new(RwLock::new(crate::connector::ConnectorRegistry::default())),
            managed_lake_config: Some(config),
            metadata_provider: Some(Arc::new(provider)),
            exchange_port: 0,
            ..Default::default()
        });
        (dir, state, table_id)
    }

    fn seed_aggregate_mv_refresh_state(
        config: ManagedLakeConfig,
    ) -> Result<
        (
            tempfile::TempDir,
            Arc<StandaloneState>,
            super::super::mv_shape::AggregateMvShape,
        ),
        String,
    > {
        let metadata_dir = tempfile::tempdir().map_err(|e| format!("tempdir failed: {e}"))?;
        let metadata_path = metadata_dir.path().join("standalone.sqlite");
        let provider = crate::meta::SqliteMetaStoreProvider::open(&metadata_path)
            .map_err(|e| format!("open meta provider failed: {e}"))?;
        let shape = aggregate_mv_shape()?;
        let output_columns = aggregate_output_columns();
        let layout =
            super::super::mv_agg_state::build_aggregate_mv_layout(&shape, &output_columns)?;
        let key_desc = TableKeyDesc {
            kind: TableKeyKind::Primary,
            columns: vec![super::super::mv_agg_state::ROW_ID_COLUMN.to_string()],
        };
        let table_columns = table_columns_from_physical_columns(&layout.physical_columns);
        let request_schema = build_tablet_schema(&table_columns, &key_desc, 100)?;
        let mut tablet_schema = build_tablet_schema_pb_from_thrift(&request_schema)?;
        patch_tablet_schema_column_flags(&mut tablet_schema, &layout.physical_columns)?;
        let stored_columns =
            stored_columns_from_physical_columns(100, &key_desc, &layout.physical_columns);
        let active_root = config.tablet_root_path(1, 10, 20);
        for tablet_id in [40_i64, 43_i64] {
            let request = build_create_tablet_request(tablet_id, 10, 20, request_schema.clone());
            create_lake_tablet_from_req_with_schema_patch(
                &request,
                &active_root,
                Some(config.s3.clone()),
                |schema| patch_tablet_schema_column_flags(schema, &layout.physical_columns),
            )?;
        }

        let snapshot = ManagedSnapshot {
            global: ManagedGlobalMeta {
                warehouse_uri: config.warehouse_uri.clone(),
                next_db_id: 2,
                next_table_id: 11,
                next_partition_id: 21,
                next_index_id: 31,
                next_tablet_id: 41,
                next_txn_id: 100,
            },
            databases: vec![StoredManagedDatabase {
                db_id: 1,
                name: "analytics".to_string(),
            }],
            tables: vec![StoredManagedTable {
                table_id: 10,
                db_id: 1,
                name: "orders_mv".to_string(),
                keys_type: keys_type_name(TableKeyKind::Primary).to_string(),
                bucket_num: 2,
                current_schema_id: 100,
                state: ManagedTableState::Active,
                kind: ManagedTableKind::MaterializedView,
            }],
            schemas: vec![StoredManagedSchema {
                schema_id: 100,
                table_id: 10,
                schema_version: 0,
                tablet_schema_pb: tablet_schema.encode_to_vec(),
            }],
            columns: stored_columns,
            partitions: vec![StoredManagedPartition {
                partition_id: 20,
                table_id: 10,
                name: "p0".to_string(),
                visible_version: 1,
                next_version: 2,
                state: ManagedPartitionState::Active,
            }],
            indexes: vec![StoredManagedIndex {
                index_id: 30,
                table_id: 10,
                partition_id: 20,
                index_type: "BASE".to_string(),
                state: ManagedIndexState::Active,
            }],
            tablets: vec![
                StoredManagedTablet {
                    tablet_id: 40,
                    partition_id: 20,
                    index_id: 30,
                    bucket_seq: 0,
                    tablet_root_path: active_root.clone(),
                },
                StoredManagedTablet {
                    tablet_id: 43,
                    partition_id: 20,
                    index_id: 30,
                    bucket_seq: 1,
                    tablet_root_path: active_root,
                },
            ],
            txns: vec![],
            erase_jobs: vec![],
            materialized_views: vec![StoredMaterializedView {
                mv_id: 10,
                select_sql: aggregate_select_sql().to_string(),
                refresh_mode: ManagedMvRefreshMode::DeferredManual,
                base_table_refs: vec![],
                last_refresh_ms: None,
                last_refresh_rows: None,
                last_refresh_snapshots: BTreeMap::new(),
                last_refresh_table_uuids: Default::default(),
                primary_key_columns: Vec::new(),
                created_at_ms: 1,
                storage_engine: ManagedMvStorageEngine::ManagedLake,
                iceberg_table_identifier: None,
                target_catalog: None,
                target_namespace: None,
                target_table: None,
                last_refreshed_iceberg_snapshot_id: None,
                refresh_in_progress: false,
                refresh_target_snapshots: Default::default(),
            }],
        };
        seed_repository_snapshot_for_mv_refresh(&provider, &snapshot)?;
        let managed = ManagedLakeCatalog::rebuild(Some(config.clone()), snapshot)?;
        let mut catalog = InMemoryCatalog::default();
        catalog.create_database("analytics")?;
        register_managed_tables_in_catalog(&mut catalog, &managed)?;
        let state = Arc::new(StandaloneState {
            catalog: RwLock::new(catalog),
            iceberg_catalogs: Arc::new(RwLock::new(IcebergCatalogRegistry::default())),
            managed_lake: RwLock::new(managed),
            statistics: RwLock::new(crate::engine::statistics::StandaloneStatistics::default()),
            connectors: Arc::new(RwLock::new(crate::connector::ConnectorRegistry::default())),
            managed_lake_config: Some(config),
            metadata_provider: Some(Arc::new(provider)),
            exchange_port: 0,
            ..Default::default()
        });
        Ok((metadata_dir, state, shape))
    }

    fn seed_repository_snapshot_for_mv_refresh(
        provider: &crate::meta::SqliteMetaStoreProvider,
        snapshot: &ManagedSnapshot,
    ) -> Result<(), String> {
        let mut txn = provider
            .begin_write("seed mv refresh repositories")
            .map_err(|e| format!("begin seed repositories failed: {e}"))?;
        for database in &snapshot.databases {
            put_seed_record(
                txn.as_mut(),
                "managed",
                vec!["database".to_string(), database.db_id.to_string()],
                "managed.database",
                serde_json::json!({ "db_id": database.db_id, "name": database.name }),
            )?;
        }
        for table in &snapshot.tables {
            put_seed_record(
                txn.as_mut(),
                "managed",
                vec!["table".to_string(), table.table_id.to_string()],
                "managed.table",
                serde_json::json!({
                    "table_id": table.table_id,
                    "db_id": table.db_id,
                    "name": table.name,
                    "keys_type": table.keys_type,
                    "bucket_num": table.bucket_num,
                    "current_schema_id": table.current_schema_id,
                    "state": managed_table_state_sql(table.state),
                    "kind": managed_table_kind_sql(table.kind),
                }),
            )?;
        }
        for schema in &snapshot.schemas {
            put_seed_record(
                txn.as_mut(),
                "managed",
                vec!["schema".to_string(), schema.schema_id.to_string()],
                "managed.schema",
                serde_json::json!({
                    "schema_id": schema.schema_id,
                    "table_id": schema.table_id,
                    "schema_version": schema.schema_version,
                    "tablet_schema_pb": schema.tablet_schema_pb,
                }),
            )?;
        }
        for column in &snapshot.columns {
            put_seed_record(
                txn.as_mut(),
                "managed",
                vec![
                    "column".to_string(),
                    column.schema_id.to_string(),
                    column.ordinal.to_string(),
                ],
                "managed.column",
                serde_json::json!({
                    "schema_id": column.schema_id,
                    "ordinal": column.ordinal,
                    "column_name": column.column_name,
                    "logical_type": column.logical_type,
                    "nullable": column.nullable,
                    "visible": column.visible,
                    "is_key": column.is_key,
                }),
            )?;
        }
        for partition in &snapshot.partitions {
            put_seed_record(
                txn.as_mut(),
                "managed",
                vec!["partition".to_string(), partition.partition_id.to_string()],
                "managed.partition",
                serde_json::json!({
                    "partition_id": partition.partition_id,
                    "table_id": partition.table_id,
                    "name": partition.name,
                    "visible_version": partition.visible_version,
                    "next_version": partition.next_version,
                    "state": managed_partition_state_sql(partition.state),
                }),
            )?;
        }
        for index in &snapshot.indexes {
            put_seed_record(
                txn.as_mut(),
                "managed",
                vec!["index".to_string(), index.index_id.to_string()],
                "managed.index",
                serde_json::json!({
                    "index_id": index.index_id,
                    "table_id": index.table_id,
                    "partition_id": index.partition_id,
                    "index_type": index.index_type,
                    "state": managed_index_state_sql(index.state),
                }),
            )?;
        }
        for tablet in &snapshot.tablets {
            put_seed_record(
                txn.as_mut(),
                "managed",
                vec!["tablet".to_string(), tablet.tablet_id.to_string()],
                "managed.tablet",
                serde_json::json!({
                    "tablet_id": tablet.tablet_id,
                    "partition_id": tablet.partition_id,
                    "index_id": tablet.index_id,
                    "bucket_seq": tablet.bucket_seq,
                    "tablet_root_path": tablet.tablet_root_path,
                }),
            )?;
        }
        for mv in &snapshot.materialized_views {
            crate::meta::repository::mv::MvMetaRepository::default()
                .create_definition_with_id(
                    txn.as_mut(),
                    mv.mv_id,
                    crate::meta::repository::mv::CreateMvDefinitionRequest {
                        select_sql: mv.select_sql.clone(),
                        base_table_refs: mv.base_table_refs.iter().map(|r| r.fqn()).collect(),
                        primary_key_columns: mv.primary_key_columns.clone(),
                        storage_engine: mv.storage_engine.as_sql_str().to_string(),
                        target_catalog: mv.target_catalog.clone(),
                        target_namespace: mv.target_namespace.clone(),
                        target_table: mv.target_table.clone(),
                        created_at_ms: mv.created_at_ms,
                    },
                )
                .map_err(|e| format!("seed mv definition failed: {e}"))?;
        }
        txn.commit()
            .map_err(|e| format!("commit seed repositories failed: {e}"))?;
        Ok(())
    }

    fn put_seed_record(
        txn: &mut dyn crate::meta::MetaWriteTxn,
        namespace: &str,
        path: Vec<String>,
        kind: &str,
        payload: serde_json::Value,
    ) -> Result<(), String> {
        txn.put(crate::meta::MetaRecordPut::new(
            crate::meta::MetaKey::new(namespace, path).map_err(|e| e.to_string())?,
            crate::meta::MetaRecordKind::new(kind).map_err(|e| e.to_string())?,
            crate::meta::ExpectedRevision::NotExists,
            crate::meta::repository::encode_json_payload(1, &payload).map_err(|e| e.to_string())?,
        ))
        .map_err(|e| e.to_string())
    }

    fn managed_table_state_sql(state: ManagedTableState) -> &'static str {
        match state {
            ManagedTableState::Creating => "CREATING",
            ManagedTableState::Active => "ACTIVE",
            ManagedTableState::Dropping => "DROPPING",
            ManagedTableState::Failed => "FAILED",
        }
    }

    fn managed_table_kind_sql(kind: ManagedTableKind) -> &'static str {
        match kind {
            ManagedTableKind::Table => "TABLE",
            ManagedTableKind::MaterializedView => "MATERIALIZED_VIEW",
        }
    }

    fn managed_partition_state_sql(state: ManagedPartitionState) -> &'static str {
        match state {
            ManagedPartitionState::Creating => "CREATING",
            ManagedPartitionState::Active => "ACTIVE",
            ManagedPartitionState::Retired => "RETIRED",
            ManagedPartitionState::Failed => "FAILED",
        }
    }

    fn managed_index_state_sql(state: ManagedIndexState) -> &'static str {
        match state {
            ManagedIndexState::Creating => "CREATING",
            ManagedIndexState::Active => "ACTIVE",
            ManagedIndexState::Retired => "RETIRED",
            ManagedIndexState::Failed => "FAILED",
        }
    }

    fn aggregate_select_sql() -> &'static str {
        "SELECT k1, count(*) AS c, sum(v2) AS s FROM ice.ns.orders GROUP BY k1"
    }

    fn aggregate_mv_shape() -> Result<super::super::mv_shape::AggregateMvShape, String> {
        let super::super::mv_shape::IncrementalMvShape::Aggregate(shape) =
            validate_incremental_mv_select(aggregate_select_sql())?
        else {
            return Err("expected aggregate MV shape".to_string());
        };
        Ok(shape)
    }

    fn aggregate_output_columns() -> Vec<OutputColumn> {
        vec![
            OutputColumn {
                name: "k1".to_string(),
                data_type: DataType::Int64,
                nullable: false,
            },
            OutputColumn {
                name: "c".to_string(),
                data_type: DataType::Int64,
                nullable: false,
            },
            OutputColumn {
                name: "s".to_string(),
                data_type: DataType::Int64,
                nullable: true,
            },
        ]
    }

    fn aggregate_visible_query_result() -> Result<QueryResult, String> {
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("k1", DataType::Int64, false),
                Field::new("c", DataType::Int64, false),
                Field::new("s", DataType::Int64, true),
            ])),
            vec![
                Arc::new(Int64Array::from(vec![1_i64, 2])),
                Arc::new(Int64Array::from(vec![3_i64, 4])),
                Arc::new(Int64Array::from(vec![30_i64, 40])),
            ],
        )
        .map_err(|e| format!("build aggregate visible batch failed: {e}"))?;
        Ok(QueryResult {
            columns: vec![
                QueryResultColumn {
                    name: "k1".to_string(),
                    data_type: DataType::Int64,
                    nullable: false,
                    logical_type: None,
                },
                QueryResultColumn {
                    name: "c".to_string(),
                    data_type: DataType::Int64,
                    nullable: false,
                    logical_type: None,
                },
                QueryResultColumn {
                    name: "s".to_string(),
                    data_type: DataType::Int64,
                    nullable: true,
                    logical_type: None,
                },
            ],
            chunks: vec![record_batch_to_chunk(batch)?],
        })
    }

    fn maybe_object_store_config_for_mv_refresh() -> Option<ManagedLakeConfig> {
        let endpoint = std::env::var("AWS_S3_ENDPOINT")
            .unwrap_or_else(|_| "http://127.0.0.1:9000".to_string());
        if !managed_lake_endpoint_reachable(&endpoint) {
            eprintln!(
                "skipping aggregate MV full refresh writer test: object store endpoint is unreachable: {endpoint}"
            );
            return None;
        }
        let access_key_id = std::env::var("AWS_S3_ACCESS_KEY_ID")
            .or_else(|_| std::env::var("MINIO_ROOT_USER"))
            .unwrap_or_else(|_| "admin".to_string());
        let access_key_secret = std::env::var("AWS_S3_SECRET_ACCESS_KEY")
            .or_else(|_| std::env::var("MINIO_ROOT_PASSWORD"))
            .unwrap_or_else(|_| "admin123".to_string());
        let bucket = std::env::var("AWS_S3_BUCKET").unwrap_or_else(|_| "novarocks".to_string());
        let root = format!(
            "novarocks-mv-refresh-test-{}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|duration| duration.as_nanos())
                .unwrap_or(0)
        );
        Some(ManagedLakeConfig {
            warehouse_uri: format!("s3://{bucket}/{root}"),
            s3: S3StoreConfig {
                endpoint,
                bucket,
                root,
                access_key_id,
                access_key_secret,
                region: Some("us-east-1".to_string()),
                enable_path_style_access: Some(true),
            },
            mv_default_storage_engine: "managed_lake".to_string(),
        })
    }

    fn managed_lake_endpoint_reachable(endpoint: &str) -> bool {
        let stripped = endpoint
            .split_once("://")
            .map(|(_, rest)| rest)
            .unwrap_or(endpoint);
        let authority = stripped.split('/').next().unwrap_or(stripped);
        let (host, port) = match authority.rsplit_once(':') {
            Some((host, port)) => match port.parse::<u16>() {
                Ok(port) => (host, port),
                Err(_) => return false,
            },
            None => {
                let default_port = if endpoint.starts_with("https://") {
                    443
                } else {
                    80
                };
                (authority, default_port)
            }
        };
        let Ok(addrs) = (host, port).to_socket_addrs() else {
            return false;
        };
        addrs
            .into_iter()
            .any(|addr| std::net::TcpStream::connect_timeout(&addr, Duration::from_secs(1)).is_ok())
    }

    fn is_unavailable_object_store_error(err: &str) -> bool {
        err.contains("NoSuchBucket")
            || err.contains("Connection refused")
            || err.contains("connection refused")
            || err.contains("deadline has elapsed")
            || err.contains("timeout")
            || err.contains("timed out")
    }

    fn test_managed_config() -> ManagedLakeConfig {
        ManagedLakeConfig {
            warehouse_uri: "s3://test/warehouse".to_string(),
            s3: S3StoreConfig {
                endpoint: "http://127.0.0.1:9000".to_string(),
                bucket: "test".to_string(),
                root: "warehouse".to_string(),
                access_key_id: "ak".to_string(),
                access_key_secret: "sk".to_string(),
                region: Some("us-east-1".to_string()),
                enable_path_style_access: Some(true),
            },
            mv_default_storage_engine: "managed_lake".to_string(),
        }
    }

    /// Build a minimal `StandaloneState` with a catalog entry for
    /// `ns.orders(k BIGINT NOT NULL, v BIGINT)` so that
    /// `analyze_visible_output_types` can resolve the table schema.
    ///
    /// The `TableStorage` path is unused during analysis-only calls.
    fn state_with_orders_table() -> Arc<crate::engine::StandaloneState> {
        use crate::sql::catalog::{ColumnDef, TableDef, TableStorage};
        let state = Arc::new(crate::engine::StandaloneState::default());
        {
            let mut catalog = state.catalog.write().expect("catalog write lock");
            catalog.create_database("ns").expect("create ns database");
            catalog
                .register(
                    "ns",
                    TableDef {
                        name: "orders".to_string(),
                        columns: vec![
                            ColumnDef {
                                name: "k".to_string(),
                                data_type: DataType::Int64,
                                nullable: false,
                                write_default: None,
                            },
                            ColumnDef {
                                name: "v".to_string(),
                                data_type: DataType::Int64,
                                nullable: false,
                                write_default: None,
                            },
                        ],
                        storage: TableStorage::LocalParquetFile {
                            path: std::path::PathBuf::from("/unused/for/analysis"),
                        },
                        iceberg_row_lineage_metadata_columns: vec![],
                        iceberg_table: None,
                    },
                )
                .expect("register orders table");
        }
        state
    }

    /// `analyze_visible_output_types` on an AVG MV SQL must return visible-shaped
    /// columns (group key + Float64 avg result), not state-shaped columns
    /// (group key + sum + count). Verifies the fix for the layout length-mismatch
    /// blocker: `build_aggregate_mv_layout` expects visible-shaped types.
    #[test]
    fn analyze_visible_output_types_for_avg_mv_returns_visible_shape() {
        let state = state_with_orders_table();

        // Original SQL: SELECT k, AVG(v) AS a FROM ice.ns.orders GROUP BY k
        // Visible output: 2 columns — k (Int64) + a (Float64).
        // State-shaped output (after rewrite): 3 columns — k + __sum + __count.
        let sql = "SELECT k, AVG(v) AS a FROM ice.ns.orders GROUP BY k";
        let columns =
            analyze_visible_output_types(&state, "ns", sql).expect("analyze visible output types");

        assert_eq!(
            columns.len(),
            2,
            "AVG MV must produce 2 visible columns (group key + avg result), \
             not the 3 state-shaped columns (group key + sum + count); \
             got: {:?}",
            columns
                .iter()
                .map(|c| (&c.name, &c.data_type))
                .collect::<Vec<_>>()
        );
        assert_eq!(
            columns[0].name, "k",
            "first visible column must be the group key"
        );
        assert_eq!(
            columns[0].data_type,
            DataType::Int64,
            "group key must keep its original Int64 type"
        );
        assert_eq!(
            columns[1].name, "a",
            "second visible column must be the avg alias"
        );
        // The analyzer returns Float64 for AVG over an integer column.
        assert_eq!(
            columns[1].data_type,
            DataType::Float64,
            "AVG visible result must be Float64"
        );
    }

    /// COUNT/SUM-only MVs (no AVG) must still produce the correct visible-shaped
    /// columns via `analyze_visible_output_types`. Regression guard: the fix must
    /// not break non-AVG aggregate MVs.
    #[test]
    fn analyze_visible_output_types_for_count_sum_mv_returns_visible_shape() {
        let state = state_with_orders_table();

        let sql = "SELECT k, COUNT(*) AS c, SUM(v) AS s FROM ice.ns.orders GROUP BY k";
        let columns =
            analyze_visible_output_types(&state, "ns", sql).expect("analyze visible output types");

        assert_eq!(
            columns.len(),
            3,
            "COUNT+SUM MV must produce 3 visible columns (k, c, s); got: {:?}",
            columns
                .iter()
                .map(|c| (&c.name, &c.data_type))
                .collect::<Vec<_>>()
        );
        assert_eq!(columns[0].name, "k");
        assert_eq!(columns[0].data_type, DataType::Int64);
        assert_eq!(columns[1].name, "c");
        // COUNT returns Int64.
        assert_eq!(columns[1].data_type, DataType::Int64);
        assert_eq!(columns[2].name, "s");
        // SUM over Int64 returns Int64.
        assert_eq!(columns[2].data_type, DataType::Int64);
    }

    /// Validates the end-to-end fix: using visible types from the analyzer allows
    /// `build_aggregate_mv_layout` to succeed for an AVG MV, where the direct
    /// approach (sourcing types from the state-shaped executor result) would fail
    /// with a column-count mismatch.
    #[test]
    fn build_aggregate_mv_layout_succeeds_with_analyzer_sourced_visible_types() {
        use super::super::mv_agg_state::{
            AGG_RETRACTION_COUNT_STATE_COLUMN, AggregateStateRole, build_aggregate_mv_layout,
        };

        let state = state_with_orders_table();

        let sql = "SELECT k, AVG(v) AS a FROM ice.ns.orders GROUP BY k";
        let shape = match validate_incremental_mv_select(sql).expect("validate mv select") {
            super::super::mv_shape::IncrementalMvShape::Aggregate(shape) => shape,
            _ => panic!("expected aggregate shape"),
        };

        // Visible output columns from the analyzer (2 columns: k + a).
        let visible_columns =
            analyze_visible_output_types(&state, "ns", sql).expect("analyze visible output types");
        assert_eq!(
            visible_columns.len(),
            shape.visible_outputs.len(),
            "analyzer output count must match shape.visible_outputs"
        );

        // Must succeed: visible-shaped types match what build_aggregate_mv_layout expects.
        let layout = build_aggregate_mv_layout(&shape, &visible_columns)
            .expect("build_aggregate_mv_layout must succeed with visible-shaped types");

        assert_eq!(
            layout.visible_columns.len(),
            2,
            "layout must have 2 visible columns (k + a)"
        );
        // AVG expands to 2 state columns, plus the hidden retraction count state
        // needed to drop fully retracted groups when the shape has no COUNT(*).
        assert_eq!(
            layout.state_columns.len(),
            3,
            "AVG must expand to 2 state columns plus hidden retraction count"
        );
        assert!(
            layout.state_columns[0].name.contains("__sum"),
            "first state column must be the sum sub-state"
        );
        assert!(
            layout.state_columns[1].name.contains("__count"),
            "second state column must be the count sub-state"
        );
        assert_eq!(
            layout.state_columns[2].state_role,
            AggregateStateRole::RetractionCount
        );
        assert_eq!(
            layout.state_columns[2].name,
            AGG_RETRACTION_COUNT_STATE_COLUMN
        );
    }
}
