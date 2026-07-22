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

use std::collections::{BTreeMap, HashSet};
use std::sync::{Arc, MutexGuard};

use arrow::array::{Array, ArrayRef, BooleanArray, Int8Array};
use arrow::compute::filter_record_batch;
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;

use crate::connector::iceberg::catalog::load_table;
use crate::connector::starrocks::ObjectStoreProfile;
use crate::connector::starrocks::lake::context::remove_tablet_runtime;
use crate::connector::starrocks::table::ivm_change_stream::plan_iceberg_change_batch_for_ivm;
use crate::connector::starrocks::table::ivm_delta_source::{
    IvmDeltaSourceInput, build_delta_source_files, execute_delta_source_query,
    projection_select_with_change_op,
};
use crate::connector::starrocks::table::mv_apply_policy::{MvApplyPolicy, apply_policy_for_change};
use crate::connector::starrocks::table::mv_refresh_strategy::{
    FullRefreshReason, MvRefreshPolicy, choose_snapshot_refresh_policy, policy_from_change_error,
};
use crate::engine::mv_flow::{
    analyze_visible_query, execute_query_for_mv_refresh, execute_query_for_mv_refresh_with_catalog,
};
use crate::engine::{StandaloneState, StatementResult};
use crate::exec::change_op::{CHANGE_OP_COLUMN, CHANGE_OP_DELETE, CHANGE_OP_INSERT};
use crate::exec::chunk::Chunk;
use crate::mv::analysis::resolve_mv_name;
use crate::runtime::query_result::{QueryResult, record_batch_to_chunk};
use crate::sql::parser::ast::RefreshMaterializedViewStmt;

use crate::connector::starrocks::table::catalog::{
    StarRocksTableCatalog, StarRocksTableRuntime, register_starrocks_tables_in_catalog,
};
use crate::connector::starrocks::table::config::StarRocksTableConfig;
use crate::connector::starrocks::table::ddl::bootstrap_empty_partition_for_tablets;
use crate::connector::starrocks::table::model::{
    StarRocksMvStorageEngine, StarRocksPartitionState, StarRocksTableKind,
};
use crate::connector::starrocks::table::schema_adapter::{
    build_create_tablet_request, request_schema_from_runtime,
};
use crate::connector::starrocks::table::txn::{
    MvRefreshWriteMetadata, PartitionTarget, load_insert_plan, load_physical_insert_plan,
    write_chunks_into_starrocks_partition,
    write_chunks_into_starrocks_partition_for_aggregate_mv_upsert,
    write_chunks_into_starrocks_partition_for_mv_refresh_with_row_delta,
};
use crate::meta::repository::job::CreateEraseJobRequest;
use crate::meta::repository::mv::UpdateStarRocksMvRefreshSummaryRequest;
use crate::meta::repository::starrocks_table::{
    StageStarRocksMvRefreshRequest, StagedStarRocksMvRefresh,
};
use crate::mv::persistence::definition::StoredMvDefinition;
use novarocks_catalog::identifier::TableIdentity;

pub(crate) fn refresh_mv(
    state: &Arc<StandaloneState>,
    _current_catalog: Option<&str>,
    current_database: &str,
    stmt: &RefreshMaterializedViewStmt,
) -> Result<StatementResult, String> {
    let (db_name, mv_name) = resolve_mv_name(&stmt.name, current_database)?;
    let _refresh_guard = acquire_mv_refresh_lock()?;

    if stmt.full {
        // REFRESH FULL is universally disabled pending redesign — see the
        // matching rejection in iceberg_refresh::refresh_iceberg_mv for the
        // rationale. Both backends produce the same error so behavior is
        // consistent regardless of MV storage engine.
        return Err(
            "REFRESH MATERIALIZED VIEW ... FULL is currently disabled pending redesign; \
             its previous behavior (drop target + delete definition + recreate empty target) \
             was misleading and non-atomic. To recover a corrupted MV, run \
             DROP MATERIALIZED VIEW <name>; CREATE MATERIALIZED VIEW <name> ...; \
             REFRESH MATERIALIZED VIEW <name>; manually."
                .to_string(),
        );
    }

    let runtime = {
        let starrocks = state
            .starrocks_table
            .read()
            .expect("standalone StarRocks table read lock");
        starrocks.table(&db_name, &mv_name)?.clone()
    };
    if runtime.table.kind != StarRocksTableKind::MaterializedView {
        return Err(format!("`{db_name}.{mv_name}` is not a materialized view"));
    }

    let mut mv_definition = load_mv_definition_by_id(state, runtime.table.table_id)?
        .ok_or_else(|| format!("materialized view {db_name}.{mv_name} has no MV definition"))?;
    if mv_definition
        .storage_engine
        .eq_ignore_ascii_case(StarRocksMvStorageEngine::Iceberg.as_sql_str())
    {
        return Err(
            "StarRocks table MV backend cannot refresh storage_engine='iceberg' materialized views"
                .to_string(),
        );
    }
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

    let pre_pin_loaded = load_current_iceberg_base_table(state, base_ref)?;
    let current_snapshot_id_before_pin = pre_pin_loaded
        .table
        .metadata()
        .current_snapshot()
        .map(|s| s.snapshot_id());
    let previous_snapshot_id = mv_definition
        .last_refresh_snapshots
        .get(&base_ref.fqn())
        .copied();
    if previous_snapshot_id.is_none() && current_snapshot_id_before_pin.is_none() {
        tracing::info!(
            "StarRocks table mv {}.{}: base table {} has no snapshot; skipping refresh",
            db_name,
            mv_name,
            base_ref.fqn()
        );
        return Ok(StatementResult::Ok);
    }

    // Freeze the snapshot pin for the duration of this refresh. From now on
    // pin is the only source of snapshot ids for base table reads, delta
    // computation, intent recording, and bookkeeping.
    let pin =
        crate::connector::starrocks::table::refresh_pin_adapter::capture_refresh_snapshot_pin(
            state, &base_refs,
        )?;
    let current_snapshot_id = pin.get(base_ref);
    let current_table_uuid = pin
        .uuid(base_ref)
        .ok_or_else(|| {
            format!(
                "refresh pin missing uuid for base {} (this should not happen)",
                base_ref.fqn()
            )
        })?
        .to_string();
    let loaded = load_current_iceberg_base_table(state, base_ref)?;
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
        begin_mv_refresh_intent(state, runtime.table.table_id, pin.to_snapshot_map())?;
    }

    let projection_apply_shape = mv_shape.clone();
    let projection_full_primary_key_columns = mv_definition.primary_key_columns.clone();
    let pinned_full_select_sql =
        rewrite_full_refresh_select_with_pin(&mv_definition.select_sql, &pin, base_ref)?;
    let pinned_base_metadata = current_base_metadata_from_pin(&pin);
    dispatch_mv_refresh_strategy(
        &mv_shape,
        policy,
        || {
            refresh_mv_full_with_pinned_executor(
                state,
                &db_name,
                &mv_name,
                pinned_full_select_sql.clone(),
                pinned_base_metadata.clone(),
                move |ctx| {
                    run_projection_mv_select_and_chunks(ctx, &projection_full_primary_key_columns)
                },
            )
        },
        |shape| {
            refresh_aggregate_mv_full_with_pinned_metadata(
                state,
                &db_name,
                &mv_name,
                shape,
                pinned_full_select_sql.clone(),
                pinned_base_metadata.clone(),
            )
        },
        |_current_snapshot_id| {
            let snapshots = pin.to_snapshot_map();
            let table_uuids = pin.to_table_uuid_map();
            update_starrocks_mv_refresh_summary(
                state,
                runtime.table.table_id,
                mv_definition.last_refresh_rows.unwrap_or(0),
                snapshots,
                table_uuids,
            )?;
            refresh_starrocks_catalog(state)?;
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
                        return refresh_mv_full_with_pinned_executor(
                            state,
                            &db_name,
                            &mv_name,
                            pinned_full_select_sql.clone(),
                            pinned_base_metadata.clone(),
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
                    return refresh_mv_full_with_pinned_executor(
                        state,
                        &db_name,
                        &mv_name,
                        pinned_full_select_sql.clone(),
                        pinned_base_metadata.clone(),
                        move |ctx| run_projection_mv_select_and_chunks(ctx, &primary_key_columns),
                    );
                }
                MvApplyPolicy::Unsupported { reason } => {
                    return Err(format!(
                        "iceberg materialized view refresh unsupported: {reason}"
                    ));
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
                    pin.to_snapshot_map(),
                    pin.to_table_uuid_map(),
                    mv_definition.last_refresh_rows.unwrap_or(0),
                )?;
                refresh_starrocks_catalog(state)?;
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
            let (chunks, row_delta) = if mv_definition.primary_key_columns.is_empty() {
                tagged_projection_insert_chunks(delta_result)?
            } else {
                tagged_projection_change_chunks(delta_result)?
            };
            let resolved_mv = novarocks_catalog::identifier::LocalTableIdentity {
                database: db_name.clone(),
                table: mv_name.clone(),
            };
            let plan = if mv_definition.primary_key_columns.is_empty() {
                load_insert_plan(state, &resolved_mv, PartitionTarget::Active)
            } else {
                load_physical_insert_plan(state, &resolved_mv, PartitionTarget::Active)
            }?;
            let previous_rows = mv_definition.last_refresh_rows.unwrap_or(0);
            let snapshots = pin.to_snapshot_map();
            let table_uuids = pin.to_table_uuid_map();
            write_chunks_into_starrocks_partition_for_mv_refresh_with_row_delta(
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
            refresh_starrocks_catalog(state)?;
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
                        return refresh_aggregate_mv_full_with_pinned_metadata(
                            state,
                            &db_name,
                            &mv_name,
                            shape,
                            pinned_full_select_sql.clone(),
                            pinned_base_metadata.clone(),
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
                refresh_snapshots: pin.to_snapshot_map(),
                refresh_table_uuids: pin.to_table_uuid_map(),
                pinned_full_select_sql: pinned_full_select_sql.clone(),
                pinned_base_metadata: pinned_base_metadata.clone(),
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
    mv_shape: &crate::mv::aggregate_state::mv_shape::IncrementalMvShape,
    strategy: MvRefreshPolicy,
    projection_full: ProjectionFull,
    aggregate_full: AggregateFull,
    no_op: NoOp,
    projection_incremental: ProjectionIncremental,
    aggregate_incremental: AggregateIncremental,
) -> Result<StatementResult, String>
where
    ProjectionFull: FnOnce() -> Result<StatementResult, String>,
    AggregateFull: FnOnce(
        &crate::mv::aggregate_state::mv_shape::AggregateMvShape,
    ) -> Result<StatementResult, String>,
    NoOp: FnOnce(i64) -> Result<StatementResult, String>,
    ProjectionIncremental: FnOnce(i64, i64) -> Result<StatementResult, String>,
    AggregateIncremental: FnOnce(
        &crate::mv::aggregate_state::mv_shape::AggregateMvShape,
        i64,
        i64,
    ) -> Result<StatementResult, String>,
{
    match (mv_shape, strategy) {
        (
            crate::mv::aggregate_state::mv_shape::IncrementalMvShape::JoinProjectionFilter(_),
            _,
        ) => Err(
            "join projection/filter IMV refresh is not supported by legacy StarRocks MV refresh"
                .to_string(),
        ),
        (crate::mv::aggregate_state::mv_shape::IncrementalMvShape::JoinAggregate(_), _) => Err(
            "join aggregate IMV refresh is not supported by legacy StarRocks MV refresh"
                .to_string(),
        ),
        (crate::mv::aggregate_state::mv_shape::IncrementalMvShape::UnionAll(_), _) => {
            Err("UNION ALL IMV refresh is not supported by legacy StarRocks MV refresh".to_string())
        }
        (
            crate::mv::aggregate_state::mv_shape::IncrementalMvShape::ProjectionFilter(_),
            MvRefreshPolicy::FullRefresh { .. },
        ) => projection_full(),
        (
            crate::mv::aggregate_state::mv_shape::IncrementalMvShape::Aggregate(shape),
            MvRefreshPolicy::FullRefresh { .. },
        ) => aggregate_full(shape),
        (
            _,
            MvRefreshPolicy::NoOp {
                current_snapshot_id,
            },
        ) => no_op(current_snapshot_id),
        (
            crate::mv::aggregate_state::mv_shape::IncrementalMvShape::ProjectionFilter(_),
            MvRefreshPolicy::Incremental {
                previous_snapshot_id,
                current_snapshot_id,
            },
        ) => projection_incremental(previous_snapshot_id, current_snapshot_id),
        (
            crate::mv::aggregate_state::mv_shape::IncrementalMvShape::Aggregate(shape),
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

#[allow(dead_code)]
fn refresh_aggregate_mv_full(
    state: &Arc<StandaloneState>,
    database: &str,
    mv_name: &str,
    shape: &crate::mv::aggregate_state::mv_shape::AggregateMvShape,
) -> Result<StatementResult, String> {
    let shape = shape.clone();
    refresh_mv_full_with_executor(state, database, mv_name, move |ctx| {
        execute_aggregate_mv_full_refresh(ctx, &shape)
    })
}

fn refresh_aggregate_mv_full_with_pinned_metadata(
    state: &Arc<StandaloneState>,
    database: &str,
    mv_name: &str,
    shape: &crate::mv::aggregate_state::mv_shape::AggregateMvShape,
    pinned_select_sql: String,
    base_metadata: CurrentBaseMetadata,
) -> Result<StatementResult, String> {
    let shape = shape.clone();
    refresh_mv_full_with_pinned_executor(
        state,
        database,
        mv_name,
        pinned_select_sql,
        base_metadata,
        move |ctx| execute_aggregate_mv_full_refresh(ctx, &shape),
    )
}

fn execute_aggregate_mv_full_refresh(
    ctx: MvRefreshContext,
    shape: &crate::mv::aggregate_state::mv_shape::AggregateMvShape,
) -> Result<Vec<Chunk>, String> {
    // Step 1: obtain visible-shaped output types by analyzing the refresh SELECT
    // without executing it. `build_aggregate_mv_layout` expects visible-shaped types
    // (one column per visible_output), not state-shaped types (which expand AVG into
    // two columns: SUM + COUNT). Running the analyzer is cheap — no execution occurs.
    let visible_analysis = analyze_visible_query(&ctx.state, &ctx.database, &ctx.select_sql)?;
    let aggregate_input_types =
        crate::mv::aggregate_state::mv_agg_state::aggregate_input_types_from_resolved_query(
            &crate::mv::aggregate_state::aggregate_sql_calls::AggregateSqlCalls::from(shape),
            &visible_analysis,
        )?;
    let visible_output_columns = visible_analysis.output_columns;

    // Step 2: build the layout from visible types.
    let layout =
        crate::mv::aggregate_state::mv_agg_state::build_aggregate_mv_layout_with_input_types(
            &crate::mv::aggregate_state::aggregate_sql_calls::AggregateSqlCalls::from(shape),
            &visible_output_columns,
            &aggregate_input_types,
        )?;

    // Step 3: rewrite the SELECT to emit state columns (AVG → SUM + COUNT) and execute
    // it to obtain the actual state-shaped data.
    let state_sql = crate::mv::aggregate_state::mv_shape::rewrite_select_sql_for_state(
        &ctx.select_sql,
        &crate::mv::aggregate_state::aggregate_sql_calls::AggregateSqlCalls::from(shape),
    )?;
    let result = execute_query_for_mv_refresh(&ctx.state, &ctx.database, &state_sql)?;

    // Step 4: materialize state-shaped executor result using the visible-type layout.
    crate::mv::aggregate_state::mv_agg_state::materialize_aggregate_result_chunks(result, &layout)
}

fn rewrite_full_refresh_select_with_pin(
    select_sql: &str,
    pin: &crate::mv::refresh::pin::RefreshSnapshotPin,
    base_ref: &TableIdentity,
) -> Result<String, String> {
    let normalized = crate::sql::parser::dialect::normalize_for_raw_parse(select_sql)
        .map_err(|e| format!("full refresh pin SELECT normalize error: {e}"))?;
    let mut stmt = crate::sql::parser::parse_normalized_sql_raw(&normalized)
        .map_err(|e| format!("full refresh pin SELECT parse error: {e}"))?;
    let sqlparser::ast::Statement::Query(query) = &mut stmt else {
        return Err("full refresh pin SELECT expects a SELECT query".to_string());
    };
    crate::mv::refresh::pin::inject_pin_as_for_version_as_of(
        query,
        pin,
        &HashSet::new(),
        Some(&base_ref.catalog),
        &base_ref.namespace,
    )?;
    Ok(stmt.to_string())
}

fn current_base_metadata_from_pin(
    pin: &crate::mv::refresh::pin::RefreshSnapshotPin,
) -> CurrentBaseMetadata {
    CurrentBaseMetadata {
        snapshots: pin.to_snapshot_map(),
        table_uuids: pin.to_table_uuid_map(),
    }
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
    base_ref: &'a TableIdentity,
    shape: &'a crate::mv::aggregate_state::mv_shape::AggregateMvShape,
    change_batch: crate::connector::iceberg::changes::IcebergChangeBatch,
    previous_refresh_rows: i64,
    previous_snapshot_id: i64,
    current_snapshot_id: i64,
    refresh_snapshots: BTreeMap<String, i64>,
    refresh_table_uuids: BTreeMap<String, String>,
    pinned_full_select_sql: String,
    pinned_base_metadata: CurrentBaseMetadata,
    loaded: &'a crate::connector::iceberg::catalog::IcebergLoadedTable,
}

fn refresh_aggregate_mv_incremental(
    ctx: AggregateMvIncrementalRefreshContext<'_>,
) -> Result<StatementResult, String> {
    let has_inserts = !ctx.change_batch.inserts.is_empty();
    let has_deletes = change_batch_has_deletes(&ctx.change_batch);
    let apply_policy = apply_policy_for_change(
        &crate::mv::aggregate_state::mv_shape::IncrementalMvShape::Aggregate(ctx.shape.clone()),
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
            return refresh_aggregate_mv_full_with_pinned_metadata(
                ctx.state,
                ctx.database,
                ctx.mv_name,
                ctx.shape,
                ctx.pinned_full_select_sql.clone(),
                ctx.pinned_base_metadata.clone(),
            );
        }
        MvApplyPolicy::Unsupported { reason } => {
            return Err(format!(
                "iceberg materialized view refresh unsupported: {reason}"
            ));
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
            ctx.refresh_snapshots.clone(),
            ctx.refresh_table_uuids.clone(),
            ctx.previous_refresh_rows,
        )?;
        refresh_starrocks_catalog(ctx.state)?;
        return Ok(StatementResult::Ok);
    }

    // The rewritten state SQL (AVG -> SUM + COUNT) produces state-shaped columns whose
    // count does not match shape.visible_outputs. Sourcing types from the analyzer
    // avoids this mismatch before materializing state chunks.
    let visible_analysis = analyze_visible_query(ctx.state, ctx.database, ctx.select_sql)?;
    let aggregate_input_types =
        crate::mv::aggregate_state::mv_agg_state::aggregate_input_types_from_resolved_query(
            &crate::mv::aggregate_state::aggregate_sql_calls::AggregateSqlCalls::from(ctx.shape),
            &visible_analysis,
        )?;
    let layout =
        crate::mv::aggregate_state::mv_agg_state::build_aggregate_mv_layout_with_input_types(
            &crate::mv::aggregate_state::aggregate_sql_calls::AggregateSqlCalls::from(ctx.shape),
            &visible_analysis.output_columns,
            &aggregate_input_types,
        )?;

    // The signed-delta rewriter emits VARBINARY state combinators for every
    // aggregate, so any error from the rewriter is now a real error.
    let signed_state_sql = super::ivm_delta_aggregate::rewrite_select_sql_for_signed_delta_state(
        ctx.select_sql,
        &crate::mv::aggregate_state::aggregate_sql_calls::AggregateSqlCalls::from(ctx.shape),
    )?;
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
        crate::mv::aggregate_state::mv_agg_state::materialize_aggregate_result_chunks(
            delta_result,
            &layout,
        )?;

    let plan = load_physical_insert_plan(
        ctx.state,
        &novarocks_catalog::identifier::LocalTableIdentity {
            database: ctx.database.to_string(),
            table: ctx.mv_name.to_string(),
        },
        PartitionTarget::Active,
    )?;
    write_chunks_into_starrocks_partition_for_aggregate_mv_upsert(
        ctx.state,
        plan,
        &delta_chunks,
        &layout,
        MvRefreshWriteMetadata {
            table_id: ctx.table_id,
            previous_refresh_rows: ctx.previous_refresh_rows,
            snapshots: ctx.refresh_snapshots,
            table_uuids: ctx.refresh_table_uuids,
        },
    )?;
    refresh_starrocks_catalog(ctx.state)?;
    Ok(StatementResult::Ok)
}

fn advance_mv_refresh_metadata_without_writes(
    state: &Arc<StandaloneState>,
    table_id: i64,
    refresh_snapshots: BTreeMap<String, i64>,
    refresh_table_uuids: BTreeMap<String, String>,
    last_refresh_rows: i64,
) -> Result<(), String> {
    update_starrocks_mv_refresh_summary(
        state,
        table_id,
        last_refresh_rows,
        refresh_snapshots,
        refresh_table_uuids,
    )
}

#[allow(dead_code)]
pub(crate) fn refresh_mv_full_with_executor<F>(
    state: &Arc<StandaloneState>,
    database: &str,
    mv_name: &str,
    executor: F,
) -> Result<StatementResult, String>
where
    F: FnOnce(MvRefreshContext) -> Result<Vec<Chunk>, String>,
{
    refresh_mv_full_with_executor_inner(state, database, mv_name, None, None, executor)
}

fn refresh_mv_full_with_pinned_executor<F>(
    state: &Arc<StandaloneState>,
    database: &str,
    mv_name: &str,
    pinned_select_sql: String,
    base_metadata: CurrentBaseMetadata,
    executor: F,
) -> Result<StatementResult, String>
where
    F: FnOnce(MvRefreshContext) -> Result<Vec<Chunk>, String>,
{
    refresh_mv_full_with_executor_inner(
        state,
        database,
        mv_name,
        Some(pinned_select_sql),
        Some(base_metadata),
        executor,
    )
}

fn refresh_mv_full_with_executor_inner<F>(
    state: &Arc<StandaloneState>,
    database: &str,
    mv_name: &str,
    select_sql_override: Option<String>,
    base_metadata_override: Option<CurrentBaseMetadata>,
    executor: F,
) -> Result<StatementResult, String>
where
    F: FnOnce(MvRefreshContext) -> Result<Vec<Chunk>, String>,
{
    let starrocks_table_config = state
        .starrocks_table_config
        .clone()
        .ok_or_else(|| "standalone StarRocks table config is missing".to_string())?;

    let runtime = {
        let starrocks = state
            .starrocks_table
            .read()
            .expect("standalone StarRocks table read lock");
        starrocks.table(database, mv_name)?.clone()
    };
    if runtime.table.kind != StarRocksTableKind::MaterializedView {
        return Err(format!("`{database}.{mv_name}` is not a materialized view"));
    }

    let mv_definition = load_mv_definition_by_id(state, runtime.table.table_id)?
        .ok_or_else(|| format!("materialized view {database}.{mv_name} has no MV definition"))?;
    let active_partition = runtime
        .partitions
        .iter()
        .find(|partition| partition.state == StarRocksPartitionState::Active)
        .cloned()
        .ok_or_else(|| format!("materialized view {database}.{mv_name} has no active partition"))?;
    let retired_root_path = starrocks_table_config.tablet_root_path(
        runtime.table.db_id,
        runtime.table.table_id,
        active_partition.partition_id,
    );

    let staged = stage_starrocks_mv_refresh_partition(
        state,
        &runtime,
        &active_partition.name,
        &starrocks_table_config.warehouse_uri,
    )?;

    if let Err(err) = refresh_starrocks_catalog(state) {
        cleanup_staged_partition(state, runtime.table.table_id, &staged, false)?;
        return Err(format!("mv refresh catalog refresh failed: {err}"));
    }

    if let Err(err) = bootstrap_mv_refresh_partition_for_tablets(
        &runtime,
        &starrocks_table_config,
        staged.partition_id,
        &staged.tablet_ids,
    ) {
        cleanup_staged_partition(state, runtime.table.table_id, &staged, false)?;
        return Err(format!("mv refresh bootstrap failed: {err}"));
    }

    let chunks = match executor(MvRefreshContext {
        state: Arc::clone(state),
        database: database.to_string(),
        select_sql: select_sql_override.unwrap_or_else(|| mv_definition.select_sql.clone()),
    }) {
        Ok(chunks) => chunks,
        Err(err) => {
            cleanup_staged_partition(state, runtime.table.table_id, &staged, true)?;
            return Err(format!("mv refresh execute failed: {err}"));
        }
    };

    let plan = match load_physical_insert_plan(
        state,
        &novarocks_catalog::identifier::LocalTableIdentity {
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

    let rows_written = match write_chunks_into_starrocks_partition(state, plan, &chunks) {
        Ok(rows_written) => rows_written,
        Err(err) => {
            cleanup_staged_partition(state, runtime.table.table_id, &staged, true)?;
            return Err(format!("mv refresh write failed: {err}"));
        }
    };

    let base_metadata = match base_metadata_override {
        Some(metadata) => metadata,
        None => {
            let base_refs = parse_iceberg_table_refs(&mv_definition.base_table_refs)?;
            collect_current_base_metadata_or_cleanup_staged_partition(
                state,
                runtime.table.table_id,
                &staged,
                &base_refs,
            )?
        }
    };
    if let Err(err) = activate_starrocks_mv_refresh_partition(
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

    refresh_starrocks_catalog(state)?;
    Ok(StatementResult::Ok)
}

#[derive(Clone)]
pub(crate) struct MvRefreshContext {
    pub(crate) state: Arc<StandaloneState>,
    pub(crate) database: String,
    pub(crate) select_sql: String,
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

pub(crate) fn run_mv_full_select_chunks_with_catalog(
    state: &Arc<StandaloneState>,
    current_catalog: Option<&str>,
    database: &str,
    select_sql: &str,
) -> Result<Vec<Chunk>, String> {
    let result =
        execute_query_for_mv_refresh_with_catalog(state, current_catalog, database, select_sql)?;
    query_result_to_chunks(result)
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
    // Collect raw delete/upsert record batches first so we can dedupe a delete
    // row against an upsert row that share the same MV apply key. Reason: the
    // StarRocks table txn log coalesces all rowsets written in a single txn into
    // ONE op_write.rowset. When that merged rowset carries both a `del_file`
    // and upsert segments for the same PK, the PK applier removes the row
    // after the upsert lands, so a COW UPDATE (deleted row + inserted row
    // with matching `_row_id` → matching apply key) ends up dropping the
    // updated PK entirely. Drop the delete side of the pair so the upsert
    // wins and the MV reflects the update.
    let mut delete_batches: Vec<RecordBatch> = Vec::new();
    let mut insert_batches: Vec<RecordBatch> = Vec::new();
    let mut change_op_index_for_batches: Option<usize> = None;

    for chunk in result.chunks {
        let batch = chunk.batch;
        let change_op_index = find_change_op_column(&batch)?;
        if let Some(prev) = change_op_index_for_batches {
            if prev != change_op_index {
                return Err(format!(
                    "projection/filter MV delta source column `{CHANGE_OP_COLUMN}` index drifted across batches"
                ));
            }
        } else {
            change_op_index_for_batches = Some(change_op_index);
        }
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

        if delete_mask.iter().any(|keep| *keep) {
            let filtered = filter_record_batch(&batch, &BooleanArray::from(delete_mask))
                .map_err(|e| format!("filter projection MV deletes failed: {e}"))?;
            let without_change_op = record_batch_without_column(filtered, change_op_index)?;
            delete_batches.push(without_change_op);
        }

        if insert_mask.iter().any(|keep| *keep) {
            let filtered = filter_record_batch(&batch, &BooleanArray::from(insert_mask))
                .map_err(|e| format!("filter projection MV upserts failed: {e}"))?;
            let without_change_op = record_batch_without_column(filtered, change_op_index)?;
            insert_batches.push(without_change_op);
        }
    }

    let mut insert_apply_keys: std::collections::HashSet<Vec<u8>> =
        std::collections::HashSet::new();
    for batch in &insert_batches {
        for sig in collect_apply_key_signatures(batch)? {
            insert_apply_keys.insert(sig);
        }
    }

    let mut delete_chunks: Vec<Chunk> = Vec::new();
    let mut delete_rows = 0_i64;
    for batch in delete_batches {
        let filtered = if insert_apply_keys.is_empty() {
            batch
        } else {
            let mut keep_mask = Vec::with_capacity(batch.num_rows());
            for sig in collect_apply_key_signatures(&batch)? {
                keep_mask.push(!insert_apply_keys.contains(&sig));
            }
            filter_record_batch(&batch, &BooleanArray::from(keep_mask))
                .map_err(|e| format!("dedupe delete vs upsert apply-key failed: {e}"))?
        };
        if filtered.num_rows() == 0 {
            continue;
        }
        delete_rows = add_row_count(delete_rows, filtered.num_rows())?;
        delete_chunks.push(record_batch_to_chunk(append_mv_op_column(
            filtered,
            MV_OP_DELETE,
        )?)?);
    }

    let mut insert_chunks: Vec<Chunk> = Vec::new();
    let mut insert_rows = 0_i64;
    for batch in insert_batches {
        insert_rows = add_row_count(insert_rows, batch.num_rows())?;
        insert_chunks.push(record_batch_to_chunk(append_mv_op_column(
            batch,
            MV_OP_UPSERT,
        )?)?);
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

/// Build per-row signatures over the MV apply-key column. The apply key is
/// the leading column produced by the projection-MV pipeline (typically
/// `__mv_pk_id` for a single-column PK MV, or the synthesized hidden PK
/// column for join MVs); collecting its byte representation per row lets us
/// dedupe deletes against upserts that target the same MV row identity.
fn collect_apply_key_signatures(batch: &RecordBatch) -> Result<Vec<Vec<u8>>, String> {
    use arrow::row::{RowConverter, SortField};
    if batch.num_columns() == 0 {
        return Err(
            "projection/filter MV chunk has no columns; cannot derive apply-key signature"
                .to_string(),
        );
    }
    let key_col = batch.column(0).clone();
    let converter = RowConverter::new(vec![SortField::new(key_col.data_type().clone())])
        .map_err(|e| format!("apply-key row converter init failed: {e}"))?;
    let rows = converter
        .convert_columns(&[key_col])
        .map_err(|e| format!("apply-key row encode failed: {e}"))?;
    let mut out = Vec::with_capacity(batch.num_rows());
    for row in 0..batch.num_rows() {
        out.push(rows.row(row).as_ref().to_vec());
    }
    Ok(out)
}

fn tagged_projection_insert_chunks(result: QueryResult) -> Result<(Vec<Chunk>, i64), String> {
    let mut chunks = Vec::new();
    let mut row_delta = 0_i64;

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

        for row in 0..batch.num_rows() {
            if change_ops.is_null(row) {
                return Err(format!(
                    "projection/filter MV delta source column `{CHANGE_OP_COLUMN}` contains NULL"
                ));
            }
            match change_ops.value(row) {
                CHANGE_OP_INSERT => {}
                CHANGE_OP_DELETE => {
                    return Err(
                        "non-primary-key projection/filter MV incremental refresh cannot apply delete rows; define PRIMARY KEY on the MV or use full refresh"
                            .to_string(),
                    );
                }
                op => {
                    return Err(format!(
                        "projection/filter MV delta source column `{CHANGE_OP_COLUMN}` contains invalid value {op}; expected {CHANGE_OP_INSERT} or {CHANGE_OP_DELETE}"
                    ));
                }
            }
        }

        if batch.num_rows() > 0 {
            row_delta = add_row_count(row_delta, batch.num_rows())?;
            let without_change_op = record_batch_without_column(batch, change_op_index)?;
            chunks.push(record_batch_to_chunk(without_change_op)?);
        }
    }

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
        .filter(|(index, _)| *index != column_index)
        .map(|(_, field)| field.as_ref().clone())
        .collect::<Vec<_>>();
    let columns = batch
        .columns()
        .iter()
        .enumerate()
        .filter(|(index, _)| *index != column_index)
        .map(|(_, column)| Arc::clone(column))
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

fn bootstrap_mv_refresh_partition_for_tablets(
    runtime: &StarRocksTableRuntime,
    starrocks_table_config: &StarRocksTableConfig,
    partition_id: i64,
    tablet_ids: &[i64],
) -> Result<(), String> {
    if runtime.columns.iter().all(|column| column.visible) {
        return bootstrap_empty_partition_for_tablets(
            runtime,
            starrocks_table_config,
            partition_id,
            tablet_ids,
        );
    }

    let request_schema = request_schema_from_runtime(runtime)?;
    let object_store_profile =
        ObjectStoreProfile::from_s3_store_config(&starrocks_table_config.s3)?;
    let tablet_root_path = starrocks_table_config.tablet_root_path(
        runtime.table.db_id,
        runtime.table.table_id,
        partition_id,
    );
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
            Some(starrocks_table_config.s3.clone()),
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
                "StarRocks bootstrap schema mismatch after bootstrap: tablet_id={tablet_id}"
            ));
        }
    }
    Ok(())
}

fn validate_incremental_mv_select(
    select_sql: &str,
) -> Result<crate::mv::aggregate_state::mv_shape::IncrementalMvShape, String> {
    let normalized = crate::sql::parser::dialect::normalize_for_raw_parse(select_sql)?;
    let statement = crate::sql::parser::parse_normalized_sql_raw(&normalized)
        .map_err(|e| format!("sql parser error: {e}"))?;
    let sqlparser::ast::Statement::Query(query) = statement else {
        return Err("REFRESH MATERIALIZED VIEW stored SQL must be a SELECT query".to_string());
    };
    crate::mv::aggregate_state::mv_shape::classify_incremental_mv_query(&query)
}

fn validate_incremental_mv_base_ref(
    base_table: &sqlparser::ast::ObjectName,
    base_ref: &TableIdentity,
) -> Result<(), String> {
    let actual = normalize_three_part_base_table(base_table)?;
    let expected = (
        novarocks_catalog::identifier::normalize_identifier(&base_ref.catalog).map_err(|e| {
            format!("incremental MV refresh stored metadata has invalid catalog reference: {e}")
        })?,
        novarocks_catalog::identifier::normalize_identifier(&base_ref.namespace).map_err(|e| {
            format!("incremental MV refresh stored metadata has invalid namespace reference: {e}")
        })?,
        novarocks_catalog::identifier::normalize_identifier(&base_ref.table).map_err(|e| {
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
                novarocks_catalog::identifier::normalize_identifier(&ident.value).map_err(|e| {
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
    table_ref: &TableIdentity,
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
    table_ref: &TableIdentity,
    snapshot_id: i64,
) -> BTreeMap<String, i64> {
    let mut snapshots = BTreeMap::new();
    snapshots.insert(table_ref.fqn(), snapshot_id);
    snapshots
}

pub(crate) fn single_table_uuid_map(
    table_ref: &TableIdentity,
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
    refs: &[TableIdentity],
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
    staged: &StagedStarRocksMvRefresh,
    refs: &[TableIdentity],
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
    crate::engine::mv::refresh_io::acquire_mv_refresh_lock()
}

fn cleanup_staged_partition(
    state: &Arc<StandaloneState>,
    table_id: i64,
    staged: &StagedStarRocksMvRefresh,
    enqueue_erase_job: bool,
) -> Result<(), String> {
    for tablet_id in &staged.tablet_ids {
        let _ = remove_tablet_runtime(*tablet_id);
    }
    let provider = state.metadata_provider.as_ref().ok_or_else(|| {
        "StarRocks table MV refresh cleanup requires metadata provider".to_string()
    })?;
    let mut txn = provider
        .begin_write("cleanup StarRocks table mv refresh partition")
        .map_err(|e| format!("open StarRocks MV refresh cleanup transaction failed: {e}"))?;
    state
        .starrocks_table_repo
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
        .map_err(|e| format!("commit StarRocks MV refresh cleanup failed: {e}"))?;
    refresh_starrocks_catalog(state)?;
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

fn update_starrocks_mv_refresh_summary(
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
        .begin_write("update StarRocks materialized view refresh summary")
        .map_err(|e| format!("open mv refresh summary transaction failed: {e}"))?;
    state
        .mv_repo
        .update_starrocks_refresh_summary_if_present(
            txn.as_mut(),
            UpdateStarRocksMvRefreshSummaryRequest {
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

fn stage_starrocks_mv_refresh_partition(
    state: &Arc<StandaloneState>,
    runtime: &StarRocksTableRuntime,
    partition_name: &str,
    warehouse_uri: &str,
) -> Result<StagedStarRocksMvRefresh, String> {
    let provider = state
        .metadata_provider
        .as_ref()
        .ok_or_else(|| "StarRocks table MV refresh requires metadata provider".to_string())?;
    let mut txn = provider
        .begin_write("stage StarRocks table mv refresh partition")
        .map_err(|e| format!("open StarRocks MV refresh stage transaction failed: {e}"))?;
    state
        .starrocks_txn_repo
        .ensure_no_inflight_for_table(txn.as_ref(), runtime.table.table_id)
        .map_err(|e| format!("validate StarRocks MV refresh failed: {e}"))?;
    let staged = state
        .starrocks_table_repo
        .stage_mv_refresh_partition(
            txn.as_mut(),
            StageStarRocksMvRefreshRequest {
                table_id: runtime.table.table_id,
                db_id: runtime.table.db_id,
                bucket_num: runtime.table.bucket_num,
                partition_name: partition_name.to_string(),
                warehouse_uri: warehouse_uri.to_string(),
            },
        )
        .map_err(|e| format!("stage StarRocks MV refresh metadata failed: {e}"))?;
    txn.commit()
        .map_err(|e| format!("commit StarRocks MV refresh stage metadata failed: {e}"))?;
    Ok(staged)
}

fn activate_starrocks_mv_refresh_partition(
    state: &Arc<StandaloneState>,
    table_id: i64,
    old_partition_id: i64,
    retired_root_path: &str,
    staged: &StagedStarRocksMvRefresh,
    rows_written: i64,
    base_metadata: CurrentBaseMetadata,
) -> Result<(), String> {
    let provider = state
        .metadata_provider
        .as_ref()
        .ok_or_else(|| "StarRocks table MV refresh requires metadata provider".to_string())?;
    let mut txn = provider
        .begin_write("activate StarRocks table mv refresh partition")
        .map_err(|e| format!("open StarRocks MV refresh activate transaction failed: {e}"))?;
    state
        .starrocks_table_repo
        .activate_mv_refresh_partition(
            txn.as_mut(),
            table_id,
            old_partition_id,
            staged.partition_id,
            staged.index_id,
        )
        .map_err(|e| format!("activate StarRocks MV refresh metadata failed: {e}"))?;
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
        .map_err(|e| format!("enqueue StarRocks MV refresh erase job failed: {e}"))?;
    state
        .mv_repo
        .update_starrocks_refresh_summary_if_present(
            txn.as_mut(),
            UpdateStarRocksMvRefreshSummaryRequest {
                mv_id: table_id,
                last_refresh_ms: super::mv_ddl::now_ms(),
                last_refresh_rows: rows_written,
                base_snapshots: base_metadata.snapshots,
                base_table_uuids: base_metadata.table_uuids,
            },
        )
        .map_err(|e| format!("update StarRocks MV refresh summary failed: {e}"))?;
    txn.commit()
        .map_err(|e| format!("commit StarRocks MV refresh activate metadata failed: {e}"))?;
    Ok(())
}

pub(crate) fn parse_iceberg_table_refs(refs: &[String]) -> Result<Vec<TableIdentity>, String> {
    refs.iter()
        .map(|fqn| {
            let parts = fqn.split('.').collect::<Vec<_>>();
            let [catalog, namespace, table] = parts.as_slice() else {
                return Err(format!(
                    "materialized view base table reference must be catalog.namespace.table, got `{fqn}`"
                ));
            };
            Ok(TableIdentity {
                catalog: novarocks_catalog::identifier::normalize_identifier(catalog)?,
                namespace: novarocks_catalog::identifier::normalize_identifier(namespace)?,
                table: novarocks_catalog::identifier::normalize_identifier(table)?,
            })
        })
        .collect()
}

fn refresh_starrocks_catalog(state: &Arc<StandaloneState>) -> Result<(), String> {
    let provider = state
        .metadata_provider
        .as_ref()
        .ok_or_else(|| "StarRocks table catalog refresh requires metadata provider".to_string())?;
    let read = provider
        .begin_read()
        .map_err(|e| format!("open StarRocks catalog refresh transaction failed: {e}"))?;
    let snapshot = state
        .starrocks_table_repo
        .load_snapshot(read.as_ref())
        .map_err(|e| format!("load StarRocks catalog metadata failed: {e}"))?;
    let rebuilt = StarRocksTableCatalog::rebuild_from_repository(
        state.starrocks_table_config.clone(),
        snapshot.clone(),
    )?;
    {
        let mut catalog = state
            .catalog_service
            .local()
            .write()
            .expect("standalone catalog write lock");
        for database in &snapshot.databases {
            catalog.create_database(&database.name)?;
        }
        register_starrocks_tables_in_catalog(&mut catalog, &rebuilt)?;
    }
    rebuilt.re_register_active_tablet_runtimes()?;
    let mut starrocks = state
        .starrocks_table
        .write()
        .expect("standalone StarRocks table write lock");
    *starrocks = rebuilt;
    Ok(())
}
