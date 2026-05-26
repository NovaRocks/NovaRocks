//! Projection/filter materialized views backed by Iceberg target tables in the
//! current Iceberg catalog. Aggregate shapes are accepted at CREATE time for
//! target schema and contract persistence; refresh execution is gated later.

use std::collections::{BTreeMap, HashSet};
use std::sync::Arc;

use iceberg::Catalog;
use iceberg::TableIdent;
use iceberg::spec::DataFile;
#[cfg(test)]
use iceberg::spec::{NestedField, PrimitiveType, Schema, Type};

use crate::connector::iceberg::changes::{
    IcebergChangePolicySignal, plan_changes, policy_signal_from_change_error,
};
use crate::connector::iceberg::commit::{
    CommitOpKind, CommitOutcome, IcebergCommitCollector, MvRefreshPublishPlan,
    MvRefreshSnapshotMarker, PositionDeleteGroup, RefAction, RefActionPlan, RunInput,
    execute_ref_action, publish_staging_branch_to_main, run_iceberg_commit,
    snapshot_matches_refresh_marker,
};
use crate::connector::iceberg::data_writer::write_record_batches_as_data_files;
use crate::connector::starrocks::managed::model::{IcebergTableRef, ManagedMvStorageEngine};
use crate::connector::starrocks::managed::mv_ddl::{
    analyze_mv_select, canonicalize_iceberg_mv_select_query, now_ms, output_column_to_table_column,
    resolve_mv_name, validate_mv_partition_columns,
};
use crate::connector::starrocks::managed::mv_refresh::{
    acquire_mv_refresh_lock, load_current_iceberg_base_table, parse_iceberg_table_refs,
    run_mv_full_select_chunks, single_snapshot_map, single_table_uuid_map,
};
use crate::connector::starrocks::managed::mv_shape::{
    AggregateMvShape, IncrementalMvShape, JoinAggregateMvShape, classify_incremental_mv_query,
};
use crate::engine::mv::iceberg_target_apply::{
    ICEBERG_MV_APPLY_KEY_COLUMN, ICEBERG_MV_APPLY_KEY_SOURCE_BASE_ROW_ID,
    ICEBERG_MV_APPLY_KEY_SOURCE_GROUP_ROW_ID, ICEBERG_MV_APPLY_KEY_SOURCE_JOIN_ROW_KEY,
    ICEBERG_MV_GROUP_APPLY_KEY_COLUMN, ICEBERG_MV_JOIN_APPLY_KEY_COLUMN,
    ICEBERG_MV_PROP_APPLY_KEY_COLUMN, ICEBERG_MV_PROP_APPLY_KEY_FIELD_ID,
    ICEBERG_MV_PROP_APPLY_KEY_SOURCE, ICEBERG_MV_PROP_HIDDEN_COLUMNS, apply_key_table_column,
    ensure_base_row_lineage_contract, find_apply_key_field_id_by_column,
    iceberg_mv_physical_select_sql, join_apply_key_table_column, load_target_apply_locator_inputs,
};
use crate::engine::mv::lifecycle::{
    BackendRefreshPlan, IcebergRefreshOutcome, IcebergRefreshPlan, MvBaseRef, MvStorageEngine,
    MvTarget, RefreshError, RefreshMode, RefreshPlan,
};
use crate::engine::mv::rebind::rewrite_select_sql_for_rebind;
use crate::engine::mv::refresh_context::IcebergMvRefreshContext;
use crate::engine::{StandaloneState, StatementResult};
use crate::meta::repository::mv::{
    BeginIcebergMvRefreshRequest, CreateMvDefinitionRequest, MvRefreshFinalizeRequest,
    MvRefreshState, RecordPublishCommitRequest, RecordStagingCommitRequest, RefreshExternalOutcome,
    StoredMvDefinition, StoredMvRefresh,
};
use crate::runtime::global_async_runtime::data_block_on;
#[cfg(test)]
use crate::sql::analysis::OutputColumn;
use crate::sql::parser::ast::{
    CreateMaterializedViewStmt, DropMaterializedViewStmt, ObjectName, RefreshMaterializedViewStmt,
};

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct IcebergMvTarget {
    pub(crate) catalog: String,
    pub(crate) namespace: String,
    pub(crate) table: String,
}

pub(crate) fn create_iceberg_mv(
    state: &Arc<StandaloneState>,
    current_catalog: Option<&str>,
    current_database: &str,
    stmt: &CreateMaterializedViewStmt,
) -> Result<StatementResult, String> {
    let target = resolve_iceberg_mv_target(state, current_catalog, current_database, stmt)?;
    let entry = {
        let catalogs = state
            .iceberg_catalogs
            .read()
            .map_err(|e| format!("iceberg catalog registry read lock: {e}"))?;
        catalogs.get(&target.catalog)?
    };
    if iceberg_mv_target_exists(&entry, &target.namespace, &target.table)? {
        return Err(format!(
            "Iceberg MV target table {}.{}.{} already exists",
            target.catalog, target.namespace, target.table
        ));
    }

    // 1. Analyze and classify shape.
    let canonical_select_query =
        canonicalize_iceberg_mv_select_query(&stmt.select_query, current_catalog, current_database);
    let analysis = analyze_mv_select(
        state,
        current_catalog,
        current_database,
        &canonical_select_query,
    )?;
    validate_mv_partition_columns(stmt.partition_by.as_deref(), &analysis.output_columns)?;
    let created_at_ms = now_ms();
    let resolved_dependencies = crate::engine::mv::dependency::resolve_create_mv_dependencies(
        state,
        &analysis.resolved_refs,
        created_at_ms,
    )?;
    let dependency_target = crate::engine::mv::dependency::iceberg_mv_dependency_ref(
        &target.catalog,
        &target.namespace,
        &target.table,
    );
    // Defensive: this check runs after the `iceberg_mv_target_exists` guard
    // above, so user-facing CREATE statements can't reach it (a brand-new MV
    // target has no inbound edges, while an already-existing target fails on
    // existence first). Kept as a safety net for future paths that bypass the
    // existence check — e.g. ALTER MATERIALIZED VIEW rewriting a SELECT, or
    // racy metadata writes. Algorithm coverage lives in
    // src/engine/mv/dependency.rs::tests.
    crate::engine::mv::dependency::validate_no_create_cycle(
        state,
        &dependency_target,
        &resolved_dependencies.dependencies,
    )
    .map_err(|e| {
        format!(
            "cannot create materialized view {}.{}.{}: {e}",
            target.catalog, target.namespace, target.table
        )
    })?;
    let base_refs = resolved_dependencies.base_refs;
    let shape = classify_incremental_mv_query(&canonical_select_query)?;
    let aggregate_shape = aggregate_shape_for_layout(&shape);
    let loaded_bases = match &shape {
        IncrementalMvShape::ProjectionFilter(_) => {
            let [base_ref] = base_refs.as_slice() else {
                return Err(
                    "iceberg-backed projection/filter materialized views require exactly one iceberg base table"
                        .to_string(),
                );
            };
            let loaded_base = load_current_iceberg_base_table(state, base_ref)?;
            ensure_base_row_lineage_contract(&loaded_base.table, &base_ref.fqn())?;
            vec![(base_ref.clone(), loaded_base)]
        }
        IncrementalMvShape::JoinProjectionFilter(join_shape) => {
            if base_refs.len() != 2 {
                return Err(
                    "iceberg-backed join materialized views require exactly two iceberg base tables"
                        .to_string(),
                );
            }
            validate_join_shape_base_refs(join_shape, &base_refs)?;
            base_refs
                .iter()
                .map(|base_ref| {
                    let loaded_base = load_current_iceberg_base_table(state, base_ref)?;
                    ensure_base_row_lineage_contract(&loaded_base.table, &base_ref.fqn())?;
                    Ok((base_ref.clone(), loaded_base))
                })
                .collect::<Result<Vec<_>, String>>()?
        }
        IncrementalMvShape::Aggregate(_) => {
            let [base_ref] = base_refs.as_slice() else {
                return Err(
                    "iceberg-backed aggregate materialized views require exactly one iceberg base table"
                        .to_string(),
                );
            };
            let loaded_base = load_current_iceberg_base_table(state, base_ref)?;
            ensure_base_row_lineage_contract(&loaded_base.table, &base_ref.fqn())?;
            vec![(base_ref.clone(), loaded_base)]
        }
        IncrementalMvShape::JoinAggregate(join_shape) => {
            if base_refs.len() != 2 {
                return Err(
                    "iceberg-backed join aggregate materialized views require exactly two iceberg base tables"
                        .to_string(),
                );
            }
            validate_join_shape_base_refs(&join_shape.join, &base_refs)?;
            base_refs
                .iter()
                .map(|base_ref| {
                    let loaded_base = load_current_iceberg_base_table(state, base_ref)?;
                    ensure_base_row_lineage_contract(&loaded_base.table, &base_ref.fqn())?;
                    Ok((base_ref.clone(), loaded_base))
                })
                .collect::<Result<Vec<_>, String>>()?
        }
    };

    // IVM Phase-2 PRIMARY KEY validation. Only runs when the user opted in
    // by writing `PRIMARY KEY (...)` in the DDL; otherwise behavior is
    // unchanged. Reuses the same descriptor + validator as the managed-
    // lake-stored path in mv_ddl::create_mv.
    if let Some(pk_cols) = stmt.primary_key.as_deref() {
        match &shape {
            IncrementalMvShape::ProjectionFilter(_) => {
                let descriptor =
                    crate::connector::starrocks::managed::mv_ddl::descriptor_from_loaded(
                        &loaded_bases[0].1,
                    );
                crate::connector::starrocks::managed::mv_ddl::validate_ivm_primary_key(
                    pk_cols,
                    &descriptor,
                )
                .map_err(|e| e.to_string())?;
            }
            IncrementalMvShape::JoinProjectionFilter(_) => {
                return Err(
                    "iceberg-backed join materialized views do not support PRIMARY KEY in this phase"
                        .to_string(),
                );
            }
            IncrementalMvShape::Aggregate(_) | IncrementalMvShape::JoinAggregate(_) => {
                return Err(
                    "iceberg-backed aggregate materialized views do not support PRIMARY KEY"
                        .to_string(),
                );
            }
        }
    }

    // 2. Create the empty Iceberg v3 target table in the current catalog.
    let apply_key_column_name = match &shape {
        IncrementalMvShape::ProjectionFilter(_) => ICEBERG_MV_APPLY_KEY_COLUMN,
        IncrementalMvShape::JoinProjectionFilter(_) => ICEBERG_MV_JOIN_APPLY_KEY_COLUMN,
        IncrementalMvShape::Aggregate(_) | IncrementalMvShape::JoinAggregate(_) => {
            ICEBERG_MV_GROUP_APPLY_KEY_COLUMN
        }
    };
    let apply_key_source_property = match &shape {
        IncrementalMvShape::ProjectionFilter(_) => ICEBERG_MV_APPLY_KEY_SOURCE_BASE_ROW_ID,
        IncrementalMvShape::JoinProjectionFilter(_) => ICEBERG_MV_APPLY_KEY_SOURCE_JOIN_ROW_KEY,
        IncrementalMvShape::Aggregate(_) | IncrementalMvShape::JoinAggregate(_) => {
            ICEBERG_MV_APPLY_KEY_SOURCE_GROUP_ROW_ID
        }
    };
    if analysis
        .output_columns
        .iter()
        .any(|column| column.name.eq_ignore_ascii_case(apply_key_column_name))
    {
        return Err(format!(
            "Iceberg MV output column name {apply_key_column_name} is reserved for internal apply key"
        ));
    }
    let mut columns = if let Some(aggregate_shape) = aggregate_shape.as_ref() {
        iceberg_aggregate_target_columns(aggregate_shape, &analysis.output_columns)?
    } else {
        analysis
            .output_columns
            .iter()
            .map(output_column_to_table_column)
            .collect::<Result<Vec<_>, _>>()?
    };
    if aggregate_shape.is_none() {
        columns.push(match &shape {
            IncrementalMvShape::ProjectionFilter(_) => apply_key_table_column(),
            IncrementalMvShape::JoinProjectionFilter(_) => join_apply_key_table_column(),
            IncrementalMvShape::Aggregate(_) | IncrementalMvShape::JoinAggregate(_) => {
                unreachable!("aggregate shape was handled above")
            }
        });
    }
    let expected_apply_key_field_id = columns
        .iter()
        .position(|column| column.name.eq_ignore_ascii_case(apply_key_column_name))
        .and_then(|idx| i32::try_from(idx + 1).ok())
        .ok_or_else(|| {
            format!(
                "Iceberg MV target columns are missing apply-key column {apply_key_column_name}"
            )
        })?;
    let partition_fields = stmt.partition_by.as_deref().unwrap_or(&[]);
    let aggregate_state_hidden_columns = if let Some(aggregate_shape) = aggregate_shape.as_ref() {
        let layout = crate::connector::starrocks::managed::mv_agg_state::build_aggregate_mv_layout(
            aggregate_shape,
            &analysis.output_columns,
        )?;
        layout
            .state_columns
            .iter()
            .map(|column| column.name.clone())
            .collect::<Vec<_>>()
    } else {
        Vec::new()
    };
    let mut target_properties = vec![
        ("format-version".to_string(), "3".to_string()),
        ("write.row-lineage".to_string(), "true".to_string()),
        (
            ICEBERG_MV_PROP_APPLY_KEY_COLUMN.to_string(),
            apply_key_column_name.to_string(),
        ),
        (
            ICEBERG_MV_PROP_APPLY_KEY_SOURCE.to_string(),
            apply_key_source_property.to_string(),
        ),
        (
            ICEBERG_MV_PROP_APPLY_KEY_FIELD_ID.to_string(),
            expected_apply_key_field_id.to_string(),
        ),
    ];
    if !aggregate_state_hidden_columns.is_empty() {
        target_properties.push((
            ICEBERG_MV_PROP_HIDDEN_COLUMNS.to_string(),
            aggregate_state_hidden_columns.join(","),
        ));
    }
    crate::connector::iceberg::catalog::registry::create_table(
        &entry,
        &target.namespace,
        &target.table,
        &columns,
        None,
        partition_fields,
        &target_properties,
    )?;
    let post_create = (|| {
        entry.invalidate_table_cache(&target.namespace, &target.table);
        let target_loaded = crate::connector::iceberg::catalog::load_table(
            &entry,
            &target.namespace,
            &target.table,
        )?;
        let actual_apply_key_field_id =
            find_apply_key_field_id_by_column(&target_loaded.table, apply_key_column_name)?;
        if actual_apply_key_field_id != expected_apply_key_field_id {
            return Err(format!(
                "Iceberg MV target apply-key field id mismatch: expected {expected_apply_key_field_id}, got {actual_apply_key_field_id}"
            ));
        }

        // 3. Build A11 lineage from the resolved query and the base Iceberg schema.
        let schema_contract = build_iceberg_mv_schema_contract(
            &shape,
            &analysis,
            &loaded_bases,
            &target,
            &target_loaded,
            actual_apply_key_field_id,
        )?;

        // 4. Persist MV metadata in the repository.
        let primary_key_columns = stmt.primary_key.clone().unwrap_or_default();
        let provider = state
            .metadata_provider
            .as_ref()
            .ok_or_else(|| "metadata provider required for iceberg mv".to_string())?;
        let mut txn = provider
            .begin_write("create iceberg materialized view definition")
            .map_err(|e| format!("open iceberg mv definition transaction failed: {e}"))?;
        let mv_definition = state
            .mv_repo
            .create_definition(
                txn.as_mut(),
                CreateMvDefinitionRequest {
                    select_sql: canonical_select_query.to_string(),
                    base_table_refs: base_refs.iter().map(IcebergTableRef::fqn).collect(),
                    primary_key_columns: primary_key_columns.clone(),
                    storage_engine: ManagedMvStorageEngine::Iceberg.as_sql_str().to_string(),
                    target_catalog: Some(target.catalog.clone()),
                    target_namespace: Some(target.namespace.clone()),
                    target_table: Some(target.table.clone()),
                    schema_contract: Some(schema_contract.clone()),
                    partition_spec: schema_contract.target.partition.clone(),
                    created_at_ms,
                },
            )
            .map_err(|e| format!("create iceberg MV repository metadata failed: {e}"))?;
        state
            .mv_repo
            .update_refresh_metadata(
                txn.as_mut(),
                crate::engine::mv_flow::refresh_metadata_request_for_create(
                    mv_definition.mv_id,
                    &stmt.refresh_policy,
                ),
            )
            .map_err(|e| format!("create iceberg MV refresh metadata failed: {e}"))?;
        state
            .mv_repo
            .replace_dependencies_for_mv(
                txn.as_mut(),
                mv_definition.mv_id,
                resolved_dependencies.dependencies.clone(),
            )
            .map_err(|e| format!("create iceberg MV dependency metadata failed: {e}"))?;
        txn.commit()
            .map_err(|e| format!("commit iceberg MV repository metadata failed: {e}"))?;
        Ok::<(), String>(())
    })();
    if let Err(err) = post_create {
        return Err(cleanup_created_iceberg_mv_target_after_failure(
            &entry, &target, err,
        ));
    }
    register_iceberg_mv_target_in_catalog(state, &target)?;

    Ok(StatementResult::Ok)
}

fn cleanup_created_iceberg_mv_target_after_failure(
    entry: &crate::connector::iceberg::catalog::IcebergCatalogEntry,
    target: &IcebergMvTarget,
    err: String,
) -> String {
    let drop_result = crate::connector::iceberg::catalog::registry::drop_table(
        entry,
        &target.namespace,
        &target.table,
    );
    format!("{err}; target cleanup={drop_result:?}")
}

fn resolve_iceberg_mv_target(
    state: &Arc<StandaloneState>,
    current_catalog: Option<&str>,
    current_database: &str,
    stmt: &CreateMaterializedViewStmt,
) -> Result<IcebergMvTarget, String> {
    let current_catalog = current_catalog.ok_or_else(|| {
        "storage_engine='iceberg' requires current catalog to be an Iceberg catalog".to_string()
    })?;
    {
        let catalogs = state
            .iceberg_catalogs
            .read()
            .map_err(|e| format!("iceberg catalog registry read lock: {e}"))?;
        if !catalogs.contains_catalog(current_catalog)? {
            return Err(
                "storage_engine='iceberg' requires current catalog to be an Iceberg catalog"
                    .to_string(),
            );
        }
    }
    let (namespace, table) = resolve_mv_name(&stmt.name, current_database)?;
    Ok(IcebergMvTarget {
        catalog: crate::engine::catalog::normalize_identifier(current_catalog)?,
        namespace,
        table,
    })
}

fn iceberg_mv_target_exists(
    entry: &crate::connector::iceberg::catalog::IcebergCatalogEntry,
    namespace: &str,
    table: &str,
) -> Result<bool, String> {
    match crate::connector::iceberg::catalog::registry::list_tables(entry, namespace) {
        Ok(tables) => Ok(tables.iter().any(|name| name.eq_ignore_ascii_case(table))),
        Err(err)
            if err.contains("No such file")
                || err.contains("os error 2")
                || err.contains("not found")
                || err.contains("NotFound") =>
        {
            Ok(false)
        }
        Err(err) => Err(err),
    }
}

fn validate_join_shape_base_refs(
    shape: &crate::connector::starrocks::managed::mv_shape::JoinProjectionFilterMvShape,
    base_refs: &[IcebergTableRef],
) -> Result<(), String> {
    for name in [
        shape.left_table.to_string().to_ascii_lowercase(),
        shape.right_table.to_string().to_ascii_lowercase(),
    ] {
        if !base_refs
            .iter()
            .any(|base| base.fqn().eq_ignore_ascii_case(&name))
        {
            return Err(format!(
                "join MV shape references base {name} but analyzer resolved {base_refs:?}"
            ));
        }
    }
    Ok(())
}

fn is_join_projection_filter_mv(shape: &IncrementalMvShape) -> bool {
    matches!(shape, IncrementalMvShape::JoinProjectionFilter(_))
}

fn aggregate_shape_for_layout(shape: &IncrementalMvShape) -> Option<AggregateMvShape> {
    match shape {
        IncrementalMvShape::Aggregate(shape) => Some(shape.clone()),
        IncrementalMvShape::JoinAggregate(shape) => Some(shape.as_aggregate_shape_for_layout()),
        _ => None,
    }
}

fn iceberg_aggregate_target_columns(
    shape: &AggregateMvShape,
    output_columns: &[crate::sql::analysis::OutputColumn],
) -> Result<Vec<crate::sql::parser::ast::TableColumnDef>, String> {
    let layout = crate::connector::starrocks::managed::mv_agg_state::build_aggregate_mv_layout(
        shape,
        output_columns,
    )?;
    crate::connector::starrocks::managed::mv_ddl::validate_unique_aggregate_physical_column_names(
        &layout.physical_columns,
    )?;
    Ok(
        crate::connector::starrocks::managed::ddl::table_columns_from_physical_columns(
            &layout.physical_columns,
        ),
    )
}

fn build_iceberg_mv_schema_contract(
    shape: &IncrementalMvShape,
    analysis: &crate::connector::starrocks::managed::mv_ddl::MvAnalysis,
    loaded_bases: &[(
        IcebergTableRef,
        crate::connector::iceberg::catalog::IcebergLoadedTable,
    )],
    target: &IcebergMvTarget,
    target_loaded: &crate::connector::iceberg::catalog::IcebergLoadedTable,
    actual_apply_key_field_id: i32,
) -> Result<crate::meta::repository::mv_contract::MvSchemaContract, String> {
    let contract = match shape {
        IncrementalMvShape::ProjectionFilter(_) => {
            let [(base_ref, loaded_base)] = loaded_bases else {
                return Err(
                    "projection/filter iceberg MV schema contract requires one loaded base"
                        .to_string(),
                );
            };
            let lineage = crate::sql::analyzer::mv_lineage::build_projection_filter_lineage(
                &analysis.resolved_query,
                loaded_base.table.metadata().current_schema(),
            )?;
            crate::meta::repository::mv_contract::MvSchemaContract {
                contract_version: 1,
                base: base_contract(base_ref, loaded_base, None, lineage.base_fields.clone()),
                bases: vec![],
                output: crate::meta::repository::mv_contract::OutputContract {
                    columns: lineage.output_columns,
                    filter: lineage.filter,
                },
                join: None,
                aggregate: None,
                target: target_contract(
                    analysis,
                    target,
                    target_loaded,
                    actual_apply_key_field_id,
                    crate::meta::repository::mv_contract::HIDDEN_APPLY_KEY_COLUMN_NAME,
                    crate::meta::repository::mv_contract::ApplyKeySource::BaseRowId,
                )?,
            }
        }
        IncrementalMvShape::JoinProjectionFilter(join_shape) => {
            let (left_ref, left_loaded) =
                loaded_base_for_shape_table(loaded_bases, &join_shape.left_table)?;
            let (right_ref, right_loaded) =
                loaded_base_for_shape_table(loaded_bases, &join_shape.right_table)?;
            let left_schema = left_loaded.table.metadata().current_schema();
            let right_schema = right_loaded.table.metadata().current_schema();
            let left_fqn = left_ref.fqn();
            let right_fqn = right_ref.fqn();
            let join_lineage =
                crate::sql::analyzer::mv_lineage::build_join_projection_filter_lineage(
                    &analysis.resolved_query,
                    &[
                        (&left_fqn, &join_shape.left_alias, left_schema.as_ref()),
                        (&right_fqn, &join_shape.right_alias, right_schema.as_ref()),
                    ],
                )?;
            let left_fields = join_lineage
                .base_fields_by_table
                .get(&left_fqn)
                .cloned()
                .unwrap_or_default();
            let right_fields = join_lineage
                .base_fields_by_table
                .get(&right_fqn)
                .cloned()
                .unwrap_or_default();
            let left_contract = base_contract(
                left_ref,
                left_loaded,
                Some(join_shape.left_alias.clone()),
                left_fields,
            );
            let right_contract = base_contract(
                right_ref,
                right_loaded,
                Some(join_shape.right_alias.clone()),
                right_fields,
            );
            crate::meta::repository::mv_contract::MvSchemaContract {
                contract_version: 2,
                base: left_contract.clone(),
                bases: vec![left_contract, right_contract],
                output: crate::meta::repository::mv_contract::OutputContract {
                    columns: join_lineage.output_columns,
                    filter: join_lineage.filter,
                },
                join: Some(join_lineage.join),
                aggregate: None,
                target: target_contract(
                    analysis,
                    target,
                    target_loaded,
                    actual_apply_key_field_id,
                    crate::meta::repository::mv_contract::JOIN_APPLY_KEY_COLUMN_NAME,
                    crate::meta::repository::mv_contract::ApplyKeySource::JoinRowKey,
                )?,
            }
        }
        IncrementalMvShape::Aggregate(aggregate_shape) => {
            let [(base_ref, loaded_base)] = loaded_bases else {
                return Err(
                    "aggregate iceberg MV schema contract requires one loaded base".to_string(),
                );
            };
            let lineage = crate::sql::analyzer::mv_lineage::build_projection_filter_lineage(
                &analysis.resolved_query,
                loaded_base.table.metadata().current_schema(),
            )?;
            let layout =
                crate::connector::starrocks::managed::mv_agg_state::build_aggregate_mv_layout(
                    aggregate_shape,
                    &analysis.output_columns,
                )?;
            crate::meta::repository::mv_contract::MvSchemaContract {
                contract_version: 3,
                base: base_contract(base_ref, loaded_base, None, lineage.base_fields.clone()),
                bases: vec![],
                output: crate::meta::repository::mv_contract::OutputContract {
                    columns: lineage.output_columns,
                    filter: lineage.filter,
                },
                join: None,
                aggregate: Some(aggregate_contract(&layout, target_loaded)?),
                target: target_contract(
                    analysis,
                    target,
                    target_loaded,
                    actual_apply_key_field_id,
                    crate::meta::repository::mv_contract::GROUP_ROW_ID_APPLY_KEY_COLUMN_NAME,
                    crate::meta::repository::mv_contract::ApplyKeySource::GroupRowId,
                )?,
            }
        }
        IncrementalMvShape::JoinAggregate(join_aggregate_shape) => {
            let join_shape = &join_aggregate_shape.join;
            let (left_ref, left_loaded) =
                loaded_base_for_shape_table(loaded_bases, &join_shape.left_table)?;
            let (right_ref, right_loaded) =
                loaded_base_for_shape_table(loaded_bases, &join_shape.right_table)?;
            let left_schema = left_loaded.table.metadata().current_schema();
            let right_schema = right_loaded.table.metadata().current_schema();
            let left_fqn = left_ref.fqn();
            let right_fqn = right_ref.fqn();
            let join_lineage =
                crate::sql::analyzer::mv_lineage::build_join_projection_filter_lineage(
                    &analysis.resolved_query,
                    &[
                        (&left_fqn, &join_shape.left_alias, left_schema.as_ref()),
                        (&right_fqn, &join_shape.right_alias, right_schema.as_ref()),
                    ],
                )?;
            let left_fields = join_lineage
                .base_fields_by_table
                .get(&left_fqn)
                .cloned()
                .unwrap_or_default();
            let right_fields = join_lineage
                .base_fields_by_table
                .get(&right_fqn)
                .cloned()
                .unwrap_or_default();
            let left_contract = base_contract(
                left_ref,
                left_loaded,
                Some(join_shape.left_alias.clone()),
                left_fields,
            );
            let right_contract = base_contract(
                right_ref,
                right_loaded,
                Some(join_shape.right_alias.clone()),
                right_fields,
            );
            let aggregate_shape = join_aggregate_shape.as_aggregate_shape_for_layout();
            let layout =
                crate::connector::starrocks::managed::mv_agg_state::build_aggregate_mv_layout(
                    &aggregate_shape,
                    &analysis.output_columns,
                )?;
            crate::meta::repository::mv_contract::MvSchemaContract {
                contract_version: 3,
                base: left_contract.clone(),
                bases: vec![left_contract, right_contract],
                output: crate::meta::repository::mv_contract::OutputContract {
                    columns: join_lineage.output_columns,
                    filter: join_lineage.filter,
                },
                join: Some(join_lineage.join),
                aggregate: Some(aggregate_contract(&layout, target_loaded)?),
                target: target_contract(
                    analysis,
                    target,
                    target_loaded,
                    actual_apply_key_field_id,
                    crate::meta::repository::mv_contract::GROUP_ROW_ID_APPLY_KEY_COLUMN_NAME,
                    crate::meta::repository::mv_contract::ApplyKeySource::GroupRowId,
                )?,
            }
        }
    };
    contract
        .ensure_self_consistent()
        .map_err(|e| format!("Iceberg MV schema contract is self-inconsistent: {e}"))?;
    Ok(contract)
}

fn loaded_base_for_shape_table<'a>(
    loaded_bases: &'a [(
        IcebergTableRef,
        crate::connector::iceberg::catalog::IcebergLoadedTable,
    )],
    shape_table: &sqlparser::ast::ObjectName,
) -> Result<
    &'a (
        IcebergTableRef,
        crate::connector::iceberg::catalog::IcebergLoadedTable,
    ),
    String,
> {
    let table_name = shape_table.to_string();
    loaded_bases
        .iter()
        .find(|(base_ref, _)| base_ref.fqn().eq_ignore_ascii_case(&table_name))
        .ok_or_else(|| format!("join MV shape base {table_name} was not loaded"))
}

fn base_contract(
    base_ref: &IcebergTableRef,
    loaded_base: &crate::connector::iceberg::catalog::IcebergLoadedTable,
    alias_at_create: Option<String>,
    fields: Vec<crate::meta::repository::mv_contract::BaseFieldRecord>,
) -> crate::meta::repository::mv_contract::BaseContract {
    crate::meta::repository::mv_contract::BaseContract {
        table_fqn: base_ref.fqn(),
        table_uuid: loaded_base.table.metadata().uuid().to_string(),
        alias_at_create,
        schema_id_at_create: loaded_base.table.metadata().current_schema_id(),
        schema_at_create: crate::meta::repository::mv_contract::BaseSchemaSnapshot { fields },
    }
}

fn target_contract(
    analysis: &crate::connector::starrocks::managed::mv_ddl::MvAnalysis,
    target: &IcebergMvTarget,
    target_loaded: &crate::connector::iceberg::catalog::IcebergLoadedTable,
    actual_apply_key_field_id: i32,
    hidden_apply_key_column_name: &str,
    hidden_apply_key_source: crate::meta::repository::mv_contract::ApplyKeySource,
) -> Result<crate::meta::repository::mv_contract::TargetContract, String> {
    Ok(crate::meta::repository::mv_contract::TargetContract {
        table_fqn: format!("{}.{}.{}", target.catalog, target.namespace, target.table),
        table_uuid: target_loaded.table.metadata().uuid().to_string(),
        schema_id_at_create: target_loaded.table.metadata().current_schema_id(),
        visible_columns: analysis
            .output_columns
            .iter()
            .map(|col| {
                let field = target_loaded
                    .table
                    .metadata()
                    .current_schema()
                    .as_struct()
                    .fields()
                    .iter()
                    .find(|f| f.name.eq_ignore_ascii_case(&col.name))
                    .ok_or_else(|| {
                        format!(
                            "iceberg MV target schema is missing visible output column `{}`",
                            col.name
                        )
                    })?;
                Ok(crate::meta::repository::mv_contract::TargetVisibleColumn {
                    output_name: col.name.clone(),
                    target_field_id: field.id,
                    type_signature: format!("{}", field.field_type),
                    nullable: !field.required,
                })
            })
            .collect::<Result<Vec<_>, String>>()?,
        hidden_apply_key: crate::meta::repository::mv_contract::HiddenApplyKeyContract {
            column_name: hidden_apply_key_column_name.to_string(),
            target_field_id: actual_apply_key_field_id,
            source: hidden_apply_key_source,
        },
        partition: Some(target_partition_contract(target_loaded)?),
    })
}

fn target_partition_contract(
    target_loaded: &crate::connector::iceberg::catalog::IcebergLoadedTable,
) -> Result<crate::meta::repository::mv_contract::MvPartitionContract, String> {
    let metadata = target_loaded.table.metadata();
    let schema = metadata.current_schema();
    let spec = metadata.default_partition_spec();
    let mut fields = Vec::with_capacity(spec.fields().len());
    for field in spec.fields() {
        let source = schema.field_by_id(field.source_id).ok_or_else(|| {
            format!(
                "iceberg MV target partition field {} references missing target field id {}",
                field.name, field.source_id
            )
        })?;
        fields.push(
            crate::meta::repository::mv_contract::MvPartitionFieldContract {
                partition_field_id: field.field_id,
                partition_field_name: field.name.clone(),
                source_target_field_id: field.source_id,
                source_column_name: source.name.clone(),
                transform: mv_partition_transform_contract(&field.transform)?,
            },
        );
    }
    Ok(crate::meta::repository::mv_contract::MvPartitionContract {
        target_spec_id: spec.spec_id(),
        fields,
    })
}

fn mv_partition_transform_contract(
    transform: &iceberg::spec::Transform,
) -> Result<crate::meta::repository::mv_contract::MvPartitionTransformContract, String> {
    match transform {
        iceberg::spec::Transform::Identity => {
            Ok(crate::meta::repository::mv_contract::MvPartitionTransformContract::Identity)
        }
        iceberg::spec::Transform::Year => {
            Ok(crate::meta::repository::mv_contract::MvPartitionTransformContract::Year)
        }
        iceberg::spec::Transform::Month => {
            Ok(crate::meta::repository::mv_contract::MvPartitionTransformContract::Month)
        }
        iceberg::spec::Transform::Day => {
            Ok(crate::meta::repository::mv_contract::MvPartitionTransformContract::Day)
        }
        iceberg::spec::Transform::Hour => {
            Ok(crate::meta::repository::mv_contract::MvPartitionTransformContract::Hour)
        }
        iceberg::spec::Transform::Bucket(num_buckets) => Ok(
            crate::meta::repository::mv_contract::MvPartitionTransformContract::Bucket {
                num_buckets: *num_buckets,
            },
        ),
        iceberg::spec::Transform::Truncate(width) => Ok(
            crate::meta::repository::mv_contract::MvPartitionTransformContract::Truncate {
                width: *width,
            },
        ),
        iceberg::spec::Transform::Void => {
            Ok(crate::meta::repository::mv_contract::MvPartitionTransformContract::Void)
        }
        iceberg::spec::Transform::Unknown => {
            Err("iceberg MV target partition contract cannot persist unknown transform".to_string())
        }
    }
}

fn aggregate_contract(
    layout: &crate::connector::starrocks::managed::mv_agg_state::AggregateMvLayout,
    target_loaded: &crate::connector::iceberg::catalog::IcebergLoadedTable,
) -> Result<crate::meta::repository::mv_contract::AggregateStateContract, String> {
    let fields = target_loaded
        .table
        .metadata()
        .current_schema()
        .as_struct()
        .fields();
    let state_columns = layout
        .state_columns
        .iter()
        .map(|column| {
            let target_field = fields
                .iter()
                .find(|field| field.name.eq_ignore_ascii_case(&column.name))
                .ok_or_else(|| {
                    format!(
                        "Iceberg MV target aggregate state column {} is missing from target schema",
                        column.name
                    )
                })?;
            Ok(
                crate::meta::repository::mv_contract::AggregateStateColumnContract {
                    column_name: column.name.clone(),
                    target_field_id: target_field.id,
                    type_signature: format!("{}", target_field.field_type),
                    nullable: !target_field.required,
                    role: aggregate_state_role_contract(column.state_role),
                },
            )
        })
        .collect::<Result<Vec<_>, String>>()?;
    Ok(
        crate::meta::repository::mv_contract::AggregateStateContract {
            state_layout_version: 1,
            row_id_column_name: layout.row_id_column.column.name.clone(),
            state_columns,
        },
    )
}

fn aggregate_state_role_contract(
    role: crate::connector::starrocks::managed::mv_agg_state::AggregateStateRole,
) -> crate::meta::repository::mv_contract::AggregateStateRoleContract {
    match role {
        crate::connector::starrocks::managed::mv_agg_state::AggregateStateRole::Single => {
            crate::meta::repository::mv_contract::AggregateStateRoleContract::Single
        }
        crate::connector::starrocks::managed::mv_agg_state::AggregateStateRole::AvgSum => {
            crate::meta::repository::mv_contract::AggregateStateRoleContract::AvgSum
        }
        crate::connector::starrocks::managed::mv_agg_state::AggregateStateRole::AvgCount => {
            crate::meta::repository::mv_contract::AggregateStateRoleContract::AvgCount
        }
        crate::connector::starrocks::managed::mv_agg_state::AggregateStateRole::RetractionCount => {
            crate::meta::repository::mv_contract::AggregateStateRoleContract::RetractionCount
        }
    }
}

pub(crate) fn register_iceberg_mv_target_in_catalog(
    state: &Arc<StandaloneState>,
    target: &IcebergMvTarget,
) -> Result<(), String> {
    let entry = {
        let catalogs = state
            .iceberg_catalogs
            .read()
            .map_err(|e| format!("iceberg catalog registry read lock: {e}"))?;
        catalogs.get(&target.catalog)?
    };
    entry.invalidate_table_cache(&target.namespace, &target.table);
    let loaded =
        crate::connector::iceberg::catalog::load_table(&entry, &target.namespace, &target.table)?;
    let files = match loaded
        .table
        .metadata()
        .current_snapshot()
        .map(|s| s.snapshot_id())
    {
        Some(snapshot_id) => {
            crate::connector::iceberg::catalog::registry::extract_data_files_with_stats_at(
                &loaded.table,
                snapshot_id,
            )?
        }
        None => Vec::new(),
    };
    let has_data_files = !files.is_empty();
    let mut table_def = crate::connector::iceberg::catalog::build_iceberg_table_def_with_files(
        &entry,
        &target.namespace,
        &target.table,
        loaded,
        files,
    )?;
    if !has_data_files {
        table_def.iceberg_row_lineage_metadata_columns.clear();
    }
    let mut catalog = state
        .catalog
        .write()
        .map_err(|e| format!("standalone catalog write lock: {e}"))?;
    catalog.create_database(&target.namespace)?;
    catalog.register(&target.namespace, table_def)?;
    Ok(())
}

pub(crate) fn restore_iceberg_mv_targets(state: &Arc<StandaloneState>) -> Result<(), String> {
    let Some(provider) = state.metadata_provider.as_ref() else {
        return Ok(());
    };
    let read = provider
        .begin_read()
        .map_err(|e| format!("open iceberg MV restore transaction failed: {e}"))?;
    for mv in state
        .mv_repo
        .list_definitions(read.as_ref())
        .map_err(|e| format!("load MV definitions for iceberg restore failed: {e}"))?
        .into_iter()
        .filter(|mv| {
            mv.storage_engine
                .eq_ignore_ascii_case(ManagedMvStorageEngine::Iceberg.as_sql_str())
        })
    {
        let target = IcebergMvTarget {
            catalog: mv
                .target_catalog
                .ok_or_else(|| format!("iceberg MV {} missing target_catalog", mv.mv_id))?,
            namespace: mv
                .target_namespace
                .ok_or_else(|| format!("iceberg MV {} missing target_namespace", mv.mv_id))?,
            table: mv
                .target_table
                .ok_or_else(|| format!("iceberg MV {} missing target_table", mv.mv_id))?,
        };
        register_iceberg_mv_target_in_catalog(state, &target)?;
    }
    Ok(())
}

pub(crate) fn recover_iceberg_mv_refreshes(state: &Arc<StandaloneState>) -> Result<(), String> {
    let Some(provider) = state.metadata_provider.as_ref() else {
        return Ok(());
    };
    let read = provider
        .begin_read()
        .map_err(|e| format!("open iceberg MV refresh recovery read transaction failed: {e}"))?;
    let unfinished = state
        .mv_repo
        .list_unfinished_branch_staged_iceberg_refreshes(read.as_ref())
        .map_err(|e| format!("load unfinished iceberg MV refreshes failed: {e}"))?;
    drop(read);
    for refresh in unfinished {
        recover_one_iceberg_mv_refresh(state, refresh)?;
    }
    Ok(())
}

fn recover_one_iceberg_mv_refresh(
    state: &Arc<StandaloneState>,
    refresh: StoredMvRefresh,
) -> Result<(), String> {
    let target =
        IcebergMvTarget {
            catalog: refresh.target_catalog.clone().ok_or_else(|| {
                format!("mv refresh {} missing target catalog", refresh.refresh_id)
            })?,
            namespace: refresh.target_namespace.clone().ok_or_else(|| {
                format!("mv refresh {} missing target namespace", refresh.refresh_id)
            })?,
            table: refresh
                .target_table
                .clone()
                .ok_or_else(|| format!("mv refresh {} missing target table", refresh.refresh_id))?,
        };
    let (entry, catalog, loaded) = load_iceberg_mv_target(state, &target)?;
    reconcile_iceberg_mv_refresh(state, refresh, &target, &entry, &catalog, &loaded.table)
}

pub(crate) fn resolve_refresh_target(
    current_catalog: Option<&str>,
    current_database: &str,
    name: &ObjectName,
) -> Result<IcebergMvTarget, String> {
    let catalog = current_catalog.ok_or_else(|| {
        "REFRESH MATERIALIZED VIEW for an Iceberg MV requires current Iceberg catalog context"
            .to_string()
    })?;
    let (namespace, table) = resolve_mv_name(name, current_database)?;
    Ok(IcebergMvTarget {
        catalog: crate::engine::catalog::normalize_identifier(catalog)?,
        namespace,
        table,
    })
}

fn load_iceberg_mv_target(
    state: &Arc<StandaloneState>,
    target: &IcebergMvTarget,
) -> Result<
    (
        crate::connector::iceberg::catalog::IcebergCatalogEntry,
        Arc<dyn iceberg::Catalog>,
        crate::connector::iceberg::catalog::IcebergLoadedTable,
    ),
    String,
> {
    let entry = {
        let catalogs = state
            .iceberg_catalogs
            .read()
            .map_err(|e| format!("iceberg catalog registry read lock: {e}"))?;
        catalogs.get(&target.catalog)?
    };
    entry.invalidate_table_cache(&target.namespace, &target.table);
    let catalog = crate::connector::iceberg::catalog::registry::build_iceberg_catalog(&entry)?;
    let loaded =
        crate::connector::iceberg::catalog::load_table(&entry, &target.namespace, &target.table)?;
    Ok((entry, catalog, loaded))
}

fn reload_iceberg_mv_target_table(
    entry: &crate::connector::iceberg::catalog::IcebergCatalogEntry,
    target: &IcebergMvTarget,
) -> Result<iceberg::table::Table, String> {
    entry.invalidate_table_cache(&target.namespace, &target.table);
    crate::connector::iceberg::catalog::load_table(entry, &target.namespace, &target.table)
        .map(|loaded| loaded.table)
}

fn iceberg_mv_table_ident(target: &IcebergMvTarget) -> Result<TableIdent, String> {
    TableIdent::from_strs([target.namespace.as_str(), target.table.as_str()])
        .map_err(|e| format!("build mv iceberg ident failed: {e}"))
}

fn validate_target_snapshot(
    target: &IcebergMvTarget,
    mv_definition: &StoredMvDefinition,
    table: &iceberg::table::Table,
) -> Result<(), String> {
    let actual = table.metadata().current_snapshot().map(|s| s.snapshot_id());
    let expected = mv_definition.last_refreshed_iceberg_snapshot_id;
    if actual != expected {
        return Err(format!(
            "target table {}.{}.{} was modified outside NovaRocks: expected snapshot {:?}, current snapshot {:?}",
            target.catalog, target.namespace, target.table, expected, actual
        ));
    }
    Ok(())
}

fn recorded_target_snapshot_id(
    target: &IcebergMvTarget,
    mv_definition: &StoredMvDefinition,
) -> Result<i64, String> {
    mv_definition
        .last_refreshed_iceberg_snapshot_id
        .ok_or_else(|| {
            format!(
                "iceberg materialized view {}.{}.{} has no recorded target snapshot",
                target.catalog, target.namespace, target.table
            )
        })
}

fn rewrite_full_refresh_select_with_pin(
    select_sql: &str,
    pin: &crate::connector::starrocks::managed::refresh_pin::RefreshSnapshotPin,
    base_ref: &IcebergTableRef,
) -> Result<String, String> {
    let normalized = crate::sql::parser::dialect::normalize_for_raw_parse(select_sql)
        .map_err(|e| format!("iceberg MV full refresh pin SELECT normalize error: {e}"))?;
    let mut stmt = crate::sql::parser::parse_normalized_sql_raw(&normalized)
        .map_err(|e| format!("iceberg MV full refresh pin SELECT parse error: {e}"))?;
    let sqlparser::ast::Statement::Query(query) = &mut stmt else {
        return Err("iceberg MV full refresh pin SELECT expects a SELECT query".to_string());
    };
    crate::connector::starrocks::managed::refresh_pin::inject_pin_as_for_version_as_of(
        query,
        pin,
        &HashSet::new(),
        Some(&base_ref.catalog),
        &base_ref.namespace,
    )?;
    Ok(stmt.to_string())
}

fn iceberg_aggregate_first_refresh_select_sql(
    select_sql: &str,
    shape: &crate::connector::starrocks::managed::mv_shape::AggregateMvShape,
) -> Result<String, String> {
    crate::connector::starrocks::managed::mv_shape::rewrite_select_sql_for_state(select_sql, shape)
}

fn iceberg_aggregate_incremental_delta_select_sql(
    select_sql: &str,
    shape: &crate::connector::starrocks::managed::mv_shape::AggregateMvShape,
    change_op_qualifier: Option<&str>,
) -> Result<String, String> {
    crate::connector::starrocks::managed::ivm_delta_aggregate::rewrite_select_sql_for_signed_delta_state_with_change_op_qualifier(
        select_sql,
        shape,
        change_op_qualifier,
    )
}

fn iceberg_join_aggregate_branch_delta_sql(
    select_sql: &str,
    shape: &JoinAggregateMvShape,
    delta_side: crate::engine::mv::iceberg_join_branch::BranchDeltaSide,
) -> Result<String, String> {
    let delta_alias = match delta_side {
        crate::engine::mv::iceberg_join_branch::BranchDeltaSide::Left => {
            shape.join.left_alias.as_str()
        }
        crate::engine::mv::iceberg_join_branch::BranchDeltaSide::Right => {
            shape.join.right_alias.as_str()
        }
    };
    iceberg_aggregate_incremental_delta_select_sql(
        select_sql,
        &shape.as_aggregate_shape_for_layout(),
        Some(delta_alias),
    )
}

/// Refresh an iceberg-backed materialized view.
///
/// Strategy dispatch:
/// - (None, None)         → no-op (base table is empty / has no snapshot)
/// - (None, Some(cur))    → first refresh: run SELECT, write parquet, commit snapshot
/// - (Some(p), Some(c)) p == c → no-op metadata refresh (bump last_refresh_ms)
/// - (Some(p), Some(c)) p != c → incremental: append-delta SELECT → fast-append MV snapshot
/// - (Some(p), None)      → fail-fast (base snapshot was garbage-collected)
pub(crate) fn refresh_iceberg_mv(
    state: &Arc<StandaloneState>,
    current_catalog: Option<&str>,
    current_database: &str,
    stmt: &RefreshMaterializedViewStmt,
) -> Result<StatementResult, String> {
    let _refresh_guard = acquire_mv_refresh_lock()?;
    let target = resolve_refresh_target(current_catalog, current_database, &stmt.name)?;
    if stmt.full {
        // REFRESH FULL is intentionally disabled. The previous implementation
        // dropped the target table, deleted the MV definition, and re-ran
        // create_iceberg_mv — but create_iceberg_mv leaves the new target
        // empty, so the user-visible effect was "MV is now empty" rather
        // than the intuitive "MV is fully repopulated". On top of that the
        // operation was non-atomic and silently lost partition_by metadata.
        // This is too misleading to ship as a single keyword and needs a
        // ground-up redesign (clearer name like REBUILD, atomic semantics,
        // explicit data-repopulation step, full DDL preservation).
        //
        // Until that redesign lands, fail fast and require the operator to
        // do the recovery by hand — no silent high-risk side effects.
        return Err(
            "REFRESH MATERIALIZED VIEW ... FULL is currently disabled pending redesign; \
             its previous behavior (drop target + delete definition + recreate empty target) \
             was misleading and non-atomic. To recover from a broken contract or corrupted \
             target, run DROP MATERIALIZED VIEW <name>; CREATE MATERIALIZED VIEW <name> ...; \
             REFRESH MATERIALIZED VIEW <name>; manually."
                .to_string(),
        );
    }
    recover_iceberg_mv_refreshes(state)?;
    let mv_definition = load_iceberg_mv_definition_by_target(state, &target)?;
    let (target_entry, iceberg_catalog, target_loaded) = load_iceberg_mv_target(state, &target)?;
    validate_target_snapshot(&target, &mv_definition, &target_loaded.table)?;
    let base_refs = parse_iceberg_table_refs(&mv_definition.base_table_refs)?;
    let canonical_select_query = canonicalize_iceberg_mv_select_query(
        &parse_mv_select_query(&mv_definition.select_sql)?,
        current_catalog,
        current_database,
    );
    let shape = classify_incremental_mv_query(&canonical_select_query)?;
    if let Some(aggregate_shape) = aggregate_shape_for_layout(&shape) {
        return refresh_iceberg_aggregate_mv(
            state,
            &target,
            &target_entry,
            &iceberg_catalog,
            &target_loaded.table,
            expected_main_snapshot_id_from_table(&target_loaded.table),
            current_catalog,
            current_database,
            &mv_definition,
            &base_refs,
            &shape,
            &aggregate_shape,
        );
    }
    if is_join_projection_filter_mv(&shape) {
        let IncrementalMvShape::JoinProjectionFilter(join_shape) = &shape else {
            unreachable!("checked join shape above");
        };
        return refresh_iceberg_join_mv(
            state,
            &target,
            &target_entry,
            &iceberg_catalog,
            &target_loaded.table,
            expected_main_snapshot_id_from_table(&target_loaded.table),
            current_database,
            &mv_definition,
            &base_refs,
            join_shape,
        );
    }
    if !matches!(shape, IncrementalMvShape::ProjectionFilter(_)) {
        return Err(
            "iceberg materialized view refresh only supports projection/filter or join projection/filter shapes"
                .to_string(),
        );
    }
    let [base_ref] = base_refs.as_slice() else {
        return Err(
            "iceberg materialized view refresh requires exactly one base table reference"
                .to_string(),
        );
    };
    let schema_contract = mv_definition.schema_contract.as_ref().ok_or_else(|| {
        format!(
            "iceberg MV target {}.{}.{} is missing A11 schema contract; rebuild or recreate the MV",
            target.catalog, target.namespace, target.table
        )
    })?;
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
    let is_empty_base_noop =
        previous_snapshot_id.is_none() && current_snapshot_id_before_pin.is_none();

    if is_empty_base_noop {
        ensure_schema_contract_compatible_for_refresh(
            schema_contract,
            &pre_pin_loaded.table,
            &target_loaded.table,
        )?;
        tracing::info!(
            "iceberg mv {}.{}.{}: base table has no snapshot; skipping refresh",
            target.catalog,
            target.namespace,
            target.table
        );
        return Ok(StatementResult::Ok);
    }

    let pin = crate::connector::starrocks::managed::refresh_pin::RefreshSnapshotPin::capture(
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

    // A11 contract guard. Validate the full base ↔ output ↔ target
    // contract after the refresh pin is captured and the base table is
    // reloaded. Use the current table schema, not the pinned data snapshot's
    // schema: Iceberg schema evolution can be metadata-only and leave the
    // current snapshot id unchanged. validate_schema_contract subsumes the earlier
    // ensure_base_row_lineage_contract check (it already enforces v3 +
    // row-lineage).
    let effective_definition = match crate::engine::mv::schema_contract::validate_schema_contract(
        schema_contract,
        &loaded.table,
        &target_loaded.table,
    ) {
        crate::engine::mv::schema_contract::ContractDecision::Incompatible(err) => {
            return Err(format!("{err}"));
        }
        crate::engine::mv::schema_contract::ContractDecision::CompatibleSafeWithRebind {
            rebound_columns,
        } => {
            tracing::info!(
                target = ?target,
                rebound = ?rebound_columns,
                "iceberg MV refresh: base columns rebound by field id; rewriting select_sql",
            );
            let rewritten_sql =
                rewrite_select_sql_for_rebind(&mv_definition.select_sql, &rebound_columns)?;
            let mut def = mv_definition.clone();
            def.select_sql = rewritten_sql;
            def
        }
        crate::engine::mv::schema_contract::ContractDecision::CompatibleSafe => {
            mv_definition.clone()
        }
    };
    let mv_definition = &effective_definition;
    let pinned_full_select_sql =
        rewrite_full_refresh_select_with_pin(&mv_definition.select_sql, &pin, base_ref)?;
    let expected_main_snapshot_id = target_loaded
        .table
        .metadata()
        .current_snapshot()
        .map(|s| s.snapshot_id());
    let staging_branch = format!(
        "__nova_mv_refresh_{}_{}",
        mv_definition.mv_id,
        uuid::Uuid::new_v4().simple()
    );

    if let Some(previous_uuid) = mv_definition.last_refresh_table_uuids.get(&base_ref.fqn())
        && previous_uuid != &current_table_uuid
    {
        return Err(format!(
            "iceberg MV base table identity changed for {}; incremental refresh is unsafe, rebuild or recreate the MV",
            base_ref.fqn()
        ));
    }

    let ctx = IcebergMvRefreshContext::new(
        target.clone(),
        mv_definition.mv_id,
        current_catalog,
        current_database,
        Arc::new(mv_definition.clone()),
        Arc::new(canonical_select_query.clone()),
        Arc::from(base_refs.clone()),
        Arc::new(pin.clone()),
        Arc::new(target_entry.clone()),
        iceberg_catalog.clone(),
        target_loaded.table.clone(),
    )?;
    tracing::info!(
        summary = ?ctx.rewrite.summary(),
        "iceberg MV refresh context constructed"
    );

    match (previous_snapshot_id, current_snapshot_id) {
        // Base table has no snapshot yet — nothing to refresh.
        (None, None) => {
            tracing::info!(
                "iceberg mv {}.{}.{}: base table has no snapshot; skipping refresh",
                target.catalog,
                target.namespace,
                target.table
            );
            Ok(StatementResult::Ok)
        }

        // First refresh: base table now has a snapshot but we haven't run yet.
        (None, Some(cur)) => {
            let refresh_id = begin_staged_iceberg_mv_refresh_intent(
                state,
                &target,
                mv_definition.mv_id,
                expected_main_snapshot_id,
                pin.to_snapshot_map(),
                &staging_branch,
            )?;
            first_refresh_iceberg_mv(
                state,
                &ctx,
                &staging_branch,
                refresh_id,
                base_ref,
                cur,
                &current_table_uuid,
                &pinned_full_select_sql,
            )
        }

        // No-op: base table snapshot has not advanced.
        (Some(prev), Some(cur)) if prev == cur => {
            tracing::info!(
                "iceberg mv {}.{}.{}: base snapshot {cur} unchanged; updating metadata only",
                target.catalog,
                target.namespace,
                target.table
            );
            let snapshots = pin.to_snapshot_map();
            let table_uuids = pin.to_table_uuid_map();
            let target_snapshot_id = recorded_target_snapshot_id(&target, mv_definition)?;
            let refresh_id =
                begin_iceberg_mv_refresh_intent(state, mv_definition.mv_id, snapshots.clone())?;
            finalize_iceberg_mv_refresh(
                state,
                refresh_id,
                mv_definition.last_refresh_rows.unwrap_or(0),
                snapshots.clone(),
                table_uuids.clone(),
                target_snapshot_id,
            )?;
            Ok(StatementResult::Ok)
        }

        // Incremental: base snapshot has advanced.
        (Some(prev), Some(cur)) => incremental_refresh_iceberg_mv(
            state,
            &ctx,
            base_ref,
            prev,
            cur,
            &loaded.table,
            &current_table_uuid,
            &pinned_full_select_sql,
        ),

        // Previous snapshot no longer reachable.
        (Some(prev), None) => Err(format!(
            "cannot refresh iceberg materialized view {}.{}.{}: \
             previously-refreshed base snapshot {prev} is no longer reachable",
            target.catalog, target.namespace, target.table
        )),
    }
}

#[allow(clippy::too_many_arguments)]
fn refresh_iceberg_aggregate_mv(
    state: &Arc<StandaloneState>,
    target: &IcebergMvTarget,
    target_entry: &crate::connector::iceberg::catalog::IcebergCatalogEntry,
    iceberg_catalog: &Arc<dyn iceberg::Catalog>,
    target_table: &iceberg::table::Table,
    expected_main_snapshot_id: Option<i64>,
    current_catalog: Option<&str>,
    current_database: &str,
    mv_definition: &StoredMvDefinition,
    base_refs: &[IcebergTableRef],
    shape: &IncrementalMvShape,
    aggregate_shape: &AggregateMvShape,
) -> Result<StatementResult, String> {
    let schema_contract = validate_aggregate_schema_contract_metadata(target, mv_definition)?;
    match shape {
        IncrementalMvShape::Aggregate(_) => refresh_single_aggregate_iceberg_mv(
            state,
            target,
            target_entry,
            iceberg_catalog,
            target_table,
            expected_main_snapshot_id,
            current_catalog,
            current_database,
            mv_definition,
            base_refs,
            schema_contract,
            aggregate_shape,
        ),
        IncrementalMvShape::JoinAggregate(join_aggregate_shape) => {
            refresh_join_aggregate_iceberg_mv(
                state,
                target,
                target_entry,
                iceberg_catalog,
                target_table,
                expected_main_snapshot_id,
                current_catalog,
                current_database,
                mv_definition,
                base_refs,
                schema_contract,
                join_aggregate_shape,
                aggregate_shape,
            )
        }
        _ => Err(
            "iceberg aggregate MV refresh dispatcher requires aggregate or join aggregate shape"
                .to_string(),
        ),
    }
}

fn validate_aggregate_schema_contract_metadata<'a>(
    target: &IcebergMvTarget,
    mv_definition: &'a StoredMvDefinition,
) -> Result<&'a crate::meta::repository::mv_contract::MvSchemaContract, String> {
    let schema_contract = mv_definition.schema_contract.as_ref().ok_or_else(|| {
        format!(
            "iceberg MV target {}.{}.{} is missing A11 schema contract; rebuild or recreate the MV",
            target.catalog, target.namespace, target.table
        )
    })?;
    if schema_contract.contract_version != 3 {
        return Err(format!(
            "iceberg aggregate MV {}.{}.{} requires schema contract version 3, got {}",
            target.catalog, target.namespace, target.table, schema_contract.contract_version
        ));
    }
    if schema_contract.aggregate.is_none() {
        return Err(format!(
            "iceberg aggregate MV {}.{}.{} is missing aggregate schema contract; recreate the MV",
            target.catalog, target.namespace, target.table
        ));
    }
    Ok(schema_contract)
}

#[allow(clippy::too_many_arguments)]
fn refresh_single_aggregate_iceberg_mv(
    state: &Arc<StandaloneState>,
    target: &IcebergMvTarget,
    target_entry: &crate::connector::iceberg::catalog::IcebergCatalogEntry,
    iceberg_catalog: &Arc<dyn iceberg::Catalog>,
    target_table: &iceberg::table::Table,
    expected_main_snapshot_id: Option<i64>,
    current_catalog: Option<&str>,
    current_database: &str,
    mv_definition: &StoredMvDefinition,
    base_refs: &[IcebergTableRef],
    schema_contract: &crate::meta::repository::mv_contract::MvSchemaContract,
    aggregate_shape: &AggregateMvShape,
) -> Result<StatementResult, String> {
    let [base_ref] = base_refs else {
        return Err(
            "iceberg aggregate materialized view refresh requires exactly one base table reference"
                .to_string(),
        );
    };
    let pre_pin_loaded = load_current_iceberg_base_table(state, base_ref)?;
    let current_before_pin = expected_main_snapshot_id_from_table(&pre_pin_loaded.table);
    let previous = mv_definition
        .last_refresh_snapshots
        .get(&base_ref.fqn())
        .copied();

    match (previous, current_before_pin) {
        (None, None) => {
            tracing::info!(
                "iceberg aggregate mv {}.{}.{}: base table has no snapshot; skipping refresh",
                target.catalog,
                target.namespace,
                target.table
            );
            return Ok(StatementResult::Ok);
        }
        (Some(prev), None) => {
            return Err(format!(
                "cannot refresh iceberg aggregate materialized view {}.{}.{}: previously-refreshed base snapshot {prev} is no longer reachable",
                target.catalog, target.namespace, target.table
            ));
        }
        _ => {}
    }

    let pin = crate::connector::starrocks::managed::refresh_pin::RefreshSnapshotPin::capture(
        state, base_refs,
    )?;
    validate_refresh_pin_table_uuids(mv_definition, &pin, base_refs)?;
    let current = pin
        .get(base_ref)
        .ok_or_else(|| format!("missing refresh pin for {}", base_ref.fqn()))?;
    let loaded = load_current_iceberg_base_table(state, base_ref)?;
    let mut rebind_happened = false;
    let effective_definition = match crate::engine::mv::schema_contract::validate_schema_contract(
        schema_contract,
        &loaded.table,
        target_table,
    ) {
        crate::engine::mv::schema_contract::ContractDecision::Incompatible(err) => {
            return Err(format!("{err}"));
        }
        crate::engine::mv::schema_contract::ContractDecision::CompatibleSafeWithRebind {
            rebound_columns,
        } => {
            tracing::info!(
                target = ?target,
                rebound = ?rebound_columns,
                "iceberg aggregate MV refresh: base columns rebound by field id; rewriting select_sql",
            );
            let rewritten_sql =
                rewrite_select_sql_for_rebind(&mv_definition.select_sql, &rebound_columns)?;
            let mut def = mv_definition.clone();
            def.select_sql = rewritten_sql;
            rebind_happened = true;
            def
        }
        crate::engine::mv::schema_contract::ContractDecision::CompatibleSafe => {
            mv_definition.clone()
        }
    };
    let mv_definition = &effective_definition;
    // When rebind rewrote stored SELECT, the original `aggregate_shape`
    // captured by `plan_iceberg_aggregate_mv_refresh` still references the
    // pre-rebind base column names. Reclassify against the rewritten SQL so
    // downstream signed-delta/full-state rewrites consistently use the
    // current base column names.
    let reclassified_aggregate_shape = if rebind_happened {
        let new_query = parse_mv_select_query(&mv_definition.select_sql)?;
        let canonical_new =
            canonicalize_iceberg_mv_select_query(&new_query, current_catalog, current_database);
        let new_shape =
            crate::connector::starrocks::managed::mv_shape::classify_incremental_mv_query(
                &canonical_new,
            )?;
        aggregate_shape_for_layout(&new_shape).ok_or_else(|| {
            "iceberg aggregate MV rebind broke aggregate classification".to_string()
        })?
    } else {
        aggregate_shape.clone()
    };
    let aggregate_shape = &reclassified_aggregate_shape;

    match previous {
        None => {
            let staging_branch = format!(
                "__nova_mv_refresh_{}_{}",
                mv_definition.mv_id,
                uuid::Uuid::new_v4().simple()
            );
            let refresh_id = begin_staged_iceberg_mv_refresh_intent(
                state,
                target,
                mv_definition.mv_id,
                expected_main_snapshot_id,
                pin.to_snapshot_map(),
                &staging_branch,
            )?;
            first_refresh_iceberg_aggregate_mv(
                state,
                target,
                target_entry,
                iceberg_catalog,
                expected_main_snapshot_id,
                &staging_branch,
                refresh_id,
                current_catalog,
                current_database,
                mv_definition,
                aggregate_shape,
                &pin,
            )
        }
        Some(prev) if prev == current => {
            tracing::info!(
                "iceberg aggregate mv {}.{}.{}: base snapshot {current} unchanged; updating metadata only",
                target.catalog,
                target.namespace,
                target.table
            );
            finalize_iceberg_mv_metadata_only_refresh(
                state,
                target,
                mv_definition,
                pin.to_snapshot_map(),
                pin.to_table_uuid_map(),
            )
        }
        Some(prev) => incremental_refresh_iceberg_aggregate_mv(
            state,
            target,
            target_entry,
            iceberg_catalog,
            target_table,
            expected_main_snapshot_id,
            current_catalog,
            current_database,
            mv_definition,
            schema_contract,
            base_ref,
            prev,
            current,
            &loaded,
            aggregate_shape,
            &pin,
        ),
    }
}

#[allow(clippy::too_many_arguments)]
fn incremental_refresh_iceberg_aggregate_mv(
    state: &Arc<StandaloneState>,
    target: &IcebergMvTarget,
    target_entry: &crate::connector::iceberg::catalog::IcebergCatalogEntry,
    iceberg_catalog: &Arc<dyn iceberg::Catalog>,
    target_table: &iceberg::table::Table,
    expected_main_snapshot_id: Option<i64>,
    current_catalog: Option<&str>,
    current_database: &str,
    mv_definition: &StoredMvDefinition,
    _schema_contract: &crate::meta::repository::mv_contract::MvSchemaContract,
    base_ref: &IcebergTableRef,
    previous_snapshot_id: i64,
    current_snapshot_id: i64,
    loaded_base: &crate::connector::iceberg::catalog::IcebergLoadedTable,
    aggregate_shape: &AggregateMvShape,
    pin: &crate::connector::starrocks::managed::refresh_pin::RefreshSnapshotPin,
) -> Result<StatementResult, String> {
    let batch = match plan_changes(
        &loaded_base.table,
        previous_snapshot_id,
        Some(current_snapshot_id),
        &[],
    ) {
        Ok(batch) => batch,
        Err(err) => match policy_signal_from_change_error(&err) {
            IcebergChangePolicySignal::FullRefresh { reason } => {
                return Err(format!(
                    "iceberg aggregate MV {}.{}.{} cannot refresh incrementally and automatic full rebuild is disabled: {reason}",
                    target.catalog, target.namespace, target.table
                ));
            }
            IcebergChangePolicySignal::Unsupported { reason } => {
                return Err(format!(
                    "iceberg aggregate MV {}.{}.{} incremental refresh unsupported: {reason}",
                    target.catalog, target.namespace, target.table
                ));
            }
            IcebergChangePolicySignal::Incremental => {
                return Err(format!(
                    "iceberg aggregate MV {}.{}.{} produced invalid incremental policy from change planner: {err}",
                    target.catalog, target.namespace, target.table
                ));
            }
        },
    };
    if batch.current_snapshot_id != current_snapshot_id {
        return Err(format!(
            "iceberg aggregate MV incremental refresh: change batch snapshot mismatch (expected {current_snapshot_id}, got {})",
            batch.current_snapshot_id,
        ));
    }

    let has_delete_changes = iceberg_change_batch_has_row_deletes(&batch);
    let is_empty_delta = batch.inserts.is_empty() && !has_delete_changes;
    if is_empty_delta {
        tracing::info!(
            "iceberg aggregate mv {}.{}.{}: incremental delta is empty; updating metadata only",
            target.catalog,
            target.namespace,
            target.table
        );
        return finalize_iceberg_mv_metadata_only_refresh(
            state,
            target,
            mv_definition,
            pin.to_snapshot_map(),
            pin.to_table_uuid_map(),
        );
    }

    let source_files =
        crate::connector::starrocks::managed::ivm_delta_source::build_delta_source_files(
            crate::connector::starrocks::managed::ivm_delta_source::IvmDeltaSourceInput {
                state,
                current_database,
                base_ref,
                loaded: loaded_base,
            },
            batch,
        )?;
    if source_files.previous_snapshot_id != previous_snapshot_id
        || source_files.current_snapshot_id != current_snapshot_id
    {
        return Err(format!(
            "iceberg aggregate MV incremental refresh delta source snapshot window mismatch: expected {} -> {}, got {} -> {}",
            previous_snapshot_id,
            current_snapshot_id,
            source_files.previous_snapshot_id,
            source_files.current_snapshot_id
        ));
    }
    if source_files.files.is_empty() {
        tracing::info!(
            "iceberg aggregate mv {}.{}.{}: delta source has no materialized rows; updating metadata only",
            target.catalog,
            target.namespace,
            target.table
        );
        return finalize_iceberg_mv_metadata_only_refresh(
            state,
            target,
            mv_definition,
            pin.to_snapshot_map(),
            pin.to_table_uuid_map(),
        );
    }

    let layout = build_aggregate_layout_for_refresh(
        state,
        current_catalog,
        current_database,
        mv_definition,
        aggregate_shape,
    )?;
    let signed_sql = iceberg_aggregate_incremental_delta_select_sql(
        &mv_definition.select_sql,
        aggregate_shape,
        None,
    )?;
    let delta_result =
        crate::connector::starrocks::managed::ivm_delta_source::execute_delta_source_query(
            crate::connector::starrocks::managed::ivm_delta_source::IvmDeltaSourceInput {
                state,
                current_database,
                base_ref,
                loaded: loaded_base,
            },
            &signed_sql,
            source_files,
        )?;
    let delta_chunks =
        crate::connector::starrocks::managed::mv_agg_state::materialize_aggregate_result_chunks(
            delta_result,
            &layout,
            aggregate_shape,
        )?;
    if delta_chunks.iter().all(|chunk| chunk.batch.num_rows() == 0) {
        return finalize_iceberg_mv_metadata_only_refresh(
            state,
            target,
            mv_definition,
            pin.to_snapshot_map(),
            pin.to_table_uuid_map(),
        );
    }

    apply_iceberg_aggregate_delta_chunks(
        state,
        target,
        target_entry,
        iceberg_catalog,
        target_table,
        expected_main_snapshot_id,
        mv_definition,
        _schema_contract,
        &layout,
        &delta_chunks,
        pin.to_snapshot_map(),
        pin.to_table_uuid_map(),
    )
}

fn target_fqn_string(target: &IcebergMvTarget) -> String {
    format!("{}.{}.{}", target.catalog, target.namespace, target.table)
}

fn partition_filter_label(
    filter: &crate::engine::mv::partition::TargetPartitionFilter,
) -> &'static str {
    match filter {
        crate::engine::mv::partition::TargetPartitionFilter::None => "none",
        crate::engine::mv::partition::TargetPartitionFilter::AllowList(_) => "allow_list",
    }
}

fn partition_filter_count(
    filter: &crate::engine::mv::partition::TargetPartitionFilter,
) -> Option<usize> {
    match filter {
        crate::engine::mv::partition::TargetPartitionFilter::None => None,
        crate::engine::mv::partition::TargetPartitionFilter::AllowList(set) => Some(set.len()),
    }
}

fn wrap_aggregate_apply_error(target_fqn: &str, mv_id: i64, cause: String) -> String {
    tracing::error!(
        event = "iceberg_aggregate_mv.partition_derivation_failed",
        mv_id = mv_id,
        target_fqn = %target_fqn,
        reason = %cause,
        "iceberg aggregate MV apply failed"
    );
    format!("iceberg aggregate MV apply failed (target={target_fqn}, mv_id={mv_id}): {cause}")
}

/// Inputs for the `iceberg_aggregate_mv.apply` tracing event. Extracted so
/// the emission site is unit-testable without standing up the full apply
/// path (staging branch + commit + publish). The end-to-end emission still
/// fires from `apply_iceberg_aggregate_delta_chunks` and is exercised by the
/// `iceberg-ivm` SQL suite.
#[derive(Debug)]
pub(crate) struct AggregateApplyEvent<'a> {
    pub(crate) target_fqn: &'a str,
    pub(crate) mv_id: i64,
    pub(crate) partition_filter: &'a crate::engine::mv::partition::TargetPartitionFilter,
    pub(crate) touched_group_count: usize,
    pub(crate) lookup_stats:
        &'a crate::engine::mv::iceberg_aggregate_state::AggregateStateLookupStats,
    pub(crate) delete_row_count: usize,
    pub(crate) insert_chunk_row_count: usize,
    pub(crate) new_total_rows: i64,
    pub(crate) iceberg_snapshot: i64,
}

fn emit_aggregate_apply_event(event: &AggregateApplyEvent<'_>) {
    tracing::info!(
        event = "iceberg_aggregate_mv.apply",
        mv_id = event.mv_id,
        target_fqn = %event.target_fqn,
        partition_filter = partition_filter_label(event.partition_filter),
        affected_partition_count = partition_filter_count(event.partition_filter).unwrap_or(0),
        touched_group_count = event.touched_group_count,
        planned_file_count = event.lookup_stats.planned_file_count,
        kept_file_count = event.lookup_stats.kept_file_count,
        scanned_target_row_count = event.lookup_stats.scanned_row_count,
        matched_target_row_count = event.lookup_stats.matched_row_count,
        delete_row_count = event.delete_row_count,
        insert_chunk_row_count = event.insert_chunk_row_count,
        new_total_rows = event.new_total_rows,
        iceberg_snapshot = event.iceberg_snapshot,
        "iceberg aggregate mv incremental refresh complete"
    );
}

fn build_aggregate_target_partition_filter(
    layout: &crate::connector::starrocks::managed::mv_agg_state::AggregateMvLayout,
    schema_contract: &crate::meta::repository::mv_contract::MvSchemaContract,
    delta_chunks: &[crate::exec::chunk::Chunk],
) -> Result<
    (
        crate::engine::mv::partition::TargetPartitionFilter,
        std::collections::BTreeSet<String>,
    ),
    String,
> {
    let touched_row_ids = aggregate_delta_touched_row_ids(layout, delta_chunks)?;

    let derived = crate::engine::mv::partition::derive_from_aggregate_delta(
        &crate::engine::mv::partition::AggregateDeltaPartitionInput {
            layout,
            schema_contract,
            delta_chunks,
        },
    )
    .map_err(|err| err.to_string())?;

    let filter = match derived {
        crate::engine::mv::partition::AffectedAggregateTargetPartitions::Unpartitioned => {
            crate::engine::mv::partition::TargetPartitionFilter::None
        }
        crate::engine::mv::partition::AffectedAggregateTargetPartitions::Known { partitions } => {
            crate::engine::mv::partition::TargetPartitionFilter::AllowList(partitions)
        }
    };
    Ok((filter, touched_row_ids))
}

fn aggregate_delta_touched_row_ids(
    layout: &crate::connector::starrocks::managed::mv_agg_state::AggregateMvLayout,
    delta_chunks: &[crate::exec::chunk::Chunk],
) -> Result<std::collections::BTreeSet<String>, String> {
    use arrow::array::{Array, StringArray};

    let row_id_column = &layout.row_id_column.column.name;
    let mut row_ids = std::collections::BTreeSet::new();
    for chunk in delta_chunks {
        let schema = chunk.batch.schema();
        let row_id_index = schema.index_of(row_id_column).map_err(|e| {
            format!("iceberg aggregate delta missing row id column `{row_id_column}`: {e}")
        })?;
        let row_id_array = chunk
            .batch
            .column(row_id_index)
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| {
                format!("iceberg aggregate delta row id column `{row_id_column}` must be Utf8")
            })?;
        for row in 0..row_id_array.len() {
            if row_id_array.is_null(row) {
                return Err(format!(
                    "iceberg aggregate delta row id column `{row_id_column}` cannot be NULL"
                ));
            }
            row_ids.insert(row_id_array.value(row).to_string());
        }
    }
    Ok(row_ids)
}

#[allow(clippy::too_many_arguments)]
fn apply_iceberg_aggregate_delta_chunks(
    state: &Arc<StandaloneState>,
    target: &IcebergMvTarget,
    target_entry: &crate::connector::iceberg::catalog::IcebergCatalogEntry,
    iceberg_catalog: &Arc<dyn iceberg::Catalog>,
    target_table: &iceberg::table::Table,
    expected_main_snapshot_id: Option<i64>,
    mv_definition: &StoredMvDefinition,
    schema_contract: &crate::meta::repository::mv_contract::MvSchemaContract,
    layout: &crate::connector::starrocks::managed::mv_agg_state::AggregateMvLayout,
    delta_chunks: &[crate::exec::chunk::Chunk],
    snapshots: BTreeMap<String, i64>,
    table_uuids: BTreeMap<String, String>,
) -> Result<StatementResult, String> {
    if delta_chunks.iter().all(|chunk| chunk.batch.num_rows() == 0) {
        return finalize_iceberg_mv_metadata_only_refresh(
            state,
            target,
            mv_definition,
            snapshots,
            table_uuids,
        );
    }

    let target_fqn = target_fqn_string(target);
    let mv_id = mv_definition.mv_id;

    let (partition_filter, touched_row_ids) =
        build_aggregate_target_partition_filter(layout, schema_contract, delta_chunks)
            .map_err(|e| wrap_aggregate_apply_error(&target_fqn, mv_id, e))?;
    let (old_chunks, lookup_stats) =
        crate::engine::mv::iceberg_aggregate_state::load_touched_aggregate_target_state(
            target_table,
            layout,
            schema_contract,
            &touched_row_ids,
            &partition_filter,
        )
        .map_err(|e| wrap_aggregate_apply_error(&target_fqn, mv_id, e))?;
    let old_touched_rows = old_chunks
        .iter()
        .map(|c| c.batch.num_rows() as i64)
        .sum::<i64>();
    let merge = crate::engine::mv::iceberg_aggregate_state::merge_aggregate_target_state(
        layout,
        &old_chunks,
        delta_chunks,
    )?;
    // merge.new_total_rows is the count of groups after merging the PARTIAL old state
    // (touched groups only) with the delta. Adjust by the previous total so that
    // groups not touched by this refresh are still counted.
    let new_total_rows =
        mv_definition.last_refresh_rows.unwrap_or(0) - old_touched_rows + merge.new_total_rows;
    let delete_row_ids = merge.delete_row_ids.clone();
    let insert_chunks = merge.insert_chunks.clone();
    let delete_row_count = merge.delete_row_ids.len();
    let insert_chunk_row_count: usize = merge
        .insert_chunks
        .iter()
        .map(|chunk| chunk.batch.num_rows())
        .sum();
    if delete_row_ids.is_empty()
        && insert_chunks
            .iter()
            .all(|chunk| chunk.batch.num_rows() == 0)
    {
        return finalize_iceberg_mv_metadata_only_refresh(
            state,
            target,
            mv_definition,
            snapshots,
            table_uuids,
        );
    }

    let staging_branch = format!(
        "__nova_mv_refresh_{}_{}",
        mv_definition.mv_id,
        uuid::Uuid::new_v4().simple()
    );
    let refresh_id = begin_staged_iceberg_mv_refresh_intent(
        state,
        target,
        mv_definition.mv_id,
        expected_main_snapshot_id,
        snapshots.clone(),
        &staging_branch,
    )?;
    let ident = iceberg_mv_table_ident(target)?;
    let marker = load_iceberg_mv_refresh_marker(state, refresh_id, mv_definition.mv_id)?
        .to_summary_properties();
    if let Err(err) = ensure_iceberg_mv_staging_branch(
        iceberg_catalog,
        target,
        &staging_branch,
        expected_main_snapshot_id,
    ) {
        abort_iceberg_mv_refresh(state, refresh_id)?;
        return Err(err);
    }
    let target_table = match reload_iceberg_mv_target_table(target_entry, target) {
        Ok(table) => table,
        Err(err) => {
            return Err(handle_iceberg_mv_definite_pre_publish_error(
                state,
                target,
                target_entry,
                &staging_branch,
                refresh_id,
                err,
            ));
        }
    };

    let delete_groups = if delete_row_ids.is_empty() {
        Vec::new()
    } else {
        let (existing_deletes_by_file, referenced_data_file_partitions) =
            match load_target_apply_locator_inputs(target_entry, &target_table) {
                Ok(inputs) => inputs,
                Err(err) => {
                    return Err(handle_iceberg_mv_commit_error(
                        state,
                        target,
                        target_entry,
                        &staging_branch,
                        refresh_id,
                        err,
                    ));
                }
            };
        let groups = match data_block_on(
            crate::engine::mv::iceberg_target_apply::locate_target_rows_by_string_apply_key(
                &target_table,
                ICEBERG_MV_GROUP_APPLY_KEY_COLUMN,
                &delete_row_ids,
                &existing_deletes_by_file,
                &referenced_data_file_partitions,
                &partition_filter,
            ),
        ) {
            Ok(Ok(groups)) => groups,
            Ok(Err(err)) | Err(err) => {
                return Err(handle_iceberg_mv_commit_error(
                    state,
                    target,
                    target_entry,
                    &staging_branch,
                    refresh_id,
                    err,
                ));
            }
        };
        if groups.is_empty() {
            return Err(handle_iceberg_mv_commit_error(
                state,
                target,
                target_entry,
                &staging_branch,
                refresh_id,
                "iceberg aggregate MV target locator did not find rows for requested group row ids"
                    .to_string(),
            ));
        }
        groups
    };

    let new_snapshot_id = match data_block_on(async {
        let data_files = write_chunks_as_iceberg_data_files(&target_table, &insert_chunks).await?;
        commit_iceberg_mv_apply_with_ref(
            &target_table,
            iceberg_catalog,
            target_entry,
            &ident,
            data_files,
            delete_groups,
            &staging_branch,
            marker,
        )
        .await
        .map(|outcome| outcome.new_snapshot_id)
    }) {
        Ok(Ok(snapshot_id)) => snapshot_id,
        Ok(Err(err)) | Err(err) => {
            return Err(handle_iceberg_mv_commit_error(
                state,
                target,
                target_entry,
                &staging_branch,
                refresh_id,
                err,
            ));
        }
    };

    record_iceberg_mv_staging_commit(
        state,
        refresh_id,
        new_snapshot_id,
        new_total_rows,
        table_uuids.clone(),
    )?;
    let published_snapshot_id = publish_iceberg_mv_refresh(
        state,
        target,
        target_entry,
        &staging_branch,
        expected_main_snapshot_id,
        new_snapshot_id,
        refresh_id,
        mv_definition.mv_id,
    )?;
    record_iceberg_mv_publish_commit(state, refresh_id, published_snapshot_id)?;
    drop_iceberg_mv_staging_branch(state, target, target_entry, &staging_branch)?;
    finalize_iceberg_mv_refresh(
        state,
        refresh_id,
        new_total_rows,
        snapshots,
        table_uuids,
        published_snapshot_id,
    )?;
    emit_aggregate_apply_event(&AggregateApplyEvent {
        target_fqn: &target_fqn,
        mv_id,
        partition_filter: &partition_filter,
        touched_group_count: touched_row_ids.len(),
        lookup_stats: &lookup_stats,
        delete_row_count,
        insert_chunk_row_count,
        new_total_rows,
        iceberg_snapshot: published_snapshot_id,
    });
    Ok(StatementResult::Ok)
}

#[allow(clippy::too_many_arguments)]
fn refresh_join_aggregate_iceberg_mv(
    state: &Arc<StandaloneState>,
    target: &IcebergMvTarget,
    target_entry: &crate::connector::iceberg::catalog::IcebergCatalogEntry,
    iceberg_catalog: &Arc<dyn iceberg::Catalog>,
    target_table: &iceberg::table::Table,
    expected_main_snapshot_id: Option<i64>,
    current_catalog: Option<&str>,
    current_database: &str,
    mv_definition: &StoredMvDefinition,
    base_refs: &[IcebergTableRef],
    schema_contract: &crate::meta::repository::mv_contract::MvSchemaContract,
    join_aggregate_shape: &JoinAggregateMvShape,
    aggregate_shape: &AggregateMvShape,
) -> Result<StatementResult, String> {
    if base_refs.len() != 2 {
        return Err(
            "iceberg join aggregate MV refresh requires exactly two base table references"
                .to_string(),
        );
    }
    let join_shape = &join_aggregate_shape.join;
    validate_join_shape_base_refs(join_shape, base_refs)?;
    let (left_ref, right_ref) = join_base_refs_for_shape(join_shape, base_refs)?;
    let left_loaded_before_pin = load_current_iceberg_base_table(state, left_ref)?;
    let right_loaded_before_pin = load_current_iceberg_base_table(state, right_ref)?;
    let left_current_before_pin =
        expected_main_snapshot_id_from_table(&left_loaded_before_pin.table);
    let right_current_before_pin =
        expected_main_snapshot_id_from_table(&right_loaded_before_pin.table);
    let left_previous = mv_definition
        .last_refresh_snapshots
        .get(&left_ref.fqn())
        .copied();
    let right_previous = mv_definition
        .last_refresh_snapshots
        .get(&right_ref.fqn())
        .copied();

    match (
        left_previous,
        right_previous,
        left_current_before_pin,
        right_current_before_pin,
    ) {
        (None, None, None, None) => {
            tracing::info!(
                "iceberg join aggregate mv {}.{}.{}: both base tables have no snapshot; skipping refresh",
                target.catalog,
                target.namespace,
                target.table
            );
            return Ok(StatementResult::Ok);
        }
        (None, None, Some(_), None) | (None, None, None, Some(_)) => {
            tracing::info!(
                "iceberg join aggregate mv {}.{}.{}: one base table has no snapshot; skipping initial refresh",
                target.catalog,
                target.namespace,
                target.table
            );
            return Ok(StatementResult::Ok);
        }
        (Some(prev), _, None, _) => {
            return Err(format!(
                "cannot refresh iceberg join aggregate MV {}.{}.{}: previously-refreshed left base snapshot {prev} is no longer reachable",
                target.catalog, target.namespace, target.table
            ));
        }
        (_, Some(prev), _, None) => {
            return Err(format!(
                "cannot refresh iceberg join aggregate MV {}.{}.{}: previously-refreshed right base snapshot {prev} is no longer reachable",
                target.catalog, target.namespace, target.table
            ));
        }
        _ => {}
    }

    let pin = crate::connector::starrocks::managed::refresh_pin::RefreshSnapshotPin::capture(
        state, base_refs,
    )?;
    if pin.len() != 2 {
        return Err(format!(
            "iceberg join aggregate MV refresh expected two refresh pins, got {}",
            pin.len()
        ));
    }
    validate_refresh_pin_table_uuids(mv_definition, &pin, base_refs)?;

    let left_loaded = load_current_iceberg_base_table(state, left_ref)?;
    let right_loaded = load_current_iceberg_base_table(state, right_ref)?;
    let decision = validate_join_schema_contract(
        schema_contract,
        &[
            (left_ref, &left_loaded.table),
            (right_ref, &right_loaded.table),
        ],
        target_table,
    )?;
    let rebind_happened = matches!(
        decision,
        JoinSchemaContractDecision::CompatibleSafeWithRebind { .. }
    );
    let effective_definition = decision.into_definition(mv_definition)?;
    let mv_definition = &effective_definition;
    // Reclassify the join aggregate shape against the rewritten SELECT so
    // downstream signed-delta / branch rewrites use the current base column
    // names (join key and group key).
    let reclassified_join_aggregate_shape = if rebind_happened {
        let new_query = parse_mv_select_query(&mv_definition.select_sql)?;
        let canonical_new =
            canonicalize_iceberg_mv_select_query(&new_query, current_catalog, current_database);
        let new_shape =
            crate::connector::starrocks::managed::mv_shape::classify_incremental_mv_query(
                &canonical_new,
            )?;
        match new_shape {
            IncrementalMvShape::JoinAggregate(shape) => shape,
            _ => {
                return Err(
                    "iceberg join aggregate MV rebind broke join aggregate classification"
                        .to_string(),
                );
            }
        }
    } else {
        join_aggregate_shape.clone()
    };
    let join_aggregate_shape = &reclassified_join_aggregate_shape;
    let reclassified_aggregate_shape = if rebind_happened {
        join_aggregate_shape.as_aggregate_shape_for_layout()
    } else {
        aggregate_shape.clone()
    };
    let aggregate_shape = &reclassified_aggregate_shape;

    let left_current = pin
        .get(left_ref)
        .ok_or_else(|| format!("missing refresh pin for {}", left_ref.fqn()))?;
    let right_current = pin
        .get(right_ref)
        .ok_or_else(|| format!("missing refresh pin for {}", right_ref.fqn()))?;

    match (left_previous, right_previous) {
        (None, None) => {
            let staging_branch = format!(
                "__nova_mv_refresh_{}_{}",
                mv_definition.mv_id,
                uuid::Uuid::new_v4().simple()
            );
            let refresh_id = begin_staged_iceberg_mv_refresh_intent(
                state,
                target,
                mv_definition.mv_id,
                expected_main_snapshot_id,
                pin.to_snapshot_map(),
                &staging_branch,
            )?;
            first_refresh_iceberg_aggregate_mv(
                state,
                target,
                target_entry,
                iceberg_catalog,
                expected_main_snapshot_id,
                &staging_branch,
                refresh_id,
                current_catalog,
                current_database,
                mv_definition,
                aggregate_shape,
                &pin,
            )
        }
        (Some(left_prev), Some(right_prev))
            if left_prev == left_current && right_prev == right_current =>
        {
            tracing::info!(
                "iceberg join aggregate mv {}.{}.{}: base snapshots unchanged; updating metadata only",
                target.catalog,
                target.namespace,
                target.table
            );
            finalize_iceberg_mv_metadata_only_refresh(
                state,
                target,
                mv_definition,
                pin.to_snapshot_map(),
                pin.to_table_uuid_map(),
            )
        }
        (Some(left_prev), Some(right_prev)) => incremental_refresh_iceberg_join_aggregate_mv(
            state,
            target,
            target_entry,
            iceberg_catalog,
            target_table,
            expected_main_snapshot_id,
            current_catalog,
            current_database,
            mv_definition,
            schema_contract,
            left_ref,
            right_ref,
            left_prev,
            right_prev,
            left_current,
            right_current,
            &left_loaded,
            &right_loaded,
            join_aggregate_shape,
            aggregate_shape,
            &pin,
        ),
        _ => Err(format!(
            "iceberg join aggregate MV {}.{}.{} has partial previous refresh snapshots; recreate the MV",
            target.catalog, target.namespace, target.table
        )),
    }
}

#[allow(clippy::too_many_arguments)]
fn incremental_refresh_iceberg_join_aggregate_mv(
    state: &Arc<StandaloneState>,
    target: &IcebergMvTarget,
    target_entry: &crate::connector::iceberg::catalog::IcebergCatalogEntry,
    iceberg_catalog: &Arc<dyn iceberg::Catalog>,
    target_table: &iceberg::table::Table,
    expected_main_snapshot_id: Option<i64>,
    current_catalog: Option<&str>,
    current_database: &str,
    mv_definition: &StoredMvDefinition,
    _schema_contract: &crate::meta::repository::mv_contract::MvSchemaContract,
    left_ref: &IcebergTableRef,
    right_ref: &IcebergTableRef,
    left_previous_snapshot_id: i64,
    right_previous_snapshot_id: i64,
    left_current_snapshot_id: i64,
    right_current_snapshot_id: i64,
    left_loaded: &crate::connector::iceberg::catalog::IcebergLoadedTable,
    right_loaded: &crate::connector::iceberg::catalog::IcebergLoadedTable,
    join_aggregate_shape: &JoinAggregateMvShape,
    aggregate_shape: &AggregateMvShape,
    pin: &crate::connector::starrocks::managed::refresh_pin::RefreshSnapshotPin,
) -> Result<StatementResult, String> {
    let left_batch = plan_join_aggregate_side_changes(
        target,
        "left",
        left_loaded,
        left_previous_snapshot_id,
        left_current_snapshot_id,
    )?;
    let right_batch = plan_join_aggregate_side_changes(
        target,
        "right",
        right_loaded,
        right_previous_snapshot_id,
        right_current_snapshot_id,
    )?;
    let left_has_changes =
        !left_batch.inserts.is_empty() || iceberg_change_batch_has_row_deletes(&left_batch);
    let right_has_changes =
        !right_batch.inserts.is_empty() || iceberg_change_batch_has_row_deletes(&right_batch);
    let branches = crate::engine::mv::iceberg_join_branch::plan_join_delta_branches(
        left_ref,
        right_ref,
        crate::engine::mv::iceberg_join_branch::SnapshotWindow {
            from: left_previous_snapshot_id,
            to: left_current_snapshot_id,
        },
        crate::engine::mv::iceberg_join_branch::SnapshotWindow {
            from: right_previous_snapshot_id,
            to: right_current_snapshot_id,
        },
        left_has_changes,
        right_has_changes,
    );
    if branches.is_empty() {
        tracing::info!(
            "iceberg join aggregate mv {}.{}.{}: incremental delta is empty; updating metadata only",
            target.catalog,
            target.namespace,
            target.table
        );
        return finalize_iceberg_mv_metadata_only_refresh(
            state,
            target,
            mv_definition,
            pin.to_snapshot_map(),
            pin.to_table_uuid_map(),
        );
    }

    let layout = build_aggregate_layout_for_refresh(
        state,
        current_catalog,
        current_database,
        mv_definition,
        aggregate_shape,
    )?;
    let base_query = parse_mv_select_query(&mv_definition.select_sql)?;
    let mut delta_chunks = Vec::new();
    for branch in branches {
        let branch_chunks = execute_join_aggregate_delta_branch(
            state,
            current_database,
            &base_query,
            &branch,
            join_aggregate_shape,
            aggregate_shape,
            &layout,
        )?;
        delta_chunks.extend(branch_chunks);
    }
    if delta_chunks.iter().all(|chunk| chunk.batch.num_rows() == 0) {
        return finalize_iceberg_mv_metadata_only_refresh(
            state,
            target,
            mv_definition,
            pin.to_snapshot_map(),
            pin.to_table_uuid_map(),
        );
    }

    apply_iceberg_aggregate_delta_chunks(
        state,
        target,
        target_entry,
        iceberg_catalog,
        target_table,
        expected_main_snapshot_id,
        mv_definition,
        _schema_contract,
        &layout,
        &delta_chunks,
        pin.to_snapshot_map(),
        pin.to_table_uuid_map(),
    )
}

fn plan_join_aggregate_side_changes(
    target: &IcebergMvTarget,
    side: &str,
    loaded_base: &crate::connector::iceberg::catalog::IcebergLoadedTable,
    previous_snapshot_id: i64,
    current_snapshot_id: i64,
) -> Result<crate::connector::iceberg::changes::IcebergChangeBatch, String> {
    let batch = match plan_changes(
        &loaded_base.table,
        previous_snapshot_id,
        Some(current_snapshot_id),
        &[],
    ) {
        Ok(batch) => batch,
        Err(err) => match policy_signal_from_change_error(&err) {
            IcebergChangePolicySignal::FullRefresh { reason } => {
                return Err(format!(
                    "iceberg join aggregate MV {}.{}.{} cannot refresh {side} side incrementally and automatic full rebuild is disabled: {reason}",
                    target.catalog, target.namespace, target.table
                ));
            }
            IcebergChangePolicySignal::Unsupported { reason } => {
                return Err(format!(
                    "iceberg join aggregate MV {}.{}.{} {side} side incremental refresh unsupported: {reason}",
                    target.catalog, target.namespace, target.table
                ));
            }
            IcebergChangePolicySignal::Incremental => {
                return Err(format!(
                    "iceberg join aggregate MV {}.{}.{} produced invalid {side} side incremental policy from change planner: {err}",
                    target.catalog, target.namespace, target.table
                ));
            }
        },
    };
    if batch.current_snapshot_id != current_snapshot_id {
        return Err(format!(
            "iceberg join aggregate MV {side} side change batch snapshot mismatch: expected {current_snapshot_id}, got {}",
            batch.current_snapshot_id
        ));
    }
    Ok(batch)
}

#[allow(clippy::too_many_arguments)]
fn execute_join_aggregate_delta_branch(
    state: &Arc<StandaloneState>,
    current_database: &str,
    base_query: &sqlparser::ast::Query,
    branch: &crate::engine::mv::iceberg_join_branch::JoinDeltaBranchPlan,
    join_aggregate_shape: &JoinAggregateMvShape,
    aggregate_shape: &AggregateMvShape,
    layout: &crate::connector::starrocks::managed::mv_agg_state::AggregateMvLayout,
) -> Result<Vec<crate::exec::chunk::Chunk>, String> {
    let mut branch_query = crate::engine::mv::iceberg_join_branch::rewrite_join_branch_query(
        base_query,
        branch,
        &join_aggregate_shape.join.left_alias,
        &join_aggregate_shape.join.right_alias,
    )?;
    normalize_join_branch_snapshot_tables(&mut branch_query, branch)?;
    let signed_sql = iceberg_join_aggregate_branch_delta_sql(
        &branch_query.to_string(),
        join_aggregate_shape,
        branch.delta_side()?,
    )?;
    let signed_query = parse_mv_select_query(&signed_sql)?;
    let branch_catalog = build_join_branch_catalog(state, branch)?;
    let catalogs_guard = state
        .iceberg_catalogs
        .read()
        .map_err(|e| format!("iceberg catalog registry read lock: {e}"))?;
    let result = crate::engine::execute_query_with_options(
        &signed_query,
        &branch_catalog,
        current_database,
        state.exchange_port,
        None,
        None,
        Some(&*catalogs_guard),
    );
    drop(catalogs_guard);
    let result = result?;
    crate::connector::starrocks::managed::mv_agg_state::materialize_aggregate_result_chunks(
        result,
        layout,
        aggregate_shape,
    )
}

// Previous implementation of REFRESH FULL — `refresh_full_iceberg_mv` —
// was removed. It dropped the target table + deleted the MV definition +
// re-ran create_iceberg_mv (which leaves the new target empty), and the
// drop and the create were in separate transactions. The user-visible
// outcome was misleading ("MV is now empty" rather than "MV is fully
// repopulated") and the operation could leave behind an inconsistent
// state on partial failure. It also silently dropped partition_by.
//
// Re-introduce only after a redesign that clarifies:
//   - the keyword name (probably REBUILD rather than REFRESH FULL),
//   - atomic drop+create+populate semantics,
//   - a deterministic data-repopulation step,
//   - faithful preservation of the original DDL (partition_by,
//     distribution, properties).
// See the rejection in refresh_iceberg_mv for the user-facing error.

fn unknown_join_affected_partitions() -> crate::engine::mv::partition::AffectedMvPartitions {
    crate::engine::mv::partition::AffectedMvPartitions::unknown(
        "join MV affected partition planning is not implemented",
    )
}

fn noop_affected_partitions(
    schema_contract: &crate::meta::repository::mv_contract::MvSchemaContract,
) -> crate::engine::mv::partition::AffectedMvPartitions {
    if is_unpartitioned_mv_contract(schema_contract) {
        crate::engine::mv::partition::AffectedMvPartitions::Unpartitioned
    } else {
        crate::engine::mv::partition::AffectedMvPartitions::known(
            std::iter::empty::<crate::engine::mv::partition::MvPartitionKey>(),
            std::iter::empty::<crate::engine::mv::partition::MvPartitionKey>(),
        )
    }
}

fn is_unpartitioned_mv_contract(
    schema_contract: &crate::meta::repository::mv_contract::MvSchemaContract,
) -> bool {
    schema_contract
        .target
        .partition
        .as_ref()
        .is_none_or(|partition| partition.fields.is_empty())
}

fn log_planned_iceberg_mv_affected_partitions(
    iceberg_target: &IcebergMvTarget,
    affected_partitions: &crate::engine::mv::partition::AffectedMvPartitions,
) {
    tracing::info!(
        target = %format!(
            "{}.{}.{}",
            iceberg_target.catalog, iceberg_target.namespace, iceberg_target.table
        ),
        affected_partitions = ?affected_partitions,
        "planned iceberg MV affected partitions"
    );
}

pub(crate) fn plan_iceberg_mv_refresh(
    state: &Arc<StandaloneState>,
    current_catalog: Option<&str>,
    current_database: &str,
    stmt: &RefreshMaterializedViewStmt,
    target: MvTarget,
) -> Result<RefreshPlan, RefreshError> {
    let iceberg_target = resolve_refresh_target(current_catalog, current_database, &stmt.name)
        .map_err(RefreshError::user)?;
    if stmt.full {
        return Err(RefreshError::user(
            "REFRESH MATERIALIZED VIEW ... FULL is currently disabled pending redesign; \
             its previous behavior (drop target + delete definition + recreate empty target) \
             was misleading and non-atomic. To recover from a broken contract or corrupted \
             target, run DROP MATERIALIZED VIEW <name>; CREATE MATERIALIZED VIEW <name> ...; \
             REFRESH MATERIALIZED VIEW <name>; manually.",
        ));
    }

    recover_iceberg_mv_refreshes(state).map_err(RefreshError::pre_commit)?;
    let mv_definition =
        load_iceberg_mv_definition_by_target(state, &iceberg_target).map_err(RefreshError::user)?;
    let (_, _, target_loaded) =
        load_iceberg_mv_target(state, &iceberg_target).map_err(RefreshError::user)?;
    validate_target_snapshot(&iceberg_target, &mv_definition, &target_loaded.table)
        .map_err(RefreshError::user)?;

    let base_refs =
        parse_iceberg_table_refs(&mv_definition.base_table_refs).map_err(RefreshError::user)?;
    let canonical_select_query = canonicalize_iceberg_mv_select_query(
        &parse_mv_select_query(&mv_definition.select_sql).map_err(RefreshError::user)?,
        current_catalog,
        current_database,
    );
    let shape =
        classify_incremental_mv_query(&canonical_select_query).map_err(RefreshError::user)?;
    if aggregate_shape_for_layout(&shape).is_some() {
        return plan_iceberg_aggregate_mv_refresh(
            state,
            &iceberg_target,
            &target_loaded.table,
            target,
            stmt,
            current_catalog,
            current_database,
            &mv_definition,
            &base_refs,
            &shape,
        );
    }
    if let IncrementalMvShape::JoinProjectionFilter(join_shape) = &shape {
        if base_refs.len() != 2 {
            return Err(RefreshError::user(
                "iceberg join materialized view refresh requires exactly two base table references",
            ));
        }
        validate_join_shape_base_refs(join_shape, &base_refs).map_err(RefreshError::user)?;
        let schema_contract = mv_definition.schema_contract.as_ref().ok_or_else(|| {
            RefreshError::user(format!(
                "iceberg MV target {}.{}.{} is missing A11 schema contract; rebuild or recreate the MV",
                iceberg_target.catalog, iceberg_target.namespace, iceberg_target.table
            ))
        })?;
        if schema_contract.contract_version != 2 {
            return Err(RefreshError::user(format!(
                "iceberg join MV {}.{}.{} requires schema contract version 2, got {}",
                iceberg_target.catalog,
                iceberg_target.namespace,
                iceberg_target.table,
                schema_contract.contract_version
            )));
        }
        let (left_ref, right_ref) =
            join_base_refs_for_shape(join_shape, &base_refs).map_err(RefreshError::user)?;
        let left_loaded =
            load_current_iceberg_base_table(state, left_ref).map_err(RefreshError::user)?;
        let right_loaded =
            load_current_iceberg_base_table(state, right_ref).map_err(RefreshError::user)?;
        let join_bases = [
            (left_ref, &left_loaded.table),
            (right_ref, &right_loaded.table),
        ];
        match validate_join_schema_contract(schema_contract, &join_bases, &target_loaded.table)
            .map_err(RefreshError::user)?
        {
            JoinSchemaContractDecision::CompatibleSafe
            | JoinSchemaContractDecision::CompatibleSafeWithRebind { .. } => {}
        }
        let left_current = left_loaded
            .table
            .metadata()
            .current_snapshot()
            .map(|s| s.snapshot_id());
        let right_current = right_loaded
            .table
            .metadata()
            .current_snapshot()
            .map(|s| s.snapshot_id());
        let mut snapshot_pins = BTreeMap::new();
        snapshot_pins.insert(left_ref.fqn(), left_current);
        snapshot_pins.insert(right_ref.fqn(), right_current);
        let mut current_snapshots = BTreeMap::new();
        current_snapshots.insert(left_ref.fqn(), left_current);
        current_snapshots.insert(right_ref.fqn(), right_current);
        let previous_snapshots = &mv_definition.last_refresh_snapshots;
        let has_previous = base_refs
            .iter()
            .any(|base_ref| previous_snapshots.contains_key(&base_ref.fqn()));
        let all_previous = base_refs
            .iter()
            .all(|base_ref| previous_snapshots.contains_key(&base_ref.fqn()));
        let all_current = current_snapshots.values().all(Option::is_some);
        if has_previous && !all_previous {
            return Err(RefreshError::user(format!(
                "iceberg join MV {}.{}.{} has partial previous refresh snapshots; recreate the MV",
                iceberg_target.catalog, iceberg_target.namespace, iceberg_target.table
            )));
        }
        if has_previous {
            for base_ref in &base_refs {
                let fqn = base_ref.fqn();
                if previous_snapshots.contains_key(&fqn)
                    && current_snapshots.get(&fqn).copied().flatten().is_none()
                {
                    return Err(RefreshError::user(format!(
                        "cannot refresh iceberg join materialized view {}.{}.{}: previously-refreshed base snapshot for {} is no longer reachable",
                        iceberg_target.catalog, iceberg_target.namespace, iceberg_target.table, fqn
                    )));
                }
            }
            for base_ref in &base_refs {
                let fqn = base_ref.fqn();
                let previous = previous_snapshots.get(&fqn).copied().ok_or_else(|| {
                    RefreshError::user(format!(
                        "iceberg join MV {}.{}.{} has partial previous refresh snapshots; recreate the MV",
                        iceberg_target.catalog, iceberg_target.namespace, iceberg_target.table
                    ))
                })?;
                let current = current_snapshots
                    .get(&fqn)
                    .copied()
                    .flatten()
                    .ok_or_else(|| {
                        RefreshError::user(format!(
                            "cannot refresh iceberg join materialized view {}.{}.{}: previously-refreshed base snapshot for {} is no longer reachable",
                            iceberg_target.catalog, iceberg_target.namespace, iceberg_target.table, fqn
                        ))
                    })?;
                let loaded =
                    load_current_iceberg_base_table(state, base_ref).map_err(RefreshError::user)?;
                crate::connector::iceberg::changes::classify_lineage(
                    loaded.table.metadata(),
                    previous,
                    current,
                )
                .map_err(|e| {
                    RefreshError::user(format!(
                        "cannot refresh iceberg join materialized view {}.{}.{}: previous base snapshot {previous} for {} is not reachable from pinned snapshot {current}: {e}",
                        iceberg_target.catalog, iceberg_target.namespace, iceberg_target.table, fqn
                    ))
                })?;
            }
        }
        let mode = if !has_previous && !all_current {
            RefreshMode::Noop
        } else if !has_previous {
            RefreshMode::Full
        } else if base_refs.iter().all(|base_ref| {
            let fqn = base_ref.fqn();
            previous_snapshots.get(&fqn).copied() == current_snapshots.get(&fqn).copied().flatten()
        }) {
            RefreshMode::Noop
        } else {
            RefreshMode::Incremental
        };
        let affected_partitions = unknown_join_affected_partitions();
        log_planned_iceberg_mv_affected_partitions(&iceberg_target, &affected_partitions);
        return Ok(RefreshPlan {
            mv_id: Some(mv_definition.mv_id),
            target,
            storage_engine: MvStorageEngine::Iceberg,
            mode,
            base_refs: base_refs
                .iter()
                .map(|base_ref| MvBaseRef {
                    catalog: base_ref.catalog.clone(),
                    namespace: base_ref.namespace.clone(),
                    table: base_ref.table.clone(),
                })
                .collect(),
            snapshot_pins,
            affected_partitions,
            backend_plan: BackendRefreshPlan::Iceberg(IcebergRefreshPlan {
                stmt: stmt.clone(),
                current_catalog: current_catalog.map(str::to_string),
                current_database: current_database.to_string(),
            }),
        });
    }
    if !matches!(shape, IncrementalMvShape::ProjectionFilter(_)) {
        return Err(RefreshError::user(
            "iceberg materialized view refresh only supports projection/filter or join projection/filter shapes",
        ));
    }
    let [base_ref] = base_refs.as_slice() else {
        return Err(RefreshError::user(
            "iceberg materialized view refresh requires exactly one base table reference",
        ));
    };
    let schema_contract = mv_definition.schema_contract.as_ref().ok_or_else(|| {
        RefreshError::user(format!(
            "iceberg MV target {}.{}.{} is missing A11 schema contract; rebuild or recreate the MV",
            iceberg_target.catalog, iceberg_target.namespace, iceberg_target.table
        ))
    })?;
    let pre_pin_loaded =
        load_current_iceberg_base_table(state, base_ref).map_err(RefreshError::user)?;
    let current_snapshot_id_before_pin = pre_pin_loaded
        .table
        .metadata()
        .current_snapshot()
        .map(|s| s.snapshot_id());
    let previous_snapshot_id = mv_definition
        .last_refresh_snapshots
        .get(&base_ref.fqn())
        .copied();
    let is_empty_base_noop =
        previous_snapshot_id.is_none() && current_snapshot_id_before_pin.is_none();
    if is_empty_base_noop {
        ensure_schema_contract_compatible_for_refresh(
            schema_contract,
            &pre_pin_loaded.table,
            &target_loaded.table,
        )
        .map_err(RefreshError::user)?;
        let mut snapshot_pins = BTreeMap::new();
        snapshot_pins.insert(base_ref.fqn(), None);
        let affected_partitions = noop_affected_partitions(schema_contract);
        log_planned_iceberg_mv_affected_partitions(&iceberg_target, &affected_partitions);
        return Ok(RefreshPlan {
            mv_id: Some(mv_definition.mv_id),
            target,
            storage_engine: MvStorageEngine::Iceberg,
            mode: RefreshMode::Noop,
            base_refs: vec![MvBaseRef {
                catalog: base_ref.catalog.clone(),
                namespace: base_ref.namespace.clone(),
                table: base_ref.table.clone(),
            }],
            snapshot_pins,
            affected_partitions,
            backend_plan: BackendRefreshPlan::Iceberg(IcebergRefreshPlan {
                stmt: stmt.clone(),
                current_catalog: current_catalog.map(str::to_string),
                current_database: current_database.to_string(),
            }),
        });
    }

    let pin = crate::connector::starrocks::managed::refresh_pin::RefreshSnapshotPin::capture(
        state, &base_refs,
    )
    .map_err(RefreshError::user)?;
    let current_snapshot_id = pin.get(base_ref);
    let loaded = load_current_iceberg_base_table(state, base_ref).map_err(RefreshError::user)?;
    match crate::engine::mv::schema_contract::validate_schema_contract(
        schema_contract,
        &loaded.table,
        &target_loaded.table,
    ) {
        crate::engine::mv::schema_contract::ContractDecision::Incompatible(err) => {
            return Err(RefreshError::user(format!("{err}")));
        }
        crate::engine::mv::schema_contract::ContractDecision::CompatibleSafeWithRebind {
            ..
        }
        | crate::engine::mv::schema_contract::ContractDecision::CompatibleSafe => {}
    }

    let mode = match (previous_snapshot_id, current_snapshot_id) {
        (None, None) => RefreshMode::Noop,
        (None, Some(_)) => RefreshMode::Full,
        (Some(prev), Some(cur)) if prev == cur => RefreshMode::Noop,
        (Some(prev), Some(cur)) => {
            crate::connector::iceberg::changes::classify_lineage(
                loaded.table.metadata(),
                prev,
                cur,
            )
            .map_err(|e| {
                RefreshError::user(format!(
                    "cannot refresh iceberg materialized view {}.{}.{}: previous base snapshot {prev} for {} is not reachable from pinned snapshot {cur}: {e}",
                    iceberg_target.catalog,
                    iceberg_target.namespace,
                    iceberg_target.table,
                    base_ref.fqn()
                ))
            })?;
            RefreshMode::Incremental
        }
        (Some(prev), None) => {
            return Err(RefreshError::user(format!(
                "cannot refresh iceberg materialized view {}.{}.{}: previously-refreshed base snapshot {prev} is no longer reachable",
                iceberg_target.catalog, iceberg_target.namespace, iceberg_target.table
            )));
        }
    };
    let mut snapshot_pins = BTreeMap::new();
    snapshot_pins.insert(base_ref.fqn(), current_snapshot_id);
    let affected_partitions = match mode {
        RefreshMode::Noop => noop_affected_partitions(schema_contract),
        RefreshMode::Incremental => {
            if is_unpartitioned_mv_contract(schema_contract) {
                crate::engine::mv::partition::AffectedMvPartitions::Unpartitioned
            } else {
                let previous =
                    previous_snapshot_id.expect("incremental refresh has previous snapshot");
                let current =
                    current_snapshot_id.expect("incremental refresh has current snapshot");
                match plan_changes(&loaded.table, previous, Some(current), &[]) {
                    Ok(batch) => crate::engine::mv::partition::planner::plan_affected_partitions(
                        &crate::engine::mv::partition::planner::AffectedPartitionPlanInput {
                            schema_contract,
                            change_batch: Some(&batch),
                        },
                    ),
                    Err(err) => crate::engine::mv::partition::AffectedMvPartitions::unknown(
                        format!("failed to plan Iceberg changes for affected partitions: {err}"),
                    ),
                }
            }
        }
        RefreshMode::Full | RefreshMode::Rebuild => {
            if is_unpartitioned_mv_contract(schema_contract) {
                crate::engine::mv::partition::AffectedMvPartitions::Unpartitioned
            } else {
                crate::engine::mv::partition::planner::plan_affected_partitions(
                    &crate::engine::mv::partition::planner::AffectedPartitionPlanInput {
                        schema_contract,
                        change_batch: None,
                    },
                )
            }
        }
    };
    log_planned_iceberg_mv_affected_partitions(&iceberg_target, &affected_partitions);
    Ok(RefreshPlan {
        mv_id: Some(mv_definition.mv_id),
        target,
        storage_engine: MvStorageEngine::Iceberg,
        mode,
        base_refs: vec![MvBaseRef {
            catalog: base_ref.catalog.clone(),
            namespace: base_ref.namespace.clone(),
            table: base_ref.table.clone(),
        }],
        snapshot_pins,
        affected_partitions,
        backend_plan: BackendRefreshPlan::Iceberg(IcebergRefreshPlan {
            stmt: stmt.clone(),
            current_catalog: current_catalog.map(str::to_string),
            current_database: current_database.to_string(),
        }),
    })
}

#[allow(clippy::too_many_arguments)]
fn plan_iceberg_aggregate_mv_refresh(
    state: &Arc<StandaloneState>,
    iceberg_target: &IcebergMvTarget,
    target_table: &iceberg::table::Table,
    target: MvTarget,
    stmt: &RefreshMaterializedViewStmt,
    current_catalog: Option<&str>,
    current_database: &str,
    mv_definition: &StoredMvDefinition,
    base_refs: &[IcebergTableRef],
    shape: &IncrementalMvShape,
) -> Result<RefreshPlan, RefreshError> {
    let schema_contract =
        validate_aggregate_schema_contract_metadata(iceberg_target, mv_definition)
            .map_err(RefreshError::user)?;
    match shape {
        IncrementalMvShape::Aggregate(_) => {
            let [base_ref] = base_refs else {
                return Err(RefreshError::user(
                    "iceberg aggregate materialized view refresh requires exactly one base table reference",
                ));
            };
            let loaded =
                load_current_iceberg_base_table(state, base_ref).map_err(RefreshError::user)?;
            match crate::engine::mv::schema_contract::validate_schema_contract(
                schema_contract,
                &loaded.table,
                target_table,
            ) {
                crate::engine::mv::schema_contract::ContractDecision::Incompatible(err) => {
                    return Err(RefreshError::user(format!("{err}")));
                }
                crate::engine::mv::schema_contract::ContractDecision::CompatibleSafe
                | crate::engine::mv::schema_contract::ContractDecision::CompatibleSafeWithRebind {
                    ..
                } => {}
            }
            let current = expected_main_snapshot_id_from_table(&loaded.table);
            let previous = mv_definition
                .last_refresh_snapshots
                .get(&base_ref.fqn())
                .copied();
            let mode = match (previous, current) {
                (None, None) => RefreshMode::Noop,
                (None, Some(_)) => RefreshMode::Full,
                (Some(prev), Some(cur)) if prev == cur => RefreshMode::Noop,
                (Some(prev), Some(cur)) => {
                    crate::connector::iceberg::changes::classify_lineage(
                        loaded.table.metadata(),
                        prev,
                        cur,
                    )
                    .map_err(|e| {
                        RefreshError::user(format!(
                            "cannot refresh iceberg aggregate materialized view {}.{}.{}: previous base snapshot {prev} for {} is not reachable from pinned snapshot {cur}: {e}",
                            iceberg_target.catalog,
                            iceberg_target.namespace,
                            iceberg_target.table,
                            base_ref.fqn()
                        ))
                    })?;
                    RefreshMode::Incremental
                }
                (Some(prev), None) => {
                    return Err(RefreshError::user(format!(
                        "cannot refresh iceberg aggregate materialized view {}.{}.{}: previously-refreshed base snapshot {prev} is no longer reachable",
                        iceberg_target.catalog, iceberg_target.namespace, iceberg_target.table
                    )));
                }
            };
            let mut snapshot_pins = BTreeMap::new();
            snapshot_pins.insert(base_ref.fqn(), current);
            Ok(build_iceberg_refresh_plan(
                mv_definition,
                target,
                stmt,
                current_catalog,
                current_database,
                base_refs,
                snapshot_pins,
                mode,
            ))
        }
        IncrementalMvShape::JoinAggregate(join_aggregate_shape) => {
            if base_refs.len() != 2 {
                return Err(RefreshError::user(
                    "iceberg join aggregate MV refresh requires exactly two base table references",
                ));
            }
            validate_join_shape_base_refs(&join_aggregate_shape.join, base_refs)
                .map_err(RefreshError::user)?;
            let (left_ref, right_ref) =
                join_base_refs_for_shape(&join_aggregate_shape.join, base_refs)
                    .map_err(RefreshError::user)?;
            let left_loaded =
                load_current_iceberg_base_table(state, left_ref).map_err(RefreshError::user)?;
            let right_loaded =
                load_current_iceberg_base_table(state, right_ref).map_err(RefreshError::user)?;
            match validate_join_schema_contract(
                schema_contract,
                &[
                    (left_ref, &left_loaded.table),
                    (right_ref, &right_loaded.table),
                ],
                target_table,
            )
            .map_err(RefreshError::user)?
            {
                JoinSchemaContractDecision::CompatibleSafe
                | JoinSchemaContractDecision::CompatibleSafeWithRebind { .. } => {}
            }

            let mut snapshot_pins = BTreeMap::new();
            let mut current_snapshots = BTreeMap::new();
            current_snapshots.insert(
                left_ref.fqn(),
                expected_main_snapshot_id_from_table(&left_loaded.table),
            );
            current_snapshots.insert(
                right_ref.fqn(),
                expected_main_snapshot_id_from_table(&right_loaded.table),
            );
            for base_ref in base_refs {
                snapshot_pins.insert(
                    base_ref.fqn(),
                    current_snapshots.get(&base_ref.fqn()).copied().flatten(),
                );
            }
            let previous_snapshots = &mv_definition.last_refresh_snapshots;
            let has_previous = base_refs
                .iter()
                .any(|base_ref| previous_snapshots.contains_key(&base_ref.fqn()));
            let all_previous = base_refs
                .iter()
                .all(|base_ref| previous_snapshots.contains_key(&base_ref.fqn()));
            let all_current = current_snapshots.values().all(Option::is_some);
            if has_previous && !all_previous {
                return Err(RefreshError::user(format!(
                    "iceberg join aggregate MV {}.{}.{} has partial previous refresh snapshots; recreate the MV",
                    iceberg_target.catalog, iceberg_target.namespace, iceberg_target.table
                )));
            }
            if has_previous {
                for base_ref in base_refs {
                    let fqn = base_ref.fqn();
                    let previous = previous_snapshots.get(&fqn).copied().ok_or_else(|| {
                        RefreshError::user(format!(
                            "iceberg join aggregate MV {}.{}.{} has partial previous refresh snapshots; recreate the MV",
                            iceberg_target.catalog, iceberg_target.namespace, iceberg_target.table
                        ))
                    })?;
                    let current = current_snapshots.get(&fqn).copied().flatten().ok_or_else(
                        || {
                            RefreshError::user(format!(
                                "cannot refresh iceberg join aggregate MV {}.{}.{}: previously-refreshed base snapshot for {} is no longer reachable",
                                iceberg_target.catalog,
                                iceberg_target.namespace,
                                iceberg_target.table,
                                fqn
                            ))
                        },
                    )?;
                    let loaded = if fqn.eq_ignore_ascii_case(&left_ref.fqn()) {
                        &left_loaded
                    } else {
                        &right_loaded
                    };
                    crate::connector::iceberg::changes::classify_lineage(
                        loaded.table.metadata(),
                        previous,
                        current,
                    )
                    .map_err(|e| {
                        RefreshError::user(format!(
                            "cannot refresh iceberg join aggregate MV {}.{}.{}: previous base snapshot {previous} for {} is not reachable from pinned snapshot {current}: {e}",
                            iceberg_target.catalog,
                            iceberg_target.namespace,
                            iceberg_target.table,
                            fqn
                        ))
                    })?;
                }
            }
            let mode = if !has_previous && !all_current {
                RefreshMode::Noop
            } else if !has_previous {
                RefreshMode::Full
            } else if base_refs.iter().all(|base_ref| {
                let fqn = base_ref.fqn();
                previous_snapshots.get(&fqn).copied()
                    == current_snapshots.get(&fqn).copied().flatten()
            }) {
                RefreshMode::Noop
            } else {
                RefreshMode::Incremental
            };
            Ok(build_iceberg_refresh_plan(
                mv_definition,
                target,
                stmt,
                current_catalog,
                current_database,
                base_refs,
                snapshot_pins,
                mode,
            ))
        }
        _ => Err(RefreshError::user(
            "iceberg aggregate MV refresh plan requires aggregate or join aggregate shape",
        )),
    }
}

fn build_iceberg_refresh_plan(
    mv_definition: &StoredMvDefinition,
    target: MvTarget,
    stmt: &RefreshMaterializedViewStmt,
    current_catalog: Option<&str>,
    current_database: &str,
    base_refs: &[IcebergTableRef],
    snapshot_pins: BTreeMap<String, Option<i64>>,
    mode: RefreshMode,
) -> RefreshPlan {
    RefreshPlan {
        mv_id: Some(mv_definition.mv_id),
        target,
        storage_engine: MvStorageEngine::Iceberg,
        mode,
        base_refs: base_refs
            .iter()
            .map(|base_ref| MvBaseRef {
                catalog: base_ref.catalog.clone(),
                namespace: base_ref.namespace.clone(),
                table: base_ref.table.clone(),
            })
            .collect(),
        snapshot_pins,
        affected_partitions: crate::engine::mv::partition::AffectedMvPartitions::unknown(
            "aggregate MV affected partition planning is not implemented",
        ),
        backend_plan: BackendRefreshPlan::Iceberg(IcebergRefreshPlan {
            stmt: stmt.clone(),
            current_catalog: current_catalog.map(str::to_string),
            current_database: current_database.to_string(),
        }),
    }
}

pub(crate) fn execute_iceberg_mv_refresh(
    state: &Arc<StandaloneState>,
    plan: &IcebergRefreshPlan,
) -> Result<IcebergRefreshOutcome, RefreshError> {
    refresh_iceberg_mv(
        state,
        plan.current_catalog.as_deref(),
        &plan.current_database,
        &plan.stmt,
    )
    .map_err(|err| {
        if is_iceberg_commit_unknown_error(&err) {
            RefreshError::commit_unknown(err)
        } else {
            RefreshError::pre_commit(err)
        }
    })?;
    Ok(IcebergRefreshOutcome {
        completed_inside_execute: true,
    })
}

fn load_iceberg_mv_definition_by_target(
    state: &Arc<StandaloneState>,
    target: &IcebergMvTarget,
) -> Result<StoredMvDefinition, String> {
    let provider = state
        .metadata_provider
        .as_ref()
        .ok_or_else(|| "metadata provider required for iceberg mv refresh".to_string())?;
    let read = provider
        .begin_read()
        .map_err(|e| format!("open iceberg mv definition read transaction failed: {e}"))?;
    state
        .mv_repo
        .find_by_target(
            read.as_ref(),
            &target.catalog,
            &target.namespace,
            &target.table,
        )
        .map_err(|e| format!("load iceberg mv definition failed: {e}"))?
        .ok_or_else(|| {
            format!(
                "iceberg materialized view {}.{}.{} has no MV definition",
                target.catalog, target.namespace, target.table
            )
        })
}

fn begin_iceberg_mv_refresh_intent(
    state: &Arc<StandaloneState>,
    mv_id: i64,
    target_snapshots: std::collections::BTreeMap<String, i64>,
) -> Result<i64, String> {
    let provider = state
        .metadata_provider
        .as_ref()
        .ok_or_else(|| "metadata provider required for iceberg mv refresh".to_string())?;
    let mut txn = provider
        .begin_write("begin iceberg materialized view refresh")
        .map_err(|e| format!("open iceberg mv refresh intent transaction failed: {e}"))?;
    let refresh = state
        .mv_repo
        .begin_refresh_intent(txn.as_mut(), mv_id, target_snapshots)
        .map_err(|e| format!("begin iceberg mv refresh intent failed: {e}"))?;
    txn.commit()
        .map_err(|e| format!("commit iceberg mv refresh intent failed: {e}"))?;
    Ok(refresh.refresh_id)
}

fn begin_staged_iceberg_mv_refresh_intent(
    state: &Arc<StandaloneState>,
    target: &IcebergMvTarget,
    mv_id: i64,
    expected_main_snapshot_id: Option<i64>,
    base_snapshots: BTreeMap<String, i64>,
    staging_branch: &str,
) -> Result<i64, String> {
    let provider = state
        .metadata_provider
        .as_ref()
        .ok_or_else(|| "metadata provider required for iceberg mv refresh".to_string())?;
    let mut txn = provider
        .begin_write("begin staged iceberg materialized view refresh")
        .map_err(|e| format!("open staged iceberg mv refresh intent transaction failed: {e}"))?;
    let refresh = state
        .mv_repo
        .begin_iceberg_refresh_intent(
            txn.as_mut(),
            BeginIcebergMvRefreshRequest {
                mv_id,
                target_catalog: target.catalog.clone(),
                target_namespace: target.namespace.clone(),
                target_table: target.table.clone(),
                staging_branch: staging_branch.to_string(),
                expected_main_snapshot_id,
                base_snapshots,
                marker_token: uuid::Uuid::new_v4().simple().to_string(),
            },
        )
        .map_err(|e| format!("begin staged iceberg mv refresh intent failed: {e}"))?;
    txn.commit()
        .map_err(|e| format!("commit staged iceberg mv refresh intent failed: {e}"))?;
    Ok(refresh.refresh_id)
}

fn abort_iceberg_mv_refresh(state: &Arc<StandaloneState>, refresh_id: i64) -> Result<(), String> {
    let provider = state
        .metadata_provider
        .as_ref()
        .ok_or_else(|| "metadata provider required for iceberg mv refresh".to_string())?;
    let mut txn = provider
        .begin_write("abort iceberg materialized view refresh")
        .map_err(|e| format!("open iceberg mv refresh abort transaction failed: {e}"))?;
    let refresh = state
        .mv_repo
        .load_refresh(txn.as_ref(), refresh_id)
        .map_err(|e| format!("load iceberg mv refresh for abort failed: {e}"))?
        .ok_or_else(|| format!("mv refresh {refresh_id} not found"))?;
    state
        .mv_repo
        .clear_refresh_progress(txn.as_mut(), refresh.mv_id)
        .map_err(|e| format!("abort iceberg mv refresh failed: {e}"))?;
    txn.commit()
        .map_err(|e| format!("commit iceberg mv refresh abort failed: {e}"))?;
    Ok(())
}

fn is_iceberg_commit_unknown_error(err: &str) -> bool {
    err.contains("iceberg commit unknown (")
}

fn mark_iceberg_mv_refresh_commit_unknown(
    state: &Arc<StandaloneState>,
    refresh_id: i64,
) -> Result<(), String> {
    let provider = state
        .metadata_provider
        .as_ref()
        .ok_or_else(|| "metadata provider required for iceberg mv refresh".to_string())?;
    let mut txn = provider
        .begin_write("mark iceberg materialized view refresh commit unknown")
        .map_err(|e| format!("open iceberg mv commit-unknown transaction failed: {e}"))?;
    state
        .mv_repo
        .mark_refresh_commit_unknown(txn.as_mut(), refresh_id)
        .map_err(|e| format!("mark iceberg mv refresh commit unknown failed: {e}"))?;
    txn.commit()
        .map_err(|e| format!("commit iceberg mv commit-unknown marker failed: {e}"))?;
    Ok(())
}

fn mark_iceberg_mv_refresh_aborted(
    state: &Arc<StandaloneState>,
    refresh_id: i64,
) -> Result<(), String> {
    abort_iceberg_mv_refresh(state, refresh_id)
}

fn reconcile_iceberg_mv_refresh(
    state: &Arc<StandaloneState>,
    refresh: StoredMvRefresh,
    target: &IcebergMvTarget,
    entry: &crate::connector::iceberg::catalog::IcebergCatalogEntry,
    _catalog: &Arc<dyn iceberg::Catalog>,
    table: &iceberg::table::Table,
) -> Result<(), String> {
    let main = table.metadata().current_snapshot().map(|s| s.snapshot_id());
    let staging_branch = refresh
        .staging_branch
        .as_deref()
        .ok_or_else(|| format!("mv refresh {} missing staging branch", refresh.refresh_id))?;
    let staging = table
        .metadata()
        .refs()
        .get(staging_branch)
        .map(|r| r.snapshot_id);

    match refresh.state {
        MvRefreshState::IntentCreated => {
            if main == refresh.expected_main_snapshot_id {
                match staging {
                    None => {
                        mark_iceberg_mv_refresh_aborted(state, refresh.refresh_id)?;
                        Ok(())
                    }
                    Some(staging_snapshot_id)
                        if snapshot_id_matches_refresh_marker(
                            table,
                            staging_snapshot_id,
                            &refresh,
                        )? =>
                    {
                        drop_iceberg_mv_staging_branch(state, target, entry, staging_branch)?;
                        mark_iceberg_mv_refresh_aborted(state, refresh.refresh_id)?;
                        Ok(())
                    }
                    _ => mark_iceberg_mv_refresh_commit_unknown(state, refresh.refresh_id),
                }
            } else {
                mark_iceberg_mv_refresh_commit_unknown(state, refresh.refresh_id)
            }
        }
        MvRefreshState::StagingCommitted => {
            if main == refresh.expected_main_snapshot_id
                && staging.is_none()
                && refresh.staging_snapshot_id.is_some()
            {
                mark_iceberg_mv_refresh_aborted(state, refresh.refresh_id)?;
                return Ok(());
            }
            if main == refresh.expected_main_snapshot_id
                && staging == refresh.staging_snapshot_id
                && refresh
                    .staging_snapshot_id
                    .map(|snapshot_id| {
                        snapshot_id_matches_refresh_marker(table, snapshot_id, &refresh)
                    })
                    .transpose()?
                    == Some(true)
            {
                drop_iceberg_mv_staging_branch(state, target, entry, staging_branch)?;
                mark_iceberg_mv_refresh_aborted(state, refresh.refresh_id)?;
                return Ok(());
            }
            if main == refresh.staging_snapshot_id
                && refresh
                    .staging_snapshot_id
                    .map(|snapshot_id| {
                        snapshot_id_matches_refresh_marker(table, snapshot_id, &refresh)
                    })
                    .transpose()?
                    == Some(true)
            {
                record_iceberg_mv_publish_commit(
                    state,
                    refresh.refresh_id,
                    refresh.staging_snapshot_id.ok_or_else(|| {
                        format!("mv refresh {} missing staging snapshot", refresh.refresh_id)
                    })?,
                )?;
                if staging.is_some() {
                    drop_iceberg_mv_staging_branch(state, target, entry, staging_branch)?;
                }
                finalize_recovered_iceberg_mv_refresh(state, &refresh)?;
                return Ok(());
            }
            mark_iceberg_mv_refresh_commit_unknown(state, refresh.refresh_id)?;
            Ok(())
        }
        MvRefreshState::PublishCommitted => {
            let published_snapshot_id =
                recovered_published_snapshot_id(&refresh).ok_or_else(|| {
                    format!(
                        "mv refresh {} missing published snapshot",
                        refresh.refresh_id
                    )
                })?;
            if main == Some(published_snapshot_id)
                && snapshot_id_matches_refresh_marker(table, published_snapshot_id, &refresh)?
            {
                if staging.is_some() {
                    drop_iceberg_mv_staging_branch(state, target, entry, staging_branch)?;
                }
                finalize_recovered_iceberg_mv_refresh(state, &refresh)?;
                return Ok(());
            }
            mark_iceberg_mv_refresh_commit_unknown(state, refresh.refresh_id)?;
            Ok(())
        }
        MvRefreshState::Finalized | MvRefreshState::Aborted => Ok(()),
        _ => mark_iceberg_mv_refresh_commit_unknown(state, refresh.refresh_id),
    }
}

fn snapshot_id_matches_refresh_marker(
    table: &iceberg::table::Table,
    snapshot_id: i64,
    refresh: &StoredMvRefresh,
) -> Result<bool, String> {
    let Some(marker) = refresh.marker.as_ref() else {
        return Ok(false);
    };
    let marker = MvRefreshSnapshotMarker {
        refresh_id: marker.refresh_id,
        mv_id: marker.mv_id,
        token: marker.token.clone(),
    };
    let Some(snapshot) = table.metadata().snapshot_by_id(snapshot_id) else {
        return Ok(false);
    };
    Ok(snapshot_matches_refresh_marker(snapshot, &marker))
}

fn recovered_published_snapshot_id(refresh: &StoredMvRefresh) -> Option<i64> {
    refresh.published_snapshot_id.or_else(|| {
        refresh
            .external_outcome
            .as_ref()
            .and_then(|outcome| outcome.target_snapshot_id)
    })
}

fn finalize_recovered_iceberg_mv_refresh(
    state: &Arc<StandaloneState>,
    refresh: &StoredMvRefresh,
) -> Result<(), String> {
    let target_snapshot_id = recovered_published_snapshot_id(refresh)
        .or(refresh.staging_snapshot_id)
        .ok_or_else(|| {
            format!(
                "mv refresh {} missing recovered target snapshot",
                refresh.refresh_id
            )
        })?;
    let rows = refresh.rows.ok_or_else(|| {
        format!(
            "mv refresh {} missing recovered row count",
            refresh.refresh_id
        )
    })?;
    finalize_iceberg_mv_refresh(
        state,
        refresh.refresh_id,
        rows,
        refresh.target_snapshots.clone(),
        refresh.base_table_uuids.clone(),
        target_snapshot_id,
    )
}

fn handle_iceberg_mv_commit_error(
    state: &Arc<StandaloneState>,
    target: &IcebergMvTarget,
    target_entry: &crate::connector::iceberg::catalog::IcebergCatalogEntry,
    staging_branch: &str,
    refresh_id: i64,
    err: String,
) -> String {
    if is_iceberg_commit_unknown_error(&err) {
        if let Err(mark_err) = mark_iceberg_mv_refresh_commit_unknown(state, refresh_id) {
            return format!(
                "{err}; additionally failed to mark mv refresh commit unknown: {mark_err}"
            );
        }
    } else {
        return handle_iceberg_mv_definite_pre_publish_error(
            state,
            target,
            target_entry,
            staging_branch,
            refresh_id,
            err,
        );
    }
    err
}

fn handle_iceberg_mv_definite_pre_publish_error(
    state: &Arc<StandaloneState>,
    target: &IcebergMvTarget,
    target_entry: &crate::connector::iceberg::catalog::IcebergCatalogEntry,
    staging_branch: &str,
    refresh_id: i64,
    err: String,
) -> String {
    let err = cleanup_iceberg_mv_staging_branch_after_failure(
        state,
        target,
        target_entry,
        staging_branch,
        err,
    );
    if let Err(abort_err) = abort_iceberg_mv_refresh(state, refresh_id) {
        return format!("{err}; additionally failed to abort mv refresh: {abort_err}");
    }
    err
}

fn cleanup_iceberg_mv_staging_branch_after_failure(
    state: &Arc<StandaloneState>,
    target: &IcebergMvTarget,
    target_entry: &crate::connector::iceberg::catalog::IcebergCatalogEntry,
    staging_branch: &str,
    err: String,
) -> String {
    match drop_iceberg_mv_staging_branch(state, target, target_entry, staging_branch) {
        Ok(()) => err,
        Err(cleanup_err) => format!(
            "{err}; additionally failed to drop staging branch {staging_branch}: {cleanup_err}"
        ),
    }
}

fn load_iceberg_mv_refresh_marker(
    state: &Arc<StandaloneState>,
    refresh_id: i64,
    mv_id: i64,
) -> Result<MvRefreshSnapshotMarker, String> {
    let provider = state
        .metadata_provider
        .as_ref()
        .ok_or_else(|| "metadata provider required for iceberg mv refresh".to_string())?;
    let txn = provider
        .begin_read()
        .map_err(|e| format!("open iceberg mv refresh marker read transaction failed: {e}"))?;
    let refresh = state
        .mv_repo
        .load_refresh(txn.as_ref(), refresh_id)
        .map_err(|e| format!("load iceberg mv refresh marker failed: {e}"))?
        .ok_or_else(|| format!("mv refresh {refresh_id} not found"))?;
    if refresh.mv_id != mv_id {
        return Err(format!(
            "mv refresh {refresh_id} belongs to mv {}, expected {mv_id}",
            refresh.mv_id
        ));
    }
    let marker = refresh
        .marker
        .ok_or_else(|| format!("mv refresh {refresh_id} missing iceberg commit marker"))?;
    Ok(MvRefreshSnapshotMarker {
        refresh_id: marker.refresh_id,
        mv_id: marker.mv_id,
        token: marker.token,
    })
}

fn record_iceberg_mv_staging_commit(
    state: &Arc<StandaloneState>,
    refresh_id: i64,
    staging_snapshot_id: i64,
    rows: i64,
    base_table_uuids: BTreeMap<String, String>,
) -> Result<(), String> {
    let provider = state
        .metadata_provider
        .as_ref()
        .ok_or_else(|| "metadata provider required for iceberg mv refresh".to_string())?;
    let mut txn = provider
        .begin_write("record iceberg materialized view staging commit")
        .map_err(|e| format!("open iceberg mv staging commit transaction failed: {e}"))?;
    state
        .mv_repo
        .record_staging_commit(
            txn.as_mut(),
            RecordStagingCommitRequest {
                refresh_id,
                staging_snapshot_id,
                rows,
                base_table_uuids,
            },
        )
        .map_err(|e| format!("record iceberg mv staging commit failed: {e}"))?;
    txn.commit()
        .map_err(|e| format!("commit iceberg mv staging commit failed: {e}"))?;
    Ok(())
}

fn record_iceberg_mv_publish_commit(
    state: &Arc<StandaloneState>,
    refresh_id: i64,
    published_snapshot_id: i64,
) -> Result<(), String> {
    let provider = state
        .metadata_provider
        .as_ref()
        .ok_or_else(|| "metadata provider required for iceberg mv refresh".to_string())?;
    let mut txn = provider
        .begin_write("record iceberg materialized view publish commit")
        .map_err(|e| format!("open iceberg mv publish commit transaction failed: {e}"))?;
    state
        .mv_repo
        .record_publish_commit(
            txn.as_mut(),
            RecordPublishCommitRequest {
                refresh_id,
                published_snapshot_id,
            },
        )
        .map_err(|e| format!("record iceberg mv publish commit failed: {e}"))?;
    txn.commit()
        .map_err(|e| format!("commit iceberg mv publish commit failed: {e}"))?;
    Ok(())
}

fn record_iceberg_mv_metadata_only_publish(
    state: &Arc<StandaloneState>,
    refresh_id: i64,
    target_snapshot_id: i64,
) -> Result<(), String> {
    let provider = state
        .metadata_provider
        .as_ref()
        .ok_or_else(|| "metadata provider required for iceberg mv refresh".to_string())?;
    let mut txn = provider
        .begin_write("record metadata-only iceberg materialized view refresh")
        .map_err(|e| format!("open metadata-only iceberg mv refresh transaction failed: {e}"))?;
    let refresh = state
        .mv_repo
        .load_refresh(txn.as_ref(), refresh_id)
        .map_err(|e| format!("load metadata-only iceberg mv refresh failed: {e}"))?
        .ok_or_else(|| format!("mv refresh {refresh_id} not found"))?;
    match refresh.state {
        MvRefreshState::IntentCreated => state
            .mv_repo
            .record_external_commit_outcome(
                txn.as_mut(),
                refresh_id,
                RefreshExternalOutcome {
                    target_snapshot_id: Some(target_snapshot_id),
                    commit_id: format!("iceberg-snapshot-{target_snapshot_id}"),
                },
            )
            .map_err(|e| format!("record metadata-only iceberg mv refresh outcome failed: {e}"))?,
        MvRefreshState::PublishCommitted => {}
        MvRefreshState::Finalized => {}
        _ => {
            return Err(format!(
                "mv refresh {refresh_id} is {}, expected {}, {}, or {}",
                refresh.state.as_str(),
                MvRefreshState::IntentCreated.as_str(),
                MvRefreshState::PublishCommitted.as_str(),
                MvRefreshState::Finalized.as_str()
            ));
        }
    }
    txn.commit()
        .map_err(|e| format!("commit metadata-only iceberg mv refresh failed: {e}"))?;
    Ok(())
}

fn finalize_iceberg_mv_refresh(
    state: &Arc<StandaloneState>,
    refresh_id: i64,
    rows: i64,
    base_snapshots: std::collections::BTreeMap<String, i64>,
    base_table_uuids: std::collections::BTreeMap<String, String>,
    target_snapshot_id: i64,
) -> Result<(), String> {
    record_iceberg_mv_metadata_only_publish(state, refresh_id, target_snapshot_id)?;
    let provider = state
        .metadata_provider
        .as_ref()
        .ok_or_else(|| "metadata provider required for iceberg mv refresh".to_string())?;
    let mut txn = provider
        .begin_write("finalize iceberg materialized view refresh")
        .map_err(|e| format!("open iceberg mv refresh finalize transaction failed: {e}"))?;
    state
        .mv_repo
        .finalize_refresh(
            txn.as_mut(),
            MvRefreshFinalizeRequest {
                refresh_id,
                rows,
                base_snapshots,
                base_table_uuids,
                target_snapshot_id: Some(target_snapshot_id),
            },
        )
        .map_err(|e| format!("finalize iceberg mv refresh failed: {e}"))?;
    txn.commit()
        .map_err(|e| format!("commit iceberg mv refresh finalize failed: {e}"))?;
    Ok(())
}

fn ensure_iceberg_mv_staging_branch(
    catalog: &Arc<dyn Catalog>,
    target: &IcebergMvTarget,
    staging_branch: &str,
    expected_main_snapshot_id: Option<i64>,
) -> Result<(), String> {
    let Some(snapshot_id) = expected_main_snapshot_id else {
        return Ok(());
    };
    data_block_on(async {
        execute_ref_action(
            catalog.as_ref(),
            &RefActionPlan {
                catalog: target.catalog.clone(),
                namespace: target.namespace.clone(),
                table: target.table.clone(),
                action: RefAction::CreateBranch {
                    name: staging_branch.to_string(),
                    snapshot_id,
                    replace: false,
                    if_not_exists: false,
                },
            },
        )
        .await
    })?
    .map(|_| ())
}

#[allow(clippy::too_many_arguments)]
fn publish_iceberg_mv_refresh(
    state: &Arc<StandaloneState>,
    target: &IcebergMvTarget,
    target_entry: &crate::connector::iceberg::catalog::IcebergCatalogEntry,
    staging_branch: &str,
    expected_main_snapshot_id: Option<i64>,
    staging_snapshot_id: i64,
    refresh_id: i64,
    mv_id: i64,
) -> Result<i64, String> {
    let marker = load_iceberg_mv_refresh_marker(state, refresh_id, mv_id)?;
    data_block_on(async {
        let catalog =
            crate::connector::iceberg::catalog::registry::build_iceberg_catalog(target_entry)?;
        publish_staging_branch_to_main(
            catalog.as_ref(),
            &MvRefreshPublishPlan {
                namespace: target.namespace.clone(),
                table: target.table.clone(),
                staging_branch: staging_branch.to_string(),
                expected_main_snapshot_id,
                staging_snapshot_id,
                marker,
            },
        )
        .await
        .map(|outcome| outcome.published_snapshot_id)
    })?
}

fn drop_iceberg_mv_staging_branch(
    state: &Arc<StandaloneState>,
    target: &IcebergMvTarget,
    target_entry: &crate::connector::iceberg::catalog::IcebergCatalogEntry,
    staging_branch: &str,
) -> Result<(), String> {
    data_block_on(async {
        let catalog =
            crate::connector::iceberg::catalog::registry::build_iceberg_catalog(target_entry)?;
        execute_ref_action(
            catalog.as_ref(),
            &RefActionPlan {
                catalog: target.catalog.clone(),
                namespace: target.namespace.clone(),
                table: target.table.clone(),
                action: RefAction::DropBranch {
                    name: staging_branch.to_string(),
                    if_exists: false,
                },
            },
        )
        .await
    })?
    .map(|_| ())?;
    register_iceberg_mv_target_in_catalog(state, target)?;
    Ok(())
}

/// Execute the first refresh of an iceberg-backed MV.
///
/// Steps:
/// 1. Run the MV's SELECT against the base table.
/// 2. Write the resulting chunks as Iceberg/Parquet data files.
/// 3. Commit a fast-append snapshot to the refresh staging branch.
/// 4. Record staging metadata, publish the staging snapshot to main, and finalize.
///
/// On failure before the staging commit, repository metadata is aborted. Once
/// the staging snapshot is committed, repository metadata records the refresh
/// stage before main is advanced.
#[allow(clippy::too_many_arguments)]
fn first_refresh_iceberg_mv(
    state: &Arc<StandaloneState>,
    ctx: &IcebergMvRefreshContext,
    staging_branch: &str,
    refresh_id: i64,
    base_ref: &IcebergTableRef,
    base_snapshot_id: i64,
    current_table_uuid: &str,
    pinned_full_select_sql: &str,
) -> Result<StatementResult, String> {
    let target = &ctx.rewrite.target;
    let target_entry = &*ctx.target_entry;
    let iceberg_catalog = &ctx.iceberg_catalog;
    let expected_main_snapshot_id = ctx.rewrite.target_snapshot_id;
    let current_database = ctx.rewrite.current_database.as_str();
    let mv_definition = &*ctx.rewrite.mv_definition;

    // 1. Run SELECT and collect chunks.
    let physical_sql = iceberg_mv_physical_select_sql(pinned_full_select_sql)?;
    let chunks = match run_mv_full_select_chunks(state, current_database, &physical_sql) {
        Ok(chunks) => chunks,
        Err(err) => {
            abort_iceberg_mv_refresh(state, refresh_id)?;
            return Err(err);
        }
    };
    let total_rows: i64 = chunks.iter().map(|c| c.batch.num_rows() as i64).sum();

    // If the base table is currently empty, do not commit an empty Iceberg
    // snapshot.  Leave the mv_row in pre-refresh state so the next REFRESH
    // can re-enter first-refresh once the base table has data.
    if total_rows == 0 {
        tracing::info!(
            "iceberg mv {}.{}.{}: first refresh produced 0 rows; \
             skipping snapshot commit so next REFRESH can retry",
            target.catalog,
            target.namespace,
            target.table
        );
        abort_iceberg_mv_refresh(state, refresh_id)?;
        return Ok(StatementResult::Ok);
    }

    // 2–3. Write data files and commit snapshot inside an async block.
    let ident = iceberg_mv_table_ident(target)?;
    let marker = load_iceberg_mv_refresh_marker(state, refresh_id, mv_definition.mv_id)?
        .to_summary_properties();
    if let Err(err) = ensure_iceberg_mv_staging_branch(
        iceberg_catalog,
        target,
        staging_branch,
        expected_main_snapshot_id,
    ) {
        abort_iceberg_mv_refresh(state, refresh_id)?;
        return Err(err);
    }
    let target_table = match reload_iceberg_mv_target_table(target_entry, target) {
        Ok(table) => table,
        Err(err) => {
            return Err(handle_iceberg_mv_definite_pre_publish_error(
                state,
                target,
                target_entry,
                staging_branch,
                refresh_id,
                err,
            ));
        }
    };
    let new_snapshot_id = match data_block_on(async {
        let data_files = write_chunks_as_iceberg_data_files(&target_table, &chunks).await?;
        commit_iceberg_mv_target_files_with_ref(
            &target_table,
            iceberg_catalog,
            target_entry,
            &ident,
            CommitOpKind::FastAppend,
            data_files,
            staging_branch,
            marker,
        )
        .await
        .map(|outcome| outcome.new_snapshot_id)
    }) {
        Ok(Ok(snapshot_id)) => snapshot_id,
        Ok(Err(err)) | Err(err) => {
            return Err(handle_iceberg_mv_commit_error(
                state,
                target,
                target_entry,
                staging_branch,
                refresh_id,
                err,
            ));
        }
    };

    // 4. Persist refresh metadata in the repository.
    let snapshots = single_snapshot_map(base_ref, base_snapshot_id);
    let table_uuids = single_table_uuid_map(base_ref, current_table_uuid);
    record_iceberg_mv_staging_commit(
        state,
        refresh_id,
        new_snapshot_id,
        total_rows,
        table_uuids.clone(),
    )?;
    let published_snapshot_id = publish_iceberg_mv_refresh(
        state,
        target,
        target_entry,
        staging_branch,
        expected_main_snapshot_id,
        new_snapshot_id,
        refresh_id,
        mv_definition.mv_id,
    )?;
    record_iceberg_mv_publish_commit(state, refresh_id, published_snapshot_id)?;
    // Once publish is recorded, cleanup must happen before terminal metadata
    // finalization so recovery can retry cleanup after a crash.
    drop_iceberg_mv_staging_branch(state, target, target_entry, staging_branch)?;
    finalize_iceberg_mv_refresh(
        state,
        refresh_id,
        total_rows,
        snapshots.clone(),
        table_uuids.clone(),
        published_snapshot_id,
    )?;

    tracing::info!(
        "iceberg mv {}.{}.{}: first refresh complete: \
         rows={total_rows} iceberg_snapshot={published_snapshot_id}",
        target.catalog,
        target.namespace,
        target.table
    );
    Ok(StatementResult::Ok)
}

fn run_mv_full_select_result(
    state: &Arc<StandaloneState>,
    current_catalog: Option<&str>,
    current_database: &str,
    mut query: sqlparser::ast::Query,
) -> Result<crate::runtime::query_result::QueryResult, String> {
    if crate::engine::query_prep::has_time_travel_refs(&query) {
        crate::engine::query_prep::rewrite_time_travel_refs(
            state,
            current_catalog,
            current_database,
            &mut query,
        )?;
    }
    let three_parts = crate::sql::parser::query_refs::extract_three_part_table_refs(&query);
    if current_catalog.is_some() || !three_parts.is_empty() {
        crate::engine::query_prep::refresh_external_tables_for_query(
            state,
            current_catalog,
            current_database,
            &query,
        )?;
    }

    if !three_parts.is_empty() {
        crate::sql::parser::query_refs::strip_catalog_from_three_part_names(&mut query);
    }
    let catalog_snapshot = state
        .catalog
        .read()
        .expect("standalone catalog read lock")
        .clone();
    crate::engine::execute_query(
        &query,
        &catalog_snapshot,
        current_database,
        state.exchange_port,
        None,
    )
}

#[allow(clippy::too_many_arguments)]
fn first_refresh_iceberg_aggregate_mv(
    state: &Arc<StandaloneState>,
    target: &IcebergMvTarget,
    target_entry: &crate::connector::iceberg::catalog::IcebergCatalogEntry,
    iceberg_catalog: &Arc<dyn iceberg::Catalog>,
    expected_main_snapshot_id: Option<i64>,
    staging_branch: &str,
    refresh_id: i64,
    current_catalog: Option<&str>,
    current_database: &str,
    mv_definition: &StoredMvDefinition,
    aggregate_shape: &AggregateMvShape,
    pin: &crate::connector::starrocks::managed::refresh_pin::RefreshSnapshotPin,
) -> Result<StatementResult, String> {
    let chunks = match prepare_aggregate_first_refresh_chunks(
        state,
        current_catalog,
        current_database,
        mv_definition,
        aggregate_shape,
        pin,
    ) {
        Ok(chunks) => chunks,
        Err(err) => {
            abort_iceberg_mv_refresh(state, refresh_id)?;
            return Err(err);
        }
    };
    let total_rows: i64 = chunks.iter().map(|c| c.batch.num_rows() as i64).sum();

    if total_rows == 0 {
        tracing::info!(
            "iceberg aggregate mv {}.{}.{}: first refresh produced 0 rows; \
             skipping snapshot commit so next REFRESH can retry",
            target.catalog,
            target.namespace,
            target.table
        );
        abort_iceberg_mv_refresh(state, refresh_id)?;
        return Ok(StatementResult::Ok);
    }

    let ident = iceberg_mv_table_ident(target)?;
    let marker = load_iceberg_mv_refresh_marker(state, refresh_id, mv_definition.mv_id)?
        .to_summary_properties();
    if let Err(err) = ensure_iceberg_mv_staging_branch(
        iceberg_catalog,
        target,
        staging_branch,
        expected_main_snapshot_id,
    ) {
        abort_iceberg_mv_refresh(state, refresh_id)?;
        return Err(err);
    }
    let target_table = match reload_iceberg_mv_target_table(target_entry, target) {
        Ok(table) => table,
        Err(err) => {
            return Err(handle_iceberg_mv_definite_pre_publish_error(
                state,
                target,
                target_entry,
                staging_branch,
                refresh_id,
                err,
            ));
        }
    };
    let new_snapshot_id = match data_block_on(async {
        let data_files = write_chunks_as_iceberg_data_files(&target_table, &chunks).await?;
        commit_iceberg_mv_target_files_with_ref(
            &target_table,
            iceberg_catalog,
            target_entry,
            &ident,
            CommitOpKind::FastAppend,
            data_files,
            staging_branch,
            marker,
        )
        .await
        .map(|outcome| outcome.new_snapshot_id)
    }) {
        Ok(Ok(snapshot_id)) => snapshot_id,
        Ok(Err(err)) | Err(err) => {
            return Err(handle_iceberg_mv_commit_error(
                state,
                target,
                target_entry,
                staging_branch,
                refresh_id,
                err,
            ));
        }
    };

    let snapshots = pin.to_snapshot_map();
    let table_uuids = pin.to_table_uuid_map();
    record_iceberg_mv_staging_commit(
        state,
        refresh_id,
        new_snapshot_id,
        total_rows,
        table_uuids.clone(),
    )?;
    let published_snapshot_id = publish_iceberg_mv_refresh(
        state,
        target,
        target_entry,
        staging_branch,
        expected_main_snapshot_id,
        new_snapshot_id,
        refresh_id,
        mv_definition.mv_id,
    )?;
    record_iceberg_mv_publish_commit(state, refresh_id, published_snapshot_id)?;
    drop_iceberg_mv_staging_branch(state, target, target_entry, staging_branch)?;
    finalize_iceberg_mv_refresh(
        state,
        refresh_id,
        total_rows,
        snapshots,
        table_uuids,
        published_snapshot_id,
    )?;
    tracing::info!(
        "iceberg aggregate mv {}.{}.{}: first refresh complete: \
         rows={total_rows} iceberg_snapshot={published_snapshot_id}",
        target.catalog,
        target.namespace,
        target.table
    );
    Ok(StatementResult::Ok)
}

fn prepare_aggregate_first_refresh_chunks(
    state: &Arc<StandaloneState>,
    current_catalog: Option<&str>,
    current_database: &str,
    mv_definition: &StoredMvDefinition,
    aggregate_shape: &AggregateMvShape,
    pin: &crate::connector::starrocks::managed::refresh_pin::RefreshSnapshotPin,
) -> Result<Vec<crate::exec::chunk::Chunk>, String> {
    let state_sql =
        iceberg_aggregate_first_refresh_select_sql(&mv_definition.select_sql, aggregate_shape)?;
    let mut state_query = parse_mv_select_query(&state_sql)?;
    crate::connector::starrocks::managed::refresh_pin::inject_pin_as_for_version_as_of(
        &mut state_query,
        pin,
        &HashSet::new(),
        current_catalog,
        current_database,
    )?;
    let layout = build_aggregate_layout_for_refresh(
        state,
        current_catalog,
        current_database,
        mv_definition,
        aggregate_shape,
    )?;
    let result = run_mv_full_select_result(state, current_catalog, current_database, state_query)?;
    crate::connector::starrocks::managed::mv_agg_state::materialize_aggregate_result_chunks(
        result,
        &layout,
        aggregate_shape,
    )
}

fn build_aggregate_layout_for_refresh(
    state: &Arc<StandaloneState>,
    current_catalog: Option<&str>,
    current_database: &str,
    mv_definition: &StoredMvDefinition,
    aggregate_shape: &AggregateMvShape,
) -> Result<crate::connector::starrocks::managed::mv_agg_state::AggregateMvLayout, String> {
    let visible_query = parse_mv_select_query(&mv_definition.select_sql)?;
    let visible_analysis =
        analyze_mv_select(state, current_catalog, current_database, &visible_query)?;
    crate::connector::starrocks::managed::mv_agg_state::build_aggregate_mv_layout(
        aggregate_shape,
        &visible_analysis.output_columns,
    )
}

#[allow(clippy::too_many_arguments)]
fn rebuild_iceberg_mv(
    state: &Arc<StandaloneState>,
    target: &IcebergMvTarget,
    target_entry: &crate::connector::iceberg::catalog::IcebergCatalogEntry,
    iceberg_catalog: &Arc<dyn iceberg::Catalog>,
    expected_main_snapshot_id: Option<i64>,
    staging_branch: &str,
    refresh_id: i64,
    current_database: &str,
    mv_definition: &StoredMvDefinition,
    pinned_full_select_sql: &str,
    base_ref: &IcebergTableRef,
    base_snapshot_id: Option<i64>,
    current_table_uuid: &str,
) -> Result<StatementResult, String> {
    let physical_sql = iceberg_mv_physical_select_sql(pinned_full_select_sql)?;
    let chunks = match run_mv_full_select_chunks(state, current_database, &physical_sql) {
        Ok(chunks) => chunks,
        Err(err) => {
            abort_iceberg_mv_refresh(state, refresh_id)?;
            return Err(err);
        }
    };
    let total_rows: i64 = chunks.iter().map(|c| c.batch.num_rows() as i64).sum();

    let ident = iceberg_mv_table_ident(target)?;
    let marker = load_iceberg_mv_refresh_marker(state, refresh_id, mv_definition.mv_id)?
        .to_summary_properties();
    if let Err(err) = ensure_iceberg_mv_staging_branch(
        iceberg_catalog,
        target,
        staging_branch,
        expected_main_snapshot_id,
    ) {
        abort_iceberg_mv_refresh(state, refresh_id)?;
        return Err(err);
    }
    let target_table = match reload_iceberg_mv_target_table(target_entry, target) {
        Ok(table) => table,
        Err(err) => {
            return Err(handle_iceberg_mv_definite_pre_publish_error(
                state,
                target,
                target_entry,
                staging_branch,
                refresh_id,
                err,
            ));
        }
    };
    let new_snapshot_id = match data_block_on(async {
        let data_files = if chunks.iter().all(|c| c.batch.num_rows() == 0) {
            Vec::new()
        } else {
            write_chunks_as_iceberg_data_files(&target_table, &chunks).await?
        };
        commit_overwrite_iceberg_mv_with_ref(
            &target_table,
            iceberg_catalog,
            target_entry,
            &ident,
            data_files,
            staging_branch,
            marker,
        )
        .await
    }) {
        Ok(Ok(snapshot_id)) => snapshot_id,
        Ok(Err(err)) | Err(err) => {
            return Err(handle_iceberg_mv_commit_error(
                state,
                target,
                target_entry,
                staging_branch,
                refresh_id,
                err,
            ));
        }
    };

    let snapshots = base_snapshot_id
        .map(|snapshot_id| single_snapshot_map(base_ref, snapshot_id))
        .unwrap_or_default();
    let table_uuids = single_table_uuid_map(base_ref, current_table_uuid);
    record_iceberg_mv_staging_commit(
        state,
        refresh_id,
        new_snapshot_id,
        total_rows,
        table_uuids.clone(),
    )?;
    let published_snapshot_id = publish_iceberg_mv_refresh(
        state,
        target,
        target_entry,
        staging_branch,
        expected_main_snapshot_id,
        new_snapshot_id,
        refresh_id,
        mv_definition.mv_id,
    )?;
    record_iceberg_mv_publish_commit(state, refresh_id, published_snapshot_id)?;
    drop_iceberg_mv_staging_branch(state, target, target_entry, staging_branch)?;
    finalize_iceberg_mv_refresh(
        state,
        refresh_id,
        total_rows,
        snapshots.clone(),
        table_uuids.clone(),
        published_snapshot_id,
    )?;

    Ok(StatementResult::Ok)
}

async fn commit_overwrite_iceberg_mv_with_ref(
    table: &iceberg::table::Table,
    catalog: &Arc<dyn Catalog>,
    entry: &crate::connector::iceberg::catalog::IcebergCatalogEntry,
    ident: &TableIdent,
    data_files: Vec<DataFile>,
    target_ref: &str,
    snapshot_properties: BTreeMap<String, String>,
) -> Result<i64, String> {
    commit_iceberg_mv_target_files_with_ref(
        table,
        catalog,
        entry,
        ident,
        CommitOpKind::Overwrite,
        data_files,
        target_ref,
        snapshot_properties,
    )
    .await
    .map(|outcome| outcome.new_snapshot_id)
}

async fn commit_iceberg_mv_target_files(
    table: &iceberg::table::Table,
    catalog: &Arc<dyn Catalog>,
    entry: &crate::connector::iceberg::catalog::IcebergCatalogEntry,
    ident: &TableIdent,
    op_kind: CommitOpKind,
    data_files: Vec<DataFile>,
) -> Result<CommitOutcome, String> {
    commit_iceberg_mv_target_files_with_ref(
        table,
        catalog,
        entry,
        ident,
        op_kind,
        data_files,
        "main",
        BTreeMap::new(),
    )
    .await
}

#[allow(clippy::too_many_arguments)]
async fn commit_iceberg_mv_target_files_with_ref(
    table: &iceberg::table::Table,
    catalog: &Arc<dyn Catalog>,
    entry: &crate::connector::iceberg::catalog::IcebergCatalogEntry,
    ident: &TableIdent,
    op_kind: CommitOpKind,
    data_files: Vec<DataFile>,
    target_ref: &str,
    snapshot_properties: BTreeMap<String, String>,
) -> Result<CommitOutcome, String> {
    let metadata = table.metadata();
    let staging_dir = format!(
        "{}/data/_staging/{}",
        metadata.location(),
        uuid::Uuid::new_v4()
    );
    let collector = Arc::new(IcebergCommitCollector::new(
        op_kind,
        ident.clone(),
        metadata
            .refs()
            .get(target_ref)
            .map(|r| r.snapshot_id)
            .or_else(|| {
                if target_ref == "main" {
                    metadata.current_snapshot().map(|s| s.snapshot_id())
                } else {
                    None
                }
            }),
        metadata.last_sequence_number(),
        metadata.current_schema().clone(),
        metadata.default_partition_spec().clone(),
        staging_dir,
        crate::common::types::UniqueId { hi: 0, lo: 0 },
    ));
    let default_spec_id = metadata.default_partition_spec_id();
    for df in data_files {
        collector.inject_written_file(crate::engine::iceberg_writer::data_file_to_written_file(
            &df,
            default_spec_id,
        )?);
    }

    let abort_cleanup =
        crate::engine::iceberg_writer::build_abort_cleanup_for_catalog_entry(entry)?;

    let mut outcome = match run_iceberg_commit(RunInput {
        collector: collector.clone(),
        catalog: catalog.clone(),
        table: table.clone(),
        fs: abort_cleanup.fs,
        file_io: table.file_io().clone(),
        cleanup_path_mapper: abort_cleanup.path_mapper,
        cow_update_rewrite: None,
        target_ref: target_ref.to_string(),
        snapshot_properties,
    })
    .await
    {
        Ok(outcome) => outcome,
        Err(err)
            if target_ref != "main" && err.contains("committed but new snapshot not visible") =>
        {
            let reloaded = catalog
                .load_table(ident)
                .await
                .map_err(|e| format!("load iceberg table after branch commit recovery failed: {e}; original error: {err}"))?;
            let new_snapshot_id = reloaded
                .metadata()
                .refs()
                .get(target_ref)
                .map(|r| r.snapshot_id)
                .ok_or_else(|| {
                    format!(
                        "iceberg branch commit recovery failed because target ref {target_ref} is missing; original error: {err}"
                    )
                })?;
            collector.mark_committed();
            CommitOutcome {
                new_snapshot_id,
                written_manifest_paths: Vec::new(),
            }
        }
        Err(err) => return Err(err),
    };
    if target_ref != "main" {
        let reloaded = catalog
            .load_table(ident)
            .await
            .map_err(|e| format!("load iceberg table after branch commit failed: {e}"))?;
        outcome.new_snapshot_id = reloaded
            .metadata()
            .refs()
            .get(target_ref)
            .map(|r| r.snapshot_id)
            .ok_or_else(|| {
                format!("iceberg branch commit completed but target ref {target_ref} is missing")
            })?;
    }
    Ok(outcome)
}

/// IVM-A1 commit entrypoint: run the iceberg commit against a collector that
/// the merge sink already populated with `WrittenFile`s and
/// `PositionDeleteGroup`s. Mirrors the post-injection portion of
/// [`commit_iceberg_mv_apply_with_ref`] but skips collector construction so
/// the caller can share the collector with the sink.
///
/// The collector's `op_kind` must be set by the caller before any inject
/// calls — typically `CommitOpKind::RowDeltaDv` when the change batch has
/// any DELETE-side rows, `CommitOpKind::FastAppend` otherwise.
#[allow(clippy::too_many_arguments)]
#[allow(dead_code)]
pub(crate) async fn commit_iceberg_mv_with_populated_collector(
    table: &iceberg::table::Table,
    catalog: &Arc<dyn Catalog>,
    entry: &crate::connector::iceberg::catalog::IcebergCatalogEntry,
    ident: &TableIdent,
    collector: Arc<IcebergCommitCollector>,
    target_ref: &str,
    snapshot_properties: BTreeMap<String, String>,
) -> Result<CommitOutcome, String> {
    let abort_cleanup =
        crate::engine::iceberg_writer::build_abort_cleanup_for_catalog_entry(entry)?;
    let mut outcome = match run_iceberg_commit(RunInput {
        collector: collector.clone(),
        catalog: catalog.clone(),
        table: table.clone(),
        fs: abort_cleanup.fs,
        file_io: table.file_io().clone(),
        cleanup_path_mapper: abort_cleanup.path_mapper,
        cow_update_rewrite: None,
        target_ref: target_ref.to_string(),
        snapshot_properties,
    })
    .await
    {
        Ok(outcome) => outcome,
        Err(err)
            if target_ref != "main" && err.contains("committed but new snapshot not visible") =>
        {
            let reloaded = catalog.load_table(ident).await.map_err(|e| {
                format!(
                    "load iceberg table after branch commit recovery failed: {e}; original error: {err}"
                )
            })?;
            let new_snapshot_id = reloaded
                .metadata()
                .refs()
                .get(target_ref)
                .map(|r| r.snapshot_id)
                .ok_or_else(|| {
                    format!(
                        "iceberg branch commit recovery failed because target ref {target_ref} is missing; original error: {err}"
                    )
                })?;
            collector.mark_committed();
            CommitOutcome {
                new_snapshot_id,
                written_manifest_paths: Vec::new(),
            }
        }
        Err(err) => return Err(err),
    };
    if target_ref != "main" {
        let reloaded = catalog
            .load_table(ident)
            .await
            .map_err(|e| format!("load iceberg table after branch commit failed: {e}"))?;
        outcome.new_snapshot_id = reloaded
            .metadata()
            .refs()
            .get(target_ref)
            .map(|r| r.snapshot_id)
            .ok_or_else(|| {
                format!("iceberg branch commit completed but target ref {target_ref} is missing")
            })?;
    }
    Ok(outcome)
}

/// IVM-A1 helper: construct an empty `IcebergCommitCollector` configured for
/// the supplied target table and branch. The caller (refresh driver) hands
/// the resulting `Arc` to `IcebergMergeSinkPlan` so the sink can inject
/// written files / position-delete groups during pipeline execution, then
/// later passes the same `Arc` to
/// [`commit_iceberg_mv_with_populated_collector`].
#[allow(dead_code)]
pub(crate) fn new_iceberg_mv_commit_collector(
    table: &iceberg::table::Table,
    ident: &TableIdent,
    target_ref: &str,
    op_kind: CommitOpKind,
) -> Arc<IcebergCommitCollector> {
    let metadata = table.metadata();
    let staging_dir = format!(
        "{}/data/_staging/{}",
        metadata.location(),
        uuid::Uuid::new_v4()
    );
    let base_snapshot_id = metadata
        .refs()
        .get(target_ref)
        .map(|r| r.snapshot_id)
        .or_else(|| {
            if target_ref == "main" {
                metadata.current_snapshot().map(|s| s.snapshot_id())
            } else {
                None
            }
        });
    Arc::new(IcebergCommitCollector::new(
        op_kind,
        ident.clone(),
        base_snapshot_id,
        metadata.last_sequence_number(),
        metadata.current_schema().clone(),
        metadata.default_partition_spec().clone(),
        staging_dir,
        crate::common::types::UniqueId { hi: 0, lo: 0 },
    ))
}

#[allow(clippy::too_many_arguments)]
async fn commit_iceberg_mv_apply_with_ref(
    table: &iceberg::table::Table,
    catalog: &Arc<dyn Catalog>,
    entry: &crate::connector::iceberg::catalog::IcebergCatalogEntry,
    ident: &TableIdent,
    data_files: Vec<DataFile>,
    delete_groups: Vec<PositionDeleteGroup>,
    target_ref: &str,
    snapshot_properties: BTreeMap<String, String>,
) -> Result<CommitOutcome, String> {
    if delete_groups.is_empty() {
        return commit_iceberg_mv_target_files_with_ref(
            table,
            catalog,
            entry,
            ident,
            CommitOpKind::FastAppend,
            data_files,
            target_ref,
            snapshot_properties,
        )
        .await;
    }

    let metadata = table.metadata();
    let staging_dir = format!(
        "{}/data/_staging/{}",
        metadata.location(),
        uuid::Uuid::new_v4()
    );
    let collector = Arc::new(IcebergCommitCollector::new(
        CommitOpKind::RowDeltaDv,
        ident.clone(),
        metadata
            .refs()
            .get(target_ref)
            .map(|r| r.snapshot_id)
            .or_else(|| {
                if target_ref == "main" {
                    metadata.current_snapshot().map(|s| s.snapshot_id())
                } else {
                    None
                }
            }),
        metadata.last_sequence_number(),
        metadata.current_schema().clone(),
        metadata.default_partition_spec().clone(),
        staging_dir,
        crate::common::types::UniqueId { hi: 0, lo: 0 },
    ));
    let default_spec_id = metadata.default_partition_spec_id();
    for df in data_files {
        collector.inject_written_file(crate::engine::iceberg_writer::data_file_to_written_file(
            &df,
            default_spec_id,
        )?);
    }
    for group in delete_groups {
        collector.inject_delete_group(group);
    }

    let abort_cleanup =
        crate::engine::iceberg_writer::build_abort_cleanup_for_catalog_entry(entry)?;
    let mut outcome = match run_iceberg_commit(RunInput {
        collector: collector.clone(),
        catalog: catalog.clone(),
        table: table.clone(),
        fs: abort_cleanup.fs,
        file_io: table.file_io().clone(),
        cleanup_path_mapper: abort_cleanup.path_mapper,
        cow_update_rewrite: None,
        target_ref: target_ref.to_string(),
        snapshot_properties,
    })
    .await
    {
        Ok(outcome) => outcome,
        Err(err)
            if target_ref != "main" && err.contains("committed but new snapshot not visible") =>
        {
            let reloaded = catalog.load_table(ident).await.map_err(|e| {
                format!(
                    "load iceberg table after branch commit recovery failed: {e}; original error: {err}"
                )
            })?;
            let new_snapshot_id = reloaded
                .metadata()
                .refs()
                .get(target_ref)
                .map(|r| r.snapshot_id)
                .ok_or_else(|| {
                    format!(
                        "iceberg branch commit recovery failed because target ref {target_ref} is missing; original error: {err}"
                    )
                })?;
            collector.mark_committed();
            CommitOutcome {
                new_snapshot_id,
                written_manifest_paths: Vec::new(),
            }
        }
        Err(err) => return Err(err),
    };
    if target_ref != "main" {
        let reloaded = catalog
            .load_table(ident)
            .await
            .map_err(|e| format!("load iceberg table after branch commit failed: {e}"))?;
        outcome.new_snapshot_id = reloaded
            .metadata()
            .refs()
            .get(target_ref)
            .map(|r| r.snapshot_id)
            .ok_or_else(|| {
                format!("iceberg branch commit completed but target ref {target_ref} is missing")
            })?;
    }
    Ok(outcome)
}

/// IVM-A1 AST helper: mutate a parsed MV SELECT in place so the unique
/// reference to `base_ref` becomes a `__nr_ivm_delta(...)` table function
/// call. Returns the number of matches replaced (must be exactly 1 for the
/// caller to proceed).
///
/// Matching rules (case-insensitive, via `normalize_identifier`):
/// - `tbl` matches when `base_ref.table` equals `tbl`. (Bare 1-part name.)
/// - `db.tbl` matches when `(db, tbl)` equals `(base_ref.namespace, base_ref.table)`.
/// - `cat.db.tbl` matches when the full triple equals `base_ref`.
///
/// Aliases are preserved. If the original factor had no alias, the rewritten
/// `__nr_ivm_delta(...)` carries an explicit alias equal to the original base
/// table name so downstream references like `<table>.<col>` keep resolving.
fn mutate_query_for_ivm_delta_scan(
    query: &mut sqlparser::ast::Query,
    base_ref: &IcebergTableRef,
    from_snapshot_id: i64,
    to_snapshot_id: i64,
) -> Result<usize, String> {
    let normalized_base = (
        crate::engine::catalog::normalize_identifier(&base_ref.catalog)?,
        crate::engine::catalog::normalize_identifier(&base_ref.namespace)?,
        crate::engine::catalog::normalize_identifier(&base_ref.table)?,
    );
    let mut state = MutateState {
        normalized_base: &normalized_base,
        base_ref,
        from_snapshot_id,
        to_snapshot_id,
        matches: 0,
        errors: Vec::new(),
    };
    if let Some(with) = &mut query.with {
        for cte in &mut with.cte_tables {
            mutate_set_expr_for_ivm(cte.query.body.as_mut(), &mut state);
        }
    }
    mutate_set_expr_for_ivm(query.body.as_mut(), &mut state);
    if let Some(err) = state.errors.into_iter().next() {
        return Err(err);
    }
    Ok(state.matches)
}

/// Append the IVM `__change_op` pseudo-column reference to the top-level
/// `SELECT` projection so the merge sink can read it from the chunk.
///
/// Only the top-level projection is mutated — subqueries / CTEs are not
/// touched because the top-level chunk is the one that reaches the merge
/// sink, and `__change_op` is only resolvable against
/// `__nr_ivm_delta(...)` source factors that contribute to the top-level
/// scan tuple. Set operations (UNION / EXCEPT / INTERSECT) are rejected
/// because each branch would need its own augmentation; the IVM-A1 contract
/// allows a single `__nr_ivm_delta` reference, so this is a defensive guard
/// rather than a supported shape.
fn append_change_op_to_projection(query: &mut sqlparser::ast::Query) -> Result<(), String> {
    let sqlparser::ast::SetExpr::Select(select) = query.body.as_mut() else {
        return Err(
            "IVM-A1 __change_op projection: top-level SELECT body required (set operations are not supported)"
                .to_string(),
        );
    };
    select
        .projection
        .push(sqlparser::ast::SelectItem::UnnamedExpr(
            sqlparser::ast::Expr::Identifier(sqlparser::ast::Ident::new(
                crate::exec::change_op::CHANGE_OP_COLUMN,
            )),
        ));
    Ok(())
}

struct MutateState<'a> {
    normalized_base: &'a (String, String, String),
    base_ref: &'a IcebergTableRef,
    from_snapshot_id: i64,
    to_snapshot_id: i64,
    matches: usize,
    errors: Vec<String>,
}

fn mutate_set_expr_for_ivm(expr: &mut sqlparser::ast::SetExpr, state: &mut MutateState<'_>) {
    use sqlparser::ast::SetExpr;
    match expr {
        SetExpr::Select(select) => {
            for from in &mut select.from {
                mutate_factor_for_ivm(&mut from.relation, state);
                for join in &mut from.joins {
                    mutate_factor_for_ivm(&mut join.relation, state);
                }
            }
            if let Some(selection) = &mut select.selection {
                mutate_expr_for_ivm(selection, state);
            }
            if let Some(having) = &mut select.having {
                mutate_expr_for_ivm(having, state);
            }
            for projection in &mut select.projection {
                match projection {
                    sqlparser::ast::SelectItem::UnnamedExpr(e)
                    | sqlparser::ast::SelectItem::ExprWithAlias { expr: e, .. } => {
                        mutate_expr_for_ivm(e, state);
                    }
                    _ => {}
                }
            }
        }
        SetExpr::SetOperation { left, right, .. } => {
            mutate_set_expr_for_ivm(left.as_mut(), state);
            mutate_set_expr_for_ivm(right.as_mut(), state);
        }
        SetExpr::Query(q) => {
            mutate_set_expr_for_ivm(q.body.as_mut(), state);
        }
        _ => {}
    }
}

fn mutate_expr_for_ivm(expr: &mut sqlparser::ast::Expr, state: &mut MutateState<'_>) {
    use sqlparser::ast::Expr;
    match expr {
        Expr::Subquery(q) | Expr::Exists { subquery: q, .. } => {
            mutate_set_expr_for_ivm(q.body.as_mut(), state);
        }
        Expr::InSubquery { subquery, expr, .. } => {
            mutate_set_expr_for_ivm(subquery.body.as_mut(), state);
            mutate_expr_for_ivm(expr, state);
        }
        Expr::BinaryOp { left, right, .. } => {
            mutate_expr_for_ivm(left, state);
            mutate_expr_for_ivm(right, state);
        }
        Expr::UnaryOp { expr, .. } | Expr::Nested(expr) => {
            mutate_expr_for_ivm(expr, state);
        }
        Expr::Between {
            expr, low, high, ..
        } => {
            mutate_expr_for_ivm(expr, state);
            mutate_expr_for_ivm(low, state);
            mutate_expr_for_ivm(high, state);
        }
        Expr::Case {
            operand,
            conditions,
            else_result,
            ..
        } => {
            if let Some(op) = operand {
                mutate_expr_for_ivm(op, state);
            }
            for case_when in conditions {
                mutate_expr_for_ivm(&mut case_when.condition, state);
                mutate_expr_for_ivm(&mut case_when.result, state);
            }
            if let Some(else_expr) = else_result {
                mutate_expr_for_ivm(else_expr, state);
            }
        }
        Expr::Cast { expr, .. } => mutate_expr_for_ivm(expr, state),
        _ => {}
    }
}

fn mutate_factor_for_ivm(factor: &mut sqlparser::ast::TableFactor, state: &mut MutateState<'_>) {
    use sqlparser::ast::TableFactor;
    match factor {
        TableFactor::Table {
            name, alias, args, ..
        } => {
            // Skip table-valued function factors (e.g. existing __nr_ivm_delta).
            if args.is_some() {
                return;
            }
            let raw_parts: Vec<String> = name
                .0
                .iter()
                .filter_map(|part| match part {
                    sqlparser::ast::ObjectNamePart::Identifier(ident) => Some(ident.value.clone()),
                    _ => None,
                })
                .collect();
            // Synthetic iceberg-metadata factors (`__nr_meta_*__`) are not
            // base-table references — leave them alone.
            let normalized_lc: Vec<String> =
                raw_parts.iter().map(|s| s.to_ascii_lowercase()).collect();
            let (base_parts, metadata_suffix) =
                crate::sql::analyzer::iceberg_metadata::split_metadata_suffix(&normalized_lc);
            if metadata_suffix.is_some() {
                return;
            }
            let normalized = match base_parts.len() {
                1 => match crate::engine::catalog::normalize_identifier(&base_parts[0]) {
                    Ok(t) => (None, None, t),
                    Err(e) => {
                        state.errors.push(format!(
                            "IVM-A1 base table candidate '{}' normalize: {e}",
                            base_parts[0]
                        ));
                        return;
                    }
                },
                2 => {
                    let db = match crate::engine::catalog::normalize_identifier(&base_parts[0]) {
                        Ok(t) => t,
                        Err(e) => {
                            state.errors.push(format!(
                                "IVM-A1 base table candidate db '{}' normalize: {e}",
                                base_parts[0]
                            ));
                            return;
                        }
                    };
                    let tbl = match crate::engine::catalog::normalize_identifier(&base_parts[1]) {
                        Ok(t) => t,
                        Err(e) => {
                            state.errors.push(format!(
                                "IVM-A1 base table candidate table '{}' normalize: {e}",
                                base_parts[1]
                            ));
                            return;
                        }
                    };
                    (None, Some(db), tbl)
                }
                3 => {
                    let cat = match crate::engine::catalog::normalize_identifier(&base_parts[0]) {
                        Ok(t) => t,
                        Err(e) => {
                            state.errors.push(format!(
                                "IVM-A1 base table candidate catalog '{}' normalize: {e}",
                                base_parts[0]
                            ));
                            return;
                        }
                    };
                    let db = match crate::engine::catalog::normalize_identifier(&base_parts[1]) {
                        Ok(t) => t,
                        Err(e) => {
                            state.errors.push(format!(
                                "IVM-A1 base table candidate db '{}' normalize: {e}",
                                base_parts[1]
                            ));
                            return;
                        }
                    };
                    let tbl = match crate::engine::catalog::normalize_identifier(&base_parts[2]) {
                        Ok(t) => t,
                        Err(e) => {
                            state.errors.push(format!(
                                "IVM-A1 base table candidate table '{}' normalize: {e}",
                                base_parts[2]
                            ));
                            return;
                        }
                    };
                    (Some(cat), Some(db), tbl)
                }
                _ => return,
            };

            let matches_base = match &normalized {
                (Some(cat), Some(db), tbl) => {
                    *cat == state.normalized_base.0
                        && *db == state.normalized_base.1
                        && *tbl == state.normalized_base.2
                }
                (None, Some(db), tbl) => {
                    *db == state.normalized_base.1 && *tbl == state.normalized_base.2
                }
                (None, None, tbl) => *tbl == state.normalized_base.2,
                (Some(_), None, _) => false,
            };
            if !matches_base {
                return;
            }

            state.matches += 1;
            let fqn = format!(
                "{}.{}.{}",
                state.base_ref.catalog, state.base_ref.namespace, state.base_ref.table
            );
            let new_factor = build_nr_ivm_delta_table_factor(
                &fqn,
                state.from_snapshot_id,
                state.to_snapshot_id,
                alias.clone(),
                &state.base_ref.table,
            );
            *factor = new_factor;
        }
        TableFactor::Derived { subquery, .. } => {
            mutate_set_expr_for_ivm(subquery.body.as_mut(), state);
        }
        _ => {}
    }
}

fn build_nr_ivm_delta_table_factor(
    fqn: &str,
    from_snapshot_id: i64,
    to_snapshot_id: i64,
    original_alias: Option<sqlparser::ast::TableAlias>,
    original_table_name: &str,
) -> sqlparser::ast::TableFactor {
    use sqlparser::ast as sqlast;
    let make_string_arg = |s: String| -> sqlast::FunctionArg {
        sqlast::FunctionArg::Unnamed(sqlast::FunctionArgExpr::Expr(sqlast::Expr::Value(
            sqlast::Value::SingleQuotedString(s).into(),
        )))
    };
    let make_number_arg = |n: i64| -> sqlast::FunctionArg {
        sqlast::FunctionArg::Unnamed(sqlast::FunctionArgExpr::Expr(sqlast::Expr::Value(
            sqlast::Value::Number(n.to_string(), false).into(),
        )))
    };
    let args = sqlast::TableFunctionArgs {
        args: vec![
            make_string_arg(fqn.to_string()),
            make_number_arg(from_snapshot_id),
            make_number_arg(to_snapshot_id),
        ],
        settings: None,
    };
    // Preserve the original alias when present, otherwise fall back to the
    // original base table name so projection references that wrote
    // `<table>.<col>` keep resolving. This mirrors the standalone analyzer
    // behaviour for `__nr_ivm_delta(...)` (it uses the alias name or, when
    // absent, the table_def name as the scope qualifier).
    let alias = original_alias.or_else(|| {
        Some(sqlast::TableAlias {
            explicit: false,
            name: sqlast::Ident::new(original_table_name),
            columns: Vec::new(),
        })
    });
    sqlast::TableFactor::Table {
        name: sqlast::ObjectName(vec![sqlast::ObjectNamePart::Identifier(
            sqlast::Ident::new("__nr_ivm_delta"),
        )]),
        alias,
        args: Some(args),
        with_hints: Vec::new(),
        version: None,
        with_ordinality: false,
        partitions: Vec::new(),
        json_path: None,
        sample: None,
        index_hints: Vec::new(),
    }
}

fn parse_mv_select_query(sql: &str) -> Result<sqlparser::ast::Query, String> {
    let normalized = crate::sql::parser::dialect::normalize_for_raw_parse(sql)
        .map_err(|e| format!("stored MV SELECT normalize error: {e}"))?;
    let statement = crate::sql::parser::parse_normalized_sql_raw(&normalized)
        .map_err(|err| format!("sql parser error: {err}"))?;
    let sqlparser::ast::Statement::Query(query) = statement else {
        return Err("stored MV SQL must be a SELECT query".to_string());
    };
    Ok(*query)
}

fn expected_main_snapshot_id_from_table(table: &iceberg::table::Table) -> Option<i64> {
    table.metadata().current_snapshot().map(|s| s.snapshot_id())
}

fn ensure_schema_contract_compatible_for_refresh(
    schema_contract: &crate::meta::repository::mv_contract::MvSchemaContract,
    base_table: &iceberg::table::Table,
    target_table: &iceberg::table::Table,
) -> Result<(), String> {
    match crate::engine::mv::schema_contract::validate_schema_contract(
        schema_contract,
        base_table,
        target_table,
    ) {
        crate::engine::mv::schema_contract::ContractDecision::Incompatible(err) => {
            Err(format!("{err}"))
        }
        crate::engine::mv::schema_contract::ContractDecision::CompatibleSafe
        | crate::engine::mv::schema_contract::ContractDecision::CompatibleSafeWithRebind {
            ..
        } => Ok(()),
    }
}

#[allow(clippy::too_many_arguments)]
fn refresh_iceberg_join_mv(
    state: &Arc<StandaloneState>,
    target: &IcebergMvTarget,
    target_entry: &crate::connector::iceberg::catalog::IcebergCatalogEntry,
    iceberg_catalog: &Arc<dyn iceberg::Catalog>,
    target_table: &iceberg::table::Table,
    expected_main_snapshot_id: Option<i64>,
    current_database: &str,
    mv_definition: &StoredMvDefinition,
    base_refs: &[IcebergTableRef],
    shape: &crate::connector::starrocks::managed::mv_shape::JoinProjectionFilterMvShape,
) -> Result<StatementResult, String> {
    if base_refs.len() != 2 {
        return Err("iceberg join MV refresh requires exactly two base tables".to_string());
    }
    validate_join_shape_base_refs(shape, base_refs)?;
    let schema_contract = mv_definition.schema_contract.as_ref().ok_or_else(|| {
        format!(
            "iceberg MV target {}.{}.{} is missing A11 schema contract; rebuild or recreate the MV",
            target.catalog, target.namespace, target.table
        )
    })?;
    if schema_contract.contract_version != 2 {
        return Err(format!(
            "iceberg join MV {}.{}.{} requires schema contract version 2, got {}",
            target.catalog, target.namespace, target.table, schema_contract.contract_version
        ));
    }
    let (left_ref, right_ref) = join_base_refs_for_shape(shape, base_refs)?;
    let left_loaded_before_pin = load_current_iceberg_base_table(state, left_ref)?;
    let right_loaded_before_pin = load_current_iceberg_base_table(state, right_ref)?;
    let left_current_before_pin =
        expected_main_snapshot_id_from_table(&left_loaded_before_pin.table);
    let right_current_before_pin =
        expected_main_snapshot_id_from_table(&right_loaded_before_pin.table);
    let left_previous = mv_definition
        .last_refresh_snapshots
        .get(&left_ref.fqn())
        .copied();
    let right_previous = mv_definition
        .last_refresh_snapshots
        .get(&right_ref.fqn())
        .copied();
    let pre_pin_join_bases = [
        (left_ref, &left_loaded_before_pin.table),
        (right_ref, &right_loaded_before_pin.table),
    ];
    let _ = validate_join_schema_contract(schema_contract, &pre_pin_join_bases, target_table)?;

    match (
        left_previous,
        right_previous,
        left_current_before_pin,
        right_current_before_pin,
    ) {
        (None, None, None, None) => {
            tracing::info!(
                "iceberg join mv {}.{}.{}: both base tables have no snapshot; skipping refresh",
                target.catalog,
                target.namespace,
                target.table
            );
            return Ok(StatementResult::Ok);
        }
        (None, None, Some(_), None) | (None, None, None, Some(_)) => {
            tracing::info!(
                "iceberg join mv {}.{}.{}: one base table has no snapshot; skipping initial refresh",
                target.catalog,
                target.namespace,
                target.table
            );
            return Ok(StatementResult::Ok);
        }
        (Some(prev), _, None, _) => {
            return Err(format!(
                "cannot refresh iceberg join materialized view {}.{}.{}: previously-refreshed left base snapshot {prev} is no longer reachable",
                target.catalog, target.namespace, target.table
            ));
        }
        (_, Some(prev), _, None) => {
            return Err(format!(
                "cannot refresh iceberg join materialized view {}.{}.{}: previously-refreshed right base snapshot {prev} is no longer reachable",
                target.catalog, target.namespace, target.table
            ));
        }
        _ => {}
    }

    let pin = crate::connector::starrocks::managed::refresh_pin::RefreshSnapshotPin::capture(
        state, base_refs,
    )?;
    if pin.len() != 2 {
        return Err(format!(
            "iceberg join MV refresh expected two refresh pins, got {}",
            pin.len()
        ));
    }
    validate_refresh_pin_table_uuids(mv_definition, &pin, base_refs)?;

    let left_loaded = load_current_iceberg_base_table(state, left_ref)?;
    let right_loaded = load_current_iceberg_base_table(state, right_ref)?;
    let effective_definition = validate_join_schema_contract(
        schema_contract,
        &[
            (left_ref, &left_loaded.table),
            (right_ref, &right_loaded.table),
        ],
        target_table,
    )?
    .into_definition(mv_definition)?;
    let mv_definition = &effective_definition;

    let left_current = pin
        .get(left_ref)
        .ok_or_else(|| format!("missing refresh pin for {}", left_ref.fqn()))?;
    let right_current = pin
        .get(right_ref)
        .ok_or_else(|| format!("missing refresh pin for {}", right_ref.fqn()))?;

    match (left_previous, right_previous) {
        (None, None) => {
            let staging_branch = format!(
                "__nova_mv_refresh_{}_{}",
                mv_definition.mv_id,
                uuid::Uuid::new_v4().simple()
            );
            let refresh_id = begin_staged_iceberg_mv_refresh_intent(
                state,
                target,
                mv_definition.mv_id,
                expected_main_snapshot_id,
                pin.to_snapshot_map(),
                &staging_branch,
            )?;
            first_refresh_iceberg_join_mv(
                state,
                target,
                target_entry,
                iceberg_catalog,
                expected_main_snapshot_id,
                &staging_branch,
                refresh_id,
                current_database,
                mv_definition,
                shape,
                &pin,
                left_ref,
                right_ref,
            )
        }
        (Some(left_prev), Some(right_prev))
            if left_prev == left_current && right_prev == right_current =>
        {
            tracing::info!(
                "iceberg join mv {}.{}.{}: base snapshots unchanged; updating metadata only",
                target.catalog,
                target.namespace,
                target.table
            );
            finalize_iceberg_mv_metadata_only_refresh(
                state,
                target,
                mv_definition,
                pin.to_snapshot_map(),
                pin.to_table_uuid_map(),
            )
        }
        (Some(_), Some(_)) => incremental_refresh_iceberg_join_mv(
            state,
            target,
            target_entry,
            iceberg_catalog,
            expected_main_snapshot_id,
            current_database,
            mv_definition,
            &[left_ref.clone(), right_ref.clone()],
            shape,
            &pin,
        ),
        _ => Err(format!(
            "iceberg join MV {}.{}.{} has partial previous refresh snapshots; recreate the MV",
            target.catalog, target.namespace, target.table
        )),
    }
}

fn join_base_refs_for_shape<'a>(
    shape: &crate::connector::starrocks::managed::mv_shape::JoinProjectionFilterMvShape,
    base_refs: &'a [IcebergTableRef],
) -> Result<(&'a IcebergTableRef, &'a IcebergTableRef), String> {
    let left_name = shape.left_table.to_string();
    let right_name = shape.right_table.to_string();
    let left = base_refs
        .iter()
        .find(|base| base.fqn().eq_ignore_ascii_case(&left_name))
        .ok_or_else(|| format!("join MV left base {left_name} was not resolved"))?;
    let right = base_refs
        .iter()
        .find(|base| base.fqn().eq_ignore_ascii_case(&right_name))
        .ok_or_else(|| format!("join MV right base {right_name} was not resolved"))?;
    Ok((left, right))
}

fn validate_refresh_pin_table_uuids(
    mv_definition: &StoredMvDefinition,
    pin: &crate::connector::starrocks::managed::refresh_pin::RefreshSnapshotPin,
    base_refs: &[IcebergTableRef],
) -> Result<(), String> {
    for base_ref in base_refs {
        let Some(previous_uuid) = mv_definition.last_refresh_table_uuids.get(&base_ref.fqn())
        else {
            continue;
        };
        let current_uuid = pin.uuid(base_ref).ok_or_else(|| {
            format!(
                "refresh pin missing uuid for base {} (this should not happen)",
                base_ref.fqn()
            )
        })?;
        if previous_uuid != current_uuid {
            return Err(format!(
                "iceberg MV base table identity changed for {}; incremental refresh is unsafe, rebuild or recreate the MV",
                base_ref.fqn()
            ));
        }
    }
    Ok(())
}

#[derive(Debug, PartialEq, Eq)]
enum JoinSchemaContractDecision {
    CompatibleSafe,
    CompatibleSafeWithRebind {
        rebound_columns: Vec<crate::engine::mv::schema_contract::RebindColumn>,
    },
}

impl JoinSchemaContractDecision {
    fn into_definition(
        self,
        mv_definition: &StoredMvDefinition,
    ) -> Result<StoredMvDefinition, String> {
        match self {
            Self::CompatibleSafe => Ok(mv_definition.clone()),
            Self::CompatibleSafeWithRebind { rebound_columns } => {
                let rewritten_sql =
                    rewrite_select_sql_for_rebind(&mv_definition.select_sql, &rebound_columns)?;
                let mut def = mv_definition.clone();
                def.select_sql = rewritten_sql;
                Ok(def)
            }
        }
    }
}

fn validate_join_base_schema_contract_for_rebind(
    base_fqn: &str,
    base_contract: &crate::meta::repository::mv_contract::BaseContract,
    current_schema: &iceberg::spec::Schema,
) -> Result<Vec<crate::engine::mv::schema_contract::RebindColumn>, String> {
    let current_schema = current_schema.as_struct();
    let mut rebound = Vec::new();
    for record in &base_contract.schema_at_create.fields {
        let Some(field) = current_schema
            .fields()
            .iter()
            .find(|field| field.id == record.field_id)
        else {
            return Err(format!(
                "iceberg join MV refresh blocked: base column \"{}\" (field id {}) was dropped from {}; recreate the MV",
                record.name_at_create, record.field_id, base_fqn
            ));
        };
        if format!("{}", field.field_type) != record.type_signature {
            return Err(format!(
                "iceberg join MV refresh blocked: base column \"{}\" (field id {}) changed type from {} to {}; recreate the MV",
                record.name_at_create, record.field_id, record.type_signature, field.field_type
            ));
        }
        if field.required != record.required {
            return Err(format!(
                "iceberg join MV refresh blocked: base column \"{}\" (field id {}) changed nullability; recreate the MV",
                record.name_at_create, record.field_id
            ));
        }
        if !field.name.eq_ignore_ascii_case(&record.name_at_create) {
            rebound.push(crate::engine::mv::schema_contract::RebindColumn {
                base_table_fqn: base_fqn.to_string(),
                field_id: record.field_id,
                name_at_create: record.name_at_create.clone(),
                current_name: field.name.clone(),
            });
        }
    }
    Ok(rebound)
}

fn validate_join_schema_contract(
    contract: &crate::meta::repository::mv_contract::MvSchemaContract,
    bases: &[(&IcebergTableRef, &iceberg::table::Table); 2],
    target_table: &iceberg::table::Table,
) -> Result<JoinSchemaContractDecision, String> {
    contract
        .ensure_self_consistent()
        .map_err(|e| format!("Iceberg join MV schema contract is self-inconsistent: {e}"))?;
    if contract.bases.len() != 2 {
        return Err(format!(
            "Iceberg join MV schema contract requires two base contracts, got {}",
            contract.bases.len()
        ));
    }
    if contract.target.table_uuid != target_table.metadata().uuid().to_string() {
        return Err(
            "iceberg join MV refresh blocked: target table identity changed; recreate the MV"
                .to_string(),
        );
    }
    let mut rebound_columns = Vec::new();
    for (base_ref, table) in bases {
        ensure_base_row_lineage_contract(table, &base_ref.fqn())?;
        let base_contract = contract
            .bases
            .iter()
            .find(|base| base.table_fqn.eq_ignore_ascii_case(&base_ref.fqn()))
            .ok_or_else(|| {
                format!(
                    "Iceberg join MV schema contract missing base {}",
                    base_ref.fqn()
                )
            })?;
        if base_contract.table_uuid != table.metadata().uuid().to_string() {
            return Err(format!(
                "iceberg join MV refresh blocked: base table identity changed for {}; recreate the MV",
                base_ref.fqn()
            ));
        }
        rebound_columns.extend(validate_join_base_schema_contract_for_rebind(
            &base_ref.fqn(),
            base_contract,
            table.metadata().current_schema(),
        )?);
    }
    let left_schema = bases[0].1.metadata().current_schema();
    match crate::engine::mv::schema_contract::validate_schema_contract_with_base_schema(
        contract,
        bases[0].1,
        left_schema.as_ref(),
        target_table,
    ) {
        crate::engine::mv::schema_contract::ContractDecision::Incompatible(err) => {
            return Err(format!("{err}"));
        }
        crate::engine::mv::schema_contract::ContractDecision::CompatibleSafe
        | crate::engine::mv::schema_contract::ContractDecision::CompatibleSafeWithRebind {
            ..
        } => {}
    }
    if rebound_columns.is_empty() {
        Ok(JoinSchemaContractDecision::CompatibleSafe)
    } else {
        Ok(JoinSchemaContractDecision::CompatibleSafeWithRebind { rebound_columns })
    }
}

fn iceberg_change_batch_has_row_deletes(
    batch: &crate::connector::iceberg::changes::IcebergChangeBatch,
) -> bool {
    !batch.deletes.is_empty()
        || !batch.equality_deletes.is_empty()
        || !batch.deleted_data_files.is_empty()
}

fn finalize_iceberg_mv_metadata_only_refresh(
    state: &Arc<StandaloneState>,
    target: &IcebergMvTarget,
    mv_definition: &StoredMvDefinition,
    snapshots: BTreeMap<String, i64>,
    table_uuids: BTreeMap<String, String>,
) -> Result<StatementResult, String> {
    let target_snapshot_id = recorded_target_snapshot_id(target, mv_definition)?;
    let refresh_id =
        begin_iceberg_mv_refresh_intent(state, mv_definition.mv_id, snapshots.clone())?;
    finalize_iceberg_mv_refresh(
        state,
        refresh_id,
        mv_definition.last_refresh_rows.unwrap_or(0),
        snapshots,
        table_uuids,
        target_snapshot_id,
    )?;
    Ok(StatementResult::Ok)
}

#[allow(clippy::too_many_arguments)]
fn first_refresh_iceberg_join_mv(
    state: &Arc<StandaloneState>,
    target: &IcebergMvTarget,
    target_entry: &crate::connector::iceberg::catalog::IcebergCatalogEntry,
    iceberg_catalog: &Arc<dyn iceberg::Catalog>,
    expected_main_snapshot_id: Option<i64>,
    staging_branch: &str,
    refresh_id: i64,
    current_database: &str,
    mv_definition: &StoredMvDefinition,
    shape: &crate::connector::starrocks::managed::mv_shape::JoinProjectionFilterMvShape,
    pin: &crate::connector::starrocks::managed::refresh_pin::RefreshSnapshotPin,
    left_ref: &IcebergTableRef,
    right_ref: &IcebergTableRef,
) -> Result<StatementResult, String> {
    if let Err(err) = ensure_iceberg_mv_staging_branch(
        iceberg_catalog,
        target,
        staging_branch,
        expected_main_snapshot_id,
    ) {
        abort_iceberg_mv_refresh(state, refresh_id)?;
        return Err(err);
    }
    let target_table = match reload_iceberg_mv_target_table(target_entry, target) {
        Ok(table) => table,
        Err(err) => {
            return Err(handle_iceberg_mv_definite_pre_publish_error(
                state,
                target,
                target_entry,
                staging_branch,
                refresh_id,
                err,
            ));
        }
    };
    let left_snapshot = pin
        .get(left_ref)
        .ok_or_else(|| format!("missing refresh pin for {}", left_ref.fqn()))
        .map_err(|err| {
            handle_iceberg_mv_commit_error(
                state,
                target,
                target_entry,
                staging_branch,
                refresh_id,
                err,
            )
        })?;
    let right_snapshot = pin
        .get(right_ref)
        .ok_or_else(|| format!("missing refresh pin for {}", right_ref.fqn()))
        .map_err(|err| {
            handle_iceberg_mv_commit_error(
                state,
                target,
                target_entry,
                staging_branch,
                refresh_id,
                err,
            )
        })?;
    let mut query = parse_mv_select_query(&mv_definition.select_sql).map_err(|err| {
        handle_iceberg_mv_commit_error(state, target, target_entry, staging_branch, refresh_id, err)
    })?;
    rewrite_join_full_refresh_query(
        &mut query,
        left_ref,
        left_snapshot,
        right_ref,
        right_snapshot,
        &shape.left_alias,
        &shape.right_alias,
    )
    .map_err(|err| {
        handle_iceberg_mv_commit_error(state, target, target_entry, staging_branch, refresh_id, err)
    })?;
    let branch_catalog = build_join_snapshot_catalog(
        state,
        &[(left_ref, left_snapshot), (right_ref, right_snapshot)],
    )
    .map_err(|err| {
        handle_iceberg_mv_commit_error(state, target, target_entry, staging_branch, refresh_id, err)
    })?;
    let coalescer = crate::engine::mv::iceberg_join_coalesce::JoinDeltaCoalescer::new(
        pin.uuid(left_ref)
            .ok_or_else(|| format!("missing uuid for {}", left_ref.fqn()))
            .map_err(|err| {
                handle_iceberg_mv_commit_error(
                    state,
                    target,
                    target_entry,
                    staging_branch,
                    refresh_id,
                    err,
                )
            })?
            .to_string(),
        pin.uuid(right_ref)
            .ok_or_else(|| format!("missing uuid for {}", right_ref.fqn()))
            .map_err(|err| {
                handle_iceberg_mv_commit_error(
                    state,
                    target,
                    target_entry,
                    staging_branch,
                    refresh_id,
                    err,
                )
            })?
            .to_string(),
        1_000_000,
    );
    let sink = crate::engine::mv::iceberg_join_coalesce::IcebergJoinCoalesceSinkFactory::new(
        Arc::clone(&coalescer),
    );
    {
        let catalogs_guard = state
            .iceberg_catalogs
            .read()
            .map_err(|e| format!("iceberg catalog registry read lock: {e}"))?;
        if let Err(err) = crate::engine::execute_query_with_options(
            &query,
            &branch_catalog,
            current_database,
            state.exchange_port,
            None,
            Some(Box::new(sink)),
            Some(&*catalogs_guard),
        ) {
            drop(catalogs_guard);
            return Err(handle_iceberg_mv_commit_error(
                state,
                target,
                target_entry,
                staging_branch,
                refresh_id,
                err,
            ));
        }
    }

    let marker = load_iceberg_mv_refresh_marker(state, refresh_id, mv_definition.mv_id)
        .map_err(|err| {
            handle_iceberg_mv_commit_error(
                state,
                target,
                target_entry,
                staging_branch,
                refresh_id,
                err,
            )
        })?
        .to_summary_properties();
    let ident = iceberg_mv_table_ident(target).map_err(|err| {
        handle_iceberg_mv_commit_error(state, target, target_entry, staging_branch, refresh_id, err)
    })?;
    let collector = new_iceberg_mv_commit_collector(
        &target_table,
        &ident,
        staging_branch,
        CommitOpKind::FastAppend,
    );
    let flush_outcome = coalescer
        .flush_to_iceberg_commit_collector(&target_table, Arc::clone(&collector), None)
        .map_err(|err| {
            handle_iceberg_mv_commit_error(
                state,
                target,
                target_entry,
                staging_branch,
                refresh_id,
                err,
            )
        })?;
    if flush_outcome.added_rows == 0 && flush_outcome.deleted_rows == 0 {
        drop_iceberg_mv_staging_branch(state, target, target_entry, staging_branch)?;
        abort_iceberg_mv_refresh(state, refresh_id)?;
        return Ok(StatementResult::Ok);
    }
    let new_snapshot_id = match data_block_on(commit_iceberg_mv_with_populated_collector(
        &target_table,
        iceberg_catalog,
        target_entry,
        &ident,
        Arc::clone(&collector),
        staging_branch,
        marker,
    )) {
        Ok(Ok(outcome)) => outcome.new_snapshot_id,
        Ok(Err(err)) | Err(err) => {
            return Err(handle_iceberg_mv_commit_error(
                state,
                target,
                target_entry,
                staging_branch,
                refresh_id,
                err,
            ));
        }
    };

    let snapshots = pin.to_snapshot_map();
    let table_uuids = pin.to_table_uuid_map();
    record_iceberg_mv_staging_commit(
        state,
        refresh_id,
        new_snapshot_id,
        flush_outcome.added_rows,
        table_uuids.clone(),
    )?;
    let published_snapshot_id = publish_iceberg_mv_refresh(
        state,
        target,
        target_entry,
        staging_branch,
        expected_main_snapshot_id,
        new_snapshot_id,
        refresh_id,
        mv_definition.mv_id,
    )?;
    record_iceberg_mv_publish_commit(state, refresh_id, published_snapshot_id)?;
    drop_iceberg_mv_staging_branch(state, target, target_entry, staging_branch)?;
    finalize_iceberg_mv_refresh(
        state,
        refresh_id,
        flush_outcome.added_rows,
        snapshots,
        table_uuids,
        published_snapshot_id,
    )?;
    Ok(StatementResult::Ok)
}

fn rewrite_join_full_refresh_query(
    query: &mut sqlparser::ast::Query,
    left_ref: &IcebergTableRef,
    left_snapshot: i64,
    right_ref: &IcebergTableRef,
    right_snapshot: i64,
    left_alias: &str,
    right_alias: &str,
) -> Result<(), String> {
    let sqlparser::ast::SetExpr::Select(select) = query.body.as_mut() else {
        return Err("join full refresh rewrite requires SELECT body".to_string());
    };
    let [from] = select.from.as_mut_slice() else {
        return Err("join full refresh rewrite requires one FROM item".to_string());
    };
    let [join] = from.joins.as_mut_slice() else {
        return Err("join full refresh rewrite requires one JOIN".to_string());
    };
    rewrite_snapshot_table_factor(
        &mut from.relation,
        left_ref,
        left_snapshot,
        Some(left_alias),
    )?;
    rewrite_snapshot_table_factor(
        &mut join.relation,
        right_ref,
        right_snapshot,
        Some(right_alias),
    )?;
    append_join_apply_hidden_projection(select, left_alias, right_alias, true)
}

fn append_join_apply_hidden_projection(
    select: &mut sqlparser::ast::Select,
    left_alias: &str,
    right_alias: &str,
    constant_insert: bool,
) -> Result<(), String> {
    let change_expr = if constant_insert {
        sqlparser::ast::Expr::Cast {
            kind: sqlparser::ast::CastKind::Cast,
            expr: Box::new(sqlparser::ast::Expr::Value(
                sqlparser::ast::Value::Number(
                    crate::exec::change_op::CHANGE_OP_INSERT.to_string(),
                    false,
                )
                .into(),
            )),
            data_type: sqlparser::ast::DataType::TinyInt(None),
            array: false,
            format: None,
        }
    } else {
        return Err("join hidden projection requires a constant insert marker".to_string());
    };
    select
        .projection
        .push(sqlparser::ast::SelectItem::ExprWithAlias {
            expr: change_expr,
            alias: sqlparser::ast::Ident::new(crate::exec::change_op::CHANGE_OP_COLUMN),
        });
    select.projection.push(join_row_id_select_item(
        left_alias,
        crate::engine::mv::iceberg_join_branch::JOIN_LEFT_ROW_ID_COLUMN,
    ));
    select.projection.push(join_row_id_select_item(
        right_alias,
        crate::engine::mv::iceberg_join_branch::JOIN_RIGHT_ROW_ID_COLUMN,
    ));
    Ok(())
}

fn join_row_id_select_item(alias: &str, output: &str) -> sqlparser::ast::SelectItem {
    sqlparser::ast::SelectItem::ExprWithAlias {
        expr: sqlparser::ast::Expr::CompoundIdentifier(vec![
            sqlparser::ast::Ident::new(alias),
            sqlparser::ast::Ident::new("_row_id"),
        ]),
        alias: sqlparser::ast::Ident::new(output),
    }
}

fn rewrite_snapshot_table_factor(
    factor: &mut sqlparser::ast::TableFactor,
    base: &IcebergTableRef,
    snapshot_id: i64,
    default_alias: Option<&str>,
) -> Result<(), String> {
    let sqlparser::ast::TableFactor::Table {
        name,
        version,
        alias,
        args,
        ..
    } = factor
    else {
        return Err("join snapshot side must be a table".to_string());
    };
    if args.is_some() {
        return Err("join snapshot side must be a base table".to_string());
    }
    if !object_name_matches_base(name, base)? {
        return Err(format!(
            "join snapshot rewrite expected base {}, got {}",
            base.fqn(),
            name
        ));
    }
    if let Some(version) = version {
        let rendered = version.to_string();
        if !rendered.contains(&snapshot_id.to_string()) {
            return Err(format!(
                "join snapshot side {} has conflicting version {rendered}",
                base.fqn()
            ));
        }
    }
    *name = synthetic_snapshot_object_name(base, snapshot_id);
    *version = None;
    if alias.is_none()
        && let Some(default_alias) = default_alias
    {
        *alias = Some(sqlparser::ast::TableAlias {
            explicit: true,
            name: sqlparser::ast::Ident::new(default_alias),
            columns: Vec::new(),
        });
    }
    Ok(())
}

fn object_name_matches_base(
    name: &sqlparser::ast::ObjectName,
    base: &IcebergTableRef,
) -> Result<bool, String> {
    let parts = object_name_identifier_parts(name);
    Ok(match parts.as_slice() {
        [table] => table.eq_ignore_ascii_case(&base.table),
        [namespace, table] => {
            namespace.eq_ignore_ascii_case(&base.namespace)
                && table.eq_ignore_ascii_case(&base.table)
        }
        [catalog, namespace, table] => {
            catalog.eq_ignore_ascii_case(&base.catalog)
                && namespace.eq_ignore_ascii_case(&base.namespace)
                && table.eq_ignore_ascii_case(&base.table)
        }
        _ => false,
    })
}

fn object_name_identifier_parts(name: &sqlparser::ast::ObjectName) -> Vec<String> {
    name.0
        .iter()
        .filter_map(|part| match part {
            sqlparser::ast::ObjectNamePart::Identifier(ident) => Some(ident.value.clone()),
            _ => None,
        })
        .collect()
}

fn synthetic_snapshot_table_name(base: &IcebergTableRef, snapshot_id: i64) -> String {
    format!("{}__at_{}", base.table, snapshot_id)
}

fn synthetic_snapshot_object_name(
    base: &IcebergTableRef,
    snapshot_id: i64,
) -> sqlparser::ast::ObjectName {
    sqlparser::ast::ObjectName(vec![
        sqlparser::ast::ObjectNamePart::Identifier(sqlparser::ast::Ident::new(&base.namespace)),
        sqlparser::ast::ObjectNamePart::Identifier(sqlparser::ast::Ident::new(
            synthetic_snapshot_table_name(base, snapshot_id),
        )),
    ])
}

fn build_join_snapshot_catalog(
    state: &Arc<StandaloneState>,
    snapshots: &[(&IcebergTableRef, i64); 2],
) -> Result<crate::engine::catalog::InMemoryCatalog, String> {
    let mut catalog = crate::engine::catalog::InMemoryCatalog::default();
    for (base, snapshot_id) in snapshots {
        register_join_snapshot_side(&mut catalog, state, base, *snapshot_id)?;
    }
    Ok(catalog)
}

fn register_join_snapshot_side(
    catalog: &mut crate::engine::catalog::InMemoryCatalog,
    state: &Arc<StandaloneState>,
    base: &IcebergTableRef,
    snapshot_id: i64,
) -> Result<(), String> {
    catalog.create_database(&base.namespace)?;
    let table_def = build_iceberg_table_def_for_snapshot_scan(state, base, snapshot_id)?;
    catalog
        .register(&base.namespace, table_def)
        .map_err(|e| format!("register join snapshot table {}: {e}", base.fqn()))
}

fn build_iceberg_table_def_for_snapshot_scan(
    state: &Arc<StandaloneState>,
    base: &IcebergTableRef,
    snapshot_id: i64,
) -> Result<crate::sql::catalog::TableDef, String> {
    let entry = {
        let registry = state
            .iceberg_catalogs
            .read()
            .map_err(|e| format!("iceberg catalog registry read lock: {e}"))?;
        registry.get(&base.catalog)?
    };
    let loaded =
        crate::connector::iceberg::catalog::load_table(&entry, &base.namespace, &base.table)?;
    let data_files =
        crate::connector::iceberg::catalog::registry::extract_data_files_with_stats_at(
            &loaded.table,
            snapshot_id,
        )?;
    let synthetic_name = synthetic_snapshot_table_name(base, snapshot_id);
    if data_files.is_empty() {
        let mut table_def =
            crate::connector::iceberg::catalog::build_iceberg_table_def_for_delta_scan(
                &base.namespace,
                &synthetic_name,
                loaded,
            )?;
        table_def
            .iceberg_row_lineage_metadata_columns
            .retain(|column| column.name != crate::exec::change_op::CHANGE_OP_COLUMN);
        return Ok(table_def);
    }
    crate::connector::iceberg::catalog::build_iceberg_table_def_with_files(
        &entry,
        &base.namespace,
        &synthetic_name,
        loaded,
        data_files,
    )
}

#[allow(clippy::too_many_arguments)]
fn incremental_refresh_iceberg_join_mv(
    state: &Arc<StandaloneState>,
    target: &IcebergMvTarget,
    target_entry: &crate::connector::iceberg::catalog::IcebergCatalogEntry,
    iceberg_catalog: &Arc<dyn iceberg::Catalog>,
    expected_main_snapshot_id: Option<i64>,
    current_database: &str,
    mv_definition: &StoredMvDefinition,
    base_refs: &[IcebergTableRef],
    shape: &crate::connector::starrocks::managed::mv_shape::JoinProjectionFilterMvShape,
    pin: &crate::connector::starrocks::managed::refresh_pin::RefreshSnapshotPin,
) -> Result<StatementResult, String> {
    if base_refs.len() != 2 {
        return Err("iceberg join MV refresh requires exactly two base tables".to_string());
    }
    let left_ref = &base_refs[0];
    let right_ref = &base_refs[1];
    let left_to = pin
        .get(left_ref)
        .ok_or_else(|| format!("missing pin for {}", left_ref.fqn()))?;
    let right_to = pin
        .get(right_ref)
        .ok_or_else(|| format!("missing pin for {}", right_ref.fqn()))?;
    let left_from = mv_definition
        .last_refresh_snapshots
        .get(&left_ref.fqn())
        .copied()
        .ok_or_else(|| {
            format!(
                "join MV {} missing previous snapshot for {}",
                mv_definition.mv_id,
                left_ref.fqn()
            )
        })?;
    let right_from = mv_definition
        .last_refresh_snapshots
        .get(&right_ref.fqn())
        .copied()
        .ok_or_else(|| {
            format!(
                "join MV {} missing previous snapshot for {}",
                mv_definition.mv_id,
                right_ref.fqn()
            )
        })?;
    let left_loaded = load_current_iceberg_base_table(state, left_ref)?;
    let right_loaded = load_current_iceberg_base_table(state, right_ref)?;
    let left_batch = plan_changes(&left_loaded.table, left_from, Some(left_to), &[])
        .map_err(|e| format!("join MV left change planning failed: {e}"))?;
    let right_batch = plan_changes(&right_loaded.table, right_from, Some(right_to), &[])
        .map_err(|e| format!("join MV right change planning failed: {e}"))?;
    if left_batch.current_snapshot_id != left_to {
        return Err(format!(
            "join MV left change batch snapshot mismatch: expected {left_to}, got {}",
            left_batch.current_snapshot_id
        ));
    }
    if right_batch.current_snapshot_id != right_to {
        return Err(format!(
            "join MV right change batch snapshot mismatch: expected {right_to}, got {}",
            right_batch.current_snapshot_id
        ));
    }
    let left_has_changes =
        !left_batch.inserts.is_empty() || iceberg_change_batch_has_row_deletes(&left_batch);
    let right_has_changes =
        !right_batch.inserts.is_empty() || iceberg_change_batch_has_row_deletes(&right_batch);
    let branches = crate::engine::mv::iceberg_join_branch::plan_join_delta_branches(
        left_ref,
        right_ref,
        crate::engine::mv::iceberg_join_branch::SnapshotWindow {
            from: left_from,
            to: left_to,
        },
        crate::engine::mv::iceberg_join_branch::SnapshotWindow {
            from: right_from,
            to: right_to,
        },
        left_has_changes,
        right_has_changes,
    );
    if branches.is_empty() {
        return finalize_iceberg_mv_metadata_only_refresh(
            state,
            target,
            mv_definition,
            pin.to_snapshot_map(),
            pin.to_table_uuid_map(),
        );
    }
    execute_join_delta_branches(
        state,
        target,
        target_entry,
        iceberg_catalog,
        expected_main_snapshot_id,
        current_database,
        mv_definition,
        shape,
        pin,
        branches,
    )
}

#[allow(clippy::too_many_arguments)]
fn execute_join_delta_branches(
    state: &Arc<StandaloneState>,
    target: &IcebergMvTarget,
    target_entry: &crate::connector::iceberg::catalog::IcebergCatalogEntry,
    iceberg_catalog: &Arc<dyn iceberg::Catalog>,
    expected_main_snapshot_id: Option<i64>,
    current_database: &str,
    mv_definition: &StoredMvDefinition,
    shape: &crate::connector::starrocks::managed::mv_shape::JoinProjectionFilterMvShape,
    pin: &crate::connector::starrocks::managed::refresh_pin::RefreshSnapshotPin,
    branches: Vec<crate::engine::mv::iceberg_join_branch::JoinDeltaBranchPlan>,
) -> Result<StatementResult, String> {
    let base_query = parse_mv_select_query(&mv_definition.select_sql)?;
    let first_branch = branches
        .first()
        .ok_or_else(|| "join delta branch execution requires at least one branch".to_string())?;
    let left_uuid = pin
        .uuid(&first_branch.left_base)
        .ok_or_else(|| format!("missing uuid for {}", first_branch.left_base.fqn()))?
        .to_string();
    let right_uuid = pin
        .uuid(&first_branch.right_base)
        .ok_or_else(|| format!("missing uuid for {}", first_branch.right_base.fqn()))?
        .to_string();
    let coalescer = crate::engine::mv::iceberg_join_coalesce::JoinDeltaCoalescer::new(
        left_uuid, right_uuid, 1_000_000,
    );
    let staging_branch = format!(
        "__nova_mv_refresh_{}_{}",
        mv_definition.mv_id,
        uuid::Uuid::new_v4().simple()
    );
    let refresh_id = begin_staged_iceberg_mv_refresh_intent(
        state,
        target,
        mv_definition.mv_id,
        expected_main_snapshot_id,
        pin.to_snapshot_map(),
        &staging_branch,
    )?;
    if let Err(err) = ensure_iceberg_mv_staging_branch(
        iceberg_catalog,
        target,
        &staging_branch,
        expected_main_snapshot_id,
    ) {
        abort_iceberg_mv_refresh(state, refresh_id)?;
        return Err(err);
    }
    let target_table = match reload_iceberg_mv_target_table(target_entry, target) {
        Ok(table) => table,
        Err(err) => {
            return Err(handle_iceberg_mv_definite_pre_publish_error(
                state,
                target,
                target_entry,
                &staging_branch,
                refresh_id,
                err,
            ));
        }
    };
    for branch in branches {
        let mut branch_query = crate::engine::mv::iceberg_join_branch::rewrite_join_branch_query(
            &base_query,
            &branch,
            &shape.left_alias,
            &shape.right_alias,
        )
        .map_err(|err| {
            handle_iceberg_mv_commit_error(
                state,
                target,
                target_entry,
                &staging_branch,
                refresh_id,
                err,
            )
        })?;
        normalize_join_branch_snapshot_tables(&mut branch_query, &branch).map_err(|err| {
            handle_iceberg_mv_commit_error(
                state,
                target,
                target_entry,
                &staging_branch,
                refresh_id,
                err,
            )
        })?;
        let branch_catalog = build_join_branch_catalog(state, &branch).map_err(|err| {
            handle_iceberg_mv_commit_error(
                state,
                target,
                target_entry,
                &staging_branch,
                refresh_id,
                err,
            )
        })?;
        let sink = crate::engine::mv::iceberg_join_coalesce::IcebergJoinCoalesceSinkFactory::new(
            Arc::clone(&coalescer),
        );
        let catalogs_guard = state
            .iceberg_catalogs
            .read()
            .map_err(|e| format!("iceberg catalog registry read lock: {e}"))?;
        if let Err(err) = crate::engine::execute_query_with_options(
            &branch_query,
            &branch_catalog,
            current_database,
            state.exchange_port,
            None,
            Some(Box::new(sink)),
            Some(&*catalogs_guard),
        ) {
            drop(catalogs_guard);
            return Err(handle_iceberg_mv_commit_error(
                state,
                target,
                target_entry,
                &staging_branch,
                refresh_id,
                err,
            ));
        }
        drop(catalogs_guard);
    }

    let pending = coalescer.pending_change_counts().map_err(|err| {
        handle_iceberg_mv_commit_error(
            state,
            target,
            target_entry,
            &staging_branch,
            refresh_id,
            err,
        )
    })?;
    if pending.added_rows == 0 && pending.deleted_rows == 0 {
        drop_iceberg_mv_staging_branch(state, target, target_entry, &staging_branch)?;
        abort_iceberg_mv_refresh(state, refresh_id)?;
        return finalize_iceberg_mv_metadata_only_refresh(
            state,
            target,
            mv_definition,
            pin.to_snapshot_map(),
            pin.to_table_uuid_map(),
        );
    }
    let new_total_rows = mv_definition
        .last_refresh_rows
        .unwrap_or(0)
        .checked_add(pending.added_rows)
        .and_then(|rows| rows.checked_sub(pending.deleted_rows))
        .ok_or_else(|| {
            handle_iceberg_mv_commit_error(
                state,
                target,
                target_entry,
                &staging_branch,
                refresh_id,
                format!(
                    "iceberg join MV row-count delta overflow: current={:?}, inserts={}, deletes={}",
                    mv_definition.last_refresh_rows, pending.added_rows, pending.deleted_rows
                ),
            )
        })?;
    let marker = load_iceberg_mv_refresh_marker(state, refresh_id, mv_definition.mv_id)
        .map_err(|err| {
            handle_iceberg_mv_commit_error(
                state,
                target,
                target_entry,
                &staging_branch,
                refresh_id,
                err,
            )
        })?
        .to_summary_properties();

    let ident = iceberg_mv_table_ident(target).map_err(|err| {
        handle_iceberg_mv_commit_error(
            state,
            target,
            target_entry,
            &staging_branch,
            refresh_id,
            err,
        )
    })?;
    let op_kind = if pending.deleted_rows > 0 {
        CommitOpKind::RowDeltaDv
    } else {
        CommitOpKind::FastAppend
    };
    let collector =
        new_iceberg_mv_commit_collector(&target_table, &ident, &staging_branch, op_kind);
    let locator_inputs = if pending.deleted_rows > 0 {
        Some(
            load_target_apply_locator_inputs(target_entry, &target_table).map_err(|err| {
                handle_iceberg_mv_commit_error(
                    state,
                    target,
                    target_entry,
                    &staging_branch,
                    refresh_id,
                    err,
                )
            })?,
        )
    } else {
        None
    };
    let flush_outcome = coalescer
        .flush_to_iceberg_commit_collector(&target_table, Arc::clone(&collector), locator_inputs)
        .map_err(|err| {
            handle_iceberg_mv_commit_error(
                state,
                target,
                target_entry,
                &staging_branch,
                refresh_id,
                err,
            )
        })?;
    if flush_outcome.added_rows == 0 && flush_outcome.deleted_rows == 0 {
        drop_iceberg_mv_staging_branch(state, target, target_entry, &staging_branch)?;
        abort_iceberg_mv_refresh(state, refresh_id)?;
        return finalize_iceberg_mv_metadata_only_refresh(
            state,
            target,
            mv_definition,
            pin.to_snapshot_map(),
            pin.to_table_uuid_map(),
        );
    }

    let commit_outcome = match data_block_on(commit_iceberg_mv_with_populated_collector(
        &target_table,
        iceberg_catalog,
        target_entry,
        &ident,
        Arc::clone(&collector),
        &staging_branch,
        marker,
    )) {
        Ok(Ok(outcome)) => outcome,
        Ok(Err(err)) | Err(err) => {
            return Err(handle_iceberg_mv_commit_error(
                state,
                target,
                target_entry,
                &staging_branch,
                refresh_id,
                err,
            ));
        }
    };

    let snapshots = pin.to_snapshot_map();
    let table_uuids = pin.to_table_uuid_map();
    record_iceberg_mv_staging_commit(
        state,
        refresh_id,
        commit_outcome.new_snapshot_id,
        new_total_rows,
        table_uuids.clone(),
    )?;
    let published_snapshot_id = publish_iceberg_mv_refresh(
        state,
        target,
        target_entry,
        &staging_branch,
        expected_main_snapshot_id,
        commit_outcome.new_snapshot_id,
        refresh_id,
        mv_definition.mv_id,
    )?;
    record_iceberg_mv_publish_commit(state, refresh_id, published_snapshot_id)?;
    drop_iceberg_mv_staging_branch(state, target, target_entry, &staging_branch)?;
    finalize_iceberg_mv_refresh(
        state,
        refresh_id,
        new_total_rows,
        snapshots,
        table_uuids,
        published_snapshot_id,
    )?;
    Ok(StatementResult::Ok)
}

fn build_join_branch_catalog(
    state: &Arc<StandaloneState>,
    branch: &crate::engine::mv::iceberg_join_branch::JoinDeltaBranchPlan,
) -> Result<crate::engine::catalog::InMemoryCatalog, String> {
    let mut catalog = crate::engine::catalog::InMemoryCatalog::default();
    register_join_branch_side(&mut catalog, state, &branch.left_base, branch.left)?;
    register_join_branch_side(&mut catalog, state, &branch.right_base, branch.right)?;
    Ok(catalog)
}

fn register_join_branch_side(
    catalog: &mut crate::engine::catalog::InMemoryCatalog,
    state: &Arc<StandaloneState>,
    base: &IcebergTableRef,
    side: crate::engine::mv::iceberg_join_branch::BranchSide,
) -> Result<(), String> {
    catalog.create_database(&base.namespace)?;
    let table_def = match side {
        crate::engine::mv::iceberg_join_branch::BranchSide::Delta(_) => {
            crate::engine::query_prep::build_iceberg_table_def_for_delta_scan(
                state,
                &base.catalog,
                &base.namespace,
                &base.table,
            )?
        }
        crate::engine::mv::iceberg_join_branch::BranchSide::Snapshot(snapshot_id) => {
            build_iceberg_table_def_for_snapshot_scan(state, base, snapshot_id)?
        }
    };
    catalog
        .register(&base.namespace, table_def)
        .map_err(|e| format!("register join branch table {}: {e}", base.fqn()))
}

fn normalize_join_branch_snapshot_tables(
    query: &mut sqlparser::ast::Query,
    branch: &crate::engine::mv::iceberg_join_branch::JoinDeltaBranchPlan,
) -> Result<(), String> {
    let sqlparser::ast::SetExpr::Select(select) = query.body.as_mut() else {
        return Err("join branch snapshot normalization requires SELECT body".to_string());
    };
    let [from] = select.from.as_mut_slice() else {
        return Err("join branch snapshot normalization requires one FROM item".to_string());
    };
    let [join] = from.joins.as_mut_slice() else {
        return Err("join branch snapshot normalization requires one JOIN".to_string());
    };
    if let crate::engine::mv::iceberg_join_branch::BranchSide::Snapshot(snapshot_id) = branch.left {
        rewrite_snapshot_table_factor(&mut from.relation, &branch.left_base, snapshot_id, None)?;
    }
    if let crate::engine::mv::iceberg_join_branch::BranchSide::Snapshot(snapshot_id) = branch.right
    {
        rewrite_snapshot_table_factor(&mut join.relation, &branch.right_base, snapshot_id, None)?;
    }
    Ok(())
}

/// Execute the incremental refresh of an iceberg-backed MV.
///
/// IVM-A1 path: rewrite the MV SELECT AST so its single base-table reference
/// becomes `__nr_ivm_delta('cat.ns.tbl', from, to)`, register the base table
/// in a one-shot `InMemoryCatalog` via `build_iceberg_table_def_for_delta_scan`,
/// and execute the resulting `Query` through `execute_query_with_options`
/// with a custom `IcebergMergeSinkFactory`. The sink fans inserts to a
/// streaming data-file writer and routes DELETE rows through the A9 target
/// locator, accumulating into a shared `IcebergCommitCollector`. After the
/// pipeline completes, the refresh driver hands the populated collector to
/// `commit_iceberg_mv_with_populated_collector` for the staging-branch commit,
/// then publishes and finalizes.
///
/// Steps:
/// 1. Plan the change batch from `previous_snapshot_id` to `current_snapshot_id`
///    (also used to short-circuit empty-delta finalize).
/// 2. If the delta yields no inserts and no deletes, advance lineage without
///    committing an empty Iceberg snapshot.
/// 3. Otherwise: begin staging branch, build the AST-mutated query, build the
///    collector + merge sink, run `execute_query_with_options`, commit, publish,
///    and finalize.
///
/// Metadata-only empty deltas keep the old finalize path because no Iceberg
/// snapshot is created.
#[allow(clippy::too_many_arguments)]
fn incremental_refresh_iceberg_mv(
    state: &Arc<StandaloneState>,
    ctx: &IcebergMvRefreshContext,
    base_ref: &IcebergTableRef,
    previous_snapshot_id: i64,
    current_snapshot_id: i64,
    base_table: &iceberg::table::Table,
    current_table_uuid: &str,
    pinned_full_select_sql: &str,
) -> Result<StatementResult, String> {
    let target = &ctx.rewrite.target;
    let target_entry = &*ctx.target_entry;
    let iceberg_catalog = &ctx.iceberg_catalog;
    let expected_main_snapshot_id = ctx.rewrite.target_snapshot_id;
    let current_database = ctx.rewrite.current_database.as_str();
    let mv_definition = &*ctx.rewrite.mv_definition;
    // 1. Plan the change batch. If the standard Iceberg diff cannot be planned
    // safely, rebuild instead of risking an incorrect incremental result.
    let batch = match plan_changes(
        base_table,
        previous_snapshot_id,
        Some(current_snapshot_id),
        &[],
    ) {
        Ok(batch) => batch,
        Err(err) => match policy_signal_from_change_error(&err) {
            IcebergChangePolicySignal::FullRefresh { reason } => {
                tracing::info!(
                    "iceberg mv {}.{}.{}: incremental planner requested full refresh: {reason}",
                    target.catalog,
                    target.namespace,
                    target.table
                );
                let staging_branch = format!(
                    "__nova_mv_refresh_{}_{}",
                    mv_definition.mv_id,
                    uuid::Uuid::new_v4().simple()
                );
                let refresh_id = begin_staged_iceberg_mv_refresh_intent(
                    state,
                    target,
                    mv_definition.mv_id,
                    expected_main_snapshot_id,
                    single_snapshot_map(base_ref, current_snapshot_id),
                    &staging_branch,
                )?;
                return rebuild_iceberg_mv(
                    state,
                    target,
                    target_entry,
                    iceberg_catalog,
                    expected_main_snapshot_id,
                    &staging_branch,
                    refresh_id,
                    current_database,
                    mv_definition,
                    pinned_full_select_sql,
                    base_ref,
                    Some(current_snapshot_id),
                    current_table_uuid,
                );
            }
            IcebergChangePolicySignal::Unsupported { reason } => {
                return Err(format!(
                    "iceberg-stored materialized view refresh unsupported: {reason}"
                ));
            }
            IcebergChangePolicySignal::Incremental => {
                return Err(
                    "iceberg-stored materialized view refresh produced invalid incremental policy from change planner"
                        .to_string(),
                );
            }
        },
    };
    if batch.current_snapshot_id != current_snapshot_id {
        return Err(format!(
            "iceberg mv incremental refresh: change batch snapshot mismatch (expected {current_snapshot_id}, got {})",
            batch.current_snapshot_id,
        ));
    }

    let has_delete_changes = !batch.deletes.is_empty()
        || !batch.equality_deletes.is_empty()
        || !batch.deleted_data_files.is_empty();
    let is_empty_delta = batch.inserts.is_empty() && !has_delete_changes;

    // 2. Empty delta: advance lineage without committing an empty Iceberg
    // snapshot. This must run before any staging-branch work.
    if is_empty_delta {
        tracing::info!(
            "iceberg mv {}.{}.{}: incremental refresh delta has 0 rows; \
             advancing lineage to base snapshot {current_snapshot_id} without new iceberg snapshot",
            target.catalog,
            target.namespace,
            target.table
        );
        let snapshots = single_snapshot_map(base_ref, current_snapshot_id);
        let table_uuids = single_table_uuid_map(base_ref, current_table_uuid);
        let target_snapshot_id = recorded_target_snapshot_id(target, mv_definition)?;
        let refresh_id =
            begin_iceberg_mv_refresh_intent(state, mv_definition.mv_id, snapshots.clone())?;
        finalize_iceberg_mv_refresh(
            state,
            refresh_id,
            mv_definition.last_refresh_rows.unwrap_or(0),
            snapshots.clone(),
            table_uuids.clone(),
            target_snapshot_id,
        )?;
        return Ok(StatementResult::Ok);
    }

    // 3. Begin the staging branch and pre-load the target Iceberg table.
    let staging_branch = format!(
        "__nova_mv_refresh_{}_{}",
        mv_definition.mv_id,
        uuid::Uuid::new_v4().simple()
    );
    let refresh_id = begin_staged_iceberg_mv_refresh_intent(
        state,
        target,
        mv_definition.mv_id,
        expected_main_snapshot_id,
        single_snapshot_map(base_ref, current_snapshot_id),
        &staging_branch,
    )?;
    let ident = iceberg_mv_table_ident(target)?;
    let marker = load_iceberg_mv_refresh_marker(state, refresh_id, mv_definition.mv_id)?
        .to_summary_properties();
    if let Err(err) = ensure_iceberg_mv_staging_branch(
        iceberg_catalog,
        target,
        &staging_branch,
        expected_main_snapshot_id,
    ) {
        abort_iceberg_mv_refresh(state, refresh_id)?;
        return Err(err);
    }
    let target_table = match reload_iceberg_mv_target_table(target_entry, target) {
        Ok(table) => table,
        Err(err) => {
            return Err(handle_iceberg_mv_definite_pre_publish_error(
                state,
                target,
                target_entry,
                &staging_branch,
                refresh_id,
                err,
            ));
        }
    };

    // 4. Build the one-shot InMemoryCatalog with the base table registered
    // via the IVM-A1 delta-scan TableDef factory (empty storage + v3
    // row-lineage virtual cols). The analyzer / planner / codegen chain
    // produces an `ICEBERG_DELTA_SCAN_NODE`, which lower_plan turns into
    // `IcebergDeltaScan` using the runtime registry passed below.
    let base_table_def = match crate::engine::query_prep::build_iceberg_table_def_for_delta_scan(
        state,
        &base_ref.catalog,
        &base_ref.namespace,
        &base_ref.table,
    ) {
        Ok(def) => def,
        Err(err) => {
            return Err(handle_iceberg_mv_commit_error(
                state,
                target,
                target_entry,
                &staging_branch,
                refresh_id,
                err,
            ));
        }
    };
    let mut catalog = crate::engine::catalog::InMemoryCatalog::default();
    if let Err(err) = catalog.create_database(&base_ref.namespace) {
        return Err(handle_iceberg_mv_commit_error(
            state,
            target,
            target_entry,
            &staging_branch,
            refresh_id,
            err,
        ));
    }
    if let Err(err) = catalog.register(&base_ref.namespace, base_table_def) {
        return Err(handle_iceberg_mv_commit_error(
            state,
            target,
            target_entry,
            &staging_branch,
            refresh_id,
            format!("register base table for IVM-A1 SELECT: {err}"),
        ));
    }

    // 5. Parse the MV physical SELECT to AST and mutate the unique base-table
    // reference into `__nr_ivm_delta(...)`.
    let physical_sql = iceberg_mv_physical_select_sql(&mv_definition.select_sql)?;
    let normalized = match crate::sql::parser::dialect::normalize_for_raw_parse(&physical_sql) {
        Ok(s) => s,
        Err(err) => {
            return Err(handle_iceberg_mv_commit_error(
                state,
                target,
                target_entry,
                &staging_branch,
                refresh_id,
                err,
            ));
        }
    };
    let statement = match crate::sql::parser::parse_normalized_sql_raw(&normalized) {
        Ok(s) => s,
        Err(err) => {
            return Err(handle_iceberg_mv_commit_error(
                state,
                target,
                target_entry,
                &staging_branch,
                refresh_id,
                format!("sql parser error: {err}"),
            ));
        }
    };
    let sqlparser::ast::Statement::Query(query_box) = statement else {
        return Err(handle_iceberg_mv_commit_error(
            state,
            target,
            target_entry,
            &staging_branch,
            refresh_id,
            "REFRESH MATERIALIZED VIEW stored SQL must be a SELECT query".to_string(),
        ));
    };
    let mut query = *query_box;
    match mutate_query_for_ivm_delta_scan(
        &mut query,
        base_ref,
        previous_snapshot_id,
        current_snapshot_id,
    ) {
        Ok(1) => {}
        Ok(n) => {
            return Err(handle_iceberg_mv_commit_error(
                state,
                target,
                target_entry,
                &staging_branch,
                refresh_id,
                format!(
                    "IVM-A1 AST mutate for MV {}.{}.{} (mv_id={}): expected exactly 1 reference \
                     to base table {}.{}.{} in physical SELECT, found {} (incremental refresh \
                     only supports single-base MVs)",
                    target.catalog,
                    target.namespace,
                    target.table,
                    mv_definition.mv_id,
                    base_ref.catalog,
                    base_ref.namespace,
                    base_ref.table,
                    n,
                ),
            ));
        }
        Err(err) => {
            return Err(handle_iceberg_mv_commit_error(
                state,
                target,
                target_entry,
                &staging_branch,
                refresh_id,
                err,
            ));
        }
    }
    // Drop any leftover catalog-qualified 3-part names (the analyzer's
    // `InMemoryCatalog` view exposes <db>.<table>, not <cat>.<db>.<table>).
    crate::sql::parser::query_refs::strip_catalog_from_three_part_names(&mut query);

    // Append the IVM `__change_op` transparent pseudo-column to the top-level
    // projection. The `IcebergDeltaScan` operator synthesizes per-row values
    // (`+1` for DataFile / `-1` for delete roles); the merge sink reads the
    // column by name to partition each chunk into INSERT and DELETE batches.
    // We append it only on the incremental refresh path because the
    // `build_iceberg_table_def_for_delta_scan` `TableDef` exposes
    // `__change_op` as a row-lineage virtual column; full-rebuild / first
    // refresh use a regular base scan whose `TableDef` does not advertise it,
    // so the same augmentation in `iceberg_mv_physical_select_sql` would
    // fail to resolve `__change_op` there.
    if let Err(err) = append_change_op_to_projection(&mut query) {
        return Err(handle_iceberg_mv_commit_error(
            state,
            target,
            target_entry,
            &staging_branch,
            refresh_id,
            err,
        ));
    }

    // 6. Pre-load the A9 target locator inputs only when the change batch
    // carries DELETE-side rows. The merge sink consumes these when it sees
    // a DELETE chunk; for insert-only batches we leave them None so the
    // sink rejects an unexpected DELETE arrival rather than silently
    // failing.
    let locator_state = if has_delete_changes {
        let inputs = match load_target_apply_locator_inputs(target_entry, &target_table) {
            Ok(v) => v,
            Err(err) => {
                return Err(handle_iceberg_mv_commit_error(
                    state,
                    target,
                    target_entry,
                    &staging_branch,
                    refresh_id,
                    err,
                ));
            }
        };
        let (existing_deletes_by_file, referenced_data_file_partitions) = inputs;
        Some(crate::engine::mv::iceberg_merge_sink::TargetLocatorState {
            existing_deletes_by_file,
            referenced_data_file_partitions,
        })
    } else {
        None
    };

    // 7. Build the shared commit collector + merge sink factory. The sink
    // injects WrittenFile / PositionDeleteGroup descriptors into the
    // collector during pipeline execution; the commit driver below
    // consumes the populated collector.
    let op_kind = if has_delete_changes {
        CommitOpKind::RowDeltaDv
    } else {
        CommitOpKind::FastAppend
    };
    let collector =
        new_iceberg_mv_commit_collector(&target_table, &ident, &staging_branch, op_kind);
    let merge_sink_plan = crate::engine::mv::iceberg_merge_sink::IcebergMergeSinkPlan {
        target_table: target_table.clone(),
        collector: Arc::clone(&collector),
        locator_state,
        apply_key_column: ICEBERG_MV_APPLY_KEY_COLUMN.to_string(),
        apply_key_value_type: crate::engine::mv::iceberg_merge_sink::ApplyKeyValueType::Int64,
    };
    let merge_sink =
        crate::engine::mv::iceberg_merge_sink::IcebergMergeSinkFactory::new(merge_sink_plan);

    // 8. Execute the mutated query with the merge sink as the terminal
    // operator. lower_plan is given the iceberg catalog registry so it
    // can resolve the IcebergRuntimeHandles for the IcebergDeltaScan
    // operator.
    {
        let catalogs_guard = state
            .iceberg_catalogs
            .read()
            .map_err(|e| format!("iceberg catalog registry read lock: {e}"))?;
        if let Err(err) = crate::engine::execute_query_with_options(
            &query,
            &catalog,
            current_database,
            state.exchange_port,
            None,
            Some(Box::new(merge_sink)),
            Some(&*catalogs_guard),
        ) {
            drop(catalogs_guard);
            return Err(handle_iceberg_mv_commit_error(
                state,
                target,
                target_entry,
                &staging_branch,
                refresh_id,
                err,
            ));
        }
        drop(catalogs_guard);
    }

    let added_rows = collector.injected_data_record_count();
    let deleted_rows = collector.injected_delete_record_count();

    // 8b. Post-execution empty-delta short-circuit.
    //
    // The file-level `is_empty_delta` check earlier in this function only
    // catches snapshot ranges that produced no inserts and no deletes at all.
    // A snapshot range that inserted rows the MV's WHERE / PROJECT removes
    // (e.g. WHERE id > 10 with an inserted row of id=1) still appears
    // non-empty at the file level, so we enter the staging-branch path. Once
    // execution finishes the merge sink reports zero contributed rows; in
    // that case there is no Iceberg data to commit, and committing an empty
    // snapshot on the staging branch is both wasteful and confuses
    // downstream consumers that diff main vs the staging branch.
    //
    // Recovery: drop the staging branch (so the next refresh starts clean),
    // abort the staging-branch refresh intent, open a fresh metadata-only
    // refresh intent (no target / staging-branch fields), and finalize it
    // with the new base snapshot id. This mirrors the file-level empty-delta
    // short-circuit semantics: lineage advances without producing a new
    // Iceberg snapshot.
    if added_rows == 0 && deleted_rows == 0 {
        tracing::info!(
            "iceberg mv {}.{}.{}: incremental refresh produced 0 effective rows after SELECT \
             evaluation; advancing lineage to base snapshot {current_snapshot_id} without new \
             iceberg snapshot",
            target.catalog,
            target.namespace,
            target.table
        );
        drop_iceberg_mv_staging_branch(state, target, target_entry, &staging_branch)?;
        abort_iceberg_mv_refresh(state, refresh_id)?;
        let snapshots = single_snapshot_map(base_ref, current_snapshot_id);
        let table_uuids = single_table_uuid_map(base_ref, current_table_uuid);
        let target_snapshot_id = recorded_target_snapshot_id(target, mv_definition)?;
        let metadata_refresh_id =
            begin_iceberg_mv_refresh_intent(state, mv_definition.mv_id, snapshots.clone())?;
        finalize_iceberg_mv_refresh(
            state,
            metadata_refresh_id,
            mv_definition.last_refresh_rows.unwrap_or(0),
            snapshots,
            table_uuids,
            target_snapshot_id,
        )?;
        return Ok(StatementResult::Ok);
    }

    // 9. Drive the commit from the populated collector.
    let new_snapshot_id = match data_block_on(commit_iceberg_mv_with_populated_collector(
        &target_table,
        iceberg_catalog,
        target_entry,
        &ident,
        Arc::clone(&collector),
        &staging_branch,
        marker,
    )) {
        Ok(Ok(outcome)) => outcome.new_snapshot_id,
        Ok(Err(err)) | Err(err) => {
            return Err(handle_iceberg_mv_commit_error(
                state,
                target,
                target_entry,
                &staging_branch,
                refresh_id,
                err,
            ));
        }
    };

    let new_total_rows = mv_definition
        .last_refresh_rows
        .unwrap_or(0)
        .checked_add(added_rows)
        .and_then(|rows| rows.checked_sub(deleted_rows))
        .ok_or_else(|| {
            format!(
                "iceberg MV row-count delta overflow: current={:?}, inserts={added_rows}, deletes={deleted_rows}",
                mv_definition.last_refresh_rows
            )
        })?;
    let snapshots = single_snapshot_map(base_ref, current_snapshot_id);
    let table_uuids = single_table_uuid_map(base_ref, current_table_uuid);
    record_iceberg_mv_staging_commit(
        state,
        refresh_id,
        new_snapshot_id,
        new_total_rows,
        table_uuids.clone(),
    )?;
    let published_snapshot_id = publish_iceberg_mv_refresh(
        state,
        target,
        target_entry,
        &staging_branch,
        expected_main_snapshot_id,
        new_snapshot_id,
        refresh_id,
        mv_definition.mv_id,
    )?;
    record_iceberg_mv_publish_commit(state, refresh_id, published_snapshot_id)?;
    drop_iceberg_mv_staging_branch(state, target, target_entry, &staging_branch)?;
    finalize_iceberg_mv_refresh(
        state,
        refresh_id,
        new_total_rows,
        snapshots.clone(),
        table_uuids.clone(),
        published_snapshot_id,
    )?;

    tracing::info!(
        "iceberg mv {}.{}.{}: incremental refresh complete: \
         added_rows={added_rows} deleted_rows={deleted_rows} total_rows={new_total_rows} iceberg_snapshot={published_snapshot_id}",
        target.catalog,
        target.namespace,
        target.table
    );
    Ok(StatementResult::Ok)
}

/// Write `chunks` into the given iceberg table as Parquet data files.
///
/// Returns the list of written `DataFile` descriptors. If `chunks` is empty
/// or all chunks contain zero rows, returns an empty vec.
///
/// The RecordBatches are re-cast to an Arrow schema annotated with the
/// Iceberg field ids that the `ParquetWriterBuilder` requires (it matches
/// columns by field-id metadata by default).
pub(crate) async fn write_chunks_as_iceberg_data_files(
    table: &iceberg::table::Table,
    chunks: &[crate::exec::chunk::Chunk],
) -> Result<Vec<iceberg::spec::DataFile>, String> {
    write_record_batches_as_data_files(table, chunks.iter().map(|chunk| chunk.batch.clone())).await
}

pub(crate) fn drop_iceberg_mv(
    state: &Arc<StandaloneState>,
    current_catalog: Option<&str>,
    current_database: &str,
    stmt: &DropMaterializedViewStmt,
) -> Result<StatementResult, String> {
    let _refresh_guard = acquire_mv_refresh_lock()?;
    let target = resolve_drop_target(current_catalog, current_database, &stmt.name)?;
    if !preflight_iceberg_mv_drop(state, &target, stmt.if_exists)? {
        return Ok(StatementResult::Ok);
    }

    let entry = {
        let catalogs = state
            .iceberg_catalogs
            .read()
            .map_err(|e| format!("iceberg catalog registry read lock: {e}"))?;
        catalogs.get(&target.catalog)?
    };
    crate::connector::iceberg::catalog::registry::drop_table(
        &entry,
        &target.namespace,
        &target.table,
    )?;
    crate::engine::delete_iceberg_table_if_needed(
        state,
        &target.catalog,
        &target.namespace,
        &target.table,
    )?;
    crate::engine::query_prep::drop_registered_external_table(
        state,
        &target.namespace,
        &target.table,
    )?;

    tracing::info!(
        "iceberg mv {}.{}.{}: dropped successfully",
        target.catalog,
        target.namespace,
        target.table
    );
    Ok(StatementResult::Ok)
}

fn preflight_iceberg_mv_drop(
    state: &Arc<StandaloneState>,
    target: &IcebergMvTarget,
    if_exists: bool,
) -> Result<bool, String> {
    let provider = state
        .metadata_provider
        .as_ref()
        .ok_or_else(|| "metadata provider required for iceberg mv drop".to_string())?;
    let txn = provider
        .begin_read()
        .map_err(|e| format!("open iceberg mv drop preflight transaction failed: {e}"))?;
    let Some(definition) = state
        .mv_repo
        .find_by_target(
            txn.as_ref(),
            &target.catalog,
            &target.namespace,
            &target.table,
        )
        .map_err(|e| format!("load iceberg mv definition for drop failed: {e}"))?
    else {
        if if_exists {
            return Ok(false);
        }
        return Err(format!(
            "materialized view does not exist: {}.{}.{}",
            target.catalog, target.namespace, target.table
        ));
    };
    if definition.refresh_in_progress || definition.active_refresh_id.is_some() {
        return Err(format!(
            "cannot drop materialized view {}.{}.{}: refresh in progress",
            target.catalog, target.namespace, target.table
        ));
    }
    crate::engine::mv::dependency::ensure_no_downstream_dependencies(
        state,
        &crate::engine::mv::dependency::iceberg_mv_dependency_ref(
            &target.catalog,
            &target.namespace,
            &target.table,
        ),
    )?;
    Ok(true)
}

fn resolve_drop_target(
    current_catalog: Option<&str>,
    current_database: &str,
    name: &ObjectName,
) -> Result<IcebergMvTarget, String> {
    let catalog = current_catalog.ok_or_else(|| {
        "DROP MATERIALIZED VIEW for an Iceberg MV requires current Iceberg catalog context"
            .to_string()
    })?;
    let (namespace, table) = resolve_mv_name(name, current_database)?;
    Ok(IcebergMvTarget {
        catalog: crate::engine::catalog::normalize_identifier(catalog)?,
        namespace,
        table,
    })
}

/// Build an Iceberg `Schema` from the MV's analyzed output columns.
/// Each column is mapped to a primitive Iceberg type; nullable columns become
/// optional fields, non-nullable columns become required fields.
#[cfg(test)]
fn build_iceberg_schema_from_outputs(output_columns: &[OutputColumn]) -> Result<Schema, String> {
    let mut fields = Vec::with_capacity(output_columns.len());
    for (idx, col) in output_columns.iter().enumerate() {
        let id = (idx + 1) as i32;
        let primitive = arrow_data_type_to_iceberg_primitive(&col.data_type)?;
        let field: Arc<NestedField> = if col.nullable {
            NestedField::optional(id, &col.name, Type::Primitive(primitive)).into()
        } else {
            NestedField::required(id, &col.name, Type::Primitive(primitive)).into()
        };
        fields.push(field);
    }
    Schema::builder()
        .with_fields(fields)
        .build()
        .map_err(|e| format!("build iceberg mv schema failed: {e}"))
}

/// Map an Arrow `DataType` to an Iceberg `PrimitiveType`. Returns an error
/// for types that cannot be represented as Iceberg primitive columns.
#[cfg(test)]
fn arrow_data_type_to_iceberg_primitive(
    arrow_type: &arrow::datatypes::DataType,
) -> Result<PrimitiveType, String> {
    use arrow::datatypes::{DataType, TimeUnit};
    Ok(match arrow_type {
        DataType::Boolean => PrimitiveType::Boolean,
        // Promote narrow integer types — Iceberg has no Int8/Int16 primitive.
        DataType::Int8 | DataType::Int16 => PrimitiveType::Int,
        DataType::Int32 => PrimitiveType::Int,
        DataType::Int64 => PrimitiveType::Long,
        DataType::Float32 => PrimitiveType::Float,
        DataType::Float64 => PrimitiveType::Double,
        DataType::Date32 => PrimitiveType::Date,
        DataType::Timestamp(TimeUnit::Microsecond, _) => PrimitiveType::Timestamp,
        DataType::Utf8 | DataType::LargeUtf8 => PrimitiveType::String,
        DataType::Binary | DataType::LargeBinary => PrimitiveType::Binary,
        DataType::Decimal128(precision, scale) => {
            let scale_u32 = u32::try_from(*scale).map_err(|_| {
                format!("iceberg-backed mv: Decimal128 negative scale {scale} is not supported")
            })?;
            PrimitiveType::Decimal {
                precision: *precision as u32,
                scale: scale_u32,
            }
        }
        DataType::Decimal256(_, _) => {
            return Err(
                "iceberg-backed mv: Decimal256 (precision > 38) is not supported by Iceberg; \
                 use DECIMAL with precision <= 38"
                    .to_string(),
            );
        }
        DataType::FixedSizeBinary(16) => {
            return Err(
                "iceberg-backed mv: LARGEINT (FixedSizeBinary(16)) is not supported in \
                 iceberg-backed MV; use BIGINT or DECIMAL"
                    .to_string(),
            );
        }
        other => {
            return Err(format!(
                "iceberg-backed mv: unsupported column type `{other:?}`"
            ));
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Int32Array, Int64Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema as ArrowSchema};
    use arrow::record_batch::RecordBatch;
    use std::sync::Arc as StdArc;
    use tempfile::TempDir;

    #[test]
    fn join_base_schema_contract_returns_rebind_for_rename() {
        let ty = iceberg::spec::Type::Primitive(iceberg::spec::PrimitiveType::Long);
        let base_contract = crate::meta::repository::mv_contract::BaseContract {
            table_fqn: "ice.db.fact".to_string(),
            table_uuid: "uuid".to_string(),
            alias_at_create: Some("f".to_string()),
            schema_id_at_create: 1,
            schema_at_create: crate::meta::repository::mv_contract::BaseSchemaSnapshot {
                fields: vec![crate::meta::repository::mv_contract::BaseFieldRecord {
                    field_id: 2,
                    name_at_create: "dim_id".to_string(),
                    type_signature: format!("{ty}"),
                    required: false,
                }],
            },
        };
        let current_schema = iceberg::spec::Schema::builder()
            .with_schema_id(2)
            .with_fields(vec![std::sync::Arc::new(
                iceberg::spec::NestedField::optional(2, "new_dim_id", ty),
            )])
            .build()
            .expect("schema");

        let rebound = validate_join_base_schema_contract_for_rebind(
            "ice.db.fact",
            &base_contract,
            &current_schema,
        )
        .expect("compatible");

        assert_eq!(rebound.len(), 1);
        assert_eq!(rebound[0].base_table_fqn, "ice.db.fact");
        assert_eq!(rebound[0].name_at_create, "dim_id");
        assert_eq!(rebound[0].current_name, "new_dim_id");
    }

    fn parse_select_query(sql: &str) -> sqlparser::ast::Query {
        let normalized =
            crate::sql::parser::dialect::normalize_for_raw_parse(sql).expect("normalize");
        let stmt = crate::sql::parser::parse_normalized_sql_raw(&normalized).expect("parse");
        let sqlparser::ast::Statement::Query(q) = stmt else {
            panic!("expected SELECT");
        };
        *q
    }

    #[test]
    fn iceberg_join_mv_uses_join_apply_key_column() {
        let column = crate::engine::mv::iceberg_target_apply::join_apply_key_table_column();
        assert_eq!(
            column.name,
            crate::engine::mv::iceberg_target_apply::ICEBERG_MV_JOIN_APPLY_KEY_COLUMN
        );
    }

    #[test]
    fn refresh_dispatch_identifies_join_shape() {
        let query =
            parse_select_query("select l.id from ice.ns.left l join ice.ns.right r on l.id = r.id");
        let shape = classify_incremental_mv_query(&query).expect("shape");
        assert!(is_join_projection_filter_mv(&shape));
    }

    #[test]
    fn iceberg_aggregate_target_columns_use_state_layout() {
        let query = parse_select_query(
            "select region, count(*) as c, sum(amount) as s \
             from ice.ns.fact group by region",
        );
        let shape = match classify_incremental_mv_query(&query).expect("shape") {
            IncrementalMvShape::Aggregate(shape) => shape,
            other => panic!("expected aggregate shape, got {other:?}"),
        };
        let output_columns = vec![
            output_col("region", arrow::datatypes::DataType::Utf8, true),
            output_col("c", arrow::datatypes::DataType::Int64, false),
            output_col("s", arrow::datatypes::DataType::Int64, true),
        ];

        let columns = iceberg_aggregate_target_columns(&shape, &output_columns).expect("columns");
        let names = columns.iter().map(|c| c.name.as_str()).collect::<Vec<_>>();

        assert_eq!(
            names,
            vec![
                "__row_id__",
                "region",
                "c",
                "s",
                "__agg_state_c",
                "__agg_state_s"
            ]
        );
    }

    #[test]
    fn aggregate_first_refresh_uses_state_shaped_select() {
        let sql = "select region, avg(amount) as a \
                   from ice.ns.fact group by region";
        let query = parse_select_query(sql);
        let shape = match classify_incremental_mv_query(&query).expect("shape") {
            IncrementalMvShape::Aggregate(shape) => shape,
            other => panic!("expected aggregate shape, got {other:?}"),
        };

        let state_sql = iceberg_aggregate_first_refresh_select_sql(sql, &shape).expect("rewrite");
        let upper = state_sql.to_uppercase();

        assert!(
            upper.contains("SUM(AMOUNT) AS __AGG_STATE_A__SUM"),
            "sql={state_sql}"
        );
        assert!(
            upper.contains("COUNT(AMOUNT) AS __AGG_STATE_A__COUNT"),
            "sql={state_sql}"
        );
        assert!(!upper.contains("__ROW_ID__"), "sql={state_sql}");
    }

    #[test]
    fn aggregate_incremental_rewrite_uses_signed_state() {
        let sql = "select region, count(*) as c, sum(amount) as s \
                   from ice.ns.fact group by region";
        let query = parse_select_query(sql);
        let shape = match classify_incremental_mv_query(&query).expect("shape") {
            IncrementalMvShape::Aggregate(shape) => shape,
            other => panic!("expected aggregate shape, got {other:?}"),
        };

        let rewritten =
            iceberg_aggregate_incremental_delta_select_sql(sql, &shape, None).expect("rewrite");
        let upper = rewritten.to_uppercase();

        assert!(upper.contains("SUM(__CHANGE_OP) AS C"), "sql={rewritten}");
        assert!(
            upper.contains("SUM(AMOUNT * __CHANGE_OP) AS S"),
            "sql={rewritten}"
        );
    }

    #[test]
    fn join_aggregate_branch_rewrite_uses_delta_side_change_op() {
        let sql = "select d.region, count(*) as c, sum(f.amount) as s \
                   from ice.ns.fact f join ice.ns.dim d on f.dim_id = d.id \
                   group by d.region";
        let query = parse_select_query(sql);
        let shape = match classify_incremental_mv_query(&query).expect("shape") {
            IncrementalMvShape::JoinAggregate(shape) => shape,
            other => panic!("expected join aggregate shape, got {other:?}"),
        };

        let branch_sql = iceberg_join_aggregate_branch_delta_sql(
            sql,
            &shape,
            crate::engine::mv::iceberg_join_branch::BranchDeltaSide::Left,
        )
        .expect("branch rewrite");
        let upper = branch_sql.to_uppercase();

        assert!(
            upper.contains("SUM(F.__CHANGE_OP) AS C"),
            "sql={branch_sql}"
        );
        assert!(!upper.contains("SUM(__CHANGE_OP) AS C"), "sql={branch_sql}");
    }

    #[test]
    fn aggregate_first_refresh_writes_state_and_refreshes_incrementally() {
        let env = open_test_state_with_hadoop_iceberg_catalog("ice", "analytics");
        create_aggregate_fact_table(&env.state, "ice", "sales", "fact");
        insert_into_aggregate_fact_table(
            &env.state,
            "ice",
            "sales",
            "fact",
            &[(1, "east", 10), (2, "west", 7), (3, "east", 5)],
        );
        let stmt = parse_create_mv(
            "CREATE MATERIALIZED VIEW mv_fact_region
             DISTRIBUTED BY HASH(region) BUCKETS 1
             PROPERTIES('storage_engine'='iceberg')
             AS SELECT region, count(*) AS c, sum(amount) AS s
                FROM ice.sales.fact
                GROUP BY region",
        );
        create_iceberg_mv(&env.state, Some("ice"), &env.current_db, &stmt)
            .expect("create aggregate iceberg mv");

        let refresh = parse_refresh_mv("REFRESH MATERIALIZED VIEW mv_fact_region");
        refresh_iceberg_mv(&env.state, Some("ice"), &env.current_db, &refresh)
            .expect("first aggregate refresh");

        let mv = find_iceberg_mv_definition(&env.state, "ice", "analytics", "mv_fact_region")
            .expect("mv definition after aggregate refresh");
        assert_eq!(mv.last_refresh_rows, Some(2));
        assert_eq!(mv.last_refresh_snapshots.len(), 1);
        assert_eq!(mv.last_refresh_table_uuids.len(), 1);
        let entry = {
            let catalogs = env.state.iceberg_catalogs.read().expect("iceberg catalogs");
            catalogs.get("ice").expect("catalog")
        };
        let loaded =
            crate::connector::iceberg::catalog::load_table(&entry, "analytics", "mv_fact_region")
                .expect("load aggregate mv target");
        let fields = loaded
            .table
            .metadata()
            .current_schema()
            .as_struct()
            .fields()
            .iter()
            .map(|field| field.name.as_str())
            .collect::<Vec<_>>();
        assert!(fields.contains(&"__row_id__"), "fields={fields:?}");
        assert!(fields.contains(&"__agg_state_c"), "fields={fields:?}");
        assert!(fields.contains(&"__agg_state_s"), "fields={fields:?}");
        assert!(loaded.table.metadata().current_snapshot().is_some());

        let first_snapshot = loaded
            .table
            .metadata()
            .current_snapshot()
            .expect("first aggregate snapshot")
            .snapshot_id();

        refresh_iceberg_mv(&env.state, Some("ice"), &env.current_db, &refresh)
            .expect("unchanged aggregate refresh should be metadata-only");
        insert_into_aggregate_fact_table(&env.state, "ice", "sales", "fact", &[(4, "north", 3)]);
        let plan = plan_iceberg_mv_refresh(
            &env.state,
            Some("ice"),
            &env.current_db,
            &refresh,
            MvTarget {
                catalog: Some("ice".to_string()),
                database: "analytics".to_string(),
                name: "mv_fact_region".to_string(),
            },
        )
        .expect("aggregate incremental refresh plan");
        assert_eq!(plan.mode, RefreshMode::Incremental);
        refresh_iceberg_mv(&env.state, Some("ice"), &env.current_db, &refresh)
            .expect("aggregate incremental refresh");

        let mv = find_iceberg_mv_definition(&env.state, "ice", "analytics", "mv_fact_region")
            .expect("mv definition after aggregate incremental refresh");
        assert_eq!(mv.last_refresh_rows, Some(3));
        let loaded =
            crate::connector::iceberg::catalog::load_table(&entry, "analytics", "mv_fact_region")
                .expect("reload aggregate mv target");
        let second_snapshot = loaded
            .table
            .metadata()
            .current_snapshot()
            .expect("second aggregate snapshot")
            .snapshot_id();
        assert_ne!(first_snapshot, second_snapshot);
    }

    #[test]
    fn join_aggregate_first_refresh_writes_state_for_two_bases() {
        let env = open_test_state_with_hadoop_iceberg_catalog("ice", "analytics");
        create_aggregate_fact_table(&env.state, "ice", "sales", "fact");
        create_aggregate_dim_table(&env.state, "ice", "sales", "dim");
        insert_into_aggregate_fact_table(
            &env.state,
            "ice",
            "sales",
            "fact",
            &[(1, "unused", 10), (2, "unused", 7), (3, "unused", 5)],
        );
        insert_into_aggregate_dim_table(
            &env.state,
            "ice",
            "sales",
            "dim",
            &[(1, "east"), (2, "west"), (3, "east")],
        );
        let stmt = parse_create_mv(
            "CREATE MATERIALIZED VIEW mv_fact_dim_region
             DISTRIBUTED BY HASH(category) BUCKETS 1
             PROPERTIES('storage_engine'='iceberg')
             AS SELECT d.category, count(*) AS c, sum(f.amount) AS s
                FROM ice.sales.fact f JOIN ice.sales.dim d ON f.id = d.id
                GROUP BY d.category",
        );
        create_iceberg_mv(&env.state, Some("ice"), &env.current_db, &stmt)
            .expect("create join aggregate iceberg mv");

        let refresh = parse_refresh_mv("REFRESH MATERIALIZED VIEW mv_fact_dim_region");
        refresh_iceberg_mv(&env.state, Some("ice"), &env.current_db, &refresh)
            .expect("first join aggregate refresh");

        let mv = find_iceberg_mv_definition(&env.state, "ice", "analytics", "mv_fact_dim_region")
            .expect("mv definition after join aggregate refresh");
        assert_eq!(mv.last_refresh_rows, Some(2));
        assert_eq!(mv.last_refresh_snapshots.len(), 2);
        assert_eq!(mv.last_refresh_table_uuids.len(), 2);
        assert!(mv.last_refresh_snapshots.contains_key("ice.sales.fact"));
        assert!(mv.last_refresh_snapshots.contains_key("ice.sales.dim"));

        let entry = {
            let catalogs = env.state.iceberg_catalogs.read().expect("iceberg catalogs");
            catalogs.get("ice").expect("catalog")
        };
        let loaded = crate::connector::iceberg::catalog::load_table(
            &entry,
            "analytics",
            "mv_fact_dim_region",
        )
        .expect("load join aggregate mv target");
        let fields = loaded
            .table
            .metadata()
            .current_schema()
            .as_struct()
            .fields()
            .iter()
            .map(|field| field.name.as_str())
            .collect::<Vec<_>>();
        assert!(fields.contains(&"__row_id__"), "fields={fields:?}");
        assert!(fields.contains(&"__agg_state_c"), "fields={fields:?}");
        assert!(fields.contains(&"__agg_state_s"), "fields={fields:?}");
        assert!(loaded.table.metadata().current_snapshot().is_some());
        let first_snapshot = loaded
            .table
            .metadata()
            .current_snapshot()
            .expect("first join aggregate snapshot")
            .snapshot_id();

        refresh_iceberg_mv(&env.state, Some("ice"), &env.current_db, &refresh)
            .expect("unchanged join aggregate refresh should be metadata-only");
        insert_into_aggregate_fact_table(&env.state, "ice", "sales", "fact", &[(4, "unused", 3)]);
        insert_into_aggregate_dim_table(&env.state, "ice", "sales", "dim", &[(4, "north")]);
        let plan = plan_iceberg_mv_refresh(
            &env.state,
            Some("ice"),
            &env.current_db,
            &refresh,
            MvTarget {
                catalog: Some("ice".to_string()),
                database: "analytics".to_string(),
                name: "mv_fact_dim_region".to_string(),
            },
        )
        .expect("join aggregate incremental refresh plan");
        assert_eq!(plan.mode, RefreshMode::Incremental);
        refresh_iceberg_mv(&env.state, Some("ice"), &env.current_db, &refresh)
            .expect("join aggregate incremental refresh");

        let mv = find_iceberg_mv_definition(&env.state, "ice", "analytics", "mv_fact_dim_region")
            .expect("mv definition after join aggregate incremental refresh");
        assert_eq!(mv.last_refresh_rows, Some(3));
        let loaded = crate::connector::iceberg::catalog::load_table(
            &entry,
            "analytics",
            "mv_fact_dim_region",
        )
        .expect("reload join aggregate mv target");
        let second_snapshot = loaded
            .table
            .metadata()
            .current_snapshot()
            .expect("second join aggregate snapshot")
            .snapshot_id();
        assert_ne!(first_snapshot, second_snapshot);
        assert_join_aggregate_rows(
            &env.state,
            "ice",
            &env.current_db,
            "mv_fact_dim_region",
            &[("east", 2, 15), ("north", 1, 3), ("west", 1, 7)],
        );

        execute_iceberg_sql(
            &env.state,
            Some("ice"),
            &env.current_db,
            "UPDATE ice.sales.fact SET amount = 20 WHERE id = 1",
        );
        refresh_iceberg_mv(&env.state, Some("ice"), &env.current_db, &refresh)
            .expect("join aggregate left update refresh");
        assert_join_aggregate_rows(
            &env.state,
            "ice",
            &env.current_db,
            "mv_fact_dim_region",
            &[("east", 2, 25), ("north", 1, 3), ("west", 1, 7)],
        );

        execute_iceberg_sql(
            &env.state,
            Some("ice"),
            &env.current_db,
            "UPDATE ice.sales.dim SET category = 'east' WHERE id = 2",
        );
        refresh_iceberg_mv(&env.state, Some("ice"), &env.current_db, &refresh)
            .expect("join aggregate right update refresh");
        assert_join_aggregate_rows(
            &env.state,
            "ice",
            &env.current_db,
            "mv_fact_dim_region",
            &[("east", 3, 32), ("north", 1, 3)],
        );

        execute_iceberg_sql(
            &env.state,
            Some("ice"),
            &env.current_db,
            "DELETE FROM ice.sales.fact WHERE id = 3",
        );
        refresh_iceberg_mv(&env.state, Some("ice"), &env.current_db, &refresh)
            .expect("join aggregate left delete refresh");
        assert_join_aggregate_rows(
            &env.state,
            "ice",
            &env.current_db,
            "mv_fact_dim_region",
            &[("east", 2, 27), ("north", 1, 3)],
        );

        execute_iceberg_sql(
            &env.state,
            Some("ice"),
            &env.current_db,
            "DELETE FROM ice.sales.dim WHERE id = 4",
        );
        refresh_iceberg_mv(&env.state, Some("ice"), &env.current_db, &refresh)
            .expect("join aggregate right delete refresh");
        assert_join_aggregate_rows(
            &env.state,
            "ice",
            &env.current_db,
            "mv_fact_dim_region",
            &[("east", 2, 27)],
        );
    }

    fn execute_iceberg_sql(
        state: &Arc<StandaloneState>,
        current_catalog: Option<&str>,
        current_database: &str,
        sql: &str,
    ) {
        let session = crate::engine::StandaloneSession {
            inner: Arc::clone(state),
        };
        match session
            .execute_in_context(sql, current_catalog, current_database, None)
            .expect("execute iceberg sql")
        {
            StatementResult::Ok => {}
            StatementResult::Query(_) => panic!("expected non-query statement for {sql}"),
        }
    }

    fn assert_join_aggregate_rows(
        state: &Arc<StandaloneState>,
        current_catalog: &str,
        current_database: &str,
        mv_name: &str,
        expected: &[(&str, i64, i64)],
    ) {
        let sql = format!("SELECT category, c, s FROM {mv_name} ORDER BY category");
        let session = crate::engine::StandaloneSession {
            inner: Arc::clone(state),
        };
        let result = match session
            .execute_in_context(&sql, Some(current_catalog), current_database, None)
            .expect("query join aggregate mv")
        {
            StatementResult::Query(result) => result,
            StatementResult::Ok => panic!("expected query result for {sql}"),
        };
        let actual = string_i64_i64_rows(&result);
        let expected = expected
            .iter()
            .map(|(category, count, sum)| ((*category).to_string(), *count, *sum))
            .collect::<Vec<_>>();
        assert_eq!(actual, expected);
    }

    fn string_i64_i64_rows(
        result: &crate::runtime::query_result::QueryResult,
    ) -> Vec<(String, i64, i64)> {
        let mut rows = Vec::new();
        for chunk in &result.chunks {
            let category = chunk
                .batch
                .column(0)
                .as_any()
                .downcast_ref::<StringArray>()
                .expect("category column");
            let count = chunk
                .batch
                .column(1)
                .as_any()
                .downcast_ref::<Int64Array>()
                .expect("count column");
            let sum = chunk
                .batch
                .column(2)
                .as_any()
                .downcast_ref::<Int64Array>()
                .expect("sum column");
            for row in 0..chunk.batch.num_rows() {
                rows.push((
                    category.value(row).to_string(),
                    count.value(row),
                    sum.value(row),
                ));
            }
        }
        rows
    }

    fn test_base_ref() -> IcebergTableRef {
        IcebergTableRef {
            catalog: "ice".to_string(),
            namespace: "db".to_string(),
            table: "orders".to_string(),
        }
    }

    #[test]
    fn mutate_query_for_ivm_delta_scan_replaces_three_part_ref() {
        let mut query = parse_select_query("SELECT * FROM ice.db.orders");
        let matches = mutate_query_for_ivm_delta_scan(&mut query, &test_base_ref(), 100, 200)
            .expect("mutate must succeed");
        assert_eq!(matches, 1);
        let rendered = query.to_string();
        assert!(
            rendered.contains("__nr_ivm_delta('ice.db.orders', 100, 200)"),
            "unexpected rendered query: {rendered}"
        );
        // Default alias falls back to the base-table name so projection scopes resolve.
        assert!(
            rendered.contains("AS orders") || rendered.contains("orders"),
            "expected alias preserved in: {rendered}"
        );
    }

    #[test]
    fn mutate_query_for_ivm_delta_scan_replaces_two_part_ref() {
        let mut query = parse_select_query("SELECT * FROM db.orders");
        let matches = mutate_query_for_ivm_delta_scan(&mut query, &test_base_ref(), 100, 200)
            .expect("mutate must succeed");
        assert_eq!(matches, 1);
        let rendered = query.to_string();
        assert!(
            rendered.contains("__nr_ivm_delta('ice.db.orders', 100, 200)"),
            "unexpected rendered query: {rendered}"
        );
    }

    #[test]
    fn mutate_query_for_ivm_delta_scan_replaces_bare_table_name() {
        let mut query = parse_select_query("SELECT * FROM orders");
        let matches = mutate_query_for_ivm_delta_scan(&mut query, &test_base_ref(), 100, 200)
            .expect("mutate must succeed");
        assert_eq!(matches, 1);
        let rendered = query.to_string();
        assert!(
            rendered.contains("__nr_ivm_delta('ice.db.orders', 100, 200)"),
            "unexpected rendered query: {rendered}"
        );
    }

    #[test]
    fn mutate_query_for_ivm_delta_scan_preserves_existing_alias() {
        let mut query = parse_select_query("SELECT * FROM ice.db.orders AS o");
        let matches = mutate_query_for_ivm_delta_scan(&mut query, &test_base_ref(), 7, 8)
            .expect("mutate must succeed");
        assert_eq!(matches, 1);
        let rendered = query.to_string();
        assert!(
            rendered.contains("__nr_ivm_delta('ice.db.orders', 7, 8) AS o"),
            "expected explicit alias to round-trip: {rendered}"
        );
    }

    #[test]
    fn mutate_query_for_ivm_delta_scan_returns_zero_when_no_match() {
        let mut query = parse_select_query("SELECT * FROM other_table");
        let matches = mutate_query_for_ivm_delta_scan(&mut query, &test_base_ref(), 100, 200)
            .expect("mutate must succeed");
        assert_eq!(matches, 0);
    }

    #[test]
    fn mutate_query_for_ivm_delta_scan_returns_multi_when_two_refs() {
        // The mutator itself reports the cardinality; the caller decides what
        // to do with a multi-match result (the IVM refresh driver rejects).
        let mut query =
            parse_select_query("SELECT * FROM ice.db.orders a JOIN ice.db.orders b ON a.id = b.id");
        let matches = mutate_query_for_ivm_delta_scan(&mut query, &test_base_ref(), 100, 200)
            .expect("mutate must succeed");
        assert_eq!(matches, 2);
    }

    #[test]
    fn mutate_query_for_ivm_delta_scan_recurses_into_subquery() {
        let mut query = parse_select_query("SELECT * FROM (SELECT * FROM ice.db.orders) AS sub");
        let matches = mutate_query_for_ivm_delta_scan(&mut query, &test_base_ref(), 5, 6)
            .expect("mutate must succeed");
        assert_eq!(matches, 1);
        let rendered = query.to_string();
        assert!(
            rendered.contains("__nr_ivm_delta('ice.db.orders', 5, 6)"),
            "expected nested derived to be rewritten: {rendered}"
        );
    }

    #[test]
    fn mutate_query_for_ivm_delta_scan_skips_existing_table_function() {
        // A pre-existing __nr_ivm_delta call is itself a TableFactor::Table
        // with `args: Some(...)`. The mutator must not double-wrap.
        let mut query = parse_select_query("SELECT * FROM __nr_ivm_delta('ice.db.orders', 1, 2)");
        let matches = mutate_query_for_ivm_delta_scan(&mut query, &test_base_ref(), 9, 10)
            .expect("mutate must succeed");
        assert_eq!(matches, 0);
    }

    fn output_col(name: &str, ty: DataType, nullable: bool) -> OutputColumn {
        OutputColumn {
            column_id: crate::sql::column_id::ColumnId::UNSET,
            name: name.to_string(),
            data_type: ty,
            nullable,
        }
    }

    struct IcebergMvTestState {
        state: Arc<StandaloneState>,
        current_db: String,
        _metadata_dir: TempDir,
        _warehouse_dir: TempDir,
    }

    fn parse_create_mv(sql: &str) -> CreateMaterializedViewStmt {
        let mut statements = crate::sql::parser::parse_sql(sql).expect("parse");
        let crate::sql::parser::ast::Statement::CreateMaterializedView(stmt) = statements.remove(0)
        else {
            panic!("expected CREATE MATERIALIZED VIEW");
        };
        stmt
    }

    fn parse_refresh_mv(sql: &str) -> RefreshMaterializedViewStmt {
        let mut statements = crate::sql::parser::parse_sql(sql).expect("parse");
        let crate::sql::parser::ast::Statement::RefreshMaterializedView(stmt) =
            statements.remove(0)
        else {
            panic!("expected REFRESH MATERIALIZED VIEW");
        };
        stmt
    }

    fn parse_drop_mv(sql: &str) -> DropMaterializedViewStmt {
        let mut statements = crate::sql::parser::parse_sql(sql).expect("parse");
        let crate::sql::parser::ast::Statement::DropMaterializedView(stmt) = statements.remove(0)
        else {
            panic!("expected DROP MATERIALIZED VIEW");
        };
        stmt
    }

    fn open_test_state_with_iceberg_catalog(catalog: &str, current_db: &str) -> IcebergMvTestState {
        let metadata_dir = TempDir::new().expect("metadata tempdir");
        let warehouse_dir = TempDir::new().expect("warehouse tempdir");
        let metadata_path = metadata_dir.path().join("standalone.sqlite");
        let metadata_provider =
            crate::meta::SqliteMetaStoreProvider::open(&metadata_path).expect("open meta provider");
        let state = Arc::new(StandaloneState {
            metadata_provider: Some(Arc::new(metadata_provider)),
            ..StandaloneState::default()
        });
        crate::connector::register_standalone_backends(&state);
        {
            let mut catalogs = state.iceberg_catalogs.write().expect("iceberg catalogs");
            catalogs
                .create_catalog(
                    catalog,
                    &[
                        ("type".to_string(), "iceberg".to_string()),
                        ("iceberg.catalog.type".to_string(), "memory".to_string()),
                        (
                            "iceberg.catalog.warehouse".to_string(),
                            warehouse_dir.path().display().to_string(),
                        ),
                    ],
                )
                .expect("create iceberg catalog");
        }
        IcebergMvTestState {
            state,
            current_db: current_db.to_string(),
            _metadata_dir: metadata_dir,
            _warehouse_dir: warehouse_dir,
        }
    }

    fn open_test_state_with_iceberg_catalog_without_metadata(
        catalog: &str,
        current_db: &str,
    ) -> IcebergMvTestState {
        let metadata_dir = TempDir::new().expect("metadata tempdir");
        let warehouse_dir = TempDir::new().expect("warehouse tempdir");
        let state = Arc::new(StandaloneState {
            metadata_provider: None,
            ..StandaloneState::default()
        });
        crate::connector::register_standalone_backends(&state);
        {
            let mut catalogs = state.iceberg_catalogs.write().expect("iceberg catalogs");
            catalogs
                .create_catalog(
                    catalog,
                    &[
                        ("type".to_string(), "iceberg".to_string()),
                        ("iceberg.catalog.type".to_string(), "memory".to_string()),
                        (
                            "iceberg.catalog.warehouse".to_string(),
                            warehouse_dir.path().display().to_string(),
                        ),
                    ],
                )
                .expect("create iceberg catalog");
        }
        IcebergMvTestState {
            state,
            current_db: current_db.to_string(),
            _metadata_dir: metadata_dir,
            _warehouse_dir: warehouse_dir,
        }
    }

    fn open_test_state_with_hadoop_iceberg_catalog(
        catalog: &str,
        current_db: &str,
    ) -> IcebergMvTestState {
        let metadata_dir = TempDir::new().expect("metadata tempdir");
        let warehouse_dir = TempDir::new().expect("warehouse tempdir");
        let metadata_path = metadata_dir.path().join("standalone.sqlite");
        let metadata_provider =
            crate::meta::SqliteMetaStoreProvider::open(&metadata_path).expect("open meta provider");
        let state = Arc::new(StandaloneState {
            metadata_provider: Some(Arc::new(metadata_provider)),
            ..StandaloneState::default()
        });
        crate::connector::register_standalone_backends(&state);
        {
            let mut catalogs = state.iceberg_catalogs.write().expect("iceberg catalogs");
            catalogs
                .create_catalog(
                    catalog,
                    &[
                        ("type".to_string(), "iceberg".to_string()),
                        ("iceberg.catalog.type".to_string(), "hadoop".to_string()),
                        (
                            "iceberg.catalog.warehouse".to_string(),
                            format!("file://{}", warehouse_dir.path().display()),
                        ),
                    ],
                )
                .expect("create iceberg catalog");
        }
        IcebergMvTestState {
            state,
            current_db: current_db.to_string(),
            _metadata_dir: metadata_dir,
            _warehouse_dir: warehouse_dir,
        }
    }

    fn find_iceberg_mv_definition(
        state: &Arc<StandaloneState>,
        catalog: &str,
        namespace: &str,
        table: &str,
    ) -> Option<StoredMvDefinition> {
        let provider = state.metadata_provider.as_ref().expect("metadata provider");
        let read = provider.begin_read().expect("open read txn");
        state
            .mv_repo
            .find_by_target(read.as_ref(), catalog, namespace, table)
            .expect("lookup mv definition")
    }

    fn create_base_table(
        state: &Arc<StandaloneState>,
        catalog: &str,
        namespace: &str,
        table: &str,
    ) {
        let entry = {
            let catalogs = state.iceberg_catalogs.read().expect("iceberg catalogs");
            catalogs.get(catalog).expect("catalog")
        };
        let columns = vec![
            crate::sql::TableColumnDef {
                name: "id".to_string(),
                data_type: crate::sql::SqlType::Int,
                nullable: false,
                aggregation: None,
                default: None,
            },
            crate::sql::TableColumnDef {
                name: "name".to_string(),
                data_type: crate::sql::SqlType::String,
                nullable: true,
                aggregation: None,
                default: None,
            },
        ];
        crate::connector::iceberg::catalog::registry::create_table(
            &entry,
            namespace,
            table,
            &columns,
            None,
            &[],
            &[
                ("format-version".to_string(), "3".to_string()),
                ("write.row-lineage".to_string(), "true".to_string()),
            ],
        )
        .expect("create iceberg table");
    }

    fn create_aggregate_fact_table(
        state: &Arc<StandaloneState>,
        catalog: &str,
        namespace: &str,
        table: &str,
    ) {
        let entry = {
            let catalogs = state.iceberg_catalogs.read().expect("iceberg catalogs");
            catalogs.get(catalog).expect("catalog")
        };
        let columns = vec![
            crate::sql::TableColumnDef {
                name: "id".to_string(),
                data_type: crate::sql::SqlType::Int,
                nullable: false,
                aggregation: None,
                default: None,
            },
            crate::sql::TableColumnDef {
                name: "region".to_string(),
                data_type: crate::sql::SqlType::String,
                nullable: true,
                aggregation: None,
                default: None,
            },
            crate::sql::TableColumnDef {
                name: "amount".to_string(),
                data_type: crate::sql::SqlType::BigInt,
                nullable: true,
                aggregation: None,
                default: None,
            },
        ];
        crate::connector::iceberg::catalog::registry::create_table(
            &entry,
            namespace,
            table,
            &columns,
            None,
            &[],
            &[
                ("format-version".to_string(), "3".to_string()),
                ("write.row-lineage".to_string(), "true".to_string()),
            ],
        )
        .expect("create aggregate fact iceberg table");
    }

    fn create_aggregate_fact_table_with_float(
        state: &Arc<StandaloneState>,
        catalog: &str,
        namespace: &str,
        table: &str,
    ) {
        let entry = {
            let catalogs = state.iceberg_catalogs.read().expect("iceberg catalogs");
            catalogs.get(catalog).expect("catalog")
        };
        let columns = vec![
            crate::sql::TableColumnDef {
                name: "id".to_string(),
                data_type: crate::sql::SqlType::Int,
                nullable: false,
                aggregation: None,
                default: None,
            },
            crate::sql::TableColumnDef {
                name: "region".to_string(),
                data_type: crate::sql::SqlType::String,
                nullable: true,
                aggregation: None,
                default: None,
            },
            crate::sql::TableColumnDef {
                name: "price".to_string(),
                data_type: crate::sql::SqlType::Double,
                nullable: true,
                aggregation: None,
                default: None,
            },
        ];
        crate::connector::iceberg::catalog::registry::create_table(
            &entry,
            namespace,
            table,
            &columns,
            None,
            &[],
            &[
                ("format-version".to_string(), "3".to_string()),
                ("write.row-lineage".to_string(), "true".to_string()),
            ],
        )
        .expect("create aggregate fact (float) iceberg table");
    }

    fn create_aggregate_dim_table(
        state: &Arc<StandaloneState>,
        catalog: &str,
        namespace: &str,
        table: &str,
    ) {
        let entry = {
            let catalogs = state.iceberg_catalogs.read().expect("iceberg catalogs");
            catalogs.get(catalog).expect("catalog")
        };
        let columns = vec![
            crate::sql::TableColumnDef {
                name: "id".to_string(),
                data_type: crate::sql::SqlType::Int,
                nullable: false,
                aggregation: None,
                default: None,
            },
            crate::sql::TableColumnDef {
                name: "category".to_string(),
                data_type: crate::sql::SqlType::String,
                nullable: true,
                aggregation: None,
                default: None,
            },
        ];
        crate::connector::iceberg::catalog::registry::create_table(
            &entry,
            namespace,
            table,
            &columns,
            None,
            &[],
            &[
                ("format-version".to_string(), "3".to_string()),
                ("write.row-lineage".to_string(), "true".to_string()),
            ],
        )
        .expect("create aggregate dim iceberg table");
    }

    fn create_identity_partitioned_base_table(
        state: &Arc<StandaloneState>,
        catalog: &str,
        namespace: &str,
        table: &str,
    ) {
        let entry = {
            let catalogs = state.iceberg_catalogs.read().expect("iceberg catalogs");
            catalogs.get(catalog).expect("catalog")
        };
        let columns = vec![
            crate::sql::TableColumnDef {
                name: "id".to_string(),
                data_type: crate::sql::SqlType::Int,
                nullable: false,
                aggregation: None,
                default: None,
            },
            crate::sql::TableColumnDef {
                name: "name".to_string(),
                data_type: crate::sql::SqlType::String,
                nullable: true,
                aggregation: None,
                default: None,
            },
        ];
        crate::connector::iceberg::catalog::registry::create_table(
            &entry,
            namespace,
            table,
            &columns,
            None,
            &[
                crate::sql::parser::ast::IcebergPartitionFieldExpr::Identity {
                    column: "id".to_string(),
                },
            ],
            &[
                ("format-version".to_string(), "3".to_string()),
                ("write.row-lineage".to_string(), "true".to_string()),
            ],
        )
        .expect("create identity-partitioned iceberg table");
    }

    fn insert_into_iceberg_table(
        state: &Arc<StandaloneState>,
        catalog: &str,
        namespace: &str,
        table: &str,
        rows: &[(i32, &str)],
    ) {
        let entry = {
            let catalogs = state.iceberg_catalogs.read().expect("iceberg catalogs");
            catalogs.get(catalog).expect("catalog")
        };
        let has_apply_key_column =
            crate::connector::iceberg::catalog::load_table(&entry, namespace, table)
                .expect("load iceberg table")
                .table
                .metadata()
                .current_schema()
                .as_struct()
                .fields()
                .iter()
                .any(|field| field.name.eq_ignore_ascii_case(ICEBERG_MV_APPLY_KEY_COLUMN));
        let rows = rows
            .iter()
            .enumerate()
            .map(|(idx, (id, name))| {
                let mut values = vec![
                    crate::sql::Literal::Int(i64::from(*id)),
                    crate::sql::Literal::String((*name).to_string()),
                ];
                if has_apply_key_column {
                    values.push(crate::sql::Literal::Int(1_000_i64 + idx as i64));
                }
                values
            })
            .collect::<Vec<_>>();
        crate::connector::iceberg::catalog::registry::insert_rows(&entry, namespace, table, &rows)
            .expect("insert iceberg rows");
    }

    fn insert_into_aggregate_fact_table(
        state: &Arc<StandaloneState>,
        catalog: &str,
        namespace: &str,
        table: &str,
        rows: &[(i32, &str, i64)],
    ) {
        let entry = {
            let catalogs = state.iceberg_catalogs.read().expect("iceberg catalogs");
            catalogs.get(catalog).expect("catalog")
        };
        let rows = rows
            .iter()
            .map(|(id, region, amount)| {
                vec![
                    crate::sql::Literal::Int(i64::from(*id)),
                    crate::sql::Literal::String((*region).to_string()),
                    crate::sql::Literal::Int(*amount),
                ]
            })
            .collect::<Vec<_>>();
        crate::connector::iceberg::catalog::registry::insert_rows(&entry, namespace, table, &rows)
            .expect("insert aggregate fact iceberg rows");
    }

    fn insert_into_aggregate_dim_table(
        state: &Arc<StandaloneState>,
        catalog: &str,
        namespace: &str,
        table: &str,
        rows: &[(i32, &str)],
    ) {
        let entry = {
            let catalogs = state.iceberg_catalogs.read().expect("iceberg catalogs");
            catalogs.get(catalog).expect("catalog")
        };
        let rows = rows
            .iter()
            .map(|(id, category)| {
                vec![
                    crate::sql::Literal::Int(i64::from(*id)),
                    crate::sql::Literal::String((*category).to_string()),
                ]
            })
            .collect::<Vec<_>>();
        crate::connector::iceberg::catalog::registry::insert_rows(&entry, namespace, table, &rows)
            .expect("insert aggregate dim iceberg rows");
    }

    fn create_base_table_with_rows(
        state: &Arc<StandaloneState>,
        catalog: &str,
        namespace: &str,
        table: &str,
        rows: &[(i32, &str)],
    ) {
        create_base_table(state, catalog, namespace, table);
        insert_into_iceberg_table(state, catalog, namespace, table, rows);
    }

    fn create_mv_and_refresh_once(
        state: &Arc<StandaloneState>,
        current_catalog: Option<&str>,
        current_db: &str,
        mv_name: &str,
    ) {
        let stmt = parse_create_mv(&format!(
            "CREATE MATERIALIZED VIEW {mv_name}
             DISTRIBUTED BY HASH(id) BUCKETS 1
             PROPERTIES('storage_engine'='iceberg')
             AS SELECT id, name FROM ice.sales.orders"
        ));
        create_iceberg_mv(state, current_catalog, current_db, &stmt).expect("create iceberg mv");
        let refresh = parse_refresh_mv(&format!("REFRESH MATERIALIZED VIEW {mv_name}"));
        refresh_iceberg_mv(state, current_catalog, current_db, &refresh)
            .expect("refresh iceberg mv");
    }

    fn create_mv_only(
        state: &Arc<StandaloneState>,
        current_catalog: Option<&str>,
        current_db: &str,
        mv_name: &str,
    ) {
        let stmt = parse_create_mv(&format!(
            "CREATE MATERIALIZED VIEW {mv_name}
             DISTRIBUTED BY HASH(id) BUCKETS 1
             PROPERTIES('storage_engine'='iceberg')
             AS SELECT id, name FROM ice.sales.orders"
        ));
        create_iceberg_mv(state, current_catalog, current_db, &stmt).expect("create iceberg mv");
    }

    fn add_target_identity_partition_column(
        state: &Arc<StandaloneState>,
        catalog: &str,
        namespace: &str,
        table: &str,
        column: &str,
    ) {
        let entry = {
            let catalogs = state.iceberg_catalogs.read().expect("iceberg catalogs");
            catalogs.get(catalog).expect("catalog")
        };
        crate::connector::iceberg::catalog::registry::alter_partition_spec(
            &entry,
            namespace,
            table,
            crate::sql::parser::ast::AlterIcebergPartitionSpecStmt::AddPartitionColumn {
                table: crate::sql::parser::ast::ObjectName {
                    parts: vec![
                        catalog.to_string(),
                        namespace.to_string(),
                        table.to_string(),
                    ],
                },
                field: crate::sql::parser::ast::IcebergPartitionFieldExpr::Identity {
                    column: column.to_string(),
                },
            },
        )
        .expect("alter target partition spec");
    }

    fn sorted_base_field_ids(
        contract: &crate::meta::repository::mv_contract::BaseContract,
    ) -> Vec<i32> {
        contract
            .schema_at_create
            .fields
            .iter()
            .map(|field| field.field_id)
            .collect::<Vec<_>>()
    }

    fn create_mv_with_select_only(
        state: &Arc<StandaloneState>,
        current_catalog: Option<&str>,
        current_db: &str,
        mv_name: &str,
        select_sql: &str,
    ) {
        let stmt = parse_create_mv(&format!(
            "CREATE MATERIALIZED VIEW {mv_name}
             DISTRIBUTED BY HASH(id) BUCKETS 1
             PROPERTIES('storage_engine'='iceberg')
             AS {select_sql}"
        ));
        create_iceberg_mv(state, current_catalog, current_db, &stmt).expect("create iceberg mv");
    }

    fn load_all_mv_refreshes(state: &Arc<StandaloneState>) -> Vec<StoredMvRefresh> {
        let provider = state.metadata_provider.as_ref().expect("metadata provider");
        let read = provider.begin_read().expect("read txn");
        let mut refreshes = read
            .scan(
                &crate::meta::MetaKeyPrefix::new(crate::meta::keys::NS_MV, ["refresh"])
                    .expect("refresh key prefix"),
                None,
            )
            .expect("scan refreshes")
            .into_iter()
            .map(|record| {
                crate::meta::repository::decode_payload_for_kind::<StoredMvRefresh>(
                    "mv.refresh",
                    &record.payload,
                )
                .expect("decode refresh")
            })
            .collect::<Vec<_>>();
        refreshes.sort_by_key(|refresh| refresh.refresh_id);
        refreshes
    }

    fn single_int_chunk(values: &[i32]) -> Vec<crate::exec::chunk::Chunk> {
        let arrow_schema = StdArc::new(ArrowSchema::new(vec![Field::new(
            "k",
            DataType::Int32,
            false,
        )]));
        let batch = RecordBatch::try_new(
            arrow_schema.clone(),
            vec![StdArc::new(Int32Array::from(values.to_vec()))],
        )
        .expect("record batch");
        let chunk_schema_ref = crate::exec::chunk::ChunkSchema::try_ref_from_schema_and_slot_ids(
            &arrow_schema,
            &[crate::common::ids::SlotId(0)],
        )
        .expect("chunk schema");
        vec![crate::exec::chunk::Chunk::new_with_chunk_schema(
            batch,
            chunk_schema_ref,
        )]
    }

    fn id_name_chunk(rows: &[(i32, &str)]) -> Vec<crate::exec::chunk::Chunk> {
        let arrow_schema = StdArc::new(ArrowSchema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, true),
            Field::new(ICEBERG_MV_APPLY_KEY_COLUMN, DataType::Int64, false),
        ]));
        let batch = RecordBatch::try_new(
            arrow_schema.clone(),
            vec![
                StdArc::new(Int32Array::from_iter_values(rows.iter().map(|(id, _)| *id))),
                StdArc::new(StringArray::from_iter_values(
                    rows.iter().map(|(_, name)| *name),
                )),
                StdArc::new(Int64Array::from_iter_values(
                    rows.iter()
                        .enumerate()
                        .map(|(idx, _)| 1_000_i64 + idx as i64),
                )),
            ],
        )
        .expect("record batch");
        let chunk_schema_ref = crate::exec::chunk::ChunkSchema::try_ref_from_schema_and_slot_ids(
            &arrow_schema,
            &[
                crate::common::ids::SlotId(0),
                crate::common::ids::SlotId(1),
                crate::common::ids::SlotId(2),
            ],
        )
        .expect("chunk schema");
        vec![crate::exec::chunk::Chunk::new_with_chunk_schema(
            batch,
            chunk_schema_ref,
        )]
    }

    fn seed_active_staging_refresh(
        state: &Arc<StandaloneState>,
        catalog_name: &str,
        namespace: &str,
        table_name: &str,
        publish_main: bool,
    ) -> i64 {
        let mv = find_iceberg_mv_definition(state, catalog_name, namespace, table_name)
            .expect("mv definition");
        let target = IcebergMvTarget {
            catalog: catalog_name.to_string(),
            namespace: namespace.to_string(),
            table: table_name.to_string(),
        };
        let entry = {
            let catalogs = state.iceberg_catalogs.read().expect("iceberg catalogs");
            catalogs.get(catalog_name).expect("catalog")
        };
        let iceberg_catalog =
            crate::connector::iceberg::catalog::registry::build_iceberg_catalog(&entry)
                .expect("catalog");
        let loaded = crate::connector::iceberg::catalog::load_table(&entry, namespace, table_name)
            .expect("load target table");
        let expected_main_snapshot_id = loaded
            .table
            .metadata()
            .current_snapshot()
            .map(|snapshot| snapshot.snapshot_id());
        let staging_branch = format!("__nova_mv_refresh_test_{}", uuid::Uuid::new_v4().simple());
        ensure_iceberg_mv_staging_branch(
            &iceberg_catalog,
            &target,
            &staging_branch,
            expected_main_snapshot_id,
        )
        .expect("create staging branch");
        let refresh_id = begin_staged_iceberg_mv_refresh_intent(
            state,
            &target,
            mv.mv_id,
            expected_main_snapshot_id,
            BTreeMap::new(),
            &staging_branch,
        )
        .expect("begin staged refresh");
        let marker = load_iceberg_mv_refresh_marker(state, refresh_id, mv.mv_id)
            .expect("marker")
            .to_summary_properties();

        let staging_snapshot = data_block_on(async {
            let ident = iceberg_mv_table_ident(&target).expect("ident");
            let table = iceberg_catalog
                .load_table(&ident)
                .await
                .expect("load target");
            let chunks = id_name_chunk(&[(1, "staged")]);
            let written = write_chunks_as_iceberg_data_files(&table, &chunks)
                .await
                .expect("write chunks");
            commit_iceberg_mv_target_files_with_ref(
                &table,
                &iceberg_catalog,
                &entry,
                &ident,
                CommitOpKind::FastAppend,
                written,
                &staging_branch,
                marker,
            )
            .await
            .expect("commit staging")
            .new_snapshot_id
        })
        .expect("runtime");
        record_iceberg_mv_staging_commit(state, refresh_id, staging_snapshot, 1, BTreeMap::new())
            .expect("record staging");

        if publish_main {
            let published_snapshot = publish_iceberg_mv_refresh(
                state,
                &target,
                &entry,
                &staging_branch,
                expected_main_snapshot_id,
                staging_snapshot,
                refresh_id,
                mv.mv_id,
            )
            .expect("publish staging");
            record_iceberg_mv_publish_commit(state, refresh_id, published_snapshot)
                .expect("record publish");
            published_snapshot
        } else {
            staging_snapshot
        }
    }

    fn advance_target_main_without_refresh_marker(
        state: &Arc<StandaloneState>,
        catalog_name: &str,
        namespace: &str,
        table_name: &str,
    ) -> i64 {
        let entry = {
            let catalogs = state.iceberg_catalogs.read().expect("iceberg catalogs");
            catalogs.get(catalog_name).expect("catalog")
        };
        let iceberg_catalog =
            crate::connector::iceberg::catalog::registry::build_iceberg_catalog(&entry)
                .expect("catalog");
        let target = IcebergMvTarget {
            catalog: catalog_name.to_string(),
            namespace: namespace.to_string(),
            table: table_name.to_string(),
        };
        data_block_on(async {
            let ident = iceberg_mv_table_ident(&target).expect("ident");
            let table = iceberg_catalog
                .load_table(&ident)
                .await
                .expect("load target");
            let chunks = id_name_chunk(&[(99, "external")]);
            let written = write_chunks_as_iceberg_data_files(&table, &chunks)
                .await
                .expect("write chunks");
            commit_iceberg_mv_target_files_with_ref(
                &table,
                &iceberg_catalog,
                &entry,
                &ident,
                CommitOpKind::FastAppend,
                written,
                "main",
                BTreeMap::new(),
            )
            .await
            .expect("commit external main")
            .new_snapshot_id
        })
        .expect("runtime")
    }

    #[test]
    fn create_iceberg_mv_creates_branch_capable_v3_target() {
        let env = open_test_state_with_iceberg_catalog("ice", "analytics");
        create_base_table(&env.state, "ice", "sales", "orders");
        let stmt = parse_create_mv(
            "CREATE MATERIALIZED VIEW mv_orders
             DISTRIBUTED BY HASH(id) BUCKETS 1
             PROPERTIES('storage_engine'='iceberg')
             AS SELECT id, name FROM ice.sales.orders",
        );

        create_iceberg_mv(&env.state, Some("ice"), &env.current_db, &stmt)
            .expect("create iceberg mv");

        let entry = {
            let catalogs = env.state.iceberg_catalogs.read().expect("iceberg catalogs");
            catalogs.get("ice").expect("catalog")
        };
        let loaded =
            crate::connector::iceberg::catalog::load_table(&entry, "analytics", "mv_orders")
                .expect("load target table");
        assert_eq!(
            loaded.table.metadata().format_version(),
            iceberg::spec::FormatVersion::V3
        );
        assert_eq!(
            loaded
                .table
                .metadata()
                .properties()
                .get("write.row-lineage")
                .map(String::as_str),
            Some("true")
        );
        assert_eq!(
            loaded
                .table
                .metadata()
                .properties()
                .get(ICEBERG_MV_PROP_APPLY_KEY_COLUMN)
                .map(String::as_str),
            Some(ICEBERG_MV_APPLY_KEY_COLUMN)
        );
        let fields = loaded
            .table
            .metadata()
            .current_schema()
            .as_struct()
            .fields();
        let apply_key_field = fields
            .iter()
            .find(|field| field.name == ICEBERG_MV_APPLY_KEY_COLUMN)
            .expect("target apply-key field");
        assert_eq!(apply_key_field.id, 3);
        assert!(apply_key_field.required);
        assert_eq!(
            find_iceberg_mv_definition(&env.state, "ice", "analytics", "mv_orders")
                .expect("mv definition")
                .schema_contract
                .expect("schema contract")
                .target
                .hidden_apply_key
                .target_field_id,
            3
        );
    }

    #[test]
    fn create_iceberg_mv_creates_partitioned_target_from_partition_by() {
        let env = open_test_state_with_iceberg_catalog("ice", "analytics");
        create_base_table(&env.state, "ice", "sales", "orders");
        let stmt = parse_create_mv(
            "CREATE MATERIALIZED VIEW mv_orders
             PARTITION BY (bucket(id, 16), truncate(name, 8))
             DISTRIBUTED BY HASH(id) BUCKETS 1
             PROPERTIES('storage_engine'='iceberg')
             AS SELECT id, name FROM ice.sales.orders",
        );

        create_iceberg_mv(&env.state, Some("ice"), &env.current_db, &stmt)
            .expect("create iceberg mv");

        let entry = {
            let catalogs = env.state.iceberg_catalogs.read().expect("iceberg catalogs");
            catalogs.get("ice").expect("catalog")
        };
        let loaded =
            crate::connector::iceberg::catalog::load_table(&entry, "analytics", "mv_orders")
                .expect("load target table");
        let spec = loaded.table.metadata().default_partition_spec();
        assert_eq!(spec.spec_id(), 0);
        let fields = spec.fields();
        assert_eq!(fields.len(), 2);
        assert_eq!(fields[0].name, "id_bucket_16");
        assert_eq!(fields[0].source_id, 1);
        assert_eq!(fields[0].transform, iceberg::spec::Transform::Bucket(16));
        assert_eq!(fields[1].name, "name_truncate_8");
        assert_eq!(fields[1].source_id, 2);
        assert_eq!(fields[1].transform, iceberg::spec::Transform::Truncate(8));
        let definition = find_iceberg_mv_definition(&env.state, "ice", "analytics", "mv_orders")
            .expect("mv definition");
        let stored_partition = definition.partition_spec.expect("stored partition spec");
        assert_eq!(stored_partition.target_spec_id, 0);
        assert_eq!(stored_partition.fields.len(), 2);
        let contract_partition = definition
            .schema_contract
            .expect("schema contract")
            .target
            .partition
            .expect("target partition contract");
        assert_eq!(contract_partition, stored_partition);
    }

    #[test]
    fn create_iceberg_aggregate_mv_persists_v3_group_row_id_contract() {
        let env = open_test_state_with_iceberg_catalog("ice", "analytics");
        create_aggregate_fact_table(&env.state, "ice", "sales", "fact");
        let stmt = parse_create_mv(
            "CREATE MATERIALIZED VIEW mv_fact_region
             DISTRIBUTED BY HASH(region) BUCKETS 1
             PROPERTIES('storage_engine'='iceberg')
             AS SELECT region, count(*) AS c, sum(amount) AS s
                FROM ice.sales.fact
                GROUP BY region",
        );

        create_iceberg_mv(&env.state, Some("ice"), &env.current_db, &stmt)
            .expect("create aggregate iceberg mv");

        let entry = {
            let catalogs = env.state.iceberg_catalogs.read().expect("iceberg catalogs");
            catalogs.get("ice").expect("catalog")
        };
        let loaded =
            crate::connector::iceberg::catalog::load_table(&entry, "analytics", "mv_fact_region")
                .expect("load aggregate target table");
        assert_eq!(
            loaded
                .table
                .metadata()
                .properties()
                .get(ICEBERG_MV_PROP_HIDDEN_COLUMNS)
                .map(String::as_str),
            Some("__agg_state_c,__agg_state_s")
        );

        let contract = find_iceberg_mv_definition(&env.state, "ice", "analytics", "mv_fact_region")
            .expect("mv definition")
            .schema_contract
            .expect("schema contract");
        assert_eq!(contract.contract_version, 3);
        assert_eq!(
            contract.target.hidden_apply_key.column_name,
            crate::meta::repository::mv_contract::GROUP_ROW_ID_APPLY_KEY_COLUMN_NAME
        );
        assert_eq!(
            contract.target.hidden_apply_key.source,
            crate::meta::repository::mv_contract::ApplyKeySource::GroupRowId
        );
        assert_eq!(contract.target.hidden_apply_key.target_field_id, 1);
        assert_eq!(sorted_base_field_ids(&contract.base), vec![2, 3]);
        assert_eq!(
            contract.output.columns[2]
                .expression
                .referenced_base_field_ids,
            vec![3]
        );
        let aggregate = contract.aggregate.expect("aggregate contract");
        assert_eq!(aggregate.state_layout_version, 1);
        assert_eq!(
            aggregate.row_id_column_name,
            crate::meta::repository::mv_contract::GROUP_ROW_ID_APPLY_KEY_COLUMN_NAME
        );
        assert_eq!(aggregate.state_columns.len(), 2);
        assert_eq!(aggregate.state_columns[0].column_name, "__agg_state_c");
        assert_eq!(aggregate.state_columns[0].target_field_id, 5);
        assert_eq!(aggregate.state_columns[0].type_signature, "long");
        assert!(aggregate.state_columns[0].nullable);
        assert_eq!(
            aggregate.state_columns[0].role,
            crate::meta::repository::mv_contract::AggregateStateRoleContract::Single
        );
        assert_eq!(aggregate.state_columns[1].column_name, "__agg_state_s");
        assert_eq!(aggregate.state_columns[1].target_field_id, 6);
        assert_eq!(aggregate.state_columns[1].type_signature, "long");
        assert!(aggregate.state_columns[1].nullable);
        assert_eq!(
            aggregate.state_columns[1].role,
            crate::meta::repository::mv_contract::AggregateStateRoleContract::Single
        );
    }

    #[test]
    fn create_iceberg_join_aggregate_mv_persists_join_and_group_row_id_contract() {
        let env = open_test_state_with_iceberg_catalog("ice", "analytics");
        create_aggregate_fact_table(&env.state, "ice", "sales", "fact");
        create_aggregate_dim_table(&env.state, "ice", "sales", "dim");
        let stmt = parse_create_mv(
            "CREATE MATERIALIZED VIEW mv_fact_dim
             DISTRIBUTED BY HASH(category) BUCKETS 1
             PROPERTIES('storage_engine'='iceberg')
             AS SELECT d.category, sum(f.amount) AS total
                FROM ice.sales.fact f JOIN ice.sales.dim d ON f.id = d.id
                GROUP BY d.category",
        );

        create_iceberg_mv(&env.state, Some("ice"), &env.current_db, &stmt)
            .expect("create join aggregate iceberg mv");

        let contract = find_iceberg_mv_definition(&env.state, "ice", "analytics", "mv_fact_dim")
            .expect("mv definition")
            .schema_contract
            .expect("schema contract");
        assert_eq!(contract.contract_version, 3);
        assert_eq!(contract.bases.len(), 2);
        assert!(contract.join.is_some());
        assert!(contract.aggregate.is_some());
        assert_eq!(
            contract.target.hidden_apply_key.source,
            crate::meta::repository::mv_contract::ApplyKeySource::GroupRowId
        );
        assert_eq!(
            contract.target.hidden_apply_key.column_name,
            crate::meta::repository::mv_contract::GROUP_ROW_ID_APPLY_KEY_COLUMN_NAME
        );

        let fact_contract = contract
            .bases
            .iter()
            .find(|base| base.table_fqn == "ice.sales.fact")
            .expect("fact base contract");
        let dim_contract = contract
            .bases
            .iter()
            .find(|base| base.table_fqn == "ice.sales.dim")
            .expect("dim base contract");
        assert_eq!(fact_contract.alias_at_create.as_deref(), Some("f"));
        assert_eq!(dim_contract.alias_at_create.as_deref(), Some("d"));
        assert_eq!(sorted_base_field_ids(fact_contract), vec![1, 3]);
        assert_eq!(sorted_base_field_ids(dim_contract), vec![1, 2]);
        assert_eq!(
            contract.join.as_ref().unwrap().predicates[0].left.table_fqn,
            "ice.sales.fact"
        );
        assert_eq!(
            contract.join.as_ref().unwrap().predicates[0].left.field_id,
            1
        );
        assert_eq!(
            contract.join.as_ref().unwrap().predicates[0]
                .right
                .table_fqn,
            "ice.sales.dim"
        );
        assert_eq!(
            contract.join.as_ref().unwrap().predicates[0].right.field_id,
            1
        );
        assert_eq!(
            contract.output.columns[1].expression.referenced_base_fields,
            vec![
                crate::meta::repository::mv_contract::QualifiedFieldLineage {
                    table_fqn: "ice.sales.fact".to_string(),
                    qualifier_at_create: "f".to_string(),
                    field_id: 3,
                }
            ]
        );
    }

    #[test]
    fn iceberg_aggregate_target_columns_reject_duplicate_physical_names() {
        let query = parse_select_query(
            "select region, sum(amount) as s, count(*) as __agg_state_s \
             from ice.ns.fact group by region",
        );
        let shape = match classify_incremental_mv_query(&query).expect("shape") {
            IncrementalMvShape::Aggregate(shape) => shape,
            other => panic!("expected aggregate shape, got {other:?}"),
        };
        let output_columns = vec![
            output_col("region", DataType::Utf8, true),
            output_col("s", DataType::Int64, true),
            output_col("__agg_state_s", DataType::Int64, false),
        ];

        let err = iceberg_aggregate_target_columns(&shape, &output_columns)
            .expect_err("duplicate aggregate physical column names should be rejected");
        assert!(
            err.contains("aggregate MV physical column name collision"),
            "err={err}"
        );
        assert!(err.contains("__agg_state_s"), "err={err}");
    }

    #[test]
    fn create_iceberg_aggregate_mv_rejects_primary_key() {
        let env = open_test_state_with_iceberg_catalog("ice", "analytics");
        create_aggregate_fact_table(&env.state, "ice", "sales", "fact");
        let stmt = parse_create_mv(
            "CREATE MATERIALIZED VIEW mv_fact_region
             DISTRIBUTED BY HASH(region) BUCKETS 1
             PRIMARY KEY (region)
             PROPERTIES('storage_engine'='iceberg')
             AS SELECT region, count(*) AS c
                FROM ice.sales.fact
                GROUP BY region",
        );

        let err = create_iceberg_mv(&env.state, Some("ice"), &env.current_db, &stmt)
            .expect_err("aggregate primary key should be rejected");
        assert!(
            err.contains("iceberg-backed aggregate materialized views do not support PRIMARY KEY"),
            "err={err}"
        );
    }

    #[test]
    fn iceberg_aggregate_mv_with_min_max_int64_passes_validation() {
        // IVM-P5 Phase 5: the DDL-time MIN/MAX rejection is removed. With
        // Phase 1-4 already wiring the detail-map state runtime path,
        // creating an aggregate MV containing MIN(int64_col) and
        // MAX(int64_col) succeeds end-to-end.
        let env = open_test_state_with_iceberg_catalog("ice", "analytics");
        create_aggregate_fact_table(&env.state, "ice", "sales", "fact");
        let stmt = parse_create_mv(
            "CREATE MATERIALIZED VIEW mv_fact_region
             DISTRIBUTED BY HASH(region) BUCKETS 1
             PROPERTIES('storage_engine'='iceberg')
             AS SELECT region, min(amount) AS min_amount, max(amount) AS max_amount
                FROM ice.sales.fact
                GROUP BY region",
        );

        create_iceberg_mv(&env.state, Some("ice"), &env.current_db, &stmt)
            .expect("MIN/MAX on int64 column should be accepted post-Phase-5");

        // Verify the resulting layout has Map<Int64, Int64> state columns
        // for both MIN and MAX (the visible scalar SQL type is BigInt, but
        // the physical state column stores a value-count detail map).
        let contract = find_iceberg_mv_definition(&env.state, "ice", "analytics", "mv_fact_region")
            .expect("mv definition")
            .schema_contract
            .expect("schema contract");
        let aggregate = contract.aggregate.expect("aggregate contract");
        // Two MIN/MAX state columns plus one retraction-count state column
        // (no explicit COUNT(*) in the shape — see
        // `aggregate_shape_needs_retraction_count_state`).
        assert_eq!(
            aggregate.state_columns.len(),
            3,
            "unexpected state column layout: {:?}",
            aggregate
                .state_columns
                .iter()
                .map(|c| c.column_name.clone())
                .collect::<Vec<_>>()
        );
        let by_name: std::collections::HashMap<&str, &str> = aggregate
            .state_columns
            .iter()
            .map(|c| (c.column_name.as_str(), c.type_signature.as_str()))
            .collect();
        // MIN and MAX state columns are Map<key, Int64> — iceberg's Type
        // display renders Map types as the literal string "map".
        assert_eq!(by_name.get("__agg_state_min_amount").copied(), Some("map"));
        assert_eq!(by_name.get("__agg_state_max_amount").copied(), Some("map"));
    }

    #[test]
    fn iceberg_aggregate_mv_with_min_max_combined_with_others_passes_validation() {
        // IVM-P5 Phase 5: MIN/MAX coexists with SUM/COUNT/AVG aggregates.
        // Validates that the layout builder handles a mixed-aggregate shape
        // and produces the expected mix of Map and scalar state columns.
        let env = open_test_state_with_iceberg_catalog("ice", "analytics");
        create_aggregate_fact_table(&env.state, "ice", "sales", "fact");
        let stmt = parse_create_mv(
            "CREATE MATERIALIZED VIEW mv_fact_mix
             DISTRIBUTED BY HASH(region) BUCKETS 1
             PROPERTIES('storage_engine'='iceberg')
             AS SELECT region,
                       min(amount) AS min_amount,
                       max(region) AS max_region,
                       sum(amount) AS sum_amount,
                       count(*) AS row_count,
                       avg(amount) AS avg_amount
                FROM ice.sales.fact
                GROUP BY region",
        );

        create_iceberg_mv(&env.state, Some("ice"), &env.current_db, &stmt)
            .expect("mixed aggregate MV with MIN/MAX should be accepted post-Phase-5");

        let contract = find_iceberg_mv_definition(&env.state, "ice", "analytics", "mv_fact_mix")
            .expect("mv definition")
            .schema_contract
            .expect("schema contract");
        let aggregate = contract.aggregate.expect("aggregate contract");
        // 1 (MIN map) + 1 (MAX map) + 1 (SUM scalar) + 1 (COUNT(*) scalar)
        // + 2 (AVG: sum + count) = 6 state columns.
        assert_eq!(
            aggregate.state_columns.len(),
            6,
            "unexpected state column layout: {:?}",
            aggregate
                .state_columns
                .iter()
                .map(|c| c.column_name.clone())
                .collect::<Vec<_>>()
        );
        let by_name: std::collections::HashMap<&str, &str> = aggregate
            .state_columns
            .iter()
            .map(|c| (c.column_name.as_str(), c.type_signature.as_str()))
            .collect();
        // MIN/MAX state columns are Map<key, Int64> — iceberg's Type display
        // renders Map types as the literal string "map".
        assert_eq!(by_name.get("__agg_state_min_amount").copied(), Some("map"));
        assert_eq!(by_name.get("__agg_state_max_region").copied(), Some("map"));
        // SUM / COUNT(*) / AVG components are scalar state columns.
        assert_eq!(by_name.get("__agg_state_sum_amount").copied(), Some("long"));
        assert_eq!(by_name.get("__agg_state_row_count").copied(), Some("long"));
        assert_eq!(
            by_name.get("__agg_state_avg_amount__sum").copied(),
            Some("long")
        );
        assert_eq!(
            by_name.get("__agg_state_avg_amount__count").copied(),
            Some("long")
        );
    }

    #[test]
    fn iceberg_aggregate_mv_with_min_float_is_accepted() {
        // IVM-P5 Float follow-up: Float MIN/MAX is now supported in
        // detail-state aggregate IMVs. NaN handling lives in three sites:
        // `scalar_keys_equal` (NaN == NaN), `sort_map_entries_by_key`
        // (NaN sorts to end), and `derive_visible_from_detail_map` (skips
        // NaN keys — matches SQL standard "ignore NaN in MIN/MAX").
        // This replaces the previous Phase 5 rejection test.
        let env = open_test_state_with_iceberg_catalog("ice", "analytics");
        create_aggregate_fact_table_with_float(&env.state, "ice", "sales", "fact_float");
        let stmt = parse_create_mv(
            "CREATE MATERIALIZED VIEW mv_fact_float
             DISTRIBUTED BY HASH(region) BUCKETS 1
             PROPERTIES('storage_engine'='iceberg')
             AS SELECT region, min(price) AS min_price
                FROM ice.sales.fact_float
                GROUP BY region",
        );

        create_iceberg_mv(&env.state, Some("ice"), &env.current_db, &stmt)
            .expect("Float MIN/MAX should now be accepted at the validator layer");
    }

    #[test]
    fn create_iceberg_mv_uses_current_catalog_target_without_managed_table_row() {
        let env = open_test_state_with_iceberg_catalog("ice", "analytics");
        create_base_table(&env.state, "ice", "sales", "orders");

        let stmt = parse_create_mv(
            "CREATE MATERIALIZED VIEW mv_orders
             DISTRIBUTED BY HASH(id) BUCKETS 1
             PROPERTIES('storage_engine'='iceberg')
             AS SELECT id, name FROM ice.sales.orders",
        );

        crate::engine::mv_flow::create_mv(&env.state, Some("ice"), &env.current_db, &stmt)
            .expect("create iceberg mv through ddl");

        let mv = find_iceberg_mv_definition(&env.state, "ice", "analytics", "mv_orders")
            .expect("mv relationship");
        assert_eq!(mv.select_sql, "SELECT id, name FROM ice.sales.orders");
        assert_eq!(mv.target_catalog.as_deref(), Some("ice"));
        assert_eq!(mv.target_namespace.as_deref(), Some("analytics"));
        assert_eq!(mv.target_table.as_deref(), Some("mv_orders"));

        let entry = {
            let catalogs = env.state.iceberg_catalogs.read().expect("iceberg catalogs");
            catalogs.get("ice").expect("catalog")
        };
        crate::connector::iceberg::catalog::load_table(&entry, "analytics", "mv_orders")
            .expect("target table");
        let catalog = env.state.catalog.read().expect("standalone catalog");
        catalog
            .get("analytics", "mv_orders")
            .expect("registered target");
    }

    #[test]
    fn refresh_iceberg_mv_empty_base_remains_noop_before_pin_capture() {
        let env = open_test_state_with_iceberg_catalog("ice", "analytics");
        create_base_table(&env.state, "ice", "sales", "orders");
        create_mv_only(&env.state, Some("ice"), &env.current_db, "mv_orders");

        let refresh = parse_refresh_mv("REFRESH MATERIALIZED VIEW mv_orders");
        refresh_iceberg_mv(&env.state, Some("ice"), &env.current_db, &refresh)
            .expect("empty base refresh should be no-op");

        let mv = find_iceberg_mv_definition(&env.state, "ice", "analytics", "mv_orders")
            .expect("mv definition");
        assert!(mv.last_refresh_snapshots.is_empty());
        assert!(mv.last_refresh_table_uuids.is_empty());

        let target = crate::engine::mv::lifecycle::MvTarget {
            catalog: Some("ice".to_string()),
            database: "analytics".to_string(),
            name: "mv_orders".to_string(),
        };
        let plan =
            plan_iceberg_mv_refresh(&env.state, Some("ice"), &env.current_db, &refresh, target)
                .expect("empty base plan should be no-op");
        assert_eq!(plan.mode, RefreshMode::Noop);
        assert_eq!(
            plan.snapshot_pins.get("ice.sales.orders").copied(),
            Some(None)
        );
    }

    #[test]
    fn empty_base_noop_refresh_rejects_external_target_partition_change() {
        let env = open_test_state_with_iceberg_catalog("ice", "analytics");
        create_base_table(&env.state, "ice", "sales", "orders");
        create_mv_only(&env.state, Some("ice"), &env.current_db, "mv_orders");
        add_target_identity_partition_column(&env.state, "ice", "analytics", "mv_orders", "id");

        let refresh = parse_refresh_mv("REFRESH MATERIALIZED VIEW mv_orders");
        let err = refresh_iceberg_mv(&env.state, Some("ice"), &env.current_db, &refresh)
            .expect_err("refresh should reject partition spec drift before no-op");
        assert!(
            err.contains("target partition spec changed externally"),
            "unexpected refresh error: {err}"
        );

        let target = crate::engine::mv::lifecycle::MvTarget {
            catalog: Some("ice".to_string()),
            database: "analytics".to_string(),
            name: "mv_orders".to_string(),
        };
        let plan_err =
            plan_iceberg_mv_refresh(&env.state, Some("ice"), &env.current_db, &refresh, target)
                .expect_err("plan should reject partition spec drift before no-op");
        assert!(
            plan_err
                .message
                .contains("target partition spec changed externally"),
            "unexpected plan error: {plan_err:?}"
        );
    }

    #[test]
    fn empty_join_base_noop_refresh_rejects_external_target_partition_change() {
        let env = open_test_state_with_iceberg_catalog("ice", "analytics");
        create_base_table(&env.state, "ice", "sales", "left_orders");
        create_base_table(&env.state, "ice", "sales", "right_orders");
        let stmt = parse_create_mv(
            "CREATE MATERIALIZED VIEW mv_join_orders
             DISTRIBUTED BY HASH(id) BUCKETS 1
             PROPERTIES('storage_engine'='iceberg')
             AS SELECT l.id, l.name
                FROM ice.sales.left_orders l
                JOIN ice.sales.right_orders r ON l.id = r.id",
        );
        create_iceberg_mv(&env.state, Some("ice"), &env.current_db, &stmt)
            .expect("create join iceberg mv");
        add_target_identity_partition_column(
            &env.state,
            "ice",
            "analytics",
            "mv_join_orders",
            "id",
        );

        let refresh = parse_refresh_mv("REFRESH MATERIALIZED VIEW mv_join_orders");
        let err = refresh_iceberg_mv(&env.state, Some("ice"), &env.current_db, &refresh)
            .expect_err("join refresh should reject partition spec drift before no-op");
        assert!(
            err.contains("target partition spec changed externally"),
            "unexpected join refresh error: {err}"
        );

        let target = crate::engine::mv::lifecycle::MvTarget {
            catalog: Some("ice".to_string()),
            database: "analytics".to_string(),
            name: "mv_join_orders".to_string(),
        };
        let plan_err =
            plan_iceberg_mv_refresh(&env.state, Some("ice"), &env.current_db, &refresh, target)
                .expect_err("join plan should reject partition spec drift before no-op");
        assert!(
            plan_err
                .message
                .contains("target partition spec changed externally"),
            "unexpected join plan error: {plan_err:?}"
        );
    }

    #[test]
    fn rewrite_full_refresh_select_with_pin_injects_version_as_of() {
        let env = open_test_state_with_iceberg_catalog("ice", "analytics");
        create_base_table_with_rows(&env.state, "ice", "sales", "orders", &[(1, "a")]);
        create_mv_only(&env.state, Some("ice"), &env.current_db, "mv_orders");
        let mv = find_iceberg_mv_definition(&env.state, "ice", "analytics", "mv_orders")
            .expect("mv definition");
        let base_refs =
            parse_iceberg_table_refs(&mv.base_table_refs).expect("parse base table refs");
        let [base_ref] = base_refs.as_slice() else {
            panic!("expected single base ref");
        };
        let pin = crate::connector::starrocks::managed::refresh_pin::RefreshSnapshotPin::capture(
            &env.state, &base_refs,
        )
        .expect("capture pin");
        let snapshot_id = pin.get(base_ref).expect("pinned snapshot");

        let rewritten = rewrite_full_refresh_select_with_pin(&mv.select_sql, &pin, base_ref)
            .expect("rewrite select with pin");

        assert!(rewritten.contains("VERSION AS OF"));
        assert!(rewritten.contains(&snapshot_id.to_string()));
    }

    #[test]
    fn rewrite_join_full_refresh_uses_tinyint_change_op() {
        let mut query = parse_select_query(
            "SELECT l.id FROM ice.db.left_orders AS l \
             JOIN ice.db.right_orders AS r ON l.rid = r.rid",
        );
        let left_ref = IcebergTableRef {
            catalog: "ice".to_string(),
            namespace: "db".to_string(),
            table: "left_orders".to_string(),
        };
        let right_ref = IcebergTableRef {
            catalog: "ice".to_string(),
            namespace: "db".to_string(),
            table: "right_orders".to_string(),
        };

        rewrite_join_full_refresh_query(&mut query, &left_ref, 11, &right_ref, 22, "l", "r")
            .expect("rewrite join full refresh");

        let sqlparser::ast::SetExpr::Select(select) = query.body.as_ref() else {
            panic!("expected select");
        };
        let change_item = select
            .projection
            .iter()
            .rev()
            .nth(2)
            .expect("change-op projection");
        let sqlparser::ast::SelectItem::ExprWithAlias { expr, alias } = change_item else {
            panic!("expected aliased change-op projection");
        };
        assert_eq!(alias.value, crate::exec::change_op::CHANGE_OP_COLUMN);
        assert!(matches!(
            expr,
            sqlparser::ast::Expr::Cast {
                data_type: sqlparser::ast::DataType::TinyInt(_),
                ..
            }
        ));
    }

    #[test]
    fn create_iceberg_mv_resolves_unqualified_base_in_current_catalog() {
        let env = open_test_state_with_iceberg_catalog("ice", "analytics");
        create_base_table(&env.state, "ice", "analytics", "orders");

        let stmt = parse_create_mv(
            "CREATE MATERIALIZED VIEW mv_orders
             DISTRIBUTED BY HASH(id) BUCKETS 1
             PROPERTIES('storage_engine'='iceberg')
             AS SELECT id, name FROM orders",
        );

        create_iceberg_mv(&env.state, Some("ice"), &env.current_db, &stmt)
            .expect("create iceberg mv");

        let mv = find_iceberg_mv_definition(&env.state, "ice", "analytics", "mv_orders")
            .expect("mv relationship");
        assert_eq!(mv.base_table_refs.len(), 1);
        assert_eq!(mv.base_table_refs[0], "ice.analytics.orders");
    }

    #[test]
    fn drop_iceberg_mv_drops_target_table_and_relationship() {
        let env = open_test_state_with_iceberg_catalog("ice", "analytics");
        create_base_table(&env.state, "ice", "sales", "orders");
        create_mv_only(&env.state, Some("ice"), &env.current_db, "mv_orders");

        let stmt = parse_drop_mv("DROP MATERIALIZED VIEW mv_orders");
        drop_iceberg_mv(&env.state, Some("ice"), &env.current_db, &stmt).expect("drop mv");

        let entry = {
            let catalogs = env.state.iceberg_catalogs.read().expect("iceberg catalogs");
            catalogs.get("ice").expect("catalog")
        };
        assert!(
            crate::connector::iceberg::catalog::load_table(&entry, "analytics", "mv_orders")
                .is_err()
        );
        assert!(find_iceberg_mv_definition(&env.state, "ice", "analytics", "mv_orders").is_none());
        let catalog = env.state.catalog.read().expect("standalone catalog");
        assert!(catalog.get("analytics", "mv_orders").is_err());
    }

    #[test]
    fn drop_iceberg_mv_rejects_active_refresh_before_external_drop() {
        let env = open_test_state_with_iceberg_catalog("ice", "analytics");
        create_base_table(&env.state, "ice", "sales", "orders");
        create_mv_only(&env.state, Some("ice"), &env.current_db, "mv_orders");

        let mv_id = {
            let provider = env
                .state
                .metadata_provider
                .as_ref()
                .expect("metadata provider");
            let read = provider.begin_read().expect("open read txn");
            env.state
                .mv_repo
                .find_by_target(read.as_ref(), "ice", "analytics", "mv_orders")
                .expect("find mv target")
                .expect("mv definition")
                .mv_id
        };
        {
            let provider = env
                .state
                .metadata_provider
                .as_ref()
                .expect("metadata provider");
            let mut txn = provider
                .begin_write("begin active mv refresh")
                .expect("write");
            env.state
                .mv_repo
                .begin_refresh_intent(txn.as_mut(), mv_id, std::collections::BTreeMap::new())
                .expect("begin refresh");
            txn.commit().expect("commit refresh intent");
        }

        let stmt = parse_drop_mv("DROP MATERIALIZED VIEW mv_orders");
        let err = drop_iceberg_mv(&env.state, Some("ice"), &env.current_db, &stmt)
            .expect_err("active refresh should block drop before external table drop");
        assert!(err.contains("refresh in progress"), "err={err}");

        let entry = {
            let catalogs = env.state.iceberg_catalogs.read().expect("iceberg catalogs");
            catalogs.get("ice").expect("catalog")
        };
        crate::connector::iceberg::catalog::load_table(&entry, "analytics", "mv_orders")
            .expect("target table should remain after rejected drop");
        assert!(find_iceberg_mv_definition(&env.state, "ice", "analytics", "mv_orders").is_some());
    }

    #[test]
    fn create_iceberg_mv_rejects_existing_target_table() {
        let env = open_test_state_with_iceberg_catalog("ice", "analytics");
        create_base_table(&env.state, "ice", "sales", "orders");
        create_base_table(&env.state, "ice", "analytics", "mv_orders");
        let stmt = parse_create_mv(
            "CREATE MATERIALIZED VIEW mv_orders
             DISTRIBUTED BY HASH(id) BUCKETS 1
             PROPERTIES('storage_engine'='iceberg')
             AS SELECT id, name FROM ice.sales.orders",
        );

        let err = create_iceberg_mv(&env.state, Some("ice"), &env.current_db, &stmt)
            .expect_err("existing target should fail");
        assert_eq!(
            err,
            "Iceberg MV target table ice.analytics.mv_orders already exists"
        );
    }

    #[test]
    fn create_iceberg_mv_post_create_failure_drops_target_table() {
        let env = open_test_state_with_iceberg_catalog_without_metadata("ice", "analytics");
        create_base_table(&env.state, "ice", "sales", "orders");
        let stmt = parse_create_mv(
            "CREATE MATERIALIZED VIEW mv_orders
             DISTRIBUTED BY HASH(id) BUCKETS 1
             PROPERTIES('storage_engine'='iceberg')
             AS SELECT id, name FROM ice.sales.orders",
        );

        // Dependency resolution now runs before the iceberg target table is
        // created. With no metadata provider attached to the test state, we
        // fail fast there and the iceberg target table is never created — so
        // there is nothing to clean up.
        let err = create_iceberg_mv(&env.state, Some("ice"), &env.current_db, &stmt)
            .expect_err("missing metadata provider should fail before target create");
        assert!(
            err.contains("materialized view dependency resolution requires metadata provider"),
            "err={err}"
        );

        let entry = {
            let catalogs = env.state.iceberg_catalogs.read().expect("iceberg catalogs");
            catalogs.get("ice").expect("catalog")
        };
        assert!(
            crate::connector::iceberg::catalog::load_table(&entry, "analytics", "mv_orders")
                .is_err(),
            "target table should never have been created"
        );
    }

    #[test]
    fn create_iceberg_mv_if_not_exists_does_not_adopt_existing_target() {
        let env = open_test_state_with_iceberg_catalog("ice", "analytics");
        create_base_table(&env.state, "ice", "sales", "orders");
        create_base_table(&env.state, "ice", "analytics", "mv_orders");
        register_iceberg_mv_target_in_catalog(
            &env.state,
            &IcebergMvTarget {
                catalog: "ice".to_string(),
                namespace: "analytics".to_string(),
                table: "mv_orders".to_string(),
            },
        )
        .expect("register existing target");
        let stmt = parse_create_mv(
            "CREATE MATERIALIZED VIEW IF NOT EXISTS mv_orders
             DISTRIBUTED BY HASH(id) BUCKETS 1
             PROPERTIES('storage_engine'='iceberg')
             AS SELECT id, name FROM ice.sales.orders",
        );

        let err =
            crate::engine::mv_flow::create_mv(&env.state, Some("ice"), &env.current_db, &stmt)
                .expect_err("existing target should fail even with IF NOT EXISTS");
        assert_eq!(
            err,
            "Iceberg MV target table ice.analytics.mv_orders already exists"
        );
    }

    #[test]
    fn create_iceberg_mv_requires_current_iceberg_catalog() {
        let env = open_test_state_with_iceberg_catalog("ice", "analytics");
        let stmt = parse_create_mv(
            "CREATE MATERIALIZED VIEW mv_orders
             DISTRIBUTED BY HASH(id) BUCKETS 1
             PROPERTIES('storage_engine'='iceberg')
             AS SELECT id, name FROM ice.sales.orders",
        );

        for current_catalog in [None, Some("default_catalog")] {
            let err = create_iceberg_mv(&env.state, current_catalog, &env.current_db, &stmt)
                .expect_err("non-iceberg catalog should fail");
            assert_eq!(
                err,
                "storage_engine='iceberg' requires current catalog to be an Iceberg catalog"
            );
        }
    }

    #[test]
    fn plan_iceberg_mv_refresh_reports_append_insert_affected_partitions() {
        let env = open_test_state_with_iceberg_catalog("ice", "analytics");
        create_identity_partitioned_base_table(&env.state, "ice", "sales", "orders");
        insert_into_iceberg_table(&env.state, "ice", "sales", "orders", &[(1, "a")]);
        let stmt = parse_create_mv(
            "CREATE MATERIALIZED VIEW mv_orders
             PARTITION BY (id)
             DISTRIBUTED BY HASH(id) BUCKETS 1
             PROPERTIES('storage_engine'='iceberg')
             AS SELECT id, name FROM ice.sales.orders",
        );
        create_iceberg_mv(&env.state, Some("ice"), &env.current_db, &stmt)
            .expect("create partitioned iceberg mv");

        let refresh = parse_refresh_mv("REFRESH MATERIALIZED VIEW mv_orders");
        refresh_iceberg_mv(&env.state, Some("ice"), &env.current_db, &refresh)
            .expect("first refresh");
        insert_into_iceberg_table(&env.state, "ice", "sales", "orders", &[(2, "b")]);

        let target = crate::engine::mv::lifecycle::MvTarget {
            catalog: Some("ice".to_string()),
            database: "analytics".to_string(),
            name: "mv_orders".to_string(),
        };
        let plan =
            plan_iceberg_mv_refresh(&env.state, Some("ice"), &env.current_db, &refresh, target)
                .expect("second refresh plan");

        let crate::engine::mv::partition::AffectedMvPartitions::Known {
            new_partitions,
            old_partitions,
        } = plan.affected_partitions
        else {
            panic!(
                "expected known affected partitions: {:?}",
                plan.affected_partitions
            );
        };
        let entry = {
            let catalogs = env.state.iceberg_catalogs.read().expect("iceberg catalogs");
            catalogs.get("ice").expect("catalog")
        };
        let target_spec_id =
            crate::connector::iceberg::catalog::load_table(&entry, "analytics", "mv_orders")
                .expect("load target table")
                .table
                .metadata()
                .default_partition_spec()
                .spec_id();
        let expected_partition = crate::engine::mv::partition::MvPartitionKey::new(
            target_spec_id,
            vec![crate::engine::mv::partition::MvPartitionKeyField::new(
                "id".to_string(),
                crate::engine::mv::partition::MvPartitionValue::String("2".to_string()),
            )],
        );
        assert_eq!(
            new_partitions.into_iter().collect::<Vec<_>>(),
            vec![expected_partition]
        );
        assert!(old_partitions.is_empty());
    }

    #[test]
    fn plan_iceberg_mv_refresh_reports_unpartitioned_for_unpartitioned_mv() {
        let env = open_test_state_with_iceberg_catalog("ice", "analytics");
        create_base_table_with_rows(&env.state, "ice", "sales", "orders", &[(1, "a")]);
        create_mv_and_refresh_once(&env.state, Some("ice"), &env.current_db, "mv_orders");
        insert_into_iceberg_table(&env.state, "ice", "sales", "orders", &[(2, "b")]);

        let refresh = parse_refresh_mv("REFRESH MATERIALIZED VIEW mv_orders");
        let target = crate::engine::mv::lifecycle::MvTarget {
            catalog: Some("ice".to_string()),
            database: "analytics".to_string(),
            name: "mv_orders".to_string(),
        };
        let plan =
            plan_iceberg_mv_refresh(&env.state, Some("ice"), &env.current_db, &refresh, target)
                .expect("unpartitioned refresh plan");

        assert_eq!(
            plan.affected_partitions,
            crate::engine::mv::partition::AffectedMvPartitions::Unpartitioned
        );
    }

    #[test]
    fn plan_iceberg_mv_refresh_reports_unknown_for_join_mv() {
        let env = open_test_state_with_iceberg_catalog("ice", "analytics");
        create_base_table(&env.state, "ice", "sales", "left_orders");
        create_base_table(&env.state, "ice", "sales", "right_orders");
        let stmt = parse_create_mv(
            "CREATE MATERIALIZED VIEW mv_join_orders
             DISTRIBUTED BY HASH(id) BUCKETS 1
             PROPERTIES('storage_engine'='iceberg')
             AS SELECT l.id, l.name
                FROM ice.sales.left_orders l
                JOIN ice.sales.right_orders r ON l.id = r.id",
        );
        create_iceberg_mv(&env.state, Some("ice"), &env.current_db, &stmt)
            .expect("create join iceberg mv");

        let refresh = parse_refresh_mv("REFRESH MATERIALIZED VIEW mv_join_orders");
        let target = crate::engine::mv::lifecycle::MvTarget {
            catalog: Some("ice".to_string()),
            database: "analytics".to_string(),
            name: "mv_join_orders".to_string(),
        };
        let plan =
            plan_iceberg_mv_refresh(&env.state, Some("ice"), &env.current_db, &refresh, target)
                .expect("join refresh plan");

        assert_eq!(
            plan.affected_partitions.unknown_reason(),
            Some("join MV affected partition planning is not implemented")
        );
    }

    #[test]
    fn plan_iceberg_mv_refresh_requires_a11_schema_contract() {
        let env = open_test_state_with_iceberg_catalog("ice", "analytics");
        create_base_table(&env.state, "ice", "sales", "orders");
        create_base_table(&env.state, "ice", "analytics", "mv_orders");
        let provider = env
            .state
            .metadata_provider
            .as_ref()
            .expect("metadata provider");
        let mut txn = provider
            .begin_write("seed iceberg mv without schema contract")
            .expect("write txn");
        env.state
            .mv_repo
            .create_definition(
                txn.as_mut(),
                CreateMvDefinitionRequest {
                    select_sql: "SELECT id, name FROM ice.sales.orders".to_string(),
                    base_table_refs: vec!["ice.sales.orders".to_string()],
                    primary_key_columns: Vec::new(),
                    storage_engine: ManagedMvStorageEngine::Iceberg.as_sql_str().to_string(),
                    target_catalog: Some("ice".to_string()),
                    target_namespace: Some("analytics".to_string()),
                    target_table: Some("mv_orders".to_string()),
                    schema_contract: None,
                    partition_spec: None,
                    created_at_ms: now_ms(),
                },
            )
            .expect("create mv definition");
        txn.commit().expect("commit mv definition");

        let stmt = parse_refresh_mv("REFRESH MATERIALIZED VIEW mv_orders");
        let target = crate::engine::mv::lifecycle::MvTarget {
            catalog: Some("ice".to_string()),
            database: "analytics".to_string(),
            name: "mv_orders".to_string(),
        };
        let err = plan_iceberg_mv_refresh(&env.state, Some("ice"), &env.current_db, &stmt, target)
            .expect_err("missing schema contract should fail");

        assert!(
            err.to_string().contains("missing A11 schema contract"),
            "{err}"
        );
    }

    #[test]
    fn refresh_iceberg_mv_fails_when_target_snapshot_was_modified_externally() {
        let env = open_test_state_with_iceberg_catalog("ice", "analytics");
        create_base_table_with_rows(&env.state, "ice", "sales", "orders", &[(1, "a")]);
        create_mv_and_refresh_once(&env.state, Some("ice"), &env.current_db, "mv_orders");

        insert_into_iceberg_table(
            &env.state,
            "ice",
            "analytics",
            "mv_orders",
            &[(99, "external")],
        );

        let stmt = parse_refresh_mv("REFRESH MATERIALIZED VIEW mv_orders");
        let err = refresh_iceberg_mv(&env.state, Some("ice"), &env.current_db, &stmt)
            .expect_err("external target write must fail");
        assert!(
            err.contains("target table ice.analytics.mv_orders was modified outside NovaRocks"),
            "{err}"
        );
    }

    #[test]
    fn refresh_iceberg_mv_second_write_refresh_publishes_and_drops_staging_branch() {
        let env = open_test_state_with_hadoop_iceberg_catalog("ice", "analytics");
        create_base_table_with_rows(&env.state, "ice", "sales", "orders", &[(1, "a")]);
        create_mv_and_refresh_once(&env.state, Some("ice"), &env.current_db, "mv_orders");

        let entry = {
            let catalogs = env.state.iceberg_catalogs.read().expect("iceberg catalogs");
            catalogs.get("ice").expect("catalog")
        };
        let first_snapshot =
            crate::connector::iceberg::catalog::load_table(&entry, "analytics", "mv_orders")
                .expect("load target after first refresh")
                .table
                .metadata()
                .current_snapshot()
                .map(|s| s.snapshot_id())
                .expect("first target snapshot");

        insert_into_iceberg_table(&env.state, "ice", "sales", "orders", &[(2, "b")]);
        let refresh = parse_refresh_mv("REFRESH MATERIALIZED VIEW mv_orders");
        refresh_iceberg_mv(&env.state, Some("ice"), &env.current_db, &refresh)
            .expect("second write-bearing refresh");

        let loaded =
            crate::connector::iceberg::catalog::load_table(&entry, "analytics", "mv_orders")
                .expect("load target after second refresh");
        let second_snapshot = loaded
            .table
            .metadata()
            .current_snapshot()
            .map(|s| s.snapshot_id())
            .expect("second target snapshot");
        assert_ne!(second_snapshot, first_snapshot);
        assert!(
            !loaded
                .table
                .metadata()
                .refs()
                .keys()
                .any(|name| name.starts_with("__nova_mv_refresh_")),
            "staging branch must be dropped after publish"
        );

        let mv = find_iceberg_mv_definition(&env.state, "ice", "analytics", "mv_orders")
            .expect("mv definition after second refresh");
        assert_eq!(mv.last_refreshed_iceberg_snapshot_id, Some(second_snapshot));
        assert_eq!(mv.last_refresh_rows, Some(2));
    }

    #[test]
    fn incremental_empty_delta_refresh_uses_metadata_only_intent() {
        let env = open_test_state_with_hadoop_iceberg_catalog("ice", "analytics");
        create_base_table_with_rows(&env.state, "ice", "sales", "orders", &[(20, "hit")]);
        create_mv_with_select_only(
            &env.state,
            Some("ice"),
            &env.current_db,
            "mv_orders",
            "SELECT id, name FROM ice.sales.orders WHERE id > 10",
        );
        let refresh = parse_refresh_mv("REFRESH MATERIALIZED VIEW mv_orders");
        refresh_iceberg_mv(&env.state, Some("ice"), &env.current_db, &refresh)
            .expect("first refresh");

        insert_into_iceberg_table(&env.state, "ice", "sales", "orders", &[(1, "miss")]);
        refresh_iceberg_mv(&env.state, Some("ice"), &env.current_db, &refresh)
            .expect("empty-delta incremental refresh");

        let refreshes = load_all_mv_refreshes(&env.state);
        let second_refresh = refreshes.last().expect("second refresh");
        assert_eq!(second_refresh.state, MvRefreshState::Finalized);
        assert_eq!(second_refresh.target_catalog, None);
        assert_eq!(second_refresh.target_namespace, None);
        assert_eq!(second_refresh.target_table, None);
        assert_eq!(second_refresh.staging_branch, None);
        assert_eq!(second_refresh.marker, None);

        let provider = env
            .state
            .metadata_provider
            .as_ref()
            .expect("metadata provider");
        let read = provider.begin_read().expect("read txn");
        let unfinished = env
            .state
            .mv_repo
            .list_unfinished_branch_staged_iceberg_refreshes(read.as_ref())
            .expect("branch staged scan");
        assert!(unfinished.is_empty());
    }

    #[test]
    fn recover_staging_committed_refresh_aborts_when_main_not_advanced() {
        let env = open_test_state_with_hadoop_iceberg_catalog("ice", "analytics");
        create_base_table(&env.state, "ice", "sales", "orders");
        create_mv_only(&env.state, Some("ice"), &env.current_db, "mv_orders");
        let mv = find_iceberg_mv_definition(&env.state, "ice", "analytics", "mv_orders")
            .expect("mv definition");
        let target = IcebergMvTarget {
            catalog: "ice".to_string(),
            namespace: "analytics".to_string(),
            table: "mv_orders".to_string(),
        };
        let entry = {
            let catalogs = env.state.iceberg_catalogs.read().expect("iceberg catalogs");
            catalogs.get("ice").expect("catalog")
        };
        let catalog = crate::connector::iceberg::catalog::registry::build_iceberg_catalog(&entry)
            .expect("catalog");
        let staging_branch = "__nova_mv_refresh_recover_staging";
        let refresh_id = begin_staged_iceberg_mv_refresh_intent(
            &env.state,
            &target,
            mv.mv_id,
            None,
            BTreeMap::new(),
            staging_branch,
        )
        .expect("begin staged refresh");
        let marker = load_iceberg_mv_refresh_marker(&env.state, refresh_id, mv.mv_id)
            .expect("marker")
            .to_summary_properties();

        let staging_snapshot = data_block_on(async {
            let ident = iceberg_mv_table_ident(&target).expect("ident");
            let table = catalog.load_table(&ident).await.expect("load target");
            let chunks = id_name_chunk(&[(1, "a")]);
            let written = write_chunks_as_iceberg_data_files(&table, &chunks)
                .await
                .expect("write chunks");
            commit_iceberg_mv_target_files_with_ref(
                &table,
                &catalog,
                &entry,
                &ident,
                CommitOpKind::FastAppend,
                written,
                staging_branch,
                marker,
            )
            .await
            .expect("commit staging")
            .new_snapshot_id
        })
        .expect("runtime");
        record_iceberg_mv_staging_commit(
            &env.state,
            refresh_id,
            staging_snapshot,
            1,
            BTreeMap::new(),
        )
        .expect("record staging");

        recover_iceberg_mv_refreshes(&env.state).expect("recover refresh");

        let provider = env
            .state
            .metadata_provider
            .as_ref()
            .expect("metadata provider");
        let read = provider.begin_read().expect("read txn");
        let refresh = env
            .state
            .mv_repo
            .load_refresh(read.as_ref(), refresh_id)
            .expect("load refresh")
            .expect("refresh");
        assert_eq!(refresh.state, MvRefreshState::Aborted);
        let definition = env
            .state
            .mv_repo
            .find_by_target(read.as_ref(), "ice", "analytics", "mv_orders")
            .expect("find mv")
            .expect("mv definition");
        assert_eq!(definition.active_refresh_id, None);
        assert!(!definition.refresh_in_progress);
        drop(read);

        let reloaded =
            crate::connector::iceberg::catalog::load_table(&entry, "analytics", "mv_orders")
                .expect("reload target");
        assert_eq!(
            reloaded
                .table
                .metadata()
                .current_snapshot()
                .map(|s| s.snapshot_id()),
            None
        );
        assert!(
            !reloaded
                .table
                .metadata()
                .refs()
                .contains_key(staging_branch),
            "staging branch should be dropped"
        );
    }

    #[test]
    fn recover_staging_committed_refresh_finalizes_when_main_already_advanced() {
        let env = open_test_state_with_hadoop_iceberg_catalog("ice", "analytics");
        create_base_table(&env.state, "ice", "sales", "orders");
        create_mv_only(&env.state, Some("ice"), &env.current_db, "mv_orders");
        let mv = find_iceberg_mv_definition(&env.state, "ice", "analytics", "mv_orders")
            .expect("mv definition");
        let target = IcebergMvTarget {
            catalog: "ice".to_string(),
            namespace: "analytics".to_string(),
            table: "mv_orders".to_string(),
        };
        let entry = {
            let catalogs = env.state.iceberg_catalogs.read().expect("iceberg catalogs");
            catalogs.get("ice").expect("catalog")
        };
        let catalog = crate::connector::iceberg::catalog::registry::build_iceberg_catalog(&entry)
            .expect("catalog");
        let staging_branch = "__nova_mv_refresh_recover_publish";
        let refresh_id = begin_staged_iceberg_mv_refresh_intent(
            &env.state,
            &target,
            mv.mv_id,
            None,
            BTreeMap::new(),
            staging_branch,
        )
        .expect("begin staged refresh");
        let marker = load_iceberg_mv_refresh_marker(&env.state, refresh_id, mv.mv_id)
            .expect("marker")
            .to_summary_properties();

        let staging_snapshot = data_block_on(async {
            let ident = iceberg_mv_table_ident(&target).expect("ident");
            let table = catalog.load_table(&ident).await.expect("load target");
            let chunks = id_name_chunk(&[(1, "a")]);
            let written = write_chunks_as_iceberg_data_files(&table, &chunks)
                .await
                .expect("write chunks");
            commit_iceberg_mv_target_files_with_ref(
                &table,
                &catalog,
                &entry,
                &ident,
                CommitOpKind::FastAppend,
                written,
                staging_branch,
                marker,
            )
            .await
            .expect("commit staging")
            .new_snapshot_id
        })
        .expect("runtime");
        record_iceberg_mv_staging_commit(
            &env.state,
            refresh_id,
            staging_snapshot,
            1,
            BTreeMap::new(),
        )
        .expect("record staging");
        let published_snapshot = publish_iceberg_mv_refresh(
            &env.state,
            &target,
            &entry,
            staging_branch,
            None,
            staging_snapshot,
            refresh_id,
            mv.mv_id,
        )
        .expect("publish staging");

        recover_iceberg_mv_refreshes(&env.state).expect("recover refresh");

        let provider = env
            .state
            .metadata_provider
            .as_ref()
            .expect("metadata provider");
        let read = provider.begin_read().expect("read txn");
        let refresh = env
            .state
            .mv_repo
            .load_refresh(read.as_ref(), refresh_id)
            .expect("load refresh")
            .expect("refresh");
        assert_eq!(refresh.state, MvRefreshState::Finalized);
        assert_eq!(refresh.published_snapshot_id, Some(published_snapshot));
        let definition = env
            .state
            .mv_repo
            .find_by_target(read.as_ref(), "ice", "analytics", "mv_orders")
            .expect("find mv")
            .expect("mv definition");
        assert_eq!(
            definition.last_refreshed_iceberg_snapshot_id,
            Some(published_snapshot)
        );
        assert_eq!(definition.active_refresh_id, None);
        assert!(!definition.refresh_in_progress);
        drop(read);

        let reloaded =
            crate::connector::iceberg::catalog::load_table(&entry, "analytics", "mv_orders")
                .expect("reload target");
        assert_eq!(
            reloaded
                .table
                .metadata()
                .current_snapshot()
                .map(|s| s.snapshot_id()),
            Some(published_snapshot)
        );
        assert!(
            !reloaded
                .table
                .metadata()
                .refs()
                .contains_key(staging_branch),
            "staging branch should be dropped"
        );
    }

    #[test]
    fn recover_staging_committed_refresh_aborts_when_staging_already_missing() {
        let env = open_test_state_with_hadoop_iceberg_catalog("ice", "analytics");
        create_base_table(&env.state, "ice", "sales", "orders");
        create_mv_only(&env.state, Some("ice"), &env.current_db, "mv_orders");
        let mv = find_iceberg_mv_definition(&env.state, "ice", "analytics", "mv_orders")
            .expect("mv definition");
        let target = IcebergMvTarget {
            catalog: "ice".to_string(),
            namespace: "analytics".to_string(),
            table: "mv_orders".to_string(),
        };
        let entry = {
            let catalogs = env.state.iceberg_catalogs.read().expect("iceberg catalogs");
            catalogs.get("ice").expect("catalog")
        };
        let catalog = crate::connector::iceberg::catalog::registry::build_iceberg_catalog(&entry)
            .expect("catalog");
        let staging_branch = "__nova_mv_refresh_recover_missing_staging";
        let refresh_id = begin_staged_iceberg_mv_refresh_intent(
            &env.state,
            &target,
            mv.mv_id,
            None,
            BTreeMap::new(),
            staging_branch,
        )
        .expect("begin staged refresh");
        let marker = load_iceberg_mv_refresh_marker(&env.state, refresh_id, mv.mv_id)
            .expect("marker")
            .to_summary_properties();

        let staging_snapshot = data_block_on(async {
            let ident = iceberg_mv_table_ident(&target).expect("ident");
            let table = catalog.load_table(&ident).await.expect("load target");
            let chunks = id_name_chunk(&[(1, "a")]);
            let written = write_chunks_as_iceberg_data_files(&table, &chunks)
                .await
                .expect("write chunks");
            commit_iceberg_mv_target_files_with_ref(
                &table,
                &catalog,
                &entry,
                &ident,
                CommitOpKind::FastAppend,
                written,
                staging_branch,
                marker,
            )
            .await
            .expect("commit staging")
            .new_snapshot_id
        })
        .expect("runtime");
        record_iceberg_mv_staging_commit(
            &env.state,
            refresh_id,
            staging_snapshot,
            1,
            BTreeMap::new(),
        )
        .expect("record staging");
        drop_iceberg_mv_staging_branch(&env.state, &target, &entry, staging_branch)
            .expect("drop staging before metadata abort");

        recover_iceberg_mv_refreshes(&env.state).expect("recover refresh");

        let provider = env
            .state
            .metadata_provider
            .as_ref()
            .expect("metadata provider");
        let read = provider.begin_read().expect("read txn");
        let refresh = env
            .state
            .mv_repo
            .load_refresh(read.as_ref(), refresh_id)
            .expect("load refresh")
            .expect("refresh");
        assert_eq!(refresh.state, MvRefreshState::Aborted);
        let definition = env
            .state
            .mv_repo
            .find_by_target(read.as_ref(), "ice", "analytics", "mv_orders")
            .expect("find mv")
            .expect("mv definition");
        assert_eq!(definition.active_refresh_id, None);
        assert!(!definition.refresh_in_progress);
    }

    #[test]
    fn recover_publish_committed_refresh_drops_branch_before_finalize() {
        let env = open_test_state_with_hadoop_iceberg_catalog("ice", "analytics");
        create_base_table(&env.state, "ice", "sales", "orders");
        create_mv_only(&env.state, Some("ice"), &env.current_db, "mv_orders");
        let mv = find_iceberg_mv_definition(&env.state, "ice", "analytics", "mv_orders")
            .expect("mv definition");
        let target = IcebergMvTarget {
            catalog: "ice".to_string(),
            namespace: "analytics".to_string(),
            table: "mv_orders".to_string(),
        };
        let entry = {
            let catalogs = env.state.iceberg_catalogs.read().expect("iceberg catalogs");
            catalogs.get("ice").expect("catalog")
        };
        let catalog = crate::connector::iceberg::catalog::registry::build_iceberg_catalog(&entry)
            .expect("catalog");
        let staging_branch = "__nova_mv_refresh_recover_publish_committed";
        let refresh_id = begin_staged_iceberg_mv_refresh_intent(
            &env.state,
            &target,
            mv.mv_id,
            None,
            BTreeMap::new(),
            staging_branch,
        )
        .expect("begin staged refresh");
        let marker = load_iceberg_mv_refresh_marker(&env.state, refresh_id, mv.mv_id)
            .expect("marker")
            .to_summary_properties();

        let staging_snapshot = data_block_on(async {
            let ident = iceberg_mv_table_ident(&target).expect("ident");
            let table = catalog.load_table(&ident).await.expect("load target");
            let chunks = id_name_chunk(&[(1, "a")]);
            let written = write_chunks_as_iceberg_data_files(&table, &chunks)
                .await
                .expect("write chunks");
            commit_iceberg_mv_target_files_with_ref(
                &table,
                &catalog,
                &entry,
                &ident,
                CommitOpKind::FastAppend,
                written,
                staging_branch,
                marker,
            )
            .await
            .expect("commit staging")
            .new_snapshot_id
        })
        .expect("runtime");
        record_iceberg_mv_staging_commit(
            &env.state,
            refresh_id,
            staging_snapshot,
            1,
            BTreeMap::new(),
        )
        .expect("record staging");
        let published_snapshot = publish_iceberg_mv_refresh(
            &env.state,
            &target,
            &entry,
            staging_branch,
            None,
            staging_snapshot,
            refresh_id,
            mv.mv_id,
        )
        .expect("publish staging");
        record_iceberg_mv_publish_commit(&env.state, refresh_id, published_snapshot)
            .expect("record publish");

        recover_iceberg_mv_refreshes(&env.state).expect("recover refresh");

        let provider = env
            .state
            .metadata_provider
            .as_ref()
            .expect("metadata provider");
        let read = provider.begin_read().expect("read txn");
        let refresh = env
            .state
            .mv_repo
            .load_refresh(read.as_ref(), refresh_id)
            .expect("load refresh")
            .expect("refresh");
        assert_eq!(refresh.state, MvRefreshState::Finalized);
        drop(read);

        let reloaded =
            crate::connector::iceberg::catalog::load_table(&entry, "analytics", "mv_orders")
                .expect("reload target");
        assert_eq!(
            reloaded
                .table
                .metadata()
                .current_snapshot()
                .map(|s| s.snapshot_id()),
            Some(published_snapshot)
        );
        assert!(
            !reloaded
                .table
                .metadata()
                .refs()
                .contains_key(staging_branch),
            "staging branch should be dropped before finalize"
        );
    }

    #[test]
    fn recover_publish_committed_refresh_finalizes_when_branch_already_missing() {
        let env = open_test_state_with_hadoop_iceberg_catalog("ice", "analytics");
        create_base_table(&env.state, "ice", "sales", "orders");
        create_mv_only(&env.state, Some("ice"), &env.current_db, "mv_orders");
        let mv = find_iceberg_mv_definition(&env.state, "ice", "analytics", "mv_orders")
            .expect("mv definition");
        let target = IcebergMvTarget {
            catalog: "ice".to_string(),
            namespace: "analytics".to_string(),
            table: "mv_orders".to_string(),
        };
        let entry = {
            let catalogs = env.state.iceberg_catalogs.read().expect("iceberg catalogs");
            catalogs.get("ice").expect("catalog")
        };
        let catalog = crate::connector::iceberg::catalog::registry::build_iceberg_catalog(&entry)
            .expect("catalog");
        let staging_branch = "__nova_mv_refresh_recover_publish_missing_branch";
        let refresh_id = begin_staged_iceberg_mv_refresh_intent(
            &env.state,
            &target,
            mv.mv_id,
            None,
            BTreeMap::new(),
            staging_branch,
        )
        .expect("begin staged refresh");
        let marker = load_iceberg_mv_refresh_marker(&env.state, refresh_id, mv.mv_id)
            .expect("marker")
            .to_summary_properties();

        let staging_snapshot = data_block_on(async {
            let ident = iceberg_mv_table_ident(&target).expect("ident");
            let table = catalog.load_table(&ident).await.expect("load target");
            let chunks = id_name_chunk(&[(1, "a")]);
            let written = write_chunks_as_iceberg_data_files(&table, &chunks)
                .await
                .expect("write chunks");
            commit_iceberg_mv_target_files_with_ref(
                &table,
                &catalog,
                &entry,
                &ident,
                CommitOpKind::FastAppend,
                written,
                staging_branch,
                marker,
            )
            .await
            .expect("commit staging")
            .new_snapshot_id
        })
        .expect("runtime");
        record_iceberg_mv_staging_commit(
            &env.state,
            refresh_id,
            staging_snapshot,
            1,
            BTreeMap::new(),
        )
        .expect("record staging");
        let published_snapshot = publish_iceberg_mv_refresh(
            &env.state,
            &target,
            &entry,
            staging_branch,
            None,
            staging_snapshot,
            refresh_id,
            mv.mv_id,
        )
        .expect("publish staging");
        record_iceberg_mv_publish_commit(&env.state, refresh_id, published_snapshot)
            .expect("record publish");
        drop_iceberg_mv_staging_branch(&env.state, &target, &entry, staging_branch)
            .expect("drop staging before finalize");

        recover_iceberg_mv_refreshes(&env.state).expect("recover refresh");

        let provider = env
            .state
            .metadata_provider
            .as_ref()
            .expect("metadata provider");
        let read = provider.begin_read().expect("read txn");
        let refresh = env
            .state
            .mv_repo
            .load_refresh(read.as_ref(), refresh_id)
            .expect("load refresh")
            .expect("refresh");
        assert_eq!(refresh.state, MvRefreshState::Finalized);
    }

    #[test]
    fn iceberg_mv_commit_unknown_marker_preserves_active_refresh() {
        let env = open_test_state_with_iceberg_catalog("ice", "analytics");
        create_base_table(&env.state, "ice", "sales", "orders");
        create_mv_only(&env.state, Some("ice"), &env.current_db, "mv_orders");
        let mv = find_iceberg_mv_definition(&env.state, "ice", "analytics", "mv_orders")
            .expect("mv definition");
        let target = IcebergMvTarget {
            catalog: "ice".to_string(),
            namespace: "analytics".to_string(),
            table: "mv_orders".to_string(),
        };
        let refresh_id = begin_staged_iceberg_mv_refresh_intent(
            &env.state,
            &target,
            mv.mv_id,
            None,
            BTreeMap::new(),
            "__nova_mv_refresh_test_unknown",
        )
        .expect("begin staged refresh");

        mark_iceberg_mv_refresh_commit_unknown(&env.state, refresh_id)
            .expect("mark commit unknown");

        let provider = env
            .state
            .metadata_provider
            .as_ref()
            .expect("metadata provider");
        let read = provider.begin_read().expect("read txn");
        let refresh = env
            .state
            .mv_repo
            .load_refresh(read.as_ref(), refresh_id)
            .expect("load refresh")
            .expect("refresh");
        assert_eq!(refresh.state, MvRefreshState::CommitUnknown);
        let definition = env
            .state
            .mv_repo
            .find_by_target(read.as_ref(), "ice", "analytics", "mv_orders")
            .expect("find mv")
            .expect("mv definition");
        assert_eq!(definition.active_refresh_id, Some(refresh_id));
        assert!(definition.refresh_in_progress);
    }

    #[test]
    fn recover_iceberg_mv_refresh_marks_unknown_when_main_changed_externally() {
        let env = open_test_state_with_iceberg_catalog("ice", "analytics");
        create_base_table(&env.state, "ice", "sales", "orders");
        create_mv_only(&env.state, Some("ice"), &env.current_db, "mv_orders");
        seed_active_staging_refresh(&env.state, "ice", "analytics", "mv_orders", false);
        advance_target_main_without_refresh_marker(&env.state, "ice", "analytics", "mv_orders");

        recover_iceberg_mv_refreshes(&env.state).expect("recover");

        let provider = env.state.metadata_provider.as_ref().expect("provider");
        let read = provider.begin_read().expect("read");
        let unfinished = env
            .state
            .mv_repo
            .list_unfinished_refreshes(read.as_ref())
            .expect("unfinished");
        assert_eq!(unfinished.len(), 1);
        assert_eq!(unfinished[0].state, MvRefreshState::CommitUnknown);
    }

    #[test]
    fn build_iceberg_schema_maps_int_bigint_string() {
        let cols = vec![
            output_col("k", DataType::Int32, false),
            output_col("v", DataType::Int64, true),
            output_col("s", DataType::Utf8, true),
        ];
        let schema = build_iceberg_schema_from_outputs(&cols).expect("schema");
        assert_eq!(schema.as_struct().fields().len(), 3);
        assert_eq!(schema.as_struct().fields()[0].name, "k");
        assert!(schema.as_struct().fields()[0].required);
        assert_eq!(schema.as_struct().fields()[1].name, "v");
        assert!(!schema.as_struct().fields()[1].required);
        assert_eq!(schema.as_struct().fields()[2].name, "s");
        assert!(!schema.as_struct().fields()[2].required);
    }

    #[test]
    fn arrow_data_type_to_iceberg_rejects_unsupported_types() {
        let err = arrow_data_type_to_iceberg_primitive(&DataType::Map(
            std::sync::Arc::new(arrow::datatypes::Field::new(
                "entries",
                DataType::Struct(arrow::datatypes::Fields::empty()),
                false,
            )),
            false,
        ))
        .unwrap_err();
        assert!(err.to_lowercase().contains("unsupported"));
    }

    #[test]
    fn arrow_decimal_negative_scale_is_rejected() {
        let err = arrow_data_type_to_iceberg_primitive(&DataType::Decimal128(10, -2)).unwrap_err();
        assert!(err.contains("negative scale"));
    }

    #[test]
    fn arrow_int8_int16_promote_to_iceberg_int() {
        use iceberg::spec::PrimitiveType;
        assert_eq!(
            arrow_data_type_to_iceberg_primitive(&DataType::Int8).unwrap(),
            PrimitiveType::Int
        );
        assert_eq!(
            arrow_data_type_to_iceberg_primitive(&DataType::Int16).unwrap(),
            PrimitiveType::Int
        );
    }

    #[test]
    fn arrow_decimal256_is_rejected() {
        let err = arrow_data_type_to_iceberg_primitive(&DataType::Decimal256(40, 2)).unwrap_err();
        assert!(err.contains("Decimal256"));
    }

    #[test]
    fn arrow_fixed_size_binary_16_is_rejected() {
        let err = arrow_data_type_to_iceberg_primitive(&DataType::FixedSizeBinary(16)).unwrap_err();
        assert!(err.contains("LARGEINT"));
    }

    /// End-to-end round-trip: write chunks to a local iceberg table and commit
    /// a fast-append snapshot, then verify the snapshot is current after reload.
    #[test]
    fn write_chunks_round_trip_through_iceberg_table() {
        use crate::connector::iceberg::catalog::registry::{
            build_catalog_entry, build_iceberg_catalog,
        };

        let dir = tempfile::tempdir().expect("tempdir");
        let warehouse = format!("file://{}/wh", dir.path().display());
        let entry = build_catalog_entry(
            "ice",
            &[
                ("type".to_string(), "iceberg".to_string()),
                ("iceberg.catalog.type".to_string(), "hadoop".to_string()),
                ("iceberg.catalog.warehouse".to_string(), warehouse.clone()),
            ],
        )
        .expect("catalog entry");
        let catalog = build_iceberg_catalog(&entry).expect("catalog");

        let runtime = tokio::runtime::Runtime::new().unwrap();
        runtime.block_on(async {
            let ns = iceberg::NamespaceIdent::from_strs(["test_ns"]).unwrap();
            catalog
                .create_namespace(&ns, std::collections::HashMap::new())
                .await
                .unwrap();

            let schema = iceberg::spec::Schema::builder()
                .with_fields(vec![
                    StdArc::new(iceberg::spec::NestedField::required(
                        1,
                        "k",
                        iceberg::spec::Type::Primitive(iceberg::spec::PrimitiveType::Int),
                    )),
                    StdArc::new(iceberg::spec::NestedField::optional(
                        2,
                        "v",
                        iceberg::spec::Type::Primitive(iceberg::spec::PrimitiveType::Long),
                    )),
                ])
                .build()
                .unwrap();

            let creation = iceberg::TableCreation::builder()
                .name("t".to_string())
                .schema(schema)
                .build();
            let table = catalog.create_table(&ns, creation).await.unwrap();

            // Build a RecordBatch and wrap it in a minimal Chunk.
            let arrow_schema = StdArc::new(ArrowSchema::new(vec![
                Field::new("k", DataType::Int32, false),
                Field::new("v", DataType::Int64, true),
            ]));
            let batch = RecordBatch::try_new(
                arrow_schema.clone(),
                vec![
                    StdArc::new(Int32Array::from(vec![1, 2, 3])),
                    StdArc::new(Int64Array::from(vec![Some(10), Some(20), None])),
                ],
            )
            .unwrap();

            // Build a Chunk by deriving ChunkSchema from the RecordBatch arrow schema
            // and synthetic slot ids.
            use crate::common::ids::SlotId;
            use crate::exec::chunk::ChunkSchema;
            let chunk_schema_ref = ChunkSchema::try_ref_from_schema_and_slot_ids(
                &arrow_schema,
                &[SlotId(0), SlotId(1)],
            )
            .expect("chunk schema");
            let chunk = crate::exec::chunk::Chunk::new_with_chunk_schema(batch, chunk_schema_ref);

            let written = write_chunks_as_iceberg_data_files(&table, &[chunk])
                .await
                .unwrap();
            assert!(
                !written.is_empty(),
                "at least one data file should be written"
            );

            let ident = TableIdent::from_strs(["test_ns", "t"]).unwrap();
            let snapshot_id = commit_iceberg_mv_target_files(
                &table,
                &catalog,
                &entry,
                &ident,
                CommitOpKind::FastAppend,
                written,
            )
            .await
            .unwrap()
            .new_snapshot_id;
            assert!(snapshot_id != 0, "snapshot id must be non-zero");

            // Reload from catalog and confirm snapshot matches.
            let reloaded = catalog.load_table(&ident).await.unwrap();
            assert_eq!(
                reloaded
                    .metadata()
                    .current_snapshot()
                    .map(|s| s.snapshot_id()),
                Some(snapshot_id),
            );
        });
    }

    #[test]
    fn iceberg_mv_commit_to_staging_branch_does_not_move_main() {
        use crate::connector::iceberg::catalog::registry::{
            build_catalog_entry, build_iceberg_catalog,
        };
        use crate::connector::iceberg::commit::MvRefreshSnapshotMarker;

        let dir = tempfile::tempdir().expect("tempdir");
        let warehouse = format!("file://{}/wh", dir.path().display());
        let entry = build_catalog_entry(
            "ice",
            &[
                ("type".to_string(), "iceberg".to_string()),
                ("iceberg.catalog.type".to_string(), "hadoop".to_string()),
                ("iceberg.catalog.warehouse".to_string(), warehouse),
            ],
        )
        .expect("catalog entry");
        let catalog = build_iceberg_catalog(&entry).expect("catalog");

        let runtime = tokio::runtime::Runtime::new().unwrap();
        runtime.block_on(async {
            let ns = iceberg::NamespaceIdent::from_strs(["test_ns"]).unwrap();
            catalog
                .create_namespace(&ns, std::collections::HashMap::new())
                .await
                .unwrap();
            let schema = iceberg::spec::Schema::builder()
                .with_fields(vec![StdArc::new(iceberg::spec::NestedField::required(
                    1,
                    "k",
                    iceberg::spec::Type::Primitive(iceberg::spec::PrimitiveType::Int),
                ))])
                .build()
                .unwrap();
            let table = catalog
                .create_table(
                    &ns,
                    iceberg::TableCreation::builder()
                        .name("t".to_string())
                        .schema(schema)
                        .format_version(iceberg::spec::FormatVersion::V3)
                        .properties([("write.row-lineage".to_string(), "true".to_string())])
                        .build(),
                )
                .await
                .unwrap();

            let ident = TableIdent::from_strs(["test_ns", "t"]).unwrap();
            let initial = single_int_chunk(&[0]);
            let initial_written = write_chunks_as_iceberg_data_files(&table, &initial)
                .await
                .unwrap();
            let initial_snapshot = commit_iceberg_mv_target_files(
                &table,
                &catalog,
                &entry,
                &ident,
                CommitOpKind::FastAppend,
                initial_written,
            )
            .await
            .unwrap()
            .new_snapshot_id;
            let table = catalog.load_table(&ident).await.unwrap();
            let current = table.metadata().current_snapshot().map(|s| s.snapshot_id());
            assert_eq!(current, Some(initial_snapshot));

            let marker = MvRefreshSnapshotMarker {
                refresh_id: 7,
                mv_id: 3,
                token: "token-7".to_string(),
            };
            let staging_branch = "__nova_mv_refresh_3_7";
            crate::connector::iceberg::commit::execute_ref_action(
                catalog.as_ref(),
                &crate::connector::iceberg::commit::RefActionPlan {
                    catalog: "ice".to_string(),
                    namespace: "test_ns".to_string(),
                    table: "t".to_string(),
                    action: crate::connector::iceberg::commit::RefAction::CreateBranch {
                        name: staging_branch.to_string(),
                        snapshot_id: current.expect("main snapshot"),
                        replace: false,
                        if_not_exists: false,
                    },
                },
            )
            .await
            .unwrap();
            let table = catalog.load_table(&ident).await.unwrap();

            let chunks = single_int_chunk(&[1, 2, 3]);
            let written = write_chunks_as_iceberg_data_files(&table, &chunks)
                .await
                .unwrap();
            let staging_snapshot = commit_iceberg_mv_target_files_with_ref(
                &table,
                &catalog,
                &entry,
                &ident,
                CommitOpKind::FastAppend,
                written,
                staging_branch,
                marker.to_summary_properties(),
            )
            .await
            .unwrap()
            .new_snapshot_id;

            let reloaded = catalog.load_table(&ident).await.unwrap();
            assert_eq!(
                reloaded
                    .metadata()
                    .current_snapshot()
                    .map(|s| s.snapshot_id()),
                current
            );
            assert_eq!(
                reloaded
                    .metadata()
                    .refs()
                    .get(staging_branch)
                    .map(|r| r.snapshot_id),
                Some(staging_snapshot)
            );
        });
    }

    #[test]
    fn iceberg_mv_drop_missing_staging_branch_errors() {
        use crate::connector::iceberg::catalog::registry::{
            build_catalog_entry, build_iceberg_catalog,
        };

        let dir = tempfile::tempdir().expect("tempdir");
        let warehouse = format!("file://{}/wh", dir.path().display());
        let catalog_props = [
            ("type".to_string(), "iceberg".to_string()),
            ("iceberg.catalog.type".to_string(), "hadoop".to_string()),
            ("iceberg.catalog.warehouse".to_string(), warehouse),
        ];
        let entry = build_catalog_entry("ice", &catalog_props).expect("catalog entry");
        let catalog = build_iceberg_catalog(&entry).expect("catalog");
        let state = Arc::new(StandaloneState::default());
        crate::connector::register_standalone_backends(&state);
        {
            let mut catalogs = state.iceberg_catalogs.write().expect("iceberg catalogs");
            catalogs
                .create_catalog("ice", &catalog_props)
                .expect("create iceberg catalog");
        }

        let runtime = tokio::runtime::Runtime::new().unwrap();
        runtime.block_on(async {
            let ns = iceberg::NamespaceIdent::from_strs(["test_ns"]).unwrap();
            catalog
                .create_namespace(&ns, std::collections::HashMap::new())
                .await
                .unwrap();
            let schema = iceberg::spec::Schema::builder()
                .with_fields(vec![StdArc::new(iceberg::spec::NestedField::required(
                    1,
                    "k",
                    iceberg::spec::Type::Primitive(iceberg::spec::PrimitiveType::Int),
                ))])
                .build()
                .unwrap();
            catalog
                .create_table(
                    &ns,
                    iceberg::TableCreation::builder()
                        .name("t".to_string())
                        .schema(schema)
                        .format_version(iceberg::spec::FormatVersion::V3)
                        .properties([("write.row-lineage".to_string(), "true".to_string())])
                        .build(),
                )
                .await
                .unwrap();
        });

        let target = IcebergMvTarget {
            catalog: "ice".to_string(),
            namespace: "test_ns".to_string(),
            table: "t".to_string(),
        };
        let err =
            drop_iceberg_mv_staging_branch(&state, &target, &entry, "__missing_staging_branch")
                .expect_err("missing staging branch must be an error");
        assert!(
            err.contains("branch '__missing_staging_branch' does not exist"),
            "{err}"
        );
    }

    #[test]
    fn iceberg_mv_cleanup_after_definite_failure_drops_staging_branch() {
        use crate::connector::iceberg::catalog::registry::{
            build_catalog_entry, build_iceberg_catalog,
        };

        let dir = tempfile::tempdir().expect("tempdir");
        let warehouse = format!("file://{}/wh", dir.path().display());
        let catalog_props = [
            ("type".to_string(), "iceberg".to_string()),
            ("iceberg.catalog.type".to_string(), "hadoop".to_string()),
            ("iceberg.catalog.warehouse".to_string(), warehouse),
        ];
        let entry = build_catalog_entry("ice", &catalog_props).expect("catalog entry");
        let catalog = build_iceberg_catalog(&entry).expect("catalog");
        let state = Arc::new(StandaloneState::default());
        crate::connector::register_standalone_backends(&state);
        {
            let mut catalogs = state.iceberg_catalogs.write().expect("iceberg catalogs");
            catalogs
                .create_catalog("ice", &catalog_props)
                .expect("create iceberg catalog");
        }

        let runtime = tokio::runtime::Runtime::new().unwrap();
        let staging_branch = "__nova_cleanup_failure";
        runtime.block_on(async {
            let ns = iceberg::NamespaceIdent::from_strs(["test_ns"]).unwrap();
            catalog
                .create_namespace(&ns, std::collections::HashMap::new())
                .await
                .unwrap();
            let schema = iceberg::spec::Schema::builder()
                .with_fields(vec![StdArc::new(iceberg::spec::NestedField::required(
                    1,
                    "k",
                    iceberg::spec::Type::Primitive(iceberg::spec::PrimitiveType::Int),
                ))])
                .build()
                .unwrap();
            let table = catalog
                .create_table(
                    &ns,
                    iceberg::TableCreation::builder()
                        .name("t".to_string())
                        .schema(schema)
                        .format_version(iceberg::spec::FormatVersion::V3)
                        .properties([("write.row-lineage".to_string(), "true".to_string())])
                        .build(),
                )
                .await
                .unwrap();
            let ident = TableIdent::from_strs(["test_ns", "t"]).unwrap();
            let initial = single_int_chunk(&[0]);
            let initial_written = write_chunks_as_iceberg_data_files(&table, &initial)
                .await
                .unwrap();
            let initial_snapshot = commit_iceberg_mv_target_files(
                &table,
                &catalog,
                &entry,
                &ident,
                CommitOpKind::FastAppend,
                initial_written,
            )
            .await
            .unwrap()
            .new_snapshot_id;
            crate::connector::iceberg::commit::execute_ref_action(
                catalog.as_ref(),
                &crate::connector::iceberg::commit::RefActionPlan {
                    catalog: "ice".to_string(),
                    namespace: "test_ns".to_string(),
                    table: "t".to_string(),
                    action: crate::connector::iceberg::commit::RefAction::CreateBranch {
                        name: staging_branch.to_string(),
                        snapshot_id: initial_snapshot,
                        replace: false,
                        if_not_exists: false,
                    },
                },
            )
            .await
            .unwrap();
        });

        let target = IcebergMvTarget {
            catalog: "ice".to_string(),
            namespace: "test_ns".to_string(),
            table: "t".to_string(),
        };
        let err = cleanup_iceberg_mv_staging_branch_after_failure(
            &state,
            &target,
            &entry,
            staging_branch,
            "original failure".to_string(),
        );
        assert_eq!(err, "original failure");

        runtime.block_on(async {
            let verify_catalog = build_iceberg_catalog(&entry).expect("verify catalog");
            let ident = TableIdent::from_strs(["test_ns", "t"]).unwrap();
            let reloaded = verify_catalog.load_table(&ident).await.unwrap();
            assert!(
                !reloaded.metadata().refs().contains_key(staging_branch),
                "staging branch should be dropped"
            );
        });
    }

    #[test]
    fn iceberg_mv_fast_append_uses_collector_abort_cleanup() {
        use crate::connector::iceberg::catalog::registry::{
            build_catalog_entry, build_iceberg_catalog,
        };
        use iceberg::spec::{DataContentType, DataFileBuilder, DataFileFormat, Struct};

        let dir = tempfile::tempdir().expect("tempdir");
        let warehouse = format!("file://{}/wh", dir.path().display());
        let entry = build_catalog_entry(
            "ice",
            &[
                ("type".to_string(), "iceberg".to_string()),
                ("iceberg.catalog.type".to_string(), "hadoop".to_string()),
                ("iceberg.catalog.warehouse".to_string(), warehouse.clone()),
            ],
        )
        .expect("catalog entry");
        let catalog = build_iceberg_catalog(&entry).expect("catalog");

        let runtime = tokio::runtime::Runtime::new().unwrap();
        runtime.block_on(async {
            let ns = iceberg::NamespaceIdent::from_strs(["test_ns"]).unwrap();
            catalog
                .create_namespace(&ns, std::collections::HashMap::new())
                .await
                .unwrap();
            let schema = iceberg::spec::Schema::builder()
                .with_fields(vec![StdArc::new(iceberg::spec::NestedField::required(
                    1,
                    "k",
                    iceberg::spec::Type::Primitive(iceberg::spec::PrimitiveType::Int),
                ))])
                .build()
                .unwrap();
            let table = catalog
                .create_table(
                    &ns,
                    iceberg::TableCreation::builder()
                        .name("mv_target".to_string())
                        .schema(schema)
                        .build(),
                )
                .await
                .unwrap();
            let ident = TableIdent::from_strs(["test_ns", "mv_target"]).unwrap();
            let staged_path = dir.path().join("staged-position-delete.parquet");
            std::fs::write(&staged_path, b"bad delete file").expect("write staged file");
            let staged_uri = format!("file://{}", staged_path.display());
            let bad_file = DataFileBuilder::default()
                .content(DataContentType::PositionDeletes)
                .file_path(staged_uri)
                .file_format(DataFileFormat::Parquet)
                .partition(Struct::empty())
                .partition_spec_id(0)
                .record_count(1)
                .file_size_in_bytes(15)
                .referenced_data_file(Some("file:///base/data.parquet".to_string()))
                .build()
                .expect("bad data file");

            let err = commit_iceberg_mv_target_files(
                &table,
                &catalog,
                &entry,
                &ident,
                CommitOpKind::FastAppend,
                vec![bad_file],
            )
            .await
            .expect_err("position delete must not be fast-appended");
            assert!(err.contains("abort cleanup ran"), "{err}");
            assert!(
                !staged_path.exists(),
                "collector abort cleanup should delete the injected file"
            );
        });
    }

    #[test]
    fn write_chunks_populates_partition_data_for_partitioned_table() {
        use crate::connector::iceberg::catalog::registry::{
            build_catalog_entry, build_iceberg_catalog,
        };
        use iceberg::spec::{Transform, UnboundPartitionSpec};

        let dir = tempfile::tempdir().expect("tempdir");
        let warehouse = format!("file://{}/wh", dir.path().display());
        let entry = build_catalog_entry(
            "ice",
            &[
                ("type".to_string(), "iceberg".to_string()),
                ("iceberg.catalog.type".to_string(), "hadoop".to_string()),
                ("iceberg.catalog.warehouse".to_string(), warehouse.clone()),
            ],
        )
        .expect("catalog entry");
        let catalog = build_iceberg_catalog(&entry).expect("catalog");

        let runtime = tokio::runtime::Runtime::new().unwrap();
        runtime.block_on(async {
            let ns = iceberg::NamespaceIdent::from_strs(["test_ns"]).unwrap();
            catalog
                .create_namespace(&ns, std::collections::HashMap::new())
                .await
                .unwrap();

            let schema = iceberg::spec::Schema::builder()
                .with_fields(vec![
                    StdArc::new(iceberg::spec::NestedField::required(
                        1,
                        "k",
                        iceberg::spec::Type::Primitive(iceberg::spec::PrimitiveType::Int),
                    )),
                    StdArc::new(iceberg::spec::NestedField::optional(
                        2,
                        "v",
                        iceberg::spec::Type::Primitive(iceberg::spec::PrimitiveType::Long),
                    )),
                ])
                .build()
                .unwrap();
            let partition_spec = UnboundPartitionSpec::builder()
                .with_spec_id(0)
                .add_partition_field(1, "k_identity", Transform::Identity)
                .unwrap()
                .build();

            let creation = iceberg::TableCreation::builder()
                .name("t".to_string())
                .schema(schema)
                .partition_spec(partition_spec)
                .build();
            let table = catalog.create_table(&ns, creation).await.unwrap();

            let arrow_schema = StdArc::new(ArrowSchema::new(vec![
                Field::new("k", DataType::Int32, false),
                Field::new("v", DataType::Int64, true),
            ]));
            let batch = RecordBatch::try_new(
                arrow_schema.clone(),
                vec![
                    StdArc::new(Int32Array::from(vec![1, 2, 3])),
                    StdArc::new(Int64Array::from(vec![Some(10), Some(20), None])),
                ],
            )
            .unwrap();

            use crate::common::ids::SlotId;
            use crate::exec::chunk::ChunkSchema;
            let chunk_schema_ref = ChunkSchema::try_ref_from_schema_and_slot_ids(
                &arrow_schema,
                &[SlotId(0), SlotId(1)],
            )
            .expect("chunk schema");
            let chunk = crate::exec::chunk::Chunk::new_with_chunk_schema(batch, chunk_schema_ref);

            let written = write_chunks_as_iceberg_data_files(&table, &[chunk])
                .await
                .unwrap();
            assert_eq!(written.len(), 3);
            assert!(
                written
                    .iter()
                    .all(|data_file| data_file.partition().fields().len() == 1)
            );
            assert!(
                written
                    .iter()
                    .all(|data_file| data_file.record_count() == 1)
            );

            let ident = TableIdent::from_strs(["test_ns", "t"]).unwrap();
            let snapshot_id = commit_iceberg_mv_target_files(
                &table,
                &catalog,
                &entry,
                &ident,
                CommitOpKind::FastAppend,
                written,
            )
            .await
            .unwrap()
            .new_snapshot_id;
            assert!(snapshot_id != 0, "snapshot id must be non-zero");
        });
    }

    mod aggregate_apply_test_helpers {
        use crate::connector::starrocks::managed::ddl::managed_physical_column;
        use crate::connector::starrocks::managed::mv_agg_state::{
            AggregateMvLayout, AggregateStateColumn, AggregateStateRole, AggregateVisibleColumn,
        };
        use crate::connector::starrocks::managed::mv_shape::AggregateFunctionKind;
        use crate::exec::chunk::Chunk;
        use crate::meta::repository::mv_contract::{
            ApplyKeySource, BaseContract, BaseFieldRecord, BaseSchemaSnapshot, ExpressionKind,
            ExpressionLineage, HiddenApplyKeyContract, MvPartitionContract,
            MvPartitionFieldContract, MvPartitionTransformContract, MvSchemaContract,
            OutputColumnLineage, OutputContract, TargetContract, TargetVisibleColumn,
        };
        use crate::sql::parser::ast::SqlType;
        use arrow::array::{ArrayRef, Int64Array, StringArray};
        use arrow::datatypes::{DataType, Field, Schema};
        use arrow::record_batch::RecordBatch;
        use std::sync::Arc;

        pub(super) fn count_layout(group_key: &str) -> AggregateMvLayout {
            let row_id = managed_physical_column(
                "__row_id__".to_string(),
                SqlType::String,
                false,
                false,
                true,
            );
            let group =
                managed_physical_column(group_key.to_string(), SqlType::String, true, true, false);
            let counter =
                managed_physical_column("c".to_string(), SqlType::BigInt, false, true, false);
            let state = managed_physical_column(
                "__agg_state_c".to_string(),
                SqlType::BigInt,
                false,
                false,
                false,
            );
            AggregateMvLayout {
                row_id_column: row_id.clone(),
                visible_columns: vec![
                    AggregateVisibleColumn {
                        name: group_key.to_string(),
                        data_type: DataType::Utf8,
                        sql_type: SqlType::String,
                        nullable: true,
                        source_index: 0,
                    },
                    AggregateVisibleColumn {
                        name: "c".to_string(),
                        data_type: DataType::Int64,
                        sql_type: SqlType::BigInt,
                        nullable: false,
                        source_index: 1,
                    },
                ],
                state_columns: vec![AggregateStateColumn {
                    name: "__agg_state_c".to_string(),
                    data_type: DataType::Int64,
                    sql_type: SqlType::BigInt,
                    nullable: false,
                    visible_source_index: 1,
                    aggregate_index: 0,
                    function: AggregateFunctionKind::Count,
                    state_role: AggregateStateRole::Single,
                    count_star: true,
                }],
                group_key_source_indexes: vec![0],
                physical_columns: vec![row_id, group, counter, state],
            }
        }

        pub(super) fn count_contract_with_identity_partition(
            partition_field_name: &str,
            source_target_field_id: i32,
        ) -> MvSchemaContract {
            count_contract_with_transform(
                partition_field_name,
                source_target_field_id,
                MvPartitionTransformContract::Identity,
            )
        }

        pub(super) fn count_contract_with_void_partition(
            partition_field_name: &str,
            source_target_field_id: i32,
        ) -> MvSchemaContract {
            count_contract_with_transform(
                partition_field_name,
                source_target_field_id,
                MvPartitionTransformContract::Void,
            )
        }

        fn count_contract_with_transform(
            partition_field_name: &str,
            source_target_field_id: i32,
            transform: MvPartitionTransformContract,
        ) -> MvSchemaContract {
            MvSchemaContract {
                contract_version: 1,
                base: BaseContract {
                    table_fqn: "ice.sales.orders".to_string(),
                    table_uuid: "base-uuid".to_string(),
                    alias_at_create: None,
                    schema_id_at_create: 0,
                    schema_at_create: BaseSchemaSnapshot {
                        fields: vec![BaseFieldRecord {
                            field_id: 1,
                            name_at_create: "region".to_string(),
                            type_signature: "string".to_string(),
                            required: true,
                        }],
                    },
                },
                bases: Vec::new(),
                output: OutputContract {
                    columns: vec![
                        OutputColumnLineage {
                            expression: ExpressionLineage {
                                kind: ExpressionKind::Column,
                                referenced_base_field_ids: vec![1],
                                referenced_base_fields: Vec::new(),
                            },
                        },
                        OutputColumnLineage {
                            expression: ExpressionLineage {
                                kind: ExpressionKind::Column,
                                referenced_base_field_ids: Vec::new(),
                                referenced_base_fields: Vec::new(),
                            },
                        },
                    ],
                    filter: None,
                },
                join: None,
                aggregate: None,
                target: TargetContract {
                    table_fqn: "ice.analytics.mv_orders".to_string(),
                    table_uuid: "target-uuid".to_string(),
                    schema_id_at_create: 0,
                    visible_columns: vec![
                        TargetVisibleColumn {
                            output_name: partition_field_name.to_string(),
                            target_field_id: source_target_field_id,
                            type_signature: "string".to_string(),
                            nullable: true,
                        },
                        TargetVisibleColumn {
                            output_name: "c".to_string(),
                            target_field_id: 12,
                            type_signature: "bigint".to_string(),
                            nullable: false,
                        },
                    ],
                    hidden_apply_key: HiddenApplyKeyContract {
                        column_name: "__row_id__".to_string(),
                        target_field_id: 10,
                        source: ApplyKeySource::GroupRowId,
                    },
                    partition: Some(MvPartitionContract {
                        target_spec_id: 7,
                        fields: vec![MvPartitionFieldContract {
                            partition_field_id: 100,
                            partition_field_name: partition_field_name.to_string(),
                            source_target_field_id,
                            source_column_name: partition_field_name.to_string(),
                            transform,
                        }],
                    }),
                },
            }
        }

        pub(super) fn batch_with_group_key(name: &str, dt: DataType, values: ArrayRef) -> Chunk {
            let n = values.len();
            let row_ids: Vec<String> = (0..n).map(|i| format!("rid-{i}")).collect();
            let row_id_arr: ArrayRef = Arc::new(StringArray::from(row_ids));
            let counts: ArrayRef = Arc::new(Int64Array::from(vec![1i64; n]));
            let states: ArrayRef = Arc::new(Int64Array::from(vec![1i64; n]));
            let schema = Arc::new(Schema::new(vec![
                Field::new("__row_id__", DataType::Utf8, false),
                Field::new(name, dt, true),
                Field::new("c", DataType::Int64, false),
                Field::new("__agg_state_c", DataType::Int64, false),
            ]));
            let batch =
                RecordBatch::try_new(schema, vec![row_id_arr, values, counts, states]).unwrap();
            crate::engine::record_batch_to_chunk(batch).unwrap()
        }
    }

    #[test]
    fn build_aggregate_target_partition_filter_returns_allow_list_for_partitioned_contract() {
        use crate::engine::mv::partition::{
            MvPartitionKey, MvPartitionKeyField, MvPartitionValue, TargetPartitionFilter,
        };
        let layout = aggregate_apply_test_helpers::count_layout("region");
        let contract =
            aggregate_apply_test_helpers::count_contract_with_identity_partition("region", 11);
        let chunk = aggregate_apply_test_helpers::batch_with_group_key(
            "region",
            arrow::datatypes::DataType::Utf8,
            std::sync::Arc::new(arrow::array::StringArray::from(vec![Some("a"), Some("b")]))
                as arrow::array::ArrayRef,
        );
        let (filter, touched) =
            build_aggregate_target_partition_filter(&layout, &contract, &[chunk]).expect("filter");
        match filter {
            TargetPartitionFilter::AllowList(set) => {
                let keys: Vec<_> = set.iter().cloned().collect();
                let want: Vec<_> = ["a", "b"]
                    .iter()
                    .map(|v| {
                        MvPartitionKey::new(
                            7,
                            vec![MvPartitionKeyField::new(
                                "region".to_string(),
                                MvPartitionValue::String((*v).to_string()),
                            )],
                        )
                    })
                    .collect();
                assert_eq!(keys, want);
            }
            other => panic!("expected AllowList, got {other:?}"),
        }
        assert_eq!(touched.len(), 2);
    }

    #[test]
    fn build_aggregate_target_partition_filter_returns_none_for_unpartitioned_contract() {
        use crate::engine::mv::partition::TargetPartitionFilter;
        let layout = aggregate_apply_test_helpers::count_layout("region");
        let mut contract =
            aggregate_apply_test_helpers::count_contract_with_identity_partition("region", 11);
        contract.target.partition = None;
        let chunk = aggregate_apply_test_helpers::batch_with_group_key(
            "region",
            arrow::datatypes::DataType::Utf8,
            std::sync::Arc::new(arrow::array::StringArray::from(vec![Some("a")]))
                as arrow::array::ArrayRef,
        );
        let (filter, touched) =
            build_aggregate_target_partition_filter(&layout, &contract, &[chunk]).expect("filter");
        assert!(matches!(filter, TargetPartitionFilter::None));
        assert_eq!(touched.len(), 1);
    }

    #[test]
    fn build_aggregate_target_partition_filter_propagates_derivation_error_with_field_name() {
        let layout = aggregate_apply_test_helpers::count_layout("region");
        let contract =
            aggregate_apply_test_helpers::count_contract_with_void_partition("region", 11);
        let chunk = aggregate_apply_test_helpers::batch_with_group_key(
            "region",
            arrow::datatypes::DataType::Utf8,
            std::sync::Arc::new(arrow::array::StringArray::from(vec![Some("a")]))
                as arrow::array::ArrayRef,
        );
        let err =
            build_aggregate_target_partition_filter(&layout, &contract, &[chunk]).unwrap_err();
        assert!(err.contains("region"), "{err}");
        assert!(err.contains("void"), "{err}");
    }

    #[test]
    fn aggregate_apply_error_message_includes_mv_id_and_target_fqn() {
        let layout = aggregate_apply_test_helpers::count_layout("region");
        let contract =
            aggregate_apply_test_helpers::count_contract_with_void_partition("region", 11);
        let chunk = aggregate_apply_test_helpers::batch_with_group_key(
            "region",
            arrow::datatypes::DataType::Utf8,
            std::sync::Arc::new(arrow::array::StringArray::from(vec![Some("a")]))
                as arrow::array::ArrayRef,
        );
        let target_fqn = "ice.analytics.mv_orders";
        let mv_id = 4242i64;
        let err = wrap_aggregate_apply_error(
            target_fqn,
            mv_id,
            build_aggregate_target_partition_filter(&layout, &contract, &[chunk])
                .err()
                .unwrap(),
        );
        assert!(err.contains("mv_id=4242"), "{err}");
        assert!(err.contains(target_fqn), "{err}");
        assert!(err.contains("void"), "{err}");
    }

    #[test]
    fn tracing_field_partition_filter_label_renders_none_and_allow_list() {
        use crate::engine::mv::partition::{
            MvPartitionKey, MvPartitionKeyField, MvPartitionValue, TargetPartitionFilter,
        };
        let none = TargetPartitionFilter::None;
        assert_eq!(partition_filter_label(&none), "none");
        let allow = TargetPartitionFilter::AllowList(
            [MvPartitionKey::new(
                7,
                vec![MvPartitionKeyField::new(
                    "region".to_string(),
                    MvPartitionValue::String("a".to_string()),
                )],
            )]
            .into_iter()
            .collect(),
        );
        assert_eq!(partition_filter_label(&allow), "allow_list");
        let empty_allow = TargetPartitionFilter::AllowList(std::collections::BTreeSet::new());
        assert_eq!(partition_filter_label(&empty_allow), "allow_list");
    }

    #[test]
    fn tracing_field_partition_filter_count_returns_kept_size() {
        use crate::engine::mv::partition::{
            MvPartitionKey, MvPartitionKeyField, MvPartitionValue, TargetPartitionFilter,
        };
        let none = TargetPartitionFilter::None;
        assert_eq!(partition_filter_count(&none), None);
        let allow = TargetPartitionFilter::AllowList(
            [
                MvPartitionKey::new(
                    7,
                    vec![MvPartitionKeyField::new(
                        "region".to_string(),
                        MvPartitionValue::String("a".to_string()),
                    )],
                ),
                MvPartitionKey::new(
                    7,
                    vec![MvPartitionKeyField::new(
                        "region".to_string(),
                        MvPartitionValue::String("b".to_string()),
                    )],
                ),
            ]
            .into_iter()
            .collect(),
        );
        assert_eq!(partition_filter_count(&allow), Some(2));
    }

    // ---- Tracing event capture ---------------------------------------------
    //
    // The two structured events emitted by the aggregate apply path
    // (`iceberg_aggregate_mv.apply` and `iceberg_aggregate_mv.partition_derivation_failed`)
    // are unit-tested by feeding the emitter helpers (`emit_aggregate_apply_event`
    // and `wrap_aggregate_apply_error`) through a `tracing_subscriber::fmt`
    // subscriber that writes into an in-memory buffer. The end-to-end
    // emission from `apply_iceberg_aggregate_delta_chunks` is exercised by
    // the `iceberg-ivm` SQL suite (the apply path takes a full StandaloneState
    // / staging branch / commit lifecycle that is out of scope for a unit
    // test), so this gives us regression coverage at the emitter contract
    // without standing the rest of the world up.
    #[derive(Clone)]
    struct TracingTestBuffer(std::sync::Arc<std::sync::Mutex<Vec<u8>>>);

    impl TracingTestBuffer {
        fn new() -> Self {
            Self(std::sync::Arc::new(std::sync::Mutex::new(Vec::new())))
        }
        fn output(&self) -> String {
            String::from_utf8(self.0.lock().unwrap().clone())
                .expect("tracing output is valid UTF-8")
        }
    }

    struct TracingTestWriter(std::sync::Arc<std::sync::Mutex<Vec<u8>>>);

    impl std::io::Write for TracingTestWriter {
        fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
            self.0.lock().unwrap().extend_from_slice(buf);
            Ok(buf.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }

    impl<'a> tracing_subscriber::fmt::MakeWriter<'a> for TracingTestBuffer {
        type Writer = TracingTestWriter;
        fn make_writer(&'a self) -> Self::Writer {
            TracingTestWriter(self.0.clone())
        }
    }

    fn capture_events<F: FnOnce()>(emit: F) -> String {
        let buf = TracingTestBuffer::new();
        let subscriber = tracing_subscriber::fmt()
            .with_writer(buf.clone())
            .with_ansi(false)
            .with_target(false)
            .with_level(false)
            .without_time()
            .finish();
        tracing::subscriber::with_default(subscriber, emit);
        buf.output()
    }

    #[test]
    fn aggregate_apply_event_emits_full_field_set_for_allow_list() {
        use crate::engine::mv::iceberg_aggregate_state::AggregateStateLookupStats;
        use crate::engine::mv::partition::{
            MvPartitionKey, MvPartitionKeyField, MvPartitionValue, TargetPartitionFilter,
        };

        let mut allow = std::collections::BTreeSet::new();
        allow.insert(MvPartitionKey::new(
            7,
            vec![MvPartitionKeyField::new(
                "region".to_string(),
                MvPartitionValue::String("a".to_string()),
            )],
        ));
        let filter = TargetPartitionFilter::AllowList(allow);
        let stats = AggregateStateLookupStats {
            planned_file_count: 10,
            kept_file_count: 1,
            scanned_row_count: 100,
            matched_row_count: 5,
        };

        let output = capture_events(|| {
            emit_aggregate_apply_event(&AggregateApplyEvent {
                target_fqn: "ice.analytics.mv_orders",
                mv_id: 4242,
                partition_filter: &filter,
                touched_group_count: 5,
                lookup_stats: &stats,
                delete_row_count: 2,
                insert_chunk_row_count: 5,
                new_total_rows: 100,
                iceberg_snapshot: 999,
            });
        });

        // Event name.
        assert!(
            output.contains("event=\"iceberg_aggregate_mv.apply\""),
            "missing event name in:\n{output}"
        );
        // The 13 structured fields the spec § 11 promises. We assert each
        // field's key=value form so a typo in the emitter (e.g. dropping a
        // field, renaming a key) fails immediately.
        for field in [
            "mv_id=4242",
            "target_fqn=ice.analytics.mv_orders",
            "partition_filter=\"allow_list\"",
            "affected_partition_count=1",
            "touched_group_count=5",
            "planned_file_count=10",
            "kept_file_count=1",
            "scanned_target_row_count=100",
            "matched_target_row_count=5",
            "delete_row_count=2",
            "insert_chunk_row_count=5",
            "new_total_rows=100",
            "iceberg_snapshot=999",
        ] {
            assert!(
                output.contains(field),
                "missing `{field}` in event output:\n{output}"
            );
        }
        // Invariants the apply path is expected to keep. These also guard
        // against accidental field swaps (e.g. matched/scanned reversed).
        assert!(stats.kept_file_count <= stats.planned_file_count);
        assert!(stats.matched_row_count <= stats.scanned_row_count);
    }

    #[test]
    fn aggregate_apply_event_renders_none_filter_with_zero_affected_count() {
        use crate::engine::mv::iceberg_aggregate_state::AggregateStateLookupStats;
        use crate::engine::mv::partition::TargetPartitionFilter;

        let filter = TargetPartitionFilter::None;
        let stats = AggregateStateLookupStats {
            planned_file_count: 4,
            kept_file_count: 4,
            scanned_row_count: 50,
            matched_row_count: 50,
        };

        let output = capture_events(|| {
            emit_aggregate_apply_event(&AggregateApplyEvent {
                target_fqn: "ice.analytics.mv_unpartitioned",
                mv_id: 7,
                partition_filter: &filter,
                touched_group_count: 50,
                lookup_stats: &stats,
                delete_row_count: 0,
                insert_chunk_row_count: 50,
                new_total_rows: 50,
                iceberg_snapshot: 12345,
            });
        });

        assert!(
            output.contains("partition_filter=\"none\""),
            "expected partition_filter=\"none\" for non-partitioned MV, got:\n{output}"
        );
        assert!(
            output.contains("affected_partition_count=0"),
            "expected affected_partition_count=0 for None filter, got:\n{output}"
        );
    }

    #[test]
    fn wrap_aggregate_apply_error_emits_partition_derivation_failed_event() {
        let output = capture_events(|| {
            let _ = wrap_aggregate_apply_error(
                "ice.analytics.mv_orders",
                4242,
                "MV partition field region uses unsupported transform Void".to_string(),
            );
        });

        assert!(
            output.contains("event=\"iceberg_aggregate_mv.partition_derivation_failed\""),
            "missing partition_derivation_failed event name in:\n{output}"
        );
        assert!(output.contains("mv_id=4242"), "missing mv_id in:\n{output}");
        assert!(
            output.contains("target_fqn=ice.analytics.mv_orders"),
            "missing target_fqn in:\n{output}"
        );
        // reason field forwards the cause verbatim.
        assert!(
            output.contains("unsupported transform Void"),
            "reason field missing transform context in:\n{output}"
        );
    }
}
