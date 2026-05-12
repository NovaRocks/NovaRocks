//! Phase4a: projection/filter materialized views backed by Iceberg target
//! tables in the current Iceberg catalog. Aggregate shapes (phase4b) and any
//! unsupported MV definitions are rejected here.

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
    CommitOpKind, CommitOutcome, IcebergCommitCollector, RunInput, run_iceberg_commit,
};
use crate::connector::iceberg::data_writer::write_record_batches_as_data_files;
use crate::connector::starrocks::managed::model::{IcebergTableRef, ManagedMvStorageEngine};
use crate::connector::starrocks::managed::mv_ddl::{
    analyze_mv_select, canonicalize_iceberg_mv_select_query, extract_base_table_refs, now_ms,
    output_column_to_table_column, resolve_mv_name, validate_mv_partition_columns,
};
use crate::connector::starrocks::managed::mv_refresh::{
    acquire_mv_refresh_lock, load_current_iceberg_base_table, parse_iceberg_table_refs,
    query_result_to_chunks, run_mv_full_select_chunks, single_snapshot_map, single_table_uuid_map,
};
use crate::connector::starrocks::managed::mv_shape::{
    IncrementalMvShape, classify_incremental_mv_query,
};
use crate::engine::mv_flow::execute_query_for_mv_incremental_refresh;
use crate::engine::query_prep::IcebergFileForQuery;
use crate::engine::{StandaloneState, StatementResult};
use crate::meta::repository::mv::{
    CreateMvDefinitionRequest, MvRefreshFinalizeRequest, RefreshExternalOutcome, StoredMvDefinition,
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

    // 1. Analyze and classify shape — phase4a only accepts projection/filter.
    let canonical_select_query =
        canonicalize_iceberg_mv_select_query(&stmt.select_query, current_catalog, current_database);
    let analysis = analyze_mv_select(
        state,
        current_catalog,
        current_database,
        &canonical_select_query,
    )?;
    validate_mv_partition_columns(stmt.partition_by.as_deref(), &analysis.output_columns)?;
    let base_refs = extract_base_table_refs(&analysis.resolved_refs)?;
    let shape = classify_incremental_mv_query(&canonical_select_query)?;
    if !matches!(shape, IncrementalMvShape::ProjectionFilter(_)) {
        return Err(
            "phase4a iceberg-backed materialized views support only projection/filter shapes; aggregates are phase4b"
                .to_string(),
        );
    }

    // IVM Phase-2 PRIMARY KEY validation. Only runs when the user opted in
    // by writing `PRIMARY KEY (...)` in the DDL; otherwise behavior is
    // unchanged. Reuses the same descriptor + validator as the managed-
    // lake-stored path in mv_ddl::create_mv.
    if let Some(pk_cols) = stmt.primary_key.as_deref() {
        if base_refs.len() != 1 {
            return Err(
                "PRIMARY KEY on materialized view requires exactly one iceberg base table"
                    .to_string(),
            );
        }
        let base_ref = &base_refs[0];
        let loaded = load_current_iceberg_base_table(state, base_ref)?;
        let descriptor =
            crate::connector::starrocks::managed::mv_ddl::descriptor_from_loaded(&loaded);
        crate::connector::starrocks::managed::mv_ddl::validate_ivm_primary_key(
            pk_cols,
            &descriptor,
        )
        .map_err(|e| e.to_string())?;
    }

    // 2. Create the empty Iceberg v2 target table in the current catalog.
    let columns = analysis
        .output_columns
        .iter()
        .map(output_column_to_table_column)
        .collect::<Result<Vec<_>, _>>()?;
    crate::connector::iceberg::catalog::registry::create_table(
        &entry,
        &target.namespace,
        &target.table,
        &columns,
        None,
        &[],
        &[("format-version".to_string(), "2".to_string())],
    )?;

    // 3. Persist MV metadata in the repository.
    let primary_key_columns = stmt.primary_key.clone().unwrap_or_default();
    let created_at_ms = now_ms();
    if let Err(err) = (|| {
        let provider = state
            .metadata_provider
            .as_ref()
            .ok_or_else(|| "metadata provider required for iceberg mv".to_string())?;
        let mut txn = provider
            .begin_write("create iceberg materialized view definition")
            .map_err(|e| format!("open iceberg mv definition transaction failed: {e}"))?;
        state
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
                    created_at_ms,
                },
            )
            .map_err(|e| format!("create iceberg MV repository metadata failed: {e}"))?;
        txn.commit()
            .map_err(|e| format!("commit iceberg MV repository metadata failed: {e}"))?;
        Ok::<(), String>(())
    })() {
        let drop_result = crate::connector::iceberg::catalog::registry::drop_table(
            &entry,
            &target.namespace,
            &target.table,
        );
        return Err(format!(
            "create iceberg MV repository metadata failed: {err}; target cleanup={drop_result:?}"
        ));
    }
    register_iceberg_mv_target_in_catalog(state, &target)?;

    Ok(StatementResult::Ok)
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
    let table_def = crate::connector::iceberg::catalog::build_iceberg_table_def_with_files(
        &entry,
        &target.namespace,
        &target.table,
        loaded,
        files,
    )?;
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
    let mv_definition = load_iceberg_mv_definition_by_target(state, &target)?;
    let (target_entry, iceberg_catalog, target_loaded) = load_iceberg_mv_target(state, &target)?;
    validate_target_snapshot(&target, &mv_definition, &target_loaded.table)?;

    // We only handle single-base-table MVs in phase4a.
    let base_refs = parse_iceberg_table_refs(&mv_definition.base_table_refs)?;
    let [base_ref] = base_refs.as_slice() else {
        return Err(
            "iceberg materialized view refresh requires exactly one base table reference"
                .to_string(),
        );
    };

    // Load the base iceberg table to get its current snapshot.
    let loaded = load_current_iceberg_base_table(state, base_ref)?;
    let current_snapshot_id = loaded
        .table
        .metadata()
        .current_snapshot()
        .map(|s| s.snapshot_id());
    let current_table_uuid = loaded.table.metadata().uuid().to_string();
    let previous_snapshot_id = mv_definition
        .last_refresh_snapshots
        .get(&base_ref.fqn())
        .copied();

    if let Some(previous_uuid) = mv_definition.last_refresh_table_uuids.get(&base_ref.fqn())
        && previous_uuid != &current_table_uuid
    {
        tracing::info!(
            "iceberg mv {}.{}.{}: base table identity changed from {previous_uuid} to {current_table_uuid}; rebuilding with overwrite",
            target.catalog,
            target.namespace,
            target.table
        );
        let target_snapshots = current_snapshot_id
            .map(|snapshot_id| single_snapshot_map(base_ref, snapshot_id))
            .unwrap_or_default();
        let refresh_id =
            begin_iceberg_mv_refresh_intent(state, mv_definition.mv_id, target_snapshots)?;
        return rebuild_iceberg_mv(
            state,
            &target,
            &target_entry,
            &iceberg_catalog,
            &target_loaded.table,
            refresh_id,
            current_database,
            &mv_definition,
            base_ref,
            current_snapshot_id,
            &current_table_uuid,
        );
    }

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
            let refresh_id = begin_iceberg_mv_refresh_intent(
                state,
                mv_definition.mv_id,
                single_snapshot_map(base_ref, cur),
            )?;
            first_refresh_iceberg_mv(
                state,
                &target,
                &target_entry,
                &iceberg_catalog,
                &target_loaded.table,
                refresh_id,
                current_database,
                &mv_definition,
                base_ref,
                cur,
                &current_table_uuid,
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
            let snapshots = single_snapshot_map(base_ref, cur);
            let table_uuids = single_table_uuid_map(base_ref, &current_table_uuid);
            let target_snapshot_id = recorded_target_snapshot_id(&target, &mv_definition)?;
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
        (Some(prev), Some(cur)) => {
            let refresh_id = begin_iceberg_mv_refresh_intent(
                state,
                mv_definition.mv_id,
                single_snapshot_map(base_ref, cur),
            )?;
            incremental_refresh_iceberg_mv(
                state,
                &target,
                &target_entry,
                &iceberg_catalog,
                &target_loaded.table,
                refresh_id,
                current_database,
                &mv_definition,
                base_ref,
                prev,
                cur,
                &loaded.table,
                &current_table_uuid,
            )
        }

        // Previous snapshot no longer reachable.
        (Some(prev), None) => Err(format!(
            "cannot refresh iceberg materialized view {}.{}.{}: \
             previously-refreshed base snapshot {prev} is no longer reachable",
            target.catalog, target.namespace, target.table
        )),
    }
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

fn finalize_iceberg_mv_refresh(
    state: &Arc<StandaloneState>,
    refresh_id: i64,
    rows: i64,
    base_snapshots: std::collections::BTreeMap<String, i64>,
    base_table_uuids: std::collections::BTreeMap<String, String>,
    target_snapshot_id: i64,
) -> Result<(), String> {
    let provider = state
        .metadata_provider
        .as_ref()
        .ok_or_else(|| "metadata provider required for iceberg mv refresh".to_string())?;
    let mut txn = provider
        .begin_write("finalize iceberg materialized view refresh")
        .map_err(|e| format!("open iceberg mv refresh finalize transaction failed: {e}"))?;
    state
        .mv_repo
        .record_external_commit_outcome(
            txn.as_mut(),
            refresh_id,
            RefreshExternalOutcome {
                target_snapshot_id: Some(target_snapshot_id),
                commit_id: format!("iceberg-snapshot-{target_snapshot_id}"),
            },
        )
        .map_err(|e| format!("record iceberg mv refresh outcome failed: {e}"))?;
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

/// Execute the first refresh of an iceberg-backed MV.
///
/// Steps:
/// 1. Run the MV's SELECT against the base table.
/// 2. Write the resulting chunks as Iceberg/Parquet data files.
/// 3. Commit a fast-append snapshot.
/// 4. Finalize repository refresh metadata.
///
/// On failure after writing but before commit, repository metadata is left
/// in an aborted refresh state. Stranded data files are orphaned until the
/// warehouse is garbage-collected.
#[allow(clippy::too_many_arguments)]
fn first_refresh_iceberg_mv(
    state: &Arc<StandaloneState>,
    target: &IcebergMvTarget,
    target_entry: &crate::connector::iceberg::catalog::IcebergCatalogEntry,
    iceberg_catalog: &Arc<dyn iceberg::Catalog>,
    target_table: &iceberg::table::Table,
    refresh_id: i64,
    current_database: &str,
    mv_definition: &StoredMvDefinition,
    base_ref: &IcebergTableRef,
    base_snapshot_id: i64,
    current_table_uuid: &str,
) -> Result<StatementResult, String> {
    // 1. Run SELECT and collect chunks.
    let chunks = match run_mv_full_select_chunks(state, current_database, &mv_definition.select_sql)
    {
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
    let new_snapshot_id = match data_block_on(async {
        let data_files = write_chunks_as_iceberg_data_files(target_table, &chunks).await?;
        commit_iceberg_mv_target_files(
            target_table,
            iceberg_catalog,
            target_entry,
            &ident,
            CommitOpKind::FastAppend,
            data_files,
        )
        .await
        .map(|outcome| outcome.new_snapshot_id)
    }) {
        Ok(Ok(snapshot_id)) => snapshot_id,
        Ok(Err(err)) | Err(err) => {
            abort_iceberg_mv_refresh(state, refresh_id)?;
            return Err(err);
        }
    };

    // 4. Persist refresh metadata in the repository.
    let snapshots = single_snapshot_map(base_ref, base_snapshot_id);
    let table_uuids = single_table_uuid_map(base_ref, current_table_uuid);
    finalize_iceberg_mv_refresh(
        state,
        refresh_id,
        total_rows,
        snapshots.clone(),
        table_uuids.clone(),
        new_snapshot_id,
    )?;
    // 5. Update the in-memory catalog so subsequent SELECTs can read the data.
    if let Err(e) = register_iceberg_mv_target_in_catalog(state, target) {
        tracing::warn!(
            "iceberg mv {}.{}.{}: catalog update after first refresh failed: {e}; \
             SELECT may return stale results until server restart",
            target.catalog,
            target.namespace,
            target.table
        );
    }

    tracing::info!(
        "iceberg mv {}.{}.{}: first refresh complete: \
         rows={total_rows} iceberg_snapshot={new_snapshot_id}",
        target.catalog,
        target.namespace,
        target.table
    );
    Ok(StatementResult::Ok)
}

#[allow(clippy::too_many_arguments)]
fn rebuild_iceberg_mv(
    state: &Arc<StandaloneState>,
    target: &IcebergMvTarget,
    target_entry: &crate::connector::iceberg::catalog::IcebergCatalogEntry,
    iceberg_catalog: &Arc<dyn iceberg::Catalog>,
    target_table: &iceberg::table::Table,
    refresh_id: i64,
    current_database: &str,
    mv_definition: &StoredMvDefinition,
    base_ref: &IcebergTableRef,
    base_snapshot_id: Option<i64>,
    current_table_uuid: &str,
) -> Result<StatementResult, String> {
    let chunks = match run_mv_full_select_chunks(state, current_database, &mv_definition.select_sql)
    {
        Ok(chunks) => chunks,
        Err(err) => {
            abort_iceberg_mv_refresh(state, refresh_id)?;
            return Err(err);
        }
    };
    let total_rows: i64 = chunks.iter().map(|c| c.batch.num_rows() as i64).sum();

    let ident = iceberg_mv_table_ident(target)?;
    let new_snapshot_id = match data_block_on(async {
        let data_files = if chunks.iter().all(|c| c.batch.num_rows() == 0) {
            Vec::new()
        } else {
            write_chunks_as_iceberg_data_files(target_table, &chunks).await?
        };
        commit_overwrite_iceberg_mv(
            target_table,
            iceberg_catalog,
            target_entry,
            &ident,
            data_files,
        )
        .await
    }) {
        Ok(Ok(snapshot_id)) => snapshot_id,
        Ok(Err(err)) | Err(err) => {
            abort_iceberg_mv_refresh(state, refresh_id)?;
            return Err(err);
        }
    };

    let snapshots = base_snapshot_id
        .map(|snapshot_id| single_snapshot_map(base_ref, snapshot_id))
        .unwrap_or_default();
    let table_uuids = single_table_uuid_map(base_ref, current_table_uuid);
    finalize_iceberg_mv_refresh(
        state,
        refresh_id,
        total_rows,
        snapshots.clone(),
        table_uuids.clone(),
        new_snapshot_id,
    )?;
    if let Err(e) = register_iceberg_mv_target_in_catalog(state, target) {
        tracing::warn!(
            "iceberg mv {}.{}.{}: catalog update after rebuild failed: {e}; \
             SELECT may return stale results until server restart",
            target.catalog,
            target.namespace,
            target.table
        );
    }

    Ok(StatementResult::Ok)
}

async fn commit_overwrite_iceberg_mv(
    table: &iceberg::table::Table,
    catalog: &Arc<dyn Catalog>,
    entry: &crate::connector::iceberg::catalog::IcebergCatalogEntry,
    ident: &TableIdent,
    data_files: Vec<DataFile>,
) -> Result<i64, String> {
    commit_iceberg_mv_target_files(
        table,
        catalog,
        entry,
        ident,
        CommitOpKind::Overwrite,
        data_files,
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
    let metadata = table.metadata();
    let staging_dir = format!(
        "{}/data/_staging/{}",
        metadata.location(),
        uuid::Uuid::new_v4()
    );
    let collector = Arc::new(IcebergCommitCollector::new(
        op_kind,
        ident.clone(),
        metadata.current_snapshot().map(|s| s.snapshot_id()),
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

    run_iceberg_commit(RunInput {
        collector,
        catalog: catalog.clone(),
        table: table.clone(),
        fs: abort_cleanup.fs,
        file_io: table.file_io().clone(),
        cleanup_path_mapper: abort_cleanup.path_mapper,
        cow_update_rewrite: None,
        target_ref: "main".to_string(),
    })
    .await
}

/// Execute the incremental refresh of an iceberg-backed MV.
///
/// Steps:
/// 1. Plan the change batch from `previous_snapshot_id` to `current_snapshot_id`.
/// 2. Run the MV SELECT scoped to the inserts only.
/// 3. If the delta yields 0 rows, advance lineage without committing an empty snapshot.
/// 4. Otherwise: verify MV iceberg table is in the expected state (inconsistent-state guard),
///    write data files, commit fast-append, and finalize repository metadata.
///
/// On failure after writing data files but before commit, no rollback is attempted.
/// Repository metadata is only finalized after a successful commit, so the prior snapshot
/// remains current and a subsequent REFRESH MATERIALIZED VIEW will retry the same
/// delta range idempotently. Stranded Parquet files are orphaned until the warehouse
/// is garbage-collected.
#[allow(clippy::too_many_arguments)]
fn incremental_refresh_iceberg_mv(
    state: &Arc<StandaloneState>,
    target: &IcebergMvTarget,
    target_entry: &crate::connector::iceberg::catalog::IcebergCatalogEntry,
    iceberg_catalog: &Arc<dyn iceberg::Catalog>,
    target_table: &iceberg::table::Table,
    refresh_id: i64,
    current_database: &str,
    mv_definition: &StoredMvDefinition,
    base_ref: &IcebergTableRef,
    previous_snapshot_id: i64,
    current_snapshot_id: i64,
    base_table: &iceberg::table::Table,
    current_table_uuid: &str,
) -> Result<StatementResult, String> {
    // 1. Plan the change batch. If the standard Iceberg diff cannot be planned
    // safely, rebuild instead of risking an incorrect incremental result.
    let batch = match plan_changes(base_table, previous_snapshot_id, &[]) {
        Ok(batch) => batch,
        Err(err) => match policy_signal_from_change_error(&err) {
            IcebergChangePolicySignal::FullRefresh { reason } => {
                tracing::info!(
                    "iceberg mv {}.{}.{}: incremental planner requested full refresh: {reason}",
                    target.catalog,
                    target.namespace,
                    target.table
                );
                return rebuild_iceberg_mv(
                    state,
                    target,
                    target_entry,
                    iceberg_catalog,
                    target_table,
                    refresh_id,
                    current_database,
                    mv_definition,
                    base_ref,
                    Some(current_snapshot_id),
                    current_table_uuid,
                );
            }
            IcebergChangePolicySignal::Unsupported { reason } => {
                abort_iceberg_mv_refresh(state, refresh_id)?;
                return Err(format!(
                    "iceberg-stored materialized view refresh unsupported: {reason}"
                ));
            }
            IcebergChangePolicySignal::Incremental => {
                abort_iceberg_mv_refresh(state, refresh_id)?;
                return Err(
                    "iceberg-stored materialized view refresh produced invalid incremental policy from change planner"
                        .to_string(),
                );
            }
        },
    };
    if !batch.deletes.is_empty()
        || !batch.equality_deletes.is_empty()
        || !batch.deleted_data_files.is_empty()
    {
        tracing::info!(
            "iceberg mv {}.{}.{}: falling back to full refresh for delete-bearing change batch: \
             position_deletes={}, equality_deletes={}, deleted_data_files={}",
            target.catalog,
            target.namespace,
            target.table,
            batch.deletes.len(),
            batch.equality_deletes.len(),
            batch.deleted_data_files.len()
        );
        return rebuild_iceberg_mv(
            state,
            target,
            target_entry,
            iceberg_catalog,
            target_table,
            refresh_id,
            current_database,
            mv_definition,
            base_ref,
            Some(current_snapshot_id),
            current_table_uuid,
        );
    }
    if batch.current_snapshot_id != current_snapshot_id {
        abort_iceberg_mv_refresh(state, refresh_id)?;
        return Err(format!(
            "iceberg mv incremental refresh: change batch snapshot mismatch (expected {current_snapshot_id}, got {})",
            batch.current_snapshot_id,
        ));
    }

    // 2. Run the MV SELECT scoped to the inserts only.
    let added_files: Vec<IcebergFileForQuery> = batch
        .inserts
        .iter()
        .map(|f| IcebergFileForQuery {
            path: f.path.clone(),
            size: f.size,
            record_count: f.record_count,
            partition_spec_id: f.partition_spec_id,
            partition_key: f.partition_key.clone(),
            first_row_id: f.first_row_id,
            data_sequence_number: f.data_sequence_number,
            change_op: None,
        })
        .collect();
    let chunks = execute_query_for_mv_incremental_refresh(
        state,
        current_database,
        &mv_definition.select_sql,
        base_ref,
        added_files,
    )
    .and_then(query_result_to_chunks)
    .map_err(|err| {
        let _ = abort_iceberg_mv_refresh(state, refresh_id);
        err
    })?;
    let added_rows = chunks
        .iter()
        .map(|c| c.batch.num_rows() as i64)
        .sum::<i64>();

    // 3. Empty delta: no new rows → advance lineage without committing an empty snapshot.
    if added_rows == 0 {
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

    // 4. Write and commit.
    let ident = iceberg_mv_table_ident(target)?;
    let new_snapshot_id = match data_block_on(async {
        let written = write_chunks_as_iceberg_data_files(target_table, &chunks).await?;
        commit_iceberg_mv_target_files(
            target_table,
            iceberg_catalog,
            target_entry,
            &ident,
            CommitOpKind::FastAppend,
            written,
        )
        .await
        .map(|outcome| outcome.new_snapshot_id)
    }) {
        Ok(Ok(snapshot_id)) => snapshot_id,
        Ok(Err(err)) | Err(err) => {
            abort_iceberg_mv_refresh(state, refresh_id)?;
            return Err(err);
        }
    };

    let new_total_rows = mv_definition.last_refresh_rows.unwrap_or(0) + added_rows;
    let snapshots = single_snapshot_map(base_ref, current_snapshot_id);
    let table_uuids = single_table_uuid_map(base_ref, current_table_uuid);
    finalize_iceberg_mv_refresh(
        state,
        refresh_id,
        new_total_rows,
        snapshots.clone(),
        table_uuids.clone(),
        new_snapshot_id,
    )?;
    // Update the in-memory catalog so subsequent SELECTs can read all data files.
    if let Err(e) = register_iceberg_mv_target_in_catalog(state, target) {
        tracing::warn!(
            "iceberg mv {}.{}.{}: catalog update after incremental refresh failed: {e}; \
             SELECT may return stale results until server restart",
            target.catalog,
            target.namespace,
            target.table
        );
    }

    tracing::info!(
        "iceberg mv {}.{}.{}: incremental refresh complete: \
         added_rows={added_rows} total_rows={new_total_rows} iceberg_snapshot={new_snapshot_id}",
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
    use arrow::array::{Int32Array, Int64Array};
    use arrow::datatypes::{DataType, Field, Schema as ArrowSchema};
    use arrow::record_batch::RecordBatch;
    use std::sync::Arc as StdArc;
    use tempfile::TempDir;

    fn output_col(name: &str, ty: DataType, nullable: bool) -> OutputColumn {
        OutputColumn {
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
            &[("format-version".to_string(), "2".to_string())],
        )
        .expect("create iceberg table");
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
        let rows = rows
            .iter()
            .map(|(id, name)| {
                vec![
                    crate::sql::Literal::Int(i64::from(*id)),
                    crate::sql::Literal::String((*name).to_string()),
                ]
            })
            .collect::<Vec<_>>();
        crate::connector::iceberg::catalog::registry::insert_rows(&entry, namespace, table, &rows)
            .expect("insert iceberg rows");
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

        crate::connector::starrocks::managed::mv_ddl::create_mv(
            &env.state,
            Some("ice"),
            &env.current_db,
            &stmt,
        )
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

        let err = crate::connector::starrocks::managed::mv_ddl::create_mv(
            &env.state,
            Some("ice"),
            &env.current_db,
            &stmt,
        )
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
}
