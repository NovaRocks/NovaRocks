//! Phase4a: projection/filter materialized views backed by Iceberg tables in
//! the NovaRocks-internal `__nova_mv__` catalog. Aggregate shapes (phase4b)
//! and any unsupported MV definitions are rejected here.

use std::collections::HashMap;
use std::sync::Arc;

use iceberg::spec::{DataFileFormat, NestedField, PrimitiveType, Schema, Type};
use iceberg::transaction::{ApplyTransactionAction, Transaction};
use iceberg::writer::base_writer::data_file_writer::DataFileWriterBuilder;
use iceberg::writer::file_writer::ParquetWriterBuilder;
use iceberg::writer::file_writer::location_generator::{
    DefaultFileNameGenerator, DefaultLocationGenerator,
};
use iceberg::writer::file_writer::rolling_writer::RollingFileWriterBuilder;
use iceberg::writer::{IcebergWriter, IcebergWriterBuilder};
use iceberg::{NamespaceIdent, TableCreation, TableIdent};
use parquet::file::properties::WriterProperties;

use crate::connector::iceberg::catalog::plan_append_delta;
use crate::connector::starrocks::managed::mv_ddl::{
    alloc_id, analyze_mv_select, extract_base_table_refs, find_or_create_managed_database, now_ms,
    resolve_mv_name,
};
use crate::connector::starrocks::managed::mv_iceberg_catalog::{
    NOVA_MV_CATALOG_NAME, build_nova_mv_catalog,
};
use crate::connector::starrocks::managed::mv_refresh::{
    load_current_iceberg_base_table, query_result_to_chunks, run_mv_full_select_chunks,
    single_snapshot_map,
};
use crate::connector::starrocks::managed::mv_shape::{
    IncrementalMvShape, classify_incremental_mv_query,
};
use crate::connector::starrocks::managed::store::{
    InsertIcebergMvRowRequest, ManagedTableKind, ManagedTableState, StoredManagedTable,
    UpdateMvIcebergRefreshMetadataRequest,
};
use crate::runtime::global_async_runtime::data_block_on;
use crate::sql::analysis::OutputColumn;
use crate::sql::catalog::{ColumnDef, S3FileInfo, TableDef, TableStorage};
use crate::sql::parser::ast::{
    CreateMaterializedViewStmt, DropMaterializedViewStmt, RefreshMaterializedViewStmt,
};
use crate::standalone::engine::mv_flow::execute_query_for_mv_incremental_refresh;
use crate::standalone::engine::{StandaloneState, StatementResult};

pub(crate) fn create_iceberg_mv(
    state: &Arc<StandaloneState>,
    current_database: &str,
    stmt: &CreateMaterializedViewStmt,
) -> Result<StatementResult, String> {
    let (db_name, mv_name) = resolve_mv_name(&stmt.name, current_database)?;
    let cfg = state
        .managed_lake_config
        .as_ref()
        .ok_or_else(|| "managed lake config required for iceberg mv".to_string())?
        .clone();
    let metadata_store = state
        .metadata_store
        .as_ref()
        .ok_or_else(|| "sqlite metadata store required for iceberg mv".to_string())?;

    // 1. Analyze and classify shape — phase4a only accepts projection/filter.
    let analysis = analyze_mv_select(state, current_database, &stmt.select_query)?;
    let base_refs = extract_base_table_refs(&analysis.resolved_refs)?;
    let shape = classify_incremental_mv_query(&stmt.select_query)?;
    if !matches!(shape, IncrementalMvShape::ProjectionFilter(_)) {
        return Err(
            "phase4a iceberg-backed materialized views support only projection/filter shapes; aggregates are phase4b"
                .to_string(),
        );
    }

    // 2. Allocate a managed-lake table_id for the MV (register it in the
    //    managed-lake `tables` row so SHOW MATERIALIZED VIEWS works uniformly,
    //    but no tablets, schemas, partitions, or indexes are allocated for the
    //    iceberg storage path).
    let mv_id = allocate_iceberg_mv_table_row(state, &db_name, &mv_name)?;

    // 3. Build Iceberg schema from analyzed output columns.
    let schema = build_iceberg_schema_from_outputs(&analysis.output_columns)?;

    // 4. Create namespace + table in __nova_mv__.
    let catalog = build_nova_mv_catalog(&cfg)?;
    let ns = NamespaceIdent::from_strs([&db_name])
        .map_err(|e| format!("namespace ident `{db_name}` failed: {e}"))?;
    data_block_on(async {
        if !catalog
            .namespace_exists(&ns)
            .await
            .map_err(|e| e.to_string())?
        {
            catalog
                .create_namespace(&ns, HashMap::new())
                .await
                .map_err(|e| format!("create namespace `{db_name}` in __nova_mv__ failed: {e}"))?;
        }
        let creation = TableCreation::builder()
            .name(mv_name.clone())
            .schema(schema)
            .build();
        catalog
            .create_table(&ns, creation)
            .await
            .map_err(|e| format!("create iceberg mv table failed: {e}"))?;
        Ok::<_, String>(())
    })??;

    // 5. Persist mv row in SQLite.
    metadata_store.insert_iceberg_mv_row(InsertIcebergMvRowRequest {
        mv_id,
        select_sql: stmt.select_sql.clone(),
        base_table_refs: base_refs,
        iceberg_table_identifier: format!("{NOVA_MV_CATALOG_NAME}.{db_name}.{mv_name}"),
        created_at_ms: now_ms(),
    })?;

    // 6. Register the MV in the in-memory catalog so that subsequent
    //    `state.catalog.read().get(&db, &mv)` calls return Ok and the
    //    duplicate-detection pre-check in create_mv works correctly.
    //    Iceberg-backed MVs have no tablet storage, so we build a TableDef
    //    directly from the analyzed output columns rather than going through
    //    ManagedLakeCatalog::rebuild (which requires a tablet schema row).
    {
        let table_def = {
            let columns = analysis
                .output_columns
                .iter()
                .map(|col| ColumnDef {
                    name: col.name.clone(),
                    data_type: col.data_type.clone(),
                    nullable: col.nullable,
                })
                .collect();
            TableDef {
                name: mv_name.clone(),
                columns,
                storage: TableStorage::S3ParquetFiles {
                    files: vec![],
                    cloud_properties: Default::default(),
                },
            }
        };
        let mut catalog = state
            .catalog
            .write()
            .expect("standalone catalog write lock");
        catalog.create_database(&db_name)?;
        catalog.register(&db_name, table_def)?;
    }

    Ok(StatementResult::Ok)
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
    current_database: &str,
    stmt: &RefreshMaterializedViewStmt,
) -> Result<StatementResult, String> {
    let (db_name, mv_name) = resolve_mv_name(&stmt.name, current_database)?;
    let cfg = state
        .managed_lake_config
        .as_ref()
        .ok_or_else(|| "managed lake config required for iceberg mv refresh".to_string())?
        .clone();
    let metadata_store = state
        .metadata_store
        .as_ref()
        .ok_or_else(|| "sqlite metadata store required for iceberg mv refresh".to_string())?;

    // Load the MV row from SQLite.
    let snapshot = metadata_store.load_snapshot()?.managed;
    let expected_id = format!("{NOVA_MV_CATALOG_NAME}.{db_name}.{mv_name}");
    let mv_row = snapshot
        .materialized_views
        .iter()
        .find(|mv| mv.iceberg_table_identifier.as_deref() == Some(expected_id.as_str()))
        .cloned()
        .ok_or_else(|| {
            format!("iceberg materialized view {db_name}.{mv_name} has no materialized_views row")
        })?;

    // We only handle single-base-table MVs in phase4a.
    let [base_ref] = mv_row.base_table_refs.as_slice() else {
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
    let previous_snapshot_id = mv_row.last_refresh_snapshots.get(&base_ref.fqn()).copied();

    match (previous_snapshot_id, current_snapshot_id) {
        // Base table has no snapshot yet — nothing to refresh.
        (None, None) => {
            tracing::info!(
                "iceberg mv {db_name}.{mv_name}: base table has no snapshot; skipping refresh"
            );
            Ok(StatementResult::Ok)
        }

        // First refresh: base table now has a snapshot but we haven't run yet.
        (None, Some(cur)) => first_refresh_iceberg_mv(
            state,
            &cfg,
            metadata_store,
            &db_name,
            &mv_name,
            &mv_row,
            base_ref,
            cur,
        ),

        // No-op: base table snapshot has not advanced.
        (Some(prev), Some(cur)) if prev == cur => {
            tracing::info!(
                "iceberg mv {db_name}.{mv_name}: base snapshot {cur} unchanged; updating metadata only"
            );
            let snapshots = single_snapshot_map(base_ref, cur);
            metadata_store.update_mv_iceberg_refresh_metadata(
                UpdateMvIcebergRefreshMetadataRequest {
                    table_id: mv_row.mv_id,
                    last_refresh_rows: mv_row.last_refresh_rows.unwrap_or(0),
                    snapshots,
                    iceberg_snapshot_id: mv_row.last_refreshed_iceberg_snapshot_id.unwrap_or(0),
                },
            )?;
            Ok(StatementResult::Ok)
        }

        // Incremental: base snapshot has advanced.
        (Some(prev), Some(cur)) => incremental_refresh_iceberg_mv(
            state,
            &cfg,
            metadata_store,
            &db_name,
            &mv_name,
            &mv_row,
            base_ref,
            prev,
            cur,
            &loaded.table,
        ),

        // Previous snapshot no longer reachable.
        (Some(prev), None) => Err(format!(
            "cannot refresh iceberg materialized view {db_name}.{mv_name}: \
             previously-refreshed base snapshot {prev} is no longer reachable"
        )),
    }
}

/// Execute the first refresh of an iceberg-backed MV.
///
/// Steps:
/// 1. Run the MV's SELECT against the base table.
/// 2. Write the resulting chunks as Iceberg/Parquet data files.
/// 3. Commit a fast-append snapshot.
/// 4. Update SQLite metadata.
///
/// On failure after writing but before commit, we attempt a best-effort
/// rollback by dropping the iceberg table.  The SQLite mv_row is only
/// updated after a successful commit, so a failure leaves the mv_row
/// pointing at a non-existent (or empty) iceberg table.  Task 9 (DROP MV)
/// is responsible for cleaning up such orphaned rows.
#[allow(clippy::too_many_arguments)]
fn first_refresh_iceberg_mv(
    state: &Arc<StandaloneState>,
    cfg: &crate::connector::starrocks::managed::config::ManagedLakeConfig,
    metadata_store: &crate::connector::starrocks::managed::store::SqliteMetadataStore,
    db_name: &str,
    mv_name: &str,
    mv_row: &crate::connector::starrocks::managed::store::StoredMaterializedView,
    base_ref: &crate::connector::starrocks::managed::store::IcebergTableRef,
    base_snapshot_id: i64,
) -> Result<StatementResult, String> {
    // 1. Run SELECT and collect chunks.
    let chunks = run_mv_full_select_chunks(state, db_name, &mv_row.select_sql)?;
    let total_rows: i64 = chunks.iter().map(|c| c.batch.num_rows() as i64).sum();

    // If the base table is currently empty, do not commit an empty Iceberg
    // snapshot.  Leave the mv_row in pre-refresh state so the next REFRESH
    // can re-enter first-refresh once the base table has data.
    if total_rows == 0 {
        tracing::info!(
            "iceberg mv {db_name}.{mv_name}: first refresh produced 0 rows; \
             skipping snapshot commit so next REFRESH can retry"
        );
        return Ok(StatementResult::Ok);
    }

    // 2–3. Write data files and commit snapshot inside an async block.
    let catalog = build_nova_mv_catalog(cfg)?;
    let ident = TableIdent::from_strs([db_name, mv_name])
        .map_err(|e| format!("build mv iceberg ident failed: {e}"))?;

    let result: Result<i64, String> = data_block_on(async {
        let table = catalog
            .load_table(&ident)
            .await
            .map_err(|e| format!("load iceberg mv table for first refresh failed: {e}"))?;

        let data_files = write_chunks_as_iceberg_data_files(&table, &chunks).await?;
        let snapshot_id = commit_fast_append(&table, &catalog, data_files).await?;
        Ok(snapshot_id)
    })?;
    let new_snapshot_id = match result {
        Ok(id) => id,
        Err(e) => {
            // Best-effort rollback: drop the (partial) iceberg table so it does
            // not linger after a failed first refresh.  The SQLite mv_row still
            // points at this table; Task 9 (DROP MV) will clean that up if the
            // rollback itself fails.
            let rollback_result = data_block_on(async {
                catalog.drop_table(&ident).await.map_err(|e| e.to_string())
            });
            match rollback_result {
                Ok(Ok(())) => {} // rollback succeeded
                Ok(Err(rollback_err)) | Err(rollback_err) => {
                    tracing::warn!(
                        "iceberg mv {db_name}.{mv_name}: best-effort rollback drop_table failed: \
                         {rollback_err}; table may remain as an orphan until DROP MATERIALIZED VIEW"
                    );
                }
            }
            return Err(format!(
                "first refresh failed (rolled back best-effort): {e}"
            ));
        }
    };

    // 4. Persist refresh metadata in SQLite.
    let snapshots = single_snapshot_map(base_ref, base_snapshot_id);
    metadata_store.update_mv_iceberg_refresh_metadata(UpdateMvIcebergRefreshMetadataRequest {
        table_id: mv_row.mv_id,
        last_refresh_rows: total_rows,
        snapshots,
        iceberg_snapshot_id: new_snapshot_id,
    })?;

    // 5. Update the in-memory catalog so subsequent SELECTs can read the data.
    if let Err(e) = update_iceberg_mv_in_catalog(state, cfg, db_name, mv_name) {
        tracing::warn!(
            "iceberg mv {db_name}.{mv_name}: catalog update after first refresh failed: {e}; \
             SELECT may return stale results until server restart"
        );
    }

    tracing::info!(
        "iceberg mv {db_name}.{mv_name}: first refresh complete: \
         rows={total_rows} iceberg_snapshot={new_snapshot_id}"
    );
    Ok(StatementResult::Ok)
}

/// Execute the incremental refresh of an iceberg-backed MV.
///
/// Steps:
/// 1. Plan the append delta from `previous_snapshot_id` to `current_snapshot_id`.
/// 2. Run the MV SELECT scoped to the delta files only.
/// 3. If the delta yields 0 rows, advance lineage without committing an empty snapshot.
/// 4. Otherwise: verify MV iceberg table is in the expected state (inconsistent-state guard),
///    write data files, commit fast-append, and update SQLite metadata.
///
/// On failure after writing data files but before commit, no rollback is attempted.
/// SQLite metadata is only updated after a successful commit, so the prior snapshot
/// remains current and a subsequent REFRESH MATERIALIZED VIEW will retry the same
/// delta range idempotently. Stranded Parquet files are orphaned until the warehouse
/// is garbage-collected.
#[allow(clippy::too_many_arguments)]
fn incremental_refresh_iceberg_mv(
    state: &Arc<StandaloneState>,
    cfg: &crate::connector::starrocks::managed::config::ManagedLakeConfig,
    metadata_store: &crate::connector::starrocks::managed::store::SqliteMetadataStore,
    db_name: &str,
    mv_name: &str,
    mv_row: &crate::connector::starrocks::managed::store::StoredMaterializedView,
    base_ref: &crate::connector::starrocks::managed::store::IcebergTableRef,
    previous_snapshot_id: i64,
    current_snapshot_id: i64,
    base_table: &iceberg::table::Table,
) -> Result<StatementResult, String> {
    // 1. Plan the append delta.
    let delta = plan_append_delta(base_table, previous_snapshot_id)?;
    if delta.current_snapshot_id != current_snapshot_id {
        return Err(format!(
            "iceberg mv incremental refresh: delta snapshot mismatch (expected {current_snapshot_id}, got {})",
            delta.current_snapshot_id,
        ));
    }

    // 2. Run the MV SELECT scoped to the delta files only.
    let chunks = execute_query_for_mv_incremental_refresh(
        state,
        db_name,
        &mv_row.select_sql,
        base_ref,
        delta.added_files,
    )
    .and_then(query_result_to_chunks)?;
    let added_rows = chunks
        .iter()
        .map(|c| c.batch.num_rows() as i64)
        .sum::<i64>();

    // 3. Empty delta: no new rows → advance lineage without committing an empty snapshot.
    if added_rows == 0 {
        tracing::info!(
            "iceberg mv {db_name}.{mv_name}: incremental refresh delta has 0 rows; \
             advancing lineage to base snapshot {current_snapshot_id} without new iceberg snapshot"
        );
        metadata_store.update_mv_iceberg_refresh_metadata(
            UpdateMvIcebergRefreshMetadataRequest {
                table_id: mv_row.mv_id,
                last_refresh_rows: mv_row.last_refresh_rows.unwrap_or(0),
                snapshots: single_snapshot_map(base_ref, current_snapshot_id),
                iceberg_snapshot_id: mv_row.last_refreshed_iceberg_snapshot_id.unwrap_or(0),
            },
        )?;
        return Ok(StatementResult::Ok);
    }

    // 4. Write and commit — guarded by inconsistent-state check first.
    let catalog = build_nova_mv_catalog(cfg)?;
    let ident =
        TableIdent::from_strs([db_name, mv_name]).map_err(|e| format!("table ident: {e}"))?;
    let new_snapshot_id = data_block_on(async {
        let table = catalog
            .load_table(&ident)
            .await
            .map_err(|e| format!("load iceberg mv table for incremental refresh failed: {e}"))?;

        // Inconsistent-state guard: the MV's iceberg current snapshot must match
        // what SQLite recorded at the last refresh.  If they diverge, the metadata is
        // out-of-sync (manual edit, prior crash, etc.) — fail fast rather than corrupt lineage.
        let prior_snapshot = table.metadata().current_snapshot().map(|s| s.snapshot_id());
        if prior_snapshot != mv_row.last_refreshed_iceberg_snapshot_id {
            return Err(format!(
                "iceberg mv `{db_name}.{mv_name}` is in inconsistent state: \
                 sqlite recorded snapshot {:?} but iceberg current is {:?}; \
                 manual reconcile required (drop and recreate)",
                mv_row.last_refreshed_iceberg_snapshot_id, prior_snapshot,
            ));
        }

        let written = write_chunks_as_iceberg_data_files(&table, &chunks).await?;
        commit_fast_append(&table, &catalog, written).await
    })??;

    let new_total_rows = mv_row.last_refresh_rows.unwrap_or(0) + added_rows;
    metadata_store.update_mv_iceberg_refresh_metadata(UpdateMvIcebergRefreshMetadataRequest {
        table_id: mv_row.mv_id,
        last_refresh_rows: new_total_rows,
        snapshots: single_snapshot_map(base_ref, current_snapshot_id),
        iceberg_snapshot_id: new_snapshot_id,
    })?;

    // Update the in-memory catalog so subsequent SELECTs can read all data files.
    if let Err(e) = update_iceberg_mv_in_catalog(state, cfg, db_name, mv_name) {
        tracing::warn!(
            "iceberg mv {db_name}.{mv_name}: catalog update after incremental refresh failed: {e}; \
             SELECT may return stale results until server restart"
        );
    }

    tracing::info!(
        "iceberg mv {db_name}.{mv_name}: incremental refresh complete: \
         added_rows={added_rows} total_rows={new_total_rows} iceberg_snapshot={new_snapshot_id}"
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
    // Build an Arrow schema annotated with Iceberg field ids. The ParquetWriterBuilder
    // uses FieldMatchMode::Id and requires PARQUET_FIELD_ID_META_KEY on each field.
    let iceberg_arrow_schema =
        iceberg::arrow::schema_to_arrow_schema(table.metadata().current_schema())
            .map_err(|e| format!("convert iceberg schema to arrow failed: {e}"))?;
    let iceberg_arrow_schema_ref = Arc::new(iceberg_arrow_schema);

    let location_generator = DefaultLocationGenerator::new(table.metadata().clone())
        .map_err(|e| format!("build iceberg location generator failed: {e}"))?;
    // Use a UUID-derived random suffix so that files written in rapid successive
    // REFRESH calls never collide on the same path — nanosecond timestamps are
    // not a uniqueness guarantee when two REFRESH calls complete within the same
    // nanosecond (e.g. in tests or under high load).  The AtomicU64 counter
    // inside DefaultFileNameGenerator resets to 0 on every new instance, making
    // a per-call unique seed necessary.
    let unique_suffix = {
        use rand::Rng;
        let mut rng = rand::thread_rng();
        let mut bytes = [0_u8; 16];
        rng.fill(&mut bytes);
        // RFC 4122 version-4 variant bits.
        bytes[6] = (bytes[6] & 0x0f) | 0x40;
        bytes[8] = (bytes[8] & 0x3f) | 0x80;
        format!(
            "{:02x}{:02x}{:02x}{:02x}{:02x}{:02x}{:02x}{:02x}{:02x}{:02x}{:02x}{:02x}{:02x}{:02x}{:02x}{:02x}",
            bytes[0],
            bytes[1],
            bytes[2],
            bytes[3],
            bytes[4],
            bytes[5],
            bytes[6],
            bytes[7],
            bytes[8],
            bytes[9],
            bytes[10],
            bytes[11],
            bytes[12],
            bytes[13],
            bytes[14],
            bytes[15],
        )
    };
    let file_name_generator = DefaultFileNameGenerator::new(
        "novarocks".to_string(),
        Some(unique_suffix),
        DataFileFormat::Parquet,
    );
    let parquet_builder = ParquetWriterBuilder::new(
        WriterProperties::default(),
        table.metadata().current_schema().clone(),
    );
    let rolling_builder = RollingFileWriterBuilder::new_with_default_file_size(
        parquet_builder,
        table.file_io().clone(),
        location_generator,
        file_name_generator,
    );
    let data_file_builder = DataFileWriterBuilder::new(rolling_builder);
    let mut writer: <DataFileWriterBuilder<
        ParquetWriterBuilder,
        DefaultLocationGenerator,
        DefaultFileNameGenerator,
    > as IcebergWriterBuilder>::R = data_file_builder
        .build(None)
        .await
        .map_err(|e| format!("build iceberg data file writer failed: {e}"))?;

    for chunk in chunks {
        if chunk.batch.num_rows() == 0 {
            continue;
        }
        // Re-cast to the iceberg-annotated schema so that field ids are present.
        let annotated = arrow::record_batch::RecordBatch::try_new(
            Arc::clone(&iceberg_arrow_schema_ref),
            chunk.batch.columns().to_vec(),
        )
        .map_err(|e| format!("re-annotate batch with iceberg field ids failed: {e}"))?;
        writer
            .write(annotated)
            .await
            .map_err(|e| format!("iceberg data file write failed: {e}"))?;
    }

    writer
        .close()
        .await
        .map_err(|e| format!("iceberg data file writer close failed: {e}"))
}

/// Commit a fast-append transaction adding `data_files` to `table`.
///
/// Returns the snapshot id of the newly created snapshot.
pub(crate) async fn commit_fast_append(
    table: &iceberg::table::Table,
    catalog: &Arc<dyn iceberg::Catalog>,
    data_files: Vec<iceberg::spec::DataFile>,
) -> Result<i64, String> {
    let txn = Transaction::new(table);
    let action = txn.fast_append().add_data_files(data_files);
    let txn = action
        .apply(txn)
        .map_err(|e| format!("iceberg fast_append apply failed: {e}"))?;
    let updated_table = txn
        .commit(catalog.as_ref())
        .await
        .map_err(|e| format!("iceberg transaction commit failed: {e}"))?;
    updated_table
        .metadata()
        .current_snapshot()
        .map(|s| s.snapshot_id())
        .ok_or_else(|| {
            "iceberg commit succeeded but resulting table has no current snapshot".to_string()
        })
}

pub(crate) fn drop_iceberg_mv(
    state: &Arc<StandaloneState>,
    current_database: &str,
    stmt: &DropMaterializedViewStmt,
) -> Result<StatementResult, String> {
    let (db_name, mv_name) = resolve_mv_name(&stmt.name, current_database)?;
    let cfg = state
        .managed_lake_config
        .as_ref()
        .ok_or_else(|| "managed lake config required".to_string())?
        .clone();
    let metadata_store = state
        .metadata_store
        .as_ref()
        .ok_or_else(|| "sqlite metadata store required".to_string())?;

    // Look up the MV by its iceberg table identifier in SQLite.
    let snapshot = metadata_store.load_snapshot()?.managed;
    let expected_id = format!("{NOVA_MV_CATALOG_NAME}.{db_name}.{mv_name}");
    let mv_row = snapshot
        .materialized_views
        .iter()
        .find(|m| m.iceberg_table_identifier.as_deref() == Some(expected_id.as_str()))
        .cloned();
    let Some(mv_row) = mv_row else {
        if stmt.if_exists {
            return Ok(StatementResult::Ok);
        }
        return Err(format!(
            "materialized view `{db_name}.{mv_name}` does not exist"
        ));
    };

    // 1. Remove SQLite rows atomically (materialized_views + tables) first.
    //    SQLite is the source of truth; iceberg drop follows.
    metadata_store.delete_iceberg_mv_row(mv_row.mv_id)?;

    // 2. Remove the in-memory catalog entry so subsequent queries do not
    //    resolve the dropped MV.
    {
        let mut catalog = state
            .catalog
            .write()
            .expect("standalone catalog write lock");
        // Ignore "unknown table" errors — the entry may already be absent
        // if the MV was registered in a prior session that did not survive.
        if let Err(e) = catalog.drop_table(&db_name, &mv_name) {
            tracing::warn!(
                "iceberg mv {db_name}.{mv_name}: in-memory catalog drop_table returned error \
                 (already gone or other): {e}"
            );
        }
    }

    // 3. Remove the stale entry from the in-memory managed_lake snapshot so
    //    that a same-session second DROP or a CREATE-after-DROP does not
    //    observe the old tables row.
    {
        let mut managed = state
            .managed_lake
            .write()
            .expect("standalone managed lake write lock");
        let mut snapshot = managed.snapshot.clone();
        snapshot.tables.retain(|t| t.table_id != mv_row.mv_id);
        managed.snapshot = snapshot;
    }

    // 4. Drop the iceberg table (deletes warehouse files best-effort via
    //    HadoopFileSystemCatalog::drop_table → FileIO::delete_prefix; file
    //    I/O errors are logged as warnings and do not propagate). The ??
    //    here therefore only surfaces data_block_on infrastructure errors.
    let catalog = build_nova_mv_catalog(&cfg)?;
    let ident =
        TableIdent::from_strs([&db_name, &mv_name]).map_err(|e| format!("table ident: {e}"))?;
    data_block_on(async { catalog.drop_table(&ident).await.map_err(|e| e.to_string()) })??;

    tracing::info!("iceberg mv {db_name}.{mv_name}: dropped successfully");
    Ok(StatementResult::Ok)
}

/// Build an Iceberg `Schema` from the MV's analyzed output columns.
/// Each column is mapped to a primitive Iceberg type; nullable columns become
/// optional fields, non-nullable columns become required fields.
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

/// After a successful REFRESH, update `state.catalog` with the current Iceberg
/// data files so that subsequent `SELECT * FROM mv` queries read the refreshed
/// data rather than the empty `files: vec![]` placeholder set at CREATE time.
///
/// This function loads the MV iceberg table, enumerates its data files, and
/// re-registers the MV in the in-memory catalog (overwrite allowed by
/// `InMemoryCatalog::register`). Column metadata is read from the existing
/// catalog entry that was set at CREATE time.
fn update_iceberg_mv_in_catalog(
    state: &Arc<StandaloneState>,
    cfg: &crate::connector::starrocks::managed::config::ManagedLakeConfig,
    db_name: &str,
    mv_name: &str,
) -> Result<(), String> {
    // Read existing column metadata from the in-memory catalog (set at CREATE time).
    let columns: Vec<ColumnDef> = {
        let guard = state.catalog.read().expect("standalone catalog read lock");
        match guard.get(db_name, mv_name) {
            Ok(def) => def.columns,
            Err(_) => {
                // MV not yet registered (e.g. fresh session after restart).
                // Derive columns from the iceberg table schema below.
                vec![]
            }
        }
    };

    let nova_catalog = build_nova_mv_catalog(cfg)?;
    let ident =
        TableIdent::from_strs([db_name, mv_name]).map_err(|e| format!("table ident: {e}"))?;

    // Load table and extract data files.
    // `extract_data_files_with_stats` is a sync wrapper that calls `block_on_iceberg`
    // internally. We call it outside of any outer `data_block_on` async block to avoid
    // nested block_on usage.
    let table = data_block_on(async {
        nova_catalog
            .load_table(&ident)
            .await
            .map_err(|e| format!("load mv table for catalog update: {e}"))
    })??;
    let data_files =
        crate::connector::iceberg::catalog::registry::extract_data_files_with_stats(&table)?;

    // Build the appropriate TableStorage depending on whether the warehouse is S3 or local.
    let warehouse = cfg.mv_iceberg_warehouse();
    let is_s3 = warehouse.starts_with("s3://")
        || warehouse.starts_with("s3a://")
        || warehouse.starts_with("oss://");

    let storage = if is_s3 {
        let mut cloud_properties = std::collections::BTreeMap::new();
        cloud_properties.insert("aws.s3.endpoint".to_string(), cfg.s3.endpoint.clone());
        cloud_properties.insert(
            "aws.s3.access_key".to_string(),
            cfg.s3.access_key_id.clone(),
        );
        cloud_properties.insert(
            "aws.s3.secret_key".to_string(),
            cfg.s3.access_key_secret.clone(),
        );
        if let Some(true) = cfg.s3.enable_path_style_access {
            cloud_properties.insert(
                "aws.s3.enable_path_style_access".to_string(),
                "true".to_string(),
            );
        }
        TableStorage::S3ParquetFiles {
            files: data_files
                .into_iter()
                .map(|f| S3FileInfo {
                    path: f.path,
                    size: f.size,
                    row_count: f.record_count,
                    column_stats: f.column_stats,
                })
                .collect(),
            cloud_properties,
        }
    } else {
        // Local filesystem warehouse (file:// URI).
        //
        // We use S3ParquetFiles with empty cloud_properties instead of
        // LocalParquetFile for two reasons:
        //   1. LocalParquetFile is a single-path variant; after a second
        //      incremental REFRESH writes a second Parquet file, all but
        //      the first would be silently dropped.
        //   2. The standalone scan path in fs/path.rs classifies "file://"
        //      URIs as ScanPathScheme::Local and handles them correctly, so
        //      S3ParquetFiles with file:// paths does not require any S3
        //      credentials and reads from the local filesystem.
        if data_files.is_empty() {
            // No data files yet (empty delta) — leave unchanged.
            tracing::debug!(
                "update_iceberg_mv_in_catalog: {db_name}.{mv_name} has no data files after refresh; \
                 catalog storage not updated"
            );
            return Ok(());
        }
        TableStorage::S3ParquetFiles {
            files: data_files
                .into_iter()
                .map(|f| S3FileInfo {
                    path: f.path,
                    size: f.size,
                    row_count: f.record_count,
                    column_stats: f.column_stats,
                })
                .collect(),
            // No cloud credentials needed — the file:// scan path reads
            // directly from the local filesystem.
            cloud_properties: std::collections::BTreeMap::new(),
        }
    };

    let table_def = TableDef {
        name: mv_name.to_string(),
        columns,
        storage,
    };

    let mut catalog_guard = state
        .catalog
        .write()
        .expect("standalone catalog write lock");
    // create_database is idempotent — ok if already exists.
    let _ = catalog_guard.create_database(db_name);
    catalog_guard
        .register(db_name, table_def)
        .map_err(|e| format!("update iceberg mv {db_name}.{mv_name} in standalone catalog: {e}"))
}

/// Allocate a managed-lake `tables` row for an Iceberg-backed MV so that
/// `SHOW MATERIALIZED VIEWS` and the name-collision check work uniformly
/// across both storage engines. No tablets, schemas, partitions, or
/// indexes are allocated for the iceberg storage path.
fn allocate_iceberg_mv_table_row(
    state: &Arc<StandaloneState>,
    db_name: &str,
    mv_name: &str,
) -> Result<i64, String> {
    use crate::connector::starrocks::managed::ddl::{
        initialize_global_meta_if_needed, reclaim_dropping_table_for_reuse,
    };

    let cfg = state
        .managed_lake_config
        .as_ref()
        .ok_or_else(|| "managed lake config required".to_string())?;
    let mut managed = state
        .managed_lake
        .write()
        .expect("standalone managed lake write lock");
    let mut snapshot = managed.snapshot.clone();
    initialize_global_meta_if_needed(&mut snapshot, cfg);
    let database = find_or_create_managed_database(&mut snapshot, db_name);
    reclaim_dropping_table_for_reuse(&mut snapshot, database.db_id, mv_name)?;
    let table_id = alloc_id(&mut snapshot.global.next_table_id);
    snapshot.tables.push(StoredManagedTable {
        table_id,
        db_id: database.db_id,
        name: mv_name.to_string(),
        keys_type: "DUP_KEYS".to_string(),
        bucket_num: 0,
        current_schema_id: 0,
        state: ManagedTableState::Active,
        kind: ManagedTableKind::MaterializedView,
    });
    let metadata_store = state
        .metadata_store
        .as_ref()
        .ok_or_else(|| "sqlite metadata store required".to_string())?;
    metadata_store.replace_managed_snapshot(&snapshot)?;
    managed.snapshot = snapshot;
    Ok(table_id)
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Int32Array, Int64Array};
    use arrow::datatypes::{DataType, Field, Schema as ArrowSchema};
    use arrow::record_batch::RecordBatch;
    use std::sync::Arc as StdArc;

    fn output_col(name: &str, ty: DataType, nullable: bool) -> OutputColumn {
        OutputColumn {
            name: name.to_string(),
            data_type: ty,
            nullable,
        }
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
        use crate::connector::iceberg::catalog::hadoop_catalog::HadoopFileSystemCatalog;
        use iceberg::Catalog;
        use iceberg::io::{FileIOBuilder, LocalFsStorageFactory};

        let dir = tempfile::tempdir().expect("tempdir");
        let warehouse = format!("file://{}/wh", dir.path().display());

        // Build FileIO using LocalFsStorageFactory — same pattern as build_file_io
        // in mv_iceberg_catalog.rs.
        let file_io = FileIOBuilder::new(
            StdArc::new(LocalFsStorageFactory) as StdArc<dyn iceberg::io::StorageFactory>
        )
        .build();
        let catalog: StdArc<dyn Catalog> = StdArc::new(HadoopFileSystemCatalog::new(
            file_io.clone(),
            warehouse.clone(),
        ));

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

            let snapshot_id = commit_fast_append(&table, &catalog, written).await.unwrap();
            assert!(snapshot_id != 0, "snapshot id must be non-zero");

            // Reload from catalog and confirm snapshot matches.
            let reloaded = catalog
                .load_table(&iceberg::TableIdent::from_strs(["test_ns", "t"]).unwrap())
                .await
                .unwrap();
            assert_eq!(
                reloaded
                    .metadata()
                    .current_snapshot()
                    .map(|s| s.snapshot_id()),
                Some(snapshot_id),
            );
        });
    }
}
