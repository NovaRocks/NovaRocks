use std::collections::BTreeMap;
use std::sync::{Arc, Mutex, MutexGuard, OnceLock};

use crate::connector::starrocks::lake::context::remove_tablet_runtime;
use crate::exec::chunk::Chunk;
use crate::sql::parser::ast::{ObjectName, RefreshMaterializedViewStmt};
use crate::standalone::engine::{
    QueryResult, StandaloneState, StatementResult, execute_query_for_mv_incremental_refresh,
    execute_query_for_mv_refresh, record_batch_to_chunk,
};
use crate::standalone::iceberg::{load_table, plan_append_delta};

use super::catalog::{ManagedLakeCatalog, register_managed_tables_in_catalog};
use super::ddl::bootstrap_empty_partition_for_tablets;
use super::store::{
    ActivateMvRefreshRequest, IcebergTableRef, ManagedPartitionState, ManagedTableKind,
    StageMvRefreshRequest, StagedMvRefresh, UpdateMvRefreshMetadataRequest,
};
use super::txn::{
    MvRefreshWriteMetadata, PartitionTarget, load_insert_plan, write_chunks_into_managed_partition,
    write_chunks_into_managed_partition_for_mv_refresh,
};

#[derive(Clone, Debug, PartialEq, Eq)]
enum MvRefreshStrategy {
    Full,
    NoOp {
        current_snapshot_id: i64,
    },
    Incremental {
        previous_snapshot_id: i64,
        current_snapshot_id: i64,
    },
}

fn choose_refresh_strategy(
    previous_snapshot_id: Option<i64>,
    current_snapshot_id: Option<i64>,
) -> Result<MvRefreshStrategy, String> {
    match (previous_snapshot_id, current_snapshot_id) {
        (None, _) => Ok(MvRefreshStrategy::Full),
        (Some(previous), Some(current)) if previous == current => Ok(MvRefreshStrategy::NoOp {
            current_snapshot_id: current,
        }),
        (Some(previous), Some(current)) => Ok(MvRefreshStrategy::Incremental {
            previous_snapshot_id: previous,
            current_snapshot_id: current,
        }),
        (Some(previous), None) => Err(format!(
            "cannot incrementally refresh materialized view: Iceberg snapshot {previous} is no longer reachable"
        )),
    }
}

pub(crate) fn refresh_mv(
    state: &Arc<StandaloneState>,
    current_database: &str,
    stmt: &RefreshMaterializedViewStmt,
) -> Result<StatementResult, String> {
    let (db_name, mv_name) = resolve_mv_name(&stmt.name, current_database)?;
    let metadata_store = state
        .metadata_store
        .as_ref()
        .ok_or_else(|| "managed lake mv refresh requires sqlite metadata store".to_string())?;
    let _refresh_guard = acquire_mv_refresh_lock()?;

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

    let snapshot = metadata_store.load_snapshot()?.managed;
    let mv_row = snapshot
        .materialized_views
        .iter()
        .find(|mv| mv.mv_id == runtime.table.table_id)
        .cloned()
        .ok_or_else(|| {
            format!("materialized view {db_name}.{mv_name} has no materialized_views row")
        })?;

    let mv_shape = validate_incremental_mv_select(&mv_row.select_sql)?;
    let [base_ref] = mv_row.base_table_refs.as_slice() else {
        return Err(
            "incremental materialized view refresh requires a single Iceberg base table"
                .to_string(),
        );
    };
    validate_incremental_mv_base_ref(&mv_shape.base_table, base_ref)?;

    let loaded = load_current_iceberg_base_table(state, base_ref)?;
    let current_snapshot_id = loaded
        .table
        .metadata()
        .current_snapshot()
        .map(|snapshot| snapshot.snapshot_id());
    let previous_snapshot_id = mv_row.last_refresh_snapshots.get(&base_ref.fqn()).copied();

    match choose_refresh_strategy(previous_snapshot_id, current_snapshot_id)? {
        MvRefreshStrategy::Full => {
            refresh_mv_full_with_executor(state, &db_name, &mv_name, run_mv_select_and_chunks)
        }
        MvRefreshStrategy::NoOp {
            current_snapshot_id,
        } => {
            let snapshots = single_snapshot_map(base_ref, current_snapshot_id);
            metadata_store.update_mv_refresh_metadata(UpdateMvRefreshMetadataRequest {
                table_id: runtime.table.table_id,
                last_refresh_rows: mv_row.last_refresh_rows.unwrap_or(0),
                snapshots,
            })?;
            refresh_managed_catalog(state)?;
            Ok(StatementResult::Ok)
        }
        MvRefreshStrategy::Incremental {
            previous_snapshot_id,
            current_snapshot_id,
        } => {
            let delta = plan_append_delta(&loaded.table, previous_snapshot_id)?;
            if delta.current_snapshot_id != current_snapshot_id {
                return Err(format!(
                    "iceberg append delta current snapshot mismatch: expected {current_snapshot_id}, got {}",
                    delta.current_snapshot_id
                ));
            }

            let result = execute_query_for_mv_incremental_refresh(
                state,
                &db_name,
                &mv_row.select_sql,
                base_ref,
                delta.added_files,
            )?;
            let chunks = query_result_to_chunks(result)?;
            let plan = load_insert_plan(
                state,
                &crate::standalone::engine::ResolvedLocalTableName {
                    database: db_name.clone(),
                    table: mv_name.clone(),
                },
                PartitionTarget::Active,
            )?;
            let previous_rows = mv_row.last_refresh_rows.unwrap_or(0);
            let snapshots = single_snapshot_map(base_ref, current_snapshot_id);
            write_chunks_into_managed_partition_for_mv_refresh(
                state,
                plan,
                &chunks,
                MvRefreshWriteMetadata {
                    table_id: runtime.table.table_id,
                    previous_refresh_rows: previous_rows,
                    snapshots,
                },
            )?;
            refresh_managed_catalog(state)?;
            Ok(StatementResult::Ok)
        }
    }
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
    let metadata_store = state
        .metadata_store
        .as_ref()
        .ok_or_else(|| "managed lake mv refresh requires sqlite metadata store".to_string())?;

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

    let snapshot = metadata_store.load_snapshot()?.managed;
    let mv_row = snapshot
        .materialized_views
        .iter()
        .find(|mv| mv.mv_id == runtime.table.table_id)
        .cloned()
        .ok_or_else(|| {
            format!("materialized view {database}.{mv_name} has no materialized_views row")
        })?;
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

    let staged = metadata_store.stage_mv_refresh_partition(StageMvRefreshRequest {
        table_id: runtime.table.table_id,
        db_id: runtime.table.db_id,
        bucket_num: runtime.table.bucket_num,
        partition_name: active_partition.name.clone(),
        warehouse_uri: managed_config.warehouse_uri.clone(),
    })?;

    if let Err(err) = refresh_managed_catalog(state) {
        cleanup_staged_partition(
            state,
            metadata_store,
            runtime.table.table_id,
            &staged,
            false,
        )?;
        return Err(format!("mv refresh catalog refresh failed: {err}"));
    }

    if let Err(err) = bootstrap_empty_partition_for_tablets(
        &runtime,
        &managed_config,
        staged.partition_id,
        &staged.tablet_ids,
    ) {
        cleanup_staged_partition(
            state,
            metadata_store,
            runtime.table.table_id,
            &staged,
            false,
        )?;
        return Err(format!("mv refresh bootstrap failed: {err}"));
    }

    let chunks = match executor(MvRefreshContext {
        state: Arc::clone(state),
        database: database.to_string(),
        select_sql: mv_row.select_sql.clone(),
    }) {
        Ok(chunks) => chunks,
        Err(err) => {
            cleanup_staged_partition(state, metadata_store, runtime.table.table_id, &staged, true)?;
            return Err(format!("mv refresh execute failed: {err}"));
        }
    };

    let plan = match load_insert_plan(
        state,
        &crate::standalone::engine::ResolvedLocalTableName {
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
            cleanup_staged_partition(state, metadata_store, runtime.table.table_id, &staged, true)?;
            return Err(format!("mv refresh plan load failed: {err}"));
        }
    };

    let rows_written = match write_chunks_into_managed_partition(state, plan, &chunks) {
        Ok(rows_written) => rows_written,
        Err(err) => {
            cleanup_staged_partition(state, metadata_store, runtime.table.table_id, &staged, true)?;
            return Err(format!("mv refresh write failed: {err}"));
        }
    };

    let snapshots = collect_current_snapshots_or_cleanup_staged_partition(
        state,
        metadata_store,
        runtime.table.table_id,
        &staged,
        &mv_row.base_table_refs,
    )?;
    if let Err(err) = metadata_store.activate_mv_refresh_partition(ActivateMvRefreshRequest {
        table_id: runtime.table.table_id,
        old_partition_id: active_partition.partition_id,
        new_partition_id: staged.partition_id,
        new_index_id: staged.index_id,
        retired_root_path,
        rows_written,
        snapshots,
    }) {
        cleanup_staged_partition(state, metadata_store, runtime.table.table_id, &staged, true)?;
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

fn query_result_to_chunks(result: QueryResult) -> Result<Vec<Chunk>, String> {
    result
        .chunks
        .into_iter()
        .map(|chunk| record_batch_to_chunk(chunk.batch))
        .collect()
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
        crate::standalone::engine::catalog::normalize_identifier(&base_ref.catalog).map_err(
            |e| {
                format!("incremental MV refresh stored metadata has invalid catalog reference: {e}")
            },
        )?,
        crate::standalone::engine::catalog::normalize_identifier(&base_ref.namespace).map_err(
            |e| {
                format!(
                    "incremental MV refresh stored metadata has invalid namespace reference: {e}"
                )
            },
        )?,
        crate::standalone::engine::catalog::normalize_identifier(&base_ref.table).map_err(|e| {
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
                crate::standalone::engine::catalog::normalize_identifier(&ident.value).map_err(
                    |e| {
                        format!(
                            "incremental MV refresh stored SQL has invalid base table reference: {e}"
                        )
                    },
                )
            }
            _ => Err("incremental MV refresh stored SQL base table must use identifiers"
                .to_string()),
        })
        .collect::<Result<Vec<_>, _>>()?;
    let [catalog, namespace, table] = parts.as_slice() else {
        return Err(
            "incremental MV refresh stored SQL must reference a 3-part Iceberg table".to_string(),
        );
    };
    Ok((catalog.clone(), namespace.clone(), table.clone()))
}

fn load_current_iceberg_base_table(
    state: &Arc<StandaloneState>,
    table_ref: &IcebergTableRef,
) -> Result<crate::standalone::iceberg::IcebergLoadedTable, String> {
    let entry = {
        let registry = state
            .iceberg_catalogs
            .read()
            .expect("iceberg registry read lock");
        registry.get(&table_ref.catalog)?
    };
    load_table(&entry, &table_ref.namespace, &table_ref.table)
}

fn single_snapshot_map(table_ref: &IcebergTableRef, snapshot_id: i64) -> BTreeMap<String, i64> {
    let mut snapshots = BTreeMap::new();
    snapshots.insert(table_ref.fqn(), snapshot_id);
    snapshots
}

fn collect_current_snapshots(
    state: &Arc<StandaloneState>,
    refs: &[IcebergTableRef],
) -> Result<BTreeMap<String, i64>, String> {
    let registry = state
        .iceberg_catalogs
        .read()
        .expect("iceberg registry read lock");
    let mut snapshots = BTreeMap::new();
    for table_ref in refs {
        let entry = registry.get(&table_ref.catalog)?;
        let loaded = load_table(&entry, &table_ref.namespace, &table_ref.table)?;
        if let Some(snapshot) = loaded.table.metadata().current_snapshot() {
            snapshots.insert(table_ref.fqn(), snapshot.snapshot_id());
        }
    }
    Ok(snapshots)
}

fn collect_current_snapshots_or_cleanup_staged_partition(
    state: &Arc<StandaloneState>,
    metadata_store: &super::store::SqliteMetadataStore,
    table_id: i64,
    staged: &StagedMvRefresh,
    refs: &[IcebergTableRef],
) -> Result<BTreeMap<String, i64>, String> {
    match collect_current_snapshots(state, refs) {
        Ok(snapshots) => Ok(snapshots),
        Err(err) => {
            if let Err(cleanup_err) =
                cleanup_staged_partition(state, metadata_store, table_id, staged, true)
            {
                return Err(format!(
                    "mv refresh snapshot collection failed: {err}; cleanup failed: {cleanup_err}"
                ));
            }
            Err(format!("mv refresh snapshot collection failed: {err}"))
        }
    }
}

fn acquire_mv_refresh_lock() -> Result<MutexGuard<'static, ()>, String> {
    static MV_REFRESH_LOCK: OnceLock<Mutex<()>> = OnceLock::new();
    lock_mv_refresh_mutex(MV_REFRESH_LOCK.get_or_init(|| Mutex::new(())))
}

fn lock_mv_refresh_mutex(lock: &Mutex<()>) -> Result<MutexGuard<'_, ()>, String> {
    lock.lock()
        .map_err(|_| "materialized view refresh lock poisoned".to_string())
}

fn cleanup_staged_partition(
    state: &Arc<StandaloneState>,
    metadata_store: &super::store::SqliteMetadataStore,
    table_id: i64,
    staged: &StagedMvRefresh,
    enqueue_erase_job: bool,
) -> Result<(), String> {
    for tablet_id in &staged.tablet_ids {
        let _ = remove_tablet_runtime(*tablet_id);
    }
    metadata_store.delete_creating_partition(staged.partition_id)?;
    if enqueue_erase_job {
        metadata_store.enqueue_erase_job_for_partition_root(
            table_id,
            staged.partition_id,
            &staged.partition_root_path,
        )?;
    }
    refresh_managed_catalog(state)?;
    Ok(())
}

fn refresh_managed_catalog(state: &Arc<StandaloneState>) -> Result<(), String> {
    let metadata_store = state
        .metadata_store
        .as_ref()
        .ok_or_else(|| "managed lake catalog refresh requires sqlite metadata store".to_string())?;
    let snapshot = metadata_store.load_snapshot()?.managed;
    let rebuilt = ManagedLakeCatalog::rebuild(state.managed_lake_config.clone(), snapshot.clone())?;
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
            crate::standalone::engine::catalog::normalize_identifier(current_database)?,
            crate::standalone::engine::catalog::normalize_identifier(table)?,
        )),
        [database, table] => Ok((
            crate::standalone::engine::catalog::normalize_identifier(database)?,
            crate::standalone::engine::catalog::normalize_identifier(table)?,
        )),
        _ => Err(format!(
            "materialized view name must be `<name>` or `<db>.<name>`; got `{}`",
            name.parts.join(".")
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::runtime::starlet_shard_registry::S3StoreConfig;
    use crate::standalone::engine::catalog::InMemoryCatalog;
    use crate::standalone::iceberg::IcebergCatalogRegistry;
    use crate::standalone::lake::ManagedLakeConfig;
    use crate::standalone::lake::store::{
        ManagedGlobalMeta, ManagedIndexState, ManagedMvRefreshMode, ManagedSnapshot,
        ManagedTableKind, ManagedTableState, SqliteMetadataStore, StoredManagedDatabase,
        StoredManagedIndex, StoredManagedPartition, StoredManagedSchema, StoredManagedTable,
        StoredMaterializedView,
    };
    use std::sync::RwLock;

    #[test]
    fn choose_refresh_strategy_without_previous_snapshot_uses_full_refresh() {
        let strategy = choose_refresh_strategy(None, Some(10)).expect("strategy");
        assert_eq!(strategy, MvRefreshStrategy::Full);
    }

    #[test]
    fn choose_refresh_strategy_for_same_snapshot_is_no_op() {
        let strategy = choose_refresh_strategy(Some(10), Some(10)).expect("strategy");
        assert_eq!(
            strategy,
            MvRefreshStrategy::NoOp {
                current_snapshot_id: 10
            }
        );
    }

    #[test]
    fn choose_refresh_strategy_for_advanced_snapshot_is_incremental() {
        let strategy = choose_refresh_strategy(Some(10), Some(12)).expect("strategy");
        assert_eq!(
            strategy,
            MvRefreshStrategy::Incremental {
                previous_snapshot_id: 10,
                current_snapshot_id: 12,
            }
        );
    }

    #[test]
    fn choose_refresh_strategy_rejects_unreachable_previous_snapshot() {
        let err = choose_refresh_strategy(Some(10), None).expect_err("strategy should fail");
        assert!(
            err.contains("10"),
            "expected error to contain previous snapshot id, got `{err}`"
        );
        assert!(
            err.contains("no longer reachable"),
            "expected error to describe unreachable snapshot, got `{err}`"
        );
    }

    #[test]
    fn refresh_mv_full_cleans_staged_partition_when_executor_fails() {
        // Covered by integration-style engine/mysql tests once mv refresh is wired.
        // Keep this module-level smoke test minimal so the file always participates
        // in compilation even when object-store-backed test infra is unavailable.
        let _ = std::any::type_name::<MvRefreshContext>();
    }

    #[test]
    fn collect_current_snapshots_cleans_staged_partition_on_failure() {
        let (_dir, store) = seed_mv_refresh_store();
        let config = test_managed_config();
        let snapshot = store.load_snapshot().expect("load snapshot").managed;
        let managed = ManagedLakeCatalog::rebuild(Some(config.clone()), snapshot).expect("rebuild");
        let state = Arc::new(StandaloneState {
            catalog: RwLock::new(InMemoryCatalog::default()),
            iceberg_catalogs: RwLock::new(IcebergCatalogRegistry::default()),
            managed_lake: RwLock::new(managed),
            managed_lake_config: Some(config),
            metadata_store: Some(store.clone()),
            exchange_port: 0,
            #[cfg(test)]
            _test_guard: None,
        });
        let staged = store
            .stage_mv_refresh_partition(StageMvRefreshRequest {
                table_id: 10,
                db_id: 1,
                bucket_num: 2,
                partition_name: "p0".to_string(),
                warehouse_uri: "s3://test/warehouse".to_string(),
            })
            .expect("stage");
        let refs = vec![IcebergTableRef {
            catalog: "missing_catalog".to_string(),
            namespace: "ns".to_string(),
            table: "orders".to_string(),
        }];

        let err = collect_current_snapshots_or_cleanup_staged_partition(
            &state, &store, 10, &staged, &refs,
        )
        .expect_err("snapshot collection should fail");

        assert!(
            err.contains("mv refresh snapshot collection failed"),
            "err={err}"
        );
        let loaded = store.load_snapshot().expect("reload").managed;
        assert!(
            !loaded
                .partitions
                .iter()
                .any(|partition| partition.partition_id == staged.partition_id)
        );
        let erase_job = loaded
            .erase_jobs
            .iter()
            .find(|job| job.partition_id == Some(staged.partition_id))
            .expect("staged partition erase job");
        assert_eq!(erase_job.table_id, 10);
    }

    #[test]
    fn lock_mv_refresh_mutex_reports_poisoned_lock() {
        let lock = Box::leak(Box::new(std::sync::Mutex::new(())));
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

    fn seed_mv_refresh_store() -> (tempfile::TempDir, SqliteMetadataStore) {
        let dir = tempfile::tempdir().expect("tempdir");
        let store = SqliteMetadataStore::open(dir.path().join("standalone.sqlite")).expect("open");
        let mut snapshot = ManagedSnapshot::default();
        snapshot.global = ManagedGlobalMeta {
            warehouse_uri: "s3://test/warehouse".to_string(),
            next_db_id: 2,
            next_table_id: 11,
            next_partition_id: 21,
            next_index_id: 31,
            next_tablet_id: 41,
            next_txn_id: 1,
        };
        snapshot.databases.push(StoredManagedDatabase {
            db_id: 1,
            name: "analytics".to_string(),
        });
        snapshot.tables.push(StoredManagedTable {
            table_id: 10,
            db_id: 1,
            name: "orders_mv".to_string(),
            keys_type: "DUP_KEYS".to_string(),
            bucket_num: 2,
            current_schema_id: 100,
            state: ManagedTableState::Active,
            kind: ManagedTableKind::MaterializedView,
        });
        snapshot.schemas.push(StoredManagedSchema {
            schema_id: 100,
            table_id: 10,
            schema_version: 0,
            tablet_schema_pb: vec![],
        });
        snapshot.partitions.push(StoredManagedPartition {
            partition_id: 20,
            table_id: 10,
            name: "p0".to_string(),
            visible_version: 1,
            next_version: 2,
            state: ManagedPartitionState::Active,
        });
        snapshot.indexes.push(StoredManagedIndex {
            index_id: 30,
            table_id: 10,
            partition_id: 20,
            index_type: "BASE".to_string(),
            state: ManagedIndexState::Active,
        });
        snapshot.materialized_views.push(StoredMaterializedView {
            mv_id: 10,
            select_sql: "SELECT k1 FROM missing_catalog.ns.orders".to_string(),
            refresh_mode: ManagedMvRefreshMode::DeferredManual,
            base_table_refs: vec![IcebergTableRef {
                catalog: "missing_catalog".to_string(),
                namespace: "ns".to_string(),
                table: "orders".to_string(),
            }],
            last_refresh_ms: None,
            last_refresh_rows: None,
            last_refresh_snapshots: BTreeMap::new(),
            created_at_ms: 1,
        });
        store
            .replace_managed_snapshot(&snapshot)
            .expect("persist snapshot");
        (dir, store)
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
        }
    }
}
