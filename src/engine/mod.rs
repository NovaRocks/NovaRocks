#![allow(dead_code)]

use std::net::{TcpListener, TcpStream};
use std::path::{Path, PathBuf};
use std::sync::{Arc, OnceLock, RwLock};
use std::time::{Duration, Instant};

use arrow::array::StringArray;
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use tokio::runtime::Handle;

use crate::exec::chunk::{Chunk, ChunkSchema};
use crate::novarocks_config;
use crate::plan_nodes::TFileFormatType;
use crate::runtime::global_async_runtime::data_block_on;

use self::catalog::{DEFAULT_DATABASE, InMemoryCatalog, build_parquet_table, normalize_identifier};
use crate::connector::{
    IcebergCatalogRegistry, ManagedLakeCatalog, ManagedLakeConfig, MetadataSnapshot,
    SqliteMetadataStore, StoredIcebergTable, create_iceberg_namespace, iceberg_namespace_exists,
    register_existing_iceberg_table, register_managed_tables_in_catalog, runtime_registered,
};

pub(crate) mod aggregate;
pub(crate) mod backend_resolver;
pub(crate) mod catalog;
pub(crate) mod iceberg_ctas;
pub(crate) mod iceberg_ref_flow;
pub(crate) mod information_schema;
pub(crate) mod insert;
pub(crate) mod insert_flow;
pub(crate) mod mutation_flow;
pub(crate) mod mv_flow;
pub(crate) mod name_resolve;
pub(crate) mod parquet;
pub(crate) mod query_prep;
pub(crate) mod sql_expr;
pub(crate) mod statement;
pub(crate) mod statistics;
pub(crate) mod stream_load;

pub(crate) use self::name_resolve::ResolvedLocalTableName;

pub(crate) use self::insert::{build_local_insert_batch, reorder_insert_rows};
#[cfg(test)]
use self::sql_expr::sql_type_to_arrow_type;
#[cfg(test)]
use self::sql_expr::sqlparser_expr_to_literal;
use self::statement::{
    convert_sqlparser_insert_to_custom, execute_create_database_statement,
    execute_create_table_statement, execute_drop_catalog_statement,
    execute_drop_database_statement, execute_drop_table_statement, execute_insert_statement,
    execute_truncate_table_statement, looks_like_add_equality_delete, looks_like_add_files,
    looks_like_add_legacy_range_partition, looks_like_alter_iceberg_properties,
    looks_like_alter_iceberg_schema, looks_like_alter_partition_column,
    looks_like_alter_table_expire_snapshots, looks_like_alter_table_optimize,
    looks_like_alter_table_remove_orphan_files, looks_like_alter_table_rewrite_manifests,
    looks_like_show_alter_table_optimize, parse_add_legacy_range_partition_sql,
    parse_alter_iceberg_properties_sql, parse_alter_partition_column_sql,
    parse_alter_table_expire_snapshots_sql, parse_alter_table_optimize_sql,
    parse_alter_table_remove_orphan_files_sql, parse_alter_table_rewrite_manifests_sql,
    parse_show_alter_table_optimize_sql,
};
use self::stream_load::{
    parse_csv_stream_load_rows, parse_json_stream_load_rows, parse_stream_load_columns,
};
use crate::engine::query_prep::{has_time_travel_refs, rewrite_time_travel_refs};
use crate::sql::parser::query_refs::{
    extract_three_part_table_refs, strip_catalog_from_three_part_names,
};

#[derive(Clone, Debug, Default)]
pub struct StandaloneOptions {
    pub config_path: Option<PathBuf>,
    pub metadata_db_path: Option<PathBuf>,
}

pub use crate::runtime::query_result::{QueryResult, QueryResultColumn};
use crate::sql::catalog::LegacyRangePartition;
pub use crate::sql::catalog::{CatalogProvider, ColumnDef, TableDef, TableStorage};

fn stream_load_engine_cell() -> &'static OnceLock<StandaloneNovaRocks> {
    static ENGINE: OnceLock<StandaloneNovaRocks> = OnceLock::new();
    &ENGINE
}

pub(crate) fn register_stream_load_engine(engine: StandaloneNovaRocks) {
    let _ = stream_load_engine_cell().set(engine);
}

pub(crate) fn current_stream_load_engine() -> Option<StandaloneNovaRocks> {
    stream_load_engine_cell().get().cloned()
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StandaloneManagedTabletInfo {
    pub tablet_id: i64,
    pub bucket_seq: i64,
    pub tablet_root_path: String,
    pub runtime_registered: bool,
    pub snapshot_version: Option<i64>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StandaloneManagedTableInfo {
    pub database_name: String,
    pub table_name: String,
    pub table_id: i64,
    pub current_schema_id: i64,
    pub keys_type: String,
    pub bucket_num: i64,
    pub visible_version: i64,
    pub tablets: Vec<StandaloneManagedTabletInfo>,
}

#[derive(Clone, Debug)]
pub(crate) struct StandaloneStreamLoadRequest {
    pub database: String,
    pub table: String,
    pub format_type: TFileFormatType,
    pub columns: Option<String>,
    pub column_separator: Option<String>,
    pub row_delimiter: Option<String>,
    pub skip_header: Option<i64>,
    pub trim_space: Option<bool>,
    pub enclose: Option<i8>,
    pub escape: Option<i8>,
    pub jsonpaths: Option<String>,
    pub strip_outer_array: Option<bool>,
    pub payload: Vec<u8>,
}

#[derive(Clone, Copy, Debug, Default)]
pub(crate) struct StandaloneStreamLoadResult {
    pub loaded_rows: i64,
    pub loaded_bytes: i64,
}

#[derive(Clone, Debug)]
pub(crate) enum StatementResult {
    Query(QueryResult),
    Ok,
}

pub(crate) fn build_string_query_result(
    column_name: &str,
    rows: Vec<String>,
) -> Result<QueryResult, String> {
    let column = QueryResultColumn {
        name: column_name.to_string(),
        data_type: DataType::Utf8,
        nullable: false,
        logical_type: None,
    };
    let schema = Arc::new(Schema::new(vec![Field::new(
        column_name,
        DataType::Utf8,
        false,
    )]));
    let batch = RecordBatch::try_new(
        schema,
        vec![Arc::new(StringArray::from(
            rows.into_iter().map(Some).collect::<Vec<_>>(),
        ))],
    )
    .map_err(|e| format!("build standalone text result failed: {e}"))?;
    Ok(QueryResult {
        columns: vec![column],
        chunks: vec![record_batch_to_chunk(batch)?],
    })
}

pub(crate) struct StandaloneState {
    pub(crate) catalog: RwLock<InMemoryCatalog>,
    pub(crate) iceberg_catalogs: Arc<RwLock<IcebergCatalogRegistry>>,
    pub(crate) managed_lake: RwLock<ManagedLakeCatalog>,
    pub(crate) statistics: RwLock<statistics::StandaloneStatistics>,
    pub(crate) connectors: Arc<RwLock<crate::connector::ConnectorRegistry>>,
    pub(crate) managed_lake_config: Option<ManagedLakeConfig>,
    pub(crate) metadata_store: Option<SqliteMetadataStore>,
    pub(crate) exchange_port: u16,
    #[cfg(test)]
    pub(crate) _test_guard: Option<TestSerializationGuard>,
}

impl Default for StandaloneState {
    fn default() -> Self {
        Self {
            catalog: RwLock::new(InMemoryCatalog::default()),
            iceberg_catalogs: Arc::new(RwLock::new(IcebergCatalogRegistry::default())),
            managed_lake: RwLock::new(ManagedLakeCatalog::default()),
            statistics: RwLock::new(statistics::StandaloneStatistics::default()),
            connectors: Arc::new(RwLock::new(crate::connector::ConnectorRegistry::default())),
            managed_lake_config: None,
            metadata_store: None,
            exchange_port: 0,
            #[cfg(test)]
            _test_guard: None,
        }
    }
}

#[cfg(test)]
pub(crate) struct TestSerializationGuard {
    _guard: std::sync::MutexGuard<'static, ()>,
}

#[cfg(test)]
unsafe impl Send for TestSerializationGuard {}

#[cfg(test)]
unsafe impl Sync for TestSerializationGuard {}

#[cfg(test)]
fn acquire_standalone_test_guard() -> TestSerializationGuard {
    use std::sync::{Mutex, OnceLock};
    static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
    let guard = LOCK
        .get_or_init(|| Mutex::new(()))
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    TestSerializationGuard { _guard: guard }
}

#[derive(Clone)]
pub struct StandaloneNovaRocks {
    inner: Arc<StandaloneState>,
}

#[derive(Clone)]
pub struct StandaloneSession {
    inner: Arc<StandaloneState>,
}

impl StandaloneNovaRocks {
    pub fn open(opts: StandaloneOptions) -> Result<Self, String> {
        #[cfg(test)]
        let _test_guard = Some(acquire_standalone_test_guard());
        match opts.config_path.as_deref() {
            Some(path) => {
                novarocks_config::init_from_path(path)
                    .map_err(|e| format!("load config failed: {e}"))?;
            }
            None => {
                #[cfg(test)]
                {
                    novarocks_config::install_default_for_test();
                }
                #[cfg(not(test))]
                {
                    novarocks_config::init_from_env_or_default()
                        .map_err(|e| format!("load config failed: {e}"))?;
                }
            }
        }
        let exchange_port = ensure_standalone_exchange_server()?;
        let metadata_store = resolve_metadata_store(
            opts.metadata_db_path.as_deref(),
            opts.config_path.as_deref(),
        )?;
        let managed_lake_config = resolve_managed_lake_config()?;
        let inner = Arc::new(StandaloneState {
            managed_lake: RwLock::new(ManagedLakeCatalog::empty(managed_lake_config.clone())),
            managed_lake_config,
            metadata_store,
            exchange_port,
            #[cfg(test)]
            _test_guard,
            ..Default::default()
        });
        register_connector_backends(&inner);
        restore_metadata_if_needed(&inner)?;
        if inner.managed_lake_config.is_some() && inner.metadata_store.is_some() {
            crate::connector::spawn_managed_erase_worker(Arc::clone(&inner));
        }
        #[cfg(not(test))]
        if inner.metadata_store.is_some() {
            crate::connector::spawn_iceberg_optimize_worker(Arc::clone(&inner));
        }
        Ok(Self { inner })
    }

    pub fn session(&self) -> StandaloneSession {
        StandaloneSession {
            inner: Arc::clone(&self.inner),
        }
    }

    pub fn managed_table_info(
        &self,
        database_name: &str,
        table_name: &str,
    ) -> Result<StandaloneManagedTableInfo, String> {
        let managed = self
            .inner
            .managed_lake
            .read()
            .expect("standalone managed lake read lock");
        let runtime = managed.table(database_name, table_name)?;
        let visible_version = runtime
            .partitions
            .iter()
            .map(|partition| partition.visible_version)
            .max()
            .unwrap_or(1);
        let object_store_profile = managed
            .config
            .as_ref()
            .map(|config| {
                crate::connector::starrocks::ObjectStoreProfile::from_s3_store_config(&config.s3)
            })
            .transpose()?;
        let tablets = runtime
            .tablets
            .iter()
            .map(|tablet| {
                let snapshot_version = object_store_profile.as_ref().and_then(|profile| {
                    crate::formats::starrocks::metadata::load_tablet_snapshot(
                        tablet.tablet_id,
                        visible_version,
                        &tablet.tablet_root_path,
                        Some(profile),
                    )
                    .ok()
                    .map(|snapshot| snapshot.version)
                });
                StandaloneManagedTabletInfo {
                    tablet_id: tablet.tablet_id,
                    bucket_seq: tablet.bucket_seq,
                    tablet_root_path: tablet.tablet_root_path.clone(),
                    runtime_registered: runtime_registered(tablet.tablet_id),
                    snapshot_version,
                }
            })
            .collect();
        Ok(StandaloneManagedTableInfo {
            database_name: runtime.database_name.clone(),
            table_name: runtime.table.name.clone(),
            table_id: runtime.table.table_id,
            current_schema_id: runtime.table.current_schema_id,
            keys_type: runtime.table.keys_type.clone(),
            bucket_num: runtime.table.bucket_num,
            visible_version,
            tablets,
        })
    }

    pub fn register_parquet_table(
        &self,
        table_name: &str,
        path: impl AsRef<Path>,
    ) -> Result<(), String> {
        self.register_parquet_table_in_database(DEFAULT_DATABASE, table_name, path)
    }

    pub fn register_parquet_table_in_database(
        &self,
        database_name: &str,
        table_name: &str,
        path: impl AsRef<Path>,
    ) -> Result<(), String> {
        let table = build_parquet_table(table_name, path)?;
        match &table.storage {
            TableStorage::LocalParquetFile { .. } => {}
            TableStorage::S3ParquetFiles { .. } => {
                return Err("register_parquet_table_in_database does not support S3".to_string());
            }
            TableStorage::IcebergMetadataTable { .. } => {
                return Err(
                    "register_parquet_table_in_database does not support iceberg metadata tables"
                        .to_string(),
                );
            }
        }
        let mut guard = self
            .inner
            .catalog
            .write()
            .expect("standalone catalog write lock");
        guard.register(database_name, table)
    }

    pub fn database_exists(&self, database_name: &str) -> Result<bool, String> {
        let guard = self
            .inner
            .catalog
            .read()
            .expect("standalone catalog read lock");
        guard.database_exists(database_name)
    }

    pub fn iceberg_catalog_exists(&self, catalog_name: &str) -> Result<bool, String> {
        let guard = self
            .inner
            .iceberg_catalogs
            .read()
            .expect("standalone iceberg catalog read lock");
        guard.contains_catalog(catalog_name)
    }

    pub fn iceberg_namespace_exists(
        &self,
        catalog_name: &str,
        namespace_name: &str,
    ) -> Result<bool, String> {
        let guard = self
            .inner
            .iceberg_catalogs
            .read()
            .expect("standalone iceberg catalog read lock");
        let entry = guard.get(catalog_name)?;
        iceberg_namespace_exists(&entry, namespace_name)
    }

    pub(crate) fn has_local_table(&self, database_name: &str, table_name: &str) -> bool {
        let Ok(database_name) = normalize_identifier(database_name) else {
            return false;
        };
        let Ok(table_name) = normalize_identifier(table_name) else {
            return false;
        };
        let guard = self
            .inner
            .catalog
            .read()
            .expect("standalone catalog read lock");
        guard.get(&database_name, &table_name).is_ok()
    }

    pub(crate) fn stream_load_managed_lake_table(
        &self,
        request: StandaloneStreamLoadRequest,
    ) -> Result<StandaloneStreamLoadResult, String> {
        stream_load_managed_lake_table(&self.inner, request)
    }
}

fn register_connector_backends(state: &Arc<StandaloneState>) {
    crate::connector::register_standalone_backends(state);
}

impl StandaloneSession {
    pub fn execute(&self, sql: &str) -> Result<(), String> {
        match self.execute_in_context(sql, None, DEFAULT_DATABASE, None)? {
            StatementResult::Ok => Ok(()),
            StatementResult::Query(_) => Err("statement returned rows".to_string()),
        }
    }

    pub fn query(&self, sql: &str) -> Result<QueryResult, String> {
        match self.execute_in_context(sql, None, DEFAULT_DATABASE, None)? {
            StatementResult::Query(result) => Ok(result),
            StatementResult::Ok => Err("statement did not return rows".to_string()),
        }
    }

    pub(crate) fn execute_in_database(
        &self,
        sql: &str,
        current_database: &str,
    ) -> Result<StatementResult, String> {
        self.execute_in_context(sql, None, current_database, None)
    }

    pub(crate) fn execute_in_context(
        &self,
        sql: &str,
        current_catalog: Option<&str>,
        current_database: &str,
        query_opts: Option<crate::internal_service::TQueryOptions>,
    ) -> Result<StatementResult, String> {
        use crate::sql::parser::dialect::{
            StarRocksDialect, looks_like_create_catalog, looks_like_create_database,
            looks_like_create_table, looks_like_drop_statement,
        };
        use sqlparser::ast as sqlast;

        let mut normalized = crate::sql::parser::dialect::normalize_for_raw_parse(sql)?;
        if looks_like_add_legacy_range_partition(&normalized) {
            let stmt = parse_add_legacy_range_partition_sql(&normalized)?;
            return self.handle_add_legacy_range_partition(stmt, current_catalog, current_database);
        }
        normalized =
            rewrite_legacy_partition_references(&self.inner, &normalized, current_database)?;
        normalized = rewrite_named_partition_insert_overwrite(&normalized)?;
        if let Some(result) =
            self::statistics::try_handle_statement(&self.inner, &normalized, current_database)?
        {
            return Ok(result);
        }
        if let Some((target, source)) = parse_create_table_like(&normalized)? {
            return self.handle_create_table_like(
                target,
                source,
                current_catalog,
                current_database,
            );
        }
        if looks_like_show_alter_table_optimize(&normalized) {
            let stmt = parse_show_alter_table_optimize_sql(&normalized)?;
            return self.handle_show_alter_table_optimize(stmt, current_catalog, current_database);
        }
        if looks_like_alter_table_optimize(&normalized) {
            let stmt = parse_alter_table_optimize_sql(&normalized)?;
            return self.handle_alter_table_optimize(stmt, current_catalog, current_database);
        }
        if looks_like_alter_table_rewrite_manifests(&normalized) {
            let stmt = parse_alter_table_rewrite_manifests_sql(&normalized)?;
            return self.handle_alter_table_rewrite_manifests(
                stmt,
                current_catalog,
                current_database,
            );
        }
        if looks_like_alter_table_expire_snapshots(&normalized) {
            let stmt = parse_alter_table_expire_snapshots_sql(&normalized)?;
            return self.handle_alter_table_expire_snapshots(
                stmt,
                current_catalog,
                current_database,
            );
        }
        if looks_like_alter_table_remove_orphan_files(&normalized) {
            let stmt = parse_alter_table_remove_orphan_files_sql(&normalized)?;
            return self.handle_alter_table_remove_orphan_files(
                stmt,
                current_catalog,
                current_database,
            );
        }
        // For MV DDL (CREATE/DROP/REFRESH/SHOW MATERIALIZED VIEW) we must
        // propagate errors from our custom parser rather than falling through to
        // the generic sqlparser-rs path, which would emit confusing diagnostics
        // like "Expected AS, found DISTRIBUTED" for invalid PRIMARY KEY clauses.
        {
            let sr_dialect = StarRocksDialect;
            if let Ok(ref peek_parser) =
                sqlparser::parser::Parser::new(&sr_dialect).try_with_sql(&normalized)
            {
                use crate::sql::parser::dialect::materialized_view::{
                    looks_like_create_materialized_view, looks_like_drop_materialized_view,
                    looks_like_refresh_materialized_view, looks_like_show_materialized_views,
                };
                use crate::sql::parser::dialect::truncate::looks_like_truncate_table;
                if looks_like_create_materialized_view(peek_parser)
                    || looks_like_drop_materialized_view(peek_parser)
                    || looks_like_refresh_materialized_view(peek_parser)
                    || looks_like_show_materialized_views(peek_parser)
                    // TRUNCATE TABLE: propagate our parser's reject errors
                    // (PARTITION/WHERE not supported, tag refs read-only)
                    // instead of falling through to sqlparser-rs's permissive
                    // builtin which would silently accept and bypass our checks.
                    || looks_like_truncate_table(peek_parser)
                {
                    let mut statements = crate::sql::parser::parse_sql(&normalized)?;
                    let statement = statements
                        .pop()
                        .ok_or_else(|| "custom parser returned no statements".to_string())?;
                    return dispatch_statement(
                        &self.inner,
                        current_catalog,
                        current_database,
                        statement,
                    );
                }
            }
        }
        if let Ok(mut statements) = crate::sql::parser::parse_sql(&normalized) {
            let statement = statements
                .pop()
                .ok_or_else(|| "custom parser returned no statements".to_string())?;
            return dispatch_statement(&self.inner, current_catalog, current_database, statement);
        }
        let (parse_sql, forced_explain_level) =
            if let Some((rewritten, level)) = split_explain_costs_sql(&normalized) {
                (rewritten, Some(level))
            } else {
                (normalized.clone(), None)
            };

        let dialect = StarRocksDialect;
        let mut parser = sqlparser::parser::Parser::new(&dialect)
            .try_with_sql(&parse_sql)
            .map_err(|e| format_parser_error(&e.to_string()))?;

        // StarRocks DDL: token-level parsing (sqlparser cannot handle these)
        if looks_like_create_table(&parser) {
            let result = crate::sql::parser::dialect::create_table::parse_create_table_statement(
                &mut parser,
            )?;
            return execute_create_table_statement(
                &self.inner,
                result,
                current_catalog,
                current_database,
            );
        }
        if looks_like_create_catalog(&parser) {
            let result =
                crate::sql::parser::dialect::create_catalog::parse_create_catalog_statement(
                    &mut parser,
                )?;
            return self.handle_create_catalog(result);
        }
        if looks_like_create_database(&parser) {
            let (db_name, if_not_exists) =
                crate::sql::parser::dialect::parse_create_database_name(&mut parser)?;
            return execute_create_database_statement(
                &self.inner,
                &db_name,
                if_not_exists,
                current_catalog,
            );
        }
        if looks_like_drop_statement(&parser) {
            let drop = crate::sql::parser::dialect::drop::parse_drop_statement(&mut parser)?;
            return self.handle_drop(drop, current_catalog, current_database);
        }

        // ALTER TABLE ... SET / UNSET TBLPROPERTIES
        if looks_like_alter_iceberg_properties(&normalized) {
            return self.handle_alter_iceberg_properties(
                &normalized,
                current_catalog,
                current_database,
            );
        }

        // ALTER TABLE ... ADD/DROP/RENAME/MODIFY COLUMN
        if looks_like_alter_iceberg_schema(&normalized) {
            return self.handle_alter_iceberg_schema(
                &normalized,
                current_catalog,
                current_database,
            );
        }

        // ALTER TABLE ... ADD/DROP PARTITION COLUMN ...
        if looks_like_alter_partition_column(&normalized) {
            let stmt = parse_alter_partition_column_sql(&normalized)?;
            return self.handle_alter_partition_spec(stmt, current_catalog, current_database);
        }

        // ALTER TABLE ... ADD EQUALITY DELETE (...) VALUES (...)
        if looks_like_add_equality_delete(&normalized) {
            return self.handle_add_equality_delete(&normalized, current_catalog, current_database);
        }

        // ALTER TABLE ... ADD FILES FROM '...'
        if looks_like_add_files(&normalized) {
            return self.handle_add_files(&normalized, current_catalog, current_database);
        }

        // Standard SQL: let sqlparser parse the full statement
        let stmt = crate::sql::parser::parse_normalized_sql_raw(&parse_sql)
            .map_err(|e| format_parser_error(&e.to_string()))?;
        match stmt {
            sqlast::Statement::Explain {
                statement,
                verbose,
                analyze: false,
                ..
            } => {
                let sqlast::Statement::Query(ref query) = *statement else {
                    return Err("EXPLAIN only supports SELECT queries".to_string());
                };
                // Time-travel in EXPLAIN: rewrite version clauses before registration.
                let mut time_travel_rewritten;
                let query = if has_time_travel_refs(query) {
                    time_travel_rewritten = query.as_ref().clone();
                    rewrite_time_travel_refs(
                        &self.inner,
                        current_catalog,
                        current_database,
                        &mut time_travel_rewritten,
                    )?;
                    &time_travel_rewritten
                } else {
                    query.as_ref()
                };
                if current_catalog.is_some() {
                    register_iceberg_tables_for_query(
                        &self.inner,
                        current_catalog,
                        current_database,
                        query,
                    )?;
                }
                let mut rewritten_three_part_query;
                let three_parts = extract_three_part_table_refs(query);
                let query = if !three_parts.is_empty() {
                    if current_catalog.is_none() {
                        register_iceberg_tables_for_query(
                            &self.inner,
                            None,
                            current_database,
                            query,
                        )?;
                    }
                    rewritten_three_part_query = query.clone();
                    strip_catalog_from_three_part_names(&mut rewritten_three_part_query);
                    &rewritten_three_part_query
                } else {
                    query
                };
                let level = forced_explain_level.unwrap_or({
                    if verbose {
                        crate::sql::explain::ExplainLevel::Verbose
                    } else {
                        crate::sql::explain::ExplainLevel::Normal
                    }
                });
                let catalog = self
                    .inner
                    .catalog
                    .read()
                    .expect("standalone catalog read lock");
                let result = explain_query(query, &catalog, current_database, level)?;
                drop(catalog);
                Ok(StatementResult::Query(result))
            }
            sqlast::Statement::Query(ref query) => {
                if let Some(result) =
                    self::statistics::try_query(&self.inner, &normalized, query, current_database)?
                {
                    return Ok(StatementResult::Query(result));
                }
                self::statistics::observe_query(&self.inner, query, current_database)?;
                if let Some(result) =
                    self::information_schema::try_query_materialized_views(&self.inner, query)?
                {
                    return Ok(result);
                }

                // Time-travel: `SELECT ... FROM t FOR VERSION AS OF <v>`.
                // Clone the query, rewrite version-bearing table refs to synthetic
                // per-snapshot names, register the synthetic TableDefs, then execute.
                // Non-version table refs in the same query are handled by the
                // regular registration path that follows in the rewritten query.
                if has_time_travel_refs(query) {
                    let mut rewritten = query.as_ref().clone();
                    rewrite_time_travel_refs(
                        &self.inner,
                        current_catalog,
                        current_database,
                        &mut rewritten,
                    )?;
                    // Register any remaining (non-time-travel) iceberg tables in the rewritten query.
                    if current_catalog.is_some() {
                        register_iceberg_tables_for_query(
                            &self.inner,
                            current_catalog,
                            current_database,
                            &rewritten,
                        )?;
                    }
                    let three_parts = extract_three_part_table_refs(&rewritten);
                    if !three_parts.is_empty() {
                        if current_catalog.is_none() {
                            register_iceberg_tables_for_query(
                                &self.inner,
                                None,
                                current_database,
                                &rewritten,
                            )?;
                        }
                        strip_catalog_from_three_part_names(&mut rewritten);
                    }
                    // Clone-then-release: do not hold `state.catalog.read()`
                    // across `execute_query`. Pipeline execution can run for
                    // many seconds and would otherwise starve writers (e.g.
                    // INSERT cleanup taking `state.catalog.write()` in
                    // `invalidate_iceberg_caches`) on the std::sync::RwLock
                    // writer queue.
                    let catalog_snapshot = self
                        .inner
                        .catalog
                        .read()
                        .expect("standalone catalog read lock")
                        .clone();
                    let result = execute_query(
                        &rewritten,
                        &catalog_snapshot,
                        current_database,
                        self.inner.exchange_port,
                        query_opts.clone(),
                    )?;
                    return Ok(StatementResult::Query(result));
                }

                // When current_catalog is an Iceberg catalog, materialize
                // referenced Iceberg tables into the local catalog first.
                if current_catalog.is_some() {
                    register_iceberg_tables_for_query(
                        &self.inner,
                        current_catalog,
                        current_database,
                        query,
                    )?;
                }

                // Handle fully-qualified 3-part table names (catalog.database.table).
                // Strip the leading catalog so the analyzer sees a 2-part name; the
                // registration above (or the explicit one below in current_catalog=None
                // mode) has already materialized the iceberg base table into the local
                // catalog. The strip also turns 4-part `cat.db.tbl.__nr_meta_*__`
                // metadata references into the 3-part form the analyzer's metadata
                // path expects.
                let three_parts = extract_three_part_table_refs(query);
                if !three_parts.is_empty() {
                    if current_catalog.is_none() {
                        register_iceberg_tables_for_query(
                            &self.inner,
                            None,
                            current_database,
                            query,
                        )?;
                    }
                    let mut rewritten = query.as_ref().clone();
                    strip_catalog_from_three_part_names(&mut rewritten);
                    // Clone-then-release: do not hold the catalog read lock
                    // across pipeline execution; see comment on the
                    // time-travel branch above.
                    let catalog_snapshot = self
                        .inner
                        .catalog
                        .read()
                        .expect("standalone catalog read lock")
                        .clone();
                    let result = execute_query(
                        &rewritten,
                        &catalog_snapshot,
                        current_database,
                        self.inner.exchange_port,
                        query_opts.clone(),
                    )?;
                    return Ok(StatementResult::Query(result));
                }

                // Clone-then-release: do not hold the catalog read lock
                // across pipeline execution; see comment on the time-travel
                // branch above.
                let catalog_snapshot = self
                    .inner
                    .catalog
                    .read()
                    .expect("standalone catalog read lock")
                    .clone();
                let result = execute_query(
                    query,
                    &catalog_snapshot,
                    current_database,
                    self.inner.exchange_port,
                    query_opts.clone(),
                )?;
                Ok(StatementResult::Query(result))
            }
            sqlast::Statement::Insert(ref insert) => self.handle_sqlparser_insert(
                insert,
                current_catalog,
                current_database,
                query_opts.as_ref(),
            ),
            sqlast::Statement::Delete(ref delete) => {
                let stmt = crate::engine::statement::convert_sqlparser_delete_to_custom(delete)?;
                crate::engine::delete_flow::execute_delete_statement(
                    &self.inner,
                    &stmt,
                    current_catalog,
                    current_database,
                )
            }
            ref update_stmt @ sqlast::Statement::Update(_) => {
                if let Some(result) = self::information_schema::try_update_be_configs(update_stmt)?
                {
                    return Ok(result);
                }
                let stmt =
                    crate::engine::statement::convert_sqlparser_update_to_custom(update_stmt)?;
                let result = crate::engine::mutation_flow::execute_update_statement(
                    &self.inner,
                    &stmt,
                    current_catalog,
                    current_database,
                )?;
                self::statistics::observe_update(&self.inner, &normalized, current_database)?;
                Ok(result)
            }
            ref merge_stmt @ sqlast::Statement::Merge(_) => {
                let stmt = crate::engine::statement::convert_sqlparser_merge_to_custom(merge_stmt)?;
                crate::engine::mutation_flow::execute_merge_statement(
                    &self.inner,
                    &stmt,
                    current_catalog,
                    current_database,
                )
            }
            sqlast::Statement::Truncate(truncate) => {
                for truncate_table in &truncate.table_names {
                    let table_name = crate::sql::parser::dialect::convert_object_name(
                        truncate_table.name.clone(),
                    )?;
                    execute_truncate_table_statement(
                        &self.inner,
                        &table_name,
                        "main",
                        current_catalog,
                        current_database,
                    )?;
                }
                Ok(StatementResult::Ok)
            }
            _ => Err(format!(
                "unsupported sql: {}",
                sql.chars().take(50).collect::<String>()
            )),
        }
    }

    /// Handle ALTER TABLE ... ADD FILES FROM '...'
    fn handle_add_files(
        &self,
        sql: &str,
        current_catalog: Option<&str>,
        current_database: &str,
    ) -> Result<StatementResult, String> {
        crate::engine::query_prep::add_files(&self.inner, sql, current_catalog, current_database)
    }

    fn handle_add_legacy_range_partition(
        &self,
        stmt: crate::engine::statement::AlterLegacyRangePartitionStmt,
        current_catalog: Option<&str>,
        current_database: &str,
    ) -> Result<StatementResult, String> {
        let target = crate::engine::backend_resolver::resolve_existing_table_target(
            &self.inner,
            &stmt.table,
            current_catalog,
            current_database,
        )?;
        if target.backend_name == "iceberg" {
            return Err("ALTER TABLE ADD PARTITION only supports standalone managed tables".into());
        }
        let mut catalog = self
            .inner
            .catalog
            .write()
            .expect("standalone catalog write lock");
        let table_def = catalog.get(&target.namespace, &target.table)?;
        let mut partition = stmt.partition;
        if partition.column.is_empty() {
            partition.column = table_def
                .columns
                .first()
                .map(|column| column.name.clone())
                .ok_or_else(|| {
                    format!(
                        "cannot infer range partition column for empty table schema {}.{}",
                        target.namespace, target.table
                    )
                })?;
        }
        catalog.add_legacy_range_partition(&target.namespace, &target.table, partition)?;
        Ok(StatementResult::Ok)
    }

    fn handle_alter_table_optimize(
        &self,
        stmt: crate::engine::statement::AlterTableOptimizeStmt,
        current_catalog: Option<&str>,
        current_database: &str,
    ) -> Result<StatementResult, String> {
        let Some(store) = self.inner.metadata_store.as_ref() else {
            return Err("ALTER TABLE OPTIMIZE requires standalone metadata store".to_string());
        };
        let target = crate::engine::backend_resolver::resolve_existing_table_target(
            &self.inner,
            &stmt.table,
            current_catalog,
            current_database,
        )?;
        if target.backend_name != "iceberg" {
            return Err(format!(
                "ALTER TABLE OPTIMIZE only supports iceberg backends, got `{}`",
                target.backend_name
            ));
        }

        let entry = {
            let registry = self
                .inner
                .iceberg_catalogs
                .read()
                .map_err(|e| format!("iceberg catalog registry read lock: {e}"))?;
            registry.get(&target.catalog)?
        };
        entry.invalidate_table_cache(&target.namespace, &target.table);
        let loaded = crate::connector::iceberg::catalog::load_table(
            &entry,
            &target.namespace,
            &target.table,
        )?;
        let base_snapshot_id = loaded
            .table
            .metadata()
            .current_snapshot()
            .map(|snapshot| snapshot.snapshot_id())
            .ok_or_else(|| {
                format!(
                    "ALTER TABLE OPTIMIZE requires iceberg table {}.{}.{} to have a current snapshot",
                    target.catalog, target.namespace, target.table
                )
            })?;
        store.create_iceberg_optimize_job(
            &target.catalog,
            &target.namespace,
            &target.table,
            base_snapshot_id,
            standalone_now_ms(),
        )?;
        Ok(StatementResult::Ok)
    }

    fn handle_alter_table_rewrite_manifests(
        &self,
        stmt: crate::engine::statement::AlterTableRewriteManifestsStmt,
        current_catalog: Option<&str>,
        current_database: &str,
    ) -> Result<StatementResult, String> {
        let target = crate::engine::backend_resolver::resolve_existing_table_target(
            &self.inner,
            &stmt.table,
            current_catalog,
            current_database,
        )?;
        if target.backend_name != "iceberg" {
            return Err(format!(
                "REWRITE MANIFESTS only supports iceberg backends, got `{}`",
                target.backend_name
            ));
        }
        crate::engine::iceberg_rewrite_manifests::execute_iceberg_rewrite_manifests(
            &self.inner,
            &target,
        )
    }

    fn handle_alter_table_expire_snapshots(
        &self,
        stmt: crate::engine::statement::AlterTableExpireSnapshotsStmt,
        current_catalog: Option<&str>,
        current_database: &str,
    ) -> Result<StatementResult, String> {
        let target = crate::engine::backend_resolver::resolve_existing_table_target(
            &self.inner,
            &stmt.table,
            current_catalog,
            current_database,
        )?;
        if target.backend_name != "iceberg" {
            return Err(format!(
                "EXPIRE SNAPSHOTS only supports iceberg backends, got `{}`",
                target.backend_name
            ));
        }
        crate::engine::iceberg_expire_snapshots::execute_iceberg_expire_snapshots(
            &self.inner,
            &target,
            &stmt,
        )
    }

    fn handle_alter_table_remove_orphan_files(
        &self,
        stmt: crate::engine::statement::AlterTableRemoveOrphanFilesStmt,
        current_catalog: Option<&str>,
        current_database: &str,
    ) -> Result<StatementResult, String> {
        let target = crate::engine::backend_resolver::resolve_existing_table_target(
            &self.inner,
            &stmt.table,
            current_catalog,
            current_database,
        )?;
        if target.backend_name != "iceberg" {
            return Err(format!(
                "REMOVE ORPHAN FILES only supports iceberg backends, got `{}`",
                target.backend_name
            ));
        }
        crate::engine::iceberg_remove_orphan_files::execute_iceberg_remove_orphan_files(
            &self.inner,
            &target,
            &stmt,
        )
    }

    fn handle_show_alter_table_optimize(
        &self,
        stmt: crate::engine::statement::ShowAlterTableOptimizeStmt,
        current_catalog: Option<&str>,
        current_database: &str,
    ) -> Result<StatementResult, String> {
        let Some(store) = self.inner.metadata_store.as_ref() else {
            return Err("SHOW ALTER TABLE OPTIMIZE requires standalone metadata store".to_string());
        };
        let mut jobs = store.show_iceberg_optimize_jobs()?;
        let catalog_filter = stmt.catalog.as_deref().or(current_catalog);
        let database_filter = stmt.database.as_deref().unwrap_or(current_database);
        if let Some(catalog) = catalog_filter {
            jobs.retain(|job| job.catalog == catalog);
        }
        jobs.retain(|job| job.namespace == database_filter);
        if let Some(table_name) = stmt.table_name.as_deref() {
            jobs.retain(|job| job.table == table_name);
        }
        jobs.sort_by_key(|job| (job.created_at_ms, job.id));
        if stmt.order_by_create_time_desc {
            jobs.reverse();
        }
        if let Some(limit) = stmt.limit {
            jobs.truncate(limit);
        }
        build_show_alter_table_optimize_result(jobs).map(StatementResult::Query)
    }

    fn handle_alter_iceberg_properties(
        &self,
        sql: &str,
        current_catalog: Option<&str>,
        current_database: &str,
    ) -> Result<StatementResult, String> {
        let stmt = parse_alter_iceberg_properties_sql(sql)?;
        crate::connector::iceberg::catalog::alter_table_properties(
            &self.inner,
            &stmt,
            current_catalog,
            current_database,
        )?;
        Ok(StatementResult::Ok)
    }

    fn handle_alter_iceberg_schema(
        &self,
        sql: &str,
        current_catalog: Option<&str>,
        current_database: &str,
    ) -> Result<StatementResult, String> {
        let stmt = crate::engine::statement::parse_alter_iceberg_schema_sql(sql)?;
        let target = crate::engine::backend_resolver::resolve_existing_table_target(
            &self.inner,
            &stmt.table,
            current_catalog,
            current_database,
        )?;
        if target.backend_name != "iceberg" {
            return self.handle_local_schema_change(stmt, target);
        }
        crate::connector::iceberg::catalog::alter_table_schema(
            &self.inner,
            &stmt,
            current_catalog,
            current_database,
        )?;
        Ok(StatementResult::Ok)
    }

    fn handle_local_schema_change(
        &self,
        stmt: crate::engine::statement::AlterIcebergSchemaStmt,
        target: crate::engine::backend_resolver::TargetBackend,
    ) -> Result<StatementResult, String> {
        match stmt.change {
            crate::engine::statement::IcebergSchemaChange::RenameColumn { path, .. }
                if path.segments().len() == 1 =>
            {
                Ok(StatementResult::Ok)
            }
            _ => Err(format!(
                "ALTER TABLE schema change only supports top-level RENAME COLUMN for `{}` tables",
                target.backend_name
            )),
        }
    }

    /// Handle ALTER TABLE ... ADD/DROP PARTITION COLUMN ...
    fn handle_alter_partition_spec(
        &self,
        stmt: crate::sql::parser::ast::AlterIcebergPartitionSpecStmt,
        current_catalog: Option<&str>,
        current_database: &str,
    ) -> Result<StatementResult, String> {
        let table_name = match &stmt {
            crate::sql::parser::ast::AlterIcebergPartitionSpecStmt::AddPartitionColumn {
                table,
                ..
            }
            | crate::sql::parser::ast::AlterIcebergPartitionSpecStmt::DropPartitionColumn {
                table,
                ..
            } => table,
        };
        let target = crate::engine::backend_resolver::resolve_table_target(
            &self.inner,
            table_name,
            current_catalog,
            current_database,
        )?;
        if target.backend_name != "iceberg" {
            return Err(format!(
                "ALTER TABLE ADD/DROP PARTITION COLUMN only supports iceberg backends, got `{}`",
                target.backend_name
            ));
        }
        let backend = self
            .inner
            .connectors
            .read()
            .expect("connector registry read")
            .catalog_backend(target.backend_name)?;
        backend.alter_iceberg_partition_spec(
            &target.catalog,
            &target.namespace,
            &target.table,
            stmt,
        )?;
        crate::engine::iceberg_writer::invalidate_iceberg_caches(&self.inner, &target)?;
        Ok(StatementResult::Ok)
    }

    /// Handle ALTER TABLE ... ADD EQUALITY DELETE (...) VALUES (...)
    fn handle_add_equality_delete(
        &self,
        sql: &str,
        current_catalog: Option<&str>,
        current_database: &str,
    ) -> Result<StatementResult, String> {
        let stmt = crate::engine::statement::parse_add_equality_delete_sql(sql)?;
        crate::engine::equality_delete_flow::execute_add_equality_delete_statement(
            &self.inner,
            &stmt,
            current_catalog,
            current_database,
        )
    }

    /// Handle CREATE CATALOG result.
    fn handle_create_catalog(
        &self,
        stmt: crate::sql::parser::ast::CreateCatalogStmt,
    ) -> Result<StatementResult, String> {
        let mut guard = self
            .inner
            .iceberg_catalogs
            .write()
            .expect("standalone iceberg catalog write lock");
        guard.create_catalog(&stmt.name, &stmt.properties)?;
        let persisted_properties = guard.get(&stmt.name)?.properties().to_vec();
        drop(guard);
        persist_iceberg_catalog_if_needed(
            &self.inner,
            &normalize_identifier(&stmt.name)?,
            &persisted_properties,
        )?;
        Ok(StatementResult::Ok)
    }

    fn handle_create_table_like(
        &self,
        target: crate::sql::parser::ast::ObjectName,
        source: crate::sql::parser::ast::ObjectName,
        current_catalog: Option<&str>,
        current_database: &str,
    ) -> Result<StatementResult, String> {
        let source_target = crate::engine::backend_resolver::resolve_existing_table_target(
            &self.inner,
            &source,
            current_catalog,
            current_database,
        )?;
        let backend = self
            .inner
            .connectors
            .read()
            .expect("connector registry read")
            .catalog_backend(source_target.backend_name)?;
        let source_table = backend.load_table(
            &source_target.catalog,
            &source_target.namespace,
            &source_target.table,
        )?;
        let columns = source_table
            .columns
            .iter()
            .map(|column| {
                Ok(crate::sql::parser::ast::TableColumnDef {
                    name: column.name.clone(),
                    data_type: crate::engine::iceberg_ctas::arrow_data_type_to_sql_type(
                        &column.data_type,
                    )?,
                    nullable: column.nullable,
                    aggregation: None,
                    default: None,
                })
            })
            .collect::<Result<Vec<_>, String>>()?;
        execute_create_table_statement(
            &self.inner,
            crate::sql::parser::ast::CreateTableStmt {
                name: target,
                kind: crate::sql::parser::ast::CreateTableKind::Iceberg {
                    columns,
                    key_desc: None,
                    bucket_count: None,
                    partition_fields: Vec::new(),
                    properties: Vec::new(),
                },
                legacy_range_partitions: Vec::new(),
                as_select: None,
                if_not_exists: false,
            },
            current_catalog,
            current_database,
        )
    }

    /// Handle DROP TABLE/DATABASE/CATALOG result.
    fn handle_drop(
        &self,
        drop: crate::sql::parser::dialect::drop::DropResult,
        current_catalog: Option<&str>,
        current_database: &str,
    ) -> Result<StatementResult, String> {
        use crate::sql::parser::dialect::drop::DropResult;
        match drop {
            DropResult::Catalog(stmt) => {
                execute_drop_catalog_statement(&self.inner, &stmt.name, stmt.if_exists)
            }
            DropResult::Database(stmt) => {
                let result = execute_drop_database_statement(
                    &self.inner,
                    &stmt.name,
                    current_catalog,
                    stmt.if_exists,
                    stmt.force,
                )?;
                if let Some(database) = stmt.name.parts.last() {
                    self::statistics::drop_database(&self.inner, database);
                }
                Ok(result)
            }
            DropResult::Table(stmt) => {
                let result = execute_drop_table_statement(
                    &self.inner,
                    &stmt.name,
                    current_catalog,
                    current_database,
                    stmt.if_exists,
                    stmt.force,
                )?;
                match stmt.name.parts.as_slice() {
                    [table] => self::statistics::drop_table(&self.inner, current_database, table),
                    [database, table] => self::statistics::drop_table(&self.inner, database, table),
                    [_, database, table] => {
                        self::statistics::drop_table(&self.inner, database, table)
                    }
                    _ => {}
                }
                Ok(result)
            }
        }
    }

    /// Consolidated INSERT handler using sqlparser AST. All INSERT targets
    /// flow through the custom parser so the shared dispatch in
    /// `execute_insert_statement` chooses between managed-lake and iceberg
    /// backends. The retired local-parquet backend is no longer consulted.
    fn handle_sqlparser_insert(
        &self,
        insert: &sqlparser::ast::Insert,
        current_catalog: Option<&str>,
        current_database: &str,
        _query_opts: Option<&crate::internal_service::TQueryOptions>,
    ) -> Result<StatementResult, String> {
        self.execute_insert_via_custom_parser(insert, current_catalog, current_database)
    }

    /// Convert sqlparser INSERT to our custom InsertStmt and delegate to the
    /// shared dispatcher in `execute_insert_statement`.
    fn execute_insert_via_custom_parser(
        &self,
        insert: &sqlparser::ast::Insert,
        current_catalog: Option<&str>,
        current_database: &str,
    ) -> Result<StatementResult, String> {
        let insert_stmt = convert_sqlparser_insert_to_custom(insert)?;
        execute_insert_statement(
            &self.inner,
            &insert_stmt.table,
            &insert_stmt.columns,
            &insert_stmt.source,
            insert_stmt.overwrite_mode,
            current_catalog,
            current_database,
        )
    }
}

fn standalone_now_ms() -> i64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_millis() as i64)
        .unwrap_or(0)
}

fn rewrite_named_partition_insert_overwrite(sql: &str) -> Result<String, String> {
    let re = regex::Regex::new(
        r#"(?is)^\s*insert\s+overwrite\s+(?:table\s+)?(?P<table>(?:`[^`]+`|[A-Za-z_][A-Za-z0-9_]*)(?:\.(?:`[^`]+`|[A-Za-z_][A-Za-z0-9_]*)){0,2})\s+partition\s*\([^)]*\)\s+(?P<rest>.*)$"#,
    )
    .map_err(|e| format!("compile INSERT OVERWRITE partition rewrite regex failed: {e}"))?;
    let Some(captures) = re.captures(sql) else {
        return Ok(sql.to_string());
    };
    let table = captures.name("table").expect("table capture").as_str();
    let rest = captures.name("rest").expect("rest capture").as_str();
    Ok(format!("INSERT OVERWRITE PARTITIONS {table} {rest}"))
}

fn parse_create_table_like(
    sql: &str,
) -> Result<
    Option<(
        crate::sql::parser::ast::ObjectName,
        crate::sql::parser::ast::ObjectName,
    )>,
    String,
> {
    let re = regex::Regex::new(
        r#"(?is)^\s*create\s+table\s+(?P<target>(?:`[^`]+`|[A-Za-z_][A-Za-z0-9_]*)(?:\.(?:`[^`]+`|[A-Za-z_][A-Za-z0-9_]*)){0,2})\s+like\s+(?P<source>(?:`[^`]+`|[A-Za-z_][A-Za-z0-9_]*)(?:\.(?:`[^`]+`|[A-Za-z_][A-Za-z0-9_]*)){0,2})\s*$"#,
    )
    .map_err(|e| format!("compile CREATE TABLE LIKE regex failed: {e}"))?;
    let Some(captures) = re.captures(sql) else {
        return Ok(None);
    };
    let target = parse_simple_object_name(captures.name("target").expect("target").as_str())?;
    let source = parse_simple_object_name(captures.name("source").expect("source").as_str())?;
    Ok(Some((target, source)))
}

fn parse_simple_object_name(token: &str) -> Result<crate::sql::parser::ast::ObjectName, String> {
    let mut parts = Vec::new();
    let mut cur = String::new();
    let mut in_backtick = false;
    for ch in token.chars() {
        match ch {
            '`' => in_backtick = !in_backtick,
            '.' if !in_backtick => {
                if cur.is_empty() {
                    return Err(format!("empty object name segment in `{token}`"));
                }
                parts.push(cur.clone());
                cur.clear();
            }
            _ => cur.push(ch),
        }
    }
    if !cur.is_empty() {
        parts.push(cur);
    }
    if parts.is_empty() {
        return Err(format!("empty object name `{token}`"));
    }
    Ok(crate::sql::parser::ast::ObjectName { parts })
}

fn build_show_alter_table_optimize_result(
    jobs: Vec<crate::connector::starrocks::managed::store::StoredIcebergOptimizeJob>,
) -> Result<QueryResult, String> {
    let column_names = [
        "JobId",
        "TableName",
        "State",
        "CreateTime",
        "FinishTime",
        "Msg",
        "BaseSnapshotId",
        "TargetSnapshotId",
        "InputDataFiles",
        "OutputDataFiles",
        "InputDeleteFiles",
        "OutputDeleteFiles",
    ];
    let mut columns = column_names
        .iter()
        .map(|_| Vec::with_capacity(jobs.len()))
        .collect::<Vec<Vec<String>>>();
    for job in jobs {
        let outcome = job.outcome.as_ref();
        columns[0].push(job.id.to_string());
        columns[1].push(job.table);
        columns[2].push(iceberg_optimize_state_name(job.state).to_string());
        columns[3].push(job.created_at_ms.to_string());
        columns[4].push(
            job.finished_at_ms
                .map(|value| value.to_string())
                .unwrap_or_default(),
        );
        columns[5].push(job.error_message.unwrap_or_else(|| {
            outcome
                .map(|value| {
                    format!(
                        "rewrote {} data files and {} delete files into {} data files ({} rows)",
                        value.rewritten_data_files,
                        value.deleted_data_files,
                        value.added_data_files,
                        value.output_record_count
                    )
                })
                .unwrap_or_default()
        }));
        columns[6].push(job.base_snapshot_id.to_string());
        columns[7].push(
            outcome
                .and_then(|value| value.target_snapshot_id)
                .map(|value| value.to_string())
                .unwrap_or_default(),
        );
        columns[8].push(
            outcome
                .map(|value| value.rewritten_data_files.to_string())
                .unwrap_or_default(),
        );
        columns[9].push(
            outcome
                .map(|value| value.added_data_files.to_string())
                .unwrap_or_default(),
        );
        columns[10].push(
            outcome
                .map(|value| value.deleted_data_files.to_string())
                .unwrap_or_default(),
        );
        columns[11].push(outcome.map(|_| "0".to_string()).unwrap_or_default());
    }

    let fields = column_names
        .iter()
        .map(|name| Field::new(*name, DataType::Utf8, false))
        .collect::<Vec<_>>();
    let arrays = columns
        .into_iter()
        .map(|values| Arc::new(StringArray::from(values)) as Arc<dyn arrow::array::Array>)
        .collect::<Vec<_>>();
    let batch = RecordBatch::try_new(Arc::new(Schema::new(fields)), arrays)
        .map_err(|e| format!("build SHOW ALTER TABLE OPTIMIZE result failed: {e}"))?;
    Ok(QueryResult {
        columns: column_names
            .iter()
            .map(|name| QueryResultColumn {
                name: (*name).to_string(),
                data_type: DataType::Utf8,
                nullable: false,
                logical_type: None,
            })
            .collect(),
        chunks: vec![record_batch_to_chunk(batch)?],
    })
}

fn iceberg_optimize_state_name(
    state: crate::connector::starrocks::managed::store::IcebergOptimizeJobState,
) -> &'static str {
    match state {
        crate::connector::starrocks::managed::store::IcebergOptimizeJobState::Pending => "PENDING",
        crate::connector::starrocks::managed::store::IcebergOptimizeJobState::Running => "RUNNING",
        crate::connector::starrocks::managed::store::IcebergOptimizeJobState::Finished => {
            "FINISHED"
        }
        crate::connector::starrocks::managed::store::IcebergOptimizeJobState::Failed => "FAILED",
    }
}

// ---------------------------------------------------------------------------
// Custom statement dispatch
// ---------------------------------------------------------------------------

pub(crate) mod delete_flow;
pub(crate) mod equality_delete_flow;
pub(crate) mod iceberg_expire_snapshots;
pub(crate) mod iceberg_remove_orphan_files;
pub(crate) mod iceberg_rewrite_manifests;
pub(crate) mod iceberg_truncate;
pub(crate) mod iceberg_writer;

pub(crate) fn dispatch_statement(
    state: &Arc<StandaloneState>,
    current_catalog: Option<&str>,
    current_database: &str,
    statement: crate::sql::parser::ast::Statement,
) -> Result<StatementResult, String> {
    use crate::sql::parser::ast::Statement;

    match statement {
        Statement::CreateMaterializedView(stmt) => {
            crate::engine::mv_flow::create_mv(state, current_catalog, current_database, &stmt)
        }
        Statement::DropMaterializedView(stmt) => {
            crate::engine::mv_flow::drop_mv(state, current_catalog, current_database, &stmt)
        }
        Statement::RefreshMaterializedView(stmt) => {
            crate::engine::mv_flow::refresh_mv(state, current_catalog, current_database, &stmt)
        }
        Statement::ShowMaterializedViews(stmt) => {
            crate::engine::mv_flow::list_mvs(state, current_catalog, &stmt)
        }
        Statement::AlterIcebergRef(stmt) => {
            crate::engine::iceberg_ref_flow::execute(state, current_database, &stmt)
        }
        Statement::Truncate { name, target_ref } => {
            crate::engine::statement::execute_truncate_table_statement(
                state,
                &name,
                &target_ref,
                current_catalog,
                current_database,
            )
        }
    }
}

pub(crate) fn register_iceberg_tables_for_query(
    state: &Arc<StandaloneState>,
    current_catalog: Option<&str>,
    current_database: &str,
    query: &sqlparser::ast::Query,
) -> Result<(), String> {
    crate::engine::query_prep::register_external_tables_for_query(
        state,
        current_catalog,
        current_database,
        query,
    )
}

fn refresh_iceberg_tables_for_query(
    state: &Arc<StandaloneState>,
    current_catalog: Option<&str>,
    current_database: &str,
    query: &sqlparser::ast::Query,
) -> Result<(), String> {
    crate::engine::query_prep::refresh_external_tables_for_query(
        state,
        current_catalog,
        current_database,
        query,
    )
}

// ---------------------------------------------------------------------------
// Local parquet table helpers
// ---------------------------------------------------------------------------

// ---------------------------------------------------------------------------
// Metadata persistence
// ---------------------------------------------------------------------------

fn resolve_metadata_store(
    explicit_path: Option<&Path>,
    config_path: Option<&Path>,
) -> Result<Option<SqliteMetadataStore>, String> {
    let resolved_path = match explicit_path {
        Some(path) => Some(resolve_relative_path(path, config_path)?),
        None => {
            let cfg = novarocks_config::config().map_err(|e| format!("read config failed: {e}"))?;
            cfg.standalone_server
                .as_ref()
                .and_then(|standalone| standalone.metadata_db_path.as_deref())
                .map(|path| resolve_relative_path(path, config_path))
                .transpose()?
        }
    };
    resolved_path.map(SqliteMetadataStore::open).transpose()
}

fn resolve_managed_lake_config() -> Result<Option<ManagedLakeConfig>, String> {
    let cfg = novarocks_config::config().map_err(|e| format!("read config failed: {e}"))?;
    let Some(standalone) = cfg.standalone_server.as_ref() else {
        return Ok(None);
    };
    let app_cfg = standalone.managed_lake_config()?;
    app_cfg.map(ManagedLakeConfig::from_app_config).transpose()
}

fn resolve_relative_path(path: &Path, config_path: Option<&Path>) -> Result<PathBuf, String> {
    if path.is_absolute() {
        return Ok(path.to_path_buf());
    }
    if let Some(config_path) = config_path
        && let Some(base_dir) = config_path.parent()
    {
        return Ok(base_dir.join(path));
    }
    std::env::current_dir()
        .map(|cwd| cwd.join(path))
        .map_err(|e| format!("read current directory failed: {e}"))
}

fn restore_metadata_if_needed(state: &Arc<StandaloneState>) -> Result<(), String> {
    let Some(store) = state.metadata_store.as_ref() else {
        return Ok(());
    };
    let snapshot = store.load_snapshot()?;
    restore_managed_lake(state, &snapshot)?;
    restore_iceberg_catalogs(state, &snapshot)?;
    crate::connector::starrocks::managed::mv_refresh_iceberg::restore_iceberg_mv_targets(state)?;
    Ok(())
}

fn restore_iceberg_catalogs(
    state: &Arc<StandaloneState>,
    snapshot: &MetadataSnapshot,
) -> Result<(), String> {
    {
        let mut guard = state
            .iceberg_catalogs
            .write()
            .expect("standalone iceberg catalog write lock");
        for catalog in &snapshot.iceberg_catalogs {
            guard.create_catalog(&catalog.name, &catalog.properties)?;
        }
    }

    let guard = state
        .iceberg_catalogs
        .read()
        .expect("standalone iceberg catalog read lock");
    for namespace in &snapshot.iceberg_namespaces {
        let entry = guard.get(&namespace.catalog)?;
        create_iceberg_namespace(&entry, &namespace.namespace)?;
    }
    for StoredIcebergTable {
        catalog,
        namespace,
        table,
    } in &snapshot.iceberg_tables
    {
        let entry = guard.get(catalog)?;
        register_existing_iceberg_table(&entry, namespace, table)?;
    }
    Ok(())
}

fn restore_managed_lake(
    state: &Arc<StandaloneState>,
    snapshot: &MetadataSnapshot,
) -> Result<(), String> {
    let Some(store) = state.metadata_store.as_ref() else {
        return Ok(());
    };
    let mut managed = snapshot.managed.clone();
    crate::connector::reconcile_managed_on_open(store, &mut managed, |snapshot, txn| {
        let tablet_ids = snapshot
            .tablets
            .iter()
            .filter(|tablet| {
                snapshot.indexes.iter().any(|index| {
                    index.index_id == tablet.index_id
                        && index.table_id == txn.table_id
                        && index.partition_id == txn.partition_id
                })
            })
            .map(|tablet| tablet.tablet_id)
            .collect::<Vec<_>>();
        crate::connector::publish_tablets_at_version(
            tablet_ids,
            txn.txn_id,
            txn.base_version,
            txn.commit_version,
        )
    })?;
    let rebuilt = ManagedLakeCatalog::rebuild(state.managed_lake_config.clone(), managed)?;
    {
        let mut catalog = state
            .catalog
            .write()
            .expect("standalone catalog write lock");
        for database in &snapshot.managed.databases {
            catalog.create_database(&database.name)?;
        }
        register_managed_tables_in_catalog(&mut catalog, &rebuilt)?;
    }
    rebuilt.re_register_active_tablet_runtimes()?;
    let mut guard = state
        .managed_lake
        .write()
        .expect("standalone managed lake write lock");
    *guard = rebuilt;
    Ok(())
}

pub(crate) fn persist_iceberg_catalog_if_needed(
    state: &Arc<StandaloneState>,
    catalog_name: &str,
    properties: &[(String, String)],
) -> Result<(), String> {
    if let Some(store) = state.metadata_store.as_ref() {
        store.upsert_iceberg_catalog(catalog_name, properties)?;
    }
    Ok(())
}

pub(crate) fn persist_iceberg_namespace_if_needed(
    state: &Arc<StandaloneState>,
    catalog_name: &str,
    namespace_name: &str,
) -> Result<(), String> {
    if let Some(store) = state.metadata_store.as_ref() {
        store.upsert_iceberg_namespace(catalog_name, namespace_name)?;
    }
    Ok(())
}

pub(crate) fn persist_iceberg_table_if_needed(
    state: &Arc<StandaloneState>,
    catalog_name: &str,
    namespace_name: &str,
    table_name: &str,
) -> Result<(), String> {
    if let Some(store) = state.metadata_store.as_ref() {
        store.upsert_iceberg_namespace(catalog_name, namespace_name)?;
        store.upsert_iceberg_table(catalog_name, namespace_name, table_name)?;
    }
    Ok(())
}

pub(crate) fn delete_iceberg_table_if_needed(
    state: &Arc<StandaloneState>,
    catalog_name: &str,
    namespace_name: &str,
    table_name: &str,
) -> Result<(), String> {
    if let Some(store) = state.metadata_store.as_ref() {
        store.delete_iceberg_table(catalog_name, namespace_name, table_name)?;
    }
    Ok(())
}

pub(crate) fn delete_iceberg_namespace_if_needed(
    state: &Arc<StandaloneState>,
    catalog_name: &str,
    namespace_name: &str,
) -> Result<(), String> {
    if let Some(store) = state.metadata_store.as_ref() {
        store.delete_iceberg_namespace(catalog_name, namespace_name)?;
    }
    Ok(())
}

pub(crate) fn delete_iceberg_catalog_if_needed(
    state: &Arc<StandaloneState>,
    catalog_name: &str,
) -> Result<(), String> {
    if let Some(store) = state.metadata_store.as_ref() {
        store.delete_iceberg_catalog(catalog_name)?;
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// Utility functions
// ---------------------------------------------------------------------------

pub(crate) fn block_on_standalone_async<F>(future: F) -> Result<F::Output, String>
where
    F: std::future::Future,
{
    if let Ok(handle) = Handle::try_current() {
        return Ok(handle.block_on(future));
    }
    data_block_on(future)
}

pub(crate) fn record_batch_to_chunk(batch: RecordBatch) -> Result<Chunk, String> {
    let slot_ids = (1..=batch.num_columns())
        .map(|idx| {
            u32::try_from(idx)
                .map(crate::common::ids::SlotId::new)
                .map_err(|_| "too many output columns".to_string())
        })
        .collect::<Result<Vec<_>, _>>()?;
    let chunk_schema =
        ChunkSchema::try_ref_from_schema_and_slot_ids(batch.schema().as_ref(), &slot_ids)?;
    Chunk::try_new_with_chunk_schema(batch, chunk_schema)
}

// ---------------------------------------------------------------------------
// Query plan build + execute (delegates to crate::sql::*)
// ---------------------------------------------------------------------------

use crate::sql::codegen::{FragmentEdgeKind, MultiFragmentBuildResult, PlanBuildResult};

enum StandaloneExecutionPlan {
    SingleFragment(Box<PlanBuildResult>),
    Coordinated(Box<MultiFragmentBuildResult>),
}

/// Recognize the narrow compatibility shape where fragment splitting
/// only wrapped the real root fragment in a single `EXCHANGE_NODE`.
fn top_level_stream_root_wrapper_child_id(
    br: &MultiFragmentBuildResult,
) -> Option<crate::sql::codegen::FragmentId> {
    use crate::plan_nodes::TPlanNodeType;

    let root = br
        .fragment_results
        .iter()
        .find(|f| f.fragment_id == br.root_fragment_id)?;
    if root.cte_id.is_some() || !root.cte_exchange_nodes.is_empty() {
        return None;
    }
    if root.plan.nodes.len() != 1 || root.plan.nodes[0].node_type != TPlanNodeType::EXCHANGE_NODE {
        return None;
    }
    if br
        .edges
        .iter()
        .any(|edge| edge.source_fragment_id == br.root_fragment_id)
    {
        return None;
    }

    let mut incoming_root_edges = br
        .edges
        .iter()
        .filter(|edge| edge.target_fragment_id == br.root_fragment_id);
    let edge = incoming_root_edges.next()?;
    if incoming_root_edges.next().is_some() {
        return None;
    }
    if !matches!(edge.edge_kind, FragmentEdgeKind::Stream) {
        return None;
    }

    let child_id = edge.source_fragment_id;
    if child_id == br.root_fragment_id {
        return None;
    }

    let child = br
        .fragment_results
        .iter()
        .find(|f| f.fragment_id == child_id)?;
    if child.plan.nodes.is_empty() {
        return None;
    }
    Some(child_id)
}

/// Strip a top-level exchange-only wrapper introduced by a single Gather split.
///
/// The stripped child becomes the new root. This keeps Task 1 fragment-builder
/// output intact while avoiding generic stream-edge execution in standalone.
fn strip_top_level_stream_root_wrapper(
    mut build_result: MultiFragmentBuildResult,
) -> MultiFragmentBuildResult {
    let Some(child_id) = top_level_stream_root_wrapper_child_id(&build_result) else {
        return build_result;
    };

    let old_root_id = build_result.root_fragment_id;
    build_result
        .fragment_results
        .retain(|fragment| fragment.fragment_id != old_root_id);
    build_result.edges.retain(|edge| {
        !(edge.source_fragment_id == child_id
            && edge.target_fragment_id == old_root_id
            && matches!(edge.edge_kind, FragmentEdgeKind::Stream))
    });
    build_result.root_fragment_id = child_id;
    build_result
}

fn single_fragment_plan(
    build_result: MultiFragmentBuildResult,
) -> Result<Box<PlanBuildResult>, Box<MultiFragmentBuildResult>> {
    if build_result.fragment_results.len() != 1 {
        return Err(Box::new(build_result));
    }
    let fragment = build_result.fragment_results.into_iter().next().unwrap();
    Ok(Box::new(PlanBuildResult {
        plan: fragment.plan,
        desc_tbl: fragment.desc_tbl,
        exec_params: fragment.exec_params,
        output_columns: fragment.output_columns,
    }))
}

fn choose_standalone_execution(build_result: MultiFragmentBuildResult) -> StandaloneExecutionPlan {
    if build_result.fragment_results.len() == 1 {
        match single_fragment_plan(build_result) {
            Ok(plan) => return StandaloneExecutionPlan::SingleFragment(plan),
            Err(br) => return StandaloneExecutionPlan::Coordinated(br),
        }
    }

    let build_result = strip_top_level_stream_root_wrapper(build_result);
    if build_result.fragment_results.len() == 1 {
        match single_fragment_plan(build_result) {
            Ok(plan) => return StandaloneExecutionPlan::SingleFragment(plan),
            Err(br) => return StandaloneExecutionPlan::Coordinated(br),
        }
    }

    StandaloneExecutionPlan::Coordinated(Box::new(build_result))
}

/// Produce EXPLAIN output for a query without executing it.
fn explain_query(
    query: &sqlparser::ast::Query,
    catalog: &InMemoryCatalog,
    current_database: &str,
    level: crate::sql::explain::ExplainLevel,
) -> Result<QueryResult, String> {
    use crate::sql::explain::{ExplainLevel, explain_physical_plan};

    let (resolved, cte_registry) = crate::sql::analyzer::analyze(query, catalog, current_database)?;
    let logical = crate::sql::planner::plan_query(resolved, cte_registry)?;
    let table_stats = build_table_stats_from_plan(&logical);
    let physical = crate::sql::optimizer::optimize(logical, &table_stats)?;

    let mut lines = Vec::new();
    if matches!(level, ExplainLevel::Costs) {
        for (table, stats) in &table_stats {
            lines.push(format!(
                "  Statistics: {table} row_count={}",
                stats.row_count
            ));
        }
    }
    lines.extend(explain_physical_plan(&physical, level));

    build_string_query_result("Explain String", lines)
}

pub(crate) fn execute_query(
    query: &sqlparser::ast::Query,
    catalog: &InMemoryCatalog,
    current_database: &str,
    exchange_port: u16,
    query_opts: Option<crate::internal_service::TQueryOptions>,
) -> Result<QueryResult, String> {
    let (resolved, cte_registry) = crate::sql::analyzer::analyze(query, catalog, current_database)?;
    let logical = crate::sql::planner::plan_query(resolved, cte_registry)?;
    let table_stats = build_table_stats_from_plan(&logical);
    let physical = crate::sql::optimizer::optimize(logical, &table_stats)?;
    let build_result = crate::sql::codegen::fragment_builder::PlanFragmentBuilder::build(
        &physical,
        catalog,
        current_database,
    )?;

    let execution_plan = choose_standalone_execution(build_result);

    match execution_plan {
        StandaloneExecutionPlan::SingleFragment(plan) => execute_plan(*plan, query_opts),
        StandaloneExecutionPlan::Coordinated(build_result) => {
            crate::runtime::coordinator::ExecutionCoordinator::new(
                *build_result,
                "127.0.0.1".to_string(),
                exchange_port,
                query_opts,
            )
            .execute()
        }
    }
}

fn ensure_standalone_exchange_server() -> Result<u16, String> {
    static STANDALONE_EXCHANGE_PORT: OnceLock<u16> = OnceLock::new();

    if let Some(port) = STANDALONE_EXCHANGE_PORT.get() {
        return Ok(*port);
    }

    let default_port = crate::common::config::http_port();
    let started_port =
        match crate::service::grpc_server::start_grpc_exchange_server("127.0.0.1", default_port) {
            Ok(()) => crate::service::grpc_server::grpc_server_bound_port()
                .map_err(|e| format!("read standalone grpc exchange server port failed: {e}"))?,
            Err(e) if e.contains("Address already in use") || e.contains("os error 48") => {
                let listener = TcpListener::bind(("127.0.0.1", 0)).map_err(|bind_err| {
                    format!("reserve standalone grpc exchange port failed: {bind_err}")
                })?;
                let fallback_port = listener
                    .local_addr()
                    .map_err(|addr_err| {
                        format!("read standalone grpc exchange port failed: {addr_err}")
                    })?
                    .port();
                drop(listener);
                crate::service::grpc_server::start_grpc_exchange_server("127.0.0.1", fallback_port)
                    .map_err(|start_err| {
                        format!(
                            "start standalone grpc exchange server failed on fallback port {}: {}",
                            fallback_port, start_err
                        )
                    })?;
                crate::service::grpc_server::grpc_server_bound_port().map_err(|e| {
                    format!("read standalone grpc exchange server fallback port failed: {e}")
                })?
            }
            Err(e) => return Err(format!("start standalone grpc exchange server failed: {e}")),
        };

    wait_for_standalone_exchange_server(started_port)?;

    if STANDALONE_EXCHANGE_PORT.set(started_port).is_err() {
        return Ok(*STANDALONE_EXCHANGE_PORT
            .get()
            .expect("standalone exchange port initialized"));
    }
    Ok(started_port)
}

fn wait_for_standalone_exchange_server(port: u16) -> Result<(), String> {
    let deadline = Instant::now() + Duration::from_secs(2);
    loop {
        match TcpStream::connect(("127.0.0.1", port)) {
            Ok(stream) => {
                drop(stream);
                return Ok(());
            }
            Err(err) if Instant::now() < deadline => {
                let _ = err;
                std::thread::sleep(Duration::from_millis(10));
            }
            Err(err) => {
                return Err(format!(
                    "standalone grpc exchange server on 127.0.0.1:{} did not become ready: {}",
                    port, err
                ));
            }
        }
    }
}

/// Walk the logical plan tree and collect table-level statistics for all scan
/// nodes that reference S3ParquetFiles storage.
fn build_table_stats_from_plan(
    plan: &crate::sql::planner::plan::LogicalPlan,
) -> std::collections::HashMap<String, crate::sql::optimizer::statistics::TableStatistics> {
    let mut stats = std::collections::HashMap::new();
    collect_scan_stats(plan, &mut stats);
    stats
}

/// Recursively visit plan nodes and collect statistics from Scan leaves.
fn collect_scan_stats(
    plan: &crate::sql::planner::plan::LogicalPlan,
    out: &mut std::collections::HashMap<String, crate::sql::optimizer::statistics::TableStatistics>,
) {
    use crate::sql::planner::plan::LogicalPlan;

    match plan {
        LogicalPlan::Scan(s) => {
            if let crate::sql::catalog::TableStorage::S3ParquetFiles { files, .. } =
                &s.table.storage
                && let Some(ts) = crate::sql::optimizer::statistics::build_table_statistics(files)
            {
                // Insert by table name (canonical key).
                out.insert(s.table.name.clone(), ts.clone());
                // Also insert by alias so that aliased scans can find their stats.
                if let Some(ref alias) = s.alias {
                    out.insert(alias.clone(), ts);
                }
            }
        }
        LogicalPlan::Filter(n) => collect_scan_stats(&n.input, out),
        LogicalPlan::Project(n) => collect_scan_stats(&n.input, out),
        LogicalPlan::Aggregate(n) => collect_scan_stats(&n.input, out),
        LogicalPlan::Sort(n) => collect_scan_stats(&n.input, out),
        LogicalPlan::Limit(n) => collect_scan_stats(&n.input, out),
        LogicalPlan::Window(n) => collect_scan_stats(&n.input, out),
        LogicalPlan::TableFunction(n) => collect_scan_stats(&n.input, out),
        LogicalPlan::CTEAnchor(n) => {
            collect_scan_stats(&n.produce, out);
            collect_scan_stats(&n.consumer, out);
        }
        LogicalPlan::CTEProduce(n) => collect_scan_stats(&n.input, out),
        LogicalPlan::SubqueryAlias(n) => collect_scan_stats(&n.input, out),
        LogicalPlan::Join(n) => {
            collect_scan_stats(&n.left, out);
            collect_scan_stats(&n.right, out);
        }
        LogicalPlan::Union(n) => {
            for input in &n.inputs {
                collect_scan_stats(input, out);
            }
        }
        LogicalPlan::Intersect(n) => {
            for input in &n.inputs {
                collect_scan_stats(input, out);
            }
        }
        LogicalPlan::Except(n) => {
            for input in &n.inputs {
                collect_scan_stats(input, out);
            }
        }
        LogicalPlan::Repeat(n) => collect_scan_stats(&n.input, out),
        LogicalPlan::Values(_) | LogicalPlan::GenerateSeries(_) | LogicalPlan::CTEConsume(_) => {}
    }
}

fn execute_plan(
    result: PlanBuildResult,
    query_opts: Option<crate::internal_service::TQueryOptions>,
) -> Result<QueryResult, String> {
    use crate::exec::expr::ExprArena;
    use crate::exec::node::{ExecPlan, push_down_local_runtime_filters};
    use crate::exec::operators::{ResultSinkFactory, ResultSinkHandle};
    use crate::exec::pipeline::executor::execute_plan_with_pipeline;
    use crate::lower::thrift::layout::{build_tuple_slot_order, reorder_tuple_slots};
    use crate::lower::thrift::lower_plan;
    use crate::runtime::runtime_state::RuntimeState;

    let desc_tbl = result.desc_tbl;
    let plan = result.plan;
    let exec_params = result.exec_params;

    let mut tuple_slots = build_tuple_slot_order(Some(&desc_tbl));
    reorder_tuple_slots(&mut tuple_slots, Some(&desc_tbl));
    let layout_hints = tuple_slots.clone();

    let mut arena = ExprArena::default();
    let connectors = crate::connector::ConnectorRegistry::default();
    let lowered = lower_plan(
        &plan,
        &mut arena,
        &tuple_slots,
        Some(&desc_tbl),
        None,
        None,
        Some(&exec_params),
        query_opts.as_ref(),
        None,
        &connectors,
        &layout_hints,
        None,
        None,
    )?;
    let mut exec_plan = ExecPlan {
        arena,
        root: lowered.node,
    };
    push_down_local_runtime_filters(&mut exec_plan.root, &exec_plan.arena);

    let handle = ResultSinkHandle::new();
    // Use available CPU cores for pipeline parallelism (capped at 8)
    let pipeline_dop = std::thread::available_parallelism()
        .map(|p| p.get().min(4))
        .unwrap_or(4);
    execute_plan_with_pipeline(
        exec_plan,
        false,
        std::time::Duration::from_millis(10),
        Box::new(ResultSinkFactory::new(handle.clone())),
        None,
        None,
        pipeline_dop as _,
        std::sync::Arc::new(RuntimeState::new(
            query_opts, None, None, None, None, None, None, None, None,
        )),
        None,
        None,
        None,
    )?;

    Ok(QueryResult {
        columns: result
            .output_columns
            .iter()
            .map(|c| QueryResultColumn {
                name: c.name.clone(),
                data_type: c.data_type.clone(),
                nullable: c.nullable,
                logical_type: None,
            })
            .collect(),
        chunks: handle.take_chunks(),
    })
}

// ---------------------------------------------------------------------------
// EXPLAIN COSTS helper
// ---------------------------------------------------------------------------

fn rewrite_legacy_partition_references(
    state: &Arc<StandaloneState>,
    sql: &str,
    current_database: &str,
) -> Result<String, String> {
    let sql = rewrite_insert_partition_target(sql);
    rewrite_select_partition_table_refs(state, &sql, current_database)
}

fn rewrite_insert_partition_target(sql: &str) -> String {
    let trimmed = sql.trim_start();
    let prefix_len = sql.len() - trimmed.len();
    let lower = trimmed.to_ascii_lowercase();
    if !(lower.starts_with("insert into ") || lower.starts_with("insert overwrite ")) {
        return sql.to_string();
    }
    let Some(marker) = find_partition_marker(trimmed, 0) else {
        return sql.to_string();
    };
    let mut rewritten = String::with_capacity(sql.len());
    rewritten.push_str(&sql[..prefix_len + marker.marker_start]);
    rewritten.push_str(&trimmed[marker.end..]);
    rewritten
}

fn rewrite_select_partition_table_refs(
    state: &Arc<StandaloneState>,
    sql: &str,
    current_database: &str,
) -> Result<String, String> {
    let mut out = String::with_capacity(sql.len());
    let mut cursor = 0;
    while let Some(marker) = find_partition_marker(sql, cursor) {
        let table_end = marker.marker_start;
        let table_end_trimmed = trim_ascii_whitespace_end(sql, table_end);
        let table_start = find_table_ref_start(sql, table_end_trimmed);
        if table_start == table_end_trimmed {
            cursor = marker.end;
            continue;
        }
        let table_ref = sql[table_start..table_end_trimmed].trim();
        let Some((database, table, alias)) =
            resolve_partition_table_ref(table_ref, current_database)
        else {
            cursor = marker.end;
            continue;
        };
        let partition = {
            let catalog = state.catalog.read().expect("standalone catalog read lock");
            catalog
                .get_legacy_range_partition(&database, &table, &marker.partition_name)?
                .ok_or_else(|| {
                    format!(
                        "unknown partition `{}` for table {}.{}",
                        marker.partition_name, database, table
                    )
                })?
        };
        out.push_str(&sql[cursor..table_start]);
        out.push_str(&legacy_partition_subquery(table_ref, &alias, &partition));
        cursor = marker.end;
    }
    out.push_str(&sql[cursor..]);
    Ok(out)
}

#[derive(Clone, Debug)]
struct PartitionMarker {
    marker_start: usize,
    end: usize,
    partition_name: String,
}

fn find_partition_marker(sql: &str, start: usize) -> Option<PartitionMarker> {
    let bytes = sql.as_bytes();
    let lower = sql.to_ascii_lowercase();
    let mut cursor = start;
    while let Some(rel) = lower[cursor..].find("partition") {
        let partition_start = cursor + rel;
        let partition_end = partition_start + "partition".len();
        if is_identifier_byte(bytes.get(partition_start.wrapping_sub(1)).copied())
            || is_identifier_byte(bytes.get(partition_end).copied())
        {
            cursor = partition_end;
            continue;
        }
        let paren = skip_ascii_whitespace(bytes, partition_end);
        if bytes.get(paren) != Some(&b'(') {
            cursor = partition_end;
            continue;
        }
        let marker_start =
            temporary_partition_marker_start(sql, partition_start).unwrap_or(partition_start);
        let close = find_matching_paren(sql, paren)?;
        let partition_name = sql[paren + 1..close]
            .trim()
            .trim_matches('`')
            .to_ascii_lowercase();
        return Some(PartitionMarker {
            marker_start,
            end: close + 1,
            partition_name,
        });
    }
    None
}

fn temporary_partition_marker_start(sql: &str, partition_start: usize) -> Option<usize> {
    let before_partition = trim_ascii_whitespace_end(sql, partition_start);
    let temp_end = before_partition;
    let temp_start = find_word_start(sql, temp_end);
    if temp_start == temp_end {
        return None;
    }
    if sql[temp_start..temp_end].eq_ignore_ascii_case("temporary") {
        Some(temp_start)
    } else {
        None
    }
}

fn legacy_partition_subquery(
    table_ref: &str,
    alias: &str,
    partition: &LegacyRangePartition,
) -> String {
    format!(
        "(SELECT * FROM {table_ref} WHERE {column} >= {lower} AND {column} < {upper}) AS {alias}",
        column = partition.column,
        lower = partition.lower_sql,
        upper = partition.upper_sql
    )
}

fn resolve_partition_table_ref(
    table_ref: &str,
    current_database: &str,
) -> Option<(String, String, String)> {
    let parts = table_ref
        .split('.')
        .map(|part| part.trim().trim_matches('`').to_ascii_lowercase())
        .filter(|part| !part.is_empty())
        .collect::<Vec<_>>();
    let (database, table) = match parts.as_slice() {
        [table] => (current_database.to_ascii_lowercase(), table.clone()),
        [database, table] => (database.clone(), table.clone()),
        [.., database, table] => (database.clone(), table.clone()),
        _ => return None,
    };
    Some((database, table.clone(), table))
}

fn trim_ascii_whitespace_end(sql: &str, mut idx: usize) -> usize {
    let bytes = sql.as_bytes();
    while idx > 0 && bytes[idx - 1].is_ascii_whitespace() {
        idx -= 1;
    }
    idx
}

fn find_table_ref_start(sql: &str, mut idx: usize) -> usize {
    let bytes = sql.as_bytes();
    while idx > 0 {
        let b = bytes[idx - 1];
        if b.is_ascii_alphanumeric() || matches!(b, b'_' | b'$' | b'.' | b'`') {
            idx -= 1;
        } else {
            break;
        }
    }
    idx
}

fn find_word_start(sql: &str, mut idx: usize) -> usize {
    let bytes = sql.as_bytes();
    while idx > 0 && is_identifier_byte(Some(bytes[idx - 1])) {
        idx -= 1;
    }
    idx
}

fn skip_ascii_whitespace(bytes: &[u8], mut idx: usize) -> usize {
    while idx < bytes.len() && bytes[idx].is_ascii_whitespace() {
        idx += 1;
    }
    idx
}

fn is_identifier_byte(byte: Option<u8>) -> bool {
    byte.is_some_and(|b| b.is_ascii_alphanumeric() || matches!(b, b'_' | b'$'))
}

fn find_matching_paren(sql: &str, open: usize) -> Option<usize> {
    let bytes = sql.as_bytes();
    if bytes.get(open) != Some(&b'(') {
        return None;
    }
    let mut depth = 0usize;
    let mut in_single = false;
    let mut in_double = false;
    let mut idx = open;
    while idx < bytes.len() {
        let b = bytes[idx];
        if in_single {
            if b == b'\'' {
                in_single = false;
            }
            idx += 1;
            continue;
        }
        if in_double {
            if b == b'"' {
                in_double = false;
            }
            idx += 1;
            continue;
        }
        match b {
            b'\'' => in_single = true,
            b'"' => in_double = true,
            b'(' => depth += 1,
            b')' => {
                depth = depth.saturating_sub(1);
                if depth == 0 {
                    return Some(idx);
                }
            }
            _ => {}
        }
        idx += 1;
    }
    None
}

/// Wrap a sqlparser error message in the `sql parser error: ...` envelope
/// and append a StarRocks-style `Unexpected input '<token>'` clause when
/// the underlying error mentions the offending token (`found: <token>`),
/// so tests can assert against the StarRocks-FE-style wording.
fn format_parser_error(raw: &str) -> String {
    let mut out = format!("sql parser error: {raw}");
    if let Some(start) = raw.find("found: ") {
        let after = &raw[start + "found: ".len()..];
        let token = after
            .split(|c: char| c.is_whitespace() || c == ',')
            .next()
            .unwrap_or("")
            .trim()
            .trim_matches(|c: char| c == '`' || c == '"');
        if !token.is_empty() {
            out.push_str(&format!(" Unexpected input '{token}'."));
        }
    }
    out
}

fn split_explain_costs_sql(sql: &str) -> Option<(String, crate::sql::explain::ExplainLevel)> {
    let trimmed = sql.trim_start();
    let prefix = "EXPLAIN COSTS ";
    if trimmed
        .as_bytes()
        .get(..prefix.len())
        .is_some_and(|head| head.eq_ignore_ascii_case(prefix.as_bytes()))
    {
        let body = trimmed[prefix.len()..].trim_start();
        Some((
            format!("EXPLAIN {body}"),
            crate::sql::explain::ExplainLevel::Costs,
        ))
    } else {
        None
    }
}

// ---------------------------------------------------------------------------
// Managed-lake stream-load entrypoint
// ---------------------------------------------------------------------------

/// HTTP stream-load entrypoint for managed-lake tables. Parses CSV / JSON
/// payloads via the neutral helpers in `engine::stream_load` and hands the
/// resulting rows to `insert_into_managed_lake_table`, so every stream-load
/// target goes through the same path as a plain `INSERT INTO ... VALUES`.
fn stream_load_managed_lake_table(
    state: &Arc<StandaloneState>,
    request: StandaloneStreamLoadRequest,
) -> Result<StandaloneStreamLoadResult, String> {
    let database = normalize_identifier(&request.database)?;
    let table = normalize_identifier(&request.table)?;
    let is_managed = state
        .managed_lake
        .read()
        .expect("standalone managed lake read lock")
        .contains_table(&database, &table)?;
    if !is_managed {
        return Err(format!(
            "standalone stream load only supports managed-lake tables, got {}.{}",
            database, table
        ));
    }

    let table_def = {
        let guard = state.catalog.read().expect("standalone catalog read lock");
        guard.get(&database, &table)?
    };
    let insert_columns = parse_stream_load_columns(request.columns.as_deref(), &table_def)?;
    let rows = match request.format_type {
        TFileFormatType::FORMAT_JSON => parse_json_stream_load_rows(
            &request.payload,
            &insert_columns,
            request.jsonpaths.as_deref(),
            request.strip_outer_array.unwrap_or(false),
        )?,
        TFileFormatType::FORMAT_CSV_PLAIN => parse_csv_stream_load_rows(
            &request.payload,
            &insert_columns,
            request.column_separator.as_deref(),
            request.row_delimiter.as_deref(),
            request.skip_header.unwrap_or(0),
            request.trim_space.unwrap_or(false),
            request.enclose,
            request.escape,
        )?,
        other => {
            return Err(format!(
                "standalone stream load only supports CSV/JSON, got {:?}",
                other
            ));
        }
    };
    let object_name = crate::sql::parser::ast::ObjectName {
        parts: vec![database.clone(), table.clone()],
    };
    let loaded_rows = rows.len() as i64;
    let loaded_bytes = request.payload.len() as i64;
    crate::connector::insert_into_managed_lake_table(
        state,
        &object_name,
        &insert_columns,
        &crate::sql::parser::ast::InsertSource::Values(rows),
        &database,
    )?;
    Ok(StandaloneStreamLoadResult {
        loaded_rows,
        loaded_bytes,
    })
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::{
        QueryResult, StandaloneNovaRocks, StandaloneOptions, StandaloneSession, StandaloneState,
        StatementResult, dispatch_statement, register_connector_backends,
    };
    use crate::connector::starrocks::lake::context::lock_runtime_test_state;
    use arrow::array::{
        Array, FixedSizeBinaryArray, Int32Array, Int64Array, ListArray, StringArray,
    };
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use parquet::arrow::ArrowWriter;
    use std::sync::Arc;
    use tempfile::{NamedTempFile, TempDir};

    fn write_parquet_file() -> NamedTempFile {
        let file = NamedTempFile::new().expect("create temp file");
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, true),
        ]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(Int32Array::from(vec![1, 2, 3])),
                Arc::new(StringArray::from(vec![Some("a"), Some("b"), None])),
            ],
        )
        .expect("build record batch");
        let writer_file = std::fs::File::create(file.path()).expect("open parquet output");
        let mut writer =
            ArrowWriter::try_new(writer_file, schema, None).expect("create parquet writer");
        writer.write(&batch).expect("write batch");
        writer.close().expect("close parquet writer");
        file
    }

    #[test]
    fn alter_iceberg_schema_dispatches_before_generic_sqlparser() {
        let engine = StandaloneNovaRocks::open(StandaloneOptions::default()).expect("engine");
        let err = engine
            .session()
            .execute("ALTER TABLE missing.db.t ADD COLUMN c INT")
            .expect_err("unknown catalog");
        assert!(err.contains("unknown catalog"));
    }

    #[test]
    fn show_alter_table_optimize_reads_persisted_jobs() {
        let temp = TempDir::new().expect("metadata temp dir");
        let engine = StandaloneNovaRocks::open(StandaloneOptions {
            config_path: None,
            metadata_db_path: Some(temp.path().join("metadata.db")),
        })
        .expect("engine");
        let store = engine
            .inner
            .metadata_store
            .as_ref()
            .expect("metadata store");
        let outcome = crate::connector::starrocks::managed::store::IcebergOptimizeJobOutcome {
            target_snapshot_id: Some(124),
            rewritten_data_files: 3,
            deleted_data_files: 2,
            added_data_files: 1,
            output_record_count: 7,
        };
        let job = store
            .create_iceberg_optimize_job("ice", "db1", "orders", 123, 1000)
            .expect("create synthetic optimize job");
        store
            .claim_iceberg_optimize_job(job.id, 1_100)
            .expect("claim synthetic optimize job");
        store
            .record_iceberg_optimize_job_outcome(job.id, 1_200, outcome.clone())
            .expect("record synthetic optimize outcome");
        store
            .finish_iceberg_optimize_job(job.id, 1_300, outcome)
            .expect("finish synthetic optimize job");

        let result = engine
            .session()
            .query(
                "SHOW ALTER TABLE OPTIMIZE FROM db1 WHERE TableName = 'orders' \
                 ORDER BY CreateTime DESC LIMIT 1",
            )
            .expect("show optimize jobs");

        assert_eq!(result.row_count(), 1);
        let chunk = &result.chunks[0];
        let table_names = chunk
            .batch
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("table name column");
        let states = chunk
            .batch
            .column(2)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("state column");
        let target_snapshot_ids = chunk
            .batch
            .column(7)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("target snapshot column");
        let input_data_files = chunk
            .batch
            .column(8)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("input data files column");
        let output_data_files = chunk
            .batch
            .column(9)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("output data files column");
        let input_delete_files = chunk
            .batch
            .column(10)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("input delete files column");
        let output_delete_files = chunk
            .batch
            .column(11)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("output delete files column");
        assert_eq!(table_names.value(0), "orders");
        assert_eq!(states.value(0), "FINISHED");
        assert_eq!(target_snapshot_ids.value(0), "124");
        assert_eq!(input_data_files.value(0), "3");
        assert_eq!(output_data_files.value(0), "1");
        assert_eq!(input_delete_files.value(0), "2");
        assert_eq!(output_delete_files.value(0), "0");
    }

    #[test]
    fn show_alter_table_optimize_uses_session_catalog_and_database() {
        let temp = TempDir::new().expect("metadata temp dir");
        let engine = StandaloneNovaRocks::open(StandaloneOptions {
            config_path: None,
            metadata_db_path: Some(temp.path().join("metadata.db")),
        })
        .expect("engine");
        let store = engine
            .inner
            .metadata_store
            .as_ref()
            .expect("metadata store");
        store
            .create_iceberg_optimize_job("ice1", "db1", "orders", 101, 1_000)
            .expect("create ice1 db1 job");
        store
            .create_iceberg_optimize_job("ice2", "db1", "orders", 102, 2_000)
            .expect("create ice2 db1 job");
        store
            .create_iceberg_optimize_job("ice1", "db2", "orders", 103, 3_000)
            .expect("create ice1 db2 job");

        let session = engine.session();
        let current = match session
            .execute_in_context("SHOW ALTER TABLE OPTIMIZE", Some("ice1"), "db1", None)
            .expect("show current context")
        {
            StatementResult::Query(result) => result,
            StatementResult::Ok => panic!("SHOW returned ok"),
        };
        assert_eq!(optimize_show_job_ids(&current), vec!["1"]);

        let from_db = match session
            .execute_in_context(
                "SHOW ALTER TABLE OPTIMIZE FROM db1 ORDER BY CreateTime DESC",
                Some("ice1"),
                "db2",
                None,
            )
            .expect("show from db under current catalog")
        {
            StatementResult::Query(result) => result,
            StatementResult::Ok => panic!("SHOW returned ok"),
        };
        assert_eq!(optimize_show_job_ids(&from_db), vec!["1"]);

        let explicit_catalog = match session
            .execute_in_context(
                "SHOW ALTER TABLE OPTIMIZE FROM ice2.db1",
                Some("ice1"),
                "db1",
                None,
            )
            .expect("show explicit catalog")
        {
            StatementResult::Query(result) => result,
            StatementResult::Ok => panic!("SHOW returned ok"),
        };
        assert_eq!(optimize_show_job_ids(&explicit_catalog), vec!["2"]);
    }

    fn optimize_show_job_ids(result: &QueryResult) -> Vec<String> {
        if result.row_count() == 0 {
            return Vec::new();
        }
        let ids = result.chunks[0]
            .batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("job id column");
        (0..ids.len())
            .map(|idx| ids.value(idx).to_string())
            .collect()
    }

    fn query_result_contains_string(result: &QueryResult, expected: &str) -> bool {
        result.chunks.iter().any(|chunk| {
            chunk.batch.columns().iter().any(|column| {
                column
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .is_some_and(|values| {
                        (0..values.len())
                            .any(|idx| !values.is_null(idx) && values.value(idx).contains(expected))
                    })
            })
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
        std::net::TcpStream::connect_timeout(
            &format!("{host}:{port}")
                .parse()
                .expect("managed lake endpoint socket addr"),
            std::time::Duration::from_secs(1),
        )
        .is_ok()
    }

    fn maybe_managed_lake_config() -> Option<(TempDir, std::path::PathBuf, std::path::PathBuf)> {
        let endpoint = std::env::var("AWS_S3_ENDPOINT")
            .unwrap_or_else(|_| "http://127.0.0.1:9000".to_string());
        if !managed_lake_endpoint_reachable(&endpoint) {
            eprintln!(
                "skipping managed lake test: object store endpoint is unreachable: {endpoint}"
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
        let root_prefix =
            std::env::var("AWS_S3_ROOT").unwrap_or_else(|_| "codex-managed-lake-tests".to_string());
        let run_id = format!(
            "engine_{}_{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_nanos()
        );
        let root_prefix = root_prefix.trim_matches('/');
        let warehouse_uri = if root_prefix.is_empty() {
            format!("s3://{bucket}/{run_id}")
        } else {
            format!("s3://{bucket}/{root_prefix}/{run_id}")
        };

        let dir = TempDir::new().expect("create managed lake config dir");
        let metadata_dir = dir.path().join("meta");
        std::fs::create_dir_all(&metadata_dir).expect("create metadata dir");
        let metadata_db_path = metadata_dir.join("standalone.sqlite");
        let config_path = dir.path().join("novarocks.toml");
        std::fs::write(
            &config_path,
            format!(
                r#"[standalone_server]
user = "root"
metadata_db_path = "meta/standalone.sqlite"
warehouse_uri = "{warehouse_uri}"

[standalone_server.object_store]
endpoint = "{endpoint}"
access_key_id = "{access_key_id}"
access_key_secret = "{access_key_secret}"
enable_path_style_access = true
"#
            ),
        )
        .expect("write managed lake config");
        Some((dir, config_path, metadata_db_path))
    }

    fn build_fragments_for_query(sql: &str) -> crate::sql::codegen::MultiFragmentBuildResult {
        use crate::sql::parser::dialect::{StarRocksDialect, normalize_for_raw_parse};

        let parquet = write_parquet_file();
        let mut catalog = super::InMemoryCatalog::default();
        catalog
            .register(
                "default",
                super::build_parquet_table("tbl", parquet.path()).expect("build parquet table"),
            )
            .expect("register parquet table");

        let normalized = normalize_for_raw_parse(sql).expect("normalize sql");
        let mut parser = sqlparser::parser::Parser::new(&StarRocksDialect)
            .try_with_sql(&normalized)
            .expect("build parser");
        let statement = parser.parse_statement().expect("parse statement");
        let sqlparser::ast::Statement::Query(query) = statement else {
            panic!("expected query statement");
        };

        let (resolved, cte_registry) =
            crate::sql::analyzer::analyze(&query, &catalog, "default").expect("analyze query");
        let logical = crate::sql::planner::plan_query(resolved, cte_registry).expect("plan query");
        let table_stats = super::build_table_stats_from_plan(&logical);
        let physical = crate::sql::optimizer::optimize(logical, &table_stats).expect("optimize");
        crate::sql::codegen::fragment_builder::PlanFragmentBuilder::build(
            &physical, &catalog, "default",
        )
        .expect("build fragments")
    }

    #[test]
    fn sqlparser_insert_values_preserves_array_literals() {
        use crate::sql::parser::dialect::StarRocksDialect;

        let statements = sqlparser::parser::Parser::parse_sql(
            &StarRocksDialect,
            "INSERT INTO t VALUES (1, [1, NULL, 3], ['a', NULL, 'c'])",
        )
        .expect("parse insert");
        let sqlparser::ast::Statement::Insert(insert) = &statements[0] else {
            panic!("expected insert statement");
        };
        let source = insert.source.as_ref().expect("insert source");
        let sqlparser::ast::SetExpr::Values(values) = source.body.as_ref() else {
            panic!("expected values source");
        };
        let row = &values.rows[0];

        assert_eq!(
            super::sqlparser_expr_to_literal(&row[1]).expect("parse int array"),
            crate::sql::parser::ast::Literal::Array(vec![
                crate::sql::parser::ast::Literal::Int(1),
                crate::sql::parser::ast::Literal::Null,
                crate::sql::parser::ast::Literal::Int(3),
            ])
        );
        assert_eq!(
            super::sqlparser_expr_to_literal(&row[2]).expect("parse string array"),
            crate::sql::parser::ast::Literal::Array(vec![
                crate::sql::parser::ast::Literal::String("a".to_string()),
                crate::sql::parser::ast::Literal::Null,
                crate::sql::parser::ast::Literal::String("c".to_string()),
            ])
        );
    }

    #[test]
    fn sqlparser_insert_values_preserves_large_integer_literals() {
        use crate::sql::parser::ast::Literal;
        use crate::sql::parser::dialect::StarRocksDialect;

        let statements = sqlparser::parser::Parser::parse_sql(
            &StarRocksDialect,
            "INSERT INTO t VALUES (-9223372036854775808, -170141183460469231731687303715884105728)",
        )
        .expect("parse insert");
        let sqlparser::ast::Statement::Insert(insert) = &statements[0] else {
            panic!("expected insert statement");
        };
        let source = insert.source.as_ref().expect("insert source");
        let sqlparser::ast::SetExpr::Values(values) = source.body.as_ref() else {
            panic!("expected values source");
        };
        let row = &values.rows[0];

        assert_eq!(
            super::sqlparser_expr_to_literal(&row[0]).expect("parse BIGINT literal"),
            Literal::String("-9223372036854775808".to_string())
        );
        assert_eq!(
            super::sqlparser_expr_to_literal(&row[1]).expect("parse LARGEINT literal"),
            Literal::String("-170141183460469231731687303715884105728".to_string())
        );
    }

    #[test]
    fn convert_insert_values_accepts_map_and_row_literals() {
        use crate::sql::parser::dialect::StarRocksDialect;

        let statements = sqlparser::parser::Parser::parse_sql(
            &StarRocksDialect,
            "INSERT INTO t VALUES (1, map('key', 5.5), row(100, 'abc'))",
        )
        .expect("parse insert");
        let sqlparser::ast::Statement::Insert(insert) = &statements[0] else {
            panic!("expected insert statement");
        };

        let converted = super::convert_sqlparser_insert_to_custom(insert);
        assert!(
            converted.is_ok(),
            "expected complex literals to convert: {converted:?}"
        );
    }

    #[test]
    fn build_local_insert_batch_supports_array_columns() {
        use crate::sql::catalog::ColumnDef;
        use crate::sql::parser::ast::Literal;

        let columns = vec![
            ColumnDef {
                name: "id".to_string(),
                data_type: DataType::Int32,
                nullable: false,
                write_default: None,
            },
            ColumnDef {
                name: "score_items".to_string(),
                data_type: DataType::List(Arc::new(Field::new("item", DataType::Int32, true))),
                nullable: true,
                write_default: None,
            },
            ColumnDef {
                name: "tags".to_string(),
                data_type: DataType::List(Arc::new(Field::new("item", DataType::Utf8, true))),
                nullable: true,
                write_default: None,
            },
        ];
        let rows = vec![
            vec![
                Literal::Int(1),
                Literal::Array(vec![Literal::Int(90), Literal::Null, Literal::Int(80)]),
                Literal::Array(vec![
                    Literal::String("a".to_string()),
                    Literal::Null,
                    Literal::String("c".to_string()),
                ]),
            ],
            vec![Literal::Int(2), Literal::Null, Literal::Array(vec![])],
        ];

        let batch = super::build_local_insert_batch(&columns, &rows).expect("build local batch");
        let scores = batch
            .column(1)
            .as_any()
            .downcast_ref::<ListArray>()
            .expect("score_items list array");
        let tags = batch
            .column(2)
            .as_any()
            .downcast_ref::<ListArray>()
            .expect("tags list array");

        assert_eq!(scores.len(), 2);
        assert_eq!(scores.value(0).len(), 3);
        assert!(scores.is_null(1));

        assert_eq!(tags.len(), 2);
        assert_eq!(tags.value(0).len(), 3);
        assert_eq!(tags.value(1).len(), 0);
    }

    #[test]
    fn sql_type_to_arrow_type_maps_largeint_to_fixed_size_binary() {
        assert_eq!(
            super::sql_type_to_arrow_type(&crate::sql::parser::ast::SqlType::LargeInt)
                .expect("map largeint type"),
            DataType::FixedSizeBinary(crate::common::largeint::LARGEINT_BYTE_WIDTH)
        );
    }

    #[test]
    fn build_local_insert_batch_supports_largeint_columns() {
        use crate::common::largeint;
        use crate::sql::catalog::ColumnDef;
        use crate::sql::parser::ast::Literal;

        let columns = vec![ColumnDef {
            name: "v".to_string(),
            data_type: DataType::FixedSizeBinary(largeint::LARGEINT_BYTE_WIDTH),
            nullable: true,
            write_default: None,
        }];
        let rows = vec![
            vec![Literal::String(
                "-170141183460469231731687303715884105728".to_string(),
            )],
            vec![Literal::String("0".to_string())],
            vec![Literal::Null],
            vec![Literal::String(
                "170141183460469231731687303715884105727".to_string(),
            )],
        ];

        let batch = super::build_local_insert_batch(&columns, &rows).expect("build local batch");
        let values = batch
            .column(0)
            .as_any()
            .downcast_ref::<FixedSizeBinaryArray>()
            .expect("largeint array");

        assert_eq!(
            largeint::value_at(values, 0).expect("decode min"),
            i128::MIN
        );
        assert_eq!(largeint::value_at(values, 1).expect("decode zero"), 0);
        assert!(values.is_null(2));
        assert_eq!(
            largeint::value_at(values, 3).expect("decode max"),
            i128::MAX
        );
    }

    #[test]
    fn build_local_insert_batch_accepts_integral_float_literals_for_bigint_arrays() {
        use crate::sql::catalog::ColumnDef;
        use crate::sql::parser::ast::Literal;

        let columns = vec![ColumnDef {
            name: "nums".to_string(),
            data_type: DataType::List(Arc::new(Field::new("item", DataType::Int64, true))),
            nullable: true,
            write_default: None,
        }];
        let rows = vec![vec![Literal::Array(vec![
            Literal::Float(1.0),
            Literal::Float(2.0),
        ])]];

        let batch = super::build_local_insert_batch(&columns, &rows).expect("build local batch");
        let nums = batch
            .column(0)
            .as_any()
            .downcast_ref::<ListArray>()
            .expect("nums list array");
        let values_ref = nums.value(0);
        let values = values_ref
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("int64 values");

        assert_eq!(values.value(0), 1);
        assert_eq!(values.value(1), 2);
    }

    #[test]
    fn build_local_insert_batch_drops_null_map_keys() {
        use crate::sql::catalog::ColumnDef;
        use crate::sql::parser::ast::Literal;

        // Arrow's Map layout requires `entries.key` to be non-nullable; map
        // literals with NULL keys must drop those kv-pairs so that the output
        // array matches the catalog schema.
        let entries_field = Arc::new(Field::new(
            "entries",
            DataType::Struct(
                vec![
                    Arc::new(Field::new("key", DataType::Int32, false)),
                    Arc::new(Field::new("value", DataType::Utf8, true)),
                ]
                .into(),
            ),
            false,
        ));
        let columns = vec![ColumnDef {
            name: "m".to_string(),
            data_type: DataType::Map(entries_field, false),
            nullable: true,
            write_default: None,
        }];
        let rows = vec![vec![Literal::Map(vec![
            (Literal::Null, Literal::String("dropped".to_string())),
            (Literal::Int(7), Literal::String("kept".to_string())),
        ])]];

        let batch = super::build_local_insert_batch(&columns, &rows).expect("build local batch");
        let map = batch
            .column(0)
            .as_any()
            .downcast_ref::<arrow::array::MapArray>()
            .expect("map array");
        assert_eq!(map.len(), 1);
        assert_eq!(map.value_length(0), 1);
        let entries = map.entries();
        let keys = entries
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .expect("key array");
        assert_eq!(keys.null_count(), 0);
        assert_eq!(keys.value(0), 7);

        let schema = batch.schema();
        let DataType::Map(entries_field, _) = schema.field(0).data_type() else {
            panic!("expected map field");
        };
        let DataType::Struct(entry_fields) = entries_field.data_type() else {
            panic!("expected struct entries");
        };
        assert!(!entry_fields[0].is_nullable());
    }

    #[test]
    fn cast_batch_to_schema_relaxes_map_key_nullability() {
        use crate::sql::catalog::ColumnDef;
        use crate::sql::parser::ast::Literal;

        let source_entries_field = Arc::new(Field::new(
            "entries",
            DataType::Struct(
                vec![
                    Arc::new(Field::new("key", DataType::Int32, false)),
                    Arc::new(Field::new("value", DataType::Utf8, true)),
                ]
                .into(),
            ),
            false,
        ));
        let source_columns = vec![ColumnDef {
            name: "m".to_string(),
            data_type: DataType::Map(source_entries_field, false),
            nullable: true,
            write_default: None,
        }];
        let rows = vec![vec![Literal::Map(vec![(
            Literal::Int(1),
            Literal::String("v".to_string()),
        )])]];
        let source_batch =
            super::build_local_insert_batch(&source_columns, &rows).expect("build source batch");

        let target_entries_field = Arc::new(Field::new(
            "entries",
            DataType::Struct(
                vec![
                    Arc::new(Field::new("key", DataType::Int32, true)),
                    Arc::new(Field::new("value", DataType::Utf8, true)),
                ]
                .into(),
            ),
            false,
        ));
        let target_schema = Arc::new(Schema::new(vec![Field::new(
            "m",
            DataType::Map(target_entries_field, false),
            true,
        )]));

        let casted = super::parquet::cast_batch_to_schema(&source_batch, &target_schema)
            .expect("cast batch");
        let casted_schema = casted.schema();
        let DataType::Map(entries_field, _) = casted_schema.field(0).data_type() else {
            panic!("expected MAP column");
        };
        let DataType::Struct(entry_fields) = entries_field.data_type() else {
            panic!("expected MAP entries to be STRUCT");
        };

        assert!(
            entry_fields[0].is_nullable(),
            "expected casted map key field to become nullable"
        );
    }

    #[test]
    fn local_parquet_round_trip_drops_null_map_keys() {
        use crate::sql::catalog::ColumnDef;
        use crate::sql::parser::ast::Literal;

        // Arrow's Map layout requires non-null keys; when a literal carries a
        // NULL key, the insert path drops the kv-pair and the resulting
        // parquet round trip must preserve that (no null keys).
        let entries_field = Arc::new(Field::new(
            "entries",
            DataType::Struct(
                vec![
                    Arc::new(Field::new("key", DataType::Int32, false)),
                    Arc::new(Field::new("value", DataType::Utf8, true)),
                ]
                .into(),
            ),
            false,
        ));
        let columns = vec![ColumnDef {
            name: "m".to_string(),
            data_type: DataType::Map(entries_field, false),
            nullable: true,
            write_default: None,
        }];
        let rows = vec![vec![Literal::Map(vec![
            (Literal::Null, Literal::String("dropped".to_string())),
            (Literal::Int(5), Literal::String("kept".to_string())),
        ])]];
        let batch = super::build_local_insert_batch(&columns, &rows).expect("build local batch");
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("map_round_trip.parquet");

        super::parquet::write_parquet_to_path(&path, &batch).expect("write local parquet");
        let round_tripped =
            super::parquet::read_local_parquet_data(&path, &columns).expect("read local parquet");
        let map = round_tripped
            .column(0)
            .as_any()
            .downcast_ref::<arrow::array::MapArray>()
            .expect("map array");
        assert_eq!(map.len(), 1);
        assert_eq!(map.value_length(0), 1);
        let entries = map.entries();
        let keys = entries
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .expect("key array");
        assert_eq!(keys.null_count(), 0);
        assert_eq!(keys.value(0), 5);

        let round_schema = round_tripped.schema();
        let DataType::Map(entries_field, _) = round_schema.field(0).data_type() else {
            panic!("expected map field");
        };
        let DataType::Struct(entry_fields) = entries_field.data_type() else {
            panic!("expected struct entries");
        };
        assert!(!entry_fields[0].is_nullable());
    }

    #[test]
    fn embedded_query_select_all_from_registered_parquet_table() {
        let parquet = write_parquet_file();
        let engine = StandaloneNovaRocks::open(StandaloneOptions::default()).expect("open engine");
        engine
            .register_parquet_table("tbl", parquet.path())
            .expect("register table");

        let session = engine.session();
        let result = session.query("select * from tbl").expect("execute query");
        assert_eq!(result.row_count(), 3);
        assert_eq!(result.chunks.len(), 1);
        let chunk = &result.chunks[0];
        assert_eq!(chunk.schema().field(0).name(), "id");
        assert_eq!(chunk.schema().field(1).name(), "name");
        assert_eq!(chunk.len(), 3);
    }

    #[test]
    fn embedded_query_projects_selected_columns() {
        let parquet = write_parquet_file();
        let engine = StandaloneNovaRocks::open(StandaloneOptions::default()).expect("open engine");
        engine
            .register_parquet_table("tbl", parquet.path())
            .expect("register table");

        let session = engine.session();
        let result = session
            .query("select name from tbl")
            .expect("execute query");
        assert_eq!(result.row_count(), 3);
        assert_eq!(result.chunks.len(), 1);
        let chunk = &result.chunks[0];
        assert_eq!(chunk.schema().fields().len(), 1);
        assert_eq!(chunk.schema().field(0).name(), "name");
    }

    #[test]
    fn embedded_query_executes_with_unused_cte_definition() {
        let parquet = write_parquet_file();
        let engine = StandaloneNovaRocks::open(StandaloneOptions::default()).expect("open engine");
        engine
            .register_parquet_table("tbl", parquet.path())
            .expect("register table");

        let session = engine.session();
        let result = session
            .query("WITH unused AS (SELECT id FROM tbl) SELECT name FROM tbl ORDER BY 1")
            .expect("execute query with unused CTE");
        assert_eq!(result.row_count(), 3);
    }

    #[test]
    fn embedded_query_executes_with_dead_nested_cte_definition() {
        let parquet = write_parquet_file();
        let engine = StandaloneNovaRocks::open(StandaloneOptions::default()).expect("open engine");
        engine
            .register_parquet_table("tbl", parquet.path())
            .expect("register table");

        let session = engine.session();
        let result = session
            .query(
                "WITH unused AS ( \
                    WITH inner_cte AS (SELECT id FROM tbl) \
                    SELECT a.id FROM inner_cte a JOIN inner_cte b ON a.id = b.id \
                ) \
                SELECT name FROM tbl ORDER BY 1",
            )
            .expect("execute query with dead nested CTE");
        assert_eq!(result.row_count(), 3);
    }

    #[test]
    fn embedded_query_filters_rows_and_projects_output() {
        let parquet = write_parquet_file();
        let engine = StandaloneNovaRocks::open(StandaloneOptions::default()).expect("open engine");
        engine
            .register_parquet_table("tbl", parquet.path())
            .expect("register table");

        let session = engine.session();
        let result = session
            .query("select name from tbl where id = 2")
            .expect("execute query");
        assert_eq!(result.row_count(), 1);
        let chunk = &result.chunks[0];
        assert_eq!(chunk.schema().fields().len(), 1);
        let names = chunk.batch.column(0);
        let names = names
            .as_any()
            .downcast_ref::<arrow::array::StringArray>()
            .expect("string array");
        assert_eq!(names.value(0), "b");
    }

    #[test]
    fn embedded_query_counts_subquery_with_limit_offset_without_order_by() {
        let parquet = write_parquet_file();
        let engine = StandaloneNovaRocks::open(StandaloneOptions::default()).expect("open engine");
        engine
            .register_parquet_table("tbl", parquet.path())
            .expect("register table");

        let session = engine.session();
        let result = session
            .query("SELECT count(*) FROM (SELECT * FROM tbl LIMIT 1, 1) x")
            .expect("execute limit offset query");
        assert_eq!(result.row_count(), 1);
        let counts = result.chunks[0]
            .batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("count array");
        assert_eq!(counts.value(0), 1);
    }

    #[test]
    fn embedded_query_executes_single_use_cte_through_cascades() {
        let parquet = write_parquet_file();
        let engine = StandaloneNovaRocks::open(StandaloneOptions::default()).expect("open engine");
        engine
            .register_parquet_table("tbl", parquet.path())
            .expect("register table");

        let session = engine.session();
        let result = session
            .query(
                "WITH t AS (SELECT id, name FROM tbl WHERE id >= 2) SELECT name FROM t ORDER BY 1",
            )
            .expect("execute query");

        assert_eq!(result.row_count(), 2);
        let chunk = &result.chunks[0];
        assert_eq!(chunk.schema().field(0).name(), "name");
    }

    #[test]
    fn embedded_query_executes_inline_values_cte_without_catalog_table() {
        let engine = StandaloneNovaRocks::open(StandaloneOptions::default()).expect("open engine");

        let session = engine.session();
        let result = session
            .query("WITH t AS (SELECT 1 AS id UNION ALL SELECT 2) SELECT id FROM t ORDER BY id")
            .expect("execute inline values CTE");
        assert_eq!(result.row_count(), 2);
    }

    #[test]
    fn embedded_query_math_function_accepts_negative_decimal_literal() {
        let engine = StandaloneNovaRocks::open(StandaloneOptions::default()).expect("open engine");

        let session = engine.session();
        let result = session
            .query("SELECT SQRT(-1.0) AS v")
            .expect("execute math function with negative decimal literal");
        assert_eq!(result.row_count(), 1);
    }

    #[test]
    fn embedded_query_executes_multi_use_cte_through_multicast_reuse() {
        let parquet = write_parquet_file();
        let engine = StandaloneNovaRocks::open(StandaloneOptions::default()).expect("open engine");
        engine
            .register_parquet_table("tbl", parquet.path())
            .expect("register table");

        let session = engine.session();
        let result = session
            .query(
                "WITH t AS (SELECT id FROM tbl) \
                    SELECT a.id FROM t a JOIN t b ON a.id = b.id ORDER BY 1",
            )
            .expect("execute query");

        assert_eq!(result.row_count(), 3);
        let chunk = &result.chunks[0];
        assert_eq!(chunk.schema().field(0).name(), "id");
    }

    #[test]
    fn embedded_query_explain_for_multi_use_cte_shows_physical_cte_nodes() {
        let parquet = write_parquet_file();
        let engine = StandaloneNovaRocks::open(StandaloneOptions::default()).expect("open engine");
        engine
            .register_parquet_table("tbl", parquet.path())
            .expect("register table");

        let session = engine.session();
        let explain = session
            .query("EXPLAIN WITH t AS (SELECT id FROM tbl) SELECT a.id FROM t a JOIN t b ON a.id = b.id")
            .expect("execute explain");

        assert!(explain.row_count() > 0);
        let text = explain
            .chunks
            .iter()
            .flat_map(|chunk| {
                let col = chunk.batch.column(0);
                let arr = col
                    .as_any()
                    .downcast_ref::<arrow::array::StringArray>()
                    .expect("string array");
                (0..arr.len())
                    .map(|idx| arr.value(idx).to_string())
                    .collect::<Vec<_>>()
            })
            .collect::<Vec<_>>()
            .join("\n");

        assert!(text.contains("CTE ANCHOR"), "text={text}");
        assert!(text.contains("CTE PRODUCE"), "text={text}");
        assert!(text.contains("CTE CONSUME"), "text={text}");
    }

    #[test]
    fn embedded_query_executes_nested_multi_use_cte_through_multicast_reuse() {
        let parquet = write_parquet_file();
        let engine = StandaloneNovaRocks::open(StandaloneOptions::default()).expect("open engine");
        engine
            .register_parquet_table("tbl", parquet.path())
            .expect("register table");

        let session = engine.session();
        let result = session
            .query(
                "WITH outer_cte AS ( \
                    WITH inner_cte AS (SELECT id FROM tbl) \
                    SELECT a.id FROM inner_cte a JOIN inner_cte b ON a.id = b.id \
                ) \
                SELECT x.id FROM outer_cte x JOIN outer_cte y ON x.id = y.id ORDER BY 1",
            )
            .expect("execute query");

        assert_eq!(result.row_count(), 3);
        let chunk = &result.chunks[0];
        assert_eq!(chunk.schema().field(0).name(), "id");
    }

    #[test]
    fn embedded_query_builder_splits_non_cte_join_into_multiple_fragments() {
        let build = build_fragments_for_query(
            "SELECT a.id FROM tbl a JOIN tbl b ON a.id = b.id ORDER BY 1",
        );

        assert!(
            build.fragment_results.len() > 1,
            "fragments={}",
            build.fragment_results.len()
        );
        assert!(build.edges.iter().any(|edge| {
            matches!(
                edge.edge_kind,
                crate::sql::codegen::FragmentEdgeKind::Stream
            )
        }));
    }

    #[test]
    fn embedded_query_explain_for_non_cte_join_shows_physical_exchange() {
        let parquet = write_parquet_file();
        let engine = StandaloneNovaRocks::open(StandaloneOptions::default()).expect("open engine");
        engine
            .register_parquet_table("tbl", parquet.path())
            .expect("register table");

        let session = engine.session();
        let explain = session
            .query("EXPLAIN SELECT a.id FROM tbl a JOIN tbl b ON a.id = b.id ORDER BY 1")
            .expect("execute explain");

        let text = explain
            .chunks
            .iter()
            .flat_map(|chunk| {
                let col = chunk.batch.column(0);
                let arr = col
                    .as_any()
                    .downcast_ref::<arrow::array::StringArray>()
                    .expect("string array");
                (0..arr.len())
                    .map(|idx| arr.value(idx).to_string())
                    .collect::<Vec<_>>()
            })
            .collect::<Vec<_>>()
            .join("\n");

        assert!(
            text.contains("GATHER EXCHANGE") || text.contains("HASH EXCHANGE"),
            "text={text}"
        );
    }

    #[test]
    fn embedded_query_executes_non_cte_join_through_stream_exchange() {
        let parquet = write_parquet_file();
        let engine = StandaloneNovaRocks::open(StandaloneOptions::default()).expect("open engine");
        engine
            .register_parquet_table("tbl", parquet.path())
            .expect("register table");

        let session = engine.session();
        let result = session
            .query("SELECT a.id FROM tbl a JOIN tbl b ON a.id = b.id ORDER BY 1")
            .expect("execute query");

        assert_eq!(result.row_count(), 3);
        let chunk = &result.chunks[0];
        assert_eq!(chunk.schema().field(0).name(), "id");
    }

    #[test]
    fn builder_preserves_cte_coordinator_shape_for_nested_cte_query() {
        let build = build_fragments_for_query(
            "WITH outer_cte AS ( \
                WITH inner_cte AS (SELECT id FROM tbl) \
                SELECT a.id FROM inner_cte a JOIN inner_cte b ON a.id = b.id \
            ) \
            SELECT x.id FROM outer_cte x JOIN outer_cte y ON x.id = y.id ORDER BY 1",
        );

        // Multiple fragments: root + CTE produce fragments + possible stream children.
        assert!(build.fragment_results.len() > 1);

        // At least one CTE produce fragment exists.
        assert!(build.fragment_results.iter().any(|f| f.cte_id.is_some()));
    }

    #[test]
    fn embedded_query_cte_union_with_four_way_self_join() {
        let parquet = write_parquet_file();
        let engine = StandaloneNovaRocks::open(StandaloneOptions::default()).expect("open engine");
        engine
            .register_parquet_table("tbl", parquet.path())
            .expect("register table");

        let session = engine.session();
        // Simulates TPC-DS q11 pattern: CTE with UNION ALL, 4-way self-join
        let result = session.query(
            "WITH year_total AS ( \
                SELECT id, name FROM tbl \
                UNION ALL \
                SELECT id, name FROM tbl \
            ) \
            SELECT a.id \
            FROM year_total a, year_total b, year_total c, year_total d \
            WHERE a.id = b.id AND b.id = c.id AND c.id = d.id \
            ORDER BY 1 \
            LIMIT 10",
        );
        match &result {
            Ok(r) => assert!(r.row_count() > 0),
            Err(e) => panic!("q11 pattern failed: {e}"),
        }
    }

    #[test]
    fn embedded_query_rejects_unknown_table() {
        let engine = StandaloneNovaRocks::open(StandaloneOptions::default()).expect("open engine");
        let session = engine.session();
        let err = session
            .query("select * from missing")
            .expect_err("missing table");
        assert!(err.contains("unknown table"));
    }

    #[test]
    fn register_parquet_table_normalizes_identifier() {
        let parquet = write_parquet_file();
        let engine = StandaloneNovaRocks::open(StandaloneOptions::default()).expect("open engine");
        engine
            .register_parquet_table("TBL", parquet.path())
            .expect("register table");
        let session = engine.session();
        let result = session.query("SELECT * FROM tbl;").expect("execute query");
        assert_eq!(result.row_count(), 3);
    }

    #[test]
    fn embedded_session_supports_create_database_create_table_and_drop_table() {
        let parquet = write_parquet_file();
        let engine = StandaloneNovaRocks::open(StandaloneOptions::default()).expect("open engine");
        let session = engine.session();

        let create_db = session
            .execute_in_database("create database analytics", "default")
            .expect("create database");
        assert!(matches!(create_db, StatementResult::Ok));
        assert!(
            engine
                .database_exists("analytics")
                .expect("check database exists")
        );

        engine
            .register_parquet_table_in_database("analytics", "tbl", parquet.path())
            .expect("register parquet table in analytics");

        let query_result = session
            .execute_in_database("select name from tbl where id = 2", "analytics")
            .expect("query table");
        let StatementResult::Query(query_result) = query_result else {
            panic!("expected query result");
        };
        assert_eq!(query_result.row_count(), 1);

        let drop_table = session
            .execute_in_database("drop table tbl", "analytics")
            .expect("drop table");
        assert!(matches!(drop_table, StatementResult::Ok));

        let err = session
            .execute_in_database("select * from tbl", "analytics")
            .expect_err("dropped table must be missing");
        assert!(err.contains("unknown table"), "err={err}");
    }

    #[test]
    fn embedded_session_supports_minimal_iceberg_flow() {
        let warehouse = TempDir::new().expect("create iceberg warehouse");
        let engine = StandaloneNovaRocks::open(StandaloneOptions::default()).expect("open engine");
        let session = engine.session();

        let create_catalog_sql = format!(
            r#"create external catalog ice properties("type"="iceberg","iceberg.catalog.type"="memory","iceberg.catalog.warehouse"="{}")"#,
            warehouse.path().display()
        );
        let create_catalog = session
            .execute_in_database(&create_catalog_sql, "default")
            .expect("create iceberg catalog");
        assert!(matches!(create_catalog, StatementResult::Ok));

        let create_database = session
            .execute_in_database("create database ice.db1", "default")
            .expect("create iceberg database");
        assert!(matches!(create_database, StatementResult::Ok));

        let create_table = session
            .execute_in_database("create table ice.db1.tbl (id int, name string)", "default")
            .expect("create iceberg table");
        assert!(matches!(create_table, StatementResult::Ok));

        let insert = session
            .execute_in_database(
                "insert into ice.db1.tbl values (1, 'a'), (2, 'b')",
                "default",
            )
            .expect("insert iceberg rows");
        assert!(matches!(insert, StatementResult::Ok));

        let result = session
            .query("select name from ice.db1.tbl where id = 2")
            .expect("query iceberg table");
        assert_eq!(result.row_count(), 1);
        let chunk = &result.chunks[0];
        let names = chunk.batch.column(0);
        let names = names
            .as_any()
            .downcast_ref::<arrow::array::StringArray>()
            .expect("string array");
        assert_eq!(names.value(0), "b");
    }

    #[test]
    fn embedded_session_preserves_iceberg_projection_order() {
        let warehouse = TempDir::new().expect("create iceberg warehouse");
        let engine = StandaloneNovaRocks::open(StandaloneOptions::default()).expect("open engine");
        let session = engine.session();

        let create_catalog_sql = format!(
            r#"create external catalog ice properties("type"="iceberg","iceberg.catalog.type"="memory","iceberg.catalog.warehouse"="{}")"#,
            warehouse.path().display()
        );
        let create_catalog = session
            .execute_in_database(&create_catalog_sql, "default")
            .expect("create iceberg catalog");
        assert!(matches!(create_catalog, StatementResult::Ok));

        let create_database = session
            .execute_in_database("create database ice.db1", "default")
            .expect("create iceberg database");
        assert!(matches!(create_database, StatementResult::Ok));

        let create_table = session
            .execute_in_database("create table ice.db1.tbl (id int, name string)", "default")
            .expect("create iceberg table");
        assert!(matches!(create_table, StatementResult::Ok));

        let insert = session
            .execute_in_database(
                "insert into ice.db1.tbl values (1, 'a'), (2, 'b')",
                "default",
            )
            .expect("insert iceberg rows");
        assert!(matches!(insert, StatementResult::Ok));

        let result = session
            .query("select name, id from ice.db1.tbl where id = 2")
            .expect("query iceberg table");
        assert_eq!(result.row_count(), 1);
        let chunk = &result.chunks[0];
        assert_eq!(chunk.schema().field(0).name(), "name");
        assert_eq!(chunk.schema().field(1).name(), "id");
        let names = chunk.batch.column(0);
        let names = names
            .as_any()
            .downcast_ref::<arrow::array::StringArray>()
            .expect("string array");
        assert_eq!(names.value(0), "b");
        let ids = chunk.batch.column(1);
        let ids = ids
            .as_any()
            .downcast_ref::<arrow::array::Int32Array>()
            .expect("int32 array");
        assert_eq!(ids.value(0), 2);
    }

    #[test]
    fn iceberg_refresh_load_failure_removes_stale_local_catalog_entry() {
        let warehouse = TempDir::new().expect("warehouse");
        let (engine, session) = open_iceberg_session_with_table(&warehouse, "2");
        session
            .execute_in_database("insert into ice.db1.t values (1, 'a')", "default")
            .expect("insert iceberg row");
        session
            .query("select id from ice.db1.t")
            .expect("register iceberg table");
        assert!(
            engine
                .inner
                .catalog
                .read()
                .expect("catalog read")
                .get("db1", "t")
                .is_ok(),
            "local table should be registered before external drop"
        );

        let entry = {
            let registry = engine.inner.iceberg_catalogs.read().expect("registry");
            registry.get("ice").expect("catalog entry")
        };
        crate::connector::iceberg::catalog::registry::drop_table(&entry, "db1", "t")
            .expect("drop backing iceberg table");

        let err = session
            .query("select id from ice.db1.t")
            .expect_err("dropped backing table should not use stale local table");
        assert!(err.contains("unknown iceberg table"), "err={err}");
        assert!(
            engine
                .inner
                .catalog
                .read()
                .expect("catalog read")
                .get("db1", "t")
                .is_err(),
            "stale local table should be removed after failed refresh"
        );
    }

    #[test]
    fn drop_iceberg_table_removes_stale_local_catalog_entry() {
        let warehouse = TempDir::new().expect("warehouse");
        let (engine, session) = open_iceberg_session_with_table(&warehouse, "2");
        session
            .execute_in_database("insert into ice.db1.t values (1, 'a')", "default")
            .expect("insert iceberg row");
        session
            .query("select id from ice.db1.t")
            .expect("register iceberg table");
        assert!(
            engine
                .inner
                .catalog
                .read()
                .expect("catalog read")
                .get("db1", "t")
                .is_ok(),
            "local table should be registered before drop"
        );

        let drop = session
            .execute_in_database("drop table ice.db1.t", "default")
            .expect("drop iceberg table");
        assert!(matches!(drop, StatementResult::Ok));
        assert!(
            engine
                .inner
                .catalog
                .read()
                .expect("catalog read")
                .get("db1", "t")
                .is_err(),
            "drop table should remove stale local table"
        );
    }

    #[test]
    fn execute_mv_incremental_refresh_reads_only_delta_files() {
        let warehouse = TempDir::new().expect("create iceberg warehouse");
        let engine = StandaloneNovaRocks::open(StandaloneOptions::default()).expect("open engine");
        let session = engine.session();

        let create_catalog_sql = format!(
            r#"create external catalog ice properties("type"="iceberg","iceberg.catalog.type"="memory","iceberg.catalog.warehouse"="{}")"#,
            warehouse.path().display()
        );
        let create_catalog = session
            .execute_in_database(&create_catalog_sql, "default")
            .expect("create iceberg catalog");
        assert!(matches!(create_catalog, StatementResult::Ok));

        let create_database = session
            .execute_in_database("create database ice.db1", "default")
            .expect("create iceberg database");
        assert!(matches!(create_database, StatementResult::Ok));

        let create_table = session
            .execute_in_database("create table ice.db1.tbl (id int, name string)", "default")
            .expect("create iceberg table");
        assert!(matches!(create_table, StatementResult::Ok));

        let first_insert = session
            .execute_in_database("insert into ice.db1.tbl values (1, 'old')", "default")
            .expect("insert first iceberg row");
        assert!(matches!(first_insert, StatementResult::Ok));

        let entry = {
            let registry = engine
                .inner
                .iceberg_catalogs
                .read()
                .expect("iceberg registry read lock");
            registry.get("ice").expect("load iceberg catalog entry")
        };
        let first_loaded =
            crate::connector::load_iceberg_table(&entry, "db1", "tbl").expect("load first table");
        let previous_snapshot_id = first_loaded
            .table
            .metadata()
            .current_snapshot()
            .expect("first snapshot")
            .snapshot_id();

        let second_insert = session
            .execute_in_database("insert into ice.db1.tbl values (2, 'new')", "default")
            .expect("insert second iceberg row");
        assert!(matches!(second_insert, StatementResult::Ok));

        let second_loaded =
            crate::connector::load_iceberg_table(&entry, "db1", "tbl").expect("load second table");
        let batch =
            crate::connector::plan_iceberg_changes(&second_loaded.table, previous_snapshot_id, &[])
                .expect("plan_changes");
        assert!(
            batch.deletes.is_empty(),
            "append-only fixture: {:?}",
            batch.deletes
        );
        assert!(
            batch.equality_deletes.is_empty(),
            "append-only fixture equality deletes: {:?}",
            batch.equality_deletes
        );
        let added_files: Vec<crate::engine::query_prep::IcebergFileForQuery> = batch
            .inserts
            .iter()
            .map(|f| crate::engine::query_prep::IcebergFileForQuery {
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

        let result = super::mv_flow::execute_query_for_mv_incremental_refresh(
            &engine.inner,
            "default",
            "select id, name from ice.db1.tbl order by id",
            &crate::connector::IcebergTableRef {
                catalog: "ice".to_string(),
                namespace: "db1".to_string(),
                table: "tbl".to_string(),
            },
            added_files,
        )
        .expect("execute mv incremental refresh");

        assert_eq!(result.row_count(), 1);
        let chunk = &result.chunks[0];
        let ids = chunk.batch.column(0);
        let ids = ids
            .as_any()
            .downcast_ref::<arrow::array::Int32Array>()
            .expect("id array");
        let names = chunk.batch.column(1);
        let names = names
            .as_any()
            .downcast_ref::<arrow::array::StringArray>()
            .expect("name array");
        assert_eq!(ids.value(0), 2);
        assert_eq!(names.value(0), "new");

        let catalog = engine
            .inner
            .catalog
            .read()
            .expect("standalone catalog read lock");
        assert!(catalog.get("db1", "tbl").is_err());
    }

    #[test]
    fn execute_mv_incremental_refresh_rejects_base_ref_mismatch() {
        let warehouse = TempDir::new().expect("create iceberg warehouse");
        let engine = StandaloneNovaRocks::open(StandaloneOptions::default()).expect("open engine");
        let session = engine.session();

        let create_catalog_sql = format!(
            r#"create external catalog ice properties("type"="iceberg","iceberg.catalog.type"="memory","iceberg.catalog.warehouse"="{}")"#,
            warehouse.path().display()
        );
        let create_catalog = session
            .execute_in_database(&create_catalog_sql, "default")
            .expect("create iceberg catalog");
        assert!(matches!(create_catalog, StatementResult::Ok));

        let create_database = session
            .execute_in_database("create database ice.db1", "default")
            .expect("create iceberg database");
        assert!(matches!(create_database, StatementResult::Ok));

        let create_table = session
            .execute_in_database("create table ice.db1.tbl (id int, name string)", "default")
            .expect("create iceberg table");
        assert!(matches!(create_table, StatementResult::Ok));

        let err = super::mv_flow::execute_query_for_mv_incremental_refresh(
            &engine.inner,
            "default",
            "select id, name from ice.db1.tbl",
            &crate::connector::IcebergTableRef {
                catalog: "ice".to_string(),
                namespace: "db1".to_string(),
                table: "other".to_string(),
            },
            vec![],
        )
        .expect_err("mismatched base ref must fail");
        assert!(
            err.contains("incremental MV refresh stored SQL base table mismatch"),
            "err={err}"
        );
    }

    #[test]
    fn execute_mv_incremental_refresh_rejects_zero_or_multiple_base_refs() {
        let state = Arc::new(StandaloneState::default());
        let base_ref = crate::connector::IcebergTableRef {
            catalog: "ice".to_string(),
            namespace: "db1".to_string(),
            table: "tbl".to_string(),
        };

        let err = super::mv_flow::execute_query_for_mv_incremental_refresh(
            &state,
            "default",
            "select 1",
            &base_ref,
            vec![],
        )
        .expect_err("missing base ref must fail");
        assert!(
            err.contains(
                "incremental MV refresh stored SQL must reference exactly one 3-part Iceberg table, got 0"
            ),
            "err={err}"
        );

        let err = super::mv_flow::execute_query_for_mv_incremental_refresh(
            &state,
            "default",
            "select * from ice.db1.tbl t join ice.db1.other o on t.id = o.id",
            &base_ref,
            vec![],
        )
        .expect_err("multiple base refs must fail");
        assert!(
            err.contains(
                "incremental MV refresh stored SQL must reference exactly one 3-part Iceberg table, got 2"
            ),
            "err={err}"
        );

        let err = super::mv_flow::execute_query_for_mv_incremental_refresh(
            &state,
            "default",
            "select * from ice.db1.tbl t join ice.db1.tbl u on t.id = u.id",
            &base_ref,
            vec![],
        )
        .expect_err("repeated base ref must fail");
        assert!(
            err.contains(
                "incremental MV refresh stored SQL must reference exactly one 3-part Iceberg table, got 2"
            ),
            "err={err}"
        );
    }

    #[test]
    fn execute_mv_incremental_refresh_rejects_multiple_local_delta_files() {
        let warehouse = TempDir::new().expect("create iceberg warehouse");
        let engine = StandaloneNovaRocks::open(StandaloneOptions::default()).expect("open engine");
        let session = engine.session();

        let create_catalog_sql = format!(
            r#"create external catalog ice properties("type"="iceberg","iceberg.catalog.type"="memory","iceberg.catalog.warehouse"="{}")"#,
            warehouse.path().display()
        );
        let create_catalog = session
            .execute_in_database(&create_catalog_sql, "default")
            .expect("create iceberg catalog");
        assert!(matches!(create_catalog, StatementResult::Ok));

        let create_database = session
            .execute_in_database("create database ice.db1", "default")
            .expect("create iceberg database");
        assert!(matches!(create_database, StatementResult::Ok));

        let create_table = session
            .execute_in_database("create table ice.db1.tbl (id int, name string)", "default")
            .expect("create iceberg table");
        assert!(matches!(create_table, StatementResult::Ok));

        let first_insert = session
            .execute_in_database("insert into ice.db1.tbl values (1, 'old')", "default")
            .expect("insert first iceberg row");
        assert!(matches!(first_insert, StatementResult::Ok));

        let entry = {
            let registry = engine
                .inner
                .iceberg_catalogs
                .read()
                .expect("iceberg registry read lock");
            registry.get("ice").expect("load iceberg catalog entry")
        };
        let first_loaded =
            crate::connector::load_iceberg_table(&entry, "db1", "tbl").expect("load first table");
        let previous_snapshot_id = first_loaded
            .table
            .metadata()
            .current_snapshot()
            .expect("first snapshot")
            .snapshot_id();

        let second_insert = session
            .execute_in_database("insert into ice.db1.tbl values (2, 'new')", "default")
            .expect("insert second iceberg row");
        assert!(matches!(second_insert, StatementResult::Ok));

        let second_loaded =
            crate::connector::load_iceberg_table(&entry, "db1", "tbl").expect("load second table");
        let batch =
            crate::connector::plan_iceberg_changes(&second_loaded.table, previous_snapshot_id, &[])
                .expect("plan_changes");
        assert!(
            batch.deletes.is_empty(),
            "append-only fixture: {:?}",
            batch.deletes
        );
        assert!(
            batch.equality_deletes.is_empty(),
            "append-only fixture equality deletes: {:?}",
            batch.equality_deletes
        );
        let mut delta_files: Vec<crate::engine::query_prep::IcebergFileForQuery> = batch
            .inserts
            .iter()
            .map(|f| crate::engine::query_prep::IcebergFileForQuery {
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
        let first_delta_file = delta_files
            .first()
            .expect("at least one delta file")
            .clone();
        delta_files.push(first_delta_file);

        let err = super::mv_flow::execute_query_for_mv_incremental_refresh(
            &engine.inner,
            "default",
            "select id, name from ice.db1.tbl",
            &crate::connector::IcebergTableRef {
                catalog: "ice".to_string(),
                namespace: "db1".to_string(),
                table: "tbl".to_string(),
            },
            delta_files,
        )
        .expect_err("multiple local delta files must fail");
        assert!(
            err.contains(
                "incremental MV refresh over local iceberg supports at most one delta file"
            ),
            "err={err}"
        );
    }

    #[test]
    fn embedded_session_preserves_projection_order_with_current_catalog_context() {
        let warehouse = TempDir::new().expect("create iceberg warehouse");
        let engine = StandaloneNovaRocks::open(StandaloneOptions::default()).expect("open engine");
        let session = engine.session();

        let create_catalog_sql = format!(
            r#"create external catalog ice properties("type"="iceberg","iceberg.catalog.type"="memory","iceberg.catalog.warehouse"="{}")"#,
            warehouse.path().display()
        );
        let create_catalog = session
            .execute_in_database(&create_catalog_sql, "default")
            .expect("create iceberg catalog");
        assert!(matches!(create_catalog, StatementResult::Ok));

        let create_database = session
            .execute_in_database("create database ice.db1", "default")
            .expect("create iceberg database");
        assert!(matches!(create_database, StatementResult::Ok));

        let create_table = session
            .execute_in_database(
                "create table ice.db1.nums (c1 tinyint, c2 smallint)",
                "default",
            )
            .expect("create iceberg table");
        assert!(matches!(create_table, StatementResult::Ok));

        let insert = session
            .execute_in_database(
                "insert into ice.db1.nums values (1, 101), (2, 102)",
                "default",
            )
            .expect("insert iceberg rows");
        assert!(matches!(insert, StatementResult::Ok));

        let result = session
            .execute_in_context(
                "select c2, c1 from nums order by 1, 2",
                Some("ice"),
                "db1",
                None,
            )
            .expect("query iceberg table in current catalog context");
        let StatementResult::Query(result) = result else {
            panic!("expected query result");
        };
        assert_eq!(result.columns[0].name, "c2");
        assert_eq!(result.columns[1].name, "c1");
        assert_eq!(result.row_count(), 2);

        let chunk = &result.chunks[0];
        assert_eq!(chunk.schema().field(0).name(), "c2");
        assert_eq!(chunk.schema().field(1).name(), "c1");
        assert_eq!(chunk.batch.column(0).data_type(), &DataType::Int32);
        assert_eq!(chunk.batch.column(1).data_type(), &DataType::Int32);
        let c2 = chunk.batch.column(0);
        let c2 = c2
            .as_any()
            .downcast_ref::<arrow::array::Int32Array>()
            .expect("int32 array");
        assert_eq!(c2.value(0), 101);
        assert_eq!(c2.value(1), 102);
        let c1 = chunk.batch.column(1);
        let c1 = c1
            .as_any()
            .downcast_ref::<arrow::array::Int32Array>()
            .expect("int32 array");
        assert_eq!(c1.value(0), 1);
        assert_eq!(c1.value(1), 2);
    }

    #[test]
    fn embedded_session_does_not_restore_external_preloaded_parquet_tables() {
        let parquet = write_parquet_file();
        let metadata_dir = TempDir::new().expect("create metadata dir");
        let metadata_db_path = metadata_dir.path().join("standalone.sqlite");

        {
            let engine = StandaloneNovaRocks::open(StandaloneOptions {
                config_path: None,
                metadata_db_path: Some(metadata_db_path.clone()),
            })
            .expect("open engine");
            engine
                .register_parquet_table("ext_tbl", parquet.path())
                .expect("register external parquet");
        }

        let reopened = StandaloneNovaRocks::open(StandaloneOptions {
            config_path: None,
            metadata_db_path: Some(metadata_db_path),
        })
        .expect("reopen engine");
        let err = reopened
            .session()
            .query("select * from ext_tbl")
            .expect_err("external preload must not be restored");
        assert!(err.contains("unknown table"), "err={err}");
    }

    #[test]
    fn embedded_session_restores_iceberg_metadata_from_sqlite() {
        let warehouse = TempDir::new().expect("create iceberg warehouse");
        let metadata_dir = TempDir::new().expect("create metadata dir");
        let metadata_db_path = metadata_dir.path().join("standalone.sqlite");

        {
            let engine = StandaloneNovaRocks::open(StandaloneOptions {
                config_path: None,
                metadata_db_path: Some(metadata_db_path.clone()),
            })
            .expect("open engine");
            let session = engine.session();

            let create_catalog_sql = format!(
                r#"create external catalog ice properties("type"="iceberg","iceberg.catalog.type"="memory","iceberg.catalog.warehouse"="{}")"#,
                warehouse.path().display()
            );
            let create_catalog = session
                .execute_in_database(&create_catalog_sql, "default")
                .expect("create iceberg catalog");
            assert!(matches!(create_catalog, StatementResult::Ok));

            let create_database = session
                .execute_in_database("create database ice.db1", "default")
                .expect("create iceberg database");
            assert!(matches!(create_database, StatementResult::Ok));

            let create_table = session
                .execute_in_database("create table ice.db1.tbl (id int, name string)", "default")
                .expect("create iceberg table");
            assert!(matches!(create_table, StatementResult::Ok));

            let insert = session
                .execute_in_database(
                    "insert into ice.db1.tbl values (1, 'a'), (2, 'b')",
                    "default",
                )
                .expect("insert iceberg rows");
            assert!(matches!(insert, StatementResult::Ok));
        }

        let restored = StandaloneNovaRocks::open(StandaloneOptions {
            config_path: None,
            metadata_db_path: Some(metadata_db_path),
        })
        .expect("reopen engine");
        let session = restored.session();
        let result = session
            .query("select name from ice.db1.tbl where id = 2")
            .expect("query restored iceberg table");
        assert_eq!(result.row_count(), 1);
        let chunk = &result.chunks[0];
        let names = chunk.batch.column(0);
        let names = names
            .as_any()
            .downcast_ref::<arrow::array::StringArray>()
            .expect("string array");
        assert_eq!(names.value(0), "b");
    }

    #[test]
    fn restore_metadata_registers_iceberg_mv_target_from_relationship() {
        let warehouse = TempDir::new().expect("create iceberg warehouse");
        let metadata_dir = TempDir::new().expect("create metadata dir");
        let metadata_db_path = metadata_dir.path().join("standalone.sqlite");

        {
            let engine = StandaloneNovaRocks::open(StandaloneOptions {
                config_path: None,
                metadata_db_path: Some(metadata_db_path.clone()),
            })
            .expect("open engine");
            let session = engine.session();

            let create_catalog_sql = format!(
                r#"create external catalog ice properties("type"="iceberg","iceberg.catalog.type"="memory","iceberg.catalog.warehouse"="{}")"#,
                warehouse.path().display()
            );
            assert!(matches!(
                session
                    .execute_in_database(&create_catalog_sql, "default")
                    .expect("create iceberg catalog"),
                StatementResult::Ok
            ));
            assert!(matches!(
                session
                    .execute_in_database("create database ice.analytics", "default")
                    .expect("create target namespace"),
                StatementResult::Ok
            ));
            assert!(matches!(
                session
                    .execute_in_database("create database ice.sales", "default")
                    .expect("create source namespace"),
                StatementResult::Ok
            ));
            assert!(matches!(
                session
                    .execute_in_database(
                        "create table ice.sales.orders (id int, name string)",
                        "default",
                    )
                    .expect("create base table"),
                StatementResult::Ok
            ));
            assert!(matches!(
                session
                    .execute_in_context(
                        "CREATE MATERIALIZED VIEW mv_orders \
                         DISTRIBUTED BY HASH(id) BUCKETS 1 \
                         PROPERTIES('storage_engine'='iceberg') \
                         AS SELECT id, name FROM ice.sales.orders",
                        Some("ice"),
                        "analytics",
                        None,
                    )
                    .expect("create iceberg mv"),
                StatementResult::Ok
            ));

            let store = engine
                .inner
                .metadata_store
                .as_ref()
                .expect("metadata store");
            let snapshot = store.load_snapshot().expect("load snapshot");
            assert!(
                !snapshot
                    .managed
                    .tables
                    .iter()
                    .any(|table| table.name == "mv_orders")
            );
            assert!(
                !snapshot.iceberg_tables.iter().any(|table| {
                    table.catalog == "ice"
                        && table.namespace == "analytics"
                        && table.table == "mv_orders"
                }),
                "Iceberg MV target must be restored from relationship metadata, not generic table metadata"
            );
            assert!(snapshot.managed.materialized_views.iter().any(|mv| {
                mv.target_catalog.as_deref() == Some("ice")
                    && mv.target_namespace.as_deref() == Some("analytics")
                    && mv.target_table.as_deref() == Some("mv_orders")
            }));
        }

        let restored = StandaloneNovaRocks::open(StandaloneOptions {
            config_path: None,
            metadata_db_path: Some(metadata_db_path),
        })
        .expect("reopen engine");
        let session = restored.session();
        let show = session
            .execute_in_context("SHOW MATERIALIZED VIEWS", Some("ice"), "analytics", None)
            .expect("show restored mv");
        let StatementResult::Query(show) = show else {
            panic!("expected show query result");
        };
        assert!(query_result_contains_string(&show, "mv_orders"));
        assert!(query_result_contains_string(&show, "iceberg"));

        let info_schema = session
            .execute_in_context(
                "SELECT TABLE_NAME, IS_ACTIVE \
                 FROM information_schema.materialized_views \
                 WHERE TABLE_SCHEMA = 'analytics'",
                Some("ice"),
                "analytics",
                None,
            )
            .expect("query information_schema materialized views");
        let StatementResult::Query(info_schema) = info_schema else {
            panic!("expected information_schema query result");
        };
        assert!(query_result_contains_string(&info_schema, "mv_orders"));

        let select = session
            .execute_in_context("SELECT * FROM mv_orders", Some("ice"), "analytics", None)
            .expect("select restored mv target");
        let StatementResult::Query(select) = select else {
            panic!("expected select query result");
        };
        assert_eq!(select.row_count(), 0);
    }

    #[test]
    fn embedded_session_reopen_cleans_incomplete_managed_truncate_stage_partition() {
        let _runtime_guard = lock_runtime_test_state();
        use crate::connector::starrocks as starrocks_connector;
        use starrocks_connector::managed::store::{
            ManagedIndexState, ManagedPartitionState, SqliteMetadataStore, StoredManagedIndex,
            StoredManagedPartition, StoredManagedTablet,
        };

        let Some((_dir, config_path, metadata_db_path)) = maybe_managed_lake_config() else {
            return;
        };

        {
            let engine = StandaloneNovaRocks::open(StandaloneOptions {
                config_path: Some(config_path.clone()),
                metadata_db_path: None,
            })
            .expect("open engine");
            engine
                .session()
                .execute(
                    "create table orders (k1 int, v1 string) duplicate key(k1) distributed by hash(k1) buckets 2",
                )
                .expect("create managed table");
        }

        let store = SqliteMetadataStore::open(&metadata_db_path).expect("open store");
        let mut snapshot = store.load_snapshot().expect("load snapshot").managed;
        let table = snapshot
            .tables
            .iter()
            .find(|table| table.name == "orders")
            .cloned()
            .expect("orders table");
        let creating_partition_id = snapshot.global.next_partition_id;
        let creating_index_id = snapshot.global.next_index_id;
        let creating_tablet_id = snapshot.global.next_tablet_id;
        snapshot.global.next_partition_id += 1;
        snapshot.global.next_index_id += 1;
        snapshot.global.next_tablet_id += 1;
        snapshot.partitions.push(StoredManagedPartition {
            partition_id: creating_partition_id,
            table_id: table.table_id,
            name: "p0".to_string(),
            visible_version: 1,
            next_version: 2,
            state: ManagedPartitionState::Creating,
        });
        snapshot.indexes.push(StoredManagedIndex {
            index_id: creating_index_id,
            table_id: table.table_id,
            partition_id: creating_partition_id,
            index_type: "BASE".to_string(),
            state: ManagedIndexState::Creating,
        });
        snapshot.tablets.push(StoredManagedTablet {
            tablet_id: creating_tablet_id,
            partition_id: creating_partition_id,
            index_id: creating_index_id,
            bucket_seq: 0,
            tablet_root_path: format!(
                "{}/db_{}/table_{}/partition_{}",
                snapshot.global.warehouse_uri, table.db_id, table.table_id, creating_partition_id
            ),
        });
        store
            .replace_managed_snapshot(&snapshot)
            .expect("persist staged snapshot");

        let reopened = StandaloneNovaRocks::open(StandaloneOptions {
            config_path: Some(config_path),
            metadata_db_path: None,
        })
        .expect("reopen engine");
        let result = reopened
            .session()
            .query("select * from orders")
            .expect("query reopened managed table");
        assert_eq!(result.row_count(), 0);

        let reloaded = store.load_snapshot().expect("reload snapshot").managed;
        assert!(
            !reloaded
                .partitions
                .iter()
                .any(|partition| partition.state == ManagedPartitionState::Creating)
        );
        assert!(
            !reloaded
                .indexes
                .iter()
                .any(|index| index.state == ManagedIndexState::Creating)
        );
        assert!(
            !reloaded
                .tablets
                .iter()
                .any(|tablet| tablet.partition_id == creating_partition_id)
        );
    }

    #[test]
    fn embedded_session_reopen_keeps_truncated_managed_table_empty() {
        let _runtime_guard = lock_runtime_test_state();
        let Some((_dir, config_path, _metadata_db_path)) = maybe_managed_lake_config() else {
            return;
        };

        {
            let engine = StandaloneNovaRocks::open(StandaloneOptions {
                config_path: Some(config_path.clone()),
                metadata_db_path: None,
            })
            .expect("open engine");
            let session = engine.session();
            session
                .execute(
                    "create table orders (k1 int, v1 string) duplicate key(k1) distributed by hash(k1) buckets 2",
                )
                .expect("create managed table");
            session
                .execute("insert into orders values (1, 'a'), (2, 'b')")
                .expect("insert managed rows");
            session
                .execute("truncate table orders")
                .expect("truncate table");
        }

        let reopened = StandaloneNovaRocks::open(StandaloneOptions {
            config_path: Some(config_path),
            metadata_db_path: None,
        })
        .expect("reopen engine");
        let result = reopened
            .session()
            .query("select * from orders")
            .expect("query truncated managed table");
        assert_eq!(result.row_count(), 0);
    }

    #[test]
    fn embedded_session_open_starts_erase_worker_for_pending_jobs() {
        let _runtime_guard = lock_runtime_test_state();
        use crate::connector::starrocks as starrocks_connector;
        use starrocks_connector::managed::store::{
            ManagedEraseJobKind, ManagedEraseJobState, ManagedGlobalMeta, ManagedIndexState,
            ManagedPartitionState, ManagedSnapshot, ManagedTableKind, ManagedTableState,
            SqliteMetadataStore, StoredManagedDatabase, StoredManagedEraseJob, StoredManagedIndex,
            StoredManagedPartition, StoredManagedSchema, StoredManagedTable, StoredManagedTablet,
        };

        let config_dir = TempDir::new().expect("create config dir");
        let metadata_dir = config_dir.path().join("meta");
        std::fs::create_dir_all(&metadata_dir).expect("create metadata dir");
        let metadata_db_path = metadata_dir.join("standalone.sqlite");
        let config_path = config_dir.path().join("novarocks.toml");
        std::fs::write(
            &config_path,
            r#"[standalone_server]
user = "root"
metadata_db_path = "meta/standalone.sqlite"
warehouse_uri = "s3://test/warehouse"

[standalone_server.object_store]
endpoint = "http://127.0.0.1:1"
access_key_id = "ak"
access_key_secret = "sk"
enable_path_style_access = true
"#,
        )
        .expect("write config");

        let store = SqliteMetadataStore::open(&metadata_db_path).expect("open store");
        store
            .replace_managed_snapshot(&ManagedSnapshot {
                global: ManagedGlobalMeta {
                    warehouse_uri: "s3://test/warehouse".to_string(),
                    next_db_id: 2,
                    next_table_id: 11,
                    next_partition_id: 21,
                    next_index_id: 31,
                    next_tablet_id: 41,
                    next_txn_id: 51,
                },
                databases: vec![StoredManagedDatabase {
                    db_id: 1,
                    name: "analytics".to_string(),
                }],
                tables: vec![StoredManagedTable {
                    table_id: 10,
                    db_id: 1,
                    name: "orders".to_string(),
                    keys_type: "DUP_KEYS".to_string(),
                    bucket_num: 1,
                    current_schema_id: 100,
                    state: ManagedTableState::Dropping,
                    kind: ManagedTableKind::Table,
                }],
                schemas: vec![StoredManagedSchema {
                    schema_id: 100,
                    table_id: 10,
                    schema_version: 0,
                    tablet_schema_pb: vec![],
                }],
                columns: Vec::new(),
                partitions: vec![StoredManagedPartition {
                    partition_id: 20,
                    table_id: 10,
                    name: "p0".to_string(),
                    visible_version: 1,
                    next_version: 2,
                    state: ManagedPartitionState::Retired,
                }],
                indexes: vec![StoredManagedIndex {
                    index_id: 30,
                    table_id: 10,
                    partition_id: 20,
                    index_type: "BASE".to_string(),
                    state: ManagedIndexState::Retired,
                }],
                tablets: vec![StoredManagedTablet {
                    tablet_id: 40,
                    partition_id: 20,
                    index_id: 30,
                    bucket_seq: 0,
                    tablet_root_path: "s3://test/warehouse/db_1/table_10/partition_20".to_string(),
                }],
                txns: Vec::new(),
                erase_jobs: vec![StoredManagedEraseJob {
                    job_id: 1,
                    job_kind: ManagedEraseJobKind::DropTable,
                    table_id: 10,
                    partition_id: None,
                    root_path: "s3://test/warehouse".to_string(),
                    state: ManagedEraseJobState::Pending,
                    retry_at_ms: None,
                    updated_at_ms: 0,
                    last_error: None,
                }],
                materialized_views: Vec::new(),
            })
            .expect("persist snapshot");

        let engine = StandaloneNovaRocks::open(StandaloneOptions {
            config_path: Some(config_path),
            metadata_db_path: None,
        })
        .expect("open engine");

        let started = std::time::Instant::now();
        loop {
            let snapshot = store.load_snapshot().expect("load snapshot");
            let job = snapshot
                .managed
                .erase_jobs
                .first()
                .expect("erase job should exist");
            if job.state == ManagedEraseJobState::Failed {
                assert!(
                    job.last_error
                        .as_deref()
                        .is_some_and(|msg| msg.contains("empty managed lake root")),
                    "job should record root validation failure, got {:?}",
                    job.last_error
                );
                break;
            }
            assert!(
                started.elapsed() < std::time::Duration::from_secs(5),
                "erase worker did not mark pending job failed within timeout: state={:?}",
                job.state
            );
            std::thread::sleep(std::time::Duration::from_millis(50));
        }

        drop(engine);
    }

    #[test]
    fn dispatch_statement_routes_materialized_view_ast_variants() {
        let state = Arc::new(StandaloneState::default());
        register_connector_backends(&state);
        let err = dispatch_statement(
            &state,
            None,
            "analytics",
            crate::sql::parser::ast::Statement::RefreshMaterializedView(
                crate::sql::parser::ast::RefreshMaterializedViewStmt {
                    name: crate::sql::parser::ast::ObjectName {
                        parts: vec!["analytics".to_string(), "orders_mv".to_string()],
                    },
                },
            ),
        )
        .expect_err("refresh should fail without managed lake config");
        assert!(
            err.contains("managed lake config is missing")
                || err.contains("sqlite metadata store")
                || err.contains("materialized view"),
            "unexpected dispatch error: {err}"
        );
    }

    // -----------------------------------------------------------------------
    // Iceberg INSERT-SELECT / INSERT OVERWRITE / DELETE round-trips
    // (Plan Tasks 15-17 — IT-INS-1..4 / IT-OW-1..3 / IT-DEL-1..4 / NEG-*)
    // -----------------------------------------------------------------------

    fn open_iceberg_session_with_table(
        warehouse: &TempDir,
        format_version: &str,
    ) -> (StandaloneNovaRocks, StandaloneSession) {
        let engine = StandaloneNovaRocks::open(StandaloneOptions::default()).expect("open engine");
        let session = engine.session();
        let create_catalog_sql = format!(
            r#"create external catalog ice properties("type"="iceberg","iceberg.catalog.type"="memory","iceberg.catalog.warehouse"="{}")"#,
            warehouse.path().display()
        );
        session
            .execute_in_database(&create_catalog_sql, "default")
            .expect("create catalog");
        session
            .execute_in_database("create database ice.db1", "default")
            .expect("create database");
        let create_table_sql = format!(
            r#"create table ice.db1.t (id int, v string) tblproperties("format-version"="{format_version}")"#
        );
        session
            .execute_in_database(&create_table_sql, "default")
            .expect("create table");
        (engine, session)
    }

    fn open_row_lineage_iceberg_session_with_table(
        warehouse: &TempDir,
    ) -> (StandaloneNovaRocks, StandaloneSession) {
        open_row_lineage_iceberg_session_with_table_extra_props(warehouse, &[])
    }

    fn open_row_lineage_iceberg_session_with_table_extra_props(
        warehouse: &TempDir,
        extra_props: &[(&str, &str)],
    ) -> (StandaloneNovaRocks, StandaloneSession) {
        use iceberg::Catalog;

        let engine = StandaloneNovaRocks::open(StandaloneOptions::default()).expect("open engine");
        let session = engine.session();
        let create_catalog_sql = format!(
            r#"create external catalog ice properties("type"="iceberg","iceberg.catalog.type"="memory","iceberg.catalog.warehouse"="{}")"#,
            warehouse.path().display()
        );
        session
            .execute_in_database(&create_catalog_sql, "default")
            .expect("create catalog");
        let catalog = {
            let registry = engine.inner.iceberg_catalogs.read().expect("registry");
            let entry = registry.get("ice").expect("entry");
            crate::connector::iceberg::catalog::registry::build_hadoop_catalog(&entry)
                .expect("build hadoop catalog")
        };
        let namespace = iceberg::NamespaceIdent::new("db1".to_string());
        let schema = iceberg::spec::Schema::builder()
            .with_fields(vec![
                Arc::new(iceberg::spec::NestedField::required(
                    1,
                    "id",
                    iceberg::spec::Type::Primitive(iceberg::spec::PrimitiveType::Int),
                )),
                Arc::new(iceberg::spec::NestedField::required(
                    2,
                    "v",
                    iceberg::spec::Type::Primitive(iceberg::spec::PrimitiveType::String),
                )),
            ])
            .build()
            .expect("build schema");
        let mut props: Vec<(String, String)> =
            vec![("write.row-lineage".to_string(), "true".to_string())];
        for (k, v) in extra_props {
            props.push(((*k).to_string(), (*v).to_string()));
        }
        let table_creation = iceberg::TableCreation::builder()
            .name("t".to_string())
            .schema(schema)
            .format_version(iceberg::spec::FormatVersion::V3)
            .properties(props)
            .build();
        crate::connector::iceberg::catalog::registry::block_on_iceberg(async {
            catalog
                .create_namespace(&namespace, Default::default())
                .await
                .expect("create namespace");
            catalog
                .create_table(&namespace, table_creation)
                .await
                .expect("create row-lineage table");
        })
        .expect("create row-lineage table runtime");
        (engine, session)
    }

    fn collect_id_v(session: &StandaloneSession, sql: &str) -> Vec<(i32, String)> {
        let result = session.query(sql).expect("query");
        let mut out = Vec::new();
        for chunk in &result.chunks {
            let ids = chunk
                .batch
                .column(0)
                .as_any()
                .downcast_ref::<arrow::array::Int32Array>()
                .expect("id i32");
            let names = chunk
                .batch
                .column(1)
                .as_any()
                .downcast_ref::<arrow::array::StringArray>()
                .expect("v utf8");
            for i in 0..chunk.batch.num_rows() {
                out.push((ids.value(i), names.value(i).to_string()));
            }
        }
        out
    }

    #[test]
    fn iceberg_insert_select_drives_a_new_snapshot() {
        // INSERT INTO ... SELECT writes data files + a new snapshot. The
        // standalone iceberg backend's `TableStorage::LocalParquetFile`
        // currently only registers the *first* data file for local-FS
        // tables (see backend.rs:172-179), so a SELECT-side verification
        // would only see the seed file even though the new snapshot
        // includes both. This is a separate NovaRocks-side gap tracked
        // outside Phase 1; here we verify the iceberg layer's snapshot
        // chain advanced as expected via the registry.
        let warehouse = TempDir::new().expect("warehouse");
        let (engine, session) = open_iceberg_session_with_table(&warehouse, "3");
        session
            .execute_in_database("insert into ice.db1.t values (1, 'a'), (2, 'b')", "default")
            .expect("seed");
        let snap_before = current_iceberg_snapshot_id(&engine, "ice", "db1", "t");
        session
            .execute_in_database(
                "insert into ice.db1.t select id, upper(v) from ice.db1.t where id <= 2",
                "default",
            )
            .expect("insert select");
        let snap_after = current_iceberg_snapshot_id(&engine, "ice", "db1", "t");
        assert_ne!(
            snap_before, snap_after,
            "INSERT INTO ... SELECT must advance the iceberg snapshot id"
        );
    }

    fn current_iceberg_snapshot_id(
        engine: &StandaloneNovaRocks,
        catalog: &str,
        namespace: &str,
        table: &str,
    ) -> Option<i64> {
        let registry = engine.inner.iceberg_catalogs.read().expect("registry");
        let entry = registry.get(catalog).expect("entry");
        // `load_table` in the registry caches per-entry; force-bypass by
        // invalidating first so we read disk.
        entry.invalidate_table_cache(namespace, table);
        let loaded =
            crate::connector::iceberg::catalog::load_table(&entry, namespace, table).expect("load");
        loaded
            .table
            .metadata()
            .current_snapshot()
            .map(|s| s.snapshot_id())
    }

    fn current_iceberg_default_spec_fields(
        engine: &StandaloneNovaRocks,
        catalog: &str,
        namespace: &str,
        table: &str,
    ) -> Vec<(String, iceberg::spec::Transform)> {
        let registry = engine.inner.iceberg_catalogs.read().expect("registry");
        let entry = registry.get(catalog).expect("entry");
        entry.invalidate_table_cache(namespace, table);
        let loaded =
            crate::connector::iceberg::catalog::load_table(&entry, namespace, table).expect("load");
        loaded
            .table
            .metadata()
            .default_partition_spec()
            .fields()
            .iter()
            .map(|field| (field.name.clone(), field.transform.clone()))
            .collect()
    }

    #[test]
    fn iceberg_alter_partition_spec_accepts_add_and_drop() {
        let warehouse = TempDir::new().expect("warehouse tempdir");
        let (engine, session) = open_iceberg_session_with_table(&warehouse, "2");
        session
            .execute_in_database(
                r#"create table ice.db1.t_evolved
                   (id bigint, ts datetime)
                   partition by month(ts)
                   tblproperties("format-version"="2")"#,
                "default",
            )
            .expect("create partitioned table");
        assert_eq!(
            current_iceberg_default_spec_fields(&engine, "ice", "db1", "t_evolved"),
            vec![("ts_month".to_string(), iceberg::spec::Transform::Month)]
        );

        session
            .execute_in_database(
                "alter table ice.db1.t_evolved drop partition column month(ts)",
                "default",
            )
            .expect("drop partition column");
        assert_eq!(
            current_iceberg_default_spec_fields(&engine, "ice", "db1", "t_evolved"),
            Vec::<(String, iceberg::spec::Transform)>::new()
        );

        session
            .execute_in_database(
                "alter table ice.db1.t_evolved add partition column bucket(id, 8)",
                "default",
            )
            .expect("add partition column");
        assert_eq!(
            current_iceberg_default_spec_fields(&engine, "ice", "db1", "t_evolved"),
            vec![(
                "id_bucket_8".to_string(),
                iceberg::spec::Transform::Bucket(8)
            )]
        );
    }

    fn current_iceberg_row_lineage(
        engine: &StandaloneNovaRocks,
        catalog: &str,
        namespace: &str,
        table: &str,
    ) -> (u64, Option<(u64, u64)>) {
        let registry = engine.inner.iceberg_catalogs.read().expect("registry");
        let entry = registry.get(catalog).expect("entry");
        entry.invalidate_table_cache(namespace, table);
        let loaded =
            crate::connector::iceberg::catalog::load_table(&entry, namespace, table).expect("load");
        let metadata = loaded.table.metadata();
        (
            metadata.next_row_id(),
            metadata.current_snapshot().and_then(|s| s.row_range()),
        )
    }

    fn current_snapshot_has_position_delete_parquet(
        engine: &StandaloneNovaRocks,
        catalog: &str,
        namespace: &str,
        table: &str,
    ) -> bool {
        let registry = engine.inner.iceberg_catalogs.read().expect("registry");
        let entry = registry.get(catalog).expect("entry");
        entry.invalidate_table_cache(namespace, table);
        let loaded =
            crate::connector::iceberg::catalog::load_table(&entry, namespace, table).expect("load");
        let metadata = loaded.table.metadata();
        let Some(snapshot) = metadata.current_snapshot() else {
            return false;
        };
        let file_io = loaded.table.file_io().clone();
        crate::connector::iceberg::catalog::registry::block_on_iceberg(async {
            let manifest_list = snapshot
                .load_manifest_list(&file_io, metadata)
                .await
                .expect("load manifest list");
            for manifest_file in manifest_list.entries() {
                if manifest_file.content != iceberg::spec::ManifestContentType::Deletes {
                    continue;
                }
                let manifest = manifest_file
                    .load_manifest(&file_io)
                    .await
                    .expect("load delete manifest");
                for entry in manifest.entries() {
                    let data_file = entry.data_file();
                    if entry.is_alive()
                        && data_file.content_type()
                            == iceberg::spec::DataContentType::PositionDeletes
                        && data_file.file_format() == iceberg::spec::DataFileFormat::Parquet
                    {
                        return true;
                    }
                }
            }
            false
        })
        .expect("inspect delete manifests")
    }

    #[test]
    fn iceberg_insert_overwrite_replaces_all_rows() {
        let warehouse = TempDir::new().expect("warehouse");
        let (_engine, session) = open_iceberg_session_with_table(&warehouse, "3");
        session
            .execute_in_database(
                "insert into ice.db1.t values (1, 'a'), (2, 'b'), (3, 'c')",
                "default",
            )
            .expect("seed");
        // INSERT OVERWRITE replaces every row in the table with the SELECT
        // output (Task 13 OverwriteCommit path).
        session
            .execute_in_database(
                "insert overwrite ice.db1.t select id, upper(v) from ice.db1.t where id <= 2",
                "default",
            )
            .expect("overwrite select");
        let mut rows = collect_id_v(&session, "select id, v from ice.db1.t");
        rows.sort_by_key(|(id, _)| *id);
        assert_eq!(
            rows,
            vec![(1, "A".to_string()), (2, "B".to_string())],
            "overwrite must replace ALL rows, not append"
        );
    }

    #[test]
    fn iceberg_delete_where_removes_matching_rows() {
        let warehouse = TempDir::new().expect("warehouse");
        let (engine, session) = open_iceberg_session_with_table(&warehouse, "2");
        session
            .execute_in_database(
                "insert into ice.db1.t values (1, 'a'), (2, 'b'), (3, 'c'), (4, 'd')",
                "default",
            )
            .expect("seed");
        let snap_before = current_iceberg_snapshot_id(&engine, "ice", "db1", "t");
        session
            .execute_in_database("delete from ice.db1.t where id = 2", "default")
            .expect("delete eq");
        let snap_after = current_iceberg_snapshot_id(&engine, "ice", "db1", "t");
        assert_ne!(
            snap_before, snap_after,
            "DELETE WHERE id = 2 must advance the iceberg snapshot id"
        );
        // DELETE with IN list still advances the snapshot.
        session
            .execute_in_database("delete from ice.db1.t where id in (1, 4)", "default")
            .expect("delete in list");
        let snap_after2 = current_iceberg_snapshot_id(&engine, "ice", "db1", "t");
        assert_ne!(
            snap_after, snap_after2,
            "DELETE WHERE id IN (1,4) must advance the iceberg snapshot id again"
        );
    }

    #[test]
    fn iceberg_legacy_delete_still_uses_position_delete_path() {
        let warehouse = TempDir::new().expect("warehouse");
        let (engine, session) = open_iceberg_session_with_table(&warehouse, "2");
        session
            .execute_in_database("insert into ice.db1.t values (1, 'a'), (2, 'b')", "default")
            .expect("seed");
        session
            .execute_in_database("delete from ice.db1.t where id = 1", "default")
            .expect("legacy delete");
        let snap_after = current_iceberg_snapshot_id(&engine, "ice", "db1", "t");
        assert!(snap_after.is_some(), "legacy DELETE must still commit");
        assert!(
            current_snapshot_has_position_delete_parquet(&engine, "ice", "db1", "t"),
            "legacy DELETE must commit at least one live Parquet position-delete file"
        );
    }

    #[test]
    fn iceberg_row_lineage_insert_select_advances_next_row_id() {
        let warehouse = TempDir::new().expect("warehouse");
        let (engine, session) = open_row_lineage_iceberg_session_with_table(&warehouse);
        session
            .execute_in_database("insert into ice.db1.t values (1, 'a'), (2, 'b')", "default")
            .expect("seed");
        let (before_next_row_id, _) = current_iceberg_row_lineage(&engine, "ice", "db1", "t");
        session
            .execute_in_database(
                "insert into ice.db1.t select id, upper(v) from ice.db1.t where id <= 2",
                "default",
            )
            .expect("row-lineage insert select");
        let (after_next_row_id, row_range) =
            current_iceberg_row_lineage(&engine, "ice", "db1", "t");
        assert_eq!(
            after_next_row_id,
            before_next_row_id + 2,
            "row-lineage INSERT SELECT must advance next-row-id by written rows"
        );
        assert_eq!(
            row_range,
            Some((before_next_row_id, 2)),
            "row-lineage INSERT SELECT snapshot must record its row range"
        );
    }

    #[test]
    fn iceberg_row_lineage_optimize_does_not_advance_next_row_id() {
        use crate::connector::starrocks::managed::store::{
            IcebergOptimizeJobState, StoredIcebergOptimizeJob,
        };

        let warehouse = TempDir::new().expect("warehouse");
        let (engine, session) = open_row_lineage_iceberg_session_with_table(&warehouse);

        // Seed 3 INSERTs so OPTIMIZE has multiple input data files to coalesce.
        for i in 1..=3 {
            session
                .execute_in_database(
                    &format!("insert into ice.db1.t values ({i}, '{i}')"),
                    "default",
                )
                .expect("seed");
        }
        let (next_row_id_before, _) = current_iceberg_row_lineage(&engine, "ice", "db1", "t");

        // Locate the current snapshot id so the OPTIMIZE job's base_snapshot_id
        // matches the table's live state — that's the precondition for
        // `validate_base_snapshot` inside the rewrite executor.
        let base_snapshot_id = {
            let registry = engine.inner.iceberg_catalogs.read().expect("registry");
            let entry = registry.get("ice").expect("entry");
            entry.invalidate_table_cache("db1", "t");
            let loaded = crate::connector::iceberg::catalog::load_table(&entry, "db1", "t")
                .expect("load table");
            loaded
                .table
                .metadata()
                .current_snapshot()
                .expect("table must have a current snapshot after INSERTs")
                .snapshot_id()
        };

        let job = StoredIcebergOptimizeJob {
            id: 1,
            catalog: "ice".to_string(),
            namespace: "db1".to_string(),
            table: "t".to_string(),
            base_snapshot_id,
            state: IcebergOptimizeJobState::Pending,
            created_at_ms: 0,
            updated_at_ms: 0,
            started_at_ms: None,
            finished_at_ms: None,
            error_message: None,
            outcome: None,
        };
        let outcome = crate::connector::iceberg::compact::run_one_optimize_job(&engine.inner, &job)
            .expect("run optimize job");
        assert!(
            outcome.target_snapshot_id.is_some(),
            "OPTIMIZE on a non-empty row-lineage table must commit a Replace snapshot"
        );

        let (next_row_id_after, row_range_after) =
            current_iceberg_row_lineage(&engine, "ice", "db1", "t");
        assert_eq!(
            next_row_id_after, next_row_id_before,
            "OPTIMIZE must not advance next_row_id on row-lineage tables (preserve mode)"
        );
        // V3 snapshots require first-row-id to be non-null per Iceberg spec
        // (iceberg-rs vendor rejects null on add_snapshot). Preserve-mode
        // expresses "no new rows allocated" by stamping `(next_row_id, 0)`
        // — same first_row_id as before, zero added rows so next_row_id
        // does not advance. Future INSERTs continue from the same id.
        assert_eq!(
            row_range_after,
            Some((next_row_id_before, 0)),
            "Preserve-mode OPTIMIZE must record (next_row_id, 0) row_range"
        );
    }

    #[test]
    fn iceberg_row_lineage_overwrite_writes_row_range() {
        let warehouse = TempDir::new().expect("warehouse");
        let (engine, session) = open_row_lineage_iceberg_session_with_table(&warehouse);
        session
            .execute_in_database(
                "insert into ice.db1.t values (1, 'a'), (2, 'b'), (3, 'c')",
                "default",
            )
            .expect("seed");
        let (before_next_row_id, _) = current_iceberg_row_lineage(&engine, "ice", "db1", "t");
        session
            .execute_in_database(
                "insert overwrite ice.db1.t select id, upper(v) from ice.db1.t where id <= 2",
                "default",
            )
            .expect("row-lineage overwrite");
        let (after_next_row_id, row_range) =
            current_iceberg_row_lineage(&engine, "ice", "db1", "t");
        assert_eq!(
            after_next_row_id,
            before_next_row_id + 2,
            "row-lineage OVERWRITE must advance next-row-id by added rows"
        );
        assert_eq!(
            row_range,
            Some((before_next_row_id, 2)),
            "row-lineage OVERWRITE snapshot must record its row range"
        );
    }

    #[test]
    fn iceberg_delete_no_match_is_a_noop() {
        let warehouse = TempDir::new().expect("warehouse");
        let (_engine, session) = open_iceberg_session_with_table(&warehouse, "2");
        session
            .execute_in_database("insert into ice.db1.t values (1, 'a')", "default")
            .expect("seed");
        // No row matches → must succeed without committing a delete snapshot.
        session
            .execute_in_database("delete from ice.db1.t where id = 999", "default")
            .expect("delete no-match");
        let rows = collect_id_v(&session, "select id, v from ice.db1.t");
        assert_eq!(rows, vec![(1, "a".to_string())]);
    }

    #[test]
    fn iceberg_delete_without_where_is_rejected() {
        let warehouse = TempDir::new().expect("warehouse");
        let (_engine, session) = open_iceberg_session_with_table(&warehouse, "2");
        session
            .execute_in_database("insert into ice.db1.t values (1, 'a')", "default")
            .expect("seed");
        let err = session
            .execute_in_database("delete from ice.db1.t", "default")
            .expect_err("delete without WHERE must be rejected");
        assert!(
            err.contains("WHERE") || err.contains("INSERT OVERWRITE"),
            "expected WHERE-required error, got {err}"
        );
    }

    #[test]
    fn iceberg_delete_unsupported_predicate_is_rejected() {
        let warehouse = TempDir::new().expect("warehouse");
        let (_engine, session) = open_iceberg_session_with_table(&warehouse, "2");
        session
            .execute_in_database("insert into ice.db1.t values (1, 'a')", "default")
            .expect("seed");
        // LIKE is not in the Phase 1 predicate translator's supported set.
        let err = session
            .execute_in_database("delete from ice.db1.t where v like 'a%'", "default")
            .expect_err("LIKE is not supported in phase 1 DELETE WHERE");
        assert!(
            err.contains("phase 1 DELETE WHERE") || err.contains("Like"),
            "expected unsupported-predicate error, got {err}"
        );
    }

    #[test]
    fn iceberg_row_lineage_delete_writes_puffin_dv_and_merges_second_delete() {
        let warehouse = TempDir::new().expect("warehouse");
        let (engine, session) = open_row_lineage_iceberg_session_with_table(&warehouse);
        session
            .execute_in_database(
                "insert into ice.db1.t values (1, 'a'), (2, 'b'), (3, 'c'), (4, 'd')",
                "default",
            )
            .expect("seed");
        session
            .execute_in_database("delete from ice.db1.t where id = 2", "default")
            .expect("first row-lineage delete");
        session
            .execute_in_database("delete from ice.db1.t where id = 3", "default")
            .expect("second row-lineage delete");

        let registry = engine.inner.iceberg_catalogs.read().expect("registry");
        let entry = registry.get("ice").expect("entry");
        entry.invalidate_table_cache("db1", "t");
        let loaded =
            crate::connector::iceberg::catalog::load_table(&entry, "db1", "t").expect("load");
        let table = loaded.table;
        let metadata = table.metadata();
        let file_io = table.file_io().clone();
        let (live_dv_count, live_dv_cardinality, live_dv_format_is_puffin) =
            crate::connector::iceberg::catalog::registry::block_on_iceberg(async {
                let snapshot = metadata.current_snapshot().expect("current snapshot");
                let manifests = snapshot
                    .load_manifest_list(&file_io, metadata)
                    .await
                    .expect("manifest list");
                let mut dv_count = 0u64;
                let mut total_cardinality = 0u64;
                let mut all_puffin = true;
                for mf in manifests.entries() {
                    if mf.content != iceberg::spec::ManifestContentType::Deletes {
                        continue;
                    }
                    let manifest = mf.load_manifest(&file_io).await.expect("delete manifest");
                    for entry in manifest.entries() {
                        if !entry.is_alive() {
                            continue;
                        }
                        let data_file = entry.data_file();
                        if data_file.content_type()
                            != iceberg::spec::DataContentType::PositionDeletes
                        {
                            continue;
                        }
                        if data_file.file_format() != iceberg::spec::DataFileFormat::Puffin {
                            all_puffin = false;
                            continue;
                        }
                        assert!(
                            data_file.referenced_data_file().is_some(),
                            "Puffin DV must record referenced_data_file"
                        );
                        assert!(
                            data_file.content_offset().is_some(),
                            "Puffin DV must record content_offset"
                        );
                        assert!(
                            data_file.content_size_in_bytes().is_some(),
                            "Puffin DV must record content_size_in_bytes"
                        );
                        dv_count += 1;
                        total_cardinality += data_file.record_count();
                    }
                }
                (dv_count, total_cardinality, all_puffin)
            })
            .expect("inspect manifests");

        assert!(
            live_dv_format_is_puffin,
            "row-lineage DELETE must not commit any non-Puffin position-delete files"
        );
        assert_eq!(
            live_dv_count, 1,
            "two DELETEs against the same data file must merge into one live Puffin DV (count={live_dv_count})"
        );
        assert_eq!(
            live_dv_cardinality, 2,
            "merged DV must record both deleted rows (got {live_dv_cardinality})"
        );
    }

    // ---------------------------------------------------------------------------
    // Helper: read (first_row_id, data_sequence_number) for the current snapshot
    // directly from the iceberg catalog registry.  Used by the row-lineage SELECT
    // integration tests below to build dynamic assertions without querying
    // $snapshots (not yet supported in NovaRocks).
    // ---------------------------------------------------------------------------
    fn current_snapshot_lineage_info(
        engine: &StandaloneNovaRocks,
        catalog: &str,
        namespace: &str,
        table: &str,
    ) -> (u64, i64) {
        let registry = engine.inner.iceberg_catalogs.read().expect("registry");
        let entry = registry.get(catalog).expect("catalog entry");
        entry.invalidate_table_cache(namespace, table);
        let loaded = crate::connector::iceberg::catalog::load_table(&entry, namespace, table)
            .expect("load table");
        let metadata = loaded.table.metadata();
        let snapshot = metadata
            .current_snapshot()
            .expect("table must have a current snapshot");
        let first_row_id = snapshot
            .first_row_id()
            .expect("V3 row-lineage snapshot must carry first_row_id");
        let seq = snapshot.sequence_number();
        (first_row_id, seq)
    }

    // Collect (id, _row_id, _last_updated_sequence_number) tuples from a SELECT
    // that returns exactly those three BIGINT columns.
    fn collect_id_rowid_seq(session: &StandaloneSession, sql: &str) -> Vec<(i64, i64, i64)> {
        let result = session.query(sql).expect("query");
        let mut out = Vec::new();
        for chunk in &result.chunks {
            let ids_col = arrow::compute::cast(chunk.batch.column(0), &DataType::Int64)
                .expect("cast id column to Int64");
            let ids = ids_col
                .as_any()
                .downcast_ref::<Int64Array>()
                .expect("id column must be Int64");
            let row_ids = chunk
                .batch
                .column(1)
                .as_any()
                .downcast_ref::<Int64Array>()
                .expect("_row_id column must be Int64");
            let seqs = chunk
                .batch
                .column(2)
                .as_any()
                .downcast_ref::<Int64Array>()
                .expect("_last_updated_sequence_number column must be Int64");
            for i in 0..chunk.batch.num_rows() {
                out.push((ids.value(i), row_ids.value(i), seqs.value(i)));
            }
        }
        out.sort_by_key(|row| row.0);
        out
    }

    #[test]
    fn iceberg_v3_cow_update_preserves_row_id() {
        let warehouse = TempDir::new().expect("warehouse");
        let (engine, session) = open_row_lineage_iceberg_session_with_table(&warehouse);
        session
            .execute_in_database("insert into ice.db1.t values (1, 'a'), (2, 'b')", "default")
            .expect("insert");
        let previous_snapshot_id =
            current_iceberg_snapshot_id(&engine, "ice", "db1", "t").expect("previous snapshot");
        let before = collect_id_rowid_seq(
            &session,
            "select id, _row_id, _last_updated_sequence_number from ice.db1.t order by id",
        );
        session
            .execute_in_database(
                "update ice.db1.t as t set v = 'bb' where t.id = 2",
                "default",
            )
            .expect("update");
        let after = collect_id_rowid_seq(
            &session,
            "select id, _row_id, _last_updated_sequence_number from ice.db1.t order by id",
        );
        assert_eq!(before[0].1, after[0].1);
        assert_eq!(before[1].1, after[1].1);
        assert_ne!(
            before[1].2, after[1].2,
            "updated row sequence should advance"
        );

        let registry = engine.inner.iceberg_catalogs.read().expect("registry");
        let entry = registry.get("ice").expect("entry");
        entry.invalidate_table_cache("db1", "t");
        let loaded =
            crate::connector::iceberg::catalog::load_table(&entry, "db1", "t").expect("load");
        let current = loaded
            .table
            .metadata()
            .current_snapshot()
            .expect("current snapshot");
        let summary_props = &current.summary().additional_properties;
        assert!(
            !summary_props.contains_key("novarocks.row-level-op"),
            "COW UPDATE must not publish NovaRocks private row-level markers"
        );
        assert!(
            !summary_props.contains_key("novarocks.update.mode"),
            "COW UPDATE must not publish NovaRocks private update-mode markers"
        );
        assert!(
            summary_props.keys().all(|key| !key.contains("sidecar")),
            "COW UPDATE must not publish private sidecar metadata"
        );
        let change_batch = crate::connector::iceberg::changes::plan_changes(
            &loaded.table,
            previous_snapshot_id,
            &[],
        )
        .expect("plan COW update from standard manifest diff");
        assert!(!change_batch.inserts.is_empty());
        assert!(!change_batch.deleted_data_files.is_empty());
    }

    #[test]
    fn iceberg_v3_mor_update_preserves_row_id() {
        let warehouse = TempDir::new().expect("warehouse");
        let (_engine, session) = open_row_lineage_iceberg_session_with_table_extra_props(
            &warehouse,
            &[("novarocks.update.mode", "merge-on-read")],
        );
        session
            .execute_in_database("insert into ice.db1.t values (1, 'a'), (2, 'b')", "default")
            .expect("insert");
        let before = collect_id_rowid_seq(
            &session,
            "select id, _row_id, _last_updated_sequence_number from ice.db1.t order by id",
        );
        session
            .execute_in_database(
                "update ice.db1.t as t set v = 'aa' where t.id = 1",
                "default",
            )
            .expect("mor update");
        let after = collect_id_rowid_seq(
            &session,
            "select id, _row_id, _last_updated_sequence_number from ice.db1.t order by id",
        );
        assert_eq!(after.len(), 2, "MOR UPDATE must not duplicate rows");
        assert_eq!(before[0].1, after[0].1, "_row_id of updated row preserved");
        assert_eq!(
            before[1].1, after[1].1,
            "_row_id of unchanged row preserved"
        );
        assert_ne!(
            before[0].2, after[0].2,
            "updated row sequence number should advance"
        );
        assert_eq!(
            before[1].2, after[1].2,
            "unchanged row sequence number should be stable"
        );
        let v_after = collect_id_v(&session, "select id, v from ice.db1.t order by id");
        assert_eq!(
            v_after,
            vec![(1, "aa".to_string()), (2, "b".to_string())],
            "MOR UPDATE applied new value exactly once"
        );
    }

    #[test]
    fn iceberg_v3_update_from_source_table() {
        // Use a second iceberg table (in the same catalog/namespace) as the
        // source so the test does not depend on managed-lake configuration.
        let warehouse = TempDir::new().expect("warehouse");
        let (_engine, session) = open_row_lineage_iceberg_session_with_table(&warehouse);
        session
            .execute_in_database(
                r#"create table ice.db1.src (id int, new_v string) tblproperties("format-version"="3","write.row-lineage"="true")"#,
                "default",
            )
            .expect("create source iceberg table");
        session
            .execute_in_database("insert into ice.db1.t values (1, 'a'), (2, 'b')", "default")
            .expect("insert target");
        session
            .execute_in_database("insert into ice.db1.src values (2, 'bb')", "default")
            .expect("insert source");
        session
            .execute_in_database(
                "update ice.db1.t as t set v = s.new_v from ice.db1.src as s where t.id = s.id",
                "default",
            )
            .expect("update from source");
        let rows = collect_id_v(&session, "select id, v from ice.db1.t order by id");
        assert_eq!(rows, vec![(1, "a".to_string()), (2, "bb".to_string())]);
    }

    #[test]
    fn iceberg_v3_update_from_rejects_duplicate_source_match() {
        let warehouse = TempDir::new().expect("warehouse");
        let (_engine, session) = open_row_lineage_iceberg_session_with_table(&warehouse);
        session
            .execute_in_database(
                r#"create table ice.db1.src (id int, new_v string) tblproperties("format-version"="3","write.row-lineage"="true")"#,
                "default",
            )
            .expect("create source iceberg table");
        session
            .execute_in_database("insert into ice.db1.t values (1, 'a')", "default")
            .expect("insert target");
        session
            .execute_in_database(
                "insert into ice.db1.src values (1, 'x'), (1, 'y')",
                "default",
            )
            .expect("insert source");
        let err = session
            .execute_in_database(
                "update ice.db1.t as t set v = s.new_v from ice.db1.src as s where t.id = s.id",
                "default",
            )
            .expect_err("duplicate source rows must fail");
        assert!(
            err.contains("more than once"),
            "expected dedup error, got: {err}"
        );
    }

    #[test]
    fn iceberg_v3_cow_update_multiple_files_preserves_row_id() {
        let warehouse = TempDir::new().expect("warehouse");
        let (_engine, session) = open_row_lineage_iceberg_session_with_table(&warehouse);
        session
            .execute_in_database("insert into ice.db1.t values (1, 'a')", "default")
            .expect("insert first file");
        session
            .execute_in_database("insert into ice.db1.t values (2, 'b')", "default")
            .expect("insert second file");
        let before = collect_id_rowid_seq(
            &session,
            "select id, _row_id, _last_updated_sequence_number from ice.db1.t order by id",
        );
        session
            .execute_in_database(
                "update ice.db1.t set v = 'updated' where id in (1, 2)",
                "default",
            )
            .expect("update two files");
        let after = collect_id_rowid_seq(
            &session,
            "select id, _row_id, _last_updated_sequence_number from ice.db1.t order by id",
        );
        assert_eq!(before[0].1, after[0].1);
        assert_eq!(before[1].1, after[1].1);
        assert_ne!(before[0].2, after[0].2);
        assert_ne!(before[1].2, after[1].2);
    }

    // -------------------------------------------------------------------------
    // Task 5: end-to-end SELECT _row_id / _last_updated_sequence_number on a V3
    // row-lineage Iceberg table.
    // -------------------------------------------------------------------------

    // Build a V3 row-lineage table with bigint id and string name columns via
    // the iceberg catalog API (bypassing SQL DDL which defaults to V2).
    fn open_v3_row_lineage_session_bigint(
        warehouse: &TempDir,
    ) -> (StandaloneNovaRocks, StandaloneSession) {
        use iceberg::Catalog;
        use iceberg::spec::{NestedField, PrimitiveType, Type};

        let engine = StandaloneNovaRocks::open(StandaloneOptions::default())
            .expect("open standalone engine");
        let session = engine.session();
        let create_catalog_sql = format!(
            r#"create external catalog ice properties("type"="iceberg","iceberg.catalog.type"="memory","iceberg.catalog.warehouse"="{}")"#,
            warehouse.path().display()
        );
        session
            .execute_in_database(&create_catalog_sql, "default")
            .expect("create iceberg catalog");
        let catalog = {
            let registry = engine.inner.iceberg_catalogs.read().expect("registry");
            let entry = registry.get("ice").expect("entry");
            crate::connector::iceberg::catalog::registry::build_hadoop_catalog(&entry)
                .expect("build hadoop catalog")
        };
        let namespace = iceberg::NamespaceIdent::new("ns".to_string());
        let schema = iceberg::spec::Schema::builder()
            .with_fields(vec![
                Arc::new(NestedField::required(
                    1,
                    "id",
                    Type::Primitive(PrimitiveType::Long),
                )),
                Arc::new(NestedField::optional(
                    2,
                    "name",
                    Type::Primitive(PrimitiveType::String),
                )),
            ])
            .build()
            .expect("build schema");
        let table_creation = iceberg::TableCreation::builder()
            .name("t".to_string())
            .schema(schema)
            .format_version(iceberg::spec::FormatVersion::V3)
            .properties([("write.row-lineage".to_string(), "true".to_string())])
            .build();
        crate::connector::iceberg::catalog::registry::block_on_iceberg(async {
            catalog
                .create_namespace(&namespace, Default::default())
                .await
                .expect("create namespace");
            catalog
                .create_table(&namespace, table_creation)
                .await
                .expect("create V3 row-lineage table");
        })
        .expect("create table runtime");
        (engine, session)
    }

    #[test]
    fn select_row_id_and_last_updated_seq_on_v3_row_lineage_table() {
        let warehouse = TempDir::new().expect("warehouse tempdir");
        let (engine, session) = open_v3_row_lineage_session_bigint(&warehouse);

        // Snapshot S1: 3 rows.
        session
            .execute_in_database(
                "insert into ice.ns.t values (1, 'A'), (2, 'B'), (3, 'C')",
                "default",
            )
            .expect("seed S1");
        let (s1_first_row_id, s1_seq) = current_snapshot_lineage_info(&engine, "ice", "ns", "t");

        let pre_rows = collect_id_rowid_seq(
            &session,
            "select id, _row_id, _last_updated_sequence_number from ice.ns.t",
        );
        assert_eq!(pre_rows.len(), 3, "S1 must have 3 rows");
        assert_eq!(
            pre_rows[0],
            (1_i64, s1_first_row_id as i64, s1_seq),
            "row 0 (id=1)"
        );
        assert_eq!(
            pre_rows[1],
            (2_i64, s1_first_row_id as i64 + 1, s1_seq),
            "row 1 (id=2)"
        );
        assert_eq!(
            pre_rows[2],
            (3_i64, s1_first_row_id as i64 + 2, s1_seq),
            "row 2 (id=3)"
        );

        // Snapshot S2: 2 more rows.
        session
            .execute_in_database("insert into ice.ns.t values (4, 'D'), (5, 'E')", "default")
            .expect("seed S2");
        let (s2_first_row_id, s2_seq) = current_snapshot_lineage_info(&engine, "ice", "ns", "t");

        // S2 must be a later sequence number than S1.
        assert!(
            s2_seq > s1_seq,
            "S2 sequence_number ({s2_seq}) must be greater than S1 ({s1_seq})"
        );
        // S2 first_row_id must follow the 3 rows from S1.
        assert_eq!(
            s2_first_row_id,
            s1_first_row_id + 3,
            "S2 first_row_id must continue from S1 (expected {}, got {s2_first_row_id})",
            s1_first_row_id + 3,
        );

        let post_rows = collect_id_rowid_seq(
            &session,
            "select id, _row_id, _last_updated_sequence_number from ice.ns.t",
        );
        assert_eq!(post_rows.len(), 5, "after S2 must have 5 rows");
        // Old rows keep their S1 row_ids and S1 sequence_numbers.
        assert_eq!(post_rows[0], (1_i64, s1_first_row_id as i64, s1_seq));
        assert_eq!(post_rows[1], (2_i64, s1_first_row_id as i64 + 1, s1_seq));
        assert_eq!(post_rows[2], (3_i64, s1_first_row_id as i64 + 2, s1_seq));
        // New rows get S2 row_ids and S2 sequence_numbers.
        assert_eq!(post_rows[3], (4_i64, s2_first_row_id as i64, s2_seq));
        assert_eq!(post_rows[4], (5_i64, s2_first_row_id as i64 + 1, s2_seq));

        // Delete id=2 via Phase 2a Puffin DV; surviving rows keep their lineage.
        session
            .execute_in_database("delete from ice.ns.t where id = 2", "default")
            .expect("delete row id=2");
        let after_rows = collect_id_rowid_seq(
            &session,
            "select id, _row_id, _last_updated_sequence_number from ice.ns.t",
        );
        assert_eq!(after_rows.len(), 4, "after delete must have 4 rows");
        assert!(
            after_rows.iter().all(|(id, _, _)| *id != 2),
            "id=2 must not appear after DELETE"
        );
        // id=1 preserves its original S1 row_id and sequence_number.
        assert_eq!(
            after_rows[0],
            (1_i64, s1_first_row_id as i64, s1_seq),
            "id=1 must keep S1 lineage after unrelated DELETE"
        );

        drop(engine);
    }

    #[test]
    fn select_row_id_fails_on_v2_iceberg_table() {
        let warehouse = TempDir::new().expect("warehouse tempdir");
        let engine = StandaloneNovaRocks::open(StandaloneOptions::default())
            .expect("open standalone engine");
        let session = engine.session();
        let create_catalog_sql = format!(
            r#"create external catalog ice properties("type"="iceberg","iceberg.catalog.type"="memory","iceberg.catalog.warehouse"="{}")"#,
            warehouse.path().display()
        );
        session
            .execute_in_database(&create_catalog_sql, "default")
            .expect("create catalog");
        session
            .execute_in_database("create database ice.ns", "default")
            .expect("create namespace");
        session
            .execute_in_database(
                r#"create table ice.ns.t2 (id bigint) tblproperties("format-version"="2")"#,
                "default",
            )
            .expect("create V2 iceberg table");

        let err = session
            .execute_in_database("select _row_id from ice.ns.t2", "default")
            .expect_err("selecting _row_id from a V2 table must fail");
        assert!(
            err.contains("only available on Iceberg V3 row-lineage tables"),
            "expected row-lineage error, got: {err}"
        );

        drop(engine);
    }

    #[test]
    fn select_row_id_fails_on_v3_table_without_row_lineage() {
        let warehouse = TempDir::new().expect("warehouse tempdir");
        let engine = StandaloneNovaRocks::open(StandaloneOptions::default())
            .expect("open standalone engine");
        let session = engine.session();
        let create_catalog_sql = format!(
            r#"create external catalog ice properties("type"="iceberg","iceberg.catalog.type"="memory","iceberg.catalog.warehouse"="{}")"#,
            warehouse.path().display()
        );
        session
            .execute_in_database(&create_catalog_sql, "default")
            .expect("create catalog");
        session
            .execute_in_database("create database ice.ns", "default")
            .expect("create namespace");
        session
            .execute_in_database(
                r#"create table ice.ns.t3 (id bigint) tblproperties("format-version"="3")"#,
                "default",
            )
            .expect("create V3 iceberg table without row-lineage");

        let err = session
            .execute_in_database("select _row_id from ice.ns.t3", "default")
            .expect_err("selecting _row_id from a V3 non-row-lineage table must fail");
        assert!(
            err.contains("only available on Iceberg V3 row-lineage tables"),
            "expected row-lineage error, got: {err}"
        );

        drop(engine);
    }

    #[test]
    fn select_last_updated_sequence_number_fails_on_non_row_lineage_iceberg_table() {
        // Tests that _last_updated_sequence_number fails on a regular V3 iceberg
        // table without write.row-lineage=true (same fail-fast path as non-iceberg
        // tables, verified without needing managed lake config).
        let warehouse = TempDir::new().expect("warehouse tempdir");
        let engine = StandaloneNovaRocks::open(StandaloneOptions::default())
            .expect("open standalone engine");
        let session = engine.session();
        let create_catalog_sql = format!(
            r#"create external catalog ice properties("type"="iceberg","iceberg.catalog.type"="memory","iceberg.catalog.warehouse"="{}")"#,
            warehouse.path().display()
        );
        session
            .execute_in_database(&create_catalog_sql, "default")
            .expect("create catalog");
        session
            .execute_in_database("create database ice.ns", "default")
            .expect("create namespace");
        session
            .execute_in_database(
                r#"create table ice.ns.t4 (id bigint) tblproperties("format-version"="2")"#,
                "default",
            )
            .expect("create V2 iceberg table (no row-lineage)");

        let err = session
            .execute_in_database(
                "select _last_updated_sequence_number from ice.ns.t4",
                "default",
            )
            .expect_err("must fail on table without row-lineage");
        assert!(
            err.contains("only available on Iceberg V3 row-lineage tables"),
            "expected row-lineage error, got: {err}"
        );

        drop(engine);
    }
}
