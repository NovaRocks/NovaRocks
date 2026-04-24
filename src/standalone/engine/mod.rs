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

use self::catalog::{
    DEFAULT_DATABASE, InMemoryCatalog, TableStorage, build_parquet_table, normalize_identifier,
};
use super::iceberg::{
    IcebergCatalogRegistry, create_namespace as create_iceberg_namespace,
    namespace_exists as iceberg_namespace_exists,
    register_existing_table as register_existing_iceberg_table,
};
use super::lake::store::{MetadataSnapshot, SqliteMetadataStore, StoredIcebergTable};
use super::lake::{
    ManagedLakeCatalog, ManagedLakeConfig, register_managed_tables_in_catalog, runtime_registered,
};

pub(crate) mod aggregate;
pub(crate) mod catalog;
pub(crate) mod iceberg_glue;
pub(crate) mod insert;
pub(crate) mod name_resolve;
pub(crate) mod parquet;
pub(crate) mod sqlparse;
pub(crate) mod stream_load;

pub(crate) use self::name_resolve::ResolvedLocalTableName;

pub(crate) use self::insert::{build_local_insert_batch, reorder_insert_rows};
use self::parquet::write_parquet_to_path;
#[cfg(test)]
use self::sqlparse::expr::sql_type_to_arrow_type;
#[cfg(test)]
use self::sqlparse::expr::sqlparser_expr_to_literal;
pub(crate) use self::sqlparse::generate_series::insert_generate_series_rows_local;
use self::sqlparse::statement::{
    convert_sqlparser_insert_to_custom, execute_create_database_statement,
    execute_create_table_statement, execute_drop_catalog_statement,
    execute_drop_database_statement, execute_drop_table_statement, execute_insert_statement,
    execute_truncate_table_statement, extract_table_names_from_query,
    extract_three_part_table_refs, looks_like_add_files, parse_add_files_sql,
    strip_catalog_from_three_part_names,
};
use self::stream_load::{
    parse_csv_stream_load_rows, parse_json_stream_load_rows, parse_stream_load_columns,
};

#[derive(Clone, Debug, Default)]
pub struct StandaloneOptions {
    pub config_path: Option<PathBuf>,
    pub metadata_db_path: Option<PathBuf>,
}

pub use crate::runtime::query_result::{QueryResult, QueryResultColumn};

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
    pub(crate) iceberg_catalogs: RwLock<IcebergCatalogRegistry>,
    pub(crate) managed_lake: RwLock<ManagedLakeCatalog>,
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
            iceberg_catalogs: RwLock::new(IcebergCatalogRegistry::default()),
            managed_lake: RwLock::new(ManagedLakeCatalog::default()),
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
        restore_metadata_if_needed(&inner)?;
        if inner.managed_lake_config.is_some() && inner.metadata_store.is_some() {
            super::lake::erase::spawn_erase_worker(Arc::clone(&inner));
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

        let normalized = crate::sql::parser::dialect::normalize_for_raw_parse(sql)?;
        if let Ok(mut statements) = crate::sql::parser::parse_sql(&normalized) {
            let statement = statements
                .pop()
                .ok_or_else(|| "custom parser returned no statements".to_string())?;
            return dispatch_statement(&self.inner, current_database, statement);
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
            .map_err(|e| format!("sql parser error: {e}"))?;

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
            let db_name = crate::sql::parser::dialect::parse_create_database_name(&mut parser)?;
            return execute_create_database_statement(&self.inner, &db_name, current_catalog);
        }
        if looks_like_drop_statement(&parser) {
            let drop = crate::sql::parser::dialect::drop::parse_drop_statement(&mut parser)?;
            return self.handle_drop(drop, current_catalog, current_database);
        }

        // ALTER TABLE ... ADD FILES FROM '...'
        if looks_like_add_files(&normalized) {
            return self.handle_add_files(&normalized, current_catalog, current_database);
        }

        // Standard SQL: let sqlparser parse the full statement
        let stmt = crate::sql::parser::parse_normalized_sql_raw(&parse_sql)
            .map_err(|e| format!("sql parser error: {e}"))?;
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
                if current_catalog.is_some() {
                    register_iceberg_tables_for_query(
                        &self.inner,
                        current_catalog,
                        current_database,
                        query,
                    )?;
                }
                let level = forced_explain_level.unwrap_or_else(|| {
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

                // Handle 3-part table names (catalog.database.table) when no
                // current catalog context is set.  Clone the query, strip the
                // catalog prefix so the analyzer sees 2-part names, and register
                // the referenced Iceberg tables in the local catalog.
                let three_parts = extract_three_part_table_refs(query);
                if !three_parts.is_empty() && current_catalog.is_none() {
                    register_iceberg_tables_for_query(&self.inner, None, current_database, query)?;
                    let mut rewritten = query.as_ref().clone();
                    strip_catalog_from_three_part_names(&mut rewritten);
                    let catalog = self
                        .inner
                        .catalog
                        .read()
                        .expect("standalone catalog read lock");
                    let result = execute_query(
                        &rewritten,
                        &catalog,
                        current_database,
                        self.inner.exchange_port,
                        query_opts.clone(),
                    )?;
                    drop(catalog);
                    return Ok(StatementResult::Query(result));
                }

                let catalog = self
                    .inner
                    .catalog
                    .read()
                    .expect("standalone catalog read lock");
                let result = execute_query(
                    query,
                    &catalog,
                    current_database,
                    self.inner.exchange_port,
                    query_opts.clone(),
                )?;
                drop(catalog);
                Ok(StatementResult::Query(result))
            }
            sqlast::Statement::Insert(ref insert) => self.handle_sqlparser_insert(
                insert,
                current_catalog,
                current_database,
                query_opts.as_ref(),
            ),
            sqlast::Statement::Truncate(truncate) => {
                for truncate_table in &truncate.table_names {
                    let table_name = crate::sql::parser::dialect::convert_object_name(
                        truncate_table.name.clone(),
                    )?;
                    execute_truncate_table_statement(&self.inner, &table_name, current_database)?;
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
        let (table_parts, s3_path) = parse_add_files_sql(sql)?;

        // Resolve catalog and namespace
        let (catalog_name, namespace, table_name) = match table_parts.len() {
            1 => {
                let cat = current_catalog
                    .ok_or("ADD FILES requires a catalog context (use SET catalog)")?;
                (
                    cat.to_string(),
                    current_database.to_string(),
                    table_parts[0].clone(),
                )
            }
            2 => {
                let cat = current_catalog.ok_or("ADD FILES requires a catalog context")?;
                (
                    cat.to_string(),
                    table_parts[0].clone(),
                    table_parts[1].clone(),
                )
            }
            3 => (
                table_parts[0].clone(),
                table_parts[1].clone(),
                table_parts[2].clone(),
            ),
            _ => return Err(format!("invalid table name in ADD FILES")),
        };

        let guard = self
            .inner
            .iceberg_catalogs
            .read()
            .expect("iceberg catalog read lock");
        let entry = guard.get(&catalog_name)?;
        drop(guard);
        let count =
            super::iceberg::add_files::add_files(&entry, &namespace, &table_name, &s3_path)?;
        let msg = format!("Added {count} file(s)");
        build_string_query_result("status", vec![msg]).map(StatementResult::Query)
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
            DropResult::Database(stmt) => execute_drop_database_statement(
                &self.inner,
                &stmt.name,
                current_catalog,
                stmt.if_exists,
                stmt.force,
            ),
            DropResult::Table(stmt) => execute_drop_table_statement(
                &self.inner,
                &stmt.name,
                current_catalog,
                current_database,
                stmt.if_exists,
                stmt.force,
            ),
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
            current_catalog,
            current_database,
        )
    }
}

// ---------------------------------------------------------------------------
// Custom statement dispatch
// ---------------------------------------------------------------------------

pub(crate) fn dispatch_statement(
    state: &Arc<StandaloneState>,
    current_database: &str,
    statement: crate::sql::parser::ast::Statement,
) -> Result<StatementResult, String> {
    match statement {
        crate::sql::parser::ast::Statement::CreateMaterializedView(stmt) => {
            super::lake::mv_ddl::create_mv(state, current_database, &stmt)
        }
        crate::sql::parser::ast::Statement::DropMaterializedView(stmt) => {
            super::lake::mv_ddl::drop_mv(state, current_database, &stmt)
        }
        crate::sql::parser::ast::Statement::RefreshMaterializedView(stmt) => {
            super::lake::mv_refresh::refresh_mv(state, current_database, &stmt)
        }
        crate::sql::parser::ast::Statement::ShowMaterializedViews(stmt) => {
            super::lake::mv_ddl::list_mvs(state, &stmt)
        }
    }
}

pub(crate) fn register_iceberg_tables_for_query(
    state: &Arc<StandaloneState>,
    current_catalog: Option<&str>,
    current_database: &str,
    query: &sqlparser::ast::Query,
) -> Result<(), String> {
    register_iceberg_tables_for_query_impl(state, current_catalog, current_database, query, false)
}

fn refresh_iceberg_tables_for_query(
    state: &Arc<StandaloneState>,
    current_catalog: Option<&str>,
    current_database: &str,
    query: &sqlparser::ast::Query,
) -> Result<(), String> {
    register_iceberg_tables_for_query_impl(state, current_catalog, current_database, query, true)
}

fn register_empty_iceberg_table(
    namespace: &str,
    table_name: &str,
    columns: &[crate::sql::catalog::ColumnDef],
) -> Result<TableStorage, String> {
    let dir = std::env::temp_dir().join("novarocks_iceberg_empty");
    std::fs::create_dir_all(&dir).map_err(|e| format!("create empty dir: {e}"))?;
    let path = dir.join(format!("{}_{}.parquet", namespace, table_name));
    let schema = std::sync::Arc::new(arrow::datatypes::Schema::new(
        columns
            .iter()
            .map(|column| {
                arrow::datatypes::Field::new(
                    &column.name,
                    column.data_type.clone(),
                    column.nullable,
                )
            })
            .collect::<Vec<_>>(),
    ));
    let empty_arrays: Vec<arrow::array::ArrayRef> = schema
        .fields()
        .iter()
        .map(|field| arrow::array::new_empty_array(field.data_type()))
        .collect();
    let empty_batch = RecordBatch::try_new(schema, empty_arrays)
        .map_err(|e| format!("build empty batch: {e}"))?;
    write_parquet_to_path(&path, &empty_batch)?;
    Ok(TableStorage::LocalParquetFile { path })
}

fn build_iceberg_table_def_with_files(
    entry: &crate::standalone::iceberg::IcebergCatalogEntry,
    namespace: &str,
    table_name: &str,
    loaded: crate::standalone::iceberg::IcebergLoadedTable,
    data_files: Vec<(String, i64, Option<i64>)>,
) -> Result<crate::sql::catalog::TableDef, String> {
    let storage = if entry.is_s3() {
        let cloud_properties = entry.cloud_properties_map();
        crate::sql::catalog::TableStorage::S3ParquetFiles {
            files: data_files
                .into_iter()
                .map(|(path, size, row_count)| crate::sql::catalog::S3FileInfo {
                    path,
                    size,
                    row_count,
                    column_stats: None,
                })
                .collect(),
            cloud_properties,
        }
    } else if let Some((first_path, _, _)) = data_files.first() {
        let local_path = first_path.strip_prefix("file://").unwrap_or(first_path);
        crate::sql::catalog::TableStorage::LocalParquetFile {
            path: std::path::PathBuf::from(local_path),
        }
    } else {
        register_empty_iceberg_table(namespace, table_name, &loaded.columns)?
    };

    Ok(crate::sql::catalog::TableDef {
        name: table_name.to_string(),
        columns: loaded.columns,
        storage,
    })
}

fn register_loaded_iceberg_table_with_files(
    state: &Arc<StandaloneState>,
    entry: &crate::standalone::iceberg::IcebergCatalogEntry,
    namespace: &str,
    table_name: &str,
    loaded: crate::standalone::iceberg::IcebergLoadedTable,
    data_files: Vec<(String, i64, Option<i64>)>,
) -> Result<(), String> {
    let table_def =
        build_iceberg_table_def_with_files(entry, namespace, table_name, loaded, data_files)?;
    let mut guard = state.catalog.write().expect("catalog write lock");
    guard.create_database(namespace).ok();
    guard
        .register(namespace, table_def)
        .map_err(|e| format!("register iceberg table: {e}"))?;
    Ok(())
}

fn register_iceberg_tables_for_query_impl(
    state: &Arc<StandaloneState>,
    current_catalog: Option<&str>,
    current_database: &str,
    query: &sqlparser::ast::Query,
    force_refresh: bool,
) -> Result<(), String> {
    let mut targets = if let Some(catalog_name) = current_catalog {
        extract_table_names_from_query(query)
            .into_iter()
            .map(|table_name| {
                (
                    catalog_name.to_string(),
                    current_database.to_string(),
                    table_name,
                )
            })
            .collect::<Vec<_>>()
    } else {
        extract_three_part_table_refs(query)
    };
    if targets.is_empty() {
        return Ok(());
    }
    targets.sort();
    targets.dedup();

    let iceberg_guard = state
        .iceberg_catalogs
        .read()
        .expect("iceberg catalog read lock");
    for (catalog_name, namespace, table_name) in targets {
        let entry = match iceberg_guard.get(&catalog_name) {
            Ok(entry) => entry,
            Err(_) => continue,
        };

        if !force_refresh {
            let local = state.catalog.read().expect("catalog read lock");
            if local.get(&namespace, &table_name).is_ok() {
                continue;
            }
        }

        let loaded = match super::iceberg::load_table(&entry, &namespace, &table_name) {
            Ok(loaded) => loaded,
            Err(_) => continue,
        };

        let data_files = super::iceberg::extract_data_files(&loaded.table)?;
        register_loaded_iceberg_table_with_files(
            state,
            &entry,
            &namespace,
            &table_name,
            loaded,
            data_files,
        )?;
    }

    Ok(())
}

pub(crate) fn execute_query_for_mv_refresh(
    state: &Arc<StandaloneState>,
    current_database: &str,
    sql: &str,
) -> Result<QueryResult, String> {
    let normalized = crate::sql::parser::dialect::normalize_for_raw_parse(sql)?;
    let statement = crate::sql::parser::parse_normalized_sql_raw(&normalized)
        .map_err(|e| format!("sql parser error: {e}"))?;
    let sqlparser::ast::Statement::Query(query) = statement else {
        return Err("REFRESH MATERIALIZED VIEW stored SQL must be a SELECT query".to_string());
    };

    let three_parts = extract_three_part_table_refs(&query);
    if !three_parts.is_empty() {
        refresh_iceberg_tables_for_query(state, None, current_database, &query)?;
    }

    let mut executable = query.as_ref().clone();
    if !three_parts.is_empty() {
        strip_catalog_from_three_part_names(&mut executable);
    }
    let catalog = state.catalog.read().expect("standalone catalog read lock");
    execute_query(
        &executable,
        &catalog,
        current_database,
        state.exchange_port,
        None,
    )
}

fn normalize_incremental_mv_base_ref(
    base_ref: &crate::standalone::lake::store::IcebergTableRef,
) -> Result<(String, String, String), String> {
    Ok((
        normalize_identifier(&base_ref.catalog)?,
        normalize_identifier(&base_ref.namespace)?,
        normalize_identifier(&base_ref.table)?,
    ))
}

fn extract_three_part_table_ref_occurrences(
    query: &sqlparser::ast::Query,
) -> Vec<(String, String, String)> {
    let mut refs = Vec::new();
    extract_three_part_ref_occurrences_from_set_expr(query.body.as_ref(), &mut refs);
    refs
}

fn extract_three_part_ref_occurrences_from_set_expr(
    expr: &sqlparser::ast::SetExpr,
    refs: &mut Vec<(String, String, String)>,
) {
    match expr {
        sqlparser::ast::SetExpr::Select(select) => {
            for from in &select.from {
                extract_three_part_ref_occurrences_from_factor(&from.relation, refs);
                for join in &from.joins {
                    extract_three_part_ref_occurrences_from_factor(&join.relation, refs);
                }
            }
        }
        sqlparser::ast::SetExpr::SetOperation { left, right, .. } => {
            extract_three_part_ref_occurrences_from_set_expr(left, refs);
            extract_three_part_ref_occurrences_from_set_expr(right, refs);
        }
        sqlparser::ast::SetExpr::Query(query) => {
            extract_three_part_ref_occurrences_from_set_expr(query.body.as_ref(), refs);
        }
        _ => {}
    }
}

fn extract_three_part_ref_occurrences_from_factor(
    factor: &sqlparser::ast::TableFactor,
    refs: &mut Vec<(String, String, String)>,
) {
    match factor {
        sqlparser::ast::TableFactor::Table { name, .. } => {
            let parts: Vec<String> = name
                .0
                .iter()
                .filter_map(|part| match part {
                    sqlparser::ast::ObjectNamePart::Identifier(ident) => {
                        Some(ident.value.to_lowercase())
                    }
                    _ => None,
                })
                .collect();
            if parts.len() == 3 {
                refs.push((parts[0].clone(), parts[1].clone(), parts[2].clone()));
            }
        }
        sqlparser::ast::TableFactor::Derived { subquery, .. } => {
            extract_three_part_ref_occurrences_from_set_expr(subquery.body.as_ref(), refs);
        }
        _ => {}
    }
}

fn validate_incremental_mv_base_ref(
    query: &sqlparser::ast::Query,
    base_ref: &crate::standalone::lake::store::IcebergTableRef,
) -> Result<(String, String, String), String> {
    let refs = extract_three_part_table_ref_occurrences(query);
    if refs.len() != 1 {
        return Err(format!(
            "incremental MV refresh stored SQL must reference exactly one 3-part Iceberg table, got {}",
            refs.len()
        ));
    }

    let actual = {
        let (catalog, namespace, table) = &refs[0];
        (
            normalize_identifier(catalog).map_err(|e| {
                format!("incremental MV refresh stored SQL has invalid catalog reference: {e}")
            })?,
            normalize_identifier(namespace).map_err(|e| {
                format!("incremental MV refresh stored SQL has invalid namespace reference: {e}")
            })?,
            normalize_identifier(table).map_err(|e| {
                format!("incremental MV refresh stored SQL has invalid table reference: {e}")
            })?,
        )
    };
    let expected = normalize_incremental_mv_base_ref(base_ref)?;
    if actual != expected {
        return Err(format!(
            "incremental MV refresh stored SQL base table mismatch: expected {}.{}.{}, got {}.{}.{}",
            expected.0, expected.1, expected.2, actual.0, actual.1, actual.2
        ));
    }
    Ok(expected)
}

pub(crate) fn execute_query_for_mv_incremental_refresh(
    state: &Arc<StandaloneState>,
    current_database: &str,
    sql: &str,
    base_ref: &crate::standalone::lake::store::IcebergTableRef,
    delta_files: Vec<(String, i64, Option<i64>)>,
) -> Result<QueryResult, String> {
    let normalized = crate::sql::parser::dialect::normalize_for_raw_parse(sql)?;
    let statement = crate::sql::parser::parse_normalized_sql_raw(&normalized)
        .map_err(|e| format!("sql parser error: {e}"))?;
    let sqlparser::ast::Statement::Query(query) = statement else {
        return Err("REFRESH MATERIALIZED VIEW stored SQL must be a SELECT query".to_string());
    };

    let (catalog_name, namespace, table_name) = validate_incremental_mv_base_ref(&query, base_ref)?;
    let entry = {
        let registry = state
            .iceberg_catalogs
            .read()
            .expect("iceberg registry read lock");
        registry.get(&catalog_name)?
    };
    if !entry.is_s3() && delta_files.len() > 1 {
        return Err(
            "incremental MV refresh over local iceberg supports at most one delta file".to_string(),
        );
    }

    let loaded = super::iceberg::load_table(&entry, &namespace, &table_name)?;
    let table_def =
        build_iceberg_table_def_with_files(&entry, &namespace, &table_name, loaded, delta_files)?;
    let mut incremental_catalog = InMemoryCatalog::default();
    incremental_catalog.create_database(&namespace)?;
    incremental_catalog
        .register(&namespace, table_def)
        .map_err(|e| format!("register incremental iceberg table: {e}"))?;

    let mut executable = query.as_ref().clone();
    strip_catalog_from_three_part_names(&mut executable);
    execute_query(
        &executable,
        &incremental_catalog,
        current_database,
        state.exchange_port,
        None,
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
    super::lake::reconcile_on_open(store, &mut managed, |snapshot, txn| {
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
        super::lake::txn::publish_tablets_at_version(
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
    SingleFragment(PlanBuildResult),
    Coordinated(MultiFragmentBuildResult),
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
) -> Result<PlanBuildResult, MultiFragmentBuildResult> {
    if build_result.fragment_results.len() != 1 {
        return Err(build_result);
    }
    let fragment = build_result.fragment_results.into_iter().next().unwrap();
    Ok(PlanBuildResult {
        plan: fragment.plan,
        desc_tbl: fragment.desc_tbl,
        exec_params: fragment.exec_params,
        output_columns: fragment.output_columns,
    })
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

    StandaloneExecutionPlan::Coordinated(build_result)
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
        StandaloneExecutionPlan::SingleFragment(plan) => execute_plan(plan, query_opts),
        StandaloneExecutionPlan::Coordinated(build_result) => {
            crate::runtime::coordinator::ExecutionCoordinator::new(
                build_result,
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
            {
                if let Some(ts) = crate::sql::optimizer::statistics::build_table_statistics(files) {
                    // Insert by table name (canonical key).
                    out.insert(s.table.name.clone(), ts.clone());
                    // Also insert by alias so that aliased scans can find their stats.
                    if let Some(ref alias) = s.alias {
                        out.insert(alias.clone(), ts);
                    }
                }
            }
        }
        LogicalPlan::Filter(n) => collect_scan_stats(&n.input, out),
        LogicalPlan::Project(n) => collect_scan_stats(&n.input, out),
        LogicalPlan::Aggregate(n) => collect_scan_stats(&n.input, out),
        LogicalPlan::Sort(n) => collect_scan_stats(&n.input, out),
        LogicalPlan::Limit(n) => collect_scan_stats(&n.input, out),
        LogicalPlan::Window(n) => collect_scan_stats(&n.input, out),
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
    super::lake::txn::insert_into_managed_lake_table(
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
        StandaloneNovaRocks, StandaloneOptions, StandaloneState, StatementResult,
        dispatch_statement,
    };
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
            },
            ColumnDef {
                name: "score_items".to_string(),
                data_type: DataType::List(Arc::new(Field::new("item", DataType::Int32, true))),
                nullable: true,
            },
            ColumnDef {
                name: "tags".to_string(),
                data_type: DataType::List(Arc::new(Field::new("item", DataType::Utf8, true))),
                nullable: true,
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
            crate::standalone::iceberg::load_table(&entry, "db1", "tbl").expect("load first table");
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

        let second_loaded = crate::standalone::iceberg::load_table(&entry, "db1", "tbl")
            .expect("load second table");
        let delta = crate::standalone::iceberg::plan_append_delta(
            &second_loaded.table,
            previous_snapshot_id,
        )
        .expect("plan append delta");

        let result = super::execute_query_for_mv_incremental_refresh(
            &engine.inner,
            "default",
            "select id, name from ice.db1.tbl order by id",
            &crate::standalone::lake::store::IcebergTableRef {
                catalog: "ice".to_string(),
                namespace: "db1".to_string(),
                table: "tbl".to_string(),
            },
            delta.added_files,
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

        let err = super::execute_query_for_mv_incremental_refresh(
            &engine.inner,
            "default",
            "select id, name from ice.db1.tbl",
            &crate::standalone::lake::store::IcebergTableRef {
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
        let base_ref = crate::standalone::lake::store::IcebergTableRef {
            catalog: "ice".to_string(),
            namespace: "db1".to_string(),
            table: "tbl".to_string(),
        };

        let err = super::execute_query_for_mv_incremental_refresh(
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

        let err = super::execute_query_for_mv_incremental_refresh(
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

        let err = super::execute_query_for_mv_incremental_refresh(
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
            crate::standalone::iceberg::load_table(&entry, "db1", "tbl").expect("load first table");
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

        let second_loaded = crate::standalone::iceberg::load_table(&entry, "db1", "tbl")
            .expect("load second table");
        let mut delta_files = crate::standalone::iceberg::plan_append_delta(
            &second_loaded.table,
            previous_snapshot_id,
        )
        .expect("plan append delta")
        .added_files;
        let first_delta_file = delta_files
            .first()
            .expect("at least one delta file")
            .clone();
        delta_files.push(first_delta_file);

        let err = super::execute_query_for_mv_incremental_refresh(
            &engine.inner,
            "default",
            "select id, name from ice.db1.tbl",
            &crate::standalone::lake::store::IcebergTableRef {
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
    fn embedded_session_reopen_cleans_incomplete_managed_truncate_stage_partition() {
        use crate::standalone::lake::store::{
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
        use crate::standalone::lake::store::{
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
        let err = dispatch_statement(
            &state,
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
            err.contains("managed lake config is missing") || err.contains("materialized view"),
            "unexpected dispatch error: {err}"
        );
    }
}
