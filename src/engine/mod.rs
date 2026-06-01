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

use self::catalog::{DEFAULT_DATABASE, InMemoryCatalog, normalize_identifier};
use crate::connector::{
    IcebergCatalogRegistry, StarRocksTableCatalog, StarRocksTableConfig, create_iceberg_namespace,
    iceberg_namespace_exists, register_existing_iceberg_table,
    register_starrocks_tables_in_catalog, runtime_registered,
};
use crate::meta::repository::iceberg_catalog::{
    IcebergCatalogMetaRepository, IcebergCatalogProperties,
};
use crate::meta::repository::job::{
    CreateIcebergOptimizeJobRequest, IcebergOptimizeJobState, JobMetaRepository,
    StoredIcebergOptimizeJob,
};
use crate::meta::repository::mv::MvMetaRepository;
use crate::meta::repository::starrocks_table::StarRocksTableMetaRepository;
use crate::meta::repository::starrocks_txn::StarRocksTxnRepository;

pub(crate) mod aggregate;
pub(crate) mod backend_resolver;
pub(crate) mod catalog;
pub(crate) mod catalog_mgr;
pub(crate) mod dictionary;
pub(crate) mod iceberg_ctas;
pub(crate) mod iceberg_ref_flow;
pub(crate) mod information_schema;
pub(crate) mod insert;
pub(crate) mod insert_flow;
pub(crate) mod mutation_flow;
pub(crate) mod mv;
pub(crate) mod mv_flow;
pub(crate) mod mv_scheduler;
pub(crate) mod name_resolve;
pub(crate) mod parquet;
pub(crate) mod query_prep;
pub(crate) mod sql_expr;
pub(crate) mod starrocks_table_ctas;
pub(crate) mod statement;
pub(crate) mod statistics;
pub(crate) mod stream_load;
pub(crate) mod view_rewrite;
pub(crate) mod virtual_table;

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
    looks_like_show_alter_table_optimize, looks_like_show_create_table,
    parse_add_legacy_range_partition_sql, parse_alter_iceberg_properties_sql,
    parse_alter_partition_column_sql, parse_alter_table_expire_snapshots_sql,
    parse_alter_table_optimize_sql, parse_alter_table_remove_orphan_files_sql,
    parse_alter_table_rewrite_manifests_sql, parse_show_alter_table_optimize_sql,
    parse_show_create_table,
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
}

pub use crate::runtime::query_result::{QueryResult, QueryResultColumn};
use crate::sql::catalog::LegacyRangePartition;
pub use crate::sql::catalog::{CatalogProvider, ColumnDef, ScanSource, TableDef};

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
pub struct StandaloneStarRocksTabletInfo {
    pub tablet_id: i64,
    pub bucket_seq: i64,
    pub tablet_root_path: String,
    pub runtime_registered: bool,
    pub snapshot_version: Option<i64>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StandaloneStarRocksTableInfo {
    pub database_name: String,
    pub table_name: String,
    pub table_id: i64,
    pub current_schema_id: i64,
    pub keys_type: String,
    pub bucket_num: i64,
    pub visible_version: i64,
    pub tablets: Vec<StandaloneStarRocksTabletInfo>,
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
    pub(crate) starrocks_table: RwLock<StarRocksTableCatalog>,
    pub(crate) statistics: RwLock<statistics::StandaloneStatistics>,
    pub(crate) connectors: Arc<RwLock<crate::connector::ConnectorRegistry>>,
    pub(crate) starrocks_table_config: Option<StarRocksTableConfig>,
    pub(crate) metadata_provider: Option<Arc<dyn crate::meta::MetaStoreProvider>>,
    pub(crate) starrocks_table_repo: StarRocksTableMetaRepository,
    pub(crate) starrocks_txn_repo: StarRocksTxnRepository,
    pub(crate) mv_repo: MvMetaRepository,
    pub(crate) iceberg_catalog_repo: IcebergCatalogMetaRepository,
    pub(crate) job_repo: JobMetaRepository,
    pub(crate) dictionary_manager: dictionary::DictionaryManager,
    pub(crate) exchange_port: u16,
    /// In-memory registry of user-defined views, keyed by lowercase
    /// (database, view-name). Each entry stores the analysed `Query` AST
    /// from `CREATE VIEW ... AS <query>`. The analyzer expands these to
    /// derived tables on `FROM <view>` references.
    pub(crate) views:
        RwLock<std::collections::HashMap<(String, String), Box<sqlparser::ast::Query>>>,
    /// information_schema virtual tables (`schemata`, ...). Rows are
    /// materialized at query rewrite time by [`virtual_table::inject_query_refs`]
    /// and injected into a cloned catalog snapshot, so the standard SQL
    /// pipeline scans them as ordinary base tables.
    pub(crate) virtual_tables: virtual_table::VirtualTableRegistry,
    #[cfg(test)]
    pub(crate) _test_guard: Option<TestSerializationGuard>,
}

impl Default for StandaloneState {
    fn default() -> Self {
        Self {
            catalog: RwLock::new(InMemoryCatalog::default()),
            iceberg_catalogs: Arc::new(RwLock::new(IcebergCatalogRegistry::default())),
            starrocks_table: RwLock::new(StarRocksTableCatalog::default()),
            statistics: RwLock::new(statistics::StandaloneStatistics::default()),
            connectors: Arc::new(RwLock::new(crate::connector::ConnectorRegistry::default())),
            starrocks_table_config: None,
            metadata_provider: None,
            starrocks_table_repo: StarRocksTableMetaRepository,
            starrocks_txn_repo: StarRocksTxnRepository,
            mv_repo: MvMetaRepository,
            iceberg_catalog_repo: IcebergCatalogMetaRepository,
            job_repo: JobMetaRepository,
            dictionary_manager: dictionary::DictionaryManager::default(),
            exchange_port: 0,
            views: RwLock::new(std::collections::HashMap::new()),
            virtual_tables: virtual_table::VirtualTableRegistry::with_defaults(),
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
        #[cfg(test)]
        return Self::open_body(opts, _test_guard);
        #[cfg(not(test))]
        Self::open_body(opts)
    }

    /// Open the engine using an already-loaded, validated config.
    ///
    /// Installs `cfg` as the process-wide active config (replacing any prior
    /// global config) and then proceeds with the normal engine-open body.
    /// `opts.config_path` is preserved for resolving relative paths (e.g.
    /// SQLite metadata DB paths) but is **not** re-read from disk.
    pub fn open_with_config(
        opts: StandaloneOptions,
        cfg: novarocks_config::NovaRocksConfig,
    ) -> Result<Self, String> {
        #[cfg(test)]
        let _test_guard = Some(acquire_standalone_test_guard());
        novarocks_config::install_preloaded_config(cfg);
        #[cfg(test)]
        return Self::open_body(opts, _test_guard);
        #[cfg(not(test))]
        Self::open_body(opts)
    }

    /// Common engine-open body.  Called after the process-wide config has
    /// already been installed by the caller.
    fn open_body(
        opts: StandaloneOptions,
        #[cfg(test)] _test_guard: Option<TestSerializationGuard>,
    ) -> Result<Self, String> {
        // Spec (PR-4): role=fe dispatches all fragments to the remote BE via
        // RemoteDispatcher and must NOT start a local gRPC/exchange server.
        // exchange_port is only used by InProcessDispatcher (AllInOne); for Fe
        // it is unused by dispatcher_for_role so a non-zero sentinel avoids
        // the force_single_fragment=true short-circuit in execute_query_inner.
        let role = crate::novarocks_config::config()
            .map(|c| c.cluster.role)
            .unwrap_or(crate::common::app_config::ClusterRole::AllInOne);
        let exchange_port = if role == crate::common::app_config::ClusterRole::Fe {
            // Sentinel: non-zero to allow coordinated execution, but no local socket is bound.
            u16::MAX
        } else {
            ensure_standalone_exchange_server()?
        };
        let metadata_backend = resolve_metadata_backend(&opts)?;
        let metadata_provider = metadata_backend
            .as_ref()
            .map(open_metadata_provider)
            .transpose()?;
        let starrocks_table_config = resolve_starrocks_table_config()?;
        let inner = Arc::new(StandaloneState {
            starrocks_table: RwLock::new(StarRocksTableCatalog::empty(
                starrocks_table_config.clone(),
            )),
            starrocks_table_config,
            metadata_provider,
            starrocks_table_repo: StarRocksTableMetaRepository,
            starrocks_txn_repo: StarRocksTxnRepository,
            mv_repo: MvMetaRepository,
            iceberg_catalog_repo: IcebergCatalogMetaRepository,
            job_repo: JobMetaRepository,
            exchange_port,
            #[cfg(test)]
            _test_guard,
            ..Default::default()
        });
        register_connector_backends(&inner);
        restore_metadata_if_needed(&inner)?;
        if inner.starrocks_table_config.is_some() && inner.metadata_provider.is_some() {
            crate::connector::spawn_starrocks_table_erase_worker(Arc::clone(&inner));
        }
        #[cfg(not(test))]
        if inner.metadata_provider.is_some() {
            crate::connector::spawn_iceberg_optimize_worker(Arc::clone(&inner));
        }
        Ok(Self { inner })
    }

    pub fn session(&self) -> StandaloneSession {
        StandaloneSession {
            inner: Arc::clone(&self.inner),
        }
    }

    #[cfg(test)]
    pub(crate) fn run_pending_optimize_jobs_for_test(&self) -> Result<(), String> {
        crate::connector::iceberg::compact::run_optimize_jobs_once(&self.inner)
    }

    pub fn starrocks_table_info(
        &self,
        database_name: &str,
        table_name: &str,
    ) -> Result<StandaloneStarRocksTableInfo, String> {
        let starrocks = self
            .inner
            .starrocks_table
            .read()
            .expect("standalone StarRocks table read lock");
        let runtime = starrocks.table(database_name, table_name)?;
        let visible_version = runtime
            .partitions
            .iter()
            .map(|partition| partition.visible_version)
            .max()
            .unwrap_or(1);
        let object_store_profile = starrocks
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
                StandaloneStarRocksTabletInfo {
                    tablet_id: tablet.tablet_id,
                    bucket_seq: tablet.bucket_seq,
                    tablet_root_path: tablet.tablet_root_path.clone(),
                    runtime_registered: runtime_registered(tablet.tablet_id),
                    snapshot_version,
                }
            })
            .collect();
        Ok(StandaloneStarRocksTableInfo {
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

    pub(crate) fn stream_load_starrocks_table(
        &self,
        request: StandaloneStreamLoadRequest,
    ) -> Result<StandaloneStreamLoadResult, String> {
        stream_load_starrocks_table(&self.inner, request)
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
        // Install the per-statement dictionary provider so optimizer
        // calls reached through nested engine entry points (insert,
        // delete, MV refresh, statistics, etc.) can resolve active
        // dictionary snapshots without each entry point having to
        // thread the provider through its signature.
        let provider: std::sync::Arc<
            dyn crate::sql::optimizer::rewrite::context::QueryDictionaryProvider,
        > = std::sync::Arc::new(crate::engine::dictionary::DictionaryQueryProvider::new(
            self.inner.clone(),
        ));
        crate::sql::optimizer::rewrite::context::with_dictionary_provider(provider, || {
            self.execute_in_context_inner(sql, current_catalog, current_database, query_opts)
        })
    }

    fn execute_in_context_inner(
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
        if let Some(result) = self::statistics::try_handle_statement(
            &self.inner,
            &normalized,
            current_catalog,
            current_database,
        )? {
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

        // SHOW CREATE TABLE ...
        if looks_like_show_create_table(&normalized) {
            return self.handle_show_create_table(&normalized, current_catalog, current_database);
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
                let prepared =
                    prepare_explain_query(&self.inner, current_catalog, current_database, query)?;
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
                let result = explain_query(&prepared, &catalog, current_database, level)?;
                drop(catalog);
                Ok(StatementResult::Query(result))
            }
            sqlast::Statement::Explain {
                statement,
                analyze: true,
                ..
            } => {
                let sqlast::Statement::Query(ref query) = *statement else {
                    return Err("EXPLAIN ANALYZE only supports SELECT queries".to_string());
                };
                let prepared =
                    prepare_explain_query(&self.inner, current_catalog, current_database, query)?;
                let catalog = self
                    .inner
                    .catalog
                    .read()
                    .expect("standalone catalog read lock");
                let connectors_snapshot = self
                    .inner
                    .connectors
                    .read()
                    .expect("standalone connector registry read lock")
                    .clone();
                let result = explain_analyze_query(
                    &prepared,
                    &catalog,
                    &connectors_snapshot,
                    current_database,
                    self.inner.exchange_port,
                    None,
                )?;
                drop(catalog);
                Ok(StatementResult::Query(result))
            }
            sqlast::Statement::Query(ref query) => {
                if let Some(result) =
                    self::statistics::try_query(&self.inner, &normalized, query, current_database)?
                {
                    return Ok(StatementResult::Query(result));
                }
                if let Some(result) =
                    self::information_schema::try_query_materialized_views(&self.inner, query)?
                {
                    return Ok(result);
                }

                // Inline any user-defined views referenced in the query so the
                // remaining rewrites (time-travel, three-part-name, iceberg
                // registration) see only base tables. `expand_views_in_query`
                // is a no-op when no views are registered.
                let mut view_expanded = query.clone();
                self::view_rewrite::expand_views_in_query(
                    view_expanded.as_mut(),
                    &self.inner.views,
                    current_database,
                );
                // Materialize information_schema virtual tables (e.g. `schemata`)
                // into VALUES-backed derived tables. Run after view expansion
                // (a view may project from a virtual table) and before iceberg
                // registration / 3-part stripping so those passes never see
                // the synthetic references.
                self::virtual_table::rewrite_query(&self.inner, view_expanded.as_mut())?;
                let query = &view_expanded;

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
                    let connectors_snapshot = self
                        .inner
                        .connectors
                        .read()
                        .expect("standalone connector registry read lock")
                        .clone();
                    self::statistics::observe_query(&self.inner, &rewritten, current_database)?;
                    let result = execute_query(
                        &rewritten,
                        &catalog_snapshot,
                        &connectors_snapshot,
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
                    let connectors_snapshot = self
                        .inner
                        .connectors
                        .read()
                        .expect("standalone connector registry read lock")
                        .clone();
                    self::statistics::observe_query(&self.inner, &rewritten, current_database)?;
                    let result = execute_query(
                        &rewritten,
                        &catalog_snapshot,
                        &connectors_snapshot,
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
                let connectors_snapshot = self
                    .inner
                    .connectors
                    .read()
                    .expect("standalone connector registry read lock")
                    .clone();
                self::statistics::observe_query(&self.inner, query, current_database)?;
                let result = execute_query(
                    query,
                    &catalog_snapshot,
                    &connectors_snapshot,
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
            return Err(
                "ALTER TABLE ADD PARTITION only supports standalone StarRocks tables".into(),
            );
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
        let Some(provider) = self.inner.metadata_provider.as_ref() else {
            return Err("ALTER TABLE OPTIMIZE requires metadata provider".to_string());
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
        let mut txn = provider
            .begin_write("create iceberg optimize job")
            .map_err(|e| format!("open iceberg optimize job transaction failed: {e}"))?;
        self.inner
            .job_repo
            .create_iceberg_optimize_job(
                txn.as_mut(),
                CreateIcebergOptimizeJobRequest {
                    catalog: target.catalog,
                    namespace: target.namespace,
                    table: target.table,
                    base_snapshot_id,
                    now_ms: standalone_now_ms(),
                },
            )
            .map_err(|e| format!("create iceberg optimize job failed: {e}"))?;
        txn.commit()
            .map_err(|e| format!("commit iceberg optimize job failed: {e}"))?;
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
        let Some(provider) = self.inner.metadata_provider.as_ref() else {
            return Err("SHOW ALTER TABLE OPTIMIZE requires metadata provider".to_string());
        };
        let read = provider
            .begin_read()
            .map_err(|e| format!("open iceberg optimize job read transaction failed: {e}"))?;
        let mut jobs = self
            .inner
            .job_repo
            .show_iceberg_optimize_jobs(read.as_ref())
            .map_err(|e| format!("show iceberg optimize jobs failed: {e}"))?;
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

    fn handle_show_create_table(
        &self,
        sql: &str,
        current_catalog: Option<&str>,
        current_database: &str,
    ) -> Result<StatementResult, String> {
        let table_name = parse_show_create_table(sql)?;
        let target = crate::engine::backend_resolver::resolve_existing_table_target(
            &self.inner,
            &table_name,
            current_catalog,
            current_database,
        )?;
        if target.backend_name != "iceberg" {
            return Err(format!(
                "SHOW CREATE TABLE only supports Iceberg tables, got `{}` backend",
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
        let loaded = crate::connector::iceberg::catalog::registry::load_table(
            &entry,
            &target.namespace,
            &target.table,
        )?;
        let ddl = build_iceberg_create_table_ddl(
            &target.catalog,
            &target.namespace,
            &target.table,
            &loaded,
        )?;
        let fields = vec![
            Field::new("Table", DataType::Utf8, false),
            Field::new("Create Table", DataType::Utf8, false),
        ];
        let arrays: Vec<Arc<dyn arrow::array::Array>> = vec![
            Arc::new(StringArray::from(vec![target.table.clone()])),
            Arc::new(StringArray::from(vec![ddl])),
        ];
        let batch = RecordBatch::try_new(Arc::new(Schema::new(fields)), arrays)
            .map_err(|e| format!("build SHOW CREATE TABLE result failed: {e}"))?;
        Ok(StatementResult::Query(QueryResult {
            columns: vec![
                QueryResultColumn {
                    name: "Table".to_string(),
                    data_type: DataType::Utf8,
                    nullable: false,
                    logical_type: None,
                },
                QueryResultColumn {
                    name: "Create Table".to_string(),
                    data_type: DataType::Utf8,
                    nullable: false,
                    logical_type: None,
                },
            ],
            chunks: vec![record_batch_to_chunk(batch)?],
        }))
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
                    distribution_columns: Vec::new(),
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
    /// `execute_insert_statement` chooses between standalone table backends.
    fn handle_sqlparser_insert(
        &self,
        insert: &sqlparser::ast::Insert,
        current_catalog: Option<&str>,
        current_database: &str,
        query_opts: Option<&crate::internal_service::TQueryOptions>,
    ) -> Result<StatementResult, String> {
        self.execute_insert_via_custom_parser(insert, current_catalog, current_database, query_opts)
    }

    /// Convert sqlparser INSERT to our custom InsertStmt and delegate to the
    /// shared dispatcher in `execute_insert_statement`.
    fn execute_insert_via_custom_parser(
        &self,
        insert: &sqlparser::ast::Insert,
        current_catalog: Option<&str>,
        current_database: &str,
        query_opts: Option<&crate::internal_service::TQueryOptions>,
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
            query_opts,
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
    jobs: Vec<StoredIcebergOptimizeJob>,
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

fn iceberg_optimize_state_name(state: IcebergOptimizeJobState) -> &'static str {
    match state {
        IcebergOptimizeJobState::Pending => "PENDING",
        IcebergOptimizeJobState::Running => "RUNNING",
        IcebergOptimizeJobState::Finished => "FINISHED",
        IcebergOptimizeJobState::Failed => "FAILED",
    }
}

/// Generate a `CREATE TABLE` DDL string from a loaded Iceberg table's current
/// schema.  Column doc strings are emitted as `COMMENT '...'` clauses.
fn build_iceberg_create_table_ddl(
    catalog: &str,
    namespace: &str,
    table: &str,
    loaded: &crate::connector::iceberg::catalog::registry::IcebergLoadedTable,
) -> Result<String, String> {
    use iceberg::spec::{PrimitiveType, Type};

    fn iceberg_type_to_sql(ty: &Type) -> String {
        match ty {
            Type::Primitive(PrimitiveType::Boolean) => "BOOLEAN".to_string(),
            Type::Primitive(PrimitiveType::Int) => "INT".to_string(),
            Type::Primitive(PrimitiveType::Long) => "BIGINT".to_string(),
            Type::Primitive(PrimitiveType::Float) => "FLOAT".to_string(),
            Type::Primitive(PrimitiveType::Double) => "DOUBLE".to_string(),
            Type::Primitive(PrimitiveType::Decimal { precision, scale }) => {
                format!("DECIMAL({precision},{scale})")
            }
            Type::Primitive(PrimitiveType::Date) => "DATE".to_string(),
            Type::Primitive(PrimitiveType::Time) => "TIME".to_string(),
            Type::Primitive(PrimitiveType::Timestamp)
            | Type::Primitive(PrimitiveType::Timestamptz)
            | Type::Primitive(PrimitiveType::TimestampNs)
            | Type::Primitive(PrimitiveType::TimestamptzNs) => "DATETIME".to_string(),
            Type::Primitive(PrimitiveType::String) => "STRING".to_string(),
            Type::Primitive(PrimitiveType::Uuid) => "STRING".to_string(),
            Type::Primitive(PrimitiveType::Fixed(n)) => format!("BINARY({n})"),
            Type::Primitive(PrimitiveType::Binary) => "BINARY".to_string(),
            Type::Primitive(PrimitiveType::Variant) => "VARIANT".to_string(),
            Type::List(l) => format!(
                "ARRAY<{}>",
                iceberg_type_to_sql(&l.element_field.field_type)
            ),
            Type::Map(m) => format!(
                "MAP<{},{}>",
                iceberg_type_to_sql(&m.key_field.field_type),
                iceberg_type_to_sql(&m.value_field.field_type)
            ),
            Type::Struct(s) => {
                let fields: Vec<String> = s
                    .fields()
                    .iter()
                    .map(|f| format!("{} {}", f.name, iceberg_type_to_sql(&f.field_type)))
                    .collect();
                format!("STRUCT<{}>", fields.join(", "))
            }
        }
    }

    let schema = loaded.table.metadata().current_schema();
    let mut col_defs: Vec<String> = Vec::new();
    for field in schema.as_struct().fields() {
        let nullable = if field.required { " NOT NULL" } else { "" };
        let comment = if let Some(doc) = &field.doc {
            let escaped = doc.replace('\'', "\\'");
            format!(" COMMENT '{escaped}'")
        } else {
            String::new()
        };
        col_defs.push(format!(
            "  `{}` {}{}{}",
            field.name,
            iceberg_type_to_sql(&field.field_type),
            nullable,
            comment
        ));
    }

    // Emit table-level COMMENT if the "comment" property is set and non-empty.
    let table_comment = loaded
        .table
        .metadata()
        .properties()
        .get("comment")
        .filter(|v| !v.is_empty())
        .map(|v| {
            let escaped = v.replace('\'', "\\'");
            format!("\nCOMMENT '{escaped}'")
        })
        .unwrap_or_default();

    Ok(format!(
        "CREATE TABLE `{catalog}`.`{namespace}`.`{table}` (\n{}\n){table_comment}",
        col_defs.join(",\n")
    ))
}

// ---------------------------------------------------------------------------
// Custom statement dispatch
// ---------------------------------------------------------------------------

pub(crate) mod delete_flow;
pub(crate) mod delete_predicate_translate;
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
        Statement::AlterMaterializedView(stmt) => {
            crate::engine::mv_flow::alter_mv(state, current_catalog, current_database, &stmt)
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

#[derive(Clone, Debug, PartialEq, Eq)]
struct ResolvedMetadataBackend {
    provider: crate::common::app_config::MetadataProviderConfig,
    path: PathBuf,
}

fn resolve_metadata_backend(
    opts: &StandaloneOptions,
) -> Result<Option<ResolvedMetadataBackend>, String> {
    let cfg = novarocks_config::config().map_err(|e| format!("read config failed: {e}"))?;
    if let Some(metadata) = cfg.metadata.as_ref() {
        return Ok(Some(ResolvedMetadataBackend {
            provider: metadata.provider,
            path: resolve_relative_path(&metadata.path, opts.config_path.as_deref())?,
        }));
    }
    Ok(None)
}

fn open_metadata_provider(
    backend: &ResolvedMetadataBackend,
) -> Result<Arc<dyn crate::meta::MetaStoreProvider>, String> {
    match backend.provider {
        crate::common::app_config::MetadataProviderConfig::Sqlite => {
            let provider = crate::meta::SqliteMetaStoreProvider::open(&backend.path)
                .map_err(|err| format!("open sqlite metadata provider failed: {err}"))?;
            Ok(Arc::new(provider))
        }
    }
}

fn resolve_starrocks_table_config() -> Result<Option<StarRocksTableConfig>, String> {
    let cfg = novarocks_config::config().map_err(|e| format!("read config failed: {e}"))?;
    let Some(standalone) = cfg.standalone_server.as_ref() else {
        return Ok(None);
    };
    let app_cfg = standalone.starrocks_table_config()?;
    app_cfg
        .map(StarRocksTableConfig::from_app_config)
        .transpose()
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
    restore_starrocks_table(state)?;
    restore_iceberg_catalogs(state)?;
    crate::engine::mv::iceberg_refresh::recover_iceberg_mv_refreshes(state)?;
    crate::engine::mv::iceberg_refresh::restore_iceberg_mv_targets(state)?;
    Ok(())
}

fn restore_iceberg_catalogs(state: &Arc<StandaloneState>) -> Result<(), String> {
    let Some(provider) = state.metadata_provider.as_ref() else {
        return Ok(());
    };
    let read = provider
        .begin_read()
        .map_err(|e| format!("open metadata read transaction failed: {e}"))?;
    let catalogs = state
        .iceberg_catalog_repo
        .list_catalogs(read.as_ref())
        .map_err(|e| format!("load iceberg catalog metadata failed: {e}"))?;
    let namespaces = state
        .iceberg_catalog_repo
        .list_namespaces(read.as_ref())
        .map_err(|e| format!("load iceberg namespace metadata failed: {e}"))?;
    let tables = state
        .iceberg_catalog_repo
        .list_tables(read.as_ref())
        .map_err(|e| format!("load iceberg table metadata failed: {e}"))?;

    {
        let mut guard = state
            .iceberg_catalogs
            .write()
            .expect("standalone iceberg catalog write lock");
        for catalog in &catalogs {
            guard.create_catalog(&catalog.catalog, &catalog.properties.properties)?;
        }
    }

    let guard = state
        .iceberg_catalogs
        .read()
        .expect("standalone iceberg catalog read lock");
    for namespace in &namespaces {
        let entry = guard.get(&namespace.catalog)?;
        if !iceberg_namespace_exists(&entry, &namespace.namespace)? {
            create_iceberg_namespace(&entry, &namespace.namespace)?;
        }
    }
    for table in &tables {
        let entry = guard.get(&table.catalog)?;
        register_existing_iceberg_table(&entry, &table.namespace, &table.table)?;
    }
    Ok(())
}

fn restore_starrocks_table(state: &Arc<StandaloneState>) -> Result<(), String> {
    let Some(provider) = state.metadata_provider.as_ref() else {
        return Ok(());
    };
    reconcile_starrocks_table_on_open_from_repositories(state)?;
    let read = provider
        .begin_read()
        .map_err(|e| format!("open StarRocks table metadata read transaction failed: {e}"))?;
    let starrocks_table_snapshot = state
        .starrocks_table_repo
        .load_snapshot(read.as_ref())
        .map_err(|e| format!("load StarRocks table metadata failed: {e}"))?;
    let rebuilt = StarRocksTableCatalog::rebuild_from_repository(
        state.starrocks_table_config.clone(),
        starrocks_table_snapshot.clone(),
    )?;
    {
        let mut catalog = state
            .catalog
            .write()
            .expect("standalone catalog write lock");
        for database in &starrocks_table_snapshot.databases {
            catalog.create_database(&database.name)?;
        }
        register_starrocks_tables_in_catalog(&mut catalog, &rebuilt)?;
    }
    rebuilt.re_register_active_tablet_runtimes()?;
    let mut guard = state
        .starrocks_table
        .write()
        .expect("standalone StarRocks table write lock");
    *guard = rebuilt;
    Ok(())
}

fn reconcile_starrocks_table_on_open_from_repositories(
    state: &Arc<StandaloneState>,
) -> Result<(), String> {
    let Some(provider) = state.metadata_provider.as_ref() else {
        return Ok(());
    };
    {
        let mut txn = provider
            .begin_write("reconcile StarRocks table open metadata")
            .map_err(|e| format!("open StarRocks table reconcile write transaction failed: {e}"))?;
        state
            .starrocks_table_repo
            .fail_creating_tables(txn.as_mut())
            .map_err(|e| format!("fail creating StarRocks tables during open failed: {e}"))?;
        state
            .starrocks_table_repo
            .delete_all_creating_partitions(txn.as_mut())
            .map_err(|e| format!("delete creating StarRocks partitions during open failed: {e}"))?;
        txn.commit()
            .map_err(|e| format!("commit StarRocks table open reconciliation failed: {e}"))?;
    }

    let read = provider
        .begin_read()
        .map_err(|e| format!("open StarRocks table txn read transaction failed: {e}"))?;
    let txns = state
        .starrocks_txn_repo
        .list_all(read.as_ref())
        .map_err(|e| format!("load StarRocks table txns during open failed: {e}"))?;
    drop(read);

    for starrocks_txn in txns {
        match starrocks_txn.state {
            crate::meta::repository::starrocks_txn::StarRocksTxnState::Prepared => {
                let mut write = provider
                    .begin_write("abort prepared StarRocks table txn on open")
                    .map_err(|e| {
                        format!("open StarRocks txn abort write transaction failed: {e}")
                    })?;
                state
                    .starrocks_txn_repo
                    .mark_aborted(write.as_mut(), starrocks_txn.txn_id)
                    .map_err(|e| {
                        format!(
                            "abort prepared StarRocks txn {} during open failed: {e}",
                            starrocks_txn.txn_id
                        )
                    })?;
                write
                    .commit()
                    .map_err(|e| format!("commit StarRocks txn abort failed: {e}"))?;
            }
            crate::meta::repository::starrocks_txn::StarRocksTxnState::Written => {
                let read = provider.begin_read().map_err(|e| {
                    format!("open StarRocks table replay read transaction failed: {e}")
                })?;
                let snapshot = state
                    .starrocks_table_repo
                    .load_snapshot(read.as_ref())
                    .map_err(|e| format!("load StarRocks table replay snapshot failed: {e}"))?;
                let tablet_ids = snapshot
                    .tablets
                    .iter()
                    .filter(|tablet| {
                        snapshot.indexes.iter().any(|index| {
                            index.index_id == tablet.index_id
                                && index.table_id == starrocks_txn.table_id
                                && index.partition_id == starrocks_txn.partition_id
                        })
                    })
                    .map(|tablet| tablet.tablet_id)
                    .collect::<Vec<_>>();
                drop(read);
                crate::connector::publish_tablets_at_version(
                    tablet_ids,
                    starrocks_txn.txn_id,
                    starrocks_txn.base_version,
                    starrocks_txn.commit_version,
                )?;
                let mut write = provider
                    .begin_write("mark replayed StarRocks table txn visible")
                    .map_err(|e| {
                        format!("open StarRocks txn visible write transaction failed: {e}")
                    })?;
                state
                    .starrocks_txn_repo
                    .mark_visible(
                        &state.starrocks_table_repo,
                        write.as_mut(),
                        starrocks_txn.txn_id,
                    )
                    .map_err(|e| {
                        format!(
                            "mark replayed StarRocks txn {} visible during open failed: {e}",
                            starrocks_txn.txn_id
                        )
                    })?;
                write
                    .commit()
                    .map_err(|e| format!("commit replayed StarRocks txn visible failed: {e}"))?;
            }
            crate::meta::repository::starrocks_txn::StarRocksTxnState::Visible
            | crate::meta::repository::starrocks_txn::StarRocksTxnState::Aborted => {}
        }
    }
    Ok(())
}

pub(crate) fn persist_iceberg_catalog_if_needed(
    state: &Arc<StandaloneState>,
    catalog_name: &str,
    properties: &[(String, String)],
) -> Result<(), String> {
    let Some(provider) = state.metadata_provider.as_ref() else {
        return Ok(());
    };
    let mut txn = provider
        .begin_write("persist iceberg catalog")
        .map_err(|e| format!("open metadata write transaction failed: {e}"))?;
    state
        .iceberg_catalog_repo
        .upsert_catalog(
            txn.as_mut(),
            catalog_name,
            IcebergCatalogProperties {
                properties: properties.to_vec(),
            },
        )
        .map_err(|e| format!("persist iceberg catalog metadata failed: {e}"))?;
    txn.commit()
        .map_err(|e| format!("commit iceberg catalog metadata failed: {e}"))?;
    Ok(())
}

pub(crate) fn persist_iceberg_namespace_if_needed(
    state: &Arc<StandaloneState>,
    catalog_name: &str,
    namespace_name: &str,
) -> Result<(), String> {
    let Some(provider) = state.metadata_provider.as_ref() else {
        return Ok(());
    };
    let mut txn = provider
        .begin_write("persist iceberg namespace")
        .map_err(|e| format!("open metadata write transaction failed: {e}"))?;
    state
        .iceberg_catalog_repo
        .upsert_namespace(txn.as_mut(), catalog_name, namespace_name)
        .map_err(|e| format!("persist iceberg namespace metadata failed: {e}"))?;
    txn.commit()
        .map_err(|e| format!("commit iceberg namespace metadata failed: {e}"))?;
    Ok(())
}

pub(crate) fn persist_iceberg_table_if_needed(
    state: &Arc<StandaloneState>,
    catalog_name: &str,
    namespace_name: &str,
    table_name: &str,
) -> Result<(), String> {
    let Some(provider) = state.metadata_provider.as_ref() else {
        return Ok(());
    };
    let mut txn = provider
        .begin_write("persist iceberg table")
        .map_err(|e| format!("open metadata write transaction failed: {e}"))?;
    state
        .iceberg_catalog_repo
        .upsert_namespace(txn.as_mut(), catalog_name, namespace_name)
        .map_err(|e| format!("persist iceberg namespace metadata failed: {e}"))?;
    state
        .iceberg_catalog_repo
        .upsert_table(txn.as_mut(), catalog_name, namespace_name, table_name)
        .map_err(|e| format!("persist iceberg table metadata failed: {e}"))?;
    txn.commit()
        .map_err(|e| format!("commit iceberg table metadata failed: {e}"))?;
    Ok(())
}

pub(crate) fn delete_iceberg_table_if_needed(
    state: &Arc<StandaloneState>,
    catalog_name: &str,
    namespace_name: &str,
    table_name: &str,
) -> Result<(), String> {
    let Some(provider) = state.metadata_provider.as_ref() else {
        return Ok(());
    };
    let mut txn = provider
        .begin_write("delete iceberg table")
        .map_err(|e| format!("open metadata write transaction failed: {e}"))?;
    state
        .iceberg_catalog_repo
        .delete_table_and_mv_relationships(
            txn.as_mut(),
            &state.mv_repo,
            catalog_name,
            namespace_name,
            table_name,
        )
        .map_err(|e| format!("delete iceberg table metadata failed: {e}"))?;
    txn.commit()
        .map_err(|e| format!("commit iceberg table metadata failed: {e}"))?;
    Ok(())
}

pub(crate) fn delete_iceberg_namespace_if_needed(
    state: &Arc<StandaloneState>,
    catalog_name: &str,
    namespace_name: &str,
) -> Result<(), String> {
    let Some(provider) = state.metadata_provider.as_ref() else {
        return Ok(());
    };
    let mut txn = provider
        .begin_write("delete iceberg namespace")
        .map_err(|e| format!("open metadata write transaction failed: {e}"))?;
    state
        .iceberg_catalog_repo
        .delete_namespace_and_mv_relationships(
            txn.as_mut(),
            &state.mv_repo,
            catalog_name,
            namespace_name,
        )
        .map_err(|e| format!("delete iceberg namespace metadata failed: {e}"))?;
    txn.commit()
        .map_err(|e| format!("commit iceberg namespace metadata failed: {e}"))?;
    Ok(())
}

pub(crate) fn delete_iceberg_catalog_if_needed(
    state: &Arc<StandaloneState>,
    catalog_name: &str,
) -> Result<(), String> {
    let Some(provider) = state.metadata_provider.as_ref() else {
        return Ok(());
    };
    let mut txn = provider
        .begin_write("delete iceberg catalog")
        .map_err(|e| format!("open metadata write transaction failed: {e}"))?;
    state
        .iceberg_catalog_repo
        .delete_catalog_and_mv_relationships(txn.as_mut(), &state.mv_repo, catalog_name)
        .map_err(|e| format!("delete iceberg catalog metadata failed: {e}"))?;
    txn.commit()
        .map_err(|e| format!("commit iceberg catalog metadata failed: {e}"))?;
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
    let root_exchange = &root.plan.nodes[0];
    if root_exchange.limit >= 0 {
        return None;
    }
    if let Some(exchange) = root_exchange.exchange_node.as_ref()
        && (exchange.sort_info.is_some() || exchange.offset.unwrap_or(0) > 0)
    {
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
    let Some(root_fragment) = build_result
        .fragment_results
        .iter()
        .find(|fragment| fragment.fragment_id == old_root_id)
    else {
        return build_result;
    };
    let root_node = &root_fragment.plan.nodes[0];
    let root_limit = root_node.limit;
    let root_offset = root_node
        .exchange_node
        .as_ref()
        .and_then(|exchange| exchange.offset)
        .unwrap_or(0);
    if root_offset > 0 {
        return build_result;
    }
    let root_output_sink = root_fragment.output_sink.clone();
    let root_output_columns = root_fragment.output_columns.clone();

    build_result
        .fragment_results
        .retain(|fragment| fragment.fragment_id != old_root_id);
    build_result.edges.retain(|edge| {
        !(edge.source_fragment_id == child_id
            && edge.target_fragment_id == old_root_id
            && matches!(edge.edge_kind, FragmentEdgeKind::Stream))
    });
    build_result.root_fragment_id = child_id;
    if root_limit >= 0
        && let Some(child) = build_result
            .fragment_results
            .iter_mut()
            .find(|fragment| fragment.fragment_id == child_id)
        && let Some(child_root) = child.plan.nodes.first_mut()
    {
        child_root.limit = if child_root.limit >= 0 {
            child_root.limit.min(root_limit)
        } else {
            root_limit
        };
    }
    if let Some(child) = build_result
        .fragment_results
        .iter_mut()
        .find(|fragment| fragment.fragment_id == child_id)
    {
        child.output_sink = root_output_sink;
        child.output_columns = root_output_columns;
    }
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
        direct_exec: fragment.direct_exec,
        query_global_dicts: fragment.query_global_dicts,
        query_global_dict_exprs: fragment.query_global_dict_exprs,
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

fn collapse_distribution_enforcers_for_single_fragment(
    mut node: crate::sql::optimizer::PhysicalPlanNode,
) -> crate::sql::optimizer::PhysicalPlanNode {
    use crate::sql::optimizer::operator::{JoinDistribution, Operator};

    node.children = node
        .children
        .into_iter()
        .map(collapse_distribution_enforcers_for_single_fragment)
        .collect();

    if let Operator::PhysicalHashJoin(join) = &mut node.op {
        join.distribution = JoinDistribution::Broadcast;
    }

    if matches!(&node.op, Operator::PhysicalDistribution(_)) && node.children.len() == 1 {
        return node.children.into_iter().next().expect("single child");
    }

    node
}

/// Common preparation pipeline shared by `EXPLAIN` and `EXPLAIN ANALYZE`:
/// inline user-defined views, rewrite time-travel refs, register Iceberg
/// tables, and strip three-part catalog names. Returns the rewritten query.
fn prepare_explain_query(
    state: &Arc<StandaloneState>,
    current_catalog: Option<&str>,
    current_database: &str,
    query: &sqlparser::ast::Query,
) -> Result<sqlparser::ast::Query, String> {
    // Inline any user-defined views before the analyzer sees the query.
    let mut prepared = query.clone();
    self::view_rewrite::expand_views_in_query(&mut prepared, &state.views, current_database);

    // Time-travel: rewrite version clauses before Iceberg registration.
    if has_time_travel_refs(&prepared) {
        rewrite_time_travel_refs(state, current_catalog, current_database, &mut prepared)?;
    }

    // When current_catalog is an Iceberg catalog, materialize referenced
    // Iceberg tables into the local catalog first.
    if current_catalog.is_some() {
        register_iceberg_tables_for_query(state, current_catalog, current_database, &prepared)?;
    }

    // Three-part catalog.database.table names: register and strip.
    let three_parts = extract_three_part_table_refs(&prepared);
    if !three_parts.is_empty() {
        if current_catalog.is_none() {
            register_iceberg_tables_for_query(state, None, current_database, &prepared)?;
        }
        strip_catalog_from_three_part_names(&mut prepared);
    }

    Ok(prepared)
}

/// Execute the query, then produce an EXPLAIN-style result whose first row is
/// `Planning: <ms> / Execution: <ms> / Rows: <N>` followed by the Verbose
/// plan body. Per-operator runtime stats merge is out of scope for OPT-5;
/// the pipeline has no systematic profile collection yet.
fn explain_analyze_query(
    query: &sqlparser::ast::Query,
    catalog: &InMemoryCatalog,
    connectors: &crate::connector::ConnectorRegistry,
    current_database: &str,
    exchange_port: u16,
    query_opts: Option<crate::internal_service::TQueryOptions>,
) -> Result<QueryResult, String> {
    use crate::sql::explain::{ExplainLevel, explain_physical_plan};

    // NOTE: planning_ms covers only the outer analyze + plan_query +
    // optimize call below; execute_query re-plans internally and its
    // planning work is charged to execution_ms. This double-count is
    // an acknowledged limitation; per-operator profile merge in a
    // follow-up PR will replace the query-level timing summary.
    let t_plan = Instant::now();
    let (resolved, cte_registry, mut factory) =
        crate::sql::analyzer::analyze(query, catalog, current_database)?;
    let logical = crate::sql::planner::plan_query(resolved, cte_registry, &mut factory)?;
    let table_stats = build_table_stats_from_plan(&logical);
    // dictionary_provider intentionally None; installed via TLS by execute_in_context.
    let physical = crate::sql::optimizer::optimize(logical, &table_stats, factory, None)?;
    let planning_ms = t_plan.elapsed().as_millis() as u64;

    let t_exec = Instant::now();
    let executed = execute_query(
        query,
        catalog,
        connectors,
        current_database,
        exchange_port,
        query_opts,
    )?;
    let rows: u64 = executed.chunks.iter().map(|c| c.len() as u64).sum();
    let execution_ms = t_exec.elapsed().as_millis() as u64;

    let mut lines = Vec::new();
    lines.push(format!(
        "Planning: {planning_ms} ms / Execution: {execution_ms} ms / Rows: {rows}"
    ));
    lines.extend(explain_physical_plan(&physical, ExplainLevel::Analyze));

    build_string_query_result("Explain String", lines)
}

/// Produce EXPLAIN output for a query without executing it.
fn explain_query(
    query: &sqlparser::ast::Query,
    catalog: &InMemoryCatalog,
    current_database: &str,
    level: crate::sql::explain::ExplainLevel,
) -> Result<QueryResult, String> {
    use crate::sql::explain::{ExplainLevel, explain_physical_plan};

    let (resolved, cte_registry, mut factory) =
        crate::sql::analyzer::analyze(query, catalog, current_database)?;
    let logical = crate::sql::planner::plan_query(resolved, cte_registry, &mut factory)?;
    let table_stats = build_table_stats_from_plan(&logical);
    // dictionary_provider intentionally None; installed via TLS by execute_in_context.
    let physical = crate::sql::optimizer::optimize(logical, &table_stats, factory, None)?;

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
    connectors: &crate::connector::ConnectorRegistry,
    current_database: &str,
    exchange_port: u16,
    query_opts: Option<crate::internal_service::TQueryOptions>,
) -> Result<QueryResult, String> {
    execute_query_with_options(
        query,
        catalog,
        connectors,
        current_database,
        exchange_port,
        query_opts,
        None,
        None,
        None,
    )
}

/// Extended `execute_query` entry that accepts an optional custom terminal
/// sink factory and an optional Iceberg catalog registry. Used by IVM-A1
/// refresh paths: the merge sink intercepts pipeline output (no result
/// rows are produced), and lower_plan needs the registry to resolve
/// `ICEBERG_DELTA_SCAN_NODE` runtime handles.
///
/// `terminal_sink = None` falls back to the default `ResultSinkFactory`.
/// `iceberg_catalogs = None` matches the legacy behaviour for non-IVM
/// callers.
/// `mv_refresh_ctx = Some(ctx)` runs the IMV rewrite pipeline on the
/// logical plan before optimization. Callers that do not need IMV rewriting
/// pass `None` (dormant until Task 9 flips the PF refresh caller).
pub(crate) fn execute_query_with_options(
    query: &sqlparser::ast::Query,
    catalog: &InMemoryCatalog,
    connectors: &crate::connector::ConnectorRegistry,
    current_database: &str,
    exchange_port: u16,
    query_opts: Option<crate::internal_service::TQueryOptions>,
    terminal_sink: Option<Box<dyn crate::exec::pipeline::operator_factory::OperatorFactory>>,
    iceberg_catalogs: Option<&crate::connector::iceberg::catalog::IcebergCatalogRegistry>,
    mv_refresh_ctx: Option<&crate::engine::mv::refresh_context::IcebergMvRefreshContext>,
) -> Result<QueryResult, String> {
    let (resolved, cte_registry, mut factory) =
        crate::sql::analyzer::analyze(query, catalog, current_database)?;
    let mut logical = crate::sql::planner::plan_query(resolved, cte_registry, &mut factory)?;
    if let Some(mv_ctx) = mv_refresh_ctx {
        let outcome = crate::sql::optimizer::rewrite::imv::entrypoint::run_imv_rewrite(
            crate::sql::optimizer::rewrite::imv::entrypoint::ImvRewriteInput {
                plan: logical,
                disabled_rules: crate::sql::optimizer::options::current_session_optimizer_settings(
                )
                .disabled_rules
                .clone(),
                mv_ctx: std::sync::Arc::clone(&mv_ctx.rewrite),
                deadline: None,
                next_column_id: factory.peek_next_id(),
            },
        )
        .map_err(|e| format!("imv rewrite: {e}"))?;
        logical = outcome.plan;
    }
    let table_stats = build_table_stats_from_plan(&logical);
    // dictionary_provider intentionally None; installed via TLS by execute_in_context.
    let mut physical = crate::sql::optimizer::optimize(logical, &table_stats, factory, None)?;
    // Unit-test states may not start the standalone exchange server. IVM-A1
    // internal queries also pass runtime-local handles (`terminal_sink` or
    // `iceberg_catalogs`) that coordinated fragments cannot currently clone
    // into remote fragment execution. Collapse distribution nodes before
    // fragment building so those refresh queries stay local.
    let force_single_fragment =
        terminal_sink.is_some() || iceberg_catalogs.is_some() || exchange_port == 0;
    if force_single_fragment {
        physical = collapse_distribution_enforcers_for_single_fragment(physical);
    }
    let build_result =
        crate::sql::codegen::fragment_builder::PlanFragmentBuilder::build_with_mv_refresh_ctx(
            &physical,
            catalog,
            connectors,
            current_database,
            mv_refresh_ctx,
        )?;

    let execution_plan = choose_standalone_execution(build_result);

    match execution_plan {
        StandaloneExecutionPlan::SingleFragment(plan) => {
            execute_plan(*plan, query_opts, terminal_sink, iceberg_catalogs)
        }
        StandaloneExecutionPlan::Coordinated(build_result) => {
            if terminal_sink.is_some() {
                return Err(
                    "IVM-A1 custom sink does not yet support coordinated multi-fragment plans"
                        .to_string(),
                );
            }
            if iceberg_catalogs.is_some() {
                return Err(
                    "IVM-A1 iceberg_catalogs runtime registry does not yet support coordinated \
                     multi-fragment plans"
                        .to_string(),
                );
            }
            let role = crate::novarocks_config::config()
                .map(|c| c.cluster.role)
                .unwrap_or(crate::common::app_config::ClusterRole::AllInOne);
            let dispatcher = dispatcher_for_role(role, "127.0.0.1", exchange_port)?;
            crate::runtime::coordinator::ExecutionCoordinator::new(
                *build_result,
                dispatcher,
                query_opts,
            )
            .execute()
        }
    }
}

/// Select a `FragmentDispatcher` implementation based on the effective cluster role.
///
/// - `AllInOne`: uses `InProcessDispatcher` bound to the local exchange endpoint.
/// - `Fe`: uses `RemoteDispatcher` bound to the first configured backend.
/// - `Be`: standalone coordinator must not be entered when the process is a pure BE.
pub(crate) fn dispatcher_for_role(
    role: crate::common::app_config::ClusterRole,
    exchange_host: &str,
    exchange_port: u16,
) -> Result<Arc<dyn crate::runtime::dispatcher::FragmentDispatcher>, String> {
    use crate::common::app_config::ClusterRole;
    match role {
        ClusterRole::AllInOne => Ok(Arc::new(
            crate::runtime::dispatcher::InProcessDispatcher::new(exchange_host, exchange_port),
        )),
        ClusterRole::Fe => {
            let cfg = crate::novarocks_config::config()
                .map_err(|e| format!("role=fe: cannot read config: {e}"))?;
            let n = cfg.cluster.backends.len();
            if n != 1 {
                return Err(format!(
                    "role=fe: expected exactly one backend, got {n} in cluster.backends"
                ));
            }
            let backend_str = cfg
                .cluster
                .backends
                .first()
                .expect("length already checked above");
            let backend: std::net::SocketAddr = backend_str
                .parse()
                .map_err(|e| format!("role=fe: invalid backend addr '{backend_str}': {e}"))?;
            Ok(Arc::new(crate::runtime::dispatcher::RemoteDispatcher::new(
                backend,
            )))
        }
        ClusterRole::Be => Err("role=be must not enter standalone coordinator".to_string()),
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
/// nodes that reference IcebergDataFiles storage.
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
            if let crate::sql::catalog::ScanSource::IcebergDataFiles {
                table,
                files,
                cloud_properties,
            } = &s.table.source
            {
                // Best-effort: pull NDV from registered Puffin statistics for
                // the table's current snapshot. Any failure quietly degrades
                // to manifest heuristics (see StatsLoader contract).
                let (ndv_by_name, name_to_field_id) =
                    load_iceberg_puffin_ndv(Some(table), cloud_properties);
                if let Some(ts) = crate::sql::optimizer::statistics::build_table_statistics_with_ndv(
                    files,
                    &s.table.columns,
                    &ndv_by_name,
                    &name_to_field_id,
                ) {
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
        LogicalPlan::TableFunction(n) => collect_scan_stats(&n.input, out),
        LogicalPlan::CTEAnchor(n) => {
            collect_scan_stats(&n.produce, out);
            collect_scan_stats(&n.consumer, out);
        }
        LogicalPlan::CTEProduce(n) => collect_scan_stats(&n.input, out),
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
        LogicalPlan::Decode(n) => collect_scan_stats(&n.input, out),
        LogicalPlan::AggregateStateMerge(n) => {
            collect_scan_stats(&n.old_input, out);
            collect_scan_stats(&n.delta_input, out);
        }
        LogicalPlan::Values(_) | LogicalPlan::GenerateSeries(_) | LogicalPlan::CTEConsume(_) => {}
        LogicalPlan::ImvDelta(_) | LogicalPlan::ImvVersion(_) => {
            panic!("imv marker leaked into non-IMV plan");
        }
    }
}

/// Best-effort load of Iceberg Puffin NDV statistics for a scan target.
///
/// Returns `(ndv_by_name, name_to_field_id)`. Both maps are keyed by the
/// lowercased column name. The second map is currently unused by callers
/// (NDV is keyed by name to match the column lookup) but is returned so
/// future schema-evolution-aware paths can use it without changing the
/// function signature.
///
/// Any failure (no Iceberg metadata, no current snapshot, no statistics
/// entry, Puffin parse error) yields a pair of empty maps so the optimizer
/// falls back to manifest-based heuristics — never blocking query planning.
fn load_iceberg_puffin_ndv(
    iceberg_table: Option<&crate::sql::catalog::IcebergTableInfo>,
    cloud_properties: &std::collections::BTreeMap<String, String>,
) -> (
    std::collections::HashMap<String, f64>,
    std::collections::HashMap<String, i32>,
) {
    use crate::connector::iceberg::stats_loader::StatsLoader;
    use crate::runtime::global_async_runtime::data_block_on;

    let empty = (
        std::collections::HashMap::new(),
        std::collections::HashMap::new(),
    );

    let Some(info) = iceberg_table else {
        return empty;
    };
    let Some(serialized) = info.serialized_metadata.as_ref() else {
        return empty;
    };

    let metadata: iceberg::spec::TableMetadata = match serde_json::from_str(serialized) {
        Ok(m) => m,
        Err(err) => {
            tracing::debug!(error = %err, "iceberg ndv: parse table metadata json failed");
            return empty;
        }
    };
    let Some(snapshot) = metadata.current_snapshot() else {
        return empty;
    };
    if metadata
        .statistics_for_snapshot(snapshot.snapshot_id())
        .is_none()
    {
        return empty;
    }

    // Build name → field_id map from the iceberg schema definition.
    let mut name_to_field_id: std::collections::HashMap<String, i32> =
        std::collections::HashMap::new();
    for field in &info.schema.fields {
        name_to_field_id.insert(field.name.to_lowercase(), field.field_id);
    }

    // Build FileIO matching the iceberg location scheme. For S3 / OSS paths
    // we honor the cloud properties; otherwise default to the local FS.
    let file_io = match build_stats_file_io(&info.location, cloud_properties) {
        Ok(io) => io,
        Err(err) => {
            tracing::debug!(error = %err, "iceberg ndv: build FileIO failed");
            return empty;
        }
    };

    let ndv_by_field_id = match data_block_on(StatsLoader::load_ndv(
        &metadata,
        snapshot.snapshot_id(),
        &file_io,
    )) {
        Ok(map) => map,
        Err(err) => {
            tracing::debug!(error = %err, "iceberg ndv: block_on StatsLoader::load_ndv failed");
            return empty;
        }
    };

    // Translate field_id → name using the schema map. Lowercased name keys
    // match the optimizer's column lookup convention.
    let mut field_id_to_name: std::collections::HashMap<i32, String> =
        std::collections::HashMap::new();
    for (name, fid) in &name_to_field_id {
        field_id_to_name.insert(*fid, name.clone());
    }
    let mut ndv_by_name: std::collections::HashMap<String, f64> = std::collections::HashMap::new();
    for (field_id, ndv) in ndv_by_field_id {
        if let Some(name) = field_id_to_name.get(&field_id) {
            ndv_by_name.insert(name.clone(), ndv);
        }
    }
    (ndv_by_name, name_to_field_id)
}

/// Build a `FileIO` capable of reading the table's Puffin statistics. For
/// `file://` and bare-path locations we return the local-FS variant; for
/// `s3://`/`s3a://`/`oss://` paths we honour the catalog's cloud_properties
/// when present. When required properties are missing the call gracefully
/// fails so the optimizer falls back to manifest heuristics.
fn build_stats_file_io(
    location: &str,
    cloud_properties: &std::collections::BTreeMap<String, String>,
) -> Result<iceberg::io::FileIO, String> {
    let scheme = location.split("://").next().unwrap_or("");
    let is_s3 = matches!(scheme, "s3" | "s3a" | "oss");
    if !is_s3 {
        return Ok(iceberg::io::FileIO::new_with_fs());
    }

    // Reuse the same property-name conventions as the catalog code path.
    let endpoint = cloud_properties
        .get("aws.s3.endpoint")
        .ok_or_else(|| "missing aws.s3.endpoint".to_string())?;
    let access_key = cloud_properties
        .get("aws.s3.access_key")
        .ok_or_else(|| "missing aws.s3.access_key".to_string())?;
    let secret_key = cloud_properties
        .get("aws.s3.secret_key")
        .ok_or_else(|| "missing aws.s3.secret_key".to_string())?;
    let region = cloud_properties
        .get("aws.s3.region")
        .cloned()
        .unwrap_or_else(|| "us-east-1".to_string());
    let path_style = cloud_properties
        .get("aws.s3.enable_path_style_access")
        .map(|v| v.eq_ignore_ascii_case("true"))
        .unwrap_or(false);

    use std::sync::Arc;
    let factory = crate::connector::iceberg::catalog::s3_storage::S3StorageFactory {
        endpoint: endpoint.clone(),
        access_key_id: access_key.clone(),
        access_key_secret: secret_key.clone(),
        region,
        enable_path_style: path_style,
    };
    Ok(iceberg::io::FileIOBuilder::new(Arc::new(factory)).build())
}

fn lower_plan_build_result(
    result: PlanBuildResult,
    arena: &mut crate::exec::expr::ExprArena,
    query_opts: Option<&crate::internal_service::TQueryOptions>,
    iceberg_catalogs: Option<&crate::connector::iceberg::catalog::IcebergCatalogRegistry>,
) -> Result<crate::exec::node::ExecNode, String> {
    use crate::lower::thrift::layout::{build_tuple_slot_order, reorder_tuple_slots};
    use crate::lower::thrift::lower_plan;

    if let Some(direct) = result.direct_exec {
        match *direct {
            crate::sql::codegen::DirectExecPlan::AggregateStateMerge {
                old_input,
                delta_input,
                layout,
            } => {
                let old_input =
                    lower_plan_build_result(*old_input, arena, query_opts, iceberg_catalogs)?;
                let delta_input =
                    lower_plan_build_result(*delta_input, arena, query_opts, iceberg_catalogs)?;
                return Ok(
                    crate::sql::codegen::nodes::build_aggregate_state_merge_exec_node(
                        old_input,
                        delta_input,
                        layout,
                    ),
                );
            }
            crate::sql::codegen::DirectExecPlan::AggregateStatePhysicalize {
                input,
                layout,
                shape,
            } => {
                let input = lower_plan_build_result(*input, arena, query_opts, iceberg_catalogs)?;
                return Ok(crate::exec::node::ExecNode {
                    kind: crate::exec::node::ExecNodeKind::AggregateStatePhysicalize(
                        crate::exec::operators::aggregate_state_merge::AggregateStatePhysicalizePlan {
                            input: Box::new(input),
                            layout,
                            shape,
                        },
                    ),
                });
            }
        }
    }

    let desc_tbl = result.desc_tbl;
    let plan = result.plan;
    let exec_params = result.exec_params;
    let query_global_dicts = result.query_global_dicts;
    let query_global_dict_exprs = result.query_global_dict_exprs;

    let mut tuple_slots = build_tuple_slot_order(Some(&desc_tbl));
    reorder_tuple_slots(&mut tuple_slots, Some(&desc_tbl));
    let layout_hints = tuple_slots.clone();

    let connectors = crate::connector::ConnectorRegistry::default();
    let lowered = lower_plan(
        &plan,
        arena,
        &tuple_slots,
        Some(&desc_tbl),
        query_global_dicts.as_deref(),
        query_global_dict_exprs.as_ref(),
        Some(&exec_params),
        query_opts,
        None,
        &connectors,
        &layout_hints,
        None,
        None,
        iceberg_catalogs,
    )?;
    Ok(lowered.node)
}

fn execute_plan(
    result: PlanBuildResult,
    query_opts: Option<crate::internal_service::TQueryOptions>,
    terminal_sink: Option<Box<dyn crate::exec::pipeline::operator_factory::OperatorFactory>>,
    iceberg_catalogs: Option<&crate::connector::iceberg::catalog::IcebergCatalogRegistry>,
) -> Result<QueryResult, String> {
    use crate::exec::expr::ExprArena;
    use crate::exec::node::{ExecPlan, push_down_local_runtime_filters};
    use crate::exec::operators::{ResultSinkFactory, ResultSinkHandle};
    use crate::exec::pipeline::executor::execute_plan_with_pipeline;
    use crate::runtime::runtime_state::RuntimeState;

    let output_columns = result.output_columns.clone();
    let mut arena = ExprArena::default();
    let root = lower_plan_build_result(result, &mut arena, query_opts.as_ref(), iceberg_catalogs)?;
    let mut exec_plan = ExecPlan { arena, root };
    push_down_local_runtime_filters(&mut exec_plan.root, &exec_plan.arena);

    // Default to the result-capturing sink unless the caller supplied a
    // custom terminal sink (e.g. IVM-A1 IcebergMergeSinkFactory). When a
    // custom sink is in use, output chunks are intercepted by the sink so
    // the returned `QueryResult` only carries the column metadata.
    let handle = ResultSinkHandle::new();
    let sink: Box<dyn crate::exec::pipeline::operator_factory::OperatorFactory> =
        match terminal_sink {
            Some(custom_sink) => custom_sink,
            None => Box::new(ResultSinkFactory::new(handle.clone())),
        };

    // Use available CPU cores for pipeline parallelism (capped at 8)
    let pipeline_dop = std::thread::available_parallelism()
        .map(|p| p.get().min(4))
        .unwrap_or(4);
    execute_plan_with_pipeline(
        exec_plan,
        false,
        std::time::Duration::from_millis(10),
        sink,
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
        columns: output_columns
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
    // TRUNCATE TABLE ... PARTITION (...) is intentionally rejected by the
    // TRUNCATE parser with a "PARTITION (...) is not supported" error. The
    // legacy-partition rewriter must not see TRUNCATE's PARTITION clause as a
    // legacy SELECT/INSERT partition reference — it would try to resolve
    // `id=1` as an identifier and surface a confusing "unsupported
    // identifier" diagnostic before the parser even runs.
    let trimmed = sql.trim_start();
    if trimmed.len() >= 8 && trimmed[..8].eq_ignore_ascii_case("truncate") {
        return Ok(sql.to_string());
    }
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
// StarRocks table stream-load entrypoint
// ---------------------------------------------------------------------------

/// HTTP stream-load entrypoint for StarRocks tables. Parses CSV / JSON
/// payloads via the neutral helpers in `engine::stream_load` and hands the
/// resulting rows to `insert_into_starrocks_table`, so every stream-load
/// target goes through the same path as a plain `INSERT INTO ... VALUES`.
fn stream_load_starrocks_table(
    state: &Arc<StandaloneState>,
    request: StandaloneStreamLoadRequest,
) -> Result<StandaloneStreamLoadResult, String> {
    let database = normalize_identifier(&request.database)?;
    let table = normalize_identifier(&request.table)?;
    let is_starrocks_table = state
        .starrocks_table
        .read()
        .expect("standalone StarRocks table read lock")
        .contains_table(&database, &table)?;
    if !is_starrocks_table {
        return Err(format!(
            "standalone stream load only supports StarRocks tables, got {}.{}",
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
    crate::connector::insert_into_starrocks_table(
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
mod build_iceberg_create_table_ddl_tests {
    use super::build_iceberg_create_table_ddl;
    use crate::connector::iceberg::catalog::registry::IcebergLoadedTable;
    use iceberg::spec::{
        FormatVersion, NestedField, PartitionSpec, PrimitiveType, Schema, SortOrder, Type,
    };
    use std::collections::HashMap;
    use std::sync::Arc;

    fn loaded_table_with_props(props: HashMap<String, String>) -> IcebergLoadedTable {
        let schema = Schema::builder()
            .with_fields(vec![Arc::new(NestedField::optional(
                1,
                "id",
                Type::Primitive(PrimitiveType::Int),
            ))])
            .build()
            .expect("build schema");
        let metadata = iceberg::spec::TableMetadataBuilder::new(
            schema,
            PartitionSpec::unpartition_spec(),
            SortOrder::unsorted_order(),
            "/tmp/test".to_string(),
            FormatVersion::V2,
            props,
        )
        .expect("builder")
        .build()
        .expect("metadata")
        .metadata;
        let table = iceberg::table::Table::builder()
            .identifier(iceberg::TableIdent::from_strs(["db", "t"]).unwrap())
            .file_io(iceberg::io::FileIO::new_with_fs())
            .metadata(metadata)
            .build()
            .expect("table");
        IcebergLoadedTable {
            table,
            columns: vec![],
            logical_types: HashMap::new(),
            key_desc: None,
            column_aggregations: HashMap::new(),
            object_store_config: None,
        }
    }

    #[test]
    fn emits_comment_when_property_is_set() {
        let mut props = HashMap::new();
        props.insert("comment".to_string(), "my table comment".to_string());
        let loaded = loaded_table_with_props(props);
        let ddl = build_iceberg_create_table_ddl("cat", "ns", "tbl", &loaded).expect("build ddl");
        assert!(
            ddl.contains("COMMENT 'my table comment'"),
            "expected COMMENT clause in DDL, got: {ddl}"
        );
    }

    #[test]
    fn no_comment_clause_when_property_absent() {
        let loaded = loaded_table_with_props(HashMap::new());
        let ddl = build_iceberg_create_table_ddl("cat", "ns", "tbl", &loaded).expect("build ddl");
        assert!(
            !ddl.contains("COMMENT"),
            "expected no COMMENT clause when property absent, got: {ddl}"
        );
    }

    #[test]
    fn no_comment_clause_when_property_empty() {
        let mut props = HashMap::new();
        props.insert("comment".to_string(), String::new());
        let loaded = loaded_table_with_props(props);
        let ddl = build_iceberg_create_table_ddl("cat", "ns", "tbl", &loaded).expect("build ddl");
        assert!(
            !ddl.contains("COMMENT"),
            "expected no COMMENT clause when property is empty string, got: {ddl}"
        );
    }

    #[test]
    fn comment_with_single_quote_is_escaped() {
        let mut props = HashMap::new();
        props.insert("comment".to_string(), "it's great".to_string());
        let loaded = loaded_table_with_props(props);
        let ddl = build_iceberg_create_table_ddl("cat", "ns", "tbl", &loaded).expect("build ddl");
        assert!(
            ddl.contains("COMMENT 'it\\'s great'"),
            "expected escaped single quote in COMMENT, got: {ddl}"
        );
    }
}

#[cfg(test)]
mod tests {
    use super::{
        QueryResult, StandaloneNovaRocks, StandaloneOptions, StandaloneSession, StandaloneState,
        StatementResult, dispatch_statement, register_connector_backends,
    };
    use crate::connector::starrocks::lake::context::lock_runtime_test_state;
    use crate::meta::MetaStoreProvider;
    use arrow::array::{
        Array, FixedSizeBinaryArray, Int32Array, Int64Array, ListArray, StringArray,
    };
    use arrow::datatypes::{DataType, Field, Schema};
    use std::path::PathBuf;
    use std::sync::Arc;
    use tempfile::TempDir;

    fn write_test_metadata_config(dir: &TempDir, metadata_path: &str) -> PathBuf {
        let config_path = dir.path().join("novarocks.toml");
        std::fs::write(
            &config_path,
            format!(
                r#"[metadata]
provider = "sqlite"
path = "{metadata_path}"
"#
            ),
        )
        .expect("write metadata config");
        config_path
    }

    #[test]
    fn single_fragment_collapse_removes_distribution_enforcers() {
        use crate::sql::analysis::JoinKind;
        use crate::sql::optimizer::operator::{
            JoinDistribution, Operator, PhysicalDistributionOp, PhysicalHashJoinOp,
            PhysicalValuesOp,
        };
        use crate::sql::optimizer::physical_plan::PhysicalPlanNode;
        use crate::sql::optimizer::property::DistributionSpec;
        use crate::sql::optimizer::statistics::Statistics;

        fn stats() -> Statistics {
            Statistics {
                output_row_count: 0.0,
                column_statistics: Default::default(),
            }
        }

        fn values_node() -> PhysicalPlanNode {
            PhysicalPlanNode {
                op: Operator::PhysicalValues(PhysicalValuesOp {
                    rows: Vec::new(),
                    columns: Vec::new(),
                }),
                children: Vec::new(),
                stats: stats(),
                output_columns: Vec::new(),
                build_runtime_filters: Vec::new(),
                probe_runtime_filters: Vec::new(),
            }
        }

        fn distributed_values_node() -> PhysicalPlanNode {
            PhysicalPlanNode {
                op: Operator::PhysicalDistribution(PhysicalDistributionOp {
                    spec: DistributionSpec::Gather,
                }),
                children: vec![values_node()],
                stats: stats(),
                output_columns: Vec::new(),
                build_runtime_filters: Vec::new(),
                probe_runtime_filters: Vec::new(),
            }
        }

        let plan = PhysicalPlanNode {
            op: Operator::PhysicalHashJoin(PhysicalHashJoinOp {
                join_type: JoinKind::Inner,
                eq_conditions: Vec::new(),
                other_condition: None,
                distribution: JoinDistribution::Shuffle,
            }),
            children: vec![distributed_values_node(), distributed_values_node()],
            stats: stats(),
            output_columns: Vec::new(),
            build_runtime_filters: Vec::new(),
            probe_runtime_filters: Vec::new(),
        };

        let collapsed = super::collapse_distribution_enforcers_for_single_fragment(plan);

        assert!(matches!(
            &collapsed.op,
            Operator::PhysicalHashJoin(join)
                if matches!(&join.distribution, JoinDistribution::Broadcast)
        ));
        assert!(matches!(
            &collapsed.children[0].op,
            Operator::PhysicalValues(_)
        ));
        assert!(matches!(
            &collapsed.children[1].op,
            Operator::PhysicalValues(_)
        ));
    }

    #[test]
    fn metadata_backend_resolves_metadata_path_relative_to_config_parent() {
        let _runtime_guard = lock_runtime_test_state();
        let dir = TempDir::new().expect("create config dir");
        let config_dir = dir.path().join("conf");
        std::fs::create_dir_all(&config_dir).expect("create config dir");
        let config_path = config_dir.join("novarocks.toml");
        std::fs::write(
            &config_path,
            r#"[metadata]
provider = "sqlite"
path = "meta/catalog.db"
"#,
        )
        .expect("write config");

        crate::novarocks_config::init_from_path(&config_path).expect("load config");
        let backend = super::resolve_metadata_backend(&StandaloneOptions {
            config_path: Some(config_path.clone()),
        })
        .expect("resolve backend")
        .expect("metadata backend");

        assert_eq!(
            backend.provider,
            crate::common::app_config::MetadataProviderConfig::Sqlite
        );
        assert_eq!(backend.path, config_dir.join("meta/catalog.db"));
    }

    #[test]
    fn metadata_backend_is_absent_without_metadata_config() {
        let _runtime_guard = lock_runtime_test_state();
        let dir = TempDir::new().expect("create config dir");
        let config_path = dir.path().join("novarocks.toml");
        std::fs::write(
            &config_path,
            r#"[standalone_server]
mysql_port = 19030
"#,
        )
        .expect("write config");

        crate::novarocks_config::init_from_path(&config_path).expect("load config");
        let backend = super::resolve_metadata_backend(&StandaloneOptions {
            config_path: Some(config_path.clone()),
        })
        .expect("resolve backend");
        assert!(backend.is_none());
    }

    #[test]
    fn standalone_state_retains_metadata_provider_from_metadata_config() {
        let _runtime_guard = lock_runtime_test_state();
        let dir = TempDir::new().expect("create config dir");
        let config_path = dir.path().join("novarocks.toml");
        std::fs::write(
            &config_path,
            r#"[metadata]
provider = "sqlite"
path = "meta/catalog.db"
"#,
        )
        .expect("write config");

        let engine = StandaloneNovaRocks::open(StandaloneOptions {
            config_path: Some(config_path),
        })
        .expect("open engine");

        assert!(engine.inner.metadata_provider.is_some());
        assert_eq!(
            engine
                .inner
                .metadata_provider
                .as_ref()
                .expect("metadata provider")
                .provider_name(),
            "sqlite"
        );
    }

    // I1: open_with_config must use the supplied NovaRocksConfig instead of
    // re-reading from disk.  If the file is overwritten with invalid TOML after
    // load but before open_with_config, the call must still succeed.
    #[test]
    fn open_with_config_does_not_reread_config_file() {
        let _runtime_guard = lock_runtime_test_state();
        let dir = TempDir::new().expect("create config dir");
        let config_path = dir.path().join("novarocks.toml");

        // Write a valid config to disk.
        std::fs::write(
            &config_path,
            r#"[standalone_server]
mysql_port = 47892
"#,
        )
        .expect("write sentinel config");

        // Load the config before corrupting the file.
        let cfg = crate::novarocks_config::NovaRocksConfig::load_from_file(&config_path)
            .expect("load sentinel config");
        assert_eq!(
            cfg.standalone_server.as_ref().map(|s| s.mysql_port),
            Some(47892),
            "preloaded config must contain sentinel port"
        );

        // Overwrite the file with invalid TOML — a reload from disk would fail.
        std::fs::write(&config_path, "NOT VALID TOML !!!").expect("corrupt config file");

        // open_with_config must use the preloaded cfg, not re-read the corrupted file.
        let result = StandaloneNovaRocks::open_with_config(
            StandaloneOptions {
                config_path: Some(config_path),
            },
            cfg,
        );
        assert!(
            result.is_ok(),
            "open_with_config must succeed with preloaded config even when the config file is \
             invalid; got: {:?}",
            result.err()
        );
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
        let config_path = write_test_metadata_config(&temp, "metadata.db");
        let engine = StandaloneNovaRocks::open(StandaloneOptions {
            config_path: Some(config_path),
        })
        .expect("engine");
        let outcome = crate::meta::repository::job::IcebergOptimizeJobOutcome {
            target_snapshot_id: Some(124),
            rewritten_data_files: 3,
            deleted_data_files: 2,
            added_data_files: 1,
            output_record_count: 7,
        };
        seed_finished_iceberg_optimize_job(&engine, "ice", "db1", "orders", 123, 1000, outcome);

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
        let config_path = write_test_metadata_config(&temp, "metadata.db");
        let engine = StandaloneNovaRocks::open(StandaloneOptions {
            config_path: Some(config_path),
        })
        .expect("engine");
        seed_pending_iceberg_optimize_job(&engine, "ice1", "db1", "orders", 101, 1_000);
        seed_pending_iceberg_optimize_job(&engine, "ice2", "db1", "orders", 102, 2_000);
        seed_pending_iceberg_optimize_job(&engine, "ice1", "db2", "orders", 103, 3_000);

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

    fn seed_pending_iceberg_optimize_job(
        engine: &StandaloneNovaRocks,
        catalog: &str,
        namespace: &str,
        table: &str,
        base_snapshot_id: i64,
        created_at_ms: i64,
    ) -> i64 {
        let provider = engine
            .inner
            .metadata_provider
            .as_ref()
            .expect("metadata provider");
        let mut txn = provider
            .begin_write("seed pending iceberg optimize job")
            .expect("write");
        let job = engine
            .inner
            .job_repo
            .create_iceberg_optimize_job(
                txn.as_mut(),
                crate::meta::repository::job::CreateIcebergOptimizeJobRequest {
                    catalog: catalog.to_string(),
                    namespace: namespace.to_string(),
                    table: table.to_string(),
                    base_snapshot_id,
                    now_ms: created_at_ms,
                },
            )
            .expect("create optimize job");
        txn.commit().expect("commit create optimize job");
        job.id
    }

    fn seed_finished_iceberg_optimize_job(
        engine: &StandaloneNovaRocks,
        catalog: &str,
        namespace: &str,
        table: &str,
        base_snapshot_id: i64,
        created_at_ms: i64,
        outcome: crate::meta::repository::job::IcebergOptimizeJobOutcome,
    ) {
        let job_id = seed_pending_iceberg_optimize_job(
            engine,
            catalog,
            namespace,
            table,
            base_snapshot_id,
            created_at_ms,
        );
        let provider = engine
            .inner
            .metadata_provider
            .as_ref()
            .expect("metadata provider");

        let mut txn = provider
            .begin_write("claim synthetic optimize job")
            .expect("claim write");
        engine
            .inner
            .job_repo
            .claim_iceberg_optimize_job(txn.as_mut(), job_id, created_at_ms + 100)
            .expect("claim synthetic optimize job");
        txn.commit().expect("commit claim optimize job");

        let mut txn = provider
            .begin_write("record synthetic optimize outcome")
            .expect("outcome write");
        engine
            .inner
            .job_repo
            .record_iceberg_optimize_job_outcome(
                txn.as_mut(),
                job_id,
                created_at_ms + 200,
                outcome.clone(),
            )
            .expect("record synthetic optimize outcome");
        txn.commit().expect("commit optimize outcome");

        let mut txn = provider
            .begin_write("finish synthetic optimize job")
            .expect("finish write");
        engine
            .inner
            .job_repo
            .finish_iceberg_optimize_job(txn.as_mut(), job_id, created_at_ms + 300, outcome)
            .expect("finish synthetic optimize job");
        txn.commit().expect("commit finish optimize job");
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

    fn starrocks_table_endpoint_reachable(endpoint: &str) -> bool {
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
                .expect("StarRocks table endpoint socket addr"),
            std::time::Duration::from_secs(1),
        )
        .is_ok()
    }

    fn maybe_starrocks_table_config() -> Option<(TempDir, std::path::PathBuf, std::path::PathBuf)> {
        let endpoint = std::env::var("AWS_S3_ENDPOINT")
            .unwrap_or_else(|_| "http://127.0.0.1:9000".to_string());
        if !starrocks_table_endpoint_reachable(&endpoint) {
            eprintln!(
                "skipping StarRocks table test: object store endpoint is unreachable: {endpoint}"
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
        let root_prefix = std::env::var("AWS_S3_ROOT")
            .unwrap_or_else(|_| "codex-starrocks-table-tests".to_string());
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

        let dir = TempDir::new().expect("create StarRocks table config dir");
        let metadata_dir = dir.path().join("meta");
        std::fs::create_dir_all(&metadata_dir).expect("create metadata dir");
        let metadata_path = metadata_dir.join("standalone.sqlite");
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
        .expect("write StarRocks table config");
        Some((dir, config_path, metadata_path))
    }

    fn build_fragments_for_query(sql: &str) -> crate::sql::codegen::MultiFragmentBuildResult {
        use crate::sql::catalog::{
            ColumnDef, PhysicalTableLayout, ScanSource, StarRocksTabletRef, TableDef,
        };
        use crate::sql::parser::dialect::{StarRocksDialect, normalize_for_raw_parse};

        let mut catalog = super::InMemoryCatalog::default();
        let table = TableDef {
            name: "tbl".to_string(),
            columns: vec![
                ColumnDef {
                    name: "id".to_string(),
                    data_type: DataType::Int32,
                    nullable: false,
                    write_default: None,
                    logical_type: None,
                },
                ColumnDef {
                    name: "name".to_string(),
                    data_type: DataType::Utf8,
                    nullable: true,
                    write_default: None,
                    logical_type: None,
                },
            ],
            iceberg_row_lineage_metadata_columns: vec![],
            // Must match the PhysicalTableLayout below so the debug_assert
            // in InMemoryCatalog::register_starrocks_table is satisfied.
            source: ScanSource::StarRocks {
                db_id: 1,
                table_id: 2,
            },
        };
        let layout = PhysicalTableLayout {
            db_id: 1,
            table_id: 2,
            schema_id: 3,
            tablets: vec![StarRocksTabletRef {
                tablet_id: 4,
                partition_id: 5,
                version: 6,
            }],
        };
        catalog
            .register_starrocks_table("default", table, layout.clone())
            .expect("register StarRocks tbl");

        let registry = mock_starrocks_registry_for_engine_test(&layout);

        let normalized = normalize_for_raw_parse(sql).expect("normalize sql");
        let mut parser = sqlparser::parser::Parser::new(&StarRocksDialect)
            .try_with_sql(&normalized)
            .expect("build parser");
        let statement = parser.parse_statement().expect("parse statement");
        let sqlparser::ast::Statement::Query(query) = statement else {
            panic!("expected query statement");
        };

        let (resolved, cte_registry, mut factory) =
            crate::sql::analyzer::analyze(&query, &catalog, "default").expect("analyze query");
        let logical = crate::sql::planner::plan_query(resolved, cte_registry, &mut factory)
            .expect("plan query");
        let table_stats = super::build_table_stats_from_plan(&logical);
        let physical = crate::sql::optimizer::optimize(logical, &table_stats, factory, None)
            .expect("optimize");
        crate::sql::codegen::fragment_builder::PlanFragmentBuilder::build(
            &physical, &catalog, &registry, "default",
        )
        .expect("build fragments")
    }

    /// Build a `ConnectorRegistry` with a mock StarRocks scan planner that
    /// returns the schema_id and tablet splits from the given layout. Used by
    /// engine-level tests that call `PlanFragmentBuilder::build` with a
    /// StarRocks table but do not have a full `StandaloneState` available.
    fn mock_starrocks_registry_for_engine_test(
        layout: &crate::sql::catalog::PhysicalTableLayout,
    ) -> crate::connector::ConnectorRegistry {
        use crate::connector::scan_planning::{
            BeginScanContext, ConnectorScanPlanner, SplitPlanningContext, ThriftScanContext,
            ThriftScanPlan,
        };
        use crate::connector::starrocks::table::{
            StarRocksScanHandle, StarRocksSplit, StarRocksTableHandle,
        };

        #[derive(Debug)]
        struct MockPlanner {
            schema_id: i64,
            splits: Vec<StarRocksSplit>,
        }

        impl ConnectorScanPlanner for MockPlanner {
            fn name(&self) -> &'static str {
                "starrocks"
            }

            fn begin_scan(
                &self,
                table: crate::connector::scan_planning::TableHandle,
                _ctx: BeginScanContext,
            ) -> Result<crate::connector::scan_planning::ScanHandle, String> {
                let inner = table
                    .downcast_ref::<StarRocksTableHandle>()
                    .ok_or_else(|| "MockPlanner expected StarRocksTableHandle".to_string())?
                    .clone();
                Ok(crate::connector::scan_planning::ScanHandle::new(
                    "starrocks",
                    StarRocksScanHandle {
                        table: inner,
                        schema_id: self.schema_id,
                    },
                ))
            }

            fn plan_splits(
                &self,
                _scan: &crate::connector::scan_planning::ScanHandle,
                _ctx: SplitPlanningContext,
            ) -> Result<Vec<crate::connector::scan_planning::Split>, String> {
                Ok(self
                    .splits
                    .iter()
                    .map(|s| crate::connector::scan_planning::Split::new("starrocks", s.clone()))
                    .collect())
            }

            fn to_thrift_scan(
                &self,
                scan: &crate::connector::scan_planning::ScanHandle,
                splits: &[crate::connector::scan_planning::Split],
                ctx: ThriftScanContext,
            ) -> Result<ThriftScanPlan, String> {
                let planner =
                    crate::connector::starrocks::table::StarRocksTableScanPlanner::stateless_for_codegen();
                <crate::connector::starrocks::table::StarRocksTableScanPlanner as ConnectorScanPlanner>::to_thrift_scan(
                    &planner, scan, splits, ctx,
                )
            }
        }

        let splits = layout
            .tablets
            .iter()
            .map(|t| StarRocksSplit {
                tablet_id: t.tablet_id,
                partition_id: t.partition_id,
                version: t.version,
            })
            .collect();
        let planner = std::sync::Arc::new(MockPlanner {
            schema_id: layout.schema_id,
            splits,
        });
        let mut registry = crate::connector::ConnectorRegistry::new();
        registry.register_scan_planner(planner);
        registry.register_scan_planner(std::sync::Arc::new(
            crate::connector::iceberg::IcebergConnectorScanPlanner::new(),
        ));
        registry
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
                logical_type: None,
            },
            ColumnDef {
                name: "score_items".to_string(),
                data_type: DataType::List(Arc::new(Field::new("item", DataType::Int32, true))),
                nullable: true,
                write_default: None,
                logical_type: None,
            },
            ColumnDef {
                name: "tags".to_string(),
                data_type: DataType::List(Arc::new(Field::new("item", DataType::Utf8, true))),
                nullable: true,
                write_default: None,
                logical_type: None,
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
            logical_type: None,
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
            logical_type: None,
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
            logical_type: None,
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
            logical_type: None,
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
            logical_type: None,
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

    /// OQ-5 Task 6: codegen must lower the runtime-filter annotations the
    /// physical-tree pass attaches to a hash join into thrift
    /// `TRuntimeFilterDescription`s on the join node, AND assemble a
    /// `RuntimeFilterPlanResult`. Exercises the full standalone pipeline
    /// (analyze -> plan -> optimize[annotate] -> codegen) over the test
    /// catalog's `tbl(id int, name varchar)`, self-joined on `id`.
    #[test]
    fn codegen_emits_build_runtime_filters_from_annotation() {
        let build =
            build_fragments_for_query("SELECT count(*) FROM tbl a JOIN tbl b ON a.id = b.id");
        let has_rf = build.fragment_results.iter().any(|fr| {
            fr.plan.nodes.iter().any(|n| {
                n.hash_join_node
                    .as_ref()
                    .and_then(|hj| hj.build_runtime_filters.as_ref())
                    .map(|v| !v.is_empty())
                    .unwrap_or(false)
            })
        });
        assert!(
            has_rf,
            "expected a hash join thrift node with build_runtime_filters"
        );
        // The coordinator-facing RF plan must be assembled (all_filters +
        // build-side mapping populated; probe placed onto the scan target).
        let rf_plan = build
            .rf_plan
            .as_ref()
            .expect("rf_plan should be Some when a join emits filters");
        assert!(
            !rf_plan.all_filters.is_empty(),
            "all_filters must carry the lowered descriptor"
        );
        assert!(
            rf_plan.build_side_filters.values().any(|v| !v.is_empty()),
            "build_side_filters must record the join fragment"
        );
        assert!(
            rf_plan.probe_side_filters.values().any(|v| !v.is_empty()),
            "probe_side_filters must record the probe target"
        );
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
    fn top_level_wrapper_with_limit_is_not_stripped() {
        let build = build_fragments_for_query("SELECT id FROM tbl LIMIT 5");

        assert!(
            super::top_level_stream_root_wrapper_child_id(&build).is_none(),
            "top-level exchange wrapper carrying LIMIT must stay as the root"
        );
    }

    #[test]
    fn top_level_merging_topn_wrapper_is_not_stripped() {
        let build = build_fragments_for_query("SELECT id FROM tbl ORDER BY id LIMIT 5");

        assert!(
            super::top_level_stream_root_wrapper_child_id(&build).is_none(),
            "top-level exchange wrapper carrying merging TopN semantics must stay as the root"
        );
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
    fn embedded_query_rejects_unknown_table() {
        let engine = StandaloneNovaRocks::open(StandaloneOptions::default()).expect("open engine");
        let session = engine.session();
        let err = session
            .query("select * from missing")
            .expect_err("missing table");
        assert!(err.contains("unknown table"));
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

        let empty_result = session
            .query("select id, name from ice.db1.tbl limit 0")
            .expect("query empty iceberg table");
        assert_eq!(empty_result.row_count(), 0);
        assert_eq!(empty_result.columns[0].name, "id");
        assert_eq!(empty_result.columns[1].name, "name");

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
    fn embedded_session_restores_iceberg_metadata_from_sqlite() {
        let warehouse = TempDir::new().expect("create iceberg warehouse");
        let metadata_dir = TempDir::new().expect("create metadata dir");
        let config_path = write_test_metadata_config(&metadata_dir, "standalone.sqlite");

        {
            let engine = StandaloneNovaRocks::open(StandaloneOptions {
                config_path: Some(config_path.clone()),
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

            assert!(matches!(
                session
                    .execute_in_database(
                        "admin set frontend config('enable_statistic_collect_on_first_load'='false')",
                        "default",
                    )
                    .expect("disable first-load stats for iceberg insert"),
                StatementResult::Ok
            ));

            let insert = session
                .execute_in_database(
                    "insert into ice.db1.tbl values (1, 'a'), (2, 'b')",
                    "default",
                )
                .expect("insert iceberg rows");
            assert!(matches!(insert, StatementResult::Ok));
        }

        let restored = StandaloneNovaRocks::open(StandaloneOptions {
            config_path: Some(config_path),
        })
        .expect("reopen engine");
        let entry = {
            let registry = restored
                .inner
                .iceberg_catalogs
                .read()
                .expect("iceberg registry read lock");
            registry.get("ice").expect("load restored iceberg catalog")
        };
        let loaded = crate::connector::load_iceberg_table(&entry, "db1", "tbl")
            .expect("load restored table");
        assert!(
            loaded.table.metadata().current_snapshot().is_some(),
            "restored iceberg table should retain inserted snapshot"
        );
    }

    #[test]
    fn restore_metadata_registers_iceberg_mv_target_from_relationship() {
        let warehouse = TempDir::new().expect("create iceberg warehouse");
        let metadata_dir = TempDir::new().expect("create metadata dir");
        let config_path = write_test_metadata_config(&metadata_dir, "standalone.sqlite");

        {
            let engine = StandaloneNovaRocks::open(StandaloneOptions {
                config_path: Some(config_path.clone()),
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
                        "create table ice.sales.orders (id int, name string) \
                         tblproperties(\"format-version\"=\"3\", \"write.row-lineage\"=\"true\")",
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
            let drop_column_err = session
                .execute_in_database("ALTER TABLE ice.sales.orders DROP COLUMN name", "default")
                .expect_err("MV base column dependency must block DROP COLUMN");
            assert!(
                drop_column_err.contains("materialized view"),
                "err={drop_column_err}"
            );

            let provider = engine
                .inner
                .metadata_provider
                .as_ref()
                .expect("metadata provider");
            let read = provider.begin_read().expect("open read txn");
            let starrocks_table_snapshot = engine
                .inner
                .starrocks_table_repo
                .load_snapshot(read.as_ref())
                .expect("load StarRocks snapshot");
            assert!(
                !starrocks_table_snapshot
                    .tables
                    .iter()
                    .any(|table| table.name == "mv_orders")
            );
            assert!(
                engine
                    .inner
                    .iceberg_catalog_repo
                    .table_exists(read.as_ref(), "ice", "analytics", "mv_orders")
                    .expect("load generic iceberg table metadata")
                    == false
            );
            assert!(
                engine
                    .inner
                    .mv_repo
                    .find_by_target(read.as_ref(), "ice", "analytics", "mv_orders")
                    .expect("find iceberg mv definition")
                    .is_some()
            );
        }

        let restored = StandaloneNovaRocks::open(StandaloneOptions {
            config_path: Some(config_path),
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
    fn embedded_session_reopen_cleans_incomplete_starrocks_truncate_stage_partition() {
        let _runtime_guard = lock_runtime_test_state();
        use crate::meta::repository::starrocks_table::{
            StageStarRocksTruncateRequest, StarRocksIndexState, StarRocksPartitionState,
        };

        let Some((_dir, config_path, metadata_path)) = maybe_starrocks_table_config() else {
            return;
        };

        {
            let engine = StandaloneNovaRocks::open(StandaloneOptions {
                config_path: Some(config_path.clone()),
            })
            .expect("open engine");
            engine
                .session()
                .execute(
                    "create table orders (k1 int, v1 string) duplicate key(k1) distributed by hash(k1) buckets 2",
                )
                .expect("create StarRocks table");
        }

        let provider =
            crate::meta::SqliteMetaStoreProvider::open(&metadata_path).expect("open provider");
        let creating_partition_id = {
            let mut txn = provider
                .begin_write("seed incomplete truncate stage")
                .expect("open write txn");
            let snapshot =
                crate::meta::repository::starrocks_table::StarRocksTableMetaRepository::default()
                    .load_snapshot(txn.as_ref())
                    .expect("load StarRocks snapshot");
            let table = snapshot
                .tables
                .iter()
                .find(|table| table.name == "orders")
                .cloned()
                .expect("orders table");
            let staged =
                crate::meta::repository::starrocks_table::StarRocksTableMetaRepository::default()
                    .stage_truncate_partition(
                        txn.as_mut(),
                        StageStarRocksTruncateRequest {
                            table_id: table.table_id,
                            db_id: table.db_id,
                            bucket_num: table.bucket_num,
                            partition_name: "p0".to_string(),
                            warehouse_uri: "s3://test/warehouse".to_string(),
                        },
                    )
                    .expect("stage truncate partition");
            txn.commit().expect("commit staged partition");
            staged.partition_id
        };

        let reopened = StandaloneNovaRocks::open(StandaloneOptions {
            config_path: Some(config_path),
        })
        .expect("reopen engine");
        let result = reopened
            .session()
            .query("select * from orders")
            .expect("query reopened StarRocks table");
        assert_eq!(result.row_count(), 0);

        let reloaded = {
            let read = provider.begin_read().expect("open read txn");
            crate::meta::repository::starrocks_table::StarRocksTableMetaRepository::default()
                .load_snapshot(read.as_ref())
                .expect("reload StarRocks snapshot")
        };
        assert!(
            !reloaded
                .partitions
                .iter()
                .any(|partition| partition.state == StarRocksPartitionState::Creating)
        );
        assert!(
            !reloaded
                .indexes
                .iter()
                .any(|index| index.state == StarRocksIndexState::Creating)
        );
        assert!(
            !reloaded
                .tablets
                .iter()
                .any(|tablet| tablet.partition_id == creating_partition_id)
        );
    }

    #[test]
    fn embedded_session_reopen_keeps_truncated_starrocks_table_empty() {
        let _runtime_guard = lock_runtime_test_state();
        let Some((_dir, config_path, _metadata_path)) = maybe_starrocks_table_config() else {
            return;
        };

        {
            let engine = StandaloneNovaRocks::open(StandaloneOptions {
                config_path: Some(config_path.clone()),
            })
            .expect("open engine");
            let session = engine.session();
            session
                .execute(
                    "create table orders (k1 int, v1 string) duplicate key(k1) distributed by hash(k1) buckets 2",
                )
                .expect("create StarRocks table");
            session
                .execute("insert into orders values (1, 'a'), (2, 'b')")
                .expect("insert StarRocks rows");
            session
                .execute("truncate table orders")
                .expect("truncate table");
        }

        let reopened = StandaloneNovaRocks::open(StandaloneOptions {
            config_path: Some(config_path),
        })
        .expect("reopen engine");
        let result = reopened
            .session()
            .query("select * from orders")
            .expect("query truncated StarRocks table");
        assert_eq!(result.row_count(), 0);
    }

    #[test]
    fn starrocks_pk_delete_via_op_column_path() {
        let _runtime_guard = lock_runtime_test_state();
        let Some((_dir, config_path, _metadata_db_path)) = maybe_starrocks_table_config() else {
            return;
        };
        let engine = StandaloneNovaRocks::open(StandaloneOptions {
            config_path: Some(config_path),
        })
        .expect("open engine");
        let session = engine.session();

        session
            .execute(
                "create table t_pk (id bigint not null, payload string) primary key(id) \
                 distributed by hash(id) buckets 2",
            )
            .expect("create pk");
        session
            .execute("insert into t_pk values (1,'a'),(2,'b'),(3,'c')")
            .expect("insert");
        session
            .execute("delete from t_pk where id = 2")
            .expect("delete pk row");

        let remaining = session
            .query("select id from t_pk order by id")
            .expect("query remaining");
        assert_eq!(remaining.row_count(), 2);
    }

    #[test]
    fn starrocks_pk_delete_complex_where() {
        let _runtime_guard = lock_runtime_test_state();
        let Some((_dir, config_path, _metadata_db_path)) = maybe_starrocks_table_config() else {
            return;
        };
        let engine = StandaloneNovaRocks::open(StandaloneOptions {
            config_path: Some(config_path),
        })
        .expect("open engine");
        let session = engine.session();
        session
            .execute(
                "create table t_pk_cmplx (id int not null, k int, label string) primary key(id) \
                 distributed by hash(id) buckets 2",
            )
            .expect("create cmplx");
        session
            .execute("insert into t_pk_cmplx values (1,10,'x'),(2,20,'y'),(3,30,'z')")
            .expect("insert");
        session
            .execute("delete from t_pk_cmplx where lower(label) = 'y'")
            .expect("delete by function on non-key");
        let remaining = session
            .query("select id from t_pk_cmplx order by id")
            .expect("query remaining");
        assert_eq!(remaining.row_count(), 2);
    }

    #[test]
    fn starrocks_pk_delete_then_insert_same_pk_visible() {
        let _runtime_guard = lock_runtime_test_state();
        let Some((_dir, config_path, _metadata_db_path)) = maybe_starrocks_table_config() else {
            return;
        };
        let engine = StandaloneNovaRocks::open(StandaloneOptions {
            config_path: Some(config_path),
        })
        .expect("open engine");
        let session = engine.session();
        session
            .execute(
                "create table t_pk_cycle (id int not null, label string) primary key(id) \
                 distributed by hash(id) buckets 1",
            )
            .expect("create cycle");
        session
            .execute("insert into t_pk_cycle values (1, 'old')")
            .expect("insert old");
        session
            .execute("delete from t_pk_cycle where id = 1")
            .expect("delete old");
        session
            .execute("insert into t_pk_cycle values (1, 'new')")
            .expect("insert new");
        let r = session
            .query("select id, label from t_pk_cycle")
            .expect("query");
        assert_eq!(
            r.row_count(),
            1,
            "expected exactly one row after delete-then-insert"
        );
    }

    #[test]
    fn starrocks_dup_delete_via_delete_predicate_path() {
        let _runtime_guard = lock_runtime_test_state();
        let Some((_dir, config_path, _metadata_db_path)) = maybe_starrocks_table_config() else {
            return;
        };
        let engine = StandaloneNovaRocks::open(StandaloneOptions {
            config_path: Some(config_path),
        })
        .expect("open engine");
        let session = engine.session();
        session
            .execute(
                "create table t_dup (id int, name string) duplicate key(id) \
                 distributed by hash(id) buckets 2",
            )
            .expect("create dup");
        session
            .execute("insert into t_dup values (1, 'a'), (2, 'b'), (3, 'c')")
            .expect("insert");
        session
            .execute("delete from t_dup where id = 2")
            .expect("delete via predicate");
        let remaining = session
            .query("select id from t_dup order by id")
            .expect("query remaining");
        assert_eq!(remaining.row_count(), 2);
        let deleted = session
            .query("select id from t_dup where id = 2")
            .expect("query deleted");
        assert_eq!(deleted.row_count(), 0);
    }

    #[test]
    fn embedded_session_open_starts_erase_worker_for_pending_jobs() {
        let _runtime_guard = lock_runtime_test_state();
        use crate::meta::repository::job::{CreateEraseJobRequest, JobState};
        use crate::meta::repository::starrocks_table::{
            CreateStarRocksTableLayoutRequest, StarRocksTableKind, StarRocksTableMetaRepository,
        };
        use crate::service::grpc_client::proto::starrocks::TabletSchemaPb;
        use prost::Message;

        let config_dir = TempDir::new().expect("create config dir");
        let metadata_dir = config_dir.path().join("meta");
        std::fs::create_dir_all(&metadata_dir).expect("create metadata dir");
        let metadata_path = metadata_dir.join("standalone.sqlite");
        let config_path = config_dir.path().join("novarocks.toml");
        std::fs::write(
            &config_path,
            r#"[metadata]
provider = "sqlite"
path = "meta/standalone.sqlite"

[standalone_server]
user = "root"
warehouse_uri = "s3://test/warehouse"

[standalone_server.object_store]
endpoint = "http://127.0.0.1:1"
access_key_id = "ak"
access_key_secret = "sk"
enable_path_style_access = true
"#,
        )
        .expect("write config");

        let provider =
            crate::meta::SqliteMetaStoreProvider::open(&metadata_path).expect("open provider");
        let job_id = {
            let mut txn = provider
                .begin_write("seed pending erase job")
                .expect("write");
            let starrocks_table_repo = StarRocksTableMetaRepository::default();
            let database = starrocks_table_repo
                .get_or_create_database(txn.as_mut(), "analytics")
                .expect("create database");
            let created = starrocks_table_repo
                .create_table_layout(
                    txn.as_mut(),
                    CreateStarRocksTableLayoutRequest {
                        db_id: database.db_id,
                        table_name: "orders".to_string(),
                        keys_type: "DUP_KEYS".to_string(),
                        bucket_num: 1,
                        kind: StarRocksTableKind::Table,
                        schema_version: 0,
                        tablet_schema_pb: TabletSchemaPb::default().encode_to_vec(),
                        columns: Vec::new(),
                        partition_name: "p0".to_string(),
                        warehouse_uri: "s3://test/warehouse".to_string(),
                    },
                )
                .expect("create StarRocks layout");
            starrocks_table_repo
                .mark_table_dropping(txn.as_mut(), created.table.table_id)
                .expect("mark table dropping");
            let erase_job = crate::meta::repository::job::JobMetaRepository::default()
                .create_erase_job(
                    txn.as_mut(),
                    CreateEraseJobRequest {
                        table_id: created.table.table_id,
                        partition_id: None,
                        root_path: "s3://test/warehouse".to_string(),
                        now_ms: 0,
                    },
                )
                .expect("create erase job");
            txn.commit().expect("commit pending erase job");
            erase_job.job_id
        };

        let engine = StandaloneNovaRocks::open(StandaloneOptions {
            config_path: Some(config_path),
        })
        .expect("open engine");

        let started = std::time::Instant::now();
        loop {
            let read = provider.begin_read().expect("open read txn");
            let job = engine
                .inner
                .job_repo
                .load_erase_job(read.as_ref(), job_id)
                .expect("load erase job")
                .expect("erase job should exist");
            if job.state == JobState::Failed {
                assert!(
                    job.last_error
                        .as_deref()
                        .is_some_and(|msg| msg.contains("empty StarRocks table root")),
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
        // This test's only goal is to confirm `Statement::RefreshMaterializedView`
        // is routed to the materialized-view dispatch path (not, say, an iceberg
        // flow or a generic statement handler). The specific error message is
        // incidental — any error surfaced from inside the StarRocks table MV
        // refresh handler proves correct routing. Accept several signposts
        // because the exact failure point depends on which precondition is
        // checked first (catalog lookup vs. StarRocks table config presence vs.
        // metadata-store availability) and that order has shifted over time.
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
                    full: false,
                },
            ),
        )
        .expect_err("refresh should fail without StarRocks table config");
        assert!(
            err.contains("StarRocks table config is missing")
                || err.contains("sqlite metadata store")
                || err.contains("materialized view")
                || err.contains("StarRocks table"),
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
        // standalone iceberg backend's local-FS path historically only
        // registered the *first* data file for local-FS tables (see
        // `connector/iceberg/catalog/backend.rs`'s data-files branch), so
        // a SELECT-side verification would only see the seed file even
        // though the new snapshot includes both. This is a separate
        // NovaRocks-side gap tracked outside Phase 1; here we verify the
        // iceberg layer's snapshot chain advanced as expected via the
        // registry.
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

    // -----------------------------------------------------------------------
    // ANALYZE TABLE / ANALYZE FULL TABLE against iceberg external-catalog
    // tables that were created + populated but never SELECTed from. Iceberg
    // tables register into the in-memory catalog lazily per SELECT, so the
    // statistics path must materialize the table itself before resolving its
    // columns (regression: ANALYZE failed with "unknown table").
    // -----------------------------------------------------------------------

    fn iceberg_column_stat_row_count(session: &StandaloneSession, table_name: &str) -> usize {
        let sql = format!(
            "select column_name from _statistics_.column_statistics where table_name = '{table_name}'"
        );
        session
            .query(&sql)
            .expect("query column statistics")
            .row_count()
    }

    #[test]
    fn analyze_table_resolves_iceberg_table_via_session_catalog() {
        // Faithful reproduction of the SQL-suite scenario: `SET catalog <ice>`
        // (current_catalog set) followed by a 2-part `db.table` ANALYZE before
        // any SELECT against the table.
        let warehouse = TempDir::new().expect("warehouse");
        let (engine, session) = open_iceberg_session_with_table(&warehouse, "3");
        session
            .execute_in_database("insert into ice.db1.t values (1, 'a'), (2, 'b')", "default")
            .expect("seed");

        // Sanity: the table is NOT in the in-memory catalog yet (never SELECTed).
        assert!(
            !engine.has_local_table("db1", "t"),
            "iceberg table must not be pre-registered before ANALYZE",
        );

        session
            .execute_in_context("analyze table db1.t", Some("ice"), "db1", None)
            .expect("ANALYZE TABLE must resolve iceberg external table via session catalog");

        // The table is now materialized and column statistics were recorded
        // for both columns.
        assert!(engine.has_local_table("db1", "t"));
        assert_eq!(
            iceberg_column_stat_row_count(&session, "db1.t"),
            2,
            "ANALYZE TABLE must record stats for both iceberg columns",
        );
    }

    #[test]
    fn analyze_full_table_resolves_iceberg_table_via_three_part_name() {
        // The 3-part `catalog.db.table` form (current_catalog = None) must also
        // resolve the iceberg table before stats collection.
        let warehouse = TempDir::new().expect("warehouse");
        let (engine, session) = open_iceberg_session_with_table(&warehouse, "3");
        session
            .execute_in_database("insert into ice.db1.t values (1, 'a'), (2, 'b')", "default")
            .expect("seed");

        assert!(!engine.has_local_table("db1", "t"));

        session
            .execute_in_database("analyze full table ice.db1.t", "default")
            .expect("ANALYZE FULL TABLE must resolve iceberg external table via 3-part name");

        assert!(engine.has_local_table("db1", "t"));
        assert_eq!(iceberg_column_stat_row_count(&session, "db1.t"), 2);
    }

    #[test]
    fn analyze_table_unknown_iceberg_table_errors() {
        // A genuinely missing iceberg table named explicitly by ANALYZE must
        // surface a hard error rather than silently succeeding.
        let warehouse = TempDir::new().expect("warehouse");
        let (_engine, session) = open_iceberg_session_with_table(&warehouse, "3");
        let err = session
            .execute_in_context("analyze table db1.does_not_exist", Some("ice"), "db1", None)
            .expect_err("ANALYZE of a missing iceberg table must fail");
        assert!(
            err.to_ascii_lowercase().contains("does_not_exist")
                || err.to_ascii_lowercase().contains("load iceberg table"),
            "expected a load/resolution error, got: {err}",
        );
    }

    #[test]
    fn select_resolves_iceberg_table_referenced_only_in_join_on_subquery() {
        // Engine gap #4 (join_apply_to_join q7): a table referenced ONLY inside
        // a subquery nested in a JOIN ON predicate must be registered into the
        // in-memory catalog at query-prep time, exactly like FROM/WHERE
        // subqueries. Before the fix the query-prep table-reference collection
        // did not descend into JOIN ON predicates, so `t2` was never
        // registered and the SELECT failed with `unknown table: t2`.
        let warehouse = TempDir::new().expect("warehouse");
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
        for (name, col) in [("t0", "v1"), ("t1", "v5"), ("t2", "v7")] {
            session
                .execute_in_database(
                    &format!(
                        r#"create table ice.db1.{name} ({col} bigint) tblproperties("format-version"="2")"#
                    ),
                    "default",
                )
                .expect("create table");
            session
                .execute_in_database(
                    &format!("insert into ice.db1.{name} values (1), (2), (3)"),
                    "default",
                )
                .expect("seed table");
        }

        // `t2` is referenced ONLY inside the ON-clause IN-subquery. Sanity:
        // it is not in the in-memory catalog before the SELECT runs.
        assert!(
            !engine.has_local_table("db1", "t2"),
            "iceberg table t2 must not be pre-registered before the SELECT",
        );

        let result = session
            .execute_in_context(
                "select count(*) from db1.t0 \
                 left join db1.t1 on v1 in (select v7 from db1.t2) or v1 < v5",
                Some("ice"),
                "db1",
                None,
            )
            .expect("SELECT with ON-clause subquery table must resolve, not error unknown table");

        // The ON-clause-only table is now materialized in the in-memory
        // catalog, and the query produced a count row.
        assert!(
            engine.has_local_table("db1", "t2"),
            "ON-clause-subquery table t2 must be registered during query-prep",
        );
        match result {
            StatementResult::Query(query_result) => {
                assert_eq!(
                    query_result.row_count(),
                    1,
                    "count(*) over a LEFT JOIN must return exactly one row",
                );
            }
            StatementResult::Ok => panic!("SELECT must return rows"),
        }
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

    fn current_live_data_file_first_row_ids(
        engine: &StandaloneNovaRocks,
        catalog: &str,
        namespace: &str,
        table: &str,
    ) -> Vec<Option<i64>> {
        let registry = engine.inner.iceberg_catalogs.read().expect("registry");
        let entry = registry.get(catalog).expect("entry");
        entry.invalidate_table_cache(namespace, table);
        let loaded =
            crate::connector::iceberg::catalog::load_table(&entry, namespace, table).expect("load");
        crate::connector::iceberg::catalog::registry::extract_data_files_with_stats(&loaded.table)
            .expect("extract data files")
            .into_iter()
            .map(|file| file.first_row_id)
            .collect()
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
        use crate::meta::repository::job::{IcebergOptimizeJobState, StoredIcebergOptimizeJob};

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
        let first_row_ids = current_live_data_file_first_row_ids(&engine, "ice", "db1", "t");
        assert!(
            !first_row_ids.is_empty(),
            "OPTIMIZE must leave at least one live data file"
        );
        assert!(
            first_row_ids.iter().all(Option::is_some),
            "OPTIMIZE preserve-mode data files must keep effective first_row_id values: {first_row_ids:?}"
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
            None,
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
        // source so the test does not depend on StarRocks table configuration.
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
        // tables, verified without needing StarRocks table config).
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

    #[test]
    fn select_with_datetime_literal_matches_microsecond_precision() {
        let _runtime_guard = lock_runtime_test_state();
        let Some((_dir, config_path, _metadata_db_path)) = maybe_starrocks_table_config() else {
            return;
        };

        let engine = StandaloneNovaRocks::open(StandaloneOptions {
            config_path: Some(config_path),
        })
        .expect("open engine");
        let session = engine.session();

        session
            .execute(
                "CREATE TABLE t_dt_coerce (c1 INT, c2 DATETIME) \
                 DUPLICATE KEY(c1) DISTRIBUTED BY HASH(c1) BUCKETS 1 \
                 PROPERTIES('replication_num'='1')",
            )
            .expect("create table");
        session
            .execute("INSERT INTO t_dt_coerce VALUES (4, '2020-01-01 00:00:00.012')")
            .expect("insert row");

        let r = session
            .query("SELECT c1 FROM t_dt_coerce WHERE c2 = '2020-01-01 00:00:00.012'")
            .expect("query with datetime literal");
        assert_eq!(
            r.row_count(),
            1,
            "implicit STRING→DATETIME coercion should match"
        );
    }

    #[test]
    fn select_with_datetime_literal_in_list_matches() {
        let _runtime_guard = lock_runtime_test_state();
        let Some((_dir, config_path, _metadata_db_path)) = maybe_starrocks_table_config() else {
            return;
        };
        let engine = StandaloneNovaRocks::open(StandaloneOptions {
            config_path: Some(config_path),
        })
        .expect("open engine");
        let session = engine.session();

        session
            .execute(
                "CREATE TABLE t_in_coerce (c1 INT, c2 DATETIME) \
                 DUPLICATE KEY(c1) DISTRIBUTED BY HASH(c1) BUCKETS 1 \
                 PROPERTIES('replication_num'='1')",
            )
            .expect("create");
        session
            .execute(
                "INSERT INTO t_in_coerce VALUES \
                 (1, '2020-01-01 00:00:00.001'), (2, '2020-01-01 00:00:00.002')",
            )
            .expect("insert");

        let r = session
            .query(
                "SELECT c1 FROM t_in_coerce \
                 WHERE c2 IN ('2020-01-01 00:00:00.001', '2020-01-01 00:00:00.002') \
                 ORDER BY c1",
            )
            .expect("in list query");
        assert_eq!(r.row_count(), 2);
    }

    // -----------------------------------------------------------------------
    // I1: dispatcher_for_role role-guard tests
    // -----------------------------------------------------------------------

    /// AllInOne role produces a dispatcher without error.
    #[test]
    fn dispatcher_for_role_all_in_one_ok() {
        use crate::common::app_config::ClusterRole;
        let result = super::dispatcher_for_role(ClusterRole::AllInOne, "127.0.0.1", 0);
        assert!(result.is_ok(), "AllInOne should produce a dispatcher");
    }

    #[test]
    fn dispatcher_for_role_fe_no_backend_configured_returns_error() {
        let _guard = super::acquire_standalone_test_guard();
        use crate::common::app_config::ClusterRole;
        crate::common::app_config::install_default_for_test();
        let result = super::dispatcher_for_role(ClusterRole::Fe, "127.0.0.1", 0);
        assert!(
            result.is_err(),
            "Fe role with no backends must return an error"
        );
        let msg = result.err().expect("expected error");
        assert!(
            msg.contains("role=fe"),
            "error must mention role=fe, got: {msg}"
        );
    }

    #[test]
    fn dispatcher_for_role_be_returns_error_instead_of_panicking() {
        use crate::common::app_config::ClusterRole;
        let result = super::dispatcher_for_role(ClusterRole::Be, "127.0.0.1", 0);
        assert!(result.is_err(), "Be role must return a recoverable error");
        let msg = result.err().expect("expected error");
        assert!(
            msg.contains("role=be") && msg.contains("coordinator"),
            "error must mention role=be and coordinator, got: {msg}"
        );
    }

    // --- PR-4 spec compliance tests ---

    /// Issue 2: FE role with a valid backend address returns a dispatcher.
    #[test]
    fn dispatcher_for_role_fe_valid_backend_returns_dispatcher() {
        let _guard = super::acquire_standalone_test_guard();
        use crate::common::app_config::{ClusterRole, NovaRocksConfig};
        let mut cfg = NovaRocksConfig::default();
        cfg.cluster.backends = vec!["127.0.0.1:9070".to_string()];
        crate::common::app_config::install_preloaded_config(cfg);
        let result = super::dispatcher_for_role(ClusterRole::Fe, "127.0.0.1", 0);
        assert!(
            result.is_ok(),
            "Fe with valid backend must return a dispatcher, got: {:?}",
            result.err()
        );
    }

    /// Issue 2: FE role with a malformed backend address returns an error that
    /// names both the role and the bad address value.
    #[test]
    fn dispatcher_for_role_fe_malformed_backend_returns_error_with_role_and_value() {
        let _guard = super::acquire_standalone_test_guard();
        use crate::common::app_config::{ClusterRole, NovaRocksConfig};
        let mut cfg = NovaRocksConfig::default();
        cfg.cluster.backends = vec!["not-an-addr".to_string()];
        crate::common::app_config::install_preloaded_config(cfg);
        let result = super::dispatcher_for_role(ClusterRole::Fe, "127.0.0.1", 0);
        assert!(result.is_err(), "malformed backend must return an error");
        let msg = result.err().expect("error");
        assert!(msg.contains("role=fe"), "must mention role=fe: {msg}");
        assert!(
            msg.contains("not-an-addr"),
            "must include the bad value: {msg}"
        );
    }

    /// Issue 4: FE role with more than one backend returns an error that
    /// includes the backend count.  Without the exactly-one guard the first
    /// backend would be silently accepted.
    #[test]
    fn dispatcher_for_role_fe_multiple_backends_returns_error_with_count() {
        let _guard = super::acquire_standalone_test_guard();
        use crate::common::app_config::{ClusterRole, NovaRocksConfig};
        let mut cfg = NovaRocksConfig::default();
        cfg.cluster.backends = vec!["127.0.0.1:9070".to_string(), "127.0.0.1:9071".to_string()];
        crate::common::app_config::install_preloaded_config(cfg);
        let result = super::dispatcher_for_role(ClusterRole::Fe, "127.0.0.1", 0);
        assert!(result.is_err(), "Fe with multiple backends must error");
        let msg = result.err().expect("expected error");
        assert!(msg.contains("role=fe"), "must mention role=fe: {msg}");
        assert!(msg.contains('2'), "must include count: {msg}");
    }
}
