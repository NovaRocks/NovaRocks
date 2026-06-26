#![allow(dead_code)]

use std::collections::{HashMap, HashSet};
use std::net::{TcpListener, TcpStream};
use std::path::{Path, PathBuf};
use std::sync::{Arc, OnceLock, RwLock};
use std::time::{Duration, Instant};

use arrow::array::StringArray;
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use tokio::runtime::Handle;

use crate::engine::mv::refresh_context::MvRefreshPruningLimits;
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
use crate::meta::repository::backend::BackendMetaRepository;
use crate::meta::repository::iceberg_catalog::{
    IcebergCatalogMetaRepository, IcebergCatalogProperties,
};
use crate::meta::repository::iceberg_operation::IcebergOperationRepository;
use crate::meta::repository::job::{
    IcebergOptimizeJobState, JobMetaRepository, StoredIcebergOptimizeJob,
};
use crate::meta::repository::mv::MvMetaRepository;
use crate::meta::repository::starrocks_table::StarRocksTableMetaRepository;
use crate::meta::repository::starrocks_txn::StarRocksTxnRepository;

pub(crate) mod aggregate;
pub(crate) mod backend_ops;
pub(crate) mod backend_resolver;
pub(crate) mod catalog;
pub(crate) mod catalog_mgr;
pub(crate) mod dictionary;
pub(crate) mod iceberg_ctas;
pub(crate) mod iceberg_maintenance;
pub(crate) mod iceberg_ref_flow;
pub(crate) mod iceberg_view;
pub(crate) mod iceberg_view_rewrite;
pub(crate) mod information_schema;
pub(crate) mod insert;
pub(crate) mod insert_flow;
pub(crate) mod mutation_flow;
pub(crate) mod mv;
pub(crate) mod mv_flow;
pub(crate) mod mv_maintenance;
pub(crate) mod mv_rewrite_prep;
pub(crate) mod mv_scheduler;
pub(crate) mod name_resolve;
pub(crate) mod parquet;
pub(crate) mod procedure;
pub(crate) mod query_prep;
mod query_stats;
pub(crate) mod sql_expr;
pub(crate) mod starrocks_table_ctas;
pub(crate) mod statement;
pub(crate) mod statistics;
pub(crate) mod stream_load;
pub(crate) mod view_rewrite;
pub(crate) mod virtual_table;
pub(crate) mod write_operation_lifecycle;
mod write_transaction;

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
    looks_like_show_create_view, looks_like_show_views, parse_add_legacy_range_partition_sql,
    parse_alter_iceberg_properties_sql, parse_alter_partition_column_sql,
    parse_alter_table_expire_snapshots_sql, parse_alter_table_optimize_sql,
    parse_alter_table_remove_orphan_files_sql, parse_alter_table_rewrite_manifests_sql,
    parse_show_alter_table_optimize_sql, parse_show_create_table,
};
use self::stream_load::{
    parse_csv_stream_load_rows, parse_json_stream_load_rows, parse_stream_load_columns,
};
use crate::engine::procedure::{looks_like_call_procedure, parse_call_procedure_sql};
use crate::engine::query_prep::{has_time_travel_refs, rewrite_time_travel_refs};

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

pub(crate) fn recover_starrocks_tablet_paths_from_current_engine(
    table: &crate::connector::starrocks::fe_v2_meta::LakeTableIdentity,
    tablet_ids: &[i64],
) -> Result<HashMap<i64, String>, String> {
    let Some(engine) = current_stream_load_engine() else {
        return recover_starrocks_tablet_paths_from_installed_config(table, tablet_ids);
    };
    recover_starrocks_tablet_paths_from_state(&engine.inner, table, tablet_ids)
}

pub(crate) fn recover_starrocks_tablet_paths_from_installed_config(
    table: &crate::connector::starrocks::fe_v2_meta::LakeTableIdentity,
    tablet_ids: &[i64],
) -> Result<HashMap<i64, String>, String> {
    if tablet_ids.is_empty() {
        return Ok(HashMap::new());
    }

    let cfg = match novarocks_config::config() {
        Ok(cfg) => cfg,
        Err(_) => return Ok(HashMap::new()),
    };
    let Some(metadata) = cfg.metadata.as_ref() else {
        return Ok(HashMap::new());
    };
    let Some(standalone) = cfg.standalone_server.as_ref() else {
        return Ok(HashMap::new());
    };
    let Some(app_cfg) = standalone.starrocks_table_config()? else {
        return Ok(HashMap::new());
    };
    let starrocks_table_config = StarRocksTableConfig::from_app_config(app_cfg)?;
    let provider = open_metadata_provider(&ResolvedMetadataBackend {
        provider: metadata.provider,
        path: metadata.path.clone(),
    })?;
    let read = provider.begin_read().map_err(|e| {
        format!("open StarRocks table metadata recovery read transaction failed: {e}")
    })?;
    let snapshot = StarRocksTableMetaRepository
        .load_snapshot(read.as_ref())
        .map_err(|e| {
            format!("load StarRocks table metadata during tablet path recovery failed: {e}")
        })?;
    let rebuilt = StarRocksTableCatalog::rebuild_from_repository(
        Some(starrocks_table_config.clone()),
        snapshot,
    )?;
    let paths = select_starrocks_tablet_paths_from_catalog(&rebuilt, table, tablet_ids)?;
    register_starrocks_shard_infos(&starrocks_table_config.s3, &paths);
    Ok(paths)
}

pub(crate) fn recover_starrocks_tablet_paths_from_state(
    state: &Arc<StandaloneState>,
    table: &crate::connector::starrocks::fe_v2_meta::LakeTableIdentity,
    tablet_ids: &[i64],
) -> Result<HashMap<i64, String>, String> {
    if tablet_ids.is_empty() {
        return Ok(HashMap::new());
    }

    let mut paths = {
        let catalog = state
            .starrocks_table
            .read()
            .expect("standalone StarRocks table read lock");
        select_starrocks_tablet_paths_from_catalog(&catalog, table, tablet_ids)?
    };
    if starrocks_tablet_paths_cover(tablet_ids, &paths) {
        register_starrocks_shard_infos_from_paths(state, &paths);
        return Ok(paths);
    }

    let Some(provider) = state.metadata_provider.as_ref() else {
        register_starrocks_shard_infos_from_paths(state, &paths);
        return Ok(paths);
    };

    let read = provider.begin_read().map_err(|e| {
        format!("open StarRocks table metadata recovery read transaction failed: {e}")
    })?;
    let snapshot = state
        .starrocks_table_repo
        .load_snapshot(read.as_ref())
        .map_err(|e| {
            format!("load StarRocks table metadata during tablet path recovery failed: {e}")
        })?;
    let rebuilt = StarRocksTableCatalog::rebuild_from_repository(
        state.starrocks_table_config.clone(),
        snapshot.clone(),
    )?;
    let recovered = select_starrocks_tablet_paths_from_catalog(&rebuilt, table, tablet_ids)?;
    if !recovered.is_empty() {
        register_starrocks_shard_infos_from_paths(state, &recovered);
        paths.extend(recovered);
    }

    {
        let mut catalog = state
            .catalog
            .write()
            .expect("standalone catalog write lock");
        for database in &snapshot.databases {
            catalog.create_database(&database.name)?;
        }
        register_starrocks_tables_in_catalog(&mut catalog, &rebuilt)?;
    }
    let mut guard = state
        .starrocks_table
        .write()
        .expect("standalone StarRocks table write lock");
    *guard = rebuilt;

    Ok(paths)
}

fn select_starrocks_tablet_paths_from_catalog(
    catalog: &StarRocksTableCatalog,
    table: &crate::connector::starrocks::fe_v2_meta::LakeTableIdentity,
    tablet_ids: &[i64],
) -> Result<HashMap<i64, String>, String> {
    let requested = tablet_ids.iter().copied().collect::<HashSet<_>>();
    let Some(runtime) = catalog
        .runtime_by_table_id(table.table_id)
        .or_else(|| catalog.table(&table.db_name, &table.table_name).ok())
    else {
        return Ok(HashMap::new());
    };
    let mut paths = HashMap::with_capacity(requested.len());
    for tablet in &runtime.tablets {
        if requested.contains(&tablet.tablet_id) {
            paths.insert(tablet.tablet_id, tablet.tablet_root_path.clone());
        }
    }
    Ok(paths)
}

fn starrocks_tablet_paths_cover(tablet_ids: &[i64], paths: &HashMap<i64, String>) -> bool {
    tablet_ids.iter().all(|tablet_id| {
        paths
            .get(tablet_id)
            .is_some_and(|path| !path.trim().is_empty())
    })
}

fn register_starrocks_shard_infos_from_paths(
    state: &StandaloneState,
    paths: &HashMap<i64, String>,
) -> usize {
    let Some(config) = state.starrocks_table_config.as_ref() else {
        return 0;
    };
    register_starrocks_shard_infos(&config.s3, paths)
}

fn register_starrocks_shard_infos(
    s3: &crate::runtime::starlet_shard_registry::S3StoreConfig,
    paths: &HashMap<i64, String>,
) -> usize {
    if paths.is_empty() {
        return 0;
    }
    crate::runtime::starlet_shard_registry::upsert_many_infos(paths.iter().map(
        |(tablet_id, full_path)| {
            (
                *tablet_id,
                crate::runtime::starlet_shard_registry::StarletShardInfo {
                    full_path: full_path.clone(),
                    s3: Some(s3.clone()),
                },
            )
        },
    ))
}

pub(crate) fn catalog_mgr_snapshot(state: &Arc<StandaloneState>) -> catalog_mgr::CatalogMgr {
    state
        .catalog_mgr
        .read()
        .expect("catalog mgr read lock")
        .clone()
}

pub(crate) fn build_analyzer_provider<'a>(
    current_catalog: Option<&'a str>,
    catalog: &'a InMemoryCatalog,
    catalog_mgr: &'a catalog_mgr::CatalogMgr,
    connectors: &'a crate::connector::ConnectorRegistry,
    mode: crate::sql::catalog::TableLookupMode,
) -> catalog_mgr::provider::CatalogMgrProvider<'a> {
    catalog_mgr::provider::CatalogMgrProvider::new(
        current_catalog,
        catalog,
        catalog_mgr,
        connectors,
        mode,
    )
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
    pub(crate) catalog: Arc<RwLock<InMemoryCatalog>>,
    pub(crate) catalog_mgr: RwLock<catalog_mgr::CatalogMgr>,
    pub(crate) iceberg_catalogs: Arc<RwLock<IcebergCatalogRegistry>>,
    pub(crate) starrocks_table: RwLock<StarRocksTableCatalog>,
    pub(crate) statistics: RwLock<statistics::StandaloneStatistics>,
    pub(crate) connectors: Arc<RwLock<crate::connector::ConnectorRegistry>>,
    pub(crate) starrocks_table_config: Option<StarRocksTableConfig>,
    pub(crate) mv_refresh_pruning_limits: MvRefreshPruningLimits,
    pub(crate) metadata_provider: Option<Arc<dyn crate::meta::MetaStoreProvider>>,
    pub(crate) backend_repo: BackendMetaRepository,
    pub(crate) starrocks_table_repo: StarRocksTableMetaRepository,
    pub(crate) starrocks_txn_repo: StarRocksTxnRepository,
    pub(crate) mv_repo: MvMetaRepository,
    pub(crate) iceberg_catalog_repo: IcebergCatalogMetaRepository,
    pub(crate) iceberg_operation_repo: IcebergOperationRepository,
    pub(crate) job_repo: JobMetaRepository,
    pub(crate) dictionary_manager: dictionary::DictionaryManager,
    pub(crate) exchange_port: u16,
    /// Wake-up channel for the iceberg maintenance coordinator; injected by
    /// the server after the coordinator thread starts, None otherwise.
    pub(crate) maintenance_signal_tx: std::sync::Mutex<
        Option<std::sync::mpsc::Sender<crate::engine::mv_maintenance::MaintenanceSignal>>,
    >,
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
        let catalog = Arc::new(RwLock::new(InMemoryCatalog::default()));
        let mut catalog_mgr = catalog_mgr::CatalogMgr::new();
        catalog_mgr.register(Arc::new(catalog_mgr::internal::InternalCatalog::new(
            "default_catalog",
            Arc::clone(&catalog),
        )));
        Self {
            catalog,
            catalog_mgr: RwLock::new(catalog_mgr),
            iceberg_catalogs: Arc::new(RwLock::new(IcebergCatalogRegistry::default())),
            starrocks_table: RwLock::new(StarRocksTableCatalog::default()),
            statistics: RwLock::new(statistics::StandaloneStatistics::default()),
            connectors: Arc::new(RwLock::new(crate::connector::ConnectorRegistry::default())),
            starrocks_table_config: None,
            mv_refresh_pruning_limits: MvRefreshPruningLimits::default(),
            metadata_provider: None,
            backend_repo: BackendMetaRepository,
            starrocks_table_repo: StarRocksTableMetaRepository,
            starrocks_txn_repo: StarRocksTxnRepository,
            mv_repo: MvMetaRepository,
            iceberg_catalog_repo: IcebergCatalogMetaRepository,
            iceberg_operation_repo: IcebergOperationRepository,
            job_repo: JobMetaRepository,
            dictionary_manager: dictionary::DictionaryManager::default(),
            exchange_port: 0,
            maintenance_signal_tx: std::sync::Mutex::new(None),
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
        #[cfg(test)]
        crate::runtime::backend_registry::replace_backend_registry_for_test(None);
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
        #[cfg(test)]
        crate::runtime::backend_registry::replace_backend_registry_for_test(None);
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
        // role=fe dispatches all fragments to registered BEs and must not
        // start a local gRPC/exchange server. All-in-one binds the local
        // exchange server and registers it as a loopback BE.
        let cfg =
            crate::novarocks_config::config().map_err(|e| format!("read config failed: {e}"))?;
        let role = cfg.cluster.role;
        let exchange_port = match role {
            crate::common::app_config::ClusterRole::Fe => {
                // Sentinel: non-zero to allow coordinated execution, but no local socket is bound.
                u16::MAX
            }
            crate::common::app_config::ClusterRole::Be
            | crate::common::app_config::ClusterRole::AllInOne => {
                ensure_standalone_exchange_server()?
            }
        };
        if role == crate::common::app_config::ClusterRole::AllInOne {
            let endpoint: std::net::SocketAddr = format!("127.0.0.1:{exchange_port}")
                .parse()
                .map_err(|e| format!("parse all-in-one loopback backend endpoint failed: {e}"))?;
            backend_ops::install_all_in_one_backend_registry(
                endpoint,
                cfg.cluster.heartbeat_timeout_retries,
            )?;
        }
        let metadata_backend = resolve_metadata_backend(&opts)?;
        let metadata_provider = metadata_backend
            .as_ref()
            .map(open_metadata_provider)
            .transpose()?;
        let starrocks_table_config = resolve_starrocks_table_config()?;
        let mv_refresh_pruning_limits = resolve_mv_refresh_pruning_limits()?;
        let catalog = Arc::new(RwLock::new(InMemoryCatalog::default()));
        let mut catalog_mgr = catalog_mgr::CatalogMgr::new();
        catalog_mgr.register(Arc::new(catalog_mgr::internal::InternalCatalog::new(
            "default_catalog",
            Arc::clone(&catalog),
        )));
        let inner = Arc::new(StandaloneState {
            catalog,
            catalog_mgr: RwLock::new(catalog_mgr),
            starrocks_table: RwLock::new(StarRocksTableCatalog::empty(
                starrocks_table_config.clone(),
            )),
            starrocks_table_config,
            mv_refresh_pruning_limits,
            metadata_provider,
            backend_repo: BackendMetaRepository,
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
        if role == crate::common::app_config::ClusterRole::Fe {
            backend_ops::ensure_backend_registry(&inner)?;
            backend_ops::wait_for_configured_backends_live(&inner)?;
        }
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
        if looks_like_call_procedure(&normalized) {
            let stmt = parse_call_procedure_sql(&normalized)?;
            let request = crate::engine::iceberg_maintenance::MaintenanceActionRequest::from_call(
                &stmt,
                current_database,
            )?;
            return crate::engine::iceberg_maintenance::execute_maintenance_action(
                &self.inner,
                request,
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
                use crate::sql::parser::dialect::backend::{
                    looks_like_add_backend, looks_like_drop_backend, looks_like_show_backends,
                };
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
                    || looks_like_add_backend(peek_parser)
                    || looks_like_drop_backend(peek_parser)
                    || looks_like_show_backends(peek_parser)
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
        let (parse_sql, forced_explain_level, force_logical_explain) =
            if let Some((rewritten, level)) = split_explain_logical_sql(&normalized) {
                (rewritten, Some(level), true)
            } else if let Some((rewritten, level)) = split_explain_costs_sql(&normalized) {
                (rewritten, Some(level), false)
            } else {
                (normalized.clone(), None, false)
            };

        if let Some(parsed) = parse_explain_refresh_materialized_view(&normalized) {
            let (stmt, level, analyze) = parsed?;
            if analyze {
                return Err(
                    "EXPLAIN ANALYZE REFRESH MATERIALIZED VIEW is not supported".to_string()
                );
            }
            let lines =
                crate::engine::mv::iceberg_refresh::explain_iceberg_mv_refresh_rewrite_plan(
                    &self.inner,
                    current_catalog,
                    current_database,
                    &stmt,
                    level,
                )?;
            return Ok(StatementResult::Query(build_string_query_result(
                "Explain String",
                lines,
            )?));
        }

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

        // SHOW CREATE VIEW ...
        if looks_like_show_create_view(&normalized) {
            return self.handle_show_create_view(&normalized, current_catalog, current_database);
        }

        // SHOW VIEWS [FROM db]
        if looks_like_show_views(&normalized) {
            return self.handle_show_views(&normalized, current_catalog, current_database);
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
                let catalog_mgr_snapshot = catalog_mgr_snapshot(&self.inner);
                let analyzer_provider = build_analyzer_provider(
                    current_catalog,
                    &catalog_snapshot,
                    &catalog_mgr_snapshot,
                    &connectors_snapshot,
                    crate::sql::catalog::TableLookupMode::ExplainStats,
                );
                let result = if force_logical_explain {
                    explain_logical_query(&prepared, &analyzer_provider, current_database, level)?
                } else {
                    explain_query(
                        &prepared,
                        &analyzer_provider,
                        &catalog_snapshot,
                        &connectors_snapshot,
                        current_database,
                        level,
                        Some(&self.inner),
                    )?
                };
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
                let catalog_mgr_snapshot = catalog_mgr_snapshot(&self.inner);
                let analyzer_provider = build_analyzer_provider(
                    current_catalog,
                    &catalog_snapshot,
                    &catalog_mgr_snapshot,
                    &connectors_snapshot,
                    crate::sql::catalog::TableLookupMode::ExplainStats,
                );
                let result = explain_analyze_query(
                    &prepared,
                    &analyzer_provider,
                    &catalog_snapshot,
                    &connectors_snapshot,
                    current_database,
                    None,
                    Some(&self.inner),
                )?;
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
                // remaining rewrites see only base tables. `expand_views_in_query`
                // is a no-op when no views are registered.
                let mut prepared = query.as_ref().clone();
                self::view_rewrite::expand_views_in_query(
                    &mut prepared,
                    &self.inner.views,
                    current_database,
                );
                // Inline iceberg-catalog views (REST only). Runs after session
                // views so local definitions keep precedence.
                self::iceberg_view_rewrite::expand_iceberg_views_in_query(
                    &self.inner,
                    &mut prepared,
                    current_catalog,
                    current_database,
                )?;
                // Materialize information_schema virtual tables (e.g. `schemata`)
                // into VALUES-backed derived tables. Run after view expansion
                // because a view may project from a virtual table.
                self::virtual_table::rewrite_query(&self.inner, &mut prepared)?;

                // Time-travel: `SELECT ... FROM t FOR VERSION AS OF <v>`.
                // Rewrite version-bearing table refs to synthetic per-snapshot
                // names and register only those synthetic TableDefs. Ordinary
                // Iceberg refs are resolved by CatalogMgrProvider during analysis.
                if has_time_travel_refs(&prepared) {
                    rewrite_time_travel_refs(
                        &self.inner,
                        current_catalog,
                        current_database,
                        &mut prepared,
                    )?;
                }

                // Clone-then-release: do not hold the catalog read lock
                // across pipeline execution. Pipeline execution can run for
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
                let catalog_mgr_snapshot = catalog_mgr_snapshot(&self.inner);
                let analyzer_provider = build_analyzer_provider(
                    current_catalog,
                    &catalog_snapshot,
                    &catalog_mgr_snapshot,
                    &connectors_snapshot,
                    crate::sql::catalog::TableLookupMode::SchemaOnly,
                );
                self::statistics::observe_query(&self.inner, &prepared, current_database)?;
                let result = execute_query_with_catalog_provider(
                    &prepared,
                    &analyzer_provider,
                    &catalog_snapshot,
                    &connectors_snapshot,
                    current_database,
                    self.inner.exchange_port,
                    query_opts.clone(),
                    Some(&self.inner),
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
        if self.inner.metadata_provider.is_none() {
            return Err("ALTER TABLE OPTIMIZE requires metadata provider".to_string());
        }
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
        let request = crate::engine::iceberg_maintenance::MaintenanceActionRequest {
            source: crate::engine::iceberg_maintenance::MaintenanceActionSource::LegacyAlter,
            kind: crate::engine::iceberg_maintenance::MaintenanceActionKind::RewriteDataFiles,
            catalog: target.catalog,
            namespace: target.namespace,
            table: target.table,
            options: crate::engine::iceberg_maintenance::MaintenanceActionOptions::default(),
            older_than_ms: None,
            retain_last: None,
            use_caching: None,
            spec_id: None,
            branch: None,
            where_clause: None,
        };
        crate::engine::iceberg_maintenance::execute_maintenance_action(&self.inner, request)
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

    fn handle_show_create_view(
        &self,
        sql: &str,
        current_catalog: Option<&str>,
        current_database: &str,
    ) -> Result<StatementResult, String> {
        let view_name = crate::engine::statement::parse_show_create_view(sql)?;
        let Some(target) = crate::engine::iceberg_view::resolve_iceberg_view_target_parts(
            &self.inner,
            &view_name.parts,
            current_catalog,
            current_database,
        )?
        else {
            return Err("SHOW CREATE VIEW only supports views in iceberg catalogs".to_string());
        };
        let backend = self
            .inner
            .connectors
            .read()
            .expect("connector registry read")
            .catalog_backend("iceberg")?;
        let view = backend.load_view(&target.catalog, &target.namespace, &target.view)?;

        let columns = view
            .column_names
            .iter()
            .map(|name| format!("`{name}`"))
            .collect::<Vec<_>>()
            .join(", ");
        let mut ddl = format!(
            "CREATE VIEW `{}`.`{}`.`{}` ({})",
            target.catalog, target.namespace, target.view, columns
        );
        if let Some(comment) = &view.comment {
            ddl.push_str(&format!("\nCOMMENT \"{}\"", comment.replace('"', "\\\"")));
        }
        ddl.push_str(&format!("\nAS {};", view.sql));

        let fields = vec![
            Field::new("View", DataType::Utf8, false),
            Field::new("Create View", DataType::Utf8, false),
        ];
        let arrays: Vec<Arc<dyn arrow::array::Array>> = vec![
            Arc::new(StringArray::from(vec![target.view.clone()])),
            Arc::new(StringArray::from(vec![ddl])),
        ];
        let batch = RecordBatch::try_new(Arc::new(Schema::new(fields)), arrays)
            .map_err(|e| format!("build SHOW CREATE VIEW result failed: {e}"))?;
        Ok(StatementResult::Query(QueryResult {
            columns: vec![
                QueryResultColumn {
                    name: "View".to_string(),
                    data_type: DataType::Utf8,
                    nullable: false,
                    logical_type: None,
                },
                QueryResultColumn {
                    name: "Create View".to_string(),
                    data_type: DataType::Utf8,
                    nullable: false,
                    logical_type: None,
                },
            ],
            chunks: vec![record_batch_to_chunk(batch)?],
        }))
    }

    fn handle_show_views(
        &self,
        sql: &str,
        current_catalog: Option<&str>,
        current_database: &str,
    ) -> Result<StatementResult, String> {
        let from_db = crate::engine::statement::parse_show_views(sql)?;
        let db = from_db.as_deref().unwrap_or(current_database);
        let session_catalog =
            current_catalog.filter(|catalog| !catalog.eq_ignore_ascii_case("default_catalog"));
        let names: Vec<String> = match session_catalog {
            Some(catalog) => {
                let backend = self
                    .inner
                    .connectors
                    .read()
                    .expect("connector registry read")
                    .catalog_backend("iceberg")?;
                backend.list_views(catalog, db)?
            }
            None => {
                let views = self
                    .inner
                    .views
                    .read()
                    .map_err(|e| format!("view registry read lock: {e}"))?;
                let db_lower = db.to_ascii_lowercase();
                let mut names: Vec<String> = views
                    .keys()
                    .filter(|(database, _)| database == &db_lower)
                    .map(|(_, view)| view.clone())
                    .collect();
                names.sort();
                names
            }
        };
        let column_name = format!("Views_in_{db}");
        let fields = vec![Field::new(column_name.clone(), DataType::Utf8, false)];
        let arrays: Vec<Arc<dyn arrow::array::Array>> = vec![Arc::new(StringArray::from(names))];
        let batch = RecordBatch::try_new(Arc::new(Schema::new(fields)), arrays)
            .map_err(|e| format!("build SHOW VIEWS result failed: {e}"))?;
        Ok(StatementResult::Query(QueryResult {
            columns: vec![QueryResultColumn {
                name: column_name,
                data_type: DataType::Utf8,
                nullable: false,
                logical_type: None,
            }],
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
        crate::connector::register_iceberg_catalog_mgr_entry(&self.inner, &stmt.name)?;
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
            | Type::Primitive(PrimitiveType::Timestamptz) => "DATETIME".to_string(),
            Type::Primitive(PrimitiveType::TimestampNs)
            | Type::Primitive(PrimitiveType::TimestamptzNs) => "TIMESTAMP_NS".to_string(),
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
        Statement::AddBackend(stmt) => crate::engine::backend_ops::execute_add_backend(state, stmt),
        Statement::DropBackend(stmt) => {
            crate::engine::backend_ops::execute_drop_backend(state, stmt)
        }
        Statement::ShowBackends(_) => crate::engine::backend_ops::execute_show_backends(state),
    }
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

fn resolve_mv_refresh_pruning_limits() -> Result<MvRefreshPruningLimits, String> {
    let cfg = novarocks_config::config().map_err(|e| format!("read config failed: {e}"))?;
    Ok(cfg
        .standalone_server
        .as_ref()
        .map(MvRefreshPruningLimits::from_standalone_config)
        .unwrap_or_default())
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
            crate::connector::register_iceberg_catalog_mgr_entry(state, &catalog.catalog)?;
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

use crate::sql::codegen::{MultiFragmentBuildResult, PlanBuildResult};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum DirectExecutionReason {
    RuntimeLocalTerminalSink,
    RuntimeLocalIcebergRegistry,
    UnitTestNoExchangeBackend,
}

fn direct_execution_reason(
    has_terminal_sink: bool,
    has_iceberg_catalogs: bool,
    exchange_port: u16,
) -> Option<DirectExecutionReason> {
    if has_terminal_sink {
        return Some(DirectExecutionReason::RuntimeLocalTerminalSink);
    }
    if has_iceberg_catalogs {
        return Some(DirectExecutionReason::RuntimeLocalIcebergRegistry);
    }
    if exchange_port == 0 {
        return Some(DirectExecutionReason::UnitTestNoExchangeBackend);
    }
    None
}

/// Convert the one-fragment result required by explicit direct-execution
/// exceptions. This is not an ordinary query fast path.
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
        boundary_schemas: fragment.boundary_schemas,
        query_global_dicts: fragment.query_global_dicts,
        query_global_dict_exprs: fragment.query_global_dict_exprs,
    }))
}

#[allow(clippy::too_many_arguments)]
fn execute_query_direct_for_explicit_exception(
    mut physical: crate::sql::optimizer::PhysicalPlanNode,
    codegen_catalog: &dyn crate::sql::catalog::CatalogProvider,
    connectors: &crate::connector::ConnectorRegistry,
    current_database: &str,
    query_opts: Option<crate::internal_service::TQueryOptions>,
    terminal_sink: Option<Box<dyn crate::exec::pipeline::operator_factory::OperatorFactory>>,
    iceberg_catalogs: Option<&crate::connector::iceberg::catalog::IcebergCatalogRegistry>,
    mv_refresh_ctx: Option<&crate::engine::mv::refresh_context::IcebergMvRefreshContext>,
    reason: DirectExecutionReason,
) -> Result<QueryResult, String> {
    physical = collapse_distribution_enforcers_for_single_fragment(physical);
    let build_result = if let Some(mv_refresh_ctx) = mv_refresh_ctx {
        crate::sql::codegen::fragment_builder::PlanFragmentBuilder::build_via_distributed_plan_with_mv_refresh_ctx(
            &physical,
            codegen_catalog,
            connectors,
            current_database,
            Some(mv_refresh_ctx),
        )?
    } else {
        crate::sql::codegen::fragment_builder::PlanFragmentBuilder::build_via_distributed_plan(
            &physical,
            codegen_catalog,
            connectors,
            current_database,
        )?
    };
    let plan = single_fragment_plan(build_result).map_err(|_| {
        format!("direct execution exception {reason:?} produced a multi-fragment plan")
    })?;
    execute_plan(*plan, query_opts, terminal_sink, iceberg_catalogs, None)
}

fn aggregate_delta_row_ids_for_position_locator(
    layout: &crate::connector::starrocks::table::mv_agg_state::AggregateMvLayout,
    chunks: &[crate::exec::chunk::Chunk],
) -> Result<Vec<String>, String> {
    use arrow::array::Array;

    let mut row_ids = std::collections::BTreeSet::new();
    let row_id_column = &layout.row_id_column.column.name;
    for chunk in chunks {
        let schema = chunk.batch.schema();
        let row_id_index = schema.index_of(row_id_column).map_err(|e| {
            format!("iceberg aggregate delta missing row id column `{row_id_column}`: {e}")
        })?;
        let row_id_array = chunk
            .batch
            .column(row_id_index)
            .as_any()
            .downcast_ref::<arrow::array::StringArray>()
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
    Ok(row_ids.into_iter().collect())
}

fn bind_scan_ranges_to_target_positions(
    scan_ranges: &mut Vec<crate::internal_service::TScanRangeParams>,
    positions_by_file: &std::collections::BTreeMap<String, Vec<i64>>,
    matched_files: &mut std::collections::BTreeSet<String>,
) {
    let mut retained_position_files = std::collections::BTreeSet::new();
    scan_ranges.retain_mut(|params| {
        let Some(hdfs_range) = params.scan_range.hdfs_scan_range.as_mut() else {
            return true;
        };
        let path = hdfs_range
            .full_path
            .as_deref()
            .or(hdfs_range.relative_path.as_deref());
        let Some(path) = path else {
            return true;
        };
        let Some(positions) = positions_by_file.get(path) else {
            return false;
        };
        if !retained_position_files.insert(path.to_string()) {
            return false;
        }
        hdfs_range.offset = Some(0);
        if let Some(file_length) = hdfs_range.file_length {
            hdfs_range.length = Some(file_length.max(0));
        }
        hdfs_range.included_positions = Some(positions.clone());
        matched_files.insert(path.to_string());
        true
    });
}

fn bind_plan_build_result_hdfs_positions(
    result: &mut PlanBuildResult,
    matched_positions: &[crate::engine::mv::iceberg_target_apply::TargetRowPositionSet],
    target: &str,
) -> Result<(), String> {
    let mut positions_by_file = std::collections::BTreeMap::<String, Vec<i64>>::new();
    for set in matched_positions {
        if set.positions.is_empty() {
            continue;
        }
        positions_by_file
            .entry(set.referenced_data_file.clone())
            .or_default()
            .extend(set.positions.iter().copied());
    }
    for positions in positions_by_file.values_mut() {
        positions.sort_unstable();
        positions.dedup();
    }

    let mut matched_files = std::collections::BTreeSet::new();
    for scan_ranges in result.exec_params.per_node_scan_ranges.values_mut() {
        bind_scan_ranges_to_target_positions(scan_ranges, &positions_by_file, &mut matched_files);
    }
    if let Some(per_driver) = result
        .exec_params
        .node_to_per_driver_seq_scan_ranges
        .as_mut()
    {
        for driver_ranges in per_driver.values_mut() {
            for scan_ranges in driver_ranges.values_mut() {
                bind_scan_ranges_to_target_positions(
                    scan_ranges,
                    &positions_by_file,
                    &mut matched_files,
                );
            }
        }
    }

    let missing = positions_by_file
        .keys()
        .filter(|path| !matched_files.contains(*path))
        .cloned()
        .collect::<Vec<_>>();
    if !missing.is_empty() {
        return Err(format!(
            "Iceberg target-state scan {target} locator returned positions for files not present in old-state scan ranges: [{}]",
            missing.join(", ")
        ));
    }
    Ok(())
}

fn bind_aggregate_old_input_positions_from_delta_preview(
    old_input: &mut PlanBuildResult,
    delta_input: &PlanBuildResult,
    layout: &crate::connector::starrocks::table::mv_agg_state::AggregateMvLayout,
    pruning_limits: crate::engine::mv::refresh_context::MvRefreshPruningLimits,
    locator: &crate::sql::codegen::AggregateStateTargetPositionLocator,
    query_opts: Option<&crate::internal_service::TQueryOptions>,
    iceberg_catalogs: Option<&crate::connector::iceberg::catalog::IcebergCatalogRegistry>,
) -> Result<(), String> {
    let target = locator.target_table.identifier().to_string();
    let delta_preview = match execute_plan(
        delta_input.clone(),
        query_opts.cloned(),
        None,
        iceberg_catalogs,
        None,
    ) {
        Ok(preview) => preview,
        Err(err) => {
            tracing::warn!(
                target = %target,
                error = %err,
                fallback_reason = "delta_preview",
                "falling back to unpositioned aggregate old-state scan"
            );
            return Ok(());
        }
    };
    let row_ids = match aggregate_delta_row_ids_for_position_locator(layout, &delta_preview.chunks)
    {
        Ok(row_ids) => row_ids,
        Err(err) => {
            tracing::warn!(
                target = %target,
                error = %err,
                fallback_reason = "delta_row_id",
                "falling back to unpositioned aggregate old-state scan"
            );
            return Ok(());
        }
    };
    if row_ids.is_empty() {
        return bind_plan_build_result_hdfs_positions(old_input, &[], &target);
    }
    if pruning_limits.touched_group_count_exceeds_limit(row_ids.len()) {
        tracing::warn!(
            target = %target,
            touched_group_count = row_ids.len(),
            max_touched_groups = pruning_limits.max_touched_groups,
            fallback_reason = "touched_group_threshold",
            "falling back to full aggregate old-state scan because touched group count exceeds configured threshold"
        );
        return Ok(());
    }

    let (existing_deletes_by_file, referenced_data_file_partitions) =
        match crate::engine::mv::iceberg_target_apply::load_target_apply_locator_inputs(
            &locator.target_entry,
            &locator.target_table,
        ) {
            Ok(inputs) => inputs,
            Err(err) => {
                tracing::warn!(
                    target = %target,
                    error = %err,
                    fallback_reason = "target_locator_input",
                    "falling back to unpositioned aggregate old-state scan"
                );
                return Ok(());
            }
        };
    let locator_result = match crate::runtime::global_async_runtime::data_block_on(
        crate::engine::mv::iceberg_target_apply::locate_target_rows_by_string_apply_key_with_matches(
            &locator.target_table,
            &locator.apply_key_column,
            &row_ids,
            &existing_deletes_by_file,
            &referenced_data_file_partitions,
            &locator.partition_filter,
        ),
    ) {
        Ok(Ok(result)) => result,
        Ok(Err(err)) | Err(err) => {
            tracing::warn!(
                target = %target,
                error = %err,
                fallback_reason = "target_locator",
                "falling back to unpositioned aggregate old-state scan"
            );
            return Ok(());
        }
    };
    if let Err(err) =
        bind_plan_build_result_hdfs_positions(old_input, &locator_result.matched_positions, &target)
    {
        tracing::warn!(
            target = %target,
            error = %err,
            fallback_reason = "position_binding",
            "falling back to unpositioned aggregate old-state scan"
        );
    }
    Ok(())
}

fn collapse_distribution_enforcers_for_single_fragment(
    mut node: crate::sql::optimizer::PhysicalPlanNode,
) -> crate::sql::optimizer::PhysicalPlanNode {
    use crate::sql::optimizer::operator::{JoinDistribution, Operator};
    use crate::sql::optimizer::physical_plan::JoinExecutionDistribution;

    node.children = node
        .children
        .into_iter()
        .map(collapse_distribution_enforcers_for_single_fragment)
        .collect();

    if let Operator::PhysicalHashJoin(join) = &mut node.op {
        join.distribution = JoinDistribution::Broadcast;
        node.execution_props.join_distribution = Some(JoinExecutionDistribution::Broadcast);
        for runtime_filter in &mut node.build_runtime_filters {
            runtime_filter.distribution = JoinDistribution::Broadcast;
        }
    }

    if matches!(&node.op, Operator::PhysicalDistribution(_)) && node.children.len() == 1 {
        return node.children.into_iter().next().expect("single child");
    }

    node
}

/// Live BE count for broadcast fanout (1 FE + N BE distributed baseline).
/// Precedence: live BE registry > [runtime] config (>0) > defensive 1.0.
/// Err / empty registry / misconfig collapse to the config tier or the 1.0
/// floor (never panics). NOT a standalone-specific branch: all-in-one is a
/// test shell that registers one loopback BE; goldens pin SET to the real N.
fn live_effective_backend_count() -> f64 {
    if let Some(registry) = crate::runtime::backend_registry::backend_registry() {
        let entries = registry.live_endpoints();
        if !entries.is_empty() {
            return (entries.len() as f64).max(1.0);
        }
    }
    let configured = crate::common::config::optimizer_effective_backend_count();
    if configured > 0 {
        return configured as f64;
    }
    1.0
}

/// Fold the live BE count into the TLS session settings before optimize(),
/// unless the session explicitly SET cbo_broadcast_backend_count (which wins
/// in from_session). Idempotent; safe to call before every optimize() site.
fn snapshot_effective_backend_count_into_session() {
    let mut snapshot = crate::sql::optimizer::options::current_session_optimizer_settings();
    if snapshot.cbo_broadcast_backend_count.is_none() {
        snapshot.effective_backend_count = Some(live_effective_backend_count());
        crate::sql::optimizer::options::install_session_optimizer_settings(snapshot);
    }
}

/// Common preparation pipeline shared by `EXPLAIN` and `EXPLAIN ANALYZE`:
/// inline user-defined views and rewrite time-travel refs. Ordinary Iceberg
/// table resolution is handled by the analyzer provider.
fn prepare_explain_query(
    state: &Arc<StandaloneState>,
    current_catalog: Option<&str>,
    current_database: &str,
    query: &sqlparser::ast::Query,
) -> Result<sqlparser::ast::Query, String> {
    // Inline any user-defined views before the analyzer sees the query.
    let mut prepared = query.clone();
    self::view_rewrite::expand_views_in_query(&mut prepared, &state.views, current_database);
    // Inline iceberg-catalog views (REST only). Runs after session
    // views so local definitions keep precedence.
    self::iceberg_view_rewrite::expand_iceberg_views_in_query(
        state,
        &mut prepared,
        current_catalog,
        current_database,
    )?;

    // Time-travel refs become synthetic local tables. Ordinary Iceberg refs
    // remain untouched and resolve through CatalogMgrProvider during analysis.
    if has_time_travel_refs(&prepared) {
        rewrite_time_travel_refs(state, current_catalog, current_database, &mut prepared)?;
    }

    Ok(prepared)
}

/// Execute the DistributedPlan, then produce an EXPLAIN-style result whose
/// first row is `Planning: <ms> / Execution: <ms> / Rows: <N>` followed by
/// the profiled plan body.
#[allow(clippy::too_many_arguments)]
fn explain_analyze_query(
    query: &sqlparser::ast::Query,
    analyzer_catalog: &dyn crate::sql::catalog::CatalogProvider,
    codegen_catalog: &InMemoryCatalog,
    connectors: &crate::connector::ConnectorRegistry,
    current_database: &str,
    query_opts: Option<crate::internal_service::TQueryOptions>,
    mv_rewrite_state: Option<&Arc<StandaloneState>>,
) -> Result<QueryResult, String> {
    use crate::sql::codegen::ir::explain_distributed_plan_analyze;
    use crate::sql::explain::ExplainLevel;
    use crate::sql::planner::build_distributed_plan;

    let planning_start = std::time::Instant::now();
    let (resolved, cte_registry, mut factory) =
        crate::sql::analyzer::analyze(query, analyzer_catalog, current_database)?;
    let logical = crate::sql::planner::plan_query(resolved, cte_registry, &mut factory)?;
    let mut scalar_arena = crate::sql::optimizer::scalar::ScalarArena::new();
    let mut opt_expr = crate::sql::planner::optimizer_bridge::plan::try_logical_plan_to_opt_expr(
        &logical,
        &mut scalar_arena,
    )?;
    let providers = mv_rewrite_state
        .map(query_stats::QueryStatsProviders::from_standalone_state)
        .unwrap_or_else(|| query_stats::QueryStatsProviders::from_connectors(connectors));
    let mut query_stats = query_stats::QueryStatsCollector::new(providers).collect(&mut opt_expr);
    let mv_candidates = match mv_rewrite_state {
        Some(state) => crate::engine::mv_rewrite_prep::prepare_mv_rewrite_candidates(
            state,
            analyzer_catalog,
            current_database,
            &logical,
            &mut factory,
            &mut query_stats,
        ),
        None => Vec::new(),
    };
    snapshot_effective_backend_count_into_session();
    let physical = crate::sql::optimizer::optimize(
        opt_expr,
        scalar_arena,
        &query_stats.snapshot,
        factory,
        None,
        mv_candidates,
    )?;

    let dp = build_distributed_plan(&physical)?;
    let build_result =
        crate::sql::codegen::fragment_builder::PlanFragmentBuilder::build_via_distributed_plan(
            &physical,
            codegen_catalog,
            connectors,
            current_database,
        )?;
    let planning_elapsed = planning_start.elapsed();

    let mut query_opts = query_opts.unwrap_or_default();
    query_opts.enable_profile = Some(true);
    let (dispatcher, scheduler) = coordinated_execution_services()?;
    let execution_start = std::time::Instant::now();
    let outcome = crate::runtime::coordinator::ExecutionCoordinator::new(
        build_result,
        dispatcher,
        scheduler,
        Some(query_opts),
    )
    .execute_with_profile_outcome()?;
    let execution_elapsed = execution_start.elapsed();
    if let Some(abort) = outcome.write_abort.as_ref() {
        return Err(abort.reason.clone());
    }
    if outcome.fragment_profiles.is_empty() {
        return Err("EXPLAIN ANALYZE completed without fragment runtime profiles".to_string());
    }

    let actuals =
        crate::runtime::profile_correlate::collect_actuals_by_plan_node_id_from_profile_trees(
            &outcome.fragment_profiles,
        );
    let profile_summary =
        crate::runtime::profile_correlate::collect_distributed_profile_summary_from_profile_trees(
            &outcome.fragment_profiles,
        );
    let per_fragment = crate::runtime::profile_correlate::collect_per_fragment_profile_summaries(
        &outcome.fragment_profiles,
    );
    let mut lines = Vec::new();
    lines.push(format!(
        "Planning: {} / Execution: {} / Rows: {}",
        format_explain_analyze_duration(planning_elapsed),
        format_explain_analyze_duration(execution_elapsed),
        outcome.query_result.row_count()
    ));
    lines.push(format_distributed_profile_summary(&profile_summary));
    lines.extend(explain_distributed_plan_analyze(
        &dp,
        ExplainLevel::Analyze,
        &actuals,
        Some(&per_fragment),
    ));
    build_string_query_result("Explain String", lines)
}

fn format_distributed_profile_summary(
    summary: &crate::runtime::profile_correlate::DistributedProfileSummary,
) -> String {
    format!(
        "Profile: fragments={} fragment_wall_max={} fragment_wall_sum={} driver_total={} driver_blocked={} source_wait={} sink_wait={} dependency_wait={} operator_active={} exchange_wait={} exchange_process={} network={} scan_io={}",
        summary.fragment_instance_count,
        format_explain_analyze_duration_ns(summary.fragment_wall_max_ns),
        format_explain_analyze_duration_ns(summary.fragment_wall_sum_ns),
        format_explain_analyze_duration_ns(summary.driver_total_time_ns),
        format_explain_analyze_duration_ns(summary.driver_blocked_time_ns),
        format_explain_analyze_duration_ns(summary.source_wait_time_ns),
        format_explain_analyze_duration_ns(summary.sink_wait_time_ns),
        format_explain_analyze_duration_ns(summary.dependency_wait_time_ns),
        format_explain_analyze_duration_ns(summary.operator_active_time_ns),
        format_explain_analyze_duration_ns(summary.exchange_wait_time_ns),
        format_explain_analyze_duration_ns(summary.exchange_process_time_ns),
        format_explain_analyze_duration_ns(summary.network_time_ns),
        format_explain_analyze_duration_ns(summary.scan_io_time_ns)
    )
}

fn format_explain_analyze_duration_ns(ns: i64) -> String {
    format_explain_analyze_duration(std::time::Duration::from_nanos(ns.max(0) as u64))
}

fn format_explain_analyze_duration(duration: std::time::Duration) -> String {
    let ms = duration.as_secs_f64() * 1000.0;
    if ms < 1.0 {
        format!("{ms:.3}ms")
    } else if ms < 1000.0 {
        format!("{ms:.1}ms")
    } else {
        format!("{:.2}s", duration.as_secs_f64())
    }
}

/// Produce non-distributed logical EXPLAIN output for a query without
/// optimizing or building the DistributedPlan IR.
fn explain_logical_query(
    query: &sqlparser::ast::Query,
    analyzer_catalog: &dyn crate::sql::catalog::CatalogProvider,
    current_database: &str,
    level: crate::sql::explain::ExplainLevel,
) -> Result<QueryResult, String> {
    let (resolved, cte_registry, mut factory) =
        crate::sql::analyzer::analyze(query, analyzer_catalog, current_database)?;
    let logical = crate::sql::planner::plan_query(resolved, cte_registry, &mut factory)?;
    let lines = crate::sql::explain::explain_plan_checked(&logical, level)?;
    build_string_query_result("Explain String", lines)
}

/// Produce EXPLAIN output for a query without executing it.
fn explain_query(
    query: &sqlparser::ast::Query,
    analyzer_catalog: &dyn crate::sql::catalog::CatalogProvider,
    _codegen_catalog: &InMemoryCatalog,
    connectors: &crate::connector::ConnectorRegistry,
    current_database: &str,
    level: crate::sql::explain::ExplainLevel,
    mv_rewrite_state: Option<&Arc<StandaloneState>>,
) -> Result<QueryResult, String> {
    use crate::sql::codegen::ir::explain_distributed_plan;
    use crate::sql::explain::ExplainLevel;
    use crate::sql::planner::build_distributed_plan;

    let (resolved, cte_registry, mut factory) =
        crate::sql::analyzer::analyze(query, analyzer_catalog, current_database)?;
    let logical = crate::sql::planner::plan_query(resolved, cte_registry, &mut factory)?;
    let mut scalar_arena = crate::sql::optimizer::scalar::ScalarArena::new();
    let mut opt_expr = crate::sql::planner::optimizer_bridge::plan::try_logical_plan_to_opt_expr(
        &logical,
        &mut scalar_arena,
    )?;
    let providers = mv_rewrite_state
        .map(query_stats::QueryStatsProviders::from_standalone_state)
        .unwrap_or_else(|| query_stats::QueryStatsProviders::from_connectors(connectors));
    let mut query_stats = query_stats::QueryStatsCollector::new(providers).collect(&mut opt_expr);
    // MV query rewrite candidate prep (plain EXPLAIN has no MV refresh
    // context, so the gate is only `mv_rewrite_state.is_some()`).
    let mv_candidates = match mv_rewrite_state {
        Some(state) => crate::engine::mv_rewrite_prep::prepare_mv_rewrite_candidates(
            state,
            analyzer_catalog,
            current_database,
            &logical,
            &mut factory,
            &mut query_stats,
        ),
        None => Vec::new(),
    };
    // dictionary_provider intentionally None; installed via TLS by execute_in_context.
    snapshot_effective_backend_count_into_session();
    let physical = crate::sql::optimizer::optimize(
        opt_expr,
        scalar_arena,
        &query_stats.snapshot,
        factory,
        None,
        mv_candidates,
    )?;

    let mut lines = Vec::new();
    if matches!(level, ExplainLevel::Costs) {
        lines.extend(query_stats.snapshot.display_rows());
    }
    let dp = build_distributed_plan(&physical)?;
    lines.extend(explain_distributed_plan(&dp, level));

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
    execute_query_with_catalog_provider(
        query,
        catalog,
        catalog,
        connectors,
        current_database,
        exchange_port,
        query_opts,
        None,
    )
}

pub(crate) fn execute_query_with_catalog_mgr(
    state: &Arc<StandaloneState>,
    current_catalog: Option<&str>,
    current_database: &str,
    query: &sqlparser::ast::Query,
    query_opts: Option<crate::internal_service::TQueryOptions>,
) -> Result<QueryResult, String> {
    let catalog_snapshot = state
        .catalog
        .read()
        .expect("standalone catalog read lock")
        .clone();
    let connectors_snapshot = state
        .connectors
        .read()
        .expect("standalone connector registry read lock")
        .clone();
    let catalog_mgr_snapshot = catalog_mgr_snapshot(state);
    let analyzer_provider = build_analyzer_provider(
        current_catalog,
        &catalog_snapshot,
        &catalog_mgr_snapshot,
        &connectors_snapshot,
        crate::sql::catalog::TableLookupMode::SchemaOnly,
    );
    execute_query_with_catalog_provider(
        query,
        &analyzer_provider,
        &catalog_snapshot,
        &connectors_snapshot,
        current_database,
        state.exchange_port,
        query_opts,
        Some(state),
    )
}

pub(crate) type IcebergWriteRootDistributionResolver = Box<
    dyn FnOnce(
        &crate::sql::planner::plan::LogicalPlanNode,
    ) -> Result<Option<crate::sql::optimizer::property::DistributionSpec>, String>,
>;

#[allow(dead_code)]
pub(crate) fn iceberg_write_shuffle_by_output_name(
    output_name: impl Into<String>,
) -> IcebergWriteRootDistributionResolver {
    let output_name = output_name.into();
    Box::new(move |logical| {
        let output_columns = crate::sql::planner::plan_output_columns(logical)?;
        let mut matches = output_columns
            .iter()
            .filter(|column| column.name == output_name);
        let column = matches.next().ok_or_else(|| {
            format!(
                "cannot derive Iceberg write root shuffle: output column '{output_name}' not found"
            )
        })?;
        if matches.next().is_some() {
            return Err(format!(
                "cannot derive Iceberg write root shuffle: output column '{output_name}' is ambiguous"
            ));
        }
        iceberg_write_shuffle_for_output_column(column)
    })
}

#[allow(dead_code)]
pub(crate) fn iceberg_write_shuffle_by_output_index(
    output_index: usize,
) -> IcebergWriteRootDistributionResolver {
    Box::new(move |logical| {
        let output_columns = crate::sql::planner::plan_output_columns(logical)?;
        let column = output_columns.get(output_index).ok_or_else(|| {
            format!(
                "cannot derive Iceberg write root shuffle: output column index {output_index} out of range ({} columns)",
                output_columns.len()
            )
        })?;
        iceberg_write_shuffle_for_output_column(column)
    })
}

fn iceberg_write_shuffle_for_output_column(
    column: &crate::sql::analysis::OutputColumn,
) -> Result<Option<crate::sql::optimizer::property::DistributionSpec>, String> {
    if column.column_id == crate::sql::column_id::ColumnId::UNSET {
        return Err(format!(
            "cannot derive Iceberg write root shuffle: output column '{}' has no ColumnId",
            column.name
        ));
    }
    Ok(Some(
        crate::sql::optimizer::property::DistributionSpec::shuffle_agg([column.column_id]),
    ))
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn execute_query_as_iceberg_write(
    state: &Arc<StandaloneState>,
    current_catalog: Option<&str>,
    current_database: &str,
    query: &sqlparser::ast::Query,
    sink_spec: crate::sql::codegen::iceberg_write_sink::IcebergWriteSinkSpec,
    query_opts: Option<crate::internal_service::TQueryOptions>,
    root_distribution_resolver: Option<IcebergWriteRootDistributionResolver>,
) -> Result<crate::runtime::coordinator::CoordinatedQueryResult, String> {
    // Time-travel: a branch DML write's scan carries `FOR VERSION AS OF '<branch>'`
    // (delete_flow's DV position scan; the MOR-UPDATE branch row scan). Resolve those
    // version-bearing refs to synthetic per-snapshot tables bound to the BRANCH head
    // BEFORE snapshotting the catalog, exactly as the read path does. Without this the
    // analyzer silently drops the version clause and the scan reads the table's current
    // (main) snapshot, so a branch DELETE/UPDATE finds rows in the wrong data files and
    // no-ops on the branch. No-op when the query has no version ref (INSERT / main
    // writes), so those paths are unchanged.
    let mut prepared = query.clone();
    if has_time_travel_refs(&prepared) {
        rewrite_time_travel_refs(state, current_catalog, current_database, &mut prepared)?;
    }

    let catalog_snapshot = state
        .catalog
        .read()
        .expect("standalone catalog read lock")
        .clone();
    let connectors_snapshot = state
        .connectors
        .read()
        .expect("standalone connector registry read lock")
        .clone();
    let catalog_mgr_snapshot = catalog_mgr_snapshot(state);
    let analyzer_provider = build_analyzer_provider(
        current_catalog,
        &catalog_snapshot,
        &catalog_mgr_snapshot,
        &connectors_snapshot,
        crate::sql::catalog::TableLookupMode::SchemaOnly,
    );

    let (resolved, cte_registry, mut factory) =
        crate::sql::analyzer::analyze(&prepared, &analyzer_provider, current_database)?;
    let logical = crate::sql::planner::plan_query(resolved, cte_registry, &mut factory)?;
    let root_distribution = match root_distribution_resolver {
        Some(resolve_root_distribution) => resolve_root_distribution(&logical)?,
        None => None,
    };
    let mut scalar_arena = crate::sql::optimizer::scalar::ScalarArena::new();
    let mut opt_expr = crate::sql::planner::optimizer_bridge::plan::try_logical_plan_to_opt_expr(
        &logical,
        &mut scalar_arena,
    )?;
    let providers = query_stats::QueryStatsProviders::from_standalone_state(state);
    let query_stats = query_stats::QueryStatsCollector::new(providers).collect(&mut opt_expr);
    snapshot_effective_backend_count_into_session();
    let physical = match root_distribution {
        Some(root_distribution) => crate::sql::optimizer::optimize_with_root_distribution(
            opt_expr,
            scalar_arena,
            &query_stats.snapshot,
            factory,
            root_distribution,
        )?,
        None => crate::sql::optimizer::optimize(
            opt_expr,
            scalar_arena,
            &query_stats.snapshot,
            factory,
            None,
            Vec::new(),
        )?,
    };
    let build_result =
        crate::sql::codegen::fragment_builder::PlanFragmentBuilder::build_via_distributed_plan_with_iceberg_sink(
            &physical,
            &catalog_snapshot,
            &connectors_snapshot,
            current_database,
            None,
            &sink_spec,
        )?;
    let (dispatcher, scheduler) = coordinated_execution_services()?;
    crate::runtime::coordinator::ExecutionCoordinator::new(
        build_result,
        dispatcher,
        scheduler,
        query_opts,
    )
    .execute_with_write_outcome()
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn execute_query_with_catalog_provider(
    query: &sqlparser::ast::Query,
    analyzer_catalog: &dyn crate::sql::catalog::CatalogProvider,
    codegen_catalog: &InMemoryCatalog,
    connectors: &crate::connector::ConnectorRegistry,
    current_database: &str,
    exchange_port: u16,
    query_opts: Option<crate::internal_service::TQueryOptions>,
    mv_rewrite_state: Option<&Arc<StandaloneState>>,
) -> Result<QueryResult, String> {
    execute_query_with_options_and_imv_validator_with_catalog_provider(
        query,
        analyzer_catalog,
        codegen_catalog,
        connectors,
        current_database,
        exchange_port,
        query_opts,
        None,
        None,
        None,
        None,
        mv_rewrite_state,
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
pub(crate) type ImvRewriteValidator<'a> = dyn Fn(&crate::sql::planner::imv_rewrite::entrypoint::ImvRewriteOutcome) -> Result<(), String>
    + 'a;

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
    execute_query_with_options_and_imv_validator(
        query,
        catalog,
        connectors,
        current_database,
        exchange_port,
        query_opts,
        terminal_sink,
        iceberg_catalogs,
        mv_refresh_ctx,
        None,
        None,
    )
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn execute_query_with_options_and_imv_validator(
    query: &sqlparser::ast::Query,
    catalog: &InMemoryCatalog,
    connectors: &crate::connector::ConnectorRegistry,
    current_database: &str,
    exchange_port: u16,
    query_opts: Option<crate::internal_service::TQueryOptions>,
    terminal_sink: Option<Box<dyn crate::exec::pipeline::operator_factory::OperatorFactory>>,
    iceberg_catalogs: Option<&crate::connector::iceberg::catalog::IcebergCatalogRegistry>,
    mv_refresh_ctx: Option<&crate::engine::mv::refresh_context::IcebergMvRefreshContext>,
    imv_rewrite_validator: Option<&ImvRewriteValidator<'_>>,
    mv_rewrite_state: Option<&Arc<StandaloneState>>,
) -> Result<QueryResult, String> {
    execute_query_with_options_and_imv_validator_with_catalog_provider(
        query,
        catalog,
        catalog,
        connectors,
        current_database,
        exchange_port,
        query_opts,
        terminal_sink,
        iceberg_catalogs,
        mv_refresh_ctx,
        imv_rewrite_validator,
        mv_rewrite_state,
    )
}

#[allow(clippy::too_many_arguments)]
fn execute_query_with_options_and_imv_validator_with_catalog_provider(
    query: &sqlparser::ast::Query,
    analyzer_catalog: &dyn crate::sql::catalog::CatalogProvider,
    codegen_catalog: &InMemoryCatalog,
    connectors: &crate::connector::ConnectorRegistry,
    current_database: &str,
    exchange_port: u16,
    query_opts: Option<crate::internal_service::TQueryOptions>,
    terminal_sink: Option<Box<dyn crate::exec::pipeline::operator_factory::OperatorFactory>>,
    iceberg_catalogs: Option<&crate::connector::iceberg::catalog::IcebergCatalogRegistry>,
    mv_refresh_ctx: Option<&crate::engine::mv::refresh_context::IcebergMvRefreshContext>,
    imv_rewrite_validator: Option<&ImvRewriteValidator<'_>>,
    mv_rewrite_state: Option<&Arc<StandaloneState>>,
) -> Result<QueryResult, String> {
    let (resolved, cte_registry, mut factory) =
        crate::sql::analyzer::analyze(query, analyzer_catalog, current_database)?;
    let mut logical = crate::sql::planner::plan_query(resolved, cte_registry, &mut factory)?;
    if let Some(mv_ctx) = mv_refresh_ctx {
        logical = crate::engine::mv::iceberg_refresh::normalize_imv_rewrite_root_project(logical);
        let outcome = crate::sql::planner::imv_rewrite::entrypoint::run_imv_rewrite(
            crate::sql::planner::imv_rewrite::entrypoint::ImvRewriteInput {
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
        if let Some(validator) = imv_rewrite_validator {
            validator(&outcome)?;
        }
        logical = outcome.plan;
    } else if imv_rewrite_validator.is_some() {
        return Err("IMV rewrite validator requires MV refresh context".to_string());
    }
    let mut scalar_arena = crate::sql::optimizer::scalar::ScalarArena::new();
    let mut opt_expr = crate::sql::planner::optimizer_bridge::plan::try_logical_plan_to_opt_expr(
        &logical,
        &mut scalar_arena,
    )?;
    let providers = mv_rewrite_state
        .map(query_stats::QueryStatsProviders::from_standalone_state)
        .unwrap_or_else(|| query_stats::QueryStatsProviders::from_connectors(connectors));
    let mut query_stats = query_stats::QueryStatsCollector::new(providers).collect(&mut opt_expr);
    // MV query rewrite: discover fresh Iceberg MV candidates and inject their
    // target-table stats. Gated on a standalone-rewrite path (`Some(state)`)
    // and disabled during MV refresh (`mv_refresh_ctx.is_some()`) so refresh
    // queries never rewrite onto the MV they are computing.
    let mv_candidates = match mv_rewrite_state {
        Some(state) if mv_refresh_ctx.is_none() => {
            crate::engine::mv_rewrite_prep::prepare_mv_rewrite_candidates(
                state,
                analyzer_catalog,
                current_database,
                &logical,
                &mut factory,
                &mut query_stats,
            )
        }
        _ => Vec::new(),
    };
    // dictionary_provider intentionally None; installed via TLS by execute_in_context.
    snapshot_effective_backend_count_into_session();
    let physical = crate::sql::optimizer::optimize(
        opt_expr,
        scalar_arena,
        &query_stats.snapshot,
        factory,
        None,
        mv_candidates,
    )?;

    if let Some(reason) = direct_execution_reason(
        terminal_sink.is_some(),
        iceberg_catalogs.is_some(),
        exchange_port,
    ) {
        return execute_query_direct_for_explicit_exception(
            physical,
            codegen_catalog,
            connectors,
            current_database,
            query_opts,
            terminal_sink,
            iceberg_catalogs,
            mv_refresh_ctx,
            reason,
        );
    }

    let build_result = if let Some(mv_refresh_ctx) = mv_refresh_ctx {
        crate::sql::codegen::fragment_builder::PlanFragmentBuilder::build_via_distributed_plan_with_mv_refresh_ctx(
            &physical,
            codegen_catalog,
            connectors,
            current_database,
            Some(mv_refresh_ctx),
        )?
    } else {
        crate::sql::codegen::fragment_builder::PlanFragmentBuilder::build_via_distributed_plan(
            &physical,
            codegen_catalog,
            connectors,
            current_database,
        )?
    };
    let (dispatcher, scheduler) = coordinated_execution_services()?;
    crate::runtime::coordinator::ExecutionCoordinator::new(
        build_result,
        dispatcher,
        scheduler,
        query_opts,
    )
    .execute()
}

fn coordinated_execution_services() -> Result<
    (
        Arc<dyn crate::runtime::dispatcher::FragmentDispatcher>,
        Arc<crate::runtime::scheduler::FragmentScheduler>,
    ),
    String,
> {
    use crate::common::app_config::ClusterRole;
    let role = crate::novarocks_config::config()
        .map(|c| c.cluster.role)
        .unwrap_or(ClusterRole::AllInOne);
    let (dispatcher, scheduler) = match role {
        ClusterRole::Fe | ClusterRole::AllInOne => {
            let entries = backend_ops::live_backend_dispatch_entries()?;
            let dispatcher = Arc::new(
                crate::runtime::dispatcher::RemoteDispatcher::new_with_backend_ids(&entries)?,
            );
            let scheduler = Arc::new(
                crate::runtime::scheduler::FragmentScheduler::new_with_backend_ids(entries),
            );
            (
                dispatcher as Arc<dyn crate::runtime::dispatcher::FragmentDispatcher>,
                scheduler,
            )
        }
        ClusterRole::Be => {
            return Err("role=be must not enter standalone coordinator".into());
        }
    };
    Ok((dispatcher, scheduler))
}

/// Select a `FragmentDispatcher` implementation based on the effective cluster role.
///
/// - `AllInOne` and `Fe`: use `RemoteDispatcher` bound to live registry backends.
/// - `Be`: standalone coordinator must not be entered when the process is a pure BE.
pub(crate) fn dispatcher_for_role(
    role: crate::common::app_config::ClusterRole,
) -> Result<Arc<dyn crate::runtime::dispatcher::FragmentDispatcher>, String> {
    use crate::common::app_config::ClusterRole;
    match role {
        ClusterRole::Fe => {
            let entries = backend_ops::live_backend_dispatch_entries()
                .map_err(|e| with_fe_error_context(e))?;
            Ok(Arc::new(
                crate::runtime::dispatcher::RemoteDispatcher::new_with_backend_ids(&entries)?,
            ))
        }
        ClusterRole::AllInOne => {
            let entries = backend_ops::live_backend_dispatch_entries()?;
            Ok(Arc::new(
                crate::runtime::dispatcher::RemoteDispatcher::new_with_backend_ids(&entries)?,
            ))
        }
        ClusterRole::Be => Err("role=be must not enter standalone coordinator".to_string()),
    }
}

fn with_fe_error_context(err: String) -> String {
    if err.starts_with("role=fe:") {
        err
    } else {
        format!("role=fe: {err}")
    }
}

#[cfg(test)]
pub(crate) fn dispatcher_kind_for_test(
    dispatcher: &Arc<dyn crate::runtime::dispatcher::FragmentDispatcher>,
) -> &'static str {
    if dispatcher
        .as_any()
        .is::<crate::runtime::dispatcher::RemoteDispatcher>()
    {
        "remote"
    } else {
        "unknown"
    }
}

#[cfg(test)]
pub(crate) struct StandaloneLoopbackTestBackend {
    pub(crate) exchange_port: u16,
    _registry_guard: crate::runtime::backend_registry::BackendRegistryTestGuard,
    _test_guard: TestSerializationGuard,
}

#[cfg(test)]
pub(crate) fn install_all_in_one_loopback_backend_for_test()
-> Result<StandaloneLoopbackTestBackend, String> {
    let test_guard = acquire_standalone_test_guard();
    let registry_guard = crate::runtime::backend_registry::BackendRegistryTestGuard::new();
    let cfg = crate::novarocks_config::install_default_for_test();
    let exchange_port = ensure_standalone_exchange_server()?;
    let endpoint: std::net::SocketAddr = format!("127.0.0.1:{exchange_port}")
        .parse()
        .map_err(|e| format!("parse all-in-one test loopback endpoint failed: {e}"))?;
    backend_ops::install_all_in_one_backend_registry(
        endpoint,
        cfg.cluster.heartbeat_timeout_retries,
    )?;
    Ok(StandaloneLoopbackTestBackend {
        exchange_port,
        _registry_guard: registry_guard,
        _test_guard: test_guard,
    })
}

fn ensure_standalone_exchange_server() -> Result<u16, String> {
    static STANDALONE_EXCHANGE_PORT: OnceLock<u16> = OnceLock::new();

    if let Some(port) = STANDALONE_EXCHANGE_PORT.get() {
        return Ok(*port);
    }

    let default_port = crate::common::config::grpc_port();
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
                branch_id,
                pruning_limits,
                target_position_locator,
            } => {
                let mut old_input = *old_input;
                let delta_input = *delta_input;
                if let Some(locator) = target_position_locator.as_ref() {
                    bind_aggregate_old_input_positions_from_delta_preview(
                        &mut old_input,
                        &delta_input,
                        &layout,
                        pruning_limits,
                        locator,
                        query_opts,
                        iceberg_catalogs,
                    )?;
                }
                let old_input =
                    lower_plan_build_result(old_input, arena, query_opts, iceberg_catalogs)?;
                let delta_input =
                    lower_plan_build_result(delta_input, arena, query_opts, iceberg_catalogs)?;
                return Ok(
                    crate::sql::codegen::nodes::build_aggregate_state_merge_exec_node(
                        old_input,
                        delta_input,
                        layout,
                        branch_id,
                        pruning_limits,
                    ),
                );
            }
            crate::sql::codegen::DirectExecPlan::AggregateStatePhysicalize { input, layout } => {
                let input = lower_plan_build_result(*input, arena, query_opts, iceberg_catalogs)?;
                return Ok(crate::exec::node::ExecNode {
                    kind: crate::exec::node::ExecNodeKind::AggregateStatePhysicalize(
                        crate::exec::operators::aggregate_state_merge::AggregateStatePhysicalizePlan {
                            input: Box::new(input),
                            layout,
                        },
                    ),
                });
            }
            crate::sql::codegen::DirectExecPlan::UnionAll { inputs } => {
                let inputs = inputs
                    .into_iter()
                    .map(|input| {
                        lower_plan_build_result(input, arena, query_opts, iceberg_catalogs)
                    })
                    .collect::<Result<Vec<_>, _>>()?;
                return Ok(crate::exec::node::ExecNode {
                    kind: crate::exec::node::ExecNodeKind::UnionAll(
                        crate::exec::node::union_all::UnionAllNode { inputs, node_id: 0 },
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
    profiler: Option<crate::runtime::profile::Profiler>,
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

    // Unified pipeline DOP: a per-session `SET pipeline_dop = N` override (on TQueryOptions) is
    // honored; otherwise auto = cores/2 via the shared exec_env helper (no hardcoded min(4) cap).
    let session_dop = query_opts
        .as_ref()
        .and_then(|opts| opts.pipeline_dop)
        .unwrap_or(0);
    let pipeline_dop = crate::runtime::exec_env::calc_pipeline_dop(session_dop) as usize;
    execute_plan_with_pipeline(
        exec_plan,
        false,
        std::time::Duration::from_millis(10),
        sink,
        None,
        profiler,
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
    let body = consume_leading_keyword(consume_leading_keyword(sql, "EXPLAIN")?, "COSTS")?;
    Some((
        format!("EXPLAIN {}", body.trim_start()),
        crate::sql::explain::ExplainLevel::Costs,
    ))
}

fn split_explain_logical_sql(sql: &str) -> Option<(String, crate::sql::explain::ExplainLevel)> {
    let mut body = consume_leading_keyword(consume_leading_keyword(sql, "EXPLAIN")?, "LOGICAL")?;
    let mut level = crate::sql::explain::ExplainLevel::Normal;
    for (keyword, candidate) in [
        ("VERBOSE", crate::sql::explain::ExplainLevel::Verbose),
        ("COSTS", crate::sql::explain::ExplainLevel::Costs),
    ] {
        if let Some(rest) = consume_leading_keyword(body, keyword) {
            level = candidate;
            body = rest;
            break;
        }
    }

    Some((format!("EXPLAIN {}", body.trim_start()), level))
}

fn consume_leading_keyword<'a>(sql: &'a str, keyword: &str) -> Option<&'a str> {
    let trimmed = sql.trim_start();
    let head = trimmed.as_bytes().get(..keyword.len())?;
    if !head.eq_ignore_ascii_case(keyword.as_bytes()) {
        return None;
    }

    let rest = &trimmed[keyword.len()..];
    if rest
        .chars()
        .next()
        .is_some_and(|ch| !ch.is_ascii_whitespace())
    {
        return None;
    }
    Some(rest)
}

fn parse_explain_refresh_materialized_view(
    sql: &str,
) -> Option<
    Result<
        (
            crate::sql::parser::ast::RefreshMaterializedViewStmt,
            crate::sql::explain::ExplainLevel,
            bool,
        ),
        String,
    >,
> {
    let trimmed = sql.trim_start();
    let prefixes = [
        (
            "EXPLAIN ANALYZE REFRESH ",
            crate::sql::explain::ExplainLevel::Analyze,
            true,
        ),
        (
            "EXPLAIN VERBOSE REFRESH ",
            crate::sql::explain::ExplainLevel::Verbose,
            false,
        ),
        (
            "EXPLAIN COSTS REFRESH ",
            crate::sql::explain::ExplainLevel::Costs,
            false,
        ),
        (
            "EXPLAIN REFRESH ",
            crate::sql::explain::ExplainLevel::Normal,
            false,
        ),
    ];
    for (prefix, level, analyze) in prefixes {
        if trimmed
            .as_bytes()
            .get(..prefix.len())
            .is_some_and(|head| head.eq_ignore_ascii_case(prefix.as_bytes()))
        {
            let body = format!("REFRESH {}", trimmed[prefix.len()..].trim_start());
            let mut statements = match crate::sql::parser::parse_sql(&body) {
                Ok(statements) => statements,
                Err(e) => return Some(Err(e)),
            };
            let Some(statement) = statements.pop() else {
                return Some(Err("EXPLAIN REFRESH parsed no statement".to_string()));
            };
            let crate::sql::parser::ast::Statement::RefreshMaterializedView(stmt) = statement
            else {
                return Some(Err(
                    "EXPLAIN REFRESH only supports REFRESH MATERIALIZED VIEW".to_string(),
                ));
            };
            return Some(Ok((stmt, level, analyze)));
        }
    }
    None
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
        StatementResult, dispatch_statement, recover_starrocks_tablet_paths_from_installed_config,
        recover_starrocks_tablet_paths_from_state, register_connector_backends,
    };
    use crate::connector::starrocks::fe_v2_meta::LakeTableIdentity;
    use crate::connector::starrocks::lake::context::lock_runtime_test_state;
    use crate::connector::starrocks::table::config::StarRocksTableConfig;
    use crate::meta::MetaStoreProvider;
    use crate::sql::planner::plan::*;
    use arrow::array::{
        Array, FixedSizeBinaryArray, Int32Array, Int64Array, ListArray, StringArray,
    };
    use arrow::datatypes::{DataType, Field, Schema};
    use std::path::PathBuf;
    use std::sync::Arc;
    use tempfile::TempDir;

    fn string_cell(result: &QueryResult, row: usize, col: usize) -> String {
        let batch = &result.chunks[0].batch;
        let array = batch
            .column(col)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("StringArray column");
        array.value(row).to_string()
    }

    fn string_column(result: &QueryResult, col: usize) -> Vec<String> {
        let mut out = Vec::new();
        for chunk in &result.chunks {
            let array = chunk
                .batch
                .column(col)
                .as_any()
                .downcast_ref::<StringArray>()
                .expect("StringArray column");
            for row in 0..array.len() {
                out.push(array.value(row).to_string());
            }
        }
        out
    }

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

    fn test_starrocks_table_config() -> StarRocksTableConfig {
        StarRocksTableConfig {
            warehouse_uri: "s3://test/warehouse".to_string(),
            s3: crate::runtime::starlet_shard_registry::S3StoreConfig {
                endpoint: "http://127.0.0.1:9000".to_string(),
                bucket: "test".to_string(),
                access_key_id: "ak".to_string(),
                access_key_secret: "sk".to_string(),
                region: None,
                enable_path_style_access: Some(true),
            },
            mv_default_storage_engine: "starrocks".to_string(),
        }
    }

    fn hdfs_scan_range_params(
        full_path: &str,
        offset: i64,
        length: i64,
        file_length: i64,
    ) -> crate::internal_service::TScanRangeParams {
        let mut hdfs_range = crate::plan_nodes::THdfsScanRange::default();
        hdfs_range.full_path = Some(full_path.to_string());
        hdfs_range.offset = Some(offset);
        hdfs_range.length = Some(length);
        hdfs_range.file_length = Some(file_length);
        crate::internal_service::TScanRangeParams::new(
            crate::plan_nodes::TScanRange::new(
                None::<crate::plan_nodes::TInternalScanRange>,
                None::<Vec<u8>>,
                None::<crate::plan_nodes::TBrokerScanRange>,
                None::<crate::plan_nodes::TEsScanRange>,
                Some(hdfs_range),
                None::<crate::plan_nodes::TBinlogScanRange>,
                None::<crate::plan_nodes::TBenchmarkScanRange>,
            ),
            None::<i32>,
            Some(false),
            Some(false),
        )
    }

    #[test]
    fn bind_scan_ranges_to_target_positions_collapses_split_ranges_for_position_bound_file() {
        let matched_file = "s3://bucket/table/data-1.parquet";
        let mut ranges = vec![
            hdfs_scan_range_params(matched_file, 0, 64, 128),
            hdfs_scan_range_params(matched_file, 64, 64, 128),
            hdfs_scan_range_params("s3://bucket/table/data-2.parquet", 0, 128, 128),
        ];
        let positions_by_file =
            std::collections::BTreeMap::from([(matched_file.to_string(), vec![3_i64, 9_i64])]);
        let mut matched_files = std::collections::BTreeSet::new();

        super::bind_scan_ranges_to_target_positions(
            &mut ranges,
            &positions_by_file,
            &mut matched_files,
        );

        assert_eq!(ranges.len(), 1);
        let hdfs_range = ranges[0]
            .scan_range
            .hdfs_scan_range
            .as_ref()
            .expect("hdfs scan range");
        assert_eq!(hdfs_range.full_path.as_deref(), Some(matched_file));
        assert_eq!(hdfs_range.offset, Some(0));
        assert_eq!(hdfs_range.length, Some(128));
        assert_eq!(hdfs_range.included_positions, Some(vec![3_i64, 9_i64]));
        assert!(matched_files.contains(matched_file));
    }

    #[test]
    fn recovers_starrocks_tablet_paths_from_metadata_after_be_startup() {
        let _runtime_guard = lock_runtime_test_state();
        use crate::meta::repository::starrocks_table::{
            CreateStarRocksTableLayoutRequest, StarRocksTableKind, StarRocksTableMetaRepository,
        };
        use crate::service::grpc_client::proto::starrocks::TabletSchemaPb;
        use prost::Message;

        let dir = TempDir::new().expect("tempdir");
        let provider =
            crate::meta::SqliteMetaStoreProvider::open(dir.path().join("standalone.sqlite"))
                .expect("open provider");
        let (db_id, table_id, schema_id, tablet_id, expected_path) = {
            let mut txn = provider
                .begin_write("seed starrocks table")
                .expect("write txn");
            let repo = StarRocksTableMetaRepository::default();
            let database = repo
                .get_or_create_database(txn.as_mut(), "analytics")
                .expect("create database");
            let created = repo
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
                .expect("create table layout");
            txn.commit().expect("commit seed");
            (
                database.db_id,
                created.table.table_id,
                created.schema.schema_id,
                created.tablets[0].tablet_id,
                created.tablets[0].tablet_root_path.clone(),
            )
        };
        let state = Arc::new(StandaloneState {
            starrocks_table_config: Some(test_starrocks_table_config()),
            metadata_provider: Some(Arc::new(provider)),
            ..StandaloneState::default()
        });
        let table = LakeTableIdentity {
            catalog: "default_catalog".to_string(),
            db_name: "analytics".to_string(),
            table_name: "orders".to_string(),
            db_id,
            table_id,
            schema_id,
        };

        let paths = recover_starrocks_tablet_paths_from_state(&state, &table, &[tablet_id])
            .expect("recover tablet paths");

        assert_eq!(
            paths.get(&tablet_id).map(String::as_str),
            Some(expected_path.as_str())
        );
        let shard = crate::runtime::starlet_shard_registry::select_infos(&[tablet_id]);
        let info = shard.get(&tablet_id).expect("shard info registered");
        assert_eq!(info.full_path, expected_path);
        assert_eq!(
            info.s3.as_ref().map(|s3| s3.endpoint.as_str()),
            Some("http://127.0.0.1:9000")
        );
    }

    #[test]
    fn recovers_starrocks_tablet_paths_from_installed_config_without_engine_state() {
        let _guard = super::acquire_standalone_test_guard();
        let _runtime_guard = lock_runtime_test_state();
        use crate::common::app_config::{
            MetadataConfig, MetadataProviderConfig, NovaRocksConfig, StandaloneObjectStoreConfig,
            StandaloneServerConfig,
        };
        use crate::meta::repository::starrocks_table::{
            CreateStarRocksTableLayoutRequest, StarRocksTableKind, StarRocksTableMetaRepository,
        };
        use crate::service::grpc_client::proto::starrocks::TabletSchemaPb;
        use prost::Message;

        let dir = TempDir::new().expect("tempdir");
        let metadata_path = dir.path().join("standalone.sqlite");
        let provider =
            crate::meta::SqliteMetaStoreProvider::open(&metadata_path).expect("open provider");
        let (db_id, table_id, schema_id, tablet_id, expected_path) = {
            let mut txn = provider
                .begin_write("seed starrocks table")
                .expect("write txn");
            let repo = StarRocksTableMetaRepository::default();
            let database = repo
                .get_or_create_database(txn.as_mut(), "analytics")
                .expect("create database");
            let created = repo
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
                .expect("create table layout");
            txn.commit().expect("commit seed");
            (
                database.db_id,
                created.table.table_id,
                created.schema.schema_id,
                created.tablets[0].tablet_id,
                created.tablets[0].tablet_root_path.clone(),
            )
        };
        let mut cfg = NovaRocksConfig::default();
        cfg.metadata = Some(MetadataConfig {
            provider: MetadataProviderConfig::Sqlite,
            path: metadata_path,
        });
        cfg.standalone_server = Some(StandaloneServerConfig {
            warehouse_uri: Some("s3://test/warehouse".to_string()),
            object_store: Some(StandaloneObjectStoreConfig {
                endpoint: Some("http://127.0.0.1:9000".to_string()),
                access_key_id: Some("ak".to_string()),
                access_key_secret: Some("sk".to_string()),
                region: None,
                enable_path_style_access: Some(true),
            }),
            ..StandaloneServerConfig::default()
        });
        crate::novarocks_config::install_preloaded_config(cfg);
        let table = LakeTableIdentity {
            catalog: "default_catalog".to_string(),
            db_name: "analytics".to_string(),
            table_name: "orders".to_string(),
            db_id,
            table_id,
            schema_id,
        };

        let paths = recover_starrocks_tablet_paths_from_installed_config(&table, &[tablet_id])
            .expect("recover tablet paths");

        assert_eq!(
            paths.get(&tablet_id).map(String::as_str),
            Some(expected_path.as_str())
        );
        let shard = crate::runtime::starlet_shard_registry::select_infos(&[tablet_id]);
        let info = shard.get(&tablet_id).expect("shard info registered");
        assert_eq!(info.full_path, expected_path);
        assert_eq!(
            info.s3.as_ref().map(|s3| s3.endpoint.as_str()),
            Some("http://127.0.0.1:9000")
        );
    }

    #[test]
    fn backend_management_sql_add_show_drop_force() {
        let mut cfg = crate::common::app_config::NovaRocksConfig::default();
        cfg.cluster.role = crate::common::app_config::ClusterRole::Fe;
        cfg.cluster.backends.clear();
        let engine = StandaloneNovaRocks::open_with_config(StandaloneOptions::default(), cfg)
            .expect("open FE engine");
        let session = engine.session();

        session
            .execute("ADD BACKEND '127.0.0.1:19170'")
            .expect("ADD BACKEND");
        let result = session.query("SHOW BACKENDS").expect("SHOW BACKENDS");
        assert_eq!(result.row_count(), 1);
        assert_eq!(string_cell(&result, 0, 1), "127.0.0.1");
        assert_eq!(string_cell(&result, 0, 2), "19170");
        assert_eq!(string_cell(&result, 0, 3), "Registering");

        session
            .execute("DROP BACKEND '127.0.0.1:19170' FORCE")
            .expect("DROP BACKEND FORCE");
        let result = session.query("SHOW BACKENDS").expect("SHOW BACKENDS");
        assert_eq!(result.row_count(), 0);
    }

    #[test]
    fn backend_management_sql_restores_persisted_backend_metadata() {
        let dir = TempDir::new().expect("tempdir");
        let metadata_path = dir.path().join("meta.sqlite");
        let mut cfg = crate::common::app_config::NovaRocksConfig::default();
        cfg.cluster.role = crate::common::app_config::ClusterRole::Fe;
        cfg.cluster.backends.clear();
        cfg.metadata = Some(crate::common::app_config::MetadataConfig {
            provider: crate::common::app_config::MetadataProviderConfig::Sqlite,
            path: metadata_path.clone(),
        });

        let engine =
            StandaloneNovaRocks::open_with_config(StandaloneOptions::default(), cfg.clone())
                .expect("open FE engine");
        engine
            .session()
            .execute("ADD BACKEND '127.0.0.1:19172'")
            .expect("ADD BACKEND");
        drop(engine);

        let reopened = StandaloneNovaRocks::open_with_config(StandaloneOptions::default(), cfg)
            .expect("reopen FE engine");
        let result = reopened
            .session()
            .query("SHOW BACKENDS")
            .expect("SHOW BACKENDS");
        assert_eq!(result.row_count(), 1);
        assert_eq!(string_cell(&result, 0, 1), "127.0.0.1");
        assert_eq!(string_cell(&result, 0, 2), "19172");
    }

    #[test]
    fn add_backend_requires_fe_role_but_show_backends_works_in_all_in_one() {
        let mut cfg = crate::common::app_config::NovaRocksConfig::default();
        cfg.cluster.role = crate::common::app_config::ClusterRole::AllInOne;
        let engine = StandaloneNovaRocks::open_with_config(StandaloneOptions::default(), cfg)
            .expect("open all-in-one engine");
        let session = engine.session();

        let err = session
            .execute("ADD BACKEND '127.0.0.1:19171'")
            .expect_err("ADD BACKEND must require FE role");
        assert!(err.contains("requires role=fe"), "{err}");

        let result = session.query("SHOW BACKENDS").expect("SHOW BACKENDS");
        assert_eq!(result.row_count(), 1);
        assert_eq!(string_cell(&result, 0, 0), "0");
        assert_eq!(string_cell(&result, 0, 1), "127.0.0.1");
        assert_eq!(
            string_cell(&result, 0, 2),
            engine.inner.exchange_port.to_string()
        );
        assert_eq!(string_cell(&result, 0, 3), "Live");
        assert_eq!(string_cell(&result, 0, 7), env!("CARGO_PKG_VERSION"));
        assert!(
            string_cell(&result, 0, 8).parse::<u32>().unwrap() > 0,
            "NumCores must be populated"
        );
    }

    #[test]
    fn create_catalog_registers_catalog_mgr_entry() {
        let engine = StandaloneNovaRocks::open(StandaloneOptions::default()).expect("open");
        let warehouse = TempDir::new().expect("warehouse");
        let sql = format!(
            r#"CREATE EXTERNAL CATALOG ice PROPERTIES("type"="iceberg","iceberg.catalog.type"="memory","iceberg.catalog.warehouse"="{}")"#,
            warehouse.path().display()
        );
        engine.session().execute(&sql).expect("create catalog");

        let mgr = engine.inner.catalog_mgr.read().expect("catalog mgr");
        assert!(mgr.get_catalog("ice").is_ok());
    }

    #[test]
    fn explain_iceberg_query_uses_catalog_mgr_without_global_registration() {
        struct TestBackend;
        impl crate::connector::backend::CatalogBackend for TestBackend {
            fn name(&self) -> &'static str {
                "iceberg"
            }

            fn namespace_exists(&self, _: &str, _: &str) -> Result<bool, String> {
                Err("unused".to_string())
            }

            fn create_namespace(&self, _: &str, _: &str) -> Result<(), String> {
                Err("unused".to_string())
            }

            fn drop_namespace(&self, _: &str, _: &str, _: bool) -> Result<(), String> {
                Err("unused".to_string())
            }

            fn create_table(
                &self,
                _: crate::connector::backend::CreateTableRequest,
            ) -> Result<(), String> {
                Err("unused".to_string())
            }

            fn table_exists(&self, _: &str, _: &str, _: &str) -> Result<bool, String> {
                Err("unused".to_string())
            }

            fn drop_table(&self, _: &str, _: &str, _: &str, _: bool) -> Result<(), String> {
                Err("unused".to_string())
            }

            fn load_table(
                &self,
                catalog: &str,
                namespace: &str,
                table: &str,
            ) -> Result<crate::connector::backend::ResolvedTable, String> {
                Ok(crate::connector::backend::ResolvedTable {
                    catalog: catalog.to_string(),
                    namespace: namespace.to_string(),
                    table: table.to_string(),
                    columns: vec![crate::sql::catalog::ColumnDef {
                        name: "id".to_string(),
                        data_type: DataType::Int64,
                        nullable: false,
                        write_default: None,
                        logical_type: None,
                    }],
                })
            }
        }

        struct TestSource;
        impl crate::connector::backend::TableSource for TestSource {
            fn name(&self) -> &'static str {
                "iceberg"
            }

            fn build_table_def(
                &self,
                table: &crate::connector::backend::ResolvedTable,
            ) -> Result<crate::sql::catalog::TableDef, String> {
                let iceberg = crate::sql::catalog::IcebergTableInfo {
                    catalog: table.catalog.clone(),
                    namespace: table.namespace.clone(),
                    table: table.table.clone(),
                    table_uuid: Some("uuid-parted".to_string()),
                    current_snapshot_id: Some(1),
                    schema_id: 1,
                    location: "memory://ice/db/parted".to_string(),
                    schema: crate::sql::catalog::IcebergSchemaDef { fields: vec![] },
                    serialized_metadata: None,
                    serialized_metadata_rows: None,
                };
                Ok(crate::sql::catalog::TableDef {
                    name: table.table.clone(),
                    columns: table.columns.clone(),
                    iceberg_row_lineage_metadata_columns: Vec::new(),
                    source: crate::sql::catalog::ScanSource::IcebergDataFiles {
                        table: iceberg,
                        files: Vec::new(),
                        cloud_properties: Default::default(),
                        binding: crate::sql::catalog::IcebergDataFileBinding::CurrentSnapshot,
                    },
                })
            }
        }

        let state = Arc::new(StandaloneState::default());
        {
            let mut connectors = state.connectors.write().expect("connectors");
            connectors.register_catalog_backend(Arc::new(TestBackend));
            connectors.register_table_source(Arc::new(TestSource));
        }
        {
            let connectors = state.connectors.read().expect("connectors");
            let mut mgr = state.catalog_mgr.write().expect("catalog mgr");
            mgr.register(Arc::new(
                crate::engine::catalog_mgr::iceberg::IcebergCatalog::new(
                    "ice",
                    connectors.catalog_backend("iceberg").expect("backend"),
                    connectors.table_source("iceberg").expect("source"),
                ),
            ));
        }
        let session = StandaloneSession {
            inner: Arc::clone(&state),
        };

        session
            .execute_in_context("EXPLAIN SELECT id FROM parted", Some("ice"), "db", None)
            .expect("explain");

        let local = state.catalog.read().expect("catalog");
        assert!(
            local.get("db", "parted").is_err(),
            "EXPLAIN analysis must not require global InMemoryCatalog registration"
        );
    }

    #[test]
    fn single_fragment_collapse_removes_distribution_enforcers() {
        use crate::sql::analysis::JoinKind;
        use crate::sql::optimizer::operator::{
            JoinDistribution, Operator, PhysicalDistributionOp, PhysicalHashJoinOp, ValuesOp,
        };
        use crate::sql::optimizer::physical_plan::{
            JoinExecutionDistribution, PhysicalPlanNode, PlanExecutionProps,
        };
        use crate::sql::optimizer::property::DistributionSpec;
        use crate::sql::optimizer::runtime_filter_pass::RuntimeFilterDesc;
        use crate::sql::optimizer::statistics::Statistics;

        fn stats() -> Statistics {
            Statistics {
                output_row_count: 0.0,
                column_statistics: Default::default(),
                ..Default::default()
            }
        }

        fn values_node() -> PhysicalPlanNode {
            PhysicalPlanNode {
                op: Operator::PhysicalValues(ValuesOp {
                    rows: Vec::new(),
                    columns: Vec::new(),
                }),
                children: Vec::new(),
                stats: stats(),
                output_columns: Vec::new(),
                execution_props: crate::sql::optimizer::physical_plan::PlanExecutionProps::default(
                ),
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
                execution_props: crate::sql::optimizer::physical_plan::PlanExecutionProps::default(
                ),
                build_runtime_filters: Vec::new(),
                probe_runtime_filters: Vec::new(),
            }
        }

        let mut scalar_arena = crate::sql::optimizer::scalar::ScalarArena::new();
        let mut rf = RuntimeFilterDesc::placeholder(&mut scalar_arena, 7);
        rf.distribution = JoinDistribution::Shuffle;

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
            execution_props: PlanExecutionProps {
                join_distribution: Some(JoinExecutionDistribution::Partitioned),
                scalar_arena: Some(std::sync::Arc::new(scalar_arena)),
                ..PlanExecutionProps::default()
            },
            build_runtime_filters: vec![rf],
            probe_runtime_filters: Vec::new(),
        };

        let collapsed = super::collapse_distribution_enforcers_for_single_fragment(plan);

        assert!(matches!(
            &collapsed.op,
            Operator::PhysicalHashJoin(join)
                if matches!(&join.distribution, JoinDistribution::Broadcast)
        ));
        assert_eq!(
            collapsed.execution_props.join_distribution,
            Some(JoinExecutionDistribution::Broadcast)
        );
        assert_eq!(collapsed.build_runtime_filters.len(), 1);
        assert!(matches!(
            collapsed.build_runtime_filters[0].distribution,
            JoinDistribution::Broadcast
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
            .register_starrocks_table("default", table.clone(), layout.clone())
            .expect("register StarRocks tbl");
        let mut date_dim = table;
        date_dim.name = "date_dim".to_string();
        catalog
            .register_starrocks_table("default", date_dim, layout.clone())
            .expect("register StarRocks date_dim");

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
        let mut scalar_arena = crate::sql::optimizer::scalar::ScalarArena::new();
        let mut opt_expr =
            crate::sql::planner::optimizer_bridge::plan::try_logical_plan_to_opt_expr(
                &logical,
                &mut scalar_arena,
            )
            .expect("logical to opt expr");
        let stats_state = Arc::new(super::StandaloneState::default());
        super::statistics::replace_catalog_stats_for_test(
            &stats_state,
            "default",
            "tbl",
            &[("id", 100_000, "1", "100000", "100000")],
        )
        .expect("install tbl stats");
        super::statistics::replace_catalog_stats_for_test(
            &stats_state,
            "default",
            "date_dim",
            &[("id", 100, "1", "100", "100")],
        )
        .expect("install date_dim stats");
        let providers =
            super::query_stats::QueryStatsProviders::from_standalone_state(&stats_state);
        let query_stats =
            super::query_stats::QueryStatsCollector::new(providers).collect(&mut opt_expr);
        let physical = crate::sql::optimizer::optimize(
            opt_expr,
            scalar_arena,
            &query_stats.snapshot,
            factory,
            None,
            Vec::new(),
        )
        .expect("optimize");
        crate::sql::codegen::fragment_builder::PlanFragmentBuilder::build_via_distributed_plan(
            &physical, &catalog, &registry, "default",
        )
        .expect("build fragments")
    }

    /// Build a `ConnectorRegistry` with a mock StarRocks scan planner that
    /// returns the schema_id and tablet splits from the given layout. Used by
    /// engine-level tests that call `PlanFragmentBuilder::build_via_distributed_plan` with a
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

    fn parse_query_for_engine_test(sql: &str) -> sqlparser::ast::Query {
        let normalized =
            crate::sql::parser::dialect::normalize_for_raw_parse(sql).expect("normalize sql");
        let statement = crate::sql::parser::parse_normalized_sql_raw(&normalized)
            .expect("parse query statement");
        let sqlparser::ast::Statement::Query(query) = statement else {
            panic!("expected query statement");
        };
        *query
    }

    fn dummy_mv_refresh_context_for_validator_test()
    -> crate::engine::mv::refresh_context::IcebergMvRefreshContext {
        use iceberg::memory::{MEMORY_CATALOG_WAREHOUSE, MemoryCatalogBuilder};
        use iceberg::spec::{
            FormatVersion, NestedField, PartitionSpec, PrimitiveType, Schema, SortOrder,
            TableMetadataBuilder, Type,
        };
        use iceberg::table::Table;
        use iceberg::{CatalogBuilder, NamespaceIdent, TableIdent};

        let warehouse = format!(
            "memory://novarocks-imv-validator-test-{}",
            uuid::Uuid::new_v4()
        );
        let runtime = tokio::runtime::Runtime::new().expect("tokio runtime");
        let iceberg_catalog: Arc<dyn iceberg::Catalog> = Arc::new(
            runtime
                .block_on(MemoryCatalogBuilder::default().load(
                    "memory",
                    std::collections::HashMap::from([(
                        MEMORY_CATALOG_WAREHOUSE.to_string(),
                        warehouse.clone(),
                    )]),
                ))
                .expect("memory catalog"),
        );

        let schema = Schema::builder()
            .with_fields(vec![
                NestedField::required(1, "k", Type::Primitive(PrimitiveType::Long)).into(),
                NestedField::optional(2, "v", Type::Primitive(PrimitiveType::Long)).into(),
            ])
            .build()
            .expect("schema");
        let metadata = TableMetadataBuilder::new(
            schema,
            PartitionSpec::unpartition_spec().into_unbound(),
            SortOrder::unsorted_order(),
            "memory://validator-target/table".to_string(),
            FormatVersion::V3,
            std::collections::HashMap::from([(
                "write.row-lineage".to_string(),
                "true".to_string(),
            )]),
        )
        .expect("metadata builder")
        .build()
        .expect("metadata")
        .metadata;
        let target_table = Table::builder()
            .file_io(iceberg::io::FileIO::new_with_memory())
            .metadata(metadata)
            .identifier(TableIdent::new(
                NamespaceIdent::new("db".to_string()),
                "mv".to_string(),
            ))
            .build()
            .expect("target table");
        let target_entry = Arc::new(
            crate::connector::iceberg::catalog::registry::build_catalog_entry(
                "tgt",
                &[
                    ("iceberg.catalog.type".to_string(), "memory".to_string()),
                    ("iceberg.catalog.warehouse".to_string(), warehouse),
                ],
            )
            .expect("catalog entry"),
        );

        crate::engine::mv::refresh_context::IcebergMvRefreshContext {
            rewrite: crate::engine::mv::refresh_context::tests_support::dummy_rewrite_context(),
            target_entry,
            base_catalog_entries: std::collections::BTreeMap::new(),
            iceberg_catalog,
            target_table,
            affected_partitions:
                crate::engine::mv::partition::AffectedTargetPartitions::not_derived(
                    "engine test context",
                ),
            pruning_limits: crate::engine::mv::refresh_context::MvRefreshPruningLimits::default(),
        }
    }

    #[test]
    fn execute_query_with_imv_validator_propagates_validator_error() {
        let query = parse_query_for_engine_test("select k, v from ice.db.b");
        let mut catalog = super::InMemoryCatalog::default();
        catalog.create_database("db").expect("create db");
        catalog
            .register(
                "db",
                crate::sql::catalog::TableDef {
                    name: "b".to_string(),
                    columns: vec![
                        crate::sql::catalog::ColumnDef {
                            name: "k".to_string(),
                            data_type: DataType::Int64,
                            nullable: false,
                            write_default: None,
                            logical_type: None,
                        },
                        crate::sql::catalog::ColumnDef {
                            name: "v".to_string(),
                            data_type: DataType::Int64,
                            nullable: true,
                            write_default: None,
                            logical_type: None,
                        },
                    ],
                    iceberg_row_lineage_metadata_columns: Vec::new(),
                    source: crate::sql::catalog::ScanSource::IcebergDataFiles {
                        table: crate::sql::catalog::IcebergTableInfo {
                            catalog: "ice".to_string(),
                            namespace: "db".to_string(),
                            table: "b".to_string(),
                            table_uuid: Some("uuid-b".to_string()),
                            current_snapshot_id: Some(22),
                            schema_id: 1,
                            location: "memory://ice/db/b".to_string(),
                            schema: crate::sql::catalog::IcebergSchemaDef {
                                fields: vec![
                                    crate::sql::catalog::IcebergSchemaFieldDef {
                                        field_id: 1,
                                        name: "k".to_string(),
                                        initial_default: None,
                                        write_default: None,
                                        initial_default_json: None,
                                        children: Vec::new(),
                                    },
                                    crate::sql::catalog::IcebergSchemaFieldDef {
                                        field_id: 2,
                                        name: "v".to_string(),
                                        initial_default: None,
                                        write_default: None,
                                        initial_default_json: None,
                                        children: Vec::new(),
                                    },
                                ],
                            },
                            serialized_metadata: None,
                            serialized_metadata_rows: None,
                        },
                        files: Vec::new(),
                        cloud_properties: Default::default(),
                        binding: crate::sql::catalog::IcebergDataFileBinding::CurrentSnapshot,
                    },
                },
            )
            .expect("register base table");
        let connectors = crate::connector::ConnectorRegistry::default();
        let mv_ctx = dummy_mv_refresh_context_for_validator_test();
        let validator =
            |_outcome: &crate::sql::planner::imv_rewrite::entrypoint::ImvRewriteOutcome| {
                Err("sentinel IMV validator error".to_string())
            };

        let err = super::execute_query_with_options_and_imv_validator(
            &query,
            &catalog,
            &connectors,
            "default",
            0,
            None,
            None,
            None,
            Some(&mv_ctx),
            Some(&validator),
            None,
        )
        .expect_err("validator errors must abort refresh query execution");

        assert_eq!(err, "sentinel IMV validator error");
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

    #[test]
    fn explain_analyze_runs_distributed_plan_and_renders_actuals() {
        let warehouse = TempDir::new().expect("warehouse");
        let (_engine, session) = open_scalar_subquery_test_engine(&warehouse);
        create_kv_tables(&session, "(1,10),(2,20),(3,30)", "(1,100),(2,200),(3,300)");

        let result = session
            .execute_in_context(
                "EXPLAIN ANALYZE \
                 SELECT count(*) \
                 FROM t1 JOIN t2 ON t1.k = t2.k",
                Some("ice"),
                "db1",
                None,
            )
            .expect("EXPLAIN ANALYZE must execute and render profile actuals");

        let StatementResult::Query(result) = result else {
            panic!("EXPLAIN ANALYZE must return rows");
        };
        let text = string_column(&result, 0).join("\n");
        assert!(text.starts_with("Planning: "), "{text}");
        assert!(text.contains(" / Execution: "), "{text}");
        assert!(text.contains(" / Rows: 1"), "{text}");
        assert!(text.contains("Profile: fragments="), "{text}");
        assert!(text.contains("operator_active="), "{text}");
        assert!(text.contains("source_wait="), "{text}");
        assert!(text.contains("sink_wait="), "{text}");
        assert!(text.contains("exchange_wait="), "{text}");
        assert!(text.contains("PLAN FRAGMENT 0"), "{text}");
        assert!(text.contains("stats={rows="), "{text}");
        assert!(text.contains("act={rows="), "{text}");
        // W0': join node act trailer includes phase timing from probe/build sides.
        assert!(text.contains("search="), "{text}");
        assert!(text.contains("output="), "{text}");
        assert!(text.contains("build_ht="), "{text}");
        // W0'b: per-fragment active/blocked Profile line under each PLAN FRAGMENT header.
        assert!(text.contains("Profile: active="), "{text}");
    }

    /// OQ-5 Task 6: codegen must lower the runtime-filter annotations the
    /// physical-tree pass attaches to a hash join into thrift
    /// `TRuntimeFilterDescription`s on the join node, AND assemble a
    /// `RuntimeFilterPlanResult`. Exercises the full standalone pipeline
    /// (analyze -> plan -> optimize[annotate] -> codegen) over the test
    /// catalog's fact-like `tbl(id int, name varchar)` joined to the small
    /// `date_dim` fixture on `id`.
    #[test]
    fn codegen_emits_build_runtime_filters_from_annotation() {
        let build =
            build_fragments_for_query("SELECT count(*) FROM tbl a JOIN date_dim b ON a.id = b.id");
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
        let engine = open_test_engine_with_metadata(&warehouse);
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
        let engine = open_test_engine_with_metadata(&warehouse);
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
    fn iceberg_refresh_load_failure_does_not_use_stale_external_metadata() {
        let warehouse = TempDir::new().expect("warehouse");
        let (engine, session) = open_iceberg_session_with_table(&warehouse, "2");
        session
            .execute_in_database("insert into ice.db1.t values (1, 'a')", "default")
            .expect("insert iceberg row");
        session
            .query("select id from ice.db1.t")
            .expect("query iceberg table");
        assert!(
            !engine.has_local_table("db1", "t"),
            "ordinary iceberg SELECT should not register a local catalog table"
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
        assert!(
            err.contains("unknown iceberg table") || err.contains("unknown table"),
            "err={err}"
        );
        assert!(
            !engine.has_local_table("db1", "t"),
            "failed refresh should not leave a local catalog table"
        );
    }

    #[test]
    fn drop_iceberg_table_invalidates_external_metadata_without_local_registration() {
        let warehouse = TempDir::new().expect("warehouse");
        let (engine, session) = open_iceberg_session_with_table(&warehouse, "2");
        session
            .execute_in_database("insert into ice.db1.t values (1, 'a')", "default")
            .expect("insert iceberg row");
        session
            .query("select id from ice.db1.t")
            .expect("query iceberg table");
        assert!(
            !engine.has_local_table("db1", "t"),
            "ordinary iceberg SELECT should not register a local catalog table"
        );

        let drop = session
            .execute_in_database("drop table ice.db1.t", "default")
            .expect("drop iceberg table");
        assert!(matches!(drop, StatementResult::Ok));
        assert!(
            !engine.has_local_table("db1", "t"),
            "drop table should keep the local catalog clear"
        );
        let err = session
            .query("select id from ice.db1.t")
            .expect_err("dropped iceberg table should not be queryable");
        assert!(
            err.contains("unknown iceberg table") || err.contains("unknown table"),
            "err={err}"
        );
    }

    #[test]
    fn embedded_session_preserves_projection_order_with_current_catalog_context() {
        let warehouse = TempDir::new().expect("create iceberg warehouse");
        let engine = open_test_engine_with_metadata(&warehouse);
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

    fn open_test_engine_with_metadata(warehouse: &TempDir) -> StandaloneNovaRocks {
        let config_path = warehouse.path().join("novarocks.toml");
        std::fs::create_dir_all(warehouse.path().join("meta")).expect("create metadata dir");
        std::fs::write(
            &config_path,
            r#"[metadata]
provider = "sqlite"
path = "meta/operations.sqlite"
"#,
        )
        .expect("write metadata config");
        StandaloneNovaRocks::open(StandaloneOptions {
            config_path: Some(config_path),
        })
        .expect("open engine")
    }

    fn open_iceberg_session_with_table(
        warehouse: &TempDir,
        format_version: &str,
    ) -> (StandaloneNovaRocks, StandaloneSession) {
        let engine = open_test_engine_with_metadata(warehouse);
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

        let engine = open_test_engine_with_metadata(warehouse);
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
        collect_id_v_from_result(result)
    }

    fn collect_id_v_from_result(result: QueryResult) -> Vec<(i32, String)> {
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
    fn time_travel_select_with_current_iceberg_catalog_resolves_synthetic_local_table() {
        let warehouse = TempDir::new().expect("warehouse");
        let (_engine, session) = open_iceberg_session_with_table(&warehouse, "2");
        session
            .execute_in_database("insert into ice.db1.t values (1, 'a')", "default")
            .expect("seed");

        let result = session
            .execute_in_context(
                "select id, v from t for version as of 'main'",
                Some("ice"),
                "db1",
                None,
            )
            .expect("time-travel select");
        let StatementResult::Query(result) = result else {
            panic!("expected query result");
        };

        assert_eq!(collect_id_v_from_result(result), vec![(1, "a".to_string())]);
    }

    #[test]
    fn time_travel_explain_with_current_iceberg_catalog_resolves_synthetic_local_table() {
        let warehouse = TempDir::new().expect("warehouse");
        let (_engine, session) = open_iceberg_session_with_table(&warehouse, "2");
        session
            .execute_in_database("insert into ice.db1.t values (1, 'a')", "default")
            .expect("seed");

        let result = session
            .execute_in_context(
                "explain select id from t for version as of 'main'",
                Some("ice"),
                "db1",
                None,
            )
            .expect("time-travel explain");

        assert!(matches!(result, StatementResult::Query(_)));
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
        assert_iceberg_operation_finalized(
            &engine,
            2,
            crate::meta::repository::iceberg_operation::IcebergOperationKind::InsertAppend,
            snap_after,
        );
    }

    #[test]
    fn iceberg_branch_writes_record_branch_head_base_snapshot() {
        let warehouse = TempDir::new().expect("warehouse");
        let (engine, session) = open_iceberg_session_with_table(&warehouse, "3");
        session
            .execute_in_database("insert into ice.db1.t values (1, 'main-1')", "default")
            .expect("seed main");
        let branch_base =
            current_iceberg_snapshot_id(&engine, "ice", "db1", "t").expect("seed snapshot");
        session
            .execute_in_database("alter table ice.db1.t create branch dev", "default")
            .expect("create branch");
        session
            .execute_in_database("insert into ice.db1.t values (2, 'main-2')", "default")
            .expect("advance main after branch creation");
        let main_after_branch = current_iceberg_snapshot_id(&engine, "ice", "db1", "t")
            .expect("main advanced snapshot");
        assert_ne!(
            branch_base, main_after_branch,
            "main must advance after the branch was created"
        );

        session
            .execute_in_database(
                "insert into ice.db1.t.branch_dev values (3, 'dev-3')",
                "default",
            )
            .expect("branch insert");
        let dev_after = iceberg_ref_snapshot_id(&engine, "ice", "db1", "t", "dev")
            .expect("dev branch snapshot");
        assert_ne!(
            branch_base, dev_after,
            "branch insert must advance the branch head"
        );

        let operation = load_iceberg_operation(&engine, 3);
        assert_eq!(
            operation.operation_kind,
            crate::meta::repository::iceberg_operation::IcebergOperationKind::InsertAppend
        );
        assert_eq!(operation.target.ref_name.as_deref(), Some("dev"));
        assert_eq!(
            operation.base_snapshot_id,
            Some(branch_base),
            "branch write operation must record the branch head before commit, not main"
        );
        assert_eq!(
            operation
                .commit_outcome
                .as_ref()
                .map(|outcome| outcome.snapshot_id),
            Some(dev_after)
        );
        assert_eq!(
            current_iceberg_snapshot_id(&engine, "ice", "db1", "t"),
            Some(main_after_branch),
            "branch insert must not advance main"
        );

        session
            .execute_in_database(
                "insert overwrite ice.db1.t.branch_dev values (4, 'dev-4')",
                "default",
            )
            .expect("branch overwrite");
        let dev_after_overwrite = iceberg_ref_snapshot_id(&engine, "ice", "db1", "t", "dev")
            .expect("dev branch snapshot after overwrite");
        assert_ne!(
            dev_after, dev_after_overwrite,
            "branch overwrite must advance the branch head"
        );

        let operation = load_iceberg_operation(&engine, 4);
        assert_eq!(
            operation.operation_kind,
            crate::meta::repository::iceberg_operation::IcebergOperationKind::InsertOverwrite
        );
        assert_eq!(operation.target.ref_name.as_deref(), Some("dev"));
        assert_eq!(
            operation.base_snapshot_id,
            Some(dev_after),
            "branch overwrite operation must record the branch head before commit, not main"
        );
        assert_eq!(
            operation
                .commit_outcome
                .as_ref()
                .map(|outcome| outcome.snapshot_id),
            Some(dev_after_overwrite)
        );
        assert_eq!(
            current_iceberg_snapshot_id(&engine, "ice", "db1", "t"),
            Some(main_after_branch),
            "branch overwrite must not advance main"
        );
    }

    // -----------------------------------------------------------------------
    // ANALYZE TABLE / ANALYZE FULL TABLE against iceberg external-catalog
    // tables. Ordinary SELECT resolves external tables through
    // CatalogMgrProvider and no longer pre-registers them in the local catalog,
    // so the statistics path must materialize the table itself before reading
    // local metadata (regression: ANALYZE failed with "unknown table").
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
    fn select_resolves_join_on_subquery_iceberg_table_without_local_registration() {
        // Engine gap #4 (join_apply_to_join q7): a table referenced ONLY inside
        // a subquery nested in a JOIN ON predicate must resolve through the
        // catalog-aware provider, exactly like FROM/WHERE subqueries. This
        // SELECT must not materialize ordinary Iceberg tables into the local
        // in-memory catalog.
        let warehouse = TempDir::new().expect("warehouse");
        let engine = open_test_engine_with_metadata(&warehouse);
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

        // The ON-clause-only table resolved through CatalogMgrProvider without
        // being materialized in the in-memory catalog, and the query produced a
        // count row.
        assert!(
            !engine.has_local_table("db1", "t2"),
            "ON-clause-subquery table t2 must not be locally registered",
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

    fn iceberg_ref_snapshot_id(
        engine: &StandaloneNovaRocks,
        catalog: &str,
        namespace: &str,
        table: &str,
        ref_name: &str,
    ) -> Option<i64> {
        let registry = engine.inner.iceberg_catalogs.read().expect("registry");
        let entry = registry.get(catalog).expect("entry");
        entry.invalidate_table_cache(namespace, table);
        let loaded =
            crate::connector::iceberg::catalog::load_table(&entry, namespace, table).expect("load");
        loaded
            .table
            .metadata()
            .refs()
            .get(ref_name)
            .map(|r| r.snapshot_id)
    }

    fn load_iceberg_operation(
        engine: &StandaloneNovaRocks,
        operation_id: i64,
    ) -> crate::meta::repository::iceberg_operation::StoredIcebergOperation {
        let provider = engine
            .inner
            .metadata_provider
            .as_ref()
            .expect("metadata provider");
        let read = provider.begin_read().expect("read operation metadata");
        engine
            .inner
            .iceberg_operation_repo
            .load_operation(read.as_ref(), operation_id)
            .expect("load iceberg operation")
            .expect("iceberg operation present")
    }

    fn assert_iceberg_operation_finalized(
        engine: &StandaloneNovaRocks,
        operation_id: i64,
        expected_kind: crate::meta::repository::iceberg_operation::IcebergOperationKind,
        expected_snapshot_id: Option<i64>,
    ) {
        let operation = load_iceberg_operation(engine, operation_id);
        assert_eq!(operation.operation_kind, expected_kind);
        assert_eq!(
            operation.state,
            crate::meta::repository::iceberg_operation::IcebergOperationState::Finalized
        );
        assert_eq!(
            operation.commit_outcome.as_ref().map(|c| c.snapshot_id),
            expected_snapshot_id
        );
    }

    fn assert_iceberg_operation_finalized_any_snapshot(
        engine: &StandaloneNovaRocks,
        operation_id: i64,
        expected_kind: crate::meta::repository::iceberg_operation::IcebergOperationKind,
    ) -> Option<i64> {
        let operation = load_iceberg_operation(engine, operation_id);
        assert_eq!(operation.operation_kind, expected_kind);
        assert_eq!(
            operation.state,
            crate::meta::repository::iceberg_operation::IcebergOperationState::Finalized
        );
        let snapshot_id = operation.commit_outcome.as_ref().map(|c| c.snapshot_id);
        assert!(
            snapshot_id.is_some(),
            "finalized write operation must record committed snapshot id"
        );
        snapshot_id
    }

    fn assert_iceberg_operation_absent(engine: &StandaloneNovaRocks, operation_id: i64) {
        let provider = engine
            .inner
            .metadata_provider
            .as_ref()
            .expect("metadata provider");
        let read = provider.begin_read().expect("read operation metadata");
        let operation = engine
            .inner
            .iceberg_operation_repo
            .load_operation(read.as_ref(), operation_id)
            .expect("load iceberg operation");
        assert!(
            operation.is_none(),
            "expected no iceberg operation #{operation_id}, found {operation:?}"
        );
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
        assert_iceberg_operation_finalized(
            &engine,
            2,
            crate::meta::repository::iceberg_operation::IcebergOperationKind::RowDelta,
            snap_after,
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
        let snap_after = current_iceberg_snapshot_id(&engine, "ice", "db1", "t");
        assert_iceberg_operation_finalized(
            &engine,
            3,
            crate::meta::repository::iceberg_operation::IcebergOperationKind::RowDelta,
            snap_after,
        );
    }

    #[test]
    fn iceberg_add_equality_delete_drives_operation_lifecycle() {
        let warehouse = TempDir::new().expect("warehouse");
        let (engine, session) = open_iceberg_session_with_table(&warehouse, "2");
        session
            .execute_in_database(
                "insert into ice.db1.t values (1, 'a'), (2, 'b'), (3, 'b')",
                "default",
            )
            .expect("seed");
        session
            .execute_in_database(
                "alter table ice.db1.t add equality delete (v) values ('b')",
                "default",
            )
            .expect("add equality delete");
        let snap_after = current_iceberg_snapshot_id(&engine, "ice", "db1", "t");
        assert_iceberg_operation_finalized(
            &engine,
            2,
            crate::meta::repository::iceberg_operation::IcebergOperationKind::RowDelta,
            snap_after,
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
        let snap_after = current_iceberg_snapshot_id(&engine, "ice", "db1", "t");
        assert_iceberg_operation_finalized(
            &engine,
            2,
            crate::meta::repository::iceberg_operation::IcebergOperationKind::RowDelta,
            snap_after,
        );
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
        let (engine, session) = open_row_lineage_iceberg_session_with_table_extra_props(
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
        let snap_after = current_iceberg_snapshot_id(&engine, "ice", "db1", "t");
        assert_iceberg_operation_finalized(
            &engine,
            2,
            crate::meta::repository::iceberg_operation::IcebergOperationKind::RowDelta,
            snap_after,
        );
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
    fn iceberg_v3_merge_upsert_drives_operation_lifecycle() {
        let warehouse = TempDir::new().expect("warehouse");
        let (engine, session) = open_row_lineage_iceberg_session_with_table(&warehouse);
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
            .execute_in_database(
                "insert into ice.db1.src values (2, 'bb'), (3, 'c')",
                "default",
            )
            .expect("insert source");
        session
            .execute_in_database(
                "merge into ice.db1.t as t using ice.db1.src as s on t.id = s.id \
                 when matched then update set v = s.new_v \
                 when not matched then insert (id, v) values (s.id, s.new_v)",
                "default",
            )
            .expect("merge upsert");
        let rows = collect_id_v(&session, "select id, v from ice.db1.t order by id");
        assert_eq!(
            rows,
            vec![
                (1, "a".to_string()),
                (2, "bb".to_string()),
                (3, "c".to_string())
            ]
        );
        // Phase 3 (commit 6e21eab0) folds all MERGE branches into one
        // collector + one commit, so an upsert MERGE
        // (WHEN MATCHED UPDATE / WHEN NOT MATCHED INSERT) now produces a
        // SINGLE iceberg operation: one folded RowDelta commit. Pre-fold it
        // produced two operations — a separate InsertAppend (not-matched
        // INSERT FastAppend) plus a RowDelta (matched UPDATE).
        //
        // Here the two preceding inserts take operation ids #1 and #2
        // (InsertAppend each), so the folded MERGE occupies the single
        // operation id #3 — the slot the not-matched INSERT branch used to
        // take — and no operation #4 exists.
        let snap_after = current_iceberg_snapshot_id(&engine, "ice", "db1", "t");
        assert_iceberg_operation_finalized(
            &engine,
            3,
            crate::meta::repository::iceberg_operation::IcebergOperationKind::RowDelta,
            snap_after,
        );
        // Prove the fold: the MERGE committed exactly one operation, so the
        // pre-fold second operation (#4) must not exist.
        assert_iceberg_operation_absent(&engine, 4);
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

        let engine = open_test_engine_with_metadata(warehouse);
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
                r#"create table ice.ns.t3 (id bigint) tblproperties("format-version"="3","write.row-lineage"="false")"#,
                "default",
            )
            .expect("create V3 iceberg table with row-lineage disabled");

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
        let _guard = super::acquire_standalone_test_guard();
        let _registry = crate::runtime::backend_registry::BackendRegistryTestGuard::new();
        crate::engine::backend_ops::install_all_in_one_backend_registry(
            "127.0.0.1:19070".parse().unwrap(),
            3,
        )
        .expect("install loopback registry");
        let result = super::dispatcher_for_role(ClusterRole::AllInOne);
        assert!(result.is_ok(), "AllInOne should produce a dispatcher");
    }

    #[test]
    fn all_in_one_dispatcher_uses_remote_registry() {
        let _guard = super::acquire_standalone_test_guard();
        let _registry = crate::runtime::backend_registry::BackendRegistryTestGuard::new();
        use crate::common::app_config::{ClusterRole, NovaRocksConfig};
        let mut cfg = NovaRocksConfig::default();
        cfg.cluster.role = ClusterRole::AllInOne;
        cfg.cluster.backends.clear();
        crate::common::app_config::install_preloaded_config(cfg);

        let endpoint = "127.0.0.1:19070".parse().unwrap();
        crate::engine::backend_ops::install_all_in_one_backend_registry(endpoint, 3)
            .expect("install loopback registry");

        let dispatcher = super::dispatcher_for_role(ClusterRole::AllInOne).expect("dispatcher");

        assert_eq!(super::dispatcher_kind_for_test(&dispatcher), "remote");
        assert_eq!(dispatcher.backend_count(), 1);
    }

    #[test]
    fn coordinated_execution_services_use_one_live_backend_snapshot() {
        let _guard = super::acquire_standalone_test_guard();
        let _registry = crate::runtime::backend_registry::BackendRegistryTestGuard::new();
        use crate::common::app_config::{ClusterRole, NovaRocksConfig};
        use crate::runtime::backend_registry::{BackendRegistry, BackendState};
        let mut cfg = NovaRocksConfig::default();
        cfg.cluster.role = ClusterRole::Fe;
        cfg.cluster.backends.clear();
        crate::common::app_config::install_preloaded_config(cfg);

        let endpoint = "127.0.0.1:19072".parse().unwrap();
        let registry = Arc::new(BackendRegistry::new(3));
        registry.restore_backend(2, endpoint, BackendState::Live);
        crate::runtime::backend_registry::replace_backend_registry_for_test(Some(registry));

        let (dispatcher, scheduler) =
            super::coordinated_execution_services().expect("coordinated services");

        assert_eq!(super::dispatcher_kind_for_test(&dispatcher), "remote");
        assert_eq!(dispatcher.backend_count(), 1);
        assert_eq!(scheduler.live_backend_entries(), &[(2usize, endpoint)]);
    }

    fn install_optimizer_backend_count_config(
        optimizer_backend_count: u64,
        cluster_backends: Vec<String>,
    ) {
        let mut cfg = crate::common::app_config::NovaRocksConfig::default();
        cfg.cluster.backends = cluster_backends;
        cfg.runtime.optimizer_effective_backend_count = optimizer_backend_count;
        crate::common::app_config::install_preloaded_config(cfg);
    }

    #[test]
    fn live_effective_backend_count_prefers_live_registry_over_runtime_config() {
        let _guard = super::acquire_standalone_test_guard();
        let _registry = crate::runtime::backend_registry::BackendRegistryTestGuard::new();
        use crate::runtime::backend_registry::{BackendRegistry, BackendState};

        install_optimizer_backend_count_config(7, vec!["127.0.0.1:19080".to_string()]);
        let registry = Arc::new(BackendRegistry::new(3));
        registry.restore_backend(2, "127.0.0.1:19081".parse().unwrap(), BackendState::Live);
        registry.restore_backend(3, "127.0.0.1:19082".parse().unwrap(), BackendState::Live);
        crate::runtime::backend_registry::replace_backend_registry_for_test(Some(registry));

        assert_eq!(super::live_effective_backend_count(), 2.0);
    }

    #[test]
    fn live_effective_backend_count_prefers_runtime_config_when_registry_absent() {
        let _guard = super::acquire_standalone_test_guard();
        let _registry = crate::runtime::backend_registry::BackendRegistryTestGuard::new();

        install_optimizer_backend_count_config(
            7,
            vec!["127.0.0.1:19083".to_string(), "127.0.0.1:19084".to_string()],
        );

        assert_eq!(super::live_effective_backend_count(), 7.0);
    }

    #[test]
    fn live_effective_backend_count_prefers_runtime_config_when_registry_empty() {
        let _guard = super::acquire_standalone_test_guard();
        let _registry = crate::runtime::backend_registry::BackendRegistryTestGuard::new();
        let registry = Arc::new(crate::runtime::backend_registry::BackendRegistry::new(3));

        install_optimizer_backend_count_config(7, vec!["127.0.0.1:19085".to_string()]);
        crate::runtime::backend_registry::replace_backend_registry_for_test(Some(registry));

        assert_eq!(super::live_effective_backend_count(), 7.0);
    }

    #[test]
    fn live_effective_backend_count_defaults_to_one_without_registry_or_runtime_config() {
        let _guard = super::acquire_standalone_test_guard();
        let _registry = crate::runtime::backend_registry::BackendRegistryTestGuard::new();

        install_optimizer_backend_count_config(0, Vec::new());

        assert_eq!(super::live_effective_backend_count(), 1.0);
    }

    #[test]
    fn dispatcher_for_role_fe_no_backend_configured_returns_error() {
        let _guard = super::acquire_standalone_test_guard();
        use crate::common::app_config::ClusterRole;
        crate::common::app_config::install_default_for_test();
        let result = super::dispatcher_for_role(ClusterRole::Fe);
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
        let result = super::dispatcher_for_role(ClusterRole::Be);
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
        let result = super::dispatcher_for_role(ClusterRole::Fe);
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
        let result = super::dispatcher_for_role(ClusterRole::Fe);
        assert!(result.is_err(), "malformed backend must return an error");
        let msg = result.err().expect("error");
        assert!(msg.contains("role=fe"), "must mention role=fe: {msg}");
        assert!(
            msg.contains("not-an-addr"),
            "must include the bad value: {msg}"
        );
    }

    /// D2: FE role with more than one backend builds a multi-backend
    /// `RemoteDispatcher`. The dispatcher reports a backend count equal to the
    /// number of configured backends.
    #[test]
    fn dispatcher_for_role_fe_multiple_backends_ok() {
        let _guard = super::acquire_standalone_test_guard();
        use crate::common::app_config::{ClusterRole, NovaRocksConfig};
        let mut cfg = NovaRocksConfig::default();
        cfg.cluster.backends = vec!["127.0.0.1:9070".to_string(), "127.0.0.1:9071".to_string()];
        crate::common::app_config::install_preloaded_config(cfg);
        let result = super::dispatcher_for_role(ClusterRole::Fe);
        assert!(
            result.is_ok(),
            "Fe with multiple backends must build a dispatcher, got: {:?}",
            result.err()
        );
        let dispatcher = result.expect("dispatcher");
        assert_eq!(
            dispatcher.backend_count(),
            2,
            "dispatcher must route to both configured backends"
        );
    }

    #[test]
    fn coordinated_iceberg_insert_requires_exchange_server() {
        let query = parse_query_for_engine_test("SELECT id FROM missing_table");
        let state = Arc::new(StandaloneState::default());
        let mut sink_spec =
            crate::sql::codegen::iceberg_write_sink::test_support::simple_sink_spec();
        sink_spec.iceberg.serialized_metadata = Some(
            crate::sql::codegen::iceberg_write_sink::test_support::single_bucket_partition_metadata_json(),
        );

        let result = super::execute_query_as_iceberg_write(
            &state, None, "default", &query, sink_spec, None, None,
        );

        let err = result.expect_err("default state should fail before executing the sink");
        assert!(
            err.contains("missing_table"),
            "error should come from analyzer/catalog lookup, got: {err}"
        );
    }

    #[test]
    fn iceberg_write_root_shuffle_by_output_name_uses_logical_output_column_id() {
        use crate::sql::catalog::{CatalogProvider, TableDef};
        use crate::sql::column_id::ColumnId;
        use crate::sql::optimizer::property::{DistributionSpec, HashSource};

        struct EmptyCatalog;
        impl CatalogProvider for EmptyCatalog {
            fn get_table(&self, _database: &str, table: &str) -> Result<TableDef, String> {
                Err(format!("table not found: {table}"))
            }
        }

        let stmt = crate::sql::parser::parse_sql_raw("SELECT 1 AS payload, 'file-a' AS _file")
            .expect("parse query");
        let sqlparser::ast::Statement::Query(query) = stmt else {
            panic!("expected query statement");
        };
        let (resolved, cte_registry, mut factory) =
            crate::sql::analyzer::analyze(&query, &EmptyCatalog, "default").expect("analyze query");
        let logical = crate::sql::planner::plan_query(resolved, cte_registry, &mut factory)
            .expect("plan query");
        let planned_file_col = crate::sql::planner::plan_output_columns(&logical)
            .expect("planned output columns")
            .into_iter()
            .find(|column| column.name == "_file")
            .expect("planned _file output")
            .column_id;
        assert_ne!(planned_file_col, ColumnId::UNSET);

        let distribution = super::iceberg_write_shuffle_by_output_name("_file")(&logical)
            .expect("resolve root distribution")
            .expect("root distribution");

        match distribution {
            DistributionSpec::HashPartitioned { cols, source } => {
                assert_eq!(cols, vec![planned_file_col]);
                assert_eq!(source, HashSource::ShuffleAgg);
            }
            other => panic!("expected shuffle distribution, got {other:?}"),
        }
    }

    #[test]
    fn execute_query_as_iceberg_write_invokes_root_distribution_resolver_after_planning() {
        let query = parse_query_for_engine_test("SELECT 1 AS payload, 'file-a' AS _file");
        let mut state = StandaloneState::default();
        state.exchange_port = 1;
        let state = Arc::new(state);
        let mut sink_spec =
            crate::sql::codegen::iceberg_write_sink::test_support::simple_sink_spec();
        sink_spec.iceberg.serialized_metadata = Some(
            crate::sql::codegen::iceberg_write_sink::test_support::single_bucket_partition_metadata_json(),
        );

        let result = super::execute_query_as_iceberg_write(
            &state,
            None,
            "default",
            &query,
            sink_spec,
            None,
            Some(Box::new(|logical| {
                let saw_file_output = crate::sql::planner::plan_output_columns(logical)?
                    .into_iter()
                    .any(|column| {
                        column.name == "_file"
                            && column.column_id != crate::sql::column_id::ColumnId::UNSET
                    });
                if !saw_file_output {
                    return Err("resolver did not see planned _file output".to_string());
                }
                Err("resolver saw planned _file output".to_string())
            })),
        );

        let err = match result {
            Ok(_) => panic!("resolver error should stop write planning before execution"),
            Err(err) => err,
        };
        assert!(
            err.contains("resolver saw planned _file output"),
            "expected resolver error, got: {err}"
        );
    }

    #[test]
    fn dispatcher_for_role_fe_uses_live_registry_backend_ids() {
        let _guard = super::acquire_standalone_test_guard();
        let _registry = crate::runtime::backend_registry::BackendRegistryTestGuard::new();
        use crate::common::app_config::{ClusterRole, NovaRocksConfig};
        use crate::runtime::backend_registry::{BackendRegistry, BackendState};
        let mut cfg = NovaRocksConfig::default();
        cfg.cluster.role = ClusterRole::Fe;
        cfg.cluster.backends.clear();
        crate::common::app_config::install_preloaded_config(cfg);

        let registry = Arc::new(BackendRegistry::new(3));
        registry.restore_backend(2, "127.0.0.1:19072".parse().unwrap(), BackendState::Live);
        crate::runtime::backend_registry::replace_backend_registry_for_test(Some(registry));

        let dispatcher = super::dispatcher_for_role(ClusterRole::Fe).expect("dispatcher");
        assert_eq!(dispatcher.backend_count(), 1);
    }

    #[test]
    fn parse_explain_refresh_materialized_view_supports_verbose_and_costs() {
        let verbose = super::parse_explain_refresh_materialized_view(
            "EXPLAIN VERBOSE REFRESH MATERIALIZED VIEW mv1",
        )
        .expect("recognized")
        .expect("parsed");
        assert_eq!(verbose.0.name.parts, vec!["mv1"]);
        assert_eq!(verbose.1, crate::sql::explain::ExplainLevel::Verbose);
        assert!(!verbose.2);

        let costs = super::parse_explain_refresh_materialized_view(
            "EXPLAIN COSTS REFRESH MATERIALIZED VIEW db.mv1",
        )
        .expect("recognized")
        .expect("parsed");
        assert_eq!(costs.0.name.parts, vec!["db", "mv1"]);
        assert_eq!(costs.1, crate::sql::explain::ExplainLevel::Costs);
        assert!(!costs.2);
    }

    #[test]
    fn parse_explain_refresh_materialized_view_marks_analyze() {
        let parsed = super::parse_explain_refresh_materialized_view(
            "EXPLAIN ANALYZE REFRESH MATERIALIZED VIEW mv1",
        )
        .expect("recognized")
        .expect("parsed");
        assert_eq!(parsed.1, crate::sql::explain::ExplainLevel::Analyze);
        assert!(parsed.2);
    }

    #[test]
    fn split_explain_logical_sql_rewrites_to_plain_explain() {
        let (rewritten, level) =
            super::split_explain_logical_sql(" EXPLAIN LOGICAL SELECT * FROM t")
                .expect("recognized");
        assert_eq!(rewritten, "EXPLAIN SELECT * FROM t");
        assert_eq!(level, crate::sql::explain::ExplainLevel::Normal);

        let (rewritten, level) =
            super::split_explain_logical_sql("explain logical verbose select k from t")
                .expect("recognized");
        assert_eq!(rewritten, "EXPLAIN select k from t");
        assert_eq!(level, crate::sql::explain::ExplainLevel::Verbose);

        let (rewritten, level) =
            super::split_explain_logical_sql("EXPLAIN\nLOGICAL\nSELECT k FROM t")
                .expect("recognized");
        assert_eq!(rewritten, "EXPLAIN SELECT k FROM t");
        assert_eq!(level, crate::sql::explain::ExplainLevel::Normal);
    }

    // -------------------------------------------------------------------------
    // Scalar subquery decorrelation — end-to-end correctness tests (Task 5).
    //
    // These tests run subquery shapes against the same Iceberg-backed
    // in-memory tables and assert the Apply framework's concrete semantics.
    //
    // Setup: t1(k BIGINT, v BIGINT) and t2(k BIGINT, v BIGINT) in an Iceberg
    // in-memory catalog, populated with controlled data per test.
    //
    // The migration-time subquery routing switch has been removed; analyzer
    // routing must now be explicit and unsupported shapes must fail clearly.
    // -------------------------------------------------------------------------

    /// Open an Iceberg-backed test engine and return (engine, session, warehouse_dir).
    /// Tables are NOT yet created; the caller creates and populates them.
    fn open_scalar_subquery_test_engine(
        warehouse: &TempDir,
    ) -> (StandaloneNovaRocks, StandaloneSession) {
        let engine = open_test_engine_with_metadata(warehouse);
        let session = engine.session();
        let create_catalog_sql = format!(
            r#"create external catalog ice properties("type"="iceberg","iceberg.catalog.type"="memory","iceberg.catalog.warehouse"="{}")"#,
            warehouse.path().display()
        );
        session
            .execute_in_database(&create_catalog_sql, "default")
            .expect("create iceberg catalog");
        session
            .execute_in_database("create database ice.db1", "default")
            .expect("create iceberg database");
        (engine, session)
    }

    /// Run a SELECT query and return the first column's values
    /// as a `Vec<Option<i64>>` (None = SQL NULL). Expects every row to have
    /// exactly one column of type BIGINT/INT.
    fn run_scalar_query_i64(
        session: &StandaloneSession,
        sql: &str,
    ) -> Result<Vec<Option<i64>>, String> {
        let result = session.execute_in_context(sql, Some("ice"), "db1", None)?;
        let qr = match result {
            StatementResult::Query(qr) => qr,
            StatementResult::Ok => {
                return Err("query returned no rows (StatementResult::Ok)".to_string());
            }
        };
        let mut values = Vec::new();
        for chunk in &qr.chunks {
            let col = chunk.batch.column(0);
            // Try Int64 first (BIGINT), then Int32 (INT).
            if let Some(arr) = col.as_any().downcast_ref::<Int64Array>() {
                for i in 0..arr.len() {
                    values.push(if arr.is_null(i) {
                        None
                    } else {
                        Some(arr.value(i))
                    });
                }
            } else if let Some(arr) = col.as_any().downcast_ref::<Int32Array>() {
                for i in 0..arr.len() {
                    values.push(if arr.is_null(i) {
                        None
                    } else {
                        Some(arr.value(i) as i64)
                    });
                }
            } else {
                return Err(format!(
                    "expected Int64 or Int32 column, got {:?}",
                    col.data_type()
                ));
            }
        }
        Ok(values)
    }

    /// Run a SELECT query and return all columns' values as a
    /// `Vec<Vec<Option<i64>>>` (outer = rows, inner = columns). Expects every
    /// column to be BIGINT/INT.
    fn run_scalar_query_multi_col(
        session: &StandaloneSession,
        sql: &str,
    ) -> Result<Vec<Vec<Option<i64>>>, String> {
        let result = session.execute_in_context(sql, Some("ice"), "db1", None)?;
        let qr = match result {
            StatementResult::Query(qr) => qr,
            StatementResult::Ok => {
                return Err("query returned no rows (StatementResult::Ok)".to_string());
            }
        };
        let num_cols = qr.columns.len();
        // Build row-major result: collect per-column arrays, then transpose.
        let mut col_values: Vec<Vec<Option<i64>>> = vec![Vec::new(); num_cols];
        for chunk in &qr.chunks {
            for col_idx in 0..num_cols {
                let col = chunk.batch.column(col_idx);
                if let Some(arr) = col.as_any().downcast_ref::<Int64Array>() {
                    for i in 0..arr.len() {
                        col_values[col_idx].push(if arr.is_null(i) {
                            None
                        } else {
                            Some(arr.value(i))
                        });
                    }
                } else if let Some(arr) = col.as_any().downcast_ref::<Int32Array>() {
                    for i in 0..arr.len() {
                        col_values[col_idx].push(if arr.is_null(i) {
                            None
                        } else {
                            Some(arr.value(i) as i64)
                        });
                    }
                } else {
                    return Err(format!(
                        "col {col_idx}: expected Int64 or Int32, got {:?}",
                        col.data_type()
                    ));
                }
            }
        }
        // Transpose to row-major.
        let num_rows = col_values.first().map(|v| v.len()).unwrap_or(0);
        let rows = (0..num_rows)
            .map(|r| (0..num_cols).map(|c| col_values[c][r]).collect())
            .collect();
        Ok(rows)
    }

    /// Run a SELECT query and expect an error containing `needle`.
    fn expect_subquery_error(session: &StandaloneSession, sql: &str, needle: &str) {
        let result = session.execute_in_context(sql, Some("ice"), "db1", None);
        let err = result.expect_err(&format!(
            "expected subquery error containing '{needle}', but query succeeded"
        ));
        assert!(
            err.contains(needle),
            "expected error containing '{needle}'; got: {err}"
        );
    }

    fn create_kv_tables(session: &StandaloneSession, t1_values: &str, t2_values: &str) {
        session
            .execute_in_database("create table ice.db1.t1 (k bigint, v bigint)", "default")
            .expect("create t1");
        session
            .execute_in_database("create table ice.db1.t2 (k bigint, v bigint)", "default")
            .expect("create t2");
        if !t1_values.trim().is_empty() {
            session
                .execute_in_database(
                    &format!("insert into ice.db1.t1 values {t1_values}"),
                    "default",
                )
                .expect("insert t1");
        }
        if !t2_values.trim().is_empty() {
            session
                .execute_in_database(
                    &format!("insert into ice.db1.t2 values {t2_values}"),
                    "default",
                )
                .expect("insert t2");
        }
    }

    fn assert_subquery_result_i64(
        session: &StandaloneSession,
        sql: &str,
        expected: Vec<Option<i64>>,
    ) {
        let result = run_scalar_query_i64(session, sql).expect("subquery query");

        assert_eq!(result, expected, "unexpected SQL result for query: {sql}");
    }

    // ---- Test 1: correlated aggregate (q17-shape) ----------------------------

    /// Correlated aggregate scalar: `WHERE v = (SELECT min(t2.v) FROM t2 WHERE t2.k = t1.k)`.
    #[test]
    fn scalar_subquery_correlated_agg_returns_expected_rows() {
        let warehouse = TempDir::new().expect("warehouse");
        let (_engine, session) = open_scalar_subquery_test_engine(&warehouse);

        session
            .execute_in_database("create table ice.db1.t1 (k bigint, v bigint)", "default")
            .expect("create t1");
        session
            .execute_in_database("create table ice.db1.t2 (k bigint, v bigint)", "default")
            .expect("create t2");
        // t1: (1,10),(2,20),(3,30)
        // t2: (1,10),(1,5),(2,20),(2,15)  -> min for k=1 is 5, k=2 is 15, k=3 has no match
        session
            .execute_in_database(
                "insert into ice.db1.t1 values (1,10),(2,20),(3,30)",
                "default",
            )
            .expect("insert t1");
        session
            .execute_in_database(
                "insert into ice.db1.t2 values (1,10),(1,5),(2,20),(2,15)",
                "default",
            )
            .expect("insert t2");

        // The subquery: SELECT t1.k FROM t1 WHERE t1.v = (SELECT min(t2.v) FROM t2 WHERE t2.k = t1.k)
        // k=1: min(t2.v) for k=1 = 5; t1.v=10 != 5 -> not selected
        // k=2: min(t2.v) for k=2 = 15; t1.v=20 != 15 -> not selected
        // k=3: no t2 rows -> NULL; t1.v=30 != NULL -> not selected
        // Result: no rows (empty)
        //
        // Alternatively use a query where some rows DO match:
        // WHERE t1.v = (SELECT min(t2.v) FROM t2 WHERE t2.k = t1.k)
        // Let's insert a t1 row where v=5 (k=1) so it matches min(t2.v)=5.
        session
            .execute_in_database("insert into ice.db1.t1 values (1,5)", "default")
            .expect("insert matching t1 row");

        let sql = "SELECT t1.k FROM t1 WHERE t1.v = (SELECT min(t2.v) FROM t2 WHERE t2.k = t1.k) ORDER BY 1";

        let result = run_scalar_query_i64(&session, sql).expect("corr-agg query");
        // k=1, v=5 matches min(t2.v for k=1)=5 — exactly one row
        assert_eq!(result, vec![Some(1)]);
    }

    // ---- Test 2: uncorrelated scalar ----------------------------------------

    #[test]
    fn scalar_subquery_uncorrelated_returns_expected_rows() {
        let warehouse = TempDir::new().expect("warehouse");
        let (_engine, session) = open_scalar_subquery_test_engine(&warehouse);

        session
            .execute_in_database("create table ice.db1.t1 (k bigint, v bigint)", "default")
            .expect("create t1");
        session
            .execute_in_database("create table ice.db1.t2 (k bigint, v bigint)", "default")
            .expect("create t2");
        session
            .execute_in_database(
                "insert into ice.db1.t1 values (1,10),(2,20),(3,30)",
                "default",
            )
            .expect("insert t1");
        session
            .execute_in_database(
                "insert into ice.db1.t2 values (1,100),(2,200),(3,300)",
                "default",
            )
            .expect("insert t2");

        // Uncorrelated scalar: v > (SELECT min(v) FROM t2). min(t2.v)=100 and all
        // t1.v are < 100, so the result is empty.
        let sql = "SELECT t1.k FROM t1 WHERE t1.v > (SELECT min(v) FROM t2) ORDER BY 1";

        let result = run_scalar_query_i64(&session, sql).expect("uncorrelated query");
        // t1.v > 100: v=20 no, v=30 no — wait, t1.v values are 10,20,30; min(t2.v)=100
        // None qualify. Let's verify.
        assert_eq!(result, vec![]);
    }

    // ---- Test 3: empty group -> NULL ----------------------------------------

    /// When some outer rows have no matching inner group, the correlated
    /// aggregate returns NULL (LEFT OUTER JOIN null-extension).
    #[test]
    fn scalar_subquery_empty_group_yields_null() {
        let warehouse = TempDir::new().expect("warehouse");
        let (_engine, session) = open_scalar_subquery_test_engine(&warehouse);

        session
            .execute_in_database("create table ice.db1.t1 (k bigint, v bigint)", "default")
            .expect("create t1");
        session
            .execute_in_database("create table ice.db1.t2 (k bigint, v bigint)", "default")
            .expect("create t2");
        // t1: k=1, k=2, k=3. t2 has only k=1 and k=2.
        // k=3 has no match → scalar is NULL.
        session
            .execute_in_database("insert into ice.db1.t1 values (1,0),(2,0),(3,0)", "default")
            .expect("insert t1");
        session
            .execute_in_database("insert into ice.db1.t2 values (1,10),(2,20)", "default")
            .expect("insert t2");

        // Project the scalar result (may be NULL) for each t1 row.
        let sql = "SELECT (SELECT min(t2.v) FROM t2 WHERE t2.k = t1.k) FROM t1 ORDER BY t1.k";

        let result = run_scalar_query_i64(&session, sql).expect("empty-group query");
        // k=1 → Some(10), k=2 → Some(20), k=3 → None (NULL)
        assert_eq!(result, vec![Some(10), Some(20), None]);
    }

    // ---- Test 4: count -> 0, not NULL ---------------------------------------

    /// Correlated count(*) scalar: must return 0 (not NULL) for outer
    /// rows with no matching inner group, thanks to the `ifnull(count,0)`
    /// normalization in ScalarApplyToJoin.
    #[test]
    fn scalar_subquery_count_zero_normalizes_correctly() {
        let warehouse = TempDir::new().expect("warehouse");
        let (_engine, session) = open_scalar_subquery_test_engine(&warehouse);

        session
            .execute_in_database("create table ice.db1.t1 (k bigint, v bigint)", "default")
            .expect("create t1");
        session
            .execute_in_database("create table ice.db1.t2 (k bigint, v bigint)", "default")
            .expect("create t2");
        session
            .execute_in_database("insert into ice.db1.t1 values (1,0),(2,0),(3,0)", "default")
            .expect("insert t1");
        session
            .execute_in_database("insert into ice.db1.t2 values (1,10),(1,20)", "default")
            .expect("insert t2");

        // count(*) for k=1 → 2, k=2 → 0, k=3 → 0 (not NULL)
        let sql = "SELECT (SELECT count(*) FROM t2 WHERE t2.k = t1.k) FROM t1 ORDER BY t1.k";

        let result = run_scalar_query_i64(&session, sql).expect("count-zero query");

        assert_eq!(
            result,
            vec![Some(2), Some(0), Some(0)],
            "count(*) must return 0 (not NULL) for unmatched outer rows (ifnull normalization)"
        );
    }

    // ---- Test 5: NULL correlation key -> NULL scalar ------------------------

    #[test]
    fn scalar_subquery_null_correlation_key_yields_null() {
        let warehouse = TempDir::new().expect("warehouse");
        let (_engine, session) = open_scalar_subquery_test_engine(&warehouse);

        session
            .execute_in_database("create table ice.db1.t1 (k bigint, v bigint)", "default")
            .expect("create t1");
        session
            .execute_in_database("create table ice.db1.t2 (k bigint, v bigint)", "default")
            .expect("create t2");
        // t1 has a NULL k; the correlated scalar must also be NULL.
        session
            .execute_in_database("insert into ice.db1.t1 values (1,0),(NULL,0)", "default")
            .expect("insert t1");
        session
            .execute_in_database("insert into ice.db1.t2 values (1,10),(1,5)", "default")
            .expect("insert t2");

        let sql =
            "SELECT (SELECT min(t2.v) FROM t2 WHERE t2.k = t1.k) FROM t1 ORDER BY t1.k NULLS LAST";

        let result = run_scalar_query_i64(&session, sql).expect("null-key query");
        // k=1 → Some(5), k=NULL → None (NULL: no match because NULL != NULL in the join)
        assert_eq!(result, vec![Some(5), None]);
    }

    // ---- Test 6: correlated non-agg single-row ------------------------------

    /// Correlated NON-aggregate scalar where the inner key is unique (≤1 row
    /// per outer key). The with-check path (count(1)/any_value/assert_true)
    /// must return the correct value without raising the row-check error.
    #[test]
    fn scalar_subquery_correlated_nonagg_single_row_ok() {
        let warehouse = TempDir::new().expect("warehouse");
        let (_engine, session) = open_scalar_subquery_test_engine(&warehouse);

        session
            .execute_in_database("create table ice.db1.t1 (k bigint, v bigint)", "default")
            .expect("create t1");
        session
            .execute_in_database("create table ice.db1.t2 (k bigint, v bigint)", "default")
            .expect("create t2");
        // t2.k is effectively unique: each k appears exactly once.
        session
            .execute_in_database("insert into ice.db1.t1 values (1,0),(2,0),(3,0)", "default")
            .expect("insert t1");
        session
            .execute_in_database("insert into ice.db1.t2 values (1,100),(2,200)", "default")
            .expect("insert t2");

        // Correlated non-aggregate: (SELECT t2.v FROM t2 WHERE t2.k = t1.k)
        // k=1 → 100, k=2 → 200, k=3 → NULL (no match)
        let sql = "SELECT (SELECT t2.v FROM t2 WHERE t2.k = t1.k) FROM t1 ORDER BY t1.k";

        let result =
            run_scalar_query_i64(&session, sql).expect("non-agg single-row query must succeed");

        assert_eq!(
            result,
            vec![Some(100), Some(200), None],
            "non-agg single-row must return correct values"
        );
    }

    // ---- Test 7: correlated non-agg MULTI-ROW -> must ERROR -----------------
    //
    // This is the most important test: the assert_true(cnt IS NULL OR cnt <= 1,
    // 'correlate scalar subquery result must 1 row') check must fire at runtime.
    #[test]
    fn scalar_subquery_correlated_nonagg_multirow_errors_with_apply_guard() {
        let warehouse = TempDir::new().expect("warehouse");
        let (_engine, session) = open_scalar_subquery_test_engine(&warehouse);

        session
            .execute_in_database("create table ice.db1.t1 (k bigint, v bigint)", "default")
            .expect("create t1");
        session
            .execute_in_database("create table ice.db1.t2 (k bigint, v bigint)", "default")
            .expect("create t2");
        // t2 has TWO rows with k=1, so the correlated scalar for t1.k=1 returns >1 row.
        session
            .execute_in_database("insert into ice.db1.t1 values (1,0)", "default")
            .expect("insert t1");
        session
            .execute_in_database("insert into ice.db1.t2 values (1,100),(1,200)", "default")
            .expect("insert t2");

        let sql = "SELECT (SELECT t2.v FROM t2 WHERE t2.k = t1.k) FROM t1";
        expect_subquery_error(&session, sql, "correlate scalar subquery result must 1 row");
    }

    // ---- Test 8: two correlated scalar subqueries in one query ---------------

    /// Two correlated scalar subqueries over different tables in the same SELECT
    /// list. M1a stacks them as left-deep Apply nodes; M1b decorrelates each one
    /// to a LEFT OUTER JOIN. This test verifies that both decorrelations succeed
    /// and that results include NULL extension for
    /// outer rows that only match one of the two subqueries.
    ///
    /// Schema:
    ///   t1(k BIGINT, v BIGINT) — outer table
    ///   t2(k BIGINT, v BIGINT) — has matches for k=1 and k=2, but NOT k=3
    ///   t3(k BIGINT, v BIGINT) — has matches for k=1 and k=3, but NOT k=2
    ///
    /// Query:
    ///   SELECT t1.k,
    ///          (SELECT min(t2.v) FROM t2 WHERE t2.k = t1.k),
    ///          (SELECT max(t3.v) FROM t3 WHERE t3.k = t1.k)
    ///   FROM t1 ORDER BY t1.k
    ///
    /// Expected results (k, min_t2, max_t3):
    ///   k=1 → (1, 5,  90)   — both subqueries match
    ///   k=2 → (2, 20, NULL) — only t2 matches; t3 NULL-extends
    ///   k=3 → (3, NULL, 30) — only t3 matches; t2 NULL-extends
    #[test]
    fn scalar_subquery_multiple_in_one_query_returns_expected_rows() {
        let warehouse = TempDir::new().expect("warehouse");
        let (_engine, session) = open_scalar_subquery_test_engine(&warehouse);

        session
            .execute_in_database("create table ice.db1.t1 (k bigint, v bigint)", "default")
            .expect("create t1");
        session
            .execute_in_database("create table ice.db1.t2 (k bigint, v bigint)", "default")
            .expect("create t2");
        session
            .execute_in_database("create table ice.db1.t3 (k bigint, v bigint)", "default")
            .expect("create t3");

        // t1: three outer rows with distinct keys
        session
            .execute_in_database("insert into ice.db1.t1 values (1,0),(2,0),(3,0)", "default")
            .expect("insert t1");
        // t2: k=1 has two rows (min=5), k=2 has one row (min=20), k=3 absent
        session
            .execute_in_database(
                "insert into ice.db1.t2 values (1,5),(1,10),(2,20)",
                "default",
            )
            .expect("insert t2");
        // t3: k=1 has one row (max=90), k=2 absent, k=3 has one row (max=30)
        session
            .execute_in_database("insert into ice.db1.t3 values (1,90),(3,30)", "default")
            .expect("insert t3");

        let sql = "SELECT t1.k, \
                   (SELECT min(t2.v) FROM t2 WHERE t2.k = t1.k), \
                   (SELECT max(t3.v) FROM t3 WHERE t3.k = t1.k) \
                   FROM t1 ORDER BY t1.k";

        let result = run_scalar_query_multi_col(&session, sql).expect("multi-scalar query");

        // Verify the concrete expected values:
        //   k=1: min(t2.v for k=1)=5,  max(t3.v for k=1)=90
        //   k=2: min(t2.v for k=2)=20, max(t3.v for k=2)=NULL (no t3 rows)
        //   k=3: min(t2.v for k=3)=NULL (no t2 rows), max(t3.v for k=3)=30
        assert_eq!(
            result,
            vec![
                vec![Some(1), Some(5), Some(90)],
                vec![Some(2), Some(20), None],
                vec![Some(3), None, Some(30)],
            ],
            "unexpected result values for multiple scalar subqueries"
        );
    }

    // -------------------------------------------------------------------------
    // EXISTS / IN Apply-to-Join — end-to-end parity tests (Task 7).
    // -------------------------------------------------------------------------

    #[test]
    fn exists_correlated_returns_expected_rows() {
        let warehouse = TempDir::new().expect("warehouse");
        let (_engine, session) = open_scalar_subquery_test_engine(&warehouse);
        create_kv_tables(
            &session,
            "(1,10),(2,20),(3,30),(NULL,40)",
            "(1,100),(1,101),(3,300),(NULL,999)",
        );

        assert_subquery_result_i64(
            &session,
            "SELECT t1.k FROM t1 WHERE EXISTS (SELECT 1 FROM t2 WHERE t2.k = t1.k) ORDER BY 1",
            vec![Some(1), Some(3)],
        );
    }

    #[test]
    fn not_exists_correlated_returns_expected_rows() {
        let warehouse = TempDir::new().expect("warehouse");
        let (_engine, session) = open_scalar_subquery_test_engine(&warehouse);
        create_kv_tables(
            &session,
            "(1,10),(2,20),(3,30),(NULL,40)",
            "(1,100),(3,300),(NULL,999)",
        );

        assert_subquery_result_i64(
            &session,
            "SELECT t1.k FROM t1 WHERE NOT EXISTS (SELECT 1 FROM t2 WHERE t2.k = t1.k) ORDER BY t1.k NULLS LAST",
            vec![Some(2), None],
        );
    }

    #[test]
    fn exists_uncorrelated_returns_expected_rows() {
        let warehouse = TempDir::new().expect("warehouse");
        let (_engine, session) = open_scalar_subquery_test_engine(&warehouse);
        create_kv_tables(&session, "(1,10),(2,20),(3,30)", "(1,101),(2,200)");

        assert_subquery_result_i64(
            &session,
            "SELECT t1.k FROM t1 WHERE EXISTS (SELECT 1 FROM t2 WHERE t2.v > 100) ORDER BY 1",
            vec![Some(1), Some(2), Some(3)],
        );
        assert_subquery_result_i64(
            &session,
            "SELECT t1.k FROM t1 WHERE EXISTS (SELECT 1 FROM t2 WHERE t2.v > 1000) ORDER BY 1",
            vec![],
        );
    }

    #[test]
    fn in_correlated_returns_expected_rows() {
        let warehouse = TempDir::new().expect("warehouse");
        let (_engine, session) = open_scalar_subquery_test_engine(&warehouse);
        create_kv_tables(
            &session,
            "(1,10),(2,20),(3,30),(4,NULL)",
            "(1,10),(1,11),(2,99),(3,30),(4,NULL)",
        );

        assert_subquery_result_i64(
            &session,
            "SELECT t1.k FROM t1 WHERE t1.v IN (SELECT t2.v FROM t2 WHERE t2.k = t1.k) ORDER BY 1",
            vec![Some(1), Some(3)],
        );
    }

    #[test]
    fn in_uncorrelated_returns_expected_rows() {
        let warehouse = TempDir::new().expect("warehouse");
        let (_engine, session) = open_scalar_subquery_test_engine(&warehouse);
        create_kv_tables(
            &session,
            "(1,10),(2,20),(3,30),(4,NULL)",
            "(9,10),(9,30),(9,NULL)",
        );

        assert_subquery_result_i64(
            &session,
            "SELECT t1.k FROM t1 WHERE t1.v IN (SELECT t2.v FROM t2) ORDER BY 1",
            vec![Some(1), Some(3)],
        );
    }

    #[test]
    fn in_inside_or_with_build_null_preserves_unknown() {
        let warehouse = TempDir::new().expect("warehouse");
        let (_engine, session) = open_scalar_subquery_test_engine(&warehouse);
        create_kv_tables(&session, "(1,10),(2,20),(3,NULL)", "(9,10),(9,NULL)");

        assert_subquery_result_i64(
            &session,
            "SELECT t1.k FROM t1 \
             WHERE (t1.v IN (SELECT t2.v FROM t2)) OR false \
             ORDER BY 1",
            vec![Some(1)],
        );
    }

    #[test]
    fn in_projection_with_build_null_preserves_unknown() {
        let warehouse = TempDir::new().expect("warehouse");
        let (_engine, session) = open_scalar_subquery_test_engine(&warehouse);
        create_kv_tables(&session, "(1,10),(2,20),(3,NULL)", "(9,10),(9,NULL)");

        assert_subquery_result_i64(
            &session,
            "SELECT CASE \
                    WHEN t1.v IN (SELECT t2.v FROM t2) THEN 1 \
                    WHEN (t1.v IN (SELECT t2.v FROM t2)) IS NULL THEN NULL \
                    ELSE 0 \
                END \
             FROM t1 ORDER BY t1.k",
            vec![Some(1), None, None],
        );
    }

    #[test]
    fn not_in_uncorrelated_no_null() {
        let warehouse = TempDir::new().expect("warehouse");
        let (_engine, session) = open_scalar_subquery_test_engine(&warehouse);
        create_kv_tables(&session, "(1,10),(2,20),(3,30)", "(9,20),(9,40)");

        assert_subquery_result_i64(
            &session,
            "SELECT t1.k FROM t1 WHERE t1.v NOT IN (SELECT t2.v FROM t2) ORDER BY 1",
            vec![Some(1), Some(3)],
        );
    }

    #[test]
    fn not_in_uncorrelated_build_null() {
        let warehouse = TempDir::new().expect("warehouse");
        let (_engine, session) = open_scalar_subquery_test_engine(&warehouse);
        create_kv_tables(&session, "(1,10),(2,20),(3,30)", "(9,20),(9,NULL)");

        assert_subquery_result_i64(
            &session,
            "SELECT t1.k FROM t1 WHERE t1.v NOT IN (SELECT t2.v FROM t2) ORDER BY 1",
            vec![],
        );
    }

    #[test]
    fn not_in_inside_or_with_build_null_is_unknown() {
        let warehouse = TempDir::new().expect("warehouse");
        let (_engine, session) = open_scalar_subquery_test_engine(&warehouse);
        create_kv_tables(&session, "(1,10),(2,20)", "(9,NULL)");

        assert_subquery_result_i64(
            &session,
            "SELECT t1.k FROM t1 \
             WHERE (t1.v NOT IN (SELECT t2.v FROM t2)) OR false \
             ORDER BY 1",
            vec![],
        );
    }

    #[test]
    fn not_in_inside_or_with_probe_null_and_empty_build_is_true() {
        let warehouse = TempDir::new().expect("warehouse");
        let (_engine, session) = open_scalar_subquery_test_engine(&warehouse);
        create_kv_tables(&session, "(1,NULL),(2,20)", "");

        assert_subquery_result_i64(
            &session,
            "SELECT t1.k FROM t1 \
             WHERE (t1.v NOT IN (SELECT t2.v FROM t2)) OR false \
             ORDER BY 1",
            vec![Some(1), Some(2)],
        );
    }

    #[test]
    fn not_in_join_on_with_build_null_is_unknown() {
        let warehouse = TempDir::new().expect("warehouse");
        let (_engine, session) = open_scalar_subquery_test_engine(&warehouse);
        create_kv_tables(&session, "(1,10),(2,20)", "(9,NULL)");
        session
            .execute_in_database("create table ice.db1.t3 (k bigint, v bigint)", "default")
            .expect("create t3");
        session
            .execute_in_database("insert into ice.db1.t3 values (100,0)", "default")
            .expect("insert t3");

        assert_subquery_result_i64(
            &session,
            "SELECT t1.k FROM t1 JOIN t3 \
             ON t1.v NOT IN (SELECT t2.v FROM t2) \
             ORDER BY 1",
            vec![],
        );
    }

    #[test]
    fn not_in_join_on_with_probe_null_is_unknown() {
        let warehouse = TempDir::new().expect("warehouse");
        let (_engine, session) = open_scalar_subquery_test_engine(&warehouse);
        create_kv_tables(&session, "(1,NULL),(2,20)", "(9,10)");
        session
            .execute_in_database("create table ice.db1.t3 (k bigint, v bigint)", "default")
            .expect("create t3");
        session
            .execute_in_database("insert into ice.db1.t3 values (100,0)", "default")
            .expect("insert t3");

        assert_subquery_result_i64(
            &session,
            "SELECT t1.k FROM t1 JOIN t3 \
             ON t1.v NOT IN (SELECT t2.v FROM t2) \
             ORDER BY 1",
            vec![Some(2)],
        );
    }

    #[test]
    fn not_in_uncorrelated_probe_null() {
        let warehouse = TempDir::new().expect("warehouse");
        let (_engine, session) = open_scalar_subquery_test_engine(&warehouse);
        create_kv_tables(&session, "(1,10),(2,NULL),(3,30)", "(9,20),(9,40)");

        assert_subquery_result_i64(
            &session,
            "SELECT t1.k FROM t1 WHERE t1.v NOT IN (SELECT t2.v FROM t2) ORDER BY 1",
            vec![Some(1), Some(3)],
        );
    }

    #[test]
    fn not_in_correlated_conjunct() {
        let warehouse = TempDir::new().expect("warehouse");
        let (_engine, session) = open_scalar_subquery_test_engine(&warehouse);
        create_kv_tables(
            &session,
            "(1,10),(2,20),(3,30),(4,40)",
            "(1,20),(1,30),(2,20),(2,NULL),(3,NULL),(3,40)",
        );

        assert_subquery_result_i64(
            &session,
            "SELECT t1.k FROM t1 WHERE t1.v NOT IN (SELECT t2.v FROM t2 WHERE t2.k = t1.k) ORDER BY 1",
            vec![Some(1), Some(4)],
        );
    }

    #[test]
    fn multi_subquery_in_and_exists_returns_expected_rows() {
        let warehouse = TempDir::new().expect("warehouse");
        let (_engine, session) = open_scalar_subquery_test_engine(&warehouse);
        create_kv_tables(
            &session,
            "(1,10),(2,20),(3,30),(4,40)",
            "(1,10),(1,150),(2,20),(2,99),(3,300),(4,400)",
        );

        assert_subquery_result_i64(
            &session,
            "SELECT t1.k FROM t1 \
             WHERE t1.v IN (SELECT t2.v FROM t2 WHERE t2.k = t1.k) \
               AND EXISTS (SELECT 1 FROM t2 WHERE t2.k = t1.k AND t2.v > 100) \
             ORDER BY 1",
            vec![Some(1)],
        );
    }
}
