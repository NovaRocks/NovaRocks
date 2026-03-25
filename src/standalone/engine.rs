#![allow(dead_code)]

use std::collections::{BTreeMap, HashMap};
use std::path::{Path, PathBuf};
use std::sync::{Arc, RwLock};
use std::time::Duration;

use arrow::array::{ArrayRef, BooleanArray, Int32Array, Int64Array, StringArray, UInt32Array};
use arrow::compute::kernels::cmp::{eq, gt, gt_eq, lt, lt_eq, neq};
use arrow::compute::{
    SortColumn, SortOptions, concat_batches, filter_record_batch, lexsort_to_indices,
    take_record_batch,
};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use futures::TryStreamExt;
use tokio::runtime::Handle;

use crate::descriptors;
use crate::exec::chunk::{Chunk, ChunkSchema};
use crate::exec::expr::ExprArena;
use crate::exec::node::{ExecPlan, push_down_local_runtime_filters};
use crate::exec::operators::{ResultSinkFactory, ResultSinkHandle};
use crate::exec::pipeline::executor::execute_plan_with_pipeline;
use crate::internal_service;
use crate::lower::thrift::layout::{build_tuple_slot_order, reorder_tuple_slots};
use crate::lower::thrift::lower_plan;
use crate::lower::thrift::type_lowering::scalar_type_desc;
use crate::novarocks_config;
use crate::opcodes;
use crate::partitions;
use crate::plan_nodes;
use crate::runtime::global_async_runtime::data_block_on;
use crate::runtime::runtime_state::RuntimeState;
use crate::service::internal_service::execute_plan_fragment_sync;
use crate::types;

use super::catalog::{
    ColumnDef, DEFAULT_DATABASE, InMemoryCatalog, TableDef, TableStorage, build_parquet_table,
    normalize_identifier,
};
use super::iceberg::{
    IcebergCatalogRegistry, IcebergLoadedTable, create_namespace as create_iceberg_namespace,
    create_table as create_iceberg_table, drop_namespace as drop_iceberg_namespace,
    drop_table as drop_iceberg_table, insert_rows as insert_iceberg_rows,
    list_tables as list_iceberg_tables, load_table as load_iceberg_table,
    namespace_exists as iceberg_namespace_exists,
    register_existing_table as register_existing_iceberg_table,
};
use super::store::{MetadataSnapshot, SqliteMetadataStore, StoredIcebergTable, StoredLocalTable};
use crate::sql::{
    AnalyzedStatement, AnalyzerContext, ColumnRef, CompareOp, CreateTableKind, Expr, InsertSource,
    Literal, ObjectName, OptimizedStatement, OrderByExpr, ProjectionItem, QuerySource, QueryStmt,
    Statement, analyze_statement, build_local_query_plan_fragment, collect_query_result,
    optimize_statement, parse_sql,
};

const STANDALONE_SCAN_TUPLE_ID: types::TTupleId = 1;
const STANDALONE_PROJECT_TUPLE_ID: types::TTupleId = 2;
const STANDALONE_PROJECT_NODE_ID: types::TPlanNodeId = 1;
const STANDALONE_SCAN_NODE_ID: types::TPlanNodeId = 2;

#[derive(Clone, Debug, Default)]
pub struct StandaloneOptions {
    pub config_path: Option<PathBuf>,
    pub metadata_db_path: Option<PathBuf>,
}

#[derive(Clone, Debug)]
pub struct QueryResultColumn {
    pub name: String,
    pub data_type: DataType,
    pub nullable: bool,
}

#[derive(Clone, Debug)]
pub struct QueryResult {
    pub columns: Vec<QueryResultColumn>,
    pub chunks: Vec<Chunk>,
}

#[derive(Clone, Debug)]
pub(crate) enum StatementResult {
    Query(QueryResult),
    Ok,
}

impl QueryResult {
    pub fn row_count(&self) -> usize {
        self.chunks.iter().map(Chunk::len).sum()
    }

    pub fn into_chunks(self) -> Vec<Chunk> {
        self.chunks
    }
}

#[derive(Default)]
struct StandaloneState {
    catalog: RwLock<InMemoryCatalog>,
    iceberg_catalogs: RwLock<IcebergCatalogRegistry>,
    metadata_store: Option<SqliteMetadataStore>,
}

#[derive(Clone, Debug)]
struct BoundScanColumn {
    name: String,
    data_type: DataType,
    nullable: bool,
    scan_slot_id: types::TSlotId,
}

#[derive(Clone, Debug)]
struct BoundOutputColumn {
    name: String,
    data_type: DataType,
    nullable: bool,
    scan_column_index: usize,
    output_slot_id: Option<types::TSlotId>,
}

#[derive(Clone, Debug)]
struct BoundPredicate {
    scan_column_index: usize,
    op: CompareOp,
    literal: Literal,
}

#[derive(Clone, Debug)]
struct BoundQuery {
    scan_columns: Vec<BoundScanColumn>,
    output_columns: Vec<BoundOutputColumn>,
    predicate: Option<BoundPredicate>,
    needs_project: bool,
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
        match opts.config_path.as_deref() {
            Some(path) => {
                novarocks_config::init_from_path(path)
                    .map_err(|e| format!("load config failed: {e}"))?;
            }
            None => {
                novarocks_config::init_from_env_or_default()
                    .map_err(|e| format!("load config failed: {e}"))?;
            }
        }
        let metadata_store = resolve_metadata_store(
            opts.metadata_db_path.as_deref(),
            opts.config_path.as_deref(),
        )?;
        let inner = Arc::new(StandaloneState {
            metadata_store,
            ..Default::default()
        });
        restore_metadata_if_needed(&inner)?;
        Ok(Self { inner })
    }

    pub fn session(&self) -> StandaloneSession {
        StandaloneSession {
            inner: Arc::clone(&self.inner),
        }
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
        let persisted_path = match &table.storage {
            TableStorage::LocalParquetFile { path } => path.clone(),
        };
        let mut guard = self
            .inner
            .catalog
            .write()
            .expect("standalone catalog write lock");
        guard.register(database_name, table)?;
        persist_local_table_if_needed(
            &self.inner,
            &normalize_identifier(database_name)?,
            &normalize_identifier(table_name)?,
            &persisted_path,
        )
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
}

impl StandaloneSession {
    pub fn query(&self, sql: &str) -> Result<QueryResult, String> {
        match self.execute_in_context(sql, None, DEFAULT_DATABASE)? {
            StatementResult::Query(result) => Ok(result),
            StatementResult::Ok => Err("statement did not return rows".to_string()),
        }
    }

    pub(crate) fn execute_in_database(
        &self,
        sql: &str,
        current_database: &str,
    ) -> Result<StatementResult, String> {
        self.execute_in_context(sql, None, current_database)
    }

    pub(crate) fn execute_in_context(
        &self,
        sql: &str,
        current_catalog: Option<&str>,
        current_database: &str,
    ) -> Result<StatementResult, String> {
        let stmt = parse_sql(sql)?;
        let local_catalog = self
            .inner
            .catalog
            .read()
            .expect("standalone catalog read lock");
        let iceberg_catalogs = self
            .inner
            .iceberg_catalogs
            .read()
            .expect("standalone iceberg catalog read lock");
        let analyzed = analyze_statement(
            stmt,
            AnalyzerContext {
                current_catalog,
                current_database,
            },
            &local_catalog,
            &iceberg_catalogs,
        )?;
        drop(iceberg_catalogs);
        drop(local_catalog);

        match optimize_statement(analyzed)? {
            OptimizedStatement::Query(query_plan) => {
                execute_optimized_query_statement(&self.inner, query_plan)
                    .map(StatementResult::Query)
            }
            OptimizedStatement::Passthrough(AnalyzedStatement::Passthrough(
                Statement::CreateCatalog(stmt),
            )) => {
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
            OptimizedStatement::Passthrough(AnalyzedStatement::Passthrough(
                Statement::CreateDatabase(stmt),
            )) => execute_create_database_statement(&self.inner, &stmt.name, current_catalog),
            OptimizedStatement::Passthrough(AnalyzedStatement::Passthrough(
                Statement::CreateTable(stmt),
            )) => {
                execute_create_table_statement(&self.inner, stmt, current_catalog, current_database)
            }
            OptimizedStatement::Passthrough(AnalyzedStatement::Passthrough(
                Statement::DropCatalog(stmt),
            )) => execute_drop_catalog_statement(&self.inner, &stmt.name, stmt.if_exists),
            OptimizedStatement::Passthrough(AnalyzedStatement::Passthrough(
                Statement::DropDatabase(stmt),
            )) => execute_drop_database_statement(
                &self.inner,
                &stmt.name,
                current_catalog,
                stmt.if_exists,
                stmt.force,
            ),
            OptimizedStatement::Passthrough(AnalyzedStatement::Passthrough(
                Statement::DropTable(stmt),
            )) => execute_drop_table_statement(
                &self.inner,
                &stmt.name,
                current_catalog,
                current_database,
                stmt.if_exists,
                stmt.force,
            ),
            OptimizedStatement::Passthrough(AnalyzedStatement::Passthrough(Statement::Insert(
                stmt,
            ))) => execute_insert_statement(
                &self.inner,
                &stmt.table,
                &stmt.source,
                current_catalog,
                current_database,
            ),
            OptimizedStatement::Passthrough(AnalyzedStatement::Passthrough(Statement::Query(
                _,
            ))) => Err("unexpected query passthrough".to_string()),
            OptimizedStatement::Passthrough(AnalyzedStatement::Query(_)) => {
                Err("unexpected analyzed query passthrough".to_string())
            }
        }
    }
}

fn execute_optimized_query_statement(
    _state: &Arc<StandaloneState>,
    query_plan: crate::sql::RelQueryPlan,
) -> Result<QueryResult, String> {
    match &query_plan.source {
        QuerySource::Local { .. } => {
            let planned = build_local_query_plan_fragment(&query_plan)?;
            let exec_result = execute_plan_fragment_sync(planned.request)?;
            let batch = collect_query_result(exec_result.finst_id, &planned.columns)?;
            let result = QueryResult {
                columns: planned
                    .columns
                    .iter()
                    .map(|column| QueryResultColumn {
                        name: column.name.clone(),
                        data_type: column.data_type.clone(),
                        nullable: column.nullable,
                    })
                    .collect(),
                chunks: vec![record_batch_to_chunk(batch)?],
            };
            apply_order_by_if_needed(result, &planned.order_by)
        }
        QuerySource::Iceberg { loaded, .. } => {
            let result = execute_iceberg_query(loaded, &query_plan.bound)?;
            apply_order_by_if_needed(result, &query_plan.order_by)
        }
    }
}

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
    restore_local_catalog(state, &snapshot)?;
    restore_iceberg_catalogs(state, &snapshot)?;
    Ok(())
}

fn restore_local_catalog(
    state: &Arc<StandaloneState>,
    snapshot: &MetadataSnapshot,
) -> Result<(), String> {
    let mut guard = state
        .catalog
        .write()
        .expect("standalone catalog write lock");
    for database_name in &snapshot.local_databases {
        guard.create_database(database_name)?;
    }
    for StoredLocalTable {
        database,
        table,
        path,
    } in &snapshot.local_tables
    {
        let table = build_parquet_table(table, path)?;
        guard.register(database, table)?;
    }
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

fn persist_local_database_if_needed(
    state: &Arc<StandaloneState>,
    database_name: &str,
) -> Result<(), String> {
    if let Some(store) = state.metadata_store.as_ref()
        && database_name != DEFAULT_DATABASE
    {
        store.upsert_local_database(database_name)?;
    }
    Ok(())
}

fn persist_local_table_if_needed(
    state: &Arc<StandaloneState>,
    database_name: &str,
    table_name: &str,
    path: &Path,
) -> Result<(), String> {
    if let Some(store) = state.metadata_store.as_ref() {
        if database_name != DEFAULT_DATABASE {
            store.upsert_local_database(database_name)?;
        }
        store.upsert_local_table(database_name, table_name, path)?;
    }
    Ok(())
}

fn delete_local_table_if_needed(
    state: &Arc<StandaloneState>,
    database_name: &str,
    table_name: &str,
) -> Result<(), String> {
    if let Some(store) = state.metadata_store.as_ref() {
        store.delete_local_table(database_name, table_name)?;
    }
    Ok(())
}

fn delete_local_database_if_needed(
    state: &Arc<StandaloneState>,
    database_name: &str,
) -> Result<(), String> {
    if let Some(store) = state.metadata_store.as_ref()
        && database_name != DEFAULT_DATABASE
    {
        store.delete_local_database(database_name)?;
    }
    Ok(())
}

fn persist_iceberg_catalog_if_needed(
    state: &Arc<StandaloneState>,
    catalog_name: &str,
    properties: &[(String, String)],
) -> Result<(), String> {
    if let Some(store) = state.metadata_store.as_ref() {
        store.upsert_iceberg_catalog(catalog_name, properties)?;
    }
    Ok(())
}

fn persist_iceberg_namespace_if_needed(
    state: &Arc<StandaloneState>,
    catalog_name: &str,
    namespace_name: &str,
) -> Result<(), String> {
    if let Some(store) = state.metadata_store.as_ref() {
        store.upsert_iceberg_namespace(catalog_name, namespace_name)?;
    }
    Ok(())
}

fn persist_iceberg_table_if_needed(
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

fn delete_iceberg_table_if_needed(
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

fn delete_iceberg_namespace_if_needed(
    state: &Arc<StandaloneState>,
    catalog_name: &str,
    namespace_name: &str,
) -> Result<(), String> {
    if let Some(store) = state.metadata_store.as_ref() {
        store.delete_iceberg_namespace(catalog_name, namespace_name)?;
    }
    Ok(())
}

fn delete_iceberg_catalog_if_needed(
    state: &Arc<StandaloneState>,
    catalog_name: &str,
) -> Result<(), String> {
    if let Some(store) = state.metadata_store.as_ref() {
        store.delete_iceberg_catalog(catalog_name)?;
    }
    Ok(())
}

#[derive(Clone, Debug)]
struct ResolvedLocalTableName {
    database: String,
    table: String,
}

#[derive(Clone, Debug)]
struct ResolvedIcebergNamespaceName {
    catalog: String,
    namespace: String,
}

#[derive(Clone, Debug)]
struct ResolvedIcebergTableName {
    catalog: String,
    namespace: String,
    table: String,
}

fn execute_query_statement(
    state: &Arc<StandaloneState>,
    query: &QueryStmt,
    current_catalog: Option<&str>,
    current_database: &str,
) -> Result<QueryResult, String> {
    if current_catalog.is_none() {
        match query.from.name.parts.len() {
            1 | 2 => {
                let resolved = resolve_local_table_name(&query.from.name, current_database)?;
                let guard = state.catalog.read().expect("standalone catalog read lock");
                let table = guard.get(&resolved.database, &resolved.table)?;
                let bound = bind_query(query, &table.columns)?;
                let result = execute_query(&table, &bound)?;
                apply_order_by_if_needed(result, &query.order_by)
            }
            3 => {
                let resolved = resolve_iceberg_table_name_explicit(&query.from.name)?;
                let guard = state
                    .iceberg_catalogs
                    .read()
                    .expect("standalone iceberg catalog read lock");
                let entry = guard.get(&resolved.catalog)?;
                let loaded = load_iceberg_table(&entry, &resolved.namespace, &resolved.table)?;
                let bound = bind_query(query, &loaded.columns)?;
                let analyzed_bound = legacy_bound_query_to_sql_bound_query(&bound);
                let result = execute_iceberg_query(&loaded, &analyzed_bound)?;
                apply_order_by_if_needed(result, &query.order_by)
            }
            _ => Err(format!(
                "unsupported table name `{}`; expected `<table>`, `<database>.<table>`, or `<catalog>.<database>.<table>`",
                query.from.name.parts.join(".")
            )),
        }
    } else {
        let resolved =
            resolve_iceberg_table_name(query.from.name.clone(), current_catalog, current_database)?;
        let guard = state
            .iceberg_catalogs
            .read()
            .expect("standalone iceberg catalog read lock");
        let entry = guard.get(&resolved.catalog)?;
        let loaded = load_iceberg_table(&entry, &resolved.namespace, &resolved.table)?;
        let bound = bind_query(query, &loaded.columns)?;
        let analyzed_bound = legacy_bound_query_to_sql_bound_query(&bound);
        let result = execute_iceberg_query(&loaded, &analyzed_bound)?;
        apply_order_by_if_needed(result, &query.order_by)
    }
}

fn legacy_bound_query_to_sql_bound_query(bound: &BoundQuery) -> crate::sql::BoundQuery {
    crate::sql::BoundQuery {
        scan_columns: bound
            .scan_columns
            .iter()
            .map(|column| crate::sql::BoundScanColumn {
                name: column.name.clone(),
                data_type: column.data_type.clone(),
                nullable: column.nullable,
                scan_slot_id: column.scan_slot_id,
            })
            .collect(),
        output_columns: bound
            .output_columns
            .iter()
            .map(|column| crate::sql::BoundOutputColumn {
                name: column.name.clone(),
                data_type: column.data_type.clone(),
                nullable: column.nullable,
                scan_column_index: column.scan_column_index,
                output_slot_id: column.output_slot_id,
            })
            .collect(),
        predicate: bound
            .predicate
            .as_ref()
            .map(|predicate| crate::sql::BoundPredicate {
                scan_column_index: predicate.scan_column_index,
                op: predicate.op,
                literal: predicate.literal.clone(),
            }),
        needs_project: bound.needs_project,
    }
}

fn execute_drop_catalog_statement(
    state: &Arc<StandaloneState>,
    catalog_name: &str,
    if_exists: bool,
) -> Result<StatementResult, String> {
    let mut guard = state
        .iceberg_catalogs
        .write()
        .expect("standalone iceberg catalog write lock");
    match guard.drop_catalog(catalog_name) {
        Ok(()) => {
            drop(guard);
            delete_iceberg_catalog_if_needed(state, &normalize_identifier(catalog_name)?)?;
            Ok(StatementResult::Ok)
        }
        Err(err) if if_exists && err.contains("unknown catalog") => Ok(StatementResult::Ok),
        Err(err) => Err(err),
    }
}

fn execute_drop_database_statement(
    state: &Arc<StandaloneState>,
    name: &ObjectName,
    current_catalog: Option<&str>,
    if_exists: bool,
    force: bool,
) -> Result<StatementResult, String> {
    if current_catalog.is_none() && name.parts.len() == 1 {
        let mut guard = state
            .catalog
            .write()
            .expect("standalone catalog write lock");
        match guard.drop_database(name.leaf()) {
            Ok(()) => {
                let database_name = normalize_identifier(name.leaf())?;
                drop(guard);
                delete_local_database_if_needed(state, &database_name)?;
                return Ok(StatementResult::Ok);
            }
            Err(err) if if_exists && err.contains("unknown database") => {
                return Ok(StatementResult::Ok);
            }
            Err(err) => return Err(err),
        }
    }

    let resolved = resolve_iceberg_namespace_name(name.clone(), current_catalog)?;
    let guard = state
        .iceberg_catalogs
        .read()
        .expect("standalone iceberg catalog read lock");
    let entry = guard.get(&resolved.catalog)?;
    let namespace_exists = iceberg_namespace_exists(&entry, &resolved.namespace)?;
    if !namespace_exists {
        return if if_exists {
            Ok(StatementResult::Ok)
        } else {
            Err(format!("unknown database `{}`", name.parts.join(".")))
        };
    }
    if force {
        for table_name in list_iceberg_tables(&entry, &resolved.namespace)? {
            drop_iceberg_table(&entry, &resolved.namespace, &table_name)?;
            delete_iceberg_table_if_needed(
                state,
                &resolved.catalog,
                &resolved.namespace,
                &table_name,
            )?;
        }
    }
    match drop_iceberg_namespace(&entry, &resolved.namespace) {
        Ok(()) => {
            delete_iceberg_namespace_if_needed(state, &resolved.catalog, &resolved.namespace)?;
            Ok(StatementResult::Ok)
        }
        Err(err) if if_exists && err.contains("namespace") => Ok(StatementResult::Ok),
        Err(err) => Err(err),
    }
}

fn execute_create_database_statement(
    state: &Arc<StandaloneState>,
    name: &ObjectName,
    current_catalog: Option<&str>,
) -> Result<StatementResult, String> {
    if current_catalog.is_none() && name.parts.len() == 1 {
        let mut guard = state
            .catalog
            .write()
            .expect("standalone catalog write lock");
        guard.create_database(name.leaf())?;
        let database_name = normalize_identifier(name.leaf())?;
        drop(guard);
        persist_local_database_if_needed(state, &database_name)?;
        return Ok(StatementResult::Ok);
    }

    let resolved = resolve_iceberg_namespace_name(name.clone(), current_catalog)?;
    let guard = state
        .iceberg_catalogs
        .read()
        .expect("standalone iceberg catalog read lock");
    let entry = guard.get(&resolved.catalog)?;
    create_iceberg_namespace(&entry, &resolved.namespace)?;
    persist_iceberg_namespace_if_needed(state, &resolved.catalog, &resolved.namespace)?;
    Ok(StatementResult::Ok)
}

fn execute_create_table_statement(
    state: &Arc<StandaloneState>,
    stmt: crate::sql::CreateTableStmt,
    current_catalog: Option<&str>,
    current_database: &str,
) -> Result<StatementResult, String> {
    match stmt.kind {
        CreateTableKind::LocalParquet { path } => {
            let resolved = resolve_local_table_name(&stmt.name, current_database)?;
            let table = build_parquet_table(stmt.name.leaf(), &path)?;
            let persisted_path = match &table.storage {
                TableStorage::LocalParquetFile { path } => path.clone(),
            };
            let mut guard = state
                .catalog
                .write()
                .expect("standalone catalog write lock");
            guard.register(&resolved.database, table)?;
            drop(guard);
            persist_local_table_if_needed(
                state,
                &resolved.database,
                &resolved.table,
                &persisted_path,
            )?;
            Ok(StatementResult::Ok)
        }
        CreateTableKind::Iceberg {
            columns,
            properties,
        } => {
            let resolved =
                resolve_iceberg_table_name(stmt.name, current_catalog, current_database)?;
            let guard = state
                .iceberg_catalogs
                .read()
                .expect("standalone iceberg catalog read lock");
            let entry = guard.get(&resolved.catalog)?;
            create_iceberg_table(
                &entry,
                &resolved.namespace,
                &resolved.table,
                &columns,
                &properties,
            )?;
            persist_iceberg_table_if_needed(
                state,
                &resolved.catalog,
                &resolved.namespace,
                &resolved.table,
            )?;
            Ok(StatementResult::Ok)
        }
    }
}

fn execute_drop_table_statement(
    state: &Arc<StandaloneState>,
    name: &ObjectName,
    current_catalog: Option<&str>,
    current_database: &str,
    if_exists: bool,
    _force: bool,
) -> Result<StatementResult, String> {
    if current_catalog.is_none() && name.parts.len() <= 2 {
        match resolve_local_table_name(name, current_database) {
            Ok(resolved) => {
                let mut guard = state
                    .catalog
                    .write()
                    .expect("standalone catalog write lock");
                guard.drop_table(&resolved.database, &resolved.table)?;
                drop(guard);
                delete_local_table_if_needed(state, &resolved.database, &resolved.table)?;
                Ok(StatementResult::Ok)
            }
            Err(err) if if_exists && err.contains("unknown table") => Ok(StatementResult::Ok),
            Err(err) => Err(err),
        }
    } else {
        let resolved = resolve_iceberg_table_name(name.clone(), current_catalog, current_database)?;
        let guard = state
            .iceberg_catalogs
            .read()
            .expect("standalone iceberg catalog read lock");
        let entry = guard.get(&resolved.catalog)?;
        match drop_iceberg_table(&entry, &resolved.namespace, &resolved.table) {
            Ok(()) => {
                delete_iceberg_table_if_needed(
                    state,
                    &resolved.catalog,
                    &resolved.namespace,
                    &resolved.table,
                )?;
                Ok(StatementResult::Ok)
            }
            Err(err) if if_exists && err.contains("table") => Ok(StatementResult::Ok),
            Err(err) => Err(err),
        }
    }
}

fn execute_insert_statement(
    state: &Arc<StandaloneState>,
    name: &ObjectName,
    source: &InsertSource,
    current_catalog: Option<&str>,
    current_database: &str,
) -> Result<StatementResult, String> {
    let resolved = resolve_iceberg_table_name(name.clone(), current_catalog, current_database)?;
    let guard = state
        .iceberg_catalogs
        .read()
        .expect("standalone iceberg catalog read lock");
    let entry = guard.get(&resolved.catalog)?;
    match source {
        InsertSource::Values(rows) => {
            insert_iceberg_rows(&entry, &resolved.namespace, &resolved.table, rows)?;
        }
        InsertSource::SelectLiteralRow(row) => {
            insert_iceberg_rows(&entry, &resolved.namespace, &resolved.table, &[row.clone()])?;
        }
    }
    Ok(StatementResult::Ok)
}

fn resolve_local_table_name(
    name: &ObjectName,
    current_database: &str,
) -> Result<ResolvedLocalTableName, String> {
    match name.parts.as_slice() {
        [table] => Ok(ResolvedLocalTableName {
            database: normalize_identifier(current_database)?,
            table: normalize_identifier(table)?,
        }),
        [database, table] => Ok(ResolvedLocalTableName {
            database: normalize_identifier(database)?,
            table: normalize_identifier(table)?,
        }),
        _ => Err(format!(
            "local table name must be `<table>` or `<database>.<table>`, got `{}`",
            name.parts.join(".")
        )),
    }
}

fn resolve_iceberg_namespace_name(
    name: ObjectName,
    current_catalog: Option<&str>,
) -> Result<ResolvedIcebergNamespaceName, String> {
    match (
        normalize_optional_identifier(current_catalog)?,
        name.parts.as_slice(),
    ) {
        (Some(catalog), [namespace]) => Ok(ResolvedIcebergNamespaceName {
            catalog,
            namespace: normalize_identifier(namespace)?,
        }),
        (_, [catalog, namespace]) => Ok(ResolvedIcebergNamespaceName {
            catalog: normalize_identifier(catalog)?,
            namespace: normalize_identifier(namespace)?,
        }),
        _ => Err(format!(
            "iceberg database name must be `<database>` with current catalog or `<catalog>.<database>`, got `{}`",
            name.parts.join(".")
        )),
    }
}

fn resolve_iceberg_table_name(
    name: ObjectName,
    current_catalog: Option<&str>,
    current_database: &str,
) -> Result<ResolvedIcebergTableName, String> {
    match (
        normalize_optional_identifier(current_catalog)?,
        name.parts.as_slice(),
    ) {
        (Some(catalog), [table]) => Ok(ResolvedIcebergTableName {
            catalog,
            namespace: normalize_identifier(current_database)?,
            table: normalize_identifier(table)?,
        }),
        (Some(catalog), [namespace, table]) => Ok(ResolvedIcebergTableName {
            catalog,
            namespace: normalize_identifier(namespace)?,
            table: normalize_identifier(table)?,
        }),
        (_, [catalog, namespace, table]) => Ok(ResolvedIcebergTableName {
            catalog: normalize_identifier(catalog)?,
            namespace: normalize_identifier(namespace)?,
            table: normalize_identifier(table)?,
        }),
        _ => Err(format!(
            "iceberg table name must be `<table>`/`<database>.<table>` with current catalog or `<catalog>.<database>.<table>`, got `{}`",
            name.parts.join(".")
        )),
    }
}

fn resolve_iceberg_table_name_explicit(
    name: &ObjectName,
) -> Result<ResolvedIcebergTableName, String> {
    let [catalog, namespace, table] = name.parts.as_slice() else {
        return Err(format!(
            "iceberg table name must be `<catalog>.<database>.<table>`, got `{}`",
            name.parts.join(".")
        ));
    };
    Ok(ResolvedIcebergTableName {
        catalog: normalize_identifier(catalog)?,
        namespace: normalize_identifier(namespace)?,
        table: normalize_identifier(table)?,
    })
}

fn normalize_optional_identifier(raw: Option<&str>) -> Result<Option<String>, String> {
    raw.map(normalize_identifier).transpose()
}

fn bind_query(query: &QueryStmt, columns: &[ColumnDef]) -> Result<BoundQuery, String> {
    let mut column_by_name = HashMap::with_capacity(columns.len());
    for (idx, column) in columns.iter().enumerate() {
        column_by_name.insert(normalize_identifier(&column.name)?, idx);
    }

    let projection_indices = expand_projection(&query.projection, columns, &column_by_name)?;
    let mut scan_columns = Vec::new();
    let mut scan_index_by_source = HashMap::new();
    let mut output_columns = Vec::with_capacity(projection_indices.len());

    for &source_idx in &projection_indices {
        let scan_column_index = ensure_scan_column(
            &mut scan_columns,
            &mut scan_index_by_source,
            &columns[source_idx],
            source_idx,
        )?;
        let column = &columns[source_idx];
        output_columns.push(BoundOutputColumn {
            name: column.name.clone(),
            data_type: column.data_type.clone(),
            nullable: column.nullable,
            scan_column_index,
            output_slot_id: None,
        });
    }

    let predicate = match query.selection.as_ref() {
        Some(expr) => Some(bind_predicate(
            expr,
            &mut scan_columns,
            &mut scan_index_by_source,
            columns,
            &column_by_name,
        )?),
        None => None,
    };

    for (idx, column) in scan_columns.iter_mut().enumerate() {
        column.scan_slot_id =
            i32::try_from(idx + 1).map_err(|_| "too many scan columns".to_string())?;
    }

    let needs_project = output_columns.len() != scan_columns.len()
        || output_columns
            .iter()
            .enumerate()
            .any(|(output_idx, column)| column.scan_column_index != output_idx);

    if needs_project {
        for (idx, column) in output_columns.iter_mut().enumerate() {
            column.output_slot_id = Some(
                i32::try_from(scan_columns.len() + idx + 1)
                    .map_err(|_| "too many output columns".to_string())?,
            );
        }
    }

    Ok(BoundQuery {
        scan_columns,
        output_columns,
        predicate,
        needs_project,
    })
}

fn expand_projection(
    projection: &[ProjectionItem],
    columns: &[ColumnDef],
    column_by_name: &HashMap<String, usize>,
) -> Result<Vec<usize>, String> {
    match projection {
        [ProjectionItem::Wildcard] => Ok((0..columns.len()).collect()),
        _ => projection
            .iter()
            .map(|item| match item {
                ProjectionItem::Wildcard => {
                    Err("wildcard projection cannot be combined with explicit columns".to_string())
                }
                ProjectionItem::Column(column) => resolve_column_index(column, column_by_name),
            })
            .collect(),
    }
}

fn resolve_column_index(
    column: &ColumnRef,
    column_by_name: &HashMap<String, usize>,
) -> Result<usize, String> {
    let name = normalize_identifier(&column.name)?;
    column_by_name
        .get(&name)
        .copied()
        .ok_or_else(|| format!("unknown column: {}", column.name))
}

fn ensure_scan_column(
    scan_columns: &mut Vec<BoundScanColumn>,
    scan_index_by_source: &mut HashMap<usize, usize>,
    column: &ColumnDef,
    source_idx: usize,
) -> Result<usize, String> {
    if let Some(&scan_idx) = scan_index_by_source.get(&source_idx) {
        return Ok(scan_idx);
    }
    let scan_idx = scan_columns.len();
    scan_columns.push(BoundScanColumn {
        name: column.name.clone(),
        data_type: column.data_type.clone(),
        nullable: column.nullable,
        scan_slot_id: 0,
    });
    scan_index_by_source.insert(source_idx, scan_idx);
    Ok(scan_idx)
}

fn bind_predicate(
    expr: &Expr,
    scan_columns: &mut Vec<BoundScanColumn>,
    scan_index_by_source: &mut HashMap<usize, usize>,
    columns: &[ColumnDef],
    column_by_name: &HashMap<String, usize>,
) -> Result<BoundPredicate, String> {
    let Expr::Comparison { left, op, right } = expr;
    let source_idx = resolve_column_index(left, column_by_name)?;
    let column = &columns[source_idx];
    validate_literal_for_column(right, &column.data_type)?;
    let scan_column_index =
        ensure_scan_column(scan_columns, scan_index_by_source, column, source_idx)?;
    Ok(BoundPredicate {
        scan_column_index,
        op: *op,
        literal: right.clone(),
    })
}

fn validate_literal_for_column(literal: &Literal, column_type: &DataType) -> Result<(), String> {
    match literal {
        Literal::Null => Ok(()),
        Literal::Bool(_) if matches!(column_type, DataType::Boolean) => Ok(()),
        Literal::Int(_) => match column_type {
            DataType::Int8
            | DataType::Int16
            | DataType::Int32
            | DataType::Int64
            | DataType::Float32
            | DataType::Float64 => Ok(()),
            other => Err(format!(
                "integer literal is not supported for column type {:?}",
                other
            )),
        },
        Literal::Float(_) => match column_type {
            DataType::Float32 | DataType::Float64 => Ok(()),
            other => Err(format!(
                "float literal is not supported for column type {:?}",
                other
            )),
        },
        Literal::String(_) => match column_type {
            DataType::Utf8
            | DataType::LargeUtf8
            | DataType::Binary
            | DataType::LargeBinary
            | DataType::Date32
            | DataType::Timestamp(_, _) => Ok(()),
            other => Err(format!(
                "string literal is not supported for column type {:?}",
                other
            )),
        },
        Literal::Date(_) => match column_type {
            DataType::Date32 | DataType::Timestamp(_, _) => Ok(()),
            other => Err(format!(
                "date literal is not supported for column type {:?}",
                other
            )),
        },
        Literal::Bool(_) => Err(format!(
            "boolean literal is not supported for column type {:?}",
            column_type
        )),
    }
}

fn execute_query(table: &TableDef, bound: &BoundQuery) -> Result<QueryResult, String> {
    let desc_tbl = build_descriptor_table(bound)?;
    let plan = build_plan(table, bound)?;
    let exec_params = build_exec_params(table, STANDALONE_SCAN_NODE_ID)?;

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
        None,
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
    execute_plan_with_pipeline(
        exec_plan,
        false,
        Duration::from_millis(10),
        Box::new(ResultSinkFactory::new(handle.clone())),
        None,
        None,
        1,
        Arc::new(RuntimeState::default()),
        None,
        None,
        None,
    )?;
    Ok(QueryResult {
        columns: bound
            .output_columns
            .iter()
            .map(|column| QueryResultColumn {
                name: column.name.clone(),
                data_type: column.data_type.clone(),
                nullable: column.nullable,
            })
            .collect(),
        chunks: handle.take_chunks(),
    })
}

fn execute_iceberg_query(
    loaded: &IcebergLoadedTable,
    bound: &crate::sql::BoundQuery,
) -> Result<QueryResult, String> {
    let scan_columns = bound
        .scan_columns
        .iter()
        .map(|column| column.name.clone())
        .collect::<Vec<_>>();
    let batches = block_on_standalone_async(async {
        loaded
            .table
            .scan()
            .select(scan_columns.iter().map(String::as_str))
            .build()
            .map_err(|e| format!("build iceberg scan failed: {e}"))?
            .to_arrow()
            .await
            .map_err(|e| format!("open iceberg arrow stream failed: {e}"))?
            .try_collect::<Vec<_>>()
            .await
            .map_err(|e| format!("read iceberg scan batches failed: {e}"))
    })??;

    let mut chunks = Vec::with_capacity(batches.len());
    for batch in batches {
        let filtered = apply_iceberg_predicate(batch, bound)?;
        let projected = project_iceberg_batch(filtered, bound)?;
        chunks.push(record_batch_to_chunk(projected)?);
    }

    Ok(QueryResult {
        columns: bound
            .output_columns
            .iter()
            .map(|column| QueryResultColumn {
                name: column.name.clone(),
                data_type: column.data_type.clone(),
                nullable: column.nullable,
            })
            .collect(),
        chunks,
    })
}

fn apply_order_by_if_needed(
    result: QueryResult,
    order_by: &[OrderByExpr],
) -> Result<QueryResult, String> {
    if order_by.is_empty() || result.chunks.is_empty() || result.row_count() <= 1 {
        return Ok(result);
    }

    let schema = result.chunks[0].schema();
    let combined = concat_batches(&schema, result.chunks.iter().map(|chunk| &chunk.batch))
        .map_err(|e| format!("concat ordered query batches failed: {e}"))?;

    let mut sort_columns = Vec::with_capacity(order_by.len());
    for item in order_by {
        let sort_idx = result
            .columns
            .iter()
            .position(|column| {
                normalize_identifier(&column.name)
                    .map(|name| name == normalize_identifier(&item.column.name).unwrap_or_default())
                    .unwrap_or(false)
            })
            .ok_or_else(|| {
                format!(
                    "ORDER BY column `{}` must appear in the projection list",
                    item.column.name
                )
            })?;
        sort_columns.push(SortColumn {
            values: combined.column(sort_idx).clone(),
            options: Some(SortOptions {
                descending: item.descending,
                nulls_first: false,
            }),
        });
    }

    let indices: UInt32Array = lexsort_to_indices(&sort_columns, None)
        .map_err(|e| format!("sort ordered query result failed: {e}"))?;
    let sorted_batch = take_record_batch(&combined, &indices)
        .map_err(|e| format!("reorder ordered query result failed: {e}"))?;

    Ok(QueryResult {
        columns: result.columns,
        chunks: vec![record_batch_to_chunk(sorted_batch)?],
    })
}

fn apply_iceberg_predicate(
    batch: RecordBatch,
    bound: &crate::sql::BoundQuery,
) -> Result<RecordBatch, String> {
    let Some(predicate) = bound.predicate.as_ref() else {
        return Ok(batch);
    };
    if matches!(predicate.literal, Literal::Null) {
        let mask = BooleanArray::from(vec![false; batch.num_rows()]);
        return filter_record_batch(&batch, &mask)
            .map_err(|e| format!("filter iceberg batch with null literal failed: {e}"));
    }

    let array = batch.column(predicate.scan_column_index).clone();
    let scalar_array =
        build_repeated_literal_array(array.data_type(), batch.num_rows(), &predicate.literal)?;
    let mask = compare_arrow_arrays(&array, &scalar_array, predicate.op)?;
    filter_record_batch(&batch, &mask).map_err(|e| format!("filter iceberg batch failed: {e}"))
}

fn project_iceberg_batch(
    batch: RecordBatch,
    bound: &crate::sql::BoundQuery,
) -> Result<RecordBatch, String> {
    if !bound.needs_project {
        return Ok(batch);
    }
    let fields = bound
        .output_columns
        .iter()
        .map(|column| Field::new(&column.name, column.data_type.clone(), column.nullable))
        .collect::<Vec<_>>();
    let arrays = bound
        .output_columns
        .iter()
        .map(|column| batch.column(column.scan_column_index).clone())
        .collect::<Vec<_>>();
    RecordBatch::try_new(Arc::new(Schema::new(fields)), arrays)
        .map_err(|e| format!("project iceberg batch failed: {e}"))
}

fn record_batch_to_chunk(batch: RecordBatch) -> Result<Chunk, String> {
    let slot_ids = (1..=batch.num_columns())
        .map(|idx| {
            u32::try_from(idx)
                .map(crate::common::ids::SlotId::new)
                .map_err(|_| "too many iceberg output columns".to_string())
        })
        .collect::<Result<Vec<_>, _>>()?;
    let chunk_schema =
        ChunkSchema::try_ref_from_schema_and_slot_ids(batch.schema().as_ref(), &slot_ids)?;
    Chunk::try_new_with_chunk_schema(batch, chunk_schema)
}

fn build_repeated_literal_array(
    data_type: &DataType,
    len: usize,
    literal: &Literal,
) -> Result<ArrayRef, String> {
    match data_type {
        DataType::Int32 => match literal {
            Literal::Int(value) => {
                let value = i32::try_from(*value)
                    .map_err(|_| format!("literal {value} out of INT range"))?;
                Ok(Arc::new(Int32Array::from(vec![Some(value); len])))
            }
            other => Err(format!("literal {:?} is not valid for INT", other)),
        },
        DataType::Int64 => match literal {
            Literal::Int(value) => Ok(Arc::new(Int64Array::from(vec![Some(*value); len]))),
            other => Err(format!("literal {:?} is not valid for BIGINT", other)),
        },
        DataType::Utf8 => match literal {
            Literal::String(value) => {
                Ok(Arc::new(StringArray::from(vec![Some(value.as_str()); len])))
            }
            other => Err(format!("literal {:?} is not valid for STRING", other)),
        },
        DataType::Boolean => match literal {
            Literal::Bool(value) => Ok(Arc::new(BooleanArray::from(vec![Some(*value); len]))),
            other => Err(format!("literal {:?} is not valid for BOOLEAN", other)),
        },
        other => Err(format!(
            "standalone iceberg predicate does not support column type {:?}",
            other
        )),
    }
}

fn compare_arrow_arrays(
    left: &ArrayRef,
    right: &ArrayRef,
    op: CompareOp,
) -> Result<BooleanArray, String> {
    if matches!(left.data_type(), DataType::Boolean) && !matches!(op, CompareOp::Eq | CompareOp::Ne)
    {
        return Err("standalone iceberg boolean predicates only support `=` and `!=`".to_string());
    }
    match op {
        CompareOp::Eq => eq(&left.as_ref(), &right.as_ref()).map_err(|e| e.to_string()),
        CompareOp::Ne => neq(&left.as_ref(), &right.as_ref()).map_err(|e| e.to_string()),
        CompareOp::Lt => lt(&left.as_ref(), &right.as_ref()).map_err(|e| e.to_string()),
        CompareOp::Le => lt_eq(&left.as_ref(), &right.as_ref()).map_err(|e| e.to_string()),
        CompareOp::Gt => gt(&left.as_ref(), &right.as_ref()).map_err(|e| e.to_string()),
        CompareOp::Ge => gt_eq(&left.as_ref(), &right.as_ref()).map_err(|e| e.to_string()),
    }
}

fn block_on_standalone_async<F>(future: F) -> Result<F::Output, String>
where
    F: std::future::Future,
{
    if let Ok(handle) = Handle::try_current() {
        return Ok(handle.block_on(future));
    }
    data_block_on(future)
}

fn build_plan(table: &TableDef, bound: &BoundQuery) -> Result<plan_nodes::TPlan, String> {
    let scan_node = build_scan_node(table, bound)?;
    if !bound.needs_project {
        return Ok(plan_nodes::TPlan::new(vec![scan_node]));
    }
    let project_node = build_project_node(bound)?;
    Ok(plan_nodes::TPlan::new(vec![project_node, scan_node]))
}

fn build_scan_node(table: &TableDef, bound: &BoundQuery) -> Result<plan_nodes::TPlanNode, String> {
    Ok(plan_nodes::TPlanNode {
        node_id: STANDALONE_SCAN_NODE_ID,
        node_type: plan_nodes::TPlanNodeType::HDFS_SCAN_NODE,
        num_children: 0,
        limit: -1,
        row_tuples: vec![STANDALONE_SCAN_TUPLE_ID],
        nullable_tuples: vec![],
        conjuncts: build_scan_conjuncts(bound)?,
        compact_data: true,
        common: None,
        hash_join_node: None,
        agg_node: None,
        sort_node: None,
        merge_node: None,
        exchange_node: None,
        mysql_scan_node: None,
        olap_scan_node: None,
        file_scan_node: None,
        schema_scan_node: None,
        meta_scan_node: None,
        analytic_node: None,
        union_node: None,
        resource_profile: None,
        es_scan_node: None,
        repeat_node: None,
        assert_num_rows_node: None,
        intersect_node: None,
        except_node: None,
        merge_join_node: None,
        raw_values_node: None,
        use_vectorized: None,
        hdfs_scan_node: Some(plan_nodes::THdfsScanNode::new(
            Some(STANDALONE_SCAN_TUPLE_ID),
            None::<BTreeMap<types::TTupleId, Vec<crate::exprs::TExpr>>>,
            None::<Vec<crate::exprs::TExpr>>,
            None::<types::TTupleId>,
            None::<BTreeMap<types::TSlotId, Vec<i32>>>,
            None::<Vec<crate::exprs::TExpr>>,
            Some(
                table
                    .columns
                    .iter()
                    .map(|c| c.name.clone())
                    .collect::<Vec<_>>(),
            ),
            Some(table.name.clone()),
            None::<String>,
            None::<String>,
            None::<String>,
            Some(true),
            None::<crate::cloud_configuration::TCloudConfiguration>,
            None::<bool>,
            None::<bool>,
            None::<bool>,
            None::<types::TTupleId>,
            None::<String>,
            None::<String>,
            None::<bool>,
            None::<String>,
            None::<crate::data_cache::TDataCacheOptions>,
            None::<Vec<types::TSlotId>>,
            None::<bool>,
            None::<Vec<partitions::TBucketProperty>>,
        )),
        project_node: None,
        table_function_node: None,
        probe_runtime_filters: None,
        decode_node: None,
        local_rf_waiting_set: None,
        filter_null_value_columns: None,
        need_create_tuple_columns: None,
        jdbc_scan_node: None,
        connector_scan_node: None,
        cross_join_node: None,
        lake_scan_node: None,
        nestloop_join_node: None,
        starrocks_scan_node: None,
        stream_scan_node: None,
        stream_join_node: None,
        stream_agg_node: None,
        select_node: None,
        fetch_node: None,
        look_up_node: None,
        benchmark_scan_node: None,
    })
}

fn build_project_node(bound: &BoundQuery) -> Result<plan_nodes::TPlanNode, String> {
    let mut slot_map = BTreeMap::new();
    for output in &bound.output_columns {
        let output_slot_id = output
            .output_slot_id
            .ok_or_else(|| "project node requires output slot ids".to_string())?;
        let scan_column = &bound.scan_columns[output.scan_column_index];
        slot_map.insert(
            output_slot_id,
            build_slot_ref_expr(
                scan_column.scan_slot_id,
                STANDALONE_SCAN_TUPLE_ID,
                arrow_type_to_type_desc(&scan_column.data_type)?,
            ),
        );
    }
    Ok(plan_nodes::TPlanNode {
        node_id: STANDALONE_PROJECT_NODE_ID,
        node_type: plan_nodes::TPlanNodeType::PROJECT_NODE,
        num_children: 1,
        limit: -1,
        row_tuples: vec![STANDALONE_PROJECT_TUPLE_ID],
        nullable_tuples: vec![],
        conjuncts: None,
        compact_data: true,
        common: None,
        hash_join_node: None,
        agg_node: None,
        sort_node: None,
        merge_node: None,
        exchange_node: None,
        mysql_scan_node: None,
        olap_scan_node: None,
        file_scan_node: None,
        schema_scan_node: None,
        meta_scan_node: None,
        analytic_node: None,
        union_node: None,
        resource_profile: None,
        es_scan_node: None,
        repeat_node: None,
        assert_num_rows_node: None,
        intersect_node: None,
        except_node: None,
        merge_join_node: None,
        raw_values_node: None,
        use_vectorized: None,
        hdfs_scan_node: None,
        project_node: Some(plan_nodes::TProjectNode::new(
            Some(slot_map),
            None::<BTreeMap<types::TSlotId, crate::exprs::TExpr>>,
        )),
        table_function_node: None,
        probe_runtime_filters: None,
        decode_node: None,
        local_rf_waiting_set: None,
        filter_null_value_columns: None,
        need_create_tuple_columns: None,
        jdbc_scan_node: None,
        connector_scan_node: None,
        cross_join_node: None,
        lake_scan_node: None,
        nestloop_join_node: None,
        starrocks_scan_node: None,
        stream_scan_node: None,
        stream_join_node: None,
        stream_agg_node: None,
        select_node: None,
        fetch_node: None,
        look_up_node: None,
        benchmark_scan_node: None,
    })
}

fn build_scan_conjuncts(bound: &BoundQuery) -> Result<Option<Vec<crate::exprs::TExpr>>, String> {
    let Some(predicate) = bound.predicate.as_ref() else {
        return Ok(None);
    };
    let scan_column = &bound.scan_columns[predicate.scan_column_index];
    let compare_type = arrow_type_to_type_desc(&scan_column.data_type)?;
    let root = crate::exprs::TExprNode {
        node_type: crate::exprs::TExprNodeType::BINARY_PRED,
        type_: scalar_type_desc(types::TPrimitiveType::BOOLEAN),
        opcode: Some(compare_op_to_opcode(predicate.op)),
        num_children: 2,
        child_type_desc: Some(compare_type.clone()),
        ..default_expr_node()
    };
    let left = slot_ref_expr_node(
        scan_column.scan_slot_id,
        STANDALONE_SCAN_TUPLE_ID,
        compare_type.clone(),
    );
    let right = literal_expr_node(&predicate.literal, &scan_column.data_type)?;
    Ok(Some(vec![crate::exprs::TExpr::new(vec![
        root, left, right,
    ])]))
}

fn compare_op_to_opcode(op: CompareOp) -> opcodes::TExprOpcode {
    match op {
        CompareOp::Eq => opcodes::TExprOpcode::EQ,
        CompareOp::Ne => opcodes::TExprOpcode::NE,
        CompareOp::Lt => opcodes::TExprOpcode::LT,
        CompareOp::Le => opcodes::TExprOpcode::LE,
        CompareOp::Gt => opcodes::TExprOpcode::GT,
        CompareOp::Ge => opcodes::TExprOpcode::GE,
    }
}

fn build_slot_ref_expr(
    slot_id: types::TSlotId,
    tuple_id: types::TTupleId,
    type_desc: types::TTypeDesc,
) -> crate::exprs::TExpr {
    crate::exprs::TExpr::new(vec![slot_ref_expr_node(slot_id, tuple_id, type_desc)])
}

fn slot_ref_expr_node(
    slot_id: types::TSlotId,
    tuple_id: types::TTupleId,
    type_desc: types::TTypeDesc,
) -> crate::exprs::TExprNode {
    crate::exprs::TExprNode {
        node_type: crate::exprs::TExprNodeType::SLOT_REF,
        type_: type_desc,
        num_children: 0,
        slot_ref: Some(crate::exprs::TSlotRef { slot_id, tuple_id }),
        ..default_expr_node()
    }
}

fn literal_expr_node(
    literal: &Literal,
    target_type: &DataType,
) -> Result<crate::exprs::TExprNode, String> {
    let node = match literal {
        Literal::Null => crate::exprs::TExprNode {
            node_type: crate::exprs::TExprNodeType::NULL_LITERAL,
            type_: arrow_type_to_type_desc(target_type)?,
            num_children: 0,
            ..default_expr_node()
        },
        Literal::Bool(value) => crate::exprs::TExprNode {
            node_type: crate::exprs::TExprNodeType::BOOL_LITERAL,
            type_: scalar_type_desc(types::TPrimitiveType::BOOLEAN),
            num_children: 0,
            bool_literal: Some(crate::exprs::TBoolLiteral { value: *value }),
            ..default_expr_node()
        },
        Literal::Int(value) => crate::exprs::TExprNode {
            node_type: crate::exprs::TExprNodeType::INT_LITERAL,
            type_: int_literal_type_desc(target_type),
            num_children: 0,
            int_literal: Some(crate::exprs::TIntLiteral { value: *value }),
            ..default_expr_node()
        },
        Literal::Float(value) => crate::exprs::TExprNode {
            node_type: crate::exprs::TExprNodeType::FLOAT_LITERAL,
            type_: float_literal_type_desc(target_type),
            num_children: 0,
            float_literal: Some(crate::exprs::TFloatLiteral {
                value: thrift::OrderedFloat::from(*value),
            }),
            ..default_expr_node()
        },
        Literal::String(value) => crate::exprs::TExprNode {
            node_type: crate::exprs::TExprNodeType::STRING_LITERAL,
            type_: string_literal_type_desc(target_type),
            num_children: 0,
            string_literal: Some(crate::exprs::TStringLiteral {
                value: value.clone(),
            }),
            ..default_expr_node()
        },
        Literal::Date(value) => match target_type {
            DataType::Date32 => crate::exprs::TExprNode {
                node_type: crate::exprs::TExprNodeType::DATE_LITERAL,
                type_: scalar_type_desc(types::TPrimitiveType::DATE),
                num_children: 0,
                date_literal: Some(crate::exprs::TDateLiteral {
                    value: value.clone(),
                }),
                ..default_expr_node()
            },
            DataType::Timestamp(_, _) => crate::exprs::TExprNode {
                node_type: crate::exprs::TExprNodeType::STRING_LITERAL,
                type_: scalar_type_desc(types::TPrimitiveType::VARCHAR),
                num_children: 0,
                string_literal: Some(crate::exprs::TStringLiteral {
                    value: value.clone(),
                }),
                ..default_expr_node()
            },
            other => {
                return Err(format!(
                    "date literal is not supported for column type {:?}",
                    other
                ));
            }
        },
    };
    Ok(node)
}

fn int_literal_type_desc(target_type: &DataType) -> types::TTypeDesc {
    match target_type {
        DataType::Int8 => scalar_type_desc(types::TPrimitiveType::TINYINT),
        DataType::Int16 => scalar_type_desc(types::TPrimitiveType::SMALLINT),
        DataType::Int32 => scalar_type_desc(types::TPrimitiveType::INT),
        DataType::Int64 => scalar_type_desc(types::TPrimitiveType::BIGINT),
        _ => scalar_type_desc(types::TPrimitiveType::BIGINT),
    }
}

fn float_literal_type_desc(target_type: &DataType) -> types::TTypeDesc {
    match target_type {
        DataType::Float32 => scalar_type_desc(types::TPrimitiveType::FLOAT),
        _ => scalar_type_desc(types::TPrimitiveType::DOUBLE),
    }
}

fn string_literal_type_desc(target_type: &DataType) -> types::TTypeDesc {
    match target_type {
        DataType::Binary | DataType::LargeBinary => {
            scalar_type_desc(types::TPrimitiveType::VARBINARY)
        }
        _ => scalar_type_desc(types::TPrimitiveType::VARCHAR),
    }
}

fn default_expr_node() -> crate::exprs::TExprNode {
    crate::exprs::TExprNode {
        node_type: crate::exprs::TExprNodeType::INT_LITERAL,
        type_: scalar_type_desc(types::TPrimitiveType::INT),
        opcode: None,
        num_children: 0,
        agg_expr: None,
        bool_literal: None,
        case_expr: None,
        date_literal: None,
        float_literal: None,
        int_literal: None,
        in_predicate: None,
        is_null_pred: None,
        like_pred: None,
        literal_pred: None,
        slot_ref: None,
        string_literal: None,
        tuple_is_null_pred: None,
        info_func: None,
        decimal_literal: None,
        output_scale: 0,
        fn_call_expr: None,
        large_int_literal: None,
        output_column: None,
        output_type: None,
        vector_opcode: None,
        fn_: None,
        vararg_start_idx: None,
        child_type: None,
        vslot_ref: None,
        used_subfield_names: None,
        binary_literal: None,
        copy_flag: None,
        check_is_out_of_bounds: None,
        use_vectorized: None,
        has_nullable_child: None,
        is_nullable: None,
        child_type_desc: None,
        is_monotonic: None,
        dict_query_expr: None,
        dictionary_get_expr: None,
        is_index_only_filter: None,
        is_nondeterministic: None,
    }
}

fn build_exec_params(
    table: &TableDef,
    scan_node_id: types::TPlanNodeId,
) -> Result<internal_service::TPlanFragmentExecParams, String> {
    let TableStorage::LocalParquetFile { path } = &table.storage;
    let metadata = std::fs::metadata(path).map_err(|e| format!("stat parquet file failed: {e}"))?;
    let file_len =
        i64::try_from(metadata.len()).map_err(|_| "parquet file is too large".to_string())?;
    let hdfs_scan_range = plan_nodes::THdfsScanRange::new(
        None::<String>,
        Some(0_i64),
        Some(file_len),
        None::<i64>,
        Some(file_len),
        Some(descriptors::THdfsFileFormat::PARQUET),
        None::<descriptors::TTextFileDesc>,
        Some(path.display().to_string()),
        None::<Vec<String>>,
        None::<bool>,
        None::<Vec<plan_nodes::TIcebergDeleteFile>>,
        None::<i64>,
        None::<bool>,
        None::<String>,
        None::<String>,
        None::<i64>,
        None::<crate::data_cache::TDataCacheOptions>,
        None::<Vec<types::TSlotId>>,
        None::<bool>,
        None::<BTreeMap<String, String>>,
        None::<Vec<types::TSlotId>>,
        None::<bool>,
        None::<String>,
        None::<bool>,
        None::<String>,
        None::<String>,
        None::<plan_nodes::TPaimonDeletionFile>,
        None::<BTreeMap<types::TSlotId, crate::exprs::TExpr>>,
        None::<descriptors::THdfsPartition>,
        None::<types::TTableId>,
        None::<plan_nodes::TDeletionVectorDescriptor>,
        None::<String>,
        None::<i64>,
        None::<bool>,
        None::<BTreeMap<i32, crate::exprs::TExprMinMaxValue>>,
        None::<i32>,
        None::<i64>,
    );
    let scan_range = internal_service::TScanRangeParams::new(
        plan_nodes::TScanRange::new(
            None::<plan_nodes::TInternalScanRange>,
            None::<Vec<u8>>,
            None::<plan_nodes::TBrokerScanRange>,
            None::<plan_nodes::TEsScanRange>,
            Some(hdfs_scan_range),
            None::<plan_nodes::TBinlogScanRange>,
            None::<plan_nodes::TBenchmarkScanRange>,
        ),
        None::<i32>,
        Some(false),
        Some(false),
    );
    Ok(internal_service::TPlanFragmentExecParams::new(
        types::TUniqueId::new(1, 1),
        types::TUniqueId::new(2, 2),
        BTreeMap::from([(scan_node_id, vec![scan_range])]),
        BTreeMap::new(),
        None::<Vec<crate::data_sinks::TPlanFragmentDestination>>,
        None::<i32>,
        None::<i32>,
        None::<bool>,
        None::<bool>,
        None::<crate::runtime_filter::TRuntimeFilterParams>,
        None::<i32>,
        None::<bool>,
        None::<BTreeMap<types::TPlanNodeId, BTreeMap<i32, Vec<internal_service::TScanRangeParams>>>>,
        None::<bool>,
        None::<i32>,
        None::<bool>,
        None::<Vec<internal_service::TExecDebugOption>>,
    ))
}

fn build_descriptor_table(bound: &BoundQuery) -> Result<descriptors::TDescriptorTable, String> {
    let mut slot_descs = Vec::with_capacity(bound.scan_columns.len() + bound.output_columns.len());
    for (idx, column) in bound.scan_columns.iter().enumerate() {
        let slot_type = arrow_type_to_type_desc(&column.data_type)?;
        slot_descs.push(descriptors::TSlotDescriptor::new(
            Some(column.scan_slot_id),
            Some(STANDALONE_SCAN_TUPLE_ID),
            Some(slot_type),
            Some(i32::try_from(idx).map_err(|_| "too many columns".to_string())?),
            Some(0),
            Some(0),
            Some(0),
            Some(column.name.clone()),
            Some(i32::try_from(idx).map_err(|_| "too many columns".to_string())?),
            Some(true),
            Some(true),
            Some(column.nullable),
            None::<i32>,
            None::<String>,
        ));
    }

    for (idx, column) in bound.output_columns.iter().enumerate() {
        let Some(output_slot_id) = column.output_slot_id else {
            continue;
        };
        let slot_type = arrow_type_to_type_desc(&column.data_type)?;
        let output_idx = i32::try_from(idx).map_err(|_| "too many output columns".to_string())?;
        slot_descs.push(descriptors::TSlotDescriptor::new(
            Some(output_slot_id),
            Some(STANDALONE_PROJECT_TUPLE_ID),
            Some(slot_type),
            Some(output_idx),
            Some(0),
            Some(0),
            Some(0),
            Some(column.name.clone()),
            Some(output_idx),
            Some(true),
            Some(true),
            Some(column.nullable),
            None::<i32>,
            None::<String>,
        ));
    }

    let mut tuple_descs = vec![descriptors::TTupleDescriptor::new(
        Some(STANDALONE_SCAN_TUPLE_ID),
        Some(0),
        Some(0),
        None::<types::TTableId>,
        Some(0),
    )];
    if bound.needs_project {
        tuple_descs.push(descriptors::TTupleDescriptor::new(
            Some(STANDALONE_PROJECT_TUPLE_ID),
            Some(0),
            Some(0),
            None::<types::TTableId>,
            Some(0),
        ));
    }
    Ok(descriptors::TDescriptorTable::new(
        Some(slot_descs),
        tuple_descs,
        None::<Vec<descriptors::TTableDescriptor>>,
        None::<bool>,
    ))
}

fn arrow_type_to_type_desc(data_type: &DataType) -> Result<types::TTypeDesc, String> {
    let primitive = match data_type {
        DataType::Boolean => types::TPrimitiveType::BOOLEAN,
        DataType::Int8 => types::TPrimitiveType::TINYINT,
        DataType::Int16 => types::TPrimitiveType::SMALLINT,
        DataType::Int32 => types::TPrimitiveType::INT,
        DataType::Int64 => types::TPrimitiveType::BIGINT,
        DataType::Float32 => types::TPrimitiveType::FLOAT,
        DataType::Float64 => types::TPrimitiveType::DOUBLE,
        DataType::Utf8 | DataType::LargeUtf8 => types::TPrimitiveType::VARCHAR,
        DataType::Binary | DataType::LargeBinary => types::TPrimitiveType::VARBINARY,
        DataType::Date32 => types::TPrimitiveType::DATE,
        DataType::Timestamp(_, _) => types::TPrimitiveType::DATETIME,
        other => {
            return Err(format!(
                "standalone MVP does not support Parquet column type {:?}",
                other
            ));
        }
    };
    Ok(scalar_type_desc(primitive))
}

#[cfg(test)]
mod tests {
    use super::{StandaloneNovaRocks, StandaloneOptions, StatementResult};
    use arrow::array::{Int32Array, StringArray};
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

        let create_table_sql = format!(
            r#"create table tbl properties("path"="{}")"#,
            parquet.path().display()
        );
        let create_table = session
            .execute_in_database(&create_table_sql, "analytics")
            .expect("create table");
        assert!(matches!(create_table, StatementResult::Ok));

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
    fn embedded_session_restores_local_metadata_from_sqlite() {
        let parquet = write_parquet_file();
        let metadata_dir = TempDir::new().expect("create metadata dir");
        let metadata_db_path = metadata_dir.path().join("standalone.sqlite");

        {
            let engine = StandaloneNovaRocks::open(StandaloneOptions {
                config_path: None,
                metadata_db_path: Some(metadata_db_path.clone()),
            })
            .expect("open engine");
            let session = engine.session();

            let create_db = session
                .execute_in_database("create database analytics", "default")
                .expect("create database");
            assert!(matches!(create_db, StatementResult::Ok));

            let create_table_sql = format!(
                r#"create table tbl properties("path"="{}")"#,
                parquet.path().display()
            );
            let create_table = session
                .execute_in_database(&create_table_sql, "analytics")
                .expect("create table");
            assert!(matches!(create_table, StatementResult::Ok));
        }

        let restored = StandaloneNovaRocks::open(StandaloneOptions {
            config_path: None,
            metadata_db_path: Some(metadata_db_path),
        })
        .expect("reopen engine");
        assert!(
            restored
                .database_exists("analytics")
                .expect("check restored database")
        );

        let session = restored.session();
        let result = session
            .execute_in_database("select name from tbl where id = 2", "analytics")
            .expect("query restored table");
        let StatementResult::Query(result) = result else {
            panic!("expected query result");
        };
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
}
