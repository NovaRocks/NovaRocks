#![allow(dead_code)]

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::{Arc, RwLock};

use arrow::array::{ArrayRef, StringArray};
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use arrow::record_batch::RecordBatch;
use futures::TryStreamExt;
use tokio::runtime::Handle;

use crate::exec::chunk::{Chunk, ChunkSchema};
use crate::novarocks_config;
use crate::runtime::global_async_runtime::data_block_on;
use crate::sql::ast::{ArithmeticOp, GenerateSeriesSelect, SqlType, TableColumnDef};
use crate::sql::ast::{
    ColumnAggregation, CreateTableKind, Expr, InsertSource, Literal, ObjectName, TableKeyKind,
};

use super::catalog::{
    ColumnDef, DEFAULT_DATABASE, InMemoryCatalog, TableStorage, build_parquet_table,
    normalize_identifier,
};
use super::iceberg::{
    IcebergCatalogRegistry, IcebergLoadedTable, create_namespace as create_iceberg_namespace,
    create_table as create_iceberg_table, drop_namespace as drop_iceberg_namespace,
    drop_table as drop_iceberg_table, insert_rows as insert_iceberg_rows,
    list_tables as list_iceberg_tables, namespace_exists as iceberg_namespace_exists,
    register_existing_table as register_existing_iceberg_table,
};
use super::store::{MetadataSnapshot, SqliteMetadataStore, StoredIcebergTable, StoredLocalTable};

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
    pub logical_type: Option<crate::sql::SqlType>,
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

#[derive(Default)]
struct StandaloneState {
    catalog: RwLock<InMemoryCatalog>,
    iceberg_catalogs: RwLock<IcebergCatalogRegistry>,
    metadata_store: Option<SqliteMetadataStore>,
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
        ensure_dual_table(&inner)?;
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
            TableStorage::S3ParquetFiles { .. } => {
                return Err("register_parquet_table_in_database does not support S3".to_string());
            }
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
        use crate::sql::dialect::{
            StarRocksDialect, looks_like_create_catalog, looks_like_create_database,
            looks_like_create_table, looks_like_drop_statement,
        };
        use sqlparser::ast as sqlast;

        let normalized = crate::sql::dialect::normalize_for_raw_parse(sql)?;
        let dialect = StarRocksDialect;
        let mut parser = sqlparser::parser::Parser::new(&dialect)
            .try_with_sql(&normalized)
            .map_err(|e| format!("sql parser error: {e}"))?;

        // StarRocks DDL: token-level parsing (sqlparser cannot handle these)
        if looks_like_create_table(&parser) {
            let result =
                crate::sql::dialect::create_table::parse_create_table_statement(&mut parser)?;
            return execute_create_table_statement(
                &self.inner,
                result,
                current_catalog,
                current_database,
            );
        }
        if looks_like_create_catalog(&parser) {
            let result =
                crate::sql::dialect::create_catalog::parse_create_catalog_statement(&mut parser)?;
            return self.handle_create_catalog(result);
        }
        if looks_like_create_database(&parser) {
            let db_name = crate::sql::dialect::parse_create_database_name(&mut parser)?;
            return execute_create_database_statement(&self.inner, &db_name, current_catalog);
        }
        if looks_like_drop_statement(&parser) {
            let drop = crate::sql::dialect::drop::parse_drop_statement(&mut parser)?;
            return self.handle_drop(drop, current_catalog, current_database);
        }

        // ALTER TABLE ... ADD FILES FROM '...'
        if looks_like_add_files(&normalized) {
            return self.handle_add_files(&normalized, current_catalog, current_database);
        }

        // Standard SQL: let sqlparser parse the full statement
        let stmt = parser
            .parse_statement()
            .map_err(|e| format!("sql parser error: {e}"))?;
        match stmt {
            sqlast::Statement::Query(ref query) => {
                // When current_catalog is an Iceberg catalog, materialize
                // referenced Iceberg tables into the local catalog first.
                if let Some(cat_name) = current_catalog {
                    self.register_iceberg_tables_for_query(cat_name, current_database, query)?;
                }
                let catalog = self
                    .inner
                    .catalog
                    .read()
                    .expect("standalone catalog read lock");
                let result = build_query_plan(query, &catalog, current_database)?;
                drop(catalog);
                execute_plan(result).map(StatementResult::Query)
            }
            sqlast::Statement::Insert(ref insert) => {
                self.handle_sqlparser_insert(insert, current_catalog, current_database)
            }
            sqlast::Statement::Truncate(truncate) => {
                for truncate_table in &truncate.table_names {
                    let table_name =
                        crate::sql::dialect::convert_object_name(truncate_table.name.clone())?;
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

    /// Register Iceberg tables referenced by a query into the local catalog.
    /// Instead of downloading data, this registers S3 file paths directly so the
    /// execution engine reads parquet files from S3 via TCloudConfiguration.
    fn register_iceberg_tables_for_query(
        &self,
        catalog_name: &str,
        current_database: &str,
        query: &sqlparser::ast::Query,
    ) -> Result<(), String> {
        // Extract all table names from the query
        let table_names = extract_table_names_from_query(query);
        if table_names.is_empty() {
            return Ok(());
        }

        let iceberg_guard = self
            .inner
            .iceberg_catalogs
            .read()
            .expect("iceberg catalog read lock");
        let entry = match iceberg_guard.get(catalog_name) {
            Ok(e) => e,
            Err(_) => return Ok(()), // not an iceberg catalog, skip
        };

        for table_name in &table_names {
            // Skip if already registered in local catalog
            {
                let local = self.inner.catalog.read().expect("catalog read lock");
                if local.get(current_database, table_name).is_ok() {
                    continue;
                }
            }

            // Load table metadata from Iceberg (lazy S3 discovery)
            let loaded = match super::iceberg::load_table(&entry, current_database, table_name) {
                Ok(l) => l,
                Err(_) => continue, // table not found in iceberg, skip
            };

            // Build the storage variant based on whether this is S3 or local
            let storage = if entry.is_s3() {
                // Extract data file paths from Iceberg scan plan
                let data_files = super::iceberg::extract_data_files(&loaded.table)?;
                let cloud_properties = entry.cloud_properties_map();
                crate::sql::catalog::TableStorage::S3ParquetFiles {
                    files: data_files
                        .into_iter()
                        .map(|(path, size)| crate::sql::catalog::S3FileInfo { path, size })
                        .collect(),
                    cloud_properties,
                }
            } else {
                // Local Iceberg: extract data files and use LocalParquetFile
                // For local, there should be exactly one data directory to scan
                let data_files = super::iceberg::extract_data_files(&loaded.table)?;
                if let Some((first_path, _)) = data_files.first() {
                    let local_path = first_path.strip_prefix("file://").unwrap_or(first_path);
                    crate::sql::catalog::TableStorage::LocalParquetFile {
                        path: std::path::PathBuf::from(local_path),
                    }
                } else {
                    // No data files, create an empty placeholder
                    let dir = std::env::temp_dir().join("novarocks_iceberg_empty");
                    std::fs::create_dir_all(&dir).map_err(|e| format!("create empty dir: {e}"))?;
                    let path = dir.join(format!("{}_{}.parquet", current_database, table_name));
                    let schema = std::sync::Arc::new(arrow::datatypes::Schema::new(
                        loaded
                            .columns
                            .iter()
                            .map(|c| {
                                arrow::datatypes::Field::new(
                                    &c.name,
                                    c.data_type.clone(),
                                    c.nullable,
                                )
                            })
                            .collect::<Vec<_>>(),
                    ));
                    let empty_arrays: Vec<arrow::array::ArrayRef> = schema
                        .fields()
                        .iter()
                        .map(|f| arrow::array::new_empty_array(f.data_type()))
                        .collect();
                    let empty_batch = RecordBatch::try_new(schema, empty_arrays)
                        .map_err(|e| format!("build empty batch: {e}"))?;
                    write_parquet_to_path(&path, &empty_batch)?;
                    crate::sql::catalog::TableStorage::LocalParquetFile { path }
                }
            };

            // Register in local catalog
            let table_def = crate::sql::catalog::TableDef {
                name: table_name.clone(),
                columns: loaded.columns,
                storage,
            };
            let mut guard = self.inner.catalog.write().expect("catalog write lock");
            if guard.get(current_database, table_name).is_err() {
                guard.create_database(current_database).ok(); // ignore if exists
                guard
                    .register(current_database, table_def)
                    .map_err(|e| format!("register iceberg table: {e}"))?;
            }
        }

        Ok(())
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
        let count = super::iceberg_add_files::add_files(&entry, &namespace, &table_name, &s3_path)?;
        let msg = format!("Added {count} file(s)");
        build_string_query_result("status", vec![msg]).map(StatementResult::Query)
    }

    /// Handle CREATE CATALOG result.
    fn handle_create_catalog(
        &self,
        stmt: crate::sql::ast::CreateCatalogStmt,
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
        drop: crate::sql::dialect::drop::DropResult,
        current_catalog: Option<&str>,
        current_database: &str,
    ) -> Result<StatementResult, String> {
        use crate::sql::dialect::drop::DropResult;
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

    /// Consolidated INSERT handler using sqlparser AST.
    fn handle_sqlparser_insert(
        &self,
        insert: &sqlparser::ast::Insert,
        current_catalog: Option<&str>,
        current_database: &str,
    ) -> Result<StatementResult, String> {
        use sqlparser::ast as sqlast;

        if let Some(ref source) = insert.source {
            // INSERT SELECT
            let is_select = matches!(source.body.as_ref(), sqlast::SetExpr::Select(_));
            if is_select && !insert.overwrite {
                return self.execute_insert_select(insert, current_catalog, current_database);
            }
            // INSERT VALUES for local tables
            if let sqlast::SetExpr::Values(_) = source.body.as_ref() {
                let raw_name = match &insert.table {
                    sqlast::TableObject::TableName(name) => name.clone(),
                    other => return Err(format!("unsupported INSERT target: {other}")),
                };
                if let Ok(table_name) = crate::sql::dialect::convert_object_name(raw_name) {
                    if let Ok(resolved) = resolve_local_table_name(&table_name, current_database) {
                        let guard = self
                            .inner
                            .catalog
                            .read()
                            .expect("standalone catalog read lock");
                        if let Ok(table_def) = guard.get(&resolved.database, &resolved.table) {
                            drop(guard);
                            return self
                                .execute_insert_values_sqlparser(insert, &resolved, &table_def);
                        }
                    }
                }
            }
        }

        // Fallback: parse INSERT via the custom parser for Iceberg tables
        // (handles generate_series, literal SELECT rows, etc.)
        self.execute_insert_via_custom_parser(insert, current_catalog, current_database)
    }

    /// Handle INSERT INTO ... SELECT ... by executing the SELECT via ThriftPlanBuilder
    /// and writing results to the target local table.
    fn execute_insert_select(
        &self,
        insert: &sqlparser::ast::Insert,
        _current_catalog: Option<&str>,
        current_database: &str,
    ) -> Result<StatementResult, String> {
        use sqlparser::ast as sqlast;

        let source_query = insert
            .source
            .as_ref()
            .ok_or_else(|| "INSERT SELECT requires a source query".to_string())?;

        // Execute the SELECT query
        let catalog = self
            .inner
            .catalog
            .read()
            .expect("standalone catalog read lock");
        let plan_result = build_query_plan(source_query, &catalog, current_database)?;
        drop(catalog);
        let query_result = execute_plan(plan_result)?;

        // Resolve target table
        let raw_name = match &insert.table {
            sqlast::TableObject::TableName(name) => name.clone(),
            other => return Err(format!("unsupported INSERT target: {other}")),
        };
        let table_name = crate::sql::dialect::convert_object_name(raw_name)?;
        let resolved = resolve_local_table_name(&table_name, current_database)?;
        let guard = self
            .inner
            .catalog
            .read()
            .expect("standalone catalog read lock");
        let table_def = guard.get(&resolved.database, &resolved.table)?;
        drop(guard);

        let path = match &table_def.storage {
            TableStorage::LocalParquetFile { path } => path.clone(),
            TableStorage::S3ParquetFiles { .. } => {
                return Err("INSERT SELECT into S3 tables is not supported".to_string());
            }
        };

        // Read existing data
        let existing_batch = read_local_parquet_data(&path, &table_def.columns)?;

        // Convert query result chunks to a single RecordBatch
        let schema = std::sync::Arc::new(arrow::datatypes::Schema::new(
            table_def
                .columns
                .iter()
                .map(|c| arrow::datatypes::Field::new(&c.name, c.data_type.clone(), c.nullable))
                .collect::<Vec<_>>(),
        ));
        let mut all_batches = Vec::new();
        if existing_batch.num_rows() > 0 {
            all_batches.push(existing_batch);
        }
        for chunk in query_result.into_chunks() {
            if chunk.len() > 0 {
                let casted = cast_batch_to_schema(&chunk.batch, &schema)?;
                all_batches.push(casted);
            }
        }

        let combined = if all_batches.is_empty() {
            arrow::record_batch::RecordBatch::new_empty(schema)
        } else {
            arrow::compute::concat_batches(&schema, all_batches.iter())
                .map_err(|e| format!("concat insert-select batches failed: {e}"))?
        };

        write_parquet_to_path(&path, &combined)?;

        // Re-register table in catalog
        let mut guard = self
            .inner
            .catalog
            .write()
            .expect("standalone catalog write lock");
        let updated = super::catalog::TableDef {
            name: table_def.name.clone(),
            columns: table_def.columns.clone(),
            storage: TableStorage::LocalParquetFile { path },
        };
        guard.register(&resolved.database, updated).ok();
        Ok(StatementResult::Ok)
    }

    /// Fallback: convert sqlparser INSERT to custom InsertStmt and execute.
    /// Used for Iceberg tables and generate_series INSERT paths.
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

    /// Handle INSERT VALUES via sqlparser AST for local tables
    fn execute_insert_values_sqlparser(
        &self,
        insert: &sqlparser::ast::Insert,
        resolved: &ResolvedLocalTableName,
        table_def: &super::catalog::TableDef,
    ) -> Result<StatementResult, String> {
        use sqlparser::ast as sqlast;

        let source = insert.source.as_ref().ok_or("INSERT missing source")?;
        let values = match source.body.as_ref() {
            sqlast::SetExpr::Values(v) => v,
            _ => return Err("expected VALUES".into()),
        };

        // Map insert column names
        let insert_columns: Vec<String> = insert
            .columns
            .iter()
            .map(|c| c.value.to_lowercase())
            .collect();

        // Convert sqlparser Values to Literals
        let mut literal_rows = Vec::new();
        for row in &values.rows {
            let mut literal_row = Vec::new();
            for expr in row {
                literal_row.push(sqlparser_expr_to_literal(expr)?);
            }
            literal_rows.push(literal_row);
        }

        // Reorder columns
        let rows = reorder_insert_rows(&literal_rows, &insert_columns, &table_def.columns)?;

        let path = match &table_def.storage {
            TableStorage::LocalParquetFile { path } => path.clone(),
            TableStorage::S3ParquetFiles { .. } => {
                return Err("INSERT VALUES into S3 tables is not supported".to_string());
            }
        };

        let existing_batch = read_local_parquet_data(&path, &table_def.columns)?;
        let new_batch = build_local_insert_batch(&table_def.columns, &rows)?;

        let combined = if existing_batch.num_rows() > 0 {
            arrow::compute::concat_batches(
                &new_batch.schema(),
                [&existing_batch, &new_batch].iter().copied(),
            )
            .map_err(|e| format!("concat batches failed: {e}"))?
        } else {
            new_batch
        };

        write_parquet_to_path(&path, &combined)?;

        let mut guard = self
            .inner
            .catalog
            .write()
            .expect("standalone catalog write lock");
        let updated = super::catalog::TableDef {
            name: table_def.name.clone(),
            columns: table_def.columns.clone(),
            storage: TableStorage::LocalParquetFile { path },
        };
        guard.register(&resolved.database, updated).ok();
        Ok(StatementResult::Ok)
    }
}

/// Convert a sqlparser INSERT AST to our custom InsertStmt.
/// Used for Iceberg tables which need the custom AST's InsertSource types.
fn convert_sqlparser_insert_to_custom(
    insert: &sqlparser::ast::Insert,
) -> Result<crate::sql::ast::InsertStmt, String> {
    use sqlparser::ast as sqlast;

    let table = match &insert.table {
        sqlast::TableObject::TableName(name) => {
            crate::sql::dialect::convert_object_name(name.clone())?
        }
        other => return Err(format!("unsupported INSERT target: {other}")),
    };
    let columns: Vec<String> = insert.columns.iter().map(|c| c.value.clone()).collect();
    let source_query = insert
        .source
        .as_ref()
        .ok_or_else(|| "INSERT requires a source".to_string())?;
    let source = match source_query.body.as_ref() {
        sqlast::SetExpr::Values(values) => {
            let mut rows = Vec::new();
            for row in &values.rows {
                let literal_row: Vec<Literal> = row
                    .iter()
                    .map(sqlparser_expr_to_literal)
                    .collect::<Result<_, _>>()?;
                rows.push(literal_row);
            }
            InsertSource::Values(rows)
        }
        sqlast::SetExpr::Select(select) => {
            if select.from.is_empty() {
                // Literal SELECT row (e.g., INSERT INTO t SELECT 1, 'a')
                let row: Vec<Literal> = select
                    .projection
                    .iter()
                    .map(|item| match item {
                        sqlast::SelectItem::UnnamedExpr(expr) => sqlparser_expr_to_literal(expr),
                        _ => Err("INSERT SELECT source only supports unnamed expressions".into()),
                    })
                    .collect::<Result<_, _>>()?;
                InsertSource::SelectLiteralRow(row)
            } else if select.from.len() == 1 {
                // Possible TABLE(generate_series(...)) source
                let table_with_joins = &select.from[0];
                if table_with_joins.joins.is_empty() {
                    if let sqlparser::ast::TableFactor::TableFunction {
                        ref expr,
                        ref alias,
                    } = table_with_joins.relation
                    {
                        let (start, end, step) = parse_generate_series_function_expr(expr)?;
                        let column_name = alias
                            .as_ref()
                            .and_then(|a| a.columns.first().map(|c| c.name.value.clone()))
                            .unwrap_or_else(|| "generate_series".to_string());
                        let projection: Vec<Expr> = select
                            .projection
                            .iter()
                            .map(|item| match item {
                                sqlast::SelectItem::UnnamedExpr(expr) => {
                                    sqlparser_expr_to_custom_expr(expr)
                                }
                                _ => {
                                    Err("INSERT SELECT source only supports unnamed expressions"
                                        .into())
                                }
                            })
                            .collect::<Result<_, _>>()?;
                        InsertSource::GenerateSeriesSelect(GenerateSeriesSelect {
                            column_name,
                            start,
                            end,
                            step,
                            projection,
                        })
                    } else {
                        return Err("unsupported INSERT SELECT source".into());
                    }
                } else {
                    return Err("INSERT SELECT with joins is not supported in this path".into());
                }
            } else {
                return Err("INSERT SELECT with multiple tables is not supported".into());
            }
        }
        _ => return Err("unsupported INSERT source".into()),
    };
    Ok(crate::sql::ast::InsertStmt {
        table,
        columns,
        source,
    })
}

/// Parse a generate_series function call expression into (start, end, step).
fn parse_generate_series_function_expr(
    expr: &sqlparser::ast::Expr,
) -> Result<(i64, i64, i64), String> {
    use sqlparser::ast as sqlast;
    let sqlast::Expr::Function(function) = expr else {
        return Err("expected generate_series function call".into());
    };
    let name = function
        .name
        .0
        .last()
        .map(|p| p.to_string().to_ascii_lowercase())
        .unwrap_or_default();
    if name != "generate_series" {
        return Err(format!("expected generate_series, got `{name}`"));
    }
    let sqlast::FunctionArguments::List(ref args) = function.args else {
        return Err("generate_series requires parenthesized arguments".into());
    };
    let values: Vec<i64> = args
        .args
        .iter()
        .map(|arg| match arg {
            sqlast::FunctionArg::Unnamed(sqlast::FunctionArgExpr::Expr(expr)) => {
                match sqlparser_expr_to_literal(expr)? {
                    Literal::Int(v) => Ok(v),
                    other => Err(format!(
                        "generate_series expects integer args, got {other:?}"
                    )),
                }
            }
            other => Err(format!(
                "generate_series expects positional args, got {other}"
            )),
        })
        .collect::<Result<_, _>>()?;
    match values.as_slice() {
        [start, end] => Ok((*start, *end, 1)),
        [start, end, step] => {
            if *step == 0 {
                return Err("generate_series step must not be zero".into());
            }
            Ok((*start, *end, *step))
        }
        _ => Err("generate_series expects 2 or 3 arguments".into()),
    }
}

/// Convert a sqlparser expression to a custom Expr (used for generate_series projections).
fn sqlparser_expr_to_custom_expr(expr: &sqlparser::ast::Expr) -> Result<Expr, String> {
    use sqlparser::ast as sqlast;
    match expr {
        sqlast::Expr::Identifier(ident) => Ok(Expr::Column(crate::sql::ast::ColumnRef {
            name: ident.value.clone(),
        })),
        sqlast::Expr::CompoundIdentifier(parts) => Ok(Expr::Column(crate::sql::ast::ColumnRef {
            name: parts
                .last()
                .map(|p| p.value.clone())
                .ok_or_else(|| "empty column reference".to_string())?,
        })),
        sqlast::Expr::Value(sqlast::ValueWithSpan { value, .. }) => {
            let lit = match value {
                sqlast::Value::Null => Literal::Null,
                sqlast::Value::Boolean(b) => Literal::Bool(*b),
                sqlast::Value::Number(n, _) => {
                    if let Ok(i) = n.parse::<i64>() {
                        Literal::Int(i)
                    } else if let Ok(f) = n.parse::<f64>() {
                        Literal::Float(f)
                    } else {
                        Literal::String(n.clone())
                    }
                }
                sqlast::Value::SingleQuotedString(s) | sqlast::Value::DoubleQuotedString(s) => {
                    Literal::String(s.clone())
                }
                _ => return Err(format!("unsupported value in expression: {value}")),
            };
            Ok(Expr::Literal(lit))
        }
        sqlast::Expr::BinaryOp { left, op, right } => {
            let left_expr = sqlparser_expr_to_custom_expr(left)?;
            let right_expr = sqlparser_expr_to_custom_expr(right)?;
            match op {
                sqlast::BinaryOperator::Plus => Ok(Expr::Arithmetic {
                    left: Box::new(left_expr),
                    op: ArithmeticOp::Add,
                    right: Box::new(right_expr),
                }),
                sqlast::BinaryOperator::Minus => Ok(Expr::Arithmetic {
                    left: Box::new(left_expr),
                    op: ArithmeticOp::Sub,
                    right: Box::new(right_expr),
                }),
                sqlast::BinaryOperator::Multiply => Ok(Expr::Arithmetic {
                    left: Box::new(left_expr),
                    op: ArithmeticOp::Mul,
                    right: Box::new(right_expr),
                }),
                sqlast::BinaryOperator::Divide => Ok(Expr::Arithmetic {
                    left: Box::new(left_expr),
                    op: ArithmeticOp::Div,
                    right: Box::new(right_expr),
                }),
                sqlast::BinaryOperator::Modulo => Ok(Expr::Arithmetic {
                    left: Box::new(left_expr),
                    op: ArithmeticOp::Mod,
                    right: Box::new(right_expr),
                }),
                other => Err(format!("unsupported operator in expression: {other}")),
            }
        }
        sqlast::Expr::Cast {
            expr: inner,
            data_type,
            ..
        } => {
            let inner_expr = sqlparser_expr_to_custom_expr(inner)?;
            let sql_type = crate::sql::dialect::convert_sql_type(data_type.clone())?;
            Ok(Expr::Cast {
                expr: Box::new(inner_expr),
                data_type: sql_type,
            })
        }
        sqlast::Expr::UnaryOp {
            op: sqlast::UnaryOperator::Minus,
            expr: inner,
        } => {
            let lit = sqlparser_expr_to_literal(inner)?;
            match lit {
                Literal::Int(i) => Ok(Expr::Literal(Literal::Int(-i))),
                Literal::Float(f) => Ok(Expr::Literal(Literal::Float(-f))),
                _ => Err(format!("cannot negate {lit:?}")),
            }
        }
        sqlast::Expr::Nested(inner) => sqlparser_expr_to_custom_expr(inner),
        other => Err(format!("unsupported expression: {other}")),
    }
}

/// Convert a sqlparser expression to a Literal (for INSERT VALUES)
fn sqlparser_expr_to_literal(expr: &sqlparser::ast::Expr) -> Result<Literal, String> {
    use sqlparser::ast as sqlast;
    match expr {
        sqlast::Expr::Value(sqlast::ValueWithSpan { value, .. }) => match value {
            sqlast::Value::Null => Ok(Literal::Null),
            sqlast::Value::Number(n, _) => {
                if let Ok(i) = n.parse::<i64>() {
                    Ok(Literal::Int(i))
                } else if let Ok(f) = n.parse::<f64>() {
                    Ok(Literal::Float(f))
                } else {
                    Ok(Literal::String(n.clone()))
                }
            }
            sqlast::Value::SingleQuotedString(s) | sqlast::Value::DoubleQuotedString(s) => {
                Ok(Literal::String(s.clone()))
            }
            sqlast::Value::Boolean(b) => Ok(Literal::Bool(*b)),
            _ => Err(format!("unsupported literal in INSERT VALUES: {value}")),
        },
        sqlast::Expr::UnaryOp {
            op: sqlast::UnaryOperator::Minus,
            expr: inner,
        } => {
            let lit = sqlparser_expr_to_literal(inner)?;
            match lit {
                Literal::Int(i) => Ok(Literal::Int(-i)),
                Literal::Float(f) => Ok(Literal::Float(-f)),
                _ => Err(format!("cannot negate {lit:?}")),
            }
        }
        sqlast::Expr::Nested(inner) => sqlparser_expr_to_literal(inner),
        // Handle CAST(expr AS type) — evaluate inner and convert to string
        sqlast::Expr::Cast { expr: inner, .. } => sqlparser_expr_to_literal(inner),
        // Handle DATE '2024-01-01' typed strings
        sqlast::Expr::TypedString(typed) => Ok(Literal::String(typed.value.to_string())),
        // In MySQL mode, "value" is parsed as an identifier — treat as string literal
        sqlast::Expr::Identifier(ident) => Ok(Literal::String(ident.value.clone())),
        // Handle binary operations like 10000 - 1
        sqlast::Expr::BinaryOp { left, op, right } => {
            let l = sqlparser_expr_to_literal(left)?;
            let r = sqlparser_expr_to_literal(right)?;
            match (l, op, r) {
                (Literal::Int(a), sqlast::BinaryOperator::Plus, Literal::Int(b)) => {
                    Ok(Literal::Int(a + b))
                }
                (Literal::Int(a), sqlast::BinaryOperator::Minus, Literal::Int(b)) => {
                    Ok(Literal::Int(a - b))
                }
                (Literal::Int(a), sqlast::BinaryOperator::Multiply, Literal::Int(b)) => {
                    Ok(Literal::Int(a * b))
                }
                (Literal::Float(a), sqlast::BinaryOperator::Plus, Literal::Float(b)) => {
                    Ok(Literal::Float(a + b))
                }
                (Literal::Float(a), sqlast::BinaryOperator::Minus, Literal::Float(b)) => {
                    Ok(Literal::Float(a - b))
                }
                _ => Err(format!("unsupported expression in INSERT VALUES: {expr}")),
            }
        }
        // Handle array literal [1, 2, 3]
        sqlast::Expr::Array(sqlast::Array { elem, .. }) => {
            // Store as JSON-like string for now
            let elements: Result<Vec<String>, _> = elem
                .iter()
                .map(|e| {
                    sqlparser_expr_to_literal(e).map(|l| match l {
                        Literal::Null => "null".to_string(),
                        Literal::Bool(b) => b.to_string(),
                        Literal::Int(i) => i.to_string(),
                        Literal::Float(f) => f.to_string(),
                        Literal::String(s) | Literal::Date(s) => format!("\"{s}\""),
                        Literal::Array(a) => format!(
                            "[{}]",
                            a.iter()
                                .map(|x| format!("{x:?}"))
                                .collect::<Vec<_>>()
                                .join(",")
                        ),
                    })
                })
                .collect();
            Ok(Literal::String(format!("[{}]", elements?.join(","))))
        }
        _ => Err(format!("unsupported expression in INSERT VALUES: {expr}")),
    }
}

// ---------------------------------------------------------------------------
// DDL handlers
// ---------------------------------------------------------------------------

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
        ensure_dual_in_database(state, &database_name)?;
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
    stmt: crate::sql::ast::CreateTableStmt,
    current_catalog: Option<&str>,
    current_database: &str,
) -> Result<StatementResult, String> {
    match stmt.kind {
        CreateTableKind::LocalParquet { path } => {
            let resolved = resolve_local_table_name(&stmt.name, current_database)?;
            let table = build_parquet_table(stmt.name.leaf(), &path)?;
            let persisted_path = match &table.storage {
                TableStorage::LocalParquetFile { path } => path.clone(),
                TableStorage::S3ParquetFiles { .. } => {
                    return Err("LocalParquet CREATE TABLE does not support S3".to_string());
                }
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
            key_desc,
            properties,
        } => {
            // When there is no iceberg catalog context, create a local parquet table
            if current_catalog.is_none() && stmt.name.parts.len() <= 2 {
                return create_local_table_from_columns(
                    state,
                    &stmt.name,
                    current_database,
                    &columns,
                );
            }

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
                key_desc.as_ref(),
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
                match guard.drop_table(&resolved.database, &resolved.table) {
                    Ok(()) => {
                        drop(guard);
                        delete_local_table_if_needed(state, &resolved.database, &resolved.table)?;
                        Ok(StatementResult::Ok)
                    }
                    Err(err) if if_exists && err.contains("unknown") => Ok(StatementResult::Ok),
                    Err(err) => Err(err),
                }
            }
            Err(err) if if_exists && err.contains("unknown") => Ok(StatementResult::Ok),
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

// ---------------------------------------------------------------------------
// DML handlers
// ---------------------------------------------------------------------------

fn execute_truncate_table_statement(
    state: &Arc<StandaloneState>,
    name: &ObjectName,
    current_database: &str,
) -> Result<StatementResult, String> {
    let resolved = resolve_local_table_name(name, current_database)?;
    let guard = state.catalog.read().expect("standalone catalog read lock");
    let table_def = guard.get(&resolved.database, &resolved.table)?;
    let path = match &table_def.storage {
        TableStorage::LocalParquetFile { path } => path.clone(),
        TableStorage::S3ParquetFiles { .. } => {
            return Err("TRUNCATE TABLE is not supported for S3 tables".to_string());
        }
    };
    let schema = Arc::new(arrow::datatypes::Schema::new(
        table_def
            .columns
            .iter()
            .map(|c| arrow::datatypes::Field::new(&c.name, c.data_type.clone(), c.nullable))
            .collect::<Vec<_>>(),
    ));
    drop(guard);
    // Write an empty parquet file with the schema to truncate the table
    let empty_arrays: Vec<ArrayRef> = schema
        .fields()
        .iter()
        .map(|f| arrow::array::new_empty_array(f.data_type()))
        .collect();
    let empty_batch = RecordBatch::try_new(schema, empty_arrays)
        .map_err(|e| format!("build empty batch for truncate failed: {e}"))?;
    write_parquet_to_path(&path, &empty_batch)?;
    Ok(StatementResult::Ok)
}

fn execute_insert_statement(
    state: &Arc<StandaloneState>,
    name: &ObjectName,
    columns: &[String],
    source: &InsertSource,
    current_catalog: Option<&str>,
    current_database: &str,
) -> Result<StatementResult, String> {
    // When there is no iceberg catalog context, try local table insert
    if current_catalog.is_none() && name.parts.len() <= 2 {
        if let Ok(resolved) = resolve_local_table_name(name, current_database) {
            let guard = state.catalog.read().expect("standalone catalog read lock");
            if let Ok(table_def) = guard.get(&resolved.database, &resolved.table) {
                drop(guard);
                return insert_into_local_table(state, &resolved, &table_def, columns, source);
            }
        }
    }

    let resolved = resolve_iceberg_table_name(name.clone(), current_catalog, current_database)?;
    let guard = state
        .iceberg_catalogs
        .read()
        .expect("standalone iceberg catalog read lock");
    let entry = guard.get(&resolved.catalog)?;
    let loaded = super::iceberg::load_table(&entry, &resolved.namespace, &resolved.table)?;
    match source {
        InsertSource::Values(rows) => {
            let rows = reorder_insert_rows(rows, columns, &loaded.columns)?;
            insert_iceberg_rows(&entry, &resolved.namespace, &resolved.table, &rows)?;
        }
        InsertSource::SelectLiteralRow(row) => {
            let rows = reorder_insert_rows(std::slice::from_ref(row), columns, &loaded.columns)?;
            insert_iceberg_rows(&entry, &resolved.namespace, &resolved.table, &rows)?;
        }
        InsertSource::GenerateSeriesSelect(source) => {
            insert_generate_series_rows(
                &entry,
                &resolved.namespace,
                &resolved.table,
                source,
                columns,
                &loaded.columns,
            )?;
        }
    }
    Ok(StatementResult::Ok)
}

fn insert_generate_series_rows(
    entry: &super::iceberg::IcebergCatalogEntry,
    namespace: &str,
    table: &str,
    source: &GenerateSeriesSelect,
    insert_columns: &[String],
    target_columns: &[ColumnDef],
) -> Result<(), String> {
    const INSERT_CHUNK_SIZE: usize = 4096;
    let mut rows = Vec::with_capacity(INSERT_CHUNK_SIZE);
    let mut current = source.start;
    let ascending = source.step > 0;
    while if ascending {
        current <= source.end
    } else {
        current >= source.end
    } {
        let row = evaluate_generate_series_row(source, current)?;
        rows.extend(reorder_insert_rows(
            std::slice::from_ref(&row),
            insert_columns,
            target_columns,
        )?);
        if rows.len() >= INSERT_CHUNK_SIZE {
            insert_iceberg_rows(entry, namespace, table, &rows)?;
            rows.clear();
        }
        current = current.saturating_add(source.step);
    }
    if !rows.is_empty() {
        insert_iceberg_rows(entry, namespace, table, &rows)?;
    }
    Ok(())
}

fn reorder_insert_rows(
    rows: &[Vec<Literal>],
    insert_columns: &[String],
    target_columns: &[ColumnDef],
) -> Result<Vec<Vec<Literal>>, String> {
    if insert_columns.is_empty() {
        return Ok(rows.to_vec());
    }
    let mapping = build_insert_column_mapping(insert_columns, target_columns)?;
    rows.iter()
        .map(|row| reorder_insert_row(row, &mapping, target_columns.len()))
        .collect()
}

fn build_insert_column_mapping(
    insert_columns: &[String],
    target_columns: &[ColumnDef],
) -> Result<Vec<Option<usize>>, String> {
    let mut insert_index_by_name = HashMap::with_capacity(insert_columns.len());
    for (idx, column) in insert_columns.iter().enumerate() {
        let key = normalize_identifier(column)?;
        if insert_index_by_name.insert(key, idx).is_some() {
            return Err(format!("duplicate INSERT column `{column}`"));
        }
    }

    let mut mapping = Vec::with_capacity(target_columns.len());
    for column in target_columns {
        let key = normalize_identifier(&column.name)?;
        mapping.push(insert_index_by_name.remove(&key));
    }
    if let Some((name, _)) = insert_index_by_name.into_iter().next() {
        return Err(format!("unknown INSERT column `{name}`"));
    }
    Ok(mapping)
}

fn reorder_insert_row(
    row: &[Literal],
    mapping: &[Option<usize>],
    target_width: usize,
) -> Result<Vec<Literal>, String> {
    if row.len() > target_width {
        return Err(format!(
            "insert column count mismatch: expected at most {target_width} values, got {}",
            row.len()
        ));
    }
    let mut reordered = Vec::with_capacity(target_width);
    for source_idx in mapping {
        match source_idx {
            Some(idx) => {
                let value = row.get(*idx).cloned().ok_or_else(|| {
                    format!("insert value for column position {} is missing", idx + 1)
                })?;
                reordered.push(value);
            }
            None => reordered.push(Literal::Null),
        }
    }
    Ok(reordered)
}

fn evaluate_generate_series_row(
    source: &GenerateSeriesSelect,
    current: i64,
) -> Result<Vec<Literal>, String> {
    source
        .projection
        .iter()
        .map(|expr| evaluate_generate_series_expr(expr, &source.column_name, current))
        .collect()
}

fn evaluate_generate_series_expr(
    expr: &Expr,
    column_name: &str,
    current: i64,
) -> Result<Literal, String> {
    match expr {
        Expr::Column(column) => {
            if normalize_identifier(&column.name)? == normalize_identifier(column_name)? {
                Ok(Literal::Int(current))
            } else {
                Err(format!(
                    "standalone generate_series source does not provide column `{}`",
                    column.name
                ))
            }
        }
        Expr::Literal(literal) => Ok(literal.clone()),
        Expr::Arithmetic { left, op, right } => {
            let left = evaluate_generate_series_expr(left, column_name, current)?;
            let right = evaluate_generate_series_expr(right, column_name, current)?;
            eval_literal_arithmetic(*op, &left, &right)
        }
        Expr::Cast { expr, data_type } => {
            let value = evaluate_generate_series_expr(expr, column_name, current)?;
            cast_literal(value, data_type)
        }
        Expr::Comparison { .. }
        | Expr::Logical { .. }
        | Expr::IsNull { .. }
        | Expr::Aggregate(_)
        | Expr::ScalarFunction(_) => Err(
            "standalone generate_series insert-select only supports literal, column, arithmetic, and CAST expressions"
                .to_string(),
        ),
    }
}

/// Evaluate arithmetic on `Literal` values without `ManualValue`.
fn eval_literal_arithmetic(
    op: ArithmeticOp,
    left: &Literal,
    right: &Literal,
) -> Result<Literal, String> {
    if matches!(left, Literal::Null) || matches!(right, Literal::Null) {
        return Ok(Literal::Null);
    }
    match (left, right) {
        (Literal::Int(l), Literal::Int(r)) => match op {
            ArithmeticOp::Add => Ok(Literal::Int(l + r)),
            ArithmeticOp::Sub => Ok(Literal::Int(l - r)),
            ArithmeticOp::Mul => Ok(Literal::Int(l * r)),
            ArithmeticOp::Div => Ok(Literal::Float(*l as f64 / *r as f64)),
            ArithmeticOp::Mod => Ok(Literal::Int(l % r)),
        },
        (Literal::Int(l), Literal::Float(r)) => {
            eval_literal_arithmetic(op, &Literal::Float(*l as f64), &Literal::Float(*r))
        }
        (Literal::Float(l), Literal::Int(r)) => {
            eval_literal_arithmetic(op, &Literal::Float(*l), &Literal::Float(*r as f64))
        }
        (Literal::Float(l), Literal::Float(r)) => match op {
            ArithmeticOp::Add => Ok(Literal::Float(l + r)),
            ArithmeticOp::Sub => Ok(Literal::Float(l - r)),
            ArithmeticOp::Mul => Ok(Literal::Float(l * r)),
            ArithmeticOp::Div => Ok(Literal::Float(l / r)),
            ArithmeticOp::Mod => {
                Err("MOD only supports integer inputs in standalone mode".to_string())
            }
        },
        (l, r) => Err(format!(
            "standalone arithmetic does not support {:?} and {:?}",
            l, r
        )),
    }
}

/// Cast a `Literal` to the given SQL type without `ManualValue`.
fn cast_literal(value: Literal, data_type: &crate::sql::SqlType) -> Result<Literal, String> {
    use crate::sql::SqlType;
    match data_type {
        SqlType::String => match &value {
            Literal::Null => Ok(Literal::Null),
            Literal::Bool(v) => Ok(Literal::String(if *v {
                "1".to_string()
            } else {
                "0".to_string()
            })),
            Literal::Int(v) => Ok(Literal::String(v.to_string())),
            Literal::Float(v) => Ok(Literal::String(v.to_string())),
            Literal::String(_) | Literal::Date(_) => Ok(value),
            Literal::Array(_) => Err("cannot cast array to string".to_string()),
        },
        SqlType::Int | SqlType::BigInt | SqlType::TinyInt | SqlType::SmallInt => match &value {
            Literal::Null => Ok(Literal::Null),
            Literal::Int(_) => Ok(value),
            Literal::Float(v) => Ok(Literal::Int(*v as i64)),
            other => Err(format!("cannot cast {:?} to integer", other)),
        },
        SqlType::Float | SqlType::Double => match &value {
            Literal::Null => Ok(Literal::Null),
            Literal::Int(v) => Ok(Literal::Float(*v as f64)),
            Literal::Float(_) => Ok(value),
            other => Err(format!("cannot cast {:?} to floating point", other)),
        },
        other => Err(format!(
            "standalone generate_series does not support CAST to {:?}",
            other
        )),
    }
}

// ---------------------------------------------------------------------------
// SELECT without FROM helpers
// ---------------------------------------------------------------------------

/// Check if a query is a SELECT without any FROM clause.
fn is_select_without_from(query: &sqlparser::ast::Query) -> bool {
    if let sqlparser::ast::SetExpr::Select(ref select) = *query.body {
        select.from.is_empty()
    } else {
        false
    }
}

/// Evaluate a constant SELECT expression (no FROM) and return a single-row result.
fn evaluate_constant_select(query: &sqlparser::ast::Query) -> Result<QueryResult, String> {
    use sqlparser::ast as sqlast;

    let select = match query.body.as_ref() {
        sqlast::SetExpr::Select(s) => s.as_ref(),
        _ => return Err("only simple SELECT is supported for constant evaluation".into()),
    };

    let mut columns = Vec::new();
    let mut arrays: Vec<ArrayRef> = Vec::new();

    for (idx, item) in select.projection.iter().enumerate() {
        match item {
            sqlast::SelectItem::UnnamedExpr(expr) => {
                let (col_name, array) = evaluate_const_expr(expr, idx)?;
                columns.push(QueryResultColumn {
                    name: col_name,
                    data_type: array.data_type().clone(),
                    nullable: true,
                    logical_type: None,
                });
                arrays.push(array);
            }
            sqlast::SelectItem::ExprWithAlias { expr, alias } => {
                let (_, array) = evaluate_const_expr(expr, idx)?;
                columns.push(QueryResultColumn {
                    name: alias.value.clone(),
                    data_type: array.data_type().clone(),
                    nullable: true,
                    logical_type: None,
                });
                arrays.push(array);
            }
            other => {
                return Err(format!(
                    "unsupported projection item in constant SELECT: {:?}",
                    other
                ));
            }
        }
    }

    let fields: Vec<Field> = columns
        .iter()
        .map(|c| Field::new(&c.name, c.data_type.clone(), c.nullable))
        .collect();
    let schema = Arc::new(Schema::new(fields));
    let batch = RecordBatch::try_new(schema, arrays)
        .map_err(|e| format!("build constant SELECT batch failed: {e}"))?;
    let chunk = record_batch_to_chunk(batch)?;
    Ok(QueryResult {
        columns,
        chunks: vec![chunk],
    })
}

/// Evaluate a constant expression and return (column_name, single-element array).
fn evaluate_const_expr(
    expr: &sqlparser::ast::Expr,
    idx: usize,
) -> Result<(String, ArrayRef), String> {
    use arrow::array::*;
    use sqlparser::ast as sqlast;

    match expr {
        sqlast::Expr::Value(value_with_span) => match &value_with_span.value {
            sqlast::Value::Number(n, _) => {
                if let Ok(i) = n.parse::<i64>() {
                    Ok((n.clone(), Arc::new(Int64Array::from(vec![i])) as ArrayRef))
                } else if let Ok(f) = n.parse::<f64>() {
                    Ok((n.clone(), Arc::new(Float64Array::from(vec![f])) as ArrayRef))
                } else {
                    Err(format!("cannot parse number literal `{n}`"))
                }
            }
            sqlast::Value::SingleQuotedString(s) | sqlast::Value::DoubleQuotedString(s) => Ok((
                s.clone(),
                Arc::new(StringArray::from(vec![s.as_str()])) as ArrayRef,
            )),
            sqlast::Value::Boolean(b) => Ok((
                b.to_string(),
                Arc::new(BooleanArray::from(vec![*b])) as ArrayRef,
            )),
            sqlast::Value::Null => Ok((
                "NULL".to_string(),
                Arc::new(arrow::array::NullArray::new(1)) as ArrayRef,
            )),
            other => Err(format!("unsupported constant value: {:?}", other)),
        },
        sqlast::Expr::BinaryOp { left, op, right } => {
            let (_, left_arr) = evaluate_const_expr(left, idx)?;
            let (_, right_arr) = evaluate_const_expr(right, idx)?;
            let left_val = extract_numeric_scalar(&left_arr)?;
            let right_val = extract_numeric_scalar(&right_arr)?;
            let result = match op {
                sqlast::BinaryOperator::Plus => left_val + right_val,
                sqlast::BinaryOperator::Minus => left_val - right_val,
                sqlast::BinaryOperator::Multiply => left_val * right_val,
                sqlast::BinaryOperator::Divide => {
                    if right_val == 0.0 {
                        return Err("division by zero".to_string());
                    }
                    left_val / right_val
                }
                sqlast::BinaryOperator::Modulo => left_val % right_val,
                other => return Err(format!("unsupported binary operator: {:?}", other)),
            };
            // Return as int if both inputs were int and result is whole
            if left_arr.data_type() == &DataType::Int64
                && right_arr.data_type() == &DataType::Int64
                && result.fract() == 0.0
                && !matches!(op, sqlast::BinaryOperator::Divide)
            {
                Ok((
                    format!("_col{idx}"),
                    Arc::new(Int64Array::from(vec![result as i64])) as ArrayRef,
                ))
            } else {
                Ok((
                    format!("_col{idx}"),
                    Arc::new(Float64Array::from(vec![result])) as ArrayRef,
                ))
            }
        }
        sqlast::Expr::UnaryOp {
            op: sqlast::UnaryOperator::Minus,
            expr: inner,
        } => {
            let (_, arr) = evaluate_const_expr(inner, idx)?;
            let val = extract_numeric_scalar(&arr)?;
            if arr.data_type() == &DataType::Int64 {
                Ok((
                    format!("_col{idx}"),
                    Arc::new(Int64Array::from(vec![(-val) as i64])) as ArrayRef,
                ))
            } else {
                Ok((
                    format!("_col{idx}"),
                    Arc::new(Float64Array::from(vec![-val])) as ArrayRef,
                ))
            }
        }
        sqlast::Expr::Nested(inner) => evaluate_const_expr(inner, idx),
        other => Err(format!(
            "unsupported expression in constant SELECT: {:?}",
            other
        )),
    }
}

/// Extract a numeric scalar value from a single-element array.
fn extract_numeric_scalar(arr: &ArrayRef) -> Result<f64, String> {
    use arrow::array::*;
    match arr.data_type() {
        DataType::Int64 => {
            let a = arr
                .as_any()
                .downcast_ref::<Int64Array>()
                .ok_or("downcast Int64Array")?;
            Ok(a.value(0) as f64)
        }
        DataType::Float64 => {
            let a = arr
                .as_any()
                .downcast_ref::<Float64Array>()
                .ok_or("downcast Float64Array")?;
            Ok(a.value(0))
        }
        other => Err(format!("cannot extract numeric from {:?}", other)),
    }
}

// ---------------------------------------------------------------------------
// Local parquet table helpers
// ---------------------------------------------------------------------------

/// Convert a SQL type to an Arrow DataType.
fn sql_type_to_arrow_type(sql_type: &SqlType) -> Result<DataType, String> {
    match sql_type {
        SqlType::TinyInt => Ok(DataType::Int8),
        SqlType::SmallInt => Ok(DataType::Int16),
        SqlType::Int => Ok(DataType::Int32),
        SqlType::BigInt => Ok(DataType::Int64),
        SqlType::LargeInt => Ok(DataType::Int64),
        SqlType::Float => Ok(DataType::Float32),
        SqlType::Double => Ok(DataType::Float64),
        SqlType::String => Ok(DataType::Utf8),
        SqlType::Boolean => Ok(DataType::Boolean),
        SqlType::Date => Ok(DataType::Date32),
        SqlType::DateTime => Ok(DataType::Timestamp(TimeUnit::Microsecond, None)),
        SqlType::Time => Ok(DataType::Time64(TimeUnit::Microsecond)),
        SqlType::Decimal { precision, scale } => Ok(DataType::Decimal128(*precision, *scale)),
        SqlType::Array(inner) => {
            let inner_type = sql_type_to_arrow_type(inner)?;
            Ok(DataType::List(Arc::new(Field::new(
                "item", inner_type, true,
            ))))
        }
    }
}

/// Create a local parquet table from SQL column definitions.
fn create_local_table_from_columns(
    state: &Arc<StandaloneState>,
    name: &ObjectName,
    current_database: &str,
    columns: &[TableColumnDef],
) -> Result<StatementResult, String> {
    let resolved = resolve_local_table_name(name, current_database)?;

    // Convert SQL columns to Arrow fields
    let arrow_fields: Vec<Field> = columns
        .iter()
        .map(|col| {
            let dt = sql_type_to_arrow_type(&col.data_type)?;
            Ok(Field::new(&col.name, dt, true))
        })
        .collect::<Result<Vec<_>, String>>()?;

    let arrow_schema = Arc::new(Schema::new(arrow_fields.clone()));

    // Build ColumnDefs for the catalog
    let catalog_columns: Vec<ColumnDef> = arrow_fields
        .iter()
        .map(|f| ColumnDef {
            name: f.name().clone(),
            data_type: f.data_type().clone(),
            nullable: f.is_nullable(),
        })
        .collect();

    // Create a temporary directory for the table data
    let data_dir = std::env::temp_dir().join("novarocks_local_tables");
    std::fs::create_dir_all(&data_dir)
        .map_err(|e| format!("create local table data directory failed: {e}"))?;
    let table_file = data_dir.join(format!("{}_{}.parquet", resolved.database, resolved.table));

    // Write an empty parquet file with the schema
    let empty_arrays: Vec<ArrayRef> = arrow_fields
        .iter()
        .map(|f| arrow::array::new_empty_array(f.data_type()))
        .collect();
    let empty_batch = RecordBatch::try_new(Arc::clone(&arrow_schema), empty_arrays)
        .map_err(|e| format!("build empty batch failed: {e}"))?;
    write_parquet_to_path(&table_file, &empty_batch)?;

    // Register in catalog
    let table_def = super::catalog::TableDef {
        name: normalize_identifier(name.leaf())?,
        columns: catalog_columns,
        storage: TableStorage::LocalParquetFile {
            path: table_file.clone(),
        },
    };
    let mut guard = state
        .catalog
        .write()
        .expect("standalone catalog write lock");
    guard.register(&resolved.database, table_def)?;
    drop(guard);
    persist_local_table_if_needed(state, &resolved.database, &resolved.table, &table_file)?;
    Ok(StatementResult::Ok)
}

/// Insert rows into a local parquet table.
fn insert_into_local_table(
    state: &Arc<StandaloneState>,
    resolved: &ResolvedLocalTableName,
    table_def: &super::catalog::TableDef,
    insert_columns: &[String],
    source: &InsertSource,
) -> Result<StatementResult, String> {
    let rows = match source {
        InsertSource::Values(rows) => {
            reorder_insert_rows(rows, insert_columns, &table_def.columns)?
        }
        InsertSource::SelectLiteralRow(row) => reorder_insert_rows(
            std::slice::from_ref(row),
            insert_columns,
            &table_def.columns,
        )?,
        InsertSource::GenerateSeriesSelect(gen_source) => {
            insert_generate_series_rows_local(gen_source, insert_columns, &table_def.columns)?
        }
    };

    let path = match &table_def.storage {
        TableStorage::LocalParquetFile { path } => path.clone(),
        TableStorage::S3ParquetFiles { .. } => {
            return Err("INSERT into S3 tables is not supported".to_string());
        }
    };

    // Read existing data if any
    let existing_batch = read_local_parquet_data(&path, &table_def.columns)?;

    // Build new batch from inserted rows
    let new_batch = build_local_insert_batch(&table_def.columns, &rows)?;

    // Concatenate existing and new
    let combined = if existing_batch.num_rows() > 0 {
        arrow::compute::concat_batches(
            &new_batch.schema(),
            [&existing_batch, &new_batch].iter().copied(),
        )
        .map_err(|e| format!("concat local table batches failed: {e}"))?
    } else {
        new_batch
    };

    // Write back
    write_parquet_to_path(&path, &combined)?;

    // Re-register table in catalog to update metadata
    let table_def_updated = super::catalog::TableDef {
        name: table_def.name.clone(),
        columns: table_def.columns.clone(),
        storage: TableStorage::LocalParquetFile { path: path.clone() },
    };
    let mut guard = state
        .catalog
        .write()
        .expect("standalone catalog write lock");
    // Drop and re-register to allow re-reading
    let _ = guard.drop_table(&resolved.database, &resolved.table);
    guard.register(&resolved.database, table_def_updated)?;

    Ok(StatementResult::Ok)
}

/// Generate series rows for local table insert.
fn insert_generate_series_rows_local(
    source: &GenerateSeriesSelect,
    insert_columns: &[String],
    target_columns: &[ColumnDef],
) -> Result<Vec<Vec<Literal>>, String> {
    let mut rows = Vec::new();
    let mut current = source.start;
    let ascending = source.step > 0;
    while if ascending {
        current <= source.end
    } else {
        current >= source.end
    } {
        let row = evaluate_generate_series_row(source, current)?;
        rows.extend(reorder_insert_rows(
            std::slice::from_ref(&row),
            insert_columns,
            target_columns,
        )?);
        current = current.saturating_add(source.step);
    }
    Ok(rows)
}

/// Read existing data from a local parquet file, returning an empty batch if the file
/// has no data rows.
fn read_local_parquet_data(path: &Path, columns: &[ColumnDef]) -> Result<RecordBatch, String> {
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

    let file =
        std::fs::File::open(path).map_err(|e| format!("open local parquet file failed: {e}"))?;
    let builder = ParquetRecordBatchReaderBuilder::try_new(file)
        .map_err(|e| format!("read local parquet metadata failed: {e}"))?;
    let reader = builder
        .build()
        .map_err(|e| format!("build local parquet reader failed: {e}"))?;
    let mut batches = Vec::new();
    for batch_result in reader {
        let batch = batch_result.map_err(|e| format!("read local parquet batch failed: {e}"))?;
        batches.push(batch);
    }
    concat_or_empty_batches(columns, batches)
}

/// Build a RecordBatch from literal value rows for a local table.
fn build_local_insert_batch(
    columns: &[ColumnDef],
    rows: &[Vec<Literal>],
) -> Result<RecordBatch, String> {
    let schema = Arc::new(Schema::new(
        columns
            .iter()
            .map(|c| Field::new(&c.name, c.data_type.clone(), c.nullable))
            .collect::<Vec<_>>(),
    ));

    for row in rows {
        if row.len() != columns.len() {
            return Err(format!(
                "insert column count mismatch: expected {} values, got {}",
                columns.len(),
                row.len()
            ));
        }
    }

    let mut arrays = Vec::with_capacity(columns.len());
    for (idx, column) in columns.iter().enumerate() {
        let values: Vec<&Literal> = rows.iter().map(|row| &row[idx]).collect();
        arrays.push(build_local_literal_array(&column.data_type, &values)?);
    }

    RecordBatch::try_new(schema, arrays)
        .map_err(|e| format!("build local insert batch failed: {e}"))
}

/// Build an Arrow array from literal values for local table insertion.
fn build_local_literal_array(
    data_type: &DataType,
    values: &[&Literal],
) -> Result<ArrayRef, String> {
    use arrow::array::*;

    match data_type {
        DataType::Int8 => Ok(Arc::new(Int8Array::from(
            values
                .iter()
                .map(|literal| match literal {
                    Literal::Null => Ok(None),
                    Literal::Int(v) => i8::try_from(*v)
                        .map(Some)
                        .map_err(|_| format!("literal {v} is out of range for TINYINT")),
                    other => Err(format!("literal {:?} is not valid for TINYINT", other)),
                })
                .collect::<Result<Vec<_>, _>>()?,
        ))),
        DataType::Int16 => Ok(Arc::new(Int16Array::from(
            values
                .iter()
                .map(|literal| match literal {
                    Literal::Null => Ok(None),
                    Literal::Int(v) => i16::try_from(*v)
                        .map(Some)
                        .map_err(|_| format!("literal {v} is out of range for SMALLINT")),
                    other => Err(format!("literal {:?} is not valid for SMALLINT", other)),
                })
                .collect::<Result<Vec<_>, _>>()?,
        ))),
        DataType::Int32 => Ok(Arc::new(Int32Array::from(
            values
                .iter()
                .map(|literal| match literal {
                    Literal::Null => Ok(None),
                    Literal::Int(v) => i32::try_from(*v)
                        .map(Some)
                        .map_err(|_| format!("literal {v} is out of range for INT")),
                    Literal::String(s) => s
                        .trim()
                        .parse::<i32>()
                        .map(Some)
                        .map_err(|_| format!("literal `{s}` is not valid for INT")),
                    other => Err(format!("literal {:?} is not valid for INT", other)),
                })
                .collect::<Result<Vec<_>, _>>()?,
        ))),
        DataType::Int64 => Ok(Arc::new(Int64Array::from(
            values
                .iter()
                .map(|literal| match literal {
                    Literal::Null => Ok(None),
                    Literal::Int(v) => Ok(Some(*v)),
                    Literal::String(s) => s
                        .trim()
                        .parse::<i64>()
                        .map(Some)
                        .map_err(|_| format!("literal `{s}` is not valid for BIGINT")),
                    other => Err(format!("literal {:?} is not valid for BIGINT", other)),
                })
                .collect::<Result<Vec<_>, _>>()?,
        ))),
        DataType::Float32 => Ok(Arc::new(Float32Array::from(
            values
                .iter()
                .map(|literal| match literal {
                    Literal::Null => Ok(None),
                    Literal::Float(v) => Ok(Some(*v as f32)),
                    Literal::Int(v) => Ok(Some(*v as f32)),
                    other => Err(format!("literal {:?} is not valid for FLOAT", other)),
                })
                .collect::<Result<Vec<_>, _>>()?,
        ))),
        DataType::Float64 => Ok(Arc::new(Float64Array::from(
            values
                .iter()
                .map(|literal| match literal {
                    Literal::Null => Ok(None),
                    Literal::Float(v) => Ok(Some(*v)),
                    Literal::Int(v) => Ok(Some(*v as f64)),
                    Literal::String(s) => s
                        .trim()
                        .parse::<f64>()
                        .map(Some)
                        .map_err(|_| format!("literal `{s}` is not valid for DOUBLE")),
                    other => Err(format!("literal {:?} is not valid for DOUBLE", other)),
                })
                .collect::<Result<Vec<_>, _>>()?,
        ))),
        DataType::Decimal128(precision, scale) => {
            let parsed = values
                .iter()
                .map(|literal| match literal {
                    Literal::Null => Ok(None),
                    Literal::Int(v) => {
                        let factor = 10_i128
                            .checked_pow(*scale as u32)
                            .ok_or_else(|| format!("decimal scale {} is too large", scale))?;
                        Ok(Some(i128::from(*v) * factor))
                    }
                    Literal::Float(v) => {
                        let factor = 10_f64.powi(*scale as i32);
                        Ok(Some((v * factor).round() as i128))
                    }
                    Literal::String(s) => parse_decimal_string_to_i128(s, *scale).map(Some),
                    other => Err(format!("literal {:?} is not valid for DECIMAL", other)),
                })
                .collect::<Result<Vec<_>, _>>()?;
            let array = arrow::array::Decimal128Array::from(parsed)
                .with_precision_and_scale(*precision, *scale)
                .map_err(|e| format!("build DECIMAL array failed: {e}"))?;
            Ok(Arc::new(array))
        }
        DataType::Utf8 => Ok(Arc::new(StringArray::from(
            values
                .iter()
                .map(|literal| match literal {
                    Literal::Null => Ok(None),
                    Literal::String(v) | Literal::Date(v) => Ok(Some(v.clone())),
                    Literal::Int(v) => Ok(Some(v.to_string())),
                    Literal::Float(v) => Ok(Some(v.to_string())),
                    Literal::Bool(v) => {
                        Ok(Some(if *v { "1".to_string() } else { "0".to_string() }))
                    }
                    other => Err(format!("literal {:?} is not valid for STRING", other)),
                })
                .collect::<Result<Vec<_>, _>>()?,
        ))),
        DataType::Boolean => Ok(Arc::new(BooleanArray::from(
            values
                .iter()
                .map(|literal| match literal {
                    Literal::Null => Ok(None),
                    Literal::Bool(v) => Ok(Some(*v)),
                    Literal::Int(0) => Ok(Some(false)),
                    Literal::Int(1) => Ok(Some(true)),
                    other => Err(format!("literal {:?} is not valid for BOOLEAN", other)),
                })
                .collect::<Result<Vec<_>, _>>()?,
        ))),
        DataType::Date32 => Ok(Arc::new(Date32Array::from(
            values
                .iter()
                .map(|literal| match literal {
                    Literal::Null => Ok(None),
                    Literal::Date(v) | Literal::String(v) => parse_date_string_to_days(v).map(Some),
                    other => Err(format!("literal {:?} is not valid for DATE", other)),
                })
                .collect::<Result<Vec<_>, _>>()?,
        ))),
        DataType::Timestamp(TimeUnit::Microsecond, _) => {
            Ok(Arc::new(TimestampMicrosecondArray::from(
                values
                    .iter()
                    .map(|literal| match literal {
                        Literal::Null => Ok(None),
                        Literal::String(v) | Literal::Date(v) => {
                            parse_datetime_string_to_micros(v).map(Some)
                        }
                        other => Err(format!("literal {:?} is not valid for DATETIME", other)),
                    })
                    .collect::<Result<Vec<_>, _>>()?,
            )))
        }
        other => Err(format!(
            "local table insert does not support column type {:?}",
            other
        )),
    }
}

fn parse_decimal_string_to_i128(s: &str, scale: i8) -> Result<i128, String> {
    let s = s.trim();
    let factor = 10_i128
        .checked_pow(scale as u32)
        .ok_or_else(|| format!("decimal scale {} is too large", scale))?;
    if let Some(dot_pos) = s.find('.') {
        let negative = s.starts_with('-');
        let int_part = &s[if negative { 1 } else { 0 }..dot_pos];
        let frac_str = &s[dot_pos + 1..];
        let int_val: i128 = if int_part.is_empty() {
            0
        } else {
            int_part
                .parse()
                .map_err(|_| format!("invalid decimal literal `{s}`"))?
        };
        let frac_len = frac_str.len();
        let frac_val: i128 = if frac_str.is_empty() {
            0
        } else {
            frac_str
                .parse()
                .map_err(|_| format!("invalid decimal literal `{s}`"))?
        };
        let scale_u = scale as usize;
        let adjusted_frac = if frac_len <= scale_u {
            frac_val * 10_i128.pow((scale_u - frac_len) as u32)
        } else {
            frac_val / 10_i128.pow((frac_len - scale_u) as u32)
        };
        let abs_val = int_val * factor + adjusted_frac;
        Ok(if negative { -abs_val } else { abs_val })
    } else {
        let int_val: i128 = s
            .parse()
            .map_err(|_| format!("invalid decimal literal `{s}`"))?;
        Ok(int_val * factor)
    }
}

fn parse_date_string_to_days(s: &str) -> Result<i32, String> {
    use chrono::NaiveDate;
    let date = NaiveDate::parse_from_str(s.trim(), "%Y-%m-%d")
        .map_err(|e| format!("invalid date literal `{s}`: {e}"))?;
    let epoch = NaiveDate::from_ymd_opt(1970, 1, 1).expect("epoch");
    Ok((date - epoch).num_days() as i32)
}

fn parse_datetime_string_to_micros(s: &str) -> Result<i64, String> {
    use chrono::NaiveDateTime;
    let s = s.trim();
    // Try datetime first, then date-only
    if let Ok(dt) = NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S") {
        return Ok(dt.and_utc().timestamp_micros());
    }
    if let Ok(dt) = NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S%.f") {
        return Ok(dt.and_utc().timestamp_micros());
    }
    if let Ok(d) = chrono::NaiveDate::parse_from_str(s, "%Y-%m-%d") {
        let dt = d.and_hms_opt(0, 0, 0).expect("midnight");
        return Ok(dt.and_utc().timestamp_micros());
    }
    Err(format!("invalid datetime literal `{s}`"))
}

/// Cast a RecordBatch to match a target schema (column-by-column cast).
fn cast_batch_to_schema(
    batch: &RecordBatch,
    target_schema: &std::sync::Arc<arrow::datatypes::Schema>,
) -> Result<RecordBatch, String> {
    if batch.schema().fields().len() != target_schema.fields().len() {
        return Err(format!(
            "INSERT SELECT column count mismatch: source={}, target={}",
            batch.schema().fields().len(),
            target_schema.fields().len()
        ));
    }
    let mut columns = Vec::with_capacity(batch.num_columns());
    for (idx, target_field) in target_schema.fields().iter().enumerate() {
        let source_col = batch.column(idx);
        if source_col.data_type() == target_field.data_type() {
            columns.push(source_col.clone());
        } else {
            let casted =
                arrow::compute::cast(source_col, target_field.data_type()).map_err(|e| {
                    format!(
                        "cast column {} from {:?} to {:?} failed: {e}",
                        target_field.name(),
                        source_col.data_type(),
                        target_field.data_type()
                    )
                })?;
            columns.push(casted);
        }
    }
    RecordBatch::try_new(target_schema.clone(), columns)
        .map_err(|e| format!("rebuild insert-select batch failed: {e}"))
}

/// Write a RecordBatch to a parquet file at the given path.
pub(crate) fn write_parquet_to_path(path: &Path, batch: &RecordBatch) -> Result<(), String> {
    use parquet::arrow::ArrowWriter;

    let file = std::fs::File::create(path)
        .map_err(|e| format!("create local parquet file failed: {e}"))?;
    let mut writer = ArrowWriter::try_new(file, batch.schema(), None)
        .map_err(|e| format!("create local parquet writer failed: {e}"))?;
    writer
        .write(batch)
        .map_err(|e| format!("write local parquet batch failed: {e}"))?;
    writer
        .close()
        .map_err(|e| format!("close local parquet writer failed: {e}"))?;
    Ok(())
}

// ---------------------------------------------------------------------------
// Dual table (virtual 1-row table for SELECT without FROM)
// ---------------------------------------------------------------------------

fn ensure_dual_table(state: &Arc<StandaloneState>) -> Result<(), String> {
    ensure_dual_in_database(state, DEFAULT_DATABASE)
}

fn ensure_dual_in_database(state: &Arc<StandaloneState>, database: &str) -> Result<(), String> {
    let guard = state.catalog.read().expect("standalone catalog read lock");
    if guard.get(database, "__dual__").is_ok() {
        return Ok(());
    }
    drop(guard);

    // Create a 1-row parquet with a single dummy column
    let dir = std::env::temp_dir().join("novarocks_dual");
    std::fs::create_dir_all(&dir).map_err(|e| format!("create dual table dir failed: {e}"))?;
    let path = dir.join(format!("dual_{}.parquet", database));
    let schema = std::sync::Arc::new(arrow::datatypes::Schema::new(vec![
        arrow::datatypes::Field::new("__dummy__", arrow::datatypes::DataType::Int8, true),
    ]));
    let col = std::sync::Arc::new(arrow::array::Int8Array::from(vec![Some(0i8)]));
    let batch = RecordBatch::try_new(schema.clone(), vec![col])
        .map_err(|e| format!("build dual batch failed: {e}"))?;
    write_parquet_to_path(&path, &batch)?;

    let table = super::catalog::TableDef {
        name: "__dual__".to_string(),
        columns: vec![super::catalog::ColumnDef {
            name: "__dummy__".to_string(),
            data_type: arrow::datatypes::DataType::Int8,
            nullable: true,
        }],
        storage: TableStorage::LocalParquetFile { path },
    };
    let mut guard = state
        .catalog
        .write()
        .expect("standalone catalog write lock");
    guard.register(database, table).ok(); // ignore if already exists
    Ok(())
}

// ---------------------------------------------------------------------------
// Iceberg helpers
// ---------------------------------------------------------------------------

fn load_full_iceberg_batch(loaded: &IcebergLoadedTable) -> Result<RecordBatch, String> {
    let batches = block_on_standalone_async(async {
        loaded
            .table
            .scan()
            .build()
            .map_err(|e| format!("build iceberg scan failed: {e}"))?
            .to_arrow()
            .await
            .map_err(|e| format!("open iceberg arrow stream failed: {e}"))?
            .try_collect::<Vec<_>>()
            .await
            .map_err(|e| format!("read iceberg scan batches failed: {e}"))
    })??;
    let normalized_batches = batches
        .into_iter()
        .map(|batch| normalize_iceberg_source_batch(batch, &loaded.columns))
        .collect::<Result<Vec<_>, _>>()?;
    let combined = concat_or_empty_batches(&loaded.columns, normalized_batches)?;
    apply_iceberg_table_semantics_if_needed(loaded, combined)
}

fn apply_iceberg_table_semantics_if_needed(
    loaded: &IcebergLoadedTable,
    batch: RecordBatch,
) -> Result<RecordBatch, String> {
    let Some(key_desc) = loaded.key_desc.as_ref() else {
        return Ok(batch);
    };
    if key_desc.kind != TableKeyKind::Aggregate || batch.num_rows() <= 1 {
        return Ok(batch);
    }

    let mut column_index_by_name = HashMap::with_capacity(loaded.columns.len());
    for (idx, column) in loaded.columns.iter().enumerate() {
        column_index_by_name.insert(normalize_identifier(&column.name)?, idx);
    }
    let key_indices = key_desc
        .columns
        .iter()
        .map(|column| {
            let key = normalize_identifier(column)?;
            column_index_by_name
                .get(&key)
                .copied()
                .ok_or_else(|| format!("aggregate key column `{column}` not found in table schema"))
        })
        .collect::<Result<Vec<_>, _>>()?;

    let num_columns = batch.num_columns();
    let mut merged_rows = Vec::<Vec<Literal>>::new();
    let mut row_index_by_key = HashMap::<Vec<LiteralKey>, usize>::new();
    for row_idx in 0..batch.num_rows() {
        let row = (0..num_columns)
            .map(|col_idx| literal_from_batch(batch.column(col_idx), row_idx))
            .collect::<Result<Vec<_>, _>>()?;
        let key = key_indices
            .iter()
            .map(|idx| literal_to_key(&row[*idx]))
            .collect::<Vec<_>>();
        if let Some(existing_idx) = row_index_by_key.get(&key).copied() {
            let existing = merged_rows
                .get_mut(existing_idx)
                .ok_or_else(|| "aggregate key merge state is inconsistent".to_string())?;
            merge_aggregate_table_row(existing, &row, &key_indices, loaded)?;
        } else {
            row_index_by_key.insert(key, merged_rows.len());
            merged_rows.push(row);
        }
    }

    super::iceberg::build_insert_batch(loaded, &merged_rows)
}

fn merge_aggregate_table_row(
    existing: &mut [Literal],
    incoming: &[Literal],
    key_indices: &[usize],
    loaded: &IcebergLoadedTable,
) -> Result<(), String> {
    for (column_idx, (existing_value, incoming_value)) in
        existing.iter_mut().zip(incoming.iter()).enumerate()
    {
        if key_indices.contains(&column_idx) {
            continue;
        }
        let column = loaded
            .columns
            .get(column_idx)
            .ok_or_else(|| "aggregate table column index is out of bounds".to_string())?;
        let key = normalize_identifier(&column.name)?;
        let aggregation = loaded
            .column_aggregations
            .get(&key)
            .copied()
            .unwrap_or(ColumnAggregation::Replace);
        merge_aggregate_table_value(existing_value, incoming_value, aggregation)?;
    }
    Ok(())
}

fn merge_aggregate_table_value(
    existing: &mut Literal,
    incoming: &Literal,
    aggregation: ColumnAggregation,
) -> Result<(), String> {
    match aggregation {
        ColumnAggregation::Sum => match (existing.clone(), incoming) {
            (_, Literal::Null) => Ok(()),
            (Literal::Null, other) => {
                *existing = other.clone();
                Ok(())
            }
            (Literal::Int(left), Literal::Int(right)) => {
                *existing = Literal::Int(
                    left.checked_add(*right)
                        .ok_or_else(|| format!("aggregate SUM overflow: {left} + {right}"))?,
                );
                Ok(())
            }
            (Literal::Float(left), Literal::Float(right)) => {
                *existing = Literal::Float(left + right);
                Ok(())
            }
            (Literal::Float(left), Literal::Int(right)) => {
                *existing = Literal::Float(left + (*right as f64));
                Ok(())
            }
            (Literal::Int(left), Literal::Float(right)) => {
                *existing = Literal::Float((left as f64) + right);
                Ok(())
            }
            (left, right) => Err(format!(
                "aggregate SUM does not support values {:?} and {:?}",
                left, right
            )),
        },
        ColumnAggregation::Min => {
            if matches!(incoming, Literal::Null) {
                return Ok(());
            }
            if matches!(existing, Literal::Null)
                || compare_literals(incoming, existing)? == std::cmp::Ordering::Less
            {
                *existing = incoming.clone();
            }
            Ok(())
        }
        ColumnAggregation::Max => {
            if matches!(incoming, Literal::Null) {
                return Ok(());
            }
            if matches!(existing, Literal::Null)
                || compare_literals(incoming, existing)? == std::cmp::Ordering::Greater
            {
                *existing = incoming.clone();
            }
            Ok(())
        }
        ColumnAggregation::Replace => {
            *existing = incoming.clone();
            Ok(())
        }
    }
}

fn compare_literals(left: &Literal, right: &Literal) -> Result<std::cmp::Ordering, String> {
    use std::cmp::Ordering;
    match (left, right) {
        (Literal::Int(l), Literal::Int(r)) => Ok(l.cmp(r)),
        (Literal::Float(l), Literal::Float(r)) => Ok(l.partial_cmp(r).unwrap_or(Ordering::Equal)),
        (Literal::Int(l), Literal::Float(r)) => {
            Ok((*l as f64).partial_cmp(r).unwrap_or(Ordering::Equal))
        }
        (Literal::Float(l), Literal::Int(r)) => {
            Ok(l.partial_cmp(&(*r as f64)).unwrap_or(Ordering::Equal))
        }
        (Literal::String(l), Literal::String(r)) => Ok(l.cmp(r)),
        (Literal::Bool(l), Literal::Bool(r)) => Ok(l.cmp(r)),
        (l, r) => Err(format!(
            "cannot compare {:?} and {:?} for aggregate merge",
            l, r
        )),
    }
}

/// Hashable key derived from `Literal` for use in aggregate-table dedup maps.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
enum LiteralKey {
    Null,
    Bool(bool),
    Int(i64),
    Float(u64),
    String(String),
}

fn literal_to_key(literal: &Literal) -> LiteralKey {
    match literal {
        Literal::Null => LiteralKey::Null,
        Literal::Bool(v) => LiteralKey::Bool(*v),
        Literal::Int(v) => LiteralKey::Int(*v),
        Literal::Float(v) => LiteralKey::Float(v.to_bits()),
        Literal::String(v) | Literal::Date(v) => LiteralKey::String(v.clone()),
        Literal::Array(values) => {
            // Flatten to a string representation for hashing
            let s = values
                .iter()
                .map(|v| format!("{:?}", v))
                .collect::<Vec<_>>()
                .join(",");
            LiteralKey::String(s)
        }
    }
}

/// Extract a `Literal` from a batch column at a specific row.
fn literal_from_batch(column: &ArrayRef, row_idx: usize) -> Result<Literal, String> {
    use arrow::array::*;
    use arrow::datatypes::TimeUnit;

    if column.is_null(row_idx) {
        return Ok(Literal::Null);
    }
    match column.data_type() {
        DataType::Boolean => {
            let arr = column
                .as_any()
                .downcast_ref::<BooleanArray>()
                .ok_or("downcast BooleanArray")?;
            Ok(Literal::Bool(arr.value(row_idx)))
        }
        DataType::Int8 => {
            let arr = column
                .as_any()
                .downcast_ref::<Int8Array>()
                .ok_or("downcast Int8Array")?;
            Ok(Literal::Int(i64::from(arr.value(row_idx))))
        }
        DataType::Int16 => {
            let arr = column
                .as_any()
                .downcast_ref::<Int16Array>()
                .ok_or("downcast Int16Array")?;
            Ok(Literal::Int(i64::from(arr.value(row_idx))))
        }
        DataType::Int32 => {
            let arr = column
                .as_any()
                .downcast_ref::<Int32Array>()
                .ok_or("downcast Int32Array")?;
            Ok(Literal::Int(i64::from(arr.value(row_idx))))
        }
        DataType::Int64 => {
            let arr = column
                .as_any()
                .downcast_ref::<Int64Array>()
                .ok_or("downcast Int64Array")?;
            Ok(Literal::Int(arr.value(row_idx)))
        }
        DataType::Float32 => {
            let arr = column
                .as_any()
                .downcast_ref::<Float32Array>()
                .ok_or("downcast Float32Array")?;
            Ok(Literal::Float(f64::from(arr.value(row_idx))))
        }
        DataType::Float64 => {
            let arr = column
                .as_any()
                .downcast_ref::<Float64Array>()
                .ok_or("downcast Float64Array")?;
            Ok(Literal::Float(arr.value(row_idx)))
        }
        DataType::Decimal128(_, scale) => {
            let arr = column
                .as_any()
                .downcast_ref::<Decimal128Array>()
                .ok_or("downcast Decimal128Array")?;
            let value = arr.value(row_idx);
            if *scale == 0 {
                i64::try_from(value)
                    .map(Literal::Int)
                    .map_err(|_| format!("decimal value {value} is out of range for INT64"))
            } else {
                Ok(Literal::String(format_decimal128_value(value, *scale)?))
            }
        }
        DataType::Utf8 => {
            let arr = column
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or("downcast StringArray")?;
            Ok(Literal::String(arr.value(row_idx).to_string()))
        }
        DataType::Date32 => {
            use chrono::{Duration as ChronoDuration, NaiveDate};
            let arr = column
                .as_any()
                .downcast_ref::<Date32Array>()
                .ok_or("downcast Date32Array")?;
            let days = arr.value(row_idx);
            let epoch = NaiveDate::from_ymd_opt(1970, 1, 1).expect("epoch");
            let formatted = (epoch + ChronoDuration::days(i64::from(days)))
                .format("%Y-%m-%d")
                .to_string();
            Ok(Literal::Date(formatted))
        }
        DataType::Timestamp(TimeUnit::Microsecond, _) => {
            use chrono::NaiveDateTime;
            let arr = column
                .as_any()
                .downcast_ref::<TimestampMicrosecondArray>()
                .ok_or("downcast TimestampMicrosecondArray")?;
            let micros = arr.value(row_idx);
            let formatted = NaiveDateTime::from_timestamp_micros(micros)
                .expect("timestamp micros should be valid")
                .format("%Y-%m-%d %H:%M:%S")
                .to_string();
            Ok(Literal::String(formatted))
        }
        DataType::List(_) => {
            let list = column
                .as_any()
                .downcast_ref::<ListArray>()
                .ok_or("downcast ListArray")?;
            let values = list.value(row_idx);
            let mut items = Vec::with_capacity(values.len());
            for idx in 0..values.len() {
                items.push(literal_from_batch(&values, idx)?);
            }
            Ok(Literal::Array(items))
        }
        other => Err(format!(
            "literal_from_batch does not support column type {:?}",
            other
        )),
    }
}

fn format_decimal128_value(value: i128, scale: i8) -> Result<String, String> {
    if scale < 0 {
        return Err(format!("unsupported decimal scale: {scale}"));
    }
    let scale = u32::try_from(scale).map_err(|_| format!("unsupported decimal scale: {scale}"))?;
    if scale == 0 {
        return Ok(value.to_string());
    }
    let factor = 10_u128
        .checked_pow(scale)
        .ok_or_else(|| format!("unsupported decimal scale: {scale}"))?;
    let negative = value.is_negative();
    let abs = value.unsigned_abs();
    let whole = abs / factor;
    let fraction = abs % factor;
    Ok(format!(
        "{}{}.{:0width$}",
        if negative { "-" } else { "" },
        whole,
        fraction,
        width = scale as usize
    ))
}

fn normalize_iceberg_source_batch(
    batch: RecordBatch,
    columns: &[ColumnDef],
) -> Result<RecordBatch, String> {
    let field_indices = iceberg_field_indices(&batch)?;
    let arrays = batch
        .schema()
        .fields()
        .iter()
        .enumerate()
        .map(|(idx, _)| batch.column(idx).clone())
        .collect::<Vec<_>>();
    let arrays = columns
        .iter()
        .map(|column| {
            let normalized = normalize_identifier(&column.name)
                .map_err(|e| format!("normalize source column `{}` failed: {e}", column.name))?;
            let batch_idx = field_indices
                .get(&normalized)
                .copied()
                .ok_or_else(|| format!("iceberg source batch missing column `{}`", column.name))?;
            normalize_iceberg_array_type(&arrays[batch_idx], &column.name, &column.data_type)
        })
        .collect::<Result<Vec<_>, _>>()?;
    let schema = Arc::new(Schema::new(
        columns
            .iter()
            .map(|column| Field::new(&column.name, column.data_type.clone(), column.nullable))
            .collect::<Vec<_>>(),
    ));
    RecordBatch::try_new(schema, arrays)
        .map_err(|e| format!("rebuild normalized iceberg source batch failed: {e}"))
}

fn iceberg_field_indices(batch: &RecordBatch) -> Result<HashMap<String, usize>, String> {
    let mut indices = HashMap::with_capacity(batch.num_columns());
    for (idx, field) in batch.schema().fields().iter().enumerate() {
        let normalized = normalize_identifier(field.name()).map_err(|e| {
            format!(
                "normalize iceberg batch column name `{}` failed: {e}",
                field.name()
            )
        })?;
        if indices.insert(normalized.clone(), idx).is_some() {
            return Err(format!(
                "duplicate iceberg batch column `{}` after normalization",
                field.name()
            ));
        }
    }
    Ok(indices)
}

fn normalize_iceberg_array_type(
    array: &ArrayRef,
    column_name: &str,
    target_type: &DataType,
) -> Result<ArrayRef, String> {
    if array.data_type() == target_type {
        return Ok(array.clone());
    }
    arrow::compute::cast(array, target_type).map_err(|e| {
        format!(
            "cast iceberg column `{column_name}` from {:?} to {:?} failed: {e}",
            array.data_type(),
            target_type
        )
    })
}

fn concat_or_empty_batches(
    columns: &[ColumnDef],
    batches: Vec<RecordBatch>,
) -> Result<RecordBatch, String> {
    if let Some(first) = batches.first() {
        arrow::compute::concat_batches(&first.schema(), batches.iter())
            .map_err(|e| format!("concat standalone batches failed: {e}"))
    } else {
        let schema = Arc::new(Schema::new(
            columns
                .iter()
                .map(|column| Field::new(&column.name, column.data_type.clone(), column.nullable))
                .collect::<Vec<_>>(),
        ));
        let arrays = columns
            .iter()
            .map(|column| arrow::array::new_empty_array(&column.data_type))
            .collect::<Vec<_>>();
        RecordBatch::try_new(schema, arrays)
            .map_err(|e| format!("build empty standalone batch failed: {e}"))
    }
}

// ---------------------------------------------------------------------------
// Table resolution helpers
// ---------------------------------------------------------------------------

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

// ---------------------------------------------------------------------------
// Utility functions
// ---------------------------------------------------------------------------

fn block_on_standalone_async<F>(future: F) -> Result<F::Output, String>
where
    F: std::future::Future,
{
    if let Ok(handle) = Handle::try_current() {
        return Ok(handle.block_on(future));
    }
    data_block_on(future)
}

fn record_batch_to_chunk(batch: RecordBatch) -> Result<Chunk, String> {
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
// Query table name extraction (for Iceberg materialization)
// ---------------------------------------------------------------------------

/// Extract simple table names from a query AST (for Iceberg table materialization).
fn extract_table_names_from_query(query: &sqlparser::ast::Query) -> Vec<String> {
    let mut names = Vec::new();
    extract_table_names_from_set_expr(query.body.as_ref(), &mut names);
    names.sort();
    names.dedup();
    names
}

fn extract_table_names_from_set_expr(expr: &sqlparser::ast::SetExpr, names: &mut Vec<String>) {
    match expr {
        sqlparser::ast::SetExpr::Select(s) => {
            for from in &s.from {
                extract_table_names_from_table_factor(&from.relation, names);
                for join in &from.joins {
                    extract_table_names_from_table_factor(&join.relation, names);
                }
            }
            // Also extract table names from subqueries in WHERE/HAVING/SELECT
            extract_table_names_from_expr_opt(s.selection.as_ref(), names);
            extract_table_names_from_expr_opt(s.having.as_ref(), names);
            for item in &s.projection {
                if let sqlparser::ast::SelectItem::UnnamedExpr(expr)
                | sqlparser::ast::SelectItem::ExprWithAlias { expr, .. } = item
                {
                    extract_table_names_from_expr(expr, names);
                }
            }
        }
        sqlparser::ast::SetExpr::SetOperation { left, right, .. } => {
            extract_table_names_from_set_expr(left, names);
            extract_table_names_from_set_expr(right, names);
        }
        sqlparser::ast::SetExpr::Query(q) => {
            extract_table_names_from_set_expr(q.body.as_ref(), names);
        }
        _ => {}
    }
}

fn extract_table_names_from_table_factor(
    factor: &sqlparser::ast::TableFactor,
    names: &mut Vec<String>,
) {
    match factor {
        sqlparser::ast::TableFactor::Table { name, .. } => {
            // Take the last part as the table name (ignore catalog/db qualifiers)
            if let Some(last) = name.0.last() {
                let n = match last {
                    sqlparser::ast::ObjectNamePart::Identifier(ident) => ident.value.to_lowercase(),
                    other => other.to_string().to_lowercase(),
                };
                names.push(n);
            }
        }
        sqlparser::ast::TableFactor::Derived { subquery, .. } => {
            extract_table_names_from_set_expr(subquery.body.as_ref(), names);
        }
        _ => {}
    }
}

fn extract_table_names_from_expr_opt(expr: Option<&sqlparser::ast::Expr>, names: &mut Vec<String>) {
    if let Some(e) = expr {
        extract_table_names_from_expr(e, names);
    }
}

fn extract_table_names_from_expr(expr: &sqlparser::ast::Expr, names: &mut Vec<String>) {
    // Use the Display impl to get the SQL string, then recursively look for
    // subquery patterns. This is simpler than matching every AST variant.
    // For subquery extraction, we only need to find Subquery/Exists/InSubquery nodes.
    use sqlparser::ast::Expr;
    match expr {
        Expr::Subquery(q) | Expr::Exists { subquery: q, .. } => {
            extract_table_names_from_subquery(q, names);
        }
        Expr::InSubquery { subquery, expr, .. } => {
            extract_table_names_from_subquery(subquery, names);
            extract_table_names_from_expr(expr, names);
        }
        Expr::BinaryOp { left, right, .. } => {
            extract_table_names_from_expr(left, names);
            extract_table_names_from_expr(right, names);
        }
        Expr::UnaryOp { expr, .. } | Expr::Nested(expr) => {
            extract_table_names_from_expr(expr, names);
        }
        Expr::Between {
            expr, low, high, ..
        } => {
            extract_table_names_from_expr(expr, names);
            extract_table_names_from_expr(low, names);
            extract_table_names_from_expr(high, names);
        }
        _ => {} // literals, column refs, functions, etc.
    }
}

fn extract_table_names_from_subquery(query: &sqlparser::ast::Query, names: &mut Vec<String>) {
    extract_table_names_from_set_expr(query.body.as_ref(), names);
}

// ---------------------------------------------------------------------------
// ADD FILES SQL parsing
// ---------------------------------------------------------------------------

/// Check if SQL looks like ALTER TABLE ... ADD FILES FROM ...
fn looks_like_add_files(sql: &str) -> bool {
    let upper = sql.trim().to_ascii_uppercase();
    upper.starts_with("ALTER TABLE") && upper.contains("ADD FILES FROM")
}

/// Parse: ALTER TABLE [catalog.db.]table ADD FILES FROM 's3://...'
fn parse_add_files_sql(sql: &str) -> Result<(Vec<String>, String), String> {
    // Extract the part between ALTER TABLE and ADD FILES FROM
    let upper = sql.to_ascii_uppercase();
    let alter_idx = upper.find("ALTER TABLE").ok_or("missing ALTER TABLE")?;
    let add_files_idx = upper
        .find("ADD FILES FROM")
        .ok_or("missing ADD FILES FROM")?;

    let table_str = sql[alter_idx + 11..add_files_idx].trim();
    let table_parts: Vec<String> = table_str
        .split('.')
        .map(|s| s.trim().trim_matches('`').to_lowercase())
        .collect();

    // Extract the path after ADD FILES FROM
    let after_from = &sql[add_files_idx + 14..];
    let path = after_from
        .trim()
        .trim_end_matches(';')
        .trim()
        .trim_matches('\'')
        .trim_matches('"')
        .to_string();

    if path.is_empty() {
        return Err("ADD FILES FROM requires a path".to_string());
    }

    Ok((table_parts, path))
}

// ---------------------------------------------------------------------------
// Query plan build + execute (delegates to crate::sql::*)
// ---------------------------------------------------------------------------

use crate::sql::physical::PlanBuildResult;

fn build_query_plan(
    query: &sqlparser::ast::Query,
    catalog: &InMemoryCatalog,
    current_database: &str,
) -> Result<PlanBuildResult, String> {
    let resolved = crate::sql::analyzer::analyze(query, catalog, current_database)?;
    let output_columns = resolved.output_columns.clone();
    let logical = crate::sql::planner::plan(resolved)?;
    let optimized = crate::sql::optimizer::optimize(logical);
    crate::sql::physical::emit(optimized, &output_columns, catalog, current_database)
}

fn execute_plan(result: PlanBuildResult) -> Result<QueryResult, String> {
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
    // Use available CPU cores for pipeline parallelism (capped at 8)
    let pipeline_dop = std::thread::available_parallelism()
        .map(|p| p.get().min(8))
        .unwrap_or(4);
    execute_plan_with_pipeline(
        exec_plan,
        false,
        std::time::Duration::from_millis(10),
        Box::new(ResultSinkFactory::new(handle.clone())),
        None,
        None,
        pipeline_dop as _,
        std::sync::Arc::new(RuntimeState::default()),
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
// Tests
// ---------------------------------------------------------------------------

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
            .execute_in_context("select c2, c1 from nums order by 1, 2", Some("ice"), "db1")
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
