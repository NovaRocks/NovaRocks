//! Connector-agnostic backend traits. Each trait represents one axis of
//! capability (catalog admin, table scan-side source, table write-side sink,
//! materialized-view lifecycle). A connector implements whichever subset
//! applies to it.
//!
//! The traits live here rather than in each per-connector mod.rs so callers
//! can program against `dyn CatalogBackend` without knowing which concrete
//! connector fulfils the request.

use arrow::record_batch::RecordBatch;

use crate::runtime::query_result::QueryResult;
use crate::sql::catalog::{ColumnDef, TableDef};
use crate::sql::parser::ast::{
    AlterIcebergPartitionSpecStmt, CreateMaterializedViewStmt, DropMaterializedViewStmt,
    IcebergPartitionFieldExpr, Literal, RefreshMaterializedViewStmt, ShowMaterializedViewsStmt,
    TableColumnDef, TableKeyDesc,
};

/// Request to create a table. Unified shape across all catalog backends;
/// backends ignore fields that don't apply to them (e.g. `bucket_count` is
/// managed-lake-only).
#[derive(Clone, Debug)]
pub(crate) struct CreateTableRequest {
    pub catalog: String,
    pub namespace: String,
    pub table: String,
    pub columns: Vec<TableColumnDef>,
    pub key_desc: Option<TableKeyDesc>,
    pub bucket_count: Option<u32>,
    pub partition_fields: Vec<IcebergPartitionFieldExpr>,
    pub properties: Vec<(String, String)>,
}

/// Resolved table metadata returned by `CatalogBackend::load_table`. This is
/// the subset of table shape the engine layer needs in order to plan INSERTs
/// and to register the table with the in-memory logical catalog.
#[derive(Clone, Debug)]
pub(crate) struct ResolvedTable {
    pub catalog: String,
    pub namespace: String,
    pub table: String,
    pub columns: Vec<ColumnDef>,
}

/// Catalog-plane operations: create/drop namespace and create/drop/load
/// tables. Implemented once per catalog type (iceberg, managed-lake, ...).
pub(crate) trait CatalogBackend: Send + Sync {
    fn name(&self) -> &'static str;

    fn namespace_exists(&self, catalog: &str, namespace: &str) -> Result<bool, String>;
    fn create_namespace(&self, catalog: &str, namespace: &str) -> Result<(), String>;
    fn drop_namespace(&self, catalog: &str, namespace: &str, force: bool) -> Result<(), String>;

    fn create_table(&self, req: CreateTableRequest) -> Result<(), String>;
    fn table_exists(&self, catalog: &str, namespace: &str, table: &str) -> Result<bool, String>;
    fn alter_iceberg_partition_spec(
        &self,
        _catalog: &str,
        _namespace: &str,
        _table: &str,
        _stmt: AlterIcebergPartitionSpecStmt,
    ) -> Result<(), String> {
        Err(format!(
            "{} backend does not support Iceberg partition evolution DDL",
            self.name()
        ))
    }
    fn drop_table(
        &self,
        catalog: &str,
        namespace: &str,
        table: &str,
        if_exists: bool,
    ) -> Result<(), String>;
    fn load_table(
        &self,
        catalog: &str,
        namespace: &str,
        table: &str,
    ) -> Result<ResolvedTable, String>;
}

/// Scan-side metadata conversion used to register external connector tables
/// into the in-memory logical catalog before planning.
pub(crate) trait TableSource: Send + Sync {
    fn name(&self) -> &'static str;

    /// Build a `TableDef` suitable for registration in the in-memory logical
    /// catalog. Different backends pick different `TableStorage` variants
    /// (LocalParquetFile / S3ParquetFiles / ManagedLake).
    fn build_table_def(&self, table: &ResolvedTable) -> Result<TableDef, String>;

    /// Phase-1 entry point for time-travel-aware table-def construction.
    /// Default impl ignores the snapshot pin and delegates to `build_table_def`,
    /// which is correct for connectors that do not have time-travel semantics.
    fn build_table_def_at(
        &self,
        table: &ResolvedTable,
        _snapshot_id: Option<i64>,
    ) -> Result<TableDef, String> {
        self.build_table_def(table)
    }
}

/// Write-side: append rows or RecordBatches to a table. The INSERT
/// orchestration layer (`insert_flow.rs`, Phase 3) chooses between the two
/// depending on whether the source is literal VALUES or a pipeline result.
pub(crate) trait TableSink: Send + Sync {
    fn name(&self) -> &'static str;
    fn append_rows(&self, table: &ResolvedTable, rows: &[Vec<Literal>]) -> Result<(), String>;
    fn append_batch(&self, table: &ResolvedTable, batch: RecordBatch) -> Result<(), String>;

    /// Whether this trait path supports INSERT SELECT materialized as a
    /// RecordBatch. FE-driven Iceberg pipeline sinks use
    /// `IcebergTableSinkFactory` directly and do not go through this trait.
    fn supports_pipeline_insert(&self) -> bool;
}

/// Materialized-view backend: CREATE / DROP / REFRESH / SHOW. Today only
/// managed-lake implements this. Future backends (e.g. iceberg-as-MV-target)
/// plug in here.
pub(crate) trait MvBackend: Send + Sync {
    fn name(&self) -> &'static str;
    fn create_mv(
        &self,
        stmt: &CreateMaterializedViewStmt,
        current_catalog: Option<&str>,
        current_database: &str,
    ) -> Result<(), String>;
    fn drop_mv(
        &self,
        stmt: &DropMaterializedViewStmt,
        current_catalog: Option<&str>,
        current_database: &str,
    ) -> Result<(), String>;
    fn refresh_mv(
        &self,
        stmt: &RefreshMaterializedViewStmt,
        current_catalog: Option<&str>,
        current_database: &str,
    ) -> Result<(), String>;
    fn list_mvs(
        &self,
        stmt: &ShowMaterializedViewsStmt,
        current_catalog: Option<&str>,
    ) -> Result<QueryResult, String>;
}
