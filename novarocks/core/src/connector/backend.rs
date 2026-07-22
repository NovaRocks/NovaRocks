// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//! Connector-agnostic backend traits. Each trait represents one axis of
//! capability (catalog admin, table scan-side source, table write-side sink,
//! materialized-view lifecycle). A connector implements whichever subset
//! applies to it.
//!
//! The traits live here rather than in each per-connector mod.rs so callers
//! can program against `dyn CatalogBackend` without knowing which concrete
//! connector fulfils the request.

use std::collections::HashMap;
use std::sync::Arc;

use arrow::record_batch::RecordBatch;

use crate::connector::stats::TableStatsProvider;
use crate::engine::mv::lifecycle::{
    CreateMvRequest, DropMvRequest, ListMvsRequest, MvListRow, RefreshCtx, RefreshError,
    RefreshOutcome, RefreshPlan, RefreshRequest,
};
use crate::sql::parser::ast::{
    AlterIcebergPartitionSpecStmt, IcebergPartitionFieldExpr, Literal, TableColumnDef, TableKeyDesc,
};
use crate::sql::planner::table::TableDef;
use novarocks_catalog::schema::ColumnDef;

/// Request to create a table. Unified shape across all catalog backends;
/// backends ignore fields that don't apply to them (e.g. `bucket_count` is
/// StarRocks table-only).
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

/// Create-view request routed to a catalog backend.
#[derive(Clone, Debug)]
pub(crate) struct CreateViewRequest {
    pub catalog: String,
    pub namespace: String,
    pub view: String,
    pub columns: Vec<TableColumnDef>,
    /// The view body as SQL text (StarRocks dialect).
    pub view_sql: String,
    pub comment: Option<String>,
    pub or_replace: bool,
    /// Extra view-metadata properties. Empty for plain user CREATE VIEW.
    pub properties: Vec<(String, String)>,
}

/// A view loaded through a catalog backend.
#[derive(Clone, Debug)]
pub(crate) struct ResolvedView {
    pub sql: String,
    pub dialect: String,
    pub default_namespace: String,
    pub column_names: Vec<String>,
    pub comment: Option<String>,
    pub properties: HashMap<String, String>,
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
/// tables. Implemented once per catalog type (iceberg, StarRocks table, ...).
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

    /// Resolve a table for read/query planning. Backends may expose synthetic
    /// read aliases here without changing strict catalog-plane `load_table`
    /// semantics used by DDL and write paths.
    fn load_table_for_read(
        &self,
        catalog: &str,
        namespace: &str,
        table: &str,
    ) -> Result<ResolvedTable, String> {
        self.load_table(catalog, namespace, table)
    }

    fn current_schema_id(
        &self,
        _catalog: &str,
        _namespace: &str,
        _table: &str,
    ) -> Result<Option<i32>, String> {
        Ok(None)
    }

    /// Resolve the physical table name and schema id for read/query planning.
    /// The returned table name may differ from the requested name for validated
    /// read aliases, while strict catalog-plane operations keep using
    /// `current_schema_id`.
    fn current_schema_id_for_read(
        &self,
        catalog: &str,
        namespace: &str,
        table: &str,
    ) -> Result<(String, Option<i32>), String> {
        Ok((
            table.to_string(),
            self.current_schema_id(catalog, namespace, table)?,
        ))
    }

    fn create_view(&self, _req: CreateViewRequest) -> Result<(), String> {
        Err(format!("{} backend does not support views", self.name()))
    }

    fn drop_view(&self, _catalog: &str, _namespace: &str, _view: &str) -> Result<(), String> {
        Err(format!("{} backend does not support views", self.name()))
    }

    fn load_view(
        &self,
        _catalog: &str,
        _namespace: &str,
        _view: &str,
    ) -> Result<ResolvedView, String> {
        Err(format!("{} backend does not support views", self.name()))
    }

    /// Whether a view with this name exists. Backends without view support
    /// report `false` so strict DROP-type checks degrade gracefully.
    fn view_exists(&self, _catalog: &str, _namespace: &str, _view: &str) -> Result<bool, String> {
        Ok(false)
    }

    fn list_views(&self, _catalog: &str, _namespace: &str) -> Result<Vec<String>, String> {
        Err(format!("{} backend does not support views", self.name()))
    }
}

/// Scan-side metadata conversion used to register external connector tables
/// into the in-memory logical catalog before planning.
pub(crate) trait TableSource: Send + Sync {
    fn name(&self) -> &'static str;

    /// Build a `TableDef` suitable for registration in the in-memory logical
    /// catalog. Different backends pick different `ScanSource` variants
    /// (IcebergDataFiles / IcebergMetadataTable / IcebergDeltaTable).
    fn build_table_def(&self, table: &ResolvedTable) -> Result<TableDef, String>;

    /// Build a schema-only `TableDef` for catalog registration. The default
    /// preserves existing connector behavior. Iceberg overrides this to avoid
    /// expanding snapshot data files during query-prep registration.
    fn build_schema_table_def(&self, table: &ResolvedTable) -> Result<TableDef, String> {
        self.build_table_def(table)
    }

    /// Build a schema-only `TableDef` that additionally carries the resolved
    /// `$files` / `$manifests` / `$entries` metadata rows. The default
    /// preserves existing connector behavior (no metadata-row materialisation);
    /// iceberg overrides this to walk the current snapshot's manifests.
    fn build_metadata_rows_table_def(
        &self,
        resolved: &ResolvedTable,
        metadata_table_type: crate::connector::iceberg::IcebergMetadataTableType,
    ) -> Result<crate::sql::planner::table::TableDef, String> {
        let _ = metadata_table_type;
        self.build_schema_table_def(resolved)
    }

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

    #[allow(dead_code)] // Task 5 wires QueryStatsCollector to this staged provider hook.
    fn stats_provider(&self) -> Option<Arc<dyn TableStatsProvider>> {
        None
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
/// StarRocks table implements this. Future backends (e.g. iceberg-as-MV-target)
/// plug in here.
pub(crate) trait MvBackend: Send + Sync {
    fn name(&self) -> &'static str;

    fn create_mv(&self, req: CreateMvRequest) -> Result<(), String>;
    fn drop_mv(&self, req: DropMvRequest) -> Result<(), String>;
    fn list_mvs(&self, req: ListMvsRequest) -> Result<Vec<MvListRow>, String>;

    fn plan_refresh(&self, req: RefreshRequest) -> Result<RefreshPlan, RefreshError>;
    fn execute_refresh(
        &self,
        plan: &RefreshPlan,
        ctx: &mut RefreshCtx,
    ) -> Result<RefreshOutcome, RefreshError>;
    fn commit_refresh(
        &self,
        outcome: &RefreshOutcome,
        ctx: &mut RefreshCtx,
    ) -> Result<(), RefreshError>;
    fn rollback_refresh(
        &self,
        outcome: Option<&RefreshOutcome>,
        ctx: &mut RefreshCtx,
    ) -> Result<(), RefreshError>;
}
