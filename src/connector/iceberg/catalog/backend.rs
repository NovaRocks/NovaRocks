//! `CatalogBackend` / `TableSource` / `TableSink` implementations for
//! Iceberg, wrapping the free functions in `registry.rs`.

use std::sync::{Arc, RwLock};

use arrow::record_batch::RecordBatch;

use crate::connector::backend::{
    CatalogBackend, CreateTableRequest, ResolvedTable, TableSink, TableSource,
};
use crate::sql::catalog::TableDef;
use crate::sql::parser::ast::Literal;

use super::registry::{
    IcebergCatalogEntry, IcebergCatalogRegistry, create_namespace as reg_create_namespace,
    create_table as reg_create_table, drop_namespace as reg_drop_namespace,
    drop_table as reg_drop_table, insert_rows as reg_insert_rows, list_tables as reg_list_tables,
    load_table as reg_load_table, namespace_exists as reg_namespace_exists,
};

pub(crate) struct IcebergCatalogBackend {
    registry: Arc<RwLock<IcebergCatalogRegistry>>,
}

impl IcebergCatalogBackend {
    pub(crate) fn new(registry: Arc<RwLock<IcebergCatalogRegistry>>) -> Self {
        Self { registry }
    }

    fn entry(&self, catalog: &str) -> Result<IcebergCatalogEntry, String> {
        let guard = self.registry.read().expect("iceberg catalog read lock");
        guard.get(catalog)
    }
}

impl CatalogBackend for IcebergCatalogBackend {
    fn name(&self) -> &'static str {
        "iceberg"
    }

    fn namespace_exists(&self, catalog: &str, namespace: &str) -> Result<bool, String> {
        reg_namespace_exists(&self.entry(catalog)?, namespace)
    }

    fn create_namespace(&self, catalog: &str, namespace: &str) -> Result<(), String> {
        reg_create_namespace(&self.entry(catalog)?, namespace)
    }

    fn drop_namespace(&self, catalog: &str, namespace: &str, force: bool) -> Result<(), String> {
        let entry = self.entry(catalog)?;
        if force {
            for table in reg_list_tables(&entry, namespace)? {
                reg_drop_table(&entry, namespace, &table)?;
            }
        }
        reg_drop_namespace(&entry, namespace)
    }

    fn create_table(&self, req: CreateTableRequest) -> Result<(), String> {
        let entry = self.entry(&req.catalog)?;
        reg_create_table(
            &entry,
            &req.namespace,
            &req.table,
            &req.columns,
            req.key_desc.as_ref(),
            &req.properties,
        )
    }

    fn drop_table(
        &self,
        catalog: &str,
        namespace: &str,
        table: &str,
        _if_exists: bool,
    ) -> Result<(), String> {
        reg_drop_table(&self.entry(catalog)?, namespace, table)
    }

    fn load_table(
        &self,
        catalog: &str,
        namespace: &str,
        table: &str,
    ) -> Result<ResolvedTable, String> {
        let loaded = reg_load_table(&self.entry(catalog)?, namespace, table)?;
        Ok(ResolvedTable {
            catalog: catalog.to_string(),
            namespace: namespace.to_string(),
            table: table.to_string(),
            columns: loaded.columns,
            logical_types: loaded.logical_types,
            key_desc: loaded.key_desc,
        })
    }

    fn list_tables(&self, catalog: &str, namespace: &str) -> Result<Vec<String>, String> {
        reg_list_tables(&self.entry(catalog)?, namespace)
    }
}

pub(crate) struct IcebergTableSource {
    registry: Arc<RwLock<IcebergCatalogRegistry>>,
}

impl IcebergTableSource {
    pub(crate) fn new(registry: Arc<RwLock<IcebergCatalogRegistry>>) -> Self {
        Self { registry }
    }
}

impl TableSource for IcebergTableSource {
    fn name(&self) -> &'static str {
        "iceberg"
    }

    fn load_full(&self, table: &ResolvedTable) -> Result<RecordBatch, String> {
        let guard = self.registry.read().expect("iceberg catalog read lock");
        let entry = guard.get(&table.catalog)?;
        let loaded = reg_load_table(&entry, &table.namespace, &table.table)?;
        crate::standalone::engine::iceberg_glue::load_full_iceberg_batch(&loaded)
    }

    fn build_table_def(&self, table: &ResolvedTable) -> Result<TableDef, String> {
        let guard = self.registry.read().expect("iceberg catalog read lock");
        let entry = guard.get(&table.catalog)?;
        let loaded = reg_load_table(&entry, &table.namespace, &table.table)?;
        let data_files = super::registry::extract_data_files(&loaded.table)?;
        crate::standalone::engine::build_iceberg_table_def_with_files_public(
            &entry,
            &table.namespace,
            &table.table,
            loaded,
            data_files,
        )
    }
}

pub(crate) struct IcebergTableSink {
    registry: Arc<RwLock<IcebergCatalogRegistry>>,
}

impl IcebergTableSink {
    pub(crate) fn new(registry: Arc<RwLock<IcebergCatalogRegistry>>) -> Self {
        Self { registry }
    }
}

impl TableSink for IcebergTableSink {
    fn name(&self) -> &'static str {
        "iceberg"
    }

    fn append_rows(&self, table: &ResolvedTable, rows: &[Vec<Literal>]) -> Result<(), String> {
        let guard = self.registry.read().expect("iceberg catalog read lock");
        let entry = guard.get(&table.catalog)?;
        reg_insert_rows(&entry, &table.namespace, &table.table, rows)
    }

    fn append_batch(&self, _table: &ResolvedTable, _batch: RecordBatch) -> Result<(), String> {
        Err(
            "iceberg append_batch uses IcebergTableSinkFactory through the execution layer"
                .to_string(),
        )
    }

    fn supports_pipeline_insert(&self) -> bool {
        true
    }
}
