//! `CatalogBackend` / `TableSource` / `TableSink` implementations for
//! Iceberg, wrapping the free functions in `registry.rs`.

use std::sync::{Arc, RwLock};

use arrow::array::ArrayRef;
use arrow::datatypes::{Field, Schema};
use arrow::record_batch::RecordBatch;

use crate::connector::backend::{
    CatalogBackend, CreateTableRequest, ResolvedTable, TableSink, TableSource,
};
use crate::connector::iceberg::catalog::IcebergLoadedTable;
use crate::sql::catalog::{ColumnDef, S3FileInfo, TableDef, TableStorage};
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
        })
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

    fn build_table_def(&self, table: &ResolvedTable) -> Result<TableDef, String> {
        let guard = self.registry.read().expect("iceberg catalog read lock");
        let entry = guard.get(&table.catalog)?;
        let loaded = reg_load_table(&entry, &table.namespace, &table.table)?;
        let data_files = super::registry::extract_data_files_with_stats(&loaded.table)?;
        build_iceberg_table_def_with_data_files(
            &entry,
            &table.namespace,
            &table.table,
            loaded,
            data_files,
        )
    }
}

pub(crate) fn build_iceberg_table_def_with_files(
    entry: &IcebergCatalogEntry,
    namespace: &str,
    table_name: &str,
    loaded: IcebergLoadedTable,
    data_files: Vec<(String, i64, Option<i64>)>,
) -> Result<TableDef, String> {
    let data_files = data_files
        .into_iter()
        .map(
            |(path, size, record_count)| super::registry::DataFileWithStats {
                path,
                size,
                record_count,
                column_stats: None,
                first_row_id: None,
                // data_sequence_number is not available from the caller-supplied
                // (path, size, record_count) tuple; callers that need it should
                // use extract_data_files_with_stats instead.
                data_sequence_number: None,
                delete_files: vec![],
            },
        )
        .collect::<Vec<_>>();
    build_iceberg_table_def_with_data_files(entry, namespace, table_name, loaded, data_files)
}

fn build_iceberg_table_def_with_data_files(
    entry: &IcebergCatalogEntry,
    namespace: &str,
    table_name: &str,
    loaded: IcebergLoadedTable,
    data_files: Vec<super::registry::DataFileWithStats>,
) -> Result<TableDef, String> {
    let storage = if entry.is_s3() {
        let cloud_properties = entry.cloud_properties_map();
        TableStorage::S3ParquetFiles {
            files: data_files
                .into_iter()
                .map(|file| S3FileInfo {
                    path: file.path,
                    size: file.size,
                    row_count: file.record_count,
                    column_stats: file.column_stats,
                    first_row_id: file.first_row_id,
                    data_sequence_number: file.data_sequence_number,
                    delete_files: file.delete_files,
                })
                .collect(),
            cloud_properties,
        }
    } else if !data_files.is_empty() {
        // Local Iceberg tables can have multiple data files across snapshots.
        // Keep the per-file lineage metadata by using the multi-file scan
        // shape with empty cloud properties; file:// paths are handled by the
        // local scan path and do not require object-store credentials.
        TableStorage::S3ParquetFiles {
            files: data_files
                .into_iter()
                .map(|file| S3FileInfo {
                    path: file.path,
                    size: file.size,
                    row_count: file.record_count,
                    column_stats: file.column_stats,
                    first_row_id: file.first_row_id,
                    data_sequence_number: file.data_sequence_number,
                    delete_files: file.delete_files,
                })
                .collect(),
            cloud_properties: Default::default(),
        }
    } else {
        register_empty_iceberg_table(namespace, table_name, &loaded.columns)?
    };

    let iceberg_row_lineage_metadata_columns = if is_v3_row_lineage(loaded.table.metadata()) {
        vec![
            ColumnDef {
                name: "_row_id".to_string(),
                data_type: arrow::datatypes::DataType::Int64,
                nullable: false,
            },
            ColumnDef {
                name: "_last_updated_sequence_number".to_string(),
                data_type: arrow::datatypes::DataType::Int64,
                nullable: false,
            },
        ]
    } else {
        vec![]
    };

    Ok(TableDef {
        name: table_name.to_string(),
        columns: loaded.columns,
        iceberg_row_lineage_metadata_columns,
        storage,
    })
}

/// Returns true when the table is Iceberg format-version=3 with
/// `write.row-lineage=true`, meaning per-row `_row_id` and
/// `_last_updated_sequence_number` metadata columns are available.
fn is_v3_row_lineage(metadata: &iceberg::spec::TableMetadata) -> bool {
    let v3 = matches!(metadata.format_version(), iceberg::spec::FormatVersion::V3);
    let lineage = metadata
        .properties()
        .get("write.row-lineage")
        .map(|v| v.eq_ignore_ascii_case("true"))
        .unwrap_or(false);
    v3 && lineage
}

fn register_empty_iceberg_table(
    namespace: &str,
    table_name: &str,
    columns: &[ColumnDef],
) -> Result<TableStorage, String> {
    let dir = std::env::temp_dir().join("novarocks_iceberg_empty");
    std::fs::create_dir_all(&dir).map_err(|e| format!("create empty dir: {e}"))?;
    let path = dir.join(format!("{}_{}.parquet", namespace, table_name));
    let schema = Arc::new(Schema::new(
        columns
            .iter()
            .map(|column| Field::new(&column.name, column.data_type.clone(), column.nullable))
            .collect::<Vec<_>>(),
    ));
    let empty_arrays: Vec<ArrayRef> = schema
        .fields()
        .iter()
        .map(|field| arrow::array::new_empty_array(field.data_type()))
        .collect();
    let empty_batch = RecordBatch::try_new(Arc::clone(&schema), empty_arrays)
        .map_err(|e| format!("build empty batch: {e}"))?;
    let file =
        std::fs::File::create(&path).map_err(|e| format!("create parquet file failed: {e}"))?;
    let mut writer = parquet::arrow::ArrowWriter::try_new(file, schema, None)
        .map_err(|e| format!("create parquet writer failed: {e}"))?;
    writer
        .write(&empty_batch)
        .map_err(|e| format!("write parquet batch failed: {e}"))?;
    writer
        .close()
        .map_err(|e| format!("close parquet writer failed: {e}"))?;
    Ok(TableStorage::LocalParquetFile { path })
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
        false
    }
}
