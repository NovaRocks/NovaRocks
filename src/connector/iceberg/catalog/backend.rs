//! `CatalogBackend` / `TableSource` / `TableSink` implementations for
//! Iceberg, wrapping the free functions in `registry.rs`.

use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use arrow::array::ArrayRef;
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use futures::TryStreamExt;

use crate::connector::backend::{
    CatalogBackend, CreateTableRequest, ResolvedTable, TableSink, TableSource,
};
use crate::connector::iceberg::catalog::{IcebergLoadedTable, build_insert_batch};
use crate::sql::catalog::{ColumnDef, S3FileInfo, TableDef, TableStorage};
use crate::sql::parser::ast::Literal;
use crate::standalone::engine::aggregate::merge_aggregate_table_rows_if_needed;
use crate::standalone::engine::block_on_standalone_async;
use crate::standalone::engine::catalog::normalize_identifier;

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
        load_full_iceberg_batch(&loaded)
    }

    fn build_table_def(&self, table: &ResolvedTable) -> Result<TableDef, String> {
        let guard = self.registry.read().expect("iceberg catalog read lock");
        let entry = guard.get(&table.catalog)?;
        let loaded = reg_load_table(&entry, &table.namespace, &table.table)?;
        let data_files = super::registry::extract_data_files(&loaded.table)?;
        build_iceberg_table_def_with_files(
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
    let storage = if entry.is_s3() {
        let cloud_properties = entry.cloud_properties_map();
        TableStorage::S3ParquetFiles {
            files: data_files
                .into_iter()
                .map(|(path, size, row_count)| S3FileInfo {
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
        TableStorage::LocalParquetFile {
            path: std::path::PathBuf::from(local_path),
        }
    } else {
        register_empty_iceberg_table(namespace, table_name, &loaded.columns)?
    };

    Ok(TableDef {
        name: table_name.to_string(),
        columns: loaded.columns,
        storage,
    })
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
    let Some(merged_rows) = merge_aggregate_table_rows_if_needed(
        &loaded.columns,
        loaded.key_desc.as_ref(),
        &loaded.column_aggregations,
        &batch,
    )?
    else {
        return Ok(batch);
    };
    build_insert_batch(loaded, &merged_rows)
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
