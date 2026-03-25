use std::collections::HashMap;
use std::future::Future;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use arrow::array::{
    ArrayRef, BooleanArray, Date32Array, Float32Array, Float64Array, Int32Array, Int64Array,
    StringArray, Time64MicrosecondArray, TimestampMicrosecondArray,
};
use arrow::datatypes::{DataType, TimeUnit};
use arrow::record_batch::RecordBatch;
use chrono::{NaiveDate, NaiveDateTime, NaiveTime, Timelike};
use iceberg::arrow::schema_to_arrow_schema;
use iceberg::io::LocalFsStorageFactory;
use iceberg::memory::{MEMORY_CATALOG_WAREHOUSE, MemoryCatalog, MemoryCatalogBuilder};
use iceberg::spec::{NestedField, PrimitiveType, Schema, Type};
use iceberg::transaction::{ApplyTransactionAction, Transaction};
use iceberg::writer::base_writer::data_file_writer::DataFileWriterBuilder;
use iceberg::writer::file_writer::ParquetWriterBuilder;
use iceberg::writer::file_writer::location_generator::{
    DefaultFileNameGenerator, DefaultLocationGenerator,
};
use iceberg::writer::file_writer::rolling_writer::RollingFileWriterBuilder;
use iceberg::{
    Catalog, CatalogBuilder, NamespaceIdent, TableCreation, TableIdent,
    writer::{IcebergWriter, IcebergWriterBuilder},
};
use parquet::file::properties::WriterProperties;
use tokio::runtime::Handle;

use crate::runtime::global_async_runtime::data_block_on;

use super::catalog::{ColumnDef, normalize_identifier};
use crate::sql::{Literal, SqlType, TableColumnDef};

#[derive(Default)]
pub(crate) struct IcebergCatalogRegistry {
    catalogs: HashMap<String, IcebergCatalogEntry>,
}

#[derive(Clone, Debug)]
pub(crate) struct IcebergCatalogEntry {
    catalog: Arc<MemoryCatalog>,
    warehouse_path: PathBuf,
    properties: Vec<(String, String)>,
}

#[derive(Clone, Debug)]
pub(crate) struct IcebergLoadedTable {
    pub table: iceberg::table::Table,
    pub columns: Vec<ColumnDef>,
}

impl IcebergCatalogRegistry {
    pub(crate) fn create_catalog(
        &mut self,
        catalog_name: &str,
        properties: &[(String, String)],
    ) -> Result<(), String> {
        let key = normalize_identifier(catalog_name)?;
        if self.catalogs.contains_key(&key) {
            return Err(format!("catalog already exists: {catalog_name}"));
        }
        let entry = build_memory_catalog(catalog_name, properties)?;
        self.catalogs.insert(key, entry);
        Ok(())
    }

    pub(crate) fn get(&self, catalog_name: &str) -> Result<IcebergCatalogEntry, String> {
        let key = normalize_identifier(catalog_name)?;
        self.catalogs
            .get(&key)
            .cloned()
            .ok_or_else(|| format!("unknown catalog: {catalog_name}"))
    }

    pub(crate) fn contains_catalog(&self, catalog_name: &str) -> Result<bool, String> {
        let key = normalize_identifier(catalog_name)?;
        Ok(self.catalogs.contains_key(&key))
    }

    pub(crate) fn drop_catalog(&mut self, catalog_name: &str) -> Result<(), String> {
        let key = normalize_identifier(catalog_name)?;
        self.catalogs
            .remove(&key)
            .map(|_| ())
            .ok_or_else(|| format!("unknown catalog: {catalog_name}"))
    }
}

impl IcebergCatalogEntry {
    pub(crate) fn properties(&self) -> &[(String, String)] {
        &self.properties
    }
}

pub(crate) fn create_namespace(
    entry: &IcebergCatalogEntry,
    namespace_name: &str,
) -> Result<(), String> {
    let namespace = NamespaceIdent::new(normalize_identifier(namespace_name)?);
    block_on_iceberg(async {
        entry
            .catalog
            .create_namespace(&namespace, HashMap::new())
            .await
    })
    .map_err(|e| format!("create iceberg namespace runtime failed: {e}"))?
    .map(|_| ())
    .map_err(|e| format!("create iceberg namespace failed: {e}"))
}

pub(crate) fn namespace_exists(
    entry: &IcebergCatalogEntry,
    namespace_name: &str,
) -> Result<bool, String> {
    let namespace = NamespaceIdent::new(normalize_identifier(namespace_name)?);
    block_on_iceberg(async { entry.catalog.namespace_exists(&namespace).await })
        .map_err(|e| format!("check iceberg namespace runtime failed: {e}"))?
        .map_err(|e| format!("check iceberg namespace failed: {e}"))
}

pub(crate) fn drop_namespace(
    entry: &IcebergCatalogEntry,
    namespace_name: &str,
) -> Result<(), String> {
    let namespace = NamespaceIdent::new(normalize_identifier(namespace_name)?);
    block_on_iceberg(async { entry.catalog.drop_namespace(&namespace).await })
        .map_err(|e| format!("drop iceberg namespace runtime failed: {e}"))?
        .map_err(|e| format!("drop iceberg namespace failed: {e}"))
}

pub(crate) fn list_tables(
    entry: &IcebergCatalogEntry,
    namespace_name: &str,
) -> Result<Vec<String>, String> {
    let namespace = NamespaceIdent::new(normalize_identifier(namespace_name)?);
    block_on_iceberg(async { entry.catalog.list_tables(&namespace).await })
        .map_err(|e| format!("list iceberg tables runtime failed: {e}"))?
        .map(|tables| {
            tables
                .into_iter()
                .map(|table| table.name().to_string())
                .collect()
        })
        .map_err(|e| format!("list iceberg tables failed: {e}"))
}

pub(crate) fn create_table(
    entry: &IcebergCatalogEntry,
    namespace_name: &str,
    table_name: &str,
    columns: &[TableColumnDef],
    properties: &[(String, String)],
) -> Result<(), String> {
    let namespace = NamespaceIdent::new(normalize_identifier(namespace_name)?);
    let table_name = normalize_identifier(table_name)?;
    let schema = build_iceberg_schema(columns)?;
    let table_creation = TableCreation::builder()
        .name(table_name)
        .schema(schema)
        .properties(properties.iter().cloned())
        .build();
    block_on_iceberg(async { entry.catalog.create_table(&namespace, table_creation).await })
        .map_err(|e| format!("create iceberg table runtime failed: {e}"))?
        .map(|_| ())
        .map_err(|e| format!("create iceberg table failed: {e}"))
}

pub(crate) fn drop_table(
    entry: &IcebergCatalogEntry,
    namespace_name: &str,
    table_name: &str,
) -> Result<(), String> {
    let table_ident = TableIdent::from_strs([
        normalize_identifier(namespace_name)?,
        normalize_identifier(table_name)?,
    ])
    .map_err(|e| format!("build iceberg table ident failed: {e}"))?;
    block_on_iceberg(async { entry.catalog.drop_table(&table_ident).await })
        .map_err(|e| format!("drop iceberg table runtime failed: {e}"))?
        .map_err(|e| format!("drop iceberg table failed: {e}"))
}

pub(crate) fn load_table(
    entry: &IcebergCatalogEntry,
    namespace_name: &str,
    table_name: &str,
) -> Result<IcebergLoadedTable, String> {
    let table_ident = TableIdent::from_strs([
        normalize_identifier(namespace_name)?,
        normalize_identifier(table_name)?,
    ])
    .map_err(|e| format!("build iceberg table ident failed: {e}"))?;
    let table = block_on_iceberg(async { entry.catalog.load_table(&table_ident).await })
        .map_err(|e| format!("load iceberg table runtime failed: {e}"))?
        .map_err(|e| format!("load iceberg table failed: {e}"))?;
    let arrow_schema = schema_to_arrow_schema(table.metadata().current_schema())
        .map_err(|e| format!("convert iceberg schema to arrow schema failed: {e}"))?;
    let columns = arrow_schema
        .fields()
        .iter()
        .map(|field| ColumnDef {
            name: field.name().clone(),
            data_type: field.data_type().clone(),
            nullable: field.is_nullable(),
        })
        .collect();
    Ok(IcebergLoadedTable { table, columns })
}

pub(crate) fn insert_rows(
    entry: &IcebergCatalogEntry,
    namespace_name: &str,
    table_name: &str,
    rows: &[Vec<Literal>],
) -> Result<(), String> {
    let loaded = load_table(entry, namespace_name, table_name)?;
    let batch = build_insert_batch(&loaded.table, rows)?;
    block_on_iceberg(async {
        let location_generator = DefaultLocationGenerator::new(loaded.table.metadata().clone())
            .map_err(|e| iceberg::Error::new(iceberg::ErrorKind::DataInvalid, e.to_string()))?;
        let file_name_generator = DefaultFileNameGenerator::new(
            "standalone".to_string(),
            None,
            iceberg::spec::DataFileFormat::Parquet,
        );
        let parquet_writer_builder = ParquetWriterBuilder::new(
            WriterProperties::default(),
            loaded.table.metadata().current_schema().clone(),
        );
        let rolling_writer_builder = RollingFileWriterBuilder::new_with_default_file_size(
            parquet_writer_builder,
            loaded.table.file_io().clone(),
            location_generator,
            file_name_generator,
        );
        let mut data_file_writer = DataFileWriterBuilder::new(rolling_writer_builder)
            .build(None)
            .await?;
        data_file_writer.write(batch).await?;
        let data_files = data_file_writer.close().await?;
        let tx = Transaction::new(&loaded.table);
        let tx = tx.fast_append().add_data_files(data_files).apply(tx)?;
        tx.commit(entry.catalog.as_ref()).await.map(|_| ())
    })
    .map_err(|e| format!("insert iceberg rows runtime failed: {e}"))?
    .map_err(|e| format!("insert iceberg rows failed: {e}"))
}

pub(crate) fn register_existing_table(
    entry: &IcebergCatalogEntry,
    namespace_name: &str,
    table_name: &str,
) -> Result<(), String> {
    let table_ident = TableIdent::from_strs([
        normalize_identifier(namespace_name)?,
        normalize_identifier(table_name)?,
    ])
    .map_err(|e| format!("build iceberg table ident failed: {e}"))?;
    let metadata_location = latest_table_metadata_location(entry, namespace_name, table_name)?;
    block_on_iceberg(async {
        entry
            .catalog
            .register_table(&table_ident, metadata_location)
            .await
    })
    .map_err(|e| format!("register iceberg table runtime failed: {e}"))?
    .map(|_| ())
    .map_err(|e| format!("register iceberg table failed: {e}"))
}

fn build_memory_catalog(
    catalog_name: &str,
    properties: &[(String, String)],
) -> Result<IcebergCatalogEntry, String> {
    let mut props = HashMap::new();
    for (key, value) in properties {
        props.insert(key.to_ascii_lowercase(), value.clone());
    }
    if let Some(kind) = props.get("type")
        && !kind.eq_ignore_ascii_case("iceberg")
    {
        return Err(format!(
            "standalone iceberg catalog only supports type=iceberg, got {kind}"
        ));
    }
    if let Some(catalog_type) = props.get("iceberg.catalog.type")
        && !catalog_type.eq_ignore_ascii_case("memory")
    {
        return Err(format!(
            "standalone iceberg catalog only supports iceberg.catalog.type=memory, got {catalog_type}"
        ));
    }

    let raw_warehouse = props
        .get("iceberg.catalog.warehouse")
        .or_else(|| props.get("warehouse"))
        .cloned()
        .ok_or_else(|| {
            "standalone iceberg catalog requires `iceberg.catalog.warehouse`".to_string()
        })?;
    let (warehouse_uri, warehouse_path) = normalize_warehouse_location(&raw_warehouse)?;
    std::fs::create_dir_all(&warehouse_path).map_err(|e| {
        format!(
            "create iceberg warehouse directory {} failed: {e}",
            warehouse_path.display()
        )
    })?;

    props.insert("type".to_string(), "iceberg".to_string());
    props.insert("iceberg.catalog.type".to_string(), "memory".to_string());
    props.insert(
        "iceberg.catalog.warehouse".to_string(),
        warehouse_uri.clone(),
    );

    let catalog = block_on_iceberg(
        MemoryCatalogBuilder::default()
            .with_storage_factory(Arc::new(LocalFsStorageFactory))
            .load(
                catalog_name.to_string(),
                HashMap::from([(MEMORY_CATALOG_WAREHOUSE.to_string(), warehouse_uri.clone())]),
            ),
    )
    .map_err(|e| format!("create iceberg catalog runtime failed: {e}"))?
    .map_err(|e| format!("create iceberg catalog failed: {e}"))?;

    Ok(IcebergCatalogEntry {
        catalog: Arc::new(catalog),
        warehouse_path,
        properties: sorted_properties(&props),
    })
}

fn block_on_iceberg<F>(future: F) -> Result<F::Output, String>
where
    F: Future,
{
    if let Ok(handle) = Handle::try_current() {
        return Ok(handle.block_on(future));
    }
    data_block_on(future)
}

fn normalize_warehouse_location(raw: &str) -> Result<(String, PathBuf), String> {
    if let Some(stripped) = raw.strip_prefix("file://") {
        let path = PathBuf::from(stripped);
        let path = canonicalize_or_join(&path)?;
        return Ok((format!("file://{}", path.display()), path));
    }

    let path = PathBuf::from(raw);
    let path = canonicalize_or_join(&path)?;
    Ok((format!("file://{}", path.display()), path))
}

fn latest_table_metadata_location(
    entry: &IcebergCatalogEntry,
    namespace_name: &str,
    table_name: &str,
) -> Result<String, String> {
    let metadata_dir = entry
        .warehouse_path
        .join(normalize_identifier(namespace_name)?)
        .join(normalize_identifier(table_name)?)
        .join("metadata");
    let mut candidates = std::fs::read_dir(&metadata_dir)
        .map_err(|e| {
            format!(
                "read iceberg metadata dir {} failed: {e}",
                metadata_dir.display()
            )
        })?
        .filter_map(|item| item.ok())
        .map(|item| item.path())
        .filter(|path| {
            path.file_name()
                .and_then(|name| name.to_str())
                .is_some_and(|name| name.ends_with(".metadata.json"))
        })
        .collect::<Vec<_>>();
    candidates.sort();
    let latest = candidates.last().ok_or_else(|| {
        format!(
            "no iceberg metadata files found under {}",
            metadata_dir.display()
        )
    })?;
    Ok(path_to_file_uri(latest))
}

fn path_to_file_uri(path: &Path) -> String {
    format!("file://{}", path.display())
}

fn sorted_properties(props: &HashMap<String, String>) -> Vec<(String, String)> {
    let mut entries = props
        .iter()
        .map(|(key, value)| (key.clone(), value.clone()))
        .collect::<Vec<_>>();
    entries.sort_by(|left, right| left.0.cmp(&right.0));
    entries
}

fn canonicalize_or_join(path: &Path) -> Result<PathBuf, String> {
    if path.exists() {
        std::fs::canonicalize(path).map_err(|e| format!("canonicalize path failed: {e}"))
    } else if path.is_absolute() {
        Ok(path.to_path_buf())
    } else {
        std::env::current_dir()
            .map(|cwd| cwd.join(path))
            .map_err(|e| format!("read current directory failed: {e}"))
    }
}

fn build_iceberg_schema(columns: &[TableColumnDef]) -> Result<Schema, String> {
    let fields = columns
        .iter()
        .enumerate()
        .map(|(idx, column)| {
            let field_id =
                i32::try_from(idx + 1).map_err(|_| "too many iceberg columns".to_string())?;
            Ok(NestedField::optional(
                field_id,
                &column.name,
                iceberg_type_for_sql_type(column.data_type),
            )
            .into())
        })
        .collect::<Result<Vec<_>, String>>()?;
    Schema::builder()
        .with_fields(fields)
        .build()
        .map_err(|e| format!("build iceberg schema failed: {e}"))
}

fn iceberg_type_for_sql_type(data_type: SqlType) -> Type {
    Type::Primitive(match data_type {
        SqlType::TinyInt | SqlType::SmallInt | SqlType::Int => PrimitiveType::Int,
        SqlType::Float => PrimitiveType::Float,
        SqlType::Double => PrimitiveType::Double,
        SqlType::BigInt => PrimitiveType::Long,
        SqlType::String => PrimitiveType::String,
        SqlType::Boolean => PrimitiveType::Boolean,
        SqlType::Date => PrimitiveType::Date,
        SqlType::DateTime => PrimitiveType::Timestamp,
        SqlType::Time => PrimitiveType::Time,
    })
}

fn build_insert_batch(
    table: &iceberg::table::Table,
    rows: &[Vec<Literal>],
) -> Result<RecordBatch, String> {
    let arrow_schema = Arc::new(
        schema_to_arrow_schema(table.metadata().current_schema())
            .map_err(|e| format!("convert iceberg schema to arrow schema failed: {e}"))?,
    );
    let fields = arrow_schema.fields();
    for row in rows {
        if row.len() != fields.len() {
            return Err(format!(
                "insert column count mismatch: expected {} values, got {}",
                fields.len(),
                row.len()
            ));
        }
    }

    let mut arrays = Vec::with_capacity(fields.len());
    for (idx, field) in fields.iter().enumerate() {
        let values = rows.iter().map(|row| &row[idx]).collect::<Vec<_>>();
        arrays.push(build_literal_array(field.data_type(), &values)?);
    }
    RecordBatch::try_new(arrow_schema, arrays)
        .map_err(|e| format!("build iceberg insert batch failed: {e}"))
}

fn build_literal_array(data_type: &DataType, values: &[&Literal]) -> Result<ArrayRef, String> {
    match data_type {
        DataType::Int32 => Ok(Arc::new(Int32Array::from(
            values
                .iter()
                .map(|literal| match literal {
                    Literal::Null => Ok(None),
                    Literal::Int(value) => i32::try_from(*value)
                        .map(Some)
                        .map_err(|_| format!("literal {value} is out of range for INT")),
                    other => Err(format!("literal {:?} is not valid for INT", other)),
                })
                .collect::<Result<Vec<_>, _>>()?,
        ))),
        DataType::Int64 => Ok(Arc::new(Int64Array::from(
            values
                .iter()
                .map(|literal| match literal {
                    Literal::Null => Ok(None),
                    Literal::Int(value) => Ok(Some(*value)),
                    other => Err(format!("literal {:?} is not valid for BIGINT", other)),
                })
                .collect::<Result<Vec<_>, _>>()?,
        ))),
        DataType::Float32 => Ok(Arc::new(Float32Array::from(
            values
                .iter()
                .map(|literal| match literal {
                    Literal::Null => Ok(None),
                    Literal::Float(value) => Ok(Some(*value as f32)),
                    Literal::Int(value) => Ok(Some(*value as f32)),
                    other => Err(format!("literal {:?} is not valid for FLOAT", other)),
                })
                .collect::<Result<Vec<_>, _>>()?,
        ))),
        DataType::Float64 => Ok(Arc::new(Float64Array::from(
            values
                .iter()
                .map(|literal| match literal {
                    Literal::Null => Ok(None),
                    Literal::Float(value) => Ok(Some(*value)),
                    Literal::Int(value) => Ok(Some(*value as f64)),
                    other => Err(format!("literal {:?} is not valid for DOUBLE", other)),
                })
                .collect::<Result<Vec<_>, _>>()?,
        ))),
        DataType::Utf8 => Ok(Arc::new(StringArray::from(
            values
                .iter()
                .map(|literal| match literal {
                    Literal::Null => Ok(None),
                    Literal::String(value) => Ok(Some(value.as_str())),
                    other => Err(format!("literal {:?} is not valid for STRING", other)),
                })
                .collect::<Result<Vec<_>, _>>()?,
        ))),
        DataType::Boolean => Ok(Arc::new(BooleanArray::from(
            values
                .iter()
                .map(|literal| match literal {
                    Literal::Null => Ok(None),
                    Literal::Bool(value) => Ok(Some(*value)),
                    other => Err(format!("literal {:?} is not valid for BOOLEAN", other)),
                })
                .collect::<Result<Vec<_>, _>>()?,
        ))),
        DataType::Date32 => Ok(Arc::new(Date32Array::from(
            values
                .iter()
                .map(|literal| match literal {
                    Literal::Null => Ok(None),
                    Literal::Date(value) | Literal::String(value) => {
                        parse_date_literal_to_days(value).map(Some)
                    }
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
                        Literal::String(value) => {
                            parse_timestamp_literal_to_micros(value).map(Some)
                        }
                        other => Err(format!("literal {:?} is not valid for DATETIME", other)),
                    })
                    .collect::<Result<Vec<_>, _>>()?,
            )))
        }
        DataType::Time64(TimeUnit::Microsecond) => Ok(Arc::new(Time64MicrosecondArray::from(
            values
                .iter()
                .map(|literal| match literal {
                    Literal::Null => Ok(None),
                    Literal::String(value) => parse_time_literal_to_micros(value).map(Some),
                    other => Err(format!("literal {:?} is not valid for TIME", other)),
                })
                .collect::<Result<Vec<_>, _>>()?,
        ))),
        other => Err(format!(
            "standalone iceberg insert does not support column type {:?}",
            other
        )),
    }
}

fn parse_date_literal_to_days(value: &str) -> Result<i32, String> {
    let date = NaiveDate::parse_from_str(value, "%Y-%m-%d")
        .map_err(|e| format!("parse DATE literal `{value}` failed: {e}"))?;
    let epoch = NaiveDate::from_ymd_opt(1970, 1, 1).expect("epoch");
    i32::try_from(date.signed_duration_since(epoch).num_days())
        .map_err(|_| format!("DATE literal `{value}` is out of range"))
}

fn parse_time_literal_to_micros(value: &str) -> Result<i64, String> {
    let time = NaiveTime::parse_from_str(value, "%H:%M:%S%.f")
        .or_else(|_| NaiveTime::parse_from_str(value, "%H:%M:%S"))
        .map_err(|e| format!("parse TIME literal `{value}` failed: {e}"))?;
    i64::try_from(time.num_seconds_from_midnight())
        .map(|seconds| seconds * 1_000_000 + i64::from(time.nanosecond() / 1_000))
        .map_err(|_| format!("TIME literal `{value}` is out of range"))
}

fn parse_timestamp_literal_to_micros(value: &str) -> Result<i64, String> {
    let timestamp = NaiveDateTime::parse_from_str(value, "%Y-%m-%d %H:%M:%S%.f")
        .or_else(|_| NaiveDateTime::parse_from_str(value, "%Y-%m-%d %H:%M:%S"))
        .map_err(|e| format!("parse DATETIME literal `{value}` failed: {e}"))?;
    let epoch = NaiveDate::from_ymd_opt(1970, 1, 1)
        .expect("epoch")
        .and_hms_opt(0, 0, 0)
        .expect("epoch timestamp");
    timestamp
        .signed_duration_since(epoch)
        .num_microseconds()
        .ok_or_else(|| format!("DATETIME literal `{value}` is out of range"))
}
