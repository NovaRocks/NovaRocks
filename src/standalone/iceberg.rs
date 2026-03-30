use std::collections::{BTreeMap, HashMap};
use std::future::Future;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use arrow::array::{
    ArrayRef, BooleanArray, Date32Array, Decimal128Array, Float32Array, Float64Array, Int32Array,
    Int32Builder, Int64Array, Int64Builder, ListBuilder, StringArray, StringBuilder,
    Time64MicrosecondArray, TimestampMicrosecondArray,
};
use arrow::datatypes::{DataType, Field, Schema as ArrowSchema, TimeUnit};
use arrow::record_batch::RecordBatch;
use chrono::{NaiveDate, NaiveDateTime, NaiveTime, Timelike};
use futures::TryStreamExt;
use iceberg::arrow::schema_to_arrow_schema;
use iceberg::io::LocalFsStorageFactory;
use iceberg::memory::{MEMORY_CATALOG_WAREHOUSE, MemoryCatalog, MemoryCatalogBuilder};
use iceberg::spec::{ListType, NestedField, PrimitiveType, Schema, Type};
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
use crate::sql::{ColumnAggregation, Literal, SqlType, TableColumnDef, TableKeyDesc, TableKeyKind};

#[derive(Default)]
pub(crate) struct IcebergCatalogRegistry {
    catalogs: HashMap<String, IcebergCatalogEntry>,
}

#[derive(Clone)]
pub(crate) struct IcebergCatalogEntry {
    pub(crate) name: String,
    pub(crate) warehouse_uri: String,
    pub(crate) properties: Vec<(String, String)>,
    s3_config: Option<crate::fs::object_store::ObjectStoreConfig>,
    pub(crate) warehouse_path: PathBuf,
    table_cache: Arc<std::sync::RwLock<HashMap<(String, String), IcebergLoadedTable>>>,
}

#[derive(Clone, Debug)]
pub(crate) struct IcebergLoadedTable {
    pub table: iceberg::table::Table,
    pub columns: Vec<ColumnDef>,
    pub logical_types: HashMap<String, SqlType>,
    pub key_desc: Option<TableKeyDesc>,
    pub column_aggregations: HashMap<String, ColumnAggregation>,
}

const LOGICAL_TYPE_PROPERTY_PREFIX: &str = "novarocks.logical_type.";
const TABLE_KEY_KIND_PROPERTY: &str = "novarocks.table.key_kind";
const TABLE_KEY_COLUMNS_PROPERTY: &str = "novarocks.table.key_columns";
const COLUMN_AGGREGATION_PROPERTY_PREFIX: &str = "novarocks.column_agg.";

impl IcebergCatalogRegistry {
    pub(crate) fn create_catalog(
        &mut self,
        catalog_name: &str,
        properties: &[(String, String)],
    ) -> Result<(), String> {
        let key = normalize_identifier(catalog_name)?;
        if self.catalogs.contains_key(&key) {
            return Ok(());
        }
        let entry = build_catalog_entry(catalog_name, properties)?;
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

    pub(crate) fn is_s3(&self) -> bool {
        self.s3_config.is_some()
    }

    pub(crate) fn cloud_properties_map(&self) -> BTreeMap<String, String> {
        let mut map = BTreeMap::new();
        for (key, value) in &self.properties {
            match key.as_str() {
                "aws.s3.endpoint"
                | "aws.s3.access_key"
                | "aws.s3.secret_key"
                | "aws.s3.enable_path_style_access"
                | "aws.s3.region" => {
                    map.insert(key.clone(), value.clone());
                }
                _ => {}
            }
        }
        map
    }
}

pub(crate) fn create_namespace(
    entry: &IcebergCatalogEntry,
    namespace_name: &str,
) -> Result<(), String> {
    let _ns_name = normalize_identifier(namespace_name)?;
    // For S3 catalogs, namespace creation is a no-op: the directory structure
    // is created when tables are written. For local catalogs, create the directory.
    if !entry.is_s3() {
        let ns_dir = entry.warehouse_path.join(&_ns_name);
        std::fs::create_dir_all(&ns_dir).map_err(|e| {
            format!(
                "create namespace directory {} failed: {e}",
                ns_dir.display()
            )
        })?;
    }
    Ok(())
}

pub(crate) fn namespace_exists(
    entry: &IcebergCatalogEntry,
    namespace_name: &str,
) -> Result<bool, String> {
    let ns_name = normalize_identifier(namespace_name)?;
    if let Some(s3_config) = &entry.s3_config {
        let op = crate::fs::object_store::build_oss_operator(s3_config)
            .map_err(|e| format!("build S3 operator for namespace check: {e}"))?;
        let (_, root_prefix) = super::iceberg_add_files::parse_s3_path(&entry.warehouse_uri)
            .map_err(|e| format!("parse warehouse URI: {e}"))?;
        let ns_prefix = format!("{}/{}/", root_prefix.trim_end_matches('/'), ns_name);
        block_on_iceberg(async {
            match op.list(&ns_prefix).await {
                Ok(entries) => Ok(!entries.is_empty()),
                Err(_) => Ok(false),
            }
        })
        .map_err(|e| format!("check namespace runtime: {e}"))?
    } else {
        let ns_dir = entry.warehouse_path.join(&ns_name);
        Ok(ns_dir.is_dir())
    }
}

pub(crate) fn drop_namespace(
    entry: &IcebergCatalogEntry,
    namespace_name: &str,
) -> Result<(), String> {
    let ns_name = normalize_identifier(namespace_name)?;
    if entry.is_s3() {
        // For S3 catalogs, dropping a namespace is a no-op
        // (we don't recursively delete S3 directories here)
        Ok(())
    } else {
        let ns_dir = entry.warehouse_path.join(&ns_name);
        if ns_dir.exists() {
            std::fs::remove_dir_all(&ns_dir).map_err(|e| {
                format!("drop namespace directory {} failed: {e}", ns_dir.display())
            })?;
        }
        Ok(())
    }
}

pub(crate) fn list_tables(
    entry: &IcebergCatalogEntry,
    namespace_name: &str,
) -> Result<Vec<String>, String> {
    let ns_name = normalize_identifier(namespace_name)?;
    if let Some(s3_config) = &entry.s3_config {
        let op = crate::fs::object_store::build_oss_operator(s3_config)
            .map_err(|e| format!("build S3 operator for list tables: {e}"))?;
        let (_, root_prefix) = super::iceberg_add_files::parse_s3_path(&entry.warehouse_uri)
            .map_err(|e| format!("parse warehouse URI: {e}"))?;
        let ns_prefix = format!("{}/{}/", root_prefix.trim_end_matches('/'), ns_name);
        block_on_iceberg(async {
            let entries = op
                .list(&ns_prefix)
                .await
                .map_err(|e| format!("list namespace {ns_name}: {e}"))?;
            let mut tables = Vec::new();
            for e in entries {
                if e.metadata().is_dir() {
                    let name = e.name().trim_end_matches('/').to_string();
                    if !name.is_empty() && !name.starts_with('.') {
                        tables.push(name);
                    }
                }
            }
            tables.sort();
            Ok(tables)
        })
        .map_err(|e| format!("list iceberg tables runtime failed: {e}"))?
    } else {
        let ns_dir = entry.warehouse_path.join(&ns_name);
        let entries = std::fs::read_dir(&ns_dir)
            .map_err(|e| format!("read namespace directory {} failed: {e}", ns_dir.display()))?;
        let mut tables = Vec::new();
        for item in entries.flatten() {
            let path = item.path();
            if path.is_dir() {
                let name = item.file_name().to_string_lossy().to_string();
                if !name.starts_with('.') && path.join("metadata").is_dir() {
                    tables.push(name);
                }
            }
        }
        tables.sort();
        Ok(tables)
    }
}

pub(crate) fn create_table(
    entry: &IcebergCatalogEntry,
    namespace_name: &str,
    table_name: &str,
    columns: &[TableColumnDef],
    key_desc: Option<&TableKeyDesc>,
    properties: &[(String, String)],
) -> Result<(), String> {
    let namespace = NamespaceIdent::new(normalize_identifier(namespace_name)?);
    let table_name = normalize_identifier(table_name)?;
    let schema = build_iceberg_schema(columns)?;
    let mut all_properties = properties.to_vec();
    all_properties.extend(build_logical_type_properties(columns)?);
    all_properties.extend(build_table_semantics_properties(columns, key_desc)?);
    let table_creation = TableCreation::builder()
        .name(table_name)
        .schema(schema)
        .properties(all_properties)
        .build();

    // Build a temporary MemoryCatalog for table creation (writes metadata to storage)
    let temp_catalog = build_temp_memory_catalog(entry)?;
    let _ = block_on_iceberg(async {
        temp_catalog
            .create_namespace(&namespace, HashMap::new())
            .await
    });
    block_on_iceberg(async { temp_catalog.create_table(&namespace, table_creation).await })
        .map_err(|e| format!("create iceberg table runtime failed: {e}"))?
        .map(|_| ())
        .map_err(|e| format!("create iceberg table failed: {e}"))
}

pub(crate) fn drop_table(
    entry: &IcebergCatalogEntry,
    namespace_name: &str,
    table_name: &str,
) -> Result<(), String> {
    let ns_name = normalize_identifier(namespace_name)?;
    let tbl_name = normalize_identifier(table_name)?;

    // Remove from cache
    {
        let mut cache = entry
            .table_cache
            .write()
            .map_err(|e| format!("table cache lock: {e}"))?;
        cache.remove(&(ns_name.clone(), tbl_name.clone()));
    }

    if entry.is_s3() {
        // For S3 catalogs, we don't delete S3 data here.
        // A proper implementation would use the Iceberg catalog API to purge.
        tracing::warn!(
            "drop_table for S3 catalog is a metadata-only operation; S3 data for {ns_name}.{tbl_name} is not deleted"
        );
        Ok(())
    } else {
        let table_dir = entry.warehouse_path.join(&ns_name).join(&tbl_name);
        if table_dir.exists() {
            std::fs::remove_dir_all(&table_dir)
                .map_err(|e| format!("drop table directory {} failed: {e}", table_dir.display()))?;
        }
        Ok(())
    }
}

pub(crate) fn load_table(
    entry: &IcebergCatalogEntry,
    namespace_name: &str,
    table_name: &str,
) -> Result<IcebergLoadedTable, String> {
    let ns_name = normalize_identifier(namespace_name)?;
    let tbl_name = normalize_identifier(table_name)?;

    // Check cache first
    {
        let cache = entry
            .table_cache
            .read()
            .map_err(|e| format!("table cache lock: {e}"))?;
        if let Some(cached) = cache.get(&(ns_name.clone(), tbl_name.clone())) {
            return Ok(cached.clone());
        }
    }

    let table = if let Some(s3_config) = &entry.s3_config {
        // S3 path: discover metadata from S3 directly
        let op = crate::fs::object_store::build_oss_operator(s3_config)
            .map_err(|e| format!("build S3 operator for load_table: {e}"))?;
        let (_, root_prefix) = super::iceberg_add_files::parse_s3_path(&entry.warehouse_uri)
            .map_err(|e| format!("parse warehouse URI: {e}"))?;
        let meta_prefix = format!(
            "{}/{}/{}/metadata/",
            root_prefix.trim_end_matches('/'),
            ns_name,
            tbl_name
        );

        // Find latest metadata JSON
        let (metadata_file_name, metadata_bytes) = block_on_iceberg(async {
            let entries = op
                .list(&meta_prefix)
                .await
                .map_err(|e| format!("list metadata dir {meta_prefix}: {e}"))?;
            let mut json_files: Vec<String> = entries
                .iter()
                .filter(|e| e.name().ends_with(".metadata.json"))
                .map(|e| e.name().to_string())
                .collect();
            json_files.sort();
            let latest = json_files
                .last()
                .ok_or_else(|| format!("no metadata files for {ns_name}.{tbl_name}"))?
                .clone();
            let path = format!("{meta_prefix}{latest}");
            let data = op
                .read(&path)
                .await
                .map_err(|e| format!("read metadata {path}: {e}"))?;
            Ok::<(String, Vec<u8>), String>((latest, data.to_vec()))
        })
        .map_err(|e| format!("load table metadata runtime: {e}"))??;

        let metadata: iceberg::spec::TableMetadata = serde_json::from_slice(&metadata_bytes)
            .map_err(|e| format!("deserialize iceberg metadata: {e}"))?;

        let warehouse_trimmed = entry.warehouse_uri.trim_end_matches('/');
        let metadata_location =
            format!("{warehouse_trimmed}/{ns_name}/{tbl_name}/metadata/{metadata_file_name}");

        let storage_factory =
            super::iceberg_s3_storage::S3StorageFactory::from_catalog_properties(&entry.properties)
                .ok_or_else(|| "missing S3 properties for FileIO".to_string())?;
        let file_io = iceberg::io::FileIOBuilder::new(Arc::new(storage_factory)).build();

        iceberg::table::Table::builder()
            .file_io(file_io)
            .metadata(Arc::new(metadata))
            .identifier(
                TableIdent::from_strs([ns_name.as_str(), tbl_name.as_str()])
                    .map_err(|e| format!("build table ident: {e}"))?,
            )
            .metadata_location(metadata_location)
            .build()
            .map_err(|e| format!("build iceberg table: {e}"))?
    } else {
        // Local path: find latest metadata on filesystem
        let metadata_location = latest_table_metadata_location_local(entry, &ns_name, &tbl_name)?;

        let metadata_path = metadata_location
            .strip_prefix("file://")
            .unwrap_or(&metadata_location);
        let metadata_bytes =
            std::fs::read(metadata_path).map_err(|e| format!("read local metadata file: {e}"))?;
        let metadata: iceberg::spec::TableMetadata = serde_json::from_slice(&metadata_bytes)
            .map_err(|e| format!("deserialize iceberg metadata: {e}"))?;

        let file_io = iceberg::io::FileIOBuilder::new(
            Arc::new(LocalFsStorageFactory) as Arc<dyn iceberg::io::StorageFactory>
        )
        .build();

        iceberg::table::Table::builder()
            .file_io(file_io)
            .metadata(Arc::new(metadata))
            .identifier(
                TableIdent::from_strs([ns_name.as_str(), tbl_name.as_str()])
                    .map_err(|e| format!("build table ident: {e}"))?,
            )
            .metadata_location(metadata_location)
            .build()
            .map_err(|e| format!("build iceberg table: {e}"))?
    };

    let logical_types = parse_logical_type_properties(table.metadata().properties())?;
    let key_desc = parse_table_key_desc_properties(table.metadata().properties())?;
    let column_aggregations = parse_column_aggregation_properties(table.metadata().properties())?;
    let arrow_schema = schema_to_arrow_schema(table.metadata().current_schema())
        .map_err(|e| format!("convert iceberg schema to arrow schema failed: {e}"))?;
    let columns = arrow_schema
        .fields()
        .iter()
        .map(|field| {
            let field_name = normalize_identifier(field.name()).map_err(|e| {
                format!(
                    "normalize iceberg column name `{}` failed: {e}",
                    field.name()
                )
            })?;
            Ok(ColumnDef {
                name: field.name().clone(),
                data_type: apply_logical_type_override(
                    field.data_type(),
                    logical_types.get(&field_name),
                ),
                nullable: field.is_nullable(),
            })
        })
        .collect::<Result<Vec<_>, String>>()?;

    let loaded = IcebergLoadedTable {
        table,
        columns,
        logical_types,
        key_desc,
        column_aggregations,
    };

    // Cache the loaded table
    {
        let mut cache = entry
            .table_cache
            .write()
            .map_err(|e| format!("table cache lock: {e}"))?;
        cache.insert((ns_name, tbl_name), loaded.clone());
    }

    Ok(loaded)
}

pub(crate) fn insert_rows(
    entry: &IcebergCatalogEntry,
    namespace_name: &str,
    table_name: &str,
    rows: &[Vec<Literal>],
) -> Result<(), String> {
    let loaded = load_table(entry, namespace_name, table_name)?;
    let batch = build_insert_batch(&loaded, rows)?;

    // Build a temporary MemoryCatalog for transaction commit
    let temp_catalog = build_temp_memory_catalog(entry)?;
    let ns = NamespaceIdent::new(normalize_identifier(namespace_name)?);
    let _ = block_on_iceberg(async { temp_catalog.create_namespace(&ns, HashMap::new()).await });
    let table_ident = TableIdent::from_strs([
        normalize_identifier(namespace_name)?,
        normalize_identifier(table_name)?,
    ])
    .map_err(|e| format!("build iceberg table ident: {e}"))?;
    let metadata_location = loaded
        .table
        .metadata_location()
        .ok_or_else(|| "no metadata location for table".to_string())?
        .to_string();
    let _ = block_on_iceberg(async {
        temp_catalog
            .register_table(&table_ident, metadata_location)
            .await
    });

    block_on_iceberg(async {
        let location_generator = DefaultLocationGenerator::new(loaded.table.metadata().clone())
            .map_err(|e| iceberg::Error::new(iceberg::ErrorKind::DataInvalid, e.to_string()))?;
        let file_name_prefix = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|duration| format!("standalone-{}", duration.as_nanos()))
            .unwrap_or_else(|_| "standalone".to_string());
        let file_name_generator = DefaultFileNameGenerator::new(
            file_name_prefix,
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
        tx.commit(&temp_catalog).await.map(|_| ())
    })
    .map_err(|e| format!("insert iceberg rows runtime failed: {e}"))?
    .map_err(|e| format!("insert iceberg rows failed: {e}"))?;

    // Invalidate cache after insert
    {
        let mut cache = entry
            .table_cache
            .write()
            .map_err(|e| format!("table cache lock: {e}"))?;
        cache.remove(&(
            normalize_identifier(namespace_name)?,
            normalize_identifier(table_name)?,
        ));
    }

    Ok(())
}

/// Extract data file paths, sizes, and row counts from an Iceberg table via scan planning.
pub(crate) fn extract_data_files(
    table: &iceberg::table::Table,
) -> Result<Vec<(String, i64, Option<i64>)>, String> {
    block_on_iceberg(async {
        let scan = table
            .scan()
            .build()
            .map_err(|e| format!("build scan: {e}"))?;
        let tasks: Vec<_> = scan
            .plan_files()
            .await
            .map_err(|e| format!("plan files: {e}"))?
            .try_collect()
            .await
            .map_err(|e| format!("collect tasks: {e}"))?;
        Ok(tasks
            .iter()
            .map(|t| {
                (
                    t.data_file_path.clone(),
                    i64::try_from(t.file_size_in_bytes).unwrap_or(i64::MAX),
                    t.record_count.map(|c| c as i64),
                )
            })
            .collect())
    })
    .map_err(|e| format!("extract data files runtime: {e}"))?
}

/// Register an existing Iceberg table in the catalog entry by loading it.
/// This is used by metadata restore to ensure tables are accessible.
pub(crate) fn register_existing_table(
    entry: &IcebergCatalogEntry,
    namespace_name: &str,
    table_name: &str,
) -> Result<(), String> {
    // Simply load the table to populate the cache
    load_table(entry, namespace_name, table_name)?;
    Ok(())
}

fn build_catalog_entry(
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
    // Accept both "memory" and "hadoop" as valid catalog types (or ignore the field)
    if let Some(catalog_type) = props.get("iceberg.catalog.type")
        && !catalog_type.eq_ignore_ascii_case("memory")
        && !catalog_type.eq_ignore_ascii_case("hadoop")
    {
        return Err(format!(
            "standalone iceberg catalog supports iceberg.catalog.type=memory|hadoop, got {catalog_type}"
        ));
    }

    let raw_warehouse = props
        .get("iceberg.catalog.warehouse")
        .or_else(|| props.get("warehouse"))
        .cloned()
        .ok_or_else(|| {
            "standalone iceberg catalog requires `iceberg.catalog.warehouse`".to_string()
        })?;

    // Detect S3 storage: if warehouse starts with s3:// or oss://, use S3StorageFactory
    let is_s3 = raw_warehouse.starts_with("s3://")
        || raw_warehouse.starts_with("s3a://")
        || raw_warehouse.starts_with("oss://");

    let (warehouse_uri, warehouse_path, s3_config) = if is_s3 {
        let s3_factory = super::iceberg_s3_storage::S3StorageFactory::from_catalog_properties(
            properties,
        )
        .ok_or_else(|| {
            "S3 iceberg catalog requires aws.s3.endpoint, aws.s3.access_key, aws.s3.secret_key"
                .to_string()
        })?;
        let (bucket, _root_prefix) = super::iceberg_add_files::parse_s3_path(&raw_warehouse)
            .map_err(|e| format!("parse warehouse URI: {e}"))?;
        let cfg = crate::fs::object_store::ObjectStoreConfig {
            endpoint: s3_factory.endpoint.clone(),
            bucket,
            root: String::new(),
            access_key_id: s3_factory.access_key_id.clone(),
            access_key_secret: s3_factory.access_key_secret.clone(),
            session_token: None,
            enable_path_style_access: Some(s3_factory.enable_path_style),
            region: Some(s3_factory.region.clone()),
            retry_max_times: Some(3),
            retry_min_delay_ms: Some(100),
            retry_max_delay_ms: Some(2000),
            timeout_ms: Some(30000),
            io_timeout_ms: Some(30000),
        };
        // S3 warehouse: keep URI as-is, use a temp local path for metadata cache
        let cache_dir = std::env::temp_dir()
            .join("novarocks_iceberg_cache")
            .join(catalog_name);
        std::fs::create_dir_all(&cache_dir)
            .map_err(|e| format!("create iceberg cache dir failed: {e}"))?;
        (raw_warehouse.clone(), cache_dir, Some(cfg))
    } else {
        let (uri, path) = normalize_warehouse_location(&raw_warehouse)?;
        std::fs::create_dir_all(&path).map_err(|e| {
            format!(
                "create iceberg warehouse directory {} failed: {e}",
                path.display()
            )
        })?;
        (uri, path, None)
    };

    props.insert("type".to_string(), "iceberg".to_string());
    props.insert(
        "iceberg.catalog.warehouse".to_string(),
        warehouse_uri.clone(),
    );

    let entry = IcebergCatalogEntry {
        name: catalog_name.to_string(),
        warehouse_uri,
        properties: sorted_properties(&props),
        s3_config,
        warehouse_path,
        table_cache: Arc::new(std::sync::RwLock::new(HashMap::new())),
    };

    Ok(entry)
}

/// Build a temporary MemoryCatalog for operations that need a Catalog impl
/// (e.g., create_table, insert_rows transaction commit).
fn build_temp_memory_catalog(entry: &IcebergCatalogEntry) -> Result<MemoryCatalog, String> {
    let storage_factory: Arc<dyn iceberg::io::StorageFactory> = if entry.is_s3() {
        let s3_factory = super::iceberg_s3_storage::S3StorageFactory::from_catalog_properties(
            &entry.properties,
        )
        .ok_or_else(|| {
            "S3 iceberg catalog requires aws.s3.endpoint, aws.s3.access_key, aws.s3.secret_key"
                .to_string()
        })?;
        Arc::new(s3_factory)
    } else {
        Arc::new(LocalFsStorageFactory)
    };

    let warehouse_uri = entry.warehouse_uri.clone();
    block_on_iceberg(
        MemoryCatalogBuilder::default()
            .with_storage_factory(storage_factory)
            .load(
                entry.name.clone(),
                HashMap::from([(MEMORY_CATALOG_WAREHOUSE.to_string(), warehouse_uri)]),
            ),
    )
    .map_err(|e| format!("create temp memory catalog runtime: {e}"))?
    .map_err(|e| format!("create temp memory catalog: {e}"))
}

pub(crate) fn block_on_iceberg<F>(future: F) -> Result<F::Output, String>
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

fn latest_table_metadata_location_local(
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
    let mut next_nested_field_id =
        i32::try_from(columns.len() + 1).map_err(|_| "too many iceberg columns".to_string())?;
    let fields = columns
        .iter()
        .enumerate()
        .map(|(idx, column)| {
            let field_id =
                i32::try_from(idx + 1).map_err(|_| "too many iceberg columns".to_string())?;
            Ok(NestedField::optional(
                field_id,
                &column.name,
                iceberg_type_for_sql_type(&column.data_type, &mut next_nested_field_id)?,
            )
            .into())
        })
        .collect::<Result<Vec<_>, String>>()?;
    Schema::builder()
        .with_fields(fields)
        .build()
        .map_err(|e| format!("build iceberg schema failed: {e}"))
}

fn iceberg_type_for_sql_type(data_type: &SqlType, next_field_id: &mut i32) -> Result<Type, String> {
    Ok(match data_type {
        SqlType::TinyInt | SqlType::SmallInt | SqlType::Int => Type::Primitive(PrimitiveType::Int),
        SqlType::Float => Type::Primitive(PrimitiveType::Float),
        SqlType::Double => Type::Primitive(PrimitiveType::Double),
        SqlType::Decimal { precision, scale } => Type::Primitive(PrimitiveType::Decimal {
            precision: *precision as u32,
            scale: *scale as u32,
        }),
        SqlType::BigInt => Type::Primitive(PrimitiveType::Long),
        SqlType::LargeInt => Type::Primitive(PrimitiveType::Decimal {
            precision: 38,
            scale: 0,
        }),
        SqlType::String => Type::Primitive(PrimitiveType::String),
        SqlType::Boolean => Type::Primitive(PrimitiveType::Boolean),
        SqlType::Date => Type::Primitive(PrimitiveType::Date),
        SqlType::DateTime => Type::Primitive(PrimitiveType::Timestamp),
        SqlType::Time => Type::Primitive(PrimitiveType::Time),
        SqlType::Array(inner) => {
            let element_field_id = *next_field_id;
            *next_field_id += 1;
            Type::List(ListType::new(Arc::new(NestedField::optional(
                element_field_id,
                "element",
                iceberg_type_for_sql_type(inner, next_field_id)?,
            ))))
        }
    })
}

pub(crate) fn build_insert_batch(
    loaded: &IcebergLoadedTable,
    rows: &[Vec<Literal>],
) -> Result<RecordBatch, String> {
    let base_arrow_schema = schema_to_arrow_schema(loaded.table.metadata().current_schema())
        .map_err(|e| format!("convert iceberg schema to arrow schema failed: {e}"))?;
    let arrow_schema = Arc::new(ArrowSchema::new(
        base_arrow_schema
            .fields()
            .iter()
            .zip(loaded.columns.iter())
            .map(|(field, column)| {
                Field::new(field.name(), column.data_type.clone(), field.is_nullable())
                    .with_metadata(field.metadata().clone())
            })
            .collect::<Vec<_>>(),
    ));
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
        let field_name = normalize_identifier(field.name()).map_err(|e| {
            format!(
                "normalize iceberg column name `{}` failed: {e}",
                field.name()
            )
        })?;
        let logical_type = loaded.logical_types.get(&field_name);
        arrays.push(build_literal_array(
            field.data_type(),
            &values,
            logical_type,
        )?);
    }
    RecordBatch::try_new(arrow_schema, arrays)
        .map_err(|e| format!("build iceberg insert batch failed: {e}"))
}

fn build_literal_array(
    data_type: &DataType,
    values: &[&Literal],
    logical_type: Option<&SqlType>,
) -> Result<ArrayRef, String> {
    match data_type {
        DataType::Int32 => Ok(Arc::new(Int32Array::from(
            values
                .iter()
                .map(|literal| match literal {
                    Literal::Null => Ok(None),
                    Literal::Int(value) => coerce_i32_literal(*value, logical_type),
                    Literal::String(value) => value
                        .trim()
                        .parse::<i64>()
                        .map_err(|_| format!("literal `{value}` is not valid for INT"))
                        .and_then(|value| coerce_i32_literal(value, logical_type)),
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
                    Literal::String(value) => value
                        .trim()
                        .parse::<i64>()
                        .map(Some)
                        .map_err(|_| format!("literal `{value}` is not valid for BIGINT")),
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
                    Literal::String(value) => value
                        .trim()
                        .parse::<f64>()
                        .map(Some)
                        .map_err(|_| format!("literal `{value}` is not valid for DOUBLE")),
                    other => Err(format!("literal {:?} is not valid for DOUBLE", other)),
                })
                .collect::<Result<Vec<_>, _>>()?,
        ))),
        DataType::Decimal128(precision, scale) => {
            let values = values
                .iter()
                .map(|literal| match literal {
                    Literal::Null => Ok(None),
                    Literal::Int(value) => scale_i128_decimal(i128::from(*value), *scale).map(Some),
                    Literal::String(value) => {
                        parse_decimal_literal_to_i128(value, *scale).map(Some)
                    }
                    Literal::Float(value) => {
                        parse_decimal_literal_to_i128(&value.to_string(), *scale).map(Some)
                    }
                    other => Err(format!("literal {:?} is not valid for DECIMAL", other)),
                })
                .collect::<Result<Vec<_>, _>>()?;
            let array = Decimal128Array::from(values)
                .with_precision_and_scale(*precision, *scale)
                .map_err(|e| format!("build DECIMAL array failed: {e}"))?;
            Ok(Arc::new(array))
        }
        DataType::Utf8 => Ok(Arc::new(StringArray::from(
            values
                .iter()
                .map(|literal| match literal {
                    Literal::Null => Ok(None),
                    Literal::String(value) | Literal::Date(value) => Ok(Some(value.clone())),
                    Literal::Int(value) => Ok(Some(value.to_string())),
                    Literal::Float(value) => Ok(Some(value.to_string())),
                    Literal::Bool(value) => Ok(Some(if *value {
                        "1".to_string()
                    } else {
                        "0".to_string()
                    })),
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
                    Literal::Int(value) if *value == 0 => Ok(Some(false)),
                    Literal::Int(value) if *value == 1 => Ok(Some(true)),
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
                    Literal::Int(value) => Ok(parse_numeric_date_literal(*value).ok()),
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
                        Literal::Date(value) => parse_timestamp_literal_to_micros(value).map(Some),
                        Literal::Int(value) => Ok(parse_numeric_timestamp_literal(*value).ok()),
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
        DataType::List(field) if matches!(field.data_type(), DataType::Int32) => {
            let mut builder = ListBuilder::new(Int32Builder::new());
            for literal in values {
                match literal {
                    Literal::Null => builder.append(false),
                    Literal::Array(items) => {
                        for item in items {
                            match item {
                                Literal::Null => builder.values().append_null(),
                                Literal::Int(value) => {
                                    let value = i32::try_from(*value).map_err(|_| {
                                        format!("literal {value} is out of range for ARRAY<INT>")
                                    })?;
                                    builder.values().append_value(value);
                                }
                                other => {
                                    return Err(format!(
                                        "literal {:?} is not valid for ARRAY<INT>",
                                        other
                                    ));
                                }
                            }
                        }
                        builder.append(true);
                    }
                    other => {
                        return Err(format!("literal {:?} is not valid for ARRAY<INT>", other));
                    }
                }
            }
            let list = builder.finish();
            let (_, offsets, values, nulls) = list.into_parts();
            Ok(Arc::new(arrow::array::ListArray::new(
                field.clone(),
                offsets,
                values,
                nulls,
            )))
        }
        DataType::List(field) if matches!(field.data_type(), DataType::Int64) => {
            let mut builder = ListBuilder::new(Int64Builder::new());
            for literal in values {
                match literal {
                    Literal::Null => builder.append(false),
                    Literal::Array(items) => {
                        for item in items {
                            match item {
                                Literal::Null => builder.values().append_null(),
                                Literal::Int(value) => builder.values().append_value(*value),
                                other => {
                                    return Err(format!(
                                        "literal {:?} is not valid for ARRAY<BIGINT>",
                                        other
                                    ));
                                }
                            }
                        }
                        builder.append(true);
                    }
                    other => {
                        return Err(format!(
                            "literal {:?} is not valid for ARRAY<BIGINT>",
                            other
                        ));
                    }
                }
            }
            let list = builder.finish();
            let (_, offsets, values, nulls) = list.into_parts();
            Ok(Arc::new(arrow::array::ListArray::new(
                field.clone(),
                offsets,
                values,
                nulls,
            )))
        }
        DataType::List(field) if matches!(field.data_type(), DataType::Utf8) => {
            let mut builder = ListBuilder::new(StringBuilder::new());
            for literal in values {
                match literal {
                    Literal::Null => builder.append(false),
                    Literal::Array(items) => {
                        for item in items {
                            match item {
                                Literal::Null => builder.values().append_null(),
                                Literal::String(value) => builder.values().append_value(value),
                                other => {
                                    return Err(format!(
                                        "literal {:?} is not valid for ARRAY<STRING>",
                                        other
                                    ));
                                }
                            }
                        }
                        builder.append(true);
                    }
                    other => {
                        return Err(format!(
                            "literal {:?} is not valid for ARRAY<STRING>",
                            other
                        ));
                    }
                }
            }
            let list = builder.finish();
            let (_, offsets, values, nulls) = list.into_parts();
            Ok(Arc::new(arrow::array::ListArray::new(
                field.clone(),
                offsets,
                values,
                nulls,
            )))
        }
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

fn build_logical_type_properties(
    columns: &[TableColumnDef],
) -> Result<Vec<(String, String)>, String> {
    let mut properties = Vec::new();
    for column in columns {
        let Some(value) = logical_type_property_value(&column.data_type) else {
            continue;
        };
        properties.push((logical_type_property_key(&column.name)?, value.to_string()));
    }
    Ok(properties)
}

fn build_table_semantics_properties(
    columns: &[TableColumnDef],
    key_desc: Option<&TableKeyDesc>,
) -> Result<Vec<(String, String)>, String> {
    let mut properties = Vec::new();
    if let Some(key_desc) = key_desc {
        properties.push((
            TABLE_KEY_KIND_PROPERTY.to_string(),
            format_table_key_kind(key_desc.kind).to_string(),
        ));
        if !key_desc.columns.is_empty() {
            let columns = key_desc
                .columns
                .iter()
                .map(|column| normalize_identifier(column))
                .collect::<Result<Vec<_>, _>>()?;
            properties.push((TABLE_KEY_COLUMNS_PROPERTY.to_string(), columns.join(",")));
        }
    }
    for column in columns {
        let Some(aggregation) = column.aggregation else {
            continue;
        };
        properties.push((
            column_aggregation_property_key(&column.name)?,
            format_column_aggregation(aggregation).to_string(),
        ));
    }
    Ok(properties)
}

fn parse_logical_type_properties(
    properties: &HashMap<String, String>,
) -> Result<HashMap<String, SqlType>, String> {
    let mut logical_types = HashMap::new();
    for (key, value) in properties {
        let Some(column_name) = key.strip_prefix(LOGICAL_TYPE_PROPERTY_PREFIX) else {
            continue;
        };
        let sql_type = parse_logical_type_property_value(value)
            .ok_or_else(|| format!("unsupported stored logical type `{value}`"))?;
        logical_types.insert(column_name.to_string(), sql_type);
    }
    Ok(logical_types)
}

fn parse_table_key_desc_properties(
    properties: &HashMap<String, String>,
) -> Result<Option<TableKeyDesc>, String> {
    let Some(kind) = properties.get(TABLE_KEY_KIND_PROPERTY) else {
        return Ok(None);
    };
    let kind = parse_table_key_kind(kind)
        .ok_or_else(|| format!("unsupported stored table key kind `{kind}`"))?;
    let columns = properties
        .get(TABLE_KEY_COLUMNS_PROPERTY)
        .map(|columns| {
            columns
                .split(',')
                .filter(|column| !column.trim().is_empty())
                .map(|column| normalize_identifier(column.trim()))
                .collect::<Result<Vec<_>, _>>()
        })
        .transpose()?
        .unwrap_or_default();
    Ok(Some(TableKeyDesc { kind, columns }))
}

fn parse_column_aggregation_properties(
    properties: &HashMap<String, String>,
) -> Result<HashMap<String, ColumnAggregation>, String> {
    let mut aggregations = HashMap::new();
    for (key, value) in properties {
        let Some(column_name) = key.strip_prefix(COLUMN_AGGREGATION_PROPERTY_PREFIX) else {
            continue;
        };
        let aggregation = parse_column_aggregation(value)
            .ok_or_else(|| format!("unsupported stored column aggregation `{value}`"))?;
        aggregations.insert(column_name.to_string(), aggregation);
    }
    Ok(aggregations)
}

fn logical_type_property_key(column_name: &str) -> Result<String, String> {
    Ok(format!(
        "{LOGICAL_TYPE_PROPERTY_PREFIX}{}",
        normalize_identifier(column_name)?
    ))
}

fn column_aggregation_property_key(column_name: &str) -> Result<String, String> {
    Ok(format!(
        "{COLUMN_AGGREGATION_PROPERTY_PREFIX}{}",
        normalize_identifier(column_name)?
    ))
}

fn format_table_key_kind(kind: TableKeyKind) -> &'static str {
    match kind {
        TableKeyKind::Duplicate => "duplicate",
        TableKeyKind::Unique => "unique",
        TableKeyKind::Aggregate => "aggregate",
        TableKeyKind::Primary => "primary",
    }
}

fn parse_table_key_kind(value: &str) -> Option<TableKeyKind> {
    match value {
        "duplicate" => Some(TableKeyKind::Duplicate),
        "unique" => Some(TableKeyKind::Unique),
        "aggregate" => Some(TableKeyKind::Aggregate),
        "primary" => Some(TableKeyKind::Primary),
        _ => None,
    }
}

fn format_column_aggregation(aggregation: ColumnAggregation) -> &'static str {
    match aggregation {
        ColumnAggregation::Sum => "sum",
        ColumnAggregation::Min => "min",
        ColumnAggregation::Max => "max",
        ColumnAggregation::Replace => "replace",
    }
}

fn parse_column_aggregation(value: &str) -> Option<ColumnAggregation> {
    match value {
        "sum" => Some(ColumnAggregation::Sum),
        "min" => Some(ColumnAggregation::Min),
        "max" => Some(ColumnAggregation::Max),
        "replace" => Some(ColumnAggregation::Replace),
        _ => None,
    }
}

fn logical_type_property_value(data_type: &SqlType) -> Option<String> {
    match data_type {
        SqlType::TinyInt => Some("tinyint".to_string()),
        SqlType::SmallInt => Some("smallint".to_string()),
        SqlType::Date => Some("date".to_string()),
        SqlType::Decimal { precision, scale } => Some(format!("decimal({precision},{scale})")),
        _ => None,
    }
}

fn parse_logical_type_property_value(value: &str) -> Option<SqlType> {
    match value {
        "tinyint" => Some(SqlType::TinyInt),
        "smallint" => Some(SqlType::SmallInt),
        "date" => Some(SqlType::Date),
        _ => parse_decimal_logical_type(value),
    }
}

fn parse_decimal_logical_type(value: &str) -> Option<SqlType> {
    let body = value.strip_prefix("decimal(")?.strip_suffix(')')?.trim();
    let (precision, scale) = body.split_once(',')?;
    let precision = precision.trim().parse::<u8>().ok()?;
    let scale = scale.trim().parse::<i8>().ok()?;
    Some(SqlType::Decimal { precision, scale })
}

fn scale_i128_decimal(value: i128, scale: i8) -> Result<i128, String> {
    if scale < 0 {
        return Err(format!("negative DECIMAL scale {scale} is not supported"));
    }
    let factor = 10_i128
        .checked_pow(scale as u32)
        .ok_or_else(|| format!("DECIMAL scale {scale} is out of range"))?;
    value
        .checked_mul(factor)
        .ok_or_else(|| format!("DECIMAL literal {value} is out of range"))
}

fn parse_decimal_literal_to_i128(value: &str, scale: i8) -> Result<i128, String> {
    const I128_MIN_ABS: &str = "170141183460469231731687303715884105728";

    if scale < 0 {
        return Err(format!("negative DECIMAL scale {scale} is not supported"));
    }
    let trimmed = value.trim();
    let (negative, raw) = if let Some(raw) = trimmed.strip_prefix('-') {
        (true, raw)
    } else if let Some(raw) = trimmed.strip_prefix('+') {
        (false, raw)
    } else {
        (false, trimmed)
    };
    let (whole, fraction) = raw.split_once('.').unwrap_or((raw, ""));
    if fraction.len() > scale as usize {
        return Err(format!(
            "DECIMAL literal `{value}` has more than {scale} fractional digits"
        ));
    }
    let padded_fraction = format!("{fraction:0<width$}", width = scale as usize);
    let combined = format!("{whole}{padded_fraction}");
    let combined = combined.trim_start_matches('+');
    let mut parsed = if combined.is_empty() {
        0_i128
    } else if negative && scale == 0 && combined == I128_MIN_ABS {
        i128::MIN
    } else {
        combined
            .parse::<i128>()
            .map_err(|_| format!("DECIMAL literal `{value}` is out of range"))?
    };
    if negative {
        if parsed != i128::MIN {
            parsed = -parsed;
        }
    }
    Ok(parsed)
}

fn coerce_i32_literal(value: i64, logical_type: Option<&SqlType>) -> Result<Option<i32>, String> {
    match logical_type {
        Some(SqlType::TinyInt) => {
            if (i64::from(i8::MIN)..=i64::from(i8::MAX)).contains(&value) {
                Ok(Some(i32::from(value as i8)))
            } else {
                Ok(None)
            }
        }
        Some(SqlType::SmallInt) => {
            if (i64::from(i16::MIN)..=i64::from(i16::MAX)).contains(&value) {
                Ok(Some(i32::from(value as i16)))
            } else {
                Ok(None)
            }
        }
        _ => i32::try_from(value)
            .map(Some)
            .map_err(|_| format!("literal {value} is out of range for INT")),
    }
}

fn apply_logical_type_override(data_type: &DataType, logical_type: Option<&SqlType>) -> DataType {
    match logical_type {
        Some(SqlType::Date) => DataType::Date32,
        _ => data_type.clone(),
    }
}

fn parse_timestamp_literal_to_micros(value: &str) -> Result<i64, String> {
    let timestamp = NaiveDateTime::parse_from_str(value, "%Y-%m-%d %H:%M:%S%.f")
        .or_else(|_| NaiveDateTime::parse_from_str(value, "%Y-%m-%d %H:%M:%S"))
        .or_else(|_| {
            NaiveDate::parse_from_str(value, "%Y-%m-%d")
                .map(|date| date.and_hms_opt(0, 0, 0).expect("midnight"))
        })
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

fn parse_numeric_date_literal(value: i64) -> Result<i32, String> {
    let raw = value.to_string();
    let date = if raw.len() == 8 {
        NaiveDate::parse_from_str(&raw, "%Y%m%d")
            .map_err(|e| format!("parse numeric DATE literal `{value}` failed: {e}"))?
    } else if raw.len() == 6 {
        NaiveDate::parse_from_str(&raw, "%y%m%d")
            .map_err(|e| format!("parse numeric DATE literal `{value}` failed: {e}"))?
    } else if raw.len() <= 5 {
        NaiveDate::parse_from_str(&format!("{value:06}"), "%y%m%d")
            .map_err(|e| format!("parse numeric DATE literal `{value}` failed: {e}"))?
    } else {
        return Err(format!(
            "parse numeric DATE literal `{value}` failed: unsupported width {}",
            raw.len()
        ));
    };
    let epoch = NaiveDate::from_ymd_opt(1970, 1, 1).expect("epoch");
    i32::try_from(date.signed_duration_since(epoch).num_days())
        .map_err(|_| format!("DATE literal `{value}` is out of range"))
}

fn parse_numeric_timestamp_literal(value: i64) -> Result<i64, String> {
    let raw = value.to_string();
    let timestamp = if raw.len() == 14 {
        NaiveDateTime::parse_from_str(&raw, "%Y%m%d%H%M%S")
            .map_err(|e| format!("parse numeric DATETIME literal `{value}` failed: {e}"))?
    } else if raw.len() == 12 {
        NaiveDateTime::parse_from_str(&raw, "%y%m%d%H%M%S")
            .map_err(|e| format!("parse numeric DATETIME literal `{value}` failed: {e}"))?
    } else if matches!(raw.len(), 1..=8) {
        let days = parse_numeric_date_literal(value)?;
        let epoch = NaiveDate::from_ymd_opt(1970, 1, 1).expect("epoch");
        let date = epoch + chrono::Duration::days(i64::from(days));
        date.and_hms_opt(0, 0, 0).expect("midnight")
    } else {
        return Err(format!(
            "parse numeric DATETIME literal `{value}` failed: unsupported width {}",
            raw.len()
        ));
    };
    let epoch = NaiveDate::from_ymd_opt(1970, 1, 1)
        .expect("epoch")
        .and_hms_opt(0, 0, 0)
        .expect("epoch timestamp");
    timestamp
        .signed_duration_since(epoch)
        .num_microseconds()
        .ok_or_else(|| format!("DATETIME literal `{value}` is out of range"))
}
