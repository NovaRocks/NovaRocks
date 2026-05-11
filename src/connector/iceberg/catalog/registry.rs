use std::collections::{BTreeMap, HashMap};
use std::future::Future;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use arrow::array::{
    ArrayRef, BooleanArray, Date32Array, Decimal128Array, Float32Array, Float64Array, Int32Array,
    Int32Builder, Int64Array, Int64Builder, ListBuilder, StringArray, StringBuilder,
    Time64MicrosecondArray, TimestampMicrosecondArray,
};
use arrow::datatypes::{DataType, Field, Schema as ArrowSchema, TimeUnit};
use arrow::record_batch::RecordBatch;
use chrono::{NaiveDate, NaiveDateTime, NaiveTime, Timelike};
use iceberg::arrow::schema_to_arrow_schema;
use iceberg::io::LocalFsStorageFactory;
use iceberg::spec::{
    FormatVersion, ListType, Literal as IcebergLiteral, MapType, NestedField, PrimitiveLiteral,
    PrimitiveType, Schema, StructType, TableMetadata, Transform, Type,
};
use iceberg::transaction::{ApplyTransactionAction, Transaction};
use iceberg::{Catalog, NamespaceIdent, TableCreation, TableIdent};

use crate::runtime::global_async_runtime::data_block_on;

use crate::connector::iceberg::data_writer::write_record_batches_as_data_files;
use crate::engine::catalog::{ColumnDef, normalize_identifier};
use crate::sql::{ColumnAggregation, Literal, SqlType, TableColumnDef, TableKeyDesc, TableKeyKind};

#[derive(Default)]
pub(crate) struct IcebergCatalogRegistry {
    catalogs: HashMap<String, IcebergCatalogEntry>,
}

/// Selects which Iceberg catalog implementation an [`IcebergCatalogEntry`]
/// stands for. Determined at `CREATE EXTERNAL CATALOG` time from the
/// `iceberg.catalog.type` property.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum IcebergCatalogKind {
    /// `iceberg.catalog.type = hadoop` (default when omitted) — NovaRocks
    /// owns the warehouse directly via `HadoopFileSystemCatalog`. Metadata
    /// lives at `<warehouse>/<ns>/<table>/metadata/v{N}.metadata.json`.
    Hadoop,
    /// `iceberg.catalog.type = memory` — testing-only, in-memory table
    /// registry; not yet used by the catalog pipeline but accepted by
    /// `build_catalog_entry` as a no-op kind.
    Memory,
    /// `iceberg.catalog.type = rest` — speak Iceberg REST Catalog
    /// protocol against an external server (`uri`). Used by Lakekeeper /
    /// Polaris / Tabular / Snowflake Open Catalog / etc.
    Rest,
}

#[derive(Clone)]
pub(crate) struct IcebergCatalogEntry {
    // Tracked but unused outside the REST catalog path / tests until the
    // engine flows migrate from `build_hadoop_catalog` to the unified
    // `build_iceberg_catalog` dispatcher.
    #[allow(dead_code)]
    pub(crate) kind: IcebergCatalogKind,
    pub(crate) warehouse_uri: String,
    /// REST endpoint URL (`uri` property) — populated only when
    /// `kind == IcebergCatalogKind::Rest`. None for Hadoop / Memory.
    #[allow(dead_code)]
    pub(crate) rest_uri: Option<String>,
    pub(crate) properties: Vec<(String, String)>,
    s3_config: Option<crate::fs::object_store::ObjectStoreConfig>,
    pub(crate) warehouse_path: PathBuf,
    table_cache: Arc<std::sync::RwLock<HashMap<(String, String), IcebergLoadedTable>>>,
    data_files_cache:
        Arc<std::sync::RwLock<HashMap<(String, String, Option<i64>), Vec<DataFileWithStats>>>>,
}

#[derive(Clone, Debug)]
pub(crate) struct IcebergLoadedTable {
    pub table: iceberg::table::Table,
    pub columns: Vec<ColumnDef>,
    pub logical_types: HashMap<String, SqlType>,
    pub key_desc: Option<TableKeyDesc>,
    pub column_aggregations: HashMap<String, ColumnAggregation>,
    pub object_store_config: Option<crate::fs::object_store::ObjectStoreConfig>,
}

const LOGICAL_TYPE_PROPERTY_PREFIX: &str = "novarocks.logical_type.";
const TABLE_KEY_KIND_PROPERTY: &str = "novarocks.table.key_kind";
pub(crate) const TABLE_KEY_COLUMNS_PROPERTY: &str = "novarocks.table.key_columns";
const COLUMN_AGGREGATION_PROPERTY_PREFIX: &str = "novarocks.column_agg.";
const S3_NAMESPACE_MARKER_FILE: &str = ".novarocks_namespace";

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

    pub(crate) fn object_store_config(
        &self,
    ) -> Option<&crate::fs::object_store::ObjectStoreConfig> {
        self.s3_config.as_ref()
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

    /// Drop the cached `IcebergLoadedTable` for `(namespace, table_name)` so
    /// the next `load_table` call re-reads the metadata. Used by the
    /// standalone INSERT / OVERWRITE / DELETE flows after a successful
    /// commit so subsequent SELECTs see the new snapshot.
    pub(crate) fn invalidate_table_cache(&self, namespace_name: &str, table_name: &str) {
        if let (Ok(ns), Ok(tbl)) = (
            normalize_identifier(namespace_name),
            normalize_identifier(table_name),
        ) {
            if let Ok(mut cache) = self.table_cache.write() {
                cache.remove(&(ns.clone(), tbl.clone()));
            }
            if let Ok(mut cache) = self.data_files_cache.write() {
                cache
                    .retain(|(cached_ns, cached_tbl, _), _| cached_ns != &ns || cached_tbl != &tbl);
            }
        }
    }

    pub(crate) fn cached_data_files(
        &self,
        namespace_name: &str,
        table_name: &str,
        snapshot_id: Option<i64>,
    ) -> Result<Option<Vec<DataFileWithStats>>, String> {
        let ns = normalize_identifier(namespace_name)?;
        let tbl = normalize_identifier(table_name)?;
        let cache = self
            .data_files_cache
            .read()
            .map_err(|e| format!("iceberg data-file cache lock: {e}"))?;
        Ok(cache.get(&(ns, tbl, snapshot_id)).cloned())
    }

    pub(crate) fn cache_data_files(
        &self,
        namespace_name: &str,
        table_name: &str,
        snapshot_id: Option<i64>,
        data_files: Vec<DataFileWithStats>,
    ) -> Result<(), String> {
        let ns = normalize_identifier(namespace_name)?;
        let tbl = normalize_identifier(table_name)?;
        let mut cache = self
            .data_files_cache
            .write()
            .map_err(|e| format!("iceberg data-file cache lock: {e}"))?;
        cache.insert((ns, tbl, snapshot_id), data_files);
        Ok(())
    }
}

pub(crate) fn create_namespace(
    entry: &IcebergCatalogEntry,
    namespace_name: &str,
) -> Result<(), String> {
    let ns_name = normalize_identifier(namespace_name)?;
    if matches!(entry.kind, IcebergCatalogKind::Rest) {
        let namespace = NamespaceIdent::new(ns_name);
        let catalog = block_on_iceberg(async { build_rest_catalog(entry).await })??;
        return block_on_iceberg(async {
            catalog.create_namespace(&namespace, HashMap::new()).await
        })
        .map_err(|e| format!("create REST namespace runtime: {e}"))?
        .map(|_| ())
        .map_err(|e| format!("create REST namespace {namespace}: {e}"));
    }
    if let Some(s3_config) = &entry.s3_config {
        let op = crate::fs::object_store::build_oss_operator(s3_config)
            .map_err(|e| format!("build S3 operator for namespace create: {e}"))?;
        let marker_key = s3_namespace_marker_key(entry, &ns_name)?;
        block_on_iceberg(async {
            op.write(&marker_key, Vec::<u8>::new())
                .await
                .map_err(|e| format!("write namespace marker {marker_key}: {e}"))
        })??;
    } else {
        let ns_dir = entry.warehouse_path.join(&ns_name);
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
    if matches!(entry.kind, IcebergCatalogKind::Rest) {
        let namespace = NamespaceIdent::new(ns_name);
        let catalog = block_on_iceberg(async { build_rest_catalog(entry).await })??;
        return block_on_iceberg(async { catalog.namespace_exists(&namespace).await })
            .map_err(|e| format!("check REST namespace runtime: {e}"))?
            .map_err(|e| format!("check REST namespace failed: {e}"));
    }
    if let Some(s3_config) = &entry.s3_config {
        let op = crate::fs::object_store::build_oss_operator(s3_config)
            .map_err(|e| format!("build S3 operator for namespace check: {e}"))?;
        let ns_prefix = s3_namespace_prefix(entry, &ns_name)?;
        let marker_key = format!("{ns_prefix}{S3_NAMESPACE_MARKER_FILE}");
        block_on_iceberg(async {
            if op.stat(&marker_key).await.is_ok() {
                return Ok(true);
            }
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
    if matches!(entry.kind, IcebergCatalogKind::Rest) {
        let namespace = NamespaceIdent::new(ns_name);
        let catalog = block_on_iceberg(async { build_rest_catalog(entry).await })??;
        return block_on_iceberg(async { catalog.drop_namespace(&namespace).await })
            .map_err(|e| format!("drop REST namespace runtime: {e}"))?
            .map_err(|e| format!("drop REST namespace {namespace}: {e}"));
    }
    if let Some(s3_config) = &entry.s3_config {
        let op = crate::fs::object_store::build_oss_operator(s3_config)
            .map_err(|e| format!("build S3 operator for namespace drop: {e}"))?;
        let marker_key = s3_namespace_marker_key(entry, &ns_name)?;
        block_on_iceberg(async {
            match op.delete(&marker_key).await {
                Ok(()) => Ok(()),
                Err(err) if err.kind() == opendal::ErrorKind::NotFound => Ok(()),
                Err(err) => Err(format!("delete namespace marker {marker_key}: {err}")),
            }
        })?
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

fn s3_namespace_prefix(entry: &IcebergCatalogEntry, ns_name: &str) -> Result<String, String> {
    let (_, root_prefix) =
        crate::connector::iceberg::catalog::add_files::parse_s3_path(&entry.warehouse_uri)
            .map_err(|e| format!("parse warehouse URI: {e}"))?;
    let root = root_prefix.trim_matches('/');
    if root.is_empty() {
        Ok(format!("{ns_name}/"))
    } else {
        Ok(format!("{root}/{ns_name}/"))
    }
}

fn s3_namespace_marker_key(entry: &IcebergCatalogEntry, ns_name: &str) -> Result<String, String> {
    Ok(format!(
        "{}{}",
        s3_namespace_prefix(entry, ns_name)?,
        S3_NAMESPACE_MARKER_FILE
    ))
}

fn s3_table_prefix(
    entry: &IcebergCatalogEntry,
    ns_name: &str,
    table_name: &str,
) -> Result<String, String> {
    Ok(format!(
        "{}{}/",
        s3_namespace_prefix(entry, ns_name)?,
        table_name.trim_matches('/')
    ))
}

pub(crate) fn list_tables(
    entry: &IcebergCatalogEntry,
    namespace_name: &str,
) -> Result<Vec<String>, String> {
    let ns_name = normalize_identifier(namespace_name)?;
    if matches!(entry.kind, IcebergCatalogKind::Rest) {
        let namespace = NamespaceIdent::new(ns_name);
        let catalog = block_on_iceberg(async { build_rest_catalog(entry).await })??;
        let mut tables = block_on_iceberg(async { catalog.list_tables(&namespace).await })
            .map_err(|e| format!("list REST tables runtime failed: {e}"))?
            .map_err(|e| format!("list REST tables for namespace {namespace}: {e}"))?
            .into_iter()
            .map(|ident| ident.name)
            .collect::<Vec<_>>();
        tables.sort();
        return Ok(tables);
    }
    if let Some(s3_config) = &entry.s3_config {
        let op = crate::fs::object_store::build_oss_operator(s3_config)
            .map_err(|e| format!("build S3 operator for list tables: {e}"))?;
        let (_, root_prefix) =
            crate::connector::iceberg::catalog::add_files::parse_s3_path(&entry.warehouse_uri)
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
    partition_fields: &[crate::sql::parser::ast::IcebergPartitionFieldExpr],
    properties: &[(String, String)],
) -> Result<(), String> {
    let namespace = NamespaceIdent::new(normalize_identifier(namespace_name)?);
    let table_name = normalize_identifier(table_name)?;
    entry.invalidate_table_cache(namespace_name, &table_name);
    let (format_version, mut all_properties) = extract_table_format_version_property(properties)?;
    let schema = build_iceberg_schema(columns, format_version)?;
    let partition_spec = crate::connector::iceberg::partition_spec::build_initial_partition_spec(
        &schema,
        partition_fields,
    )?;
    all_properties.extend(build_logical_type_properties(columns)?);
    all_properties.extend(build_table_semantics_properties(columns, key_desc)?);
    // The iceberg-rust REST client serialises only the `properties` map of
    // TableCreation; the typed `format_version` builder field never makes it
    // to the wire, so v3 tables would otherwise be created as v2 on REST.
    // Re-insert `format-version` into the property list for REST only.
    // Hadoop / Memory catalogs route through `TableMetadataBuilder::from_
    // table_creation`, which calls `set_properties` and rejects reserved
    // properties (`format-version` is reserved); the typed `format_version`
    // builder field is the supported channel there.
    if matches!(entry.kind, IcebergCatalogKind::Rest) {
        all_properties.push((
            "format-version".to_string(),
            format!("{}", format_version as u8),
        ));
    }
    let table_creation = TableCreation::builder()
        .name(table_name)
        .schema(schema)
        .properties(all_properties)
        .format_version(format_version);
    let table_creation = if let Some(spec) = partition_spec {
        table_creation.partition_spec(spec).build()
    } else {
        table_creation.build()
    };

    let catalog = build_iceberg_catalog(entry)?;
    // For Hadoop/Memory catalogs, ensure the namespace exists before table creation.
    // REST catalogs manage namespace separately via CREATE DATABASE.
    if !matches!(entry.kind, IcebergCatalogKind::Rest) {
        let _ =
            block_on_iceberg(async { catalog.create_namespace(&namespace, HashMap::new()).await });
    }
    block_on_iceberg(async { catalog.create_table(&namespace, table_creation).await })
        .map_err(|e| format!("create iceberg table runtime failed: {e}"))?
        .map_err(|e| format!("create iceberg table failed: {e}"))?;
    Ok(())
}

pub(crate) fn alter_partition_spec(
    entry: &IcebergCatalogEntry,
    namespace_name: &str,
    table_name: &str,
    stmt: crate::sql::parser::ast::AlterIcebergPartitionSpecStmt,
) -> Result<(), String> {
    use iceberg::{TableCommit, TableRequirement, TableUpdate};

    let namespace = NamespaceIdent::new(normalize_identifier(namespace_name)?);
    let table_name = normalize_identifier(table_name)?;
    let catalog = build_iceberg_catalog(entry)?;
    let ident = TableIdent::new(namespace, table_name.clone());
    let table = block_on_iceberg(async { catalog.load_table(&ident).await })
        .map_err(|e| format!("load iceberg table runtime failed: {e}"))?
        .map_err(|e| format!("load iceberg table {ident}: {e}"))?;
    let metadata = table.metadata();
    let base_default_spec_id = metadata.default_partition_spec_id();
    let schema = metadata.current_schema();
    let current = metadata.default_partition_spec();
    let change = match &stmt {
        crate::sql::parser::ast::AlterIcebergPartitionSpecStmt::AddPartitionColumn {
            field,
            ..
        } => crate::connector::iceberg::partition_spec::PartitionSpecChange::Add(field),
        crate::sql::parser::ast::AlterIcebergPartitionSpecStmt::DropPartitionColumn {
            field,
            ..
        } => crate::connector::iceberg::partition_spec::PartitionSpecChange::Drop(field),
    };
    let evolved = crate::connector::iceberg::partition_spec::build_evolved_partition_spec(
        schema.as_ref(),
        current,
        change,
    )?;

    let commit = TableCommit::builder()
        .ident(ident.clone())
        .requirements(vec![TableRequirement::DefaultSpecIdMatch {
            default_spec_id: base_default_spec_id,
        }])
        .updates(vec![
            TableUpdate::AddSpec { spec: evolved },
            TableUpdate::SetDefaultSpec { spec_id: -1 },
        ])
        .build();
    block_on_iceberg(async { catalog.update_table(commit).await })
        .map_err(|e| format!("alter iceberg partition spec runtime failed: {e}"))?
        .map_err(|e| format!("alter iceberg partition spec failed: {e}"))?;
    entry.invalidate_table_cache(namespace_name, &table_name);
    Ok(())
}

pub(crate) fn drop_table(
    entry: &IcebergCatalogEntry,
    namespace_name: &str,
    table_name: &str,
) -> Result<(), String> {
    let ns_name = normalize_identifier(namespace_name)?;
    let tbl_name = normalize_identifier(table_name)?;

    entry.invalidate_table_cache(&ns_name, &tbl_name);

    if matches!(entry.kind, IcebergCatalogKind::Rest) {
        let ident = TableIdent::from_strs([ns_name.as_str(), tbl_name.as_str()])
            .map_err(|e| format!("build REST table ident: {e}"))?;
        let catalog = block_on_iceberg(async { build_rest_catalog(entry).await })??;
        return block_on_iceberg(async { catalog.drop_table(&ident).await })
            .map_err(|e| format!("drop REST iceberg table runtime failed: {e}"))?
            .map_err(|e| format!("drop REST iceberg table {ident}: {e}"));
    }

    if let Some(s3_config) = &entry.s3_config {
        let op = crate::fs::object_store::build_oss_operator(s3_config)
            .map_err(|e| format!("build S3 operator for table drop: {e}"))?;
        let table_prefix = s3_table_prefix(entry, &ns_name, &tbl_name)?;
        if table_prefix.trim_matches('/').is_empty() {
            return Err(format!(
                "refuse to drop S3 iceberg table {ns_name}.{tbl_name}: resolved empty table prefix"
            ));
        }
        block_on_iceberg(async {
            op.remove_all(&table_prefix)
                .await
                .map_err(|e| format!("remove S3 table prefix {table_prefix}: {e}"))
        })?
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

    let table = if matches!(entry.kind, IcebergCatalogKind::Rest) {
        let catalog = block_on_iceberg(async { build_rest_catalog(entry).await })??;
        let ident = TableIdent::from_strs([ns_name.as_str(), tbl_name.as_str()])
            .map_err(|e| format!("build REST table ident: {e}"))?;
        block_on_iceberg(async { catalog.load_table(&ident).await })
            .map_err(|e| format!("load REST iceberg table runtime failed: {e}"))?
            .map_err(|e| format!("load REST iceberg table {ident}: {e}"))?
    } else if let Some(s3_config) = &entry.s3_config {
        // S3 path: discover metadata from S3 directly
        let op = crate::fs::object_store::build_oss_operator(s3_config)
            .map_err(|e| format!("build S3 operator for load_table: {e}"))?;
        let (_, root_prefix) =
            crate::connector::iceberg::catalog::add_files::parse_s3_path(&entry.warehouse_uri)
                .map_err(|e| format!("parse warehouse URI: {e}"))?;
        let meta_prefix = format!(
            "{}/{}/{}/metadata/",
            root_prefix.trim_end_matches('/'),
            ns_name,
            tbl_name
        );

        // Find the latest metadata JSON — prefer Hadoop-catalog format (`vN.metadata.json`)
        // which is the canonical format written by HadoopFileSystemCatalog, with fallback
        // to the internal format (`{version}-{uuid}.metadata.json`) for pre-migration tables.
        let (metadata_file_name, metadata_bytes) = block_on_iceberg(async {
            let entries = op
                .list(&meta_prefix)
                .await
                .map_err(|e| format!("list metadata dir {meta_prefix}: {e}"))?;
            let file_names: Vec<String> = entries.iter().map(|e| e.name().to_string()).collect();
            let latest = choose_latest_metadata_filename(&file_names)
                .map_err(|_| format!("no metadata files for {ns_name}.{tbl_name}"))?;
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
            crate::connector::iceberg::catalog::s3_storage::S3StorageFactory::from_catalog_properties(&entry.properties)
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
    let iceberg_schema = table.metadata().current_schema();
    let arrow_schema = schema_to_arrow_schema(iceberg_schema)
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
            let nested = iceberg_schema
                .field_by_name(field.name())
                .ok_or_else(|| format!("iceberg column `{}` missing from schema", field.name()))?;
            // Variant columns surface as Struct{metadata, value} in the
            // iceberg arrow schema (PATCH 6), but NovaRocks carries variants
            // internally as LargeBinary `[size:u32 LE | metadata | value]`.
            // The data_writer's transform_variant_columns_for_write splits
            // the LargeBinary into the Struct shape right before
            // ParquetWriter::write, so we expose LargeBinary at the
            // ColumnDef level for INSERT-side literal building.
            let is_variant = matches!(
                nested.field_type.as_ref(),
                iceberg::spec::Type::Primitive(iceberg::spec::PrimitiveType::Variant)
            );
            let data_type = if is_variant {
                DataType::LargeBinary
            } else {
                apply_logical_type_override(field.data_type(), logical_types.get(&field_name))
            };
            Ok(ColumnDef {
                name: field.name().clone(),
                data_type,
                nullable: field.is_nullable(),
                write_default: nested.write_default.clone(),
            })
        })
        .collect::<Result<Vec<_>, String>>()?;

    let loaded = IcebergLoadedTable {
        table,
        columns,
        logical_types,
        key_desc,
        column_aggregations,
        object_store_config: entry.s3_config.clone(),
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
    reject_unsupported_iceberg_table_semantics(&loaded)?;
    let batch = build_insert_batch(&loaded, rows)?;

    let catalog = build_iceberg_catalog(entry)?;

    // For Hadoop/Memory catalogs: ensure namespace exists and register the table
    // by its metadata location so the catalog can resolve it for the commit.
    // REST catalogs already track tables through the REST API; skip registration.
    if !matches!(entry.kind, IcebergCatalogKind::Rest) {
        let ns = NamespaceIdent::new(normalize_identifier(namespace_name)?);
        let _ = block_on_iceberg(async { catalog.create_namespace(&ns, HashMap::new()).await });
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
            catalog
                .register_table(&table_ident, metadata_location)
                .await
        });
    }

    block_on_iceberg(async {
        let data_files = write_record_batches_as_data_files(&loaded.table, [batch])
            .await
            .map_err(|e| iceberg::Error::new(iceberg::ErrorKind::DataInvalid, e))?;
        let tx = Transaction::new(&loaded.table);
        let tx = tx.fast_append().add_data_files(data_files).apply(tx)?;
        tx.commit(catalog.as_ref()).await
    })
    .map_err(|e| format!("insert iceberg rows runtime failed: {e}"))?
    .map_err(|e| format!("insert iceberg rows failed: {e}"))?;

    entry.invalidate_table_cache(namespace_name, table_name);

    Ok(())
}

fn reject_unsupported_iceberg_table_semantics(loaded: &IcebergLoadedTable) -> Result<(), String> {
    if let Some(key_desc) = loaded.key_desc.as_ref()
        && key_desc.kind != TableKeyKind::Duplicate
    {
        return Err(format!(
            "iceberg INSERT does not support {:?} key table semantics",
            key_desc.kind
        ));
    }
    if !loaded.column_aggregations.is_empty() {
        return Err("iceberg INSERT does not support aggregate column semantics".to_string());
    }
    Ok(())
}

/// Result of extracting data files with column-level statistics from Iceberg manifests.
#[derive(Clone)]
pub(crate) struct DataFileWithStats {
    pub path: String,
    pub size: i64,
    pub record_count: Option<i64>,
    pub column_stats: Option<HashMap<String, crate::sql::catalog::IcebergColumnStats>>,
    pub partition_spec_id: Option<i32>,
    pub partition_key: Option<String>,
    pub partition_values: Option<iceberg::spec::Struct>,
    pub manifest_path: Option<String>,
    pub partition_field_values: Vec<crate::sql::catalog::IcebergPartitionFieldValue>,
    /// Iceberg v3 row-lineage: first row id assigned to this data file.
    pub first_row_id: Option<i64>,
    /// Iceberg v3 row-lineage: data sequence number of the manifest entry this
    /// file belongs to.  Falls back to the manifest file's sequence number when
    /// the entry itself does not carry one (e.g. V1/V2 manifests).
    pub data_sequence_number: Option<i64>,
    pub delete_files: Vec<crate::sql::catalog::IcebergDeleteFileInfo>,
}

fn iceberg_partition_field_values(
    metadata: &TableMetadata,
    spec_id: i32,
    partition: &iceberg::spec::Struct,
) -> Result<Vec<crate::sql::catalog::IcebergPartitionFieldValue>, String> {
    let Some(spec) = metadata.partition_spec_by_id(spec_id) else {
        return Err(format!(
            "iceberg table metadata missing partition spec id {spec_id}"
        ));
    };
    let schema = metadata.current_schema();
    let mut values = Vec::with_capacity(spec.fields().len());
    for (idx, field) in spec.fields().iter().enumerate() {
        let source_column = schema
            .field_by_id(field.source_id)
            .map(|source| source.name.clone())
            .unwrap_or_else(|| format!("#{}", field.source_id));
        let value = partition
            .fields()
            .get(idx)
            .and_then(|literal| literal.as_ref())
            .and_then(iceberg_partition_value_from_literal);
        values.push(crate::sql::catalog::IcebergPartitionFieldValue {
            source_column,
            field_name: field.name.clone(),
            transform: iceberg_partition_transform_name(&field.transform),
            value,
        });
    }
    Ok(values)
}

fn iceberg_partition_transform_name(transform: &Transform) -> String {
    match transform {
        Transform::Identity => "identity".to_string(),
        other => format!("{:?}", other).to_ascii_lowercase(),
    }
}

fn iceberg_partition_value_from_literal(
    literal: &IcebergLiteral,
) -> Option<crate::sql::catalog::IcebergPartitionValue> {
    let IcebergLiteral::Primitive(value) = literal else {
        return None;
    };
    match value {
        PrimitiveLiteral::Boolean(v) => {
            Some(crate::sql::catalog::IcebergPartitionValue::Boolean(*v))
        }
        PrimitiveLiteral::Int(v) => Some(crate::sql::catalog::IcebergPartitionValue::Int32(*v)),
        PrimitiveLiteral::Long(v) => Some(crate::sql::catalog::IcebergPartitionValue::Int64(*v)),
        PrimitiveLiteral::Float(v) => Some(crate::sql::catalog::IcebergPartitionValue::Float(v.0)),
        PrimitiveLiteral::Double(v) => {
            Some(crate::sql::catalog::IcebergPartitionValue::Double(v.0))
        }
        PrimitiveLiteral::String(v) => Some(crate::sql::catalog::IcebergPartitionValue::String(
            v.clone(),
        )),
        PrimitiveLiteral::Binary(v) => Some(crate::sql::catalog::IcebergPartitionValue::Binary(
            v.clone(),
        )),
        PrimitiveLiteral::Int128(_)
        | PrimitiveLiteral::UInt128(_)
        | PrimitiveLiteral::AboveMax
        | PrimitiveLiteral::BelowMin => None,
    }
}

fn equality_delete_column_names_for_field_ids(
    file_path: &str,
    equality_ids: Option<Vec<i32>>,
    field_id_to_name: &HashMap<i32, String>,
) -> Result<Vec<String>, String> {
    let equality_ids = equality_ids
        .ok_or_else(|| format!("iceberg equality-delete file {file_path} missing equality_ids"))?;
    if equality_ids.is_empty() {
        return Err(format!(
            "iceberg equality-delete file {file_path} has empty equality_ids"
        ));
    }
    equality_ids
        .iter()
        .map(|id| {
            field_id_to_name.get(id).cloned().ok_or_else(|| {
                format!("iceberg equality-delete file {file_path} references unknown field id {id}")
            })
        })
        .collect()
}

pub(crate) fn current_equality_delete_column_names(
    table: &iceberg::table::Table,
) -> Result<Vec<String>, String> {
    use iceberg::spec::{DataContentType, DataFileFormat, ManifestContentType, ManifestStatus};

    let metadata = table.metadata();
    let snapshot = match metadata.current_snapshot() {
        Some(s) => s,
        None => return Ok(Vec::new()),
    };
    let schema = metadata.current_schema();
    let field_id_to_name: HashMap<i32, String> = schema
        .as_struct()
        .fields()
        .iter()
        .map(|f| (f.id, f.name.clone()))
        .collect();
    let file_io = table.file_io();

    block_on_iceberg(async {
        let manifest_list = snapshot
            .load_manifest_list(file_io, metadata)
            .await
            .map_err(|e| format!("load manifest list: {e}"))?;
        let mut columns = Vec::new();
        for manifest_file in manifest_list.entries() {
            if manifest_file.content != ManifestContentType::Deletes {
                continue;
            }
            let manifest = manifest_file
                .load_manifest(file_io)
                .await
                .map_err(|e| format!("load manifest: {e}"))?;
            for entry in manifest.entries() {
                if entry.status == ManifestStatus::Deleted {
                    continue;
                }
                let df = entry.data_file();
                if df.content_type() != DataContentType::EqualityDeletes {
                    continue;
                }
                if df.file_format() != DataFileFormat::Parquet {
                    return Err(format!(
                        "unsupported iceberg equality-delete file format {:?}: {}",
                        df.file_format(),
                        df.file_path()
                    ));
                }
                columns.extend(equality_delete_column_names_for_field_ids(
                    df.file_path(),
                    df.equality_ids(),
                    &field_id_to_name,
                )?);
            }
        }
        Ok(columns)
    })
    .map_err(|e| format!("extract equality delete columns runtime: {e}"))?
}

/// Extract data file paths, sizes, row counts, and per-column statistics from
/// Iceberg manifest entries for a specific snapshot.
///
/// This reads the manifest list from the given snapshot, loads each data
/// manifest, and collects per-column stats (null counts, column sizes,
/// lower/upper bounds) mapped to column names via the snapshot's own schema.
pub(crate) fn extract_data_files_with_stats_at(
    table: &iceberg::table::Table,
    snapshot_id: i64,
) -> Result<Vec<DataFileWithStats>, String> {
    let metadata = table.metadata();
    let read_snapshot =
        crate::connector::iceberg::read::build_read_snapshot_at(table, snapshot_id)?;
    read_snapshot
        .files
        .into_iter()
        .map(|file| {
            let partition_field_values =
                match (file.partition_spec_id, file.partition_values.as_ref()) {
                    (Some(spec_id), Some(partition_values)) => {
                        iceberg_partition_field_values(metadata, spec_id, partition_values)?
                    }
                    _ => Vec::new(),
                };
            let delete_files = file
                .deletes
                .into_iter()
                .map(read_delete_to_catalog_delete)
                .collect::<Result<Vec<_>, _>>()?;
            Ok(DataFileWithStats {
                path: file.path,
                size: file.size,
                record_count: file.record_count,
                column_stats: file.column_stats,
                partition_spec_id: file.partition_spec_id,
                partition_key: file.partition_key,
                partition_values: file.partition_values,
                manifest_path: file.manifest_path,
                partition_field_values,
                first_row_id: file.first_row_id,
                data_sequence_number: file.data_sequence_number,
                delete_files,
            })
        })
        .collect()
}

/// Extract data file paths, sizes, row counts, and per-column statistics from
/// Iceberg manifest entries for the current snapshot.
///
/// This reads the manifest list from the current snapshot, loads each data
/// manifest, and collects per-column stats (null counts, column sizes,
/// lower/upper bounds) mapped to column names via the table schema.
///
/// If no snapshot exists the result is an empty vec.
pub(crate) fn extract_data_files_with_stats(
    table: &iceberg::table::Table,
) -> Result<Vec<DataFileWithStats>, String> {
    match table.metadata().current_snapshot() {
        Some(s) => extract_data_files_with_stats_at(table, s.snapshot_id()),
        None => Ok(Vec::new()),
    }
}

fn read_delete_to_catalog_delete(
    delete_file: crate::connector::iceberg::read::IcebergReadDeleteFile,
) -> Result<crate::sql::catalog::IcebergDeleteFileInfo, String> {
    use crate::sql::catalog::{
        IcebergDeleteFileContent, IcebergDeleteFileFormat, IcebergDeleteFileInfo,
    };

    let file_format = match delete_file.file_format {
        crate::connector::iceberg::read::IcebergReadDeleteFormat::Parquet => {
            IcebergDeleteFileFormat::Parquet
        }
        crate::connector::iceberg::read::IcebergReadDeleteFormat::Puffin => {
            IcebergDeleteFileFormat::Puffin
        }
    };
    let (file_content, equality_column_names, equality_field_ids) = match delete_file.kind {
        crate::connector::iceberg::read::IcebergReadDeleteKind::Position => {
            (IcebergDeleteFileContent::Position, Vec::new(), Vec::new())
        }
        crate::connector::iceberg::read::IcebergReadDeleteKind::Equality { equality_field_ids } => {
            if file_format != IcebergDeleteFileFormat::Parquet {
                return Err(format!(
                    "iceberg equality-delete file {} must use Parquet format",
                    delete_file.path
                ));
            }
            (
                IcebergDeleteFileContent::Equality,
                Vec::new(),
                equality_field_ids,
            )
        }
    };

    Ok(IcebergDeleteFileInfo {
        path: delete_file.path,
        file_format,
        file_content,
        length: delete_file.length,
        content_offset: delete_file.content_offset,
        content_size_in_bytes: delete_file.content_size_in_bytes,
        sequence_number: delete_file.sequence_number,
        partition_spec_id: delete_file.partition_spec_id,
        partition_key: delete_file.partition_key,
        equality_column_names,
        equality_field_ids,
    })
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

pub(crate) fn build_catalog_entry(
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
    let kind = match props.get("iceberg.catalog.type") {
        None => IcebergCatalogKind::Hadoop,
        Some(v) if v.eq_ignore_ascii_case("hadoop") => IcebergCatalogKind::Hadoop,
        Some(v) if v.eq_ignore_ascii_case("memory") => IcebergCatalogKind::Memory,
        Some(v) if v.eq_ignore_ascii_case("rest") => IcebergCatalogKind::Rest,
        Some(v) => {
            return Err(format!(
                "standalone iceberg catalog supports iceberg.catalog.type=memory|hadoop|rest, got {v}"
            ));
        }
    };

    if matches!(kind, IcebergCatalogKind::Rest) {
        return build_rest_catalog_entry(&mut props);
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
        let s3_factory = crate::connector::iceberg::catalog::s3_storage::S3StorageFactory::from_catalog_properties(properties)
            .ok_or_else(|| {
            "S3 iceberg catalog requires aws.s3.endpoint, aws.s3.access_key, aws.s3.secret_key"
                .to_string()
        })?;
        let (bucket, _root_prefix) =
            crate::connector::iceberg::catalog::add_files::parse_s3_path(&raw_warehouse)
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
        kind,
        warehouse_uri,
        rest_uri: None,
        properties: sorted_properties(&props),
        s3_config,
        warehouse_path,
        table_cache: Arc::new(std::sync::RwLock::new(HashMap::new())),
        data_files_cache: Arc::new(std::sync::RwLock::new(HashMap::new())),
    };

    Ok(entry)
}

/// Build an [`IcebergCatalogEntry`] for `iceberg.catalog.type = rest`. The
/// REST flavor differs from Hadoop in two ways:
///
/// 1. **Warehouse is server-resolved.** A REST catalog returns its own
///    `warehouse` via `GET /v1/config` (overrides + defaults). The user
///    MAY pre-declare `iceberg.catalog.warehouse` (some servers forward
///    this back to the storage backend), but it is not required.
/// 2. **`uri` is required.** It points at the REST endpoint
///    (`http(s)://host:port`), not at a filesystem warehouse.
///
/// The S3 / object-store properties (`aws.s3.endpoint` / `aws.s3.access_key`
/// / `aws.s3.secret_key`) are still read here so that NovaRocks can hand the
/// `RestCatalog` a `StorageFactory` capable of opening the data files the
/// server points at. When those properties are absent, the catalog still
/// builds — it just operates against local filesystem paths.
fn build_rest_catalog_entry(
    props: &mut HashMap<String, String>,
) -> Result<IcebergCatalogEntry, String> {
    let uri = props
        .get("uri")
        .or_else(|| props.get("iceberg.catalog.uri"))
        .cloned()
        .ok_or_else(|| {
            "REST iceberg catalog requires `uri` property pointing at the REST endpoint".to_string()
        })?;
    let warehouse = props
        .get("warehouse")
        .or_else(|| props.get("iceberg.catalog.warehouse"))
        .cloned()
        .unwrap_or_default();

    // Optional S3 storage props — populated only when the user provided them.
    // The REST server may also vend storage credentials; that codepath is a
    // follow-up (Issue: REST credential vending).
    let raw_props: Vec<(String, String)> =
        props.iter().map(|(k, v)| (k.clone(), v.clone())).collect();
    let s3_config = if let Some(s3_factory) =
        crate::connector::iceberg::catalog::s3_storage::S3StorageFactory::from_catalog_properties(
            &raw_props,
        ) {
        let bucket = warehouse
            .strip_prefix("s3://")
            .or_else(|| warehouse.strip_prefix("s3a://"))
            .or_else(|| warehouse.strip_prefix("oss://"))
            .and_then(|rest| rest.split('/').next())
            .unwrap_or_default()
            .to_string();
        Some(crate::fs::object_store::ObjectStoreConfig {
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
        })
    } else {
        None
    };

    // No local warehouse_path for REST; allocate an empty placeholder so any
    // legacy hadoop-only code path that touches `entry.warehouse_path` fails
    // loudly rather than corrupting an unrelated directory.
    let warehouse_path = PathBuf::from("/__novarocks_rest_catalog_no_local_warehouse__");

    props.insert("type".to_string(), "iceberg".to_string());
    props.insert("iceberg.catalog.type".to_string(), "rest".to_string());
    props.insert("uri".to_string(), uri.clone());
    if !warehouse.is_empty() {
        props.insert("iceberg.catalog.warehouse".to_string(), warehouse.clone());
    }

    Ok(IcebergCatalogEntry {
        kind: IcebergCatalogKind::Rest,
        warehouse_uri: warehouse,
        rest_uri: Some(uri),
        properties: sorted_properties(props),
        s3_config,
        warehouse_path,
        table_cache: Arc::new(std::sync::RwLock::new(HashMap::new())),
        data_files_cache: Arc::new(std::sync::RwLock::new(HashMap::new())),
    })
}

/// Build a `HadoopFileSystemCatalog` that writes metadata in the Hadoop naming
/// convention (`v{N}.metadata.json` + `version-hint.text`).
pub(crate) fn build_hadoop_catalog(
    entry: &IcebergCatalogEntry,
) -> Result<crate::connector::iceberg::catalog::hadoop_catalog::HadoopFileSystemCatalog, String> {
    let storage_factory = build_storage_factory_for_entry(entry)?;
    let file_io = iceberg::io::FileIOBuilder::new(storage_factory).build();
    Ok(
        crate::connector::iceberg::catalog::hadoop_catalog::HadoopFileSystemCatalog::new(
            file_io,
            entry.warehouse_uri.clone(),
        ),
    )
}

/// Build an Iceberg REST `RestCatalog` for an entry whose
/// `kind == IcebergCatalogKind::Rest`. Performs the `GET /v1/config`
/// handshake against the REST endpoint declared by `entry.rest_uri`.
///
/// Asynchronous because the spec requires a config call before any other
/// REST operation. Synchronous callers should go through
/// [`build_iceberg_catalog`], which wraps this with `block_on_iceberg`.
#[allow(dead_code)] // Wired to engine flows in a follow-up; covered by mockito tests for now.
pub(crate) async fn build_rest_catalog(
    entry: &IcebergCatalogEntry,
) -> Result<iceberg_catalog_rest::RestCatalog, String> {
    use iceberg::CatalogBuilder;
    use iceberg_catalog_rest::{
        REST_CATALOG_PROP_URI, REST_CATALOG_PROP_WAREHOUSE, RestCatalogBuilder,
    };

    if !matches!(entry.kind, IcebergCatalogKind::Rest) {
        return Err(format!(
            "build_rest_catalog called on non-REST entry kind={:?}",
            entry.kind
        ));
    }
    let uri = entry.rest_uri.clone().ok_or_else(|| {
        "REST iceberg catalog entry missing rest_uri (CREATE EXTERNAL CATALOG must set `uri`)"
            .to_string()
    })?;

    // Carry through every user-supplied property except `type` (which is
    // NovaRocks-internal) so OAuth credentials, prefix, signing-region and
    // other RESTSessionCatalog options reach the iceberg-rust builder.
    let mut props: HashMap<String, String> = HashMap::new();
    for (k, v) in &entry.properties {
        if k == "type" {
            continue;
        }
        props.insert(k.clone(), v.clone());
    }
    props.insert(REST_CATALOG_PROP_URI.to_string(), uri);
    if !entry.warehouse_uri.is_empty() {
        props.insert(
            REST_CATALOG_PROP_WAREHOUSE.to_string(),
            entry.warehouse_uri.clone(),
        );
    }

    let storage_factory = build_storage_factory_for_entry(entry)?;
    let catalog_name = "rest".to_string();
    RestCatalogBuilder::default()
        .with_storage_factory(storage_factory)
        .load(catalog_name, props)
        .await
        .map_err(|e| format!("build REST iceberg catalog: {e}"))
}

fn build_storage_factory_for_entry(
    entry: &IcebergCatalogEntry,
) -> Result<Arc<dyn iceberg::io::StorageFactory>, String> {
    if entry.is_s3() {
        let s3_factory = crate::connector::iceberg::catalog::s3_storage::S3StorageFactory::from_catalog_properties(
            &entry.properties,
        )
        .ok_or_else(|| {
            "S3 iceberg catalog requires aws.s3.endpoint, aws.s3.access_key, aws.s3.secret_key"
                .to_string()
        })?;
        Ok(Arc::new(s3_factory))
    } else {
        Ok(Arc::new(iceberg::io::LocalFsStorageFactory))
    }
}

/// Synchronous dispatcher that returns an `Arc<dyn Catalog>` regardless of
/// whether the entry is Hadoop or REST. REST catalog construction is
/// asynchronous (one `/v1/config` handshake is required), so this helper
/// blocks on it via `block_on_iceberg`. Callers in synchronous engine
/// flows can swap `build_hadoop_catalog(...).map(Arc::new)` for
/// `build_iceberg_catalog(...)` to gain REST support transparently.
#[allow(dead_code)] // Will replace explicit build_hadoop_catalog calls in a follow-up.
pub(crate) fn build_iceberg_catalog(
    entry: &IcebergCatalogEntry,
) -> Result<Arc<dyn iceberg::Catalog>, String> {
    match entry.kind {
        IcebergCatalogKind::Hadoop | IcebergCatalogKind::Memory => {
            let hadoop = build_hadoop_catalog(entry)?;
            Ok(Arc::new(hadoop) as Arc<dyn iceberg::Catalog>)
        }
        IcebergCatalogKind::Rest => {
            let rest = block_on_iceberg(async { build_rest_catalog(entry).await })??;
            Ok(Arc::new(rest) as Arc<dyn iceberg::Catalog>)
        }
    }
}

pub(crate) fn block_on_iceberg<F>(future: F) -> Result<F::Output, String>
where
    F: Future,
{
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
    let ns = normalize_identifier(namespace_name)?;
    let tbl = normalize_identifier(table_name)?;
    let metadata_dir = entry.warehouse_path.join(&ns).join(&tbl).join("metadata");
    let read_dir = match std::fs::read_dir(&metadata_dir) {
        Ok(read_dir) => read_dir,
        // The Hadoop catalog stores table metadata under
        // `<warehouse>/<ns>/<tbl>/metadata/`. A missing directory means the
        // table no longer exists in this catalog (dropped, never created,
        // or pruned externally). Surface this as an "unknown table" error
        // so callers can distinguish absence from genuine I/O trouble —
        // notably, `register_external_tables_for_query_impl` uses the
        // distinction to evict a stale local-catalog entry instead of
        // hanging onto it.
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
            return Err(format!("unknown iceberg table {ns}.{tbl}"));
        }
        Err(e) => {
            return Err(format!(
                "read iceberg metadata dir {} failed: {e}",
                metadata_dir.display()
            ));
        }
    };
    let file_names: Vec<String> = read_dir
        .filter_map(|item| item.ok())
        .filter_map(|item| {
            item.file_name()
                .to_str()
                .filter(|name| name.ends_with(".metadata.json"))
                .map(|name| name.to_string())
        })
        .collect();
    let latest = choose_latest_metadata_filename(&file_names)
        .map_err(|_| format!("unknown iceberg table {ns}.{tbl}"))?;
    Ok(path_to_file_uri(&metadata_dir.join(latest)))
}

fn parse_internal_metadata_version(file_name: &str) -> Option<i32> {
    let base = file_name.strip_suffix(".metadata.json")?;
    let (version, uuid) = base.split_once('-')?;
    if uuid.is_empty() {
        return None;
    }
    version.parse::<i32>().ok()
}

fn parse_hadoop_metadata_version(file_name: &str) -> Option<i32> {
    let base = file_name.strip_suffix(".metadata.json")?;
    let version_str = base.strip_prefix('v')?;
    // Must be purely numeric (no dash, no UUID suffix) to distinguish from internal format.
    if version_str.contains('-') {
        return None;
    }
    version_str.parse::<i32>().ok()
}

/// Choose the latest metadata file from a list of file names found in the
/// metadata directory. Prefers Hadoop-catalog format (`v{N}.metadata.json`)
/// which is the canonical format written by `HadoopFileSystemCatalog`. Falls
/// back to the internal format (`{version}-{uuid}.metadata.json`) for
/// pre-migration tables.
fn choose_latest_metadata_filename(file_names: &[String]) -> Result<String, String> {
    // Prefer Hadoop-catalog format — canonical format written by HadoopFileSystemCatalog
    let mut hadoop: Vec<(i32, &str)> = file_names
        .iter()
        .filter_map(|name| parse_hadoop_metadata_version(name).map(|v| (v, name.as_str())))
        .collect();
    if !hadoop.is_empty() {
        hadoop.sort_by_key(|(v, _)| *v);
        return Ok(hadoop.last().unwrap().1.to_string());
    }
    // Fallback: internal format for pre-migration tables
    let mut internal: Vec<(i32, &str)> = file_names
        .iter()
        .filter_map(|name| parse_internal_metadata_version(name).map(|v| (v, name.as_str())))
        .collect();
    if !internal.is_empty() {
        internal.sort_by_key(|(v, _)| *v);
        return Ok(internal.last().unwrap().1.to_string());
    }
    Err("no iceberg metadata files found".to_string())
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

fn build_iceberg_schema(
    columns: &[TableColumnDef],
    format_version: FormatVersion,
) -> Result<Schema, String> {
    let mut next_nested_field_id =
        i32::try_from(columns.len() + 1).map_err(|_| "too many iceberg columns".to_string())?;
    let fields = columns
        .iter()
        .enumerate()
        .map(|(idx, column)| {
            let field_id =
                i32::try_from(idx + 1).map_err(|_| "too many iceberg columns".to_string())?;
            let iceberg_type =
                iceberg_type_for_sql_type(&column.data_type, &mut next_nested_field_id)?;
            // Honor NOT NULL: required = non-nullable, optional = nullable.
            let mut field = if column.nullable {
                NestedField::optional(field_id, &column.name, iceberg_type)
            } else {
                NestedField::required(field_id, &column.name, iceberg_type)
            };
            // Persist DEFAULT literal for v3; reject non-NULL defaults on v1/v2.
            if let Some(default_literal) = &column.default {
                if let Some(iceberg_lit) =
                    crate::connector::iceberg::default_value::default_literal_to_iceberg(
                        default_literal,
                        &column.data_type,
                    )?
                {
                    crate::connector::iceberg::default_value::require_v3_for_default(
                        format_version,
                        &Some(iceberg_lit.clone()),
                    )?;
                    field = field
                        .with_initial_default(iceberg_lit.clone())
                        .with_write_default(iceberg_lit);
                }
            }
            Ok(field.into())
        })
        .collect::<Result<Vec<_>, String>>()?;
    Schema::builder()
        .with_fields(fields)
        .build()
        .map_err(|e| format!("build iceberg schema failed: {e}"))
}

#[cfg(test)]
pub(crate) fn build_iceberg_schema_for_test(
    columns: &[TableColumnDef],
    format_version: FormatVersion,
) -> Result<Schema, String> {
    build_iceberg_schema(columns, format_version)
}

pub(crate) fn iceberg_type_for_sql_type(
    data_type: &SqlType,
    next_field_id: &mut i32,
) -> Result<Type, String> {
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
        SqlType::Binary => Type::Primitive(PrimitiveType::Binary),
        SqlType::Boolean => Type::Primitive(PrimitiveType::Boolean),
        SqlType::Date => Type::Primitive(PrimitiveType::Date),
        SqlType::DateTime => Type::Primitive(PrimitiveType::Timestamp),
        SqlType::Time => Type::Primitive(PrimitiveType::Time),
        SqlType::Variant => Type::Primitive(PrimitiveType::Variant),
        SqlType::Array(inner) => {
            let element_field_id = *next_field_id;
            *next_field_id += 1;
            Type::List(ListType::new(Arc::new(NestedField::optional(
                element_field_id,
                "element",
                iceberg_type_for_sql_type(inner, next_field_id)?,
            ))))
        }
        SqlType::Map(key, value) => {
            let key_field_id = *next_field_id;
            *next_field_id += 1;
            let value_field_id = *next_field_id;
            *next_field_id += 1;
            Type::Map(MapType::new(
                Arc::new(NestedField::required(
                    key_field_id,
                    "key",
                    iceberg_type_for_sql_type(key, next_field_id)?,
                )),
                Arc::new(NestedField::optional(
                    value_field_id,
                    "value",
                    iceberg_type_for_sql_type(value, next_field_id)?,
                )),
            ))
        }
        SqlType::Struct(fields) => Type::Struct(StructType::new(
            fields
                .iter()
                .map(|(name, field_type)| {
                    let field_id = *next_field_id;
                    *next_field_id += 1;
                    Ok(Arc::new(NestedField::optional(
                        field_id,
                        name,
                        iceberg_type_for_sql_type(field_type, next_field_id)?,
                    )))
                })
                .collect::<Result<Vec<_>, String>>()?,
        )),
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
                    Literal::Int(value) => Ok(Some(*value != 0)),
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
        DataType::LargeBinary => {
            // Variant columns: the literal extractor (`parse_json` arm in
            // `engine/sql_expr.rs::sqlparser_function_to_literal`) packs the
            // [size:u32 LE | metadata | value] payload as a Latin-1 String.
            // We unpack via the same convention `to_binary` uses.
            use arrow::array::LargeBinaryBuilder;
            let mut builder = LargeBinaryBuilder::new();
            for literal in values {
                match literal {
                    Literal::Null => builder.append_null(),
                    Literal::String(value) => builder
                        .append_value(crate::engine::sql_expr::latin1_string_to_bytes(value)?),
                    other => {
                        return Err(format!(
                            "literal {:?} is not valid for VARIANT column",
                            other
                        ));
                    }
                }
            }
            Ok(Arc::new(builder.finish()))
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
    Ok(i64::from(time.num_seconds_from_midnight()) * 1_000_000
        + i64::from(time.nanosecond() / 1_000))
}

fn extract_table_format_version_property(
    properties: &[(String, String)],
) -> Result<(FormatVersion, Vec<(String, String)>), String> {
    let mut format_version = FormatVersion::V2;
    let mut remaining = Vec::with_capacity(properties.len());
    for (key, value) in properties {
        if key.eq_ignore_ascii_case("format-version") {
            format_version = match value.trim() {
                "1" => FormatVersion::V1,
                "2" => FormatVersion::V2,
                "3" => FormatVersion::V3,
                other => {
                    return Err(format!(
                        "unsupported iceberg format-version `{other}`; expected 1, 2, or 3"
                    ));
                }
            };
        } else {
            remaining.push((key.clone(), value.clone()));
        }
    }
    Ok((format_version, remaining))
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

pub(crate) fn logical_type_property_key(column_name: &str) -> Result<String, String> {
    Ok(format!(
        "{LOGICAL_TYPE_PROPERTY_PREFIX}{}",
        normalize_identifier(column_name)?
    ))
}

pub(crate) fn column_aggregation_property_key(column_name: &str) -> Result<String, String> {
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

pub(crate) fn logical_type_property_value(data_type: &SqlType) -> Option<String> {
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
    if negative && parsed != i128::MIN {
        parsed = -parsed;
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

#[cfg(test)]
mod read_delete_conversion_tests {
    use super::read_delete_to_catalog_delete;
    use crate::connector::iceberg::read::{
        IcebergReadDeleteFile, IcebergReadDeleteFormat, IcebergReadDeleteKind,
    };
    use crate::sql::catalog::{IcebergDeleteFileContent, IcebergDeleteFileFormat};

    fn read_delete(
        file_format: IcebergReadDeleteFormat,
        kind: IcebergReadDeleteKind,
    ) -> IcebergReadDeleteFile {
        IcebergReadDeleteFile {
            path: "s3://bucket/table/delete-file".to_string(),
            file_format,
            kind,
            length: Some(128),
            content_offset: None,
            content_size_in_bytes: None,
            sequence_number: Some(7),
            partition_spec_id: Some(1),
            partition_key: Some("city=A".to_string()),
            referenced_data_file: None,
        }
    }

    #[test]
    fn parquet_equality_delete_carries_explicit_field_ids() {
        let delete_file = read_delete(
            IcebergReadDeleteFormat::Parquet,
            IcebergReadDeleteKind::Equality {
                equality_field_ids: vec![3, 1],
            },
        );

        let catalog_delete = read_delete_to_catalog_delete(delete_file).expect("convert");

        assert_eq!(catalog_delete.file_format, IcebergDeleteFileFormat::Parquet);
        assert_eq!(
            catalog_delete.file_content,
            IcebergDeleteFileContent::Equality
        );
        assert_eq!(catalog_delete.equality_field_ids, vec![3, 1]);
        assert!(catalog_delete.equality_column_names.is_empty());
    }

    #[test]
    fn puffin_position_delete_preserves_content_range() {
        let mut delete_file = read_delete(
            IcebergReadDeleteFormat::Puffin,
            IcebergReadDeleteKind::Position,
        );
        delete_file.content_offset = Some(64);
        delete_file.content_size_in_bytes = Some(512);

        let catalog_delete = read_delete_to_catalog_delete(delete_file).expect("convert");

        assert_eq!(catalog_delete.file_format, IcebergDeleteFileFormat::Puffin);
        assert_eq!(
            catalog_delete.file_content,
            IcebergDeleteFileContent::Position
        );
        assert_eq!(catalog_delete.content_offset, Some(64));
        assert_eq!(catalog_delete.content_size_in_bytes, Some(512));
        assert!(catalog_delete.equality_field_ids.is_empty());
    }

    #[test]
    fn puffin_equality_delete_is_rejected() {
        let delete_file = read_delete(
            IcebergReadDeleteFormat::Puffin,
            IcebergReadDeleteKind::Equality {
                equality_field_ids: vec![3],
            },
        );

        let err = read_delete_to_catalog_delete(delete_file).expect_err("reject puffin equality");

        assert!(err.contains("must use Parquet format"));
    }
}

#[cfg(test)]
mod data_file_with_stats_tests {
    use super::{DataFileWithStats, IcebergCatalogEntry, IcebergCatalogKind};
    use std::collections::HashMap;
    use std::path::PathBuf;
    use std::sync::{Arc, RwLock};

    fn data_file(path: &str) -> DataFileWithStats {
        DataFileWithStats {
            path: path.to_string(),
            size: 1024,
            record_count: Some(100),
            column_stats: None,
            partition_spec_id: None,
            partition_key: None,
            partition_values: None,
            manifest_path: None,
            partition_field_values: vec![],
            first_row_id: Some(7),
            data_sequence_number: Some(42),
            delete_files: vec![],
        }
    }

    /// Regression test: data_sequence_number must be threaded from the
    /// DataFileWithStats struct through to S3FileInfo.  The full
    /// extract_data_files_with_stats -> build_iceberg_table_def_with_data_files
    /// path is covered by Task 5's integration tests; this test validates the
    /// struct plumbing so a future refactor cannot drop the field silently.
    #[test]
    fn data_file_with_stats_carries_data_sequence_number() {
        let f = data_file("s3://bucket/data/part-0.parquet");
        assert_eq!(
            f.data_sequence_number,
            Some(42),
            "data_sequence_number must be preserved on DataFileWithStats"
        );
    }

    #[test]
    fn data_file_with_stats_data_sequence_number_none_for_non_iceberg() {
        let f = {
            let mut f = data_file("/local/data.parquet");
            f.record_count = None;
            f.size = 512;
            f.first_row_id = None;
            f.data_sequence_number = None;
            f
        };
        assert_eq!(
            f.data_sequence_number, None,
            "data_sequence_number should be None for non-Iceberg sources"
        );
    }

    #[test]
    fn data_file_cache_is_snapshot_scoped_and_table_invalidation_clears_it() {
        let entry = IcebergCatalogEntry {
            kind: IcebergCatalogKind::Hadoop,
            warehouse_uri: "file:///tmp/warehouse".to_string(),
            rest_uri: None,
            properties: vec![],
            s3_config: None,
            warehouse_path: PathBuf::from("/tmp/warehouse"),
            table_cache: Arc::new(RwLock::new(HashMap::new())),
            data_files_cache: Arc::new(RwLock::new(HashMap::new())),
        };
        entry
            .cache_data_files("Db1", "Tbl1", Some(7), vec![data_file("file:///a.parquet")])
            .expect("cache snapshot 7");
        entry
            .cache_data_files("Db1", "Tbl1", Some(8), vec![data_file("file:///b.parquet")])
            .expect("cache snapshot 8");

        assert_eq!(
            entry
                .cached_data_files("db1", "tbl1", Some(7))
                .expect("read snapshot 7")
                .expect("snapshot 7 cached")[0]
                .path,
            "file:///a.parquet"
        );
        assert_eq!(
            entry
                .cached_data_files("db1", "tbl1", Some(8))
                .expect("read snapshot 8")
                .expect("snapshot 8 cached")[0]
                .path,
            "file:///b.parquet"
        );

        entry.invalidate_table_cache("db1", "tbl1");

        assert!(
            entry
                .cached_data_files("db1", "tbl1", Some(7))
                .expect("read invalidated snapshot 7")
                .is_none()
        );
        assert!(
            entry
                .cached_data_files("db1", "tbl1", Some(8))
                .expect("read invalidated snapshot 8")
                .is_none()
        );
    }
}

#[cfg(test)]
mod equality_delete_dependency_tests {
    use super::equality_delete_column_names_for_field_ids;
    use std::collections::HashMap;

    #[test]
    fn equality_delete_column_names_follow_current_schema_field_ids() {
        let fields = HashMap::from([(1, "id".to_string()), (2, "category".to_string())]);
        let names =
            equality_delete_column_names_for_field_ids("delete.parquet", Some(vec![2, 1]), &fields)
                .expect("column names");

        assert_eq!(names, vec!["category".to_string(), "id".to_string()]);
    }

    #[test]
    fn equality_delete_column_names_reject_unknown_field_id() {
        let fields = HashMap::from([(1, "id".to_string())]);
        let err =
            equality_delete_column_names_for_field_ids("delete.parquet", Some(vec![7]), &fields)
                .expect_err("unknown field id");

        assert!(err.contains("unknown field id 7"));
    }
}

#[cfg(test)]
mod table_property_tests {
    use super::*;

    #[test]
    fn format_version_property_becomes_table_creation_field() {
        let props = vec![
            ("format-version".to_string(), "3".to_string()),
            ("write.row-lineage".to_string(), "true".to_string()),
        ];
        let (format_version, remaining) =
            extract_table_format_version_property(&props).expect("extract");

        assert_eq!(format_version, FormatVersion::V3);
        assert_eq!(
            remaining,
            vec![("write.row-lineage".to_string(), "true".to_string())]
        );
    }

    #[test]
    fn invalid_format_version_is_rejected() {
        let err = extract_table_format_version_property(&[(
            "format-version".to_string(),
            "9".to_string(),
        )])
        .expect_err("format-version=9 should fail");
        assert!(
            err.contains("unsupported iceberg format-version"),
            "error was: {err}"
        );
    }

    #[test]
    fn create_v2_table_with_non_null_default_rejected() {
        let columns = vec![crate::sql::parser::ast::TableColumnDef {
            name: "c".to_string(),
            data_type: crate::sql::parser::ast::SqlType::Int,
            nullable: true,
            aggregation: None,
            default: Some(crate::sql::parser::ast::DefaultLiteral::Int(5)),
        }];
        let err = build_iceberg_schema_for_test(&columns, FormatVersion::V2)
            .expect_err("v2 + default rejected");
        assert!(err.contains("format-version 3"));
    }

    #[test]
    fn create_v3_table_with_int_default_persists_literal() {
        let columns = vec![crate::sql::parser::ast::TableColumnDef {
            name: "c".to_string(),
            data_type: crate::sql::parser::ast::SqlType::Int,
            nullable: true,
            aggregation: None,
            default: Some(crate::sql::parser::ast::DefaultLiteral::Int(5)),
        }];
        let schema =
            build_iceberg_schema_for_test(&columns, FormatVersion::V3).expect("v3 + default ok");
        let field = schema.field_by_name("c").expect("c");
        let expected = iceberg::spec::Literal::Primitive(iceberg::spec::PrimitiveLiteral::Int(5));
        assert_eq!(field.initial_default.as_ref(), Some(&expected));
        assert_eq!(field.write_default.as_ref(), Some(&expected));
    }
}

#[cfg(test)]
mod rest_catalog_tests {
    //! Mocked unit tests for the REST catalog wiring. These tests use
    //! `mockito` to stand up a fake REST endpoint locally so we can verify
    //! the property-parsing → `IcebergCatalogEntry` → `build_iceberg_catalog`
    //! → `Arc<dyn iceberg::Catalog>` chain without depending on a Docker
    //! container or external service.
    //!
    //! The mock pattern mirrors `iceberg-catalog-rest`'s own internal tests
    //! (see its `src/catalog.rs` `mod tests` block).
    use std::sync::Arc;

    use iceberg::{Catalog, NamespaceIdent};
    use mockito::Server;

    use super::{
        IcebergCatalogEntry, IcebergCatalogKind, build_catalog_entry, build_iceberg_catalog,
        build_rest_catalog,
    };

    fn rest_props(uri: &str) -> Vec<(String, String)> {
        vec![
            ("type".to_string(), "iceberg".to_string()),
            ("iceberg.catalog.type".to_string(), "rest".to_string()),
            ("uri".to_string(), uri.to_string()),
        ]
    }

    /// Smallest config response a spec-compliant REST server returns to the
    /// initial `GET /v1/config` call. Empty overrides + defaults are valid.
    const EMPTY_CONFIG_BODY: &str = r#"{"overrides":{},"defaults":{}}"#;

    #[test]
    fn build_catalog_entry_accepts_rest_kind_with_uri() {
        let entry = build_catalog_entry(
            "ice_rest",
            &[
                ("type".to_string(), "iceberg".to_string()),
                ("iceberg.catalog.type".to_string(), "rest".to_string()),
                ("uri".to_string(), "http://localhost:8181".to_string()),
            ],
        )
        .expect("rest entry");
        assert_eq!(entry.kind, IcebergCatalogKind::Rest);
        assert_eq!(entry.rest_uri.as_deref(), Some("http://localhost:8181"));
        assert!(
            entry.warehouse_uri.is_empty(),
            "warehouse is optional for REST and resolved from /v1/config"
        );
    }

    #[test]
    fn build_catalog_entry_rejects_rest_without_uri() {
        let result = build_catalog_entry(
            "ice_rest",
            &[
                ("type".to_string(), "iceberg".to_string()),
                ("iceberg.catalog.type".to_string(), "rest".to_string()),
            ],
        );
        let err = result.map(|_| ()).expect_err("uri is required");
        assert!(
            err.contains("uri"),
            "error should mention uri requirement, got: {err}"
        );
    }

    #[test]
    fn build_catalog_entry_accepts_rest_with_warehouse() {
        let entry = build_catalog_entry(
            "ice_rest",
            &[
                ("iceberg.catalog.type".to_string(), "rest".to_string()),
                ("uri".to_string(), "http://localhost:8181".to_string()),
                ("warehouse".to_string(), "s3://demo/wh".to_string()),
            ],
        )
        .expect("rest entry with warehouse");
        assert_eq!(entry.kind, IcebergCatalogKind::Rest);
        assert_eq!(entry.warehouse_uri, "s3://demo/wh");
    }

    #[test]
    fn build_catalog_entry_rejects_unknown_catalog_type() {
        let err = build_catalog_entry(
            "ice",
            &[("iceberg.catalog.type".to_string(), "weird".to_string())],
        )
        .map(|_| ())
        .expect_err("unknown type should be rejected");
        assert!(err.contains("memory|hadoop|rest"), "{err}");
    }

    /// Confirms `build_rest_catalog` performs the spec-required
    /// `GET /v1/config` handshake against the configured `uri` before
    /// returning a usable `RestCatalog`.
    #[tokio::test]
    async fn build_rest_catalog_handshakes_v1_config() {
        let mut server = Server::new_async().await;
        let config_mock = server
            .mock("GET", "/v1/config")
            .with_status(200)
            .with_body(EMPTY_CONFIG_BODY)
            .create_async()
            .await;

        let entry =
            build_catalog_entry("ice_rest", &rest_props(&server.url())).expect("rest entry");
        let catalog = build_rest_catalog(&entry)
            .await
            .expect("build rest catalog");

        // Round-trip a list_namespaces call to force the catalog to issue its
        // first authenticated request, which forces the config handshake.
        let _ = mock_list_namespaces(&mut server, "[]").await;
        let namespaces = catalog
            .list_namespaces(None)
            .await
            .expect("list namespaces over mock");
        assert!(namespaces.is_empty(), "mock returns no namespaces");
        config_mock.assert_async().await;
    }

    /// Confirms the full property-parsing → entry → dispatcher chain returns
    /// an `Arc<dyn Catalog>` and that the dispatcher routes to the REST
    /// implementation (not Hadoop) when the entry's kind is REST.
    #[tokio::test]
    async fn build_iceberg_catalog_dispatches_rest_kind() {
        let mut server = Server::new_async().await;
        let _config_mock = server
            .mock("GET", "/v1/config")
            .with_status(200)
            .with_body(EMPTY_CONFIG_BODY)
            .create_async()
            .await;
        let _list_mock = mock_list_namespaces(&mut server, "[]").await;

        let entry =
            build_catalog_entry("ice_rest", &rest_props(&server.url())).expect("rest entry");
        let catalog: Arc<dyn Catalog> = tokio::task::spawn_blocking({
            let entry = entry.clone();
            move || build_iceberg_catalog(&entry)
        })
        .await
        .expect("blocking task")
        .expect("build_iceberg_catalog");

        let namespaces = catalog
            .list_namespaces(None)
            .await
            .expect("list namespaces via dispatcher");
        assert!(namespaces.is_empty());
    }

    /// Confirms a NovaRocks REST catalog can create a namespace by issuing
    /// `POST /v1/namespaces` to the mock server with the right payload.
    #[tokio::test]
    async fn rest_catalog_creates_namespace() {
        let mut server = Server::new_async().await;
        let _config_mock = server
            .mock("GET", "/v1/config")
            .with_status(200)
            .with_body(EMPTY_CONFIG_BODY)
            .create_async()
            .await;
        let create_mock = server
            .mock("POST", "/v1/namespaces")
            .with_status(200)
            .with_body(r#"{"namespace":["analytics"],"properties":{}}"#)
            .expect_at_least(1)
            .create_async()
            .await;

        let entry =
            build_catalog_entry("ice_rest", &rest_props(&server.url())).expect("rest entry");
        let catalog = build_rest_catalog(&entry)
            .await
            .expect("build rest catalog");
        let ns = NamespaceIdent::from_strs(["analytics"]).expect("ns ident");
        let created = catalog
            .create_namespace(&ns, std::collections::HashMap::new())
            .await
            .expect("create namespace via mock");
        assert_eq!(created.name(), &ns);
        create_mock.assert_async().await;
    }

    /// Helper: register a `GET /v1/namespaces` mock that returns the given
    /// JSON array body. Used by tests that need to walk past the initial
    /// config handshake to a real catalog operation.
    async fn mock_list_namespaces(server: &mut Server, namespaces_array: &str) -> mockito::Mock {
        let body = format!(
            r#"{{"namespaces":{namespaces_array}}}"#,
            namespaces_array = namespaces_array
        );
        server
            .mock("GET", "/v1/namespaces")
            .with_status(200)
            .with_body(body)
            .create_async()
            .await
    }

    /// Sanity check: building a Hadoop entry (default kind) still works
    /// after the REST extension. This guards against regressions in the
    /// shared `build_catalog_entry` parsing path.
    #[test]
    fn build_catalog_entry_hadoop_default_still_works() {
        let warehouse = tempfile::TempDir::new().expect("tempdir");
        let entry = build_catalog_entry(
            "ice_hadoop",
            &[
                ("type".to_string(), "iceberg".to_string()),
                (
                    "iceberg.catalog.warehouse".to_string(),
                    warehouse.path().display().to_string(),
                ),
            ],
        )
        .expect("hadoop entry");
        assert_eq!(entry.kind, IcebergCatalogKind::Hadoop);
        assert!(entry.rest_uri.is_none());
    }

    #[test]
    fn entry_clone_preserves_rest_kind() {
        let entry = build_catalog_entry(
            "ice_rest",
            &[
                ("iceberg.catalog.type".to_string(), "rest".to_string()),
                ("uri".to_string(), "http://localhost:8181".to_string()),
            ],
        )
        .expect("rest entry");
        let _: IcebergCatalogEntry = entry.clone();
        assert_eq!(entry.kind, IcebergCatalogKind::Rest);
    }
}
