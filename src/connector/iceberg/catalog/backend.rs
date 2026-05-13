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
use crate::sql::catalog::{
    ColumnDef, IcebergSchemaDef, IcebergSchemaFieldDef, IcebergTableInfo, S3FileInfo, TableDef,
    TableStorage,
};
use crate::sql::parser::ast::Literal;

use super::registry::{
    IcebergCatalogEntry, IcebergCatalogRegistry, create_namespace as reg_create_namespace,
    create_table as reg_create_table, drop_namespace as reg_drop_namespace,
    drop_table as reg_drop_table, insert_rows as reg_insert_rows, list_tables as reg_list_tables,
    load_table as reg_load_table, namespace_exists as reg_namespace_exists,
};

const NOVAROCKS_MV_APPLY_KEY_COLUMN_PROPERTY: &str = "novarocks.mv.apply-key.column";

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
            &req.partition_fields,
            &req.properties,
        )
    }

    fn table_exists(&self, catalog: &str, namespace: &str, table: &str) -> Result<bool, String> {
        let entry = self.entry(catalog)?;
        let normalized = crate::engine::catalog::normalize_identifier(table)?;
        let tables = reg_list_tables(&entry, namespace)?;
        Ok(tables.iter().any(|t| t.eq_ignore_ascii_case(&normalized)))
    }

    fn alter_iceberg_partition_spec(
        &self,
        catalog: &str,
        namespace: &str,
        table: &str,
        stmt: crate::sql::parser::ast::AlterIcebergPartitionSpecStmt,
    ) -> Result<(), String> {
        let entry = self.entry(catalog)?;
        super::registry::alter_partition_spec(&entry, namespace, table, stmt)
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

    fn build_table_def_at(
        &self,
        table: &ResolvedTable,
        snapshot_id: Option<i64>,
    ) -> Result<TableDef, String> {
        let guard = self.registry.read().expect("iceberg catalog read lock");
        let entry = guard.get(&table.catalog)?;
        let loaded = reg_load_table(&entry, &table.namespace, &table.table)?;
        let effective_snapshot_id =
            snapshot_id.or_else(|| loaded.table.metadata().current_snapshot_id());
        let data_files = if let Some(id) = effective_snapshot_id {
            if let Some(cached) =
                entry.cached_data_files(&table.namespace, &table.table, Some(id))?
            {
                cached
            } else {
                let extracted =
                    super::registry::extract_data_files_with_stats_at(&loaded.table, id)?;
                entry.cache_data_files(
                    &table.namespace,
                    &table.table,
                    Some(id),
                    extracted.clone(),
                )?;
                extracted
            }
        } else {
            Vec::new()
        };
        build_iceberg_table_def_with_data_files(
            &entry,
            &table.namespace,
            &table.table,
            loaded,
            data_files,
        )
    }

    fn build_table_def(&self, table: &ResolvedTable) -> Result<TableDef, String> {
        self.build_table_def_at(table, None)
    }
}

pub(crate) fn build_iceberg_table_def_with_files(
    entry: &IcebergCatalogEntry,
    namespace: &str,
    table_name: &str,
    loaded: IcebergLoadedTable,
    data_files: Vec<super::registry::DataFileWithStats>,
) -> Result<TableDef, String> {
    build_iceberg_table_def_with_data_files(entry, namespace, table_name, loaded, data_files)
}

fn build_iceberg_table_def_with_data_files(
    entry: &IcebergCatalogEntry,
    namespace: &str,
    table_name: &str,
    loaded: IcebergLoadedTable,
    data_files: Vec<super::registry::DataFileWithStats>,
) -> Result<TableDef, String> {
    let iceberg_table = Some(build_iceberg_table_info(&loaded));
    let has_data_files = !data_files.is_empty();
    let columns =
        hide_novarocks_mv_apply_key_columns(loaded.table.metadata(), loaded.columns.clone())?;
    let storage = if entry.is_s3() {
        let cloud_properties = entry.cloud_properties_map();
        TableStorage::S3ParquetFiles {
            files: data_files
                .into_iter()
                .map(data_file_with_stats_to_s3_file_info)
                .collect(),
            cloud_properties,
        }
    } else if has_data_files {
        // Local Iceberg tables can have multiple data files across snapshots.
        // Keep the per-file lineage metadata by using the multi-file scan
        // shape with empty cloud properties; file:// paths are handled by the
        // local scan path and do not require object-store credentials.
        TableStorage::S3ParquetFiles {
            files: data_files
                .into_iter()
                .map(data_file_with_stats_to_s3_file_info)
                .collect(),
            cloud_properties: Default::default(),
        }
    } else {
        register_empty_iceberg_table(namespace, table_name, &loaded.columns)?
    };

    let iceberg_row_lineage_metadata_columns =
        if has_data_files && is_v3_row_lineage(loaded.table.metadata()) {
            vec![
                ColumnDef {
                    name: "_file".to_string(),
                    data_type: arrow::datatypes::DataType::Utf8,
                    nullable: false,
                    write_default: None,
                },
                ColumnDef {
                    name: "_pos".to_string(),
                    data_type: arrow::datatypes::DataType::Int64,
                    nullable: false,
                    write_default: None,
                },
                ColumnDef {
                    name: "_row_id".to_string(),
                    data_type: arrow::datatypes::DataType::Int64,
                    nullable: false,
                    write_default: None,
                },
                ColumnDef {
                    name: "_last_updated_sequence_number".to_string(),
                    data_type: arrow::datatypes::DataType::Int64,
                    nullable: false,
                    write_default: None,
                },
            ]
        } else {
            vec![]
        };

    Ok(TableDef {
        name: table_name.to_string(),
        columns,
        iceberg_row_lineage_metadata_columns,
        iceberg_table,
        storage,
    })
}

fn hide_novarocks_mv_apply_key_columns(
    metadata: &iceberg::spec::TableMetadata,
    columns: Vec<ColumnDef>,
) -> Result<Vec<ColumnDef>, String> {
    hide_novarocks_mv_apply_key_columns_by_property(
        metadata
            .properties()
            .get(NOVAROCKS_MV_APPLY_KEY_COLUMN_PROPERTY)
            .map(String::as_str),
        columns,
    )
}

fn hide_novarocks_mv_apply_key_columns_by_property(
    apply_key_column: Option<&str>,
    columns: Vec<ColumnDef>,
) -> Result<Vec<ColumnDef>, String> {
    let Some(apply_key_column) = apply_key_column else {
        return Ok(columns);
    };

    let matching_count = columns
        .iter()
        .filter(|column| column.name.eq_ignore_ascii_case(apply_key_column))
        .count();
    if matching_count == 0 {
        return Err(format!(
            "Iceberg MV target schema is missing apply-key column '{apply_key_column}'"
        ));
    }
    if matching_count > 1 {
        return Err(format!(
            "Iceberg MV target schema has {matching_count} apply-key columns named '{apply_key_column}'"
        ));
    }

    Ok(columns
        .into_iter()
        .filter(|column| !column.name.eq_ignore_ascii_case(apply_key_column))
        .collect())
}

fn data_file_with_stats_to_s3_file_info(file: super::registry::DataFileWithStats) -> S3FileInfo {
    S3FileInfo {
        path: file.path,
        size: file.size,
        row_count: file.record_count,
        column_stats: file.column_stats,
        partition_spec_id: file.partition_spec_id,
        partition_key: file.partition_key,
        first_row_id: file.first_row_id,
        data_sequence_number: file.data_sequence_number,
        ivm_change_op: None,
        delete_files: file.delete_files,
        manifest_path: file.manifest_path,
        partition_values: file.partition_field_values,
    }
}

fn build_iceberg_table_info(loaded: &IcebergLoadedTable) -> IcebergTableInfo {
    // Serialise the iceberg `TableMetadata` so a subsequent metadata-table
    // reference (`t$snapshots` etc.) can hand the JSON to the JVM scanner
    // bridge without having to re-resolve the table at codegen time. Loss
    // of serialisation is non-fatal for normal data scans, so we fall back
    // to None instead of failing here.
    let serialized_metadata = serde_json::to_string(loaded.table.metadata()).ok();
    IcebergTableInfo {
        location: loaded.table.metadata().location().to_string(),
        schema: iceberg_schema_def(loaded.table.metadata().current_schema()),
        serialized_metadata,
    }
}

fn iceberg_schema_def(schema: &iceberg::spec::Schema) -> IcebergSchemaDef {
    IcebergSchemaDef {
        fields: schema
            .as_struct()
            .fields()
            .iter()
            .map(|field| iceberg_field_def(field.as_ref()))
            .collect(),
    }
}

fn iceberg_field_def(field: &iceberg::spec::NestedField) -> IcebergSchemaFieldDef {
    IcebergSchemaFieldDef {
        field_id: field.id,
        name: field.name.clone(),
        initial_default: field.initial_default.clone(),
        write_default: field.write_default.clone(),
        children: iceberg_type_children(field.field_type.as_ref()),
    }
}

fn iceberg_type_children(ty: &iceberg::spec::Type) -> Vec<IcebergSchemaFieldDef> {
    match ty {
        iceberg::spec::Type::Struct(struct_ty) => struct_ty
            .fields()
            .iter()
            .map(|field| iceberg_field_def(field.as_ref()))
            .collect(),
        iceberg::spec::Type::List(list_ty) => {
            vec![iceberg_field_def(list_ty.element_field.as_ref())]
        }
        iceberg::spec::Type::Map(map_ty) => vec![
            iceberg_field_def(map_ty.key_field.as_ref()),
            iceberg_field_def(map_ty.value_field.as_ref()),
        ],
        iceberg::spec::Type::Primitive(_) => vec![],
    }
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

/// True iff the table can carry row-lineage metadata under the Iceberg V3
/// spec rules: format-version >= 3 AND `write.row-lineage` is not
/// explicitly disabled. Per the Iceberg V3 spec, row-lineage is enabled
/// by default on V3 tables; writers may opt out with
/// `write.row-lineage=false`.
///
/// This is intentionally more permissive than `is_v3_row_lineage`, which
/// is used to gate exposure of the `_row_id` /
/// `_last_updated_sequence_number` pseudo-columns at SQL analysis time
/// (where the explicit opt-in property is the safer signal). OPTIMIZE
/// preserves row-lineage whenever the writer would emit it on a fresh
/// INSERT, which follows the V3-default semantics modelled here.
pub(crate) fn row_lineage_enabled(metadata: &iceberg::spec::TableMetadata) -> bool {
    if !matches!(metadata.format_version(), iceberg::spec::FormatVersion::V3) {
        return false;
    }
    match metadata.properties().get("write.row-lineage") {
        Some(v) => !v.eq_ignore_ascii_case("false"),
        None => true,
    }
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

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Arc;

    use iceberg::spec::{
        FormatVersion, ListType, MapType, NestedField, PartitionSpec, PrimitiveType, Schema,
        SortOrder, TableMetadataBuilder, Type,
    };
    use iceberg::table::Table;
    use iceberg::{NamespaceIdent, TableIdent};

    use super::*;

    fn test_entry() -> IcebergCatalogEntry {
        let warehouse =
            std::env::temp_dir().join(format!("novarocks_backend_test_{}", std::process::id()));
        crate::connector::iceberg::catalog::registry::build_catalog_entry(
            "ice",
            &[(
                "iceberg.catalog.warehouse".to_string(),
                warehouse.to_string_lossy().to_string(),
            )],
        )
        .expect("catalog entry")
    }

    fn v3_row_lineage_loaded_table() -> IcebergLoadedTable {
        let schema = Schema::builder()
            .with_fields(vec![
                NestedField::required(1, "id", Type::Primitive(PrimitiveType::Long)).into(),
            ])
            .build()
            .expect("schema");
        let metadata = TableMetadataBuilder::new(
            schema,
            PartitionSpec::unpartition_spec().into_unbound(),
            SortOrder::unsorted_order(),
            "memory://test/table".to_string(),
            FormatVersion::V3,
            HashMap::from([("write.row-lineage".to_string(), "true".to_string())]),
        )
        .expect("metadata builder")
        .build()
        .expect("metadata")
        .metadata;
        let table = Table::builder()
            .file_io(iceberg::io::FileIO::new_with_memory())
            .metadata(metadata)
            .identifier(TableIdent::new(
                NamespaceIdent::new("db".to_string()),
                "t".to_string(),
            ))
            .build()
            .expect("table");

        IcebergLoadedTable {
            table,
            columns: vec![ColumnDef {
                name: "id".to_string(),
                data_type: arrow::datatypes::DataType::Int64,
                nullable: false,
                write_default: None,
            }],
            logical_types: HashMap::new(),
            key_desc: None,
            column_aggregations: HashMap::new(),
            object_store_config: None,
        }
    }

    fn test_data_file() -> crate::connector::iceberg::catalog::registry::DataFileWithStats {
        crate::connector::iceberg::catalog::registry::DataFileWithStats {
            path: "file:///tmp/table/data.parquet".to_string(),
            size: 12,
            record_count: Some(1),
            column_stats: None,
            partition_spec_id: Some(0),
            partition_key: None,
            partition_values: None,
            manifest_path: Some("file:///tmp/table/metadata/manifest.avro".to_string()),
            partition_field_values: vec![],
            first_row_id: Some(100),
            data_sequence_number: Some(1),
            delete_files: vec![],
        }
    }

    fn test_column(name: &str) -> ColumnDef {
        ColumnDef {
            name: name.to_string(),
            data_type: arrow::datatypes::DataType::Int64,
            nullable: false,
            write_default: None,
        }
    }

    #[test]
    fn hide_apply_key_columns_returns_columns_when_property_absent() {
        let columns = vec![test_column("id"), test_column("__nova_base_row_id")];

        let hidden = hide_novarocks_mv_apply_key_columns_by_property(None, columns.clone())
            .expect("hide columns");

        assert_eq!(
            hidden
                .iter()
                .map(|column| column.name.as_str())
                .collect::<Vec<_>>(),
            columns
                .iter()
                .map(|column| column.name.as_str())
                .collect::<Vec<_>>()
        );
    }

    #[test]
    fn hide_apply_key_columns_removes_one_case_insensitive_match() {
        let columns = vec![test_column("id"), test_column("__NOVA_BASE_ROW_ID")];

        let hidden =
            hide_novarocks_mv_apply_key_columns_by_property(Some("__nova_base_row_id"), columns)
                .expect("hide columns");

        assert_eq!(
            hidden
                .iter()
                .map(|column| column.name.as_str())
                .collect::<Vec<_>>(),
            vec!["id"]
        );
    }

    #[test]
    fn hide_apply_key_columns_errors_when_marked_column_is_missing() {
        let err = hide_novarocks_mv_apply_key_columns_by_property(
            Some("__nova_base_row_id"),
            vec![test_column("id")],
        )
        .expect_err("missing apply-key column should fail");

        assert!(err.contains("missing apply-key column"));
        assert!(err.contains("__nova_base_row_id"));
    }

    #[test]
    fn empty_v3_row_lineage_table_def_hides_metadata_columns() {
        let table_def = build_iceberg_table_def_with_data_files(
            &test_entry(),
            "db",
            "t",
            v3_row_lineage_loaded_table(),
            vec![],
        )
        .expect("table def");

        assert!(table_def.iceberg_row_lineage_metadata_columns.is_empty());
    }

    #[test]
    fn non_empty_v3_row_lineage_table_def_keeps_metadata_columns() {
        let table_def = build_iceberg_table_def_with_data_files(
            &test_entry(),
            "db",
            "t",
            v3_row_lineage_loaded_table(),
            vec![test_data_file()],
        )
        .expect("table def");

        let names = table_def
            .iceberg_row_lineage_metadata_columns
            .iter()
            .map(|column| column.name.as_str())
            .collect::<Vec<_>>();
        assert_eq!(
            names,
            vec!["_file", "_pos", "_row_id", "_last_updated_sequence_number"]
        );
    }

    #[test]
    fn data_file_with_stats_to_s3_file_info_preserves_read_metadata() {
        let file = crate::connector::iceberg::catalog::registry::DataFileWithStats {
            path: "s3://bucket/table/data.parquet".to_string(),
            size: 12,
            record_count: Some(3),
            column_stats: None,
            partition_spec_id: Some(7),
            partition_key: Some("city=A".to_string()),
            partition_values: None,
            manifest_path: Some("s3://bucket/table/metadata/manifest.avro".to_string()),
            partition_field_values: vec![],
            first_row_id: Some(100),
            data_sequence_number: Some(11),
            delete_files: vec![],
        };

        let s3_file = data_file_with_stats_to_s3_file_info(file);

        assert_eq!(s3_file.partition_spec_id, Some(7));
        assert_eq!(s3_file.partition_key.as_deref(), Some("city=A"));
        assert_eq!(s3_file.first_row_id, Some(100));
        assert_eq!(s3_file.data_sequence_number, Some(11));
        assert_eq!(s3_file.ivm_change_op, None);
        assert_eq!(
            s3_file.manifest_path.as_deref(),
            Some("s3://bucket/table/metadata/manifest.avro")
        );
        assert!(s3_file.delete_files.is_empty());
    }

    #[test]
    fn iceberg_schema_def_includes_nested_list_map_field_ids() {
        let struct_field = Arc::new(NestedField::required(
            2,
            "payload",
            Type::Struct(iceberg::spec::StructType::new(vec![Arc::new(
                NestedField::optional(3, "inner", Type::Primitive(PrimitiveType::String)),
            )])),
        ));
        let list_field = Arc::new(NestedField::optional(
            4,
            "items",
            Type::List(ListType::new(Arc::new(NestedField::list_element(
                5,
                Type::Primitive(PrimitiveType::Int),
                false,
            )))),
        ));
        let map_field = Arc::new(NestedField::optional(
            6,
            "attrs",
            Type::Map(MapType::new(
                Arc::new(NestedField::map_key_element(
                    7,
                    Type::Primitive(PrimitiveType::String),
                )),
                Arc::new(NestedField::map_value_element(
                    8,
                    Type::Primitive(PrimitiveType::Long),
                    false,
                )),
            )),
        ));
        let schema = Schema::builder()
            .with_fields(vec![struct_field, list_field, map_field])
            .build()
            .expect("schema");

        let def = iceberg_schema_def(&schema);

        assert_eq!(def.fields[0].field_id, 2);
        assert_eq!(def.fields[0].children[0].field_id, 3);
        assert_eq!(def.fields[1].field_id, 4);
        assert_eq!(def.fields[1].children[0].field_id, 5);
        assert_eq!(def.fields[2].field_id, 6);
        assert_eq!(def.fields[2].children[0].field_id, 7);
        assert_eq!(def.fields[2].children[1].field_id, 8);
    }
}
