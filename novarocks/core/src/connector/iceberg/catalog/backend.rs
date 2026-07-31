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

//! Iceberg table-definition helpers.

use std::sync::{Arc, RwLock};

use crate::connector::iceberg::catalog::IcebergLoadedTable;
use crate::connector::iceberg::scan_model::{
    IcebergDataFileInfo, IcebergSchemaDef, IcebergSchemaFieldDef, IcebergTableInfo,
};
use crate::mv::persistence::schema::{APPLY_KEY_COLUMN_PROPERTY, HIDDEN_COLUMNS_PROPERTY};
use crate::sql::planner::table::{ScanSource, TableDef};
use novarocks_catalog::schema::ColumnDef;

#[cfg(test)]
use super::registry::load_table as reg_load_table;
use super::registry::{IcebergCatalogEntry, IcebergCatalogRegistry};

pub(crate) const ICEBERG_ROW_IDENTITY_FILE_COLUMN: &str = "_file";
pub(crate) const ICEBERG_ROW_IDENTITY_POS_COLUMN: &str = "_pos";

pub(crate) fn build_iceberg_table_def_with_files(
    entry: &IcebergCatalogEntry,
    catalog_name: &str,
    namespace: &str,
    table_name: &str,
    loaded: IcebergLoadedTable,
    data_files: Vec<super::registry::DataFileWithStats>,
) -> Result<TableDef, String> {
    build_iceberg_table_def_with_data_files(
        entry,
        catalog_name,
        namespace,
        table_name,
        loaded,
        data_files,
        crate::connector::iceberg::scan_model::IcebergDataFileBinding::ExplicitFiles,
    )
}

fn iceberg_row_identity_metadata_columns() -> Vec<ColumnDef> {
    vec![
        ColumnDef {
            name: ICEBERG_ROW_IDENTITY_FILE_COLUMN.to_string(),
            data_type: arrow::datatypes::DataType::Utf8,
            nullable: false,
            write_default: None,
            logical_type: None,
        },
        ColumnDef {
            name: ICEBERG_ROW_IDENTITY_POS_COLUMN.to_string(),
            data_type: arrow::datatypes::DataType::Int64,
            nullable: false,
            write_default: None,
            logical_type: None,
        },
    ]
}

fn iceberg_v3_row_lineage_metadata_columns() -> Vec<ColumnDef> {
    vec![
        ColumnDef {
            name: "_row_id".to_string(),
            data_type: arrow::datatypes::DataType::Int64,
            nullable: false,
            write_default: None,
            logical_type: None,
        },
        ColumnDef {
            name: "_last_updated_sequence_number".to_string(),
            data_type: arrow::datatypes::DataType::Int64,
            nullable: false,
            write_default: None,
            logical_type: None,
        },
    ]
}

fn iceberg_row_lineage_metadata_columns() -> Vec<ColumnDef> {
    let mut columns = iceberg_row_identity_metadata_columns();
    columns.extend(iceberg_v3_row_lineage_metadata_columns());
    columns
}

/// IVM-A1 helper: build a `TableDef` for the base table without registering
/// any data files. Always advertises Iceberg v3 row-lineage virtual columns
/// (`_file`, `_pos`, `_row_id`, `_last_updated_sequence_number`) plus the
/// transparent IVM `__change_op` pseudo-column when the metadata declares
/// row-lineage, so the analyzer can resolve `_row_id` references that the
/// IVM apply-key flow depends on and the merge sink can locate the
/// per-row insert / delete tag. The four lineage columns are listed in the
/// exact order the codegen scan-tuple builder consumes them; the
/// `IcebergDeltaScan` operator (not the standard scan) supplies these
/// columns per-file at runtime by synthesizing them from
/// `DeltaSourceFile.{path, first_row_id, data_sequence_number}` and the
/// per-row position. `__change_op` is set to `+1` for `DataFile` and `-1`
/// for the three delete roles.
///
/// `__change_op` is grouped with the lineage columns rather than living on
/// its own field because the codegen scan-tuple builder is the only consumer
/// that needs to allocate slots for transparent pseudo-columns, and folding
/// the change-op column into the same carrier keeps slot allocation in a
/// single loop. The downstream IcebergDeltaScan codegen path does not
/// consume `iceberg_metadata_pseudo_column_slots` for extended_columns
/// emission (that is HDFS_SCAN-only). The reason `__change_op` survives
/// column pruning all the way down to the operator is not that it is
/// listed here — column pruning would still drop a slot that no expression
/// references — but that the refresh driver explicitly includes `__change_op`
/// in the rewrite-path SELECT before analysis runs. That inclusion is what
/// makes the slot load-bearing;
/// declaring the column on the `TableDef` only ensures it resolves.
///
/// The `IcebergDeltaScanOperator` projects its internal superset (all data
/// columns + all virtual columns) onto the codegen tuple by name via
/// `iceberg_delta_scan::project_superset_to_tuple`, so the set of virtual
/// columns exposed here and the operator's output are decoupled: the TableDef
/// may expose the full five-column set while the codegen tuple requests only
/// a subset (e.g. `{data, _row_id, __change_op}` for projection/filter MVs).
///
/// Returns Err if the table metadata does not declare v3 row-lineage; A1
/// requires v3 row-lineage to compute the apply key.
pub(crate) fn build_iceberg_table_def_for_delta_scan(
    catalog_name: &str,
    namespace: &str,
    table_name: &str,
    loaded: IcebergLoadedTable,
) -> Result<TableDef, String> {
    if !is_v3_row_lineage(loaded.table.metadata()) {
        return Err(format!(
            "iceberg table {namespace}.{table_name} cannot back an IVM-A1 delta scan because its \
             metadata does not declare Iceberg v3 row-lineage; rebuild the base table with \
             write.row-lineage=true before creating the MV"
        ));
    }
    let columns =
        hide_novarocks_mv_internal_columns(loaded.table.metadata(), loaded.columns.clone())?;
    let schema = iceberg_schema_def(loaded.table.metadata().current_schema());
    let iceberg_table_info =
        build_iceberg_table_info(catalog_name, namespace, table_name, &loaded, schema)?;
    let source = empty_iceberg_scan_source(
        iceberg_table_info.clone(),
        crate::connector::iceberg::scan_model::IcebergDataFileBinding::ExplicitFiles,
    );
    let mut iceberg_row_lineage_metadata_columns = iceberg_row_lineage_metadata_columns();
    iceberg_row_lineage_metadata_columns.push(ColumnDef {
        name: crate::exec::change_op::CHANGE_OP_COLUMN.to_string(),
        data_type: arrow::datatypes::DataType::Int8,
        nullable: false,
        write_default: None,
        logical_type: None,
    });
    Ok(TableDef {
        name: table_name.to_string(),
        columns,
        iceberg_row_lineage_metadata_columns,
        source,
    })
}

pub(crate) fn build_iceberg_schema_table_def_from_loaded(
    entry: &IcebergCatalogEntry,
    catalog_name: &str,
    namespace: &str,
    table_name: &str,
    loaded: IcebergLoadedTable,
) -> Result<TableDef, String> {
    build_iceberg_table_def_with_data_files_impl(
        entry,
        catalog_name,
        namespace,
        table_name,
        loaded,
        Vec::new(),
        IcebergTableDefOptions {
            mode: IcebergTableDefMode::SchemaOnly,
            binding: crate::connector::iceberg::scan_model::IcebergDataFileBinding::CurrentSnapshot,
        },
    )
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum IcebergTableDefMode {
    ScanBinding,
    SchemaOnly,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct IcebergTableDefOptions {
    mode: IcebergTableDefMode,
    binding: crate::connector::iceberg::scan_model::IcebergDataFileBinding,
}

fn build_iceberg_table_def_with_data_files(
    entry: &IcebergCatalogEntry,
    catalog_name: &str,
    namespace: &str,
    table_name: &str,
    loaded: IcebergLoadedTable,
    data_files: Vec<super::registry::DataFileWithStats>,
    binding: crate::connector::iceberg::scan_model::IcebergDataFileBinding,
) -> Result<TableDef, String> {
    build_iceberg_table_def_with_data_files_impl(
        entry,
        catalog_name,
        namespace,
        table_name,
        loaded,
        data_files,
        IcebergTableDefOptions {
            mode: IcebergTableDefMode::ScanBinding,
            binding,
        },
    )
}

fn build_iceberg_table_def_with_data_files_impl(
    entry: &IcebergCatalogEntry,
    catalog_name: &str,
    namespace: &str,
    table_name: &str,
    loaded: IcebergLoadedTable,
    data_files: Vec<super::registry::DataFileWithStats>,
    options: IcebergTableDefOptions,
) -> Result<TableDef, String> {
    let has_data_files = !data_files.is_empty();
    // Row-lineage metadata columns (_row_id etc.) are only usable when every
    // data file in the snapshot carries a first_row_id.  Files written by
    // engines that do not support Iceberg v3 row-lineage (e.g. Spark 3.5 /
    // iceberg-spark 1.8.1) leave first_row_id unset.  Advertising _row_id as a
    // virtual column for such a snapshot would cause the lower to fail with
    // "missing first_row_id" for every file that lacks it.
    let all_files_have_first_row_id = data_files.iter().all(|f| f.first_row_id.is_some());
    let columns =
        hide_novarocks_mv_internal_columns(loaded.table.metadata(), loaded.columns.clone())?;
    let schema = iceberg_schema_def(loaded.table.metadata().current_schema());
    let iceberg_table_info =
        build_iceberg_table_info(catalog_name, namespace, table_name, &loaded, schema)?;
    let source = if entry.is_s3() {
        let cloud_properties = entry.cloud_properties_map();
        ScanSource::IcebergDataFiles {
            table: iceberg_table_info.clone(),
            files: data_files
                .into_iter()
                .map(data_file_with_stats_to_iceberg_data_file_info)
                .collect(),
            cloud_properties,
            binding: options.binding,
        }
    } else if has_data_files {
        // Local Iceberg tables can have multiple data files across snapshots.
        // Keep the per-file lineage metadata by using the multi-file scan
        // shape with empty cloud properties; file:// paths are handled by the
        // local scan path and do not require object-store credentials.
        ScanSource::IcebergDataFiles {
            table: iceberg_table_info.clone(),
            files: data_files
                .into_iter()
                .map(data_file_with_stats_to_iceberg_data_file_info)
                .collect(),
            cloud_properties: Default::default(),
            binding: options.binding,
        }
    } else {
        empty_iceberg_scan_source(iceberg_table_info, options.binding)
    };

    let iceberg_row_lineage_metadata_columns = match options.mode {
        IcebergTableDefMode::SchemaOnly => {
            let mut metadata_columns = iceberg_row_identity_metadata_columns();
            if row_lineage_enabled(loaded.table.metadata()) {
                metadata_columns.extend(iceberg_v3_row_lineage_metadata_columns());
            }
            metadata_columns
        }
        IcebergTableDefMode::ScanBinding => {
            let mut metadata_columns = iceberg_row_identity_metadata_columns();
            if has_data_files
                && is_v3_row_lineage(loaded.table.metadata())
                && all_files_have_first_row_id
            {
                metadata_columns.extend(iceberg_v3_row_lineage_metadata_columns());
            } else {
                if has_data_files
                    && is_v3_row_lineage(loaded.table.metadata())
                    && !all_files_have_first_row_id
                {
                    tracing::warn!(
                        table = %format!("{}.{}", namespace, table_name),
                        "iceberg table declares write.row-lineage=true but at least one data file lacks \
                         first_row_id; row-lineage metadata columns (_row_id, _last_updated_sequence_number) \
                         are hidden; downstream features depending on row lineage \
                         (e.g. IVM apply-key) will not see correct data for those rows"
                    );
                }
            }
            metadata_columns
        }
    };

    Ok(TableDef {
        name: table_name.to_string(),
        columns,
        iceberg_row_lineage_metadata_columns,
        source,
    })
}

fn hide_novarocks_mv_internal_columns(
    metadata: &iceberg::spec::TableMetadata,
    columns: Vec<ColumnDef>,
) -> Result<Vec<ColumnDef>, String> {
    hide_novarocks_mv_internal_columns_by_property(
        metadata
            .properties()
            .get(APPLY_KEY_COLUMN_PROPERTY)
            .map(String::as_str),
        metadata
            .properties()
            .get(HIDDEN_COLUMNS_PROPERTY)
            .map(String::as_str),
        columns,
    )
}

fn hide_novarocks_mv_internal_columns_by_property(
    apply_key_column: Option<&str>,
    hidden_columns: Option<&str>,
    columns: Vec<ColumnDef>,
) -> Result<Vec<ColumnDef>, String> {
    let hidden_column_names = hidden_internal_column_names(apply_key_column, hidden_columns);
    if hidden_column_names.is_empty() {
        return Ok(columns);
    };

    for hidden_column in &hidden_column_names {
        let matching_count = columns
            .iter()
            .filter(|column| column.name.eq_ignore_ascii_case(hidden_column))
            .count();
        if matching_count == 0 {
            return Err(format!(
                "Iceberg MV target schema is missing hidden internal column '{hidden_column}'"
            ));
        }
        if matching_count > 1 {
            return Err(format!(
                "Iceberg MV target schema has {matching_count} hidden internal columns named '{hidden_column}'"
            ));
        }
    }

    Ok(columns
        .into_iter()
        .filter(|column| {
            !hidden_column_names
                .iter()
                .any(|hidden| column.name.eq_ignore_ascii_case(hidden))
        })
        .collect())
}

/// Names of the NovaRocks MV internal columns (the apply-key column plus any
/// declared hidden aggregate-state columns) that `hide_novarocks_mv_internal_columns`
/// strips from a table's analyzer-visible schema. Derived from the same table
/// properties used by the hiding logic, so callers (e.g. the OPTIMIZE rewrite)
/// can detect whether a table carries hidden physical columns that a plain
/// `SELECT *` would omit, and react accordingly. An empty result means the
/// table has no hidden internal columns (plain Iceberg table or non-MV table).
pub(crate) fn hidden_internal_column_names_from_metadata(
    metadata: &iceberg::spec::TableMetadata,
) -> Vec<String> {
    hidden_internal_column_names(
        metadata
            .properties()
            .get(APPLY_KEY_COLUMN_PROPERTY)
            .map(String::as_str),
        metadata
            .properties()
            .get(HIDDEN_COLUMNS_PROPERTY)
            .map(String::as_str),
    )
}

fn hidden_internal_column_names(
    apply_key_column: Option<&str>,
    hidden_columns: Option<&str>,
) -> Vec<String> {
    let mut out = Vec::new();
    if let Some(apply_key_column) = apply_key_column {
        let trimmed = apply_key_column.trim();
        if !trimmed.is_empty() {
            out.push(trimmed.to_string());
        }
    }
    if let Some(hidden_columns) = hidden_columns {
        for hidden_column in hidden_columns.split(',') {
            let trimmed = hidden_column.trim();
            if trimmed.is_empty()
                || out
                    .iter()
                    .any(|existing| existing.eq_ignore_ascii_case(trimmed))
            {
                continue;
            }
            out.push(trimmed.to_string());
        }
    }
    out
}

pub(crate) fn data_file_with_stats_to_iceberg_data_file_info(
    file: super::registry::DataFileWithStats,
) -> IcebergDataFileInfo {
    IcebergDataFileInfo {
        path: file.path,
        size: file.size,
        row_count: file.record_count,
        column_stats: file.column_stats,
        partition_spec_id: file.partition_spec_id,
        partition_key: file.partition_key,
        first_row_id: file.first_row_id,
        data_sequence_number: file.data_sequence_number,
        ivm_change_op: None,
        included_positions: None,
        delete_files: file.delete_files,
        manifest_path: file.manifest_path,
        partition_values: file.partition_field_values,
    }
}

fn build_iceberg_table_info(
    catalog_name: &str,
    namespace_name: &str,
    table_name: &str,
    loaded: &IcebergLoadedTable,
    schema: IcebergSchemaDef,
) -> Result<IcebergTableInfo, String> {
    let table = &loaded.table;
    Ok(IcebergTableInfo {
        catalog: catalog_name.to_string(),
        namespace: namespace_name.to_string(),
        table: table_name.to_string(),
        table_uuid: Some(table.metadata().uuid().to_string()),
        current_snapshot_id: table.metadata().current_snapshot_id(),
        schema_id: table.metadata().current_schema_id(),
        location: loaded.table.metadata().location().to_string(),
        schema,
        serialized_metadata: Some(
            serde_json::to_string(table.metadata())
                .map_err(|err| format!("serialize iceberg table metadata failed: {err}"))?,
        ),
        serialized_metadata_rows: None,
    })
}

pub(crate) fn iceberg_schema_def_for_codegen(schema: &iceberg::spec::Schema) -> IcebergSchemaDef {
    iceberg_schema_def(schema)
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
    let initial_default_json = field.initial_default.as_ref().and_then(|literal| {
        literal
            .clone()
            .try_into_json(field.field_type.as_ref())
            .ok()
            .map(|json| json.to_string())
    });
    let write_default_json = field.write_default.as_ref().and_then(|literal| {
        literal
            .clone()
            .try_into_json(field.field_type.as_ref())
            .ok()
            .map(|json| json.to_string())
    });
    IcebergSchemaFieldDef {
        field_id: field.id,
        name: field.name.clone(),
        initial_default: field.initial_default.clone(),
        write_default: field.write_default.clone(),
        initial_default_json,
        write_default_json,
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
/// spec rules: format-version is V3 AND `write.row-lineage` is not
/// explicitly disabled. Per the Iceberg V3 spec, row-lineage is enabled
/// by default on V3 tables; writers may opt out with
/// `write.row-lineage=false`.
///
/// This is intentionally more permissive than `is_v3_row_lineage`. Schema-only
/// table definitions use this to expose row-lineage metadata columns for
/// catalog registration before scan files are bound, following V3-default
/// semantics. Scan-binding table definitions remain stricter and use
/// `is_v3_row_lineage` plus per-file `first_row_id` because ordinary scans can
/// only synthesize row-lineage metadata when every bound file carries row IDs.
/// OPTIMIZE preserves row-lineage whenever the writer would emit it on a fresh
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

/// Storage marker for an Iceberg table that has no data files yet.
///
/// The scan path treats `IcebergDataFiles { files: vec![] }` as "no
/// ranges to read"; the runtime returns an empty result without ever
/// touching the filesystem. This keeps empty Iceberg tables represented as
/// catalog-owned scan sources instead of synthetic placeholder files.
fn empty_iceberg_scan_source(
    table: IcebergTableInfo,
    binding: crate::connector::iceberg::scan_model::IcebergDataFileBinding,
) -> ScanSource {
    ScanSource::IcebergDataFiles {
        table,
        files: Vec::new(),
        cloud_properties: Default::default(),
        binding,
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::{Arc, RwLock};
    use std::time::{SystemTime, UNIX_EPOCH};

    use iceberg::spec::{
        FormatVersion, ListType, MapType, NestedField, PartitionSpec, PrimitiveType, Schema,
        SortOrder, TableMetadataBuilder, Type,
    };
    use iceberg::table::Table;
    use iceberg::{NamespaceIdent, TableIdent};

    use crate::connector::iceberg::catalog::registry::DataFileWithStats;
    use crate::sql::parser::ast::TableColumnDef;
    use novarocks_catalog::schema::SqlType;

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

    fn unique_warehouse(test_name: &str) -> String {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system time")
            .as_nanos();
        std::env::temp_dir()
            .join(format!(
                "novarocks_backend_test_{}_{}_{}",
                test_name,
                std::process::id(),
                nanos
            ))
            .to_string_lossy()
            .to_string()
    }

    fn latest_local_metadata_json_path(
        warehouse: &str,
        namespace: &str,
        table: &str,
    ) -> std::path::PathBuf {
        let metadata_dir = std::path::Path::new(warehouse)
            .join(namespace)
            .join(table)
            .join("metadata");
        let mut files = std::fs::read_dir(&metadata_dir)
            .unwrap_or_else(|err| panic!("read metadata dir {}: {err}", metadata_dir.display()))
            .filter_map(|entry| entry.ok())
            .filter_map(|entry| {
                let path = entry.path();
                let name = path.file_name()?.to_str()?;
                if name.starts_with('v') && name.ends_with(".metadata.json") {
                    let version = name
                        .strip_prefix('v')?
                        .strip_suffix(".metadata.json")?
                        .parse::<i32>()
                        .ok()?;
                    Some((version, path))
                } else {
                    None
                }
            })
            .collect::<Vec<_>>();
        files.sort_by_key(|(version, _)| *version);
        files.pop().map(|(_, path)| path).unwrap_or_else(|| {
            panic!(
                "metadata dir {} has no metadata json",
                metadata_dir.display()
            )
        })
    }

    fn loaded_table_with_properties(
        format_version: FormatVersion,
        properties: HashMap<String, String>,
    ) -> IcebergLoadedTable {
        let schema = Schema::builder()
            .with_fields(vec![
                NestedField::required(1, "id", Type::Primitive(PrimitiveType::Long)).into(),
            ])
            .build()
            .expect("schema");
        let location = "file:///novarocks-test/table".to_string();
        let metadata = TableMetadataBuilder::new(
            schema,
            PartitionSpec::unpartition_spec().into_unbound(),
            SortOrder::unsorted_order(),
            location.clone(),
            format_version,
            properties,
        )
        .expect("metadata builder")
        .build()
        .expect("metadata")
        .metadata;
        let table = Table::builder()
            .file_io(crate::connector::iceberg::fs_io::build_file_io_for_location(&location, None))
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
                logical_type: None,
            }],
            logical_types: HashMap::new(),
            key_desc: None,
            column_aggregations: HashMap::new(),
            object_store_config: None,
        }
    }

    fn v3_row_lineage_loaded_table() -> IcebergLoadedTable {
        loaded_table_with_properties(
            FormatVersion::V3,
            HashMap::from([("write.row-lineage".to_string(), "true".to_string())]),
        )
    }

    fn v3_default_row_lineage_loaded_table() -> IcebergLoadedTable {
        loaded_table_with_properties(FormatVersion::V3, HashMap::new())
    }

    fn v3_disabled_row_lineage_loaded_table() -> IcebergLoadedTable {
        loaded_table_with_properties(
            FormatVersion::V3,
            HashMap::from([("write.row-lineage".to_string(), "false".to_string())]),
        )
    }

    fn loaded_table() -> IcebergLoadedTable {
        loaded_table_with_properties(FormatVersion::V2, HashMap::new())
    }

    fn test_data_file() -> DataFileWithStats {
        DataFileWithStats {
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

    fn test_data_file_without_first_row_id() -> DataFileWithStats {
        let mut file = test_data_file();
        file.first_row_id = None;
        file
    }

    fn test_column(name: &str) -> ColumnDef {
        ColumnDef {
            name: name.to_string(),
            data_type: arrow::datatypes::DataType::Int64,
            nullable: false,
            write_default: None,
            logical_type: None,
        }
    }

    fn assert_row_lineage_metadata_columns(table_def: &TableDef) {
        assert_metadata_column_names(
            table_def,
            &["_file", "_pos", "_row_id", "_last_updated_sequence_number"],
        );
    }

    fn assert_row_identity_metadata_columns(table_def: &TableDef) {
        assert_metadata_column_names(table_def, &["_file", "_pos"]);
    }

    fn assert_metadata_column_names(table_def: &TableDef, expected: &[&str]) {
        let names = table_def
            .iceberg_row_lineage_metadata_columns
            .iter()
            .map(|column| column.name.as_str())
            .collect::<Vec<_>>();
        assert_eq!(names, expected);
    }

    #[test]
    fn hide_apply_key_columns_returns_columns_when_property_absent() {
        let columns = vec![test_column("id"), test_column("__nova_base_row_id")];

        let hidden = hide_novarocks_mv_internal_columns_by_property(None, None, columns.clone())
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

        let hidden = hide_novarocks_mv_internal_columns_by_property(
            Some("__nova_base_row_id"),
            None,
            columns,
        )
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
        let err = hide_novarocks_mv_internal_columns_by_property(
            Some("__nova_base_row_id"),
            None,
            vec![test_column("id")],
        )
        .expect_err("missing apply-key column should fail");

        assert!(err.contains("missing hidden internal column"));
        assert!(err.contains("__nova_base_row_id"));
    }

    #[test]
    fn hide_internal_columns_removes_apply_key_and_aggregate_state_columns() {
        let columns = vec![
            test_column("__row_id__"),
            test_column("region"),
            test_column("c"),
            test_column("__agg_state_c"),
            test_column("__agg_state_s"),
        ];

        let hidden = hide_novarocks_mv_internal_columns_by_property(
            Some("__row_id__"),
            Some("__agg_state_c, __agg_state_s"),
            columns,
        )
        .expect("hide columns");

        assert_eq!(
            hidden
                .iter()
                .map(|column| column.name.as_str())
                .collect::<Vec<_>>(),
            vec!["region", "c"]
        );
    }

    #[test]
    fn current_schema_id_bypasses_cached_loaded_table_for_local_catalog() {
        let warehouse = unique_warehouse("current_schema_id_uncached");
        let registry = Arc::new(RwLock::new(IcebergCatalogRegistry::default()));
        {
            let mut guard = registry.write().expect("iceberg catalog write lock");
            guard
                .create_catalog(
                    "ice",
                    &[("iceberg.catalog.warehouse".to_string(), warehouse.clone())],
                )
                .expect("create catalog");
        }
        let entry = registry
            .read()
            .expect("registry")
            .get("ice")
            .expect("entry");
        crate::connector::iceberg::catalog::registry::create_namespace(&entry, "db")
            .expect("create namespace");
        crate::connector::iceberg::catalog::registry::create_table(
            &entry,
            "db",
            "t",
            &[TableColumnDef {
                name: "id".to_string(),
                data_type: SqlType::BigInt,
                nullable: false,
                aggregation: None,
                default: None,
            }],
            None,
            &[],
            &[],
        )
        .expect("create table");

        let entry = registry
            .read()
            .expect("registry")
            .get("ice")
            .expect("entry");
        let cached = reg_load_table(&entry, "db", "t").expect("seed cache");
        let initial =
            crate::connector::iceberg::catalog::registry::current_schema_id(&entry, "db", "t")
                .expect("tracked schema id");
        let metadata_path = latest_local_metadata_json_path(&warehouse, "db", "t");
        let mut metadata_json: serde_json::Value =
            serde_json::from_slice(&std::fs::read(&metadata_path).expect("read metadata json"))
                .expect("parse metadata json");
        let json_schema_id = metadata_json
            .get("current-schema-id")
            .and_then(|value| value.as_i64())
            .expect("current-schema-id");
        assert_eq!(initial as i64, json_schema_id);
        assert_eq!(cached.columns.len(), 1);

        let changed = initial + 17;
        metadata_json["current-schema-id"] = serde_json::Value::from(changed);
        std::fs::write(
            &metadata_path,
            serde_json::to_vec_pretty(&metadata_json).expect("serialize metadata json"),
        )
        .expect("write metadata json");

        let probed =
            crate::connector::iceberg::catalog::registry::current_schema_id(&entry, "db", "t")
                .expect("tracked schema id");
        assert_eq!(probed, changed);
    }

    #[test]
    fn schema_only_v3_row_lineage_table_def_keeps_metadata_columns_without_files() {
        let table_def = build_iceberg_schema_table_def_from_loaded(
            &test_entry(),
            "ice",
            "db",
            "t",
            v3_row_lineage_loaded_table(),
        )
        .expect("schema-only table def");

        assert_row_lineage_metadata_columns(&table_def);
        let ScanSource::IcebergDataFiles { files, .. } = &table_def.source else {
            panic!("expected iceberg data-file scan source");
        };
        assert!(
            files.is_empty(),
            "schema-only registration must not carry scan-binding files"
        );
    }

    #[test]
    fn schema_only_v2_table_def_keeps_row_identity_metadata_columns() {
        let table_def = build_iceberg_schema_table_def_from_loaded(
            &test_entry(),
            "ice",
            "db",
            "t",
            loaded_table(),
        )
        .expect("schema-only table def");

        assert_row_identity_metadata_columns(&table_def);
    }

    #[test]
    fn schema_only_v3_default_row_lineage_table_def_keeps_metadata_columns_without_files() {
        let table_def = build_iceberg_schema_table_def_from_loaded(
            &test_entry(),
            "ice",
            "db",
            "t",
            v3_default_row_lineage_loaded_table(),
        )
        .expect("schema-only table def");

        assert_row_lineage_metadata_columns(&table_def);
        let ScanSource::IcebergDataFiles { files, .. } = &table_def.source else {
            panic!("expected iceberg data-file scan source");
        };
        assert!(
            files.is_empty(),
            "schema-only registration must not carry scan-binding files"
        );
    }

    #[test]
    fn schema_only_v3_row_lineage_false_table_def_keeps_row_identity_metadata_columns() {
        let table_def = build_iceberg_schema_table_def_from_loaded(
            &test_entry(),
            "ice",
            "db",
            "t",
            v3_disabled_row_lineage_loaded_table(),
        )
        .expect("schema-only table def");

        assert_row_identity_metadata_columns(&table_def);
    }

    #[test]
    fn schema_only_table_source_v3_row_lineage_uses_override_without_scan_files() {
        let registry = Arc::new(RwLock::new(IcebergCatalogRegistry::default()));
        {
            let mut guard = registry.write().expect("iceberg catalog write lock");
            guard
                .create_catalog(
                    "ice",
                    &[(
                        "iceberg.catalog.warehouse".to_string(),
                        unique_warehouse("schema_only_table_source"),
                    )],
                )
                .expect("create catalog");
        }
        let entry = registry
            .read()
            .expect("registry")
            .get("ice")
            .expect("entry");
        crate::connector::iceberg::catalog::registry::create_namespace(&entry, "db")
            .expect("create namespace");
        crate::connector::iceberg::catalog::registry::create_table(
            &entry,
            "db",
            "t",
            &[TableColumnDef {
                name: "id".to_string(),
                data_type: SqlType::BigInt,
                nullable: false,
                aggregation: None,
                default: None,
            }],
            None,
            &[],
            &[
                ("format-version".to_string(), "3".to_string()),
                ("write.row-lineage".to_string(), "true".to_string()),
            ],
        )
        .expect("create v3 row-lineage table");

        let entry = registry
            .read()
            .expect("registry")
            .get("ice")
            .expect("entry");
        let loaded = reg_load_table(&entry, "db", "t").expect("load table");
        let table_def =
            build_iceberg_schema_table_def_from_loaded(&entry, "ice", "db", "t", loaded)
                .expect("schema-only table def through connector metadata adapter");

        assert_row_lineage_metadata_columns(&table_def);
        let ScanSource::IcebergDataFiles { files, .. } = &table_def.source else {
            panic!("expected iceberg data-file scan source");
        };
        assert!(
            files.is_empty(),
            "schema-only table source must not bind snapshot data files"
        );
    }

    #[test]
    fn empty_v3_row_lineage_table_def_keeps_row_identity_metadata_columns() {
        let table_def = build_iceberg_table_def_with_data_files(
            &test_entry(),
            "ice",
            "db",
            "t",
            v3_row_lineage_loaded_table(),
            vec![],
            crate::connector::iceberg::scan_model::IcebergDataFileBinding::CurrentSnapshot,
        )
        .expect("table def");

        assert_row_identity_metadata_columns(&table_def);
    }

    #[test]
    fn non_empty_v3_row_lineage_table_def_keeps_metadata_columns() {
        let table_def = build_iceberg_table_def_with_data_files(
            &test_entry(),
            "ice",
            "db",
            "t",
            v3_row_lineage_loaded_table(),
            vec![test_data_file()],
            crate::connector::iceberg::scan_model::IcebergDataFileBinding::CurrentSnapshot,
        )
        .expect("table def");

        assert_row_lineage_metadata_columns(&table_def);
    }

    #[test]
    fn non_empty_v3_row_lineage_table_def_keeps_row_identity_columns_when_file_lacks_first_row_id()
    {
        let table_def = build_iceberg_table_def_with_data_files(
            &test_entry(),
            "ice",
            "db",
            "t",
            v3_row_lineage_loaded_table(),
            vec![test_data_file_without_first_row_id()],
            crate::connector::iceberg::scan_model::IcebergDataFileBinding::CurrentSnapshot,
        )
        .expect("table def");

        assert_row_identity_metadata_columns(&table_def);
    }

    #[test]
    fn data_file_with_stats_to_iceberg_data_file_info_preserves_read_metadata() {
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

        let data_file = data_file_with_stats_to_iceberg_data_file_info(file);

        assert_eq!(data_file.partition_spec_id, Some(7));
        assert_eq!(data_file.partition_key.as_deref(), Some("city=A"));
        assert_eq!(data_file.first_row_id, Some(100));
        assert_eq!(data_file.data_sequence_number, Some(11));
        assert_eq!(data_file.ivm_change_op, None);
        assert_eq!(
            data_file.manifest_path.as_deref(),
            Some("s3://bucket/table/metadata/manifest.avro")
        );
        assert!(data_file.delete_files.is_empty());
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
