use std::collections::BTreeMap;
use std::collections::HashMap;
use std::path::PathBuf;

use arrow::datatypes::DataType;

#[derive(Clone, Debug, PartialEq)]
pub struct ColumnDef {
    pub name: String,
    pub data_type: DataType,
    pub nullable: bool,
    pub write_default: Option<iceberg::spec::Literal>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LegacyRangePartition {
    pub name: String,
    pub column: String,
    pub lower_sql: String,
    pub upper_sql: String,
}

/// Raw per-column statistics from Iceberg manifest DataFile entries.
#[derive(Clone, Debug)]
pub struct IcebergColumnStats {
    pub null_count: Option<i64>,
    pub column_size: Option<i64>,
    pub lower_bound: Option<Vec<u8>>,
    pub upper_bound: Option<Vec<u8>>,
}

#[derive(Clone, Debug, PartialEq)]
pub enum IcebergPartitionValue {
    Boolean(bool),
    Int32(i32),
    Int64(i64),
    Float(f32),
    Double(f64),
    String(String),
    Binary(Vec<u8>),
}

#[derive(Clone, Debug, PartialEq)]
pub struct IcebergPartitionFieldValue {
    pub source_column: String,
    pub field_name: String,
    pub transform: String,
    pub value: Option<IcebergPartitionValue>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum IcebergDeleteFileFormat {
    Parquet,
    Puffin,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum IcebergDeleteFileContent {
    Position,
    Equality,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct IcebergDeleteFileInfo {
    pub path: String,
    pub file_format: IcebergDeleteFileFormat,
    pub file_content: IcebergDeleteFileContent,
    pub length: Option<i64>,
    pub content_offset: Option<i64>,
    pub content_size_in_bytes: Option<i64>,
    pub sequence_number: Option<i64>,
    pub partition_spec_id: Option<i32>,
    pub partition_key: Option<String>,
    pub equality_column_names: Vec<String>,
    pub equality_field_ids: Vec<i32>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct IcebergSchemaFieldDef {
    pub field_id: i32,
    pub name: String,
    pub initial_default: Option<iceberg::spec::Literal>,
    pub write_default: Option<iceberg::spec::Literal>,
    pub children: Vec<IcebergSchemaFieldDef>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct IcebergSchemaDef {
    pub fields: Vec<IcebergSchemaFieldDef>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct IcebergTableInfo {
    pub location: String,
    pub schema: IcebergSchemaDef,
    /// JSON-serialized iceberg `TableMetadata`. Required when the table
    /// is referenced as an Iceberg metadata table (`t$snapshots`,
    /// `t$history`, `t$refs`, `t$partitions`) — the JNI metadata-scan
    /// bridge consumes this string to materialise the metadata rows.
    /// `None` for tables resolved via paths that do not have access to
    /// the iceberg `TableMetadata` (e.g. synthetic test fixtures).
    pub serialized_metadata: Option<String>,
}

#[derive(Clone, Debug)]
pub struct S3FileInfo {
    pub path: String,
    pub size: i64,
    /// Row count from Iceberg file metadata. None for non-Iceberg sources.
    pub row_count: Option<i64>,
    pub column_stats: Option<HashMap<String, IcebergColumnStats>>,
    /// Iceberg partition spec id for this data file. None for non-Iceberg
    /// sources or synthetic scans where partition metadata is unavailable.
    pub partition_spec_id: Option<i32>,
    /// Stable string form of the Iceberg partition struct. Used only as
    /// metadata for read-planning paths that need delete applicability.
    pub partition_key: Option<String>,
    /// Iceberg v3 row-lineage: first row id assigned to this data file.
    /// Used as the fallback base for `_row_id` reads. None for non-Iceberg
    /// sources and tables without row-lineage metadata.
    pub first_row_id: Option<i64>,
    /// Iceberg v3 row-lineage: data sequence number of the manifest entry this
    /// file belongs to.  Populated from the Iceberg manifest at catalog scan
    /// time.  None for non-Iceberg sources.
    pub data_sequence_number: Option<i64>,
    /// IVM delta source tag for this file/range. None for ordinary scans.
    pub ivm_change_op: Option<i8>,
    /// Iceberg position-delete / Puffin deletion-vector files that apply to
    /// this data file. Empty for append-only snapshots and non-Iceberg scans.
    pub delete_files: Vec<IcebergDeleteFileInfo>,
    /// Data manifest path that contributed this file. None for non-Iceberg
    /// sources and synthetic test files.
    pub manifest_path: Option<String>,
    /// Partition values decoded from the Iceberg DataFile partition struct.
    /// Currently used for conservative identity-partition pruning.
    pub partition_values: Vec<IcebergPartitionFieldValue>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ManagedTabletRef {
    pub tablet_id: i64,
    pub partition_id: i64,
    pub version: i64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PhysicalTableLayout {
    pub db_id: i64,
    pub table_id: i64,
    pub schema_id: i64,
    pub tablets: Vec<ManagedTabletRef>,
}

#[derive(Clone, Debug)]
pub enum TableStorage {
    LocalParquetFile {
        path: PathBuf,
    },
    S3ParquetFiles {
        files: Vec<S3FileInfo>,
        cloud_properties: BTreeMap<String, String>,
    },
    /// Synthetic storage backing an Iceberg metadata-table reference
    /// (`t$snapshots` / `t$history` / `t$refs` / `t$partitions`). The
    /// analyzer rewrites such references into a regular `Scan` over a
    /// synthetic `TableDef` whose storage is this variant; codegen then
    /// emits an `HDFS_SCAN_NODE` whose lowering builds an
    /// `IcebergMetadataScanOp` that reads `iceberg::spec::TableMetadata`
    /// natively (no JNI bridge) — see
    /// `src/connector/iceberg/metadata.rs`.
    IcebergMetadataTable {
        metadata_table_type: crate::connector::iceberg::IcebergMetadataTableType,
        /// JSON-serialized iceberg-rust `TableMetadata` (produced by
        /// `serde_json::to_string` in
        /// `connector/iceberg/catalog/backend.rs`). The metadata-scan
        /// operator parses it back into a `TableMetadata` and reads
        /// snapshots / history / refs directly off it.
        serialized_table: String,
        /// Cloud properties from the underlying iceberg table's storage.
        /// Forwarded onto `THdfsScanNode.cloud_configuration` for parity
        /// with regular HDFS scans; the native metadata-scan path itself
        /// does not need them today.
        cloud_properties: BTreeMap<String, String>,
    },
}

#[derive(Clone, Debug)]
pub struct TableDef {
    pub name: String,
    pub columns: Vec<ColumnDef>,
    /// Iceberg V3 row-lineage reserved metadata pseudo-columns. Empty for
    /// non-Iceberg tables, V2 Iceberg tables, and V3 tables without
    /// `write.row-lineage=true`. Populated by the iceberg `CatalogProvider`
    /// implementation when the base table satisfies the row-lineage
    /// preconditions. The analyzer registers these into the per-relation
    /// scope as resolvable pseudo-columns but **not** into `SELECT *`
    /// expansion.
    pub iceberg_row_lineage_metadata_columns: Vec<ColumnDef>,
    pub iceberg_table: Option<IcebergTableInfo>,
    pub storage: TableStorage,
}

/// Catalog abstraction for SQL analysis.
pub trait CatalogProvider {
    fn get_table(&self, database: &str, table: &str) -> Result<TableDef, String>;

    fn get_legacy_range_partition(
        &self,
        _database: &str,
        _table: &str,
        _partition: &str,
    ) -> Result<Option<LegacyRangePartition>, String> {
        Ok(None)
    }

    fn get_physical_layout(
        &self,
        _database: &str,
        _table: &str,
    ) -> Result<Option<PhysicalTableLayout>, String> {
        Ok(None)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn table_def_can_carry_iceberg_schema_metadata() {
        let table = TableDef {
            name: "orders".to_string(),
            columns: vec![ColumnDef {
                name: "order_id".to_string(),
                data_type: DataType::Int64,
                nullable: false,
                write_default: None,
            }],
            iceberg_row_lineage_metadata_columns: vec![],
            iceberg_table: Some(IcebergTableInfo {
                location: "file:///tmp/orders".to_string(),
                schema: IcebergSchemaDef {
                    fields: vec![IcebergSchemaFieldDef {
                        field_id: 10,
                        name: "order_id".to_string(),
                        initial_default: None,
                        write_default: None,
                        children: vec![IcebergSchemaFieldDef {
                            field_id: 11,
                            name: "nested".to_string(),
                            initial_default: None,
                            write_default: None,
                            children: vec![],
                        }],
                    }],
                },
                serialized_metadata: None,
            }),
            storage: TableStorage::S3ParquetFiles {
                files: vec![],
                cloud_properties: BTreeMap::new(),
            },
        };

        let iceberg = table.iceberg_table.expect("iceberg table metadata");
        assert_eq!(iceberg.location, "file:///tmp/orders");
        assert_eq!(iceberg.schema.fields[0].field_id, 10);
        assert_eq!(iceberg.schema.fields[0].children[0].field_id, 11);
    }
}
