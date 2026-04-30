use std::collections::BTreeMap;
use std::collections::HashMap;
use std::path::PathBuf;

use arrow::datatypes::DataType;

#[derive(Clone, Debug, PartialEq)]
pub struct ColumnDef {
    pub name: String,
    pub data_type: DataType,
    pub nullable: bool,
}

/// Raw per-column statistics from Iceberg manifest DataFile entries.
#[derive(Clone, Debug)]
pub struct IcebergColumnStats {
    pub null_count: Option<i64>,
    pub column_size: Option<i64>,
    pub lower_bound: Option<Vec<u8>>,
    pub upper_bound: Option<Vec<u8>>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum IcebergDeleteFileFormat {
    Parquet,
    Puffin,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct IcebergDeleteFileInfo {
    pub path: String,
    pub file_format: IcebergDeleteFileFormat,
    pub length: Option<i64>,
    pub content_offset: Option<i64>,
    pub content_size_in_bytes: Option<i64>,
}

#[derive(Clone, Debug)]
pub struct S3FileInfo {
    pub path: String,
    pub size: i64,
    /// Row count from Iceberg file metadata. None for non-Iceberg sources.
    pub row_count: Option<i64>,
    pub column_stats: Option<HashMap<String, IcebergColumnStats>>,
    /// Iceberg v3 row-lineage: first row id assigned to this data file.
    /// Used as the fallback base for `_row_id` reads. None for non-Iceberg
    /// sources and tables without row-lineage metadata.
    pub first_row_id: Option<i64>,
    /// Iceberg v3 row-lineage: data sequence number of the manifest entry this
    /// file belongs to.  Populated from the Iceberg manifest at catalog scan
    /// time.  None for non-Iceberg sources.
    pub data_sequence_number: Option<i64>,
    /// Iceberg position-delete / Puffin deletion-vector files that apply to
    /// this data file. Empty for append-only snapshots and non-Iceberg scans.
    pub delete_files: Vec<IcebergDeleteFileInfo>,
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
    pub storage: TableStorage,
}

/// Catalog abstraction for SQL analysis.
pub trait CatalogProvider {
    fn get_table(&self, database: &str, table: &str) -> Result<TableDef, String>;

    fn get_physical_layout(
        &self,
        _database: &str,
        _table: &str,
    ) -> Result<Option<PhysicalTableLayout>, String> {
        Ok(None)
    }
}
