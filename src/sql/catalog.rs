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

#[derive(Clone, Debug)]
pub struct S3FileInfo {
    pub path: String,
    pub size: i64,
    /// Row count from Iceberg file metadata. None for non-Iceberg sources.
    pub row_count: Option<i64>,
    pub column_stats: Option<HashMap<String, IcebergColumnStats>>,
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
    pub storage: TableStorage,
}

/// Catalog abstraction for SQL analysis.
pub trait CatalogProvider {
    fn get_table(&self, database: &str, table: &str) -> Result<TableDef, String>;
}
