//! Iceberg catalog subsystem.

pub(crate) mod add_files;
pub(crate) mod hadoop_catalog;
pub(crate) mod registry;
pub(crate) mod s3_storage;

// Preserve the surface that callers previously got from
// `crate::standalone::iceberg::*` (when iceberg.rs was a flat file).
pub(crate) use registry::{
    DataFileWithStats, IcebergCatalogEntry, IcebergCatalogRegistry, IcebergLoadedTable,
    block_on_iceberg, build_hadoop_catalog, build_insert_batch, create_namespace, create_table,
    drop_namespace, drop_table, extract_data_files, extract_data_files_with_stats, insert_rows,
    list_tables, load_table, namespace_exists, register_existing_table,
};
