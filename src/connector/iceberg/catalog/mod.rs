//! Iceberg catalog registry, table loading, hadoop/S3 storage backends,
//! and `ADD FILES` support.

pub(crate) mod add_files;
pub(crate) mod backend;
pub(crate) mod hadoop_catalog;
pub(crate) mod registry;
pub(crate) mod s3_storage;

// Re-export the same surface the previous `standalone::iceberg::*` module
// offered, so callers only need to update the module prefix, not each
// imported symbol.
pub(crate) use backend::{IcebergCatalogBackend, IcebergTableSink, IcebergTableSource};
pub(crate) use registry::{
    DataFileWithStats, IcebergAppendDelta, IcebergCatalogEntry, IcebergCatalogRegistry,
    IcebergLoadedTable, block_on_iceberg, build_hadoop_catalog, build_insert_batch,
    create_namespace, create_table, drop_namespace, drop_table, extract_data_files,
    extract_data_files_with_stats, insert_rows, list_tables, load_table, namespace_exists,
    plan_append_delta, register_existing_table,
};
