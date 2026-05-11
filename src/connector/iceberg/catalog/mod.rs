//! Iceberg catalog registry, table loading, hadoop/S3 storage backends,
//! and `ADD FILES` support.

pub(crate) mod add_files;
pub(crate) mod backend;
pub(crate) mod hadoop_catalog;
pub(crate) mod registry;
pub(crate) mod s3_storage;
pub(crate) mod schema_update;

// Re-export the same surface the previous `standalone::iceberg::*` module
// offered, so callers only need to update the module prefix, not each
// imported symbol.
pub(crate) use backend::{
    IcebergCatalogBackend, IcebergTableSink, IcebergTableSource,
    build_iceberg_table_def_with_files, row_lineage_enabled,
};
pub(crate) use registry::{
    IcebergCatalogEntry, IcebergCatalogRegistry, IcebergLoadedTable, create_namespace, load_table,
    namespace_exists, register_existing_table,
};
pub(crate) use schema_update::{alter_table_properties, alter_table_schema};
