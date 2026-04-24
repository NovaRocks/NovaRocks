//! DEPRECATED in favor of `crate::connector::iceberg::catalog`. This file
//! exists only during the standalone/connector decoupling refactor so that
//! existing call sites keep compiling while imports are rewritten one
//! caller at a time. Will be deleted at the end of Phase 1 (Task 1.10).

pub(crate) use crate::connector::iceberg::catalog::{
    DataFileWithStats, IcebergAppendDelta, IcebergCatalogEntry, IcebergCatalogRegistry,
    IcebergLoadedTable, block_on_iceberg, build_hadoop_catalog, build_insert_batch,
    create_namespace, create_table, drop_namespace, drop_table, extract_data_files,
    extract_data_files_with_stats, insert_rows, list_tables, load_table, namespace_exists,
    plan_append_delta, register_existing_table,
};

pub(crate) mod add_files {
    pub(crate) use crate::connector::iceberg::catalog::add_files::*;
}
pub(crate) mod registry {
    pub(crate) use crate::connector::iceberg::catalog::registry::*;
}
