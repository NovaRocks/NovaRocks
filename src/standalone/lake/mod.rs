//! Managed-lake subsystem: config, catalog rebuild/reconcile, DDL, transactional
//! insert + publish, and SQLite-backed metadata persistence.

pub(crate) mod catalog;
pub(crate) mod config;
pub(crate) mod ddl;
pub(crate) mod erase;
pub(crate) mod store;
pub(crate) mod txn;

// Preserve the public surface previously exposed by the flat lake_*.rs files.
pub(crate) use catalog::{
    ManagedLakeCatalog, ManagedTableRuntime, reconcile_on_open, register_managed_table_in_catalog,
    register_managed_tables_in_catalog, runtime_registered, snapshot_is_empty,
};
pub(crate) use config::ManagedLakeConfig;
