//! Managed-lake subsystem: catalog rebuild/reconcile, DDL, transactional
//! insert + publish, and SQLite-backed metadata persistence.
//! catalog, store, ddl, txn live under connector::starrocks::managed; only
//! mv_ddl/mv_refresh/mv_shape remain here until Task 1.9.

pub(crate) mod mv_ddl;
pub(crate) mod mv_refresh;
pub(crate) mod mv_shape;

// Backward-compat re-exports: catalog/store/ddl/txn moved to connector::starrocks::managed.
pub(crate) use crate::connector::starrocks::managed::{
    ManagedLakeCatalog, reconcile_on_open, register_managed_table_in_catalog,
    register_managed_tables_in_catalog, runtime_registered,
};
pub(crate) use crate::connector::starrocks::managed::ManagedLakeConfig;
