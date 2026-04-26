//! Managed-lake subsystem: config, catalog rebuild/reconcile, DDL,
//! transactional INSERT + publish, SQLite-backed metadata persistence,
//! and materialized-view lifecycle. Migrated here from
//! `src/standalone/lake/` during the standalone/connector decoupling
//! refactor (2026-04-24).
//!
//! Files will be added incrementally by the next tasks in this plan.

pub(crate) mod backend;
pub(crate) mod catalog;
pub(crate) mod config;
pub(crate) mod ddl;
pub(crate) mod erase;
pub(crate) mod mv_ddl;
pub(crate) mod mv_refresh;
pub(crate) mod mv_shape;
pub(crate) mod store;
pub(crate) mod txn;

pub(crate) use backend::{
    ManagedLakeBackend, ManagedLakeMvBackend, ManagedLakeTableSink, ManagedLakeTableSource,
};
pub(crate) use catalog::{
    ManagedLakeCatalog, reconcile_on_open, register_managed_tables_in_catalog, runtime_registered,
};
pub(crate) use config::ManagedLakeConfig;
