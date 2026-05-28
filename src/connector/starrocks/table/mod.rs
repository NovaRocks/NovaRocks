//! StarRocks table subsystem: config, catalog rebuild/reconcile, DDL,
//! transactional INSERT + publish, SQLite-backed metadata persistence,
//! and materialized-view lifecycle. Migrated here from the former standalone
//! lake module during the standalone/connector decoupling refactor
//! (2026-04-24).
//!
//! Files will be added incrementally by the next tasks in this plan.

pub(crate) mod backend;
pub(crate) mod catalog;
pub(crate) mod config;
pub(crate) mod ddl;
pub(crate) mod erase;
pub(crate) mod ivm_change_stream;
pub(crate) mod ivm_delta_aggregate;
pub(crate) mod ivm_delta_source;
pub(crate) mod ivm_row_identity;
pub(crate) mod model;
pub(crate) mod mv_agg_state;
pub(crate) mod mv_apply_policy;
pub(crate) mod mv_ddl;
pub(crate) mod mv_refresh;
pub(crate) mod mv_refresh_strategy;
pub(crate) mod mv_shape;
pub(crate) mod refresh_pin;
pub(crate) mod scan_planner;
pub(crate) mod state_codec;
pub(crate) mod txn;

pub(crate) use backend::{
    StarRocksTableBackend, StarRocksTableMvBackend, StarRocksTableSink, StarRocksTableSource,
};
pub(crate) use catalog::{
    StarRocksTableCatalog, register_starrocks_tables_in_catalog, runtime_registered,
};
pub(crate) use config::StarRocksTableConfig;
pub(crate) use scan_planner::{
    StarRocksScanHandle, StarRocksSplit, StarRocksTableHandle, StarRocksTableScanPlanner,
};
