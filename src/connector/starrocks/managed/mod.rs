//! Managed-lake subsystem: config, catalog rebuild/reconcile, DDL,
//! transactional INSERT + publish, SQLite-backed metadata persistence,
//! and materialized-view lifecycle. Migrated here from
//! `src/standalone/lake/` during the standalone/connector decoupling
//! refactor (2026-04-24).
//!
//! Files will be added incrementally by the next tasks in this plan.

pub(crate) mod config;
pub(crate) mod erase;

pub(crate) use config::ManagedLakeConfig;
