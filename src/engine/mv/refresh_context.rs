//! Immutable refresh-time context for Iceberg MV refresh.
//!
//! Two layers:
//! - `IcebergMvRewriteContext` — pure metadata that future optimizer rewrite
//!   rules (TODO list tasks 2 / 3 / 4) consume.
//! - `IcebergMvRefreshContext` — wraps the rewrite layer and adds the
//!   execution handles only the current refresh path needs.
//!
//! Constructed once per refresh attempt, after pin capture and schema-contract
//! rebind. See `docs/superpowers/specs/2026-05-26-iceberg-mv-rewrite-context-design.md`.

use std::collections::BTreeMap;
use std::sync::Arc;

use iceberg::spec::Schema;

use crate::connector::iceberg::catalog::registry::IcebergCatalogEntry;
use crate::connector::starrocks::managed::model::IcebergTableRef;
use crate::connector::starrocks::managed::refresh_pin::RefreshSnapshotPin;
use crate::meta::repository::mv::StoredMvDefinition;
use crate::meta::repository::mv_contract::MvSchemaContract;

use super::iceberg_refresh::IcebergMvTarget;

/// Read-only metadata that drives Iceberg MV refresh rewrite.
///
/// Future optimizer rewrite rules consume `Arc<IcebergMvRewriteContext>` and
/// MUST NOT depend on `iceberg::table::Table`, `iceberg::Catalog`, or
/// `IcebergCatalogEntry` — those live in `IcebergMvRefreshContext`.
pub(crate) struct IcebergMvRewriteContext {
    // ---- Identity ----
    pub target: IcebergMvTarget,
    pub mv_id: i64,

    // ---- Session ----
    pub current_catalog: Option<String>,
    pub current_database: String,

    // ---- MV definition (post schema-contract rebind) ----
    pub mv_definition: Arc<StoredMvDefinition>,
    pub canonical_select_query: Arc<sqlparser::ast::Query>,

    // ---- Base table inputs ----
    pub base_refs: Arc<[IcebergTableRef]>,
    pub pin: Arc<RefreshSnapshotPin>,
    pub previous_snapshot_ids: BTreeMap<String, i64>,
    pub previous_table_uuids: BTreeMap<String, String>,

    // ---- Target table inputs (extracted from target_table.metadata()) ----
    pub target_snapshot_id: Option<i64>,
    pub target_table_uuid: String,
    pub target_schema: Arc<Schema>,

    // ---- Contracts ----
    pub schema_contract: Arc<MvSchemaContract>,
}

/// Refresh-time context. Wraps `IcebergMvRewriteContext` and adds execution
/// handles only the refresh path needs.
pub(crate) struct IcebergMvRefreshContext {
    pub rewrite: Arc<IcebergMvRewriteContext>,
    pub target_entry: Arc<IcebergCatalogEntry>,
    pub iceberg_catalog: Arc<dyn iceberg::Catalog>,
    pub target_table: iceberg::table::Table,
}
