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

impl IcebergMvRewriteContext {
    /// Build the rewrite layer from already-derived primitive inputs.
    ///
    /// `IcebergMvRefreshContext::new` uses this internally after pulling
    /// `target_snapshot_id` / `target_table_uuid` / `target_schema` out of
    /// `target_table.metadata()`. Unit tests construct the rewrite layer
    /// directly via this helper without needing a real `iceberg::table::Table`.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn from_parts(
        target: IcebergMvTarget,
        mv_id: i64,
        current_catalog: Option<String>,
        current_database: String,
        mv_definition: Arc<StoredMvDefinition>,
        canonical_select_query: Arc<sqlparser::ast::Query>,
        base_refs: Arc<[IcebergTableRef]>,
        pin: Arc<RefreshSnapshotPin>,
        target_snapshot_id: Option<i64>,
        target_table_uuid: String,
        target_schema: Arc<Schema>,
        schema_contract: Arc<MvSchemaContract>,
    ) -> Result<Self, String> {
        let previous_snapshot_ids = mv_definition.last_refresh_snapshots.clone();
        let previous_table_uuids = mv_definition.last_refresh_table_uuids.clone();

        Ok(Self {
            target,
            mv_id,
            current_catalog,
            current_database,
            mv_definition,
            canonical_select_query,
            base_refs,
            pin,
            previous_snapshot_ids,
            previous_table_uuids,
            target_snapshot_id,
            target_table_uuid,
            target_schema,
            schema_contract,
        })
    }
}

impl IcebergMvRefreshContext {
    /// Build the full refresh context from raw inputs. Extracts target
    /// snapshot id / uuid / schema from `target_table.metadata()` and forwards
    /// the rest to `IcebergMvRewriteContext::from_parts`.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        target: IcebergMvTarget,
        mv_id: i64,
        current_catalog: Option<&str>,
        current_database: &str,
        mv_definition: Arc<StoredMvDefinition>,
        canonical_select_query: Arc<sqlparser::ast::Query>,
        base_refs: Arc<[IcebergTableRef]>,
        pin: Arc<RefreshSnapshotPin>,
        target_entry: Arc<IcebergCatalogEntry>,
        iceberg_catalog: Arc<dyn iceberg::Catalog>,
        target_table: iceberg::table::Table,
    ) -> Result<Self, String> {
        let metadata = target_table.metadata();
        let target_snapshot_id = metadata.current_snapshot().map(|s| s.snapshot_id());
        let target_table_uuid = metadata.uuid().to_string();
        let target_schema = metadata.current_schema().clone();
        let schema_contract_opt = mv_definition.schema_contract.clone().map(Arc::new);
        let schema_contract = schema_contract_opt.ok_or_else(|| {
            format!(
                "IcebergMvRewriteContext::new: missing schema contract on target {}.{}.{}; rebuild or recreate the MV",
                target.catalog, target.namespace, target.table
            )
        })?;

        let rewrite = IcebergMvRewriteContext::from_parts(
            target,
            mv_id,
            current_catalog.map(str::to_string),
            current_database.to_string(),
            mv_definition,
            canonical_select_query,
            base_refs,
            pin,
            target_snapshot_id,
            target_table_uuid,
            target_schema,
            schema_contract,
        )?;

        Ok(Self {
            rewrite: Arc::new(rewrite),
            target_entry,
            iceberg_catalog,
            target_table,
        })
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use iceberg::spec::{NestedField, PrimitiveType, Schema, Type};

    use crate::connector::starrocks::managed::model::IcebergTableRef;
    use crate::connector::starrocks::managed::refresh_pin::RefreshSnapshotPin;
    use crate::meta::repository::mv::StoredMvDefinition;
    use crate::meta::repository::mv_contract::{
        ApplyKeySource, BaseContract, BaseFieldRecord, BaseSchemaSnapshot,
        HiddenApplyKeyContract, MvSchemaContract, OutputContract, TargetContract,
        TargetVisibleColumn,
    };

    use super::*;

    fn make_ref(c: &str, n: &str, t: &str) -> IcebergTableRef {
        IcebergTableRef {
            catalog: c.to_string(),
            namespace: n.to_string(),
            table: t.to_string(),
        }
    }

    fn make_pin(entries: &[(&str, i64, &str)]) -> RefreshSnapshotPin {
        RefreshSnapshotPin::from_entries_for_tests(entries)
    }

    fn make_target_schema() -> Arc<Schema> {
        Arc::new(
            Schema::builder()
                .with_schema_id(7)
                .with_fields(vec![
                    Arc::new(NestedField::required(
                        100,
                        "k",
                        Type::Primitive(PrimitiveType::Long),
                    )),
                    Arc::new(NestedField::optional(
                        101,
                        "v",
                        Type::Primitive(PrimitiveType::Long),
                    )),
                ])
                .build()
                .expect("build schema"),
        )
    }

    fn make_schema_contract() -> MvSchemaContract {
        MvSchemaContract {
            contract_version: 3,
            base: BaseContract {
                table_fqn: "ice.db.b".to_string(),
                table_uuid: "uuid-b".to_string(),
                alias_at_create: None,
                schema_id_at_create: 0,
                schema_at_create: BaseSchemaSnapshot {
                    fields: vec![BaseFieldRecord {
                        field_id: 1,
                        name_at_create: "k".to_string(),
                        type_signature: "long".to_string(),
                        required: true,
                    }],
                },
            },
            bases: Vec::new(),
            output: OutputContract {
                columns: Vec::new(),
                filter: None,
            },
            join: None,
            aggregate: None,
            target: TargetContract {
                table_fqn: "tgt.db.mv".to_string(),
                table_uuid: "uuid-tgt".to_string(),
                schema_id_at_create: 7,
                visible_columns: vec![
                    TargetVisibleColumn {
                        output_name: "k".to_string(),
                        target_field_id: 100,
                        type_signature: "long".to_string(),
                        nullable: false,
                    },
                    TargetVisibleColumn {
                        output_name: "v".to_string(),
                        target_field_id: 101,
                        type_signature: "long".to_string(),
                        nullable: true,
                    },
                ],
                hidden_apply_key: HiddenApplyKeyContract {
                    column_name: "k".to_string(),
                    target_field_id: 100,
                    source: ApplyKeySource::BaseRowId,
                },
                partition: None,
            },
        }
    }

    fn make_mv_definition() -> StoredMvDefinition {
        StoredMvDefinition {
            mv_id: 42,
            select_sql: "SELECT k, v FROM ice.db.b".to_string(),
            base_table_refs: vec!["ice.db.b".to_string()],
            primary_key_columns: vec!["k".to_string()],
            storage_engine: "iceberg".to_string(),
            target_catalog: Some("tgt".to_string()),
            target_namespace: Some("db".to_string()),
            target_table: Some("mv".to_string()),
            schema_contract: Some(make_schema_contract()),
            partition_spec: None,
            last_refresh_ms: None,
            last_refresh_rows: None,
            last_refresh_snapshots: [("ice.db.b".to_string(), 11i64)].into_iter().collect(),
            last_refresh_table_uuids: [("ice.db.b".to_string(), "uuid-b".to_string())]
                .into_iter()
                .collect(),
            last_refreshed_iceberg_snapshot_id: Some(99),
            refresh_in_progress: false,
            active_refresh_id: None,
            refresh_target_snapshots: Default::default(),
            refresh_policy: Default::default(),
            refresh_paused: false,
            refresh_interval_ms: None,
            max_staleness_ms: None,
            last_scheduler_error: None,
            next_refresh_after_ms: None,
            created_at_ms: 0,
        }
    }

    fn parse_query(sql: &str) -> sqlparser::ast::Query {
        let dialect = sqlparser::dialect::GenericDialect {};
        let statements =
            sqlparser::parser::Parser::parse_sql(&dialect, sql).expect("parse_sql");
        match statements.into_iter().next().expect("one statement") {
            sqlparser::ast::Statement::Query(q) => *q,
            other => panic!("expected SELECT, got {other:?}"),
        }
    }

    fn make_target() -> IcebergMvTarget {
        IcebergMvTarget {
            catalog: "tgt".to_string(),
            namespace: "db".to_string(),
            table: "mv".to_string(),
        }
    }

    #[test]
    fn from_parts_happy_path_derives_all_fields() {
        let target = make_target();
        let mv_def = Arc::new(make_mv_definition());
        let query = Arc::new(parse_query("SELECT k, v FROM ice.db.b"));
        let base_refs: Arc<[IcebergTableRef]> =
            Arc::from(vec![make_ref("ice", "db", "b")]);
        let pin = Arc::new(make_pin(&[("ice.db.b", 22, "uuid-b")]));
        let schema = make_target_schema();
        let contract = Arc::new(make_schema_contract());

        let ctx = IcebergMvRewriteContext::from_parts(
            target.clone(),
            42,
            Some("sess_cat".to_string()),
            "sess_db".to_string(),
            mv_def.clone(),
            query.clone(),
            base_refs.clone(),
            pin.clone(),
            Some(99),
            "uuid-tgt".to_string(),
            schema.clone(),
            contract.clone(),
        )
        .expect("constructor should succeed on happy path");

        assert_eq!(ctx.target.table, "mv");
        assert_eq!(ctx.mv_id, 42);
        assert_eq!(ctx.current_catalog.as_deref(), Some("sess_cat"));
        assert_eq!(ctx.current_database, "sess_db");
        assert!(Arc::ptr_eq(&ctx.mv_definition, &mv_def));
        assert!(Arc::ptr_eq(&ctx.canonical_select_query, &query));
        assert_eq!(ctx.base_refs.len(), 1);
        assert!(Arc::ptr_eq(&ctx.pin, &pin));
        assert_eq!(ctx.previous_snapshot_ids.get("ice.db.b"), Some(&11));
        assert_eq!(
            ctx.previous_table_uuids.get("ice.db.b").map(String::as_str),
            Some("uuid-b")
        );
        assert_eq!(ctx.target_snapshot_id, Some(99));
        assert_eq!(ctx.target_table_uuid, "uuid-tgt");
        assert!(Arc::ptr_eq(&ctx.target_schema, &schema));
        assert!(Arc::ptr_eq(&ctx.schema_contract, &contract));
    }
}
