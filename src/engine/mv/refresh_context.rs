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

use std::collections::{BTreeMap, BTreeSet};
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
#[derive(Debug)]
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

/// Debug-only view of an `IcebergMvRewriteContext`. No `Display` impl — log
/// via `tracing::info!(summary = ?ctx.rewrite.summary(), ...)`.
#[derive(Debug)]
pub(crate) struct CtxSummary<'a> {
    pub target: &'a IcebergMvTarget,
    pub mv_id: i64,
    pub base_count: usize,
    pub base_fqns: Vec<String>,
    pub pinned_snapshots: Vec<(String, i64)>,
    pub previous_snapshots: Vec<(String, Option<i64>)>,
    pub target_snapshot_id: Option<i64>,
    pub schema_contract_version: u16,
    pub partition_contract_present: bool,
    pub visible_output_column_count: usize,
    pub hidden_apply_key_column: &'a str,
}

fn err(msg: impl Into<String>) -> String {
    format!("IcebergMvRewriteContext::new: {}", msg.into())
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
        schema_contract: Option<Arc<MvSchemaContract>>,
    ) -> Result<Self, String> {
        let target_fqn = format!("{}.{}.{}", target.catalog, target.namespace, target.table);
        let schema_contract = schema_contract.ok_or_else(|| {
            err(format!(
                "missing schema contract on target {target_fqn}; rebuild or recreate the MV"
            ))
        })?;

        if base_refs.is_empty() {
            return Err(err("mv definition has no base table refs"));
        }

        let pin_count = pin.len();
        if pin_count != base_refs.len() {
            return Err(err(format!(
                "refresh pin covers {} bases but definition has {}",
                pin_count,
                base_refs.len()
            )));
        }

        for base_ref in base_refs.iter() {
            if pin.uuid(base_ref).is_none() {
                return Err(err(format!(
                    "refresh pin missing uuid for base {}",
                    base_ref.fqn()
                )));
            }
        }

        let previous_snapshot_ids = mv_definition.last_refresh_snapshots.clone();
        let previous_table_uuids = mv_definition.last_refresh_table_uuids.clone();

        for base_ref in base_refs.iter() {
            let fqn = base_ref.fqn();
            if let Some(previous_uuid) = previous_table_uuids.get(&fqn) {
                let current_uuid = pin
                    .uuid(base_ref)
                    .expect("uuid presence verified above")
                    .to_string();
                if previous_uuid != &current_uuid {
                    return Err(err(format!(
                        "base table identity changed for {fqn}; incremental refresh unsafe, rebuild the MV"
                    )));
                }
            }
        }

        // The target schema is the union of visible columns (those listed in
        // `schema_contract.target.visible_columns`), the hidden apply-key
        // column (`schema_contract.target.hidden_apply_key.target_field_id`),
        // and — for aggregate MVs — the hidden aggregate-state columns listed
        // in `schema_contract.aggregate.state_columns`. The hidden apply-key
        // field id can coincide with a visible column (when the apply-key
        // aliases an existing user column) or be a distinct field (the
        // common case, e.g. `__nova_base_row_id` / `__row_id__`).
        let schema_field_ids: BTreeSet<i32> = target_schema
            .as_ref()
            .as_struct()
            .fields()
            .iter()
            .map(|f| f.id)
            .collect();
        let mut contract_field_ids: BTreeSet<i32> = schema_contract
            .target
            .visible_columns
            .iter()
            .map(|c| c.target_field_id)
            .collect();
        contract_field_ids.insert(schema_contract.target.hidden_apply_key.target_field_id);
        if let Some(aggregate) = &schema_contract.aggregate {
            for state_col in &aggregate.state_columns {
                contract_field_ids.insert(state_col.target_field_id);
            }
        }
        if schema_field_ids != contract_field_ids {
            return Err(err(format!(
                "target schema/contract field id mismatch: schema has {:?}, contract has {:?}",
                schema_field_ids, contract_field_ids
            )));
        }

        let apply_key_name = &schema_contract.target.hidden_apply_key.column_name;
        let apply_key_in_schema = target_schema
            .as_ref()
            .as_struct()
            .fields()
            .iter()
            .any(|f| &f.name == apply_key_name);
        if !apply_key_in_schema {
            return Err(err(format!(
                "target apply-key column {apply_key_name} not present in target schema"
            )));
        }

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

    pub(crate) fn summary(&self) -> CtxSummary<'_> {
        let n = self.base_refs.len();
        let mut base_fqns: Vec<String> = Vec::with_capacity(n);
        let mut pinned_snapshots: Vec<(String, i64)> = Vec::with_capacity(n);
        let mut previous_snapshots: Vec<(String, Option<i64>)> = Vec::with_capacity(n);
        for r in self.base_refs.iter() {
            let fqn = r.fqn();
            let snap = self
                .pin
                .get(r)
                .expect("pin coverage verified in constructor");
            let prev = self.previous_snapshot_ids.get(&fqn).copied();
            pinned_snapshots.push((fqn.clone(), snap));
            previous_snapshots.push((fqn.clone(), prev));
            base_fqns.push(fqn);
        }

        CtxSummary {
            target: &self.target,
            mv_id: self.mv_id,
            base_count: self.base_refs.len(),
            base_fqns,
            pinned_snapshots,
            previous_snapshots,
            target_snapshot_id: self.target_snapshot_id,
            schema_contract_version: self.schema_contract.contract_version,
            partition_contract_present: self.schema_contract.target.partition.is_some(),
            visible_output_column_count: self.schema_contract.target.visible_columns.len(),
            hidden_apply_key_column: &self.schema_contract.target.hidden_apply_key.column_name,
        }
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
        let schema_contract = mv_definition.schema_contract.clone().map(Arc::new);

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
pub(crate) mod tests_support {
    use std::sync::Arc;

    use iceberg::spec::{NestedField, PrimitiveType, Schema, Type};

    use crate::connector::starrocks::managed::model::IcebergTableRef;
    use crate::connector::starrocks::managed::refresh_pin::RefreshSnapshotPin;
    use crate::meta::repository::mv::StoredMvDefinition;
    use crate::meta::repository::mv_contract::{
        ApplyKeySource, BaseContract, BaseFieldRecord, BaseSchemaSnapshot, HiddenApplyKeyContract,
        MvSchemaContract, OutputContract, TargetContract, TargetVisibleColumn,
    };

    use super::*;

    pub(crate) fn make_ref(c: &str, n: &str, t: &str) -> IcebergTableRef {
        IcebergTableRef {
            catalog: c.to_string(),
            namespace: n.to_string(),
            table: t.to_string(),
        }
    }

    pub(crate) fn make_pin(entries: &[(&str, i64, &str)]) -> RefreshSnapshotPin {
        RefreshSnapshotPin::from_entries_for_tests(entries)
    }

    pub(crate) fn make_target_schema() -> Arc<Schema> {
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

    pub(crate) fn make_schema_contract() -> MvSchemaContract {
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

    pub(crate) fn make_mv_definition() -> StoredMvDefinition {
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

    pub(crate) fn parse_query(sql: &str) -> sqlparser::ast::Query {
        let dialect = sqlparser::dialect::GenericDialect {};
        let statements = sqlparser::parser::Parser::parse_sql(&dialect, sql).expect("parse_sql");
        match statements.into_iter().next().expect("one statement") {
            sqlparser::ast::Statement::Query(q) => *q,
            other => panic!("expected SELECT, got {other:?}"),
        }
    }

    pub(crate) fn make_target() -> IcebergMvTarget {
        IcebergMvTarget {
            catalog: "tgt".to_string(),
            namespace: "db".to_string(),
            table: "mv".to_string(),
        }
    }

    /// Returns a minimally-valid `Arc<IcebergMvRewriteContext>` for use in
    /// unit tests outside this module (e.g. `imv/entrypoint.rs`).
    pub(crate) fn dummy_rewrite_context() -> Arc<IcebergMvRewriteContext> {
        let target = make_target();
        let mv_def = Arc::new(make_mv_definition());
        let query = Arc::new(parse_query("SELECT k, v FROM ice.db.b"));
        let base_refs: Arc<[IcebergTableRef]> = Arc::from(vec![make_ref("ice", "db", "b")]);
        let pin = Arc::new(make_pin(&[("ice.db.b", 22, "uuid-b")]));
        let schema = make_target_schema();
        let contract = Arc::new(make_schema_contract());

        Arc::new(
            IcebergMvRewriteContext::from_parts(
                target,
                42,
                Some("sess_cat".to_string()),
                "sess_db".to_string(),
                mv_def,
                query,
                base_refs,
                pin,
                Some(99),
                "uuid-tgt".to_string(),
                schema,
                Some(contract),
            )
            .expect("dummy_rewrite_context: from_parts must succeed on canonical fixture"),
        )
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use iceberg::spec::{NestedField, PrimitiveType, Schema, Type};

    use crate::connector::starrocks::managed::model::IcebergTableRef;
    use crate::connector::starrocks::managed::refresh_pin::RefreshSnapshotPin;
    use crate::meta::repository::mv_contract::{
        AggregateStateColumnContract, AggregateStateContract, AggregateStateRoleContract,
        ApplyKeySource,
    };

    use super::tests_support::*;
    use super::*;

    #[test]
    fn from_parts_happy_path_derives_all_fields() {
        let target = make_target();
        let mv_def = Arc::new(make_mv_definition());
        let query = Arc::new(parse_query("SELECT k, v FROM ice.db.b"));
        let base_refs: Arc<[IcebergTableRef]> = Arc::from(vec![make_ref("ice", "db", "b")]);
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
            Some(contract.clone()),
        )
        .expect("constructor should succeed on happy path");

        assert_eq!(ctx.target.table, "mv");
        assert_eq!(ctx.mv_id, 42);
        assert_eq!(ctx.current_catalog.as_deref(), Some("sess_cat"));
        assert_eq!(ctx.current_database, "sess_db");
        assert!(Arc::ptr_eq(&ctx.mv_definition, &mv_def));
        assert!(Arc::ptr_eq(&ctx.canonical_select_query, &query));
        assert_eq!(ctx.base_refs.len(), 1);
        assert!(Arc::ptr_eq(&ctx.base_refs, &base_refs));
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

    #[test]
    fn from_parts_rejects_missing_schema_contract() {
        let target = make_target();
        let mv_def = Arc::new(make_mv_definition());
        let query = Arc::new(parse_query("SELECT k, v FROM ice.db.b"));
        let base_refs: Arc<[IcebergTableRef]> = Arc::from(vec![make_ref("ice", "db", "b")]);
        let pin = Arc::new(make_pin(&[("ice.db.b", 22, "uuid-b")]));
        let schema = make_target_schema();

        let err_msg = IcebergMvRewriteContext::from_parts(
            target,
            42,
            None,
            "db".to_string(),
            mv_def,
            query,
            base_refs,
            pin,
            Some(99),
            "uuid-tgt".to_string(),
            schema,
            None,
        )
        .expect_err("missing schema contract must fail");
        assert!(
            err_msg.contains("missing schema contract on target tgt.db.mv"),
            "got: {err_msg}"
        );
    }

    #[test]
    fn from_parts_rejects_empty_base_refs() {
        let target = make_target();
        let mv_def = Arc::new(make_mv_definition());
        let query = Arc::new(parse_query("SELECT k, v FROM ice.db.b"));
        let base_refs: Arc<[IcebergTableRef]> = Arc::from(Vec::<IcebergTableRef>::new());
        let pin = Arc::new(RefreshSnapshotPin::default());
        let schema = make_target_schema();
        let contract = Arc::new(make_schema_contract());

        let err = IcebergMvRewriteContext::from_parts(
            target,
            42,
            None,
            "db".to_string(),
            mv_def,
            query,
            base_refs,
            pin,
            Some(99),
            "uuid-tgt".to_string(),
            schema,
            Some(contract),
        )
        .expect_err("empty base_refs must fail");
        assert!(err.contains("no base table refs"), "got: {err}");
    }

    #[test]
    fn from_parts_rejects_pin_coverage_mismatch() {
        let target = make_target();
        let mv_def = Arc::new(make_mv_definition());
        let query = Arc::new(parse_query("SELECT k, v FROM ice.db.b"));
        let base_refs: Arc<[IcebergTableRef]> =
            Arc::from(vec![make_ref("ice", "db", "b"), make_ref("ice", "db", "c")]);
        let pin = Arc::new(make_pin(&[("ice.db.b", 22, "uuid-b")]));
        let schema = make_target_schema();
        let contract = Arc::new(make_schema_contract());

        let err = IcebergMvRewriteContext::from_parts(
            target,
            42,
            None,
            "db".to_string(),
            mv_def,
            query,
            base_refs,
            pin,
            Some(99),
            "uuid-tgt".to_string(),
            schema,
            Some(contract),
        )
        .expect_err("pin coverage mismatch must fail");
        assert!(err.contains("refresh pin covers"), "got: {err}");
    }

    #[test]
    fn from_parts_rejects_pin_missing_uuid() {
        let target = make_target();
        let mv_def = Arc::new(make_mv_definition());
        let query = Arc::new(parse_query("SELECT k, v FROM ice.db.b"));
        let base_refs: Arc<[IcebergTableRef]> = Arc::from(vec![make_ref("ice", "db", "b")]);
        // Pin has the right count but the entry is for a different fqn.
        let pin = Arc::new(make_pin(&[("ice.db.OTHER", 22, "uuid-x")]));
        let schema = make_target_schema();
        let contract = Arc::new(make_schema_contract());

        let err = IcebergMvRewriteContext::from_parts(
            target,
            42,
            None,
            "db".to_string(),
            mv_def,
            query,
            base_refs,
            pin,
            Some(99),
            "uuid-tgt".to_string(),
            schema,
            Some(contract),
        )
        .expect_err("missing pin uuid must fail");
        assert!(
            err.contains("refresh pin missing uuid for base"),
            "got: {err}"
        );
    }

    #[test]
    fn from_parts_rejects_base_identity_drift() {
        let target = make_target();
        let mut def = make_mv_definition();
        def.last_refresh_table_uuids
            .insert("ice.db.b".to_string(), "uuid-OLD".to_string());
        let mv_def = Arc::new(def);
        let query = Arc::new(parse_query("SELECT k, v FROM ice.db.b"));
        let base_refs: Arc<[IcebergTableRef]> = Arc::from(vec![make_ref("ice", "db", "b")]);
        let pin = Arc::new(make_pin(&[("ice.db.b", 22, "uuid-NEW")]));
        let schema = make_target_schema();
        let contract = Arc::new(make_schema_contract());

        let err = IcebergMvRewriteContext::from_parts(
            target,
            42,
            None,
            "db".to_string(),
            mv_def,
            query,
            base_refs,
            pin,
            Some(99),
            "uuid-tgt".to_string(),
            schema,
            Some(contract),
        )
        .expect_err("identity drift must fail");
        assert!(err.contains("base table identity changed"), "got: {err}");
    }

    #[test]
    fn from_parts_first_refresh_passes_with_empty_previous() {
        let target = make_target();
        let mut def = make_mv_definition();
        def.last_refresh_snapshots.clear();
        def.last_refresh_table_uuids.clear();
        let mv_def = Arc::new(def);
        let query = Arc::new(parse_query("SELECT k, v FROM ice.db.b"));
        let base_refs: Arc<[IcebergTableRef]> = Arc::from(vec![make_ref("ice", "db", "b")]);
        let pin = Arc::new(make_pin(&[("ice.db.b", 22, "uuid-b")]));
        let schema = make_target_schema();
        let contract = Arc::new(make_schema_contract());

        let ctx = IcebergMvRewriteContext::from_parts(
            target,
            42,
            None,
            "db".to_string(),
            mv_def,
            query,
            base_refs,
            pin,
            Some(99),
            "uuid-tgt".to_string(),
            schema,
            Some(contract),
        )
        .expect("first refresh must succeed");
        assert!(ctx.previous_snapshot_ids.is_empty());
        assert!(ctx.previous_table_uuids.is_empty());
    }

    #[test]
    fn from_parts_rejects_target_schema_contract_field_mismatch() {
        let target = make_target();
        let mv_def = Arc::new(make_mv_definition());
        let query = Arc::new(parse_query("SELECT k FROM ice.db.b"));
        let base_refs: Arc<[IcebergTableRef]> = Arc::from(vec![make_ref("ice", "db", "b")]);
        let pin = Arc::new(make_pin(&[("ice.db.b", 22, "uuid-b")]));
        let schema = make_target_schema();
        let mut contract = make_schema_contract();
        contract.target.visible_columns.pop();
        let contract = Arc::new(contract);

        let err = IcebergMvRewriteContext::from_parts(
            target,
            42,
            None,
            "db".to_string(),
            mv_def,
            query,
            base_refs,
            pin,
            Some(99),
            "uuid-tgt".to_string(),
            schema,
            Some(contract),
        )
        .expect_err("schema/contract mismatch must fail");
        assert!(
            err.contains("target schema/contract field id mismatch"),
            "got: {err}"
        );
    }

    #[test]
    fn from_parts_rejects_target_schema_contract_field_ids_differ_same_count() {
        let target = make_target();
        let mv_def = Arc::new(make_mv_definition());
        let query = Arc::new(parse_query("SELECT k, v FROM ice.db.b"));
        let base_refs: Arc<[IcebergTableRef]> = Arc::from(vec![make_ref("ice", "db", "b")]);
        let pin = Arc::new(make_pin(&[("ice.db.b", 22, "uuid-b")]));
        let schema = make_target_schema();
        // Contract has two columns (matching schema count) but one has a wrong
        // target_field_id (schema has 100/101; contract claims 100/999).
        let mut contract = make_schema_contract();
        contract.target.visible_columns[1].target_field_id = 999;
        let contract = Arc::new(contract);

        let err = IcebergMvRewriteContext::from_parts(
            target,
            42,
            None,
            "db".to_string(),
            mv_def,
            query,
            base_refs,
            pin,
            Some(99),
            "uuid-tgt".to_string(),
            schema,
            Some(contract),
        )
        .expect_err("field-id set mismatch must fail even when counts match");
        assert!(
            err.contains("target schema/contract field id mismatch"),
            "got: {err}"
        );
    }

    #[test]
    fn summary_orders_by_base_refs_declared_order() {
        let target = make_target();
        let query = Arc::new(parse_query("SELECT k FROM ice.db.b"));
        let base_refs: Arc<[IcebergTableRef]> = Arc::from(vec![
            make_ref("ice", "db", "b"),
            make_ref("ice", "db", "a"),
            make_ref("ice", "db", "c"),
        ]);
        let pin = Arc::new(make_pin(&[
            // Insert in NON-declared order to confirm summary reorders.
            ("ice.db.a", 30, "uuid-a"),
            ("ice.db.c", 50, "uuid-c"),
            ("ice.db.b", 20, "uuid-b"),
        ]));
        let schema = make_target_schema();
        let mut def_for_three_bases = make_mv_definition();
        def_for_three_bases.last_refresh_snapshots.clear();
        def_for_three_bases
            .last_refresh_snapshots
            .insert("ice.db.b".to_string(), 11);
        def_for_three_bases.last_refresh_table_uuids.clear();
        def_for_three_bases
            .last_refresh_table_uuids
            .insert("ice.db.b".to_string(), "uuid-b".to_string());
        def_for_three_bases
            .last_refresh_table_uuids
            .insert("ice.db.a".to_string(), "uuid-a".to_string());
        def_for_three_bases
            .last_refresh_table_uuids
            .insert("ice.db.c".to_string(), "uuid-c".to_string());
        let mv_def = Arc::new(def_for_three_bases);
        let contract = Arc::new(make_schema_contract());

        let ctx = IcebergMvRewriteContext::from_parts(
            target,
            42,
            None,
            "db".to_string(),
            mv_def,
            query,
            base_refs,
            pin,
            Some(99),
            "uuid-tgt".to_string(),
            schema,
            Some(contract),
        )
        .expect("ctx happy path");

        let summary = ctx.summary();
        assert_eq!(
            summary.base_fqns,
            vec![
                "ice.db.b".to_string(),
                "ice.db.a".to_string(),
                "ice.db.c".to_string()
            ],
            "base_fqns must use base_refs declared order"
        );
        assert_eq!(
            summary.pinned_snapshots,
            vec![
                ("ice.db.b".to_string(), 20),
                ("ice.db.a".to_string(), 30),
                ("ice.db.c".to_string(), 50),
            ],
            "summary must use base_refs declared order, not BTreeMap key order"
        );
        assert_eq!(
            summary.previous_snapshots,
            vec![
                ("ice.db.b".to_string(), Some(11)),
                ("ice.db.a".to_string(), None),
                ("ice.db.c".to_string(), None),
            ]
        );
    }

    #[test]
    fn from_parts_rejects_apply_key_not_in_target_schema() {
        let target = make_target();
        let mv_def = Arc::new(make_mv_definition());
        let query = Arc::new(parse_query("SELECT k, v FROM ice.db.b"));
        let base_refs: Arc<[IcebergTableRef]> = Arc::from(vec![make_ref("ice", "db", "b")]);
        let pin = Arc::new(make_pin(&[("ice.db.b", 22, "uuid-b")]));
        let schema = make_target_schema();
        let mut contract = make_schema_contract();
        contract.target.hidden_apply_key.column_name = "nonexistent".to_string();
        let contract = Arc::new(contract);

        let err = IcebergMvRewriteContext::from_parts(
            target,
            42,
            None,
            "db".to_string(),
            mv_def,
            query,
            base_refs,
            pin,
            Some(99),
            "uuid-tgt".to_string(),
            schema,
            Some(contract),
        )
        .expect_err("apply-key absence must fail");
        assert!(err.contains("apply-key column"), "got: {err}");
    }

    #[test]
    fn from_parts_succeeds_with_distinct_hidden_apply_key_field() {
        let target = make_target();
        let mv_def = Arc::new(make_mv_definition());
        let query = Arc::new(parse_query("SELECT k, v FROM ice.db.b"));
        let base_refs: Arc<[IcebergTableRef]> = Arc::from(vec![make_ref("ice", "db", "b")]);
        let pin = Arc::new(make_pin(&[("ice.db.b", 22, "uuid-b")]));

        // Three-field target schema: 100=k (visible), 101=v (visible),
        // 999=__nova_apply_key (hidden — present in schema but NOT in
        // visible_columns).
        let schema = Arc::new(
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
                    Arc::new(NestedField::required(
                        999,
                        "__nova_apply_key",
                        Type::Primitive(PrimitiveType::Long),
                    )),
                ])
                .build()
                .expect("build schema"),
        );

        // Contract: visible columns are 100/101; hidden apply key is 999.
        let mut contract = make_schema_contract();
        contract.target.hidden_apply_key.column_name = "__nova_apply_key".to_string();
        contract.target.hidden_apply_key.target_field_id = 999;
        let contract = Arc::new(contract);

        IcebergMvRewriteContext::from_parts(
            target,
            42,
            None,
            "db".to_string(),
            mv_def,
            query,
            base_refs,
            pin,
            Some(99),
            "uuid-tgt".to_string(),
            schema,
            Some(contract),
        )
        .expect("ctx must succeed when apply-key is a distinct hidden schema field");
    }

    #[test]
    fn from_parts_succeeds_with_aggregate_state_columns_in_schema() {
        let target = make_target();
        let mv_def = Arc::new(make_mv_definition());
        let query = Arc::new(parse_query("SELECT k, v FROM ice.db.b"));
        let base_refs: Arc<[IcebergTableRef]> = Arc::from(vec![make_ref("ice", "db", "b")]);
        let pin = Arc::new(make_pin(&[("ice.db.b", 22, "uuid-b")]));

        // Aggregate target schema: visible columns 100=k and 101=v, hidden
        // apply key 999=__row_id__, and aggregate-state columns
        // 200=__agg_state_c, 201=__agg_state_s. All must be accepted by
        // from_parts.
        let schema = Arc::new(
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
                    Arc::new(NestedField::required(
                        999,
                        "__row_id__",
                        Type::Primitive(PrimitiveType::String),
                    )),
                    Arc::new(NestedField::optional(
                        200,
                        "__agg_state_c",
                        Type::Primitive(PrimitiveType::Long),
                    )),
                    Arc::new(NestedField::optional(
                        201,
                        "__agg_state_s",
                        Type::Primitive(PrimitiveType::Long),
                    )),
                ])
                .build()
                .expect("build schema"),
        );

        let mut contract = make_schema_contract();
        contract.target.hidden_apply_key.column_name = "__row_id__".to_string();
        contract.target.hidden_apply_key.target_field_id = 999;
        contract.target.hidden_apply_key.source = ApplyKeySource::GroupRowId;
        contract.aggregate = Some(AggregateStateContract {
            state_layout_version: 1,
            row_id_column_name: "__row_id__".to_string(),
            state_columns: vec![
                AggregateStateColumnContract {
                    column_name: "__agg_state_c".to_string(),
                    target_field_id: 200,
                    type_signature: "long".to_string(),
                    nullable: true,
                    role: AggregateStateRoleContract::Single,
                },
                AggregateStateColumnContract {
                    column_name: "__agg_state_s".to_string(),
                    target_field_id: 201,
                    type_signature: "long".to_string(),
                    nullable: true,
                    role: AggregateStateRoleContract::Single,
                },
            ],
        });
        let contract = Arc::new(contract);

        IcebergMvRewriteContext::from_parts(
            target,
            42,
            None,
            "db".to_string(),
            mv_def,
            query,
            base_refs,
            pin,
            Some(99),
            "uuid-tgt".to_string(),
            schema,
            Some(contract),
        )
        .expect("ctx must accept aggregate state columns in target schema");
    }
}
