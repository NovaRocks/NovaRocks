# Iceberg MV Rewrite Context Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Introduce a two-layer immutable refresh-time context (`IcebergMvRewriteContext` ⊂ `IcebergMvRefreshContext`) that collapses the ≥12-arg refresh-helper signatures in `src/engine/mv/iceberg_refresh.rs` into `(state, &ctx, ...shape-specific args)` and centralises self-check, with no change to refresh semantics.

**Architecture:** New module `src/engine/mv/refresh_context.rs` holds both context structs. Construction happens after pin capture and schema-contract rebind inside each shape's top-level helper. Sub-helpers (`first_refresh_iceberg_mv`, `incremental_refresh_iceberg_mv`, `refresh_single_aggregate_iceberg_mv`, `refresh_join_aggregate_iceberg_mv`, and join-shape equivalents) take `&IcebergMvRefreshContext` and read `.rewrite.*` for metadata. Future optimizer rules (TODO tasks 2 / 3 / 4) consume only `Arc<IcebergMvRewriteContext>` — they never see `iceberg::table::Table`, `iceberg::Catalog`, or `IcebergCatalogEntry`.

**Tech Stack:** Rust, `iceberg` crate, `sqlparser` crate, `tokio` runtime via existing NovaRocks `StandaloneState`. Tests use `cargo test`; SQL regression uses `tests/sql-test-runner` against a NovaRocks standalone-server backed by the docker iceberg-rest + MinIO fixture in [docker/iceberg-rest/](docker/iceberg-rest/).

---

## File Structure

| File | Responsibility | Action |
|---|---|---|
| [`src/engine/mv/refresh_context.rs`](src/engine/mv/refresh_context.rs) | New module holding `IcebergMvRewriteContext`, `IcebergMvRefreshContext`, `CtxSummary<'a>`, constructors, self-check, summary, unit tests. | Create |
| [`src/engine/mv/mod.rs`](src/engine/mv/mod.rs) | Re-export the new module. | Modify (1 line added) |
| [`src/engine/mv/iceberg_refresh.rs`](src/engine/mv/iceberg_refresh.rs) | Construct ctx at three shape sites; migrate immediate sub-helpers to take `&ctx`; remove duplicate parsing of base_refs / previous_snapshot maps / target snapshot id / target uuid. | Modify |

No other files change in this plan. Deeper sub-helpers in `iceberg_aggregate_state.rs`, `iceberg_join_branch.rs`, `iceberg_join_coalesce.rs`, `iceberg_merge_sink.rs`, `iceberg_target_apply.rs` stay on their current signatures — collapsing those is deferred per the spec §6 "incidental call sites that take only one or two can keep their existing form to limit blast radius".

---

## Environment Setup (Required for SQL Regression Tasks 5, 6, 7, 8)

Read [CLAUDE.md §7.3](CLAUDE.md) for the canonical procedure. Summary:

```bash
source docker/iceberg-rest/runtime/current/env.sh
docker/iceberg-rest/up.sh
```

Then in a separate terminal start the standalone-server (so SQL tests can connect):

```bash
LOG=/tmp/novarocks-server.log
NO_PROXY=127.0.0.1,localhost target/debug/novarocks standalone-server \
  --config "$NOVAROCKS_STANDALONE_CONFIG" >"$LOG" 2>&1 &
SRV_PID=$!
for i in $(seq 1 60); do
  if grep -q '^NOVAROCKS_READY ' "$LOG"; then break; fi
  if ! kill -0 "$SRV_PID" 2>/dev/null; then
    echo "standalone-server died during startup; tail of $LOG:" >&2
    tail -20 "$LOG" >&2
    exit 1
  fi
  sleep 1
done
grep -q '^NOVAROCKS_READY ' "$LOG" || { echo "timed out"; kill -9 "$SRV_PID"; exit 1; }
```

`iceberg-ivm` SQL suite invocation (used at every checkpoint):

```bash
cargo run --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests -- \
  --config "$NOVAROCKS_SQL_TEST_CONFIG" --suite iceberg-ivm --mode verify
```

---

### Task 1: Create the `refresh_context` module skeleton

**Files:**
- Create: [`src/engine/mv/refresh_context.rs`](src/engine/mv/refresh_context.rs)
- Modify: [`src/engine/mv/mod.rs`](src/engine/mv/mod.rs) (add 1 line)

This task creates the module with type declarations only — no logic, no constructor. The struct fields are declared so later tasks can fill them in. We commit this as a compile-only step.

- [ ] **Step 1: Create `src/engine/mv/refresh_context.rs` with type declarations**

```rust
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
```

- [ ] **Step 2: Add the module to `src/engine/mv/mod.rs`**

Modify [`src/engine/mv/mod.rs`](src/engine/mv/mod.rs). It currently lists modules alphabetically; insert `refresh_context` in alphabetical order between `rebind` and `schema_contract`:

```rust
pub(crate) mod dependency;
pub(crate) mod iceberg_aggregate_state;
pub(crate) mod iceberg_backend;
pub(crate) mod iceberg_join_branch;
pub(crate) mod iceberg_join_coalesce;
pub(crate) mod iceberg_merge_sink;
pub(crate) mod iceberg_refresh;
pub(crate) mod iceberg_target_apply;
pub(crate) mod lifecycle;
pub(crate) mod partition;
pub(crate) mod rebind;
pub(crate) mod refresh_context;
pub(crate) mod schema_contract;
```

- [ ] **Step 3: Verify it compiles**

Run: `cargo build --lib`
Expected: compiles clean. The new module is referenced from `mod.rs`, the types are declared, no callers exist yet — the compiler may warn about unused fields. That's fine for now (the warnings disappear once tests in Task 2 use the fields).

If `cargo build` fails because `IcebergMvTarget` is `pub(crate)` but only visible via `super::iceberg_refresh::IcebergMvTarget`, that's already the existing visibility — no change needed. If it fails because of an import path mismatch, double-check against [src/engine/mv/iceberg_refresh.rs:64](src/engine/mv/iceberg_refresh.rs:64), [src/connector/starrocks/managed/model.rs:63](src/connector/starrocks/managed/model.rs:63), [src/connector/iceberg/catalog/registry.rs:60](src/connector/iceberg/catalog/registry.rs:60), [src/meta/repository/mv.rs:24](src/meta/repository/mv.rs:24), [src/meta/repository/mv_contract.rs:10](src/meta/repository/mv_contract.rs:10).

- [ ] **Step 4: Commit**

```bash
git add src/engine/mv/refresh_context.rs src/engine/mv/mod.rs
git commit -m "engine/mv: add refresh_context module skeleton (no logic yet)"
```

---

### Task 2: Add the constructor with happy-path test (TDD)

**Files:**
- Modify: [`src/engine/mv/refresh_context.rs`](src/engine/mv/refresh_context.rs)
- Modify: [`src/connector/starrocks/managed/refresh_pin.rs`](src/connector/starrocks/managed/refresh_pin.rs) (add a test-only constructor)

Add `IcebergMvRewriteContext::from_parts(...)` (pub(crate) helper used by both the outer `IcebergMvRefreshContext::new` and unit tests) and `IcebergMvRefreshContext::new(...)`. This task is the field derivation only — no self-check yet (that's Task 3). The happy-path test locks down derivation.

`RefreshSnapshotPin` has private `snapshots` / `table_uuids` fields ([refresh_pin.rs:30-31](src/connector/starrocks/managed/refresh_pin.rs:30)) and only exposes read accessors. Tests outside `refresh_pin.rs` cannot construct a pin with entries, so we add a `#[cfg(test)] pub(crate) fn from_entries_for_tests` test helper that mirrors the in-module `make_pin` helper at [refresh_pin.rs:338](src/connector/starrocks/managed/refresh_pin.rs:338).

- [ ] **Step 1a: Add the test-only pin builder to `refresh_pin.rs`**

Insert immediately after the closing brace of `impl RefreshSnapshotPin { ... }` block ([refresh_pin.rs:98](src/connector/starrocks/managed/refresh_pin.rs:98)):

```rust
#[cfg(test)]
impl RefreshSnapshotPin {
    /// Build a pin with explicit entries; for use from other modules' unit
    /// tests that need to construct a `RefreshSnapshotPin` without going
    /// through `capture`. Each tuple is `(fqn, snapshot_id, table_uuid)`.
    pub(crate) fn from_entries_for_tests(entries: &[(&str, i64, &str)]) -> Self {
        let mut pin = RefreshSnapshotPin::default();
        for (fqn, snapshot_id, uuid) in entries {
            pin.snapshots.insert((*fqn).to_string(), *snapshot_id);
            pin.table_uuids
                .insert((*fqn).to_string(), (*uuid).to_string());
        }
        pin
    }
}
```

- [ ] **Step 1b: Write the failing happy-path test**

Append to [`src/engine/mv/refresh_context.rs`](src/engine/mv/refresh_context.rs):

```rust
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
        // Lightweight: use sqlparser directly with the default dialect.
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
```

- [ ] **Step 2: Run the test, confirm it fails to compile**

Run: `cargo test -p novarocks --lib engine::mv::refresh_context::tests::from_parts_happy_path -- --nocapture`

Expected: compile error — `IcebergMvRewriteContext::from_parts` does not exist.

- [ ] **Step 3: Implement `from_parts` (no checks, just field copy)**

Insert above the `#[cfg(test)] mod tests` block in [`src/engine/mv/refresh_context.rs`](src/engine/mv/refresh_context.rs):

```rust
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
```

- [ ] **Step 4: Re-run the test and confirm it passes**

Run: `cargo test -p novarocks --lib engine::mv::refresh_context::tests::from_parts_happy_path -- --nocapture`

Expected: PASS.

- [ ] **Step 5: Add `IcebergMvRefreshContext::new` wrapping `from_parts`**

Append (still in the same file, outside the `#[cfg(test)]` block):

```rust
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
        let schema_contract = mv_definition
            .schema_contract
            .clone()
            .map(Arc::new);

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
```

- [ ] **Step 6: Build to confirm everything still compiles**

Run: `cargo build --lib`
Expected: clean compile, no errors.

- [ ] **Step 7: Commit**

```bash
git add src/engine/mv/refresh_context.rs
git commit -m "engine/mv: rewrite/refresh context constructor + happy-path test"
```

---

### Task 3: Self-check rules (TDD: one test per failure mode)

**Files:**
- Modify: [`src/engine/mv/refresh_context.rs`](src/engine/mv/refresh_context.rs)

Wire the seven self-check rules from spec §4.5 into `IcebergMvRewriteContext::from_parts`. One failing test per rule first, then implement the rule, then re-run.

For each rule, the cycle is: write failing test → implement check → re-run → commit incrementally (one commit per rule keeps PR review tractable, or one combined commit at the end — your call).

The error message format is `IcebergMvRewriteContext::new: <specific message>`. Add a helper:

```rust
fn err(msg: impl Into<String>) -> String {
    format!("IcebergMvRewriteContext::new: {}", msg.into())
}
```

Place `fn err` next to `from_parts` (private to the module).

- [ ] **Step 1: Refactor `from_parts` to take `Option<Arc<MvSchemaContract>>` so the missing-contract check is unit-testable**

Change `IcebergMvRewriteContext::from_parts`'s `schema_contract` parameter from `Arc<MvSchemaContract>` to `Option<Arc<MvSchemaContract>>`. At the top of the body, before any other checks, add:

```rust
let target_fqn = format!(
    "{}.{}.{}",
    target.catalog, target.namespace, target.table
);
let schema_contract = schema_contract.ok_or_else(|| {
    err(format!(
        "missing schema contract on target {target_fqn}; rebuild or recreate the MV"
    ))
})?;
```

Also update `IcebergMvRefreshContext::new` to pass `mv_definition.schema_contract.clone().map(Arc::new)` directly without unwrapping at that layer.

Update the happy-path test to wrap its contract in `Some(...)`:

```rust
// was: let contract = Arc::new(make_schema_contract());
let contract = Some(Arc::new(make_schema_contract()));
```

…and similarly for every other test that passes `contract` to `from_parts`. Run `cargo build --lib` to confirm; expect compile errors only at the test call sites (fix them by wrapping with `Some(...)`).

Add the missing-contract test:

```rust
#[test]
fn from_parts_rejects_missing_schema_contract() {
    let target = make_target();
    let mv_def = Arc::new(make_mv_definition());
    let query = Arc::new(parse_query("SELECT k, v FROM ice.db.b"));
    let base_refs: Arc<[IcebergTableRef]> = Arc::from(vec![make_ref("ice", "db", "b")]);
    let pin = Arc::new(make_pin(&[("ice.db.b", 22, "uuid-b")]));
    let schema = make_target_schema();

    let err_msg = IcebergMvRewriteContext::from_parts(
        target, 42, None, "db".to_string(),
        mv_def, query, base_refs, pin,
        Some(99), "uuid-tgt".to_string(), schema, None,
    )
    .expect_err("missing schema contract must fail");
    assert!(
        err_msg.contains("missing schema contract on target tgt.db.mv"),
        "got: {err_msg}"
    );
}
```

Run; expected PASS once the `Option` refactor and the new check are in.

- [ ] **Step 2: Test — empty base_refs**

Append in `mod tests`:

```rust
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
        target, 42, None, "db".to_string(),
        mv_def, query, base_refs, pin,
        Some(99), "uuid-tgt".to_string(), schema, contract,
    )
    .expect_err("empty base_refs must fail");
    assert!(err.contains("no base table refs"), "got: {err}");
}
```

- [ ] **Step 3: Run, confirm fails** (`cargo test -p novarocks --lib engine::mv::refresh_context::tests::from_parts_rejects_empty_base_refs`). Expected: assertion failure (constructor returned `Ok`).

- [ ] **Step 4: Implement rule** in `from_parts`, right after the `previous_snapshot_ids` derivation:

```rust
if base_refs.is_empty() {
    return Err(err("mv definition has no base table refs"));
}
```

Run the test again; expected PASS.

- [ ] **Step 5: Test — pin coverage mismatch**

Append:

```rust
#[test]
fn from_parts_rejects_pin_coverage_mismatch() {
    let target = make_target();
    let mv_def = Arc::new(make_mv_definition());
    let query = Arc::new(parse_query("SELECT k, v FROM ice.db.b"));
    let base_refs: Arc<[IcebergTableRef]> =
        Arc::from(vec![make_ref("ice", "db", "b"), make_ref("ice", "db", "c")]);
    // Pin only covers one of the two bases.
    let pin = Arc::new(make_pin(&[("ice.db.b", 22, "uuid-b")]));
    let schema = make_target_schema();
    let contract = Arc::new(make_schema_contract());

    let err = IcebergMvRewriteContext::from_parts(
        target, 42, None, "db".to_string(),
        mv_def, query, base_refs, pin,
        Some(99), "uuid-tgt".to_string(), schema, contract,
    )
    .expect_err("pin coverage mismatch must fail");
    assert!(err.contains("refresh pin covers"), "got: {err}");
}
```

- [ ] **Step 6: Run, confirm fails. Then implement rule** in `from_parts`:

```rust
let pin_count = pin.len();
if pin_count != base_refs.len() {
    return Err(err(format!(
        "refresh pin covers {} bases but definition has {}",
        pin_count,
        base_refs.len()
    )));
}
```

`RefreshSnapshotPin::len()` is `pub(crate)` and returns the count of entries in `snapshots` ([refresh_pin.rs:79](src/connector/starrocks/managed/refresh_pin.rs:79)).

Re-run; expected PASS.

- [ ] **Step 7: Test — missing pin uuid for some base**

Append:

```rust
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
        target, 42, None, "db".to_string(),
        mv_def, query, base_refs, pin,
        Some(99), "uuid-tgt".to_string(), schema, contract,
    )
    .expect_err("missing pin uuid must fail");
    assert!(err.contains("refresh pin missing uuid for base"), "got: {err}");
}
```

- [ ] **Step 8: Run, confirm fails. Then implement rule** (after the pin-count check):

```rust
for base_ref in base_refs.iter() {
    if pin.uuid(base_ref).is_none() {
        return Err(err(format!(
            "refresh pin missing uuid for base {}",
            base_ref.fqn()
        )));
    }
}
```

Re-run; expected PASS.

- [ ] **Step 9: Test — base identity drift (previous uuid != pin uuid)**

Append:

```rust
#[test]
fn from_parts_rejects_base_identity_drift() {
    let target = make_target();
    // Definition says we previously refreshed against uuid-OLD, but pin says
    // current uuid-NEW. Identity changed → reject.
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
        target, 42, None, "db".to_string(),
        mv_def, query, base_refs, pin,
        Some(99), "uuid-tgt".to_string(), schema, contract,
    )
    .expect_err("identity drift must fail");
    assert!(
        err.contains("base table identity changed"),
        "got: {err}"
    );
}
```

- [ ] **Step 10: Run, confirm fails. Then implement rule** (after the pin-uuid presence loop):

```rust
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
```

Re-run; expected PASS.

- [ ] **Step 11: Test — first refresh has no `previous_table_uuids` and must NOT trigger drift check**

```rust
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
        target, 42, None, "db".to_string(),
        mv_def, query, base_refs, pin,
        Some(99), "uuid-tgt".to_string(), schema, contract,
    )
    .expect("first refresh must succeed");
    assert!(ctx.previous_snapshot_ids.is_empty());
    assert!(ctx.previous_table_uuids.is_empty());
}
```

Run; expected PASS already (the drift loop is conditional on `previous_table_uuids` having an entry). No new implementation needed.

- [ ] **Step 12: Test — target schema vs visible_columns mismatch**

```rust
#[test]
fn from_parts_rejects_target_schema_contract_field_mismatch() {
    let target = make_target();
    let mv_def = Arc::new(make_mv_definition());
    let query = Arc::new(parse_query("SELECT k FROM ice.db.b"));
    let base_refs: Arc<[IcebergTableRef]> = Arc::from(vec![make_ref("ice", "db", "b")]);
    let pin = Arc::new(make_pin(&[("ice.db.b", 22, "uuid-b")]));
    let schema = make_target_schema();
    // Contract drops one visible column → mismatch.
    let mut contract = make_schema_contract();
    contract.target.visible_columns.pop();
    let contract = Arc::new(contract);

    let err = IcebergMvRewriteContext::from_parts(
        target, 42, None, "db".to_string(),
        mv_def, query, base_refs, pin,
        Some(99), "uuid-tgt".to_string(), schema, contract,
    )
    .expect_err("schema/contract mismatch must fail");
    assert!(
        err.contains("target schema/contract column count mismatch"),
        "got: {err}"
    );
}
```

- [ ] **Step 13: Run, confirm fails. Then implement rule** (after identity drift loop):

```rust
let schema_field_ids: std::collections::BTreeSet<i32> = target_schema
    .as_ref()
    .as_struct()
    .fields()
    .iter()
    .map(|f| f.id)
    .collect();
let contract_field_ids: std::collections::BTreeSet<i32> = schema_contract
    .target
    .visible_columns
    .iter()
    .map(|c| c.target_field_id)
    .collect();
if schema_field_ids != contract_field_ids {
    return Err(err(format!(
        "target schema/contract column count mismatch: schema has {}, contract has {}",
        schema_field_ids.len(),
        contract_field_ids.len()
    )));
}
```

`Schema::as_struct().fields()` is the canonical iter over schema fields with their `field_id`s. If the iceberg crate exposes a different idiomatic accessor (`schema.fields()`, `schema.as_struct().fields()`), use whichever compiles and yields `(id: i32, name)`. Cross-check against [src/connector/iceberg/commit/snapshot_lifecycle_helpers.rs:166](src/connector/iceberg/commit/snapshot_lifecycle_helpers.rs:166) which uses `Schema::builder().with_fields(vec![Arc::new(NestedField::required(1, ...))])` so each field's id is `NestedField.id` — adjust accordingly.

Re-run; expected PASS.

- [ ] **Step 14: Test — hidden apply key not in target schema**

```rust
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
        target, 42, None, "db".to_string(),
        mv_def, query, base_refs, pin,
        Some(99), "uuid-tgt".to_string(), schema, contract,
    )
    .expect_err("apply-key absence must fail");
    assert!(err.contains("apply-key column"), "got: {err}");
}
```

- [ ] **Step 15: Run, confirm fails. Then implement rule** (after schema/contract field-id mismatch check):

```rust
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
```

Re-run; expected PASS. Confirm the happy-path test from Task 2 still passes too.

- [ ] **Step 16: Commit**

```bash
git add src/engine/mv/refresh_context.rs
git commit -m "engine/mv: self-check rules in IcebergMvRewriteContext::from_parts"
```

---

### Task 4: `summary()` method + ordering test

**Files:**
- Modify: [`src/engine/mv/refresh_context.rs`](src/engine/mv/refresh_context.rs)

- [ ] **Step 1: Add the `CtxSummary<'a>` struct (above `impl IcebergMvRewriteContext`)**

```rust
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
```

- [ ] **Step 2: Write the failing ordering test**

Append in `mod tests`:

```rust
#[test]
fn summary_orders_by_base_refs_declared_order() {
    let target = make_target();
    let mv_def = Arc::new(make_mv_definition());
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
    def_for_three_bases.last_refresh_snapshots.insert("ice.db.b".to_string(), 11);
    def_for_three_bases.last_refresh_table_uuids.clear();
    def_for_three_bases.last_refresh_table_uuids.insert("ice.db.b".to_string(), "uuid-b".to_string());
    def_for_three_bases.last_refresh_table_uuids.insert("ice.db.a".to_string(), "uuid-a".to_string());
    def_for_three_bases.last_refresh_table_uuids.insert("ice.db.c".to_string(), "uuid-c".to_string());
    let mv_def = Arc::new(def_for_three_bases);
    let contract = Arc::new(make_schema_contract());

    let ctx = IcebergMvRewriteContext::from_parts(
        target, 42, None, "db".to_string(),
        mv_def, query, base_refs, pin,
        Some(99), "uuid-tgt".to_string(), schema, contract,
    )
    .expect("ctx happy path");

    let summary = ctx.summary();
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
```

- [ ] **Step 3: Run, confirm compile failure (no `summary()` yet)**

Run: `cargo test -p novarocks --lib engine::mv::refresh_context::tests::summary_orders_by_base_refs_declared_order`
Expected: compile error — `summary` not defined.

- [ ] **Step 4: Implement `summary()`**

Append to the `impl IcebergMvRewriteContext` block in [refresh_context.rs](src/engine/mv/refresh_context.rs):

```rust
pub(crate) fn summary(&self) -> CtxSummary<'_> {
    let base_fqns: Vec<String> = self.base_refs.iter().map(|r| r.fqn()).collect();
    let pinned_snapshots: Vec<(String, i64)> = self
        .base_refs
        .iter()
        .map(|r| {
            let snap = self
                .pin
                .get(r)
                .expect("pin coverage verified in constructor");
            (r.fqn(), snap)
        })
        .collect();
    let previous_snapshots: Vec<(String, Option<i64>)> = self
        .base_refs
        .iter()
        .map(|r| {
            let fqn = r.fqn();
            let prev = self.previous_snapshot_ids.get(&fqn).copied();
            (fqn, prev)
        })
        .collect();

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
```

- [ ] **Step 5: Re-run, expected PASS.** Then run the full module test set:

`cargo test -p novarocks --lib engine::mv::refresh_context`

Expected: all tests pass (happy path + 6 negative + first refresh + summary ordering = at least 9 tests).

- [ ] **Step 6: Commit**

```bash
git add src/engine/mv/refresh_context.rs
git commit -m "engine/mv: add IcebergMvRewriteContext::summary with stable ordering"
```

---

### Task 5: Wire ctx into the ProjectionFilter shape path

**Files:**
- Modify: [`src/engine/mv/iceberg_refresh.rs`](src/engine/mv/iceberg_refresh.rs)

This is the simplest of the three integration sites: the projection/filter shape is handled inline in `refresh_iceberg_mv` (the path falls through after the aggregate and join shape branches). Construct ctx after the rebind block ends, then pass `&ctx` into the two sub-helpers that dispatch from the `(previous, current)` match.

Look at [src/engine/mv/iceberg_refresh.rs:1382-1500](src/engine/mv/iceberg_refresh.rs:1382). Identify which helpers are called from the match arms. Currently the relevant ones include `first_refresh_iceberg_mv` and `incremental_refresh_iceberg_mv` (locate them via `grep -n "^fn first_refresh_iceberg_mv\|^fn incremental_refresh_iceberg_mv" src/engine/mv/iceberg_refresh.rs`). Each takes ≥10 individual arguments.

- [ ] **Step 1: Identify the integration site**

Run: `grep -n "fn first_refresh_iceberg_mv\|fn incremental_refresh_iceberg_mv" src/engine/mv/iceberg_refresh.rs`

Note the file:line locations. Read each function signature so you know which args become `ctx.rewrite.<field>` lookups vs. which args remain (e.g., the shape-specific `staging_branch`, `refresh_id`, `pin: &RefreshSnapshotPin` if you can read it from `ctx.rewrite.pin` instead, `current_snapshot_id` from `pin.get(base_ref)`, etc.).

- [ ] **Step 2: Construct ctx in the projection/filter path**

Locate the line after `let pinned_full_select_sql = ...` and `let expected_main_snapshot_id = ...` and `let staging_branch = format!(...)` ([iceberg_refresh.rs:1382-1392](src/engine/mv/iceberg_refresh.rs:1382)). Just before the `match (previous_snapshot_id, current_snapshot_id) {` block, insert:

```rust
let ctx = crate::engine::mv::refresh_context::IcebergMvRefreshContext::new(
    target.clone(),
    mv_definition.mv_id,
    current_catalog,
    current_database,
    Arc::new(mv_definition.clone()),
    Arc::new(canonical_select_query.clone()),
    Arc::from(base_refs.clone()),
    Arc::new(pin.clone()),
    Arc::new(target_entry.clone()),
    iceberg_catalog.clone(),
    target_loaded.table.clone(),
)?;
tracing::info!(
    summary = ?ctx.rewrite.summary(),
    "iceberg MV refresh context constructed"
);
```

Local types referenced (`target`, `mv_definition`, `canonical_select_query`, `base_refs`, `pin`, `target_entry`, `iceberg_catalog`, `target_loaded`) are all in scope from earlier in `refresh_iceberg_mv`. `IcebergMvTarget` is `Clone` (verify via [iceberg_refresh.rs:64](src/engine/mv/iceberg_refresh.rs:64); if not, derive `Clone`).

Note: `mv_definition` here is `&effective_definition` after the rebind block; `.clone()` materializes the post-rebind definition into the `Arc`.

- [ ] **Step 3: Migrate `first_refresh_iceberg_mv` signature**

Pick the function (e.g., `first_refresh_iceberg_mv`) and rewrite its signature to:

```rust
fn first_refresh_iceberg_mv(
    state: &Arc<StandaloneState>,
    ctx: &IcebergMvRefreshContext,
    staging_branch: &str,
    refresh_id: i64,
    base_ref: &IcebergTableRef,
    current_snapshot_id: i64,
) -> Result<StatementResult, String> {
    // inside the body, replace:
    //   target            -> &ctx.rewrite.target
    //   mv_definition     -> &*ctx.rewrite.mv_definition
    //   target_entry      -> &*ctx.rewrite.target_entry (path: &ctx.target_entry — note rewrite vs outer)
    //   iceberg_catalog   -> &ctx.iceberg_catalog
    //   target_table      -> &ctx.target_table
    //   expected_main_snapshot_id -> ctx.rewrite.target_snapshot_id
    //   current_database  -> &ctx.rewrite.current_database
    //   pinned_full_select_sql / current_table_uuid stay as args if they are
    //     shape-specific (recompute or keep them as fn-local from caller).
    ...
}
```

`pinned_full_select_sql` and `current_table_uuid` are derived inside the projection/filter path and stay as inline args; do not bake them into ctx.

Adjust the corresponding call site (inside the `(None, Some(cur))` arm of the match) to pass `&ctx` plus the remaining shape-specific args.

- [ ] **Step 4: Migrate `incremental_refresh_iceberg_mv` signature**

Same pattern as Step 3 — collapse `target`, `target_entry`, `iceberg_catalog`, `target_table`, `expected_main_snapshot_id`, `mv_definition`, `base_refs`, `current_database` into `&ctx`. Keep `staging_branch`, `refresh_id`, `pinned_full_select_sql`, `prev`, `cur`, `base_ref`, `current_table_uuid` as shape-specific args.

Adjust the call site (inside the `(Some(prev), Some(cur)) if prev != cur` arm).

- [ ] **Step 5: Build**

Run: `cargo build --lib`

Expected: clean. Fix any compilation errors by matching the new signatures.

- [ ] **Step 6: Verify nothing in the unchanged paths broke**

Run: `cargo test -p novarocks --lib`

Expected: all unit tests pass.

- [ ] **Step 7: Run the `iceberg-ivm` SQL suite**

Start (or reuse) the standalone-server per the Environment Setup section.

```bash
cargo run --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests -- \
  --config "$NOVAROCKS_SQL_TEST_CONFIG" --suite iceberg-ivm --mode verify
```

Expected: all cases pass. If a case fails, the most likely cause is a stale field lookup in the migrated helpers — re-read the diff and check whether you mapped each old arg to the right `ctx.rewrite.*` or `ctx.*` field.

- [ ] **Step 8: Commit**

```bash
git add src/engine/mv/iceberg_refresh.rs
git commit -m "engine/mv: thread refresh context through ProjectionFilter shape"
```

---

### Task 6: Wire ctx into the Aggregate shape path

**Files:**
- Modify: [`src/engine/mv/iceberg_refresh.rs`](src/engine/mv/iceberg_refresh.rs)

`refresh_iceberg_aggregate_mv` (the dispatcher) does NOT construct ctx — the ctx is constructed in each of its two sub-helpers (`refresh_single_aggregate_iceberg_mv`, `refresh_join_aggregate_iceberg_mv`) because each does its own pin + schema-rebind. After rebind in each sub-helper, construct ctx.

- [ ] **Step 1: Construct ctx in `refresh_single_aggregate_iceberg_mv`**

Locate the line after `let aggregate_shape = &reclassified_aggregate_shape;` ([iceberg_refresh.rs:1675](src/engine/mv/iceberg_refresh.rs:1675)). Insert immediately after:

```rust
let canonical_select_query = canonicalize_iceberg_mv_select_query(
    &parse_mv_select_query(&mv_definition.select_sql)?,
    current_catalog,
    current_database,
);
let ctx = crate::engine::mv::refresh_context::IcebergMvRefreshContext::new(
    target.clone(),
    mv_definition.mv_id,
    current_catalog,
    current_database,
    Arc::new(mv_definition.clone()),
    Arc::new(canonical_select_query),
    Arc::from(base_refs.to_vec()),
    Arc::new(pin.clone()),
    Arc::new(target_entry.clone()),
    iceberg_catalog.clone(),
    target_table.clone(),
)?;
tracing::info!(
    summary = ?ctx.rewrite.summary(),
    "iceberg MV refresh context constructed"
);
```

`target_entry` is `&IcebergCatalogEntry`; `target_entry.clone()` requires `IcebergCatalogEntry: Clone`. Verify at [src/connector/iceberg/catalog/registry.rs:60](src/connector/iceberg/catalog/registry.rs:60). If it's not `Clone`, change the caller to pass `Arc<IcebergCatalogEntry>` instead — bubble that up by changing `refresh_iceberg_aggregate_mv` to take `target_entry: &Arc<IcebergCatalogEntry>`.

Same applies to the other sites — the cleanest is to make `target_entry` flow as `Arc<IcebergCatalogEntry>` everywhere ctx is in scope.

- [ ] **Step 2: Migrate downstream sub-helpers in single-aggregate path**

In the `match previous` block following ctx construction, two arms call `first_refresh_iceberg_aggregate_mv` and `incremental_refresh_iceberg_aggregate_mv`. Locate them via:

```bash
grep -n "^fn first_refresh_iceberg_aggregate_mv\|^fn incremental_refresh_iceberg_aggregate_mv" src/engine/mv/iceberg_refresh.rs
```

Migrate both signatures to `(state, ctx, ...shape-specific args)` per the Task 5 pattern. Drop `target / target_entry / iceberg_catalog / target_table / expected_main_snapshot_id / current_catalog / current_database / mv_definition / base_refs / schema_contract` arguments; they now come from `&ctx`.

Adjust the call sites in the match arms.

- [ ] **Step 3: Construct ctx in `refresh_join_aggregate_iceberg_mv` and migrate its downstream helpers**

`refresh_join_aggregate_iceberg_mv` is the other arm of `refresh_iceberg_aggregate_mv`. Same pattern: after pin capture + schema rebind, construct ctx and migrate its sub-helpers (`first_refresh_iceberg_join_aggregate_mv` / `incremental_refresh_iceberg_join_aggregate_mv` — confirm exact names via `grep`).

- [ ] **Step 4: Build**

Run: `cargo build --lib`
Expected: clean.

- [ ] **Step 5: Run unit tests**

Run: `cargo test -p novarocks --lib`
Expected: all pass.

- [ ] **Step 6: Run the `iceberg-ivm` SQL suite**

```bash
cargo run --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests -- \
  --config "$NOVAROCKS_SQL_TEST_CONFIG" --suite iceberg-ivm --mode verify
```

Expected: all cases pass.

- [ ] **Step 7: Commit**

```bash
git add src/engine/mv/iceberg_refresh.rs
git commit -m "engine/mv: thread refresh context through Aggregate / JoinAggregate shapes"
```

---

### Task 7: Wire ctx into the JoinProjectionFilter shape path

**Files:**
- Modify: [`src/engine/mv/iceberg_refresh.rs`](src/engine/mv/iceberg_refresh.rs)

`refresh_iceberg_join_mv` ([iceberg_refresh.rs:5309](src/engine/mv/iceberg_refresh.rs:5309)) is the third dispatcher. Note: it does NOT have a rebind path (it uses `validate_join_schema_contract` instead). Construct ctx after `validate_join_schema_contract` succeeds, before the snapshot-state match.

- [ ] **Step 1: Locate the construction site**

Inside `refresh_iceberg_join_mv`, after the `validate_join_schema_contract(...)?` call ([iceberg_refresh.rs:5356](src/engine/mv/iceberg_refresh.rs:5356)) but before the `match (left_previous, right_previous, ...)` block, capture the pin. The join path does NOT capture pin upfront the way aggregate / projection paths do — it captures pin inside specific arms where it actually needs one. Re-read the function body to confirm.

If the function captures pin only inside the live arms, decide:
- (a) Hoist pin capture above the match (parallel to other shapes); construct ctx once before the match. This is the cleanest design but slightly changes the semantics of when pin is captured for the no-op arms.
- (b) Construct ctx INSIDE each non-no-op arm that captures pin. More duplication but no semantic shift.

Choose (a) ONLY if it's safe: re-inspect each no-op arm to confirm it doesn't depend on "pin not yet captured" as a side-effect (e.g., does any code path conditionally avoid the pin's SQLite write?). If unclear, prefer (b).

Document the choice in the commit message.

- [ ] **Step 2: Implement ctx construction**

Same pattern as Task 6 Step 1. Build `canonical_select_query` via `canonicalize_iceberg_mv_select_query(&parse_mv_select_query(&mv_definition.select_sql)?, ...)`. Construct ctx, log the summary.

- [ ] **Step 3: Migrate downstream join sub-helpers**

Identify the immediate sub-helpers via:

```bash
grep -n "^fn .*join.*refresh\|^fn .*refresh.*join" src/engine/mv/iceberg_refresh.rs | grep -v "fn refresh_iceberg_join_mv\b\|fn refresh_iceberg_join_aggregate"
```

Migrate the immediate ones (those called directly from the match arms in `refresh_iceberg_join_mv`) to `(state, ctx, ...shape-specific args)`.

- [ ] **Step 4: Build**

Run: `cargo build --lib`
Expected: clean.

- [ ] **Step 5: Run unit + SQL suites**

```bash
cargo test -p novarocks --lib
cargo run --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests -- \
  --config "$NOVAROCKS_SQL_TEST_CONFIG" --suite iceberg-ivm --mode verify
```

Expected: all pass.

- [ ] **Step 6: Commit**

```bash
git add src/engine/mv/iceberg_refresh.rs
git commit -m "engine/mv: thread refresh context through JoinProjectionFilter shape"
```

---

### Task 8: Final verification across the wider SQL suites

**Files:** none.

The acceptance criteria require `iceberg-rest` and `iceberg-compatibility` to also pass unchanged, plus a tracing summary emitted exactly once per refresh attempt.

- [ ] **Step 1: Verify the `iceberg-rest` suite**

Make sure the docker fixture is up:

```bash
source docker/iceberg-rest/runtime/current/env.sh
docker/iceberg-rest/up.sh
```

Run:

```bash
cargo run --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests -- \
  --config "$NOVAROCKS_SQL_TEST_CONFIG" --suite iceberg-rest --mode verify
```

Expected: all cases pass.

- [ ] **Step 2: Verify the `iceberg-compatibility` suite**

```bash
cargo run --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests -- \
  --config "$NOVAROCKS_SQL_TEST_CONFIG" --suite iceberg-compatibility --mode verify
```

Expected: all cases pass.

- [ ] **Step 3: Manually confirm the tracing summary fires once per refresh**

In one terminal, run the standalone-server with `RUST_LOG=info` so tracing output is captured:

```bash
RUST_LOG=info NO_PROXY=127.0.0.1,localhost target/debug/novarocks standalone-server \
  --config "$NOVAROCKS_STANDALONE_CONFIG" 2>&1 | tee /tmp/srv.log &
```

In another terminal, connect via mysql and trigger a refresh:

```bash
mysql --host=127.0.0.1 --port="$NOVA_ENV_MYSQL_PORT" --user=root <<'SQL'
USE default;
-- (use an existing iceberg-ivm fixture MV or create one)
REFRESH MATERIALIZED VIEW some_mv;
SQL
```

Then:

```bash
grep -c "iceberg MV refresh context constructed" /tmp/srv.log
```

Expected: exactly `1` for that single refresh. If `0`, the construction site for the shape this MV uses was skipped — re-check Task 5 / 6 / 7. If `2+`, the same construction site fires twice — re-check that you didn't accidentally leave the construction in two places (e.g., once inline before dispatch and once inside the shape helper).

- [ ] **Step 4: Run `cargo fmt --check` and `cargo clippy --lib -- -D warnings`**

```bash
cargo fmt --check
cargo clippy --lib -- -D warnings
```

Fix any warnings before final commit. New methods with `#[allow(clippy::too_many_arguments)]` already account for the wide constructors — anything else should be addressed.

- [ ] **Step 5: Final commit (only if `cargo fmt` / `clippy` produced any changes)**

```bash
git add -u
git commit -m "engine/mv: fmt + clippy cleanups after refresh-context migration"
```

If there's nothing to commit (clean run), skip.

---

## Post-Plan Sanity

The implementation is complete when all of the following hold:

- [ ] All unit tests in `src/engine/mv/refresh_context.rs` pass (≥9 cases covering happy path + 6 negative + first refresh + summary ordering).
- [ ] `iceberg-ivm`, `iceberg-rest`, `iceberg-compatibility` SQL suites all pass `--mode verify`.
- [ ] `cargo fmt --check` and `cargo clippy --lib -- -D warnings` are clean.
- [ ] Each refresh attempt emits exactly one `iceberg MV refresh context constructed` tracing line.
- [ ] `refresh_single_aggregate_iceberg_mv`, `refresh_join_aggregate_iceberg_mv`, `first_refresh_iceberg_mv`, `incremental_refresh_iceberg_mv`, and the join-shape equivalents take `(state, ctx, ...shape-specific args)` and no longer carry the collapsed individual params.
- [ ] No deeper helper signatures changed (per spec §6 "incidental call sites that take only one or two can keep their existing form").

When all boxes are checked, the work is ready for review.
