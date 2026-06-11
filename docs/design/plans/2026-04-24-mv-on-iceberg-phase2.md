# MV on Iceberg Phase 2 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add append-only incremental refresh for standalone materialized views over a single Iceberg base table with projection/filter SELECTs.

**Architecture:** Phase 2 keeps Phase 1's MV storage model and adds strategy selection inside `mv_refresh.rs`. First refresh still uses the full partition-swap path; later refreshes either no-op or plan Iceberg append deltas, execute the stored SELECT over only delta files, and append the result chunks into the MV's active managed-lake partition with MV metadata updated atomically.

**Tech Stack:** Rust, sqlparser AST, Iceberg Rust crate, rusqlite metadata store, Arrow `RecordBatch` / `Chunk`, existing managed-lake txn/publish path, `cargo test`, standalone MySQL integration tests, SQL-tests runner.

---

## File Structure

**Create**
- `src/standalone/lake/mv_shape.rs` - classifier for Phase 2 incrementally refreshable MV SELECT shape.

**Modify**
- `src/standalone/lake/mod.rs` - register `mv_shape`.
- `src/standalone/lake/mv_ddl.rs` - reject unsupported MV SELECT shapes at CREATE time and keep existing base-table validation.
- `src/standalone/lake/mv_refresh.rs` - choose full/no-op/incremental strategy and execute incremental refresh.
- `src/standalone/lake/store.rs` - add MV refresh metadata update helpers and atomic visible+metadata update.
- `src/standalone/lake/txn.rs` - expose an active-partition MV incremental write helper that reuses existing routing/publish code.
- `src/standalone/iceberg/registry.rs` - add append-delta planning over Iceberg snapshot lineage and manifests.
- `src/standalone/iceberg/mod.rs` - re-export append-delta types and helper.
- `src/standalone/engine/mod.rs` - add delta-file query execution helper and shared Iceberg table registration path.
- `tests/standalone_mysql_server.rs` - add Phase 2 integration tests.
- `sql-tests/write-path/sql/managed_lake_mv_incremental.sql` - add SQL regression.
- `sql-tests/write-path/result/managed_lake_mv_incremental.result` - expected SQL regression output.

---

## Task 1: Add MV Incremental Shape Classifier

**Files:**
- Create: `src/standalone/lake/mv_shape.rs`
- Modify: `src/standalone/lake/mod.rs`
- Later users: `src/standalone/lake/mv_ddl.rs`, `src/standalone/lake/mv_refresh.rs`

- [ ] **Step 1: Write classifier tests**

Create `src/standalone/lake/mv_shape.rs` with the test module first:

```rust
use sqlparser::ast::{Expr, Function, FunctionArg, FunctionArgExpr, FunctionArguments, GroupByExpr, ObjectName, Query, SelectItem, SetExpr, TableFactor};

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct IncrementalMvShape {
    pub(crate) base_table: ObjectName,
}

pub(crate) fn classify_incremental_mv_query(query: &Query) -> Result<IncrementalMvShape, String> {
    let _ = query;
    Err("incremental materialized view refresh supports only projection/filter SELECTs".to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parse_query(sql: &str) -> Query {
        let normalized = crate::sql::parser::dialect::normalize_for_raw_parse(sql)
            .expect("normalize");
        let stmt = crate::sql::parser::parse_normalized_sql_raw(&normalized).expect("parse");
        let sqlparser::ast::Statement::Query(query) = stmt else {
            panic!("not a query: {stmt:?}");
        };
        *query
    }

    #[test]
    fn classify_accepts_single_table_projection_filter() {
        let query = parse_query("select k1, v2 + 1 as v3 from ice.ns.orders where v2 > 10");
        let shape = classify_incremental_mv_query(&query).expect("shape");
        assert_eq!(shape.base_table.to_string(), "ice.ns.orders");
    }

    #[test]
    fn classify_rejects_multi_table_join() {
        let query = parse_query("select o.k1 from ice.ns.orders o join ice.ns.items i on o.k1 = i.k1");
        let err = classify_incremental_mv_query(&query).expect_err("join rejected");
        assert!(err.contains("single Iceberg base table"), "err={err}");
    }

    #[test]
    fn classify_rejects_aggregation() {
        let query = parse_query("select k1, sum(v2) from ice.ns.orders group by k1");
        let err = classify_incremental_mv_query(&query).expect_err("agg rejected");
        assert!(err.contains("projection/filter"), "err={err}");
    }

    #[test]
    fn classify_rejects_distinct_window_limit_and_subquery() {
        for sql in [
            "select distinct k1 from ice.ns.orders",
            "select row_number() over(partition by k1) from ice.ns.orders",
            "select k1 from ice.ns.orders limit 10",
            "select k1 from (select k1 from ice.ns.orders) t",
        ] {
            let query = parse_query(sql);
            let err = classify_incremental_mv_query(&query).expect_err(sql);
            assert!(err.contains("projection/filter"), "sql={sql}, err={err}");
        }
    }

    #[test]
    fn classify_rejects_non_deterministic_functions() {
        let query = parse_query("select now() from ice.ns.orders");
        let err = classify_incremental_mv_query(&query).expect_err("now rejected");
        assert!(err.contains("non-deterministic"), "err={err}");
    }
}
```

- [ ] **Step 2: Run tests to verify failure**

```bash
cargo test --lib standalone::lake::mv_shape -- --nocapture
```

Expected: FAIL because `mv_shape` is not registered in `src/standalone/lake/mod.rs`, or because the stub always rejects.

- [ ] **Step 3: Register the module**

In `src/standalone/lake/mod.rs`, add:

```rust
pub(crate) mod mv_shape;
```

- [ ] **Step 4: Implement the classifier**

Replace the stub in `src/standalone/lake/mv_shape.rs` with:

```rust
pub(crate) fn classify_incremental_mv_query(query: &Query) -> Result<IncrementalMvShape, String> {
    const ERR: &str = "incremental materialized view refresh supports only projection/filter SELECTs";

    if query.with.is_some()
        || query.order_by.is_some()
        || query.limit_clause.is_some()
        || query.fetch.is_some()
        || !query.locks.is_empty()
        || query.for_clause.is_some()
        || query.format_clause.is_some()
        || !query.pipe_operators.is_empty()
    {
        return Err(ERR.to_string());
    }

    let SetExpr::Select(select) = query.body.as_ref() else {
        return Err(ERR.to_string());
    };
    if select.distinct.is_some()
        || select.top.is_some()
        || select.into.is_some()
        || !select.lateral_views.is_empty()
        || select.prewhere.is_some()
        || !select.connect_by.is_empty()
        || !matches!(&select.group_by, GroupByExpr::Expressions(exprs, modifiers) if exprs.is_empty() && modifiers.is_empty())
        || !select.cluster_by.is_empty()
        || !select.distribute_by.is_empty()
        || !select.sort_by.is_empty()
        || select.having.is_some()
        || !select.named_window.is_empty()
        || select.qualify.is_some()
        || select.value_table_mode.is_some()
    {
        return Err(ERR.to_string());
    }
    if select.from.len() != 1 || !select.from[0].joins.is_empty() {
        return Err("incremental materialized view refresh requires a single Iceberg base table".to_string());
    }
    let TableFactor::Table { name, .. } = &select.from[0].relation else {
        return Err(ERR.to_string());
    };
    if name.0.len() != 3 {
        return Err("incremental materialized view refresh requires a single Iceberg base table".to_string());
    }

    for item in &select.projection {
        match item {
            SelectItem::UnnamedExpr(expr) => validate_incremental_expr(expr)?,
            SelectItem::ExprWithAlias { expr, .. } => validate_incremental_expr(expr)?,
            SelectItem::Wildcard(_) | SelectItem::QualifiedWildcard(_, _) => {}
        }
    }
    if let Some(selection) = &select.selection {
        validate_incremental_expr(selection)?;
    }

    Ok(IncrementalMvShape {
        base_table: name.clone(),
    })
}

fn validate_incremental_expr(expr: &Expr) -> Result<(), String> {
    let rendered = expr.to_string().to_ascii_lowercase();
    for banned in ["now(", "current_timestamp", "random(", "rand(", "uuid("] {
        if rendered.contains(banned) {
            return Err("incremental materialized view refresh rejects non-deterministic functions".to_string());
        }
    }
    if rendered.contains(" over ") || rendered.contains(" over(") {
        return Err("incremental materialized view refresh supports only projection/filter SELECTs".to_string());
    }
    if contains_aggregate_name(&rendered) {
        return Err("incremental materialized view refresh supports only projection/filter SELECTs".to_string());
    }
    Ok(())
}

fn contains_aggregate_name(rendered: &str) -> bool {
    ["sum(", "count(", "avg(", "min(", "max(", "array_agg(", "bitmap_", "hll_"]
        .iter()
        .any(|needle| rendered.contains(needle))
}
```

Keep the imports exactly as needed after `cargo fmt`; remove unused imports.

- [ ] **Step 5: Run tests to verify pass**

```bash
cargo test --lib standalone::lake::mv_shape -- --nocapture
```

Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add src/standalone/lake/mod.rs src/standalone/lake/mv_shape.rs
git commit -m "feat: classify incremental mv select shape"
```

---

## Task 2: Enforce Phase 2 Shape at MV CREATE

**Files:**
- Modify: `src/standalone/lake/mv_ddl.rs`
- Test: `src/standalone/lake/mv_ddl.rs`

- [ ] **Step 1: Add failing tests**

Append to the existing `#[cfg(test)] mod tests` in `src/standalone/lake/mv_ddl.rs`:

```rust
fn parse_create_mv(sql: &str) -> crate::sql::parser::ast::CreateMaterializedViewStmt {
    let stmt = crate::sql::parser::parse_sql(sql).expect("parse").remove(0);
    let crate::sql::parser::ast::Statement::CreateMaterializedView(stmt) = stmt else {
        panic!("not create mv");
    };
    stmt
}

#[test]
fn create_mv_shape_accepts_projection_filter() {
    let stmt = parse_create_mv(
        "create materialized view mv1 distributed by hash(k1) buckets 2 \
         as select k1, v2 from ice.ns.orders where v2 > 10",
    );
    super::validate_incremental_create_shape(&stmt).expect("shape ok");
}

#[test]
fn create_mv_shape_rejects_aggregation() {
    let stmt = parse_create_mv(
        "create materialized view mv1 distributed by hash(k1) buckets 2 \
         as select k1, sum(v2) from ice.ns.orders group by k1",
    );
    let err = super::validate_incremental_create_shape(&stmt).expect_err("agg rejected");
    assert!(err.contains("projection/filter"), "err={err}");
}
```

- [ ] **Step 2: Run tests to verify failure**

```bash
cargo test --lib standalone::lake::mv_ddl::tests::create_mv_shape_ -- --nocapture
```

Expected: FAIL because `validate_incremental_create_shape` does not exist.

- [ ] **Step 3: Add validation helper and call it**

In `src/standalone/lake/mv_ddl.rs`, add near `create_mv`:

```rust
fn validate_incremental_create_shape(stmt: &CreateMaterializedViewStmt) -> Result<(), String> {
    crate::standalone::lake::mv_shape::classify_incremental_mv_query(&stmt.select_query)?;
    Ok(())
}
```

Then in `create_mv`, immediately after resolving `db_name` / `mv_name`, before catalog and object-store work:

```rust
validate_incremental_create_shape(stmt)?;
```

- [ ] **Step 4: Run targeted tests**

```bash
cargo test --lib standalone::lake::mv_ddl::tests::create_mv_shape_ -- --nocapture
cargo test --lib standalone::lake::mv_ddl -- --nocapture
```

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add src/standalone/lake/mv_ddl.rs
git commit -m "feat: reject non-incremental mv definitions"
```

---

## Task 3: Add Atomic MV Refresh Metadata Store Helpers

**Files:**
- Modify: `src/standalone/lake/store.rs`

- [ ] **Step 1: Add failing tests**

In `src/standalone/lake/store.rs`, add tests near `mark_txn_written_and_visible_advances_partition_version`:

```rust
#[test]
fn update_mv_refresh_metadata_only_updates_last_refresh_fields() {
    let (_dir, store) = bootstrapped_store_for_txn();
    let mut snapshot = store.load_snapshot().expect("load").managed;
    snapshot.tables[0].kind = ManagedTableKind::MaterializedView;
    snapshot.materialized_views.push(StoredMaterializedView {
        mv_id: 10,
        select_sql: "select k1 from ice.ns.orders".to_string(),
        refresh_mode: ManagedMvRefreshMode::DeferredManual,
        base_table_refs: vec![IcebergTableRef {
            catalog: "ice".to_string(),
            namespace: "ns".to_string(),
            table: "orders".to_string(),
        }],
        last_refresh_ms: None,
        last_refresh_rows: Some(3),
        last_refresh_snapshots: BTreeMap::new(),
        created_at_ms: 1,
    });
    store.replace_managed_snapshot(&snapshot).expect("persist");

    let mut snapshots = BTreeMap::new();
    snapshots.insert("ice.ns.orders".to_string(), 88);
    store
        .update_mv_refresh_metadata(UpdateMvRefreshMetadataRequest {
            table_id: 10,
            last_refresh_rows: 3,
            snapshots: snapshots.clone(),
        })
        .expect("update metadata");

    let loaded = store.load_snapshot().expect("reload").managed;
    let mv = loaded.materialized_views.iter().find(|mv| mv.mv_id == 10).expect("mv");
    assert_eq!(mv.last_refresh_rows, Some(3));
    assert_eq!(mv.last_refresh_snapshots, snapshots);
    assert!(mv.last_refresh_ms.is_some());
}

#[test]
fn mark_txn_visible_with_mv_refresh_metadata_is_atomic() {
    let (_dir, store) = bootstrapped_store_for_txn();
    let mut snapshot = store.load_snapshot().expect("load").managed;
    snapshot.tables[0].kind = ManagedTableKind::MaterializedView;
    snapshot.materialized_views.push(StoredMaterializedView {
        mv_id: 10,
        select_sql: "select k1 from ice.ns.orders".to_string(),
        refresh_mode: ManagedMvRefreshMode::DeferredManual,
        base_table_refs: vec![],
        last_refresh_ms: Some(1),
        last_refresh_rows: Some(2),
        last_refresh_snapshots: BTreeMap::new(),
        created_at_ms: 1,
    });
    store.replace_managed_snapshot(&snapshot).expect("persist");
    let prepared = store.prepare_txn(10, 20, 1).expect("prepare");
    store.mark_txn_written(prepared.txn_id).expect("written");

    let mut snapshots = BTreeMap::new();
    snapshots.insert("ice.ns.orders".to_string(), 99);
    store
        .mark_txn_visible_with_mv_refresh_metadata(
            prepared.txn_id,
            prepared.commit_version,
            UpdateMvRefreshMetadataRequest {
                table_id: 10,
                last_refresh_rows: 4,
                snapshots: snapshots.clone(),
            },
        )
        .expect("visible with metadata");

    let loaded = store.load_snapshot().expect("reload").managed;
    let partition = loaded.partitions.iter().find(|p| p.partition_id == 20).expect("partition");
    assert_eq!(partition.visible_version, 2);
    let mv = loaded.materialized_views.iter().find(|mv| mv.mv_id == 10).expect("mv");
    assert_eq!(mv.last_refresh_rows, Some(4));
    assert_eq!(mv.last_refresh_snapshots, snapshots);
}
```

- [ ] **Step 2: Run tests to verify failure**

```bash
cargo test --lib standalone::lake::store::tests::update_mv_refresh_metadata_only_updates_last_refresh_fields -- --nocapture
cargo test --lib standalone::lake::store::tests::mark_txn_visible_with_mv_refresh_metadata_is_atomic -- --nocapture
```

Expected: FAIL because `UpdateMvRefreshMetadataRequest`, `update_mv_refresh_metadata`, and `mark_txn_visible_with_mv_refresh_metadata` do not exist.

- [ ] **Step 3: Add request type**

Near `ActivateMvRefreshRequest` in `src/standalone/lake/store.rs`, add:

```rust
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct UpdateMvRefreshMetadataRequest {
    pub table_id: i64,
    pub last_refresh_rows: i64,
    pub snapshots: std::collections::BTreeMap<String, i64>,
}
```

Use field name `table_id` instead of `mv_id` in the tests and call sites so it matches existing request naming.

- [ ] **Step 4: Implement helpers**

Inside `impl SqliteMetadataStore`, add:

```rust
pub(crate) fn update_mv_refresh_metadata(
    &self,
    req: UpdateMvRefreshMetadataRequest,
) -> Result<(), String> {
    let conn = self.connection()?;
    let tx = conn
        .unchecked_transaction()
        .map_err(|e| format!("begin update_mv_refresh_metadata transaction failed: {e}"))?;
    update_mv_refresh_metadata_in_tx(&tx, &req)?;
    tx.commit()
        .map_err(|e| format!("commit update_mv_refresh_metadata failed: {e}"))?;
    Ok(())
}

pub(crate) fn mark_txn_visible_with_mv_refresh_metadata(
    &self,
    txn_id: i64,
    commit_version: i64,
    req: UpdateMvRefreshMetadataRequest,
) -> Result<(), String> {
    let conn = self.connection()?;
    let tx = conn
        .unchecked_transaction()
        .map_err(|e| format!("begin mark_txn_visible_with_mv_refresh_metadata transaction failed: {e}"))?;
    let partition_id: i64 = tx
        .query_row(
            "SELECT partition_id FROM txns WHERE txn_id = ?1",
            params![txn_id],
            |row| row.get(0),
        )
        .map_err(|e| format!("load partition for txn {txn_id} failed: {e}"))?;
    tx.execute(
        "UPDATE txns SET state = 'VISIBLE', updated_at_ms = strftime('%s','now') * 1000
         WHERE txn_id = ?1",
        params![txn_id],
    )
    .map_err(|e| format!("mark txn visible failed: {e}"))?;
    tx.execute(
        "UPDATE partitions SET visible_version = ?1, next_version = ?2
         WHERE partition_id = ?3",
        params![commit_version, commit_version + 1, partition_id],
    )
    .map_err(|e| format!("advance partition version failed: {e}"))?;
    update_mv_refresh_metadata_in_tx(&tx, &req)?;
    tx.commit()
        .map_err(|e| format!("commit mark_txn_visible_with_mv_refresh_metadata failed: {e}"))?;
    Ok(())
}
```

Add the private helper below the impl or as an associated private function:

```rust
fn update_mv_refresh_metadata_in_tx(
    tx: &rusqlite::Transaction<'_>,
    req: &UpdateMvRefreshMetadataRequest,
) -> Result<(), String> {
    let snapshots_json = if req.snapshots.is_empty() {
        None
    } else {
        Some(
            serde_json::to_string(&req.snapshots)
                .map_err(|e| format!("serialize mv refresh snapshots failed: {e}"))?,
        )
    };
    let changed = tx
        .execute(
            "UPDATE materialized_views
             SET last_refresh_ms = strftime('%s','now') * 1000,
                 last_refresh_rows = ?1,
                 last_refresh_snapshots_json = ?2
             WHERE mv_id = ?3",
            params![req.last_refresh_rows, snapshots_json, req.table_id],
        )
        .map_err(|e| format!("update materialized_view refresh metadata failed: {e}"))?;
    if changed != 1 {
        return Err(format!(
            "materialized view {} metadata row not found",
            req.table_id
        ));
    }
    Ok(())
}
```

- [ ] **Step 5: Run tests**

```bash
cargo test --lib standalone::lake::store::tests::update_mv_refresh_metadata_only_updates_last_refresh_fields -- --nocapture
cargo test --lib standalone::lake::store::tests::mark_txn_visible_with_mv_refresh_metadata_is_atomic -- --nocapture
cargo test --lib standalone::lake::store::tests -- --nocapture
```

Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add src/standalone/lake/store.rs
git commit -m "feat: update mv refresh metadata atomically"
```

---

## Task 4: Add MV Incremental Active-Partition Write Helper

**Files:**
- Modify: `src/standalone/lake/txn.rs`
- Uses: `UpdateMvRefreshMetadataRequest` from `store.rs`

- [ ] **Step 1: Add failing test**

In `src/standalone/lake/txn.rs`, extend `mv_target_tests` with:

```rust
#[test]
fn write_chunks_into_managed_partition_for_mv_refresh_updates_metadata_atomically() {
    let state = seed_state_with_active_mv();
    let plan = load_insert_plan(
        &state,
        &ResolvedLocalTableName {
            database: "analytics".to_string(),
            table: "orders_mv".to_string(),
        },
        PartitionTarget::Active,
    )
    .expect("plan");
    let chunk = single_i32_chunk("k1", &[1, 2, 3]);
    let mut snapshots = std::collections::BTreeMap::new();
    snapshots.insert("ice.ns.orders".to_string(), 42);

    let rows = write_chunks_into_managed_partition_for_mv_refresh(
        &state,
        plan,
        &[chunk],
        super::store::UpdateMvRefreshMetadataRequest {
            table_id: 10,
            last_refresh_rows: 3,
            snapshots: snapshots.clone(),
        },
    )
    .expect("write");
    assert_eq!(rows, 3);

    let store = state.metadata_store.as_ref().expect("store");
    let loaded = store.load_snapshot().expect("snapshot").managed;
    let mv = loaded.materialized_views.iter().find(|mv| mv.mv_id == 10).expect("mv");
    assert_eq!(mv.last_refresh_rows, Some(3));
    assert_eq!(mv.last_refresh_snapshots, snapshots);
}
```

Add helper `seed_state_with_active_mv()` by cloning the existing staged-MV test fixture and using `ManagedPartitionState::Active`, `ManagedIndexState::Active`, `ManagedTableKind::MaterializedView`, plus a `StoredMaterializedView` row with `last_refresh_rows: Some(0)`.

- [ ] **Step 2: Run test to verify failure**

```bash
cargo test --lib standalone::lake::txn::mv_target_tests::write_chunks_into_managed_partition_for_mv_refresh_updates_metadata_atomically -- --nocapture
```

Expected: FAIL because `write_chunks_into_managed_partition_for_mv_refresh` does not exist.

- [ ] **Step 3: Refactor write helper**

In `src/standalone/lake/txn.rs`, add a private enum:

```rust
enum VisibleCommitAction {
    Plain,
    MvRefresh(super::store::UpdateMvRefreshMetadataRequest),
}
```

Change `write_chunks_into_managed_partition` to call a new private function:

```rust
pub(crate) fn write_chunks_into_managed_partition(
    state: &Arc<StandaloneState>,
    plan: ManagedInsertPlan,
    chunks: &[Chunk],
) -> Result<i64, String> {
    write_chunks_into_managed_partition_inner(state, plan, chunks, VisibleCommitAction::Plain)
}

pub(crate) fn write_chunks_into_managed_partition_for_mv_refresh(
    state: &Arc<StandaloneState>,
    plan: ManagedInsertPlan,
    chunks: &[Chunk],
    metadata: super::store::UpdateMvRefreshMetadataRequest,
) -> Result<i64, String> {
    write_chunks_into_managed_partition_inner(state, plan, chunks, VisibleCommitAction::MvRefresh(metadata))
}
```

Move the existing body into `write_chunks_into_managed_partition_inner`. Replace the final visible commit with:

```rust
match commit_action {
    VisibleCommitAction::Plain => {
        metadata_store.mark_txn_visible(prepared.txn_id, prepared.commit_version)?;
    }
    VisibleCommitAction::MvRefresh(metadata) => {
        metadata_store.mark_txn_visible_with_mv_refresh_metadata(
            prepared.txn_id,
            prepared.commit_version,
            metadata,
        )?;
    }
}
commit_catalog_visible_version(state, &plan, prepared.commit_version)?;
```

- [ ] **Step 4: Run tests**

```bash
cargo test --lib standalone::lake::txn::mv_target_tests -- --nocapture
cargo test --lib standalone::lake::store::tests::mark_txn_visible_with_mv_refresh_metadata_is_atomic -- --nocapture
```

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add src/standalone/lake/txn.rs
git commit -m "feat: write incremental mv chunks into active partition"
```

---

## Task 5: Add Iceberg Append-Delta Planner

**Files:**
- Modify: `src/standalone/iceberg/registry.rs`
- Modify: `src/standalone/iceberg/mod.rs`

- [ ] **Step 1: Add failing integration-style unit test**

Add a `#[cfg(test)] mod phase2_delta_tests` module at the bottom of `src/standalone/iceberg/registry.rs`. Because the tests live in the same module, they can call the private `build_catalog_entry` helper:

```rust
#[test]
fn plan_append_delta_collects_files_after_previous_snapshot() {
    let dir = tempfile::tempdir().expect("tempdir");
    let warehouse = format!("file://{}", dir.path().join("warehouse").display());
    let entry = test_hadoop_catalog_entry("ice", &warehouse);
    create_namespace(&entry, "ns").expect("namespace");
    create_table(
        &entry,
        "ns",
        "orders",
        vec![
            crate::standalone::engine::catalog::ColumnDef {
                name: "k1".to_string(),
                data_type: arrow::datatypes::DataType::Int32,
                nullable: true,
            },
        ],
    )
    .expect("table");
    insert_rows(&entry, "ns", "orders", vec![vec![crate::sql::parser::ast::Literal::Number("1".to_string())]])
        .expect("first insert");
    let loaded = load_table(&entry, "ns", "orders").expect("load first");
    let previous = loaded.table.metadata().current_snapshot().expect("snapshot").snapshot_id();

    insert_rows(&entry, "ns", "orders", vec![vec![crate::sql::parser::ast::Literal::Number("2".to_string())]])
        .expect("second insert");
    let loaded = load_table(&entry, "ns", "orders").expect("load second");
    let delta = plan_append_delta(&loaded.table, previous).expect("delta");
    assert_eq!(delta.previous_snapshot_id, previous);
    assert_eq!(delta.current_snapshot_id, loaded.table.metadata().current_snapshot().unwrap().snapshot_id());
    assert!(!delta.added_files.is_empty());
    assert!(delta.added_files.iter().all(|(_, _, rows)| rows.unwrap_or_default() > 0));
}
```

Add this test-only helper inside `phase2_delta_tests`:

```rust
fn test_hadoop_catalog_entry(catalog_name: &str, warehouse_uri: &str) -> IcebergCatalogEntry {
    build_catalog_entry(
        catalog_name,
        &[
            ("type".to_string(), "iceberg".to_string()),
            ("iceberg.catalog.type".to_string(), "hadoop".to_string()),
            (
                "iceberg.catalog.warehouse".to_string(),
                warehouse_uri.to_string(),
            ),
        ],
    )
    .expect("catalog entry")
}
```

- [ ] **Step 2: Run test to verify failure**

```bash
cargo test --lib standalone::iceberg::registry::phase2_delta_tests::plan_append_delta_collects_files_after_previous_snapshot -- --nocapture
```

Expected: FAIL because `plan_append_delta` does not exist.

- [ ] **Step 3: Add append-delta types**

In `src/standalone/iceberg/registry.rs`, add:

```rust
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct IcebergAppendDelta {
    pub previous_snapshot_id: i64,
    pub current_snapshot_id: i64,
    pub added_files: Vec<(String, i64, Option<i64>)>,
}
```

- [ ] **Step 4: Implement `plan_append_delta`**

Add below `extract_data_files`:

```rust
pub(crate) fn plan_append_delta(
    table: &iceberg::table::Table,
    previous_snapshot_id: i64,
) -> Result<IcebergAppendDelta, String> {
    use iceberg::spec::{DataContentType, ManifestContentType, ManifestStatus, Operation};

    let metadata = table.metadata();
    let current = metadata
        .current_snapshot()
        .ok_or_else(|| "cannot plan incremental refresh: Iceberg table has no current snapshot".to_string())?;
    let current_snapshot_id = current.snapshot_id();
    if current_snapshot_id == previous_snapshot_id {
        return Ok(IcebergAppendDelta {
            previous_snapshot_id,
            current_snapshot_id,
            added_files: Vec::new(),
        });
    }

    let mut chain = Vec::new();
    let mut cursor = Some(current.clone());
    while let Some(snapshot) = cursor {
        if snapshot.snapshot_id() == previous_snapshot_id {
            break;
        }
        if snapshot.summary().operation != Operation::Append {
            return Err(format!(
                "incremental materialized view refresh supports only append snapshots; snapshot {} operation is {:?}",
                snapshot.snapshot_id(),
                snapshot.summary().operation
            ));
        }
        let parent = snapshot.parent_snapshot_id();
        chain.push(snapshot.clone());
        cursor = parent.and_then(|id| metadata.snapshot_by_id(id).cloned());
    }
    if !chain
        .last()
        .and_then(|snapshot| snapshot.parent_snapshot_id())
        .is_some_and(|id| id == previous_snapshot_id)
        && !metadata
            .snapshot_by_id(previous_snapshot_id)
            .is_some_and(|snapshot| chain.iter().any(|child| child.parent_snapshot_id() == Some(snapshot.snapshot_id())))
    {
        return Err(format!(
            "cannot plan incremental refresh: snapshot {previous_snapshot_id} is not an ancestor of {current_snapshot_id}"
        ));
    }

    let file_io = table.file_io();
    let added_files = block_on_iceberg(async {
        let mut files = Vec::new();
        for snapshot in chain.iter().rev() {
            let manifest_list = snapshot
                .load_manifest_list(file_io, metadata)
                .await
                .map_err(|e| format!("load manifest list for snapshot {}: {e}", snapshot.snapshot_id()))?;
            for manifest_file in manifest_list.entries() {
                if manifest_file.content != ManifestContentType::Data {
                    return Err(format!(
                        "incremental materialized view refresh does not support delete manifests in snapshot {}",
                        snapshot.snapshot_id()
                    ));
                }
                if manifest_file.added_snapshot_id != snapshot.snapshot_id() {
                    continue;
                }
                let manifest = manifest_file
                    .load_manifest(file_io)
                    .await
                    .map_err(|e| format!("load manifest for snapshot {}: {e}", snapshot.snapshot_id()))?;
                for entry in manifest.entries() {
                    if entry.status == ManifestStatus::Deleted {
                        return Err(format!(
                            "incremental materialized view refresh does not support deleted manifest entries in snapshot {}",
                            snapshot.snapshot_id()
                        ));
                    }
                    if entry.status != ManifestStatus::Added {
                        continue;
                    }
                    let df = entry.data_file();
                    if df.content_type() != DataContentType::Data {
                        continue;
                    }
                    files.push((
                        df.file_path().to_string(),
                        i64::try_from(df.file_size_in_bytes()).unwrap_or(i64::MAX),
                        Some(df.record_count() as i64),
                    ));
                }
            }
        }
        Ok::<_, String>(files)
    })
    .map_err(|e| format!("plan append delta runtime failed: {e}"))??;

    Ok(IcebergAppendDelta {
        previous_snapshot_id,
        current_snapshot_id,
        added_files,
    })
}
```

Keep the ancestry check explicit in this task: previous snapshot must appear in the current snapshot parent chain, and a missing ancestor is an error.

- [ ] **Step 5: Re-export from `mod.rs`**

In `src/standalone/iceberg/mod.rs`, add `IcebergAppendDelta` and `plan_append_delta` to the `pub(crate) use registry::{...};` list.

- [ ] **Step 6: Run tests**

```bash
cargo test --lib standalone::iceberg::registry::phase2_delta_tests::plan_append_delta_collects_files_after_previous_snapshot -- --nocapture
cargo test --lib standalone::engine::tests::embedded_session_supports_minimal_iceberg_flow -- --nocapture
```

Expected: PASS.

- [ ] **Step 7: Commit**

```bash
git add src/standalone/iceberg/registry.rs src/standalone/iceberg/mod.rs
git commit -m "feat: plan iceberg append deltas"
```

---

## Task 6: Execute Stored MV SELECT Over Delta Files

**Files:**
- Modify: `src/standalone/engine/mod.rs`

- [ ] **Step 1: Split Iceberg table registration**

In `src/standalone/engine/mod.rs`, extract the storage registration logic from `register_iceberg_tables_for_query_impl` into:

```rust
fn register_loaded_iceberg_table_with_files(
    state: &Arc<StandaloneState>,
    entry: &crate::standalone::iceberg::IcebergCatalogEntry,
    namespace: &str,
    table_name: &str,
    loaded: crate::standalone::iceberg::IcebergLoadedTable,
    data_files: Vec<(String, i64, Option<i64>)>,
) -> Result<(), String> {
    let storage = if entry.is_s3() {
        let cloud_properties = entry.cloud_properties_map();
        crate::sql::catalog::TableStorage::S3ParquetFiles {
            files: data_files
                .into_iter()
                .map(|(path, size, row_count)| crate::sql::catalog::S3FileInfo {
                    path,
                    size,
                    row_count,
                    column_stats: None,
                })
                .collect(),
            cloud_properties,
        }
    } else {
        let Some((first_path, _, _)) = data_files.first() else {
            return register_empty_iceberg_table(state, namespace, table_name, loaded);
        };
        let local_path = first_path.strip_prefix("file://").unwrap_or(first_path);
        crate::sql::catalog::TableStorage::LocalParquetFile {
            path: std::path::PathBuf::from(local_path),
        }
    };
    let table_def = crate::sql::catalog::TableDef {
        name: table_name.to_string(),
        columns: loaded.columns,
        storage,
    };
    let mut guard = state.catalog.write().expect("catalog write lock");
    guard.create_database(namespace).ok();
    guard
        .register(namespace, table_def)
        .map_err(|e| format!("register iceberg table: {e}"))
}
```

Move the existing empty-table local parquet fallback into `register_empty_iceberg_table`.

- [ ] **Step 2: Add incremental query helper**

Add:

```rust
pub(crate) fn execute_query_for_mv_incremental_refresh(
    state: &Arc<StandaloneState>,
    current_database: &str,
    sql: &str,
    base_ref: &crate::standalone::lake::store::IcebergTableRef,
    delta_files: Vec<(String, i64, Option<i64>)>,
) -> Result<QueryResult, String> {
    let normalized = crate::sql::parser::dialect::normalize_for_raw_parse(sql)?;
    let statement = crate::sql::parser::parse_normalized_sql_raw(&normalized)
        .map_err(|e| format!("sql parser error: {e}"))?;
    let sqlparser::ast::Statement::Query(query) = statement else {
        return Err("REFRESH MATERIALIZED VIEW stored SQL must be a SELECT query".to_string());
    };

    let entry = {
        let registry = state.iceberg_catalogs.read().expect("iceberg registry read lock");
        registry.get(&base_ref.catalog)?
    };
    let loaded = crate::standalone::iceberg::load_table(&entry, &base_ref.namespace, &base_ref.table)?;
    register_loaded_iceberg_table_with_files(
        state,
        &entry,
        &base_ref.namespace,
        &base_ref.table,
        loaded,
        delta_files,
    )?;

    let mut executable = query.as_ref().clone();
    strip_catalog_from_three_part_names(&mut executable);
    let catalog = state.catalog.read().expect("standalone catalog read lock");
    execute_query(
        &executable,
        &catalog,
        current_database,
        state.exchange_port,
        None,
    )
}
```

- [ ] **Step 3: Run tests**

```bash
cargo test --lib standalone::engine -- --nocapture
```

Expected: PASS.

- [ ] **Step 4: Commit**

```bash
git add src/standalone/engine/mod.rs
git commit -m "feat: execute mv refresh over iceberg delta files"
```

---

## Task 7: Wire Incremental Refresh Strategy

**Files:**
- Modify: `src/standalone/lake/mv_refresh.rs`

- [ ] **Step 1: Add strategy tests with injected executor**

In `src/standalone/lake/mv_refresh.rs`, add tests that exercise strategy selection without object-store IO by extracting the decision into a pure helper:

```rust
#[derive(Clone, Debug, PartialEq, Eq)]
enum MvRefreshStrategy {
    Full,
    NoOp { current_snapshot_id: i64 },
    Incremental { previous_snapshot_id: i64, current_snapshot_id: i64 },
}

#[cfg(test)]
mod strategy_tests {
    use super::*;

    #[test]
    fn choose_strategy_uses_full_when_no_previous_snapshot() {
        let strategy = choose_refresh_strategy(None, Some(10)).expect("strategy");
        assert_eq!(strategy, MvRefreshStrategy::Full);
    }

    #[test]
    fn choose_strategy_noops_when_snapshot_unchanged() {
        let strategy = choose_refresh_strategy(Some(10), Some(10)).expect("strategy");
        assert_eq!(strategy, MvRefreshStrategy::NoOp { current_snapshot_id: 10 });
    }

    #[test]
    fn choose_strategy_incremental_when_snapshot_advances() {
        let strategy = choose_refresh_strategy(Some(10), Some(12)).expect("strategy");
        assert_eq!(
            strategy,
            MvRefreshStrategy::Incremental {
                previous_snapshot_id: 10,
                current_snapshot_id: 12,
            }
        );
    }
}
```

- [ ] **Step 2: Run tests to verify failure**

```bash
cargo test --lib standalone::lake::mv_refresh::strategy_tests -- --nocapture
```

Expected: FAIL because `choose_refresh_strategy` does not exist.

- [ ] **Step 3: Implement strategy helper**

In `src/standalone/lake/mv_refresh.rs`, add:

```rust
#[derive(Clone, Debug, PartialEq, Eq)]
enum MvRefreshStrategy {
    Full,
    NoOp { current_snapshot_id: i64 },
    Incremental { previous_snapshot_id: i64, current_snapshot_id: i64 },
}

fn choose_refresh_strategy(
    previous_snapshot_id: Option<i64>,
    current_snapshot_id: Option<i64>,
) -> Result<MvRefreshStrategy, String> {
    match (previous_snapshot_id, current_snapshot_id) {
        (None, _) => Ok(MvRefreshStrategy::Full),
        (Some(previous), Some(current)) if previous == current => {
            Ok(MvRefreshStrategy::NoOp { current_snapshot_id: current })
        }
        (Some(previous), Some(current)) => Ok(MvRefreshStrategy::Incremental {
            previous_snapshot_id: previous,
            current_snapshot_id: current,
        }),
        (Some(previous), None) => Err(format!(
            "cannot incrementally refresh materialized view: Iceberg snapshot {previous} is no longer reachable"
        )),
    }
}
```

- [ ] **Step 4: Implement incremental refresh path**

Refactor `refresh_mv` so it:

1. loads `mv_row`;
2. calls `mv_shape::classify_incremental_mv_query` on `mv_row.select_sql`;
3. asserts `mv_row.base_table_refs.len() == 1`;
4. loads the current Iceberg table and current snapshot id;
5. computes previous snapshot id from `mv_row.last_refresh_snapshots.get(&base_ref.fqn())`;
6. dispatches:
   - `Full` -> existing `refresh_mv_full_with_executor`;
   - `NoOp` -> `metadata_store.update_mv_refresh_metadata(...)`;
   - `Incremental` -> `plan_append_delta`, `execute_query_for_mv_incremental_refresh`, `query_result_to_chunks`, `write_chunks_into_managed_partition_for_mv_refresh`.

The incremental write uses:

```rust
let new_total_rows = mv_row.last_refresh_rows.unwrap_or(0) + rows_written;
write_chunks_into_managed_partition_for_mv_refresh(
    state,
    plan,
    &chunks,
    UpdateMvRefreshMetadataRequest {
        table_id: runtime.table.table_id,
        last_refresh_rows: new_total_rows,
        snapshots,
    },
)?;
```

For no-op:

```rust
metadata_store.update_mv_refresh_metadata(UpdateMvRefreshMetadataRequest {
    table_id: runtime.table.table_id,
    last_refresh_rows: mv_row.last_refresh_rows.unwrap_or(0),
    snapshots,
})?;
```

After either no-op or incremental update, call `refresh_managed_catalog(state)?`.

- [ ] **Step 5: Run targeted tests**

```bash
cargo test --lib standalone::lake::mv_refresh -- --nocapture
cargo test --lib standalone::lake -- --nocapture
```

Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add src/standalone/lake/mv_refresh.rs
git commit -m "feat: incrementally refresh projection mvs"
```

---

## Task 8: Add MySQL Integration Coverage

**Files:**
- Modify: `tests/standalone_mysql_server.rs`

- [ ] **Step 1: Add no-op and append incremental tests**

Append tests:

```rust
#[test]
fn standalone_mysql_server_mv_incremental_refresh_noops_when_snapshot_unchanged() {
    let port = alloc_port();
    let Some((_config_dir, config_path)) = maybe_write_managed_lake_config(port) else {
        return;
    };
    let iceberg_warehouse = unique_iceberg_warehouse("mv_incremental_noop");
    let args = vec![
        "standalone-server".to_string(),
        "--config".to_string(),
        config_path.display().to_string(),
    ];
    let mut server = ServerGuard::spawn(&args);
    let mut conn = server.connect_root(port);
    conn.query_drop(create_s3_iceberg_catalog_sql("ice", &iceberg_warehouse)).expect("catalog");
    conn.query_drop("create database ice.ns").expect("ns");
    conn.query_drop("create table ice.ns.orders (k1 int, v2 bigint)").expect("base");
    conn.query_drop("insert into ice.ns.orders values (1, 10), (2, 20)").expect("seed");
    conn.query_drop("create database analytics").expect("db");
    conn.query_drop("use analytics").expect("use");
    conn.query_drop(
        "create materialized view orders_mv distributed by hash(k1) buckets 2 \
         as select k1, v2 from ice.ns.orders where v2 >= 10",
    )
    .expect("mv");
    conn.query_drop("refresh materialized view orders_mv").expect("first refresh");
    conn.query_drop("refresh materialized view orders_mv").expect("noop refresh");
    let rows: Vec<(Option<i32>, Option<i64>)> = conn
        .query("select k1, v2 from orders_mv order by k1")
        .expect("rows");
    assert_eq!(rows, vec![(Some(1), Some(10)), (Some(2), Some(20))]);
}

#[test]
fn standalone_mysql_server_mv_incremental_refresh_appends_only_new_rows() {
    let port = alloc_port();
    let Some((_config_dir, config_path)) = maybe_write_managed_lake_config(port) else {
        return;
    };
    let iceberg_warehouse = unique_iceberg_warehouse("mv_incremental_append");
    let args = vec![
        "standalone-server".to_string(),
        "--config".to_string(),
        config_path.display().to_string(),
    ];
    let mut server = ServerGuard::spawn(&args);
    let mut conn = server.connect_root(port);
    conn.query_drop(create_s3_iceberg_catalog_sql("ice", &iceberg_warehouse)).expect("catalog");
    conn.query_drop("create database ice.ns").expect("ns");
    conn.query_drop("create table ice.ns.orders (k1 int, v2 bigint)").expect("base");
    conn.query_drop("insert into ice.ns.orders values (1, 10), (2, 20)").expect("seed");
    conn.query_drop("create database analytics").expect("db");
    conn.query_drop("use analytics").expect("use");
    conn.query_drop(
        "create materialized view orders_mv distributed by hash(k1) buckets 2 \
         as select k1, v2 from ice.ns.orders where v2 >= 20",
    )
    .expect("mv");
    conn.query_drop("refresh materialized view orders_mv").expect("first refresh");
    conn.query_drop("insert into ice.ns.orders values (3, 30), (4, 5)").expect("append");
    conn.query_drop("refresh materialized view orders_mv").expect("incremental refresh");
    let rows: Vec<(Option<i32>, Option<i64>)> = conn
        .query("select k1, v2 from orders_mv order by k1")
        .expect("rows");
    assert_eq!(rows, vec![(Some(2), Some(20)), (Some(3), Some(30))]);
}
```

- [ ] **Step 2: Add unsupported aggregate CREATE test**

```rust
#[test]
fn standalone_mysql_server_mv_incremental_rejects_aggregate_definition() {
    let port = alloc_port();
    let Some((_config_dir, config_path)) = maybe_write_managed_lake_config(port) else {
        return;
    };
    let iceberg_warehouse = unique_iceberg_warehouse("mv_incremental_reject");
    let args = vec![
        "standalone-server".to_string(),
        "--config".to_string(),
        config_path.display().to_string(),
    ];
    let mut server = ServerGuard::spawn(&args);
    let mut conn = server.connect_root(port);
    conn.query_drop(create_s3_iceberg_catalog_sql("ice", &iceberg_warehouse)).expect("catalog");
    conn.query_drop("create database ice.ns").expect("ns");
    conn.query_drop("create table ice.ns.orders (k1 int, v2 bigint)").expect("base");
    conn.query_drop("create database analytics").expect("db");
    conn.query_drop("use analytics").expect("use");
    let err = conn
        .query_drop(
            "create materialized view orders_mv distributed by hash(k1) buckets 2 \
             as select k1, sum(v2) total from ice.ns.orders group by k1",
        )
        .expect_err("aggregate should be rejected");
    assert!(
        err.to_string().contains("projection/filter"),
        "unexpected error: {err}"
    );
}
```

- [ ] **Step 3: Run integration tests**

```bash
cargo test --test standalone_mysql_server standalone_mysql_server_mv_incremental_ -- --nocapture --test-threads=1
```

Expected: PASS when MinIO is reachable; otherwise the tests return early through existing managed-lake skip behavior.

- [ ] **Step 4: Commit**

```bash
git add tests/standalone_mysql_server.rs
git commit -m "test: cover incremental mv refresh over iceberg"
```

---

## Task 9: Add SQL-Test Regression

**Files:**
- Create: `sql-tests/write-path/sql/managed_lake_mv_incremental.sql`
- Create: `sql-tests/write-path/result/managed_lake_mv_incremental.result`

- [ ] **Step 1: Write SQL case**

Create `sql-tests/write-path/sql/managed_lake_mv_incremental.sql`:

```sql
-- @order_sensitive=true
-- @tags=write_path,managed_lake,mv,iceberg,incremental
-- Test Objective:
-- 1. Validate first refresh materializes projection/filter MV over Iceberg.
-- 2. Validate second refresh with appended Iceberg rows adds only matching new rows.

-- query 1
-- @skip_result_check=true
CREATE EXTERNAL CATALOG mv_inc_ice_${uuid0}
PROPERTIES (
  "type" = "iceberg",
  "iceberg.catalog.type" = "hadoop",
  "iceberg.catalog.warehouse" = "${managed_lake_warehouse}/iceberg_inc_${uuid0}",
  "aws.s3.endpoint" = "${oss_endpoint}",
  "aws.s3.access_key" = "${oss_ak}",
  "aws.s3.secret_key" = "${oss_sk}",
  "aws.s3.enable_path_style_access" = "true"
);
CREATE DATABASE mv_inc_ice_${uuid0}.ns_${uuid0};
CREATE TABLE mv_inc_ice_${uuid0}.ns_${uuid0}.orders (
  k1 INT,
  v2 BIGINT
);
INSERT INTO mv_inc_ice_${uuid0}.ns_${uuid0}.orders VALUES
  (1, 10),
  (2, 20);
CREATE MATERIALIZED VIEW ${case_db}.orders_mv
DISTRIBUTED BY HASH(k1) BUCKETS 2
AS SELECT k1, v2 FROM mv_inc_ice_${uuid0}.ns_${uuid0}.orders WHERE v2 >= 20;

-- query 2
-- @skip_result_check=true
REFRESH MATERIALIZED VIEW ${case_db}.orders_mv;

-- query 3
SELECT k1, v2 FROM ${case_db}.orders_mv ORDER BY k1;

-- query 4
-- @skip_result_check=true
INSERT INTO mv_inc_ice_${uuid0}.ns_${uuid0}.orders VALUES (3, 30), (4, 5);
REFRESH MATERIALIZED VIEW ${case_db}.orders_mv;

-- query 5
SELECT k1, v2 FROM ${case_db}.orders_mv ORDER BY k1;

-- query 6
-- @skip_result_check=true
DROP MATERIALIZED VIEW ${case_db}.orders_mv;
DROP TABLE mv_inc_ice_${uuid0}.ns_${uuid0}.orders FORCE;
DROP DATABASE mv_inc_ice_${uuid0}.ns_${uuid0};
DROP CATALOG mv_inc_ice_${uuid0};
```

- [ ] **Step 2: Write expected result**

Create `sql-tests/write-path/result/managed_lake_mv_incremental.result`:

```text
-- query 3
k1	v2
2	20

-- query 5
k1	v2
2	20
3	30
```

- [ ] **Step 3: Run SQL-test**

Start standalone server in another terminal/session:

```bash
NO_PROXY=127.0.0.1,localhost cargo run -- standalone-server --config tests/sql-test-runner/conf/standalone_managed_lake.toml
```

Then run:

```bash
cargo run --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests -- \
  --config tests/sql-test-runner/conf/standalone_managed_lake.conf \
  --suite write-path --only managed_lake_mv_incremental --mode verify
```

Expected: PASS.

- [ ] **Step 4: Commit**

```bash
git add sql-tests/write-path/sql/managed_lake_mv_incremental.sql \
        sql-tests/write-path/result/managed_lake_mv_incremental.result
git commit -m "test: add incremental mv sql regression"
```

---

## Task 10: Final Verification

**Files:**
- Inspect all files touched in Tasks 1-9.

- [ ] **Step 1: Formatting and whitespace**

```bash
cargo fmt
git diff --check
```

Expected: both commands complete successfully.

- [ ] **Step 2: Targeted library tests**

```bash
cargo test --lib standalone::lake::mv_shape -- --nocapture
cargo test --lib standalone::lake::mv_ddl -- --nocapture
cargo test --lib standalone::lake::mv_refresh -- --nocapture
cargo test --lib standalone::lake::txn -- --nocapture
cargo test --lib standalone::lake::store::tests -- --nocapture
cargo test --lib standalone::iceberg::registry -- --nocapture
cargo test --lib standalone::engine -- --nocapture
```

Expected: PASS.

- [ ] **Step 3: Integration and SQL-test verification**

```bash
cargo test --test standalone_mysql_server standalone_mysql_server_mv_ -- --nocapture --test-threads=1
cargo run --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests -- \
  --config tests/sql-test-runner/conf/standalone_managed_lake.conf \
  --suite write-path --only managed_lake_mv_basic,managed_lake_mv_incremental --mode verify
```

Expected: PASS when MinIO is reachable; skip behavior only applies to Rust tests that already check managed-lake config availability.

- [ ] **Step 4: Clippy reality check**

```bash
cargo clippy --all-targets -- -D warnings
```

Expected: either PASS or fail only on pre-existing unrelated `src/build.rs` warnings. If new warnings appear in files touched by Phase 2, fix them before final commit.

- [ ] **Step 5: Final status**

```bash
git status --short
git log --oneline --decorate -8
```

Expected: clean worktree after all commits; latest commits are the Phase 2 task commits.
