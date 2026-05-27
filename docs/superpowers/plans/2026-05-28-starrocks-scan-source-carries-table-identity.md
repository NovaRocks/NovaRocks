# StarRocks `ScanSource` carries table identity — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make `DictionaryQueryProvider::owner_for` lock-free for StarRocks tables by carrying `(db_id, table_id)` inside `ScanSource::StarRocks`, eliminating the hot reader on `state.starrocks_table` introduced by PR #191/#194.

**Architecture:** Mirror the existing Iceberg pattern — `ScanSource::IcebergDataFiles { table: info, .. }` already carries plan-time identity. Add `{ db_id, table_id }` payload to `ScanSource::StarRocks` (currently a unit variant), populate it from `StarRocksTableRuntime.table.db_id/table_id` at the four production registration sites, and let `owner_for` read identity directly from the plan node. A `debug_assert!` in `InMemoryCatalog::register_starrocks_table` guards future drift between `TableDef::source` and `PhysicalTableLayout`.

**Tech Stack:** Rust 1.x (stable), `std::sync::RwLock`, `cargo` / `cargo test --lib`, the project's `sql-tests` binary for SQL suite verification.

**Spec:** [`docs/superpowers/specs/2026-05-28-starrocks-scan-source-carries-table-identity-design.md`](../specs/2026-05-28-starrocks-scan-source-carries-table-identity-design.md)

---

## File Structure

### Core change (3 files)

- **`src/sql/catalog.rs`** — enum definition. The single source of truth for `ScanSource::StarRocks`. Change unit variant → struct variant with `{ db_id: i64, table_id: i64 }`.
- **`src/engine/dictionary/mod.rs`** — consumer. Rewrite the `StarRocks` arm in `DictionaryQueryProvider::owner_for` to read identity from the plan node; drop the `state.starrocks_table.read()` call. Add one new TDD-anchor unit test in the existing `tests` module.
- **`src/engine/catalog.rs`** — registration invariant. Add `debug_assert!` in `InMemoryCatalog::register_starrocks_table` ensuring `TableDef::source` and `PhysicalTableLayout` agree on `(db_id, table_id)`. Update an existing test fixture (`test_table`) to use the new variant shape.

### Production constructor sites (3 files outside the core)

- **`src/connector/starrocks/table/catalog.rs:621`** — `starrocks_table_def(runtime)` is the main DDL / refresh path. Reads `runtime.table.db_id`, `runtime.table.table_id`. Also has an existing test (`matches!` assertion on line 983) and an opportunity to add a new unit test.
- **`src/connector/starrocks/table/mv_refresh.rs:4562`** — a test-only `state_with_orders_table()` builder. Uses placeholder IDs because the test does not flow through the dict subsystem.
- **`src/engine/mod.rs:4070`** — a test-only `register_starrocks_table` call site exercising analyzer/planner end-to-end. Uses placeholder IDs (matches its existing `PhysicalTableLayout { db_id: 1, table_id: 2, ... }`).

### Mechanical compile-fix sites (15+ files, no behavior change)

Either match-only (need `{ .. }` rest pattern) or fixture construction (need `{ db_id: 0, table_id: 0 }` placeholder). The compiler enumerates these — list per spec:

Match-only:
- `src/engine/dictionary/rebuild.rs:60`
- `src/engine/dictionary/rebuild.rs:103`
- `src/sql/explain.rs:844`
- `src/sql/explain.rs:867`

Fixture construction (no dict-rewrite exposure):
- `src/sql/optimizer/convert.rs:338`
- `src/sql/optimizer/mod.rs:395`
- `src/sql/optimizer/logical_props.rs:355`
- `src/sql/optimizer/rewrite/context.rs:286`
- `src/sql/optimizer/rewrite/tree.rs:400`
- `src/sql/optimizer/cte_rewrite.rs:309`
- `src/sql/optimizer/rewrite/rules/predicate_pushdown/push_to_scan.rs:125`
- `src/sql/optimizer/rewrite/rules/predicate_pushdown/push_to_join.rs:500`
- `src/sql/optimizer/cost.rs:207`
- `src/sql/optimizer/cost.rs:288`
- `src/sql/explain.rs:1102`
- `src/sql/explain.rs:1147`
- `src/sql/explain.rs:1188`
- `src/sql/explain.rs:1265`
- `src/sql/explain.rs:1329`

### Test surfaces

- New TDD anchor in `src/engine/dictionary/mod.rs::tests` (proves lock-free behavior; fails before fix).
- New unit test in `src/connector/starrocks/table/catalog.rs::tests` (proves `starrocks_table_def` populates IDs from the runtime).
- Existing `iceberg-ivm` SQL suite (regression gate; must stay 61/61).
- `filter` SQL suite at default parallelism (acceptance gate; goes from fail=10/15 → 15/15 PASS).
- `filter` SQL suite at `-j 1` (no regression in sequential perf).

---

## Worktree assumptions

This plan executes inside the active worktree at `/Users/harbor/project/NovaRocks/.claude/worktrees/goofy-yonath-bbc82d` on branch `claude/goofy-yonath-bbc82d`. The shared Docker fixture is already running (MinIO `9000`, REST `8181`, Spark `4040`); this worktree's MySQL port is `9128`. The generated runtime entry is at `docker/iceberg-rest/runtime/current/` and exposes `$NOVAROCKS_STANDALONE_CONFIG` / `$NOVAROCKS_SQL_TEST_CONFIG` after `source docker/iceberg-rest/runtime/current/env.sh`.

---

## Task 1: TDD anchor — failing test for lock-free dict owner lookup

**Files:**
- Modify: `src/engine/dictionary/mod.rs` (append to existing `#[cfg(test)] mod tests` block, around line 358+)

- [ ] **Step 1.1: Add the failing test**

Append to the `tests` module in `src/engine/dictionary/mod.rs` (after the existing tests, inside `mod tests { ... }`):

```rust
    /// Lock-free owner-lookup contract: `DictionaryQueryProvider::owner_for`
    /// must derive `(db_id, table_id)` from `ScanSource::StarRocks` directly,
    /// without consulting `state.starrocks_table`. This test leaves
    /// `state.starrocks_table` empty (no runtime registered) but registers a
    /// snapshot owned by `(db_id=100, table_id=200)`; if the provider still
    /// tries to look up the runtime in the catalog, it would return `Ok(None)`
    /// and `load_active_snapshot` would miss the snapshot. After the fix,
    /// identity flows from the plan node and the snapshot is found.
    #[test]
    fn dictionary_provider_owner_for_starrocks_reads_identity_from_plan_node() {
        use crate::engine::dictionary::DictionaryQueryProvider;
        use crate::sql::catalog::{ColumnDef, ScanSource, TableDef};
        use arrow::datatypes::DataType;
        use std::sync::Arc;

        let (_dir, state) = open_state();
        let state = Arc::new(state);

        // sample_owner() / sample_snapshot() use db_id=100, table_id=200,
        // database="demo", table="t1", column_name="s".
        let snapshot = sample_snapshot(1, DictionaryState::Active);
        state
            .dictionary_manager
            .upsert_snapshot(&state, snapshot.clone())
            .expect("upsert sample snapshot");

        // Construct a Scan-level TableDef carrying the SAME (db_id, table_id)
        // in ScanSource. Note: state.starrocks_table is empty — no runtime
        // is registered. The lock-free provider must consult ScanSource
        // directly to resolve the owner.
        let table = TableDef {
            name: "t1".to_string(),
            columns: vec![ColumnDef {
                name: "s".to_string(),
                data_type: DataType::Utf8,
                nullable: false,
                write_default: None,
                logical_type: None,
            }],
            iceberg_row_lineage_metadata_columns: vec![],
            source: ScanSource::StarRocks {
                db_id: 100,
                table_id: 200,
            },
        };

        let provider = DictionaryQueryProvider::new(state);
        let loaded = provider
            .load_active_snapshot(&table, "demo", "s")
            .expect("load_active_snapshot returns Ok");

        assert!(
            loaded.is_some(),
            "lock-free owner_for must resolve identity from ScanSource::StarRocks {{ db_id, table_id }} payload, not from state.starrocks_table",
        );
    }
```

- [ ] **Step 1.2: Confirm the test fails to compile**

Run:
```bash
cargo build --tests 2>&1 | grep -E "error\[|dictionary_provider_owner_for_starrocks_reads_identity_from_plan_node" | head -20
```

Expected: compilation errors including
`error[E0559]: variant `ScanSource::StarRocks` has no field named `db_id``
(or equivalent — the enum variant is a unit variant and does not accept fields).

This is the failing-test signal. Do NOT commit yet; the next task migrates the enum.

---

## Task 2: Migrate `ScanSource::StarRocks` to carry `{ db_id, table_id }` (atomic batch)

This task is a single mechanical migration. The enum change cascades through ~25 sites; build only goes green after every site is updated. We do them together and commit once.

**Files:**
- Modify: `src/sql/catalog.rs:195-218`
- Modify: `src/engine/dictionary/rebuild.rs:60`
- Modify: `src/engine/dictionary/rebuild.rs:103`
- Modify: `src/sql/explain.rs:844`
- Modify: `src/sql/explain.rs:867`
- Modify: `src/sql/optimizer/convert.rs:338`
- Modify: `src/sql/optimizer/mod.rs:395`
- Modify: `src/sql/optimizer/logical_props.rs:355`
- Modify: `src/sql/optimizer/rewrite/context.rs:286`
- Modify: `src/sql/optimizer/rewrite/tree.rs:400`
- Modify: `src/sql/optimizer/cte_rewrite.rs:309`
- Modify: `src/sql/optimizer/rewrite/rules/predicate_pushdown/push_to_scan.rs:125`
- Modify: `src/sql/optimizer/rewrite/rules/predicate_pushdown/push_to_join.rs:500`
- Modify: `src/sql/optimizer/cost.rs:207`
- Modify: `src/sql/optimizer/cost.rs:288`
- Modify: `src/sql/explain.rs:1102`
- Modify: `src/sql/explain.rs:1147`
- Modify: `src/sql/explain.rs:1188`
- Modify: `src/sql/explain.rs:1265`
- Modify: `src/sql/explain.rs:1329`
- Modify: `src/engine/catalog.rs:339` (the `test_table` fixture)
- Modify: `src/connector/starrocks/table/mv_refresh.rs:4562`
- Modify: `src/engine/mod.rs:4070`

- [ ] **Step 2.1: Update the enum definition**

In `src/sql/catalog.rs`, replace the `StarRocks` variant (currently lines 196-203):

OLD:
```rust
pub enum ScanSource {
    /// StarRocks table: data lives in object storage (s3:// or
    /// file://) and metadata lives in a `MetaStoreProvider` (currently
    /// SQLite). The per-table physical layout (tablet/partition/version
    /// list) is carried separately on `PhysicalTableLayout`, so this
    /// variant is a marker without payload — the catalog only needs to
    /// know "this table flows through the StarRocks table scan path".
    StarRocks,
    IcebergDataFiles {
```

NEW:
```rust
pub enum ScanSource {
    /// StarRocks table: data lives in object storage (s3:// or
    /// file://) and metadata lives in a `MetaStoreProvider` (currently
    /// SQLite). The per-table physical layout (tablet/partition/version
    /// list) is carried separately on `PhysicalTableLayout`; the
    /// `(db_id, table_id)` identity carried here lets plan-time consumers
    /// (e.g. `DictionaryQueryProvider::owner_for`) resolve the StarRocks
    /// dictionary owner without taking `state.starrocks_table.read()` on
    /// every Scan column. The two fields must always agree with the
    /// matching `PhysicalTableLayout` entry; `InMemoryCatalog::register_starrocks_table`
    /// enforces this invariant in debug builds.
    StarRocks {
        db_id: i64,
        table_id: i64,
    },
    IcebergDataFiles {
```

- [ ] **Step 2.2: Update the four match-only sites to use `{ .. }` rest pattern**

In `src/engine/dictionary/rebuild.rs`:

Line 60 (inside a `match` on `&table.source`):
OLD: `ScanSource::StarRocks => build_starrocks_watermark(state, database, table)?,`
NEW: `ScanSource::StarRocks { .. } => build_starrocks_watermark(state, database, table)?,`

Line 103 (inside a `match` on `&table.source`):
OLD: `ScanSource::StarRocks => {`
NEW: `ScanSource::StarRocks { .. } => {`

In `src/sql/explain.rs`:

Line 844 (in `ScanSource::IcebergDataFiles { .. } | ScanSource::StarRocks => {`):
OLD: `ScanSource::IcebergDataFiles { .. } | ScanSource::StarRocks => {`
NEW: `ScanSource::IcebergDataFiles { .. } | ScanSource::StarRocks { .. } => {`

Line 867 (same pattern):
OLD: `ScanSource::IcebergDataFiles { .. } | ScanSource::StarRocks => {}`
NEW: `ScanSource::IcebergDataFiles { .. } | ScanSource::StarRocks { .. } => {}`

- [ ] **Step 2.3: Update fixture construction sites to use `{ db_id: 0, table_id: 0 }` placeholders**

Each of the following sites currently constructs a `TableDef` with `source: ScanSource::StarRocks,`. Change to `source: ScanSource::StarRocks { db_id: 0, table_id: 0 },`.

The sites are pure fixtures: optimizer rule fixtures, EXPLAIN pretty-printer fixtures, cost-model fixtures, predicate-pushdown fixtures. None of them flow through the dict rewrite (which is the only consumer of these IDs), so placeholders are correct.

Sites to update (one line each, identical edit):
- `src/sql/optimizer/convert.rs:338`
- `src/sql/optimizer/mod.rs:395`
- `src/sql/optimizer/logical_props.rs:355`
- `src/sql/optimizer/rewrite/context.rs:286`
- `src/sql/optimizer/rewrite/tree.rs:400`
- `src/sql/optimizer/cte_rewrite.rs:309`
- `src/sql/optimizer/rewrite/rules/predicate_pushdown/push_to_scan.rs:125`
- `src/sql/optimizer/rewrite/rules/predicate_pushdown/push_to_join.rs:500`
- `src/sql/optimizer/cost.rs:207`
- `src/sql/optimizer/cost.rs:288`
- `src/sql/explain.rs:1102`
- `src/sql/explain.rs:1147`
- `src/sql/explain.rs:1188`
- `src/sql/explain.rs:1265`
- `src/sql/explain.rs:1329`
- `src/engine/catalog.rs:339` (the `test_table` helper)
- `src/connector/starrocks/table/mv_refresh.rs:4562`

For `src/engine/mod.rs:4070`, the fixture sets `PhysicalTableLayout { db_id: 1, table_id: 2, ... }`. To respect the future debug_assert in Task 4, use matching IDs:

OLD:
```rust
            iceberg_row_lineage_metadata_columns: vec![],
            source: ScanSource::StarRocks,
        };
        let layout = PhysicalTableLayout {
            db_id: 1,
            table_id: 2,
```

NEW:
```rust
            iceberg_row_lineage_metadata_columns: vec![],
            source: ScanSource::StarRocks { db_id: 1, table_id: 2 },
        };
        let layout = PhysicalTableLayout {
            db_id: 1,
            table_id: 2,
```

- [ ] **Step 2.4: Update the `matches!` assertion in `catalog.rs:983`**

In `src/connector/starrocks/table/catalog.rs:983`:

OLD: `assert!(matches!(table.source, ScanSource::StarRocks));`
NEW: `assert!(matches!(table.source, ScanSource::StarRocks { .. }));`

- [ ] **Step 2.5: Update `starrocks_table_def` (the main production constructor)**

In `src/connector/starrocks/table/catalog.rs`, around line 617-622:

OLD:
```rust
    Ok(TableDef {
        name: runtime.table.name.clone(),
        columns,
        iceberg_row_lineage_metadata_columns: vec![],
        source: ScanSource::StarRocks,
    })
```

NEW:
```rust
    Ok(TableDef {
        name: runtime.table.name.clone(),
        columns,
        iceberg_row_lineage_metadata_columns: vec![],
        source: ScanSource::StarRocks {
            db_id: runtime.table.db_id,
            table_id: runtime.table.table_id,
        },
    })
```

This is the only production constructor whose IDs MATTER — the path that registers real StarRocks tables for dict consumption. `runtime.table.db_id` / `runtime.table.table_id` are `i64` fields on `StoredStarRocksTable` (see `src/meta/repository/starrocks_table.rs:40-46`).

- [ ] **Step 2.6: Run `cargo build` and confirm clean**

Run:
```bash
cargo build 2>&1 | tail -5
```

Expected: `Finished \`dev\` profile [unoptimized + debuginfo] target(s)` (no errors). Warnings about unused variables / dead code are fine.

- [ ] **Step 2.7: Run `cargo build --tests` and confirm clean**

Run:
```bash
cargo build --tests 2>&1 | tail -5
```

Expected: build succeeds (tests now compile, including the Task 1 anchor test).

- [ ] **Step 2.8: Verify the Task 1 anchor test now compiles but FAILS at runtime**

Run:
```bash
cargo test --lib engine::dictionary::tests::dictionary_provider_owner_for_starrocks_reads_identity_from_plan_node 2>&1 | tail -15
```

Expected: the test runs and FAILS with
`assertion failed: loaded.is_some()` or `lock-free owner_for must resolve identity from ScanSource::StarRocks { db_id, table_id } payload, not from state.starrocks_table`.

This confirms the migration is mechanical-only (no behavior change yet) and the TDD anchor is wired correctly.

- [ ] **Step 2.9: Run the broader lib tests to confirm no incidental regressions**

Run:
```bash
cargo test --lib 2>&1 | tail -10
```

Expected: only `dictionary_provider_owner_for_starrocks_reads_identity_from_plan_node` fails; everything else passes. Note the exact pass/fail counts for the next step's commit message.

- [ ] **Step 2.10: Commit the mechanical migration**

```bash
git add -A
git commit -m "$(cat <<'EOF'
refactor(catalog): ScanSource::StarRocks carries (db_id, table_id)

Mechanical-only enum migration; behavior unchanged. The new payload
mirrors how ScanSource::IcebergDataFiles already carries IcebergTableInfo,
giving plan-time consumers a path to resolve StarRocks dict identity
without taking state.starrocks_table.read() on every Scan column.

Production constructor (`starrocks_table_def`) populates the payload
from `runtime.table.db_id/table_id`. Fixture construction sites use
{ db_id: 0, table_id: 0 } placeholders because they do not flow through
the dict rewrite consumer.

Follow-ups in subsequent commits: rewrite DictionaryQueryProvider::owner_for
to read the new payload (drop the read lock), add a debug_assert in
InMemoryCatalog::register_starrocks_table guarding the
TableDef.source ↔ PhysicalTableLayout invariant.

The dictionary_provider_owner_for_starrocks_reads_identity_from_plan_node
test (added in the same commit) is the TDD anchor — it currently fails
because owner_for still locks; it will pass after the consumer rewrite.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

## Task 3: Add the `register_starrocks_table` debug_assert (invariant guard)

**Files:**
- Modify: `src/engine/catalog.rs:106-121` (`InMemoryCatalog::register_starrocks_table`)

- [ ] **Step 3.1: Add the debug_assert**

In `src/engine/catalog.rs`, inside `register_starrocks_table`, insert the assert at the top of the function body (after parameter validation, before any state mutation):

OLD:
```rust
    pub(crate) fn register_starrocks_table(
        &mut self,
        database_name: &str,
        table: TableDef,
        physical_layout: PhysicalTableLayout,
    ) -> Result<(), String> {
        let db_key = normalize_identifier(database_name)?;
        let db = self
            .databases
            .get_mut(&db_key)
            .ok_or_else(|| format!("unknown database: {database_name}"))?;
```

NEW:
```rust
    pub(crate) fn register_starrocks_table(
        &mut self,
        database_name: &str,
        table: TableDef,
        physical_layout: PhysicalTableLayout,
    ) -> Result<(), String> {
        // Invariant: TableDef::source for a StarRocks table must carry the
        // same (db_id, table_id) as the PhysicalTableLayout. The dict provider
        // resolves identity from ScanSource::StarRocks; the rest of the
        // execution layer still uses PhysicalTableLayout. If they disagree,
        // dict snapshots will silently miss. Asserted in debug builds only —
        // release builds skip to keep the registration path tight.
        debug_assert!(
            matches!(
                &table.source,
                ScanSource::StarRocks { db_id, table_id }
                    if *db_id == physical_layout.db_id && *table_id == physical_layout.table_id
            ),
            "StarRocks TableDef.source must agree with PhysicalTableLayout on (db_id, table_id); \
             got source={:?} layout=(db_id={}, table_id={})",
            table.source,
            physical_layout.db_id,
            physical_layout.table_id,
        );

        let db_key = normalize_identifier(database_name)?;
        let db = self
            .databases
            .get_mut(&db_key)
            .ok_or_else(|| format!("unknown database: {database_name}"))?;
```

If `ScanSource` does not derive `Debug`, the `{:?}` formatter call will fail to compile; check by skimming the enum declaration in `src/sql/catalog.rs`. If `Debug` is not derived (it is, per Step 2.1 — `#[derive(Clone, Debug)]` already), use the variant name in the message instead.

- [ ] **Step 3.2: Update the `test_table` fixture in the same file to satisfy the assert**

In `src/engine/catalog.rs:328-341` (the `test_table` helper added in Task 2 with `{ db_id: 0, table_id: 0 }` placeholders):

The existing tests in this module register `test_table()` with a `PhysicalTableLayout { db_id: 10, table_id: 20, ... }` (see line 346-355 from spec exploration). With the new debug_assert, the placeholder `{ db_id: 0, table_id: 0 }` in `test_table()` would trip the assert in debug builds.

Change `test_table()` to accept the layout's IDs or hardcode matching values. Pick hardcoded matching values (simplest):

OLD (the version landed in Task 2):
```rust
    fn test_table(name: &str) -> TableDef {
        TableDef {
            name: name.to_string(),
            columns: vec![ColumnDef {
                name: "id".to_string(),
                data_type: DataType::Int32,
                nullable: false,
                write_default: None,
                logical_type: None,
            }],
            iceberg_row_lineage_metadata_columns: vec![],
            source: ScanSource::StarRocks { db_id: 0, table_id: 0 },
        }
    }
```

NEW:
```rust
    fn test_table(name: &str) -> TableDef {
        TableDef {
            name: name.to_string(),
            columns: vec![ColumnDef {
                name: "id".to_string(),
                data_type: DataType::Int32,
                nullable: false,
                write_default: None,
                logical_type: None,
            }],
            iceberg_row_lineage_metadata_columns: vec![],
            // Must match the PhysicalTableLayout used by the tests in this
            // module (db_id: 10, table_id: 20) so the debug_assert in
            // register_starrocks_table is satisfied.
            source: ScanSource::StarRocks { db_id: 10, table_id: 20 },
        }
    }
```

- [ ] **Step 3.3: Run the catalog tests to confirm the debug_assert is satisfied**

Run:
```bash
cargo test --lib engine::catalog::tests 2>&1 | tail -20
```

Expected: all tests in the `engine::catalog::tests` module pass. If `register_starrocks_table_tracks_and_clears_physical_layout` (or any sibling) panics on the debug_assert, fix the corresponding fixture to use matching IDs.

- [ ] **Step 3.4: Run the broader lib test suite to confirm no other fixture trips the assert**

Run:
```bash
cargo test --lib 2>&1 | tail -10
```

Expected: same baseline as Step 2.9 — only the Task 1 anchor test fails. No new failures from the debug_assert.

If a test panics with the new assert message, the offending fixture's `TableDef::source` IDs and `PhysicalTableLayout` IDs disagree. Fix by matching them (prefer using the real IDs the test already has on the layout).

- [ ] **Step 3.5: Commit the invariant guard**

```bash
git add src/engine/catalog.rs
git commit -m "$(cat <<'EOF'
fix(catalog): guard ScanSource::StarRocks vs PhysicalTableLayout identity

debug_assert! in InMemoryCatalog::register_starrocks_table catches future
construction sites that drift from the (db_id, table_id) invariant. Release
builds skip; debug builds (sql-tests, cargo test) trip immediately.

Updates the engine::catalog test_table fixture to match the layout IDs the
existing tests in the module already use.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

## Task 4: Make `DictionaryQueryProvider::owner_for` lock-free for StarRocks

This is the behavior-change commit — it makes the Task 1 anchor test pass and eliminates the hot reader on `state.starrocks_table`.

**Files:**
- Modify: `src/engine/dictionary/mod.rs:307-323`

- [ ] **Step 4.1: Rewrite the `StarRocks` arm in `owner_for`**

In `src/engine/dictionary/mod.rs`, replace the StarRocks arm (currently lines 307-324):

OLD:
```rust
    fn owner_for(
        &self,
        table: &TableDef,
        database: &str,
    ) -> Result<Option<DictionaryOwner>, String> {
        match &table.source {
            ScanSource::StarRocks => {
                let catalog = self
                    .state
                    .starrocks_table
                    .read()
                    .map_err(|e| format!("starrocks table catalog read lock poisoned: {e}"))?;
                let runtime = match catalog.table(database, &table.name) {
                    Ok(rt) => rt,
                    Err(_) => return Ok(None),
                };
                Ok(Some(DictionaryOwner::StarRocksTable {
                    database: database.to_string(),
                    table: table.name.clone(),
                    db_id: runtime.table.db_id,
                    table_id: runtime.table.table_id,
                }))
            }
            ScanSource::IcebergDataFiles { table: info, .. } => {
                Ok(Some(DictionaryOwner::IcebergTable {
                    catalog: info.catalog.clone(),
                    namespace: info.namespace.clone(),
                    table: info.table.clone(),
                    table_uuid: info.table_uuid.clone(),
                }))
            }
            // Metadata tables and IVM delta scans never participate in
            // dictionary rewriting.
            ScanSource::IcebergMetadataTable { .. } | ScanSource::IcebergDeltaTable { .. } => {
                Ok(None)
            }
        }
    }
```

NEW:
```rust
    fn owner_for(
        &self,
        table: &TableDef,
        database: &str,
    ) -> Result<Option<DictionaryOwner>, String> {
        match &table.source {
            // Lock-free: (db_id, table_id) live in the plan node, populated
            // when the StarRocks table was registered via
            // `InMemoryCatalog::register_starrocks_table`. We do NOT take
            // `state.starrocks_table.read()` here — every Scan column of every
            // SELECT calls this method, and that lock is contended with
            // INSERT / DROP DATABASE writers under parallel sql-tests.
            ScanSource::StarRocks { db_id, table_id } => {
                Ok(Some(DictionaryOwner::StarRocksTable {
                    database: database.to_string(),
                    table: table.name.clone(),
                    db_id: *db_id,
                    table_id: *table_id,
                }))
            }
            ScanSource::IcebergDataFiles { table: info, .. } => {
                Ok(Some(DictionaryOwner::IcebergTable {
                    catalog: info.catalog.clone(),
                    namespace: info.namespace.clone(),
                    table: info.table.clone(),
                    table_uuid: info.table_uuid.clone(),
                }))
            }
            // Metadata tables and IVM delta scans never participate in
            // dictionary rewriting.
            ScanSource::IcebergMetadataTable { .. } | ScanSource::IcebergDeltaTable { .. } => {
                Ok(None)
            }
        }
    }
```

- [ ] **Step 4.2: Run the TDD anchor test and confirm it now passes**

Run:
```bash
cargo test --lib engine::dictionary::tests::dictionary_provider_owner_for_starrocks_reads_identity_from_plan_node 2>&1 | tail -5
```

Expected: `test ... ok`. This proves owner_for resolves identity from the plan node without touching the (empty) catalog lock.

- [ ] **Step 4.3: Run the full dictionary tests module**

Run:
```bash
cargo test --lib engine::dictionary:: 2>&1 | tail -15
```

Expected: all existing dictionary tests still pass; the new test passes.

- [ ] **Step 4.4: Run the broader lib tests**

Run:
```bash
cargo test --lib 2>&1 | tail -10
```

Expected: no regressions. Pass count must be ≥ the baseline from Step 2.9 + 1 (the new test now passes).

- [ ] **Step 4.5: Commit the lock-free rewrite**

```bash
git add src/engine/dictionary/mod.rs
git commit -m "$(cat <<'EOF'
fix(dictionary): owner_for reads StarRocks identity from plan node, not catalog

DictionaryQueryProvider::owner_for previously took state.starrocks_table.read()
on every Scan column of every SELECT to pull (db_id, table_id) for the dict
owner key. With std::sync::RwLock's FIFO queue and INSERT/DROP DATABASE
writers holding the same lock across SQLite txn + S3 IO, parallel sql-tests
collapse to zero throughput within minutes (every SELECT scan queues new
read requests behind a slow writer, readers stop coalescing, query_timeout
fires at 120s without server-side cancellation).

(db_id, table_id) is already plan-time data: the prior commit added the
fields to ScanSource::StarRocks, populated from runtime.table.db_id/table_id
at the production constructor (starrocks_table_def).

Read directly from the plan node; drop the read lock. Mirrors how the
Iceberg arm of owner_for already reads identity from
ScanSource::IcebergDataFiles { table: info, .. }.

Anchor test (added in the prior commit) now passes:
  cargo test --lib dictionary_provider_owner_for_starrocks_reads_identity_from_plan_node

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

## Task 5: Add `starrocks_table_def` unit test (constructor contract)

A focused unit test in the catalog module proving that `starrocks_table_def` faithfully copies `runtime.table.db_id` / `runtime.table.table_id` into `TableDef::source`. Defense-in-depth in case a future refactor forgets the wiring.

**Files:**
- Modify: `src/connector/starrocks/table/catalog.rs` (append to existing `#[cfg(test)] mod tests` block, near the existing `matches!` assertion around line 983)

- [ ] **Step 5.1: Locate the existing tests module**

Run:
```bash
grep -n "#\[cfg(test)\]" src/connector/starrocks/table/catalog.rs | head -3
grep -n "matches!(table.source, ScanSource::StarRocks" src/connector/starrocks/table/catalog.rs
```

Note the line of `mod tests` and the helper fns it already exposes (look for `fn ...runtime()` or similar). The test will need to construct a minimal `StarRocksTableRuntime`; if a helper exists, reuse it.

- [ ] **Step 5.2: Add the constructor unit test**

Append to the `tests` module in `src/connector/starrocks/table/catalog.rs` (next to or below the existing `matches!(table.source, ScanSource::StarRocks { .. })` assertion):

```rust
    /// `starrocks_table_def` must populate ScanSource::StarRocks { db_id, table_id }
    /// from the runtime's identity fields. The dict-rewrite hot path
    /// (`DictionaryQueryProvider::owner_for`) reads these values directly to
    /// avoid taking state.starrocks_table.read() on every Scan column.
    #[test]
    fn starrocks_table_def_carries_runtime_ids_in_scan_source() {
        // Build a minimal-but-valid runtime with distinguishable IDs. The
        // helper used elsewhere in this module is the simplest path; if no
        // such helper exists, construct the runtime inline by mirroring
        // `rebuild_from_repository`'s output shape (see fixture setup near
        // line 829 — `register_starrocks_tables_in_catalog_populates_logical_table_and_layout`).
        let runtime = sample_runtime_with_ids(/* db_id */ 12_345, /* table_id */ 67_890);

        let table = super::starrocks_table_def(&runtime)
            .expect("starrocks_table_def must succeed for the sample runtime");

        match table.source {
            ScanSource::StarRocks { db_id, table_id } => {
                assert_eq!(db_id, 12_345, "db_id must come from runtime.table.db_id");
                assert_eq!(table_id, 67_890, "table_id must come from runtime.table.table_id");
            }
            other => panic!("expected ScanSource::StarRocks, got {other:?}"),
        }
    }
```

If `sample_runtime_with_ids` does not exist in the module, add a private test helper above the test that builds a minimal valid `StarRocksTableRuntime` (mirror the fixture at `register_starrocks_tables_in_catalog_populates_logical_table_and_layout`). Inline it as a `fn sample_runtime_with_ids(db_id: i64, table_id: i64) -> StarRocksTableRuntime`. The runtime must populate `runtime.table.db_id`, `runtime.table.table_id`, plus a non-empty `tablet_schema` and at least one visible column so `starrocks_table_def` produces a non-empty column list. If unsure of the minimal shape, copy from the existing fixture and parameterise the IDs.

- [ ] **Step 5.3: Run the new test**

Run:
```bash
cargo test --lib connector::starrocks::table::catalog::tests::starrocks_table_def_carries_runtime_ids_in_scan_source 2>&1 | tail -5
```

Expected: `test ... ok`.

- [ ] **Step 5.4: Run the broader lib tests one more time**

Run:
```bash
cargo test --lib 2>&1 | tail -10
```

Expected: no regressions.

- [ ] **Step 5.5: Commit the constructor unit test**

```bash
git add src/connector/starrocks/table/catalog.rs
git commit -m "$(cat <<'EOF'
test(catalog): starrocks_table_def carries runtime (db_id, table_id) into ScanSource

Focused unit test guarding the main production constructor's wiring of
identity into ScanSource::StarRocks { db_id, table_id }. The downstream
debug_assert in InMemoryCatalog::register_starrocks_table catches drift
at registration time; this test catches drift at construction time.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

## Task 6: Regression gate — iceberg-ivm SQL suite

The Iceberg path is untouched by this change, but the dict subsystem path changed. Iceberg-ivm exercises both. Must stay 61/61.

**Files:** (no source changes — verification only)

- [ ] **Step 6.1: Build the binary for the SQL suite to drive**

Run:
```bash
cargo build --bin novarocks 2>&1 | tail -3
```

Expected: clean build.

- [ ] **Step 6.2: Start standalone-server in background, wait for ready marker**

Run:
```bash
source docker/iceberg-rest/runtime/current/env.sh
rm -f /tmp/novarocks-iceberg-ivm.log
NO_PROXY=127.0.0.1,localhost target/debug/novarocks standalone-server \
  --config "$NOVAROCKS_STANDALONE_CONFIG" > /tmp/novarocks-iceberg-ivm.log 2>&1 &
echo $! > /tmp/novarocks-iceberg-ivm.pid
SRV_PID=$(cat /tmp/novarocks-iceberg-ivm.pid)
for i in $(seq 1 60); do
  if grep -q '^NOVAROCKS_READY ' /tmp/novarocks-iceberg-ivm.log; then
    echo "ready after ${i}s pid=$SRV_PID"
    grep '^NOVAROCKS_READY ' /tmp/novarocks-iceberg-ivm.log
    break
  fi
  if ! kill -0 "$SRV_PID" 2>/dev/null; then
    echo "DIED:"; tail -30 /tmp/novarocks-iceberg-ivm.log
    exit 1
  fi
  sleep 1
done
```

Expected: `NOVAROCKS_READY mysql_port=9128 pid=<pid>`.

- [ ] **Step 6.3: Run iceberg-ivm verify**

Run:
```bash
source docker/iceberg-rest/runtime/current/env.sh
docker/iceberg-rest/up.sh
cargo run --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests -- \
  --config "$NOVAROCKS_SQL_TEST_CONFIG" \
  --suite iceberg-ivm --mode verify 2>&1 | tail -30
```

Expected: final progress line reads `progress: pass=61, fail=0, total=61`. Suite wall_time is in the same ballpark as recent runs (no major slowdown).

- [ ] **Step 6.4: Kill the server**

Run:
```bash
SRV_PID=$(cat /tmp/novarocks-iceberg-ivm.pid)
kill -9 $SRV_PID 2>/dev/null
rm -f /tmp/novarocks-iceberg-ivm.pid
```

If the suite failed in 6.3, do NOT proceed — investigate the regression first. Diff the test output against the most recent known-passing baseline; likely culprits are a missed match-site update from Task 2 or a fixture with mismatched IDs tripping the debug_assert (Task 3) in a code path that previously ran fine.

---

## Task 7: Acceptance gate — filter suite at default parallelism

The acceptance criterion: filter goes from `fail=10/15` (with 120s timeouts) to `15/15 PASS` at default parallelism.

**Files:** (no source changes — verification only)

- [ ] **Step 7.1: Restart standalone-server**

Run:
```bash
source docker/iceberg-rest/runtime/current/env.sh
rm -f /tmp/novarocks-filter-parallel.log
NO_PROXY=127.0.0.1,localhost target/debug/novarocks standalone-server \
  --config "$NOVAROCKS_STANDALONE_CONFIG" > /tmp/novarocks-filter-parallel.log 2>&1 &
echo $! > /tmp/novarocks-filter-parallel.pid
SRV_PID=$(cat /tmp/novarocks-filter-parallel.pid)
for i in $(seq 1 60); do
  if grep -q '^NOVAROCKS_READY ' /tmp/novarocks-filter-parallel.log; then
    echo "ready after ${i}s pid=$SRV_PID"; break
  fi
  if ! kill -0 "$SRV_PID" 2>/dev/null; then
    echo "DIED:"; tail -30 /tmp/novarocks-filter-parallel.log
    exit 1
  fi
  sleep 1
done
```

Expected: `ready after Ns`.

- [ ] **Step 7.2: Run filter at default parallelism**

Run:
```bash
source docker/iceberg-rest/runtime/current/env.sh
cargo run --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests -- \
  --config "$NOVAROCKS_SQL_TEST_CONFIG" \
  --suite filter --mode verify 2>&1 | tail -20
```

Expected: final progress line reads `progress: pass=15, fail=0, total=15`. Wall time is significantly higher than `-j 1` (because INSERT/DROP writer serialization remains), but the suite **does not hang** and no case hits the 120s timeout.

If a case still times out, the deadlock is NOT purely caused by the dict hot reader; a follow-up investigation is required (see "Follow-up" section at the end of this plan). Capture the server-side sample with `sample <pid> 5 -mayDie` while the hang is in progress.

- [ ] **Step 7.3: Kill the server**

Run:
```bash
SRV_PID=$(cat /tmp/novarocks-filter-parallel.pid)
kill -9 $SRV_PID 2>/dev/null
rm -f /tmp/novarocks-filter-parallel.pid
```

---

## Task 8: No-regression gate — filter suite at -j 1 (sequential perf)

`-j 1` was the workaround we used before the fix; it ran 15/15 in 3.4s. The fix must not regress this number.

**Files:** (no source changes — verification only)

- [ ] **Step 8.1: Restart standalone-server**

Run:
```bash
source docker/iceberg-rest/runtime/current/env.sh
rm -f /tmp/novarocks-filter-j1.log
NO_PROXY=127.0.0.1,localhost target/debug/novarocks standalone-server \
  --config "$NOVAROCKS_STANDALONE_CONFIG" > /tmp/novarocks-filter-j1.log 2>&1 &
echo $! > /tmp/novarocks-filter-j1.pid
SRV_PID=$(cat /tmp/novarocks-filter-j1.pid)
for i in $(seq 1 60); do
  if grep -q '^NOVAROCKS_READY ' /tmp/novarocks-filter-j1.log; then
    echo "ready after ${i}s pid=$SRV_PID"; break
  fi
  if ! kill -0 "$SRV_PID" 2>/dev/null; then tail -30 /tmp/novarocks-filter-j1.log; exit 1; fi
  sleep 1
done
```

- [ ] **Step 8.2: Run filter at `-j 1`**

Run:
```bash
source docker/iceberg-rest/runtime/current/env.sh
cargo run --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests -- \
  --config "$NOVAROCKS_SQL_TEST_CONFIG" \
  --suite filter --mode verify -j 1 2>&1 | tail -10
```

Expected: `progress: pass=15, fail=0, total=15`. Wall time ≤ 5s (baseline was 3.42s; allow some headroom for system jitter). If wall time exceeds 6s, investigate — the lock-free path should be slightly faster than the previous locked path, not slower.

- [ ] **Step 8.3: Kill the server**

Run:
```bash
SRV_PID=$(cat /tmp/novarocks-filter-j1.pid)
kill -9 $SRV_PID 2>/dev/null
rm -f /tmp/novarocks-filter-j1.pid
```

---

## Task 9: Final clean-up and PR notes

**Files:** (no source changes — repo hygiene only)

- [ ] **Step 9.1: Confirm git state is clean**

Run:
```bash
git status
git log --oneline main..HEAD
```

Expected: working tree clean. The branch should have, in order, the prior spec commit plus these new commits:
- `refactor(catalog): ScanSource::StarRocks carries (db_id, table_id)` (Task 2)
- `fix(catalog): guard ScanSource::StarRocks vs PhysicalTableLayout identity` (Task 3)
- `fix(dictionary): owner_for reads StarRocks identity from plan node, not catalog` (Task 4)
- `test(catalog): starrocks_table_def carries runtime (db_id, table_id) into ScanSource` (Task 5)

- [ ] **Step 9.2: Run cargo fmt and cargo clippy**

Run:
```bash
cargo fmt
cargo clippy --lib 2>&1 | grep -E "warning:|error" | head -20
```

Expected: no new clippy errors; warnings are at or below the baseline (28 warnings per recent build).

If `cargo fmt` made changes, commit them as a tiny `chore(fmt)` commit:
```bash
git add -A
git diff --cached --quiet || git commit -m "chore(fmt): apply rustfmt"
```

- [ ] **Step 9.3: Final cargo test --lib pass**

Run:
```bash
cargo test --lib 2>&1 | tail -5
```

Expected: all lib tests pass. Pass count matches the baseline + 2 (the new TDD anchor + the new constructor unit test).

- [ ] **Step 9.4: Record final acceptance numbers**

Capture the numbers from Tasks 6, 7, 8 for the PR description:

- iceberg-ivm: 61/61 PASS, wall_time ≈ ___s
- filter parallel: 15/15 PASS, wall_time ≈ ___s
- filter -j 1: 15/15 PASS, wall_time ≈ ___s
- cargo test --lib: baseline + 2 pass

If any of these is off, do NOT mark the plan complete. Investigate and fix.

---

## Follow-up (NOT in scope of this plan)

If filter parallel still occasionally times out after Task 7 — for example one case in 20 runs — there is residual contention on `state.catalog` (INSERT path's `commit_catalog_visible_version` takes `state.catalog.write()`). The dict hot reader removal eliminates the dominant pressure, but `state.catalog` writer serialization remains. Address separately:

- Shrink the INSERT writer critical section so it does not hold `state.starrocks_table.write()` across SQLite txn + S3 IO.
- Apply the same clone-then-release pattern documented in `src/engine/mod.rs:821-832` to any remaining hot readers on `state.starrocks_table` (`collect_namespace_owners`, `resolve_owner_from_target`, `mark_target_stale`).

Neither is required to pass the acceptance gate of this plan (filter parallel 15/15 PASS).
