# StarRocks Codegen via Connector `begin_scan`/`plan_splits` Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Let standalone StarRocks scan codegen call `ConnectorScanPlanner::begin_scan` / `plan_splits` from a registered planner to obtain the `ScanHandle` and splits, instead of bootstrapping a `PlannedConnectorScan` from `PhysicalTableLayout` inside `visit_scan`.

**Architecture:** `PlanFragmentBuilder::build` gains a `&ConnectorRegistry` parameter. `execute_query` / `execute_query_with_options` thread the registry from each `Arc<StandaloneState>` caller (clone-then-release snapshot, same as the existing `&InMemoryCatalog` snapshot pattern). `visit_scan` reads the StarRocks planner from the registry and replaces the layout-derived bootstrap with real connector calls. After this slice, `ResolvedTable.physical_layout` is deleted, the `plan_starrocks_connector_scan` helper is deleted, and `build_exec_params_multi` collapses to a 2-arm chain (StarRocks-via-planned-scan + Iceberg `ScanSource` match). The follow-up slice (Stage 5 cleanup) removes `CatalogProvider::get_physical_layout` and `InMemoryCatalog.physical_layouts` once no caller remains.

**Tech Stack:** Rust, existing `ConnectorRegistry` / `ConnectorScanPlanner` from the previous slice, existing standalone SQL fragment builder, `cargo test --lib`.

---

## Scope Check

The parent spec (`docs/superpowers/specs/2026-05-28-connector-first-standalone-scan-design.md`) covers five migration stages. This plan completes Stage 2:

- StarRocks codegen calls `connector.begin_scan` and `plan_splits` rather than reading `PhysicalTableLayout`.
- `ResolvedTable.physical_layout` is removed.
- `nodes.rs::build_internal_scan_range_params` and the `physical_layout` bridge arm are removed.

This plan does NOT cover:

- Deleting `CatalogProvider::get_physical_layout` (Stage 5).
- Deleting `InMemoryCatalog.physical_layouts` field and its population (Stage 5).
- Routing `to_thrift_scan` through the registry — codegen still uses `StarRocksTableScanPlanner::stateless_for_codegen()` for thrift conversion (follow-up).
- Iceberg migration (Stage 3).
- Optimizer capabilities (Stage 4).

## File Structure

- Modify: `src/sql/codegen/fragment_builder.rs`
  - Add `connectors: &'a ConnectorRegistry` field on `PlanFragmentBuilder<'a>`.
  - Add `connectors: &'a ConnectorRegistry` parameter on `PlanFragmentBuilder::build`.
  - In `visit_scan`, replace the `catalog.get_physical_layout` + `plan_starrocks_connector_scan` bootstrap with `self.connectors.scan_planner("starrocks")?.begin_scan/plan_splits`.
  - Compute `scan_table_id` from `ScanSource::StarRocks.table_id` directly; keep the existing synthetic-id branch for Iceberg unchanged.
  - Delete the local `plan_starrocks_connector_scan` helper.
  - In `tests`, introduce `MockScanPlanner` + `mock_starrocks_registry(layout)` helper; update every `PlanFragmentBuilder::build` test call to pass a registry (`&ConnectorRegistry::new()` for non-StarRocks tests; `mock_starrocks_registry(&layout)` for StarRocks tests).

- Modify: `src/sql/codegen/resolve.rs`
  - Delete `ResolvedTable.physical_layout: Option<PhysicalTableLayout>` field.
  - Delete the now-unused `use crate::sql::catalog::{PhysicalTableLayout, ...};` import (if `PhysicalTableLayout` becomes unused).

- Modify: `src/sql/codegen/nodes.rs`
  - Update the two test `ResolvedTable` initializers (`physical_change_op_column_does_not_emit_extended_columns` and `metadata_change_op_column_emits_extended_columns`) to drop `physical_layout: None`.
  - Delete the private `build_internal_scan_range_params(resolved, layout, tablet)` function (no caller after the bridge arm goes away).
  - Simplify `build_exec_params_multi` to a 2-arm chain (StarRocks via planned scan | Iceberg `ScanSource` match).

- Modify: `src/engine/mod.rs`
  - Add `connectors: &ConnectorRegistry` to `execute_query` and `execute_query_with_options` signatures.
  - Snapshot connectors at every internal call site in `execute_in_context_inner` and `explain_analyze_query` (clone-then-release, matching the catalog snapshot pattern).
  - Update the unit test at `engine/mod.rs:4107` to pass `&ConnectorRegistry::new()`.
  - Update the `PlanFragmentBuilder::build` call inside `execute_query_with_options` to pass the connectors through.

- Modify: External `execute_query` / `execute_query_with_options` callers (mechanical pass-through):
  - `src/engine/iceberg_writer.rs`
  - `src/engine/insert_flow.rs`
  - `src/engine/mutation_flow.rs`
  - `src/engine/delete_flow.rs`
  - `src/engine/mv_flow.rs`
  - `src/engine/statistics.rs`
  - `src/engine/dictionary/rebuild.rs`
  - `src/engine/mv/iceberg_refresh.rs`

Each caller already has `state: &Arc<StandaloneState>` in scope and uses `state.catalog.read().clone()` snapshot. They will add a parallel `state.connectors.read().clone()` snapshot.

## Task 1: Plumb `&ConnectorRegistry` through `PlanFragmentBuilder::build`

**Files:**
- Modify: `src/sql/codegen/fragment_builder.rs`
- Modify: `src/engine/mod.rs`

Behavior unchanged. The new parameter is held on the builder and not yet consumed by `visit_scan`. All `PlanFragmentBuilder::build` test sites pass `&ConnectorRegistry::new()`.

- [ ] **Step 1: Add the field to `PlanFragmentBuilder`**

In `src/sql/codegen/fragment_builder.rs`, locate the struct definition (~line 199):

```rust
pub(crate) struct PlanFragmentBuilder<'a> {
    catalog: &'a dyn CatalogProvider,
    desc_builder: DescriptorTableBuilder,
    ...
}
```

Add a new field after `catalog`:

```rust
    connectors: &'a crate::connector::ConnectorRegistry,
```

- [ ] **Step 2: Add the parameter to `PlanFragmentBuilder::build`**

In the same file (~line 250), change:

```rust
pub(crate) fn build(
    plan: &PhysicalPlanNode,
    catalog: &'a dyn CatalogProvider,
    _current_database: &str,
) -> Result<MultiFragmentBuildResult, String> {
```

to:

```rust
pub(crate) fn build(
    plan: &PhysicalPlanNode,
    catalog: &'a dyn CatalogProvider,
    connectors: &'a crate::connector::ConnectorRegistry,
    _current_database: &str,
) -> Result<MultiFragmentBuildResult, String> {
```

In the same function, find the `PlanFragmentBuilder { catalog, desc_builder, ... }` initializer and add `connectors` next to `catalog`:

```rust
    let mut builder = PlanFragmentBuilder {
        catalog,
        connectors,
        desc_builder: DescriptorTableBuilder::new(),
        ...
```

- [ ] **Step 3: Update the production caller in `execute_query_with_options`**

In `src/engine/mod.rs` (~line 2628), change:

```rust
let build_result = crate::sql::codegen::fragment_builder::PlanFragmentBuilder::build(
    &physical,
    catalog,
    current_database,
)?;
```

to:

```rust
let build_result = crate::sql::codegen::fragment_builder::PlanFragmentBuilder::build(
    &physical,
    catalog,
    connectors,
    current_database,
)?;
```

(`connectors` is the new `&ConnectorRegistry` parameter that Step 4 adds.)

- [ ] **Step 4: Add `connectors` to `execute_query` and `execute_query_with_options`**

In `src/engine/mod.rs`, change `execute_query` (~line 2576):

```rust
pub(crate) fn execute_query(
    query: &sqlparser::ast::Query,
    catalog: &InMemoryCatalog,
    current_database: &str,
    exchange_port: u16,
    query_opts: Option<crate::internal_service::TQueryOptions>,
) -> Result<QueryResult, String> {
    execute_query_with_options(
        query,
        catalog,
        current_database,
        exchange_port,
        query_opts,
        None,
        None,
    )
}
```

to:

```rust
pub(crate) fn execute_query(
    query: &sqlparser::ast::Query,
    catalog: &InMemoryCatalog,
    connectors: &crate::connector::ConnectorRegistry,
    current_database: &str,
    exchange_port: u16,
    query_opts: Option<crate::internal_service::TQueryOptions>,
) -> Result<QueryResult, String> {
    execute_query_with_options(
        query,
        catalog,
        connectors,
        current_database,
        exchange_port,
        query_opts,
        None,
        None,
    )
}
```

Change `execute_query_with_options` (~line 2603):

```rust
pub(crate) fn execute_query_with_options(
    query: &sqlparser::ast::Query,
    catalog: &InMemoryCatalog,
    current_database: &str,
    ...
```

to:

```rust
pub(crate) fn execute_query_with_options(
    query: &sqlparser::ast::Query,
    catalog: &InMemoryCatalog,
    connectors: &crate::connector::ConnectorRegistry,
    current_database: &str,
    ...
```

- [ ] **Step 5: Update internal `execute_query` callers in `engine/mod.rs`**

There are four internal call sites in `engine/mod.rs`:

- Line ~834 (time-travel branch)
- Line ~884 (three-part-name branch)
- Line ~904 (default SELECT branch)
- Line ~2533 (inside `explain_analyze_query`)

For each, add a `connectors_snapshot` next to the existing `catalog_snapshot` and pass it as the new argument. Example (line 834-841 area):

```rust
let catalog_snapshot = self
    .inner
    .catalog
    .read()
    .expect("standalone catalog read lock")
    .clone();
let connectors_snapshot = self
    .inner
    .connectors
    .read()
    .expect("standalone connector registry read lock")
    .clone();
self::statistics::observe_query(&self.inner, &rewritten, current_database)?;
let result = execute_query(
    &rewritten,
    &catalog_snapshot,
    &connectors_snapshot,
    current_database,
    self.inner.exchange_port,
    query_opts.clone(),
)?;
```

Repeat for the three other internal call sites. For the `explain_analyze_query` call (~line 2533) the `catalog` parameter is already passed in; add a `connectors: &ConnectorRegistry` parameter to `explain_analyze_query`'s signature and thread it from its caller (`execute_in_context_inner` is the entry point — find the `explain_analyze_query(...)` call site and add the connectors snapshot there as well).

- [ ] **Step 6: Update the test at `engine/mod.rs:4107`**

The unit test (`sqlparser_insert_values_preserves_array_literals`) constructs its own catalog locally:

```rust
crate::sql::codegen::fragment_builder::PlanFragmentBuilder::build(
    &physical, &catalog, "default",
)
```

Change to:

```rust
crate::sql::codegen::fragment_builder::PlanFragmentBuilder::build(
    &physical,
    &catalog,
    &crate::connector::ConnectorRegistry::new(),
    "default",
)
```

- [ ] **Step 7: Update every `PlanFragmentBuilder::build` site in `fragment_builder.rs` tests**

In `src/sql/codegen/fragment_builder.rs` test module, every `PlanFragmentBuilder::build(&plan, &<catalog>, "default")` call must pass an empty registry. Replace each with:

```rust
PlanFragmentBuilder::build(&plan, &<catalog>, &crate::connector::ConnectorRegistry::new(), "default")
```

Apply to every call site in the test module. Expected count: ~14 (use `grep -n "PlanFragmentBuilder::build" src/sql/codegen/fragment_builder.rs` to enumerate).

- [ ] **Step 8: Build**

Run:

```bash
cargo build
```

Expected: build succeeds. Warnings about the unused `connectors` field on `PlanFragmentBuilder` are acceptable for now (Task 4 consumes it).

- [ ] **Step 9: Run focused tests to confirm no regression**

Run:

```bash
cargo test --lib sql::codegen::fragment_builder::tests
```

Expected: all existing tests pass (the registry parameter is plumbed but `visit_scan` does not yet consult it).

- [ ] **Step 10: Commit**

```bash
git add src/sql/codegen/fragment_builder.rs src/engine/mod.rs
git commit -m "refactor(codegen): thread connector registry into PlanFragmentBuilder"
```

## Task 2: Update external `execute_query` callers

**Files:**
- Modify: `src/engine/iceberg_writer.rs`
- Modify: `src/engine/insert_flow.rs`
- Modify: `src/engine/mutation_flow.rs`
- Modify: `src/engine/delete_flow.rs`
- Modify: `src/engine/mv_flow.rs`
- Modify: `src/engine/statistics.rs`
- Modify: `src/engine/dictionary/rebuild.rs`
- Modify: `src/engine/mv/iceberg_refresh.rs`

All callers already have `state: &Arc<StandaloneState>` in scope. Each adds a parallel connectors snapshot.

- [ ] **Step 1: Enumerate every call site**

Run:

```bash
grep -rn "crate::engine::execute_query\|crate::engine::execute_query_with_options" src/engine/ | grep -v "src/engine/mod.rs"
```

Expected list (from current tree):

- `src/engine/iceberg_writer.rs:366`
- `src/engine/iceberg_writer.rs:416`
- `src/engine/insert_flow.rs:199`
- `src/engine/mutation_flow.rs:570`
- `src/engine/mutation_flow.rs:1661`
- `src/engine/delete_flow.rs:543`
- `src/engine/mv_flow.rs:759`
- `src/engine/statistics.rs:1083`
- `src/engine/dictionary/rebuild.rs:225`
- `src/engine/mv/iceberg_refresh.rs:2713`
- `src/engine/mv/iceberg_refresh.rs:4243`
- `src/engine/mv/iceberg_refresh.rs:5866`
- `src/engine/mv/iceberg_refresh.rs:6594`

- [ ] **Step 2: Update each call site**

For each call site, add a `connectors_snapshot` adjacent to the existing `catalog_snapshot` (or to the `state.catalog.read().clone()` expression already used inline) and pass it as the new argument to `execute_query` / `execute_query_with_options`. Pattern:

Before:

```rust
let catalog_snapshot = state
    .catalog
    .read()
    .expect("standalone catalog read lock")
    .clone();
let result = crate::engine::execute_query(
    &query,
    &catalog_snapshot,
    current_database,
    state.exchange_port,
    query_opts,
)?;
```

After:

```rust
let catalog_snapshot = state
    .catalog
    .read()
    .expect("standalone catalog read lock")
    .clone();
let connectors_snapshot = state
    .connectors
    .read()
    .expect("standalone connector registry read lock")
    .clone();
let result = crate::engine::execute_query(
    &query,
    &catalog_snapshot,
    &connectors_snapshot,
    current_database,
    state.exchange_port,
    query_opts,
)?;
```

Apply the same pattern to every call site listed in Step 1. Some sites may pass `&catalog` from a parameter rather than locking — in those cases, also add a `connectors: &ConnectorRegistry` parameter and have the caller thread it through.

- [ ] **Step 3: Build**

```bash
cargo build
```

Expected: build succeeds.

- [ ] **Step 4: Run lib tests**

```bash
cargo test --lib
```

Expected: all tests pass.

- [ ] **Step 5: Commit**

```bash
git add -A
git commit -m "refactor(engine): pass connector registry to execute_query callers"
```

## Task 3: Introduce `MockScanPlanner` and `mock_starrocks_registry` helper

**Files:**
- Modify: `src/sql/codegen/fragment_builder.rs`

Add the test infrastructure that StarRocks-using tests will rely on after Task 4 wires `visit_scan` to consume the registry.

- [ ] **Step 1: Add the `MockScanPlanner` test struct**

In `src/sql/codegen/fragment_builder.rs`, inside the existing `#[cfg(test)] mod tests` (near `StarRocksCatalog` and `MixedCatalog` declarations, ~line 4018), add:

```rust
#[derive(Debug)]
struct MockScanPlanner {
    schema_id: i64,
    splits: Vec<crate::connector::starrocks::table::StarRocksSplit>,
}

impl crate::connector::scan_planning::ConnectorScanPlanner for MockScanPlanner {
    fn name(&self) -> &'static str {
        "starrocks"
    }

    fn begin_scan(
        &self,
        table: crate::connector::scan_planning::TableHandle,
        _ctx: crate::connector::scan_planning::BeginScanContext,
    ) -> Result<crate::connector::scan_planning::ScanHandle, String> {
        let inner = table
            .downcast_ref::<crate::connector::starrocks::table::StarRocksTableHandle>()
            .ok_or_else(|| "MockScanPlanner expected StarRocksTableHandle".to_string())?
            .clone();
        Ok(crate::connector::scan_planning::ScanHandle::new(
            "starrocks",
            crate::connector::starrocks::table::StarRocksScanHandle {
                table: inner,
                schema_id: self.schema_id,
            },
        ))
    }

    fn plan_splits(
        &self,
        _scan: &crate::connector::scan_planning::ScanHandle,
        _ctx: crate::connector::scan_planning::SplitPlanningContext,
    ) -> Result<Vec<crate::connector::scan_planning::Split>, String> {
        Ok(self
            .splits
            .iter()
            .map(|split| {
                crate::connector::scan_planning::Split::new("starrocks", split.clone())
            })
            .collect())
    }

    fn to_thrift_scan(
        &self,
        _scan: &crate::connector::scan_planning::ScanHandle,
        _splits: &[crate::connector::scan_planning::Split],
        _ctx: crate::connector::scan_planning::ThriftScanContext,
    ) -> Result<crate::connector::scan_planning::ThriftScanPlan, String> {
        Err("MockScanPlanner::to_thrift_scan is not exercised by tests".to_string())
    }
}
```

`to_thrift_scan` returns an error because codegen calls `StarRocksTableScanPlanner::stateless_for_codegen()` for that step, not the registered planner.

- [ ] **Step 2: Add the helper functions**

Right after `MockScanPlanner`, add:

```rust
fn mock_starrocks_registry(
    layout: &crate::sql::catalog::PhysicalTableLayout,
) -> crate::connector::ConnectorRegistry {
    use crate::connector::starrocks::table::StarRocksSplit;
    let splits = layout
        .tablets
        .iter()
        .map(|tablet| StarRocksSplit {
            tablet_id: tablet.tablet_id,
            partition_id: tablet.partition_id,
            version: tablet.version,
        })
        .collect();
    let planner = std::sync::Arc::new(MockScanPlanner {
        schema_id: layout.schema_id,
        splits,
    });
    let mut registry = crate::connector::ConnectorRegistry::new();
    registry.register_scan_planner(planner);
    registry
}
```

- [ ] **Step 3: Update StarRocks-using tests to pass the mock registry**

Find every test that uses `StarRocksCatalog` or `MixedCatalog` (e.g. `physical_decode_emits_decode_node` at ~line 5247, `scan_emits_single_slot_per_dict_column`, the mixed-join test, the previous-slice `starrocks_fragment_exec_params_are_generated_from_planned_connector_scan` test, etc.). For each, replace:

```rust
let build = PlanFragmentBuilder::build(&plan, &catalog, &crate::connector::ConnectorRegistry::new(), "default")
    .expect("...");
```

with:

```rust
let registry = mock_starrocks_registry(&layout);
let build = PlanFragmentBuilder::build(&plan, &catalog, &registry, "default")
    .expect("...");
```

Where `layout` is the same `PhysicalTableLayout` value used by `StarRocksCatalog { layout }` / `MixedCatalog { starrocks_layout }` (often produced by the existing `starrocks_layout()` helper).

For the `MixedCatalog` join test (StarRocks + Iceberg), build the registry from the StarRocks layout only — Iceberg scans don't yet need a registered planner in this slice.

DummyCatalog tests (no StarRocks scan) continue to pass `&crate::connector::ConnectorRegistry::new()`.

- [ ] **Step 4: Run the StarRocks fragment_builder tests**

```bash
cargo test --lib sql::codegen::fragment_builder::tests
```

Expected: all tests still pass. The registry is registered but `visit_scan` still uses the bootstrap path (Task 4 switches it over).

- [ ] **Step 5: Commit**

```bash
git add src/sql/codegen/fragment_builder.rs
git commit -m "test(codegen): add MockScanPlanner for StarRocks fragment_builder tests"
```

## Task 4: Switch `visit_scan` to the registry and remove `ResolvedTable.physical_layout`

**Files:**
- Modify: `src/sql/codegen/fragment_builder.rs`
- Modify: `src/sql/codegen/resolve.rs`
- Modify: `src/sql/codegen/nodes.rs`

This task is atomic: deleting only some of the pieces below would leave the tree in a state that does not compile (e.g. `ResolvedTable` initializer missing a field, or referencing a deleted local). Apply all edits before running `cargo build`.

- [ ] **Step 1: Replace the layout/identity block at the top of `visit_scan`**

In `src/sql/codegen/fragment_builder.rs`, `visit_scan` (~line 459) currently starts:

```rust
fn visit_scan(
    &mut self,
    op: &PhysicalScanOp,
    _node: &PhysicalPlanNode,
) -> Result<VisitResult, String> {
    let scan_tuple_id = self.alloc_tuple();
    let scan_node_id = self.alloc_node();

    let mut scope = ExprScope::new();
    ...
    let physical_layout = self
        .catalog
        .get_physical_layout(&op.database, &op.table.name)?;
    let scan_table_id = physical_layout
        .as_ref()
        .map(|layout| layout.table_id)
        .or_else(|| {
            iceberg_table_info(&op.table.source)
                .is_some()
                .then_some(synthetic_iceberg_table_id(scan_node_id))
        });
```

Delete the `let physical_layout = self.catalog.get_physical_layout(...)` block and the layout-based `scan_table_id` derivation. Replace with:

```rust
    let planned_scan = match &op.table.source {
        crate::sql::catalog::ScanSource::StarRocks { db_id, table_id } => {
            let planner = self.connectors.scan_planner("starrocks")?;
            let table_handle =
                crate::connector::starrocks::table::StarRocksTableScanPlanner::table_handle_from_source(
                    &op.database,
                    &op.table.name,
                    *db_id,
                    *table_id,
                );
            let scan = planner.begin_scan(
                table_handle,
                crate::connector::scan_planning::BeginScanContext::default(),
            )?;
            let splits = planner.plan_splits(
                &scan,
                crate::connector::scan_planning::SplitPlanningContext::default(),
            )?;
            Some(crate::sql::codegen::resolve::PlannedConnectorScan { scan, splits })
        }
        _ => None,
    };
    let scan_table_id = match &op.table.source {
        crate::sql::catalog::ScanSource::StarRocks { table_id, .. } => Some(*table_id),
        _ => iceberg_table_info(&op.table.source)
            .is_some()
            .then_some(synthetic_iceberg_table_id(scan_node_id)),
    };
```

- [ ] **Step 2: Delete the now-unused `plan_starrocks_connector_scan` helper**

Locate `fn plan_starrocks_connector_scan(...)` (placed near `iceberg_table_info`, ~line 99 area) and delete the entire function body. It is no longer referenced.

- [ ] **Step 3: Delete the second `planned_scan` block from `visit_scan`**

The previous slice added a second `let planned_scan = match (&op.table.source, physical_layout.as_ref()) { ... };` block placed just above the `ResolvedTable { ... }` initializer (~line 985 in current tree). Delete that block — Step 1 already computes `planned_scan` at the top.

- [ ] **Step 4: Update the `ResolvedTable` initializer in `visit_scan`**

The current initializer (~line 722) reads:

```rust
let resolved = ResolvedTable {
    database: op.database.clone(),
    table: op.table.clone(),
    physical_layout,
    planned_scan,
    alias: op.alias.clone(),
};
```

Change to:

```rust
let resolved = ResolvedTable {
    database: op.database.clone(),
    table: op.table.clone(),
    planned_scan,
    alias: op.alias.clone(),
};
```

- [ ] **Step 5: Delete the `physical_layout` field on `ResolvedTable`**

In `src/sql/codegen/resolve.rs`, change:

```rust
#[derive(Clone, Debug)]
pub(crate) struct ResolvedTable {
    #[allow(dead_code)]
    pub database: String,
    pub table: TableDef,
    pub physical_layout: Option<PhysicalTableLayout>,
    pub planned_scan: Option<PlannedConnectorScan>,
    #[allow(dead_code)]
    pub alias: Option<String>,
}
```

to:

```rust
#[derive(Clone, Debug)]
pub(crate) struct ResolvedTable {
    #[allow(dead_code)]
    pub database: String,
    pub table: TableDef,
    pub planned_scan: Option<PlannedConnectorScan>,
    #[allow(dead_code)]
    pub alias: Option<String>,
}
```

If the `use crate::sql::catalog::{PhysicalTableLayout, TableDef};` import line in `resolve.rs` becomes unused for `PhysicalTableLayout`, narrow it to `use crate::sql::catalog::TableDef;`. Let `cargo build` confirm.

- [ ] **Step 6: Delete `physical_layout: None,` from `nodes.rs` test initializers**

Three test initializers in `src/sql/codegen/nodes.rs` currently include `physical_layout: None,`:

1. `physical_change_op_column_does_not_emit_extended_columns` (~line 1328)
2. `metadata_change_op_column_emits_extended_columns` (~line 1384)
3. `starrocks_scan_ranges_use_planned_connector_scan_without_physical_layout` (added in the previous slice, ~line 1078)

Delete the `physical_layout: None,` line from each. Surrounding fields stay unchanged.

- [ ] **Step 7: Drop orphan imports**

After all deletions, `cargo build` may flag a few orphan imports in `fragment_builder.rs` (e.g. previously-needed but now fully-qualified-only usages of `BeginScanContext`, `SplitPlanningContext`, `TableHandle`, `StarRocksTableScanPlanner`). Remove the orphan `use` lines if any. Keep imports that remain in use.

- [ ] **Step 8: Build**

```bash
cargo build
```

Expected: build succeeds. A warning that `CatalogProvider::get_physical_layout` is now unused is acceptable — the trait method stays for Stage 5 cleanup.

- [ ] **Step 9: Run StarRocks fragment_builder tests**

```bash
cargo test --lib sql::codegen::fragment_builder::tests
```

Expected: all StarRocks tests pass via the MockScanPlanner registered in Task 3.

- [ ] **Step 10: Run full lib tests**

```bash
cargo test --lib
```

Expected: all tests pass.

- [ ] **Step 11: Commit**

```bash
git add src/sql/codegen/fragment_builder.rs src/sql/codegen/resolve.rs src/sql/codegen/nodes.rs
git commit -m "refactor(codegen): visit_scan obtains StarRocks splits via connector registry"
```

## Task 5: Collapse `build_exec_params_multi` to a 2-arm chain

**Files:**
- Modify: `src/sql/codegen/nodes.rs`

After Task 4, the `physical_layout` bridge arm in `build_exec_params_multi` is unreachable, and `build_internal_scan_range_params` has no callers.

- [ ] **Step 1: Delete the bridge `physical_layout` arm**

In `src/sql/codegen/nodes.rs::build_exec_params_multi` (~line 569), the current structure is:

```rust
let ranges = if matches!(
    resolved.table.source,
    crate::sql::catalog::ScanSource::StarRocks { .. }
) {
    let ranges = build_starrocks_scan_ranges_from_planned_scan(resolved)?;
    if ranges.is_empty() {
        return Err(format!(
            "StarRocks table {}.{} has no selected tablet splits",
            resolved.database, resolved.table.name
        ));
    }
    ranges
} else if let Some(layout) = resolved.physical_layout.as_ref() {
    // ... bridge arm ...
} else {
    match &resolved.table.source {
        ScanSource::IcebergDataFiles { files, .. } => { ... }
        ScanSource::IcebergMetadataTable { .. } => { ... }
        ScanSource::IcebergDeltaTable { .. } => { ... }
        ScanSource::StarRocks { .. } => unreachable!(...),
    }
};
```

After Task 4, `resolved.physical_layout` no longer exists, so the `else if let Some(layout) = resolved.physical_layout.as_ref()` arm fails to compile. Delete that entire arm. The result:

```rust
let ranges = if matches!(
    resolved.table.source,
    crate::sql::catalog::ScanSource::StarRocks { .. }
) {
    let ranges = build_starrocks_scan_ranges_from_planned_scan(resolved)?;
    if ranges.is_empty() {
        return Err(format!(
            "StarRocks table {}.{} has no selected tablet splits",
            resolved.database, resolved.table.name
        ));
    }
    ranges
} else {
    match &resolved.table.source {
        ScanSource::IcebergDataFiles { files, .. } => {
            let file_predicates = scan_file_min_max_predicates(planned);
            let change_op_slot = planned_change_op_slot(planned);
            let mut ranges = Vec::new();
            for file in files
                .iter()
                .filter(|f| file_may_satisfy_min_max(f, &file_predicates))
            {
                ranges.extend(build_hdfs_scan_range_params_for_file(file, change_op_slot)?);
            }
            ranges
        }
        ScanSource::IcebergMetadataTable { .. } => {
            vec![build_iceberg_metadata_scan_range_params()]
        }
        ScanSource::IcebergDeltaTable { .. } => {
            vec![build_iceberg_metadata_scan_range_params()]
        }
        ScanSource::StarRocks { .. } => unreachable!(
            "StarRocks scan source is handled by the planned-connector branch above"
        ),
    }
};
```

- [ ] **Step 2: Delete the private `build_internal_scan_range_params` helper**

In the same file (~line 1007), delete the entire function:

```rust
fn build_internal_scan_range_params(
    resolved: &ResolvedTable,
    layout: &crate::sql::catalog::PhysicalTableLayout,
    tablet: &crate::sql::catalog::StarRocksTabletRef,
) -> internal_service::TScanRangeParams {
    ...
}
```

If `crate::sql::catalog::PhysicalTableLayout` is no longer referenced anywhere in `nodes.rs` (the `ResolvedTable` field is gone, this function is gone), remove the import too. `cargo build` will tell you.

- [ ] **Step 3: Build**

```bash
cargo build
```

Expected: build succeeds.

- [ ] **Step 4: Run focused nodes tests**

```bash
cargo test --lib sql::codegen::nodes::tests
```

Expected: all pass.

- [ ] **Step 5: Run full lib tests**

```bash
cargo test --lib
```

Expected: all pass.

- [ ] **Step 6: Commit**

```bash
git add src/sql/codegen/nodes.rs
git commit -m "refactor(codegen): remove physical_layout bridge in build_exec_params_multi"
```

## Task 6: End-to-end guard that `begin_scan` and `plan_splits` are invoked

**Files:**
- Modify: `src/sql/codegen/fragment_builder.rs`

Add a test that fails if `visit_scan` regresses to bootstrapping splits without calling the registry. Use a counting `MockScanPlanner` variant or extend the existing mock to record calls.

- [ ] **Step 1: Add a counting wrapper around `MockScanPlanner`**

In `src/sql/codegen/fragment_builder.rs::tests` (near the existing `MockScanPlanner` declaration), add:

```rust
#[derive(Debug, Default)]
struct ScanPlannerCallCounts {
    begin_scan: std::sync::atomic::AtomicUsize,
    plan_splits: std::sync::atomic::AtomicUsize,
}

#[derive(Debug)]
struct CountingScanPlanner {
    inner: MockScanPlanner,
    counts: std::sync::Arc<ScanPlannerCallCounts>,
}

impl crate::connector::scan_planning::ConnectorScanPlanner for CountingScanPlanner {
    fn name(&self) -> &'static str {
        self.inner.name()
    }

    fn begin_scan(
        &self,
        table: crate::connector::scan_planning::TableHandle,
        ctx: crate::connector::scan_planning::BeginScanContext,
    ) -> Result<crate::connector::scan_planning::ScanHandle, String> {
        self.counts
            .begin_scan
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        self.inner.begin_scan(table, ctx)
    }

    fn plan_splits(
        &self,
        scan: &crate::connector::scan_planning::ScanHandle,
        ctx: crate::connector::scan_planning::SplitPlanningContext,
    ) -> Result<Vec<crate::connector::scan_planning::Split>, String> {
        self.counts
            .plan_splits
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        self.inner.plan_splits(scan, ctx)
    }

    fn to_thrift_scan(
        &self,
        scan: &crate::connector::scan_planning::ScanHandle,
        splits: &[crate::connector::scan_planning::Split],
        ctx: crate::connector::scan_planning::ThriftScanContext,
    ) -> Result<crate::connector::scan_planning::ThriftScanPlan, String> {
        self.inner.to_thrift_scan(scan, splits, ctx)
    }
}
```

- [ ] **Step 2: Add the test**

Place this test next to `starrocks_fragment_exec_params_are_generated_from_planned_connector_scan`:

```rust
#[test]
fn visit_scan_calls_connector_begin_scan_and_plan_splits_for_starrocks() {
    use crate::connector::starrocks::table::StarRocksSplit;
    let layout = starrocks_layout();
    let plan = starrocks_scan_plan();
    let catalog = StarRocksCatalog {
        layout: layout.clone(),
    };

    let splits: Vec<StarRocksSplit> = layout
        .tablets
        .iter()
        .map(|tablet| StarRocksSplit {
            tablet_id: tablet.tablet_id,
            partition_id: tablet.partition_id,
            version: tablet.version,
        })
        .collect();
    let counts = std::sync::Arc::new(ScanPlannerCallCounts::default());
    let planner = std::sync::Arc::new(CountingScanPlanner {
        inner: MockScanPlanner {
            schema_id: layout.schema_id,
            splits,
        },
        counts: counts.clone(),
    });
    let mut registry = crate::connector::ConnectorRegistry::new();
    registry.register_scan_planner(planner);

    let _ = PlanFragmentBuilder::build(&plan, &catalog, &registry, "default")
        .expect("build StarRocks fragment");

    assert_eq!(
        counts
            .begin_scan
            .load(std::sync::atomic::Ordering::SeqCst),
        1,
        "begin_scan must be invoked exactly once for the StarRocks scan"
    );
    assert_eq!(
        counts
            .plan_splits
            .load(std::sync::atomic::Ordering::SeqCst),
        1,
        "plan_splits must be invoked exactly once for the StarRocks scan"
    );
}
```

- [ ] **Step 3: Run the new test**

```bash
cargo test --lib sql::codegen::fragment_builder::tests::visit_scan_calls_connector_begin_scan_and_plan_splits_for_starrocks
```

Expected: passes.

- [ ] **Step 4: Commit**

```bash
git add src/sql/codegen/fragment_builder.rs
git commit -m "test(codegen): assert visit_scan invokes connector begin_scan/plan_splits"
```

## Task 7: Validation pass

**Files:**
- No source edits expected unless validation exposes a real bug.

- [ ] **Step 1: Formatting**

```bash
cargo fmt --check
```

Expected: no formatting diffs. If diffs appear, run `cargo fmt`, inspect the diff with `git diff`, and only commit changes that touch files modified by this plan. Out-of-scope drift (e.g. unrelated `src/sql/analyzer/*.rs`) must be reverted with `git checkout --` before committing.

- [ ] **Step 2: Build**

```bash
cargo build
```

Expected: build succeeds. New warnings are acceptable but report them.

- [ ] **Step 3: Run focused connector and codegen tests**

```bash
cargo test --lib connector::scan_planning connector::starrocks::table::scan_planner sql::codegen::fragment_builder::tests sql::codegen::nodes::tests
```

Expected: all pass.

- [ ] **Step 4: Run dictionary lock-free regression**

```bash
cargo test --lib engine::dictionary::tests::dictionary_provider_owner_for_starrocks_reads_identity_from_plan_node
```

Expected: passes.

- [ ] **Step 5: Run full lib tests**

```bash
cargo test --lib
```

Expected: all pass. If an unrelated pre-existing failure appears, record it and rerun the focused tests from Steps 3-4. Do NOT attempt to fix unrelated failures in this task.

- [ ] **Step 6: Optional commit**

If validation required any small fixes:

```bash
git add <changed-files>
git commit -m "fix(connector): address StarRocks codegen connector validation"
```

If no fixes were needed, do not create an empty commit.

## Follow-on Plans

After this plan lands, write separate implementation plans for:

1. Stage 5 cleanup: delete `CatalogProvider::get_physical_layout`, `InMemoryCatalog.physical_layouts`, and the layout-population code in `register_starrocks_table_in_catalog`.
2. Route `to_thrift_scan` through the registry (replacing `StarRocksTableScanPlanner::stateless_for_codegen()` calls with `registry.scan_planner("starrocks")?.to_thrift_scan(...)`), and retire `stateless_for_codegen`.
3. Stage 3: migrate Iceberg from `ScanSource::IcebergDataFiles` to connector-owned file splits.
4. Stage 4: optimizer column-pruning / predicate-pushdown / statistics / dictionary / explain through connector capabilities.
