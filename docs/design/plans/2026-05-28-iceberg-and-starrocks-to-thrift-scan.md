# Iceberg & StarRocks `to_thrift_scan` Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Migrate `TLakeScanNode` + StarRocks scan-range generation, and `THdfsScanNode` + Iceberg HDFS scan-range generation, from `src/sql/codegen/nodes.rs` into each connector's `to_thrift_scan`. After this slice, `nodes.rs::build_scan_node` and `build_exec_params_multi` delegate to the connector planner whenever `resolved.planned_scan` is populated and the source is `StarRocks` or `IcebergDataFiles`.

**Architecture:** Extend `ThriftScanContext` with the per-query state both planners need (node_id, scan_tuple_id, conjuncts) plus Iceberg-specific state (min_max_predicates, change_op_slot, cloud_properties). Capture the projected column list in `IcebergTableHandle` (consumed by `THdfsScanNode.column_names`). StarRocks first (TLakeScanNode is mostly default-valued), Iceberg second (richer — cloud_config, column_names, file-level min/max). `IcebergMetadataTable` and `IcebergDeltaTable` keep their existing placeholder/dedicated-node codegen.

**Tech Stack:** Rust, existing `ConnectorRegistry` / `ConnectorScanPlanner`, existing `MinMaxPredicate` from `src/common/min_max_predicate.rs`, `cargo test --lib`.

---

## Scope Check

The parent spec (`docs/design/specs/2026-05-28-connector-first-standalone-scan-design.md`) Stage 3 covers full Iceberg migration. The previous slice (PR #202) wired `begin_scan` / `plan_splits` for `IcebergDataFiles`. This slice completes the `to_thrift_scan` half for `IcebergDataFiles` and brings StarRocks' `to_thrift_scan` to feature parity (returning `Some(node)` as well as `scan_ranges`).

- Extend `ThriftScanContext` with shared and Iceberg-specific per-query state.
- Capture `column_names` in `IcebergTableHandle`.
- Move `build_lake_scan_node` into `StarRocksTableScanPlanner::to_thrift_scan`.
- Move `build_hdfs_scan_node` IcebergDataFiles logic + HDFS scan-range helpers into `IcebergConnectorScanPlanner::to_thrift_scan`.
- Route StarRocks + IcebergDataFiles scans through the connector from `nodes.rs::build_scan_node` and `build_exec_params_multi`.

Out of scope (later slices):

- Migrating `IcebergMetadataTable` and `IcebergDeltaTable` through the connector.
- Caching `ThriftScanPlan` so `to_thrift_scan` is invoked once per scan instead of twice (this slice tolerates the double-call; see `Risks` in the spec).
- Hooking optimizer capabilities (`apply_projection`, `apply_predicate`, ...). That is Stage 4.
- Moving dict-slot patching (`lake_scan_node.dict_string_id_to_int_ids`) into the StarRocks connector.
- Deleting `ScanSource::IcebergDataFiles.files`. Optimizer/planner/query_prep/dictionary/explain still read it.

## File Structure

- Modify: `src/connector/scan_planning.rs`
  - Extend `ThriftScanContext` with 6 new fields + `#[derive(Default)]`.

- Modify: `src/connector/iceberg/scan_planner.rs`
  - Add `column_names: Vec<String>` to `IcebergTableHandle`.
  - Add `column_names` parameter to `table_handle_from_source`.
  - Move HDFS scan-range helpers (`build_hdfs_scan_range_params_for_file`, `plan_hdfs_file_splits`, `validate_iceberg_delete_apply_cost`, `build_hdfs_scan_range_params`) and constants from `nodes.rs` into this module.
  - Implement `to_thrift_scan` to return `Some(THdfsScanNode) + scan_ranges`. Add `pub(crate) fn build_iceberg_scan_ranges` and `pub(crate) fn build_iceberg_hdfs_scan_node` as private helpers inside this module.
  - Update the existing `downcasts_iceberg_scan_and_split` test fixture to pass `column_names`.
  - Add a new `to_thrift_scan_returns_node_and_scan_ranges` unit test.

- Modify: `src/connector/starrocks/table/scan_planner.rs`
  - Implement `to_thrift_scan` to return `Some(TLakeScanNode) + scan_ranges`. The TLakeScanNode body is the current `build_lake_scan_node` from `nodes.rs`, minus the `resolved.planned_scan` lookup.
  - Add a new `to_thrift_scan_returns_lake_scan_node_and_scan_ranges` unit test.

- Modify: `src/sql/codegen/nodes.rs`
  - Update `build_scan_node` signature to accept `connectors: &ConnectorRegistry`, return `Result<TPlanNode, String>`, and route StarRocks + IcebergDataFiles through the connector when `planned_scan` is populated.
  - Update `build_exec_params_multi` to route IcebergDataFiles through the connector via `to_thrift_scan` (StarRocks already routes through it indirectly via `build_starrocks_scan_ranges_from_planned_scan`).
  - Delete `build_lake_scan_node`.
  - Delete `build_hdfs_scan_range_params_for_file`, `plan_hdfs_file_splits`, `validate_iceberg_delete_apply_cost`, `build_hdfs_scan_range_params`, and constants `ICEBERG_SCAN_SPLIT_TARGET_BYTES`, `ICEBERG_DELETE_APPLY_MAX_FILES_PER_DATA_FILE`, `ICEBERG_DELETE_APPLY_MAX_BYTES_PER_DATA_FILE` (moved into iceberg connector).
  - Strip the IcebergDataFiles arm from `build_hdfs_scan_node`'s `cloud_properties` and `serialized_table` matches — those arms are unreachable after Task 4. `build_hdfs_scan_node` retains only the IcebergMetadataTable code path.
  - Update `build_starrocks_scan_ranges_from_planned_scan` (line ~1019) so the ThriftScanContext it constructs uses `..Default::default()` for the new fields (this helper currently passes only `database` and `table`; works as-is after Task 1 since extra fields default).
  - Update the two fixture tests (`physical_change_op_column_does_not_emit_extended_columns`, `metadata_change_op_column_emits_extended_columns`) to pass `column_names` to `table_handle_from_source`.

- Modify: `src/sql/codegen/fragment_builder.rs`
  - Update `visit_scan`'s `nodes::build_scan_node(...)` call site (~line 779) to pass `&self.connectors` and `?` propagate the new `Result`.
  - Update the Iceberg arm of the `planned_scan` match (~line 510) to pass `column_names` to `table_handle_from_source`.
  - Extend `ScanPlannerCallCounts` and both counting wrappers with a `to_thrift_scan` counter.
  - Update the two existing `visit_scan_calls_…_begin_scan_and_plan_splits_for_…` tests to additionally assert `to_thrift_scan` invoked exactly twice per scan (once from `build_scan_node`, once from `build_exec_params_multi`).

---

## Task 1: Extend `ThriftScanContext`

**Files:**
- Modify: `src/connector/scan_planning.rs`
- Modify: `src/sql/codegen/nodes.rs` (one helper construction site)

The current `ThriftScanContext` carries only `{ database, table }`. Extend it with the per-query fields each planner's `to_thrift_scan` will read in Tasks 3-4. `Default`-derive so existing construction sites can use `..Default::default()` for the new fields.

- [ ] **Step 1: Add `MinMaxPredicate` import + new fields + `Default` to `ThriftScanContext`**

In `src/connector/scan_planning.rs`, locate the existing imports near the top:

```rust
use crate::{internal_service, plan_nodes};
```

Add adjacent imports for `exprs`, `types`, `MinMaxPredicate`, and `BTreeMap`:

```rust
use std::collections::BTreeMap;

use crate::common::min_max_predicate::MinMaxPredicate;
use crate::{exprs, internal_service, plan_nodes, types};
```

Then replace the existing `ThriftScanContext` struct (currently around line 135):

```rust
#[derive(Clone, Debug)]
pub(crate) struct ThriftScanContext {
    pub(crate) database: String,
    pub(crate) table: String,
}
```

with the extended shape:

```rust
#[derive(Clone, Debug, Default)]
pub(crate) struct ThriftScanContext {
    // Shared core: every TPlanNode needs these
    pub(crate) database: String,
    pub(crate) table: String,
    pub(crate) node_id: i32,
    pub(crate) scan_tuple_id: types::TTupleId,
    pub(crate) conjuncts: Vec<exprs::TExpr>,

    // Iceberg-specific per-query state (flat for now; will be encapsulated
    // into a connector-specific payload when a second HDFS-style connector
    // arrives — see slice spec).
    pub(crate) min_max_predicates: Vec<MinMaxPredicate>,
    pub(crate) change_op_slot: Option<types::TSlotId>,
    pub(crate) cloud_properties: BTreeMap<String, String>,
}
```

- [ ] **Step 2: Update the lone existing construction site**

In `src/sql/codegen/nodes.rs`, the helper `build_starrocks_scan_ranges_from_planned_scan` (around line 1019) constructs `ThriftScanContext` inline. Locate this block:

```rust
let thrift = planner.to_thrift_scan(
    &planned.scan,
    &planned.splits,
    ThriftScanContext {
        database: resolved.database.clone(),
        table: resolved.table.name.clone(),
    },
)?;
```

Change to:

```rust
let thrift = planner.to_thrift_scan(
    &planned.scan,
    &planned.splits,
    ThriftScanContext {
        database: resolved.database.clone(),
        table: resolved.table.name.clone(),
        ..ThriftScanContext::default()
    },
)?;
```

No other production code constructs `ThriftScanContext` directly; the trait-method receivers consume `ctx: ThriftScanContext` by value via the existing `to_thrift_scan` signature, so adding fields is backwards compatible.

- [ ] **Step 3: Build**

Run:

```bash
cargo build
```

Expected: clean build. Pre-existing warnings only.

- [ ] **Step 4: Run focused tests**

Run:

```bash
cargo test --lib connector::scan_planning connector::starrocks::table::scan_planner connector::iceberg::scan_planner
```

Expected: all existing tests still pass. The new fields default to zero/empty values so the StarRocks `to_thrift_scan` (which only reads `database`/`table`) is unaffected; the Iceberg stub `to_thrift_scan` returns the same error string.

- [ ] **Step 5: Commit**

```bash
git add src/connector/scan_planning.rs src/sql/codegen/nodes.rs
git commit -m "feat(connector): extend ThriftScanContext with per-query state"
```

**No `Co-Authored-By: Claude` trailer.**

## Task 2: Add `column_names` to `IcebergTableHandle`

**Files:**
- Modify: `src/connector/iceberg/scan_planner.rs`
- Modify: `src/sql/codegen/fragment_builder.rs`
- Modify: `src/sql/codegen/nodes.rs`

`THdfsScanNode.column_names` currently reads `resolved.table.columns` inside `build_hdfs_scan_node`. After Task 4, the iceberg connector's `to_thrift_scan` will need this list without access to `ResolvedTable`. Capture it in `IcebergTableHandle` at `begin_scan` time. This aligns with Stage 4's projection-pushdown direction (`apply_projection` will eventually mutate the projected column list on the scan handle).

- [ ] **Step 1: Add `column_names` field to `IcebergTableHandle`**

In `src/connector/iceberg/scan_planner.rs`, locate the `IcebergTableHandle` struct (around line 10):

```rust
#[derive(Clone, Debug)]
pub(crate) struct IcebergTableHandle {
    pub(crate) catalog: String,
    pub(crate) namespace: String,
    pub(crate) table: String,
    pub(crate) snapshot_id: Option<i64>,
    pub(crate) table_info: IcebergTableInfo,
    pub(crate) files: Vec<IcebergDataFileInfo>,
}
```

Add the `column_names` field at the end:

```rust
#[derive(Clone, Debug)]
pub(crate) struct IcebergTableHandle {
    pub(crate) catalog: String,
    pub(crate) namespace: String,
    pub(crate) table: String,
    pub(crate) snapshot_id: Option<i64>,
    pub(crate) table_info: IcebergTableInfo,
    pub(crate) files: Vec<IcebergDataFileInfo>,
    pub(crate) column_names: Vec<String>,
}
```

- [ ] **Step 2: Extend `table_handle_from_source` signature**

Same file, locate `table_handle_from_source` (around line 72). Replace it with:

```rust
pub(crate) fn table_handle_from_source(
    catalog: &str,
    namespace: &str,
    table: &str,
    snapshot_id: Option<i64>,
    table_info: IcebergTableInfo,
    files: Vec<IcebergDataFileInfo>,
    column_names: Vec<String>,
) -> TableHandle {
    TableHandle::new(
        CONNECTOR_ID,
        IcebergTableHandle {
            catalog: catalog.to_string(),
            namespace: namespace.to_string(),
            table: table.to_string(),
            snapshot_id,
            table_info,
            files,
            column_names,
        },
    )
}
```

- [ ] **Step 3: Update the downcast unit test fixture**

Same file, in `#[cfg(test)] mod tests`, locate the `downcasts_iceberg_scan_and_split` test (around line 183). The test constructs `IcebergTableHandle { … }` directly. Replace its construction block with:

```rust
let table = IcebergTableHandle {
    catalog: "memory".to_string(),
    namespace: "default".to_string(),
    table: "orders".to_string(),
    snapshot_id: Some(42),
    table_info: dummy_iceberg_table_info(),
    files: vec![dummy_iceberg_file()],
    column_names: vec!["id".to_string()],
};
```

(Adds `column_names: vec!["id".to_string()]` at the end.)

- [ ] **Step 4: Update visit_scan's iceberg arm to pass `column_names`**

In `src/sql/codegen/fragment_builder.rs::visit_scan`, locate the iceberg arm of the `planned_scan` match (around line 510). The current code calls `table_handle_from_source` with 6 args. Replace just the call to add the 7th argument:

```rust
crate::sql::catalog::ScanSource::IcebergDataFiles {
    table: iceberg_table,
    files,
    ..
} => {
    let planner = self.connectors.scan_planner("iceberg")?;
    let column_names = op
        .table
        .columns
        .iter()
        .map(|c| c.name.clone())
        .collect::<Vec<_>>();
    let table_handle =
        crate::connector::iceberg::IcebergConnectorScanPlanner::table_handle_from_source(
            &iceberg_table.catalog,
            &iceberg_table.namespace,
            &iceberg_table.table,
            iceberg_table.current_snapshot_id,
            iceberg_table.clone(),
            files.clone(),
            column_names,
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
```

- [ ] **Step 5: Update the two nodes.rs test fixtures**

In `src/sql/codegen/nodes.rs`, two test fixtures construct via `table_handle_from_source`:
- `physical_change_op_column_does_not_emit_extended_columns` (around line 1337)
- `metadata_change_op_column_emits_extended_columns` (around line 1489)

For BOTH fixtures, locate the `table_handle_from_source` call:

```rust
let table_handle =
    crate::connector::iceberg::IcebergConnectorScanPlanner::table_handle_from_source(
        &iceberg_table_info.catalog,
        &iceberg_table_info.namespace,
        &iceberg_table_info.table,
        iceberg_table_info.current_snapshot_id,
        iceberg_table_info.clone(),
        iceberg_files.clone(),
    );
```

Change to (adds a `column_names` arg at the end):

```rust
let table_handle =
    crate::connector::iceberg::IcebergConnectorScanPlanner::table_handle_from_source(
        &iceberg_table_info.catalog,
        &iceberg_table_info.namespace,
        &iceberg_table_info.table,
        iceberg_table_info.current_snapshot_id,
        iceberg_table_info.clone(),
        iceberg_files.clone(),
        vec![crate::exec::change_op::CHANGE_OP_COLUMN.to_string()],
    );
```

Both fixtures use the `__change_op` pseudo column; the `column_names` list mirrors what `resolved.table.columns` (or `iceberg_row_lineage_metadata_columns`) currently spells out for these tests. The value is not asserted in either test — only the scan-range output is — so any non-empty list satisfies the contract.

- [ ] **Step 6: Build**

Run:

```bash
cargo build
```

Expected: clean build.

- [ ] **Step 7: Run focused tests**

Run:

```bash
cargo test --lib connector::iceberg::scan_planner sql::codegen::nodes::tests sql::codegen::fragment_builder::tests
```

Expected: all pass.

- [ ] **Step 8: Commit**

```bash
git add src/connector/iceberg/scan_planner.rs src/sql/codegen/fragment_builder.rs src/sql/codegen/nodes.rs
git commit -m "feat(iceberg): capture column_names on IcebergTableHandle"
```

**No `Co-Authored-By: Claude` trailer.**

## Task 3: Migrate StarRocks `to_thrift_scan` to return `Some(TLakeScanNode) + scan_ranges` (atomic)

**Files:**
- Modify: `src/connector/starrocks/table/scan_planner.rs`
- Modify: `src/sql/codegen/nodes.rs`
- Modify: `src/sql/codegen/fragment_builder.rs`

This task is atomic: the StarRocks `to_thrift_scan` starts producing `Some(node)`, `nodes.rs::build_scan_node` starts routing StarRocks through the connector, and `visit_scan`'s call site is updated to thread `&self.connectors` and `?`. The intermediate state would not compile.

The Iceberg branch of `build_scan_node` continues to use `build_hdfs_scan_node` from `nodes.rs` — Task 4 migrates it. `build_starrocks_scan_ranges_from_planned_scan` at `nodes.rs::~1019` is left in place (it still calls `to_thrift_scan` to obtain `scan_ranges`); after this task it discards the returned `Some(node)`.

- [ ] **Step 1: Implement StarRocks `to_thrift_scan` returning `Some(TLakeScanNode) + scan_ranges`**

In `src/connector/starrocks/table/scan_planner.rs`, locate the existing `to_thrift_scan` (around line 206):

```rust
fn to_thrift_scan(
    &self,
    scan: &ScanHandle,
    splits: &[Split],
    ctx: ThriftScanContext,
) -> Result<ThriftScanPlan, String> {
    validate_split_connectors(scan, splits)?;
    let scan = starrocks_scan_handle(scan)?;
    let scan_ranges = splits
        .iter()
        .map(|split| {
            let split = starrocks_split(split)?;
            Ok(Self::build_internal_scan_range_params(
                &ctx.database,
                &ctx.table,
                scan.schema_id,
                split,
            ))
        })
        .collect::<Result<Vec<_>, String>>()?;
    Ok(ThriftScanPlan {
        node: None,
        scan_ranges,
    })
}
```

Replace with:

```rust
fn to_thrift_scan(
    &self,
    scan: &ScanHandle,
    splits: &[Split],
    ctx: ThriftScanContext,
) -> Result<ThriftScanPlan, String> {
    validate_split_connectors(scan, splits)?;
    let scan = starrocks_scan_handle(scan)?;
    let scan_ranges = splits
        .iter()
        .map(|split| {
            let split = starrocks_split(split)?;
            Ok(Self::build_internal_scan_range_params(
                &ctx.database,
                &ctx.table,
                scan.schema_id,
                split,
            ))
        })
        .collect::<Result<Vec<_>, String>>()?;
    let node = Self::build_lake_scan_node(scan, &ctx);
    Ok(ThriftScanPlan {
        node: Some(node),
        scan_ranges,
    })
}
```

- [ ] **Step 2: Add `build_lake_scan_node` helper inside the StarRocks planner**

Same file, locate the existing `build_internal_scan_range_params` helper (around line 110) inside `impl StarRocksTableScanPlanner`. Add this helper immediately after `build_internal_scan_range_params`'s closing brace:

```rust
fn build_lake_scan_node(
    scan: &StarRocksScanHandle,
    ctx: &ThriftScanContext,
) -> plan_nodes::TPlanNode {
    let mut node = crate::sql::codegen::nodes::default_plan_node();
    node.node_id = ctx.node_id;
    node.node_type = plan_nodes::TPlanNodeType::LAKE_SCAN_NODE;
    node.num_children = 0;
    node.limit = -1;
    node.row_tuples = vec![ctx.scan_tuple_id];
    node.nullable_tuples = vec![];
    node.conjuncts = if ctx.conjuncts.is_empty() {
        None
    } else {
        Some(ctx.conjuncts.clone())
    };
    node.compact_data = true;
    node.lake_scan_node = Some(plan_nodes::TLakeScanNode {
        tuple_id: ctx.scan_tuple_id,
        key_column_name: vec![],
        key_column_type: vec![],
        is_preaggregation: false,
        sort_column: None,
        rollup_name: None,
        sql_predicates: None,
        enable_column_expr_predicate: None,
        dict_string_id_to_int_ids: None,
        unused_output_column_name: None,
        sort_key_column_names: None,
        bucket_exprs: None,
        column_access_paths: None,
        sorted_by_keys_per_tablet: None,
        output_chunk_by_bucket: None,
        output_asc_hint: None,
        partition_order_hint: None,
        enable_topn_filter_back_pressure: None,
        back_pressure_max_rounds: None,
        back_pressure_throttle_time: None,
        back_pressure_throttle_time_upper_bound: None,
        back_pressure_num_rows: None,
        schema_key: Some(crate::descriptors::TTableSchemaKey::new(
            Some(scan.table.db_id),
            Some(scan.table.table_id),
            Some(scan.schema_id),
        )),
        enable_prune_column_after_index_filter: None,
        enable_gin_filter: None,
        next_uniq_id: None,
        enable_global_late_materialization: None,
    });
    node
}
```

Note: `crate::sql::codegen::nodes::default_plan_node` is `pub(crate)` and used here from the connector layer. This is a logical-only cross-module call (Rust crates have a flat namespace); the layering blemish is acceptable for this slice and is tracked in the spec's "Out-of-scope follow-ups". `crate::descriptors` is already accessible (same crate root).

- [ ] **Step 3: Add a unit test for the new `to_thrift_scan` shape**

Same file, inside the existing `#[cfg(test)] mod tests` (after the `downcasts_starrocks_scan_and_split` test), add:

```rust
#[test]
fn to_thrift_scan_returns_lake_scan_node_and_scan_ranges() {
    let scan = ScanHandle::new(
        CONNECTOR_ID,
        StarRocksScanHandle {
            table: StarRocksTableHandle {
                database: "default".to_string(),
                table: "orders".to_string(),
                db_id: 10,
                table_id: 20,
            },
            schema_id: 30,
        },
    );
    let splits = vec![Split::new(
        CONNECTOR_ID,
        StarRocksSplit {
            tablet_id: 300,
            partition_id: 100,
            version: 7,
        },
    )];
    let ctx = ThriftScanContext {
        database: "default".to_string(),
        table: "orders".to_string(),
        node_id: 11,
        scan_tuple_id: 1,
        ..ThriftScanContext::default()
    };

    let planner = StarRocksTableScanPlanner::stateless_for_codegen();
    let plan = planner
        .to_thrift_scan(&scan, &splits, ctx)
        .expect("to_thrift_scan");

    let node = plan.node.expect("planner must return a TPlanNode");
    assert_eq!(node.node_id, 11);
    assert_eq!(node.node_type, plan_nodes::TPlanNodeType::LAKE_SCAN_NODE);
    let lake = node.lake_scan_node.as_ref().expect("lake_scan_node");
    assert_eq!(lake.tuple_id, 1);
    let schema_key = lake.schema_key.as_ref().expect("schema_key");
    assert_eq!(schema_key.db_id, Some(10));
    assert_eq!(schema_key.table_id, Some(20));
    assert_eq!(schema_key.schema_id, Some(30));

    assert_eq!(plan.scan_ranges.len(), 1);
    let internal = plan.scan_ranges[0]
        .scan_range
        .internal_scan_range
        .as_ref()
        .expect("internal scan range");
    assert_eq!(internal.tablet_id, 300);
}
```

- [ ] **Step 4: Update `nodes.rs::build_scan_node` signature and StarRocks routing**

In `src/sql/codegen/nodes.rs`, locate `build_scan_node` (around line 38):

```rust
pub(crate) fn build_scan_node(
    node_id: i32,
    scan_tuple_id: i32,
    resolved: &ResolvedTable,
    conjuncts: Vec<exprs::TExpr>,
) -> plan_nodes::TPlanNode {
    if matches!(resolved.table.source, ScanSource::StarRocks { .. }) {
        return build_lake_scan_node(node_id, scan_tuple_id, resolved, conjuncts);
    }
    if matches!(resolved.table.source, ScanSource::IcebergDeltaTable { .. }) {
        return build_iceberg_delta_scan_node(node_id, scan_tuple_id, resolved, conjuncts);
    }
    build_hdfs_scan_node(node_id, scan_tuple_id, resolved, conjuncts)
}
```

Replace with:

```rust
pub(crate) fn build_scan_node(
    connectors: &crate::connector::ConnectorRegistry,
    node_id: i32,
    scan_tuple_id: i32,
    resolved: &ResolvedTable,
    conjuncts: Vec<exprs::TExpr>,
) -> Result<plan_nodes::TPlanNode, String> {
    if matches!(resolved.table.source, ScanSource::StarRocks { .. }) {
        let planned = resolved.planned_scan.as_ref().ok_or_else(|| {
            format!(
                "StarRocks scan {}.{} reached build_scan_node without planned connector scan",
                resolved.database, resolved.table.name
            )
        })?;
        let planner = connectors.scan_planner("starrocks")?;
        let ctx = crate::connector::scan_planning::ThriftScanContext {
            database: resolved.database.clone(),
            table: resolved.table.name.clone(),
            node_id,
            scan_tuple_id,
            conjuncts,
            ..crate::connector::scan_planning::ThriftScanContext::default()
        };
        let plan = planner.to_thrift_scan(&planned.scan, &planned.splits, ctx)?;
        return plan.node.ok_or_else(|| {
            format!(
                "StarRocks to_thrift_scan returned no node for {}.{}",
                resolved.database, resolved.table.name
            )
        });
    }
    if matches!(resolved.table.source, ScanSource::IcebergDeltaTable { .. }) {
        return Ok(build_iceberg_delta_scan_node(
            node_id,
            scan_tuple_id,
            resolved,
            conjuncts,
        ));
    }
    let _ = connectors;
    Ok(build_hdfs_scan_node(
        node_id,
        scan_tuple_id,
        resolved,
        conjuncts,
    ))
}
```

(The `let _ = connectors;` line silences the unused-variable warning for the IcebergDataFiles / IcebergMetadataTable path that Task 4 will rewrite.)

- [ ] **Step 5: Delete `build_lake_scan_node`**

Same file, locate the `build_lake_scan_node` function (lines ~239 through ~307) and delete the entire function. It is no longer referenced after Step 4.

- [ ] **Step 6: Update `visit_scan` call site to pass `&self.connectors` and `?`**

In `src/sql/codegen/fragment_builder.rs`, locate the `build_scan_node` call (around line 779):

```rust
let mut scan_plan_node = nodes::build_scan_node(
    scan_node_id,
    scan_tuple_id,
    &resolved,
    pushed_conjuncts.clone(),
);
```

Replace with:

```rust
let mut scan_plan_node = nodes::build_scan_node(
    self.connectors,
    scan_node_id,
    scan_tuple_id,
    &resolved,
    pushed_conjuncts.clone(),
)?;
```

- [ ] **Step 7: Build**

Run:

```bash
cargo build
```

Expected: clean build. New warnings are acceptable; report them.

- [ ] **Step 8: Run focused tests**

Run:

```bash
cargo test --lib connector::starrocks::table::scan_planner sql::codegen::nodes::tests sql::codegen::fragment_builder::tests
```

Expected: all pass. The new `to_thrift_scan_returns_lake_scan_node_and_scan_ranges` test passes; existing StarRocks fixtures (e.g., `build_starrocks_scan_emits_lake_scan_with_internal_ranges`) still pass because the connector now produces the same `TLakeScanNode` shape they previously asserted on.

- [ ] **Step 9: Run full lib tests**

```bash
cargo test --lib
```

Expected: all pass.

- [ ] **Step 10: Commit**

```bash
git add src/connector/starrocks/table/scan_planner.rs src/sql/codegen/nodes.rs src/sql/codegen/fragment_builder.rs
git commit -m "refactor(codegen): StarRocks to_thrift_scan generates TLakeScanNode"
```

**No `Co-Authored-By: Claude` trailer.**

## Task 4: Migrate Iceberg `to_thrift_scan` to return `Some(THdfsScanNode) + scan_ranges` (atomic)

**Files:**
- Modify: `src/connector/iceberg/scan_planner.rs`
- Modify: `src/sql/codegen/nodes.rs`
- Modify: `src/sql/codegen/fragment_builder.rs`

This task is atomic across:
1. Iceberg `to_thrift_scan` starts producing `Some(node) + scan_ranges`.
2. HDFS scan-range helpers move from `nodes.rs` into the iceberg connector.
3. `nodes.rs::build_scan_node`'s default arm starts routing `IcebergDataFiles` through the connector.
4. `nodes.rs::build_exec_params_multi` reads ranges via `to_thrift_scan` for IcebergDataFiles.
5. `nodes.rs::build_hdfs_scan_node` is stripped of the IcebergDataFiles code path.
6. `visit_scan` threads `min_max_predicates` and `change_op_slot` into `build_scan_node`.

- [ ] **Step 1: Move HDFS scan-range helpers + constants into iceberg connector**

In `src/connector/iceberg/scan_planner.rs`, append (after the existing `IcebergConnectorScanPlanner` impl block and before the `#[cfg(test)] mod tests`):

```rust
pub(crate) const ICEBERG_SCAN_SPLIT_TARGET_BYTES: i64 = 128 * 1024 * 1024;
pub(crate) const ICEBERG_DELETE_APPLY_MAX_FILES_PER_DATA_FILE: usize = 1024;
pub(crate) const ICEBERG_DELETE_APPLY_MAX_BYTES_PER_DATA_FILE: i64 = 512 * 1024 * 1024;

pub(crate) fn build_hdfs_scan_range_params_for_file(
    file: &IcebergDataFileInfo,
    change_op_slot: Option<crate::types::TSlotId>,
) -> Result<Vec<crate::internal_service::TScanRangeParams>, String> {
    validate_iceberg_delete_apply_cost(&file.path, &file.delete_files)?;
    let splits = plan_hdfs_file_splits(file);
    splits
        .into_iter()
        .map(|(offset, length)| {
            build_hdfs_scan_range_params(
                &file.path,
                file.size,
                offset,
                length,
                file.first_row_id,
                file.data_sequence_number,
                file.ivm_change_op,
                change_op_slot,
                &file.delete_files,
            )
        })
        .collect()
}

fn plan_hdfs_file_splits(file: &IcebergDataFileInfo) -> Vec<(i64, i64)> {
    let file_len = file.size.max(0);
    if file_len <= ICEBERG_SCAN_SPLIT_TARGET_BYTES
        || file.first_row_id.is_some()
        || !file.delete_files.is_empty()
    {
        return vec![(0, file_len)];
    }

    let mut out = Vec::new();
    let mut offset = 0_i64;
    while offset < file_len {
        let remaining = file_len - offset;
        let length = remaining.min(ICEBERG_SCAN_SPLIT_TARGET_BYTES);
        out.push((offset, length));
        offset += length;
    }
    if out.is_empty() {
        out.push((0, 0));
    }
    out
}

fn validate_iceberg_delete_apply_cost(
    data_path: &str,
    delete_files: &[crate::sql::catalog::IcebergDeleteFileInfo],
) -> Result<(), String> {
    if delete_files.len() > ICEBERG_DELETE_APPLY_MAX_FILES_PER_DATA_FILE {
        return Err(format!(
            "too many Iceberg delete files attached to data file {data_path}: count={} max={}",
            delete_files.len(),
            ICEBERG_DELETE_APPLY_MAX_FILES_PER_DATA_FILE
        ));
    }
    let total_bytes = delete_files.iter().try_fold(0_i64, |acc, delete_file| {
        let Some(length) = delete_file.length else {
            return Ok(acc);
        };
        acc.checked_add(length.max(0))
            .ok_or_else(|| format!("Iceberg delete file length overflow for data file {data_path}"))
    })?;
    if total_bytes > ICEBERG_DELETE_APPLY_MAX_BYTES_PER_DATA_FILE {
        return Err(format!(
            "Iceberg delete files attached to data file {data_path} are too large: bytes={total_bytes} max={ICEBERG_DELETE_APPLY_MAX_BYTES_PER_DATA_FILE}"
        ));
    }
    Ok(())
}

fn int_literal_expr(value: i64) -> crate::exprs::TExpr {
    crate::exprs::TExpr::new(vec![
        crate::sql::codegen::expr_compiler::int_literal_node(value),
    ])
}

fn build_hdfs_scan_range_params(
    full_path: &str,
    file_len: i64,
    offset: i64,
    length: i64,
    first_row_id: Option<i64>,
    data_sequence_number: Option<i64>,
    ivm_change_op: Option<i8>,
    change_op_slot: Option<crate::types::TSlotId>,
    delete_files: &[crate::sql::catalog::IcebergDeleteFileInfo],
) -> Result<crate::internal_service::TScanRangeParams, String> {
    use crate::sql::catalog::{IcebergDeleteFileContent, IcebergDeleteFileFormat};
    use std::collections::BTreeMap;

    let mut parquet_delete_files = Vec::new();
    let mut deletion_vector_descriptor = None;
    for delete_file in delete_files {
        match delete_file.file_format {
            IcebergDeleteFileFormat::Parquet => {
                let file_content = match delete_file.file_content {
                    IcebergDeleteFileContent::Position => {
                        crate::types::TIcebergFileContent::POSITION_DELETES
                    }
                    IcebergDeleteFileContent::Equality => {
                        // Equality field IDs are read from the equality-delete Parquet schema by
                        // the Rust scan runner. The Thrift scan range only needs to identify the
                        // delete file as an equality-delete file.
                        crate::types::TIcebergFileContent::EQUALITY_DELETES
                    }
                };
                parquet_delete_files.push(crate::plan_nodes::TIcebergDeleteFile::new(
                    Some(delete_file.path.clone()),
                    Some(crate::descriptors::THdfsFileFormat::PARQUET),
                    Some(file_content),
                    delete_file.length,
                ));
            }
            IcebergDeleteFileFormat::Puffin => {
                if deletion_vector_descriptor.is_some() {
                    return Err(format!(
                        "multiple Puffin deletion vectors are attached to data file {}",
                        full_path
                    ));
                }
                let offset = delete_file.content_offset.ok_or_else(|| {
                    format!(
                        "Puffin deletion vector {} for data file {} is missing content_offset",
                        delete_file.path, full_path
                    )
                })?;
                let size = delete_file.content_size_in_bytes.ok_or_else(|| {
                    format!(
                        "Puffin deletion vector {} for data file {} is missing content_size_in_bytes",
                        delete_file.path, full_path
                    )
                })?;
                deletion_vector_descriptor = Some(crate::plan_nodes::TDeletionVectorDescriptor::new(
                    Some("PUFFIN".to_string()),
                    Some(delete_file.path.clone()),
                    Some(offset),
                    Some(size),
                    None::<i64>,
                ));
            }
        }
    }
    let parquet_delete_files = if parquet_delete_files.is_empty() {
        None
    } else {
        Some(parquet_delete_files)
    };
    let extended_columns = match (ivm_change_op, change_op_slot) {
        (Some(op), Some(slot_id)) => {
            crate::exec::change_op::validate_change_op_value(op)?;
            Some(BTreeMap::from([(slot_id, int_literal_expr(op as i64))]))
        }
        _ => None,
    };
    let hdfs_scan_range = crate::plan_nodes::THdfsScanRange::new(
        None::<String>,
        Some(offset),
        Some(length),
        None::<i64>,
        Some(file_len),
        Some(crate::descriptors::THdfsFileFormat::PARQUET),
        None::<crate::descriptors::TTextFileDesc>,
        Some(full_path.to_string()),
        None::<Vec<String>>,
        None::<bool>,
        parquet_delete_files,
        None::<i64>,
        None::<bool>,
        None::<String>,
        None::<String>,
        None::<i64>,
        None::<crate::data_cache::TDataCacheOptions>,
        None::<Vec<crate::types::TSlotId>>,
        None::<bool>,
        None::<BTreeMap<String, String>>,
        None::<Vec<crate::types::TSlotId>>,
        None::<bool>,
        None::<String>,
        None::<bool>,
        None::<String>,
        None::<String>,
        None::<crate::plan_nodes::TPaimonDeletionFile>,
        extended_columns,
        None::<crate::descriptors::THdfsPartition>,
        None::<crate::types::TTableId>,
        deletion_vector_descriptor,
        None::<String>,
        None::<i64>,
        None::<bool>,
        None::<BTreeMap<i32, crate::exprs::TExprMinMaxValue>>,
        None::<i32>,
        first_row_id,
        data_sequence_number,
    );

    Ok(crate::internal_service::TScanRangeParams::new(
        crate::plan_nodes::TScanRange::new(
            None::<crate::plan_nodes::TInternalScanRange>,
            None::<Vec<u8>>,
            None::<crate::plan_nodes::TBrokerScanRange>,
            None::<crate::plan_nodes::TEsScanRange>,
            Some(hdfs_scan_range),
            None::<crate::plan_nodes::TBinlogScanRange>,
            None::<crate::plan_nodes::TBenchmarkScanRange>,
        ),
        None::<i32>,
        Some(false),
        Some(false),
    ))
}
```

(`crate::sql::codegen::expr_compiler::int_literal_node` must be reachable; it is `pub(crate)` in the same crate. Same layering blemish as `default_plan_node` in Task 3.)

- [ ] **Step 2: Implement Iceberg `to_thrift_scan` returning `Some(THdfsScanNode) + scan_ranges`**

Same file, locate the existing stub `to_thrift_scan` (around line 145):

```rust
fn to_thrift_scan(
    &self,
    scan: &ScanHandle,
    splits: &[Split],
    _ctx: ThriftScanContext,
) -> Result<ThriftScanPlan, String> {
    validate_split_connectors(scan, splits)?;
    Err(
        "IcebergConnectorScanPlanner::to_thrift_scan is not yet implemented; \
         codegen still produces HDFS scan ranges via build_hdfs_scan_range_params_for_file"
            .to_string(),
    )
}
```

Replace with:

```rust
fn to_thrift_scan(
    &self,
    scan: &ScanHandle,
    splits: &[Split],
    ctx: ThriftScanContext,
) -> Result<ThriftScanPlan, String> {
    validate_split_connectors(scan, splits)?;
    let scan_handle = iceberg_scan_handle(scan)?;
    let scan_ranges = build_iceberg_scan_ranges(splits, &ctx)?;
    let node = build_iceberg_hdfs_scan_node(scan_handle, &ctx);
    Ok(ThriftScanPlan {
        node: Some(node),
        scan_ranges,
    })
}
```

Then append the two new private helpers immediately after the trait `impl`:

```rust
fn build_iceberg_scan_ranges(
    splits: &[Split],
    ctx: &ThriftScanContext,
) -> Result<Vec<crate::internal_service::TScanRangeParams>, String> {
    use crate::sql::codegen::nodes::file_may_satisfy_min_max;

    let mut ranges = Vec::new();
    for split in splits {
        let iceberg_split = iceberg_split(split)?;
        let file = &iceberg_split.data_file;
        if !file_may_satisfy_min_max(file, &ctx.min_max_predicates) {
            continue;
        }
        ranges.extend(build_hdfs_scan_range_params_for_file(file, ctx.change_op_slot)?);
    }
    Ok(ranges)
}

fn build_iceberg_hdfs_scan_node(
    scan: &IcebergScanHandle,
    ctx: &ThriftScanContext,
) -> crate::plan_nodes::TPlanNode {
    let mut node = crate::sql::codegen::nodes::default_plan_node();
    node.node_id = ctx.node_id;
    node.node_type = crate::plan_nodes::TPlanNodeType::HDFS_SCAN_NODE;
    node.num_children = 0;
    node.limit = -1;
    node.row_tuples = vec![ctx.scan_tuple_id];
    node.nullable_tuples = vec![];
    let min_max_conjuncts = if ctx.conjuncts.is_empty() {
        None
    } else {
        Some(ctx.conjuncts.clone())
    };
    let min_max_tuple_id = min_max_conjuncts.as_ref().map(|_| ctx.scan_tuple_id);
    node.conjuncts = if ctx.conjuncts.is_empty() {
        None
    } else {
        Some(ctx.conjuncts.clone())
    };
    node.compact_data = true;

    let cloud_config = if ctx.cloud_properties.is_empty() {
        None
    } else {
        Some(crate::cloud_configuration::TCloudConfiguration::new(
            None::<crate::cloud_configuration::TCloudType>,
            None::<Vec<crate::cloud_configuration::TCloudProperty>>,
            Some(ctx.cloud_properties.clone()),
            None::<bool>,
        ))
    };

    node.hdfs_scan_node = Some(crate::plan_nodes::THdfsScanNode::new(
        Some(ctx.scan_tuple_id),
        None::<std::collections::BTreeMap<crate::types::TTupleId, Vec<crate::exprs::TExpr>>>,
        min_max_conjuncts,
        min_max_tuple_id,
        None::<std::collections::BTreeMap<crate::types::TSlotId, Vec<i32>>>,
        None::<Vec<crate::exprs::TExpr>>,
        Some(scan.table.column_names.clone()),
        Some(scan.table.table.clone()),
        None::<String>,
        None::<String>,
        None::<String>,
        Some(true), // case_sensitive
        cloud_config,
        None::<bool>,
        None::<bool>,
        None::<bool>,
        None::<crate::types::TTupleId>,
        None::<String>,
        None::<Vec<crate::exprs::TExpr>>,
        None::<bool>,
        None::<String>,
        None::<crate::data_cache::TDataCacheOptions>,
        None::<Vec<crate::types::TSlotId>>,
        None::<bool>,
        None::<Vec<crate::partitions::TBucketProperty>>,
        None::<bool>,
        None::<i64>,
        None::<Vec<crate::plan_nodes::TColumnAccessPath>>,
    ));

    node
}
```

Finally, inside the existing `#[cfg(test)] mod tests` block (after the `downcasts_iceberg_scan_and_split` test), add a focused unit test asserting `to_thrift_scan` returns the expected `THdfsScanNode` shape and one scan range per file:

```rust
#[test]
fn to_thrift_scan_returns_hdfs_scan_node_and_scan_ranges() {
    let column_names = vec!["id".to_string()];
    let table = IcebergTableHandle {
        catalog: "memory".to_string(),
        namespace: "default".to_string(),
        table: "orders".to_string(),
        snapshot_id: Some(42),
        table_info: dummy_iceberg_table_info(),
        files: vec![dummy_iceberg_file()],
        column_names: column_names.clone(),
    };
    let scan = ScanHandle::new(
        CONNECTOR_ID,
        IcebergScanHandle {
            table: table.clone(),
        },
    );
    let splits = vec![Split::new(
        CONNECTOR_ID,
        IcebergSplit {
            data_file: dummy_iceberg_file(),
        },
    )];
    let mut cloud_properties = std::collections::BTreeMap::new();
    cloud_properties.insert("fs.s3a.endpoint".to_string(), "http://minio:9000".to_string());
    let ctx = ThriftScanContext {
        database: "default".to_string(),
        table: "orders".to_string(),
        node_id: 17,
        scan_tuple_id: 2,
        cloud_properties,
        ..ThriftScanContext::default()
    };

    let planner = IcebergConnectorScanPlanner::new();
    let plan = planner
        .to_thrift_scan(&scan, &splits, ctx)
        .expect("to_thrift_scan");

    let node = plan.node.expect("planner must return a TPlanNode");
    assert_eq!(node.node_id, 17);
    assert_eq!(node.node_type, crate::plan_nodes::TPlanNodeType::HDFS_SCAN_NODE);
    let hdfs = node.hdfs_scan_node.as_ref().expect("hdfs_scan_node");
    assert_eq!(hdfs.tuple_id, Some(2));
    assert_eq!(hdfs.column_names.as_ref(), Some(&column_names));
    assert_eq!(hdfs.table_name.as_deref(), Some("orders"));
    assert!(hdfs.cloud_configuration.is_some(), "cloud_config should be populated");

    assert_eq!(plan.scan_ranges.len(), 1);
    let hdfs_range = plan.scan_ranges[0]
        .scan_range
        .hdfs_scan_range
        .as_ref()
        .expect("hdfs scan range");
    assert_eq!(hdfs_range.full_path.as_deref(), Some("s3://bucket/data/file.parquet"));
}
```

- [ ] **Step 3: Bump visibility of helpers the connector now calls**

The Iceberg connector's new `to_thrift_scan` body calls two helpers that currently have narrower visibility than `pub(crate)`. Bump both.

**3a. `file_may_satisfy_min_max`** (in `src/sql/codegen/nodes.rs`, around line 701) currently is a private `fn`. Change its visibility to `pub(crate) fn`:

```rust
pub(crate) fn file_may_satisfy_min_max(
    file: &IcebergDataFileInfo,
    predicates: &[MinMaxPredicate],
) -> bool {
```

(The body and the private sibling helpers it calls — `partition_may_satisfy_predicate`, `find_column_stats`, `stats_may_satisfy_predicate` — stay unchanged. A `pub(crate)` function can call private siblings within its own module.)

**3b. `int_literal_node`** (in `src/sql/codegen/expr_compiler.rs`, around line 1734) currently is `pub(super)`. Change to `pub(crate)`:

```rust
pub(crate) fn int_literal_node(value: i64) -> exprs::TExprNode {
    int_literal_node_typed(value, &DataType::Int64)
}
```

(The private `int_literal_node_typed` it delegates to stays unchanged.)

Both bumps are the same logical layering blemish as `default_plan_node` in Task 3 — the connector layer reaches into `crate::sql::codegen::*`. Tracked under follow-ups.

- [ ] **Step 4: Update `build_scan_node`'s default arm and `build_exec_params_multi` to route IcebergDataFiles through the connector**

In `src/sql/codegen/nodes.rs::build_scan_node`, locate the function as left by Task 3:

```rust
pub(crate) fn build_scan_node(
    connectors: &crate::connector::ConnectorRegistry,
    node_id: i32,
    scan_tuple_id: i32,
    resolved: &ResolvedTable,
    conjuncts: Vec<exprs::TExpr>,
) -> Result<plan_nodes::TPlanNode, String> {
    if matches!(resolved.table.source, ScanSource::StarRocks { .. }) {
        // ... StarRocks routing from Task 3 ...
    }
    if matches!(resolved.table.source, ScanSource::IcebergDeltaTable { .. }) {
        return Ok(build_iceberg_delta_scan_node(
            node_id,
            scan_tuple_id,
            resolved,
            conjuncts,
        ));
    }
    let _ = connectors;
    Ok(build_hdfs_scan_node(
        node_id,
        scan_tuple_id,
        resolved,
        conjuncts,
    ))
}
```

The function needs two more pieces of state for the Iceberg routing: `min_max_predicates` and `change_op_slot`. Bump the signature to take them as parameters (alongside `conjuncts`), and add an IcebergDataFiles arm before the IcebergDeltaTable arm:

```rust
pub(crate) fn build_scan_node(
    connectors: &crate::connector::ConnectorRegistry,
    node_id: i32,
    scan_tuple_id: i32,
    resolved: &ResolvedTable,
    conjuncts: Vec<exprs::TExpr>,
    min_max_predicates: Vec<crate::common::min_max_predicate::MinMaxPredicate>,
    change_op_slot: Option<types::TSlotId>,
) -> Result<plan_nodes::TPlanNode, String> {
    if matches!(resolved.table.source, ScanSource::StarRocks { .. }) {
        let planned = resolved.planned_scan.as_ref().ok_or_else(|| {
            format!(
                "StarRocks scan {}.{} reached build_scan_node without planned connector scan",
                resolved.database, resolved.table.name
            )
        })?;
        let planner = connectors.scan_planner("starrocks")?;
        let ctx = crate::connector::scan_planning::ThriftScanContext {
            database: resolved.database.clone(),
            table: resolved.table.name.clone(),
            node_id,
            scan_tuple_id,
            conjuncts,
            ..crate::connector::scan_planning::ThriftScanContext::default()
        };
        let plan = planner.to_thrift_scan(&planned.scan, &planned.splits, ctx)?;
        return plan.node.ok_or_else(|| {
            format!(
                "StarRocks to_thrift_scan returned no node for {}.{}",
                resolved.database, resolved.table.name
            )
        });
    }
    if matches!(resolved.table.source, ScanSource::IcebergDataFiles { .. }) {
        let planned = resolved.planned_scan.as_ref().ok_or_else(|| {
            format!(
                "Iceberg scan {}.{} reached build_scan_node without planned connector scan",
                resolved.database, resolved.table.name
            )
        })?;
        let cloud_properties = match &resolved.table.source {
            ScanSource::IcebergDataFiles {
                cloud_properties, ..
            } => cloud_properties.clone(),
            _ => unreachable!("IcebergDataFiles arm matched above"),
        };
        let planner = connectors.scan_planner("iceberg")?;
        let ctx = crate::connector::scan_planning::ThriftScanContext {
            database: resolved.database.clone(),
            table: resolved.table.name.clone(),
            node_id,
            scan_tuple_id,
            conjuncts,
            min_max_predicates,
            change_op_slot,
            cloud_properties,
        };
        let plan = planner.to_thrift_scan(&planned.scan, &planned.splits, ctx)?;
        return plan.node.ok_or_else(|| {
            format!(
                "Iceberg to_thrift_scan returned no node for {}.{}",
                resolved.database, resolved.table.name
            )
        });
    }
    if matches!(resolved.table.source, ScanSource::IcebergDeltaTable { .. }) {
        return Ok(build_iceberg_delta_scan_node(
            node_id,
            scan_tuple_id,
            resolved,
            conjuncts,
        ));
    }
    Ok(build_hdfs_scan_node(
        node_id,
        scan_tuple_id,
        resolved,
        conjuncts,
    ))
}
```

Then update `build_exec_params_multi`'s IcebergDataFiles arm. Locate the arm (around line 602) and replace with a connector-routed version:

```rust
ScanSource::IcebergDataFiles { .. } => {
    let planned_scan = resolved.planned_scan.as_ref().ok_or_else(|| {
        format!(
            "Iceberg scan {}.{} reached scan-range builder without planned connector scan",
            resolved.database, resolved.table.name
        )
    })?;
    let cloud_properties = match &resolved.table.source {
        ScanSource::IcebergDataFiles {
            cloud_properties, ..
        } => cloud_properties.clone(),
        _ => unreachable!("IcebergDataFiles arm matched above"),
    };
    // IcebergConnectorScanPlanner is already stateless; `::new()` is
    // equivalent to a hypothetical `stateless_for_codegen()`.
    let planner = crate::connector::iceberg::IcebergConnectorScanPlanner::new();
    let ctx = crate::connector::scan_planning::ThriftScanContext {
        database: resolved.database.clone(),
        table: resolved.table.name.clone(),
        node_id: planned.scan_node_id,
        scan_tuple_id: 0, // scan_ranges do not read tuple_id
        conjuncts: Vec::new(),
        min_max_predicates: scan_file_min_max_predicates(planned),
        change_op_slot: planned_change_op_slot(planned),
        cloud_properties,
    };
    let plan = planner
        .to_thrift_scan(&planned_scan.scan, &planned_scan.splits, ctx)?;
    plan.scan_ranges
}
```

`IcebergConnectorScanPlanner::new()` is the canonical way for codegen to obtain a planner without going through `&ConnectorRegistry`. It works because `IcebergConnectorScanPlanner` is already stateless. (Contrast with `StarRocksTableScanPlanner::stateless_for_codegen()`, which exists only because the normal `::new(state)` takes a `StandaloneState`.)

- [ ] **Step 5: Strip IcebergDataFiles from `build_hdfs_scan_node`**

In `src/sql/codegen/nodes.rs::build_hdfs_scan_node` (around line 107), the function reads `cloud_properties` and metadata-table fields from both `IcebergDataFiles` and `IcebergMetadataTable`. After this slice, only `IcebergMetadataTable` reaches this function. Replace the function body to drop the IcebergDataFiles paths.

Locate the existing function:

```rust
fn build_hdfs_scan_node(
    node_id: i32,
    scan_tuple_id: i32,
    resolved: &ResolvedTable,
    conjuncts: Vec<exprs::TExpr>,
) -> plan_nodes::TPlanNode {
    let mut node = default_plan_node();
    node.node_id = node_id;
    node.node_type = plan_nodes::TPlanNodeType::HDFS_SCAN_NODE;
    node.num_children = 0;
    node.limit = -1;
    node.row_tuples = vec![scan_tuple_id];
    node.nullable_tuples = vec![];
    let min_max_conjuncts = if conjuncts.is_empty() {
        None
    } else {
        Some(conjuncts.clone())
    };
    let min_max_tuple_id = min_max_conjuncts.as_ref().map(|_| scan_tuple_id);
    node.conjuncts = if conjuncts.is_empty() {
        None
    } else {
        Some(conjuncts)
    };
    node.compact_data = true;

    let cloud_config = match &resolved.table.source {
        ScanSource::IcebergDataFiles {
            cloud_properties, ..
        }
        | ScanSource::IcebergMetadataTable {
            cloud_properties, ..
        } => Some(crate::cloud_configuration::TCloudConfiguration::new(
            None::<crate::cloud_configuration::TCloudType>,
            None::<Vec<crate::cloud_configuration::TCloudProperty>>,
            Some(cloud_properties.clone()),
            None::<bool>,
        )),
        _ => None,
    };

    let (serialized_table, metadata_table_type, serialized_predicate) = match &resolved.table.source
    {
        ScanSource::IcebergMetadataTable {
            metadata_table_type,
            serialized_table,
            metadata_payload,
            ..
        } => (
            Some(serialized_table.clone()),
            Some(iceberg_metadata_table_type_thrift_str(metadata_table_type).to_string()),
            metadata_payload.clone(),
        ),
        _ => (None, None, None),
    };
    // ... existing body continues ...
```

Replace the two match blocks (`cloud_config` and the metadata destructure) with metadata-table-only versions:

```rust
let cloud_config = match &resolved.table.source {
    ScanSource::IcebergMetadataTable {
        cloud_properties, ..
    } => Some(crate::cloud_configuration::TCloudConfiguration::new(
        None::<crate::cloud_configuration::TCloudType>,
        None::<Vec<crate::cloud_configuration::TCloudProperty>>,
        Some(cloud_properties.clone()),
        None::<bool>,
    )),
    _ => None,
};

let (serialized_table, metadata_table_type, serialized_predicate) = match &resolved.table.source
{
    ScanSource::IcebergMetadataTable {
        metadata_table_type,
        serialized_table,
        metadata_payload,
        ..
    } => (
        Some(serialized_table.clone()),
        Some(iceberg_metadata_table_type_thrift_str(metadata_table_type).to_string()),
        metadata_payload.clone(),
    ),
    _ => (None, None, None),
};
```

Leave the rest of `build_hdfs_scan_node` unchanged.

- [ ] **Step 6: Delete the moved helpers and constants from `nodes.rs`**

In `src/sql/codegen/nodes.rs`, delete:

- Constants `ICEBERG_SCAN_SPLIT_TARGET_BYTES`, `ICEBERG_DELETE_APPLY_MAX_FILES_PER_DATA_FILE`, `ICEBERG_DELETE_APPLY_MAX_BYTES_PER_DATA_FILE` (around line 34).
- Function `build_hdfs_scan_range_params_for_file` (around line 946).
- Function `plan_hdfs_file_splits` (around line 970).
- Function `validate_iceberg_delete_apply_cost` (around line 993).
- Function `int_literal_expr` (around line 697) — only used by `build_hdfs_scan_range_params` which is also moving.
- Function `build_hdfs_scan_range_params` (around line 1044).

Confirm via `cargo build` that nothing in `nodes.rs` still references these symbols (the only remaining caller, `build_exec_params_multi`'s IcebergDataFiles arm, was rewritten in Step 4 to use the connector).

- [ ] **Step 7: Update `visit_scan` to compute and thread `min_max_predicates` + `change_op_slot`**

In `src/sql/codegen/fragment_builder.rs::visit_scan`, the current call (left by Task 3) reads:

```rust
let mut scan_plan_node = nodes::build_scan_node(
    self.connectors,
    scan_node_id,
    scan_tuple_id,
    &resolved,
    pushed_conjuncts.clone(),
)?;
```

Replace with:

```rust
let min_max_predicates = nodes::scan_file_min_max_predicates_from_state(
    &pushed_conjuncts,
    &slot_to_column,
);
let change_op_slot = nodes::planned_change_op_slot_from_state(
    &iceberg_metadata_pseudo_column_slots,
    &slot_to_column,
);
let mut scan_plan_node = nodes::build_scan_node(
    self.connectors,
    scan_node_id,
    scan_tuple_id,
    &resolved,
    pushed_conjuncts.clone(),
    min_max_predicates,
    change_op_slot,
)?;
```

The helpers `scan_file_min_max_predicates_from_state` and `planned_change_op_slot_from_state` are extracted-from-`PlannedScanTable` thin variants of the existing `scan_file_min_max_predicates` and `planned_change_op_slot`. Add them to `src/sql/codegen/nodes.rs` (next to the existing functions, around line 668):

```rust
pub(crate) fn scan_file_min_max_predicates_from_state(
    min_max_conjuncts: &[exprs::TExpr],
    slot_to_column: &HashMap<types::TSlotId, String>,
) -> Vec<MinMaxPredicate> {
    let mut predicates = Vec::new();
    for conjunct in min_max_conjuncts {
        let parsed = parse_min_max_conjuncts_with_column_resolver(conjunct, |slot_ref| {
            slot_to_column
                .get(&slot_ref.slot_id)
                .cloned()
                .ok_or_else(|| format!("slot_id {} has no scan column", slot_ref.slot_id))
        });
        if let Ok(parsed) = parsed {
            predicates.extend(parsed);
        }
    }
    predicates
}

pub(crate) fn planned_change_op_slot_from_state(
    iceberg_metadata_pseudo_column_slots: &BTreeSet<types::TSlotId>,
    slot_to_column: &HashMap<types::TSlotId, String>,
) -> Option<types::TSlotId> {
    iceberg_metadata_pseudo_column_slots
        .iter()
        .copied()
        .find(|slot_id| {
            slot_to_column.get(slot_id).is_some_and(|column| {
                column.eq_ignore_ascii_case(crate::exec::change_op::CHANGE_OP_COLUMN)
            })
        })
}
```

Leave the existing `scan_file_min_max_predicates(planned: &PlannedScanTable)` and `planned_change_op_slot(planned: &PlannedScanTable)` in place — they are still used by `build_exec_params_multi` for the IcebergDataFiles arm rewritten in Step 4.

- [ ] **Step 8: Fix the two `nodes.rs` fixture tests for the new `build_scan_node` signature**

The two test fixtures (`physical_change_op_column_does_not_emit_extended_columns`, `metadata_change_op_column_emits_extended_columns`) call `build_exec_params_multi`, not `build_scan_node`, so they require no changes from this step. Verify by running the focused tests in Step 11; if any new compile error appears in these fixtures because of test-private helper accessibility, the fix is to add `pub(crate)` to the helper (already `pub(crate)` per Step 8). Do not modify the test assertions.

- [ ] **Step 9: Build**

Run:

```bash
cargo build
```

Expected: clean build.

- [ ] **Step 10: Run focused tests**

Run:

```bash
cargo test --lib connector::iceberg::scan_planner sql::codegen::nodes::tests sql::codegen::fragment_builder::tests
```

Expected: all pass. The two `nodes.rs::tests` IcebergDataFiles fixtures keep the same assertions; the new connector path produces the same scan-range layout because the helpers moved verbatim.

- [ ] **Step 11: Run full lib tests**

```bash
cargo test --lib
```

Expected: all pass. If an engine integration test (e.g., dictionary, mv refresh) fails because its mock registry does not have iceberg registered, the Task 3 fix to `mock_starrocks_registry_for_engine_test` (from PR #202) should already cover it. If a new failure appears, surface it as a concern — do not fix unrelated tests in this task.

- [ ] **Step 12: Commit**

```bash
git add src/connector/iceberg/scan_planner.rs src/sql/codegen/nodes.rs src/sql/codegen/fragment_builder.rs
git commit -m "refactor(codegen): Iceberg to_thrift_scan generates THdfsScanNode + scan ranges"
```

**No `Co-Authored-By: Claude` trailer.**

## Task 5: Counting test that `to_thrift_scan` is invoked

**Files:**
- Modify: `src/sql/codegen/fragment_builder.rs`

Extend the existing counting wrappers (`CountingScanPlanner`, `CountingIcebergScanPlanner`) and shared `ScanPlannerCallCounts` with a `to_thrift_scan` counter, then update the two existing `visit_scan_calls_…` tests to assert it is invoked exactly twice per scan (once from `build_scan_node`, once from `build_exec_params_multi`).

- [ ] **Step 1: Add `to_thrift_scan` to `ScanPlannerCallCounts`**

In `src/sql/codegen/fragment_builder.rs::tests`, locate `ScanPlannerCallCounts` (around line 4089) and add a new atomic field:

```rust
#[derive(Debug, Default)]
struct ScanPlannerCallCounts {
    begin_scan: std::sync::atomic::AtomicUsize,
    plan_splits: std::sync::atomic::AtomicUsize,
    to_thrift_scan: std::sync::atomic::AtomicUsize,
}
```

- [ ] **Step 2: Increment in both wrappers**

Same file, locate `CountingScanPlanner::to_thrift_scan` (around line 4128). Replace its body to increment the counter:

```rust
fn to_thrift_scan(
    &self,
    scan: &crate::connector::scan_planning::ScanHandle,
    splits: &[crate::connector::scan_planning::Split],
    ctx: crate::connector::scan_planning::ThriftScanContext,
) -> Result<crate::connector::scan_planning::ThriftScanPlan, String> {
    self.counts
        .to_thrift_scan
        .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
    self.inner.to_thrift_scan(scan, splits, ctx)
}
```

Locate `CountingIcebergScanPlanner::to_thrift_scan` and apply the same pattern:

```rust
fn to_thrift_scan(
    &self,
    scan: &crate::connector::scan_planning::ScanHandle,
    splits: &[crate::connector::scan_planning::Split],
    ctx: crate::connector::scan_planning::ThriftScanContext,
) -> Result<crate::connector::scan_planning::ThriftScanPlan, String> {
    self.counts
        .to_thrift_scan
        .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
    self.inner.to_thrift_scan(scan, splits, ctx)
}
```

- [ ] **Step 3: Add assertion to the StarRocks counting test**

Locate `visit_scan_calls_connector_begin_scan_and_plan_splits_for_starrocks` (around line 6068). Add a new assertion at the end of the function, after the existing `plan_splits` assertion:

```rust
assert_eq!(
    counts
        .to_thrift_scan
        .load(std::sync::atomic::Ordering::SeqCst),
    2,
    "to_thrift_scan must be invoked twice (build_scan_node for node, build_exec_params_multi for ranges)"
);
```

- [ ] **Step 4: Add the same assertion to the Iceberg counting test**

Locate `visit_scan_calls_connector_begin_scan_and_plan_splits_for_iceberg` (around line 6154; added in PR #202). Add the same `to_thrift_scan` assertion at the end of the function.

- [ ] **Step 5: Run the focused tests**

```bash
cargo test --lib sql::codegen::fragment_builder::tests::visit_scan_calls_connector_begin_scan_and_plan_splits_for_starrocks sql::codegen::fragment_builder::tests::visit_scan_calls_connector_begin_scan_and_plan_splits_for_iceberg
```

Expected: both pass with `to_thrift_scan == 2`.

- [ ] **Step 6: Commit**

```bash
git add src/sql/codegen/fragment_builder.rs
git commit -m "test(connector): assert to_thrift_scan invocation count for both planners"
```

**No `Co-Authored-By: Claude` trailer.**

## Task 6: Validation pass

**Files:**
- No source edits expected unless validation surfaces a real bug.

- [ ] **Step 1: Formatting**

```bash
cargo fmt --check
```

Expected: no diffs in files modified by this slice (`src/connector/scan_planning.rs`, `src/connector/iceberg/scan_planner.rs`, `src/connector/starrocks/table/scan_planner.rs`, `src/sql/codegen/nodes.rs`, `src/sql/codegen/fragment_builder.rs`). If diffs appear, run `cargo fmt` and inspect with `git diff`. Out-of-scope drift (e.g., `src/sql/analyzer/**`) must be reverted with `git checkout --` before committing — same protocol as PR #202.

- [ ] **Step 2: Build**

```bash
cargo build
```

Expected: clean build. New warnings are acceptable; report them.

- [ ] **Step 3: Run focused connector and codegen tests**

```bash
cargo test --lib connector::iceberg::scan_planner connector::starrocks::table::scan_planner connector::scan_planning_registry_tests sql::codegen::fragment_builder::tests sql::codegen::nodes::tests
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

Expected: all pass. If an unrelated pre-existing failure appears, record the failure name and rerun the focused tests from Step 3-4. Do NOT attempt to fix unrelated failures in this task.

- [ ] **Step 6: Optional commit for validation-only fixes**

If validation required any small fixes (cargo fmt diff in-scope, a missed test fixture update, etc.):

```bash
git add <changed-files>
git commit -m "fix(iceberg): address to_thrift_scan migration validation"
```

If no fixes were needed, do not create an empty commit.

## Follow-on Plans

After this plan lands:

1. Cache the `ThriftScanPlan` so `to_thrift_scan` is invoked exactly once per scan instead of twice. Most natural shape: add `cached_thrift_plan: Option<ThriftScanPlan>` to `PlannedScanTable`, populated by `visit_scan` at the same time as `planned_scan`; `build_scan_node` and `build_exec_params_multi` read from the cache. This change widens `PlannedScanTable`'s test-fixture surface, so it is deferred to its own slice.
2. Migrate `IcebergMetadataTable` and `IcebergDeltaTable` through the connector (different handle/split semantics — `serialized_table` and change-file enumeration).
3. Move `default_plan_node` and `expr_compiler::int_literal_node` into a connector-accessible module (e.g., `src/connector/codegen_helpers.rs`) so the connector layer does not reach into `crate::sql::codegen::nodes` / `crate::sql::codegen::expr_compiler`.
4. Migrate dict-slot patching (`lake_scan_node.dict_string_id_to_int_ids`) into the StarRocks connector via Stage 4's `SupportsDictionary` capability.
5. Encapsulate `change_op_slot` / `cloud_properties` into a connector-specific `ConnectorThriftPayload` once a second HDFS-style connector (Paimon / Delta) ships.
6. Stage 5 cleanup: delete `ScanSource::IcebergDataFiles.files` (after Stage 4 capabilities work removes the remaining read sites in optimizer / planner / dictionary / explain).
