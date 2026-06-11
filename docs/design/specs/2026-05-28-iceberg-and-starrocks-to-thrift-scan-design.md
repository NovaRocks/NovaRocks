# Iceberg & StarRocks `to_thrift_scan`: TPlanNode + scan-range generation via connector

## Background

Parent spec: `docs/design/specs/2026-05-28-connector-first-standalone-scan-design.md` (Stage 3).
Previous slice: `docs/design/plans/2026-05-28-iceberg-codegen-via-connector-begin-scan.md` (merged as PR #202).

After the previous slice landed, `IcebergConnectorScanPlanner` is registered and `visit_scan` calls `begin_scan` / `plan_splits` for `ScanSource::IcebergDataFiles`. But:

- `IcebergConnectorScanPlanner::to_thrift_scan` is a stub that returns an error.
- `StarRocksTableScanPlanner::to_thrift_scan` only returns `scan_ranges`; `node` is always `None`.
- TPlanNode generation (`TLakeScanNode` for StarRocks, `THdfsScanNode` for IcebergDataFiles) and HDFS scan-range generation (`build_hdfs_scan_range_params_for_file`) still live in `src/sql/codegen/nodes.rs`.

This slice migrates both the TPlanNode and scan-range generation for `IcebergDataFiles` and `ScanSource::StarRocks` into their respective `to_thrift_scan` implementations. After this slice, `nodes.rs::build_scan_node` and `build_exec_params_multi` become thin wrappers that delegate to the connector planner whenever `resolved.planned_scan` is populated.

`IcebergMetadataTable` and `IcebergDeltaTable` continue to use the existing placeholder-range / dedicated-node codegen path. Their migration is a later slice.

## Goal

1. `IcebergConnectorScanPlanner::to_thrift_scan` returns `ThriftScanPlan { node: Some(THdfsScanNode), scan_ranges }` for `IcebergDataFiles`.
2. `StarRocksTableScanPlanner::to_thrift_scan` returns `ThriftScanPlan { node: Some(TLakeScanNode), scan_ranges }` for `ScanSource::StarRocks`.
3. `nodes.rs::build_scan_node` and `build_exec_params_multi` route StarRocks and IcebergDataFiles scans through the registered connector planner.
4. HDFS scan-range generation helpers (`build_hdfs_scan_range_params_for_file`, `plan_hdfs_file_splits`, `validate_iceberg_delete_apply_cost`, `build_hdfs_scan_range_params`) move into the Iceberg connector module.
5. The Iceberg-specific code path inside `build_hdfs_scan_node` is removed; the function remains in `nodes.rs` only to serve `IcebergMetadataTable`.

## Non-goals

- Migrating `IcebergMetadataTable` or `IcebergDeltaTable` through the connector.
- Deleting `ScanSource::IcebergDataFiles.files`. The optimizer / planner / query_prep / dictionary / explain still read it.
- Hooking optimizer capabilities (`apply_projection`, `apply_predicate`, `estimate_statistics`, dictionary, explain). That is Stage 4.
- Migrating dict-slot patching (`lake_scan_node.dict_string_id_to_int_ids`). It stays as a post-`build_scan_node` patch in `visit_scan`.
- Migrating min-max conjunct derivation (`scan_file_min_max_predicates`). That stays in codegen; the result is passed to `to_thrift_scan` via context.

## `ThriftScanContext` shape

Current shape:

```rust
pub(crate) struct ThriftScanContext {
    pub(crate) database: String,
    pub(crate) table: String,
}
```

New shape:

```rust
pub(crate) struct ThriftScanContext {
    // Shared core: every TPlanNode needs these
    pub(crate) database: String,
    pub(crate) table: String,
    pub(crate) node_id: i32,
    pub(crate) scan_tuple_id: types::TTupleId,
    pub(crate) conjuncts: Vec<exprs::TExpr>,

    // Iceberg-specific per-query state (flat for now; will be encapsulated
    // into a connector-specific payload when a second HDFS-style connector
    // — Paimon / Delta etc. — appears)
    pub(crate) min_max_predicates: Vec<MinMaxPredicate>,
    pub(crate) change_op_slot: Option<types::TSlotId>,
    pub(crate) cloud_properties: BTreeMap<String, String>,
}
```

Rationale:
- Flat fields keep the trait simple.
- `THdfsScanNode.min_max_conjuncts` and `THdfsScanNode.conjuncts` are populated from the same `conjuncts` field in the current `build_hdfs_scan_node`, so no separate thrift-form `min_max_conjuncts` field is needed in the context.
- `min_max_predicates: Vec<MinMaxPredicate>` carries the already-parsed form used by per-file pruning (`file_may_satisfy_min_max`). Codegen derives it via `scan_file_min_max_predicates(planned)` and passes the result; the planner does not re-parse. `MinMaxPredicate` is already a `pub` type in `src/common/min_max_predicate.rs`.
- `change_op_slot` and `cloud_properties` are Iceberg-specific. Accepted as temporary coupling; revisit when a second HDFS-style connector arrives.
- StarRocks `to_thrift_scan` reads only the shared core; it ignores the Iceberg-specific fields.

## `IcebergScanHandle` extension

`build_hdfs_scan_node` currently reads `resolved.table.columns` to fill the `column_names` of `THdfsScanNode`. The connector planner does not have access to `ResolvedTable`. Two routes considered:

- (a) Add `column_names: Vec<String>` to `ThriftScanContext`.
- (b) Capture the column list in `IcebergScanHandle` at `begin_scan` time.

This slice picks **(b)**. It is more aligned with the Stage 4 capabilities direction: `apply_projection` will eventually mutate the projected columns on the scan handle. Capturing the initial column list at `begin_scan` is the natural first step.

Changes:

```rust
pub(crate) struct IcebergTableHandle {
    // existing fields …
    pub(crate) column_names: Vec<String>,
}
```

`IcebergConnectorScanPlanner::table_handle_from_source` gains a `column_names: Vec<String>` parameter. `visit_scan` passes `op.table.columns.iter().map(|c| c.name.clone()).collect()`.

`StarRocksScanHandle` does not need an analogous change in this slice; `TLakeScanNode` does not use a `column_names` field. If a future StarRocks-side feature needs the column list, that change is separate.

## Iceberg `to_thrift_scan` (new behaviour)

```rust
fn to_thrift_scan(
    &self,
    scan: &ScanHandle,
    splits: &[Split],
    ctx: ThriftScanContext,
) -> Result<ThriftScanPlan, String> {
    validate_split_connectors(scan, splits)?;
    let scan_handle = iceberg_scan_handle(scan)?;

    // 1. Per-file scan ranges (min/max pruning applied here)
    let scan_ranges = build_iceberg_scan_ranges(
        splits,
        &ctx.min_max_predicates,
        ctx.change_op_slot,
    )?;

    // 2. THdfsScanNode
    let node = build_iceberg_hdfs_scan_node(scan_handle, &ctx)?;

    Ok(ThriftScanPlan { node: Some(node), scan_ranges })
}
```

`build_iceberg_scan_ranges` and `build_iceberg_hdfs_scan_node` are new `pub(crate)` helpers inside the Iceberg connector module. The body of `build_iceberg_scan_ranges` is essentially the current `build_exec_params_multi`'s `IcebergDataFiles` arm body, minus the `resolved` access. `build_iceberg_hdfs_scan_node` is the current `build_hdfs_scan_node`'s IcebergDataFiles-side logic (cloud_config from `ctx.cloud_properties`, column_names from `scan_handle.table.column_names`, table name from `scan_handle.table.table`).

## StarRocks `to_thrift_scan` (new behaviour)

```rust
fn to_thrift_scan(
    &self,
    scan: &ScanHandle,
    splits: &[Split],
    ctx: ThriftScanContext,
) -> Result<ThriftScanPlan, String> {
    validate_split_connectors(scan, splits)?;
    let scan_handle = starrocks_scan_handle(scan)?;
    let scan_ranges = build_starrocks_scan_ranges(scan_handle, splits, &ctx)?;
    let node = build_starrocks_lake_scan_node(scan_handle, &ctx);
    Ok(ThriftScanPlan { node: Some(node), scan_ranges })
}
```

`build_starrocks_scan_ranges` is the current loop body of `to_thrift_scan`. `build_starrocks_lake_scan_node` is the current `build_lake_scan_node` from `nodes.rs`, minus the `resolved.planned_scan` lookup (the planner already has the scan handle).

## `nodes.rs` simplification

`build_scan_node` gains a `connectors: &ConnectorRegistry` parameter and routes through the planner when `resolved.planned_scan` is populated and the source is StarRocks or IcebergDataFiles:

```rust
pub(crate) fn build_scan_node(
    connectors: &ConnectorRegistry,
    node_id: i32,
    scan_tuple_id: i32,
    resolved: &ResolvedTable,
    conjuncts: Vec<exprs::TExpr>,
    min_max_predicates: Vec<MinMaxPredicate>,
    change_op_slot: Option<types::TSlotId>,
) -> Result<plan_nodes::TPlanNode, String> {
    if let Some(planned) = resolved.planned_scan.as_ref() {
        let planner_name = match &resolved.table.source {
            ScanSource::StarRocks { .. } => Some("starrocks"),
            ScanSource::IcebergDataFiles { .. } => Some("iceberg"),
            _ => None,
        };
        if let Some(name) = planner_name {
            let planner = connectors.scan_planner(name)?;
            let ctx = thrift_scan_context_for(
                resolved, node_id, scan_tuple_id,
                conjuncts.clone(), min_max_predicates, change_op_slot,
            );
            let plan = planner.to_thrift_scan(&planned.scan, &planned.splits, ctx)?;
            return plan.node.ok_or_else(|| format!(
                "{name} to_thrift_scan returned no node for {}.{}",
                resolved.database, resolved.table.name
            ));
        }
    }
    // Existing path for IcebergMetadataTable / IcebergDeltaTable
    if matches!(resolved.table.source, ScanSource::IcebergDeltaTable { .. }) {
        return Ok(build_iceberg_delta_scan_node(node_id, scan_tuple_id, resolved, conjuncts));
    }
    Ok(build_hdfs_scan_node(node_id, scan_tuple_id, resolved, conjuncts))
}
```

`build_exec_params_multi` for IcebergDataFiles routes the same way; instead of building scan-ranges locally, it asks the planner via `to_thrift_scan` and uses `plan.scan_ranges`. `IcebergMetadataTable` arm still produces a placeholder range locally.

`build_hdfs_scan_node` in `nodes.rs` keeps only the IcebergMetadataTable branch. The IcebergDataFiles branch (and the dual-source `cloud_properties` match) is removed.

`build_lake_scan_node` in `nodes.rs` is deleted; its body moves to the StarRocks connector module.

`build_hdfs_scan_range_params_for_file`, `plan_hdfs_file_splits`, `validate_iceberg_delete_apply_cost`, `build_hdfs_scan_range_params`, and the related constants (`ICEBERG_SCAN_SPLIT_TARGET_BYTES`, `ICEBERG_DELETE_APPLY_MAX_FILES_PER_DATA_FILE`, `ICEBERG_DELETE_APPLY_MAX_BYTES_PER_DATA_FILE`) move into the Iceberg connector module.

## File structure changes

- Modify: `src/connector/scan_planning.rs` (extend `ThriftScanContext`)
- Modify: `src/connector/iceberg/scan_planner.rs` (extend `IcebergTableHandle`, real `to_thrift_scan`, add codegen helpers)
- Possible new file: `src/connector/iceberg/thrift_codegen.rs` if `scan_planner.rs` grows beyond ~400 lines; otherwise inline.
- Modify: `src/connector/starrocks/table/scan_planner.rs` (real `to_thrift_scan` returning `node: Some(...)`)
- Modify: `src/sql/codegen/nodes.rs` (route through connector for planned scans; remove `build_lake_scan_node`; strip IcebergDataFiles from `build_hdfs_scan_node`; delete moved helpers)
- Modify: `src/sql/codegen/fragment_builder.rs` (`build_scan_node` call gains `&self.connectors`; tests adjusted)
- Tests: counting-wrapper tests assert `to_thrift_scan` is invoked once for both planners; nodes.rs fixtures adapted.

## Atomicity

The migration of each connector's node generation is atomic across `nodes.rs::build_scan_node`, the connector's `to_thrift_scan`, and the test fixtures that call `build_scan_node`. The intermediate state would not compile. This will be at least one atomic task in the implementation plan (similar shape to Task 4 of the previous slice).

The two connectors (StarRocks and Iceberg) can land sequentially within the slice: do StarRocks first (smaller — TLakeScanNode is mostly default-valued), then Iceberg (richer — cloud_config, column_names, min/max). This sequencing reduces the size of each atomic switch.

## Test coverage

- `IcebergConnectorScanPlanner::to_thrift_scan` unit test: build a scan with one file, assert `node = Some(THdfsScanNode { … })` with expected `column_names`, `cloud_config`, and `scan_ranges.len() == 1`.
- `StarRocksTableScanPlanner::to_thrift_scan` unit test: assert `node = Some(TLakeScanNode { tuple_id, … })` and `scan_ranges` content unchanged from current.
- End-to-end counting tests (`fragment_builder.rs::tests`):
  - Extend the existing `CountingScanPlanner` / `CountingIcebergScanPlanner` wrappers with a `to_thrift_scan` counter.
  - Assert both wrappers' `to_thrift_scan` is invoked exactly once per scan via the same `PlanFragmentBuilder::build` path that already exercises `begin_scan` / `plan_splits`.
- Existing `nodes.rs::tests` IcebergDataFiles fixtures (`physical_change_op_column_does_not_emit_extended_columns`, `metadata_change_op_column_emits_extended_columns`) keep the same assertions; they will need to thread a `ConnectorRegistry` (via a helper) into `build_scan_node`.

## Risks

1. **`to_thrift_scan` invoked twice per scan**: After this slice, `build_scan_node` calls `to_thrift_scan` to get the `TPlanNode` (discarding `scan_ranges`), and `build_exec_params_multi` calls `to_thrift_scan` again to get `scan_ranges` (discarding `node`). Both paths receive a stateless planner; the work is bounded by `O(splits)` and the planner has no side effects (no logging, no I/O). The double call is wasteful but functionally correct. A follow-up slice will cache the `ThriftScanPlan` (e.g., on `PlannedScanTable`) so it is computed once; that cache change is deferred because it widens `PlannedScanTable`'s shape and would force every test fixture in `nodes.rs::tests` to be updated.
2. **TLakeScanNode default-field drift**: `TLakeScanNode` has many optional fields currently all `None`. If a downstream consumer ever needs to set one of them, the `ThriftScanContext` will need another field. Acceptable risk; current behaviour is `None` for all.
3. **`cloud_properties` duplicated source**: After this slice, `cloud_properties` lives in both `ScanSource::IcebergDataFiles.cloud_properties` (read by the IcebergMetadataTable arm of `build_hdfs_scan_node`) and `ThriftScanContext.cloud_properties` (read by the Iceberg connector for IcebergDataFiles). They are populated from the same place in `visit_scan`. Cleanup happens in Stage 5 when `ScanSource` is deleted.
4. **dict-slot patching after `build_scan_node`**: `visit_scan` patches `lake_scan_node.dict_string_id_to_int_ids` after the node is built. The patching stays out-of-scope; we just need to ensure `build_scan_node` returns the `TLakeScanNode` with the same shape so the patch site continues to work.
5. **Test fixture churn**: Several `nodes.rs` and `fragment_builder.rs` tests will need a `ConnectorRegistry` argument or to use existing mock helpers. Each change is mechanical but adds up.

## Out-of-scope follow-ups

- Cache `ThriftScanPlan` on `PlannedScanTable` (or restructure `build_scan_node` to return both node and ranges) so `to_thrift_scan` is invoked exactly once per scan.
- Migrate `IcebergMetadataTable` / `IcebergDeltaTable` through the connector (next Stage 3 slice).
- Encapsulate `change_op_slot` / `cloud_properties` into a connector-specific `ConnectorThriftPayload` once a second HDFS-style connector ships.
- Move dict-slot patching into the StarRocks connector (Stage 4 capability).
- Delete `ScanSource::IcebergDataFiles.files` field (Stage 5, after Stage 4 capabilities).
