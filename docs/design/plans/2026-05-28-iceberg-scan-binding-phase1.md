# Iceberg Scan Binding Phase 1 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Implement phase 1 of the Iceberg IMV rewrite cutover: bind `Delta(Scan)` and `Version(Scan)` to refresh snapshot windows inside the IMV pipeline, without switching refresh execution to the rewrite outcome.

**Architecture:** Add a focused IMV scan-binding module under `src/sql/optimizer/rewrite/imv/`. The rule consumes `ImvDelta(Scan)` and `ImvVersion(Scan)` markers by using `IcebergMvRewriteContext` to resolve the base table's previous snapshot and pinned refresh snapshot. Delta scans are lowered to the existing `ScanSource::IcebergDeltaTable`; version scans use a new refresh-only `ScanSource::IcebergVersionTable` guard that is inspectable in rewrite tests but intentionally not executable during phase 1.

**Tech Stack:** Rust, existing logical rewrite framework, `IcebergMvRewriteContext`, `ScanSource`, `cargo test --lib`.

---

## Scope

This plan implements only umbrella spec phase 1. It does not implement action column propagation, aggregate state rewrite, join delta algebra, UNION ALL rewrite, or refresh execution cutover.

The implementation must preserve current refresh behavior: `try_run_imv_rewrite_pipeline` may observe a successful outcome for simple scan fixtures, but production refresh still keeps the existing hand-built path.

## File Structure

- Create `src/sql/optimizer/rewrite/imv/scan_binding.rs`
  - Owns `ImvSnapshotWindow`, `ImvVersionRole`, scan-source mutation helpers, and `BindIcebergScanRule`.
  - Contains unit tests for snapshot-window lookup and marker consumption.
- Modify `src/sql/optimizer/rewrite/imv/mod.rs`
  - Exposes the new `scan_binding` module.
- Modify `src/sql/optimizer/rewrite/imv/marker.rs`
  - Replaces placeholder `ImvVersionRef` with a concrete `role: ImvVersionRole`.
- Modify `src/sql/optimizer/rewrite/imv/pipeline.rs`
  - Adds `imv-scan-binding` stage after `imv-delta-marker`.
- Modify `src/sql/optimizer/rewrite/imv/entrypoint.rs`
  - Updates stage-name tests and adds a simple `Delta(Scan)` end-to-end success test.
- Modify `src/sql/catalog.rs`
  - Adds `ScanSource::IcebergVersionTable { table, snapshot_id }` as a refresh-only logical placeholder.
- Modify scan-source exhaustive matches:
  - `src/sql/codegen/descriptors.rs`
  - `src/sql/codegen/fragment_builder.rs`
  - `src/sql/codegen/nodes.rs`
  - `src/sql/explain.rs`
  - `src/sql/planner/mod.rs`
  - `src/sql/optimizer/rewrite/rules/ukfk.rs`
  - `src/sql/optimizer/rewrite/rules/join_reorder/reorder.rs`
  - `src/engine/dictionary/mod.rs`
- Test commands:
  - `cargo test --lib scan_binding`
  - `cargo test --lib imv`
  - `cargo test --lib`

---

### Task 1: Add Concrete Version Marker Types

**Files:**
- Modify: `src/sql/optimizer/rewrite/imv/marker.rs`
- Create in Task 2: `src/sql/optimizer/rewrite/imv/scan_binding.rs`

- [ ] **Step 1: Write the failing compile target**

Edit `src/sql/optimizer/rewrite/imv/marker.rs` so `ImvVersionRef` refers to a type that does not exist yet:

```rust
/// Snapshot window descriptor used by `ImvVersionNode`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct ImvVersionRef {
    pub(crate) role: crate::sql::optimizer::rewrite::imv::scan_binding::ImvVersionRole,
}

impl ImvVersionRef {
    pub(crate) fn from_snapshot() -> Self {
        Self {
            role: crate::sql::optimizer::rewrite::imv::scan_binding::ImvVersionRole::From,
        }
    }

    pub(crate) fn to_snapshot() -> Self {
        Self {
            role: crate::sql::optimizer::rewrite::imv::scan_binding::ImvVersionRole::To,
        }
    }
}

impl Default for ImvVersionRef {
    fn default() -> Self {
        Self::to_snapshot()
    }
}
```

- [ ] **Step 2: Run compile to verify the expected failure**

Run:

```bash
cargo test --lib marker --no-run
```

Expected: compile fails with an unresolved import/path mentioning `imv::scan_binding::ImvVersionRole`.

- [ ] **Step 3: Keep this change uncommitted**

Do not commit after this task. Task 2 creates the missing module and turns the compile failure into passing tests.

---

### Task 2: Add Scan Binding Data Model and Window Resolution

**Files:**
- Create: `src/sql/optimizer/rewrite/imv/scan_binding.rs`
- Modify: `src/sql/optimizer/rewrite/imv/mod.rs`

- [ ] **Step 1: Expose the module**

Add this line to `src/sql/optimizer/rewrite/imv/mod.rs`:

```rust
pub(crate) mod scan_binding;
```

- [ ] **Step 2: Create `scan_binding.rs` with data types, window lookup, and tests**

Create `src/sql/optimizer/rewrite/imv/scan_binding.rs` with this content:

```rust
//! Iceberg IMV scan binding.
//!
//! This module consumes refresh-only IMV scan markers by resolving snapshot
//! windows from `IcebergMvRewriteContext`. It must never fall back to the
//! current Iceberg snapshot: the refresh pin is the read upper bound.

use crate::connector::starrocks::table::model::IcebergTableRef;
use crate::engine::mv::refresh_context::IcebergMvRewriteContext;
use crate::sql::catalog::{IcebergTableInfo, ScanSource};
use crate::sql::optimizer::rewrite::context::RewriteContext;
use crate::sql::optimizer::rewrite::imv::annotation::ImvExtension;
use crate::sql::optimizer::rewrite::imv::marker::{ImvDeltaNode, ImvVersionNode};
use crate::sql::optimizer::rewrite::phase::RewritePhase;
use crate::sql::optimizer::rewrite::result::RewriteResult;
use crate::sql::optimizer::rewrite::rule::{LogicalRewriteRule, RewriteTraversal};
use crate::sql::planner::plan::{LogicalPlan, ScanNode};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum ImvVersionRole {
    From,
    To,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct ImvSnapshotWindow {
    pub(crate) base_fqn: String,
    pub(crate) from_snapshot_id: i64,
    pub(crate) to_snapshot_id: i64,
    pub(crate) table_uuid: String,
}

pub(crate) struct BindIcebergScanRule;

impl LogicalRewriteRule for BindIcebergScanRule {
    fn name(&self) -> &'static str {
        "BindIcebergScan"
    }

    fn phase(&self) -> RewritePhase {
        RewritePhase::SemanticRewrite
    }

    fn traversal(&self) -> RewriteTraversal {
        RewriteTraversal::BottomUp
    }

    fn matches(&self, plan: &LogicalPlan, _ctx: &RewriteContext) -> bool {
        matches!(
            plan,
            LogicalPlan::ImvDelta(ImvDeltaNode { input, .. })
                if matches!(input.as_ref(), LogicalPlan::Scan(_))
        ) || matches!(
            plan,
            LogicalPlan::ImvVersion(ImvVersionNode { input, .. })
                if matches!(input.as_ref(), LogicalPlan::Scan(_))
        )
    }

    fn apply(&self, plan: LogicalPlan, ctx: &mut RewriteContext) -> Result<RewriteResult, String> {
        let ext = ctx
            .extension::<ImvExtension>()
            .ok_or_else(|| "BindIcebergScan requires ImvExtension in RewriteContext".to_string())?;
        match plan {
            LogicalPlan::ImvDelta(node) => {
                let LogicalPlan::Scan(scan) = *node.input else {
                    return Ok(RewriteResult::Unchanged);
                };
                let bound = bind_delta_scan(scan, &ext.mv_ctx)?;
                Ok(RewriteResult::Changed(LogicalPlan::Scan(bound)))
            }
            LogicalPlan::ImvVersion(node) => {
                let LogicalPlan::Scan(scan) = *node.input else {
                    return Ok(RewriteResult::Unchanged);
                };
                let bound = bind_version_scan(scan, &ext.mv_ctx, node.version_ref.role)?;
                Ok(RewriteResult::Changed(LogicalPlan::Scan(bound)))
            }
            other => Ok(RewriteResult::Unchanged),
        }
    }
}

fn bind_delta_scan(
    mut scan: ScanNode,
    mv_ctx: &IcebergMvRewriteContext,
) -> Result<ScanNode, String> {
    let table = iceberg_table_info_from_source(&scan.table.source)?.clone();
    let window = resolve_snapshot_window(mv_ctx, &table)?;
    scan.table.source = ScanSource::IcebergDeltaTable {
        table,
        from_snapshot_id: window.from_snapshot_id,
        to_snapshot_id: window.to_snapshot_id,
    };
    Ok(scan)
}

fn bind_version_scan(
    mut scan: ScanNode,
    mv_ctx: &IcebergMvRewriteContext,
    role: ImvVersionRole,
) -> Result<ScanNode, String> {
    let table = iceberg_table_info_from_source(&scan.table.source)?.clone();
    let window = resolve_snapshot_window(mv_ctx, &table)?;
    let snapshot_id = match role {
        ImvVersionRole::From => window.from_snapshot_id,
        ImvVersionRole::To => window.to_snapshot_id,
    };
    scan.table.source = ScanSource::IcebergVersionTable { table, snapshot_id };
    Ok(scan)
}

fn iceberg_table_info_from_source(source: &ScanSource) -> Result<&IcebergTableInfo, String> {
    match source {
        ScanSource::IcebergDataFiles { table, .. }
        | ScanSource::IcebergMetadataTable { table, .. }
        | ScanSource::IcebergDeltaTable { table, .. }
        | ScanSource::IcebergVersionTable { table, .. } => Ok(table),
        ScanSource::StarRocks { .. } => {
            Err("BindIcebergScan only supports Iceberg scan sources".to_string())
        }
    }
}

fn resolve_snapshot_window(
    mv_ctx: &IcebergMvRewriteContext,
    table: &IcebergTableInfo,
) -> Result<ImvSnapshotWindow, String> {
    let base_ref = find_base_ref(mv_ctx, table)?;
    let base_fqn = base_ref.fqn();
    let from_snapshot_id = mv_ctx.previous_snapshot_ids.get(&base_fqn).copied().ok_or_else(|| {
        format!(
            "IMV scan binding requires previous snapshot for base {base_fqn}; first refresh/full rebuild must not enter incremental scan binding"
        )
    })?;
    let to_snapshot_id = mv_ctx.pin.get(base_ref).ok_or_else(|| {
        format!("IMV scan binding refresh pin missing snapshot for base {base_fqn}")
    })?;
    let pin_uuid = mv_ctx.pin.uuid(base_ref).ok_or_else(|| {
        format!("IMV scan binding refresh pin missing uuid for base {base_fqn}")
    })?;
    if let Some(table_uuid) = table.table_uuid.as_deref()
        && table_uuid != pin_uuid
    {
        return Err(format!(
            "IMV scan binding base table uuid mismatch for {base_fqn}: plan has {table_uuid}, pin has {pin_uuid}"
        ));
    }
    Ok(ImvSnapshotWindow {
        base_fqn,
        from_snapshot_id,
        to_snapshot_id,
        table_uuid: pin_uuid.to_string(),
    })
}

fn find_base_ref<'a>(
    mv_ctx: &'a IcebergMvRewriteContext,
    table: &IcebergTableInfo,
) -> Result<&'a IcebergTableRef, String> {
    mv_ctx
        .base_refs
        .iter()
        .find(|base| {
            base.catalog.eq_ignore_ascii_case(&table.catalog)
                && base.namespace.eq_ignore_ascii_case(&table.namespace)
                && base.table.eq_ignore_ascii_case(&table.table)
        })
        .ok_or_else(|| {
            format!(
                "IMV scan binding base {}.{}.{} is not part of MV refresh context",
                table.catalog, table.namespace, table.table
            )
        })
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use arrow::datatypes::DataType;

    use super::*;
    use crate::engine::mv::refresh_context::tests_support::dummy_rewrite_context;
    use crate::sql::analysis::OutputColumn;
    use crate::sql::catalog::{ColumnDef, IcebergSchemaDef, IcebergTableInfo, TableDef};
    use crate::sql::column_id::ColumnId;

    fn iceberg_table_info(uuid: Option<&str>) -> IcebergTableInfo {
        IcebergTableInfo {
            catalog: "ice".to_string(),
            namespace: "db".to_string(),
            table: "b".to_string(),
            table_uuid: uuid.map(str::to_string),
            current_snapshot_id: Some(22),
            schema_id: 7,
            location: "file:///tmp/ice/db/b".to_string(),
            schema: IcebergSchemaDef { fields: Vec::new() },
            serialized_metadata: None,
        }
    }

    fn iceberg_scan(uuid: Option<&str>) -> ScanNode {
        let column = ColumnDef {
            name: "k".to_string(),
            data_type: DataType::Int64,
            nullable: false,
            write_default: None,
            logical_type: None,
        };
        ScanNode {
            database: "db".to_string(),
            table: TableDef {
                name: "b".to_string(),
                columns: vec![column],
                iceberg_row_lineage_metadata_columns: Vec::new(),
                source: ScanSource::IcebergDataFiles {
                    table: iceberg_table_info(uuid),
                    files: Vec::new(),
                    cloud_properties: BTreeMap::new(),
                },
            },
            alias: None,
            columns: vec![OutputColumn {
                column_id: ColumnId(1),
                name: "k".to_string(),
                data_type: DataType::Int64,
                nullable: false,
            }],
            predicates: Vec::new(),
            required_columns: None,
            dict_columns: Vec::new(),
        }
    }

    #[test]
    fn resolve_window_uses_previous_snapshot_and_refresh_pin() {
        let ctx = dummy_rewrite_context();
        let window = resolve_snapshot_window(&ctx, &iceberg_table_info(Some("uuid-b")))
            .expect("window should resolve");
        assert_eq!(window.base_fqn, "ice.db.b");
        assert_eq!(window.from_snapshot_id, 11);
        assert_eq!(window.to_snapshot_id, 22);
        assert_eq!(window.table_uuid, "uuid-b");
    }

    #[test]
    fn resolve_window_rejects_uuid_mismatch() {
        let ctx = dummy_rewrite_context();
        let err = resolve_snapshot_window(&ctx, &iceberg_table_info(Some("other-uuid")))
            .expect_err("uuid mismatch must fail");
        assert!(err.contains("uuid mismatch"), "unexpected error: {err}");
        assert!(err.contains("ice.db.b"), "unexpected error: {err}");
    }

    #[test]
    fn bind_delta_scan_replaces_source_with_iceberg_delta_table() {
        let ctx = dummy_rewrite_context();
        let bound = bind_delta_scan(iceberg_scan(Some("uuid-b")), &ctx)
            .expect("delta scan should bind");
        match bound.table.source {
            ScanSource::IcebergDeltaTable {
                from_snapshot_id,
                to_snapshot_id,
                ..
            } => {
                assert_eq!(from_snapshot_id, 11);
                assert_eq!(to_snapshot_id, 22);
            }
            other => panic!("expected IcebergDeltaTable, got {other:?}"),
        }
    }

    #[test]
    fn bind_version_scan_uses_from_snapshot() {
        let ctx = dummy_rewrite_context();
        let bound = bind_version_scan(iceberg_scan(Some("uuid-b")), &ctx, ImvVersionRole::From)
            .expect("version scan should bind");
        match bound.table.source {
            ScanSource::IcebergVersionTable { snapshot_id, .. } => {
                assert_eq!(snapshot_id, 11);
            }
            other => panic!("expected IcebergVersionTable, got {other:?}"),
        }
    }

    #[test]
    fn bind_version_scan_uses_to_snapshot() {
        let ctx = dummy_rewrite_context();
        let bound = bind_version_scan(iceberg_scan(Some("uuid-b")), &ctx, ImvVersionRole::To)
            .expect("version scan should bind");
        match bound.table.source {
            ScanSource::IcebergVersionTable { snapshot_id, .. } => {
                assert_eq!(snapshot_id, 22);
            }
            other => panic!("expected IcebergVersionTable, got {other:?}"),
        }
    }
}
```

- [ ] **Step 3: Run the focused tests**

Run:

```bash
cargo test --lib scan_binding
```

Expected: compile fails because `ScanSource::IcebergVersionTable` does not exist yet.

- [ ] **Step 4: Commit only after Task 3 passes**

Do not commit now. Task 3 adds the catalog variant and match arms needed for compilation.

---

### Task 3: Add Refresh-Only `IcebergVersionTable` Scan Source and Match Arms

**Files:**
- Modify: `src/sql/catalog.rs`
- Modify: `src/sql/codegen/descriptors.rs`
- Modify: `src/sql/codegen/fragment_builder.rs`
- Modify: `src/sql/codegen/nodes.rs`
- Modify: `src/sql/explain.rs`
- Modify: `src/sql/planner/mod.rs`
- Modify: `src/sql/optimizer/rewrite/rules/ukfk.rs`
- Modify: `src/sql/optimizer/rewrite/rules/join_reorder/reorder.rs`
- Modify: `src/engine/dictionary/mod.rs`

- [ ] **Step 1: Add the source variant**

In `src/sql/catalog.rs`, add this variant immediately after `IcebergDeltaTable`:

```rust
    /// Refresh-only pinned Iceberg version scan placeholder. Produced by the
    /// IMV scan-binding rule for `Version(IcebergScan)`. Phase 1 keeps this
    /// variant non-executable: it is inspectable in rewrite tests and guarded
    /// at scan-range construction so it cannot silently read current snapshot.
    IcebergVersionTable {
        table: IcebergTableInfo,
        snapshot_id: i64,
    },
```

- [ ] **Step 2: Update Iceberg table-info helper matches**

Update every `iceberg_table_info(source)` helper to include `IcebergVersionTable`.

Use this match shape in `src/sql/codegen/descriptors.rs`,
`src/sql/codegen/fragment_builder.rs`, `src/sql/planner/mod.rs`, and
`src/sql/optimizer/rewrite/rules/ukfk.rs`:

```rust
    match source {
        crate::sql::catalog::ScanSource::IcebergDataFiles { table, .. }
        | crate::sql::catalog::ScanSource::IcebergMetadataTable { table, .. }
        | crate::sql::catalog::ScanSource::IcebergDeltaTable { table, .. }
        | crate::sql::catalog::ScanSource::IcebergVersionTable { table, .. } => Some(table),
        crate::sql::catalog::ScanSource::StarRocks { .. } => None,
    }
```

- [ ] **Step 3: Add the non-executable codegen guard**

In `src/sql/codegen/nodes.rs`, update `build_exec_params_multi` so the `match &resolved.table.source` arm includes:

```rust
                ScanSource::IcebergVersionTable { table, snapshot_id } => {
                    return Err(format!(
                        "IMV version scan {}.{}.{} at snapshot {} reached scan-range construction before execution cutover",
                        table.catalog, table.namespace, table.table, snapshot_id
                    ));
                }
```

Keep `build_scan_node` unchanged for this phase. The guard belongs in scan-range construction because that function already returns `Result`.

- [ ] **Step 4: Update explain capability guards**

In `src/sql/explain.rs`, treat `IcebergVersionTable` like `IcebergDeltaTable` for stats and decode hints:

```rust
        ScanSource::IcebergMetadataTable { .. } => false,
        ScanSource::IcebergDeltaTable { .. } | ScanSource::IcebergVersionTable { .. } => false,
```

For `scan_supports_min_max_stats`, use:

```rust
    match &table.source {
        ScanSource::IcebergDataFiles { .. } | ScanSource::StarRocks { .. } => {}
        ScanSource::IcebergMetadataTable { .. } => return false,
        ScanSource::IcebergDeltaTable { .. } | ScanSource::IcebergVersionTable { .. } => {
            return false;
        }
    }
```

- [ ] **Step 5: Update dictionary ownership handling**

In `src/engine/dictionary/mod.rs`, update the match that skips non-StarRocks scan sources:

```rust
            ScanSource::IcebergMetadataTable { .. }
            | ScanSource::IcebergDeltaTable { .. }
            | ScanSource::IcebergVersionTable { .. } => {
                return Ok(None);
            }
```

- [ ] **Step 6: Update join reorder cardinality heuristic**

In `src/sql/optimizer/rewrite/rules/join_reorder/reorder.rs`, treat the refresh-only version placeholder as tiny and non-statistical:

```rust
                ScanSource::IcebergDeltaTable { .. } | ScanSource::IcebergVersionTable { .. } => 1,
```

- [ ] **Step 7: Compile and run scan binding tests**

Run:

```bash
cargo test --lib scan_binding
```

Expected: tests compile and pass.

- [ ] **Step 8: Commit**

```bash
git add src/sql/catalog.rs src/sql/codegen/descriptors.rs src/sql/codegen/fragment_builder.rs src/sql/codegen/nodes.rs src/sql/explain.rs src/sql/planner/mod.rs src/sql/optimizer/rewrite/rules/ukfk.rs src/sql/optimizer/rewrite/rules/join_reorder/reorder.rs src/engine/dictionary/mod.rs src/sql/optimizer/rewrite/imv/mod.rs src/sql/optimizer/rewrite/imv/marker.rs src/sql/optimizer/rewrite/imv/scan_binding.rs
git commit -m "feat: add IMV scan binding model"
```

---

### Task 4: Register the Scan Binding Stage in the IMV Pipeline

**Files:**
- Modify: `src/sql/optimizer/rewrite/imv/pipeline.rs`
- Modify: `src/sql/optimizer/rewrite/imv/entrypoint.rs`

- [ ] **Step 1: Write failing stage-name expectation**

In `src/sql/optimizer/rewrite/imv/entrypoint.rs`, update `empty_pipeline_traces_all_four_stage_names` to expect five stage names and rename it:

```rust
    #[test]
    fn imv_pipeline_traces_scan_binding_stage_name() {
        let outcome = run_imv_rewrite(ImvRewriteInput {
            plan: empty_values_plan(),
            mv_ctx: dummy_mv_ctx(),
            disabled_rules: vec!["WrapRootInImvDelta".to_string()],
            deadline: None,
        })
        .expect("pipeline must succeed when wrap rule is disabled");

        assert_eq!(
            outcome.trace.stage_names(),
            vec![
                "imv-logical-normalize",
                "imv-delta-marker",
                "imv-scan-binding",
                "imv-marker-cleanup",
                "imv-validation",
            ]
        );
    }
```

- [ ] **Step 2: Run the focused test and verify failure**

Run:

```bash
cargo test --lib imv_pipeline_traces_scan_binding_stage_name
```

Expected: test fails because the pipeline still reports four stages.

- [ ] **Step 3: Register the stage and rule**

Modify `src/sql/optimizer/rewrite/imv/pipeline.rs`:

```rust
use crate::sql::optimizer::rewrite::imv::marker::{
    UnresolvedMarkerCheckRule, WrapRootInImvDeltaRule,
};
use crate::sql::optimizer::rewrite::imv::scan_binding::BindIcebergScanRule;
```

Then replace `build_imv_pipeline()` with:

```rust
pub(crate) fn build_imv_pipeline() -> RewritePipeline {
    RewritePipeline::from_stages(vec![
        RewriteStage::new(
            "imv-logical-normalize",
            RewritePhase::LogicalNormalize,
            Vec::new(),
        ),
        RewriteStage::new(
            "imv-delta-marker",
            RewritePhase::StructuralRewrite,
            vec![Box::new(WrapRootInImvDeltaRule::new()) as Box<dyn LogicalRewriteRule>],
        ),
        RewriteStage::new(
            "imv-scan-binding",
            RewritePhase::SemanticRewrite,
            vec![Box::new(BindIcebergScanRule) as Box<dyn LogicalRewriteRule>],
        ),
        RewriteStage::new(
            "imv-marker-cleanup",
            RewritePhase::SemanticRewrite,
            Vec::new(),
        ),
        RewriteStage::new(
            "imv-validation",
            RewritePhase::Validation,
            vec![Box::new(UnresolvedMarkerCheckRule) as Box<dyn LogicalRewriteRule>],
        ),
    ])
}
```

- [ ] **Step 4: Run the focused test and verify pass**

Run:

```bash
cargo test --lib imv_pipeline_traces_scan_binding_stage_name
```

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add src/sql/optimizer/rewrite/imv/pipeline.rs src/sql/optimizer/rewrite/imv/entrypoint.rs
git commit -m "feat: register IMV scan binding stage"
```

---

### Task 5: Add End-to-End Delta Scan Binding Tests

**Files:**
- Modify: `src/sql/optimizer/rewrite/imv/entrypoint.rs`

- [ ] **Step 1: Add test helpers**

Inside the `#[cfg(test)] mod tests` in `src/sql/optimizer/rewrite/imv/entrypoint.rs`, add these imports:

```rust
    use std::collections::BTreeMap;
    use arrow::datatypes::DataType;
    use crate::sql::analysis::OutputColumn;
    use crate::sql::catalog::{ColumnDef, IcebergSchemaDef, IcebergTableInfo, ScanSource, TableDef};
    use crate::sql::column_id::ColumnId;
    use crate::sql::planner::plan::ScanNode;
```

Add this helper near `empty_values_plan()`:

```rust
    fn iceberg_scan_plan() -> LogicalPlan {
        let column = ColumnDef {
            name: "k".to_string(),
            data_type: DataType::Int64,
            nullable: false,
            write_default: None,
            logical_type: None,
        };
        LogicalPlan::Scan(ScanNode {
            database: "db".to_string(),
            table: TableDef {
                name: "b".to_string(),
                columns: vec![column],
                iceberg_row_lineage_metadata_columns: Vec::new(),
                source: ScanSource::IcebergDataFiles {
                    table: IcebergTableInfo {
                        catalog: "ice".to_string(),
                        namespace: "db".to_string(),
                        table: "b".to_string(),
                        table_uuid: Some("uuid-b".to_string()),
                        current_snapshot_id: Some(22),
                        schema_id: 7,
                        location: "file:///tmp/ice/db/b".to_string(),
                        schema: IcebergSchemaDef { fields: Vec::new() },
                        serialized_metadata: None,
                    },
                    files: Vec::new(),
                    cloud_properties: BTreeMap::new(),
                },
            },
            alias: None,
            columns: vec![OutputColumn {
                column_id: ColumnId(1),
                name: "k".to_string(),
                data_type: DataType::Int64,
                nullable: false,
            }],
            predicates: Vec::new(),
            required_columns: None,
            dict_columns: Vec::new(),
        })
    }
```

- [ ] **Step 2: Add the passing delta binding test**

Add this test:

```rust
    #[test]
    fn imv_pipeline_binds_root_delta_scan() {
        let outcome = run_imv_rewrite(ImvRewriteInput {
            plan: iceberg_scan_plan(),
            mv_ctx: dummy_mv_ctx(),
            disabled_rules: Vec::new(),
            deadline: None,
        })
        .expect("Delta(Scan) must bind and pass validation");

        let LogicalPlan::Scan(scan) = outcome.plan else {
            panic!("expected scan outcome");
        };
        match scan.table.source {
            ScanSource::IcebergDeltaTable {
                from_snapshot_id,
                to_snapshot_id,
                ..
            } => {
                assert_eq!(from_snapshot_id, 11);
                assert_eq!(to_snapshot_id, 22);
            }
            other => panic!("expected IcebergDeltaTable, got {other:?}"),
        }
    }
```

- [ ] **Step 3: Run the focused test**

Run:

```bash
cargo test --lib imv_pipeline_binds_root_delta_scan
```

Expected: PASS.

- [ ] **Step 4: Re-run the existing marker leak tests**

Run:

```bash
cargo test --lib pr_beta_pipeline_runs_wrap_and_validation_against_plain_plan
cargo test --lib imv_pipeline_returns_err_on_plain_plan_in_pr_beta
```

Expected: both still PASS because non-scan plans remain unresolved and validation rejects them.

- [ ] **Step 5: Commit**

```bash
git add src/sql/optimizer/rewrite/imv/entrypoint.rs
git commit -m "test: cover IMV delta scan binding"
```

---

### Task 6: Add End-to-End Version Scan Binding Tests

**Files:**
- Modify: `src/sql/optimizer/rewrite/imv/entrypoint.rs`

- [ ] **Step 1: Add manual `ImvVersion` tests**

Add this import inside the test module if it is not already present:

```rust
    use crate::sql::optimizer::rewrite::imv::marker::{ImvVersionNode, ImvVersionRef};
```

Add these tests:

```rust
    #[test]
    fn imv_pipeline_binds_version_from_scan() {
        let plan = LogicalPlan::ImvVersion(ImvVersionNode {
            input: Box::new(iceberg_scan_plan()),
            version_ref: ImvVersionRef::from_snapshot(),
        });
        let outcome = run_imv_rewrite(ImvRewriteInput {
            plan,
            mv_ctx: dummy_mv_ctx(),
            disabled_rules: vec!["WrapRootInImvDelta".to_string()],
            deadline: None,
        })
        .expect("Version(Scan, From) must bind and pass validation");

        let LogicalPlan::Scan(scan) = outcome.plan else {
            panic!("expected scan outcome");
        };
        match scan.table.source {
            ScanSource::IcebergVersionTable { snapshot_id, .. } => {
                assert_eq!(snapshot_id, 11);
            }
            other => panic!("expected IcebergVersionTable, got {other:?}"),
        }
    }

    #[test]
    fn imv_pipeline_binds_version_to_scan() {
        let plan = LogicalPlan::ImvVersion(ImvVersionNode {
            input: Box::new(iceberg_scan_plan()),
            version_ref: ImvVersionRef::to_snapshot(),
        });
        let outcome = run_imv_rewrite(ImvRewriteInput {
            plan,
            mv_ctx: dummy_mv_ctx(),
            disabled_rules: vec!["WrapRootInImvDelta".to_string()],
            deadline: None,
        })
        .expect("Version(Scan, To) must bind and pass validation");

        let LogicalPlan::Scan(scan) = outcome.plan else {
            panic!("expected scan outcome");
        };
        match scan.table.source {
            ScanSource::IcebergVersionTable { snapshot_id, .. } => {
                assert_eq!(snapshot_id, 22);
            }
            other => panic!("expected IcebergVersionTable, got {other:?}"),
        }
    }
```

- [ ] **Step 2: Run the focused tests**

Run:

```bash
cargo test --lib imv_pipeline_binds_version
```

Expected: both version binding tests PASS.

- [ ] **Step 3: Commit**

```bash
git add src/sql/optimizer/rewrite/imv/entrypoint.rs
git commit -m "test: cover IMV version scan binding"
```

---

### Task 7: Verify Codegen Guard for Version Placeholder

**Files:**
- Modify: `src/sql/codegen/nodes.rs`

- [ ] **Step 1: Add a unit test for the guard**

Inside the existing `#[cfg(test)] mod tests` in `src/sql/codegen/nodes.rs`, add this test. It constructs a `ResolvedTable` with `ScanSource::IcebergVersionTable` and calls `build_exec_params_multi`.

Add this test body:

```rust
    #[test]
    fn iceberg_version_table_reaches_scan_range_guard() {
        use crate::sql::catalog::{ColumnDef, IcebergSchemaDef, IcebergTableInfo, ScanSource, TableDef};

        let resolved = ResolvedTable {
            database: "db".to_string(),
            table: TableDef {
                name: "b".to_string(),
                columns: vec![ColumnDef {
                    name: "k".to_string(),
                    data_type: arrow::datatypes::DataType::Int64,
                    nullable: false,
                    write_default: None,
                    logical_type: None,
                }],
                iceberg_row_lineage_metadata_columns: Vec::new(),
                source: ScanSource::IcebergVersionTable {
                    table: IcebergTableInfo {
                        catalog: "ice".to_string(),
                        namespace: "db".to_string(),
                        table: "b".to_string(),
                        table_uuid: Some("uuid-b".to_string()),
                        current_snapshot_id: Some(22),
                        schema_id: 7,
                        location: "file:///tmp/ice/db/b".to_string(),
                        schema: IcebergSchemaDef { fields: Vec::new() },
                        serialized_metadata: None,
                    },
                    snapshot_id: 11,
                },
            },
            physical_layout: None,
        };
        let planned = PlannedScanTable {
            scan_node_id: 9,
            resolved,
            min_max_conjuncts: Vec::new(),
            slot_to_column: std::collections::HashMap::new(),
            iceberg_metadata_pseudo_column_slots: std::collections::BTreeSet::new(),
        };

        let err = build_exec_params_multi(&[planned])
            .expect_err("version table must not be executable in phase 1");
        assert!(
            err.contains("IMV version scan ice.db.b at snapshot 11 reached scan-range construction before execution cutover"),
            "unexpected error: {err}"
        );
    }
```

- [ ] **Step 2: Run the guard test**

Run:

```bash
cargo test --lib iceberg_version_table_reaches_scan_range_guard
```

Expected: PASS.

- [ ] **Step 3: Commit**

```bash
git add src/sql/codegen/nodes.rs
git commit -m "test: guard IMV version scan execution"
```

---

### Task 8: Final Verification

**Files:**
- No edits unless verification exposes a compile or test failure.

- [ ] **Step 1: Run focused IMV tests**

Run:

```bash
cargo test --lib scan_binding
cargo test --lib imv_pipeline_binds_root_delta_scan
cargo test --lib imv_pipeline_binds_version
```

Expected: all PASS.

- [ ] **Step 2: Run codegen guard test**

Run:

```bash
cargo test --lib iceberg_version_table_reaches_scan_range_guard
```

Expected: PASS.

- [ ] **Step 3: Run full library tests**

Run:

```bash
cargo test --lib
```

Expected: PASS. If an existing unrelated test fails, capture the test name and failure output in the handoff and do not change unrelated code.

- [ ] **Step 4: Check formatting**

Run:

```bash
cargo fmt --check
```

Expected: PASS.

- [ ] **Step 5: Commit verification-only formatting changes if any**

If `cargo fmt --check` fails, run:

```bash
cargo fmt
```

Then commit only formatting changes caused by the implementation:

```bash
git add src/sql/optimizer/rewrite/imv src/sql/catalog.rs src/sql/codegen/descriptors.rs src/sql/codegen/fragment_builder.rs src/sql/codegen/nodes.rs src/sql/explain.rs src/sql/planner/mod.rs src/sql/optimizer/rewrite/rules/ukfk.rs src/sql/optimizer/rewrite/rules/join_reorder/reorder.rs src/engine/dictionary/mod.rs
git commit -m "style: format IMV scan binding changes"
```

If `cargo fmt --check` passes, do not create a formatting commit.

---

## Self-Review Notes

Spec coverage:

- Phase 1 scan binding is covered by Tasks 1-6.
- Refresh execution cutover is intentionally not covered; the version placeholder has an explicit codegen guard in Task 7.
- Action column, aggregate, join algebra, and UNION ALL are outside this phase and remain for separate plans.
- Snapshot pin semantics are enforced by `resolve_snapshot_window`: previous snapshot + pinned to snapshot + pin UUID.

Ambiguity resolved:

- `Version(Scan)` is inspectable but non-executable in phase 1. This avoids an unsafe current-snapshot fallback while preserving the optimizer-only milestone.
- `Delta(Scan)` uses existing `ScanSource::IcebergDeltaTable`, so execution-cutover behavior can reuse the existing delta scan node.

Validation commands:

- `cargo test --lib scan_binding`
- `cargo test --lib imv_pipeline_binds_root_delta_scan`
- `cargo test --lib imv_pipeline_binds_version`
- `cargo test --lib iceberg_version_table_reaches_scan_range_guard`
- `cargo test --lib`
- `cargo fmt --check`
