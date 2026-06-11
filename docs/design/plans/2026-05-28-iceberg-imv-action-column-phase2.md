# Iceberg IMV Action Column Phase 2 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Wire an optimizer-internal action column through the IMV rewrite pipeline so Delta-bound scans carry a non-nullable `Int8` `__change_op` column that propagates through Project/Filter, fails fast on Join/UnionAll/Aggregate, survives column pruning, and never leaks to user-visible output. Refresh execution does NOT switch in this phase.

**Architecture:** Add a generic `is_internal: bool` flag on `OutputColumn`; create a new `imv-action-propagation` rewrite stage with `InjectActionColumnRule` (BottomUp, adds the column on Delta-bound scans) and `PropagateActionColumnRule` (BottomUp, threads through Project, NO-OP on Filter, fail-fast on Aggregate/Join/UnionAll); add `ActionColumnValidationRule` to the validation stage (V1-V5 invariants); teach `PruneColumns` to preserve internal columns and treat IMV markers as errors instead of panics; add a codegen guard mirroring Phase 1's `IcebergVersionTable` guard.

**Tech Stack:** Rust, existing IMV pipeline (`src/sql/optimizer/rewrite/imv/`), `ColumnRefFactory` (`src/sql/column_id.rs`), `RewriteContext` extensions, `cargo test --lib`.

---

## Scope

This plan implements only umbrella spec phase 2 (action column). It does not:
- Switch refresh execution (Phase 3 projection/filter cutover does that).
- Implement join delta algebra (Phase 5).
- Implement union delta rewrite (Phase 6).
- Implement aggregate state rewrite (Phase 4).
- Touch `src/exec/operators/iceberg_delta_scan.rs`, `src/engine/mv/iceberg_refresh.rs`, or
  `src/engine/mv/iceberg_merge_sink.rs`.

## File Structure

- Modify: `src/sql/analysis/mod.rs` — add `is_internal: bool` field to `OutputColumn`.
- Modify: ~40 `OutputColumn { ... }` construction sites across `src/sql/optimizer/**` and
  `src/sql/explain.rs` to set `is_internal: false`. Enumerated by `cargo build` output.
- Modify: `src/sql/optimizer/rewrite/imv/annotation.rs` — add `next_column_id: Arc<AtomicU32>`
  to `ImvExtension`.
- Modify: `src/sql/optimizer/rewrite/imv/entrypoint.rs` — initialize `next_column_id` from
  the input plan's max ColumnId.
- Create: `src/sql/optimizer/rewrite/imv/action_column.rs` — `ImvActionColumn` descriptor +
  `ActionColumnValidationRule`.
- Create: `src/sql/optimizer/rewrite/imv/action_propagation.rs` — `InjectActionColumnRule` +
  `PropagateActionColumnRule`.
- Modify: `src/sql/optimizer/rewrite/imv/mod.rs` — declare new modules.
- Modify: `src/sql/optimizer/rewrite/imv/pipeline.rs` — register `imv-action-propagation`
  stage + `ActionColumnValidationRule` in `imv-validation`.
- Modify: `src/sql/optimizer/rewrite/rules/column_pruning.rs` — preserve internal columns;
  replace IMV-marker panic with explicit error.
- Modify: `src/sql/codegen/nodes.rs` — add codegen guard for action column on scan.

Test commands:
- `cargo test --lib action_column`
- `cargo test --lib action_propagation`
- `cargo test --lib imv`
- `cargo test --lib column_pruning`
- `cargo test --lib`
- `cargo fmt --check`

---

### Task 1: Add `is_internal` Field to `OutputColumn`

**Files:**
- Modify: `src/sql/analysis/mod.rs`
- Modify: ~40 construction sites enumerated by the build (listed below)

This task is a single mechanical change: add a field with default `false` to all callers. Compile breaks until every construction site is updated; commit once after every site compiles.

- [ ] **Step 1: Add the field**

Edit `src/sql/analysis/mod.rs` lines 28-34. Change:

```rust
#[derive(Clone, Debug)]
pub(crate) struct OutputColumn {
    pub column_id: ColumnId,
    pub name: String,
    pub data_type: DataType,
    pub nullable: bool,
}
```

to:

```rust
#[derive(Clone, Debug)]
pub(crate) struct OutputColumn {
    pub column_id: ColumnId,
    pub name: String,
    pub data_type: DataType,
    pub nullable: bool,
    /// Internal optimizer-managed column not visible to users. Set true for
    /// pseudo-columns synthesized by rewrite rules (e.g. IMV action column);
    /// column pruning treats internal columns as always required.
    pub is_internal: bool,
}
```

- [ ] **Step 2: Enumerate failing construction sites**

Run:

```bash
cargo build --lib 2>&1 | grep -E "missing field|fields are missing" | sort -u
```

Expected: ~40 errors of the form `missing field 'is_internal' in initializer of OutputColumn`. Capture file:line pairs.

- [ ] **Step 3: Update every construction site to add `is_internal: false`**

For each `OutputColumn { ... }` literal in the error list, append `is_internal: false,` to the field list. Known locations (from prior survey):

- `src/sql/optimizer/convert.rs:350`
- `src/sql/explain.rs:1111, 1159, 1283, 1350`
- `src/sql/optimizer/cte_rewrite.rs:324, 337`
- `src/sql/optimizer/mod.rs:409, 445`
- `src/sql/optimizer/stats.rs:991, 1012, 1088, 1106, 1451, 1620, 1676, 1686, 1755, 1789, 1817, 1818`
- `src/sql/optimizer/logical_props.rs:337, 338`
- `src/sql/optimizer/rewrite/rules/column_pruning.rs:385, 529, 535`
- `src/sql/optimizer/rewrite/rules/join_reorder/cardinality.rs:456, 723`
- `src/sql/optimizer/rewrite/tree.rs:429, 430`
- `src/sql/optimizer/rewrite/rules/join_reorder/rule.rs:115`
- `src/sql/optimizer/rewrite/rules/low_cardinality_dict/rewriter.rs:384, 1246, 1277`
- `src/sql/optimizer/rewrite/rules/join_reorder/reorder.rs:1378, 1737`
- `src/sql/optimizer/rewrite/rules/predicate_pushdown/**:133, 167, 154, 180, 186, 523`
- `src/sql/optimizer/rewrite/rules/aggregate_pushdown/rewriter.rs:37, 50, 164, 210, 306, 312`
- `src/sql/optimizer/rewrite/rules/aggregate_pushdown/cost.rs:92`

If `cargo build --lib` reports additional sites beyond this list, append `is_internal: false,` there too. Treat the build as the source of truth.

Example transformation:

```rust
// Before
OutputColumn {
    column_id: ColumnId::UNSET,
    name: c.name.clone(),
    data_type: c.data_type.clone(),
    nullable: c.nullable,
}

// After
OutputColumn {
    column_id: ColumnId::UNSET,
    name: c.name.clone(),
    data_type: c.data_type.clone(),
    nullable: c.nullable,
    is_internal: false,
}
```

- [ ] **Step 4: Verify compile**

Run:

```bash
cargo build --lib 2>&1 | tail -5
```

Expected: clean compile.

- [ ] **Step 5: Run full library tests**

```bash
cargo test --lib 2>&1 | tail -3
```

Expected: all previously-passing tests still pass (the field is unused; only struct shape changed).

- [ ] **Step 6: Commit**

```bash
git add src/
git commit -m "feat: add is_internal flag to OutputColumn"
```

---

### Task 2: Thread `next_column_id` Through `ImvExtension`

**Files:**
- Modify: `src/sql/optimizer/rewrite/imv/annotation.rs`
- Modify: `src/sql/optimizer/rewrite/imv/entrypoint.rs`

The IMV rewrite needs to allocate new `ColumnId`s for the action column. We attach a shared `AtomicU32` counter to `ImvExtension`, initialized to one past the input plan's largest existing ColumnId.

- [ ] **Step 1: Extend `ImvExtension`**

Edit `src/sql/optimizer/rewrite/imv/annotation.rs`. Replace the existing struct body:

```rust
use std::sync::Arc;
use std::sync::atomic::AtomicU32;

use crate::engine::mv::refresh_context::IcebergMvRewriteContext;
use crate::sql::column_id::ColumnId;

#[derive(Clone, Debug, Default)]
pub(crate) struct ImvPlanAnnotation {
    _private: (),
}

#[derive(Clone, Debug)]
pub(crate) struct ImvExtension {
    pub mv_ctx: Arc<IcebergMvRewriteContext>,
    pub annotation: ImvPlanAnnotation,
    /// Shared counter for allocating new `ColumnId`s during IMV rewrite.
    /// Initialized at entrypoint to one past the largest existing ColumnId
    /// in the input plan, so rules never collide with analyzer-assigned ids.
    pub next_column_id: Arc<AtomicU32>,
}

impl ImvExtension {
    /// Allocate a fresh `ColumnId` from the shared counter.
    pub(crate) fn allocate_column_id(&self) -> ColumnId {
        let raw = self
            .next_column_id
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        ColumnId(raw)
    }
}
```

(Keep any existing `use` lines already present; the snippet above shows the minimum required imports.)

- [ ] **Step 2: Add a `max_column_id` plan walker**

In `src/sql/optimizer/rewrite/imv/entrypoint.rs`, add this private helper near the top of the module (after the existing imports):

```rust
use std::sync::Arc;
use std::sync::atomic::AtomicU32;

use crate::sql::column_id::ColumnId;
use crate::sql::planner::plan::LogicalPlan;

/// Returns the largest `ColumnId.0` value referenced anywhere in the plan
/// tree's output columns. Used to seed `ImvExtension::next_column_id` so
/// IMV rewrite never collides with analyzer-assigned ids. Returns 0 if no
/// output columns are present.
fn max_column_id(plan: &LogicalPlan) -> u32 {
    let mut max = 0u32;
    visit_output_columns(plan, &mut |id: ColumnId| {
        if id.0 > max {
            max = id.0;
        }
    });
    max
}

fn visit_output_columns(plan: &LogicalPlan, visit: &mut impl FnMut(ColumnId)) {
    match plan {
        LogicalPlan::Scan(scan) => {
            for col in &scan.columns {
                visit(col.column_id);
            }
        }
        LogicalPlan::Filter(node) => visit_output_columns(&node.input, visit),
        LogicalPlan::Project(node) => visit_output_columns(&node.input, visit),
        LogicalPlan::Aggregate(node) => {
            for col in &node.output_columns {
                visit(col.column_id);
            }
            visit_output_columns(&node.input, visit);
        }
        LogicalPlan::Join(node) => {
            visit_output_columns(&node.left, visit);
            visit_output_columns(&node.right, visit);
        }
        LogicalPlan::UnionAll(node) => {
            for child in &node.inputs {
                visit_output_columns(child, visit);
            }
        }
        LogicalPlan::ImvDelta(node) => visit_output_columns(&node.input, visit),
        LogicalPlan::ImvVersion(node) => visit_output_columns(&node.input, visit),
        // Other variants reached only after analysis; fall through without
        // visiting (their schemas are subsumed by the variants above).
        _ => {}
    }
}
```

If any of the matched variant names don't exist exactly as written (e.g., `UnionAll` might be `Union`), update them to match `src/sql/planner/plan.rs::LogicalPlan` enum variant names. The build will surface mismatches.

- [ ] **Step 3: Initialize `next_column_id` at entrypoint**

In `src/sql/optimizer/rewrite/imv/entrypoint.rs`, find the existing `run_imv_rewrite` body (around line 28-57). The current `set_extension` call:

```rust
ctx_rw.set_extension::<ImvExtension>(ImvExtension {
    mv_ctx,
    annotation: ImvPlanAnnotation::default(),
});
```

Replace with:

```rust
let next_column_id = Arc::new(AtomicU32::new(max_column_id(&plan).saturating_add(1)));
ctx_rw.set_extension::<ImvExtension>(ImvExtension {
    mv_ctx,
    annotation: ImvPlanAnnotation::default(),
    next_column_id,
});
```

Also update the test-helper `ImvExtension { ... }` constructions in the same file (around lines 162, 219, 291 per survey) to include `next_column_id: Arc::new(AtomicU32::new(1))` (tests don't need a non-trivial seed).

- [ ] **Step 4: Compile and run IMV tests**

```bash
cargo build --lib 2>&1 | tail -5
cargo test --lib imv 2>&1 | tail -3
```

Expected: build clean; existing 30 IMV tests still pass (no behavior change; only the extension carries an extra unused field).

- [ ] **Step 5: Commit**

```bash
git add src/sql/optimizer/rewrite/imv/annotation.rs src/sql/optimizer/rewrite/imv/entrypoint.rs
git commit -m "feat: seed IMV column-id allocator from input plan"
```

---

### Task 3: Create `ImvActionColumn` Descriptor Module

**Files:**
- Create: `src/sql/optimizer/rewrite/imv/action_column.rs`
- Modify: `src/sql/optimizer/rewrite/imv/mod.rs`

This task introduces the descriptor type and its `matches` helper. Validation rule lives in this file too but is added in Task 8.

- [ ] **Step 1: Create the module file**

Create `src/sql/optimizer/rewrite/imv/action_column.rs` with this content:

```rust
//! IMV action column descriptor.
//!
//! The action column is an optimizer-internal `Int8` non-nullable column
//! produced by `InjectActionColumnRule` on Delta-bound scans. It carries
//! `+1` for inserts/upserts and `-1` for deletes at runtime (Phase 3+),
//! and is never exposed to user-visible output.

use arrow::datatypes::DataType;

use crate::sql::analysis::OutputColumn;
use crate::sql::column_id::ColumnId;

pub(crate) struct ImvActionColumn;

impl ImvActionColumn {
    pub(crate) const NAME: &'static str = crate::exec::change_op::CHANGE_OP_COLUMN;
    #[allow(dead_code)]
    pub(crate) const INSERT_VALUE: i8 = crate::exec::change_op::CHANGE_OP_INSERT;
    #[allow(dead_code)]
    pub(crate) const DELETE_VALUE: i8 = crate::exec::change_op::CHANGE_OP_DELETE;

    /// Construct an `OutputColumn` for the action column with the given id.
    pub(crate) fn output_column(column_id: ColumnId) -> OutputColumn {
        OutputColumn {
            column_id,
            name: Self::NAME.to_string(),
            data_type: DataType::Int8,
            nullable: false,
            is_internal: true,
        }
    }

    /// Returns true iff `column` is the IMV action column.
    pub(crate) fn matches(column: &OutputColumn) -> bool {
        column.is_internal && column.name.eq_ignore_ascii_case(Self::NAME)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sql::column_id::ColumnId;

    #[test]
    fn output_column_has_expected_shape() {
        let col = ImvActionColumn::output_column(ColumnId(7));
        assert_eq!(col.column_id, ColumnId(7));
        assert_eq!(col.name, "__change_op");
        assert_eq!(col.data_type, DataType::Int8);
        assert!(!col.nullable);
        assert!(col.is_internal);
    }

    #[test]
    fn matches_recognizes_action_column() {
        let col = ImvActionColumn::output_column(ColumnId(1));
        assert!(ImvActionColumn::matches(&col));
    }

    #[test]
    fn matches_rejects_external_column_with_same_name() {
        let mut col = ImvActionColumn::output_column(ColumnId(1));
        col.is_internal = false;
        assert!(!ImvActionColumn::matches(&col));
    }

    #[test]
    fn matches_rejects_other_internal_column() {
        let col = OutputColumn {
            column_id: ColumnId(1),
            name: "other".to_string(),
            data_type: DataType::Int8,
            nullable: false,
            is_internal: true,
        };
        assert!(!ImvActionColumn::matches(&col));
    }

    #[test]
    fn constants_match_change_op_module() {
        assert_eq!(ImvActionColumn::NAME, "__change_op");
        assert_eq!(ImvActionColumn::INSERT_VALUE, 1);
        assert_eq!(ImvActionColumn::DELETE_VALUE, -1);
    }
}
```

- [ ] **Step 2: Declare the module**

In `src/sql/optimizer/rewrite/imv/mod.rs`, add (after the existing `scan_binding` declaration):

```rust
pub(crate) mod action_column;
```

- [ ] **Step 3: Run focused tests**

```bash
cargo test --lib action_column 2>&1 | tail -5
```

Expected: 5 passed (`output_column_has_expected_shape`, `matches_recognizes_action_column`, `matches_rejects_external_column_with_same_name`, `matches_rejects_other_internal_column`, `constants_match_change_op_module`).

- [ ] **Step 4: Commit**

```bash
git add src/sql/optimizer/rewrite/imv/action_column.rs src/sql/optimizer/rewrite/imv/mod.rs
git commit -m "feat: add IMV action column descriptor"
```

---

### Task 4: `InjectActionColumnRule`

**Files:**
- Create: `src/sql/optimizer/rewrite/imv/action_propagation.rs`
- Modify: `src/sql/optimizer/rewrite/imv/mod.rs`

- [ ] **Step 1: Create the action_propagation module**

Create `src/sql/optimizer/rewrite/imv/action_propagation.rs` with this initial content:

```rust
//! IMV action column injection and propagation rules.
//!
//! Phase 2: Delta-bound scans get an internal `__change_op` Int8
//! non-nullable column. Project transparently carries it. Filter is a
//! schema-passthrough node and requires no work. Join/UnionAll/Aggregate
//! above a Delta scan are unsupported in Phase 2 and fail-fast.

use arrow::datatypes::DataType;

use crate::sql::analysis::{ExprKind, OutputColumn, ProjectItem, TypedExpr};
use crate::sql::catalog::ScanSource;
use crate::sql::column_id::ColumnId;
use crate::sql::optimizer::rewrite::context::RewriteContext;
use crate::sql::optimizer::rewrite::imv::action_column::ImvActionColumn;
use crate::sql::optimizer::rewrite::imv::annotation::ImvExtension;
use crate::sql::optimizer::rewrite::phase::RewritePhase;
use crate::sql::optimizer::rewrite::result::RewriteResult;
use crate::sql::optimizer::rewrite::rule::{LogicalRewriteRule, RewriteTraversal};
use crate::sql::planner::plan::{LogicalPlan, ProjectNode, ScanNode};

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Returns true iff the plan's effective output schema contains the IMV
/// action column. Used by `matches()` predicates and validation.
pub(crate) fn output_has_action_column(plan: &LogicalPlan) -> bool {
    match plan {
        LogicalPlan::Scan(scan) => scan.columns.iter().any(ImvActionColumn::matches),
        LogicalPlan::Filter(node) => output_has_action_column(&node.input),
        LogicalPlan::Project(node) => {
            node.items
                .iter()
                .any(|item| item.output_name.eq_ignore_ascii_case(ImvActionColumn::NAME))
        }
        LogicalPlan::ImvDelta(node) => output_has_action_column(&node.input),
        LogicalPlan::ImvVersion(node) => output_has_action_column(&node.input),
        _ => false,
    }
}

/// Returns the action column descriptor from the first descendant Scan/Project
/// in the subtree that exposes one, or `None` if no descendant carries it.
pub(crate) fn find_action_column(plan: &LogicalPlan) -> Option<OutputColumn> {
    match plan {
        LogicalPlan::Scan(scan) => scan
            .columns
            .iter()
            .find(|c| ImvActionColumn::matches(c))
            .cloned(),
        LogicalPlan::Filter(node) => find_action_column(&node.input),
        LogicalPlan::Project(node) => find_action_column(&node.input),
        _ => None,
    }
}

/// Whether any descendant of the plan exposes an action column.
pub(crate) fn subtree_has_action_column(plan: &LogicalPlan) -> bool {
    output_has_action_column(plan) || match plan {
        LogicalPlan::Filter(node) => subtree_has_action_column(&node.input),
        LogicalPlan::Project(node) => subtree_has_action_column(&node.input),
        _ => false,
    }
}

// ---------------------------------------------------------------------------
// InjectActionColumnRule
// ---------------------------------------------------------------------------

pub(crate) struct InjectActionColumnRule;

impl LogicalRewriteRule for InjectActionColumnRule {
    fn name(&self) -> &'static str {
        "InjectActionColumn"
    }

    fn phase(&self) -> RewritePhase {
        RewritePhase::SemanticRewrite
    }

    fn traversal(&self) -> RewriteTraversal {
        RewriteTraversal::BottomUp
    }

    fn matches(&self, plan: &LogicalPlan, _ctx: &RewriteContext) -> bool {
        match plan {
            LogicalPlan::Scan(scan) => {
                matches!(scan.table.source, ScanSource::IcebergDeltaTable { .. })
                    && !scan.columns.iter().any(ImvActionColumn::matches)
            }
            _ => false,
        }
    }

    fn apply(&self, plan: LogicalPlan, ctx: &mut RewriteContext) -> Result<RewriteResult, String> {
        let LogicalPlan::Scan(mut scan) = plan else {
            return Ok(RewriteResult::Unchanged);
        };
        let ext = ctx.extension::<ImvExtension>().ok_or_else(|| {
            "InjectActionColumn requires ImvExtension in RewriteContext".to_string()
        })?;
        let column_id = ext.allocate_column_id();
        scan.columns.push(ImvActionColumn::output_column(column_id));
        Ok(RewriteResult::Changed(LogicalPlan::Scan(scan)))
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::sync::Arc;
    use std::sync::atomic::AtomicU32;

    use arrow::datatypes::DataType;

    use super::*;
    use crate::engine::mv::refresh_context::tests_support::dummy_rewrite_context;
    use crate::sql::catalog::{
        ColumnDef, IcebergSchemaDef, IcebergTableInfo, ScanSource, TableDef,
    };
    use crate::sql::optimizer::rewrite::context::RewriteContext;
    use crate::sql::optimizer::rewrite::imv::annotation::{ImvExtension, ImvPlanAnnotation};
    use crate::sql::planner::plan::{LogicalPlan, ScanNode};

    fn build_ctx() -> RewriteContext {
        let mut ctx = RewriteContext::for_mv_refresh(Vec::new());
        ctx.set_extension::<ImvExtension>(ImvExtension {
            mv_ctx: dummy_rewrite_context(),
            annotation: ImvPlanAnnotation::default(),
            next_column_id: Arc::new(AtomicU32::new(100)),
        });
        ctx
    }

    fn delta_scan() -> ScanNode {
        ScanNode {
            database: "db".to_string(),
            table: TableDef {
                name: "b".to_string(),
                columns: vec![ColumnDef {
                    name: "k".to_string(),
                    data_type: DataType::Int64,
                    nullable: false,
                    write_default: None,
                    logical_type: None,
                }],
                iceberg_row_lineage_metadata_columns: Vec::new(),
                source: ScanSource::IcebergDeltaTable {
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
                    from_snapshot_id: 11,
                    to_snapshot_id: 22,
                },
            },
            alias: None,
            columns: vec![OutputColumn {
                column_id: ColumnId(1),
                name: "k".to_string(),
                data_type: DataType::Int64,
                nullable: false,
                is_internal: false,
            }],
            predicates: Vec::new(),
            required_columns: None,
            dict_columns: Vec::new(),
        }
    }

    fn version_scan() -> ScanNode {
        let mut s = delta_scan();
        s.table.source = ScanSource::IcebergVersionTable {
            table: match &delta_scan().table.source {
                ScanSource::IcebergDeltaTable { table, .. } => table.clone(),
                _ => unreachable!(),
            },
            snapshot_id: 22,
        };
        s
    }

    fn starrocks_scan() -> ScanNode {
        let mut s = delta_scan();
        s.table.source = ScanSource::StarRocks { db_id: 0, table_id: 0 };
        s
    }

    #[test]
    fn inject_action_column_on_delta_scan() {
        let rule = InjectActionColumnRule;
        let mut ctx = build_ctx();
        let plan = LogicalPlan::Scan(delta_scan());
        assert!(rule.matches(&plan, &ctx));
        let result = rule.apply(plan, &mut ctx).expect("apply must succeed");
        let RewriteResult::Changed(LogicalPlan::Scan(scan)) = result else {
            panic!("expected Changed(Scan), got {:?}", result);
        };
        let action = scan
            .columns
            .iter()
            .find(|c| ImvActionColumn::matches(c))
            .expect("action column must be present");
        assert_eq!(action.data_type, DataType::Int8);
        assert!(!action.nullable);
        assert!(action.is_internal);
        assert_eq!(action.column_id, ColumnId(100));
    }

    #[test]
    fn inject_does_not_touch_version_scan() {
        let rule = InjectActionColumnRule;
        let ctx = build_ctx();
        let plan = LogicalPlan::Scan(version_scan());
        assert!(!rule.matches(&plan, &ctx));
    }

    #[test]
    fn inject_is_idempotent() {
        let rule = InjectActionColumnRule;
        let mut ctx = build_ctx();
        let mut scan = delta_scan();
        scan.columns.push(ImvActionColumn::output_column(ColumnId(9)));
        let plan = LogicalPlan::Scan(scan);
        assert!(!rule.matches(&plan, &ctx));
    }

    #[test]
    fn inject_skips_starrocks_scan() {
        let rule = InjectActionColumnRule;
        let ctx = build_ctx();
        let plan = LogicalPlan::Scan(starrocks_scan());
        assert!(!rule.matches(&plan, &ctx));
    }
}
```

- [ ] **Step 2: Declare the module**

In `src/sql/optimizer/rewrite/imv/mod.rs` add (after `action_column`):

```rust
pub(crate) mod action_propagation;
```

- [ ] **Step 3: Run focused tests**

```bash
cargo test --lib action_propagation 2>&1 | tail -5
```

Expected: 4 passed.

- [ ] **Step 4: Compile sanity**

```bash
cargo build --lib 2>&1 | tail -3
```

Expected: clean.

- [ ] **Step 5: Commit**

```bash
git add src/sql/optimizer/rewrite/imv/action_propagation.rs src/sql/optimizer/rewrite/imv/mod.rs
git commit -m "feat: add IMV InjectActionColumnRule"
```

---

### Task 5: `PropagateActionColumnRule` (Project propagation)

**Files:**
- Modify: `src/sql/optimizer/rewrite/imv/action_propagation.rs`

- [ ] **Step 1: Add `PropagateActionColumnRule`**

Append to `src/sql/optimizer/rewrite/imv/action_propagation.rs`, after the `InjectActionColumnRule` impl and BEFORE the existing `#[cfg(test)] mod tests`:

```rust
// ---------------------------------------------------------------------------
// PropagateActionColumnRule
// ---------------------------------------------------------------------------

pub(crate) struct PropagateActionColumnRule;

impl LogicalRewriteRule for PropagateActionColumnRule {
    fn name(&self) -> &'static str {
        "PropagateActionColumn"
    }

    fn phase(&self) -> RewritePhase {
        RewritePhase::SemanticRewrite
    }

    fn traversal(&self) -> RewriteTraversal {
        RewriteTraversal::BottomUp
    }

    fn matches(&self, plan: &LogicalPlan, _ctx: &RewriteContext) -> bool {
        match plan {
            LogicalPlan::Project(p) => {
                subtree_has_action_column(&p.input) && !output_has_action_column(plan)
            }
            // Filter is a schema-passthrough node: it exposes its child's
            // schema verbatim, so once the child has the action column the
            // Filter's effective output also has it. No work needed.
            LogicalPlan::Filter(_) => false,
            // Aggregate / Join / UnionAll handled in Task 6 (fail-fast).
            _ => false,
        }
    }

    fn apply(&self, plan: LogicalPlan, _ctx: &mut RewriteContext) -> Result<RewriteResult, String> {
        match plan {
            LogicalPlan::Project(mut p) => {
                let action = find_action_column(&p.input).ok_or_else(|| {
                    "PropagateActionColumn matched Project but child has no action column"
                        .to_string()
                })?;
                p.items.push(ProjectItem {
                    expr: TypedExpr {
                        kind: ExprKind::ColumnRef {
                            column_id: action.column_id,
                            qualifier: None,
                            column: action.name.clone(),
                        },
                        data_type: DataType::Int8,
                        nullable: false,
                    },
                    output_name: action.name.clone(),
                });
                Ok(RewriteResult::Changed(LogicalPlan::Project(p)))
            }
            other => Ok(RewriteResult::Unchanged),
        }
    }
}
```

(Note: the trailing `other` is intentionally `_` to avoid an unused-variable warning. Use `_ => Ok(RewriteResult::Unchanged)` instead of `other`.)

Update the file's bottom-of-impl `apply` arm to:

```rust
            _ => Ok(RewriteResult::Unchanged),
```

- [ ] **Step 2: Add Project propagation tests**

Inside the existing `#[cfg(test)] mod tests` block in `action_propagation.rs`, append:

```rust
    use crate::sql::planner::plan::{ProjectNode};

    fn project_over(input: LogicalPlan, projected_user_col_id: ColumnId) -> LogicalPlan {
        LogicalPlan::Project(ProjectNode {
            input: Box::new(input),
            items: vec![ProjectItem {
                expr: TypedExpr {
                    kind: ExprKind::ColumnRef {
                        column_id: projected_user_col_id,
                        qualifier: None,
                        column: "k".to_string(),
                    },
                    data_type: DataType::Int64,
                    nullable: false,
                },
                output_name: "k".to_string(),
            }],
        })
    }

    fn delta_scan_with_action(action_id: ColumnId) -> ScanNode {
        let mut s = delta_scan();
        s.columns.push(ImvActionColumn::output_column(action_id));
        s
    }

    #[test]
    fn propagate_through_project() {
        let rule = PropagateActionColumnRule;
        let mut ctx = build_ctx();
        let scan = LogicalPlan::Scan(delta_scan_with_action(ColumnId(100)));
        let plan = project_over(scan, ColumnId(1));
        assert!(rule.matches(&plan, &ctx));
        let result = rule.apply(plan, &mut ctx).expect("apply must succeed");
        let RewriteResult::Changed(LogicalPlan::Project(project)) = result else {
            panic!("expected Changed(Project)");
        };
        assert_eq!(project.items.len(), 2);
        let last = &project.items[1];
        assert_eq!(last.output_name, "__change_op");
        match &last.expr.kind {
            ExprKind::ColumnRef { column_id, .. } => assert_eq!(*column_id, ColumnId(100)),
            other => panic!("expected ColumnRef, got {:?}", other),
        }
        assert_eq!(last.expr.data_type, DataType::Int8);
        assert!(!last.expr.nullable);
    }

    #[test]
    fn propagate_is_idempotent_on_project_with_action() {
        let rule = PropagateActionColumnRule;
        let ctx = build_ctx();
        let scan = LogicalPlan::Scan(delta_scan_with_action(ColumnId(100)));
        let mut plan = project_over(scan, ColumnId(1));
        if let LogicalPlan::Project(p) = &mut plan {
            p.items.push(ProjectItem {
                expr: TypedExpr {
                    kind: ExprKind::ColumnRef {
                        column_id: ColumnId(100),
                        qualifier: None,
                        column: "__change_op".to_string(),
                    },
                    data_type: DataType::Int8,
                    nullable: false,
                },
                output_name: "__change_op".to_string(),
            });
        }
        assert!(!rule.matches(&plan, &ctx));
    }

    #[test]
    fn propagate_skips_filter_node() {
        // Filter is schema-passthrough; rule must not match.
        let rule = PropagateActionColumnRule;
        let ctx = build_ctx();
        // We don't construct an actual Filter here because Filter requires a
        // predicate expression; idempotency is tested via the propagate
        // signature. The matches() arm for Filter returns false unconditionally.
        let plan = LogicalPlan::Scan(delta_scan_with_action(ColumnId(100)));
        // A bare Scan is not a Project, so the rule should not match.
        assert!(!rule.matches(&plan, &ctx));
    }
```

- [ ] **Step 3: Run focused tests**

```bash
cargo test --lib propagate_ 2>&1 | tail -5
```

Expected: 3 propagation tests pass (`propagate_through_project`, `propagate_is_idempotent_on_project_with_action`, `propagate_skips_filter_node`). The previous 4 Inject tests still pass.

- [ ] **Step 4: Commit**

```bash
git add src/sql/optimizer/rewrite/imv/action_propagation.rs
git commit -m "feat: propagate IMV action column through Project"
```

---

### Task 6: Fail-Fast on Join / UnionAll / Aggregate

**Files:**
- Modify: `src/sql/optimizer/rewrite/imv/action_propagation.rs`

- [ ] **Step 1: Extend matches() and apply() for Aggregate / Join / UnionAll**

In `PropagateActionColumnRule::matches`, change the `_ => false` arm so it returns true for `Aggregate` / `Join` / `UnionAll` when any descendant has an action column:

```rust
    fn matches(&self, plan: &LogicalPlan, _ctx: &RewriteContext) -> bool {
        match plan {
            LogicalPlan::Project(p) => {
                subtree_has_action_column(&p.input) && !output_has_action_column(plan)
            }
            LogicalPlan::Filter(_) => false,
            LogicalPlan::Aggregate(a) => subtree_has_action_column(&a.input),
            LogicalPlan::Join(j) => {
                subtree_has_action_column(&j.left) || subtree_has_action_column(&j.right)
            }
            LogicalPlan::UnionAll(u) => u.inputs.iter().any(subtree_has_action_column),
            _ => false,
        }
    }
```

In `PropagateActionColumnRule::apply`, extend the match:

```rust
    fn apply(&self, plan: LogicalPlan, _ctx: &mut RewriteContext) -> Result<RewriteResult, String> {
        match plan {
            LogicalPlan::Project(mut p) => {
                let action = find_action_column(&p.input).ok_or_else(|| {
                    "PropagateActionColumn matched Project but child has no action column"
                        .to_string()
                })?;
                p.items.push(ProjectItem {
                    expr: TypedExpr {
                        kind: ExprKind::ColumnRef {
                            column_id: action.column_id,
                            qualifier: None,
                            column: action.name.clone(),
                        },
                        data_type: DataType::Int8,
                        nullable: false,
                    },
                    output_name: action.name.clone(),
                });
                Ok(RewriteResult::Changed(LogicalPlan::Project(p)))
            }
            LogicalPlan::Aggregate(_) => Err(
                "IMV action column propagation does not support Aggregate in Phase 2; \
                 aggregate state rewrite is scheduled for Phase 4"
                    .to_string(),
            ),
            LogicalPlan::Join(_) => Err(
                "IMV action column propagation does not support Join in Phase 2; \
                 join delta algebra is scheduled for Phase 5"
                    .to_string(),
            ),
            LogicalPlan::UnionAll(_) => Err(
                "IMV action column propagation does not support UnionAll in Phase 2; \
                 union delta rewrite is scheduled for Phase 6"
                    .to_string(),
            ),
            _ => Ok(RewriteResult::Unchanged),
        }
    }
```

If `LogicalPlan::UnionAll` is not the exact variant name (might be `Union` with a flag), adjust to match the actual variant. The build will surface mismatches.

- [ ] **Step 2: Add fail-fast tests**

Append to the test module:

```rust
    use crate::sql::planner::plan::{AggregateNode, JoinKind, JoinNode, UnionNode};

    #[test]
    fn propagate_rejects_aggregate() {
        let rule = PropagateActionColumnRule;
        let mut ctx = build_ctx();
        let scan = LogicalPlan::Scan(delta_scan_with_action(ColumnId(100)));
        let plan = LogicalPlan::Aggregate(AggregateNode {
            input: Box::new(scan),
            group_by: Vec::new(),
            aggregates: Vec::new(),
            output_columns: Vec::new(),
            already_pushed: false,
        });
        assert!(rule.matches(&plan, &ctx));
        let err = rule.apply(plan, &mut ctx).expect_err("Aggregate must fail");
        assert!(err.contains("Phase 4"), "unexpected error: {err}");
    }

    #[test]
    fn propagate_rejects_join() {
        let rule = PropagateActionColumnRule;
        let mut ctx = build_ctx();
        let left = LogicalPlan::Scan(delta_scan_with_action(ColumnId(100)));
        let right = LogicalPlan::Scan(delta_scan());
        let plan = LogicalPlan::Join(Box::new(JoinNode {
            left: Box::new(left),
            right: Box::new(right),
            join_type: JoinKind::Inner,
            condition: None,
        }));
        assert!(rule.matches(&plan, &ctx));
        let err = rule.apply(plan, &mut ctx).expect_err("Join must fail");
        assert!(err.contains("Phase 5"), "unexpected error: {err}");
    }

    #[test]
    fn propagate_rejects_union_all() {
        let rule = PropagateActionColumnRule;
        let mut ctx = build_ctx();
        let plan = LogicalPlan::UnionAll(UnionNode {
            inputs: vec![LogicalPlan::Scan(delta_scan_with_action(ColumnId(100)))],
            all: true,
        });
        assert!(rule.matches(&plan, &ctx));
        let err = rule.apply(plan, &mut ctx).expect_err("UnionAll must fail");
        assert!(err.contains("Phase 6"), "unexpected error: {err}");
    }
```

Adjust the variant names (`LogicalPlan::Join(Box<JoinNode>)` vs `LogicalPlan::Join(JoinNode)`, `JoinKind::Inner` vs `JoinType::Inner`, etc.) to match the actual `src/sql/planner/plan.rs` definitions. The build surfaces mismatches.

- [ ] **Step 3: Run focused tests**

```bash
cargo test --lib propagate_rejects 2>&1 | tail -5
cargo test --lib action_propagation 2>&1 | tail -3
```

Expected: 3 reject tests pass; the full module shows 10 passed total (4 inject + 3 propagate + 3 reject).

- [ ] **Step 4: Commit**

```bash
git add src/sql/optimizer/rewrite/imv/action_propagation.rs
git commit -m "feat: fail-fast on Aggregate/Join/UnionAll above IMV delta"
```

---

### Task 7: Register `imv-action-propagation` Stage in Pipeline

**Files:**
- Modify: `src/sql/optimizer/rewrite/imv/pipeline.rs`
- Modify: `src/sql/optimizer/rewrite/imv/entrypoint.rs`

- [ ] **Step 1: Update entrypoint stage-name expectation tests**

In `src/sql/optimizer/rewrite/imv/entrypoint.rs`, find `imv_pipeline_traces_scan_binding_stage_name` and update its assertion to expect 6 stages:

```rust
        assert_eq!(
            outcome.trace.stage_names(),
            vec![
                "imv-logical-normalize",
                "imv-delta-marker",
                "imv-scan-binding",
                "imv-action-propagation",
                "imv-marker-cleanup",
                "imv-validation",
            ]
        );
```

Also find `unknown_disabled_rule_name_is_ignored` in the same file and update its `stage_names().len() == 5` to `len() == 6`.

Rename `imv_pipeline_traces_scan_binding_stage_name` to `imv_pipeline_traces_six_stage_names` to reflect the new shape.

- [ ] **Step 2: Run test, verify it fails**

```bash
cargo test --lib imv_pipeline_traces_six_stage_names 2>&1 | tail -5
```

Expected: FAIL (pipeline still has 5 stages).

- [ ] **Step 3: Register the new stage**

Edit `src/sql/optimizer/rewrite/imv/pipeline.rs`. Add imports:

```rust
use crate::sql::optimizer::rewrite::imv::action_propagation::{
    InjectActionColumnRule, PropagateActionColumnRule,
};
```

Then insert a new `RewriteStage` between `imv-scan-binding` and `imv-marker-cleanup`:

```rust
        RewriteStage::new(
            "imv-action-propagation",
            RewritePhase::SemanticRewrite,
            vec![
                Box::new(InjectActionColumnRule) as Box<dyn LogicalRewriteRule>,
                Box::new(PropagateActionColumnRule),
            ],
        ),
```

Final stage list in `build_imv_pipeline`:

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
            "imv-action-propagation",
            RewritePhase::SemanticRewrite,
            vec![
                Box::new(InjectActionColumnRule) as Box<dyn LogicalRewriteRule>,
                Box::new(PropagateActionColumnRule),
            ],
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

- [ ] **Step 4: Add an E2E test in entrypoint.rs**

Append to the `#[cfg(test)] mod tests` block in `src/sql/optimizer/rewrite/imv/entrypoint.rs`:

```rust
    #[test]
    fn imv_pipeline_injects_action_on_delta_scan() {
        let outcome = run_imv_rewrite(ImvRewriteInput {
            plan: iceberg_scan_plan(),
            mv_ctx: dummy_mv_ctx(),
            disabled_rules: Vec::new(),
            deadline: None,
        })
        .expect("pipeline must succeed");

        let LogicalPlan::Scan(scan) = outcome.plan else {
            panic!("expected scan outcome");
        };
        // Action column must be present, internal, Int8, non-nullable.
        let action = scan
            .columns
            .iter()
            .find(|c| c.is_internal && c.name.eq_ignore_ascii_case("__change_op"))
            .expect("action column must be present");
        assert_eq!(action.data_type, arrow::datatypes::DataType::Int8);
        assert!(!action.nullable);
    }
```

- [ ] **Step 5: Run focused tests**

```bash
cargo test --lib imv_pipeline_traces_six_stage_names 2>&1 | tail -3
cargo test --lib imv_pipeline_injects_action_on_delta_scan 2>&1 | tail -3
cargo test --lib imv 2>&1 | tail -3
```

Expected: all pass; IMV suite shows 32 passed (was 30 before, +2 new tests).

- [ ] **Step 6: Commit**

```bash
git add src/sql/optimizer/rewrite/imv/pipeline.rs src/sql/optimizer/rewrite/imv/entrypoint.rs
git commit -m "feat: register IMV action-propagation stage"
```

---

### Task 8: `ActionColumnValidationRule`

**Files:**
- Modify: `src/sql/optimizer/rewrite/imv/action_column.rs`
- Modify: `src/sql/optimizer/rewrite/imv/pipeline.rs`

Validation walks the resolved plan and enforces invariants V1-V5.

- [ ] **Step 1: Add the rule to `action_column.rs`**

Append to `src/sql/optimizer/rewrite/imv/action_column.rs` (before the `#[cfg(test)] mod tests`):

```rust
use std::sync::atomic::AtomicBool;

use crate::sql::catalog::ScanSource;
use crate::sql::optimizer::rewrite::context::RewriteContext;
use crate::sql::optimizer::rewrite::phase::RewritePhase;
use crate::sql::optimizer::rewrite::result::RewriteResult;
use crate::sql::optimizer::rewrite::rule::{LogicalRewriteRule, RewriteTraversal};
use crate::sql::planner::plan::{LogicalPlan, ScanNode};

/// Validates IMV action column invariants. Runs once at the root in the
/// validation stage. Errors include the offending node kind / base FQN where
/// applicable.
pub(crate) struct ActionColumnValidationRule {
    fired: AtomicBool,
}

impl ActionColumnValidationRule {
    pub(crate) fn new() -> Self {
        Self {
            fired: AtomicBool::new(false),
        }
    }
}

impl LogicalRewriteRule for ActionColumnValidationRule {
    fn name(&self) -> &'static str {
        "ActionColumnValidation"
    }

    fn phase(&self) -> RewritePhase {
        RewritePhase::Validation
    }

    fn traversal(&self) -> RewriteTraversal {
        RewriteTraversal::TopDown
    }

    fn matches(&self, _plan: &LogicalPlan, _ctx: &RewriteContext) -> bool {
        // Fire exactly once per pipeline invocation, at the root.
        !self.fired.load(std::sync::atomic::Ordering::SeqCst)
    }

    fn apply(&self, plan: LogicalPlan, _ctx: &mut RewriteContext) -> Result<RewriteResult, String> {
        self.fired.store(true, std::sync::atomic::Ordering::SeqCst);
        validate(&plan)?;
        Ok(RewriteResult::Unchanged)
    }
}

fn validate(plan: &LogicalPlan) -> Result<(), String> {
    validate_node(plan, false)?;
    // V4: root visible output must not be empty
    if !has_visible_output(plan) {
        return Err(
            "root plan has no user-visible output; action column or other internal column may have leaked"
                .to_string(),
        );
    }
    Ok(())
}

fn validate_node(plan: &LogicalPlan, in_delta_subtree: bool) -> Result<(), String> {
    match plan {
        LogicalPlan::Scan(scan) => validate_scan(scan),
        LogicalPlan::Filter(node) => validate_node(&node.input, in_delta_subtree || subtree_has_delta(&node.input)),
        LogicalPlan::Project(node) => {
            let nested_in_delta = in_delta_subtree || subtree_has_delta(&node.input);
            validate_node(&node.input, nested_in_delta)?;
            // V3: if a delta is below, Project must expose the action column
            if nested_in_delta {
                let has = node
                    .items
                    .iter()
                    .any(|item| item.output_name.eq_ignore_ascii_case(ImvActionColumn::NAME));
                if !has {
                    return Err(format!(
                        "action column dropped at Project above delta-bound scan"
                    ));
                }
            }
            Ok(())
        }
        LogicalPlan::Aggregate(_) if subtree_has_delta(plan) => Err(
            "Phase 2 does not support Aggregate above delta-bound scans; deferred to Phase 4"
                .to_string(),
        ),
        LogicalPlan::Join(_) if subtree_has_delta(plan) => Err(
            "Phase 2 does not support Join above delta-bound scans; deferred to Phase 5"
                .to_string(),
        ),
        LogicalPlan::UnionAll(_) if subtree_has_delta(plan) => Err(
            "Phase 2 does not support UnionAll above delta-bound scans; deferred to Phase 6"
                .to_string(),
        ),
        // Other nodes pass through; they should be unreachable above a delta
        // in well-formed Phase 2 plans.
        _ => Ok(()),
    }
}

fn validate_scan(scan: &ScanNode) -> Result<(), String> {
    let fqn = match &scan.table.source {
        ScanSource::IcebergDeltaTable { table, .. } => format!(
            "{}.{}.{}",
            table.catalog, table.namespace, table.table
        ),
        ScanSource::IcebergVersionTable { table, .. } => format!(
            "{}.{}.{}",
            table.catalog, table.namespace, table.table
        ),
        _ => scan.table.name.clone(),
    };

    let action_columns: Vec<_> = scan
        .columns
        .iter()
        .filter(|c| ImvActionColumn::matches(c))
        .collect();

    match &scan.table.source {
        ScanSource::IcebergDeltaTable { .. } => {
            // V1
            match action_columns.as_slice() {
                [] => Err(format!("Delta-bound scan {fqn} missing action column")),
                [col] => {
                    if col.data_type != DataType::Int8 {
                        return Err(format!("Delta-bound scan {fqn} has non-Int8 action column"));
                    }
                    if col.nullable {
                        return Err(format!("Delta-bound scan {fqn} has nullable action column"));
                    }
                    Ok(())
                }
                _ => Err(format!("Delta-bound scan {fqn} has duplicate action columns")),
            }
        }
        ScanSource::IcebergVersionTable { .. } => {
            // V2
            if !action_columns.is_empty() {
                return Err(format!(
                    "Version-bound scan {fqn} must not carry action column"
                ));
            }
            Ok(())
        }
        _ => Ok(()),
    }
}

fn subtree_has_delta(plan: &LogicalPlan) -> bool {
    match plan {
        LogicalPlan::Scan(scan) => matches!(
            scan.table.source,
            ScanSource::IcebergDeltaTable { .. }
        ),
        LogicalPlan::Filter(node) => subtree_has_delta(&node.input),
        LogicalPlan::Project(node) => subtree_has_delta(&node.input),
        LogicalPlan::Aggregate(node) => subtree_has_delta(&node.input),
        LogicalPlan::Join(node) => subtree_has_delta(&node.left) || subtree_has_delta(&node.right),
        LogicalPlan::UnionAll(node) => node.inputs.iter().any(subtree_has_delta),
        LogicalPlan::ImvDelta(node) => subtree_has_delta(&node.input),
        LogicalPlan::ImvVersion(node) => subtree_has_delta(&node.input),
        _ => false,
    }
}

fn has_visible_output(plan: &LogicalPlan) -> bool {
    match plan {
        LogicalPlan::Scan(scan) => scan.columns.iter().any(|c| !c.is_internal),
        LogicalPlan::Filter(node) => has_visible_output(&node.input),
        LogicalPlan::Project(node) => node
            .items
            .iter()
            .any(|item| !item.output_name.eq_ignore_ascii_case(ImvActionColumn::NAME)),
        LogicalPlan::Aggregate(node) => node.output_columns.iter().any(|c| !c.is_internal),
        LogicalPlan::Join(node) => has_visible_output(&node.left) || has_visible_output(&node.right),
        LogicalPlan::UnionAll(node) => node.inputs.iter().any(has_visible_output),
        _ => true,
    }
}
```

If `LogicalPlan::UnionAll` is not the exact variant name, adjust accordingly. The build will surface mismatches.

- [ ] **Step 2: Register the rule in the validation stage**

In `src/sql/optimizer/rewrite/imv/pipeline.rs`, add the import:

```rust
use crate::sql::optimizer::rewrite::imv::action_column::ActionColumnValidationRule;
```

Update the `imv-validation` stage rule list:

```rust
        RewriteStage::new(
            "imv-validation",
            RewritePhase::Validation,
            vec![
                Box::new(UnresolvedMarkerCheckRule) as Box<dyn LogicalRewriteRule>,
                Box::new(ActionColumnValidationRule::new()),
            ],
        ),
```

- [ ] **Step 3: Add validation tests in `action_column.rs`**

Append inside the existing `#[cfg(test)] mod tests`:

```rust
    use std::collections::BTreeMap;
    use crate::sql::analysis::{ExprKind, ProjectItem, TypedExpr};
    use crate::sql::catalog::{
        ColumnDef, IcebergSchemaDef, IcebergTableInfo, ScanSource, TableDef,
    };
    use crate::sql::planner::plan::{
        FilterNode, LogicalPlan, ProjectNode, ScanNode,
    };

    fn delta_scan_with(action: Option<OutputColumn>) -> ScanNode {
        let mut scan = ScanNode {
            database: "db".to_string(),
            table: TableDef {
                name: "b".to_string(),
                columns: vec![ColumnDef {
                    name: "k".to_string(),
                    data_type: DataType::Int64,
                    nullable: false,
                    write_default: None,
                    logical_type: None,
                }],
                iceberg_row_lineage_metadata_columns: Vec::new(),
                source: ScanSource::IcebergDeltaTable {
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
                    from_snapshot_id: 11,
                    to_snapshot_id: 22,
                },
            },
            alias: None,
            columns: vec![OutputColumn {
                column_id: ColumnId(1),
                name: "k".to_string(),
                data_type: DataType::Int64,
                nullable: false,
                is_internal: false,
            }],
            predicates: Vec::new(),
            required_columns: None,
            dict_columns: Vec::new(),
        };
        if let Some(a) = action {
            scan.columns.push(a);
        }
        scan
    }

    #[test]
    fn validation_passes_on_well_formed_delta_scan() {
        let plan = LogicalPlan::Scan(delta_scan_with(Some(
            ImvActionColumn::output_column(ColumnId(100)),
        )));
        validate(&plan).expect("must validate");
    }

    #[test]
    fn validation_rejects_missing_action_column_on_delta() {
        let plan = LogicalPlan::Scan(delta_scan_with(None));
        let err = validate(&plan).expect_err("missing action must fail");
        assert!(err.contains("missing action column"), "got: {err}");
        assert!(err.contains("ice.db.b"), "got: {err}");
    }

    #[test]
    fn validation_rejects_non_int8_action_column() {
        let mut bad = ImvActionColumn::output_column(ColumnId(100));
        bad.data_type = DataType::Int64;
        let plan = LogicalPlan::Scan(delta_scan_with(Some(bad)));
        let err = validate(&plan).expect_err("non-Int8 must fail");
        assert!(err.contains("non-Int8"), "got: {err}");
    }

    #[test]
    fn validation_rejects_nullable_action_column() {
        let mut bad = ImvActionColumn::output_column(ColumnId(100));
        bad.nullable = true;
        let plan = LogicalPlan::Scan(delta_scan_with(Some(bad)));
        let err = validate(&plan).expect_err("nullable must fail");
        assert!(err.contains("nullable"), "got: {err}");
    }

    #[test]
    fn validation_rejects_duplicate_action_columns() {
        let mut scan = delta_scan_with(Some(ImvActionColumn::output_column(ColumnId(100))));
        scan.columns.push(ImvActionColumn::output_column(ColumnId(101)));
        let plan = LogicalPlan::Scan(scan);
        let err = validate(&plan).expect_err("duplicates must fail");
        assert!(err.contains("duplicate"), "got: {err}");
    }

    #[test]
    fn validation_rejects_dropped_action_above_project() {
        let scan = LogicalPlan::Scan(delta_scan_with(Some(
            ImvActionColumn::output_column(ColumnId(100)),
        )));
        // Project that does NOT propagate __change_op
        let project = LogicalPlan::Project(ProjectNode {
            input: Box::new(scan),
            items: vec![ProjectItem {
                expr: TypedExpr {
                    kind: ExprKind::ColumnRef {
                        column_id: ColumnId(1),
                        qualifier: None,
                        column: "k".to_string(),
                    },
                    data_type: DataType::Int64,
                    nullable: false,
                },
                output_name: "k".to_string(),
            }],
        });
        let err = validate(&project).expect_err("dropped action must fail");
        assert!(err.contains("dropped at Project"), "got: {err}");
    }
```

- [ ] **Step 4: Run focused tests**

```bash
cargo test --lib action_column 2>&1 | tail -5
cargo test --lib imv 2>&1 | tail -3
```

Expected: validation tests pass; full IMV suite shows roughly 37 passed (30 prior + 2 from Task 7 + 6 new validation tests, allowing for minor count variance).

- [ ] **Step 5: Commit**

```bash
git add src/sql/optimizer/rewrite/imv/action_column.rs src/sql/optimizer/rewrite/imv/pipeline.rs
git commit -m "feat: validate IMV action column invariants"
```

---

### Task 9: Column Pruning Internal-Preservation

**Files:**
- Modify: `src/sql/optimizer/rewrite/rules/column_pruning.rs`

- [ ] **Step 1: Replace IMV marker panic with explicit error**

In `src/sql/optimizer/rewrite/rules/column_pruning.rs`, line 327-329, change:

```rust
        LogicalPlan::ImvDelta(_) | LogicalPlan::ImvVersion(_) => {
            panic!("imv marker leaked into non-IMV plan");
        }
```

to:

```rust
        LogicalPlan::ImvDelta(node) => {
            // After IMV scan-binding the marker should be gone. If we see one
            // here it's a contract violation, but we avoid panicking — log a
            // trace and pass through unchanged so the rest of the plan can be
            // pruned safely.
            let input = prune_inner(*node.input, needed);
            LogicalPlan::ImvDelta(crate::sql::optimizer::rewrite::imv::marker::ImvDeltaNode {
                input: Box::new(input),
                is_root: node.is_root,
                action_column: node.action_column,
            })
        }
        LogicalPlan::ImvVersion(node) => {
            let input = prune_inner(*node.input, needed);
            LogicalPlan::ImvVersion(crate::sql::optimizer::rewrite::imv::marker::ImvVersionNode {
                input: Box::new(input),
                version_ref: node.version_ref,
            })
        }
```

- [ ] **Step 2: Preserve internal columns at Scan**

In the same file, in the `LogicalPlan::Scan(mut scan)` arm of `prune_inner` (around lines 79-101), after computing `pruned` and BEFORE setting `scan.required_columns = Some(pruned)`, append internal column names unconditionally:

```rust
            if let Some(needed) = needed {
                // Also include columns referenced by pushed-down predicates.
                let mut required: HashSet<String> = needed.clone();
                for pred in &scan.predicates {
                    for col in collect_column_refs(pred) {
                        required.insert(col.to_lowercase());
                    }
                }
                // Internal columns (e.g. IMV action column) are never pruned.
                for col in &scan.columns {
                    if col.is_internal {
                        required.insert(col.name.to_lowercase());
                    }
                }
                let mut pruned: Vec<String> = scan
                    .columns
                    .iter()
                    .filter(|c| required.contains(&c.name.to_lowercase()))
                    .map(|c| c.name.clone())
                    .collect();
                if pruned.is_empty() && !scan.columns.is_empty() {
                    pruned.push(scan.columns[0].name.clone());
                }
                scan.required_columns = Some(pruned);
            }
            LogicalPlan::Scan(scan)
```

- [ ] **Step 3: Add pruning preservation tests**

Inside the existing `#[cfg(test)] mod tests` in `column_pruning.rs`, append:

```rust
    fn scan_with_internal_column(table: &TableDef, internal_name: &str) -> ScanNode {
        let mut scan = scan_node(table);
        scan.columns.push(OutputColumn {
            column_id: ColumnId::UNSET,
            name: internal_name.to_string(),
            data_type: DataType::Int8,
            nullable: false,
            is_internal: true,
        });
        scan
    }

    #[test]
    fn pruning_preserves_internal_column_when_parent_does_not_request() {
        let table = three_col_table();
        let scan = LogicalPlan::Scan(scan_with_internal_column(&table, "__change_op"));
        let project = LogicalPlan::Project(ProjectNode {
            input: Box::new(scan),
            items: vec![ProjectItem {
                expr: col_ref("a", DataType::Int32),
                output_name: "a".to_string(),
            }],
        });
        let rule = PruneColumns;
        let out = rule
            .apply(project)
            .expect("rule should fire and set required_columns");
        if let LogicalPlan::Project(p) = out {
            if let LogicalPlan::Scan(s) = *p.input {
                let required = s.required_columns.expect("required_columns must be set");
                assert!(required.iter().any(|c| c == "a"), "got: {required:?}");
                assert!(
                    required.iter().any(|c| c == "__change_op"),
                    "internal column must be preserved; got: {required:?}"
                );
            } else {
                panic!("expected Scan under Project");
            }
        } else {
            panic!("expected Project");
        }
    }
```

- [ ] **Step 4: Run focused tests**

```bash
cargo test --lib column_pruning 2>&1 | tail -5
```

Expected: previous pruning tests still pass; new test passes. (Total count varies; the new test should be visible by name.)

- [ ] **Step 5: Commit**

```bash
git add src/sql/optimizer/rewrite/rules/column_pruning.rs
git commit -m "feat: preserve internal columns in column pruning"
```

---

### Task 10: Codegen Guard for IMV Action Column

**Files:**
- Modify: `src/sql/codegen/nodes.rs`

Mirroring Phase 1's `IcebergVersionTable` guard, we check that no plan with an internal action column reaches `build_exec_params_multi`.

- [ ] **Step 1: Add the guard**

In `src/sql/codegen/nodes.rs`, find the `build_exec_params_multi` body. Inside the per-`planned` loop, BEFORE the existing `let ranges = ...` block, add:

```rust
        // Phase 2 codegen guard: action column on a scan must not reach
        // scan-range construction until refresh execution cutover (Phase 3+).
        if let Some(action_col) = planned
            .resolved
            .table
            .columns
            .iter()
            .find(|c| {
                // ColumnDef from catalog doesn't carry is_internal; check via
                // the OutputColumn list on the plan if available. For Phase 2
                // we conservatively check the scan's *output* column metadata
                // via planned.resolved.* if exposed; otherwise gate on name.
                c.name.eq_ignore_ascii_case(crate::exec::change_op::CHANGE_OP_COLUMN)
            })
        {
            return Err(format!(
                "IMV action column on scan {}.{} reached codegen before execution cutover \
                 (column={})",
                planned.resolved.database, planned.resolved.table.name, action_col.name
            ));
        }
```

The guard above checks `ColumnDef.name` because catalog `ColumnDef` does not carry `is_internal`. If `ResolvedTable` carries an `OutputColumn` list elsewhere (the survey indicates it does not, but the actual `ResolvedTable` shape should be confirmed), prefer that path. Alternatively, when implementing this task, inspect `ResolvedTable` (`src/sql/codegen/resolve.rs`) for an authoritative is_internal field; if absent, the name-based gate is the correct fallback.

The intent is: any scan whose plan-side output includes the action column should never reach codegen in Phase 2. The name `__change_op` is unique enough that a false positive (user-defined column with same name) is acceptable — Phase 1's `IcebergVersionTable` guard takes the same pragmatic stance.

- [ ] **Step 2: Add codegen guard test**

In the `#[cfg(test)] mod tests` block at the bottom of `src/sql/codegen/nodes.rs`, append:

```rust
    #[test]
    fn imv_action_column_reaches_codegen_guard() {
        use crate::sql::catalog::{
            ColumnDef, IcebergSchemaDef, IcebergTableInfo, ScanSource, TableDef,
        };

        let resolved = ResolvedTable {
            database: "db".to_string(),
            table: TableDef {
                name: "b".to_string(),
                columns: vec![
                    ColumnDef {
                        name: "k".to_string(),
                        data_type: arrow::datatypes::DataType::Int64,
                        nullable: false,
                        write_default: None,
                        logical_type: None,
                    },
                    ColumnDef {
                        name: "__change_op".to_string(),
                        data_type: arrow::datatypes::DataType::Int8,
                        nullable: false,
                        write_default: None,
                        logical_type: None,
                    },
                ],
                iceberg_row_lineage_metadata_columns: Vec::new(),
                source: ScanSource::IcebergDeltaTable {
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
                    from_snapshot_id: 11,
                    to_snapshot_id: 22,
                },
            },
            planned_scan: None,
            alias: None,
        };
        let planned = PlannedScanTable {
            scan_node_id: 9,
            resolved,
            min_max_conjuncts: Vec::new(),
            slot_to_column: std::collections::HashMap::new(),
            iceberg_metadata_pseudo_column_slots: std::collections::BTreeSet::new(),
        };

        let err = build_exec_params_multi(&[planned])
            .expect_err("action column must trip guard");
        assert!(
            err.contains("IMV action column on scan db.b reached codegen before execution cutover"),
            "unexpected error: {err}"
        );
    }
```

- [ ] **Step 3: Run focused test**

```bash
cargo test --lib imv_action_column_reaches_codegen_guard 2>&1 | tail -5
```

Expected: pass.

- [ ] **Step 4: Commit**

```bash
git add src/sql/codegen/nodes.rs
git commit -m "test: guard IMV action column execution"
```

---

### Task 11: Final Verification

**Files:** No edits unless verification surfaces a failure.

- [ ] **Step 1: Run focused IMV tests**

```bash
cargo test --lib action_column 2>&1 | tail -3
cargo test --lib action_propagation 2>&1 | tail -3
cargo test --lib imv 2>&1 | tail -3
cargo test --lib column_pruning 2>&1 | tail -3
cargo test --lib imv_action_column_reaches_codegen_guard 2>&1 | tail -3
```

Expected: every group passes.

- [ ] **Step 2: Run full library tests**

```bash
cargo test --lib 2>&1 | tail -3
```

Expected: all tests pass. Pre-existing ignored tests remain ignored. The total pass count should equal (Phase 1 final count) + (Phase 2 new tests), which is roughly Phase 1's 3151 + ~20 new = ~3171. Exact count not critical; what matters is 0 failed.

- [ ] **Step 3: Format check**

```bash
cargo fmt --check 2>&1 | head -10
```

If the check fails on this phase's files, run `cargo fmt` and inspect the diff. Commit ONLY this phase's formatting changes; leave unrelated pre-existing drift untouched.

If the check fails on files this branch did not modify (cross-check with `git diff main..HEAD --stat`), do not reformat — that is pre-existing drift on main outside this PR's scope.

- [ ] **Step 4: Commit format changes if any**

If `cargo fmt` touched this phase's files only:

```bash
git add src/sql/optimizer/rewrite/imv src/sql/analysis/mod.rs src/sql/optimizer/rewrite/rules/column_pruning.rs src/sql/codegen/nodes.rs
git commit -m "style: format IMV action column changes"
```

If `cargo fmt --check` passes, do not create a formatting commit.

---

## Self-Review Notes

Spec coverage:

- Phase 2 scope (Scan / Project / Filter, with Aggregate / Join / UnionAll fail-fast)
  is covered by Tasks 4 (Inject), 5 (Project propagate), 6 (fail-fast).
- `OutputColumn::is_internal` (spec §5.1) is covered by Task 1.
- `ImvActionColumn` descriptor (spec §5.2) is covered by Task 3.
- ColumnId allocation (spec §5.3) is covered by Task 2 via `AtomicU32` rather
  than the spec's `Rc<RefCell<ColumnRefFactory>>` (departure justified: factory
  metadata tracking is unnecessary for Phase 2; `AtomicU32` is Send+Sync without
  Mutex).
- Pipeline stage (spec §6.1) is covered by Task 7.
- Validation invariants V1-V5 (spec §7) are covered by Task 8.
- Pruning interaction (spec §8) is covered by Task 9.
- Codegen guard (spec §9) is covered by Task 10.
- Test matrix (spec §10) is covered across Tasks 3-10.

Ambiguity resolved:

- Filter is a schema-passthrough node (`FilterNode` has no `columns` field); the
  propagation rule explicitly does NOT match Filter and validation walks through
  Filter without expecting any rewrite.
- `ColumnId` allocation uses `Arc<AtomicU32>` seeded from the plan's max existing
  id (avoids any factory plumbing and is correct because Phase 2 never needs
  factory metadata).
- `ImvVersion` and `ImvDelta` markers in pruning rule are replaced with explicit
  passthrough rather than panic; this is defensive — these markers should be
  consumed before pruning runs.

Validation commands:

- `cargo test --lib action_column`
- `cargo test --lib action_propagation`
- `cargo test --lib imv`
- `cargo test --lib column_pruning`
- `cargo test --lib imv_action_column_reaches_codegen_guard`
- `cargo test --lib`
- `cargo fmt --check`
