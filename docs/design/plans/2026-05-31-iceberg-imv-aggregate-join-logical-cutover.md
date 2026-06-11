# Iceberg IMV Aggregate/Join Logical Cutover Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Cut Iceberg aggregate and join-aggregate MV incremental refresh over to the IMV logical rewrite path, with explicit target-state lookup and aggregate-state merge in the plan.

**Architecture:** Extend the existing IMV rewrite pipeline so `Delta(Aggregate(...))` becomes a signed aggregate-delta plan, and `Delta(Aggregate(Join(...)))` expands into the supported two-branch inner/cross join delta algebra. Add refresh-only target-state scan and aggregate-state merge plan contracts so the merge sink receives a normal change stream; remove aggregate/join legacy fallback and fail fast on unsupported shapes.

**Tech Stack:** Rust, Arrow `RecordBatch`/`Chunk`, NovaRocks logical optimizer rewrite pipeline (`src/sql/optimizer/rewrite/imv/`), SQL planner/codegen/lowering, Iceberg MV refresh lifecycle, SQL golden tests.

**Spec:** `docs/design/specs/2026-05-31-iceberg-imv-aggregate-join-logical-cutover-design.md`

---

## Scope Check

This plan covers one tightly coupled subsystem: Iceberg IMV aggregate/join-aggregate cutover. The target-state scan, aggregate merge, and join delta rewrite are dependent pieces of the same end-to-end refresh path; splitting them into independent projects would create temporary behavior that cannot be validated by the existing SQL refresh lifecycle.

---

## File Structure

| File | Responsibility | Change |
| --- | --- | --- |
| `src/sql/optimizer/rewrite/imv/aggregate_rewrite.rs` | Rewrite `Delta(Aggregate)` into signed state aggregate plus merge contract | Create |
| `src/sql/optimizer/rewrite/imv/join_delta.rs` | Rewrite supported join-aggregate delta into two delta branches | Create |
| `src/sql/optimizer/rewrite/imv/target_state.rs` | Build refresh-only target-state scan and aggregate merge helpers | Create |
| `src/sql/optimizer/rewrite/imv/pipeline.rs` | Register aggregate/join stages before generic delta pushdown | Modify |
| `src/sql/optimizer/rewrite/imv/mod.rs` | Export new IMV modules | Modify |
| `src/sql/optimizer/rewrite/imv/delta_pushdown.rs` | Replace Phase 4/5 diagnostics with final aggregate/join cutover diagnostics | Modify |
| `src/sql/catalog.rs` | Add refresh-only `ScanSource::IcebergMvTargetState` metadata | Modify |
| `src/sql/planner/plan.rs` | Add logical `AggregateStateMergeNode` and traversal support | Modify |
| `src/sql/optimizer/operator.rs` | Add optimizer operator for aggregate-state merge | Modify |
| `src/sql/optimizer/convert.rs` | Convert logical plan to optimizer expression and back | Modify |
| `src/sql/optimizer/cascades_rules/implement.rs` | Implement logical merge as physical merge | Modify |
| `src/sql/explain.rs` | Print stable merge/target-state plan evidence | Modify |
| `src/sql/codegen/nodes.rs` | Build refresh-only target/version scan ranges and merge exec node | Modify |
| `src/exec/node/mod.rs` | Add `ExecNodeKind::AggregateStateMerge` | Modify |
| `src/exec/operators/aggregate_state_merge.rs` | Runtime operator that merges target state plus signed delta and emits change stream | Create |
| `src/exec/operators/mod.rs` | Register merge operator factory | Modify |
| `src/engine/mv/iceberg_refresh.rs` | Cut aggregate/join refresh over to rewrite execution; remove legacy fallback | Modify |
| `src/engine/mv/iceberg_aggregate_state.rs` | Reuse merge helpers from runtime operator; keep old chunk-level tests | Modify |
| `sql-tests/optimizer/imv_aggregate_logical_cutover.sql` | Optimizer plan-shape test for single-base aggregate | Create |
| `sql-tests/optimizer/imv_join_aggregate_logical_cutover.sql` | Optimizer plan-shape test for join aggregate | Create |
| `sql-tests/iceberg-ivm/sql/iceberg_ivm_aggregate_target.sql` | Add cutover evidence assertions to existing correctness test | Modify |
| `sql-tests/iceberg-ivm/sql/iceberg_ivm_join_aggregate.sql` | Add cutover evidence assertions to existing correctness test | Modify |

---

## Task 1: Add target-state scan metadata contract

**Files:**
- Modify: `src/sql/catalog.rs`
- Test: `src/sql/catalog.rs` inline `#[cfg(test)]`

- [ ] **Step 1: Write the failing tests**

Add this test module near existing `ScanSource` tests in `src/sql/catalog.rs`:

```rust
#[cfg(test)]
mod imv_target_state_tests {
    use super::*;

    fn sample_columns() -> Vec<ColumnDef> {
        vec![
            ColumnDef {
                name: "region".to_string(),
                data_type: arrow::datatypes::DataType::Utf8,
                nullable: true,
                write_default: None,
                logical_type: None,
            },
            ColumnDef {
                name: "c".to_string(),
                data_type: arrow::datatypes::DataType::Int64,
                nullable: true,
                write_default: None,
                logical_type: None,
            },
        ]
    }

    #[test]
    fn iceberg_mv_target_state_scan_source_carries_logical_contract() {
        let source = ScanSource::IcebergMvTargetState(IcebergMvTargetStateScan {
            catalog: "ice".to_string(),
            database: "ns".to_string(),
            table: "mv_sales".to_string(),
            columns: sample_columns(),
            group_key_names: vec!["region".to_string()],
            aggregate_state_names: vec!["c".to_string()],
        });

        let ScanSource::IcebergMvTargetState(scan) = source else {
            panic!("expected target-state scan source");
        };
        assert_eq!(scan.fqn(), "ice.ns.mv_sales");
        assert_eq!(scan.group_key_names, vec!["region"]);
        assert_eq!(scan.aggregate_state_names, vec!["c"]);
    }
}
```

- [ ] **Step 2: Run the failing test**

```bash
cargo test --lib sql::catalog::imv_target_state_tests --quiet
```

Expected: compile fails because `IcebergMvTargetStateScan` and `ScanSource::IcebergMvTargetState` do not exist.

- [ ] **Step 3: Implement the metadata type and variant**

Add this struct near the existing Iceberg scan metadata structs:

```rust
#[derive(Clone, Debug, PartialEq)]
pub(crate) struct IcebergMvTargetStateScan {
    pub(crate) catalog: String,
    pub(crate) database: String,
    pub(crate) table: String,
    pub(crate) columns: Vec<ColumnDef>,
    pub(crate) group_key_names: Vec<String>,
    pub(crate) aggregate_state_names: Vec<String>,
}

impl IcebergMvTargetStateScan {
    pub(crate) fn fqn(&self) -> String {
        format!("{}.{}.{}", self.catalog, self.database, self.table)
    }
}
```

Add the variant to `ScanSource`:

```rust
IcebergMvTargetState(IcebergMvTargetStateScan),
```

Update all `match ScanSource` sites in `src/sql/catalog.rs` so this variant behaves like an Iceberg table with respect to table schema and column lookup:

```rust
ScanSource::IcebergMvTargetState(scan) => &scan.columns,
```

- [ ] **Step 4: Run the test**

```bash
cargo test --lib sql::catalog::imv_target_state_tests --quiet
```

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add src/sql/catalog.rs
git commit -m "feat(iceberg-imv): add target state scan metadata"
```

---

## Task 2: Add logical aggregate-state merge node

**Files:**
- Modify: `src/sql/planner/plan.rs`
- Modify: `src/sql/optimizer/operator.rs`
- Modify: `src/sql/optimizer/convert.rs`
- Modify: `src/sql/optimizer/cascades_rules/implement.rs`
- Modify: `src/sql/explain.rs`

- [ ] **Step 1: Write failing unit tests for logical round-trip and EXPLAIN**

Add to `src/sql/planner/plan.rs` test module:

```rust
#[test]
fn aggregate_state_merge_node_preserves_inputs_and_output_columns() {
    use crate::sql::analysis::OutputColumn;
    use crate::sql::column_id::ColumnId;

    fn empty_values_for_test() -> LogicalPlan {
        LogicalPlan::Values(ValuesNode {
            rows: vec![],
            columns: vec![],
            required_output_columns: None,
        })
    }

    let old_input = empty_values_for_test();
    let delta_input = empty_values_for_test();
    let node = AggregateStateMergeNode {
        old_input: Box::new(old_input),
        delta_input: Box::new(delta_input),
        group_key_names: vec!["region".to_string()],
        aggregate_state_names: vec!["c".to_string(), "s".to_string()],
        change_op_column: "__change_op".to_string(),
        output_columns: vec![
            OutputColumn {
                column_id: ColumnId::new_for_test(1),
                name: "region".to_string(),
                data_type: arrow::datatypes::DataType::Utf8,
                nullable: true,
                is_internal: false,
            },
            OutputColumn {
                column_id: ColumnId::new_for_test(2),
                name: "c".to_string(),
                data_type: arrow::datatypes::DataType::Int64,
                nullable: true,
                is_internal: false,
            },
        ],
    };

    assert_eq!(node.group_key_names, vec!["region"]);
    assert_eq!(node.aggregate_state_names, vec!["c", "s"]);
    assert_eq!(node.change_op_column, "__change_op");
    assert_eq!(node.output_columns.len(), 2);
}
```

Add to `src/sql/explain.rs` tests:

```rust
#[test]
fn explain_prints_aggregate_state_merge_evidence() {
    fn empty_values_for_test() -> LogicalPlan {
        LogicalPlan::Values(ValuesNode {
            rows: vec![],
            columns: vec![],
            required_output_columns: None,
        })
    }

    let plan = LogicalPlan::AggregateStateMerge(AggregateStateMergeNode {
        old_input: Box::new(empty_values_for_test()),
        delta_input: Box::new(empty_values_for_test()),
        group_key_names: vec!["region".to_string()],
        aggregate_state_names: vec!["c".to_string()],
        change_op_column: "__change_op".to_string(),
        output_columns: vec![],
    });
    let text = explain_logical_plan_for_test(&plan);
    assert!(text.contains("AggregateStateMerge"), "{text}");
    assert!(text.contains("keys=[region]"), "{text}");
    assert!(text.contains("states=[c]"), "{text}");
}
```

If `explain_logical_plan_for_test` does not exist, add this private test helper in `src/sql/explain.rs`:

```rust
#[cfg(test)]
fn explain_logical_plan_for_test(plan: &LogicalPlan) -> String {
    explain_plan(plan, ExplainLevel::Normal).join("\n")
}
```

- [ ] **Step 2: Run the failing tests**

```bash
cargo test --lib aggregate_state_merge_node_preserves_inputs_and_output_columns explain_prints_aggregate_state_merge_evidence --quiet
```

Expected: compile fails because `AggregateStateMergeNode` and `LogicalPlan::AggregateStateMerge` do not exist.

- [ ] **Step 3: Add the logical node**

Add to `src/sql/planner/plan.rs`:

```rust
#[derive(Clone, Debug)]
pub(crate) struct AggregateStateMergeNode {
    pub(crate) old_input: Box<LogicalPlan>,
    pub(crate) delta_input: Box<LogicalPlan>,
    pub(crate) group_key_names: Vec<String>,
    pub(crate) aggregate_state_names: Vec<String>,
    pub(crate) change_op_column: String,
    pub(crate) output_columns: Vec<crate::sql::analysis::OutputColumn>,
}
```

Add to `LogicalPlan`:

```rust
AggregateStateMerge(AggregateStateMergeNode),
```

Update logical traversal helpers in `plan.rs` so this node has two children, in stable order:

```rust
LogicalPlan::AggregateStateMerge(node) => vec![node.old_input.as_ref(), node.delta_input.as_ref()],
```

For mutable child replacement, preserve the same order:

```rust
LogicalPlan::AggregateStateMerge(node) => {
    node.old_input = Box::new(children.remove(0));
    node.delta_input = Box::new(children.remove(0));
}
```

- [ ] **Step 4: Add optimizer/operator conversion plumbing**

In `src/sql/optimizer/operator.rs`, add:

```rust
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub(crate) struct AggregateStateMergeOp {
    pub(crate) group_key_names: Vec<String>,
    pub(crate) aggregate_state_names: Vec<String>,
    pub(crate) change_op_column: String,
}
```

Add logical and physical operator variants:

```rust
LogicalOperator::AggregateStateMerge(AggregateStateMergeOp)
PhysicalOperator::AggregateStateMerge(AggregateStateMergeOp)
```

In `src/sql/optimizer/convert.rs`, convert `LogicalPlan::AggregateStateMerge` into an optimizer expression with two children and convert it back with the original fields. Preserve `output_columns` in the logical plan by deriving from the operator's output properties if existing conversion stores output there; otherwise carry it in `AggregateStateMergeOp`.

Use this conversion shape:

```rust
LogicalPlan::AggregateStateMerge(node) => OptExpr::new(
    Operator::Logical(LogicalOperator::AggregateStateMerge(AggregateStateMergeOp {
        group_key_names: node.group_key_names.clone(),
        aggregate_state_names: node.aggregate_state_names.clone(),
        change_op_column: node.change_op_column.clone(),
    })),
    vec![convert_plan(*node.old_input)?, convert_plan(*node.delta_input)?],
)
```

In `src/sql/optimizer/cascades_rules/implement.rs`, implement the logical operator by replacing it with the physical operator and preserving the two children:

```rust
LogicalOperator::AggregateStateMerge(op) => Some(Operator::Physical(
    PhysicalOperator::AggregateStateMerge(op.clone()),
)),
```

- [ ] **Step 5: Add EXPLAIN formatting**

In `src/sql/explain.rs`, add a stable line for the logical and physical node:

```rust
LogicalPlan::AggregateStateMerge(node) => {
    out.push(format!(
        "{}AggregateStateMerge keys=[{}] states=[{}] change_op={}",
        pad,
        node.group_key_names.join(","),
        node.aggregate_state_names.join(","),
        node.change_op_column
    ));
    format_node(&node.old_input, level, indent + 1, out);
    format_node(&node.delta_input, level, indent + 1, out);
}
```

- [ ] **Step 6: Run the tests**

```bash
cargo test --lib aggregate_state_merge_node_preserves_inputs_and_output_columns explain_prints_aggregate_state_merge_evidence --quiet
```

Expected: PASS.

- [ ] **Step 7: Commit**

```bash
git add src/sql/planner/plan.rs src/sql/optimizer/operator.rs src/sql/optimizer/convert.rs src/sql/optimizer/cascades_rules/implement.rs src/sql/explain.rs
git commit -m "feat(iceberg-imv): add aggregate state merge logical node"
```

---

## Task 3: Make refresh-only version/target-state scans executable

**Files:**
- Modify: `src/sql/codegen/nodes.rs`
- Modify: `src/engine/mv/refresh_context.rs`
- Test: `src/sql/codegen/nodes.rs` inline `#[cfg(test)]`

- [ ] **Step 1: Write failing codegen guard tests**

Add tests near the existing Iceberg scan tests in `src/sql/codegen/nodes.rs`:

```rust
#[test]
fn iceberg_version_scan_without_refresh_context_fails_fast() {
    let err = build_iceberg_version_scan_node_for_test(None)
        .expect_err("version scan outside MV refresh must fail");
    assert!(
        err.to_string().contains("Iceberg version scan requires MV refresh context"),
        "{err}"
    );
}

#[test]
fn iceberg_target_state_scan_without_refresh_context_fails_fast() {
    let err = build_iceberg_target_state_scan_node_for_test(None)
        .expect_err("target-state scan outside MV refresh must fail");
    assert!(
        err.to_string().contains("Iceberg target-state scan requires MV refresh context"),
        "{err}"
    );
}
```

Add private test helpers that construct the smallest `ScanNode` for `ScanSource::IcebergVersionTable` and `ScanSource::IcebergMvTargetState`, then call the same production builder used by normal scan codegen. Keep the helpers in `#[cfg(test)]`; do not introduce production-only constructors.

- [ ] **Step 2: Run the failing tests**

```bash
cargo test --lib iceberg_version_scan_without_refresh_context_fails_fast iceberg_target_state_scan_without_refresh_context_fails_fast --quiet
```

Expected: compile fails for the target-state variant and existing version path still reports the old "reached scan-range construction before execution cutover" guard.

- [ ] **Step 3: Replace the old version-scan cutover guard**

In `src/sql/codegen/nodes.rs`, replace the current `IcebergVersionTable` guard with:

```rust
let refresh_ctx = ctx.mv_refresh_ctx.as_ref().ok_or_else(|| {
    CodegenError::unsupported("Iceberg version scan requires MV refresh context")
})?;
```

Build the scan range using the table identity and snapshot/version recorded in `ScanSource::IcebergVersionTable`. Reuse the same Iceberg table lookup already used by `build_iceberg_delta_scan_node`, but pass a fixed snapshot selector instead of a delta selector.

- [ ] **Step 4: Add target-state scan codegen**

Add a `ScanSource::IcebergMvTargetState(scan)` arm in `src/sql/codegen/nodes.rs`:

```rust
ScanSource::IcebergMvTargetState(scan) => {
    let refresh_ctx = ctx.mv_refresh_ctx.as_ref().ok_or_else(|| {
        CodegenError::unsupported("Iceberg target-state scan requires MV refresh context")
    })?;
    build_iceberg_target_state_scan_node(ctx, scan, refresh_ctx, node)
}
```

Implement `build_iceberg_target_state_scan_node` by reusing the normal Iceberg scan builder against the MV target table at the refresh target snapshot. Project only `group_key_names + aggregate_state_names + "_row_id"`; the merge node owns `__change_op` emission.

- [ ] **Step 5: Run the tests**

```bash
cargo test --lib iceberg_version_scan_without_refresh_context_fails_fast iceberg_target_state_scan_without_refresh_context_fails_fast --quiet
```

Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add src/sql/codegen/nodes.rs src/engine/mv/refresh_context.rs
git commit -m "feat(iceberg-imv): enable refresh-only target scans"
```

---

## Task 4: Register aggregate/join rewrite stages before generic pushdown

**Files:**
- Modify: `src/sql/optimizer/rewrite/imv/mod.rs`
- Modify: `src/sql/optimizer/rewrite/imv/pipeline.rs`
- Create: `src/sql/optimizer/rewrite/imv/aggregate_rewrite.rs`
- Create: `src/sql/optimizer/rewrite/imv/join_delta.rs`
- Create: `src/sql/optimizer/rewrite/imv/target_state.rs`

- [ ] **Step 1: Write the failing pipeline order test**

Add to `src/sql/optimizer/rewrite/imv/pipeline.rs` tests:

```rust
#[test]
fn pipeline_runs_join_and_aggregate_rewrite_before_generic_delta_pushdown() {
    let p = build_imv_pipeline();
    let names = p.stage_names();
    let join = names
        .iter()
        .position(|n| *n == "imv-join-delta")
        .expect("join delta stage must exist");
    let agg = names
        .iter()
        .position(|n| *n == "imv-aggregate-state")
        .expect("aggregate state stage must exist");
    let pushdown = names
        .iter()
        .position(|n| *n == "imv-delta-pushdown")
        .expect("delta pushdown stage must exist");

    assert!(join < agg, "stage order: {names:?}");
    assert!(agg < pushdown, "stage order: {names:?}");
}
```

- [ ] **Step 2: Run the failing test**

```bash
cargo test --lib pipeline_runs_join_and_aggregate_rewrite_before_generic_delta_pushdown --quiet
```

Expected: FAIL because the new stages are absent.

- [ ] **Step 3: Add module declarations and empty rule structs**

In `src/sql/optimizer/rewrite/imv/mod.rs`:

```rust
pub(crate) mod aggregate_rewrite;
pub(crate) mod join_delta;
pub(crate) mod target_state;
```

Create `src/sql/optimizer/rewrite/imv/join_delta.rs`:

```rust
use crate::sql::optimizer::rewrite::context::RewriteContext;
use crate::sql::optimizer::rewrite::phase::RewritePhase;
use crate::sql::optimizer::rewrite::result::RewriteResult;
use crate::sql::optimizer::rewrite::rule::{LogicalRewriteRule, RewriteTraversal};
use crate::sql::planner::plan::LogicalPlan;

pub(crate) struct RewriteJoinAggregateDeltaRule;

impl LogicalRewriteRule for RewriteJoinAggregateDeltaRule {
    fn name(&self) -> &'static str {
        "RewriteJoinAggregateDelta"
    }

    fn phase(&self) -> RewritePhase {
        RewritePhase::StructuralRewrite
    }

    fn traversal(&self) -> RewriteTraversal {
        RewriteTraversal::TopDown
    }

    fn matches(&self, _plan: &LogicalPlan, _ctx: &RewriteContext) -> bool {
        false
    }

    fn apply(&self, _plan: LogicalPlan, _ctx: &mut RewriteContext) -> Result<RewriteResult, String> {
        Ok(RewriteResult::Unchanged)
    }
}
```

Create `src/sql/optimizer/rewrite/imv/aggregate_rewrite.rs` with the same trait implementation, using struct `RewriteAggregateStateRule` and rule name `"RewriteAggregateState"`.

Create `src/sql/optimizer/rewrite/imv/target_state.rs`:

```rust
use crate::sql::catalog::{ColumnDef, IcebergMvTargetStateScan, ScanSource};

pub(crate) fn build_target_state_scan_source(
    catalog: String,
    database: String,
    table: String,
    columns: Vec<ColumnDef>,
    group_key_names: Vec<String>,
    aggregate_state_names: Vec<String>,
) -> ScanSource {
    ScanSource::IcebergMvTargetState(IcebergMvTargetStateScan {
        catalog,
        database,
        table,
        columns,
        group_key_names,
        aggregate_state_names,
    })
}
```

- [ ] **Step 4: Register stages**

In `src/sql/optimizer/rewrite/imv/pipeline.rs`, import the rules:

```rust
use crate::sql::optimizer::rewrite::imv::aggregate_rewrite::RewriteAggregateStateRule;
use crate::sql::optimizer::rewrite::imv::join_delta::RewriteJoinAggregateDeltaRule;
```

Insert stages after `imv-delta-marker` and before `imv-delta-pushdown`:

```rust
RewriteStage::new(
    "imv-join-delta",
    RewritePhase::StructuralRewrite,
    vec![Box::new(RewriteJoinAggregateDeltaRule) as Box<dyn LogicalRewriteRule>],
),
RewriteStage::new(
    "imv-aggregate-state",
    RewritePhase::StructuralRewrite,
    vec![Box::new(RewriteAggregateStateRule) as Box<dyn LogicalRewriteRule>],
),
```

- [ ] **Step 5: Run the test**

```bash
cargo test --lib pipeline_runs_join_and_aggregate_rewrite_before_generic_delta_pushdown --quiet
```

Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add src/sql/optimizer/rewrite/imv/mod.rs src/sql/optimizer/rewrite/imv/pipeline.rs src/sql/optimizer/rewrite/imv/aggregate_rewrite.rs src/sql/optimizer/rewrite/imv/join_delta.rs src/sql/optimizer/rewrite/imv/target_state.rs
git commit -m "feat(iceberg-imv): register aggregate rewrite stages"
```

---

## Task 5: Rewrite single-base `Delta(Aggregate)` to signed aggregate state

**Files:**
- Modify: `src/sql/optimizer/rewrite/imv/aggregate_rewrite.rs`
- Modify: `src/sql/optimizer/rewrite/imv/delta_pushdown.rs`
- Test: `src/sql/optimizer/rewrite/imv/aggregate_rewrite.rs`

- [ ] **Step 1: Write failing tests for function mapping and unsupported shapes**

Add to `aggregate_rewrite.rs`:

```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn signed_state_function_maps_supported_aggregates() {
        assert_eq!(signed_state_function("count").unwrap(), "count_state_signed");
        assert_eq!(signed_state_function("sum").unwrap(), "sum_state_signed");
        assert_eq!(signed_state_function("avg").unwrap(), "avg_state_signed");
        assert_eq!(signed_state_function("min").unwrap(), "min_state_signed");
        assert_eq!(signed_state_function("max").unwrap(), "max_state_signed");
        assert_eq!(signed_state_function("bool_or").unwrap(), "bool_or_state_signed");
        assert_eq!(signed_state_function("bool_and").unwrap(), "bool_and_state_signed");
        assert_eq!(
            signed_state_function("count_distinct").unwrap(),
            "count_distinct_state_signed"
        );
        assert_eq!(
            signed_state_function("approx_count_distinct").unwrap(),
            "approx_count_distinct_state_signed"
        );
    }

    #[test]
    fn signed_state_function_rejects_unsupported_aggregate() {
        let err = signed_state_function("median").expect_err("median must be unsupported");
        assert!(err.contains("unsupported IMV aggregate function median"), "{err}");
    }
}
```

- [ ] **Step 2: Run the failing tests**

```bash
cargo test --lib sql::optimizer::rewrite::imv::aggregate_rewrite::tests --quiet
```

Expected: compile fails because `signed_state_function` does not exist.

- [ ] **Step 3: Implement function mapping**

Add to `aggregate_rewrite.rs`:

```rust
pub(crate) fn signed_state_function(name: &str) -> Result<&'static str, String> {
    match name.to_ascii_lowercase().as_str() {
        "count" => Ok("count_state_signed"),
        "sum" => Ok("sum_state_signed"),
        "avg" => Ok("avg_state_signed"),
        "min" => Ok("min_state_signed"),
        "max" => Ok("max_state_signed"),
        "bool_or" => Ok("bool_or_state_signed"),
        "bool_and" => Ok("bool_and_state_signed"),
        "count_distinct" => Ok("count_distinct_state_signed"),
        "approx_count_distinct" => Ok("approx_count_distinct_state_signed"),
        other => Err(format!("unsupported IMV aggregate function {other}")),
    }
}
```

- [ ] **Step 4: Implement `RewriteAggregateStateRule`**

Update `matches` so it only matches a root `ImvDelta` whose child is `LogicalPlan::Aggregate`.

In `apply`, perform these validations in this order and return `Err` with the shown text:

```rust
if aggregate.group_expr.is_empty() {
    return Err("Iceberg IMV aggregate rewrite requires at least one GROUP BY key".to_string());
}
if aggregate.distinct {
    return Err("Iceberg IMV aggregate rewrite does not support SELECT DISTINCT".to_string());
}
```

For each aggregate call:

1. Resolve the signed function with `signed_state_function`.
2. Wrap each original aggregate input value and the propagated action column into the struct input expected by state combinators.
3. Replace the aggregate function name with the signed state function.
4. Preserve output names and types from the original aggregate output columns.

The output plan shape must be:

```text
AggregateStateMerge
  old_input: ScanSource::IcebergMvTargetState
  delta_input: Aggregate(signed_state_functions)
```

Use `target_state::build_target_state_scan_source` for `old_input`, and construct `LogicalPlan::AggregateStateMerge` with:

```rust
AggregateStateMergeNode {
    old_input: Box::new(target_state_scan),
    delta_input: Box::new(signed_aggregate_plan),
    group_key_names,
    aggregate_state_names,
    change_op_column: "__change_op".to_string(),
    output_columns: aggregate.output_columns.clone(),
}
```

- [ ] **Step 5: Replace old Phase 4 aggregate error**

In `src/sql/optimizer/rewrite/imv/delta_pushdown.rs`, replace the aggregate arm with:

```rust
LogicalPlan::Aggregate(_) => {
    return Err(
        "Iceberg IMV aggregate rewrite did not consume Delta(Aggregate); \
         verify RewriteAggregateStateRule ran before PushDeltaThroughUnary"
            .to_string(),
    );
}
```

- [ ] **Step 6: Run focused tests**

```bash
cargo test --lib sql::optimizer::rewrite::imv::aggregate_rewrite::tests --quiet
cargo test --lib sql::optimizer::rewrite::imv::delta_pushdown --quiet
```

Expected: PASS.

- [ ] **Step 7: Commit**

```bash
git add src/sql/optimizer/rewrite/imv/aggregate_rewrite.rs src/sql/optimizer/rewrite/imv/delta_pushdown.rs
git commit -m "feat(iceberg-imv): rewrite aggregate delta state"
```

---

## Task 6: Rewrite supported join aggregate delta algebra

**Files:**
- Modify: `src/sql/optimizer/rewrite/imv/join_delta.rs`
- Modify: `src/sql/optimizer/rewrite/imv/delta_pushdown.rs`
- Test: `src/sql/optimizer/rewrite/imv/join_delta.rs`

- [ ] **Step 1: Write failing tests for join kind validation**

Add to `join_delta.rs`:

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use crate::sql::analysis::JoinKind;

    #[test]
    fn supported_join_delta_kinds_are_inner_and_cross_only() {
        assert!(join_delta_kind_supported(JoinKind::Inner));
        assert!(join_delta_kind_supported(JoinKind::Cross));
        assert!(!join_delta_kind_supported(JoinKind::LeftOuter));
        assert!(!join_delta_kind_supported(JoinKind::RightOuter));
        assert!(!join_delta_kind_supported(JoinKind::FullOuter));
        assert!(!join_delta_kind_supported(JoinKind::LeftSemi));
        assert!(!join_delta_kind_supported(JoinKind::LeftAnti));
    }
}
```

- [ ] **Step 2: Run the failing test**

```bash
cargo test --lib sql::optimizer::rewrite::imv::join_delta::tests --quiet
```

Expected: compile fails because `join_delta_kind_supported` does not exist.

- [ ] **Step 3: Implement join kind validation**

Add:

```rust
pub(crate) fn join_delta_kind_supported(kind: crate::sql::analysis::JoinKind) -> bool {
    matches!(
        kind,
        crate::sql::analysis::JoinKind::Inner | crate::sql::analysis::JoinKind::Cross
    )
}
```

- [ ] **Step 4: Implement `RewriteJoinAggregateDeltaRule`**

Match only this shape:

```text
ImvDelta
  Aggregate
    Join(left, right, kind = Inner|Cross)
```

For unsupported join kinds, return:

```rust
Err(format!(
    "Iceberg IMV join aggregate rewrite supports inner/cross joins only, got {:?}",
    join.join_type
))
```

For supported joins, rewrite the aggregate input to:

```text
UnionAll
  Join(ImvDelta(left), IcebergVersionTable(right, FromSnapshot))
  Join(IcebergVersionTable(left, ToSnapshot), ImvDelta(right))
```

Rules:

1. Preserve the original join condition on both branches.
2. Preserve original join output columns.
3. Use `ImvDeltaNode { is_root: false, action_column: "__change_op" }` for the delta side.
4. Use the existing `ScanSource::IcebergVersionTable` contract for the version side.
5. Leave the outer `ImvDelta(Aggregate(...))` marker in place only until the aggregate rewrite consumes it in the next stage; do not emit a plan that reaches generic delta pushdown with `Delta(Join)`.

- [ ] **Step 5: Replace old Phase 5 join error**

In `src/sql/optimizer/rewrite/imv/delta_pushdown.rs`, replace the join arm with:

```rust
LogicalPlan::Join(_) => {
    return Err(
        "Iceberg IMV join delta rewrite did not consume Delta(Join); \
         verify RewriteJoinAggregateDeltaRule ran before PushDeltaThroughUnary"
            .to_string(),
    );
}
```

- [ ] **Step 6: Run focused tests**

```bash
cargo test --lib sql::optimizer::rewrite::imv::join_delta::tests --quiet
cargo test --lib sql::optimizer::rewrite::imv::delta_pushdown --quiet
```

Expected: PASS.

- [ ] **Step 7: Commit**

```bash
git add src/sql/optimizer/rewrite/imv/join_delta.rs src/sql/optimizer/rewrite/imv/delta_pushdown.rs
git commit -m "feat(iceberg-imv): rewrite join aggregate delta"
```

---

## Task 7: Add aggregate-state merge exec/operator

**Files:**
- Create: `src/exec/operators/aggregate_state_merge.rs`
- Modify: `src/exec/operators/mod.rs`
- Modify: `src/exec/node/mod.rs`
- Modify: `src/sql/codegen/nodes.rs`
- Modify: `src/engine/mv/iceberg_aggregate_state.rs`

- [ ] **Step 1: Write failing runtime merge tests**

Create `src/exec/operators/aggregate_state_merge.rs` with this test module first:

```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn merge_operator_emits_delete_for_old_state_and_insert_for_new_state() {
        let old = aggregate_state_chunk_for_test(vec![("east", 2_i64, 300_i64)]);
        let delta = signed_delta_state_chunk_for_test(vec![("east", -1_i8, 1_i64, -100_i64)]);
        let output = merge_state_chunks_for_test(vec![old], vec![delta]).unwrap();

        assert_eq!(output.num_rows(), 2);
        assert_eq!(string_value(&output, "region", 0), "east");
        assert_eq!(int8_value(&output, "__change_op", 0), -1);
        assert_eq!(int64_value(&output, "c", 0), 2);
        assert_eq!(int8_value(&output, "__change_op", 1), 1);
        assert_eq!(int64_value(&output, "c", 1), 1);
    }

    #[test]
    fn merge_operator_emits_insert_only_for_new_group() {
        let delta = signed_delta_state_chunk_for_test(vec![("west", 1_i8, 1_i64, 80_i64)]);
        let output = merge_state_chunks_for_test(vec![], vec![delta]).unwrap();

        assert_eq!(output.num_rows(), 1);
        assert_eq!(string_value(&output, "region", 0), "west");
        assert_eq!(int8_value(&output, "__change_op", 0), 1);
    }
}
```

The helper functions in this test module should build Arrow `RecordBatch` values with columns `region`, `c`, `s`, and `__change_op`, then wrap them as `Chunk`. Keep them private to the test module.

- [ ] **Step 2: Run the failing tests**

```bash
cargo test --lib exec::operators::aggregate_state_merge --quiet
```

Expected: compile fails because the operator module is not registered and the helper functions do not exist.

- [ ] **Step 3: Extract reusable merge core**

In `src/engine/mv/iceberg_aggregate_state.rs`, expose a crate-private helper:

```rust
pub(crate) fn merge_aggregate_state_chunks_for_change_stream(
    old_chunks: &[Chunk],
    delta_chunks: &[Chunk],
    layout: &AggregateMvLayout,
) -> Result<Vec<Chunk>, String> {
    let old_rows = build_old_state_map(old_chunks, layout)?;
    let merge_result = merge_aggregate_state_batches_with_retractions(&old_rows, delta_chunks, layout)?;
    build_aggregate_change_chunks(&merge_result, layout)
}
```

Keep existing `merge_aggregate_target_state` behavior unchanged and rewrite it to call this helper before adding Iceberg target-specific row-id/partition handling.

- [ ] **Step 4: Implement operator plan and factory**

In `aggregate_state_merge.rs`, add:

```rust
#[derive(Clone, Debug)]
pub(crate) struct AggregateStateMergePlan {
    pub(crate) layout: crate::connector::starrocks::table::mv_agg_state::AggregateMvLayout,
}
```

Implement an operator that buffers all chunks from its old-state child and delta-state child, calls `merge_aggregate_state_chunks_for_change_stream`, then emits the returned chunks. Surface errors directly; do not return an empty stream on merge failure.

In `src/exec/operators/mod.rs`, add:

```rust
pub(crate) mod aggregate_state_merge;
```

Register the factory in the same style as `iceberg_merge_sink`.

- [ ] **Step 5: Add exec node kind and codegen**

In `src/exec/node/mod.rs`, add:

```rust
AggregateStateMerge(crate::exec::operators::aggregate_state_merge::AggregateStateMergePlan),
```

In `src/sql/codegen/nodes.rs`, lower physical `AggregateStateMerge` into this exec node with two children. Build `AggregateMvLayout` from the logical node's group key and aggregate state column names; reuse layout construction from `src/engine/mv/iceberg_refresh.rs` rather than duplicating column-index rules.

- [ ] **Step 6: Run runtime tests**

```bash
cargo test --lib exec::operators::aggregate_state_merge --quiet
```

Expected: PASS.

- [ ] **Step 7: Commit**

```bash
git add src/exec/operators/aggregate_state_merge.rs src/exec/operators/mod.rs src/exec/node/mod.rs src/sql/codegen/nodes.rs src/engine/mv/iceberg_aggregate_state.rs
git commit -m "feat(iceberg-imv): execute aggregate state merge"
```

---

## Task 8: Add optimizer SQL plan-shape coverage

**Files:**
- Create: `sql-tests/optimizer/imv_aggregate_logical_cutover.sql`
- Create: `sql-tests/optimizer/imv_join_aggregate_logical_cutover.sql`

- [ ] **Step 1: Add aggregate plan-shape SQL test**

Create `sql-tests/optimizer/imv_aggregate_logical_cutover.sql`:

```sql
-- @sequential=true
-- @tags=mv,iceberg,ivm,aggregate,rewrite
-- @explain_contains=AggregateStateMerge
-- @explain_contains=IcebergMvTargetState
-- @explain_contains=count_state_signed
-- @explain_contains=sum_state_signed

CREATE TABLE imv_agg_base (
  k BIGINT,
  region STRING,
  amount BIGINT
);

CREATE MATERIALIZED VIEW imv_agg_mv
DISTRIBUTED BY HASH(region) BUCKETS 1
PROPERTIES ('storage_engine' = 'iceberg')
AS
SELECT region, COUNT(*) AS c, SUM(amount) AS s
FROM imv_agg_base
GROUP BY region;

EXPLAIN
SELECT region, COUNT(*) AS c, SUM(amount) AS s
FROM imv_agg_base
GROUP BY region;
```

- [ ] **Step 2: Add join-aggregate plan-shape SQL test**

Create `sql-tests/optimizer/imv_join_aggregate_logical_cutover.sql`:

```sql
-- @sequential=true
-- @tags=mv,iceberg,ivm,join,aggregate,rewrite
-- @explain_contains=AggregateStateMerge
-- @explain_contains=IcebergMvTargetState
-- @explain_contains=IcebergVersionTable
-- @explain_contains=UNION
-- @explain_contains=sum_state_signed

CREATE TABLE imv_join_fact (
  id BIGINT,
  dim_id BIGINT,
  amount BIGINT
);

CREATE TABLE imv_join_dim (
  id BIGINT,
  region STRING
);

CREATE MATERIALIZED VIEW imv_join_agg_mv
DISTRIBUTED BY HASH(region) BUCKETS 1
PROPERTIES ('storage_engine' = 'iceberg')
AS
SELECT d.region, COUNT(*) AS c, SUM(f.amount) AS s
FROM imv_join_fact AS f
JOIN imv_join_dim AS d ON f.dim_id = d.id
GROUP BY d.region;

EXPLAIN
SELECT d.region, COUNT(*) AS c, SUM(f.amount) AS s
FROM imv_join_fact AS f
JOIN imv_join_dim AS d ON f.dim_id = d.id
GROUP BY d.region;
```

- [ ] **Step 3: Run optimizer tests**

```bash
cargo run --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests -- \
  --suite optimizer --only imv_aggregate_logical_cutover,imv_join_aggregate_logical_cutover --mode verify
```

Expected before implementation is wired to the SQL runner: FAIL with missing plan evidence. After Tasks 1-7: PASS.

- [ ] **Step 4: Commit**

```bash
git add sql-tests/optimizer/imv_aggregate_logical_cutover.sql sql-tests/optimizer/imv_join_aggregate_logical_cutover.sql
git commit -m "test(iceberg-imv): cover aggregate logical cutover plans"
```

---

## Task 9: Cut single-base aggregate refresh over to rewrite execution

**Files:**
- Modify: `src/engine/mv/iceberg_refresh.rs`
- Modify: `sql-tests/iceberg-ivm/sql/iceberg_ivm_aggregate_target.sql`

- [ ] **Step 1: Add cutover evidence to the SQL test**

In `sql-tests/iceberg-ivm/sql/iceberg_ivm_aggregate_target.sql`, add metadata above the refresh query that mutates the base after the initial MV load:

```sql
-- @explain_contains=AggregateStateMerge
-- @explain_contains=IcebergMvTargetState
-- @explain_contains=__change_op
```

Keep the existing result checks unchanged.

- [ ] **Step 2: Run the failing SQL test**

```bash
source docker/iceberg-rest/runtime/current/env.sh
docker/iceberg-rest/up.sh
cargo run --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests -- \
  --config "$NOVAROCKS_SQL_TEST_CONFIG" \
  --suite iceberg-ivm --only iceberg_ivm_aggregate_target --mode verify
```

Expected before cutover: FAIL because the old aggregate refresh path does not expose `AggregateStateMerge` evidence.

- [ ] **Step 3: Replace aggregate refresh fallback**

In `src/engine/mv/iceberg_refresh.rs`, change `refresh_single_aggregate_iceberg_mv` so the incremental branch calls the same rewrite execution surface used by projection/filter cutover:

```rust
let outcome = execute_iceberg_mv_rewrite_refresh(
    engine,
    session,
    mv,
    refresh_ctx,
    IcebergMvRefreshShape::SingleAggregate,
)
.await?;
```

The function must:

1. Pass `Some(&IcebergMvRefreshContext)` into `execute_query_with_options`.
2. Require the rewrite trace to contain `RewriteAggregateState` and `AggregateStateMerge`.
3. Return an error if the rewrite result is `Unchanged`, if trace is missing, or if the plan contains no `AggregateStateMerge`.
4. Send the emitted change stream to `IcebergMergeSink` exactly like PF cutover.

Remove the call chain:

```rust
incremental_refresh_iceberg_aggregate_mv
iceberg_aggregate_incremental_delta_select_sql
execute_delta_source_query
materialize_aggregate_result_chunks
apply_iceberg_aggregate_delta_chunks
```

Only delete helper functions when no join refresh path still uses them; otherwise leave them for Task 10 and mark them private to the remaining caller.

- [ ] **Step 4: Run the SQL test**

```bash
source docker/iceberg-rest/runtime/current/env.sh
cargo run --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests -- \
  --config "$NOVAROCKS_SQL_TEST_CONFIG" \
  --suite iceberg-ivm --only iceberg_ivm_aggregate_target --mode verify
```

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add src/engine/mv/iceberg_refresh.rs sql-tests/iceberg-ivm/sql/iceberg_ivm_aggregate_target.sql
git commit -m "feat(iceberg-imv): cut over aggregate refresh"
```

---

## Task 10: Cut join-aggregate refresh over to rewrite execution

**Files:**
- Modify: `src/engine/mv/iceberg_refresh.rs`
- Modify: `sql-tests/iceberg-ivm/sql/iceberg_ivm_join_aggregate.sql`

- [ ] **Step 1: Add cutover evidence to the SQL test**

In `sql-tests/iceberg-ivm/sql/iceberg_ivm_join_aggregate.sql`, add metadata above the second `REFRESH MATERIALIZED VIEW join_agg_mv_${uuid0};`:

```sql
-- @explain_contains=AggregateStateMerge
-- @explain_contains=IcebergVersionTable
-- @explain_contains=IcebergMvTargetState
```

Keep result queries 5 and 6 unchanged so the MV and base query remain compared.

- [ ] **Step 2: Run the failing SQL test**

```bash
source docker/iceberg-rest/runtime/current/env.sh
docker/iceberg-rest/up.sh
cargo run --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests -- \
  --config "$NOVAROCKS_SQL_TEST_CONFIG" \
  --suite iceberg-ivm --only iceberg_ivm_join_aggregate --mode verify
```

Expected before cutover: FAIL because the old join-aggregate refresh path does not expose rewrite evidence.

- [ ] **Step 3: Replace join-aggregate refresh fallback**

In `src/engine/mv/iceberg_refresh.rs`, change `refresh_join_aggregate_iceberg_mv` so the incremental branch calls `execute_iceberg_mv_rewrite_refresh` with shape `IcebergMvRefreshShape::JoinAggregate`.

The function must require trace evidence:

```rust
require_rewrite_trace(&outcome, &[
    "RewriteJoinAggregateDelta",
    "RewriteAggregateState",
    "AggregateStateMerge",
])?;
```

Remove the old call chain:

```rust
incremental_refresh_iceberg_join_aggregate_mv
plan_join_delta_branches
execute_join_aggregate_delta_branch
apply_iceberg_aggregate_delta_chunks
```

Delete helper structs/functions that become unused after the cutover. Keep shared layout/schema helpers that are still used by the merge operator.

- [ ] **Step 4: Run the SQL test**

```bash
source docker/iceberg-rest/runtime/current/env.sh
cargo run --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests -- \
  --config "$NOVAROCKS_SQL_TEST_CONFIG" \
  --suite iceberg-ivm --only iceberg_ivm_join_aggregate --mode verify
```

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add src/engine/mv/iceberg_refresh.rs sql-tests/iceberg-ivm/sql/iceberg_ivm_join_aggregate.sql
git commit -m "feat(iceberg-imv): cut over join aggregate refresh"
```

---

## Task 11: Harden fail-fast boundaries and remove telemetry-only fallback

**Files:**
- Modify: `src/engine/mv/iceberg_refresh.rs`
- Modify: `src/sql/optimizer/rewrite/imv/action_column.rs`
- Modify: `src/sql/optimizer/rewrite/imv/delta_pushdown.rs`
- Test: inline tests in touched modules

- [ ] **Step 1: Write failing tests for no fallback**

Add to `src/engine/mv/iceberg_refresh.rs` tests:

```rust
#[test]
fn aggregate_rewrite_error_is_not_downgraded_to_legacy_fallback() {
    let err = simulate_rewrite_refresh_error_for_test("unsupported IMV aggregate function median")
        .expect_err("rewrite error must propagate");
    assert!(
        err.to_string().contains("unsupported IMV aggregate function median"),
        "{err}"
    );
}
```

The test helper should call the same error handling function used by `try_run_imv_rewrite_pipeline`; it must not execute a real query.

- [ ] **Step 2: Run the failing test**

```bash
cargo test --lib aggregate_rewrite_error_is_not_downgraded_to_legacy_fallback --quiet
```

Expected: FAIL if the code still logs and continues.

- [ ] **Step 3: Replace telemetry-only behavior**

In `try_run_imv_rewrite_pipeline`, replace the old aggregate/join telemetry path with:

```rust
let rewritten = run_imv_rewrite(plan, rewrite_ctx)
    .map_err(|err| anyhow::anyhow!("Iceberg IMV rewrite failed: {err}"))?;
if !rewritten.changed {
    anyhow::bail!("Iceberg IMV rewrite made no changes for refresh shape {shape:?}");
}
```

No aggregate/join incremental refresh function may call a legacy SQL-string delta path after this point.

- [ ] **Step 4: Update validation diagnostics**

In `action_column.rs` and `delta_pushdown.rs`, remove references to "Phase 2", "Phase 4", "Phase 5", and "scheduled". Use final-state diagnostics:

```rust
"Iceberg IMV rewrite does not support this aggregate shape"
"Iceberg IMV rewrite does not support this join shape"
```

- [ ] **Step 5: Run focused tests**

```bash
cargo test --lib sql::optimizer::rewrite::imv engine::mv::iceberg_refresh --quiet
```

Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add src/engine/mv/iceberg_refresh.rs src/sql/optimizer/rewrite/imv/action_column.rs src/sql/optimizer/rewrite/imv/delta_pushdown.rs
git commit -m "refactor(iceberg-imv): remove aggregate legacy fallback"
```

---

## Task 12: End-to-end validation and cleanup

**Files:**
- Modify: `docs/design/specs/2026-05-31-iceberg-imv-aggregate-join-logical-cutover-design.md` only if implementation changes the approved contract
- Modify: `docs/design/plans/2026-05-31-iceberg-imv-aggregate-join-logical-cutover.md` checkbox statuses only while executing

- [ ] **Step 1: Run formatting**

```bash
cargo fmt
```

Expected: exits 0.

- [ ] **Step 2: Run focused Rust tests**

```bash
cargo test --lib sql::optimizer::rewrite::imv --quiet
cargo test --lib exec::operators::aggregate_state_merge --quiet
cargo test --lib engine::mv::iceberg_aggregate_state --quiet
```

Expected: all pass.

- [ ] **Step 3: Run optimizer plan-shape tests**

```bash
cargo run --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests -- \
  --suite optimizer --only imv_aggregate_logical_cutover,imv_join_aggregate_logical_cutover --mode verify
```

Expected: both pass and include `AggregateStateMerge`, `IcebergMvTargetState`, and signed state function evidence.

- [ ] **Step 4: Run Iceberg IVM correctness tests**

```bash
source docker/iceberg-rest/runtime/current/env.sh
docker/iceberg-rest/up.sh
cargo run --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests -- \
  --config "$NOVAROCKS_SQL_TEST_CONFIG" \
  --suite iceberg-ivm --only iceberg_ivm_aggregate_target,iceberg_ivm_join_aggregate --mode verify
```

Expected: both pass; join aggregate result query and base query remain equal.

- [ ] **Step 5: Run clippy after the feature compiles**

```bash
cargo clippy --all-targets --all-features
```

Expected: exits 0.

- [ ] **Step 6: Commit validation-only cleanup**

If formatting or cleanup changed files:

```bash
git add .
git commit -m "chore(iceberg-imv): finalize aggregate cutover"
```

If no files changed, do not create an empty commit.

---

## Self-Review

**Spec coverage:**
- Logical target-state lookup: Tasks 1, 3, 5, 7, 9, 10.
- State merge in plan, not sink: Tasks 2, 7, 9, 10.
- Single-base aggregate rewrite: Task 5 and Task 9.
- Join aggregate two-branch algebra for inner/cross: Task 6 and Task 10.
- Refresh lifecycle remains responsible for Iceberg transactions/publish/recovery: Tasks 9 and 10 call existing refresh execution and sink paths; optimizer additions carry only metadata.
- No legacy fallback: Task 11.
- Observability/EXPLAIN evidence: Tasks 2, 8, 9, 10.
- Correctness SQL coverage: Tasks 9, 10, 12.

**Placeholder scan:** The plan avoids open-ended placeholder instructions. Each task lists exact files, commands, expected outcomes, and concrete code or SQL blocks for the first implementation pass.

**Type consistency:** `IcebergMvTargetStateScan`, `ScanSource::IcebergMvTargetState`, `AggregateStateMergeNode`, `AggregateStateMergeOp`, and `AggregateStateMergePlan` are introduced before later tasks reference them. `__change_op` is consistently the change operation column name. Stage names are stable: `imv-join-delta`, `imv-aggregate-state`, `RewriteJoinAggregateDelta`, `RewriteAggregateState`, and `AggregateStateMerge`.
