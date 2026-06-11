# OQ-5 Runtime Filter — Stage 1 (core) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Move runtime-filter (RF) planning into a `PhysicalPlanNode` pass so EXPLAIN and codegen share one annotation, add cardinality gating + within-fragment probe push-down, and make RF visible in `EXPLAIN VERBOSE` — replacing the existing post-codegen thrift pass.

**Architecture:** A new `runtime_filter_pass` runs once on the physical tree right after `extract_best` (`src/sql/optimizer/mod.rs:145`). It annotates each eligible hash-join `PhysicalPlanNode` with a build-side `RuntimeFilterDesc` list and pushes a matching probe `RuntimeFilterProbe` down to the deepest descendant node that can bind the probe column (within the same fragment for Stage 1). `explain.rs` renders these annotations; `fragment_builder.rs` consumes them to emit thrift `TRuntimeFilterDescription` + the `RuntimeFilterPlanResult` handed to `coordinator.rs`. The execution layer (`runtime_filter_hub`, build sink, scan probe, `TRuntimeFilterDescription` shape) is untouched.

**Tech Stack:** Rust; NovaRocks standalone optimizer (`src/sql/optimizer`), codegen (`src/sql/codegen`), EXPLAIN (`src/sql/explain.rs`); `sql-tests/optimizer` golden suite with `-- @explain_contains`.

**Spec:** `docs/design/specs/2026-06-01-oq-5-runtime-filter-wiring-design.md` (read §3–§9, §13 first).

---

## Repo conventions (read before starting)

- **Build profile for correctness iteration:** `cargo build` (dev). Use `cargo test` for unit tests. See `CLAUDE.md` §8.2.
- **Unit tests** live inline (`#[cfg(test)] mod tests`) in the same file as the code. Run a single one with `cargo test <test_name> -- --nocapture`.
- **SQL golden (EXPLAIN) tests** need a running standalone-server. Bring up env + server:
  ```bash
  source docker/iceberg-rest/runtime/current/env.sh   # if the generated entry exists
  LOG=/tmp/novarocks-rf.log
  NO_PROXY=127.0.0.1,localhost target/debug/novarocks standalone-server \
    --config "$NOVAROCKS_STANDALONE_CONFIG" >"$LOG" 2>&1 &
  SRV_PID=$!
  for i in $(seq 1 60); do grep -q '^NOVAROCKS_READY ' "$LOG" && break
    kill -0 "$SRV_PID" 2>/dev/null || { tail -20 "$LOG"; exit 1; }; sleep 1; done
  grep -q '^NOVAROCKS_READY ' "$LOG" || { echo timeout; kill -9 "$SRV_PID"; exit 1; }
  ```
  If no generated entry exists, start with `--port 9030` and target `127.0.0.1:9030` instead.
- **sql-tests runner** (golden EXPLAIN suite):
  ```bash
  cargo run --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests -- \
    --suite optimizer --only <case> --mode <record|verify>
  ```
  `--mode record` writes `sql-tests/optimizer/result/<case>.result`; `--mode verify` checks it.
- **Commits:** English message. **Do NOT add `Co-Authored-By: Claude` trailers** (project rule).
- **StarRocks reference** for plan diffing: `~/project/starrocks`, FE on 9030 (see `starrocks-fe-on-novarocks` skill). Used in Task B and Task 12.

---

## File structure

| File | Responsibility | Change |
|------|----------------|--------|
| `src/sql/optimizer/runtime_filter_pass.rs` | **NEW.** Physical-tree RF annotation pass: eligibility, gating, within-fragment push-down. | Create |
| `src/sql/optimizer/physical_plan.rs` | Physical tree node. | Add `build_runtime_filters` / `probe_runtime_filters` fields |
| `src/sql/optimizer/mod.rs` | optimize() driver; `is_known_rule_name`. | Call pass after `extract_best`; register rule name; declare module |
| `src/sql/explain.rs` | Physical-plan EXPLAIN formatter. | Render build/probe RF lines on join + scan/project/filter/agg |
| `src/sql/codegen/fragment_builder.rs` | Physical → thrift fragments. | Consume annotations in `visit_hash_join` + scan/project/agg; build `RuntimeFilterPlanResult`; drop old post-pass call |
| `src/sql/codegen/nodes.rs` | thrift node builders. | `build_hash_join_node` accepts build RF descriptors |
| `src/sql/optimizer/runtime_filter_planner.rs` | OLD post-codegen thrift pass. | **Delete** in Task 11 (reuse its `TRuntimeFilterDescription` construction in Task 8 first) |
| `sql-tests/optimizer/sql/runtime_filter_*.sql` | Golden plan cases. | Create |

**Key types already in the tree (do not redefine):**
- `PhysicalPlanNode { op: Operator, children: Vec<PhysicalPlanNode>, stats: Statistics, output_columns: Vec<OutputColumn> }` — `src/sql/optimizer/physical_plan.rs:9`
- `Operator::PhysicalHashJoin(PhysicalHashJoinOp)` — `operator.rs:482`
- `PhysicalHashJoinOp { join_type: JoinKind, eq_conditions: Vec<PhysicalHashJoinEqCondition>, other_condition, distribution: JoinDistribution }` — `operator.rs:301`
- `PhysicalHashJoinEqCondition { left: TypedExpr, right: TypedExpr, null_safe: bool }` — `operator.rs:309`
- `TypedExpr { kind: ExprKind, data_type, nullable }`, `ExprKind::ColumnRef { column_id: ColumnId, .. }` — `src/sql/analysis/mod.rs:278` / `:293`
- `OutputColumn { column_id: ColumnId, name, data_type, nullable, is_internal }` — `analysis/mod.rs:29`
- `Statistics { output_row_count: f64, column_statistics }`, `compute_size()` — `statistics.rs:30`
- `OptimizerOptions::is_enabled(name)` — `options.rs:65`

---

## Task A: Stage 0 — verify build-side orientation (no code; correctness gate)

The pass will assume `build_expr = eq.right` and that **the right child (`children[1]`) is the build side**. `visit_hash_join` (`fragment_builder.rs:1240-1241`) does `left = visit(children[0]); right = visit(children[1])`, and eq pairs are oriented `left↔children[0]`, `right↔children[1]`. Confirm the **execution** side treats the right input as build.

- [ ] **Step 1: Find where the join build side is chosen in execution lowering.**

Run:
```bash
grep -rn "build_side\|is_build\|build_child\|probe_side\|right.*build\|BuildSide\|build_input" src/lower/node/hash_join.rs src/exec/operators/hashjoin/ | head -30
```
Read `src/lower/node/hash_join.rs` around the hits and `src/exec/operators/hashjoin/hash_join_build_sink.rs` factory construction. Determine: does the build sink consume the join's **right** input (`children[1]`)?

- [ ] **Step 2: Record the verdict in the spec.**

Append a short note under spec §14 risk #1 stating: "Confirmed: right child (`children[1]`) = build side" (or, if not, the actual mapping). If the build side is the **left** child, the pass must use `build_expr = eq.left` instead — adjust Task 3/4 accordingly and note it here.

- [ ] **Step 3: Commit the note.**
```bash
git add docs/design/specs/2026-06-01-oq-5-runtime-filter-wiring-design.md
git commit -m "docs(oq-5): record hash-join build-side orientation verdict (stage 0)"
```

---

## Task B: Stage 0 — capture baseline (no code; needed to prove wall_time win)

- [ ] **Step 1: Build dev binary and start server** (see "Repo conventions").

- [ ] **Step 2: Capture current RF behavior + timing for the benchmark join.**

Run (adjust port to `$NOVA_ENV_MYSQL_PORT` or `9030`); the q22 schema/data come from the `join` suite fixture — load it first if needed via the runner, then:
```bash
PORT=${NOVA_ENV_MYSQL_PORT:-9030}
echo "USE opt_probe; EXPLAIN ANALYZE WITH w1 AS (SELECT * FROM opt_probe.t1 WHERE k1<100) \
SELECT count(1), count(t1.k1), count(t1.c_tinyint_null) FROM opt_probe.t1 t1 \
LEFT SEMI JOIN w1 t2 ON t1.c_tinyint_null = t2.c_tinyint_null;" | mysql -h 127.0.0.1 -P $PORT -uroot
```
Record: total wall time, and the per-scan row counts (does the probe scan read fewer rows today? — tells us whether v1 RF is already firing).

- [ ] **Step 3: Capture join suite baseline wall_time.**
```bash
cargo run --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests -- \
  --suite join -j 1 --mode verify 2>&1 | tail -20
```
Record total wall_time.

- [ ] **Step 4: Write both numbers into the spec progress note + roadmap.**

Append to spec a "## Stage 0 baseline (2026-06-01)" section with the q22 timing/rows and join-suite wall_time. Commit:
```bash
git add docs/design/specs/2026-06-01-oq-5-runtime-filter-wiring-design.md
git commit -m "docs(oq-5): stage 0 RF baseline (q22 rows/time, join suite wall_time)"
```

---

## Task 1: Add RF annotation IR + fields to `PhysicalPlanNode`

**Files:**
- Create: `src/sql/optimizer/runtime_filter_pass.rs` (IR structs only in this task)
- Modify: `src/sql/optimizer/physical_plan.rs`
- Modify: `src/sql/optimizer/mod.rs` (declare module)

- [ ] **Step 1: Write the failing test** (in `physical_plan.rs`, append a `#[cfg(test)]` block).

```rust
#[cfg(test)]
mod rf_field_tests {
    use super::*;
    use crate::sql::optimizer::runtime_filter_pass::{RuntimeFilterDesc, RuntimeFilterProbe};

    #[test]
    fn physical_node_carries_rf_annotations() {
        // Build a trivial node and confirm the new vectors default-empty and accept entries.
        let mut node = PhysicalPlanNode {
            op: Operator::PhysicalValues(Default::default()),
            children: vec![],
            stats: Statistics { output_row_count: 1.0, column_statistics: Default::default() },
            output_columns: vec![],
            build_runtime_filters: vec![],
            probe_runtime_filters: vec![],
        };
        assert!(node.build_runtime_filters.is_empty());
        node.build_runtime_filters.push(RuntimeFilterDesc::placeholder(0));
        node.probe_runtime_filters.push(RuntimeFilterProbe::placeholder(0));
        assert_eq!(node.build_runtime_filters.len(), 1);
        assert_eq!(node.probe_runtime_filters.len(), 1);
    }
}
```
> If `Operator::PhysicalValues(Default::default())` does not compile, use the simplest constructible variant — confirm with `grep -n "PhysicalValues\|PhysicalScan" src/sql/optimizer/operator.rs` and pick one whose payload is `Default` or cheap to build. The test only needs *a* node.

- [ ] **Step 2: Run test to verify it fails.**

Run: `cargo test physical_node_carries_rf_annotations 2>&1 | tail -20`
Expected: FAIL — `RuntimeFilterDesc`/`RuntimeFilterProbe` unresolved and fields missing.

- [ ] **Step 3: Create the IR in `runtime_filter_pass.rs`.**

```rust
//! OQ-5 Stage 1: physical-tree runtime-filter planning pass.
//!
//! Annotates eligible hash-join `PhysicalPlanNode`s with build-side filter
//! descriptors and pushes a matching probe descriptor down to the deepest
//! descendant that can bind the probe column. EXPLAIN renders the annotations;
//! codegen lowers them to thrift `TRuntimeFilterDescription`.

use crate::sql::analysis::TypedExpr;
use crate::sql::optimizer::operator::JoinDistribution;

/// The optimizer-layer name used by `SET disable_optimizer_rules`.
pub(crate) const RUNTIME_FILTER_RULE: &str = "RuntimeFilterPushDown";

/// Build-side runtime filter produced by a hash join (one per equi-conjunct
/// that survives gating + push-down).
#[derive(Clone, Debug)]
pub(crate) struct RuntimeFilterDesc {
    pub filter_id: i32,
    /// Build-side key expression (eq.right, in build-child column space).
    pub build_expr: TypedExpr,
    /// Probe-side key expression (eq.left), in the *target node's* column space.
    pub probe_expr: TypedExpr,
    /// Index into the join's `eq_conditions`.
    pub expr_order: usize,
    /// Join distribution, drives thrift build_join_mode + layout.
    pub distribution: JoinDistribution,
    /// Estimated build-side row count (for thrift build_cardinality / debugging).
    pub build_cardinality: f64,
}

impl RuntimeFilterDesc {
    #[cfg(test)]
    pub(crate) fn placeholder(filter_id: i32) -> Self {
        use crate::sql::analysis::{ExprKind, TypedExpr};
        let e = TypedExpr {
            kind: ExprKind::Literal(crate::sql::analysis::LiteralValue::Null),
            data_type: arrow::datatypes::DataType::Null,
            nullable: true,
        };
        Self {
            filter_id,
            build_expr: e.clone(),
            probe_expr: e,
            expr_order: 0,
            distribution: JoinDistribution::Broadcast,
            build_cardinality: 0.0,
        }
    }
}

/// Probe-side runtime filter consumed by a node (scan or intermediate).
#[derive(Clone, Debug)]
pub(crate) struct RuntimeFilterProbe {
    pub filter_id: i32,
    /// Probe key expression in this node's column space.
    pub probe_expr: TypedExpr,
}

impl RuntimeFilterProbe {
    #[cfg(test)]
    pub(crate) fn placeholder(filter_id: i32) -> Self {
        use crate::sql::analysis::{ExprKind, TypedExpr};
        Self {
            filter_id,
            probe_expr: TypedExpr {
                kind: ExprKind::Literal(crate::sql::analysis::LiteralValue::Null),
                data_type: arrow::datatypes::DataType::Null,
                nullable: true,
            },
        }
    }
}
```
> Confirm `LiteralValue::Null` exists: `grep -n "enum LiteralValue" -A12 src/sql/analysis/mod.rs`. If the null variant has another name, use any constructible literal — the placeholder only needs to compile under `#[cfg(test)]`.

- [ ] **Step 4: Add the two fields to `PhysicalPlanNode`** (`physical_plan.rs:9`).

```rust
#[derive(Clone, Debug)]
pub(crate) struct PhysicalPlanNode {
    pub op: Operator,
    pub children: Vec<PhysicalPlanNode>,
    pub stats: Statistics,
    pub output_columns: Vec<OutputColumn>,
    /// OQ-5: build-side runtime filters produced here (hash joins only).
    pub build_runtime_filters: Vec<crate::sql::optimizer::runtime_filter_pass::RuntimeFilterDesc>,
    /// OQ-5: probe-side runtime filters consumed here.
    pub probe_runtime_filters: Vec<crate::sql::optimizer::runtime_filter_pass::RuntimeFilterProbe>,
}
```

- [ ] **Step 5: Declare the module** in `src/sql/optimizer/mod.rs` (near the other `pub(crate) mod` lines, e.g. by `runtime_filter_planner` at `:17`).

```rust
pub(crate) mod runtime_filter_pass;
```

- [ ] **Step 6: Fix all `PhysicalPlanNode { .. }` constructors.** The struct gains two required fields; every literal construction must add them. Find them:
```bash
grep -rn "PhysicalPlanNode {" src/ | grep -v "physical_plan.rs"
```
For each (notably `extract.rs` and any test fixtures in `fragment_builder.rs`), add:
```rust
            build_runtime_filters: Vec::new(),
            probe_runtime_filters: Vec::new(),
```
> Tip: build first (`cargo build 2>&1 | grep "missing field"`) to enumerate them mechanically.

- [ ] **Step 7: Run test to verify it passes.**

Run: `cargo test physical_node_carries_rf_annotations 2>&1 | tail -20`
Expected: PASS.

- [ ] **Step 8: Commit.**
```bash
git add src/sql/optimizer/physical_plan.rs src/sql/optimizer/runtime_filter_pass.rs src/sql/optimizer/mod.rs src/sql/optimizer/extract.rs
git commit -m "feat(oq-5): add runtime-filter annotation IR + PhysicalPlanNode fields"
```

---

## Task 2: Pass skeleton — eligibility + optimize() wiring + disable rule

Generate one build RF per eligible equi-conjunct, attach to the join node only (push-down + gating come in Tasks 3–4). Probe lands directly on the build/probe children for now (refined in Task 3).

**Files:**
- Modify: `src/sql/optimizer/runtime_filter_pass.rs`
- Modify: `src/sql/optimizer/mod.rs` (call pass after `extract_best`; add rule name to `is_known_rule_name`)

- [ ] **Step 1: Write the failing test** (append to `runtime_filter_pass.rs` tests).

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use crate::sql::optimizer::options::OptimizerOptions;

    // Build a minimal INNER hash join over two single-column scans and assert
    // annotate() puts exactly one build RF on the join node.
    #[test]
    fn inner_join_gets_one_build_rf() {
        let mut join = super::test_support::inner_join_two_scans();
        let opts = OptimizerOptions::default_settings();
        annotate(&mut join, &opts);
        assert_eq!(join.build_runtime_filters.len(), 1);
        assert_eq!(join.build_runtime_filters[0].filter_id, 0);
    }

    #[test]
    fn disabled_rule_emits_nothing() {
        let mut join = super::test_support::inner_join_two_scans();
        let mut opts = OptimizerOptions::default_settings();
        opts.disable(RUNTIME_FILTER_RULE);
        annotate(&mut join, &opts);
        assert!(join.build_runtime_filters.is_empty());
    }
}
```

- [ ] **Step 2: Add a `test_support` helper** (in `runtime_filter_pass.rs`, behind `#[cfg(test)]`) building a real two-scan INNER join `PhysicalPlanNode`.

```rust
#[cfg(test)]
pub(crate) mod test_support {
    use super::*;
    use crate::sql::analysis::{ColumnId, ExprKind, OutputColumn, TypedExpr};
    use crate::sql::optimizer::operator::{
        JoinDistribution, JoinKind, Operator, PhysicalHashJoinEqCondition, PhysicalHashJoinOp,
    };
    use crate::sql::optimizer::physical_plan::PhysicalPlanNode;
    use crate::sql::optimizer::statistics::Statistics;

    fn col(id: i32, name: &str) -> (OutputColumn, TypedExpr) {
        let cid = ColumnId::from(id); // confirm ColumnId constructor (see note)
        (
            OutputColumn {
                column_id: cid.clone(),
                name: name.to_string(),
                data_type: arrow::datatypes::DataType::Int32,
                nullable: true,
                is_internal: false,
            },
            TypedExpr {
                kind: ExprKind::ColumnRef { column_id: cid, qualifier: None, column: name.to_string() },
                data_type: arrow::datatypes::DataType::Int32,
                nullable: true,
            },
        )
    }

    fn scan(rows: f64, oc: OutputColumn) -> PhysicalPlanNode {
        PhysicalPlanNode {
            op: Operator::PhysicalScan(Default::default()), // confirm constructible scan op
            children: vec![],
            stats: Statistics { output_row_count: rows, column_statistics: Default::default() },
            output_columns: vec![oc],
            build_runtime_filters: vec![],
            probe_runtime_filters: vec![],
        }
    }

    pub(crate) fn inner_join_two_scans() -> PhysicalPlanNode {
        let (loc, lexpr) = col(1, "lc");
        let (roc, rexpr) = col(2, "rc");
        let left = scan(1_000_000.0, loc.clone());
        let right = scan(10.0, roc.clone());
        PhysicalPlanNode {
            op: Operator::PhysicalHashJoin(PhysicalHashJoinOp {
                join_type: JoinKind::Inner,
                eq_conditions: vec![PhysicalHashJoinEqCondition {
                    left: lexpr, right: rexpr, null_safe: false,
                }],
                other_condition: None,
                distribution: JoinDistribution::Broadcast,
            }),
            children: vec![left, right],
            stats: Statistics { output_row_count: 10.0, column_statistics: Default::default() },
            output_columns: vec![loc, roc],
            build_runtime_filters: vec![],
            probe_runtime_filters: vec![],
        }
    }
}
```
> Confirm three constructors before running: `ColumnId` (try `grep -n "struct ColumnId\|enum ColumnId\|type ColumnId" src/sql/analysis/mod.rs` — use its real constructor, often `ColumnId(id)` or `ColumnId::new(id)`); that `Operator::PhysicalScan` has a `Default` or build it explicitly from `grep -n "struct PhysicalScanOp" -A12 src/sql/optimizer/operator.rs`. Adjust the two `Default::default()` calls to real constructions. These are test-only.

- [ ] **Step 3: Run test to verify it fails.**

Run: `cargo test -p $(sed -n 's/^name = "\(.*\)"/\1/p' Cargo.toml | head -1) runtime_filter_pass 2>&1 | tail -25`
(Or simply `cargo test runtime_filter_pass 2>&1 | tail -25`.)
Expected: FAIL — `annotate` not defined.

- [ ] **Step 4: Implement `annotate` + eligibility** (in `runtime_filter_pass.rs`).

```rust
use crate::sql::optimizer::operator::{JoinKind, Operator};
use crate::sql::optimizer::options::OptimizerOptions;
use crate::sql::optimizer::physical_plan::PhysicalPlanNode;

/// Entry point: annotate the physical tree in place with runtime filters.
pub(crate) fn annotate(root: &mut PhysicalPlanNode, options: &OptimizerOptions) {
    if !options.is_enabled(RUNTIME_FILTER_RULE) {
        return;
    }
    let mut next_filter_id: i32 = 0;
    annotate_node(root, &mut next_filter_id);
}

/// True for join types that may build a runtime filter (StarRocks JoinNode.java).
fn join_builds_rf(kind: JoinKind) -> bool {
    matches!(
        kind,
        JoinKind::Inner
            | JoinKind::LeftSemi
            | JoinKind::RightOuter
            | JoinKind::RightSemi
            | JoinKind::RightAnti
            | JoinKind::Cross
    )
}

fn annotate_node(node: &mut PhysicalPlanNode, next_filter_id: &mut i32) {
    // Post-order so child fragments/joins are annotated first.
    for child in &mut node.children {
        annotate_node(child, next_filter_id);
    }
    let Operator::PhysicalHashJoin(join) = &node.op else { return };
    if !join_builds_rf(join.join_type) {
        return;
    }
    // Snapshot the data we need before borrowing children mutably.
    let eq_conditions = join.eq_conditions.clone();
    let distribution = join.distribution.clone();
    let build_card = node.children[1].stats.output_row_count; // right child = build (Task A)

    let mut descs: Vec<RuntimeFilterDesc> = Vec::new();
    for (expr_order, eq) in eq_conditions.iter().enumerate() {
        if eq.null_safe {
            continue; // RF does not handle null-safe keys
        }
        let filter_id = *next_filter_id;
        *next_filter_id += 1;
        descs.push(RuntimeFilterDesc {
            filter_id,
            build_expr: eq.right.clone(),
            probe_expr: eq.left.clone(),
            expr_order,
            distribution: distribution.clone(),
            build_cardinality: build_card,
        });
    }
    // Stage 1 placeholder targeting: attach probe to the immediate probe child
    // (children[0]). Task 3 replaces this with real push-down.
    for d in &descs {
        node.children[0].probe_runtime_filters.push(RuntimeFilterProbe {
            filter_id: d.filter_id,
            probe_expr: d.probe_expr.clone(),
        });
    }
    node.build_runtime_filters = descs;
}
```

- [ ] **Step 5: Wire into `optimize()`** (`mod.rs:145`). Replace the tail expression:

```rust
    // 11. Extract best plan, then annotate runtime filters (OQ-5).
    let mut physical = extract::extract_best(&memo, root_group, &root_required, &ctx.winners)?;
    runtime_filter_pass::annotate(&mut physical, &options);
    Ok(physical)
```
> `options` is already in scope (built at `mod.rs:78`). `extract_best` returns `Result<PhysicalPlanNode, String>` (matches the fn return at `:65`).

- [ ] **Step 6: Register the rule name** in `is_known_rule_name` (`mod.rs:169`). Add `RuntimeFilterPushDown` to whatever collection it checks (read the fn body first). If it matches against a literal list, add:
```rust
        || name == crate::sql::optimizer::runtime_filter_pass::RUNTIME_FILTER_RULE
```

- [ ] **Step 7: Run tests to verify they pass.**

Run: `cargo test runtime_filter_pass 2>&1 | tail -25`
Expected: PASS (`inner_join_gets_one_build_rf`, `disabled_rule_emits_nothing`).

- [ ] **Step 8: Add an is_known_rule_name assertion** (in `mod.rs` `is_known_rule_name_tests`):
```rust
    #[test]
    fn is_known_rule_name_recognizes_runtime_filter() {
        assert!(is_known_rule_name("RuntimeFilterPushDown"));
    }
```
Run: `cargo test is_known_rule_name_recognizes_runtime_filter 2>&1 | tail`
Expected: PASS.

- [ ] **Step 9: Commit.**
```bash
git add src/sql/optimizer/runtime_filter_pass.rs src/sql/optimizer/mod.rs
git commit -m "feat(oq-5): runtime-filter pass skeleton + optimize() wiring + disable rule"
```

---

## Task 3: Within-fragment probe push-down (through project/filter/agg)

Replace the placeholder targeting with a real descent that lands each probe on the **deepest** descendant that can bind the probe column. Stop at Exchange nodes (cross-fragment push-down is Stage 3).

**Files:** Modify `src/sql/optimizer/runtime_filter_pass.rs`.

- [ ] **Step 1: Write the failing test.** Build `scan(probe) <- project <- INNER join` and assert the probe RF lands on the **scan**, not the project.

```rust
    #[test]
    fn probe_pushes_through_project_to_scan() {
        let mut join = super::test_support::join_with_project_over_probe_scan();
        annotate(&mut join, &OptimizerOptions::default_settings());
        // No probe RF on the join's immediate child (the project)...
        assert!(join.children[0].probe_runtime_filters.is_empty());
        // ...it reached the scan beneath the project.
        let scan = &join.children[0].children[0];
        assert_eq!(scan.probe_runtime_filters.len(), 1);
    }
```
Add `join_with_project_over_probe_scan()` to `test_support` mirroring `inner_join_two_scans` but wrapping the left scan in a `PhysicalProject` node that passes the probe column through unchanged (same `column_id` in its `output_columns`).

- [ ] **Step 2: Run test to verify it fails.**

Run: `cargo test probe_pushes_through_project_to_scan 2>&1 | tail -20`
Expected: FAIL — probe currently lands on `children[0]` (the project).

- [ ] **Step 3: Implement column-id extraction + push-down.**

```rust
use crate::sql::analysis::{ColumnId, ExprKind, TypedExpr};
use std::collections::HashSet;

/// Collect all column ids referenced by an expression.
fn column_ids(expr: &TypedExpr, out: &mut HashSet<ColumnId>) {
    match &expr.kind {
        ExprKind::ColumnRef { column_id, .. } => { out.insert(column_id.clone()); }
        ExprKind::BinaryOp { left, right, .. } => { column_ids(left, out); column_ids(right, out); }
        ExprKind::UnaryOp { expr, .. } | ExprKind::Cast { expr, .. }
        | ExprKind::IsNull { expr, .. } | ExprKind::Nested(expr)
        | ExprKind::IsTruthValue { expr, .. } => column_ids(expr, out),
        ExprKind::FunctionCall { args, .. } | ExprKind::AggregateCall { args, .. } => {
            for a in args { column_ids(a, out); }
        }
        ExprKind::InList { expr, list, .. } => {
            column_ids(expr, out); for e in list { column_ids(e, out); }
        }
        ExprKind::Between { expr, low, high, .. } => {
            column_ids(expr, out); column_ids(low, out); column_ids(high, out);
        }
        ExprKind::Like { expr, pattern, .. } => { column_ids(expr, out); column_ids(pattern, out); }
        ExprKind::Case { operand, when_then, else_expr } => {
            if let Some(o) = operand { column_ids(o, out); }
            for (w, t) in when_then { column_ids(w, out); column_ids(t, out); }
            if let Some(e) = else_expr { column_ids(e, out); }
        }
        // Literals / lambda params / window calls etc.: no plain column ids to bind.
        _ => {}
    }
}

/// True if `node` outputs every column the probe expr references (StarRocks couldBound).
fn could_bound(node: &PhysicalPlanNode, probe_expr: &TypedExpr) -> bool {
    let mut needed = HashSet::new();
    column_ids(probe_expr, &mut needed);
    if needed.is_empty() {
        return false; // probe must reference at least one column
    }
    let have: HashSet<ColumnId> =
        node.output_columns.iter().map(|c| c.column_id.clone()).collect();
    needed.iter().all(|id| have.contains(id))
}

/// Is this node a fragment boundary the probe cannot cross in Stage 1?
fn is_exchange(node: &PhysicalPlanNode) -> bool {
    // Stage 1: do not push across distribution/exchange enforcers.
    matches!(node.op, Operator::PhysicalDistribution(_)) // confirm enforcer variant name
}

/// Descend from `node` and attach the probe at the deepest binding node.
/// Returns true if placed somewhere in this subtree.
fn push_probe_down(node: &mut PhysicalPlanNode, probe: &RuntimeFilterProbe) -> bool {
    if is_exchange(node) {
        return false; // Stage 1 stops at fragment boundary
    }
    if !could_bound(node, &probe.probe_expr) {
        return false;
    }
    // Try to go deeper first.
    for child in &mut node.children {
        if push_probe_down(child, probe) {
            return true;
        }
    }
    // Deepest binding node in this subtree: attach here.
    node.probe_runtime_filters.push(probe.clone());
    true
}
```
> Confirm the enforcer/exchange operator variant: `grep -n "Physical.*Distribution\|PhysicalExchange\|Enforcer\|PhysicalShuffle" src/sql/optimizer/operator.rs`. Use the real variant in `is_exchange`. (`operator.rs:363` mentions a "Distribution enforcer node".)

- [ ] **Step 4: Replace the placeholder targeting block in `annotate_node`** (from Task 2 Step 4) with push-down into the probe child:

```rust
    for d in &descs {
        let probe = RuntimeFilterProbe { filter_id: d.filter_id, probe_expr: d.probe_expr.clone() };
        // children[0] = probe side; descend to deepest binding node.
        let _placed = push_probe_down(&mut node.children[0], &probe);
        // If not placed (no binding node found), the RF is build-only; that is
        // fine — execution simply has no probe target. Keep the build desc;
        // codegen will skip emitting a probe spec.
    }
```

- [ ] **Step 5: Run tests.**

Run: `cargo test runtime_filter_pass 2>&1 | tail -25`
Expected: PASS (including `probe_pushes_through_project_to_scan`).

- [ ] **Step 6: Commit.**
```bash
git add src/sql/optimizer/runtime_filter_pass.rs
git commit -m "feat(oq-5): within-fragment probe push-down to deepest binding node"
```

---

## Task 4: Cardinality gating (constant thresholds)

Apply StarRocks' build-side and probe-side gates using `Statistics`. Thresholds are constants in Stage 1; Stage 2 turns them into session variables.

**Files:** Modify `src/sql/optimizer/runtime_filter_pass.rs`.

- [ ] **Step 1: Write failing tests.**

```rust
    #[test]
    fn skips_rf_when_build_side_too_large_for_shuffle() {
        // Shuffle join, build child huge -> build gate rejects.
        let mut join = super::test_support::shuffle_join(/*build_rows=*/ 50_000_000.0,
                                                         /*probe_rows=*/ 50_000_000.0);
        annotate(&mut join, &OptimizerOptions::default_settings());
        assert!(join.build_runtime_filters.is_empty());
    }

    #[test]
    fn keeps_rf_when_build_small_relative_to_probe() {
        // Broadcast join, tiny build, huge probe -> kept.
        let mut join = super::test_support::inner_join_two_scans(); // build=10, probe=1e6
        annotate(&mut join, &OptimizerOptions::default_settings());
        assert_eq!(join.build_runtime_filters.len(), 1);
    }

    #[test]
    fn skips_rf_low_selectivity_across_exchange() {
        // Shuffle join, build ~ probe (ratio ~1.0) -> probe gate rejects.
        let mut join = super::test_support::shuffle_join(900_000.0, 1_000_000.0);
        annotate(&mut join, &OptimizerOptions::default_settings());
        assert!(join.build_runtime_filters.is_empty());
    }
```
Add `shuffle_join(build_rows, probe_rows)` to `test_support` (like `inner_join_two_scans` but `distribution: JoinDistribution::Shuffle` and parameterized child `output_row_count`s; give columns enough `column_statistics` so `compute_size()` is non-trivial, or rely on the row-count path — see thresholds below).

- [ ] **Step 2: Run tests to verify they fail.**

Run: `cargo test runtime_filter_pass 2>&1 | tail -25`
Expected: the three new tests FAIL (no gating yet).

- [ ] **Step 3: Implement gating constants + predicates.**

```rust
// StarRocks defaults (SessionVariable.java). Bytes; size = rows * avg_row_size.
const BUILD_MAX_SIZE: f64 = 64.0 * 1024.0 * 1024.0;
const BUILD_MIN_SIZE: f64 = 128.0 * 1024.0;
const PROBE_MIN_SIZE: f64 = 100.0 * 1024.0;
const PROBE_MIN_SELECTIVITY: f64 = 0.5;

/// StarRocks JoinNode.java:173 — only PARTITIONED/SHUFFLE gate on build size.
fn build_gate_passes(distribution: &JoinDistribution, build_size: f64) -> bool {
    match distribution {
        JoinDistribution::Shuffle => {
            // build_max_size > 0 && (build_size <= 0 || build_size > max) -> skip
            !(build_size <= 0.0 || build_size > BUILD_MAX_SIZE)
        }
        // Broadcast / Colocate: no build-size gate.
        _ => true,
    }
}

/// StarRocks RuntimeFilterDescription.canProbeUse — selectivity gate for the
/// cross-exchange case. Stage 1 is within-fragment (local) so this only matters
/// for shuffle joins. `local` callers accept unconditionally.
fn probe_gate_passes(local: bool, build_size: f64, probe_size: f64) -> bool {
    if local {
        return true;
    }
    if build_size <= BUILD_MIN_SIZE {
        return true;
    }
    if probe_size < PROBE_MIN_SIZE {
        return false;
    }
    let ratio = build_size / probe_size.max(1.0);
    ratio <= 1.0 - PROBE_MIN_SELECTIVITY
}
```

- [ ] **Step 4: Apply gates in `annotate_node`.** After computing `build_card` and before the per-conjunct loop, compute sizes and the build gate; inside the loop, apply the probe gate (treat Shuffle as non-local for the gate, Broadcast/Colocate as local in Stage 1):

```rust
    let build_size = node.children[1].stats.compute_size();
    let probe_size = node.children[0].stats.compute_size();
    if !build_gate_passes(&distribution, build_size) {
        return;
    }
    let local = !matches!(distribution, JoinDistribution::Shuffle);
    // ... inside the eq loop, after the null_safe check:
    if !probe_gate_passes(local, build_size, probe_size) {
        continue;
    }
```
> `Statistics::compute_size()` (`statistics.rs:48`) = `output_row_count * avg_row_size()`; `avg_row_size()` falls back to `8.0` when `column_statistics` is empty, so the row-count-driven tests work without populating column stats.

- [ ] **Step 5: Run tests.**

Run: `cargo test runtime_filter_pass 2>&1 | tail -25`
Expected: PASS (all gating tests + earlier tests).

- [ ] **Step 6: Commit.**
```bash
git add src/sql/optimizer/runtime_filter_pass.rs
git commit -m "feat(oq-5): StarRocks-faithful RF gating (build-max / probe-min / selectivity)"
```

---

## Task 5: EXPLAIN rendering of build/probe runtime filters

**Files:** Modify `src/sql/explain.rs` (`format_physical_node`).

- [ ] **Step 1: Write the failing test** (append to `explain.rs` tests, or add one). Construct an annotated join (reuse `runtime_filter_pass::test_support`) and assert the rendered lines contain the RF text.

```rust
#[cfg(test)]
mod rf_explain_tests {
    use super::*;
    use crate::sql::optimizer::runtime_filter_pass::{self, test_support};
    use crate::sql::optimizer::options::OptimizerOptions;

    #[test]
    fn explain_shows_build_and_probe_rf() {
        let mut join = test_support::inner_join_two_scans();
        runtime_filter_pass::annotate(&mut join, &OptimizerOptions::default_settings());
        let lines = explain_physical_plan(&join, ExplainLevel::Verbose).join("\n");
        assert!(lines.contains("build runtime filters:"), "got:\n{lines}");
        assert!(lines.contains("filter_id = 0"), "got:\n{lines}");
        assert!(lines.contains("probe runtime filters:"), "got:\n{lines}");
    }
}
```
> If `test_support` is not visible from `explain.rs` (different module), make `test_support` `pub(crate)` (it already is in Task 2) — confirm it compiles across modules.

- [ ] **Step 2: Run test to verify it fails.**

Run: `cargo test explain_shows_build_and_probe_rf 2>&1 | tail -20`
Expected: FAIL — no RF lines emitted.

- [ ] **Step 3: Render build RF on the hash-join branch.** In `format_physical_node`, in the `Operator::PhysicalHashJoin` arm, **after** the `HASH JOIN (...)` push (`explain.rs:466`) and the optional `other:` line, and **before** the children loop (`:470`), insert:

```rust
            if matches!(
                level,
                ExplainLevel::Verbose | ExplainLevel::Costs | ExplainLevel::Analyze
            ) && !node.build_runtime_filters.is_empty()
            {
                out.push(format!("{pad}  build runtime filters:"));
                for rf in &node.build_runtime_filters {
                    out.push(format!(
                        "{pad}  - filter_id = {}, build_expr = ({})",
                        rf.filter_id,
                        format_expr(&rf.build_expr),
                    ));
                }
            }
```

- [ ] **Step 4: Render probe RF on every node.** Add a small shared helper and call it in the SCAN, FILTER, PROJECT, and (hash) AGGREGATE arms — the nodes a Stage-1 probe can land on. Add near the top of `format_physical_node`, right after `stats_suffix` is computed, a closure-free helper call is awkward; instead define a module-level fn:

```rust
fn push_probe_rf_lines(node: &PhysicalPlanNode, level: ExplainLevel, pad: &str, out: &mut Vec<String>) {
    if !matches!(
        level,
        ExplainLevel::Verbose | ExplainLevel::Costs | ExplainLevel::Analyze
    ) || node.probe_runtime_filters.is_empty()
    {
        return;
    }
    out.push(format!("{pad}     probe runtime filters:"));
    for rf in &node.probe_runtime_filters {
        out.push(format!(
            "{pad}     - filter_id = {}, probe_expr = ({})",
            rf.filter_id,
            format_expr(&rf.probe_expr),
        ));
    }
}
```
Then, in the `PhysicalScan` arm (after the predicates block, before the arm ends ~`:403`), call:
```rust
            push_probe_rf_lines(node, level, &pad, out);
```
Do the same at the end of the `PhysicalFilter`, `PhysicalProject`, and `PhysicalHashAggregate` arms (before their children loops). Use the matching indent (`{pad}  ` for filter/project/agg which indent with two spaces — mirror their existing sub-line indent).
> The exact indent prefix differs per arm (scan uses `{pad}     `, filter uses `{pad}  `). Match each arm's existing sub-line style so the golden output is clean.

- [ ] **Step 5: Run test.**

Run: `cargo test explain_shows_build_and_probe_rf 2>&1 | tail -20`
Expected: PASS.

- [ ] **Step 6: Commit.**
```bash
git add src/sql/explain.rs
git commit -m "feat(oq-5): render build/probe runtime filters in EXPLAIN VERBOSE/COSTS"
```

---

## Task 6: codegen — consume annotations, emit thrift, build RuntimeFilterPlanResult

Make `fragment_builder` build thrift `TRuntimeFilterDescription` from `node.build_runtime_filters` and probe specs from `node.probe_runtime_filters`, accumulating a `RuntimeFilterPlanResult` for the coordinator. Reuse the descriptor construction currently in `runtime_filter_planner.rs:96-194`.

**Files:**
- Modify: `src/sql/codegen/nodes.rs` (`build_hash_join_node`)
- Modify: `src/sql/codegen/fragment_builder.rs` (`visit_hash_join`, scan/project/agg visitors, the assembly block at `:300-323`)

- [ ] **Step 1: Write the failing test.** Use the existing `build_fragments_for_query` test helper (`fragment_builder.rs:4295`) to build a join query and assert the join thrift node has `build_runtime_filters = Some(..)`.

```rust
    #[test]
    fn codegen_emits_build_runtime_filters_from_annotation() {
        let build = build_fragments_for_query(
            "SELECT count(*) FROM tbl a JOIN tbl b ON a.id = b.id",
        );
        let has_rf = build.fragment_results.iter().any(|fr| {
            fr.plan.nodes.iter().any(|n| {
                n.hash_join_node
                    .as_ref()
                    .and_then(|hj| hj.build_runtime_filters.as_ref())
                    .map(|v| !v.is_empty())
                    .unwrap_or(false)
            })
        });
        assert!(has_rf, "expected a join node with build_runtime_filters");
    }
```
> Confirm `tbl` exists in the test catalog used by `build_fragments_for_query` (`grep -n "fn build_fragments_for_query" -A40 src/sql/codegen/fragment_builder.rs` to see the registered tables). Use a table the helper registers; adjust the SQL.

- [ ] **Step 2: Run test to verify it fails.**

Run: `cargo test codegen_emits_build_runtime_filters_from_annotation 2>&1 | tail -25`
Expected: FAIL — old post-pass may already set it OR not; if it passes spuriously because the **old** post-pass is still active, that is expected until Task 11. To make this test meaningful now, temporarily assert the descriptor's `build_plan_node_id` equals the join node id AND that it came from the annotation path (add a unique marker in Step 3, e.g. ordering by annotation). Simpler: proceed — Task 11 removes the old pass; this test then proves the new path.

- [ ] **Step 3: Extend `build_hash_join_node`** (`nodes.rs:329`) to accept build RF descriptors.

```rust
pub(crate) fn build_hash_join_node(
    node_id: i32,
    left_tuple_ids: &[i32],
    right_tuple_ids: &[i32],
    join_op: plan_nodes::TJoinOp,
    eq_join_conjuncts: Vec<plan_nodes::TEqJoinCondition>,
    other_join_conjuncts: Vec<exprs::TExpr>,
    build_runtime_filters: Option<Vec<runtime_filter::TRuntimeFilterDescription>>, // NEW
) -> plan_nodes::TPlanNode {
    // ... existing body ...
    // at the THashJoinNode construction, set:
    //     build_runtime_filters,                         // was: None
    //     build_runtime_filters_from_planner: None,
}
```
Update the existing `build_runtime_filters: None` line (`nodes.rs:385`) to use the new parameter. Update **all** callers of `build_hash_join_node` (`grep -rn "build_hash_join_node(" src/`) to pass the new arg (`None` for any non-join-visitor caller).

- [ ] **Step 4: Build the thrift descriptors in `visit_hash_join`.** After `join_node_id` is allocated and before/at the `build_hash_join_node` call (`fragment_builder.rs:1333`), translate `node.build_runtime_filters` into thrift. Factor the construction into a helper in `runtime_filter_pass.rs` or a new `codegen/runtime_filter_lower.rs` reusing the field layout from `runtime_filter_planner.rs:96-164`:

```rust
// New: src/sql/codegen/runtime_filter_lower.rs
use crate::runtime_filter;
use crate::sql::optimizer::operator::JoinDistribution;
use crate::sql::optimizer::runtime_filter_pass::RuntimeFilterDesc;

/// Build a thrift TRuntimeFilterDescription for a single annotated build RF.
/// `compiled_build_expr` is the build key already compiled to a thrift TExpr
/// (reuse the join's eq_join_conjuncts[expr_order].right).
pub(crate) fn lower_build_rf(
    rf: &RuntimeFilterDesc,
    compiled_build_expr: crate::exprs::TExpr,
    probe_target_node_id: i32,
    compiled_probe_expr: crate::exprs::TExpr,
    join_node_id: i32,
    has_remote_targets: bool,
    pipeline_dop: i32,
) -> runtime_filter::TRuntimeFilterDescription {
    // ... identical field layout to runtime_filter_planner.rs:96-164,
    //     deriving build_join_mode + layout from rf.distribution ...
}
```
In `visit_hash_join`, for each `desc` in `node.build_runtime_filters`, the build key is the already-compiled `eq_join_conjuncts[desc.expr_order].right` and the probe key must be compiled against the probe target node's scope. **Stage 1 simplification:** since push-down stays within the fragment and project/filter pass the column through unchanged, compile the probe expr against the probe-side child scope (`left.scope`) — the same scope used for `eq.left`. Collect `(join_node_id, Vec<filter_id>)` into `self`-held accumulators mirroring the old `RuntimeFilterPlanResult`.
> Reuse the exact `TRuntimeFilterDescription::new(...)` argument list from `runtime_filter_planner.rs:140-163` — copy it verbatim, swapping inputs.

- [ ] **Step 5: Emit probe specs from scan/project/agg visitors.** Where each scan/project/agg node is lowered, read the corresponding `PhysicalPlanNode.probe_runtime_filters` and record `(fragment_id, [(filter_id, node_id)])` into the probe accumulator. (The existing thrift `plan_node_id_to_target_expr` map on the build descriptor already carries the probe target; the coordinator side reads `probe_side_filters`.) Confirm how the old code mapped probe targets (`runtime_filter_planner.rs:137-174`) and reproduce the accumulation.

- [ ] **Step 6: Build `RuntimeFilterPlanResult` in the assembly block** (`fragment_builder.rs:300-316`). Replace the `plan_runtime_filters(...)` call with construction from the accumulators populated during visiting:

```rust
        // Runtime filter planning result now comes from physical-tree
        // annotations consumed during visiting (OQ-5 Stage 1).
        let rf_plan = self.take_runtime_filter_plan(); // Option<RuntimeFilterPlanResult>
```
Keep `RuntimeFilterPlanResult` defined where the coordinator expects it. Move its struct definition out of `runtime_filter_planner.rs` (which Task 11 deletes) into `src/sql/codegen/mod.rs` or a small `codegen/runtime_filter_lower.rs`, and update the `use` in `coordinator.rs:29`.

- [ ] **Step 7: Run codegen test + coordinator compile.**

Run: `cargo build 2>&1 | tail -25 && cargo test codegen_emits_build_runtime_filters_from_annotation 2>&1 | tail -20`
Expected: build OK; test PASS.

- [ ] **Step 8: Commit.**
```bash
git add src/sql/codegen/nodes.rs src/sql/codegen/fragment_builder.rs src/sql/codegen/runtime_filter_lower.rs src/sql/codegen/mod.rs src/runtime/coordinator.rs
git commit -m "feat(oq-5): codegen lowers RF annotations to thrift; build RuntimeFilterPlanResult"
```

---

## Task 7: End-to-end smoke — RF still executes via coordinator

Confirm the new path produces a working query (build sink publishes, scan probes) just like the old path.

**Files:** none (verification) — or a small integration test if one exists for RF execution.

- [ ] **Step 1: Start server (dev build) and run the benchmark join from Task B.**

Run the same q22 `EXPLAIN ANALYZE` from Task B Step 2. Expected: query succeeds; row counts at the probe scan are ≤ the Task B baseline (RF still firing). Capture the numbers.

- [ ] **Step 2: Confirm EXPLAIN VERBOSE now shows RF.**
```bash
PORT=${NOVA_ENV_MYSQL_PORT:-9030}
echo "USE opt_probe; EXPLAIN VERBOSE WITH w1 AS (SELECT * FROM opt_probe.t1 WHERE k1<100) \
SELECT count(1) FROM opt_probe.t1 t1 LEFT SEMI JOIN w1 t2 \
ON t1.c_tinyint_null = t2.c_tinyint_null;" | mysql -h 127.0.0.1 -P $PORT -uroot
```
Expected: output contains `build runtime filters:` and `probe runtime filters:`.

- [ ] **Step 3: No commit** (verification only). If row counts regressed vs baseline, STOP and debug before continuing (likely orientation or probe-target compilation).

---

## Task 8: Golden plan tests (`sql-tests/optimizer`)

**Files:** Create `sql-tests/optimizer/sql/runtime_filter_inner_join.sql`, `runtime_filter_disabled.sql`.

- [ ] **Step 1: Write `runtime_filter_inner_join.sql`.**

```sql
-- OQ-5: hash join emits a build runtime filter, pushed to the probe scan.
CREATE TABLE ${case_db}.rf_build (k INT, v INT);
CREATE TABLE ${case_db}.rf_probe (k INT, v INT);
INSERT INTO ${case_db}.rf_build VALUES (1,1),(2,2),(3,3);
INSERT INTO ${case_db}.rf_probe SELECT generate_series, generate_series FROM TABLE(generate_series(1, 100000));
ANALYZE TABLE ${case_db}.rf_build;
ANALYZE TABLE ${case_db}.rf_probe;

-- @explain_contains=build runtime filters:
-- @explain_contains=filter_id = 0
-- @explain_contains=probe runtime filters:
EXPLAIN VERBOSE
SELECT count(*) FROM ${case_db}.rf_probe p JOIN ${case_db}.rf_build b ON p.k = b.k;
```

- [ ] **Step 2: Write `runtime_filter_disabled.sql`** (master switch off → no RF lines).

```sql
-- OQ-5: disable_optimizer_rules='RuntimeFilterPushDown' suppresses RF.
CREATE TABLE ${case_db}.rf_b (k INT);
CREATE TABLE ${case_db}.rf_p (k INT);
INSERT INTO ${case_db}.rf_b VALUES (1),(2),(3);
INSERT INTO ${case_db}.rf_p SELECT generate_series FROM TABLE(generate_series(1, 100000));
ANALYZE TABLE ${case_db}.rf_b;
ANALYZE TABLE ${case_db}.rf_p;

SET disable_optimizer_rules = 'RuntimeFilterPushDown';
-- @explain_contains_not=build runtime filters:
EXPLAIN VERBOSE
SELECT count(*) FROM ${case_db}.rf_p p JOIN ${case_db}.rf_b b ON p.k = b.k;
```
> Confirm `-- @explain_contains_not=` is a supported directive: `grep -rn "explain_contains_not\|explain_contains" tests/sql-test-runner/src | head`. If only `@explain_contains` exists, assert a different stable substring instead and note it.

- [ ] **Step 3: Record golden output** (server must be running).
```bash
cargo run --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests -- \
  --suite optimizer --only runtime_filter_inner_join,runtime_filter_disabled --mode record
```

- [ ] **Step 4: Verify golden.**
```bash
cargo run --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests -- \
  --suite optimizer --only runtime_filter_inner_join,runtime_filter_disabled --mode verify
```
Expected: PASS.

- [ ] **Step 5: Commit.**
```bash
git add sql-tests/optimizer/sql/runtime_filter_inner_join.sql sql-tests/optimizer/sql/runtime_filter_disabled.sql sql-tests/optimizer/result/runtime_filter_inner_join.result sql-tests/optimizer/result/runtime_filter_disabled.result
git commit -m "test(oq-5): golden plans for RF visibility + disable switch"
```

---

## Task 9: RF on/off equivalence + suite regression

**Files:** Create `sql-tests/optimizer/sql/runtime_filter_equivalence.sql` (result-golden, not explain).

- [ ] **Step 1: Write an equivalence case** — same query result with RF on vs off must be identical. Run the join twice (default = on; then `SET disable_optimizer_rules='RuntimeFilterPushDown'`) and let the runner golden the rows; both SELECTs must return the same rows.

```sql
-- OQ-5: RF must not change results (only reduce work).
CREATE TABLE ${case_db}.eq_b (k INT, v INT);
CREATE TABLE ${case_db}.eq_p (k INT, v INT);
INSERT INTO ${case_db}.eq_b VALUES (1,10),(2,20),(5,50);
INSERT INTO ${case_db}.eq_p SELECT generate_series % 7, generate_series FROM TABLE(generate_series(1, 5000));

SELECT b.k, count(*) FROM ${case_db}.eq_p p JOIN ${case_db}.eq_b b ON p.k = b.k GROUP BY b.k ORDER BY b.k;
SET disable_optimizer_rules = 'RuntimeFilterPushDown';
SELECT b.k, count(*) FROM ${case_db}.eq_p p JOIN ${case_db}.eq_b b ON p.k = b.k GROUP BY b.k ORDER BY b.k;
```
Record + verify as in Task 8 (the two SELECT result blocks must match).

- [ ] **Step 2: Run the regression suites that must not break.**
```bash
for s in join cte filter; do
  cargo run --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests -- \
    --suite $s -j 1 --mode verify 2>&1 | tail -5
done
```
Expected: same pass counts as before OQ-5 (no regressions). Investigate any new failure before continuing.

- [ ] **Step 3: Commit.**
```bash
git add sql-tests/optimizer/sql/runtime_filter_equivalence.sql sql-tests/optimizer/result/runtime_filter_equivalence.result
git commit -m "test(oq-5): RF on/off result equivalence"
```

---

## Task 10: `cargo fmt` + `cargo clippy` pass

- [ ] **Step 1:** `cargo fmt`
- [ ] **Step 2:** `cargo clippy --all-targets 2>&1 | tail -30` — fix new warnings in touched files.
- [ ] **Step 3:** Commit.
```bash
git add -A && git commit -m "chore(oq-5): fmt + clippy for runtime filter pass"
```

---

## Task 11: Delete the old thrift post-pass

Only after Task 6 proved the new path produces the same `RuntimeFilterPlanResult`.

**Files:** Delete `src/sql/optimizer/runtime_filter_planner.rs`; clean references.

- [ ] **Step 1: Remove the call + module.**
```bash
grep -rn "runtime_filter_planner\|plan_runtime_filters" src/
```
Remove the `plan_runtime_filters(...)` call (already replaced in Task 6 Step 6), the `pub(crate) mod runtime_filter_planner;` line (`mod.rs:17`), and `git rm src/sql/optimizer/runtime_filter_planner.rs`. Ensure `RuntimeFilterPlanResult` now lives in its Task-6 home and `coordinator.rs:29` imports it from there.

- [ ] **Step 2: Remove now-dead builder tracking if unused.** If `scan_tuple_owners` / `join_fragment_map` / `join_distributions` are no longer read (the new pass uses physical-tree topology, not these maps), delete them from `PlanFragmentBuilder` and their `insert` sites in `visit_scan`/`visit_hash_join`. Confirm with `grep -rn "scan_tuple_owners\|join_fragment_map\|join_distributions" src/sql/codegen/`. **Keep** any that the coordinator still needs for `has_remote_targets`.
> Stage 3 (cross-exchange) may re-introduce fragment tracking; for Stage 1 within-fragment, remove what is unused to avoid dead code.

- [ ] **Step 3: Build + full touched-suite verify.**
```bash
cargo build 2>&1 | tail -20
cargo test runtime_filter_pass 2>&1 | tail -10
cargo run --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests -- \
  --suite optimizer --only runtime_filter_inner_join,runtime_filter_disabled,runtime_filter_equivalence --mode verify 2>&1 | tail -10
```
Expected: build OK; all PASS — proving the new path fully replaced the old.

- [ ] **Step 4: Commit.**
```bash
git add -A
git commit -m "refactor(oq-5): delete post-codegen RF thrift pass; physical-tree pass is source of truth"
```

---

## Task 12: Benchmark comparison + roadmap update (Stage 1 acceptance)

- [ ] **Step 1: Re-run the three benchmark EXPLAINs vs StarRocks.** For `join_one_key` q22, `join_linear_chained` q31, and a simple `INNER count(*)`, run `EXPLAIN VERBOSE` on NovaRocks (`$NOVA_ENV_MYSQL_PORT`) and StarRocks FE (9030, via `starrocks-fe-on-novarocks` skill). Confirm build/probe RF appear and broadly match StarRocks placement. Save the diffs for the PR description.

- [ ] **Step 2: Re-run join suite wall_time and compare to Task B baseline.**
```bash
cargo run --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests -- \
  --suite join -j 1 --mode verify 2>&1 | tail -5
```
Record the new wall_time and the delta vs Task B.

- [ ] **Step 3: Update the roadmap progress + spec.** In `~/Documents/Obsidian/NovaRocks TODO/NovaRocks Roadmap.md` OQ progress section, mark OQ-5 Stage 1 done with the wall_time delta and the EXPLAIN-now-visible note. (Roadmap lives outside the repo — edit in place, no commit.) Add a "Stage 1 done" note to the spec and commit the spec.
```bash
git add docs/design/specs/2026-06-01-oq-5-runtime-filter-wiring-design.md
git commit -m "docs(oq-5): stage 1 complete — RF visible, gated, pushed-down; wall_time delta recorded"
```

---

## Self-review notes (for the implementer)

- **Spec coverage:** Tasks cover spec §3 (pass placement, Task 2/5/6), §4 (IR, Task 1), §5 (eligibility/gating/push-down/orientation, Tasks 2–4 + A), §6 (constants now; session vars are **Stage 2**, out of this plan), §7 (EXPLAIN, Task 5), §8 (codegen, Task 6/11), §9 (filter-type is **Stage 4**, out of this plan), §11 (fail-fast: unplaced probe = build-only, Task 3 Step 4), §12 (tests, Tasks 8–9, 12). Cross-exchange push-down is **Stage 3** (out of this plan) — `is_exchange` deliberately stops descent.
- **Known confirm-before-coding points** (flagged inline, none are placeholders — they are real-code lookups): `ColumnId` constructor; the simplest constructible `Operator` variant for tests; the distribution/exchange enforcer variant name; `LiteralValue` null variant; `@explain_contains_not` support; the test catalog tables for `build_fragments_for_query`; the exact `TRuntimeFilterDescription::new` arg list (copy verbatim from `runtime_filter_planner.rs:140-163`).
- **Type consistency:** `RuntimeFilterDesc` / `RuntimeFilterProbe` are defined once (Task 1) and reused everywhere; `annotate(&mut PhysicalPlanNode, &OptimizerOptions)` signature is stable across Tasks 2–5; `RUNTIME_FILTER_RULE = "RuntimeFilterPushDown"` is the single source for the rule name (pass guard, is_known_rule_name, golden SET).
