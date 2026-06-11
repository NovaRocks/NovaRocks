# OQ-10 Runtime Filter Hardening Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Harden NovaRocks runtime-filter planning so representative semi/anti/oriented/cross-exchange join shapes get safe RF coverage and pass correctness regressions.

**Architecture:** Keep RF runtime data structures intact. Add a small plan-quality collector, then refactor `runtime_filter_pass.rs` around explicit child binding, join-side eligibility, conservative probe placement, and stable RF limits. Codegen remains defensive lowering; it no longer repairs optimizer-side orientation as a normal path.

**Tech Stack:** Rust optimizer/codegen, NovaRocks SQL test runner, Python 3 standard library for plan-quality collection, MySQL CLI/protocol, StarRocks FE-on-NovaRocks runtime.

---

## Scope Check

This plan covers one coherent OQ-10 workstream with four independently committable stages:

1. Stage 0 creates the baseline collector and proves the old `logs/plan-quality` input can be regenerated.
2. Stage 1 hardens fragment-local RF planning.
3. Stage 2 enables conservative cross-exchange placement only for complete build-side RF.
4. Stage 3 runs validation and updates OQ-10 status.

The plan does not implement partitioned multi-BE partial RF merge. That remains outside this work because the approved design explicitly marks it as a follow-up.

## File Structure

- Create: `tools/plan-quality/rf_plan_diff.py`
  - Runs `EXPLAIN VERBOSE` against FE and NovaRocks ports, writes raw output and RF summaries.
- Modify: `src/sql/optimizer/runtime_filter_pass.rs`
  - Owns RF eligibility, expression orientation, probe placement, dedup, and unit tests.
- Modify: `src/sql/optimizer/options.rs`
  - Adds RF count cap and changes cross-exchange flag default after conservative rules exist.
- Modify: `src/sql/codegen/fragment_builder.rs`
  - Keeps defensive RF lowering, removes the normal-path orientation fallback, and skips invalid RFs cleanly.
- Modify: `sql-tests/optimizer/sql/runtime_filter_inner_join.sql`
  - Preserve baseline RF explain checks.
- Modify: `sql-tests/optimizer/sql/runtime_filter_cross_exchange.sql`
  - Change expectations from global flag-off to conservative broadcast-only crossing.
- Create: `sql-tests/optimizer/sql/runtime_filter_semi_anti_orientation.sql`
  - Plan-shape golden for semi/anti/orientation/guard behavior.
- Create: `sql-tests/optimizer/sql/runtime_filter_project_setop_remap.sql`
  - Plan-shape golden for project/derived and set-op boundaries.
- Create: `sql-tests/runtime-filter/sql/runtime_filter_outer_cross_exchange_guard.sql`
  - Runtime correctness for outer/null-key guard.
- Modify: `/Users/harbor/Documents/Obsidian/NovaRocks TODO/OQ-10-runtime-filter-hardening.md`
  - Final validation note after code and tests pass.

## Task 1: Stage 0 Plan-Quality Collector

**Files:**
- Create: `tools/plan-quality/rf_plan_diff.py`

- [ ] **Step 1: Write the collector script**

Create `tools/plan-quality/rf_plan_diff.py` with this content:

```python
#!/usr/bin/env python3
"""Collect FE-vs-NovaRocks runtime-filter EXPLAIN VERBOSE differences."""

from __future__ import annotations

import argparse
import json
import os
import re
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable


RF_PATTERNS = (
    "runtime filter",
    "runtime filters",
    "build runtime filters",
    "probe runtime filters",
    "build_expr",
    "probe_expr",
)


DEFAULT_CASES = (
    "tpc-h/q4",
    "tpc-h/q22",
    "tpc-ds/q41",
    "tpc-ds/q72",
    "ssb/q1.1",
    "ssb/q1.2",
    "ssb/q1.3",
    "ssb/q2.1",
    "ssb/q2.2",
    "ssb/q2.3",
    "ssb/q3.1",
    "ssb/q3.2",
    "ssb/q3.3",
    "ssb/q3.4",
    "ssb/q4.1",
    "ssb/q4.2",
    "ssb/q4.3",
)


@dataclass(frozen=True)
class Endpoint:
    name: str
    host: str
    port: str
    user: str


def repo_root() -> Path:
    return Path(__file__).resolve().parents[2]


def case_to_sql_path(case_id: str) -> Path:
    suite, case_name = case_id.split("/", 1)
    return repo_root() / "sql-tests" / suite / "sql" / f"{case_name}.sql"


def explain_sql(raw_sql: str) -> str:
    stripped = raw_sql.strip().rstrip(";")
    return f"EXPLAIN VERBOSE {stripped};"


def run_mysql(endpoint: Endpoint, sql: str, timeout: int) -> str:
    cmd = [
        "mysql",
        "-h",
        endpoint.host,
        "-P",
        endpoint.port,
        "-u",
        endpoint.user,
        "--batch",
        "--raw",
        "--skip-column-names",
        "-e",
        sql,
    ]
    env = os.environ.copy()
    env.update(
        {
            "NO_PROXY": "127.0.0.1,localhost",
            "no_proxy": "127.0.0.1,localhost",
            "HTTP_PROXY": "",
            "HTTPS_PROXY": "",
            "ALL_PROXY": "",
            "http_proxy": "",
            "https_proxy": "",
            "all_proxy": "",
        }
    )
    result = subprocess.run(
        cmd,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        timeout=timeout,
        env=env,
        check=False,
    )
    if result.returncode != 0:
        raise RuntimeError(
            f"{endpoint.name} mysql failed for port {endpoint.port}: {result.stderr.strip()}"
        )
    return result.stdout


def rf_lines(explain: str) -> list[str]:
    lines = []
    for line in explain.splitlines():
        lowered = line.lower()
        if any(pattern in lowered for pattern in RF_PATTERNS):
            lines.append(line)
    return lines


def safe_file_name(case_id: str) -> str:
    return re.sub(r"[^A-Za-z0-9_.-]+", "__", case_id)


def collect_case(
    case_id: str,
    endpoints: Iterable[Endpoint],
    output_dir: Path,
    timeout: int,
) -> dict[str, object]:
    sql_path = case_to_sql_path(case_id)
    raw_sql = sql_path.read_text()
    sql = explain_sql(raw_sql)
    entry: dict[str, object] = {"case": case_id, "sql_path": str(sql_path)}
    for endpoint in endpoints:
        explain = run_mysql(endpoint, sql, timeout)
        out_path = output_dir / endpoint.name / f"{safe_file_name(case_id)}.out"
        out_path.parent.mkdir(parents=True, exist_ok=True)
        out_path.write_text(explain)
        lines = rf_lines(explain)
        entry[endpoint.name] = {
            "file": str(out_path),
            "runtime_filter_line_count": len(lines),
            "runtime_filter_lines": lines,
        }
    return entry


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--fe-host", default="127.0.0.1")
    parser.add_argument("--fe-port", required=True)
    parser.add_argument("--nr-host", default="127.0.0.1")
    parser.add_argument("--nr-port", required=True)
    parser.add_argument("--user", default="root")
    parser.add_argument("--output-dir", required=True)
    parser.add_argument("--timeout", type=int, default=120)
    parser.add_argument("--case", action="append", dest="cases")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    cases = args.cases or list(DEFAULT_CASES)
    output_dir = Path(args.output_dir)
    endpoints = (
        Endpoint("fe", args.fe_host, args.fe_port, args.user),
        Endpoint("nr", args.nr_host, args.nr_port, args.user),
    )
    summary = []
    for case_id in cases:
        summary.append(collect_case(case_id, endpoints, output_dir, args.timeout))
    status_dir = output_dir / "status"
    status_dir.mkdir(parents=True, exist_ok=True)
    (status_dir / "aggregate_summary.json").write_text(
        json.dumps(summary, indent=2, ensure_ascii=False) + "\n"
    )
    rows = [
        "| case | FE RF lines | NR RF lines |",
        "|---|---:|---:|",
    ]
    for item in summary:
        fe_count = item["fe"]["runtime_filter_line_count"]  # type: ignore[index]
        nr_count = item["nr"]["runtime_filter_line_count"]  # type: ignore[index]
        rows.append(f"| {item['case']} | {fe_count} | {nr_count} |")
    (status_dir / "representative_queries.md").write_text("\n".join(rows) + "\n")
    return 0


if __name__ == "__main__":
    sys.exit(main())
```

- [ ] **Step 2: Run the collector help**

Run:

```bash
python3 tools/plan-quality/rf_plan_diff.py --help
```

Expected: PASS and output includes `--fe-port`, `--nr-port`, and `--output-dir`.

- [ ] **Step 3: Commit Stage 0 tool**

```bash
git add tools/plan-quality/rf_plan_diff.py
git commit -m "Add runtime filter plan diff collector"
```

## Task 2: Add Optimizer Unit Tests for RF Side Semantics

**Files:**
- Modify: `src/sql/optimizer/runtime_filter_pass.rs`

- [ ] **Step 1: Add test-support helpers**

Inside `#[cfg(test)] pub(crate) mod test_support`, add these helper functions after `inner_join_two_scans()`:

```rust
    pub(crate) fn hash_join_two_scans(join_type: JoinKind) -> PhysicalPlanNode {
        let (loc, lexpr) = col(1, "lc");
        let (roc, rexpr) = col(2, "rc");
        let left = leaf(1_000_000.0, loc.clone());
        let right = leaf(10.0, roc.clone());
        PhysicalPlanNode {
            op: Operator::PhysicalHashJoin(PhysicalHashJoinOp {
                join_type,
                eq_conditions: vec![PhysicalHashJoinEqCondition {
                    left: lexpr,
                    right: rexpr,
                    null_safe: false,
                }],
                other_condition: None,
                distribution: JoinDistribution::Broadcast,
            }),
            children: vec![left, right],
            stats: Statistics {
                output_row_count: 10.0,
                column_statistics: Default::default(),
                ..Default::default()
            },
            output_columns: vec![loc, roc],
            execution_props: crate::sql::optimizer::physical_plan::PlanExecutionProps::default(),
            build_runtime_filters: vec![],
            probe_runtime_filters: vec![],
        }
    }

    pub(crate) fn cross_hash_join_without_eq_conditions() -> PhysicalPlanNode {
        let (loc, _) = col(1, "lc");
        let (roc, _) = col(2, "rc");
        let left = leaf(1_000_000.0, loc.clone());
        let right = leaf(10.0, roc.clone());
        PhysicalPlanNode {
            op: Operator::PhysicalHashJoin(PhysicalHashJoinOp {
                join_type: JoinKind::Cross,
                eq_conditions: vec![],
                other_condition: None,
                distribution: JoinDistribution::Broadcast,
            }),
            children: vec![left, right],
            stats: Statistics {
                output_row_count: 10.0,
                column_statistics: Default::default(),
                ..Default::default()
            },
            output_columns: vec![loc, roc],
            execution_props: crate::sql::optimizer::physical_plan::PlanExecutionProps::default(),
            build_runtime_filters: vec![],
            probe_runtime_filters: vec![],
        }
    }

    pub(crate) fn inner_join_with_swapped_eq_labels() -> PhysicalPlanNode {
        let (loc, lexpr) = col(1, "lc");
        let (roc, rexpr) = col(2, "rc");
        let left = leaf(1_000_000.0, loc.clone());
        let right = leaf(10.0, roc.clone());
        PhysicalPlanNode {
            op: Operator::PhysicalHashJoin(PhysicalHashJoinOp {
                join_type: JoinKind::Inner,
                eq_conditions: vec![PhysicalHashJoinEqCondition {
                    left: rexpr,
                    right: lexpr,
                    null_safe: false,
                }],
                other_condition: None,
                distribution: JoinDistribution::Broadcast,
            }),
            children: vec![left, right],
            stats: Statistics {
                output_row_count: 10.0,
                column_statistics: Default::default(),
                ..Default::default()
            },
            output_columns: vec![loc, roc],
            execution_props: crate::sql::optimizer::physical_plan::PlanExecutionProps::default(),
            build_runtime_filters: vec![],
            probe_runtime_filters: vec![],
        }
    }
```

- [ ] **Step 2: Add failing eligibility tests**

Inside `#[cfg(test)] mod tests`, add:

```rust
    #[test]
    fn rf_join_eligibility_matches_probe_output_semantics() {
        use crate::sql::analysis::JoinKind;

        for kind in [
            JoinKind::Inner,
            JoinKind::LeftSemi,
            JoinKind::RightOuter,
            JoinKind::RightSemi,
            JoinKind::RightAnti,
        ] {
            let mut join = super::test_support::hash_join_two_scans(kind);
            annotate(&mut join, &OptimizerOptions::default_settings());
            assert_eq!(
                join.build_runtime_filters.len(),
                1,
                "{kind:?} should build an RF"
            );
        }

        for kind in [
            JoinKind::LeftOuter,
            JoinKind::FullOuter,
            JoinKind::LeftAnti,
            JoinKind::NullAwareLeftAnti,
        ] {
            let mut join = super::test_support::hash_join_two_scans(kind);
            annotate(&mut join, &OptimizerOptions::default_settings());
            assert!(
                join.build_runtime_filters.is_empty(),
                "{kind:?} should not build an RF"
            );
        }

        let mut cross = super::test_support::cross_hash_join_without_eq_conditions();
        annotate(&mut cross, &OptimizerOptions::default_settings());
        assert!(
            cross.build_runtime_filters.is_empty(),
            "Cross without equality keys should not build an RF"
        );
    }

    #[test]
    fn rf_orients_swapped_eq_labels_by_child_column_ids() {
        let mut join = super::test_support::inner_join_with_swapped_eq_labels();
        annotate(&mut join, &OptimizerOptions::default_settings());
        assert_eq!(join.build_runtime_filters.len(), 1);
        assert_eq!(join.children[0].probe_runtime_filters.len(), 1);
        assert!(join.children[1].probe_runtime_filters.is_empty());
        assert_eq!(
            column_id_vec(&join.build_runtime_filters[0].build_expr),
            vec![crate::sql::column_id::ColumnId::new_for_test(2)]
        );
        assert_eq!(
            column_id_vec(&join.build_runtime_filters[0].probe_expr),
            vec![crate::sql::column_id::ColumnId::new_for_test(1)]
        );
    }
```

- [ ] **Step 3: Run tests and verify the orientation test fails**

Run:

```bash
cargo test --lib rf_orients_swapped_eq_labels_by_child_column_ids
```

Expected: FAIL because current RF pass stores `eq.right` as build expr and `eq.left` as probe expr even when those labels are swapped.

- [ ] **Step 4: Commit failing tests**

```bash
git add src/sql/optimizer/runtime_filter_pass.rs
git commit -m "test: cover runtime filter join side semantics"
```

## Task 3: Implement Join-Side Model and Eq Orientation

**Files:**
- Modify: `src/sql/optimizer/runtime_filter_pass.rs`

- [ ] **Step 1: Add child-side helpers**

Near `join_builds_rf`, replace that function with the following model:

```rust
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct JoinRfSides {
    probe_child: usize,
    build_child: usize,
}

fn rf_sides_for_join(kind: JoinKind) -> Option<JoinRfSides> {
    match kind {
        JoinKind::Inner
        | JoinKind::LeftSemi
        | JoinKind::RightOuter
        | JoinKind::RightSemi
        | JoinKind::RightAnti => Some(JoinRfSides {
            probe_child: 0,
            build_child: 1,
        }),
        JoinKind::LeftOuter
        | JoinKind::FullOuter
        | JoinKind::LeftAnti
        | JoinKind::NullAwareLeftAnti
        | JoinKind::Cross => None,
    }
}

fn column_id_vec(expr: &TypedExpr) -> Vec<ColumnId> {
    let mut ids = HashSet::new();
    column_ids(expr, &mut ids);
    let mut ids: Vec<ColumnId> = ids.into_iter().collect();
    ids.sort_unstable();
    ids
}

fn child_column_set(node: &PhysicalPlanNode) -> HashSet<ColumnId> {
    node.output_columns.iter().map(|c| c.column_id).collect()
}

fn expr_bound_child(node: &PhysicalPlanNode, expr: &TypedExpr) -> Option<usize> {
    let ids = column_id_vec(expr);
    if ids.is_empty() {
        return None;
    }
    let mut matched = Vec::new();
    for (idx, child) in node.children.iter().enumerate() {
        let have = child_column_set(child);
        if ids.iter().all(|id| have.contains(id)) {
            matched.push(idx);
        }
    }
    match matched.as_slice() {
        [idx] => Some(*idx),
        _ => None,
    }
}

#[derive(Clone, Debug)]
struct OrientedRfKey {
    build_expr: TypedExpr,
    probe_expr: TypedExpr,
    expr_order: usize,
}

fn orient_rf_key(
    node: &PhysicalPlanNode,
    sides: JoinRfSides,
    expr_order: usize,
    eq: &crate::sql::optimizer::operator::PhysicalHashJoinEqCondition,
) -> Option<OrientedRfKey> {
    let left_child = expr_bound_child(node, &eq.left)?;
    let right_child = expr_bound_child(node, &eq.right)?;
    if left_child == sides.probe_child && right_child == sides.build_child {
        return Some(OrientedRfKey {
            build_expr: eq.right.clone(),
            probe_expr: eq.left.clone(),
            expr_order,
        });
    }
    if left_child == sides.build_child && right_child == sides.probe_child {
        return Some(OrientedRfKey {
            build_expr: eq.left.clone(),
            probe_expr: eq.right.clone(),
            expr_order,
        });
    }
    None
}
```

- [ ] **Step 2: Update `annotate_node` to use `JoinRfSides`**

In `annotate_node`, replace the `join_builds_rf` check and fixed child indexing with:

```rust
    let Some(sides) = rf_sides_for_join(join.join_type) else {
        return;
    };
    if node.children.len() <= sides.build_child || node.children.len() <= sides.probe_child {
        return;
    }
    let eq_conditions = join.eq_conditions.clone();
    let distribution = join_distribution_for_runtime_filter(node, &join.distribution);
    if matches!(distribution, JoinDistribution::Unknown) {
        return;
    }
    let build_size = node.children[sides.build_child].stats.compute_size();
    let probe_size = node.children[sides.probe_child].stats.compute_size();
```

Then replace descriptor construction with:

```rust
    let mut descs: Vec<RuntimeFilterDesc> = Vec::new();
    for (expr_order, eq) in eq_conditions.iter().enumerate() {
        if eq.null_safe {
            continue;
        }
        if !probe_gate_passes(local, build_size, probe_size, build_min, probe_min, min_sel) {
            continue;
        }
        let Some(oriented) = orient_rf_key(node, sides, expr_order, eq) else {
            continue;
        };
        let filter_id = *next_filter_id;
        *next_filter_id += 1;
        descs.push(RuntimeFilterDesc {
            filter_id,
            build_expr: oriented.build_expr,
            probe_expr: oriented.probe_expr,
            expr_order: oriented.expr_order,
            distribution: distribution.clone(),
        });
    }
```

Finally replace probe pushdown target:

```rust
        let _ = push_probe_down(
            &mut node.children[sides.probe_child],
            &probe,
            options.allow_cross_exchange_rf,
        );
```

- [ ] **Step 3: Run orientation and existing RF tests**

Run:

```bash
cargo test --lib rf_orients_swapped_eq_labels_by_child_column_ids
cargo test --lib runtime_filter_pass
```

Expected: both commands PASS.

- [ ] **Step 4: Commit join-side model**

```bash
git add src/sql/optimizer/runtime_filter_pass.rs
git commit -m "Refine runtime filter join side orientation"
```

## Task 4: Add RF Count Cap and Stable Per-Join Dedup

**Files:**
- Modify: `src/sql/optimizer/options.rs`
- Modify: `src/sql/optimizer/runtime_filter_pass.rs`

- [ ] **Step 1: Add option tests**

In `src/sql/optimizer/options.rs`, add to `runtime_filter_thresholds_default_to_starrocks` assertions:

```rust
        assert_eq!(o.rf_max_count, 1024);
```

Add a new test:

```rust
    #[test]
    fn runtime_filter_max_count_default_is_stable() {
        let opts = OptimizerOptions::default_settings();
        assert_eq!(opts.rf_max_count, 1024);
    }
```

- [ ] **Step 2: Run option test and verify it fails**

Run:

```bash
cargo test --lib runtime_filter_max_count_default_is_stable
```

Expected: FAIL because `OptimizerOptions` has no `rf_max_count` field.

- [ ] **Step 3: Add `rf_max_count`**

In `OptimizerOptions`, add:

```rust
    /// Hard cap on runtime-filter descriptors emitted by one optimize call.
    /// The cap prevents complex TPC-DS plans from producing unbounded RF lists.
    pub rf_max_count: usize,
```

In `default_settings()`, add:

```rust
            rf_max_count: 1024,
```

- [ ] **Step 4: Enforce the cap**

In `annotate_node`, before assigning `filter_id`, add:

```rust
        if (*next_filter_id as usize) >= options.rf_max_count {
            continue;
        }
```

- [ ] **Step 5: Add a cap unit test**

In `runtime_filter_pass.rs` tests, add:

```rust
    #[test]
    fn rf_count_cap_limits_new_descriptors() {
        let mut join = super::test_support::inner_join_two_scans();
        let mut opts = OptimizerOptions::default_settings();
        opts.rf_max_count = 0;
        annotate(&mut join, &opts);
        assert!(join.build_runtime_filters.is_empty());
        assert_eq!(probe_runtime_filter_count(&join), 0);
    }
```

- [ ] **Step 6: Run cap tests**

Run:

```bash
cargo test --lib runtime_filter_max_count_default_is_stable
cargo test --lib rf_count_cap_limits_new_descriptors
cargo test --lib runtime_filter_pass
```

Expected: all commands PASS.

- [ ] **Step 7: Commit RF cap**

```bash
git add src/sql/optimizer/options.rs src/sql/optimizer/runtime_filter_pass.rs
git commit -m "Cap runtime filter descriptors per query"
```

## Task 5: Add Conservative Probe Placement Tests

**Files:**
- Modify: `src/sql/optimizer/runtime_filter_pass.rs`

- [ ] **Step 1: Add broadcast exchange and outer boundary helpers**

Inside `test_support`, add:

```rust
    pub(crate) fn broadcast_join_with_probe_exchange() -> PhysicalPlanNode {
        use crate::sql::optimizer::operator::PhysicalDistributionOp;
        use crate::sql::optimizer::property::{DistributionSpec, HashSource};
        let (loc, lexpr) = col(1, "lc");
        let (roc, rexpr) = col(2, "rc");
        let scan = leaf(1_000_000.0, loc.clone());
        let exch = PhysicalPlanNode {
            op: Operator::PhysicalDistribution(PhysicalDistributionOp {
                spec: DistributionSpec::HashPartitioned {
                    cols: vec![loc.column_id],
                    source: HashSource::ShuffleJoin,
                },
            }),
            children: vec![scan],
            stats: Statistics {
                output_row_count: 1_000_000.0,
                column_statistics: Default::default(),
                ..Default::default()
            },
            output_columns: vec![loc.clone()],
            execution_props: crate::sql::optimizer::physical_plan::PlanExecutionProps::default(),
            build_runtime_filters: vec![],
            probe_runtime_filters: vec![],
        };
        let build = leaf(10.0, roc.clone());
        PhysicalPlanNode {
            op: Operator::PhysicalHashJoin(PhysicalHashJoinOp {
                join_type: JoinKind::Inner,
                eq_conditions: vec![PhysicalHashJoinEqCondition {
                    left: lexpr,
                    right: rexpr,
                    null_safe: false,
                }],
                other_condition: None,
                distribution: JoinDistribution::Broadcast,
            }),
            children: vec![exch, build],
            stats: Statistics {
                output_row_count: 10.0,
                column_statistics: Default::default(),
                ..Default::default()
            },
            output_columns: vec![loc, roc],
            execution_props: crate::sql::optimizer::physical_plan::PlanExecutionProps::default(),
            build_runtime_filters: vec![],
            probe_runtime_filters: vec![],
        }
    }

    pub(crate) fn inner_join_over_left_outer_probe_child() -> PhysicalPlanNode {
        let (a_oc, a_expr) = col(1, "a");
        let (b_oc, b_expr) = col(2, "b");
        let (c_oc, c_expr) = col(3, "c");
        let left_outer = PhysicalPlanNode {
            op: Operator::PhysicalHashJoin(PhysicalHashJoinOp {
                join_type: JoinKind::LeftOuter,
                eq_conditions: vec![PhysicalHashJoinEqCondition {
                    left: a_expr.clone(),
                    right: b_expr,
                    null_safe: false,
                }],
                other_condition: None,
                distribution: JoinDistribution::Broadcast,
            }),
            children: vec![leaf(1_000_000.0, a_oc.clone()), leaf(10.0, b_oc.clone())],
            stats: Statistics {
                output_row_count: 1_000_000.0,
                column_statistics: Default::default(),
                ..Default::default()
            },
            output_columns: vec![a_oc.clone(), b_oc],
            execution_props: crate::sql::optimizer::physical_plan::PlanExecutionProps::default(),
            build_runtime_filters: vec![],
            probe_runtime_filters: vec![],
        };
        let build = leaf(10.0, c_oc.clone());
        PhysicalPlanNode {
            op: Operator::PhysicalHashJoin(PhysicalHashJoinOp {
                join_type: JoinKind::Inner,
                eq_conditions: vec![PhysicalHashJoinEqCondition {
                    left: a_expr,
                    right: c_expr,
                    null_safe: false,
                }],
                other_condition: None,
                distribution: JoinDistribution::Broadcast,
            }),
            children: vec![left_outer, build],
            stats: Statistics {
                output_row_count: 10.0,
                column_statistics: Default::default(),
                ..Default::default()
            },
            output_columns: vec![a_oc, c_oc],
            execution_props: crate::sql::optimizer::physical_plan::PlanExecutionProps::default(),
            build_runtime_filters: vec![],
            probe_runtime_filters: vec![],
        }
    }
```

- [ ] **Step 2: Add failing conservative placement tests**

Add these tests:

```rust
    #[test]
    fn partitioned_rf_does_not_cross_exchange_even_when_flag_enabled() {
        let mut join = super::test_support::shuffle_join_with_probe_exchange();
        let mut opts = OptimizerOptions::default_settings();
        opts.allow_cross_exchange_rf = true;
        annotate(&mut join, &opts);
        let exch = &join.children[0];
        assert!(exch.probe_runtime_filters.is_empty());
        assert!(
            exch.children[0].probe_runtime_filters.is_empty(),
            "partitioned build RF is partial and must not cross exchange"
        );
    }

    #[test]
    fn broadcast_rf_crosses_exchange_when_flag_enabled() {
        let mut join = super::test_support::broadcast_join_with_probe_exchange();
        let mut opts = OptimizerOptions::default_settings();
        opts.allow_cross_exchange_rf = true;
        annotate(&mut join, &opts);
        let exch = &join.children[0];
        assert!(exch.probe_runtime_filters.is_empty());
        assert_eq!(exch.children[0].probe_runtime_filters.len(), 1);
    }

    #[test]
    fn probe_does_not_descend_through_outer_join_boundary() {
        let mut join = super::test_support::inner_join_over_left_outer_probe_child();
        annotate(&mut join, &OptimizerOptions::default_settings());
        assert_eq!(join.children[0].probe_runtime_filters.len(), 1);
        assert!(
            join.children[0].children[0].probe_runtime_filters.is_empty(),
            "outer preserved child must not receive probe RF"
        );
    }
```

- [ ] **Step 3: Run placement tests and verify failures**

Run:

```bash
cargo test --lib partitioned_rf_does_not_cross_exchange_even_when_flag_enabled
cargo test --lib broadcast_rf_crosses_exchange_when_flag_enabled
cargo test --lib probe_does_not_descend_through_outer_join_boundary
```

Expected: the partitioned and outer-boundary tests FAIL under the current `push_probe_down` implementation; broadcast crossing may pass or fail depending on existing flag behavior.

- [ ] **Step 4: Commit failing placement tests**

```bash
git add src/sql/optimizer/runtime_filter_pass.rs
git commit -m "test: cover conservative runtime filter placement"
```

## Task 6: Implement Conservative Probe Placement

**Files:**
- Modify: `src/sql/optimizer/runtime_filter_pass.rs`
- Modify: `src/sql/optimizer/options.rs`

- [ ] **Step 1: Replace boolean placement policy**

In `runtime_filter_pass.rs`, replace the `push_probe_down` signature that currently accepts `allow_cross_exchange: bool` with this policy:

```rust
#[derive(Clone, Copy, Debug)]
struct ProbePushPolicy {
    allow_cross_exchange: bool,
    cross_exchange_build_complete: bool,
}

fn join_is_outer_or_anti_boundary(kind: JoinKind) -> bool {
    matches!(
        kind,
        JoinKind::LeftOuter
            | JoinKind::RightOuter
            | JoinKind::FullOuter
            | JoinKind::LeftAnti
            | JoinKind::RightAnti
            | JoinKind::NullAwareLeftAnti
    )
}

fn is_probe_semantic_boundary(node: &PhysicalPlanNode) -> bool {
    matches!(&node.op, Operator::PhysicalHashJoin(join) if join_is_outer_or_anti_boundary(join.join_type))
}
```

Then replace the function body with:

```rust
fn push_probe_down(
    node: &mut PhysicalPlanNode,
    probe: &RuntimeFilterProbe,
    policy: ProbePushPolicy,
) -> bool {
    if policy.allow_cross_exchange
        && policy.cross_exchange_build_complete
        && distribution_is_crossable(node)
    {
        if let Some(child) = node.children.first_mut() {
            return push_probe_down(child, probe, policy);
        }
        return false;
    }
    if is_exchange(node) {
        return false;
    }
    if !could_bound(node, &probe.probe_expr) {
        return false;
    }
    if is_probe_semantic_boundary(node) {
        node.probe_runtime_filters.push(probe.clone());
        return true;
    }
    for child in &mut node.children {
        if push_probe_down(child, probe, policy) {
            return true;
        }
    }
    node.probe_runtime_filters.push(probe.clone());
    true
}
```

- [ ] **Step 2: Create policy in `annotate_node`**

Before pushing probes, add:

```rust
    let push_policy = ProbePushPolicy {
        allow_cross_exchange: options.allow_cross_exchange_rf,
        cross_exchange_build_complete: matches!(distribution, JoinDistribution::Broadcast),
    };
```

Then call:

```rust
        let _ = push_probe_down(&mut node.children[sides.probe_child], &probe, push_policy);
```

- [ ] **Step 3: Update default cross-exchange option**

In `src/sql/optimizer/options.rs`, change the `allow_cross_exchange_rf` doc to:

```rust
    /// Enables conservative cross-exchange probe placement. The placement code
    /// only crosses exchange when the build-side RF is complete, currently
    /// broadcast build, and stops at outer/null-preserving semantic boundaries.
    pub allow_cross_exchange_rf: bool,
```

In `default_settings()`, set:

```rust
            allow_cross_exchange_rf: true,
```

In `from_session()`, replace the stale false-inheritance comment with:

```rust
        // `allow_cross_exchange_rf` has no session override; the default is safe
        // because the placement rule itself rejects partial partitioned RF.
```

- [ ] **Step 4: Update stale cross-exchange tests**

Rename the old `probe_crosses_exchange_when_flag_enabled` test to `partitioned_rf_does_not_cross_exchange_even_when_flag_enabled` if both exist after rebasing this task. The expected assertions must be:

```rust
        assert!(
            exch.children[0].probe_runtime_filters.is_empty(),
            "partitioned RF must not cross exchange"
        );
```

- [ ] **Step 5: Run placement and options tests**

Run:

```bash
cargo test --lib runtime_filter_pass
cargo test --lib runtime_filter_thresholds_default_to_starrocks
```

Expected: both commands PASS. The old expectation that any shuffle exchange crosses when the flag is enabled must be gone.

- [ ] **Step 6: Commit conservative placement**

```bash
git add src/sql/optimizer/runtime_filter_pass.rs src/sql/optimizer/options.rs
git commit -m "Enable conservative runtime filter exchange placement"
```

## Task 7: Tighten Codegen RF Lowering Defensiveness

**Files:**
- Modify: `src/sql/codegen/fragment_builder.rs`

- [ ] **Step 1: Add a codegen unit test for invalid build binding**

In `fragment_builder.rs` tests near existing RF tests, add:

```rust
    #[test]
    fn runtime_filter_invalid_build_binding_is_skipped() {
        let mut plan = mixed_starrocks_iceberg_join_plan();
        let Operator::PhysicalHashJoin(op) = &plan.op else {
            panic!("expected hash join");
        };
        let left_expr = op.eq_conditions[0].left.clone();
        plan.build_runtime_filters = vec![RuntimeFilterDesc {
            filter_id: 99,
            build_expr: left_expr.clone(),
            probe_expr: left_expr,
            expr_order: 0,
            distribution: JoinDistribution::Broadcast,
        }];

        let starrocks_layout = PhysicalTableLayout {
            db_id: 11,
            table_id: 22,
            schema_id: 33,
            tablets: vec![StarRocksTabletRef {
                tablet_id: 101,
                partition_id: 201,
                version: 7,
            }],
        };
        let registry = mock_starrocks_and_iceberg_registry(&starrocks_layout);
        let catalog = MixedCatalog { starrocks_layout };

        let build =
            PlanFragmentBuilder::build(&plan, &catalog, &registry, "default").expect("build");
        let root = build
            .fragment_results
            .iter()
            .find(|fragment| fragment.fragment_id == build.root_fragment_id)
            .expect("root fragment");
        let hash_join_node = root
            .plan
            .nodes
            .iter()
            .find(|node| node.node_type == plan_nodes::TPlanNodeType::HASH_JOIN_NODE)
            .expect("hash join node");
        let join = hash_join_node.hash_join_node.as_ref().expect("hash join");
        assert!(
            join.build_runtime_filters
                .as_ref()
                .map(|filters| filters.is_empty())
                .unwrap_or(true),
            "invalid RF descriptor should be skipped"
        );
    }
```

This test uses the existing `mixed_starrocks_iceberg_join_plan`, `mock_starrocks_and_iceberg_registry`, and `MixedCatalog` helpers already defined in `fragment_builder.rs` tests.

- [ ] **Step 2: Run the codegen test and verify failure**

Run:

```bash
cargo test --lib runtime_filter_invalid_build_binding_is_skipped
```

Expected: FAIL if current code returns an error after both build/probe compile attempts fail.

- [ ] **Step 3: Change `build_rf_descriptors` to skip invalid build bindings**

In `build_rf_descriptors`, replace the fallback compile block:

```rust
            let build_texpr = match ExprCompiler::new(self.slot_allocator(), build_scope)
                .compile_typed(&rf.build_expr)
            {
                Ok(t) => t,
                Err(_) => ExprCompiler::new(self.slot_allocator(), build_scope)
                    .compile_typed(&rf.probe_expr)
                    .map_err(|e| {
                        format!(
                            "runtime filter {filter_id}: neither build nor probe key \
                             binds the build child scope: {e}"
                        )
                    })?,
            };
```

with:

```rust
            let build_texpr = match ExprCompiler::new(self.slot_allocator(), build_scope)
                .compile_typed(&rf.build_expr)
            {
                Ok(t) => t,
                Err(err) => {
                    log::debug!(
                        "skip runtime filter {filter_id}: build expr does not bind build scope: {err}"
                    );
                    continue;
                }
            };
```

- [ ] **Step 4: Run codegen RF tests**

Run:

```bash
cargo test --lib runtime_filter_invalid_build_binding_is_skipped
cargo test --lib remap_rf_expr_order
cargo test --lib runtime_filter_uses_execution_distribution_metadata
```

Expected: all commands PASS.

- [ ] **Step 5: Commit codegen defensive skip**

```bash
git add src/sql/codegen/fragment_builder.rs
git commit -m "Skip invalid runtime filter descriptors during lowering"
```

## Task 8: Add Optimizer Plan Golden Coverage

**Files:**
- Create: `sql-tests/optimizer/sql/runtime_filter_semi_anti_orientation.sql`
- Create: `sql-tests/optimizer/sql/runtime_filter_project_setop_remap.sql`
- Modify: `sql-tests/optimizer/sql/runtime_filter_cross_exchange.sql`
- Create: `sql-tests/optimizer/result/runtime_filter_semi_anti_orientation.result`
- Create: `sql-tests/optimizer/result/runtime_filter_project_setop_remap.result`
- Modify: `sql-tests/optimizer/result/runtime_filter_cross_exchange.result`

- [ ] **Step 1: Add semi/anti/orientation SQL case**

Create `sql-tests/optimizer/sql/runtime_filter_semi_anti_orientation.sql`:

```sql
CREATE TABLE ${case_db}.rf_side_l (k INT, v INT);
CREATE TABLE ${case_db}.rf_side_r (k INT, v INT);
INSERT INTO ${case_db}.rf_side_l VALUES (1, 10), (2, 20), (3, 30), (4, 40);
INSERT INTO ${case_db}.rf_side_r VALUES (2, 200), (3, 300);
ANALYZE TABLE ${case_db}.rf_side_l;
ANALYZE TABLE ${case_db}.rf_side_r;

-- @explain_contains=HASH JOIN (BROADCAST, LEFT SEMI
-- @explain_contains=build runtime filters:
-- @explain_contains=build_expr = (r.k)
-- @explain_contains=probe runtime filters:
-- @explain_contains=probe_expr = (l.k)
SELECT l.k
FROM ${case_db}.rf_side_l l
LEFT SEMI JOIN ${case_db}.rf_side_r r ON r.k = l.k
ORDER BY l.k;

-- @explain_contains=HASH JOIN (BROADCAST, LEFT ANTI
-- @explain_not_contains=build runtime filters:
SELECT l.k
FROM ${case_db}.rf_side_l l
LEFT ANTI JOIN ${case_db}.rf_side_r r ON r.k = l.k
ORDER BY l.k;
```

- [ ] **Step 2: Add project/remap SQL case**

Create `sql-tests/optimizer/sql/runtime_filter_project_setop_remap.sql`:

```sql
CREATE TABLE ${case_db}.rf_remap_a (k INT, v INT);
CREATE TABLE ${case_db}.rf_remap_b (k INT, v INT);
CREATE TABLE ${case_db}.rf_remap_c (k INT, v INT);
INSERT INTO ${case_db}.rf_remap_a VALUES (1, 10), (2, 20), (3, 30);
INSERT INTO ${case_db}.rf_remap_b VALUES (1, 100), (2, 200), (4, 400);
INSERT INTO ${case_db}.rf_remap_c VALUES (1, 1000), (2, 2000);
ANALYZE TABLE ${case_db}.rf_remap_a;
ANALYZE TABLE ${case_db}.rf_remap_b;
ANALYZE TABLE ${case_db}.rf_remap_c;

-- @explain_contains=build runtime filters:
-- @explain_contains=build_expr = (c.k)
-- @explain_contains=probe runtime filters:
-- @explain_contains=probe_expr = (pa.ak)
SELECT pa.ak
FROM (
    SELECT k AS ak, v FROM ${case_db}.rf_remap_a
) pa
JOIN ${case_db}.rf_remap_c c ON pa.ak = c.k
ORDER BY pa.ak;

-- @explain_contains=UNION ALL
-- @explain_contains=build runtime filters:
SELECT u.k
FROM (
    SELECT k FROM ${case_db}.rf_remap_a
    UNION ALL
    SELECT k FROM ${case_db}.rf_remap_b
) u
JOIN ${case_db}.rf_remap_c c ON u.k = c.k
ORDER BY u.k;
```

- [ ] **Step 3: Update cross-exchange SQL case expectation**

In `sql-tests/optimizer/sql/runtime_filter_cross_exchange.sql`, keep the existing partitioned query and add a broadcast-oriented query:

```sql
CREATE TABLE ${case_db}.rf_x_probe (k INT, v INT);
CREATE TABLE ${case_db}.rf_x_build (k INT, v INT);
INSERT INTO ${case_db}.rf_x_probe
    SELECT generate_series, generate_series FROM TABLE(generate_series(1, 100000));
INSERT INTO ${case_db}.rf_x_build VALUES (1, 1), (2, 2), (3, 3);
ANALYZE TABLE ${case_db}.rf_x_probe;
ANALYZE TABLE ${case_db}.rf_x_build;

-- @explain_contains=HASH JOIN (BROADCAST
-- @explain_contains=build runtime filters:
-- @explain_contains=probe runtime filters:
-- @explain_contains=probe_expr = (p.k)
SELECT count(*)
FROM (
    SELECT k, v FROM ${case_db}.rf_x_probe
) p
JOIN ${case_db}.rf_x_build b ON p.k = b.k;
```

Keep a negative assertion for partitioned crossing:

```sql
-- @explain_not_contains=probe_expr = (t1.av)
```

- [ ] **Step 4: Record optimizer expected results**

Start a standalone server using the generated environment:

```bash
source docker/iceberg-rest/runtime/current/env.sh
NO_PROXY=127.0.0.1,localhost cargo run -- standalone-server --config "$NOVAROCKS_STANDALONE_CONFIG"
```

In another shell, run:

```bash
source docker/iceberg-rest/runtime/current/env.sh
cargo run --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests -- \
  --config "$NOVAROCKS_SQL_TEST_CONFIG" \
  --suite optimizer \
  --only runtime_filter_semi_anti_orientation,runtime_filter_project_setop_remap,runtime_filter_cross_exchange \
  --mode record \
  --update-expected \
  --ref-port "$NOVA_ENV_MYSQL_PORT"
```

Expected: PASS and the three result files are created or updated.

- [ ] **Step 5: Verify optimizer RF golden**

Run:

```bash
source docker/iceberg-rest/runtime/current/env.sh
cargo run --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests -- \
  --config "$NOVAROCKS_SQL_TEST_CONFIG" \
  --suite optimizer \
  --only runtime_filter_semi_anti_orientation,runtime_filter_project_setop_remap,runtime_filter_cross_exchange \
  --mode verify
```

Expected: PASS.

- [ ] **Step 6: Commit optimizer golden**

```bash
git add sql-tests/optimizer/sql/runtime_filter_semi_anti_orientation.sql \
        sql-tests/optimizer/sql/runtime_filter_project_setop_remap.sql \
        sql-tests/optimizer/sql/runtime_filter_cross_exchange.sql \
        sql-tests/optimizer/result/runtime_filter_semi_anti_orientation.result \
        sql-tests/optimizer/result/runtime_filter_project_setop_remap.result \
        sql-tests/optimizer/result/runtime_filter_cross_exchange.result
git commit -m "Add runtime filter optimizer golden coverage"
```

## Task 9: Add Runtime Correctness Coverage

**Files:**
- Create: `sql-tests/runtime-filter/sql/runtime_filter_outer_cross_exchange_guard.sql`
- Create: `sql-tests/runtime-filter/result/runtime_filter_outer_cross_exchange_guard.result`

- [ ] **Step 1: Add runtime guard case**

Create `sql-tests/runtime-filter/sql/runtime_filter_outer_cross_exchange_guard.sql`:

```sql
-- @order_sensitive=true
-- @tags=runtime_filter,outer_join,cross_exchange_guard
DROP TABLE IF EXISTS ${case_db}.rf_outer_l;
DROP TABLE IF EXISTS ${case_db}.rf_outer_r;
DROP TABLE IF EXISTS ${case_db}.rf_outer_dim;
CREATE TABLE ${case_db}.rf_outer_l (id INT, k INT);
CREATE TABLE ${case_db}.rf_outer_r (k INT);
CREATE TABLE ${case_db}.rf_outer_dim (k INT);

INSERT INTO ${case_db}.rf_outer_l VALUES
    (1, 10),
    (2, NULL),
    (3, 30);
INSERT INTO ${case_db}.rf_outer_r VALUES (10);
INSERT INTO ${case_db}.rf_outer_dim VALUES (10), (30);

SET disable_optimizer_rules = '';
SELECT id, k
FROM (
    SELECT l.id, l.k, r.k AS rk
    FROM ${case_db}.rf_outer_l l
    FULL OUTER JOIN ${case_db}.rf_outer_r r
      ON l.k = r.k
) x
WHERE x.k IS NULL OR x.k IN (
    SELECT d.k FROM ${case_db}.rf_outer_dim d
)
ORDER BY id;

SET disable_optimizer_rules = 'RuntimeFilterPushDown';
SELECT id, k
FROM (
    SELECT l.id, l.k, r.k AS rk
    FROM ${case_db}.rf_outer_l l
    FULL OUTER JOIN ${case_db}.rf_outer_r r
      ON l.k = r.k
) x
WHERE x.k IS NULL OR x.k IN (
    SELECT d.k FROM ${case_db}.rf_outer_dim d
)
ORDER BY id;
```

- [ ] **Step 2: Record runtime expected result**

Run:

```bash
source docker/iceberg-rest/runtime/current/env.sh
cargo run --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests -- \
  --config "$NOVAROCKS_SQL_TEST_CONFIG" \
  --suite runtime-filter \
  --only runtime_filter_outer_cross_exchange_guard \
  --mode record \
  --update-expected \
  --ref-port "$NOVA_ENV_MYSQL_PORT"
```

Expected: PASS and both query blocks have identical rows in the result file.

- [ ] **Step 3: Verify targeted runtime-filter suite**

Run:

```bash
source docker/iceberg-rest/runtime/current/env.sh
cargo run --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests -- \
  --config "$NOVAROCKS_SQL_TEST_CONFIG" \
  --suite runtime-filter \
  --only runtime_filter_outer_cross_exchange_guard,runtime_filter_semi_join_exists,runtime_filter_anti_join_not_exists,runtime_filter_left_semi_null_probe \
  --mode verify
```

Expected: PASS.

- [ ] **Step 4: Commit runtime correctness case**

```bash
git add sql-tests/runtime-filter/sql/runtime_filter_outer_cross_exchange_guard.sql \
        sql-tests/runtime-filter/result/runtime_filter_outer_cross_exchange_guard.result
git commit -m "Add runtime filter outer join guard regression"
```

## Task 10: Refresh FE-vs-NovaRocks RF Baseline

**Files:**
- Create: `logs/plan-quality/YYYYMMDD-fe-nr-plan-diff/fe/*.out`
- Create: `logs/plan-quality/YYYYMMDD-fe-nr-plan-diff/nr/*.out`
- Create: `logs/plan-quality/YYYYMMDD-fe-nr-plan-diff/status/aggregate_summary.json`
- Create: `logs/plan-quality/YYYYMMDD-fe-nr-plan-diff/status/representative_queries.md`

- [ ] **Step 1: Discover runtime ports**

Run:

```bash
CURRENT_DIR_NAME=$(basename "$(pwd)" | tr '[:upper:]' '[:lower:]')
if [ "${CURRENT_DIR_NAME}" = "starrocks" ]; then
  STARROCKS_ROOT=$(pwd)
else
  STARROCKS_ROOT="${STARROCKS_ROOT:-$HOME/project/starrocks}"
fi
if [ "${CURRENT_DIR_NAME}" = "novarocks" ]; then
  NOVAROCKS_ROOT=$(pwd)
else
  NOVAROCKS_ROOT="${NOVAROCKS_ROOT:-$HOME/project/NovaRocks}"
fi
DEPLOY_ROOT="${DEPLOY_ROOT:-$HOME/starrocks-on-novarocks}"
FE_RUNTIME_ROOT="${FE_RUNTIME_ROOT:-${DEPLOY_ROOT}/fe}"
BE_RUNTIME_ROOT="${BE_RUNTIME_ROOT:-${DEPLOY_ROOT}/novarocks}"
FE_CONF="${FE_CONF:-${FE_RUNTIME_ROOT}/conf/fe.conf}"
BE_CONF="${BE_CONF:-${BE_RUNTIME_ROOT}/conf/novarocks.toml}"
QUERY_PORT=$(grep -E '^[[:space:]]*query_port[[:space:]]*=' "${FE_CONF}" | awk -F= '{gsub(/[[:space:]]/, "", $2); print $2}')
source docker/iceberg-rest/runtime/current/env.sh
printf 'FE query port: %s\nNovaRocks standalone port: %s\n' "$QUERY_PORT" "$NOVA_ENV_MYSQL_PORT"
```

Expected: prints both ports.

- [ ] **Step 2: Run collector**

Run:

```bash
OUT="logs/plan-quality/$(date +%Y%m%d)-fe-nr-plan-diff"
python3 tools/plan-quality/rf_plan_diff.py \
  --fe-port "$QUERY_PORT" \
  --nr-port "$NOVA_ENV_MYSQL_PORT" \
  --output-dir "$OUT"
```

Expected: PASS and `status/representative_queries.md` contains rows for `tpc-h/q4`, `tpc-h/q22`, `tpc-ds/q41`, `tpc-ds/q72`, and all 13 `ssb` cases.

- [ ] **Step 3: Inspect representative query summary**

Run:

```bash
sed -n '1,80p' "$OUT/status/representative_queries.md"
```

Expected: table shows FE and NR RF line counts for every collected case. The table includes all 13 `ssb` rows.

- [ ] **Step 4: Commit plan-quality output**

```bash
git add "$OUT"
git commit -m "Refresh OQ-10 runtime filter plan baseline"
```

## Task 11: Full Targeted Verification

**Files:**
- No source changes unless a verification failure exposes a scoped fix.

- [ ] **Step 1: Run Rust targeted tests**

Run:

```bash
cargo test --lib runtime_filter_pass
cargo test --lib runtime_filter_thresholds_default_to_starrocks
cargo test --lib runtime_filter_invalid_build_binding_is_skipped
```

Expected: all commands PASS.

- [ ] **Step 2: Run optimizer RF golden**

Run:

```bash
source docker/iceberg-rest/runtime/current/env.sh
cargo run --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests -- \
  --config "$NOVAROCKS_SQL_TEST_CONFIG" \
  --suite optimizer \
  --only runtime_filter_inner_join,runtime_filter_cross_exchange,runtime_filter_equivalence,runtime_filter_semi_anti_orientation,runtime_filter_project_setop_remap \
  --mode verify
```

Expected: PASS.

- [ ] **Step 3: Run runtime-filter suite**

Run:

```bash
source docker/iceberg-rest/runtime/current/env.sh
cargo run --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests -- \
  --config "$NOVAROCKS_SQL_TEST_CONFIG" \
  --suite runtime-filter \
  --mode verify
```

Expected: PASS.

- [ ] **Step 4: Run SSB plan-shape smoke**

Run:

```bash
source docker/iceberg-rest/runtime/current/env.sh
cargo run --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests -- \
  --config "$NOVAROCKS_SQL_TEST_CONFIG" \
  --suite ssb \
  --mode verify \
  -j 1
```

Expected: PASS. If it fails, stop and fix the scoped RF regression before continuing.

- [ ] **Step 5: Run git whitespace check**

Run:

```bash
git diff --check HEAD~10..HEAD
```

Expected: no output.

## Task 12: Update OQ-10 Status Note

**Files:**
- Modify: `/Users/harbor/Documents/Obsidian/NovaRocks TODO/OQ-10-runtime-filter-hardening.md`

- [ ] **Step 1: Add validation summary**

Append a validation section to the Obsidian note with the latest generated plan-quality path:

```bash
LATEST_PLAN_DIR=$(ls -dt logs/plan-quality/*-fe-nr-plan-diff | head -1)
python3 - "$LATEST_PLAN_DIR" '/Users/harbor/Documents/Obsidian/NovaRocks TODO/OQ-10-runtime-filter-hardening.md' <<'PY'
from pathlib import Path
import sys

plan_dir = Path(sys.argv[1])
note = Path(sys.argv[2])
section = f"""

## 2026-06-08 实施记录

- Stage 0 基线刷新：`{plan_dir}/status/aggregate_summary.json`
- Stage 1 fragment 内 RF hardening：完成 semi/anti eligibility、ColumnId orientation、project/derived remap、稳定去重和 RF 数量上限。
- Stage 2 conservative cross-exchange：只允许 broadcast/global-complete build 跨 exchange；partitioned multi-BE partial RF 不跨；outer/null-producing boundary 会停止下推。
- 验证：
  - `cargo test --lib runtime_filter_pass`
  - `cargo test --lib runtime_filter_thresholds_default_to_starrocks`
  - `cargo test --lib runtime_filter_invalid_build_binding_is_skipped`
  - `sql-tests optimizer` targeted RF cases
  - `sql-tests runtime-filter`
- 保留非目标：partitioned multi-BE global RF merge 未实现，仍作为后续项。
"""
note.write_text(note.read_text() + section)
PY
```

- [ ] **Step 2: Confirm repository status**

Run:

```bash
git status --short
```

Expected: clean repo status after all repository commits. The Obsidian note is outside this repository.

## Final Handoff Checklist

- [ ] `git status --short` is clean except the external Obsidian note if it is not tracked by this repo.
- [ ] New plan-quality output path is included in the final response.
- [ ] The final response lists every verification command and PASS/failure status.
- [ ] The final response explicitly states that partitioned multi-BE global RF merge remains outside this implementation.
