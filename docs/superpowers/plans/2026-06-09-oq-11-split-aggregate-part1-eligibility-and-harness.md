# OQ-11 Part 1 — Measurement Harness + Capability-Driven Eligibility (avg) Implementation Plan

> **CORRECTION (post-rebase onto main #277):** Task 7 originally recorded an `avg`
> golden asserting a Local/Global split on small data. With real iceberg stats
> (main #277) small tables correctly use single-phase, so that golden was dropped;
> `avg` eligibility is covered by the `splits_grouped_avg_aggregate` unit test, and
> split-at-scale by the benchmark suites. The shipped change is eligibility + harness +
> constant cleanup (no cost-model change). See
> `docs/superpowers/specs/2026-06-09-oq-11-bucket2-diagnosis.md`.

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add the aggregate-split FE-vs-NR measurement harness and a capability-driven eligibility oracle that lets `avg` (the highest-leverage blocked function) generate two-phase Local/Global aggregates, then diagnose the eligible-but-unsplit (Bucket 2) queries to seed the follow-up plans.

**Architecture:** Introduce a single-source-of-truth capability oracle (`aggregate_mergeability`) in the shared `src/sql/` layer; `SplitAggregateRule` delegates eligibility to it. Add a Python plan-diff tool that counts aggregate phase markers on both StarRocks-FE and NovaRocks EXPLAIN output. The execution layer already merges `avg` correctly (`fragment_builder.rs` derives the Local slot type from the function's `intermediate_type`), so adding `avg` is an optimizer-only change verified by result-equality goldens.

**Tech Stack:** Rust (optimizer cascades rules, codegen type inference), Python 3 (plan-diff harness), `sql-tests` optimizer golden suite, MySQL-protocol `EXPLAIN VERBOSE`.

---

## Scope

This is **Part 1 of OQ-11**. The approved spec (`docs/superpowers/specs/2026-06-09-oq-11-split-aggregate-coverage-parity-design.md`) is a full-parity, six-phase program. This plan covers what is fully specifiable today with exact TDD code:

- **Phase 0** — the `agg_plan_diff.py` harness (Tasks 1–2).
- **Phase 1** — capability oracle + `avg` eligibility (Tasks 3–7). Fixes the Bucket-1 blocker in `tpc-h/q1`, `tpc-ds/q28/q44/q85` (all blocked solely by `avg`).
- **Phase 2 diagnosis** — classify the Bucket-2 (eligible-but-unsplit) queries into root-cause buckets (Task 8). Output is the work list that seeds the follow-up plans.

**Deferred to follow-up plans (NOT in this plan), because they depend on diagnosis data that does not exist until Task 8 runs:**

- Phase 1 round 2: expanding the oracle past `avg` to the float (`stddev`/`variance`/`covar`/`corr`) and binary-state/sketch families (`percentile_*`, `approx_*`, `bitmap_union`, `hll`, state-combinators). These need tolerance-based or sketch-equality correctness tests, not the exact `==` goldens used here. The oracle in Task 4 is structured so each addition is a one-line change plus one test.
- Phase 2 fixes (structural coverage for derived/set-op/rollup shapes), Phase 3 (AggregatePushdown coordination), Phase 4 (distribution/RF-probe preservation), Phase 5 (FE convergence report).

This Part-1 plan produces working, testable software on its own: a usable measurement tool and a real parity win (`avg` two-phase) with regression coverage.

---

## File Structure

- **Create** `tools/plan-quality/agg_plan_diff.py` — FE-vs-NR aggregate phase-marker counter (derived from `rf_plan_diff.py`). One responsibility: run `EXPLAIN VERBOSE` over both endpoints and tabulate Single/Local/Global (NR) vs update/merge (FE) aggregate counts per case.
- **Create** `tools/plan-quality/test_agg_plan_diff.py` — standalone unit test for the pure counting function (no live server needed).
- **Create** `src/sql/agg_mergeability.rs` — the capability oracle (single source of truth for two-phase splittability). One responsibility: classify an `AggregateCall` as `TwoPhase` or `SinglePhaseOnly`.
- **Modify** `src/sql/mod.rs` — register the new module.
- **Modify** `src/sql/codegen/expr_compiler.rs` — widen `infer_agg_function_types` visibility to `pub(crate)` so the oracle's drift-guard test can cross-check it.
- **Modify** `src/sql/optimizer/cascades_rules/split_aggregate.rs` — `is_splittable_aggregate` delegates to the oracle; add an `avg`-splits rule test.
- **Create** `sql-tests/optimizer/sql/split_aggregate_avg.sql` — result-equality + plan-shape golden for `avg` two-phase.
- **Create** `docs/superpowers/specs/2026-06-09-oq-11-bucket2-diagnosis.md` — Task 8 output: the Bucket-2 root-cause work list.

---

## Task 1: Aggregate phase-marker counter (pure function + test)

**Files:**
- Create: `tools/plan-quality/test_agg_plan_diff.py`
- Create: `tools/plan-quality/agg_plan_diff.py` (counting function only in this task; the live-run wiring is Task 2)

- [ ] **Step 1: Write the failing test**

Create `tools/plan-quality/test_agg_plan_diff.py`:

```python
#!/usr/bin/env python3
"""Standalone unit test for agg_plan_diff.agg_counts (no live server needed).

Run: python3 tools/plan-quality/test_agg_plan_diff.py
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from agg_plan_diff import agg_counts  # noqa: E402

NR_EXPLAIN = """
PLAN FRAGMENT 0
  HASH AGGREGATE (GLOBAL) stats={rows=3}
    HASH EXCHANGE (source: ShuffleAgg)
      HASH AGGREGATE (LOCAL) stats={rows=3}
        OLAP SCAN (t)
"""

FE_EXPLAIN = """
PLAN FRAGMENT 0
  3:AGGREGATE (merge finalize)
  |  group by: 1: k
  2:EXCHANGE
  1:AGGREGATE (update serialize)
  0:OlapScanNode
"""


def main() -> int:
    nr = agg_counts(NR_EXPLAIN, "nr")
    assert nr == {"single": 0, "local": 1, "global": 1}, nr

    fe = agg_counts(FE_EXPLAIN, "fe")
    assert fe == {"single": 0, "update": 1, "merge": 1}, fe

    print("OK")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
```

- [ ] **Step 2: Run test to verify it fails**

Run: `python3 tools/plan-quality/test_agg_plan_diff.py`
Expected: FAIL with `ModuleNotFoundError: No module named 'agg_plan_diff'` (the module does not exist yet).

- [ ] **Step 3: Write the minimal implementation**

Create `tools/plan-quality/agg_plan_diff.py` with ONLY the counting function for now:

```python
#!/usr/bin/env python3
"""Collect FE-vs-NovaRocks aggregate split-shape EXPLAIN VERBOSE differences."""

from __future__ import annotations

import re


# NovaRocks EXPLAIN aggregate-mode markers (see src/sql/explain.rs:581).
_NR_PATTERNS = {
    "single": re.compile(r"HASH AGGREGATE \(SINGLE"),
    "local": re.compile(r"HASH AGGREGATE \(LOCAL"),
    "global": re.compile(r"HASH AGGREGATE \(GLOBAL"),
}

# StarRocks FE EXPLAIN aggregate-phase markers.
_FE_PATTERNS = {
    "single": re.compile(r"AGGREGATE \((?:update|merge) finalize\)"),
    "update": re.compile(r"AGGREGATE \(update (?:serialize|finalize)\)"),
    "merge": re.compile(r"AGGREGATE \(merge (?:serialize|finalize)\)"),
}


def agg_counts(explain: str, dialect: str) -> dict[str, int]:
    """Count aggregate phase markers per dialect.

    NR keys: single/local/global. FE keys: single/update/merge.
    FE 'single' (a lone `update finalize` with no matching merge) is resolved
    by the caller via update-vs-merge balance; here we report raw marker hits
    for update/merge and reserve 'single' for the NR dialect only.
    """
    if dialect == "nr":
        return {k: len(p.findall(explain)) for k, p in _NR_PATTERNS.items()}
    if dialect == "fe":
        update = len(_FE_PATTERNS["update"].findall(explain))
        merge = len(_FE_PATTERNS["merge"].findall(explain))
        # An FE aggregate that is NOT split shows a single `update finalize`
        # with no paired merge; treat unpaired update-finalize as 'single'.
        single = max(0, len(re.findall(r"AGGREGATE \(update finalize\)", explain)))
        return {"single": single if merge == 0 else 0, "update": update, "merge": merge}
    raise ValueError(f"unknown dialect: {dialect!r}")
```

- [ ] **Step 4: Run test to verify it passes**

Run: `python3 tools/plan-quality/test_agg_plan_diff.py`
Expected: `OK`

- [ ] **Step 5: Commit**

```bash
git add tools/plan-quality/agg_plan_diff.py tools/plan-quality/test_agg_plan_diff.py
git commit -m "feat(plan-quality): aggregate phase-marker counter for split parity diff"
```

---

## Task 2: Wire the harness to live FE + NR endpoints

**Files:**
- Modify: `tools/plan-quality/agg_plan_diff.py` (append the CLI/run wiring)

This task adapts the proven structure of `tools/plan-quality/rf_plan_diff.py` (Endpoint / run_mysql / explain_sql / collect_case / main) to drive both endpoints and emit a summary. The live run itself is a documented procedure (its output is an artifact, not a code test), so there is no unit test here — Task 1 already covers the pure logic.

- [ ] **Step 1: Append the run wiring to `agg_plan_diff.py`**

Append to `tools/plan-quality/agg_plan_diff.py`:

```python
import argparse
import json
import subprocess
import sys
from pathlib import Path
from typing import NamedTuple


DEFAULT_CASES = (
    "tpc-h/q1", "tpc-h/q7", "tpc-h/q8", "tpc-h/q9",
    "tpc-ds/q28", "tpc-ds/q44", "tpc-ds/q54", "tpc-ds/q67", "tpc-ds/q75", "tpc-ds/q85",
    "ssb/q1.1", "ssb/q2.1", "ssb/q3.1", "ssb/q4.1",
)
DEFAULT_DATABASES = {"ssb": "ssb", "tpc-h": "tpch", "tpc-ds": "tpcds"}


class Endpoint(NamedTuple):
    name: str
    host: str
    port: str
    user: str
    dialect: str


def repo_root() -> Path:
    return Path(__file__).resolve().parents[2]


def case_to_sql_path(case_id: str) -> Path:
    suite, case = case_id.split("/", 1)
    return repo_root() / "sql-tests" / suite / "sql" / f"{case}.sql"


def case_default_database(case_id: str) -> str | None:
    return DEFAULT_DATABASES.get(case_id.split("/", 1)[0])


def explain_sql(raw_sql: str) -> str:
    stripped = raw_sql.strip().rstrip(";").strip()
    return f"EXPLAIN VERBOSE {stripped};"


def run_mysql(endpoint: Endpoint, sql: str, timeout: int, database: str | None) -> str:
    cmd = ["mysql", "-h", endpoint.host, "-P", endpoint.port, "-u", endpoint.user, "--batch", "--raw"]
    if database:
        cmd += ["-D", database]
    cmd += ["-e", sql]
    proc = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout)
    if proc.returncode != 0:
        raise RuntimeError(f"{endpoint.name} mysql failed: {proc.stderr.strip()}")
    return proc.stdout


def safe_file_name(case_id: str) -> str:
    return case_id.replace("/", "__")


def collect_case(endpoint: Endpoint, case_id: str, out_dir: Path, timeout: int) -> dict[str, int]:
    raw = case_to_sql_path(case_id).read_text()
    explain = run_mysql(endpoint, explain_sql(raw), timeout, case_default_database(case_id))
    out_path = out_dir / endpoint.name / f"{safe_file_name(case_id)}.out"
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(explain)
    return agg_counts(explain, endpoint.dialect)


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
    out_dir = Path(args.output_dir)
    fe = Endpoint("fe", args.fe_host, args.fe_port, args.user, "fe")
    nr = Endpoint("nr", args.nr_host, args.nr_port, args.user, "nr")
    cases = tuple(args.cases) if args.cases else DEFAULT_CASES

    rows = []
    summary = []
    for case_id in cases:
        try:
            fe_counts = collect_case(fe, case_id, out_dir, args.timeout)
            nr_counts = collect_case(nr, case_id, out_dir, args.timeout)
        except Exception as exc:  # fail-loud per case, keep going
            print(f"[skip] {case_id}: {exc}", file=sys.stderr)
            continue
        fe_split = fe_counts["update"] + fe_counts["merge"]
        nr_split = nr_counts["local"] + nr_counts["global"]
        rows.append(f"| {case_id} | {fe_split} | {nr_split} |")
        summary.append({"case": case_id, "fe": fe_counts, "nr": nr_counts})

    status = out_dir / "status"
    status.mkdir(parents=True, exist_ok=True)
    (status / "aggregate_split_summary.json").write_text(json.dumps(summary, indent=2))
    table = ["| case | FE split markers | NR split markers |", "|---|---:|---:|", *rows]
    (status / "aggregate_split_table.md").write_text("\n".join(table) + "\n")
    print("\n".join(table))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
```

- [ ] **Step 2: Verify the file still imports cleanly (Task 1 test must still pass)**

Run: `python3 tools/plan-quality/test_agg_plan_diff.py`
Expected: `OK` (appending the CLI wiring must not break the pure function).

- [ ] **Step 3: Verify the CLI parses (no live server required)**

Run: `python3 tools/plan-quality/agg_plan_diff.py --help`
Expected: argparse usage text listing `--fe-port` and `--nr-port` as required.

- [ ] **Step 4: Commit**

```bash
git add tools/plan-quality/agg_plan_diff.py
git commit -m "feat(plan-quality): drive FE+NR EXPLAIN and tabulate aggregate split counts"
```

---

## Task 3: Expose `infer_agg_function_types` for the drift-guard

**Files:**
- Modify: `src/sql/codegen/expr_compiler.rs:2720`

The oracle's drift-guard test (Task 5) cross-checks each `TwoPhase` function against the planning-layer type inference. That function is currently a private `fn`; widen it to `pub(crate)`.

- [ ] **Step 1: Widen visibility**

In `src/sql/codegen/expr_compiler.rs`, change the signature at line 2720:

```rust
/// Returns (output_type, intermediate_type) for aggregate functions.
/// `None` as intermediate_type means the execution layer should use its default.
pub(crate) fn infer_agg_function_types(
    name: &str,
    arg_types: &[DataType],
    _is_distinct: bool,
) -> Result<(DataType, Option<DataType>), String> {
```

(Only the `fn` keyword line gains `pub(crate)`; the body is unchanged.)

- [ ] **Step 2: Verify it compiles**

Run: `cargo build 2>&1 | tail -5`
Expected: build succeeds (no errors). A `dead_code`/unused warning is acceptable until Task 5 consumes it.

- [ ] **Step 3: Commit**

```bash
git add src/sql/codegen/expr_compiler.rs
git commit -m "refactor(codegen): expose infer_agg_function_types for agg-mergeability drift guard"
```

---

## Task 4: Capability oracle module (`aggregate_mergeability`)

**Files:**
- Create: `src/sql/agg_mergeability.rs`
- Modify: `src/sql/mod.rs:1-13`

- [ ] **Step 1: Write the failing tests (and the module skeleton they compile against)**

Create `src/sql/agg_mergeability.rs`:

```rust
//! Single source of truth for two-phase (Local/Global) aggregate split
//! eligibility. Shared by `SplitAggregateRule`. A function is `TwoPhase` only
//! when it has a well-defined local-update + global-merge decomposition whose
//! parallel-partition result equals the single-pass result.
//!
//! Conservative by default: distinct, ordered, order-sensitive, and unknown
//! functions stay `SinglePhaseOnly`. Distinct goes through `SplitDistinctAgg`.

use crate::sql::planner::plan::AggregateCall;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum AggMergeability {
    /// Local emits intermediate state, Global merges. Safe two-phase split.
    TwoPhase,
    /// Cannot be safely two-phased.
    SinglePhaseOnly,
}

/// Order-sensitive aggregates whose parallel-partition merge would change
/// concatenation/array ordering. These stay single-phase.
fn is_order_sensitive(name: &str) -> bool {
    matches!(
        name,
        "group_concat" | "string_agg" | "array_agg" | "array_agg_distinct"
    )
}

/// Functions with an exact, deterministically-verifiable local-update +
/// global-merge decomposition. Part 1 scope: the existing whitelist plus
/// `avg`. Float/sketch families (stddev/variance/percentile/approx/bitmap/hll)
/// are added in a follow-up round with tolerance/sketch-equality tests.
fn has_two_phase_merge(name: &str) -> bool {
    matches!(name, "sum" | "min" | "max" | "count" | "avg")
}

pub(crate) fn aggregate_mergeability(call: &AggregateCall) -> AggMergeability {
    let name = call.name.to_ascii_lowercase();
    if call.distinct
        || !call.order_by.is_empty()
        || is_order_sensitive(&name)
        || !has_two_phase_merge(&name)
    {
        AggMergeability::SinglePhaseOnly
    } else {
        AggMergeability::TwoPhase
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sql::analysis::{ExprKind, TypedExpr};
    use crate::sql::column_id::ColumnId;
    use arrow::datatypes::DataType;

    fn arg(ty: DataType) -> TypedExpr {
        TypedExpr {
            kind: ExprKind::ColumnRef {
                column_id: ColumnId::new_for_test(1),
                qualifier: None,
                column: "v".into(),
            },
            data_type: ty,
            nullable: true,
        }
    }

    fn call(name: &str, distinct: bool, ordered: bool) -> AggregateCall {
        AggregateCall {
            name: name.into(),
            args: vec![arg(DataType::Int64)],
            distinct,
            result_type: DataType::Float64,
            order_by: if ordered {
                vec![crate::sql::planner::plan::SortItem::default()]
            } else {
                vec![]
            },
            output_column_id: ColumnId::UNSET,
        }
    }

    #[test]
    fn avg_and_existing_whitelist_are_two_phase() {
        for name in ["sum", "min", "max", "count", "avg"] {
            assert_eq!(
                aggregate_mergeability(&call(name, false, false)),
                AggMergeability::TwoPhase,
                "{name} should be TwoPhase"
            );
        }
    }

    #[test]
    fn distinct_ordered_and_order_sensitive_are_single_phase() {
        assert_eq!(
            aggregate_mergeability(&call("avg", true, false)),
            AggMergeability::SinglePhaseOnly
        );
        assert_eq!(
            aggregate_mergeability(&call("sum", false, true)),
            AggMergeability::SinglePhaseOnly
        );
        assert_eq!(
            aggregate_mergeability(&call("group_concat", false, false)),
            AggMergeability::SinglePhaseOnly
        );
    }

    #[test]
    fn unknown_function_is_single_phase() {
        assert_eq!(
            aggregate_mergeability(&call("my_udaf", false, false)),
            AggMergeability::SinglePhaseOnly
        );
    }
}
```

Then register the module in `src/sql/mod.rs` — add the line in alphabetical position after `column_id`:

```rust
pub(crate) mod agg_mergeability;
```

(Insert it so the top of `src/sql/mod.rs` reads `analysis; catalog; column_id; agg_mergeability; functions; ...` — exact ordering is cosmetic; the line must be present.)

- [ ] **Step 2: Verify `SortItem::default()` exists; if not, build a minimal SortItem inline**

Run: `grep -n "impl Default for SortItem\|#\[derive(.*Default.*)\]" src/sql/planner/plan.rs | head; grep -n "pub struct SortItem" -A 8 src/sql/planner/plan.rs`
Expected: shows the `SortItem` definition. If `SortItem` does NOT derive `Default`, replace `SortItem::default()` in the test with a literal constructed from the fields the grep reveals (e.g. `SortItem { expr: arg(DataType::Int64), asc: true, nulls_first: false }`), matching the exact field names shown. Do not invent fields.

- [ ] **Step 3: Run the tests to verify they pass**

Run: `cargo test --lib agg_mergeability 2>&1 | tail -15`
Expected: `test result: ok. 3 passed`.

- [ ] **Step 4: Commit**

```bash
git add src/sql/agg_mergeability.rs src/sql/mod.rs
git commit -m "feat(optimizer): capability oracle for two-phase aggregate split eligibility"
```

---

## Task 5: Drift-guard test (oracle ⊆ planning-layer decomposable set)

**Files:**
- Modify: `src/sql/agg_mergeability.rs` (add one test)

This guarantees the oracle cannot classify a function `TwoPhase` that the planning layer cannot even infer an intermediate type for — the "single source of truth, no drift" contract from spec §5.1.

- [ ] **Step 1: Write the failing test**

Add to the `tests` module in `src/sql/agg_mergeability.rs`:

```rust
#[test]
fn two_phase_functions_have_planning_layer_intermediate_type() {
    use crate::sql::codegen::infer_agg_function_types;
    use arrow::datatypes::DataType;

    // Every name the oracle calls TwoPhase must be inferrable with a defined
    // intermediate type by the planning layer. `count` takes no args; the rest
    // are exercised with a single Int64 arg.
    for name in ["sum", "min", "max", "count", "avg"] {
        let args: &[DataType] = if name == "count" { &[] } else { &[DataType::Int64] };
        let inferred = infer_agg_function_types(name, args, false);
        assert!(
            matches!(inferred, Ok((_, Some(_)))),
            "{name} must infer (output, Some(intermediate)); got {inferred:?}"
        );
    }
}
```

- [ ] **Step 2: Verify the import path resolves; fix the re-export if needed**

Run: `cargo test --lib agg_mergeability::tests::two_phase_functions_have_planning_layer_intermediate_type 2>&1 | tail -20`
Expected: PASS. If it fails with `infer_agg_function_types is private` or an unresolved path, add a re-export to `src/sql/codegen/mod.rs`:

```rust
pub(crate) use expr_compiler::infer_agg_function_types;
```

then re-run. Expected after fix: `test result: ok. 1 passed`.

- [ ] **Step 3: Commit**

```bash
git add src/sql/agg_mergeability.rs src/sql/codegen/mod.rs
git commit -m "test(optimizer): drift-guard ties agg-mergeability oracle to planning-layer types"
```

---

## Task 6: SplitAggregateRule delegates to the oracle

**Files:**
- Modify: `src/sql/optimizer/cascades_rules/split_aggregate.rs:79-86` (replace `is_splittable_aggregate`)
- Modify: `src/sql/optimizer/cascades_rules/split_aggregate.rs` (tests module — add an `avg` split test)

- [ ] **Step 1: Write the failing rule test for `avg`**

Add to the `tests` module in `src/sql/optimizer/cascades_rules/split_aggregate.rs` (reuse the existing `values_group`, `output_column`, `nullable_col_ref` helpers already in that module):

```rust
fn avg_call() -> AggregateCall {
    AggregateCall {
        name: "avg".to_string(),
        args: vec![col_ref(2, "v")],
        distinct: false,
        result_type: arrow::datatypes::DataType::Float64,
        order_by: vec![],
        output_column_id: ColumnId::UNSET,
    }
}

#[test]
fn splits_grouped_avg_aggregate() {
    let mut memo = Memo::new();
    let child = values_group(&mut memo);
    let expr = MExpr {
        id: memo.next_expr_id(),
        op: Operator::LogicalAggregate(LogicalAggregateOp::single(
            vec![nullable_col_ref(1, "k", true)],
            vec![avg_call()],
            vec![output_column(1, "k"), output_column(3, "avg(v)")],
        )),
        children: vec![child],
    };
    let out = SplitAggregateRule.apply(&expr, &mut memo);
    assert_eq!(out.len(), 1, "avg must now produce a split alternative");
    let Operator::LogicalAggregate(global) = &out[0].op else {
        panic!("expected global aggregate");
    };
    assert_eq!(global.stage, AggStage::Global);
    assert_eq!(global.is_merge, vec![true]);
}
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `cargo test --lib splits_grouped_avg_aggregate 2>&1 | tail -15`
Expected: FAIL — `assertion failed: out.len() == 1` shows `0` (current whitelist rejects `avg`).

- [ ] **Step 3: Replace `is_splittable_aggregate` with the oracle delegate**

In `src/sql/optimizer/cascades_rules/split_aggregate.rs`, replace the function at lines 79-86:

```rust
fn is_splittable_aggregate(call: &AggregateCall) -> bool {
    !call.distinct
        && call.order_by.is_empty()
        && matches!(
            call.name.to_ascii_lowercase().as_str(),
            "sum" | "min" | "max" | "count"
        )
}
```

with:

```rust
fn is_splittable_aggregate(call: &AggregateCall) -> bool {
    use crate::sql::agg_mergeability::{aggregate_mergeability, AggMergeability};
    aggregate_mergeability(call) == AggMergeability::TwoPhase
}
```

- [ ] **Step 4: Run the new test and the existing rule tests to verify all pass**

Run: `cargo test --lib split_aggregate 2>&1 | tail -20`
Expected: all pass, including the pre-existing `rejects_distinct_and_already_split_aggregate`, `splits_grouped_aggregate_into_global_over_local`, and the new `splits_grouped_avg_aggregate`.

- [ ] **Step 5: Commit**

```bash
git add src/sql/optimizer/cascades_rules/split_aggregate.rs
git commit -m "feat(optimizer): SplitAggregateRule eligibility via capability oracle (enables avg)"
```

---

## Task 7: `avg` two-phase result + plan-shape golden

**Files:**
- Create: `sql-tests/optimizer/sql/split_aggregate_avg.sql`

This golden proves two things at once: (a) the plan splits (`@explain_contains`), and (b) the split result equals the single-pass result. The data is chosen so every group's `avg` is exact (no float-tolerance ambiguity).

- [ ] **Step 1: Write the golden SQL case**

Create `sql-tests/optimizer/sql/split_aggregate_avg.sql` (mirrors the structure of the existing `split_aggregate_grouped.sql`):

```sql
-- OQ-11: avg becomes two-phase via the capability oracle. Data is chosen so
-- each group's avg is exact (no floating-point tolerance needed).

CREATE TABLE ${case_db}.t_split_agg_avg (k INT, v INT);
INSERT INTO ${case_db}.t_split_agg_avg VALUES
    (1, 10), (1, 20), (1, 30),
    (2, 5),  (2, 15), (2, 25),
    (3, 100), (3, 200), (3, 300);
ANALYZE TABLE ${case_db}.t_split_agg_avg;

-- @explain_contains=HASH AGGREGATE (LOCAL
-- @explain_contains=HASH AGGREGATE (GLOBAL
SELECT k, AVG(v) AS a
FROM ${case_db}.t_split_agg_avg
GROUP BY k
ORDER BY k;
```

- [ ] **Step 2: Start a standalone server on the new binary (debug build is fine for correctness)**

Run:

```bash
source docker/iceberg-rest/runtime/current/env.sh
LOG=/tmp/novarocks-oq11.log
NO_PROXY=127.0.0.1,localhost cargo run -- standalone-server --port "$NOVA_ENV_MYSQL_PORT" >"$LOG" 2>&1 &
SRV_PID=$!
for i in $(seq 1 120); do
  grep -q '^NOVAROCKS_READY ' "$LOG" && break
  kill -0 "$SRV_PID" 2>/dev/null || { echo "server died"; tail -20 "$LOG"; exit 1; }
  sleep 1
done
grep -q '^NOVAROCKS_READY ' "$LOG" || { echo "timeout"; kill -9 "$SRV_PID"; exit 1; }
```

Expected: the loop ends with `NOVAROCKS_READY` present in the log.

- [ ] **Step 3: Record the golden**

Run:

```bash
cargo run --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests -- \
  --suite optimizer --only split_aggregate_avg --mode record
```

Expected: writes `sql-tests/optimizer/result/split_aggregate_avg.result`; exit code 0. (If `--only` matches by filename stem, this records just this case.)

- [ ] **Step 4: Verify the golden passes in verify mode**

Run:

```bash
cargo run --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests -- \
  --suite optimizer --only split_aggregate_avg --mode verify
```

Expected: `1 passed` (the `@explain_contains=HASH AGGREGATE (LOCAL` and `(GLOBAL` assertions hold, and the result rows match the recorded golden). Stop the server afterward: `kill "$SRV_PID"`.

- [ ] **Step 5: Commit**

```bash
git add sql-tests/optimizer/sql/split_aggregate_avg.sql sql-tests/optimizer/result/split_aggregate_avg.result
git commit -m "test(optimizer): golden for avg two-phase split (plan shape + result parity)"
```

---

## Task 8: Diagnose Bucket-2 (eligible-but-unsplit) queries → work list

**Files:**
- Create: `docs/superpowers/specs/2026-06-09-oq-11-bucket2-diagnosis.md`

This is an investigation task. Its deliverable is a classified work list (not code) that seeds the follow-up plans. Each query is checked with the split rule ON vs OFF to see whether NovaRocks emits a split today and, if not, why the alternative loses or is pruned.

- [ ] **Step 1: With the standalone server running (Task 7 Step 2), capture each Bucket-2 query's plan with split enabled vs disabled**

For each of `q7 q8 q9` (tpc-h) and `q54 q67 q75` (tpc-ds), run both:

```bash
# split enabled (default)
mysql -h 127.0.0.1 -P "$NOVA_ENV_MYSQL_PORT" -u root -D tpch \
  -e "EXPLAIN VERBOSE $(sed 's/;[[:space:]]*$//' sql-tests/tpc-h/sql/q7.sql);"
# split disabled
mysql -h 127.0.0.1 -P "$NOVA_ENV_MYSQL_PORT" -u root -D tpch \
  -e "SET disable_optimizer_rules='SplitAggregateRule'; EXPLAIN VERBOSE $(sed 's/;[[:space:]]*$//' sql-tests/tpc-h/sql/q7.sql);"
```

(Use `-D tpcds` and the `sql-tests/tpc-ds/sql/<q>.sql` path for the tpc-ds cases.)

- [ ] **Step 2: Classify each query and write the findings**

Create `docs/superpowers/specs/2026-06-09-oq-11-bucket2-diagnosis.md` with one row per query and a verdict drawn ONLY from the observed plans:

```markdown
# OQ-11 Bucket-2 Diagnosis (eligible-but-unsplit aggregates)

For each query: does NR split today? If not, which root-cause hypothesis from
spec §5.2 (H1 cost / H2 group-by ColumnId / H3 set-op pushdown / H4 rollup /
H5 window) does the evidence support?

| query | splits today? | evidence (plan delta enabled vs disabled) | root cause | follow-up plan |
|---|---|---|---|---|
| tpc-h/q7 | <yes/no> | <observed> | <H?> | <which plan> |
| tpc-h/q8 | ... | ... | ... | ... |
| tpc-h/q9 | ... | ... | ... | ... |
| tpc-ds/q54 | ... | ... | ... | ... |
| tpc-ds/q67 | ... | ... | ... | ... |
| tpc-ds/q75 | ... | ... | ... | ... |

## Conclusion
- Queries already split (no work needed): ...
- Cost-gate calibration (H1): ...
- ColumnId/validity fix (H2): ...
- Set-op pushdown mechanism (H3): ...
- Rollup/grouping-sets (H4): ...

Each non-empty bucket becomes a follow-up plan under docs/superpowers/plans/.
```

Fill every `<...>` from the captured plans. Leave no angle-bracket placeholder in the committed file.

- [ ] **Step 3: Commit**

```bash
git add docs/superpowers/specs/2026-06-09-oq-11-bucket2-diagnosis.md
git commit -m "docs(optimizer): OQ-11 Bucket-2 eligible-but-unsplit diagnosis and work list"
```

---

## Self-Review

- **Spec coverage (Part-1 portion of the spec):**
  - §4 Phase 0 harness → Tasks 1–2. ✓
  - §5.1 capability oracle (single source of truth + distinct/ordered/order-sensitive guards + drift guard) → Tasks 3–6. ✓ (Float/sketch families explicitly deferred in Scope, with rationale — not a gap.)
  - §6.2 result-correctness + §6.3 `split_aggregate_avg` golden → Task 7. ✓
  - §5.2 Bucket-2 diagnosis method (NR EXPLAIN, rule on/off) → Task 8. ✓
  - §5.3/§5.4/§6.4 (pushdown coordination, distribution/RF, FE convergence report) → deferred to follow-up plans (documented in Scope). ✓
- **Placeholder scan:** No "TBD"/"implement later" in code steps; every code step has full code. The only angle-bracket template is Task 8's diagnosis table, whose Step 2 explicitly requires filling all `<...>` before commit (it is a data-collection deliverable, not code). ✓
- **Type consistency:** `AggMergeability::{TwoPhase, SinglePhaseOnly}`, `aggregate_mergeability(&AggregateCall)`, `has_two_phase_merge(&str)`, `is_order_sensitive(&str)` used identically across Tasks 4–6. `infer_agg_function_types(name, &[DataType], bool) -> Result<(DataType, Option<DataType>), String>` matches the real signature at `expr_compiler.rs:2720` and is used consistently in Tasks 3 and 5. `AggregateCall` fields (`name/args/distinct/result_type/order_by/output_column_id`) match `plan.rs:297`. ✓
- **Guards on unknowns:** Task 4 Step 2 and Task 5 Step 2 explicitly verify `SortItem` shape and the import path against the real code before relying on them, instead of assuming. ✓
