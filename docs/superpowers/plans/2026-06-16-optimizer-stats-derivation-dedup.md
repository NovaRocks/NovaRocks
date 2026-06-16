# Optimizer Statistics-Derivation Dedup Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Eliminate two redundant statistics computations in the Cascades optimizer — the second full `derive_group_statistics` re-deriving already-computed groups (A1), and `own_stats` being re-derived per property-alternative in search (B') — with zero plan change.

**Architecture:** A1 adds a memoization guard to the bulk `derive_group_statistics` loop so a group's `logical_props` is computed exactly once (StarRocks `isStatsDerived` semantics). B' hoists the per-expr `own_stats` derivation out of the inner `for alt` loop in `optimize_group` (it is invariant across `alt`). Both are behavior-preserving; correctness is pinned by an A1 red-green unit test, a B' regression-guard unit test, and the `optimizer` golden plan suite remaining byte-identical.

**Tech Stack:** Rust, `src/sql/optimizer/` (standalone Cascades optimizer), inline `#[cfg(test)]` unit tests, `sql-tests` golden plan runner.

**Spec:** `docs/superpowers/specs/2026-06-16-optimizer-stats-derivation-dedup-design.md`

---

## File Structure

- Modify `src/sql/optimizer/stats.rs`
  - `derive_group_statistics` (around line 703-710): add the `is_some()` memoization guard.
  - `derive_group_statistics_for` (around line 712-718 doc-comment): add the append-only invariant note.
  - `#[cfg(test)] mod tests`: add A1 red-green test and B' regression-guard test.
- Modify `src/sql/optimizer/search.rs`
  - `optimize_group` (around line 126-168): hoist `own_stats` out of the `for alt` loop.

No new files. No production code outside these two files.

---

### Task 1: A1 — Memoize `derive_group_statistics` (skip already-computed groups)

**Files:**
- Modify: `src/sql/optimizer/stats.rs:703-750`
- Test: `src/sql/optimizer/stats.rs` (inside the existing `#[cfg(test)] mod tests`)

- [ ] **Step 1: Write the failing test**

Add this test inside `mod tests` in `src/sql/optimizer/stats.rs` (place it next to the other `derive_group_statistics` tests, e.g. after the test at ~line 1969):

```rust
#[test]
fn derive_group_statistics_skips_already_computed_groups() {
    use crate::sql::optimizer::memo::{LogicalProperties, MExpr, Memo};
    use crate::sql::optimizer::operator::{LogicalValuesOp, Operator};
    use std::collections::HashMap;

    let mut memo = Memo::new();

    // Group A: simulates a group computed by an earlier derive pass. The
    // sentinel row_count 999_999 is a value the real derivation would never
    // produce for an empty LogicalValues (which derives to 0).
    let group_a = memo.new_group(MExpr {
        id: memo.next_expr_id(),
        op: Operator::LogicalValues(LogicalValuesOp { rows: vec![], columns: vec![] }),
        children: vec![],
    });
    memo.groups[group_a].logical_props = Some(LogicalProperties::new(vec![], 999_999.0));

    // Group B: simulates a fresh group minted by implement() — logical_props=None.
    let group_b = memo.new_group(MExpr {
        id: memo.next_expr_id(),
        op: Operator::LogicalValues(LogicalValuesOp { rows: vec![], columns: vec![] }),
        children: vec![],
    });
    assert!(memo.groups[group_b].logical_props.is_none());

    derive_group_statistics(&mut memo, &HashMap::new());

    // Group A was memoized/skipped — sentinel preserved (NOT recomputed to 0).
    assert_eq!(
        memo.groups[group_a].logical_props.as_ref().unwrap().row_count,
        999_999.0,
        "already-computed group must be skipped, not recomputed"
    );
    // Group B (None) must still be derived.
    assert!(
        memo.groups[group_b].logical_props.is_some(),
        "fresh (None) group must still be derived"
    );
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test --lib derive_group_statistics_skips_already_computed_groups`
Expected: FAIL — `assert_eq!` on `row_count` fails because the current unguarded loop recomputes group A's stats, overwriting `999_999.0` with `0.0` (empty `LogicalValues`).

- [ ] **Step 3: Add the memoization guard + invariant note**

In `src/sql/optimizer/stats.rs`, change `derive_group_statistics` (lines 703-710) from:

```rust
pub(crate) fn derive_group_statistics(
    memo: &mut Memo,
    table_stats: &HashMap<String, TableStatistics>,
) {
    for group_idx in 0..memo.groups.len() {
        derive_group_statistics_for(memo, group_idx, table_stats);
    }
}
```

to:

```rust
pub(crate) fn derive_group_statistics(
    memo: &mut Memo,
    table_stats: &HashMap<String, TableStatistics>,
) {
    for group_idx in 0..memo.groups.len() {
        // Memoized derive: a group's logical_props are computed exactly once,
        // when first needed (StarRocks isStatsDerived semantics). Safe because
        // the memo is append-only — explore()/implement() only append new exprs
        // and never rewrite an existing group's logical_exprs.first() in place,
        // so re-deriving an already-computed group would reproduce the identical
        // value. INVARIANT: any future rule that mutates an existing group's
        // first logical expr in place MUST reset that group's logical_props to
        // None, or this skip will serve stale statistics.
        if memo.groups[group_idx].logical_props.is_some() {
            continue;
        }
        derive_group_statistics_for(memo, group_idx, table_stats);
    }
}
```

And extend the existing doc-comment on `derive_group_statistics_for` (line 712-718) by appending this sentence to it:

```rust
/// Callers rely on the append-only memo invariant: if a rule ever rewrites an
/// existing group's first expression in place, it must reset that group's
/// `logical_props` to `None` so a later derive recomputes it.
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cargo test --lib derive_group_statistics_skips_already_computed_groups`
Expected: PASS — group A keeps `999_999.0` (skipped), group B is `Some` (derived).

- [ ] **Step 5: Run the surrounding test module to confirm no regression**

Run: `cargo test --lib sql::optimizer::stats`
Expected: PASS — all existing `derive_group_statistics` tests still pass (they build fresh memos where every group is `None`, so the guard is a no-op for them).

- [ ] **Step 6: Commit**

```bash
git add src/sql/optimizer/stats.rs
git commit -m "perf(optimizer): memoize derive_group_statistics (skip already-computed groups)"
```

---

### Task 2: B' — Hoist `own_stats` out of the `for alt` loop in `optimize_group`

**Files:**
- Modify: `src/sql/optimizer/search.rs:126-168`
- Test: `src/sql/optimizer/stats.rs` (inside the existing `#[cfg(test)] mod tests`)

- [ ] **Step 1: Write the regression-guard test**

This test locks in *why* B' must keep `own_stats` per-expr (and must NOT be "fixed" later by reading the group cache): two `PhysicalHashAggregate` exprs with the same op over differently-sized children produce different `own_stats`, so one cached group value cannot represent both. It passes before and after B' (B' does not change values) — it is a characterization/regression guard, not a red-green driver.

Add inside `mod tests` in `src/sql/optimizer/stats.rs`:

```rust
#[test]
fn physical_hash_aggregate_own_stats_are_per_expr_not_per_group() {
    use crate::sql::analysis::{ExprKind, OutputColumn, TypedExpr};
    use crate::sql::column_id::ColumnId;
    use crate::sql::optimizer::memo::{LogicalProperties, MExpr, Memo};
    use crate::sql::optimizer::operator::{
        AggMode, LogicalValuesOp, Operator, PhysicalHashAggregateOp,
    };
    use crate::sql::optimizer::statistics::ColumnStatistic;
    use std::collections::HashMap;

    fn col_ref(id: u32, name: &str) -> TypedExpr {
        TypedExpr {
            kind: ExprKind::ColumnRef {
                column_id: ColumnId::new_for_test(id),
                qualifier: Some("t".to_string()),
                column: name.to_string(),
            },
            data_type: arrow::datatypes::DataType::Int64,
            nullable: false,
        }
    }
    fn output_column(id: u32, name: &str) -> OutputColumn {
        OutputColumn {
            column_id: ColumnId::new_for_test(id),
            name: name.to_string(),
            data_type: arrow::datatypes::DataType::Int64,
            nullable: false,
            is_internal: false,
        }
    }
    // A leaf group with the given row_count and a single group-by column (id=1) of NDV=100.
    fn child_with_rows(memo: &mut Memo, rows: f64) -> usize {
        let id = memo.next_expr_id();
        let g = memo.new_group(MExpr {
            id,
            op: Operator::LogicalValues(LogicalValuesOp { rows: vec![], columns: vec![] }),
            children: vec![],
        });
        let mut props = LogicalProperties::new(vec![output_column(1, "k")], rows);
        props.column_statistics.insert(
            ColumnId::new_for_test(1),
            ColumnStatistic {
                min_value: 0.0,
                max_value: rows,
                nulls_fraction: 0.0,
                average_row_size: 8.0,
                distinct_values_count: 100.0,
                ..Default::default()
            },
        );
        memo.groups[g].logical_props = Some(props);
        g
    }
    fn agg_over(child: usize, memo: &Memo) -> MExpr {
        MExpr {
            id: memo.next_expr_id(), // id is irrelevant to derive_statistics
            op: Operator::PhysicalHashAggregate(PhysicalHashAggregateOp {
                mode: AggMode::Single,
                group_by: vec![col_ref(1, "k")],
                aggregates: vec![],
                output_columns: vec![output_column(1, "k")],
                is_merge: vec![],
            }),
            children: vec![child],
        }
    }

    let mut memo = Memo::new();
    // big: 200 * 0.75 = 150 >= NDV 100  -> agg_group_rows = min(100, 150) = 100 (NDV-capped)
    let big = child_with_rows(&mut memo, 200.0);
    // small: 100 * 0.75 = 75 < NDV 100  -> agg_group_rows = min(100, 75) = 75 (row-capped)
    let small = child_with_rows(&mut memo, 100.0);

    let big_stats = derive_statistics(&agg_over(big, &memo), &memo, &HashMap::new());
    let small_stats = derive_statistics(&agg_over(small, &memo), &memo, &HashMap::new());

    // Same op, different children -> different own_stats. A single group cache
    // cannot represent both, which is exactly why search.rs keeps own_stats per-expr.
    assert!(
        big_stats.output_row_count > small_stats.output_row_count,
        "per-expr own_stats must differ: big={} small={}",
        big_stats.output_row_count,
        small_stats.output_row_count
    );
    assert_ne!(big_stats.output_row_count, small_stats.output_row_count);
}
```

- [ ] **Step 2: Run the regression-guard test to verify it passes (characterization)**

Run: `cargo test --lib physical_hash_aggregate_own_stats_are_per_expr_not_per_group`
Expected: PASS — `big` is NDV-capped (~100), `small` is row-capped (~75), so they differ. (If this fails to compile due to a helper signature, fix the construction to match the patterns already used in the neighboring aggregate tests in the same file; do not change the assertion intent.)

- [ ] **Step 3: Hoist `own_stats` out of the `for alt` loop**

In `src/sql/optimizer/search.rs`, the `optimize_group` loop currently derives `own_stats` deep inside the inner `for alt` loop at line 168:

```rust
        for expr_idx in 0..num_physical {
            let expr = &memo.groups[group_id].physical_exprs[expr_idx];

            let alternatives = super::derive::derive_required_alternatives(
                &expr.op,
                required,
                expr.children.len(),
            );
            // ...
            for alt in alternatives {
                // ...
                let own_stats = derive_statistics(expr, memo, &self.table_stats); // line 168
                let child_stats_vec: Vec<_> = expr
                    .children
                    .iter()
                    .map(|&cg| stats_for_group(&memo.groups[cg], memo, &self.table_stats))
                    .collect();
                // ... own_stats used at the compute_cost_with_properties call (~line 200)
            }
        }
```

Move the `own_stats` binding up to the top of the `for expr_idx` body — after `let expr = ...` (line 126) and before `derive_required_alternatives` (line 128). It depends only on `expr` and `memo`, both invariant across `alt`. Result:

```rust
        for expr_idx in 0..num_physical {
            let expr = &memo.groups[group_id].physical_exprs[expr_idx];

            // own_stats is the cardinality of THIS physical expr; it does not
            // depend on the property alternative, so derive it once per expr
            // instead of once per (expr, alt). Kept per-expr (not the group
            // cache) because same-group exprs can have different own_stats
            // (see physical_hash_aggregate_own_stats_are_per_expr_not_per_group).
            let own_stats = derive_statistics(expr, memo, &self.table_stats);

            let alternatives = super::derive::derive_required_alternatives(
                &expr.op,
                required,
                expr.children.len(),
            );
            // ...
            for alt in alternatives {
                // ...
                // (the `let own_stats = derive_statistics(...)` line is now removed from here)
                let child_stats_vec: Vec<_> = expr
                    .children
                    .iter()
                    .map(|&cg| stats_for_group(&memo.groups[cg], memo, &self.table_stats))
                    .collect();
                // ... own_stats still referenced at compute_cost_with_properties (~line 200)
            }
        }
```

Delete the original `let own_stats = derive_statistics(expr, memo, &self.table_stats);` line inside the `for alt` loop. Leave every other line (including the `&own_stats` argument at the `compute_cost_with_properties` call) unchanged.

- [ ] **Step 4: Run tests to verify no behavior change**

Run: `cargo test --lib sql::optimizer::search`
Expected: PASS — all `optimize_group` tests (cost/winner selection) are unchanged because `own_stats` has the identical value, just computed once per expr.

Run: `cargo test --lib physical_hash_aggregate_own_stats_are_per_expr_not_per_group`
Expected: PASS — still green (B' did not touch `derive_statistics`).

- [ ] **Step 5: Commit**

```bash
git add src/sql/optimizer/search.rs src/sql/optimizer/stats.rs
git commit -m "perf(optimizer): hoist per-expr own_stats out of optimize_group alt loop"
```

---

### Task 3: Regression verification — golden plans byte-identical + full lib suite

**Files:** none (verification only)

- [ ] **Step 1: Build (dev profile, fast)**

Run: `cargo build`
Expected: builds clean, no warnings introduced by the two changed files.

- [ ] **Step 2: Run the full optimizer unit suite**

Run: `cargo test --lib sql::optimizer`
Expected: PASS — includes the two new tests plus all existing optimizer unit + plan tests.

- [ ] **Step 3: Run the `optimizer` golden plan SQL suite (plan must be byte-identical)**

Start a standalone server (per CLAUDE.md; use the generated env when present):

```bash
# If the local test env exists:
source docker/iceberg-rest/runtime/current/env.sh 2>/dev/null || true
LOG=/tmp/novarocks-server.log
NO_PROXY=127.0.0.1,localhost target/debug/novarocks standalone-server \
  ${NOVAROCKS_STANDALONE_CONFIG:+--config "$NOVAROCKS_STANDALONE_CONFIG"} \
  ${NOVAROCKS_STANDALONE_CONFIG:---port 9030} >"$LOG" 2>&1 &
SRV_PID=$!
for i in $(seq 1 60); do
  grep -q '^NOVAROCKS_READY ' "$LOG" && break
  kill -0 "$SRV_PID" 2>/dev/null || { echo "server died:"; tail -20 "$LOG"; exit 1; }
  sleep 1
done
grep -q '^NOVAROCKS_READY ' "$LOG" || { echo "timeout"; kill -9 "$SRV_PID"; exit 1; }
```

Then run the optimizer plan-golden suite in verify mode:

```bash
cargo run --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests -- \
  ${NOVAROCKS_SQL_TEST_CONFIG:+--config "$NOVAROCKS_SQL_TEST_CONFIG"} \
  --suite optimizer --mode verify
```

Expected: all cases PASS with no plan diff. A1 + B' are behavior-preserving, so every EXPLAIN/plan golden must be byte-identical. Stop the server when done: `kill "$SRV_PID"`.

- [ ] **Step 4: (Optional) Quantify the win with a throwaway counter**

To confirm the dedup reduced work (not required to merge), temporarily add a `thread_local!` counter incremented at the top of `derive_group_statistics_for` and `derive_statistics`, log it after `optimize()` on a wide-table large join (e.g. a `LEFT SEMI` over a 33-column, >1M-row table — the case that historically exhausted the optimizer budget), compare before/after this branch, then **remove the counter** before merging (no measurement code is committed).

- [ ] **Step 5: Final confirmation**

Run: `cargo test --lib sql::optimizer && echo "ALL GREEN"`
Expected: prints `ALL GREEN`. No further commit unless Step 4 left changes (it must not).

---

## Notes for the implementer

- **Why no config flag:** A1 and B' have no behavior fork (only faster), so there is nothing to toggle. Each task is a standalone commit; revert the single commit to roll back.
- **A1 safety rests on the append-only invariant.** Verified across `explore`/`implement`/`run_multi_join_reorder` and all rules: new exprs are only ever appended (`add_expr_to_group`), groups are only ever created with `logical_props=None` (`new_group`), and no rule rewrites an existing group's first expr in place or overwrites an existing group's `logical_props`. The guard comment + the `derive_group_statistics_for` doc note pin this for future changes.
- **Do NOT** replace `search.rs:168` `own_stats` with `stats_for_group(group_id)`. That is a behavior change (it would canonicalize Global-agg own_stats from 75 to the cached 100, changing cost and possibly the plan) and is explicitly out of scope — see the spec §2.2 and §4.
