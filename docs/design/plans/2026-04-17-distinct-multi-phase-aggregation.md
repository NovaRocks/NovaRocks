# DISTINCT Multi-Phase Aggregation Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Emit StarRocks-style multi-phase aggregation alternatives for queries containing a single DISTINCT column — 3-phase chain (`LOCAL → DISTINCT_GLOBAL → GLOBAL`) when there is a GROUP BY, 4-phase chain (`LOCAL → DISTINCT_GLOBAL → DISTINCT_LOCAL → GLOBAL`) for scalar DISTINCT — so that cost search can pick distributed execution over today's SINGLE-phase fallback.

**Architecture:** Extend `AggMode` with two new variants `DistinctLocal` and `DistinctGlobal`. Add per-call `is_merge: Vec<bool>` to `PhysicalHashAggregateOp` so the fragment builder can mix update-style and merge-style aggregate compilation inside a single phase (needed for `DISTINCT_LOCAL` which has `count(x)` update + non-DISTINCT merge, and for DISTINCT-driven `GLOBAL` which is similar). A new implementation rule `SplitDistinctAgg` matches `LogicalAggregate` with any DISTINCT call on a single column and emits one alternative (the top of the 3- or 4-phase chain). Distribution contracts for the new modes added in `search.rs`. Cost search picks between `SINGLE` (AggToHashAgg) and multi-phase (SplitDistinctAgg).

**Tech Stack:** Rust, Cascades optimizer, Thrift plan nodes, StarRocks execution layer.

**Spec:** `docs/design/specs/2026-04-17-distinct-multi-phase-aggregation-design.md`

---

## File Map

| File | Action | Responsibility |
|------|--------|----------------|
| `src/sql/optimizer/operator.rs` | Modify | Add `DistinctLocal`, `DistinctGlobal` to `AggMode`; add `is_merge: Vec<bool>` to `PhysicalHashAggregateOp` |
| `src/sql/optimizer/rules/implement.rs` | Modify | `AggToHashAgg` populates `is_merge` for Single / Local / Global (false / false / true); update tests |
| `src/sql/codegen/fragment_builder.rs` | Modify | Replace single `is_global` flag in `visit_hash_aggregate` with per-call `op.is_merge[idx]`; extend `need_finalize` to five-mode dispatch |
| `src/sql/optimizer/rules/split_distinct_agg.rs` | Create | New implementation rule `SplitDistinctAgg`; emits 3- or 4-phase chain |
| `src/sql/optimizer/rules/mod.rs` | Modify | Declare `split_distinct_agg` module; register rule in `all_implementation_rules()` |
| `src/sql/optimizer/search.rs` | Modify | Distribution contracts for `DistinctLocal` + `DistinctGlobal` in `output_properties` and `required_input_properties` |
| `sql-tests/aggregate/distinct_group_by_multi_phase.sql` + `.result` | Create | Hand-crafted 3-phase coverage (TPC-DS has no `GROUP BY + count(distinct)` query) |

---

### Task 1: Extend `AggMode` enum

**Files:**
- Modify: `src/sql/optimizer/operator.rs`

- [ ] **Step 1: Add two new variants**

In `src/sql/optimizer/operator.rs` the current enum is at line 25-30:

```rust
#[allow(dead_code)]
#[derive(Clone, Debug)]
pub(crate) enum AggMode {
    Single,
    Local,
    Global,
}
```

Replace with:

```rust
#[allow(dead_code)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum AggMode {
    Single,
    Local,
    Global,
    /// Dedup by distinct-column + merge non-DISTINCT aggregate states across
    /// instances. Used as the shuffle-receive phase of 3- and 4-phase DISTINCT
    /// aggregation.
    DistinctGlobal,
    /// Per-instance scalar rollup of DISTINCT_GLOBAL output — emits
    /// `count(x)` (update) for each DISTINCT call and merges threaded
    /// non-DISTINCT states. Only used in 4-phase (scalar DISTINCT).
    DistinctLocal,
}
```

The added `Copy, PartialEq, Eq` derives match the `TopNPhase` enum pattern and are needed because later code dispatches via `match op.mode { … }` and the operator carries `mode` by value into `matches!`.

- [ ] **Step 2: Verify build**

Run: `cargo build 2>&1 | tail -10`
Expected: no errors. Each `match AggMode { Single => …, Local => …, Global => … }` site must still cover the exhaustive set. Since the old enum had 3 variants and the new has 5, Rust's exhaustiveness check will flag every unadorned match. Fix each flagged site by adding arms — for this task, route new variants to a `panic!("not implemented in task 1, added in task 2")` stub so nothing silently misbehaves:

Anticipated sites (verify with `cargo build` error list, then patch each):

1. `src/sql/codegen/fragment_builder.rs:837`: `let is_global = matches!(op.mode, AggMode::Global);` — `matches!` does not require exhaustiveness; leaves `DistinctGlobal`/`DistinctLocal` as `false`. **No code change needed here until Task 2 rewrites the body.**
2. `src/sql/codegen/fragment_builder.rs:885`: `let need_finalize = !matches!(op.mode, AggMode::Local);` — same; `DistinctGlobal` and `DistinctLocal` would evaluate to `true` here, which is wrong (both are intermediate phases). **Patched in Task 2.**
3. `src/sql/optimizer/search.rs:475-494`: `match a.mode { Single => …, Local => …, Global => … }` — this WILL fail to compile (non-exhaustive). Add temporary arms:

```rust
        Operator::PhysicalHashAggregate(a) => match a.mode {
            AggMode::Single => {
                if a.group_by.is_empty() {
                    vec![PhysicalPropertySet::gather()]
                } else {
                    vec![PhysicalPropertySet::any()]
                }
            }
            AggMode::Local => vec![PhysicalPropertySet::any()],
            AggMode::Global => {
                let cols = typed_exprs_to_column_refs(&a.group_by);
                if cols.is_empty() {
                    vec![PhysicalPropertySet::gather()]
                } else {
                    vec![PhysicalPropertySet {
                        distribution: DistributionSpec::HashPartitioned(cols),
                        ordering: OrderingSpec::Any,
                    }]
                }
            }
            AggMode::DistinctGlobal | AggMode::DistinctLocal => {
                // Contracts added in Task 6.
                vec![PhysicalPropertySet::any()]
            }
        },
```

No other exhaustive matches are expected, but run `cargo build` and patch whatever else the compiler flags.

- [ ] **Step 3: Run tests**

Run: `cargo test --lib 2>&1 | tail -5`
Expected: 957 pass / 0 fail (unchanged — no query constructs the new variants yet).

- [ ] **Step 4: Commit**

```bash
git add src/sql/optimizer/operator.rs src/sql/optimizer/search.rs
git commit -m "refactor: extend AggMode with DistinctLocal and DistinctGlobal variants"
```

---

### Task 2: Add per-call `is_merge` and rewire fragment builder

**Files:**
- Modify: `src/sql/optimizer/operator.rs`
- Modify: `src/sql/optimizer/rules/implement.rs` (`AggToHashAgg` + tests)
- Modify: `src/sql/codegen/fragment_builder.rs` (`visit_hash_aggregate`)

- [ ] **Step 1: Add `is_merge` field to `PhysicalHashAggregateOp`**

Find the struct (operator.rs line 204-210):

```rust
#[derive(Clone, Debug)]
pub(crate) struct PhysicalHashAggregateOp {
    pub mode: AggMode,
    pub group_by: Vec<TypedExpr>,
    pub aggregates: Vec<AggregateCall>,
    pub output_columns: Vec<OutputColumn>,
}
```

Replace with:

```rust
#[derive(Clone, Debug)]
pub(crate) struct PhysicalHashAggregateOp {
    pub mode: AggMode,
    pub group_by: Vec<TypedExpr>,
    pub aggregates: Vec<AggregateCall>,
    pub output_columns: Vec<OutputColumn>,
    /// Per-aggregate merge flag. `true` → this phase applies the aggregate's
    /// merge function over an intermediate state slot from the child; `false`
    /// → this phase applies the update function over raw args from the child
    /// scope. Length must equal `aggregates.len()`.
    pub is_merge: Vec<bool>,
}
```

- [ ] **Step 2: Update `AggToHashAgg` to populate `is_merge`**

In `src/sql/optimizer/rules/implement.rs` around lines 648-690, each of the three `PhysicalHashAggregateOp { … }` literals gets an `is_merge` field. The rule knows exactly what's expected:

- Single: all update → `vec![false; op.aggregates.len()]`
- Local: all update → `vec![false; op.aggregates.len()]`
- Global: all merge → `vec![true; op.aggregates.len()]`

Replace the three `PhysicalHashAggregateOp { … }` literals inside `AggToHashAgg::apply`:

```rust
        let single = NewExpr {
            op: Operator::PhysicalHashAggregate(PhysicalHashAggregateOp {
                mode: AggMode::Single,
                group_by: op.group_by.clone(),
                aggregates: op.aggregates.clone(),
                output_columns: op.output_columns.clone(),
                is_merge: vec![false; op.aggregates.len()],
            }),
            children: expr.children.clone(),
        };

        let has_distinct = op.aggregates.iter().any(|a| a.distinct);
        if has_distinct || op.group_by.is_empty() {
            return vec![single];
        }

        let local_mexpr = MExpr {
            id: memo.next_expr_id(),
            op: Operator::PhysicalHashAggregate(PhysicalHashAggregateOp {
                mode: AggMode::Local,
                group_by: op.group_by.clone(),
                aggregates: op.aggregates.clone(),
                output_columns: op.output_columns.clone(),
                is_merge: vec![false; op.aggregates.len()],
            }),
            children: expr.children.clone(),
        };
        let local_group_id = memo.new_group(local_mexpr);

        let global = NewExpr {
            op: Operator::PhysicalHashAggregate(PhysicalHashAggregateOp {
                mode: AggMode::Global,
                group_by: op.group_by.clone(),
                aggregates: op.aggregates.clone(),
                output_columns: op.output_columns.clone(),
                is_merge: vec![true; op.aggregates.len()],
            }),
            children: vec![local_group_id],
        };

        vec![single, global]
```

- [ ] **Step 3: Update the three tests in `implement.rs` that assert on `PhysicalHashAggregateOp`**

Around `implement.rs:1650-1700`, the tests construct `PhysicalHashAggregateOp { mode, group_by, aggregates, output_columns }`. Each needs the new field. Add `is_merge: vec![]` or `is_merge: vec![false; N]`/`vec![true; N]` consistent with the mode under test.

Example pattern — for any test literal shaped:

```rust
Operator::PhysicalHashAggregate(PhysicalHashAggregateOp {
    mode: AggMode::Single,
    group_by: …,
    aggregates: …,
    output_columns: …,
})
```

add the missing field. When the aggregates vector has length N, use `vec![false; N]` for Single/Local and `vec![true; N]` for Global. For convenience, a helper already-existing test may use `aggregates.iter().map(|_| false).collect()`.

- [ ] **Step 4: Rewire `visit_hash_aggregate` to use per-call `is_merge`**

In `src/sql/codegen/fragment_builder.rs`, find `visit_hash_aggregate` (line 790-907). The current code uses a single boolean `is_global` that gates all calls:

```rust
        let is_global = matches!(op.mode, AggMode::Global);

        for (idx, agg_call) in op.aggregates.iter().enumerate() {
            let texpr = if is_global {
                // merge path
                let child_columns: Vec<_> = child.scope.iter_columns().collect();
                let child_col_idx = agg_start_col + idx;
                let (_, binding) = child_columns.get(child_col_idx).ok_or_else(|| {
                    format!(
                        "Global agg: child scope missing intermediate column at index {}",
                        child_col_idx
                    )
                })?;
                let mut compiler = ExprCompiler::new(&child.scope);
                compiler.compile_merge_aggregate_call(
                    agg_call,
                    binding.slot_id,
                    binding.tuple_id,
                    &binding.data_type,
                )?
            } else {
                let mut compiler = ExprCompiler::new(&child.scope);
                compiler.compile_aggregate_call_typed(agg_call)?
            };
            // …
        }
```

Replace the `is_global` binding with a consistency check, and dispatch per call:

```rust
        if op.is_merge.len() != op.aggregates.len() {
            return Err(format!(
                "PhysicalHashAggregate: is_merge length {} != aggregates length {}",
                op.is_merge.len(),
                op.aggregates.len()
            ));
        }

        for (idx, agg_call) in op.aggregates.iter().enumerate() {
            let texpr = if op.is_merge[idx] {
                let child_columns: Vec<_> = child.scope.iter_columns().collect();
                let child_col_idx = agg_start_col + idx;
                let (_, binding) = child_columns.get(child_col_idx).ok_or_else(|| {
                    format!(
                        "Merge agg: child scope missing intermediate column at index {}",
                        child_col_idx
                    )
                })?;
                let mut compiler = ExprCompiler::new(&child.scope);
                compiler.compile_merge_aggregate_call(
                    agg_call,
                    binding.slot_id,
                    binding.tuple_id,
                    &binding.data_type,
                )?
            } else {
                let mut compiler = ExprCompiler::new(&child.scope);
                compiler.compile_aggregate_call_typed(agg_call)?
            };
            // remainder unchanged — slot alloc, scope bind, push to aggregate_functions
```

- [ ] **Step 5: Update `need_finalize` to cover five modes**

Still in `visit_hash_aggregate`, the current line 885 is:

```rust
        let need_finalize = !matches!(op.mode, AggMode::Local);
```

This flagged `Global` as finalized and `Local` as not. For DistinctGlobal and DistinctLocal (both intermediate phases), `need_finalize` must be false. Replace with:

```rust
        let need_finalize = matches!(op.mode, AggMode::Single | AggMode::Global);
```

- [ ] **Step 6: Verify build**

Run: `cargo build 2>&1 | tail -10`
Expected: no errors.

- [ ] **Step 7: Run lib tests**

Run: `cargo test --lib 2>&1 | tail -5`
Expected: 957 pass / 0 fail. The refactor is behavior-preserving for existing queries (Single/Local/Global map to the same compilation they did before).

- [ ] **Step 8: Run TPC-DS suite**

The backend has to confirm no semantic regression. Ensure standalone-server + minio are running on 9030 / 9000 (restart if needed; see Task 4 in the TopN plan for the exact incantation). Then:

```bash
pkill -f "novarocks standalone-server" || true
sleep 2
cargo build --release 2>&1 | tail -3
NO_PROXY=127.0.0.1,localhost ./target/release/novarocks standalone-server --port 9030 &
sleep 8
NO_PROXY=127.0.0.1,localhost mysql -h 127.0.0.1 -P 9030 -u root -e "SELECT 1"

cargo run --release --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests -- \
    --suite tpc-ds --mode verify --query-timeout 120 -j 1 2>&1 | tail -5
```

Expected: **99/99 pass**.

- [ ] **Step 9: Commit**

```bash
git add src/sql/optimizer/operator.rs src/sql/optimizer/rules/implement.rs src/sql/codegen/fragment_builder.rs
git commit -m "refactor: per-call is_merge flag for PhysicalHashAggregate"
```

---

### Task 3: `SplitDistinctAgg` skeleton and `matches`-filter tests

**Files:**
- Create: `src/sql/optimizer/rules/split_distinct_agg.rs`
- Modify: `src/sql/optimizer/rules/mod.rs` (add `pub(crate) mod split_distinct_agg;` but NOT registered yet)

- [ ] **Step 1: Create the rule file with `matches` implementation and unit tests**

Create `src/sql/optimizer/rules/split_distinct_agg.rs`:

```rust
//! Implementation rule: multi-phase DISTINCT aggregation.
//!
//! Matches a `LogicalAggregate` with at least one DISTINCT aggregate call,
//! where all DISTINCT calls share a single simple column as their argument.
//! Emits one alternative physical chain:
//!   - 3-phase (LOCAL → DISTINCT_GLOBAL → GLOBAL) when `group_by` is non-empty.
//!   - 4-phase (LOCAL → DISTINCT_GLOBAL → DISTINCT_LOCAL → GLOBAL) when scalar.
//!
//! Mirrors StarRocks's `SplitAggregateRule` / `AggType.java` convention.

use crate::sql::ir::{ExprKind, TypedExpr};
use crate::sql::optimizer::memo::{MExpr, Memo};
use crate::sql::optimizer::operator::{AggMode, Operator, PhysicalHashAggregateOp};
use crate::sql::optimizer::rule::{NewExpr, Rule, RuleType};
use crate::sql::plan::AggregateCall;

pub(crate) struct SplitDistinctAgg;

impl Rule for SplitDistinctAgg {
    fn name(&self) -> &str {
        "SplitDistinctAgg"
    }

    fn rule_type(&self) -> RuleType {
        RuleType::Implementation
    }

    fn matches(&self, op: &Operator) -> bool {
        matches!(op, Operator::LogicalAggregate(a) if a.aggregates.iter().any(|c| c.distinct))
    }

    fn apply(&self, expr: &MExpr, memo: &mut Memo) -> Vec<NewExpr> {
        let Operator::LogicalAggregate(agg) = &expr.op else {
            return vec![];
        };

        // Validate single-DISTINCT-column precondition.
        let distinct_col = match extract_single_distinct_col(&agg.aggregates) {
            Some(c) => c,
            None => return vec![], // multi-column DISTINCT, or multiple different DISTINCT cols
        };

        // Partition aggregates into DISTINCT-bearing (which are deduped away at LOCAL)
        // and non-DISTINCT (which flow as merge states through the phases).
        let non_distinct: Vec<AggregateCall> = agg
            .aggregates
            .iter()
            .filter(|c| !c.distinct)
            .cloned()
            .collect();

        if agg.group_by.is_empty() {
            apply_four_phase(expr, memo, agg, &distinct_col, &non_distinct)
        } else {
            apply_three_phase(expr, memo, agg, &distinct_col, &non_distinct)
        }
    }
}

/// Return the shared DISTINCT column if every DISTINCT aggregate takes exactly
/// one argument and all such arguments are the same simple `ColumnRef`.
/// Returns `None` for:
///   - no DISTINCT calls at all (shouldn't happen — `matches` filters this)
///   - multi-arg DISTINCT (`count(distinct a, b)`)
///   - multiple distinct columns (`count(distinct a), count(distinct b)`)
///   - DISTINCT arg that is not a plain ColumnRef
fn extract_single_distinct_col(calls: &[AggregateCall]) -> Option<TypedExpr> {
    let mut distinct_calls = calls.iter().filter(|c| c.distinct);
    let first = distinct_calls.next()?;
    if first.args.len() != 1 {
        return None;
    }
    if !matches!(first.args[0].kind, ExprKind::ColumnRef { .. }) {
        return None;
    }
    for c in distinct_calls {
        if c.args.len() != 1 {
            return None;
        }
        if !typed_exprs_structurally_equal(&c.args[0], &first.args[0]) {
            return None;
        }
    }
    Some(first.args[0].clone())
}

fn typed_exprs_structurally_equal(a: &TypedExpr, b: &TypedExpr) -> bool {
    match (&a.kind, &b.kind) {
        (
            ExprKind::ColumnRef {
                qualifier: qa,
                column: ca,
            },
            ExprKind::ColumnRef {
                qualifier: qb,
                column: cb,
            },
        ) => qa == qb && ca == cb,
        _ => false,
    }
}

fn apply_three_phase(
    _expr: &MExpr,
    _memo: &mut Memo,
    _agg: &crate::sql::optimizer::operator::LogicalAggregateOp,
    _distinct_col: &TypedExpr,
    _non_distinct: &[AggregateCall],
) -> Vec<NewExpr> {
    // Implemented in Task 4.
    vec![]
}

fn apply_four_phase(
    _expr: &MExpr,
    _memo: &mut Memo,
    _agg: &crate::sql::optimizer::operator::LogicalAggregateOp,
    _distinct_col: &TypedExpr,
    _non_distinct: &[AggregateCall],
) -> Vec<NewExpr> {
    // Implemented in Task 5.
    vec![]
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sql::ir::{ExprKind, OutputColumn, TypedExpr};
    use crate::sql::optimizer::memo::Memo;
    use crate::sql::optimizer::operator::{LogicalAggregateOp, LogicalScanOp};
    use arrow::datatypes::DataType;

    fn col(name: &str) -> TypedExpr {
        TypedExpr {
            kind: ExprKind::ColumnRef {
                qualifier: None,
                column: name.into(),
            },
            data_type: DataType::Int64,
            nullable: false,
        }
    }

    fn scan_group(memo: &mut Memo) -> usize {
        memo.new_group(MExpr {
            id: memo.next_expr_id(),
            op: Operator::LogicalScan(LogicalScanOp {
                database: "db".into(),
                table: crate::sql::catalog::TableDef {
                    name: "t".into(),
                    columns: vec![],
                    storage: crate::sql::catalog::TableStorage::LocalParquetFile {
                        path: std::path::PathBuf::from("/tmp/t.parquet"),
                    },
                },
                alias: None,
                columns: vec![],
                predicates: vec![],
                required_columns: None,
            }),
            children: vec![],
        })
    }

    fn count_distinct(arg_name: &str) -> AggregateCall {
        AggregateCall {
            name: "count".into(),
            args: vec![col(arg_name)],
            distinct: true,
            result_type: DataType::Int64,
            order_by: vec![],
        }
    }

    fn sum_non_distinct(arg_name: &str) -> AggregateCall {
        AggregateCall {
            name: "sum".into(),
            args: vec![col(arg_name)],
            distinct: false,
            result_type: DataType::Int64,
            order_by: vec![],
        }
    }

    #[test]
    fn matches_when_any_distinct() {
        let op = Operator::LogicalAggregate(LogicalAggregateOp {
            group_by: vec![],
            aggregates: vec![count_distinct("x"), sum_non_distinct("a")],
            output_columns: vec![],
        });
        assert!(SplitDistinctAgg.matches(&op));
    }

    #[test]
    fn does_not_match_when_no_distinct() {
        let op = Operator::LogicalAggregate(LogicalAggregateOp {
            group_by: vec![],
            aggregates: vec![sum_non_distinct("a")],
            output_columns: vec![],
        });
        assert!(!SplitDistinctAgg.matches(&op));
    }

    #[test]
    fn apply_skips_multi_arg_distinct() {
        let mut memo = Memo::new();
        let sg = scan_group(&mut memo);
        let two_arg = AggregateCall {
            name: "count".into(),
            args: vec![col("a"), col("b")],
            distinct: true,
            result_type: DataType::Int64,
            order_by: vec![],
        };
        let mexpr = MExpr {
            id: memo.next_expr_id(),
            op: Operator::LogicalAggregate(LogicalAggregateOp {
                group_by: vec![],
                aggregates: vec![two_arg],
                output_columns: vec![],
            }),
            children: vec![sg],
        };
        assert!(SplitDistinctAgg.apply(&mexpr, &mut memo).is_empty());
    }

    #[test]
    fn apply_skips_distinct_on_different_cols() {
        let mut memo = Memo::new();
        let sg = scan_group(&mut memo);
        let mexpr = MExpr {
            id: memo.next_expr_id(),
            op: Operator::LogicalAggregate(LogicalAggregateOp {
                group_by: vec![],
                aggregates: vec![count_distinct("a"), count_distinct("b")],
                output_columns: vec![],
            }),
            children: vec![sg],
        };
        assert!(SplitDistinctAgg.apply(&mexpr, &mut memo).is_empty());
    }

    #[test]
    fn apply_accepts_multiple_distinct_on_same_col() {
        let mut memo = Memo::new();
        let sg = scan_group(&mut memo);
        let sum_distinct_x = AggregateCall {
            name: "sum".into(),
            args: vec![col("x")],
            distinct: true,
            result_type: DataType::Int64,
            order_by: vec![],
        };
        let mexpr = MExpr {
            id: memo.next_expr_id(),
            op: Operator::LogicalAggregate(LogicalAggregateOp {
                group_by: vec![],
                aggregates: vec![count_distinct("x"), sum_distinct_x],
                output_columns: vec![],
            }),
            children: vec![sg],
        };
        // Skeleton apply returns empty (Tasks 4/5 fill in), but extraction logic
        // must have accepted the input — i.e., apply_four_phase was entered.
        // Verify indirectly: extract_single_distinct_col returns Some("x").
        let logop = match &mexpr.op {
            Operator::LogicalAggregate(a) => a,
            _ => unreachable!(),
        };
        let col = extract_single_distinct_col(&logop.aggregates);
        assert!(col.is_some());
        assert!(matches!(
            col.unwrap().kind,
            ExprKind::ColumnRef { ref column, .. } if column == "x"
        ));
    }
}
```

- [ ] **Step 2: Declare the module in `rules/mod.rs`**

Add near the top (after `pub(crate) mod split_top_n;`):

```rust
pub(crate) mod split_distinct_agg;
```

**Do NOT register the rule yet** — the `all_implementation_rules()` call stays unchanged in this task. We register in Task 7, after the full chain is implementable.

- [ ] **Step 3: Run tests**

Run: `cargo test --lib split_distinct_agg 2>&1 | tail -15`
Expected: 5 tests pass.

Run: `cargo test --lib 2>&1 | tail -5`
Expected: 962 pass / 0 fail (957 + 5 new).

- [ ] **Step 4: Commit**

```bash
git add src/sql/optimizer/rules/split_distinct_agg.rs src/sql/optimizer/rules/mod.rs
git commit -m "feat: SplitDistinctAgg skeleton with matches filter + tests"
```

---

### Task 4: 3-phase chain construction

**Files:**
- Modify: `src/sql/optimizer/rules/split_distinct_agg.rs` (`apply_three_phase`)

- [ ] **Step 1: Implement `apply_three_phase`**

Replace the stub `apply_three_phase` body with:

```rust
fn apply_three_phase(
    expr: &MExpr,
    memo: &mut Memo,
    agg: &crate::sql::optimizer::operator::LogicalAggregateOp,
    distinct_col: &TypedExpr,
    non_distinct: &[AggregateCall],
) -> Vec<NewExpr> {
    // Group-by for LOCAL and DISTINCT_GLOBAL: original group_by + distinct_col.
    let mut gb_with_distinct = agg.group_by.clone();
    gb_with_distinct.push(distinct_col.clone());

    // LOCAL: group_by = g + x; non_distinct aggs computed with update semantics.
    let local = MExpr {
        id: memo.next_expr_id(),
        op: Operator::PhysicalHashAggregate(PhysicalHashAggregateOp {
            mode: AggMode::Local,
            group_by: gb_with_distinct.clone(),
            aggregates: non_distinct.to_vec(),
            output_columns: vec![],
            is_merge: vec![false; non_distinct.len()],
        }),
        children: expr.children.clone(),
    };
    let local_group = memo.new_group(local);

    // DISTINCT_GLOBAL: same group_by; merge non_distinct states.
    let dg = MExpr {
        id: memo.next_expr_id(),
        op: Operator::PhysicalHashAggregate(PhysicalHashAggregateOp {
            mode: AggMode::DistinctGlobal,
            group_by: gb_with_distinct,
            aggregates: non_distinct.to_vec(),
            output_columns: vec![],
            is_merge: vec![true; non_distinct.len()],
        }),
        children: vec![local_group],
    };
    let dg_group = memo.new_group(dg);

    // GLOBAL: group_by = original g; aggregates = [count(x) update, then each
    // non_distinct merged]. is_merge = [false, true, true, ...].
    let count_x = AggregateCall {
        name: "count".into(),
        args: vec![distinct_col.clone()],
        distinct: false,
        result_type: arrow::datatypes::DataType::Int64,
        order_by: vec![],
    };
    let mut global_aggs = Vec::with_capacity(1 + non_distinct.len());
    global_aggs.push(count_x);
    global_aggs.extend(non_distinct.iter().cloned());
    let mut global_merge = Vec::with_capacity(1 + non_distinct.len());
    global_merge.push(false); // count(x) is an update in the GLOBAL phase
    global_merge.extend(std::iter::repeat(true).take(non_distinct.len()));

    vec![NewExpr {
        op: Operator::PhysicalHashAggregate(PhysicalHashAggregateOp {
            mode: AggMode::Global,
            group_by: agg.group_by.clone(),
            aggregates: global_aggs,
            output_columns: agg.output_columns.clone(),
            is_merge: global_merge,
        }),
        children: vec![dg_group],
    }]
}
```

Note: `count(x)` at GLOBAL is `is_merge = false`. The argument resolves by name (`x`) from DISTINCT_GLOBAL's output scope, where `x` is a grouping key (preserved as a named column). This works because §1 of the spec restricts DISTINCT args to a single simple `ColumnRef`.

Also note: the GLOBAL's `aggregates` ordering `[count(x), ...non_distinct]` corresponds to a specific positional mapping expected by `compile_merge_aggregate_call`. DISTINCT_GLOBAL's child scope order is `[g…, x, non_distinct_state_0, non_distinct_state_1, …]`. At GLOBAL (which consumes DISTINCT_GLOBAL's output), grouping on `g` means the residual column positions in the child scope are `[x, non_distinct_state_0, …]`. The positional index for the ith non_distinct merge at GLOBAL is `agg_start_col + i` where `agg_start_col = group_by.len() = g.len()`. In the child scope (DISTINCT_GLOBAL output) that position maps to `[g.len() + 0] = x`, `[g.len() + 1] = non_distinct_state_0`, …. So:

- Call 0 (`count(x)`, is_merge=false) → uses args; resolves by name `x`. ✓
- Call 1+i (`non_distinct[i]`, is_merge=true) → compile_merge_aggregate_call uses positional SlotRef to child_scope[agg_start_col + 1 + i] = child_scope[g.len() + 1 + i] = non_distinct_state_i. ✓

This is the trick that makes positional merge compilation work without knowing phase-internal names.

- [ ] **Step 2: Add integration test for 3-phase chain structure**

Append to the `tests` module in `split_distinct_agg.rs`:

```rust
    #[test]
    fn three_phase_chain_with_group_by() {
        let mut memo = Memo::new();
        let sg = scan_group(&mut memo);
        let mexpr = MExpr {
            id: memo.next_expr_id(),
            op: Operator::LogicalAggregate(LogicalAggregateOp {
                group_by: vec![col("g")],
                aggregates: vec![count_distinct("x"), sum_non_distinct("a")],
                output_columns: vec![
                    OutputColumn {
                        name: "g".into(),
                        data_type: DataType::Int64,
                        nullable: false,
                    },
                    OutputColumn {
                        name: "count(distinct x)".into(),
                        data_type: DataType::Int64,
                        nullable: true,
                    },
                    OutputColumn {
                        name: "sum(a)".into(),
                        data_type: DataType::Int64,
                        nullable: true,
                    },
                ],
            }),
            children: vec![sg],
        };
        let out = SplitDistinctAgg.apply(&mexpr, &mut memo);
        assert_eq!(out.len(), 1, "expected one multi-phase alternative");
        // Top: GLOBAL, group_by=[g], aggregates[0] = count(x), aggregates[1] = sum(a) (merge)
        let top = match &out[0].op {
            Operator::PhysicalHashAggregate(p) => p,
            other => panic!("expected GLOBAL PhysicalHashAggregate, got {:?}", other),
        };
        assert!(matches!(top.mode, AggMode::Global));
        assert_eq!(top.group_by.len(), 1, "GLOBAL group_by is just [g]");
        assert_eq!(top.aggregates.len(), 2);
        assert_eq!(top.aggregates[0].name, "count");
        assert!(!top.aggregates[0].distinct);
        assert_eq!(top.is_merge, vec![false, true]);

        // Follow chain: GLOBAL -> DISTINCT_GLOBAL -> LOCAL -> scan
        assert_eq!(out[0].children.len(), 1);
        let dg_group = &memo.groups[out[0].children[0]];
        let dg = match &dg_group.physical_exprs[0].op {
            Operator::PhysicalHashAggregate(p) => p,
            other => panic!("expected DISTINCT_GLOBAL, got {:?}", other),
        };
        assert!(matches!(dg.mode, AggMode::DistinctGlobal));
        assert_eq!(dg.group_by.len(), 2, "DG group_by is [g, x]");
        assert_eq!(dg.aggregates.len(), 1); // only sum(a); count(distinct x) is folded into grouping
        assert_eq!(dg.is_merge, vec![true]);
        assert_eq!(dg_group.physical_exprs[0].children.len(), 1);

        let local_group = &memo.groups[dg_group.physical_exprs[0].children[0]];
        let local = match &local_group.physical_exprs[0].op {
            Operator::PhysicalHashAggregate(p) => p,
            other => panic!("expected LOCAL, got {:?}", other),
        };
        assert!(matches!(local.mode, AggMode::Local));
        assert_eq!(local.group_by.len(), 2, "LOCAL group_by is [g, x]");
        assert_eq!(local.aggregates.len(), 1);
        assert_eq!(local.is_merge, vec![false]);
        assert_eq!(local_group.physical_exprs[0].children, vec![sg]);
    }
```

- [ ] **Step 3: Run tests**

Run: `cargo test --lib split_distinct_agg 2>&1 | tail -20`
Expected: 6 tests pass (5 skeleton + 1 new).

Run: `cargo test --lib 2>&1 | tail -5`
Expected: 963 pass / 0 fail.

- [ ] **Step 4: Commit**

```bash
git add src/sql/optimizer/rules/split_distinct_agg.rs
git commit -m "feat: SplitDistinctAgg 3-phase chain for GROUP BY + DISTINCT"
```

---

### Task 5: 4-phase chain construction

**Files:**
- Modify: `src/sql/optimizer/rules/split_distinct_agg.rs` (`apply_four_phase`)

- [ ] **Step 1: Implement `apply_four_phase`**

Replace the stub `apply_four_phase` body with:

```rust
fn apply_four_phase(
    expr: &MExpr,
    memo: &mut Memo,
    agg: &crate::sql::optimizer::operator::LogicalAggregateOp,
    distinct_col: &TypedExpr,
    non_distinct: &[AggregateCall],
) -> Vec<NewExpr> {
    // LOCAL: group_by = [x]; non_distinct aggs with update semantics.
    let local = MExpr {
        id: memo.next_expr_id(),
        op: Operator::PhysicalHashAggregate(PhysicalHashAggregateOp {
            mode: AggMode::Local,
            group_by: vec![distinct_col.clone()],
            aggregates: non_distinct.to_vec(),
            output_columns: vec![],
            is_merge: vec![false; non_distinct.len()],
        }),
        children: expr.children.clone(),
    };
    let local_group = memo.new_group(local);

    // DISTINCT_GLOBAL: group_by = [x]; merge non_distinct states.
    let dg = MExpr {
        id: memo.next_expr_id(),
        op: Operator::PhysicalHashAggregate(PhysicalHashAggregateOp {
            mode: AggMode::DistinctGlobal,
            group_by: vec![distinct_col.clone()],
            aggregates: non_distinct.to_vec(),
            output_columns: vec![],
            is_merge: vec![true; non_distinct.len()],
        }),
        children: vec![local_group],
    };
    let dg_group = memo.new_group(dg);

    // DISTINCT_LOCAL: scalar (empty group_by); [count(x) update, non_distinct merge...].
    let count_x = AggregateCall {
        name: "count".into(),
        args: vec![distinct_col.clone()],
        distinct: false,
        result_type: arrow::datatypes::DataType::Int64,
        order_by: vec![],
    };
    let mut dl_aggs = Vec::with_capacity(1 + non_distinct.len());
    dl_aggs.push(count_x.clone());
    dl_aggs.extend(non_distinct.iter().cloned());
    let mut dl_merge = Vec::with_capacity(1 + non_distinct.len());
    dl_merge.push(false);
    dl_merge.extend(std::iter::repeat(true).take(non_distinct.len()));

    let dl = MExpr {
        id: memo.next_expr_id(),
        op: Operator::PhysicalHashAggregate(PhysicalHashAggregateOp {
            mode: AggMode::DistinctLocal,
            group_by: vec![],
            aggregates: dl_aggs.clone(),
            output_columns: vec![],
            is_merge: dl_merge.clone(),
        }),
        children: vec![dg_group],
    };
    let dl_group = memo.new_group(dl);

    // GLOBAL: scalar; aggregates are MERGES (count state merge across instances +
    // non_distinct state merges). This works because count's merge is
    // "state + incoming state" which is the desired "sum partial counts" behavior.
    let global_aggs = dl_aggs;
    let global_merge = vec![true; 1 + non_distinct.len()];

    vec![NewExpr {
        op: Operator::PhysicalHashAggregate(PhysicalHashAggregateOp {
            mode: AggMode::Global,
            group_by: vec![],
            aggregates: global_aggs,
            output_columns: agg.output_columns.clone(),
            is_merge: global_merge,
        }),
        children: vec![dl_group],
    }]
}
```

Key insight: at GLOBAL in the 4-phase path, `count(x)` is_merge=true. The backend interprets this as "merge partial count states", which is exactly what we want — each partial count coming from a DISTINCT_LOCAL instance becomes a state to be merged (added) into the running count. This avoids synthesizing a separate `sum(partial_count)` call with late-bound column names.

- [ ] **Step 2: Add integration test for 4-phase chain**

Append to the `tests` module:

```rust
    #[test]
    fn four_phase_chain_when_scalar() {
        let mut memo = Memo::new();
        let sg = scan_group(&mut memo);
        let mexpr = MExpr {
            id: memo.next_expr_id(),
            op: Operator::LogicalAggregate(LogicalAggregateOp {
                group_by: vec![],
                aggregates: vec![count_distinct("x"), sum_non_distinct("a")],
                output_columns: vec![
                    OutputColumn {
                        name: "count(distinct x)".into(),
                        data_type: DataType::Int64,
                        nullable: true,
                    },
                    OutputColumn {
                        name: "sum(a)".into(),
                        data_type: DataType::Int64,
                        nullable: true,
                    },
                ],
            }),
            children: vec![sg],
        };
        let out = SplitDistinctAgg.apply(&mexpr, &mut memo);
        assert_eq!(out.len(), 1);

        // Top: GLOBAL, scalar, [count(x) merge, sum(a) merge]
        let top = match &out[0].op {
            Operator::PhysicalHashAggregate(p) => p,
            other => panic!("expected GLOBAL, got {:?}", other),
        };
        assert!(matches!(top.mode, AggMode::Global));
        assert_eq!(top.group_by.len(), 0);
        assert_eq!(top.aggregates.len(), 2);
        assert_eq!(top.is_merge, vec![true, true]);

        // DISTINCT_LOCAL: scalar, [count(x) update, sum(a) merge]
        let dl_group = &memo.groups[out[0].children[0]];
        let dl = match &dl_group.physical_exprs[0].op {
            Operator::PhysicalHashAggregate(p) => p,
            other => panic!("expected DISTINCT_LOCAL, got {:?}", other),
        };
        assert!(matches!(dl.mode, AggMode::DistinctLocal));
        assert_eq!(dl.group_by.len(), 0);
        assert_eq!(dl.is_merge, vec![false, true]);

        // DISTINCT_GLOBAL: group_by=[x], [sum(a) merge]
        let dg_group = &memo.groups[dl_group.physical_exprs[0].children[0]];
        let dg = match &dg_group.physical_exprs[0].op {
            Operator::PhysicalHashAggregate(p) => p,
            other => panic!("expected DISTINCT_GLOBAL, got {:?}", other),
        };
        assert!(matches!(dg.mode, AggMode::DistinctGlobal));
        assert_eq!(dg.group_by.len(), 1);
        assert_eq!(dg.is_merge, vec![true]);

        // LOCAL: group_by=[x], [sum(a) update]
        let local_group = &memo.groups[dg_group.physical_exprs[0].children[0]];
        let local = match &local_group.physical_exprs[0].op {
            Operator::PhysicalHashAggregate(p) => p,
            other => panic!("expected LOCAL, got {:?}", other),
        };
        assert!(matches!(local.mode, AggMode::Local));
        assert_eq!(local.group_by.len(), 1);
        assert_eq!(local.is_merge, vec![false]);
        assert_eq!(local_group.physical_exprs[0].children, vec![sg]);
    }
```

- [ ] **Step 3: Run tests**

Run: `cargo test --lib split_distinct_agg 2>&1 | tail -10`
Expected: 7 tests pass.

Run: `cargo test --lib 2>&1 | tail -5`
Expected: 964 pass / 0 fail.

- [ ] **Step 4: Commit**

```bash
git add src/sql/optimizer/rules/split_distinct_agg.rs
git commit -m "feat: SplitDistinctAgg 4-phase chain for scalar DISTINCT"
```

---

### Task 6: Distribution contracts for the new modes

**Files:**
- Modify: `src/sql/optimizer/search.rs`

- [ ] **Step 1: Refine `output_properties` HashAgg arm**

Current implementation (lines 309-326) already produces `Hash(group_by)` or `Gather` for any AggMode, which is correct for all five variants:
- LOCAL/DISTINCT_LOCAL (scalar) → Gather (group_by empty).
- DISTINCT_GLOBAL (with group-by x) → Hash([…, x]).
- GLOBAL (group-by or scalar) → Hash(group_by) or Gather.

No change needed.

- [ ] **Step 2: Refine `required_input_properties` HashAgg arm**

The arm at lines 475-495 currently covers Single/Local/Global only. Replace the match body (added placeholder arms in Task 1 shall now become real):

```rust
        Operator::PhysicalHashAggregate(a) => match a.mode {
            AggMode::Single => {
                if a.group_by.is_empty() {
                    vec![PhysicalPropertySet::gather()]
                } else {
                    vec![PhysicalPropertySet::any()]
                }
            }
            AggMode::Local => vec![PhysicalPropertySet::any()],
            AggMode::Global => {
                let cols = typed_exprs_to_column_refs(&a.group_by);
                if cols.is_empty() {
                    vec![PhysicalPropertySet::gather()]
                } else {
                    vec![PhysicalPropertySet {
                        distribution: DistributionSpec::HashPartitioned(cols),
                        ordering: OrderingSpec::Any,
                    }]
                }
            }
            // DISTINCT_GLOBAL receives shuffled-by-group_by input. Its own
            // group_by already includes the distinct column, so the enforcer
            // inserts a Hash(group_by) exchange between LOCAL and DISTINCT_GLOBAL.
            AggMode::DistinctGlobal => {
                let cols = typed_exprs_to_column_refs(&a.group_by);
                if cols.is_empty() {
                    // Shouldn't happen in practice — SplitDistinctAgg always
                    // adds the distinct column to group_by — but handle it.
                    vec![PhysicalPropertySet::gather()]
                } else {
                    vec![PhysicalPropertySet {
                        distribution: DistributionSpec::HashPartitioned(cols),
                        ordering: OrderingSpec::Any,
                    }]
                }
            }
            // DISTINCT_LOCAL runs per-instance on DISTINCT_GLOBAL's output; no
            // exchange needed between them (the data is already "partitioned by
            // distinct-col" and DISTINCT_LOCAL aggregates scalar).
            AggMode::DistinctLocal => vec![PhysicalPropertySet::any()],
        },
```

- [ ] **Step 3: Add two unit tests**

Append to the HashAgg test module in `search.rs` (near the existing `top_n_*` tests, or at the hash-agg tests — find `fn required_input_shuffle_join` or similar nearby and put them after it):

```rust
    #[test]
    fn distinct_global_requires_hash_on_group_by() {
        let op = Operator::PhysicalHashAggregate(PhysicalHashAggregateOp {
            mode: AggMode::DistinctGlobal,
            group_by: vec![col("g"), col("x")],
            aggregates: vec![],
            output_columns: vec![],
            is_merge: vec![],
        });
        let reqs = required_input_properties(&op, &PhysicalPropertySet::any(), 1);
        assert_eq!(reqs.len(), 1);
        match &reqs[0].distribution {
            DistributionSpec::HashPartitioned(cols) => {
                assert_eq!(cols.len(), 2, "Hash on both g and x");
            }
            other => panic!("expected HashPartitioned, got {:?}", other),
        }
    }

    #[test]
    fn distinct_local_requires_any() {
        let op = Operator::PhysicalHashAggregate(PhysicalHashAggregateOp {
            mode: AggMode::DistinctLocal,
            group_by: vec![],
            aggregates: vec![],
            output_columns: vec![],
            is_merge: vec![],
        });
        let reqs = required_input_properties(&op, &PhysicalPropertySet::gather(), 1);
        assert_eq!(reqs.len(), 1);
        assert!(matches!(reqs[0].distribution, DistributionSpec::Any));
    }
```

**Note:** The `col(…)` helper may not be in scope in `search.rs`'s test module; if not, construct `TypedExpr::ColumnRef` inline with the helper pattern used by nearby tests, or lift a small helper similar to the one in `split_distinct_agg.rs`.

- [ ] **Step 4: Run tests**

Run: `cargo test --lib 2>&1 | tail -5`
Expected: 966 pass / 0 fail.

- [ ] **Step 5: Commit**

```bash
git add src/sql/optimizer/search.rs
git commit -m "feat: distribution contracts for DistinctGlobal + DistinctLocal"
```

---

### Task 7: Register rule, TPC-DS validation, hand-crafted 3-phase test, EXPLAIN diff, landing note

**Files:**
- Modify: `src/sql/optimizer/rules/mod.rs` (register rule)
- Create: `sql-tests/aggregate/distinct_group_by_multi_phase.sql` + `.result` (hand-crafted 3-phase verify case)
- Modify: `docs/design/specs/2026-04-17-distinct-multi-phase-aggregation-design.md` (landing note)

- [ ] **Step 1: Register `SplitDistinctAgg`**

In `src/sql/optimizer/rules/mod.rs`, find the `all_implementation_rules()` function (around line 11) and add `Box::new(split_distinct_agg::SplitDistinctAgg)` at the end, after `SubqueryAliasToPhysical`:

```rust
pub(crate) fn all_implementation_rules() -> Vec<Box<dyn Rule>> {
    vec![
        // … existing rules …
        Box::new(implement::SubqueryAliasToPhysical),
        Box::new(split_distinct_agg::SplitDistinctAgg),
    ]
}
```

- [ ] **Step 2: Rebuild release and run TPC-DS**

```bash
cargo build --release 2>&1 | tail -5
pkill -f "novarocks standalone-server" || true
sleep 2
NO_PROXY=127.0.0.1,localhost ./target/release/novarocks standalone-server --port 9030 &
sleep 8
NO_PROXY=127.0.0.1,localhost mysql -h 127.0.0.1 -P 9030 -u root -e "SELECT 1"
curl -s http://127.0.0.1:9000/minio/health/live && echo minio-ok

cargo run --release --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests -- \
    --suite tpc-ds --mode verify --query-timeout 120 -j 1 2>&1 | tail -10
```

Expected: **99/99 pass**. The four DISTINCT queries (q16, q28, q94, q95) should still produce correct results, now via the 4-phase plan. If any query fails, stop and report BLOCKED with the failing query and its `EXPLAIN`.

- [ ] **Step 3: EXPLAIN spot-check — confirm multi-phase materialized**

For q16 (smallest):

```bash
NO_PROXY=127.0.0.1,localhost mysql -h 127.0.0.1 -P 9030 -u root --table -e "
CREATE EXTERNAL CATALOG IF NOT EXISTS iceberg_ref PROPERTIES (
    'type'='iceberg','iceberg.catalog.type'='hadoop',
    'iceberg.catalog.warehouse'='oss://novarocks/iceberg-catalog/',
    'aws.s3.access_key'='admin','aws.s3.secret_key'='admin123',
    'aws.s3.endpoint'='http://127.0.0.1:9000',
    'aws.s3.enable_path_style_access'='true');
USE iceberg_ref.tpcds;
$(cat sql-tests/tpc-ds/sql/q16.sql | sed 's/^/EXPLAIN /' | head -1)
$(cat sql-tests/tpc-ds/sql/q16.sql | tail -n +2)
" 2>&1 | head -40
```

Expected output should contain at minimum:
- One `HASH AGGREGATE (GLOBAL, …)` line
- One `HASH AGGREGATE (DISTINCT_LOCAL, …)` line
- One `HASH AGGREGATE (DISTINCT_GLOBAL, group by: […, cs_order_number])` line
- One `HASH AGGREGATE (LOCAL, group by: […, cs_order_number])` line
- `HASH EXCHANGE (cs_order_number)` between LOCAL and DISTINCT_GLOBAL

If the plan is still `HASH AGGREGATE (SINGLE)`, the cost model picked SINGLE — proceed anyway (TPC-DS correctness is the gate; cost tuning is a follow-up).

**Note on EXPLAIN display:** the existing `src/sql/explain.rs` will print mode names. If `DistinctLocal` / `DistinctGlobal` render with their default Debug form, leave as-is — that's still readable.

- [ ] **Step 4: Create hand-crafted 3-phase verify case**

Create `sql-tests/aggregate/distinct_group_by_multi_phase.sql`:

```sql
-- @order_sensitive=true
-- Test Objective:
-- 1. Exercise the 3-phase DISTINCT aggregation (LOCAL → DISTINCT_GLOBAL → GLOBAL).
-- 2. TPC-DS has no GROUP BY + count(distinct) query; this is the coverage gap filler.

DROP TABLE IF EXISTS ${case_db}.t_dg;
CREATE TABLE ${case_db}.t_dg (
    g INT,
    x INT,
    a BIGINT
);
INSERT INTO ${case_db}.t_dg VALUES
    (1, 100, 10), (1, 100, 20), (1, 200, 30),
    (2, 100, 40), (2, 300, 50), (2, 300, 60),
    (3, 400, 70);

-- query 1
SELECT g, count(distinct x) AS dc, sum(a) AS sa
FROM ${case_db}.t_dg
GROUP BY g
ORDER BY g;
```

Create `sql-tests/aggregate/distinct_group_by_multi_phase.result`:

```
-- query 1
g	dc	sa
1	2	60
2	2	150
3	1	70
```

Verify that the aggregate suite still picks up this case:

```bash
cargo run --release --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests -- \
    --suite aggregate --only distinct_group_by_multi_phase --mode verify 2>&1 | tail -10
```

Expected: 1/1 pass. If the suite doesn't find the case, check the `aggregate` directory path and file naming matches the suite's convention (look at neighboring `.sql` files in `sql-tests/aggregate/` for reference).

**If `sql-tests/aggregate/` doesn't exist**, inspect `sql-tests/` directory listing and pick the appropriate suite directory (likely the one containing other aggregate tests). The file name can remain `distinct_group_by_multi_phase.sql` regardless.

- [ ] **Step 5: Run full aggregate suite to confirm no regression**

```bash
cargo run --release --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests -- \
    --suite aggregate --mode verify -j 4 2>&1 | tail -5
```

Expected: all pre-existing cases + the new one pass.

- [ ] **Step 6: EXPLAIN baseline diff vs TopN baseline**

Baseline from the TopN landing: `/tmp/novarocks-plan-compare/standalone-merge-topn/` (99 files).

Capture new:

```bash
mkdir -p /tmp/novarocks-plan-compare/standalone-distinct
for f in sql-tests/tpc-ds/sql/q*.sql; do
  name=$(basename "$f" .sql)
  {
    echo "USE iceberg_ref.tpcds;"
    echo "EXPLAIN"
    cat "$f"
  } | NO_PROXY=127.0.0.1,localhost mysql -h 127.0.0.1 -P 9030 -u root --table \
    > "/tmp/novarocks-plan-compare/standalone-distinct/$name.txt"
done
diff -q /tmp/novarocks-plan-compare/standalone-merge-topn/ \
        /tmp/novarocks-plan-compare/standalone-distinct/ \
  > /tmp/distinct-diff-summary.txt 2>&1 || true
cat /tmp/distinct-diff-summary.txt
```

Expected: the 4 DISTINCT queries (q16, q28, q94, q95) differ. All other 95 queries byte-identical.

Spot-check the 4 diffing queries — confirm the new plan shape contains `DISTINCT_LOCAL` / `DISTINCT_GLOBAL` phases. Record counts. If any non-DISTINCT query differs, stop and report BLOCKED (Risk 2 in the spec fired).

- [ ] **Step 7: Append landing note to the design spec**

Append to `docs/design/specs/2026-04-17-distinct-multi-phase-aggregation-design.md`:

```markdown
---

## 8. Landing Note

**Landed.** Date: YYYY-MM-DD. HEAD: `<commit sha of Step 8 commit>`. Implementation complete across 7 tasks. TPC-DS standalone suite verify: **99/99 pass** (`-j 1`, `--query-timeout 120`). Lib tests: **<final count> / 0 fail**. Hand-crafted 3-phase case: passed.

**EXPLAIN diff summary** (vs post-TopN baseline `/tmp/novarocks-plan-compare/standalone-merge-topn/`):
- Queries that differ: **<N> of 99** (expected exactly the 4 DISTINCT queries).
- Queries with multi-phase DISTINCT plan: <list>.
- Queries with plan shape unchanged: **<95> of 99** (hard requirement).

**Representative EXPLAIN (q16):**
```
<paste a few lines showing DISTINCT_GLOBAL + DISTINCT_LOCAL or the equivalent 4-phase form>
```

**Non-goals deferred:**
- Multi-column DISTINCT args (`count(distinct a, b)`).
- Multiple DISTINCT on different columns (`count(distinct a), count(distinct b)`).
- `multi_distinct_count` / `fused_multi_distinct` low-NDV rewrite.
- q80's SINGLE-over-REPEAT cost-tuning.
```

Fill in the `<…>` placeholders with the measured values.

- [ ] **Step 8: Commit**

```bash
git add src/sql/optimizer/rules/mod.rs \
        sql-tests/aggregate/distinct_group_by_multi_phase.sql \
        sql-tests/aggregate/distinct_group_by_multi_phase.result \
        docs/design/specs/2026-04-17-distinct-multi-phase-aggregation-design.md
git commit -m "feat: register SplitDistinctAgg + DISTINCT multi-phase verified end-to-end"
```

---

## Risks Recap

- **Task 2 is the biggest change** — per-call `is_merge` refactor touches operator struct, rule, fragment builder. Commit size 300+ LOC. Full lib-test + TPC-DS gate before moving on.
- **Task 7 Step 3 (EXPLAIN spot-check)** is the safety signal that the rule actually materializes. If cost search picks SINGLE for all four DISTINCT queries, TPC-DS still passes but the feature is effectively dormant — record adoption rate in the landing note and open a follow-up for cost tuning.
- **Task 7 Step 6 unrelated-plan changes** (any non-DISTINCT query showing a different EXPLAIN): STOP and investigate. The per-call `is_merge` refactor should be behavior-preserving; any unexpected shift is a Task 2 bug.
