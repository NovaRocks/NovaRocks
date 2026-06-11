# Phase 1 TopN Pilot — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Move the "Sort + Limit → TOP-N" optimization out of `fragment_builder.rs` (post-mutation) and into the cascades layer as a proper transformation rule. Introduce `LogicalTopN` and `PhysicalTopN` operators so that cascades chooses TOP-N as a first-class plan.

**Architecture:** Add `LogicalTopN` + `PhysicalTopN` to the cascades operator enum. A transformation rule `SortLimitToTopN` detects `LogicalLimit(LogicalSort(x))` and produces an equivalent `LogicalTopN(x)` in the same memo group. An implementation rule `TopNToPhysical` converts `LogicalTopN` to `PhysicalTopN`. Fragment builder gains a `visit_physical_top_n` method that emits a `SORT_NODE` with `use_top_n=true` — pure translation, no post-mutation. The existing `visit_limit` loses its `use_top_n` post-mutation.

**Tech Stack:** Rust 2021, cascades optimizer in `src/sql/cascades/`, StarRocks Thrift `SORT_NODE` with `use_top_n` flag, `cargo test` for unit tests, `sql-test-runner` for end-to-end TPC-DS verification.

**Spec reference:** `docs/design/specs/2026-04-13-planner-layering-and-two-phase-agg-design.md` §4.1.

---

## Task 0: Capture Phase-0 Baseline

Capture the current standalone EXPLAIN plans for all 99 TPC-DS queries as the Phase-0 baseline. Phase 1's final verification diffs against this snapshot.

**Files:**
- Scripts run; no code changes.

- [ ] **Step 1: Make sure FE cluster is up.**

```bash
NO_PROXY=127.0.0.1,localhost mysql -h 127.0.0.1 -P9130 -u root --batch -e "SHOW BACKENDS;" 2>&1 | head -3
```

Expected: one row with `Alive=true`. If not, follow `starrocks-fe-on-novarocks` skill to start FE and register the BE.

- [ ] **Step 2: Build and start the standalone server on port 9030.**

```bash
cd /Users/harbor/project/NovaRocks
cargo build 2>&1 | tail -5
NO_PROXY=127.0.0.1,localhost ./target/debug/novarocks standalone-server --port 9030 &
disown
# wait a few seconds, then verify
NO_PROXY=127.0.0.1,localhost mysql -h 127.0.0.1 -P9030 -u root -e "SELECT 1;"
```

Expected: build succeeds; `SELECT 1` returns `1`.

- [ ] **Step 3: Create the Iceberg TPC-DS catalog in standalone.**

```bash
NO_PROXY=127.0.0.1,localhost mysql -h 127.0.0.1 -P9030 -u root --batch -e "
CREATE EXTERNAL CATALOG IF NOT EXISTS iceberg_tpcds PROPERTIES(
  \"type\"=\"iceberg\",
  \"iceberg.catalog.type\"=\"hadoop\",
  \"iceberg.catalog.warehouse\"=\"oss://novarocks/iceberg-catalog/\",
  \"aws.s3.access_key\"=\"admin\",
  \"aws.s3.secret_key\"=\"admin123\",
  \"aws.s3.endpoint\"=\"http://127.0.0.1:9000\",
  \"aws.s3.enable_path_style_access\"=\"true\"
);"
# verify catalog works
NO_PROXY=127.0.0.1,localhost mysql -h 127.0.0.1 -P9030 -u root -e "USE iceberg_tpcds.tpcds; SELECT 1 FROM store_sales LIMIT 1;"
```

Expected: `SELECT 1` returns `1`.

- [ ] **Step 4: Capture 99 standalone EXPLAIN plans as Phase-0 baseline.**

```bash
mkdir -p /tmp/novarocks-plan-compare/standalone-phase0
for i in $(seq 1 99); do
  qfile="/Users/harbor/project/NovaRocks/sql-tests/tpc-ds/sql/q${i}.sql"
  if [ -f "$qfile" ]; then
    sql=$(cat "$qfile")
    NO_PROXY=127.0.0.1,localhost mysql -h 127.0.0.1 -P9030 -u root --batch -N \
      -e "USE iceberg_tpcds.tpcds; EXPLAIN ${sql}" \
      > "/tmp/novarocks-plan-compare/standalone-phase0/q${i}.plan" 2>&1
    ret=$?
    if [ $ret -ne 0 ]; then echo "q${i}: FAILED"; fi
  fi
done
ls /tmp/novarocks-plan-compare/standalone-phase0/ | wc -l
```

Expected: output is `99`. No `FAILED` lines.

- [ ] **Step 5: Stop standalone server.**

```bash
kill $(ps aux | grep 'standalone-server.*9030' | grep -v grep | awk '{print $2}')
```

- [ ] **Step 6: Commit the baseline snapshot as reference (via git note or sidecar file).**

No git commit needed — snapshots live in `/tmp`. Note the timestamp and commit sha in a scratch file:

```bash
echo "phase0 baseline: $(git rev-parse HEAD) at $(date -Iseconds)" \
  > /tmp/novarocks-plan-compare/phase0-ref.txt
```

---

## Task 1: Add `LogicalTopN` and `PhysicalTopN` Operator Types

Define the new operator structs and wire them into the `Operator` enum. No behavior change yet — just type-level additions that compile.

**Files:**
- Modify: `src/sql/cascades/operator.rs`
- Test: `src/sql/cascades/operator.rs` (inline `#[cfg(test)]` is not used here; the compile-test is the check)

- [ ] **Step 1: Add `LogicalTopNOp` struct.**

Edit `src/sql/cascades/operator.rs`. Insert the struct definition after `LogicalLimitOp` (around line 78):

```rust
#[derive(Clone, Debug)]
pub(crate) struct LogicalTopNOp {
    pub items: Vec<SortItem>,
    pub limit: Option<i64>,
    pub offset: Option<i64>,
}
```

- [ ] **Step 2: Add `PhysicalTopNOp` struct.**

Insert after `PhysicalLimitOp` (around line 199):

```rust
#[derive(Clone, Debug)]
pub(crate) struct PhysicalTopNOp {
    pub items: Vec<SortItem>,
    pub limit: Option<i64>,
    pub offset: Option<i64>,
}
```

- [ ] **Step 3: Add both variants to the `Operator` enum.**

In the `Operator` enum (around line 276), add `LogicalTopN(LogicalTopNOp)` alongside the other logical variants (put it just after `LogicalLimit(LogicalLimitOp)`):

```rust
    LogicalLimit(LogicalLimitOp),
    LogicalTopN(LogicalTopNOp),
    LogicalWindow(LogicalWindowOp),
```

Add `PhysicalTopN(PhysicalTopNOp)` just after `PhysicalLimit(PhysicalLimitOp)`:

```rust
    PhysicalLimit(PhysicalLimitOp),
    PhysicalTopN(PhysicalTopNOp),
    PhysicalWindow(PhysicalWindowOp),
```

- [ ] **Step 4: Add `LogicalTopN` to `is_logical()`.**

In `impl Operator::is_logical` (around line 321), add the new variant to the pattern:

```rust
                | Operator::LogicalLimit(_)
                | Operator::LogicalTopN(_)
                | Operator::LogicalWindow(_)
```

- [ ] **Step 5: Compile check.**

```bash
cd /Users/harbor/project/NovaRocks
cargo build 2>&1 | tail -20
```

Expected: compilation fails with many "non-exhaustive patterns" errors across the cascades modules. That is the whole point — we now have to teach every consumer how to handle the new variants. The rest of the tasks address these one file at a time.

**Stop here — do not try to "suppress" the errors with wildcards or TODOs.** Those errors are the plan's working set.

- [ ] **Step 6: Commit the operator-type addition, even though build is broken.**

Committing a red state here is deliberate: this commit isolates the type change from the consumer-side fixes that follow.

```bash
cd /Users/harbor/project/NovaRocks
git add src/sql/cascades/operator.rs
git commit -m "Add LogicalTopN and PhysicalTopN operator variants (skeleton)

Consumers (cost, search, extract, explain, fragment builder, rules) do
not yet handle the new variants; subsequent commits fill them in.
"
```

---

## Task 2: Cost Model for `PhysicalTopN`

Teach `compute_cost` that `PhysicalTopN` costs `n × log2(k)` where `k = min(n, limit+offset)`. This is cheaper than `PhysicalSort` when `limit` is small, which drives the optimizer to prefer TOP-N over `Sort + Limit`.

**Files:**
- Modify: `src/sql/cascades/cost.rs`
- Test: `src/sql/cascades/cost.rs` (existing `#[cfg(test)] mod tests`)

- [ ] **Step 1: Write the failing test.**

Append to `src/sql/cascades/cost.rs` inside `mod tests`:

```rust
#[test]
fn top_n_cheaper_than_sort_for_small_limit() {
    // 10 million input rows; TOP-N with limit=100 should be far cheaper than a full sort.
    let s = stats(10_000_000.0, 50.0);
    let sort = Operator::PhysicalSort(PhysicalSortOp { items: vec![] });
    let top_n = Operator::PhysicalTopN(PhysicalTopNOp {
        items: vec![],
        limit: Some(100),
        offset: None,
    });
    let cost_sort = compute_cost(&sort, &s, &[]);
    let cost_top_n = compute_cost(&top_n, &s, &[]);
    assert!(cost_top_n < cost_sort / 10.0,
        "expected TOP-N << Sort; got top_n={} sort={}", cost_top_n, cost_sort);
}

#[test]
fn top_n_falls_back_to_sort_cost_when_limit_exceeds_rows() {
    // If limit >= n, TOP-N degenerates to full sort; costs should be within a small factor.
    let s = stats(100.0, 10.0);
    let sort = Operator::PhysicalSort(PhysicalSortOp { items: vec![] });
    let top_n = Operator::PhysicalTopN(PhysicalTopNOp {
        items: vec![],
        limit: Some(10_000),
        offset: None,
    });
    let cost_sort = compute_cost(&sort, &s, &[]);
    let cost_top_n = compute_cost(&top_n, &s, &[]);
    assert!((cost_top_n - cost_sort).abs() < 1.0);
}
```

- [ ] **Step 2: Run tests to verify they fail.**

```bash
cd /Users/harbor/project/NovaRocks
cargo test --package novarocks sql::cascades::cost::tests::top_n_ 2>&1 | tail -20
```

Expected: compile errors (PhysicalTopN already exists from Task 1, but `compute_cost` has no arm for it, so the match is non-exhaustive). Test does not build.

- [ ] **Step 3: Add the cost arm.**

In `src/sql/cascades/cost.rs`, inside `compute_cost`'s match, add a new arm before `PhysicalDistribution` (so it sits near `PhysicalSort`):

```rust
        Operator::PhysicalTopN(t) => {
            let n = own_stats.output_row_count.max(1.0);
            // k = effective rows that must be tracked = min(n, limit + offset).
            let k = match (t.limit, t.offset) {
                (Some(l), Some(o)) => ((l as f64) + (o as f64)).min(n).max(1.0),
                (Some(l), None) => (l as f64).min(n).max(1.0),
                _ => n,
            };
            n * k.log2()
        }
```

- [ ] **Step 4: Run tests to verify they pass.**

```bash
cd /Users/harbor/project/NovaRocks
cargo test --package novarocks sql::cascades::cost::tests::top_n_ 2>&1 | tail -10
```

Expected: both new tests pass; other tests in the module still pass.

- [ ] **Step 5: Commit.**

```bash
cd /Users/harbor/project/NovaRocks
git add src/sql/cascades/cost.rs
git commit -m "Cost model for PhysicalTopN: n * log2(min(n, limit+offset))"
```

---

## Task 3: EXPLAIN Formatting for TopN

Format `LogicalTopN` and `PhysicalTopN` in EXPLAIN output so the Phase-0 vs Phase-1 diff is meaningful.

**Files:**
- Modify: `src/sql/explain.rs`
- Test: run EXPLAIN on a small in-memory plan or rely on Task 9's end-to-end diff

- [ ] **Step 1: Read the current `PhysicalSort` and `PhysicalLimit` formatters to match style.**

```bash
cd /Users/harbor/project/NovaRocks
grep -n 'PhysicalSort\|PhysicalLimit' src/sql/explain.rs
```

Review the formatting: `PhysicalSort` emits `SORT BY [item1 ASC NULLS FIRST, ...]`; `PhysicalLimit` emits `LIMIT [limit=N, offset=M]`.

- [ ] **Step 2: Add `PhysicalTopN` arm to `format_physical_node`.**

Insert near the `PhysicalSort` arm in `src/sql/explain.rs` (around line 398):

```rust
        Operator::PhysicalTopN(op) => {
            let items: Vec<String> = op
                .items
                .iter()
                .map(|s| {
                    let dir = if s.asc { "ASC" } else { "DESC" };
                    let nulls = if s.nulls_first { " NULLS FIRST" } else { " NULLS LAST" };
                    format!("{} {dir}{nulls}", format_expr(&s.expr))
                })
                .collect();
            let mut parts = Vec::new();
            if let Some(l) = op.limit { parts.push(format!("limit={l}")); }
            if let Some(o) = op.offset { parts.push(format!("offset={o}")); }
            out.push(format!(
                "{pad}TOP-N ({}) [{}]{costs_suffix}",
                parts.join(", "),
                items.join(", ")
            ));
            for child in &node.children {
                format_physical_node(child, level, indent + 1, out);
            }
        }
```

- [ ] **Step 3: No changes needed for logical EXPLAIN.**

`Operator::LogicalTopN` is a cascades-internal operator that only appears inside the memo. The user-facing logical EXPLAIN path goes through `format_node`, which takes `LogicalPlan`, not `Operator`. `LogicalPlan` does not have a `TopN` variant (and does not need one for this pilot), so no additions are required. Only `format_physical_node` (covered in Step 2) needs a new arm.

- [ ] **Step 4: Compile check.**

```bash
cd /Users/harbor/project/NovaRocks
cargo build 2>&1 | tail -10
```

Expected: fewer non-exhaustive-pattern errors than after Task 1. `explain.rs` is now OK; errors remain in rules/search/fragment_builder until their respective tasks land.

- [ ] **Step 5: Commit.**

```bash
cd /Users/harbor/project/NovaRocks
git add src/sql/explain.rs
git commit -m "EXPLAIN formatter for PhysicalTopN"
```

---

## Task 4: `SortLimitToTopN` Transformation Rule

This is the rule that produces `LogicalTopN` from `LogicalLimit(LogicalSort(x))`. It lives in a new file to keep rule code isolated.

**Files:**
- Create: `src/sql/cascades/rules/sort_limit_to_top_n.rs`
- Modify: `src/sql/cascades/rules/mod.rs`
- Test: `src/sql/cascades/rules/sort_limit_to_top_n.rs` (inline `#[cfg(test)]`)

- [ ] **Step 1: Write the failing test.**

Create `src/sql/cascades/rules/sort_limit_to_top_n.rs` with this scaffold:

```rust
//! Transformation rule: LogicalLimit(LogicalSort(x)) -> LogicalTopN(x).
//!
//! Produces an equivalent LogicalTopN expression in the Limit's group.
//! The Limit group's children are replaced: where Limit had [sort_group],
//! TopN has [grandchild_group].

use crate::sql::cascades::memo::{MExpr, Memo};
use crate::sql::cascades::operator::{LogicalLimitOp, LogicalSortOp, LogicalTopNOp, Operator};
use crate::sql::cascades::rule::{NewExpr, Rule, RuleType};

pub(crate) struct SortLimitToTopN;

impl Rule for SortLimitToTopN {
    fn name(&self) -> &str {
        "SortLimitToTopN"
    }

    fn rule_type(&self) -> RuleType {
        RuleType::Transformation
    }

    fn matches(&self, op: &Operator) -> bool {
        matches!(op, Operator::LogicalLimit(_))
    }

    fn apply(&self, expr: &MExpr, memo: &mut Memo) -> Vec<NewExpr> {
        let Operator::LogicalLimit(limit_op) = &expr.op else {
            return vec![];
        };
        // LogicalLimit has exactly one child.
        if expr.children.len() != 1 {
            return vec![];
        }
        let child_group_id = expr.children[0];

        // Look for any LogicalSort MExpr in the child group.
        let child_group = match memo.groups.get(child_group_id) {
            Some(g) => g,
            None => return vec![],
        };

        let mut results = Vec::new();
        for child_mexpr in child_group.logical_exprs.iter() {
            let Operator::LogicalSort(sort_op) = &child_mexpr.op else {
                continue;
            };
            if child_mexpr.children.len() != 1 {
                continue;
            }
            let grandchild_group_id = child_mexpr.children[0];
            results.push(NewExpr {
                op: Operator::LogicalTopN(LogicalTopNOp {
                    items: sort_op.items.clone(),
                    limit: limit_op.limit,
                    offset: limit_op.offset,
                }),
                children: vec![grandchild_group_id],
            });
        }
        results
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sql::cascades::memo::Memo;
    use crate::sql::cascades::operator::{
        LogicalLimitOp, LogicalScanOp, LogicalSortOp,
    };

    fn mk_scan_mexpr(memo: &mut Memo) -> MExpr {
        MExpr {
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
        }
    }

    #[test]
    fn fires_when_limit_has_sort_child() {
        let mut memo = Memo::new();
        let scan_mexpr = mk_scan_mexpr(&mut memo);
        let scan_group = memo.new_group(scan_mexpr);

        let sort_mexpr = MExpr {
            id: memo.next_expr_id(),
            op: Operator::LogicalSort(LogicalSortOp { items: vec![] }),
            children: vec![scan_group],
        };
        let sort_group = memo.new_group(sort_mexpr);

        let limit_mexpr = MExpr {
            id: memo.next_expr_id(),
            op: Operator::LogicalLimit(LogicalLimitOp {
                limit: Some(100),
                offset: None,
            }),
            children: vec![sort_group],
        };

        let rule = SortLimitToTopN;
        let out = rule.apply(&limit_mexpr, &mut memo);
        assert_eq!(out.len(), 1, "expected one TopN alternative");
        match &out[0].op {
            Operator::LogicalTopN(t) => {
                assert_eq!(t.limit, Some(100));
                assert_eq!(t.offset, None);
            }
            other => panic!("expected LogicalTopN, got {:?}", other),
        }
        // Children must point to the scan group, skipping the sort.
        assert_eq!(out[0].children, vec![scan_group]);
    }

    #[test]
    fn does_not_fire_when_limit_has_non_sort_child() {
        let mut memo = Memo::new();
        let scan_mexpr = mk_scan_mexpr(&mut memo);
        let scan_group = memo.new_group(scan_mexpr);

        let limit_mexpr = MExpr {
            id: memo.next_expr_id(),
            op: Operator::LogicalLimit(LogicalLimitOp {
                limit: Some(10),
                offset: None,
            }),
            children: vec![scan_group],
        };

        let rule = SortLimitToTopN;
        let out = rule.apply(&limit_mexpr, &mut memo);
        assert!(out.is_empty(), "expected no alternatives without a Sort child");
    }
}
```

- [ ] **Step 2: Register the rule module.**

Edit `src/sql/cascades/rules/mod.rs` and add the new module + include the rule in the transformation list:

```rust
pub(crate) mod implement;
pub(crate) mod join_associativity;
pub(crate) mod join_commutativity;
pub(crate) mod sort_limit_to_top_n;  // <-- NEW

// ...

pub(crate) fn all_transformation_rules() -> Vec<Box<dyn Rule>> {
    vec![
        Box::new(join_commutativity::JoinCommutativity),
        Box::new(join_associativity::JoinAssociativity),
        Box::new(sort_limit_to_top_n::SortLimitToTopN),  // <-- NEW
    ]
}
```

- [ ] **Step 3: Run the rule's tests to verify they pass.**

```bash
cd /Users/harbor/project/NovaRocks
cargo test --package novarocks sql::cascades::rules::sort_limit_to_top_n 2>&1 | tail -15
```

Expected: both tests pass. If `Memo::new()` / `memo.add_logical(...)` has a different API, adjust the test helpers — the goal is (a) build a memo where the LogicalLimit's child group contains a LogicalSort, and (b) call `rule.apply(&limit_mexpr, &mut memo)` and assert one `LogicalTopN` alternative pointing at the grandchild group.

- [ ] **Step 4: Commit.**

```bash
cd /Users/harbor/project/NovaRocks
git add src/sql/cascades/rules/sort_limit_to_top_n.rs src/sql/cascades/rules/mod.rs
git commit -m "Cascades transformation rule: SortLimitToTopN

Matches LogicalLimit(LogicalSort(x)); produces LogicalTopN(x) as an
alternative in the Limit's memo group, carrying merged sort_items,
limit, and offset. Children bypass the Sort group.
"
```

---

## Task 5: `TopNToPhysical` Implementation Rule

Convert `LogicalTopN` into `PhysicalTopN`. Same shape as `SortToPhysical` / `LimitToPhysical`.

**Files:**
- Modify: `src/sql/cascades/rules/implement.rs`
- Modify: `src/sql/cascades/rules/mod.rs`
- Test: `src/sql/cascades/rules/implement.rs` (inline `#[cfg(test)]` if present; otherwise add a small test alongside)

- [ ] **Step 1: Write the failing test.**

Add to the bottom of `src/sql/cascades/rules/implement.rs`:

```rust
#[cfg(test)]
mod top_n_tests {
    use super::*;
    use crate::sql::cascades::memo::{MExpr, Memo};
    use crate::sql::cascades::operator::LogicalTopNOp;

    #[test]
    fn top_n_to_physical_produces_physical_top_n() {
        let mut memo = Memo::new();
        let values_mexpr = MExpr {
            id: memo.next_expr_id(),
            op: Operator::LogicalValues(LogicalValuesOp {
                rows: vec![],
                columns: vec![],
            }),
            children: vec![],
        };
        let dummy_child = memo.new_group(values_mexpr);

        let expr = MExpr {
            id: memo.next_expr_id(),
            op: Operator::LogicalTopN(LogicalTopNOp {
                items: vec![],
                limit: Some(50),
                offset: Some(10),
            }),
            children: vec![dummy_child],
        };
        let rule = TopNToPhysical;
        let out = rule.apply(&expr, &mut memo);
        assert_eq!(out.len(), 1);
        match &out[0].op {
            Operator::PhysicalTopN(p) => {
                assert_eq!(p.limit, Some(50));
                assert_eq!(p.offset, Some(10));
            }
            other => panic!("expected PhysicalTopN, got {:?}", other),
        }
        assert_eq!(out[0].children, vec![dummy_child]);
    }
}
```

- [ ] **Step 2: Add the implementation rule.**

Insert in `src/sql/cascades/rules/implement.rs`, right after `LimitToPhysical` (around line 595):

```rust
// ---------------------------------------------------------------------------
// 8b. TopNToPhysical
// ---------------------------------------------------------------------------

pub(crate) struct TopNToPhysical;

impl Rule for TopNToPhysical {
    fn name(&self) -> &str {
        "TopNToPhysical"
    }
    fn rule_type(&self) -> RuleType {
        RuleType::Implementation
    }
    fn matches(&self, op: &Operator) -> bool {
        matches!(op, Operator::LogicalTopN(_))
    }
    fn apply(&self, expr: &MExpr, _memo: &mut Memo) -> Vec<NewExpr> {
        let Operator::LogicalTopN(op) = &expr.op else {
            return vec![];
        };
        vec![NewExpr {
            op: Operator::PhysicalTopN(PhysicalTopNOp {
                items: op.items.clone(),
                limit: op.limit,
                offset: op.offset,
            }),
            children: expr.children.clone(),
        }]
    }
}
```

- [ ] **Step 3: Register in `all_implementation_rules()`.**

In `src/sql/cascades/rules/mod.rs`, add to the list right after `implement::LimitToPhysical`:

```rust
        Box::new(implement::LimitToPhysical),
        Box::new(implement::TopNToPhysical),  // <-- NEW
        Box::new(implement::WindowToPhysical),
```

- [ ] **Step 4: Run the test.**

```bash
cd /Users/harbor/project/NovaRocks
cargo test --package novarocks sql::cascades::rules::implement::top_n_tests 2>&1 | tail -10
```

Expected: test passes.

- [ ] **Step 5: Commit.**

```bash
cd /Users/harbor/project/NovaRocks
git add src/sql/cascades/rules/implement.rs src/sql/cascades/rules/mod.rs
git commit -m "Cascades implementation rule: TopNToPhysical

Maps LogicalTopN -> PhysicalTopN, preserving sort items, limit, offset
and child group.
"
```

---

## Task 6: Property Derivation for `PhysicalTopN`

Teach `output_properties` and `required_input_properties` about `PhysicalTopN`. The physical contract matches `PhysicalSort`: global ordering, single-node output.

**Files:**
- Modify: `src/sql/cascades/search.rs`
- Test: `src/sql/cascades/search.rs` (add alongside existing tests)

- [ ] **Step 1: Write failing tests.**

Add near the bottom of `src/sql/cascades/search.rs`, in whatever `#[cfg(test)] mod` already exists (or create one if not):

```rust
#[cfg(test)]
mod top_n_property_tests {
    use super::*;
    use crate::sql::cascades::operator::PhysicalTopNOp;

    #[test]
    fn top_n_output_is_gather_when_sort_keys_resolve() {
        let op = Operator::PhysicalTopN(PhysicalTopNOp {
            items: vec![],
            limit: Some(100),
            offset: None,
        });
        let out = output_properties(&op);
        // With no sort keys, ordering is Any but distribution should still be Gather
        // because TopN produces a globally-ordered single-partition output.
        assert!(matches!(out.distribution, DistributionSpec::Gather));
    }

    #[test]
    fn top_n_requires_gather_input() {
        let op = Operator::PhysicalTopN(PhysicalTopNOp {
            items: vec![],
            limit: Some(100),
            offset: None,
        });
        let req = required_input_properties(&op, &PhysicalPropertySet::gather(), 1);
        assert_eq!(req.len(), 1);
        assert!(matches!(req[0].distribution, DistributionSpec::Gather));
    }
}
```

- [ ] **Step 2: Run to confirm failure.**

```bash
cd /Users/harbor/project/NovaRocks
cargo test --package novarocks sql::cascades::search::top_n_property_tests 2>&1 | tail -10
```

Expected: non-exhaustive-match errors, not test failure.

- [ ] **Step 3: Add `PhysicalTopN` arm to `output_properties`.**

In `src/sql/cascades/search.rs`, inside `output_properties`, add an arm next to `PhysicalSort` (around line 307):

```rust
        // TopN: Gather distribution + Ordered (same contract as Sort).
        Operator::PhysicalTopN(t) => {
            let sort_keys: Vec<SortKey> = t
                .items
                .iter()
                .filter_map(|item| {
                    typed_expr_to_column_ref(&item.expr).map(|col| SortKey {
                        column: col,
                        asc: item.asc,
                        nulls_first: item.nulls_first,
                    })
                })
                .collect();
            PhysicalPropertySet {
                distribution: DistributionSpec::Gather,
                ordering: if sort_keys.is_empty() {
                    OrderingSpec::Any
                } else {
                    OrderingSpec::Required(sort_keys)
                },
            }
        }
```

- [ ] **Step 4: Add `PhysicalTopN` arm to `required_input_properties`.**

In the same file, inside `required_input_properties`, add an arm next to `PhysicalSort` (around line 449):

```rust
        // TopN: child must be Gather (same as Sort).
        Operator::PhysicalTopN(_) => vec![PhysicalPropertySet::gather()],
```

- [ ] **Step 5: Run the tests.**

```bash
cd /Users/harbor/project/NovaRocks
cargo test --package novarocks sql::cascades::search::top_n_property_tests 2>&1 | tail -10
```

Expected: both pass.

- [ ] **Step 6: Compile the whole crate to catch any remaining non-exhaustive patterns.**

```bash
cd /Users/harbor/project/NovaRocks
cargo build 2>&1 | tail -15
```

Expected: any remaining errors are in `fragment_builder.rs` or similar consumers. If `cost.rs`, `explain.rs`, `search.rs`, and `extract.rs` are all clean, Task 7 is the only remaining surface.

- [ ] **Step 7: Commit.**

```bash
cd /Users/harbor/project/NovaRocks
git add src/sql/cascades/search.rs
git commit -m "Cascades property derivation for PhysicalTopN

Output: Gather distribution + Required ordering (same as Sort).
Requires input: Gather distribution.
"
```

---

## Task 7: Fragment Builder `visit_physical_top_n`

Translate `PhysicalTopN` to a Thrift `SORT_NODE` with `use_top_n=true`. Pure translation — no optimization decisions.

**Files:**
- Modify: `src/sql/cascades/fragment_builder.rs`
- Test: relies on Task 9 end-to-end verification

- [ ] **Step 1: Update `use` list to import `PhysicalTopNOp`.**

In `src/sql/cascades/fragment_builder.rs`, find the existing imports around line 23 and add `PhysicalTopNOp`:

```rust
    PhysicalHashJoinOp, PhysicalIntersectOp, PhysicalLimitOp, PhysicalNestLoopJoinOp,
    PhysicalTopNOp,  // <-- NEW (keep alphabetical if the existing list is alphabetical)
```

- [ ] **Step 2: Dispatch `PhysicalTopN` in the central `visit` match.**

In `src/sql/cascades/fragment_builder.rs` around line 263, add a new arm next to `PhysicalSort`:

```rust
            Operator::PhysicalSort(op) => self.visit_sort(op, node),
            Operator::PhysicalTopN(op) => self.visit_physical_top_n(op, node),
            Operator::PhysicalLimit(op) => self.visit_limit(op, node),
```

- [ ] **Step 3: Implement `visit_physical_top_n`.**

Insert near `visit_sort` and `visit_limit`. The method largely mirrors `visit_sort` but sets `use_top_n=true`, `limit`, and `offset` on the sort node.

```rust
    // -------------------------------------------------------------------
    // visit_physical_top_n — Sort + Limit as a single operator
    // -------------------------------------------------------------------

    fn visit_physical_top_n(
        &mut self,
        op: &PhysicalTopNOp,
        node: &PhysicalPlanNode,
    ) -> Result<VisitResult, String> {
        let child = self.visit(&node.children[0])?;

        let sort_node_id = self.alloc_node();
        let sort_tuple_id = self.desc_builder.alloc_tuple();
        for col in child.scope.columns() {
            self.desc_builder
                .copy_column_into_tuple(sort_tuple_id, col)?;
        }

        let mut ordering_exprs = Vec::new();
        let mut is_asc = Vec::new();
        let mut nulls_first_list = Vec::new();
        for item in &op.items {
            let mut compiler = ExprCompiler::new(&child.scope);
            let texpr = compiler.compile_typed(&item.expr)?;
            ordering_exprs.push(texpr);
            is_asc.push(item.asc);
            nulls_first_list.push(item.nulls_first);
        }

        let sort_info = plan_nodes::TSortInfo::new(
            ordering_exprs,
            is_asc,
            nulls_first_list,
            None::<Vec<exprs::TExpr>>,
        );

        let mut sort_plan_node = nodes::default_plan_node();
        sort_plan_node.node_id = sort_node_id;
        sort_plan_node.node_type = plan_nodes::TPlanNodeType::SORT_NODE;
        sort_plan_node.num_children = 1;
        sort_plan_node.limit = op.limit.unwrap_or(-1);
        sort_plan_node.row_tuples = vec![sort_tuple_id];
        sort_plan_node.nullable_tuples = vec![];
        sort_plan_node.compact_data = true;
        sort_plan_node.sort_node = Some(plan_nodes::TSortNode {
            sort_info,
            use_top_n: true,
            offset: op.offset,
            ordering_exprs: None,
            is_asc_order: None,
            is_default_limit: None,
            nulls_first: None,
            sort_tuple_slot_exprs: None,
            has_outer_join_child: None,
            sql_sort_keys: None,
            analytic_partition_exprs: None,
            partition_exprs: None,
            partition_limit: None,
            topn_type: None,
            build_runtime_filters: None,
            max_buffered_rows: None,
            max_buffered_bytes: None,
            late_materialization: None,
            enable_parallel_merge: None,
            analytic_partition_skewed: None,
            pre_agg_exprs: None,
            pre_agg_output_slot_id: None,
            pre_agg_insert_local_shuffle: None,
            parallel_merge_late_materialize_mode: None,
            per_pipeline: None,
        });

        // Pre-order: top-n first, then child.
        let mut plan_nodes = vec![sort_plan_node];
        plan_nodes.extend(child.plan_nodes);

        Ok(VisitResult {
            plan_nodes,
            scope: child.scope,
            tuple_ids: child.tuple_ids,
            cte_exchange_nodes: child.cte_exchange_nodes,
        })
    }
```

- [ ] **Step 4: Compile check.**

```bash
cd /Users/harbor/project/NovaRocks
cargo build 2>&1 | tail -10
```

Expected: clean build. If `desc_builder.copy_column_into_tuple` has a different signature, mirror whatever `visit_sort` uses verbatim.

- [ ] **Step 5: Run the full test suite to catch any collateral damage.**

```bash
cd /Users/harbor/project/NovaRocks
cargo test 2>&1 | tail -15
```

Expected: all green.

- [ ] **Step 6: Commit.**

```bash
cd /Users/harbor/project/NovaRocks
git add src/sql/cascades/fragment_builder.rs
git commit -m "Fragment builder visit_physical_top_n: translate to SORT_NODE with use_top_n

Pure translation; no optimization decisions. Mirrors visit_sort but
sets use_top_n=true, plan_node.limit=op.limit, sort_node.offset=op.offset.
"
```

---

## Task 8: Remove `use_top_n` Post-Mutation from `visit_limit`

Delete the 25-line block where `visit_limit` peeks at the child, detects a SORT_NODE, and retroactively flips `use_top_n=true` / sets `limit` / sets `offset`. The cascades layer now produces `PhysicalTopN` directly; `PhysicalLimit` should no longer sit on top of a `PhysicalSort` in chosen plans.

**Files:**
- Modify: `src/sql/cascades/fragment_builder.rs`
- Test: Task 9's regression suite

- [ ] **Step 1: Replace the body of `visit_limit`.**

In `src/sql/cascades/fragment_builder.rs`, lines 967–991, replace the SORT-aware post-mutation with a minimal limit-propagation that only applies to non-Sort top nodes. The updated body:

```rust
    fn visit_limit(
        &mut self,
        op: &PhysicalLimitOp,
        node: &PhysicalPlanNode,
    ) -> Result<VisitResult, String> {
        let mut child = self.visit(&node.children[0])?;

        // Limit on a Sort should have been rewritten to TopN at the cascades
        // level. Here we only apply the limit value to the child's top node
        // when it is not a SORT_NODE (e.g., Limit on a Scan or Filter).
        if let Some(limit) = op.limit {
            if let Some(top) = child.plan_nodes.first_mut() {
                debug_assert!(
                    top.node_type != plan_nodes::TPlanNodeType::SORT_NODE,
                    "Limit on Sort should have been rewritten to TopN; found SORT_NODE under Limit"
                );
                top.limit = limit;
            }
        }
        // Offset on a plain Limit has no direct execution slot outside of
        // SORT_NODE; it is expected to be folded into a TopN at the cascades
        // level. Warn via debug_assert if offset is set here.
        debug_assert!(
            op.offset.is_none(),
            "Limit offset without a Sort child is not supported; cascades should have folded into TopN"
        );

        Ok(child)
    }
```

- [ ] **Step 2: Compile and test.**

```bash
cd /Users/harbor/project/NovaRocks
cargo build 2>&1 | tail -5
cargo test 2>&1 | tail -10
```

Expected: clean build; all tests green. If a unit test elsewhere simulates "Limit on Sort" via hand-constructed PhysicalPlanNodes, rewrite that test to use `PhysicalTopN` instead (this is an explicit part of the cleanup).

- [ ] **Step 3: Commit.**

```bash
cd /Users/harbor/project/NovaRocks
git add src/sql/cascades/fragment_builder.rs
git commit -m "Remove use_top_n post-mutation from visit_limit

The cascades SortLimitToTopN rule now emits PhysicalTopN directly; Limit
on Sort should no longer appear in chosen physical plans. visit_limit is
reduced to applying the limit value to its child's top node, with a
debug_assert guarding the removed case.
"
```

---

## Task 9: Phase-1 Regression Verification

Confirm behavioral equivalence with Phase-0 and that TOP-N replaces the SORT+LIMIT pattern in TPC-DS EXPLAIN output.

**Files:**
- None (verification only)

- [ ] **Step 1: Rebuild and start the standalone server.**

```bash
cd /Users/harbor/project/NovaRocks
cargo build 2>&1 | tail -5
# make sure no stale instance is running
kill $(ps aux | grep 'standalone-server.*9030' | grep -v grep | awk '{print $2}') 2>/dev/null
NO_PROXY=127.0.0.1,localhost ./target/debug/novarocks standalone-server --port 9030 &
disown
# verify
NO_PROXY=127.0.0.1,localhost mysql -h 127.0.0.1 -P9030 -u root -e "SELECT 1;"
```

Expected: `SELECT 1` returns `1`.

- [ ] **Step 2: Recreate the Iceberg catalog (standalone server is stateless between restarts).**

```bash
NO_PROXY=127.0.0.1,localhost mysql -h 127.0.0.1 -P9030 -u root --batch -e "
CREATE EXTERNAL CATALOG IF NOT EXISTS iceberg_tpcds PROPERTIES(
  \"type\"=\"iceberg\",
  \"iceberg.catalog.type\"=\"hadoop\",
  \"iceberg.catalog.warehouse\"=\"oss://novarocks/iceberg-catalog/\",
  \"aws.s3.access_key\"=\"admin\",
  \"aws.s3.secret_key\"=\"admin123\",
  \"aws.s3.endpoint\"=\"http://127.0.0.1:9000\",
  \"aws.s3.enable_path_style_access\"=\"true\"
);"
```

If the server persists catalogs across restarts, this command is a no-op (idempotent thanks to `IF NOT EXISTS`).

- [ ] **Step 3: Capture 99 Phase-1 EXPLAIN snapshots.**

```bash
mkdir -p /tmp/novarocks-plan-compare/standalone-phase1
for i in $(seq 1 99); do
  qfile="/Users/harbor/project/NovaRocks/sql-tests/tpc-ds/sql/q${i}.sql"
  if [ -f "$qfile" ]; then
    sql=$(cat "$qfile")
    NO_PROXY=127.0.0.1,localhost mysql -h 127.0.0.1 -P9030 -u root --batch -N \
      -e "USE iceberg_tpcds.tpcds; EXPLAIN ${sql}" \
      > "/tmp/novarocks-plan-compare/standalone-phase1/q${i}.plan" 2>&1
    ret=$?
    if [ $ret -ne 0 ]; then echo "q${i}: FAILED"; fi
  fi
done
ls /tmp/novarocks-plan-compare/standalone-phase1/ | wc -l
```

Expected: `99` files; no `FAILED` lines.

- [ ] **Step 4: Compare snapshots — verify the expected change.**

```bash
# Count queries where the Phase-1 plan contains TOP-N and the Phase-0 plan contained SORT+LIMIT.
grep -l 'TOP-N' /tmp/novarocks-plan-compare/standalone-phase1/q*.plan | wc -l
echo "Phase-1 queries with TOP-N"

grep -lE 'SORT BY' /tmp/novarocks-plan-compare/standalone-phase0/q*.plan | wc -l
echo "Phase-0 queries with SORT BY (LIMIT may or may not be present)"

# Overall: every SORT immediately above a LIMIT should be replaced by TOP-N.
# Phase-0 plans had them as "LIMIT -> SORT BY"; Phase-1 should have "TOP-N".
grep -lE 'LIMIT \[limit=' /tmp/novarocks-plan-compare/standalone-phase0/q*.plan \
  | xargs -I{} sh -c 'q=$(basename {} .plan); grep -qE "LIMIT|SORT" /tmp/novarocks-plan-compare/standalone-phase1/${q}.plan && echo "{} may still have Limit+Sort"' \
  | wc -l
```

Expected: the TOP-N count in Phase-1 is roughly equal to the count of Phase-0 queries that had a `LIMIT` directly above a `SORT BY`. Concretely: Phase-1 plans should have `TOP-N` where Phase-0 had `LIMIT [limit=N] → SORT BY`; pure-LIMIT queries (no SORT) and pure-SORT queries (no LIMIT) stay unchanged.

- [ ] **Step 5: Verify non-expected changes are absent.**

```bash
# For 10 representative queries, diff the non-TOP-N/SORT/LIMIT lines.
# The expectation: those lines are identical.
for q in q3 q7 q15 q22 q52 q55 q77 q80 q96 q99; do
  diff \
    <(grep -vE '(TOP-N|SORT BY|LIMIT \[)' /tmp/novarocks-plan-compare/standalone-phase0/${q}.plan) \
    <(grep -vE '(TOP-N|SORT BY|LIMIT \[)' /tmp/novarocks-plan-compare/standalone-phase1/${q}.plan) \
    && echo "${q}: non-sort/limit lines identical" \
    || echo "${q}: UNEXPECTED DIFF — investigate"
done
```

Expected: all 10 print `identical`. Any `UNEXPECTED DIFF` must be investigated before the phase is considered complete.

- [ ] **Step 6: Run TPC-DS end-to-end verification.**

```bash
cd /Users/harbor/project/NovaRocks
NO_PROXY=127.0.0.1,localhost cargo run --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests -- \
  --suite tpc-ds --mode verify -j 4 2>&1 | tail -30
```

Expected: all passing (or matching the pre-Phase-1 pass rate if there are pre-existing failures unrelated to TopN).

- [ ] **Step 7: Stop the standalone server; record the Phase-1 sha.**

```bash
kill $(ps aux | grep 'standalone-server.*9030' | grep -v grep | awk '{print $2}')
echo "phase1 complete: $(git rev-parse HEAD) at $(date -Iseconds)" \
  >> /tmp/novarocks-plan-compare/phase0-ref.txt
```

- [ ] **Step 8: Final commit — documentation update.**

Append a "Phase 1 complete" note to the spec document so future readers can see the phase's actual outcome (not just the plan).

```bash
cd /Users/harbor/project/NovaRocks
# Edit docs/design/specs/2026-04-13-planner-layering-and-two-phase-agg-design.md
# Append at the end of section 4.1:
#   "Implementation landed on <sha> <date>. Observed: N queries switched from
#    SORT+LIMIT to TOP-N; TPC-DS verify suite green."
git add docs/design/specs/2026-04-13-planner-layering-and-two-phase-agg-design.md
git commit -m "Phase 1 TopN pilot: mark spec section 4.1 as landed"
```

---

## Done

At this point Phase 1 is complete:

- `LogicalTopN` and `PhysicalTopN` are first-class cascades operators.
- `SortLimitToTopN` transformation rule + `TopNToPhysical` implementation rule are registered.
- `fragment_builder.rs` `visit_physical_top_n` translates TOP-N to `SORT_NODE` with `use_top_n=true`.
- `fragment_builder.rs` `visit_limit` no longer performs post-mutation on SORT_NODE.
- TPC-DS EXPLAIN snapshots show the expected SORT+LIMIT → TOP-N substitution with no collateral changes.
- TPC-DS verify suite passes.

Next round: Phase 2 (join condition normalization, window decomposition, nullability widening) gets its own plan written using the same spec.
