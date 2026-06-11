# Phase 5+6 Join Reorder Migration + Legacy Deletion — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Move `join_reorder.rs`, `cardinality.rs`, and `cost.rs` from `src/sql/optimizer/` into the cascades RBO framework as a `JoinReorderRule` RewriteRule, delete `src/sql/optimizer/` entirely, and delete `cascades/rewriter.rs` — completing the optimizer unification. After this, `src/sql/cascades/` is the sole optimizer.

**Architecture:** `JoinReorderRule` implements `RewriteRule` (same pattern as `PruneColumns`): matches any LogicalPlan root, internally calls the existing `reorder_joins_cbo` algorithm unchanged. The rule stores `table_stats` as an `Arc<HashMap>` set during construction. `all_rbo_rules` gains a `table_stats` parameter so `cascades::optimize` can thread stats through. The three legacy algorithm files are moved verbatim into a `join_reorder/` sub-module under `rbo/rules/`. The two-pass RBO invocation (before + after join reorder) is preserved by running the RBO driver twice — once without JoinReorderRule (structural rules only), once with all rules including JoinReorderRule (catches post-reorder opportunities).

**Tech Stack:** Rust 2021, cascades RBO framework from Phase 1-4.

**Spec reference:** `docs/design/specs/2026-04-13-unify-rbo-cbo-optimizer-design.md` §4.5 + §4.6.

---

## Task 1: Move Algorithm Files into RBO Namespace

Move `join_reorder.rs`, `cardinality.rs`, and `cost.rs` verbatim into `src/sql/cascades/rbo/rules/join_reorder/` as a sub-module. Fix internal `use` paths. No behavior change — the legacy `rewriter.rs` still calls via the old path until Task 3.

**Files:**
- Create: `src/sql/cascades/rbo/rules/join_reorder/mod.rs`
- Create: `src/sql/cascades/rbo/rules/join_reorder/reorder.rs` (copy of join_reorder.rs)
- Create: `src/sql/cascades/rbo/rules/join_reorder/cardinality.rs` (copy of cardinality.rs)
- Create: `src/sql/cascades/rbo/rules/join_reorder/cost.rs` (copy of cost.rs)
- Modify: `src/sql/cascades/rbo/rules/mod.rs` (declare submodule)

- [ ] **Step 1: Create the sub-module directory and copy files.**

```bash
cd /Users/harbor/project/NovaRocks
mkdir -p src/sql/cascades/rbo/rules/join_reorder
cp src/sql/optimizer/join_reorder.rs src/sql/cascades/rbo/rules/join_reorder/reorder.rs
cp src/sql/optimizer/cardinality.rs src/sql/cascades/rbo/rules/join_reorder/cardinality.rs
cp src/sql/optimizer/cost.rs src/sql/cascades/rbo/rules/join_reorder/cost.rs
```

- [ ] **Step 2: Create the sub-module's `mod.rs`.**

Create `src/sql/cascades/rbo/rules/join_reorder/mod.rs`:

```rust
//! Join reorder — DP / Greedy / LeftDeep / Heuristic algorithms.
//!
//! Moved from `src/sql/optimizer/{join_reorder,cardinality,cost}.rs` during
//! Phase 5 of the optimizer unification. Algorithm logic is unchanged;
//! only the module paths have been updated.

pub(crate) mod cardinality;
pub(crate) mod cost;
pub(crate) mod reorder;

// Re-export the main entry point for convenience.
pub(crate) use reorder::reorder_joins_cbo;
```

- [ ] **Step 3: Fix import paths in `reorder.rs`.**

In `src/sql/cascades/rbo/rules/join_reorder/reorder.rs`, find:

```rust
use crate::sql::optimizer::cardinality;
use crate::sql::optimizer::cost;
```

Replace with:

```rust
use super::cardinality;
use super::cost;
```

Also check if `reorder.rs` uses `map_children` or other items from `crate::sql::optimizer` and fix those imports too. If `map_children` is used, copy it into `reorder.rs` as a private helper or import from `rbo::utils` if it's already there.

```bash
cd /Users/harbor/project/NovaRocks
grep -n 'crate::sql::optimizer' src/sql/cascades/rbo/rules/join_reorder/reorder.rs
```

Fix every hit.

- [ ] **Step 4: Fix import paths in `cardinality.rs`.**

In `src/sql/cascades/rbo/rules/join_reorder/cardinality.rs`, check for references to `crate::sql::optimizer::`:

```bash
grep -n 'crate::sql::optimizer' src/sql/cascades/rbo/rules/join_reorder/cardinality.rs
```

The `estimate_selectivity` function was moved to `crate::sql::cascades::stats` in Phase 4. If `cardinality.rs` references it via the old path, update to the new path. If it defines `estimate_selectivity` locally, check whether this is a stale copy vs the authoritative version in `cascades::stats`.

Fix every hit.

- [ ] **Step 5: Declare the sub-module in rules/mod.rs.**

Edit `src/sql/cascades/rbo/rules/mod.rs`. Add:

```rust
pub(crate) mod join_reorder;
```

alongside the existing `pub(crate) mod column_pruning;` and `pub(crate) mod predicate_pushdown;`.

- [ ] **Step 6: Verify the new module compiles.**

```bash
cd /Users/harbor/project/NovaRocks
cargo build 2>&1 | tail -10
```

Expected: build succeeds. There may be "unused" warnings for the new module since nothing calls it yet — that's fine.

- [ ] **Step 7: Commit.**

```bash
cd /Users/harbor/project/NovaRocks
git add src/sql/cascades/rbo/rules/join_reorder/
git add src/sql/cascades/rbo/rules/mod.rs
git commit -m "Copy join_reorder + cardinality + cost into cascades RBO namespace

Verbatim copy from src/sql/optimizer/ with only import path updates
(cardinality/cost use super::, not crate::sql::optimizer::). The legacy
files remain alive until Task 3 switches the call site. No behavior
change — the legacy rewriter.rs still calls the old path.

Phase 5 of optimizer unification, spec §4.5.
"
```

---

## Task 2: Create `JoinReorderRule` Wrapper

A RewriteRule that wraps `reorder_joins_cbo`. Like `PruneColumns`, it's a convention exception (internally recurses through the full join tree). Carries `table_stats` as `Arc<HashMap>`.

**Files:**
- Create: `src/sql/cascades/rbo/rules/join_reorder/rule.rs`
- Modify: `src/sql/cascades/rbo/rules/join_reorder/mod.rs`
- Test: inline in `rule.rs`

- [ ] **Step 1: Write the rule wrapper.**

Create `src/sql/cascades/rbo/rules/join_reorder/rule.rs`:

```rust
//! JoinReorderRule — RBO rule wrapping the DP/Greedy/LeftDeep/Heuristic
//! join reorder algorithms.
//!
//! **Convention exception.** Like PruneColumns, this rule recurses
//! internally: it takes the full plan tree, finds inner-join chains,
//! flattens them, runs cost-based reorder, and rebuilds. The RBO driver's
//! bottom-up traversal can't express global join-graph optimization.

use std::collections::HashMap;
use std::sync::Arc;

use crate::sql::cascades::rbo::rule::RewriteRule;
use crate::sql::plan::LogicalPlan;
use crate::sql::statistics::TableStatistics;

/// Wraps `reorder_joins_cbo` as a RewriteRule.
///
/// Stores `table_stats` internally (set at construction time by
/// `all_rbo_rules(table_stats)`).
pub(crate) struct JoinReorderRule {
    table_stats: Arc<HashMap<String, TableStatistics>>,
}

impl JoinReorderRule {
    pub(crate) fn new(table_stats: Arc<HashMap<String, TableStatistics>>) -> Self {
        Self { table_stats }
    }
}

impl RewriteRule for JoinReorderRule {
    fn name(&self) -> &'static str {
        "JoinReorder"
    }

    fn matches(&self, _plan: &LogicalPlan) -> bool {
        // Like PruneColumns, this rule takes the full tree and recurses
        // internally to find join chains. The driver invokes it at every
        // node bottom-up; the first invocation at the tree root does the
        // work; subsequent invocations at interior nodes are no-ops
        // (reorder_joins_cbo is idempotent on an already-reordered tree).
        true
    }

    fn apply(&self, plan: LogicalPlan) -> Option<LogicalPlan> {
        let before = plan.clone();
        let after = super::reorder::reorder_joins_cbo(plan, &self.table_stats);
        // Structural comparison to detect no-op.
        if format!("{:?}", before) == format!("{:?}", after) {
            None
        } else {
            Some(after)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sql::catalog::{TableDef, TableStorage};
    use crate::sql::ir::OutputColumn;
    use crate::sql::plan::ScanNode;
    use arrow::datatypes::DataType;

    fn dummy_scan(name: &str) -> LogicalPlan {
        LogicalPlan::Scan(ScanNode {
            database: "db".into(),
            table: TableDef {
                name: name.into(),
                columns: vec![],
                storage: TableStorage::LocalParquetFile {
                    path: std::path::PathBuf::from("/tmp/t.parquet"),
                },
            },
            alias: None,
            columns: vec![OutputColumn {
                name: "id".into(),
                data_type: DataType::Int32,
                nullable: false,
            }],
            predicates: vec![],
            required_columns: None,
        })
    }

    #[test]
    fn single_scan_is_no_op() {
        let rule = JoinReorderRule::new(Arc::new(HashMap::new()));
        let plan = dummy_scan("t1");
        assert!(rule.matches(&plan));
        assert!(rule.apply(plan).is_none(), "single scan should be no-op");
    }
}
```

- [ ] **Step 2: Declare the rule module.**

Edit `src/sql/cascades/rbo/rules/join_reorder/mod.rs`. Add:

```rust
pub(crate) mod rule;
```

after the existing `pub(crate) mod reorder;` line. Also add a re-export:

```rust
pub(crate) use rule::JoinReorderRule;
```

- [ ] **Step 3: Run the test.**

```bash
cd /Users/harbor/project/NovaRocks
cargo test --lib sql::cascades::rbo::rules::join_reorder::rule::tests 2>&1 | tail -10
```

Expected: 1 test passes.

- [ ] **Step 4: Commit.**

```bash
cd /Users/harbor/project/NovaRocks
git add src/sql/cascades/rbo/rules/join_reorder/rule.rs src/sql/cascades/rbo/rules/join_reorder/mod.rs
git commit -m "Add JoinReorderRule wrapper for reorder_joins_cbo

Implements RewriteRule; carries Arc<HashMap<String, TableStatistics>>
internally. Convention exception: recurses the full tree internally to
find and reorder inner-join chains (same pattern as PruneColumns).
Idempotent: returns None if the plan is already optimally ordered.

Phase 5 of optimizer unification, spec §4.5.
"
```

---

## Task 3: Thread `table_stats` Through Registry + Register + Delete Legacy Call

Three changes in one commit (keep build green at every commit):
1. Change `all_rbo_rules()` to take `table_stats` parameter.
2. Register `JoinReorderRule` in the returned rule list.
3. Replace the legacy `rewriter::rewrite` call with a second RBO driver pass.

**Files:**
- Modify: `src/sql/cascades/rbo/rules/mod.rs` (all_rbo_rules signature + registration)
- Modify: `src/sql/cascades/mod.rs` (thread table_stats; replace rewriter call)

- [ ] **Step 1: Update `all_rbo_rules` signature and register JoinReorderRule.**

Edit `src/sql/cascades/rbo/rules/mod.rs`:

```rust
//! RBO rule registry.

use std::collections::HashMap;
use std::sync::Arc;

use super::rule::RewriteRule;
use crate::sql::statistics::TableStatistics;

pub(crate) mod column_pruning;
pub(crate) mod join_reorder;
pub(crate) mod predicate_pushdown;

pub(crate) fn column_pruning_rules() -> Vec<Box<dyn RewriteRule>> {
    vec![Box::new(column_pruning::PruneColumns)]
}

/// Structural RBO rules that should run both before and after join reorder.
pub(crate) fn structural_rbo_rules() -> Vec<Box<dyn RewriteRule>> {
    let mut all = Vec::new();
    all.extend(column_pruning_rules());
    all.extend(predicate_pushdown::predicate_pushdown_rules());
    all
}

/// All RBO rules including join reorder. Requires table_stats for the
/// JoinReorderRule's CBO algorithm.
pub(crate) fn all_rbo_rules(
    table_stats: &HashMap<String, TableStatistics>,
) -> Vec<Box<dyn RewriteRule>> {
    let mut all = structural_rbo_rules();
    all.push(Box::new(join_reorder::JoinReorderRule::new(
        Arc::new(table_stats.clone()),
    )));
    all
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn registry_contains_expected_rules() {
        let rules = all_rbo_rules(&HashMap::new());
        assert_eq!(rules.len(), 7);
        let mut names: Vec<&str> = rules.iter().map(|r| r.name()).collect();
        names.sort();
        assert_eq!(
            names,
            vec![
                "JoinReorder",
                "PruneColumns",
                "PushDownPredicateAggregate",
                "PushDownPredicateJoin",
                "PushDownPredicateProject",
                "PushDownPredicateScan",
                "PushSemiAntiRightOnlyCondition",
            ]
        );
    }
}
```

- [ ] **Step 2: Update `cascades::optimize` to use unified RBO and remove legacy rewriter.**

Edit `src/sql/cascades/mod.rs`. Replace the current three-step pattern (RBO pass 1 → legacy rewriter → RBO pass 2) with:

```rust
    // 1. RBO rewriter — structural rules first (predicate pushdown + column
    //    pruning), then a second pass with ALL rules including join reorder.
    //    The first pass ensures filter predicates are attached to scans before
    //    join reorder evaluates cardinality; the second pass catches new
    //    predicate-push opportunities exposed by join reorder (mirrors the
    //    legacy "push, reorder, push" pattern).
    let options = options::OptimizerOptions::default_settings();
    let rewritten = rbo::driver::rewrite_to_fixed_point(
        plan,
        &rbo::rules::structural_rbo_rules(),
        &options,
        deadline,
    )?;
    let rewritten = rbo::driver::rewrite_to_fixed_point(
        rewritten,
        &rbo::rules::all_rbo_rules(table_stats),
        &options,
        deadline,
    )?;

    // 2. CTE cleanup: intentional pre-Memo structural rewrite.
    let cte_ctx = cte_rewrite::collect_cte_counts(&rewritten);
    let rewritten = cte_rewrite::inline_single_use_ctes(rewritten, &cte_ctx);
```

Remove the `rewriter::rewrite(...)` call entirely. If `rewriter` is the only module that imports from `crate::sql::optimizer::`, this unblocks full deletion in Task 4.

- [ ] **Step 3: Verify build + tests.**

```bash
cd /Users/harbor/project/NovaRocks
cargo build 2>&1 | tail -5
cargo test 2>&1 | grep "^test result:" | head -1
```

Expected: clean build; 945+ passed / 0 failed. May get "dead code" warnings for the old `src/sql/optimizer/` files since nothing imports them anymore — that's expected and will be cleaned in Task 4.

- [ ] **Step 4: Commit.**

```bash
cd /Users/harbor/project/NovaRocks
git add src/sql/cascades/rbo/rules/mod.rs src/sql/cascades/mod.rs
git commit -m "Register JoinReorderRule in RBO; replace legacy rewriter with unified RBO passes

all_rbo_rules() now takes table_stats and includes JoinReorderRule.
cascades::optimize runs two RBO driver passes:
  1. structural_rbo_rules() — PruneColumns + PushDownPredicate* only
  2. all_rbo_rules(table_stats) — structural rules + JoinReorder

This mirrors the legacy 'push, reorder, push' pattern. The legacy
cascades::rewriter::rewrite call is removed; nothing references
src/sql/optimizer/ anymore.

Phase 5 of optimizer unification, spec §4.5.
"
```

---

## Task 4: Delete `src/sql/optimizer/` and `cascades/rewriter.rs`

All code has been migrated. Delete the remnants.

**Files:**
- Delete: `src/sql/optimizer/` (entire directory)
- Delete: `src/sql/cascades/rewriter.rs`
- Modify: `src/sql/mod.rs` (remove `pub(crate) mod optimizer;`)
- Modify: `src/sql/cascades/mod.rs` (remove `pub(crate) mod rewriter;`)

- [ ] **Step 1: Verify no remaining references.**

```bash
cd /Users/harbor/project/NovaRocks
grep -rn 'crate::sql::optimizer::\|use crate::sql::optimizer\|mod optimizer;' src/ 2>/dev/null | grep -v target/
grep -rn 'rewriter::rewrite\|mod rewriter;' src/sql/cascades/ 2>/dev/null
```

Expected: no production references (may appear in comments or docs — those are fine to leave as historical notes, but code imports must be gone).

- [ ] **Step 2: Delete files.**

```bash
cd /Users/harbor/project/NovaRocks
rm -rf src/sql/optimizer/
rm src/sql/cascades/rewriter.rs
```

- [ ] **Step 3: Remove module declarations.**

Edit `src/sql/mod.rs` — find and delete: `pub(crate) mod optimizer;`

Edit `src/sql/cascades/mod.rs` — find and delete: `pub(crate) mod rewriter;`

- [ ] **Step 4: Build + test.**

```bash
cd /Users/harbor/project/NovaRocks
cargo build 2>&1 | tail -5
cargo test 2>&1 | grep "^test result:" | head -1
```

Expected: clean build (no more dead-code warnings for optimizer/); test count stable.

- [ ] **Step 5: Commit.**

```bash
cd /Users/harbor/project/NovaRocks
git add -u
git commit -m "Delete src/sql/optimizer/ and cascades/rewriter.rs — unification complete

All legacy optimizer passes have been migrated:
  - predicate_pushdown → PushDownPredicate* RBO rules (Phase 3)
  - column_pruning → PruneColumns RBO rule (Phase 2)
  - join_reorder → JoinReorderRule RBO rule (Phase 5)
  - cardinality + cost → join_reorder sub-module helpers
  - expr_utils → rbo/utils.rs (Phase 4)

src/sql/cascades/ is now the sole optimizer. The legacy rewriter.rs
pass-through is deleted.

Phase 6 of optimizer unification, spec §4.6.
"
```

---

## Task 5: Phase 5+6 Regression Verification

Confirm EXPLAIN snapshots are stable and TPC-DS suite still passes.

**Files:** None — verification only.

- [ ] **Step 1: Rebuild standalone and start on port 9030.**

```bash
cd /Users/harbor/project/NovaRocks
cargo build 2>&1 | tail -3
kill $(ps aux | grep 'standalone-server.*9030' | grep -v grep | awk '{print $2}') 2>/dev/null
NO_PROXY=127.0.0.1,localhost ./target/debug/novarocks standalone-server --port 9030 > /tmp/standalone-phase56.log 2>&1 &
disown
NO_PROXY=127.0.0.1,localhost mysql -h 127.0.0.1 -P9030 -u root -e "SELECT 1;"
```

- [ ] **Step 2: Recreate Iceberg catalog + capture EXPLAIN snapshots.**

```bash
NO_PROXY=127.0.0.1,localhost mysql -h 127.0.0.1 -P9030 -u root --batch -e "
CREATE EXTERNAL CATALOG IF NOT EXISTS iceberg_tpcds PROPERTIES(
  \"type\"=\"iceberg\",\"iceberg.catalog.type\"=\"hadoop\",
  \"iceberg.catalog.warehouse\"=\"oss://novarocks/iceberg-catalog/\",
  \"aws.s3.access_key\"=\"admin\",\"aws.s3.secret_key\"=\"admin123\",
  \"aws.s3.endpoint\"=\"http://127.0.0.1:9000\",
  \"aws.s3.enable_path_style_access\"=\"true\");"

mkdir -p /tmp/novarocks-plan-compare/standalone-unified-phase56
for i in $(seq 1 99); do
  qfile="/Users/harbor/project/NovaRocks/sql-tests/tpc-ds/sql/q${i}.sql"
  if [ -f "$qfile" ]; then
    sql=$(cat "$qfile")
    NO_PROXY=127.0.0.1,localhost mysql -h 127.0.0.1 -P9030 -u root --batch -N \
      -e "USE iceberg_tpcds.tpcds; EXPLAIN ${sql}" \
      > "/tmp/novarocks-plan-compare/standalone-unified-phase56/q${i}.plan" 2>&1
  fi
done
ls /tmp/novarocks-plan-compare/standalone-unified-phase56/ | wc -l
```

Expected: 99.

- [ ] **Step 3: Diff against Phase 4 baseline.**

```bash
diff_count=0
for i in $(seq 1 99); do
  if ! diff -q \
    /tmp/novarocks-plan-compare/standalone-unified-phase2/q${i}.plan \
    /tmp/novarocks-plan-compare/standalone-unified-phase56/q${i}.plan > /dev/null 2>&1; then
    echo "q${i}: DIFFERS"
    diff_count=$((diff_count + 1))
  fi
done
echo "Total differing queries: $diff_count"
```

Expected: 0 or small number. Join reorder moved from legacy to RBO with identical algorithm — plans should match. If any differ, examine and classify as ACCEPTABLE (cosmetic/order difference) or REGRESSION.

- [ ] **Step 4: Run TPC-DS end-to-end verify (serial, 60s timeout) — the Phase 7 final gate.**

```bash
cd /Users/harbor/project/NovaRocks
NO_PROXY=127.0.0.1,localhost cargo run --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests --release -- \
  --port 9030 --suite tpc-ds --mode verify -j 1 --query-timeout 60 2>&1 | tail -20
```

Expected: ≥98/99 pass (matching latest baseline). q22 expected to still timeout.

- [ ] **Step 5: Stop standalone, update spec, commit.**

```bash
kill $(ps aux | grep 'standalone-server.*9030' | grep -v grep | awk '{print $2}')
```

Append landing notes to both spec §4.5 (Phase 5) and §4.6 (Phase 6), then commit. Include actual SHA, test count, EXPLAIN diff count, and TPC-DS pass count.

```bash
cd /Users/harbor/project/NovaRocks
git add docs/design/specs/2026-04-13-unify-rbo-cbo-optimizer-design.md
git commit -m "Phase 5+6 join_reorder migration + legacy deletion: mark spec as landed"
```

---

## Done

After Phase 5+6 lands:

- `src/sql/optimizer/` is GONE. Zero lines of legacy optimizer.
- `src/sql/cascades/rewriter.rs` is GONE. No more legacy bridge.
- `src/sql/cascades/` is the sole optimizer:
  - RBO phase: `rbo/driver.rs` runs `structural_rbo_rules()` then `all_rbo_rules(table_stats)`
  - CBO phase: existing cascades memo + search
- Join reorder algorithm (DP/Greedy/LeftDeep/Heuristic) lives at `src/sql/cascades/rbo/rules/join_reorder/`
- All 7 RBO rules registered: PruneColumns, PushDownPredicate{Scan,Project,Join,Aggregate}, PushSemiAntiRightOnlyCondition, JoinReorder

Phase 7 of the spec is the final regression verification step — covered by Task 5 of this plan.
