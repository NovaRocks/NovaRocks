# Phase 1 RBO Framework Skeleton — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Stand up the RBO infrastructure (`RewriteRule` trait, fixed-point driver, `OptimizerOptions`, empty rule registry) and wire it into `cascades::optimize` AFTER the existing legacy `rewriter::rewrite` call. With an empty rule list the driver is a true no-op; this lets us verify the framework end-to-end without changing any plan output, before migrating real rules in subsequent phases.

**Architecture:** New `src/sql/cascades/rbo/` subdirectory holds the `RewriteRule` trait, the bottom-up + node-level + tree-level fixed-point driver, and the rule registry. `OptimizerOptions` lives at `src/sql/cascades/options.rs`. `cascades::optimize` calls the new driver immediately after the legacy `rewriter::rewrite` path; with an empty rule list this changes no behavior.

**Tech Stack:** Rust 2021, cascades framework in `src/sql/cascades/`, `cargo test` for unit tests.

**Spec reference:** `docs/design/specs/2026-04-13-unify-rbo-cbo-optimizer-design.md` §4.1.

---

## Task 0: Capture Phase-0 Baseline

Standalone EXPLAIN snapshots become the baseline for verifying that Phase 1 is a no-op. Reuse the snapshot scaffold from previous TopN/fragment_builder phases.

**Files:** scripts only; no code changes.

- [ ] **Step 1: Verify FE cluster is up.**

```bash
NO_PROXY=127.0.0.1,localhost mysql -h 127.0.0.1 -P9130 -u root --batch -e "SHOW BACKENDS;" 2>&1 | head -3
```

Expected: one row with `Alive=true`. If not, follow the `starrocks-fe-on-novarocks` skill to start FE and register the BE before proceeding.

- [ ] **Step 2: Build standalone and start it on port 9030.**

```bash
cd /Users/harbor/project/NovaRocks
cargo build 2>&1 | tail -3
kill $(ps aux | grep 'standalone-server.*9030' | grep -v grep | awk '{print $2}') 2>/dev/null
NO_PROXY=127.0.0.1,localhost ./target/debug/novarocks standalone-server --port 9030 > /tmp/standalone-baseline.log 2>&1 &
disown
NO_PROXY=127.0.0.1,localhost mysql -h 127.0.0.1 -P9030 -u root -e "SELECT 1;"
```

Expected: build succeeds; `SELECT 1` returns `1`.

- [ ] **Step 3: Recreate the Iceberg TPC-DS catalog.**

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

- [ ] **Step 4: Capture 99 standalone EXPLAIN plans into `standalone-unified-baseline/`.**

```bash
mkdir -p /tmp/novarocks-plan-compare/standalone-unified-baseline
for i in $(seq 1 99); do
  qfile="/Users/harbor/project/NovaRocks/sql-tests/tpc-ds/sql/q${i}.sql"
  if [ -f "$qfile" ]; then
    sql=$(cat "$qfile")
    NO_PROXY=127.0.0.1,localhost mysql -h 127.0.0.1 -P9030 -u root --batch -N \
      -e "USE iceberg_tpcds.tpcds; EXPLAIN ${sql}" \
      > "/tmp/novarocks-plan-compare/standalone-unified-baseline/q${i}.plan" 2>&1
  fi
done
ls /tmp/novarocks-plan-compare/standalone-unified-baseline/ | wc -l
```

Expected: `99`.

- [ ] **Step 5: Stop standalone server; record baseline reference.**

```bash
kill $(ps aux | grep 'standalone-server.*9030' | grep -v grep | awk '{print $2}')
echo "unified-baseline: $(cd /Users/harbor/project/NovaRocks && git rev-parse HEAD) at $(date -Iseconds)" \
  > /tmp/novarocks-plan-compare/unified-ref.txt
cat /tmp/novarocks-plan-compare/unified-ref.txt
```

Expected: file contents print one line with the current commit SHA and a timestamp.

---

## Task 1: Add `OptimizerOptions` Struct

Owns the per-optimize-call configuration: disabled rule names, iteration cap, deadline. Used by the driver in Task 3 and threaded through the pipeline in Task 5.

**Files:**
- Create: `src/sql/cascades/options.rs`
- Modify: `src/sql/cascades/mod.rs`
- Test: inline in `src/sql/cascades/options.rs`

- [ ] **Step 1: Write the failing tests.**

Create `src/sql/cascades/options.rs`:

```rust
//! Per-optimize-call configuration shared by the RBO and CBO drivers.

use std::collections::HashSet;
use std::time::Duration;

/// Controls which rules fire and bounds resource use.
///
/// Constructed once per `optimize()` call. Held by both the RBO driver
/// (`rbo::driver::rewrite_to_fixed_point`) and the CBO search loop. Rule
/// names live in a single namespace shared across `RewriteRule` (RBO) and
/// `Rule` (CBO); names must be unique across both trait families.
pub(crate) struct OptimizerOptions {
    disabled_rules: HashSet<String>,
    /// Hard cap on the RBO driver's tree-level fixed-point loop.
    pub rbo_max_iterations: usize,
    /// Hard cap on the CBO Memo group count (existing constant; documented here).
    pub cbo_max_groups: usize,
    /// Wall-clock budget for the entire `optimize()` call (existing constant; documented here).
    pub optimize_timeout: Duration,
}

impl OptimizerOptions {
    pub(crate) fn default_settings() -> Self {
        Self {
            disabled_rules: HashSet::new(),
            rbo_max_iterations: 32,
            cbo_max_groups: 5000,
            optimize_timeout: Duration::from_secs(10),
        }
    }

    pub(crate) fn is_enabled(&self, rule_name: &str) -> bool {
        !self.disabled_rules.contains(rule_name)
    }

    pub(crate) fn disable(&mut self, rule_name: &str) {
        self.disabled_rules.insert(rule_name.to_string());
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_enables_all_rules() {
        let opts = OptimizerOptions::default_settings();
        assert!(opts.is_enabled("AnyRuleName"));
        assert!(opts.is_enabled("PushDownPredicateScan"));
    }

    #[test]
    fn disable_blocks_named_rule_only() {
        let mut opts = OptimizerOptions::default_settings();
        opts.disable("PushDownPredicateScan");
        assert!(!opts.is_enabled("PushDownPredicateScan"));
        assert!(opts.is_enabled("PushDownPredicateProject"));
    }

    #[test]
    fn defaults_match_existing_optimizer_constants() {
        let opts = OptimizerOptions::default_settings();
        assert_eq!(opts.rbo_max_iterations, 32);
        assert_eq!(opts.cbo_max_groups, 5000);
        assert_eq!(opts.optimize_timeout, Duration::from_secs(10));
    }
}
```

- [ ] **Step 2: Declare the module in `src/sql/cascades/mod.rs`.**

In `src/sql/cascades/mod.rs`, add to the module declaration list (the block of `pub(crate) mod ...` lines near the top):

```rust
pub(crate) mod options;
```

(Insert in alphabetical position; the existing block lists modules in order. Place between `operator` and `physical_plan`.)

- [ ] **Step 3: Run tests to verify they pass.**

```bash
cd /Users/harbor/project/NovaRocks
cargo test --lib sql::cascades::options::tests 2>&1 | tail -10
```

Expected: 3 tests pass.

- [ ] **Step 4: Verify full crate still builds clean.**

```bash
cd /Users/harbor/project/NovaRocks
cargo build 2>&1 | tail -3
```

Expected: build succeeds; only pre-existing warnings.

- [ ] **Step 5: Commit.**

```bash
cd /Users/harbor/project/NovaRocks
git add src/sql/cascades/options.rs src/sql/cascades/mod.rs
git commit -m "Add OptimizerOptions for unified RBO+CBO configuration

Holds per-optimize-call settings: disabled rule name set, RBO iteration
cap, CBO group cap, and wall-clock timeout. Defaults preserve existing
optimizer constants (32 / 5000 / 10s). Disable API will be wired to a
session variable in a future round; for now, tests validate the lookup
behavior.

Spec §3.3.
"
```

---

## Task 2: Add `RewriteRule` Trait

Defines the contract for every RBO rule. Operates on a `LogicalPlan` node (no Memo). MUST NOT recurse into children — the driver in Task 3 owns traversal.

**Files:**
- Create: `src/sql/cascades/rbo/mod.rs`
- Create: `src/sql/cascades/rbo/rule.rs`
- Modify: `src/sql/cascades/mod.rs`
- Test: inline in `src/sql/cascades/rbo/rule.rs`

- [ ] **Step 1: Write the trait file.**

Create `src/sql/cascades/rbo/rule.rs`:

```rust
//! RBO rule trait. Operates directly on a LogicalPlan tree (no Memo).
//!
//! The driver (see `rbo::driver`) owns traversal; rules are pure local
//! rewrites. A rule's `apply` MUST NOT recurse into its children. The
//! driver visits children bottom-up, then applies all enabled rules at
//! each node to a node-level fixed-point.

use crate::sql::plan::LogicalPlan;

/// A logical-plan rewrite rule applied during the RBO phase.
pub(crate) trait RewriteRule: Send + Sync {
    /// Stable rule name used for `OptimizerOptions::is_enabled` lookups
    /// and trace logging. Must be unique across both `RewriteRule` (RBO)
    /// and `Rule` (CBO) namespaces.
    fn name(&self) -> &'static str;

    /// Cheap discriminant precheck. If false, `apply` is not called.
    /// Implementations should match on the operator kind without deep
    /// inspection or recursion.
    fn matches(&self, plan: &LogicalPlan) -> bool;

    /// Try to rewrite the plan rooted at this node.
    /// Return `Some(new_plan)` when the rule fires and produces a different
    /// shape; `None` when the rule is a no-op at this node.
    /// MUST NOT recurse into children.
    fn apply(&self, plan: LogicalPlan) -> Option<LogicalPlan>;
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sql::catalog::{TableDef, TableStorage};
    use crate::sql::plan::ScanNode;

    /// Test-only helper: a no-op rule that never fires.
    struct NoopRule;
    impl RewriteRule for NoopRule {
        fn name(&self) -> &'static str {
            "TestNoop"
        }
        fn matches(&self, _plan: &LogicalPlan) -> bool {
            false
        }
        fn apply(&self, _plan: LogicalPlan) -> Option<LogicalPlan> {
            None
        }
    }

    /// Test-only helper: a rule that fires once on Scan and turns it into
    /// itself (returns Some with structurally-identical input). Used to
    /// verify the driver detects "no observable change" via Option semantics.
    struct AlwaysFireOnScan;
    impl RewriteRule for AlwaysFireOnScan {
        fn name(&self) -> &'static str {
            "TestAlwaysFireOnScan"
        }
        fn matches(&self, plan: &LogicalPlan) -> bool {
            matches!(plan, LogicalPlan::Scan(_))
        }
        fn apply(&self, plan: LogicalPlan) -> Option<LogicalPlan> {
            // Return Some(plan) — driver treats Some as "changed".
            Some(plan)
        }
    }

    fn dummy_scan() -> LogicalPlan {
        LogicalPlan::Scan(ScanNode {
            database: "db".into(),
            table: TableDef {
                name: "t".into(),
                columns: vec![],
                storage: TableStorage::LocalParquetFile {
                    path: std::path::PathBuf::from("/tmp/t.parquet"),
                },
            },
            alias: None,
            columns: vec![],
            predicates: vec![],
            required_columns: None,
        })
    }

    #[test]
    fn noop_rule_does_not_match_or_apply() {
        let rule = NoopRule;
        let plan = dummy_scan();
        assert_eq!(rule.name(), "TestNoop");
        assert!(!rule.matches(&plan));
        assert!(rule.apply(plan).is_none());
    }

    #[test]
    fn always_fire_rule_matches_scan_only() {
        let rule = AlwaysFireOnScan;
        let scan = dummy_scan();
        assert!(rule.matches(&scan));
        assert!(rule.apply(scan).is_some());
    }
}
```

- [ ] **Step 2: Create the `rbo` module entry.**

Create `src/sql/cascades/rbo/mod.rs`:

```rust
//! Rule-based optimization (RBO) phase: tree-level fixed-point rewrites
//! over `LogicalPlan` before Memo insertion.
//!
//! Mirrors StarRocks FE's `TaskScheduler.rewriteIterative` model: a
//! shared `RewriteRule` trait, a single bottom-up driver, and a central
//! `RuleSet`. Rules are pure local rewrites; the driver owns traversal
//! and iteration.

pub(crate) mod rule;
```

- [ ] **Step 3: Declare the `rbo` submodule in `src/sql/cascades/mod.rs`.**

Add to the module declaration block, in alphabetical position:

```rust
pub(crate) mod rbo;
```

Place between `physical_plan` and `property` (alphabetical).

- [ ] **Step 4: Run tests.**

```bash
cd /Users/harbor/project/NovaRocks
cargo test --lib sql::cascades::rbo::rule::tests 2>&1 | tail -10
```

Expected: 2 tests pass (`noop_rule_does_not_match_or_apply`, `always_fire_rule_matches_scan_only`).

- [ ] **Step 5: Verify full crate builds clean.**

```bash
cd /Users/harbor/project/NovaRocks
cargo build 2>&1 | tail -3
```

Expected: clean build.

- [ ] **Step 6: Commit.**

```bash
cd /Users/harbor/project/NovaRocks
git add src/sql/cascades/rbo/mod.rs src/sql/cascades/rbo/rule.rs src/sql/cascades/mod.rs
git commit -m "Add RewriteRule trait for the RBO phase

The trait operates on a LogicalPlan node directly: matches on the
operator discriminant, and apply returns Some(rewritten) on change or
None on no-op. MUST NOT recurse — the upcoming RBO driver owns traversal
and per-node fixed-point iteration. Two test-only rule implementations
exercise the trait shape; real rules land in subsequent phases.

Spec §3.1.
"
```

---

## Task 3: Add RBO Driver

Implements `rewrite_to_fixed_point` plus its two helpers `apply_rules_one_pass` and `rewrite_children`. Bottom-up traversal; node-level fixed-point; tree-level fixed-point bounded by `OptimizerOptions::rbo_max_iterations`; deadline check each iteration.

**Files:**
- Create: `src/sql/cascades/rbo/driver.rs`
- Modify: `src/sql/cascades/rbo/mod.rs`
- Test: inline in `src/sql/cascades/rbo/driver.rs`

- [ ] **Step 1: Write the driver.**

Create `src/sql/cascades/rbo/driver.rs`:

```rust
//! Bottom-up tree rewriter that applies a list of `RewriteRule`s to a
//! `LogicalPlan` until a fixed point is reached.
//!
//! Iteration order:
//!   1. For each node, recursively rewrite children first.
//!   2. After children are stable, apply each enabled rule at this node
//!      and repeat at this node until no rule fires (node-level
//!      fixed-point).
//!   3. Repeat the whole bottom-up pass until no rule fires anywhere
//!      (tree-level fixed-point).
//!
//! The tree-level loop is bounded by `OptimizerOptions::rbo_max_iterations`
//! and by the optimize-call deadline.

use std::time::Instant;

use super::rule::RewriteRule;
use crate::sql::cascades::options::OptimizerOptions;
use crate::sql::plan::*;

/// Apply `rules` to `plan` until a fixed point is reached or a hard limit
/// is hit. Returns the rewritten plan on success, or an error string when
/// the optimize-call deadline is exceeded.
pub(crate) fn rewrite_to_fixed_point(
    mut plan: LogicalPlan,
    rules: &[Box<dyn RewriteRule>],
    options: &OptimizerOptions,
    deadline: Instant,
) -> Result<LogicalPlan, String> {
    for _round in 0..options.rbo_max_iterations {
        if Instant::now() > deadline {
            return Err(format!(
                "optimizer timeout during RBO: exceeded {}s budget",
                options.optimize_timeout.as_secs()
            ));
        }
        let (next, changed) = apply_rules_one_pass(plan, rules, options);
        plan = next;
        if !changed {
            break;
        }
    }
    Ok(plan)
}

fn apply_rules_one_pass(
    plan: LogicalPlan,
    rules: &[Box<dyn RewriteRule>],
    options: &OptimizerOptions,
) -> (LogicalPlan, bool) {
    // 1. Rewrite children first (bottom-up).
    let (plan, child_changed) = rewrite_children(plan, rules, options);

    // 2. Node-level fixed-point: apply enabled rules until none fires here.
    let mut current = plan;
    let mut local_changed = false;
    loop {
        let mut applied = false;
        for rule in rules {
            if !options.is_enabled(rule.name()) || !rule.matches(&current) {
                continue;
            }
            if let Some(next) = rule.apply(current.clone()) {
                current = next;
                local_changed = true;
                applied = true;
            }
        }
        if !applied {
            break;
        }
    }

    (current, child_changed || local_changed)
}

fn rewrite_children(
    plan: LogicalPlan,
    rules: &[Box<dyn RewriteRule>],
    options: &OptimizerOptions,
) -> (LogicalPlan, bool) {
    // For each variant with children, recurse on each child via
    // apply_rules_one_pass. OR-fold the per-child changed flags.
    // Leaf variants pass through unchanged.
    macro_rules! rec {
        ($child:expr) => {{
            let (out, ch) = apply_rules_one_pass(*$child, rules, options);
            (Box::new(out), ch)
        }};
    }

    match plan {
        // Leaves: no children.
        LogicalPlan::Scan(_)
        | LogicalPlan::Values(_)
        | LogicalPlan::GenerateSeries(_)
        | LogicalPlan::CTEConsume(_) => (plan, false),

        // Single-input variants.
        LogicalPlan::Filter(n) => {
            let (input, ch) = rec!(n.input);
            (
                LogicalPlan::Filter(FilterNode {
                    input,
                    predicate: n.predicate,
                }),
                ch,
            )
        }
        LogicalPlan::Project(n) => {
            let (input, ch) = rec!(n.input);
            (
                LogicalPlan::Project(ProjectNode {
                    input,
                    items: n.items,
                }),
                ch,
            )
        }
        LogicalPlan::Aggregate(n) => {
            let (input, ch) = rec!(n.input);
            (
                LogicalPlan::Aggregate(AggregateNode { input, ..n }),
                ch,
            )
        }
        LogicalPlan::Sort(n) => {
            let (input, ch) = rec!(n.input);
            (
                LogicalPlan::Sort(SortNode {
                    input,
                    items: n.items,
                }),
                ch,
            )
        }
        LogicalPlan::Limit(n) => {
            let (input, ch) = rec!(n.input);
            (
                LogicalPlan::Limit(LimitNode {
                    input,
                    limit: n.limit,
                    offset: n.offset,
                }),
                ch,
            )
        }
        LogicalPlan::Window(n) => {
            let (input, ch) = rec!(n.input);
            (LogicalPlan::Window(WindowNode { input, ..n }), ch)
        }
        LogicalPlan::SubqueryAlias(n) => {
            let (input, ch) = rec!(n.input);
            (
                LogicalPlan::SubqueryAlias(SubqueryAliasNode {
                    input,
                    alias: n.alias,
                    output_columns: n.output_columns,
                }),
                ch,
            )
        }
        LogicalPlan::Repeat(n) => {
            let (input, ch) = rec!(n.input);
            (
                LogicalPlan::Repeat(RepeatPlanNode {
                    input,
                    repeat_column_ref_list: n.repeat_column_ref_list,
                    grouping_ids: n.grouping_ids,
                    all_rollup_columns: n.all_rollup_columns,
                    grouping_fn_args: n.grouping_fn_args,
                }),
                ch,
            )
        }
        LogicalPlan::CTEProduce(n) => {
            let (input, ch) = rec!(n.input);
            (
                LogicalPlan::CTEProduce(CTEProduceNode {
                    cte_id: n.cte_id,
                    input,
                    output_columns: n.output_columns,
                }),
                ch,
            )
        }

        // Two-child variants.
        LogicalPlan::Join(n) => {
            let (left, lch) = rec!(n.left);
            let (right, rch) = rec!(n.right);
            (
                LogicalPlan::Join(JoinNode {
                    left,
                    right,
                    join_type: n.join_type,
                    condition: n.condition,
                }),
                lch || rch,
            )
        }
        LogicalPlan::CTEAnchor(n) => {
            let (produce, pch) = rec!(n.produce);
            let (consumer, cch) = rec!(n.consumer);
            (
                LogicalPlan::CTEAnchor(CTEAnchorNode {
                    cte_id: n.cte_id,
                    produce,
                    consumer,
                }),
                pch || cch,
            )
        }

        // N-child variants (set ops).
        LogicalPlan::Union(n) => {
            let mut changed = false;
            let inputs = n
                .inputs
                .into_iter()
                .map(|child| {
                    let (out, ch) = apply_rules_one_pass(child, rules, options);
                    if ch {
                        changed = true;
                    }
                    out
                })
                .collect();
            (LogicalPlan::Union(UnionNode { inputs, all: n.all }), changed)
        }
        LogicalPlan::Intersect(n) => {
            let mut changed = false;
            let inputs = n
                .inputs
                .into_iter()
                .map(|child| {
                    let (out, ch) = apply_rules_one_pass(child, rules, options);
                    if ch {
                        changed = true;
                    }
                    out
                })
                .collect();
            (LogicalPlan::Intersect(IntersectNode { inputs }), changed)
        }
        LogicalPlan::Except(n) => {
            let mut changed = false;
            let inputs = n
                .inputs
                .into_iter()
                .map(|child| {
                    let (out, ch) = apply_rules_one_pass(child, rules, options);
                    if ch {
                        changed = true;
                    }
                    out
                })
                .collect();
            (LogicalPlan::Except(ExceptNode { inputs }), changed)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sql::cascades::rbo::rule::RewriteRule;
    use crate::sql::catalog::{TableDef, TableStorage};
    use crate::sql::plan::{FilterNode, ScanNode};
    use std::sync::atomic::{AtomicUsize, Ordering};

    fn dummy_scan() -> LogicalPlan {
        LogicalPlan::Scan(ScanNode {
            database: "db".into(),
            table: TableDef {
                name: "t".into(),
                columns: vec![],
                storage: TableStorage::LocalParquetFile {
                    path: std::path::PathBuf::from("/tmp/t.parquet"),
                },
            },
            alias: None,
            columns: vec![],
            predicates: vec![],
            required_columns: None,
        })
    }

    #[test]
    fn empty_rule_list_returns_input_unchanged_after_one_round() {
        let plan = dummy_scan();
        let rules: Vec<Box<dyn RewriteRule>> = vec![];
        let options = OptimizerOptions::default_settings();
        let deadline = Instant::now() + std::time::Duration::from_secs(1);
        let out = rewrite_to_fixed_point(plan.clone(), &rules, &options, deadline)
            .expect("should succeed");
        // Structurally identical (Scan with same fields).
        match (plan, out) {
            (LogicalPlan::Scan(a), LogicalPlan::Scan(b)) => assert_eq!(a.table.name, b.table.name),
            _ => panic!("expected Scan plan unchanged"),
        }
    }

    #[test]
    fn driver_terminates_when_rule_fires_finitely() {
        // Rule fires up to N times, then stops. Driver must terminate.
        struct FireNTimes(AtomicUsize, usize);
        impl RewriteRule for FireNTimes {
            fn name(&self) -> &'static str {
                "TestFireNTimes"
            }
            fn matches(&self, plan: &LogicalPlan) -> bool {
                matches!(plan, LogicalPlan::Scan(_))
            }
            fn apply(&self, plan: LogicalPlan) -> Option<LogicalPlan> {
                let count = self.0.load(Ordering::Relaxed);
                if count >= self.1 {
                    None
                } else {
                    self.0.store(count + 1, Ordering::Relaxed);
                    Some(plan)
                }
            }
        }

        let rules: Vec<Box<dyn RewriteRule>> = vec![Box::new(FireNTimes(AtomicUsize::new(0), 3))];
        let options = OptimizerOptions::default_settings();
        let deadline = Instant::now() + std::time::Duration::from_secs(1);
        let out = rewrite_to_fixed_point(dummy_scan(), &rules, &options, deadline);
        assert!(out.is_ok(), "driver should terminate cleanly");
    }

    #[test]
    fn driver_recurses_through_filter_into_scan() {
        // A rule that targets Scan should be invoked at the Scan node even
        // when the root is Filter(Scan). Use an Arc<AtomicUsize> shared
        // between the test code and the rule so we can read back the visit
        // count after the rule has been boxed into the rules vec.
        use std::sync::Arc;

        struct OnScan(Arc<AtomicUsize>);
        impl RewriteRule for OnScan {
            fn name(&self) -> &'static str {
                "TestOnScan"
            }
            fn matches(&self, plan: &LogicalPlan) -> bool {
                matches!(plan, LogicalPlan::Scan(_))
            }
            fn apply(&self, _plan: LogicalPlan) -> Option<LogicalPlan> {
                self.0.fetch_add(1, Ordering::Relaxed);
                None // no change, but we counted the visit
            }
        }

        let counter = Arc::new(AtomicUsize::new(0));
        let rule = Box::new(OnScan(counter.clone()));
        let plan = LogicalPlan::Filter(FilterNode {
            input: Box::new(dummy_scan()),
            predicate: crate::sql::ir::TypedExpr {
                kind: crate::sql::ir::ExprKind::Literal(crate::sql::ir::LiteralValue::Bool(true)),
                data_type: arrow::datatypes::DataType::Boolean,
                nullable: false,
            },
        });
        let rules: Vec<Box<dyn RewriteRule>> = vec![rule];
        let options = OptimizerOptions::default_settings();
        let deadline = Instant::now() + std::time::Duration::from_secs(1);
        let _ = rewrite_to_fixed_point(plan, &rules, &options, deadline).unwrap();
        assert!(
            counter.load(Ordering::Relaxed) >= 1,
            "rule should have visited the Scan child"
        );
    }

    #[test]
    fn driver_returns_error_on_deadline_exceeded() {
        // Create a rule that always reports a change so the driver loops
        // until it hits the deadline. Set deadline to "already past" so it
        // hits immediately on the first iteration boundary check.
        struct AlwaysChange;
        impl RewriteRule for AlwaysChange {
            fn name(&self) -> &'static str {
                "TestAlwaysChange"
            }
            fn matches(&self, plan: &LogicalPlan) -> bool {
                matches!(plan, LogicalPlan::Scan(_))
            }
            fn apply(&self, plan: LogicalPlan) -> Option<LogicalPlan> {
                Some(plan)
            }
        }
        let rules: Vec<Box<dyn RewriteRule>> = vec![Box::new(AlwaysChange)];
        let options = OptimizerOptions::default_settings();
        let past_deadline = Instant::now() - std::time::Duration::from_secs(1);
        let result = rewrite_to_fixed_point(dummy_scan(), &rules, &options, past_deadline);
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("timeout"));
    }
}
```

- [ ] **Step 2: Add `driver` module to `rbo/mod.rs`.**

Edit `src/sql/cascades/rbo/mod.rs` to declare the new submodule:

```rust
pub(crate) mod driver;
pub(crate) mod rule;
```

(`driver` first alphabetically.)

- [ ] **Step 3: Run tests.**

```bash
cd /Users/harbor/project/NovaRocks
cargo test --lib sql::cascades::rbo::driver::tests 2>&1 | tail -15
```

Expected: 4 tests pass.

- [ ] **Step 4: Verify the whole crate still builds.**

```bash
cd /Users/harbor/project/NovaRocks
cargo build 2>&1 | tail -3
```

Expected: clean build.

- [ ] **Step 5: Commit.**

```bash
cd /Users/harbor/project/NovaRocks
git add src/sql/cascades/rbo/driver.rs src/sql/cascades/rbo/mod.rs
git commit -m "Add RBO driver: bottom-up + node-level + tree-level fixed-point

rewrite_to_fixed_point applies a list of RewriteRule to a LogicalPlan
until a fixed point. Three nested loops:
- rewrite_children: recurse into every child variant (Scan/Values/etc.
  are leaves; Filter/Project/Sort/Limit/Window/SubqueryAlias/Repeat/
  CTEProduce have one child; Join/CTEAnchor have two; Union/Intersect/
  Except have N).
- apply_rules_one_pass: at each node, after children are stable, apply
  each enabled rule until none fires here.
- rewrite_to_fixed_point: outer loop bounded by
  OptimizerOptions.rbo_max_iterations (default 32) and by the optimize-
  call deadline.

Spec §3.2.
"
```

---

## Task 4: Add Empty RuleSet Registry

Add the `rbo::rules` namespace and a single `all_rbo_rules()` function returning an empty vector. Keeps the wiring in Task 5 ergonomic and gives Phase 2/3 a clean place to extend.

**Files:**
- Create: `src/sql/cascades/rbo/rules/mod.rs`
- Modify: `src/sql/cascades/rbo/mod.rs`
- Test: inline

- [ ] **Step 1: Create the rules registry.**

Create `src/sql/cascades/rbo/rules/mod.rs`:

```rust
//! RBO rule registry. Phases 2-5 of the unification spec land their
//! migrated rules here; Phase 1 ships an empty registry so the framework
//! is wired end-to-end with no behavior change.

use super::rule::RewriteRule;

/// All RBO rules in canonical application order. Returns an empty vec
/// in Phase 1; Phase 2 (column_pruning) and Phase 3 (predicate_pushdown)
/// fill this in.
pub(crate) fn all_rbo_rules() -> Vec<Box<dyn RewriteRule>> {
    Vec::new()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn registry_is_empty_in_phase_1() {
        assert_eq!(all_rbo_rules().len(), 0);
    }
}
```

- [ ] **Step 2: Declare the submodule in `rbo/mod.rs`.**

Edit `src/sql/cascades/rbo/mod.rs`:

```rust
pub(crate) mod driver;
pub(crate) mod rule;
pub(crate) mod rules;
```

- [ ] **Step 3: Run the test.**

```bash
cd /Users/harbor/project/NovaRocks
cargo test --lib sql::cascades::rbo::rules::tests 2>&1 | tail -5
```

Expected: 1 test passes.

- [ ] **Step 4: Verify build.**

```bash
cd /Users/harbor/project/NovaRocks
cargo build 2>&1 | tail -3
```

Expected: clean build.

- [ ] **Step 5: Commit.**

```bash
cd /Users/harbor/project/NovaRocks
git add src/sql/cascades/rbo/rules/mod.rs src/sql/cascades/rbo/mod.rs
git commit -m "Add empty RBO rule registry placeholder

all_rbo_rules() returns Vec::new() in Phase 1. Phase 2 (column_pruning)
and Phase 3 (predicate_pushdown) populate this list as part of their
respective migrations. Wired into the optimize() pipeline in the next
task so the framework is end-to-end testable as a no-op before any
rules exist.

Spec §3.4.
"
```

---

## Task 5: Wire RBO Driver into `cascades::optimize`

The driver is invoked AFTER the existing `rewriter::rewrite` call. With an empty rule list, it visits every LogicalPlan node, invokes no rules (since the list is empty), and returns the input unchanged. This is intentionally a no-op for Phase 1; later phases activate it by populating `all_rbo_rules()` and removing the legacy `rewriter::rewrite` call piece by piece.

**Files:**
- Modify: `src/sql/cascades/mod.rs`
- Test: existing tests must stay green; Phase 1 final EXPLAIN snapshot diff is the integration test.

- [ ] **Step 1: Update the `optimize` pipeline.**

Edit `src/sql/cascades/mod.rs`. Find the `optimize()` function and the existing `rewriter::rewrite(...)` call (currently around line 52). Just after that line and before the CTE inline call, insert the new RBO driver invocation:

```rust
    // 1. RBO rewrite (legacy; will be progressively migrated to RBO rules below).
    let rewritten = rewriter::rewrite(plan, table_stats);

    // 1b. RBO rule-based rewriter (Phase 1: empty rule list = no-op;
    //     subsequent phases migrate predicate pushdown, column pruning, etc.
    //     from the legacy rewriter above into rules invoked here).
    let options = options::OptimizerOptions::default_settings();
    let rewritten = rbo::driver::rewrite_to_fixed_point(
        rewritten,
        &rbo::rules::all_rbo_rules(),
        &options,
        deadline,
    )?;

    // 2. CTE cleanup (existing).
    let cte_ctx = cte_rewrite::collect_cte_counts(&rewritten);
    let rewritten = cte_rewrite::inline_single_use_ctes(rewritten, &cte_ctx);
```

- [ ] **Step 2: Verify the full crate builds.**

```bash
cd /Users/harbor/project/NovaRocks
cargo build 2>&1 | tail -5
```

Expected: clean build.

- [ ] **Step 3: Run the entire test suite.**

```bash
cd /Users/harbor/project/NovaRocks
cargo test 2>&1 | grep "^test result" | tail -3
```

Expected: same passing count as the pre-Phase-1 baseline (currently 899 passed / 19 failed at HEAD `27a09ad`, where the 19 failures are pre-existing per the recent regression history). No new failures.

- [ ] **Step 4: Commit.**

```bash
cd /Users/harbor/project/NovaRocks
git add src/sql/cascades/mod.rs
git commit -m "Wire RBO driver into cascades::optimize as a no-op pass

Invoke rbo::driver::rewrite_to_fixed_point with the empty rule list
returned by rbo::rules::all_rbo_rules() immediately after the existing
legacy rewriter::rewrite call. Phase 1 introduces the framework end-to-
end: every LogicalPlan node is visited by the driver, no rules are
applied (the list is empty), and the plan passes through unchanged.

Phases 2-5 will populate the rule list and progressively delete legacy
calls from rewriter::rewrite until rewriter.rs itself can be removed
in Phase 6.

Spec §3.5.
"
```

---

## Task 6: Phase-1 Regression Verification

Confirm the framework is a true no-op: full unit tests still green, full crate build clean, and TPC-DS standalone EXPLAIN snapshots byte-identical to the Phase-0 baseline captured in Task 0.

**Files:** None — verification only, plus a final commit to record landing in the spec.

- [ ] **Step 1: Rebuild standalone and start it on port 9030.**

```bash
cd /Users/harbor/project/NovaRocks
cargo build 2>&1 | tail -3
kill $(ps aux | grep 'standalone-server.*9030' | grep -v grep | awk '{print $2}') 2>/dev/null
NO_PROXY=127.0.0.1,localhost ./target/debug/novarocks standalone-server --port 9030 > /tmp/standalone-phase1.log 2>&1 &
disown
NO_PROXY=127.0.0.1,localhost mysql -h 127.0.0.1 -P9030 -u root -e "SELECT 1;"
```

- [ ] **Step 2: Recreate the Iceberg catalog.**

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

- [ ] **Step 3: Capture Phase-1 EXPLAIN snapshots.**

```bash
mkdir -p /tmp/novarocks-plan-compare/standalone-unified-phase1
for i in $(seq 1 99); do
  qfile="/Users/harbor/project/NovaRocks/sql-tests/tpc-ds/sql/q${i}.sql"
  if [ -f "$qfile" ]; then
    sql=$(cat "$qfile")
    NO_PROXY=127.0.0.1,localhost mysql -h 127.0.0.1 -P9030 -u root --batch -N \
      -e "USE iceberg_tpcds.tpcds; EXPLAIN ${sql}" \
      > "/tmp/novarocks-plan-compare/standalone-unified-phase1/q${i}.plan" 2>&1
  fi
done
ls /tmp/novarocks-plan-compare/standalone-unified-phase1/ | wc -l
```

Expected: `99`.

- [ ] **Step 4: Diff against the baseline. Every plan must be byte-identical.**

```bash
echo "=== Queries that differ between baseline and Phase 1 ==="
diff_count=0
for i in $(seq 1 99); do
  if ! diff -q \
    /tmp/novarocks-plan-compare/standalone-unified-baseline/q${i}.plan \
    /tmp/novarocks-plan-compare/standalone-unified-phase1/q${i}.plan > /dev/null 2>&1; then
    echo "q${i}: DIFFERS — investigate"
    diff_count=$((diff_count + 1))
  fi
done
echo ""
echo "Total differing queries: $diff_count (expected: 0)"
```

Expected: `Total differing queries: 0`. Any non-zero count is a regression — the no-op driver should not change any plan.

If diffs appear, dump the first one for diagnosis:

```bash
for i in $(seq 1 99); do
  if ! diff -q /tmp/novarocks-plan-compare/standalone-unified-baseline/q${i}.plan /tmp/novarocks-plan-compare/standalone-unified-phase1/q${i}.plan > /dev/null 2>&1; then
    echo "=== q${i} diff (first 30 lines) ==="
    diff /tmp/novarocks-plan-compare/standalone-unified-baseline/q${i}.plan /tmp/novarocks-plan-compare/standalone-unified-phase1/q${i}.plan | head -30
    break
  fi
done
```

If a diff exists, halt the phase and investigate before committing the landing note.

- [ ] **Step 5: Stop standalone, record Phase-1 sha.**

```bash
kill $(ps aux | grep 'standalone-server.*9030' | grep -v grep | awk '{print $2}')
echo "unified-phase1 complete: $(cd /Users/harbor/project/NovaRocks && git rev-parse HEAD) at $(date -Iseconds)" \
  >> /tmp/novarocks-plan-compare/unified-ref.txt
cat /tmp/novarocks-plan-compare/unified-ref.txt
```

- [ ] **Step 6: Append landing note to the spec doc and commit.**

Capture the current commit SHA and the actual cargo-test pass/fail counts, then edit `docs/design/specs/2026-04-13-unify-rbo-cbo-optimizer-design.md`. At the end of section 4.1 (Phase 1 — RBO Framework Skeleton), append a paragraph using actual numbers from this run (substitute `<sha>`, `<pass-count>`, `<fail-count>` with values you observed):

```bash
cd /Users/harbor/project/NovaRocks
SHA=$(git rev-parse HEAD)
PASS=$(cargo test 2>&1 | grep -E "^test result:" | head -1 | sed -E 's/.*([0-9]+) passed.*/\1/')
FAIL=$(cargo test 2>&1 | grep -E "^test result:" | head -1 | sed -E 's/.*([0-9]+) failed.*/\1/')
echo "SHA=$SHA  pass=$PASS  fail=$FAIL"
```

Use those values to write the landing paragraph (open the spec file and add at the very end of section 4.1):

```markdown

**Phase 1 landed.** Date: 2026-04-13. HEAD at landing: <SHA>. RBO framework (RewriteRule trait, OptimizerOptions, fixed-point driver, empty rule registry) is wired into cascades::optimize as a no-op pass. All 99 TPC-DS standalone EXPLAIN snapshots are byte-identical to the pre-Phase-1 baseline. Unit tests: <PASS> passed / <FAIL> failed (no new failures vs the pre-Phase-1 baseline of 899 / 19). Phase 2 (column_pruning migration) is unblocked.
```

Then commit:

```bash
cd /Users/harbor/project/NovaRocks
git add docs/design/specs/2026-04-13-unify-rbo-cbo-optimizer-design.md
git commit -m "Phase 1 RBO framework skeleton: mark spec section 4.1 as landed"
```

---

## Done

After Phase 1 lands:

- `RewriteRule` trait, `OptimizerOptions`, fixed-point driver, and the empty rule registry are in place under `src/sql/cascades/rbo/`.
- `cascades::optimize` calls the new driver immediately after the legacy `rewriter::rewrite` call.
- With the empty rule list the driver is a true no-op; all 99 TPC-DS EXPLAIN snapshots are byte-identical to the baseline.
- Unit-test coverage exercises every key path: empty rule list, rule that fires N times, recursion through Filter into Scan, deadline-exceeded error.

Phase 2 (column_pruning migration) gets its own plan and consumes this framework: it adds `PruneColumns` to the registry, then deletes the corresponding `crate::sql::optimizer::column_pruning::prune_columns(plan)` line from `cascades/rewriter.rs`.
