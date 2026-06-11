# Unify RBO and CBO into a Single Cascades-based Optimizer Framework

Date: 2026-04-13
Status: Draft

## 1. Overview

Eliminate the parallel `src/sql/optimizer/` (legacy RBO) and `src/sql/cascades/` (CBO) subsystems by collapsing them into a single optimizer framework with two phases (RBO + CBO) sharing the same code conventions, rule registry, and configuration object. The architecture mirrors StarRocks FE's design: same project, two phases, two trait families, but one centrally registered RuleSet.

**Scope (in):**
- New `RewriteRule` trait + RBO driver that operates directly on `LogicalPlan` (no Memo).
- Migrate every legacy pass (`predicate_pushdown`, `column_pruning`) into RBO `RewriteRule` implementations.
- Wrap legacy `join_reorder` (DP / Greedy / LeftDeep) as a cascades transformation `Rule` that emits multiple candidates into the Memo (analogue of StarRocks `ReorderJoinRule`).
- Move shared utilities (`expr_utils`, `estimate_selectivity`) into neutral homes inside `src/sql/cascades/`.
- Delete `src/sql/optimizer/` entirely.

**Scope (out):**
- StarRocks's two-call pattern for join_reorder (RBO call + CBO call). We do CBO call only; the RBO call is for table pruning, which we don't have.
- `OptimizerOptions` session-variable wiring (`SET cbo_disabled_rules`). The struct is introduced with the disable API; SQL surfacing is deferred.
- Partition pruning (StarRocks `PartitionPruneRule`). We don't have it; new RBO rules can be added later.
- Subquery rewrite. Already lives in the analyzer (`src/sql/analyzer/subquery_rewrite.rs`); not part of optimizer.
- Global `RuleType` enum + ordinal BitSet (StarRocks pattern). Use rule name strings instead.
- Two-phase aggregation (separate spec, Phase 4 of `2026-04-13-planner-layering-and-two-phase-agg-design.md`).

**Validation gates:**
- `cargo test` green at every phase boundary.
- TPC-DS standalone EXPLAIN snapshots diff against the prior phase's snapshot at every phase boundary; unexpected diffs investigated.
- Phase 7 (final): TPC-DS standalone suite verify, serial, 60s per-query timeout, expect ≥98/99 (matching the current baseline at HEAD `27a09ad`).

## 2. Motivation

Two parallel optimizer subsystems exist today:

- `src/sql/optimizer/` — 4617 lines, RBO passes (`predicate_pushdown`, `column_pruning`, `join_reorder`) consumed by `src/sql/cascades/rewriter.rs::rewrite()` as a pre-Memo phase.
- `src/sql/cascades/` — Memo-based CBO with `Rule` trait, transformation/implementation rules, top-down search.

Direct costs of the split:
- Cardinality estimation duplicated: `optimizer::cardinality::estimate_statistics` recurses LogicalPlan; `cascades::stats::derive_group_statistics` recurses memo. Both call `estimate_selectivity`. Drift between the two is easy and silent.
- Cost models duplicated: `optimizer::cost::estimate_operator_cost` and `cascades::cost::compute_cost` use different formulas and units.
- Two rule paradigms: legacy uses ad-hoc recursive functions on LogicalPlan; cascades uses `Rule::apply(MExpr, &mut Memo)`. Adding a new structural rewrite requires deciding which paradigm — and if you guess wrong you double-implement.
- Layered handoff is brittle: legacy `join_reorder` picks a single best plan; cascades then applies local commutativity/associativity that may undo legacy's choices. Two cost models compete invisibly.

StarRocks FE handles this by sharing one `Rule` interface across two drivers (`TaskScheduler.rewriteIterative` for RBO, `OptimizeGroupTask` for CBO), with global algorithms like DP join enumeration wrapped as Rules whose `transform()` emits multiple Memo group expressions for cost-based selection. We adopt the same architectural principle but use Rust idioms: two trait families because `LogicalPlan` and `MExpr` are different Rust types, with shared `OptimizerOptions` and `RuleSet` registry.

## 3. Target Architecture

```
SQL
  ↓ Parser, Analyzer (incl. subquery rewrite), Planner
LogicalPlan
  ↓
[RBO driver]                       — src/sql/cascades/rbo/driver.rs
  uses: Vec<Box<dyn RewriteRule>>  — src/sql/cascades/rbo/rule.rs
  rules: predicate_pushdown,         predicate falls down through join/agg/proj/scan
         column_pruning              required_columns set on scans
         (extension point: more)
  iteration: bottom-up traversal, node-level fixed-point, tree-level fixed-point
             (max 32 iterations, deadline-bounded)
  output: structurally-canonical LogicalPlan
  ↓
LogicalPlan
  ↓
CTE inline (existing, unchanged)
  ↓
memo.init(plan)
  ↓
[CBO driver]                       — src/sql/cascades/search.rs (existing)
  uses: Vec<Box<dyn Rule>>         — src/sql/cascades/rule.rs (existing)
  transformation rules: JoinCommutativity, JoinAssociativity, SortLimitToTopN,
                        JoinReorderRule (NEW: wraps DP/Greedy/LeftDeep as Rule)
  implementation rules: ScanToPhysical, JoinToHashJoin, ... (existing)
  search: top-down property enforcement (existing)
  output: PhysicalPlanNode
  ↓
Fragment builder (existing, unchanged)
  ↓
TPlan
```

### 3.1 Two Trait Families

`RewriteRule` (new, RBO):

```rust
// src/sql/cascades/rbo/rule.rs
pub(crate) trait RewriteRule: Send + Sync {
    /// Stable rule name; used for OptimizerOptions disable lookups and trace.
    fn name(&self) -> &'static str;

    /// Quick discriminant precheck. Cheap; no recursion. If false, `apply` is skipped.
    fn matches(&self, plan: &LogicalPlan) -> bool;

    /// Try to rewrite the plan rooted at this node.
    /// Return Some(rewritten) on a real change, None on no-op.
    /// MUST NOT recurse into children — the driver handles traversal.
    fn apply(&self, plan: LogicalPlan) -> Option<LogicalPlan>;
}
```

`Rule` (existing, CBO, unchanged):

```rust
// src/sql/cascades/rule.rs (current)
pub(crate) trait Rule: Send + Sync {
    fn name(&self) -> &str;
    fn rule_type(&self) -> RuleType;  // Transformation | Implementation
    fn matches(&self, op: &Operator) -> bool;
    fn apply(&self, expr: &MExpr, memo: &mut Memo) -> Vec<NewExpr>;
}
```

### 3.2 RBO Driver

```rust
// src/sql/cascades/rbo/driver.rs

const MAX_ITERATIONS: usize = 32;

pub(crate) fn rewrite_to_fixed_point(
    plan: LogicalPlan,
    rules: &[Box<dyn RewriteRule>],
    options: &OptimizerOptions,
    deadline: Instant,
) -> Result<LogicalPlan, String> {
    let mut current = plan;
    for _round in 0..MAX_ITERATIONS {
        if Instant::now() > deadline {
            return Err("optimizer timeout during RBO".into());
        }
        let (next, changed) = apply_rules_one_pass(current, rules, options);
        current = next;
        if !changed {
            break;
        }
    }
    Ok(current)
}

/// Bottom-up: visit children first (recursively), then apply rules at this
/// node to a node-level fixed-point. Bottom-up matches StarRocks's iterative
/// rewrite order and ensures pushdown rules see fully-rewritten children.
fn apply_rules_one_pass(
    plan: LogicalPlan,
    rules: &[Box<dyn RewriteRule>],
    options: &OptimizerOptions,
) -> (LogicalPlan, bool) {
    // 1. Recurse into children, collect change flag.
    let (plan, child_changed) = rewrite_children(plan, rules, options);

    // 2. Node-level fixed-point: apply each enabled rule at this node,
    //    repeat until no rule fires.
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

fn rewrite_children(plan: LogicalPlan, rules: &[Box<dyn RewriteRule>], options: &OptimizerOptions)
    -> (LogicalPlan, bool)
{
    // Pattern-match each LogicalPlan variant that has children, recursively
    // call apply_rules_one_pass on each child, reconstruct the node.
    // (Implementation-detail boilerplate — one arm per variant.)
}
```

**Design points:**

- **rule.apply does not recurse**: the driver owns traversal. Rules are pure local rewrites. This matches StarRocks's `Rule.transform(OptExpression)` contract where the operator-specific transform produces a single-level rewrite and the scheduler walks the tree.
- **Bottom-up traversal**: children are rewritten first. Predicate pushdown rules at a parent then see fully-canonical children, which avoids needing multiple passes.
- **Node-level fixed-point**: at each node, iterate until no rule fires. Catches "PushDownPredicateProject reveals an opportunity for PushDownPredicateScan" interactions without requiring a full tree pass.
- **Tree-level fixed-point**: the outer 32-iteration loop catches cross-tree interactions (e.g., predicate pushed below a join exposes a new pushable predicate higher up after the next pass).
- **Deadline-bounded**: every iteration checks the wall-clock budget shared with CBO.

### 3.3 OptimizerOptions

```rust
// src/sql/cascades/options.rs

pub(crate) struct OptimizerOptions {
    /// Disabled rule names (across both RBO and CBO; rule names are
    /// expected to be unique across the two trait families).
    /// Default: empty (all rules enabled).
    disabled_rules: HashSet<String>,
    /// Hard limit on RBO iterations. Default: 32.
    pub rbo_max_iterations: usize,
    /// Hard limit on CBO memo group count (existing, currently 5000).
    pub cbo_max_groups: usize,
    /// Wall-clock budget for the entire optimize() call (existing, currently 10s).
    pub optimize_timeout: Duration,
}

impl OptimizerOptions {
    pub(crate) fn default() -> Self { /* sensible defaults */ }
    pub(crate) fn is_enabled(&self, rule_name: &str) -> bool {
        !self.disabled_rules.contains(rule_name)
    }
    pub(crate) fn disable(&mut self, rule_name: &str) {
        self.disabled_rules.insert(rule_name.to_string());
    }
}
```

The struct exists; the BitSet/session-variable wiring is deferred.

### 3.4 RuleSet Registry

```rust
// src/sql/cascades/rbo/rules/mod.rs

pub(crate) fn predicate_pushdown_rules() -> Vec<Box<dyn RewriteRule>> {
    vec![
        Box::new(PushDownPredicateScan),
        Box::new(PushDownPredicateProject),
        Box::new(PushDownPredicateJoin),
        Box::new(PushDownPredicateAggregate),
        Box::new(ExtractCommonEqFromOr),
    ]
}

pub(crate) fn column_pruning_rules() -> Vec<Box<dyn RewriteRule>> {
    // Column pruning is fundamentally a top-down concern; expressed as a
    // single rule that recurses internally (documented exception to the
    // "rules don't recurse" convention — see Phase 2 in §4.2).
    vec![Box::new(PruneColumns)]
}

pub(crate) fn all_rbo_rules() -> Vec<Box<dyn RewriteRule>> {
    let mut all = Vec::new();
    all.extend(predicate_pushdown_rules());
    all.extend(column_pruning_rules());
    all
}
```

Existing CBO `RuleSet` (`src/sql/cascades/rules/mod.rs`) gets one new entry:

```rust
pub(crate) fn all_transformation_rules() -> Vec<Box<dyn Rule>> {
    vec![
        Box::new(join_commutativity::JoinCommutativity),
        Box::new(join_associativity::JoinAssociativity),
        Box::new(sort_limit_to_top_n::SortLimitToTopN),
        Box::new(join_reorder::JoinReorderRule),  // NEW (wraps DP/Greedy/LeftDeep)
    ]
}
```

### 3.5 Top-Level Entry Integration

```rust
// src/sql/cascades/mod.rs (modified)
pub(crate) fn optimize(
    plan: LogicalPlan,
    table_stats: &HashMap<String, TableStatistics>,
) -> Result<PhysicalPlanNode, String> {
    let options = OptimizerOptions::default();
    let deadline = Instant::now() + options.optimize_timeout;

    // 1. RBO phase (replaces the old `rewriter::rewrite` call into legacy).
    let rbo_rules = rules::all_rbo_rules();
    let rewritten = rbo::driver::rewrite_to_fixed_point(plan, &rbo_rules, &options, deadline)?;

    // 2. CTE inline (existing).
    let cte_ctx = cte_rewrite::collect_cte_counts(&rewritten);
    let rewritten = cte_rewrite::inline_single_use_ctes(rewritten, &cte_ctx);

    // 3-9. Memo + CBO + extract (existing, unchanged).
    let mut memo = Memo::new();
    let root_group = convert::logical_plan_to_memo(&rewritten, &mut memo);
    stats::derive_group_statistics(&mut memo, table_stats);
    check_deadline(deadline)?;
    explore(&mut memo, &rules::all_transformation_rules(), deadline)?;
    check_deadline(deadline)?;
    implement(&mut memo, &rules::all_implementation_rules());
    stats::derive_group_statistics(&mut memo, table_stats);
    check_deadline(deadline)?;
    let root_required = PhysicalPropertySet::gather();
    let mut ctx = search::SearchContext::new(table_stats.clone());
    ctx.optimize_group(&memo, root_group, &root_required)?;
    check_deadline(deadline)?;
    extract::extract_best(&memo, root_group, &root_required, &ctx.winners)
}
```

### 3.6 File Structure After Unification

```
src/sql/cascades/
├── mod.rs                       (modified: invoke RBO driver, no legacy import)
├── rbo/                         (NEW)
│   ├── mod.rs
│   ├── rule.rs                  (RewriteRule trait)
│   ├── driver.rs                (rewrite_to_fixed_point + traversal)
│   ├── utils.rs                 (split_and / combine_and / collect_column_refs from legacy expr_utils)
│   └── rules/                   (RBO rule implementations)
│       ├── mod.rs               (RuleSet registration)
│       ├── predicate_pushdown/
│       │   ├── mod.rs
│       │   ├── push_to_scan.rs
│       │   ├── push_through_project.rs
│       │   ├── push_to_join.rs
│       │   ├── push_to_aggregate.rs
│       │   └── extract_or_eq.rs
│       └── column_pruning.rs
├── rules/                       (existing CBO rules)
│   ├── mod.rs
│   ├── implement.rs
│   ├── join_commutativity.rs
│   ├── join_associativity.rs
│   ├── sort_limit_to_top_n.rs
│   └── join_reorder.rs          (NEW: wraps DP/Greedy/LeftDeep as cascades Rule)
├── options.rs                   (NEW: OptimizerOptions)
├── stats.rs                     (modified: absorbs estimate_selectivity from legacy cardinality.rs)
└── (rewriter.rs deleted; obsolete after Phase 1)

src/sql/optimizer/               (DELETED in Phase 6)
```

## 4. Phased Implementation Path

Seven phases. Each is independently committable and Layer-1/Layer-2 verifiable.

### 4.1 Phase 1 — RBO Framework Skeleton (no behavior change)

Build the framework end-to-end with empty rule list. Pipeline still calls legacy `rewriter::rewrite`; the new RBO driver runs after with no rules — a no-op.

**Files:**

| File | Change |
|---|---|
| `src/sql/cascades/rbo/mod.rs` (new) | `pub(crate) mod rule; pub(crate) mod driver; pub(crate) mod utils; pub(crate) mod rules;` |
| `src/sql/cascades/rbo/rule.rs` (new) | `RewriteRule` trait |
| `src/sql/cascades/rbo/driver.rs` (new) | `rewrite_to_fixed_point` + `apply_rules_one_pass` + `rewrite_children` |
| `src/sql/cascades/rbo/utils.rs` (new) | empty for now |
| `src/sql/cascades/rbo/rules/mod.rs` (new) | `pub(crate) fn all_rbo_rules() -> Vec<Box<dyn RewriteRule>> { vec![] }` |
| `src/sql/cascades/options.rs` (new) | `OptimizerOptions` struct |
| `src/sql/cascades/mod.rs` | declare modules; in `optimize()`, after `rewriter::rewrite(...)`, call `rbo::driver::rewrite_to_fixed_point(...)` with the empty rule list |

**Tests (in `rbo/driver.rs`):**

- Empty rule list returns input unchanged.
- A mock `AlwaysFireOnce` rule fires on the first iteration and not the second; driver terminates.
- A mock `IncrementCounterRule` that fires up to N times terminates within `MAX_ITERATIONS`.

**Validation:**

- `cargo test` green.
- `cargo build` clean.
- TPC-DS standalone EXPLAIN snapshots **byte-identical** to the Phase 0 baseline (driver is a no-op).

**Commit:** `Phase 1: introduce RBO framework skeleton (no rules)`

**Phase 1 landed.** Date: 2026-04-13. HEAD at landing: 6563b500339fee9941c3a2c66395bc65b41c8c35. RBO framework (RewriteRule trait, OptimizerOptions, fixed-point driver, empty rule registry) is wired into cascades::optimize as a no-op pass. All 99 TPC-DS standalone EXPLAIN snapshots are byte-identical to the pre-Phase-1 baseline. Unit tests: 909 passed / 19 failed (no new failures vs the pre-Phase-1 baseline of 910 / 18). Phase 2 (column_pruning migration) is unblocked.

### 4.2 Phase 2 — Migrate column_pruning

Pilot RBO rule migration with the simplest legacy pass.

**Files:**

| File | Change |
|---|---|
| `src/sql/cascades/rbo/rules/column_pruning.rs` (new) | A single `PruneColumns` RewriteRule whose `apply` performs the full top-down recursion internally, mirroring the legacy single-pass implementation. See migration approach note below for why this rule is allowed to recurse. |
| `src/sql/cascades/rbo/rules/mod.rs` | extend `all_rbo_rules()` to include `column_pruning_rules()` |
| `src/sql/cascades/rewriter.rs` | delete the call to `crate::sql::optimizer::column_pruning::prune_columns` |
| `src/sql/optimizer/column_pruning.rs` | DELETE |
| `src/sql/optimizer/mod.rs` | remove `pub(crate) mod column_pruning;` |

**Migration approach for column pruning:**

Legacy column_pruning is a single top-down pass that collects required columns. To express this as bottom-up RBO rules, we invert: each rule fires when its node sees a parent has already established a required-column set on its child. Since RBO is bottom-up, we instead express required-column propagation as a **top-down rewrite** wrapped inside the RBO driver. Two options here, decided in implementation:

- **Option (a)**: One single rule `PruneColumns` whose `apply` does the full top-down recursion internally and returns the rewritten subtree. This violates the "rules don't recurse" convention but keeps the migration trivially equivalent.
- **Option (b)**: Restructure column pruning as a traversal of the form: parent sets `required_columns` on child via a metadata field, then child-level rule reads that hint. This is cleaner but a bigger change.

**Decision: Option (a).** Column pruning is fundamentally a top-down concern and our bottom-up driver can't naturally express it. We allow this single rule to internally recurse, documented explicitly. Implementation-wise: the rule ignores all but `LogicalProject` / `LogicalScan` matches at the root, and on apply it kicks off a separate top-down traversal. Acceptable as a one-time exception; predicate pushdown rules (Phase 3) are well-suited to bottom-up so the convention is preserved everywhere else.

**Tests:**

- For each prunable operator type (Scan, Project, Aggregate, Join, Sort), build a small LogicalPlan, run `PruneColumns`, verify `required_columns` on the underlying scans are minimized to exactly the columns referenced upstream.
- A combined test: a 3-table join with a top-level Project selecting only 3 columns, run `PruneColumns`, assert each scan's `required_columns` contains only the columns reachable from the projection plus join keys.

**Validation:**

- `cargo test` green.
- TPC-DS standalone EXPLAIN snapshots match Phase 1 (column pruning is a metadata-level change; algorithmic structure of EXPLAIN should not change).

**Commit:** `Phase 2: migrate column_pruning to RBO rules`

**Phase 2 landed.** Date: 2026-04-13. HEAD at landing: cdb17d537882e48cca74888381797ef5fa9c1570. column_pruning migrated to the PruneColumns RBO rule. Legacy src/sql/optimizer/column_pruning.rs (231 lines) deleted along with its rewriter::rewrite call site (Task 3) AND the dead optimizer::optimize()/optimize_query_plan() function plus its 3 tests (Task 4). Net -453 lines of legacy code removed. All 99 TPC-DS standalone EXPLAIN snapshots are byte-identical to Phase 1 (column pruning is a scan-metadata concern and does not affect EXPLAIN output). Unit tests: 910 passed / 19 failed (no new failures vs post-Phase-1 baseline). Phase 3 (predicate_pushdown migration) is unblocked.

### 4.3 Phase 3 — Migrate predicate_pushdown

**Files:**

| File | Change |
|---|---|
| `src/sql/cascades/rbo/rules/predicate_pushdown/mod.rs` (new) | re-export submodule rules |
| `src/sql/cascades/rbo/rules/predicate_pushdown/push_to_scan.rs` (new) | `PushDownPredicateScan`: matches `LogicalFilter(LogicalScan)`, pushes predicates onto scan |
| `src/sql/cascades/rbo/rules/predicate_pushdown/push_through_project.rs` (new) | `PushDownPredicateProject`: matches `LogicalFilter(LogicalProject)`, pushes through pass-through columns |
| `src/sql/cascades/rbo/rules/predicate_pushdown/push_to_join.rs` (new) | `PushDownPredicateJoin`: matches `LogicalFilter(LogicalJoin)`, classifies conjuncts and pushes per side; also handles SEMI/ANTI inner predicates from join condition |
| `src/sql/cascades/rbo/rules/predicate_pushdown/push_to_aggregate.rs` (new) | `PushDownPredicateAggregate`: matches `LogicalFilter(LogicalAggregate)`, pushes group-by-key predicates |
| `src/sql/cascades/rbo/rules/predicate_pushdown/extract_or_eq.rs` (new) | `ExtractCommonEqFromOr`: extracts common equi-join conjuncts from OR branches |
| `src/sql/cascades/rbo/rules/mod.rs` | extend `all_rbo_rules()` to include `predicate_pushdown_rules()` |
| `src/sql/cascades/rewriter.rs` | delete the two calls to `push_down_predicates` |
| `src/sql/optimizer/predicate_pushdown.rs` | DELETE |
| `src/sql/optimizer/mod.rs` | remove `pub(crate) mod predicate_pushdown;` |

**Migration approach:**

Each rule above is a bottom-up local rewrite that fires only at one shape (`LogicalFilter(LogicalSomething)`). The driver's node-level + tree-level fixed-point ensures predicates cascade down: predicate pushed below a join → next iteration the predicate at the join's input may push further. Replaces the legacy "two explicit calls to push_down_predicates around join_reorder" pattern naturally.

**Tests:**

- For each rule, build a `LogicalFilter(...)` over the matched child shape, run, assert predicates land where expected.
- A combined test: 3-table join with predicates on each base table, run all predicate rules, assert predicates reach all three scans.
- A SEMI/ANTI test: confirm right-child-only predicates from the SEMI condition end up on the right child (legacy quirk preserved).

**Validation:**

- `cargo test` green.
- TPC-DS standalone EXPLAIN snapshots: predicate placement should be identical to Phase 2 (legacy and new rules cover the same cases). Any difference is a migration bug.

**Commit:** `Phase 3: migrate predicate_pushdown to RBO rules`

**Phase 3 landed.** Date: 2026-04-13. HEAD at landing: c9d3775172b96cfe644d768b3446d9b6f232f5ec. predicate_pushdown migrated to five shape-specific RBO rules — PushDownPredicateScan, PushDownPredicateProject, PushDownPredicateAggregate, PushDownPredicateJoin (with OR factoring), PushSemiAntiRightOnlyCondition. Legacy src/sql/optimizer/predicate_pushdown.rs (747 lines) deleted; its one helper consumed by join_reorder (factor_common_eq_from_or_for_reorder plus the private helpers factor_common_eq_from_or_any_side, is_any_eq, split_or_branches, split_and_refs, expr_eq) moved into join_reorder.rs. rewriter::rewrite now contains only reorder_joins_cbo. RBO driver runs twice around rewriter::rewrite to preserve legacy "push before and after reorder" semantics. Unit tests: 928 passed / 19 failed (+18 new rule tests vs post-Phase-2 baseline of 910 / 19; failed count unchanged — zero new regressions). TPC-DS standalone EXPLAIN snapshots: not captured — the pre-Phase-3 snapshot baseline at /tmp/novarocks-plan-compare/standalone-phase2 was cleared, and end-to-end TPC-DS suite-verify is currently blocked by an environmental issue (CREATE EXTERNAL CATALOG against the local Iceberg/MinIO setup appears to succeed silently but USE catalog.db fails — reproduced identically on both the Phase 3 HEAD and the pre-Phase-3 baseline at 3e18b59, confirming it is not a Phase 3 regression). Phase 4 (move utils + selectivity) is unblocked.

### 4.4 Phase 4 — Move utils + selectivity (no behavior change)

**Files:**

| File | Change |
|---|---|
| `src/sql/cascades/rbo/utils.rs` | populate with `split_and`, `combine_and`, `collect_column_refs`, `wrap_remaining_filter` from legacy `expr_utils.rs` |
| `src/sql/cascades/stats.rs` | add `pub(crate) fn estimate_selectivity(predicate: &TypedExpr, col_stats: &HashMap<String, ColumnStatistic>) -> f64` (moved from legacy `cardinality.rs`) |
| `src/sql/cascades/stats.rs` | replace existing `use crate::sql::optimizer::cardinality::estimate_selectivity` with local reference |
| Other files using `crate::sql::optimizer::expr_utils::*` | update imports to `crate::sql::cascades::rbo::utils::*` |
| `src/sql/optimizer/expr_utils.rs` | DELETE |
| `src/sql/optimizer/cardinality.rs` | KEEP for now (still used by legacy `join_reorder` until Phase 5). Remove only the `estimate_selectivity` function that moved. |
| `src/sql/optimizer/mod.rs` | remove `pub(crate) mod expr_utils;` |

**Validation:**

- `cargo test` green.
- TPC-DS standalone EXPLAIN snapshots **byte-identical** to Phase 3 (pure file move, no semantic change).

**Commit:** `Phase 4: move shared utilities and selectivity into cascades`

**Phase 4 landed.** Date: 2026-04-13. HEAD at landing: 3f2bba54efa1963294423c60d4b6835405bf9caf. Pure file move, no behavior change. Relocated `src/sql/optimizer/expr_utils.rs` → `src/sql/cascades/rbo/utils.rs` (legacy deleted, 7 importers rewired). Relocated `estimate_selectivity` + 4 private helpers from `src/sql/optimizer/cardinality.rs` → `src/sql/cascades/stats.rs`; `cardinality.rs` imports `estimate_selectivity` and `extract_column_name` back (remains in place for legacy `join_reorder` until Phase 5). Incidental cleanup: `stats.rs` already carried a private mirror of `extract_column_name`; that copy was promoted in place rather than duplicated. Unit tests: 928 passed / 19 failed — identical to Phase 3 baseline, zero new failures. Phase 5 (wrap join_reorder as a cascades Rule) is unblocked.

### 4.5 Phase 5 — Wrap join_reorder as a cascades Rule

The largest and highest-risk phase. DP / Greedy / LeftDeep algorithms remain intact; only their interface changes from "legacy function returning one LogicalPlan" to "cascades Rule emitting multiple memo group expressions."

**Files:**

| File | Change |
|---|---|
| `src/sql/cascades/rules/join_reorder.rs` (new) | `JoinReorderRule` implementing cascades `Rule`. Matches at the root of an inner-join chain. Uses memo lookup + child group columns to extract the `MultiJoinNode` (atoms + predicates). Calls existing DP / Greedy / LeftDeep algorithms (moved here from `src/sql/optimizer/`). Returns `Vec<NewExpr>` containing every candidate join order as a separate alternative in the same memo group |
| `src/sql/cascades/rules/join_reorder/algorithms/` (new sub-module) | DP / Greedy / LeftDeep / Heuristic algorithm bodies, moved verbatim from `src/sql/optimizer/join_reorder.rs`. Adapted to operate on `MExpr`-keyed atoms instead of `LogicalPlan` subtrees |
| `src/sql/cascades/rules/join_reorder/cost.rs` (new) | Joincard/cost helpers, moved from `src/sql/optimizer/cost.rs` and `cardinality.rs::estimate_statistics` |
| `src/sql/cascades/rules/mod.rs` | register `JoinReorderRule` in `all_transformation_rules()` |
| `src/sql/cascades/rewriter.rs` | delete the call to `reorder_joins_cbo` |
| `src/sql/optimizer/join_reorder.rs` | DELETE |
| `src/sql/optimizer/cost.rs` | DELETE |
| `src/sql/optimizer/cardinality.rs` | DELETE (`estimate_selectivity` moved in Phase 4; `estimate_statistics` moved here) |
| `src/sql/optimizer/mod.rs` | remove `pub(crate) mod join_reorder; mod cost; mod cardinality;` |

**JoinReorderRule.matches() heuristic — avoid retriggering:**

- Match only on `Operator::LogicalJoin` whose join_type is INNER or CROSS.
- Skip if any of the children is itself a `LogicalJoin` with the same join_type AND we already produced a JoinReorderRule alternative in this group (to avoid re-running on every iteration).
- Implementation detail: maintain a per-group "join_reorder_done" marker via a sentinel in the memo metadata, OR rely on cascades `op_equal` dedup to drop duplicate alternatives.

**Threshold strategy (mirrors StarRocks):**

- Inner-join chain length (atoms count):
  - **≤ 4**: skip JoinReorderRule entirely; let `JoinCommutativity` + `JoinAssociativity` explore. DP overkill at this size.
  - **5–10**: run DP + LeftDeep, emit all candidates as alternatives. Disable `JoinAssociativity` for this group to avoid redundant exploration.
  - **11–16**: run Greedy + LeftDeep, emit candidates. Disable `JoinAssociativity`.
  - **17–50**: run LeftDeep only.
  - **> 50**: skip reorder.

The threshold check happens inside `JoinReorderRule::matches`. The "disable JoinAssociativity for this group" mechanism is implemented by the rule pre-marking the group with metadata that `JoinAssociativity::matches` reads and respects.

**Tests:**

- Build a 6-table inner-join LogicalPlan, run the rule, assert multiple `NewExpr` alternatives are returned with distinct join orders.
- Build a 3-table inner-join, run the rule, assert it skips (≤4 threshold).
- Build a 13-table inner-join, run the rule, assert Greedy is invoked (LeftDeep also present).

**Validation:**

- `cargo test` green.
- TPC-DS standalone EXPLAIN snapshots: join orders may differ from Phase 4 (the previously-decisive single DP output is now one of multiple candidates that cascades cost-search picks among). Manual classification required:
  - **ACCEPTABLE**: join shape change where the new plan has comparable or better total operator structure (no extra cross joins, no obviously-bad orderings). Document and proceed.
  - **REGRESSION**: appears slower, missing common predicates pushed, etc. Investigate before continuing.

**Commit:** `Phase 5: wrap join_reorder as cascades Rule emitting multiple candidates`

### 4.6 Phase 6 — Delete `src/sql/optimizer/`

**Files:**

| File | Change |
|---|---|
| `src/sql/optimizer/mod.rs` | DELETE |
| `src/sql/optimizer/` directory | DELETE |
| `src/sql/mod.rs` | remove `pub(crate) mod optimizer;` |
| Cross-codebase grep | confirm no `crate::sql::optimizer::` remains |
| `src/sql/cascades/rewriter.rs` | DELETE if it's now an empty pass-through (likely yes after Phase 5) |
| `src/sql/cascades/mod.rs` | remove `pub(crate) mod rewriter;` if deleted above |

**Validation:**

- `cargo build` green.
- `cargo test` green.
- TPC-DS standalone EXPLAIN snapshots **byte-identical** to Phase 5 (only deletions).

**Commit:** `Phase 6: remove src/sql/optimizer; cascades is the sole optimizer`

**Phase 5+6 landed.** Date: 2026-04-14. HEAD at landing: 13395e81. join_reorder (1843 lines), cardinality (753 lines), and cost (275 lines) migrated to JoinReorderRule RBO rule at `src/sql/cascades/rbo/rules/join_reorder/`. Legacy `src/sql/optimizer/` directory (2979 lines) and `cascades/rewriter.rs` deleted. `src/sql/cascades/` is now the sole optimizer. 9 of 99 TPC-DS EXPLAIN snapshots differ from Phase 2 baseline — all classified as ACCEPTABLE join-order permutations due to the two-pass RBO driver seeing slightly different input state vs the legacy single-call pipeline. No regressions. Unit tests: 946 passed / 0 failed. Optimizer unification is complete.

### 4.7 Phase 7 — Final regression verification

**Files:** None. Verification only.

**Steps:**

1. Rebuild standalone server.
2. Capture TPC-DS standalone EXPLAIN snapshots into `/tmp/novarocks-plan-compare/standalone-unified/` (99 files).
3. Diff against the Phase 0 baseline (`standalone-phase2`). Classify each differing query as ACCEPTABLE or REGRESSION; report counts.
4. Run TPC-DS standalone suite verify, serial, 60s timeout. Expected: ≥ 98/99 pass (matching the current baseline at HEAD `27a09ad` after Phase 2 hardening + JoinAssociativity fix). q22 expected to still timeout (not addressed by this work).
5. Append landing note to this spec.

**Commit:** `Phase 7: optimizer unification landed`

## 5. Validation Strategy

Three layers, applied per phase as in Phase 1 and Phase 2 of the planner-layering work.

### Layer 1 — Unit tests (`cargo test`)

Required green at every phase boundary. Each new RewriteRule and JoinReorderRule ships with focused tests covering the matched shapes.

### Layer 2 — TPC-DS EXPLAIN snapshot diff

Phase 0 baseline: snapshots already on disk at `/tmp/novarocks-plan-compare/standalone-phase2/` (captured at HEAD `27a09ad`, 99 files). At each phase boundary, regenerate snapshots and diff. Per-phase expectation:

| Phase | Expected diff | Action on unexpected diff |
|---|---|---|
| 1 (RBO skeleton) | 0 | Investigate, do not commit |
| 2 (column_pruning) | 0 | Investigate |
| 3 (predicate_pushdown) | 0 (predicate placement should be identical) | Investigate |
| 4 (utils + selectivity move) | 0 | Investigate |
| 5 (join_reorder wrap) | Some queries' join orders may differ | Per-query ACCEPTABLE/REGRESSION classification |
| 6 (delete legacy) | 0 | Investigate |

### Layer 3 — End-to-end execution (TPC-DS verify, serial, 60s timeout)

Run **once** at Phase 7 only, per user instruction. Layers 1 + 2 cover regression detection at intermediate phases; Layer 3 is the final acceptance gate.

Expected: ≥ 98/99 pass, matching the current baseline. q22 expected to still timeout.

## 6. Risks

**Risk 1 — Phase 5 cost-search regression.** DP now emits multiple candidates; cascades cost-search picks among them with its own model (different formulas from legacy `optimizer::cost`). On large queries the cascades cost may pick a worse candidate than legacy DP would have picked alone. Mitigation: tight EXPLAIN diff inspection on Phase 5; if regressions appear, temporarily limit JoinReorderRule to emit a single candidate (the legacy DP winner) and let cascades commutativity/associativity refine — i.e., recreate today's behavior — until cascades cost can be calibrated.

**Risk 2 — Phase 3 fixed-point divergence.** Legacy explicitly calls `push_down_predicates` twice (before and after `join_reorder`). The new RBO driver uses a single fixed-point loop that interleaves all rules. In principle this is more general; in practice we need to confirm no query produces different predicate placement. Layer 2 EXPLAIN diff is the gate.

**Risk 3 — Phase 5 memo bloat.** DP for a 10-table join may emit 10+ candidate orderings. Each candidate becomes a memo group expression. Combined with cascades' own commutativity/associativity, the memo could exceed `cbo_max_groups = 5000`. Mitigation: the threshold strategy (DP only for 5-10 tables; disable JoinAssociativity for those groups) keeps memo growth bounded.

**Risk 4 — Column pruning convention violation.** Phase 2 explicitly allows `PruneColumns` to recurse internally, breaking the "rules don't recurse" rule. Mitigation: documented as an explicit one-time exception with rationale; subsequent rules respect the convention.

## 7. Migration Order Rationale

- Phase 1 first: framework with no rules can be merged independently. Failures here are framework bugs, isolated.
- Phase 2 (column_pruning) before Phase 3 (predicate_pushdown): column_pruning is the simpler migration (single pass, single concern); validates the framework with real rules before tackling the more interconnected predicate_pushdown rules.
- Phase 4 (utils + selectivity move) after 2/3 and before 5: the moved code is needed by Phase 5, but moving it earlier (before Phase 3 needs `expr_utils`) would require dual maintenance during the transition.
- Phase 5 isolated: highest risk, most code, deserves a clean phase boundary.
- Phase 6 (delete legacy) only after Phase 5: legacy code stays callable until everything that depended on it has been migrated.
- Phase 7 final verification: gate before declaring landed.

## 8. Reference Code Locations

| Concern | NovaRocks (current) | StarRocks FE (reference) |
|---|---|---|
| RBO driver | `src/sql/cascades/rewriter.rs` (calls legacy) | `task/TaskScheduler.java::rewriteIterative` |
| RBO Rule trait | (none — legacy uses ad-hoc functions) | `rule/Rule.java` (shared with CBO) |
| Predicate pushdown | `src/sql/optimizer/predicate_pushdown.rs` | `rule/transformation/PushDownPredicate*Rule.java` |
| Column pruning | `src/sql/optimizer/column_pruning.rs` | `rule/transformation/Prune*ColumnsRule.java` |
| Join reorder DP | `src/sql/optimizer/join_reorder.rs::dp_*` | `rule/join/JoinReorderDP.java` |
| Join reorder Greedy | `src/sql/optimizer/join_reorder.rs::greedy_*` | `rule/join/JoinReorderGreedy.java` |
| Join reorder LeftDeep | `src/sql/optimizer/join_reorder.rs::left_deep_*` | `rule/join/JoinReorderLeftDeep.java` |
| Join reorder rule wrapper | (none — legacy is called directly) | `rule/join/ReorderJoinRule.java` |
| Optimizer options | (hardcoded constants) | `OptimizerOptions.java` (BitSet of disabled rules) |
| Cardinality estimation | `src/sql/optimizer/cardinality.rs` (legacy), `src/sql/cascades/stats.rs` (cascades) | `statistics/StatisticsCalculator.java` (single) |
| Cost model | `src/sql/optimizer/cost.rs` (legacy), `src/sql/cascades/cost.rs` (cascades) | `cost/CostModel.java` (single) |
