# NovaRocks Optimizer Alignment with StarRocks FE — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Align the NovaRocks standalone SQL optimizer with StarRocks FE's core capabilities, targeting all 99 TPC-DS queries passing.

**Architecture:** The optimizer pipeline stays as-is (RBO → CTE Rewrite → Memo → Explore → Implement → Search → Extract). We upgrade the RBO join reorder from single-algorithm to adaptive 3-algorithm (DP/Greedy/LeftDeep), add systematic subquery unnesting via an Apply operator framework, improve statistics from Iceberg metadata, refine the cost model, and add predicate pushdown rules through joins.

**Tech Stack:** Rust, Arrow DataType, existing cascades framework in `src/sql/cascades/`, existing optimizer in `src/sql/optimizer/`.

**StarRocks Reference:** `~/worktree/starrocks/main/fe/fe-core/src/main/java/com/starrocks/sql/optimizer/`

**Validation:** TPC-DS suite via `cargo run --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests -- --suite tpc-ds --mode verify`

---

## Phase 1: Adaptive Join Reorder (DP + Greedy + LeftDeep)

**Target queries:** q13, q15, q16, q31, q48, q54, q70, q73, q74, q75, q76, q77 (optimizer timeout on multi-join queries)

**StarRocks reference:**
- `rule/join/JoinOrder.java` — base class, cost model, data structures
- `rule/join/JoinReorderDP.java` — DP algorithm with partition enumeration
- `rule/join/JoinReorderGreedy.java` — greedy with top-K pruning
- `rule/join/JoinReorderLeftDeep.java` — heuristic left-deep
- `rule/join/JoinReorderFactory.java` — adaptive algorithm selection
- `rule/join/MultiJoinNode.java` — multi-join graph flattening

**NovaRocks current code:**
- `src/sql/optimizer/join_reorder.rs` — DP up to 10 tables + heuristic fallback
- `src/sql/cascades/rewriter.rs` — RBO pre-pass calling join_reorder
- `src/sql/cascades/rules/join_associativity.rs` — cascades exploration (causes timeout)

### File Structure

| File | Action | Responsibility |
|------|--------|---------------|
| `src/sql/optimizer/join_reorder.rs` | Modify | Add greedy algorithm, left-deep algorithm, adaptive selection; raise DP limit to 16 |
| `src/sql/cascades/mod.rs` | Modify | Disable join associativity for >4 tables; raise timeout to 10s |
| `src/sql/cascades/rules/join_associativity.rs` | Modify | Add table count guard |

---

### Task 1.1: Greedy Join Reorder Algorithm

**Files:**
- Modify: `src/sql/optimizer/join_reorder.rs`

Reference: StarRocks `JoinReorderGreedy.java` — level-by-level enumeration, connect pairs with join predicates, keep best plan per group.

- [ ] **Step 1: Add greedy algorithm core structures**

Add to `src/sql/optimizer/join_reorder.rs` after the existing `DpEntry` struct:

```rust
/// Greedy join reorder: level-by-level, keep best plan per table subset.
/// Reference: StarRocks JoinReorderGreedy.java
fn greedy_join_reorder(
    graph: JoinGraph,
    table_stats: &HashMap<String, TableStatistics>,
) -> Option<LogicalPlan> {
    let n = graph.relations.len();
    if n < 2 { return None; }

    // groups[bitmask] = (plan, stats, cost)
    let mut groups: HashMap<u64, DpEntry> = HashMap::new();

    // Init level 1: single relations
    for (i, rel) in graph.relations.iter().enumerate() {
        let mask = 1u64 << i;
        let stats = estimate_relation_stats(rel, table_stats);
        let cost = stats.output_row_count;
        groups.insert(mask, DpEntry {
            plan: rel.clone(),
            stats,
            cumulative_cost: cost,
        });
    }

    // Level 2..n: greedily build joins
    for level in 2..=n {
        let prev_groups: Vec<(u64, DpEntry)> = groups
            .iter()
            .filter(|(mask, _)| mask.count_ones() as usize == level - 1)
            .map(|(&m, e)| (m, e.clone()))
            .collect();

        let atoms: Vec<(u64, DpEntry)> = groups
            .iter()
            .filter(|(mask, _)| mask.count_ones() == 1)
            .map(|(&m, e)| (m, e.clone()))
            .collect();

        for (left_mask, left_entry) in &prev_groups {
            for (right_mask, right_entry) in &atoms {
                if left_mask & right_mask != 0 { continue; }

                let join_mask = left_mask | right_mask;
                let preds = collect_predicates(&graph, *left_mask, *right_mask);
                if preds.is_empty() && level > 2 { continue; } // avoid cross join except pairs

                let (plan, stats, cost) = build_join_plan(
                    &left_entry, &right_entry, &preds, table_stats,
                );

                let entry = groups.entry(join_mask).or_insert_with(|| DpEntry {
                    plan: plan.clone(),
                    stats: stats.clone(),
                    cumulative_cost: f64::MAX,
                });
                if cost < entry.cumulative_cost {
                    entry.plan = plan;
                    entry.stats = stats;
                    entry.cumulative_cost = cost;
                }
            }
        }
    }

    // Return best full-set plan
    let full_mask = (1u64 << n) - 1;
    groups.remove(&full_mask).map(|e| e.plan)
}
```

- [ ] **Step 2: Add helper functions**

```rust
/// Collect predicates that connect left_mask and right_mask table sets.
fn collect_predicates(graph: &JoinGraph, left: u64, right: u64) -> Vec<TypedExpr> {
    let combined = left | right;
    graph.predicates.iter()
        .filter(|(_, mask)| {
            let m = *mask as u64;
            (m & combined) == m && (m & left) != 0 && (m & right) != 0
        })
        .map(|(expr, _)| expr.clone())
        .collect()
}

/// Build a join plan from left and right entries with given predicates.
/// Places smaller relation on the right (build side).
fn build_join_plan(
    left: &DpEntry,
    right: &DpEntry,
    preds: &[TypedExpr],
    table_stats: &HashMap<String, TableStatistics>,
) -> (LogicalPlan, Statistics, f64) {
    let condition = if preds.is_empty() {
        None
    } else {
        Some(conjoin_predicates(preds))
    };

    let join_kind = if condition.is_some() {
        JoinKind::Inner
    } else {
        JoinKind::Cross
    };

    // Smaller on the right (build side)
    let (l, r) = if left.stats.output_row_count < right.stats.output_row_count {
        (right, left)
    } else {
        (left, right)
    };

    let plan = LogicalPlan::Join(JoinNode {
        left: Box::new(l.plan.clone()),
        right: Box::new(r.plan.clone()),
        join_type: join_kind,
        condition,
    });

    let join_rows = estimate_join_rows(
        l.stats.output_row_count,
        r.stats.output_row_count,
        preds,
    );
    let stats = Statistics {
        output_row_count: join_rows,
        column_statistics: HashMap::new(),
    };

    // Cost = left_cumulative + right_cumulative + join_output_rows
    // Cross join penalty: 10x
    let mut self_cost = join_rows;
    if condition.is_none() {
        self_cost *= 10.0;
    }
    let total_cost = l.cumulative_cost + r.cumulative_cost + self_cost;

    (plan, stats, total_cost)
}
```

- [ ] **Step 3: Integrate into `reorder_joins_cbo`**

Modify the existing `reorder_joins_cbo` function to use adaptive algorithm selection:

```rust
pub(crate) fn reorder_joins_cbo(
    plan: LogicalPlan,
    table_stats: &HashMap<String, TableStatistics>,
) -> LogicalPlan {
    // ... existing recursive traversal ...
    // In the join chain handling section, replace:
    //   if relation_count > 10 { return heuristic }
    //   dp_join_reorder(graph)
    // With:
    let result = if relation_count <= 10 {
        // DP: optimal for small joins
        dp_join_reorder(graph.clone(), table_stats)
    } else {
        None
    };
    let result = result.or_else(|| {
        // Greedy: good for medium joins
        if relation_count <= 20 {
            greedy_join_reorder(graph.clone(), table_stats)
        } else {
            None
        }
    });
    let result = result.or_else(|| {
        // LeftDeep: always works
        left_deep_join_reorder(graph, table_stats)
    });
    // ... use result or fallback to heuristic ...
}
```

- [ ] **Step 4: Add left-deep join reorder**

```rust
/// Left-deep join reorder: greedy, sort by size, always produce left-deep tree.
/// Reference: StarRocks JoinReorderLeftDeep.java
fn left_deep_join_reorder(
    graph: JoinGraph,
    table_stats: &HashMap<String, TableStatistics>,
) -> Option<LogicalPlan> {
    let n = graph.relations.len();
    if n < 2 { return None; }

    // Sort relations by estimated row count descending (largest first)
    let mut indexed: Vec<(usize, f64)> = graph.relations.iter().enumerate()
        .map(|(i, rel)| {
            let rows = estimate_relation_stats(rel, table_stats).output_row_count;
            (i, rows)
        })
        .collect();
    indexed.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal));

    let mut used = vec![false; n];
    let first_idx = indexed[0].0;
    used[first_idx] = true;
    let mut left_plan = graph.relations[first_idx].clone();
    let mut left_mask = 1u64 << first_idx;

    for round in 1..n {
        // Find best next table: prefer equi-join connected
        let mut best_idx = None;
        for &(idx, _) in &indexed {
            if used[idx] { continue; }
            let right_mask = 1u64 << idx;
            let preds = collect_predicates(&graph, left_mask, right_mask);
            if !preds.is_empty() {
                best_idx = Some(idx);
                break;
            }
        }
        // Fallback: pick next unused
        let next_idx = best_idx.unwrap_or_else(|| {
            indexed.iter().find(|(i, _)| !used[*i]).unwrap().0
        });

        used[next_idx] = true;
        let right_mask = 1u64 << next_idx;
        let preds = collect_predicates(&graph, left_mask, right_mask);

        let condition = if preds.is_empty() { None } else { Some(conjoin_predicates(&preds)) };
        let join_kind = if condition.is_some() { JoinKind::Inner } else { JoinKind::Cross };

        left_plan = LogicalPlan::Join(JoinNode {
            left: Box::new(left_plan),
            right: Box::new(graph.relations[next_idx].clone()),
            join_type: join_kind,
            condition,
        });
        left_mask |= right_mask;
    }

    Some(left_plan)
}
```

- [ ] **Step 5: Test with timeout queries**

Run: `cargo build && cargo run -- standalone-server --port 9030` (in background)
Then: `cargo run --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests -- --suite tpc-ds --only q15,q16,q31 --mode verify --query-timeout 30`
Expected: Previously-timeout queries now produce plans (may still have data issues)

- [ ] **Step 6: Commit**

```bash
git add src/sql/optimizer/join_reorder.rs
git commit -m "feat(optimizer): adaptive join reorder with DP + Greedy + LeftDeep

Implements three join ordering algorithms aligned with StarRocks FE:
- DP (≤10 tables): optimal enumeration with memoization
- Greedy (≤20 tables): level-by-level with best-per-group
- LeftDeep (any): heuristic greedy left-deep tree

Adaptive selection tries DP first, falls back to Greedy, then LeftDeep."
```

---

### Task 1.2: Limit Cascades Exploration for Large Joins

**Files:**
- Modify: `src/sql/cascades/mod.rs`
- Modify: `src/sql/cascades/rules/join_associativity.rs`

- [ ] **Step 1: Add table count guard to join associativity**

In `src/sql/cascades/rules/join_associativity.rs`, modify the `matches` method to skip when the memo has too many join groups:

```rust
fn matches(&self, op: &Operator) -> bool {
    matches!(op, Operator::LogicalJoin(j) if j.join_type == JoinKind::Inner)
}

// Add a new method to Rule trait or check in the exploration loop:
// In mod.rs exploration phase, skip associativity when memo.groups.len() > 200
```

In `src/sql/cascades/mod.rs`, in the exploration loop:

```rust
// Before applying transformation rules, check group count
let skip_associativity = memo.groups.len() > 200;
for rule in &transformation_rules {
    if skip_associativity && rule.name() == "JoinAssociativity" {
        continue;
    }
    // ... existing rule application ...
}
```

- [ ] **Step 2: Raise optimizer timeout to 10s**

In `src/sql/cascades/mod.rs`:

```rust
const OPTIMIZE_TIMEOUT: Duration = Duration::from_secs(10);
```

- [ ] **Step 3: Test**

Run: `cargo run --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests -- --suite tpc-ds --only q54,q77,q38,q48,q70,q73,q74,q75,q76 --mode verify --query-timeout 30`

- [ ] **Step 4: Commit**

```bash
git add src/sql/cascades/mod.rs src/sql/cascades/rules/join_associativity.rs
git commit -m "feat(optimizer): limit cascades exploration for large joins

Skip JoinAssociativity rule when memo exceeds 200 groups to prevent
exponential blowup. Raise optimizer timeout from 3s to 10s.
RBO pre-pass greedy/DP handles join ordering for large queries."
```

---

## Phase 2: Subquery Unnesting via Apply Operator

**Target queries:** Additional timeout queries with complex subqueries; improve plan quality for IN/EXISTS/scalar subqueries beyond what the analyzer-level rewrite handles.

**StarRocks reference:**
- `rule/transformation/ScalarApply2JoinRule.java`
- `rule/transformation/ExistentialApply2JoinRule.java`
- `rule/transformation/QuantifiedApply2JoinRule.java`
- `rule/transformation/PushDownApply*.java`

**NovaRocks current code:**
- `src/sql/analyzer/subquery_rewrite.rs` — ad-hoc rewrites in analyzer

### Task 2.1: Add Apply (LogicalApply) Operator to the Plan

**Files:**
- Modify: `src/sql/plan/mod.rs` — add `Apply` variant to `LogicalPlan`
- Modify: `src/sql/cascades/operator.rs` — add `LogicalApply` operator
- Modify: `src/sql/cascades/convert.rs` — handle Apply conversion to memo
- Create: `src/sql/cascades/rules/apply_to_join.rs` — Apply→Join transformation rules

- [ ] **Step 1: Define Apply operator in plan and cascades**

In `src/sql/plan/mod.rs`, add:
```rust
pub(crate) struct ApplyNode {
    pub input: Box<LogicalPlan>,
    pub subquery: Box<LogicalPlan>,
    pub correlation_columns: Vec<TypedExpr>,
    pub subquery_kind: ApplyKind,  // Scalar, Existential, Quantified
}

pub(crate) enum ApplyKind {
    Scalar,           // (SELECT scalar FROM ...)
    Existential,      // EXISTS (SELECT ...)
    Quantified,       // col IN (SELECT ...)
}
```

In `src/sql/cascades/operator.rs`, add:
```rust
pub(crate) struct LogicalApplyOp {
    pub correlation_columns: Vec<TypedExpr>,
    pub subquery_kind: ApplyKind,
}
```

- [ ] **Step 2: Implement Apply→Join transformation rules**

Create `src/sql/cascades/rules/apply_to_join.rs`:

```rust
/// ScalarApply → CrossJoin or LeftOuterJoin
/// Reference: StarRocks ScalarApply2JoinRule.java
pub(crate) struct ScalarApplyToJoin;

impl Rule for ScalarApplyToJoin {
    fn name(&self) -> &str { "ScalarApplyToJoin" }
    fn rule_type(&self) -> RuleType { RuleType::Transformation }
    fn matches(&self, op: &Operator) -> bool {
        matches!(op, Operator::LogicalApply(a) if a.subquery_kind == ApplyKind::Scalar)
    }
    fn apply(&self, expr: &MExpr, memo: &mut Memo) -> Vec<NewExpr> {
        // If uncorrelated: CROSS JOIN
        // If correlated: LEFT OUTER JOIN on correlation keys
        // Add GROUP BY on correlation keys to subquery side
        todo!()
    }
}

/// ExistentialApply → LeftSemiJoin
/// Reference: StarRocks ExistentialApply2JoinRule.java
pub(crate) struct ExistentialApplyToJoin;

/// QuantifiedApply (IN) → LeftSemiJoin or LeftOuterJoin
/// Reference: StarRocks QuantifiedApply2JoinRule.java
pub(crate) struct QuantifiedApplyToJoin;
```

- [ ] **Step 3: Register rules and test**

In `src/sql/cascades/rules/mod.rs`, register the new Apply→Join rules in the transformation rules list.

- [ ] **Step 4: Migrate analyzer subquery rewriting to produce Apply nodes**

Gradually migrate `src/sql/analyzer/subquery_rewrite.rs` to produce `LogicalPlan::Apply` instead of directly constructing JOINs. This allows the cascades optimizer to explore multiple rewrite strategies.

- [ ] **Step 5: Commit**

---

## Phase 3: Statistics from Iceberg Metadata

**Target:** Accurate row counts and column NDV for cost model.

### Task 3.1: Read Iceberg table statistics

**Files:**
- Modify: `src/sql/statistics.rs` — enhance `build_table_statistics` to read Iceberg snapshot summary
- Modify: `src/sql/cascades/stats.rs` — fix CTE consume hardcoded 1000 rows

- [ ] **Step 1:** In `build_table_statistics`, use `snapshot.summary["total-records"]` from Iceberg metadata.
- [ ] **Step 2:** In `derive_cte_consume`, propagate the CTEProduce group's row_count instead of hardcoding 1000.
- [ ] **Step 3:** Commit

---

## Phase 4: Cost Model Refinement

**Target:** Better join distribution decisions (prevent broadcasting large tables).

### Task 4.1: Broadcast join threshold

**Files:**
- Modify: `src/sql/cascades/search.rs` — add broadcast row count limit
- Modify: `src/sql/cascades/cost.rs` — increase cross join penalty

- [ ] **Step 1:** In search.rs join distribution selection, add: if right_side_rows > 500_000, skip Broadcast option.
- [ ] **Step 2:** In cost.rs, increase NestLoop cost penalty and cross join penalty (10x → 100x, matching StarRocks `EXECUTE_COST_PENALTY`).
- [ ] **Step 3:** Commit

---

## Phase 5: Predicate Pushdown Through Joins

**Target:** Reduce intermediate data volume.

### Task 5.1: Push predicates through join ON clause

**Files:**
- Create: `src/sql/cascades/rules/push_down_predicate.rs`

- [ ] **Step 1:** Implement `PushDownPredicateThroughJoin` rule: when Filter is above Join, push equi-predicates referencing only one side down to that side.
- [ ] **Step 2:** Register rule and test.
- [ ] **Step 3:** Commit

---

## Phase 6: Aggregate Optimization

**Target:** Reduce shuffle data volume for aggregation queries.

### Task 6.1: Aggregate pushdown through join

- [ ] **Step 1:** When Aggregate is above Join and group-by keys are a subset of one side, push aggregate below join.
- [ ] **Step 2:** Commit

---

## Validation Checkpoints

After each phase, run:
```bash
cargo run --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests -- \
  --suite tpc-ds --mode verify --query-timeout 60
```

Track pass count progression:
| Phase | Expected Pass/99 |
|-------|-----------------|
| Current | 70 |
| Phase 1 | 80-85 |
| Phase 2 | 85-90 |
| Phase 3+4 | 90-95 |
| Phase 5+6 | 95-99 |
