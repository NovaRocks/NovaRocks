# Cascades Phase 2+3 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace the current 3-pass optimizer with a Cascades framework that produces PhysicalPlan with distribution properties, and a StarRocks-aligned PlanFragmentBuilder visitor that performs fragment splitting and Thrift emission in a single pass.

**Architecture:** LogicalPlan enters the Cascades optimizer via RBO rewrite then Memo-based CBO search. The Memo explores logical equivalences (join reorder) and generates physical alternatives (HashJoin with Shuffle/Broadcast, two-phase aggregation). Top-down property enforcement inserts PhysicalDistribution enforcers. `extract_best` produces a PhysicalPlan tree. A `PlanFragmentBuilder` visitor walks the PhysicalPlan, creating fragment boundaries at PhysicalDistribution nodes, and emitting Thrift TPlan per fragment — reusing existing `physical/nodes.rs` helpers. The output feeds into the existing `ExecutionCoordinator`.

**Tech Stack:** Rust, existing Thrift types (`plan_nodes`, `data_sinks`, `descriptors`), existing `physical/nodes.rs` helpers, existing `ExecutionCoordinator`

**Regression gate:** TPC-DS 99/99 after switching engine.rs to the new pipeline.

---

## File Map

### New files (src/sql/cascades/)

| File | Responsibility |
|------|----------------|
| `src/sql/cascades/mod.rs` | `optimize()` entry, re-exports |
| `src/sql/cascades/memo.rs` | Memo, Group, MExpr, GroupId, MExprId |
| `src/sql/cascades/operator.rs` | Operator enum (logical + physical variants) |
| `src/sql/cascades/property.rs` | PhysicalPropertySet, DistributionSpec, OrderingSpec |
| `src/sql/cascades/rule.rs` | Rule trait, RuleType, Pattern |
| `src/sql/cascades/rules/mod.rs` | Rule registration |
| `src/sql/cascades/rules/implement.rs` | Implementation rules (logical -> physical) |
| `src/sql/cascades/rules/join_commutativity.rs` | JoinCommutativity transform |
| `src/sql/cascades/rules/join_associativity.rs` | JoinAssociativity transform |
| `src/sql/cascades/rules/agg_split.rs` | AggSplit transform (single -> local+global) |
| `src/sql/cascades/cost.rs` | Cost model for physical operators |
| `src/sql/cascades/stats.rs` | Statistics derivation for Memo groups |
| `src/sql/cascades/rewriter.rs` | RBO rewrite entry (predicate pushdown + column pruning) |
| `src/sql/cascades/search.rs` | Top-down optimization with property enforcement |
| `src/sql/cascades/extract.rs` | extract_best: Memo -> PhysicalPlan tree |
| `src/sql/cascades/convert.rs` | LogicalPlan -> Memo insertion |
| `src/sql/cascades/fragment_builder.rs` | PlanFragmentBuilder visitor |
| `src/sql/cascades/physical_plan.rs` | PhysicalPlan tree (extracted from Memo) |

### Modified files

| File | Change |
|------|--------|
| `src/sql/mod.rs` | Add `pub(crate) mod cascades;` |
| `src/sql/explain.rs` | Add `explain_physical_plan()` for PhysicalPlan |
| `src/standalone/engine.rs` | Switch `execute_query` + `explain_query` to Cascades pipeline |
| `src/standalone/coordinator.rs` | Accept `BuildResult` from fragment_builder |

---

### Task 1: Cascades data structures — Memo, Group, MExpr, Operator

**Files:**
- Create: `src/sql/cascades/mod.rs`
- Create: `src/sql/cascades/memo.rs`
- Create: `src/sql/cascades/operator.rs`
- Modify: `src/sql/mod.rs`

This task creates the core Memo data structure and the Operator enum.

- [ ] **Step 1: Create cascades module entry**

Create `src/sql/cascades/mod.rs`:

```rust
//! Cascades optimizer framework.

pub(crate) mod memo;
pub(crate) mod operator;

pub(crate) use memo::{GroupId, MExprId, Memo};
pub(crate) use operator::Operator;
```

- [ ] **Step 2: Create Operator enum**

Create `src/sql/cascades/operator.rs` with all logical and physical operator variants. Each variant holds the operator-specific data (expressions, join type, agg mode, etc.) directly — no child references (children are in MExpr.children as GroupIds).

The logical operator structs mirror the corresponding `LogicalPlan` node fields, minus the `input`/`left`/`right` child references. The physical operator structs add physical decisions (JoinDistribution, AggMode, DistributionSpec).

Key physical operators:
- `PhysicalHashJoinOp { join_type, eq_conditions, other_condition, distribution: JoinDistribution }`
- `PhysicalNestLoopJoinOp { join_type, condition }`
- `PhysicalHashAggregateOp { mode: AggMode, group_by, aggregates, output_columns }`
- `PhysicalDistributionOp { spec: DistributionSpec }` (enforcer node)
- `PhysicalSortOp { items }`

Enum variants:
- Logical: `LogicalScan`, `LogicalFilter`, `LogicalProject`, `LogicalAggregate`, `LogicalJoin`, `LogicalSort`, `LogicalLimit`, `LogicalWindow`, `LogicalUnion`, `LogicalIntersect`, `LogicalExcept`, `LogicalCTEProduce`, `LogicalCTEConsume`, `LogicalRepeat`, `LogicalValues`, `LogicalGenerateSeries`, `LogicalSubqueryAlias`
- Physical: `PhysicalScan`, `PhysicalFilter`, `PhysicalProject`, `PhysicalHashJoin`, `PhysicalNestLoopJoin`, `PhysicalHashAggregate`, `PhysicalSort`, `PhysicalLimit`, `PhysicalWindow`, `PhysicalDistribution`, `PhysicalCTEProduce`, `PhysicalCTEConsume`, `PhysicalRepeat`, `PhysicalUnion`, `PhysicalIntersect`, `PhysicalExcept`, `PhysicalValues`, `PhysicalGenerateSeries`, `PhysicalSubqueryAlias`

Include `Operator::is_logical()` and `Operator::is_physical()` methods.

- [ ] **Step 3: Create Memo, Group, MExpr**

Create `src/sql/cascades/memo.rs`:

```rust
use super::operator::Operator;
use crate::sql::statistics::Statistics;

pub(crate) type GroupId = usize;
pub(crate) type MExprId = usize;
pub(crate) type Cost = f64;

pub(crate) struct Memo {
    pub(crate) groups: Vec<Group>,
}

pub(crate) struct Group {
    pub(crate) id: GroupId,
    pub(crate) logical_exprs: Vec<MExpr>,
    pub(crate) physical_exprs: Vec<MExpr>,
    /// Logical properties derived from first logical expr.
    pub(crate) logical_props: Option<LogicalProperties>,
}

pub(crate) struct LogicalProperties {
    pub(crate) output_columns: Vec<crate::sql::ir::OutputColumn>,
    pub(crate) row_count: f64,
}

pub(crate) struct MExpr {
    pub(crate) id: MExprId,
    pub(crate) op: Operator,
    pub(crate) children: Vec<GroupId>,
}

impl Memo {
    pub(crate) fn new() -> Self { Self { groups: Vec::new() } }

    pub(crate) fn new_group(&mut self, expr: MExpr) -> GroupId {
        let id = self.groups.len();
        let is_physical = expr.op.is_physical();
        let mut group = Group {
            id,
            logical_exprs: Vec::new(),
            physical_exprs: Vec::new(),
            logical_props: None,
        };
        if is_physical {
            group.physical_exprs.push(expr);
        } else {
            group.logical_exprs.push(expr);
        }
        self.groups.push(group);
        id
    }

    pub(crate) fn add_expr_to_group(&mut self, group_id: GroupId, expr: MExpr) {
        let group = &mut self.groups[group_id];
        if expr.op.is_physical() {
            group.physical_exprs.push(expr);
        } else {
            group.logical_exprs.push(expr);
        }
    }

    pub(crate) fn next_expr_id(&self) -> MExprId {
        self.groups.iter()
            .map(|g| g.logical_exprs.len() + g.physical_exprs.len())
            .sum()
    }
}
```

- [ ] **Step 4: Register cascades module**

In `src/sql/mod.rs`, add:
```rust
pub(crate) mod cascades;
```

- [ ] **Step 5: Build and verify**

Run: `cargo build 2>&1 | tail -3`
Expected: Build succeeds.

- [ ] **Step 6: Commit**

```bash
git add src/sql/cascades/ src/sql/mod.rs
git commit -m "feat(cascades): memo, group, mexpr, and operator data structures"
```

---

### Task 2: Physical properties — DistributionSpec, OrderingSpec, PhysicalPropertySet

**Files:**
- Create: `src/sql/cascades/property.rs`
- Modify: `src/sql/cascades/mod.rs`

- [ ] **Step 1: Create property types**

Create `src/sql/cascades/property.rs`:

```rust
//! Physical properties for Cascades optimizer.

/// A column reference used in distribution/ordering specs.
/// Uses column name (not TypedExpr) for hashability.
#[derive(Clone, Debug, Hash, Eq, PartialEq)]
pub(crate) struct ColumnRef {
    pub qualifier: Option<String>,
    pub column: String,
}

#[derive(Clone, Debug, Hash, Eq, PartialEq)]
pub(crate) struct PhysicalPropertySet {
    pub distribution: DistributionSpec,
    pub ordering: OrderingSpec,
}

impl PhysicalPropertySet {
    pub fn any() -> Self {
        Self {
            distribution: DistributionSpec::Any,
            ordering: OrderingSpec::Any,
        }
    }

    pub fn gather() -> Self {
        Self {
            distribution: DistributionSpec::Gather,
            ordering: OrderingSpec::Any,
        }
    }
}

#[derive(Clone, Debug, Hash, Eq, PartialEq)]
pub(crate) enum DistributionSpec {
    Any,
    Gather,
    HashPartitioned(Vec<ColumnRef>),
}

impl DistributionSpec {
    pub fn satisfies(&self, required: &DistributionSpec) -> bool {
        match required {
            DistributionSpec::Any => true,
            DistributionSpec::Gather => matches!(self, DistributionSpec::Gather),
            DistributionSpec::HashPartitioned(req_cols) => {
                if let DistributionSpec::HashPartitioned(my_cols) = self {
                    my_cols == req_cols
                } else {
                    false
                }
            }
        }
    }
}

#[derive(Clone, Debug, Hash, Eq, PartialEq)]
pub(crate) enum OrderingSpec {
    Any,
    Required(Vec<SortKey>),
}

#[derive(Clone, Debug, Hash, Eq, PartialEq)]
pub(crate) struct SortKey {
    pub column: ColumnRef,
    pub asc: bool,
    pub nulls_first: bool,
}

impl OrderingSpec {
    pub fn satisfies(&self, required: &OrderingSpec) -> bool {
        match required {
            OrderingSpec::Any => true,
            OrderingSpec::Required(req_keys) => {
                if let OrderingSpec::Required(my_keys) = self {
                    // Provided ordering must be a prefix-or-equal match
                    my_keys.len() >= req_keys.len()
                        && my_keys.iter().zip(req_keys).all(|(m, r)| m == r)
                } else {
                    false
                }
            }
        }
    }
}

impl PhysicalPropertySet {
    pub fn satisfies(&self, required: &PhysicalPropertySet) -> bool {
        self.distribution.satisfies(&required.distribution)
            && self.ordering.satisfies(&required.ordering)
    }
}
```

- [ ] **Step 2: Register in mod.rs**

Add to `src/sql/cascades/mod.rs`:
```rust
pub(crate) mod property;
pub(crate) use property::{PhysicalPropertySet, DistributionSpec, OrderingSpec, ColumnRef};
```

- [ ] **Step 3: Build and commit**

```bash
cargo build 2>&1 | tail -3
git add src/sql/cascades/
git commit -m "feat(cascades): physical property types (distribution, ordering)"
```

---

### Task 3: LogicalPlan -> Memo insertion (convert.rs)

**Files:**
- Create: `src/sql/cascades/convert.rs`
- Modify: `src/sql/cascades/mod.rs`

Recursively walk a `LogicalPlan` tree and insert it into the Memo, creating one Group per plan node.

- [ ] **Step 1: Create convert.rs**

Create `src/sql/cascades/convert.rs` with function:

```rust
pub(crate) fn logical_plan_to_memo(
    plan: &crate::sql::plan::LogicalPlan,
    memo: &mut Memo,
) -> GroupId
```

For each LogicalPlan variant:
1. Recursively convert children to get child GroupIds
2. Create a logical `Operator` variant with the node's data (minus child references)
3. Create an `MExpr` with the operator and child GroupIds
4. Insert into a new Group via `memo.new_group(expr)`
5. Return the GroupId

Must handle all 16 LogicalPlan variants (Scan, Filter, Project, Aggregate, Join, Sort, Limit, Union, Intersect, Except, Values, GenerateSeries, Window, SubqueryAlias, Repeat, CTEConsume).

- [ ] **Step 2: Unit test**

Add test at bottom of convert.rs:
```rust
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn test_scan_to_memo() {
        // Create a simple ScanNode, convert, verify 1 group with 1 logical expr
    }

    #[test]
    fn test_filter_scan_to_memo() {
        // Filter(Scan) -> 2 groups, Filter's child = Scan's GroupId
    }
}
```

- [ ] **Step 3: Build, test, commit**

```bash
cargo test cascades::convert 2>&1 | tail -5
git add src/sql/cascades/
git commit -m "feat(cascades): LogicalPlan to Memo insertion"
```

---

### Task 4: Rule trait + Implementation rules

**Files:**
- Create: `src/sql/cascades/rule.rs`
- Create: `src/sql/cascades/rules/mod.rs`
- Create: `src/sql/cascades/rules/implement.rs`
- Modify: `src/sql/cascades/mod.rs`

Implementation rules convert each logical operator to its physical counterpart(s). JoinToHashJoin produces two alternatives: Shuffle and Broadcast.

- [ ] **Step 1: Create Rule trait**

Create `src/sql/cascades/rule.rs`:

```rust
use super::memo::{MExpr, Memo, GroupId};
use super::operator::Operator;

pub(crate) enum RuleType {
    Transformation,
    Implementation,
}

/// A new expression to add to a group.
pub(crate) struct NewExpr {
    pub op: Operator,
    pub children: Vec<GroupId>,
}

pub(crate) trait Rule: Send + Sync {
    fn name(&self) -> &str;
    fn rule_type(&self) -> RuleType;
    /// Returns true if this rule can apply to the given operator.
    fn matches(&self, op: &Operator) -> bool;
    /// Produce alternative expressions for the given MExpr.
    fn apply(&self, expr: &MExpr, memo: &Memo) -> Vec<NewExpr>;
}
```

- [ ] **Step 2: Create implementation rules**

Create `src/sql/cascades/rules/implement.rs` with one struct per rule. Each implements `Rule`. The `apply` method creates the physical variant of the operator.

Rules:
- `ScanToPhysical`: `LogicalScan -> PhysicalScan`
- `FilterToPhysical`: `LogicalFilter -> PhysicalFilter`
- `ProjectToPhysical`: `LogicalProject -> PhysicalProject`
- `JoinToHashJoin`: `LogicalJoin(has eq) -> PhysicalHashJoin(Shuffle) + PhysicalHashJoin(Broadcast)`
- `JoinToNestLoop`: `LogicalJoin(no eq) -> PhysicalNestLoopJoin`
- `AggToHashAgg`: `LogicalAggregate -> PhysicalHashAggregate(Single)`
- `SortToPhysical`: `LogicalSort -> PhysicalSort`
- `LimitToPhysical`: `LogicalLimit -> PhysicalLimit`
- `WindowToPhysical`: `LogicalWindow -> PhysicalWindow`
- `CTEProduceToPhysical`, `CTEConsumeToPhysical`
- `RepeatToPhysical`, `UnionToPhysical`, `IntersectToPhysical`, `ExceptToPhysical`
- `ValuesToPhysical`, `GenerateSeriesToPhysical`, `SubqueryAliasToPhysical`

`JoinToHashJoin.apply()` extracts equality conditions from the join condition, produces both Shuffle and Broadcast alternatives:
```rust
fn apply(&self, expr: &MExpr, _memo: &Memo) -> Vec<NewExpr> {
    let Operator::LogicalJoin(op) = &expr.op else { return vec![] };
    let (eq_conds, other) = extract_eq_conditions(&op.condition, &op.join_type);
    if eq_conds.is_empty() { return vec![] }  // needs NestLoop instead
    vec![
        NewExpr {
            op: Operator::PhysicalHashJoin(PhysicalHashJoinOp {
                join_type: op.join_type,
                eq_conditions: eq_conds.clone(),
                other_condition: other.clone(),
                distribution: JoinDistribution::Shuffle,
            }),
            children: expr.children.clone(),
        },
        NewExpr {
            op: Operator::PhysicalHashJoin(PhysicalHashJoinOp {
                join_type: op.join_type,
                eq_conditions: eq_conds,
                other_condition: other,
                distribution: JoinDistribution::Broadcast,
            }),
            children: expr.children.clone(),
        },
    ]
}
```

- [ ] **Step 3: Create rules/mod.rs with rule registry**

```rust
pub(crate) mod implement;

use super::rule::Rule;

pub(crate) fn all_implementation_rules() -> Vec<Box<dyn Rule>> {
    vec![
        Box::new(implement::ScanToPhysical),
        Box::new(implement::FilterToPhysical),
        // ... all rules ...
    ]
}

pub(crate) fn all_transformation_rules() -> Vec<Box<dyn Rule>> {
    vec![]  // added in Task 8
}
```

- [ ] **Step 4: Build, test, commit**

```bash
cargo build 2>&1 | tail -3
git add src/sql/cascades/
git commit -m "feat(cascades): rule trait and implementation rules"
```

---

### Task 5: Statistics derivation for Memo groups

**Files:**
- Create: `src/sql/cascades/stats.rs`
- Modify: `src/sql/cascades/mod.rs`

Reuse logic from `src/sql/optimizer/cardinality.rs` but operate on Memo operators instead of LogicalPlan.

- [ ] **Step 1: Create stats.rs**

```rust
pub(crate) fn derive_statistics(
    expr: &MExpr,
    memo: &Memo,
    table_stats: &HashMap<String, TableStatistics>,
) -> Statistics
```

For each operator, compute `output_row_count` and `column_statistics`. Use child group's `logical_props.row_count` for child cardinality. Logic mirrors `cardinality.rs` but reads from Memo groups instead of recursing LogicalPlan.

- [ ] **Step 2: Build, commit**

```bash
cargo build 2>&1 | tail -3
git add src/sql/cascades/
git commit -m "feat(cascades): statistics derivation for memo groups"
```

---

### Task 6: Cost model for physical operators

**Files:**
- Create: `src/sql/cascades/cost.rs`
- Modify: `src/sql/cascades/mod.rs`

- [ ] **Step 1: Create cost.rs**

```rust
pub(crate) fn compute_cost(
    op: &Operator,
    own_stats: &Statistics,
    child_stats: &[&Statistics],
) -> Cost
```

Migrate cost formulas from `src/sql/optimizer/cost.rs`:
- `PhysicalScan`: `own_stats.compute_size()`
- `PhysicalFilter/Project`: `own_stats.output_row_count * avg_row_size * 0.01`
- `PhysicalHashJoin(Shuffle)`: `(build_size + probe_size) * NETWORK_COST + probe_size`
- `PhysicalHashJoin(Broadcast)`: `build_size * NETWORK_COST + probe_size`
- `PhysicalHashAggregate(Single)`: `input_size`
- `PhysicalHashAggregate(Local)`: `input_size * 0.5`
- `PhysicalHashAggregate(Global)`: `input_size * 0.3`
- `PhysicalSort`: `n * log2(n)`
- `PhysicalDistribution`: `own_stats.compute_size() * NETWORK_COST`
- Other: `own_stats.output_row_count * 0.01`

`const NETWORK_COST: f64 = 1.5;`

- [ ] **Step 2: Build, commit**

```bash
cargo build 2>&1 | tail -3
git add src/sql/cascades/
git commit -m "feat(cascades): cost model for physical operators"
```

---

### Task 7: Top-down search with property enforcement

**Files:**
- Create: `src/sql/cascades/search.rs`
- Modify: `src/sql/cascades/mod.rs`

The core Cascades optimization algorithm: top-down search with memoization per (group, required_props).

- [ ] **Step 1: Create search.rs**

```rust
use std::collections::HashMap;
use super::memo::*;
use super::property::*;
use super::cost::compute_cost;
use super::stats::derive_statistics;

/// Winner cache: (GroupId, PhysicalPropertySet) -> (best MExprId, Cost)
pub(crate) struct SearchContext {
    winners: HashMap<(GroupId, PhysicalPropertySet), (MExprId, Cost)>,
    table_stats: HashMap<String, crate::sql::statistics::TableStatistics>,
}

impl SearchContext {
    pub(crate) fn optimize_group(
        &mut self,
        memo: &Memo,
        group_id: GroupId,
        required: &PhysicalPropertySet,
    ) -> Result<Cost, String>
}
```

`optimize_group` implementation:
1. Check winner cache — if hit, return cached cost
2. For each physical expr in the group:
   a. Compute `provided = output_properties(expr, memo)`
   b. If `provided.satisfies(required)`:
      - Compute `child_reqs = required_input_properties(expr, required)`
      - Recurse into children: `sum(optimize_group(child, child_req))`
      - `total = own_cost + children_cost`
      - Update winner if total < best
   c. Else: try enforcer
      - `enforcer_cost = distribution/sort enforcer cost`
      - `child_cost = optimize_group(same_group, provided_props)`
      - Update winner with enforcer if cheaper
3. Cache and return best cost

`output_properties()` and `required_input_properties()` are methods on physical operators following the table in the spec (Section 3.4).

- [ ] **Step 2: Build, commit**

```bash
cargo build 2>&1 | tail -3
git add src/sql/cascades/
git commit -m "feat(cascades): top-down search with property enforcement"
```

---

### Task 8: Transformation rules — JoinCommutativity, JoinAssociativity, AggSplit

**Files:**
- Create: `src/sql/cascades/rules/join_commutativity.rs`
- Create: `src/sql/cascades/rules/join_associativity.rs`
- Create: `src/sql/cascades/rules/agg_split.rs`
- Modify: `src/sql/cascades/rules/mod.rs`

- [ ] **Step 1: JoinCommutativity**

Swaps children of LogicalJoin (A JOIN B -> B JOIN A). Adjusts join type: LeftOuter <-> RightOuter, LeftSemi <-> RightSemi, etc.

- [ ] **Step 2: JoinAssociativity**

Pattern: `LogicalJoin(LogicalJoin(A, B), C)` where both are INNER joins.
Produces: `LogicalJoin(A, LogicalJoin(B, C))`.
Requires re-associating join conditions.

- [ ] **Step 3: AggSplit**

Converts `LogicalAggregate(Single)` into two groups:
- `LogicalAggregate(Local)` + child = original child
- `LogicalAggregate(Global)` + child = Local group

Only applies when GROUP BY is non-empty (otherwise single-phase is fine).

- [ ] **Step 4: Register in rules/mod.rs**

```rust
pub(crate) fn all_transformation_rules() -> Vec<Box<dyn Rule>> {
    vec![
        Box::new(join_commutativity::JoinCommutativity),
        Box::new(join_associativity::JoinAssociativity),
        Box::new(agg_split::AggSplit),
    ]
}
```

- [ ] **Step 5: Build, test, commit**

```bash
cargo build 2>&1 | tail -3
git add src/sql/cascades/
git commit -m "feat(cascades): transformation rules (join reorder, agg split)"
```

---

### Task 9: RBO rewriter + extract_best + optimize() entry point

**Files:**
- Create: `src/sql/cascades/rewriter.rs`
- Create: `src/sql/cascades/extract.rs`
- Create: `src/sql/cascades/physical_plan.rs`
- Modify: `src/sql/cascades/mod.rs`

- [ ] **Step 1: Create rewriter.rs**

```rust
pub(crate) fn rewrite(plan: LogicalPlan) -> LogicalPlan {
    let plan = crate::sql::optimizer::predicate_pushdown::push_down_predicates(plan);
    let plan = crate::sql::optimizer::column_pruning::prune_columns(plan);
    plan
}
```

Note: This calls the existing RBO passes directly. Their visibility may need to change from `pub(super)` to `pub(crate)` in `src/sql/optimizer/predicate_pushdown.rs` and `column_pruning.rs`.

- [ ] **Step 2: Create physical_plan.rs**

The PhysicalPlan tree type — extracted from Memo:

```rust
pub(crate) struct PhysicalPlanNode {
    pub op: Operator,
    pub children: Vec<PhysicalPlanNode>,
    pub stats: Statistics,
    /// Output columns from this node.
    pub output_columns: Vec<OutputColumn>,
}
```

- [ ] **Step 3: Create extract.rs**

```rust
pub(crate) fn extract_best(
    memo: &Memo,
    root_group: GroupId,
    required: &PhysicalPropertySet,
    winners: &HashMap<(GroupId, PhysicalPropertySet), (MExprId, Cost)>,
) -> Result<PhysicalPlanNode, String>
```

Walks the winner map starting from root_group with required props, recursively building a PhysicalPlanNode tree.

- [ ] **Step 4: Create optimize() entry in mod.rs**

```rust
pub(crate) fn optimize(
    plan: LogicalPlan,
    table_stats: &HashMap<String, TableStatistics>,
) -> Result<PhysicalPlanNode, String> {
    // 1. RBO rewrite
    let rewritten = rewriter::rewrite(plan);

    // 2. Convert to Memo
    let mut memo = Memo::new();
    let root_group = convert::logical_plan_to_memo(&rewritten, &mut memo);

    // 3. Derive initial statistics
    stats::derive_group_statistics(&mut memo, table_stats);

    // 4. Explore (apply transformation rules)
    let transform_rules = rules::all_transformation_rules();
    explore(&mut memo, &transform_rules);

    // 5. Implement (apply implementation rules)
    let impl_rules = rules::all_implementation_rules();
    implement(&mut memo, &impl_rules);

    // 6. Top-down search with property enforcement
    let root_required = PhysicalPropertySet::gather();
    let mut ctx = search::SearchContext::new(table_stats.clone());
    ctx.optimize_group(&memo, root_group, &root_required)?;

    // 7. Extract best plan
    extract::extract_best(&memo, root_group, &root_required, &ctx.winners)
}
```

- [ ] **Step 5: Build, commit**

```bash
cargo build 2>&1 | tail -3
git add src/sql/cascades/ src/sql/optimizer/
git commit -m "feat(cascades): rewriter, extract_best, optimize() entry point"
```

---

### Task 10: PlanFragmentBuilder visitor (single-fragment)

**Files:**
- Create: `src/sql/cascades/fragment_builder.rs`
- Modify: `src/sql/cascades/mod.rs`

First version handles single-fragment plans (no PhysicalDistribution splitting). Reuses existing `physical/nodes.rs` helpers for Thrift generation.

- [ ] **Step 1: Create fragment_builder.rs**

```rust
use crate::sql::physical::{PlanBuildResult, FragmentBuildResult, MultiFragmentBuildResult, OutputColumn};

pub(crate) struct PlanFragmentBuilder { ... }

pub(crate) struct BuildResult {
    pub fragments: Vec<FragmentBuildResult>,
    pub root_fragment_id: u32,
}

impl PlanFragmentBuilder {
    pub(crate) fn build(
        plan: &PhysicalPlanNode,
        catalog: &dyn CatalogProvider,
        current_database: &str,
    ) -> Result<BuildResult, String>;
}
```

Visitor methods: `visit(node)` dispatches by operator type. Each returns the Thrift `TPlanNode` list and the fragment index. Maintains:
- `desc_builder`: slot/tuple allocation (reuse existing `DescriptorTableBuilder`)
- `scan_tables`: for exec_params
- `next_node_id`, `next_slot_id`, `next_tuple_id` counters
- `fragments: Vec<FragmentBuildResult>`

For this task, implement: `visit_scan`, `visit_filter`, `visit_project`, `visit_hash_join`, `visit_nest_loop_join`, `visit_hash_aggregate`, `visit_sort`, `visit_limit`, `visit_window`, `visit_values`, `visit_generate_series`, `visit_subquery_alias`, `visit_repeat`.

Each visitor method creates the TPlanNode by calling existing helpers from `physical/nodes.rs` (e.g., `build_scan_node`, `build_hash_join_node`). The expression compilation uses existing `physical/expr_compiler.rs`.

- [ ] **Step 2: Build, commit**

```bash
cargo build 2>&1 | tail -3
git add src/sql/cascades/
git commit -m "feat(cascades): PlanFragmentBuilder visitor (single-fragment)"
```

---

### Task 11: PlanFragmentBuilder — multi-fragment (distribution + CTE)

**Files:**
- Modify: `src/sql/cascades/fragment_builder.rs`

Add `visit_distribution` and `visit_cte_produce/consume` to create multi-fragment plans.

- [ ] **Step 1: Add visit_distribution**

When visiting `PhysicalDistribution`:
1. Visit child subtree → child fragment created
2. Set child fragment's sink to `DataStreamSink` with partition spec
3. Create new fragment with `ExchangeNode` as root
4. Return new fragment index

- [ ] **Step 2: Add visit_cte_produce / visit_cte_consume**

`visit_cte_produce`: wrap child fragment as multicast, store in cte_fragments map.
`visit_cte_consume`: create ExchangeNode pointing to multicast fragment, add as multicast destination.

- [ ] **Step 3: Build, commit**

```bash
cargo build 2>&1 | tail -3
git add src/sql/cascades/
git commit -m "feat(cascades): multi-fragment support in PlanFragmentBuilder"
```

---

### Task 12: EXPLAIN update for PhysicalPlan

**Files:**
- Modify: `src/sql/explain.rs`

- [ ] **Step 1: Add explain_physical_plan()**

```rust
pub(crate) fn explain_physical_plan(
    plan: &PhysicalPlanNode,
    level: ExplainLevel,
) -> Vec<String>
```

Formats physical operators with distribution info. Example output:
```
LIMIT [limit=100]
  PHYSICAL SORT [sum_sales DESC]
    GATHER EXCHANGE
      HASH AGGREGATE (GLOBAL, group by: [ss.s_id])
        HASH EXCHANGE (hash: [ss.s_id])
          HASH AGGREGATE (LOCAL, group by: [ss.s_id])
            HASH JOIN (SHUFFLE, eq: [ss.d_id = d.d_id])
              HASH EXCHANGE (hash: [ss.d_id])
                SCAN tpcds.store_sales
              HASH EXCHANGE (hash: [d.d_id])
                SCAN tpcds.date_dim
```

- [ ] **Step 2: Build, commit**

```bash
cargo build 2>&1 | tail -3
git add src/sql/explain.rs
git commit -m "feat(explain): physical plan formatting with distribution info"
```

---

### Task 13: Wire into engine.rs — replace old optimizer

**Files:**
- Modify: `src/standalone/engine.rs`
- Modify: `src/standalone/coordinator.rs`

- [ ] **Step 1: Update execute_query**

Replace the body of `execute_query`:

```rust
fn execute_query(
    query: &sqlparser::ast::Query,
    catalog: &InMemoryCatalog,
    current_database: &str,
) -> Result<QueryResult, String> {
    let (resolved, cte_registry) =
        crate::sql::analyzer::analyze(query, catalog, current_database)?;

    let logical = if cte_registry.entries.is_empty() {
        crate::sql::planner::plan(resolved)?
    } else {
        // For CTE queries, plan_query produces a QueryPlan; optimize each part
        let query_plan = crate::sql::planner::plan_query(resolved, cte_registry)?;
        // TODO: handle CTE in Cascades (Phase 2+3 scope)
        // For now, convert QueryPlan.main_plan
        query_plan.main_plan
    };

    let table_stats = build_table_stats_from_plan(&logical);
    let physical = crate::sql::cascades::optimize(logical, &table_stats)?;

    let build_result = crate::sql::cascades::PlanFragmentBuilder::build(
        &physical, catalog, current_database,
    )?;

    if build_result.fragments.len() == 1 {
        execute_plan(build_result.fragments.into_iter().next().unwrap())
    } else {
        // Multi-fragment: use ExecutionCoordinator
        let exchange_port = crate::common::config::http_port();
        super::coordinator::ExecutionCoordinator::new(
            build_result, "127.0.0.1".to_string(), exchange_port
        ).execute()
    }
}
```

- [ ] **Step 2: Update explain_query**

Replace the body to use `cascades::optimize()` + `explain_physical_plan()`.

- [ ] **Step 3: Run TPC-DS regression**

```bash
pkill -f "novarocks standalone" 2>/dev/null; sleep 1
cargo build
nohup ./target/debug/novarocks standalone-server >/dev/null 2>&1 &
sleep 8
# Create catalog, run suite
cargo run --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests -- \
  --suite tpc-ds --mode verify 2>&1 | grep -E "^total=|^pass=|^fail="
```

Expected: `total=99 pass=99 fail=0`

- [ ] **Step 4: Fix any regressions**

Iterate until 99/99 passes.

- [ ] **Step 5: Commit**

```bash
git add src/standalone/ src/sql/
git commit -m "feat: wire Cascades optimizer into standalone engine, replace old optimizer"
```

---

## Implementation Notes

### Visibility adjustments needed

- `src/sql/optimizer/predicate_pushdown.rs`: change `pub(super) fn push_down_predicates` to `pub(crate) fn push_down_predicates`
- `src/sql/optimizer/column_pruning.rs`: change `pub(super) fn prune_columns` to `pub(crate) fn prune_columns`
- `src/sql/physical/nodes.rs`: some `pub(super)` functions need `pub(crate)` for `fragment_builder.rs` to call them
- `src/sql/physical/expr_compiler.rs`: `ExprCompiler` and `ExprScope` need `pub(crate)` visibility

### Existing code retained

- `src/sql/optimizer/cardinality.rs` — logic reused in `cascades/stats.rs`
- `src/sql/optimizer/cost.rs` — formulas reused in `cascades/cost.rs`
- `src/sql/physical/nodes.rs` — Thrift node builders called by `fragment_builder.rs`
- `src/sql/physical/expr_compiler.rs` — expression compilation reused
- `src/sql/physical/descriptors.rs` — descriptor table builder reused

### Existing code removed (after Task 13 verified)

- `src/sql/optimizer/join_reorder.rs` — replaced by Cascades join rules
- `src/sql/optimizer/mod.rs` (`optimize()`, `optimize_query_plan()`) — replaced by `cascades::optimize()`
- `src/sql/fragment.rs` — replaced by `cascades::fragment_builder.rs`
- `src/sql/physical/emitter/mod.rs` — replaced by `cascades::fragment_builder.rs`
