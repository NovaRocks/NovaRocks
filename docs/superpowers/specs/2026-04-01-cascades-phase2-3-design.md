# Cascades Optimizer Phase 2+3: Framework + Distribution Property

Date: 2026-04-01
Status: Draft

## 1. Overview

Implement the Cascades optimizer framework with distribution property support, replacing the current 3-pass optimizer (`predicate_pushdown`, `column_pruning`, `join_reorder`). The optimizer produces a PhysicalPlan with distribution enforcement nodes, which a StarRocks-aligned `PlanFragmentBuilder` visitor converts into per-fragment Thrift plans in a single pass.

**Scope:** Sections 3 (Cascades Core), 5 (Fragment Planning), and the distribution portion of Section 3.3-3.4 from the original design spec (`2026-03-31-cascades-multi-fragment-multicast-design.md`).

**Validation:** TPC-DS 99/99 pass rate maintained after direct replacement of the old optimizer.

## 2. Architecture

```
SQL
 -> Parser (sqlparser-rs)
 -> Analyzer (semantic analysis, CTE registry)
 -> Planner (LogicalPlan with CTEProduce/CTEConsume)
 -> RBO Rewriter (predicate pushdown, column pruning, constant folding)
 -> Cascades Memo Search (join reorder, distribution, agg split)
 -> Extract Best PhysicalPlan
 -> PlanFragmentBuilder (visitor: fragment split + Thrift emission)
 -> ExecutionCoordinator (multi-fragment submission)
```

### 2.1 Key Design Decisions

1. **New PhysicalPlan IR** -- Cascades outputs a tree of physical operators (PhysicalScan, PhysicalHashJoin, PhysicalDistribution, etc.), distinct from LogicalPlan.
2. **StarRocks-aligned single visitor** -- `PlanFragmentBuilder` walks the PhysicalPlan, creating fragment boundaries at PhysicalDistribution nodes and emitting Thrift TPlan per fragment. No separate fragment planner step.
3. **Direct replacement** -- The old `optimizer::optimize()` + `physical::emit()` path is replaced, not feature-flagged. TPC-DS 99/99 is the regression gate.

## 3. Cascades Optimizer Core

### 3.1 Memo Structure

```rust
// src/sql/cascades/memo.rs

pub(crate) struct Memo {
    groups: Vec<Group>,
}

pub(crate) struct Group {
    id: GroupId,
    logical_exprs: Vec<MExpr>,
    physical_exprs: Vec<MExpr>,
    /// Best physical expression for each required property set.
    winners: HashMap<PhysicalPropertySet, Winner>,
    /// Logical properties (output columns, row count estimate).
    logical_props: LogicalProperties,
}

pub(crate) struct Winner {
    pub expr_id: MExprId,
    pub cost: Cost,
}

/// A memo expression: an operator whose children are GroupIds.
pub(crate) struct MExpr {
    pub id: MExprId,
    pub op: Operator,
    pub children: Vec<GroupId>,
    pub stats: Option<Statistics>,
}
```

### 3.2 Operator System

Two enums: logical operators for equivalence exploration, physical operators for implementation.

```rust
// src/sql/cascades/operator.rs

pub(crate) enum Operator {
    // --- Logical ---
    LogicalScan(LogicalScanOp),
    LogicalFilter(LogicalFilterOp),
    LogicalProject(LogicalProjectOp),
    LogicalAggregate(LogicalAggregateOp),
    LogicalJoin(LogicalJoinOp),
    LogicalSort(LogicalSortOp),
    LogicalLimit(LogicalLimitOp),
    LogicalWindow(LogicalWindowOp),
    LogicalUnion(LogicalUnionOp),
    LogicalIntersect(LogicalIntersectOp),
    LogicalExcept(LogicalExceptOp),
    LogicalCTEProduce(LogicalCTEProduceOp),
    LogicalCTEConsume(LogicalCTEConsumeOp),
    LogicalRepeat(LogicalRepeatOp),

    // --- Physical ---
    PhysicalScan(PhysicalScanOp),
    PhysicalFilter(PhysicalFilterOp),
    PhysicalProject(PhysicalProjectOp),
    PhysicalHashJoin(PhysicalHashJoinOp),
    PhysicalNestLoopJoin(PhysicalNestLoopJoinOp),
    PhysicalHashAggregate(PhysicalHashAggregateOp),
    PhysicalSort(PhysicalSortOp),
    PhysicalLimit(PhysicalLimitOp),
    PhysicalWindow(PhysicalWindowOp),
    PhysicalDistribution(PhysicalDistributionOp),
    PhysicalCTEProduce(PhysicalCTEProduceOp),
    PhysicalCTEConsume(PhysicalCTEConsumeOp),
    PhysicalRepeat(PhysicalRepeatOp),
    PhysicalUnion(PhysicalUnionOp),
    PhysicalIntersect(PhysicalIntersectOp),
    PhysicalExcept(PhysicalExceptOp),
}
```

Physical operator structs carry the physical decisions:

```rust
pub(crate) struct PhysicalHashJoinOp {
    pub join_type: JoinKind,
    pub eq_conditions: Vec<(TypedExpr, TypedExpr)>,
    pub other_conditions: Option<TypedExpr>,
    pub distribution: JoinDistribution,
}

pub(crate) enum JoinDistribution {
    Shuffle,     // both sides hash-partitioned by join keys
    Broadcast,   // right side broadcast, left partitioned
    Colocate,    // already co-located (future: for internal tables)
}

pub(crate) struct PhysicalHashAggregateOp {
    pub mode: AggMode,
    pub group_by: Vec<TypedExpr>,
    pub aggregates: Vec<AggregateCall>,
    pub output_columns: Vec<OutputColumn>,
}

pub(crate) enum AggMode {
    Single,  // one-phase aggregation
    Local,   // partial aggregation (before exchange)
    Global,  // final aggregation (after exchange)
}

pub(crate) struct PhysicalDistributionOp {
    pub spec: DistributionSpec,
}
```

### 3.3 Physical Properties

```rust
// src/sql/cascades/property.rs

#[derive(Clone, Hash, Eq, PartialEq)]
pub(crate) struct PhysicalPropertySet {
    pub distribution: DistributionSpec,
    pub ordering: OrderingSpec,
}

#[derive(Clone, Hash, Eq, PartialEq)]
pub(crate) enum DistributionSpec {
    Any,                           // no requirement
    Gather,                        // single node
    HashPartitioned(Vec<ColumnRef>), // hash by columns
}

#[derive(Clone, Hash, Eq, PartialEq)]
pub(crate) enum OrderingSpec {
    Any,
    Required(Vec<SortKey>),
}
```

### 3.4 Property Derivation per Physical Operator

| Physical Operator | required_input_properties | output_property |
|---|---|---|
| PhysicalScan | -- | Any (single-node standalone) |
| PhysicalFilter | passthrough parent | passthrough child |
| PhysicalProject | passthrough (column-mapped) | passthrough (column-mapped) |
| PhysicalHashJoin(Shuffle) | [Hash(left_keys), Hash(right_keys)] | Hash(left_keys) |
| PhysicalHashJoin(Broadcast) | [Any, Gather] | passthrough left |
| PhysicalNestLoopJoin | [Gather, Gather] | Gather |
| PhysicalHashAggregate(Single) | Any | Hash(group_keys) or Gather |
| PhysicalHashAggregate(Local) | Any | Hash(group_keys) |
| PhysicalHashAggregate(Global) | Hash(group_keys) | Hash(group_keys) |
| PhysicalSort | Gather | Gather + Ordered |
| PhysicalLimit | passthrough | passthrough |
| PhysicalWindow | Hash(partition_keys) or Gather | passthrough |
| PhysicalCTEConsume | -- | Any |
| PhysicalRepeat | passthrough | passthrough |
| PhysicalDistribution | -- (enforcer) | spec |

### 3.5 Rule System

```rust
// src/sql/cascades/rule.rs

pub(crate) trait Rule {
    fn name(&self) -> &str;
    fn rule_type(&self) -> RuleType;
    fn pattern(&self) -> &Pattern;
    fn apply(&self, expr: &MExpr, memo: &Memo) -> Vec<NewExpr>;
}

pub(crate) enum RuleType {
    Transformation, // logical -> logical
    Implementation, // logical -> physical
}

/// Pattern for matching MExpr trees.
pub(crate) enum Pattern {
    Leaf,
    Node { op_type: OpType, children: Vec<Pattern> },
}
```

Initial rule set:

| Rule | Type | Input -> Output |
|---|---|---|
| JoinCommutativity | Transform | LogicalJoin(A,B) -> LogicalJoin(B,A) |
| JoinAssociativity | Transform | LogicalJoin(LogicalJoin(A,B),C) -> LogicalJoin(A,LogicalJoin(B,C)) |
| AggSplit | Transform | LogicalAggregate -> LogicalAggregate(Local) + LogicalAggregate(Global) |
| ScanToPhysical | Implement | LogicalScan -> PhysicalScan |
| FilterToPhysical | Implement | LogicalFilter -> PhysicalFilter |
| ProjectToPhysical | Implement | LogicalProject -> PhysicalProject |
| JoinToHashJoin | Implement | LogicalJoin(eq) -> PhysicalHashJoin(Shuffle) + PhysicalHashJoin(Broadcast) |
| JoinToNestLoop | Implement | LogicalJoin(no eq) -> PhysicalNestLoopJoin |
| AggToHashAgg | Implement | LogicalAggregate -> PhysicalHashAggregate |
| SortToPhysical | Implement | LogicalSort -> PhysicalSort |
| LimitToPhysical | Implement | LogicalLimit -> PhysicalLimit |
| WindowToPhysical | Implement | LogicalWindow -> PhysicalWindow |
| CTEProduceToPhysical | Implement | LogicalCTEProduce -> PhysicalCTEProduce |
| CTEConsumeToPhysical | Implement | LogicalCTEConsume -> PhysicalCTEConsume |
| RepeatToPhysical | Implement | LogicalRepeat -> PhysicalRepeat |
| UnionToPhysical | Implement | LogicalUnion -> PhysicalUnion |
| IntersectToPhysical | Implement | LogicalIntersect -> PhysicalIntersect |
| ExceptToPhysical | Implement | LogicalExcept -> PhysicalExcept |

### 3.6 Optimization Flow

```
optimize(logical_plan, table_stats):
  1. rewrite(logical_plan)           // RBO: predicate pushdown, column pruning
  2. memo = init_memo(rewritten)     // Insert LogicalPlan into Memo groups
  3. explore(memo, transform_rules)  // Expand logical equivalences (join reorder)
  4. implement(memo, impl_rules)     // Generate physical alternatives per group
  5. enforce(memo, root_required)    // Top-down cost-based search with property enforcement
  6. return extract_best(memo, root_required)  // -> PhysicalPlan tree
```

### 3.7 Property Enforcement (Top-Down Search)

```
optimize_group(group_id, required_props) -> Cost:
  if cached(group_id, required_props): return cached_cost

  best_cost = infinity
  for each physical_expr in group.physical_exprs:
    // Check if this expr can satisfy required_props
    provided = physical_expr.output_properties()

    if required_props.satisfied_by(provided):
      // Direct satisfaction: recurse into children
      child_reqs = physical_expr.required_input_properties(required_props)
      total = physical_expr.cost(stats)
      for (child_group, child_req) in zip(physical_expr.children, child_reqs):
        total += optimize_group(child_group, child_req)
      if total < best_cost:
        best_cost = total
        update_winner(group_id, required_props, physical_expr, total)

    else:
      // Need enforcer (PhysicalDistribution or PhysicalSort)
      enforcer_cost = estimate_enforcer(required_props, provided, stats)
      child_cost = optimize_group(group_id, provided)
      total = enforcer_cost + child_cost
      if total < best_cost:
        best_cost = total
        // Winner is the enforcer wrapping the group's best for `provided`
        update_winner_with_enforcer(group_id, required_props, total)

  return best_cost
```

Root required properties: `PhysicalPropertySet { distribution: Gather, ordering: Any }` (standalone collects results to single node).

### 3.8 Cost Model

```rust
// src/sql/cascades/cost.rs

pub(crate) type Cost = f64;

pub(crate) fn compute_cost(op: &Operator, stats: &Statistics) -> Cost {
    match op {
        PhysicalScan(_) => stats.row_count as f64,
        PhysicalFilter(_) => stats.row_count as f64 * 0.1,
        PhysicalProject(_) => stats.row_count as f64 * 0.01,
        PhysicalHashJoin(j) => {
            let build_cost = stats.children[1].row_count as f64;
            let probe_cost = stats.children[0].row_count as f64;
            match j.distribution {
                Broadcast => build_cost * NETWORK_COST + probe_cost,
                Shuffle => (build_cost + probe_cost) * NETWORK_COST + probe_cost,
                Colocate => probe_cost,
            }
        }
        PhysicalHashAggregate(a) => match a.mode {
            Single => stats.row_count as f64,
            Local => stats.row_count as f64 * 0.5,
            Global => stats.row_count as f64 * 0.3,
        },
        PhysicalDistribution(_) => stats.row_count as f64 * NETWORK_COST,
        PhysicalSort(_) => {
            let n = stats.row_count.max(1) as f64;
            n * n.log2()
        }
        _ => stats.row_count as f64 * 0.01,
    }
}

const NETWORK_COST: f64 = 1.5;
```

### 3.9 Statistics

Reuse existing `src/sql/statistics/` and `src/sql/optimizer/cardinality.rs`. Migrate cardinality estimation functions to work with Memo groups:

```rust
pub(crate) struct Statistics {
    pub row_count: f64,
    pub column_stats: HashMap<String, ColumnStatistics>,
}

pub(crate) struct ColumnStatistics {
    pub ndv: f64,
    pub min: Option<LiteralValue>,
    pub max: Option<LiteralValue>,
    pub null_fraction: f64,
}
```

## 4. RBO Rewriter

Deterministic transformations applied to LogicalPlan before Memo insertion. Migrated from existing optimizer passes:

```rust
// src/sql/cascades/rewriter.rs

pub(crate) fn rewrite(plan: LogicalPlan) -> LogicalPlan {
    let plan = predicate_pushdown(plan);
    let plan = column_pruning(plan);
    plan
}
```

The existing `optimizer/predicate_pushdown.rs` and `optimizer/column_pruning.rs` logic is reused directly -- they already operate on LogicalPlan. The rewriter simply calls them in sequence.

## 5. PlanFragmentBuilder (StarRocks-aligned Visitor)

### 5.1 Design

A single visitor that walks the extracted PhysicalPlan tree and produces:
- A list of `FragmentBuildResult` (each with TPlan + TDataSink + TDescriptorTable)
- Fragment-level metadata (partition type, exchange destinations)

Fragment boundaries are created at `PhysicalDistribution` nodes. CTE produce/consume creates multicast fragments.

```rust
// src/sql/cascades/fragment_builder.rs

pub(crate) struct PlanFragmentBuilder {
    fragments: Vec<FragmentBuildResult>,
    cte_fragments: HashMap<CteId, usize>,  // cte_id -> fragment index
    desc_builder: DescriptorTableBuilder,
    next_node_id: i32,
    next_slot_id: i32,
    next_tuple_id: i32,
}

impl PlanFragmentBuilder {
    pub fn build(
        plan: PhysicalPlan,
        catalog: &dyn CatalogProvider,
        current_database: &str,
    ) -> Result<BuildResult, String>;
}
```

### 5.2 Visitor Methods

```rust
/// Returns (fragment_index, plan_nodes for this subtree).
fn visit(&mut self, node: &PhysicalPlanNode) -> Result<VisitResult, String> {
    match &node.op {
        PhysicalScan(op)           => self.visit_scan(op),
        PhysicalFilter(op)         => self.visit_filter(op, &node.children),
        PhysicalProject(op)        => self.visit_project(op, &node.children),
        PhysicalHashJoin(op)       => self.visit_hash_join(op, &node.children),
        PhysicalNestLoopJoin(op)   => self.visit_nest_loop_join(op, &node.children),
        PhysicalHashAggregate(op)  => self.visit_hash_aggregate(op, &node.children),
        PhysicalSort(op)           => self.visit_sort(op, &node.children),
        PhysicalLimit(op)          => self.visit_limit(op, &node.children),
        PhysicalWindow(op)         => self.visit_window(op, &node.children),
        PhysicalDistribution(op)   => self.visit_distribution(op, &node.children),
        PhysicalCTEProduce(op)     => self.visit_cte_produce(op, &node.children),
        PhysicalCTEConsume(op)     => self.visit_cte_consume(op),
        PhysicalRepeat(op)         => self.visit_repeat(op, &node.children),
        PhysicalUnion(op)          => self.visit_union(op, &node.children),
        // ...
    }
}
```

### 5.3 Fragment Boundary Creation (visit_distribution)

```rust
fn visit_distribution(&mut self, op: &PhysicalDistributionOp, children: &[PhysicalPlanNode])
    -> Result<VisitResult, String>
{
    // 1. Visit child subtree -> produces a fragment with plan nodes
    let child_result = self.visit(&children[0])?;
    let child_fragment_idx = child_result.fragment_idx;

    // 2. Finalize child fragment: set its sink to DataStreamSink
    //    targeting the new exchange node
    let exchange_node_id = self.alloc_node_id();
    self.set_fragment_sink(child_fragment_idx, DataStreamSink {
        dest_node_id: exchange_node_id,
        partition: op.spec.to_thrift_partition(),
    });

    // 3. Create new fragment with ExchangeNode as root
    let exchange_node = build_exchange_node(exchange_node_id, ...);
    let new_fragment_idx = self.create_fragment(exchange_node, op.spec.to_data_partition());

    Ok(VisitResult { fragment_idx: new_fragment_idx, plan_nodes: vec![exchange_node] })
}
```

### 5.4 CTE Multicast Handling

```rust
fn visit_cte_produce(&mut self, op: &PhysicalCTEProduceOp, children: &[PhysicalPlanNode])
    -> Result<VisitResult, String>
{
    let child_result = self.visit(&children[0])?;
    // Wrap child fragment as multicast (sink will be set later by consumers)
    self.mark_as_multicast(child_result.fragment_idx, op.cte_id);
    self.cte_fragments.insert(op.cte_id, child_result.fragment_idx);
    Ok(child_result)
}

fn visit_cte_consume(&mut self, op: &PhysicalCTEConsumeOp)
    -> Result<VisitResult, String>
{
    let producer_idx = self.cte_fragments[&op.cte_id];
    let exchange_node_id = self.alloc_node_id();

    // Add this consumer as a destination in the multicast fragment
    self.add_multicast_destination(producer_idx, exchange_node_id);

    // Create consumer fragment with ExchangeNode as root
    let exchange_node = build_exchange_node(exchange_node_id, ...);
    let fragment_idx = self.create_fragment(exchange_node, DataPartition::Unpartitioned);

    Ok(VisitResult { fragment_idx, plan_nodes: vec![exchange_node] })
}
```

### 5.5 Thrift Node Generation

Each `visit_*` method produces Thrift `TPlanNode` structs. The generation logic is migrated from the existing `physical/emitter/` and `physical/nodes.rs`:

- `visit_scan` -> `TPlanNodeType::HDFS_SCAN_NODE` (reuses existing `build_hdfs_scan_node`)
- `visit_hash_join` -> `TPlanNodeType::HASH_JOIN_NODE` (reuses existing `build_hash_join_node`)
- `visit_hash_aggregate` -> `TPlanNodeType::AGGREGATION_NODE`
- `visit_sort` -> `TPlanNodeType::SORT_NODE`
- etc.

The Thrift construction helpers in `physical/nodes.rs` remain utility functions called by the visitor.

### 5.6 Build Output

```rust
pub(crate) struct BuildResult {
    /// All fragments in dependency order (leaves first, root last).
    pub fragments: Vec<FragmentBuildResult>,
    pub root_fragment_id: FragmentId,
}
```

This feeds directly into the existing `ExecutionCoordinator` for multi-fragment execution, or into `execute_plan` for single-fragment execution.

## 6. Integration with Engine

### 6.1 New Query Execution Flow

```rust
// In standalone/engine.rs

fn execute_query(query, catalog, current_database) -> Result<QueryResult> {
    let (resolved, cte_registry) = analyzer::analyze(query, catalog, current_database)?;

    // RBO: deterministic rewrites
    let logical = planner::plan_with_ctes(resolved, cte_registry)?;
    let rewritten = cascades::rewriter::rewrite(logical);

    // CBO: Cascades memo search
    let table_stats = build_table_stats(&rewritten);
    let physical = cascades::optimize(rewritten, &table_stats)?;

    // Fragment build + Thrift emission (single visitor pass)
    let build_result = cascades::PlanFragmentBuilder::build(
        physical, catalog, current_database
    )?;

    if build_result.fragments.len() == 1 {
        execute_plan(build_result.fragments.into_iter().next().unwrap())
    } else {
        ExecutionCoordinator::new(build_result).execute()
    }
}
```

### 6.2 EXPLAIN Integration

The existing `explain_query` function is updated to format the PhysicalPlan tree (not LogicalPlan). `src/sql/explain.rs` adds `explain_physical_plan()` that walks the extracted PhysicalPlan and produces text similar to the current logical explain but with physical operator names and distribution info:

```
EXPLAIN SELECT ... FROM store_sales ss JOIN date_dim d ON ...

LIMIT [limit=100]
  SORT BY [sum_sales DESC]
    GATHER EXCHANGE
      HASH AGGREGATE (GLOBAL, group by: [ss.s_id])
        SHUFFLE EXCHANGE (hash: [ss.s_id])
          HASH AGGREGATE (LOCAL, group by: [ss.s_id])
            HASH JOIN (SHUFFLE, eq: [ss.d_id = d.d_id])
              SHUFFLE EXCHANGE (hash: [ss.d_id])
                SCAN tpcds.store_sales
              SHUFFLE EXCHANGE (hash: [d.d_id])
                SCAN tpcds.date_dim
                  predicates: d.d_year = 2001
```

## 7. Module Layout

### New modules

```
src/sql/cascades/
    mod.rs           -- optimize() entry point
    memo.rs          -- Memo, Group, MExpr
    operator.rs      -- Operator enum (logical + physical)
    property.rs      -- PhysicalPropertySet, DistributionSpec, OrderingSpec
    rule.rs          -- Rule trait, Pattern
    rules/
        mod.rs
        join_commutativity.rs
        join_associativity.rs
        agg_split.rs
        implement.rs    -- all implementation rules (logical -> physical)
    cost.rs          -- cost model
    statistics.rs    -- Statistics struct, cardinality derivation
    rewriter.rs      -- RBO rewrite entry (calls predicate_pushdown + column_pruning)
    search.rs        -- top-down optimization search with enforcement
    extract.rs       -- extract_best: Memo -> PhysicalPlan tree
    fragment_builder.rs -- PlanFragmentBuilder visitor
```

### Modified modules

- `src/sql/explain.rs` -- add `explain_physical_plan()` for PhysicalPlan formatting
- `src/standalone/engine.rs` -- switch `execute_query` to Cascades pipeline
- `src/standalone/coordinator.rs` -- accept `BuildResult` from PlanFragmentBuilder

### Retained modules (utility)

- `src/sql/optimizer/predicate_pushdown.rs` -- called by `rewriter.rs`
- `src/sql/optimizer/column_pruning.rs` -- called by `rewriter.rs`
- `src/sql/optimizer/cardinality.rs` -- cardinality estimation logic reused
- `src/sql/optimizer/cost.rs` -- cost model components reused
- `src/sql/physical/nodes.rs` -- Thrift node construction helpers reused by `fragment_builder.rs`
- `src/sql/physical/emitter/` -- retained for reference, removed after migration verified

### Removed modules (after migration)

- `src/sql/optimizer/mod.rs` (`optimize()` entry) -- replaced by `cascades::optimize()`
- `src/sql/optimizer/join_reorder.rs` -- replaced by JoinCommutativity + JoinAssociativity rules
- `src/sql/fragment.rs` -- replaced by `cascades::fragment_builder.rs`
- `src/sql/physical/emitter/mod.rs` -- replaced by `fragment_builder.rs`

## 8. Standalone-Specific Considerations

In standalone mode (single BE, single node), the distribution model is simplified:

- **No real shuffle** -- all data is local. PhysicalDistribution(Hash) still creates fragment boundaries for pipeline parallelism, but no network transfer occurs.
- **Gather = local merge** -- the root fragment collects from local exchange buffers.
- **Broadcast = no-op** -- right side of broadcast join is already local.

The cost model accounts for this: `NETWORK_COST` is low (1.5x) since exchange is local memory copy, not network. This allows the optimizer to still prefer broadcast for small right tables and shuffle for large joins, even though both are local.

## 9. Phased Implementation Strategy

Despite Phase 2+3 being a single design, implementation is ordered to allow incremental testing:

| Step | Content | Testable |
|------|---------|----------|
| 1 | Memo + Group + MExpr + Operator definitions | Unit tests |
| 2 | PhysicalPropertySet + DistributionSpec | Unit tests |
| 3 | LogicalPlan -> Memo insertion (init_memo) | Unit tests |
| 4 | Implementation rules (all logical -> physical 1:1 mappings) | Unit tests |
| 5 | Top-down search with property enforcement (no transform rules yet) | Single-table queries pass |
| 6 | extract_best -> PhysicalPlan tree | Unit tests |
| 7 | PlanFragmentBuilder (single fragment, no distribution) | TPC-DS single-table queries |
| 8 | PlanFragmentBuilder (distribution: exchange nodes, multi-fragment) | TPC-DS join queries |
| 9 | RBO rewriter (predicate pushdown + column pruning) | TPC-DS with filters |
| 10 | Transform rules: JoinCommutativity + JoinAssociativity | TPC-DS join reorder |
| 11 | AggSplit rule (Local + Global aggregation) | TPC-DS aggregation queries |
| 12 | EXPLAIN update for PhysicalPlan | Manual verification |
| 13 | Wire into engine.rs, remove old optimizer | TPC-DS 99/99 |
