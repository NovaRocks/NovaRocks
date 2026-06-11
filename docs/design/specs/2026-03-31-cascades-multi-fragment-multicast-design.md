# Cascades Optimizer + Multi-Fragment + Multicast Design

Date: 2026-03-31
Status: Approved

## 1. Overview

NovaRocks standalone mode currently executes all queries as a single fragment. This design adds:

1. **Cascades optimizer** — Memo-based search framework replacing the current 3-pass optimizer, with distribution and ordering as physical properties
2. **Multi-fragment planning** — automatic fragment splitting at exchange boundaries based on distribution property enforcement
3. **CTE multicast** — shared CTE execution via `MULTI_CAST_DATA_STREAM_SINK`, eliminating redundant computation
4. **Fragment execution coordination** — standalone engine submits multiple fragments via gRPC exchange

Target: progressively pass TPC-DS suite in `sql-tests`.

## 2. Architecture

```
SQL
 → Parser (sqlparser-rs)
 → Analyzer (semantic analysis, CTE registry with ref counting)
 → Planner (LogicalPlan with CTEProduce/CTEConsume nodes)
 → Rewriter (RBO: predicate pushdown, column pruning, constant folding)
 → Cascades Memo Search (CBO: join reorder, distribution, agg split, CTE inline-vs-share)
 → Extract Best PhysicalPlan
 → Fragment Planner (split at PhysicalDistribution boundaries)
 → Physical Emitter (per-fragment Thrift emission)
 → Execution Coordinator (multi-fragment submission via gRPC exchange)
```

### 2.1 RBO / CBO Separation

Two-phase model aligned with StarRocks FE:

- **Phase 1 — Rewriter (RBO):** Deterministic transformations applied on LogicalPlan before Memo insertion. No cost comparison needed. Includes: predicate pushdown, column pruning, constant folding, subquery unnesting.
- **Phase 2 — Memo Search (CBO):** Generates alternative plans in Memo, selects lowest-cost option. Includes: join reorder (commutativity + associativity), distribution selection (shuffle vs broadcast), aggregate mode (single vs local-global), CTE sharing decision, property enforcement.

Current optimizer passes migrate as:
- `predicate_pushdown.rs` → RBO rewrite rule
- `join_reorder.rs` → CBO transformation rules (JoinCommutativity + JoinAssociativity) + cost model
- `column_pruning.rs` → RBO rewrite rule

## 3. Cascades Optimizer Core

### 3.1 Memo Structure

```rust
pub struct Memo {
    groups: Vec<Group>,
}

pub struct Group {
    id: GroupId,
    logical_exprs: Vec<GroupExpr>,
    physical_exprs: Vec<GroupExpr>,
    required_properties: Vec<PhysicalPropertySet>,
    best_exprs: HashMap<PhysicalPropertySet, (GroupExpr, Cost)>,
}

pub struct GroupExpr {
    op: Operator,
    children: Vec<GroupId>,
    stats: Option<Statistics>,
}
```

### 3.2 Operator System

```rust
pub enum Operator {
    // --- Logical ---
    LogicalScan { table, columns, predicates },
    LogicalFilter { predicate },
    LogicalProject { items },
    LogicalAggregate { group_by, aggregates },
    LogicalJoin { join_type, condition },
    LogicalSort { items },
    LogicalLimit { limit, offset },
    LogicalWindow { window_exprs },
    LogicalUnion { all },
    LogicalIntersect { all },
    LogicalExcept { all },
    LogicalCTEProduce { cte_id },
    LogicalCTEConsume { cte_id },

    // --- Physical ---
    PhysicalScan { ... },
    PhysicalFilter { ... },
    PhysicalProject { ... },
    PhysicalHashJoin { join_type, eq_conditions, dist: JoinDistribution },
    PhysicalNestLoopJoin { ... },
    PhysicalHashAggregate { agg_mode: AggMode },
    PhysicalSort { ... },
    PhysicalDistribution { spec: DistributionSpec },
    PhysicalCTEProduce { cte_id },
    PhysicalCTEConsume { cte_id },
    PhysicalWindow { ... },
    // ...
}

pub enum JoinDistribution {
    Shuffle,    // both sides hash redistributed by join keys
    Broadcast,  // right side broadcast to all left instances
    Colocate,   // data already co-located
}

pub enum AggMode {
    Single,
    Local,   // partial aggregation before exchange
    Global,  // final aggregation after exchange
}
```

### 3.3 Physical Properties

```rust
#[derive(Clone, Hash, Eq, PartialEq)]
pub struct PhysicalPropertySet {
    pub distribution: DistributionSpec,
    pub ordering: OrderingSpec,
}

#[derive(Clone, Hash, Eq, PartialEq)]
pub enum DistributionSpec {
    Any,
    Gather,
    HashPartitioned(Vec<ExprId>),
    Random,
}

#[derive(Clone, Hash, Eq, PartialEq)]
pub enum OrderingSpec {
    Any,
    Required(Vec<SortItem>),
}
```

### 3.4 Property Rules per Operator

| Physical Operator | required_input | provided_output |
|-------------------|---------------|-----------------|
| PhysicalScan | — | Random |
| PhysicalFilter | pass-through | pass-through |
| PhysicalProject | pass-through (column-mapped) | pass-through (column-mapped) |
| PhysicalHashJoin(Shuffle) | [Hash(left_keys), Hash(right_keys)] | Hash(left_keys) |
| PhysicalHashJoin(Broadcast) | [Any, Gather] | pass-through left |
| PhysicalNestLoopJoin | [Gather, Gather] | Gather |
| PhysicalHashAggregate(Single) | Hash(group_keys) or Gather(no group by) | Hash(group_keys) |
| PhysicalHashAggregate(Local) | Any | Hash(group_keys) |
| PhysicalHashAggregate(Global) | Hash(group_keys) | Hash(group_keys) |
| PhysicalSort(final) | Gather | Gather + Ordered |
| PhysicalWindow | Hash(partition_keys) or Gather | pass-through |
| PhysicalCTEConsume | — | Random |

### 3.5 Rule System

```rust
pub trait Rule {
    fn pattern(&self) -> &Pattern;
    fn rule_type(&self) -> RuleType;  // Transformation | Implementation
    fn apply(&self, expr: &GroupExpr, memo: &mut Memo) -> Vec<GroupExpr>;
}
```

Initial rule set for TPC-DS:

| Rule | Type | Effect |
|------|------|--------|
| JoinCommutativity | Transform | A JOIN B <-> B JOIN A |
| JoinAssociativity | Transform | (A JOIN B) JOIN C <-> A JOIN (B JOIN C) |
| AggSplit | Transform | Single Agg -> Local + Global |
| CTEInlineOrShare | Transform | CTE inline vs multicast (cost-based) |
| ScanToPhysical | Implement | LogicalScan -> PhysicalScan |
| JoinToHashJoin | Implement | LogicalJoin -> PhysicalHashJoin(Shuffle) + PhysicalHashJoin(Broadcast) |
| JoinToNestLoop | Implement | LogicalJoin(no eq) -> PhysicalNestLoopJoin |
| AggToHashAgg | Implement | LogicalAggregate -> PhysicalHashAggregate |
| SortToPhysical | Implement | LogicalSort -> PhysicalSort |

### 3.6 Optimization Flow

```
optimize(logical_plan, stats):
  1. rewrite(logical_plan)           // RBO: deterministic transforms
  2. memo = init_memo(rewritten)     // Register into Memo groups
  3. explore(memo, transform_rules)  // Expand logical equivalences
  4. implement(memo, impl_rules)     // Generate physical alternatives
  5. enforce(memo, root_required)    // Top-down property enforcement
  6. return extract_best(memo, root_required)
```

Property enforcement (step 5) pseudo-code:

```
optimize_group(group_id, required_props):
  if cached(group_id, required_props): return cached_cost

  for each physical_expr in group:
    provided = expr.provided_properties()

    if !required.satisfied_by(provided):
      // Insert enforcer (PhysicalDistribution)
      enforcer_cost = estimate_enforcer(required, provided, stats)
      child_cost = optimize_group(group_id, provided)
      total = enforcer_cost + child_cost
      update_best(group_id, required, total)

    // Recurse into children with operator's requirements
    child_reqs = expr.required_input_properties(required)
    children_cost = sum(optimize_group(child, req) for child, req in zip)
    total = expr.cost(stats) + children_cost
    update_best(group_id, required, total)
```

## 4. CTE Reuse & Multicast

### 4.1 Analyzer Changes

Current behavior: CTE references are inlined (each reference gets a full copy of the CTE subplan).

New behavior:

```rust
pub struct CTERegistry {
    entries: HashMap<CteId, CTEEntry>,
}

pub struct CTEEntry {
    pub resolved_query: ResolvedQuery,
    pub ref_count: u32,
    pub column_aliases: Vec<String>,
}
```

1. First pass: analyze each CTE in WITH clause, register in CTERegistry with ref_count
2. Second pass: analyze main query body. On CTE reference:
   - ref_count == 1: inline (no benefit from multicast)
   - ref_count >= 2: emit CTEConsume { cte_id }

### 4.2 LogicalPlan Nodes

```rust
pub struct CTEProduceNode {
    pub cte_id: CteId,
    pub input: Box<LogicalPlan>,
    pub output_columns: Vec<OutputColumn>,
}

pub struct CTEConsumeNode {
    pub cte_id: CteId,
    pub output_columns: Vec<OutputColumn>,
}
```

Plan structure for multi-referenced CTE:

```
CTEProduce(cte_id=0)
  └── Scan(store_returns) JOIN Scan(date_dim)    // computed once

Root
  ├── SubqueryIn
  │   └── CTEConsume(cte_id=0)                   // reference
  └── SubqueryIn
      └── CTEConsume(cte_id=0)                   // reference
```

### 4.3 CBO Decision: Inline vs Multicast

```
CTEInlineOrShareRule:
  ref_count == 1 → always inline
  ref_count >= 2 →
    inline_cost  = ref_count * cte_subtree_cost
    share_cost   = cte_subtree_cost + multicast_overhead + ref_count * exchange_cost
    → choose lower cost
```

### 4.4 Physical Mapping

- `PhysicalCTEProduce` → independent fragment with `MULTI_CAST_DATA_STREAM_SINK`
  - Each consumer gets one `DataStreamSink` in the sink list
- `PhysicalCTEConsume` → `EXCHANGE_NODE` receiving from multicast fragment

```
Fragment 0 (CTE Produce):
  Scan -> Join -> ... -> MultiCastDataStreamSink
                          ├── DataStreamSink -> Fragment 1
                          └── DataStreamSink -> Fragment 2

Fragment 1 (CTE Consumer A):
  ExchangeNode(from F0) -> Filter -> ...

Fragment 2 (CTE Consumer B):
  ExchangeNode(from F0) -> Aggregate -> ...
```

## 5. Fragment Planning

### 5.1 Fragment Planner

Splits Cascades output (physical plan with PhysicalDistribution nodes) into a fragment DAG.

```rust
pub struct FragmentPlanner {
    fragments: Vec<PlanFragment>,
    next_fragment_id: u32,
}

pub struct PlanFragment {
    pub id: FragmentId,
    pub plan_root: PhysicalPlanNode,
    pub partition: DataPartition,
    pub sink: DataSinkType,
    pub children: Vec<FragmentId>,
}
```

Algorithm:
1. Post-order traversal of physical plan
2. On `PhysicalDistribution`: create child fragment containing subtree below, replace with `ExchangeNode` in current fragment. Child fragment sink = `DataStreamSink` targeting current fragment's ExchangeNode.
3. On `PhysicalCTEProduce`: create multicast fragment, sink = `MultiCastDataStreamSink`
4. On `PhysicalCTEConsume`: replace with `ExchangeNode` receiving from multicast fragment

### 5.2 Fragment Instance Assignment

| Fragment Type | Instance Count | Rationale |
|--------------|---------------|-----------|
| Scan fragment | ceil(total_files / files_per_instance) | Split by data shards |
| Hash exchange fragment | Aligned with upstream sender count | Match hash buckets |
| Gather fragment (root) | 1 | Collect results |
| Multicast fragment | 1 | Compute CTE once |

Scan range assignment: Iceberg/Parquet files distributed evenly across instances.

### 5.3 Example Fragment DAG

```
Query: SELECT ... FROM store_sales ss JOIN date_dim d ON ss.d_id = d.d_id
       WHERE d.d_year = 2001 GROUP BY ss.s_id ORDER BY sum_sales LIMIT 100

Fragment 0 (scan store_sales):       partition=Random, instances=4
  PhysicalScan(store_sales)
  sink=DataStreamSink(Hash[ss.d_id]) -> Fragment 2

Fragment 1 (scan date_dim):          partition=Random, instances=1
  PhysicalScan(date_dim)
  sink=DataStreamSink(Hash[d.d_id]) -> Fragment 2

Fragment 2 (join + local agg):       partition=Hash[d_id], instances=4
  ExchangeNode(from F0)
  ExchangeNode(from F1)
  HashJoin(eq=[ss.d_id = d.d_id])
  Filter(d.d_year = 2001)
  HashAggregate(LOCAL, group_by=[ss.s_id])
  sink=DataStreamSink(Hash[s_id]) -> Fragment 3

Fragment 3 (global agg):             partition=Hash[s_id], instances=4
  ExchangeNode(from F2)
  HashAggregate(GLOBAL, group_by=[ss.s_id])
  sink=DataStreamSink(Gather) -> Fragment 4

Fragment 4 (sort + result):          partition=Gather, instances=1
  ExchangeNode(from F3)
  Sort(sum_sales) + Limit(100)
  sink=ResultSink
```

## 6. Fragment Execution Coordination

### 6.1 Execution Coordinator

```rust
pub struct ExecutionCoordinator {
    query_id: TUniqueId,
    fragments: Vec<FragmentBuildResult>,
    dag: HashMap<FragmentId, Vec<FragmentId>>,
}
```

### 6.2 Per-Fragment Physical Emission

```rust
pub struct FragmentBuildResult {
    pub fragment_id: FragmentId,
    pub plan: TPlan,
    pub desc_tbl: TDescriptorTable,
    pub output_sink: TDataSink,
    pub partition: TDataPartition,
    pub instances: Vec<FragmentInstanceParams>,
}

pub struct FragmentInstanceParams {
    pub instance_id: TUniqueId,
    pub scan_ranges: BTreeMap<i32, Vec<TScanRangeParams>>,
    pub dop: i32,
}
```

Existing `ThriftEmitter` change is minimal: emit per-fragment subtree instead of full tree. New `emit_exchange_node()` added.

### 6.3 Submission Flow

1. Compute sender counts for all exchange nodes
2. Topological sort fragments (leaf-first, root-last)
3. For each fragment, construct `TExecBatchPlanFragmentsParams` and submit via `submit_exec_batch_plan_fragments`
4. Fetch results from root fragment's `ResultSink`

### 6.4 Sender Count Computation

For `DataStreamSink`: sender_count = number of instances in source fragment.
For `MultiCastDataStreamSink`: each sub-sink independently contributes sender_count = source fragment instances.

### 6.5 Integration with Existing Code

| Component | File | Change |
|-----------|------|--------|
| Fragment submission | `internal_service.rs` | No change — reuse existing batch submission |
| Exchange send | `exchange_sender.rs` + `grpc_client.rs` | No change |
| Exchange receive | `exchange.rs` + `grpc_server.rs` | No change |
| Multicast sink | `multi_cast_data_stream_sink.rs` | No change |
| Result fetch | `result_buffer.rs` | No change |
| Standalone engine | `engine.rs` | Change: single-fragment -> ExecutionCoordinator |
| Physical emitter | `physical/emitter/` | Add: `emit_exchange_node()`; change: per-fragment emission |

## 7. Implementation Phases

| Phase | Content | Validation |
|-------|---------|------------|
| Phase 1: CTE Multicast | Analyzer CTE registry + CTEProduce/CTEConsume nodes + simplified fragment planner (only splits at CTE boundaries, no distribution-based splitting yet — entire non-CTE plan remains single fragment) + multicast fragment + ExecutionCoordinator | CTE-heavy TPC-DS queries (q1, q4, q11, etc.) pass instead of timeout |
| Phase 2: Cascades Framework | Memo + Group + GroupExpr + RBO Rewriter + basic CBO search (no distribution) + migrate existing 3 passes as rules | Existing 60-70 passing queries don't regress |
| Phase 3: Distribution Property | DistributionSpec + property enforcement + PhysicalDistribution insertion + join distribution decision (Shuffle/Broadcast) + aggregate two-stage split | Multi-fragment plans generated correctly, hash redistribute queries pass |
| Phase 4: Fragment Instance Assignment | Scan range sharding + multi-instance parallelism + full sender count computation | TPC-DS large table scans parallel, overall pass rate > 90% |

## 8. Key Files Index

### New Modules
- `src/sql/cascades/` — Memo, Group, GroupExpr, Operator, Rule trait, optimizer entry
- `src/sql/cascades/rules/` — transformation and implementation rules
- `src/sql/cascades/properties.rs` — DistributionSpec, OrderingSpec, PhysicalPropertySet
- `src/sql/cascades/cost.rs` — cost model (migrated + extended with network_cost)
- `src/sql/cascades/statistics.rs` — cardinality/NDV estimation
- `src/sql/fragment/` — FragmentPlanner, PlanFragment, fragment splitting algorithm
- `src/standalone/coordinator.rs` — ExecutionCoordinator

### Modified Modules
- `src/sql/analyzer/mod.rs` — CTERegistry, ref counting
- `src/sql/analyzer/resolve_from.rs` — CTE consume instead of inline
- `src/sql/plan/mod.rs` — add CTEProduce, CTEConsume to LogicalPlan enum
- `src/sql/physical/emitter/mod.rs` — per-fragment emission, emit_exchange_node
- `src/sql/physical/nodes.rs` — exchange node Thrift construction
- `src/standalone/engine.rs` — integrate ExecutionCoordinator

### Unchanged Modules
- `src/service/internal_service.rs` — batch fragment submission
- `src/runtime/exchange.rs` — exchange receiver
- `src/service/exchange_sender.rs` — exchange sender
- `src/exec/operators/multi_cast_data_stream_sink.rs` — multicast operator
- `src/runtime/result_buffer.rs` — result collection
- `src/lower/` — plan lowering (no changes, consumes Thrift as before)
- `src/exec/pipeline/` — pipeline execution (no changes)
