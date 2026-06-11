# Standalone Runtime Filter Design

## Overview

Enable runtime filter support in NovaRocks standalone mode, covering query planning (RF identification and pushdown) and coordinator-level parameter assembly. The execution layer already supports runtime filters in compat mode; this design adds the planning layer to make them work end-to-end in standalone mode via gRPC.

Aligned with StarRocks FE runtime filter behavior (`JoinNode.buildRuntimeFilters()`, `DefaultCoordinator.setGlobalRuntimeFilterParams()`).

## Scope

| Feature | Included | Notes |
|---------|----------|-------|
| Hash Join equi-condition RF | Yes | Core feature |
| Local RF (same fragment) | Yes | Build/probe in same fragment |
| Remote RF (cross fragment) | Yes | Probe scan in child fragment, gRPC transmission |
| Bloom filter | Yes | Primary filter type |
| In filter | Yes | Small cardinality scenarios |
| Join type eligibility | Yes | INNER, LEFT SEMI, RIGHT OUTER/SEMI/ANTI, CROSS |
| Merge node coordination | Yes | Simplified to self (single instance) |
| gRPC transmission | Yes | Reuse existing TransmitRuntimeFilter RPC |
| TopN filter / Agg filter | No | Future extension |
| Broadcast speculative delivery | No | Single instance, not needed |
| Skew join optimization | No | Future extension |
| Multi-instance parallelism | No | Current architecture is single instance per fragment |

### Key Simplifications (Single Instance)

- `runtime_filter_builder_number` is always 1 (no partial filter merging)
- Merge node = self address
- Layout: local = `SINGLETON`, global = `GLOBAL_SHUFFLE_1L` (or `SINGLETON` for broadcast)
- No `broadcast_grf_senders/destinations`

## Architecture

### Data Flow

```
Cascades Optimizer
  -> fragment_builder.rs: visit_hash_join() tracks scan/join metadata
  -> RuntimeFilterPlanner: post-build pass, pushdown to find probe scan targets
  -> TRuntimeFilterDescription populated on THashJoinNode.build_runtime_filters

Coordinator
  -> Iterates all fragments, collects build/probe side RFs
  -> Sets merge node address, builder_number
  -> Builds TRuntimeFilterParams, attaches to exec_params

Execution (existing, zero modifications)
  -> RuntimeFilterHub registers filter specs
  -> Hash join build publishes filter
  -> gRPC TransmitRuntimeFilter for remote filters
  -> Scan probe waits for filter, applies on arrival
```

### Files Modified

| File | Change |
|------|--------|
| `src/sql/cascades/fragment_builder.rs` | Add scan/join tracking during visit; call RF planner post-build |
| `src/standalone/coordinator.rs` | Build TRuntimeFilterParams, patch merge node addresses |
| `src/sql/cascades/runtime_filter_planner.rs` (new) | RF planning logic: scan target identification, TRuntimeFilterDescription generation |

### Files NOT Modified (execution layer)

| File | Reason |
|------|--------|
| `src/lower/fragment.rs` | Already parses TRuntimeFilterDescription |
| `src/runtime/runtime_filter_hub.rs` | Already handles filter registration/publish/receive |
| `src/runtime/runtime_filter_worker.rs` | builder_number=1 degenerates to pass-through |
| `src/exec/operators/hash_join_build_sink.rs` | Already publishes filters and sends remote |
| `src/service/exchange_sender.rs` | Already has gRPC branch for `#[cfg(not(feature = "compat"))]` |
| `src/service/grpc_server.rs` / `grpc_client.rs` | Already implements TransmitRuntimeFilter |
| Scan operators | Already wait for and apply filters |

## Detailed Design

### 1. Build-Phase Tracking (fragment_builder.rs)

Add tracking structures to `PlanFragmentBuilder`:

```rust
/// tuple_id -> owning scan node and fragment
scan_tuple_owners: HashMap<i32, ScanTupleOwner>,

/// hash join node_id -> owning fragment
join_fragment_map: HashMap<i32, FragmentId>,

struct ScanTupleOwner {
    scan_node_id: i32,
    fragment_id: FragmentId,
}
```

**In `visit_scan()`:** After creating the scan node, register each output tuple:
```
scan_tuple_owners[tuple_id] = ScanTupleOwner { scan_node_id, current_fragment_id }
```

**In `visit_hash_join()`:** After creating the join node, register:
```
join_fragment_map[join_node_id] = current_fragment_id
```

These maps are passed to the RF planner along with the `MultiFragmentBuildResult`.

### 2. RF Planning Pass (runtime_filter_planner.rs)

New module: `src/sql/cascades/runtime_filter_planner.rs`

Entry function:

```rust
pub(crate) fn plan_runtime_filters(
    fragment_results: &mut [FragmentBuildResult],
    scan_tuple_owners: &HashMap<i32, ScanTupleOwner>,
    join_fragment_map: &HashMap<i32, FragmentId>,
    desc_builder: &DescriptorTableBuilder,
) -> RuntimeFilterPlanResult
```

**Algorithm:**

1. Iterate all fragments' plan_nodes, find `HASH_JOIN_NODE` nodes.

2. Filter by join type — only generate RF for:
   - INNER_JOIN
   - LEFT_SEMI_JOIN
   - RIGHT_OUTER_JOIN, RIGHT_SEMI_JOIN, RIGHT_ANTI_JOIN
   - CROSS_JOIN
   (Aligned with StarRocks `JoinNode.buildRuntimeFilters()`)

3. For each `eq_join_conjuncts[i]` = `TEqJoinCondition { left, right }`:
   - `right` (build-side TExpr) -> `build_expr`
   - `left` (probe-side TExpr) -> candidate target expr
   - **Check if `left` is a simple SlotRef** (v1 limitation: skip complex expressions)
   - Extract slot_id -> look up tuple_id via desc_builder -> look up `scan_tuple_owners`
   - If scan target found -> create RF description

4. Determine local vs remote:
   - `join_fragment_id == scan_fragment_id` -> local, `has_remote_targets = false`
   - Otherwise -> remote, `has_remote_targets = true`

5. Generate `TRuntimeFilterDescription`:
   ```
   filter_id:                   globally incrementing
   build_expr:                  right TExpr from eq_join_conjunct
   expr_order:                  index i in eq_conditions
   plan_node_id_to_target_expr: { scan_node_id -> left TExpr }
   has_remote_targets:          whether cross-fragment
   build_plan_node_id:          hash join node_id
   build_join_mode:             mapped from JoinDistribution
   filter_type:                 JOIN_FILTER
   layout:                      see Layout Strategy below
   ```

6. Patch hash join node: set `hash_join_node.build_runtime_filters = Some(vec![...])` on the TPlanNode.

**Output:**

```rust
pub(crate) struct RuntimeFilterPlanResult {
    /// filter_id -> RF description (global)
    pub all_filters: HashMap<i32, TRuntimeFilterDescription>,
    /// fragment_id -> build-side filter IDs in that fragment
    pub build_side_filters: HashMap<FragmentId, Vec<i32>>,
    /// fragment_id -> (filter_id, scan_node_id) for probe-side targets
    pub probe_side_filters: HashMap<FragmentId, Vec<(i32, i32)>>,
}
```

### 3. Join Distribution -> RF Join Mode Mapping

```
JoinDistribution::Broadcast -> TRuntimeFilterBuildJoinMode::BROADCAST
JoinDistribution::Shuffle   -> TRuntimeFilterBuildJoinMode::PARTITIONED
JoinDistribution::Colocate  -> TRuntimeFilterBuildJoinMode::COLOCATE
```

### 4. Layout Strategy (Simplified for Single Instance)

```
local_layout:  SINGLETON  (all join modes)
global_layout:
  Broadcast -> SINGLETON
  Shuffle   -> GLOBAL_SHUFFLE_1L
  Colocate  -> GLOBAL_BUCKET_1L
pipeline_level_multi_partitioned: false
num_instances: 1
num_drivers_per_instance: pipeline_dop (from config, capped at available_parallelism or 4)
```

### 5. Coordinator RF Parameter Assembly (coordinator.rs)

New function called during `execute()`, after instance ID assignment and sink wiring, before building `TExecPlanFragmentParams`:

```rust
fn setup_runtime_filter_params(
    rf_plan: &RuntimeFilterPlanResult,
    instance_map: &BTreeMap<FragmentId, (i64, i64)>,
    exchange_address: &TNetworkAddress,
) -> HashMap<FragmentId, TRuntimeFilterParams>
```

**Logic:**

1. **`id_to_prober_params`**: For each `(filter_id, scan_node_id)` in `probe_side_filters`:
   ```
   TRuntimeFilterProberParams {
     fragment_instance_id: instance_map[probe_fragment_id],
     fragment_instance_address: exchange_address,
   }
   ```

2. **`runtime_filter_builder_number`**: For each filter_id in `build_side_filters`:
   ```
   runtime_filter_builder_number[filter_id] = 1
   ```

3. **`runtime_filter_max_size`**: From config (default 16MB).

4. **Merge node addresses**: Patch `TRuntimeFilterDescription.runtime_filter_merge_nodes` for remote filters:
   ```
   runtime_filter_merge_nodes = vec![exchange_address]
   ```

5. **Attach to exec_params**: All fragments receive the same `TRuntimeFilterParams` containing the complete `id_to_prober_params` and `runtime_filter_builder_number` maps. This matches StarRocks behavior where the merge node (which in standalone is every fragment) needs the full picture to route merged filters correctly.

**Integration point in coordinator flow:**

```
execute()
  -> assign instance IDs
  -> classify fragments (CTE / Stream / Root)
  -> wire multicast sinks
  -> [NEW] setup_runtime_filter_params()
  -> [NEW] patch RF merge node addresses on TRuntimeFilterDescription
  -> build TExecPlanFragmentParams (with rf_params)
  -> spawn background threads for non-root fragments
  -> execute root fragment
```

### 6. Execution Layer Integration (Zero Changes)

The execution layer already handles runtime filters end-to-end:

- **Lowering** (`lower/fragment.rs`): Parses `TRuntimeFilterDescription` from plan and registers with `RuntimeFilterHub`.
- **Build side** (`hash_join_build_sink.rs`): Publishes local filters via `hub.publish_filters()`, sends remote filters via `exchange_sender::send_runtime_filter()`.
- **Network** (`exchange_sender.rs`): `#[cfg(not(feature = "compat"))]` branch calls `grpc_client::transmit_runtime_filter()`.
- **Receive** (`grpc_server.rs`): `transmit_runtime_filter()` dispatches to `handle_transmit_runtime_filter()`, which routes to `RuntimeFilterWorker` (partial) or `RuntimeFilterHub` (final).
- **Worker** (`runtime_filter_worker.rs`): With `builder_number=1`, the first (only) partial filter immediately becomes the merged result.
- **Probe** (scan operators): Wait via `RuntimeFilterProbe::dependency_or_timeout()`, apply filter on arrival.

## End-to-End Example

```sql
SELECT * FROM t1 JOIN t2 ON t1.id = t2.id WHERE t1.name = 'foo'
```

Optimizer produces 2 fragments:
- Fragment 0 (root): HashJoin(probe=Exchange, build=Scan(t2))
- Fragment 1 (child): Scan(t1)

**RF Planning produces:**
```
TRuntimeFilterDescription {
  filter_id: 0,
  build_expr: SlotRef(t2.id),
  expr_order: 0,
  plan_node_id_to_target_expr: { scan_node_3 -> SlotRef(t1.id) },
  has_remote_targets: true,
  build_plan_node_id: 1,  // hash join node
  build_join_mode: PARTITIONED,
  filter_type: JOIN_FILTER,
  layout: { local: SINGLETON, global: GLOBAL_SHUFFLE_1L, num_instances: 1, num_drivers: 4 },
}
```

**Coordinator produces:**
```
TRuntimeFilterParams {
  id_to_prober_params: {
    0 -> [{ fragment_instance_id: (qid, 2), address: 127.0.0.1:grpc_port }]
  },
  runtime_filter_builder_number: { 0 -> 1 },
  runtime_filter_max_size: 16777216,
}

Patched on description:
  runtime_filter_merge_nodes: [127.0.0.1:grpc_port]
```

**Execution flow:**
1. Coordinator spawns Fragment 1 (Scan t1) in background thread
2. Coordinator executes Fragment 0 (root) in foreground
3. Hash join builds hash table from t2, produces bloom filter on t2.id
4. Hash join calls `hub.publish_filters()` (no local targets in this fragment)
5. Hash join calls `send_runtime_filter()` -> gRPC to 127.0.0.1:grpc_port
6. gRPC server receives, routes to RuntimeFilterWorker (is_partial=true)
7. Worker has builder_number=1, immediately merges (no-op) and sends to probe targets
8. RuntimeFilterHub in Fragment 1 receives the merged filter
9. Scan(t1) applies bloom filter on t1.id, skipping non-matching rows
10. Filtered t1 rows flow through exchange to hash join probe side
