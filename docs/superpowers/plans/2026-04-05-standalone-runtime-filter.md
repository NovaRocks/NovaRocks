# Standalone Runtime Filter Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Enable runtime filter planning and coordination in standalone mode so hash join bloom/in filters reach scan nodes via gRPC, matching StarRocks FE behavior.

**Architecture:** A post-build RF planning pass in `fragment_builder.rs` walks all fragment plan nodes, identifies hash join equi-conditions eligible for RF, finds target scan nodes via slot→tuple→scan tracking, and populates `TRuntimeFilterDescription` on join nodes. The coordinator then assembles `TRuntimeFilterParams` (merge node, builder number, prober params) and attaches them to each fragment's exec params. The execution layer already handles everything downstream and requires zero changes.

**Tech Stack:** Rust, Thrift-generated types (`runtime_filter::TRuntimeFilterDescription`, `TRuntimeFilterParams`), existing gRPC `TransmitRuntimeFilter` RPC.

**Spec:** `docs/superpowers/specs/2026-04-05-standalone-runtime-filter-design.md`

---

### Task 1: Add scan/join tracking to PlanFragmentBuilder

**Files:**
- Modify: `src/sql/cascades/fragment_builder.rs`

This task adds the metadata maps that the RF planner needs: which tuple belongs to which scan node in which fragment, and which fragment each hash join lives in.

- [ ] **Step 1: Add tracking fields to PlanFragmentBuilder struct**

In `src/sql/cascades/fragment_builder.rs`, add two new fields to the `PlanFragmentBuilder` struct and a helper struct above it:

```rust
// Above PlanFragmentBuilder struct definition (after VisitResult):

#[derive(Clone, Debug)]
pub(crate) struct ScanTupleOwner {
    pub scan_node_id: i32,
    pub fragment_id: FragmentId,
}
```

Add to `PlanFragmentBuilder` fields (after `cte_fragments`):

```rust
    /// tuple_id -> owning scan node and fragment (for RF target identification).
    pub(crate) scan_tuple_owners: HashMap<i32, ScanTupleOwner>,
    /// hash join node_id -> (fragment_id, join_op, distribution) for RF eligibility.
    pub(crate) join_fragment_map: HashMap<i32, FragmentId>,
```

Initialize them in `build()`:

```rust
            scan_tuple_owners: HashMap::new(),
            join_fragment_map: HashMap::new(),
```

- [ ] **Step 2: Record scan tuple ownership in visit_scan()**

At the end of `visit_scan()`, just before the `Ok(VisitResult { ... })` return, add:

```rust
        // Track tuple -> scan node ownership for runtime filter planning.
        let current_frag = self.current_fragment_id()?;
        self.scan_tuple_owners.insert(
            scan_tuple_id,
            ScanTupleOwner {
                scan_node_id,
                fragment_id: current_frag,
            },
        );
```

- [ ] **Step 3: Record join node fragment in visit_hash_join()**

In `visit_hash_join()`, right after `let join_node_id = self.alloc_node();`, add:

```rust
        // Track join node -> fragment for runtime filter planning.
        if let Ok(frag_id) = self.current_fragment_id() {
            self.join_fragment_map.insert(join_node_id, frag_id);
        }
```

- [ ] **Step 4: Verify it compiles**

Run: `cargo build 2>&1 | tail -5`
Expected: Build succeeds (new fields are populated but not yet consumed).

- [ ] **Step 5: Commit**

```bash
git add src/sql/cascades/fragment_builder.rs
git commit -m "Add scan/join tracking to PlanFragmentBuilder for RF planning"
```

---

### Task 2: Create runtime_filter_planner module

**Files:**
- Create: `src/sql/cascades/runtime_filter_planner.rs`
- Modify: `src/sql/cascades/mod.rs`

Core RF planning logic: walk fragment plan nodes, find eligible hash joins, identify scan targets, produce `TRuntimeFilterDescription`.

- [ ] **Step 1: Register the new module**

In `src/sql/cascades/mod.rs`, add after `pub(crate) mod fragment_builder;`:

```rust
pub(crate) mod runtime_filter_planner;
```

- [ ] **Step 2: Create runtime_filter_planner.rs with types and entry function**

Create `src/sql/cascades/runtime_filter_planner.rs`:

```rust
//! Post-build runtime filter planning pass.
//!
//! Walks all fragment plan nodes, identifies hash join equi-conditions
//! eligible for runtime filters, finds target scan nodes, and produces
//! `TRuntimeFilterDescription` on join nodes.

use std::collections::{BTreeMap, HashMap};

use crate::exprs;
use crate::plan_nodes;
use crate::runtime_filter;
use crate::sql::cascades::operator::JoinDistribution;
use crate::sql::fragment::FragmentId;
use crate::sql::physical::FragmentBuildResult;

use super::fragment_builder::ScanTupleOwner;

/// Result of the runtime filter planning pass.
pub(crate) struct RuntimeFilterPlanResult {
    /// filter_id -> RF description.
    pub all_filters: HashMap<i32, runtime_filter::TRuntimeFilterDescription>,
    /// fragment_id -> build-side filter IDs in that fragment.
    pub build_side_filters: HashMap<FragmentId, Vec<i32>>,
    /// fragment_id -> (filter_id, scan_node_id) for probe-side targets.
    pub probe_side_filters: HashMap<FragmentId, Vec<(i32, i32)>>,
}

/// Plan runtime filters for all fragments.
///
/// Iterates plan nodes across fragments, finds hash join nodes with
/// equi-conditions, identifies target scan nodes via slot->tuple->scan
/// lookup, and populates `build_runtime_filters` on join nodes.
pub(crate) fn plan_runtime_filters(
    fragment_results: &mut [FragmentBuildResult],
    scan_tuple_owners: &HashMap<i32, ScanTupleOwner>,
    join_fragment_map: &HashMap<i32, FragmentId>,
    join_distributions: &HashMap<i32, JoinDistribution>,
    pipeline_dop: i32,
) -> RuntimeFilterPlanResult {
    let mut next_filter_id: i32 = 0;
    let mut all_filters: HashMap<i32, runtime_filter::TRuntimeFilterDescription> = HashMap::new();
    let mut build_side_filters: HashMap<FragmentId, Vec<i32>> = HashMap::new();
    let mut probe_side_filters: HashMap<FragmentId, Vec<(i32, i32)>> = HashMap::new();

    // Collect (fragment_id, node_index, join_node_id) for all eligible hash joins.
    let mut join_targets: Vec<(FragmentId, usize, i32)> = Vec::new();
    for fr in fragment_results.iter() {
        for (idx, node) in fr.plan.nodes.iter().enumerate() {
            if node.node_type == plan_nodes::TPlanNodeType::HASH_JOIN_NODE {
                if is_rf_eligible_join_op(node) {
                    join_targets.push((fr.fragment_id, idx, node.node_id));
                }
            }
        }
    }

    // For each eligible hash join, generate RF descriptions.
    for (join_frag_id, node_idx, join_node_id) in &join_targets {
        // Find the fragment and node (re-borrow immutably for reading).
        let fr = fragment_results
            .iter()
            .find(|f| f.fragment_id == *join_frag_id)
            .unwrap();
        let node = &fr.plan.nodes[*node_idx];
        let hash_join = match &node.hash_join_node {
            Some(hj) => hj,
            None => continue,
        };

        let distribution = join_distributions.get(join_node_id).cloned();

        let mut rf_descs: Vec<runtime_filter::TRuntimeFilterDescription> = Vec::new();

        for (expr_order, eq_cond) in hash_join.eq_join_conjuncts.iter().enumerate() {
            // The probe expr is `left` (probe side), build expr is `right`.
            let probe_expr = &eq_cond.left;
            let build_expr = &eq_cond.right;

            // v1: only support simple SlotRef probe expressions.
            let (probe_slot_id, probe_tuple_id) = match extract_slot_ref(probe_expr) {
                Some(sr) => sr,
                None => continue,
            };

            // Find the scan node that owns this tuple.
            let scan_owner = match scan_tuple_owners.get(&probe_tuple_id) {
                Some(owner) => owner,
                None => continue,
            };

            let has_remote_targets = *join_frag_id != scan_owner.fragment_id;

            let filter_id = next_filter_id;
            next_filter_id += 1;

            let build_join_mode = match distribution {
                Some(JoinDistribution::Broadcast) => {
                    runtime_filter::TRuntimeFilterBuildJoinMode::BORADCAST
                }
                Some(JoinDistribution::Shuffle) => {
                    runtime_filter::TRuntimeFilterBuildJoinMode::PARTITIONED
                }
                Some(JoinDistribution::Colocate) => {
                    runtime_filter::TRuntimeFilterBuildJoinMode::COLOCATE
                }
                None => runtime_filter::TRuntimeFilterBuildJoinMode::BORADCAST,
            };

            let (local_layout, global_layout) = match &distribution {
                Some(JoinDistribution::Broadcast) => (
                    runtime_filter::TRuntimeFilterLayoutMode::SINGLETON,
                    runtime_filter::TRuntimeFilterLayoutMode::SINGLETON,
                ),
                Some(JoinDistribution::Shuffle) => (
                    runtime_filter::TRuntimeFilterLayoutMode::SINGLETON,
                    runtime_filter::TRuntimeFilterLayoutMode::GLOBAL_SHUFFLE_1L,
                ),
                Some(JoinDistribution::Colocate) => (
                    runtime_filter::TRuntimeFilterLayoutMode::SINGLETON,
                    runtime_filter::TRuntimeFilterLayoutMode::GLOBAL_BUCKET_1L,
                ),
                None => (
                    runtime_filter::TRuntimeFilterLayoutMode::SINGLETON,
                    runtime_filter::TRuntimeFilterLayoutMode::SINGLETON,
                ),
            };

            let layout = runtime_filter::TRuntimeFilterLayout::new(
                filter_id,                                                   // filter_id
                local_layout,                                                // local_layout
                global_layout,                                               // global_layout
                false,                                                       // pipeline_level_multi_partitioned
                1_i32,                                                       // num_instances
                pipeline_dop,                                                // num_drivers_per_instance
                None::<Vec<i32>>,                                            // bucketseq_to_instance
                None::<Vec<i32>>,                                            // bucketseq_to_driverseq
                None::<Vec<i32>>,                                            // bucketseq_to_partition
                None::<Vec<crate::partitions::TBucketProperty>>,             // bucket_properties
            );

            let mut target_map = BTreeMap::new();
            target_map.insert(scan_owner.scan_node_id, probe_expr.clone());

            let desc = runtime_filter::TRuntimeFilterDescription::new(
                filter_id,                                                              // filter_id
                build_expr.clone(),                                                     // build_expr
                expr_order as i32,                                                      // expr_order
                target_map,                                                             // plan_node_id_to_target_expr
                has_remote_targets,                                                     // has_remote_targets
                None::<i64>,                                                            // bloom_filter_size (let BE decide)
                None::<Vec<crate::types::TNetworkAddress>>,                             // runtime_filter_merge_nodes (set by coordinator)
                build_join_mode,                                                        // build_join_mode
                None::<crate::types::TUniqueId>,                                        // sender_finst_id
                *join_node_id,                                                          // build_plan_node_id
                None::<Vec<crate::types::TUniqueId>>,                                   // broadcast_grf_senders
                None::<Vec<runtime_filter::TRuntimeFilterDestination>>,                 // broadcast_grf_destinations
                None::<Vec<i32>>,                                                       // bucketseq_to_instance
                None::<BTreeMap<i32, Vec<exprs::TExpr>>>,                               // plan_node_id_to_partition_by_exprs
                runtime_filter::TRuntimeFilterBuildType::JOIN_FILTER,                   // filter_type
                layout,                                                                 // layout
                None::<bool>,                                                           // build_from_group_execution
                None::<bool>,                                                           // is_broad_cast_join_in_skew
                None::<i32>,                                                            // skew_shuffle_filter_id
            );

            rf_descs.push(desc.clone());
            all_filters.insert(filter_id, desc);
            build_side_filters
                .entry(*join_frag_id)
                .or_default()
                .push(filter_id);
            probe_side_filters
                .entry(scan_owner.fragment_id)
                .or_default()
                .push((filter_id, scan_owner.scan_node_id));
        }

        // Patch the join node in-place with build_runtime_filters.
        if !rf_descs.is_empty() {
            let fr_mut = fragment_results
                .iter_mut()
                .find(|f| f.fragment_id == *join_frag_id)
                .unwrap();
            if let Some(ref mut hj) = fr_mut.plan.nodes[*node_idx].hash_join_node {
                hj.build_runtime_filters = Some(rf_descs);
            }
        }
    }

    RuntimeFilterPlanResult {
        all_filters,
        build_side_filters,
        probe_side_filters,
    }
}

/// Check if the join operation type is eligible for runtime filter generation.
/// Aligned with StarRocks `JoinNode.buildRuntimeFilters()`.
fn is_rf_eligible_join_op(node: &plan_nodes::TPlanNode) -> bool {
    let hj = match &node.hash_join_node {
        Some(hj) => hj,
        None => return false,
    };
    matches!(
        hj.join_op,
        plan_nodes::TJoinOp::INNER_JOIN
            | plan_nodes::TJoinOp::LEFT_SEMI_JOIN
            | plan_nodes::TJoinOp::RIGHT_OUTER_JOIN
            | plan_nodes::TJoinOp::RIGHT_SEMI_JOIN
            | plan_nodes::TJoinOp::RIGHT_ANTI_JOIN
            | plan_nodes::TJoinOp::CROSS_JOIN
    )
}

/// Extract (slot_id, tuple_id) from a TExpr if it is a simple SlotRef.
/// Returns None for complex expressions (v1 limitation).
fn extract_slot_ref(expr: &exprs::TExpr) -> Option<(i32, i32)> {
    if expr.nodes.len() == 1
        && expr.nodes[0].node_type == exprs::TExprNodeType::SLOT_REF
    {
        if let Some(ref sr) = expr.nodes[0].slot_ref {
            return Some((sr.slot_id, sr.tuple_id));
        }
    }
    None
}
```

- [ ] **Step 3: Verify it compiles**

Run: `cargo build 2>&1 | tail -5`
Expected: Build succeeds. The module is registered but not yet called.

- [ ] **Step 4: Commit**

```bash
git add src/sql/cascades/runtime_filter_planner.rs src/sql/cascades/mod.rs
git commit -m "Add runtime_filter_planner module for standalone RF planning"
```

---

### Task 3: Wire RF planner into fragment_builder.build()

**Files:**
- Modify: `src/sql/cascades/fragment_builder.rs`

Call the RF planner after building all fragments and pass the result out through `MultiFragmentBuildResult`.

- [ ] **Step 1: Add join_distributions tracking to PlanFragmentBuilder**

Add a new field to `PlanFragmentBuilder`:

```rust
    /// hash join node_id -> JoinDistribution for RF join mode mapping.
    pub(crate) join_distributions: HashMap<i32, crate::sql::cascades::operator::JoinDistribution>,
```

Initialize in `build()`:

```rust
            join_distributions: HashMap::new(),
```

In `visit_hash_join()`, right after the `join_fragment_map.insert(...)` line added in Task 1:

```rust
        self.join_distributions
            .insert(join_node_id, op.distribution.clone());
```

- [ ] **Step 2: Add RuntimeFilterPlanResult to MultiFragmentBuildResult**

In `src/sql/physical/mod.rs`, add to `MultiFragmentBuildResult`:

```rust
    /// Runtime filter planning result (populated for standalone mode).
    pub rf_plan: Option<crate::sql::cascades::runtime_filter_planner::RuntimeFilterPlanResult>,
```

- [ ] **Step 3: Call plan_runtime_filters in build()**

In `fragment_builder.rs`, at the end of `build()`, just before the `Ok(MultiFragmentBuildResult { ... })` return, add the RF planning call:

```rust
        // Runtime filter planning pass: identify RF opportunities and patch
        // join nodes with TRuntimeFilterDescription.
        let pipeline_dop = std::thread::available_parallelism()
            .map(|p| p.get().min(4))
            .unwrap_or(4) as i32;
        let rf_plan = super::runtime_filter_planner::plan_runtime_filters(
            &mut fragment_results,
            &builder.scan_tuple_owners,
            &builder.join_fragment_map,
            &builder.join_distributions,
            pipeline_dop,
        );
        let rf_plan = if rf_plan.all_filters.is_empty() {
            None
        } else {
            Some(rf_plan)
        };
```

Update the return to include `rf_plan`:

```rust
        Ok(MultiFragmentBuildResult {
            fragment_results,
            root_fragment_id,
            edges: builder.completed_edges,
            rf_plan,
        })
```

- [ ] **Step 4: Fix all other construction sites of MultiFragmentBuildResult**

Search for other places that construct `MultiFragmentBuildResult` and add `rf_plan: None`. There may be none (only `build()` constructs it), but verify.

- [ ] **Step 5: Verify it compiles**

Run: `cargo build 2>&1 | tail -5`
Expected: Build succeeds. RF planner is now called during fragment building.

- [ ] **Step 6: Commit**

```bash
git add src/sql/cascades/fragment_builder.rs src/sql/physical/mod.rs
git commit -m "Wire RF planner into fragment builder post-build pass"
```

---

### Task 4: Add RF param assembly to ExecutionCoordinator

**Files:**
- Modify: `src/standalone/coordinator.rs`

Build `TRuntimeFilterParams` from the RF plan result and attach to every fragment's exec params. Patch merge node addresses on remote filter descriptions.

- [ ] **Step 1: Add setup_runtime_filter_params function**

At the bottom of `coordinator.rs`, add:

```rust
use crate::runtime_filter;
use crate::sql::cascades::runtime_filter_planner::RuntimeFilterPlanResult;

/// Build TRuntimeFilterParams from the RF planning result.
///
/// Sets id_to_prober_params, runtime_filter_builder_number, and
/// runtime_filter_max_size.  Also patches merge node addresses on
/// TRuntimeFilterDescription for remote filters.
fn setup_runtime_filter_params(
    rf_plan: &mut RuntimeFilterPlanResult,
    fragment_results: &mut [FragmentBuildResult],
    instance_map: &BTreeMap<FragmentId, (i64, i64)>,
    exchange_addr: &types::TNetworkAddress,
) -> runtime_filter::TRuntimeFilterParams {
    let mut id_to_prober_params: BTreeMap<i32, Vec<runtime_filter::TRuntimeFilterProberParams>> =
        BTreeMap::new();
    let mut builder_number: BTreeMap<i32, i32> = BTreeMap::new();

    // Populate prober params: for each probe-side filter, record the
    // fragment instance that will apply it.
    for (frag_id, probes) in &rf_plan.probe_side_filters {
        if let Some(&(hi, lo)) = instance_map.get(frag_id) {
            for (filter_id, _scan_node_id) in probes {
                let prober = runtime_filter::TRuntimeFilterProberParams::new(
                    types::TUniqueId::new(hi, lo),
                    exchange_addr.clone(),
                );
                id_to_prober_params
                    .entry(*filter_id)
                    .or_default()
                    .push(prober);
            }
        }
    }

    // Builder number: always 1 in standalone (single instance per fragment).
    for (_, filter_ids) in &rf_plan.build_side_filters {
        for fid in filter_ids {
            builder_number.insert(*fid, 1);
        }
    }

    // Patch merge node addresses on remote filter descriptions and on
    // the join plan nodes.
    for (filter_id, desc) in rf_plan.all_filters.iter_mut() {
        if desc.has_remote_targets == Some(true) {
            desc.runtime_filter_merge_nodes = Some(vec![exchange_addr.clone()]);
        }
    }
    // Also patch the TRuntimeFilterDescription inside the plan nodes.
    for fr in fragment_results.iter_mut() {
        for node in fr.plan.nodes.iter_mut() {
            if let Some(ref mut hj) = node.hash_join_node {
                if let Some(ref mut rf_descs) = hj.build_runtime_filters {
                    for desc in rf_descs.iter_mut() {
                        if desc.has_remote_targets == Some(true) {
                            desc.runtime_filter_merge_nodes =
                                Some(vec![exchange_addr.clone()]);
                        }
                    }
                }
            }
        }
    }

    runtime_filter::TRuntimeFilterParams::new(
        id_to_prober_params,
        builder_number,
        16_i64 * 1024 * 1024, // runtime_filter_max_size: 16MB default
        None::<std::collections::BTreeSet<i32>>,
    )
}
```

- [ ] **Step 2: Call setup_runtime_filter_params in execute()**

In `ExecutionCoordinator::execute()`, after the existing instance_map and per_fragment_exch_num_senders setup, and before the loop that builds cte_thrift_fragments, add:

```rust
        // ---------------------------------------------------------------
        // Runtime filter parameter assembly
        // ---------------------------------------------------------------
        let rf_params = if let Some(ref mut rf_plan) = /* need mutable access to rf_plan */ {
            Some(setup_runtime_filter_params(
                rf_plan,
                &mut /* fragment_results - need to restructure access */,
                &instance_map,
                &brpc_addr,
            ))
        } else {
            None
        };
```

Note: The exact integration depends on where `build_result` is destructured. The `rf_plan` is on `MultiFragmentBuildResult`. Destructure it alongside existing fields:

In the `let MultiFragmentBuildResult { ... } = self.build_result;` destructuring, add `mut rf_plan`:

```rust
        let MultiFragmentBuildResult {
            mut fragment_results,
            root_fragment_id,
            edges,
            rf_plan,
        } = self.build_result;
```

Then before the fragment classification loop, insert:

```rust
        let rf_params = match rf_plan {
            Some(mut plan) => Some(setup_runtime_filter_params(
                &mut plan,
                &mut fragment_results,
                &instance_map,
                &brpc_addr,
            )),
            None => None,
        };
```

- [ ] **Step 3: Attach rf_params to every fragment's exec_params**

For each fragment exec_params construction in the coordinator (stream_exec_params, cte_exec_params, and root_exec_params), add:

```rust
        // After setting per_exch_num_senders on the exec_params:
        if let Some(ref rf) = rf_params {
            exec_params.runtime_filter_params = Some(rf.clone());
        }
```

Apply this to all three places where exec_params are built:
1. `stream_exec_params` (around line 267-275)
2. `cte_exec_params` (around line 377-384)
3. `root_exec_params` (around line 435-439)

- [ ] **Step 4: Verify it compiles**

Run: `cargo build 2>&1 | tail -5`
Expected: Build succeeds.

- [ ] **Step 5: Commit**

```bash
git add src/standalone/coordinator.rs
git commit -m "Add RF param assembly to ExecutionCoordinator"
```

---

### Task 5: End-to-end verification with a join query

**Files:**
- No code changes, testing only.

Verify the runtime filter planning produces correct output and the execution layer consumes it correctly.

- [ ] **Step 1: Start standalone server or use existing test infra**

Use the SQL test runner or direct standalone engine to run a join query. Verify with debug logging.

- [ ] **Step 2: Run a simple two-table join**

Execute a query like:

```sql
CREATE TABLE rf_test_a (id INT, name VARCHAR(100));
CREATE TABLE rf_test_b (id INT, value INT);
INSERT INTO rf_test_a VALUES (1, 'a'), (2, 'b'), (3, 'c');
INSERT INTO rf_test_b VALUES (1, 10), (2, 20), (4, 40);
SELECT a.name, b.value FROM rf_test_a a JOIN rf_test_b b ON a.id = b.id;
```

Expected result:
```
a, 10
b, 20
```

- [ ] **Step 3: Verify RF descriptions are generated**

Add temporary debug logging in `plan_runtime_filters()` to confirm:
- At least one `TRuntimeFilterDescription` is created for the join
- `filter_id`, `build_expr`, `plan_node_id_to_target_expr`, `has_remote_targets` are all populated
- The join node's `build_runtime_filters` field is `Some(vec![...])` not `None`

Check logs for any runtime filter hub/worker activity indicating the filter was received and applied.

- [ ] **Step 4: Test with multi-fragment (cross-exchange) join**

If the optimizer produces a multi-fragment plan (e.g., shuffle join), verify:
- `has_remote_targets = true` on the RF description
- `runtime_filter_merge_nodes` is set to the exchange address
- gRPC `TransmitRuntimeFilter` is called (check logs or add trace)

- [ ] **Step 5: Remove debug logging and commit**

```bash
git add -A
git commit -m "Verify standalone runtime filter end-to-end"
```

---

### Task 6: Handle edge cases and robustness

**Files:**
- Modify: `src/sql/cascades/runtime_filter_planner.rs`

- [ ] **Step 1: Skip RF when probe expr passes through a project**

The v1 `extract_slot_ref` already handles this (returns `None` for non-SlotRef). Verify that queries with computed join keys (e.g., `ON a.id + 1 = b.id`) don't crash — they should silently skip RF generation.

Run: `SELECT * FROM rf_test_a a JOIN rf_test_b b ON a.id + 1 = b.id`
Expected: Query succeeds, no RF generated (complex probe expr).

- [ ] **Step 2: Skip RF for LEFT OUTER, FULL OUTER, LEFT ANTI joins**

These join types are already excluded by `is_rf_eligible_join_op()`. Verify:

Run: `SELECT * FROM rf_test_a a LEFT JOIN rf_test_b b ON a.id = b.id`
Expected: Query succeeds, no RF generated.

- [ ] **Step 3: Handle single-fragment join (both sides scan in same fragment)**

For single-fragment joins, `has_remote_targets = false`. The hash join build side will call `hub.publish_filters()` which delivers directly in-memory.

Run: A simple join that the optimizer places in a single fragment.
Expected: Query succeeds, RF applied locally.

- [ ] **Step 4: Commit**

```bash
git add -A
git commit -m "Verify RF edge cases: complex exprs, outer joins, single-fragment"
```
