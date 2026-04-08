# AGG TopN Runtime Filter Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Implement aggregate TopN runtime filter in NovaRocks BE, then write red-light tests reproducing the FE exprOrder bug (StarRocksTest#11224).

**Architecture:** AGG operator computes min/max from group-by columns during streaming, publishes `RuntimeMinMaxFilter` to `RuntimeFilterHub`. Scan operator extracts the filter, converts it to `MinMaxPredicate`, and pushes it to the storage layer for segment/page zone-map pruning. The FE bug sends wrong `expr_order`, causing type mismatch or wrong column data.

**Tech Stack:** Rust, Arrow arrays, StarRocks Thrift protocol, NovaRocks pipeline executor, sql-test-runner

**Spec:** `docs/superpowers/specs/2026-04-08-agg-topn-runtime-filter-design.md`

**Key reference files for understanding existing patterns:**
- Hash join runtime filter build: `src/exec/operators/hashjoin/hash_join_build_sink.rs`
- Hash join lowering (RF parsing): `src/lower/node/hash_join.rs:258-357`
- MinMax predicate infrastructure: `src/common/min_max_predicate.rs`
- Existing MinMaxFilter: `src/exec/runtime_filter/min_max.rs`

---

### Task 1: Extend RuntimeMinMaxFilter with `from_array()` and `to_min_max_predicates()`

**Files:**
- Modify: `src/exec/runtime_filter/min_max.rs`

These are the foundation methods. `from_array()` computes min/max from a single Arrow array (used by AGG operator). `to_min_max_predicates()` converts to storage-layer predicates (used by Scan).

- [ ] **Step 1: Add `from_array()` method**

Add after the existing `from_arrays()` method (around line 289). This is a convenience wrapper for single arrays, plus it needs to handle the case where the array is a single column from the key table.

```rust
/// Compute min/max from a single Arrow array.
/// Used by the AGG operator to build TopN runtime filters from group-by key columns.
pub(crate) fn from_array(ltype: TPrimitiveType, array: &ArrayRef) -> Result<Self, String> {
    Self::from_arrays(ltype, &[Arc::clone(array)])
}
```

- [ ] **Step 2: Add `to_min_max_predicates()` method**

Add after `min_max_predicate_values()` (around line 178). Converts the filter into `MinMaxPredicate` structs that the storage layer understands.

```rust
/// Convert this min/max filter into storage-layer MinMaxPredicate values.
/// Returns Ge (>=min) and Le (<=max) predicates for the given column.
pub(crate) fn to_min_max_predicates(
    &self,
    column_name: &str,
) -> Result<Vec<MinMaxPredicate>, String> {
    let Some((min_val, max_val)) = self.min_max_predicate_values()? else {
        return Ok(Vec::new());
    };
    Ok(vec![
        MinMaxPredicate::Ge {
            column: column_name.to_string(),
            value: min_val,
        },
        MinMaxPredicate::Le {
            column: column_name.to_string(),
            value: max_val,
        },
    ])
}
```

Add the import at the top of the file:
```rust
use crate::common::min_max_predicate::{MinMaxPredicate, MinMaxPredicateValue};
```

- [ ] **Step 3: Verify compilation**

Run: `cargo check -p novarocks --lib 2>&1 | head -20`
Expected: No errors in `min_max.rs`

- [ ] **Step 4: Commit**

```bash
git add src/exec/runtime_filter/min_max.rs
git commit -m "feat(runtime-filter): add from_array and to_min_max_predicates to RuntimeMinMaxFilter"
```

---

### Task 2: Add MinMax filter support to RuntimeFilterHub

**Files:**
- Modify: `src/runtime/runtime_filter_hub.rs`

The hub currently stores `in_filters` and `membership_filters`. We add parallel support for `min_max_filters`.

- [ ] **Step 1: Add min_max_filters to RuntimeFilterHandleStore**

Find the `RuntimeFilterHandleStore` struct (or the inner store used by `RuntimeFilterHandle`). Add a `min_max_filters` field alongside existing filter maps. The store is inside `RuntimeFilterHandleInner` at the `store: RwLock<...>` field.

Add to the store struct:
```rust
pub(crate) min_max_filters: HashMap<i32, Arc<RuntimeMinMaxFilter>>,
```

Initialize it as `HashMap::new()` in the constructor.

- [ ] **Step 2: Add publish_min_max_filter() to RuntimeFilterHub**

Add a method similar to `publish_in_filter_to_targets()` but for min_max:

```rust
pub(crate) fn publish_min_max_filter(&self, filter_id: i32, filter: RuntimeMinMaxFilter) {
    let filter = Arc::new(filter);
    // Publish to all handles that have this filter_id registered
    let entries = self.inner.entries.lock().expect("runtime filter hub lock");
    for entry in entries.values() {
        let mut store = entry.handle.inner.store.write().expect("rf handle store lock");
        store.min_max_filters.insert(filter_id, Arc::clone(&filter));
        drop(store);
        entry.handle.inner.version.fetch_add(1, std::sync::atomic::Ordering::Release);
    }
}
```

Note: study the existing `publish_in_filter_to_targets()` and `publish_membership_filter_to_targets()` methods to match the exact pattern (they may iterate over specific target entries rather than all entries). Replicate that pattern for min_max.

- [ ] **Step 3: Add min_max_filters to RuntimeFilterSnapshot**

Update `RuntimeFilterSnapshot` (line 49-53):
```rust
#[derive(Clone, Debug, Default)]
pub(crate) struct RuntimeFilterSnapshot {
    in_filters: Vec<Arc<RuntimeInFilter>>,
    membership_filters: Vec<Arc<RuntimeMembershipFilter>>,
    pub(crate) min_max_filters: Vec<Arc<RuntimeMinMaxFilter>>,
}
```

Update the `snapshot()` method on `RuntimeFilterHandle` (line 65-79) to include min_max:
```rust
pub(crate) fn snapshot(&self) -> RuntimeFilterSnapshot {
    let guard = self.inner.store.read().expect("runtime filter handle lock");
    // ... existing in_filters and membership_filters collection ...
    let mut min_max_filters = Vec::with_capacity(guard.min_max_filters.len());
    for filter in guard.min_max_filters.values() {
        min_max_filters.push(Arc::clone(filter));
    }
    RuntimeFilterSnapshot {
        in_filters,
        membership_filters,
        min_max_filters,
    }
}
```

Also update `RuntimeFilterSnapshot::from_filters()` if it exists, adding `min_max_filters: Vec::new()`.

- [ ] **Step 4: Add import for RuntimeMinMaxFilter**

```rust
use crate::exec::runtime_filter::min_max::RuntimeMinMaxFilter;
```

- [ ] **Step 5: Verify compilation**

Run: `cargo check -p novarocks --lib 2>&1 | head -30`
Expected: No errors. There may be warnings about unused fields — that's expected until scan side consumes them.

- [ ] **Step 6: Commit**

```bash
git add src/runtime/runtime_filter_hub.rs
git commit -m "feat(runtime-filter): add MinMax filter support to RuntimeFilterHub"
```

---

### Task 3: Add MinMax filters to RuntimeFilterContext

**Files:**
- Modify: `src/exec/node/scan.rs`

The `RuntimeFilterContext` wraps filter access for scan operators. It needs to expose min_max filters.

- [ ] **Step 1: Add accessor for min_max_filters on RuntimeFilterContext**

Add a method to `RuntimeFilterContext` impl (after `snapshot()`, around line 167):

```rust
pub(crate) fn min_max_filters(&self) -> Vec<Arc<RuntimeMinMaxFilter>> {
    match &self.inner {
        RuntimeFilterContextInner::Static { .. } => Vec::new(),
        RuntimeFilterContextInner::Handle { handle } => {
            handle.snapshot().min_max_filters
        }
    }
}
```

Add import:
```rust
use crate::exec::runtime_filter::min_max::RuntimeMinMaxFilter;
```

- [ ] **Step 2: Verify compilation**

Run: `cargo check -p novarocks --lib 2>&1 | head -20`

- [ ] **Step 3: Commit**

```bash
git add src/exec/node/scan.rs
git commit -m "feat(scan): expose MinMax filters from RuntimeFilterContext"
```

---

### Task 4: Define TopNRuntimeFilterSpec and add to AggregateNode

**Files:**
- Modify: `src/exec/node/aggregate.rs`

- [ ] **Step 1: Define TopNRuntimeFilterSpec struct**

Add after the `AggFunction` struct definition (around line 39):

```rust
/// Spec for a TopN runtime filter built by the AGG operator.
/// `expr_order` indexes into the group-by columns to select which column
/// to compute min/max from. The FE bug hardcodes this to 0.
#[derive(Clone, Debug)]
pub struct TopNRuntimeFilterSpec {
    pub filter_id: i32,
    pub expr_order: usize,
    /// The primitive type of the build expression (from FE).
    /// Used to create the RuntimeMinMaxFilter with the correct type.
    pub build_type: TPrimitiveType,
    /// The column name on the probe (scan) side that this filter targets.
    pub probe_column_name: String,
    /// TopN limit — filter is only published when group count >= limit.
    pub limit: usize,
}
```

Add the thrift type import at the top:
```rust
use crate::thrift_rs::types::TPrimitiveType;
```

- [ ] **Step 2: Add topn_rf_specs to AggregateNode**

Update `AggregateNode` struct (line 43-54):

```rust
#[derive(Clone, Debug)]
pub struct AggregateNode {
    pub input: Box<ExecNode>,
    pub node_id: i32,
    pub group_by: Vec<ExprId>,
    pub functions: Vec<AggFunction>,
    pub need_finalize: bool,
    pub input_is_intermediate: bool,
    pub output_chunk_schema: ChunkSchemaRef,
    pub topn_rf_specs: Vec<TopNRuntimeFilterSpec>,
}
```

- [ ] **Step 3: Fix all AggregateNode construction sites**

Search for all places where `AggregateNode` is constructed and add `topn_rf_specs: Vec::new()`. The main site is in `src/lower/node/aggregate.rs` (the `lower_aggregate_node()` function). There may be other sites in tests.

Run: `grep -rn "AggregateNode {" src/ --include="*.rs"` to find all construction sites.

- [ ] **Step 4: Verify compilation**

Run: `cargo check -p novarocks --lib 2>&1 | head -30`

- [ ] **Step 5: Commit**

```bash
git add src/exec/node/aggregate.rs src/lower/node/aggregate.rs
git commit -m "feat(aggregate): define TopNRuntimeFilterSpec and add to AggregateNode"
```

---

### Task 5: Parse TOPN_FILTER from Thrift in aggregate lowering

**Files:**
- Modify: `src/lower/node/aggregate.rs`

Follow the pattern from `src/lower/node/hash_join.rs:258-357` for parsing runtime filter descriptions.

- [ ] **Step 1: Parse build_runtime_filters from TAggregationNode**

In `lower_aggregate_node()`, after existing field parsing and before constructing `AggregateNode`, add:

```rust
// Parse TopN runtime filter specs from build_runtime_filters
let mut topn_rf_specs = Vec::new();
if let Some(ref filters) = agg_node.build_runtime_filters {
    for desc in filters {
        let Some(filter_type) = desc.filter_type.as_ref() else {
            continue;
        };
        // Only handle TOPN_FILTER type
        if *filter_type != runtime_filter::TRuntimeFilterBuildType::TOPN_FILTER {
            continue;
        }
        let filter_id = desc.filter_id.ok_or_else(|| {
            "TopN runtime filter missing filter_id".to_string()
        })?;
        let expr_order = desc.expr_order.ok_or_else(|| {
            "TopN runtime filter missing expr_order".to_string()
        })? as usize;
        
        // Get build expression type
        let build_expr = desc.build_expr.as_ref().ok_or_else(|| {
            "TopN runtime filter missing build_expr".to_string()
        })?;
        let build_type = build_expr.nodes.first()
            .and_then(|n| n.r#type.as_ref())
            .and_then(|t| t.types.first())
            .and_then(|st| st.r#type.clone())
            .ok_or_else(|| "TopN runtime filter: cannot extract build type".to_string())?;

        // Get probe column name from plan_node_id_to_target_expr
        // The probe expr should be a SlotRef; we need the column name for MinMaxPredicate
        let probe_column_name = extract_probe_column_name(desc, out)?;

        let limit = desc.topn.unwrap_or(100) as usize;

        topn_rf_specs.push(TopNRuntimeFilterSpec {
            filter_id,
            expr_order,
            build_type,
            probe_column_name,
            limit,
        });
    }
}
```

The `extract_probe_column_name` helper extracts the column name from the probe expression. Study how the `plan_node_id_to_target_expr` map works — it maps plan node IDs to probe expressions. The probe expression for a scan node is typically a SlotRef. You need to resolve the slot to a column name using the scan node's output layout.

Note: this may require passing additional context (the scan node's column layout) or deferring column name resolution to the pipeline builder. If so, store the slot_id in the spec and resolve later.

- [ ] **Step 2: Pass topn_rf_specs to AggregateNode construction**

Update the `AggregateNode` construction at the end of `lower_aggregate_node()`:
```rust
AggregateNode {
    input: Box::new(input),
    node_id: *node_id,
    group_by,
    functions,
    need_finalize: *need_finalize,
    input_is_intermediate: *input_is_intermediate,
    output_chunk_schema,
    topn_rf_specs,
}
```

- [ ] **Step 3: Add necessary imports**

```rust
use crate::exec::node::aggregate::TopNRuntimeFilterSpec;
use crate::thrift_rs::runtime_filter;
```

- [ ] **Step 4: Verify compilation**

Run: `cargo check -p novarocks --lib 2>&1 | head -30`

- [ ] **Step 5: Commit**

```bash
git add src/lower/node/aggregate.rs
git commit -m "feat(lower): parse TOPN_FILTER specs from TAggregationNode thrift"
```

---

### Task 6: AGG Operator — build and publish MinMax filter

**Files:**
- Modify: `src/exec/operators/aggregate/mod.rs`

This is the core task. The AGG operator needs to periodically compute min/max from the group-by key column and publish it to the RuntimeFilterHub.

- [ ] **Step 1: Add fields to AggregateProcessorFactory**

Update struct (line 76-84):
```rust
pub struct AggregateProcessorFactory {
    name: String,
    arena: Arc<ExprArena>,
    group_by: Vec<ExprId>,
    functions: Vec<AggFunction>,
    output_intermediate: bool,
    direct_input: bool,
    output_chunk_schema: ChunkSchemaRef,
    topn_rf_specs: Vec<TopNRuntimeFilterSpec>,
    runtime_filter_hub: Option<Arc<RuntimeFilterHub>>,
}
```

Update `new()` to accept and store the new fields:
```rust
pub fn new(
    node_id: i32,
    arena: Arc<ExprArena>,
    group_by: Vec<ExprId>,
    functions: Vec<AggFunction>,
    output_intermediate: bool,
    direct_input: bool,
    output_chunk_schema: ChunkSchemaRef,
    topn_rf_specs: Vec<TopNRuntimeFilterSpec>,
    runtime_filter_hub: Option<Arc<RuntimeFilterHub>>,
) -> Self {
    // ... existing name logic ...
    Self {
        name,
        arena,
        group_by,
        functions,
        output_intermediate,
        direct_input,
        output_chunk_schema,
        topn_rf_specs,
        runtime_filter_hub,
    }
}
```

- [ ] **Step 2: Add fields to AggregateProcessorOperator**

Update struct (line 147-171):
```rust
struct AggregateProcessorOperator {
    // ... all existing fields ...
    topn_rf_specs: Vec<TopNRuntimeFilterSpec>,
    runtime_filter_hub: Option<Arc<RuntimeFilterHub>>,
    topn_rf_rows_since_publish: usize,
}
```

Update `create()` in the OperatorFactory impl (line 119) to pass the new fields:
```rust
topn_rf_specs: self.topn_rf_specs.clone(),
runtime_filter_hub: self.runtime_filter_hub.clone(),
topn_rf_rows_since_publish: 0,
```

- [ ] **Step 3: Implement `try_publish_topn_runtime_filter()` method**

Add to the `impl AggregateProcessorOperator` block:

```rust
/// Attempt to publish TopN runtime filter if conditions are met.
/// Called after each push_chunk to incrementally update the filter.
fn try_publish_topn_runtime_filter(&mut self) {
    const PUBLISH_THRESHOLD: usize = 4096;

    if self.topn_rf_specs.is_empty() {
        return;
    }
    let Some(hub) = &self.runtime_filter_hub else {
        return;
    };
    let Some(key_table) = &self.key_table else {
        return;
    };

    if self.topn_rf_rows_since_publish < PUBLISH_THRESHOLD {
        return;
    }

    for spec in &self.topn_rf_specs {
        // Check if we have enough groups to make the filter useful
        if key_table.num_groups() < spec.limit {
            continue;
        }

        // Get the group-by key column at expr_order
        // This is where the FE bug manifests: wrong expr_order → wrong column
        let key_columns = key_table.key_columns();
        let Some(column_array) = key_columns.get(spec.expr_order) else {
            continue;
        };

        // Build MinMax filter from the column data
        let filter = match RuntimeMinMaxFilter::from_array(spec.build_type, column_array) {
            Ok(f) => f,
            Err(e) => {
                tracing::warn!("Failed to build TopN MinMax filter: {}", e);
                continue;
            }
        };

        // Publish to hub
        hub.publish_min_max_filter(spec.filter_id, filter);
    }
    self.topn_rf_rows_since_publish = 0;
}
```

- [ ] **Step 4: Call publish from push_chunk()**

Update `push_chunk()` (line 600-615) to track rows and call publish:

```rust
fn push_chunk(&mut self, _state: &RuntimeState, chunk: Chunk) -> Result<(), String> {
    if self.finished {
        return Ok(());
    }
    if self.finishing {
        return Err("aggregate received input after set_finishing".to_string());
    }
    if self.pending_output.is_some() {
        return Err("aggregate received input while output buffer is full".to_string());
    }
    let num_rows = chunk.num_rows();
    let out = self.process(chunk)?;
    if out.is_some() {
        return Err("aggregate produced output before finishing".to_string());
    }
    // Incremental TopN runtime filter publishing
    self.topn_rf_rows_since_publish += num_rows;
    self.try_publish_topn_runtime_filter();
    Ok(())
}
```

- [ ] **Step 5: Add imports**

```rust
use crate::exec::node::aggregate::TopNRuntimeFilterSpec;
use crate::exec::runtime_filter::min_max::RuntimeMinMaxFilter;
use crate::runtime::runtime_filter_hub::RuntimeFilterHub;
```

- [ ] **Step 6: Verify compilation**

Run: `cargo check -p novarocks --lib 2>&1 | head -30`
Note: This will fail until Task 7 updates all `AggregateProcessorFactory::new()` call sites. That's expected.

- [ ] **Step 7: Commit**

```bash
git add src/exec/operators/aggregate/mod.rs
git commit -m "feat(aggregate): build and publish TopN MinMax runtime filter"
```

---

### Task 7: Pipeline builder — wire RuntimeFilterHub to AGG factory

**Files:**
- Modify: `src/exec/pipeline/builder.rs`

Update ALL `AggregateProcessorFactory::new()` call sites to pass the new parameters.

- [ ] **Step 1: Update all AggregateProcessorFactory::new() calls**

There are ~4 call sites (lines 692, 726, 743, 804). For each, add the two new parameters.

For the **final stage** aggregate (the one that has the original AggregateNode context and does finalization), pass the real specs and hub:

```rust
AggregateProcessorFactory::new(
    *node_id,
    Arc::clone(&ctx.arena),
    group_by.clone(),
    functions.clone(),
    !*need_finalize,
    false,
    output_chunk_schema.clone(),
    topn_rf_specs.clone(),                    // NEW
    Some(Arc::clone(&ctx.runtime_filter_hub)), // NEW
)
```

For **partial/intermediate** aggregate stages that should NOT publish filters, pass empty specs:

```rust
AggregateProcessorFactory::new(
    *node_id,
    Arc::clone(&ctx.arena),
    group_by.clone(),
    partial_functions,
    true,
    false,
    output_chunk_schema.clone(),
    Vec::new(),  // NEW: no TopN RF for partial agg
    None,        // NEW: no hub needed
)
```

Extract `topn_rf_specs` from the `AggregateNode` at the top of the aggregate handling branch:
```rust
let topn_rf_specs = agg_node.topn_rf_specs.clone();
```

- [ ] **Step 2: Register build specs with hub**

Before creating the factory, register specs if non-empty:

```rust
if !topn_rf_specs.is_empty() {
    // Register with the hub so scan nodes know to wait for these filters
    for spec in &topn_rf_specs {
        ctx.runtime_filter_hub.register_min_max_build_spec(
            *node_id,
            spec.filter_id,
        );
    }
}
```

Note: you may need to add a `register_min_max_build_spec()` method to `RuntimeFilterHub` if the existing `register_filter_specs()` doesn't cover min_max. Study the existing registration pattern and adapt.

- [ ] **Step 3: Verify compilation**

Run: `cargo check -p novarocks --lib 2>&1 | head -30`
Expected: Clean compilation (all `AggregateProcessorFactory::new()` sites updated)

- [ ] **Step 4: Commit**

```bash
git add src/exec/pipeline/builder.rs
git commit -m "feat(pipeline): wire RuntimeFilterHub to aggregate factory for TopN RF"
```

---

### Task 8: Scan probe — apply MinMax filter at storage level

**Files:**
- Modify: `src/connector/starrocks/scan/op.rs`
- Modify: `src/exec/operators/scan/runner.rs` (if needed for version tracking)

- [ ] **Step 1: Update StarRocksScanOp::execute_iter() to use runtime filters**

In `execute_iter()` (line 105-120 of `op.rs`), change `_runtime_filters` to `runtime_filters` and extract min_max predicates:

```rust
fn execute_iter(
    &self,
    morsel: ScanMorsel,
    profile: Option<RuntimeProfile>,
    runtime_filters: Option<&crate::exec::node::scan::RuntimeFilterContext>,
) -> Result<BoxedExecIter, String> {
    let ScanMorsel::StarRocksRange { index, .. } = morsel else {
        return Err("starrocks scan received unexpected morsel".to_string());
    };

    let range = self
        .cfg
        .ranges
        .get(index)
        .cloned()
        .ok_or_else(|| format!("starrocks scan range index out of bounds: {index}"))?;

    let mut cfg = self.cfg.clone();

    // Apply MinMax runtime filters from AGG TopN as storage-level predicates
    if let Some(rf_ctx) = runtime_filters {
        let min_max_filters = rf_ctx.min_max_filters();
        for filter in &min_max_filters {
            // Convert RuntimeMinMaxFilter to MinMaxPredicate for storage layer
            // We need to find the column name this filter targets.
            // The filter's probe column is stored alongside the filter metadata.
            // For now, we use the probe_column_name from the spec,
            // which should be passed through the hub or stored on the filter.
            if let Ok(predicates) = filter.to_min_max_predicates(&self.probe_column_for_filter(filter)) {
                cfg.min_max_predicates.extend(predicates);
            }
        }
    }

    // ... rest of existing code with `cfg` ...
}
```

Note: the mapping from filter_id → probe_column_name needs to be available at the scan operator. Options:
1. Store it on `RuntimeMinMaxFilter` itself (add a `probe_column_name: Option<String>` field)
2. Store it in the scan config during lowering
3. Use a separate mapping in `StarRocksScanOp`

Option 1 is simplest. Add `probe_column_name: Option<String>` to `RuntimeMinMaxFilter` and set it during AGG publish. Or better: store the column name mapping in `StarRocksScanConfig` during lowering (Task 9).

- [ ] **Step 2: Ensure scan runner passes RuntimeFilterContext to each morsel**

Check `src/exec/operators/scan/runner.rs`. The `ScanAsyncRunner` should already pass `runtime_filter_ctx` to `execute_iter()`. Verify this is the case. If the context is only read once at startup, it needs to be refreshed between morsels using the handle's version tracking.

Look at `next_chunk()` in runner.rs — it should call `scan.execute_iter(morsel, profile, self.runtime_filter_ctx.as_ref())`. Verify runtime_filter_ctx is refreshed or is handle-based (which auto-refreshes via version tracking).

- [ ] **Step 3: Verify compilation**

Run: `cargo check -p novarocks --lib 2>&1 | head -30`

- [ ] **Step 4: Commit**

```bash
git add src/connector/starrocks/scan/op.rs src/exec/operators/scan/runner.rs
git commit -m "feat(scan): apply MinMax runtime filters at storage level"
```

---

### Task 9: Lowering — scan probe registration for TOPN_FILTER

**Files:**
- Modify: `src/lower/node/starrocks_scan.rs`

The scan node needs to know about incoming TOPN_FILTER runtime filters so it can:
1. Register with the hub to wait for them
2. Know which column each filter targets

- [ ] **Step 1: Parse TOPN_FILTER probe specs from scan node's runtime filters**

In the scan lowering function, find where `probe_runtime_filters` are processed. Currently, only `JOIN_FILTER` types are handled (they're rejected for non-JOIN types in hash_join.rs:304).

For the scan node, look for where runtime filter probe descriptors are registered. Add handling for TOPN_FILTER:

```rust
// For each probe runtime filter descriptor on this scan node
if let Some(ref probe_filters) = scan_node.probe_runtime_filters {
    for desc in probe_filters {
        let filter_type = desc.filter_type.as_ref();
        match filter_type {
            Some(t) if *t == runtime_filter::TRuntimeFilterBuildType::TOPN_FILTER => {
                // Extract the probe column name from the probe expression
                let filter_id = desc.filter_id.unwrap_or(-1);
                // The probe expression tells us which scan column to apply the filter to
                let probe_expr = desc.plan_node_id_to_target_expr
                    .as_ref()
                    .and_then(|m| m.get(&node_id))
                    .ok_or_else(|| format!("TOPN_FILTER {filter_id}: missing probe expr for node {node_id}"))?;
                let probe_column_name = resolve_slot_to_column_name(probe_expr, &out_layout)?;
                
                // Store this mapping so the scan operator knows which column
                // each min_max filter targets
                topn_filter_column_map.insert(filter_id, probe_column_name);
                
                // Register with hub to receive this filter
                // (done in pipeline builder)
            }
            _ => {} // JOIN_FILTER handled elsewhere
        }
    }
}
```

Store the `topn_filter_column_map: HashMap<i32, String>` in `StarRocksScanConfig` or a new field on the scan node. The scan operator needs this to convert filter_id → column_name when applying predicates.

- [ ] **Step 2: Pass column mapping to scan config**

Add a field to `StarRocksScanConfig`:
```rust
pub topn_filter_column_map: HashMap<i32, String>,
```

- [ ] **Step 3: Verify compilation**

Run: `cargo check -p novarocks --lib 2>&1 | head -30`

- [ ] **Step 4: Commit**

```bash
git add src/lower/node/starrocks_scan.rs src/connector/starrocks/scan/
git commit -m "feat(lower): parse TOPN_FILTER probe specs and map to scan columns"
```

---

### Task 10: Integration verification — compile and basic smoke test

**Files:** None new — integration test

- [ ] **Step 1: Full build**

```bash
cd /Users/harbor/worktree/NovaRocks/main
cargo build --release 2>&1 | tail -20
```

Expected: Clean build with no errors.

- [ ] **Step 2: Package and deploy**

```bash
./build.sh --release --package --output /Users/harbor/starrocks-on-novarocks/novarocks
```

- [ ] **Step 3: Restart cluster**

```bash
cd /Users/harbor/starrocks-on-novarocks/novarocks && ./bin/novarocksctl stop
cd /Users/harbor/starrocks-on-novarocks/fe && bin/stop_fe.sh
cd /Users/harbor/starrocks-on-novarocks/fe && bin/start_fe.sh --daemon
sleep 15
cd /Users/harbor/starrocks-on-novarocks/novarocks && ./bin/novarocksctl start --daemon
sleep 5
```

- [ ] **Step 4: Smoke test — basic GROUP BY ORDER BY LIMIT**

```bash
export NO_PROXY=127.0.0.1,localhost
QUERY_PORT=$(grep -E '^[[:space:]]*query_port' /Users/harbor/starrocks-on-novarocks/fe/conf/fe.conf | awk -F= '{gsub(/[[:space:]]/, "", $2); print $2}')

mysql -h 127.0.0.1 -P"${QUERY_PORT}" -u root -e "
CREATE DATABASE IF NOT EXISTS test_topn_rf;
USE test_topn_rf;
DROP TABLE IF EXISTS t_smoke;
CREATE TABLE t_smoke (k1 INT, k2 INT, v BIGINT)
DUPLICATE KEY(k1) DISTRIBUTED BY HASH(k1) BUCKETS 1
PROPERTIES ('replication_num'='1');
INSERT INTO t_smoke VALUES (1,10,100),(2,20,200),(3,30,300),(4,40,400),(5,50,500);
SET enable_topn_runtime_filter = true;
SELECT k1, k2, count(*) FROM t_smoke GROUP BY k1, k2 ORDER BY k1 LIMIT 3;
"
```

Expected: Query returns results without crash.

- [ ] **Step 5: Commit any fixes**

If smoke test reveals issues, fix and commit.

---

### Task 11: Red-light test 1 — type mismatch (DATE vs DATETIME)

**Files:**
- Create: `tests/sql-test-runner/suites/topn-rf-bug/T/test_topn_rf_type_mismatch`
- Create: `tests/sql-test-runner/suites/topn-rf-bug/R/test_topn_rf_type_mismatch`

- [ ] **Step 1: Create test suite directory**

```bash
mkdir -p /Users/harbor/worktree/NovaRocks/main/tests/sql-test-runner/suites/topn-rf-bug/T
mkdir -p /Users/harbor/worktree/NovaRocks/main/tests/sql-test-runner/suites/topn-rf-bug/R
```

- [ ] **Step 2: Write the test case**

Create `tests/sql-test-runner/suites/topn-rf-bug/T/test_topn_rf_type_mismatch`:

```sql
-- name: test_topn_rf_type_mismatch
-- Test that GROUP BY k1(DATE), k2(DATETIME) ORDER BY k2 LIMIT
-- triggers the FE exprOrder bug: expr_order=0 points to DATE column
-- but filter is applied to DATETIME column → type mismatch

CREATE DATABASE IF NOT EXISTS db_${uuid0};
USE db_${uuid0};

DROP TABLE IF EXISTS t_type_mismatch;
CREATE TABLE t_type_mismatch (
    k1 DATE,
    k2 DATETIME,
    v BIGINT
) DUPLICATE KEY(k1)
DISTRIBUTED BY HASH(k1) BUCKETS 1
PROPERTIES ('replication_num' = '1');

-- Insert enough distinct rows to exceed the LIMIT and trigger TopN filter
INSERT INTO t_type_mismatch VALUES
    ('2024-01-01', '2024-01-01 00:00:00', 1),
    ('2024-01-02', '2024-01-02 01:00:00', 2),
    ('2024-01-03', '2024-01-03 02:00:00', 3),
    ('2024-01-04', '2024-01-04 03:00:00', 4),
    ('2024-01-05', '2024-01-05 04:00:00', 5),
    ('2024-01-06', '2024-01-06 05:00:00', 6),
    ('2024-01-07', '2024-01-07 06:00:00', 7),
    ('2024-01-08', '2024-01-08 07:00:00', 8),
    ('2024-01-09', '2024-01-09 08:00:00', 9),
    ('2024-01-10', '2024-01-10 09:00:00', 10);

-- Need more rows for TopN filter to actually trigger (group count > limit)
INSERT INTO t_type_mismatch
SELECT DATE_ADD('2024-02-01', INTERVAL number DAY),
       DATE_ADD('2024-06-01 00:00:00', INTERVAL number HOUR),
       number
FROM TABLE(generate_series(1, 500));

SET enable_topn_runtime_filter = true;

-- This query has: GROUP BY k1(DATE), k2(DATETIME) ORDER BY k2(DATETIME) LIMIT 5
-- FE bug: expr_order=0 → BE builds MinMax<DATE> for k1 → applies to k2(DATETIME)
-- Expected: error due to type mismatch, or incorrect results
SELECT k1, k2, count(*) as cnt
FROM t_type_mismatch
GROUP BY k1, k2
ORDER BY k2
LIMIT 5;

DROP DATABASE db_${uuid0} FORCE;
```

- [ ] **Step 3: Record expected results**

Run in record mode to capture what actually happens (may be error or wrong results):

```bash
cd /Users/harbor/worktree/NovaRocks/main
cargo run --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests -- \
  --suite topn-rf-bug \
  --only test_topn_rf_type_mismatch \
  --mode record \
  --update-expected
```

Examine the result file. If the query errors, the result will show the error. If it returns wrong results, note that for the test assertion.

- [ ] **Step 4: Commit**

```bash
git add tests/sql-test-runner/suites/topn-rf-bug/
git commit -m "test: red-light test for TopN RF type mismatch (DATE vs DATETIME)"
```

---

### Task 12: Red-light test 2 — same type, wrong data (silent corruption)

**Files:**
- Create: `tests/sql-test-runner/suites/topn-rf-bug/T/test_topn_rf_wrong_data`
- Create: `tests/sql-test-runner/suites/topn-rf-bug/R/test_topn_rf_wrong_data`

- [ ] **Step 1: Write the test case**

Create `tests/sql-test-runner/suites/topn-rf-bug/T/test_topn_rf_wrong_data`:

```sql
-- name: test_topn_rf_wrong_data
-- Test that GROUP BY k1(INT), k2(INT) ORDER BY k2 LIMIT
-- with different value ranges causes wrong results due to FE exprOrder bug.
-- k1 range: [1, 10000] (wide), k2 range: [1, 10] (narrow)
-- FE sends expr_order=0 → BE builds MinMax from k1 → applies to k2
-- k1's min/max won't filter k2 effectively, but the filter is built from wrong data

CREATE DATABASE IF NOT EXISTS db_${uuid0};
USE db_${uuid0};

DROP TABLE IF EXISTS t_wrong_data;
CREATE TABLE t_wrong_data (
    k1 INT,
    k2 INT,
    v BIGINT
) DUPLICATE KEY(k1)
DISTRIBUTED BY HASH(k1) BUCKETS 1
PROPERTIES ('replication_num' = '1');

-- k1 has wide range, k2 has narrow range
-- Each (k1, k2) combination is unique
INSERT INTO t_wrong_data
SELECT number, (number % 10) + 1, number
FROM TABLE(generate_series(1, 10000));

-- Baseline: correct result without TopN runtime filter
SET enable_topn_runtime_filter = false;
SELECT k2, count(*) as cnt
FROM t_wrong_data
GROUP BY k1, k2
ORDER BY k2
LIMIT 5;

-- Bug reproduction: TopN RF enabled, wrong expr_order
SET enable_topn_runtime_filter = true;
SELECT k2, count(*) as cnt
FROM t_wrong_data
GROUP BY k1, k2
ORDER BY k2
LIMIT 5;

-- The two results above should be identical if the feature works correctly.
-- With the FE bug, the second query may return different results because
-- the MinMax filter is built from k1's data but applied to k2's column.

DROP DATABASE db_${uuid0} FORCE;
```

- [ ] **Step 2: Record and verify**

```bash
cd /Users/harbor/worktree/NovaRocks/main
cargo run --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests -- \
  --suite topn-rf-bug \
  --only test_topn_rf_wrong_data \
  --mode record \
  --update-expected
```

Examine the result file. The two SELECT queries should produce different results if the bug is active. The test demonstrates the silent data corruption.

- [ ] **Step 3: Commit**

```bash
git add tests/sql-test-runner/suites/topn-rf-bug/
git commit -m "test: red-light test for TopN RF wrong data (same type, wrong column)"
```

---

### Task 13: Run full test suite and verify red-light tests

- [ ] **Step 1: Run both red-light tests**

```bash
cd /Users/harbor/worktree/NovaRocks/main
cargo run --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests -- \
  --suite topn-rf-bug \
  --mode verify
```

- [ ] **Step 2: Verify test 1 shows type mismatch behavior**

Check that `test_topn_rf_type_mismatch` either errors or produces results where the filter was applied with wrong type.

- [ ] **Step 3: Verify test 2 shows data corruption**

Check that `test_topn_rf_wrong_data` shows different results between RF-disabled and RF-enabled queries. This is the "silent wrong results" bug.

- [ ] **Step 4: Document findings**

Add a comment to the test result files explaining what the expected buggy behavior is and what correct behavior would look like after the FE fix.

- [ ] **Step 5: Final commit**

```bash
git add -A
git commit -m "feat: complete AGG TopN runtime filter implementation with red-light tests

Implements aggregate TopN runtime filter in NovaRocks BE:
- AGG operator builds MinMaxRuntimeFilter from group-by key columns
- Published incrementally to RuntimeFilterHub during streaming
- Scan operator converts to MinMaxPredicate for storage-level segment pruning

Red-light tests reproduce the FE exprOrder bug (StarRocksTest#11224):
- test_topn_rf_type_mismatch: DATE vs DATETIME type mismatch
- test_topn_rf_wrong_data: same type but wrong column data"
```
