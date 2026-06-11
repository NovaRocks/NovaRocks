# Repeat Operator for ROLLUP/CUBE/GROUPING SETS

**Date:** 2026-03-31
**Goal:** Replace the UNION ALL expansion of ROLLUP with a Repeat operator that executes the base join once and replicates rows with different null patterns, reducing q18 peak memory from 5.8 GB to ~2 GB.

## Context

The current ROLLUP implementation expands `GROUP BY ROLLUP(a,b,c,d)` into N+1 separate SELECT statements joined by UNION ALL at the AST level. Each branch has its own full join tree, scan operators, and hash tables. For q18 (7-table join × 5 ROLLUP levels), this causes 5x memory multiplication — peak 5.8 GB on a 16 GB machine.

StarRocks uses a RepeatNode operator: the base join executes once, then RepeatNode replicates each output row N+1 times with different null-out patterns and grouping_id values. A single Aggregate then groups by (grouping_id, all_keys).

NovaRocks already has `lower_repeat_node` in the lowering layer and `RepeatOperator` in the pipeline executor (from the FE+BE path). The standalone emitter just needs to generate the correct `TRepeatNode` Thrift structure.

## 1. Data Structures

### 1.1 RepeatInfo (IR level)

```rust
// In src/sql/ir/mod.rs
pub struct RepeatInfo {
    /// For each repeat level, which column names are NON-null.
    /// Level 0 = all keys active, last level = no keys active.
    pub repeat_column_ref_list: Vec<Vec<String>>,
    /// Grouping ID bitmap for each level. Bit=1 means column is NULLed.
    pub grouping_ids: Vec<u64>,
    /// All rollup column names (used to compute all_slot_ids).
    pub all_rollup_columns: Vec<String>,
    /// GROUPING() function calls: maps output_name → list of argument column names.
    pub grouping_fn_args: Vec<(String, Vec<String>)>,
}
```

`ResolvedSelect` gets a new field: `pub repeat: Option<RepeatInfo>`.

### 1.2 LogicalPlan::Repeat

```rust
// In src/sql/plan/mod.rs
pub struct RepeatNode {
    pub input: Box<LogicalPlan>,
    pub repeat_column_ref_list: Vec<Vec<String>>,
    pub grouping_ids: Vec<u64>,
    pub all_rollup_columns: Vec<String>,
    pub grouping_fn_args: Vec<(String, Vec<String>)>,
}
```

## 2. Analyzer Changes

### 2.1 ROLLUP Detection

`extract_rollup_from_group_by` already detects ROLLUP. Currently it calls `expand_rollup` which generates UNION ALL. Change to call `resolve_rollup` which:

1. Computes N+1 levels of non-null column sets (same logic as current `expand_rollup`'s `nulled_exprs`, but inverted)
2. Computes grouping_id bitmaps for each level
3. Detects GROUPING() function calls in the projection and records their arguments
4. Stores everything in `RepeatInfo` on the `ResolvedSelect`
5. Analyzes the SELECT once (with all GROUP BY keys active) instead of N+1 times
6. Returns `(QueryBody::Select(sel), cols)` — a single SELECT, not UNION ALL

### 2.2 GROUPING() Handling

GROUPING() calls in the projection are NOT replaced with literals (unlike the current UNION ALL approach). Instead:
- The analyzer recognizes `grouping(col)` as a special function
- It records the mapping in `RepeatInfo.grouping_fn_args`
- The expression is resolved with return type Int64

At the emitter level, GROUPING() references are mapped to the virtual grouping slots produced by the Repeat operator.

## 3. Planner Changes

In `plan_select`, when `select.repeat` is `Some(repeat_info)`:

1. Plan the FROM + WHERE as usual → `current` plan (the base join+filter)
2. Insert `LogicalPlan::Repeat(RepeatNode { input: current, ...repeat_info })` above the join
3. Insert Aggregate above the Repeat:
   - GROUP BY keys = grouping_id_column + all original group-by keys
   - Aggregate functions = same as original SELECT's aggregates
4. Insert Project above the Aggregate to produce final output columns

The key: the Repeat node sits BETWEEN the join and the aggregate.

## 4. Emitter Changes

### 4.1 New file: `emit_repeat.rs`

Generates `TRepeatNode` Thrift structure:

1. Emit the child (join+filter) → get child EmitResult with scope
2. Allocate a new output tuple with:
   - All columns from the child scope (pass-through)
   - One virtual slot for `grouping_id` (Int64)
   - One virtual slot per GROUPING() function call (Int64)
3. Build `slot_id_set_list`: for each repeat level, the set of slot IDs that are NON-null
4. Build `all_slot_ids`: union of all rollup column slot IDs
5. Build `repeat_id_list` and `grouping_list` from the grouping_ids
6. Construct the TPlanNode with node_type = REPEAT_NODE

### 4.2 Scope

The Repeat node's output scope includes:
- All child columns (with rollup columns potentially nullable)
- `grouping_id` virtual column (Int64)
- Per-GROUPING()-call virtual columns (Int64)

## 5. Lowering / Executor

No changes needed. Verify that:
- `lower/node/mod.rs` dispatches `REPEAT_NODE` to `lower_repeat_node`
- The existing `RepeatOperator` in the pipeline executor handles the Thrift format
- The null-out logic and grouping_id emission work correctly

## 6. Optimizer Integration

- `map_children` in `optimizer/mod.rs`: handle `LogicalPlan::Repeat` (recurse into input)
- `cardinality.rs`: Repeat node statistics = `input_rows * repeat_levels`
- `cost.rs`: Repeat node cost = cpu: `input_compute_size * repeat_levels`, mem: 0 (streaming)
- Column pruning: pass through Repeat (it doesn't reduce columns)
- Join reorder: unaffected (Repeat is above the join)

## 7. Backward Compatibility

The old UNION ALL expansion code (`expand_rollup`, `replace_grouping_calls`, etc.) is removed. All ROLLUP queries go through the new Repeat path.

Queries that previously worked with ROLLUP (q14, q22, q27, q36) must continue to produce correct results.

## 8. Files Changed

| File | Change |
|------|--------|
| `src/sql/ir/mod.rs` | Add `RepeatInfo`, `ResolvedSelect.repeat` field |
| `src/sql/analyzer/mod.rs` | `expand_rollup` → `resolve_rollup`; remove UNION ALL expansion |
| `src/sql/plan/mod.rs` | Add `RepeatNode`, `LogicalPlan::Repeat` |
| `src/sql/planner/mod.rs` | Handle repeat in `plan_select` |
| `src/sql/physical/emitter/emit_repeat.rs` | **New**: generate TRepeatNode |
| `src/sql/physical/emitter/mod.rs` | Register emit_repeat, dispatch Repeat |
| `src/sql/optimizer/mod.rs` | `map_children` for Repeat |
| `src/sql/optimizer/cardinality.rs` | Repeat cardinality estimation |
| `src/sql/optimizer/cost.rs` | Repeat cost estimation |

## 9. Test Plan

- Unit: `resolve_rollup` generates correct RepeatInfo for ROLLUP(a,b)
- Unit: emitter produces valid TRepeatNode with correct slot_id_set_list
- Integration: `SELECT a, count(*) FROM t GROUP BY ROLLUP(a)` returns correct results
- Integration: `GROUPING(a)` returns 0/1 correctly
- Regression: q14, q22, q27, q36 produce same results as before
- Performance: q18 peak memory ≤ 2.5 GB (down from 5.8 GB)
- Performance: q18 completes within 60s on release build
