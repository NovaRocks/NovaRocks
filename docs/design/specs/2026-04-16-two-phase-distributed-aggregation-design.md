# Two-Phase Distributed Aggregation

**Date:** 2026-04-16
**Scope:** Cascades optimizer + fragment builder — GROUP BY aggregation only (scalar agg deferred).
**Motivation:** Standalone plans emit 215 single-phase aggregates across TPC-DS 103 statements vs 507 two-phase (update-serialize + merge-finalize) in FE plans. Two-phase pre-aggregates in each scan fragment before shuffling, cutting inter-node bytes proportional to group-by NDV.

---

## 1. Approach

**Fragment-builder-level mode dispatch.** The Cascades rule emits a Local+Global alternative using the same `AggregateCall` expressions as the logical operator. The fragment builder checks `AggMode` and handles Thrift generation differently per phase. No expression rewriting at the optimizer level.

**Why this approach over Cascades-level rewriting:** Avoids fragile coupling between optimizer-generated synthetic column names and fragment builder scope resolution. The fragment builder already has full access to the child scope and positional column mapping, making it the natural place for merge-expression generation.

---

## 2. Changes by File

### 2.1 `src/sql/cascades/rules/implement.rs` — AggToHashAgg

In `apply()`, alongside the existing Single alternative:

1. **Guard:** skip two-phase if any `AggregateCall` has `distinct: true` or if `group_by` is empty (scalar agg deferred).
2. Build a `PhysicalHashAggregate(AggMode::Local)` MExpr with `children = expr.children` (same input group as the logical aggregate). Use the same `group_by`, `aggregates`, and `output_columns`.
3. Call `memo.new_group(local_mexpr)` to create an intermediate group. Set `logical_props` on the new group (copy from the current group, adjusted for aggregation row-count reduction) so the cost model can derive statistics.
4. Build a `PhysicalHashAggregate(AggMode::Global)` NewExpr with `children = [local_group_id]`. Same `group_by`, `aggregates`, `output_columns`.
5. Return `vec![single, global]`.

The search driver automatically handles property matching:
- Global requires `Hash(group_keys)` → Local provides `Hash(group_keys)` ✓
- If properties don't match, the enforcer inserts a hash exchange.

### 2.2 `src/sql/cascades/fragment_builder.rs` — visit_hash_aggregate

Mode-aware compilation in `visit_hash_aggregate`:

**Single (existing path, unchanged):**
- `need_finalize = true`
- Compile group-by and aggregate functions normally against child scope.

**Local:**
- `need_finalize = false`
- Compile group-by and aggregate functions identically to Single (same scope, same expressions).
- The execution layer's `update + serialize` behavior is driven by `need_finalize = false`.

**Global:**
- `need_finalize = true`
- **Group-by expressions:** compile normally against child scope. The Local's output scope registers the same qualified column names (via `add_qualified_alias`), so resolution works without rewriting.
- **Aggregate functions:** instead of `compile_aggregate_call_typed(agg_call)`, call `compile_merge_aggregate_call(agg_call, intermediate_slot_id, intermediate_type)`. For each aggregate at index `i`, the intermediate slot is the child scope column at position `group_by.len() + i`.

### 2.3 `src/sql/physical/nodes.rs` — build_aggregation_node

Add `need_finalize: bool` parameter (currently hardcoded to `true`). Fragment builder passes the value based on mode:
- Single/Global: `true`
- Local: `false`

### 2.4 `src/sql/physical/expr_compiler.rs` — compile_merge_aggregate_call

New method:

```rust
pub fn compile_merge_aggregate_call(
    &mut self,
    agg_call: &AggregateCall,
    input_slot_id: i32,
    input_type: &DataType,
) -> Result<TExpr, String>
```

Generates a TExpr structurally identical to `compile_aggregate_call_typed` except:
- **Root node:** `agg_expr.is_merge_agg = true` (instead of `false`)
- **Child node:** a single `SlotRef(input_slot_id)` of `input_type` (instead of compiling `agg_call.args`)
- **`num_children`:** `1` (the intermediate slot ref)

Everything else (function name, type signature, intermediate type, arg_types for the TFunction) stays the same as the update-phase version.

### 2.5 Files unchanged

| File | Reason |
|------|--------|
| `operator.rs` | `AggMode::{Single, Local, Global}` already defined |
| `search.rs` | Property requirements and output derivation already correct for GROUP BY |
| `cost.rs` | Local = 0.5× input, Global = 0.3× input already implemented |
| `explain.rs` | Already prints `HASH AGGREGATE (LOCAL)` / `HASH AGGREGATE (GLOBAL)` |
| `stats.rs` | `PhysicalHashAggregate` stats derivation works for all modes |

---

## 3. Property Flow

```
Search driver optimizes the aggregate group:

  Alternative 1: Single
    requires Any from scan_group
    cost = 1.0 × input_size

  Alternative 2: Global → [local_group]
    Global requires Hash(keys) from local_group
    Local provides Hash(keys) ✓
    Local requires Any from scan_group
    cost = Local(0.5 × input) + exchange_cost + Global(0.3 × input)

When NDV(group_keys) << row_count, two-phase wins on cost.
```

---

## 4. Fragment Builder Output

For a query like `SELECT d_year, SUM(ss_sales_price) FROM ... GROUP BY d_year`:

**Single-phase (current):**
```
AggNode(need_finalize=true, aggregate_functions=[sum(slot_ref_sales)])
  ScanNode
```

**Two-phase (new):**
```
Fragment 0:
  AggNode(need_finalize=true, is_merge_agg=true, aggregate_functions=[sum(slot_ref_intermediate)])
    ExchangeNode(HASH_PARTITIONED by [d_year])

Fragment 1:
  AggNode(need_finalize=false, aggregate_functions=[sum(slot_ref_sales)])
    ScanNode
```

---

## 5. Edge Cases

**DISTINCT aggregates:** Excluded. `COUNT(DISTINCT x)` requires all distinct values in one place. The guard in `AggToHashAgg` skips two-phase when any aggregate has `distinct: true`.

**Scalar aggregation (no GROUP BY):** Deferred. Requires fixing `output_properties` so Local scalar agg outputs `Any` instead of `Gather`. Without this, the search driver sees properties already satisfied and won't insert an exchange.

**Mixed DISTINCT + non-DISTINCT:** If any aggregate is DISTINCT, the entire aggregation stays single-phase. A future optimization could split the aggregation into DISTINCT and non-DISTINCT streams, but that's out of scope.

**AVG and other multi-state aggregates:** No special handling needed at the Cascades/fragment-builder level. The execution layer's `avg` function handles update/merge/finalize phases internally via serialized intermediate state. The `infer_agg_function_types` function already returns the correct intermediate type (`Utf8` for avg).

---

## 6. Testing

1. **EXPLAIN verification:** Run `EXPLAIN` on TPC-DS queries and verify `HASH AGGREGATE (LOCAL)` + `HASH AGGREGATE (GLOBAL)` appear with hash exchange between them.
2. **Correctness:** Run `sql-tests --suite ssb,tpc-h,tpc-ds --mode verify` to confirm results match.
3. **Single-phase fallback:** Verify DISTINCT aggregates still produce `HASH AGGREGATE (SINGLE)`.
4. **Unit tests:** Add test in `implement.rs` verifying `AggToHashAgg` produces both Single and Global alternatives for non-distinct GROUP BY, and only Single for DISTINCT.
