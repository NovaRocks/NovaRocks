# IVM Detail-State Framework: VARBINARY Migration + Distinct-Count Aggregates

**Date:** 2026-05-26
**Goal:** Migrate the P5 IVM detail-state framework from Arrow `Map<K, Int64>` to opaque `VARBINARY` state columns dispatched per `AggregateFunctionKind`, and add IVM support for `COUNT(DISTINCT col)` and `APPROX_COUNT_DISTINCT(col)` aggregates.
**Status:** Draft, pending implementation plan.

## Context

NovaRocks's IVM (incremental materialized view) framework currently supports five aggregate kinds: `COUNT`, `SUM`, `AVG`, `MIN`, `MAX`, plus `BOOL_OR` / `BOOL_AND` landed via [PR #165](https://github.com/.../pull/165). The MIN/MAX/BOOL_OR family uses a "detail-state" approach built in P5: each agg call materializes an Arrow `Map<key_type, Int64>` column tracking distinct keys and their net contribution counts, enabling retract on Iceberg row-level DELETE.

This spec extends that family with two new kinds — `COUNT(DISTINCT col)` and `APPROX_COUNT_DISTINCT(col)` — and simultaneously restructures the underlying state column from `Map<K, Int64>` to opaque `VARBINARY`, with per-kind serialization/deserialization. The restructure unblocks future encoding evolution (Roaring bitmaps, true HLL register state, ARRAY_AGG multiset, etc.) without further MV schema migrations.

### Comparison with StarRocks

| Capability | StarRocks IVM | NovaRocks (after this spec) |
|---|---|---|
| State column physical layout | `__AGG_STATE_<n>__ VARBINARY` | Aligned ✓ |
| State combinator surface | `F_state` / `F_state_union` / `F_state_merge` SQL functions | Aligned ✓ |
| `COUNT(DISTINCT col)` IVM | Rejected outright ([IVMAnalyzer.java:331](https://github.com/StarRocks/starrocks/blob/main/fe/fe-core/src/main/java/com/starrocks/sql/analyzer/mv/IVMAnalyzer.java)) | ✅ Supported |
| `APPROX_COUNT_DISTINCT(col)` IVM | Supported, but only on append-only Iceberg ([IvmDeltaIcebergScanRule.java:62](https://github.com/StarRocks/starrocks/blob/main/fe/fe-core/src/main/java/com/starrocks/sql/optimizer/rule/ivm/IvmDeltaIcebergScanRule.java)) | ✅ Supported including Iceberg with row-level deletes |
| Iceberg with row-level deletes in IVM | Append-only only | Full support (carries P5 capability forward) |

NovaRocks's physical state layout aligns with StarRocks, while semantic IVM capability strictly exceeds StarRocks's (Iceberg-with-deletes and `COUNT(DISTINCT)` both supported here, neither in StarRocks).

## 1. High-level Architecture

### 1.1 Spec scope

Two concerns delivered together in a single architectural change:

**A. P5 detail-state framework migration to VARBINARY.**
All detail-state aggregates — including existing `BOOL_OR` / `BOOL_AND` / `MIN` / `MAX` and the new `COUNT(DISTINCT)` / `APPROX_COUNT_DISTINCT` — share a single state column form: `__AGG_STATE_<n>__ VARBINARY`. State semantics, byte layout, accumulate / derive-visible behaviors are dispatched on `AggregateFunctionKind`. The schema validator collapses to "column type is VARBINARY".

**B. Two new IVM-supported aggregate kinds.**
- `AggregateFunctionKind::CountDistinct` covering SQL aliases `count(DISTINCT col)`, `count_distinct(col)`, `multi_distinct_count(col)`.
- `AggregateFunctionKind::ApproxCountDistinct` covering SQL aliases `approx_count_distinct(col)`, `ndv(col)`, `hll_ndv(col)`.

Both share V1 internal encoding (sorted multiset; see §3) and reuse the P5 detail-state retract path. They differ only in the `_state_visible` function: `COUNT(DISTINCT)` yields exact `BigInt`, `APPROX_COUNT_DISTINCT` yields HLL-derived `BigInt` matching plain-path `approx_count_distinct(col)` results.

### 1.2 Framework dispatcher shape

```text
MV table schema:
  group_keys columns ...  (regular Arrow types)
  __AGG_STATE_0__         VARBINARY   ← agg call 0 opaque bytes
  __AGG_STATE_1__         VARBINARY   ← agg call 1
  ...
  __retraction_count__    BIGINT      ← P5 row-lifecycle column, unchanged

Framework dispatcher (per AggregateFunctionKind):
  fn empty_state()              -> Vec<u8>
  fn accumulate_insert(s, args) -> Vec<u8>
  fn accumulate_delete(s, args) -> Vec<u8>
  fn merge_states(a, b)         -> Vec<u8>
  fn derive_visible(s)          -> ArrowScalar
  fn is_empty(s)                -> bool
```

### 1.3 Relationship to existing P5 MVs

**Breaking change**: existing BOOL_OR / BOOL_AND / MIN / MAX materialized views have `__agg_state_<n>__` column type `Map<K, Int64>` (Parquet `List<Struct<key, value>>`). After this migration, the same column type is `VARBINARY`. **Users must `DROP` + `CREATE` to recreate any existing detail-state MV.** Detection and migration policy specified in §8.

This is named **P5.5 architecture migration** in commit messages and release notes — it is neither a P5 patch nor a P6 new feature but a focused architectural integration step.

### 1.4 Capabilities lost in the migration

| Item | Note |
|---|---|
| External engine direct reads of MV state | Spark / DuckDB / Trino see a `BINARY` column; decoding requires NovaRocks-specific knowledge. The Iceberg table itself remains readable, but the state column content is opaque. |
| `EXPLAIN ANALYZE` introspection of state | Mitigated by `DEBUG_DUMP_MV_STATE(mv_name, row_id)` tool function delivered in this spec (§7.5). |
| `PARQUET:field_id` metadata for Map nested fields | No longer required (VARBINARY has no nested structure); removed from `validate_state_column_type`. |

### 1.5 Capabilities unchanged

- Delta-SELECT entry: from FE perspective, MV is still driven by INSERT/DELETE/REFRESH.
- `group_keys` columns remain regular Arrow types.
- Iceberg row-lineage requirement (`format-version = 3` + `write.row-lineage = true`).
- Visible semantics for `BOOL_OR` / `BOOL_AND` / `MIN` / `MAX` (bytes change, observable behavior identical).

## 2. Framework Refactor: Per-Kind State Combinator SQL Functions

### 2.1 Naming convention (StarRocks-aligned)

Each `AggregateFunctionKind` registers three or four SQL functions:

| Function | Kind | Signature | Purpose |
|---|---|---|---|
| `<kind>_state(args)` | aggregate | `args... → VARBINARY` | Produce per-group partial state from delta rows |
| `<kind>_state_signed(args, __change_op)` | aggregate | `args..., TinyInt → VARBINARY` | Same but with INSERT/DELETE sign |
| `<kind>_state_union(a, b)` | scalar | `VARBINARY, VARBINARY → VARBINARY` | Merge two states (partial-agg internal / MV-state merge) |
| `<kind>_state_visible(state)` | scalar | `VARBINARY → <original return type>` | Finalize to user-visible value |

These functions are **internal-only**. User-written SQL never calls them directly; they appear only in IVM-rewriter-generated delta SQL.

The `map_value_count` / `map_value_count_signed` helpers introduced in P5 are removed.

Full function list:

```
count_state(col)                    count_state_union(a, b)                    count_state_visible(s)                    -> BigInt
sum_state(col)                      sum_state_union(a, b)                      sum_state_visible(s)                      -> <numeric>
avg_state(col)                      avg_state_union(a, b)                      avg_state_visible(s)                      -> <numeric>
min_state(col)                      min_state_union(a, b)                      min_state_visible(s)                      -> <col_type>
max_state(col)                      max_state_union(a, b)                      max_state_visible(s)                      -> <col_type>
bool_or_state(col)                  bool_or_state_union(a, b)                  bool_or_state_visible(s)                  -> Boolean
bool_and_state(col)                 bool_and_state_union(a, b)                 bool_and_state_visible(s)                 -> Boolean
count_distinct_state(col)           count_distinct_state_union(a, b)           count_distinct_state_visible(s)           -> BigInt
approx_count_distinct_state(col)    approx_count_distinct_state_union(a, b)    approx_count_distinct_state_visible(s)    -> BigInt
```

`_signed` variants exist for the same set; they take an additional `__change_op TinyInt` argument letting DELETE rows contribute negatively.

### 2.2 Registration locations

- Scalar functions (`_state_union`, `_state_visible`): registered in `src/sql/analyzer/functions.rs` and implemented in `src/exec/expr/scalar/`.
- Aggregate functions (`_state`, `_state_signed`): registered in `src/exec/expr/agg/functions/mod.rs`, each kind with an `AggregateFunction` impl.

### 2.3 IVM rewriter changes

`src/connector/starrocks/managed/ivm_delta_aggregate.rs::signed_delta_projection` currently hard-codes `map_value_count(arg)` projections. After refactor, projection emission is per-kind:

```rust
for agg_call in shape.aggregates {
    let combinator = match agg_call.function {
        AggregateFunctionKind::Count               => "count_state_signed",
        AggregateFunctionKind::Sum                 => "sum_state_signed",
        AggregateFunctionKind::Avg                 => "avg_state_signed",
        AggregateFunctionKind::Min                 => "min_state_signed",
        AggregateFunctionKind::Max                 => "max_state_signed",
        AggregateFunctionKind::BoolOr              => "bool_or_state_signed",
        AggregateFunctionKind::BoolAnd             => "bool_and_state_signed",
        AggregateFunctionKind::CountDistinct       => "count_distinct_state_signed",
        AggregateFunctionKind::ApproxCountDistinct => "approx_count_distinct_state_signed",
    };
    projection.push(make_function_call(combinator, [agg_call.input, change_op]));
}
```

The INSERT-only delta path uses the non-`_signed` variant.

### 2.4 Refresh flow

```text
                  ┌──────────────────────────────┐
                  │ delta SELECT                 │ ← rewriter emits <kind>_state_signed(args, __change_op)
                  │   GROUP BY group_keys        │   for each agg call
                  └──────────┬───────────────────┘
                             │ (partial state per group, VARBINARY)
                             ▼
                  ┌────────────────────────────────┐
                  │ LEFT OUTER JOIN MV state       │ ← on group_keys = encode(group_keys)
                  └──────────┬─────────────────────┘
                             │ (delta_state, mv_state) pairs
                             ▼
                  ┌──────────────────────────────────────┐
                  │ Project:                             │
                  │   <kind>_state_union(delta_s, mv_s)  │ ← per agg call
                  └──────────┬───────────────────────────┘
                             │ (new VARBINARY state)
                             ▼
                       UPSERT into MV table
```

When users query the MV, the view definition rewrites `<original agg call>` to `<kind>_state_visible(__AGG_STATE_<n>__)`. The user never sees the state column directly.

### 2.5 Schema validator simplification

`src/connector/starrocks/managed/mv_agg_state.rs::validate_state_column_type` collapses to:

```rust
fn validate_state_column_type(column: &Field, _kind: AggregateFunctionKind) -> Result<()> {
    if column.data_type() != &DataType::Binary {
        return Err(format!(
            "expected VARBINARY state column type, got: {:?}",
            column.data_type()
        ));
    }
    // Byte-level validity is checked at deserialize time, not at schema validation.
    Ok(())
}
```

The PARQUET field-id handling for Map nested fields is removed.

### 2.6 `_state_union` algebraic requirements

`<kind>_state_union` must be **associative** and **commutative**: partial-aggregation runs in parallel and the framework merges results in unspecified order. Every kind in this spec satisfies these requirements; each kind's encoding section (§3.2, §3.3) calls this out explicitly.

## 3. Per-Kind Byte Encoding

### 3.1 General rules

- **Version byte**: every non-empty state begins with `u8 format_version = 0x01`. This enables non-breaking encoding evolution (see §9.1).
- **Empty state**: a zero-length VARBINARY (no version byte). Distinguishes "this group has no contribution for this agg call" from "this group has contribution but it cancels to zero".
- **Variable-length integers**: ULEB128 for unsigned, SLEB128 for signed.
- **Fixed-width integers**: little-endian (matches NovaRocks's storage convention).

### 3.2 Fixed-size kinds

| Kind | Layout | Size |
|---|---|---|
| Count | `version(1)` + `i64 count(8)` | 9 bytes |
| BoolOr / BoolAnd | `version(1)` + `i64 count_true(8)` + `i64 count_false(8)` | 17 bytes |
| Sum (Int64) | `version(1)` + `i64 row_count(8)` + `i64 sum(8)` | 17 bytes |
| Sum (Decimal128) | `version(1)` + `i64 row_count(8)` + `i128 sum(16)` | 25 bytes |
| Avg (Int64) | same as Sum (Int64) | 17 bytes |
| Avg (Decimal128) | same as Sum (Decimal128) | 25 bytes |

Sum/Avg state internally carries `row_count` so visible can distinguish "no rows → NULL" from "rows summing to zero → 0".

Sum and Avg did not have detail-state in pre-spec P5 (they used plain additive aggregates). The migration moves them into the VARBINARY family **for architectural uniformity only** — execution behavior is unchanged.

### 3.3 Multiset kinds (Min / Max / CountDistinct / ApproxCountDistinct)

All four share **the same byte layout**, differing only in `_state_visible`:

```text
multiset_state :=
    u8       version = 0x01
    ULEB128  num_entries
    entry[num_entries]               -- sorted ascending by serialized_key

entry :=
    serialized_key                   -- per §3.4
    SLEB128  signed_count            -- can be negative during partial-agg merge;
                                     -- normalized to positive before write-back
```

**Normalized form invariants** (post-`_state_union`, pre-write):
- All `signed_count > 0`.
- Entries sorted by serialized_key bytes, ascending, no duplicate keys.
- The union operator canonicalizes before producing output.

During partial-aggregation, the unnormalized form (negative or zero counts, possibly unsorted) is tolerated as an intermediate.

### 3.4 Key serialization

| Arrow type | Encoding | Bytes |
|---|---|---|
| Boolean | `u8` (0 = false, 1 = true) | 1 |
| Int8 / Int16 / Int32 / Int64 | LE, native width | 1 / 2 / 4 / 8 |
| Float32 / Float64 | LE; NaN canonicalized to a single bit pattern; `-0.0` normalized to `+0.0` | 4 / 8 |
| Decimal128 | LE i128 | 16 |
| Date32 (days since epoch) | LE i32 | 4 |
| Timestamp (microseconds) | LE i64 | 8 |
| Utf8 / LargeUtf8 | ULEB128 length + UTF8 bytes | 1+N |

**Rejected as keys in V1**: Binary / LargeBinary, Struct / List / Map / Union, Dictionary. Detection happens at MV CREATE time in `AggregateMvLayout` construction (error message guides the user to project to a scalar).

**NULL keys**: NULL inputs are filtered at the aggregate function entry — they never reach the multiset encoder. Consistent with P5's existing `map_value_count` NULL-skip behavior.

### 3.5 `is_empty` rules

A zero-length VARBINARY state is `is_empty == true` for all kinds (no decode required). For non-zero-length states, the per-kind decoded condition is:

| Kind | `is_empty` condition (after decode) |
|---|---|
| Count | `count == 0` |
| Sum / Avg | `row_count == 0` |
| Min / Max | `num_entries == 0` |
| BoolOr / BoolAnd | `count_true == 0 && count_false == 0` |
| CountDistinct / ApproxCountDistinct | `num_entries == 0` |

After a `_state_union` that cancels all contributions, the implementation may either canonicalize to a zero-length state or keep the decoded all-zero state — `is_empty` returns true in both cases.

MV row-level drop is decided by the framework when all agg states are empty **and** the auxiliary `__retraction_count__` column reaches zero (existing P5 mechanism, unchanged).

### 3.6 Reserved evolution paths

The `version` byte allows non-breaking V2 encoding additions:
- `version = 0x02`: Roaring-bitmap-of-row-ids for CountDistinct/ApproxCountDistinct multisets.
- `version = 0x03`: true HLL register array for ApproxCountDistinct (bounded ~16 KB state).
- Future: balanced-tree encoding for Min/Max; zstd-compressed payload.

V2 decoders dispatch on version byte; V1 MV state remains readable.

## 4. Classifier Dispatch and SQL Surface

### 4.1 Function-name to kind mapping

In `src/connector/starrocks/managed/mv_shape.rs::classify_aggregate_call`:

```rust
let (function, input) = match function_name.as_str() {
    "count"   => classify_count_input(&args)?,       // count(*), count(col), count(DISTINCT col)
    "sum"     => (Sum, classify_sum_input(&args)?),
    "avg"     => (Avg, classify_avg_input(&args)?),
    "min"     => (Min, classify_min_max_input(&args)?),
    "max"     => (Max, classify_min_max_input(&args)?),

    "bool_or"  | "boolor_agg"  => (BoolOr,  classify_bool_or_and_input(&args)?),
    "bool_and" | "booland_agg" => (BoolAnd, classify_bool_or_and_input(&args)?),

    // NEW kinds
    "count_distinct" | "multi_distinct_count"
        => (CountDistinct, classify_count_distinct_input(&args)?),

    "approx_count_distinct" | "ndv" | "hll_ndv"
        => (ApproxCountDistinct, classify_approx_count_distinct_input(&args)?),

    _ => return Err(aggregate_error()),
};
```

### 4.2 `count(DISTINCT col)` syntax routing

The current classifier rejects any `args.duplicate_treatment.is_some()` outright. The refactor relaxes this for `count` only:

```rust
if function_name == "count" {
    if let Some(dup) = &args.duplicate_treatment {
        if matches!(dup, DuplicateTreatment::Distinct) {
            return classify_count_distinct_from_count_distinct_syntax(&args, output_name);
        }
        return Err(aggregate_error());  // count(ALL col) and others still rejected
    }
}
if args.duplicate_treatment.is_some() {
    return Err(aggregate_error());  // DISTINCT on non-count remains rejected
}
```

All three forms below produce the same `AggregateFunctionKind::CountDistinct`:

```sql
SELECT region, COUNT(DISTINCT user_id) ...
SELECT region, count_distinct(user_id) ...
SELECT region, multi_distinct_count(user_id) ...
```

### 4.3 `CountDistinct` input validation

- Exactly one argument expression. Multi-column (`COUNT(DISTINCT a, b)`) rejected.
- `COUNT(DISTINCT *)` rejected (semantically nonsensical).
- Argument must be a column reference or a `reject_unsupported_expr`-clean expression (consistent with P5).

### 4.4 `ApproxCountDistinct` input validation

- Exactly one argument expression. The hint variant `approx_count_distinct(col, 14)` is **explicitly rejected** with a guiding error message (StarRocks's IVM whitelist also requires single-arg, see [IVMAnalyzer.java:124](https://github.com/StarRocks/starrocks/blob/main/fe/fe-core/src/main/java/com/starrocks/sql/analyzer/mv/IVMAnalyzer.java)).
- `approx_count_distinct(*)` rejected.
- `approx_count_distinct(DISTINCT col)` rejected (DISTINCT modifier is meaningless on this function).

### 4.5 Type-domain check location

Type validation runs in `AggregateMvLayout` construction at MV CREATE time, not in the syntactic classifier. The allowed type set is in §6.1. Unsupported types fail with actionable errors:

```
"COUNT(DISTINCT struct_col) — struct/list/map values cannot serve as distinct keys
 in incremental materialized views; please project to a scalar (e.g., a hashed or
 concatenated representation)"

"APPROX_COUNT_DISTINCT(col, 14) — the precision hint argument is not supported in
 materialized view; please remove the second argument"
```

### 4.6 Plain-path behavior unchanged

`count_distinct(col)` / `approx_count_distinct(col)` / `ndv(col)` / `hll_ndv(col)` in non-MV SQL paths route to the existing plain executors:

- `count_distinct(col)`: exact multiset, see `src/exec/expr/agg/functions/`.
- `approx_count_distinct(col)` / `ndv(col)` / `hll_ndv(col)`: HLL estimate, see `src/exec/expr/agg/functions/hll_raw.rs`.

The new `*_state` combinator family is invoked **only** inside IVM-rewritten MV view bodies.

## 5. Visible Derivation

Each `<kind>_state_visible(state: VARBINARY) → <return_type>` is a deterministic pure function.

### 5.1 Fixed-size kinds

```python
count_state_visible(s) -> BigInt:
    if s.is_empty(): return 0
    assert s[0] == 0x01
    return decode_le_i64(s[1..9])

sum_state_visible(s) -> <numeric>:
    if s.is_empty(): return NULL
    (row_count, sum) = decode(s)
    return NULL if row_count == 0 else sum

avg_state_visible(s) -> <numeric>:
    if s.is_empty(): return NULL
    (row_count, sum) = decode(s)
    return NULL if row_count == 0 else sum / row_count

bool_or_state_visible(s) -> Boolean:
    if s.is_empty(): return NULL
    (ct, cf) = decode(s)
    if ct > 0: return Boolean(true)
    if cf > 0: return Boolean(false)
    return NULL  # all-NULL group or fully retracted

bool_and_state_visible(s) -> Boolean:
    if s.is_empty(): return NULL
    (ct, cf) = decode(s)
    if cf > 0: return Boolean(false)
    if ct > 0: return Boolean(true)
    return NULL
```

Sum/Avg "empty → NULL" matches SQL standard (`SUM`/`AVG` over empty set = NULL); Count "empty → 0" matches the standard for `COUNT`.

### 5.2 Min / Max (multiset scan, pick extremum)

```python
min_state_visible(s) -> <col_type>:
    if s.is_empty(): return NULL
    entries = decode_multiset(s)  # ascending by key
    for (key, count) in entries:
        if count > 0:
            return key   # first positive entry is min
    return NULL

max_state_visible(s) -> <col_type>:
    if s.is_empty(): return NULL
    entries = decode_multiset(s)
    for (key, count) in reversed(entries):
        if count > 0:
            return key   # last positive entry is max
    return NULL
```

In normalized form, the early-return is `O(1)` amortized.

### 5.3 CountDistinct (multiset scan, count)

```python
count_distinct_state_visible(s) -> BigInt:
    if s.is_empty(): return 0
    entries = decode_multiset(s)
    return sum(1 for (_, count) in entries if count > 0)
```

`O(num_entries)`. In normalized form this equals `num_entries`; the filter is defensive against unnormalized intermediate state from `_state_union`.

### 5.4 ApproxCountDistinct (multiset → HLL estimate)

```python
approx_count_distinct_state_visible(s) -> BigInt:
    if s.is_empty(): return 0
    entries = decode_multiset(s)

    registers = [0u8; HLL_REGISTERS_COUNT]  # 16384 bytes, see §5.7
    for (key, count) in entries:
        if count <= 0: continue
        # HLL ignores multiplicity: same key contributes the same register update
        # whether it appears once or 100 times in the multiset.
        hash = hash_key_to_u64(key)
        update_register_from_hash(registers, hash)  # reuse hll_raw.rs helper

    return estimate_cardinality_from_registers(registers)
```

### 5.5 Cross-path equivalence with plain `approx_count_distinct(col)`

Required invariant:

> For any base dataset, the value returned by `SELECT approx_count_distinct_state_visible(__AGG_STATE_0__) FROM mv` is **bit-exactly** equal to the value returned by `SELECT approx_count_distinct(col) FROM <base>` on the same group.

This holds because:
- `HLL_REGISTERS_COUNT = 16384` is fixed across both paths.
- The hash function and `update_register_from_hash` helper are reused from `src/exec/expr/agg/functions/hll_raw.rs`. **The spec forbids introducing a new hash for ApproxCountDistinct's state-visible path.**
- The `estimate_cardinality_from_registers` formula (including bias correction for cardinality < 72000 at 16384 registers) is reused unchanged.

### 5.6 Complexity per kind

| Kind | Complexity | Notes |
|---|---|---|
| Count | O(1) | |
| Sum / Avg | O(1) | |
| BoolOr / BoolAnd | O(1) | |
| Min / Max | O(1) amortized | early-return in sorted multiset |
| CountDistinct | O(`num_entries`) | scan to count positive |
| ApproxCountDistinct | O(`num_entries`) + 16 KB transient buffer | see §5.7 |

### 5.7 ApproxCountDistinct buffer reuse (implementation note)

ApproxCountDistinct's visible allocates an HLL register array per call. For a chunk-level visible invocation on N rows, naïve allocation costs N × 16 KB transient memory.

**Implementation requirement (not normative in spec; enforced in implementation plan):** maintain a thread-local register buffer reused across rows in one chunk. Each row resets (memcpy zeros) → populates from multiset → estimates → emits BigInt → next row. The 16 KB zero-fill is ~1 µs per row, amortized across chunks.

### 5.8 Byte-symmetry between CountDistinct and ApproxCountDistinct MVs

Because both kinds share the §3.3 multiset encoding, two MVs built on the same base data — one using `COUNT(DISTINCT)` and one using `APPROX_COUNT_DISTINCT` — have **byte-identical state columns**. Only the visible function differs.

This is exploited as an MV semantic-upgrade hook: a future SQL command can switch an existing CountDistinct MV's visible to `approx_count_distinct_state_visible(...)` (or vice versa) without rebuilding state. Tracked under "future ergonomics" (§9.7).

## 6. Type Domain and NULL Semantics

### 6.1 Allowed input types for new kinds

CountDistinct and ApproxCountDistinct accept the same key types as P5's existing detail-state aggregates (§3.4):

| Type | Allowed? | Notes |
|---|---|---|
| Boolean | ✅ | Distinct count ≤ 2; useful only in pathological cases |
| Int8 / Int16 / Int32 / Int64 | ✅ | |
| Float32 / Float64 | ✅ | NaN canonicalized; ±0.0 unified |
| Decimal128 | ✅ | precision/scale must match base column |
| Date32 | ✅ | |
| Timestamp (μs) | ✅ | timezone-aware/naïve passthrough |
| Utf8 / LargeUtf8 | ✅ | |
| Binary / LargeBinary | ❌ V1 rejected | Future: §9.3 |
| Struct / List / Map / Union | ❌ V1 rejected | §4.5 error message guides projection |
| Dictionary | ❌ V1 rejected | Layout should unwrap to underlying type |

### 6.2 NULL semantics

| Scenario | Behavior |
|---|---|
| `COUNT(DISTINCT col)` row where `col IS NULL` | NULL **not counted** (SQL standard) |
| `APPROX_COUNT_DISTINCT(col)` row where `col IS NULL` | Same |
| Group consisting entirely of NULL rows | CountDistinct = 0; ApproxCountDistinct = 0; MV row preserved |
| Group with all rows deleted | MV row dropped via `__retraction_count__` reaching zero (P5 mechanism, unchanged) |

Implementation point: the `<kind>_state` and `<kind>_state_signed` aggregate functions skip rows where `args` is NULL during multiset encoding. This matches P5's existing `map_value_count` NULL-skip behavior.

### 6.3 Cross-path NULL consistency

Plain `count_distinct(col)` and `approx_count_distinct(col)` already skip NULL. The MV path **must** match this behavior — for the same base data, plain SELECT and MV SELECT must return identical values.

This is a **mandatory test invariant**, enforced in fixtures (§7).

### 6.4 Floating-point edge cases

For Float32 / Float64 keys:
- `NaN` canonicalizes to `f64::NAN.to_bits()` (single bit pattern). All `NaN`s share one distinct slot.
- `+0.0` and `-0.0` canonicalize to `+0.0` bytes. Treated as one distinct value.
- `+∞` and `-∞` each occupy their own distinct slot.

P5's MIN/MAX (`mv_agg_state.rs::derive_visible_from_detail_map_with_nan_min_finite_max_nan` test and friends) already encodes this canonicalization. CountDistinct/ApproxCountDistinct reuse it.

### 6.5 Type stability under Iceberg schema evolution

`AggregateMvLayout` pins input column types at MV CREATE time. If the base table's column type changes later:

| Change | Behavior |
|---|---|
| Compatible widening (Int32 → Int64) | Refresh reinterprets per new type; state remains usable |
| Incompatible (Int64 → Utf8) | Refresh fails at `validate_state_column_type`; user must DROP + CREATE |
| Column dropped | Existing P5 `iceberg_ivm_a11_*` behavior; MV enters failed state |

This entire surface inherits from P5; no new behavior introduced here.

### 6.6 ApproxCountDistinct precision range

For HLL with 16384 registers, the standard error is `1.04 / sqrt(16384) ≈ 0.81%`. Worst-case observed estimate deviation for unbiased input is ~1–2%. The hash function maps any allowed key type to a 64-bit value, so per-type accuracy is comparable.

Spec documents this user-facing accuracy guarantee.

## 7. SQL Fixtures

All fixtures live under `sql-tests/iceberg-ivm/sql/`, following the existing `iceberg_ivm_aggregate_*.sql` naming pattern.

### 7.1 New fixtures: CountDistinct

| File | Test point |
|---|---|
| `iceberg_ivm_aggregate_count_distinct_insert_only.sql` | Base INSERT only; verify multiset accumulation + visible count |
| `iceberg_ivm_aggregate_count_distinct_delete_boundary.sql` | DELETE removes the last row of a distinct value; entry drops from multiset; count decreases |
| `iceberg_ivm_aggregate_count_distinct_delete_non_boundary.sql` | DELETE removes a non-last row of a distinct value; entry count > 0 remains; distinct count unchanged |
| `iceberg_ivm_aggregate_count_distinct_null_skipped.sql` | NULL rows INSERT/DELETE don't affect distinct count; all-NULL group has count = 0 |
| `iceberg_ivm_aggregate_count_distinct_string.sql` | Utf8 keys: multi-byte characters, empty strings, long strings |
| `iceberg_ivm_aggregate_count_distinct_decimal.sql` | Decimal128 keys |
| `iceberg_ivm_aggregate_count_distinct_float_nan.sql` | NaN canonicalization, ±0.0 unified |
| `iceberg_ivm_aggregate_count_distinct_date.sql` | Date32 keys |
| `iceberg_ivm_aggregate_count_distinct_timestamp.sql` | Timestamp(μs) keys |
| `iceberg_ivm_aggregate_count_distinct_syntax_aliases.sql` | `count(DISTINCT col)`, `count_distinct(col)`, `multi_distinct_count(col)` all produce identical MV state |
| `iceberg_ivm_aggregate_count_distinct_partitioned.sql` | Partitioned base table + partition evolution |
| `iceberg_ivm_aggregate_count_distinct_reject_nested_key.sql` | `count(DISTINCT struct_col)` fails at MV CREATE with guiding error |
| `iceberg_ivm_aggregate_count_distinct_reject_multi_arg.sql` | `count(DISTINCT a, b)` fails at MV CREATE |

### 7.2 New fixtures: ApproxCountDistinct

| File | Test point |
|---|---|
| `iceberg_ivm_aggregate_approx_count_distinct_insert_only.sql` | Baseline; MV estimate matches plain `approx_count_distinct(col)` query |
| `iceberg_ivm_aggregate_approx_count_distinct_delete_retract.sql` | Estimate decreases after DELETE; tracks plain query |
| `iceberg_ivm_aggregate_approx_count_distinct_high_cardinality.sql` | 10⁵ distinct values; validates ~1% accuracy and state size growth |
| `iceberg_ivm_aggregate_approx_count_distinct_low_cardinality.sql` | 10 distinct values; bias correction path |
| `iceberg_ivm_aggregate_approx_count_distinct_aliases.sql` | `approx_count_distinct(col)` / `ndv(col)` / `hll_ndv(col)` produce identical state |
| `iceberg_ivm_aggregate_approx_count_distinct_reject_hint.sql` | `approx_count_distinct(col, 14)` fails at MV CREATE |
| `iceberg_ivm_aggregate_approx_count_distinct_null_skipped.sql` | NULL skip; all-NULL group estimate = 0 |
| `iceberg_ivm_aggregate_approx_count_distinct_string.sql` | Utf8 hash path |
| `iceberg_ivm_aggregate_approx_count_distinct_cross_check_with_plain.sql` | Bit-equal comparison: MV state visible == plain `approx_count_distinct` query result |

### 7.3 Cross-kind symmetry fixture

| File | Test point |
|---|---|
| `iceberg_ivm_aggregate_count_vs_approx_state_equality.sql` | Two MVs on the same base data (one CountDistinct, one ApproxCountDistinct). Assert via `DEBUG_DUMP_MV_STATE` that state column bytes are **byte-identical**; only visible values differ. |

### 7.4 P5 existing-fixture regression updates

The migration does not change visible semantics for BOOL_OR / BOOL_AND / MIN / MAX, but it does change MV state column type from `Map<K, Int64>` to `VARBINARY`. Fixtures fall into:

| Class | Update needed? |
|---|---|
| Result rows from `SELECT` (visible values) | No |
| Result rows from `SHOW CREATE MATERIALIZED VIEW` / `DESCRIBE` | Yes — state column type changes |
| Result rows from INFORMATION_SCHEMA / system catalog state inspection | Yes — same |

A specific file list is enumerated during implementation planning (depends on `grep` over current fixture contents). Spec only mandates: visible behavior has zero regression; only schema-reflection results require updates.

### 7.5 `DEBUG_DUMP_MV_STATE` tool function

Delivered as part of this spec (§1.4 motivation, §7.6 fixtures). Surface:

```sql
SELECT DEBUG_DUMP_MV_STATE(mv_table_name, row_id) AS state_json;
-- Returns JSON decoding of all __AGG_STATE_<n>__ columns for the given row.
-- Example:
--   {"agg_state_0": {"kind": "CountDistinct", "entries": [{"key": "alice", "count": 2}, ...]}}
```

Implementation: scalar function in `src/exec/expr/scalar/`, dispatches to per-kind decoder.

### 7.6 Fixtures for the debug tool

| File | Test point |
|---|---|
| `debug_dump_mv_state_count_distinct.sql` | Decode CountDistinct state, observe entries |
| `debug_dump_mv_state_min_max.sql` | Decode Min/Max state (verify post-migration introspection still works) |
| `debug_dump_mv_state_approx_count_distinct.sql` | Decode and display multiset contents + estimated cardinality |

### 7.7 ApproxCountDistinct golden-result strategy

HLL estimates depend on the hash function's exact bit behavior. To avoid bleeding hash implementation details into fixtures, ApproxCountDistinct fixtures either:

- Use `-- @approximate_value_tolerance=<percent>` annotation for tolerance-based comparison, OR
- Use deterministic input known to hit the estimation formula exactly, allowing bit-equal golden results.

Cross-platform float-hash consistency is already enforced by plain HLL tests (NovaRocks supports little-endian platforms only).

## 8. Migration Plan

### 8.1 Impact scope

Pre-migration schema: BOOL_OR / BOOL_AND / MIN / MAX MV `__agg_state_<n>__` column type is `Map<K, Int64>` (Parquet `List<Struct<key, value>>`).

Post-migration schema: same column type is `VARBINARY`.

This is **byte-level incompatible**. In-place upgrade is not possible; users must DROP + CREATE.

### 8.2 Migration trigger

NovaRocks does not have a versioned release process yet; MV refresh is not required to maintain cross-version compatibility. The migration lands atomically in a single PR:

1. Framework switched to VARBINARY, per-kind state combinator functions registered.
2. All `Map<K, Int64>`-dependent code paths in `mv_agg_state.rs` (Map serialization, PARQUET:field_id handling, accumulator) deleted in the same PR.
3. All existing BOOL_OR / MIN / MAX fixture results for schema-reflection queries updated in the same PR.

No grace period, no dual-mode framework.

### 8.3 User-side action

Any MV created with P5 prior to this PR will fail on refresh with:

```
materialized view `<mv_name>` was created with a legacy state column format
(MAP<...>). This format is no longer supported as of <commit-sha>. Please
recreate the materialized view:

  DROP MATERIALIZED VIEW <mv_name>;
  CREATE MATERIALIZED VIEW <mv_name> AS <original SELECT>;
```

The error message provides copy-executable DROP + CREATE guidance.

### 8.4 Detection mechanism

`AggregateMvLayout::load_from_existing_mv`, upon opening an existing MV, checks the first `__agg_state_<n>__` column's physical Arrow type:

```rust
match column.data_type() {
    DataType::Binary => Ok(layout),  // new format
    DataType::Map(_, _) | DataType::List(_) => Err(legacy_format_error(mv_name)),
    other => Err(unexpected_state_type_error(other)),
}
```

Failure occurs fail-fast at MV open (refresh / query entry), not at chunk-level deserialization.

### 8.5 Explicitly out of scope for migration

- **Automatic data migration / state conversion tool**: not delivered. Justification: (a) tangent to design goal; (b) correctness of an automatic converter would itself require a fixture matrix and slow the mainline.
- **Multi-version coexistence**: framework does not maintain dual Map+VARBINARY support.
- **Release-notes auto-generation**: hand-written in the migration PR.

### 8.6 Release-notes template (normative)

The migration PR ships with the following text in commit message and release notes:

```
**Breaking change**: incremental materialized view state column format
migrated from MAP<K, Int64> to opaque VARBINARY. This unifies state encoding
across all aggregate functions and unblocks future kinds (count_distinct,
approx_count_distinct, array_agg). Existing materialized views built with
BOOL_OR / BOOL_AND / MIN / MAX must be recreated:

  DROP MATERIALIZED VIEW <name>;
  CREATE MATERIALIZED VIEW <name> AS <original SELECT>;

External tools reading the MV's Iceberg-backed Parquet files will see state
columns as BINARY; decoding requires NovaRocks-specific knowledge. Use
DEBUG_DUMP_MV_STATE(mv_name, row_id) for inspection.
```

## 9. Future Evolution

All items below are deferred work; none of them require breaking the V1 state schema, because the §3.1 `version` byte and per-kind dispatcher are forward-compatible hooks.

### 9.1 V2 candidates: internal encoding switches

| Change | Path | Blocked by this spec? |
|---|---|---|
| CountDistinct: multiset entries → Roaring of row_ids | New version byte `0x02`; new deserializer; rewriter emits new `count_distinct_state` body | No |
| ApproxCountDistinct: → true HLL register array (bounded ~16 KB state) | New version byte `0x03`; visible reads registers directly | No |
| Min/Max: sorted list → balanced-tree encoding | New version byte; performance optimization | No |

Old V1 MVs remain readable by new code (version byte dispatch); newly created MVs use the new encoding.

### 9.2 Candidate new kinds (deferred)

| Kind | Approach | Estimate |
|---|---|---|
| `ARRAY_AGG(col)` | Shares §3.3 multiset encoding; visible flattens `entries × count` into `Array<T>`, sorted by key for determinism | 2–3 days |
| `SUM_DISTINCT(col)` | Distinct multiset already in place; visible sums positive keys | 0.5 day |
| `AVG_DISTINCT(col)` | Same as SUM_DISTINCT, divides by positive entry count | 0.5 day |
| `MIN_DISTINCT` / `MAX_DISTINCT` | Equivalent to MIN/MAX (DISTINCT is a no-op); classifier maps directly | 0.1 day |
| `BITMAP_UNION` / `BITMAP_AGG` | Requires Roaring IVM infra (§9.1 V2 path) as prerequisite | Depends on §9.1 |
| Multi-column `COUNT(DISTINCT a, b)` | Key serialization extends to `Struct<a, b>` (see §9.4) | 1–2 days |

### 9.3 Binary key types

Binary / LargeBinary rejected in V1 (§6.1). Enablement path: extend §3.4 key serialization with `Binary → ULEB128 length + raw bytes`, accept Binary in the classifier. No framework change.

### 9.4 Multi-column DISTINCT

`COUNT(DISTINCT a, b)` rejected in V1 (§4.3). Enablement path:
- Key serialization extends to `Struct` encoding: `ULEB128 num_fields + field_serializations[num_fields]`.
- Classifier accepts argument count > 1.
- Visible derivation unchanged.

### 9.5 ApproxCountDistinct hint argument

`approx_count_distinct(col, hint)` rejected in V1 (§4.4). Enablement path:
- MV metadata gains `hll_precision: u8` field (default 14).
- Visible function reads metadata to size the register array (2^precision).
- `_state_compute` unchanged (multiset encoding has no precision concept).
- §5.8 byte-symmetry breaks for MVs with non-default precision; document at enablement time.

### 9.6 Cross-engine readability

Loss of external direct reads (§1.4) can be mitigated by a "compatibility view": a parallel `<mv>_compat` Iceberg view exposing decoded state as Arrow Map via NovaRocks-emitted UDF. Read-only; writes still flow through NovaRocks.

Not in this spec.

### 9.7 In-place state re-encoding

When V2 encodings (§9.1) land, two upgrade paths exist for V1 MVs:

- **Lazy**: framework reads both V1 and V2, writes only V2. Existing MV state is overwritten as V2 on the next refresh.
- **Eager**: `ALTER MATERIALIZED VIEW <name> REENCODE STATE` SQL command forces full state re-serialization.

V1 spec defaults to Lazy (the version-byte path is already prepared); Eager is future ergonomics.

## 10. Out of Scope

### 10.1 Aggregate function extensions not in scope

From [aggregate-function-gaps.md](../../../NovaRocks%20TODO/aggregate-function-gaps.md):

| Function | Status | Note |
|---|---|---|
| `ARRAY_AGG(col)` | Deferred (own spec) | §9.2 future; shares multiset encoding |
| `BITMAP_UNION` / `BITMAP_AGG` / `BITMAP_UNION_COUNT` | Deferred (depends on V2 encoding) | StarRocks IVM also doesn't support |
| `HLL_UNION` / `HLL_RAW_AGG` | Deferred (depends on true-HLL V2 path) | StarRocks IVM also doesn't support |
| `STDDEV` / `VAR_*` / `PERCENTILE_*` | **Declined** | StarRocks IVM also doesn't; no alignment value |
| `GROUP_CONCAT` / `STRING_AGG` | **Declined** | Same; output overlap with ARRAY_AGG |
| `JSON_OBJECTAGG` / `JSON_ARRAYAGG` | **Declined** | Retract semantics undefined |
| `FIRST_VALUE` / `LAST_VALUE` / `ANY_VALUE` | **Declined** | Non-deterministic; IVM retract behavior undefinable |
| `MAX_BY` / `MIN_BY` | Deferred | StarRocks also not in IVM whitelist |

### 10.2 Framework changes not in scope

| Item | Note |
|---|---|
| Map ↔ VARBINARY dual-mode coexistence | §8 mandates single-PR full migration |
| Automatic state-data migration tooling | §8.5 |
| Multi-version dispatcher (V1 + V2 simultaneously) | §9.1 reserves version byte; V2 decoders are future |
| `ALTER MATERIALIZED VIEW <name> REENCODE STATE` | §9.7 future ergonomics |
| Cross-engine MV state compatibility view | §9.6 future |

### 10.3 SQL surface extensions not in scope

| Item | Note |
|---|---|
| Multi-column `COUNT(DISTINCT a, b)` | §9.4 deferred |
| `approx_count_distinct(col, precision)` hint argument | §9.5 deferred |
| Binary / LargeBinary distinct keys | §6.1 deferred |
| Dictionary distinct keys | §6.1 deferred |
| Window function aggregate IVM | Orthogonal to detail-state framework; own design |

### 10.4 Optimization items not in scope

| Item | Note |
|---|---|
| State size pressure protection / alarms / spill | Observability work, separate spec |
| ApproxCountDistinct register-buffer pooling specifics | §5.7 implementation detail, decided in plan phase |
| State encoding compression (zstd, etc.) | §9.1 implicit V2 path; not delivered here |

### 10.5 Related TODO items not in scope

| TODO | Relationship |
|---|---|
| [aggregate-mv-min-max-detail-state](../../../NovaRocks%20TODO/aggregate-mv-min-max-detail-state.md) | Completed in P5. This spec migrates it to VARBINARY without changing MIN/MAX semantics. |
| [union-all-multi-base aggregate IMV](../../../NovaRocks%20TODO/union-all-multi-base.md) | About IVM shape, not agg function. Orthogonal. |
| FE iceberg v1.metadata.json compat | Unrelated to IVM agg functions. |

---

## Summary of normative requirements

The implementing PR must:

1. Migrate all detail-state aggregate state columns from `Map<K, Int64>` to opaque `VARBINARY`, removing the legacy Map path entirely.
2. Register the `<kind>_state` / `<kind>_state_signed` / `<kind>_state_union` / `<kind>_state_visible` SQL function families for `Count`, `Sum`, `Avg`, `Min`, `Max`, `BoolOr`, `BoolAnd`, `CountDistinct`, `ApproxCountDistinct` per §2.
3. Add `AggregateFunctionKind::CountDistinct` and `AggregateFunctionKind::ApproxCountDistinct` to the enum and classifier dispatch per §4.
4. Implement byte encodings per §3 with version byte `0x01`.
5. Implement visible derivations per §5; ApproxCountDistinct visible must reuse the plain HLL hash and estimator to satisfy cross-path bit-equality.
6. Implement `DEBUG_DUMP_MV_STATE(mv_name, row_id)` as a JSON-emitting scalar function.
7. Land all SQL fixtures listed in §7.1–§7.6.
8. Update existing P5 schema-reflection fixture results per §7.4.
9. Land the migration error message and release-notes template per §8.3, §8.6.

Implementation phasing is decided in the writing-plans skill output.
