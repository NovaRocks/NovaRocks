# Iceberg Target Aggregate MV `BOOL_OR` / `BOOL_AND` (+ MIN/MAX-over-Boolean) via Detail-State Map

**Status:** Draft for user review
**Date:** 2026-05-23
**Builds on:**
- `docs/superpowers/specs/2026-05-21-iceberg-mv-min-max-detail-state-design.md` (IVM-P5 detail-state framework)
- PR [#160](https://github.com/NovaRocks/NovaRocks/pull/160) — MIN/MAX via `Map<K, Int64>` detail state landed
- PR [#162](https://github.com/NovaRocks/NovaRocks/pull/162) — Float MIN/MAX + non-Int64 SQL coverage landed

**Related Obsidian roadmap:**
- `IVM-aggregate-function-gaps.md` (entry point)
- `IVM-P5-aggregate-mv-min-max-detail-state.md` (predecessor; framework already in place)

**StarRocks cross-reference:**
- `fe/fe-core/src/main/java/com/starrocks/sql/analyzer/mv/IVMAnalyzer.java:104-130`
  `IVM_SUPPORTED_AGG_FUNCTIONS` includes `BOOL_OR` (with retract via detail-map count state, same family as MIN/MAX)

---

## 1. Goal

Extend NovaRocks Iceberg target aggregate / join-aggregate IMV's
detail-state framework to cover three additional aggregate operations:

1. **`BOOL_OR(col)`** — visible = `OR` of all non-null booleans in the group
2. **`BOOL_AND(col)`** — visible = `AND` of all non-null booleans in the group
3. **`MIN(bool_col)` / `MAX(bool_col)`** — already worked in plain SQL but
   was rejected at the IVM DDL gate because of a stale "AggScalarValue does
   not support Boolean" comment. Lift that.

All three share one underlying state: `Map<Boolean, Int64>` per group
(detail map with `key ∈ {true, false}` and `count = positive int`).

### Scope

- Iceberg target aggregate MV (`IncrementalMvShape::Aggregate`)
- Iceberg target join-aggregate MV (`IncrementalMvShape::JoinAggregate`)
- Boolean argument column only (not nullable bool — handled per existing NULL
  semantics in `map_value_count`)
- Both INSERT and DELETE delta paths via existing `__change_op` signed-delta
  rewriter (full reuse of IVM-P5 path)
- Plain `BOOL_AND` aggregate executor (currently missing from
  `src/exec/expr/agg/functions/`) is part of this PR's pre-work

### Non-Goals

- Other aggregate functions (ARRAY_AGG / NDV / HLL etc.) — separate TODO entries
- managed-lake target aggregate MV — separate path, may follow later
- `BOOL_XOR` / `BIT_OR` / `BIT_AND` / `BIT_XOR` — not in StarRocks IVM either
- Performance optimization (`is_sync` cache flag) — same v1 simplification
  as P5; visible is recomputed at every merge step
- Schema migration for existing MVs — DDL is the schema boundary
  (memory note `feedback_no_backwards_compat_for_novarocks`)

---

## 2. Decision Summary

| Dimension | Decision |
|---|---|
| State representation | Per-group `Map<Boolean, Int64>` (Arrow `DataType::Map` with key = `Boolean`, value = `Int64`). Key cardinality bounded at 2 (`true`/`false`) — extremely cheap |
| Detail-map ownership | Identical to P5: `__agg_state_<col>` column holds the map; no separate scalar. Visible Boolean derived at write time |
| INSERT delta | `map_value_count(arg)` — already implemented in P5 |
| DELETE delta | `map_value_count_signed(arg, __change_op)` — already implemented in P5 |
| Visible derive | Per-function dispatch in `update_visible_values_from_state` (`mv_agg_state.rs:1971`); new helpers `derive_bool_or_from_detail_map` / `derive_bool_and_from_detail_map`; MIN/MAX-over-Boolean reuses existing `derive_visible_from_detail_map` |
| `is_sync` optimization | Not included in v1 — visible is recomputed by counting entries every merge |
| DDL gate | `validate_state_column_type` allows `DataType::Boolean` Map key for `(BoolOr \| BoolAnd, Single)`, and (newly) also for `(Min \| Max, Single)`. Other aggregates over Boolean still rejected |
| Empty group lifecycle | Identical to P5: group is deleted from MV when all detail counts == 0 |
| NULL handling | NULL boolean inputs are skipped (do not contribute to detail map) — same as `map_value_count` already does for other K types |
| Backwards compat | None — DDL is the schema boundary |

---

## 3. Architecture: What Changes

### 3.1 Reused without modification

IVM-P5 already provides:

- `AggScalarValue::Bool(bool)` variant + `scalar_from_array` Boolean path
  (`src/exec/expr/agg/functions/common.rs:185, 204-213`)
- `key_fingerprint` Boolean encoding (`common.rs:703-706`)
- `compare_agg_scalar_values` handles Bool (verified in existing tests)
- `map_value_count(arg)` and `map_value_count_signed(arg, __change_op)`
  aggregate functions are generic over K — work with Boolean inputs without
  any change
- `merge_value_count_map_state` and `accumulate_map_entry`
  (`mv_agg_state.rs:1132-1215`) are generic over K
- Detail-map signed-delta rewriter pattern in
  `ivm_delta_aggregate.rs:159-184` — extend the match arm
- Arrow `MapArray` field-name convention (`key_value`, value nullable=false)
  + `PARQUET:field_id` metadata propagation — all P5 bug-fix infrastructure
  carries over

### 3.2 New / Modified Code

| File | Change |
|---|---|
| `src/exec/expr/agg/functions/bool_and.rs` | **NEW** — clone of `bool_or.rs`; `BoolAndAgg` struct with `accumulate = state.value && row`, `init = (has_value: false, value: true)`. Trivial swap |
| `src/exec/expr/agg/functions/mod.rs` | Add `pub mod bool_and;`; register `"bool_and" \| "booland_agg" => Ok(&BOOL_AND)` in `resolve_by_func`. Add `AggKind::BoolAnd` variant if a single enum gates them |
| `src/connector/starrocks/managed/mv_shape.rs` | `AggregateFunctionKind` extends to 7 variants: add `BoolOr`, `BoolAnd`. `classify_aggregate_call` dispatch adds `"bool_or" \| "boolor_agg"` and `"bool_and" \| "booland_agg"` cases — both share `classify_bool_or_and_input` (single Boolean-coerceable expr, no DISTINCT/OVER/FILTER) |
| `src/connector/starrocks/managed/mv_agg_state.rs` | (a) Layout: `AggregateMvLayout` recognizes BoolOr/BoolAnd as detail-state form, same `Map<K, Int64>` physical type as Min/Max but with K = Boolean.<br>(b) `validate_state_column_type`: extend Min/Max Boolean-key acceptance (remove `1795-1797` reject); add new BoolOr/BoolAnd arms accepting only Boolean.<br>(c) Visible derive: `update_visible_values_from_state` adds BoolOr/BoolAnd arms calling new helpers `derive_bool_or_from_detail_map` / `derive_bool_and_from_detail_map`. MIN/MAX arm continues to call generic `derive_visible_from_detail_map`, which already handles Bool via existing `compare_agg_scalar_values` |
| `src/connector/starrocks/managed/ivm_delta_aggregate.rs` | Extend the MIN/MAX `map_value_count_signed` rewriter arm (line 159-184) to also cover BoolOr/BoolAnd. Insert-side rewriter (P5 Phase 3 location) same extension |
| Other files mentioning `AggregateFunctionKind` exhaustive matches | New variants will need wildcard or explicit arms. Compiler will flag — fix as they appear |
| `sql-tests/iceberg-ivm/sql/iceberg_ivm_aggregate_bool_or.sql` | **NEW** fixture |
| `sql-tests/iceberg-ivm/sql/iceberg_ivm_aggregate_bool_and.sql` | **NEW** fixture |
| `sql-tests/iceberg-ivm/sql/iceberg_ivm_aggregate_min_max_bool.sql` | **NEW** fixture — verifies MIN(bool)/MAX(bool) IVM end-to-end since the DDL gate is being lifted |

---

## 4. Semantics — Visible Derivation

### 4.1 BOOL_OR

Standard SQL: `BOOL_OR(col)` returns `true` if any non-null `col` is `true`,
`false` if at least one `col` exists and all non-null are `false`, `NULL` if
no non-null inputs exist.

In detail-map terms (`m: Map<Boolean, Int64>`):

```
true_count  = m.get(true).unwrap_or(0)
false_count = m.get(false).unwrap_or(0)

if true_count > 0:           visible = true
elif false_count > 0:        visible = false
else (both 0 / empty):       visible = NULL
```

(`count == 0` entries are pruned eagerly at write time per P5 contract, so
empty map and "all counts 0" are the same case.)

### 4.2 BOOL_AND

Standard SQL: `BOOL_AND(col)` returns `false` if any non-null `col` is
`false`, `true` if at least one `col` exists and all non-null are `true`,
`NULL` if no non-null inputs exist.

```
true_count  = m.get(true).unwrap_or(0)
false_count = m.get(false).unwrap_or(0)

if false_count > 0:          visible = false
elif true_count > 0:         visible = true
else:                        visible = NULL
```

### 4.3 MIN(bool) / MAX(bool)

Convention: `false < true` (matches NovaRocks plain MIN/MAX via
`AggKind::MinBool` / `MaxBool`).

```
MIN: pick the smallest key with count > 0 (false if false_count > 0, else true)
MAX: pick the largest key with count > 0 (true if true_count > 0, else false)
```

Existing `derive_visible_from_detail_map` already does
"smallest/largest key with positive count" generically; for K=Boolean it
falls back on `compare_agg_scalar_values(Bool, Bool)` which handles
`false < true` correctly. **No new helper needed for MIN/MAX-bool** — the
generic helper just works once Boolean is unlocked at the DDL gate.

---

## 5. NULL handling — verify reuse

`map_value_count(arg)` semantics for NULL inputs: drop them. Confirmed by
inspection of `mv_agg_state.rs` Boolean key path and `key_fingerprint`
behavior (NULL keys collapsed). Boolean follows the same path.

A group consisting entirely of NULL Booleans → detail map is empty → visible
falls to `NULL` for BOOL_OR/BOOL_AND/MIN/MAX. Matches standard SQL.

A group with a mix of NULLs and one `true` → map = `{true → 1}` →
`BOOL_OR = true, BOOL_AND = true, MIN = true, MAX = true`. Standard SQL agrees.

---

## 6. Edge Cases

| Case | Detail map | BOOL_OR | BOOL_AND | MIN | MAX |
|---|---|---|---|---|---|
| Empty / all NULL | `{}` | NULL | NULL | NULL | NULL |
| All true | `{true → n}` | true | true | true | true |
| All false | `{false → n}` | false | false | false | false |
| Mix | `{true → a, false → b}` (a>0, b>0) | true | false | false | true |
| INSERT then DELETE last true | start `{true→1}`, delete brings to `{true→0}` → pruned → `{}` | NULL | NULL | NULL | NULL |
| Boundary retract | INSERT 3 true, DELETE 3 true (after `{true→3}` ↓ to `{true→0}` pruned) | NULL | NULL | NULL | NULL |

These edge cases will be exercised by the SQL fixtures.

---

## 7. Risks / Open Questions

| Risk | Mitigation |
|---|---|
| `BOOL_AND` is referenced in NovaRocks codegen / analyzer (`expr_compiler.rs`, `helpers.rs`, etc.) but executor is missing — surprises may surface | Phase Pre-A writes the executor first and runs `cargo build --lib + test --lib` to confirm it compiles + matches the speculative type declarations |
| `AggregateFunctionKind` is matched exhaustively in many places (not just mv_shape.rs / mv_agg_state.rs / ivm_delta_aggregate.rs) | Trust compiler errors; add arms as they appear. P5 already extended this enum by 0 variants (Min/Max reused existing slot) so this is the first extension of the enum. Expect ~5-10 match sites to update |
| Boolean as Iceberg field-id round-trip — was it covered in P5 metadata-handling bugs? | The Arrow `MapArray` field naming + `PARQUET:field_id` propagation infrastructure (P5 bug fixes #2 / #3 / #4) is type-agnostic — Boolean keys go through the same paths. Will verify via fixture |
| Detail-map merge for K=Boolean is *trivially* small (≤ 2 entries) — micro-perf gap vs higher-cardinality keys nonexistent | N/A — actually a win |

---

## 8. Success Criteria

1. `cargo build --lib` + `cargo clippy --lib` clean (no new warnings)
2. `cargo test --lib` all tests pass (currently 2584; adding ~6 unit tests should bring it to ~2590)
3. SQL suites all green, no regression:
   - `iceberg-ivm`: 46/46 → **49/49** (add bool_or / bool_and / min_max_bool fixtures)
   - `iceberg`: 67/67 → 67/67
   - `iceberg-rest`: 9/9 → 9/9
   - `iceberg-compatibility`: 12/12 → 12/12
4. DDL `CREATE MATERIALIZED VIEW ... AS SELECT region, BOOL_OR(flag) FROM t GROUP BY region` succeeds
5. INSERT + DELETE retract correctness verified end-to-end (boundary case: last `true` row deleted → BOOL_OR transitions `true → NULL` correctly without falling back to full refresh)
6. Plain `SELECT BOOL_AND(flag)` works (proves Pre-Phase executor is healthy)

---

## 9. Out of Scope (Explicit Non-Goals)

- ARRAY_AGG / NDV / APPROX_COUNT_DISTINCT — separate Obsidian TODOs
- `BOOL_XOR`, `BIT_*` aggregates — not in StarRocks IVM either
- managed-lake target MV BOOL_OR/AND — separate connector path
- `BIT_OR` / `BIT_AND` / `BIT_XOR` over integer columns — different state shape
  (running int reduction, not detail-map)
- Boolean as a GROUP BY key in IVM — orthogonal feature; the IVM shape
  classifier doesn't currently restrict GROUP BY key types and we're not
  touching that
