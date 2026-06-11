# Iceberg MV `MIN/MAX` via Value-Count Detail State — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task.

- **Spec**: `docs/design/specs/2026-05-21-iceberg-mv-min-max-detail-state-design.md`
- **Date**: 2026-05-21
- **Goal**: Allow Iceberg target aggregate / join aggregate IMV to contain
  `MIN(col)` / `MAX(col)` and refresh incrementally on both INSERT and DELETE
  deltas, without falling back to full refresh.
- **Architecture**: Store per-group `Map<value, Int64>` detail state in
  hidden `__agg_state_<col>` columns; derive visible MIN/MAX by iterating
  non-zero map entries at every merge. Reuses 90% of the existing aggregate
  IMV pipeline (PRIMARY KEY layout, delta SELECT rewrite, signed-delta path,
  staging-branch commit).
- **Tech stack**: Rust, Arrow `MapArray`, Iceberg `map<K, V>` field type,
  existing NovaRocks IVM engine.

**Scope:** This plan covers PR 1 through PR 5; PR 6 is verification only. Each
phase is one independently mergeable PR.

---

## Phase 1: New aggregates `map_value_count` and `map_value_count_signed`

**Files:**
- New: `src/exec/expr/agg/functions/map_value_count.rs` (the aggregate
  implementation file — follow `src/exec/expr/agg/functions/map_agg.rs` as
  the nearest example: `pub(super) struct ...Agg; impl AggregateFunction for
  ...Agg { ... }`)
- Modify: `src/exec/expr/agg/functions/mod.rs` — add two variants to
  `AggKind` enum (`MapValueCount`, `MapValueCountSigned`), add `mod
  map_value_count;` and the corresponding `use map_value_count::...`, wire
  them into `kind_from_name` and the dispatch switch
- Modify: `src/sql/analyzer/functions.rs` — declare `map_value_count` /
  `map_value_count_signed` as recognized aggregate function names with the
  right argument arity + return type binding
- Modify: any codegen site that maps function name strings to `AggKind`
  (search `kind_from_name` for the canonical entry point in
  `src/exec/expr/agg/functions/`)

**What to do:**

1. Implement `MapValueCountAccumulator<K>` parameterized by the input scalar
   type `K ∈ {Int8, Int16, Int32, Int64, Float32, Float64, Decimal128,
   Decimal256, Utf8, Date32, Timestamp}`:
   - Internal state: `HashMap<K, i64>`
   - `update(value)`: `state[value] += 1` (skip if `value` is NULL)
   - `update_signed(value, change_op)`: `state[value] += change_op`
   - `finalize()`: serialize the hash map into an Arrow
     `MapArray<K, Int64>` row. Order keys deterministically (sorted
     ascending) so unit tests are stable.
   - `merge_states(other_map_array_row)`: take an Arrow MapArray slice
     representing another accumulator's finalized state, accumulate it into
     `self` via key-wise addition. Used when the executor merges partial
     aggregate states across pipeline stages.
2. Register both functions in the function registry so analyzer / codegen
   recognize them with return type `Map<K, Int64>`.
3. NULL handling: `update` skips NULL input rows entirely. Output is an empty
   map if all input rows were NULL.
4. Empty group: produces an empty `MapArray` row, not NULL.

**Unit tests** (in `src/exec/expr/agg/functions/map_value_count.rs` `#[cfg(test)] mod tests`):

- Insert 3 distinct Int64 values 5 times each → finalize → assert the
  MapArray row has exactly 3 entries, each with count 5.
- Insert mix including NULL → NULL rows ignored; non-NULL counts correct.
- `update_signed(v, -1)` on a value not previously seen → result map has that
  value with count -1 (the merge layer handles the consolidation later).
- Merge two partial states → sum of counts per key; keys union.
- All scalar input types: round-trip `update → finalize → re-deserialize via
  merge_states` to make sure the MapArray layout is stable.
- Edge: empty input → empty MapArray row.

**Acceptance:** `cargo test --lib map_value_count` green. `cargo clippy` no new warnings.

**Commit message:** `feat(ivm): map_value_count aggregate for MIN/MAX detail state`

---

## Phase 2: Schema — `Map<K, Int64>` state column for MIN/MAX

**Files:**
- Modify: `src/connector/starrocks/managed/mv_agg_state.rs`
  - `validate_state_column_type` (search for the existing helper at module
    scope) — allow `Map<K, Int64>` for `(Min|Max, Single)`
  - `build_aggregate_mv_layout` lines 147-177 — for `Min`/`Max`, set
    `data_type = DataType::Map(...)` with key type = input arrow type, value
    type = Int64; set `sql_type = SqlType::Map(...)` accordingly
- Modify: `src/connector/starrocks/managed/mv_ddl.rs`
  - `arrow_data_type_to_sql_type` if it does not already handle Map; if it
    panics on Map, add the `DataType::Map(...) => SqlType::Map(...)` branch
- Modify: `src/engine/mv/iceberg_aggregate_state.rs` (if it has any narrowing
  on state column type — search for `state_columns.iter()` patterns)
- Modify: `src/connector/iceberg/catalog/backend.rs` or sink path — confirm
  Iceberg target table can be created with a `map<K, BigInt>` column type;
  add a smoke test if not.

**What to do:**

1. In `build_aggregate_mv_layout`, the `Min`/`Max` branch (currently the
   same as `Count`/`Sum`):
   - Resolve the input type of the aggregate's argument from the analyzed
     query (`shape.aggregates[i].input` or equivalent)
   - Build `DataType::Map(Arc::new(Field::new("entries",
     DataType::Struct(...), false)), false)` with key=input arrow type
     value=Int64
   - Build matching `SqlType::Map(Box::new(<input_sql_type>),
     Box::new(SqlType::BigInt))`
   - `nullable = false` (empty map is valid, never NULL)
2. Extend `validate_state_column_type` to accept Map for MIN/MAX-Single.
3. Add a unit test verifying that a synthesized MV with
   `MIN(int64_col), MAX(varchar_col)` produces `AggregateMvLayout` with two
   state columns, both `DataType::Map(...)` typed correctly.

**Unit tests** (in `src/connector/starrocks/managed/mv_agg_state.rs`):

- `build_aggregate_mv_layout` with `MIN(amount)` argument type Int64 → state
  column data_type is `Map<Int64, Int64>`
- Same with `MAX(name)` argument type Utf8 → state column data_type is
  `Map<Utf8, Int64>`
- AVG / SUM / COUNT branches untouched (regression).

**Acceptance:** `cargo test --lib mv_agg_state` includes new Map-shape
assertions and passes. `cargo build` succeeds against any Iceberg
infrastructure that scans `AggregateMvLayout`.

**Commit message:** `feat(ivm): allow Map<K, Int64> state column for MIN/MAX`

---

## Phase 3: Delta SELECT rewrite emits `map_value_count` for MIN/MAX

**Files:**
- Modify: `src/connector/starrocks/managed/mv_shape.rs` line 1350
  (`rewrite_select_sql_for_state`)
- Modify: `src/connector/starrocks/managed/ivm_delta_aggregate.rs` lines
  45-55 (signed-delta rejection) and lines 165-175 (`unreachable!`
  fallthrough)

**What to do:**

1. In `mv_shape::rewrite_select_sql_for_state` (insert path):
   - For each MIN/MAX aggregate in the shape, emit **two** projection items
     in the rewritten SELECT:
     - Visible: `MIN(arg)` / `MAX(arg)` with alias = visible column name
     - State: `map_value_count(arg)` with alias = `__agg_state_<sanitized>`
   - Today MIN/MAX produces only the visible item (scalar state was the same
     value); now state is a Map, so it requires a separate projection item.
2. In `ivm_delta_aggregate::rewrite_select_sql_for_signed_delta_state` and
   `_with_change_op_qualifier`:
   - Remove the early `MIN/MAX aggregate outputs are not reversible` rejection
   - In the `push_signed_aggregate_state_projection` function around line
     170, the `MIN/MAX` branch currently does `unreachable!(...)`. Replace
     with: emit `map_value_count_signed(arg, change_op)` as the state column
     projection, and `NULL` (or omit) for the visible column projection — the
     visible column will be re-derived from the merged state, not from this
     delta's data (see Phase 4 derive helper).
3. Update existing unit tests in those files:
   - `signed_delta_rewrite_rejects_min_max` (`ivm_delta_aggregate.rs:374`) —
     rename to `signed_delta_rewrite_accepts_min_max_with_map_value_count`
     and replace the assertion with: the rewritten SQL contains
     `map_value_count_signed`.

**Unit tests** (extend existing test modules):

- `mv_shape.rs`: rewrite `SELECT region, MIN(amount), COUNT(*) FROM t GROUP
  BY region` → rewritten SQL contains both `MIN(amount) AS <visible>` and
  `map_value_count(amount) AS __agg_state_<...>`
- `ivm_delta_aggregate.rs`: rewrite same query under signed delta → SQL
  contains `map_value_count_signed(amount, __change_op)`
- Negative: aggregate with un-supported input type still rejected at layout
  layer (Phase 2 ensures this).

**Acceptance:** `cargo test --lib` covers the new positive cases for both
rewriters; previous rejection tests are flipped to acceptance tests.

**Commit message:** `feat(ivm): rewrite MIN/MAX to map_value_count in delta state SELECT`

---

## Phase 4: Merge / Negate / Derive-Visible for Map state

**Files:**
- Modify: `src/connector/starrocks/managed/mv_agg_state.rs`
  - Lines 846-878 (`merge_state_value` dispatch)
  - Lines 959-970 (replace `merge_min_max_state_value` body — keep the name
    or rename, but the dispatch must now go through Map)
  - Lines 780-822 (`negate_aggregate_state_chunks` — remove the panic;
    route MIN/MAX-Single to `negate_value_count_map_state`)
- Modify: `src/engine/mv/iceberg_aggregate_state.rs`
  (`merge_aggregate_target_state` / visible derivation — search for sites
  that build visible columns from state)
- New helpers (same file, `mv_agg_state.rs`):
  - `fn merge_value_count_map_state(old, delta, state_column) -> Result<...>`
  - `fn negate_value_count_map_state(array: &MapArray, state_column) -> Result<ArrayRef, String>`
  - `fn derive_visible_from_detail_map(array: &MapArray, op: MinMax) -> Result<Option<AggScalarValue>, String>`
  - `fn prune_zero_entries_from_map(array: &MapArray) -> Result<ArrayRef, String>`

**What to do:**

1. Implement `merge_value_count_map_state`:
   - Both inputs are `AggScalarValue::Map(...)` (or extend AggScalarValue if
     it has no Map variant; if Decoder needed, see note below)
   - Build merged HashMap<K, i64>: iterate old entries, then delta entries,
     summing counts per key
   - Call `prune_zero_entries_from_map` on the merged result
   - Return `Some(AggScalarValue::Map(...))`
   - **AggScalarValue note**: if `AggScalarValue` is purely scalar (no
     composite type variant), introduce `AggScalarValue::Map(MapArray)` — but
     this changes the enum's semantics. Alternative: bypass AggScalarValue
     entirely for Map state by storing the merged Arrow MapArray directly in
     the row chunk and writing a separate `merge_value_count_map_arrays` that
     operates on `&MapArray` inputs.
2. Implement `negate_value_count_map_state`:
   - Input: `MapArray` (one row = one map per group)
   - Output: `MapArray` with the same keys, all values negated (`v_new = -v_old`)
   - Use Arrow `MapBuilder` / `MapArray::try_new` to construct the output
3. Implement `derive_visible_from_detail_map`:
   - Iterate map entries
   - Skip entries with count <= 0
   - Reduce with min or max over remaining keys
   - Return `None` if zero non-positive-count entries remain (group is being
     retracted; the existing `__ivm_row_count == 0` retraction logic will
     remove the row)
4. Implement `prune_zero_entries_from_map`:
   - Iterate entries; keep only those with count != 0
   - Build new MapArray
5. Wire dispatch:
   - In `merge_state_value` (line 846), the `(Min|Max, Single)` arm calls
     `merge_value_count_map_state` instead of `merge_min_max_state_value`
   - In `negate_aggregate_state_chunks` (line 780), remove the panic; for
     each MIN/MAX-Single state column, call `negate_value_count_map_state` on
     its `MapArray`
   - In the apply pipeline (`iceberg_aggregate_state.rs` —
     `merge_aggregate_target_state` or whatever populates the visible chunk
     before sink-write), derive visible MIN/MAX from the merged state via
     `derive_visible_from_detail_map`
6. Decide AggScalarValue extension or bypass (see step 1 note). Recommended:
   bypass — only the merge layer needs to handle Map state, and it can
   operate directly on `MapArray` slices keyed by row index, avoiding a new
   enum variant.

**Unit tests** (`src/connector/starrocks/managed/mv_agg_state.rs`):

- `merge_value_count_map_state_empty_plus_empty`
- `merge_value_count_map_state_populated_plus_empty`
- `merge_value_count_map_state_disjoint_keys`
- `merge_value_count_map_state_overlapping_keys`
- `merge_value_count_map_state_negative_count_in_delta` (DELETE case)
- `merge_value_count_map_state_prunes_zero_counts`
- `negate_value_count_map_state_flips_values_preserves_keys`
- `derive_visible_from_detail_map_min_returns_smallest_active_value`
- `derive_visible_from_detail_map_max_returns_largest_active_value`
- `derive_visible_from_detail_map_all_zero_returns_none`
- `derive_visible_from_detail_map_with_negative_counts_skipped`
- `prune_zero_entries_from_map_removes_zero_keeps_others`
- Round-trip: insert 5 rows → DELETE 1 boundary row → final visible matches
  the corresponding plain SQL result on the same data

**Acceptance:** `cargo test --lib` covers all new helpers; `cargo clippy`
clean; `cargo build` clean.

**Commit message:** `feat(ivm): merge/negate/derive-visible for MIN/MAX detail-map state`

---

## Phase 5: DDL gate removal

**Files:**
- Modify: `src/engine/mv/iceberg_refresh.rs` line 106 (call site of
  `reject_min_max_for_iceberg_target_aggregate`)
- Delete: `src/engine/mv/iceberg_refresh.rs` lines 473-486
  (`reject_min_max_for_iceberg_target_aggregate` fn body) and its test at
  line ~9043
- Modify: any other guard that mentions "does not support MIN/MAX in
  incremental mode" — search the repo for that string

**What to do:**

1. Remove `reject_min_max_for_iceberg_target_aggregate(aggregate_shape)?;` at
   `iceberg_refresh.rs:106`
2. Delete the function definition lines 473-486
3. Delete or update its test (search `expect_err("MIN/MAX should be rejected")`)
4. Search for any other DDL-time rejection of MIN/MAX in iceberg MV path
   (`grep -rn "MIN/MAX" src/`); update each occurrence
5. Add a positive unit test: synthesized MV with MIN/MAX over Int64 input
   passes through `iceberg_refresh` analysis (or whatever entry point did
   the rejection)

**Unit tests:**

- `iceberg_refresh.rs` (or its test module): create MV with
  `SELECT region, MIN(amount), MAX(amount), COUNT(*) FROM tab GROUP BY region`
  → analysis succeeds, `AggregateMvLayout` has Map state columns
- Negative: MIN over unsupported type (Bool) still fails at layout layer
  (Phase 2 ensures this)

**Acceptance:** `cargo test --lib` green. No DDL-time MIN/MAX rejection
remains in the iceberg target path.

**Commit message:** `feat(ivm): allow MIN/MAX in iceberg target aggregate MV DDL`

---

## Phase 6: SQL regression coverage + suite verification

**Files:**
- New: `sql-tests/iceberg-ivm/sql/iceberg_ivm_aggregate_min_max_insert_only.sql`
- New: `sql-tests/iceberg-ivm/sql/iceberg_ivm_aggregate_min_max_delete_non_boundary.sql`
- New: `sql-tests/iceberg-ivm/sql/iceberg_ivm_aggregate_min_max_delete_boundary.sql`
- New: `sql-tests/iceberg-ivm/sql/iceberg_ivm_aggregate_min_max_partitioned.sql`
- New: `sql-tests/iceberg-ivm/sql/iceberg_ivm_join_aggregate_min_max.sql`
- Result goldens for each fixture (record mode after live run)

**What to do:**

For each fixture, follow the existing `iceberg-ivm` suite pattern (see
`sql-tests/iceberg-ivm/init.sql` and any existing aggregate fixture for
reference structure). Each fixture:

1. CREATE Iceberg catalog (init.sql does this at suite level — use
   `iceberg_cat_${suite_uuid0}`)
2. CREATE base Iceberg table (v3 + row-lineage required for iceberg-ivm
   suite)
3. INSERT initial rows
4. CREATE MATERIALIZED VIEW with MIN/MAX
5. REFRESH (initial)
6. SELECT from MV; assert against expected
7. INSERT delta rows; REFRESH; SELECT; assert
8. DELETE delta rows; REFRESH; SELECT; assert
9. (For `_partitioned`) Repeat 7-8 across multiple partitions
10. (For `_join_aggregate`) Two base tables joined; deltas on one side

Specific scenarios per fixture:

- **insert_only**: 5 initial rows + 3 INSERT deltas (no DELETE). Verify
  visible MIN/MAX matches plain GROUP BY query.
- **delete_non_boundary**: 5 rows, DELETE a row whose value is between min
  and max. Verify visible MIN/MAX unchanged.
- **delete_boundary**: 5 rows, DELETE the row that IS the current min.
  Verify new visible MIN matches the second-smallest value (the case StarRocks's
  `is_sync` flag accelerates).
- **partitioned**: 3 partitions × 5 rows each; DELETE boundary rows in 2 of
  3 partitions; verify only those 2 partitions' MIN/MAX changes.
- **join_aggregate**: orders ⋈ users; MIN(order_amount), MAX(order_amount)
  GROUP BY user_region. Deletes on orders side.

Recording the goldens:

```bash
# Setup
source docker/iceberg-rest/runtime/current/env.sh
docker/iceberg-rest/up.sh
target/debug/novarocks standalone-server --config "$NOVAROCKS_STANDALONE_CONFIG" &
# Wait for NOVAROCKS_READY marker

cargo run --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests -- \
  --config "$NOVAROCKS_SQL_TEST_CONFIG" \
  --suite iceberg-ivm \
  --only iceberg_ivm_aggregate_min_max_insert_only,iceberg_ivm_aggregate_min_max_delete_non_boundary,iceberg_ivm_aggregate_min_max_delete_boundary,iceberg_ivm_aggregate_min_max_partitioned,iceberg_ivm_join_aggregate_min_max \
  --mode record --record-from target
```

**Acceptance:**

- All 5 new fixtures verify-pass on a clean run.
- Full `iceberg-ivm` suite passes (35 + 5 = 40 cases, 40/40 pass).
- Full `iceberg` suite still passes (67/67).
- Full `iceberg-rest` suite still passes (9/9).
- Full `iceberg-compatibility` suite still passes (12/12).
- cargo test --lib all green; cargo clippy clean; cargo build clean.

**Commit message:**

```
test(iceberg-ivm): add MIN/MAX detail-state SQL regression coverage

Five new fixtures verifying that the Iceberg target aggregate IMV path
correctly maintains MIN/MAX via per-group value-count detail maps:

- insert_only — INSERT-only delta, no DELETE
- delete_non_boundary — DELETE a row that is not current min/max
- delete_boundary — DELETE a row that IS the current min (the headline
  case that previously required full refresh)
- partitioned — affected-partition pruning still works with MIN/MAX
- join_aggregate — two-base join aggregate IMV with MIN/MAX

Full iceberg-ivm suite: 40/40 pass.
```

---

## Cross-Cutting Concerns

### Order of PRs

The phases above are designed to be **independently mergeable** but executed
in this order. Specifically:

- Phase 1 (new aggregates) is a pure addition; merges first, no behavior change.
- Phase 2 (schema) makes layout produce Map state types but is benign because
  the merge / negate paths haven't been taught to handle Map yet — DDL is
  still rejected at Phase 5's gate, so users can't trigger the new code yet.
- Phase 3 (rewriter) modifies the SELECT rewrite but still benign for the
  same reason.
- Phase 4 (merge/negate/derive) lights up the actual code path; still gated.
- Phase 5 (DDL gate removal) is the **flag-flip** that exposes the feature.
- Phase 6 (tests) verifies the feature end-to-end.

This ordering means Phases 1-4 can land in main without risk; only Phase 5
exposes the new behavior to users. If a regression is discovered after
Phase 5, revert Phase 5 alone to restore the gate.

### TDD discipline

Per existing project convention (`CLAUDE.md`):
- Phase 1, 2, 4: TDD — write failing test, see it fail, implement, see it pass
- Phase 3: rewriter is straightforward enough that test-first works; do it
- Phase 5: minimal code change, tests included
- Phase 6: pure test coverage

### Performance benchmarking (optional, future PR)

This plan does not include a benchmark harness for MIN/MAX detail-map vs.
hypothetical baseline. The detail-map approach is strictly better than the
current "fall back to full refresh" path in all realistic cases, so a
benchmark is not gating. If a future PR wants to add `is_sync` optimization,
it should add a benchmark at that point.

### Open implementation risks

See spec §6 for unresolved design questions; resolve them during Phase 2
(schema) and Phase 4 (merge) implementation. Specifically:

- **AggScalarValue extension** vs. **bypass for Map state**: recommended
  bypass — handle Map state directly via Arrow `MapArray` slices in the
  merge layer, do not introduce `AggScalarValue::Map(...)` variant. Decide
  in Phase 4.
- **Iceberg `map<K, V>` target table column round-trip**: verify in Phase 2
  with a smoke unit test before lighting up the full path.

---

## Self-Review (per writing-plans skill)

Spec coverage check:

| Spec section | Covered by |
|---|---|
| §3.1 Schema | Phase 2 |
| §3.2 Delta SELECT rewrite | Phase 3 |
| §3.3 State merge | Phase 4 |
| §3.4 Visible-column derivation | Phase 4 |
| §3.5 Negate path | Phase 4 |
| §3.6 Group retraction (invariant) | Implicit — existing retraction logic still applies |
| §3.7 DDL gate | Phase 5 |
| §4 Worked example | Verified end-to-end by Phase 6 fixtures |
| §5 Files to touch | Cross-checked with phase Files lists |
| §6 Risks | Open implementation risks subsection |
| §7 Acceptance criteria | Mapped to Phase 6 acceptance |

Placeholder scan: phases use TBD-free language; every step has actual files
and either code or test names. Phase 4 has one design decision point flagged
explicitly (AggScalarValue extension vs. bypass) — this is intentional, the
spec calls it an open question and Phase 4 is the venue for resolving it.

Type consistency: function names match across phases:
- `map_value_count` (Phase 1) / `map_value_count` (Phase 3 rewrite)
- `map_value_count_signed` (Phase 1) / `map_value_count_signed` (Phase 3)
- `merge_value_count_map_state` (Phase 4 helper) / dispatched from
  `merge_state_value` (Phase 4 modification)
- `negate_value_count_map_state` (Phase 4)
- `derive_visible_from_detail_map` (Phase 4)
- `prune_zero_entries_from_map` (Phase 4)
- `reject_min_max_for_iceberg_target_aggregate` (Phase 5: deleted)

---

## Execution Recommendation

This plan is sized for **5-6 PRs over ~1-2 weeks** (single engineer or one
subagent per PR). Each PR is independent; mid-flight revert of any single PR
is safe. The flag-flip is concentrated in Phase 5 — until then, no user can
trigger the new path.

For the inline-execution variant (executing-plans skill), batch Phases 1-2
as one checkpoint (foundation), Phases 3-4 as a second (the actual feature),
and Phase 5-6 as a third (release + verification).
