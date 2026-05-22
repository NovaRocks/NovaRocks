# Implementation Plan — Iceberg Target Aggregate MV `BOOL_OR`/`BOOL_AND` + MIN/MAX-over-Boolean

**Date:** 2026-05-23
**Spec:** `docs/superpowers/specs/2026-05-23-iceberg-mv-bool-aggregates-design.md`
**Branch:** `claude/ivm-bool-aggregates` (from `origin/main @ e88cd2d5`, post PR #162 merge)
**Worktree:** `/Users/harbor/project/NovaRocks/.claude/worktrees/ivm-bool-aggregates`

## Phase Order

Each phase ends with `cargo build --lib` + `cargo test --lib` green before moving on.
The whole sequence ends with full 4-suite SQL verify.

### Pre-Phase A — Write plain `BOOL_AND` executor (~30 min)

**Goal:** Close the pre-condition gap. NovaRocks codegen/analyzer claim
`BOOL_AND` exists; the executor is missing.

**Files:**
- `src/exec/expr/agg/functions/bool_and.rs` — NEW. Clone of `bool_or.rs`:
  - `BoolAndAgg` struct (`BoolOrAgg` → `BoolAndAgg`)
  - `AggKind::BoolAnd` variant
  - `init_state`: `BoolState { has_value: false, value: true }` (vs `false` for OR)
  - `update_batch` / `merge_batch`: `state.value = state.value && row` (vs `||`)
  - Reuse `build_bool_array` finalize
- `src/exec/expr/agg/functions/mod.rs`:
  - `pub mod bool_and;` near `pub mod bool_or;`
  - In `resolve_by_func`: add `"bool_and" | "booland_agg" => Ok(&BOOL_AND)`
  - Re-export `BoolAndAgg`/`BOOL_AND` symbol parallel to existing `BOOL_OR`
- Other AggKind exhaustive matches: compiler will list them. Likely 2-4 sites
  (size_of_state, alignment, dispatch in update_batch_dispatch).

**Verification:**
- `cargo build --lib`
- Quick smoke: temporarily add a doc-test or unit test inside `bool_and.rs`
  asserting `BoolAndAgg::name() == "bool_and"`
- `cargo test --lib functions::bool_and` if module-level tests exist
- Sanity: full `cargo test --lib` clean

### Phase 1 — `AggregateFunctionKind` extension + classifier (~30 min)

**Goal:** IVM-side recognition of `BOOL_OR(col)` and `BOOL_AND(col)`.

**Files:**
- `src/connector/starrocks/managed/mv_shape.rs`:
  - `AggregateFunctionKind` enum at line 95-101: add `BoolOr`, `BoolAnd` variants
  - `classify_aggregate_call` at line 609-622: add cases
    ```rust
    "bool_or" | "boolor_agg" => (
        AggregateFunctionKind::BoolOr,
        classify_bool_or_and_input(&args.args)?,
    ),
    "bool_and" | "booland_agg" => (
        AggregateFunctionKind::BoolAnd,
        classify_bool_or_and_input(&args.args)?,
    ),
    ```
  - New function `classify_bool_or_and_input(args)` — single arg, must be an
    `Expr` (not `*`), pass through `reject_unsupported_expr`. Identical
    structure to `classify_min_max_input`

**Verification:**
- `cargo build --lib` — expect compile errors at exhaustive AggregateFunctionKind
  match sites; add arms as compiler complains. Phase 2 / Phase 4 will properly
  populate them, so for Phase 1 we can use `_ => unimplemented!("phase 2")`
  placeholders if the compiler insists. Prefer adding the actual layout/dispatch
  in this single pass so we don't lose track of compile-driven discovery.
- Unit test: `classify_aggregate_call` recognizes `BOOL_OR(col)` → kind=BoolOr,
  recognizes `bool_or(col)` (lowercase) same way, recognizes `boolor_agg(col)`
  same way

### Phase 2 — `AggregateMvLayout` + state column physical schema (~45 min)

**Goal:** When the IVM shape contains `BoolOr`/`BoolAnd`, the layout should
emit a `Map<Boolean, Int64>` state column (`__agg_state_<name>`).

**Files:**
- `src/connector/starrocks/managed/mv_agg_state.rs`:
  - In whatever function maps `AggregateCallShape` → `AggregateStateColumn`
    (likely `AggregateMvLayout::for_aggregate_call` or similar; locate via
    "AggregateFunctionKind::Min" usage near a Map<...> physical type
    construction):
    - Add `AggregateFunctionKind::BoolOr` and `AggregateFunctionKind::BoolAnd`
      arms producing the same `Map<inferred_key_dt, Int64>` shape used by
      MIN/MAX. The `inferred_key_dt` comes from the input column type;
      for BoolOr/BoolAnd it must be `DataType::Boolean` (reject any other
      input type via a clear error in this layout step or in classifier)
    - `state_role = AggregateStateRole::Single`
    - `visible_dt = DataType::Boolean` (nullable)

**Verification:**
- `cargo build --lib`
- Trace a synthetic AggregateMvShape with one BoolOr aggregate through layout
  and assert `state_columns[0].physical_dt == Map<Boolean, Int64>` and
  `visible_dt == Boolean`

### Phase 3 — Delta SELECT rewriter (BoolOr/BoolAnd → `map_value_count{,_signed}`) (~30 min)

**Goal:** Existing P5 rewriter that emits `map_value_count(arg)` for INSERT
delta and `map_value_count_signed(arg, __change_op)` for signed-delta path
extends to BoolOr/BoolAnd.

**Files:**
- `src/connector/starrocks/managed/ivm_delta_aggregate.rs`:
  - Line 159-184 (`AggregateFunctionKind::Min | AggregateFunctionKind::Max` arm):
    extend pattern to include `BoolOr | BoolAnd`. The body is identical —
    detail-state column produced by `map_value_count_signed(expr,
    __change_op)`. Update the comment to mention BoolOr/BoolAnd
- The corresponding INSERT-only rewriter (P5 Phase 3 also lives somewhere
  nearby — probably `mv_shape::rewrite_select_sql_for_state` or similar):
  - Same pattern extension

**Verification:**
- `cargo build --lib`
- Unit test (if there are rewriter unit tests): synthetic select
  `SELECT region, BOOL_OR(flag) FROM t GROUP BY region` rewrites to use
  `map_value_count(flag)` (INSERT path) and
  `map_value_count_signed(flag, __change_op)` (signed-delta path).
  If no unit tests, defer to integration via SQL fixture in Phase 6.

### Phase 4 — Visible derivation: new bool helpers + dispatch (~45 min)

**Goal:** `update_visible_values_from_state` (`mv_agg_state.rs:1971`)
correctly derives visible Boolean from detail map for BoolOr/BoolAnd.

**Files:**
- `src/connector/starrocks/managed/mv_agg_state.rs`:
  - New helper `derive_bool_or_from_detail_map(m: &AggScalarValue::Map)
    -> Result<Option<AggScalarValue>, String>`:
    - Scan entries; track `true_count`, `false_count`
    - If `true_count > 0` → `Some(Bool(true))`
    - Elif `false_count > 0` → `Some(Bool(false))`
    - Else → `None` (= visible NULL)
  - New helper `derive_bool_and_from_detail_map(...)`:
    - Same scan; if `false_count > 0` → `Some(Bool(false))`; elif
      `true_count > 0` → `Some(Bool(true))`; else `None`
  - `update_visible_values_from_state` at line 1971: add arms for
    `AggregateFunctionKind::BoolOr` / `BoolAnd` calling the new helpers
  - **MIN/MAX-over-Boolean**: confirm `derive_visible_from_detail_map`
    already produces the correct Bool(false) / Bool(true) via
    `compare_agg_scalar_values`. No code change needed in that helper if
    it's truly generic. (If it isn't, add a Boolean fast-path that picks
    `false` for MIN / `true` for MAX when both counts > 0.)

**Verification:**
- Add 4 unit tests (in mv_agg_state.rs `#[cfg(test)] mod tests`):
  1. `derive_bool_or_from_detail_map` covers four cases (empty / only-true /
     only-false / mix)
  2. `derive_bool_and_from_detail_map` covers four cases
  3. `derive_visible_from_detail_map` for K=Boolean MIN picks Bool(false)
     when both keys present
  4. Same for MAX picks Bool(true) when both keys present
- `cargo test --lib` should pass them

### Phase 5 — Unlock Boolean in `validate_state_column_type` (~20 min)

**Goal:** DDL gate lets `Map<Boolean, Int64>` through for
`(BoolOr | BoolAnd, Single)` and (newly) for `(Min | Max, Single)`.

**Files:**
- `src/connector/starrocks/managed/mv_agg_state.rs:1704-1801`:
  - Lines 1795-1797 (stale "Boolean not supported by AggScalarValue" reject):
    REMOVE
  - Add `DataType::Boolean => Ok(())` arm in the MIN/MAX (Single) Map-key
    type matrix
  - Add a new branch for `AggregateFunctionKind::BoolOr | BoolAnd` with
    `state_role == Single` that accepts ONLY `DataType::Boolean` map keys
    (anything else is rejected with a clear error). This is stricter than
    MIN/MAX because BOOL_OR/AND semantics over non-Boolean are undefined
- Also: remove or update the comment at line 1759 about "minus Boolean"

**Verification:**
- `cargo build --lib`
- Add 2 unit tests:
  - `validate_state_column_type` accepts `Map<Boolean, Int64>` for BoolOr/Single
  - `validate_state_column_type` rejects `Map<Int32, Int64>` for BoolOr/Single
    (because BoolOr only makes sense over Boolean)

### Phase 6 — SQL fixtures + unit tests (~1.5 hr)

**Goal:** End-to-end coverage from DDL → INSERT → REFRESH → DELETE → REFRESH
across all 3 new aggregates.

**Fixtures (3 new files under `sql-tests/iceberg-ivm/sql/`):**

1. `iceberg_ivm_aggregate_bool_or.sql` (~120 lines)
   - Header: `@sequential=true`, `@order_sensitive=true`,
     `@tags=mv,iceberg,ivm,aggregate,bool_or,detail_state`
   - DDL: catalog + db + base table (`flag BOOLEAN`) + MV
     `SELECT region, BOOL_OR(flag) AS any_true, COUNT(*) AS c FROM events GROUP BY region`
   - INSERT 5 rows: mix true / false / NULL across 2 regions
   - SELECT MV vs SELECT plain — confirm match
   - DELETE the only `true` row in one region → MV row's `any_true` flips to
     `false`; plain check matches
   - DELETE all `true` rows in the other region → `any_true` becomes `false`;
     when *all* non-null rows deleted, `any_true` becomes NULL
   - Cleanup

2. `iceberg_ivm_aggregate_bool_and.sql` (~120 lines)
   - Mirror of (1) with BOOL_AND
   - Boundary: DELETE the only `false` row in a region → `all_true` flips to
     `true`; when all rows go → NULL

3. `iceberg_ivm_aggregate_min_max_bool.sql` (~100 lines)
   - DDL: `SELECT region, MIN(flag) AS mn, MAX(flag) AS mx, COUNT(*) AS c
     FROM events GROUP BY region`
   - Covers: mix → MIN=false, MAX=true; delete last `false` → MIN flips to
     `true`; delete last `true` → MAX flips to `false`; full delete →
     MIN=MAX=NULL

**Recording:**
- Start standalone-server with `NOVAROCKS_STANDALONE_CONFIG`
- `cargo run --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests
  -- --config "$NOVAROCKS_SQL_TEST_CONFIG" --suite iceberg-ivm --mode record
  --record-from target --update-expected --only iceberg_ivm_aggregate_bool_or,
  iceberg_ivm_aggregate_bool_and,iceberg_ivm_aggregate_min_max_bool`

**Verification:**
- Re-run with `--mode verify`; expect 49/49 (was 46/46 + 3 new fixtures)

### Phase 7 — Full suite verify + commit + PR (~45 min)

**Verification:**
- `cargo fmt --all -- --check`
- `cargo clippy --lib -- -D warnings` (no new warnings)
- `cargo build --lib`
- `cargo test --lib`
- 4 iceberg SQL suites all green:
  - `iceberg-ivm` — 49/49 (was 46/46)
  - `iceberg` — 67/67
  - `iceberg-rest` — 9/9
  - `iceberg-compatibility` — 12/12

**Commit + PR:**
- Commit message: `feat(ivm): BOOL_OR / BOOL_AND / MIN-MAX-Boolean aggregate IMV via Map<Boolean, Int64> detail state`
- Per-phase commits if helpful (Pre-A as standalone "feat(agg): add BOOL_AND
  aggregate function executor" then the IVM work as a second commit) —
  cleaner for review
- Push to `origin/claude/ivm-bool-aggregates`
- Open PR; body recaps spec § 2 decision table + § 6 edge cases + § 8 success
  criteria

## Risk Log

- **Compiler-driven discovery of `AggregateFunctionKind` exhaustive match
  sites**: Estimate 5-10. Will be visible during Phase 2.
- **Iceberg field-id round-trip for Boolean map keys**: P5 bug-fix
  infrastructure is type-agnostic, but Boolean has never been exercised. If
  end-to-end fixture surfaces a metadata-mismatch, fall back to P5 Bug #2 / #3
  / #4 playbook — `reannotate_array` + `arrow_type_equals_ignoring_metadata`
  already in place. Time budget for this risk: 2 hr.
- **`bool_and` codegen speculative typing wrong**: if `expr_compiler.rs:2095`
  declared return type doesn't match new `BoolAndAgg::return_type()`, fix at
  codegen layer. Time budget: 30 min.

## Total Budget

- Pre-A: 30 min
- Phase 1: 30 min
- Phase 2: 45 min
- Phase 3: 30 min
- Phase 4: 45 min
- Phase 5: 20 min
- Phase 6: 1.5 hr (mostly fixture writing + recording)
- Phase 7: 45 min
- Risk buffer: 2 hr

**Total: ~7 hours of focused work.** Fits in a single day if executed end-to-end without major surprises.
