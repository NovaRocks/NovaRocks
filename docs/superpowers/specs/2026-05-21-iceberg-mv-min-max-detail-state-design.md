# Iceberg Target Aggregate MV `MIN/MAX` via Value-Count Detail State

**Status:** Draft for user review
**Date:** 2026-05-21
**Builds on:**
- `docs/superpowers/specs/2026-04-26-mv-on-iceberg-aggregate-ivm-design.md`
- `docs/superpowers/specs/2026-04-30-aggregate-mv-avg-min-max-design.md` (predecessor: scalar MIN/MAX state with full-refresh fallback)
- `docs/superpowers/specs/2026-05-20-ivm-p1-partition-pruned-touched-group-lookup-design.md` (PR #145 partition-pruned apply path)

**Related Obsidian roadmap:**
- `IVM-P5-aggregate-mv-min-max-detail-state.md` (renamed from `IVM-P5-aggregate-partition-rebuild-min-max.md`)

**StarRocks cross-reference (verified by live test on `apache/spark-iceberg:3.5.5_1.11.0` + StarRocks FE on NovaRocks BE, 2026-05-21):**
- IVM analyzer: `fe/fe-core/src/main/java/com/starrocks/sql/analyzer/mv/IVMAnalyzer.java:82-115` — IVM whitelist includes MIN/MAX for numeric/temporal/string
- BE retract path: `be/src/exprs/agg/stream/retract_maxmin.h:35-243` — `MaxAggregateDataRetractable` / `MinAggregateDataRetractable` with `StreamDetailState<LT>`
- Detail state structure: `be/src/exprs/agg/stream/stream_detail_state.h` — `flat_hash_map<value, int64_t>` per group

---

## 1. Goal

Allow Iceberg target aggregate / join aggregate IMV (created via NovaRocks
`CREATE MATERIALIZED VIEW`) to contain `MIN(col)` / `MAX(col)` in the SELECT
projection, **and refresh incrementally on both INSERT and DELETE deltas**
without falling back to full refresh or rejecting the DDL.

### Scope

- Iceberg target aggregate MV (`IncrementalMvShape::Aggregate`)
- Iceberg target join aggregate MV (`IncrementalMvShape::JoinAggregate`)
- `MIN/MAX` over the AggScalarValue-supported primitive types: `Int8` / `Int16` /
  `Int32` / `Int64` / `Float32` / `Float64` / `Decimal128` / `Decimal256` /
  `Utf8` / `Date32` / `Timestamp`
- Both single-base and two-base join aggregate paths

### Non-Goals

- managed-lake target aggregate MV (separate path, may follow later)
- Iceberg target projection/filter MV (no aggregate, not relevant)
- `MIN/MAX` over `Bool` (already rejected by AggScalarValue)
- `MIN/MAX` over composite types (arrays, structs, maps) — not in
  `AggScalarValue`
- Performance optimization via `is_sync` cache flag — explicitly deferred to a
  follow-up; first version recomputes visible value from detail every refresh
- Partition rebuild as a refresh strategy — superseded by this proposal
- Schema migration for existing IMVs — NovaRocks has no historical users
  (memory note `feedback_no_backwards_compat_for_novarocks`); DDL of MV is the
  schema change boundary

---

## 2. Decision Summary

| Dimension | Decision |
|---|---|
| State representation | Per-group `Map<value, Int64>` (Arrow `DataType::Map`) where Int64 is the row count of that value in the group |
| Detail-map ownership | Detail map IS the state. `__agg_state_<col>` column holds the map, no separate scalar `result`. Visible `<col>` is derived from the map at write time. |
| INSERT delta | `detail[value] += 1` per row; map-merge with `+addition` semantics |
| DELETE delta | `detail[value] -= 1` per row; same map-merge path (count goes negative; entries with `count == 0` are pruned at write time, never pruned mid-merge) |
| `is_sync` optimization | **Not included** in v1; visible MIN/MAX is recomputed by iterating the detail map at every merge step. Future PR can add it. |
| DDL gate | Allow MIN/MAX once `IcebergMvBackend::supports_min_max_detail_state()` is true. Remove `reject_min_max_for_iceberg_target_aggregate`. |
| Storage type per row | `Map<input_type, Int64>` Arrow array → Iceberg `map<K, V>` field. Iceberg target table column type follows Iceberg spec mapping. |
| Empty group lifecycle | Group is deleted (row removed from MV) when all detail counts == 0 — same retraction logic as today (`__ivm_row_count == 0` removal). |
| Backwards compat | None — DDL is the schema boundary. Pre-existing MVs without MIN/MAX continue to use scalar state columns; once user adds MIN/MAX to a new MV, the new MV uses detail-map state. |

---

## 3. Architecture: What Changes

NovaRocks's aggregate IMV pipeline already has:

1. `AggregateMvLayout` with `state_columns` per aggregate (`mv_agg_state.rs:28-76`)
2. Delta SELECT rewriter (`mv_shape::rewrite_select_sql_for_state`,
   `ivm_delta_aggregate::rewrite_select_sql_for_signed_delta_state`)
3. State merge / negate / apply path
   (`mv_agg_state::merge_aggregate_state_batches`, `negate_aggregate_state_chunks`)
4. PRIMARY KEY MV target with `__row_id__` + visible cols + hidden state cols

The only delta vs. SUM/COUNT/AVG is **what's stored in `__agg_state_<name>` for
MIN/MAX**. Every code path that fans out by `(function, state_role)` needs a
new `Single + Min/Max` branch that goes through the Map type.

### 3.1 Schema (`mv_agg_state.rs:147-177`)

For each `MIN(col)` / `MAX(col)` in the MV SELECT, build a state column with:

```rust
AggregateStateColumn {
    name:        "__agg_state_<sanitized_output>",  // unchanged naming
    data_type:   DataType::Map(
                     Arc::new(Field::new("entries",
                         DataType::Struct(Fields::from(vec![
                             Field::new("key",   <input_dt>,    /* nullable= */ false),
                             Field::new("value", DataType::Int64, /* nullable= */ false),
                         ])),
                         /* nullable= */ false)),
                     /* keys_sorted= */ false),
    sql_type:    SqlType::Map(Box::new(<input_sql_type>), Box::new(SqlType::BigInt)),
    nullable:    false,                              // empty map is valid; never NULL
    visible_source_index: <index in shape.visible_outputs>,
    aggregate_index: <agg index>,
    function:    AggregateFunctionKind::Min | Max,
    state_role:  AggregateStateRole::Single,
    count_star:  false,
}
```

`<input_dt>` is the Arrow data type of `MIN/MAX`'s argument column (resolved
via the analyzer). `validate_state_column_type` (`mv_agg_state.rs:?`) must
accept `Map<K, Int64>` for MIN/MAX-Single.

**No change** to `AggregateStateRole` enum: `Single` still suffices. The
distinction is at the *physical type* of the state column, not at the role
level.

### 3.2 Delta SELECT Rewrite

Today: `MIN(col)` in delta SELECT → either the scalar `MIN(col)` (insert path,
`mv_shape::rewrite_select_sql_for_state`) or rejected (signed-delta path,
`ivm_delta_aggregate.rs:49`).

New: `MIN(col)` / `MAX(col)` → `map_value_count(col)` aggregate that produces
`Map<col_type, Int64>` per group. The map's keys are the distinct non-null
values, the values are the per-group occurrence counts (including 1 for an
INSERT row, will be flipped to -1 for DELETE delta during negate).

```sql
-- Original MV SELECT:
SELECT region, MIN(amount), MAX(amount), COUNT(*) FROM orders GROUP BY region;

-- Insert-path delta state SELECT (rewritten by mv_shape::rewrite_select_sql_for_state):
SELECT
  encode_row_id(region)         AS __row_id__,
  region,
  MIN(amount)                   AS <visible_mn>,        -- computed once per delta
  MAX(amount)                   AS <visible_mx>,
  COUNT(*)                      AS <visible_cnt>,
  map_value_count(amount)       AS __agg_state_mn,      -- detail map
  map_value_count(amount)       AS __agg_state_mx,      -- shared computation, may dedup
  SUM(1)                        AS __agg_state_cnt,
  SUM(1)                        AS __agg_state___ivm_row_count
FROM <delta_source>
GROUP BY region;

-- Signed-delta path (rewritten by ivm_delta_aggregate when base has deletes):
-- map_value_count(amount * __change_op) — count is signed by the change_op flag
-- where __change_op = +1 for insert, -1 for delete.
-- Concretely: argument becomes (amount, __change_op) and the aggregate accumulates
--             detail[amount] += __change_op
```

A new aggregate function `map_value_count(col)` returns `Map<col_type, Int64>`
where `result[value] = count(rows in group with col == value)`. For the
signed-delta variant, `map_value_count_signed(col, change_op)` returns
`result[value] = sum(change_op for rows with col == value)`.

This is the **only new aggregate function** introduced by this work. Everything
else (SUM/COUNT/AVG) stays put.

### 3.3 State Merge (`mv_agg_state::merge_state_value`)

Current `merge_min_max_state_value` does `min(a, b)` / `max(a, b)` on scalars.
Replace with `merge_value_count_map_state`:

```rust
// New helper, replaces merge_min_max_state_value
fn merge_value_count_map_state(
    old:           Option<AggScalarValue>,  // Map<K, Int64>
    delta:         Option<AggScalarValue>,  // Map<K, Int64>
    state_column:  &AggregateStateColumn,
) -> Result<Option<AggScalarValue>, String> {
    // Pseudocode — actual impl uses Arrow MapArray builders:
    //   For each key k in (old.keys ∪ delta.keys):
    //     merged[k] = old.get(k).unwrap_or(0) + delta.get(k).unwrap_or(0)
    //   Prune entries with merged[k] == 0 (keeps map size bounded).
    //   Return Some(Map(merged))
}
```

Merge dispatch (`merge_state_value`, `mv_agg_state.rs:846-878`):
```rust
match (state_column.function, state_column.state_role) {
    // ... existing branches ...
    (AggregateFunctionKind::Min, AggregateStateRole::Single)
    | (AggregateFunctionKind::Max, AggregateStateRole::Single) => {
        merge_value_count_map_state(old, delta, state_column)
    }
    // ...
}
```

### 3.4 Visible-Column Derivation

Today: visible column is computed in the delta SELECT (e.g. `MIN(amount) AS
<visible_mn>`) and merged with the old visible via `min_max_pair`
(`mv_agg_state.rs:972-992`).

New: after merging the **detail map** state, derive the visible MIN/MAX from
the merged map:

```rust
fn derive_visible_from_detail_map(
    map:  &MapArray,
    op:   MinMax,
) -> Result<Option<AggScalarValue>, String> {
    // Iterate map entries; skip count <= 0; reduce with MIN or MAX.
    // Return None if all counts <= 0 (group is being retracted).
}
```

The visible-column branch in the apply pipeline must call this helper for
MIN/MAX-Single before writing the row back to the MV target.

### 3.5 Negate Path (`mv_agg_state::negate_aggregate_state_chunks`)

Today: panics if it sees a Min/Max state column (line 799-803).

New: for Min/Max-Single state column (which is now Arrow `Map`), negate by
flipping the **count** field inside each map entry, not by `arrow::neg` on the
whole array:

```rust
// For Map state columns: build a new MapArray where every (k, v) entry has
// v_new = -v_old. Arrow Map → unwrap keys + values → negate values → rebuild.
```

The negate fan-out per state column type:
- COUNT/SUM/AVG Single — `arrow::compute::kernels::numeric::neg` (existing)
- Min/Max Single (Map<K, Int64>) — map-entries value negate (new helper)

### 3.6 Group Retraction (Empty-Detail Detection)

Today: a group is removed from the MV when `__agg_state___ivm_row_count == 0`
(or `COUNT(*)` state == 0).

With detail-map state, an alternative signal is "all entries in
`__agg_state_<min>` map have count ≤ 0". But the existing `__ivm_row_count`
mechanism is unchanged and remains the canonical retraction signal — the
detail map's emptiness must agree with `__ivm_row_count == 0` by construction,
because INSERT and DELETE deltas affect both atomically.

**Invariant** (maintained by construction):
```
sum(map[k] for k in __agg_state_<col>) == __agg_state_<count_or_ivm_row_count>
```

### 3.7 DDL Gate

`reject_min_max_for_iceberg_target_aggregate` (`iceberg_refresh.rs:473-486`)
is removed. The MIN/MAX whitelist check moves into
`build_aggregate_mv_layout` (`mv_agg_state.rs:147`), which validates that
input type is one of the supported scalar types.

---

## 4. Worked Example

Same example as the discussion: `MIN(amount), MAX(amount), COUNT(*) GROUP BY region`.

### 4.1 Initial INSERT (5 rows)

Delta INSERT into base:
```
(1, 'SF', 10), (2, 'SF', 30), (3, 'SF', 20), (4, 'NY', 100), (5, 'NY', 200)
```

Delta SELECT (rewritten by `rewrite_select_sql_for_state`):
```
SELECT
  encode_row_id(region) AS __row_id__,
  region,
  MIN(amount)           AS mn,
  MAX(amount)           AS mx,
  COUNT(*)              AS cnt,
  map_value_count(amount) AS __agg_state_mn,
  map_value_count(amount) AS __agg_state_mx,
  SUM(1)                AS __agg_state_cnt,
  SUM(1)                AS __agg_state___ivm_row_count
FROM base GROUP BY region;
```

Delta result chunk (per group):

| __row_id__ | region | mn | mx | cnt | __agg_state_mn | __agg_state_mx | __agg_state_cnt | __ivm_row_count |
|---|---|---|---|---|---|---|---|---|
| h('SF') | SF | 10 | 30 | 3 | `{10:1, 20:1, 30:1}` | `{10:1, 20:1, 30:1}` | 3 | 3 |
| h('NY') | NY | 100 | 200 | 2 | `{100:1, 200:1}` | `{100:1, 200:1}` | 2 | 2 |

MV is empty → merge with empty old state → write delta as is. MV table contains
these two rows.

### 4.2 Second INSERT: `(6, 'SF', 25)`

Delta SELECT (single row):

| __row_id__ | region | mn | mx | cnt | __agg_state_mn | __agg_state_mx | __agg_state_cnt | __ivm_row_count |
|---|---|---|---|---|---|---|---|---|
| h('SF') | SF | 25 | 25 | 1 | `{25:1}` | `{25:1}` | 1 | 1 |

Merge against MV's existing 'SF' row:
```
__agg_state_mn merged = {10:1, 20:1, 25:1, 30:1}
__agg_state_mx merged = {10:1, 20:1, 25:1, 30:1}
__agg_state_cnt merged = 3 + 1 = 4
__ivm_row_count merged = 3 + 1 = 4
```

Derive visible:
```
mn = min(k for k,v in __agg_state_mn if v > 0) = 10
mx = max(k for k,v in __agg_state_mx if v > 0) = 30
cnt = 4
```

MV 'SF' row after merge:
| region | mn | mx | cnt | __agg_state_mn | __agg_state_mx | __agg_state_cnt | __ivm_row_count |
|---|---|---|---|---|---|---|---|
| SF | 10 | 30 | 4 | `{10:1, 20:1, 25:1, 30:1}` | `{10:1, 20:1, 25:1, 30:1}` | 4 | 4 |

### 4.3 DELETE: `WHERE id = 3` (deletes ('SF', 20))

Signed-delta SELECT (rewritten by `rewrite_select_sql_for_signed_delta_state`):
```
SELECT
  encode_row_id(region)                  AS __row_id__,
  region,
  -- visible cols are derived post-merge; delta visible can use SIGN(...) etc.
  MIN(amount)                            AS mn,    -- only over inserts in this delta
  MAX(amount)                            AS mx,    -- only over inserts
  SUM(__change_op)                       AS cnt,
  map_value_count_signed(amount, __change_op) AS __agg_state_mn,
  map_value_count_signed(amount, __change_op) AS __agg_state_mx,
  SUM(__change_op)                       AS __agg_state_cnt,
  SUM(__change_op)                       AS __agg_state___ivm_row_count
FROM <delta_source>
GROUP BY region;
```

The delta has one row: `(3, 'SF', 20)` with `__change_op = -1`.

Delta result:

| region | __agg_state_mn | __agg_state_mx | __agg_state_cnt | __ivm_row_count |
|---|---|---|---|---|
| SF | `{20:-1}` | `{20:-1}` | -1 | -1 |

Visible mn/mx in delta SELECT are computed only over the delta's positive-count
rows (insert side) — for a pure delete delta this is empty, so the delta's
visible mn/mx is NULL. After merge they will be re-derived from the merged
state anyway.

Merge against MV 'SF' row:
```
__agg_state_mn = {10:1, 20:0, 25:1, 30:1}   // 20:1 + 20:-1 = 0
__agg_state_mx = {10:1, 20:0, 25:1, 30:1}
__agg_state_cnt = 4 + (-1) = 3
__ivm_row_count = 4 + (-1) = 3
```

After merge, prune entries with count == 0:
```
__agg_state_mn = {10:1, 25:1, 30:1}
__agg_state_mx = {10:1, 25:1, 30:1}
```

Derive visible:
```
mn = min(10, 25, 30) = 10
mx = max(10, 25, 30) = 30
cnt = 3
```

MV 'SF' row updated:

| region | mn | mx | cnt | __agg_state_mn | __agg_state_mx | __agg_state_cnt | __ivm_row_count |
|---|---|---|---|---|---|---|---|
| SF | 10 | 30 | 3 | `{10:1, 25:1, 30:1}` | `{10:1, 25:1, 30:1}` | 3 | 3 |

### 4.4 DELETE: `WHERE id = 1` (deletes ('SF', 10), which IS the current min)

Delta:

| region | __agg_state_mn | __agg_state_mx | __agg_state_cnt | __ivm_row_count |
|---|---|---|---|---|
| SF | `{10:-1}` | `{10:-1}` | -1 | -1 |

Merge:
```
__agg_state_mn = {10:0, 25:1, 30:1}
__agg_state_cnt = 2
__ivm_row_count = 2
```

Prune zeros:
```
__agg_state_mn = {25:1, 30:1}
__agg_state_mx = {25:1, 30:1}
```

Derive visible (this is the case StarRocks's `is_sync` flag accelerates,
NovaRocks v1 just iterates):
```
mn = min(25, 30) = 25       // changed!
mx = max(25, 30) = 30
cnt = 2
```

MV 'SF' row final:

| region | mn | mx | cnt | __agg_state_mn | __agg_state_mx | __agg_state_cnt | __ivm_row_count |
|---|---|---|---|---|---|---|---|
| SF | 25 | 30 | 2 | `{25:1, 30:1}` | `{25:1, 30:1}` | 2 | 2 |

No base table re-scan. All correctness comes from the detail map.

### 4.5 Last DELETE that empties the group

If we deleted the remaining 'SF' rows in two more deltas, `__ivm_row_count`
would hit 0 and the existing retraction logic removes the row from the MV
target table. The detail map being empty is consistent with that.

---

## 5. Files To Touch

### 5.1 New code

| File | Responsibility |
|---|---|
| `src/connector/starrocks/managed/mv_agg_state.rs` (new helpers) | `merge_value_count_map_state`, `negate_value_count_map_state`, `derive_visible_from_detail_map`, type validation for Map state |
| `src/connector/starrocks/managed/ivm_delta_aggregate.rs` | Replace MIN/MAX rejection with `map_value_count_signed` projection rewrite |
| `src/connector/starrocks/managed/mv_shape.rs` | Replace MIN/MAX rejection (if any) with `map_value_count` projection rewrite for insert path |
| `src/sql/...` (aggregate function registry) | Register new aggregates `map_value_count(col)` → `Map<K, Int64>` and `map_value_count_signed(col, change_op)` → `Map<K, Int64>` |
| `src/exec/operators/aggregate/...` (operator side) | Implement the two new aggregates' BE-side accumulator (hash-map per group, ser/deser to Arrow MapArray) |

### 5.2 Modified call sites

| File:line | Change |
|---|---|
| `src/engine/mv/iceberg_refresh.rs:106` | Remove call to `reject_min_max_for_iceberg_target_aggregate` |
| `src/engine/mv/iceberg_refresh.rs:473-486` | Delete `reject_min_max_for_iceberg_target_aggregate` fn |
| `src/connector/starrocks/managed/mv_agg_state.rs:147-177` | Branch `Min/Max` to use `Map<K, Int64>` state type |
| `src/connector/starrocks/managed/mv_agg_state.rs:794-804` | Remove panic; route Min/Max-Single Map state to `negate_value_count_map_state` |
| `src/connector/starrocks/managed/mv_agg_state.rs:867-872` | Replace `merge_min_max_state_value` dispatch with `merge_value_count_map_state` |
| `src/connector/starrocks/managed/mv_shape.rs` (rewrite_select_sql_for_state) | Emit `map_value_count(arg)` instead of `MIN/MAX(arg)` for state column |
| `src/connector/starrocks/managed/ivm_delta_aggregate.rs:49-51` | Remove signed-delta MIN/MAX rejection |
| `src/connector/starrocks/managed/ivm_delta_aggregate.rs:170-174` | Replace `unreachable!` with `map_value_count_signed` projection |
| Anywhere that introspects state columns and assumes scalar type (search `state_column.data_type` for narrowing) | Add Map branch or generic-up if needed |

### 5.3 Tests

| File | Coverage |
|---|---|
| `mv_agg_state.rs` (unit) | `merge_value_count_map_state` — empty + empty, populated + empty, populated + populated, count-zero pruning, type mismatch error |
| `mv_agg_state.rs` (unit) | `negate_value_count_map_state` — value counts flipped, keys preserved |
| `mv_agg_state.rs` (unit) | `derive_visible_from_detail_map` — Min/Max over non-zero entries, all-zero returns None |
| `ivm_delta_aggregate.rs` (unit) | signed-delta rewrite turns MIN/MAX into `map_value_count_signed` |
| `mv_shape.rs` (unit) | insert-path rewrite turns MIN/MAX into `map_value_count` |
| `sql-tests/iceberg-ivm/sql/iceberg_ivm_aggregate_min_max_insert_only.sql` | INSERT-only INCREMENTAL refresh with MIN/MAX |
| `sql-tests/iceberg-ivm/sql/iceberg_ivm_aggregate_min_max_delete_non_boundary.sql` | DELETE a row that is NOT the current min/max |
| `sql-tests/iceberg-ivm/sql/iceberg_ivm_aggregate_min_max_delete_boundary.sql` | DELETE a row that IS the current min/max (the headline case) |
| `sql-tests/iceberg-ivm/sql/iceberg_ivm_aggregate_min_max_partitioned.sql` | Partitioned MV with MIN/MAX over multiple partitions, deltas hit some partitions |
| `sql-tests/iceberg-ivm/sql/iceberg_ivm_join_aggregate_min_max.sql` | Two-base join aggregate IMV with MIN/MAX |

---

## 6. Open Design Questions / Risks

### 6.1 Map state per row size

For high-cardinality columns (distinct values per group ≈ group size), the
detail map is essentially a copy of the group's input column. This is a
**storage trade**: we trade space for incremental refresh of MIN/MAX, vs. the
alternative of full refresh on every DELETE.

For typical analytical workloads (e.g. `MIN(amount) GROUP BY region`),
`distinct(amount) << count(*)` per region, so the map is small. The risk is
worst-case workloads where the user puts a high-cardinality column under
MIN/MAX — those should be discouraged but not blocked.

### 6.2 Iceberg Map column type encoding

Iceberg has native `map<K, V>` field type. NovaRocks Iceberg target sink already
writes Maps for the catalog backend's general-purpose use. **Risk:** confirm
that the Iceberg target sink fully round-trips `Map<input_type, Int64>` for
all `input_type` we support (especially Decimal128 keys, Utf8 keys with NULL
behavior — though MIN/MAX state map keys are never NULL by construction).

**Mitigation:** in PR 1 (schema), add a unit test that builds a small Map
MapArray and writes/reads it through the Iceberg target's `IcebergMergeSink` +
target read path. Catch round-trip issues early.

### 6.3 Pruning timing

When does `count == 0` entries get pruned from the map?
- Option A: at every merge step (eager pruning)
- Option B: lazily at visible derivation only
- Option C: at write-to-target time

**Decision:** Option A (eager pruning) — keeps map size minimal and avoids
ambiguity. The cost is one O(n) pass per merge but that's already required
for derive_visible.

### 6.4 NULL semantics for MIN/MAX argument

Today's NovaRocks MIN/MAX state ignores NULL inputs (SQL standard). With
detail map, NULL inputs must not become a map key. The aggregate
`map_value_count(col)` skips rows where `col IS NULL`. The visible MIN/MAX is
NULL only when the detail map is empty (i.e. all rows in group have NULL col,
or group has zero rows post-retraction).

### 6.5 Concurrent refresh

Detail-map state is no different from scalar state in terms of refresh
locking — the existing staging-branch atomic commit (A7) covers the whole MV
target write atomically. No new concurrency risk.

### 6.6 Performance compared to full refresh

For DELETE-bearing refresh, today's path is full refresh of the whole MV. The
detail-map approach is O(touched groups × map size + delta size). For:
- few groups, small maps → detail-map is orders of magnitude faster
- many groups, large maps → detail-map ≈ map serialization cost (still
  cheaper than reading the entire base table)
- pathological huge maps → similar order to full refresh

This is a strict improvement over the current behavior (which fall-backs to
full refresh) in essentially all realistic cases.

---

## 7. Acceptance Criteria

1. `CREATE MATERIALIZED VIEW ... AS SELECT region, MIN(x), MAX(x), COUNT(*) FROM iceberg_table GROUP BY region` succeeds without `does not support MIN/MAX in incremental mode` error.
2. INSERT-only refresh on such MV produces visible MIN/MAX correct results.
3. DELETE-bearing refresh on such MV produces visible MIN/MAX correct results (including the "delete the current min/max" case).
4. The MV target Iceberg table contains a hidden `__agg_state_<col>` column of type `Map<K, Int64>` for each MIN/MAX aggregate.
5. Existing `sql-tests/iceberg-ivm` suite (35/35) still passes.
6. `sql-tests/iceberg-ivm` adds at least 4 new MIN/MAX cases (insert-only, delete-non-boundary, delete-boundary, partitioned).
7. cargo test --lib green; cargo clippy clean; cargo build clean.

---

## 8. Out of Scope (Explicitly)

- `is_sync` cache flag for skipping detail-map iteration on non-boundary
  deltas (future PR; we always recompute visible from detail in v1).
- Per-group sub-state externalization (StarRocks's "detail state table"
  side-car) — v1 keeps detail map inline in the same MV row, which is simpler
  and avoids a second target write.
- `MIN/MAX` over array / struct / map types — not supported by AggScalarValue
  and not part of this work.
- `MIN/MAX` over Bool — explicitly out per the predecessor spec
  (2026-04-30-aggregate-mv-avg-min-max-design.md).
- Managed-lake target aggregate MV — separate code path, not touched.
- Optimizer cost model awareness of detail-map state — not relevant; refresh
  scheduling stays manual.

---

## 9. Open Questions for User Review

1. **Map state physical layout**: Arrow `Map<K, Int64>` vs. binary-blob VARBINARY with custom serde — design picks Arrow Map for type safety and Iceberg native compatibility. OK?
2. **Pruning timing** (§6.3): eager pruning at every merge step. OK?
3. **NULL semantics** (§6.4): `MIN(NULL) = NULL` if entire group is NULL, otherwise ignore NULLs. Standard SQL semantics. OK?
4. **First-version transformation coverage**: identity / year / month / day / hour / bucket / truncate all work for MIN/MAX since the transform decision is at the partition-pruning layer (PR #145) — orthogonal to this work. No transform-specific limitations.
