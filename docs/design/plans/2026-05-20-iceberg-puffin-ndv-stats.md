# Iceberg Puffin NDV Stats — Implementation Plan

- **Spec**: `docs/design/specs/2026-05-20-iceberg-puffin-ndv-stats-design.md`
- **Date**: 2026-05-20

---

## Phase 1: Foundation

### Step 1.1 — Theta Sketch Wrapper

**Files**: new `src/connector/iceberg/theta_sketch.rs`, modify `src/connector/iceberg/mod.rs`

**What to do**:
1. Create `ThetaSketchHandle` wrapping `datasketches::theta::ThetaSketch`
2. Implement `new(lg_k)`, `update<T: Hash>()`, `update_f64()`, `estimate()`
3. Implement `serialize()` → Apache DataSketches compact binary format:
   - Use `ThetaSketch::iter()` to extract retained hash values (u64)
   - Use `ThetaSketch::theta64()` for theta
   - Write header: preamble_longs, serial_version=3, family=3, flags(compact+ordered), seed_hash=0x93CC
   - Write retained_count, theta (if < u64::MAX), sorted hash values (little-endian)
4. Implement `deserialize(bytes)` → parse header, extract hashes + theta, rebuild internal state
   - For deserialization, create a new ThetaSketch with matching lg_k, insert the hash values
   - Handle edge cases: empty sketch, theta < MAX
5. Implement `union(sketches)`:
   - Collect all retained hashes from all sketches
   - Take `min(theta)` across all sketches
   - Keep only hashes < min_theta, deduplicate
   - Build result with the merged hash set and min_theta
6. Implement `union_bytes(serialized_blobs)` — deserialize then union
7. Add module to `src/connector/iceberg/mod.rs`

**Unit tests**:
- Build sketch, update 10k distinct values, estimate within 1.5% of 10k
- Serialize → deserialize round-trip preserves estimate
- Union of two disjoint sketches ≈ sum of estimates
- Union of two overlapping sketches ≈ actual distinct count
- Empty sketch serialize/deserialize
- Interop: if possible, verify against a known Java-serialized compact sketch fixture

**Acceptance**: `cargo test` passes for theta_sketch module, serialize/deserialize round-trip works.

---

### Step 1.2 — Manifest min/max Decode

**Files**: `src/sql/optimizer/statistics.rs`

**What to do**:
1. Find the `build_table_statistics()` function (around line 85-163)
2. Locate lines 140-141 where `min_value`/`max_value` are hardcoded to ±∞
3. Replace with actual decode logic based on Iceberg type:
   - Need the column's Iceberg type (from schema or IcebergColumnStats)
   - INT: 4-byte LE i32 → f64
   - LONG: 8-byte LE i64 → f64
   - FLOAT: 4-byte LE f32 → f64
   - DOUBLE: 8-byte LE f64
   - DATE: 4-byte LE i32 (days) → f64
   - TIMESTAMP/TIMESTAMPTZ: 8-byte LE i64 (microseconds) → f64
   - DECIMAL: big-endian unscaled bytes + scale → f64 (lossy OK)
   - BOOLEAN: single byte → 0.0/1.0
   - STRING/BINARY: skip (leave as ±∞)
4. Need to pass Iceberg schema info alongside IcebergColumnStats so we know the type
   - Check how IcebergColumnStats is constructed in `src/connector/iceberg/read.rs:358-394`
   - May need to add `iceberg_type: IcebergType` field to IcebergColumnStats
   - Or pass schema separately into build_table_statistics

**Also**:
- Improve NDV fallback: if `value_counts` is available, use `min(value_counts, current_heuristic)` as NDV upper bound
- Currently NDV = `sqrt(non_null) * 10` — replace with `min(value_counts, row_count)` when value_counts available

**Unit tests**:
- Decode INT bytes → correct f64
- Decode DOUBLE bytes → correct f64
- Decode TIMESTAMP bytes → correct f64
- Skip STRING → still ±∞

**Acceptance**: existing optimizer tests still pass; new test validates decode correctness.

---

## Phase 2: Write Path

### Step 2.1 — Per-file Theta Computation in Sink

**Files**: `src/connector/iceberg/sink.rs`

**What to do**:
1. Find `collect_iceberg_column_stats()` (around line 1053-1124)
2. After the existing stats collection (value_counts, null_counts, min, max), add:
   - For each primitive column (by Iceberg type), create `ThetaSketchHandle::new(lg_k)`
   - Iterate the Parquet column data, call `sketch.update(value_bytes)` for each non-null value
   - Return `HashMap<i32, ThetaSketchHandle>` alongside existing `TIcebergColumnStats`
3. Define `FileSketchSet` struct to bundle file path + column sketches
4. Propagate sketches up through the write result to the commit caller

**Note on column data access**: The existing code reads Parquet row-group metadata for stats. For Theta sketch, we need actual column VALUES, not just metadata. Two approaches:
- Read column data from the just-written Parquet file (extra I/O but simple)
- Capture values during write (intercept the arrow RecordBatch before Parquet write)
  - Prefer this approach: iterate the RecordBatch columns that were just written

**Type-to-hash mapping for Theta update**:
- INT/LONG/DATE/TIMESTAMP: use the native integer bytes
- FLOAT/DOUBLE: use IEEE 754 bytes (NaN normalization needed)
- STRING: use the string bytes directly
- BOOLEAN: use single byte 0/1
- DECIMAL: use the unscaled bytes

**Acceptance**: Writing a data file produces both column stats AND per-column Theta sketches.

---

### Step 2.2 — StatsAssembler Module

**Files**: new `src/connector/iceberg/stats_assembler.rs`

**What to do**:
1. Implement `StatsAssembler::assemble()` per the spec:
   - Input: table metadata, commit type, new file sketches, snapshot/sequence ids, file_io
   - APPEND logic:
     a. Find previous snapshot's StatisticsFile via `table_metadata.statistics_for_snapshot(prev_snapshot_id)`
     b. Read previous Puffin via `PuffinReader`
     c. For each column: deserialize previous aggregate sketch, union with new file sketches
     d. Serialize new aggregate per column
     e. Write new Puffin via `PuffinWriter` to `<table>/metadata/snap-<id>-statistics.puffin`
     f. Return `StatisticsFile` with blob metadata
   - DELETE/REWRITE logic: Return None (reuse previous Puffin; caller handles statistics entry)
   - OVERWRITE logic:
     a. Full rescan: read all live data files from the table's current manifest list
     b. For each file, read column data, compute Theta sketch
     c. Union all file sketches per column → aggregate
     d. Write new Puffin
     e. Return StatisticsFile
   - FIRST COMMIT (no previous Puffin): same as OVERWRITE path

2. The "full rescan" path:
   - Get live data files from table's manifest entries
   - For each file, download Parquet, iterate primitive columns, compute Theta sketch
   - This is O(total data) but only happens on OVERWRITE or first commit

**Acceptance**: StatsAssembler produces correct Puffin for APPEND and OVERWRITE paths.

---

### Step 2.3 — Commit Hook Integration

**Files**: `src/connector/iceberg/commit/fast_append.rs`, `overwrite.rs`, `row_delta.rs`, `row_delta_dv.rs`, `update_cow.rs`, `rewrite_data_files.rs`, `overwrite_partitions.rs`, `action.rs`

**What to do**:
1. Extend `CommitCtx` (or create a new parameter) to carry per-file Theta sketches from sink
2. In each commit action's `commit()` method:
   a. Determine CommitType from the action type
   b. Call `StatsAssembler::assemble()`
   c. If result is `Some(stats_file)`:
      - Upload Puffin to object store
      - Add `UpdateStatisticsAction::new().set_statistics(stats_file)` to the transaction
   d. If result is `None`:
      - Carry forward previous statistics entry for new snapshot
3. Also update `CommitOutcome` if needed to report statistics status

**Key mapping** (action → CommitType):
| Action | CommitType |
|---|---|
| `fast_append` | Append |
| `overwrite` | Overwrite |
| `overwrite_partitions` | Overwrite |
| `row_delta` | Delete |
| `row_delta_dv` | Delete |
| `update_cow` | Overwrite |
| `rewrite_data_files` | Rewrite |
| `truncate` | Overwrite |

**Acceptance**: After INSERT, the metadata.json has a statistics entry pointing to a valid Puffin.

---

## Phase 3: Read Path + Optimizer

### Step 3.1 — StatsLoader Module

**Files**: new `src/connector/iceberg/stats_loader.rs`

**What to do**:
1. Implement `StatsLoader::load_ndv()` per the spec:
   - Look up StatisticsFile for snapshot_id
   - Download Puffin via PuffinReader
   - Filter blobs by type = `apache-datasketches-theta-v1`
   - Deserialize each, estimate(), map field_id → ndv
   - Return HashMap
2. Handle edge cases:
   - No statistics entry → return empty map
   - Puffin download fails → log warning, return empty map
   - Blob deserialization fails → log warning, skip that column

**Acceptance**: Given a table with statistics, `load_ndv()` returns correct NDV per column.

---

### Step 3.2 — NDV Injection into Optimizer Stats

**Files**: `src/sql/optimizer/statistics.rs`, `src/engine/mod.rs`

**What to do**:
1. In `build_table_statistics()` or its caller:
   - Before building ColumnStatistic entries, call `StatsLoader::load_ndv()` with table metadata + snapshot
   - Need to pass table metadata / file_io into the stats building path
   - Currently `build_table_statistics()` takes `&[S3FileInfo]` — may need to widen the interface
2. When building each `ColumnStatistic`:
   - Check if ndv_map has this column's field_id
   - If yes: `distinct_values_count = ndv`
   - If no: use value_counts (from Step 1.2) or heuristic
3. Integration point in `src/engine/mod.rs`:
   - `collect_scan_stats()` (around line 2569-2580) calls `build_table_statistics`
   - Need to pass Iceberg table metadata reference for StatsLoader access
   - May need to make the call async (StatsLoader downloads Puffin)

**Acceptance**: EXPLAIN shows real NDV for Iceberg tables with statistics.

---

### Step 3.3 — Join Cost Model Update

**Files**: `src/sql/optimizer/cost.rs`

**What to do**:
1. Find the join cost / cardinality estimation logic (around line 175-192)
2. When estimating join output cardinality:
   - Check if both join key columns have `distinct_values_count > 0` from ColumnStatistic
   - If yes: `card = left_rows * right_rows / max(ndv_left, ndv_right)`
   - Clamp result: `max(1, min(card, left_rows * right_rows))`
   - If no: fall back to existing logic
3. For multi-key equi-join: product of per-key selectivities
4. Add stats derivation for GROUP BY:
   - If `derive_agg` computes GROUP BY cardinality, use `ndv(group_key)` when available

**Acceptance**: Join cost model uses NDV when available; existing optimizer tests still pass.

---

## Phase 4: Integration & Verification

### Step 4.1 — End-to-End SQL Tests

**Files**: new `sql-tests/iceberg/sql/iceberg_statistics*.sql`

**What to do**:
1. Create test cases:
   - INSERT → verify stats exist (via metadata table or EXPLAIN)
   - INSERT + INSERT (append) → verify NDV updates incrementally
   - INSERT + DELETE → verify NDV preserved
   - INSERT OVERWRITE → verify NDV recomputed
   - Multi-column table → verify per-column NDV
2. Plan-shape tests:
   - Table with known NDV → EXPLAIN shows expected cardinality at join nodes
   - Compare with/without `SET disable_optimizer_rules` to verify stats impact

### Step 4.2 — Build Verification

1. `cargo fmt`
2. `cargo clippy`
3. `cargo build`
4. `cargo test`
5. Run relevant SQL suites: `iceberg`, `iceberg-rest`

**Acceptance**: All build checks pass, no regressions in existing SQL suites.

---

## Agent Dispatch Strategy

### Round 1 (sequential in current worktree):
**Agent 1: Foundation + Write Path** (Steps 1.1, 1.2, 2.1, 2.2)
- Creates theta_sketch.rs, stats_assembler.rs
- Modifies statistics.rs (manifest decode), sink.rs (per-file Theta)
- Runs cargo build + cargo test

### Round 2 (sequential, continues from Agent 1):
**Agent 2: Read Path + Optimizer + Commit Hooks + Tests** (Steps 2.3, 3.1, 3.2, 3.3, 4.1, 4.2)
- Creates stats_loader.rs
- Modifies commit actions, cost.rs, engine/mod.rs
- Creates SQL test files
- Runs full verification
