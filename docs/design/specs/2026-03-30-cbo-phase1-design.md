# CBO Phase 1: Statistics, Cardinality Estimation, and DP Join Reorder

**Date:** 2026-03-30
**Goal:** Replace the rule-based join reorder with a cost-based optimizer (CBO) backed by Iceberg column-level statistics, cardinality propagation, and a three-dimensional cost model.

## Context

NovaRocks standalone currently has a rule-based optimizer with three passes: predicate pushdown, heuristic join reorder (by byte size), and column pruning. The join reorder uses `row_count` from Iceberg metadata (added in this sprint) but has no column-level statistics, no cardinality propagation, and no cost model.

StarRocks FE has a full CBO with Statistics/ColumnStatistic objects, a CPU+Memory+Network cost model, and DP/Greedy join reorder algorithms. This design aligns with StarRocks FE's architecture for the standalone path.

## 1. Statistics Data Structures

### 1.1 New file: `src/sql/statistics.rs`

```rust
/// Per-column statistics, aligned with StarRocks ColumnStatistic.
pub struct ColumnStatistic {
    pub min_value: f64,
    pub max_value: f64,
    pub nulls_fraction: f64,        // [0.0, 1.0]
    pub average_row_size: f64,      // bytes per non-null value
    pub distinct_values_count: f64, // NDV
}

/// Operator-level statistics, propagated through the plan tree.
pub struct Statistics {
    pub output_row_count: f64,
    pub column_statistics: HashMap<String, ColumnStatistic>,
}

/// Three-dimensional cost estimate, aligned with StarRocks CostEstimate.
pub struct CostEstimate {
    pub cpu_cost: f64,
    pub memory_cost: f64,
    pub network_cost: f64,  // reserved for future distributed execution
}

/// Aggregated table-level statistics built from Iceberg file metadata.
pub struct TableStatistics {
    pub row_count: u64,
    pub column_stats: HashMap<String, ColumnStatistic>,
}
```

`CostEstimate::total_cost()` = `cpu * 0.5 + memory * 2.0 + network * 1.5` (StarRocks coefficients).

### 1.2 S3FileInfo extension: `src/sql/catalog.rs`

```rust
pub struct S3FileInfo {
    pub path: String,
    pub size: i64,
    pub row_count: Option<i64>,
    pub column_stats: Option<HashMap<String, IcebergColumnStats>>,
}

/// Raw per-column stats from Iceberg manifest DataFile entries.
pub struct IcebergColumnStats {
    pub null_count: Option<i64>,
    pub column_size: Option<i64>,
    pub lower_bound: Option<Vec<u8>>,  // Iceberg binary-encoded min
    pub upper_bound: Option<Vec<u8>>,  // Iceberg binary-encoded max
}
```

## 2. Statistics Extraction from Iceberg

### 2.1 `src/standalone/iceberg.rs`

New function `extract_data_files_with_stats` that reads manifest entries directly (not via `plan_files()` → `FileScanTask`, which doesn't expose column stats).

For each DataFile in the current snapshot's manifest list:
- Extract `file_path`, `file_size_in_bytes`, `record_count`
- Extract per-column: `column_sizes`, `null_value_counts`, `lower_bounds`, `upper_bounds`
- Return as `Vec<DataFileWithStats>` containing path, size, row_count, and per-column stats map keyed by field ID

Keep existing `extract_data_files` as lightweight fallback.

### 2.2 `src/standalone/engine.rs`

`register_iceberg_tables_for_query` calls `extract_data_files_with_stats`, maps field IDs to column names using the Iceberg schema, and populates `S3FileInfo.column_stats`.

### 2.3 Raw stats to ColumnStatistic conversion

At optimizer entry, aggregate raw `IcebergColumnStats` across all files into `ColumnStatistic`:
- `min_value` = min of all files' lower_bounds (decoded by column type)
- `max_value` = max of all files' upper_bounds
- `nulls_fraction` = sum(null_count) / sum(row_count)
- `average_row_size` = sum(column_size) / sum(row_count - null_count)
- `distinct_values_count` = heuristic: `min(row_count, sqrt(row_count) * 10)` (no Puffin NDV in phase 1)

## 3. Cardinality Estimation

### 3.1 New file: `src/sql/optimizer/cardinality.rs`

`estimate_statistics(plan: &LogicalPlan, table_stats: &HashMap<String, TableStatistics>) -> Statistics`

Recursively traverses the LogicalPlan bottom-up, propagating Statistics:

**Scan**: Look up TableStatistics, build initial Statistics.

**Filter**: `output_rows = input_rows * selectivity(predicate)`
- `col = literal`: `1.0 / ndv`
- `col > literal`: `(max - literal) / (max - min)`, fallback 0.25
- `col IS NULL`: `nulls_fraction`
- `AND(A, B)`: `sel(A) * sel(B)` (independence)
- `OR(A, B)`: `sel(A) + sel(B) - sel(A) * sel(B)`
- `IN(v1..vN)`: `min(N / ndv, 1.0)`
- Unknown predicate: 0.25

**Project**: row_count unchanged, filter column_stats to projected columns.

**Aggregate**: `output_rows = min(product(group_key_ndvs), input_rows * 0.75)`

**Join** (inner, `A JOIN B ON a.k = b.k`):
- `output_rows = rows_A * rows_B / max(ndv_A(k), ndv_B(k))`
- Join key output NDV = `min(ndv_A(k), ndv_B(k))`
- Left Outer: `max(inner_estimate, rows_A)`
- Cross: `rows_A * rows_B`
- Semi: `rows_A * min(1.0, ndv_B(k) / ndv_A(k))`
- Anti: `rows_A * (1.0 - min(1.0, ndv_B(k) / ndv_A(k)))`
- Non-equi: `rows_A * rows_B * 0.25`

**Sort**: row_count unchanged.
**Limit**: `min(limit, input_rows)`.
**Union**: `sum(child_rows)`.

### 3.2 Selectivity constants (aligned with StarRocks)

```rust
const PREDICATE_UNKNOWN_FILTER: f64 = 0.25;
const IS_NULL_FILTER: f64 = 0.1;
const IN_PREDICATE_DEFAULT_FILTER: f64 = 0.5;
const UNKNOWN_GROUP_BY_CORRELATION: f64 = 0.75;
const ANTI_JOIN_SELECTIVITY: f64 = 0.4;
```

## 4. Cost Model

### 4.1 New file: `src/sql/optimizer/cost.rs`

`estimate_cost(plan: &LogicalPlan, stats: &Statistics, child_stats: &[Statistics]) -> CostEstimate`

`compute_size = output_row_count * avg_row_size`

| Operator | CPU | Memory | Network |
|----------|-----|--------|---------|
| Scan | compute_size | 0 | 0 |
| Filter | input_compute_size | 0 | 0 |
| Project | input_compute_size | 0 | 0 |
| Aggregate | input_compute_size | output_compute_size | 0 |
| Sort/TopN | input_compute_size * log2(rows) | output_compute_size | 0 |
| HashJoin | build + probe + output | right_compute_size | 0 |
| NestLoopJoin | left_size * right_rows * 2 | right_size * 200 | 0 |

HashJoin detail:
```
build_cost = right_compute_size
probe_penalty = clamp(ln(right_rows / 100_000), 1.0, 12.0)
probe_cost = left_compute_size * probe_penalty
cpu = build_cost + probe_cost + output_compute_size
mem = right_compute_size
```

Cumulative cost: node total = self cost + sum of all child costs.

## 5. DP Join Reorder

### 5.1 Restructure `src/sql/optimizer/join_reorder.rs`

Replace the current pairwise heuristic with DP enumeration.

**Input extraction**: Flatten the join tree into a join graph:
- `relations: Vec<LogicalPlan>` — leaf nodes, each assigned a bit position
- `predicates: Vec<JoinPredicate>` — extracted from join conditions, tagged with which relations they reference
- `base_stats: Vec<Statistics>` — per-relation statistics

**DP algorithm** (aligned with StarRocks JoinReorderDP):

```
memo: HashMap<u16, DpEntry>  // bitmask → {plan, cumulative_cost, statistics}

// Init: single relations
for i in 0..n:
    memo[1 << i] = { plan: relations[i], cost: scan_cost, stats: base_stats[i] }

// Enumerate subsets of increasing size
for size in 2..=n:
    for each subset S of `size` bits:
        for each non-empty proper subset L of S:
            R = S ^ L  // complement within S
            if L > R: continue  // avoid duplicate pairs
            if !memo.contains(L) || !memo.contains(R): continue
            if !has_join_predicate_between(L, R): continue

            // Try both directions
            try_join(memo, S, L, R)  // L probe, R build
            try_join(memo, S, R, L)  // R probe, L build

// Result: memo[(1 << n) - 1].plan
```

**Constraints**:
- Only rearranges INNER JOINs; LEFT/RIGHT/SEMI/ANTI stay in original order
- Tables > 10: fallback to current heuristic (Greedy in phase 2)
- BitSet via u16 (supports up to 16 tables)

### 5.2 Optimizer pipeline

```rust
pub fn optimize(
    plan: LogicalPlan,
    table_stats: &HashMap<String, TableStatistics>,
) -> LogicalPlan {
    let plan = predicate_pushdown::push_down_predicates(plan);
    let plan = join_reorder::reorder_joins_cbo(plan, table_stats);
    let plan = column_pruning::prune_columns(plan);
    plan
}
```

Predicate pushdown first (filters at scan improve cardinality accuracy), then CBO join reorder, then column pruning.

## 6. Files Changed

| File | Change |
|------|--------|
| `src/sql/statistics.rs` | **New**: ColumnStatistic, Statistics, CostEstimate, TableStatistics |
| `src/sql/mod.rs` | Add `pub mod statistics;` |
| `src/sql/catalog.rs` | S3FileInfo.column_stats, IcebergColumnStats struct |
| `src/standalone/iceberg.rs` | New `extract_data_files_with_stats` from manifest |
| `src/standalone/engine.rs` | Thread column stats into S3FileInfo; build TableStatistics for optimizer |
| `src/sql/optimizer/cardinality.rs` | **New**: cardinality estimation framework |
| `src/sql/optimizer/cost.rs` | **New**: cost model |
| `src/sql/optimizer/join_reorder.rs` | Replace heuristic with DP + cost-based join reorder |
| `src/sql/optimizer/mod.rs` | `optimize` signature change, wire new passes |

## 7. Scope Exclusions

- Histogram support (phase 2)
- Puffin NDV reading (phase 2)
- Greedy join reorder for >10 tables (phase 2)
- Distribution costing / broadcast vs shuffle (phase 2, requires distributed execution)
- Data skew detection and penalty (phase 2)
- Multi-column correlation stats (phase 2)
- ANALYZE TABLE command (phase 2)

## 8. Test Plan

- Unit tests: ColumnStatistic aggregation from IcebergColumnStats
- Unit tests: selectivity calculation for each predicate type
- Unit tests: cardinality propagation through Scan → Filter → Join → Aggregate
- Unit tests: cost model for each operator type
- Unit tests: DP join reorder for 2, 3, 4 table inner join scenarios
- Integration: TPC-DS q1-50 regression (no breakage of currently passing queries)
- Performance: release build TPC-DS timing comparison before/after CBO
