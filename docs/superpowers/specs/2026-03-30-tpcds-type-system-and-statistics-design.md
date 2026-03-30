# TPC-DS: Type System Alignment & Statistics Pipeline

**Date:** 2026-03-30
**Goal:** Align standalone SQL layer with StarRocks FE type semantics and add row-count-based join cost estimation, validated against TPC-DS q1-50.

## Context

NovaRocks standalone mode has its own SQL analyzer, planner, optimizer, and Thrift emitter. These components must produce plans whose type declarations match what the pipeline executor expects — which is the same as what StarRocks FE would declare.

TPC-DS q1-50 testing revealed two systemic issues:

1. **Type mismatch** (q1, q5, q6): Decimal + Float arithmetic produces Decimal instead of Float64. The pipeline executor sees a schema mismatch and fails.
2. **Bad join ordering** (many timeouts): The optimizer estimates table size by parquet file bytes, not row counts. TPC-DS dimension tables with many string columns (large bytes, few rows) get misclassified as "big tables" and placed on the wrong side of hash joins.

## Design

### 1. Decimal + Float Type Promotion

**File:** `src/sql/types.rs`

**Change `arithmetic_result_type_with_op`:** When either operand is Float32/Float64 and the other is Decimal128, both operands promote to Float64 and the result is Float64. This applies to all arithmetic operators (+, -, *, /, %).

Current behavior:
```
Decimal128(7,2) * Float64  →  treats Float as Decimal(38,9), result Decimal128
```

Target behavior (matches StarRocks FE `rewriteDecimalFloatingPointOperation`):
```
Decimal128(7,2) * Float64  →  both sides Float64, result Float64
```

**Change `wider_type`:** When finding the common type between Decimal128 and Float32/Float64 (used in CASE, UNION, comparison coercion), return Float64 instead of Decimal.

Current behavior:
```
wider_type(Decimal128(7,2), Float64)  →  Decimal128(7,2)
```

Target behavior:
```
wider_type(Decimal128(7,2), Float64)  →  Float64
```

**Why:** StarRocks FE always promotes Decimal + Float to Float64 in arithmetic contexts. The pipeline executor trusts the Thrift plan's declared types. When the standalone SQL layer declares Decimal where Float64 is expected, the executor's Arrow RecordBatch schema validation fails.

### 2. Row-Count Statistics Pipeline

Thread Iceberg `record_count` from file metadata to the join reorder optimizer.

#### 2a. S3FileInfo (src/sql/catalog.rs)

Add `row_count` field:

```rust
pub struct S3FileInfo {
    pub path: String,
    pub size: i64,
    pub row_count: Option<i64>,
}
```

`Option` because non-Iceberg scenarios (direct parquet path) may not have row counts. The struct is designed for future extension with column-level stats (min/max bounds, null counts, column sizes) when a CBO is introduced.

#### 2b. extract_data_files (src/standalone/iceberg.rs)

Change return type from `Vec<(String, i64)>` to include row_count. The Iceberg `FileScanTask` already carries `record_count` — just stop discarding it.

#### 2c. Table Registration (src/standalone/engine.rs)

In `register_iceberg_tables_for_query`, pass the row_count from `extract_data_files` into the `S3FileInfo` structs stored in `TableStorage::S3ParquetFiles`.

#### 2d. Join Reorder (src/sql/optimizer/join_reorder.rs)

Change `estimate_size` for `S3ParquetFiles`:

```
if all files have row_count → sum(row_count)
else → sum(file_size)  (current behavior, backward compatible)
```

Local parquet files (no row_count) continue using file size. This is backward compatible with all existing behavior.

## Scope Exclusions

These are known TPC-DS failures that are **not** addressed in this design:

- Column scope/qualification bugs (q16, q23, q41) — SubqueryAlias qualified column propagation
- Missing `GROUPING()` function (q27, q36)
- Subquery rewrite failure (q45) — SubqueryPlaceholder not replaced
- `stddev_samp` for Int32 input (q17)
- Unsupported SQL pattern (q39)

These will be addressed in subsequent iterations.

## Test Plan

1. `cargo test` — no regressions in existing unit tests
2. Rerun TPC-H suite — verify no breakage to currently passing queries
3. Rerun TPC-DS q1-50 with 8s timeout — compare pass count before/after
4. Expected fixes: q1, q5, q6 type errors resolved; timeout query count reduced due to better join ordering

## Files Changed

| File | Change |
|------|--------|
| `src/sql/types.rs` | Decimal+Float→Float64 in `arithmetic_result_type_with_op` and `wider_type` |
| `src/sql/catalog.rs` | `S3FileInfo.row_count: Option<i64>` |
| `src/standalone/iceberg.rs` | `extract_data_files` returns row_count |
| `src/standalone/engine.rs` | `register_iceberg_tables_for_query` fills row_count |
| `src/sql/optimizer/join_reorder.rs` | `estimate_size` prefers row_count over file_size |
