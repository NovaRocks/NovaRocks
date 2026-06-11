# TPC-DS Type System & Statistics Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Align standalone SQL Decimal+Float type rules with StarRocks FE and add row-count-based join cost estimation.

**Architecture:** Two independent changes: (1) modify `types.rs` so Decimal+Float promotes to Float64 instead of staying Decimal, (2) thread Iceberg `record_count` through `S3FileInfo` into the join reorder optimizer.

**Tech Stack:** Rust, Arrow DataType, Iceberg crate (v0.9.0)

---

### Task 1: Decimal + Float arithmetic promotes to Float64

**Files:**
- Modify: `src/sql/types.rs:63-68` (arithmetic_result_type_with_op)
- Modify: `src/sql/types.rs:109-117` (wider_type)

- [ ] **Step 1: Write failing test for arithmetic_result_type_with_op**

Add to the bottom of `src/sql/types.rs`:

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use arrow::datatypes::DataType;

    #[test]
    fn decimal_times_float_returns_float64() {
        let result = arithmetic_result_type_with_op(
            &DataType::Decimal128(7, 2),
            &DataType::Float64,
            "mul",
        );
        assert_eq!(result, DataType::Float64);
    }

    #[test]
    fn float_plus_decimal_returns_float64() {
        let result = arithmetic_result_type_with_op(
            &DataType::Float64,
            &DataType::Decimal128(18, 6),
            "add",
        );
        assert_eq!(result, DataType::Float64);
    }

    #[test]
    fn decimal_div_float32_returns_float64() {
        let result = arithmetic_result_type_with_op(
            &DataType::Decimal128(10, 4),
            &DataType::Float32,
            "div",
        );
        assert_eq!(result, DataType::Float64);
    }

    #[test]
    fn wider_type_decimal_vs_float64_returns_float64() {
        let result = wider_type(&DataType::Decimal128(7, 2), &DataType::Float64);
        assert_eq!(result, DataType::Float64);
    }

    #[test]
    fn wider_type_float32_vs_decimal_returns_float64() {
        let result = wider_type(&DataType::Float32, &DataType::Decimal128(18, 6));
        assert_eq!(result, DataType::Float64);
    }

    #[test]
    fn decimal_times_decimal_unchanged() {
        let result = arithmetic_result_type_with_op(
            &DataType::Decimal128(7, 2),
            &DataType::Decimal128(10, 4),
            "mul",
        );
        // scale=2+4=6, precision=7+10=17
        assert_eq!(result, DataType::Decimal128(17, 6));
    }

    #[test]
    fn decimal_plus_int_unchanged() {
        let result = arithmetic_result_type_with_op(
            &DataType::Decimal128(7, 2),
            &DataType::Int32,
            "add",
        );
        // Decimal + Int(19,0) -> add: scale=max(2,0)=2, precision=max(5,19)+2+1=22
        assert_eq!(result, DataType::Decimal128(22, 2));
    }
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cargo test -p novarocks --lib sql::types::tests -- --nocapture`

Expected: `decimal_times_float_returns_float64`, `float_plus_decimal_returns_float64`, `decimal_div_float32_returns_float64`, `wider_type_decimal_vs_float64_returns_float64`, `wider_type_float32_vs_decimal_returns_float64` all FAIL (they return Decimal128 instead of Float64). The two unchanged tests should PASS.

- [ ] **Step 3: Fix arithmetic_result_type_with_op**

In `src/sql/types.rs`, replace lines 63-68:

```rust
        // Decimal + Float -> Decimal (treat float literal as decimal)
        (DataType::Decimal128(p, s), DataType::Float64 | DataType::Float32)
        | (DataType::Float64 | DataType::Float32, DataType::Decimal128(p, s)) => {
            // Treat float as Decimal(38, 9) and compute result
            decimal_arithmetic_result_type(*p, *s as i8, 38, 9, op)
        }
```

with:

```rust
        // Decimal + Float -> Float64 (StarRocks FE: both sides promote to Double)
        (DataType::Decimal128(_, _), DataType::Float64 | DataType::Float32)
        | (DataType::Float64 | DataType::Float32, DataType::Decimal128(_, _)) => DataType::Float64,
```

- [ ] **Step 4: Fix wider_type**

In `src/sql/types.rs`, replace lines 109-117:

```rust
        // Decimal + Float -> Decimal (for comparisons, cast float to decimal)
        (DataType::Decimal128(_, _), DataType::Float64 | DataType::Float32)
        | (DataType::Float64 | DataType::Float32, DataType::Decimal128(_, _)) => {
            let (p, s) = match (a, b) {
                (DataType::Decimal128(p, s), _) | (_, DataType::Decimal128(p, s)) => (*p, *s),
                _ => unreachable!(),
            };
            DataType::Decimal128(p, s)
        }
```

with:

```rust
        // Decimal + Float -> Float64 (StarRocks FE: promote to Double)
        (DataType::Decimal128(_, _), DataType::Float64 | DataType::Float32)
        | (DataType::Float64 | DataType::Float32, DataType::Decimal128(_, _)) => DataType::Float64,
```

- [ ] **Step 5: Run tests to verify they pass**

Run: `cargo test -p novarocks --lib sql::types::tests -- --nocapture`

Expected: All 7 tests PASS.

- [ ] **Step 6: Run full test suite for regressions**

Run: `cargo test -p novarocks`

Expected: No new failures. Existing tests that depend on Decimal+Float→Decimal may need updating — fix any such failures by updating expected values to Float64.

- [ ] **Step 7: Commit**

```bash
git add src/sql/types.rs
git commit -m "fix: Decimal + Float promotes to Float64 (align with StarRocks FE)"
```

---

### Task 2: Add row_count to S3FileInfo

**Files:**
- Modify: `src/sql/catalog.rs:14-17`
- Modify: `src/sql/optimizer/join_reorder.rs:216-219` (test helper)
- Modify: `src/standalone/engine.rs:364`

- [ ] **Step 1: Add row_count field to S3FileInfo**

In `src/sql/catalog.rs`, replace:

```rust
#[derive(Clone, Debug)]
pub struct S3FileInfo {
    pub path: String,
    pub size: i64,
}
```

with:

```rust
#[derive(Clone, Debug)]
pub struct S3FileInfo {
    pub path: String,
    pub size: i64,
    /// Row count from Iceberg file metadata. None for non-Iceberg sources.
    pub row_count: Option<i64>,
}
```

- [ ] **Step 2: Fix all S3FileInfo construction sites**

In `src/sql/optimizer/join_reorder.rs` line 216, replace:

```rust
                files: vec![S3FileInfo {
                    path: format!("s3://bucket/{}.parquet", name),
                    size: total_bytes,
                }],
```

with:

```rust
                files: vec![S3FileInfo {
                    path: format!("s3://bucket/{}.parquet", name),
                    size: total_bytes,
                    row_count: None,
                }],
```

In `src/standalone/engine.rs` line 364, replace:

```rust
                        .map(|(path, size)| crate::sql::catalog::S3FileInfo { path, size })
```

with:

```rust
                        .map(|(path, size, row_count)| crate::sql::catalog::S3FileInfo {
                            path,
                            size,
                            row_count,
                        })
```

- [ ] **Step 3: Build to check for remaining construction sites**

Run: `cargo build 2>&1 | grep "S3FileInfo"`

Expected: If there are other construction sites missing `row_count`, the compiler will flag them. Fix any remaining ones by adding `row_count: None`.

- [ ] **Step 4: Commit**

```bash
git add src/sql/catalog.rs src/sql/optimizer/join_reorder.rs src/standalone/engine.rs
git commit -m "feat: add row_count field to S3FileInfo for statistics pipeline"
```

---

### Task 3: Thread row_count from Iceberg extract_data_files

**Files:**
- Modify: `src/standalone/iceberg.rs:542-569`

- [ ] **Step 1: Change extract_data_files return type**

In `src/standalone/iceberg.rs`, replace the entire `extract_data_files` function (lines 542-569):

```rust
/// Extract data file paths and sizes from an Iceberg table via scan planning.
pub(crate) fn extract_data_files(
    table: &iceberg::table::Table,
) -> Result<Vec<(String, i64)>, String> {
    block_on_iceberg(async {
        let scan = table
            .scan()
            .build()
            .map_err(|e| format!("build scan: {e}"))?;
        let tasks: Vec<_> = scan
            .plan_files()
            .await
            .map_err(|e| format!("plan files: {e}"))?
            .try_collect()
            .await
            .map_err(|e| format!("collect tasks: {e}"))?;
        Ok(tasks
            .iter()
            .map(|t| {
                (
                    t.data_file_path.clone(),
                    i64::try_from(t.file_size_in_bytes).unwrap_or(i64::MAX),
                )
            })
            .collect())
    })
    .map_err(|e| format!("extract data files runtime: {e}"))?
}
```

with:

```rust
/// Extract data file paths, sizes, and row counts from an Iceberg table via scan planning.
pub(crate) fn extract_data_files(
    table: &iceberg::table::Table,
) -> Result<Vec<(String, i64, Option<i64>)>, String> {
    block_on_iceberg(async {
        let scan = table
            .scan()
            .build()
            .map_err(|e| format!("build scan: {e}"))?;
        let tasks: Vec<_> = scan
            .plan_files()
            .await
            .map_err(|e| format!("plan files: {e}"))?
            .try_collect()
            .await
            .map_err(|e| format!("collect tasks: {e}"))?;
        Ok(tasks
            .iter()
            .map(|t| {
                (
                    t.data_file_path.clone(),
                    i64::try_from(t.file_size_in_bytes).unwrap_or(i64::MAX),
                    t.record_count.map(|c| c as i64),
                )
            })
            .collect())
    })
    .map_err(|e| format!("extract data files runtime: {e}"))?
}
```

- [ ] **Step 2: Fix all callers of extract_data_files**

The local-Iceberg path in `src/standalone/engine.rs` (around line 371) also calls `extract_data_files`. Replace:

```rust
                let data_files = super::iceberg::extract_data_files(&loaded.table)?;
                if let Some((first_path, _)) = data_files.first() {
```

with:

```rust
                let data_files = super::iceberg::extract_data_files(&loaded.table)?;
                if let Some((first_path, _, _)) = data_files.first() {
```

Search for any other callers:

Run: `grep -rn "extract_data_files" src/`

Fix each caller to handle the new 3-tuple.

- [ ] **Step 3: Build and verify**

Run: `cargo build`

Expected: Clean build with no errors.

- [ ] **Step 4: Commit**

```bash
git add src/standalone/iceberg.rs src/standalone/engine.rs
git commit -m "feat: extract row_count from Iceberg FileScanTask metadata"
```

---

### Task 4: Use row_count in join reorder optimizer

**Files:**
- Modify: `src/sql/optimizer/join_reorder.rs:38-65`

- [ ] **Step 1: Write failing test**

Add to the `tests` module at the bottom of `src/sql/optimizer/join_reorder.rs`:

```rust
    /// Helper: build a TableDef with S3 files that have row counts.
    fn s3_table_with_rows(name: &str, total_bytes: i64, row_count: i64) -> TableDef {
        TableDef {
            name: name.to_string(),
            columns: vec![ColumnDef {
                name: "id".to_string(),
                data_type: DataType::Int32,
                nullable: false,
            }],
            storage: TableStorage::S3ParquetFiles {
                files: vec![S3FileInfo {
                    path: format!("s3://bucket/{}.parquet", name),
                    size: total_bytes,
                    row_count: Some(row_count),
                }],
                cloud_properties: Default::default(),
            },
        }
    }

    #[test]
    fn row_count_overrides_file_size_for_join_reorder() {
        // dim_table: large file (10 MB) but few rows (1000)
        // fact_table: smaller file (5 MB) but many rows (1_000_000)
        // Without row_count: dim(10MB) > fact(5MB), so dim probes, fact builds — WRONG
        // With row_count: fact(1M) > dim(1K), so fact probes, dim builds — CORRECT
        let dim = s3_table_with_rows("dim", 10_000_000, 1_000);
        let fact = s3_table_with_rows("fact", 5_000_000, 1_000_000);

        let plan = LogicalPlan::Join(JoinNode {
            left: Box::new(scan_for(&dim)),
            right: Box::new(scan_for(&fact)),
            join_type: JoinKind::Inner,
            condition: eq_condition(),
        });

        let reordered = reorder_joins(plan);

        match reordered {
            LogicalPlan::Join(j) => {
                let left_name = match j.left.as_ref() {
                    LogicalPlan::Scan(s) => s.table.name.clone(),
                    other => panic!("expected Scan, got {:?}", std::mem::discriminant(other)),
                };
                let right_name = match j.right.as_ref() {
                    LogicalPlan::Scan(s) => s.table.name.clone(),
                    other => panic!("expected Scan, got {:?}", std::mem::discriminant(other)),
                };
                assert_eq!(left_name, "fact", "fact (more rows) should be probe side");
                assert_eq!(right_name, "dim", "dim (fewer rows) should be build side");
            }
            other => panic!("expected Join, got {:?}", std::mem::discriminant(&other)),
        }
    }
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test -p novarocks --lib sql::optimizer::join_reorder::tests::row_count_overrides_file_size_for_join_reorder -- --nocapture`

Expected: FAIL — currently dim(10MB) is on the left (probe) and fact(5MB) on the right (build), which is the opposite of what we want.

- [ ] **Step 3: Implement row_count-based estimation**

In `src/sql/optimizer/join_reorder.rs`, replace the `S3ParquetFiles` arm in `estimate_size` (lines 42-45):

```rust
                TableStorage::S3ParquetFiles { files, .. } => {
                    let total: u64 = files.iter().map(|f| f.size.max(0) as u64).sum();
                    total.max(1)
                }
```

with:

```rust
                TableStorage::S3ParquetFiles { files, .. } => {
                    // Prefer row_count when available (from Iceberg metadata).
                    // Fall back to file size in bytes if any file lacks row_count.
                    let all_have_row_count =
                        !files.is_empty() && files.iter().all(|f| f.row_count.is_some());
                    if all_have_row_count {
                        let total: u64 = files
                            .iter()
                            .map(|f| f.row_count.unwrap().max(0) as u64)
                            .sum();
                        total.max(1)
                    } else {
                        let total: u64 = files.iter().map(|f| f.size.max(0) as u64).sum();
                        total.max(1)
                    }
                }
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `cargo test -p novarocks --lib sql::optimizer::join_reorder::tests -- --nocapture`

Expected: All tests PASS including the new `row_count_overrides_file_size_for_join_reorder`.

- [ ] **Step 5: Run full test suite**

Run: `cargo test -p novarocks`

Expected: No regressions.

- [ ] **Step 6: Commit**

```bash
git add src/sql/optimizer/join_reorder.rs
git commit -m "feat: use Iceberg row_count for join cost estimation"
```

---

### Task 5: Integration verification

**Files:** None (testing only)

- [ ] **Step 1: Build release-ready binary**

Run: `cargo build`

Expected: Clean build.

- [ ] **Step 2: Restart standalone-server**

```bash
pkill -f "novarocks standalone-server" 2>/dev/null
sleep 1
nohup ./target/debug/novarocks standalone-server > /tmp/novarocks_test.log 2>&1 &
sleep 3
```

- [ ] **Step 3: Recreate TPC-DS catalog (if needed)**

```sql
CREATE EXTERNAL CATALOG IF NOT EXISTS tpcds_cat
PROPERTIES (
    "type"="iceberg",
    "iceberg.catalog.type"="hadoop",
    "iceberg.catalog.warehouse"="oss://novarocks/iceberg-catalog/",
    "aws.s3.access_key"="admin",
    "aws.s3.secret_key"="admin123",
    "aws.s3.endpoint"="http://127.0.0.1:9000",
    "aws.s3.enable_path_style_access"="true"
);
```

- [ ] **Step 4: Verify type fix with q1-style query**

```sql
SET CATALOG tpcds_cat;
USE tpcds;
SELECT avg(sr_return_amt) * 1.2 FROM store_returns LIMIT 1;
```

Expected: Returns a Float64 result (not Decimal), no type error.

- [ ] **Step 5: Run TPC-DS q1-50 quick test**

Use the 8-second-timeout test script from the previous session to compare pass/fail counts before and after the fix.

- [ ] **Step 6: Run TPC-H regression check**

```bash
cargo run --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests -- \
  --suite tpc-h --mode verify --config tests/sql-test-runner/conf/standalone_iceberg.conf
```

Expected: TPC-H passes at same level as before (no regressions).
