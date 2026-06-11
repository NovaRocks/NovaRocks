# BITMAP / HLL Types Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Register `BITMAP` and `HLL` as first-class column types so that `CREATE TABLE`, `INSERT`, `SELECT`, and `AGGREGATE KEY` modifiers (`BITMAP_UNION` / `HLL_UNION`) work, while analyzer rejects unsafe usages (ORDER BY / GROUP BY / comparison / PRIMARY KEY / DISTRIBUTED BY) and 5 missing binary BITMAP scalar functions are filled in.

**Architecture:** Add two `SqlType` variants and two `ColumnAggregation` variants in the AST; mechanically fan out match arms across the existing SqlType consumption sites (parser, lowering, managed-lake DDL, iceberg catalog/default, sql_expr, iceberg_ctas, insert). Wire `SqlType::Bitmap → TPrimitiveType::OBJECT` and `SqlType::Hll → TPrimitiveType::HLL` (both already exist on the StarRocks thrift side). Add 5 binary BITMAP scalar functions in `bitmap_functions.rs` and wire them into `dispatch.rs`. Add 5 analyzer-side fail-fast checks. Record analytic case `analytic_test_bitmap_union_window`.

**Tech Stack:** Rust, sqlparser-rs custom dialect, arrow `BinaryArray`, `roaring::RoaringBitmap`, StarRocks SeriV2 binary format (helpers in `bitmap_common.rs`).

---

## Spec reference

[docs/design/specs/2026-05-20-bitmap-hll-types-design.md](../specs/2026-05-20-bitmap-hll-types-design.md)

## File overview

```
src/sql/parser/ast/mod.rs                          modify  + SqlType::{Bitmap,Hll}; ColumnAggregation::{BitmapUnion,HllUnion}
src/sql/parser/dialect/mod.rs                      modify  + convert_sql_type Custom("bitmap"/"hll")
src/sql/parser/dialect/create_table.rs             modify  + parse_column_aggregation BITMAP_UNION/HLL_UNION; + default literal handling
src/connector/starrocks/managed/ddl.rs             modify  + key_eligible_type/short_key_index_size/sql_type_to_tcolumn_type/parse_managed_logical_type/logical_type_name/ColumnAggregation map/is_complex_type
src/connector/iceberg/catalog/registry.rs          modify  + sql_type → iceberg PrimitiveType::Binary; ColumnAggregation roundtrip
src/connector/iceberg/catalog/schema_update.rs     modify  + handle Bitmap/Hll in schema comparison if hit
src/connector/iceberg/default_value.rs             modify  + default literal mapping
src/engine/sql_expr.rs                             modify  + SqlType::Bitmap/Hll → Arrow Binary; literal coercion
src/engine/iceberg_ctas.rs                         modify  + DataType::Binary → may need to disambiguate (out of scope: see §risk)
src/engine/insert.rs                               modify  + DataType::Binary fallback already covers it (no-op if Binary stays sufficient)
src/lower/type_lowering.rs                         modify  (only if SqlType is referenced; otherwise the TPrimitiveType::OBJECT/HLL → Arrow Binary path already exists)
src/sql/analyzer/mod.rs                            modify  + 5 fail-fast checks (ORDER BY, GROUP BY, comparison, PRIMARY KEY, DISTRIBUTED BY)
src/exec/expr/function/object/bitmap_functions.rs  modify  + eval_bitmap_or/xor/andnot/contains/intersect
src/exec/expr/function/object/dispatch.rs          modify  + register 5 new functions in match + OBJECT_FUNCTIONS + OBJECT_METADATA
sql-tests/function/sql/bitmap_binary_ops.sql       create
sql-tests/function/result/bitmap_binary_ops.result create (after record)
sql-tests/function/sql/bitmap_hll_type_restrictions.sql       create
sql-tests/function/result/bitmap_hll_type_restrictions.result create (after record)
sql-tests/analytic/result/analytic_test_bitmap_union_window.result  re-record
```

---

## Task 1: Register `SqlType::Bitmap` / `SqlType::Hll` in the AST

**Files:**
- Modify: `src/sql/parser/ast/mod.rs:321-346`

- [ ] **Step 1: Add the two enum variants**

In `src/sql/parser/ast/mod.rs`, locate `pub enum SqlType { ... }` (around line 321) and add two new variants near `Json` / `Binary`:

```rust
pub enum SqlType {
    // ... existing variants ...
    String,
    Json,
    Binary,
    Bitmap,
    Hll,
    Boolean,
    // ... rest ...
}
```

- [ ] **Step 2: Build to surface fan-out compile errors**

Run: `cargo build 2>&1 | grep -E 'error\[|non-exhaustive' | head -40`

Expected: a list of non-exhaustive `match` errors across `src/sql/parser/dialect/`, `src/connector/starrocks/managed/ddl.rs`, `src/connector/iceberg/catalog/registry.rs`, `src/connector/iceberg/default_value.rs`, `src/engine/sql_expr.rs`. These will be addressed in Tasks 2–4. Do not commit yet.

---

## Task 2: Wire SqlType::Bitmap / Hll into parser

**Files:**
- Modify: `src/sql/parser/dialect/mod.rs:166-180`
- Modify: `src/sql/parser/dialect/create_table.rs:964-980` (default-literal handling)
- Test: same file's existing `mod tests` (around line 1046)

- [ ] **Step 1: Add a failing parser test**

In `src/sql/parser/dialect/create_table.rs` `mod tests` (near line 1046), append:

```rust
#[test]
fn parse_create_table_with_bitmap_and_hll_columns() {
    let sql = "CREATE TABLE foo (k INT, bm BITMAP, hv HLL) \
               DUPLICATE KEY(k) DISTRIBUTED BY HASH(k) BUCKETS 1";
    let stmt = parse_starrocks_create_table(sql).expect("parse must succeed");
    let columns = match &stmt.kind {
        CreateTableKind::Columns(cols) => cols,
        _ => panic!("expected Columns kind"),
    };
    assert_eq!(columns[1].data_type, SqlType::Bitmap);
    assert_eq!(columns[2].data_type, SqlType::Hll);
}
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `cargo test -p novarocks parse_create_table_with_bitmap_and_hll_columns -- --nocapture 2>&1 | tail -20`

Expected: fails with `unsupported data type: BITMAP` (or similar).

- [ ] **Step 3: Add Custom-branch mapping**

In `src/sql/parser/dialect/mod.rs`, locate the `sqlast::DataType::Custom(name, modifiers)` arm (around line 166). Inside the inner match, add two cases:

```rust
"json" | "jsonb" => Ok(SqlType::Json),
"varbinary" | "binary" => Ok(SqlType::Binary),
"bitmap" => Ok(SqlType::Bitmap),
"hll" => Ok(SqlType::Hll),
"variant" => Ok(SqlType::Variant),
```

- [ ] **Step 4: Run the test to verify it passes**

Run: `cargo test -p novarocks parse_create_table_with_bitmap_and_hll_columns -- --nocapture 2>&1 | tail -20`

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add src/sql/parser/ast/mod.rs src/sql/parser/dialect/mod.rs src/sql/parser/dialect/create_table.rs
git commit -m "feat(types): add SqlType::Bitmap and SqlType::Hll variants + parser

Lexer maps the BITMAP and HLL custom identifiers from sqlparser-rs into
the new SqlType variants. Match-arm fan-out into downstream sites lands
in the following commits."
```

---

## Task 3: Wire SqlType::Bitmap / Hll through managed-lake DDL

**Files:**
- Modify: `src/connector/starrocks/managed/ddl.rs:419-445` (key eligibility, short-key index, string family)
- Modify: `src/connector/starrocks/managed/ddl.rs:1085-1130` (is_complex_type, sql_type_to_tcolumn_type)
- Modify: `src/connector/starrocks/managed/ddl.rs:1145-1265` (sql_type_to_ttype_desc, logical_type_name)
- Modify: `src/connector/starrocks/managed/ddl.rs:1308-1320` (parse_managed_logical_type)
- Modify: `src/connector/starrocks/managed/ddl.rs:2045-2090` (existing tests; add coverage)

- [ ] **Step 1: Write failing test for thrift mapping**

In `src/connector/starrocks/managed/ddl.rs` `mod tests`, append:

```rust
#[test]
fn bitmap_hll_thrift_mapping() {
    let bm = sql_type_to_tcolumn_type(&SqlType::Bitmap).expect("bitmap thrift");
    assert_eq!(bm.primitive_type, crate::types::TPrimitiveType::OBJECT);

    let hv = sql_type_to_tcolumn_type(&SqlType::Hll).expect("hll thrift");
    assert_eq!(hv.primitive_type, crate::types::TPrimitiveType::HLL);

    assert_eq!(logical_type_name(&SqlType::Bitmap), "BITMAP");
    assert_eq!(logical_type_name(&SqlType::Hll), "HLL");

    assert_eq!(
        parse_managed_logical_type("BITMAP").expect("bitmap parse"),
        SqlType::Bitmap
    );
    assert_eq!(
        parse_managed_logical_type("HLL").expect("hll parse"),
        SqlType::Hll
    );

    // BITMAP/HLL are not eligible as key columns.
    assert!(!key_eligible_type(&SqlType::Bitmap));
    assert!(!key_eligible_type(&SqlType::Hll));
}
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `cargo test -p novarocks bitmap_hll_thrift_mapping -- --nocapture 2>&1 | tail -30`

Expected: fails with non-exhaustive match errors and/or function returning Err.

- [ ] **Step 3: Fan out match arms in `ddl.rs`**

The match sites to update (use Read/Edit to confirm the exact context around each line, but the additions are mechanical):

1. `key_eligible_type` (around line 419): leave Bitmap/Hll **out** of the eligible list — they must not be PK / sort / agg-key columns. The function already returns `false` by default, so as long as it uses a positive whitelist, nothing to add. If it uses an exhaustive match, add `SqlType::Bitmap | SqlType::Hll => false`.

2. `short_key_index_size` (around line 433): add arms for Bitmap/Hll. Use 0 (not key-eligible) or sentinel; mirror `SqlType::Json` arm.

3. `is_string_family` (around line 450): leave Bitmap/Hll out (not in string family).

4. `is_complex_type` (around line 1085): leave Bitmap/Hll out (not complex — they're scalar Binary).

5. `sql_type_to_tcolumn_type` (around line 1092): add

```rust
SqlType::Bitmap => (crate::types::TPrimitiveType::OBJECT, None, None, None),
SqlType::Hll    => (crate::types::TPrimitiveType::HLL,    None, None, None),
```

6. Wherever `SqlType::Json` is listed alongside `Binary` (around lines 424, 440, 1221, 1233), append `| SqlType::Bitmap | SqlType::Hll` so that downstream behavior treats them like binary blobs.

7. `logical_type_name` (around line 1250):

```rust
SqlType::Bitmap => "BITMAP".to_string(),
SqlType::Hll    => "HLL".to_string(),
```

8. `parse_managed_logical_type` (around line 1313):

```rust
"BITMAP" => Ok(SqlType::Bitmap),
"HLL"    => Ok(SqlType::Hll),
```

- [ ] **Step 4: Run the test to verify it passes**

Run: `cargo test -p novarocks bitmap_hll_thrift_mapping -- --nocapture 2>&1 | tail -10`

Expected: PASS.

- [ ] **Step 5: Run full build to confirm no other ddl.rs fan-out is missed**

Run: `cargo build 2>&1 | grep -E "error" | grep "ddl.rs" | head -10`

Expected: no errors from `ddl.rs`. If any appear, add the missing arm.

- [ ] **Step 6: Commit**

```bash
git add src/connector/starrocks/managed/ddl.rs
git commit -m "feat(managed-lake): map SqlType::Bitmap/Hll to TPrimitiveType OBJECT/HLL

key_eligible_type leaves Bitmap/Hll out (not allowed as key columns).
logical_type_name + parse_managed_logical_type round-trip BITMAP/HLL."
```

---

## Task 4: Wire SqlType::Bitmap / Hll through iceberg + sql_expr + insert + ctas

**Files:**
- Modify: `src/connector/iceberg/catalog/registry.rs:1684`
- Modify: `src/connector/iceberg/default_value.rs:63, 68, 111, 124`
- Modify: `src/connector/iceberg/catalog/schema_update.rs:1604` (if hit)
- Modify: `src/engine/sql_expr.rs:643, 947`
- Modify: `src/engine/iceberg_ctas.rs:319`
- Modify: `src/engine/insert.rs:122` (no-op if `DataType::Binary` already maps to `SqlType::Binary` — Bitmap/Hll go through Json-style code paths)

- [ ] **Step 1: Run build to see exact remaining fan-out errors**

Run: `cargo build 2>&1 | grep -E 'error|non-exhaustive' | head -40`

Expected: errors point to the four files listed above.

- [ ] **Step 2: Add iceberg primitive mapping**

In `src/connector/iceberg/catalog/registry.rs`, locate `SqlType::Binary => Type::Primitive(PrimitiveType::Binary)` (around line 1684):

```rust
SqlType::String | SqlType::Json => Type::Primitive(PrimitiveType::String),
SqlType::Binary | SqlType::Bitmap | SqlType::Hll => Type::Primitive(PrimitiveType::Binary),
```

- [ ] **Step 3: Add iceberg default-literal mapping**

In `src/connector/iceberg/default_value.rs`, extend the binary-default arms (around lines 68 and 124) so BITMAP/HLL accept binary literals:

```rust
(DefaultLiteral::Binary(b), SqlType::Binary | SqlType::Bitmap | SqlType::Hll) =>
    PrimitiveLiteral::Binary(b.clone()),
```

```rust
| (IcebergLiteral::Primitive(PrimitiveLiteral::Binary(_)),
   SqlType::Binary | SqlType::Bitmap | SqlType::Hll)
```

Reject string literals for Bitmap/Hll explicitly so the user is told to use `to_bitmap` / `hll_hash`. In the arm matching `SqlType::String | SqlType::Json` (around line 111), do **not** add Bitmap/Hll.

- [ ] **Step 4: Add sql_expr arrow + literal mapping**

In `src/engine/sql_expr.rs`, locate the two SqlType match sites:

- Around line 643 (`SqlType::String | SqlType::Json` literal handling): leave Bitmap/Hll out; they should not accept arbitrary string/number literals at this site.
- Around line 657 (`SqlType::Binary` literal): extend:

```rust
SqlType::Binary | SqlType::Bitmap | SqlType::Hll => match &value { ... },
```

- Around line 947 (Arrow type mapping):

```rust
SqlType::String | SqlType::Json => Ok(DataType::Utf8),
SqlType::Binary | SqlType::Bitmap | SqlType::Hll => Ok(DataType::Binary),
```

- [ ] **Step 5: Touch iceberg_ctas + insert if necessary**

In `src/engine/iceberg_ctas.rs:319`, the existing `DataType::Binary | DataType::LargeBinary => SqlType::Binary` collapses binary back to `SqlType::Binary`. That is correct: when reading **back** from arrow, we have no way to know it was Bitmap/Hll — that information lives in the column schema metadata, not in the arrow `DataType`. Leave as-is.

In `src/engine/insert.rs:122`, same reasoning. Leave as-is.

In `src/connector/iceberg/catalog/schema_update.rs`, if `cargo build` reports a non-exhaustive match here, add `SqlType::Bitmap | SqlType::Hll` to whichever existing `SqlType::Binary` arm appears in context.

- [ ] **Step 6: Run build clean**

Run: `cargo build 2>&1 | grep -E 'error\[' | head -10`

Expected: no errors.

- [ ] **Step 7: Run all existing tests to check no regressions**

Run: `cargo test -p novarocks --lib 2>&1 | tail -20`

Expected: existing tests all pass.

- [ ] **Step 8: Commit**

```bash
git add src/connector/iceberg src/engine/sql_expr.rs src/engine/iceberg_ctas.rs src/engine/insert.rs
git commit -m "feat(types): fan SqlType::Bitmap/Hll through iceberg + sql_expr

Both new types resolve to PrimitiveType::Binary on the iceberg side and
to Arrow DataType::Binary at runtime, matching the existing convention
used by the to_bitmap / hll_serialize scalar implementations."
```

---

## Task 5: Add `ColumnAggregation::BitmapUnion` / `HllUnion`

**Files:**
- Modify: `src/sql/parser/ast/mod.rs:297-310` (enum)
- Modify: `src/sql/parser/dialect/create_table.rs:656-665` (parse_column_aggregation)
- Modify: `src/connector/starrocks/managed/ddl.rs:1078-1083` (ColumnAggregation → TAggregationType)
- Modify: `src/connector/iceberg/catalog/registry.rs:2330-2345` (ColumnAggregation roundtrip)
- Modify: `src/engine/aggregate.rs:88-160` (existing aggregate match — add explicit rejection for BitmapUnion/HllUnion since semantic merge for these columns lives in the reader path, not in `aggregate.rs`)

- [ ] **Step 1: Write failing parser test**

In `src/sql/parser/dialect/create_table.rs` `mod tests`:

```rust
#[test]
fn parse_aggregate_key_with_bitmap_hll_state_columns() {
    let sql = "CREATE TABLE foo (
        k INT,
        bm BITMAP BITMAP_UNION,
        hv HLL HLL_UNION
    ) AGGREGATE KEY(k) DISTRIBUTED BY HASH(k) BUCKETS 1";
    let stmt = parse_starrocks_create_table(sql).expect("parse must succeed");
    let columns = match &stmt.kind {
        CreateTableKind::Columns(cols) => cols,
        _ => panic!("expected Columns kind"),
    };
    assert_eq!(columns[1].aggregation, Some(ColumnAggregation::BitmapUnion));
    assert_eq!(columns[2].aggregation, Some(ColumnAggregation::HllUnion));
}
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `cargo test -p novarocks parse_aggregate_key_with_bitmap_hll_state_columns -- --nocapture 2>&1 | tail -15`

Expected: fails to parse or unknown variant.

- [ ] **Step 3: Add enum variants**

In `src/sql/parser/ast/mod.rs:297`:

```rust
pub(crate) enum ColumnAggregation {
    Sum,
    Min,
    Max,
    Replace,
    BitmapUnion,
    HllUnion,
}
```

- [ ] **Step 4: Extend `parse_column_aggregation`**

In `src/sql/parser/dialect/create_table.rs:656`:

```rust
fn parse_column_aggregation(parser: &mut Parser<'_>) -> Option<ColumnAggregation> {
    let aggregation = if peek_word_eq(parser, 0, "SUM") {
        Some(ColumnAggregation::Sum)
    } else if peek_word_eq(parser, 0, "MIN") {
        Some(ColumnAggregation::Min)
    } else if peek_word_eq(parser, 0, "MAX") {
        Some(ColumnAggregation::Max)
    } else if peek_word_eq(parser, 0, "REPLACE") {
        Some(ColumnAggregation::Replace)
    } else if peek_word_eq(parser, 0, "BITMAP_UNION") {
        Some(ColumnAggregation::BitmapUnion)
    } else if peek_word_eq(parser, 0, "HLL_UNION") {
        Some(ColumnAggregation::HllUnion)
    } else {
        None
    };
    if aggregation.is_some() {
        parser.next_token();
    }
    aggregation
}
```

(Match the existing function's structure; the precise control flow may differ — verify by reading lines 656-670 first.)

- [ ] **Step 5: Run the test to verify it passes**

Run: `cargo test -p novarocks parse_aggregate_key_with_bitmap_hll_state_columns -- --nocapture 2>&1 | tail -10`

Expected: PASS.

- [ ] **Step 6: Fan out downstream consumers**

Run: `cargo build 2>&1 | grep -E 'error|non-exhaustive' | grep ColumnAggregation | head -20`

Expected: 3 sites — `ddl.rs:1078`, `registry.rs:2330`, `aggregate.rs:88+`.

- [ ] **Step 7: Map ColumnAggregation → TAggregationType**

In `src/connector/starrocks/managed/ddl.rs:1078`:

```rust
ColumnAggregation::Sum => crate::types::TAggregationType::SUM,
ColumnAggregation::Min => crate::types::TAggregationType::MIN,
ColumnAggregation::Max => crate::types::TAggregationType::MAX,
ColumnAggregation::Replace => crate::types::TAggregationType::REPLACE,
ColumnAggregation::BitmapUnion => crate::types::TAggregationType::BITMAP_UNION,
ColumnAggregation::HllUnion => crate::types::TAggregationType::HLL_UNION,
```

If `TAggregationType::BITMAP_UNION` / `HLL_UNION` are not in `src/types/`, grep first:
```bash
grep -n "BITMAP_UNION\|HLL_UNION" src/types/*.rs
```
StarRocks `TAggregationType` thrift enum almost certainly has both; if not, use the integer discriminant per StarRocks `gen-cpp/AggregationType_types.h` (BITMAP_UNION=5, HLL_UNION=4).

- [ ] **Step 8: Add iceberg round-trip**

In `src/connector/iceberg/catalog/registry.rs:2330` (forward map) and `:2341` (reverse map):

```rust
// Forward
ColumnAggregation::Sum => "sum",
ColumnAggregation::Min => "min",
ColumnAggregation::Max => "max",
ColumnAggregation::Replace => "replace",
ColumnAggregation::BitmapUnion => "bitmap_union",
ColumnAggregation::HllUnion => "hll_union",
```

```rust
// Reverse
"sum" => Some(ColumnAggregation::Sum),
"min" => Some(ColumnAggregation::Min),
"max" => Some(ColumnAggregation::Max),
"replace" => Some(ColumnAggregation::Replace),
"bitmap_union" => Some(ColumnAggregation::BitmapUnion),
"hll_union" => Some(ColumnAggregation::HllUnion),
```

- [ ] **Step 9: Update `src/engine/aggregate.rs`**

Around line 88, `aggregate.rs` resolves columnar aggregation merges during in-memory aggregation paths. BITMAP_UNION / HLL_UNION are intentionally NOT handled here — they are merged via the reader-side `AggOp::BitmapUnion` path in `src/formats/starrocks/reader/model/agg.rs`. So make this explicit:

In the relevant match (around line 100), after the Replace arm:

```rust
ColumnAggregation::BitmapUnion | ColumnAggregation::HllUnion => {
    return Err(format!(
        "{:?} column aggregation is applied at storage read time, not at INSERT merge",
        agg
    ));
}
```

If this arm is hit at runtime it means an INSERT path is wrongly trying to merge BITMAP/HLL state in memory — fail fast and revisit.

- [ ] **Step 10: Run build and existing tests**

Run: `cargo build 2>&1 | grep error | head -5`

Expected: clean.

Run: `cargo test -p novarocks aggregate -- --nocapture 2>&1 | tail -10`

Expected: existing aggregate tests pass.

- [ ] **Step 11: Commit**

```bash
git add src/sql/parser src/connector/starrocks/managed/ddl.rs src/connector/iceberg src/engine/aggregate.rs
git commit -m "feat(parser): BITMAP_UNION / HLL_UNION column aggregations

Parser recognises both modifiers. Forward maps to TAggregationType
BITMAP_UNION / HLL_UNION on the storage side; iceberg property
round-trip uses the lowercase string form. In-memory aggregate path
explicitly rejects them since the union merge is a storage read-time
operation."
```

---

## Task 6: Analyzer-side fail-fast for BITMAP / HLL misuse

**Files:**
- Modify: `src/sql/analyzer/mod.rs` (search for the ORDER BY, GROUP BY, and PRIMARY KEY / DISTRIBUTED BY validation points)
- Modify: `src/sql/analyzer/resolve_expr.rs` (comparison operators)
- Modify: `src/connector/starrocks/managed/ddl.rs:949-1020` (PRIMARY KEY + DISTRIBUTED BY validation lives here for managed-lake CREATE TABLE)

- [ ] **Step 1: Write a failing test file for restrictions**

Create `sql-tests/function/sql/bitmap_hll_type_restrictions.sql`:

```sql
-- @skip_result_check=false

-- query 1: ORDER BY on BITMAP column must reject
CREATE TABLE ${case_db}.t1 (k INT, bm BITMAP) DUPLICATE KEY(k)
  DISTRIBUTED BY HASH(k) BUCKETS 1 PROPERTIES("replication_num"="1");

-- query 2: ORDER BY rejection
-- @expect_error
SELECT * FROM ${case_db}.t1 ORDER BY bm;

-- query 3: GROUP BY rejection
-- @expect_error
SELECT bm FROM ${case_db}.t1 GROUP BY bm;

-- query 4: comparison rejection
-- @expect_error
SELECT * FROM ${case_db}.t1 WHERE bm = bm;

-- query 5: PRIMARY KEY rejection
-- @expect_error
CREATE TABLE ${case_db}.t2 (k INT, bm BITMAP) PRIMARY KEY(bm)
  DISTRIBUTED BY HASH(k) BUCKETS 1 PROPERTIES("replication_num"="1");

-- query 6: DISTRIBUTED BY rejection
-- @expect_error
CREATE TABLE ${case_db}.t3 (k INT, bm BITMAP) DUPLICATE KEY(k)
  DISTRIBUTED BY HASH(bm) BUCKETS 1 PROPERTIES("replication_num"="1");
```

Note: confirm the sql-tests runner directive for "this step is expected to fail" — current convention may be `-- @expect_error=true` or similar. Read `sql-tests/function/sql/*.sql` and the runner README to verify.

- [ ] **Step 2: Run the case to see which checks already fire (some may pass-fail without the change)**

```bash
source docker/iceberg-rest/runtime/current/env.sh
# Start server (debug build)
NO_PROXY=127.0.0.1,localhost cargo run -- standalone-server \
  --config "$NOVAROCKS_STANDALONE_CONFIG" > /tmp/srv.log 2>&1 &
SRV=$!
for i in $(seq 1 60); do grep -q '^NOVAROCKS_READY ' /tmp/srv.log && break; sleep 1; done

cargo run --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests -- \
  --config "$NOVAROCKS_SQL_TEST_CONFIG" \
  --suite function --mode verify -j 1 \
  --only bitmap_hll_type_restrictions 2>&1 | tail -20

kill -9 $SRV
```

Expected: at least one of the 5 queries does NOT fail at the analyzer layer (i.e. they slip through and run, returning wrong results or panicking).

- [ ] **Step 3: Identify column-type reachability in analyzer**

To reject ORDER BY / GROUP BY / comparison on Bitmap/Hll, you need column-type information at the analyzer site. Use Read to inspect:

- `src/sql/analyzer/mod.rs` near line 387 (GROUP BY analysis) and line 1607 (ORDER BY analysis): scope-typed expressions already carry a resolved `SqlType` via the `scope` or `TypedExpr` structures used in this module. Verify by reading 50 lines around each match site.
- `src/sql/analyzer/resolve_expr.rs`: comparison binary op resolution.

If `SqlType` is reachable, add a check before the expression is accepted:

```rust
if matches!(expr_type, SqlType::Bitmap | SqlType::Hll) {
    return Err("BITMAP/HLL columns cannot appear in ORDER BY".to_string());
}
```

Adapt the message for each site.

- [ ] **Step 4: Add CREATE TABLE-side PRIMARY KEY + DISTRIBUTED BY rejection**

`src/connector/starrocks/managed/ddl.rs:949-1020` builds the table schema. `key_eligible_type` already returns `false` for Bitmap/Hll (Task 3). Confirm the function that walks `key_desc.columns` calls `key_eligible_type` and emits a clear error. If the existing error is generic (e.g. "column X type is not a valid key type"), enrich it to mention BITMAP/HLL specifically:

```rust
if !key_eligible_type(&column.data_type) {
    return Err(format!(
        "column `{}` of type {} cannot be a key column",
        column.name,
        logical_type_name(&column.data_type)
    ));
}
```

For DISTRIBUTED BY, the validation point is `resolve_managed_create_defaults` (around lines 118-220). Find where distribution columns are matched against schema and add a similar rejection for Bitmap/Hll.

- [ ] **Step 5: Run the restrictions case to verify**

Re-run the same command from Step 2.

Expected: all 5 reject-expected steps pass (i.e. produce an error), and step 1 (CREATE TABLE WITH BITMAP column) succeeds.

- [ ] **Step 6: Record the case**

```bash
cargo run --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests -- \
  --config "$NOVAROCKS_SQL_TEST_CONFIG" \
  --suite function --mode record -j 1 \
  --only bitmap_hll_type_restrictions
```

Verify the `.result` file is produced.

- [ ] **Step 7: Commit**

```bash
git add src/sql/analyzer/ src/connector/starrocks/managed/ddl.rs \
  sql-tests/function/sql/bitmap_hll_type_restrictions.sql \
  sql-tests/function/result/bitmap_hll_type_restrictions.result
git commit -m "feat(analyzer): reject BITMAP/HLL in ORDER BY / GROUP BY / comparison / key / distribution

5 fail-fast checks. Errors match StarRocks form: \"BITMAP/HLL columns
cannot appear in <context>\". Test suite: function/bitmap_hll_type_restrictions."
```

---

## Task 7: Add 5 binary BITMAP scalar functions

**Files:**
- Modify: `src/exec/expr/function/object/bitmap_functions.rs`
- Modify: `src/exec/expr/function/object/dispatch.rs:54-140`

- [ ] **Step 1: Write failing Rust unit tests**

`bitmap_common.rs` exposes:

- `decode_bitmap(&[u8]) -> Result<BTreeSet<u64>, String>` (universal decode covering all SeriV2 type bytes)
- `encode_internal_bitmap(&BTreeSet<u64>) -> Result<Vec<u8>, String>` (StarRocks internal SeriV2 form — used by `to_bitmap` etc.)

These operate on `BTreeSet<u64>`, which itself has `union` / `intersection` / `difference` / `symmetric_difference` set ops — no need for `roaring::RoaringTreemap` in the binary scalars.

Append to `src/exec/expr/function/object/bitmap_functions.rs` `mod tests` (or create one if missing):

```rust
#[cfg(test)]
mod bitmap_binary_op_tests {
    use super::*;
    use crate::exec::expr::function::object::bitmap_common::{
        decode_bitmap, encode_internal_bitmap,
    };
    use arrow::array::{ArrayRef, BinaryArray, BinaryBuilder, BooleanArray, Int64Array};
    use std::collections::BTreeSet;
    use std::sync::Arc;

    fn encode(values: &[u64]) -> Vec<u8> {
        let set: BTreeSet<u64> = values.iter().copied().collect();
        encode_internal_bitmap(&set).expect("encode")
    }

    fn binary_array(values: &[Option<Vec<u8>>]) -> ArrayRef {
        let mut b = BinaryBuilder::new();
        for v in values {
            match v {
                Some(bs) => b.append_value(bs),
                None => b.append_null(),
            }
        }
        Arc::new(b.finish()) as ArrayRef
    }

    fn decode_row(arr: &BinaryArray, row: usize) -> Vec<u64> {
        decode_bitmap(arr.value(row)).expect("decode").into_iter().collect()
    }

    #[test]
    fn bitmap_or_basic() {
        let lhs = binary_array(&[Some(encode(&[1, 2])), Some(encode(&[]))]);
        let rhs = binary_array(&[Some(encode(&[3])), Some(encode(&[42]))]);
        let out = eval_bitmap_or_arrays(&lhs, &rhs).expect("or");
        let arr = out.as_any().downcast_ref::<BinaryArray>().unwrap();
        assert_eq!(decode_row(arr, 0), vec![1, 2, 3]);
        assert_eq!(decode_row(arr, 1), vec![42]);
    }

    #[test]
    fn bitmap_xor_basic() {
        let lhs = binary_array(&[Some(encode(&[1, 2, 3]))]);
        let rhs = binary_array(&[Some(encode(&[2, 3, 4]))]);
        let out = eval_bitmap_xor_arrays(&lhs, &rhs).expect("xor");
        let arr = out.as_any().downcast_ref::<BinaryArray>().unwrap();
        assert_eq!(decode_row(arr, 0), vec![1, 4]);
    }

    #[test]
    fn bitmap_andnot_basic() {
        let lhs = binary_array(&[Some(encode(&[1, 2, 3]))]);
        let rhs = binary_array(&[Some(encode(&[2]))]);
        let out = eval_bitmap_andnot_arrays(&lhs, &rhs).expect("andnot");
        let arr = out.as_any().downcast_ref::<BinaryArray>().unwrap();
        assert_eq!(decode_row(arr, 0), vec![1, 3]);
    }

    #[test]
    fn bitmap_intersect_scalar_basic() {
        let lhs = binary_array(&[Some(encode(&[1, 2, 3]))]);
        let rhs = binary_array(&[Some(encode(&[2, 3, 4]))]);
        let out = eval_bitmap_intersect_arrays(&lhs, &rhs).expect("intersect");
        let arr = out.as_any().downcast_ref::<BinaryArray>().unwrap();
        assert_eq!(decode_row(arr, 0), vec![2, 3]);
    }

    #[test]
    fn bitmap_contains_basic() {
        let lhs = binary_array(&[Some(encode(&[1, 5, 9])), Some(encode(&[1, 5, 9]))]);
        let rhs = Arc::new(Int64Array::from(vec![5, 2])) as ArrayRef;
        let out = eval_bitmap_contains_arrays(&lhs, &rhs).expect("contains");
        let arr = out.as_any().downcast_ref::<BooleanArray>().unwrap();
        assert!(arr.value(0));
        assert!(!arr.value(1));
    }

    #[test]
    fn bitmap_or_propagates_nulls() {
        let lhs = binary_array(&[None]);
        let rhs = binary_array(&[Some(encode(&[1]))]);
        let out = eval_bitmap_or_arrays(&lhs, &rhs).expect("or");
        let arr = out.as_any().downcast_ref::<BinaryArray>().unwrap();
        assert!(arr.is_null(0));
    }
}
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `cargo test -p novarocks bitmap_binary_op_tests 2>&1 | tail -20`

Expected: fails because the functions don't exist.

- [ ] **Step 3: Implement the helpers + eval functions**

In `src/exec/expr/function/object/bitmap_functions.rs`:

```rust
use crate::exec::expr::function::object::bitmap_common::{
    decode_bitmap, encode_internal_bitmap,
};
use arrow::array::{ArrayRef, BinaryArray, BinaryBuilder, BooleanBuilder, Int64Array};
use std::collections::BTreeSet;
use std::sync::Arc;

fn bitmap_binary_op(
    lhs: &ArrayRef,
    rhs: &ArrayRef,
    op: impl Fn(&BTreeSet<u64>, &BTreeSet<u64>) -> BTreeSet<u64>,
) -> Result<ArrayRef, String> {
    let lhs = lhs
        .as_any()
        .downcast_ref::<BinaryArray>()
        .ok_or_else(|| "bitmap op expects BITMAP/BINARY input".to_string())?;
    let rhs = rhs
        .as_any()
        .downcast_ref::<BinaryArray>()
        .ok_or_else(|| "bitmap op expects BITMAP/BINARY input".to_string())?;
    if lhs.len() != rhs.len() {
        return Err(format!(
            "bitmap op length mismatch: lhs={} rhs={}",
            lhs.len(),
            rhs.len()
        ));
    }
    let mut out = BinaryBuilder::new();
    for i in 0..lhs.len() {
        if lhs.is_null(i) || rhs.is_null(i) {
            out.append_null();
            continue;
        }
        let a = decode_bitmap(lhs.value(i))?;
        let b = decode_bitmap(rhs.value(i))?;
        let merged = op(&a, &b);
        out.append_value(encode_internal_bitmap(&merged)?);
    }
    Ok(Arc::new(out.finish()) as ArrayRef)
}

pub(crate) fn eval_bitmap_or_arrays(lhs: &ArrayRef, rhs: &ArrayRef) -> Result<ArrayRef, String> {
    bitmap_binary_op(lhs, rhs, |a, b| a.union(b).copied().collect())
}

pub(crate) fn eval_bitmap_xor_arrays(lhs: &ArrayRef, rhs: &ArrayRef) -> Result<ArrayRef, String> {
    bitmap_binary_op(lhs, rhs, |a, b| a.symmetric_difference(b).copied().collect())
}

pub(crate) fn eval_bitmap_andnot_arrays(lhs: &ArrayRef, rhs: &ArrayRef) -> Result<ArrayRef, String> {
    bitmap_binary_op(lhs, rhs, |a, b| a.difference(b).copied().collect())
}

pub(crate) fn eval_bitmap_intersect_arrays(lhs: &ArrayRef, rhs: &ArrayRef) -> Result<ArrayRef, String> {
    bitmap_binary_op(lhs, rhs, |a, b| a.intersection(b).copied().collect())
}

pub(crate) fn eval_bitmap_contains_arrays(
    lhs: &ArrayRef,
    rhs: &ArrayRef,
) -> Result<ArrayRef, String> {
    let lhs = lhs
        .as_any()
        .downcast_ref::<BinaryArray>()
        .ok_or_else(|| "bitmap_contains expects BITMAP/BINARY input for arg 1".to_string())?;
    let rhs = rhs
        .as_any()
        .downcast_ref::<Int64Array>()
        .ok_or_else(|| "bitmap_contains expects BIGINT input for arg 2".to_string())?;
    if lhs.len() != rhs.len() {
        return Err(format!(
            "bitmap_contains length mismatch: lhs={} rhs={}",
            lhs.len(),
            rhs.len()
        ));
    }
    let mut out = BooleanBuilder::new();
    for i in 0..lhs.len() {
        if lhs.is_null(i) || rhs.is_null(i) {
            out.append_null();
            continue;
        }
        let a = decode_bitmap(lhs.value(i))?;
        let v = rhs.value(i);
        out.append_value(v >= 0 && a.contains(&(v as u64)));
    }
    Ok(Arc::new(out.finish()) as ArrayRef)
}

pub fn eval_bitmap_or(
    arena: &ExprArena,
    _expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
) -> Result<ArrayRef, String> {
    if args.len() != 2 {
        return Err(format!("bitmap_or expects 2 arguments, got {}", args.len()));
    }
    let lhs = arena.eval(args[0], chunk)?;
    let rhs = arena.eval(args[1], chunk)?;
    eval_bitmap_or_arrays(&lhs, &rhs)
}

pub fn eval_bitmap_xor(
    arena: &ExprArena,
    _expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
) -> Result<ArrayRef, String> {
    if args.len() != 2 {
        return Err(format!("bitmap_xor expects 2 arguments, got {}", args.len()));
    }
    let lhs = arena.eval(args[0], chunk)?;
    let rhs = arena.eval(args[1], chunk)?;
    eval_bitmap_xor_arrays(&lhs, &rhs)
}

pub fn eval_bitmap_andnot(
    arena: &ExprArena,
    _expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
) -> Result<ArrayRef, String> {
    if args.len() != 2 {
        return Err(format!("bitmap_andnot expects 2 arguments, got {}", args.len()));
    }
    let lhs = arena.eval(args[0], chunk)?;
    let rhs = arena.eval(args[1], chunk)?;
    eval_bitmap_andnot_arrays(&lhs, &rhs)
}

pub fn eval_bitmap_intersect(
    arena: &ExprArena,
    _expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
) -> Result<ArrayRef, String> {
    if args.len() != 2 {
        return Err(format!("bitmap_intersect expects 2 arguments, got {}", args.len()));
    }
    let lhs = arena.eval(args[0], chunk)?;
    let rhs = arena.eval(args[1], chunk)?;
    eval_bitmap_intersect_arrays(&lhs, &rhs)
}

pub fn eval_bitmap_contains(
    arena: &ExprArena,
    _expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
) -> Result<ArrayRef, String> {
    if args.len() != 2 {
        return Err(format!("bitmap_contains expects 2 arguments, got {}", args.len()));
    }
    let lhs = arena.eval(args[0], chunk)?;
    let rhs = arena.eval(args[1], chunk)?;
    eval_bitmap_contains_arrays(&lhs, &rhs)
}
```

Notes:
- `decode_bitmap` handles all SeriV2 type bytes (single32/single64/bitmap32/bitmap64/set + SeriV2 variants).
- `encode_internal_bitmap` chooses the optimal type byte based on element count and max value, matching what `to_bitmap` produces.
- Set ops are done on `BTreeSet<u64>` which is cheap for the cardinalities expected in these tests; for production-scale BITMAP merges the aggregate path (`bitmap_union_int.rs`) is already optimized — these scalar functions are correctness-first.

- [ ] **Step 4: Run tests to verify they pass**

Run: `cargo test -p novarocks bitmap_binary_op_tests 2>&1 | tail -15`

Expected: PASS for all 6 tests.

- [ ] **Step 5: Register the 5 functions in dispatch.rs**

In `src/exec/expr/function/object/dispatch.rs`, in the `match canonical` block (line 54), add 5 arms:

```rust
"bitmap_or" => super::bitmap_functions::eval_bitmap_or(arena, expr, args, chunk),
"bitmap_xor" => super::bitmap_functions::eval_bitmap_xor(arena, expr, args, chunk),
"bitmap_andnot" => super::bitmap_functions::eval_bitmap_andnot(arena, expr, args, chunk),
"bitmap_intersect" => super::bitmap_functions::eval_bitmap_intersect(arena, expr, args, chunk),
"bitmap_contains" => super::bitmap_functions::eval_bitmap_contains(arena, expr, args, chunk),
```

In `OBJECT_FUNCTIONS` (line 109), add 5 alias entries:

```rust
("bitmap_or", "bitmap_or"),
("bitmap_xor", "bitmap_xor"),
("bitmap_andnot", "bitmap_andnot"),
("bitmap_intersect", "bitmap_intersect"),
("bitmap_contains", "bitmap_contains"),
```

In `OBJECT_METADATA`, add 5 entries:

```rust
FunctionMeta { name: "bitmap_or",        min_args: 2, max_args: 2 },
FunctionMeta { name: "bitmap_xor",       min_args: 2, max_args: 2 },
FunctionMeta { name: "bitmap_andnot",    min_args: 2, max_args: 2 },
FunctionMeta { name: "bitmap_intersect", min_args: 2, max_args: 2 },
FunctionMeta { name: "bitmap_contains",  min_args: 2, max_args: 2 },
```

- [ ] **Step 6: Build to confirm everything compiles**

Run: `cargo build 2>&1 | grep error | head -5`

Expected: clean.

- [ ] **Step 7: Add SQL-level coverage**

Create `sql-tests/function/sql/bitmap_binary_ops.sql`:

```sql
-- query 1
-- @order_sensitive=true
SELECT bitmap_to_string(bitmap_or(to_bitmap(1), to_bitmap(2)));

-- query 2
-- @order_sensitive=true
SELECT bitmap_to_string(bitmap_xor(bitmap_from_string('1,2,3'), bitmap_from_string('2,3,4')));

-- query 3
-- @order_sensitive=true
SELECT bitmap_to_string(bitmap_andnot(bitmap_from_string('1,2,3'), bitmap_from_string('2')));

-- query 4
-- @order_sensitive=true
SELECT bitmap_to_string(bitmap_intersect(bitmap_from_string('1,2,3'), bitmap_from_string('2,3,4')));

-- query 5
SELECT bitmap_contains(to_bitmap(1), 1), bitmap_contains(to_bitmap(1), 2);

-- query 6
-- NULL propagation
SELECT bitmap_or(NULL, to_bitmap(1)) IS NULL;
```

- [ ] **Step 8: Record the case**

```bash
source docker/iceberg-rest/runtime/current/env.sh
NO_PROXY=127.0.0.1,localhost cargo run -- standalone-server \
  --config "$NOVAROCKS_STANDALONE_CONFIG" > /tmp/srv.log 2>&1 &
SRV=$!
for i in $(seq 1 60); do grep -q '^NOVAROCKS_READY ' /tmp/srv.log && break; sleep 1; done

cargo run --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests -- \
  --config "$NOVAROCKS_SQL_TEST_CONFIG" \
  --suite function --mode record -j 1 \
  --only bitmap_binary_ops

cargo run --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests -- \
  --config "$NOVAROCKS_SQL_TEST_CONFIG" \
  --suite function --mode verify -j 1 \
  --only bitmap_binary_ops

kill -9 $SRV
```

Expected: record completes; verify passes.

- [ ] **Step 9: Commit**

```bash
git add src/exec/expr/function/object/bitmap_functions.rs \
  src/exec/expr/function/object/dispatch.rs \
  sql-tests/function/sql/bitmap_binary_ops.sql \
  sql-tests/function/result/bitmap_binary_ops.result
git commit -m "feat(bitmap): add bitmap_or / xor / andnot / contains / intersect scalars

Round-trip through StarRocks SeriV2 binary. Rust unit tests cover
overlap / disjoint / null cases. SQL coverage: function/bitmap_binary_ops."
```

---

## Task 8: Record `analytic_test_bitmap_union_window`

**Files:**
- Modify: `sql-tests/analytic/result/analytic_test_bitmap_union_window.result` (re-record)

- [ ] **Step 1: Run the case in verify mode to confirm it fails before recording**

```bash
source docker/iceberg-rest/runtime/current/env.sh
NO_PROXY=127.0.0.1,localhost cargo run -- standalone-server \
  --config "$NOVAROCKS_STANDALONE_CONFIG" > /tmp/srv.log 2>&1 &
SRV=$!
for i in $(seq 1 60); do grep -q '^NOVAROCKS_READY ' /tmp/srv.log && break; sleep 1; done

cargo run --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests -- \
  --config "$NOVAROCKS_SQL_TEST_CONFIG" \
  --suite analytic --mode verify -j 1 \
  --only analytic_test_bitmap_union_window 2>&1 | tail -25
```

Expected: Either (a) all 5 steps pass — done; or (b) step 1 (CREATE TABLE) now succeeds where it didn't before, and downstream steps may need re-recording.

- [ ] **Step 2: Re-record if needed**

If verify shows a row-count or value mismatch (because the previous result file was recorded against the failure case), re-record:

```bash
cargo run --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests -- \
  --config "$NOVAROCKS_SQL_TEST_CONFIG" \
  --suite analytic --mode record -j 1 \
  --only analytic_test_bitmap_union_window
```

- [ ] **Step 3: Verify the recording**

```bash
cargo run --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests -- \
  --config "$NOVAROCKS_SQL_TEST_CONFIG" \
  --suite analytic --mode verify -j 1 \
  --only analytic_test_bitmap_union_window 2>&1 | tail -10

kill -9 $SRV
```

Expected: 5/5 pass.

- [ ] **Step 4: Also probe `analytic_test_window_hll_bitmap` (informational)**

```bash
cargo run --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests -- \
  --config "$NOVAROCKS_SQL_TEST_CONFIG" \
  --suite analytic --mode verify -j 1 \
  --only analytic_test_window_hll_bitmap 2>&1 | tail -30
```

If it passes: re-record it and add to commit.
If it fails at `INSERT VALUES (..., to_bitmap(1001), hll_hash(5))`: that is the INT-3 dependency from §7 of the spec. Do NOT record; leave a comment in the PR description.

- [ ] **Step 5: Commit**

```bash
git add sql-tests/analytic/result/analytic_test_bitmap_union_window.result
# include analytic_test_window_hll_bitmap.result only if step 4 passed
git commit -m "test(analytic): record analytic_test_bitmap_union_window result

BITMAP type registration + bitmap_union window function now produce
deterministic output for the global / partitioned / ordered window cases."
```

---

## Task 9: Full-suite sanity sweep

- [ ] **Step 1: Run analytic suite to catch regressions**

```bash
cargo run --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests -- \
  --config "$NOVAROCKS_SQL_TEST_CONFIG" \
  --suite analytic --mode verify 2>&1 | tail -10
```

Expected: no new failures versus baseline before this work.

- [ ] **Step 2: Run function suite to catch regressions**

```bash
cargo run --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests -- \
  --config "$NOVAROCKS_SQL_TEST_CONFIG" \
  --suite function --mode verify 2>&1 | tail -10
```

Expected: no new failures versus baseline.

- [ ] **Step 3: cargo clippy clean**

```bash
cargo clippy --all-targets 2>&1 | grep -E "warning|error" | head -20
```

Expected: no new clippy violations from this PR's files.

- [ ] **Step 4: cargo fmt clean**

```bash
cargo fmt
git diff --stat
```

If `git diff` shows changes, `git add . && git commit -m "style: cargo fmt"`.

---

## Risks called out in design

- `iceberg_ctas.rs:319` collapses Arrow `Binary` → `SqlType::Binary` and does not preserve Bitmap/Hll. This is correct because Arrow has no extension type for them at read time; CTAS from a BITMAP source column will land as `BINARY` in the target schema. Documented in spec §7. Out of scope for this PR.
- The reader-side merge for `AggregateKey` tables already supports `AggOp::BitmapUnion` but not yet `HllUnion`. If `cargo grep AggOp::HllUnion` returns 0 matches and `analytic_test_window_hll_bitmap` is needed, that's an INT-3 follow-up — not blocking PR-B2.

## Done criteria

1. `CREATE TABLE foo (k INT, bm BITMAP, hv HLL)` succeeds.
2. `CREATE TABLE foo (k INT, bm BITMAP BITMAP_UNION, hv HLL HLL_UNION) AGGREGATE KEY(k)` parses.
3. `analytic_test_bitmap_union_window` 5/5 verify pass.
4. `function/bitmap_binary_ops.sql` 6/6 pass.
5. `function/bitmap_hll_type_restrictions.sql` 5+ fail-fast checks all reject as expected.
6. `cargo clippy` / `cargo fmt --check` clean.
7. Analytic + function suites no new regressions.
