# write-path → iceberg-dml Suite Migration Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a new `iceberg-dml` SQL test suite that exercises NovaRocks's Iceberg v3 backend with the surviving DML test points from `write-path`, then delete `write-path` entirely.

**Architecture:** A new directory `sql-tests/iceberg-dml/` (init/cleanup/sql/result) registers an external Iceberg catalog on the same hadoop+MinIO fixture used by `sql-tests/iceberg`. Twenty-three cases are rewritten from `write-path` (StarRocks OLAP DDL stripped, mutation cases declare `format-version = 3`). Fifteen StarRocks-only cases are dropped. The runner is updated in one line to inherit hadoop-catalog placeholder defaults for the new suite name.

**Tech Stack:** Rust runner (`tests/sql-test-runner`), SQL test suites under `sql-tests/`, Iceberg hadoop catalog over MinIO via `docker/iceberg-rest`, NovaRocks `standalone-server`.

**Spec:** [docs/superpowers/specs/2026-05-22-iceberg-dml-migration-design.md](../specs/2026-05-22-iceberg-dml-migration-design.md)

**No-fallback rule** (from spec §8): if any migration step hits a feature NovaRocks does not support on Iceberg v3, **stop and report the exact statement + error to the user**. Do not rewrite the case into an equivalent that bypasses the missing feature.

---

## Prerequisites

Before starting any task:

1. Bring up the local Iceberg + MinIO fixture and source the env file:

   ```bash
   docker/iceberg-rest/up.sh
   source docker/iceberg-rest/runtime/current/env.sh
   ```

2. Build NovaRocks debug binary:

   ```bash
   cargo build
   ```

3. Start `standalone-server` in the background, gated on the readiness marker:

   ```bash
   LOG=/tmp/novarocks-icedml.log
   NO_PROXY=127.0.0.1,localhost target/debug/novarocks standalone-server \
     --config "$NOVAROCKS_STANDALONE_CONFIG" >"$LOG" 2>&1 &
   SRV_PID=$!
   for i in $(seq 1 60); do
     if grep -q '^NOVAROCKS_READY ' "$LOG"; then break; fi
     if ! kill -0 "$SRV_PID" 2>/dev/null; then
       echo "standalone-server died during startup; tail of $LOG:" >&2
       tail -20 "$LOG" >&2
       exit 1
     fi
     sleep 1
   done
   grep -q '^NOVAROCKS_READY ' "$LOG" || { echo "timed out waiting for NOVAROCKS_READY" >&2; kill -9 "$SRV_PID"; exit 1; }
   ```

4. Common helper variable (used in every record/verify command below):

   ```bash
   SQLT="cargo run --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests --"
   ```

The server stays up across tasks. Restart it only if it crashes.

---

## Task 1: Scaffold the iceberg-dml suite

**Files:**
- Create: `sql-tests/iceberg-dml/init.sql`
- Create: `sql-tests/iceberg-dml/cleanup.sql`
- Create: `sql-tests/iceberg-dml/sql/.gitkeep`
- Create: `sql-tests/iceberg-dml/result/.gitkeep`
- Modify: `tests/sql-test-runner/src/config.rs:147`

- [ ] **Step 1: Create `sql-tests/iceberg-dml/init.sql`**

```sql
-- @catalog=iceberg_dml_cat_${suite_uuid0}
CREATE EXTERNAL CATALOG IF NOT EXISTS `iceberg_dml_cat_${suite_uuid0}`
PROPERTIES (
    "type"="iceberg",
    "iceberg.catalog.type"="${iceberg_catalog_type}",
    "iceberg.catalog.warehouse"="${iceberg_catalog_warehouse}",
    "aws.s3.access_key"="${oss_ak}",
    "aws.s3.secret_key"="${oss_sk}",
    "aws.s3.endpoint"="${oss_endpoint}",
    "aws.s3.enable_path_style_access"="true"
);
```

- [ ] **Step 2: Create `sql-tests/iceberg-dml/cleanup.sql`**

```sql
DROP CATALOG IF EXISTS `iceberg_dml_cat_${suite_uuid0}`;
```

- [ ] **Step 3: Create empty placeholder dirs**

```bash
mkdir -p sql-tests/iceberg-dml/sql sql-tests/iceberg-dml/result
touch sql-tests/iceberg-dml/sql/.gitkeep sql-tests/iceberg-dml/result/.gitkeep
```

- [ ] **Step 4: Register `iceberg-dml` in placeholder defaults**

Open [tests/sql-test-runner/src/config.rs:147](../../tests/sql-test-runner/src/config.rs) and change the existing hadoop-catalog match arm from:

```rust
        "iceberg" | "iceberg-ivm" | "mv-on-iceberg" => {
```

to:

```rust
        "iceberg" | "iceberg-dml" | "iceberg-ivm" | "mv-on-iceberg" => {
```

- [ ] **Step 5: Build runner to confirm the edit compiles**

```bash
cargo build --manifest-path tests/sql-test-runner/Cargo.toml
```

Expected: success, no warnings about the new arm.

- [ ] **Step 6: Confirm the suite is auto-discovered**

```bash
$SQLT --suite iceberg-dml --list
```

Expected: prints "no cases" (sql/ is empty) but does not error with "suite not found".

- [ ] **Step 7: Commit**

```bash
git add sql-tests/iceberg-dml tests/sql-test-runner/src/config.rs
git commit -m "test(iceberg-dml): scaffold suite with hadoop catalog init/cleanup"
```

---

## Task 2: Smoke-test the pipeline with one trivial INSERT/SELECT case

This is the de-risking step: confirm catalog creation, per-case `${case_db}` provisioning, and result recording all work end-to-end before writing any other cases.

**Files:**
- Create: `sql-tests/iceberg-dml/sql/create_insert_select.sql`
- Create: `sql-tests/iceberg-dml/result/create_insert_select.result`

- [ ] **Step 1: Write `sql-tests/iceberg-dml/sql/create_insert_select.sql`**

```sql
-- @order_sensitive=true
-- @tags=iceberg_dml
-- Test Objective:
-- 1. Validate CREATE TABLE and multi-row INSERT for primitive + nullable values on Iceberg.
-- 2. Validate deterministic read-back for inserted rows.
DROP TABLE IF EXISTS ${case_db}.t_basic;
CREATE TABLE ${case_db}.t_basic (
  id BIGINT,
  name STRING,
  qty BIGINT
);
INSERT INTO ${case_db}.t_basic VALUES
  (1, 'apple', 10),
  (2, 'banana', 20),
  (3, 'banana', NULL);
SELECT id, name, qty
FROM ${case_db}.t_basic
ORDER BY id;
```

- [ ] **Step 2: Record the result**

```bash
$SQLT --suite iceberg-dml --only create_insert_select --mode record
```

Expected: success message; `sql-tests/iceberg-dml/result/create_insert_select.result` created.

- [ ] **Step 3: Inspect the recorded result**

```bash
cat sql-tests/iceberg-dml/result/create_insert_select.result
```

Expected: 3 rows; row order `1, 2, 3`; row 3 has NULL `qty`. If anything else, stop and report.

- [ ] **Step 4: Verify the result reproduces**

```bash
$SQLT --suite iceberg-dml --only create_insert_select --mode verify
```

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add sql-tests/iceberg-dml/sql/create_insert_select.sql sql-tests/iceberg-dml/result/create_insert_select.result
git rm sql-tests/iceberg-dml/sql/.gitkeep sql-tests/iceberg-dml/result/.gitkeep
git commit -m "test(iceberg-dml): add create_insert_select smoke case"
```

---

## Task 3: Smoke-test the three risky features early

Goal: confirm MERGE INTO, TRUNCATE TABLE, and identity-partition INSERT all work on Iceberg v3 in the standalone engine **before** writing the cases that depend on them. If any rejects, stop and report.

**Files:** none (transient SQL via `mysql` client)

- [ ] **Step 1: Open a mysql session against standalone-server**

```bash
mysql -h 127.0.0.1 -P "$NOVA_ENV_MYSQL_PORT" -u root --protocol=TCP
```

- [ ] **Step 2: Set up a scratch catalog + database**

```sql
CREATE EXTERNAL CATALOG IF NOT EXISTS iceberg_smoke
PROPERTIES (
    "type"="iceberg",
    "iceberg.catalog.type"="hadoop",
    "iceberg.catalog.warehouse"="${iceberg_catalog_warehouse}",  -- expand by hand from env.sh
    "aws.s3.access_key"="admin",
    "aws.s3.secret_key"="admin123",
    "aws.s3.endpoint"="http://127.0.0.1:9000",
    "aws.s3.enable_path_style_access"="true"
);
CREATE DATABASE iceberg_smoke.smoke_db;
```

Expected: both succeed. (Adjust `iceberg.catalog.warehouse` to whatever `$iceberg_catalog_warehouse` resolves to in your env — `oss://novarocks/iceberg-catalog/` by default.)

- [ ] **Step 3: Smoke-test MERGE INTO on a v3 table**

```sql
CREATE TABLE iceberg_smoke.smoke_db.t_merge (
  id INT, v INT
) TBLPROPERTIES ("format-version" = "3");
INSERT INTO iceberg_smoke.smoke_db.t_merge VALUES (1, 10), (2, 20);
CREATE TABLE iceberg_smoke.smoke_db.t_merge_src (
  id INT, v INT
) TBLPROPERTIES ("format-version" = "3");
INSERT INTO iceberg_smoke.smoke_db.t_merge_src VALUES (2, 22), (3, 33);
MERGE INTO iceberg_smoke.smoke_db.t_merge AS t
  USING iceberg_smoke.smoke_db.t_merge_src AS s
  ON t.id = s.id
  WHEN MATCHED THEN UPDATE SET v = s.v
  WHEN NOT MATCHED THEN INSERT (id, v) VALUES (s.id, s.v);
SELECT id, v FROM iceberg_smoke.smoke_db.t_merge ORDER BY id;
```

Expected: 3 rows — `(1, 10), (2, 22), (3, 33)`. If MERGE is rejected (parser error, executor error, "not supported"), copy the exact statement + error and **stop the plan here**. Report to the user; do not silently rewrite Task 9.

- [ ] **Step 4: Smoke-test TRUNCATE TABLE on a v3 table**

```sql
CREATE TABLE iceberg_smoke.smoke_db.t_trunc (
  k INT, v STRING
) TBLPROPERTIES ("format-version" = "3");
INSERT INTO iceberg_smoke.smoke_db.t_trunc VALUES (1, 'a'), (2, 'b');
SELECT COUNT(*) FROM iceberg_smoke.smoke_db.t_trunc;  -- expect 2
TRUNCATE TABLE iceberg_smoke.smoke_db.t_trunc;
SELECT COUNT(*) FROM iceberg_smoke.smoke_db.t_trunc;  -- expect 0
INSERT INTO iceberg_smoke.smoke_db.t_trunc VALUES (10, 'z');
SELECT k, v FROM iceberg_smoke.smoke_db.t_trunc ORDER BY k;  -- expect (10, 'z')
```

Expected: counts as commented. If TRUNCATE is rejected, stop and report.

- [ ] **Step 5: Smoke-test identity-partition INSERT on a v3 table**

```sql
CREATE TABLE iceberg_smoke.smoke_db.t_part (
  p BIGINT, k BIGINT, v BIGINT
)
PARTITION BY identity(p)
TBLPROPERTIES ("format-version" = "3");
INSERT INTO iceberg_smoke.smoke_db.t_part VALUES (1, 1, 10), (2, 1, 20);
SELECT p, k, v FROM iceberg_smoke.smoke_db.t_part ORDER BY p, k;
```

Expected: 2 rows. If `PARTITION BY identity(p)` or the partitioned INSERT is rejected, stop and report.

- [ ] **Step 6: Clean up scratch catalog**

```sql
DROP DATABASE iceberg_smoke.smoke_db FORCE;
DROP CATALOG iceberg_smoke;
```

- [ ] **Step 7: No commit**

This task produces no committed artifacts. Proceed to Task 4 only if all three smoke checks passed.

---

## Task 4: Migrate basic DML cases (5 cases)

**Files:**
- Create: `sql-tests/iceberg-dml/sql/create_database.sql`
- Create: `sql-tests/iceberg-dml/sql/null_insert_select.sql`
- Create: `sql-tests/iceberg-dml/sql/int_insert_cast.sql`
- Create: `sql-tests/iceberg-dml/sql/insert_values_select_repeated.sql`
- Create: `sql-tests/iceberg-dml/sql/group_by_aggregate.sql`
- Plus corresponding `result/*.result` files

- [ ] **Step 1: Verify the `information_schema` route for `create_database` before writing**

In the same mysql session, run both candidates and pick whichever returns 1:

```sql
SELECT COUNT(*) FROM iceberg_dml_cat_${suite_uuid0}.information_schema.schemata
  WHERE schema_name = '<some existing db in iceberg_dml_cat>';
SHOW DATABASES FROM iceberg_dml_cat_${suite_uuid0};
```

(Use a manually-created db name in `iceberg_smoke` for the test or create one in `iceberg_dml_cat_*` first if it exists.)

If `information_schema.schemata` returns the database, use form A below. If only `SHOW DATABASES` lists it, use form B. If neither works, stop and report.

- [ ] **Step 2: Write `sql-tests/iceberg-dml/sql/create_database.sql`**

Form A (preferred if information_schema works):

```sql
-- @order_sensitive=true
-- @tags=iceberg_dml
-- Test Objective:
-- 1. Validate runner auto-creates ${case_db_3} under the iceberg catalog before execution.
-- 2. Validate metadata visibility immediately after database creation.
SELECT COUNT(*) AS db_exists
FROM iceberg_dml_cat_${suite_uuid0}.information_schema.schemata
WHERE schema_name = '${case_db_3}';
```

Form B (fallback only if Step 1 proved information_schema doesn't list it):

```sql
-- @order_sensitive=true
-- @tags=iceberg_dml
-- Test Objective:
-- 1. Validate runner auto-creates ${case_db_3} under the iceberg catalog before execution.
-- @result_contains=${case_db_3}
SHOW DATABASES FROM iceberg_dml_cat_${suite_uuid0};
```

- [ ] **Step 3: Write `sql-tests/iceberg-dml/sql/null_insert_select.sql`**

```sql
-- @order_sensitive=true
-- @tags=iceberg_dml,null
-- Test Objective:
-- 1. Validate NULL values are preserved across INSERT-SELECT into typed sink columns.
-- 2. Validate mixed NULL/non-NULL rows across INT/STRING/DECIMAL/DATETIME columns.
DROP TABLE IF EXISTS ${case_db}.t_null_insert_src;
DROP TABLE IF EXISTS ${case_db}.t_null_insert_sink;
CREATE TABLE ${case_db}.t_null_insert_src (
  id BIGINT,
  c_int BIGINT,
  c_str STRING,
  c_dec DECIMAL(9, 2),
  c_dt DATETIME
);
CREATE TABLE ${case_db}.t_null_insert_sink (
  id INT,
  c_int INT,
  c_str STRING,
  c_dec DECIMAL(9, 2),
  c_dt DATETIME
);
INSERT INTO ${case_db}.t_null_insert_src VALUES
  (1, NULL, NULL, NULL, NULL),
  (2, 20, 'ok', 12.30, '2024-01-02 03:04:05'),
  (3, NULL, 'tail', NULL, '2024-06-01 00:00:00');
INSERT INTO ${case_db}.t_null_insert_sink
SELECT id, c_int, c_str, c_dec, c_dt
FROM ${case_db}.t_null_insert_src;
SELECT id, c_int, c_str, c_dec, c_dt
FROM ${case_db}.t_null_insert_sink
ORDER BY id;
```

- [ ] **Step 4: Write `sql-tests/iceberg-dml/sql/int_insert_cast.sql`**

```sql
-- @order_sensitive=true
-- @tags=iceberg_dml,cast
-- Test Objective:
-- 1. Regression coverage for writing INT target columns through Iceberg sink.
-- 2. Validate implicit type alignment from INT64-producing expressions to INT schema.
DROP TABLE IF EXISTS ${case_db}.t_int_insert_regression;
DROP TABLE IF EXISTS ${case_db}.t_int_insert_src;
CREATE TABLE ${case_db}.t_int_insert_regression (
  id INT,
  v INT
);
CREATE TABLE ${case_db}.t_int_insert_src (
  id BIGINT,
  v BIGINT
);
SET @i = 1;
INSERT INTO ${case_db}.t_int_insert_regression VALUES (@i, @i);
INSERT INTO ${case_db}.t_int_insert_regression VALUES (2, 2);
INSERT INTO ${case_db}.t_int_insert_src VALUES (3, 3);
INSERT INTO ${case_db}.t_int_insert_regression
SELECT id, v
FROM ${case_db}.t_int_insert_src;
SELECT id, v
FROM ${case_db}.t_int_insert_regression
ORDER BY id;
```

Note: `SET @i = 1` is a MySQL-style user variable.  If the standalone server
rejects it, **stop and report** — do not rewrite the case to `VALUES (1, 1)`.

- [ ] **Step 5: Write `sql-tests/iceberg-dml/sql/insert_values_select_repeated.sql`**

```sql
-- @order_sensitive=false
-- @tags=iceberg_dml,insert_values
-- Test Objective:
-- 1. Validate repeated INSERT ... VALUES writes are visible through SELECT.
-- 2. Prevent regressions where first insert succeeds but subsequent inserts are lost.
DROP TABLE IF EXISTS ${case_db}.t_insert_values_select_repeated;
CREATE TABLE ${case_db}.t_insert_values_select_repeated (
  c1 BIGINT
);
INSERT INTO ${case_db}.t_insert_values_select_repeated VALUES (11);
INSERT INTO ${case_db}.t_insert_values_select_repeated VALUES (22), (33);
INSERT INTO ${case_db}.t_insert_values_select_repeated VALUES (44);
SELECT c1
FROM ${case_db}.t_insert_values_select_repeated;
```

- [ ] **Step 6: Write `sql-tests/iceberg-dml/sql/group_by_aggregate.sql`**

```sql
-- @order_sensitive=true
-- @tags=iceberg_dml,aggregate
-- Test Objective:
-- 1. Validate GROUP BY aggregate semantics on BIGINT columns after Iceberg INSERT.
-- 2. Validate COUNT(*) vs COUNT(col) behavior with NULL values.
-- 3. Validate SUM aggregate on positive and negative values.
DROP TABLE IF EXISTS ${case_db}.t_metrics;
CREATE TABLE ${case_db}.t_metrics (
  grp STRING,
  v BIGINT
);
INSERT INTO ${case_db}.t_metrics VALUES
  ('A', 1),
  ('A', 2),
  ('A', NULL),
  ('B', 5),
  ('B', -1),
  ('B', NULL);
SELECT grp, COUNT(*) AS cnt_all, COUNT(v) AS cnt_v, SUM(v) AS sum_v
FROM ${case_db}.t_metrics
GROUP BY grp
ORDER BY grp;
```

- [ ] **Step 7: Record results for the batch**

```bash
$SQLT --suite iceberg-dml --only create_database,null_insert_select,int_insert_cast,insert_values_select_repeated,group_by_aggregate --mode record
```

- [ ] **Step 8: Spot-check each new `.result` file**

```bash
for c in create_database null_insert_select int_insert_cast insert_values_select_repeated group_by_aggregate; do
  echo "=== $c ==="
  cat sql-tests/iceberg-dml/result/$c.result
done
```

Confirm row counts and NULL-handling rows match the write-path originals' result files. If any result is empty or shows errors, stop and report.

- [ ] **Step 9: Verify**

```bash
$SQLT --suite iceberg-dml --only create_database,null_insert_select,int_insert_cast,insert_values_select_repeated,group_by_aggregate --mode verify
```

Expected: all PASS.

- [ ] **Step 10: Commit**

```bash
git add sql-tests/iceberg-dml/sql sql-tests/iceberg-dml/result
git commit -m "test(iceberg-dml): migrate basic DML cases from write-path"
```

---

## Task 5: Migrate Datetime cases (5 cases)

**Files:**
- Create: `sql-tests/iceberg-dml/sql/datetime_insert_values.sql`
- Create: `sql-tests/iceberg-dml/sql/date_datetime_insert_select.sql`
- Create: `sql-tests/iceberg-dml/sql/datetime_invalid_literal_sanitize_insert_select.sql`
- Create: `sql-tests/iceberg-dml/sql/datetime_timezone_normalization_insert_select.sql`
- Create: `sql-tests/iceberg-dml/sql/datetime_microsecond_precision_delete.sql`
- Plus corresponding `result/*.result` files

**Rewrite policy for the SET enable_* lines:** the four write-path datetime
cases that include them are tuning datacache/spill, which has no semantic
relevance to DML. Drop those `SET enable_*` lines entirely.  If a future
case needs datacache disabled for determinism, add it back then.

- [ ] **Step 1: Write `sql-tests/iceberg-dml/sql/datetime_insert_values.sql`**

```sql
-- @order_sensitive=true
-- @tags=iceberg_dml,datetime
-- Test Objective:
-- 1. Validate DATETIME writes through direct VALUES and INSERT-SELECT constant expressions.
-- 2. Validate derived function result (YEAR) is consistent after sink persistence.
DROP TABLE IF EXISTS ${case_db}.t_datetime_insert_values;
CREATE TABLE ${case_db}.t_datetime_insert_values (
  id INT,
  dt DATETIME
);
INSERT INTO ${case_db}.t_datetime_insert_values VALUES
  (1, '2024-03-01 10:20:30');
INSERT INTO ${case_db}.t_datetime_insert_values
SELECT 2, CAST('2024-12-31 23:59:59' AS DATETIME);
INSERT INTO ${case_db}.t_datetime_insert_values
SELECT 3, NULL;
SELECT id, dt, YEAR(dt) AS y
FROM ${case_db}.t_datetime_insert_values
ORDER BY id;
```

- [ ] **Step 2: Write `sql-tests/iceberg-dml/sql/date_datetime_insert_select.sql`**

```sql
-- @order_sensitive=true
-- @tags=iceberg_dml,datetime
-- Test Objective:
-- 1. Validate DATE/DATETIME values are persisted correctly through INSERT-SELECT into Iceberg sink.
-- 2. Cover leap-day and epoch-style values together with NULL temporal fields.
DROP TABLE IF EXISTS ${case_db}.t_temporal_insert_src;
DROP TABLE IF EXISTS ${case_db}.t_temporal_insert_sink;
CREATE TABLE ${case_db}.t_temporal_insert_src (
  id BIGINT,
  d DATE,
  dt DATETIME
);
CREATE TABLE ${case_db}.t_temporal_insert_sink (
  id INT,
  d DATE,
  dt DATETIME
);
INSERT INTO ${case_db}.t_temporal_insert_src VALUES
  (1, '1970-01-01', '1970-01-01 00:00:00'),
  (2, '2024-02-29', '2024-02-29 23:59:59'),
  (3, NULL, NULL);
INSERT INTO ${case_db}.t_temporal_insert_sink
SELECT id, d, dt
FROM ${case_db}.t_temporal_insert_src;
SELECT id, d, dt
FROM ${case_db}.t_temporal_insert_sink
ORDER BY id;
```

- [ ] **Step 3: Write `sql-tests/iceberg-dml/sql/datetime_invalid_literal_sanitize_insert_select.sql`**

```sql
-- @order_sensitive=true
-- @tags=iceberg_dml,datetime,invalid_literal
-- Test Objective:
-- 1. Validate invalid temporal literal rows can be sanitized to NULL before DATETIME sink writes.
-- 2. Validate valid temporal literals are still persisted correctly in the same batch.
DROP TABLE IF EXISTS ${case_db}.t_datetime_literal_src;
DROP TABLE IF EXISTS ${case_db}.t_datetime_literal_sink;
CREATE TABLE ${case_db}.t_datetime_literal_src (
  id BIGINT,
  raw_dt STRING
);
CREATE TABLE ${case_db}.t_datetime_literal_sink (
  id INT,
  dt DATETIME
);
INSERT INTO ${case_db}.t_datetime_literal_src VALUES
  (1, '2024-02-29 12:34:56'),
  (2, '2024-02-30 00:00:00'),
  (3, 'not-a-datetime'),
  (4, NULL);
INSERT INTO ${case_db}.t_datetime_literal_sink
SELECT
  id,
  CASE
    WHEN raw_dt IS NULL THEN NULL
    WHEN raw_dt = '2024-02-29 12:34:56' THEN CAST('2024-02-29 12:34:56' AS DATETIME)
    ELSE NULL
  END AS dt
FROM ${case_db}.t_datetime_literal_src;
SELECT
  id,
  dt,
  YEAR(dt) AS y
FROM ${case_db}.t_datetime_literal_sink
ORDER BY id;
```

- [ ] **Step 4: Write `sql-tests/iceberg-dml/sql/datetime_timezone_normalization_insert_select.sql`**

```sql
-- @order_sensitive=true
-- @tags=iceberg_dml,datetime,timezone
-- Test Objective:
-- 1. Validate timezone-tagged temporal rows can be normalized to deterministic DATETIME values before sink writes.
-- 2. Validate normalized DATETIME values remain stable after persistence.
DROP TABLE IF EXISTS ${case_db}.t_datetime_tz_src;
DROP TABLE IF EXISTS ${case_db}.t_datetime_tz_sink;
CREATE TABLE ${case_db}.t_datetime_tz_src (
  id BIGINT,
  local_dt STRING,
  tz STRING
);
CREATE TABLE ${case_db}.t_datetime_tz_sink (
  id INT,
  local_dt STRING,
  tz STRING,
  normalized_dt DATETIME
);
INSERT INTO ${case_db}.t_datetime_tz_src VALUES
  (1, '2024-01-01 08:00:00', '+08:00'),
  (2, '2023-12-31 19:00:00', '-05:00'),
  (3, '2024-01-01 00:00:00', '+00:00'),
  (4, '2024-01-01 08:00:00', '+08:00');
INSERT INTO ${case_db}.t_datetime_tz_sink
SELECT
  id,
  local_dt,
  tz,
  CASE
    WHEN local_dt IS NULL THEN NULL
    WHEN tz = '+08:00' THEN CAST('2024-01-01 00:00:00' AS DATETIME)
    WHEN tz = '-05:00' THEN CAST('2024-01-01 00:00:00' AS DATETIME)
    WHEN tz = '+00:00' THEN CAST(local_dt AS DATETIME)
    ELSE NULL
  END AS normalized_dt
FROM ${case_db}.t_datetime_tz_src;
SELECT
  id,
  tz,
  normalized_dt,
  YEAR(normalized_dt) AS y
FROM ${case_db}.t_datetime_tz_sink
ORDER BY id;
```

- [ ] **Step 5: Write `sql-tests/iceberg-dml/sql/datetime_microsecond_precision_delete.sql`**

```sql
-- @tags=iceberg_dml,datetime,delete
-- Test Objective:
-- 1. Validate DATETIME stores sub-second fractional precision up to microseconds (6 digits).
-- 2. Validate DELETE by exact DATETIME value including sub-second comparisons on an Iceberg v3 table.
-- 3. Cover: truncation of excess digits, zero-padding on display, same value via different literal forms.
-- Key semantics:
--   '2020-01-01 00:00:00'    == '2020-01-01 00:00:00.0' (both stored as 00:00:00)
--   '2020-01-01 00:00:00.1'  == '2020-01-01 00:00:00.100000'
--   '2020-01-01 00:00:00.123450' == '2020-01-01 00:00:00.12345' (trailing zero trimmed)

-- query 1
-- @skip_result_check=true
DROP TABLE IF EXISTS ${case_db}.t_datetime;
CREATE TABLE ${case_db}.t_datetime (
    c1 int,
    c2 datetime
) TBLPROPERTIES ("format-version" = "3");
INSERT INTO ${case_db}.t_datetime VALUES
(1, '2020-01-01 00:00:00'),
(2, '2020-01-01 00:00:00.0'),
(3, '2020-01-01 00:00:00.01'),
(4, '2020-01-01 00:00:00.012'),
(5, '2020-01-01 00:00:00.0123'),
(6, '2020-01-01 00:00:00.01234'),
(7, '2020-01-01 00:00:00.012345'),
(8, '2020-01-01 00:00:00.1'),
(9, '2020-01-01 00:00:00.12'),
(10, '2020-01-01 00:00:00.123'),
(11, '2020-01-01 00:00:00.1234'),
(12, '2020-01-01 00:00:00.12345'),
(13, '2020-01-01 00:00:00.123450');

-- query 2
-- @order_sensitive=true
-- c1=1,2 both display as '00:00:00'; c1=3..13 show padded microseconds
SELECT * FROM ${case_db}.t_datetime ORDER BY c1;

-- query 3
-- @skip_result_check=true
-- Delete by exact datetime: '00:00:00' matches c1=1 and c1=2 (same value after normalization)
DELETE FROM ${case_db}.t_datetime WHERE c2 = '2020-01-01 00:00:00';
-- '00:00:00.0' is same as '00:00:00'; rows already gone
DELETE FROM ${case_db}.t_datetime WHERE c2 = '2020-01-01 00:00:00.0';
-- Delete c1=4: '00:00:00.012' → stored as '00:00:00.012000'
DELETE FROM ${case_db}.t_datetime WHERE c2 = '2020-01-01 00:00:00.012';

-- query 4
-- @order_sensitive=true
-- Remaining: c1=3,5,6,7,8,9,10,11,12,13 (c1=1,2,4 deleted)
SELECT * FROM ${case_db}.t_datetime ORDER BY c1;

-- query 5
-- @skip_result_check=true
DELETE FROM ${case_db}.t_datetime WHERE c2 = '2020-01-01 00:00:00.1';
DELETE FROM ${case_db}.t_datetime WHERE c2 = '2020-01-01 00:00:00.123';
DELETE FROM ${case_db}.t_datetime WHERE c2 = '2020-01-01 00:00:00.123450';

-- query 6
-- @order_sensitive=true
-- Final remaining: c1=3,5,6,7,9,11 (6 rows)
SELECT * FROM ${case_db}.t_datetime ORDER BY c1;
```

- [ ] **Step 6: Record results**

```bash
$SQLT --suite iceberg-dml --only datetime_insert_values,date_datetime_insert_select,datetime_invalid_literal_sanitize_insert_select,datetime_timezone_normalization_insert_select,datetime_microsecond_precision_delete --mode record
```

- [ ] **Step 7: Spot-check results**

```bash
for c in datetime_insert_values date_datetime_insert_select datetime_invalid_literal_sanitize_insert_select datetime_timezone_normalization_insert_select datetime_microsecond_precision_delete; do
  echo "=== $c ==="
  cat sql-tests/iceberg-dml/result/$c.result
done
```

Compare row counts and DATETIME formatting against the write-path originals' result files (especially microsecond formatting in `datetime_microsecond_precision_delete`, which expects 6 remaining rows after the deletes). If the microsecond DELETE case ends up with the wrong remaining rows, stop and report.

- [ ] **Step 8: Verify**

```bash
$SQLT --suite iceberg-dml --only datetime_insert_values,date_datetime_insert_select,datetime_invalid_literal_sanitize_insert_select,datetime_timezone_normalization_insert_select,datetime_microsecond_precision_delete --mode verify
```

Expected: all PASS.

- [ ] **Step 9: Commit**

```bash
git add sql-tests/iceberg-dml/sql sql-tests/iceberg-dml/result
git commit -m "test(iceberg-dml): migrate datetime cases incl v3 DELETE precision"
```

---

## Task 6: Migrate Decimal cases (3 cases)

**Files:**
- Create: `sql-tests/iceberg-dml/sql/decimal_insert_select.sql`
- Create: `sql-tests/iceberg-dml/sql/decimal_overflow_to_null_insert_select.sql`
- Create: `sql-tests/iceberg-dml/sql/decimal_rounding_insert_select.sql`
- Plus corresponding `result/*.result` files

None of these cases mutate after the initial INSERT, so no
`format-version = 3` declaration is needed.  Drop the `SET enable_*`
datacache/spill toggles from the two `decimal_*_insert_select` cases that
carry them — they're cache tuning, not DML semantics.

- [ ] **Step 1: Write `sql-tests/iceberg-dml/sql/decimal_insert_select.sql`**

```sql
-- @order_sensitive=true
-- @tags=iceberg_dml,decimal
-- Test Objective:
-- 1. Validate INSERT-SELECT writing DECIMAL values from a wider DECIMAL source into a narrower sink schema.
-- 2. Validate NULL propagation for DECIMAL columns through Iceberg table sink.
DROP TABLE IF EXISTS ${case_db}.t_decimal_insert_src;
DROP TABLE IF EXISTS ${case_db}.t_decimal_insert_sink;
CREATE TABLE ${case_db}.t_decimal_insert_src (
  id BIGINT,
  v DECIMAL(20, 6)
);
CREATE TABLE ${case_db}.t_decimal_insert_sink (
  id INT,
  v DECIMAL(10, 3)
);
INSERT INTO ${case_db}.t_decimal_insert_src VALUES
  (1, 123.456000),
  (2, -99.125000),
  (3, NULL);
INSERT INTO ${case_db}.t_decimal_insert_sink
SELECT id, v
FROM ${case_db}.t_decimal_insert_src;
SELECT id, v
FROM ${case_db}.t_decimal_insert_sink
ORDER BY id;
```

If the Iceberg writer rejects `DECIMAL(20,6) → DECIMAL(10,3)` narrowing,
**stop and report**.  Do not loosen the sink type.

- [ ] **Step 2: Write `sql-tests/iceberg-dml/sql/decimal_overflow_to_null_insert_select.sql`**

```sql
-- @order_sensitive=true
-- @tags=iceberg_dml,decimal,overflow
-- Test Objective:
-- 1. Validate overflow-range decimal rows can be sanitized to NULL before sink writes.
-- 2. Validate in-range boundary rows are still written after scale narrowing.
DROP TABLE IF EXISTS ${case_db}.t_decimal_overflow_sink;
CREATE TABLE ${case_db}.t_decimal_overflow_sink (
  id INT,
  v DECIMAL(10, 2)
);
INSERT INTO ${case_db}.t_decimal_overflow_sink
SELECT
  CAST(1 AS INT),
  CASE
    WHEN ABS(CAST(99999999.9949 AS DECIMAL(13, 4))) > 99999999.9999 THEN NULL
    ELSE CAST(99999999.9949 AS DECIMAL(13, 4))
  END;
INSERT INTO ${case_db}.t_decimal_overflow_sink
SELECT
  CAST(2 AS INT),
  CASE
    WHEN ABS(CAST(-99999999.9949 AS DECIMAL(13, 4))) > 99999999.9999 THEN NULL
    ELSE CAST(-99999999.9949 AS DECIMAL(13, 4))
  END;
INSERT INTO ${case_db}.t_decimal_overflow_sink
SELECT
  CAST(3 AS INT),
  CASE
    WHEN ABS(CAST(100000000.0000 AS DECIMAL(13, 4))) > 99999999.9999 THEN NULL
    ELSE CAST(100000000.0000 AS DECIMAL(13, 4))
  END;
INSERT INTO ${case_db}.t_decimal_overflow_sink
SELECT
  CAST(4 AS INT),
  CASE
    WHEN ABS(CAST(-100000000.0000 AS DECIMAL(13, 4))) > 99999999.9999 THEN NULL
    ELSE CAST(-100000000.0000 AS DECIMAL(13, 4))
  END;
INSERT INTO ${case_db}.t_decimal_overflow_sink
SELECT
  CAST(5 AS INT),
  CAST(NULL AS DECIMAL(13, 4));
SELECT
  id,
  v,
  IF(v IS NULL, 1, 0) AS is_null_v
FROM ${case_db}.t_decimal_overflow_sink
ORDER BY id;
```

- [ ] **Step 3: Write `sql-tests/iceberg-dml/sql/decimal_rounding_insert_select.sql`**

```sql
-- @order_sensitive=true
-- @tags=iceberg_dml,decimal,rounding
-- Test Objective:
-- 1. Validate decimal scale narrowing writes apply deterministic rounding for positive and negative values.
-- 2. Validate NULL propagation for decimal rows during sink writes.
DROP TABLE IF EXISTS ${case_db}.t_decimal_rounding_sink;
CREATE TABLE ${case_db}.t_decimal_rounding_sink (
  id INT,
  v DECIMAL(10, 2)
);
INSERT INTO ${case_db}.t_decimal_rounding_sink
SELECT CAST(1 AS INT), CAST(1.2344 AS DECIMAL(10, 4));
INSERT INTO ${case_db}.t_decimal_rounding_sink
SELECT CAST(2 AS INT), CAST(1.2356 AS DECIMAL(10, 4));
INSERT INTO ${case_db}.t_decimal_rounding_sink
SELECT CAST(3 AS INT), CAST(-2.3444 AS DECIMAL(10, 4));
INSERT INTO ${case_db}.t_decimal_rounding_sink
SELECT CAST(4 AS INT), CAST(-2.3456 AS DECIMAL(10, 4));
INSERT INTO ${case_db}.t_decimal_rounding_sink
SELECT CAST(5 AS INT), CAST(NULL AS DECIMAL(10, 4));
SELECT id, v
FROM ${case_db}.t_decimal_rounding_sink
ORDER BY id;
```

- [ ] **Step 4: Record results**

```bash
$SQLT --suite iceberg-dml --only decimal_insert_select,decimal_overflow_to_null_insert_select,decimal_rounding_insert_select --mode record
```

- [ ] **Step 5: Spot-check**

```bash
for c in decimal_insert_select decimal_overflow_to_null_insert_select decimal_rounding_insert_select; do
  echo "=== $c ==="
  cat sql-tests/iceberg-dml/result/$c.result
done
```

For overflow: rows 3, 4, 5 must have `v` NULL with `is_null_v=1`; rows 1, 2 keep their decimal values.  For rounding: row 1 = 1.23, row 2 = 1.24, row 3 = -2.34, row 4 = -2.35, row 5 = NULL.  If anything diverges, stop and report.

- [ ] **Step 6: Verify**

```bash
$SQLT --suite iceberg-dml --only decimal_insert_select,decimal_overflow_to_null_insert_select,decimal_rounding_insert_select --mode verify
```

- [ ] **Step 7: Commit**

```bash
git add sql-tests/iceberg-dml/sql sql-tests/iceberg-dml/result
git commit -m "test(iceberg-dml): migrate decimal precision/overflow/rounding cases"
```

---

## Task 7: Migrate DELETE cases (5 cases)

All five tables use `TBLPROPERTIES ("format-version" = "3")` since they exercise row-level DELETE on Iceberg.

**Files:**
- Create: `sql-tests/iceberg-dml/sql/delete_in_list.sql`
- Create: `sql-tests/iceberg-dml/sql/delete_is_null.sql`
- Create: `sql-tests/iceberg-dml/sql/delete_non_key_col.sql`
- Create: `sql-tests/iceberg-dml/sql/delete_complex_where.sql`
- Create: `sql-tests/iceberg-dml/sql/delete_no_match.sql`
- Plus corresponding `result/*.result` files

- [ ] **Step 1: Write `sql-tests/iceberg-dml/sql/delete_in_list.sql`**

```sql
-- @order_sensitive=true
-- @tags=iceberg_dml,delete
-- Test Objective:
-- DELETE WHERE col IN (...) removes matching rows on an Iceberg v3 table.
DROP TABLE IF EXISTS ${case_db}.t_delete_in_list;
CREATE TABLE ${case_db}.t_delete_in_list (
  id INT,
  v INT
) TBLPROPERTIES ("format-version" = "3");
INSERT INTO ${case_db}.t_delete_in_list VALUES (1, 10), (2, 20), (3, 30), (4, 40);
DELETE FROM ${case_db}.t_delete_in_list WHERE id IN (1, 3);
SELECT id, v FROM ${case_db}.t_delete_in_list ORDER BY id;
```

- [ ] **Step 2: Write `sql-tests/iceberg-dml/sql/delete_is_null.sql`**

```sql
-- @order_sensitive=true
-- @tags=iceberg_dml,delete
-- Test Objective:
-- DELETE WHERE col IS NULL removes rows whose column is NULL on an Iceberg v3 table.
DROP TABLE IF EXISTS ${case_db}.t_delete_is_null;
CREATE TABLE ${case_db}.t_delete_is_null (
  id INT,
  v INT
) TBLPROPERTIES ("format-version" = "3");
INSERT INTO ${case_db}.t_delete_is_null VALUES (1, 10), (2, NULL), (3, 30), (4, NULL);
DELETE FROM ${case_db}.t_delete_is_null WHERE v IS NULL;
SELECT id, v FROM ${case_db}.t_delete_is_null ORDER BY id;
```

- [ ] **Step 3: Write `sql-tests/iceberg-dml/sql/delete_non_key_col.sql`**

```sql
-- @order_sensitive=true
-- @tags=iceberg_dml,delete
-- Test Objective:
-- DELETE WHERE on a non-PK-equivalent column on an Iceberg v3 table.
DROP TABLE IF EXISTS ${case_db}.t_delete_non_key;
CREATE TABLE ${case_db}.t_delete_non_key (
  id INT,
  v INT
) TBLPROPERTIES ("format-version" = "3");
INSERT INTO ${case_db}.t_delete_non_key VALUES (1, 10), (2, 20), (3, 30);
DELETE FROM ${case_db}.t_delete_non_key WHERE v = 20;
SELECT id, v FROM ${case_db}.t_delete_non_key ORDER BY id;
```

- [ ] **Step 4: Write `sql-tests/iceberg-dml/sql/delete_complex_where.sql`**

```sql
-- @order_sensitive=true
-- @tags=iceberg_dml,delete
-- Test Objective:
-- DELETE with a function-call WHERE on an Iceberg v3 table.
DROP TABLE IF EXISTS ${case_db}.t_delete_complex_where;
CREATE TABLE ${case_db}.t_delete_complex_where (
  id INT,
  k INT,
  label STRING
) TBLPROPERTIES ("format-version" = "3");
INSERT INTO ${case_db}.t_delete_complex_where VALUES (1, 10, 'X'), (2, 20, 'Y'), (3, 30, 'Z');
DELETE FROM ${case_db}.t_delete_complex_where WHERE LOWER(label) = 'y';
SELECT id, k, label FROM ${case_db}.t_delete_complex_where ORDER BY id;
```

- [ ] **Step 5: Write `sql-tests/iceberg-dml/sql/delete_no_match.sql`**

```sql
-- @order_sensitive=true
-- @tags=iceberg_dml,delete
-- Test Objective:
-- DELETE matching zero rows on an Iceberg v3 table is a no-op: no error, all
-- original rows remain visible.
DROP TABLE IF EXISTS ${case_db}.t_delete_no_match;
CREATE TABLE ${case_db}.t_delete_no_match (
  id INT,
  v INT
) TBLPROPERTIES ("format-version" = "3");
INSERT INTO ${case_db}.t_delete_no_match VALUES (1, 100);
DELETE FROM ${case_db}.t_delete_no_match WHERE id = 999;
SELECT id, v FROM ${case_db}.t_delete_no_match ORDER BY id;
```

- [ ] **Step 6: Record results**

```bash
$SQLT --suite iceberg-dml --only delete_in_list,delete_is_null,delete_non_key_col,delete_complex_where,delete_no_match --mode record
```

- [ ] **Step 7: Spot-check**

Expected remaining rows:
- `delete_in_list`: 2 rows — `(2,20), (4,40)`
- `delete_is_null`: 2 rows — `(1,10), (3,30)`
- `delete_non_key_col`: 2 rows — `(1,10), (3,30)`
- `delete_complex_where`: 2 rows — `(1,10,'X'), (3,30,'Z')`
- `delete_no_match`: 1 row — `(1,100)`

```bash
for c in delete_in_list delete_is_null delete_non_key_col delete_complex_where delete_no_match; do
  echo "=== $c ==="
  cat sql-tests/iceberg-dml/result/$c.result
done
```

If any DELETE was rejected by the runtime or produced wrong row counts, stop and report.

- [ ] **Step 8: Verify**

```bash
$SQLT --suite iceberg-dml --only delete_in_list,delete_is_null,delete_non_key_col,delete_complex_where,delete_no_match --mode verify
```

- [ ] **Step 9: Commit**

```bash
git add sql-tests/iceberg-dml/sql sql-tests/iceberg-dml/result
git commit -m "test(iceberg-dml): migrate v3 DELETE coverage cases"
```

---

## Task 8: Migrate INSERT+DELETE flow (1 case)

**Files:**
- Create: `sql-tests/iceberg-dml/sql/insert_delete_select.sql`
- Create: `sql-tests/iceberg-dml/result/insert_delete_select.result`

- [ ] **Step 1: Write `sql-tests/iceberg-dml/sql/insert_delete_select.sql`**

```sql
-- @order_sensitive=true
-- @tags=iceberg_dml,delete
-- Test Objective:
-- 1. Validate Iceberg v3 table accepts INSERT then DELETE and exposes correct visible rows.
-- 2. Prevent regression where DELETE is accepted but reads still return the deleted row.
DROP TABLE IF EXISTS ${case_db_2}.t_insert_delete_select;
CREATE TABLE ${case_db_2}.t_insert_delete_select (
  city_id INT,
  population INT,
  city STRING
) TBLPROPERTIES ("format-version" = "3");
INSERT INTO ${case_db_2}.t_insert_delete_select VALUES
  (1, 100, 'Beijing'),
  (2, 200, 'Shanghai'),
  (3, 300, 'Shenzhen');
DELETE FROM ${case_db_2}.t_insert_delete_select
WHERE city_id = 2;
SELECT city_id, population, city
FROM ${case_db_2}.t_insert_delete_select
ORDER BY city_id;
```

- [ ] **Step 2: Record + verify + commit**

```bash
$SQLT --suite iceberg-dml --only insert_delete_select --mode record
cat sql-tests/iceberg-dml/result/insert_delete_select.result
# Expect 2 rows: Beijing and Shenzhen.
$SQLT --suite iceberg-dml --only insert_delete_select --mode verify
git add sql-tests/iceberg-dml/sql/insert_delete_select.sql sql-tests/iceberg-dml/result/insert_delete_select.result
git commit -m "test(iceberg-dml): migrate insert+delete flow case"
```

---

## Task 9: Migrate MERGE INTO upsert + delete (1 case)

Depends on Task 3 Step 3 having confirmed MERGE INTO works.

**Files:**
- Create: `sql-tests/iceberg-dml/sql/merge_into_upsert_delete.sql`
- Create: `sql-tests/iceberg-dml/result/merge_into_upsert_delete.result`

- [ ] **Step 1: Write `sql-tests/iceberg-dml/sql/merge_into_upsert_delete.sql`**

```sql
-- @order_sensitive=true
-- @tags=iceberg_dml,merge,delete
-- Test Objective:
-- 1. Validate MERGE INTO on an Iceberg v3 table updates existing rows and inserts new ones.
-- 2. Validate a follow-up DELETE removes the target row from visible result.

-- query 1
-- @skip_result_check=true
DROP TABLE IF EXISTS ${case_db_2}.t_merge_target;
DROP TABLE IF EXISTS ${case_db_2}.t_merge_source;
CREATE TABLE ${case_db_2}.t_merge_target (
  city_id INT,
  population INT,
  city STRING
) TBLPROPERTIES ("format-version" = "3");
CREATE TABLE ${case_db_2}.t_merge_source (
  city_id INT,
  population INT,
  city STRING
) TBLPROPERTIES ("format-version" = "3");
INSERT INTO ${case_db_2}.t_merge_target VALUES
  (1, 100, 'Beijing'),
  (2, 200, 'Shanghai');
INSERT INTO ${case_db_2}.t_merge_source VALUES
  (2, 250, 'Shanghai-updated'),
  (3, 300, 'Shenzhen');

-- query 2
-- @skip_result_check=true
-- Upsert: update Shanghai (matched), insert Shenzhen (not matched).
MERGE INTO ${case_db_2}.t_merge_target AS t
  USING ${case_db_2}.t_merge_source AS s
  ON t.city_id = s.city_id
  WHEN MATCHED THEN UPDATE SET population = s.population, city = s.city
  WHEN NOT MATCHED THEN INSERT (city_id, population, city) VALUES (s.city_id, s.population, s.city);

-- query 3
-- Expect: Beijing(1,100), Shanghai-updated(2,250), Shenzhen(3,300).
SELECT city_id, population, city
FROM ${case_db_2}.t_merge_target
ORDER BY city_id;

-- query 4
-- @skip_result_check=true
DELETE FROM ${case_db_2}.t_merge_target
WHERE city_id = 1;

-- query 5
-- Expect: Shanghai-updated(2,250), Shenzhen(3,300).
SELECT city_id, population, city
FROM ${case_db_2}.t_merge_target
ORDER BY city_id;
```

- [ ] **Step 2: Record + verify + commit**

```bash
$SQLT --suite iceberg-dml --only merge_into_upsert_delete --mode record
cat sql-tests/iceberg-dml/result/merge_into_upsert_delete.result
# Spot-check: query 3 has 3 rows; query 5 has 2 rows.
$SQLT --suite iceberg-dml --only merge_into_upsert_delete --mode verify
git add sql-tests/iceberg-dml/sql/merge_into_upsert_delete.sql sql-tests/iceberg-dml/result/merge_into_upsert_delete.result
git commit -m "test(iceberg-dml): migrate MERGE INTO upsert+delete case"
```

If MERGE rejected at runtime despite Task 3 passing, stop and report — do not weaken the case.

---

## Task 10: Migrate Partitioned INSERT (1 case)

Depends on Task 3 Step 5 having confirmed identity-partition INSERT works.

**Files:**
- Create: `sql-tests/iceberg-dml/sql/partitioned_insert.sql`
- Create: `sql-tests/iceberg-dml/result/partitioned_insert.result`

- [ ] **Step 1: Write `sql-tests/iceberg-dml/sql/partitioned_insert.sql`**

```sql
-- @order_sensitive=true
-- @tags=iceberg_dml,partition
-- Test Objective:
-- 1. Validate Iceberg identity-partitioned table accepts multi-row INSERT spanning partitions.
-- 2. Validate read-back returns the inserted rows.
DROP TABLE IF EXISTS ${case_db_2}.t_partitioned_insert;
CREATE TABLE ${case_db_2}.t_partitioned_insert (
  p BIGINT,
  k BIGINT,
  v BIGINT
)
PARTITION BY identity(p);
INSERT INTO ${case_db_2}.t_partitioned_insert VALUES
  (1, 1, 10),
  (2, 1, 20);
SELECT p, k, v
FROM ${case_db_2}.t_partitioned_insert
ORDER BY p, k, v;
```

- [ ] **Step 2: Record + verify + commit**

```bash
$SQLT --suite iceberg-dml --only partitioned_insert --mode record
cat sql-tests/iceberg-dml/result/partitioned_insert.result
# Expect 2 rows.
$SQLT --suite iceberg-dml --only partitioned_insert --mode verify
git add sql-tests/iceberg-dml/sql/partitioned_insert.sql sql-tests/iceberg-dml/result/partitioned_insert.result
git commit -m "test(iceberg-dml): migrate identity-partitioned INSERT case"
```

---

## Task 11: Migrate Lifecycle (1 case)

Depends on Task 3 Step 4 having confirmed TRUNCATE works.

**Files:**
- Create: `sql-tests/iceberg-dml/sql/table_lifecycle.sql`
- Create: `sql-tests/iceberg-dml/result/table_lifecycle.result`

- [ ] **Step 1: Write `sql-tests/iceberg-dml/sql/table_lifecycle.sql`**

```sql
-- @order_sensitive=true
-- @tags=iceberg_dml,lifecycle
-- Test Objective:
-- 1. Iceberg TRUNCATE TABLE clears rows but keeps the table available for new writes.
-- 2. Iceberg DROP TABLE removes the table from the catalog.

-- query 1
-- @skip_result_check=true
DROP TABLE IF EXISTS ${case_db}.t_table_lifecycle;
CREATE TABLE ${case_db}.t_table_lifecycle (
  k1 INT,
  v1 STRING
);
INSERT INTO ${case_db}.t_table_lifecycle VALUES
  (1, 'a'),
  (2, 'b');

-- query 2
SELECT k1, v1
FROM ${case_db}.t_table_lifecycle
ORDER BY k1;

-- query 3
-- @skip_result_check=true
TRUNCATE TABLE ${case_db}.t_table_lifecycle;

-- query 4
SELECT count(*)
FROM ${case_db}.t_table_lifecycle;

-- query 5
-- @skip_result_check=true
INSERT INTO ${case_db}.t_table_lifecycle VALUES (10, 'z');

-- query 6
SELECT k1, v1
FROM ${case_db}.t_table_lifecycle
ORDER BY k1;

-- query 7
-- @skip_result_check=true
DROP TABLE ${case_db}.t_table_lifecycle;

-- query 8
-- @expect_error=Unknown table
SELECT * FROM ${case_db}.t_table_lifecycle;
```

- [ ] **Step 2: Record + verify + commit**

```bash
$SQLT --suite iceberg-dml --only table_lifecycle --mode record
cat sql-tests/iceberg-dml/result/table_lifecycle.result
# Spot-check: query 2 = 2 rows, query 4 = 0, query 6 = 1 row (10,'z').
$SQLT --suite iceberg-dml --only table_lifecycle --mode verify
git add sql-tests/iceberg-dml/sql/table_lifecycle.sql sql-tests/iceberg-dml/result/table_lifecycle.result
git commit -m "test(iceberg-dml): migrate lifecycle case (CREATE/INSERT/TRUNCATE/DROP)"
```

If the `@expect_error=Unknown table` step matches a different error string, update it to whatever Iceberg returns for read-after-drop and re-record.

---

## Task 12: Whole-suite verify before deletion

Before deleting `write-path`, confirm the new suite is fully green standalone.

- [ ] **Step 1: Run the whole suite**

```bash
$SQLT --suite iceberg-dml --mode verify
```

Expected: 23/23 PASS.

- [ ] **Step 2: List the cases to double-check the count**

```bash
ls sql-tests/iceberg-dml/sql/*.sql | wc -l
ls sql-tests/iceberg-dml/result/*.result | wc -l
```

Both must be `23`.

- [ ] **Step 3: If anything fails, stop and report**

Per the no-fallback rule, do not "fix" a failure by removing the case. Surface the failing case + statement + error.

---

## Task 13: Delete the write-path suite

**Files:**
- Delete: `sql-tests/write-path/` (entire directory)

- [ ] **Step 1: Confirm no other code references write-path**

```bash
grep -rn "write-path\|write_path" tests/ docs/ sql-tests/ --include="*.rs" --include="*.md" --include="*.sql" --include="*.toml" 2>/dev/null | grep -v "sql-tests/write-path/\|docs/superpowers/specs/2026-05-22\|docs/superpowers/plans/2026-05-22"
```

Expected: empty output. If anything else matches, evaluate whether it must be updated or removed before proceeding.

- [ ] **Step 2: Remove the directory**

```bash
git rm -r sql-tests/write-path
```

- [ ] **Step 3: Confirm the runner no longer discovers the suite**

```bash
$SQLT --suite write-path --list
```

Expected: errors with "suite not found" / unknown suite.

- [ ] **Step 4: Commit**

```bash
git commit -m "test(write-path): delete suite; superseded by iceberg-dml"
```

---

## Task 14: Final hygiene

- [ ] **Step 1: Format + lint runner code**

```bash
cargo fmt
cargo clippy --manifest-path tests/sql-test-runner/Cargo.toml -- -D warnings
```

- [ ] **Step 2: Re-run the new suite end to end one more time**

```bash
$SQLT --suite iceberg-dml --mode verify
```

Expected: 23/23 PASS.

- [ ] **Step 3: Stop the standalone-server**

```bash
kill "$SRV_PID" 2>/dev/null || true
```

- [ ] **Step 4: Commit any fmt/clippy fixups**

```bash
git status
# If fmt/clippy made changes:
git add -A
git commit -m "chore: cargo fmt after iceberg-dml migration"
```

- [ ] **Step 5: Print final summary**

```bash
git log --oneline main..HEAD
```

Expected: a clean stack of focused commits (suite scaffold; smoke case; basic DML batch; datetime batch; decimal batch; DELETE batch; insert+delete; MERGE; partitioned; lifecycle; write-path removal; optional fmt).

---

## Appendix: One-line summary of what the engineer must remember

1. **No silent fallback.** If MERGE / TRUNCATE / partitioned INSERT / DELETE / any other Iceberg-v3 feature is rejected, **stop and report**. Don't rewrite the case. Don't skip it.
2. **v3 always.** Mutation cases declare `TBLPROPERTIES ("format-version" = "3")` on CREATE TABLE.
3. **Strip OLAP DDL.** No `DUPLICATE/UNIQUE/PRIMARY/AGGREGATE KEY`, no `DISTRIBUTED BY ... BUCKETS`, no `PROPERTIES("replication_num"=...)`.
4. **Catalog is implicit.** Per-case SQL refers to `${case_db}` directly; don't write `SET catalog default_catalog;`.
5. **Commit often.** Each task block ends with a focused commit.
