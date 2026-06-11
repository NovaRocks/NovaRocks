# schema-be + schema-change + schema-info → iceberg-ddl Suite Migration Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a focused `iceberg-ddl` SQL test suite that exercises Iceberg ALTER TABLE semantics on the standalone engine, migrating the surviving DDL test points from `sql-tests/{schema-be, schema-change, schema-info}/` and deleting those three source suites.

**Architecture:** New directory `sql-tests/iceberg-ddl/{init.sql, cleanup.sql, sql/, result/}` mirrors the iceberg-dml layout — hadoop catalog over MinIO via `docker/iceberg-rest`. Per-case SQL exercises Iceberg-native ALTER TABLE (ADD COLUMN, DROP COLUMN, dotted-path STRUCT field evolution, ALTER COLUMN COMMENT, DEFAULT values). NovaRocks gaps surfaced during migration are fixed in this PR rather than worked around.

**Tech Stack:** Rust runner (`tests/sql-test-runner`), SQL test suites under `sql-tests/`, Iceberg hadoop catalog over MinIO via `docker/iceberg-rest`, NovaRocks `standalone-server`.

**Spec:** [docs/design/specs/2026-05-22-iceberg-ddl-migration-design.md](../specs/2026-05-22-iceberg-ddl-migration-design.md)

**No-fallback rule** (from spec §8): if any migration step hits a feature NovaRocks does not support on Iceberg (DEFAULT values, ALTER COLUMN COMMENT, STRUCT field evolution, etc.), **stop and report the exact statement + error to the user**. Do not rewrite the case into an equivalent that bypasses the missing feature.

---

## Key syntax differences from source suites

The source cases use StarRocks-flavored DDL. The target suite uses Iceberg-native DDL. Three syntactic transforms recur:

| StarRocks (source) | Iceberg (target) |
|---|---|
| `ALTER TABLE t MODIFY COLUMN c1 ADD FIELD foo INT` | `ALTER TABLE t ADD COLUMN c1.foo INT` |
| `ALTER TABLE t MODIFY COLUMN c1 ADD FIELD foo.bar INT` | `ALTER TABLE t ADD COLUMN c1.foo.bar INT` |
| `ALTER TABLE t MODIFY COLUMN c1 ADD FIELD [*].foo INT` (ARRAY<STRUCT> element) | `ALTER TABLE t ADD COLUMN c1.element.foo INT` |
| `ALTER TABLE t MODIFY COLUMN c1 DROP FIELD foo` | `ALTER TABLE t DROP COLUMN c1.foo` |
| `ALTER TABLE t MODIFY COLUMN k COMMENT 'x'` | `ALTER TABLE t ALTER COLUMN k COMMENT 'x'` (subject to Task 2 smoke) |
| `SET @a = sleep(2)` after each ALTER | Removed — Iceberg ALTER is synchronous at metadata commit |

Note: existing `sql-tests/iceberg/sql/iceberg_schema_evolution_nested.sql` exercises `ALTER TABLE t ADD COLUMN parent.child TYPE` on Iceberg today, confirming the dotted-path syntax works. `iceberg_schema_evolution_nested.sql:3-7` also notes: "NovaRocks's standalone INSERT path doesn't yet support STRUCT column writes" — so STRUCT field-evolution cases will be DDL-only (no INSERT data round-trips through STRUCT columns) unless Task 2 smoke proves STRUCT INSERT now works.

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
   LOG=/tmp/novarocks-iceddl.log
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
   echo "$SRV_PID" > /tmp/novarocks-iceddl.pid
   ```

4. Common helper variable (used in every record/verify command below):

   ```bash
   SQLT="cargo run --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests --"
   ```

The server stays up across tasks. Restart it only if it crashes.

---

## Task 1: Scaffold the iceberg-ddl suite

**Files:**
- Create: `sql-tests/iceberg-ddl/init.sql`
- Create: `sql-tests/iceberg-ddl/cleanup.sql`
- Create: `sql-tests/iceberg-ddl/sql/.gitkeep`
- Create: `sql-tests/iceberg-ddl/result/.gitkeep`
- Modify: `tests/sql-test-runner/src/config.rs` — one match arm only

- [ ] **Step 1: Create `sql-tests/iceberg-ddl/init.sql`**

```sql
-- @catalog=iceberg_ddl_cat_${suite_uuid0}
CREATE EXTERNAL CATALOG IF NOT EXISTS `iceberg_ddl_cat_${suite_uuid0}`
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

- [ ] **Step 2: Create `sql-tests/iceberg-ddl/cleanup.sql`**

```sql
DROP CATALOG IF EXISTS `iceberg_ddl_cat_${suite_uuid0}`;
```

- [ ] **Step 3: Create empty placeholder dirs**

```bash
mkdir -p sql-tests/iceberg-ddl/sql sql-tests/iceberg-ddl/result
touch sql-tests/iceberg-ddl/sql/.gitkeep sql-tests/iceberg-ddl/result/.gitkeep
```

- [ ] **Step 4: Register `iceberg-ddl` in placeholder defaults**

Open `tests/sql-test-runner/src/config.rs` (around line 147) and change the existing hadoop-catalog match arm from:

```rust
        "iceberg" | "iceberg-dml" | "iceberg-ivm" | "mv-on-iceberg" => {
```

to:

```rust
        "iceberg" | "iceberg-ddl" | "iceberg-dml" | "iceberg-ivm" | "mv-on-iceberg" => {
```

- [ ] **Step 5: Build the runner to confirm the edit compiles**

```bash
cargo build --manifest-path tests/sql-test-runner/Cargo.toml
```

Expected: success, no warnings about the new arm.

- [ ] **Step 6: Confirm the suite is auto-discovered**

```bash
source docker/iceberg-rest/runtime/current/env.sh
$SQLT --config "$NOVAROCKS_SQL_TEST_CONFIG" --suite iceberg-ddl --dry-run 2>&1 | head -5
```

Expected: an error like `no SQL files found in .../iceberg-ddl/sql with pattern *.sql (suite iceberg-ddl)`. The success criterion is that the suite IS recognized (the runner resolved the directory), NOT that the command exits 0.

- [ ] **Step 7: Commit**

```bash
git add sql-tests/iceberg-ddl tests/sql-test-runner/src/config.rs
git commit -m "test(iceberg-ddl): scaffold suite with hadoop catalog init/cleanup"
```

---

## Task 2: Smoke-test the three risky DDL paths

This is the de-risking step. Before writing any case that depends on a particular ALTER TABLE feature, confirm it works on the standalone+Iceberg-hadoop path. **No commit from this task** — its output is a smoke decision tree for Tasks 4–9.

**Smoke A — `ALTER TABLE ... ALTER COLUMN ... COMMENT 'x'`:**

```bash
source docker/iceberg-rest/runtime/current/env.sh
HW=$(grep '^iceberg_catalog_warehouse' "$NOVAROCKS_SQL_TEST_CONFIG" | sed 's/.*= //')
mysql -h 127.0.0.1 -P "$NOVA_ENV_MYSQL_PORT" -u root --protocol=TCP <<EOF
CREATE EXTERNAL CATALOG IF NOT EXISTS ddl_smk PROPERTIES (
  "type"="iceberg", "iceberg.catalog.type"="hadoop", "iceberg.catalog.warehouse"="$HW",
  "aws.s3.access_key"="admin", "aws.s3.secret_key"="admin123",
  "aws.s3.endpoint"="http://127.0.0.1:9000", "aws.s3.enable_path_style_access"="true"
);
CREATE DATABASE ddl_smk.smk_db;
CREATE TABLE ddl_smk.smk_db.t_cmt (k INT, v INT);
ALTER TABLE ddl_smk.smk_db.t_cmt ALTER COLUMN v COMMENT 'value column';
SHOW CREATE TABLE ddl_smk.smk_db.t_cmt;
DROP TABLE ddl_smk.smk_db.t_cmt;
EOF
```

Outcome decision for Task 4 (alter_column_comment):
- ALTER COLUMN COMMENT accepted + SHOW CREATE TABLE shows the comment → Task 4 proceeds normally.
- ALTER COLUMN COMMENT rejected → Task 4 becomes a defer; add a follow-up fix task and skip the case.

**Smoke B — `ALTER TABLE ... ADD COLUMN parent.child TYPE` on STRUCT:**

```bash
mysql -h 127.0.0.1 -P "$NOVA_ENV_MYSQL_PORT" -u root --protocol=TCP <<EOF
CREATE TABLE ddl_smk.smk_db.t_st (
  c0 INT,
  c1 STRUCT<v1 INT, v2 STRUCT<v3 INT, v4 INT>>
);
ALTER TABLE ddl_smk.smk_db.t_st ADD COLUMN c1.val1 INT;
ALTER TABLE ddl_smk.smk_db.t_st DROP COLUMN c1.v1;
-- Negative: drop the last remaining field of a STRUCT.
CREATE TABLE ddl_smk.smk_db.t_last (c0 INT, c1 STRUCT<v1 INT>);
ALTER TABLE ddl_smk.smk_db.t_last DROP COLUMN c1.v1;
SHOW CREATE TABLE ddl_smk.smk_db.t_last;
DROP TABLE ddl_smk.smk_db.t_st;
DROP TABLE ddl_smk.smk_db.t_last;
EOF
```

Outcome decision for Tasks 5, 6, 7 (drop_last_field, drop_add_same_name_field, add_drop_field_struct):
- ADD COLUMN dotted-path + DROP COLUMN dotted-path both work → proceed.
- DROP COLUMN of the last field is rejected with some error (any error — Iceberg may say "cannot drop the only field" or "STRUCT must have at least one field") → record the error string for use in the `@expect_error=` annotation; proceed.
- ADD COLUMN dotted-path rejected → stop, report.

**Smoke C — `ALTER TABLE ... ADD COLUMN ... DEFAULT v`:**

```bash
mysql -h 127.0.0.1 -P "$NOVA_ENV_MYSQL_PORT" -u root --protocol=TCP <<EOF
CREATE TABLE ddl_smk.smk_db.t_def (id INT, v INT) TBLPROPERTIES ("format-version"="3");
INSERT INTO ddl_smk.smk_db.t_def VALUES (1, 10);
ALTER TABLE ddl_smk.smk_db.t_def ADD COLUMN c INT DEFAULT 42;
SELECT id, v, c FROM ddl_smk.smk_db.t_def;  -- expect (1, 10, 42) per Iceberg initial-default
INSERT INTO ddl_smk.smk_db.t_def (id, v) VALUES (2, 20);
SELECT id, v, c FROM ddl_smk.smk_db.t_def ORDER BY id;  -- expect (1,10,42),(2,20,42) per write-default
DROP TABLE ddl_smk.smk_db.t_def;
EOF
```

Outcome decision for Task 9 (default-value batch):
- All three SELECTs return as commented → proceed with all 9 default cases.
- ALTER ADD COLUMN ... DEFAULT rejected → defer all 9 default cases; add a single follow-up fix task ("Iceberg DEFAULT value support on ADD COLUMN").
- ADD COLUMN ... DEFAULT works but INSERT subset doesn't fill DEFAULT → record this gap; default cases land partially (ADD-COLUMN-DEFAULT-only test points) and the INSERT-default test points are deferred.

**Cleanup:**

```bash
mysql -h 127.0.0.1 -P "$NOVA_ENV_MYSQL_PORT" -u root --protocol=TCP <<'EOF'
DROP DATABASE ddl_smk.smk_db FORCE;
DROP CATALOG ddl_smk;
EOF
```

- [ ] **Step 1: Run Smoke A; capture verdict**
- [ ] **Step 2: Run Smoke B; capture verdict + the actual "drop last field" error string**
- [ ] **Step 3: Run Smoke C; capture verdict**
- [ ] **Step 4: Clean up the scratch catalog**
- [ ] **Step 5: Record outcomes in the implementer's report — no commit**

---

## Task 3: Migrate `add_column_count_star`

**Files:**
- Create: `sql-tests/iceberg-ddl/sql/add_column_count_star.sql`
- Create: `sql-tests/iceberg-ddl/result/add_column_count_star.result`

- [ ] **Step 1: Write `sql-tests/iceberg-ddl/sql/add_column_count_star.sql`**

```sql
-- @order_sensitive=true
-- @tags=iceberg_ddl
-- Test Objective:
-- 1. Validate ALTER TABLE ADD COLUMN on an Iceberg table preserves COUNT(*).
-- 2. Verify the new column is visible with NULL backfill after schema change.
-- Iceberg ALTER is synchronous at metadata commit, so no sleep/retry is needed
-- between the ALTER and the follow-up reads.

-- query 1
-- @skip_result_check=true
DROP TABLE IF EXISTS ${case_db}.t0;
CREATE TABLE ${case_db}.t0 (
  k1 INT,
  c1 INT
);
INSERT INTO ${case_db}.t0 VALUES (1, 1);

-- query 2
-- @order_sensitive=true
SELECT count(*) AS row_count FROM ${case_db}.t0;

-- query 3
-- @order_sensitive=true
SELECT k1, c1 FROM ${case_db}.t0 ORDER BY k1;

-- query 4
-- @skip_result_check=true
ALTER TABLE ${case_db}.t0 ADD COLUMN b1 BOOLEAN;

-- query 5
-- @order_sensitive=true
SELECT count(*) AS row_count FROM ${case_db}.t0;

-- query 6
-- @order_sensitive=true
SELECT k1, c1, b1 FROM ${case_db}.t0 ORDER BY k1;
```

- [ ] **Step 2: Record**

```bash
source docker/iceberg-rest/runtime/current/env.sh
$SQLT --config "$NOVAROCKS_SQL_TEST_CONFIG" \
  --suite iceberg-ddl --only add_column_count_star \
  --mode record --record-from target
```

- [ ] **Step 3: Spot-check**

```bash
cat sql-tests/iceberg-ddl/result/add_column_count_star.result
```

Expected: query 2 = 1 row (row_count=1); query 3 = 1 row (1, 1); query 5 = 1 row (row_count=1); query 6 = 1 row (1, 1, NULL).

- [ ] **Step 4: Verify**

```bash
$SQLT --config "$NOVAROCKS_SQL_TEST_CONFIG" \
  --suite iceberg-ddl --only add_column_count_star \
  --mode verify
```

- [ ] **Step 5: Commit**

```bash
git add sql-tests/iceberg-ddl/sql/add_column_count_star.sql sql-tests/iceberg-ddl/result/add_column_count_star.result
git rm sql-tests/iceberg-ddl/sql/.gitkeep sql-tests/iceberg-ddl/result/.gitkeep
git commit -m "test(iceberg-ddl): add ADD COLUMN count(*) preservation case"
```

---

## Task 4: Migrate `alter_column_comment` (gated on Task 2 Smoke A)

**Skip if Smoke A failed.** Stop the plan and report BLOCKED with the failing statement; add a follow-up fix task for ALTER COLUMN COMMENT support on Iceberg.

**Files:**
- Create: `sql-tests/iceberg-ddl/sql/alter_column_comment.sql`
- Create: `sql-tests/iceberg-ddl/result/alter_column_comment.result`

- [ ] **Step 1: Write `sql-tests/iceberg-ddl/sql/alter_column_comment.sql`**

```sql
-- @tags=iceberg_ddl
-- Test Objective:
-- 1. Validate ALTER TABLE ... ALTER COLUMN ... COMMENT 'x' on an Iceberg table.
-- 2. Verify SHOW CREATE TABLE reflects the updated comments.

-- query 1
-- @skip_result_check=true
DROP TABLE IF EXISTS ${case_db}.t;
CREATE TABLE ${case_db}.t (k INT, v INT);

-- query 2
-- Verify initial column comments are absent (or empty).
-- @skip_result_check=true
SHOW CREATE TABLE ${case_db}.t;

-- query 3
-- @skip_result_check=true
ALTER TABLE ${case_db}.t ALTER COLUMN k COMMENT 'k';
ALTER TABLE ${case_db}.t ALTER COLUMN v COMMENT 'v';

-- query 4
-- Verify updated comments appear in SHOW CREATE TABLE output.
-- @result_contains=k
-- @result_contains=v
-- @skip_result_check=true
SHOW CREATE TABLE ${case_db}.t;
```

The substring assertions on the comments are deliberately loose (just `k` and `v`) because the exact `COMMENT "..."` format in Iceberg's SHOW CREATE TABLE output may differ from StarRocks's. After recording, eyeball the result file and **tighten the assertions** to match the actual Iceberg output exactly (e.g., `COMMENT 'k'` or `COMMENT "k"` depending on what NovaRocks emits).

- [ ] **Step 2: Record**

```bash
$SQLT --config "$NOVAROCKS_SQL_TEST_CONFIG" \
  --suite iceberg-ddl --only alter_column_comment \
  --mode record --record-from target
cat sql-tests/iceberg-ddl/result/alter_column_comment.result
```

- [ ] **Step 3: Tighten `@result_contains` substrings**

Edit `sql-tests/iceberg-ddl/sql/alter_column_comment.sql` query 4 — replace the loose `@result_contains=k` / `@result_contains=v` with the actual Iceberg COMMENT fragments from the recorded result (e.g., `@result_contains=COMMENT 'k'`).

- [ ] **Step 4: Re-record + verify**

```bash
$SQLT --config "$NOVAROCKS_SQL_TEST_CONFIG" \
  --suite iceberg-ddl --only alter_column_comment \
  --mode record --record-from target
$SQLT --config "$NOVAROCKS_SQL_TEST_CONFIG" \
  --suite iceberg-ddl --only alter_column_comment \
  --mode verify
```

- [ ] **Step 5: Commit**

```bash
git add sql-tests/iceberg-ddl/sql/alter_column_comment.sql sql-tests/iceberg-ddl/result/alter_column_comment.result
git commit -m "test(iceberg-ddl): add ALTER COLUMN COMMENT round-trip case"
```

---

## Task 5: Migrate `drop_last_field`

The actual error string for "drop last field of STRUCT" was captured in Task 2 Smoke B. Substitute it into the `@expect_error` annotation below as `<smoke_b_last_field_error_substring>`.

**Files:**
- Create: `sql-tests/iceberg-ddl/sql/drop_last_field.sql`
- Create: `sql-tests/iceberg-ddl/result/drop_last_field.result`

- [ ] **Step 1: Write `sql-tests/iceberg-ddl/sql/drop_last_field.sql`**

```sql
-- @tags=iceberg_ddl,struct
-- Test Objective:
-- 1. Dropping the final remaining field of a STRUCT is rejected on Iceberg.

-- query 1
-- @skip_result_check=true
DROP TABLE IF EXISTS ${case_db}.tab1;
CREATE TABLE ${case_db}.tab1 (
  c0 INT,
  c1 STRUCT<v1 INT>
);

-- query 2
-- @expect_error=<smoke_b_last_field_error_substring>
ALTER TABLE ${case_db}.tab1 DROP COLUMN c1.v1;
```

Substitute `<smoke_b_last_field_error_substring>` with the substring captured in Task 2 Smoke B (the error message returned when dropping the last field). Examples of plausible substrings: `cannot drop`, `must have at least one`, `last field`. Whatever the actual error contains, use a distinctive substring.

- [ ] **Step 2: Record + spot-check + verify + commit**

```bash
$SQLT --config "$NOVAROCKS_SQL_TEST_CONFIG" \
  --suite iceberg-ddl --only drop_last_field \
  --mode record --record-from target
cat sql-tests/iceberg-ddl/result/drop_last_field.result
# Expect: query 2 records the error matching @expect_error.
$SQLT --config "$NOVAROCKS_SQL_TEST_CONFIG" \
  --suite iceberg-ddl --only drop_last_field \
  --mode verify
git add sql-tests/iceberg-ddl/sql/drop_last_field.sql sql-tests/iceberg-ddl/result/drop_last_field.result
git commit -m "test(iceberg-ddl): add drop_last_field negative case (STRUCT last-field guard)"
```

If the actual error string doesn't contain the substring you used — the recorded result will fail @expect_error matching. Adjust the substring once based on the actual recorded error and re-record. This is matching the test infrastructure to actual engine output, not weakening the test point.

---

## Task 6: Migrate `drop_add_same_name_field`

DDL-only — STRUCT INSERT data round-trips are dropped per the existing iceberg-suite limitation. The test point becomes: "drop a STRUCT field, re-add with a different type, the SELECT after re-add works and the original column is still queryable."

**Files:**
- Create: `sql-tests/iceberg-ddl/sql/drop_add_same_name_field.sql`
- Create: `sql-tests/iceberg-ddl/result/drop_add_same_name_field.result`

- [ ] **Step 1: Write `sql-tests/iceberg-ddl/sql/drop_add_same_name_field.sql`**

```sql
-- @tags=iceberg_ddl,struct
-- Test Objective:
-- 1. Validate dropping then re-adding a STRUCT field with the same name but a different
--    type is accepted on Iceberg.
-- 2. Verify SELECT after re-add returns the new type's NULL for older rows.

-- query 1
-- @skip_result_check=true
DROP TABLE IF EXISTS ${case_db}.t;
CREATE TABLE ${case_db}.t (
  c1 INT,
  c2 STRUCT<v2_1 INT>
);

-- query 2
-- @skip_result_check=true
ALTER TABLE ${case_db}.t ADD COLUMN c2.v2_2 STRING;

-- query 3
-- @skip_result_check=true
ALTER TABLE ${case_db}.t DROP COLUMN c2.v2_2;

-- query 4
-- @skip_result_check=true
ALTER TABLE ${case_db}.t ADD COLUMN c2.v2_2 DATE;

-- query 5
-- @order_sensitive=true
SELECT c1 FROM ${case_db}.t ORDER BY c1;
```

The trailing SELECT just confirms the table is still queryable on the non-STRUCT column. Per `iceberg_schema_evolution_nested.sql:3-7`, STRUCT INSERT and SELECT-with-nested-projection on NovaRocks's standalone Iceberg path is limited, so the test is DDL-only.

- [ ] **Step 2: Record + spot-check + verify + commit**

```bash
$SQLT --config "$NOVAROCKS_SQL_TEST_CONFIG" \
  --suite iceberg-ddl --only drop_add_same_name_field \
  --mode record --record-from target
cat sql-tests/iceberg-ddl/result/drop_add_same_name_field.result
# Expect: query 5 returns 0 rows (table never had inserts).
$SQLT --config "$NOVAROCKS_SQL_TEST_CONFIG" \
  --suite iceberg-ddl --only drop_add_same_name_field \
  --mode verify
git add sql-tests/iceberg-ddl/sql/drop_add_same_name_field.sql sql-tests/iceberg-ddl/result/drop_add_same_name_field.result
git commit -m "test(iceberg-ddl): add drop+re-add same STRUCT field case (DDL-only)"
```

---

## Task 7: Migrate `add_drop_field_struct`

DDL-only, exercising both nested STRUCT ADD COLUMN/DROP COLUMN happy paths and the original case's negative paths translated to Iceberg dotted-path syntax.

**Files:**
- Create: `sql-tests/iceberg-ddl/sql/add_drop_field_struct.sql`
- Create: `sql-tests/iceberg-ddl/result/add_drop_field_struct.result`

- [ ] **Step 1: Write `sql-tests/iceberg-ddl/sql/add_drop_field_struct.sql`**

```sql
-- @tags=iceberg_ddl,struct
-- Test Objective:
-- 1. Validate nested STRUCT field add/drop via dotted-path syntax on Iceberg.
-- 2. Verify add-field on a non-struct path is rejected.
-- 3. Verify add-field for an already-existing field is rejected.
-- 4. Verify drop-field for a non-existent field is rejected.
-- Note: NovaRocks's standalone INSERT path does not currently support STRUCT
-- column writes (see sql-tests/iceberg/sql/iceberg_schema_evolution_nested.sql),
-- so this test is DDL-only.

-- query 1
-- @skip_result_check=true
DROP TABLE IF EXISTS ${case_db}.tab1;
CREATE TABLE ${case_db}.tab1 (
  c0 INT,
  c1 STRUCT<v1 INT, v2 STRUCT<v3 INT, v4 INT>>
);

-- query 2
-- Negative: cannot ADD COLUMN under a non-struct path (v1 is an INT, not a STRUCT).
-- @expect_error=not a struct
ALTER TABLE ${case_db}.tab1 ADD COLUMN c1.v1.v5 INT;

-- query 3
-- Negative: cannot ADD COLUMN with an existing top-level field name (v2).
-- @expect_error=already exists
ALTER TABLE ${case_db}.tab1 ADD COLUMN c1.v2 INT;

-- query 4
-- Negative: cannot ADD COLUMN with an existing nested field name (v2.v3).
-- @expect_error=already exists
ALTER TABLE ${case_db}.tab1 ADD COLUMN c1.v2.v3 INT;

-- query 5
-- Positive: add a new top-level field.
-- @skip_result_check=true
ALTER TABLE ${case_db}.tab1 ADD COLUMN c1.val1 INT;

-- query 6
-- Negative: cannot DROP COLUMN a non-existent nested field.
-- @expect_error=not found
ALTER TABLE ${case_db}.tab1 DROP COLUMN c1.v2.v5;

-- query 7
-- Positive: drop a top-level field.
-- @skip_result_check=true
ALTER TABLE ${case_db}.tab1 DROP COLUMN c1.v1;

-- query 8
-- Positive: re-add a previously-dropped field name with a new type.
-- @skip_result_check=true
ALTER TABLE ${case_db}.tab1 ADD COLUMN c1.v1 INT;
```

The `@expect_error` substrings (`not a struct`, `already exists`, `not found`) are educated guesses — the exact NovaRocks Iceberg error messages may differ. After recording, tighten each substring based on the actual error.

- [ ] **Step 2: Record**

```bash
$SQLT --config "$NOVAROCKS_SQL_TEST_CONFIG" \
  --suite iceberg-ddl --only add_drop_field_struct \
  --mode record --record-from target
cat sql-tests/iceberg-ddl/result/add_drop_field_struct.result
```

- [ ] **Step 3: Tighten `@expect_error` substrings**

For each query 2 / 3 / 4 / 6, look at the recorded actual error and update the substring to match. Distinct substrings per case (so a "not found" message can't match an "already exists" case).

- [ ] **Step 4: Re-record + verify + commit**

```bash
$SQLT --config "$NOVAROCKS_SQL_TEST_CONFIG" \
  --suite iceberg-ddl --only add_drop_field_struct \
  --mode record --record-from target
$SQLT --config "$NOVAROCKS_SQL_TEST_CONFIG" \
  --suite iceberg-ddl --only add_drop_field_struct \
  --mode verify
git add sql-tests/iceberg-ddl/sql/add_drop_field_struct.sql sql-tests/iceberg-ddl/result/add_drop_field_struct.result
git commit -m "test(iceberg-ddl): add nested STRUCT field evolution case (DDL-only)"
```

---

## Task 8: Migrate `add_drop_field_array`

ARRAY<STRUCT> field evolution on Iceberg uses the `c1.element.field` notation (Iceberg's `element` is the array element's pseudo-name).

**Files:**
- Create: `sql-tests/iceberg-ddl/sql/add_drop_field_array.sql`
- Create: `sql-tests/iceberg-ddl/result/add_drop_field_array.result`

- [ ] **Step 1: Smoke first — confirm Iceberg `element` notation works on NovaRocks**

```bash
source docker/iceberg-rest/runtime/current/env.sh
HW=$(grep '^iceberg_catalog_warehouse' "$NOVAROCKS_SQL_TEST_CONFIG" | sed 's/.*= //')
mysql -h 127.0.0.1 -P "$NOVA_ENV_MYSQL_PORT" -u root --protocol=TCP <<EOF
CREATE EXTERNAL CATALOG IF NOT EXISTS arr_smk PROPERTIES (
  "type"="iceberg", "iceberg.catalog.type"="hadoop", "iceberg.catalog.warehouse"="$HW",
  "aws.s3.access_key"="admin", "aws.s3.secret_key"="admin123",
  "aws.s3.endpoint"="http://127.0.0.1:9000", "aws.s3.enable_path_style_access"="true"
);
CREATE DATABASE arr_smk.db;
CREATE TABLE arr_smk.db.t (c0 INT, c1 ARRAY<STRUCT<v1 INT, v2 INT>>);
ALTER TABLE arr_smk.db.t ADD COLUMN c1.element.val1 INT;
ALTER TABLE arr_smk.db.t DROP COLUMN c1.element.v1;
SHOW CREATE TABLE arr_smk.db.t;
DROP DATABASE arr_smk.db FORCE;
DROP CATALOG arr_smk;
EOF
```

If either ALTER is rejected, the Iceberg `element` notation isn't supported on NovaRocks. **Stop and report BLOCKED**. Add a follow-up fix task: "ARRAY<STRUCT> element field evolution on Iceberg". Do NOT skip the test case — surface the gap.

If both ALTERs succeed, the SHOW CREATE TABLE output should reflect the new field set on the element struct.

- [ ] **Step 2: Write `sql-tests/iceberg-ddl/sql/add_drop_field_array.sql`**

```sql
-- @tags=iceberg_ddl,struct,array
-- Test Objective:
-- 1. Validate ARRAY<STRUCT> element field add/drop via Iceberg `element` notation.
-- 2. Verify negative paths (add to non-struct element path, drop non-existent field).
-- Note: NovaRocks standalone INSERT for ARRAY<STRUCT> column writes is limited,
-- so this test is DDL-only.

-- query 1
-- @skip_result_check=true
DROP TABLE IF EXISTS ${case_db}.tab1;
CREATE TABLE ${case_db}.tab1 (
  c0 INT,
  c1 ARRAY<STRUCT<v1 INT, v2 INT>>
);

-- query 2
-- Negative: cannot DROP the array itself (not a struct path).
-- @expect_error=not a struct
ALTER TABLE ${case_db}.tab1 DROP COLUMN c1.element;

-- query 3
-- Negative: cannot DROP a non-existent element field.
-- @expect_error=not found
ALTER TABLE ${case_db}.tab1 DROP COLUMN c1.element.v3;

-- query 4
-- Positive: add a new field to the array's element struct.
-- @skip_result_check=true
ALTER TABLE ${case_db}.tab1 ADD COLUMN c1.element.val1 INT;

-- query 5
-- Positive: drop a previously-existing element field.
-- @skip_result_check=true
ALTER TABLE ${case_db}.tab1 DROP COLUMN c1.element.v1;

-- query 6
-- Positive: re-add a previously-dropped element field name.
-- @skip_result_check=true
ALTER TABLE ${case_db}.tab1 ADD COLUMN c1.element.v1 INT;
```

The `@expect_error` substrings are educated guesses. Tighten them after recording.

- [ ] **Step 3: Record + tighten + re-record + verify + commit**

```bash
$SQLT --config "$NOVAROCKS_SQL_TEST_CONFIG" \
  --suite iceberg-ddl --only add_drop_field_array \
  --mode record --record-from target
cat sql-tests/iceberg-ddl/result/add_drop_field_array.result
# Tighten @expect_error substrings to actual messages.
# Re-record after editing the .sql:
$SQLT --config "$NOVAROCKS_SQL_TEST_CONFIG" \
  --suite iceberg-ddl --only add_drop_field_array \
  --mode record --record-from target
$SQLT --config "$NOVAROCKS_SQL_TEST_CONFIG" \
  --suite iceberg-ddl --only add_drop_field_array \
  --mode verify
git add sql-tests/iceberg-ddl/sql/add_drop_field_array.sql sql-tests/iceberg-ddl/result/add_drop_field_array.result
git commit -m "test(iceberg-ddl): add ARRAY<STRUCT> element field evolution case (DDL-only)"
```

---

## Task 9: Default-value batch (gated on Task 2 Smoke C)

**Skip the whole task if Smoke C failed.** Add a follow-up fix task: "Iceberg DEFAULT value support on ALTER ADD COLUMN" and proceed to Task 10.

The 9 default cases are written in one batch with one commit each (or grouped if convenient). All follow the same template: CREATE TABLE → INSERT seed rows → ALTER ADD COLUMN ... DEFAULT v → verify the new column reads the default value for both old and new rows.

Each case strips the source's StarRocks-specific scaffolding (DUP/UNIQUE/PRIMARY KEY tables, fast_schema_evolution properties, partial_update_mode, AGG SUM/MAX/MIN/REPLACE column attributes) and keeps only the per-type DEFAULT semantic.

**Files (9 cases):**
- Create: `sql-tests/iceberg-ddl/sql/default_boolean.sql`
- Create: `sql-tests/iceberg-ddl/sql/default_numeric.sql`
- Create: `sql-tests/iceberg-ddl/sql/default_decimal.sql`
- Create: `sql-tests/iceberg-ddl/sql/default_string.sql`
- Create: `sql-tests/iceberg-ddl/sql/default_date.sql`
- Create: `sql-tests/iceberg-ddl/sql/default_varbinary.sql`
- Create: `sql-tests/iceberg-ddl/sql/default_json.sql`
- Create: `sql-tests/iceberg-ddl/sql/default_json_strict_validation.sql`
- Create: `sql-tests/iceberg-ddl/sql/default_complex.sql`
- Plus the matching 9 `.result` files

- [ ] **Step 1: Write `sql-tests/iceberg-ddl/sql/default_boolean.sql`**

```sql
-- @order_sensitive=true
-- @tags=iceberg_ddl,default
-- Test Objective:
-- 1. ALTER TABLE ADD COLUMN BOOLEAN DEFAULT v applies the default to existing
--    rows (Iceberg initial-default) and to subsequent INSERT-with-subset-columns
--    (Iceberg write-default).

DROP TABLE IF EXISTS ${case_db}.t;
CREATE TABLE ${case_db}.t (id INT, name STRING) TBLPROPERTIES ("format-version" = "3");
INSERT INTO ${case_db}.t VALUES (1, 'alice'), (2, 'bob');
ALTER TABLE ${case_db}.t ADD COLUMN flag BOOLEAN DEFAULT true;
SELECT id, name, flag FROM ${case_db}.t ORDER BY id;
INSERT INTO ${case_db}.t (id, name) VALUES (3, 'charlie');
SELECT id, name, flag FROM ${case_db}.t ORDER BY id;
```

- [ ] **Step 2: Write `sql-tests/iceberg-ddl/sql/default_numeric.sql`**

```sql
-- @order_sensitive=true
-- @tags=iceberg_ddl,default,numeric
-- Test Objective:
-- 1. ALTER ADD COLUMN with INT/BIGINT/FLOAT/DOUBLE/SMALLINT DEFAULT v applies
--    correctly to old rows and new rows. SMALLINT is exercised; TINYINT is not
--    an Iceberg primitive so the source's TINYINT coverage is dropped.
-- 2. Negative, zero, and boundary numeric defaults are stored verbatim.

DROP TABLE IF EXISTS ${case_db}.t;
CREATE TABLE ${case_db}.t (id INT, name STRING) TBLPROPERTIES ("format-version" = "3");
INSERT INTO ${case_db}.t VALUES (1, 'alice'), (2, 'bob');

ALTER TABLE ${case_db}.t ADD COLUMN score SMALLINT DEFAULT 100;
ALTER TABLE ${case_db}.t ADD COLUMN salary INT DEFAULT 50000;
ALTER TABLE ${case_db}.t ADD COLUMN revenue BIGINT DEFAULT 1000000;
ALTER TABLE ${case_db}.t ADD COLUMN rating FLOAT DEFAULT 4.5;
ALTER TABLE ${case_db}.t ADD COLUMN percentage DOUBLE DEFAULT 95.5;
ALTER TABLE ${case_db}.t ADD COLUMN zero_v INT DEFAULT 0;
ALTER TABLE ${case_db}.t ADD COLUMN neg_v INT DEFAULT -100;

SELECT id, name, score, salary, revenue, rating, percentage, zero_v, neg_v
FROM ${case_db}.t
ORDER BY id;

INSERT INTO ${case_db}.t (id, name) VALUES (3, 'charlie');

SELECT id, name, score, salary, revenue, rating, percentage, zero_v, neg_v
FROM ${case_db}.t
ORDER BY id;
```

If `SMALLINT` is rejected by NovaRocks's Iceberg DDL path, downgrade to `INT` and note it in the case header. (Iceberg supports INT and LONG; SMALLINT depends on NovaRocks's type mapping.)

- [ ] **Step 3: Write `sql-tests/iceberg-ddl/sql/default_decimal.sql`**

```sql
-- @order_sensitive=true
-- @tags=iceberg_ddl,default,decimal
-- Test Objective:
-- 1. ALTER ADD COLUMN DECIMAL(p, s) DEFAULT v applies correctly to old + new rows.

DROP TABLE IF EXISTS ${case_db}.t;
CREATE TABLE ${case_db}.t (id INT, name STRING) TBLPROPERTIES ("format-version" = "3");
INSERT INTO ${case_db}.t VALUES (1, 'alice'), (2, 'bob');

ALTER TABLE ${case_db}.t ADD COLUMN price DECIMAL(10, 2) DEFAULT 9.99;
ALTER TABLE ${case_db}.t ADD COLUMN rate DECIMAL(5, 4) DEFAULT 0.1234;
ALTER TABLE ${case_db}.t ADD COLUMN big DECIMAL(20, 6) DEFAULT 123456789.000001;

SELECT id, name, price, rate, big FROM ${case_db}.t ORDER BY id;

INSERT INTO ${case_db}.t (id, name) VALUES (3, 'charlie');
SELECT id, name, price, rate, big FROM ${case_db}.t ORDER BY id;
```

- [ ] **Step 4: Write `sql-tests/iceberg-ddl/sql/default_string.sql`**

```sql
-- @order_sensitive=true
-- @tags=iceberg_ddl,default,string
-- Test Objective:
-- 1. ALTER ADD COLUMN STRING / VARCHAR DEFAULT v applies correctly.
-- 2. Empty-string and special-character defaults survive round-trip.
-- Note: CHAR(N) was in the source; Iceberg has no fixed-CHAR so we widen to STRING.

DROP TABLE IF EXISTS ${case_db}.t;
CREATE TABLE ${case_db}.t (id INT, name STRING) TBLPROPERTIES ("format-version" = "3");
INSERT INTO ${case_db}.t VALUES (1, 'alice'), (2, 'bob');

ALTER TABLE ${case_db}.t ADD COLUMN tag STRING DEFAULT 'default';
ALTER TABLE ${case_db}.t ADD COLUMN empty_v STRING DEFAULT '';
ALTER TABLE ${case_db}.t ADD COLUMN unicode_v STRING DEFAULT '日本語';
ALTER TABLE ${case_db}.t ADD COLUMN special_v STRING DEFAULT 'a,b\nc';

SELECT id, name, tag, empty_v, unicode_v, special_v FROM ${case_db}.t ORDER BY id;

INSERT INTO ${case_db}.t (id, name) VALUES (3, 'charlie');
SELECT id, name, tag, empty_v, unicode_v, special_v FROM ${case_db}.t ORDER BY id;
```

- [ ] **Step 5: Write `sql-tests/iceberg-ddl/sql/default_date.sql`**

```sql
-- @order_sensitive=true
-- @tags=iceberg_ddl,default,date
-- Test Objective:
-- 1. ALTER ADD COLUMN DATE / DATETIME DEFAULT v applies correctly.
-- 2. Various date / datetime literal forms are accepted as defaults.

DROP TABLE IF EXISTS ${case_db}.t;
CREATE TABLE ${case_db}.t (id INT, name STRING) TBLPROPERTIES ("format-version" = "3");
INSERT INTO ${case_db}.t VALUES (1, 'alice'), (2, 'bob');

ALTER TABLE ${case_db}.t ADD COLUMN d DATE DEFAULT '2024-01-01';
ALTER TABLE ${case_db}.t ADD COLUMN dt DATETIME DEFAULT '2024-01-01 12:00:00';

SELECT id, name, d, dt FROM ${case_db}.t ORDER BY id;

INSERT INTO ${case_db}.t (id, name) VALUES (3, 'charlie');
SELECT id, name, d, dt FROM ${case_db}.t ORDER BY id;
```

- [ ] **Step 6: Write `sql-tests/iceberg-ddl/sql/default_varbinary.sql`**

```sql
-- @order_sensitive=true
-- @tags=iceberg_ddl,default,varbinary
-- Test Objective:
-- 1. ALTER ADD COLUMN VARBINARY DEFAULT v applies correctly.

DROP TABLE IF EXISTS ${case_db}.t;
CREATE TABLE ${case_db}.t (id INT, name STRING) TBLPROPERTIES ("format-version" = "3");
INSERT INTO ${case_db}.t VALUES (1, 'alice'), (2, 'bob');

ALTER TABLE ${case_db}.t ADD COLUMN payload VARBINARY DEFAULT 'abc';

SELECT id, name, payload FROM ${case_db}.t ORDER BY id;

INSERT INTO ${case_db}.t (id, name) VALUES (3, 'charlie');
SELECT id, name, payload FROM ${case_db}.t ORDER BY id;
```

- [ ] **Step 7: Write `sql-tests/iceberg-ddl/sql/default_json.sql`**

```sql
-- @order_sensitive=true
-- @tags=iceberg_ddl,default,json
-- Test Objective:
-- 1. ALTER ADD COLUMN JSON DEFAULT v applies correctly with a valid JSON literal.

DROP TABLE IF EXISTS ${case_db}.t;
CREATE TABLE ${case_db}.t (id INT, name STRING) TBLPROPERTIES ("format-version" = "3");
INSERT INTO ${case_db}.t VALUES (1, 'alice'), (2, 'bob');

ALTER TABLE ${case_db}.t ADD COLUMN meta JSON DEFAULT '{"k":"v"}';

SELECT id, name, meta FROM ${case_db}.t ORDER BY id;

INSERT INTO ${case_db}.t (id, name) VALUES (3, 'charlie');
SELECT id, name, meta FROM ${case_db}.t ORDER BY id;
```

If NovaRocks's Iceberg path doesn't support JSON columns, stop and report — do not switch to STRING-with-JSON-literal as a workaround.

- [ ] **Step 8: Write `sql-tests/iceberg-ddl/sql/default_json_strict_validation.sql`**

```sql
-- @tags=iceberg_ddl,default,json,validation
-- Test Objective:
-- 1. Invalid JSON in DEFAULT is rejected at ALTER time.

DROP TABLE IF EXISTS ${case_db}.t;
CREATE TABLE ${case_db}.t (id INT, name STRING) TBLPROPERTIES ("format-version" = "3");

-- @expect_error=JSON
ALTER TABLE ${case_db}.t ADD COLUMN meta JSON DEFAULT 'not-a-json';
```

After recording, tighten `@expect_error` to the actual NovaRocks error substring.

- [ ] **Step 9: Write `sql-tests/iceberg-ddl/sql/default_complex.sql`**

Consolidates `test_complex_default_all_paths` + `test_complex_default_correctness`. ARRAY/MAP/STRUCT default literals depend on NovaRocks's Iceberg-side ADD COLUMN DEFAULT supporting compound literals — uncertain. The case writes the most representative subset; if any literal form is rejected, surface the gap.

```sql
-- @order_sensitive=true
-- @tags=iceberg_ddl,default,complex
-- Test Objective:
-- 1. ALTER ADD COLUMN with ARRAY / MAP / STRUCT DEFAULT applies for empty + small literals.

DROP TABLE IF EXISTS ${case_db}.t;
CREATE TABLE ${case_db}.t (id INT, name STRING) TBLPROPERTIES ("format-version" = "3");
INSERT INTO ${case_db}.t VALUES (1, 'alice'), (2, 'bob');

ALTER TABLE ${case_db}.t ADD COLUMN tags ARRAY<INT> DEFAULT '[]';
ALTER TABLE ${case_db}.t ADD COLUMN counts MAP<STRING, INT> DEFAULT '{}';

SELECT id, name, tags, counts FROM ${case_db}.t ORDER BY id;

INSERT INTO ${case_db}.t (id, name) VALUES (3, 'charlie');
SELECT id, name, tags, counts FROM ${case_db}.t ORDER BY id;
```

If ARRAY/MAP literal defaults are rejected, surface and add a follow-up fix task. STRUCT default literals are deferred to that follow-up.

- [ ] **Step 10: Record all 9 cases**

```bash
source docker/iceberg-rest/runtime/current/env.sh
$SQLT --config "$NOVAROCKS_SQL_TEST_CONFIG" \
  --suite iceberg-ddl --only default_boolean,default_numeric,default_decimal,default_string,default_date,default_varbinary,default_json,default_json_strict_validation,default_complex \
  --mode record --record-from target
```

- [ ] **Step 11: Per-case spot-check**

```bash
for c in default_boolean default_numeric default_decimal default_string default_date default_varbinary default_json default_json_strict_validation default_complex; do
  echo "=== $c ==="
  cat sql-tests/iceberg-ddl/result/$c.result
done
```

For each: confirm both the "old rows show default" and "new INSERT-subset rows show default" parts are correct. If any case fails for an underlying NovaRocks gap (e.g., JSON column unsupported, MAP literal default rejected), capture the case + error, stop the task, surface as a fix-task candidate. **Don't delete the failing case file silently** — leave the .sql file in working tree but un-commit it, and proceed with the other passing cases.

- [ ] **Step 12: Per-case commit (for the cases that passed)**

Pass per-case batched commit, e.g. one per type bucket or all at once:

```bash
git add sql-tests/iceberg-ddl/sql/default_boolean.sql sql-tests/iceberg-ddl/sql/default_numeric.sql \
        sql-tests/iceberg-ddl/sql/default_decimal.sql sql-tests/iceberg-ddl/sql/default_string.sql \
        sql-tests/iceberg-ddl/sql/default_date.sql sql-tests/iceberg-ddl/sql/default_varbinary.sql \
        sql-tests/iceberg-ddl/result/default_boolean.result sql-tests/iceberg-ddl/result/default_numeric.result \
        sql-tests/iceberg-ddl/result/default_decimal.result sql-tests/iceberg-ddl/result/default_string.result \
        sql-tests/iceberg-ddl/result/default_date.result sql-tests/iceberg-ddl/result/default_varbinary.result
git commit -m "test(iceberg-ddl): add primitive-type DEFAULT cases"

# If JSON cases passed:
git add sql-tests/iceberg-ddl/sql/default_json.sql sql-tests/iceberg-ddl/sql/default_json_strict_validation.sql \
        sql-tests/iceberg-ddl/result/default_json.result sql-tests/iceberg-ddl/result/default_json_strict_validation.result
git commit -m "test(iceberg-ddl): add JSON DEFAULT cases"

# If default_complex passed:
git add sql-tests/iceberg-ddl/sql/default_complex.sql sql-tests/iceberg-ddl/result/default_complex.result
git commit -m "test(iceberg-ddl): add ARRAY/MAP DEFAULT cases"
```

- [ ] **Step 13: Final verify on the committed default cases**

```bash
$SQLT --config "$NOVAROCKS_SQL_TEST_CONFIG" \
  --suite iceberg-ddl --mode verify 2>&1 | grep -E "total=|pass=|fail=" | tail -3
```

---

## Task 10: Whole-suite verify before deletion

- [ ] **Step 1: Run the whole suite**

```bash
source docker/iceberg-rest/runtime/current/env.sh
$SQLT --config "$NOVAROCKS_SQL_TEST_CONFIG" --suite iceberg-ddl --mode verify
```

Expected: every case in `sql/` has a matching `.result` and PASSes verify.

- [ ] **Step 2: Confirm file counts match**

```bash
ls sql-tests/iceberg-ddl/sql/*.sql | wc -l
ls sql-tests/iceberg-ddl/result/*.result | wc -l
```

Both must be equal.

- [ ] **Step 3: If anything fails, stop and report**

Per the no-fallback rule, do not "fix" a failure by removing the case.

---

## Task 11: Delete the three source suites

**Files:**
- Delete: `sql-tests/schema-be/` (entire directory)
- Delete: `sql-tests/schema-change/` (entire directory)
- Delete: `sql-tests/schema-info/` (entire directory)

- [ ] **Step 1: Confirm no other code references these suites by name**

```bash
grep -rn "schema-be\|schema_be\|schema-change\|schema_change\|schema-info\|schema_info" \
  tests/ docs/ sql-tests/ --include="*.rs" --include="*.md" --include="*.sql" --include="*.toml" 2>/dev/null \
  | grep -v "sql-tests/schema-be/\|sql-tests/schema-change/\|sql-tests/schema-info/\|docs/design/specs/2026-05-22-iceberg-ddl\|docs/design/plans/2026-05-22-iceberg-ddl"
```

Expected: any remaining matches are in historical plan/spec docs (acceptable) or stale `@tags=schema_change,...` labels in other suites (harmless). If anything actually depends on the directories (e.g., a runner config that hardcodes the suite name), update it.

- [ ] **Step 2: Remove the three directories**

```bash
git rm -r sql-tests/schema-be sql-tests/schema-change sql-tests/schema-info
```

- [ ] **Step 3: Confirm the runner no longer discovers any of them**

```bash
source docker/iceberg-rest/runtime/current/env.sh
for s in schema-be schema-change schema-info; do
  $SQLT --config "$NOVAROCKS_SQL_TEST_CONFIG" --suite "$s" --mode verify 2>&1 | grep -E "unknown suite|ERROR" | head -1
done
```

Expected: each prints `unknown suite '<name>'`.

- [ ] **Step 4: Commit**

```bash
git commit -m "test: delete schema-be / schema-change / schema-info suites; superseded by iceberg-ddl"
```

---

## Task 12: Final hygiene

- [ ] **Step 1: Format**

```bash
cargo fmt
```

- [ ] **Step 2: Clippy on main crate (catch anything from NovaRocks-side fixes)**

```bash
cargo clippy 2>&1 | tail -5
```

If new warnings were introduced by any fix tasks landed during the migration, address them.

- [ ] **Step 3: Final whole-suite verify**

```bash
source docker/iceberg-rest/runtime/current/env.sh
$SQLT --config "$NOVAROCKS_SQL_TEST_CONFIG" --suite iceberg-ddl --mode verify 2>&1 | grep -E "total=|pass=|fail=" | tail -3
```

Expected: total = number of files in `sql-tests/iceberg-ddl/sql/`, pass = total, fail = 0.

- [ ] **Step 4: Stop the standalone-server**

```bash
kill "$(cat /tmp/novarocks-iceddl.pid)" 2>/dev/null || true
rm -f /tmp/novarocks-iceddl.pid
```

- [ ] **Step 5: Commit any fmt fixups**

```bash
git status
# If fmt made changes:
git add -A
git commit -m "chore: cargo fmt after iceberg-ddl migration"
```

- [ ] **Step 6: Print final summary**

```bash
git log --oneline main..HEAD
```

Expected: a clean stack of focused commits — suite scaffold, smoke (no commit), per-case commits, three-suites delete, optional fmt.

---

## Appendix: One-line summary of what the engineer must remember

1. **No silent fallback.** If an Iceberg DDL feature is rejected (DEFAULT, ALTER COLUMN COMMENT, STRUCT field add/drop, ARRAY<STRUCT> `element` notation, JSON, MAP), **stop and report**. Don't rewrite the case. Don't skip it.
2. **Iceberg dotted-path syntax.** STRUCT field evolution uses `ALTER TABLE t ADD COLUMN parent.child TYPE` and `ALTER TABLE t DROP COLUMN parent.child`. ARRAY<STRUCT> uses `c1.element.field`.
3. **DDL-only for nested types.** NovaRocks's standalone INSERT for STRUCT / ARRAY<STRUCT> columns is limited, so nested-evolution cases skip INSERT data round-trips.
4. **Strip OLAP DDL.** No `DUPLICATE/UNIQUE/PRIMARY/AGGREGATE KEY`, no `DISTRIBUTED BY ... BUCKETS`, no `PROPERTIES("replication_num"=...)`, no `fast_schema_evolution`, no `partial_update_mode`, no `ADMIN SET FRONTEND CONFIG`.
5. **No sleep / retry.** Iceberg ALTER is synchronous at metadata commit.
6. **Catalog is implicit.** Per-case SQL refers to `${case_db}` directly; don't write `SET catalog default_catalog;`.
7. **Commit often.** Each task block ends with focused commits.
