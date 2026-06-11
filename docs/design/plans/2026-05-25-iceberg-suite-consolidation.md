# ddl + iceberg → existing iceberg-* sub-suites Consolidation Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Move every case from `sql-tests/iceberg/` that primarily exercises an existing sub-suite's theme into the matching sub-suite (`iceberg-ddl` or `iceberg-dml`); migrate the salvageable DDL test points from `sql-tests/ddl/` into `iceberg-ddl`; delete `sql-tests/ddl/` entirely.

**Architecture:** Mechanical `git mv` (43 case moves) + catalog-name sed rename per moved file + selective rewrite of `ddl/` cases. After each batch, run `--mode verify` on the destination suite to confirm no regression. Surface any NovaRocks gap as a fix commit in this PR.

**Tech Stack:** sql-tests/ directory layout, `tests/sql-test-runner` (suite auto-discovery), Iceberg hadoop catalog over MinIO via `docker/iceberg-rest`, NovaRocks `standalone-server`.

**Spec:** [docs/design/specs/2026-05-25-iceberg-suite-consolidation-design.md](../specs/2026-05-25-iceberg-suite-consolidation-design.md)

**No-fallback rule** (from spec §6.4): if any moved case fails after the catalog-rename + move, stop and report the exact failing statement + error. Do not silently update the `.result` to match potentially-wrong output.

---

## Catalog rename mapping

All cases moving from `iceberg/` to a sub-suite need the catalog identifier inside the case body renamed:

| Source suite | Source catalog identifier | Destination suite | Destination catalog identifier |
|---|---|---|---|
| `iceberg` | `iceberg_cat_${suite_uuid0}` | `iceberg-ddl` | `iceberg_ddl_cat_${suite_uuid0}` |
| `iceberg` | `iceberg_cat_${suite_uuid0}` | `iceberg-dml` | `iceberg_dml_cat_${suite_uuid0}` |

Each moved file is run through one `sed` substitution.

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
   LOG=/tmp/novarocks-icecon.log
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
   echo "$SRV_PID" > /tmp/novarocks-icecon.pid
   ```

4. Common helper variable used throughout:

   ```bash
   SQLT="cargo run --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests --"
   ```

The server stays up across all tasks. Restart only if it crashes. Sqlite cleanup (`rm -f .../standalone-managed-lake.sqlite`) may be needed if a prior crash left stale state — see PR #172's experience.

Baseline counts before any work in this PR:
- `iceberg/` — 67 cases
- `iceberg-ddl/` — 15 cases
- `iceberg-dml/` — 24 cases
- `ddl/` — 6 cases

---

## Task 1: Smoke `CREATE TABLE LIKE` on Iceberg

Determines whether ddl suite's `ddl_create_table_like.sql` can be migrated to iceberg-ddl. No commit from this task.

- [ ] **Step 1: Run the smoke**

```bash
source docker/iceberg-rest/runtime/current/env.sh
HW=$(grep '^iceberg_catalog_warehouse' "$NOVAROCKS_SQL_TEST_CONFIG" | sed 's/.*= //')
mysql -h 127.0.0.1 -P 9055 -u root --protocol=TCP <<EOF 2>&1
CREATE EXTERNAL CATALOG IF NOT EXISTS like_smk PROPERTIES (
  "type"="iceberg", "iceberg.catalog.type"="hadoop", "iceberg.catalog.warehouse"="$HW",
  "aws.s3.access_key"="admin", "aws.s3.secret_key"="admin123",
  "aws.s3.endpoint"="http://127.0.0.1:9000", "aws.s3.enable_path_style_access"="true"
);
CREATE DATABASE like_smk.smk;
CREATE TABLE like_smk.smk.src (a INT, b STRING) TBLPROPERTIES ("format-version"="3");
CREATE TABLE like_smk.smk.dst LIKE like_smk.smk.src;
SHOW CREATE TABLE like_smk.smk.dst;
DROP DATABASE like_smk.smk FORCE;
DROP CATALOG like_smk;
EOF
```

- [ ] **Step 2: Capture verdict**

If `CREATE TABLE LIKE` succeeds and `SHOW CREATE TABLE dst` shows a schema mirroring `src` — Task 3 proceeds.

If rejected — Task 3 is deferred. Add a follow-up fix task: "NovaRocks Iceberg CREATE TABLE LIKE support". Capture the exact error.

---

## Task 2: Migrate `alter_table_comment` (ddl → iceberg-ddl)

Migrate the salvageable table-level COMMENT slice from `ddl_alter_table.sql`.

**Files:**
- Create: `sql-tests/iceberg-ddl/sql/alter_table_comment.sql`
- Create: `sql-tests/iceberg-ddl/result/alter_table_comment.result`

- [ ] **Step 1: Write `sql-tests/iceberg-ddl/sql/alter_table_comment.sql`**

```sql
-- @tags=iceberg_ddl
-- Test Objective:
-- 1. Validate ALTER TABLE t COMMENT 'x' updates the table-level comment on an Iceberg table.
-- 2. Verify SHOW CREATE TABLE reflects the updated comment.

-- query 1
-- @skip_result_check=true
DROP TABLE IF EXISTS ${case_db}.t;
CREATE TABLE ${case_db}.t (id INT, v INT) COMMENT 'c1';

-- query 2
-- @result_contains=COMMENT 'c1'
SHOW CREATE TABLE ${case_db}.t;

-- query 3
-- @skip_result_check=true
ALTER TABLE ${case_db}.t COMMENT 'c2';

-- query 4
-- @result_contains=COMMENT 'c2'
SHOW CREATE TABLE ${case_db}.t;
```

The `@result_contains` substring assumes NovaRocks's Iceberg SHOW CREATE TABLE emits `COMMENT 'c1'` (single-quoted). After recording, if the actual output uses double quotes or a different format, update the substring to match. Do NOT change the syntax — change the assertion.

- [ ] **Step 2: Record + spot-check + verify**

```bash
source docker/iceberg-rest/runtime/current/env.sh
cargo run --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests -- \
  --config "$NOVAROCKS_SQL_TEST_CONFIG" \
  --suite iceberg-ddl --only alter_table_comment \
  --mode record --record-from target
cat sql-tests/iceberg-ddl/result/alter_table_comment.result
# If the SHOW CREATE TABLE output reveals the actual COMMENT format, tighten the
# substring and re-record. Then run verify.
cargo run --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests -- \
  --config "$NOVAROCKS_SQL_TEST_CONFIG" \
  --suite iceberg-ddl --only alter_table_comment \
  --mode verify
```

If the case fails — either `ALTER TABLE t COMMENT 'x'` is rejected (NovaRocks doesn't support table-level comment alter on Iceberg) or `CREATE TABLE ... COMMENT 'x'` at table-level is rejected. In either case, STOP and report the exact failing statement. Add a follow-up fix task. **Do not** rewrite to use column-level COMMENT.

- [ ] **Step 3: Commit**

```bash
git add sql-tests/iceberg-ddl/sql/alter_table_comment.sql sql-tests/iceberg-ddl/result/alter_table_comment.result
git commit -m "test(iceberg-ddl): add ALTER TABLE table-level COMMENT case"
```

---

## Task 3: Migrate `create_table_like` (ddl → iceberg-ddl) — gated on Task 1

**Skip if Task 1 smoke failed.** Defer to a follow-up fix task; proceed to Task 4 without writing this case.

**Files:**
- Create: `sql-tests/iceberg-ddl/sql/create_table_like.sql`
- Create: `sql-tests/iceberg-ddl/result/create_table_like.result`

- [ ] **Step 1: Write `sql-tests/iceberg-ddl/sql/create_table_like.sql`**

```sql
-- @tags=iceberg_ddl
-- Test Objective:
-- 1. Validate CREATE TABLE ... LIKE copies the source schema on an Iceberg table.
-- 2. Verify INSERT into the LIKE'd table works and the row count is correct.

-- query 1
-- @skip_result_check=true
DROP TABLE IF EXISTS ${case_db}.src;
DROP TABLE IF EXISTS ${case_db}.dst;
CREATE TABLE ${case_db}.src (
  id INT,
  name STRING,
  v BIGINT
) COMMENT 'source-table';
CREATE TABLE ${case_db}.dst LIKE ${case_db}.src;

-- query 2
-- The dst table should mirror src's column list (and ideally the COMMENT).
-- @result_contains=id
-- @result_contains=name
-- @result_contains=v
SHOW CREATE TABLE ${case_db}.dst;

-- query 3
-- @skip_result_check=true
INSERT INTO ${case_db}.dst VALUES (1, 'alice', 100), (2, 'bob', 200);

-- query 4
SELECT count(1) AS n FROM ${case_db}.dst;
```

After recording, tighten the `@result_contains` substrings in query 2 to match the actual SHOW CREATE TABLE output (e.g., backticked column names like `` `id` `` if NovaRocks emits them that way).

- [ ] **Step 2: Record + verify + commit**

```bash
source docker/iceberg-rest/runtime/current/env.sh
cargo run --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests -- \
  --config "$NOVAROCKS_SQL_TEST_CONFIG" \
  --suite iceberg-ddl --only create_table_like \
  --mode record --record-from target
cat sql-tests/iceberg-ddl/result/create_table_like.result
# Tighten substrings if needed, re-record, verify.
cargo run --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests -- \
  --config "$NOVAROCKS_SQL_TEST_CONFIG" \
  --suite iceberg-ddl --only create_table_like \
  --mode verify
git add sql-tests/iceberg-ddl/sql/create_table_like.sql sql-tests/iceberg-ddl/result/create_table_like.result
git commit -m "test(iceberg-ddl): add CREATE TABLE LIKE case"
```

---

## Task 4: Delete `sql-tests/ddl/`

The other 4 ddl cases are StarRocks-only (SWAP, PK ORDER BY reorder, RANDOM distribution, sync rollup MV) per the spec §4.2 — drop the whole source directory.

- [ ] **Step 1: Confirm no external references**

```bash
grep -rn '"ddl"' tests/sql-test-runner/src/ docs/design/ 2>/dev/null | grep -v "iceberg-ddl\|2026-05-25-iceberg" | head -5
```

Expected: no matches except historical doc references in this PR's own spec/plan files.

- [ ] **Step 2: Remove the directory**

```bash
git rm -r sql-tests/ddl
```

- [ ] **Step 3: Confirm the runner no longer discovers it**

```bash
source docker/iceberg-rest/runtime/current/env.sh
cargo run --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests -- \
  --config "$NOVAROCKS_SQL_TEST_CONFIG" --suite ddl --mode verify 2>&1 | grep -E "unknown suite|ERROR" | head -1
```

Expected: `unknown suite 'ddl'`.

- [ ] **Step 4: Commit**

```bash
git commit -m "test: delete sql-tests/ddl/ suite; salvageable cases migrated to iceberg-ddl"
```

---

## Task 5: Move iceberg → iceberg-ddl, batch A — schema_evolution (9 cases)

The 9 `iceberg_schema_evolution_*.sql` cases all exercise Iceberg ALTER TABLE schema evolution (add/drop/rename/widen on STRUCT, ARRAY<STRUCT>, MAP, decimal, date→timestamp, nullability, reorder). They belong in iceberg-ddl.

**Files moved (each: .sql + .result if it exists):**

| Source (iceberg/sql/) | Destination (iceberg-ddl/sql/) | .result? |
|---|---|---|
| `iceberg_schema_evolution_array_map_widen.sql` | `schema_evolution_array_map_widen.sql` | yes |
| `iceberg_schema_evolution_date_to_timestamp_widen.sql` | `schema_evolution_date_to_timestamp_widen.sql` | yes |
| `iceberg_schema_evolution_decimal_widen.sql` | `schema_evolution_decimal_widen.sql` | yes |
| `iceberg_schema_evolution_local.sql` | `schema_evolution_local.sql` | yes |
| `iceberg_schema_evolution_nested.sql` | `schema_evolution_nested.sql` | yes |
| `iceberg_schema_evolution_nullability.sql` | `schema_evolution_nullability.sql` | yes |
| `iceberg_schema_evolution_reorder.sql` | `schema_evolution_reorder.sql` | yes |
| `iceberg_schema_evolution_s3.sql` | `schema_evolution_s3.sql` | yes |
| `iceberg_schema_evolution_widen_reject.sql` | `schema_evolution_widen_reject.sql` | NO (negative-only with `@expect_error`) |

- [ ] **Step 1: Move all 9 SQL files**

```bash
cd /Users/harbor/.claude/worktrees/NovaRocks/beautiful-cerf-02884b
for n in array_map_widen date_to_timestamp_widen decimal_widen local nested nullability reorder s3 widen_reject; do
  src="sql-tests/iceberg/sql/iceberg_schema_evolution_${n}.sql"
  dst="sql-tests/iceberg-ddl/sql/schema_evolution_${n}.sql"
  git mv "$src" "$dst"
done
```

- [ ] **Step 2: Move 8 result files (widen_reject has no .result)**

```bash
for n in array_map_widen date_to_timestamp_widen decimal_widen local nested nullability reorder s3; do
  src="sql-tests/iceberg/result/iceberg_schema_evolution_${n}.result"
  dst="sql-tests/iceberg-ddl/result/schema_evolution_${n}.result"
  git mv "$src" "$dst"
done
```

- [ ] **Step 3: Catalog rename in moved .sql files**

```bash
for n in array_map_widen date_to_timestamp_widen decimal_widen local nested nullability reorder s3 widen_reject; do
  sed -i '' 's/iceberg_cat_/iceberg_ddl_cat_/g' "sql-tests/iceberg-ddl/sql/schema_evolution_${n}.sql"
done
```

Verify the substitution landed (no remaining `iceberg_cat_` in the new files):

```bash
grep -l 'iceberg_cat_' sql-tests/iceberg-ddl/sql/schema_evolution_*.sql && echo "FAIL: stale catalog name" || echo "OK"
```

- [ ] **Step 4: Verify the 9 cases against the iceberg-ddl suite**

```bash
source docker/iceberg-rest/runtime/current/env.sh
cargo run --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests -- \
  --config "$NOVAROCKS_SQL_TEST_CONFIG" \
  --suite iceberg-ddl \
  --only schema_evolution_array_map_widen,schema_evolution_date_to_timestamp_widen,schema_evolution_decimal_widen,schema_evolution_local,schema_evolution_nested,schema_evolution_nullability,schema_evolution_reorder,schema_evolution_s3,schema_evolution_widen_reject \
  --mode verify 2>&1 | grep -E "total=|pass=|fail=" | tail -3
```

Expected: total=9, pass=9, fail=0. If any case fails, capture the exact error — usually means the catalog rename missed a spot, or the case references something specific to the iceberg suite's init.sql. STOP and report; do NOT re-record to mask the failure.

- [ ] **Step 5: Commit**

```bash
git add -A
git commit -m "test(iceberg-ddl): move 9 schema_evolution cases from iceberg suite"
```

---

## Task 6: Move iceberg → iceberg-ddl, batch B — v3_default (10 cases)

The 10 `iceberg_v3_default_*.sql` cases exercise Iceberg v3 column DEFAULT semantics (initial-default, write-default, ALTER ADD COLUMN DEFAULT, type rejections). They belong in iceberg-ddl.

**Files moved:**

| Source (iceberg/sql/) | Destination (iceberg-ddl/sql/) | .result? |
|---|---|---|
| `iceberg_v3_default_add_column_existing_data.sql` | `v3_default_add_column_existing_data.sql` | yes |
| `iceberg_v3_default_alter_v2_rejected.sql` | `v3_default_alter_v2_rejected.sql` | NO (negative-only) |
| `iceberg_v3_default_complex_type_rejected.sql` | `v3_default_complex_type_rejected.sql` | NO (negative-only) |
| `iceberg_v3_default_create_table.sql` | `v3_default_create_table.sql` | yes |
| `iceberg_v3_default_decimal_scale_mismatch.sql` | `v3_default_decimal_scale_mismatch.sql` | NO (negative-only) |
| `iceberg_v3_default_insert_select.sql` | `v3_default_insert_select.sql` | yes |
| `iceberg_v3_default_null_on_v2.sql` | `v3_default_null_on_v2.sql` | yes |
| `iceberg_v3_default_positional_count_mismatch.sql` | `v3_default_positional_count_mismatch.sql` | NO (negative-only) |
| `iceberg_v3_default_primitive_types.sql` | `v3_default_primitive_types.sql` | yes |
| `iceberg_v3_default_v2_rejected.sql` | `v3_default_v2_rejected.sql` | NO (negative-only) |

- [ ] **Step 1: Move SQL files**

```bash
for n in add_column_existing_data alter_v2_rejected complex_type_rejected create_table decimal_scale_mismatch insert_select null_on_v2 positional_count_mismatch primitive_types v2_rejected; do
  src="sql-tests/iceberg/sql/iceberg_v3_default_${n}.sql"
  dst="sql-tests/iceberg-ddl/sql/v3_default_${n}.sql"
  git mv "$src" "$dst"
done
```

- [ ] **Step 2: Move result files (5 cases have .result, 5 don't)**

```bash
for n in add_column_existing_data create_table insert_select null_on_v2 primitive_types; do
  src="sql-tests/iceberg/result/iceberg_v3_default_${n}.result"
  dst="sql-tests/iceberg-ddl/result/v3_default_${n}.result"
  git mv "$src" "$dst"
done
```

- [ ] **Step 3: Catalog rename**

```bash
for n in add_column_existing_data alter_v2_rejected complex_type_rejected create_table decimal_scale_mismatch insert_select null_on_v2 positional_count_mismatch primitive_types v2_rejected; do
  sed -i '' 's/iceberg_cat_/iceberg_ddl_cat_/g' "sql-tests/iceberg-ddl/sql/v3_default_${n}.sql"
done
grep -l 'iceberg_cat_' sql-tests/iceberg-ddl/sql/v3_default_*.sql && echo "FAIL" || echo "OK"
```

- [ ] **Step 4: Verify**

```bash
source docker/iceberg-rest/runtime/current/env.sh
cargo run --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests -- \
  --config "$NOVAROCKS_SQL_TEST_CONFIG" \
  --suite iceberg-ddl \
  --only v3_default_add_column_existing_data,v3_default_alter_v2_rejected,v3_default_complex_type_rejected,v3_default_create_table,v3_default_decimal_scale_mismatch,v3_default_insert_select,v3_default_null_on_v2,v3_default_positional_count_mismatch,v3_default_primitive_types,v3_default_v2_rejected \
  --mode verify 2>&1 | grep -E "total=|pass=|fail=" | tail -3
```

Expected: total=10, pass=10, fail=0.

- [ ] **Step 5: Commit**

```bash
git add -A
git commit -m "test(iceberg-ddl): move 10 v3_default cases from iceberg suite"
```

---

## Task 7: Move iceberg → iceberg-ddl, batch C — remaining DDL (11 cases)

The remaining iceberg-ddl-bound cases: 4 table_properties + 2 catalog/type + 1 truncate + 3 partition_evolution + 1 pkfk_property.

**Files moved:**

| Source (iceberg/sql/) | Destination (iceberg-ddl/sql/) | .result? |
|---|---|---|
| `iceberg_table_properties_combined_reject.sql` | `table_properties_combined_reject.sql` | NO (negative-only) |
| `iceberg_table_properties_reject_reserved.sql` | `table_properties_reject_reserved.sql` | NO (negative-only) |
| `iceberg_table_properties_set_unset.sql` | `table_properties_set_unset.sql` | yes |
| `iceberg_table_properties_unset_if_exists.sql` | `table_properties_unset_if_exists.sql` | yes |
| `iceberg_catalog_complex_type.sql` | `catalog_complex_type.sql` | yes |
| `iceberg_catalog_time_type.sql` | `catalog_time_type.sql` | yes |
| `iceberg_truncate.sql` | `truncate.sql` | yes |
| `iceberg_partition_evolution_1.sql` | `partition_evolution_basic.sql` | yes |
| `iceberg_partition_evolution_replace.sql` | `partition_evolution_replace.sql` | yes |
| `iceberg_partition_evolution_unsupported.sql` | `partition_evolution_unsupported.sql` | NO (negative-only) |
| `pkfk_property.sql` | `pkfk_property.sql` | NO (negative-only) |

- [ ] **Step 1: Move SQL files**

```bash
# table_properties × 4
for n in combined_reject reject_reserved set_unset unset_if_exists; do
  git mv "sql-tests/iceberg/sql/iceberg_table_properties_${n}.sql" "sql-tests/iceberg-ddl/sql/table_properties_${n}.sql"
done

# catalog/type × 2
git mv sql-tests/iceberg/sql/iceberg_catalog_complex_type.sql sql-tests/iceberg-ddl/sql/catalog_complex_type.sql
git mv sql-tests/iceberg/sql/iceberg_catalog_time_type.sql sql-tests/iceberg-ddl/sql/catalog_time_type.sql

# truncate
git mv sql-tests/iceberg/sql/iceberg_truncate.sql sql-tests/iceberg-ddl/sql/truncate.sql

# partition_evolution × 3 (basic / replace / unsupported)
git mv sql-tests/iceberg/sql/iceberg_partition_evolution_1.sql sql-tests/iceberg-ddl/sql/partition_evolution_basic.sql
git mv sql-tests/iceberg/sql/iceberg_partition_evolution_replace.sql sql-tests/iceberg-ddl/sql/partition_evolution_replace.sql
git mv sql-tests/iceberg/sql/iceberg_partition_evolution_unsupported.sql sql-tests/iceberg-ddl/sql/partition_evolution_unsupported.sql

# pkfk
git mv sql-tests/iceberg/sql/pkfk_property.sql sql-tests/iceberg-ddl/sql/pkfk_property.sql
```

- [ ] **Step 2: Move result files (6 have .result, 5 don't)**

```bash
# table_properties: set_unset + unset_if_exists have .result
git mv sql-tests/iceberg/result/iceberg_table_properties_set_unset.result sql-tests/iceberg-ddl/result/table_properties_set_unset.result
git mv sql-tests/iceberg/result/iceberg_table_properties_unset_if_exists.result sql-tests/iceberg-ddl/result/table_properties_unset_if_exists.result

# catalog/type × 2
git mv sql-tests/iceberg/result/iceberg_catalog_complex_type.result sql-tests/iceberg-ddl/result/catalog_complex_type.result
git mv sql-tests/iceberg/result/iceberg_catalog_time_type.result sql-tests/iceberg-ddl/result/catalog_time_type.result

# truncate
git mv sql-tests/iceberg/result/iceberg_truncate.result sql-tests/iceberg-ddl/result/truncate.result

# partition_evolution basic + replace (unsupported has no .result)
git mv sql-tests/iceberg/result/iceberg_partition_evolution_1.result sql-tests/iceberg-ddl/result/partition_evolution_basic.result
git mv sql-tests/iceberg/result/iceberg_partition_evolution_replace.result sql-tests/iceberg-ddl/result/partition_evolution_replace.result
```

- [ ] **Step 3: Catalog rename**

```bash
for f in \
  sql-tests/iceberg-ddl/sql/table_properties_*.sql \
  sql-tests/iceberg-ddl/sql/catalog_complex_type.sql \
  sql-tests/iceberg-ddl/sql/catalog_time_type.sql \
  sql-tests/iceberg-ddl/sql/truncate.sql \
  sql-tests/iceberg-ddl/sql/partition_evolution_basic.sql \
  sql-tests/iceberg-ddl/sql/partition_evolution_replace.sql \
  sql-tests/iceberg-ddl/sql/partition_evolution_unsupported.sql \
  sql-tests/iceberg-ddl/sql/pkfk_property.sql; do
  sed -i '' 's/iceberg_cat_/iceberg_ddl_cat_/g' "$f"
done
grep -l 'iceberg_cat_[^d]' sql-tests/iceberg-ddl/sql/*.sql && echo "FAIL: stale" || echo "OK"
```

Note: the grep above checks for `iceberg_cat_` NOT followed by `d` to avoid matching the legitimate `iceberg_ddl_cat_` substring.

- [ ] **Step 4: Verify**

```bash
source docker/iceberg-rest/runtime/current/env.sh
cargo run --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests -- \
  --config "$NOVAROCKS_SQL_TEST_CONFIG" \
  --suite iceberg-ddl \
  --only table_properties_combined_reject,table_properties_reject_reserved,table_properties_set_unset,table_properties_unset_if_exists,catalog_complex_type,catalog_time_type,truncate,partition_evolution_basic,partition_evolution_replace,partition_evolution_unsupported,pkfk_property \
  --mode verify 2>&1 | grep -E "total=|pass=|fail=" | tail -3
```

Expected: total=11, pass=11, fail=0.

- [ ] **Step 5: Commit**

```bash
git add -A
git commit -m "test(iceberg-ddl): move 11 cases (table_properties, catalog types, truncate, partition_evolution, pkfk) from iceberg suite"
```

---

## Task 8: Move iceberg → iceberg-dml, batch D — v3 DML core (6 cases)

The 6 v3 DML core cases: MERGE × 2, UPDATE × 2, INSERT OVERWRITE × 1, CTAS × 1.

**Files moved (all 6 have .result):**

| Source | Destination |
|---|---|
| `iceberg_v3_merge_cow.sql` | `merge_into_cow.sql` |
| `iceberg_v3_merge_mor.sql` | `merge_into_mor.sql` |
| `iceberg_v3_update_cow.sql` | `update_cow.sql` |
| `iceberg_v3_update_mor.sql` | `update_mor.sql` |
| `iceberg_v3_overwrite_partitions.sql` | `overwrite_partitions.sql` |
| `iceberg_v3_ctas.sql` | `ctas.sql` |

- [ ] **Step 1: Move SQL + result**

```bash
declare -A moves=(
  [iceberg_v3_merge_cow]=merge_into_cow
  [iceberg_v3_merge_mor]=merge_into_mor
  [iceberg_v3_update_cow]=update_cow
  [iceberg_v3_update_mor]=update_mor
  [iceberg_v3_overwrite_partitions]=overwrite_partitions
  [iceberg_v3_ctas]=ctas
)
for src in "${!moves[@]}"; do
  dst="${moves[$src]}"
  git mv "sql-tests/iceberg/sql/${src}.sql" "sql-tests/iceberg-dml/sql/${dst}.sql"
  git mv "sql-tests/iceberg/result/${src}.result" "sql-tests/iceberg-dml/result/${dst}.result"
done
```

- [ ] **Step 2: Catalog rename (iceberg_cat_ → iceberg_dml_cat_)**

```bash
for f in sql-tests/iceberg-dml/sql/merge_into_cow.sql \
         sql-tests/iceberg-dml/sql/merge_into_mor.sql \
         sql-tests/iceberg-dml/sql/update_cow.sql \
         sql-tests/iceberg-dml/sql/update_mor.sql \
         sql-tests/iceberg-dml/sql/overwrite_partitions.sql \
         sql-tests/iceberg-dml/sql/ctas.sql; do
  sed -i '' 's/iceberg_cat_/iceberg_dml_cat_/g' "$f"
done
grep -l 'iceberg_cat_[^d]' sql-tests/iceberg-dml/sql/*.sql && echo "FAIL" || echo "OK"
```

- [ ] **Step 3: Verify**

```bash
source docker/iceberg-rest/runtime/current/env.sh
cargo run --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests -- \
  --config "$NOVAROCKS_SQL_TEST_CONFIG" \
  --suite iceberg-dml \
  --only merge_into_cow,merge_into_mor,update_cow,update_mor,overwrite_partitions,ctas \
  --mode verify 2>&1 | grep -E "total=|pass=|fail=" | tail -3
```

Expected: total=6, pass=6, fail=0. If any case collides with an existing iceberg-dml case name (e.g., `merge_into_upsert_delete.sql` exists in iceberg-dml from PR #166), the move should still produce a distinct filename — `merge_into_cow.sql` and `merge_into_mor.sql` are new names.

- [ ] **Step 4: Commit**

```bash
git add -A
git commit -m "test(iceberg-dml): move 6 v3 DML core cases (MERGE/UPDATE/OVERWRITE/CTAS) from iceberg suite"
```

---

## Task 9: Move iceberg → iceberg-dml, batch E — remaining DML (7 cases)

variant × 2 + partition_evolution_delete × 2 + equality_delete × 1 + none_write_mode × 1 + branch_write × 1.

**Files moved:**

| Source | Destination | .result? |
|---|---|---|
| `iceberg_v3_variant_insert.sql` | `variant_insert.sql` | yes |
| `iceberg_v3_variant_unsupported.sql` | `variant_unsupported.sql` | NO (negative-only) |
| `iceberg_partition_evolution_delete.sql` | `partition_evolution_delete.sql` | yes |
| `iceberg_partition_evolution_v3_delete.sql` | `partition_evolution_v3_delete.sql` | yes |
| `iceberg_equality_delete_schema_evolution.sql` | `equality_delete_schema_evolution.sql` | yes |
| `iceberg_none_write_mode.sql` | `none_write_mode.sql` | yes |
| `iceberg_branch_write.sql` | `branch_write.sql` | yes |

- [ ] **Step 1: Move SQL + matching result files**

```bash
# All 7 SQL moves
git mv sql-tests/iceberg/sql/iceberg_v3_variant_insert.sql sql-tests/iceberg-dml/sql/variant_insert.sql
git mv sql-tests/iceberg/sql/iceberg_v3_variant_unsupported.sql sql-tests/iceberg-dml/sql/variant_unsupported.sql
git mv sql-tests/iceberg/sql/iceberg_partition_evolution_delete.sql sql-tests/iceberg-dml/sql/partition_evolution_delete.sql
git mv sql-tests/iceberg/sql/iceberg_partition_evolution_v3_delete.sql sql-tests/iceberg-dml/sql/partition_evolution_v3_delete.sql
git mv sql-tests/iceberg/sql/iceberg_equality_delete_schema_evolution.sql sql-tests/iceberg-dml/sql/equality_delete_schema_evolution.sql
git mv sql-tests/iceberg/sql/iceberg_none_write_mode.sql sql-tests/iceberg-dml/sql/none_write_mode.sql
git mv sql-tests/iceberg/sql/iceberg_branch_write.sql sql-tests/iceberg-dml/sql/branch_write.sql

# 6 result moves (variant_unsupported has none)
git mv sql-tests/iceberg/result/iceberg_v3_variant_insert.result sql-tests/iceberg-dml/result/variant_insert.result
git mv sql-tests/iceberg/result/iceberg_partition_evolution_delete.result sql-tests/iceberg-dml/result/partition_evolution_delete.result
git mv sql-tests/iceberg/result/iceberg_partition_evolution_v3_delete.result sql-tests/iceberg-dml/result/partition_evolution_v3_delete.result
git mv sql-tests/iceberg/result/iceberg_equality_delete_schema_evolution.result sql-tests/iceberg-dml/result/equality_delete_schema_evolution.result
git mv sql-tests/iceberg/result/iceberg_none_write_mode.result sql-tests/iceberg-dml/result/none_write_mode.result
git mv sql-tests/iceberg/result/iceberg_branch_write.result sql-tests/iceberg-dml/result/branch_write.result
```

- [ ] **Step 2: Catalog rename**

```bash
for f in sql-tests/iceberg-dml/sql/variant_insert.sql \
         sql-tests/iceberg-dml/sql/variant_unsupported.sql \
         sql-tests/iceberg-dml/sql/partition_evolution_delete.sql \
         sql-tests/iceberg-dml/sql/partition_evolution_v3_delete.sql \
         sql-tests/iceberg-dml/sql/equality_delete_schema_evolution.sql \
         sql-tests/iceberg-dml/sql/none_write_mode.sql \
         sql-tests/iceberg-dml/sql/branch_write.sql; do
  sed -i '' 's/iceberg_cat_/iceberg_dml_cat_/g' "$f"
done
grep -l 'iceberg_cat_[^d]' sql-tests/iceberg-dml/sql/*.sql && echo "FAIL" || echo "OK"
```

- [ ] **Step 3: Verify**

```bash
source docker/iceberg-rest/runtime/current/env.sh
cargo run --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests -- \
  --config "$NOVAROCKS_SQL_TEST_CONFIG" \
  --suite iceberg-dml \
  --only variant_insert,variant_unsupported,partition_evolution_delete,partition_evolution_v3_delete,equality_delete_schema_evolution,none_write_mode,branch_write \
  --mode verify 2>&1 | grep -E "total=|pass=|fail=" | tail -3
```

Expected: total=7, pass=7, fail=0.

- [ ] **Step 4: Commit**

```bash
git add -A
git commit -m "test(iceberg-dml): move 7 cases (variant, partition DELETE, equality DELETE, write mode, branch write) from iceberg suite"
```

---

## Task 10: Differentiate iceberg-ddl `default_*` headers

Per spec §5.5, the 9 existing `iceberg-ddl/default_*.sql` cases (PR #172 origin) and the newly-moved 10 `iceberg-ddl/v3_default_*.sql` cases (Task 6) overlap in coverage. Keep both, but **update each PR #172 case's `Test Objective` header** to call out what it covers that `v3_default_primitive_types.sql` does not.

**Files:** modify each of the 9 cases in `sql-tests/iceberg-ddl/sql/default_*.sql`.

- [ ] **Step 1: Update `default_boolean.sql`**

Replace the existing `-- Test Objective:` block with:

```sql
-- @order_sensitive=true
-- @tags=iceberg_ddl,default
-- Test Objective:
-- 1. Validate BOOLEAN initial-default + write-default in isolation (one column, two
--    explicit INSERT patterns: full-list and subset-list).
-- 2. Complementary to v3_default_primitive_types.sql which exercises BOOLEAN as one
--    of 11 types in a single combined case.
```

(Body unchanged.)

- [ ] **Step 2: Update `default_numeric.sql`**

```sql
-- @order_sensitive=true
-- @tags=iceberg_ddl,default,numeric
-- Test Objective:
-- 1. Validate ALTER ADD COLUMN ... DEFAULT for numeric types at boundary values
--    (negative, zero, very large), which v3_default_primitive_types.sql does not.
-- 2. Cover SMALLINT/INT/BIGINT/FLOAT/DOUBLE with explicit large/negative/zero
--    defaults distinct from the small literals in v3_default_primitive_types.
```

- [ ] **Step 3: Update `default_decimal.sql`**

```sql
-- @order_sensitive=true
-- @tags=iceberg_ddl,default,decimal
-- Test Objective:
-- 1. Validate ALTER ADD COLUMN ... DEFAULT for DECIMAL across multiple
--    precision/scale combos including DECIMAL(20, 6) (exceeds the single
--    DECIMAL(10, 2) example in v3_default_primitive_types.sql).
```

- [ ] **Step 4: Update `default_string.sql`**

```sql
-- @order_sensitive=true
-- @tags=iceberg_ddl,default,string
-- Test Objective:
-- 1. Validate STRING DEFAULT with empty-string, unicode, comma, and newline-escape
--    literals. v3_default_primitive_types.sql covers only the trivial 'hi' literal.
```

- [ ] **Step 5: Update `default_date.sql`**

```sql
-- @order_sensitive=true
-- @tags=iceberg_ddl,default,date
-- Test Objective:
-- 1. Validate DATE and DATETIME DEFAULT with mid-2024 calendar dates.
--    v3_default_primitive_types.sql uses the epoch + epoch-plus-1, which is
--    a different reference point.
```

- [ ] **Step 6: Update `default_varbinary.sql`**

```sql
-- @order_sensitive=true
-- @tags=iceberg_ddl,default,varbinary
-- Test Objective:
-- 1. Validate VARBINARY (Iceberg Binary) DEFAULT — a type NOT covered by
--    v3_default_primitive_types.sql.
```

- [ ] **Step 7: Update `default_json.sql`**

```sql
-- @order_sensitive=true
-- @tags=iceberg_ddl,default,json
-- Test Objective:
-- 1. Validate JSON DEFAULT — a type NOT covered by v3_default_primitive_types.sql.
-- 2. Positive companion to v3_default_complex_type_rejected.sql (negative).
```

- [ ] **Step 8: Update `default_json_strict_validation.sql`**

```sql
-- @tags=iceberg_ddl,default,json,validation
-- Test Objective:
-- 1. Validate JSON DEFAULT with an invalid JSON literal is rejected at ALTER time.
-- 2. Complement to default_json.sql (positive case for JSON DEFAULT).
```

- [ ] **Step 9: Update `default_complex.sql`**

```sql
-- @order_sensitive=true
-- @tags=iceberg_ddl,default,complex
-- Test Objective:
-- 1. Validate ARRAY<INT> and MAP<STRING, INT> DEFAULT with empty-collection literals
--    ('[]' / '{}'). Positive counterpart to v3_default_complex_type_rejected.sql
--    (which probes the non-empty / unsupported forms).
```

- [ ] **Step 10: Re-verify all 9 cases still pass after header updates**

```bash
source docker/iceberg-rest/runtime/current/env.sh
cargo run --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests -- \
  --config "$NOVAROCKS_SQL_TEST_CONFIG" \
  --suite iceberg-ddl \
  --only default_boolean,default_numeric,default_decimal,default_string,default_date,default_varbinary,default_json,default_json_strict_validation,default_complex \
  --mode verify 2>&1 | grep -E "total=|pass=|fail=" | tail -3
```

Expected: total=9, pass=9, fail=0. Headers are comments — they don't change runtime behavior.

- [ ] **Step 11: Commit**

```bash
git add -A
git commit -m "test(iceberg-ddl): differentiate default_* case headers from v3_default_*"
```

---

## Task 11: Whole-suite verification

After all moves, run each affected suite end-to-end.

- [ ] **Step 1: Verify iceberg suite (shrunken — 24 cases left)**

```bash
source docker/iceberg-rest/runtime/current/env.sh
cargo run --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests -- \
  --config "$NOVAROCKS_SQL_TEST_CONFIG" \
  --suite iceberg --mode verify 2>&1 | grep -E "total=|pass=|fail=" | tail -3
```

Expected: total=24, pass=24, fail=0.

- [ ] **Step 2: Verify iceberg-ddl suite (46 cases: 15 baseline + 30 from iceberg + 1 alter_table_comment from ddl + maybe 1 create_table_like)**

```bash
cargo run --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests -- \
  --config "$NOVAROCKS_SQL_TEST_CONFIG" \
  --suite iceberg-ddl --mode verify 2>&1 | grep -E "total=|pass=|fail=" | tail -3
```

Expected: total=46 (or 45 if `create_table_like` was deferred), pass=total, fail=0.

- [ ] **Step 3: Verify iceberg-dml suite (37 cases: 24 baseline + 13 from iceberg)**

```bash
cargo run --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests -- \
  --config "$NOVAROCKS_SQL_TEST_CONFIG" \
  --suite iceberg-dml --mode verify 2>&1 | grep -E "total=|pass=|fail=" | tail -3
```

Expected: total=37, pass=37, fail=0.

- [ ] **Step 4: Confirm ddl suite is gone**

```bash
cargo run --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests -- \
  --config "$NOVAROCKS_SQL_TEST_CONFIG" \
  --suite ddl --mode verify 2>&1 | grep -E "unknown suite|ERROR" | head -1
```

Expected: `unknown suite 'ddl'`.

If any of the three live suites has failures — stop, capture, surface. Don't paper over.

---

## Task 12: Final hygiene

- [ ] **Step 1: Format**

```bash
cargo fmt
```

- [ ] **Step 2: Final whole-suite verify (sanity)**

```bash
source docker/iceberg-rest/runtime/current/env.sh
for s in iceberg iceberg-ddl iceberg-dml; do
  echo "=== $s ==="
  cargo run --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests -- \
    --config "$NOVAROCKS_SQL_TEST_CONFIG" --suite "$s" --mode verify 2>&1 | grep -E "total=|pass=|fail=" | tail -3
done
```

All three must be all-green.

- [ ] **Step 3: Stop the standalone-server**

```bash
kill "$(cat /tmp/novarocks-icecon.pid)" 2>/dev/null || true
rm -f /tmp/novarocks-icecon.pid
```

- [ ] **Step 4: Commit any fmt fixups**

```bash
git status
# If fmt made changes:
git add -A
git commit -m "chore: cargo fmt after iceberg suite consolidation"
```

- [ ] **Step 5: Print final summary**

```bash
git log --oneline main..HEAD
```

Expected: a clean stack of focused commits — smoke (no commit), 2 ddl→ddl migrations + ddl-delete, 3 iceberg→iceberg-ddl moves, 2 iceberg→iceberg-dml moves, 1 default header differentiation, optional fmt.

---

## Appendix: One-line summary of what the engineer must remember

1. **No silent fallback.** If a moved case fails or a missing feature surfaces, stop and report. Don't re-record results to mask failures, don't rewrite SQL to bypass missing features.
2. **`git mv` then sed.** Each move is `git mv .sql + .result` then `sed -i '' 's/iceberg_cat_/iceberg_<dst>_cat_/g'` on the destination .sql file.
3. **No `.result` for negative-only cases.** A `.sql` with only `@expect_error` annotations doesn't produce a `.result` file — skip the .result mv for those.
4. **Re-verify after each batch.** Don't accumulate moves without verifying each batch; a missed catalog rename or schema-bound assumption fails fast.
5. **Headers in PR #172 default_* cases need differentiation** (Task 10) — purely comment-level, no behavior change.
6. **The standalone-server stays up across all tasks**; only restart if it crashes.
