# schema-be + schema-change + schema-info → iceberg-ddl Suite Migration Design

Status: Draft
Date: 2026-05-22
Author: harbor.liu

## 1. Background

NovaRocks's standalone SQL engine moved Iceberg-as-backend to first-class
status (see PR #166 / `iceberg-dml` migration).  Three legacy SQL test
suites carry residual StarRocks-FE-era DDL coverage:

- `sql-tests/schema-be/` (4 cases) — query `default_catalog.information_schema.be_*`
  observability virtual tables (BE compactions, BE metrics, tablet write log,
  transactions).  Not DDL semantically.
- `sql-tests/schema-info/` (2 cases) — both StarRocks-only:
  `auto_increment_create_table` (AUTO_INCREMENT column) and `loads_count`
  (stream load history).
- `sql-tests/schema-change/` (32 cases) — mix of:
  - Iceberg-mappable ALTER TABLE coverage (ADD/DROP COLUMN, STRUCT/ARRAY
    field evolution, COMMENT) — ~6 cases.
  - Per-type DEFAULT value coverage tightly coupled to StarRocks's
    `fast_schema_evolution`, table models (DUP/UNIQUE/PRIMARY/AGG KEY),
    `partial_update_mode` — 10 cases.
  - StarRocks-only abnormal-path tests for the various table models — 6 cases.
  - StarRocks-only features (storage_cooldown_ttl, ROLLUP MV, char padding,
    partition-by-list distribution columns, etc.) — 8 cases.
  - StarRocks-flavored MV alter — 2 cases.

This design migrates the surviving Iceberg-relevant DDL test points into a
new `sql-tests/iceberg-ddl/` suite and deletes the three source suites
outright.  Per [memory/feedback_no_backwards_compat_for_novarocks.md], no
parallel run or compat shim — the three suites are removed in the same
change set after the new suite is verified.

## 2. Goals

1. Create a focused `iceberg-ddl` SQL test suite that exercises Iceberg
   ALTER TABLE semantics on the standalone engine via the hadoop+MinIO
   fixture used by the other `iceberg-*` suites.
2. Preserve every DDL test point from the three source suites that survives
   the Iceberg-as-backend model.
3. Eliminate every test point that only makes sense under a StarRocks OLAP
   table model or StarRocks-only feature (AUTO_INCREMENT, BITMAP/HLL,
   storage_cooldown_ttl, fast_schema_evolution, ROLLUP, char padding, etc.).
4. Apply the same **no-fallback rule** as PR #166: if a migration step hits
   a NovaRocks gap (e.g., Iceberg DEFAULT values not implemented), stop and
   surface the gap; fix it in this PR rather than silently rewriting the
   case to bypass the feature.
5. Leave the runner architecture untouched except for adding the suite name
   to the placeholder-defaults map.

## 3. Non-Goals

- Adding test coverage beyond what the three source suites carry today.
- Migrating cases to REST catalog or managed-lake catalog.
- Migrating the StarRocks-flavored MV alter cases (alter_mv_basic,
  alter_mv_schema_change) — those are ROLLUP / async MV semantics that
  belong elsewhere if anywhere; for this PR they are dropped.
- Replacing the schema-be observability coverage.  If we later want
  `information_schema.be_*` virtual-table tests on the standalone engine,
  they go into a separate `engine-info` suite, not `iceberg-ddl`.

## 4. Suite Layout

```
sql-tests/iceberg-ddl/
  init.sql       # creates external catalog iceberg_ddl_cat_${suite_uuid0}
  cleanup.sql    # drops the catalog
  sql/           # 6 core cases + 0..10 default cases (smoke-gated)
  result/        # generated via --mode record
```

### 4.1 `init.sql`

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

Identical to `sql-tests/iceberg-dml/init.sql` except for the catalog name.

### 4.2 `cleanup.sql`

```sql
DROP CATALOG IF EXISTS `iceberg_ddl_cat_${suite_uuid0}`;
```

### 4.3 Runner wiring

`tests/sql-test-runner/src/config.rs::apply_suite_placeholder_defaults` already
gives hadoop-catalog defaults to `"iceberg" | "iceberg-dml" | "iceberg-ivm" |
"mv-on-iceberg"`.  Add `"iceberg-ddl"` to that match arm.  No other runner
changes are required — suite discovery is already directory-driven.

## 5. Case Inventory

### 5.1 Migrate (6 core cases)

Files keep their original test intent but DDL is rewritten to Iceberg.
StarRocks-only clauses are stripped per the rewrite rules in §6.

| New filename | Source (schema-change) | Iceberg ALTER under test |
|---|---|---|
| `add_column_count_star.sql` | same | `ALTER TABLE ADD COLUMN` then verify `COUNT(*)` is stable and NULL backfill is visible |
| `alter_column_comment.sql` | same | `ALTER TABLE ... ALTER COLUMN ... COMMENT 'x'` round-trip via SHOW CREATE TABLE |
| `add_drop_field_struct.sql` | same | `ALTER TABLE ... MODIFY COLUMN c1 ADD FIELD ...` / `DROP FIELD ...` on STRUCT (positive + negative paths) |
| `add_drop_field_array.sql` | same | Same on `ARRAY<STRUCT<...>>` using the `[*]` element-target syntax |
| `drop_add_same_name_field.sql` | same | Drop a field then re-add with a different type; old rows backfill to NULL |
| `drop_last_field.sql` | same | Negative: dropping the last remaining field of a STRUCT is rejected |

### 5.2 Default-value cases (10) — smoke-gated

The 10 `test_*_default` cases each bundle three orthogonal concerns:
1. Per-type DEFAULT value semantics (the actual test point).
2. StarRocks fast_schema_evolution behavior (irrelevant on Iceberg).
3. StarRocks table-model interactions: DUPLICATE/UNIQUE/PRIMARY/AGG KEY plus
   `partial_update_mode` (irrelevant on Iceberg).

To migrate, we extract concern (1) and discard (2)+(3).  The migration is
viable only if NovaRocks supports `ALTER TABLE ADD COLUMN <type> DEFAULT
<value>` on an Iceberg table, and `INSERT INTO t (subset_cols) VALUES (...)`
correctly fills DEFAULT-bearing columns, and `UPDATE t SET c = DEFAULT
WHERE ...` works.

**Smoke gate (run before writing any default case):**

```sql
CREATE TABLE iceberg_ddl_smoke.smoke_db.t (id INT, v INT) TBLPROPERTIES ("format-version"="3");
INSERT INTO iceberg_ddl_smoke.smoke_db.t VALUES (1, 10);
ALTER TABLE iceberg_ddl_smoke.smoke_db.t ADD COLUMN c INT DEFAULT 42;
SELECT id, v, c FROM iceberg_ddl_smoke.smoke_db.t;  -- expect (1, 10, 42) per Iceberg initial-default
INSERT INTO iceberg_ddl_smoke.smoke_db.t (id, v) VALUES (2, 20);
SELECT id, v, c FROM iceberg_ddl_smoke.smoke_db.t ORDER BY id;  -- expect (1,10,42),(2,20,42) per write-default
```

Outcome decides migration:

- **All three pass** → migrate all 10 default cases (rewritten per §6 to
  drop SR-only noise, keep only the per-type DEFAULT semantic).
- **ADD COLUMN ... DEFAULT works but INSERT subset doesn't** → migrate the
  cases' first-half (ADD COLUMN coverage only), surface the INSERT-subset
  gap as a follow-up fix task.
- **ADD COLUMN ... DEFAULT rejected outright** → defer all 10 default
  cases; file a fix task for NovaRocks Iceberg DEFAULT support; the
  iceberg-ddl PR lands without them.

Cases (each will be rewritten to focus on its per-type DEFAULT semantic):

| New filename | Type focus |
|---|---|
| `default_boolean.sql` | BOOLEAN |
| `default_numeric.sql` | INT / BIGINT / FLOAT / DOUBLE / SMALLINT (TINYINT widened — Iceberg has no TINYINT) |
| `default_decimal.sql` | DECIMAL(p, s) |
| `default_string.sql` | STRING / VARCHAR (CHAR widened to STRING) |
| `default_date.sql` | DATE / DATETIME |
| `default_varbinary.sql` | VARBINARY |
| `default_json.sql` | JSON |
| `default_json_strict_validation.sql` | JSON strict validation |
| `default_complex.sql` | ARRAY / MAP / STRUCT (consolidates `test_complex_default_all_paths` + `test_complex_default_correctness`) |

(Final count after smoke gating: up to 9 — `default_complex` consolidates
the two originally-separate complex cases.)

### 5.3 Delete without replacement

Removed because either the feature has no Iceberg equivalent or the test
point is a StarRocks table-model abnormal that's not meaningful on Iceberg.

**schema-be (4 — all):**
- `be_compactions_single_snapshot` — queries BE compactions virtual table
- `be_metrics_unsupported` — negative test on be_metrics
- `be_tablet_write_log_default_off` — BE tablet write log
- `be_txns_empty_filter` — BE txns

**schema-info (2 — all):**
- `auto_increment_create_table` — AUTO_INCREMENT + PRIMARY KEY
- `loads_count` — `information_schema.loads` (stream load history)

**schema-change SR-only abnormal (6):**
- `alter_pk_table_abnormal`
- `alter_pk_char_index`
- `alter_unique_table_abnormal`
- `alter_duplicate_table_abnormal`
- `auto_increment_schema_change`
- `test_hll_bitmap_default`

**schema-change SR-only feature (8):**
- `alter_cooldown` (storage_cooldown_ttl)
- `alter_table_storage_ttl`
- `alter_char_padding`
- `alter_table_with_rollup_mv`
- `meta_scan_fast_schema_evolution`
- `drop_partition_distribution_column`
- `alter_partition_rename`
- `add_drop_field_not_allowed` (fast_schema_evolution=false coverage)

**schema-change MV alter (2):**
- `alter_mv_basic`
- `alter_mv_schema_change`

Total: **22 cases deleted without replacement** plus the three source
directories removed in the same commit.

Final counts: 38 source cases → 6 + 0..10 migrated, 22 dropped, plus a
small number consolidated (2 complex-default cases → 1 file in the new
suite).

## 6. Rewrite Guidelines

Every rewritten case follows these rules:

1. **Catalog binding.** No `SET catalog default_catalog;` — the suite's
   `init.sql` already sets the active catalog to
   `iceberg_ddl_cat_${suite_uuid0}`.
2. **Database.** Reference `${case_db}` (and `${case_db_2}` if the original
   used it).  The runner auto-creates these per case.
3. **CREATE TABLE.** Strip `DUPLICATE/UNIQUE/PRIMARY/AGGREGATE KEY(...)`,
   `DISTRIBUTED BY HASH(...) BUCKETS N`, and `PROPERTIES(...)` that carry
   StarRocks OLAP knobs (`replication_num`, `replicated_storage`,
   `compression`, `fast_schema_evolution`, `storage_format`,
   `enable_persistent_index`).  Iceberg v3 tables use
   `TBLPROPERTIES ("format-version" = "3")`; for cases that exercise
   row-level mutation (DELETE / UPDATE / MERGE) also add
   `"write.row-lineage" = "true"`.  Pure schema-evolution cases that don't
   mutate existing rows may omit the v3 property (Iceberg default works).
4. **Types.** Iceberg primitives only:
   `BOOLEAN / INT / BIGINT / FLOAT / DOUBLE / DECIMAL(p,s) / DATE /
   DATETIME / STRING / VARBINARY / JSON / ARRAY / MAP / STRUCT`.
   `TINYINT` and `SMALLINT` widen to `INT`; `CHAR(N)` widens to `STRING`.
   Iceberg has no `LARGEINT`.
5. **Sleep + retry hooks.** Original cases use `SET @a = sleep(2)` /
   `@retry_count` / `SHOW ALTER TABLE COLUMN ... WHERE ... = 'FINISHED'`
   because StarRocks schema change is asynchronous.  Iceberg ALTER TABLE
   is synchronous at metadata commit, so these sleeps and SHOW ALTER
   probes are removed.
6. **Tags and comments.** Replace `schema_be` / `schema_change` /
   `schema_info` in `@tags` with `iceberg_ddl`.  Drop "Migrated from
   dev/test ..." preambles.  Rewrite "primary key table" / "duplicate key
   table" / "fast schema evolution" wording to neutral Iceberg DDL wording.
7. **Result determinism.** Every visible SELECT keeps `ORDER BY` on all
   visible columns.
8. **SHOW CREATE TABLE assertions.** Iceberg's SHOW CREATE TABLE output
   differs from StarRocks's.  Cases that asserted on StarRocks-specific
   property fragments (e.g., `"replication_num" = "1"`) drop those
   assertions and instead assert on the genuinely-portable parts
   (`COMMENT "..."` on columns, the column type, etc.).

## 7. Result Files

`result/*.result` are generated fresh via `--mode record` after every case
is written.  Old `.result` files from the source suites are never reused
because:
- DDL is rewritten so the output schema and row layout differ.
- Iceberg writes through different file layouts than the StarRocks OLAP
  engine.
- The case's `${case_db}` expansion differs.

After `record`, each result is eyeballed for sanity (row counts, NULL
backfill correctness, expected error substrings) before committing.

## 8. Open Risks and Verification

**No-fallback rule.**  If implementation hits a feature NovaRocks does not
support on Iceberg (DEFAULT values, STRUCT field evolution add/drop, ARRAY
field evolution, ALTER COLUMN COMMENT, etc.), **stop and surface the gap to
the user with the exact failing statement, error message, and reproducing
case.**  Do **not** silently rewrite the case into an equivalent that
bypasses the missing feature.

| Risk | Action if hit |
|---|---|
| Iceberg `ALTER TABLE ... ADD COLUMN ... DEFAULT v` not implemented | Stop, report; the smoke gate from §5.2 catches this before any default case is written |
| `INSERT INTO t (subset_cols) VALUES (...)` doesn't fill DEFAULT for unspecified columns on Iceberg | Stop, report (separate from ADD COLUMN DEFAULT support) |
| `UPDATE t SET col = DEFAULT WHERE ...` not implemented | Stop, report; some default cases use this — those subqueries are deferred if it's unsupported |
| `ALTER TABLE ... MODIFY COLUMN c1 ADD FIELD ...` on STRUCT not implemented for our catalog kind | Stop, report.  sql-tests/iceberg already has `iceberg_schema_evolution_nested.sql` which suggests it works; verify before assuming |
| `ALTER TABLE ... ALTER COLUMN ... COMMENT ...` not implemented | Stop, report; alter_column_comment is the only case that exercises it |
| `SHOW CREATE TABLE` output for an external Iceberg catalog table differs enough that the `@result_contains` substrings need rework | Determine the actual output and update the substring assertions to match Iceberg's stable output |
| NovaRocks's `ARRAY<STRUCT>` field-evolution uses different syntax than StarRocks's `[*].field` notation | Discover the correct Iceberg-side syntax (verify against existing sql-tests/iceberg cases) and use it; do not weaken the test point |
| MinIO/Iceberg-REST fixture not running | Cases will fail with S3 unreachable; same prerequisite as iceberg-dml — `docker/iceberg-rest/up.sh` |

### Verification plan

1. `cargo build` after the `config.rs` edit.
2. `docker/iceberg-rest/up.sh` to ensure MinIO + REST catalog are up; start
   `standalone-server` against the generated standalone config.
3. Run §5.2 smoke gate manually via the mysql client; record outcome before
   writing default cases.
4. `sql-tests --suite iceberg-ddl --mode record --record-from target` to
   generate result files for the cases that survive smoke gating.
5. Spot-check each `.result` file for empty / error / wrong row count.
6. `sql-tests --suite iceberg-ddl --mode verify` — all-green required
   before commit.
7. After commit, `sql-tests --suite schema-be` / `schema-change` /
   `schema-info` each must error with "unknown suite" (proves directory
   removal landed).
8. `cargo fmt` + `cargo clippy` on whatever crates were touched by
   NovaRocks-side fixes.

## 9. Out of Scope (Filed for Later)

- Restoring schema-be observability coverage as a new `engine-info` suite.
- Restoring StarRocks-flavored MV alter coverage (alter_mv_basic /
  alter_mv_schema_change) — would belong in `mv-on-iceberg` after Iceberg
  MV alter is fully wired.
- Iceberg-side type-evolution coverage that doesn't already exist in
  sql-tests/iceberg (date→timestamp widen, decimal widen, etc., which are
  already covered there).
