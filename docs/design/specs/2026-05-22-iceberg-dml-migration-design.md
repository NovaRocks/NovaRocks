# write-path → iceberg-dml Suite Migration Design

Status: Draft
Date: 2026-05-22
Author: harbor.liu

## 1. Background

The `sql-tests/write-path` suite was authored when NovaRocks ran behind a
StarRocks FE.  It encodes DDL and DML semantics that depend on:

- StarRocks OLAP table models (`DUPLICATE / UNIQUE / PRIMARY / AGGREGATE KEY`,
  `DISTRIBUTED BY HASH ... BUCKETS`, `replication_num`).
- Features with no equivalent under Iceberg-as-backend
  (`AUTO_INCREMENT` columns, `BITMAP_UNION` / `HLL_UNION` aggregate columns,
  stream-load HTTP API, `PROPERTIES("merge_condition"=...)`).
- Managed-lake table-model DELETE restrictions (UNIQUE/AGG reject non-key
  `WHERE`, DUP rejects `OR`).

NovaRocks has since moved to a standalone SQL engine whose primary storage
backend is Apache Iceberg (see CLAUDE.md §1, §4.7).  Many `write-path` cases
fail because the StarRocks-shaped DDL or the table-model semantics no longer
apply.  The remaining DML-shape test points (INSERT / SELECT / DELETE / type
coercion / partitioning) are still valuable but need to be re-expressed
against an Iceberg backend.

This design covers a one-shot migration: build a new `iceberg-dml` suite,
rewrite the surviving test points against Iceberg, and delete the
`write-path` directory entirely.  Per
[memory/feedback_no_backwards_compat_for_novarocks.md], no compatibility
shim or dual run is required — `write-path` is removed in the same change.

## 2. Goals

1. Create a focused `iceberg-dml` test suite whose cases run end-to-end
   against a hadoop-Iceberg + MinIO backend via the existing
   `docker/iceberg-rest` fixture.
2. Preserve every DML test point from `write-path` that survives the
   Iceberg-as-backend model.
3. Eliminate every test point that only makes sense under a StarRocks OLAP
   table model.
4. Leave the runner architecture untouched except for adding the suite to
   the placeholder-defaults map.

## 3. Non-Goals

- Adding new test points beyond what `write-path` covers today.
- Changing the SQL test runner contract, placeholder semantics, or case
  metadata syntax.
- Migrating tests to REST catalog or managed-lake catalogs.
- Touching other suites (`iceberg`, `iceberg-compatibility`,
  `iceberg-ivm`, `mv-on-iceberg`, `optimize-table`, ...).

## 4. Suite Layout

```
sql-tests/iceberg-dml/
  init.sql       # creates external catalog iceberg_dml_cat_${suite_uuid0}
  cleanup.sql    # drops the catalog
  sql/           # 21 cases (listed below)
  result/        # generated via --mode record
```

### 4.1 `init.sql`

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

This matches `sql-tests/iceberg/init.sql` byte-for-byte except for the
catalog name.  Sharing the same warehouse defaults keeps the suite aligned
with the existing local-dev fixture.

### 4.2 `cleanup.sql`

```sql
DROP CATALOG IF EXISTS `iceberg_dml_cat_${suite_uuid0}`;
```

### 4.3 Runner wiring

`tests/sql-test-runner/src/config.rs::apply_suite_placeholder_defaults`
currently keys hadoop-catalog defaults on `"iceberg"`, `"iceberg-ivm"`,
`"mv-on-iceberg"`.  Add `"iceberg-dml"` to the same match arm so it inherits
`iceberg_catalog_type=hadoop`, `iceberg_catalog_warehouse=...` and OSS env
defaults.  No other runner changes are required — suite discovery is
already directory-driven.

## 5. Case Inventory and Migration Map

### 5.1 Migrate (23 cases)

Files keep their original test intent but DDL is rewritten to Iceberg.
Where the original name encodes a removed table model (e.g. `managed_dup_…`),
the migrated name drops the prefix.

| Theme | New filename | Source | Notes |
|---|---|---|---|
| Basic DML | `create_database.sql` | same | unchanged content; verifies `${case_db}` visible in `iceberg_dml_cat.information_schema` |
| Basic DML | `create_insert_select.sql` | same | strip OLAP DDL; pure Iceberg `CREATE TABLE ... (id BIGINT, name STRING, qty BIGINT);` |
| Basic DML | `null_insert_select.sql` | same | strip OLAP DDL |
| Basic DML | `int_insert_cast.sql` | same | strip OLAP DDL |
| Basic DML | `insert_values_select_repeated.sql` | same | strip OLAP DDL |
| Basic DML | `group_by_aggregate.sql` | same | strip OLAP DDL |
| Datetime | `datetime_insert_values.sql` | same | strip OLAP DDL |
| Datetime | `date_datetime_insert_select.sql` | same | strip OLAP DDL |
| Datetime | `datetime_invalid_literal_sanitize_insert_select.sql` | same | strip OLAP DDL |
| Datetime | `datetime_timezone_normalization_insert_select.sql` | same | strip OLAP DDL |
| Datetime | `datetime_microsecond_precision_delete.sql` | same | `TBLPROPERTIES ("format-version" = "3")` (every DELETE / MERGE case in this suite uses v3) |
| Decimal | `decimal_insert_select.sql` | same | strip OLAP DDL |
| Decimal | `decimal_overflow_to_null_insert_select.sql` | same | strip OLAP DDL |
| Decimal | `decimal_rounding_insert_select.sql` | same | strip OLAP DDL |
| DELETE | `delete_in_list.sql` | `managed_dup_delete_in_list` | v3 Iceberg table; DELETE WHERE id IN (...) |
| DELETE | `delete_is_null.sql` | `managed_dup_delete_is_null` | v3 Iceberg table; DELETE WHERE v IS NULL |
| DELETE | `delete_non_key_col.sql` | `managed_dup_delete_non_key_col` | v3 Iceberg table; DELETE WHERE v = 20 |
| DELETE | `delete_complex_where.sql` | `managed_pk_delete_complex_where` | v3 Iceberg table; DELETE WHERE LOWER(label) = 'y' |
| DELETE | `delete_no_match.sql` | `managed_pk_delete_no_match` | v3 Iceberg table; DELETE no-op |
| INSERT+DELETE flow | `insert_delete_select.sql` | `primary_key_insert_delete_select` | v3 Iceberg; INSERT seed → DELETE one row → ordered SELECT |
| MERGE INTO | `merge_into_upsert_delete.sql` | `primary_key_upsert_delete_select` | v3 Iceberg; INSERT seed → MERGE INTO with `WHEN MATCHED UPDATE / WHEN NOT MATCHED INSERT` for upsert → DELETE → ordered SELECT |
| Partitioned INSERT | `partitioned_insert.sql` | `multi_partition_multi_bucket_create_insert` | `PARTITION BY identity(p)` on Iceberg; INSERT rows that exercise both partitions |
| Lifecycle | `table_lifecycle.sql` | `managed_lake_lifecycle` (positive path only) | Iceberg CREATE → INSERT → SELECT → TRUNCATE → INSERT → SELECT → DROP → `@expect_error` on stale read.  Negative cases for DUPLICATE-key validation are removed. |

Total: **23**.

### 5.2 Delete without replacement (15 cases)

Removed because the underlying feature has no Iceberg equivalent or because
the surviving semantic is already covered by another migrated case.

| Case | Reason |
|---|---|
| `agg_keys_bitmap_hll` | AGG_KEYS table + BITMAP_UNION / HLL_UNION aggregate columns are StarRocks-only |
| `auto_increment_column_partial_update` | AUTO_INCREMENT column + ADMIN SET FRONTEND CONFIG, StarRocks-only |
| `auto_increment_insert_select_null` | same |
| `auto_increment_insert_update` | same |
| `auto_increment_multi_row_insert` | same |
| `auto_increment_non_key_column` | same |
| `auto_increment_null_handling` | same |
| `auto_increment_partial_update` | same |
| `write_condition_update` | stream-load HTTP API + `merge_condition` PROPERTY, StarRocks-only |
| `tinyint_insert_values` | TINYINT is not an Iceberg primitive; type-cast semantics covered by `int_insert_cast` |
| `managed_dup_delete_or_rejected` | Tests a managed-lake DUP-key restriction (`OR` rejected); Iceberg accepts arbitrary `WHERE` |
| `managed_unique_delete_nonkey_rejected` | UNIQUE-key table model does not exist on Iceberg |
| `managed_unique_delete_keyonly` | UNIQUE-key table model does not exist on Iceberg; DELETE-by-key semantics already covered by `delete_in_list` |
| `managed_agg_delete_keyonly` | AGG-key table model does not exist on Iceberg |
| `managed_lake_insert_select` | duplicates `create_insert_select` after DDL rewrite |

`primary_key_insert_delete_select` and `primary_key_upsert_delete_select`
are not listed here because they are migration sources for
`insert_delete_select.sql` and `merge_into_upsert_delete.sql` respectively.
The original files vanish when the `write-path` directory is removed.

The whole `sql-tests/write-path/` directory (`sql/`, `result/`, anything
else) is removed in the same commit set after the new suite is verified.
Total cases lost without replacement: **15**.  Migrated: **23**.  Original
write-path total: **38**.

## 6. Rewrite Guidelines

To keep the migration mechanical and reviewable, every rewritten case
follows these rules:

1. **Catalog binding.**  Remove any `SET catalog default_catalog;` line.
   The suite's `init.sql` already sets the active catalog to
   `iceberg_dml_cat_${suite_uuid0}`.
2. **Database.**  Continue to reference `${case_db}` (and `${case_db_2}` if
   present in the original).  The runner auto-creates these per case.
3. **CREATE TABLE.**  Strip `DUPLICATE/UNIQUE/PRIMARY/AGGREGATE KEY(...)`,
   `DISTRIBUTED BY HASH(...) BUCKETS N`, and `PROPERTIES (...)` clauses
   that carry StarRocks OLAP knobs (`replication_num`, `replicated_storage`,
   `compression`).  All tables in this suite are Iceberg **format-version 3**
   (NovaRocks's first-class write target).  Every CREATE TABLE that
   exercises row-level mutation (DELETE / MERGE / INSERT-then-DELETE)
   declares `TBLPROPERTIES ("format-version" = "3")` explicitly so the
   intent is obvious in the case file; pure INSERT-only cases may omit
   the property and accept whatever default NovaRocks emits.  Iceberg v2
   is not used anywhere in this suite.
4. **Types.**  Iceberg primitives only:
   `BOOLEAN / INT / BIGINT / FLOAT / DOUBLE / DECIMAL(p,s) / DATE /
   DATETIME / STRING / VARBINARY / ARRAY / MAP / STRUCT`.
   `TINYINT` is rejected at DDL time on Iceberg, so any TINYINT in a
   migrated case is widened to `INT`.  No such widening is needed for the
   23 migrated cases.
5. **Comments and tags.**  Replace `write_path` in `@tags` and in the
   Test Objective preamble with `iceberg_dml`.  Rewrite "PRIMARY KEY
   DELETE" / "DUP_KEYS DELETE WHERE" / "managed-lake" wording to neutral
   Iceberg DELETE wording.  Drop any "Migrated from dev/test ..." lines —
   they refer to a defunct upstream tree.
6. **Result determinism.**  Every SELECT keeps `ORDER BY` on every visible
   column to guarantee a stable ordering on Iceberg's file-level output.
7. **DROP at end.**  Where the original ended without an explicit DROP,
   no DROP is added — the per-case database is recreated each run, which
   already gives clean state.

## 7. Result Files

`result/*.result` files are generated fresh by running the new suite under
`--mode record`.  Old `write-path` result files are not reused because:

- The DDL changes the column-store representation, which can change file
  ordering for queries without `ORDER BY` (we add `ORDER BY` for every
  SELECT, so this is belt-and-braces).
- Some cases now go through Iceberg's writer path and the `${case_db}`
  placeholder expansion changes.

After `record`, every result is reviewed for sanity (no stray error rows,
expected row counts) before committing.

## 8. Open Risks and Verification

**No-fallback rule.**  If implementation hits a feature NovaRocks does not
support on Iceberg v3 (MERGE INTO, TRUNCATE, identity-partition INSERT,
multi-row DELETE on a v3 table with a non-key WHERE, etc.), stop and
surface the gap to the user with the exact failing statement, error
message, and reproducing case.  Do **not** silently rewrite the case into
an equivalent that bypasses the missing feature — the point of these
cases is to exercise the feature in question.

| Risk | Action if hit |
|---|---|
| Iceberg `MERGE INTO` not implemented on standalone+Iceberg v3 | Stop, report the rejected MERGE statement + error to user.  `merge_into_upsert_delete` is the only MERGE case; if MERGE is unsupported, the user decides whether to drop the case, file a NovaRocks gap, or unblock the suite without it |
| `TRUNCATE TABLE` not implemented on Iceberg v3 | Stop, report.  `table_lifecycle` is the only case that uses TRUNCATE |
| `PARTITION BY identity(p)` INSERT path not implemented on Iceberg v3 | Stop, report.  `partitioned_insert` is the only case that uses identity-partition writes |
| `create_database.sql` queries `default_catalog.information_schema.schemata`, which does not surface databases created in an external Iceberg catalog | Determine the correct query against `iceberg_dml_cat_${suite_uuid0}` (information_schema or `SHOW DATABASES`) by running it manually first.  If neither path returns the per-case database, stop and report |
| `format-version = 3` default is unclear | Always emit `TBLPROPERTIES ("format-version" = "3")` on mutation cases; emit nothing on pure INSERT cases.  Capture the standalone default in the case header comment once observed |
| MinIO fixture not running in dev shell | Cases will fail with a `S3 endpoint unreachable` error; the suite README points at `docker/iceberg-rest/up.sh` (same prerequisite as the existing `iceberg` suite) |

### Verification plan

1. `cargo build` (after `config.rs` edit) — sanity-compile runner.
2. `cargo build` of `tests/sql-test-runner/Cargo.toml`.
3. `docker/iceberg-rest/up.sh` to ensure the catalog + MinIO are up; start
   `standalone-server` against the generated config.
4. `sql-tests --suite iceberg-dml --mode record` to generate `result/`.
5. Manual eyeball over each `.result` for empty / error / wrong-row-count
   signals.
6. `sql-tests --suite iceberg-dml --mode verify` — must be all-green.
7. After commit, `sql-tests --suite write-path --mode verify` must error
   with "suite not found" (proves directory removal landed).
8. `cargo clippy -- -D warnings` and `cargo fmt --check`.

## 9. Out of Scope (Filed for Later)

- Extending `iceberg-dml` with type-coverage cases the original suite
  didn't exercise (TIME, FIXED, UUID, nested STRUCT writes, etc.).
- Migrating the same logical cases to REST catalog or managed-lake catalog.
- Iceberg v3 row-lineage variants of these DML tests (already covered by
  the existing `iceberg` suite).
