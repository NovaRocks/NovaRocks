# ddl + iceberg → existing iceberg-* sub-suites Consolidation Design

Status: Draft
Date: 2026-05-25
Author: harbor.liu

## 1. Background

After PR #166 (write-path → iceberg-dml) and PR #172 (schema-* → iceberg-ddl)
landed, the `sql-tests/` tree has settled around a "function-named" pattern:

- `iceberg` — kitchen-sink Iceberg coverage that pre-dates the sub-suite split
- `iceberg-ddl` — DDL on Iceberg (ALTER, DEFAULT, STRUCT/ARRAY evolution)
- `iceberg-dml` — DML on Iceberg (INSERT, DELETE, MERGE)
- `iceberg-compatibility` — Spark/REST interop
- `iceberg-rest` — REST catalog
- `iceberg-ivm` — IVM (incremental MV)
- `mv-on-iceberg` — MV on Iceberg

Two sources still don't fit cleanly:

- `sql-tests/ddl/` (6 cases) — StarRocks-flavored DDL (PRIMARY KEY ORDER BY
  reorder, ALTER TABLE SWAP, RANDOM distribution, sync rollup MV, CREATE
  TABLE LIKE on PK+BITMAP tables, etc.). Most are StarRocks-only; one has
  an Iceberg-relevant slice (table-level COMMENT).
- `sql-tests/iceberg/` (67 cases) — the kitchen sink that pre-dates the
  sub-suite split. About half the cases now belong in `iceberg-ddl` or
  `iceberg-dml` and the move would make the suite easier to scan.

This design covers a one-shot consolidation: move the cases that belong in
sub-suites; consolidate similar adjacent cases where the test points
overlap; leave the rest in `iceberg`; delete `sql-tests/ddl/` entirely.

Per [memory/feedback_no_backwards_compat_for_novarocks.md], no
parallel-run or compat shim — cases move and the source directory is
removed in the same change set.

## 2. Goals

1. Move every case from `sql-tests/iceberg/` that primarily exercises a
   sub-suite's theme into the matching sub-suite.
2. Migrate the salvageable DDL test points from `sql-tests/ddl/` into
   `iceberg-ddl`, drop the rest, delete the directory.
3. Where a moved case overlaps with an existing iceberg-ddl/iceberg-dml
   case, keep both but **differentiate the test points** — adjust headers
   and column coverage so each case has a clear, non-overlapping intent.
4. Apply the same **no-fallback rule** as PR #166/#172: any NovaRocks gap
   that surfaces during the move stops the work and gets a fix commit
   in this PR.

## 3. Non-Goals

- Adding new test coverage beyond what the two source suites carry today.
- Creating new sub-suites (`iceberg-snapshots`, `iceberg-stats`, …) — user
  preference is to only use *existing* iceberg-* suites.
- Moving cases out of the existing sub-suites (`iceberg-ivm`,
  `iceberg-rest`, `iceberg-compatibility`, `mv-on-iceberg`) into other
  homes.

## 4. ddl → iceberg-ddl

### 4.1 Migrate (up to 2)

| New filename | Source | Test point |
|---|---|---|
| `alter_table_comment.sql` | `ddl_alter_table.sql` (queries 1-2 only) | `ALTER TABLE t COMMENT 'x'` (table-level comment) + SHOW CREATE TABLE round-trip |
| `create_table_like.sql` | `ddl_create_table_like.sql` (smoke-gated) | `CREATE TABLE t2 LIKE t1` on Iceberg — schema mirroring |

The `create_table_like` migration is **smoke-gated**. The pre-task smoke
(see §6.1) runs `CREATE TABLE iceberg_ddl_cat.db.t2 LIKE iceberg_ddl_cat.db.t1`
against the standalone server:

- If Iceberg `CREATE TABLE LIKE` is supported → migrate the case.
- If rejected → defer with a follow-up fix task. Don't silently
  rewrite to `CREATE TABLE ... (col1 type1, col2 type2, ...)` (that
  defeats the test point).

### 4.2 Delete without replacement (4)

- `ddl_alter_pk_reorder` — PRIMARY KEY ORDER BY sort-key reorder
- `ddl_alter_swap_table` — ALTER TABLE SWAP (StarRocks-only)
- `ddl_random_distribution` — DUPLICATE KEY + RANDOM distribution
- `ddl_random_distribution_mv` — sync rollup MV + RANDOM

The non-comment portion of `ddl_alter_table` (queries 3-24: PK ORDER BY,
ADD/MODIFY COLUMN with BITMAP indexes on PK tables, retry/sleep around
async schema-change job) is **dropped**. The ADD/MODIFY COLUMN test
points are already covered by iceberg-ddl's `add_column_count_star`,
`alter_column_comment`, and the various STRUCT field cases.

After the migration, `sql-tests/ddl/` is `git rm -r`'d.

## 5. iceberg → iceberg-ddl + iceberg-dml

### 5.1 Naming convention

Cases moving into `iceberg-ddl/` or `iceberg-dml/` drop the redundant
`iceberg_` prefix from their filenames. e.g. `iceberg_schema_evolution_nested.sql`
→ `schema_evolution_nested.sql`. Cases that stay in `iceberg/` keep their
existing names.

### 5.2 Move to iceberg-ddl (30 cases)

| Theme | New filename (in iceberg-ddl) | Source filename |
|---|---|---|
| schema evolution | `schema_evolution_array_map_widen.sql` | `iceberg_schema_evolution_array_map_widen.sql` |
| schema evolution | `schema_evolution_date_to_timestamp_widen.sql` | `iceberg_schema_evolution_date_to_timestamp_widen.sql` |
| schema evolution | `schema_evolution_decimal_widen.sql` | `iceberg_schema_evolution_decimal_widen.sql` |
| schema evolution | `schema_evolution_local.sql` | `iceberg_schema_evolution_local.sql` |
| schema evolution | `schema_evolution_nested.sql` | `iceberg_schema_evolution_nested.sql` |
| schema evolution | `schema_evolution_nullability.sql` | `iceberg_schema_evolution_nullability.sql` |
| schema evolution | `schema_evolution_reorder.sql` | `iceberg_schema_evolution_reorder.sql` |
| schema evolution | `schema_evolution_s3.sql` | `iceberg_schema_evolution_s3.sql` |
| schema evolution | `schema_evolution_widen_reject.sql` | `iceberg_schema_evolution_widen_reject.sql` |
| v3 DEFAULT | `v3_default_add_column_existing_data.sql` | `iceberg_v3_default_add_column_existing_data.sql` |
| v3 DEFAULT | `v3_default_alter_v2_rejected.sql` | `iceberg_v3_default_alter_v2_rejected.sql` |
| v3 DEFAULT | `v3_default_complex_type_rejected.sql` | `iceberg_v3_default_complex_type_rejected.sql` |
| v3 DEFAULT | `v3_default_create_table.sql` | `iceberg_v3_default_create_table.sql` |
| v3 DEFAULT | `v3_default_decimal_scale_mismatch.sql` | `iceberg_v3_default_decimal_scale_mismatch.sql` |
| v3 DEFAULT | `v3_default_insert_select.sql` | `iceberg_v3_default_insert_select.sql` |
| v3 DEFAULT | `v3_default_null_on_v2.sql` | `iceberg_v3_default_null_on_v2.sql` |
| v3 DEFAULT | `v3_default_positional_count_mismatch.sql` | `iceberg_v3_default_positional_count_mismatch.sql` |
| v3 DEFAULT | `v3_default_primitive_types.sql` | `iceberg_v3_default_primitive_types.sql` |
| v3 DEFAULT | `v3_default_v2_rejected.sql` | `iceberg_v3_default_v2_rejected.sql` |
| table properties | `table_properties_combined_reject.sql` | `iceberg_table_properties_combined_reject.sql` |
| table properties | `table_properties_reject_reserved.sql` | `iceberg_table_properties_reject_reserved.sql` |
| table properties | `table_properties_set_unset.sql` | `iceberg_table_properties_set_unset.sql` |
| table properties | `table_properties_unset_if_exists.sql` | `iceberg_table_properties_unset_if_exists.sql` |
| catalog/type | `catalog_complex_type.sql` | `iceberg_catalog_complex_type.sql` |
| catalog/type | `catalog_time_type.sql` | `iceberg_catalog_time_type.sql` |
| DDL — truncate | `truncate.sql` | `iceberg_truncate.sql` |
| partition evolution | `partition_evolution_basic.sql` | `iceberg_partition_evolution_1.sql` |
| partition evolution | `partition_evolution_replace.sql` | `iceberg_partition_evolution_replace.sql` |
| partition evolution | `partition_evolution_unsupported.sql` | `iceberg_partition_evolution_unsupported.sql` |
| pk/fk property | `pkfk_property.sql` | `pkfk_property.sql` |

Total: **30 cases** move from `iceberg/` to `iceberg-ddl/`.

### 5.3 Move to iceberg-dml (13 cases)

| Theme | New filename (in iceberg-dml) | Source filename |
|---|---|---|
| MERGE | `merge_into_cow.sql` | `iceberg_v3_merge_cow.sql` |
| MERGE | `merge_into_mor.sql` | `iceberg_v3_merge_mor.sql` |
| UPDATE | `update_cow.sql` | `iceberg_v3_update_cow.sql` |
| UPDATE | `update_mor.sql` | `iceberg_v3_update_mor.sql` |
| INSERT OVERWRITE | `overwrite_partitions.sql` | `iceberg_v3_overwrite_partitions.sql` |
| CTAS | `ctas.sql` | `iceberg_v3_ctas.sql` |
| variant | `variant_insert.sql` | `iceberg_v3_variant_insert.sql` |
| variant | `variant_unsupported.sql` | `iceberg_v3_variant_unsupported.sql` |
| partition DELETE | `partition_evolution_delete.sql` | `iceberg_partition_evolution_delete.sql` |
| partition DELETE | `partition_evolution_v3_delete.sql` | `iceberg_partition_evolution_v3_delete.sql` |
| equality DELETE + evolution | `equality_delete_schema_evolution.sql` | `iceberg_equality_delete_schema_evolution.sql` |
| write mode | `none_write_mode.sql` | `iceberg_none_write_mode.sql` |
| branch write | `branch_write.sql` | `iceberg_branch_write.sql` |

Total: **13 cases** move from `iceberg/` to `iceberg-dml/`.

### 5.4 Stay in `iceberg/` (24 cases)

- **Query / optimization**: `iceberg_in_list_predicate`, `iceberg_min_max_opt`, `iceberg_scan_pruning_correctness`
- **Read semantics**: `iceberg_read_semantics_equality_partition`, `iceberg_read_semantics_row_lineage_evolution`
- **Statistics** (6): `iceberg_statistics_*`
- **Metadata tables** (4): `iceberg_metadata_history`, `iceberg_metadata_partitions`, `iceberg_metadata_refs`, `iceberg_metadata_snapshots`
- **Branch / Tag / Time travel** (4): `iceberg_branch_tag_ddl`, `iceberg_time_travel_select`, `iceberg_v3_branch_row_lineage`, `iceberg_v3_row_lineage_uniqueness`
- **Maintenance procedures** (5): `iceberg_v3_expire_snapshots`, `iceberg_v3_optimize_compact_data_files`, `iceberg_v3_optimize_row_lineage`, `iceberg_v3_remove_orphan_files`, `iceberg_v3_rewrite_manifests`

Total: **24 cases** stay in `iceberg/`. (67 source = 30 to iceberg-ddl + 13 to iceberg-dml + 24 stay.)

### 5.5 Consolidation policy

The 9 existing `iceberg-ddl/default_*` cases (created in PR #172) and the
10 moved `iceberg-ddl/v3_default_*` cases (moving in this PR) have
overlapping coverage of "ADD COLUMN with DEFAULT" semantics. User's
preference: **keep both sides, but differentiate test points** so each
has a clear, non-redundant intent.

After the move, each `iceberg-ddl/default_*` case (PR #172 origin) gets
its `Test Objective` comment updated to call out **what it covers that
`v3_default_primitive_types.sql` does not**:

- `default_boolean.sql` — focused write-default + initial-default with
  `BOOLEAN DEFAULT true`. v3_default_primitive_types covers boolean as
  one of 11 types; this case isolates it and exercises the empty
  INSERT (id, name) form.
- `default_numeric.sql` — covers boundary numerics (negative, zero,
  big integers, decimals at multiple scales) that primitive_types
  doesn't cover.
- `default_decimal.sql` — covers DECIMAL(20, 6) which exceeds the
  primitive types coverage; tests narrow-vs-wide scale specifically.
- `default_string.sql` — covers unicode and escape characters
  (newline, comma) that primitive_types' `'hi'` example doesn't.
- `default_date.sql` — covers DATETIME with sub-second-zero literal and
  DATE far from epoch.
- `default_varbinary.sql` — VARBINARY isn't in primitive_types.
- `default_json.sql` / `default_json_strict_validation.sql` — JSON
  isn't in primitive_types.
- `default_complex.sql` — ARRAY/MAP isn't in primitive_types
  (`v3_default_complex_type_rejected.sql` is a *negative* test; this
  case is the *positive* counterpart on the supported empty-literal
  form).

For schema_evolution, no consolidation is needed — the existing
iceberg-ddl cases (STRUCT/ARRAY field add-drop, COMMENT, count-star)
and the moved cases (widen, reorder, nullability, etc.) cover
distinct concerns by name.

## 6. Implementation Notes

### 6.1 Pre-task smoke

Before writing `create_table_like.sql`, smoke `CREATE TABLE ... LIKE` on
the existing standalone-server + Iceberg hadoop catalog:

```sql
CREATE TABLE iceberg_ddl_cat.smk.src (a INT, b STRING) TBLPROPERTIES ("format-version"="3");
CREATE TABLE iceberg_ddl_cat.smk.dst LIKE iceberg_ddl_cat.smk.src;
SHOW CREATE TABLE iceberg_ddl_cat.smk.dst;
```

Outcome decides §4.1 second-row migration.

### 6.2 Mechanical move

For the 43 case moves (30 to iceberg-ddl + 13 to iceberg-dml), each is
a `git mv` of both the `.sql` and matching `.result` file:

```bash
git mv sql-tests/iceberg/sql/iceberg_schema_evolution_nested.sql \
        sql-tests/iceberg-ddl/sql/schema_evolution_nested.sql
git mv sql-tests/iceberg/result/iceberg_schema_evolution_nested.result \
        sql-tests/iceberg-ddl/result/schema_evolution_nested.result
```

The case body shouldn't need changes — both source and destination
suites use the same hadoop+MinIO catalog setup (per
`apply_suite_placeholder_defaults`), the same `${case_db}` placeholders,
and the same `format-version=3` TBLPROPERTIES convention.

If a case references the catalog by name (e.g.
`iceberg_cat_${suite_uuid0}` vs `iceberg_ddl_cat_${suite_uuid0}`), the
case body needs to be updated to use the destination suite's catalog
name. Per-case `init.sql` style isn't used in the iceberg suite — the
suite-level `init.sql` creates the catalog and cases reference it by
name. Both `iceberg/init.sql` (`iceberg_cat_*`) and
`iceberg-ddl/init.sql` (`iceberg_ddl_cat_*`) need the case body's
catalog reference renamed when moving between them.

After moving, run `--mode verify` on the destination suite to confirm
no regression. If a case fails after the move (e.g., the destination
catalog has different defaults), STOP and report — no silent
re-recording.

### 6.3 Naming convention

The catalog references inside the moved case bodies must change:
`iceberg_cat_${suite_uuid0}` → `iceberg_ddl_cat_${suite_uuid0}` for
iceberg-ddl moves, or `iceberg_dml_cat_${suite_uuid0}` for iceberg-dml
moves. This is a single sed-style replace per case file:

```bash
sed -i '' 's/iceberg_cat_/iceberg_ddl_cat_/g' \
  sql-tests/iceberg-ddl/sql/<new_name>.sql
```

After replace, the `.result` file's recorded outputs should still match
because none of the catalog-name references end up in the case's SELECT
output (catalog name appears only in DDL identifier paths, which the
runner doesn't echo in result file rows).

### 6.4 No-fallback rule

If any moved case fails after the catalog-rename + move, STOP and
report:
- exact failing statement
- exact error
- whether it's a missing-feature gap (add fix task) or a test-design
  issue (rare; usually means the source case was using a hadoop-vs-rest
  catalog-specific path)

Don't silently update the `.result` to match the new (potentially
wrong) output.

## 7. Verification

1. `cargo build` after any engine changes (should be none unless smoke
   surfaces a gap).
2. `docker/iceberg-rest/up.sh` to ensure MinIO + REST catalog are up;
   start `standalone-server` against the generated standalone config.
3. After each suite's batch of moves: `sql-tests --suite <suite>
   --mode verify` — must pass for the destination suite.
4. After all moves: `sql-tests --suite iceberg --mode verify` for the
   shrunken `iceberg` suite — still must pass.
5. After `git rm -r sql-tests/ddl`: `sql-tests --suite ddl --mode verify`
   must error with "unknown suite".
6. `cargo fmt` if any engine fixes landed.
7. Final summary: total iceberg-ddl case count = 15 (PR #172 baseline) + 30 (from iceberg) + 1-2 (from ddl) ≈ 46-47;
   iceberg-dml case count = 24 (PR #166 baseline) + 13 = 37;
   iceberg case count = 67 - 43 (30 + 13) = 24.

## 8. Open Risks and Verification

**No-fallback rule.** If any moved case fails (parser rejects, executor
rejects, result mismatch), **stop and surface the gap**.

| Risk | Action if hit |
|---|---|
| Iceberg `CREATE TABLE LIKE` rejected | §4.1: defer the second ddl→ddl migration; add follow-up fix task |
| Moved case's `${case_db}` provisioning differs between suites | Investigate: both suites should auto-provision per-case. If iceberg-ddl provisions differently, fix the suite config |
| A moved case references `default_catalog.information_schema.*` for cross-catalog reasons | Inspect, fix, re-record on destination |
| A moved case fails because hadoop vs. REST catalog metadata path | Inspect, fix the case, document the change |
| Empty-string DEFAULT for VARBINARY (or other type) is per-source-suite specific | The PR #172 fix `e6d51c61` covered VARBINARY string DEFAULT broadly — should work; if the moved case uses a different VARBINARY syntax, harmonize on the working syntax |

## 9. Out of Scope (Filed for Later)

- Creating `iceberg-snapshots`, `iceberg-stats`, `iceberg-maintenance`,
  or similar new sub-suites for the cases that stay in `iceberg/`.
- Moving cases from `iceberg-ivm`, `iceberg-rest`,
  `iceberg-compatibility`, `mv-on-iceberg` between each other.
- Consolidating the existing iceberg-ddl `default_*` cases (PR #172
  origin) with the moved `v3_default_*` cases — both stay, with
  differentiated test points.
- Renaming the existing `iceberg-ddl/default_*` filenames for symmetry
  with the moved `v3_default_*` (e.g., `default_boolean` →
  `v3_default_boolean`). These are kept as-is to preserve PR history.
