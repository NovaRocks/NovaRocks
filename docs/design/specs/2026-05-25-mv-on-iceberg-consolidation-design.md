# mv-on-iceberg → iceberg-ivm Consolidation Design

Status: Draft
Date: 2026-05-25
Author: harbor.liu

## 1. Background

`sql-tests/mv-on-iceberg/` (17 cases) and `sql-tests/iceberg-ivm/` (51 cases)
overlap in scope — both cover materialized views with Iceberg base tables.
After a pair-wise analysis (research subagent run 2026-05-25), 9 mv-on-iceberg
cases were found to test points NOT covered by iceberg-ivm and should
migrate; the other 8 are redundant and should be dropped.

## 2. Key architectural difference

- **mv-on-iceberg** cases use **OLAP-target MVs** — default storage, no
  `storage_engine` property. The MV stores its data in NovaRocks's local
  OLAP storage.
- **iceberg-ivm** cases use **Iceberg-target MVs** — `PROPERTIES(
  'storage_engine'='iceberg')`. The MV stores its data in the Iceberg
  catalog alongside the base table.

The **base-side Iceberg semantics** (snapshot scan, deletes, COW/MOR,
equality delete, row-lineage, partition evolution) is the same on both
sides — only the MV target storage differs. Since iceberg-ivm is the
strategic direction (Iceberg-target MVs), migration of the 9 KEEP cases
involves a **semantic conversion**: the MV is rewritten to use
`storage_engine='iceberg'`, the base table is upgraded to
`format-version=3` + `write.row-lineage=true` (required for Iceberg-target
IVM), and the result file is re-recorded.

## 3. Goals

1. Move the 9 unique-coverage cases from `mv-on-iceberg/` to
   `iceberg-ivm/`, applying the OLAP-target → Iceberg-target semantic
   conversion.
2. Delete the 8 redundant cases.
3. Delete `sql-tests/mv-on-iceberg/` entirely.
4. After migration, run `iceberg-ivm --mode verify` — must stay all-green.
5. Apply the **no-fallback rule**: if any KEEP case fails after
   conversion, stop and surface (fix NovaRocks if needed, or split the
   case to isolate the failure).

## 4. Non-Goals

- Adding new test coverage beyond what mv-on-iceberg already carries.
- Renaming or reorganizing the existing 51 iceberg-ivm cases.
- Consolidating cases within iceberg-ivm itself (out of scope).
- Adding documentation for the storage_engine='iceberg' property
  (covered elsewhere in the project).

## 5. Migration map

### 5.1 Keep + Migrate (9 cases)

| Source (mv-on-iceberg/sql/) | Destination (iceberg-ivm/sql/) | Why this case is unique |
|---|---|---|
| `managed_lake_mv_basic.sql` | `iceberg_backed_mv_basic_lifecycle.sql` | The only basic happy-path lifecycle test: CREATE / pre-refresh-empty SELECT / first REFRESH / append / second REFRESH / SHOW / DROP |
| `managed_lake_mv_aggregate_avg_min_max.sql` | `iceberg_backed_mv_aggregate_avg_min_max.sql` | AVG state type semantics + DDL rejection list (`AVG(*)`, `AVG(string)`, `MIN(*)`) |
| `managed_lake_mv_iceberg_ivm_strategy.sql` | `iceberg_backed_mv_refresh_strategy.sql` | INSERT OVERWRITE refresh strategy fallback (full rebuild) + S3-backed base DELETE driving mixed full/incremental decisions |
| `managed_lake_mv_ivm_pk_invalid.sql` | `iceberg_backed_mv_pk_invalid.sql` | IVM Phase-2 PRIMARY KEY DDL validation (missing column / nullable / empty / duplicate) |
| `managed_lake_mv_merge_cow.sql` | `iceberg_backed_mv_merge_cow.sql` | MERGE INTO driving COW UPDATE + FastAppend INSERT snapshots in one snapshot pair |
| `managed_lake_mv_merge_mor.sql` | `iceberg_backed_mv_merge_mor.sql` | MERGE INTO driving MOR update-marker snapshot + FastAppend INSERT snapshot |
| `managed_lake_mv_projection_hidden_pk_delete.sql` | `iceberg_backed_mv_projection_hidden_pk_delete.sql` | User PK hidden from MV SELECT output, deletion still applies through hidden PK |
| `managed_lake_mv_read_semantics_partition_v3_delete.sql` | `iceberg_backed_mv_projection_partition_evolution_delete.sql` | Partition spec evolution (DROP PARTITION COLUMN) + cross-spec DELETE on projection MV |
| `test_mv_with_iceberg_recreate.sql` | `iceberg_backed_mv_base_recreate_and_rewrite.sql` | MV rewrite (EXPLAIN with enable_materialized_view_rewrite) + base DROP/CREATE same name + automatic_active_check + information_schema.materialized_views inspection |

### 5.2 Drop redundant (8 cases)

| Source | Covered by (iceberg-ivm) |
|---|---|
| `managed_lake_mv_aggregate_ivm.sql` | `iceberg_ivm_aggregate_target.sql`, `iceberg_ivm_aggregate_min_max_insert_only.sql` |
| `managed_lake_mv_equality_delete.sql` | `iceberg_ivm_aggregate_count_only_delete_boundary.sql`, `iceberg_ivm_aggregate_min_max_delete_boundary.sql`, `iceberg_ivm_aggregate_min_max_delete_non_boundary.sql` |
| `managed_lake_mv_incremental.sql` | `iceberg_backed_mv_projection_filter.sql` (same projection+filter+append shape) |
| `managed_lake_mv_projection_delete.sql` | `iceberg_ivm_base_delete_row_lineage.sql`, `iceberg_ivm_a1_large_delta_mixed.sql` |
| `managed_lake_mv_projection_v3_delete.sql` | `iceberg_ivm_base_delete_row_lineage.sql`, `iceberg_ivm_a1_update_only.sql` |
| `managed_lake_mv_read_semantics_equality_delete.sql` | Combined coverage by `iceberg_ivm_a11_base_type_change_referenced.sql` + `iceberg_ivm_base_delete_row_lineage.sql` |
| `managed_lake_mv_update_cow.sql` | `iceberg_ivm_a1_update_only.sql`, `iceberg_ivm_base_delete_row_lineage.sql` |
| `managed_lake_mv_update_mor.sql` | `iceberg_ivm_a1_update_only.sql` (tagged merge_on_read,position_delete — same MOR path) |

### 5.3 Delete source directory

After the 9 migrations, delete `sql-tests/mv-on-iceberg/` entirely with
`git rm -r`. Confirm `sql-tests --suite mv-on-iceberg` errors with
"unknown suite".

## 6. Migration mechanics per KEEP case

Each KEEP case migration requires **four edits** from the source to
destination form:

1. **MV PROPERTIES**: add `PROPERTIES('storage_engine'='iceberg', ...)` to the
   `CREATE MATERIALIZED VIEW` statement. Existing OLAP-target properties
   (e.g., `partition_refresh_number`, `partition_ttl`) stay if Iceberg
   target supports them; drop OLAP-only properties like `replication_num`.

2. **Base table v3 + row-lineage**: add
   `TBLPROPERTIES ("format-version"="3", "write.row-lineage"="true")` to
   the base CREATE TABLE if missing. (Iceberg-target IVM requires v3
   row-lineage for the change-set scan.)

3. **Catalog naming**: mv-on-iceberg cases use case-local catalog
   `mv_*_${uuid0}`; iceberg-ivm uses the same per-case pattern. The
   catalog name conventions don't need a sed pass — keep the case-local
   catalog name but rename to match iceberg-ivm style if it improves
   readability (e.g., `mv_basic_${uuid0}` → `ice_basic_${uuid0}`).

4. **Re-record `.result`**: the row format differs between OLAP-target
   and Iceberg-target SHOW MATERIALIZED VIEW output and possibly other
   result-bearing queries. Always `--mode record` the destination case
   fresh.

## 7. No-fallback rule

Per PR #166 / #172 / #173 standing rule: if a KEEP case fails after
conversion (e.g., an MV feature the OLAP target supports is missing on
Iceberg target, or NovaRocks doesn't accept a particular property
combination), STOP and surface the gap. Either:
- Fix the NovaRocks gap in this PR (preferred), or
- Defer the case with a follow-up fix task (only if the gap is large).

Do NOT silently rewrite the case to bypass the missing feature.

## 8. Verification

1. `cargo build` — should be clean unless a NovaRocks fix lands.
2. `docker/iceberg-rest/up.sh` + start `standalone-server`.
3. After each KEEP case migration: `sql-tests --suite iceberg-ivm
   --only <new_name> --mode verify` → PASS.
4. After all 9 migrated: `sql-tests --suite iceberg-ivm --mode verify`
   — total = 51 + 9 = 60, all PASS.
5. After deleting source dir: `sql-tests --suite mv-on-iceberg`
   — `unknown suite`.

## 9. Final counts

- `iceberg-ivm/` — 51 → **60** cases.
- `mv-on-iceberg/` — 17 → **deleted**.
- 8 redundant cases dropped without replacement.
