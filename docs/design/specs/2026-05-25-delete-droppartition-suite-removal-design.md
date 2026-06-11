# delete + drop-partition Suite Removal Design

Status: Draft
Date: 2026-05-25
Author: harbor.liu

## 1. Background

After PR #166 / #172 / #173 consolidated Iceberg test coverage into the
sub-suites (`iceberg`, `iceberg-ddl`, `iceberg-dml`, etc.), two legacy
StarRocks-flavored suites still sit in `sql-tests/` with no Iceberg
analog:

- `sql-tests/delete/` (2 cases) — DELETE on DUPLICATE KEY tables, with
  ALTER RENAME COLUMN coverage.
- `sql-tests/drop-partition/` (8 cases) — DROP PARTITIONS with
  StarRocks-only features (RECOVER, `START/END/EVERY` batch syntax,
  `DROP PARTITIONS WHERE expr`, shadow partitions on auto-partitioned
  tables, LIST/RANGE partition individual drop).

This design covers a one-shot removal: both suite directories are
deleted. No cases migrate.

## 2. Why no migration

### delete suite (2 cases)

| Case | Why not migrated |
|---|---|
| `delete_dupkey_rename.sql` | Combines two test points: (a) DELETE on DUP table, (b) ALTER RENAME COLUMN + DELETE round-trip. (a) is fully covered by iceberg-dml's `delete_*` family from PR #166. (b)'s RENAME COLUMN is covered by `iceberg-ddl/sql/schema_evolution_{s3,nested,local}.sql` and `iceberg-dml/sql/equality_delete_schema_evolution.sql`. |
| `delete_empty.sql` | DELETE on empty partitioned DUP table is a no-op assertion already covered by `iceberg-dml/sql/delete_no_match.sql` (DELETE matching zero rows is a no-op). The partition wrapping is StarRocks-specific (DUP key + range partition); on Iceberg the same no-op semantic is preserved without needing a separate case. |

### drop-partition suite (8 cases)

All 8 cases exercise StarRocks-only partition features that have no
direct Iceberg analog:

- **RECOVER PARTITION** (`drop_recover_*`, `batch_drop_recover_*`) —
  Iceberg has no partition recover; metadata-level partition spec
  evolution doesn't soft-delete partitions.
- **`DROP PARTITIONS WHERE expr`** (`drop_partition_*_with_where`,
  `partition_retention_condition_expr`) — Iceberg drops partition
  *specs* via ALTER PARTITION DROP / SET, not by predicate over
  individual partition values.
- **`START/END/EVERY`** batch syntax — StarRocks DDL extension.
- **Shadow partition** (`shadow_partition_not_dropped`) — StarRocks
  auto-partition has a `$shadow_automatic_partition` placeholder
  partition; Iceberg has no equivalent.
- **LIST partition individual DROP** (`drop_recover_list_partition`) —
  Iceberg LIST partitions evolve via spec changes, not by dropping
  individual VALUES IN buckets.

Iceberg partition evolution coverage already lives in `iceberg-ddl/`
(`partition_evolution_basic`, `partition_evolution_replace`,
`partition_evolution_unsupported`) and `iceberg-dml/`
(`partition_evolution_delete`, `partition_evolution_v3_delete`) per PR #173.
None of the 8 drop-partition cases adds a test point not already covered
by these.

## 3. What this PR does

In the same branch as PR #173 (`claude/iceberg-suite-consolidation`):

1. Confirm no external code references `"delete"` or `"drop-partition"`
   as suite names.
2. `git rm -r sql-tests/delete sql-tests/drop-partition`.
3. Confirm `sql-tests --suite delete --mode verify` and
   `sql-tests --suite drop-partition --mode verify` both error with
   `unknown suite`.
4. Commit.

## 4. Verification

- `cargo build` — clean (no engine changes).
- `cargo run --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests -- --suite delete --mode verify` → `unknown suite 'delete'`.
- Same for `drop-partition`.
- Existing iceberg-* suites untouched and still all-green (will re-verify in the rolled-up PR).
