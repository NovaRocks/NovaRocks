# Materialized Views on Iceberg - Phase 2 Design

**Status:** Draft for user review
**Date:** 2026-04-24
**Builds on:** `docs/superpowers/specs/2026-04-23-mv-on-iceberg-phase1-design.md`

## 1. Goal

Phase 2 adds a narrow, restart-safe incremental refresh path for
standalone materialized views over Iceberg. The supported case is:

- one Iceberg base table;
- append-only base-table changes;
- MV SELECT shape limited to projection plus optional filter;
- manual `REFRESH MATERIALIZED VIEW` only;
- MV physical data stored in the existing managed-lake table created
  by Phase 1.

The main behavioral change is that a second and later refresh can append
only newly added Iceberg rows into the MV's active partition instead of
running Phase 1's full partition-swap refresh.

## 2. Non-goals

Phase 2 does not cover:

- aggregation MVs (`GROUP BY`, aggregate functions, `DISTINCT`);
- join MVs or multi-base-table MVs;
- deletes, equality deletes, position deletes, overwrite snapshots, or
  schema evolution between refreshes;
- query rewrite;
- async or scheduled refresh;
- `PARTITION BY`, partition-level refresh, or partition-level
  staleness;
- MVs that depend on another MV;
- FE-driven StarRocks MV paths.

These stay in later phases. In particular, incrementally mergeable
aggregation belongs to Phase 3.

## 3. User-visible Semantics

`REFRESH MATERIALIZED VIEW mv` keeps the same SQL surface. The engine
chooses the refresh strategy from stored MV metadata and the current
Iceberg snapshot state:

| Condition | Behavior |
|---|---|
| Supported MV has no stored base snapshot | Run Phase 1 full refresh. |
| Current Iceberg snapshot equals stored snapshot | No-op refresh; update refresh time, keep row count unchanged. |
| Current snapshot is an append-only descendant of stored snapshot | Incremental refresh; append only newly added base rows to the MV. |
| Snapshot lineage is missing, rewritten, deleted, or schema-incompatible | Fail fast with an explicit unsupported-incremental-refresh error. |

There is no automatic fallback from an unsafe incremental state to full
refresh after the first completed refresh. Silent fallback would hide
delete/overwrite semantics and can make stale rows look correct. The
first refresh is the only automatic full refresh because there is no
previous snapshot to diff against.

`SHOW MATERIALIZED VIEWS` keeps the Phase 1 columns. `LastRefreshRows`
continues to mean the MV row count after the last successful refresh:
for incremental append, it is previous row count plus appended row
count; for no-op refresh, it is unchanged.

## 4. Supported MV Shape

Phase 2 supports only SELECTs that can be evaluated independently on the
newly appended Iceberg rows:

```sql
SELECT <column refs and deterministic scalar expressions>
FROM iceberg_catalog.namespace.table
[WHERE <deterministic predicate over base-table columns>]
```

Rejected SELECT features:

- more than one table reference;
- joins, subqueries, CTEs, set operations;
- `GROUP BY`, `HAVING`, aggregate functions, `DISTINCT`;
- window functions;
- `ORDER BY`, `LIMIT`, `OFFSET`;
- non-deterministic functions such as `now()`, `random()`, or functions
  that depend on session state not captured in the MV definition.

The classifier runs at CREATE time and REFRESH time. New
`CREATE MATERIALIZED VIEW` statements fail if the SELECT is outside this
shape. REFRESH also revalidates the stored SELECT because MV rows created
before Phase 2 may already exist on disk. If a stored MV is outside this
shape, REFRESH fails with a message saying Phase 2 incremental refresh
supports only projection/filter MVs.

This avoids pretending aggregation is incrementally correct. Phase 1's
full-refresh implementation remains available as an internal primitive,
but Phase 2's user-facing MV feature only accepts the projection/filter
subset.

## 5. Architecture

Phase 2 adds three small pieces around the existing Phase 1 pipeline.

```text
REFRESH MATERIALIZED VIEW
  |
  v
mv_refresh.rs
  |- load MV metadata and active partition
  |- classify stored SELECT
  |- load current Iceberg snapshot
  |- decide full / no-op / incremental / unsupported
  |
  +-- full ----------> Phase 1 refresh_mv_full
  |
  +-- no-op ---------> store.update_mv_refresh_metadata(...)
  |
  `-- incremental ---> iceberg::plan_append_delta(...)
                       engine registers base table with only delta files
                       execute stored SELECT over delta table
                       txn writes chunks into active partition
                       store.update_mv_refresh_metadata(...)
```

No SQLite schema bump is required. Phase 1 already stores:

- `base_table_refs_json`;
- `last_refresh_snapshots_json`;
- `last_refresh_ms`;
- `last_refresh_rows`.

Phase 2 consumes those fields and adds store helpers for metadata-only
refresh updates. The managed-lake partition and txn tables stay
unchanged.

## 6. Iceberg Delta Planning

Add a helper in `src/standalone/iceberg/registry.rs`:

```rust
pub(crate) struct IcebergAppendDelta {
    pub previous_snapshot_id: i64,
    pub current_snapshot_id: i64,
    pub added_files: Vec<(String, i64, Option<i64>)>,
}

pub(crate) fn plan_append_delta(
    table: &iceberg::table::Table,
    previous_snapshot_id: i64,
) -> Result<IcebergAppendDelta, String>;
```

The helper:

1. reads the current snapshot;
2. returns an empty delta if current snapshot id equals the previous id;
3. walks the snapshot parent chain from current back to previous;
4. rejects if previous is not an ancestor;
5. rejects any snapshot in the chain whose operation is not append;
6. loads each delta snapshot's manifest list;
7. rejects delete manifests and deleted entries;
8. collects data-file entries added by delta snapshots.

The output file tuple matches the existing
`extract_data_files()` storage registration path.

If the current table has no snapshot, refresh is treated as no-op only
when the stored previous snapshot is also absent. Once an MV has a stored
snapshot, losing the snapshot lineage is an error.

## 7. Executing the Delta Query

The existing Phase 1 refresh path registers an Iceberg base table in the
local standalone catalog by extracting all data files. Phase 2 adds an
internal variant that registers the same logical table using only the
delta files:

```rust
execute_query_for_mv_incremental_refresh(
    state,
    current_database,
    select_sql,
    base_ref,
    delta_files,
)
```

The query text remains unchanged. For a stored query such as:

```sql
SELECT k1, v2 FROM ice.ns.orders WHERE v2 > 10
```

the execution helper registers `ns.orders` with only the newly appended
Iceberg files, strips the catalog prefix as Phase 1 does, and runs the
normal query executor. Projection and filter semantics are therefore
identical to full refresh; only the base file set changes.

For S3-backed Iceberg tables, the helper uses
`TableStorage::S3ParquetFiles` with the delta files and the existing
cloud properties. For local Iceberg tables, Phase 2 supports the same
single-file local path behavior that Phase 1 currently has; multi-file
local delta support can be added later if needed.

## 8. Writing the Delta Into the MV

Phase 1 already generalized managed-lake writes through:

- `PartitionTarget::Active`;
- `PartitionTarget::Staged`;
- `write_chunks_into_managed_partition`.

Incremental refresh uses `PartitionTarget::Active` for the MV table and
writes the delta chunks as a normal lake transaction. It must not create
a new partition and must not enqueue erase jobs.

After the write succeeds:

1. collect the current Iceberg snapshot id;
2. compute `new_last_refresh_rows = old_last_refresh_rows + rows_written`;
3. update `materialized_views.last_refresh_ms`;
4. update `materialized_views.last_refresh_rows`;
5. update `last_refresh_snapshots_json`.

If the delta contains zero rows but the snapshot advanced through an
append snapshot with empty files, the metadata update still advances the
stored snapshot id so future refreshes do not revisit the same snapshot.

## 9. Error Handling and Concurrency

Phase 2 preserves Phase 1's single-refresh-at-a-time rule. Before
incremental write starts, REFRESH still checks that no `CREATING`
partition exists under the MV. Incremental refresh does not create one,
but this check prevents overlap with a full refresh or failed staged
refresh residue.

Failure handling:

| Failure point | Handling |
|---|---|
| Delta planning fails | No data written; return explicit error. |
| Query over delta files fails | No data written; return explicit error. |
| Managed-lake write fails before visible commit | Existing txn abort path applies. |
| Metadata update fails after successful write | Not allowed as a separate committed state. The store helper must mark the write visible and update MV refresh metadata in one SQLite transaction. |
| Process crash during incremental write | Existing managed-lake txn recovery replays or aborts the write, same as INSERT. |

Phase 2 therefore adds a store-level helper that can mark the active
partition write visible and update MV refresh metadata in one SQLite
transaction. A two-step write-visible-then-metadata-update path is out
of scope because it can duplicate appended rows on retry.

## 10. Testing

Unit tests:

- classify projection/filter MV as incrementally supported;
- reject aggregate, join, distinct, window, limit, and multi-table
  definitions;
- `plan_append_delta` returns no-op for same snapshot;
- `plan_append_delta` collects only files added after the stored
  snapshot;
- `plan_append_delta` rejects missing ancestor, delete manifests, and
  non-append snapshot operations;
- store helper updates last refresh metadata without partition swap.

Integration tests in `tests/standalone_mysql_server.rs`:

- first refresh full, second refresh no-op keeps rows stable;
- append to Iceberg base, refresh appends only new rows into MV;
- projection/filter MV applies filter only to delta rows;
- unsupported aggregate MV create fails with the expected unsupported
  incremental message;
- lineage/overwrite/delete simulation fails fast; if the SQL fixture
  cannot create that Iceberg state, cover it with a registry-level unit
  fixture.

SQL-test case:

- extend or add a write-path case that creates a projection/filter MV,
  refreshes once, inserts a new Iceberg row, refreshes again, and checks
  the MV contains old plus new rows without requiring a full partition
  swap.

## 11. Rollout Plan

1. Add MV SELECT shape classifier and tests.
2. Add Iceberg append-delta planner and tests.
3. Add delta-file query execution helper.
4. Add active-partition incremental write path with metadata update.
5. Wire refresh strategy selection in `mv_refresh.rs`.
6. Add integration and SQL-test coverage.

Phase 2 is complete when projection/filter MV refreshes use the
incremental path for append-only Iceberg snapshots, unsafe snapshot
changes fail explicitly, and the existing Phase 1 parser/lifecycle tests
plus the first-full-refresh path for supported MVs continue to pass.
