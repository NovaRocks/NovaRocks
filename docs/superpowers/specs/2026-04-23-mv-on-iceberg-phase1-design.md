# Materialized Views on Iceberg — Phase 1 Design

**Status:** Draft (for review before implementation plan)
**Date:** 2026-04-23
**Supersedes:** none
**Related:**
- `docs/superpowers/specs/2026-04-21-standalone-managed-lake-lifecycle-design.md`
- `docs/superpowers/specs/2026-04-22-delete-local-tables-design.md`

## 1. Goal

Deliver a working `MATERIALIZED VIEW` surface in the standalone engine
where:

- The MV definition's base tables are Iceberg tables (already registered
  through the existing `IcebergCatalogRegistry`).
- The MV itself is materialized as a kind-tagged managed lake table.
- Users query the MV directly by name (`SELECT ... FROM mv`) like any
  other table. **There is no transparent query rewrite.**
- `REFRESH MATERIALIZED VIEW` performs a synchronous, full-refresh
  partition swap.
- Metadata and physical data survive process restart via the existing
  managed-lake lifecycle machinery.

The explicit non-goal for Phase 1 is incremental refresh; the design
persists the hooks required by Phase 2 but does not consume them.

## 2. Why now

- Managed-lake lifecycle (PR #50 → #54) gave us a reliable, crash-safe
  `stage → execute → activate` topology for `TRUNCATE`. A full-refresh
  MV is topologically identical: allocate a new partition, populate it,
  atomically swap, erase the old one.
- The existing `src/standalone/engine/sqlparse/materialized_view.rs`
  stub is a text-tokenizing placeholder with a hardcoded string match
  for one TPC-like test query. It persists nothing and cannot survive
  a restart. It needs to be replaced, not extended.
- NovaRocks users are starting to ask for MV over Iceberg workloads.
  Phase 1 delivers the catalog, storage, and refresh skeleton that
  every subsequent MV phase builds on.

## 3. Non-goals (Phase 1)

- **No incremental refresh.** Deferred to Phase 2.
- **No query rewrite.** MVs are queried by name; they do not
  transparently back other queries.
- **No async / scheduled refresh.** `REFRESH DEFERRED MANUAL` only.
  Async/immediate modes are parser-rejected with an explicit error.
- **No `PARTITION BY` or `ORDER BY` on the MV.** Rejected at parse
  time. MV physical layout follows the managed-lake single-active-
  partition model.
- **No MVs on non-Iceberg base tables.** Base tables must live in an
  Iceberg catalog already registered through
  `IcebergCatalogRegistry`.
- **No `DROP MATERIALIZED VIEW ... FORCE`.** Users must wait for the
  in-progress refresh to finish.
- **No `information_schema.materialized_views`.** Introspection in
  Phase 1 is `SHOW MATERIALIZED VIEWS`.
- **No FE-driven MV path.** Standalone only.
- **No cross-MV references.** An MV's SELECT cannot name another MV.

## 4. Architecture

### 4.1 Physical model

An MV is stored as one row in `tables` with `kind = 'MATERIALIZED_VIEW'`
plus one row in `materialized_views` with the MV-specific metadata.
The underlying `partitions / indexes / tablets / erase_jobs` lifecycle
tables are reused without change.

```
sqlparser dialect (StarRocks-flavored)
  └─> standalone/engine/sqlparse/materialized_view.rs   [replaced]
        └─> standalone/engine/mod.rs dispatch
              ├─ CREATE   → lake/mv_ddl.rs::create_mv
              ├─ DROP     → lake/mv_ddl.rs::drop_mv
              ├─ REFRESH  → lake/mv_refresh.rs::refresh_mv_full
              └─ SHOW     → lake/mv_ddl.rs::list_mvs
                    │
                    ├─ metadata: lake/store.rs (new rows in
                    │             materialized_views + tables.kind)
                    └─ physical: reuse managed-lake stage/activate
                                  + erase worker (lake/erase.rs)
                       data path: reuse query executor (planner +
                                  pipeline) with a sink that targets
                                  a specific partition_id
```

### 4.2 Lifetime of a REFRESH

Identical topology to `TRUNCATE`:

1. **stage** — SQLite transaction allocates a new `CREATING` partition,
   a new `CREATING` index, and a bucket-sized set of `tablets` rows.
2. **bootstrap** — Per-tablet empty lake-format tablet metadata is
   written to object storage (reuses `bootstrap_empty_partition`).
3. **execute** — Parse and plan the stored SELECT; run it through the
   query executor with the managed-lake INSERT sink targeting the
   newly staged `partition_id`. On success, capture the row count and
   the per-base-table Iceberg snapshot IDs.
4. **activate** — SQLite transaction atomically flips the new partition
   to `ACTIVE`, the old one to `RETIRED`, enqueues a `DROP_PARTITION`
   erase job for the old partition root, and updates
   `materialized_views.last_refresh_*`.
5. **erase (async)** — The existing erase worker deletes the old
   partition's object-store data.

Readers holding a SELECT plan against the MV always see the current
`ACTIVE` partition; the new partition is invisible until activation,
so refresh is transparent to concurrent readers.

## 5. Data model

### 5.1 SQLite schema changes

The managed-lake SQLite schema moves from `user_version = 3` to
`user_version = 4`. Per existing convention, there is no migration —
opening a `user_version = 3` database fails fast with a message asking
the operator to delete the DB and reopen. Because old databases are
never upgraded in place, the changes below are applied to
`init_schema()`'s `CREATE TABLE` statements for fresh databases; no
`ALTER TABLE` statements run at runtime.

```sql
-- tables (existing) gains a kind column
CREATE TABLE IF NOT EXISTS tables (
    table_id INTEGER PRIMARY KEY,
    db_id INTEGER NOT NULL,
    name TEXT NOT NULL,
    keys_type TEXT NOT NULL,
    bucket_num INTEGER NOT NULL,
    current_schema_id INTEGER NOT NULL,
    state TEXT NOT NULL,
    kind TEXT NOT NULL DEFAULT 'TABLE'
        CHECK (kind IN ('TABLE', 'MATERIALIZED_VIEW')),
    UNIQUE(db_id, name)
);

-- materialized_views is new
CREATE TABLE IF NOT EXISTS materialized_views (
    mv_id INTEGER PRIMARY KEY REFERENCES tables(table_id),
    select_sql TEXT NOT NULL,
    refresh_mode TEXT NOT NULL DEFAULT 'DEFERRED_MANUAL'
      CHECK (refresh_mode IN ('DEFERRED_MANUAL')),
    base_table_refs_json TEXT NOT NULL,
    last_refresh_ms INTEGER,
    last_refresh_rows INTEGER,
    last_refresh_snapshots_json TEXT,
    created_at_ms INTEGER NOT NULL
);
```

Field semantics:

- `mv_id` is identical to the `tables.table_id` of the underlying
  `kind='MATERIALIZED_VIEW'` row (1:1 binding, FK).
- `select_sql` stores the user-provided SELECT text (minimally
  normalized: trimmed, internal whitespace runs collapsed). Shown by
  `SHOW MATERIALIZED VIEWS`; re-parsed at every REFRESH.
- `refresh_mode` is enumerated so Phase 2 / Phase 3 can extend the
  CHECK constraint.
- `base_table_refs_json` is populated at CREATE time with the set of
  Iceberg tables the SELECT touches, e.g.
  `[{"catalog":"iceberg_cat","namespace":"ns","table":"orders"}]`.
  Phase 1 writes this field; it is not consumed until Phase 2.
- `last_refresh_snapshots_json` is `{"iceberg_cat.ns.orders": 7391...}`
  after each REFRESH; again, Phase 1 writes, Phase 2 reads.
- `last_refresh_ms` / `last_refresh_rows` drive the `SHOW
  MATERIALIZED VIEWS` output and serve as a coarse staleness signal.

MV lifecycle state (`ACTIVE` / `DROPPING` / `FAILED`) is stored in
`tables.state` exactly like a normal managed lake table. There is no
separate MV state column; "is the MV alive" is answered by reading the
`tables` row.

### 5.2 Rust types (in `src/standalone/lake/store.rs`)

```rust
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum ManagedTableKind {
    Table,
    MaterializedView,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum ManagedMvRefreshMode {
    DeferredManual,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct IcebergTableRef {
    pub catalog: String,
    pub namespace: String,
    pub table: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct StoredMaterializedView {
    pub mv_id: i64,
    pub select_sql: String,
    pub refresh_mode: ManagedMvRefreshMode,
    pub base_table_refs: Vec<IcebergTableRef>,
    pub last_refresh_ms: Option<i64>,
    pub last_refresh_rows: Option<i64>,
    pub last_refresh_snapshots: BTreeMap<String, i64>,
    pub created_at_ms: i64,
}
```

`StoredManagedTable` gains a `kind: ManagedTableKind` field; all
read/write paths in `store.rs`, `catalog.rs`, and `ddl.rs` thread it
through. `ManagedSnapshot` gains a `materialized_views:
Vec<StoredMaterializedView>` field, persisted and reloaded as part of
the normal snapshot round trip.

## 6. DDL surface

### 6.1 Parser integration

The existing custom dialect at `src/sql/parser/dialect/` already parses
`CREATE TABLE ... DISTRIBUTED BY HASH(...) BUCKETS n PROPERTIES(...)`.
Phase 1 extends the same dialect with four new AST statements:

- `CreateMaterializedView`
- `DropMaterializedView`
- `RefreshMaterializedView`
- `ShowMaterializedViews`

The text-tokenizing helpers in
`src/standalone/engine/sqlparse/materialized_view.rs` are removed.
Engine-level dispatch switches on the new AST variants.

### 6.2 Accepted grammar

```sql
CREATE MATERIALIZED VIEW [IF NOT EXISTS] [db.]mv_name
  [COMMENT 'string']
  DISTRIBUTED BY HASH(col_list) BUCKETS n
  [REFRESH DEFERRED MANUAL]
  [PROPERTIES ('k' = 'v', ...)]
AS
  <select_query>;

REFRESH MATERIALIZED VIEW [db.]mv_name;

DROP MATERIALIZED VIEW [IF EXISTS] [db.]mv_name;

SHOW MATERIALIZED VIEWS [FROM db];
```

Clause handling:

| Clause                                   | Phase 1 treatment                        |
|------------------------------------------|------------------------------------------|
| `IF NOT EXISTS` / `IF EXISTS`            | Honored.                                 |
| `[db.]` prefix                           | Honored; defaults to session current db. |
| `COMMENT`                                | Parsed and dropped.                      |
| `DISTRIBUTED BY HASH(...) BUCKETS n`     | Required. No defaults.                   |
| `REFRESH DEFERRED MANUAL`                | Optional; absence is equivalent.         |
| `REFRESH IMMEDIATE`                      | Parser rejects: "not supported".         |
| `REFRESH ASYNC [START ... EVERY ...]`    | Parser rejects: "not supported".         |
| `PARTITION BY ...`                       | Parser rejects: "not supported yet".     |
| `ORDER BY (cols)`                        | Parser rejects: "not supported yet".     |
| `PROPERTIES(...)`                        | Parsed and dropped.                      |
| `REFRESH ... WITH {SYNC\|ASYNC} MODE`    | Parser rejects.                          |
| `REFRESH ... PARTITION START(...) END(...)` | Parser rejects.                       |
| `SHOW MATERIALIZED VIEWS LIKE '...'`     | Parser rejects.                          |
| `SHOW MATERIALIZED VIEWS WHERE ...`      | Parser rejects.                          |
| `DROP MATERIALIZED VIEW ... FORCE`       | Parser rejects.                          |

### 6.3 Execution-side validation (engine layer, not parser)

At CREATE time, after the SELECT has been analyzed:

1. The SELECT must produce a schema (names + types); that schema becomes
   the MV's physical schema.
2. Every base table referenced by the SELECT must resolve to an Iceberg
   table in a registered catalog. Encountering a managed-lake table,
   view, MV, or system table yields
   `MV base tables must be Iceberg tables`.
3. Every column named in `DISTRIBUTED BY HASH(...)` must exist in the
   SELECT output schema (case-insensitive name match).
4. The MV name must not collide with an existing `tables` row in the
   same database (the `UNIQUE(db_id, name)` index already enforces this).

### 6.4 `SHOW MATERIALIZED VIEWS` output

Fixed columns:

```
Name | Database | RefreshMode | LastRefreshTime | LastRefreshRows
     | BaseTables | SelectText
```

`BaseTables` renders `base_table_refs_json` as
`["iceberg_cat.ns.orders", ...]`. `SHOW TABLES` is filtered with
`kind = 'TABLE'` so MVs do not appear there.

## 7. Data flow

### 7.1 `CREATE MATERIALIZED VIEW`

```
parse + analyze:
  ├─ parse dialect → CreateMaterializedView AST
  ├─ analyze AS <select> → output schema, base_table_refs
  └─ execution-side validation (§6.3)

one sqlite txn:
  ├─ allocate table_id / schema_id / partition_id (= p0) / index_id
  │                          / tablet_ids from global_meta
  ├─ INSERT tables(kind='MATERIALIZED_VIEW', state='ACTIVE',
  │                bucket_num, current_schema_id, ...)
  ├─ INSERT schemas + columns derived from SELECT output schema
  ├─ INSERT partitions(p0, state='ACTIVE',
  │                    visible_version=1, next_version=2)
  ├─ INSERT indexes(index_0, state='ACTIVE')
  ├─ INSERT tablets(0..bucket_num) with tablet_root_path =
  │         "<warehouse>/db_<id>/table_<id>/partition_<p0>"
  ├─ INSERT materialized_views(mv_id=table_id, select_sql,
  │                            refresh_mode, base_table_refs_json,
  │                            created_at_ms)
  └─ commit

bootstrap object store:
  └─ bootstrap_empty_partition(runtime, staged, managed_config)
     for each tablet in p0
```

After CREATE completes, `p0` is `ACTIVE` but empty. `SELECT * FROM mv`
returns 0 rows until the first REFRESH. This aligns with the
`REFRESH DEFERRED MANUAL` semantics.

### 7.2 `REFRESH MATERIALIZED VIEW`

```
stage phase (sqlite txn):
  ├─ SELECT tables WHERE table_id = mv.mv_id
  │                  AND kind = 'MATERIALIZED_VIEW'
  │                  AND state = 'ACTIVE'
  │    else → error "MV is not active"
  ├─ Reject concurrent refresh: if a partition with
  │   state='CREATING' already exists under this table,
  │   → error "refresh already in progress"
  ├─ alloc new partition_id (p_new), index_id, tablet_ids
  ├─ INSERT partitions(p_new, state='CREATING')
  ├─ INSERT indexes(index_new, state='CREATING')
  ├─ INSERT tablets for p_new's buckets
  └─ commit; return a StagedManagedMvRefresh handle

bootstrap object store:
  └─ bootstrap_empty_partition for p_new's tablets

execute phase (not in a sqlite txn):
  ├─ re-parse materialized_views.select_sql
  ├─ plan the SELECT; invoke the managed-lake INSERT pipeline
  │   programmatically with target_partition_id = p_new (not a
  │   user-facing SQL syntax; an internal plan parameter, see §7.4)
  ├─ run through the pipeline executor; sink writes to p_new
  └─ on success:
       - capture rows_written from sink metrics
       - for each base table in base_table_refs, load its current
         Iceberg snapshot_id → snapshots_map

activate phase (sqlite txn):
  ├─ identify current ACTIVE partition p_old
  ├─ UPDATE partitions SET state = CASE
  │       WHEN partition_id = p_new THEN 'ACTIVE'
  │       WHEN partition_id = p_old THEN 'RETIRED'
  │     END
  │   WHERE table_id = mv.mv_id
  ├─ UPDATE indexes analogously
  ├─ INSERT erase_jobs(job_kind='DROP_PARTITION',
  │                     partition_id=p_old,
  │                     root_path=p_old.root)
  ├─ UPDATE materialized_views SET
  │     last_refresh_ms = strftime('%s','now') * 1000,
  │     last_refresh_rows = ?rows_written,
  │     last_refresh_snapshots_json = ?snapshots_map
  └─ commit

post-commit:
  ├─ ManagedLakeCatalog::rebuild (existing path)
  └─ session catalog refresh so the next SELECT sees the new ACTIVE
     partition
```

### 7.3 `DROP MATERIALIZED VIEW`

```
resolve mv_name → table_id
reuse drop_managed_table(table_id, table_root) with one new check:
  - reject if any CREATING partition exists under this table
    (covers "REFRESH in progress" races)

sqlite txn:
  - UPDATE tables SET state='DROPPING'
  - UPDATE partitions / indexes → 'RETIRED'
  - INSERT erase_jobs(DROP_TABLE, root=table_root)
commit

async (erase worker):
  - erase object store under root
  - purge_retired_table_metadata(table_id):
      DELETE FROM materialized_views WHERE mv_id = ?
      DELETE FROM tablets / indexes / partitions / tables …
```

### 7.4 New executor capability

The managed-lake INSERT sink currently targets whichever partition is
`ACTIVE`. Phase 1 extends it with an optional
`target_partition_id: Option<i64>`; when set, the sink resolves that
partition's tablets instead of the active one. This capability is
also the foundation for Phase 2 incremental refresh (which writes
into the `ACTIVE` partition as an additional lake transaction).

## 8. Concurrency and recovery

### 8.1 Concurrency

- **Two REFRESHes on the same MV** — the second one's `stage` txn
  observes the first REFRESH's `CREATING` partition and errors out.
- **REFRESH and DROP** — DROP's augmented check rejects while a
  `CREATING` partition is in flight; once DROP wins, the subsequent
  REFRESH finds `tables.state != 'ACTIVE'` and errors out.
- **SELECT during REFRESH** — SELECT binds to the current `ACTIVE`
  partition; the `CREATING` partition is invisible to the catalog
  read path until `rebuild` runs after activation.
- **Process-wide locking** — not needed. SQLite's transaction
  serialization and the lifecycle state machine are sufficient.

### 8.2 Failure handling

| Failure point | Handling |
|---|---|
| stage sqlite txn fails | No residue; REFRESH returns the error. |
| bootstrap fails | `store.delete_creating_partition(p_new)`; REFRESH returns the error. |
| executor fails | `store.delete_creating_partition(p_new)`; enqueue an erase job for `p_new`'s root; REFRESH returns the error. |
| activate sqlite txn fails | Same as executor failure. |
| Process crashes mid-execute | Next `reconcile_on_open` observes the dangling `CREATING` partition under the MV and deletes it via the existing cleanup path. The ACTIVE partition is untouched. |
| CREATE object-store bootstrap fails after sqlite commit | Mark `tables.state = 'FAILED'` and `p0.state = 'FAILED'`; CREATE returns the error; reconcile / erase cleans up on next open. |

### 8.3 Restart recovery

No new recovery machinery. The MV table's `CREATING` partitions are
handled by the existing `reconcile_on_open`
(`src/standalone/lake/catalog.rs`). Because MV lifecycle state lives
in `tables.state`, a `DROPPING` MV with pending erase jobs resumes
through the erase worker automatically.

## 9. Testing

### 9.1 Library unit tests (`src/standalone/lake/*`)

`store.rs`:

- `managed_mv_schema_round_trips`
- `managed_mv_kind_column_defaults_to_table`
- `drop_managed_mv_purges_materialized_views_row`
- `drop_managed_mv_rejects_inflight_creating_partition`
- `stage_mv_refresh_rejects_when_refresh_in_progress`
- `activate_mv_refresh_swaps_partition_and_updates_last_refresh_fields`

`catalog.rs`:

- `rebuild_treats_mv_as_table_kind_and_registers_in_catalog`
- `reconcile_on_open_drops_incomplete_mv_refresh_partition`

`mv_ddl.rs` / `mv_refresh.rs` (new files):

- `create_mv_rejects_non_iceberg_base_tables`
- `create_mv_rejects_partition_by_and_order_by`
- `create_mv_populates_materialized_views_row_with_base_refs`
- `refresh_mv_cleans_staged_partition_on_executor_error`

### 9.2 Parser tests (`src/sql/parser/dialect/`)

- `parse_create_mv_accepts_distributed_by_and_refresh_deferred_manual`
- `parse_create_mv_records_but_ignores_comment_and_properties`
- `parse_create_mv_rejects_partition_by`
- `parse_create_mv_rejects_refresh_async`
- `parse_create_mv_rejects_refresh_immediate`
- `parse_refresh_mv`
- `parse_refresh_mv_rejects_partition_range`
- `parse_refresh_mv_rejects_with_async_mode`
- `parse_drop_mv_with_if_exists`
- `parse_show_materialized_views_with_from_db`
- `parse_show_materialized_views_rejects_like_and_where`

### 9.3 MySQL-protocol integration tests (`tests/standalone_mysql_server.rs`)

Building on the existing managed-lake + Iceberg harness:

- `standalone_mysql_server_mv_create_and_manual_refresh_round_trip` —
  the happy path (CREATE, initial 0-row SELECT, REFRESH, populated
  SELECT, repeat REFRESH after new base-table rows, DROP, post-drop
  error).
- `standalone_mysql_server_mv_show_output_matches_expected_columns`.
- `standalone_mysql_server_mv_create_rejects_non_iceberg_base_table`.
- `standalone_mysql_server_mv_reopen_recovers_after_crashed_refresh` —
  inject a dangling `CREATING` partition + matching object-store
  residue, reopen the engine, assert the MV still serves baseline
  data and the residue is reconciled away.

### 9.4 SQL-tests suite

Add one happy-path case under the existing managed-lake suite
(`tests/sql-test-runner/conf/standalone_managed_lake.*`) covering
CREATE / REFRESH / SELECT / DROP. Recorded in `record` mode; used for
regression protection, not primary functional verification.

### 9.5 Out of test scope (deferred to later phases)

- Incremental refresh correctness.
- Multi-base-table snapshot consistency.
- Async / scheduled refresh.
- MVs depending on other MVs.
- `DROP MATERIALIZED VIEW ... FORCE` (not supported).

## 10. Phase roadmap (context; not part of Phase 1 delivery)

- **Phase 2** — Append-only incremental refresh. Consume
  `last_refresh_snapshots_json`; compute per-base-table snapshot deltas
  via Iceberg's incremental-append read path; append deltas into the
  active partition via a new lake transaction. Covers
  projection/filter MVs first.
- **Phase 3** — Incrementally-mergeable aggregation MVs (`sum`,
  `count`, `min`, `max`) with either append-and-rollup reads or a
  merge-on-read primitive.
- **Phase 4** — Async / scheduled refresh via a dedicated worker
  (pattern mirrors `lake/erase.rs`); multi-base-table snapshot
  consistency on refresh start.
- **Phase 5** — Join MVs, upsert/delete base-table support,
  partition-level staleness, optional query rewrite.

## 11. Files touched in Phase 1

New:
- `src/standalone/lake/mv_ddl.rs`
- `src/standalone/lake/mv_refresh.rs`

Modified:
- `src/sql/parser/dialect/mod.rs` (or a new `mv.rs` submodule under it)
- `src/standalone/lake/store.rs` (schema bump, new types, transactions)
- `src/standalone/lake/catalog.rs` (rebuild threads `kind`)
- `src/standalone/lake/ddl.rs` (DROP path augmented with the
  CREATING-partition rejection)
- `src/standalone/engine/mod.rs` (dispatch on the new AST variants)
- `src/standalone/engine/sqlparse/materialized_view.rs` (deleted; its
  text-tokenizing helpers are entirely replaced by real dialect
  parsing)
- Managed-lake INSERT sink (target partition override) — exact file
  identified during plan writing.
- `tests/standalone_mysql_server.rs`
- `tests/sql-test-runner/conf/standalone_managed_lake.*` (new case)

## 12. Open questions deferred to the implementation plan

- Exact file for the managed-lake INSERT sink override (sink lives
  downstream of planner; will be pinpointed during plan writing, not
  redesigned here).
- Whether `base_table_refs_json` captures purely user-spelled FQNs or
  analyzer-resolved FQNs (recommendation: analyzer-resolved, so alias
  and case normalization are stable across Phase 2).
- Minimum hardening of `select_sql` normalization beyond whitespace
  collapse (e.g., remove trailing semicolon). Cosmetic; settled during
  implementation.
