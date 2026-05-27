# StarRocks `ScanSource` carries table identity (lock-free dict owner lookup)

Date: 2026-05-28
Status: design

## Background

Parallel runs of any SQL suite that exercises StarRocks managed-lake tables (filter,
join, sort, cte, …) hang within seconds and most cases time out at the 120s mysql
client query_timeout. With `-j 1` (sequential), filter passes 15/15 in 3.4s.

The contended lock is `state.starrocks_table: RwLock<StarRocksTableCatalog>`.

The codebase already documents that `std::sync::RwLock` on this project is FIFO-queue
and prone to writer starvation when long readers run in the pipeline path
(`src/engine/mod.rs:821-832`):

> Pipeline execution can run for many seconds and would otherwise starve writers
> (e.g. INSERT cleanup taking `state.catalog.write()` in `invalidate_iceberg_caches`)
> on the std::sync::RwLock writer queue.

The catalog side already mitigates this via a **clone-then-release** pattern.

PR #191 (low-cardinality dictionary rewrite, landed `3f9843f0` / `82744a01`) added a
new hot reader on the **other** lock (`state.starrocks_table`) and did not follow
that convention:

- `DictionaryQueryProvider::owner_for` (`src/engine/dictionary/mod.rs:307-313`) takes
  `state.starrocks_table.read()` on every Scan column of every SELECT, in order to
  pull `runtime.table.db_id` / `runtime.table.table_id`.
- Iceberg's branch of the same function (`src/engine/dictionary/mod.rs:325-331`) is
  **already lock-free** — it reads `ScanSource::IcebergDataFiles { table: info, .. }`
  directly from the plan node.

Under default parallelism (`-j 8`), 8 sql-test workers concurrently issue DROP
DATABASE FORCE + CREATE + INSERT + SELECT cycles. INSERT writers hold
`starrocks_table.write()` across SQLite txns and S3/MinIO IO; DROP DATABASE writers
hold it across SQLite txn + reload + rebuild; every SELECT scan column queues a new
`starrocks_table.read()` request. With FIFO queueing, readers stop coalescing and
the queue grows unbounded. Combined with client-side-only 120s timeouts (the
server-side wait isn't cancelled), throughput collapses to zero within minutes.

`PhysicalTableLayout { db_id, table_id, schema_id, tablets }` is already stored
alongside `TableDef` in `InMemoryCatalog` when StarRocks tables are registered
(`src/engine/catalog.rs:106-121`). So the identity the dict provider needs is
already plan-time data — it does not require a runtime catalog lookup.

## One-line fix

Make `ScanSource::StarRocks` carry `db_id` and `table_id` (mirroring how
`ScanSource::IcebergDataFiles` carries `IcebergTableInfo`), and read those from the
plan node in `DictionaryQueryProvider::owner_for`. The hot reader on
`state.starrocks_table` goes to zero.

## Scope

### In scope

- Change `ScanSource::StarRocks` from a unit variant to
  `ScanSource::StarRocks { db_id: i64, table_id: i64 }`.
- Update the **4 production construction sites** to populate `db_id` / `table_id`
  from the `StarRocksTableRuntime` already in scope.
- Update the **single dict-provider consumer** (`owner_for`) to read identity from
  the plan node, dropping the `state.starrocks_table.read()`.
- Update the **20+ mechanical match / fixture sites** so the project compiles. Most
  are test fixtures, explain pretty-printers, optimizer rule fixtures, and predicate
  pushdown fixtures — they do not consume the payload, so `{ db_id: 0, table_id: 0 }`
  is acceptable for fixtures that do not flow through the dict subsystem.
- Add a debug-assert in `register_starrocks_table_in_catalog` ensuring
  `TableDef::source` and `PhysicalTableLayout` agree on `(db_id, table_id)`.
- Add a unit test verifying `starrocks_table_def(&runtime)` populates
  `ScanSource::StarRocks { db_id, table_id }` from the runtime.
- Verify the parallel `filter` SQL suite now passes 15/15.

### Out of scope (explicitly deferred)

- Other readers of `state.starrocks_table` outside the optimizer hot path
  (`collect_namespace_owners`, `resolve_owner_from_target`, `mark_target_stale`,
  scan-time table lookup) — low frequency, not hot.
- Shrinking INSERT / DROP writer critical sections (no SQLite txn / S3 IO refactor).
- Switching to `parking_lot::RwLock`.
- Server-side cancellation of long waits on mysql client timeout.
- Other lock primitives (`state.catalog`, `state.iceberg_catalogs`,
  `state.connectors`, `state.statistics`, `tablet_runtime_registry`).
- Any change to `DictionarySnapshot` / `DictionaryOwner` / `DictionaryManager` —
  the data model is unchanged.

## Design

### Enum shape change

```rust
// src/sql/catalog.rs
pub enum ScanSource {
    StarRocks { db_id: i64, table_id: i64 },          // payload added
    IcebergDataFiles { table, files, cloud_properties },
    IcebergMetadataTable { ... },
    IcebergDeltaTable { ... },
}
```

Identity invariant: for any `TableDef { source: ScanSource::StarRocks { db_id, table_id }, .. }`
registered in `InMemoryCatalog`, the same `(db_id, table_id)` appears in the matching
`PhysicalTableLayout` entry. Asserted in `register_starrocks_table` (debug builds).

### Provider consumer

```rust
// src/engine/dictionary/mod.rs::DictionaryQueryProvider::owner_for
match &table.source {
    ScanSource::StarRocks { db_id, table_id } => {
        // No lock; identity comes from the plan node.
        Ok(Some(DictionaryOwner::StarRocksTable {
            database: database.to_string(),
            table: table.name.clone(),
            db_id: *db_id,
            table_id: *table_id,
        }))
    }
    ScanSource::IcebergDataFiles { table: info, .. } => { /* unchanged */ }
    ScanSource::IcebergMetadataTable { .. } | ScanSource::IcebergDeltaTable { .. } => Ok(None),
}
```

The `database` and `table` strings are still taken from the `TableDef` like they
already were — only the integer IDs change source from catalog lock to plan node.

### Production construction sites

The four production sites that materialize `TableDef` for a StarRocks table all
already have a `StarRocksTableRuntime` (or equivalent) in scope and can read
`runtime.table.db_id` / `runtime.table.table_id`:

| Site | Purpose | Source of IDs |
|---|---|---|
| `src/connector/starrocks/table/catalog.rs:621` (`starrocks_table_def`) | main DDL / refresh path | `runtime.table.db_id`, `runtime.table.table_id` |
| `src/connector/starrocks/table/mv_refresh.rs:4562` | MV-refresh catalog rebuild | `runtime.table.db_id`, `runtime.table.table_id` |
| `src/engine/catalog.rs:339` | MV registration helper | `runtime.table.db_id`, `runtime.table.table_id` |
| `src/engine/mod.rs:4070` | embedding API `register_starrocks_table` | parameters from caller; for the embedded test/dev path the synthetic table can use `0, 0` (does not flow through dict rewrite) |

### Mechanical fixture sites

Roughly twenty other construction sites are test fixtures, EXPLAIN pretty-printers,
optimizer-rule fixtures, predicate-pushdown fixtures, and cost-model fixtures. These
do not consume `db_id` / `table_id` because:

- `EXPLAIN` only matches the `ScanSource` shape to print "StarRocks" vs "Iceberg".
- Optimizer rules clone `Scan` nodes; they propagate `source` unchanged.
- Predicate-pushdown and cost fixtures build dummy plans for unit tests; the dict
  rule is not exercised on these dummy plans.

These sites get `ScanSource::StarRocks { db_id: 0, table_id: 0 }`. The exhaustive
list:

```
src/sql/optimizer/convert.rs:338
src/sql/optimizer/mod.rs:395
src/sql/optimizer/logical_props.rs:355
src/sql/optimizer/rewrite/context.rs:286
src/sql/optimizer/rewrite/tree.rs:400
src/sql/optimizer/cte_rewrite.rs:309
src/sql/optimizer/rewrite/rules/predicate_pushdown/push_to_scan.rs:125
src/sql/optimizer/rewrite/rules/predicate_pushdown/push_to_join.rs:500
src/sql/optimizer/cost.rs:207
src/sql/optimizer/cost.rs:288
src/sql/explain.rs:1102
src/sql/explain.rs:1147
src/sql/explain.rs:1188
src/sql/explain.rs:1265
src/sql/explain.rs:1329
```

Match-only sites that need `{ .. }` rest pattern:

```
src/engine/dictionary/rebuild.rs:60
src/engine/dictionary/rebuild.rs:103
src/sql/explain.rs:844
src/sql/explain.rs:867
src/connector/starrocks/table/catalog.rs:983   (test assertion: matches! pattern)
```

### Invariant assertion

```rust
// src/engine/catalog.rs::InMemoryCatalog::register_starrocks_table
debug_assert!(
    matches!(
        table.source,
        ScanSource::StarRocks { db_id, table_id }
        if db_id == physical_layout.db_id && table_id == physical_layout.table_id
    ),
    "StarRocks TableDef.source must agree with PhysicalTableLayout on (db_id, table_id)",
);
```

Release builds skip the assert; debug builds (sql-tests, unit tests) trip it if any
future construction site drifts from the contract.

## Validation

| Gate | Command | Expected |
|---|---|---|
| Build | `cargo build` | clean |
| Lib tests | `cargo test --lib` | unchanged pass rate |
| Targeted unit test | `cargo test --lib starrocks_table_def_carries_runtime_ids` | new test passes |
| iceberg-ivm regression | `sql-tests --suite iceberg --mode verify` | 61/61 (unchanged) |
| **filter parallel (target)** | `sql-tests --suite filter --mode verify` (default `-j`) | **15/15 PASS** |
| filter sequential (no regression) | `sql-tests --suite filter --mode verify -j 1` | 15/15 PASS in ≤ 5s wall |
| low-cardinality-dict suite | `sql-tests --suite low-cardinality-dict --mode verify` if it exists | unchanged |

Parallel filter going from "fail=10/15 within minutes" to "15/15 PASS" is the
definition of done.

## Risks

1. **Construction site drift**: a future StarRocks-table registration writes the
   wrong `(db_id, table_id)` into `TableDef::source`. Mitigated by the debug-assert
   in `register_starrocks_table`.
2. **Fixture pollution**: a test fixture that flows through the dict rewrite path
   uses `{ db_id: 0, table_id: 0 }`, silently failing dict snapshot lookup.
   Mitigated by reviewing fixture sites during enum migration — fixtures that touch
   dict rewrite must use realistic IDs. Lib tests in `src/engine/dictionary/` will
   catch this.
3. **Secondary contention remains on `state.catalog`**: INSERT path still acquires
   `state.catalog.write()` inside `commit_catalog_visible_version`. Removing the
   dict hot reader on `state.starrocks_table` is necessary; remaining catalog
   contention is acceptable because the catalog reader path already follows
   clone-then-release. The acceptance bar is "filter parallel passes 15/15", not
   "linear scaling vs `-j 1`".
4. **Iceberg path unaffected**: code paths reading `ScanSource::IcebergDataFiles`
   are untouched; iceberg-ivm 61/61 must be unchanged.

## Non-goals reaffirmed

This is a single-PR fix targeted at the regression introduced by PR #191. It does
not redesign the catalog locking model, does not change writer critical sections,
and does not introduce new lock primitives.
