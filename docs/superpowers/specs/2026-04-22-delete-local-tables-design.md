# Delete Local-Table Backend, Unify Around Managed Lake — Design

**Status:** Draft (for review before implementation plan)
**Date:** 2026-04-22
**Supersedes:** none
**Related:**
- `docs/superpowers/specs/2026-04-20-standalone-lake-metadata-design.md`
- `docs/superpowers/specs/2026-04-21-standalone-managed-lake-lifecycle-design.md`

## 1. Goal

Delete the parquet-on-disk "local tables" backend from the standalone
engine so `CREATE TABLE`, `INSERT`, `SELECT` and stream load all flow
through the managed-lake (SQLite control plane + S3/MinIO object store +
StarRocks lake format) path.

Tests under `sql-tests/**` must continue to pass. Their CREATE TABLE
statements stay unchanged — the DDL layer absorbs the StarRocks-style
defaults so bare `CREATE TABLE t (a INT, b STRING)` still works.

## 2. Why now

- PR #51 landed the managed-lake lifecycle (DROP, TRUNCATE, async erase,
  restart recovery). Managed lake is the production data path.
- Local tables were the bootstrap backend. They still carry
  `LocalTableSemantics`, aggregate merge-on-write, stream load, parquet
  I/O — ~2k lines that duplicate functionality the managed lake already
  has or should have.
- Two backends means two code paths for every new feature. Collapsing to
  managed lake removes that tax.

## 3. Non-goals

- No migration tool for existing local-table data (there is no
  production usage outside dev/tests).
- No support for `CREATE TABLE ... ENGINE=parquet` or external parquet
  file tables — Iceberg remains the path for "mount external data".
- No new lake backend; we rely on the existing managed lake.

## 4. In-scope deletions

### 4.1 Source files to delete

- `src/standalone/engine/local/parquet.rs`
- `src/standalone/engine/local/insert.rs`
- `src/standalone/engine/local/stream_load.rs`
- `src/standalone/engine/local/aggregate.rs`
- All `LocalTableSemantics`-related helpers currently in
  `src/standalone/engine/local/mod.rs`:
  - `LocalTableSemantics` struct
  - `update_local_table_semantics` / `get_local_table_semantics` /
    `remove_local_table_semantics` / `remove_local_database_semantics`
  - `create_local_table_from_columns`
  - `apply_local_table_semantics_if_needed`
  - `build_parquet_table`
  - `read_local_parquet_data`
  - `write_parquet_to_path`
  - `persist_local_database_if_needed` / `persist_local_table_if_needed`
  - `delete_local_table_if_needed` / `delete_local_database_if_needed`
  - `restore_local_catalog`
  - `ensure_dual_table` / `ensure_dual_in_database` — replaced by a
    parser-layer `FROM dual` rewrite (see §5.1)
- `StandaloneState.local_table_semantics` field
- `TableStorage::Parquet` enum variant (in `sql::catalog::TableStorage`)
- `PhysicalTableLayout` fields only used by local tables (audit during
  implementation)

### 4.2 Source files to keep (the "A" split from review)

- `normalize_identifier` → promoted back up to
  `src/standalone/catalog.rs` (or a fresh
  `src/standalone/engine/catalog/` module). These are the utilities the
  lake/iceberg/sqlparse layers share and were only living under
  `engine/local/` because of Task 14 of PR1's refactor.
- `InMemoryCatalog`, `CatalogProvider`, `ColumnDef`, `TableDef`,
  `TableStorage::{Iceberg, ManagedLake}`, `PhysicalTableLayout`,
  `ManagedTabletRef` — moved alongside `normalize_identifier`.
- `DEFAULT_DATABASE` constant.

`engine/local/` directory is deleted after this refactor.

### 4.3 Call sites to rewrite

- `src/service/stream_load.rs:1130`
  `engine.stream_load_local_table(...)` → route to a new
  `engine.stream_load_managed_lake_table(...)` that:
  1. Runs the same CSV/JSON parsing (moved to
     `engine/sqlparse/stream_load.rs` or similar neutral location)
  2. Produces `Vec<Vec<Literal>>` rows
  3. Calls the managed-lake INSERT path instead of parquet I/O
- `src/standalone/engine/sqlparse/statement.rs` — remove branches that
  dispatch to `create_local_table_from_columns`,
  `insert_into_local_table`, `build_parquet_table`. All CREATE/INSERT
  on a managed database land in the managed-lake DDL/txn path.
- `src/standalone/engine/mod.rs::StandaloneNovaRocks::open` — remove
  the `ensure_dual_table` call; dual is handled by parser rewrite.
- `src/standalone/engine/mod.rs::StandaloneNovaRocks::stream_load_local_table`
  public method — rename / reroute (see above).

## 5. Behaviour changes

### 5.1 `FROM dual` → no-FROM rewrite

**Decision:** recognise `FROM dual` in the SQL normaliser
(`sql::parser::dialect::normalize_for_raw_parse`) and strip it, so
downstream sees `SELECT 1` and the existing `evaluate_constant_select`
path picks it up.

Rewrite rules:

- `SELECT <projection> FROM dual [alias]`
  → `SELECT <projection>` (alias dropped)
- `SELECT <projection> FROM dual WHERE ...`
  → `SELECT <projection> FROM (SELECT 1 AS __dual_c) t WHERE ...` —
  actually simpler: only strip when the query has no WHERE/GROUP/
  HAVING/LIMIT/ORDER that would need a from clause. If any of those
  are present, fall through to the managed-lake path (no `dual` table
  exists, so it errors as "unknown table: dual") — which mirrors what a
  user would hit with a misspelled table name.

Keeping this narrow matches the four problem's **option (b)** from the
review.

### 5.2 StarRocks-style defaults for CREATE TABLE

**Source of truth:** StarRocks branch-4.1
`CreateTableAnalyzer.java:325-370` (keys default) and `:715-736`
(distribution default).

Default rules (adapted for NovaRocks managed lake):

- **`KEY` clause omitted** → choose DUPLICATE KEY from leading columns:
  - Skip generated columns
  - Add columns until 3 columns or cumulative index size > 36 bytes
    (StarRocks' `SHORTKEY_MAX_COLUMN_COUNT` / `SHORTKEY_MAXSIZE_BYTES`)
  - Stop on VARCHAR (include it then stop)
  - Exclude FLOAT/DOUBLE (not key-eligible)
  - If the resulting set is empty, error:
    "data type of first column cannot be a key column"
- **`DISTRIBUTED BY` clause omitted** → HASH on first key column
  (simpler than StarRocks' random-distribution path; NovaRocks doesn't
  have a random-distribution physical implementation yet, and the test
  suite is small enough that a single-bucket hash is fine)
- **`BUCKETS n` clause omitted** → default `n = 1`
  (StarRocks uses runtime auto-bucketing which is out of scope; `1` is
  the smallest functional default)
- **AGGREGATE KEY** — unchanged; still rejected by managed-lake
  (`ddl.rs` explicitly errors today)
- **PRIMARY KEY** — unchanged; still behaves as today if the user
  writes it explicitly (managed lake today supports DUP only; if PK is
  written, keep the existing error "managed standalone CREATE TABLE
  does not support PRIMARY KEY yet")

The defaults live in one place: a new helper
`apply_create_table_defaults` in
`src/standalone/engine/sqlparse/statement.rs` that takes the parsed
`CreateTableKind` + column list and returns the canonicalised one
before it flows into `create_managed_table`. Alternatively, extend
`create_managed_table` in `lake/ddl.rs` to absorb `key_desc: None`,
`bucket_count: None`. The plan picks the first (parser-side defaulting
keeps DDL semantics unchanged and the code colocated with the rest of
the sqlparse layer).

### 5.3 Stream load target

The HTTP stream load endpoint already accepts `{database, table}`. Once
local tables are gone, every target is managed. The handler:

1. Parse payload (CSV or JSON) into `Vec<Vec<Literal>>` using the
   existing `parse_csv_stream_load_rows` / `parse_json_stream_load_rows`
   helpers — unchanged, these are generic.
2. Resolve the managed table via `ManagedLakeCatalog::table(db, t)`.
3. Feed the rows through `insert_into_managed_lake_table`
   (`lake/txn.rs`), which already builds tablets, writes native rowsets,
   publishes version.

The parse helpers currently live in `engine/local/stream_load.rs`. They
move to a neutral location: `engine/stream_load.rs` (new file) or
`engine/sqlparse/stream_load.rs`.

## 6. Test strategy

### 6.1 sql-tests

- **No `.sql` file changes required** because the DDL defaults make bare
  `CREATE TABLE t (...)` valid.
- Test runner must ensure MinIO is reachable (new doc in
  `sql-tests/README.md`, pre-test script checking
  `http://127.0.0.1:9000`).
- Each `.sql` case runs under a unique managed warehouse prefix
  (`codex-sql-tests/<run_id>/...`) — matches what
  `maybe_managed_lake_config` already does for engine-level tests.
- Expect a notable slowdown on the sql-test suite (now S3 write per
  INSERT). Accept it — the trade-off for a single backend.

### 6.2 Unit tests

- Delete all tests that exercise
  `build_local_insert_batch` / `cast_batch_to_schema_relaxes_map_key_nullability`
  / any `LocalTableSemantics`-specific test.
- Keep the arrow-MapArray nullability fix that moved into the local
  insert path; re-home the regression test under the managed-lake
  insert path so the same coverage stays.
- Keep / re-home the aggregate-merge tests only if managed lake plans
  to support aggregate tables — otherwise delete them outright (the
  current `ddl.rs` rejects `AGGREGATE KEY`, so the merge code would
  have no caller anyway).

### 6.3 Engine-level integration tests

`standalone::engine::tests::embedded_session_*` — several rely on local
tables for setup (constant `SELECT`, small in-memory flows). Audit and
either:
- Rewrite to use managed tables (preferred), or
- Delete the test if its coverage is redundant with managed-lake tests

## 7. Out-of-scope / follow-ups

- Random distribution (StarRocks `RandomDistributionDesc`) — not needed
  for default-DDL handling, left as future work if a perf case emerges.
- Auto-bucket count based on data-volume estimate — StarRocks 3.x has
  this; we default to 1 for now.
- PRIMARY KEY managed lake — tracked separately.
- `CREATE TABLE AS SELECT` on managed lake — tracked separately.

## 8. Risks & mitigations

| Risk | Mitigation |
|---|---|
| CI without MinIO breaks everything | Document MinIO as a hard dev dependency; add a pre-test sanity check that fails fast with a clear message; provide a one-liner `docker run --rm -d -p 9000:9000 minio/minio ...` snippet |
| sql-tests slowdown unacceptable | Measure on a representative subset before committing; if >5× slowdown, consider an in-process S3 fake (e.g., `s3s`/`s3-tempo`) for tests only — but NOT now, that's scope creep |
| StarRocks default KEY rule picks a column that can't be a key in managed lake's current implementation | Same constraint applies to NovaRocks (VARCHAR OK, FLOAT rejected) — mirror StarRocks' rejection message so behaviour is predictable |
| Stream load HTTP endpoint breaks in production | Nobody runs it in production today outside dev, but mention in release notes |
| `FROM dual` rewrite is too narrow and breaks some test | Keep the rewrite narrow; if a test regresses, extend the rewrite case-by-case (do not auto-materialise a real `dual` managed table) |

## 9. Acceptance criteria

1. `git grep -n "LocalTableSemantics\|engine::local\|stream_load_local_table"` returns zero hits outside this spec/plan.
2. `engine/local/` directory no longer exists.
3. `cargo test --lib` — all tests pass.
4. `cargo run --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests -- --suite <any> --mode verify` — all existing suites pass without editing any `.sql` file.
5. `src/standalone/engine/mod.rs` line count decreases (no absorption of local code).
6. A new `CREATE TABLE t (a INT, b VARCHAR(20))` without KEY/DISTRIBUTED/BUCKETS succeeds and creates a DUP_KEYS managed table with first-column hash, 1 bucket.
7. `SELECT 1 FROM dual` succeeds with value `1`.

## 10. Plan link

Implementation plan: `docs/superpowers/plans/2026-04-22-delete-local-tables.md`
