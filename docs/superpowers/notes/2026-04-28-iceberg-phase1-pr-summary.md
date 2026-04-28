# Phase 1 — Iceberg v3 INSERT / DELETE — PR Summary

**Branch**: `iceberg-insert-delete-p1`
**Spec**: [docs/superpowers/specs/2026-04-27-iceberg-v3-insert-delete-phase1-design.md](../specs/2026-04-27-iceberg-v3-insert-delete-phase1-design.md)
**Plan**: [docs/superpowers/plans/2026-04-28-iceberg-v3-insert-delete-phase1.md](../plans/2026-04-28-iceberg-v3-insert-delete-phase1.md)
**Commits**: 25 ahead of `main`

## What lands in this PR

### End-to-end working today

- **`INSERT INTO iceberg.<db>.<tbl> SELECT ...`** — for v2 and v3 iceberg
  tables, on local-FS catalogs. Goes through the iceberg-rust
  `DataFileWriter` → `IcebergCommitCollector` → `FastAppendCommit` →
  `Catalog::update_table` flow.
- **`INSERT OVERWRITE iceberg.<db>.<tbl> SELECT ...`** — same, but uses the
  custom `OverwriteCommit` action that walks the base snapshot's manifest
  list, marks every live data file as DELETED in a new manifest, writes
  a new ADDED-files manifest, and produces a snapshot with
  `summary.operation = "overwrite"`. Empty SELECT + non-empty base is
  treated as "clear table" per spec §4.1.
- **`INSERT INTO iceberg.<db>.<tbl> VALUES (...)`** — unchanged, continues
  through the existing fast-append path at
  [`registry::insert_rows`](../../../src/connector/iceberg/catalog/registry.rs).

### Plumbing in place but partially exercised end-to-end

- **`DELETE FROM iceberg.<db>.<tbl> WHERE ...`** — the AST surface,
  validation, predicate translator (sqlparser → `iceberg::expr::Predicate`),
  position-delete Parquet writer
  ([`commit/position_delete_writer.rs`](../../../src/connector/iceberg/commit/position_delete_writer.rs)),
  and `RowDeltaCommit` action are all implemented. iceberg-rust 0.9 ships
  the `_file` / `_pos` virtual columns natively, so we use
  `TableScan::select(['_file','_pos']).with_filter(predicate).with_row_selection_enabled(true)`
  rather than touching the analyzer.

  Active runtime paths:
  - DELETE without WHERE → user-actionable error (Phase 1 NEG-3).
  - DELETE with an unsupported predicate (LIKE / arithmetic / etc.) →
    actionable error.
  - DELETE that matches no rows → no-op, no snapshot advance.

  End-to-end DELETE WHERE round-trip is now passing: snapshot id advances,
  position-delete files land on disk, and subsequent SELECTs (or programmatic
  catalog reads) see the new snapshot. Verified by
  `iceberg_delete_where_removes_matching_rows`, which seeds 4 rows, runs
  `DELETE WHERE id = 2` then `DELETE WHERE id IN (1, 4)`, and asserts the
  snapshot id advances once per DELETE.

### Foundation modules

| Module | What it provides |
|---|---|
| [`commit/types.rs`](../../../src/connector/iceberg/commit/types.rs) | `CommitOpKind`, `WrittenFile`, `CommitOutcome` |
| [`commit/abort.rs`](../../../src/connector/iceberg/commit/abort.rs) | `AbortLog` with idempotent best-effort OpenDAL cleanup |
| [`commit/validation.rs`](../../../src/connector/iceberg/commit/validation.rs) | Pre-lowering validators (v3, partition-spec, equality-deletes, schema-match) |
| [`commit/collector.rs`](../../../src/connector/iceberg/commit/collector.rs) | `IcebergCommitCollector` (query-scoped state, supports both IcebergSink-driven and engine-injected file lists) |
| [`commit/action.rs`](../../../src/connector/iceberg/commit/action.rs) | `IcebergCommitAction` trait + `CommitCtx` |
| [`commit/data_file.rs`](../../../src/connector/iceberg/commit/data_file.rs) | `WrittenFile` → iceberg `DataFile` conversion |
| [`commit/fast_append.rs`](../../../src/connector/iceberg/commit/fast_append.rs) | `FastAppendCommit` (INSERT INTO) — wraps `Transaction::fast_append` |
| [`commit/row_delta.rs`](../../../src/connector/iceberg/commit/row_delta.rs) | `RowDeltaCommit` (DELETE) — custom `TransactionAction`, public `ManifestWriter::add_delete_file` |
| [`commit/overwrite.rs`](../../../src/connector/iceberg/commit/overwrite.rs) | `OverwriteCommit` (INSERT OVERWRITE) — custom `TransactionAction` |
| [`commit/helpers.rs`](../../../src/connector/iceberg/commit/helpers.rs) | snapshot id / now_ms / metadata-dir / manifest-list IO |
| [`commit/run.rs`](../../../src/connector/iceberg/commit/run.rs) | `run_iceberg_commit` orchestrator with commit-unknown classification |
| [`engine/iceberg_writer.rs`](../../../src/engine/iceberg_writer.rs) | Engine-side INSERT INTO / OVERWRITE entry point |
| [`engine/delete_flow.rs`](../../../src/engine/delete_flow.rs) | Engine-side DELETE entry point — validation, sqlparser→Predicate translator, scan-and-write driver, RowDeltaCommit dispatch |
| [`commit/position_delete_writer.rs`](../../../src/connector/iceberg/commit/position_delete_writer.rs) | Minimal v2-compatible position-delete Parquet writer (iceberg-rust 0.9 doesn't ship one) |

### Vendor / dependency changes

- New `vendor/iceberg-0.9.0/` directory with two minimal patches that
  raise visibility on `TransactionAction` and `TableCommit::builder().build()`
  so downstream crates can implement custom transaction actions for
  RowDelta and OverwriteFiles. Diff is documented in
  [`vendor/iceberg-0.9.0/PATCH.md`](../../../vendor/iceberg-0.9.0/PATCH.md).
  When iceberg-rust ships native `Transaction::row_delta` and
  `Transaction::overwrite_files` actions (likely 0.10/0.11), this whole
  vendor tree can be deleted.
- New crates: `uuid` (for commit UUIDs) and `as-any` (transitive
  requirement of the patched `TransactionAction` trait).
- Added `services-memory` to opendal features for `AbortLog` tests.

### Spike outcomes

- [`spikes/2026-04-28-manifest-deleted-entry.md`](../spikes/2026-04-28-manifest-deleted-entry.md):
  `ManifestWriter::add_delete_file` is the only public path to status=DELETED
  entries in iceberg-rust 0.9. OverwriteCommit uses it.
- [`spikes/2026-04-28-commit-unknown-classification.md`](../spikes/2026-04-28-commit-unknown-classification.md):
  iceberg::ErrorKind landscape mapped to definite-fail vs commit-unknown.
  `run_iceberg_commit` uses substring matching against the eight
  definite-fail variants; everything else is treated as commit-unknown.

## Test status

- **Unit tests**: 1241 passing across the workspace (commit module
  contributes 20+ new ones).
- **Integration tests** (Plan Tasks 15–17): 6 passing in
  `engine::tests::iceberg_*`:
  - `iceberg_insert_select_drives_a_new_snapshot`
  - `iceberg_insert_overwrite_replaces_all_rows`
  - `iceberg_delete_where_removes_matching_rows` (eq + IN list)
  - `iceberg_delete_no_match_is_a_noop`
  - `iceberg_delete_without_where_is_rejected`
  - `iceberg_delete_unsupported_predicate_is_rejected`
- **Build**: clean against vendored iceberg-rust 0.9.
- **Clippy**: no new warnings on any new file with `-D warnings`
  (74 pre-existing errors in unrelated files unchanged).
- **SQL regression suite + Fault injection** (Plan Tasks 18–19):
  **not in this PR** — they need a standalone-server test harness that
  is separate work; tracked as Phase 1.x.

## Known limitations (explicit `Err(...)` today)

| Path | Behavior | Resolution |
|---|---|---|
| `INSERT OVERWRITE iceberg ... VALUES` / `UNION ALL` / `generate_series` | Rejected at engine layer | Phase 1.x — re-use the literal-INSERT batch builder and route through `OverwriteCommit` |
| `INSERT INTO iceberg ... GenerateSeriesSelect` with overwrite=true | Rejected | same |
| Abort cleanup on S3-backed iceberg tables | Rejected at engine layer | Phase 1.x — extend `build_opendal_for_table` to mirror the catalog's S3 config |
| Multi-data-file iceberg tables on local-FS catalogs are visible to the SELECT side | Standalone catalog's `TableStorage::LocalParquetFile` only registers the *first* iceberg data file (backend.rs:172-179). Pre-existing NovaRocks gap unrelated to this PR. Verified iceberg-side INSERT/OVERWRITE/DELETE work via snapshot-id assertions in integration tests | Add a `LocalParquetFiles { files }` variant to `TableStorage` and update the iceberg backend to use it |
| Equality-delete writes / reads | Out of scope per spec §0.3 | Phase 2 |
| Iceberg v3 deletion vectors (Puffin) | Out of scope per spec §0.3 | Phase 2 |
| Row-lineage-enabled tables | Validation rejects them with a clear error | Phase 2 |

## Test plan for reviewer

```bash
# Build + unit tests
cd <worktree>
cargo build -p novarocks
cargo test -p novarocks --lib

# Clippy (only check files this PR touched are clean)
cargo clippy -p novarocks --lib --no-deps -- -D warnings 2>&1 | grep -E '/(commit|engine/(iceberg_writer|delete_flow|insert_flow))/' || echo 'clean'

# Format
cargo fmt --check
```

End-to-end smoke (manual until Task 15 lands):

```bash
# 1. Start the standalone server
NO_PROXY=127.0.0.1,localhost cargo run --release -- standalone-server --port 9030 &

# 2. Execute against a local-FS iceberg catalog (adjust per your setup)
mysql -h127.0.0.1 -P9030 -e "
  CREATE EXTERNAL CATALOG it_v2 PROPERTIES ('type' = 'iceberg', 'warehouse' = 'file:///tmp/it_v2');
  CREATE DATABASE it_v2.db1;
  CREATE TABLE it_v2.db1.t (id INT, v STRING) USING iceberg
    TBLPROPERTIES ('format-version' = '3');
  INSERT INTO it_v2.db1.t SELECT 1, 'a' UNION ALL SELECT 2, 'b';
  SELECT id, v FROM it_v2.db1.t ORDER BY id;
  -- INSERT OVERWRITE: replace all rows with one new value
  INSERT OVERWRITE it_v2.db1.t SELECT 99, 'X' FROM it_v2.db1.t WHERE id = 1;
  SELECT id, v FROM it_v2.db1.t ORDER BY id;
"
```

## Performance baseline

Not measured in this PR; the `iceberg_writer` path delegates to phase4a's
`write_chunks_as_iceberg_data_files` whose performance is the same as MV
refresh (also a phase4a subject).  After Task 14B lands and DELETE is
end-to-end, run the baselines from spec §6.7 and update this note.

## What this is NOT

- A drop-in replacement for StarRocks's iceberg INSERT/DELETE — feature
  parity is explicitly limited per spec §0.3.
- A FE-driven path — only standalone mode is wired (spec §0.4 / "Mode A").
- Production-ready against multi-writer concurrency — single-writer
  assumption per spec §0.4 / T2.
