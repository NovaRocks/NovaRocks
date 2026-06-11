# Iceberg v3 Row-Lineage Completion — Design

Status: Draft
Date: 2026-05-06
Scope: Close the remaining gaps in `NovaRocks Iceberg v3 完成度清单.md` §12 (Row Lineage):
preserve `_row_id` / `_last_updated_sequence_number` across `OPTIMIZE`, plus the SQL
regression tests that lock in the V3 row-identity invariant. Includes the §9
prerequisite of routing the four Iceberg metadata tables
(`$snapshots` / `$history` / `$refs` / `$partitions`) through the standalone-server
SQL parser, since the uniqueness-invariant test consumes them.

## 0. Goals and Non-Goals

### 0.1 Goals

1. **OPTIMIZE preserves row identity.** When `OPTIMIZE TABLE t` rewrites data files
   on a v3 row-lineage table, every surviving row keeps its original `_row_id` and
   `_last_updated_sequence_number`, so downstream IVM can pair post-OPTIMIZE rows
   with their MV state by `_row_id` after subsequent UPDATE/DELETE.
2. **OPTIMIZE does not bump `next_row_id`.** Compaction is a pure rewrite; the
   table's monotonic row-id allocator must not advance.
3. **IVM stays a no-op for OPTIMIZE.** Existing
   `validate_replace_snapshot` (`src/connector/iceberg/changes.rs:386-405`) already
   skips a Replace snapshot when total-records and schema_id are unchanged. We
   confirm this remains true after the rewrite (test C1 enforces it).
4. **Metadata tables are queryable.** `SELECT ... FROM t$snapshots`,
   `t$history`, `t$refs`, `t$partitions` work end-to-end through standalone-server
   so the cross-snapshot uniqueness regression can drive itself.
5. **SQL regression coverage** for: OPTIMIZE row-identity preservation, branch/tag
   row-identity isolation, cross-snapshot row-id uniqueness invariant.

### 0.2 Non-Goals

- **No read-side changes.** `synthesize_row_lineage_columns`
  (`src/exec/operators/scan/runner.rs:246-304`) already prefers stored physical
  columns at the V3 reserved field IDs.
- **No IVM code changes.** `classify_snapshot` (`src/connector/iceberg/changes.rs:372`)
  already skips validated `Replace` snapshots.
- **No V2 / non-row-lineage path changes.** OPTIMIZE on V2 tables and on V3 tables
  with `write.row-lineage=false` keeps the existing
  `write_chunks_as_iceberg_data_files` writer.
- **No incremental OPTIMIZE.** Whole-table rewrite only; partition-scoped /
  small-file-only OPTIMIZE is §11 future work.
- **No OPTIMIZE on branches.** Whether `compact.rs` accepts a `target_ref` other
  than `main` is out of scope; the branch consistency test exercises INSERT /
  UPDATE / DELETE on a branch but not OPTIMIZE.
- **No cross-engine row-id invariant.** Spark / Trino / PyIceberg ↔ NovaRocks
  cross-write coverage is §17.
- **No new metadata table types.** Only the four already implemented at the BE
  layer (`SNAPSHOTS` / `HISTORY` / `REFS` / `PARTITIONS`); `FILES`, `MANIFESTS`,
  `LOGICAL_ICEBERG_METADATA` are not exposed via SQL in this PR.
- **No `LATERAL` join support.** Test C3 uses `UNION ALL` over hardcoded snapshot
  ids captured beforehand.

## 1. Background

### 1.1 What already works

- **Read path** materializes `_row_id` from either a stored physical column with
  reserved field id `i32::MAX - 107` or, when absent, `first_row_id + offset`.
  Same for `_last_updated_sequence_number` at field id `i32::MAX - 108`. See
  `src/exec/operators/scan/runner.rs:246-304` and `src/exec/row_position.rs:82-83`.
- **Writer for stored row-lineage columns** exists:
  `write_row_lineage_batches_as_data_files` /  `append_row_lineage_columns` at
  `src/connector/iceberg/data_writer.rs:197-297`. Both COW UPDATE
  (`src/engine/mutation_flow.rs:672-708`) and MOR UPDATE
  (`src/engine/mutation_flow.rs:218-269`) already use it.
- **IVM Replace-snapshot skip** is implemented:
  `validate_replace_snapshot` (`src/connector/iceberg/changes.rs:430-493`) checks
  total-records unchanged + schema_id unchanged + added/deleted file counts
  present, then `classify_snapshot` returns `Ok(None)` so the snapshot
  contributes zero rows to lineage batches.
- **Iceberg metadata BE bridge** exists for the four tables in scope. See
  `src/connector/iceberg/metadata.rs:38-72` and the lowering glue at
  `src/lower/node/hdfs_scan.rs:894-907`. The Java side is `IcebergMetadataBridge`.

### 1.2 What is missing

- **OPTIMIZE write path** (`src/connector/iceberg/compact.rs::execute_whole_table_rewrite`)
  selects user columns only, calls `write_chunks_as_iceberg_data_files`, and the
  `RewriteDataFiles` commit op (`src/connector/iceberg/commit/rewrite_data_files.rs:63`)
  allocates a fresh `first_row_id` from `next_row_id`. Surviving rows lose their
  original `_row_id` / `_last_updated_sequence_number`.
- **Metadata table SQL parser entry** is not wired. The standalone-server
  sqlparser dialect does not accept the `$` sigil between table name and
  metadata-type. There is no analyzer dispatch from `t$snapshots` to
  `IcebergMetadataScanOp`.
- **Branch / cross-snapshot invariant tests** do not exist. The current OPTIMIZE
  test (`sql-tests/iceberg/sql/iceberg_v3_optimize_compact_data_files.sql`) only
  asserts post-OPTIMIZE row counts, not row-id identity.

## 2. Decisions (Locked)

| # | Decision |
|---|---|
| D1 | **Three-phase scope.** Phase A (metadata table parser routing, all 4 tables) + Phase B (OPTIMIZE preserve row-lineage) + Phase C (3 SQL regression files). Single PR. |
| D2 | **Preserve strategy = stored physical column.** OPTIMIZE rewrites materialize `_row_id` and `_last_updated_sequence_number` as parquet columns at field ids `i32::MAX-107` / `i32::MAX-108`. The output `DataFile.first_row_id` stays `None`; the read path resolves via stored column. |
| D3 | **Path split = capability check.** OPTIMIZE detects `format_version >= 3 && write.row-lineage = true` per loaded table metadata; only that branch goes through the row-lineage writer. V2 and V3-without-row-lineage retain the existing path. |
| D4 | **`next_row_id` is not bumped by OPTIMIZE.** The `RewriteDataFiles` commit op skips `next_row_id` allocation in the row-lineage path. |
| D5 | **Metadata table syntax = pre-parse normalizer.** Mirrors the existing `__nr_ref:` pattern in `src/sql/parser/dialect/mod.rs:232-365`. Before handing the SQL to sqlparser, rewrite the trailing `$<metatype>` of any FROM/JOIN identifier into a `.__nr_meta_<metatype>__` last part. All three identifier forms are accepted: `<tbl>$<type>`, `<db>.<tbl>$<type>`, `<cat>.<db>.<tbl>$<type>`. The analyzer detects the `__nr_meta_*__` last-part suffix on the rewritten name, strips it, resolves the base table_def from the remaining parts, and routes the relation to `IcebergMetadataScanOp`. Decision rationale: keeps sqlparser dialect untouched, reuses an established pattern, no collision with real tables. |
| D6 | **Metadata table set = the 4 BE-supported types only.** `SNAPSHOTS`, `HISTORY`, `REFS`, `PARTITIONS`. Other types (FILES, MANIFESTS, LOGICAL_ICEBERG_METADATA) remain rejected by the parser normalizer until further work. |
| D7 | **OPTIMIZE on a branch is rejected.** If the target table reference is not the main ref, the OPTIMIZE statement returns an actionable error: `OPTIMIZE on branch/tag is not supported`. Exception-free; consistent with pre-existing behaviour. |
| D8 | **C3 Invariant 2 implementation.** Capture the snapshot id list ahead of time (one query per snapshot via `SELECT ... FOR VERSION AS OF <id>`), `UNION ALL` the historic projections, then `GROUP BY id` to assert each logical row maps to a single `_row_id`. The hardcoded snapshot ids are obtained from `t$snapshots` earlier in the same test file. |
| D9 | **Fail-fast on missing row-lineage column.** OPTIMIZE on a row-lineage table that produces a chunk without the `_row_id` / `_last_updated_sequence_number` projection (i.e., scan-side regression) must error explicitly, not silently fall back to fresh-id allocation. |

## 3. Architecture and Data Flow

### 3.1 Phase A — Metadata table SQL routing

```
SQL text:           SELECT * FROM ice.db1.t$snapshots
                              │
                              ▼
src/sql/parser/dialect/mod.rs::normalize_for_raw_parse
  - regex over identifier-context: `(<part>)\$(snapshots|history|refs|partitions)`
  - rewrite to:    SELECT * FROM ice.db1.t.__nr_meta_snapshots__
                              │
                              ▼
sqlparser parses cleanly (only standard identifiers)
                              │
                              ▼
src/sql/analyzer/resolve_from.rs::resolve_table_factor (extended)
  - inspect last name part; if matches `__nr_meta_<type>__`:
      strip suffix → resolve base table_def
      build Relation::IcebergMetadataScan { table_def, metatype }
                              │
                              ▼
src/lower/node/hdfs_scan.rs (existing)
  - already accepts metadata_table_type → IcebergMetadataScanConfig
                              │
                              ▼
src/connector/iceberg/metadata.rs::IcebergMetadataScanOp (existing)
```

### 3.2 Phase B — OPTIMIZE row-lineage path

```
OPTIMIZE TABLE ice.db1.t
            │
            ▼
src/connector/iceberg/compact.rs::execute_whole_table_rewrite
  ├─ load_table → metadata
  ├─ if metadata.format_version >= 3 && metadata.row_lineage_enabled():
  │     row_lineage_path()
  │  else:
  │     classic_path()              ← unchanged today
  │
  ▼
row_lineage_path():
  ├─ build SELECT including reserved meta cols:
  │     SELECT <user_cols>, _row_id, _last_updated_sequence_number FROM t
  │     ▲ scan applies DV / position-delete / equality-delete; deleted rows drop out
  │
  ├─ pipeline drives chunks → write_row_lineage_batches_as_data_files
  │     ▲ append_row_lineage_columns sets parquet field ids 2147483540 / 2147483539
  │     ▲ writer returns WrittenFile with first_row_id = None
  │
  └─ commit:
        CommitOpKind::RewriteDataFiles {
          deleted_data_files: <input live files>,
          added_data_files:   <rewritten files, first_row_id=None>,
          equality_delete_files_to_delete: <subsumed e-deletes>,
          dv_files_to_delete:              <subsumed DVs>,
        }
        rewrite_data_files.rs::commit:
          if any added file has first_row_id=None → DO NOT call next_row_id().fetch_add()
          else (classic path)                     → existing fetch_add behaviour
```

### 3.3 IVM interaction (no code change required)

```
post-OPTIMIZE Snapshot:
  operation = "replace"
  total-records: unchanged
  schema_id:      unchanged
  added-data-files: K
  deleted-data-files: M
            │
            ▼
src/connector/iceberg/changes.rs::classify_snapshot (existing)
  → validate_replace_snapshot OK
  → returns Ok(None)
            │
            ▼
IVM treats the snapshot as a no-op for MV refresh.

Subsequent UPDATE on a row in a rewritten file:
  - new file inherits row's `_row_id` (preserved across OPTIMIZE)
  - sidecar / MOR delta carries the same row_ids the MV remembers
  - IVM's row-identity matching by `_row_id` continues to work correctly.
```

## 4. Module-by-module changes

### 4.1 `src/sql/parser/dialect/mod.rs`

- Extend `normalize_for_raw_parse` (or its cousin used during `__nr_ref:` rewriting)
  with a tokenizer-aware pass that recognises `<ident>$<metatype>` in FROM /
  JOIN / table-factor positions and rewrites to `<ident>.__nr_meta_<metatype>__`.
  Restrict `<metatype>` to: `snapshots`, `history`, `refs`, `partitions`.
- An unrecognised metatype after `$` produces a parse error with an actionable
  message: `unsupported iceberg metadata table type: <metatype>; expected one of snapshots/history/refs/partitions`.
- Tokenizer concern: the `$` sigil must not be confused with parameter
  placeholders (which sqlparser uses for `$1`-style numbered parameters in the
  PostgreSQL dialect). Verify the StarRocks dialect either disables that or that
  our rewrite runs before sqlparser tokenisation. The plan tier will resolve
  exact insertion point.

### 4.2 `src/sql/analyzer/resolve_from.rs`

- After locating `last_part` of the table identifier, check for the
  `__nr_meta_<type>__` suffix.
- If matched, strip the suffix, resolve the base table_def, and emit a new
  `Relation::IcebergMetadataScan { table_def, metatype: IcebergMetadataTableType }`
  variant instead of `Relation::Scan`.
- Build the scope from a fixed schema per metatype (matches what
  `IcebergMetadataScanOp::output_schema` produces). The schemas are:
  - **Snapshots:** committed_at, snapshot_id, parent_id, operation, manifest_list, summary
  - **History:** made_current_at, snapshot_id, parent_id, is_current_ancestor
  - **Refs:** name, type, snapshot_id + 3 retention fields
  - **Partitions:** partition (struct), record_count, file_count, position_delete_file_count, equality_delete_file_count
  Reuse the column shapes already encoded in `IcebergMetadataBridge` Java side.
- Ensure interaction with branch/tag suffix handling: a query like
  `t.branch_dev$snapshots` is rejected at analysis time with an explicit error
  (or normalized — decision deferred to plan; default = reject).

### 4.3 `src/sql/optimizer/`

- Add a planner rule (or extend the existing scan planner) so
  `Relation::IcebergMetadataScan` lowers to a node that maps directly to the
  existing `IcebergMetadataScanConfig` constructed in
  `src/lower/node/hdfs_scan.rs:894`. No optimizer-level rewriting; metadata
  scans are always single-instance.

### 4.4 `src/connector/iceberg/compact.rs`

- In `execute_whole_table_rewrite`, branch on
  `metadata.format_version() >= FormatVersion::V3 && row_lineage_enabled(metadata)`.
- When the branch is taken:
  - SELECT projection includes the two reserved meta columns. The exact builder
    is the same one used by COW / MOR UPDATE — extract the helper if not yet
    shared, or call it directly.
  - Writer call switches to `write_row_lineage_batches_as_data_files`.
  - `data_file_to_written_file` (`compact.rs:287`) preserves the writer-supplied
    `first_row_id` (which is `None` in this path).
- `row_lineage_enabled(metadata)` reads `write.row-lineage` table property; the
  helper already exists or is trivial to add (look up `properties.get("write.row-lineage")`
  and parse `true`/`false`, default `true` for V3 per spec).

### 4.5 `src/connector/iceberg/commit/rewrite_data_files.rs`

- Around the existing `next_row_id` allocation (`rewrite_data_files.rs:63`,
  `:273`):
  - Pre-flight scan of the to-be-added data files; if every added file has
    `first_row_id().is_none()`, skip the allocation entirely.
  - Otherwise (classic path, no row-lineage), keep the existing fetch-add. The
    classic path is also responsible for assigning sequential `first_row_id`s to
    each added file — that loop is gated by the same condition.
- The commit must still emit valid manifest entries; an absent `first_row_id` is
  represented per Iceberg V3 spec as `null` in the manifest, which the existing
  manifest writer already supports for COW UPDATE.

### 4.6 No changes

The following modules are explicitly **not** modified:

- `src/exec/operators/scan/runner.rs` (stored-column read already supported)
- `src/connector/iceberg/changes.rs` (Replace-snapshot skip already correct)
- `src/connector/iceberg/data_writer.rs` (writer already exists)
- `src/connector/starrocks/managed/mv_refresh_*.rs` (IVM logic unchanged)
- `src/connector/iceberg/metadata.rs` BE side (already complete)

## 5. SQL regression tests

All three files live under `sql-tests/iceberg/sql/`. The test harness conventions
(verifyable golden output + comment style) match the existing v3 row-lineage
suite (`iceberg_v3_update_cow.sql`, `iceberg_v3_update_mor.sql`, etc.).

### 5.1 `iceberg_v3_optimize_row_lineage.sql`

End-to-end OPTIMIZE row-identity preservation. Steps:

1. Create a v3 row-lineage table with `format-version=3`, `write.row-lineage=true`.
2. Multiple INSERTs spread across snapshots so OPTIMIZE has > 1 input file.
3. UPDATE one row (forces COW write or MOR DV); DELETE one row (DV update).
4. Capture `(id, _row_id, _last_updated_sequence_number)` for all surviving rows.
5. `OPTIMIZE TABLE t`.
6. Re-capture the same triple set; assert it equals (4) row-by-row.
7. Assert the data file count strictly decreased. Read the latest snapshot's
   `summary` map from `t$snapshots` and inspect `total-data-files` (or
   `added-data-files` / `removed-data-files` deltas on the `replace` snapshot).
   Do **not** rely on `t$files`; that table is out of scope per D6.
8. Define an aggregate MV pre-step (4'); REFRESH; OPTIMIZE; REFRESH again;
   assert MV contents identical pre/post OPTIMIZE — proves IVM treated OPTIMIZE
   as no-op.
9. UPDATE one row again post-OPTIMIZE; REFRESH MV; assert the MV reflects the
   single row's new value — proves `_row_id` continuity enables IVM to find the
   row in MV state.

### 5.2 `iceberg_v3_branch_row_lineage.sql`

Branch / tag row-id isolation. Steps:

1. Create v3 row-lineage table; INSERT 3 rows on main; capture R0 = main's
   `(id, _row_id)` set.
2. `ALTER TABLE t CREATE BRANCH feat`.
3. INSERT 1 row, UPDATE 1 row, DELETE 1 row on `t.branch_feat`.
4. Assert main's `(id, _row_id)` set = R0 (branch did not perturb main).
5. Assert branch view: untouched rows still carry their R0 `_row_id`; updated
   row keeps its R0 `_row_id` (V3 row-lineage preserves identity through UPDATE);
   inserted row gets a fresh row-id ≥ pre-branch `next_row_id`.
6. `ALTER TABLE t CREATE TAG snap1` (on main); confirm `t.tag_snap1` reads
   match R0.
7. Time-travel: `SELECT ... FROM t FOR VERSION AS OF <main_initial_snapshot>`
   returns R0 unchanged.

(OPTIMIZE on branch is **not** tested per D7.)

### 5.3 `iceberg_v3_row_lineage_uniqueness.sql`

Cross-snapshot row-id uniqueness invariant, single-engine. Steps:

1. Create v3 row-lineage table.
2. Multiple INSERT/UPDATE/DELETE/OPTIMIZE rounds, including:
   - INSERT a batch
   - DELETE some rows
   - UPDATE some rows
   - OPTIMIZE
   - INSERT more
   - UPDATE more
   - OPTIMIZE again
3. **Invariant 1 (intra-snapshot):**
   `SELECT count(*) = count(DISTINCT _row_id) FROM t` — must hold at every
   intermediate snapshot. The test runs this check after each OPTIMIZE.
4. **Invariant 2 (cross-snapshot):** for each logical row identity (the user's
   PK column), its `_row_id` is stable across every snapshot in which it exists.
   - Step a: capture all snapshot ids by `SELECT snapshot_id FROM t$snapshots
     ORDER BY committed_at`.
   - Step b: union historical projections:
     ```sql
     SELECT id, _row_id, <snap> AS snap FROM t FOR VERSION AS OF <snap1>
     UNION ALL
     SELECT id, _row_id, <snap> AS snap FROM t FOR VERSION AS OF <snap2>
     ...
     ```
     The snapshot ids are produced as part of the test (deterministic by setup).
   - Step c: `GROUP BY id` and assert
     `count(DISTINCT _row_id) = 1` for every id present in ≥ 1 snapshot.
5. **Invariant 3 (`next_row_id` upper bound):**
   `SELECT max(_row_id) FROM t` < table's `next_row_id`. The four metadata
   tables in scope do not expose `next_row_id` directly, so this invariant is
   enforced by a Rust unit test next to the row-id allocator (e.g., in
   `src/connector/iceberg/commit/`) rather than by SQL. The unit test asserts
   that after OPTIMIZE the `next_row_id` counter has not advanced and that
   every existing row's `_row_id` is < counter. If a future PR exposes
   `next_row_id` via a metadata column, the invariant migrates back into the
   SQL test naturally.

### 5.4 Phase A standalone tests

Four small new files exercising the metadata routes themselves:

- `sql-tests/iceberg/sql/iceberg_metadata_snapshots.sql`
- `sql-tests/iceberg/sql/iceberg_metadata_history.sql`
- `sql-tests/iceberg/sql/iceberg_metadata_refs.sql`
- `sql-tests/iceberg/sql/iceberg_metadata_partitions.sql`

Each sets up a small table with a couple of INSERTs / a branch / a partitioned
write as relevant, then `SELECT *` from the metadata variant and asserts the
expected schema + key fields.

## 6. Risks and Mitigations

| Risk | Likelihood | Impact | Mitigation |
|---|---|---|---|
| Pre-parse `$` rewrite collides with sqlparser parameter placeholders | medium | high | Verify the StarRocks dialect's `$<digit>` handling. If placeholders are enabled, scope the rewrite to identifier context only (preceded by an identifier, followed by a known metatype keyword). Falls back to a syntax error if context doesn't match. |
| `validate_replace_snapshot` becomes too strict if OPTIMIZE re-orders rows | low | high | Existing validation only checks row counts and schema_id, not row content. Re-ordering is invisible to it. Confirmed by reading `changes.rs:430-493`. |
| Stored `_row_id` column inflates rewritten file size | medium | low | 16 B/row raw; `_last_updated_sequence_number` is highly clusterable (RLE-friendly in parquet). Empirical check during impl: compare file size pre/post on a TPC-H-scale fixture and document. If unacceptable, future optimisation can switch to delta-coded row id within page. |
| Test C3 Invariant 2 grows quadratically in snapshot count | low | low | Cap test setup at ≤ 8 snapshots (still validates the invariant); the 8 `FOR VERSION AS OF` projections are cheap. |
| `t.branch_<x>$snapshots` accidentally accepted | low | medium | Analyzer rejects mixing branch suffix with `__nr_meta_*__` suffix (D5 explicit). |
| OPTIMIZE on a v3 row-lineage table without the `write.row-lineage=true` property still ends up in the row-lineage path | low | medium | The capability check (D3) tests `write.row-lineage` explicitly, defaulting per Iceberg V3 spec (which is `true`); the property is read once when entering OPTIMIZE. |
| `data_file_to_written_file` needs to flow `first_row_id=None` | low | medium | Already handled in COW UPDATE path; same code path is reused. |
| **`validate_replace_snapshot` semantics on tables with pre-existing DVs.** If `total-records` in a snapshot summary counts physical rows (including DV-deleted ones), then OPTIMIZE — which strips DVs and rewrites only live rows — will reduce `total-records` and trip `ReplaceValidationFailed`, sending IVM into `MvRefreshPolicy::Unsupported`. That defeats the purpose of preserving `_row_id`. | medium | high | Plan stage's first task is to read `validate_replace_snapshot` (`src/connector/iceberg/changes.rs:430-493`) and confirm whether `total-records` is logical (live row count) or physical. If logical: no action. If physical: extend the validation to compare `total-records + deleted-records` (or equivalent) so DV-laden inputs still pass. C1 step 8 (REFRESH MV after OPTIMIZE asserts no MV change) is the catch-all guard regardless of which interpretation is correct. |

## 7. Out-of-scope follow-ups

- Cross-engine row-id invariant (§17): Spark / Trino / PyIceberg ↔ NovaRocks
  cross-write fixture.
- OPTIMIZE on branch (§8.2 / §11): branch-targeted rewrite once `compact.rs`
  learns about `target_ref`.
- Incremental / partition-scoped OPTIMIZE (§11): keep current whole-table only.
- `t$files` / `t$manifests` / `t$logical_iceberg_metadata` SQL exposure (§9):
  same routing pattern, separate PR.
- `FOR VERSION AS OF <snapshot_id>` already supports numeric ids; if test C3
  needs ergonomic helpers like `for_each(snapshot_id) { ... }` it's a separate
  language-level effort.

## 8. Open Items (resolved during planning)

- **Pre-parse rewrite insertion point.** Whether the `$<metatype>` normalizer
  lives in the same function as `__nr_ref:` rewriting or as a new pass is a
  plan-time decision. Constraint: must run before sqlparser tokenisation and
  preserve correct identifier quoting / case handling.
- **`row_lineage_enabled` helper location.** New helper in
  `src/connector/iceberg/catalog/` or a method on the loaded-table wrapper.
  Convention: extend the existing wrapper if one exists; otherwise add a
  free function next to `format_version` accessor.
- **Test C3 Invariant 3 placement.** Resolved: lives as a Rust unit test next
  to the row-id allocator (D8/§5.3). The four metadata tables in scope do not
  expose `next_row_id`, so a SQL-only assertion is not feasible in this PR.
