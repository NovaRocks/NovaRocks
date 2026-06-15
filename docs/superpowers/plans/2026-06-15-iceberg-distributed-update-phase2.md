# Phase 2: Distributed UPDATE (COW rewrite + MOR-update DV→BE) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make standalone `UPDATE` fully BE-written (FE commits metadata only) for both modes — COW via a distributed whole-file rewrite, MOR via the Phase-1 `DeletionVectors` sink — and extend `RowDeltaDvFromFiles` to carry data files so MOR-update commits new-data + DV in one snapshot. This is the shared dependency before the Phase 3 atomic MERGE.

**Architecture:** Reuse Phase 1 wholesale. MOR-update = existing `RowLineageData` data sink + a new `DeletionVectors` DV-sink query (per-`_file` shuffle), both → one extended-`RowDeltaDvFromFiles` commit (drops the coordinator `inject_delete_group` + `RowDeltaDvCommit` Puffin-at-commit). COW-update = per-touched-file `ExplicitFiles`-scoped rewrite query → `RowLineageData` sink → `CowUpdateCommit` (Overwrite), with the `CowUpdateRewriteSet` built from the distributed write output instead of a local rewrite.

**Tech Stack:** Rust; reuses Phase 1 (`IcebergWriteSinkMode::{DeletionVectors,RowLineageData}`, `iceberg_write_shuffle_by_output_index`, `RowDeltaDvFromFilesCommit`, `DistributedDvDeleteWriteExecutor`); `ScanSource::IcebergDataFiles{ExplicitFiles}` for scoped scans; sql-test-runner.

**Profile note:** `cargo test --lib` (profile `dev`) for unit/guard tests; `--profile dev-opt` for SQL-suite runs.

**Test-harness note (corrected):** standalone is single-process by default; "distributed" = the coordinated-plan write path + per-`_file` shuffle, which runs identically under the in-process dispatcher. **Primary correctness gate = all-in-one byte-identical** against the recorded `.result`. `--cluster-mode cross-process --cluster-size 2` is the additional cross-process check **if** the harness supports the `iceberg-dml` fixture cross-process (as Phase 1's `dv_delete_distributed.sql` did) — confirm during Task 5, don't assume.

---

## File Structure

| File | Change |
|---|---|
| `src/connector/iceberg/commit/row_delta_dv_from_files.rs` | extend to accept `content==Data` written files (partition + `written` field + data-manifest append + `added_data_records`) |
| `src/engine/mutation_flow.rs` | MOR-update: add DV-sink query + run both sinks + switch to `RowDeltaDvFromFiles`, drop inject. COW-update: replace `run_cow_update_write` with distributed rewrite; delete local rewrite helpers |
| `sql-tests/iceberg-dml/sql/update_mor.sql`, `update_cow.sql` | behavioral baselines (already exist) — must stay byte-identical |

Reused unchanged: `CowUpdateCommit`/`CowUpdateTxnAction` (`commit/update_cow.rs`), `build_row_lineage_data_sink_spec`, `build_position_delete_sink_spec`, the runner/commit-executor, `select_iceberg_update_mode`, `ScanSource::IcebergDataFiles`.

---

## Task 1: Extend `RowDeltaDvFromFiles` to carry data files (prerequisite)

**Files:** `src/connector/iceberg/commit/row_delta_dv_from_files.rs` (commit `:55-120`, txn action struct `:122-133`, txn commit `:135-340`). Template to port: `row_delta_dv.rs:290-320` (data-manifest block), `:364-384` (`added_data_records`/`dv_summary`), `:465-474` (`mark_replacement_manifest_row_id_assigned`).
**Test:** inline `#[cfg(test)] mod tests` in `row_delta_dv_from_files.rs`.

- [ ] **Step 1: Failing unit test** — assert a mixed batch partitions correctly and the data path is reached.

```rust
#[test]
fn commit_partitions_data_and_dv_written_files() {
    // A Data WrittenFile + a Puffin-DV WrittenFile must both be accepted:
    // DV -> dv_descriptor_from_written; Data -> the new `written` data path.
    let data = sample_written_file(DataContentType::Data, DataFileFormat::Parquet);
    let dv = sample_written_dv_file(); // content=PositionDeletes, format=Puffin, offset/size/cardinality set
    let (dvs, data_files) = partition_written_for_dv_from_files(vec![data.clone(), dv.clone()]).unwrap();
    assert_eq!(dvs.len(), 1);
    assert_eq!(data_files.len(), 1);
    assert_eq!(data_files[0].content, DataContentType::Data);
    // A non-Data, non-Puffin file must be rejected.
    let bad = sample_written_file(DataContentType::PositionDeletes, DataFileFormat::Parquet);
    assert!(partition_written_for_dv_from_files(vec![bad]).is_err());
}
```

(Add `sample_written_file`/`sample_written_dv_file` helpers in the test mod — mirror existing `WrittenFile`-constructing tests in this file, e.g. around `:602-659`.)

- [ ] **Step 2: Run — expect FAIL** (`partition_written_for_dv_from_files` undefined).

Run: `cargo test --lib commit::row_delta_dv_from_files::tests::commit_partitions_data_and_dv_written_files`

- [ ] **Step 3: Implement the partition helper + wire into `commit`**

```rust
fn partition_written_for_dv_from_files(
    written: Vec<WrittenFile>,
) -> Result<(Vec<WrittenDvFile>, Vec<WrittenFile>), String> {
    let mut dvs = Vec::new();
    let mut data = Vec::new();
    for f in written {
        match (f.content, f.format) {
            (DataContentType::PositionDeletes, DataFileFormat::Puffin) => dvs.push(dv_descriptor_from_written(&f)?),
            (DataContentType::Data, _) => data.push(f),
            (c, fmt) => return Err(format!(
                "RowDeltaDvFromFilesCommit: unexpected written file {} (content {:?}, format {:?})", f.path, c, fmt)),
        }
    }
    Ok((dvs, data))
}
```

In `RowDeltaDvFromFilesCommit::commit` (`:58-92`), replace the `written.iter().map(dv_descriptor_from_written)` block with `let (written_dvs, written_data) = partition_written_for_dv_from_files(written)?;`, keep `validate_unique_referenced_files(&written_dvs)?;`, and pass `written: written_data` into the action.

- [ ] **Step 4: Add `written` to the txn action + the data-manifest block**

Add field to `RowDeltaDvFromFilesTxnAction` (`:122-133`): `written: Vec<WrittenFile>,`. In `RowDeltaDvFromFilesTxnAction::commit`, **after the added-DV-manifest loop ends (after `:225`)**, insert verbatim the data-manifest block from `row_delta_dv.rs:290-320` (the `if !self.written.is_empty() { ... super::overwrite::write_added_data_manifest(...) ... new_manifests.push(mark_replacement_manifest_row_id_assigned(...)) }`). Add a local `mark_replacement_manifest_row_id_assigned` (copy `row_delta_dv.rs:465-474`) or import it.

- [ ] **Step 5: Feed `added_data_records` into the summary** — replace the literal `0` (3rd arg to `dv_total_records`, `:271`) and `&[]` (2nd arg to `dv_summary`, `:277`):

```rust
let added_data_records = self.written.iter().try_fold(0u64, |s, f|
    s.checked_add(f.record_count).ok_or_else(|| to_iceberg_unexpected("added data record overflow".into())))?;
let total_records = dv_total_records(snapshot_total_records(m, parent_snapshot_id).map_err(to_iceberg_unexpected)?,
    newly_deleted_records, added_data_records).map_err(to_iceberg_unexpected)?;
let mut dv_props = dv_summary(&self.written_dvs, &self.written, total_records, newly_deleted_records,
    index.replaced_delete_files, index.replaced_delete_records).map_err(to_iceberg_unexpected)?;
```

- [ ] **Step 6: Run unit tests + the existing source guard**

Run: `cargo test --lib commit::row_delta_dv_from_files`
Expected: PASS, including `source_does_not_call_puffin_read_or_write_helpers` (`:595`) — the data-manifest path touches no Puffin helpers.

- [ ] **Step 7: Commit**

```bash
git add src/connector/iceberg/commit/row_delta_dv_from_files.rs
git commit -m "feat(commit): RowDeltaDvFromFiles also appends BE-written data files in one snapshot"
```

---

## Task 2: MOR-update — move DV side to the BE `DeletionVectors` sink

**Files:** `src/engine/mutation_flow.rs` — `MorUpdateDistributedWrite` (`:614-617`), `build_update_mor_distributed_write` (`:244-301`), `DistributedMorUpdateExecutor` (`:619-664`), `run_mor_update_distributed_transaction` (`:666-732`), `execute_mor_update` (`:553-612`).

- [ ] **Step 1: Failing guard test** (mirror `dv_delete_uses_distributed_dv_sink_not_local_collect`, `delete_flow.rs:1870`)

```rust
#[test]
fn mor_update_uses_be_dv_sink_not_coordinator_inject() {
    let src = include_str!("mutation_flow.rs");
    let body = src.split("fn execute_mor_update").nth(1).expect("fn")
        .split("\nfn ").next().expect("body");
    assert!(!body.contains("build_position_delete_groups_from_matched"),
        "MOR-update must not materialize position groups on the coordinator");
    assert!(!body.contains("inject_delete_group"),
        "MOR-update must not inject DV groups (FE central write)");
    let run = src.split("fn run_mor_update_distributed_transaction").nth(1).expect("fn")
        .split("\nfn ").next().expect("body");
    assert!(run.contains("RowDeltaDvFromFiles"),
        "MOR-update must commit via RowDeltaDvFromFiles (BE-written DV)");
    assert!(!run.contains("CommitOpKind::RowDeltaDv "),
        "MOR-update must not use the coordinator-merge RowDeltaDv commit");
}
```

- [ ] **Step 2: Run — expect FAIL.**

- [ ] **Step 3: Add the DV-sink query to the write plan.** Extend `MorUpdateDistributedWrite`:

```rust
struct MorUpdateDistributedWrite {
    data_query: sqlparser::ast::Query,
    data_sink_spec: IcebergWriteSinkSpec,
    dv_query: sqlparser::ast::Query,          // NEW: old-row positions
    dv_sink_spec: IcebergWriteSinkSpec,       // NEW: DeletionVectors mode
}
```

In `build_update_mor_distributed_write` (and `build_merge_mor_distributed_write`), after building `data_sink_spec`, build the DV side:

```rust
let mut dv_sink_spec = crate::engine::iceberg_writer::build_position_delete_sink_spec(target, &resolved, &table, &entry)?;
dv_sink_spec.mode = IcebergWriteSinkMode::DeletionVectors;
dv_sink_spec.set_planned_snapshot_id(base_snapshot_id)?;
// old-row positions: SELECT _file,_pos[,part] FROM target [FOR VERSION AS OF] [CROSS JOIN source] WHERE <pred>
let dv_query = build_update_dv_sink_query(target, target_alias, source_sql.as_deref(),
    where_sql.as_deref(), &dv_sink_spec.target_columns, target_ref)?;
```

Add `build_update_dv_sink_query` — same projection shape as `delete_flow::build_delete_position_sink_query` (project `dv_sink_spec.target_columns` = `[_file,_pos,<part>]`, `_file` first) but with the UPDATE's `CROSS JOIN source + WHERE <pred>` tail (reuse `build_update_distributed_select_sql`, `:489-523`).

- [ ] **Step 4: Run both sinks in `run_coordinated_write`.** `DistributedMorUpdateExecutor::run_coordinated_write` runs the data write (shuffle `None`) AND the DV write (`Some(iceberg_write_shuffle_by_output_index(0))`), merging both `write_commit`s' `sink_commit_infos` into the returned result so the commit executor injects both:

```rust
let data = crate::engine::execute_query_as_iceberg_write(&self.state, Some(&self.target.catalog),
    &self.target.namespace, &self.write.data_query, self.write.data_sink_spec.clone(), None, None)?;
let dv = crate::engine::execute_query_as_iceberg_write(&self.state, Some(&self.target.catalog),
    &self.target.namespace, &self.write.dv_query, self.write.dv_sink_spec.clone(), None,
    Some(crate::engine::iceberg_write_shuffle_by_output_index(0)))?;
// merge dv.write_commit writers into data.write_commit (both feed one collector); propagate any write_abort.
let merged = merge_write_commits(data, dv)?;
Ok(merged)
```

Add `merge_write_commits` (concatenates the two `WriteCommitInput.writers`; if either has `write_abort`, return it). The collector's `commit_write_input` already converts+injects all writers' `sink_commit_infos` (data → `content=Data`, DV → `content=PositionDeletes/Puffin`) — Task 1 makes the commit accept both.

- [ ] **Step 5: Switch the commit op + drop the inject.** In `execute_mor_update`: remove `load_referenced_data_file_partitions_at` + `build_position_delete_groups_from_matched`; build the collector with `CommitOpKind::RowDeltaDvFromFiles`. In `run_mor_update_distributed_transaction`: remove the `for group in delete_groups { collector.inject_delete_group(group); }` loop and the `delete_groups` param; set `commit_op_kind: CommitOpKind::RowDeltaDvFromFiles`.

- [ ] **Step 6: Run guard + module tests**

Run: `cargo test --lib engine::mutation_flow::tests`
Expected: PASS.

- [ ] **Step 7: Byte-identical SQL check** (server up):

Run: `... --suite iceberg-dml --only update_mor --mode verify`
Expected: PASS — same `update_mor.result` (UPDATE result + `_row_id` preserved). Proves the BE-DV path is behavior-equivalent.

- [ ] **Step 8: Commit**

```bash
git add src/engine/mutation_flow.rs
git commit -m "feat(update): MOR-update writes DV on BE via DeletionVectors sink + RowDeltaDvFromFiles"
```

---

## Task 3: COW-update — distributed whole-file rewrite

**Files:** `src/engine/mutation_flow.rs` — replace `MutationWriteExecutor::run_cow_update_write` (`:863-906`); reuse `build_row_lineage_data_sink_spec`; use `ScanSource::IcebergDataFiles{ExplicitFiles}` (`src/sql/catalog.rs:362`).

**Approach (per-touched-file write — clear old→new mapping, reuses `CowUpdateCommit` unchanged):** for each touched file, run ONE distributed rewrite write scoped to just that file via `ExplicitFiles`; that write's output files = that old file's replacements. (Perf note: N touched files → N coordinated writes; acceptable v1 since COW rewrites whole files anyway. Optimization — a single write sharded by `_file` with source-file-tagged outputs — is deferred; see Risks.)

- [ ] **Step 1: Failing guard test**

```rust
#[test]
fn cow_update_uses_distributed_rewrite_not_local_scan() {
    let src = include_str!("mutation_flow.rs");
    let body = src.split("fn run_cow_update_write").nth(1).expect("fn")
        .split("\n    fn ").next().expect("body");
    assert!(body.contains("execute_query_as_iceberg_write"),
        "COW-update must rewrite via the distributed write path");
    assert!(!body.contains("write_cow_update_files") && !body.contains("build_cow_rewrite_batches"),
        "COW-update must not run the local in-process rewrite");
}
```

- [ ] **Step 2: Run — expect FAIL.**

- [ ] **Step 3: Build per-touched-file rewrite query + `IcebergDataFileInfo`.** For the touched set (`matched.file_paths` distinct), build per-file `IcebergDataFileInfo { path, first_row_id, data_sequence_number, partition_spec_id, delete_files, ... }` using the existing FE loaders (`load_data_file_lineage` `:1453`, `load_existing_delete_visibility_by_data_file`, `load_referenced_data_file_partitions`). The rewrite query (whole-file, SET via CASE on the WHERE predicate):

```rust
// SELECT
//   CASE WHEN <where_pred> THEN (<set_expr>) ELSE <col> END AS <col>, ...   -- per user column
//   _row_id AS _row_id,
//   CASE WHEN <where_pred> THEN <new_seq> ELSE _last_updated_sequence_number END AS _last_updated_sequence_number
// FROM <scan bound to ScanSource::IcebergDataFiles{ files:[this file], binding: ExplicitFiles }>
// ORDER BY _row_id
```

Add `build_cow_rewrite_query(target, stmt, file_info, target_columns, new_seq)`. Sink = `build_row_lineage_data_sink_spec(...)`. (The `ExplicitFiles` binding scopes the scan to exactly this file — `catalog.rs:362-371`; this is how the BE reads the whole file + applies updates + writes new files.)

- [ ] **Step 4: Replace `run_cow_update_write`.** For each touched file: `execute_query_as_iceberg_write(rewrite_query, row_lineage_sink_spec, None, None)`; collect that write's output file paths (from `write_commit`) as that old file's `new_files`. Build `CowUpdateTouchedFile { old_file, new_files, row_ids }` (row_ids from the matched rows for that file). Assemble `CowUpdateRewriteSet { base_snapshot_id, target_table_uuid, updated_row_ids, touched_data_files }`. Inject all written files into the collector (already happens via each write's `write_commit` → `commit_write_input`) and stash the rewrite set in `cow_update_rewrite`. The `CowUpdateCommit` (Overwrite: remove old + add new) is **unchanged**.

  Model the executor on `DistributedMorUpdateExecutor` (`:619-664`); the commit path (`CommitOpKind::CowUpdate` → `CowUpdateCommit`, `run.rs:104`) is reused as-is.

- [ ] **Step 5: Run guard + module tests; Step 6: Byte-identical SQL check**

Run: `cargo test --lib engine::mutation_flow::tests` then `... --suite iceberg-dml --only update_cow --mode verify`
Expected: PASS — same `update_cow.result`.

- [ ] **Step 7: Commit**

```bash
git add src/engine/mutation_flow.rs src/sql/catalog.rs
git commit -m "feat(update): distributed COW whole-file rewrite via ExplicitFiles scan + RowLineageData sink"
```

---

## Task 4: Delete the local COW rewrite code

**Files:** `src/engine/mutation_flow.rs`

- [ ] **Step 1: Delete** the now-unused local rewrite engine: `write_cow_update_files` (`:1206-1246`), `build_cow_rewrite_batches` (`:1260-1352`), `collect_cow_rewrite_rows_from_batch` (`:1354-1422`), `user_batch_from_scan_batch` (`:1424-1445`), `CowRewriteFile`/`CowRewriteAccumulator` (`:1248-1258`). Keep `load_data_file_lineage` / `load_existing_delete_visibility_by_data_file` / `load_referenced_data_file_partitions` (now feed the per-file `IcebergDataFileInfo`). Keep `build_position_delete_groups_from_matched` **only if** MERGE (Phase 3) still references it — grep; if MOR-update was its last caller, move its removal to Phase 3.

Run: `rg "write_cow_update_files|build_cow_rewrite_batches|CowRewriteAccumulator" src/` → expect no hits.

- [ ] **Step 2: Build + tests + guards**

Run: `cargo build --lib && cargo test --lib engine::mutation_flow::tests`
Expected: PASS; no `unused` warnings.

- [ ] **Step 3: Commit**

```bash
git add src/engine/mutation_flow.rs
git commit -m "refactor(update): remove dead local COW rewrite path"
```

---

## Task 5: End-to-end verification

- [ ] **Step 1: All-in-one byte-identical (primary gate)**

Run: `... --suite iceberg-dml --only update_cow,update_mor --mode verify`
Expected: PASS — identical to recorded `.result`s.

- [ ] **Step 2: Regression sweep** (MERGE update branches reuse MOR/COW; ensure unbroken)

Run: `... --suite iceberg-dml --only merge_into_cow,merge_into_mor,merge_into_upsert_delete --mode verify`
Expected: PASS.

- [ ] **Step 3: Cross-process check (if harness supports iceberg-dml cross-process)**

```bash
cargo build --profile dev-opt
NOVAROCKS_BIN=target/dev-opt/novarocks NO_PROXY=127.0.0.1,localhost \
cargo run --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests -- \
  --config "$NOVAROCKS_SQL_TEST_CONFIG" --suite iceberg-dml --only update_cow,update_mor \
  --mode verify --cluster-mode cross-process --cluster-size 2 -j 1
```
Expected: PASS if supported. If the iceberg-dml fixture isn't wired cross-process, record that and rely on the all-in-one gate (the BE-write logic is identical under both dispatchers).

- [ ] **Step 4: Commit** (only if `.result` updates were needed — there should be none)

```bash
git commit -am "test(iceberg-dml): verify distributed UPDATE (COW + MOR)" || echo "nothing to commit"
```

---

## Acceptance (Phase 2 done when all true)

- [ ] `mor_update_uses_be_dv_sink_not_coordinator_inject` + `cow_update_uses_distributed_rewrite_not_local_scan` guards pass.
- [ ] `update_mor.sql` + `update_cow.sql` pass all-in-one, byte-identical to baseline.
- [ ] `merge_into_*` regressions green.
- [ ] FE writes no DV/data files for UPDATE: DV via BE `DeletionVectors` sink; COW rewrite files via BE `RowLineageData` sink; FE only commits manifests.
- [ ] `RowDeltaDvFromFiles` commits data + DV in one snapshot (Task 1 unit test).
- [ ] `cargo fmt` + `cargo clippy --lib` clean.

## Out of scope (later phases)

- **Phase 3 — atomic MERGE**: reuses Task 1's data+DV `RowDeltaDvFromFiles` and Task 3's COW rewrite; folds all MERGE branches into one collector → one commit (honoring `write.update.mode`).
- equality-delete (Phase 4); deletion 收口 incl. `local_writer_commit_input` / `has_preloaded_commit_output` (Phase 5).

## Risks

1. **COW old→new mapping**: per-touched-file write keeps the mapping unambiguous and reuses `CowUpdateCommit` as-is, at the cost of N coordinated writes per UPDATE. The single-write-sharded-by-`_file` optimization requires the sink to tag each output file with its source `_file` (not currently supported) + aggregating `CowUpdateRewriteSet` — defer until perf demands it.
2. **`ExplicitFiles` rewrite must read the whole file** (matched + unmatched rows), applying SET only to matched rows via CASE on the WHERE predicate — confirm the scan over an `ExplicitFiles`-bound source surfaces `_file`/`_pos`/`_row_id` and honors pre-existing deletes (Phase 1 `read_existing_dv_positions` proves the read-view is constructible).
3. **MOR-update double-read**: the data sink (matched rows → new data) and the DV sink (matched rows → old positions) both scan with the same WHERE; acceptable (each writes a different output). The match is no longer materialized to the coordinator for the DV side.
4. **`build_position_delete_groups_from_matched` lifetime**: keep until Phase 3 if MERGE still uses it.
