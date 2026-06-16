# Phase 3: Atomic Distributed MERGE Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make a standalone Iceberg `MERGE` statement commit as **exactly one Iceberg snapshot** — folding its not-matched-INSERT and matched (UPDATE xor DELETE) branches into a single distributed-write + single metadata commit — while honoring `write.update.mode` (COW table → Overwrite; MOR table → RowDelta) and keeping every data/DV file BE-written (FE commits metadata only).

**Architecture:** Reuse Phase 1 (DV sink + `RowDeltaDvFromFiles`) and Phase 2 (distributed MOR/COW UPDATE, `merge_write_commits`, `CowUpdateCommit`) wholesale. The fix is structural: today `execute_merge_statement` runs the INSERT branch and the matched branch as **two independent `IcebergWriteTransactionRunner::run` calls** (`iceberg_writer.rs:254` FastAppend + a separate matched runner), producing up to 2 snapshots. Phase 3 introduces **one shared `IcebergCommitCollector` + a new multi-branch `DistributedMergeExecutor`** whose single `run_coordinated_write` runs all active branches into that collector and whose `commit` fires once. Two prerequisites unblock the fold: (M1) extend the COW commit to also append net-new INSERT data files, and (M2) move matched-DELETE off the coordinator `inject_delete_group` path onto a BE `DeletionVectors` sink (the unified `RowDeltaDvFromFilesCommit` rejects coordinator delete groups).

**Tech Stack:** Rust; reuses `IcebergWriteSinkMode::{DeletionVectors,RowLineageData,Data}`, `iceberg_write_shuffle_by_output_index(0)`, `execute_query_as_iceberg_write`, `RowDeltaDvFromFilesCommit`, `CowUpdateCommit`, `materialize_merge_match`; sql-test-runner with `<tbl>$snapshots` metadata-table queries for snapshot-count assertions.

**Profile note:** `cargo test --lib` (profile `dev`) for unit/guard tests; `--profile dev-opt` only for cross-process SQL runs.

**Test-harness note:** standalone is single-process by default; "distributed" = the coordinated-plan write path + per-`_file` shuffle, which runs identically under the in-process dispatcher. **Primary correctness gate = all-in-one byte-identical** against recorded `.result` goldens. Atomicity is asserted in-SQL via `SELECT count(*) FROM …$snapshots` before/after each MERGE (golden = exactly +1). Cross-process (`--cluster-mode cross-process --cluster-size 2`, runner owns the server via `NOVAROCKS_BIN`) is the extra FE/BE-separation check.

---

## Current-state anchors (verified against `claude/iceberg-atomic-merge-phase3`, base = main @ 20fcc608)

- `execute_merge_statement` — `src/engine/mutation_flow.rs:2076-2232`. Materializes match once (`materialize_merge_match`, `:2136`), then runs **NOT-MATCHED INSERT** (`execute_merge_unmatched_insert`, `:2146-2163`) and **MATCHED** (`:2165-2228`) as separate transactions.
- The 2-commit defect — commit site A: `execute_merge_unmatched_insert` (`:2733-2763`) → `execute_iceberg_insert_or_overwrite` → own runner at `iceberg_writer.rs:254` (FastAppend). Commit site B (one of): matched-UPDATE-MOR `run_mor_update_distributed_transaction` runner `mutation_flow.rs:1001`; matched-UPDATE-COW `run_cow_update_distributed_transaction` runner `:1778`; matched-DELETE `run_mutation_write_transaction` runner `:1184`.
- matched-DELETE coordinator-local path: `execute_merge_matched_delete` (`:2688`) → `MutationWriteExecutor` (`:1047`) → `run_coordinated_write` builds delete groups (`build_position_delete_groups_from_matched`, def `:1006`) + `collector.inject_delete_group` (`:1075`); commits `CommitOpKind::RowDeltaDv` (`:2704`).
- `MutationWritePlan` single-variant placeholder + comment: `:1029-1037`.
- Phase-2 reusable blocks: `MorUpdateDistributedWrite` (`:814`), `build_merge_mor_distributed_write` (`:335`, already wired into MERGE at `:2189`), `DistributedMorUpdateExecutor` (`:824`), `merge_write_commits` (`:906`), `DistributedCowUpdateExecutor` (`:1540`), `build_cow_update_distributed_write` (`:1283`) → `CowUpdateDistributedWrite` (`:1274`), `build_position_delete_sink_spec` (`iceberg_writer.rs:314`), `build_row_lineage_data_sink_spec` (`iceberg_writer.rs:331`), `build_insert_write_sink_spec` (`iceberg_writer.rs:297`).
- Commit ops: `RowDeltaDvFromFilesCommit` (`src/connector/iceberg/commit/row_delta_dv_from_files.rs:53`), `partition_written_for_dv_from_files` (`:517-539`, splits by content type; rejects coordinator delete groups at `:59-65`). `CowUpdateCommit` (`src/connector/iceberg/commit/update_cow.rs:65`) + `CowUpdateRewriteSet` (`:51-56`); rejects non-`Data` written files (`:73-79`). Append-manifest helper: `super::overwrite::write_added_data_manifest`.
- Runner seam: trait `IcebergWriteTransactionExecutor` (`src/engine/write_transaction.rs:88-108`), `IcebergWriteTransactionRunner::run` (`:271-360`, one write → one commit), `IcebergWriteCommitExecutor::commit_write_input` (`:128-166`, converts writers → `inject_written_files` → one `run_iceberg_commit_typed`). Collector additive: `inject_written_files` appends (`collector.rs:225`), `take_written_files` drains (`:289`).
- Cardinality: `validate_unique_target_row_ids` (`:2012-2023`), enforced on coordinator at `:2168`.
- Snapshots metadata-table query syntax (from `sql-tests/iceberg/sql/iceberg_metadata_snapshots.sql`): `SELECT count(*) FROM <cat>.<db>.<tbl>$snapshots;` and `SELECT operation, count(*) FROM …$snapshots GROUP BY operation`.
- Existing MERGE cases: `sql-tests/iceberg-dml/sql/merge_into_{cow,mor,upsert_delete}.sql`. **No `WHEN MATCHED THEN DELETE` case exists. No snapshot-count assertion exists.** The cow/mor `.sql` headers currently *document the 2-snapshot behavior* (comments only — not assertions).

---

## File Structure

| File | Change |
|---|---|
| `src/connector/iceberg/commit/update_cow.rs` | M1: extend `CowUpdateRewriteSet` with `appended_files: Vec<WrittenFile>`; `CowUpdateCommit` adds them as a net-new data manifest alongside the rewrite (Overwrite = remove touched + add rewritten + append). |
| `src/engine/mutation_flow.rs` | M2: matched-DELETE → BE `DeletionVectors` sink query + `RowDeltaDvFromFiles`. M3: new `DistributedMergeExecutor` + N-ary `merge_write_commits` + rewire `execute_merge_statement` to one collector/one runner. M5: delete `MutationWritePlan::MergeMatchedDelete` arm, `MutationWriteExecutor`, `build_position_delete_groups_from_matched`, `run_mutation_write_transaction`. |
| `src/engine/iceberg_writer.rs` | M3: expose an INSERT *write-plan* builder (sink_spec + query) separable from the commit, so the INSERT branch routes through `execute_query_as_iceberg_write` into the shared collector. |
| `sql-tests/iceberg-dml/sql/merge_into_matched_delete.sql` (+ `.result`) | M2/M4: new `WHEN MATCHED THEN DELETE` case (+ `WHEN MATCHED THEN DELETE` *with* not-matched INSERT for the fold). |
| `sql-tests/iceberg-dml/sql/merge_into_{cow,mor,upsert_delete}.sql` (+ `.result`) | M4: add `…$snapshots` count assertions (golden = +1 per MERGE); fix the stale "2 snapshots" header comments. |

Reused **unchanged**: `materialize_merge_match`, `build_merge_mor_distributed_write`, `DistributedMorUpdateExecutor`, `build_cow_update_distributed_write` (assembly extended in M3 to attach appended INSERT files), the DV/data sink builders, `RowDeltaDvFromFilesCommit`, the runner core, `validate_unique_target_row_ids`.

---

## Task M1: Extend `CowUpdateCommit` to append net-new INSERT data files (prerequisite for COW fold)

**Why:** A COW MERGE = matched-UPDATE (rewrite touched files) **+** optional not-matched-INSERT (brand-new data files with no `old_file`). The single Overwrite snapshot must remove touched old files, add rewritten files, **and append the INSERT files**. `CowUpdateRewriteSet` today models only rewrite (touched-file old→new); it has no channel for net-new appended data.

**Files:**
- Modify: `src/connector/iceberg/commit/update_cow.rs` (`CowUpdateRewriteSet` `:51-56`; validation `:73-79`/`:700-713`; the txn-action commit body).
- Test: inline `#[cfg(test)] mod tests` in `update_cow.rs`.

- [ ] **Step 1: Write the failing unit test**

```rust
#[test]
fn cow_update_commit_appends_net_new_data_files() {
    // A rewrite set with one touched file (old -> [new_rewrite]) PLUS one appended
    // INSERT data file must produce: remove(old), add(new_rewrite), add(appended).
    let rewrite = CowUpdateRewriteSet {
        base_snapshot_id: BASE_SNAP,
        target_table_uuid: TABLE_UUID.to_string(),
        updated_row_ids: vec![10, 11],
        touched_data_files: vec![CowUpdateTouchedFile {
            old_file: sample_data_path("old.parquet"),
            new_files: vec![sample_written_file(DataContentType::Data, "new_rewrite.parquet")],
            row_ids: vec![10, 11],
        }],
        appended_files: vec![sample_written_file(DataContentType::Data, "insert.parquet")],
    };
    let plan = build_cow_overwrite_plan(&rewrite).expect("plan");
    assert_eq!(plan.removed_data_files, vec![sample_data_path("old.parquet")]);
    // both the rewritten file and the appended INSERT file are added in this one commit
    let added: Vec<_> = plan.added_data_files.iter().map(|f| f.path.clone()).collect();
    assert!(added.contains(&"new_rewrite.parquet".to_string()));
    assert!(added.contains(&"insert.parquet".to_string()));
    // appended files must NOT be required to map to any old_file (validation tolerates them)
    assert!(validate_cow_update_inputs(&rewrite, &collected_written(&rewrite)).is_ok());
}
```

(Add `appended_files` to the helper that constructs the test `CowUpdateRewriteSet` elsewhere in the file — default `vec![]` for existing tests so they stay green.)

- [ ] **Step 2: Run — expect FAIL** (field `appended_files` does not exist).

Run: `cargo test --lib commit::update_cow::tests::cow_update_commit_appends_net_new_data_files`

- [ ] **Step 3: Add the field**

In `CowUpdateRewriteSet` (`update_cow.rs:51-56`) add:

```rust
    /// BE-written data files that are NET-NEW to this commit (e.g. a folded MERGE
    /// not-matched INSERT), not tied to any rewritten `old_file`. Added to the same
    /// Overwrite snapshot alongside the rewrite outputs. Empty for a pure UPDATE.
    pub appended_files: Vec<WrittenFile>,
```

Set `appended_files: Vec::new()` at the existing construction site in `build_cow_update_distributed_write` assembly (mutation_flow.rs — the `CowUpdateRewriteSet { … }` literal) so the UPDATE path is unchanged.

- [ ] **Step 4: Make validation tolerate appended files**

The bidirectional written-vs-`new_files` set-equality check (`:700-713`) currently requires every collected written file to appear in some `new_files`. Appended files are written-but-not-in-any-`new_files`. Update the check so the expected written set = `union(new_files) ∪ appended_files` (both directions). Keep rejecting any file that is in neither. The non-`Data` rejection (`:73-79`) stays — appended files must also be `content==Data`.

- [ ] **Step 5: Add appended files to the added-data manifest**

In the `CowUpdateTxnAction::commit` body, after building the rewrite's added-data manifest, fold `self.rewrite.appended_files` into the same added-data set (reuse the existing `super::overwrite::write_added_data_manifest` call — extend the slice it receives to include appended files; they carry their own `record_count` for the snapshot summary). The removed-files set (touched `old_file`s) is unchanged — appended files remove nothing.

- [ ] **Step 6: Run unit tests**

Run: `cargo test --lib commit::update_cow`
Expected: PASS, including pre-existing COW commit tests (unchanged behavior when `appended_files` is empty).

- [ ] **Step 7: Commit**

```bash
git add src/connector/iceberg/commit/update_cow.rs src/engine/mutation_flow.rs
git commit -m "feat(commit): CowUpdate Overwrite can append net-new data files (folded MERGE INSERT)"
```

---

## Task M2: matched-DELETE → BE `DeletionVectors` sink (off coordinator inject)

**Why:** matched-DELETE is the only branch still writing DVs on the coordinator (`inject_delete_group` → `RowDeltaDv`). The unified `RowDeltaDvFromFilesCommit` rejects coordinator delete groups (`row_delta_dv_from_files.rs:59-65`), so before matched-DELETE can be folded into the shared collector it must produce **BE-written** DV files. This mirrors Phase 2's MOR-update DV-side conversion (E2).

**Files:**
- Modify: `src/engine/mutation_flow.rs` — `execute_merge_matched_delete` (`:2688`) and its transaction (`run_mutation_write_transaction`, runner `:1184`).
- Create: `sql-tests/iceberg-dml/sql/merge_into_matched_delete.sql` (+ record `.result`).
- Test: guard test in `mutation_flow.rs` tests mod.

- [ ] **Step 1: Add the new SQL case (records the behavioral baseline)**

Create `sql-tests/iceberg-dml/sql/merge_into_matched_delete.sql` exercising `WHEN MATCHED THEN DELETE` **without** a not-matched clause first (single-branch, already one snapshot today), modeled on `merge_into_mor.sql`'s setup (v3 table, small rows). Include a post-MERGE row-content check and a `_row_id` check. This is the first such case in the repo.

```sql
-- merge_into_matched_delete.sql
-- v3 MERGE with WHEN MATCHED THEN DELETE only (single matched branch).
-- Phase 3: matched-DELETE writes its deletion vector on the BE (DeletionVectors sink),
-- committed via RowDeltaDvFromFiles. FE commits metadata only.
-- @catalog=<as in merge_into_mor.sql>
-- ... CREATE v3 table, INSERT base rows (ids 1,2,3) ...
MERGE INTO t USING (SELECT 2 AS id) s ON t.id = s.id
  WHEN MATCHED THEN DELETE;
SELECT id, v FROM t ORDER BY id;                 -- rows 1,3 remain
SELECT count(DISTINCT _row_id) = count(*) AS row_ids_unique FROM t;
```

- [ ] **Step 2: Record the golden on CURRENT code, then run to confirm green**

```bash
source docker/iceberg-rest/runtime/current/env.sh && docker/iceberg-rest/up.sh
# start server gated on NOVAROCKS_READY (see CLAUDE.md), then:
cargo run --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests -- \
  --config "$NOVAROCKS_SQL_TEST_CONFIG" --suite iceberg-dml --only merge_into_matched_delete --mode record
cargo run … --only merge_into_matched_delete --mode verify     # PASS on current (pre-conversion) code
```

This locks the row-level baseline so the BE-DV conversion can be proven byte-identical.

- [ ] **Step 3: Failing guard test**

```rust
#[test]
fn merge_matched_delete_writes_dv_on_be_not_coordinator() {
    let src = include_str!("mutation_flow.rs");
    let body = src.split("fn execute_merge_matched_delete").nth(1).expect("fn")
        .split("\nfn ").next().expect("body");
    assert!(!body.contains("build_position_delete_groups_from_matched"),
        "matched-DELETE must not materialize position groups on the coordinator");
    assert!(!body.contains("inject_delete_group"),
        "matched-DELETE must not inject coordinator delete groups (FE central write)");
    assert!(body.contains("DeletionVectors"),
        "matched-DELETE must write its DV via the BE DeletionVectors sink");
}
```

- [ ] **Step 4: Run — expect FAIL.**

Run: `cargo test --lib engine::mutation_flow::tests::merge_matched_delete_writes_dv_on_be_not_coordinator`

- [ ] **Step 5: Convert matched-DELETE to a BE DV sink**

Rewrite `execute_merge_matched_delete` to build a `DeletionVectors` sink query over the matched rows' old-row positions — exactly the MOR-update DV side (E2). Reuse `build_merge_mor_dv_sink_query_from_matched` (`:483`, the inline-VALUES `_file,_pos,<part>` projection from `matched`) + `build_position_delete_sink_spec` with `mode = DeletionVectors` + `set_planned_snapshot_id(base)`. Run it via `execute_query_as_iceberg_write(…, Some(iceberg_write_shuffle_by_output_index(0)))`. Commit via `CommitOpKind::RowDeltaDvFromFiles` (no data files on a pure matched-DELETE — DV only). Add the same "DV sink produced no files" guard E2 added (a non-empty matched batch must yield DV files).

Keep `validate_unique_target_row_ids` on the coordinator (it already runs at `:2168`).

- [ ] **Step 6: Run guard + byte-identical**

Run: `cargo test --lib engine::mutation_flow::tests` then `--only merge_into_matched_delete --mode verify`
Expected: PASS, byte-identical to the M2-Step-2 baseline (same surviving rows + `_row_id`).

- [ ] **Step 7: Commit**

```bash
git add src/engine/mutation_flow.rs sql-tests/iceberg-dml/sql/merge_into_matched_delete.sql sql-tests/iceberg-dml/result/merge_into_matched_delete.result
git commit -m "feat(merge): matched-DELETE writes DV on the BE via DeletionVectors sink + RowDeltaDvFromFiles"
```

---

## Task M3a: Commit-layer — fresh row-ids for appended/INSERT data (prerequisite for the fold)

**Why:** A folded MERGE snapshot mixes two row-lineage classes of data files: UPDATE / COW-rewrite outputs (`RowLineageData` sink, carry explicit `_row_id` → **REUSE**, must NOT advance `next-row-id`) and not-matched-INSERT outputs (`Data` sink, no `_row_id` → **FRESH**, MUST advance `next-row-id`). The snapshot row-range `n` is computed **per-manifest-path** (`fast_append.rs` / `overwrite.rs`: `n = Σ record_count`, then `with_row_range(first_row_id, n)`). M1 added `CowUpdateRewriteSet.appended_files` but routes them through `mark_replacement_manifest_row_id_assigned` (SUPPRESS, `n`-contribution 0) — correct for the empty case but **wrong for net-new INSERT rows**. This task makes BOTH atomic-fold commit ops allocate FRESH ids for INSERT data, in a separate manifest path, mirroring `overwrite.rs`.

**Decision (settled — Iceberg v3 semantics + verified sink behavior):** INSERT rows are net-new and MUST get fresh ids: `first_row_id = effective_n(table.metadata())`, advance by `Σ appended record_count`. The not-matched-INSERT `Data` sink emits NO `_row_id` (`src/sql/codegen/iceberg_write_sink.rs:335,345`), so implicit manifest assignment is the mechanism (same as today's FastAppend INSERT). UPDATE/rewrite `RowLineageData` files keep the existing reuse/suppress path. The structural signal for fresh-vs-reuse is *which channel the file is in* (appended/fresh vs rewrite-or-`written`/reuse), populated by M3b — not a per-file content sniff.

**Files:** `src/connector/iceberg/commit/update_cow.rs` (CowUpdate appended block), `src/connector/iceberg/commit/row_delta_dv_from_files.rs` (add a fresh-INSERT-data channel parallel to E1's reuse `written`), reusing `snapshot_lifecycle_helpers::effective_n` + the `overwrite.rs` fresh-allocation shape (`with_row_range(effective_n, Σ)`).

- [ ] **Step 1: Failing unit test (CowUpdate fresh allocation).** A `CowUpdateRewriteSet` with non-empty `appended_files` (total record_count R) must advance the snapshot row-range by R (i.e., the appended manifest uses `with_row_range(effective_n, R)`), while the rewritten files still REUSE (no advance). Assert the appended manifest's row-range / the snapshot `next-row-id` delta = R. (Reuse the M1 test helpers; you may need a minimal `Table`/metadata stub or assert at the manifest-build level — match how `overwrite.rs`/`fast_append.rs` tests assert row-range.)
- [ ] **Step 2: Run — expect FAIL** (M1 suppresses → advance is 0).
- [ ] **Step 3: CowUpdate — route `appended_files` through FRESH allocation.** In the appended block (the `if !appended_files.is_empty()` path), use `first_row_id = effective_n(ctx.table.metadata())` and `with_row_range(first_row_id, Σ appended record_count)` instead of `mark_replacement_manifest_row_id_assigned(...)`. Keep the rewritten-files manifest on the existing reuse path. Remove the M1 `TODO(M3)` fence now that it's resolved, and update the block comment.
- [ ] **Step 4: Failing unit test (RowDeltaDvFromFiles fresh channel).** A commit carrying reuse-data (E1's `written`, RowLineageData — suppress) PLUS fresh-data (a new `appended_files`/INSERT channel, `Data`, no `_row_id`) must advance `next-row-id` by only the fresh files' `Σ record_count`. The DV-only and reuse-only cases stay byte-identical to E1.
- [ ] **Step 5: Run — expect FAIL** (no fresh channel yet).
- [ ] **Step 6: RowDeltaDvFromFiles — add the fresh-INSERT-data channel.** Add `appended_files: Vec<WrittenFile>` (or equivalently-named fresh-data field) to `RowDeltaDvFromFilesTxnAction`; partition keeps `(PositionDeletes,Puffin)→DV`, `(Data,_)→` whichever channel the caller routed it to. Reuse-data → E1's existing suppress manifest; fresh-data → a `with_row_range(effective_n, Σ)` manifest (mirror the CowUpdate fix + `overwrite.rs`). Empty fresh channel ⇒ byte-identical to E1.
- [ ] **Step 7: Run unit tests for both commit ops** — `cargo test --lib commit::update_cow commit::row_delta_dv_from_files` — PASS (incl. all pre-existing).
- [ ] **Step 8: Commit** — `git commit -m "feat(commit): allocate fresh row-ids for appended INSERT data in CowUpdate + RowDeltaDvFromFiles (atomic MERGE fold)"`.

---

## Task M3b: Multi-branch `DistributedMergeExecutor` — one collector, one commit (the fold)

**Why:** This is the atomicity fix. Replace the two independent `runner.run` calls with one shared collector + one executor whose `run_coordinated_write` runs every active branch and whose `commit` fires once. Uses M3a's fresh-INSERT channels so folded INSERT rows get correct fresh `_row_id`s.

**Files:** `src/engine/mutation_flow.rs` (new executor + rewire `execute_merge_statement` `:2076-2232`); `src/engine/iceberg_writer.rs` (expose an INSERT write-plan builder).

- [ ] **Step 1: Failing guard test**

```rust
#[test]
fn merge_folds_all_branches_into_one_runner() {
    let src = include_str!("mutation_flow.rs");
    let body = src.split("fn execute_merge_statement").nth(1).expect("fn")
        .split("\nfn ").next().expect("body");
    // INSERT must NOT run its own insert/commit transaction inside MERGE.
    assert!(!body.contains("execute_merge_unmatched_insert"),
        "MERGE must not commit the INSERT branch in a separate transaction");
    // exactly one runner pass for the whole statement:
    assert!(body.contains("DistributedMergeExecutor"),
        "MERGE must use the single multi-branch executor");
}
```

- [ ] **Step 2: Run — expect FAIL.**

- [ ] **Step 3: Generalize `merge_write_commits` to N writers**

Add `fn merge_all_write_commits(parts: Vec<CoordinatedQueryResult>) -> Result<CoordinatedQueryResult, String>` next to `merge_write_commits` (`:906`): concatenate all parts' `WriteCommitInput.writers`; if any part carries a `write_abort`, propagate the first. Keep `merge_write_commits` (2-ary) delegating to it, or fold callers over.

- [ ] **Step 4: Expose an INSERT write-plan builder**

In `iceberg_writer.rs`, factor out (do not duplicate) the INSERT query + `build_insert_write_sink_spec` construction from `execute_iceberg_insert_or_overwrite` (`:71`) into a `pub(crate) fn build_insert_write_plan(state, target, resolved, source, columns) -> Result<(Query, IcebergWriteSinkSpec), String>` so the MERGE executor can run the INSERT via `execute_query_as_iceberg_write` (no own collector/runner). The standalone INSERT path calls the builder then its existing runner — behavior unchanged.

- [ ] **Step 5: Implement `DistributedMergeExecutor`**

```rust
struct MergeBranchWrites {
    // populated per active branch; each is a (query, sink_spec) run via execute_query_as_iceberg_write
    insert: Option<(Query, IcebergWriteSinkSpec)>,            // not-matched INSERT -> Data sink
    mor_data: Option<(Query, IcebergWriteSinkSpec)>,          // matched-UPDATE-MOR new rows -> RowLineageData
    mor_dv: Option<(Query, IcebergWriteSinkSpec)>,            // matched-UPDATE-MOR old rows OR matched-DELETE -> DeletionVectors
    cow: Option<CowUpdateDistributedWrite>,                   // matched-UPDATE-COW per-file rewrite
}

struct DistributedMergeExecutor {
    state: Arc<StandaloneState>,
    target: TargetBackend,
    branches: Mutex<Option<MergeBranchWrites>>,
    commit_executor: IcebergWriteCommitExecutor,             // holds the ONE shared collector + commit_op_kind
}

impl IcebergWriteTransactionExecutor for DistributedMergeExecutor {
    fn run_coordinated_write(&self, _spec) -> Result<CoordinatedQueryResult, String> {
        let b = self.branches.lock()…take()…;
        let mut parts = Vec::new();
        if let Some((q, s)) = b.insert  { parts.push(run_write(q, s, None)?); }
        if let Some((q, s)) = b.mor_data{ parts.push(run_write(q, s, None)?); }
        if let Some((q, s)) = b.mor_dv  { parts.push(run_write(q, s, Some(shuffle_by_file_0()))?); }
        if let Some(cow) = b.cow        { parts.push(run_cow_rewrites(cow)?); } // per-file loop -> merged part
        merge_all_write_commits(parts)
    }
    fn commit(&self, spec, wc) -> … { self.commit_executor.commit_write_input(wc) }   // ONE commit
}
```

`run_cow_rewrites` reuses the per-file loop already in `DistributedCowUpdateExecutor` (`:1548-1636`); its COW commit routes the INSERT's BE-written data files into `CowUpdateRewriteSet.appended_files` (M1 field, **M3a** fresh-allocation). Add the same "non-empty matched batch ⇒ DV/data files produced" guards as E2.

**Row-lineage routing (mechanism implemented in M3a):** populate the FRESH channel with the not-matched-INSERT branch's `Data`-sink files (M3a allocates them fresh `_row_id`s via `effective_n`), and the REUSE path with UPDATE/rewrite `RowLineageData` files (explicit `_row_id`, suppressed). Concretely: COW fold → INSERT files into `CowUpdateRewriteSet.appended_files`, rewrite outputs into `touched_data_files[*].new_files`. MOR fold → INSERT files into the `RowDeltaDvFromFiles` fresh channel (M3a), UPDATE new-row files into E1's reuse `written`, DVs as Puffin. matched-DELETE+INSERT → only DV + INSERT(fresh), no reuse data. M4's `COUNT(DISTINCT _row_id)=COUNT(*)` golden is the gate.

- [ ] **Step 6: Rewire `execute_merge_statement` to one transaction**

Replace the separate INSERT and matched calls (`:2146-2228`) with: materialize match once (unchanged); build a single `IcebergCommitCollector` + `IcebergWriteCommitExecutor`; populate `MergeBranchWrites` from the materialized batches (INSERT from `unmatched_insert_batch`; matched from `matched_batch` → COW rewrite or MOR data+DV, or matched-DELETE DV per M2); choose `commit_op_kind`:
  - COW table + matched-UPDATE present → `CommitOpKind::CowUpdate` (with `appended_files` = INSERT data).
  - else if any DV/data fold (matched-UPDATE-MOR, matched-DELETE, ± INSERT) → `CommitOpKind::RowDeltaDvFromFiles`.
  - else (INSERT only, no matched) → `CommitOpKind::FastAppend`.
  Run **one** `IcebergWriteTransactionRunner::new(state, &DistributedMergeExecutor{…}).run(spec)`. Keep `validate_unique_target_row_ids` before building branches.

- [ ] **Step 7: Run guard + module tests + existing MERGE byte-identical**

Run: `cargo test --lib engine::mutation_flow::tests` then `--only merge_into_cow,merge_into_mor,merge_into_upsert_delete,merge_into_matched_delete --mode verify`
Expected: PASS. **Row results unchanged** (the goldens query data, not snapshots), so byte-identical even though the snapshot count drops from 2→1.

- [ ] **Step 8: Commit**

```bash
git add src/engine/mutation_flow.rs src/engine/iceberg_writer.rs
git commit -m "feat(merge): fold all MERGE branches into one collector + one commit (atomic snapshot)"
```

---

## Task M4: Atomicity assertions + golden / header updates

**Why:** Prove "one MERGE = one snapshot" and add the matched-DELETE-with-INSERT fold case. Pure-SQL assertion via the `…$snapshots` metadata table — no new harness directive.

**Files:** `sql-tests/iceberg-dml/sql/merge_into_{cow,mor,matched_delete,upsert_delete}.sql` (+ `.result`); a new fold case for matched-DELETE + INSERT.

- [ ] **Step 1: Add snapshot-count assertions to each MERGE case**

In each `merge_into_*.sql`, capture the snapshot count immediately before and after the MERGE statement and assert the delta is exactly 1:

```sql
SELECT count(*) AS snaps_before FROM <cat>.<db>.<tbl>$snapshots;
MERGE INTO <tbl> … ;   -- matched + not-matched
SELECT count(*) AS snaps_after  FROM <cat>.<db>.<tbl>$snapshots;
-- golden: snaps_after = snaps_before + 1   (was +2 before Phase 3)
SELECT operation FROM <cat>.<db>.<tbl>$snapshots ORDER BY committed_at DESC LIMIT 1;
-- golden: 'overwrite' for COW, 'overwrite'/'delete'(RowDelta) for MOR per Iceberg op naming
```

- [ ] **Step 2: Add the matched-DELETE + INSERT fold case**

Extend `merge_into_matched_delete.sql` (or add `merge_into_matched_delete_insert.sql`) with `WHEN MATCHED THEN DELETE WHEN NOT MATCHED THEN INSERT …` and the same before/after snapshot-count assertion (golden = +1). This is the case that most directly exercises the matched-DELETE ⊕ INSERT fold into one `RowDeltaDvFromFiles` snapshot.

- [ ] **Step 3: Fix the stale header comments**

Update the `merge_into_cow.sql` / `merge_into_mor.sql` header comments that currently say "COW UPDATE snapshot + FastAppend INSERT snapshot" to state the Phase-3 invariant: one MERGE = one snapshot.

- [ ] **Step 4: Record + verify**

```bash
cargo run … --suite iceberg-dml --only merge_into_cow,merge_into_mor,merge_into_matched_delete,merge_into_matched_delete_insert,merge_into_upsert_delete --mode record
cargo run … --only <same> --mode verify
```
Expected: PASS; the new `snaps_after - snaps_before` rows all show +1.

- [ ] **Step 5: Commit**

```bash
git add sql-tests/iceberg-dml/sql/merge_into_*.sql sql-tests/iceberg-dml/result/merge_into_*.result
git commit -m "test(iceberg-dml): assert one-snapshot-per-MERGE via \$snapshots + add matched-DELETE cases"
```

---

## Task M5: Delete the coordinator-local MERGE matched path (deletion 收口)

**Why:** matched-DELETE is now BE-written and folded (M2+M3); the coordinator-local arm is dead.

**Files:** `src/engine/mutation_flow.rs`.

- [ ] **Step 1: Delete the dead path**

Remove: `MutationWritePlan` enum + its single `MergeMatchedDelete` variant (`:1029-1037`), `MutationWriteExecutor` (`:1041-…`) and its `run_coordinated_write`/`commit`, `build_position_delete_groups_from_matched` (`:1006`), `run_mutation_write_transaction` (runner `:1184`), and any `MatchedUpdateBatch`-derived position-group helpers used only here. Keep `materialize_merge_match`, `matched_update_batch_from_record_batch`, `validate_unique_target_row_ids`.

- [ ] **Step 2: Check `inject_delete_group` is now unused for standalone writes**

Run: `rg -n "inject_delete_group|build_position_delete_groups_from_matched|MutationWritePlan|MutationWriteExecutor" src/`
If `inject_delete_group` (collector) has **no remaining callers**, leave its removal to Phase 5's `local_writer_commit_input` 收口 (note it), or remove if trivially dead. Do **not** touch `RowDeltaDvCommit` if other paths still use it — confirm by grep.

- [ ] **Step 3: Build + tests + guards (no new warnings in touched files)**

Run: `cargo build --lib && cargo test --lib engine::mutation_flow::tests`
Expected: PASS; no `unused` warnings attributable to this change.

- [ ] **Step 4: Commit**

```bash
git add src/engine/mutation_flow.rs
git commit -m "refactor(merge): remove dead coordinator-local matched-DELETE inject path"
```

---

## Task M6: End-to-end verification + final review

- [ ] **Step 1: Full lib suite**

Run: `cargo test --lib 2>&1 | grep "test result:"`
Expected: 0 failed.

- [ ] **Step 2: fmt + clippy**

Run: `cargo fmt --check` (clean) and `cargo clippy --lib` (no NEW lints in `mutation_flow.rs` / `iceberg_writer.rs` / `update_cow.rs` vs base).

- [ ] **Step 3: All-in-one byte-identical (primary gate, incl. snapshot-count goldens)**

Run: `--suite iceberg-dml --only merge_into_cow,merge_into_mor,merge_into_matched_delete,merge_into_matched_delete_insert,merge_into_upsert_delete,update_cow,update_mor --mode verify`
Expected: PASS — every MERGE shows +1 snapshot; UPDATE regressions green.

- [ ] **Step 4: Cross-process (FE/BE separation)**

```bash
cargo build --profile dev-opt
NOVAROCKS_BIN=target/dev-opt/novarocks NO_PROXY=127.0.0.1,localhost \
cargo run --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests -- \
  --config "$NOVAROCKS_SQL_TEST_CONFIG" --suite iceberg-dml \
  --only merge_into_cow,merge_into_mor,merge_into_matched_delete_insert \
  --mode verify --cluster-mode cross-process --cluster-size 2 -j 1
```
Expected: PASS — confirms all MERGE files are BE-written under real FE/BE separation.

- [ ] **Step 5: Final whole-Phase-3 code review** (cross-task integration: the one-collector fold, commit-op selection by mode, M1 append correctness, M2 BE-DV conversion, deletion completeness) then **superpowers:finishing-a-development-branch**.

---

## Acceptance (Phase 3 done when all true)

- [ ] `merge_matched_delete_writes_dv_on_be_not_coordinator` + `merge_folds_all_branches_into_one_runner` guards pass.
- [ ] A MERGE with matched + not-matched produces **exactly one** new snapshot (asserted via `…$snapshots` in cow/mor/matched_delete_insert).
- [ ] All `merge_into_*` row goldens byte-identical (values unchanged); the only result-delta is `snaps_after − snaps_before = 1`.
- [ ] FE writes no data/DV files for MERGE: INSERT data, matched-UPDATE data/rewrite, and matched-DELETE/MOR-old-row DV are all BE-written; FE only commits manifests.
- [ ] MERGE honors `write.update.mode`: COW table → Overwrite (`CowUpdateCommit` w/ appended INSERT); MOR table → RowDelta (`RowDeltaDvFromFiles` w/ data + DV).
- [ ] Coordinator-local matched-DELETE path (`MutationWritePlan`/`MutationWriteExecutor`/`build_position_delete_groups_from_matched`) deleted.
- [ ] `cargo fmt` + `cargo clippy --lib` clean (no new lints); full `cargo test --lib` green.

## Out of scope (later phases)

- **Phase 4 — equality-delete** (`execute_add_equality_delete_statement` → new `EqualityDeletes` sink).
- **Phase 5 — deletion 收口**: `local_writer_commit_input` / `new_local_writer_write_id` (`write_transaction.rs:205-242`), `has_preloaded_commit_output` trait method + runner gate; remove `inject_delete_group`/`RowDeltaDvCommit` if fully dead after M5.
- **Perf**: the match join currently runs twice (`materialize_merge_match` + the INSERT re-join). Driving the INSERT branch inline from the already-materialized unmatched batch (avoiding the second join) is a deferred optimization — M3 keeps the existing INSERT query for minimal blast radius.

## Risks

1. **COW fold commit shape (M1)**: appended INSERT files share the Overwrite snapshot with rewritten files. Mitigation: appended files are `content==Data`, carry their own `record_count`, remove nothing; validation tolerates "written-but-not-a-rewrite-output" only for the declared `appended_files` set. Unit-tested in M1.
2. **matched-DELETE DV dedup**: multiple source rows hitting one target row produce duplicate `(_file,_pos)`; the BE `DeletionVector` dedups, and `validate_unique_target_row_ids` still guards cardinality on the coordinator (M2 keeps it).
3. **Commit-op selection** must be exhaustive over {COW,MOR} × {matched-UPDATE, matched-DELETE, none} × {INSERT, none}. M3-Step-6 enumerates; the guard test + the 5 SQL cases cover the live combinations.
4. **Snapshot `operation` golden naming**: Iceberg names RowDelta commits `overwrite`/`delete` depending on content. M4-Step-1 records the actual value rather than hard-coding — record-then-verify avoids guessing the label.
