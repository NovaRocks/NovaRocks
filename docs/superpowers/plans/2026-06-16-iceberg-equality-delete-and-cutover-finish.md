# Phase 4 (equality-delete → BE sink) + Phase 5 (deletion 收口) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Finish the distributed Iceberg DML write cutover. **Phase 4:** convert `ADD EQUALITY DELETE` from an in-process coordinator write to a BE `EqualityDeletes` sink (literal rows → `VALUES` source query → BE writes the equality-delete file → FE commits metadata only). **Phase 5:** with equality-delete being the last local-write path, delete the in-process write shims (`local_writer_commit_input`/`new_local_writer_write_id`), the now-redundant `has_preloaded_commit_output` trait method + runner gate clause, and fold in the tracked #24 write-path edit-safety debt.

**Architecture:** Reuse the established pattern. The COMMIT side needs **zero** changes — `RowDeltaCommit` already accepts `content=EqualityDeletes` `WrittenFile`s with `equality_ids`, and the `WrittenFile → TSinkCommitInfo → convert_sink_commit_info → RowDeltaCommit` round-trip already carries `equality_ids` (exactly as Phase 1 reused an existing commit for BE-written DVs). The new work is all on the **write/sink** side: a new `EqualityDeletes` sink mode + BE sink operator that calls the existing `write_equality_delete_file`, fed by a `VALUES`-source query, run through a `DistributedEqualityDeleteWriteExecutor` mirroring Phase 1's `DistributedDvDeleteWriteExecutor`. Then Phase 5 removes the dead in-process shims.

**Tech Stack:** Rust; thrift (`idl/thrift/DataSinks.thrift`); reuses `write_equality_delete_file`/`build_equality_delete_batch`, `RowDeltaCommit`, `execute_query_as_iceberg_write`, `IcebergWriteTransactionRunner`, the `IcebergSinkMode` BE operator framework; sql-test-runner.

**Profile note:** `cargo test --lib` (profile `dev`) for unit/guard tests; `cargo build --bin novarocks` (debug) for SQL-suite runs; `--profile dev-opt` only for cross-process.

**Scope decision (preserve, don't expand):** Phase 4 keeps the **unpartitioned-only** limitation (the in-process writer is unpartitioned-only by construction; the goal is the BE-write cutover, NOT per-partition equality-delete routing). Lifting that is an explicit non-goal — see Out of scope.

---

## Current-state anchors (verified against branch off main `cd00885c`)

**Equality-delete (Phase 4 target):**
- `execute_add_equality_delete_statement` — `src/engine/equality_delete_flow.rs:52`. Triggered by string-sniffed `ALTER TABLE [cat.db.]t ADD EQUALITY DELETE (k1,k2) VALUES (..),(..)` (`statement.rs:2689` `looks_like_add_equality_delete`, parsed `:2695` → `AddEqualityDeleteStmt { table, columns, rows: Vec<Vec<Literal>> }` `:1269`). Dispatch `mod.rs:1780` `handle_add_equality_delete`.
- Flow: resolve+`load_table` → validators (`ensure_no_variant_columns_for_row_level_mutation` `:82`; reject v1 `:85`; `ensure_equality_delete_single_partition_spec` `:88`; **reject partitioned** `:89-93`) → `build_equality_delete_batch(schema,&cols,&rows)` `:95` → empty ⇒ no-op `:100` → `RowDelta` collector `:115` + `EqualityDeleteWriteExecutor` `:142` → runner (`operation_kind=RowDelta`, `commit_op_kind=RowDelta`, `source=CoordinatedPlan`).
- `EqualityDeleteWriteExecutor::run_coordinated_write` (`:188-229`) writes the file **on the coordinator**: `write_equality_delete_file(&file_io, staging_dir, default_spec_id, delete_columns, batch)` (`src/connector/iceberg/commit/equality_delete_writer.rs:49`) → `WrittenFile{content=EqualityDeletes, format=Parquet, equality_ids=Some([field_ids]), partition_values=Struct::empty()}` (`:69-94`); then `written_file_to_sink_commit_info` `:217`; then `write_commit: Some(local_writer_commit_input(new_local_writer_write_id(), vec![info]))` (`:223-226`) — **the sole production use of `local_writer_commit_input`**.
- `build_equality_delete_batch` `:244-310` (validates cols, dedups, builds typed arrays from literals via `build_literal_array_for_equality` `:345`).
- Commit reuse (NO change): `CommitOpKind::RowDelta → RowDeltaCommit` (`commit/run.rs:100`); `RowDeltaCommit` accepts `EqualityDeletes` requiring `equality_ids` (`commit/row_delta.rs:77-89`, writes via `builder.equality_ids(...)` `:387`); `convert_sink_commit_info` maps `EQUALITY_DELETES→EqualityDeletes` + round-trips `equality_ids` (`commit/collector.rs:391,452`); `written_file_to_sink_commit_info` carries `equality_ids` + content map (`data_writer.rs:453,484`).

**Sink machinery:**
- `IcebergWriteSinkMode` — `src/sql/codegen/iceberg_write_sink.rs:27-33` (`Data, RowLineageData, PositionDeletes, DeletionVectors`); `data_sink_type` match `:35-42`; `build_sink` emits one `TIcebergTableSink` for all modes `:66-96`.
- Thrift `TDataSinkType` — `idl/thrift/DataSinks.thrift:45-65` (`…, ICEBERG_TABLE_SINK, ICEBERG_DELETE_SINK, ICEBERG_DV_SINK=18`; NovaRocks-only extensions appended past upstream `NOOP_SINK`).
- Lowering — `src/lower/fragment.rs:150` `iceberg_sink_mode_for_type` (sink type → `IcebergSinkMode`), dispatch `:534-549`.
- BE operator — `src/connector/iceberg/sink.rs`: `IcebergSinkMode` enum `:88-93` (`Data, PositionDeletes, DeletionVectors`); `push_chunk` dispatch `:606-608`; `push_chunk_deletion_vector` `:927` (the closest template); schema-validation arms in `IcebergTableSinkFactory::try_new` `:168-201`.

**Executor template (Phase 1):** `DistributedDvDeleteWriteExecutor` `src/engine/delete_flow.rs:259-302` + `run_delete_dv_write_transaction` `:304-406` (sink spec → source query → `RowDeltaDvFromFiles` collector → runner). `execute_query_as_iceberg_write` `src/engine/mod.rs:3470`. `VALUES`-source builder to mirror: `build_merge_mor_dv_sink_query_from_matched` `src/engine/mutation_flow.rs:481-591` (`"(VALUES {rows}) AS {alias}({cols})"` `:555`).

**Phase 5 targets:**
- `new_local_writer_write_id` `src/engine/write_transaction.rs:205`; `local_writer_commit_input` `:209-242`. Callers: `equality_delete_flow.rs:223` (prod, sole) + unit test `write_transaction.rs:769/781`.
- `has_preloaded_commit_output`: trait default `write_transaction.rs:105-107`; overrides — `DistributedMergeExecutor` `mutation_flow.rs:2975` (prod, **provably redundant**, see comment `:2976-2980`), `FakeExecutor` `write_transaction.rs:552` (test). Equality-delete does NOT override it.
- Runner gate `write_transaction.rs:302-314`, clause `|| self.executor.has_preloaded_commit_output()` `:304`.
- `has_injected_written_files`/`has_injected_appended_files` `commit/collector.rs:245/280` (only consumed by the `DistributedMergeExecutor` override).

**#24 debt:** MOR data+DV dup — `DistributedMorUpdateExecutor::run_coordinated_write` `mutation_flow.rs:830-884` vs MERGE fold MorUpdate arm `:2815-2859`. Micro-fixes — `CowUpdateCommit::commit` `update_cow.rs:79` (debug_assert appended channel empty); `merge_write_commits` `unreachable!()` `mutation_flow.rs:916` → return `Err`.

---

## File Structure

| File | Change |
|---|---|
| `idl/thrift/DataSinks.thrift` | P4: add `ICEBERG_EQUALITY_DELETE_SINK = 19` to `TDataSinkType` (NovaRocks-only; FE never sees it) |
| `src/sql/codegen/iceberg_write_sink.rs` | P4: add `IcebergWriteSinkMode::EqualityDeletes` + `data_sink_type` arm → `ICEBERG_EQUALITY_DELETE_SINK` |
| `src/lower/fragment.rs` | P4: `iceberg_sink_mode_for_type` arm `ICEBERG_EQUALITY_DELETE_SINK → IcebergSinkMode::EqualityDeletes`; sink dispatch arm |
| `src/connector/iceberg/sink.rs` | P4: `IcebergSinkMode::EqualityDeletes`; `try_new` schema-shaping arm (input = equality-key cols); `push_chunk_equality_delete` calling shared equality-delete writer |
| `src/connector/iceberg/commit/equality_delete_writer.rs` | P4: expose a chunk→equality-delete-file write usable from the BE sink (reuse `write_equality_delete_file`) |
| `src/engine/equality_delete_flow.rs` | P4: replace the in-process `EqualityDeleteWriteExecutor` with a distributed path (`VALUES` query + `EqualityDeletes` sink + `DistributedEqualityDeleteWriteExecutor`); drop `local_writer_commit_input` use. P5: nothing |
| `src/engine/write_transaction.rs` | P5: delete `local_writer_commit_input`/`new_local_writer_write_id` + unit test; delete `has_preloaded_commit_output` trait method + gate clause |
| `src/engine/mutation_flow.rs` | P5: delete `DistributedMergeExecutor::has_preloaded_commit_output`; #24: extract `run_mor_data_and_dv` helper; `merge_write_commits` return `Err` not `unreachable!` |
| `src/connector/iceberg/commit/collector.rs` | P5: delete `has_injected_written_files`/`has_injected_appended_files` if dead after the override removal |
| `src/connector/iceberg/commit/update_cow.rs` | #24: debug_assert the collector appended channel is empty in `CowUpdateCommit::commit` |
| `sql-tests/iceberg-dml/sql/equality_delete_*.sql` (+ `.result`) | P4: existing equality-delete cases must stay byte-identical; add a `…$snapshots` +1 assertion |

Reused **unchanged**: `RowDeltaCommit`, `convert_sink_commit_info`, `written_file_to_sink_commit_info`, `IcebergWriteCommitExecutor`, `IcebergWriteTransactionRunner`, `build_equality_delete_batch`, `write_equality_delete_file`.

---

## Phase 4 — equality-delete → BE sink

## Task Q1: Add the `EqualityDeletes` sink-mode plumbing (thrift + codegen + lowering)

**Files:** `idl/thrift/DataSinks.thrift`, `src/sql/codegen/iceberg_write_sink.rs` (`:27-42`), `src/lower/fragment.rs` (`:150`, `:534-549`), `src/connector/iceberg/sink.rs` (`IcebergSinkMode` `:88-93`).

- [ ] **Step 1: Thrift.** Add `ICEBERG_EQUALITY_DELETE_SINK = 19` to `TDataSinkType` (`idl/thrift/DataSinks.thrift`, after `ICEBERG_DV_SINK = 18`). Confirm how thrift is regenerated (build.rs auto vs a committed generated module) by grepping for the generated `TDataSinkType` in `target/`/`src/`; run the project's regen (usually `cargo build` triggers build.rs). The new value must round-trip in the generated Rust enum.

- [ ] **Step 2: Codegen.** Add `EqualityDeletes` to `IcebergWriteSinkMode` (`iceberg_write_sink.rs:27-33`) and a `data_sink_type` arm `Self::EqualityDeletes => TDataSinkType::ICEBERG_EQUALITY_DELETE_SINK` (`:35-42`). `build_sink` is mode-agnostic — no change.

- [ ] **Step 3: Lowering.** Add `IcebergSinkMode::EqualityDeletes` (`sink.rs:88-93`). In `fragment.rs` `iceberg_sink_mode_for_type` (`:150`) add `TDataSinkType::ICEBERG_EQUALITY_DELETE_SINK => IcebergSinkMode::EqualityDeletes`; add the matching arm in the sink dispatch (`:534-549`) so it builds `IcebergTableSinkFactory::try_new(..., IcebergSinkMode::EqualityDeletes, ...)`.

- [ ] **Step 4: Unit test + build.** Add a codegen unit test asserting `IcebergWriteSinkMode::EqualityDeletes.data_sink_type() == ICEBERG_EQUALITY_DELETE_SINK` and the `fragment.rs` mapping round-trips. Run `cargo build --lib` (regenerates thrift) — clean.

- [ ] **Step 5: Commit** — `git commit -m "feat(sink): add EqualityDeletes sink mode plumbing (thrift + codegen + lowering)"`.

---

## Task Q2: BE `EqualityDeletes` sink operator

**Files:** `src/connector/iceberg/sink.rs` (`try_new` `:168-201`, `push_chunk` dispatch `:606-608`, new `push_chunk_equality_delete` modeled on `push_chunk_deletion_vector` `:927`); `src/connector/iceberg/commit/equality_delete_writer.rs` (expose a chunk-based writer).

- [ ] **Step 1: Failing unit test.** Drive `IcebergTableSinkFactory::try_new(..., EqualityDeletes, ...)` with a small chunk of equality-key columns; assert it produces a `WrittenFile { content: DataContentType::EqualityDeletes, format: Parquet, equality_ids: Some([field_ids]) }` whose `equality_ids` match the key columns' field ids. (Mirror the existing DV-sink unit test if present; else assert via the operator's finished-files output.)

- [ ] **Step 2: Run — expect FAIL** (`IcebergSinkMode::EqualityDeletes` unhandled in `push_chunk`/`try_new`).

- [ ] **Step 3: Schema-shaping in `try_new`.** Add an `EqualityDeletes` arm to the schema-validation block (`sink.rs:168-201`): the sink's INPUT schema is the **equality-key columns** (the `VALUES` projection), NOT the full table schema and NOT the `[file_path,_pos]` position-delete schema. Resolve each input column to its iceberg field id; store the key field ids on the operator (for `equality_ids`).

- [ ] **Step 4: `push_chunk_equality_delete`.** Add the dispatch arm (`sink.rs:606-608`) and the method: convert the chunk to the equality-delete `RecordBatch` and call a chunk-based equality-delete writer. Refactor `equality_delete_writer.rs` so `write_equality_delete_file`'s core (build the parquet with `PARQUET_FIELD_ID_META_KEY` per key column + `equality_ids`) is callable from the BE sink given (file_io, staging dir from the sink's write context, spec_id, key columns, batch). Produce a `WrittenFile{content=EqualityDeletes, equality_ids=Some(field_ids), partition_values=Struct::empty()}` (unpartitioned — preserve the limitation). Accumulate into the sink's finished-files like the DV path.

- [ ] **Step 5: Run unit test** — `cargo test --lib connector::iceberg::sink` — PASS. Confirm the existing `Data`/`PositionDeletes`/`DeletionVectors` sink tests still pass (no shared-path regression).

- [ ] **Step 6: Commit** — `git commit -m "feat(sink): BE EqualityDeletes sink operator (writes equality-delete file on the BE)"`.

---

## Task Q3: Distributed equality-delete path (rewire `execute_add_equality_delete_statement`)

**Files:** `src/engine/equality_delete_flow.rs` (replace `EqualityDeleteWriteExecutor` `:188-237` + rewire `execute_add_equality_delete_statement` `:52-178`).

- [ ] **Step 1: Failing guard test.** Mirror `dv_delete_uses_distributed_dv_sink_not_local_collect` (delete_flow.rs):
```rust
#[test]
fn equality_delete_writes_file_on_be_not_coordinator() {
    let src = include_str!("equality_delete_flow.rs");
    let body = src.split("fn execute_add_equality_delete_statement").nth(1).expect("fn")
        .split("\nfn ").next().expect("body");
    assert!(!body.contains("write_equality_delete_file"),
        "equality-delete must not write the file on the coordinator");
    assert!(!body.contains("local_writer_commit_input"),
        "equality-delete must not wrap a coordinator-written file (FE central write)");
    assert!(body.contains("EqualityDeletes"),
        "equality-delete must write via the BE EqualityDeletes sink");
}
```
- [ ] **Step 2: Run — expect FAIL.**

- [ ] **Step 3: Build the `VALUES` source query.** Add `build_equality_delete_sink_query(table, &columns, &rows) -> Query`: emit `SELECT <key cols, typed-cast> FROM (VALUES {rows}) AS t(<key cols>)`, mirroring `build_merge_mor_dv_sink_query_from_matched` (`mutation_flow.rs:481-591`) — reuse its literal/cast helpers. The projection is exactly the equality-key columns (these become the sink's input schema, Q2).

- [ ] **Step 4: Distributed executor + rewire.** Add `DistributedEqualityDeleteWriteExecutor` + `run_equality_delete_distributed_transaction`, modeled on `DistributedDvDeleteWriteExecutor`/`run_delete_dv_write_transaction` (delete_flow.rs:259-406): build an `EqualityDeletes` sink spec (set `mode = EqualityDeletes`, `set_planned_snapshot_id(current_snapshot)`), run `execute_query_as_iceberg_write(state, Some(cat), ns, &values_query, sink_spec, None, None)` (no shuffle — equality deletes aren't keyed by `_file`), commit via a `RowDelta` collector (`CommitOpKind::RowDelta`, unchanged). Rewire `execute_add_equality_delete_statement` to: keep the validators (incl. the unpartitioned-only reject `:89-93`) + the empty-rows no-op, then call the distributed path instead of `EqualityDeleteWriteExecutor`. **Delete** the in-process `EqualityDeleteWriteExecutor` (`:188-237`) and its `write_equality_delete_file`/`local_writer_commit_input` use. Keep `build_equality_delete_batch` only if still needed for validation; if the BE sink now does the batch-building, drop it from the FE path (grep before deleting).

  Add the "non-empty rows ⇒ sink produced files" guard (mirror E2/M2): a non-empty `VALUES` must yield an equality-delete `WrittenFile`, else fail fast.

- [ ] **Step 5: Run guard + module tests + byte-identical** (server up; see Q-VERIFY for setup):
```bash
cargo test --lib engine::equality_delete_flow
... --suite iceberg-dml --only equality_delete_schema_evolution --mode verify   # + any other equality_delete_* case
```
Expected: PASS, byte-identical (same rows deleted; the equality-delete file is now BE-written). Confirm no regression in `delete_*`/`update_*`/`merge_*`.

- [ ] **Step 6: Commit** — `git commit -m "feat(equality-delete): write equality-delete file on the BE via EqualityDeletes sink + VALUES source"`.

---

## Task Q4: Phase-4 verification

- [ ] **Step 1: All-in-one byte-identical** — every `equality_delete_*` case + a regression sweep of `delete_*`, `update_cow/mor`, `merge_into_*` via the full `iceberg-dml` suite (`--mode verify`, no `--only`). Add a `SELECT count(*) FROM <tbl>$snapshots` before/after assertion to one equality-delete case (golden = +1; equality-delete is one RowDelta snapshot).
- [ ] **Step 2: Cross-process** — `--only <an equality_delete case> --mode verify --cluster-mode cross-process --cluster-size 2 -j 1` (dev-opt binary) — confirms the equality-delete file is BE-written under real FE/BE separation.
- [ ] **Step 3: Commit** any `.result` updates (there should be none beyond the new `$snapshots` assertion).

---

## Phase 5 — deletion 收口 + #24 debt

## Task Q5: Delete the in-process write shims (now dead after Q3)

**Files:** `src/engine/write_transaction.rs`.

- [ ] **Step 1: Confirm dead.** `rg "local_writer_commit_input|new_local_writer_write_id" src/` → only the definitions (`:205`,`:209`) + the unit test (`:769`/`:781`) remain (Q3 removed the equality-delete caller).
- [ ] **Step 2: Delete** `local_writer_commit_input` (`:209-242`), `new_local_writer_write_id` (`:205-207`), and the `local_writer_commit_input_carries_sink_commit_infos` unit test (`:769`). Remove now-unused imports.
- [ ] **Step 3: Build + tests** — `cargo build --lib && cargo test --lib engine::write_transaction` — clean, no `unused`/dead warnings from this.
- [ ] **Step 4: Commit** — `git commit -m "refactor(write): remove in-process local_writer_commit_input shims (Phase 5)"`.

## Task Q6: Delete the redundant `has_preloaded_commit_output` + gate clause

**Files:** `src/engine/write_transaction.rs` (`:105`, `:302-314`, test `:552`), `src/engine/mutation_flow.rs` (`:2975`), `src/connector/iceberg/commit/collector.rs` (`:245`,`:280`).

- [ ] **Step 1: Failing/anchor test.** Confirm via a unit test (or the existing runner tests) that an empty-write `RowDeltaDvFromFiles`/`RowDelta` no-op still aborts correctly through gate condition #3 (`!FastAppend`) and a real-file write commits through condition #1 — i.e. condition #2 is never decisive. (The study verified this; lock it with an assertion if a seam exists, else rely on the existing runner tests.)
- [ ] **Step 2: Delete** the `|| self.executor.has_preloaded_commit_output()` clause from the gate (`write_transaction.rs:304`), the trait method (`:105-107`), the `DistributedMergeExecutor` override (`mutation_flow.rs:2975-2983`), and the `FakeExecutor` test override (`write_transaction.rs:552-554`).
- [ ] **Step 3:** `rg "has_injected_written_files|has_injected_appended_files" src/` — if now only the deleted override referenced them, delete them from `collector.rs` (`:245`,`:280`) too. If other callers exist, keep + note.
- [ ] **Step 4: Build + full runner/commit tests** — `cargo test --lib engine::write_transaction connector::iceberg::commit` — PASS (no behavior change: every shape still admitted by conditions #1/#3).
- [ ] **Step 5: Commit** — `git commit -m "refactor(write): drop redundant has_preloaded_commit_output + gate clause (Phase 5)"`.

## Task Q7: #24 write-path edit-safety debt

**Files:** `src/engine/mutation_flow.rs` (`:830-884` vs `:2815-2859`, `:916`), `src/connector/iceberg/commit/update_cow.rs` (`:79`).

- [ ] **Step 1: Extract `run_mor_data_and_dv`.** Factor the duplicated MOR data-write + DV-write + two file-less guards into `fn run_mor_data_and_dv(state, target, &MorUpdateDistributedWrite) -> Result<(CoordinatedQueryResult, CoordinatedQueryResult), String>`. `DistributedMorUpdateExecutor` feeds it to `merge_write_commits`; the MERGE fold pushes both parts to `commit_parts`. Run `engine::mutation_flow::tests` — PASS.
- [ ] **Step 2: `merge_write_commits` return `Err`.** Change the `unreachable!()` (`:916`) to propagate the `merge_all_write_commits` `Err` (make `merge_write_commits` return the same `Result`, or surface a real error). Update the (single) caller if its signature changes.
- [ ] **Step 3: `CowUpdateCommit` debug_assert.** After `take_written_files()` (`update_cow.rs:79`), add `debug_assert!(ctx.collector.take_appended_files().is_empty(), "CowUpdate routes net-new INSERT data via the rewrite set, not the collector appended channel")` (or check `has_injected_appended_files()` if `take_*` would consume needed state — pick the non-destructive check).
- [ ] **Step 4: Build + tests** — `cargo test --lib engine::mutation_flow commit::update_cow` — PASS.
- [ ] **Step 5: Commit** — `git commit -m "refactor(write): extract shared MOR data+DV helper + edit-safety guards (#24)"`.

## Task Q8: Final end-to-end verification + review + finish

- [ ] **Step 1: Full lib suite** — `cargo test --lib` → 0 failed.
- [ ] **Step 2: fmt + clippy** — `cargo fmt --check` clean; `cargo clippy --lib` no new lints on touched files.
- [ ] **Step 3: All-in-one byte-identical** — full `iceberg-dml` suite `--mode verify` (equality-delete now BE-written + the $snapshots assertion; everything else unchanged).
- [ ] **Step 4: Cross-process** — equality-delete + a couple of merge/update cases under `--cluster-mode cross-process --cluster-size 2`.
- [ ] **Step 5: Final whole-Phase-4+5 code review** (cross-task: the new sink end-to-end + commit reuse; the 收口 deletions left nothing dangling; #24). Then **superpowers:finishing-a-development-branch**.

---

## Acceptance (done when all true)

- [ ] `equality_delete_writes_file_on_be_not_coordinator` guard passes; equality-delete files are BE-written (cross-process proves FE writes no equality-delete file).
- [ ] All `equality_delete_*` + `delete_*` + `update_*` + `merge_into_*` cases byte-identical; equality-delete shows +1 `$snapshots`.
- [ ] `local_writer_commit_input` / `new_local_writer_write_id` / `has_preloaded_commit_output` deleted; runner gate simplified; no dangling refs; full `cargo test --lib` green.
- [ ] #24 debt resolved (shared MOR helper; `merge_write_commits` returns `Err`; CowUpdate appended-channel assert).
- [ ] `cargo fmt` + `cargo clippy --lib` clean. The distributed DML write cutover is complete: DELETE/UPDATE/MERGE/equality-delete all BE-written, FE commits metadata only.

## Out of scope (non-goals)

- **Per-partition equality-delete routing.** Phase 4 preserves the unpartitioned-only limitation (the in-process writer never supported partitioned; a BE sink *could* route per-partition, but that's a feature, not a cutover). The reject at `equality_delete_flow.rs:89-93` stays.
- StarRocks-mode / FE-driven equality deletes (standalone only).
- IMV refresh's `inject_delete_group`/`RowDeltaDv` paths (live MV-refresh use; not part of this cutover).

## Risks

1. **Thrift regen** (Q1): adding a `TDataSinkType` value needs the generated Rust enum updated. Confirm the regen mechanism early; if a committed generated file exists, regenerate it. NovaRocks-only value (FE/C++ shim never receives `ICEBERG_EQUALITY_DELETE_SINK`) — safe.
2. **Equality-delete sink input schema** (Q2): the sink input is the equality-KEY columns (the `VALUES` projection), a genuinely new schema shape vs Data/position-delete sinks. Get the field-id resolution right so `equality_ids` matches; the byte-identical golden gates correctness.
3. **`VALUES`-rooted write sink** (Q3): the source query is a pure synthetic `VALUES` relation with no table scan. The MERGE matched-delete fold already plans a `VALUES`-rooted DV write, so the planner supports it — confirm during Q3; if not, materialize via a trivial wrapper.
4. **Gate simplification safety** (Q6): condition #2 removal is safe only because every shape is covered by #1 (real files) or #3 (`!FastAppend`). Keep equality-delete on `CommitOpKind::RowDelta` (its files land in the flat channel → condition #1). The runner tests are the gate.
