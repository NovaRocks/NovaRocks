# Iceberg V3 MERGE INTO Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build SQL `MERGE INTO ... USING ... ON ... WHEN MATCHED ... WHEN NOT MATCHED INSERT` for Iceberg v3 row-lineage tables on top of the UPDATE executor landed by `2026-05-04-iceberg-v3-update-merge.md`.

**Status gate:** Do not start this plan until the UPDATE plan's stages 1–3 are committed and validated:

- COW UPDATE correctness with stable row lineage (Tasks 1–6).
- MOR UPDATE correctness with DV plus updated rows (Task 7).
- MV incremental refresh integration for COW and MOR (Tasks 9–10).

The MERGE plumbing depends on the shared mutation executor (`engine::mutation_flow`) and the MV change planner extensions made in those stages.

**Tech Stack:** Rust, sqlparser, Apache Arrow `RecordBatch`, vendored `iceberg-rust` 0.9 APIs, NovaRocks `sql-tests`.

---

## Scope

This plan covers MERGE only. The UPDATE work is already merged. The plan is intentionally staged so each stage is testable on its own:

1. Parse `MERGE INTO` into a single `MergeStmt` AST node.
2. Lower `MergeStmt` to a list of `MutationAction`s per matched/not-matched row.
3. Reuse the COW / MOR executors from the UPDATE plan to apply the matched rows; reuse the existing fast-append path for `WHEN NOT MATCHED INSERT`.
4. Detect duplicate target `_row_id`s in the matched set before any commit; fail fast with the same error wording the UPDATE path emits.
5. Add MV change-planner classification for MERGE-produced snapshots (they reuse the existing UPDATE markers; one MERGE may emit one or two snapshots — one for matched, one for inserts — depending on how the executor sequences them).

Out of scope:

- Schema evolution inside MERGE.
- `WHEN NOT MATCHED BY SOURCE` (Iceberg/Spark dialect extension).
- MERGE on partition-evolved tables; the UPDATE plan already constrains UPDATE to non-partition columns and that constraint extends here.
- Optimizer rewrites of MERGE (planner stays at the executor level).

---

## File Structure

- Modify `src/sql/parser/ast/mod.rs`: add `MergeStmt`, `MergeWhenClause`, `MergeMatchedAction`, `MergeNotMatchedAction`.
- Modify `src/engine/statement.rs`: add conversion from sqlparser `Statement::Merge` into the new AST.
- Modify `src/engine/mod.rs`: dispatch `Statement::Merge` to the new mutation_flow entrypoint.
- Modify `src/engine/mutation_flow.rs`: add `execute_merge_statement`. Reuse `materialize_update_matches` and `mutation_source_to_sql` introduced in the UPDATE plan; add `materialize_merge_actions` to project rows into per-action vectors.
- Modify `src/connector/iceberg/changes.rs` and `src/connector/starrocks/managed/ivm_change_stream.rs`: extend the MV change planner so a MERGE snapshot pair (matched UPDATE + unmatched INSERT) is treated as a single logical change set.
- Add SQL tests under `sql-tests/iceberg/` and `sql-tests/mv-on-iceberg/` for MERGE COW + MERGE MOR + MERGE MV refresh.

---

### Task 1: Add MERGE AST nodes and route from sqlparser

**Files:** `src/sql/parser/ast/mod.rs`, `src/engine/statement.rs`, `src/engine/mod.rs`.

- [ ] Define `MergeStmt`, `MergeWhenClause`, `MergeMatchedAction { Update { assignments }, Delete }`, `MergeNotMatchedAction { Insert { columns, values_or_query } }`.
- [ ] Convert sqlparser `Statement::Merge` → `MergeStmt`. Reject lateral source subqueries, multiple `WHEN MATCHED` clauses without `AND` predicates, and `WHEN NOT MATCHED BY SOURCE` for now.
- [ ] Route `Statement::Merge` to `mutation_flow::execute_merge_statement`.

### Task 2: Resolve target + source and build matched / unmatched plans

**Files:** `src/engine/mutation_flow.rs`.

- [ ] Reuse `resolve_existing_table_target` for the target.
- [ ] Reuse `mutation_source_to_sql` for the source.
- [ ] Build a single SELECT that LEFT JOINs source onto target on the user's `ON` predicate; project `_file`, `_pos`, `_row_id`, target columns, source columns, plus a synthetic `__nr_match_kind` column derived from `target._row_id IS NOT NULL`.
- [ ] Run the SELECT once via `execute_update_match_query`.

### Task 3: Materialize matched rows as MutationAction::Update

**Files:** `src/engine/mutation_flow.rs`.

- [ ] For rows where `__nr_match_kind = 'matched'`, validate target `_row_id` uniqueness (mirror the UPDATE path's `validate_unique_target_row_ids`). On duplicates, fail fast with the existing wording: ``UPDATE source matched target row _row_id={id} more than once``.
- [ ] Translate matched-action assignments into the existing `MatchedUpdateBatch` shape and dispatch through `execute_cow_update` / `execute_mor_update`.

### Task 4: Materialize unmatched rows as INSERT

**Files:** `src/engine/mutation_flow.rs`.

- [ ] For rows where `__nr_match_kind = 'unmatched'`, project the `WHEN NOT MATCHED INSERT (cols) VALUES (...)` columns into a `RecordBatch` aligned with the target schema.
- [ ] Append via the existing iceberg fast-append path (`InsertCommit`) so a single MERGE produces an UPDATE snapshot plus an INSERT snapshot in lineage order — both already classifiable by the MV change planner.

### Task 5: MERGE INTO ... WHEN MATCHED DELETE

**Files:** `src/engine/mutation_flow.rs`.

- [ ] Translate `WHEN MATCHED DELETE` into the existing position-delete / DV path. For row-lineage tables this is a no-op extension of `delete_flow::execute_delete_statement` over the matched subset.

### Task 6: MV change planner integration

**Files:** `src/connector/iceberg/changes.rs`, `src/connector/starrocks/managed/ivm_change_stream.rs`.

- [ ] Confirm `LineageAction::CollectCowUpdate` / `CollectMorUpdate` already cover the MERGE-produced UPDATE snapshot. The INSERT snapshot is already classified as `CollectInserts`.
- [ ] Add a regression test confirming the planner walks both snapshots and produces a single materialized batch for MV refresh.

### Task 7: SQL regression coverage

**Files:** `sql-tests/iceberg/`, `sql-tests/mv-on-iceberg/`.

- [ ] `iceberg_v3_merge_cow.sql`: MERGE on a default-update-mode v3 table; assert merged rows + new row appear exactly once.
- [ ] `iceberg_v3_merge_mor.sql`: MERGE on a `merge-on-read` v3 table; assert same observable result.
- [ ] `managed_lake_mv_merge_cow.sql` / `managed_lake_mv_merge_mor.sql`: MV refresh after MERGE reflects matched updates + inserts.

### Task 8: Final verification

- [ ] `cargo fmt --check`, `cargo test --lib`.
- [ ] Run the four new SQL cases via the standalone-server runner on a private port.
- [ ] Summarize: MERGE supports MATCHED UPDATE/DELETE and NOT MATCHED INSERT for v3 row-lineage targets; reuses the UPDATE executor and the MV change planner.

---

## Why a separate plan

MERGE is straightforward to layer on top of the executor that the UPDATE plan delivered, but it adds non-trivial parser/AST surface and a new SELECT shape (LEFT JOIN with match-kind classification). Keeping it in its own plan means the UPDATE work could ship and bake before MERGE either lands or surfaces a missing executor primitive that requires UPDATE-side changes.
