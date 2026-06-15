# Phase 2: Distributed MERGE matched-delete (reuse the DV sink) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Cut `MERGE ... WHEN MATCHED THEN DELETE` from the in-process build-groups-and-inject path to Phase 1's distributed `DeletionVectors` sink (BE writes the merged Puffin DV; FE commits metadata only), reusing the Phase 1 machinery verbatim.

**Architecture:** A MERGE is **N independent Iceberg commits** today (the match is materialized once, then each non-empty branch commits separately — verified in `mutation_flow.rs`), so matched-delete is cut over on its own with no atomicity change. The matched-delete row set is expressed as a distributed `SELECT t._file, t._pos, <partition src> FROM <target> t JOIN <source> s ON <on> [WHERE <matched-AND-pred>]`, fed to the existing `DeletionVectors` sink (shuffled by output column 0 = `_file`) and committed via `RowDeltaDvFromFiles`. The DV bitmap naturally dedups a target row matched by multiple source rows; MERGE cardinality is still validated coordinator-side by the orchestrator's existing `validate_unique_target_row_ids`.

**Tech Stack:** Rust; reuses Phase 1 (`IcebergWriteSinkMode::DeletionVectors`, `iceberg_write_shuffle_by_output_index`, `RowDeltaDvFromFilesCommit`, `DistributedDvDeleteWriteExecutor`); sql-test-runner.

**Profile note:** `cargo test --lib` (profile `dev`) for unit/guard tests; `--profile dev-opt` only for SQL-suite runs.

---

## What Phase 1 left reusable (as-merged, verified)

- `src/engine/delete_flow.rs:304` `run_delete_dv_write_transaction(state, target, catalog, table, entry, base_snapshot_id, target_ref, where_clause)` — builds the `SELECT _file,_pos,<part> ... WHERE <where>` query, flips `sink_spec.mode = DeletionVectors`, `set_planned_snapshot_id`, runs `DistributedDvDeleteWriteExecutor` with `CommitOpKind::RowDeltaDvFromFiles`. **The only DELETE-specific bit is `build_delete_position_sink_query` (a `WHERE` query).** Everything after the query is op-agnostic → extract and reuse.
- `src/engine/delete_flow.rs:259` `DistributedDvDeleteWriteExecutor` — runs `execute_query_as_iceberg_write(query, sink_spec, None, Some(iceberg_write_shuffle_by_output_index(0)))`. Query-agnostic.
- `src/engine/mod.rs:3433` `iceberg_write_shuffle_by_output_index(0)` — per-`_file` shuffle (output column 0 = `_file`).
- `src/engine/iceberg_writer.rs:485` `position_delete_sink_input_columns` → `[_file, _pos, <partition src cols>]` (the sink's `target_columns`, `_file` first).
- `CommitOpKind::RowDeltaDvFromFiles` + `RowDeltaDvFromFilesCommit` (metadata-only; BE wrote the Puffin DVs).

## What's local today (to replace) — `src/engine/mutation_flow.rs`

- `execute_merge_matched_delete` (`:2329-2372`) — packages the coordinator-materialized `matched: MatchedUpdateBatch` into `MutationWritePlan::MergeMatchedDelete` + `run_mutation_write_transaction(CommitOpKind::RowDeltaDv)`.
- `MutationWriteExecutor::run_coordinated_write` arm `MutationWritePlan::MergeMatchedDelete` (`:814-825`) — `build_position_delete_groups_from_matched(&matched, ...)` + `self.collector.inject_delete_group(group)` (the local inject; central FE-side DV write at commit). **This is what Phase 2 removes for matched-delete.**
- `build_position_delete_groups_from_matched` (`:734`) — **keep** (MOR-UPDATE still uses it at `:682`).

## File Structure

| File | Change |
|---|---|
| `sql-tests/iceberg-dml/sql/merge_matched_delete.sql` (+ `.result`) | NEW behavioral test for `WHEN MATCHED THEN DELETE` (none exists today) |
| `src/engine/delete_flow.rs` | extract `run_dv_delete_write_transaction_with_query(...)` (pub(crate)); `run_delete_dv_write_transaction` builds the WHERE query then calls it |
| `src/engine/mutation_flow.rs` | add `build_merge_matched_delete_dv_query`; rewire `execute_merge_matched_delete` to the distributed helper; remove the `MergeMatchedDelete` local arm + variant |

---

## Task 1: Add the missing `WHEN MATCHED THEN DELETE` SQL test (behavioral anchor)

No MERGE-matched-delete test exists; add one first so the cutover is provably behavior-preserving (record on the current local path, then it must stay identical after cutover).

**Files:** Create `sql-tests/iceberg-dml/sql/merge_matched_delete.sql` + `result/merge_matched_delete.result`

- [ ] **Step 1: Write the case** (covers matched-delete with a source-referencing AND predicate + a combined matched-delete/not-matched-insert)

```sql
-- @order_sensitive=true
-- @tags=iceberg_dml,merge,delete
-- query 1
-- @skip_result_check=true
DROP TABLE IF EXISTS ${case_db}.t_mmd_target;
DROP TABLE IF EXISTS ${case_db}.t_mmd_source;
CREATE TABLE ${case_db}.t_mmd_target (id INT, g INT, v INT)
  PARTITION BY bucket(g, 4)
  TBLPROPERTIES ("format-version" = "3", "write.row-lineage" = "true");
CREATE TABLE ${case_db}.t_mmd_source (id INT, g INT, drop_it INT)
  TBLPROPERTIES ("format-version" = "3");
INSERT INTO ${case_db}.t_mmd_target VALUES (1,10,100),(2,20,200),(3,30,300),(4,40,400);
INSERT INTO ${case_db}.t_mmd_source VALUES (2,20,1),(3,30,0),(5,50,1);
-- delete target rows matched by a source row with drop_it>0 (id 2); insert unmatched source (id 5)
MERGE INTO ${case_db}.t_mmd_target AS t
  USING ${case_db}.t_mmd_source AS s
  ON t.id = s.id
  WHEN MATCHED AND s.drop_it > 0 THEN DELETE
  WHEN NOT MATCHED THEN INSERT (id, g, v) VALUES (s.id, s.g, 0);
-- query 2
SELECT id, g, v FROM ${case_db}.t_mmd_target ORDER BY id;
-- query 3
-- @skip_result_check=true
DROP TABLE ${case_db}.t_mmd_target FORCE;
DROP TABLE ${case_db}.t_mmd_source FORCE;
```

- [ ] **Step 2: Record on the CURRENT (local) path + verify it passes**

Run (server up per CLAUDE.md §7.3):
```bash
cargo run --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests -- \
  --config "$NOVAROCKS_SQL_TEST_CONFIG" --suite iceberg-dml --only merge_matched_delete --mode record
cargo run --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests -- \
  --config "$NOVAROCKS_SQL_TEST_CONFIG" --suite iceberg-dml --only merge_matched_delete --mode verify
```
Expected: result rows `id` 1,3,4,5 (id 2 deleted by matched-DELETE; id 5 inserted). `verify` PASS. This `.result` is the invariant the cutover must preserve.

- [ ] **Step 3: Commit**

```bash
git add sql-tests/iceberg-dml/sql/merge_matched_delete.sql sql-tests/iceberg-dml/result/merge_matched_delete.result
git commit -m "test(iceberg-dml): add MERGE WHEN MATCHED THEN DELETE case (behavioral baseline)"
```

---

## Task 2: Extract a query-agnostic distributed DV-delete transaction helper (DRY)

**Files:** Modify `src/engine/delete_flow.rs` (`run_delete_dv_write_transaction`, `:304-406`)
**Test:** existing DV-delete guard/tests (`engine::delete_flow::tests`) must stay green.

- [ ] **Step 1: Extract the helper.** Split `run_delete_dv_write_transaction` so the query construction is the only DELETE-specific part:

```rust
/// Run a distributed v3 deletion-vector write for a pre-built position-delete
/// SELECT (projecting `_file, _pos, <partition src cols>`, `_file` first).
/// Shared by standalone DELETE and MERGE matched-delete.
pub(crate) fn run_dv_delete_write_transaction_with_query(
    state: &Arc<StandaloneState>,
    target: &crate::engine::backend_resolver::TargetBackend,
    catalog: Arc<dyn iceberg::Catalog>,
    table: iceberg::table::Table,
    entry: crate::connector::iceberg::catalog::IcebergCatalogEntry,
    base_snapshot_id: Option<i64>,
    target_ref: &str,
    delete_query: sqlparser::ast::Query,
) -> Result<(), String> {
    let resolved = {
        let registry = state.connectors.read().expect("connector registry read");
        let backend = registry.catalog_backend("iceberg")?;
        backend.load_table(&target.catalog, &target.namespace, &target.table)?
    };
    let mut sink_spec = crate::engine::iceberg_writer::build_position_delete_sink_spec(
        target, &resolved, &table, &entry,
    )?;
    sink_spec.mode = IcebergWriteSinkMode::DeletionVectors;
    sink_spec.set_planned_snapshot_id(base_snapshot_id)?;
    // ... identical to current run_delete_dv_write_transaction body from here:
    //     build collector (CommitOpKind::RowDeltaDvFromFiles) + commit_executor + spec
    //     + DistributedDvDeleteWriteExecutor { ..., delete_query, sink_spec, ... }
    //     + IcebergWriteTransactionRunner::new(state, &executor).run(spec)
    // (move lines :316-405 of the current fn here verbatim, using `delete_query` param)
}
```

Then `run_delete_dv_write_transaction` becomes a thin caller:

```rust
fn run_delete_dv_write_transaction(
    state: &Arc<StandaloneState>,
    target: &crate::engine::backend_resolver::TargetBackend,
    catalog: Arc<dyn iceberg::Catalog>,
    table: iceberg::table::Table,
    entry: crate::connector::iceberg::catalog::IcebergCatalogEntry,
    base_snapshot_id: Option<i64>,
    target_ref: &str,
    where_clause: &sqlast::Expr,
) -> Result<(), String> {
    // sink columns come from the spec built inside the helper; build the WHERE query
    // using the same target_columns the helper will project. Build the spec once here
    // only to obtain target_columns for the query, OR keep build_delete_position_sink_query
    // taking the resolved sink columns (it already takes `sink_columns: &[ColumnDef]`).
    let resolved = { /* same load_table as helper */ };
    let sink_spec = crate::engine::iceberg_writer::build_position_delete_sink_spec(target, &resolved, &table, &entry)?;
    let delete_query = build_delete_position_sink_query(target, where_clause, &sink_spec.target_columns, target_ref)?;
    run_dv_delete_write_transaction_with_query(state, target, catalog, table, entry, base_snapshot_id, target_ref, delete_query)
}
```

(If building the spec twice is undesirable, have the helper return early-built `target_columns`, or pass `sink_spec` in. Simplest correct form: build the spec inside the helper as shown and let the caller build the query from `position_delete_sink_input_columns(&resolved, &table)` directly — `position_delete_sink_input_columns` is the same source `build_position_delete_sink_spec` uses for `target_columns`.)

- [ ] **Step 2: Build + run the existing DV-delete tests**

Run: `cargo test --lib engine::delete_flow::tests`
Expected: PASS (no behavior change — pure extraction). Re-verify one DV-delete SQL case (`dv_delete_distributed`) PASSes.

- [ ] **Step 3: Commit**

```bash
git add src/engine/delete_flow.rs
git commit -m "refactor(delete): extract query-agnostic run_dv_delete_write_transaction_with_query"
```

---

## Task 3: Build the matched-delete DV-sink query

**Files:** Modify `src/engine/mutation_flow.rs` (add `build_merge_matched_delete_dv_query`)
**Test:** inline unit test on the generated SQL (pattern: `merge_unmatched_insert_query_uses_distributed_append_shape`, `mutation_flow.rs:2560`)

- [ ] **Step 1: Failing unit test**

```rust
#[test]
fn merge_matched_delete_dv_query_projects_file_pos_and_joins_source() {
    let raw = crate::sql::parser::parse_sql_raw(
        "MERGE INTO t AS t USING (SELECT 2 AS id, 1 AS drop_it) AS s \
         ON t.id = s.id WHEN MATCHED AND s.drop_it > 0 THEN DELETE",
    ).expect("parse");
    let stmt = crate::engine::statement::convert_sqlparser_merge_to_custom(&raw).expect("convert");
    let target_columns = vec![col("id"), col("g")];
    // partition source cols empty for this unpartitioned target in the test
    let sql = build_merge_matched_delete_dv_query(&iceberg_target(), &stmt, None, &target_columns, "main")
        .expect("query").to_string();
    assert!(sql.contains("`_file`"), "{sql}");
    assert!(sql.contains("`_pos`"), "{sql}");
    assert!(sql.to_uppercase().contains("JOIN"), "{sql}");
    assert!(sql.contains("(s.drop_it > 0)"), "{sql}");
    // _file must be the FIRST projected column (shuffle key = output index 0)
    let sel = sql.split_once("SELECT ").unwrap().1;
    assert!(sel.trim_start().starts_with("t.`_file`") || sel.trim_start().starts_with("`_file`"), "{sql}");
}
```

- [ ] **Step 2: Run — expect FAIL** (fn doesn't exist).

- [ ] **Step 3: Implement** (model on `build_merge_match_query_sql`, `:2186-2257`, but project the position-delete sink columns from the target alias `t`, inner-join source, filter to the matched-clause AND predicate):

```rust
/// Build `SELECT t._file, t._pos, <partition src cols> FROM <target> t
///        JOIN <source> s ON <on> [WHERE <matched AND pred>]` for matched-delete.
/// `_file` is projected first so the DV sink can shuffle by output index 0.
/// A target row matched by multiple source rows yields duplicate (file,pos) rows;
/// the BE DeletionVector dedups them (set semantics), and MERGE cardinality is
/// validated coordinator-side by validate_unique_target_row_ids before this runs.
fn build_merge_matched_delete_dv_query(
    target: &crate::engine::backend_resolver::TargetBackend,
    stmt: &MergeStmt,
    base_snapshot_id: Option<i64>,
    target_columns: &[crate::engine::catalog::ColumnDef],
    target_ref: &str,
) -> Result<sqlparser::ast::Query, String> {
    // sink columns = [_file, _pos, <partition source columns>], _file first
    let sink_columns = crate::engine::iceberg_writer::position_delete_sink_projection_names(target_columns);
    let projection = sink_columns.iter()
        .map(|c| format!("t.{}", sql_identifier(c)))
        .collect::<Vec<_>>().join(", ");
    let on = render_merge_on_expr(&stmt.on);                       // reuse the ON renderer used by build_merge_match_query_sql
    let matched_and = match stmt.matched.as_ref().and_then(|c| c.predicate.as_ref()) {
        Some(p) => format!(" AND ({})", render_expr(p)),
        None => String::new(),
    };
    let version = if target_ref == "main" { String::new() }
                  else { format!(" FOR VERSION AS OF {}", sql_string_literal(target_ref)) };
    let sql = format!(
        "SELECT {projection} FROM {target_tbl} AS t{version} \
         JOIN ({source}) AS s ON {on}{matched_and}",
        target_tbl = qualify_iceberg_table(target),
        source = render_merge_source_subquery(&stmt.source),       // reuse source renderer from build_merge_match_query_sql
    );
    let _ = base_snapshot_id; // snapshot pinning handled by sink_spec.set_planned_snapshot_id in the helper
    parse_generated_query(&sql, "MERGE matched-delete DV rewrite")
}
```

Reuse the existing renderers from `build_merge_match_query_sql` (`render_merge_on_expr`/source subquery/expr rendering — factor them `pub(self)` if currently inline). Add `position_delete_sink_projection_names` to `iceberg_writer.rs` returning the `[_file,_pos,<part>]` names (it already builds these as `ColumnDef`s in `position_delete_sink_input_columns`, `:485`).

- [ ] **Step 4: Run — expect PASS; Step 5: Commit**

```bash
git add src/engine/mutation_flow.rs src/engine/iceberg_writer.rs
git commit -m "feat(merge): build distributed matched-delete DV-sink query"
```

---

## Task 4: Rewire `execute_merge_matched_delete` to the distributed DV path

**Files:** Modify `src/engine/mutation_flow.rs` (`execute_merge_matched_delete`, `:2329-2372`; its call site `:1856-1864`)
**Test:** source-introspection guard.

- [ ] **Step 1: Failing guard test**

```rust
#[test]
fn merge_matched_delete_uses_distributed_dv_sink_not_inject() {
    let src = include_str!("mutation_flow.rs");
    let body = src.split("fn execute_merge_matched_delete").nth(1).expect("fn")
        .split("\n fn ").next().expect("body");
    assert!(body.contains("run_dv_delete_write_transaction_with_query")
            || body.contains("build_merge_matched_delete_dv_query"),
        "matched-delete must use the distributed DV path");
    assert!(!body.contains("MergeMatchedDelete"),
        "matched-delete must not build the local MutationWritePlan::MergeMatchedDelete");
    assert!(!body.contains("inject_delete_group"),
        "matched-delete must not inject delete groups in the coordinator");
}
```

- [ ] **Step 2: Run — expect FAIL.**

- [ ] **Step 3: Rewire.** `execute_merge_matched_delete` no longer needs the materialized `matched` batch; it builds the JOIN query and calls the shared helper. Update its signature (drop `matched`, add `stmt`/`current_catalog`/`target_columns`) and its call site at `:1856-1864`:

```rust
fn execute_merge_matched_delete(
    state: &Arc<StandaloneState>,
    target: &crate::engine::backend_resolver::TargetBackend,
    catalog: Arc<dyn Catalog>,
    table: iceberg::table::Table,
    entry: crate::connector::iceberg::catalog::IcebergCatalogEntry,
    stmt: &MergeStmt,
    target_columns: &[crate::engine::catalog::ColumnDef],
) -> Result<StatementResult, String> {
    let base_snapshot_id = table.metadata().current_snapshot().map(|s| s.snapshot_id());
    let delete_query = build_merge_matched_delete_dv_query(target, stmt, base_snapshot_id, target_columns, "main")?;
    crate::engine::delete_flow::run_dv_delete_write_transaction_with_query(
        state, target, catalog, table, entry, base_snapshot_id, "main", delete_query,
    )?;
    Ok(StatementResult::Ok)
}
```

At the call site (`:1856-1864`), the orchestrator still materializes the match for cardinality validation (`validate_unique_target_row_ids`, `:1810`) and the not-matched-insert / matched-update branches; for the `MergeMatchedAction::Delete` arm it now calls the new `execute_merge_matched_delete(state, &target, catalog, table, entry, stmt, &target_columns)`. (The `matched` batch is no longer threaded into delete; the distributed query re-derives the matched rows. Note: the join runs twice — once for validation/materialization, once for the DV write — a known cost; see Out-of-scope.)

- [ ] **Step 4: Run guard + the mutation_flow test module**

Run: `cargo test --lib engine::mutation_flow::tests`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add src/engine/mutation_flow.rs
git commit -m "feat(merge): cut matched-delete to distributed DeletionVectors sink"
```

---

## Task 5: Remove the dead local matched-delete arm

**Files:** Modify `src/engine/mutation_flow.rs`

- [ ] **Step 1: Delete** the `MutationWritePlan::MergeMatchedDelete { matched }` variant (`:762-765`) and its handling arm in `MutationWriteExecutor::run_coordinated_write` (`:814-825`). **Keep** `build_position_delete_groups_from_matched` (`:734`) — MOR-UPDATE still calls it at `:682`. Grep to confirm `MergeMatchedDelete` has no other references.

Run: `rg "MergeMatchedDelete" src/` → expect no hits after removal.

- [ ] **Step 2: Build + tests + the new guard**

Run: `cargo build --lib && cargo test --lib engine::mutation_flow::tests engine::delete_flow::tests`
Expected: PASS; no unused-variant/`unused` warnings.

- [ ] **Step 3: Commit**

```bash
git add src/engine/mutation_flow.rs
git commit -m "refactor(merge): remove dead local matched-delete inject path"
```

---

## Task 6: End-to-end verification (all-in-one + 1FE+2BE)

- [ ] **Step 1: Verify the Task 1 case still passes on the new distributed path (all-in-one), byte-identical**

Run: `... --suite iceberg-dml --only merge_matched_delete --mode verify`
Expected: PASS — same `.result` recorded in Task 1 (proves behavior preserved).

- [ ] **Step 2: Verify 1FE+2BE** (the real distribution check)

```bash
cargo build --profile dev-opt
NOVAROCKS_BIN=target/dev-opt/novarocks NO_PROXY=127.0.0.1,localhost \
cargo run --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests -- \
  --config "$NOVAROCKS_SQL_TEST_CONFIG" --suite iceberg-dml --only merge_matched_delete \
  --mode verify --cluster-mode cross-process --cluster-size 2 -j 1
```
Expected: PASS — identical result across 2 BEs (per-`_file` shuffle + BE DV write + merged-DV correctness for matched-delete).

- [ ] **Step 3: Regression sweep of the MERGE suite**

Run: `... --suite iceberg-dml --only merge_into_cow,merge_into_mor,merge_into_upsert_delete,merge_matched_delete --mode verify`
Expected: PASS (matched-update/insert paths unchanged).

- [ ] **Step 4: Commit** (if any `.result` updates were needed — there should be none)

```bash
git commit -am "test(iceberg-dml): verify distributed MERGE matched-delete (all-in-one + 1FE+2BE)" || echo "nothing to commit"
```

---

## Acceptance (Phase 2 done when all true)

- [ ] `merge_matched_delete_uses_distributed_dv_sink_not_inject` guard passes; no `MergeMatchedDelete` / `inject_delete_group` in `execute_merge_matched_delete`.
- [ ] `merge_matched_delete.sql` passes in **both** all-in-one and `--cluster-mode cross-process --cluster-size 2`, byte-identical to the Task 1 baseline.
- [ ] `merge_into_cow` / `merge_into_mor` / `merge_into_upsert_delete` still green.
- [ ] FE writes no DV for matched-delete (the Puffin DVs are BE-written via the `DeletionVectors` sink → `RowDeltaDvFromFiles`).
- [ ] `cargo fmt` + `cargo clippy --lib` clean.

## Out of scope (later phases / follow-ups)

- **MOR-UPDATE delete side** still uses `CommitOpKind::RowDeltaDv` + `collector.inject_delete_group` (`mutation_flow.rs:682`) — i.e. central FE-side DV write, which violates the hard invariant. The study surfaced this; it should be migrated to the `DeletionVectors` sink + `RowDeltaDvFromFiles` too (fold into Phase 4 COW/UPDATE work or a dedicated follow-up). Phase 2 does not touch it.
- equality-delete (Phase 3), COW-update (Phase 4), removal of shared `local_writer_commit_input`/`has_preloaded_commit_output` (Phase 5).
- **Double match compute**: the orchestrator materializes the match (for cardinality validation + other branches) and the distributed matched-delete re-runs the join. Correct but redundant; optimize later (e.g. skip re-materialization when matched-delete is the only branch, or derive cardinality from the distributed query).

## Risk note

The matched-delete predicate may reference source columns (`WHEN MATCHED AND s.x THEN DELETE`), so the query must be a JOIN (not a semi-join). Duplicate `(_file,_pos)` from multi-match are deduped by the BE `DeletionVector` (set semantics); MERGE cardinality (at-most-one-match) is still enforced coordinator-side by the orchestrator's existing `validate_unique_target_row_ids`, so duplicates should not occur in valid MERGEs — the dedup is a safety net.
