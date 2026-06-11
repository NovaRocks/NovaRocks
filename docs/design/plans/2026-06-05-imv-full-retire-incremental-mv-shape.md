# Full Iceberg-path `IncrementalMvShape` Retirement — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: superpowers:subagent-driven-development (recommended) or superpowers:executing-plans. Steps use checkbox (`- [ ]`) syntax.

**Goal:** Remove `classify_incremental_mv_query` / `IncrementalMvShape` (and the `From<&AggregateMvShape> for AggregateSqlCalls` bridge) entirely from the **Iceberg** IMV refresh path, so every refresh shape sources its layout / first-refresh / CREATE-contract from the persisted `MvSchemaContract` + the focused `extract_aggregate_sql_calls` extractor (+ one new tiny join-alias extractor) — finishing the Phase-4 de-classification for the 5 shapes the branch-union path did not cover.

**Architecture:** The branch-union path (Phase 4) already proved the pattern: dispatch on `RefreshCapabilities` (from the persisted contract), source the aggregate-call model from `extract_aggregate_sql_calls(&Query)` (FROM-agnostic), and let `MvSchemaContract.bases[]` supply base FQNs/aliases. This plan applies that pattern to the remaining shapes — **single-aggregate, fan-in aggregate, join-aggregate, join projection/filter, union projection/filter** — then deletes the classifier from the Iceberg path. **Behavior-preserving**: the existing iceberg-ivm e2e suite (67 cases) is the guard; `extract_aggregate_sql_calls` and the old `From<&AggregateMvShape>` bridge share `classify_aggregate_select_outputs`, so the aggregate-call model is byte-identical by construction.

**Tech Stack:** Rust. One new focused extractor (`extract_join_aliases`, reusing `mv_shape::table_factor_name_and_alias`). Touches `src/engine/mv/iceberg_refresh.rs` (the 17 non-test sites + driver signatures), `src/connector/starrocks/table/aggregate_sql_calls.rs` (new extractor + delete the bridge at the end). Validation: `cargo test --lib` + the iceberg-ivm e2e (server orchestrated per CLAUDE.md §7.3).

**Grounding:** the file:line map this plan is built on lives in the session investigation (2026-06-05). **Re-grep every site before editing** — line numbers drift as tasks land.

**Branch:** `claude/imv-retire-incremental-mv-shape` (off `origin/main`, all Phase 1–4 merged). PR fork→main when done.

---

## Locked decisions (from the scope map)
- **No genuine blocker.** Every shape field consumed by a refresh/plan/CREATE path is recoverable WITHOUT classify: aggregate calls → `extract_aggregate_sql_calls`; base FQNs/aliases → `MvSchemaContract.bases[].{table_fqn, alias_at_create}`; branch count → `contract.branch.branch_count` / `union_branch_count(&Query)`. The legacy `join_keys` raw-expr field is **never read** by any refresh/plan path (the JOIN ON condition lives in the stored `select_sql` AST, executed as-is).
- **One new extractor only:** `extract_join_aliases(&Query) -> JoinAliases{left_table,left_alias,right_table,right_alias}` — the join `left_alias`/`right_alias` is the single execution-load-bearing field (used by `rewrite_join_full_refresh_query` + `rewrite_join_branch_query`). Extract from the AST (FROM = left, JOIN = right → unambiguous) reusing `mv_shape::table_factor_name_and_alias`.
- **StarRocks path is OUT OF SCOPE and stays:** `mv_shape.rs` (the `IncrementalMvShape` def + `classify_incremental_mv_query` + `classify_aggregate_select_outputs`, which `extract_aggregate_sql_calls` reuses), `mv_ddl.rs`, `mv_refresh.rs`, `mv_apply_policy.rs`, `mv_agg_state.rs`, `txn.rs`, `ivm_delta_aggregate.rs`. Do not touch them.
- **Behavior-preserving.** No new capability, no contract change. Each task keeps the mapped iceberg-ivm test(s) green; no golden may change. Rebind sites re-`extract_aggregate_sql_calls` off the *rewritten* SQL (the rewrite changed column names in the SELECT; the extractor reads them) — same data the old rebind-classify produced.

## The 17 non-test sites (re-grep to confirm), grouped by task
- **R1 single-agg:** layout/first-refresh — sites at `aggregate_shape_from_query` (~935), dispatch agg arm (~2476), `refresh_iceberg_aggregate_mv` SingleBase sub-arm (~3156), single-agg rebind (~3358).
- **R2 fan-in:** dispatch AllBasesRequired+GroupRowId (~2476/3198 fan-in sub-arm), `validate_aggregate_fan_in_base_refs` (~437), plan fan-in (~5231).
- **R3 join-agg:** `refresh_iceberg_aggregate_mv` join sub-arm (~3198), join-agg rebind (~4305), plan join-agg (~5458), base-ref matching (~5458/`join_base_refs_for_shape` ~7979).
- **R4 join-pf:** dispatch join arm (~2529), plan join arm (~4700), drivers `refresh_iceberg_join_mv` (~7767)/`first_refresh_iceberg_join_mv` (~8185)/`incremental_refresh_iceberg_join_mv` (~9201)/`execute_join_delta_branches` (~9307), `validate_join_shape_base_refs` (~417), `join_base_refs_for_shape` (~7979).
- **R5 union-pf:** dispatch union arm (~2553), plan union arm (~4652), `refresh_iceberg_union_projection_mv` (~2831)/`plan_iceberg_union_projection_mv_refresh` (~5032), validators (~656/689).
- **R6 CREATE builders:** `representative_aggregate_shape` (~915)/`aggregate_shape_from_query` (~935), `build_aggregate_contract_core` (~1385), `build_non_branch_contract_core` JoinRowKey arm + `join_shape_from_query` (~1344/1790), `build_branch_union_schema_contract` first-branch (~1607) + `first_branch_loaded_bases` (~1753). Confirm the single-base projection/filter REFRESH arm (~2584) carries no shape.
- **R7 deletion:** `mv_shape` import (~35), `aggregate_shape_for_layout` (~815), `is_join_projection_filter_mv` (~811, already dead), `first_union_aggregate_branch` (~1145, becomes dead), the `From<&AggregateMvShape> for AggregateSqlCalls` bridge (`aggregate_sql_calls.rs:38-46`), and the 5 `mod tests` sites (~10650+).

---

## Standing verification protocol (every task)
Implementer (subagent) does, before commit:
- `cargo build --lib` clean; `cargo fmt`; `cargo clippy --lib` no new warnings.
- `cargo test --lib engine::mv:: && cargo test --lib connector::starrocks::table` green.
Controller (between tasks, server orchestrated per CLAUDE.md §7.3 with `NOVAROCKS_READY` gating): run the task's mapped iceberg-ivm cases `--mode verify`; **goldens must NOT change** (behavior-preserving). The final task runs the full `iceberg-ivm` suite (expect 67/0).

---

## Task R0 — focused `extract_join_aliases` extractor

**Files:** `src/connector/starrocks/table/aggregate_sql_calls.rs` (add); `src/connector/starrocks/table/mv_shape.rs` (promote `table_factor_name_and_alias` to `pub(crate)` if private).

- [ ] **Step 1: failing tests** — in `aggregate_sql_calls.rs` `#[cfg(test)]`: (a) `SELECT ... FROM fact f JOIN dim d ON f.dim_id=d.id` → `JoinAliases{left_table:"fact", left_alias:Some("f"), right_table:"dim", right_alias:Some("d")}`; (b) no-alias join `FROM fact JOIN dim ON ...` → aliases `None`, tables filled; (c) a non-join SELECT → `Err`. Model parsing on the existing `parse_query` test helper.
- [ ] **Step 2: run, FAIL.** `cargo test --lib aggregate_sql_calls 2>&1 | tail`.
- [ ] **Step 3: implement** `pub(crate) struct JoinAliases { left_table: String, left_alias: Option<String>, right_table: String, right_alias: Option<String> }` + `pub(crate) fn extract_join_aliases(query: &sqlparser::ast::Query) -> Result<JoinAliases, String>`. Read the single top-level `SELECT`'s `from[0]` (relation = left) and its first `join` (relation = right); lift `mv_shape::table_factor_name_and_alias` (promote to `pub(crate)`) to get `(table, alias)` for each side. `Err` if the FROM is not exactly a two-relation inner join shape (a focused mirror of what the join paths expect; do NOT re-validate join keys — they are never read). English error messages.
- [ ] **Step 4: run, PASS.** Build/fmt/clippy clean.
- [ ] **Step 5: commit** `feat(imv): focused join-alias extractor (extract_join_aliases) for the Iceberg path`.

## Task R1 — single-aggregate refresh off the extractor

**Files:** `iceberg_refresh.rs` (`refresh_iceberg_aggregate_mv` SingleBase sub-arm ~3156, `refresh_single_aggregate_iceberg_mv` ~3251 + its `aggregate_shape:&AggregateMvShape` param ~3263, the single-agg rebind reclassify ~3358, the dispatch agg arm ~2476).

- [ ] **Step 1: baseline e2e (controller)** — run `iceberg_ivm_aggregate_target`, `iceberg_ivm_aggregate_min_max_insert_only`, `iceberg_ivm_aggregate_min_max_delete_boundary` `--mode verify`; confirm green (records the pre-change golden baseline).
- [ ] **Step 2: re-thread.** Change `refresh_single_aggregate_iceberg_mv` to take `&AggregateSqlCalls` instead of `&AggregateMvShape`. At the dispatch agg arm (~2476), source `AggregateSqlCalls` via `extract_aggregate_sql_calls(&canonical_select_query)` instead of `classify_incremental_mv_query` + `aggregate_shape_for_layout` + `AggregateSqlCalls::from(&shape)`. Replace the single-agg rebind reclassify (~3358) with `extract_aggregate_sql_calls(&rewritten_query)`. The `refresh_iceberg_aggregate_mv` SingleBase sub-arm (~3156) destructure `IncrementalMvShape::Aggregate(..)` is removed — pass `AggregateSqlCalls` through.
- [ ] **Step 3: unit guards** — `cargo test --lib engine::mv:: && cargo test --lib connector::starrocks::table` green; build/fmt/clippy clean.
- [ ] **Step 4: e2e (controller)** — re-run the Step-1 cases + `iceberg_ivm_aggregate_min_max_{string,date,decimal128,timestamp,float}`, `iceberg_ivm_aggregate_bool_and`, `iceberg_ivm_aggregate_count_only_delete_boundary`, `iceberg_ivm_partitioned_aggregate_target`, `iceberg_ivm_aggregate_a11_base_rename_group_key` `--mode verify`; all green, **no golden diff**.
- [ ] **Step 5: commit** `refactor(imv): single-aggregate refresh sources AggregateSqlCalls, not the classifier`.

## Task R2 — fan-in aggregate refresh off the extractor + contract bases

**Files:** `iceberg_refresh.rs` (`refresh_iceberg_aggregate_mv` AllBasesRequired+GroupRowId fan-in sub-arm ~3198, `validate_aggregate_fan_in_base_refs` ~437, `plan_iceberg_all_bases_aggregate_mv_refresh` fan-in ~5231).

- [ ] **Step 1: baseline e2e** — `iceberg_ivm_fan_in_aggregate_union`, `iceberg_ivm_aggregate_base_partition_evolution` `--mode verify` green.
- [ ] **Step 2: re-thread.** Fan-in sub-arm sources `AggregateSqlCalls` via `extract_aggregate_sql_calls`. Replace `validate_aggregate_fan_in_base_refs` (which read `shape.fan_in_bases`) with a check that the resolved `base_refs` are the fan-in base set — the validator's invariant is "fan_in == resolved", trivially satisfied from `base_refs` / `contract.bases[]`; drop the shape param. Plan fan-in (~5231) drops its classify.
- [ ] **Step 3: unit guards** green.
- [ ] **Step 4: e2e** — Step-1 cases green, no golden diff.
- [ ] **Step 5: commit** `refactor(imv): fan-in aggregate refresh sources extractor + contract bases`.

## Task R3 — join-aggregate refresh off extractor + join-alias extractor

**Files:** `iceberg_refresh.rs` (`refresh_iceberg_aggregate_mv` join sub-arm ~3198, `refresh_join_aggregate_iceberg_mv` ~4198 + params `join_aggregate_shape`/`aggregate_shape` ~4210, join-agg rebind ~4305, `plan_iceberg_aggregate_mv_refresh` join-agg arm ~5458, `join_base_refs_for_shape` ~7979).

- [ ] **Step 1: baseline e2e** — `iceberg_ivm_join_aggregate`, `iceberg_ivm_join_aggregate_min_max`, `iceberg_ivm_partitioned_join_aggregate_dim_move`, `iceberg_ivm_join_aggregate_a11_base_rename_join_key` `--mode verify` green.
- [ ] **Step 2: re-thread.** Replace `refresh_join_aggregate_iceberg_mv`'s `join_aggregate_shape`/`aggregate_shape` params with `JoinAliases` (from `extract_join_aliases`) + `&AggregateSqlCalls` (from `extract_aggregate_sql_calls`). The aliases feed `rewrite_join_full_refresh_query`/`rewrite_join_branch_query` (the only execution use); base-ref matching (`join_base_refs_for_shape`) uses `contract.bases[].table_fqn` (or `JoinAliases.{left,right}_table`). Join-agg rebind (~4305) → `extract_aggregate_sql_calls(&rewritten_query)` (aliases unchanged by rebind — re-extract or carry forward). Plan arm (~5458) drops classify.
- [ ] **Step 3: unit guards** green.
- [ ] **Step 4: e2e** — Step-1 cases + `iceberg_ivm_join_aggregate_a11_base_rename_group_key`, `iceberg_ivm_join_aggregate_base_partition_evolution` green, no golden diff. **The `a11_base_rename_join_key` case is the critical guard that alias re-sourcing survives a rebind.**
- [ ] **Step 5: commit** `refactor(imv): join-aggregate refresh sources extractor + join-alias extractor`.

## Task R4 — join projection/filter refresh off the join-alias extractor

**Files:** `iceberg_refresh.rs` (dispatch join arm ~2529, plan join arm ~4700, drivers `refresh_iceberg_join_mv` ~7767 / `first_refresh_iceberg_join_mv` ~8185 / `incremental_refresh_iceberg_join_mv` ~9201 / `execute_join_delta_branches` ~9307, `validate_join_shape_base_refs` ~417, `rewrite_join_full_refresh_query` ~8258 / `rewrite_join_branch_query` ~9373).

- [ ] **Step 1: baseline e2e** — `iceberg_ivm_join_two_base_delta`, `iceberg_ivm_join_key_update_multiplicity`, `iceberg_ivm_join_a11_base_drop_referenced`, `iceberg_ivm_join_a11_base_type_change_referenced`, `iceberg_ivm_join_reject_unsupported` `--mode verify` green.
- [ ] **Step 2: re-thread.** Replace the `shape:&JoinProjectionFilterMvShape` param threaded through the join drivers with a small carrier holding `{left_table,left_alias,right_table,right_alias}` (= `JoinAliases`, from `extract_join_aliases`). `validate_join_shape_base_refs` matches against `contract.bases[]`. Dispatch (~2529) + plan (~4700) arms drop classify and build `JoinAliases` instead. The `rewrite_join_*` functions read aliases from the carrier (unchanged behavior).
- [ ] **Step 3: unit guards** green.
- [ ] **Step 4: e2e** — Step-1 cases green, no golden diff (`join_reject_unsupported` still rejects with the same error).
- [ ] **Step 5: commit** `refactor(imv): join projection/filter refresh sources the join-alias extractor`.

## Task R5 — union projection/filter refresh off contract + branch count

**Files:** `iceberg_refresh.rs` (dispatch union arm ~2553, plan union arm ~4652, `refresh_iceberg_union_projection_mv` ~2831 / `plan_iceberg_union_projection_mv_refresh` ~5032, `validate_union_projection_shape_base_refs` ~656 / `validate_union_projection_schema_contract_for_base` ~689).

- [ ] **Step 1: baseline e2e** — `iceberg_ivm_union_projection_filter`, `iceberg_ivm_union_shape_rejects_unsupported` `--mode verify` green.
- [ ] **Step 2: re-thread.** Replace `union_shape:&UnionAllMvShape` with `branch_count` (from `contract.branch.branch_count`, else `union_branch_count(&canonical_select_query)`) + base-ref-set validation against `contract.bases[]` (the validators currently read per-branch `base_table` + `branches.len()`; both are in the contract). Dispatch (~2553) + plan (~4652) arms drop classify.
- [ ] **Step 3: unit guards** green.
- [ ] **Step 4: e2e** — Step-1 cases green, no golden diff (`union_shape_rejects_unsupported` still rejects all 7 cases with the same errors).
- [ ] **Step 5: commit** `refactor(imv): union projection/filter refresh sources contract + branch count`.

## Task R6 — CREATE-side contract builders off extractor + contract

**Files:** `iceberg_refresh.rs` (`representative_aggregate_shape` ~915 / `aggregate_shape_from_query` ~935, `build_aggregate_contract_core` ~1385, `build_non_branch_contract_core` JoinRowKey arm ~1344 + `join_shape_from_query` ~1790, `build_branch_union_schema_contract` first-branch ~1607 + `first_branch_loaded_bases` ~1753).

- [ ] **Step 1: re-thread CREATE.** `representative_aggregate_shape`/`aggregate_shape_from_query` collapse to `extract_aggregate_sql_calls`. `build_aggregate_contract_core` sources aggregate calls via the extractor and join lineage via `extract_join_aliases` + the resolved `MvAnalysis` already computed at CREATE (the contract's `join`/`aggregate`/`bases` sections are built from analysis + extractor, not classify). `join_shape_from_query` (CREATE join lineage) → `extract_join_aliases` (+ analysis for predicates field-ids). `build_branch_union_schema_contract` first-branch + `first_branch_loaded_bases` source via extractor + per-branch AST + `contract.bases`. Confirm the single-base projection/filter REFRESH arm (~2584) carries no shape (no change expected).
- [ ] **Step 2: unit guards** green (`engine::mv` includes CREATE-contract unit tests).
- [ ] **Step 3: e2e (controller)** — run a CREATE-heavy cross-section: `iceberg_ivm_aggregate_target`, `iceberg_ivm_join_aggregate`, `iceberg_ivm_fan_in_aggregate_union`, `iceberg_ivm_union_projection_filter`, `iceberg_ivm_union_of_aggregate_over_join` (composed — must stay green), `iceberg_backed_mv_basic_lifecycle` `--mode verify`; no golden diff.
- [ ] **Step 4: commit** `refactor(imv): CREATE schema-contract builders source extractor + analysis, not the classifier`.

## Task R7 — delete `IncrementalMvShape` / `classify_incremental_mv_query` from the Iceberg path

**Files:** `iceberg_refresh.rs` (import ~35, `aggregate_shape_for_layout` ~815, `is_join_projection_filter_mv` ~811, `first_union_aggregate_branch` ~1145, the 5 `mod tests` sites ~10650+), `aggregate_sql_calls.rs` (the `From<&AggregateMvShape> for AggregateSqlCalls` bridge ~38-46 + its now-unused `AggregateMvShape` import).

- [ ] **Step 1: delete.** `cargo build --lib 2>&1` after R1–R6 — every remaining Iceberg-path `classify_incremental_mv_query` / `IncrementalMvShape` / `aggregate_shape_for_layout` / `From<&AggregateMvShape>` ref should now be dead. Remove them + the dead helpers (`is_join_projection_filter_mv`, `first_union_aggregate_branch`) + the bridge + test-only references. Update/remove the 5 test sites (they tested classify-driven paths now sourced differently).
- [ ] **Step 2: grep clean.** `grep -rn "classify_incremental_mv_query\|IncrementalMvShape\|aggregate_shape_for_layout" src/engine/mv/` → zero (only `src/connector/starrocks/table/**` StarRocks refs remain). `grep -rn "From<&AggregateMvShape>\|AggregateMvShape" src/engine/mv/ src/sql/optimizer/rewrite/imv/` → zero. Confirm StarRocks path still compiles (`mv_ddl.rs`/`mv_refresh.rs`/`mv_apply_policy.rs` untouched).
- [ ] **Step 3: unit guards** — `cargo build --lib` clean; `cargo test --lib` (broad) green; fmt; clippy (no new warnings; expect net reduction).
- [ ] **Step 4: FULL e2e (controller)** — `iceberg-ivm --mode verify` → **67/0, no golden diff**.
- [ ] **Step 5: commit** `refactor(imv): retire IncrementalMvShape + classify_incremental_mv_query from the Iceberg path`.

---

## Self-Review
- **Spec coverage:** R1–R6 migrate the 5 remaining shapes' (refresh + plan + CREATE) classify sites; R0 supplies the one new datum (join aliases); R7 deletes the classifier + bridge from the Iceberg path. Maps 1:1 to the scope-map inventory (17 non-test sites + drivers).
- **Placeholder scan:** R0 carries full code (the only genuinely new code). R1–R7 are transformation-over-mapped-sites — authority is the scope map's file:line + per-site demand; each task names the exact functions and what they become. The runtime-critical sites (first-refresh/layout) are byte-identical by construction (`extract_aggregate_sql_calls` shares `classify_aggregate_select_outputs` with the deleted bridge).
- **Type consistency:** `AggregateSqlCalls` (existing) + `JoinAliases` (R0) are the only carriers threaded through R1–R6; the deleted bridge `From<&AggregateMvShape>` and `aggregate_shape_for_layout` go in R7 once nothing references them.
- **Risk:** highest at R3/R4 (join alias is the one execution-load-bearing field) — guarded by the `*_a11_base_rename_join_key` rebind e2e; and at R1 first-refresh/layout — guarded by the aggregate_min_max delete-boundary e2e. Every task is behavior-preserving with **no golden change** as the hard gate.
- **Coverage:** every shape #1–#5 has existing iceberg-ivm e2e (mapped per task); composed branch-union (Phase 4) stays as a regression guard. No new e2e test required.

## Execution Handoff
Subagent-driven (recommended): R0 → R1 → R2 → R3 → R4 → R5 → R6 → R7, two-stage review each (spec + quality); the per-task e2e is controller-orchestrated (server gated on `NOVAROCKS_READY`). Behavior-preservation (no golden diff) is the gate at every task; R7 runs the full suite. Finish → PR `claude/imv-retire-incremental-mv-shape` (fork) → main.
