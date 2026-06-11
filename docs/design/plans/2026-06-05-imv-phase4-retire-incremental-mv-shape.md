# IMV Phase 4 — Retire Iceberg-path `IncrementalMvShape`; enable composed branch-union refresh

> **For agentic workers:** REQUIRED SUB-SKILL: superpowers:subagent-driven-development (recommended) or superpowers:executing-plans. Steps use checkbox (`- [ ]`) syntax.

**Goal:** Fully retire `IncrementalMvShape` + `classify_incremental_mv_query` from the **Iceberg** IMV path, and enable **homogeneous-base composed branch-union refresh** (`UNION ALL` of `Agg(Join)` / `Agg(fan-in)`) end-to-end. The StarRocks (non-Iceberg) MV path keeps `IncrementalMvShape` unchanged.

**Architecture (refined after focused investigation — see below):** The blocker was misdiagnosed earlier. Refresh re-derives per-aggregate `function`/`input` by re-classifying the stored SELECT via the full `IncrementalMvShape` classifier; the persisted contract is **not** on this path and needs **no extension**. The real obstacle is that `IncrementalMvShape`'s `classify_single_union_branch` structurally **rejects joins inside union branches** (`mv_shape.rs:258`), and the classifier is used both to build the aggregate **layout** and as the composed-branch **rejection gates**. Phase 4 introduces a **focused aggregate-call extractor** (`AggregateSqlCalls = {group_keys, aggregates, visible_outputs}` — the non-base subset of `AggregateMvShape`, reusing existing types) that parses the stored SELECT's aggregate calls + group-by **without** union/join/base classification; the layout + SQL rewrites source from it. The merge operator/codegen derive visible-output ordering from the **layout** (which already encodes it) instead of the shape. The 6 composed-branch rejection gates are lifted by routing branch decomposition through the already-working property + `RewriteBranchUnion` path. The CREATE-time homogeneity gate is **retained** (composed support is homogeneous-base only; no per-branch lineage / `Vec<BranchContract>` needed — refresh does not consume branch lineage).

**Tech Stack:** Rust. New: a focused extractor (reusing `mv_shape.rs` types). Touches `mv_agg_state.rs` (layout), `aggregate_state_merge.rs` + `fragment_builder.rs` (operator/codegen ordering), `mv_shape.rs::rewrite_select_sql_for_state` + `ivm_delta_aggregate.rs` (SQL rewrites), `refresh_context.rs` + `iceberg_refresh.rs` + `refresh_property.rs` (refresh/CREATE routing + gates). Tests: `cargo test --lib`, `iceberg-ivm` e2e (server per CLAUDE.md §7.3).

**Canonical spec:** [docs/design/specs/2026-06-04-iceberg-imv-refresh-property-framework-design.md](../specs/2026-06-04-iceberg-imv-refresh-property-framework-design.md) §13/§15. **Builds on** Phases 1–3 (PR #254). This branch: `claude/imv-phase4` (off #254 tip); PR fork→main, separate from #254.

---

## Locked decisions (from the focused investigation; user-confirmed)
- **Focused extractor (3a), NOT a contract extension.** Refresh parses the stored SELECT regardless (the SQL rewrites need input exprs); a single focused extractor feeding both the layout and the rewrites is simpler than persisting `function` (which only helps the layout). No `AggregateStateColumnContract`/`MvSchemaContract` change.
- **Homogeneous-base composed only.** Keep the `derive_from_set_operation` homogeneity gate (refresh_property.rs ~957-976). No per-branch lineage / `Vec<BranchContract>` — `validate_schema_contract` is per-base and the rewrite works off the logical plan, so first-branch lineage is a faithful representative under homogeneity. (Heterogeneous-base composed stays rejected at CREATE.)
- **`RewriteBranchUnion` is NOT a blocker** — it already composes `Union(Agg(Join))` end-to-end (proven by `pipeline_branch_union_of_aggregate_over_join_composes`). The blockers are the `IncrementalMvShape`-classifier gates.
- **StarRocks path untouched** (`mv_ddl.rs`/`mv_refresh.rs`/`mv_apply_policy.rs` + the `mv_shape.rs` enum/classifier/types stay; `create_mv` rejects iceberg at line 116).

## The 6 gates to lift (investigation-confirmed)
1. `into_refresh_contract` — `refresh_property.rs:~552` `Some(BranchShape::Composed) => Err(...)`.
2. `classify_single_union_branch` join rejection — `mv_shape.rs:258-259` (dominant; removed by routing branch decomposition off the classifier).
3. `validate_branch_union_aggregate_base_refs` — `iceberg_refresh.rs:491-540` (`fan_in_bases.is_empty()`, `branches.len()==base_refs.len()`).
4. `branch_union_aggregate_shape_for_refresh` + `first_union_aggregate_branch` — `iceberg_refresh.rs:467-489, 1158-1166` (require single-scan `Aggregate` branches).
5. `build_aggregate_contract_core` / `first_branch_loaded_bases` CREATE re-classify — `iceberg_refresh.rs:1389-1506, 1749-1773`.
6. `validate_branch_union_aggregate_branch_layout` — `iceberg_refresh.rs:6737` (arity-only; coupled to shape).

---

## File Structure
- **Create** `src/connector/starrocks/table/aggregate_sql_calls.rs` — `AggregateSqlCalls { group_keys: Vec<GroupKeyShape>, aggregates: Vec<AggregateCallShape>, visible_outputs: Vec<VisibleAggregateOutput> }` + `extract_aggregate_sql_calls(&sqlparser::ast::Query) -> Result<AggregateSqlCalls, String>` (reuses `mv_shape.rs` types + the `classify_aggregate_call`/`classify_*_input`/`aggregate_group_by_exprs`/`projection_expr_and_output_name` helpers — make those `pub(crate)` if needed). NO union/join/base classification.
- **Modify** `mv_agg_state.rs` — `build_aggregate_mv_layout_with_input_types` takes `&AggregateSqlCalls`; layout gains a derived `visible_outputs` accessor (or the existing fields suffice). `aggregate_shape_needs_retraction_count_state` takes `&AggregateSqlCalls`.
- **Modify** `aggregate_state_merge.rs` + `fragment_builder.rs` + `mv_agg_state.rs::materialize_*` — derive visible-output ordering from the layout; drop the `shape` field from the physicalize plan/operator.
- **Modify** `mv_shape.rs::rewrite_select_sql_for_state` + `ivm_delta_aggregate.rs` — take `&AggregateSqlCalls`.
- **Modify** `refresh_context.rs::aggregate_shape_and_layout_for_execution` + `iceberg_refresh.rs` (refresh/CREATE routing + the 6 gates) + `refresh_property.rs` (gate #1).
- **Tests**: extractor unit tests; `iceberg-ivm` composed-refresh e2e + suite.

---

## Task P4.1 — Focused aggregate-call extractor

**Files:** Create `src/connector/starrocks/table/aggregate_sql_calls.rs`; register in `mod.rs`; make reused `mv_shape.rs` helpers `pub(crate)`.

- [ ] **Step 1: failing tests** — `extract_aggregate_sql_calls` over (a) `SELECT k, sum(v) FROM t GROUP BY k` → `{group_keys:[k], aggregates:[sum(v)], visible_outputs:[GroupKey(0),Aggregate(0)]}`; (b) `SELECT k, sum(v) FROM (a JOIN b ...) GROUP BY k` → SAME aggregate-call output (the join in FROM is IGNORED — this is the point); (c) `SELECT k, count(*), max(x) ...` → correct functions + `count_star`. Assert it does NOT reject the join branch.
- [ ] **Step 2: run, FAIL.**
- [ ] **Step 3: implement.** `AggregateSqlCalls` struct (reuse `GroupKeyShape`/`AggregateCallShape`/`VisibleAggregateOutput` from `mv_shape.rs`). `extract_aggregate_sql_calls(query)` parses the SELECT's projection + GROUP BY: classify each projection item as group-key (matches a GROUP BY expr) or aggregate (via `classify_aggregate_call` → function + `classify_*_input` → input), building `visible_outputs`. Reuse `mv_shape.rs:413-470, 757-1008` helpers (promote to `pub(crate)`). It must work for an aggregate over ANY FROM (scan/join/union) — it only reads the projection + GROUP BY, never the FROM structure. (Per investigation: this is exactly the non-base subset of `AggregateMvShape`.)
- [ ] **Step 4: run, PASS.** `cargo build --lib` clean; fmt; clippy clean on the new module.
- [ ] **Step 5: commit** `feat(imv): focused aggregate-call extractor (AggregateSqlCalls) for the Iceberg path`.

## Task P4.2 — Build the aggregate layout + drive operator/codegen ordering, from the extractor/layout (behavior-preserving)

**Files:** `mv_agg_state.rs` (`build_aggregate_mv_layout_with_input_types`, `aggregate_shape_needs_retraction_count_state`, `materialize_aggregate_result_batch`/`compute_batch_col_indexes`/`validate_state_shaped_input_schema`), `aggregate_state_merge.rs` (physicalize plan/operator), `fragment_builder.rs` (`aggregate_state_shaped_output_columns`), `engine/mod.rs` (`DirectExecPlan::AggregateStatePhysicalize`).

- [ ] **Step 1:** change `build_aggregate_mv_layout_with_input_types(shape: &AggregateMvShape, ...)` → `(calls: &AggregateSqlCalls, ...)`. (Per investigation, it reads only `aggregates.{function,input,output_name}` + `visible_outputs` + arity — all in `AggregateSqlCalls`.) The layout already encodes the visible-output interleaving via `group_key_source_indexes` + state-column `visible_source_index`.
- [ ] **Step 2:** drive `materialize_aggregate_result_batch` / `compute_batch_col_indexes` / `validate_state_shaped_input_schema` (mv_agg_state.rs ~475-741), `aggregate_state_shaped_output_columns` (fragment_builder.rs ~177-240), and the physicalize operator (aggregate_state_merge.rs ~420-468) **from the layout** (derive a `visible_outputs`-equivalent from `group_key_source_indexes` + `state_columns[].visible_source_index`) instead of holding/reading an `AggregateMvShape`. Remove the `shape` field from `AggregateStatePhysicalizePlan`/processor + the `DirectExecPlan::AggregateStatePhysicalize { shape }` thread.
- [ ] **Step 3:** run `cargo test --lib engine::mv:: connector::starrocks::table::mv_agg_state -- --nocapture` → green (behavior-preserving). Build/fmt/clippy clean.
- [ ] **Step 4: commit** `refactor(imv): build aggregate layout from AggregateSqlCalls; drive merge operator/codegen ordering from the layout`.

## Task P4.3 — De-shape the SQL rewrites (behavior-preserving)

**Files:** `mv_shape.rs::rewrite_select_sql_for_state` (~1675), `ivm_delta_aggregate.rs` (signed-delta rewriter), `aggregate_rewrite.rs` (the `shape.visible_outputs` capacity/ordering uses).

- [ ] **Step 1:** change `rewrite_select_sql_for_state(select_sql, shape: &AggregateMvShape)` → `(..., calls: &AggregateSqlCalls)`; it reads only `visible_outputs`, `group_keys[].{expr,output_name}`, `aggregates[].{function,input,output_name}` (all in `AggregateSqlCalls`). Same for `ivm_delta_aggregate.rs`'s signed-delta projection rewriter and `aggregate_shape_needs_retraction_count_state`.
- [ ] **Step 2:** `aggregate_rewrite.rs` uses only `shape.visible_outputs` (+ capacity hints) — switch to `AggregateSqlCalls` or derive ordering from the layout (it's in `build_aggregate_state_merge` which has the layout).
- [ ] **Step 3:** run `cargo test --lib -- --nocapture` for the touched modules → green. Build/fmt/clippy clean.
- [ ] **Step 4: commit** `refactor(imv): de-shape the IMV SQL rewrites onto AggregateSqlCalls`.

## Task P4.4 — Route refresh/CREATE through the extractor + property; lift the 6 composed-branch gates (ENABLES composed refresh)

**Files:** `refresh_context.rs::aggregate_shape_and_layout_for_execution` (~265), `iceberg_refresh.rs` (the 6 gates + the branch-union refresh/CREATE paths), `refresh_property.rs` (gate #1).

- [ ] **Step 1: failing test** — a pipeline/integration test (unit-level if feasible, else rely on P4.6 e2e) that a homogeneous-base `Union(Agg(Join))` refresh contract derivation + layout build SUCCEEDS (no classifier rejection). At minimum, assert `into_refresh_contract` now ACCEPTS `BranchShape::Composed` (gate #1 lifted) and the contract builds.
- [ ] **Step 2:** lift the 6 gates:
  - #1 `refresh_property.rs:~552`: `Composed` → accept (build the `BranchUnionAggregate` contract).
  - #2: `aggregate_shape_and_layout_for_execution` (refresh_context.rs:274) + the refresh/CREATE callers no longer call `classify_incremental_mv_query`; they use `extract_aggregate_sql_calls` for the aggregate-call model and the **property** for branch structure/dispatch. The branch decomposition is already handled by `RewriteBranchUnion` (logical plan) — refresh sources per-branch aggregate calls via the extractor on each branch's SELECT.
  - #3 `validate_branch_union_aggregate_base_refs` (iceberg_refresh.rs:491): relax `fan_in_bases.is_empty()` + `branches.len()==base_refs.len()` to allow composed (multi-base) branches under the homogeneity guarantee.
  - #4 `branch_union_aggregate_shape_for_refresh` / `first_union_aggregate_branch`: source the per-branch aggregate-call model from the extractor (works for agg-over-join), not the single-scan `IncrementalMvShape::Aggregate` requirement.
  - #5 `build_aggregate_contract_core` / `first_branch_loaded_bases` (CREATE): build branch lineage from the property/logical-plan + extractor, not `classify_incremental_mv_query`.
  - #6 `validate_branch_union_aggregate_branch_layout`: arity check off `AggregateSqlCalls`.
  - KEEP the `derive_from_set_operation` homogeneity gate (homogeneous-base only).
- [ ] **Step 3:** run `cargo test --lib engine::mv:: -- --nocapture` → green (supported shapes unchanged; composed now builds).
- [ ] **Step 4: commit** `feat(imv): enable homogeneous-base composed branch-union refresh via property + extractor; lift classifier gates`.

## Task P4.5 — Retire `IncrementalMvShape` from the Iceberg path

**Files:** `iceberg_refresh.rs`, `refresh_context.rs` (remove the now-unused `classify_incremental_mv_query`/`IncrementalMvShape` Iceberg callers + imports). Do NOT touch `mv_shape.rs` (def/classifier/types stay for StarRocks) or `mv_ddl.rs`/`mv_refresh.rs`/`mv_apply_policy.rs`.

- [ ] **Step 1:** `cargo build --lib 2>&1` after P4.4 — find remaining Iceberg-path `IncrementalMvShape`/`classify_incremental_mv_query` refs; remove each (their purpose is now served by `AggregateSqlCalls` + the property). `grep -rn "IncrementalMvShape\|classify_incremental_mv_query" src/engine/mv/` → only tests or zero.
- [ ] **Step 2:** confirm StarRocks path still compiles + uses the classifier (`grep` in `connector/starrocks/table/{mv_ddl,mv_refresh,mv_apply_policy}.rs` unchanged).
- [ ] **Step 3:** `cargo test --lib engine::mv:: -- --nocapture` + `cargo build --lib` clean; fmt; clippy.
- [ ] **Step 4: commit** `refactor(imv): retire Iceberg-path IncrementalMvShape (now sourced from AggregateSqlCalls + property)`.

## Task P4.6 — e2e: composed refresh + no regression

**Files:** `sql-tests/iceberg-ivm/sql/iceberg_ivm_union_of_aggregate_over_join.sql` (new); update `iceberg_ivm_union_shape_rejects_unsupported.sql` (the composed-over-join query that was "not yet supported" is now SUPPORTED — move it to the new positive test; heterogeneous-base composed stays rejected via the homogeneity gate, keep that query).

- [ ] **Step 1:** new sql-test: homogeneous-base `UNION ALL` of `Agg(a JOIN b)` × 2 — CREATE + INSERT into a base + REFRESH, assert result == full-recompute (the `iceberg-ivm` cross-check convention). Follow existing join-using iceberg-ivm test setup.
- [ ] **Step 2:** in `iceberg_ivm_union_shape_rejects_unsupported.sql`, remove/relocate the composed-over-join query (now supported); keep a HETEROGENEOUS-base composed query asserting it's still rejected (`@expect_error` = the homogeneity-gate message).
- [ ] **Step 3 (controller-orchestrated):** build server; run the full `iceberg-ivm` suite `--mode verify` → all green (no regression + composed refresh works + heterogeneous still rejected). Record the new test's golden with `--mode record --record-from target`.
- [ ] **Step 4: commit** `test(imv): end-to-end homogeneous-base composed branch-union refresh; update union-shape rejection scope`.

---

## Self-Review
- **Spec coverage:** retires Iceberg `IncrementalMvShape` (P4.1-P4.5) + enables homogeneous composed refresh (P4.4, P4.6) — spec §13/§15 Phase 4. The investigation's "focused extractor, no contract change, homogeneous-only, 6 gates" is realized by P4.1 (extractor), P4.2-P4.3 (de-shape execution), P4.4 (gates + enable), P4.5 (retire), P4.6 (e2e).
- **Placeholder scan:** P4.2-P4.5 are transformation-over-mapped-sites (authority = the investigation's file:line); P4.4's gate-lifting is enumerated (the 6 gates). The extractor (P4.1) carries full code. The runtime-critical changes (layout/operator/codegen) are behavior-preserving (P4.2-P4.3) and gated by the unit suite + the P4.6 e2e.
- **Risk:** P4.2 (operator/codegen de-shape) + P4.4 (gate-lifting) touch runtime-critical merge/codec → P4.6 iceberg-ivm e2e is the mandatory gate (unit tests insufficient for runtime correctness).
- **Type consistency:** `AggregateSqlCalls` (P4.1) is consumed by the layout builder (P4.2), SQL rewrites (P4.3), and refresh/CREATE routing (P4.4); reuses `mv_shape.rs` types verbatim.

## Execution Handoff
Subagent-driven (recommended): P4.1 → P4.2 → P4.3 → P4.4 → P4.5 → P4.6, two-stage review each; the e2e tasks (P4.6) controller-orchestrated. Behavior-preserving tasks (P4.2/P4.3) guarded by the unit suite; the enabling tasks (P4.4/P4.6) guarded by the iceberg-ivm e2e. Finish → PR `claude/imv-phase4` (fork) → main, separate from #254.

---

## Outcome (2026-06-05)

**Landed (P4.1–P4.4, P4.6):** the focused `AggregateSqlCalls` extractor; the layout/operator/codegen and SQL rewrites de-shaped onto it; the 6 composed-branch gates lifted; **homogeneous-base composed branch-union refresh enabled and validated end-to-end** (iceberg-ivm 67/67; the new `iceberg_ivm_union_of_aggregate_over_join` case cross-checks MV == full recompute across initial + insert + delete and matches exactly). Two refresh-side fixes the e2e surfaced were folded in: `RefreshCapabilities::from_schema_contract` now admits the composed `(join, agg, branch)` shape with `has_branch` taking snapshot-policy precedence, and `explain_iceberg_mv_refresh_rewrite_plan` derives `has_agg_state` from the persisted contract instead of re-classifying (so EXPLAIN REFRESH works on a composed union). The branch-union refresh path, the EXPLAIN agg-gate, and the shared delta path (`refresh_context.rs` + `aggregate_rewrite.rs`, now 0 `AggregateMvShape`/`classify` refs) no longer classify.

**P4.5 deferred (scope discovery).** P4.5 as written assumed "remove now-unused classify callers," but the non-composed shapes (single-aggregate, join-aggregate, fan-in, projection-filter, join-projection-filter, union-projection-filter) were **never** migrated off `classify_incremental_mv_query` — `iceberg_refresh.rs` still has ~22 active `classify` calls + ~54 `IncrementalMvShape`/`AggregateMvShape` refs feeding those paths' layout/first-refresh/contract derivation. Full retirement means de-classifying every shape onto property+contract+extractor (the extractor is aggregate-call-only; join/base structure must come from the persisted contract) — a large, runtime-critical, capability-neutral refactor. By decision (2026-06-05), the composed-refresh deliverable lands now and full IncrementalMvShape retirement is tracked as a separate follow-up PR.

**Known limitation (pre-existing, documented).** A composed branch whose aggregate sits over a *filtered* join — `Aggregate(Filter(Join))` — is not refreshable: the join-aggregate delta rule (`join_delta.rs`) matches `Aggregate(Join)` directly, so a `Filter` between the aggregate and the join is not delta-expanded. This is a pre-existing join-delta limitation (a plain filtered join-aggregate MV hits the same wall) independent of branch-union composition; the e2e test differentiates branches by aggregate input rather than a `WHERE` filter to stay on the supported join shape. Extending join-delta through `Filter(Join)` is a separate follow-up.

**Update (2026-06-06).** Resolved by the join-delta decomposition (`docs/design/specs/2026-06-06-imv-join-delta-decomposition-design.md`): `Aggregate(Filter(Join))` and multi-level inner/cross join nesting now refresh incrementally. join-of-aggregate and nested join-projection remain deferred.
