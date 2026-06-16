# DistributedPlan IR — Execution Cutover, Legacy-Visitor Deletion & Multi-Fragment ANALYZE

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Finish the spec's **single-source** goal (§2: "EXPLAIN 与执行都从 IR 派生") and the spec's deferred M0-S6/S7/S8: make `build_distributed_plan`/`lower_distributed_plan` the **only** builder for execution too, **delete the legacy `fragment_builder` visitor**, and make **EXPLAIN ANALYZE work on multi-fragment** plans (not just forced-single-fragment).

**Why:** Today (after M0 #318/#322, M1 #328, M2 #330) EXPLAIN renders from the IR, but **execution still uses the legacy `PlanFragmentBuilder::build_with_mv_refresh_ctx`/`build_with_iceberg_sink`**, and the legacy `visit_*` visitor (~9k lines) still exists. Two builders are kept in lock-step by `equiv.rs`. And `explain_analyze_query` **forces single-fragment** (multi-fragment ANALYZE is rejected). This plan closes all of that.

**Architecture:** Spec `docs/superpowers/specs/2026-06-15-plannode-ir-explain-observability-design.md` §2/§9 (M0-S6 cutover, M0-S7 direct-exec/mv passthrough, M0-S8 delete visitor) + §F multi-fragment ANALYZE. Strategy: **first make the IR builder cover everything the legacy builder does for execution** (direct-exec / mv-refresh / iceberg-sink), gated by the existing byte-identical `equiv.rs`; **then** flip the engine call sites and run the full SQL suites; **then** delete the legacy visitor; **then** thread profiling through the multi-fragment coordinator.

**Tech Stack:** Rust, `cargo test`, `sql-tests` runner (`--mode verify`), live standalone server for the multi-fragment ANALYZE smoke.

**Branch:** `claude/dist-plan-ir-cutover` (off `origin/main` = #330).

**Verbatim anchors (current, #330):**
- Legacy direct-exec: `fragment_builder.rs` `build_with_mv_refresh_ctx:600` (short-circuits at `:647-665`), `try_build_branch_union_aggregate_direct:920`, `build_aggregate_state_merge_direct:760` + `_with_layout:796`, `build_with_iceberg_sink:514`.
- `DirectExecPlan` enum: `src/sql/codegen/mod.rs:38` (`AggregateStateMerge`/`AggregateStatePhysicalize`/`UnionAll`); consumed by `lower_plan_build_result` (`engine/mod.rs:4102-4164`).
- IR entries: `build_distributed_plan` (`ir/build.rs:659`, pure structure, catch-all error `:564` on `PhysicalAggregateStateMerge`), `build_via_distributed_plan` (`fragment_builder.rs:489`, no mv_refresh_ctx), `lower_distributed_plan` (`ir/lowering.rs:53`, hardcodes `direct_exec: None` `:115`, no mv_refresh_ctx). IR `DistributedPlan`/`PlanFragment` (`ir/fragment.rs:56,69`) have no `direct_exec`; `DistributedPlanNodeKind` (`ir/node.rs:59`) has no `AggregateStateMerge`.
- Engine call sites: execute `build_with_mv_refresh_ctx` (`engine/mod.rs:3731`), insert `build_with_iceberg_sink` (`:3543`), test helper `build` (`:5976`); `choose_standalone_execution:2918`; `execute_plan:4196` (takes `profiler`); `explain_query`/`explain_analyze_query` already on IR (`:3263`).
- Equiv harness: `ir/equiv.rs` `build_both_paths:510` (legacy `build` vs `build_via_distributed_plan`), `assert_multi_fragment_equivalent:561`.
- Multi-fragment ANALYZE: `explain_analyze_query:3224` (collapse `:3262`, reject Coordinated `:3268`), `ExecutionCoordinator` (`runtime/coordinator.rs:67`, no profiler), `InProcessDispatcher` (`runtime/dispatcher.rs:346`; root `run_root_fragment_in_process:845` profiler=None `:935`; non-root `execute_plan_fragment_sync`→`execute_fragment`), `execute_fragment` (`lower/fragment.rs:170`, creates per-fragment profiler, **discards it** — `FragmentOutput{profile_json:None}:617`), `collect_actuals_by_plan_node_id` (`runtime/profile_correlate.rs:29`, walks ONE profiler), `merge_pipeline_profiles_for_fe` (`fe_report.rs:515`).

**Run unit tests:** `cargo test --lib`. **SQL suites:** `cargo run --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests -- --suite <s> --mode verify`.

---

## Phase ordering

```
Phase A  IR covers execution-only paths   (direct-exec/AggStateMerge + branch-union + mv_refresh_ctx + iceberg sink), equiv-gated
Phase B  Execution cutover                (flip engine execute + insert call sites to the IR; full SQL suites gate)
Phase C  Delete legacy visitor            (remove visit_*/build*/short-circuits; IR is the only builder; repurpose equiv)
Phase D  Multi-fragment EXPLAIN ANALYZE   (gather per-fragment profiles through the coordinator; drop forced-single-fragment)
```

A→B→C are the single-source line (each gated). D is the orthogonal ANALYZE-coverage fix (cleanest after cutover, since execution then = IR). Every task is independently committable.

---

## Phase A — IR builder covers the execution-only paths (cutover prerequisite)

The IR builder must do everything legacy `build_with_mv_refresh_ctx`/`build_with_iceberg_sink` do for execution: thread `mv_refresh_ctx`, produce direct-exec fragments (AggStateMerge / branch-union), and apply the iceberg sink. Gated by extending `equiv.rs` (byte-identical vs legacy).

### Task A1: Thread `mv_refresh_ctx` through the IR lower path

**Files:** `ir/lowering.rs`, `fragment_builder.rs`

- [ ] `mv_refresh_ctx` is a **lower-time** concern (scan refresh via `refresh_scan_table_for_codegen`, exec_params via `build_exec_params_multi_with_refresh_context`). `build_distributed_plan` stays pure structure. Add `mv_refresh_ctx: Option<&IcebergMvRefreshContext>` to `lower_distributed_plan` + `OwnedLoweringState::new` (it already accepts an `Option` slot per M1 — wire it), and pass it to the exec-params build + scan refresh (already on `LoweringStateAccess`).
- [ ] Add `build_via_distributed_plan_with_mv_refresh_ctx(plan, catalog, connectors, db, mv_refresh_ctx)` (mirrors `build_via_distributed_plan` but threads the ctx into `lower_distributed_plan`). Keep `build_via_distributed_plan` as `…_with_mv_refresh_ctx(…, None)`.
- [ ] Run `cargo test --lib sql::codegen`. Expected: PASS (no behavior change; ctx `None` everywhere yet).
- [ ] Commit: `codegen/ir: thread mv_refresh_ctx through lower_distributed_plan`.

### Task A2: Direct-exec (AggregateStateMerge + branch-union UnionAll) in the IR entry

**Files:** `fragment_builder.rs` (the `build_via_distributed_plan_with_mv_refresh_ctx` entry)

Direct-exec bypasses the normal plan tree (as it does in legacy): the entry recognizes the patterns and emits `FragmentBuildResult.direct_exec` with child `PlanBuildResult`s built via the IR recursively. `lower_distributed_plan`'s `direct_exec: None` for normal fragments is unchanged; `lower_plan_build_result` (engine) already consumes `direct_exec` downstream.

- [ ] In `build_via_distributed_plan_with_mv_refresh_ctx`, before calling `build_distributed_plan`, mirror legacy `build_with_mv_refresh_ctx:647-665`:
  - `if let Some(b) = try_build_branch_union_aggregate_direct_via_ir(plan, …, mv_refresh_ctx)? { return Ok(b); }`
  - `if matches!(plan.op, Operator::PhysicalAggregateStateMerge(_)) { return build_aggregate_state_merge_direct_via_ir(plan, …, mv_refresh_ctx); }`
- [ ] Port `try_build_branch_union_aggregate_direct` (`:920`) and `build_aggregate_state_merge_direct[_with_layout]` (`:760`/`:796`) to IR variants: identical pattern-matching + layout validation, but build the child inputs (`old_input`, `delta_state_input`, branch children) via `single_fragment_child_plan(build_via_distributed_plan_with_mv_refresh_ctx(child, …))` instead of the legacy recursion. The produced `DirectExecPlan::{AggregateStateMerge, AggregateStatePhysicalize, UnionAll}` + `FragmentBuildResult` assembly are identical (reuse `DirectExecPlan`, `build_aggregate_state_merge_exec_node`, the layout/pruning/locator fields).
- [ ] `build_distributed_plan`'s catch-all on `PhysicalAggregateStateMerge` (`:564`) stays — it is now only reachable for AggStateMerge placements legacy also rejected (non-root, non-branch-union), preserving behavior.
- [ ] Run `cargo test --lib sql::codegen`. Expected: PASS.
- [ ] Commit: `codegen/ir: direct-exec (aggregate-state-merge + branch-union) via the IR entry`.

### Task A3: Iceberg write sink via the IR

**Files:** `fragment_builder.rs`

- [ ] Add `build_via_distributed_plan_with_iceberg_sink(plan, catalog, connectors, db, mv_refresh_ctx, sink_spec)` mirroring `build_with_iceberg_sink:514` **verbatim except** the inner build: call `build_via_distributed_plan_with_mv_refresh_ctx` instead of `build_with_mv_refresh_ctx`, then the identical post-processing (find root fragment, `root_output_tuple_id_for_sink`, `iceberg_sink_output_exprs_for_tuple`, set `output_sink = sink_spec.build_sink(tuple)`, `output_exprs`, `DescriptorTableBuilder::from_existing` + `add_iceberg_target_table`, patch all fragments' `desc_tbl`).
- [ ] Run `cargo test --lib sql::codegen`. Expected: PASS.
- [ ] Commit: `codegen/ir: iceberg write sink via the IR entry`.

### Task A4: Extend the equivalence harness to direct-exec / mv / sink

**Files:** `ir/equiv.rs`

- [ ] Add cases asserting `build_with_mv_refresh_ctx(plan, …, ctx)` **==** `build_via_distributed_plan_with_mv_refresh_ctx(plan, …, ctx)` for: an MV-refresh AggregateStateMerge plan, a branch-union plan, and a plan with `mv_refresh_ctx = Some` (reuse an `iceberg-ivm` test fixture); and `build_with_iceberg_sink(plan, …, sink)` == `build_via_distributed_plan_with_iceberg_sink(plan, …, sink)` for an INSERT-into-iceberg plan (assert root `output_sink` + `output_exprs` + `desc_tbl` byte-equal, incl. `direct_exec` structural equality for the direct-exec cases — extend `assert_fragment_equivalent` to compare `direct_exec` instead of only asserting both `None`).
- [ ] Run `cargo test --lib sql::codegen::ir::equiv`. Expected: PASS. Any diff is a real gap in A1–A3; fix before Phase B.
- [ ] Commit: `codegen/ir: equiv coverage for direct-exec / mv / iceberg sink`.

**Phase A exit:** the IR entry (`build_via_distributed_plan*`) is byte-equivalent to every legacy `build*` over all execution shapes (normal multi-fragment, direct-exec, mv-refresh, iceberg sink). Execution can now switch to it.

---

## Phase B — Execution cutover

Flip the engine call sites from legacy `build*` to the IR. Gate on the full SQL suites (the end-to-end behavior check) — the equiv harness already proves per-plan byte equality.

### Task B1: Cut the execute path over to the IR

**Files:** `src/engine/mod.rs`

- [ ] At the execute call site (`:3731`), replace `PlanFragmentBuilder::build_with_mv_refresh_ctx(&physical, codegen_catalog, connectors, current_database, mv_refresh_ctx)` with `PlanFragmentBuilder::build_via_distributed_plan_with_mv_refresh_ctx(&physical, codegen_catalog, connectors, current_database, mv_refresh_ctx)`. `choose_standalone_execution` + `execute_plan`/`ExecutionCoordinator` consume the `MultiFragmentBuildResult` unchanged.
- [ ] Run `cargo test --lib`. Expected: PASS.
- [ ] **SQL-suite gate:** run `filter sort join cte aggregate ssb tpc-h tpc-ds` in `--mode verify`; and the iceberg suites with the docker env (CLAUDE.md §7.3). Expected: all green. Any failure = a residual gap; fix in the owning Phase-A task and re-run.
- [ ] Commit: `engine: cut standalone execute path over to the DistributedPlan IR`.

### Task B2: Cut the iceberg-insert path over to the IR

**Files:** `src/engine/mod.rs`

- [ ] At the insert call site (`:3543`), replace `build_with_iceberg_sink(…)` with `build_via_distributed_plan_with_iceberg_sink(…)`.
- [ ] Switch the test helper at `:5976` (and any remaining `build`/`build_with_*` callers found via `git grep -n 'PlanFragmentBuilder::build' src/engine`) to the IR entry.
- [ ] **Gate:** the iceberg suites (`iceberg`, `iceberg-rest`, `iceberg-ivm`) in `--mode verify` (docker env). Expected: green.
- [ ] Commit: `engine: cut iceberg-insert path over to the DistributedPlan IR`.

**Phase B exit:** execution derives entirely from the IR (spec §2 "执行也从 IR 派生" met). Legacy `build*` is dead code (only the equiv harness + tests still call it).

---

## Phase C — Delete the legacy visitor

Now the legacy visitor is unreferenced by production code. Remove it; keep the shared cores the IR path uses.

### Task C1: Repurpose the equivalence harness to IR-only

**Files:** `ir/equiv.rs`

- [ ] The harness compares legacy `build` vs IR `build_via_distributed_plan`. After deleting legacy `build`, that comparison can't exist. Convert each `assert_distributed_plan_equivalent` case into an **IR-only assertion**: `build_via_distributed_plan*` succeeds + the lowered `MultiFragmentBuildResult` matches a recorded golden (or asserts structural invariants: fragment count, node_ids monotonic, edges well-formed). Keep the same query corpus (it's valuable coverage). Do this **before** C2 so the corpus isn't lost.
- [ ] Run `cargo test --lib sql::codegen::ir::equiv`. Expected: PASS (IR-only).
- [ ] Commit: `codegen/ir: convert equiv harness to IR-only golden assertions`.

### Task C2: Delete the legacy `visit_*` visitor + build entries

**Files:** `src/sql/codegen/fragment_builder.rs`, callers

- [ ] Delete: `PlanFragmentBuilder::build`, `build_with_mv_refresh_ctx`, `build_with_iceberg_sink`, `try_build_branch_union_aggregate_direct`, `build_aggregate_state_merge_direct[_with_layout]`, all `visit_*` methods, `VisitResult`, and the now-unused `PlanFragmentBuilder` fragment-stack/accumulator fields. **Keep** the extracted `LoweringCtx` cores (`lower_scan`/`lower_hash_aggregate`/`lower_sort`/… — used by the IR `lower_distributed_plan`), `nodes.rs` helpers, `slot_ref_exprs_for_columns`, and the `build_via_distributed_plan*` entries (rename to drop the `_via_distributed_plan` infix if desired — e.g. `PlanFragmentBuilder::build` becomes the IR entry; update call sites + equiv).
- [ ] Update the ~40 inline `fragment_builder.rs` tests + the `:5976` helper that called legacy `build*` to call the IR entry (or delete tests that only tested the legacy visitor's internals, now covered by the IR equiv/golden).
- [ ] Run `cargo build --lib` (no dead-code warnings for IR entries — drop their `#[allow(dead_code)]`), `cargo test --lib`, `cargo clippy --lib` (clean).
- [ ] **Gate:** re-run the full SQL suites once more. Expected: green.
- [ ] Commit: `codegen: delete legacy fragment-builder visitor; DistributedPlan IR is the only builder`.

**Phase C exit:** one builder. `PhysicalPlanNode → DistributedPlan → thrift` is the single lowering path for both EXPLAIN and execution. Spec §2 "单一来源" fully met; ~9k lines of legacy visitor removed.

---

## Phase D — Multi-fragment EXPLAIN ANALYZE

Drop the forced-single-fragment in ANALYZE; gather per-fragment runtime profiles through the coordinator and correlate across fragments. (Independent of A–C, but cleanest now that execution = IR.)

### Task D1: Make `execute_fragment` return its profiler

**Files:** `src/lower/fragment.rs`

- [ ] `FragmentOutput` carries `profile_json: None` and discards the profiler (`:617`). Add `pub profiler: Option<Profiler>` to `FragmentOutput` and return the (populated, Arc-backed) `profiler` the fn already created/used. No behavior change when `enable_profile` is false (profiler stays `None`).
- [ ] Run `cargo test --lib`. Expected: PASS.
- [ ] Commit: `lower: return the per-fragment profiler from execute_fragment`.

### Task D2: Gather per-fragment profilers in the standalone dispatcher + coordinator

**Files:** `src/runtime/dispatcher.rs`, `src/runtime/coordinator.rs`

- [ ] **InProcessDispatcher**: capture the profiler each fragment produces. Root path `run_root_fragment_in_process` (`:845`) currently passes `profiler: None` to `execute_plan_with_pipeline` (`:935`) — create a profiler named `execute_fragment (plan_node_id=<root>)` and stash it; non-root path (`execute_plan_fragment_sync`→`execute_fragment`) now returns its profiler (D1) — capture it. Collect them keyed by fragment instance into the shared `InProcessState` (mirror how chunks/reports are already gathered; the non-root path already does `register_in_process_report_instance`).
- [ ] **ExecutionCoordinator**: add a way to surface the gathered per-fragment profilers after `execute_with_write_outcome` (e.g. return `Vec<Profiler>` alongside the result, or expose via a method). Only populated when profiling is requested.
- [ ] (Investigate-then-implement) If the existing in-process report path (`register_in_process_report_instance` + `fe_report`) already serializes per-fragment profiles, prefer tapping that gather over hand-threading; D2's sub-step 1 is to map that path and choose the lighter mechanism.
- [ ] Run `cargo test --lib`. Expected: PASS (gather is inert unless profiling on).
- [ ] Commit: `runtime: gather per-fragment profilers through the in-process coordinator`.

### Task D3: Multi-fragment correlation + drop forced-single-fragment in ANALYZE

**Files:** `src/runtime/profile_correlate.rs`, `src/engine/mod.rs`

- [ ] Add `collect_actuals_by_plan_node_id_multi(profilers: &[Profiler]) -> HashMap<i32, ActualMetrics>` that walks each fragment profiler (reuse `collect_rec` per profiler, merging by `plan_node_id` with the existing `.max()` accumulation). `node_id` is globally unique across fragments (M0 allocator), so cross-fragment keys don't collide.
- [ ] Rewrite `explain_analyze_query` (`:3224`): **remove** the `collapse_distribution_enforcers_for_single_fragment` (`:3262`) and the Coordinated-rejection (`:3268`). Build the IR once (`build_distributed_plan` — keeps the true multi-fragment shape, so ANALYZE now matches VERBOSE), lower, and dispatch via `choose_standalone_execution`: SingleFragment → `execute_plan(..., Some(profiler))` (as today); Coordinated → run the `ExecutionCoordinator` with profiling enabled and gather per-fragment profilers (D2). Feed all profilers to `collect_actuals_by_plan_node_id_multi`, then `explain_distributed_plan_analyze(&dp, Analyze, &actuals)` — which now renders the multi-fragment structure (PLAN FRAGMENT N) **with** per-node actuals.
- [ ] Run `cargo test --lib engine`. Expected: PASS.
- [ ] Commit: `engine: multi-fragment EXPLAIN ANALYZE (gather per-fragment actuals; drop forced single-fragment)`.

### Task D4: Tests + golden

- [ ] **Live smoke** (CLAUDE.md §7.3): `EXPLAIN ANALYZE` a shuffle-aggregate and a partitioned-join query; confirm multiple `PLAN FRAGMENT N` blocks each with per-node `act={rows=…}`, and that leaf-scan actuals match `SELECT count(*)`.
- [ ] **sql-test**: extend `explain_analyze_actuals.sql` (or add a multi-fragment case) with `-- @normalize_explain_timing=true` asserting ≥2 `PLAN FRAGMENT` blocks + `act={rows=` on nodes in more than one fragment. Record with `--mode record --record-from target`.
- [ ] Run the optimizer suite `--mode verify`. Expected: green.
- [ ] Commit: `tests: multi-fragment EXPLAIN ANALYZE smoke + sql-test`.

**Phase D exit:** `EXPLAIN ANALYZE` shows the real multi-fragment structure with per-operator actual-vs-estimate across fragments (matches VERBOSE's shape); the forced-single-fragment workaround is gone.

---

## Risks & notes

- **Phase B is execution-critical.** The equiv harness (extended in A4) proves per-plan byte equality before the flip; the full SQL suites are the end-to-end gate. Do **not** delete legacy (Phase C) until B's suites are green — keep the fallback until then.
- **Direct-exec via IR** must reproduce the legacy short-circuits exactly (layout validation, pruning limits, target-position locator, branch_id). A4's equiv cases (asserting `direct_exec` structural equality) are the gate. This is the subtlest part of Phase A.
- **Phase D's profiler gather is the novel work.** Standalone multi-fragment runs each fragment in-process via the dispatcher; the profilers exist but are discarded. D1/D2 capture + gather them. Prefer reusing the existing in-process report path if it already gathers profiles (D2 step 1).
- **`node_id` global uniqueness across fragments** (M0 allocator) is what makes cross-fragment correlation a simple keyed merge — confirm no fragment re-bases node ids.
- **Same-`node_id` multiple operators** (join build+probe): the existing `.max(PullRowNum)` heuristic carries over; verify on the partitioned-join smoke.
- **Golden churn:** ANALYZE multi-fragment changes the ANALYZE output shape (now multi-fragment) — re-record with `@normalize_explain_timing`.
- **No backwards-compat shim** (project memory): delete legacy outright in Phase C.
- **Scope:** this plan does not change the optimizer, thrift wire format, or the FE-compatible path (spec non-goals upheld).

## Self-review (spec coverage)
- M0-S7 direct-exec/mv passthrough in IR — Phase A (A1 mv, A2 direct-exec, A3 sink). ✓
- M0-S6 execution cutover — Phase B. ✓
- M0-S8 delete legacy visitor — Phase C. ✓
- Spec §2 "执行也从 IR 派生 / 单一来源" — met after Phase C. ✓
- §F multi-fragment ANALYZE — Phase D. ✓
- Gates: equiv (byte) at A4, full SQL suites at B/C, live+sql-test at D. ✓
