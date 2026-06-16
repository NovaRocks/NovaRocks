# DistributedPlan IR — M2: EXPLAIN ANALYZE with real per-operator profiles

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Turn `EXPLAIN ANALYZE` from a "fake analyze" (estimates + a query-level timing header) into a real one: **single build** of the DistributedPlan IR, **profiled execution** of that exact plan, and **per-operator actual-vs-estimate** rendered next to each node, correlated by `node_id`.

**Architecture:** Spec `docs/superpowers/specs/2026-06-15-plannode-ir-explain-observability-design.md` §F. Builds on M0 (IR + lowering, #322) and M1 (EXPLAIN renders from the IR, #328). The pipeline executor **already** accepts a `profiler: Option<Profiler>` (`exec/pipeline/executor.rs:55`) and `Profiler` is `Arc`-backed (a caller-held clone is populated after execution). The standalone `execute_plan` (`engine/mod.rs:4174`) hardcodes `profiler: None` — that is the gap. M2: thread a profiler through, run ANALYZE single-fragment so *displayed == executed == profiled* (node_ids line up), merge the profile (`merge_pipeline_profiles_for_fe`), correlate by `plan_node_id` (== IR `node_id`, by M0 equivalence), render actuals.

**Scope / non-goals:**
- **Single-fragment ANALYZE only.** ANALYZE forces single-fragment execution (reuse `collapse_distribution_enforcers_for_single_fragment`), so the whole query runs via `execute_plan` (one profilable fragment) and the displayed plan equals the executed/profiled one. Row counts are fragmentation-independent, so per-operator actuals are accurate for calibration. **Multi-fragment (coordinator) ANALYZE profiling is deferred** — `ExecutionCoordinator` carries no profiler today; surfacing fragment-structured actuals across coordinated fragments is a follow-up. (Consequence: ANALYZE shows the collapsed single-fragment shape, which can differ from VERBOSE's multi-fragment shape — acceptable for M2; flagged below.)
- Reuse existing counters only (PullRowNum / OperatorTotalTime / OperatorPeakMemoryUsage); no new counter schema.
- No execution cutover for normal queries (still the legacy `build`); ANALYZE independently builds + lowers the IR.

**Tech Stack:** Rust, `cargo test`, `sql-tests` runner, a live standalone server for the ANALYZE smoke (per CLAUDE.md §7.3).

**Branch:** `claude/dist-plan-ir-m2-analyze` (off `origin/main` = #328).

**Verbatim anchors (current, this branch):**
- `src/engine/mod.rs`: `explain_analyze_query:3223` (double-plan, profiler=None, renders `build_distributed_plan`+`explain_distributed_plan(Analyze)`), `execute_plan:4174` (hardcodes `profiler: None` at the `execute_plan_with_pipeline` call), `collapse_distribution_enforcers_for_single_fragment` (~:3624), `execute_query_with_options_and_imv_validator_with_catalog_provider`, `choose_standalone_execution:2918`, `lower_plan_build_result`.
- `src/exec/pipeline/executor.rs:49` `execute_plan_with_pipeline(profiler: Option<Profiler>, …)`; `fragment_context.rs:62` stores it; `driver.rs:206` `OperatorCounters` populates CommonMetrics when a profiler is present.
- `src/runtime/profile.rs`: `RuntimeProfile`/`Profiler` (Arc-backed, `Clone`), `OperatorProfiles{operator,common,unique}`, `merge_isomorphic_profiles:309`, `children()`, `counter_snapshot(name)`.
- `src/service/fe_report.rs`: `merge_pipeline_profiles_for_fe:515` (collapse per-driver DOP → one profile per operator), `normalize_profile_tree_for_fe:552` (names → `(plan_node_id=N)`).
- `src/sql/codegen/ir/explain.rs`: `explain_distributed_plan:26`, `format_distributed_node:100`, `node_prefix:200` (`"{node_id}:"`), `stats_suffix:693` (estimate trailer — the actual-column injection point), `is_detailed`.
- IR `DistributedPlanNode.node_id` == thrift `TPlanNode.node_id` == operator profile `plan_node_id` (M0 equivalence; operator names embed it via `RESULT_SINK (plan_node_id=…)`, scan `node_id()`, join dep-key, agg param).

**Run unit tests:** `cargo test --lib sql::codegen::ir::explain` + `cargo test --lib engine`.
**Live ANALYZE smoke:** start standalone-server (CLAUDE.md §7.3) and run `EXPLAIN ANALYZE SELECT …`.

---

## Phase ordering

```
Phase 1  Profiler through execute + single-build ANALYZE   (execute_plan takes profiler; explain_analyze builds IR once, single-fragment, profiled)
Phase 2  Merge + node_id correlation                       (merge_pipeline_profiles_for_fe → HashMap<plan_node_id, ActualMetrics>)
Phase 3  Render actual-vs-estimate                         (explain_distributed_plan(Analyze) appends act={rows,time,peak} per node)
Phase 4  Tests + golden                                    (unit + live smoke + sql-test @normalize_explain_timing)
```

Each task is independently committable. Phases 1–2 are observable only via Phase 3's rendering + tests.

---

## Phase 1 — Profiler through execute; single-build ANALYZE

### Task 1.1: Thread `profiler: Option<Profiler>` through `execute_plan`

**Files:** `src/engine/mod.rs`

- [ ] **Add a `profiler` param to `execute_plan`** (`:4174`) and pass it to `execute_plan_with_pipeline` (which already accepts it — currently `None` is hardcoded at that call). Signature:
```rust
fn execute_plan(
    result: PlanBuildResult,
    query_opts: Option<crate::internal_service::TQueryOptions>,
    terminal_sink: Option<Box<dyn crate::exec::pipeline::operator_factory::OperatorFactory>>,
    iceberg_catalogs: Option<&crate::connector::iceberg::catalog::IcebergCatalogRegistry>,
    profiler: Option<crate::runtime::profile::Profiler>,   // NEW
) -> Result<QueryResult, String> {
    // … unchanged until the executor call …
    execute_plan_with_pipeline(
        exec_plan, false, std::time::Duration::from_millis(10), sink,
        None,            // exchange_finst_id
        profiler,        // was: None
        pipeline_dop as _,
        std::sync::Arc::new(RuntimeState::new(query_opts, None, None, None, None, None, None, None, None)),
        None, None, None,
    )?;
    // … unchanged …
}
```
- [ ] Update all existing `execute_plan(` call sites to pass `None` (no behavior change for non-ANALYZE callers). `grep -n 'execute_plan(' src/engine/mod.rs`.
- [ ] Run `cargo test --lib engine`. Expected: PASS (no behavior change; profiler `None` everywhere yet).
- [ ] Commit: `engine: thread Option<Profiler> through execute_plan`.

### Task 1.2: Single-build, single-fragment, profiled `explain_analyze_query`

**Files:** `src/engine/mod.rs`

Rewrite `explain_analyze_query` to build the IR once, force single-fragment, lower, execute that exact plan with a profiler, and render from the same IR. This removes the double-plan and makes displayed == executed == profiled.

- [ ] **Step 1 (test first):** add an engine unit test `explain_analyze_populates_per_operator_profile` that runs `EXPLAIN ANALYZE SELECT <a join/agg query>` against the in-memory test catalog (reuse the standalone test setup near `engine/mod.rs:5880`) and asserts the output contains an `act_rows=` token on at least one node line (will fail until Phase 3, but pins the end-to-end wiring). Run it → FAIL.
- [ ] **Step 2:** rewrite the body:
```rust
fn explain_analyze_query(/* …same args… */) -> Result<QueryResult, String> {
    use crate::sql::codegen::ir::{build_distributed_plan, lower_distributed_plan, explain_distributed_plan_analyze};
    use crate::sql::explain::ExplainLevel;
    use crate::runtime::profile::Profiler;

    // Plan once.
    let t_plan = Instant::now();
    let (resolved, cte_registry, mut factory) =
        crate::sql::analyzer::analyze(query, analyzer_catalog, current_database)?;
    let logical = crate::sql::planner::plan_query(resolved, cte_registry, &mut factory)?;
    let mut table_stats = build_table_stats_from_plan(&logical);
    let mv_candidates = match mv_rewrite_state { /* …unchanged… */ };
    let mut physical =
        crate::sql::optimizer::optimize(logical, &table_stats, factory, None, mv_candidates)?;
    // ANALYZE runs single-fragment so the displayed plan == the profiled plan
    // (one fragment, node_ids stable). Row counts are fragmentation-independent.
    physical = collapse_distribution_enforcers_for_single_fragment(physical);
    let dp = build_distributed_plan(&physical)?;
    let planning_ms = t_plan.elapsed().as_millis() as u64;

    // Lower the SAME IR and execute it with a profiler.
    let t_exec = Instant::now();
    let build_result = lower_distributed_plan(&dp, codegen_catalog, connectors)?;
    let single = match choose_standalone_execution(build_result) {
        StandaloneExecutionPlan::SingleFragment(plan) => *plan,
        StandaloneExecutionPlan::Coordinated(_) => {
            // collapse should have guaranteed single-fragment; fail fast if not.
            return Err("EXPLAIN ANALYZE expects a single-fragment plan after collapse".into());
        }
    };
    let profiler = Profiler::new("explain_analyze");
    let executed = execute_plan(single, query_opts, None, None, Some(profiler.clone()))?;
    let rows: u64 = executed.chunks.iter().map(|c| c.len() as u64).sum();
    let execution_ms = t_exec.elapsed().as_millis() as u64;

    // Correlate (Phase 2) + render (Phase 3).
    let actuals = crate::runtime::profile_correlate::collect_actuals_by_plan_node_id(&profiler);
    let mut lines = vec![format!(
        "Planning: {planning_ms} ms / Execution: {execution_ms} ms / Rows: {rows}"
    )];
    lines.extend(explain_distributed_plan_analyze(&dp, ExplainLevel::Analyze, &actuals));
    build_string_query_result("Explain String", lines)
}
```
(`collect_actuals_by_plan_node_id` is built in Phase 2; `explain_distributed_plan_analyze` in Phase 3. Until then, stub `actuals` as empty + call the existing `explain_distributed_plan` so this compiles after Task 1.2; wire the real calls in Phases 2–3.)
- [ ] Run `cargo test --lib engine` (the new test still FAILs on the `act_rows=` assertion until Phase 3; the rest compiles + the double-plan is gone). Verify no other engine test regressed.
- [ ] Commit: `engine: single-build single-fragment profiled EXPLAIN ANALYZE`.

---

## Phase 2 — Merge + node_id correlation

### Task 2.1: `collect_actuals_by_plan_node_id`

**Files:** create `src/runtime/profile_correlate.rs`; `src/runtime/mod.rs` (module decl)

- [ ] **Define `ActualMetrics`** and the collector that merges per-driver instances and keys by `plan_node_id`:
```rust
// src/runtime/profile_correlate.rs
use std::collections::HashMap;
use crate::runtime::profile::Profiler;

#[derive(Clone, Copy, Debug, Default)]
pub struct ActualMetrics {
    pub output_rows: i64,        // CommonMetrics.PullRowNum (the node's actual output rows)
    pub total_time_ns: i64,      // CommonMetrics.OperatorTotalTime
    pub peak_mem_bytes: i64,     // CommonMetrics.OperatorPeakMemoryUsage
}

/// Merge per-driver DOP instances (reuse the FE collapse), then walk the tree
/// and pull CommonMetrics counters per `plan_node_id` parsed from operator names.
pub fn collect_actuals_by_plan_node_id(profiler: &Profiler) -> HashMap<i32, ActualMetrics> {
    let merged = crate::service::fe_report::merge_pipeline_profiles_for_fe(profiler); // make pub(crate)
    let mut out: HashMap<i32, ActualMetrics> = HashMap::new();
    collect_rec(&merged, &mut out);
    out
}

fn collect_rec(node: &Profiler, out: &mut HashMap<i32, ActualMetrics>) {
    if let Some(id) = parse_plan_node_id(&node.name()) {
        if let Some(common) = node.get_child("CommonMetrics") {
            let m = out.entry(id).or_default();
            // Output rows: prefer the operator that produces rows. When several
            // operator profiles share a node_id (e.g. join build sink + probe processor),
            // take the MAX PullRowNum as the node's output (matches StarRocks' use of the
            // output operator's PullRowNum; refine per-role if needed).
            m.output_rows = m.output_rows.max(counter(&common, "PullRowNum"));
            m.total_time_ns = m.total_time_ns.max(counter(&common, "OperatorTotalTime"));
            m.peak_mem_bytes = m.peak_mem_bytes.max(counter(&common, "OperatorPeakMemoryUsage"));
        }
    }
    for child in node.children() {
        collect_rec(&child, out);
    }
}

fn counter(common: &Profiler, name: &str) -> i64 {
    common.counter_value(name).unwrap_or(0) // add a small `counter_value(&self, &str) -> Option<i64>` accessor to RuntimeProfile if absent
}

/// "OP (plan_node_id=5)" -> Some(5). Mirrors ExplainAnalyzer's regex.
fn parse_plan_node_id(name: &str) -> Option<i32> {
    let key = "plan_node_id=";
    let start = name.find(key)? + key.len();
    let rest = &name[start..];
    let end = rest.find(|c: char| !c.is_ascii_digit() && c != '-').unwrap_or(rest.len());
    rest[..end].parse().ok()
}
```
- [ ] Make `merge_pipeline_profiles_for_fe` `pub(crate)` (it is private today). Add a `counter_value(&self, name: &str) -> Option<i64>` accessor on `RuntimeProfile` if not present (read `counter_snapshot(name).map(|s| s.value)`).
- [ ] **Unit test** (`profile_correlate.rs`): build a small `Profiler` by hand with `Pipeline (id=)`→`PipelineDriver (id=)`→`SCAN (plan_node_id=2)`→`CommonMetrics{PullRowNum=10, OperatorTotalTime=5, OperatorPeakMemoryUsage=64}`, run `collect_actuals_by_plan_node_id`, assert `actuals[&2] == {10,5,64}`. Also test two driver instances merge (DOP=2 → counters summed by `merge_isomorphic_profiles`).
- [ ] Run `cargo test --lib runtime::profile_correlate`. Expected: PASS.
- [ ] Commit: `runtime: collect actual per-operator metrics by plan_node_id`.

---

## Phase 3 — Render actual-vs-estimate

### Task 3.1: `explain_distributed_plan_analyze` injects per-node actuals

**Files:** `src/sql/codegen/ir/explain.rs`, `src/sql/codegen/ir/mod.rs`

- [ ] **Add an Analyze-with-actuals entry** that threads the correlation map to per-node rendering. Rather than duplicate the renderer, parameterize it:
```rust
// ir/explain.rs
pub(crate) fn explain_distributed_plan_analyze(
    dp: &DistributedPlan,
    level: ExplainLevel,
    actuals: &std::collections::HashMap<i32, crate::runtime::profile_correlate::ActualMetrics>,
) -> Vec<String> {
    explain_distributed_plan_inner(dp, level, Some(actuals))
}
// Existing `explain_distributed_plan(dp, level)` becomes `explain_distributed_plan_inner(dp, level, None)`.
```
- [ ] **Inject the actual column** where `stats_suffix` is appended (`:693`). When `actuals` is `Some` and `actuals.get(&node.node_id)` is `Some(m)`, append after the estimate trailer:
```rust
// in the per-node line builder, alongside stats_suffix:
let actual_suffix = match actuals.and_then(|a| a.get(&node.node_id)) {
    Some(m) => format!(" act={{rows={} time={} peak={}}}",
        m.output_rows, fmt_time_ns(m.total_time_ns), fmt_bytes(m.peak_mem_bytes)),
    None => String::new(),   // node_id not in profile (e.g. folded Filter / collapsed) → no actual
};
```
Thread `actuals: Option<&HashMap<i32, ActualMetrics>>` through `format_distributed_node` and the per-kind formatters (it's a read-only extra arg). Add `fmt_time_ns` (ns → `"2.3ms"`/`"450us"`) and `fmt_bytes` (`"4.0MB"`) helpers.
- [ ] **Estimate vs actual side-by-side:** the existing `stats={rows=<est>}` trailer stays; `act={rows=<actual> …}` is appended, so each node reads e.g. `3:HASH JOIN (PARTITIONED, INNER, eq:[a=b]) stats={rows=124} act={rows=131 time=2.3ms peak=4.0MB}`.
- [ ] **Folded / no-profile nodes:** nodes whose `node_id` is absent from `actuals` (folded Filter, or any node that lowered to no thrift node) get no `act=` — do **not** print `act=n/a` (which implies a missing measurement). Confirm which IR kinds can lack a profile (Filter when folded) and leave them estimate-only.
- [ ] **Unit test** (`ir/explain.rs`): build a `DistributedPlan` (via `build_distributed_plan` on a hand-built scan/agg physical plan) + a hand-built `actuals` map keyed by the plan's node_ids; assert the Analyze output has `act={rows=…}` on the scan/agg lines and none on a node_id absent from the map.
- [ ] Run `cargo test --lib sql::codegen::ir::explain`. Expected: PASS.
- [ ] Commit: `codegen/ir: render per-node actual-vs-estimate in EXPLAIN ANALYZE`.

### Task 3.2: Wire the real calls in `explain_analyze_query`

**Files:** `src/engine/mod.rs`

- [ ] Replace the Phase-1 stubs: call `collect_actuals_by_plan_node_id(&profiler)` and `explain_distributed_plan_analyze(&dp, Analyze, &actuals)` (added in Phases 2–3). The Task-1.2 `explain_analyze_populates_per_operator_profile` test now PASSES (a node line carries `act_rows=`/`act={rows=`).
- [ ] Run `cargo test --lib engine`. Expected: PASS incl. the new test.
- [ ] Commit: `engine: wire profile correlation + actual rendering into EXPLAIN ANALYZE`.

---

## Phase 4 — Tests + golden

### Task 4.1: Live ANALYZE smoke + sql-test

- [ ] **Live smoke** (manual, per CLAUDE.md §7.3): start standalone-server, run a few `EXPLAIN ANALYZE SELECT …` (a scan+filter, a group-by, a join) and eyeball that per-node `act={rows=…}` appears and the actual rows are sane (match `SELECT count(*)` for the leaf scan, etc.). Capture one as a sql-test.
- [ ] **sql-test** under `sql-tests/optimizer/`: add `explain_analyze_actuals.sql` with `-- @normalize_explain_timing=true` (timing is nondeterministic) asserting the structure (`Planning:`/`Execution:` header + at least one `act={rows=` line). Record with `--mode record --record-from target`.
- [ ] Run the optimizer suite in `--mode verify`. Expected: PASS.
- [ ] Commit: `tests: EXPLAIN ANALYZE actuals smoke + sql-test`.

---

## Risks & notes

- **Single-fragment ANALYZE is a deliberate scope choice.** Forcing single-fragment makes displayed == executed == profiled (node_ids align trivially) and gives accurate per-operator row actuals (row counts don't depend on fragmentation). The cost: ANALYZE shows the collapsed shape, which can differ from VERBOSE's multi-fragment shape. **Multi-fragment (coordinator) ANALYZE** — threading a profiler through `ExecutionCoordinator` and gathering per-fragment profiles — is a follow-up (the coordinator carries no profiler today).
- **Same-`node_id` multiple operators** (join build sink + probe processor both tagged with the join's id): the collector takes MAX PullRowNum as the node's output rows. This matches "the operator that produces the node's output"; if a case mis-attributes, refine by operator role (skip build-sink profiles). Verify against a join in the live smoke.
- **`merge_pipeline_profiles_for_fe` visibility:** make it `pub(crate)` (Task 2.1). It already collapses per-driver DOP instances; reusing it keeps M2's merge consistent with the FE path.
- **Profiler overhead:** ANALYZE always enables the profiler; normal queries are unaffected (they pass `None`). The driver only allocates counters when a profiler is present (`driver.rs:235`).
- **`RESULT_SINK (plan_node_id=-1)`** and other negative/`-1` ids are not real plan nodes — `parse_plan_node_id` returns them but no IR node has `node_id=-1`, so they're harmless (no IR node matches).
- **Timing is nondeterministic** — every ANALYZE golden uses `@normalize_explain_timing`; never assert raw `time=`/`Execution:` values.
- **EXPLAIN ANALYZE on MV REFRESH** stays unsupported (existing error); M2 is SELECT-only.

## Self-review (spec §F coverage)
- Single build + profiled execute — Phase 1 (1.1 profiler param, 1.2 single-build single-fragment). ✓ (removes the double-plan)
- Merge + node_id correlation — Phase 2 (reuse `merge_pipeline_profiles_for_fe`; key by `plan_node_id` == IR `node_id`). ✓
- Per-node actual-vs-estimate render — Phase 3 (`act={rows,time,peak}` beside `stats={rows}`). ✓
- Folded/no-profile nodes handled (no `act=n/a`). ✓
- Multi-fragment coordinator profiling — explicitly deferred (documented).
- Reuse existing counters only; no schema expansion (spec non-goal honored). ✓
