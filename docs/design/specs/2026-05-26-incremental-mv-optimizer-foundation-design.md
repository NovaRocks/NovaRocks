# Incremental MV Optimizer Foundation — Combined Design (PR-α + PR-β)

Date: 2026-05-26
Tasks:
- [`Incremental MV optimizer foundation`](../../../../../../../Documents/Obsidian/NovaRocks%20TODO/incremental-mv-optimizer-foundation.md) (TODO List task 2 — landed by PR-α of this spec)
- [`Logical Delta / Version marker operators`](../../../../../../../Documents/Obsidian/NovaRocks%20TODO/logical-delta-version-marker-operators.md) (TODO List task 3 — landed by PR-β of this spec)
Predecessor: [`Iceberg MV rewrite context`](2026-05-26-iceberg-mv-rewrite-context-design.md) (task 1, PR #185)

## 1. Goal

Provide the IMV-specific logical rewrite substrate on top of the existing
`src/sql/optimizer/rewrite/` framework so that future Iceberg v3 IMV work
(scan delta binding, action column propagation, aggregate state rewrite,
join delta, UNION ALL delta) can be expressed as registered rewrite rules
rather than as shape-specific hand-written refresh helpers. This spec covers
two sequenced PRs:

- **PR-α** lands the foundation: a new `src/sql/optimizer/rewrite/imv/`
  sub-module, an `ImvPlanAnnotation` extension placeholder, an IMV
  entrypoint `run_imv_rewrite`, and the wire-up into the three refresh
  construction sites established in task 1. The rule set is empty; the
  pipeline is provably identity.
- **PR-β** lands the marker operators: `ImvDelta` / `ImvVersion` logical
  nodes, the root-plan `Delta(root)` wrap rule, the Validation-phase
  convergence check, and mutation guards. PR-β can adjust the
  `ImvRewriteOutcome` shape if it picks the wrapped-plan representation
  (§7.2 R3) — this is anticipated and not a PR-α API breach.

PR-α must not change refresh semantics, must not regress the `iceberg-ivm`,
`iceberg`, or `iceberg-rest` SQL suites, and must not produce any
non-identity rewrite of the canonical select plan. PR-β similarly must not
change refresh semantics: the markers it introduces are stripped by its own
Validation phase if they remain unresolved — they never leak to downstream
consumers in this spec.

## 2. Non-Goals

- No change to physical lowering, codegen, or runtime execution.
- No introduction of an `EXPLAIN IMV REFRESH` SQL extension. EXPLAIN
  integration is deferred to a later task — both PRs rely on `tracing`
  events for observability.
- No `tracing-capture` test harness (was deferred by task 1; remains
  deferred here).
- No per-MV refresh-policy-level rule disable. Session-level
  `disable_optimizer_rules` is the only knob.
- No FE-compatible plan lowering changes.
- No action column / branch identity / internal column tracking
  implementation. PR-α defines an empty `ImvPlanAnnotation` placeholder;
  the actual lineage representation lands in task 5
  (Action column propagation).
- No new optimizer SQL golden fixtures in PR-α. PR-β adds the first
  IMV-specific plan-shape golden once markers exist.

## 3. Decision: PR-α vs PR-β split

Tasks 2 and 3 are designed in this single combined spec but shipped as two
sequenced PRs, matching the recommendation in task 1's design doc §3:

- **PR-α** lands tasks 2's acceptance criteria. The IMV pipeline runs on
  every refresh attempt but executes zero rules, so `iceberg-ivm` 61/61
  must hold unchanged. This proves the foundation works on real refresh
  plans before any semantic rule lands.
- **PR-β** lands task 3's acceptance criteria. Marker nodes are introduced
  and the root-plan wrap + Validation-phase convergence check are
  registered. Marker rules in PR-β do not yet emit a fully-resolved
  delta plan — that's task 6+ — so PR-β's Validation phase intentionally
  rejects unresolved markers and `run_imv_rewrite` fails fast back to
  the refresh path's caller. In other words, PR-β's "happy path" is
  marker convergence by Validation; the user-visible acceptance for PR-β
  is "marker leak detected, refresh fails fast with deterministic error",
  not "refresh produces an incremental result through marker rules".

This split bounds the review surface of each PR while keeping the design
self-consistent.

## 4. Architecture

### 4.1 Module layout

```text
src/sql/optimizer/rewrite/
    ├── context.rs           (existing — reused; extension slot already supports any T: Any+Send+Sync)
    ├── phase.rs             (existing — reused; not extended)
    ├── pipeline.rs          (existing — reused)
    ├── rule.rs              (existing — reused)
    ├── result.rs            (existing — reused)
    ├── trace.rs             (existing — small additive helpers; see §6)
    ├── tree.rs              (existing — verified covers Scan/Project/Filter/Aggregate/Join/Union; see §4.5)
    ├── rules/               (existing — query rewrite rules; not modified)
    └── imv/                 (NEW)
        ├── mod.rs
        ├── entrypoint.rs     (PR-α — run_imv_rewrite; ImvRewriteOutcome)
        ├── annotation.rs     (PR-α — ImvPlanAnnotation placeholder)
        ├── pipeline.rs       (PR-α — build_imv_pipeline; PR-α empty rule set, PR-β adds marker rules)
        ├── marker.rs         (PR-β — ImvDeltaNode / ImvVersionNode definitions + the wrap rule + convergence check)
        ├── explain.rs        (PR-α — small summary helpers used by refresh-path log line)
        └── tests/
            ├── noop_pipeline.rs            (PR-α)
            ├── pipeline_disable_rule.rs    (PR-α)
            ├── pipeline_failure.rs         (PR-α)
            └── marker_unresolved.rs        (PR-β)
```

### 4.2 PR-α public API

```rust
// src/sql/optimizer/rewrite/imv/entrypoint.rs
pub(crate) struct ImvRewriteInput {
    pub plan: LogicalPlan,
    pub mv_ctx: Arc<IcebergMvRewriteContext>,
    pub disabled_rules: Vec<String>,
    pub deadline: Option<Instant>,
}

pub(crate) struct ImvRewriteOutcome {
    pub plan: LogicalPlan,             // PR-α: == input.plan; PR-β: may carry markers (rejected by Validation)
    pub trace: RewriteTrace,
    pub annotation: ImvPlanAnnotation,
}

pub(crate) fn run_imv_rewrite(input: ImvRewriteInput) -> Result<ImvRewriteOutcome, String>;

// src/sql/optimizer/rewrite/imv/annotation.rs
#[derive(Clone, Debug, Default)]
pub(crate) struct ImvPlanAnnotation {
    // Empty in PR-α. PR-β/task 5 add fields; the struct is intentionally
    // a placeholder so that downstream sites can already reference it.
}

/// Single value placed in RewriteContext::set_extension. Bundles the
/// IMV-specific rewrite context handle with the per-pipeline annotation
/// because the framework's extension slot is single-tenant (see §10 R-5).
#[derive(Clone, Debug)]
pub(crate) struct ImvExtension {
    pub mv_ctx: Arc<IcebergMvRewriteContext>,
    pub annotation: ImvPlanAnnotation,
}

// src/sql/optimizer/rewrite/imv/pipeline.rs
pub(crate) fn build_imv_pipeline() -> RewritePipeline;
```

Both `ImvRewriteOutcome` field layout and `build_imv_pipeline` rule list are
expected to grow in PR-β; PR-α intentionally fixes only the *names* and
*entry signature*, not the internal contents.

### 4.3 Stage naming

The IMV pipeline reuses the existing `RewritePhase` enum (`LogicalNormalize`,
`StructuralRewrite`, `SemanticRewrite`, `Validation`). To make trace events
unambiguous between IMV and query rewrite runs, IMV stages get explicit
human-readable names via `RewritePipeline::from_stages`:

```rust
RewritePipeline::from_stages(vec![
    RewriteStage::new("imv-logical-normalize", RewritePhase::LogicalNormalize, vec![]),
    RewriteStage::new("imv-delta-marker",      RewritePhase::StructuralRewrite, vec![]),
    RewriteStage::new("imv-marker-cleanup",    RewritePhase::SemanticRewrite,   vec![]),
    RewriteStage::new("imv-validation",        RewritePhase::Validation,        vec![]),
])
```

PR-α ships all four stages with empty rule lists. PR-β adds rules into
`imv-delta-marker` (root wrap) and `imv-validation` (convergence check).

We deliberately do NOT extend `RewritePhase` with IMV-specific variants:
phases describe semantic intent, stage names describe ownership. A new
IMV-specific Validation phase enum value would change the framework type
surface for no semantic gain.

### 4.4 What stays outside `imv/`

- `IcebergMvRewriteContext` continues to live in
  [`src/engine/mv/refresh_context.rs`](../../../src/engine/mv/refresh_context.rs).
  The IMV pipeline only borrows it via
  `Arc<IcebergMvRewriteContext>` placed in `RewriteContext::set_extension`.
- The refresh-path glue (re-plan canonical select query → call
  `run_imv_rewrite` → discard outcome in PR-α) lives in
  [`src/engine/mv/iceberg_refresh.rs`](../../../src/engine/mv/iceberg_refresh.rs),
  not in `imv/`. Reason: the glue knows about analyzer + planner +
  `StandaloneState`, and pulling those imports into `src/sql/optimizer/`
  would invert the existing dependency direction.

### 4.5 Tree traversal coverage check

`src/sql/optimizer/rewrite/tree.rs` already implements `rewrite_with_rule`
with top-down / bottom-up traversal. PR-α adds a small test (one
`#[test]` in `tree.rs`) that walks every `LogicalPlan` variant
(`Scan / Filter / Project / Aggregate / Join / Sort / Limit / Union /
Intersect / Except / Values / GenerateSeries / TableFunction / Window /
SubqueryAlias / Repeat / CTEAnchor / CTEProduce / CTEConsume`) through a
dummy "visit-all" rule and asserts the visit count equals the node count.
This is an acceptance-style sanity check, not a redesign of `tree.rs`.

## 5. Data Flow

Each refresh attempt, after `IcebergMvRefreshContext::new()` returns
successfully (task 1), the refresh path executes the following sequence:

```text
 1. canonicalize_iceberg_mv_select_query           (existing)
       -> canonical_select_query: sqlparser::ast::Query
 2. classify_incremental_mv_query                  (existing)
       -> IncrementalMvShape
 3. IcebergMvRefreshContext::new                   (task 1)
       -> ctx (with ctx.rewrite: Arc<IcebergMvRewriteContext>)
 4. plan_canonical_select_for_imv                  (NEW in PR-α)
       a. construct a RefreshAnalysisSession wrapping current catalog/db
       b. Analyzer::analyze_query(canonical_select_query) -> ResolvedQuery
       c. plan_query(resolved, cte_registry, &mut factory) -> LogicalPlan
       failure: fail-fast, RefreshError::user("imv plan failed: ...")
 5. run_imv_rewrite                                (NEW in PR-α)
       a. ctx_rw = RewriteContext::for_mv_refresh(disabled_rules)
       b. ctx_rw.set_extension::<ImvExtension>(ImvExtension {
              mv_ctx,
              annotation: ImvPlanAnnotation::default(),
          })
          // Single set_extension call. The framework's extension slot is
          // a single Option<Arc<dyn Any + Send + Sync>>; wrapping both
          // mv_ctx and annotation into one ImvExtension struct keeps the
          // framework unchanged. See §10 R-5.
       c. ctx_rw.set_deadline(deadline)            (if Some)
       d. pipeline = build_imv_pipeline()
       e. plan_out = pipeline.rewrite(plan_in, &mut ctx_rw)?
       f. ext_out = ctx_rw.extension::<ImvExtension>()
                       .expect("ImvExtension installed at step b");
       g. annotation_out = ext_out.annotation.clone()
       h. trace_out = ctx_rw.trace().clone()         // RewriteTrace is Clone+Default
       returns: ImvRewriteOutcome { plan: plan_out, trace: trace_out, annotation: annotation_out }
       failure: fail-fast, error transparently propagated as String
 6. tracing::info!(
       target = %ctx.rewrite.target,
       mv_id  = ctx.rewrite.mv_id,
       stages = ?outcome.trace.stage_names(),
       rules_changed  = outcome.trace.changed_rules_count(),
       rules_rejected = outcome.trace.rejected_rules_count(),
       rules_failed   = outcome.trace.failed_rules_count(),
       "imv rewrite completed",
     )
 7. PR-α: outcome dropped; original refresh path continues (hand-built
    ExecPlan). PR-β/task 4+: outcome.plan and outcome.annotation feed
    the downstream marker-consuming code.
```

### 5.1 Call sites

The three sites that task 1 established for `IcebergMvRefreshContext::new`
each gain a steps 4–7 block:

| Shape | Site | Notes |
|---|---|---|
| `IncrementalMvShape::ProjectionFilter(_)` | inside `refresh_iceberg_mv` after the ctx construction at `iceberg_refresh.rs` | The canonical site. |
| `IncrementalMvShape::Aggregate(_)` / `JoinAggregate(_)` | top of `refresh_iceberg_aggregate_mv` after ctx construction | Same sequence. |
| `IncrementalMvShape::JoinProjectionFilter(_)` | top of `refresh_iceberg_join_mv` after ctx construction, inside the post-pin region | Constraint inherited from task 1 §4.3. |

PR-α keeps the three call sites textually similar to make PR-β's wire-up
mechanical.

### 5.2 plan_canonical_select_for_imv responsibilities

This helper is the only piece of glue truly new in PR-α. Its sole job is
to take `ctx.rewrite.canonical_select_query` + the session-level catalog
context and produce a `LogicalPlan` ready for `run_imv_rewrite`.

```rust
fn plan_canonical_select_for_imv(
    state: &Arc<StandaloneState>,
    ctx: &IcebergMvRefreshContext,
) -> Result<LogicalPlan, RefreshError>;
```

- It does NOT mutate `state`.
- It uses `ctx.rewrite.current_catalog` and `ctx.rewrite.current_database`
  to scope catalog resolution exactly the way `StandaloneSession::execute_in_context`
  does for a regular query.
- Failure surfaces as `RefreshError::user("imv plan failed for {fqn}: {error}")`.
- It is the only place in PR-α that depends on
  `crate::sql::analyzer` and `crate::sql::planner` from `src/engine/mv/`.

### 5.3 Disabled-rule passthrough

The third argument of `run_imv_rewrite`'s input is the
`disabled_rules: Vec<String>` derived from the active session's
`disable_optimizer_rules` SET var (or `cbo_disabled_rules` alias). The
refresh path reads this from `state` / session and threads it through.
PR-α writes one test that verifies a disabled (registered) rule is
short-circuited; PR-β extends with "user can disable LogicalDeltaOp via SET".

## 6. Error Handling

| Source | Handling | Returned to refresh |
|---|---|---|
| Step 4 analyzer/planner failure | fail-fast, no pipeline entry | `RefreshError::user("imv plan failed for {fqn}: {original}")` |
| Step 5 rule `apply` returns `Err(s)` | `RewritePipeline::rewrite` aborts current phase, trace records `RuleFailed` | `Err(s)` transparently propagated; refresh path wraps as `RefreshError::user` |
| Step 5 rule `apply` returns `Ok(RewriteResult::Rejected(diag))` | `for_mv_refresh()` default is `FailFast`, pipeline converts diag.reason to `Err`, trace records `RuleRejected` | `Err(reason)`, wrapped same way |
| Step 5 deadline | `ctx_rw.check_deadline()` at stage and rule level | `Err("optimizer timeout during {operation}")` |
| Step 5 PR-α plan-not-equal-input (illegal in PR-α) | unit-test invariant `assert_eq!(format!("{plan:?}"), format!("{orig:?}"))`; if it ever fires in production it surfaces as a generic-suite regression first | n/a |

**Plan immutability on failure.** `LogicalPlan` is `Clone`. `pipeline.rewrite`
takes ownership of the plan and consumes it. The refresh path that calls
`run_imv_rewrite` keeps no reference to the consumed `plan_in` after the
call. Specifically:

- The refresh path constructs `plan_in` only to hand it to
  `run_imv_rewrite`; it does not retain a separate "old plan" copy.
- On `Err(...)`, no caller in PR-α uses a partially-mutated plan because
  the post-pipeline plan is discarded anyway.
- For PR-β, when the outcome plan starts being consumed, the failure path
  unwinds the local LogicalPlan binding via normal Rust value semantics —
  no shared mutable state exists.

This satisfies the task 3 acceptance criterion "rewrite 失败时后续 fallback
或错误处理看到的是原 plan" without introducing rollback infrastructure: the
type system is already sufficient.

## 7. Observability and Rule Disable

### 7.1 Trace already covered by framework

`RewritePipeline::rewrite` already emits, for each stage + rule + iteration:
`PhaseStarted / IterationStarted / RuleSkipped / RuleChanged / RuleFailed /
RuleRejected / PhaseEnded`. The IMV pipeline inherits this by construction.

`RUST_LOG=novarocks::sql::optimizer::rewrite=debug` surfaces the full event
stream. No additional plumbing required in PR-α.

### 7.2 Refresh-path summary log line

After step 5 succeeds, the refresh path emits exactly one
`info`-level line:

```rust
tracing::info!(
    target = %ctx.rewrite.target,
    mv_id  = ctx.rewrite.mv_id,
    stages = ?outcome.trace.stage_names(),
    rules_changed  = outcome.trace.changed_rules_count(),
    rules_rejected = outcome.trace.rejected_rules_count(),
    rules_failed   = outcome.trace.failed_rules_count(),
    "imv rewrite completed",
);
```

This requires four small additive helpers on `RewriteTrace` in `trace.rs`:

```rust
pub(crate) fn stage_names(&self) -> Vec<&'static str>;
pub(crate) fn changed_rules_count(&self) -> usize;
pub(crate) fn rejected_rules_count(&self) -> usize;
pub(crate) fn failed_rules_count(&self) -> usize;
```

Each is implementable as a straightforward iterator over `events()`.

### 7.3 Failure-path log

When `run_imv_rewrite` returns `Err(s)`, the refresh path emits one
`error`-level line:

```rust
tracing::error!(
    target = %ctx.rewrite.target,
    mv_id  = ctx.rewrite.mv_id,
    error  = %s,
    "imv rewrite failed",
);
```

before propagating the `RefreshError`. This is symmetric with task 1's
existing refresh-failure logging.

### 7.4 Rule disable wiring

- The IMV entrypoint receives `disabled_rules: Vec<String>` (cheap to
  clone; small set).
- The refresh path derives `disabled_rules` from the active session's
  `disable_optimizer_rules` SET var (already parsed into the session by
  `src/sql/optimizer/options.rs`).
- `RewriteContext::for_mv_refresh(disabled_rules)` already stores the set
  and short-circuits `matches()` for disabled rules (validated by the
  existing `disabled_rule_is_skipped_before_match` test).
- PR-α adds one new test that confirms the wire-up: a dummy registered IMV
  rule, disabled via the entrypoint argument, is skipped and never has its
  `matches()` invoked.

### 7.5 EXPLAIN integration

PR-α does NOT integrate `EXPLAIN` with the IMV pipeline. Reasons:

- Refresh path is not a query; there is no natural `EXPLAIN` entrypoint.
- `EXPLAIN ANALYZE` for MV refresh would need its own design (per-MV vs
  per-attempt, where to print the trace).
- The four trace-helper accessors in §7.2 give EXPLAIN-equivalent
  observability through `tracing` until a dedicated entrypoint is needed.

A future task (probably as part of `Refresh lifecycle hardening` task 12)
can add an `EXPLAIN MV REFRESH` SQL extension. Not in this spec.

## 8. PR-β — Marker Operators (Preparation)

### 8.1 Marker semantics

| Marker | Wraps | Meaning |
|---|---|---|
| `Delta(plan)` | typically root | "compute the incremental of plan" |
| `Version(plan, version_ref)` | typically Scan | "scan plan over snapshot window [from, to]" |

The PR-β driver registers one rule in the `imv-delta-marker` stage that
wraps the root with `Delta(root)` if not already wrapped. The
`imv-validation` stage registers one rule that walks the plan and rejects
if any `Delta` or `Version` remains unconsumed.

### 8.2 Representation candidates

PR-β picks one of:

- **R1 — extend `LogicalPlan` enum.** Add `LogicalPlan::ImvDelta(ImvDeltaNode)`
  and `LogicalPlan::ImvVersion(ImvVersionNode)`. All exhaustive matches in
  `src/sql/{planner,codegen,explain,analyzer}` get arms that panic or
  return error (these layers must never see a marker). `tree.rs` recursion
  naturally dispatches. **Pros:** type-safe, leverages existing traversal,
  panic arms are reliable trip-wires. **Cons:** ~20 match sites updated.

- **R2 — side-table annotation.** Markers live as
  `ImvPlanAnnotation { deltas: HashMap<NodeId, ...>, versions: HashMap<NodeId, ...> }`.
  `LogicalPlan` is unchanged. **Pros:** zero blast radius on shared types.
  **Cons:** `LogicalPlan` needs `NodeId` (it lacks one today); rules read
  annotation indirectly; tree traversal helper needs a paired traversal of
  the side table; trace less obvious.

- **R3 — private `ImvLogicalPlan` wrapper.** `imv/` defines
  `enum ImvLogicalPlan { Delta(...), Version(...), Plain(LogicalPlan) }`.
  Pipeline operates on the wrapper. **Pros:** marker fully encapsulated.
  **Cons:** `tree.rs` not reusable; need a parallel `imv/tree.rs`;
  task 4 (scan delta binding) would have to bridge the wrapper boundary
  every time it touches a Scan node.

### 8.3 Default starting position

**R1** is the default starting position when PR-β implementation begins.
Rationale:

1. The current `LogicalPlan` enum already has 19 variants; two more is not
   a structural change.
2. Exhaustive-match arms with `panic!("imv marker leaked into non-IMV plan")`
   are the most reliable guard against marker leak, complementing the
   `imv-validation` rule.
3. `tree.rs` and future task 4 (`Iceberg scan delta/version binding`)
   benefit from natively walking marker nodes.
4. R3 doubles `LogicalPlan ↔ ImvLogicalPlan` conversion cost per rule.

This is the *starting position*, not a binding decision: PR-β is allowed
to switch to R2 or R3 in its own design review if it surfaces new evidence.
PR-α's API surface is representation-agnostic — only the `plan` field of
`ImvRewriteOutcome` would change type under R3, which is anticipated.

### 8.4 Convergence check rule (PR-β)

```rust
struct UnresolvedMarkerCheckRule;
impl LogicalRewriteRule for UnresolvedMarkerCheckRule {
    fn name(&self) -> &'static str { "UnresolvedMarkerCheck" }
    fn phase(&self) -> RewritePhase { RewritePhase::Validation }
    fn matches(&self, plan: &LogicalPlan, _ctx: &RewriteContext) -> bool {
        plan_contains_imv_marker(plan)
    }
    fn apply(&self, plan: LogicalPlan, _ctx: &mut RewriteContext) -> Result<RewriteResult, String> {
        let markers = collect_marker_kinds(&plan);
        Ok(RewriteResult::Rejected(RewriteDiagnostic::rejected(
            self.name(),
            format!("IVM rewrite failed to resolve incremental markers: {:?}", markers),
        )))
    }
}
```

`for_mv_refresh()` defaults to `FailFast`, so a `Rejected` outcome aborts
the pipeline immediately, satisfying the task 3 acceptance criterion
`marker 未被消费时明确报错: 'IVM rewrite failed to resolve incremental markers'`.

### 8.5 Action column placeholder

`ImvDeltaNode` carries an `action_column: Option<ColumnRef>` field. PR-β
initializes it to `None`. Task 5 (Action column propagation) fills the
field and propagates it through Project/Filter/Aggregate/Join/Union.

### 8.6 Tests (PR-β)

- `marker_unresolved_yields_rejected_outcome`: pipeline + a rule that
  injects `Delta` but no rule consumes it → outcome is `Err("...IVM
  rewrite failed to resolve incremental markers...")` and trace contains
  `RuleRejected { rule: "UnresolvedMarkerCheck" }`.
- `marker_wrap_idempotent`: the wrap rule wraps a plain plan once;
  re-running the pipeline on the wrapped plan does not double-wrap.
- `mutation_guard_on_apply_error`: a rule that mutates `ImvDeltaNode` and
  returns `Err` does not corrupt the caller's plan.
- `regular_query_pipeline_does_not_produce_markers`: a non-IMV
  `RewritePipeline` (built by `RewritePipeline::new` with non-IMV rules)
  run on the same input plan does not emit `ImvDelta` or `ImvVersion`.

## 9. Testing

### 9.1 PR-α unit tests

Inside `src/sql/optimizer/rewrite/imv/tests/`:

| # | File | Test | Asserts |
|---|---|---|---|
| 1 | `noop_pipeline.rs` | `empty_imv_pipeline_returns_input_plan_verbatim` | `outcome.plan` debug-equal to input; trace contains every stage's `PhaseStarted/IterationStarted/PhaseEnded`; no `RuleChanged/RuleFailed/RuleRejected` |
| 2 | `noop_pipeline.rs` | `empty_pipeline_traces_all_four_stage_names` | `outcome.trace.stage_names() == ["imv-logical-normalize", "imv-delta-marker", "imv-marker-cleanup", "imv-validation"]` |
| 3 | `noop_pipeline.rs` | `annotation_is_default_initialized_in_extension_slot` | After pipeline run, `ctx.extension::<ImvExtension>().unwrap().annotation` equals `ImvPlanAnnotation::default()` |
| 4 | `noop_pipeline.rs` | `imv_rewrite_context_visible_through_extension` | A test-only IMV rule reads `ctx.extension::<ImvExtension>().unwrap().mv_ctx` and confirms target fqn matches input |
| 5 | `pipeline_disable_rule.rs` | `disabled_imv_rule_skipped_with_trace` | A dummy registered IMV rule, with its name in `disabled_rules`, is never `matches()`-called; trace contains `RuleSkipped { reason: "disabled" }` |
| 6 | `pipeline_disable_rule.rs` | `unknown_disabled_rule_name_is_ignored` | A non-existent rule name in `disabled_rules` does not break the pipeline |
| 7 | `pipeline_failure.rs` | `failing_imv_rule_does_not_mutate_input_plan` | A dummy rule returning `Err("boom")` → entrypoint returns Err; trace contains `RuleFailed`; caller's local plan binding still debug-equal to input |

### 9.2 PR-α traversal sanity test

In `src/sql/optimizer/rewrite/tree.rs`:

| # | Test | Asserts |
|---|---|---|
| 8 | `rewrite_visits_all_logical_plan_variants` | Build a synthetic `LogicalPlan` that includes one instance of every variant; run a counting "visit-all" dummy rule; assert visit count equals node count |

### 9.3 PR-α refresh-path integration check

Inside `src/engine/mv/iceberg_refresh.rs` (or a sibling test file):

| # | Test | Asserts |
|---|---|---|
| 9 | `refresh_completes_with_imv_pipeline_no_op_for_projection_filter` | Construct a ProjectionFilter MV; run refresh; assert refresh returns success and the result row set matches the baseline that today's hand-built path produces |

The repo does not currently depend on `tracing-test`, so test 9 verifies
behaviour rather than log output. Verifying the `"imv rewrite completed"`
log line is deferred to a later task (likely bundled with whichever task
introduces the first end-to-end tracing-capture harness). The
`iceberg-ivm` suite (run as part of §9.4) already covers production
refresh behavior end-to-end through the IMV pipeline; test 9 exists only
to keep the lib-test fast feedback loop sensitive to pipeline regressions
without requiring SQLite/MinIO setup.

### 9.4 PR-α suite gates

| Gate | Command | Expected |
|---|---|---|
| Rust lib unit tests | `cargo test -p novarocks --lib` | 2823 + 9 = 2832 passing (allowing ±1 for test count drift) |
| Cargo clippy | `cargo clippy --all-targets` | 0 warnings |
| Rustfmt | `cargo fmt -- --check` | clean |
| iceberg-ivm | `sql-tests --suite iceberg-ivm --mode verify` | 61/61 |
| iceberg | `sql-tests --suite iceberg --mode verify` | unchanged baseline |
| iceberg-rest | `sql-tests --suite iceberg-rest --mode verify` | unchanged baseline |

### 9.5 No PR-α SQL goldens

PR-α does not add `sql-tests/optimizer/imv_*.sql` fixtures because no IMV
plan-shape exists yet. The first goldens land in PR-β with marker shape.

## 10. Risks

- **R-1: re-planning canonical_select_query adds CPU per refresh.**
  The cost is one `Analyzer::analyze_query` + `plan_query` per refresh
  attempt. Refresh is not on the query hot path, but if this becomes a
  measurable cost, cache the resulting `LogicalPlan` in
  `IcebergMvRewriteContext` (with invalidation tied to MV definition
  version). Not implemented in PR-α — recorded as a follow-up.

- **R-2: analyzer behavior drift.** If
  `canonical_select_query` was canonicalized in a way that the standalone
  analyzer rejects (e.g., uses a syntactic form only the parser accepts),
  PR-α will fail every refresh. Mitigation: PR-α's iceberg-ivm gate
  catches this immediately; if any case fails analysis, the canonical
  form needs to be patched in the canonicalize step rather than working
  around it in `plan_canonical_select_for_imv`.

- **R-3: marker representation churn in PR-β.** Choosing R1/R2/R3 has
  cross-cutting consequences. Mitigation: §8.3's default starting position
  + the rule that PR-β can revisit before implementation; PR-α deliberately
  refuses to lock the `plan` field type of `ImvRewriteOutcome` beyond
  "LogicalPlan in PR-α; may evolve in PR-β".

- **R-4: trace summary helpers may be wanted elsewhere.** The four
  helpers on `RewriteTrace` (`stage_names / changed_rules_count /
  rejected_rules_count / failed_rules_count`) are useful in any tracing
  consumer. Mitigation: implement them on the generic `RewriteTrace`, not
  on a wrapper type, so they're shared.

- **R-5: extension slot is single-tenant.** `RewriteContext` holds one
  `Option<Arc<dyn Any + Send + Sync>>`. Calling `set_extension::<T>()`
  twice with different `T` overwrites. PR-α therefore wraps both
  `mv_ctx` and `annotation` into a single
  `ImvExtension { mv_ctx: Arc<IcebergMvRewriteContext>, annotation:
  ImvPlanAnnotation }` and calls `set_extension::<ImvExtension>(...)`
  exactly once (see §5 step 5b). Rules access mv_ctx and annotation
  through `ctx.extension::<ImvExtension>()`.

  **Mutability deferred.** `extension::<T>()` returns `&T`, immutable.
  PR-α rules do not mutate annotation (PR-α has no rules). PR-β / task 5
  will need a mutable path; candidates include extending the framework
  with `extension_mut::<T>()`, wrapping fields in `Arc<Mutex<...>>`, or
  using a "take + set" pattern. This is recorded as a known design
  surface for PR-β; PR-α does not pre-decide.

## 11. Acceptance

### PR-α

- `iceberg-ivm`, `iceberg`, and `iceberg-rest` SQL suites unchanged.
- `cargo test -p novarocks --lib` passes; ≥7 new IMV unit tests + 1
  traversal sanity test + 1 refresh-path integration test (or downgrade
  per §9.3).
- One `info`-level `"imv rewrite completed"` log line per refresh attempt;
  one `error`-level `"imv rewrite failed"` per failure.
- `disable_optimizer_rules` SET var disables a registered IMV rule (PR-α
  asserts the wire-up via a dummy rule; PR-β asserts on real rules).
- No new `EXPLAIN` SQL extension.
- No marker types introduced; `ImvPlanAnnotation` is empty.

### PR-β

- All PR-α acceptance still holds.
- `ImvDelta` and `ImvVersion` operator types exist (per §8.2's chosen
  representation).
- `Delta(root)` wrap rule registered in `imv-delta-marker` stage; unit
  test confirms idempotency.
- `UnresolvedMarkerCheck` rule registered in `imv-validation` stage; unit
  test confirms rejection with the canonical error message.
- A regular SELECT query optimizer run (non-IMV pipeline) does not produce
  markers in its output plan.
- One `sql-tests/optimizer/imv_marker_unresolved.sql` golden locks the
  fail-fast message.

## 12. Out-of-scope follow-ups

- Task 4 (Iceberg scan delta/version binding) consumes `ImvVersion`
  markers and binds them to Iceberg scan inputs.
- Task 5 (Action column propagation) fills the `ImvDeltaNode.action_column`
  field and propagates it through ProjectionFilter / Aggregate / Join /
  Union.
- Task 6 (Aggregate state rewrite over Iceberg target) migrates the
  single-base aggregate refresh path from hand-written helpers to IMV
  rules consumed by this foundation.
- Task 12 (Refresh lifecycle hardening) may introduce an `EXPLAIN MV
  REFRESH` SQL entrypoint that renders `ImvRewriteOutcome.trace`.
- Caching the planned `LogicalPlan` in `IcebergMvRewriteContext` for cost
  amortization (R-1).
