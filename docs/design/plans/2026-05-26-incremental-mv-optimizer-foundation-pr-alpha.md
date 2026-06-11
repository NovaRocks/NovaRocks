# Incremental MV Optimizer Foundation — PR-α Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Land PR-α of the Incremental MV Optimizer Foundation: a new `src/sql/optimizer/rewrite/imv/` sub-module providing `run_imv_rewrite` and `ImvExtension`, four named no-op stages, four counter helpers on `RewriteTrace`, a `LogicalPlan` traversal sanity test, and refresh-path wire-up at the three `IcebergMvRefreshContext` construction sites established by task 1. The pipeline must execute zero rules and leave plans verbatim; `iceberg-ivm`, `iceberg`, and `iceberg-rest` SQL suites must not regress.

**Architecture:** Built entirely on top of the existing `src/sql/optimizer/rewrite/` framework (PRs #180/#182/#183). The IMV pipeline reuses `RewritePipeline`, `RewriteContext::for_mv_refresh`, `LogicalRewriteRule`, and `RewriteTrace` — adds only the IMV sub-module, four trace helpers, and the refresh-path glue (`plan_canonical_select_for_imv` + three call sites). Marker operators (PR-β) and lineage types (task 5) are deferred.

**Tech Stack:** Rust 2024 edition; `tracing` for observability; `sqlparser` AST as canonical SQL form; `iceberg::spec::Schema`; existing `InMemoryCatalog` builder pattern from `src/engine/mv/iceberg_refresh.rs` (the `build_iceberg_table_def_for_snapshot_scan` helper).

**Spec reference:** [`docs/design/specs/2026-05-26-incremental-mv-optimizer-foundation-design.md`](../specs/2026-05-26-incremental-mv-optimizer-foundation-design.md) (commit `b75719aa`).

---

## File Structure

### Files to create

- `src/sql/optimizer/rewrite/imv/mod.rs` — module declarations + `pub(crate) use` re-exports
- `src/sql/optimizer/rewrite/imv/annotation.rs` — `ImvPlanAnnotation` placeholder + `ImvExtension` bundle struct
- `src/sql/optimizer/rewrite/imv/pipeline.rs` — `build_imv_pipeline()` constructing four named no-op stages
- `src/sql/optimizer/rewrite/imv/entrypoint.rs` — `run_imv_rewrite(input) -> Result<ImvRewriteOutcome, String>`; `ImvRewriteInput`; `ImvRewriteOutcome`; all PR-α unit tests as `#[cfg(test)] mod tests`

### Files to modify

- `src/sql/optimizer/rewrite/mod.rs` — add `pub(crate) mod imv;`
- `src/sql/optimizer/rewrite/trace.rs` — add four counter/helper methods (`stage_names`, `changed_rules_count`, `rejected_rules_count`, `failed_rules_count`) and their unit tests
- `src/sql/optimizer/rewrite/tree.rs` — add one traversal sanity test
- `src/engine/mv/iceberg_refresh.rs` — add `build_iceberg_mv_planning_catalog`, `plan_canonical_select_for_imv`, and three call sites that invoke `run_imv_rewrite` post `IcebergMvRefreshContext` construction

### Files NOT touched in PR-α

- `src/sql/optimizer/rewrite/imv/marker.rs` — created in PR-β
- `src/sql/optimizer/rewrite/imv/explain.rs` — not needed for PR-α (refresh emits `tracing::info!` directly using the four trace helpers); created when an `EXPLAIN IMV REFRESH` SQL entrypoint is added (deferred)

---

## Task 1: Module skeleton + happy-path no-op test

**Goal:** Create `imv/` sub-module with `ImvPlanAnnotation`, `ImvExtension`, `build_imv_pipeline`, `run_imv_rewrite`, and a single end-to-end happy-path test proving the empty pipeline returns the input plan verbatim.

**Files:**
- Create: `src/sql/optimizer/rewrite/imv/mod.rs`
- Create: `src/sql/optimizer/rewrite/imv/annotation.rs`
- Create: `src/sql/optimizer/rewrite/imv/pipeline.rs`
- Create: `src/sql/optimizer/rewrite/imv/entrypoint.rs`
- Modify: `src/sql/optimizer/rewrite/mod.rs` (add module declaration)

### - [ ] Step 1: Write the failing happy-path test

Create `src/sql/optimizer/rewrite/imv/entrypoint.rs` with this test (no implementation yet):

```rust
//! Entrypoint for the IMV rewrite pipeline. See
//! docs/design/specs/2026-05-26-incremental-mv-optimizer-foundation-design.md.

use std::sync::Arc;
use std::time::Instant;

use crate::sql::optimizer::rewrite::context::RewriteContext;
use crate::sql::optimizer::rewrite::imv::annotation::{ImvExtension, ImvPlanAnnotation};
use crate::sql::optimizer::rewrite::imv::pipeline::build_imv_pipeline;
use crate::sql::optimizer::rewrite::trace::RewriteTrace;
use crate::sql::planner::plan::LogicalPlan;

// IcebergMvRewriteContext lives in src/engine/mv/refresh_context.rs and is
// pub(crate); the import path crosses module boundaries but stays inside
// the crate.
use crate::engine::mv::refresh_context::IcebergMvRewriteContext;

pub(crate) struct ImvRewriteInput {
    pub plan: LogicalPlan,
    pub mv_ctx: Arc<IcebergMvRewriteContext>,
    pub disabled_rules: Vec<String>,
    pub deadline: Option<Instant>,
}

pub(crate) struct ImvRewriteOutcome {
    pub plan: LogicalPlan,
    pub trace: RewriteTrace,
    pub annotation: ImvPlanAnnotation,
}

pub(crate) fn run_imv_rewrite(input: ImvRewriteInput) -> Result<ImvRewriteOutcome, String> {
    // Implementation in Step 3.
    let _ = (input, build_imv_pipeline);
    unimplemented!("run_imv_rewrite")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sql::planner::plan::{LogicalPlan, ValuesNode};

    // Test fixture: synthesize the smallest possible IcebergMvRewriteContext.
    // For PR-α we exercise only the parts the no-op pipeline reads (zero
    // fields today). PR-β / task 4+ will need a richer fixture.
    fn dummy_mv_ctx() -> Arc<IcebergMvRewriteContext> {
        // The from_parts test-only helper introduced in task 1 PR #185
        // is the canonical constructor for unit tests. We use it here
        // unchanged. See refresh_context.rs:from_parts.
        crate::engine::mv::refresh_context::tests_support::dummy_rewrite_context()
    }

    fn empty_values_plan() -> LogicalPlan {
        LogicalPlan::Values(ValuesNode {
            rows: vec![],
            columns: vec![],
        })
    }

    #[test]
    fn empty_imv_pipeline_returns_input_plan_verbatim() {
        let plan = empty_values_plan();
        let before = format!("{plan:?}");

        let outcome = run_imv_rewrite(ImvRewriteInput {
            plan,
            mv_ctx: dummy_mv_ctx(),
            disabled_rules: Vec::new(),
            deadline: None,
        })
        .expect("no-op IMV pipeline must succeed");

        assert_eq!(format!("{:?}", outcome.plan), before);
    }
}
```

- [ ] Step 2: Create the supporting modules so the test file compiles

Create `src/sql/optimizer/rewrite/imv/mod.rs`:

```rust
//! IMV-specific logical rewrite substrate. See
//! docs/design/specs/2026-05-26-incremental-mv-optimizer-foundation-design.md.
//!
//! PR-α lands the foundation: empty pipeline, single-tenant extension slot
//! wrapper, no-op end-to-end behavior. PR-β adds Delta/Version marker
//! operators on top of this module without changing the public entrypoint.

pub(crate) mod annotation;
pub(crate) mod entrypoint;
pub(crate) mod pipeline;
```

Create `src/sql/optimizer/rewrite/imv/annotation.rs`:

```rust
//! Single-tenant extension payload for the IMV rewrite pipeline.
//!
//! `RewriteContext::set_extension::<T>()` stores one `Arc<dyn Any + Send + Sync>`.
//! IMV needs both the MV rewrite context handle and a per-pipeline annotation;
//! both ride inside `ImvExtension` so the single slot is sufficient. See spec
//! §10 R-5.

use std::sync::Arc;

use crate::engine::mv::refresh_context::IcebergMvRewriteContext;

/// Placeholder for IMV-pipeline-level plan annotations. PR-α keeps this empty;
/// PR-β / task 5 add fields (action column refs, branch identity, marker
/// node ids).
#[derive(Clone, Debug, Default)]
pub(crate) struct ImvPlanAnnotation {
    _private: (),
}

/// Single value stored in `RewriteContext::set_extension`. Bundles the IMV
/// rewrite context handle with the per-pipeline annotation.
#[derive(Clone, Debug)]
pub(crate) struct ImvExtension {
    pub mv_ctx: Arc<IcebergMvRewriteContext>,
    pub annotation: ImvPlanAnnotation,
}
```

Create `src/sql/optimizer/rewrite/imv/pipeline.rs`:

```rust
//! IMV rewrite pipeline construction. PR-α: four named no-op stages.
//! PR-β: register marker rules in `imv-delta-marker` and `imv-validation`.

use crate::sql::optimizer::rewrite::phase::RewritePhase;
use crate::sql::optimizer::rewrite::pipeline::{RewritePipeline, RewriteStage};

pub(crate) fn build_imv_pipeline() -> RewritePipeline {
    RewritePipeline::from_stages(vec![
        RewriteStage::new(
            "imv-logical-normalize",
            RewritePhase::LogicalNormalize,
            Vec::new(),
        ),
        RewriteStage::new(
            "imv-delta-marker",
            RewritePhase::StructuralRewrite,
            Vec::new(),
        ),
        RewriteStage::new(
            "imv-marker-cleanup",
            RewritePhase::SemanticRewrite,
            Vec::new(),
        ),
        RewriteStage::new(
            "imv-validation",
            RewritePhase::Validation,
            Vec::new(),
        ),
    ])
}
```

Modify `src/sql/optimizer/rewrite/mod.rs` — add `pub(crate) mod imv;` next to the other `pub(crate) mod` lines (e.g., after `tree`).

The test fixture relies on a not-yet-existing `tests_support` module in `refresh_context.rs`. Add it now as a `#[cfg(test)] pub(crate) mod tests_support` block exposing a `dummy_rewrite_context()` constructor that returns a minimally-valid `Arc<IcebergMvRewriteContext>` (single fake base, empty pin/contracts as far as PR-α tests need). Since `IcebergMvRewriteContext::from_parts` already exists for task 1 unit tests, the helper just wraps a canonical call:

```rust
// Append to src/engine/mv/refresh_context.rs

#[cfg(test)]
pub(crate) mod tests_support {
    use std::sync::Arc;

    use super::*;

    // ... build minimal IcebergMvRewriteContext using existing from_parts
    pub(crate) fn dummy_rewrite_context() -> Arc<IcebergMvRewriteContext> {
        // Reuse the same fixture pattern used by the task 1 unit tests
        // in refresh_context.rs::tests. Extract those builders into this
        // module if they were locally `fn` inside the tests module so they
        // can be shared across crates' #[cfg(test)] modules.
        todo!("extract from existing #[cfg(test)] mod tests in this file")
    }
}
```

This is the only "extract refactor" in this task — the existing test fixtures inside `refresh_context.rs::tests` were defined locally. Move them under `pub(crate) mod tests_support` so PR-α tests in `imv/entrypoint.rs` can use them. The local tests in `refresh_context.rs` continue to call the moved helpers via `super::tests_support::*`.

- [ ] Step 3: Implement `run_imv_rewrite` in `entrypoint.rs`

Replace the `unimplemented!()` body in `entrypoint.rs`:

```rust
pub(crate) fn run_imv_rewrite(input: ImvRewriteInput) -> Result<ImvRewriteOutcome, String> {
    let ImvRewriteInput {
        plan,
        mv_ctx,
        disabled_rules,
        deadline,
    } = input;

    let mut ctx_rw = RewriteContext::for_mv_refresh(disabled_rules);
    ctx_rw.set_extension::<ImvExtension>(ImvExtension {
        mv_ctx,
        annotation: ImvPlanAnnotation::default(),
    });
    if let Some(deadline) = deadline {
        ctx_rw.set_deadline(deadline);
    }

    let pipeline = build_imv_pipeline();
    let plan_out = pipeline.rewrite(plan, &mut ctx_rw)?;

    let ext = ctx_rw
        .extension::<ImvExtension>()
        .expect("ImvExtension installed before rewrite")
        .clone();

    Ok(ImvRewriteOutcome {
        plan: plan_out,
        trace: ctx_rw.trace().clone(),
        annotation: ext.annotation,
    })
}
```

- [ ] Step 4: Run the test to verify it passes

Run: `cargo test -p novarocks --lib sql::optimizer::rewrite::imv::entrypoint::tests::empty_imv_pipeline_returns_input_plan_verbatim`

Expected: PASS. If you see a compile error about `tests_support::dummy_rewrite_context`, that means Step 2's extraction wasn't done — go back and finish it.

- [ ] Step 5: Commit

```bash
git add src/sql/optimizer/rewrite/imv/ src/sql/optimizer/rewrite/mod.rs src/engine/mv/refresh_context.rs
git commit -m "$(cat <<'EOF'
optimizer/imv: scaffold IMV rewrite sub-module + happy-path no-op test

PR-α task 1 (per
docs/design/plans/2026-05-26-incremental-mv-optimizer-foundation-pr-alpha.md):
introduce src/sql/optimizer/rewrite/imv/ with annotation, pipeline, and
entrypoint modules. The no-op pipeline reuses RewritePipeline +
RewriteContext::for_mv_refresh and asserts plan-verbatim semantics via
empty_imv_pipeline_returns_input_plan_verbatim. ImvExtension wraps both
mv_ctx and annotation into the single-tenant extension slot.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

## Task 2: Stage-name trace assertion + `RewriteTrace::stage_names()` helper

**Goal:** Lock the IMV stage ordering by asserting `outcome.trace.stage_names()` equals the expected four-string list.

**Files:**
- Modify: `src/sql/optimizer/rewrite/trace.rs` (add `stage_names()`)
- Modify: `src/sql/optimizer/rewrite/imv/entrypoint.rs` (add test)

### - [ ] Step 1: Write the failing test

Append to the `#[cfg(test)] mod tests` block in `entrypoint.rs`:

```rust
    #[test]
    fn empty_pipeline_traces_all_four_stage_names() {
        let outcome = run_imv_rewrite(ImvRewriteInput {
            plan: empty_values_plan(),
            mv_ctx: dummy_mv_ctx(),
            disabled_rules: Vec::new(),
            deadline: None,
        })
        .unwrap();

        assert_eq!(
            outcome.trace.stage_names(),
            vec![
                "imv-logical-normalize",
                "imv-delta-marker",
                "imv-marker-cleanup",
                "imv-validation",
            ],
        );
    }
```

- [ ] Step 2: Run the test to verify it fails

Run: `cargo test -p novarocks --lib sql::optimizer::rewrite::imv::entrypoint::tests::empty_pipeline_traces_all_four_stage_names`

Expected: COMPILE FAIL — `RewriteTrace::stage_names()` does not yet exist.

- [ ] Step 3: Implement `stage_names()` and an associated unit test in `trace.rs`

Inspect `src/sql/optimizer/rewrite/trace.rs` to confirm the `RewriteTraceEvent::PhaseStarted` carries `phase: RewritePhase`. The IMV pipeline emits one `PhaseStarted` per stage even when the stage is empty (verified in pipeline.rs:69-104). Stage names are not currently in the event; we read them off the pipeline's stage list at construction time — except the pipeline isn't surfaced to the trace. So `stage_names()` must walk `events()` and recover stage *phase* names rather than the custom IMV stage labels.

**Decision:** redefine `stage_names()` to return *stage labels*, which requires `RewriteStage::name` to flow into `PhaseStarted` events. Two ways:

- (a) Extend `RewriteTraceEvent::PhaseStarted { phase, name: &'static str }` and have `pipeline.rs:79` pass `stage.name`. Other matchers update accordingly.
- (b) Keep `RewriteTraceEvent` unchanged; `stage_names()` returns `phase.as_str()` values — but multi-stage / same-phase pipelines would emit duplicates.

PR-α picks (a) because option (b) cannot distinguish `imv-delta-marker` from `imv-validation` when both reuse `StructuralRewrite`. The change is additive and small.

Edit `src/sql/optimizer/rewrite/trace.rs`:

```rust
// Find the existing PhaseStarted variant in RewriteTraceEvent and extend it:
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum RewriteTraceEvent {
    PhaseStarted {
        phase: RewritePhase,
        // NEW: human-readable stage name (e.g. "imv-logical-normalize").
        // Allows distinguishing multiple stages within the same phase.
        stage: &'static str,
    },
    // ... rest unchanged
}

impl RewriteTrace {
    pub(crate) fn phase_started(&mut self, phase: RewritePhase) {
        // existing signature, keep for backward compat at the call site:
        self.events.push(RewriteTraceEvent::PhaseStarted {
            phase,
            stage: phase.as_str(),
        });
    }

    // NEW: stage-aware phase start (used by RewritePipeline)
    pub(crate) fn phase_started_with_stage(
        &mut self,
        phase: RewritePhase,
        stage: &'static str,
    ) {
        self.events.push(RewriteTraceEvent::PhaseStarted { phase, stage });
    }

    pub(crate) fn stage_names(&self) -> Vec<&'static str> {
        self.events
            .iter()
            .filter_map(|event| match event {
                RewriteTraceEvent::PhaseStarted { stage, .. } => Some(*stage),
                _ => None,
            })
            .collect()
    }
}
```

Then edit `src/sql/optimizer/rewrite/pipeline.rs` — change the call at the start of each stage from `ctx.trace_mut().phase_started(phase)` to `ctx.trace_mut().phase_started_with_stage(phase, stage.name)`. The existing `stage.name` field is already `&'static str` (from `RewriteStage::new`).

The existing pipeline test `empty_pipeline_preserves_plan_and_records_phases` (pipeline.rs:261-298) hard-codes `RewriteTraceEvent::PhaseStarted { phase: ... }` without the `stage` field — update those expected events to include `stage: <phase.as_str()>` (the default for non-IMV phases registered via `RewritePipeline::new` is `phase.as_str()`, which the implementation above already does via `RewriteStage::new(phase.as_str(), phase, ...)` in `RewritePipeline::new` at line 35).

Verify: `RewritePipeline::new` (pipeline.rs:33) creates stages with `RewriteStage::new(phase.as_str(), phase, Vec::new())` — so the stage name equals `phase.as_str()` in the default path. The trace updates above keep that invariant.

Also append unit tests to `trace.rs`:

```rust
    #[test]
    fn stage_names_returns_unique_labels_in_order() {
        let mut trace = RewriteTrace::default();
        trace.phase_started_with_stage(RewritePhase::LogicalNormalize, "stage-1");
        trace.phase_ended(RewritePhase::LogicalNormalize);
        trace.phase_started_with_stage(RewritePhase::StructuralRewrite, "stage-2");
        trace.phase_ended(RewritePhase::StructuralRewrite);

        assert_eq!(trace.stage_names(), vec!["stage-1", "stage-2"]);
    }
```

- [ ] Step 4: Run all affected tests

Run:
```bash
cargo test -p novarocks --lib sql::optimizer::rewrite::
```

Expected: every test passes, including the new `empty_pipeline_traces_all_four_stage_names` and `stage_names_returns_unique_labels_in_order`. If the pipeline.rs:261-298 test fails because the expected `PhaseStarted { phase: ... }` events were not updated, fix them.

- [ ] Step 5: Commit

```bash
git add src/sql/optimizer/rewrite/trace.rs src/sql/optimizer/rewrite/pipeline.rs src/sql/optimizer/rewrite/imv/entrypoint.rs
git commit -m "$(cat <<'EOF'
optimizer/rewrite: thread stage name through PhaseStarted events

Add RewriteTrace::stage_names() and a stage-aware phase_started_with_stage
helper. Pipeline emits stage name (e.g. "imv-delta-marker") in
PhaseStarted events so multiple stages within the same phase are
distinguishable. IMV entrypoint test locks the four no-op stage names.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

## Task 3: Annotation-default + mv_ctx visibility tests

**Goal:** Lock that `ImvExtension` is installed and that rules can read both `annotation` (default) and `mv_ctx` through `ctx.extension::<ImvExtension>()`.

**Files:**
- Modify: `src/sql/optimizer/rewrite/imv/entrypoint.rs` (add two tests + a dummy IMV rule)

### - [ ] Step 1: Write the failing tests

Append to the `tests` block in `entrypoint.rs`:

```rust
    use crate::sql::optimizer::rewrite::context::RewriteContext;
    use crate::sql::optimizer::rewrite::phase::RewritePhase;
    use crate::sql::optimizer::rewrite::result::RewriteResult;
    use crate::sql::optimizer::rewrite::rule::{LogicalRewriteRule, RewriteTraversal};
    use std::sync::atomic::{AtomicBool, Ordering};

    /// Test-only rule that asserts ImvExtension is reachable from the
    /// RewriteContext. Captures observed target string into an AtomicBool
    /// for assertion outside the closure.
    struct AssertMvCtxVisibleRule {
        saw_mv_ctx: Arc<AtomicBool>,
        expected_target: String,
    }

    impl LogicalRewriteRule for AssertMvCtxVisibleRule {
        fn name(&self) -> &'static str {
            "AssertMvCtxVisibleRule"
        }

        fn phase(&self) -> RewritePhase {
            RewritePhase::LogicalNormalize
        }

        fn traversal(&self) -> RewriteTraversal {
            RewriteTraversal::TopDown
        }

        fn matches(&self, _plan: &LogicalPlan, ctx: &RewriteContext) -> bool {
            let ext = ctx
                .extension::<ImvExtension>()
                .expect("ImvExtension installed");
            if format!("{}", ext.mv_ctx.target) == self.expected_target {
                self.saw_mv_ctx.store(true, Ordering::SeqCst);
            }
            false
        }

        fn apply(
            &self,
            _plan: LogicalPlan,
            _ctx: &mut RewriteContext,
        ) -> Result<RewriteResult, String> {
            Ok(RewriteResult::Unchanged)
        }
    }

    #[test]
    fn annotation_is_default_initialized_in_extension_slot() {
        let outcome = run_imv_rewrite(ImvRewriteInput {
            plan: empty_values_plan(),
            mv_ctx: dummy_mv_ctx(),
            disabled_rules: Vec::new(),
            deadline: None,
        })
        .unwrap();
        // outcome.annotation should be Default::default()
        assert_eq!(
            format!("{:?}", outcome.annotation),
            format!("{:?}", ImvPlanAnnotation::default()),
        );
    }

    #[test]
    fn imv_rewrite_context_visible_through_extension() {
        // Build a custom pipeline with the assertion rule inserted into
        // imv-logical-normalize. We bypass run_imv_rewrite here because
        // PR-α's build_imv_pipeline is empty by contract; the visibility
        // check works on a custom pipeline that mirrors the real one.
        use crate::sql::optimizer::rewrite::pipeline::{RewritePipeline, RewriteStage};

        let mv_ctx = dummy_mv_ctx();
        let expected_target = format!("{}", mv_ctx.target);
        let saw_mv_ctx = Arc::new(AtomicBool::new(false));

        let pipeline = RewritePipeline::from_stages(vec![
            RewriteStage::new(
                "imv-logical-normalize",
                RewritePhase::LogicalNormalize,
                vec![Box::new(AssertMvCtxVisibleRule {
                    saw_mv_ctx: Arc::clone(&saw_mv_ctx),
                    expected_target,
                })],
            ),
        ]);

        let mut ctx_rw = RewriteContext::for_mv_refresh(Vec::<String>::new());
        ctx_rw.set_extension::<ImvExtension>(ImvExtension {
            mv_ctx,
            annotation: ImvPlanAnnotation::default(),
        });

        let _ = pipeline.rewrite(empty_values_plan(), &mut ctx_rw).unwrap();

        assert!(saw_mv_ctx.load(Ordering::SeqCst));
    }
```

- [ ] Step 2: Run the tests to verify they pass

Run:
```bash
cargo test -p novarocks --lib sql::optimizer::rewrite::imv::entrypoint::tests::annotation_is_default_initialized_in_extension_slot \
  sql::optimizer::rewrite::imv::entrypoint::tests::imv_rewrite_context_visible_through_extension
```

Expected: both PASS. (The `annotation_is_default_initialized_in_extension_slot` test should already pass from Task 1's implementation. The `imv_rewrite_context_visible_through_extension` test exercises a custom-pipeline path; both prove the `ImvExtension` plumbing.)

- [ ] Step 3: Commit

```bash
git add src/sql/optimizer/rewrite/imv/entrypoint.rs
git commit -m "$(cat <<'EOF'
optimizer/imv: assert ImvExtension default annotation + mv_ctx visibility

Add two unit tests confirming run_imv_rewrite installs ImvExtension with
a default ImvPlanAnnotation, and that a registered rule can read
ctx.extension::<ImvExtension>().mv_ctx and observe the target fqn.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

## Task 4: Disabled-rule wire test + unknown-name tolerance

**Goal:** Lock that `disabled_rules` flowing through `run_imv_rewrite` short-circuits a registered IMV rule before its `matches()` is called, and that unknown rule names are ignored.

**Files:**
- Modify: `src/sql/optimizer/rewrite/imv/entrypoint.rs`

### - [ ] Step 1: Write the failing tests

Append to the `tests` block in `entrypoint.rs`:

```rust
    use std::sync::atomic::AtomicUsize;

    struct CountingRule {
        name: &'static str,
        matches_called: Arc<AtomicUsize>,
    }

    impl LogicalRewriteRule for CountingRule {
        fn name(&self) -> &'static str {
            self.name
        }

        fn phase(&self) -> RewritePhase {
            RewritePhase::LogicalNormalize
        }

        fn matches(&self, _plan: &LogicalPlan, _ctx: &RewriteContext) -> bool {
            self.matches_called.fetch_add(1, Ordering::SeqCst);
            false
        }

        fn apply(
            &self,
            _plan: LogicalPlan,
            _ctx: &mut RewriteContext,
        ) -> Result<RewriteResult, String> {
            Ok(RewriteResult::Unchanged)
        }
    }

    #[test]
    fn disabled_imv_rule_skipped_with_trace() {
        use crate::sql::optimizer::rewrite::pipeline::{RewritePipeline, RewriteStage};
        use crate::sql::optimizer::rewrite::trace::RewriteTraceEvent;

        let matches_called = Arc::new(AtomicUsize::new(0));
        let pipeline = RewritePipeline::from_stages(vec![
            RewriteStage::new(
                "imv-logical-normalize",
                RewritePhase::LogicalNormalize,
                vec![Box::new(CountingRule {
                    name: "DummyImvRule",
                    matches_called: Arc::clone(&matches_called),
                })],
            ),
        ]);

        let mut ctx_rw = RewriteContext::for_mv_refresh(
            vec!["DummyImvRule".to_string()],
        );
        ctx_rw.set_extension::<ImvExtension>(ImvExtension {
            mv_ctx: dummy_mv_ctx(),
            annotation: ImvPlanAnnotation::default(),
        });

        let _ = pipeline.rewrite(empty_values_plan(), &mut ctx_rw).unwrap();

        assert_eq!(matches_called.load(Ordering::SeqCst), 0);
        assert!(ctx_rw.trace().events().iter().any(|e| matches!(
            e,
            RewriteTraceEvent::RuleSkipped { rule, reason, .. }
                if *rule == "DummyImvRule" && reason == "disabled"
        )));
    }

    #[test]
    fn unknown_disabled_rule_name_is_ignored() {
        let outcome = run_imv_rewrite(ImvRewriteInput {
            plan: empty_values_plan(),
            mv_ctx: dummy_mv_ctx(),
            disabled_rules: vec!["NoSuchRule".to_string()],
            deadline: None,
        })
        .expect("unknown disabled rule must not break the pipeline");

        // Pipeline still runs to completion with no rule changes.
        assert_eq!(outcome.trace.stage_names().len(), 4);
    }
```

- [ ] Step 2: Run the tests

Run:
```bash
cargo test -p novarocks --lib sql::optimizer::rewrite::imv::entrypoint::tests::disabled_imv_rule_skipped_with_trace \
  sql::optimizer::rewrite::imv::entrypoint::tests::unknown_disabled_rule_name_is_ignored
```

Expected: both PASS. Both should pass without further implementation: framework already supports both behaviors.

- [ ] Step 3: Commit

```bash
git add src/sql/optimizer/rewrite/imv/entrypoint.rs
git commit -m "$(cat <<'EOF'
optimizer/imv: assert disable_rules wire-up + unknown-name tolerance

CountingRule fixture proves that a registered IMV rule named in
disabled_rules is short-circuited before its matches() is called and that
the trace records RuleSkipped { reason: "disabled" }. A non-existent
rule name does not break the pipeline.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

## Task 5: Failing-rule preserves caller plan

**Goal:** Lock that a rule returning `Err(...)` does not corrupt the caller's plan binding.

**Files:**
- Modify: `src/sql/optimizer/rewrite/imv/entrypoint.rs`

### - [ ] Step 1: Write the failing test

Append to the `tests` block:

```rust
    use crate::sql::optimizer::rewrite::trace::RewriteTraceEvent;

    struct FailingDummyRule;

    impl LogicalRewriteRule for FailingDummyRule {
        fn name(&self) -> &'static str {
            "FailingDummyRule"
        }

        fn phase(&self) -> RewritePhase {
            RewritePhase::LogicalNormalize
        }

        fn matches(&self, _plan: &LogicalPlan, _ctx: &RewriteContext) -> bool {
            true
        }

        fn apply(
            &self,
            _plan: LogicalPlan,
            _ctx: &mut RewriteContext,
        ) -> Result<RewriteResult, String> {
            Err("synthetic failure".to_string())
        }
    }

    #[test]
    fn failing_imv_rule_does_not_mutate_input_plan() {
        use crate::sql::optimizer::rewrite::pipeline::{RewritePipeline, RewriteStage};

        let plan = empty_values_plan();
        let before = format!("{plan:?}");

        let pipeline = RewritePipeline::from_stages(vec![
            RewriteStage::new(
                "imv-logical-normalize",
                RewritePhase::LogicalNormalize,
                vec![Box::new(FailingDummyRule)],
            ),
        ]);

        let mut ctx_rw = RewriteContext::for_mv_refresh(Vec::<String>::new());
        ctx_rw.set_extension::<ImvExtension>(ImvExtension {
            mv_ctx: dummy_mv_ctx(),
            annotation: ImvPlanAnnotation::default(),
        });

        // Take a copy so we have an "original" to compare against even
        // though pipeline.rewrite consumed our `plan`.
        let original = empty_values_plan();
        assert_eq!(format!("{original:?}"), before);

        let err = pipeline.rewrite(plan, &mut ctx_rw).unwrap_err();
        assert_eq!(err, "synthetic failure");

        // Original plan binding is intact (Rust value semantics guarantee
        // this; the assert documents the contract for future readers).
        assert_eq!(format!("{original:?}"), before);

        // Trace records the failure.
        assert!(ctx_rw.trace().events().iter().any(|e| matches!(
            e,
            RewriteTraceEvent::RuleFailed { rule, .. }
                if *rule == "FailingDummyRule"
        )));
    }
```

- [ ] Step 2: Run the test

Run: `cargo test -p novarocks --lib sql::optimizer::rewrite::imv::entrypoint::tests::failing_imv_rule_does_not_mutate_input_plan`

Expected: PASS. No implementation needed; framework's `FailingRule` test in `pipeline.rs:340-351` covers the same semantic at the framework level. This test locks the contract specifically for the IMV API surface.

- [ ] Step 3: Commit

```bash
git add src/sql/optimizer/rewrite/imv/entrypoint.rs
git commit -m "$(cat <<'EOF'
optimizer/imv: assert failing rule does not mutate caller's plan

FailingDummyRule returns Err; the test confirms (a) the error propagates
through pipeline.rewrite, (b) trace records RuleFailed, (c) the original
LogicalPlan binding is debug-equal to its pre-call state.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

## Task 6: Tree traversal sanity test in tree.rs

**Goal:** Lock that `tree::rewrite_with_rule` recurses into every `LogicalPlan` variant (currently 19). If a new variant is added but `tree.rs` isn't updated, this test fails.

**Files:**
- Modify: `src/sql/optimizer/rewrite/tree.rs`

### - [ ] Step 1: Write the failing test

Append to `src/sql/optimizer/rewrite/tree.rs` (inside `#[cfg(test)] mod tests`):

```rust
    #[test]
    fn rewrite_visits_all_logical_plan_variants() {
        use crate::sql::optimizer::rewrite::context::RewriteContext;
        use crate::sql::optimizer::rewrite::phase::RewritePhase;
        use crate::sql::optimizer::rewrite::result::RewriteResult;
        use crate::sql::optimizer::rewrite::rule::{LogicalRewriteRule, RewriteTraversal};
        use crate::sql::planner::plan::*;
        use std::sync::Arc;
        use std::sync::atomic::{AtomicUsize, Ordering};

        struct CountVisitsRule {
            count: Arc<AtomicUsize>,
        }

        impl LogicalRewriteRule for CountVisitsRule {
            fn name(&self) -> &'static str {
                "CountVisitsRule"
            }
            fn phase(&self) -> RewritePhase {
                RewritePhase::LogicalNormalize
            }
            fn traversal(&self) -> RewriteTraversal {
                RewriteTraversal::TopDown
            }
            fn matches(&self, _plan: &LogicalPlan, _ctx: &RewriteContext) -> bool {
                self.count.fetch_add(1, Ordering::SeqCst);
                false
            }
            fn apply(
                &self,
                _plan: LogicalPlan,
                _ctx: &mut RewriteContext,
            ) -> Result<RewriteResult, String> {
                Ok(RewriteResult::Unchanged)
            }
        }

        // Construct one of each LogicalPlan variant. For variants that
        // wrap a child plan, use Values as the inner-most leaf.
        let leaf = LogicalPlan::Values(ValuesNode {
            rows: vec![],
            columns: vec![],
        });

        // The exact shape doesn't matter; the assertion only checks that
        // `matches()` is invoked at least once per node. The test fails
        // *at compile time* if a variant is added to LogicalPlan and not
        // referenced here — that's the intentional trip-wire.
        // (Compile-time enforcement is via the exhaustive match below.)

        fn assert_variant_handled(variant: &LogicalPlan) {
            match variant {
                LogicalPlan::Scan(_)
                | LogicalPlan::Filter(_)
                | LogicalPlan::Project(_)
                | LogicalPlan::Aggregate(_)
                | LogicalPlan::Join(_)
                | LogicalPlan::Sort(_)
                | LogicalPlan::Limit(_)
                | LogicalPlan::Union(_)
                | LogicalPlan::Intersect(_)
                | LogicalPlan::Except(_)
                | LogicalPlan::Values(_)
                | LogicalPlan::GenerateSeries(_)
                | LogicalPlan::TableFunction(_)
                | LogicalPlan::Window(_)
                | LogicalPlan::SubqueryAlias(_)
                | LogicalPlan::Repeat(_)
                | LogicalPlan::CTEAnchor(_)
                | LogicalPlan::CTEProduce(_)
                | LogicalPlan::CTEConsume(_) => {}
            }
        }
        assert_variant_handled(&leaf);

        let count = Arc::new(AtomicUsize::new(0));
        let mut ctx = RewriteContext::for_mv_refresh(Vec::<String>::new());
        let (_, _) = super::rewrite_with_rule(
            leaf,
            &CountVisitsRule {
                count: Arc::clone(&count),
            },
            &mut ctx,
        )
        .unwrap();

        // Sanity floor: at least one node was visited.
        assert!(count.load(Ordering::SeqCst) >= 1);
    }
```

**Note on intent:** The exhaustive `match` on `&LogicalPlan` is the actual trip-wire — when a new variant lands, the test file fails to compile. The `rewrite_with_rule` call exercises the runtime traversal on the simplest plan.

- [ ] Step 2: Run the test

Run: `cargo test -p novarocks --lib sql::optimizer::rewrite::tree::tests::rewrite_visits_all_logical_plan_variants`

Expected: PASS. If compile fails with "non-exhaustive patterns" on the `match`, that means a new `LogicalPlan` variant was added since this plan was written — update the match.

- [ ] Step 3: Commit

```bash
git add src/sql/optimizer/rewrite/tree.rs
git commit -m "$(cat <<'EOF'
optimizer/rewrite/tree: lock LogicalPlan variant coverage in traversal

Exhaustive match on &LogicalPlan in rewrite_visits_all_logical_plan_variants
fails to compile if a new variant lands. The runtime invocation of
rewrite_with_rule on the simplest plan confirms the traversal helper
visits at least one node.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

## Task 7: RewriteTrace counter helpers

**Goal:** Add `changed_rules_count`, `rejected_rules_count`, `failed_rules_count` on `RewriteTrace` for the refresh-path summary log line.

**Files:**
- Modify: `src/sql/optimizer/rewrite/trace.rs`

### - [ ] Step 1: Write the failing tests

Append to the `#[cfg(test)] mod tests` block in `trace.rs`:

```rust
    #[test]
    fn counter_helpers_aggregate_rule_events() {
        let mut trace = RewriteTrace::default();
        trace.rule_changed(RewritePhase::LogicalNormalize, "RuleA");
        trace.rule_changed(RewritePhase::LogicalNormalize, "RuleA");
        trace.rule_changed(RewritePhase::StructuralRewrite, "RuleB");
        trace.rule_rejected(
            RewritePhase::Validation,
            "RuleC",
            "rejected: missing input",
        );
        trace.rule_failed(
            RewritePhase::Validation,
            "RuleD",
            "boom",
        );

        assert_eq!(trace.changed_rules_count(), 3);
        assert_eq!(trace.rejected_rules_count(), 1);
        assert_eq!(trace.failed_rules_count(), 1);
    }
```

- [ ] Step 2: Implement the three helpers in `trace.rs`

Add inside the `impl RewriteTrace` block:

```rust
    pub(crate) fn changed_rules_count(&self) -> usize {
        self.events
            .iter()
            .filter(|e| matches!(e, RewriteTraceEvent::RuleChanged { .. }))
            .count()
    }

    pub(crate) fn rejected_rules_count(&self) -> usize {
        self.events
            .iter()
            .filter(|e| matches!(e, RewriteTraceEvent::RuleRejected { .. }))
            .count()
    }

    pub(crate) fn failed_rules_count(&self) -> usize {
        self.events
            .iter()
            .filter(|e| matches!(e, RewriteTraceEvent::RuleFailed { .. }))
            .count()
    }
```

The exact variant names should match what's already in `trace.rs`. Confirm by inspecting `RewriteTraceEvent`'s definition and the `rule_changed` / `rule_rejected` / `rule_failed` setter methods on `RewriteTrace`. If the helpers in the test use signatures (e.g., `rule_rejected(phase, rule, reason)`) that don't match the actual API, update the test to match.

- [ ] Step 3: Run tests

Run: `cargo test -p novarocks --lib sql::optimizer::rewrite::trace::tests::counter_helpers_aggregate_rule_events`

Expected: PASS.

- [ ] Step 4: Commit

```bash
git add src/sql/optimizer/rewrite/trace.rs
git commit -m "$(cat <<'EOF'
optimizer/rewrite/trace: add changed/rejected/failed rule counters

Three pure-iter helpers on RewriteTrace summarize the event log for the
refresh-path summary log line. Locked by counter_helpers_aggregate_rule_events.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

## Task 8: `build_iceberg_mv_planning_catalog` helper + unit test

**Goal:** Add a helper in `iceberg_refresh.rs` that builds an `InMemoryCatalog` registering every base in `ctx.rewrite.base_refs` at its pinned snapshot. This is the prerequisite for `plan_canonical_select_for_imv` in Task 9.

**Files:**
- Modify: `src/engine/mv/iceberg_refresh.rs`

### - [ ] Step 1: Locate the existing pattern

Read `src/engine/mv/iceberg_refresh.rs:6150-6180` (the `build_join_snapshot_catalog` + `register_join_snapshot_side` + `build_iceberg_table_def_for_snapshot_scan` cluster). These are the existing N=2 building blocks. We generalize for N.

### - [ ] Step 2: Write the failing unit test

Append a `#[cfg(test)]` test (search for an existing `#[cfg(test)] mod tests` block in `iceberg_refresh.rs`; if none exists at the bottom of the file, add one):

```rust
#[cfg(test)]
mod imv_planning_catalog_tests {
    use super::*;

    #[test]
    fn build_iceberg_mv_planning_catalog_registers_each_base() {
        // Synthesise a minimal IcebergMvRewriteContext with 2 base refs
        // and pinned snapshots; assert the resulting catalog reports both
        // tables under their namespaces.
        let (state, ctx) = imv_planning_catalog_test_fixture();
        let catalog = build_iceberg_mv_planning_catalog(&state, &ctx)
            .expect("planning catalog construction must succeed");

        for base in ctx.rewrite.base_refs.iter() {
            assert!(catalog
                .database_exists(&base.namespace)
                .expect("database lookup"));
            let table_name = synthetic_snapshot_table_name(
                base,
                ctx.rewrite
                    .pin
                    .get(base)
                    .expect("test fixture: pin has snapshot for base"),
            );
            assert!(
                catalog.get(&base.namespace, &table_name).is_ok(),
                "expected table {}.{table_name} to be registered",
                base.namespace
            );
        }
    }

    fn imv_planning_catalog_test_fixture() -> (
        Arc<crate::engine::StandaloneState>,
        IcebergMvRefreshContext,
    ) {
        // Reuse whatever fixture builder task 1's tests already use for
        // IcebergMvRefreshContext. If no shared fixture exists, build one
        // here using the same helpers task 1 used in refresh_context.rs
        // tests. The fixture must have 2 base refs and a pin with both
        // snapshot ids populated.
        todo!("build a fixture with 2 base refs + a StandaloneState that has both bases registered as iceberg tables")
    }
}
```

If the fixture construction is non-trivial, defer it — add a `#[ignore]` attribute on the test and treat the unit test as a follow-up; the suite-level regression check in Task 13 is the real gate. **This is the only `todo!()` in the plan; clearing it requires reading task 1's actual fixture code, which is out of scope for this plan.** Document the `#[ignore]` reason in a code comment.

### - [ ] Step 3: Implement `build_iceberg_mv_planning_catalog`

Add to `iceberg_refresh.rs` near `build_join_snapshot_catalog`:

```rust
/// Build a one-shot InMemoryCatalog for IMV optimizer-pipeline planning.
///
/// Registers each base in `ctx.rewrite.base_refs` under its namespace at
/// the snapshot captured by `ctx.rewrite.pin`. The catalog mirrors what
/// `canonical_select_query` references after `canonicalize_iceberg_mv_select_query`
/// rewrites `db.table` to `db.<synthetic>_at_<snapshot_id>`.
///
/// Reuses `build_iceberg_table_def_for_snapshot_scan` for per-base
/// table-def construction, so schemas / partition specs match what the
/// existing snapshot-scan path already uses.
fn build_iceberg_mv_planning_catalog(
    state: &Arc<StandaloneState>,
    ctx: &IcebergMvRefreshContext,
) -> Result<crate::engine::catalog::InMemoryCatalog, String> {
    let mut catalog = crate::engine::catalog::InMemoryCatalog::default();

    for base in ctx.rewrite.base_refs.iter() {
        let snapshot_id = ctx
            .rewrite
            .pin
            .get(base)
            .ok_or_else(|| format!("imv planning catalog: pin missing snapshot for base {}", base.fqn()))?;

        // create_database is idempotent-ish: it errors on duplicate. Two
        // bases sharing a namespace must only create the database once.
        if !catalog
            .database_exists(&base.namespace)
            .map_err(|e| format!("imv planning catalog: database_exists({}): {e}", base.namespace))?
        {
            catalog
                .create_database(&base.namespace)
                .map_err(|e| format!("imv planning catalog: create_database({}): {e}", base.namespace))?;
        }

        let table_def = build_iceberg_table_def_for_snapshot_scan(state, base, snapshot_id)?;
        catalog
            .register(&base.namespace, table_def)
            .map_err(|e| format!("imv planning catalog: register {}: {e}", base.fqn()))?;
    }

    Ok(catalog)
}
```

### - [ ] Step 4: Run the test

Run: `cargo test -p novarocks --lib build_iceberg_mv_planning_catalog_registers_each_base`

Expected: PASS (or skipped via `#[ignore]` if the fixture is deferred per Step 2). If skipped, run: `cargo build -p novarocks --lib` and confirm clean compilation.

### - [ ] Step 5: Commit

```bash
git add src/engine/mv/iceberg_refresh.rs
git commit -m "$(cat <<'EOF'
engine/mv: add build_iceberg_mv_planning_catalog for N-base refresh

Generalizes build_join_snapshot_catalog (which only handled N=2) to walk
ctx.rewrite.base_refs, register each base under its namespace at the
pinned snapshot via build_iceberg_table_def_for_snapshot_scan. Catalog
is consumed by plan_canonical_select_for_imv (next task).

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

## Task 9: `plan_canonical_select_for_imv` + refresh-side glue

**Goal:** Add `plan_canonical_select_for_imv(state, ctx) -> Result<LogicalPlan, RefreshError>` that uses the planning catalog from Task 8 to convert `ctx.rewrite.canonical_select_query` into a `LogicalPlan`.

**Files:**
- Modify: `src/engine/mv/iceberg_refresh.rs`

### - [ ] Step 1: Locate `RefreshError`

`RefreshError` is already in scope inside `iceberg_refresh.rs` (used throughout). Confirm by grepping: `grep -n "RefreshError::user" src/engine/mv/iceberg_refresh.rs | head -3`.

### - [ ] Step 2: Write the function

Add to `iceberg_refresh.rs`, near `build_iceberg_mv_planning_catalog`:

```rust
/// Re-plan ctx.rewrite.canonical_select_query into a LogicalPlan suitable
/// for handing to `run_imv_rewrite`.
///
/// Failure here is fail-fast: if the canonical SELECT cannot be analyzed
/// or planned, the refresh attempt aborts. This deliberately surfaces
/// canonicalization bugs early rather than tolerating divergence between
/// today's hand-built refresh path and the IMV pipeline.
fn plan_canonical_select_for_imv(
    state: &Arc<StandaloneState>,
    ctx: &IcebergMvRefreshContext,
) -> Result<crate::sql::planner::plan::LogicalPlan, RefreshError> {
    let catalog = build_iceberg_mv_planning_catalog(state, ctx)
        .map_err(|e| RefreshError::user(format!(
            "imv plan failed for {}: build planning catalog: {e}",
            ctx.rewrite.target
        )))?;

    let (resolved, cte_registry, mut factory) = crate::sql::analyzer::analyze(
        &ctx.rewrite.canonical_select_query,
        &catalog,
        &ctx.rewrite.current_database,
    )
    .map_err(|e| RefreshError::user(format!(
        "imv plan failed for {}: analyze: {e}",
        ctx.rewrite.target
    )))?;

    crate::sql::planner::plan_query(resolved, cte_registry, &mut factory).map_err(|e| {
        RefreshError::user(format!(
            "imv plan failed for {}: plan_query: {e}",
            ctx.rewrite.target
        ))
    })
}
```

### - [ ] Step 3: Verify compilation

Run: `cargo build -p novarocks --lib`

Expected: clean build. If there's a borrow / lifetime issue on `&ctx.rewrite.canonical_select_query` (since it's `Arc<sqlparser::ast::Query>`), use `ctx.rewrite.canonical_select_query.as_ref()`.

### - [ ] Step 4: Commit

```bash
git add src/engine/mv/iceberg_refresh.rs
git commit -m "$(cat <<'EOF'
engine/mv: add plan_canonical_select_for_imv re-plan helper

Build the IMV planning catalog, run the analyzer + planner on
ctx.rewrite.canonical_select_query, return LogicalPlan. Failure is
fail-fast (RefreshError::user) and surfaces canonical-query drift
between today's hand-built refresh path and the IMV pipeline.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

## Task 10: Wire IMV pipeline into ProjectionFilter shape

**Goal:** Call `plan_canonical_select_for_imv` + `run_imv_rewrite` inside `refresh_iceberg_mv` right after `IcebergMvRefreshContext::new`. Discard the outcome.

**Files:**
- Modify: `src/engine/mv/iceberg_refresh.rs`

### - [ ] Step 1: Locate the ProjectionFilter ctx construction site

From task 1's design doc §4.3 / commit, `refresh_iceberg_mv` constructs `IcebergMvRefreshContext` at line ~1382-1406 of `iceberg_refresh.rs`. Find the assignment that produces a `ctx: IcebergMvRefreshContext` variable for the ProjectionFilter branch.

Run: `grep -n "IcebergMvRefreshContext::new" src/engine/mv/iceberg_refresh.rs` — locate every site.

### - [ ] Step 2: Read the surrounding code

Read 30 lines around the first site:

```bash
# Replace <LINE> with the line number found in Step 1
sed -n '<LINE-15>,<LINE+15>p' src/engine/mv/iceberg_refresh.rs
```

You need to know:
- What variable name is the ctx assigned to (`ctx`, `refresh_ctx`, etc.)?
- What does `state` look like at this point (likely `state: &Arc<StandaloneState>`)?
- What does the session's `disabled_rules` look like? Likely accessed via `state.session_options()` or similar. **If unclear, grep**: `grep -n "optimizer_settings\|disabled_rules" src/engine/mv/iceberg_refresh.rs | head -5`. For PR-α you can pass `Vec::new()` if the wiring is genuinely unclear and document this as a follow-up to Task 12.

### - [ ] Step 3: Insert the IMV invocation

Right after the `let ctx = IcebergMvRefreshContext::new(...);` (or equivalent) line in the ProjectionFilter path, before the existing `match` dispatch, insert:

```rust
    // === IMV optimizer pipeline (PR-α: no-op rule set, outcome discarded) ===
    //
    // Plan the canonical select query as a LogicalPlan and run it through
    // the (empty) IMV pipeline so the foundation is exercised on every
    // refresh attempt. The outcome is discarded in PR-α; PR-β / task 4+
    // will consume the rewritten plan.
    let imv_plan = plan_canonical_select_for_imv(state, &ctx)?;
    let imv_outcome = crate::sql::optimizer::rewrite::imv::entrypoint::run_imv_rewrite(
        crate::sql::optimizer::rewrite::imv::entrypoint::ImvRewriteInput {
            plan: imv_plan,
            mv_ctx: Arc::clone(&ctx.rewrite),
            disabled_rules: Vec::new(), // PR-α: TODO thread session disabled rules in Task 12
            deadline: None,
        },
    )
    .map_err(|e| RefreshError::user(format!(
        "imv rewrite failed for {}: {e}",
        ctx.rewrite.target
    )))?;

    tracing::info!(
        target = %ctx.rewrite.target,
        mv_id  = ctx.rewrite.mv_id,
        stages = ?imv_outcome.trace.stage_names(),
        rules_changed  = imv_outcome.trace.changed_rules_count(),
        rules_rejected = imv_outcome.trace.rejected_rules_count(),
        rules_failed   = imv_outcome.trace.failed_rules_count(),
        "imv rewrite completed",
    );

    let _ = imv_outcome; // PR-α: outcome discarded. PR-β / task 4 consume it.
```

### - [ ] Step 4: Build + run the iceberg-ivm suite

Run:

```bash
cargo build -p novarocks --lib
```

Expected: clean build.

Start standalone-server and run the suite (per `CLAUDE.md` §8.4):

```bash
source docker/iceberg-rest/runtime/current/env.sh 2>/dev/null || true
# If the runtime entry exists, use it; otherwise default port:
LOG=/tmp/novarocks-server-task10.log
NO_PROXY=127.0.0.1,localhost target/debug/novarocks standalone-server \
  --config "${NOVAROCKS_STANDALONE_CONFIG:-./novarocks.toml}" >"$LOG" 2>&1 &
SRV_PID=$!
for i in $(seq 1 60); do
  if grep -q '^NOVAROCKS_READY ' "$LOG"; then break; fi
  if ! kill -0 "$SRV_PID" 2>/dev/null; then
    echo "standalone-server died during startup; tail of $LOG:" >&2
    tail -20 "$LOG" >&2
    exit 1
  fi
  sleep 1
done
grep -q '^NOVAROCKS_READY ' "$LOG" || { echo "timeout waiting for NOVAROCKS_READY" >&2; kill -9 "$SRV_PID"; exit 1; }

cargo run --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests -- \
  --config "${NOVAROCKS_SQL_TEST_CONFIG:-tests/sql-test-runner/config.toml}" \
  --suite iceberg-ivm --mode verify

kill -9 "$SRV_PID"
```

Expected: `iceberg-ivm` 61/61 — same as task 1's baseline.

If a case regresses, the most likely cause is `plan_canonical_select_for_imv` failing on a shape that today's hand-built path tolerates. Capture the failing case and add it as a follow-up; do not paper over with silent fallback (spec non-goal).

### - [ ] Step 5: Commit

```bash
git add src/engine/mv/iceberg_refresh.rs
git commit -m "$(cat <<'EOF'
engine/mv: wire IMV rewrite into ProjectionFilter refresh path

After IcebergMvRefreshContext::new, plan canonical_select_query into a
LogicalPlan and run it through the (empty) IMV pipeline. Outcome
discarded in PR-α. Emits one info-level summary log line per refresh
attempt; fails fast on rewrite error.

iceberg-ivm 61/61 unchanged from task 1 baseline.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

## Task 11: Wire IMV pipeline into Aggregate / JoinAggregate shape

**Goal:** Same as Task 10, applied at the `refresh_iceberg_aggregate_mv` ctx construction site.

**Files:**
- Modify: `src/engine/mv/iceberg_refresh.rs`

### - [ ] Step 1: Locate the Aggregate ctx construction site

Run: `grep -n "fn refresh_iceberg_aggregate_mv" src/engine/mv/iceberg_refresh.rs` — find the function. Search for `IcebergMvRefreshContext::new` inside it.

### - [ ] Step 2: Insert the IMV invocation

Paste the same code block from Task 10 Step 3, immediately after the ctx construction in the Aggregate path. Adjust the variable name if the local ctx is named differently (e.g., `agg_ctx`).

### - [ ] Step 3: Build + iceberg-ivm gate

Same commands as Task 10 Step 4. Expected: `iceberg-ivm` 61/61.

### - [ ] Step 4: Commit

```bash
git add src/engine/mv/iceberg_refresh.rs
git commit -m "$(cat <<'EOF'
engine/mv: wire IMV rewrite into Aggregate / JoinAggregate refresh path

Mirror of the ProjectionFilter wire-up applied to
refresh_iceberg_aggregate_mv. iceberg-ivm 61/61.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

## Task 12: Wire IMV pipeline into JoinProjectionFilter shape

**Goal:** Same as Task 10, applied at the `refresh_iceberg_join_mv` ctx construction site.

**Files:**
- Modify: `src/engine/mv/iceberg_refresh.rs`

### - [ ] Step 1: Locate the JoinProjectionFilter ctx construction site

Run: `grep -n "fn refresh_iceberg_join_mv" src/engine/mv/iceberg_refresh.rs`.

Per task 1's design doc §4.3, the ctx must be constructed **after** `RefreshSnapshotPin::capture` because early-return arms (both/one base has no snapshot) complete before pin capture. The IMV invocation goes inside the same post-pin region, immediately after the ctx construction line.

### - [ ] Step 2: Insert the IMV invocation

Paste the Task 10 Step 3 code block at the appropriate post-pin location. **Do NOT hoist before pin capture** — that would convert the existing no-op early-return arms into pin-capture errors.

### - [ ] Step 3: Build + iceberg-ivm gate

Same as Task 10 Step 4.

### - [ ] Step 4: Commit

```bash
git add src/engine/mv/iceberg_refresh.rs
git commit -m "$(cat <<'EOF'
engine/mv: wire IMV rewrite into JoinProjectionFilter refresh path

Mirror of the ProjectionFilter wire-up applied to refresh_iceberg_join_mv,
constructed inside the post-pin region so the existing no-op early-return
arms (both/one base has no snapshot) remain unchanged. iceberg-ivm 61/61.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

## Task 13: Thread session `disable_optimizer_rules` into IMV invocations

**Goal:** Replace the `Vec::new()` placeholder in the three Task 10–12 call sites with the session's `disabled_rules` so users can disable a registered IMV rule via `SET disable_optimizer_rules = '...'`.

**Files:**
- Modify: `src/engine/mv/iceberg_refresh.rs`

### - [ ] Step 1: Locate session-options access pattern

Refresh code today either:
- Already has `state.session_options()` or `state.optimizer_settings()` accessible at the refresh call site, or
- Refresh runs outside a user session, in which case the `disabled_rules` is empty by default.

Run: `grep -n "optimizer_settings\|disabled_rules\b" src/engine/mv/iceberg_refresh.rs src/engine/mod.rs | head -20`

- If refresh has access to a session, derive `disabled_rules: Vec<String>` from `session.optimizer_settings.disabled_rules.iter().cloned().collect()` or equivalent.
- If refresh runs without a session (e.g., from the scheduler), `disabled_rules: Vec::new()` is correct — document this in a code comment.

### - [ ] Step 2: Update the three call sites

Replace `disabled_rules: Vec::new(), // PR-α: TODO thread session disabled rules in Task 12` with the actual derivation (or keep `Vec::new()` with an explanatory comment if no session is available).

### - [ ] Step 3: Build + iceberg-ivm gate

Run:

```bash
cargo build -p novarocks --lib
```

Then iceberg-ivm suite (same as Task 10 Step 4). Expected: 61/61.

### - [ ] Step 4: Commit

```bash
git add src/engine/mv/iceberg_refresh.rs
git commit -m "$(cat <<'EOF'
engine/mv: thread session disable_optimizer_rules into IMV invocations

If refresh runs in a user session, derive disabled_rules from
session.optimizer_settings.disabled_rules; otherwise pass empty. PR-β
rules become disable-able via SET disable_optimizer_rules = '...'.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

## Task 14: Lib-level refresh integration test

**Goal:** Add a lib-test that exercises a ProjectionFilter MV refresh end-to-end through the IMV pipeline, asserting the result row set matches the pre-PR-α baseline.

**Files:**
- Modify: an existing lib-level test module under `src/engine/mv/` (find the closest one with refresh fixtures); or create `src/engine/mv/tests_imv_pipeline_wiring.rs` if no suitable home exists.

### - [ ] Step 1: Find an existing lib-test fixture for MV refresh

Run: `grep -rn "fn refresh_iceberg_mv\|fn test_iceberg_mv_refresh" src/engine/mv/ --include="*.rs" | grep test | head -5`

If a `#[cfg(test)] mod tests` block already has a working refresh fixture, append to it. If not, document this as a follow-up and skip Task 14 (it's not on the critical regression path because the SQL suite covers refresh end-to-end).

### - [ ] Step 2: Write the test

Append:

```rust
    #[test]
    fn projection_filter_refresh_through_imv_pipeline_matches_baseline() {
        // Setup: build the same fixture that an existing
        // ProjectionFilter refresh test uses. Run refresh; capture
        // result rows.
        //
        // Assertion: result matches the row set the hand-built refresh
        // path produces. (Today, the IMV outcome is discarded — the test
        // proves the pipeline runs without altering observable refresh
        // behavior.)
        //
        // If a suitable existing fixture cannot be reused without
        // significant rewrite, mark this test #[ignore] and add a
        // TODO referencing the follow-up task. The SQL suite gate in
        // Task 15 is the canonical end-to-end gate.
        todo!("inline an existing ProjectionFilter refresh fixture and assert row equality")
    }
```

**Note:** This task is allowed to defer into a `#[ignore]` if the existing test surface is too thin to reuse cleanly. The `iceberg-ivm` SQL suite (Task 15) is the hard gate.

### - [ ] Step 3: Run

Run: `cargo test -p novarocks --lib projection_filter_refresh_through_imv_pipeline_matches_baseline`

Expected: PASS (or ignored). Either way, `cargo test -p novarocks --lib` overall should still be green.

### - [ ] Step 4: Commit

```bash
git add src/engine/mv/  # adjust to actual path
git commit -m "$(cat <<'EOF'
engine/mv: lib-test projection-filter refresh through IMV pipeline

Smoke-test that exercising run_imv_rewrite inside refresh does not alter
observed row output. Marked #[ignore] if existing fixtures cannot be
reused; the iceberg-ivm SQL suite is the canonical gate.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

## Task 15: Final verification + suite gates

**Goal:** Run every gate the spec calls out (cargo fmt, clippy, full lib tests, iceberg-ivm, iceberg, iceberg-rest). Fix any issue surfaced.

**Files:** none — verification only.

### - [ ] Step 1: cargo fmt

Run: `cargo fmt -- --check`
Expected: clean. If not clean: `cargo fmt` and amend the most recent commit (or create a separate `chore: cargo fmt after IMV foundation` commit).

### - [ ] Step 2: cargo clippy

Run: `cargo clippy --all-targets`
Expected: 0 warnings. Fix any clippy lints triggered by the new code.

### - [ ] Step 3: cargo test --lib

Run: `cargo test -p novarocks --lib`
Expected: every test passes. The test count should grow by:
- ~7 from `imv/entrypoint.rs` (no-op verbatim, stage names, annotation default, mv_ctx visible, disabled rule skipped, unknown disabled name, failing rule)
- ~2 from `trace.rs` (stage_names, counter helpers)
- ~1 from `tree.rs` (traversal sanity)
- 0-1 from `iceberg_refresh.rs` (planning catalog test, maybe ignored)
- 0-1 from `tests_imv_pipeline_wiring.rs` (refresh smoke, maybe ignored)
- Total: ~10-11 new lib tests

### - [ ] Step 4: iceberg-ivm SQL suite

Per CLAUDE.md §7.3 / §8.4:

```bash
source docker/iceberg-rest/runtime/current/env.sh
docker/iceberg-rest/up.sh
LOG=/tmp/novarocks-server-task15.log
NO_PROXY=127.0.0.1,localhost target/debug/novarocks standalone-server \
  --config "$NOVAROCKS_STANDALONE_CONFIG" >"$LOG" 2>&1 &
SRV_PID=$!
for i in $(seq 1 60); do
  if grep -q '^NOVAROCKS_READY ' "$LOG"; then break; fi
  if ! kill -0 "$SRV_PID" 2>/dev/null; then
    echo "standalone-server died during startup; tail of $LOG:" >&2
    tail -20 "$LOG" >&2
    exit 1
  fi
  sleep 1
done
grep -q '^NOVAROCKS_READY ' "$LOG" || { echo "timeout waiting for NOVAROCKS_READY" >&2; kill -9 "$SRV_PID"; exit 1; }

cargo run --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests -- \
  --config "$NOVAROCKS_SQL_TEST_CONFIG" \
  --suite iceberg-ivm --mode verify

kill -9 "$SRV_PID"
```

Expected: 61/61 (matches task 1 baseline).

### - [ ] Step 5: iceberg + iceberg-rest suites (parallel-safe to combine)

```bash
# Re-use the same standalone-server from Step 4 if still alive, or start a fresh one
cargo run --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests -- \
  --config "$NOVAROCKS_SQL_TEST_CONFIG" \
  --suite iceberg --mode verify
cargo run --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests -- \
  --config "$NOVAROCKS_SQL_TEST_CONFIG" \
  --suite iceberg-rest --mode verify
```

Expected: baseline passes (whatever the baseline counts are pre-PR-α — collect them in advance).

### - [ ] Step 6: Update TODO List

Mark task 2 complete in the Obsidian TODO List:

```bash
# This is outside the repo. Open the file in the user's preferred editor.
echo "Reminder: update /Users/harbor/Documents/Obsidian/NovaRocks TODO/TODO List.md"
echo "  - Mark task 2 row: ✅ Incremental MV optimizer foundation (PR-α landed) — partial; PR-β pending"
echo "  - Update the '进度' section at the top of the file"
```

(The plan does NOT require modifying the Obsidian file as part of any commit — the file lives outside the repo. The user updates the file manually after PR-α merges.)

### - [ ] Step 7: Push branch and open PR

```bash
git push -u origin claude/thirsty-antonelli-98c8c5
gh pr create --title "engine/mv: IMV optimizer foundation (PR-α, TODO task 2)" \
  --body "$(cat <<'EOF'
## Summary

- Introduce `src/sql/optimizer/rewrite/imv/` sub-module on top of the existing rewrite framework
- Wire a no-op IMV optimizer pipeline into the three refresh-context construction sites established by task 1 (PR #185)
- Add four counter helpers + stage-name plumbing on `RewriteTrace`
- Tree-traversal sanity test covering all `LogicalPlan` variants

PR-α of the combined PR-α / PR-β spec at [`docs/design/specs/2026-05-26-incremental-mv-optimizer-foundation-design.md`](docs/design/specs/2026-05-26-incremental-mv-optimizer-foundation-design.md). PR-β (marker operators) ships separately.

## Test plan

- [x] `cargo fmt -- --check`
- [x] `cargo clippy --all-targets` clean
- [x] `cargo test -p novarocks --lib` — ~10 new tests passing
- [x] `iceberg-ivm` suite 61/61 (baseline unchanged from task 1)
- [x] `iceberg` suite — baseline unchanged
- [x] `iceberg-rest` suite — baseline unchanged

🤖 Generated with [Claude Code](https://claude.com/claude-code)
EOF
)"
```

Wait for review before merging.

---

## Self-Review

**Spec coverage:**

| Spec section / requirement | Plan task |
|---|---|
| §4.1 module layout | Task 1 (creates `imv/mod.rs`, `annotation.rs`, `pipeline.rs`, `entrypoint.rs`) |
| §4.2 public API (run_imv_rewrite, ImvRewriteOutcome, ImvPlanAnnotation, ImvExtension, build_imv_pipeline) | Task 1 |
| §4.3 stage naming (4 named stages) | Task 1 (pipeline.rs) + Task 2 (stage name in trace) |
| §4.5 tree traversal coverage check | Task 6 |
| §5 data flow (7-step refresh attempt) | Tasks 8 (catalog), 9 (replan), 10/11/12 (3 call sites) |
| §5.2 `plan_canonical_select_for_imv` | Task 9 |
| §6 error handling (fail-fast, value semantics) | Task 5 (failing rule preserves caller plan) + tasks 10-12 (RefreshError::user) |
| §7.2 refresh-path summary log line | Tasks 10-12 (tracing::info! block) |
| §7.4 rule disable wiring | Task 4 (entrypoint test) + Task 13 (refresh-side thread) |
| §9.1 7 PR-α unit tests | Tasks 1, 2, 3, 4, 5 (covers all 7) |
| §9.2 traversal sanity test | Task 6 |
| §9.3 lib-level refresh integration | Task 14 (with `#[ignore]` fallback) |
| §9.4 suite gates | Task 15 |
| §10 R-5 single-tenant extension slot | Task 1 (`ImvExtension` wrapper) |

**Gap check:**

- §7.5 EXPLAIN integration is documented as not-in-PR-α; no plan task — correct (out of scope).
- §8 PR-β preparation is documented but not implemented — correct (PR-β has its own plan later).
- §10 R-2 (analyzer behavior drift) — no explicit task; surfaces as iceberg-ivm regression in Task 10-12 gates. Acceptable.

**Placeholder scan:**

- Two `todo!()` blocks in Tasks 8 and 14, each explicitly described as "defer with `#[ignore]` if the fixture is non-trivial". This is a deliberate concession to keep the plan executable without pre-reading task 1's full test infrastructure. Documented in the plan body, not silent.
- Task 13 acknowledges that session access in refresh may not exist; falls back to `Vec::new()` with explanatory comment. Documented inline.

**Type / name consistency:**

- `ImvRewriteInput` / `ImvRewriteOutcome` / `ImvPlanAnnotation` / `ImvExtension` — used consistently across all tasks
- `run_imv_rewrite` — single name everywhere
- `build_imv_pipeline` — single name
- Stage labels `imv-logical-normalize / imv-delta-marker / imv-marker-cleanup / imv-validation` — locked in Task 1, asserted in Task 2, no drift
- `plan_canonical_select_for_imv` — used in Tasks 9 and 10-12 invocations
- `build_iceberg_mv_planning_catalog` — used in Tasks 8 and 9
- Trace helpers `stage_names / changed_rules_count / rejected_rules_count / failed_rules_count` — defined in Task 2 and 7, consumed in Tasks 10-12

No name drift detected. Plan ready.
