//! Entrypoint for the IMV rewrite pipeline. See
//! docs/superpowers/specs/2026-05-26-incremental-mv-optimizer-foundation-design.md.

use std::sync::Arc;
use std::time::Instant;

use crate::engine::mv::refresh_context::IcebergMvRewriteContext;
use crate::sql::optimizer::rewrite::context::RewriteContext;
use crate::sql::optimizer::rewrite::imv::annotation::{ImvExtension, ImvPlanAnnotation};
use crate::sql::optimizer::rewrite::imv::pipeline::build_imv_pipeline;
use crate::sql::optimizer::rewrite::trace::RewriteTrace;
use crate::sql::planner::plan::LogicalPlan;

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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sql::optimizer::rewrite::context::RewriteContext;
    use crate::sql::optimizer::rewrite::phase::RewritePhase;
    use crate::sql::optimizer::rewrite::result::RewriteResult;
    use crate::sql::optimizer::rewrite::rule::{LogicalRewriteRule, RewriteTraversal};
    use crate::sql::planner::plan::{LogicalPlan, ValuesNode};
    use std::sync::atomic::{AtomicBool, Ordering};

    fn dummy_mv_ctx() -> Arc<IcebergMvRewriteContext> {
        crate::engine::mv::refresh_context::tests_support::dummy_rewrite_context()
    }

    fn empty_values_plan() -> LogicalPlan {
        LogicalPlan::Values(ValuesNode {
            rows: vec![],
            columns: vec![],
        })
    }

    // ── Task-3 helpers ──────────────────────────────────────────────────────

    /// Test-only rule that asserts ImvExtension is reachable from the
    /// RewriteContext. Captures whether the observed target fqn matched into
    /// an AtomicBool for assertion outside the rule.
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
            let t = &ext.mv_ctx.target;
            let fqn = format!("{}.{}.{}", t.catalog, t.namespace, t.table);
            if fqn == self.expected_target {
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
        assert_eq!(
            format!("{:?}", outcome.annotation),
            format!("{:?}", ImvPlanAnnotation::default()),
        );
    }

    #[test]
    fn imv_rewrite_context_visible_through_extension() {
        use crate::sql::optimizer::rewrite::pipeline::{RewritePipeline, RewriteStage};

        let mv_ctx = dummy_mv_ctx();
        let t = &mv_ctx.target;
        let expected_target = format!("{}.{}.{}", t.catalog, t.namespace, t.table);
        let saw_mv_ctx = Arc::new(AtomicBool::new(false));

        let pipeline = RewritePipeline::from_stages(vec![RewriteStage::new(
            "imv-logical-normalize",
            RewritePhase::LogicalNormalize,
            vec![Box::new(AssertMvCtxVisibleRule {
                saw_mv_ctx: Arc::clone(&saw_mv_ctx),
                expected_target,
            })],
        )]);

        let mut ctx_rw = RewriteContext::for_mv_refresh(Vec::<String>::new());
        ctx_rw.set_extension::<ImvExtension>(ImvExtension {
            mv_ctx,
            annotation: ImvPlanAnnotation::default(),
        });

        let _ = pipeline.rewrite(empty_values_plan(), &mut ctx_rw).unwrap();

        assert!(saw_mv_ctx.load(Ordering::SeqCst));
    }

    // ── Pre-existing tests ──────────────────────────────────────────────────

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

    #[test]
    fn empty_pipeline_traces_all_four_stage_names() {
        let outcome = run_imv_rewrite(ImvRewriteInput {
            plan: empty_values_plan(),
            mv_ctx: dummy_mv_ctx(),
            disabled_rules: Vec::new(),
            deadline: None,
        })
        .expect("no-op IMV pipeline must succeed");

        assert_eq!(
            outcome.trace.stage_names(),
            vec![
                "imv-logical-normalize",
                "imv-delta-marker",
                "imv-marker-cleanup",
                "imv-validation",
            ]
        );
    }
}
