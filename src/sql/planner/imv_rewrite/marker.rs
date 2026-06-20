//! Logical marker operators for Incremental MV (IMV) rewrite. See
//! docs/design/specs/2026-05-26-incremental-mv-optimizer-foundation-design.md §8.
//!
//! These markers must never reach physical lowering. The `imv-delta-marker`
//! stage of the IMV pipeline wraps the root; the `imv-validation` stage
//! rejects any plan that still carries a marker afterwards.

use std::sync::atomic::{AtomicBool, Ordering};

use crate::sql::optimizer::operator::{ImvDeltaOp, Operator};
use crate::sql::optimizer::opt_expr::OptExpr;
use crate::sql::optimizer::rewrite::context::RewriteContext;
use crate::sql::optimizer::rewrite::phase::RewritePhase;
use crate::sql::optimizer::rewrite::result::RewriteResult;
use crate::sql::optimizer::rewrite::rule::{LogicalRewriteRule, RewriteTraversal};
use crate::sql::planner::imv_rewrite::opt_expr_to_plan;
use crate::sql::planner::plan::{LogicalPlanNode, PlanNodeKind};

/// Snapshot window descriptor used by `LogicalImvVersionNode`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct ImvVersionRef {
    pub(crate) role: crate::sql::planner::imv_rewrite::scan_binding::ImvVersionRole,
}

impl ImvVersionRef {
    pub(crate) fn from_snapshot() -> Self {
        Self {
            role: crate::sql::planner::imv_rewrite::scan_binding::ImvVersionRole::From,
        }
    }

    pub(crate) fn to_snapshot() -> Self {
        Self {
            role: crate::sql::planner::imv_rewrite::scan_binding::ImvVersionRole::To,
        }
    }
}

impl Default for ImvVersionRef {
    fn default() -> Self {
        Self::to_snapshot()
    }
}

/// Wraps the root of an IMV refresh plan in `ImvDelta { is_root: true }`.
///
/// # Instance-scope one-shot state
///
/// `wrapped` is stored on the **rule instance**, not in `RewriteContext`.
/// Once the flag is set it stays set for the lifetime of this object.
/// Callers **must** construct a fresh `WrapRootInImvDeltaRule` (and
/// therefore a fresh `RewritePipeline`) for every independent `rewrite()`
/// invocation.  The production path satisfies this contract because
/// `run_imv_rewrite()` calls `build_imv_pipeline()` which allocates a new
/// pipeline and a new rule instance on every call.  Reusing the same
/// pipeline across multiple `rewrite()` calls is incorrect: the second call
/// will silently skip wrapping because `wrapped` is already `true`.
pub(crate) struct WrapRootInImvDeltaRule {
    wrapped: AtomicBool,
}

impl WrapRootInImvDeltaRule {
    pub(crate) fn new() -> Self {
        Self {
            wrapped: AtomicBool::new(false),
        }
    }
}

impl LogicalRewriteRule for WrapRootInImvDeltaRule {
    fn name(&self) -> &'static str {
        "WrapRootInImvDelta"
    }

    fn phase(&self) -> RewritePhase {
        RewritePhase::StructuralRewrite
    }

    fn traversal(&self) -> RewriteTraversal {
        RewriteTraversal::TopDown
    }

    fn matches(&self, expr: &OptExpr, _ctx: &RewriteContext) -> bool {
        if self.wrapped.load(Ordering::SeqCst) {
            return false;
        }
        // Plan was already wrapped before the pipeline ran (e.g. re-entry
        // on a previously wrapped plan): record the fact and skip.
        // This store-then-return-false pattern is load-bearing: the side effect
        // ensures that when TopDown traversal descends into the children of the
        // existing ImvDelta wrapper, the matches() call on those descendants will
        // return false, preventing apply() from running on them. TopDown still visits
        // all nodes; the wrapped flag just short-circuits matches() to prevent
        // double-wrapping.
        if matches!(
            &expr.op,
            Operator::LogicalImvDelta(ImvDeltaOp { is_root: true, .. })
        ) {
            self.wrapped.store(true, Ordering::SeqCst);
            return false;
        }
        true
    }

    fn apply(&self, expr: OptExpr, _ctx: &mut RewriteContext) -> Result<RewriteResult, String> {
        self.wrapped.store(true, Ordering::SeqCst);
        Ok(RewriteResult::Changed(OptExpr::new(
            Operator::LogicalImvDelta(ImvDeltaOp {
                is_root: true,
                action_column: None,
                branch_scope: None,
            }),
            vec![expr],
        )))
    }
}

use crate::sql::optimizer::rewrite::result::RewriteDiagnostic;

/// Validation-stage rule. Rejects any plan that still carries an IMV
/// marker (`ImvDelta` or `ImvVersion`) by the time the pipeline reaches
/// the `imv-validation` stage. The for-MV-refresh policy is FailFast,
/// so rejection becomes `Err(message)` for `run_imv_rewrite`'s caller.
pub(crate) struct UnresolvedMarkerCheckRule;

impl LogicalRewriteRule for UnresolvedMarkerCheckRule {
    fn name(&self) -> &'static str {
        "UnresolvedMarkerCheck"
    }

    fn phase(&self) -> RewritePhase {
        RewritePhase::Validation
    }

    fn traversal(&self) -> RewriteTraversal {
        // TopDown fires at the root first. Once apply() rejects, the
        // framework aborts the phase (FailFast policy), so children are
        // never visited.
        RewriteTraversal::TopDown
    }

    fn matches(&self, expr: &OptExpr, ctx: &RewriteContext) -> bool {
        let plan = opt_expr_to_plan(expr.clone(), ctx);
        plan_contains_imv_marker(&plan)
    }

    fn apply(&self, expr: OptExpr, ctx: &mut RewriteContext) -> Result<RewriteResult, String> {
        let plan = opt_expr_to_plan(expr, ctx);
        let markers = collect_marker_kinds(&plan);
        Ok(RewriteResult::Rejected(RewriteDiagnostic::rejected(
            "UnresolvedMarkerCheck",
            format!("IVM rewrite failed to resolve incremental markers: {markers:?}"),
        )))
    }
}

/// Returns true if `plan` contains any `ImvDelta` or `ImvVersion` node at
/// any depth. The Validation stage uses this to detect unresolved markers.
pub(crate) fn plan_contains_imv_marker(plan: &LogicalPlanNode) -> bool {
    match &plan.kind {
        PlanNodeKind::ImvDelta(_) | PlanNodeKind::ImvVersion(_) => true,
        _ => plan.children.iter().any(plan_contains_imv_marker),
    }
}

/// Returns the distinct kinds of marker present in `plan`, in stable
/// order. Used by the Validation stage's error message.
pub(crate) fn collect_marker_kinds(plan: &LogicalPlanNode) -> Vec<&'static str> {
    let mut found: Vec<&'static str> = Vec::new();
    collect_into(plan, &mut found);
    found.sort();
    found.dedup();
    found
}

fn collect_into(plan: &LogicalPlanNode, found: &mut Vec<&'static str>) {
    match &plan.kind {
        PlanNodeKind::ImvDelta(_) => {
            found.push("ImvDelta");
            for child in &plan.children {
                collect_into(child, found);
            }
        }
        PlanNodeKind::ImvVersion(_) => {
            found.push("ImvVersion");
            for child in &plan.children {
                collect_into(child, found);
            }
        }
        _ => {
            for child in &plan.children {
                collect_into(child, found);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sql::optimizer::convert::{logical_plan_to_opt_expr, opt_expr_to_logical_plan};
    use crate::sql::optimizer::scalar::ScalarArena;
    use crate::sql::planner::plan::*;
    use crate::sql::planner::plan::{LogicalPlanNode, LogicalValuesNode, PlanNodeKind};

    #[test]
    fn wrap_rule_wraps_plain_root_once() {
        use crate::sql::optimizer::rewrite::context::RewriteContext;
        use crate::sql::optimizer::rewrite::phase::RewritePhase;
        use crate::sql::optimizer::rewrite::pipeline::{RewritePipeline, RewriteStage};

        let plan = empty_values_plan();
        let mut ctx = RewriteContext::for_mv_refresh(Vec::<String>::new());

        let pipeline = RewritePipeline::from_stages(vec![RewriteStage::new(
            "imv-delta-marker",
            RewritePhase::StructuralRewrite,
            vec![Box::new(WrapRootInImvDeltaRule::new())],
        )]);

        let arena = std::rc::Rc::new(std::cell::RefCell::new(ScalarArena::new()));
        ctx.set_scalar_arena(std::rc::Rc::clone(&arena));
        let opt_in = logical_plan_to_opt_expr(&plan, &mut arena.borrow_mut());
        let opt_out = pipeline.rewrite(opt_in, &mut ctx).unwrap();
        let out = opt_expr_to_logical_plan(opt_out, &arena.borrow());

        let PlanNodeKind::ImvDelta(delta) = &out.kind else {
            panic!("expected ImvDelta at root");
        };
        assert!(delta.is_root);
        assert!(delta.action_column.is_none());
        assert!(matches!(&out.children[0].kind, PlanNodeKind::Values(_)));
    }

    #[test]
    fn wrap_rule_is_idempotent_on_already_wrapped_plan() {
        use crate::sql::optimizer::rewrite::context::RewriteContext;
        use crate::sql::optimizer::rewrite::phase::RewritePhase;
        use crate::sql::optimizer::rewrite::pipeline::{RewritePipeline, RewriteStage};

        let already = LogicalPlanNode::new(
            PlanNodeKind::ImvDelta(LogicalImvDeltaNode {
                is_root: true,
                action_column: None,
                branch_scope: None,
            }),
            vec![empty_values_plan()],
            None,
        );
        let before = format!("{already:?}");

        let mut ctx = RewriteContext::for_mv_refresh(Vec::<String>::new());
        let pipeline = RewritePipeline::from_stages(vec![RewriteStage::new(
            "imv-delta-marker",
            RewritePhase::StructuralRewrite,
            vec![Box::new(WrapRootInImvDeltaRule::new())],
        )]);

        let arena = std::rc::Rc::new(std::cell::RefCell::new(ScalarArena::new()));
        ctx.set_scalar_arena(std::rc::Rc::clone(&arena));
        let opt_in = logical_plan_to_opt_expr(&already, &mut arena.borrow_mut());
        let opt_out = pipeline.rewrite(opt_in, &mut ctx).unwrap();
        let out = opt_expr_to_logical_plan(opt_out, &arena.borrow());
        assert_eq!(format!("{out:?}"), before, "wrap must not double-wrap");
    }

    fn empty_values_plan() -> LogicalPlanNode {
        LogicalPlanNode::new(
            PlanNodeKind::Values(LogicalValuesNode {
                rows: vec![],
                columns: vec![],
            }),
            vec![],
            None,
        )
    }

    /// Two independent invocations each using their **own** fresh rule/pipeline
    /// must both succeed.  This documents the required usage contract: callers
    /// (i.e. `build_imv_pipeline` / `run_imv_rewrite`) must never reuse the
    /// same `WrapRootInImvDeltaRule` instance across multiple `rewrite()` calls
    /// because the `wrapped` flag is per-instance and is never reset.
    #[test]
    fn two_fresh_pipelines_each_wrap_independently() {
        use crate::sql::optimizer::rewrite::context::RewriteContext;
        use crate::sql::optimizer::rewrite::phase::RewritePhase;
        use crate::sql::optimizer::rewrite::pipeline::{RewritePipeline, RewriteStage};

        fn make_pipeline() -> RewritePipeline {
            RewritePipeline::from_stages(vec![RewriteStage::new(
                "imv-delta-marker",
                RewritePhase::StructuralRewrite,
                vec![Box::new(WrapRootInImvDeltaRule::new())],
            )])
        }

        // First invocation — fresh rule instance
        let plan1 = empty_values_plan();
        let mut ctx1 = RewriteContext::for_mv_refresh(Vec::<String>::new());
        let arena1 = std::rc::Rc::new(std::cell::RefCell::new(ScalarArena::new()));
        ctx1.set_scalar_arena(std::rc::Rc::clone(&arena1));
        let opt_in1 = logical_plan_to_opt_expr(&plan1, &mut arena1.borrow_mut());
        let opt_out1 = make_pipeline().rewrite(opt_in1, &mut ctx1).unwrap();
        let out1 = opt_expr_to_logical_plan(opt_out1, &arena1.borrow());
        assert!(
            matches!(
                &out1.kind,
                PlanNodeKind::ImvDelta(LogicalImvDeltaNode { is_root: true, .. })
            ),
            "first fresh pipeline must wrap the root"
        );

        // Second invocation — another fresh rule instance (simulates a second
        // `run_imv_rewrite()` call the way `build_imv_pipeline()` works in
        // production).
        let plan2 = empty_values_plan();
        let mut ctx2 = RewriteContext::for_mv_refresh(Vec::<String>::new());
        let arena2 = std::rc::Rc::new(std::cell::RefCell::new(ScalarArena::new()));
        ctx2.set_scalar_arena(std::rc::Rc::clone(&arena2));
        let opt_in2 = logical_plan_to_opt_expr(&plan2, &mut arena2.borrow_mut());
        let opt_out2 = make_pipeline().rewrite(opt_in2, &mut ctx2).unwrap();
        let out2 = opt_expr_to_logical_plan(opt_out2, &arena2.borrow());
        assert!(
            matches!(
                &out2.kind,
                PlanNodeKind::ImvDelta(LogicalImvDeltaNode { is_root: true, .. })
            ),
            "second fresh pipeline must also wrap the root independently"
        );
    }

    #[test]
    fn imv_delta_node_constructs_with_none_action_column() {
        let node = LogicalImvDeltaNode {
            is_root: true,
            action_column: None,
            branch_scope: None,
        };
        assert!(node.is_root);
        assert!(node.action_column.is_none());
    }

    #[test]
    fn imv_version_node_constructs_with_default_ref() {
        let node = LogicalImvVersionNode {
            version_ref: ImvVersionRef::default(),
        };
        assert_eq!(node.version_ref, ImvVersionRef::default());
    }

    #[test]
    fn plan_contains_imv_marker_false_for_plain_plan() {
        let plan = empty_values_plan();
        assert!(!plan_contains_imv_marker(&plan));
    }

    #[test]
    fn plan_contains_imv_marker_true_for_root_delta() {
        let plan = LogicalPlanNode::new(
            PlanNodeKind::ImvDelta(LogicalImvDeltaNode {
                is_root: true,
                action_column: None,
                branch_scope: None,
            }),
            vec![empty_values_plan()],
            None,
        );
        assert!(plan_contains_imv_marker(&plan));
    }

    #[test]
    fn plan_contains_imv_marker_true_for_nested_version() {
        use crate::sql::planner::plan::LogicalLimitNode;
        // Build Limit(Limit(ImvVersion(Values))). The marker is
        // deeply nested; the helper must recurse.
        let nested = LogicalPlanNode::new(
            PlanNodeKind::ImvVersion(LogicalImvVersionNode {
                version_ref: ImvVersionRef::default(),
            }),
            vec![empty_values_plan()],
            None,
        );
        let inner = LogicalPlanNode::new(
            PlanNodeKind::Limit(LogicalLimitNode {
                limit: None,
                offset: None,
            }),
            vec![nested],
            None,
        );
        let outer = LogicalPlanNode::new(
            PlanNodeKind::Limit(LogicalLimitNode {
                limit: None,
                offset: None,
            }),
            vec![inner],
            None,
        );
        assert!(plan_contains_imv_marker(&outer));
    }

    #[test]
    fn collect_marker_kinds_reports_each_distinct_kind() {
        let delta = LogicalPlanNode::new(
            PlanNodeKind::ImvDelta(LogicalImvDeltaNode {
                is_root: true,
                action_column: None,
                branch_scope: None,
            }),
            vec![LogicalPlanNode::new(
                PlanNodeKind::ImvVersion(LogicalImvVersionNode {
                    version_ref: ImvVersionRef::default(),
                }),
                vec![empty_values_plan()],
                None,
            )],
            None,
        );
        assert_eq!(collect_marker_kinds(&delta), vec!["ImvDelta", "ImvVersion"]);
    }

    #[test]
    fn marker_unresolved_yields_rejected_outcome() {
        use crate::sql::optimizer::rewrite::context::RewriteContext;
        use crate::sql::optimizer::rewrite::phase::RewritePhase;
        use crate::sql::optimizer::rewrite::pipeline::{RewritePipeline, RewriteStage};
        use crate::sql::optimizer::rewrite::trace::RewriteTraceEvent;

        // imv-delta-marker wraps; imv-validation rejects. Build a minimal
        // two-stage pipeline that mirrors the production stage names.
        let pipeline = RewritePipeline::from_stages(vec![
            RewriteStage::new(
                "imv-delta-marker",
                RewritePhase::StructuralRewrite,
                vec![Box::new(WrapRootInImvDeltaRule::new())],
            ),
            RewriteStage::new(
                "imv-validation",
                RewritePhase::Validation,
                vec![Box::new(UnresolvedMarkerCheckRule)],
            ),
        ]);

        let mut ctx = RewriteContext::for_mv_refresh(Vec::<String>::new());
        let arena = std::rc::Rc::new(std::cell::RefCell::new(ScalarArena::new()));
        ctx.set_scalar_arena(std::rc::Rc::clone(&arena));
        let plan = empty_values_plan();
        let opt_in = logical_plan_to_opt_expr(&plan, &mut arena.borrow_mut());
        let err = pipeline
            .rewrite(opt_in, &mut ctx)
            .expect_err("Validation must reject the wrapped-but-unconsumed plan");
        assert!(
            err.starts_with("IVM rewrite failed to resolve incremental markers:"),
            "unexpected error message: {err}"
        );
        assert!(err.contains("\"ImvDelta\""), "kind list missing: {err}");

        // Trace must record the rejection under the rule's name.
        assert!(ctx.trace().events().iter().any(|e| matches!(
            e,
            RewriteTraceEvent::RuleRejected { rule, .. }
                if *rule == "UnresolvedMarkerCheck"
        )));
    }

    #[test]
    fn validation_passes_when_no_marker_present() {
        use crate::sql::optimizer::rewrite::context::RewriteContext;
        use crate::sql::optimizer::rewrite::phase::RewritePhase;
        use crate::sql::optimizer::rewrite::pipeline::{RewritePipeline, RewriteStage};

        // Validation alone, no wrap rule. Plain plan → no marker → pass.
        let pipeline = RewritePipeline::from_stages(vec![RewriteStage::new(
            "imv-validation",
            RewritePhase::Validation,
            vec![Box::new(UnresolvedMarkerCheckRule)],
        )]);

        let mut ctx = RewriteContext::for_mv_refresh(Vec::<String>::new());
        let arena = std::rc::Rc::new(std::cell::RefCell::new(ScalarArena::new()));
        ctx.set_scalar_arena(std::rc::Rc::clone(&arena));
        let plan = empty_values_plan();
        let opt_in = logical_plan_to_opt_expr(&plan, &mut arena.borrow_mut());
        let opt_out = pipeline
            .rewrite(opt_in, &mut ctx)
            .expect("plain plan must pass validation");
        let out = opt_expr_to_logical_plan(opt_out, &arena.borrow());
        assert!(matches!(&out.kind, PlanNodeKind::Values(_)));
    }

    #[test]
    fn regular_query_pipeline_does_not_produce_markers() {
        use crate::sql::optimizer::rewrite::context::RewriteContext;
        use crate::sql::optimizer::rewrite::registry::query_rewrite_pipeline;
        use std::collections::HashMap;

        // Exercise the real query rewrite pipeline (the one used by the
        // optimizer before CBO) to ensure it never introduces IMV markers
        // on a plain non-IMV plan.
        let pipeline = query_rewrite_pipeline(&HashMap::new());
        let mut ctx = RewriteContext::for_query(Vec::<String>::new());
        let arena = std::rc::Rc::new(std::cell::RefCell::new(ScalarArena::new()));
        ctx.set_scalar_arena(std::rc::Rc::clone(&arena));
        let plan = empty_values_plan();
        let opt_in = logical_plan_to_opt_expr(&plan, &mut arena.borrow_mut());
        let opt_out = pipeline
            .rewrite(opt_in, &mut ctx)
            .expect("query pipeline must not error on plain plan");
        let out = opt_expr_to_logical_plan(opt_out, &arena.borrow());
        assert!(
            !plan_contains_imv_marker(&out),
            "non-IMV pipeline must not emit markers, got {out:?}"
        );
    }
}
