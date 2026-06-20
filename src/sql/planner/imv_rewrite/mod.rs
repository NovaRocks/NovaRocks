//! IMV-specific logical rewrite substrate. See
//! docs/design/specs/2026-05-26-incremental-mv-optimizer-foundation-design.md.
//!
//! The module owns the IMV rewrite entrypoint, pipeline rules, annotations,
//! and temporary bridges used by rules that still rely on `LogicalPlanNode`
//! helpers during the OptExpr cutover.

/// Temporary internal bridge for IMV rule helpers that still operate on
/// `LogicalPlanNode`. This is not the optimizer main path or an engine
/// boundary.
pub(crate) fn opt_expr_to_plan(
    expr: crate::sql::optimizer::opt_expr::OptExpr,
    ctx: &crate::sql::optimizer::rewrite::context::RewriteContext,
) -> crate::sql::planner::plan::LogicalPlanNode {
    let arena = ctx.scalar_arena();
    crate::sql::planner::optimizer_bridge::plan::opt_expr_to_logical_plan(expr, &arena.borrow())
}

/// Intermediate result type used by closures passed to [`bridge_apply_result`].
/// Mirrors [`RewriteResult`] but holds a [`LogicalPlanNode`] in the `Changed`
/// variant so closures can work with plan-level types directly.
pub(crate) enum PlanRewriteResult {
    Unchanged,
    Changed(crate::sql::planner::plan::LogicalPlanNode),
    Rejected(crate::sql::optimizer::rewrite::result::RewriteDiagnostic),
}

/// Temporary internal bridge for IMV rule helpers that still return
/// `LogicalPlanNode`. This is not the optimizer main path or an engine
/// boundary.
///
/// The closure returns [`PlanRewriteResult`] so it can work entirely with
/// `LogicalPlanNode` types. The wrapper converts `PlanRewriteResult::Changed`
/// back to `RewriteResult::Changed(OptExpr)`.
pub(crate) fn bridge_apply_result<F>(
    expr: crate::sql::optimizer::opt_expr::OptExpr,
    ctx: &crate::sql::optimizer::rewrite::context::RewriteContext,
    f: F,
) -> Result<crate::sql::optimizer::rewrite::result::RewriteResult, String>
where
    F: FnOnce(
        crate::sql::planner::plan::LogicalPlanNode,
        &crate::sql::optimizer::rewrite::context::RewriteContext,
    ) -> Result<PlanRewriteResult, String>,
{
    let plan = opt_expr_to_plan(expr, ctx);
    let result = f(plan, ctx)?;
    let arena = ctx.scalar_arena();
    let converted = match result {
        PlanRewriteResult::Changed(plan_out) => {
            let opt_out = crate::sql::planner::optimizer_bridge::plan::logical_plan_to_opt_expr(
                &plan_out,
                &mut arena.borrow_mut(),
            );
            crate::sql::optimizer::rewrite::result::RewriteResult::Changed(opt_out)
        }
        PlanRewriteResult::Unchanged => {
            crate::sql::optimizer::rewrite::result::RewriteResult::Unchanged
        }
        PlanRewriteResult::Rejected(diag) => {
            crate::sql::optimizer::rewrite::result::RewriteResult::Rejected(diag)
        }
    };
    Ok(converted)
}

pub(crate) mod action_column;
pub(crate) mod action_propagation;
pub(crate) mod aggregate_rewrite;
pub(crate) mod annotation;
pub(crate) mod apply_key;
pub(crate) mod branch_union;
pub(crate) mod delta_pushdown;
pub(crate) mod entrypoint;
pub(crate) mod join_delta;
pub(crate) mod join_delta_shape;
pub(crate) mod marker;
pub(crate) mod partition_derivation;
pub(crate) mod pipeline;
pub(crate) mod row_id_column;
pub(crate) mod scan_binding;
pub(crate) mod target_state;
pub(crate) mod union_delta;
