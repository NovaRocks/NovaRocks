// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

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
    expr: crate::optimizer::opt_expr::OptExpr,
    ctx: &crate::optimizer::rewrite::context::RewriteContext,
) -> crate::planner::logical::LogicalPlanNode {
    let arena = ctx.scalar_arena();
    crate::planner::optimizer_bridge::logical::to_logical_plan(expr, &arena.borrow())
}

/// Intermediate result type used by closures passed to [`bridge_apply_result`].
/// Mirrors [`RewriteResult`] but holds a [`LogicalPlanNode`] in the `Changed`
/// variant so closures can work with plan-level types directly.
#[expect(
    clippy::large_enum_variant,
    reason = "Rewrite results retain the logical plan inline because this internal handoff is short-lived and allocation-free."
)]
#[allow(
    dead_code,
    reason = "Rejected rewrite diagnostics remain part of the internal bridge contract for rule implementations enabled by other targets."
)]
pub(crate) enum PlanRewriteResult {
    Unchanged,
    Changed(crate::planner::logical::LogicalPlanNode),
    Rejected(crate::optimizer::rewrite::result::RewriteDiagnostic),
}

/// Temporary internal bridge for IMV rule helpers that still return
/// `LogicalPlanNode`. This is not the optimizer main path or an engine
/// boundary.
///
/// The closure returns [`PlanRewriteResult`] so it can work entirely with
/// `LogicalPlanNode` types. The wrapper converts `PlanRewriteResult::Changed`
/// back to `RewriteResult::Changed(OptExpr)`.
pub(crate) fn bridge_apply_result<F>(
    expr: crate::optimizer::opt_expr::OptExpr,
    ctx: &crate::optimizer::rewrite::context::RewriteContext,
    f: F,
) -> Result<crate::optimizer::rewrite::result::RewriteResult, crate::compiler::SqlCompileError>
where
    F: FnOnce(
        crate::planner::logical::LogicalPlanNode,
        &crate::optimizer::rewrite::context::RewriteContext,
    ) -> Result<PlanRewriteResult, String>,
{
    bridge_apply_result_typed(expr, ctx, |plan, ctx| {
        f(plan, ctx).map_err(crate::compiler::SqlCompileError::Compilation)
    })
}

/// Typed source-control failures pass through without entering the ordinary
/// legacy helper error facade.
pub(crate) fn bridge_apply_result_typed<F>(
    expr: crate::optimizer::opt_expr::OptExpr,
    ctx: &crate::optimizer::rewrite::context::RewriteContext,
    f: F,
) -> Result<crate::optimizer::rewrite::result::RewriteResult, crate::compiler::SqlCompileError>
where
    F: FnOnce(
        crate::planner::logical::LogicalPlanNode,
        &crate::optimizer::rewrite::context::RewriteContext,
    ) -> Result<PlanRewriteResult, crate::compiler::SqlCompileError>,
{
    let plan = opt_expr_to_plan(expr, ctx);
    let result = f(plan, ctx)?;
    let arena = ctx.scalar_arena();
    let converted = match result {
        PlanRewriteResult::Changed(plan_out) => {
            let opt_out = crate::planner::optimizer_bridge::logical::try_to_optimizer_expr(
                &plan_out,
                &mut arena.borrow_mut(),
                &ctx.control_view(),
            )?;
            crate::optimizer::rewrite::result::RewriteResult::Changed(opt_out)
        }
        PlanRewriteResult::Unchanged => crate::optimizer::rewrite::result::RewriteResult::Unchanged,
        PlanRewriteResult::Rejected(diag) => {
            crate::optimizer::rewrite::result::RewriteResult::Rejected(diag)
        }
    };
    Ok(converted)
}

/// Mutable-context variant of [`bridge_apply_result`] for IMV rules that need
/// to update [`RewriteContext`] extensions while still using
/// `LogicalPlanNode` helpers.
pub(crate) fn bridge_apply_result_mut_typed<F>(
    expr: crate::optimizer::opt_expr::OptExpr,
    ctx: &mut crate::optimizer::rewrite::context::RewriteContext,
    f: F,
) -> Result<crate::optimizer::rewrite::result::RewriteResult, crate::compiler::SqlCompileError>
where
    F: FnOnce(
        crate::planner::logical::LogicalPlanNode,
        &mut crate::optimizer::rewrite::context::RewriteContext,
    ) -> Result<PlanRewriteResult, crate::compiler::SqlCompileError>,
{
    let plan = opt_expr_to_plan(expr, ctx);
    let result = f(plan, ctx)?;
    let arena = ctx.scalar_arena();
    let converted = match result {
        PlanRewriteResult::Changed(plan_out) => {
            let opt_out = crate::planner::optimizer_bridge::logical::try_to_optimizer_expr(
                &plan_out,
                &mut arena.borrow_mut(),
                &ctx.control_view(),
            )?;
            crate::optimizer::rewrite::result::RewriteResult::Changed(opt_out)
        }
        PlanRewriteResult::Unchanged => crate::optimizer::rewrite::result::RewriteResult::Unchanged,
        PlanRewriteResult::Rejected(diag) => {
            crate::optimizer::rewrite::result::RewriteResult::Rejected(diag)
        }
    };
    Ok(converted)
}

pub(crate) mod action_column;
pub(crate) mod action_propagation;
pub(crate) mod aggregate_rewrite;
pub(crate) mod annotation;
pub(crate) mod branch_union;
pub(crate) mod change_stream;
pub(crate) mod column_alloc;
pub(crate) mod content_apply;
pub(crate) mod delta_pushdown;
pub(crate) mod entrypoint;
pub(crate) mod join_delta;
pub(crate) mod join_delta_shape;
pub(crate) mod marker;
pub(crate) mod partition_derivation;
pub(crate) mod pipeline;
pub(crate) mod position_metadata;
pub(crate) mod row_id_column;
pub(crate) mod scan_binding;
pub(crate) mod target_state;
pub(crate) mod union_delta;
