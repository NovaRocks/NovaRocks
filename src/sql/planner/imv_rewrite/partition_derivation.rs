//! Plan-time partition derivation: resolve the contract-level
//! `PartitionDerivationSpec` and record the outcome on `ImvPlanAnnotation`.
//!
//! P1 scope (umbrella spec §5.1 / D5): matches aggregate-state-merge shapes
//! only; the annotation is observability + P2 input — live pruning still
//! flows from plan-time manifest derivation, so this rule never changes the
//! plan and never fails the rewrite.

use crate::engine::mv::partition::resolve_partition_derivation_spec;
use crate::sql::optimizer::operator::Operator;
use crate::sql::optimizer::opt_expr::OptExpr;
use crate::sql::optimizer::rewrite::context::RewriteContext;
use crate::sql::optimizer::rewrite::phase::RewritePhase;
use crate::sql::optimizer::rewrite::result::RewriteResult;
use crate::sql::optimizer::rewrite::rule::{LogicalRewriteRule, RewriteTraversal};
use crate::sql::planner::imv_rewrite::annotation::{
    ImvExtension, ImvPartitionAnnotation, ImvPlanAnnotation,
};

pub(crate) struct DerivePartitionSpecRule;

impl LogicalRewriteRule for DerivePartitionSpecRule {
    fn name(&self) -> &'static str {
        "DerivePartitionSpec"
    }

    fn phase(&self) -> RewritePhase {
        RewritePhase::SemanticRewrite
    }

    fn traversal(&self) -> RewriteTraversal {
        RewriteTraversal::TopDown
    }

    fn matches(&self, expr: &OptExpr, ctx: &RewriteContext) -> bool {
        if !matches!(&expr.op, Operator::LogicalAggregateStateMerge(_)) {
            return false;
        }
        ctx.extension::<ImvExtension>()
            .is_some_and(|ext| ext.annotation.partition.is_none())
    }

    fn apply(&self, _expr: OptExpr, ctx: &mut RewriteContext) -> Result<RewriteResult, String> {
        let ext = ctx
            .extension::<ImvExtension>()
            .ok_or("DerivePartitionSpec requires ImvExtension")?
            .clone();

        let outcome = match resolve_partition_derivation_spec(&ext.mv_ctx.schema_contract) {
            Ok(None) => ImvPartitionAnnotation::Unpartitioned,
            Ok(Some(spec)) => ImvPartitionAnnotation::Derivable { specs: vec![spec] },
            Err(err) => ImvPartitionAnnotation::NotDerivable {
                reason: err.to_string(),
            },
        };

        tracing::info!(
            event = "iceberg_mv.partition_derivation",
            mv_id = ext.mv_ctx.mv_id,
            outcome = match &outcome {
                ImvPartitionAnnotation::Unpartitioned => "unpartitioned",
                ImvPartitionAnnotation::Derivable { .. } => "derivable",
                ImvPartitionAnnotation::NotDerivable { .. } => "not_derivable",
            },
            reason = outcome_reason(&outcome),
            "IMV partition derivation spec resolved"
        );

        ctx.set_extension::<ImvExtension>(ImvExtension {
            annotation: ImvPlanAnnotation {
                partition: Some(outcome),
            },
            ..ext
        });
        Ok(RewriteResult::Unchanged)
    }
}

fn outcome_reason(outcome: &ImvPartitionAnnotation) -> &str {
    match outcome {
        ImvPartitionAnnotation::NotDerivable { reason } => reason.as_str(),
        _ => "",
    }
}
