//! IMV rewrite pipeline construction.
//!
//! Stages run in order: logical normalize, delta marker, branch union, union
//! delta, aggregate state, delta pushdown, scan binding, action propagation,
//! apply key, partition derivation, marker cleanup, validation. Each stage's
//! name is part of the trace contract and is asserted in pipeline tests.

use crate::sql::optimizer::rewrite::phase::RewritePhase;
use crate::sql::optimizer::rewrite::pipeline::{RewritePipeline, RewriteStage};
use crate::sql::optimizer::rewrite::rule::LogicalRewriteRule;
use crate::sql::planner::imv_rewrite::action_column::ActionColumnValidationRule;
use crate::sql::planner::imv_rewrite::action_propagation::{
    InjectActionColumnRule, PropagateActionColumnRule,
};
use crate::sql::planner::imv_rewrite::aggregate_rewrite::RewriteAggregateStateRule;
use crate::sql::planner::imv_rewrite::apply_key::InjectApplyKeyProjectRule;
use crate::sql::planner::imv_rewrite::branch_union::RewriteBranchUnionRule;
use crate::sql::planner::imv_rewrite::delta_pushdown::PushDeltaThroughUnaryRule;
use crate::sql::planner::imv_rewrite::join_delta::RewriteJoinDeltaRule;
use crate::sql::planner::imv_rewrite::marker::{UnresolvedMarkerCheckRule, WrapRootInImvDeltaRule};
use crate::sql::planner::imv_rewrite::partition_derivation::DerivePartitionSpecRule;
use crate::sql::planner::imv_rewrite::row_id_column::InjectRowIdRule;
use crate::sql::planner::imv_rewrite::scan_binding::BindIcebergScanRule;
use crate::sql::planner::imv_rewrite::union_delta::{
    RewriteTopLevelUnionDeltaRule, RewriteUnionAggregateDeltaRule,
};

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
            vec![Box::new(WrapRootInImvDeltaRule::new()) as Box<dyn LogicalRewriteRule>],
        ),
        RewriteStage::new(
            "imv-branch-union",
            RewritePhase::StructuralRewrite,
            vec![Box::new(RewriteBranchUnionRule) as Box<dyn LogicalRewriteRule>],
        ),
        RewriteStage::new(
            "imv-union-delta",
            RewritePhase::StructuralRewrite,
            vec![
                Box::new(RewriteUnionAggregateDeltaRule) as Box<dyn LogicalRewriteRule>,
                Box::new(RewriteTopLevelUnionDeltaRule) as Box<dyn LogicalRewriteRule>,
            ],
        ),
        RewriteStage::new(
            "imv-aggregate-state",
            RewritePhase::StructuralRewrite,
            vec![Box::new(RewriteAggregateStateRule) as Box<dyn LogicalRewriteRule>],
        ),
        RewriteStage::new(
            "imv-delta-pushdown",
            RewritePhase::StructuralRewrite,
            vec![
                Box::new(PushDeltaThroughUnaryRule) as Box<dyn LogicalRewriteRule>,
                Box::new(RewriteJoinDeltaRule) as Box<dyn LogicalRewriteRule>,
            ],
        ),
        RewriteStage::new(
            "imv-scan-binding",
            RewritePhase::SemanticRewrite,
            vec![Box::new(BindIcebergScanRule) as Box<dyn LogicalRewriteRule>],
        ),
        RewriteStage::new(
            "imv-action-propagation",
            RewritePhase::SemanticRewrite,
            vec![
                Box::new(InjectActionColumnRule) as Box<dyn LogicalRewriteRule>,
                Box::new(InjectRowIdRule),
                Box::new(PropagateActionColumnRule),
            ],
        ),
        RewriteStage::new(
            "imv-apply-key",
            RewritePhase::SemanticRewrite,
            vec![Box::new(InjectApplyKeyProjectRule::new()) as Box<dyn LogicalRewriteRule>],
        ),
        RewriteStage::new(
            "imv-partition-derivation",
            RewritePhase::SemanticRewrite,
            vec![Box::new(DerivePartitionSpecRule) as Box<dyn LogicalRewriteRule>],
        ),
        RewriteStage::new(
            "imv-marker-cleanup",
            RewritePhase::SemanticRewrite,
            Vec::new(),
        ),
        RewriteStage::new(
            "imv-validation",
            RewritePhase::Validation,
            vec![
                Box::new(UnresolvedMarkerCheckRule) as Box<dyn LogicalRewriteRule>,
                Box::new(ActionColumnValidationRule::new()),
            ],
        ),
    ])
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pipeline_has_apply_key_stage_after_action_propagation() {
        let p = build_imv_pipeline();
        let names = p.stage_names();
        let ap = names
            .iter()
            .position(|n| *n == "imv-action-propagation")
            .unwrap();
        let ak = names
            .iter()
            .position(|n| *n == "imv-apply-key")
            .expect("imv-apply-key stage must exist");
        let val = names.iter().position(|n| *n == "imv-validation").unwrap();
        assert!(ap < ak && ak < val, "stage order: {names:?}");
    }

    #[test]
    fn pipeline_runs_partition_derivation_after_apply_key_before_validation() {
        let p = build_imv_pipeline();
        let names = p.stage_names();
        let ak = names.iter().position(|n| *n == "imv-apply-key").unwrap();
        let pd = names
            .iter()
            .position(|n| *n == "imv-partition-derivation")
            .expect("imv-partition-derivation stage must exist");
        let val = names.iter().position(|n| *n == "imv-validation").unwrap();
        assert!(ak < pd && pd < val, "stage order: {names:?}");
        assert!(
            p.rule_names().iter().any(|n| *n == "DerivePartitionSpec"),
            "DerivePartitionSpec must be registered"
        );
    }

    #[test]
    fn pipeline_runs_join_delta_inside_pushdown_after_aggregate_state() {
        let p = build_imv_pipeline();
        let names = p.stage_names();
        assert!(
            !names.iter().any(|n| *n == "imv-join-delta"),
            "imv-join-delta stage must be removed: {names:?}"
        );
        let union = names
            .iter()
            .position(|n| *n == "imv-union-delta")
            .expect("union delta stage must exist");
        let branch_union = names
            .iter()
            .position(|n| *n == "imv-branch-union")
            .expect("branch union stage must exist");
        let agg = names
            .iter()
            .position(|n| *n == "imv-aggregate-state")
            .expect("aggregate state stage must exist");
        let pushdown = names
            .iter()
            .position(|n| *n == "imv-delta-pushdown")
            .expect("delta pushdown stage must exist");

        assert!(branch_union < pushdown, "stage order: {names:?}");
        assert!(union < agg, "stage order: {names:?}");
        assert!(agg < pushdown, "stage order: {names:?}");
        assert!(
            p.rule_names().iter().any(|n| *n == "RewriteJoinDelta"),
            "RewriteJoinDelta must run inside imv-delta-pushdown"
        );
    }
}
