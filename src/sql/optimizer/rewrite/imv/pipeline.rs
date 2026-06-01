//! IMV rewrite pipeline construction.
//!
//! Stages run in order: logical normalize, delta marker, join delta, aggregate
//! state, delta pushdown, scan binding, action propagation, marker cleanup,
//! validation. Each stage's name is part of the trace contract and is asserted
//! in pipeline tests.

use crate::sql::optimizer::rewrite::imv::action_column::ActionColumnValidationRule;
use crate::sql::optimizer::rewrite::imv::action_propagation::{
    InjectActionColumnRule, PropagateActionColumnRule,
};
use crate::sql::optimizer::rewrite::imv::aggregate_rewrite::RewriteAggregateStateRule;
use crate::sql::optimizer::rewrite::imv::apply_key::InjectApplyKeyProjectRule;
use crate::sql::optimizer::rewrite::imv::delta_pushdown::PushDeltaThroughUnaryRule;
use crate::sql::optimizer::rewrite::imv::join_delta::RewriteJoinAggregateDeltaRule;
use crate::sql::optimizer::rewrite::imv::marker::{
    UnresolvedMarkerCheckRule, WrapRootInImvDeltaRule,
};
use crate::sql::optimizer::rewrite::imv::row_id_column::InjectRowIdRule;
use crate::sql::optimizer::rewrite::imv::scan_binding::BindIcebergScanRule;
use crate::sql::optimizer::rewrite::phase::RewritePhase;
use crate::sql::optimizer::rewrite::pipeline::{RewritePipeline, RewriteStage};
use crate::sql::optimizer::rewrite::rule::LogicalRewriteRule;

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
            "imv-join-delta",
            RewritePhase::StructuralRewrite,
            vec![Box::new(RewriteJoinAggregateDeltaRule) as Box<dyn LogicalRewriteRule>],
        ),
        RewriteStage::new(
            "imv-aggregate-state",
            RewritePhase::StructuralRewrite,
            vec![Box::new(RewriteAggregateStateRule) as Box<dyn LogicalRewriteRule>],
        ),
        RewriteStage::new(
            "imv-delta-pushdown",
            RewritePhase::StructuralRewrite,
            vec![Box::new(PushDeltaThroughUnaryRule) as Box<dyn LogicalRewriteRule>],
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
    fn pipeline_runs_join_and_aggregate_rewrite_before_generic_delta_pushdown() {
        let p = build_imv_pipeline();
        let names = p.stage_names();
        let join = names
            .iter()
            .position(|n| *n == "imv-join-delta")
            .expect("join delta stage must exist");
        let agg = names
            .iter()
            .position(|n| *n == "imv-aggregate-state")
            .expect("aggregate state stage must exist");
        let pushdown = names
            .iter()
            .position(|n| *n == "imv-delta-pushdown")
            .expect("delta pushdown stage must exist");

        assert!(join < agg, "stage order: {names:?}");
        assert!(agg < pushdown, "stage order: {names:?}");
    }
}
