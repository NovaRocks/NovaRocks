//! IMV rewrite pipeline construction.
//!
//! Stages run in order: logical normalize, delta marker, delta pushdown, scan
//! binding, action propagation, marker cleanup, validation. Each stage's name
//! is part of the trace contract and is asserted in pipeline tests.

use crate::sql::optimizer::rewrite::imv::action_column::ActionColumnValidationRule;
use crate::sql::optimizer::rewrite::imv::action_propagation::{
    InjectActionColumnRule, PropagateActionColumnRule,
};
use crate::sql::optimizer::rewrite::imv::delta_pushdown::PushDeltaThroughUnaryRule;
use crate::sql::optimizer::rewrite::imv::marker::{
    UnresolvedMarkerCheckRule, WrapRootInImvDeltaRule,
};
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
                Box::new(PropagateActionColumnRule),
            ],
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
