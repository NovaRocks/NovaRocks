//! IMV rewrite pipeline construction.
//!
//! Stages run in order: logical normalize, delta marker, scan binding, marker
//! cleanup, validation. Each stage's name is part of the trace contract and is
//! asserted in pipeline tests.

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
            "imv-scan-binding",
            RewritePhase::SemanticRewrite,
            vec![Box::new(BindIcebergScanRule) as Box<dyn LogicalRewriteRule>],
        ),
        RewriteStage::new(
            "imv-marker-cleanup",
            RewritePhase::SemanticRewrite,
            Vec::new(),
        ),
        RewriteStage::new(
            "imv-validation",
            RewritePhase::Validation,
            vec![Box::new(UnresolvedMarkerCheckRule) as Box<dyn LogicalRewriteRule>],
        ),
    ])
}
