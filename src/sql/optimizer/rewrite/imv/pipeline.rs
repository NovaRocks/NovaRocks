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
