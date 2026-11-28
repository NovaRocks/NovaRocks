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

//! IMV rewrite pipeline construction.
//!
//! Stages run in order: logical normalize, delta marker, branch union, union
//! delta, aggregate state, delta pushdown, scan binding, action propagation,
//! change-stream descriptor, partition derivation,
//! marker cleanup, validation.
//! Each stage's name is part of the trace contract and is asserted in pipeline
//! tests.

use crate::optimizer::rewrite::phase::RewritePhase;
use crate::optimizer::rewrite::pipeline::{RewritePipeline, RewriteStage};
use crate::optimizer::rewrite::rule::LogicalRewriteRule;
use crate::planner::imv_rewrite::action_column::ActionColumnValidationRule;
use crate::planner::imv_rewrite::action_propagation::{
    InjectActionColumnRule, PropagateActionColumnRule,
};
use crate::planner::imv_rewrite::aggregate_rewrite::RewriteAggregateStateRule;
use crate::planner::imv_rewrite::branch_union::RewriteBranchUnionRule;
use crate::planner::imv_rewrite::change_stream::{
    BuildChangeStreamDescriptorRule, ValidateChangeStreamDescriptorRule,
};
use crate::planner::imv_rewrite::delta_pushdown::PushDeltaThroughUnaryRule;
use crate::planner::imv_rewrite::join_delta::{RewriteJoinDeltaRule, UnsupportedJoinKindCheckRule};
use crate::planner::imv_rewrite::marker::{UnresolvedMarkerCheckRule, WrapRootInImvDeltaRule};
use crate::planner::imv_rewrite::partition_derivation::DerivePartitionSpecRule;
use crate::planner::imv_rewrite::row_id_column::InjectRowIdRule;
use crate::planner::imv_rewrite::scan_binding::BindIcebergScanRule;
use crate::planner::imv_rewrite::union_delta::{
    RewriteTopLevelUnionDeltaRule, RewriteUnionAggregateDeltaRule,
};

#[cfg(test)]
pub(crate) fn build_imv_pipeline() -> RewritePipeline {
    build_imv_pipeline_for_target(false)
}

pub(super) fn build_imv_pipeline_for_target(aggregate: bool) -> RewritePipeline {
    let mut action_rules: Vec<Box<dyn LogicalRewriteRule>> = vec![Box::new(InjectActionColumnRule)];
    if aggregate {
        action_rules.push(Box::new(InjectRowIdRule));
    }
    action_rules.push(Box::new(PropagateActionColumnRule));
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
                Box::new(RewriteTopLevelUnionDeltaRule) as Box<dyn LogicalRewriteRule>,
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
            action_rules,
        ),
        RewriteStage::new(
            "imv-change-stream-descriptor",
            RewritePhase::SemanticRewrite,
            vec![Box::new(BuildChangeStreamDescriptorRule::new()) as Box<dyn LogicalRewriteRule>],
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
                Box::new(ValidateChangeStreamDescriptorRule::new()) as Box<dyn LogicalRewriteRule>,
                Box::new(UnresolvedMarkerCheckRule) as Box<dyn LogicalRewriteRule>,
                Box::new(ActionColumnValidationRule::new()),
                Box::new(UnsupportedJoinKindCheckRule) as Box<dyn LogicalRewriteRule>,
            ],
        ),
    ])
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pipeline_validates_change_stream_descriptor_before_action_columns() {
        let p = build_imv_pipeline();
        let rules = p.rule_names();
        let descriptor = rules
            .iter()
            .position(|n| *n == "ValidateChangeStreamDescriptor")
            .expect("ValidateChangeStreamDescriptor must be registered");
        let action = rules
            .iter()
            .position(|n| *n == "ActionColumnValidation")
            .expect("ActionColumnValidation must be registered");
        assert!(descriptor < action, "rule order: {rules:?}");
    }

    #[test]
    fn pipeline_runs_join_delta_inside_pushdown_after_aggregate_state() {
        let p = build_imv_pipeline();
        let names = p.stage_names();
        assert!(
            !names.contains(&"imv-join-delta"),
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
            p.rule_names().contains(&"RewriteJoinDelta"),
            "RewriteJoinDelta must run inside imv-delta-pushdown"
        );
    }
}
