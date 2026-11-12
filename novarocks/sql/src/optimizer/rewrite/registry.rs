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

use crate::optimizer::rewrite::phase::RewritePhase;
use crate::optimizer::rewrite::pipeline::{RewritePipeline, RewriteStage};
use crate::optimizer::rewrite::rules;

#[allow(
    dead_code,
    reason = "Retained for staged SQL planner migration consumers and test helpers."
)]
pub(crate) fn default_rewrite_phases() -> Vec<RewritePhase> {
    vec![
        RewritePhase::LogicalNormalize,
        RewritePhase::StructuralRewrite,
        RewritePhase::SemanticRewrite,
        RewritePhase::Validation,
    ]
}

pub(crate) fn query_rewrite_pipeline() -> RewritePipeline {
    RewritePipeline::from_stages(vec![
        RewriteStage::new(
            "UnionDistinctNormalize",
            RewritePhase::LogicalNormalize,
            rules::union_distinct_normalize_rules(),
        ),
        // Constant folding must precede every structural rewrite: predicate
        // pushdown's static-predicate extraction only sees a bare literal if
        // `Cast(Literal)` has already collapsed by then.
        RewriteStage::new(
            "ConstantFold",
            RewritePhase::LogicalNormalize,
            rules::fold_constant_rules(),
        ),
        RewriteStage::new(
            "SubqueryRewrite",
            RewritePhase::StructuralRewrite,
            rules::subquery::subquery_rewrite_rules(),
        ),
        RewriteStage::new(
            "PredicatePushdownPreJoin",
            RewritePhase::StructuralRewrite,
            rules::predicate_pushdown_rules(),
        ),
        RewriteStage::new(
            "PredicatePushdownPostJoin",
            RewritePhase::StructuralRewrite,
            {
                let mut rules = rules::predicate_pushdown_rules();
                rules.push(Box::new(
                    rules::derive_join_not_null::DeriveJoinNotNullPredicate,
                ));
                rules
            },
        ),
        RewriteStage::new(
            "PredicateMoveAround",
            RewritePhase::StructuralRewrite,
            rules::predicate_move_around_rules(),
        ),
        RewriteStage::new(
            "PredicatePushdownAfterMoveAround",
            RewritePhase::StructuralRewrite,
            rules::predicate_pushdown_rules(),
        ),
        RewriteStage::new(
            "VariantPathPushdown",
            RewritePhase::StructuralRewrite,
            rules::variant_path_pushdown_rules(),
        ),
        RewriteStage::new(
            "RankingWindowPredicatePushdown",
            RewritePhase::StructuralRewrite,
            rules::ranking_window_predicate_pushdown::ranking_window_predicate_pushdown_rules(),
        ),
        RewriteStage::new(
            "AggregatePushdown",
            RewritePhase::StructuralRewrite,
            rules::aggregate_pushdown::aggregate_pushdown_rules(),
        ),
        RewriteStage::new(
            "TagRequiredColumns",
            RewritePhase::StructuralRewrite,
            vec![Box::new(
                crate::optimizer::rewrite::required_columns::TagRequiredColumns,
            )],
        ),
        RewriteStage::new(
            "ColumnPruning",
            RewritePhase::StructuralRewrite,
            rules::column_pruning_rules(),
        ),
    ])
}

#[allow(
    dead_code,
    reason = "Retained for staged SQL planner migration consumers and test helpers."
)]
pub(crate) fn mv_rewrite_pipeline() -> RewritePipeline {
    RewritePipeline::new(default_rewrite_phases(), Vec::new())
}

#[allow(
    dead_code,
    reason = "Retained for staged SQL planner migration consumers and test helpers."
)]
pub(crate) fn is_known_rewrite_rule_name(name: &str) -> bool {
    let query_pipeline = query_rewrite_pipeline();
    let mv_pipeline = mv_rewrite_pipeline();

    query_pipeline
        .rule_names()
        .into_iter()
        .chain(mv_pipeline.rule_names())
        .any(|rule_name| rule_name == name)
}

#[cfg(test)]
mod tests {
    use super::{
        default_rewrite_phases, is_known_rewrite_rule_name, mv_rewrite_pipeline,
        query_rewrite_pipeline,
    };

    use crate::optimizer::operator::{Operator, ValuesOp};
    use crate::optimizer::opt_expr::OptExpr;
    use crate::optimizer::rewrite::context::RewriteContext;
    use crate::optimizer::rewrite::phase::RewritePhase;
    use crate::optimizer::rewrite::trace::RewriteTraceEvent;

    #[derive(Debug, PartialEq, Eq)]
    struct TestMvExtension {
        marker: String,
    }

    #[test]
    fn query_pipeline_uses_expected_stage_order_and_rules() {
        let pipeline = query_rewrite_pipeline();

        assert_eq!(
            pipeline.stage_names(),
            vec![
                "UnionDistinctNormalize",
                "ConstantFold",
                "SubqueryRewrite",
                "PredicatePushdownPreJoin",
                "PredicatePushdownPostJoin",
                "PredicateMoveAround",
                "PredicatePushdownAfterMoveAround",
                "VariantPathPushdown",
                "RankingWindowPredicatePushdown",
                "AggregatePushdown",
                "TagRequiredColumns",
                "ColumnPruning",
            ]
        );

        let mut names = pipeline.rule_names();
        names.sort();

        assert_eq!(
            names,
            vec![
                "AggregatePushdown",
                "ApplyException",
                "ApplyToWindow",
                "DeriveJoinNotNullPredicate",
                "EliminateUniqueAggregate",
                "ExistentialApplyToJoin",
                "FoldConstant",
                "JoinPredicateMoveAround",
                "PruneAggregateColumns",
                "PruneCTEAnchorColumns",
                "PruneCTEConsumeColumns",
                "PruneCTEProduceColumns",
                "PruneExceptColumns",
                "PruneFilterColumns",
                "PruneIntersectColumns",
                "PruneJoinColumns",
                "PruneLimitColumns",
                "PruneProjectColumns",
                "PruneRepeatColumns",
                "PruneScanColumns",
                "PruneSortColumns",
                "PruneSystemTableColumns",
                "PruneTableFunctionColumns",
                "PruneUkFkJoin",
                "PruneUnionColumns",
                "PruneValuesColumns",
                "PruneWindowColumns",
                "PushDownApplyAggFilter",
                "PushDownApplyFilter",
                "PushDownPredicateAggregate",
                "PushDownPredicateAggregate",
                "PushDownPredicateAggregate",
                "PushDownPredicateJoin",
                "PushDownPredicateJoin",
                "PushDownPredicateJoin",
                "PushDownPredicateProject",
                "PushDownPredicateProject",
                "PushDownPredicateProject",
                "PushDownPredicateScan",
                "PushDownPredicateScan",
                "PushDownPredicateScan",
                "PushSemiAntiRightOnlyCondition",
                "PushSemiAntiRightOnlyCondition",
                "PushSemiAntiRightOnlyCondition",
                "QuantifiedApplyToJoin",
                "RankingWindowPredicatePushdown",
                "ScalarApplyToJoin",
                "TagRequiredColumns",
                "UnionDistinctToAggregate",
                "VariantPathPushdown",
            ]
        );
    }

    #[test]
    fn mv_pipeline_is_empty_and_noop_in_phase_one() {
        let pipeline = mv_rewrite_pipeline();
        assert!(pipeline.rule_names().is_empty());

        let plan = empty_values_plan();
        let before = format!("{plan:?}");
        let mut ctx = RewriteContext::for_mv_refresh(Vec::<String>::new());
        ctx.set_extension(TestMvExtension {
            marker: "mv-refresh".to_string(),
        });

        let rewritten = pipeline.rewrite(plan, &mut ctx).unwrap();

        assert_eq!(format!("{rewritten:?}"), before);
        assert_eq!(
            ctx.extension::<TestMvExtension>(),
            Some(&TestMvExtension {
                marker: "mv-refresh".to_string(),
            })
        );
        assert_default_phase_trace(&ctx);
    }

    #[test]
    fn rewrite_registry_recognizes_migrated_query_rules() {
        assert!(!is_known_rewrite_rule_name(""));
        assert!(is_known_rewrite_rule_name("AggregatePushdown"));
        assert!(is_known_rewrite_rule_name("PushDownPredicateProject"));
        assert!(is_known_rewrite_rule_name("TagRequiredColumns"));
        assert!(is_known_rewrite_rule_name("VariantPathPushdown"));
        assert!(!is_known_rewrite_rule_name("PushFilterThroughProject"));
        assert!(is_known_rewrite_rule_name("DeriveJoinNotNullPredicate"));
        assert!(is_known_rewrite_rule_name("JoinPredicateMoveAround"));
        assert!(is_known_rewrite_rule_name("ApplyException"));
        // M1b scalar decorrelation rules (Task 4).
        assert!(is_known_rewrite_rule_name("PushDownApplyAggFilter"));
        assert!(is_known_rewrite_rule_name("PushDownApplyFilter"));
        // M2 window decorrelation rule.
        assert!(is_known_rewrite_rule_name("ApplyToWindow"));
        assert!(is_known_rewrite_rule_name("ScalarApplyToJoin"));
        assert!(is_known_rewrite_rule_name("ExistentialApplyToJoin"));
        assert!(is_known_rewrite_rule_name("QuantifiedApplyToJoin"));
        // Ranking window predicate pushdown skeleton (Tasks 4.1+4.2).
        assert!(is_known_rewrite_rule_name("RankingWindowPredicatePushdown"));
        assert!(is_known_rewrite_rule_name("UnionDistinctToAggregate"));
        // Constant folding (LogicalNormalize, before any structural rewrite).
        assert!(is_known_rewrite_rule_name("FoldConstant"));
    }

    fn assert_default_phase_trace(ctx: &RewriteContext) {
        assert_eq!(
            default_rewrite_phases(),
            vec![
                RewritePhase::LogicalNormalize,
                RewritePhase::StructuralRewrite,
                RewritePhase::SemanticRewrite,
                RewritePhase::Validation,
            ]
        );
        assert_eq!(
            ctx.trace().events(),
            &[
                RewriteTraceEvent::PhaseStarted {
                    phase: RewritePhase::LogicalNormalize,
                    stage: "LogicalNormalize",
                },
                RewriteTraceEvent::IterationStarted {
                    phase: RewritePhase::LogicalNormalize,
                    iteration: 1,
                },
                RewriteTraceEvent::PhaseEnded {
                    phase: RewritePhase::LogicalNormalize,
                },
                RewriteTraceEvent::PhaseStarted {
                    phase: RewritePhase::StructuralRewrite,
                    stage: "StructuralRewrite",
                },
                RewriteTraceEvent::IterationStarted {
                    phase: RewritePhase::StructuralRewrite,
                    iteration: 1,
                },
                RewriteTraceEvent::PhaseEnded {
                    phase: RewritePhase::StructuralRewrite,
                },
                RewriteTraceEvent::PhaseStarted {
                    phase: RewritePhase::SemanticRewrite,
                    stage: "SemanticRewrite",
                },
                RewriteTraceEvent::IterationStarted {
                    phase: RewritePhase::SemanticRewrite,
                    iteration: 1,
                },
                RewriteTraceEvent::PhaseEnded {
                    phase: RewritePhase::SemanticRewrite,
                },
                RewriteTraceEvent::PhaseStarted {
                    phase: RewritePhase::Validation,
                    stage: "Validation",
                },
                RewriteTraceEvent::IterationStarted {
                    phase: RewritePhase::Validation,
                    iteration: 1,
                },
                RewriteTraceEvent::PhaseEnded {
                    phase: RewritePhase::Validation,
                },
            ]
        );
    }

    fn empty_values_plan() -> OptExpr {
        OptExpr::new(
            Operator::LogicalValues(ValuesOp {
                rows: vec![],
                columns: vec![],
            }),
            vec![],
        )
    }
}
