use std::collections::HashMap;

use crate::sql::optimizer::rewrite::phase::RewritePhase;
use crate::sql::optimizer::rewrite::pipeline::{RewritePipeline, RewriteStage};
use crate::sql::optimizer::rewrite::rules;
use crate::sql::optimizer::statistics::TableStatistics;

pub(crate) fn default_rewrite_phases() -> Vec<RewritePhase> {
    vec![
        RewritePhase::LogicalNormalize,
        RewritePhase::StructuralRewrite,
        RewritePhase::SemanticRewrite,
        RewritePhase::Validation,
    ]
}

pub(crate) fn query_rewrite_pipeline(
    table_stats: &HashMap<String, TableStatistics>,
) -> RewritePipeline {
    RewritePipeline::from_stages(vec![
        RewriteStage::new(
            "PredicatePushdownPreJoin",
            RewritePhase::StructuralRewrite,
            rules::predicate_pushdown_rules(),
        ),
        RewriteStage::new(
            "JoinReorder",
            RewritePhase::StructuralRewrite,
            rules::join_reorder_rules(table_stats),
        ),
        RewriteStage::new(
            "PredicatePushdownPostJoin",
            RewritePhase::StructuralRewrite,
            rules::predicate_pushdown_rules(),
        ),
        RewriteStage::new(
            "AggregatePushdown",
            RewritePhase::StructuralRewrite,
            rules::aggregate_pushdown::aggregate_pushdown_rules(table_stats),
        ),
        RewriteStage::new(
            "ColumnPruning",
            RewritePhase::StructuralRewrite,
            rules::column_pruning_rules(),
        ),
    ])
}

pub(crate) fn mv_rewrite_pipeline() -> RewritePipeline {
    RewritePipeline::new(default_rewrite_phases(), Vec::new())
}

pub(crate) fn is_known_rewrite_rule_name(name: &str) -> bool {
    let table_stats = HashMap::new();
    let query_pipeline = query_rewrite_pipeline(&table_stats);
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
    use std::collections::HashMap;

    use crate::sql::optimizer::rewrite::context::RewriteContext;
    use crate::sql::optimizer::rewrite::phase::RewritePhase;
    use crate::sql::optimizer::rewrite::trace::RewriteTraceEvent;
    use crate::sql::planner::plan::{LogicalPlan, ValuesNode};

    #[derive(Debug, PartialEq, Eq)]
    struct TestMvExtension {
        marker: String,
    }

    #[test]
    fn query_pipeline_contains_migrated_query_rules() {
        let table_stats = HashMap::new();
        let pipeline = query_rewrite_pipeline(&table_stats);
        let mut names = pipeline.rule_names();
        names.sort();

        assert_eq!(
            names,
            vec![
                "AggregatePushdown",
                "EliminateUniqueAggregate",
                "JoinReorder",
                "PruneColumns",
                "PruneUkFkJoin",
                "PushDownPredicateAggregate",
                "PushDownPredicateAggregate",
                "PushDownPredicateJoin",
                "PushDownPredicateJoin",
                "PushDownPredicateProject",
                "PushDownPredicateProject",
                "PushDownPredicateScan",
                "PushDownPredicateScan",
                "PushSemiAntiRightOnlyCondition",
                "PushSemiAntiRightOnlyCondition",
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
        assert!(!is_known_rewrite_rule_name("PushFilterThroughProject"));
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

    fn empty_values_plan() -> LogicalPlan {
        LogicalPlan::Values(ValuesNode {
            rows: vec![],
            columns: vec![],
        })
    }
}
