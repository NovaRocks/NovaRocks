use crate::sql::optimizer::rewrite::context::RewriteContext;
use crate::sql::optimizer::rewrite::phase::RewritePhase;
use crate::sql::optimizer::rewrite::rule::LogicalRewriteRule;
use crate::sql::optimizer::rewrite::tree::rewrite_with_rule;
use crate::sql::planner::plan::LogicalPlan;

pub(crate) struct RewriteStage {
    name: &'static str,
    phase: RewritePhase,
    rules: Vec<Box<dyn LogicalRewriteRule>>,
}

impl RewriteStage {
    pub(crate) fn new(
        name: &'static str,
        phase: RewritePhase,
        rules: Vec<Box<dyn LogicalRewriteRule>>,
    ) -> Self {
        Self { name, phase, rules }
    }

    #[cfg(test)]
    pub(crate) fn name(&self) -> &'static str {
        self.name
    }
}

pub(crate) struct RewritePipeline {
    stages: Vec<RewriteStage>,
}

impl RewritePipeline {
    pub(crate) fn new(phases: Vec<RewritePhase>, rules: Vec<Box<dyn LogicalRewriteRule>>) -> Self {
        let mut stages: Vec<RewriteStage> = phases
            .into_iter()
            .map(|phase| RewriteStage::new(phase.as_str(), phase, Vec::new()))
            .collect();

        for rule in rules {
            let phase = rule.phase();
            if let Some(stage) = stages.iter_mut().find(|stage| stage.phase == phase) {
                stage.rules.push(rule);
            } else {
                stages.push(RewriteStage::new(phase.as_str(), phase, vec![rule]));
            }
        }

        Self { stages }
    }

    pub(crate) fn from_stages(stages: Vec<RewriteStage>) -> Self {
        Self { stages }
    }

    pub(crate) fn rule_names(&self) -> Vec<&'static str> {
        self.stages
            .iter()
            .flat_map(|stage| stage.rules.iter().map(|rule| rule.name()))
            .collect()
    }

    pub(crate) fn rewrite(
        &self,
        plan: LogicalPlan,
        ctx: &mut RewriteContext,
    ) -> Result<LogicalPlan, String> {
        let mut current = plan;

        for stage in &self.stages {
            let phase = stage.phase;
            ctx.check_deadline(stage.name)?;
            ctx.trace_mut().phase_started_with_stage(phase, stage.name);

            for iteration in 1..=ctx.policy().max_iterations {
                ctx.check_deadline(stage.name)?;
                ctx.trace_mut().iteration_started(phase, iteration);
                let mut phase_changed = false;

                for rule in &stage.rules {
                    ctx.check_deadline(rule.name())?;
                    let rule_name = rule.name();
                    if !ctx.is_rule_enabled(rule_name) {
                        ctx.trace_mut().rule_skipped(phase, rule_name, "disabled");
                        continue;
                    }

                    match rewrite_with_rule(current, rule.as_ref(), ctx) {
                        Ok((rewritten, changed)) => {
                            current = rewritten;
                            phase_changed |= changed;
                        }
                        Err(message) => {
                            ctx.trace_mut().phase_ended(phase);
                            return Err(message);
                        }
                    }
                }

                if !phase_changed {
                    break;
                }
            }

            ctx.trace_mut().phase_ended(phase);
        }

        Ok(current)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use super::{RewritePipeline, RewriteStage};
    use crate::sql::optimizer::rewrite::context::RewriteContext;
    use crate::sql::optimizer::rewrite::phase::RewritePhase;
    use crate::sql::optimizer::rewrite::result::{RewriteDiagnostic, RewriteResult};
    use crate::sql::optimizer::rewrite::rule::LogicalRewriteRule;
    use crate::sql::optimizer::rewrite::trace::RewriteTraceEvent;
    use crate::sql::planner::plan::{LogicalPlan, ValuesNode};

    struct DisabledRule {
        matches_called: Arc<AtomicUsize>,
    }

    impl LogicalRewriteRule for DisabledRule {
        fn name(&self) -> &'static str {
            "DisabledRule"
        }

        fn phase(&self) -> RewritePhase {
            RewritePhase::LogicalNormalize
        }

        fn matches(&self, _plan: &LogicalPlan, _ctx: &RewriteContext) -> bool {
            self.matches_called.fetch_add(1, Ordering::SeqCst);
            true
        }

        fn apply(
            &self,
            _plan: LogicalPlan,
            _ctx: &mut RewriteContext,
        ) -> Result<RewriteResult, String> {
            Ok(RewriteResult::Unchanged)
        }
    }

    struct FailingRule;

    impl LogicalRewriteRule for FailingRule {
        fn name(&self) -> &'static str {
            "FailingRule"
        }

        fn phase(&self) -> RewritePhase {
            RewritePhase::LogicalNormalize
        }

        fn matches(&self, plan: &LogicalPlan, _ctx: &RewriteContext) -> bool {
            matches!(plan, LogicalPlan::Values(_))
        }

        fn apply(
            &self,
            _plan: LogicalPlan,
            _ctx: &mut RewriteContext,
        ) -> Result<RewriteResult, String> {
            Err("boom".to_string())
        }
    }

    struct RejectingRule;

    impl LogicalRewriteRule for RejectingRule {
        fn name(&self) -> &'static str {
            "RejectingRule"
        }

        fn phase(&self) -> RewritePhase {
            RewritePhase::LogicalNormalize
        }

        fn matches(&self, plan: &LogicalPlan, _ctx: &RewriteContext) -> bool {
            matches!(plan, LogicalPlan::Values(_))
        }

        fn apply(
            &self,
            _plan: LogicalPlan,
            _ctx: &mut RewriteContext,
        ) -> Result<RewriteResult, String> {
            Ok(RewriteResult::Rejected(RewriteDiagnostic::rejected(
                self.name(),
                "not supported",
            )))
        }
    }

    struct ValuesToGenerateSeriesRule;

    impl LogicalRewriteRule for ValuesToGenerateSeriesRule {
        fn name(&self) -> &'static str {
            "ValuesToGenerateSeriesRule"
        }

        fn phase(&self) -> RewritePhase {
            RewritePhase::StructuralRewrite
        }

        fn matches(&self, plan: &LogicalPlan, _ctx: &RewriteContext) -> bool {
            matches!(plan, LogicalPlan::Values(_))
        }

        fn apply(
            &self,
            _plan: LogicalPlan,
            _ctx: &mut RewriteContext,
        ) -> Result<RewriteResult, String> {
            use crate::sql::planner::plan::GenerateSeriesNode;

            Ok(RewriteResult::Changed(LogicalPlan::GenerateSeries(
                GenerateSeriesNode {
                    start: 1,
                    end: 1,
                    step: 1,
                    column_name: "stage1".to_string(),
                    alias: None,
                },
            )))
        }
    }

    struct GenerateSeriesToValuesRule;

    impl LogicalRewriteRule for GenerateSeriesToValuesRule {
        fn name(&self) -> &'static str {
            "GenerateSeriesToValuesRule"
        }

        fn phase(&self) -> RewritePhase {
            RewritePhase::StructuralRewrite
        }

        fn matches(&self, plan: &LogicalPlan, _ctx: &RewriteContext) -> bool {
            matches!(plan, LogicalPlan::GenerateSeries(_))
        }

        fn apply(
            &self,
            _plan: LogicalPlan,
            _ctx: &mut RewriteContext,
        ) -> Result<RewriteResult, String> {
            Ok(RewriteResult::Changed(empty_values_plan()))
        }
    }

    #[test]
    fn empty_pipeline_preserves_plan_and_records_phases() {
        let pipeline = RewritePipeline::new(
            vec![RewritePhase::LogicalNormalize, RewritePhase::Validation],
            vec![],
        );
        let plan = empty_values_plan();
        let before = format!("{plan:?}");
        let mut ctx = RewriteContext::for_query(Vec::<String>::new());

        let rewritten = pipeline.rewrite(plan, &mut ctx).unwrap();

        assert_eq!(format!("{rewritten:?}"), before);
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

    #[test]
    fn disabled_rule_is_skipped_before_match() {
        let matches_called = Arc::new(AtomicUsize::new(0));
        let pipeline = RewritePipeline::new(
            vec![RewritePhase::LogicalNormalize],
            vec![Box::new(DisabledRule {
                matches_called: Arc::clone(&matches_called),
            })],
        );
        let plan = empty_values_plan();
        let before = format!("{plan:?}");
        let mut ctx = RewriteContext::for_query(vec!["DisabledRule".to_string()]);

        let rewritten = pipeline.rewrite(plan, &mut ctx).unwrap();

        assert_eq!(format!("{rewritten:?}"), before);
        assert_eq!(matches_called.load(Ordering::SeqCst), 0);
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
                RewriteTraceEvent::RuleSkipped {
                    phase: RewritePhase::LogicalNormalize,
                    rule: "DisabledRule",
                    reason: "disabled".to_string(),
                },
                RewriteTraceEvent::PhaseEnded {
                    phase: RewritePhase::LogicalNormalize,
                },
            ]
        );
    }

    #[test]
    fn failed_rule_records_one_failed_event() {
        let pipeline = RewritePipeline::new(
            vec![RewritePhase::LogicalNormalize],
            vec![Box::new(FailingRule)],
        );
        let mut ctx = RewriteContext::for_query(Vec::<String>::new());

        let result = pipeline.rewrite(empty_values_plan(), &mut ctx);

        assert_eq!(result.unwrap_err(), "boom");
        assert_eq!(count_failed_events(&ctx, "FailingRule"), 1);
    }

    #[test]
    fn fail_fast_rejection_records_rejected_without_failed_event() {
        let pipeline = RewritePipeline::new(
            vec![RewritePhase::LogicalNormalize],
            vec![Box::new(RejectingRule)],
        );
        let mut ctx = RewriteContext::for_mv_refresh(Vec::<String>::new());

        let result = pipeline.rewrite(empty_values_plan(), &mut ctx);

        assert_eq!(result.unwrap_err(), "not supported");
        assert_eq!(count_rejected_events(&ctx, "RejectingRule"), 1);
        assert_eq!(count_failed_events(&ctx, "RejectingRule"), 0);
    }

    #[test]
    fn duplicate_phase_stages_run_in_declared_order() {
        let pipeline = RewritePipeline::from_stages(vec![
            RewriteStage::new(
                "first-structural-stage",
                RewritePhase::StructuralRewrite,
                vec![Box::new(ValuesToGenerateSeriesRule)],
            ),
            RewriteStage::new(
                "second-structural-stage",
                RewritePhase::StructuralRewrite,
                vec![Box::new(GenerateSeriesToValuesRule)],
            ),
        ]);
        let stage_names: Vec<&'static str> =
            pipeline.stages.iter().map(|stage| stage.name()).collect();
        assert_eq!(
            stage_names,
            vec!["first-structural-stage", "second-structural-stage"]
        );

        let mut ctx = RewriteContext::for_query(Vec::<String>::new());
        let rewritten = pipeline.rewrite(empty_values_plan(), &mut ctx).unwrap();

        assert!(matches!(rewritten, LogicalPlan::Values(_)));
        let changed_rules: Vec<&'static str> = ctx
            .trace()
            .events()
            .iter()
            .filter_map(|event| {
                if let RewriteTraceEvent::RuleChanged { rule, .. } = event {
                    Some(*rule)
                } else {
                    None
                }
            })
            .collect();
        assert_eq!(
            changed_rules,
            vec!["ValuesToGenerateSeriesRule", "GenerateSeriesToValuesRule"]
        );
    }

    fn count_failed_events(ctx: &RewriteContext, rule_name: &'static str) -> usize {
        ctx.trace()
            .events()
            .iter()
            .filter(|event| {
                matches!(
                    event,
                    RewriteTraceEvent::RuleFailed { rule, .. } if *rule == rule_name
                )
            })
            .count()
    }

    fn count_rejected_events(ctx: &RewriteContext, rule_name: &'static str) -> usize {
        ctx.trace()
            .events()
            .iter()
            .filter(|event| {
                matches!(
                    event,
                    RewriteTraceEvent::RuleRejected { rule, .. } if *rule == rule_name
                )
            })
            .count()
    }

    fn empty_values_plan() -> LogicalPlan {
        LogicalPlan::Values(ValuesNode {
            rows: vec![],
            columns: vec![],
        })
    }
}
