use crate::sql::optimizer::rewrite::phase::RewritePhase;

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum RewriteTraceEvent {
    PhaseStarted {
        phase: RewritePhase,
        stage: &'static str,
    },
    PhaseEnded {
        phase: RewritePhase,
    },
    IterationStarted {
        phase: RewritePhase,
        iteration: usize,
    },
    RuleSkipped {
        phase: RewritePhase,
        rule: &'static str,
        reason: String,
    },
    RuleMatched {
        phase: RewritePhase,
        rule: &'static str,
    },
    RuleChanged {
        phase: RewritePhase,
        rule: &'static str,
        elapsed_micros: u128,
    },
    RuleRejected {
        phase: RewritePhase,
        rule: &'static str,
        message: String,
    },
    RuleFailed {
        phase: RewritePhase,
        rule: &'static str,
        message: String,
    },
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(crate) struct RewriteTrace {
    events: Vec<RewriteTraceEvent>,
}

impl RewriteTrace {
    pub(crate) fn events(&self) -> &[RewriteTraceEvent] {
        &self.events
    }

    pub(crate) fn phase_started(&mut self, phase: RewritePhase) {
        self.events.push(RewriteTraceEvent::PhaseStarted {
            stage: phase.as_str(),
            phase,
        });
    }

    pub(crate) fn phase_started_with_stage(&mut self, phase: RewritePhase, stage: &'static str) {
        self.events
            .push(RewriteTraceEvent::PhaseStarted { phase, stage });
    }

    pub(crate) fn stage_names(&self) -> Vec<&'static str> {
        self.events
            .iter()
            .filter_map(|event| {
                if let RewriteTraceEvent::PhaseStarted { stage, .. } = event {
                    Some(*stage)
                } else {
                    None
                }
            })
            .collect()
    }

    pub(crate) fn phase_ended(&mut self, phase: RewritePhase) {
        self.events.push(RewriteTraceEvent::PhaseEnded { phase });
    }

    pub(crate) fn iteration_started(&mut self, phase: RewritePhase, iteration: usize) {
        self.events
            .push(RewriteTraceEvent::IterationStarted { phase, iteration });
    }

    pub(crate) fn rule_skipped(
        &mut self,
        phase: RewritePhase,
        rule: &'static str,
        reason: impl Into<String>,
    ) {
        self.events.push(RewriteTraceEvent::RuleSkipped {
            phase,
            rule,
            reason: reason.into(),
        });
    }

    pub(crate) fn rule_matched(&mut self, phase: RewritePhase, rule: &'static str) {
        self.events
            .push(RewriteTraceEvent::RuleMatched { phase, rule });
    }

    pub(crate) fn rule_changed(
        &mut self,
        phase: RewritePhase,
        rule: &'static str,
        elapsed_micros: u128,
    ) {
        self.events.push(RewriteTraceEvent::RuleChanged {
            phase,
            rule,
            elapsed_micros,
        });
    }

    pub(crate) fn rule_rejected(
        &mut self,
        phase: RewritePhase,
        rule: &'static str,
        message: impl Into<String>,
    ) {
        self.events.push(RewriteTraceEvent::RuleRejected {
            phase,
            rule,
            message: message.into(),
        });
    }

    pub(crate) fn rule_failed(
        &mut self,
        phase: RewritePhase,
        rule: &'static str,
        message: impl Into<String>,
    ) {
        self.events.push(RewriteTraceEvent::RuleFailed {
            phase,
            rule,
            message: message.into(),
        });
    }

    pub(crate) fn changed_rules_count(&self) -> usize {
        self.events
            .iter()
            .filter(|e| matches!(e, RewriteTraceEvent::RuleChanged { .. }))
            .count()
    }

    pub(crate) fn rejected_rules_count(&self) -> usize {
        self.events
            .iter()
            .filter(|e| matches!(e, RewriteTraceEvent::RuleRejected { .. }))
            .count()
    }

    pub(crate) fn failed_rules_count(&self) -> usize {
        self.events
            .iter()
            .filter(|e| matches!(e, RewriteTraceEvent::RuleFailed { .. }))
            .count()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sql::optimizer::rewrite::phase::RewritePhase;

    #[test]
    fn trace_records_phase_and_rule_events() {
        let mut trace = RewriteTrace::default();
        trace.phase_started(RewritePhase::LogicalNormalize);
        trace.iteration_started(RewritePhase::LogicalNormalize, 1);
        trace.rule_skipped(RewritePhase::LogicalNormalize, "RuleA", "disabled");
        trace.rule_matched(RewritePhase::LogicalNormalize, "RuleB");
        trace.rule_changed(RewritePhase::LogicalNormalize, "RuleB", 42);
        trace.rule_rejected(RewritePhase::LogicalNormalize, "RuleC", "not applicable");
        trace.rule_failed(RewritePhase::LogicalNormalize, "RuleD", "boom");
        trace.phase_ended(RewritePhase::LogicalNormalize);

        assert_eq!(trace.events().len(), 8);
        assert!(matches!(
            trace.events()[0],
            RewriteTraceEvent::PhaseStarted {
                phase: RewritePhase::LogicalNormalize,
                ..
            }
        ));
        assert!(matches!(
            trace.events()[4],
            RewriteTraceEvent::RuleChanged {
                phase: RewritePhase::LogicalNormalize,
                rule: "RuleB",
                elapsed_micros: 42
            }
        ));
        assert!(matches!(
            trace.events()[7],
            RewriteTraceEvent::PhaseEnded {
                phase: RewritePhase::LogicalNormalize
            }
        ));
    }

    #[test]
    fn stage_names_returns_unique_labels_in_order() {
        let mut trace = RewriteTrace::default();
        trace.phase_started_with_stage(RewritePhase::LogicalNormalize, "stage-one");
        trace.phase_ended(RewritePhase::LogicalNormalize);
        trace.phase_started_with_stage(RewritePhase::StructuralRewrite, "stage-two");
        trace.phase_ended(RewritePhase::StructuralRewrite);
        trace.phase_started_with_stage(RewritePhase::StructuralRewrite, "stage-three");
        trace.phase_ended(RewritePhase::StructuralRewrite);

        assert_eq!(
            trace.stage_names(),
            vec!["stage-one", "stage-two", "stage-three"]
        );
    }

    #[test]
    fn counter_helpers_aggregate_rule_events() {
        let mut trace = RewriteTrace::default();
        trace.rule_changed(RewritePhase::LogicalNormalize, "RuleA", 0);
        trace.rule_changed(RewritePhase::LogicalNormalize, "RuleA", 0);
        trace.rule_changed(RewritePhase::StructuralRewrite, "RuleB", 0);
        trace.rule_rejected(
            RewritePhase::Validation,
            "RuleC",
            "rejected: missing input",
        );
        trace.rule_failed(RewritePhase::Validation, "RuleD", "boom");

        assert_eq!(trace.changed_rules_count(), 3);
        assert_eq!(trace.rejected_rules_count(), 1);
        assert_eq!(trace.failed_rules_count(), 1);
    }
}
