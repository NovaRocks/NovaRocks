// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. See the License for the
// specific language governing permissions and limitations
// under the License.

//! First-cut result control, independent of remote cleanup and reply consumption.

use crate::{error::TaskExecutionError, graph::TaskGraph};
use novarocks_coordination_contract::{
    AttemptActivationIdentity, AttemptRequestToken, ResultParticipationWithdrawal,
};
use novarocks_execution_contract::{ExchangeDeliveryRequirement, ParticipationRef, TaskIdentity};
use std::{collections::BTreeMap, sync::Arc};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RootSealRefusal {
    TerminalControlRequired,
    CoverageRecoveryExhausted,
    IncompleteObservation,
    TerminalWithoutSuccess,
    AttemptCleanup,
}

impl RootSealRefusal {
    pub const fn message(self) -> &'static str {
        match self {
            Self::TerminalControlRequired => {
                "revoked root output requires terminal control and cannot seal success"
            }
            Self::CoverageRecoveryExhausted => {
                "required covered observation evidence could not be recovered within its budget"
            }
            Self::IncompleteObservation => {
                "success seal refused because Task status observation was incomplete before its linearization point"
            }
            Self::TerminalWithoutSuccess => {
                "success seal reached TaskRound without exact root Finished and an empty attempt failure latch"
            }
            Self::AttemptCleanup => {
                "TaskRound entered terminal cleanup before accepting result success"
            }
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RootCompletion {
    Pending,
    ExactFinished,
    TerminalWithoutSuccess,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum FailureReadiness {
    Empty,
    DerivedPending,
    Authoritative,
}

/// Primary aggregates at this input's serial cut. Computing these facts must
/// not defer acceptance of the current status or scan the graph at this cut.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct SealFacts {
    pub root: RootCompletion,
    pub failure: FailureReadiness,
    pub business_failed: bool,
    pub terminal_control_required: bool,
    pub coverage_ready: bool,
    pub coverage_exhausted: bool,
    /// Exact old advance_normal_drain gate, including terminal-cleanup and
    /// every Task touching either end of a MustDrain edge.
    pub normal_drain_can_start: bool,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SealSettlement {
    pub token: AttemptRequestToken,
    pub result: Result<Arc<[ResultParticipationWithdrawal]>, RootSealRefusal>,
}

/// One pending request and the original root publisher's consumed state.
/// The immutable graph projection is built once before runnable registration;
/// the ready cut selects an Arc in constant time, never walks Tasks or edges.
#[derive(Debug)]
pub struct RootControl {
    pending: Option<AttemptRequestToken>,
    sealed: bool,
    withdrawals: Arc<[ResultParticipationWithdrawal]>,
    empty: Arc<[ResultParticipationWithdrawal]>,
}

impl RootControl {
    pub fn new(
        activation: AttemptActivationIdentity,
        graph: &TaskGraph,
    ) -> Result<Self, TaskExecutionError> {
        if activation.execution() != graph.execution_id() {
            return Err(TaskExecutionError::Schedule(
                "root control activation differs from the frozen graph".into(),
            ));
        }
        let mut output = BTreeMap::<TaskIdentity, bool>::new();
        for edge in graph.edges() {
            let ordinary = edge.delivery() != ExchangeDeliveryRequirement::MustDrain;
            for &producer in edge.producers() {
                output
                    .entry(producer)
                    .and_modify(|all| *all &= ordinary)
                    .or_insert(ordinary);
            }
        }
        let mut participation = BTreeMap::<ParticipationRef, bool>::new();
        for task in graph.tasks() {
            let withdrawn = output.get(&task.identity()) == Some(&true);
            participation
                .entry(task.participation())
                .and_modify(|all| *all &= withdrawn)
                .or_insert(withdrawn);
        }
        let root = graph.root_identity();
        let withdrawals = graph
            .participations()
            .filter(|p| {
                p.backend_process_id() != root.backend_process_id()
                    && participation.get(p) == Some(&true)
            })
            .map(|&p| ResultParticipationWithdrawal::new(activation, root, p))
            .collect::<Arc<[_]>>();
        Ok(Self {
            pending: None,
            sealed: false,
            withdrawals,
            empty: Arc::new([]),
        })
    }

    pub fn request(&mut self, token: AttemptRequestToken) -> Result<(), TaskExecutionError> {
        if self.pending.is_some() || self.sealed {
            return Err(TaskExecutionError::Schedule(
                "more than one result success-seal request reached one TaskRound".into(),
            ));
        }
        self.pending = Some(token);
        Ok(())
    }

    pub const fn pending(&self) -> bool {
        self.pending.is_some()
    }
    pub const fn sealed(&self) -> bool {
        self.sealed
    }

    pub fn refuse(&mut self, reason: RootSealRefusal) -> Option<SealSettlement> {
        self.pending.take().map(|token| SealSettlement {
            token,
            result: Err(reason),
        })
    }

    /// Call after each accepted observation and before the next ordered input.
    /// A consumed request cannot be re-evaluated against a later failure or a
    /// later strict Task completion, even while physical cleanup remains live.
    pub fn evaluate(&mut self, facts: SealFacts) -> Option<SealSettlement> {
        self.pending?;
        if facts.terminal_control_required {
            return self.refuse(RootSealRefusal::TerminalControlRequired);
        }
        if !facts.coverage_ready {
            return if facts.coverage_exhausted {
                self.refuse(RootSealRefusal::CoverageRecoveryExhausted)
            } else {
                None
            };
        }
        if facts.business_failed {
            return self.refuse(RootSealRefusal::TerminalWithoutSuccess);
        }
        if facts.root == RootCompletion::ExactFinished && facts.failure == FailureReadiness::Empty {
            let token = self.pending.take().expect("pending request checked above");
            self.sealed = true;
            let withdrawals = if facts.normal_drain_can_start {
                &self.withdrawals
            } else {
                &self.empty
            };
            return Some(SealSettlement {
                token,
                result: Ok(Arc::clone(withdrawals)),
            });
        }
        if facts.failure == FailureReadiness::Authoritative
            || (facts.failure == FailureReadiness::Empty
                && facts.root == RootCompletion::TerminalWithoutSuccess)
        {
            return self.refuse(RootSealRefusal::TerminalWithoutSuccess);
        }
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::num::NonZeroU64;

    fn graph(extra_leaf_on_producer: bool) -> (TaskGraph, AttemptActivationIdentity) {
        crate::test_support::graph_with_strict_input(extra_leaf_on_producer)
    }
    fn token(value: u64) -> AttemptRequestToken {
        AttemptRequestToken::new(NonZeroU64::new(value).unwrap())
    }
    fn ready(strict_finished: bool) -> SealFacts {
        SealFacts {
            root: RootCompletion::ExactFinished,
            failure: FailureReadiness::Empty,
            business_failed: false,
            terminal_control_required: false,
            coverage_ready: true,
            coverage_exhausted: false,
            normal_drain_can_start: strict_finished,
        }
    }

    #[test]
    fn first_ready_cut_does_not_wait_for_strict_or_change_after_late_failure() {
        let (graph, activation) = graph(false);
        let mut control = RootControl::new(activation, &graph).unwrap();
        control.request(token(1)).unwrap();
        let first = control.evaluate(ready(false)).unwrap();
        assert!(first.result.as_ref().unwrap().is_empty());
        assert!(control.sealed());
        let mut later = ready(true);
        later.failure = FailureReadiness::Authoritative;
        assert_eq!(control.evaluate(later), None);
        assert!(first.result.unwrap().is_empty());
        assert!(control.request(token(2)).is_err());
    }

    #[test]
    fn strict_input_with_only_ordinary_outputs_is_withdrawn_at_ready_cut() {
        let (graph, activation) = graph(false);
        let mut control = RootControl::new(activation, &graph).unwrap();
        control.request(token(1)).unwrap();
        let result = control.evaluate(ready(true)).unwrap().result.unwrap();
        assert_eq!(result.len(), 1);
        assert_eq!(
            result[0].participation().backend_process_id(),
            crate::test_support::backend(2)
        );
        assert_eq!(result[0].root(), graph.root_identity());
        assert_eq!(result[0].activation(), activation);
    }

    #[test]
    fn every_task_in_participation_needs_real_output_withdrawal() {
        let (graph, activation) = graph(true);
        let mut control = RootControl::new(activation, &graph).unwrap();
        control.request(token(1)).unwrap();
        assert!(
            control
                .evaluate(ready(true))
                .unwrap()
                .result
                .unwrap()
                .is_empty()
        );
    }

    #[test]
    fn coverage_and_derived_failure_keep_same_request_pending() {
        let (graph, activation) = graph(false);
        let mut control = RootControl::new(activation, &graph).unwrap();
        control.request(token(1)).unwrap();
        let mut facts = ready(true);
        facts.coverage_ready = false;
        assert_eq!(control.evaluate(facts), None);
        facts.coverage_ready = true;
        facts.failure = FailureReadiness::DerivedPending;
        facts.root = RootCompletion::TerminalWithoutSuccess;
        assert_eq!(control.evaluate(facts), None);
        facts.failure = FailureReadiness::Authoritative;
        assert_eq!(
            control.evaluate(facts).unwrap().result,
            Err(RootSealRefusal::TerminalWithoutSuccess)
        );
    }

    #[test]
    fn local_non_task_failure_cannot_turn_terminal_strict_tasks_into_success() {
        let (graph, activation) = graph(false);
        let mut control = RootControl::new(activation, &graph).unwrap();
        control.request(token(1)).unwrap();
        let mut facts = ready(true);
        facts.business_failed = true;
        assert_eq!(
            control.evaluate(facts).unwrap().result,
            Err(RootSealRefusal::TerminalWithoutSuccess)
        );
        assert!(!control.sealed());
    }

    #[test]
    fn terminal_control_precedes_coverage_and_incomplete_ingress_refuses_once() {
        let (graph, activation) = graph(false);
        let mut control = RootControl::new(activation, &graph).unwrap();
        control.request(token(1)).unwrap();
        let mut facts = ready(true);
        facts.terminal_control_required = true;
        facts.coverage_ready = false;
        assert_eq!(
            control.evaluate(facts).unwrap().result,
            Err(RootSealRefusal::TerminalControlRequired)
        );
        assert_eq!(control.refuse(RootSealRefusal::IncompleteObservation), None);
        control.request(token(2)).unwrap();
        assert_eq!(
            control
                .refuse(RootSealRefusal::IncompleteObservation)
                .unwrap()
                .token,
            token(2)
        );
    }

    #[test]
    fn duplicate_pending_request_preserves_the_original_correlation() {
        let (graph, activation) = graph(false);
        let mut control = RootControl::new(activation, &graph).unwrap();
        control.request(token(1)).unwrap();
        assert!(control.request(token(2)).is_err());
        assert_eq!(control.evaluate(ready(true)).unwrap().token, token(1));
    }
}
