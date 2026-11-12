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

//! Remote tracking over the original frozen participations. An immutable
//! remote disposition never proves a local Source, capture or Worker exit.
//!
//! The Core preserves the original inline-read turn while this charged pass
//! is active. Correlated endpoint reads use an original independently prepaid
//! Source; neither cached membership hints nor a deadline can invent a reply.
//! P10a must prove both credit independence and continuation reachability under
//! the runtime's same-Source FIFO. This component cannot grant those rights.

use crate::{
    capacity::StepContext, error::TaskExecutionError, execution::QueryTaskExecution,
    graph::TaskGraph, input::MembershipObservation,
};
use novarocks_coordination_contract::{
    AttemptConvergenceClosure, AttemptConvergenceMode, AttemptParticipationConvergence,
    AttemptRequestToken, MonotonicInstant,
};
use novarocks_execution_contract::{ParticipationRef, TaskIdentity};
use std::sync::Arc;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum RemoteConvergenceDirective {
    ObserveProcess {
        request: AttemptRequestToken,
        participation: ParticipationRef,
    },
    NeedRequestCredit,
    Yield,
    Waiting,
    ClosureReady,
    Inactive,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ProcessReplyDisposition {
    Applied,
    Stale,
}
#[derive(Clone, Copy, Debug)]
struct Flight {
    request: AttemptRequestToken,
    issued_at: MonotonicInstant,
}
#[derive(Clone, Copy, Debug)]
struct ReadRow {
    participation: ParticipationRef,
    flight: Option<Flight>,
}
#[derive(Clone, Copy, Debug)]
struct Pass {
    // Neither later control nor a new caller extends/rewrites this old turn.
    mode: AttemptConvergenceMode,
    deadline: MonotonicInstant,
    next: usize,
    read: Option<ReadRow>,
}
#[derive(Clone, Copy, Debug)]
struct LocalEvidence {
    worker_stopped_and_fenced: bool,
    released: bool,
    never_established: bool,
}
impl LocalEvidence {
    fn from_execution(
        execution: &QueryTaskExecution,
        participation: ParticipationRef,
    ) -> Result<Self, TaskExecutionError> {
        let owner = execution
            .owner(participation)
            .ok_or(TaskExecutionError::UnknownOperation)?;
        Ok(Self {
            worker_stopped_and_fenced: execution
                .participation_convergence_receipt(participation)
                .is_some(),
            released: owner.is_released(),
            never_established: execution.participation_never_established(participation),
        })
    }
    fn selected(self, mode: AttemptConvergenceMode) -> Option<AttemptParticipationConvergence> {
        if self.worker_stopped_and_fenced
            && (mode == AttemptConvergenceMode::Cleanup || self.released)
        {
            Some(AttemptParticipationConvergence::WorkerStoppedAndParticipationFenced)
        } else if self.never_established {
            Some(AttemptParticipationConvergence::NeverEstablished)
        } else {
            None
        }
    }
}
#[derive(Debug)]
pub(crate) struct RemoteConvergence {
    root: TaskIdentity,
    targets: Arc<[ParticipationRef]>,
    mode: Option<AttemptConvergenceMode>,
    deadline: Option<MonotonicInstant>,
    pass: Option<Pass>,
    // One constructor-priced row per frozen participation. Rows contain only
    // Copy protocol facts, so a failed pass reuses this allocation in O(1).
    evidence: Vec<(ParticipationRef, AttemptParticipationConvergence)>,
    closure: Option<Arc<AttemptConvergenceClosure>>,
}
impl RemoteConvergence {
    /// Called before registering the actor, under the same priced metadata
    /// declaration as its validated graph. The graph is nonempty and ordered.
    pub(crate) fn new(graph: &TaskGraph) -> Self {
        let targets: Arc<[ParticipationRef]> = graph.participations().copied().collect();
        Self {
            root: graph.root_identity(),
            evidence: Vec::with_capacity(targets.len()),
            targets,
            mode: None,
            deadline: None,
            pass: None,
            closure: None,
        }
    }
    /// Same trusted request timeline. Later entry can shorten, never extend.
    /// A current old inline pass retains its own mode/deadline; new control
    /// takes effect in the next pass, not midway through already-read rows.
    pub(crate) fn begin(&mut self, mode: AttemptConvergenceMode, deadline: MonotonicInstant) {
        self.mode = Some(match (self.mode, mode) {
            (Some(AttemptConvergenceMode::Cleanup), _) | (_, AttemptConvergenceMode::Cleanup) => {
                AttemptConvergenceMode::Cleanup
            }
            _ => AttemptConvergenceMode::Normal,
        });
        self.deadline = Some(self.deadline.map_or(deadline, |old| old.min(deadline)));
    }
    pub(crate) fn mode(&self) -> Option<AttemptConvergenceMode> {
        self.mode
    }

    pub(crate) fn upgrade_to_cleanup(&mut self) -> bool {
        if self.mode == Some(AttemptConvergenceMode::Normal) && self.closure.is_none() {
            self.mode = Some(AttemptConvergenceMode::Cleanup);
            return true;
        }
        false
    }
    pub(crate) fn closure(&self) -> Option<&Arc<AttemptConvergenceClosure>> {
        self.closure.as_ref()
    }
    #[cfg(test)]
    pub(crate) fn permits_protocol_turn(&self) -> bool {
        self.pass.is_none()
    }
    /// Only a genuine original turn/endpoint or protocol change/deadline
    /// opens a pass. Its own read reply/exit is a continuation, not a new pass.
    pub(crate) fn start_pass(&mut self) -> bool {
        if self.closure.is_some() || self.pass.is_some() {
            return false;
        }
        let (Some(mode), Some(deadline)) = (self.mode, self.deadline) else {
            return false;
        };
        self.evidence.clear();
        self.pass = Some(Pass {
            mode,
            deadline,
            next: 0,
            read: None,
        });
        true
    }
    pub(crate) fn advance(
        &mut self,
        now: MonotonicInstant,
        execution: &QueryTaskExecution,
        context: &mut dyn StepContext,
        request: Option<AttemptRequestToken>,
    ) -> Result<RemoteConvergenceDirective, TaskExecutionError> {
        if execution.graph().root_identity() != self.root {
            return Err(TaskExecutionError::Schedule(
                "remote convergence received another original graph".into(),
            ));
        }
        self.advance_rows(now, context, request, |participation| {
            LocalEvidence::from_execution(execution, participation)
        })
    }
    fn advance_rows(
        &mut self,
        now: MonotonicInstant,
        context: &mut dyn StepContext,
        request: Option<AttemptRequestToken>,
        mut local: impl FnMut(ParticipationRef) -> Result<LocalEvidence, TaskExecutionError>,
    ) -> Result<RemoteConvergenceDirective, TaskExecutionError> {
        if self.closure.is_some() {
            return Ok(RemoteConvergenceDirective::ClosureReady);
        }
        loop {
            let Some(pass) = self.pass.as_mut() else {
                return Ok(RemoteConvergenceDirective::Inactive);
            };
            if let Some(read) = pass.read.as_mut() {
                if read.flight.is_some() {
                    return Ok(RemoteConvergenceDirective::Waiting);
                }
                let Some(request) = request else {
                    return Ok(RemoteConvergenceDirective::NeedRequestCredit);
                };
                read.flight = Some(Flight {
                    request,
                    issued_at: now,
                });
                return Ok(RemoteConvergenceDirective::ObserveProcess {
                    request,
                    participation: read.participation,
                });
            }
            if pass.next == self.targets.len() {
                self.closure = Some(Arc::new(AttemptConvergenceClosure::Evidence(
                    std::mem::take(&mut self.evidence),
                )));
                self.pass = None;
                return Ok(RemoteConvergenceDirective::ClosureReady);
            }
            if !context.charge(1) {
                return Ok(RemoteConvergenceDirective::Yield);
            }
            let participation = self.targets[pass.next];
            if let Some(fact) = local(participation)?.selected(pass.mode) {
                self.evidence.push((participation, fact));
                pass.next += 1;
            } else {
                // Enter the original read before waiting for credit. Later
                // ACK/status cannot change this row's already-tested branches.
                pass.read = Some(ReadRow {
                    participation,
                    flight: None,
                });
            }
        }
    }
    pub(crate) fn process_reply(
        &mut self,
        now: MonotonicInstant,
        request: AttemptRequestToken,
        participation: ParticipationRef,
        sampled_at: MonotonicInstant,
        observation: MembershipObservation,
    ) -> Result<ProcessReplyDisposition, TaskExecutionError> {
        let Some(pass) = self.pass.as_mut() else {
            return Ok(ProcessReplyDisposition::Stale);
        };
        let Some(read) = pass.read else {
            return Ok(ProcessReplyDisposition::Stale);
        };
        let Some(flight) = read.flight else {
            return Ok(ProcessReplyDisposition::Stale);
        };
        if request != flight.request {
            return Ok(ProcessReplyDisposition::Stale);
        }
        if participation != read.participation || sampled_at < flight.issued_at || sampled_at > now
        {
            return Err(TaskExecutionError::Schedule(
                "remote convergence sample differs from its original row/request/timeline".into(),
            ));
        }
        match observation {
            MembershipObservation::ExactReplacement(current)
                if current != participation.backend_process_id() =>
            {
                self.evidence.push((
                    participation,
                    AttemptParticipationConvergence::WorkerProcessReplaced,
                ));
                pass.next += 1;
                pass.read = None;
                if pass.next == self.targets.len() {
                    self.closure = Some(Arc::new(AttemptConvergenceClosure::Evidence(
                        std::mem::take(&mut self.evidence),
                    )));
                    self.pass = None;
                }
            }
            _ => {
                let deadline = pass.deadline;
                // The first actual unresolved read ends THIS pass. Later
                // protocol progress is legal again; no periodic recheck starts.
                self.pass = None;
                self.evidence.clear();
                if now.has_reached(deadline) {
                    self.closure = Some(Arc::new(AttemptConvergenceClosure::RemoteTrackingEnded {
                        exact_participations: Arc::clone(&self.targets),
                    }));
                }
            }
        }
        Ok(ProcessReplyDisposition::Applied)
    }
    pub(crate) fn waiting_for_process(&self) -> bool {
        self.pass
            .and_then(|pass| pass.read)
            .is_some_and(|read| read.flight.is_some())
    }

    /// Source credit/read waits have their own real wakes. The deadline may
    /// not repeatedly wake an unfinished read, or manufacture its result.
    pub(crate) fn next_deadline(&self) -> Option<MonotonicInstant> {
        if self.closure.is_some() || self.pass.is_some() {
            None
        } else {
            self.deadline
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{capacity::QueueStage, config::DispatchLane};
    use novarocks_types::BackendProcessId;
    use std::{collections::BTreeMap, num::NonZeroU64, time::Duration};
    struct Units {
        left: usize,
        charged: usize,
    }
    impl Units {
        fn new(left: usize) -> Self {
            Self { left, charged: 0 }
        }
    }
    impl StepContext for Units {
        fn charge(&mut self, units: usize) -> bool {
            if units > self.left {
                false
            } else {
                self.left -= units;
                self.charged += units;
                true
            }
        }
        fn reserve_queue_stage(
            &mut self,
            _: DispatchLane,
            _: bool,
            _: BackendProcessId,
            _: usize,
            _: usize,
        ) -> QueueStage {
            panic!("remote tracking does not acquire a Native transport window")
        }
    }
    fn now(ms: u64) -> MonotonicInstant {
        MonotonicInstant::from_origin(Duration::from_millis(ms))
    }
    fn token(n: u64) -> AttemptRequestToken {
        AttemptRequestToken::new(NonZeroU64::new(n).unwrap())
    }
    fn fixture() -> (QueryTaskExecution, RemoteConvergence) {
        let execution = crate::test_support::execution_with_strict_input();
        let remote = RemoteConvergence::new(execution.graph());
        (execution, remote)
    }
    fn unresolved() -> LocalEvidence {
        LocalEvidence {
            worker_stopped_and_fenced: false,
            released: false,
            never_established: false,
        }
    }
    fn advance(
        remote: &mut RemoteConvergence,
        at: u64,
        units: usize,
        request: Option<u64>,
        facts: &BTreeMap<ParticipationRef, LocalEvidence>,
    ) -> RemoteConvergenceDirective {
        remote
            .advance_rows(now(at), &mut Units::new(units), request.map(token), |p| {
                Ok(facts.get(&p).copied().unwrap_or_else(unresolved))
            })
            .unwrap()
    }
    fn sample(
        remote: &mut RemoteConvergence,
        at: u64,
        request: u64,
        part: ParticipationRef,
        observation: MembershipObservation,
    ) {
        assert_eq!(
            remote
                .process_reply(now(at), token(request), part, now(at), observation)
                .unwrap(),
            ProcessReplyDisposition::Applied
        );
    }
    fn evidence(
        remote: &RemoteConvergence,
    ) -> &[(ParticipationRef, AttemptParticipationConvergence)] {
        match remote.closure().unwrap().as_ref() {
            AttemptConvergenceClosure::Evidence(rows) => rows,
            _ => panic!("expected original positive evidence"),
        }
    }
    #[test]
    fn real_original_never_issued_owners_complete_one_charged_row_at_a_time_even_after_deadline() {
        let (execution, mut remote) = fixture();
        remote.begin(AttemptConvergenceMode::Cleanup, now(1));
        assert!(remote.start_pass());
        let expected = execution
            .graph()
            .participations()
            .copied()
            .collect::<Vec<_>>();
        for index in 0..expected.len() {
            let mut units = Units::new(1);
            let result = remote
                .advance(now(10), &execution, &mut units, None)
                .unwrap();
            assert_eq!(units.charged, 1);
            assert_eq!(
                result,
                if index + 1 == expected.len() {
                    RemoteConvergenceDirective::ClosureReady
                } else {
                    RemoteConvergenceDirective::Yield
                }
            );
        }
        assert_eq!(
            evidence(&remote),
            expected
                .iter()
                .map(|&p| (p, AttemptParticipationConvergence::NeverEstablished))
                .collect::<Vec<_>>()
        );
        assert!(remote.permits_protocol_turn());
        assert_eq!(remote.next_deadline(), None);
    }
    #[test]
    fn local_receipt_requires_original_release_in_normal_mode_and_precedes_never_or_process() {
        for mode in [
            AttemptConvergenceMode::Normal,
            AttemptConvergenceMode::Cleanup,
        ] {
            for released in [false, true] {
                let (_, mut remote) = fixture();
                let parts = remote.targets.to_vec();
                let facts = parts
                    .iter()
                    .map(|&p| {
                        (
                            p,
                            LocalEvidence {
                                worker_stopped_and_fenced: true,
                                released,
                                never_established: false,
                            },
                        )
                    })
                    .collect();
                remote.begin(mode, now(100));
                remote.start_pass();
                let result = advance(&mut remote, 0, 10, Some(1), &facts);
                if mode == AttemptConvergenceMode::Cleanup || released {
                    assert_eq!(result, RemoteConvergenceDirective::ClosureReady);
                    assert!(evidence(&remote).iter().all(|(_, kind)| *kind
                        == AttemptParticipationConvergence::WorkerStoppedAndParticipationFenced));
                } else {
                    assert_eq!(
                        result,
                        RemoteConvergenceDirective::ObserveProcess {
                            request: token(1),
                            participation: parts[0]
                        }
                    );
                }
            }
        }
        let facts = LocalEvidence {
            worker_stopped_and_fenced: true,
            released: true,
            never_established: true,
        };
        assert_eq!(
            facts.selected(AttemptConvergenceMode::Normal),
            Some(AttemptParticipationConvergence::WorkerStoppedAndParticipationFenced)
        );
    }
    #[test]
    fn expired_pass_still_reads_every_required_original_endpoint_before_positive_evidence() {
        let (_, mut remote) = fixture();
        remote.begin(AttemptConvergenceMode::Normal, now(1));
        remote.start_pass();
        let parts = remote.targets.to_vec();
        for (index, &part) in parts.iter().enumerate() {
            let request = index as u64 + 1;
            assert_eq!(
                advance(&mut remote, 20, 1, Some(request), &BTreeMap::new()),
                RemoteConvergenceDirective::ObserveProcess {
                    request: token(request),
                    participation: part
                }
            );
            assert!(remote.closure().is_none());
            sample(
                &mut remote,
                21,
                request,
                part,
                MembershipObservation::ExactReplacement(crate::test_support::backend(50 + request)),
            );
        }
        assert!(
            evidence(&remote)
                .iter()
                .all(|(_, kind)| *kind == AttemptParticipationConvergence::WorkerProcessReplaced)
        );
        assert!(!remote.start_pass());
    }
    #[test]
    fn first_actual_unresolved_uses_return_time_and_freezes_same_set_without_reading_remaining_rows()
     {
        for observation in [
            MembershipObservation::Unobserved,
            MembershipObservation::ExactOriginal,
            MembershipObservation::ExactReplacement(crate::test_support::backend(1)),
        ] {
            let (_, mut remote) = fixture();
            remote.begin(AttemptConvergenceMode::Cleanup, now(5));
            remote.start_pass();
            let first = remote.targets[0];
            let observation = if matches!(observation, MembershipObservation::ExactReplacement(_)) {
                MembershipObservation::ExactReplacement(first.backend_process_id())
            } else {
                observation
            };
            assert!(matches!(
                advance(&mut remote, 0, 1, Some(1), &BTreeMap::new()),
                RemoteConvergenceDirective::ObserveProcess { .. }
            ));
            assert_eq!(
                remote
                    .process_reply(now(6), token(1), first, now(4), observation)
                    .unwrap(),
                ProcessReplyDisposition::Applied
            );
            let closure = Arc::clone(remote.closure().unwrap());
            match closure.as_ref() {
                AttemptConvergenceClosure::RemoteTrackingEnded {
                    exact_participations,
                } => assert!(Arc::ptr_eq(exact_participations, &remote.targets)),
                _ => panic!("unobservable is not positive exit evidence"),
            }
            remote.begin(AttemptConvergenceMode::Normal, now(500));
            assert_eq!(remote.mode(), Some(AttemptConvergenceMode::Cleanup));
            assert!(!remote.start_pass());
            assert_eq!(
                remote
                    .process_reply(
                        now(7),
                        token(1),
                        first,
                        now(7),
                        MembershipObservation::ExactReplacement(crate::test_support::backend(900))
                    )
                    .unwrap(),
                ProcessReplyDisposition::Stale
            );
            assert!(Arc::ptr_eq(remote.closure().unwrap(), &closure));
        }
    }
    #[test]
    fn unexpired_first_unresolved_releases_turn_for_late_release_without_periodic_or_self_read_loop()
     {
        let (_, mut remote) = fixture();
        remote.begin(AttemptConvergenceMode::Normal, now(100));
        remote.start_pass();
        let first = remote.targets[0];
        advance(&mut remote, 0, 1, Some(1), &BTreeMap::new());
        sample(&mut remote, 1, 1, first, MembershipObservation::Unobserved);
        assert!(remote.permits_protocol_turn());
        assert!(remote.closure().is_none());
        assert_eq!(
            advance(&mut remote, 1, 100, Some(2), &BTreeMap::new()),
            RemoteConvergenceDirective::Inactive
        );
        assert_eq!(remote.next_deadline(), Some(now(100)));
        // A real subsequent protocol turn has accepted the original Release.
        let facts = remote
            .targets
            .iter()
            .map(|&p| {
                (
                    p,
                    LocalEvidence {
                        worker_stopped_and_fenced: true,
                        released: true,
                        never_established: false,
                    },
                )
            })
            .collect();
        assert!(remote.start_pass());
        assert_eq!(
            advance(&mut remote, 2, 100, None, &facts),
            RemoteConvergenceDirective::ClosureReady
        );
        assert!(evidence(&remote).iter().all(|(_, kind)| *kind
            == AttemptParticipationConvergence::WorkerStoppedAndParticipationFenced));
    }
    #[test]
    fn original_deadline_only_shortens_and_late_mode_does_not_rewrite_current_pass_or_fixed_closure()
     {
        let (_, mut remote) = fixture();
        remote.begin(AttemptConvergenceMode::Normal, now(20));
        remote.begin(AttemptConvergenceMode::Normal, now(100));
        assert_eq!(remote.next_deadline(), Some(now(20)));
        remote.start_pass();
        let first = remote.targets[0];
        advance(&mut remote, 0, 1, Some(1), &BTreeMap::new());
        remote.begin(AttemptConvergenceMode::Cleanup, now(5));
        assert_eq!(remote.pass.unwrap().mode, AttemptConvergenceMode::Normal);
        assert_eq!(remote.pass.unwrap().deadline, now(20));
        sample(&mut remote, 6, 1, first, MembershipObservation::Unobserved);
        assert!(remote.closure().is_none());
        assert_eq!(remote.next_deadline(), Some(now(5)));
        assert!(remote.start_pass());
        assert_eq!(remote.pass.unwrap().mode, AttemptConvergenceMode::Cleanup);
        let facts = remote
            .targets
            .iter()
            .map(|&p| {
                (
                    p,
                    LocalEvidence {
                        worker_stopped_and_fenced: true,
                        released: false,
                        never_established: false,
                    },
                )
            })
            .collect();
        assert_eq!(
            advance(&mut remote, 6, 100, None, &facts),
            RemoteConvergenceDirective::ClosureReady
        );
        let closure = Arc::clone(remote.closure().unwrap());
        remote.begin(AttemptConvergenceMode::Normal, now(1));
        assert!(!remote.start_pass());
        assert!(Arc::ptr_eq(remote.closure().unwrap(), &closure));
    }
    #[test]
    fn credit_full_retains_same_read_branch_and_current_row_without_charging_or_rechecking_local_state()
     {
        let (_, mut remote) = fixture();
        remote.begin(AttemptConvergenceMode::Normal, now(5));
        remote.start_pass();
        let first = remote.targets[0];
        assert_eq!(
            advance(&mut remote, 0, 1, None, &BTreeMap::new()),
            RemoteConvergenceDirective::NeedRequestCredit
        );
        assert!(!remote.permits_protocol_turn());
        assert_eq!(remote.next_deadline(), None);
        let mut units = Units::new(0);
        let result=remote.advance_rows(now(6),&mut units,Some(token(1)),|_|panic!("the already-tested row cannot adopt later primary facts before its original read")).unwrap();
        assert_eq!(units.charged, 0);
        assert_eq!(
            result,
            RemoteConvergenceDirective::ObserveProcess {
                request: token(1),
                participation: first
            }
        );
        assert_eq!(
            advance(&mut remote, 7, 0, Some(2), &BTreeMap::new()),
            RemoteConvergenceDirective::Waiting
        );
        assert_eq!(remote.next_deadline(), None);
        sample(&mut remote, 7, 1, first, MembershipObservation::Unobserved);
        assert!(matches!(
            remote.closure().unwrap().as_ref(),
            AttemptConvergenceClosure::RemoteTrackingEnded { .. }
        ));
    }
    #[test]
    fn wrong_old_or_cached_process_reply_cannot_advance_another_original_row() {
        let (_, mut remote) = fixture();
        remote.begin(AttemptConvergenceMode::Cleanup, now(100));
        remote.start_pass();
        let first = remote.targets[0];
        let second = remote.targets[1];
        advance(&mut remote, 10, 1, Some(1), &BTreeMap::new());
        let replacement =
            MembershipObservation::ExactReplacement(crate::test_support::backend(500));
        assert_eq!(
            remote
                .process_reply(now(11), token(99), first, now(11), replacement)
                .unwrap(),
            ProcessReplyDisposition::Stale
        );
        for (part, sampled) in [(second, 11), (first, 9), (first, 12)] {
            assert!(
                remote
                    .process_reply(now(11), token(1), part, now(sampled), replacement)
                    .is_err()
            );
        }
        sample(&mut remote, 11, 1, first, replacement);
        assert!(remote.closure().is_none());
        advance(&mut remote, 12, 1, Some(2), &BTreeMap::new());
        assert_eq!(
            remote
                .process_reply(now(13), token(1), first, now(13), replacement)
                .unwrap(),
            ProcessReplyDisposition::Stale
        );
        assert_eq!(remote.pass.unwrap().read.unwrap().participation, second);
    }
    #[test]
    fn frozen_backend_lookup_returns_only_original_graph_rows_without_scanning_or_inventing_owner()
    {
        let (execution, _) = fixture();
        for &part in execution.graph().participations() {
            assert_eq!(
                execution
                    .graph()
                    .participation_on_backend(part.backend_process_id()),
                Some(part)
            );
        }
        assert_eq!(
            execution
                .graph()
                .participation_on_backend(crate::test_support::backend(999)),
            None
        );
    }
}
