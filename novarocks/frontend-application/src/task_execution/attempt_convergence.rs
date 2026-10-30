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

//! Shared convergence of one original Native Task attempt.
//!
//! This owner borrows the same TaskRound that ran the attempt. It does not
//! authorize admission, select a logical outcome, or own publication. The
//! frozen process identities and covered stream remain the only sources of
//! positive remote convergence; a control ACK or an elapsed wait is not exit.

use std::collections::BTreeSet;

use novarocks_execution::runtime::endpoint::RuntimeEndpoint;
use novarocks_execution_contract::QueryContextRef;
use novarocks_query_application::api::{
    BackendProcessObservation, BackendProcessObservationService, NativeActiveConvergenceClosure,
    NativeActiveConvergenceRequest, NativeAttemptConvergence, NativeContextConvergence,
    StatementNativeHealthFailure,
};

use super::error::TaskExecutionError;
use super::round::{TaskRound, TurnReport};
use super::split_transport::SplitDeliveryBridge;

const IDLE_RECHECK: std::time::Duration = std::time::Duration::from_millis(5);

/// The process identity is an indivisible component of the frozen context.
/// Its original endpoint is retained only to observe exact process replacement.
#[derive(Clone, Debug)]
pub(crate) struct NativeAttemptConvergenceTarget {
    context: QueryContextRef,
    endpoint: RuntimeEndpoint,
}

impl NativeAttemptConvergenceTarget {
    pub(crate) fn new(context: QueryContextRef, endpoint: RuntimeEndpoint) -> Self {
        Self { context, endpoint }
    }
}

/// The caller supplies the attempt's physical drain policy. Normal drain is
/// not a logical or publication success verdict, and cleanup cannot restore it.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum NativeAttemptConvergenceMode {
    Normal,
    Cleanup,
}

pub(crate) enum NativeAttemptConvergenceStep {
    Ready,
    Pending { moved: bool },
}

/// Role-local convergence policy over the original TaskRound's facts.
/// Task/context lifecycle, operation replay and first cause stay in TaskRound.
pub(crate) struct NativeAttemptConvergenceOwner {
    source: BackendProcessObservationService,
    targets: Box<[NativeAttemptConvergenceTarget]>,
    mode: Option<NativeAttemptConvergenceMode>,
    /// One request to the original bridge, not another delivery or Task verdict.
    split_close_requested: bool,
    split_failure:
        Option<crate::query_execution::split_assignment_round::SplitAssignmentFailureSource>,
    remote_deadline: Option<std::time::Instant>,
    remote_closure: Option<NativeActiveConvergenceClosure>,
    local_unresolved: Option<StatementNativeHealthFailure>,
}

impl NativeAttemptConvergenceOwner {
    pub(crate) fn new(
        round: &TaskRound,
        source: BackendProcessObservationService,
        targets: Vec<NativeAttemptConvergenceTarget>,
    ) -> Result<Self, TaskExecutionError> {
        let expected = round
            .execution()
            .graph()
            .contexts()
            .copied()
            .collect::<BTreeSet<_>>();
        let mut actual = BTreeSet::new();
        for target in &targets {
            if !actual.insert(target.context) {
                return Err(TaskExecutionError::Schedule(
                    "Native convergence repeats a frozen query context".to_owned(),
                ));
            }
        }
        if expected.is_empty() || actual != expected {
            return Err(TaskExecutionError::Schedule(
                "Native convergence targets do not exactly cover the original TaskRound".to_owned(),
            ));
        }
        Ok(Self {
            source,
            targets: targets.into_boxed_slice(),
            mode: None,
            split_close_requested: false,
            split_failure: None,
            remote_deadline: None,
            remote_closure: None,
            local_unresolved: None,
        })
    }

    pub(crate) fn bind_split_failure_source(
        &mut self,
        source: Option<
            crate::query_execution::split_assignment_round::SplitAssignmentFailureSource,
        >,
    ) {
        self.split_failure = source;
    }

    /// Idempotently closes the original producer path. A later cleanup cut
    /// may strengthen Normal, but no later call can restore normal production.
    pub(crate) fn begin(
        &mut self,
        round: &mut TaskRound,
        split_delivery: &SplitDeliveryBridge,
        mode: NativeAttemptConvergenceMode,
    ) {
        match (self.mode, mode) {
            (None, mode) => {
                self.mode = Some(mode);
                self.split_close_requested = begin_convergence(round, split_delivery, mode);
            }
            (Some(NativeAttemptConvergenceMode::Normal), NativeAttemptConvergenceMode::Cleanup) => {
                self.mode = Some(NativeAttemptConvergenceMode::Cleanup);
                self.split_close_requested =
                    begin_convergence(round, split_delivery, NativeAttemptConvergenceMode::Cleanup);
            }
            _ => {
                if !self.split_close_requested
                    && let Some(effective) = self.mode
                {
                    // Normal's initial request may have preceded the strict
                    // Task tail. Reconcile actual closure on this same owner.
                    self.split_close_requested =
                        begin_convergence(round, split_delivery, effective);
                }
            }
        }
    }

    /// The same exact process-evidence source serves pending pre-ready
    /// observation and residual convergence. It grants no scheduling authority.
    pub(crate) fn subscribe_process_changes(&self) -> tokio::sync::watch::Receiver<u64> {
        self.source.subscribe_process_changes()
    }

    pub(crate) fn process_replaced(&self, backend: novarocks_types::BackendProcessId) -> bool {
        self.replacement_process(backend).is_some()
    }

    /// Preserve the observed replacement identity from the same frozen
    /// endpoint. Missing, unhealthy or equal identities are not replacement.
    pub(crate) fn replacement_process(
        &self,
        backend: novarocks_types::BackendProcessId,
    ) -> Option<novarocks_types::BackendProcessId> {
        let target = self
            .targets
            .iter()
            .find(|target| target.context.backend_process_id() == backend)?;
        match self
            .source
            .observe_process_at_endpoint(backend, &target.endpoint)
        {
            Ok(BackendProcessObservation::Replaced { current_process })
                if current_process != backend =>
            {
                Some(current_process)
            }
            _ => None,
        }
    }

    /// Read only facts already accepted by the original covered owner, plus
    /// exact process replacement at the original endpoint. Unobservable is
    /// never success. Normal drain must also consume its original Release ACK.
    pub(crate) fn observed_contexts(
        &self,
        round: &TaskRound,
    ) -> Option<Box<[NativeContextConvergence]>> {
        let mode = self.mode?;
        let mut convergence = Vec::with_capacity(self.targets.len());
        for target in &self.targets {
            if round
                .execution()
                .context_convergence_receipt(target.context)
                .is_some()
                && (mode == NativeAttemptConvergenceMode::Cleanup
                    || round
                        .execution()
                        .owner(target.context)
                        .is_some_and(|owner| owner.is_released()))
            {
                convergence.push(NativeContextConvergence::worker_stopped_and_context_fenced(
                    target.context,
                ));
                continue;
            }
            if round.execution().context_never_established(target.context) {
                convergence.push(NativeContextConvergence::never_established(target.context));
                continue;
            }
            if self.process_replaced(target.context.backend_process_id()) {
                convergence.push(NativeContextConvergence::worker_process_replaced(
                    target.context,
                ));
            } else {
                return None;
            }
        }
        Some(convergence.into_boxed_slice())
    }

    /// One bounded turn, usable by the existing synchronous statement bridge
    /// and by the async attempt owner. Err reports this turn's real failure;
    /// it is not a stopped fact and never releases or replaces the owner.
    pub(crate) fn step(
        &mut self,
        round: &mut TaskRound,
        split_delivery: &SplitDeliveryBridge,
        mode: NativeAttemptConvergenceMode,
        requested_cleanup: bool,
    ) -> Result<NativeAttemptConvergenceStep, TaskExecutionError> {
        let effective = if requested_cleanup || round.failure_cause().is_some() {
            NativeAttemptConvergenceMode::Cleanup
        } else {
            mode
        };
        if effective == NativeAttemptConvergenceMode::Cleanup
            && self.mode != Some(NativeAttemptConvergenceMode::Cleanup)
        {
            tracing::warn!(
                requested_cleanup,
                failure = ?round.failure_cause(),
                "Native convergence entered terminal cleanup after a late cancellation or Task failure"
            );
        }
        self.begin(round, split_delivery, effective);
        if self.mode == Some(NativeAttemptConvergenceMode::Normal)
            && let Some(error) = self
                .split_failure
                .as_ref()
                .and_then(|source| source.failure())
        {
            // The original split driver still owns this cause. Convergence
            // observes it once while normal input remains live, then keeps
            // advancing actual cleanup without rewriting an accepted result.
            tracing::warn!(%error, "Native split assignment failed before strict drain closed");
            self.begin(round, split_delivery, NativeAttemptConvergenceMode::Cleanup);
            return Err(TaskExecutionError::SplitAssignment(error));
        }
        if round.pumps_closed() && self.observed_contexts(round).is_some() {
            return Ok(NativeAttemptConvergenceStep::Ready);
        }
        let advanced = if self.mode == Some(NativeAttemptConvergenceMode::Normal)
            && !round.execution().normal_drain_started()
        {
            // Required inputs retain the exact original bridge. The same
            // take/enqueue/admit path answers each immutable delivery once.
            advance_task_round(round, split_delivery)
        } else {
            round.turn()
        };
        match advanced {
            Ok(report) => {
                let effective = if round.failure_cause().is_some() {
                    NativeAttemptConvergenceMode::Cleanup
                } else {
                    effective
                };
                // This turn may have applied the final strict terminal and
                // started normal closure. Close the original bridge now.
                self.begin(round, split_delivery, effective);
                Ok(NativeAttemptConvergenceStep::Pending {
                    moved: !report.is_idle(),
                })
            }
            Err(error) => {
                if self.mode == Some(NativeAttemptConvergenceMode::Normal) {
                    tracing::warn!(%error, "Native convergence Task turn failed before cleanup");
                }
                self.begin(round, split_delivery, NativeAttemptConvergenceMode::Cleanup);
                Err(error)
            }
        }
    }

    /// Finishes local close/join independently of the bounded remote observation.
    /// The original owner retains the first deadline and remote disposition if
    /// this borrowed future is dropped. Neither cancellation nor that deadline
    /// can release a pump whose real close job has not exited.
    pub(crate) async fn drive(
        &mut self,
        round: &mut TaskRound,
        split_delivery: &SplitDeliveryBridge,
        mode: NativeAttemptConvergenceMode,
        request: NativeActiveConvergenceRequest,
        notify: &tokio::sync::Notify,
        mut profile: Option<&mut super::profile_diagnostics::TaskFinalInfoCollector>,
    ) -> Result<NativeActiveConvergenceClosure, StatementNativeHealthFailure> {
        let deadline = self.retain_remote_deadline(request.remote_observation_deadline());
        loop {
            let notified = notify.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            if let Some(profile) = profile.as_deref_mut() {
                let now = round.execution().monotonic_now();
                let cancelled = request.cancellation().reason().is_some();
                let round_poll = &mut *round;
                let profile_poll = &mut *profile;
                std::future::poll_fn(move |cx| {
                    profile_poll.poll_profile_observation(round_poll, now, cancelled, cx);
                    std::task::Poll::Ready(())
                })
                .await;
                if profile.final_info_closed() || profile.observation_deadline().is_none() {
                    round.execution_mut().release_profile_diagnostics();
                }
            }
            let effective =
                if request.cancellation().reason().is_some() || round.failure_cause().is_some() {
                    NativeAttemptConvergenceMode::Cleanup
                } else {
                    mode
                };
            self.begin(round, split_delivery, effective);
            self.observe_local_close(round)?;
            self.observe_remote_closure(round, deadline);
            if round.pumps_closed()
                && profile.as_ref().is_none_or(|profile| {
                    profile.is_closed() && profile.observation_deadline().is_none()
                })
                && let Some(closure) = &self.remote_closure
            {
                return Ok(closure.clone());
            }
            let moved = match self.step(
                round,
                split_delivery,
                mode,
                request.cancellation().reason().is_some(),
            ) {
                // A retained remote fact is not fresh progress while an
                // independent original diagnostic read still has to exit.
                Ok(NativeAttemptConvergenceStep::Ready) => false,
                Ok(NativeAttemptConvergenceStep::Pending { moved }) => moved,
                Err(_) => false,
            };
            self.observe_local_close(round)?;
            self.observe_remote_closure(round, deadline);
            if round.pumps_closed()
                && profile.as_ref().is_none_or(|profile| {
                    profile.is_closed() && profile.observation_deadline().is_none()
                })
                && let Some(closure) = &self.remote_closure
            {
                return Ok(closure.clone());
            }
            if moved {
                continue;
            }
            tokio::select! {
                _ = notified => {}
                _ = tokio::time::sleep(IDLE_RECHECK) => {}
            }
        }
    }

    /// Closes the SAME unstarted round without obtaining a remote budget.
    /// Every frozen context must still have its original never-issued proof;
    /// local jobs/diagnostics nevertheless have to actually close and join.
    pub(crate) async fn drive_unstarted(
        &mut self,
        round: &mut TaskRound,
        split_delivery: &SplitDeliveryBridge,
        notify: &tokio::sync::Notify,
        mut profile: Option<&mut super::profile_diagnostics::TaskFinalInfoCollector>,
    ) -> Result<(), StatementNativeHealthFailure> {
        if !round.pumps_declared() {
            return Err(self.retain_unstarted_failure(
                "Native dormant cleanup requires its original source declaration",
                "the original unstarted round never declared its per-turn owners",
            ));
        }
        self.verify_unstarted(round)?;
        self.begin(round, split_delivery, NativeAttemptConvergenceMode::Cleanup);
        if let Some(profile) = profile.as_deref_mut() {
            profile.stop_starting();
        }
        loop {
            let notified = notify.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            self.verify_unstarted(round)?;
            if let Some(profile) = profile.as_deref_mut() {
                let now = round.execution().monotonic_now();
                let round_poll = &mut *round;
                let profile_poll = &mut *profile;
                std::future::poll_fn(move |cx| {
                    profile_poll.poll_profile_observation(round_poll, now, true, cx);
                    std::task::Poll::Ready(())
                })
                .await;
                if profile.final_info_closed() || profile.observation_deadline().is_none() {
                    round.execution_mut().release_profile_diagnostics();
                }
            }
            self.observe_local_close(round)?;
            if round.pumps_closed()
                && profile
                    .as_ref()
                    .is_none_or(|p| p.is_closed() && p.observation_deadline().is_none())
            {
                // Reuse the original positive-fact author, then check the
                // exact nonempty frozen set; None/empty/remote timeout is not proof.
                let facts = self
                    .observed_contexts(round)
                    .ok_or_else(|| self.unstarted_failure())?;
                if facts.len() != self.targets.len()
                    || facts.iter().zip(self.targets.iter()).any(|(fact, target)| {
                        fact.context() != target.context
                            || fact.kind() != novarocks_query_application::api::NativeContextConvergenceKind::NeverEstablished
                    })
                {
                    return Err(self.unstarted_failure());
                }
                return Ok(());
            }
            let moved = match self.step(
                round,
                split_delivery,
                NativeAttemptConvergenceMode::Cleanup,
                true,
            ) {
                Ok(NativeAttemptConvergenceStep::Ready) => false,
                Ok(NativeAttemptConvergenceStep::Pending { moved }) => moved,
                Err(_) => false,
            };
            self.observe_local_close(round)?;
            self.verify_unstarted(round)?;
            if moved {
                continue;
            }
            // Same original attempt-local wait policy as drive(), with no
            // extra service, remote deadline, task, or independent timer owner.
            tokio::select! {
                _ = notified => {}
                _ = tokio::time::sleep(IDLE_RECHECK) => {}
            }
        }
    }

    fn verify_unstarted(&mut self, round: &TaskRound) -> Result<(), StatementNativeHealthFailure> {
        if let Some(original) = &self.local_unresolved {
            return Err(original.clone());
        }
        if self.targets.is_empty()
            || self
                .targets
                .iter()
                .any(|target| !round.execution().context_never_established(target.context))
        {
            return Err(self.unstarted_failure());
        }
        Ok(())
    }

    fn unstarted_failure(&mut self) -> StatementNativeHealthFailure {
        self.retain_unstarted_failure(
            "Native dormant cleanup requires every exact context to be never established",
            "the original unstarted round lost its exact never-established proof",
        )
    }

    pub(crate) fn retain_unstarted_gate_failure(&mut self) -> StatementNativeHealthFailure {
        self.retain_unstarted_failure(
            "Native dormant cleanup found outstanding actor transport authority",
            "the original unstarted actor gate still owns transport effects",
        )
    }

    fn retain_unstarted_failure(
        &mut self,
        detail: &'static str,
        message: &'static str,
    ) -> StatementNativeHealthFailure {
        // The same pre-existing first-health slot owns this Arc before any
        // borrowed F or CancellationView can Drop and raise a secondary panic.
        if let Some(original) = &self.local_unresolved {
            return original.clone();
        }
        let failure = StatementNativeHealthFailure::actual_exit_unproven(
            detail,
            std::sync::Arc::new(TaskExecutionError::Schedule(message.to_owned())),
        );
        self.local_unresolved = Some(failure.clone());
        failure
    }

    fn observe_local_close(
        &mut self,
        round: &mut TaskRound,
    ) -> Result<(), StatementNativeHealthFailure> {
        if let Some(error) = &self.local_unresolved {
            return Err(error.clone());
        }
        while let Some(error) = round.take_pump_close_error() {
            // The business terminal was already selected before convergence.
            // A joined close error is diagnostic, not a second Task verdict.
            tracing::warn!(%error, "Native attempt local pump cleanup reported an error");
        }
        if let Some(unresolved) = round.take_pump_close_unresolved() {
            let error = StatementNativeHealthFailure::actual_exit_unproven(
                "the original Native attempt pump close job did not exit normally",
                unresolved.cause,
            );
            self.local_unresolved = Some(error.clone());
            return Err(error);
        }
        Ok(())
    }

    fn retain_remote_deadline(&mut self, incoming: std::time::Instant) -> std::time::Instant {
        let deadline = self
            .remote_deadline
            .map_or(incoming, |original| original.min(incoming));
        // Only the trusted original owner's shutdown request can shorten its
        // remote budget. Re-entry can never extend or re-borrow that budget.
        self.remote_deadline = Some(deadline);
        deadline
    }

    fn observe_remote_closure(&mut self, round: &TaskRound, deadline: std::time::Instant) {
        if self.remote_closure.is_some() {
            return;
        }
        // A budget bounds further waiting; it cannot erase positive facts
        // already accepted by the original covered/Release owners.
        if let Some(contexts) = self.observed_contexts(round) {
            self.remote_closure = Some(NativeActiveConvergenceClosure::Evidence(
                NativeAttemptConvergence::contexts(contexts),
            ));
        } else if std::time::Instant::now() >= deadline {
            self.remote_closure = Some(NativeActiveConvergenceClosure::RemoteTrackingEnded {
                exact_contexts: self.targets.iter().map(|target| target.context).collect(),
            });
        }
    }
}

/// Split updates enter the original serial Task owner before its next turn.
/// Actor callers settle their original admission gate after this helper.
pub(crate) fn advance_task_round(
    round: &mut TaskRound,
    split_delivery: &SplitDeliveryBridge,
) -> Result<TurnReport, TaskExecutionError> {
    for pending in split_delivery.take_pending() {
        let delivery = pending.delivery();
        let task = pending.task();
        let admitted = round
            .execution_mut()
            .enqueue_task_update(task, pending.into_update());
        let reported = match &admitted {
            Ok(admission) => Ok(*admission),
            Err(error) => Err(error),
        };
        split_delivery
            .admit(delivery, reported)
            .map_err(|error| TaskExecutionError::Schedule(error.to_string()))?;
        admitted?;
    }
    round.turn()
}

pub(super) fn begin_convergence(
    round: &mut TaskRound,
    split_delivery: &SplitDeliveryBridge,
    mode: NativeAttemptConvergenceMode,
) -> bool {
    match mode {
        NativeAttemptConvergenceMode::Normal => {
            round.begin_normal_drain();
            if !round.execution().normal_drain_started() {
                return false;
            }
            split_delivery.abandon("Native attempt entered normal drain");
        }
        NativeAttemptConvergenceMode::Cleanup => {
            round.begin_terminal_cleanup();
            split_delivery.abandon("Native attempt entered convergence");
        }
    }
    true
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    use super::super::intent::{AckPayload, OperationAcknowledgement, OperationIntent};
    use super::super::status_intake::{ObservationFrame, ObservationIntakeEntry};
    use novarocks_execution_contract::{
        OperationKind, OperationOutcome, QueryContextConvergenceReceipt,
        QueryContextConvergenceState, QueryContextConvergenceVersion, QueryContextReceipt,
        QueryContextState, ReleaseOutcome,
    };
    use novarocks_query_application::api::{BackendProcessObservationPort, BackendTopologyError};
    use novarocks_task_codec::operation::{CoveredStatusStreamEvent, CoveredStatusStreamFact};

    struct OriginalProcess;

    impl BackendProcessObservationPort for OriginalProcess {
        fn observe_process_at_endpoint(
            &self,
            _: novarocks_types::BackendProcessId,
            _: &RuntimeEndpoint,
        ) -> Result<BackendProcessObservation, BackendTopologyError> {
            Ok(BackendProcessObservation::Current)
        }
    }

    fn publish_exit(round: &mut TaskRound, context: QueryContextRef) {
        round
            .execution_mut()
            .apply_covered_entries(vec![ObservationIntakeEntry::Frame(Box::new(
                ObservationFrame::Covered {
                    context,
                    generation: 1,
                    event: CoveredStatusStreamEvent {
                        fact: CoveredStatusStreamFact::ContextConvergence(
                            QueryContextConvergenceReceipt::new(
                                context,
                                QueryContextConvergenceVersion::FIRST,
                                QueryContextConvergenceState::WorkerStoppedAndContextFenced,
                            ),
                        ),
                        source_revision: None,
                    },
                },
            ))])
            .unwrap();
    }

    #[test]
    fn shared_convergence_rejects_missing_duplicate_and_foreign_context_targets() {
        let (round, _) = super::super::tests::manifest_terminal_regression_fixture();
        let contexts = round
            .execution()
            .graph()
            .contexts()
            .copied()
            .collect::<Vec<_>>();
        let targets = contexts
            .iter()
            .map(|&context| {
                NativeAttemptConvergenceTarget::new(
                    context,
                    RuntimeEndpoint::new("127.0.0.1", 1).unwrap(),
                )
            })
            .collect::<Vec<_>>();
        assert!(
            NativeAttemptConvergenceOwner::new(&round, Arc::new(OriginalProcess), targets.clone())
                .is_ok()
        );
        assert!(
            NativeAttemptConvergenceOwner::new(&round, Arc::new(OriginalProcess), Vec::new())
                .is_err()
        );
        let mut duplicate = targets.clone();
        duplicate.push(targets[0].clone());
        assert!(
            NativeAttemptConvergenceOwner::new(&round, Arc::new(OriginalProcess), duplicate)
                .is_err()
        );
        let mut missing = targets.clone();
        missing.pop();
        assert!(
            NativeAttemptConvergenceOwner::new(&round, Arc::new(OriginalProcess), missing).is_err()
        );
        let mut foreign = targets;
        foreign[0].context = QueryContextRef::new(
            contexts[0].query_execution_id(),
            contexts[0].frontend_process_id(),
            novarocks_types::BackendProcessId::new_v7(),
        );
        assert!(
            NativeAttemptConvergenceOwner::new(&round, Arc::new(OriginalProcess), foreign).is_err()
        );
    }

    #[test]
    fn statement_bridge_shared_step_joins_original_release_and_covered_exit_in_both_orders() {
        let expected = super::super::tests::fixture_runtime_filter_contribution();
        for covered_first in [true, false] {
            // This is the original directly dispatched TaskRound. No actor
            // gate, NativeAttemptDrive or replacement WorkOwner is constructed.
            let (mut round, acks, release) =
                super::super::tests::manifest_normal_release_ack_regression_fixture();
            let context = *round.execution().graph().contexts().next().unwrap();
            let split_delivery = SplitDeliveryBridge::for_graph(round.execution().graph());
            let mut convergence = NativeAttemptConvergenceOwner::new(
                &round,
                Arc::new(OriginalProcess),
                vec![NativeAttemptConvergenceTarget::new(
                    context,
                    RuntimeEndpoint::new("127.0.0.1", 1).unwrap(),
                )],
            )
            .unwrap();
            let OperationIntent::ReleaseQueryContext(request) = &release else {
                unreachable!()
            };
            let ack = OperationAcknowledgement::new(
                release.operation_id(),
                OperationKind::ReleaseQueryContext,
                OperationOutcome::Accepted,
                AckPayload::Release {
                    receipt: QueryContextReceipt::new(
                        request.context(),
                        QueryContextState::TerminalRetained,
                    ),
                    outcome: ReleaseOutcome::Released,
                    runtime_filter: Some(expected.clone()),
                },
            );
            convergence.begin(
                &mut round,
                &split_delivery,
                NativeAttemptConvergenceMode::Normal,
            );
            assert!(convergence.observed_contexts(&round).is_none());
            if covered_first {
                publish_exit(&mut round, context);
                assert!(matches!(
                    convergence
                        .step(
                            &mut round,
                            &split_delivery,
                            NativeAttemptConvergenceMode::Normal,
                            false,
                        )
                        .unwrap(),
                    NativeAttemptConvergenceStep::Pending { .. }
                ));
                acks.publish(ack);
                let _ = convergence
                    .step(
                        &mut round,
                        &split_delivery,
                        NativeAttemptConvergenceMode::Normal,
                        false,
                    )
                    .unwrap();
            } else {
                acks.publish(ack);
                assert!(matches!(
                    convergence
                        .step(
                            &mut round,
                            &split_delivery,
                            NativeAttemptConvergenceMode::Normal,
                            false,
                        )
                        .unwrap(),
                    NativeAttemptConvergenceStep::Pending { .. }
                ));
                assert!(round.execution().owner(context).unwrap().is_released());
                assert!(!round.execution().worker_contexts_stopped());
                publish_exit(&mut round, context);
            }
            let NativeAttemptConvergenceStep::Ready = convergence
                .step(
                    &mut round,
                    &split_delivery,
                    NativeAttemptConvergenceMode::Normal,
                    false,
                )
                .unwrap()
            else {
                panic!("both original facts must make this same owner convergent")
            };
            let contribution = round.execution().released_runtime_filter_contributions();
            assert!(contribution.is_complete());
            assert_eq!(
                contribution.contributions(),
                &[(context.backend_process_id(), expected.clone())]
            );
        }
    }

    #[test]
    fn statement_bridge_shared_step_does_not_turn_abort_closure_into_actual_exit() {
        let (mut round, _acks) = super::super::tests::manifest_terminal_regression_fixture();
        let context = *round.execution().graph().contexts().next().unwrap();
        let split_delivery = SplitDeliveryBridge::for_graph(round.execution().graph());
        let mut convergence = NativeAttemptConvergenceOwner::new(
            &round,
            Arc::new(OriginalProcess),
            vec![NativeAttemptConvergenceTarget::new(
                context,
                RuntimeEndpoint::new("127.0.0.1", 1).unwrap(),
            )],
        )
        .unwrap();
        convergence.begin(
            &mut round,
            &split_delivery,
            NativeAttemptConvergenceMode::Cleanup,
        );
        round
            .execution_mut()
            .observe_actor_abort_context_closed(context)
            .unwrap();
        assert!(round.execution().owner(context).unwrap().is_released());
        assert!(convergence.observed_contexts(&round).is_none());
        assert!(matches!(
            convergence
                .step(
                    &mut round,
                    &split_delivery,
                    NativeAttemptConvergenceMode::Cleanup,
                    true,
                )
                .unwrap(),
            NativeAttemptConvergenceStep::Pending { .. }
        ));
        assert!(!round.execution().worker_contexts_stopped());
        publish_exit(&mut round, context);
        assert!(matches!(
            convergence
                .step(
                    &mut round,
                    &split_delivery,
                    NativeAttemptConvergenceMode::Cleanup,
                    true,
                )
                .unwrap(),
            NativeAttemptConvergenceStep::Ready
        ));
        assert!(round.execution().worker_contexts_stopped());
    }

    struct OriginalCloseResource(Arc<std::sync::atomic::AtomicBool>);
    impl Drop for OriginalCloseResource {
        fn drop(&mut self) {
            self.0.store(true, std::sync::atomic::Ordering::SeqCst);
        }
    }

    struct GatedOriginalClose {
        start: Option<std::sync::mpsc::Sender<()>>,
        release: std::sync::mpsc::Sender<()>,
        job: Option<std::thread::JoinHandle<()>>,
    }
    impl super::super::round::TurnPump for GatedOriginalClose {
        fn name(&self) -> &'static str {
            "original-gated-close"
        }
        fn drive(
            &mut self,
            _: &mut super::super::execution::QueryTaskExecution,
        ) -> Result<usize, TaskExecutionError> {
            Ok(0)
        }
        fn begin_close(&mut self) {
            if let Some(start) = self.start.take() {
                let _ = start.send(());
            }
        }
        fn close_turn(&mut self) -> Result<usize, TaskExecutionError> {
            if self
                .job
                .as_ref()
                .is_some_and(std::thread::JoinHandle::is_finished)
            {
                self.job.take().unwrap().join().unwrap();
                Ok(1)
            } else {
                Ok(0)
            }
        }
        fn is_closed(&self) -> bool {
            self.job.is_none()
        }
        fn take_close_unresolved(&mut self) -> Option<super::super::round::PumpCloseUnresolved> {
            None
        }
    }
    impl Drop for GatedOriginalClose {
        fn drop(&mut self) {
            // A failed assertion must still release and join this real fixture job.
            <Self as super::super::round::TurnPump>::begin_close(self);
            let _ = self.release.send(());
            if let Some(job) = self.job.take() {
                job.join().unwrap();
            }
        }
    }
    fn install_gated_close(
        round: &mut TaskRound,
    ) -> (
        std::sync::mpsc::Receiver<()>,
        std::sync::mpsc::Sender<()>,
        Arc<std::sync::atomic::AtomicBool>,
    ) {
        let (start, started) = std::sync::mpsc::channel();
        let (entered, entry) = std::sync::mpsc::channel();
        let (release, released) = std::sync::mpsc::channel();
        let dropped = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let resource = OriginalCloseResource(Arc::clone(&dropped));
        let job = std::thread::spawn(move || {
            started.recv().unwrap();
            entered.send(()).unwrap();
            released.recv().unwrap();
            drop(resource);
        });
        round.add_pump(Box::new(GatedOriginalClose {
            start: Some(start),
            release: release.clone(),
            job: Some(job),
        }));
        (entry, release, dropped)
    }
    fn original_convergence(round: &TaskRound) -> NativeAttemptConvergenceOwner {
        NativeAttemptConvergenceOwner::new(
            round,
            Arc::new(OriginalProcess),
            round
                .execution()
                .graph()
                .contexts()
                .map(|&context| {
                    NativeAttemptConvergenceTarget::new(
                        context,
                        RuntimeEndpoint::new("127.0.0.1", 1).unwrap(),
                    )
                })
                .collect(),
        )
        .unwrap()
    }

    #[test]
    fn remote_ready_keeps_the_same_round_until_its_original_close_job_joins() {
        use std::sync::atomic::Ordering;
        let (mut round, _) = super::super::tests::manifest_terminal_regression_fixture();
        let (entry, release, dropped) = install_gated_close(&mut round);
        let context = *round.execution().graph().contexts().next().unwrap();
        let split_delivery = SplitDeliveryBridge::for_graph(round.execution().graph());
        let mut owner = original_convergence(&round);
        owner.begin(
            &mut round,
            &split_delivery,
            NativeAttemptConvergenceMode::Cleanup,
        );
        entry
            .recv_timeout(std::time::Duration::from_secs(5))
            .unwrap();
        publish_exit(&mut round, context);
        assert!(owner.observed_contexts(&round).is_some());
        let pending = matches!(
            owner
                .step(
                    &mut round,
                    &split_delivery,
                    NativeAttemptConvergenceMode::Cleanup,
                    false
                )
                .unwrap(),
            NativeAttemptConvergenceStep::Pending { .. }
        );
        let held = !dropped.load(Ordering::SeqCst);
        release.send(()).unwrap();
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
        loop {
            if matches!(
                owner
                    .step(
                        &mut round,
                        &split_delivery,
                        NativeAttemptConvergenceMode::Cleanup,
                        false
                    )
                    .unwrap(),
                NativeAttemptConvergenceStep::Ready
            ) {
                break;
            }
            assert!(
                std::time::Instant::now() < deadline,
                "the released original job must join"
            );
            std::thread::yield_now();
        }
        assert!(pending, "remote proof alone must not retire the pump owner");
        assert!(
            held,
            "the original close resource must remain behind its real gate"
        );
        assert!(dropped.load(Ordering::SeqCst));
        assert!(round.pumps_closed());
    }

    #[tokio::test]
    async fn remote_deadline_and_borrowed_future_drop_do_not_retire_the_original_local_close() {
        use std::sync::atomic::Ordering;
        let (mut round, _) = super::super::tests::manifest_terminal_regression_fixture();
        let (entry, release, dropped) = install_gated_close(&mut round);
        let context = *round.execution().graph().contexts().next().unwrap();
        let split_delivery = SplitDeliveryBridge::for_graph(round.execution().graph());
        let mut owner = original_convergence(&round);
        let notify = tokio::sync::Notify::new();
        let control = novarocks_workload_control::WorkloadControl::try_new(
            novarocks_workload_control::WorkloadConfig::default(),
            novarocks_workload_control::ResourceConfig {
                total_bytes: 1 << 20,
                control_bytes: 1 << 10,
                per_scope_bytes: 1 << 18,
            },
        )
        .unwrap();
        control.mark_ready().unwrap();
        let root = control
            .try_begin_root(novarocks_workload_control::WorkRequest::new(
                novarocks_workload_control::WorkClass::Query,
            ))
            .unwrap();
        let cancellation = root.owner.scope().cancellation().unwrap();
        let request = NativeActiveConvergenceRequest::new(
            cancellation.clone(),
            std::time::Instant::now() + std::time::Duration::from_secs(60),
        );
        let pending = tokio::time::timeout(
            std::time::Duration::from_millis(20),
            owner.drive(
                &mut round,
                &split_delivery,
                NativeAttemptConvergenceMode::Cleanup,
                request,
                &notify,
                None,
            ),
        )
        .await
        .is_err();
        entry
            .recv_timeout(std::time::Duration::from_secs(5))
            .unwrap();
        let held = !dropped.load(Ordering::SeqCst);
        // The original supervisor may shorten only remote observation for
        // shutdown. It still cannot return while this local job is gated.
        let shutdown_pending = tokio::time::timeout(
            std::time::Duration::from_millis(20),
            owner.drive(
                &mut round,
                &split_delivery,
                NativeAttemptConvergenceMode::Cleanup,
                NativeActiveConvergenceRequest::new(
                    cancellation.clone(),
                    std::time::Instant::now(),
                ),
                &notify,
                None,
            ),
        )
        .await
        .is_err();
        // Evidence arriving after the original deadline must not turn an
        // await-drop/re-entry into a fresh remote tracking budget.
        publish_exit(&mut round, context);
        release.send(()).unwrap();
        let closure = tokio::time::timeout(
            std::time::Duration::from_secs(5),
            owner.drive(
                &mut round,
                &split_delivery,
                NativeAttemptConvergenceMode::Cleanup,
                NativeActiveConvergenceRequest::new(
                    cancellation,
                    std::time::Instant::now() + std::time::Duration::from_secs(60),
                ),
                &notify,
                None,
            ),
        )
        .await
        .unwrap()
        .unwrap();
        assert!(pending);
        assert!(
            shutdown_pending,
            "remote shutdown budget cannot retire local work"
        );
        assert!(held);
        assert!(dropped.load(Ordering::SeqCst));
        assert!(round.pumps_closed());
        match closure {
            NativeActiveConvergenceClosure::RemoteTrackingEnded { exact_contexts } => {
                assert_eq!(exact_contexts.as_ref(), &[context])
            }
            NativeActiveConvergenceClosure::Evidence(_) => {
                panic!("late evidence cannot refresh the original remote deadline")
            }
        }
    }

    #[test]
    fn expired_remote_budget_keeps_original_accepted_exit_and_release_evidence() {
        let (mut round, acks, release) =
            super::super::tests::manifest_normal_release_ack_regression_fixture();
        let context = *round.execution().graph().contexts().next().unwrap();
        let split_delivery = SplitDeliveryBridge::for_graph(round.execution().graph());
        let mut owner = original_convergence(&round);
        owner.begin(
            &mut round,
            &split_delivery,
            NativeAttemptConvergenceMode::Normal,
        );
        let contribution = super::super::tests::fixture_runtime_filter_contribution();
        let OperationIntent::ReleaseQueryContext(request) = &release else {
            unreachable!("the original fixture selects its exact Release")
        };
        acks.publish(OperationAcknowledgement::new(
            release.operation_id(),
            OperationKind::ReleaseQueryContext,
            OperationOutcome::Accepted,
            AckPayload::Release {
                receipt: QueryContextReceipt::new(
                    request.context(),
                    QueryContextState::TerminalRetained,
                ),
                outcome: ReleaseOutcome::Released,
                runtime_filter: Some(contribution.clone()),
            },
        ));
        round
            .turn()
            .expect("the original Release acknowledgement settles");
        publish_exit(&mut round, context);
        assert!(round.attempt_drained());
        assert!(
            owner.observed_contexts(&round).is_some(),
            "positive original facts precede the expired request"
        );
        let expired = std::time::Instant::now() - std::time::Duration::from_secs(1);
        let deadline = owner.retain_remote_deadline(expired);
        owner.observe_remote_closure(&round, deadline);
        let closure = owner
            .remote_closure
            .clone()
            .expect("already accepted evidence");
        match closure {
            NativeActiveConvergenceClosure::Evidence(NativeAttemptConvergence::Contexts(
                contexts,
            )) => {
                assert_eq!(
                    contexts.as_ref(),
                    &[NativeContextConvergence::worker_stopped_and_context_fenced(
                        context
                    )]
                );
            }
            other => panic!("an expired wait must not erase original positive facts: {other:?}"),
        }
        // A later shutdown/re-entry cannot replace the previously frozen
        // positive evidence with a second remote disposition either.
        let deadline = owner.retain_remote_deadline(expired - std::time::Duration::from_secs(1));
        owner.observe_remote_closure(&round, deadline);
        assert!(matches!(
            owner.remote_closure.clone(),
            Some(NativeActiveConvergenceClosure::Evidence(_))
        ));
        let contributions = round.execution().released_runtime_filter_contributions();
        assert!(contributions.is_complete());
        assert_eq!(
            contributions.contributions(),
            &[(context.backend_process_id(), contribution)]
        );
    }

    include!("attempt_convergence_diagnostic_exit_tests.rs");
    include!("attempt_convergence_split_failure_tests.rs");
}
