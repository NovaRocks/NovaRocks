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

use super::*;
use crate::{
    cancel::cancellation_decision,
    convergence::{CleanupIntent, CleanupPoll},
    ready::ConvergenceSnapshot,
    stand_down::{AbortPoll, ParticipationStandDownCause, StandDownMode},
    state::ExecutionPhase,
};
use novarocks_coordination_contract::logical_port::LogicalToAttemptBody;
use std::ops::Bound::{Excluded, Unbounded};

impl LogicalExecutionCore {
    pub(super) fn cancel(&mut self, reason: LogicalCancellationReason) {
        if self.cancellation.is_some() {
            return;
        }
        self.scope_cancellation_requested = false;
        // The original scope retains late causes for the outer statement.
        // A fixed success or Ready promise cannot select a new attempt stop.
        if self.state.conclusion().is_some() {
            return;
        }
        self.cancellation = Some(reason);
        self.refresh_pre_ready = true;
        self.pre_ready_cursor = None;
        let decision = cancellation_decision(reason);
        self.commit_failure(decision.conclusion, decision.failure);
    }
    pub(super) fn conclude_failure(&mut self, error: QueryExecutionError) {
        self.conclude_failure_publishing(error.clone(), error);
    }
    pub(super) fn conclude_replacement_failure(&mut self, error: QueryExecutionError) {
        if self.state.conclusion().is_some() {
            return;
        }
        let original = self
            .recovery
            .as_ref()
            .expect("replacement setup retains its originating attempt failure")
            .failure
            .error()
            .clone();
        self.conclude_setup_failure(error, original);
    }
    pub(super) fn conclude_setup_failure(
        &mut self,
        error: QueryExecutionError,
        original: QueryExecutionError,
    ) {
        use novarocks_coordination_contract::AttemptFailureClass;
        if self.state.conclusion().is_some() {
            return;
        }
        // This closed projection verifies the original coordinator author,
        // originating evidence and exact scope cancellation/deadline reason.
        // It is not a guess from a generic Control class or error rendering.
        let scope_cause = error.execution_failure().is_some_and(|input| {
            matches!(
                input.attempt_class(),
                AttemptFailureClass::Cancelled | AttemptFailureClass::DeadlineExceeded
            )
        });
        if scope_cause {
            // Native observed the same original latch, but its error cannot
            // reconstruct the exact reason (for example a drain deadline).
            // The exclusive observer supplies that reason and its one Stop.
            self.scope_cancellation_requested = true;
            return;
        }
        let published = if self.start.policy.output == LogicalOutputMode::ResultStream {
            original
        } else {
            error.clone()
        };
        self.conclude_failure_publishing(error, published);
    }
    fn conclude_failure_publishing(
        &mut self,
        error: QueryExecutionError,
        published: QueryExecutionError,
    ) {
        if self.state.conclusion().is_some() {
            return;
        }
        let conclusion = if error.kind()
            == novarocks_coordination_contract::QueryExecutionErrorKind::Cancelled
        {
            LogicalConclusion::Cancelled
        } else {
            LogicalConclusion::Failed
        };
        self.commit_failure_publishing(conclusion, error, published);
    }
    fn commit_failure(&mut self, conclusion: LogicalConclusion, error: QueryExecutionError) {
        self.commit_failure_publishing(conclusion, error.clone(), error);
    }
    fn commit_failure_publishing(
        &mut self,
        conclusion: LogicalConclusion,
        error: QueryExecutionError,
        published: QueryExecutionError,
    ) {
        match self.state.phase() {
            ExecutionPhase::Instantiating { activation }
            | ExecutionPhase::Running { activation } => self
                .state
                .conclude(activation, conclusion)
                .expect("the serial current owner concludes once"),
            ExecutionPhase::Replacing { identity } => self
                .state
                .conclude_replacement(identity, conclusion)
                .expect("the serial replacement owner concludes once"),
            ExecutionPhase::Converging => return,
        }
        self.logical_failure = Some(error.clone());
        self.reject_pending_result_requests(error.clone());
        self.queue(LogicalEffectBody::Consumer(ConsumerEffect::PublishFailure(
            published,
        )));
        self.begin_conclusion();
    }
    pub(super) fn begin_conclusion(&mut self) {
        self.closing_pass = true;
        self.closing_cursor = None;
        self.refresh_pre_ready = true;
        self.pre_ready_cursor = None;
    }
    pub(super) fn freeze_terminal_cut(&mut self, activation: AttemptActivationIdentity) {
        let now = self.now;
        let timeout = self.start.budgets.remote_cleanup_timeout;
        let ready = self
            .ready
            .as_ref()
            .and_then(InternalReady::accepted)
            .filter(|a| a.cut.activation == activation);
        let attempt = self
            .attempts
            .get_mut(&activation.execution())
            .expect("terminal cut retains its original attempt");
        if attempt.terminal_cut.is_some() {
            return;
        }
        let deadline = ready.map_or_else(
            || now.saturating_add(timeout),
            |r| r.cut.convergence_deadline,
        );
        let deadline = self
            .close_deadline
            .map_or(deadline, |bound| bound.min(deadline));
        attempt.terminal_cut = Some(now);
        attempt.remote_deadline = Some(deadline);
        self.queue(LogicalEffectBody::Native(NativeEffect::RecordTerminalCut {
            activation,
            cut: now,
            remote_deadline: deadline,
        }));
    }
    pub(super) fn begin_attempt_convergence(
        &mut self,
        activation: AttemptActivationIdentity,
        mode: AttemptConvergenceMode,
    ) {
        self.freeze_terminal_cut(activation);
        let attempt = self.attempts.get_mut(&activation.execution()).unwrap();
        if attempt.closing.is_some() {
            return;
        }
        attempt.establish.close_authority();
        attempt.closing = Some(mode);
        let deadline = attempt
            .remote_deadline
            .expect("convergence has its original terminal cut");
        let stand_down = if mode == AttemptConvergenceMode::Normal {
            StandDownMode::SuccessfulCleanup
        } else {
            StandDownMode::Abort(if self.cancellation.is_some() {
                ParticipationStandDownCause::LogicalExecutionCancelled
            } else {
                ParticipationStandDownCause::LogicalExecutionFailed
            })
        };
        attempt
            .stand_down
            .begin(stand_down, deadline)
            .expect("one original stand-down begins once");
        attempt.convergence.begin(mode, deadline);
        if !attempt.relay_issued {
            attempt.convergence.observe_relay_stopped(true);
        }
        if mode == AttemptConvergenceMode::Cleanup {
            self.stop_relay(activation);
        }
    }
    pub(super) fn stop_relay(&mut self, activation: AttemptActivationIdentity) {
        let attempt = self
            .attempts
            .get_mut(&activation.execution())
            .expect("relay refers to one original attempt");
        if !attempt.relay_issued || attempt.relay_stopping {
            return;
        }
        attempt.relay_stopping = true;
        self.queue(LogicalEffectBody::Relay(RelayEffect::Stop { activation }));
    }
    fn notify_convergence(&mut self, execution: QueryExecutionId) {
        let cancelled = self.cancellation.map(cancellation_decision);
        let cut = self
            .ready
            .as_ref()
            .and_then(InternalReady::accepted)
            .map(|a| a.cut);
        let retain_diagnostics = self.start.policy.retain_diagnostics;
        let attempt = self.attempts.get_mut(&execution).unwrap();
        if !attempt.activated || attempt.closing.is_none() {
            return;
        }
        let activation = attempt.activation;
        let stop =
            if !attempt.stop_notified && attempt.closing == Some(AttemptConvergenceMode::Cleanup) {
                cancelled.inspect(|_| {
                    attempt.stop_notified = true;
                })
            } else {
                None
            };
        let begin = if !attempt.closing_notified {
            attempt.closing_notified = true;
            Some((attempt.closing.unwrap(), attempt.remote_deadline.unwrap()))
        } else {
            None
        };
        if let Some(c) = stop {
            let failure = ExecutionFailure::from_input(
                c.failure
                    .execution_failure()
                    .expect("cancellation has its original typed fact")
                    .clone(),
            );
            self.attempt_reply(
                activation,
                LogicalToAttemptBody::Stop {
                    reason: c.stop,
                    failure,
                },
            );
        }
        if let Some((mode, remote_deadline)) = begin {
            self.attempt_reply(
                activation,
                LogicalToAttemptBody::BeginConvergence {
                    mode,
                    remote_deadline,
                    computation_ready: cut.filter(|c| c.activation == activation),
                    retain_diagnostics,
                },
            );
        }
    }
    pub(super) fn drive(&mut self, budget: &mut dyn StepBudget) -> Result<(), CoreFault> {
        if self.supervision_failure.is_some() {
            return Ok(());
        }
        while self.shortening_pass {
            let Some(execution) = self.next_attempt(self.shortening_cursor) else {
                self.shortening_pass = false;
                self.shortening_cursor = None;
                break;
            };
            if !budget.charge(1) {
                self.continuation = true;
                return Ok(());
            }
            self.shortening_cursor = Some(execution);
            let bound = self
                .close_deadline
                .expect("registry close has its original monotonic bound");
            let attempt = self.attempts.get_mut(&execution).unwrap();
            if attempt.diagnostic_emitted {
                continue;
            }
            if let Some(old) = attempt.remote_deadline
                && bound < old
            {
                attempt.remote_deadline = Some(bound);
                if let Some(mode) = attempt.closing {
                    attempt.convergence.begin(mode, bound);
                    attempt.stand_down.shorten_remote_deadline(bound);
                    attempt.closing_notified = false;
                }
                let activation = attempt.activation;
                let cut = attempt
                    .terminal_cut
                    .expect("existing observation bound retains its first cut");
                self.queue(LogicalEffectBody::Native(NativeEffect::RecordTerminalCut {
                    activation,
                    cut,
                    remote_deadline: bound,
                }));
            }
        }
        while self.closing_pass {
            let next = self.next_attempt(self.closing_cursor);
            let Some(execution) = next else {
                self.closing_pass = false;
                self.closing_cursor = None;
                break;
            };
            if !budget.charge(1) {
                self.continuation = true;
                return Ok(());
            }
            let activation = self.attempts.get(&execution).unwrap().activation;
            let mode = if matches!(
                self.state.conclusion(),
                Some(LogicalConclusion::Succeeded | LogicalConclusion::ComputationReady)
            ) {
                AttemptConvergenceMode::Normal
            } else {
                AttemptConvergenceMode::Cleanup
            };
            self.begin_attempt_convergence(activation, mode);
            self.closing_cursor = Some(execution);
        }
        self.drive_recovery_cleanup();
        // The original preparation factory is no longer needed after the
        // business cut. Live attempts and aliases retain their own resources;
        // waiting for their convergence would retain unused preparation.
        // A pending candidate must first leave its original dormant custody.
        if self.state.conclusion().is_some()
            && !self.preparation_retiring
            && self.recovery_resources_closed()
        {
            self.preparation_retiring = true;
            self.queue(LogicalEffectBody::Native(NativeEffect::RetirePreparation));
        }
        self.drive_recovery(budget)?;
        loop {
            let Some(execution) = self.next_attempt(self.scan_cursor) else {
                self.scan_cursor = None;
                if std::mem::take(&mut self.scan_dirty) {
                    self.continuation = true;
                }
                break;
            };
            if !budget.charge(1) {
                self.continuation = true;
                return Ok(());
            }
            self.drive_attempt(execution, budget)?;
            self.scan_cursor = Some(execution);
            if !self.outbox.is_empty() {
                self.continuation = true;
                return Ok(());
            }
        }
        self.drive_results(budget)?;
        self.drive_pre_ready(budget)?;
        self.drive_observations();
        self.drive_final();
        Ok(())
    }
    fn next_attempt(&self, after: Option<QueryExecutionId>) -> Option<QueryExecutionId> {
        match after {
            Some(key) => self
                .attempts
                .range((Excluded(key), Unbounded))
                .next()
                .map(|(key, _)| *key),
            None => self.attempts.first_key_value().map(|(key, _)| *key),
        }
    }

    fn drive_attempt(
        &mut self,
        execution: QueryExecutionId,
        budget: &mut dyn StepBudget,
    ) -> Result<(), CoreFault> {
        let attempt = self.attempts.get_mut(&execution).unwrap();
        if let Some((withdrawals, index)) = attempt.pending_withdrawals.as_mut() {
            if !budget.charge(1) {
                self.continuation = true;
                return Ok(());
            }
            if let Some(withdrawal) = withdrawals.get(*index) {
                if withdrawal.activation() != attempt.activation
                    || attempt
                        .root
                        .as_ref()
                        .is_none_or(|r| r.root() != withdrawal.root())
                    || withdrawal.participation().backend_process_id()
                        == withdrawal.root().backend_process_id()
                {
                    return Err(CoreFault::UnexpectedInput);
                }
                attempt
                    .establish
                    .record_result_withdrawal(withdrawal.participation())
                    .map_err(|_| CoreFault::UnexpectedInput)?;
                *index += 1;
                self.continuation = true;
                return Ok(());
            }
            attempt.pending_withdrawals = None;
        }
        if attempt.closing.is_none() {
            if self.state.conclusion().is_none()
                && attempt.activated
                && !attempt.relay_started
                && self.start.policy.output != LogicalOutputMode::CompletionOnly
                && attempt.root.as_ref().is_some_and(RootControl::readable)
                && !self.visibility.frozen(attempt.activation)
            {
                attempt.relay_started = true;
                let activation = attempt.activation;
                self.queue(LogicalEffectBody::Relay(RelayEffect::Start { activation }));
            }
            self.drive_root_control(execution);
            return Ok(());
        }
        if !attempt.initialized {
            if !budget.charge(1) {
                self.continuation = true;
                return Ok(());
            }
            let next = match attempt.initialization_cursor {
                Some(p) => attempt
                    .establish
                    .required_participations()
                    .range((Excluded(p), Unbounded))
                    .next()
                    .copied(),
                None => attempt.establish.required_participations().first().copied(),
            };
            if let Some(p) = next {
                let fact = attempt
                    .establish
                    .stand_down_fact(p)
                    .map_err(|_| CoreFault::UnexpectedInput)?;
                attempt
                    .stand_down
                    .initialize_participation(p, fact)
                    .map_err(|_| CoreFault::UnexpectedInput)?;
                attempt.initialization_cursor = Some(p);
                self.continuation = true;
                return Ok(());
            }
            attempt
                .stand_down
                .finish_initialization()
                .map_err(|_| CoreFault::UnexpectedInput)?;
            attempt.initialized = true;
        }
        if let Some((p, fact)) = attempt.pending_registry.pop_first() {
            if !budget.charge(1) {
                attempt.pending_registry.insert(p, fact);
                self.continuation = true;
                return Ok(());
            }
            attempt
                .stand_down
                .observe_registry(self.now, p, fact)
                .map_err(|_| CoreFault::UnexpectedInput)?;
            self.continuation = true;
            return Ok(());
        }
        if attempt.closure.is_some() {
            if !budget.charge(1) {
                self.continuation = true;
                return Ok(());
            }
            Self::advance_closure(attempt, self.now)?;
            if attempt.closure.is_some() {
                self.continuation = true;
                return Ok(());
            }
        }
        let deadline = attempt
            .remote_deadline
            .expect("closing attempt retains its original observation bound");
        if self.now >= deadline
            && !attempt.tracking_pass_done
            && !attempt.stand_down.progress().tracking_complete
        {
            if !budget.charge(1) {
                self.continuation = true;
                return Ok(());
            }
            let next = match attempt.tracking_cursor {
                Some(p) => attempt
                    .establish
                    .required_participations()
                    .range((Excluded(p), Unbounded))
                    .next()
                    .copied(),
                None => attempt.establish.required_participations().first().copied(),
            };
            if let Some(p) = next {
                // This explicitly ends finite tracking after the original bound.
                // It is Unknown, never a positive Worker or local-exit proof.
                attempt
                    .stand_down
                    .observe_registry(
                        self.now,
                        p,
                        RegistryParticipationConvergence::RemoteTrackingEnded,
                    )
                    .map_err(|_| CoreFault::UnexpectedInput)?;
                attempt.tracking_cursor = Some(p);
                self.continuation = true;
                return Ok(());
            }
            attempt.tracking_pass_done = true;
        }
        self.notify_convergence(execution);
        let reserved = self.fresh_action();
        let abort_action = AttemptActionId::new(NonZeroU64::new(reserved.get()).unwrap());
        let attempt = self.attempts.get_mut(&execution).unwrap();
        match attempt
            .stand_down
            .poll_next_abort(self.now, abort_action, &mut self.ids, budget)
        {
            Ok(AbortPoll::Issued(dispatch)) => {
                let activation = attempt.activation;
                let action = self.issue_action();
                assert_eq!(action, reserved);
                self.queue_reserved(
                    action,
                    LogicalEffectBody::AttemptPort(Box::new(
                        novarocks_coordination_contract::logical_port::LogicalToAttempt {
                            activation,
                            body: LogicalToAttemptBody::Abort {
                                action: dispatch.action,
                                request: dispatch.request,
                            },
                        },
                    )),
                );
                self.continuation = true;
                return Ok(());
            }
            Ok(AbortPoll::Progressed) => {
                self.continuation = true;
                return Ok(());
            }
            Ok(AbortPoll::Yielded) => {
                self.continuation = true;
                return Ok(());
            }
            Ok(AbortPoll::Idle) => {}
            Err(error) => {
                self.conclude_failure(Self::local_error(&error));
                return Ok(());
            }
        }
        let reserved = self.fresh_action();
        let attempt = self.attempts.get_mut(&execution).unwrap();
        let ready_needs_record = self
            .ready
            .as_ref()
            .and_then(InternalReady::accepted)
            .is_some_and(|a| a.cut.activation == attempt.activation && !attempt.ready_cut_recorded);
        if ready_needs_record {
            return Ok(());
        }
        match attempt.convergence.poll_cleanup(
            self.now,
            attempt.stand_down.progress(),
            reserved,
            budget,
        ) {
            Ok(CleanupPoll::Issued(cleanup)) => {
                let activation = attempt.activation;
                let body = match cleanup.intent {
                    CleanupIntent::ResolveObligation(kind) => {
                        LogicalEffectBody::Governance(GovernanceEffect::ResolveObligation {
                            activation,
                            kind,
                        })
                    }
                    CleanupIntent::EndRemoteTracking(kind) => {
                        LogicalEffectBody::Governance(GovernanceEffect::EndRemoteTracking {
                            activation,
                            kind,
                        })
                    }
                    CleanupIntent::FinishResources => {
                        LogicalEffectBody::Native(NativeEffect::FinishResources { activation })
                    }
                    CleanupIntent::AbandonResources => {
                        LogicalEffectBody::Native(NativeEffect::AbandonResources { activation })
                    }
                };
                let action = self.issue_action();
                assert_eq!(action, reserved);
                self.queue_reserved(action, body);
            }
            Ok(CleanupPoll::Yielded) => self.continuation = true,
            Ok(CleanupPoll::Idle) => {}
            Err(error) => {
                self.report_supervision_failure(Self::local_error(&error));
                return Ok(());
            }
        }
        let attempt = self.attempts.get_mut(&execution).unwrap();
        if !attempt.diagnostic_emitted
            && attempt
                .convergence
                .cleanup_complete(attempt.stand_down.progress())
        {
            attempt.diagnostic_emitted = true;
            self.completed_attempts += 1;
            let activation = attempt.activation;
            self.pre_ready
                .remove(activation)
                .map_err(|_| CoreFault::ForeignActivation)?;
            let all_participations_closed = attempt.stand_down.residual_resource_settled();
            let runtime_filters = attempt.runtime_filters.clone();
            let accepted_failure = self
                .state
                .attempt(execution)
                .and_then(|a| a.accepted_failure.clone());
            self.queue(LogicalEffectBody::AttemptLifecycleDiagnostic(Box::new(
                crate::effect::AttemptLifecycleDiagnostic {
                    activation,
                    accepted_failure,
                    all_participations_closed,
                    runtime_filters,
                },
            )));
        }
        Ok(())
    }
    fn advance_closure(attempt: &mut Attempt, now: MonotonicInstant) -> Result<(), CoreFault> {
        let pass = attempt.closure.as_mut().unwrap();
        if pass.retiring {
            if pass.seen.pop_first().is_none() {
                attempt.closure = None;
            }
            return Ok(());
        }
        let len = match pass.original.as_ref() {
            AttemptConvergenceClosure::Evidence(rows) => rows.len(),
            AttemptConvergenceClosure::RemoteTrackingEnded {
                exact_participations,
            } => exact_participations.len(),
        };
        if len != attempt.establish.required_participations().len() {
            return Err(CoreFault::UnexpectedInput);
        }
        if pass.index == len {
            if pass.validating {
                pass.validating = false;
                pass.index = 0;
            } else {
                pass.retiring = true;
            }
            return Ok(());
        }
        let (p, fact) = match pass.original.as_ref() {
            AttemptConvergenceClosure::Evidence(rows) => {
                let (p, f) = rows[pass.index];
                use novarocks_coordination_contract::AttemptParticipationConvergence as F;
                (
                    p,
                    match f {
                        F::NeverEstablished => RegistryParticipationConvergence::NeverEstablished,
                        F::WorkerStoppedAndParticipationFenced => {
                            RegistryParticipationConvergence::WorkerStoppedAndParticipationFenced
                        }
                        F::WorkerProcessReplaced => {
                            RegistryParticipationConvergence::WorkerProcessReplaced
                        }
                    },
                )
            }
            AttemptConvergenceClosure::RemoteTrackingEnded {
                exact_participations,
            } => (
                exact_participations[pass.index],
                RegistryParticipationConvergence::RemoteTrackingEnded,
            ),
        };
        if pass.validating {
            if !attempt.establish.required_participations().contains(&p) || !pass.seen.insert(p) {
                return Err(CoreFault::UnexpectedInput);
            }
        } else {
            attempt
                .stand_down
                .observe_registry(now, p, fact)
                .map_err(|_| CoreFault::UnexpectedInput)?;
        }
        pass.index += 1;
        Ok(())
    }

    fn drive_recovery_cleanup(&mut self) {
        if self.state.conclusion().is_none() {
            return;
        }
        let Some(r) = self.recovery.as_mut() else {
            return;
        };
        if r.abandoning || !r.prepare_finished {
            return;
        }
        r.abandoning = true;
        let qualification = r
            .qualification
            .as_ref()
            .filter(|_| r.qualification_issued)
            .map(|q| (q.request().identity, q.request().operation_id));
        let dormant = r.replacement;
        let failed = r.failed.execution();
        if let Some((identity, operation_id)) = qualification {
            self.queue(LogicalEffectBody::Native(
                NativeEffect::CancelQualification {
                    identity,
                    operation_id,
                },
            ));
            self.queue(LogicalEffectBody::Native(
                NativeEffect::AbandonReservation { identity },
            ));
        }
        let remote_deadline = self
            .attempts
            .get(&failed)
            .and_then(|a| a.remote_deadline)
            .expect("replacement cleanup consumes the predecessor's original terminal bound");
        self.queue(LogicalEffectBody::Native(NativeEffect::ConvergeDormant {
            execution: dormant,
            remote_deadline,
        }));
    }
    fn recovery_resources_closed(&self) -> bool {
        self.recovery
            .as_ref()
            .is_none_or(|r| r.prepare_finished && r.abandoning && r.dormant_converged)
    }
    fn drive_observations(&mut self) {
        let Some(accepted) = self.ready.as_ref().and_then(InternalReady::accepted) else {
            return;
        };
        let Some(attempt) = self.attempts.get(&accepted.cut.activation.execution()) else {
            return;
        };
        if !attempt.ready_cut_recorded {
            return;
        }
        let snapshot = ConvergenceSnapshot {
            all_participations_closed: attempt.stand_down.residual_resource_settled(),
            remote_tracking_ended: attempt.stand_down.remote_tracking_ended(),
            local_resources_exited: attempt
                .convergence
                .cleanup_complete(attempt.stand_down.progress()),
        };
        let diagnostics_complete =
            attempt.runtime_filters.is_some() || !self.start.policy.retain_diagnostics;
        let ready = self.ready.as_mut().unwrap();
        let diagnostics = ready.begin_diagnostics(self.now, diagnostics_complete);
        let convergence = ready.begin_convergence(self.now, snapshot);
        if diagnostics {
            self.queue(LogicalEffectBody::Consumer(
                ConsumerEffect::PublishDiagnostics {
                    activation: accepted.cut.activation,
                    deadline: accepted.cut.diagnostic_deadline,
                },
            ));
        }
        if convergence {
            self.queue(LogicalEffectBody::Consumer(
                ConsumerEffect::PublishConvergence {
                    activation: accepted.cut.activation,
                    all_participations_closed: snapshot.all_participations_closed,
                    remote_tracking_ended: snapshot.remote_tracking_ended,
                    local_resources_exited: snapshot.local_resources_exited,
                },
            ));
        }
    }
    fn drive_final(&mut self) {
        if self.state.conclusion().is_none()
            || self.closing_pass
            || self.completed_attempts != self.attempts.len()
            || !self.recovery_resources_closed()
            || !self.actions.is_empty()
            || !self.outbox.is_empty()
        {
            return;
        }
        if self.start.policy.output == LogicalOutputMode::ResultStream {
            if !self.output_exposed {
                return;
            }
            if self.state.conclusion() == Some(LogicalConclusion::Succeeded) && !self.end_settled {
                return;
            }
        }
        if self.ready.as_ref().is_some_and(|r| {
            r.accepted().is_some()
                && (r.diagnostics() != crate::ready::PublicationState::Published
                    || r.convergence() != crate::ready::PublicationState::Published)
        }) {
            return;
        }
        if !self.preparation_retired {
            return;
        }
        if !self.stage_releasing {
            self.stage_releasing = true;
            self.queue(LogicalEffectBody::Governance(
                GovernanceEffect::ReleaseExecutionStage,
            ));
            return;
        }
        if !self.stage_released {
            return;
        }
        if !self.owner_completing {
            self.owner_completing = true;
            self.queue(LogicalEffectBody::Governance(
                GovernanceEffect::CompleteWorkOwner,
            ));
            return;
        }
        if !self.owner_completed {
            return;
        }
        if self.close.is_some() && self.ingress_closed && !self.retirable {
            self.retirable = true;
            self.queue(LogicalEffectBody::Retirable);
        }
    }
    pub(super) fn protocol_join_ready(&self) -> bool {
        self.state.conclusion().is_some()
            && self.completed_attempts == self.attempts.len()
            && self.recovery_resources_closed()
            && self.preparation_retired
            && self.stage_released
            && self.owner_completed
            && self.actions.is_empty()
            && self.outbox.iter().all(|effect| {
                matches!(
                    effect.body,
                    LogicalEffectBody::RegistryJoinReply { .. }
                        | LogicalEffectBody::SnapshotReply { .. }
                        | LogicalEffectBody::Retirable
                )
            })
    }
    pub fn next_deadline(&self) -> Option<MonotonicInstant> {
        if self.supervision_failure.is_some() {
            return (!self.outbox.is_empty()).then_some(self.now);
        }
        if self.continuation || !self.outbox.is_empty() || self.closing_pass || self.shortening_pass
        {
            return Some(self.now);
        }
        let mut next = None;
        let mut offer = |at: MonotonicInstant| {
            next = Some(next.map_or(at, |old: MonotonicInstant| old.min(at)));
        };
        if self.state.conclusion().is_none()
            && !self.scope_cancellation_requested
            && let Some(r) = self.recovery.as_ref()
        {
            if let Some(at) = self.preparation_window.deadline() {
                offer(at);
            }
            if let Some(q) = r.qualification.as_ref() {
                offer(q.deadline());
            }
        }
        for attempt in self.attempts.values() {
            if attempt.closing.is_none() {
                continue;
            }
            if attempt.diagnostic_emitted {
                continue;
            }
            if !attempt.initialized
                || !attempt.pending_registry.is_empty()
                || attempt.closure.is_some()
            {
                offer(self.now);
            }
            if let Some(at) = attempt.stand_down.next_abort_at(self.now) {
                offer(at);
            }
            if !attempt.tracking_pass_done
                && !attempt.stand_down.progress().tracking_complete
                && let Some(at) = attempt.remote_deadline
            {
                offer(at);
            }
        }
        if let Some(at) = self
            .ready
            .as_ref()
            .and_then(InternalReady::pending_observation_deadline)
        {
            offer(at);
        }
        next
    }
    pub fn check_invariants(&self) -> Result<(), InvariantViolation> {
        self.state
            .check_invariants()
            .map_err(|_| InvariantViolation::State)?;
        for (execution, attempt) in &self.attempts {
            if *execution != attempt.activation.execution()
                || attempt.activation.actor() != self.start.actor_instance
                || attempt.convergence.activation() != attempt.activation
            {
                return Err(InvariantViolation::Activation);
            }
            if self.state.conclusion().is_some()
                && !self.closing_pass
                && attempt.establish.authority_open()
            {
                return Err(InvariantViolation::OpenAuthorityAfterConclusion);
            }
        }
        if self.start.policy.output == LogicalOutputMode::ResultStream
            && self.state.output_visible() != self.visibility.visible_to().is_some()
        {
            return Err(InvariantViolation::State);
        }
        if self.visibility.committed()
            && !matches!(
                self.state.conclusion(),
                Some(LogicalConclusion::Succeeded | LogicalConclusion::ComputationReady)
            )
        {
            return Err(InvariantViolation::State);
        }
        if self.state.output_visible()
            && self.state.conclusion().is_none()
            && self.recovery.is_some()
        {
            return Err(InvariantViolation::VisibleRecovery);
        }
        if self.retirable
            && (!self.owner_completed
                || self.completed_attempts != self.attempts.len()
                || !self.ingress_closed)
        {
            return Err(InvariantViolation::PrematureRetirement);
        }
        Ok(())
    }
    pub(super) fn report_supervision_failure(&mut self, error: QueryExecutionError) {
        if self.supervision_failure.is_some() {
            return;
        }
        self.supervision_failure = Some(error.clone());
        self.queue(LogicalEffectBody::SupervisionFailed(error));
    }
}
