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
    failure::LogicalRequestFailure,
    input::{FailureReport, SuccessorTopology},
    pre_ready::PreReadyOpportunity,
    recovery::{RecoveryDecision, RecoveryInput, evaluate_recovery},
    replacement::{
        QualificationPhase, QualificationProgress, QualificationRequest, ReplacementPrerequisites,
        ReplacementQualificationFailure,
    },
};
use novarocks_coordination_contract::{AttemptFailureClass, ExecutionFailureSite};
use novarocks_types::AttemptId;

impl LogicalExecutionCore {
    pub(super) fn accept_attempt_failure(
        &mut self,
        activation: AttemptActivationIdentity,
        report: FailureReport,
    ) -> Result<(), CoreFault> {
        self.attempt_mut(activation)?;
        if report
            .original_attempt_failure
            .as_deref()
            .or(report.error.execution_failure())
            .is_some_and(|input| {
                input.fact().evidence()
                    == novarocks_failure_contract::FailureEvidence::StopObservation
            })
        {
            // A control observer is not the original failure author, even if
            // its receiver wakes after the real failure or a successor cut.
            return Ok(());
        }
        // Only an originating report authorizes local terminated read close.
        // Pending-root/withdrawal Stop and StopObservation never reach here.
        self.attempt_mut(activation)?.read_termination_authorized = true;
        if self.state.conclusion().is_some() || self.state.current_activation() != Some(activation)
        {
            return Ok(());
        }
        if let Some(input) = report
            .original_attempt_failure
            .as_deref()
            .or(report.error.execution_failure())
        {
            self.state
                .record_original_attempt_failure(activation, input.clone())
                .map_err(|_| CoreFault::ForeignActivation)?;
        }
        if self
            .ready
            .as_ref()
            .is_some_and(|r| r.has_activation(activation))
        {
            self.queue(LogicalEffectBody::Consumer(
                ConsumerEffect::PublishAttemptFailure {
                    activation,
                    error: report.error.clone(),
                },
            ));
        }
        self.freeze_terminal_cut(activation);
        let _frozen = self.visibility.failure(activation, true, false);
        self.reject_visibility_waiter(LogicalRequestFailure::RootAttemptTerminal.error());
        self.stop_relay(activation);
        if self.recovery.is_some() {
            return Ok(());
        }
        let Some(input) = report
            .original_attempt_failure
            .as_deref()
            .or(report.error.execution_failure())
        else {
            self.conclude_failure(report.error);
            return Ok(());
        };
        let mut failure = ExecutionFailure::from_input(input.clone());
        if let Some(witness) = report.pre_ready_replacement
            && self.attempt_mut(activation)?.establish.correlates(input)
            && witness.matches_input(input)
        {
            failure
                .attach_pre_ready_replacement(witness)
                .map_err(|_| CoreFault::UnexpectedInput)?;
        }
        let decision = evaluate_recovery(RecoveryInput {
            mode: self.start.policy.recovery,
            effect: self.start.policy.effect,
            failure: failure.class(),
            pre_ready_replacement: failure.pre_ready_replacement(),
            output_visible: self.state.output_visible(),
            attempts_started: self.state.attempts_started(),
            max_attempts: self.start.budgets.max_attempts.get(),
            deadline_reached: false,
        });
        if self.state.require_running(activation).is_err()
            || self.attempt_mut(activation)?.native_run_failed
            || decision != RecoveryDecision::BeginReplacement
            || self.scope_cancellation_requested
        {
            if self.scope_cancellation_requested {
                return Ok(());
            }
            self.conclude_failure(report.error);
            return Ok(());
        }
        let next = activation
            .execution()
            .attempt_id()
            .get()
            .checked_add(1)
            .expect("attempt identity exhausted");
        let replacement = QueryExecutionId::new(
            self.start.identity,
            AttemptId::new(next).expect("nonzero successor attempt"),
        )
        .expect("same original nonzero query");
        let valid_for = self
            .start
            .budgets
            .replacement_reservation_valid_for
            .expect("validated recovery retains its original preparation budget");
        let deadline = self.preparation_window.open_once(self.now, valid_for);
        if self.now >= deadline {
            self.conclude_setup_failure(Self::preparation_expired(), failure.error().clone());
            return Ok(());
        }
        // Originating backend loss excludes that exact process even without
        // a pre-ready replacement witness. Other sites invent no exclusion.
        let topology = failure.pre_ready_replacement().map_or_else(
            || match (failure.class(), failure.input().site()) {
                (
                    AttemptFailureClass::RecoverableInfrastructure,
                    ExecutionFailureSite::Backend { process },
                ) => SuccessorTopology::ExcludeProcess(process),
                _ => SuccessorTopology::LiveSnapshot,
            },
            |w| SuccessorTopology::ExcludeProcess(w.participation().backend_process_id()),
        );
        self.recovery = Some(Recovery {
            failed: activation,
            replacement,
            failure,
            prepared: None,
            identity: None,
            qualification: None,
            builder: None,
            build_cursor: 0,
            import_cursor: 0,
            reservation_activated: false,
            candidate: None,
            inherited: Vec::new(),
            roster_validated: false,
            prepare_finished: false,
            prepare_issued: false,
            qualification_issued: false,
            abandoning: false,
            dormant_converged: false,
        });
        self.queue(LogicalEffectBody::Native(NativeEffect::PrepareSuccessor {
            failed: activation.execution(),
            replacement,
            topology,
            deadline,
        }));
        Ok(())
    }
    pub(super) fn pre_ready_opportunity(&self) -> PreReadyOpportunity {
        PreReadyOpportunity {
            current_running: self
                .state
                .current_activation()
                .filter(|a| self.state.require_running(*a).is_ok()),
            cancellation_pending: self.cancellation.is_some() || self.scope_cancellation_requested,
            concluded: self.state.conclusion().is_some(),
            window: self.state.recovery_window(false),
        }
    }
    pub(super) fn drive_pre_ready(&mut self, budget: &mut dyn StepBudget) -> Result<(), CoreFault> {
        while self.refresh_pre_ready {
            let Some(execution) = self.pre_ready.next_watch_after(self.pre_ready_cursor) else {
                self.refresh_pre_ready = false;
                self.pre_ready_cursor = None;
                break;
            };
            if !budget.charge(1) {
                self.continuation = true;
                return Ok(());
            }
            let opportunity = self.pre_ready_opportunity();
            let correlated = self.pre_ready.refusal(execution).is_some_and(|f| {
                self.attempts
                    .get(&execution)
                    .is_some_and(|a| a.establish.correlates(f))
            });
            let _changed = self
                .pre_ready
                .refresh_one(execution, opportunity, correlated);
            self.pre_ready_cursor = Some(execution);
        }
        while self.pre_ready.has_changes() {
            if !budget.charge(1) {
                self.continuation = true;
                return Ok(());
            }
            let change = self
                .pre_ready
                .take_change()
                .expect("checked pending exact watch change");
            self.attempt_reply(change.activation,novarocks_coordination_contract::logical_port::LogicalToAttemptBody::PreReadyReplacementChanged{token:change.token});
        }
        Ok(())
    }
    pub(super) fn drive_recovery(&mut self, budget: &mut dyn StepBudget) -> Result<(), CoreFault> {
        if self.recovery.is_none()
            || self.state.conclusion().is_some()
            || self.scope_cancellation_requested
        {
            return Ok(());
        }
        if self.preparation_window.expired(self.now) {
            let preparing = self.recovery.as_ref().is_some_and(|r| r.identity.is_none());
            self.conclude_replacement_failure(if preparing {
                Self::preparation_expired()
            } else {
                ReplacementQualificationFailure::Expired.error()
            });
            return Ok(());
        }
        let mut recovery = self.recovery.take().expect("checked pending recovery");
        let result = self.advance_recovery(&mut recovery, budget);
        if result.as_ref().is_ok_and(|done| *done) {
            if let Some(qualification) = recovery.qualification.take() {
                self.qualification_history.push(qualification);
            }
        } else {
            self.recovery = Some(recovery);
        }
        match result {
            Ok(_) => Ok(()),
            Err(error) => {
                self.conclude_replacement_failure(error);
                Ok(())
            }
        }
    }

    fn advance_recovery(
        &mut self,
        r: &mut Recovery,
        budget: &mut dyn StepBudget,
    ) -> Result<bool, QueryExecutionError> {
        if self
            .attempts
            .get(&r.failed.execution())
            .is_some_and(|a| a.native_run_failed)
        {
            return Err(r.failure.error().clone());
        }
        let Some(participations) = r.prepared.as_ref() else {
            return Ok(false);
        };
        if !r.roster_validated {
            if !budget.charge(1) {
                self.continuation = true;
                return Ok(false);
            }
            if let Some(p) = participations.get(r.build_cursor) {
                r.builder
                    .as_mut()
                    .expect("prepared roster has its incremental builder")
                    .push(*p)
                    .map_err(|e| e.failure().error().clone())?;
                r.build_cursor += 1;
                self.continuation = true;
                return Ok(false);
            }
            r.roster_validated = true;
        }
        if r.identity.is_none() {
            let failed = self
                .attempts
                .get(&r.failed.execution())
                .expect("recovering predecessor remains owned");
            if failed.native_run_failed
                || r.failure.pre_ready_replacement().is_some_and(|w| {
                    !failed.establish.correlates(r.failure.input())
                        || !w.matches_input(r.failure.input())
                })
            {
                return Err(r.failure.error().clone());
            }
            let identity = self
                .state
                .begin_replacement(r.failed, r.replacement, &r.failure, false)
                .map_err(|_| LogicalRequestFailure::ReplacementNotReady.error())?;
            self.begin_attempt_convergence(r.failed, AttemptConvergenceMode::Cleanup);
            if let Some(old) = self
                .ready
                .as_mut()
                .filter(|ready| ready.has_activation(r.failed))
                .and_then(|ready| ready.retire_source(r.failed).ok().flatten())
            {
                self.queue(LogicalEffectBody::Consumer(ConsumerEffect::InternalReply {
                    source: old.source,
                    token: old.token,
                    activation: old.activation,
                    response: crate::effect::InternalResponse::Rejected(
                        LogicalRequestFailure::StaleAuthority.error(),
                    ),
                }));
            }
            let operation_id = self.ids.issue();
            let failed_participations = Arc::clone(
                &self
                    .attempts
                    .get(&r.failed.execution())
                    .unwrap()
                    .participations,
            );
            let issued_at = self.now;
            let expires_at = self
                .preparation_window
                .deadline()
                .expect("one original preparation window");
            let request = QualificationRequest {
                identity,
                operation_id,
                failed_participations: Arc::clone(&failed_participations),
                replacement_participations: Arc::clone(participations),
                issued_at,
                expires_at,
            };
            r.qualification = Some(QualificationState::new(request).map_err(|e| e.error())?);
            r.identity = Some(identity);
            let request = r.qualification.as_ref().unwrap().request();
            self.queue(LogicalEffectBody::Native(
                NativeEffect::DispatchQualification {
                    identity: request.identity,
                    operation_id: request.operation_id,
                    failed_participations: Arc::clone(&request.failed_participations),
                    replacement_participations: Arc::clone(&request.replacement_participations),
                    issued_at: request.issued_at,
                    expires_at: request.expires_at,
                },
            ));
            return Ok(false);
        }
        let qualification = r
            .qualification
            .as_mut()
            .expect("replacement identity has its exact qualification");
        match qualification.phase() {
            QualificationPhase::Validating => {
                if !budget.charge(1) {
                    self.continuation = true;
                    return Ok(false);
                }
                match qualification
                    .validate_next(self.now)
                    .map_err(|e| e.error())?
                {
                    QualificationProgress::Pending | QualificationProgress::Qualified => {
                        self.continuation = true
                    }
                    QualificationProgress::Failed => {
                        return Err(qualification
                            .failure()
                            .expect("failed qualification preserves original error")
                            .clone());
                    }
                }
            }
            QualificationPhase::Qualified => {
                let predecessor = self.attempts.get(&r.failed.execution()).unwrap();
                if !predecessor.convergence.running_work_registered() {
                    return Ok(false);
                }
                let prerequisites = ReplacementPrerequisites {
                    reachable_participations_closed: true,
                    attempt_isolation_proven: true,
                    stale_usage_accounted: true,
                    new_capacity_admitted: true,
                };
                // Qualified is the original isolation author's signed binding;
                // ledger validation and original RunningWork precede activation.
                qualification
                    .begin_activation(self.now, prerequisites)
                    .map_err(|e| e.error())?;
                let request = qualification.request();
                self.queue(LogicalEffectBody::Native(
                    NativeEffect::ActivateReservation {
                        identity: request.identity,
                        operation_id: request.operation_id,
                    },
                ));
            }
            QualificationPhase::Activated if r.reservation_activated => {
                let identity = r.identity.unwrap();
                let activation = AttemptActivationIdentity::from_parts(
                    identity.actor.get(),
                    identity.replacement,
                    identity.generation,
                );
                if r.candidate.is_none() {
                    r.candidate = Some(
                        r.builder
                            .take()
                            .expect("validated successor retains its two roster indexes")
                            .finish(
                                activation,
                                self.start.budgets.admission_issues_per_participation,
                                self.start.budgets.establish_per_participation,
                            ),
                    );
                }
                if !budget.charge(1) {
                    self.continuation = true;
                    return Ok(false);
                }
                let facts = qualification
                    .facts()
                    .expect("activated reservation retains original qualified metadata");
                if let Some(admission) = facts.admissions.get(r.import_cursor) {
                    r.candidate
                        .as_mut()
                        .unwrap()
                        .import_admission(
                            admission.request,
                            admission.receipt,
                            qualification
                                .dispatched_at()
                                .expect("qualified grants retain original actual handoff"),
                            self.now,
                        )
                        .map_err(|e| e.failure().error().clone())?;
                    r.inherited
                        .push(novarocks_coordination_contract::AttemptInheritedAdmission {
                            participation: admission.request.participation(),
                            receipt: admission.receipt,
                        });
                    r.import_cursor += 1;
                    self.continuation = true;
                    return Ok(false);
                }
                let prerequisites = ReplacementPrerequisites {
                    reachable_participations_closed: true,
                    attempt_isolation_proven: true,
                    stale_usage_accounted: true,
                    new_capacity_admitted: true,
                };
                let accepted = self
                    .state
                    .activate_replacement(identity, prerequisites)
                    .map_err(|_| LogicalRequestFailure::ReplacementNotReady.error())?;
                assert_eq!(
                    accepted, activation,
                    "the core owns the one successor activation generation"
                );
                let ledger = r.candidate.take().unwrap();
                self.attempts.insert(
                    accepted.execution(),
                    Attempt::new(accepted, Arc::clone(participations), ledger, &self.start),
                );
                self.queue(LogicalEffectBody::Governance(
                    GovernanceEffect::RegisterObligation {
                        activation: accepted,
                        kind: ObligationKind::RunningWork,
                    },
                ));
                self.queue(LogicalEffectBody::Native(NativeEffect::ActivateAttempt {
                    activation: accepted,
                    inherited_admissions: std::mem::take(&mut r.inherited),
                }));
                self.refresh_pre_ready = true;
                self.pre_ready_cursor = None;
                return Ok(true);
            }
            QualificationPhase::Failed => {
                return Err(qualification
                    .failure()
                    .expect("failed qualification retains typed refusal")
                    .clone());
            }
            _ => {}
        }
        Ok(false)
    }
    fn preparation_expired() -> QueryExecutionError {
        crate::failure::query_failure(
            novarocks_coordination_contract::QueryFailureReason::RecoveryPreparationBudgetExceeded,
            "Native replacement preparation exceeded its recovery budget",
        )
    }
}
