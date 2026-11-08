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

//! A retained actor has no execution supervisor Work. Its actual failure
//! publication and panic disposal use one short process-owned Cleanup.

use super::*;
use crate::control_runtime::participation_host::{
    ParticipationIoHandoffError,
    task_io::{TaskIoHostJob, TaskIoPorts},
};
use novarocks_control_runtime::{
    actor_turn::{ActorFailureProof, ActorRegistration},
    completion::ReportSlot,
    obligation::{ReservationObligation, WorkObligation, WorkRetirementObserver},
};
use novarocks_control_runtime_core::{
    capacity::{ReservationResources, ResourceVector},
    completion::ConsumerKind,
    identity::ConsumerId,
    owned_storage::OwnedSlotNeed,
    scope::{WorkAdmission, WorkKind},
};

pub(in crate::control_runtime) struct ColdFailureAdmission {
    session: FailurePublicationSession<WorkerActor>,
    cleanup: ColdFailureCleanup,
}
pub(in crate::control_runtime) struct ColdFailurePartial {
    work: WorkObligation,
    capacity: ReservationObligation,
    report: Option<ReportSlot>,
}
pub(in crate::control_runtime) struct ColdFailureAdmissionRefusal {
    pub error: FailurePublicationError,
    pub original: Option<ColdFailurePartial>,
}
impl std::fmt::Debug for ColdFailureAdmissionRefusal {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ColdFailureAdmissionRefusal")
            .field("error", &self.error)
            .field("retains_original", &self.original.is_some())
            .finish()
    }
}
pub(super) struct ColdFailureCleanup {
    original: ColdFailurePartial,
    retirement: WorkRetirementObserver,
    exited: bool,
    report_retired: bool,
}
impl ColdFailureAdmission {
    /// Called while borrowing only Root. No Context lock is held during this
    /// actual derivation from the same Root's still-running lifetime author.
    pub(in crate::control_runtime) fn admit(
        runtime: &ActorRuntime<WorkerActor>,
        root: &ActorRegistration,
        parent: &WorkObligation,
        proof: &ActorFailureProof,
    ) -> Result<Self, ColdFailureAdmissionRefusal> {
        let no_work = |error| ColdFailureAdmissionRefusal {
            error,
            original: None,
        };
        runtime
            .validate_work_author(root, parent)
            .map_err(|e| no_work(FailurePublicationError::ActorTurn(e)))?;
        if !proof.registration().retained_in_process(parent.scope()) {
            return Err(no_work(FailurePublicationError::WrongOwner));
        }
        let generation = parent
            .scope()
            .gates()
            .map_err(|e| no_work(FailurePublicationError::Runtime(e)))?
            .generation;
        let resources = ReservationResources {
            keyed: None,
            quota: ResourceVector::default(),
            owned_slots: OwnedSlotNeed {
                work_headers: 1,
                completions: 1,
                captures: 1,
                ..OwnedSlotNeed::default()
            },
        };
        let guard = parent
            .scope()
            .reserve_next(
                generation,
                resources,
                WorkKind::Cleanup,
                WorkAdmission::Derived(parent.id()),
            )
            .map_err(|e| no_work(FailurePublicationError::Runtime(e)))?;
        let (work, capacity) = guard
            .bind(WorkKind::Cleanup)
            .unwrap_or_else(|_| std::process::abort());
        let mut original = ColdFailurePartial {
            work,
            capacity,
            report: None,
        };
        let report = match ReportSlot::reserve_next(
            &mut original.capacity,
            generation,
            1,
            [(ConsumerId::new(1).unwrap(), ConsumerKind::Observer)],
        ) {
            Ok(report) => report,
            Err(error) => {
                return Err(ColdFailureAdmissionRefusal {
                    error: FailurePublicationError::Runtime(error),
                    original: Some(original),
                });
            }
        };
        let reported = crate::control_runtime::reported_work_admission::adopt_reported_work(
            original.work,
            original.capacity,
            report,
        );
        let (work, capacity, report) = reported.into_parts();
        let retirement = work.retirement_observer();
        let mut original = ColdFailurePartial {
            work,
            capacity,
            report: Some(report),
        };
        if let Err(error) = original.work.start() {
            return Err(ColdFailureAdmissionRefusal {
                error: FailurePublicationError::Runtime(error),
                original: Some(original),
            });
        }
        let session = match runtime.claim_actor_failure_publication(proof, &original.work) {
            Ok(session) => session,
            Err(error) => {
                return Err(ColdFailureAdmissionRefusal {
                    error: FailurePublicationError::ActorTurn(error),
                    original: Some(original),
                });
            }
        };
        Ok(Self {
            session,
            cleanup: ColdFailureCleanup {
                original,
                retirement,
                exited: false,
                report_retired: false,
            },
        })
    }
}
impl ColdFailureCleanup {
    fn advance_retirement(&mut self) -> Result<(bool, bool), FailurePublicationError> {
        tracing::debug!(work=?self.original.work.reference(),
            actual_exit=?self.original.work.actual_exit_ready(),
            header_retired=self.retirement.is_retired(),
            bundle_finished=self.original.capacity.is_released(),
            exited=self.exited, report_retired=self.report_retired,
            "Worker cold failure original Cleanup retirement");
        let mut progress = false;
        let report = self
            .original
            .report
            .as_ref()
            .expect("same actual cold Cleanup report");
        if !self.exited {
            // The publication claim has closed and the original panic's
            // physical FailureTail already reported its actual Drop exit.
            // Capture exit publishes this report in the same transaction;
            // filling it separately would duplicate the original completion.
            self.original
                .work
                .body_exited()
                .map_err(FailurePublicationError::Runtime)?;
            self.original
                .work
                .captures_exited_with_report(report)
                .map_err(FailurePublicationError::Runtime)?;
            report
                .body_exited()
                .map_err(FailurePublicationError::Runtime)?;
            report
                .captures_exited()
                .map_err(FailurePublicationError::Runtime)?;
            report
                .consume(ConsumerId::new(1).unwrap())
                .map_err(FailurePublicationError::Runtime)?;
            self.exited = true;
            progress = true;
        }
        if !self
            .original
            .work
            .actual_exit_ready()
            .map_err(FailurePublicationError::Runtime)?
        {
            return Ok((progress, false));
        }
        if !self.report_retired {
            if !report
                .try_retire()
                .map_err(FailurePublicationError::Runtime)?
            {
                return Ok((progress, false));
            }
            self.report_retired = true;
            progress = true;
        }
        // Match the common ExitCustody retirement order: actual holder exit,
        // report consumption/retirement, then release its original bundle.
        // Header reclamation remains a separate exact observation.
        if !self.original.capacity.is_released() {
            self.original
                .capacity
                .finish()
                .map_err(FailurePublicationError::Runtime)?;
            progress = true;
        }
        if !self.retirement.is_retired() {
            progress |= self
                .original
                .work
                .scope()
                .reap_ready(1)
                .map_err(FailurePublicationError::Runtime)?
                > 0;
            if !self.retirement.is_retired() {
                return Ok((progress, false));
            }
        }
        Ok((progress, true))
    }
}
impl ParticipationHostRecord {
    pub(in crate::control_runtime) fn cold_failure_admission_needed(
        &self,
    ) -> Result<Option<ActorFailureProof>, ActorTurnError> {
        if self.resources.scope.is_some()
            || self.resources.failure.is_some()
            || self.failed_owner_completed()
        {
            return Ok(None);
        }
        if !self.control_runtime.snapshot(self.actor)?.failed {
            return Ok(None);
        }
        self.control_runtime
            .failure_proof_for(self.registration())
            .map(Some)
    }
    pub(in crate::control_runtime) fn install_cold_failure(
        &mut self,
        original: ColdFailureAdmission,
    ) -> Result<(), ColdFailureAdmission> {
        if self.resources.scope.is_some()
            || self.resources.failure.is_some()
            || self.failed_owner_completed()
            || !original
                .session
                .registration()
                .same_registration(self.registration())
        {
            return Err(original);
        }
        self.resources.failure = Some(ScopeFailureOwner {
            session: Some(original.session),
            ticket: None,
            cold: Some(original.cleanup),
            phase: Phase::Tasks(None),
            pending_projection: None,
            pending_projection_task: None,
            next_input_class: 0,
            empty_input_classes: 0,
            physical: None,
            completed_tail_input: None,
            refused_request_input_after: None,
            cleanup_bindings: CleanupBindingCursor::Batches(None),
        });
        self.notify_host_progress();
        Ok(())
    }
    pub(in crate::control_runtime::participation_host) fn advance_cold_failure_tail(
        &mut self,
        ports: &TaskIoPorts,
        budget: usize,
    ) -> Result<bool, ParticipationIoHandoffError> {
        let Some(mut original) = self.resources.failure.take() else {
            return Ok(false);
        };
        let result = self.advance_cold_failure_owned(&mut original, ports, budget);
        match result {
            Ok((runnable, true)) => {
                self.resources.teardown.failed_owner_completed = true;
                Ok(runnable)
            }
            other => {
                self.resources.failure = Some(original);
                other.map(|(runnable, _)| runnable)
            }
        }
    }
    fn advance_cold_failure_owned(
        &mut self,
        original: &mut ScopeFailureOwner,
        ports: &TaskIoPorts,
        budget: usize,
    ) -> Result<(bool, bool), ParticipationIoHandoffError> {
        if budget == 0 {
            return Ok((false, false));
        }
        self.flush_task_cleanup_pending(ports, budget)
            .map_err(ParticipationIoHandoffError::Task)?;
        tracing::debug!(participation=?self.identity, actor=?self.control_runtime.snapshot(self.actor),
            publication_active=original.session.is_some(), drained=original.is_drained(),
            final_tail=?original.physical.as_ref().map(|(purpose,capture)|
                (*purpose==FailurePhysicalPurpose::FinalCarrier,capture.actual_exit_observed())),
            requests=self.resources.requests.len(), partials=self.resources.request_partials.len(),
            routes=self.resources.routes.len(), tasks=self.tasks.len(), driver_idle=self.task_io.is_idle(),
            "Worker cold failure original custodian progress");
        if original.session.is_none() {
            return original
                .cold
                .as_mut()
                .ok_or(ParticipationIoHandoffError::WrongOwner)?
                .advance_retirement()
                .map_err(ParticipationIoHandoffError::Failure);
        }
        original
            .drained()
            .map_err(ParticipationIoHandoffError::Failure)?
            .validate(&self.control_runtime, self.actor)
            .map_err(ParticipationIoHandoffError::ActorTurn)?;
        let proof = self
            .control_runtime
            .failure_proof_for(self.registration())
            .map_err(ParticipationIoHandoffError::ActorTurn)?;
        if !self
            .resources
            .lifecycle
            .as_mut()
            .ok_or(ParticipationIoHandoffError::WrongOwner)?
            .withdraw_failed_callback(&proof)
            .map_err(ParticipationIoHandoffError::Host)?
        {
            return Ok((false, false));
        }
        let mut after = original.refused_request_input_after;
        let runnable = {
            let stage = original
                .drained()
                .map_err(ParticipationIoHandoffError::Failure)?;
            self.advance_refused_neutral_inputs(&stage, &mut after, budget)
                .map_err(ParticipationIoHandoffError::Request)?
        };
        original.refused_request_input_after = after;
        if !self.tasks.is_empty()
            || !self.task_io.is_idle()
            || !self.resources.requests.is_empty()
            || !self.resources.request_partials.is_empty()
            || !self.resources.routes.is_empty()
        {
            return Ok((runnable, false));
        }
        if !self
            .dispatch
            .drain_failed_index_turn(&self.control_runtime, budget)
            .map_err(ParticipationIoHandoffError::Dispatch)?
        {
            return Ok((true, false));
        }
        if let Some((FailurePhysicalPurpose::FinalCarrier, capture)) = &original.physical {
            if !capture.actual_exit_observed() {
                return Ok((false, false));
            }
            original
                .close_session()
                .map_err(ParticipationIoHandoffError::Failure)?;
            original.physical = None;
            return original
                .cold
                .as_mut()
                .ok_or(ParticipationIoHandoffError::WrongOwner)?
                .advance_retirement()
                .map_err(ParticipationIoHandoffError::Failure);
        }
        if !self
            .control_runtime
            .input_inventory(self.actor, None, 1)
            .map_err(ParticipationIoHandoffError::ActorTurn)?
            .is_empty()
        {
            return Ok((false, false));
        }
        let payload = self
            .control_runtime
            .take_failure_payload(self.actor)
            .map_err(ParticipationIoHandoffError::ActorTurn)?
            .ok_or(ParticipationIoHandoffError::WrongOwner)?;
        let work = original
            .cold
            .as_ref()
            .ok_or(ParticipationIoHandoffError::WrongOwner)?
            .original
            .work
            .reference();
        let health = ports
            .protocol_events
            .as_ref()
            .ok_or(ParticipationIoHandoffError::WrongOwner)?
            .execution_health();
        let (capture, job) = super::super::failure_tail::FailureTail::original(
            (work.scope, work.work),
            payload,
            self.ready_notice().clone(),
            health,
        );
        original.physical = Some((FailurePhysicalPurpose::FinalCarrier, capture));
        self.queue_task_cleanup(TaskIoHostJob::FailureTail(job), ports)
            .map_err(ParticipationIoHandoffError::Task)?;
        Ok((false, false))
    }
}
