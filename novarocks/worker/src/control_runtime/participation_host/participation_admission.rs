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

//! The actual CloseExternalAdmission effect owns its once-only physical host
//! invocation. No request, exit role, Work or reply author is manufactured here.
//! Its concrete once-only capture cell is one of the Participation admission's
//! PARTICIPATION_ADMISSION_CLOSE_CAPTURES (Quiesce/first Abort). The actual original
//! input/batch blocks Scope and parent-fee retirement until completion; the
//! self pin only retains Unknown and supplies no payment or exit authority.

use super::{ParticipationHostRecord, TaskIoError, TaskIoHostJob, TaskIoPorts, TaskIoRefusal};
use crate::control_runtime::{
    actors::{WorkerAction, WorkerActor},
    participation_ready::ParticipationReadyNotice,
    request_endpoint::RequestNotice,
};
use novarocks_control_runtime::actor_turn::{ActorRuntime, EffectTicket};
use novarocks_control_runtime_core::identity::{Generation, ScopeId, WorkId};
use novarocks_execution::runtime::execution_health::{ExecutionExitOrigin, ExecutionHealth};
use novarocks_execution_contract::{AbortCause, ParticipationRef};
use novarocks_worker_protocol::participation::ParticipationEffect;
use std::{
    any::Any,
    mem::ManuallyDrop,
    panic::{AssertUnwindSafe, catch_unwind},
    sync::{Arc, Mutex},
};

/// Custody of one actual Core-authored close action. Counts describe handoff
/// of that action's original fanout, never Task completion or physical exit.
pub(in crate::control_runtime::participation_host) struct ParticipationCloseFanout {
    pub(in crate::control_runtime::participation_host) generation: Generation,
    pub(in crate::control_runtime::participation_host) reason: Option<AbortCause>,
    pub(in crate::control_runtime::participation_host) targets: Option<usize>,
    pub(in crate::control_runtime::participation_host) staged: usize,
    pub(in crate::control_runtime::participation_host) ticket: Option<EffectTicket<WorkerAction>>,
    pub(in crate::control_runtime::participation_host) gate:
        Arc<crate::control_runtime::participation_close::ParticipationCloseGate>,
}

struct State {
    ticket: Option<ManuallyDrop<EffectTicket<WorkerAction>>>,
    host: Option<ManuallyDrop<Arc<dyn crate::TaskExecutionHost>>>,
    started: bool,
    panic: Option<ManuallyDrop<Box<dyn Any + Send>>>,
}
struct Custody {
    runtime: ActorRuntime<WorkerActor>,
    participation: ParticipationRef,
    reason: Option<AbortCause>,
    key: (ScopeId, WorkId),
    notice: ParticipationReadyNotice,
    request: Option<RequestNotice>,
    health: Arc<ExecutionHealth>,
    state: Mutex<State>,
    // This same finite effect cell retains Unknown on callback/capture panic.
    // It is not a second obligation index or a replacement payment.
    retained: Mutex<Option<Arc<Self>>>,
    gate: Option<Arc<crate::control_runtime::participation_close::ParticipationCloseGate>>,
}
#[must_use = "The original close effect must execute or retain its real capture"]
pub(crate) struct ParticipationAdmissionCloseJob(Arc<Custody>);
impl ParticipationAdmissionCloseJob {
    pub(in crate::control_runtime::participation_host) fn participation(&self) -> ParticipationRef {
        self.0.participation
    }
    pub(in crate::control_runtime::participation_host) fn queue_key(&self) -> (ScopeId, WorkId) {
        self.0.key
    }
    fn notify(&self) {
        if let Some(request) = &self.0.request {
            request.notify();
        }
        self.0.notice.notify();
    }
    fn fail(&self) {
        self.0.health.report_unresolved(
            ExecutionExitOrigin::Process,
            novarocks_execution_contract::failure::FailureFact::new(
                novarocks_execution_contract::failure::FailureClass::Invariant,
                crate::failure_reason::WorkerFailureReason::CompletionWorkUnresolved.reason(),
            ),
        );
        self.notify();
    }
    pub(in crate::control_runtime::participation_host) fn run(self) {
        let host = {
            let mut state = self
                .0
                .state
                .lock()
                .expect("original Participation close capture");
            assert!(
                state.panic.is_none(),
                "an unresolved original close is never retried"
            );
            assert!(!state.started, "original Participation close runs once");
            state.started = true;
            ManuallyDrop::into_inner(state.host.take().expect("same original execution host"))
        };
        let returned = catch_unwind(AssertUnwindSafe(|| match self.0.reason {
            None => host.close_participation_admission(self.0.participation),
            Some(_) => host.abort_participation_admission(self.0.participation),
        }));
        if let Err(payload) = returned {
            let mut state = self
                .0
                .state
                .lock()
                .expect("original Participation close panic");
            state.host = Some(ManuallyDrop::new(host));
            state.panic = Some(ManuallyDrop::new(payload));
            drop(state);
            self.fail();
            return;
        }
        // Actual capture destruction is also part of this original effect.
        // The poller never drops the captured Host or an original panic carrier.
        if let Err(payload) = catch_unwind(AssertUnwindSafe(|| drop(host))) {
            self.0
                .state
                .lock()
                .expect("Participation close capture panic")
                .panic = Some(ManuallyDrop::new(payload));
            self.fail();
            return;
        }
        let ticket = ManuallyDrop::into_inner(
            self.0
                .state
                .lock()
                .expect("original Participation close completion")
                .ticket
                .take()
                .expect("same original close ticket"),
        );
        if let Err(refusal) = self.0.runtime.effect_completed(ticket) {
            self.0
                .state
                .lock()
                .expect("original refused close completion")
                .ticket = Some(ManuallyDrop::new(refusal.ticket));
            self.fail();
            return;
        }
        if let Some(gate) = &self.0.gate {
            gate.complete();
        }
        self.notify();
        let retained = self
            .0
            .retained
            .lock()
            .expect("actual Participation close exit")
            .take();
        drop(retained);
    }
}
impl ParticipationHostRecord {
    pub(in crate::control_runtime::participation_host) fn stage_participation_admission_close(
        &mut self,
        ticket: EffectTicket<WorkerAction>,
        generation: Generation,
        reason: Option<AbortCause>,
    ) {
        let fanouts = &mut self.resources.pending_host_jobs.close_fanouts;
        assert!(
            !fanouts
                .iter()
                .any(|f| f.generation == generation && f.reason == reason),
            "one original close action per intent"
        );
        // The protocol admits one normal Quiesce and one first Abort.
        assert!(
            fanouts.len() < 2,
            "original Participation close action inventory"
        );
        fanouts.push(ParticipationCloseFanout {
            generation,
            reason,
            targets: None,
            staged: 0,
            ticket: Some(ticket),
            gate: Arc::default(),
        });
    }

    pub(in crate::control_runtime::participation_host) fn advance_participation_admission_closes(
        &mut self,
        ports: &TaskIoPorts,
    ) -> Result<(), TaskIoError> {
        for index in 0..self.resources.pending_host_jobs.close_fanouts.len() {
            let fanout = &mut self.resources.pending_host_jobs.close_fanouts[index];
            let gate = fanout.gate.clone();
            let reason = fanout.reason;
            let ticket = if fanout.targets == Some(fanout.staged) {
                fanout.ticket.take()
            } else {
                None
            };
            if let Some(ticket) = ticket
                && let Err(refusal) = self.handoff_participation_admission_close_for_owner(
                    ticket,
                    reason,
                    ports,
                    None,
                    Some(gate),
                )
            {
                self.resources.pending_host_jobs.close_fanouts[index].ticket = refusal.ticket;
                return Err(refusal.error);
            }
        }
        Ok(())
    }

    pub(in crate::control_runtime::participation_host) fn advance_participation_close_notifications(
        &self,
        budget: usize,
    ) -> bool {
        let mut runnable = false;
        for fanout in &self.resources.pending_host_jobs.close_fanouts {
            runnable |= fanout.gate.notify_ready(budget);
        }
        runnable
    }

    pub(in crate::control_runtime::participation_host) fn handoff_failed_participation_admission_close(
        &mut self,
        ticket: EffectTicket<WorkerAction>,
        reason: Option<AbortCause>,
        ports: &TaskIoPorts,
        stage: &super::super::failure::ScopePublicationsDrained<'_>,
        gate: Arc<crate::control_runtime::participation_close::ParticipationCloseGate>,
    ) -> Result<(), TaskIoRefusal> {
        self.handoff_participation_admission_close_for_owner(
            ticket,
            reason,
            ports,
            Some(stage),
            Some(gate),
        )
    }

    fn handoff_participation_admission_close_for_owner(
        &mut self,
        ticket: EffectTicket<WorkerAction>,
        reason: Option<AbortCause>,
        ports: &TaskIoPorts,
        failed: Option<&super::super::failure::ScopePublicationsDrained<'_>>,
        gate: Option<Arc<crate::control_runtime::participation_close::ParticipationCloseGate>>,
    ) -> Result<(), TaskIoRefusal> {
        let reject = |error, ticket| TaskIoRefusal {
            error,
            ticket: Some(ticket),
        };
        if !matches!(&ticket.effect,
            WorkerAction::Participation(ParticipationEffect::CloseExternalAdmission {participation, reason: frozen, ..})
            if *participation == self.identity && *frozen == reason)
        {
            return Err(reject(TaskIoError::WrongOwner, ticket));
        }
        // The actual failure stage may adopt this already Started close. It
        // preserves the same ticket, capture payment and physical invocation;
        // it does not fabricate a new normal-generation publication.
        let authority = match failed {
            Some(stage) => stage.validate(&self.control_runtime, self.actor),
            None => self.control_runtime.validate_effect_ticket(&ticket),
        };
        if let Err(error) = authority {
            return Err(reject(TaskIoError::ActorTurn(error), ticket));
        }
        let Some(host) = ports.execution.as_ref() else {
            return Err(reject(TaskIoError::MissingExecutionPort, ticket));
        };
        if ports.lifecycle.is_none() {
            return Err(reject(TaskIoError::MissingLifecyclePort, ticket));
        }
        let Some(protocol) = ports.protocol_events.as_ref() else {
            return Err(reject(TaskIoError::MissingExecutionHealth, ticket));
        };
        let owner = Arc::new(Custody {
            runtime: self.control_runtime.clone(),
            participation: self.identity,
            reason,
            key: (ticket.descriptor.work.scope, ticket.descriptor.work.work),
            notice: self.ready_notice().clone(),
            request: self.request_notice_for_input(ticket.input),
            health: protocol.execution_health(),
            state: Mutex::new(State {
                ticket: Some(ManuallyDrop::new(ticket)),
                host: Some(ManuallyDrop::new(host.clone())),
                started: false,
                panic: None,
            }),
            retained: Mutex::new(None),
            gate,
        });
        *owner.retained.lock().expect("same original close custody") = Some(owner.clone());
        // Busy retains this same original effect/capture in physical_pending;
        // only a real capacity revision schedules another submission attempt.
        self.queue_task_cleanup(
            TaskIoHostJob::CloseParticipationAdmission(ParticipationAdmissionCloseJob(owner)),
            ports,
        )
        .map_err(|error| TaskIoRefusal {
            error,
            ticket: None,
        })
    }
}
