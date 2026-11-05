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

//! One finite physical observer capture, funded and retained by the original R.

use super::{request_endpoint::RequestNotice, request_ready::ControlRequestReady};
use crate::{TaskExecutionPorts, TaskProtocolEvent};
use novarocks_control_runtime_core::identity::{ScopeId, WorkId};
use std::{
    any::Any,
    mem::ManuallyDrop,
    panic::{AssertUnwindSafe, catch_unwind},
    sync::{Arc, Mutex},
};

pub(crate) const REQUEST_OBSERVATION_CAPTURES: usize = 1;

#[derive(Clone, Copy, Default)]
pub(super) enum ParticipationObservationKind {
    #[default]
    Other,
    Renew,
    Abort,
}

#[derive(Default)]
struct State {
    staged: bool,
    pending: Option<TaskProtocolEvent>,
    in_flight: bool,
    failed: bool,
    // A panic or lost physical job never becomes an idle/exit observation.
    retained: Option<Box<ManuallyDrop<Payload>>>,
    panic: Option<ManuallyDrop<Box<dyn Any + Send>>>,
}
struct Payload {
    ports: TaskExecutionPorts,
    event: TaskProtocolEvent,
}
#[derive(Default)]
pub(super) struct RequestObservation {
    state: Arc<Mutex<State>>,
}
impl RequestObservation {
    pub(super) fn stage(&mut self, event: Option<TaskProtocolEvent>) {
        let mut state = self.state.lock().expect("original R observation capture");
        assert!(!state.staged, "one committed decision per original R");
        state.staged = true;
        state.pending = event;
    }
    pub(super) fn has_pending(&self) -> bool {
        self.state
            .lock()
            .expect("original R observation capture")
            .pending
            .is_some()
    }
    pub(super) fn is_idle(&self) -> bool {
        let state = self.state.lock().expect("original R observation capture");
        state.pending.is_none() && !state.in_flight && !state.failed
    }
    pub(super) fn failed(&self) -> bool {
        self.state
            .lock()
            .expect("original R observation capture")
            .failed
    }
    pub(super) fn take_job(
        &mut self,
        ports: &TaskExecutionPorts,
        scope: ScopeId,
        work: WorkId,
        notice: RequestNotice,
    ) -> Option<RequestObservationJob> {
        let mut state = self.state.lock().expect("original R observation capture");
        if state.in_flight || state.failed {
            return None;
        }
        let event = state.pending.take()?;
        state.in_flight = true;
        Some(RequestObservationJob {
            state: self.state.clone(),
            scope,
            work,
            notice,
            payload: Some(Box::new(ManuallyDrop::new(Payload {
                ports: ports.clone(),
                event,
            }))),
        })
    }
}
#[must_use = "Preserve the same original R observer job on physical queue refusal"]
pub(crate) struct RequestObservationJob {
    state: Arc<Mutex<State>>,
    scope: ScopeId,
    work: WorkId,
    notice: RequestNotice,
    payload: Option<Box<ManuallyDrop<Payload>>>,
}
impl RequestObservationJob {
    pub(crate) fn queue_key(&self) -> (ScopeId, WorkId) {
        (self.scope, self.work)
    }
    pub(crate) fn belongs_to(&self, ready: &ControlRequestReady) -> bool {
        self.notice.belongs_to(ready)
    }
    pub(crate) fn run(mut self) {
        let mut payload = self
            .payload
            .take()
            .expect("one original R observer capture");
        // The original callback and its last capture destruction both run on
        // the physical worker. An unwind retains their unresolved inventory.
        let result = catch_unwind(AssertUnwindSafe(|| {
            payload.ports.observe(payload.event.clone());
            if matches!(payload.event, TaskProtocolEvent::TaskCreateApplied { .. }) {
                payload.ports.record_task_created();
            }
            // SAFETY: the original payload stays at this stable Box address.
            // Its destruction is attempted exactly once on the physical worker.
            // On unwind only the Box pointer moves into unresolved custody; no
            // partly destroyed Payload is moved, read, or destroyed again.
            unsafe {
                ManuallyDrop::drop(payload.as_mut());
            }
        }));
        let mut state = self.state.lock().expect("original R observation capture");
        state.in_flight = false;
        if let Err(panic) = result {
            state.failed = true;
            state.retained = Some(payload);
            state.panic = Some(ManuallyDrop::new(panic));
        }
        drop(state);
        self.notice.notify();
    }
}
impl Drop for RequestObservationJob {
    fn drop(&mut self) {
        if let Some(payload) = self.payload.take() {
            let mut state = self.state.lock().expect("original R observation capture");
            state.in_flight = false;
            state.failed = true;
            state.retained = Some(payload);
            drop(state);
            self.notice.notify();
        }
    }
}

pub(super) fn establish_event(
    operation: novarocks_execution_contract::TaskOperationId,
    body: &novarocks_worker_protocol::control::ControlReplyBody,
) -> Option<TaskProtocolEvent> {
    let novarocks_worker_protocol::control::ControlReplyBody::Participation(ack) = body else {
        return None;
    };
    let receipt = crate::ParticipationOutcome::acknowledged(
        operation,
        ack.outcome,
        ack.receipt.as_ref().clone(),
    );
    TaskProtocolEvent::establish_participation(ack.receipt.participation(), &receipt)
}

pub(super) fn participation_event(
    operation: novarocks_execution_contract::TaskOperationId,
    kind: ParticipationObservationKind,
    body: &novarocks_worker_protocol::participation::ParticipationReplyBody,
    result: novarocks_worker_protocol::participation::RequestResult,
    release_observation: Option<crate::RuntimeFilterReleaseObservation>,
) -> Option<TaskProtocolEvent> {
    use novarocks_worker_protocol::participation::ParticipationReplyBody;
    match body {
        ParticipationReplyBody::Create { outcome, receipt } => {
            let original = crate::CreateTaskOutcome::acknowledged(
                operation,
                *outcome,
                receipt.as_ref().clone(),
            );
            TaskProtocolEvent::create_task(receipt.identity(), &original)
        }
        ParticipationReplyBody::Participation(ack) => {
            let original = crate::ParticipationOutcome::acknowledged(
                operation,
                ack.outcome,
                ack.receipt.as_ref().clone(),
            );
            match kind {
                ParticipationObservationKind::Renew => ack.receipt.lease().and_then(|lease| {
                    TaskProtocolEvent::renew_participation_lease(
                        ack.receipt.participation(),
                        lease.sequence(),
                        &original,
                    )
                }),
                ParticipationObservationKind::Abort => {
                    TaskProtocolEvent::abort_participation(ack.receipt.participation(), &original)
                }
                ParticipationObservationKind::Other => None,
            }
        }
        ParticipationReplyBody::Release {
            participation,
            release,
            state,
            termination_cause,
        } => {
            let novarocks_worker_protocol::participation::RequestResult::Outcome(outcome) = result
            else {
                return None;
            };
            let original = crate::ReleaseParticipationOutcome::acknowledged(
                operation,
                outcome,
                crate::ReleaseAcknowledgement::new(
                    *participation,
                    *release,
                    *state,
                    *termination_cause,
                ),
            );
            TaskProtocolEvent::release_participation(
                *participation,
                &original,
                release_observation?,
            )
        }
        _ => None,
    }
}
