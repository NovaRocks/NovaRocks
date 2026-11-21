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

//! Only this adapter knows Actor routing and its original paid causal result.
//! Replacement qualification supplies its QA leg directly to the same kernel.
//! Test fixture custody for one actual admission or Establish launch job.
//!
//! Mount only beneath the cfg(test) transport module. The caller must retain
//! this decorator together with its SAME original Source/Host tracker in the
//! whole-run guard. A held launch is not a start, report, Join or Scope exit.

use super::{ActorTransportWork, BoundActorTransport};
use crate::control_runtime::{
    actor::FrontendEffect,
    host::{EffectExecutor, PreparedEffectJob},
};
use novarocks_attempt_protocol::effect::{EffectBody, OperationIntent};
use novarocks_coordination_contract::AttemptActionId;
use novarocks_execution_contract::{ParticipationRef, TaskOperationId};
use std::{
    sync::{Arc, Mutex, OnceLock, Weak},
    task::Waker,
};

/// Passive keys read from the actual bound job, never an issue authority.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct OriginalHeldSend {
    pub(crate) participation: ParticipationRef,
    pub(crate) operation: TaskOperationId,
    pub(crate) action: AttemptActionId,
    pub(crate) result_actor: novarocks_control_runtime_core::identity::ActorId,
    pub(crate) result_position: novarocks_control_runtime_core::identity::InputId,
    pub(crate) result_work: novarocks_control_runtime_core::identity::ScopedWorkRef,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum OriginalSendRelease {
    NotHeld,
    Busy,
    Accepted(OriginalHeldSend),
    Refused(OriginalHeldSend),
}

struct HeldOriginal {
    metadata: OriginalHeldSend,
    job: PreparedEffectJob,
}

#[derive(Default)]
struct OriginalHandoffState {
    held: Option<HeldOriginal>,
    releasing: bool,
    captured: bool,
    additional_matches: usize,
    accepted: Option<OriginalHeldSend>,
}

#[derive(Clone, Copy)]
pub(crate) enum OriginalSendTarget {
    AdmissionTicket(ParticipationRef),
    #[cfg(novarocks_dev_controls)]
    Establish(ParticipationRef),
}

impl OriginalSendTarget {
    fn matched_participation(self, operation: &OperationIntent) -> Option<ParticipationRef> {
        match (self, operation) {
            (
                Self::AdmissionTicket(expected),
                OperationIntent::AcquireParticipationAdmissionTicket(request),
            ) if request.participation() == expected => Some(expected),
            #[cfg(novarocks_dev_controls)]
            (
                Self::Establish(expected),
                OperationIntent::EstablishParticipation { participation, .. },
            ) if *participation == expected => Some(expected),
            _ => None,
        }
    }
}

pub(crate) struct OriginalSendHandoffFixture<'a> {
    pub(crate) target: OriginalSendTarget,
    pub(crate) observation: &'a Mutex<Weak<OriginalSendHandoff>>,
}

impl OriginalSendHandoffFixture<'_> {
    pub(crate) fn decorate(
        self,
        original: Arc<dyn EffectExecutor>,
        wake: Waker,
    ) -> Arc<dyn EffectExecutor> {
        let decorator = OriginalSendHandoff::new(original, self.target, wake);
        *self.observation.lock().unwrap() = Arc::downgrade(&decorator);
        decorator
    }
}

/// One bounded slot for the SAME paid job. Ordinary jobs use the original
/// executor unchanged; this fixture cannot create or poll a transport body.
pub(crate) struct OriginalSendHandoff {
    original: Arc<dyn EffectExecutor>,
    target: OriginalSendTarget,
    /// Issued by the same registered Host, not a synthetic test wake.
    wake: Waker,
    state: Mutex<OriginalHandoffState>,
}

impl OriginalSendHandoff {
    fn new(
        original: Arc<dyn EffectExecutor>,
        target: OriginalSendTarget,
        wake: Waker,
    ) -> Arc<Self> {
        Arc::new(Self {
            original,
            target,
            wake,
            state: Mutex::new(OriginalHandoffState::default()),
        })
    }

    pub(crate) fn held(&self) -> Option<OriginalHeldSend> {
        self.state
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .held
            .as_ref()
            .map(|held| held.metadata)
    }

    pub(crate) fn held_handoff_state(
        &self,
    ) -> Option<novarocks_control_runtime::exit_work::HandoffState> {
        let state = self.state.lock().unwrap_or_else(|error| error.into_inner());
        let FrontendEffect::Attempt(effect) = &state.held.as_ref()?.job.effect else {
            return None;
        };
        Some(effect.handoff()?.state())
    }

    /// Successful return observes only the original executor's acceptance.
    /// The Source still owes its actual result, body/captures, Join and header.
    pub(crate) fn accepted(&self) -> Option<OriginalHeldSend> {
        self.state
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .accepted
    }

    /// The complete job stays in this slot if the original executor refuses.
    /// No rich callback or original-job destructor runs under the slot lock.
    pub(crate) fn release(&self) -> OriginalSendRelease {
        let held = {
            let mut state = self.state.lock().unwrap_or_else(|error| error.into_inner());
            if state.releasing {
                return OriginalSendRelease::Busy;
            }
            let Some(held) = state.held.take() else {
                return OriginalSendRelease::NotHeld;
            };
            state.releasing = true;
            held
        };
        let metadata = held.metadata;
        match self.original.handoff(held.job) {
            Ok(()) => {
                {
                    let mut state = self.state.lock().unwrap_or_else(|error| error.into_inner());
                    state.releasing = false;
                    state.accepted = Some(metadata);
                }
                // One real original-job handoff, never an empty scan or Full,
                // notifies only the same registered Host.
                self.wake.wake_by_ref();
                OriginalSendRelease::Accepted(metadata)
            }
            Err(job) => {
                let mut state = self.state.lock().unwrap_or_else(|error| error.into_inner());
                state.releasing = false;
                state.held = Some(HeldOriginal { metadata, job });
                // The original executor owns its availability subscription.
                // Refusal alone never creates a self-wake or new Work.
                OriginalSendRelease::Refused(metadata)
            }
        }
    }

    /// Extra matches go to the original executor and remain an independent
    /// oracle. This fixture never manufactures target-specific Full.
    pub(crate) fn additional_matches(&self) -> usize {
        self.state
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .additional_matches
    }

    fn original_send(&self, job: &PreparedEffectJob) -> Option<OriginalHeldSend> {
        let FrontendEffect::Attempt(effect) = &job.effect else {
            return None;
        };
        let EffectBody::SendBatch { batch, .. } = &effect.protocol.body else {
            return None;
        };
        let result_author = job.result.event_author().ok()?;
        let mut original = batch.operations.iter().filter_map(|operation| {
            let participation = self.target.matched_participation(operation)?;
            Some(OriginalHeldSend {
                participation,
                operation: operation.operation_id(),
                action: effect.protocol.id,
                result_actor: result_author.actor(),
                result_position: result_author.input(),
                result_work: result_author.work(),
            })
        });
        let metadata = original.next()?;
        // A batch with two matching operations is not the one-operation cut.
        if original.next().is_some() || job.transport.is_some() {
            return None;
        }
        let bound = job.captures.downcast_ref::<BoundActorTransport>()?;
        let ActorTransportWork::Send(send) = &bound.work else {
            return None;
        };
        let reports = send.reports.actor()?;
        if !Arc::ptr_eq(effect, &bound.effect)
            || !bound.launch.owns_exit(&job.exit)
            || !send.resolved.matches_effect(effect)
            || !reports.matches(
                effect.protocol.id,
                &send.resolved.keys,
                &send.resolved.operations,
            )
        {
            return None;
        }
        Some(metadata)
    }
}

impl EffectExecutor for OriginalSendHandoff {
    fn handoff(&self, job: PreparedEffectJob) -> Result<(), PreparedEffectJob> {
        let Some(metadata) = self.original_send(&job) else {
            return self.original.handoff(job);
        };
        let mut state = self.state.lock().unwrap_or_else(|error| error.into_inner());
        if state.captured {
            state.additional_matches += 1;
            drop(state);
            return self.original.handoff(job);
        }
        state.captured = true;
        state.held = Some(HeldOriginal { metadata, job });
        Ok(())
    }
}

/// Last-resort original custody only. It makes no successful-cleanup claim:
/// the caller's whole-run guard must also retain the original Source/Host
/// tracker because that separate owner still holds TransportRetirement.
struct UnresolvedOriginalSend {
    _executor: Arc<dyn EffectExecutor>,
    _held: HeldOriginal,
}

impl Drop for OriginalSendHandoff {
    fn drop(&mut self) {
        let state = self
            .state
            .get_mut()
            .unwrap_or_else(|error| error.into_inner());
        if let Some(held) = state.held.take() {
            static UNRESOLVED: OnceLock<Mutex<Vec<UnresolvedOriginalSend>>> = OnceLock::new();
            UNRESOLVED
                .get_or_init(|| Mutex::new(Vec::new()))
                .lock()
                .unwrap_or_else(|error| error.into_inner())
                .push(UnresolvedOriginalSend {
                    _executor: Arc::clone(&self.original),
                    _held: held,
                });
        }
    }
}
