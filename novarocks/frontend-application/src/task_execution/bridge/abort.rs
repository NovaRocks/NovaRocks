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

//! One original Control position and submission holder per frozen participation.

use crate::{
    control_runtime::actor::{FrontendActor, FrontendInput},
    task_execution::{abort_effect::NativeAbortEffect, intent::OperationIntent},
};
use novarocks_attempt_protocol::input::AttemptInput;
use novarocks_control_runtime::actor_turn::{ActorTurnError, StandingInputObligation};
use novarocks_control_runtime_core::identity::InputId;
use novarocks_coordination_contract::{
    AttemptActionId, AttemptActivationIdentity, LogicalToAttempt, LogicalToAttemptBody,
};
use novarocks_execution_contract::{ParticipationRef, TaskOperationId};
use novarocks_query_application::coordination::{
    AbortParticipationEffectAdmission, AbortParticipationEffectPort,
    AbortParticipationEffectReservation, AbortParticipationEffectSubmission,
    AbortParticipationIssueIdentity,
};
use std::{
    collections::BTreeMap,
    fmt,
    num::NonZeroU64,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
};
use tokio::sync::watch;

type Position = StandingInputObligation<FrontendActor>;

struct Submitted {
    action: AttemptActionId,
    input: InputId,
    original: NativeAbortEffect,
}

struct Slot {
    position: Position,
    booked: AtomicBool,
    submitted: Mutex<Option<Submitted>>,
}

struct Ingress {
    activation: AttemptActivationIdentity,
    slots: BTreeMap<ParticipationRef, Slot>,
    next_action: AtomicU64,
    closed: AtomicBool,
    capacity: watch::Sender<u64>,
    worker_closed: Option<Arc<dyn Fn(ParticipationRef) + Send + Sync>>,
}

/// The original QA submission remains here even after the Core has processed
/// its passive input. Only its exact settlement may take that submission.
#[must_use]
pub(crate) struct AttemptAbortIngress {
    original: Arc<Ingress>,
}

impl AttemptAbortIngress {
    pub(crate) fn new(
        activation: AttemptActivationIdentity,
        positions: impl IntoIterator<Item = (ParticipationRef, Position)>,
        capacity: watch::Sender<u64>,
        worker_closed: Option<Arc<dyn Fn(ParticipationRef) + Send + Sync>>,
    ) -> Self {
        let mut slots = BTreeMap::new();
        for (participation, position) in positions {
            assert_eq!(participation.query_execution_id(), activation.execution());
            assert!(
                slots
                    .insert(
                        participation,
                        Slot {
                            position,
                            booked: AtomicBool::new(false),
                            submitted: Mutex::new(None),
                        }
                    )
                    .is_none()
            );
        }
        Self {
            original: Arc::new(Ingress {
                activation,
                slots,
                next_action: AtomicU64::new(1),
                closed: AtomicBool::new(false),
                capacity,
                worker_closed,
            }),
        }
    }

    /// The exact Core-selected logical action is the authority to take this
    /// QA submission. A mismatching operation or action leaves the slot intact.
    pub(crate) fn take_submission(
        &self,
        action: AttemptActionId,
        participation: ParticipationRef,
        operation: TaskOperationId,
    ) -> Option<(NativeAbortEffect, AbortInputRetirement)> {
        let slot = self.original.slots.get(&participation)?;
        let mut current = slot.submitted.lock().expect("original Abort submission");
        if !current.as_ref().is_some_and(|row| {
            row.action == action && row.original.intent().operation_id() == operation
        }) {
            return None;
        }
        let submitted = current.take().unwrap();
        Some((
            submitted.original,
            AbortInputRetirement {
                original: self.original.clone(),
                participation,
                input: Some(submitted.input),
            },
        ))
    }

    pub(crate) fn close_external(&self) {
        if !self.original.closed.swap(true, Ordering::AcqRel) {
            publish_capacity(&self.original.capacity);
        }
    }

    /// Unregister the route and join its original callers before consuming
    /// this owner. Refusal keeps every still-live position.
    pub(crate) fn close_after_exit(self) -> Result<(), Self> {
        self.close_external();
        if self
            .original
            .slots
            .values()
            .any(|slot| slot.booked.load(Ordering::Acquire))
        {
            return Err(self);
        }
        let original = Arc::try_unwrap(self.original).map_err(|original| Self { original })?;
        for mut slot in original.slots.into_values() {
            slot.position
                .close_after_exit()
                .expect("exited original Abort Control position");
        }
        Ok(())
    }
}

impl fmt::Debug for AttemptAbortIngress {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("AttemptAbortIngress")
            .field("activation", &self.original.activation)
            .field("participations", &self.original.slots.len())
            .finish_non_exhaustive()
    }
}

impl AbortParticipationEffectPort for AttemptAbortIngress {
    fn subscribe_capacity(&self) -> watch::Receiver<u64> {
        self.original.capacity.subscribe()
    }

    fn try_reserve(
        &self,
        identity: AbortParticipationIssueIdentity,
    ) -> AbortParticipationEffectAdmission {
        if identity.activation() != self.original.activation
            || self.original.closed.load(Ordering::Acquire)
        {
            return AbortParticipationEffectAdmission::Closed;
        }
        let Some(slot) = self.original.slots.get(&identity.participation()) else {
            return AbortParticipationEffectAdmission::Closed;
        };
        if slot
            .booked
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_err()
        {
            return AbortParticipationEffectAdmission::Backpressured;
        }
        if self.original.closed.load(Ordering::Acquire) {
            slot.booked.store(false, Ordering::Release);
            return AbortParticipationEffectAdmission::Closed;
        }
        let action =
            self.original
                .next_action
                .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |next| {
                    next.checked_add(1)
                });
        let Ok(action) = action else {
            self.close_external();
            slot.booked.store(false, Ordering::Release);
            return AbortParticipationEffectAdmission::Closed;
        };
        AbortParticipationEffectAdmission::Admitted(Box::new(AbortReservation {
            original: self.original.clone(),
            identity,
            action: AttemptActionId::new(NonZeroU64::new(action).unwrap()),
            owns_position: true,
        }))
    }
}

struct AbortReservation {
    original: Arc<Ingress>,
    identity: AbortParticipationIssueIdentity,
    action: AttemptActionId,
    owns_position: bool,
}

impl fmt::Debug for AbortReservation {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("AbortReservation")
            .field("identity", &self.identity)
            .field("action", &self.action)
            .finish_non_exhaustive()
    }
}

impl AbortParticipationEffectReservation for AbortReservation {
    fn submit(mut self: Box<Self>, submission: AbortParticipationEffectSubmission) {
        assert_eq!(submission.identity(), self.identity);
        let slot = &self.original.slots[&self.identity.participation()];
        let original =
            NativeAbortEffect::from_submission(submission, self.original.worker_closed.clone());
        let OperationIntent::AbortParticipation(request) = original.intent() else {
            unreachable!("original Abort intent");
        };
        let input = FrontendInput::Attempt(Box::new(AttemptInput::Port(LogicalToAttempt {
            activation: self.original.activation,
            body: LogicalToAttemptBody::Abort {
                action: self.action,
                request: *request,
            },
        })));
        // Install the original submission under the same lock its consumer
        // uses. An immediately woken Core cannot settle an unbound action.
        let mut current = slot.submitted.lock().expect("original Abort submission");
        assert!(current.is_none());
        match slot.position.fill(input) {
            Ok(input) => {
                *current = Some(Submitted {
                    action: self.action,
                    input,
                    original,
                });
                self.owns_position = false;
            }
            Err(_) => {
                drop(current);
                // The original submission's Drop sends FailedClosed to QA.
                drop(original);
            }
        }
    }
}

impl Drop for AbortReservation {
    fn drop(&mut self) {
        if self.owns_position {
            let slot = &self.original.slots[&self.identity.participation()];
            assert!(slot.booked.swap(false, Ordering::AcqRel));
            publish_capacity(&self.original.capacity);
        }
    }
}

/// Credit is returned only after this exact runtime occurrence and its
/// effects are retired. Settling the QA submission does not supply that fact.
#[must_use]
pub(crate) struct AbortInputRetirement {
    original: Arc<Ingress>,
    participation: ParticipationRef,
    input: Option<InputId>,
}

impl AbortInputRetirement {
    pub(crate) fn retire_ready(&mut self) -> Result<bool, ActorTurnError> {
        let Some(expected) = self.input else {
            return Ok(true);
        };
        let slot = &self.original.slots[&self.participation];
        if slot
            .position
            .in_flight()?
            .is_some_and(|input| input != expected)
        {
            return Err(ActorTurnError::InputAuthorMismatch);
        }
        if !slot.position.retire_ready_after_exit()? {
            return Ok(false);
        }
        self.input = None;
        assert!(slot.booked.swap(false, Ordering::AcqRel));
        publish_capacity(&self.original.capacity);
        Ok(true)
    }
}

fn publish_capacity(capacity: &watch::Sender<u64>) {
    capacity.send_modify(|epoch| {
        *epoch = epoch
            .checked_add(1)
            .expect("Abort capacity epoch exhausted");
    });
}
