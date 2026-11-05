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

//! Original lifecycle facts serialized through Root's existing internal input.
//! Receipts bind their actual Root and observe that same input. A removal's
//! subject Work is its original owner, never this input's financial author.

use super::{ControlInput, ControlInternalError};
use crate::control_runtime::actors::WorkerActor;
use crate::control_runtime::participation_ready::ParticipationReadyNotice;
use novarocks_control_runtime::actor_turn::{ActorRegistration, ActorRuntime};
use novarocks_control_runtime_core::identity::{InputId, ScopedWorkRef};
use novarocks_execution_contract::ParticipationRef;
use novarocks_worker_protocol::retention::ParticipationRetirementIdentity;
use std::sync::{Arc, Mutex};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ControlLifecycleFact {
    ExecutionRetired {
        participation: ParticipationRef,
        retirement: ParticipationRetirementIdentity,
    },
    ParticipationExpired {
        participation: ParticipationRef,
        retirement: ParticipationRetirementIdentity,
    },
    HostRemoved {
        participation: ParticipationRef,
        subject: ScopedWorkRef,
    },
}
impl ControlLifecycleFact {
    pub(crate) fn participation(self) -> ParticipationRef {
        match self {
            Self::ExecutionRetired { participation, .. }
            | Self::ParticipationExpired { participation, .. }
            | Self::HostRemoved { participation, .. } => participation,
        }
    }
    pub(crate) fn retirement(self) -> Option<ParticipationRetirementIdentity> {
        match self {
            Self::ExecutionRetired { retirement, .. }
            | Self::ParticipationExpired { retirement, .. } => Some(retirement),
            Self::HostRemoved { .. } => None,
        }
    }
    pub(super) fn into_command(self) -> ControlInput {
        match self {
            Self::ExecutionRetired {
                participation,
                retirement,
            } => ControlInput::ExecutionRetired {
                participation,
                retirement,
            },
            Self::ParticipationExpired {
                participation,
                retirement,
            } => ControlInput::ParticipationExpired {
                participation,
                retirement,
            },
            Self::HostRemoved { participation, .. } => ControlInput::HostRemoved { participation },
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ControlLifecycleConsumption {
    Queued,
    Submitted(InputId),
    Consumed(InputId),
    Retired,
}
/// Clones only read the original source's receipt. Neither consumption nor
/// retirement of this input proves another Work, Scope or host body exited.
#[derive(Clone)]
pub(crate) struct ControlLifecycleReceipt(
    Arc<Mutex<ControlLifecycleConsumption>>,
    ControlLifecycleFact,
    ActorRegistration,
);
impl ControlLifecycleReceipt {
    pub(crate) fn fact(&self) -> ControlLifecycleFact {
        self.1
    }
    pub(crate) fn consumption(&self) -> ControlLifecycleConsumption {
        *self.0.lock().expect("original Root lifecycle receipt")
    }
    pub(crate) fn is_retired(&self) -> bool {
        self.consumption() == ControlLifecycleConsumption::Retired
    }
    pub(crate) fn matches_host_removed(
        &self,
        root: &ActorRegistration,
        subject: ScopedWorkRef,
        participation: ParticipationRef,
    ) -> bool {
        self.2.same_registration(root)
            && self.1
                == (ControlLifecycleFact::HostRemoved {
                    participation,
                    subject,
                })
    }
}
impl std::fmt::Debug for ControlLifecycleReceipt {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_tuple("RootLifecycleReceipt")
            .field(&self.consumption())
            .finish()
    }
}

pub(super) struct PendingLifecycle {
    pub(super) fact: ControlLifecycleFact,
    receipt: ControlLifecycleReceipt,
    notice: LifecycleNotice,
}
enum LifecycleNotice {
    Participation(ParticipationReadyNotice),
    Root {
        runtime: ActorRuntime<WorkerActor>,
        registration: ActorRegistration,
    },
}
impl LifecycleNotice {
    fn notify(&self) -> Result<(), ControlInternalError> {
        match self {
            Self::Participation(notice) => {
                notice.notify();
                Ok(())
            }
            Self::Root {
                runtime,
                registration,
            } => runtime
                .notify_host_actor(registration)
                .map_err(ControlInternalError::ActorTurn),
        }
    }
}
impl PendingLifecycle {
    pub(super) fn new(
        fact: ControlLifecycleFact,
        notice: ParticipationReadyNotice,
        root: &ActorRegistration,
    ) -> (Self, ControlLifecycleReceipt) {
        Self::with_notice(fact, LifecycleNotice::Participation(notice), root)
    }
    pub(super) fn for_root(
        fact: ControlLifecycleFact,
        runtime: &ActorRuntime<WorkerActor>,
        root: &ActorRegistration,
    ) -> (Self, ControlLifecycleReceipt) {
        Self::with_notice(
            fact,
            LifecycleNotice::Root {
                runtime: runtime.clone(),
                registration: root.clone(),
            },
            root,
        )
    }
    fn with_notice(
        fact: ControlLifecycleFact,
        notice: LifecycleNotice,
        root: &ActorRegistration,
    ) -> (Self, ControlLifecycleReceipt) {
        let receipt = ControlLifecycleReceipt(
            Arc::new(Mutex::new(ControlLifecycleConsumption::Queued)),
            fact,
            root.clone(),
        );
        (
            Self {
                fact,
                receipt: receipt.clone(),
                notice,
            },
            receipt,
        )
    }
    pub(super) fn submitted(&self, input: InputId) -> Result<(), ControlInternalError> {
        let mut state = self
            .receipt
            .0
            .lock()
            .expect("original Root lifecycle submission");
        if *state != ControlLifecycleConsumption::Queued {
            return Err(ControlInternalError::WrongOwner);
        }
        *state = ControlLifecycleConsumption::Submitted(input);
        Ok(())
    }
    pub(super) fn consumed(&self, input: InputId) -> Result<(), ControlInternalError> {
        let mut state = self
            .receipt
            .0
            .lock()
            .expect("original Root lifecycle consumption");
        match *state {
            ControlLifecycleConsumption::Submitted(original) if original == input => {
                *state = ControlLifecycleConsumption::Consumed(input);
                drop(state);
                self.notice.notify()
            }
            ControlLifecycleConsumption::Consumed(original) if original == input => Ok(()),
            _ => Err(ControlInternalError::WrongOwner),
        }
    }
    pub(super) fn retired(&self, input: InputId) -> Result<(), ControlInternalError> {
        let mut state = self
            .receipt
            .0
            .lock()
            .expect("original Root lifecycle retirement");
        if *state != ControlLifecycleConsumption::Consumed(input) {
            return Err(ControlInternalError::WrongOwner);
        }
        *state = ControlLifecycleConsumption::Retired;
        drop(state);
        self.notice.notify()
    }
}
pub(super) struct InFlightLifecycle {
    pub(super) input: InputId,
    pub(super) original: PendingLifecycle,
}
