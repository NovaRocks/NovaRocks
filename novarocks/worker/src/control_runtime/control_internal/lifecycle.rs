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
use crate::control_runtime::context_ready::ContextReadyNotice;
use novarocks_control_runtime::driver::{ActorRegistration, ActorRuntime};
use novarocks_control_runtime_core::identity::{InputId, ScopedWorkRef};
use novarocks_execution_contract::QueryContextRef;
use novarocks_worker_protocol::retention::ContextRetirementIdentity;
use std::sync::{Arc, Mutex};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum RootLifecycleFact {
    ExecutionRetired {
        context: QueryContextRef,
        retirement: ContextRetirementIdentity,
    },
    ContextExpired {
        context: QueryContextRef,
        retirement: ContextRetirementIdentity,
    },
    HostRemoved {
        context: QueryContextRef,
        subject: ScopedWorkRef,
    },
}
impl RootLifecycleFact {
    pub(crate) fn context(self) -> QueryContextRef {
        match self {
            Self::ExecutionRetired { context, .. }
            | Self::ContextExpired { context, .. }
            | Self::HostRemoved { context, .. } => context,
        }
    }
    pub(crate) fn retirement(self) -> Option<ContextRetirementIdentity> {
        match self {
            Self::ExecutionRetired { retirement, .. } | Self::ContextExpired { retirement, .. } => {
                Some(retirement)
            }
            Self::HostRemoved { .. } => None,
        }
    }
    pub(super) fn into_command(self) -> ControlInput {
        match self {
            Self::ExecutionRetired {
                context,
                retirement,
            } => ControlInput::ExecutionRetired {
                context,
                retirement,
            },
            Self::ContextExpired {
                context,
                retirement,
            } => ControlInput::ContextExpired {
                context,
                retirement,
            },
            Self::HostRemoved { context, .. } => ControlInput::HostRemoved { context },
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum RootLifecycleConsumption {
    Queued,
    Submitted(InputId),
    Consumed(InputId),
    Retired,
}
/// Clones only read the original source's receipt. Neither consumption nor
/// retirement of this input proves another Work, Scope or host body exited.
#[derive(Clone)]
pub(crate) struct RootLifecycleReceipt(
    Arc<Mutex<RootLifecycleConsumption>>,
    RootLifecycleFact,
    ActorRegistration,
);
impl RootLifecycleReceipt {
    pub(crate) fn fact(&self) -> RootLifecycleFact {
        self.1
    }
    pub(crate) fn consumption(&self) -> RootLifecycleConsumption {
        *self.0.lock().expect("original Root lifecycle receipt")
    }
    pub(crate) fn is_retired(&self) -> bool {
        self.consumption() == RootLifecycleConsumption::Retired
    }
    pub(crate) fn matches_host_removed(
        &self,
        root: &ActorRegistration,
        subject: ScopedWorkRef,
        context: QueryContextRef,
    ) -> bool {
        self.2.same_registration(root)
            && self.1 == (RootLifecycleFact::HostRemoved { context, subject })
    }
}
impl std::fmt::Debug for RootLifecycleReceipt {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_tuple("RootLifecycleReceipt")
            .field(&self.consumption())
            .finish()
    }
}

pub(super) struct PendingLifecycle {
    pub(super) fact: RootLifecycleFact,
    receipt: RootLifecycleReceipt,
    notice: LifecycleNotice,
}
enum LifecycleNotice {
    Context(ContextReadyNotice),
    Root {
        runtime: ActorRuntime<WorkerActor>,
        registration: ActorRegistration,
    },
}
impl LifecycleNotice {
    fn notify(&self) -> Result<(), ControlInternalError> {
        match self {
            Self::Context(notice) => {
                notice.notify();
                Ok(())
            }
            Self::Root {
                runtime,
                registration,
            } => runtime
                .notify_host_actor(registration)
                .map_err(ControlInternalError::Driver),
        }
    }
}
impl PendingLifecycle {
    pub(super) fn new(
        fact: RootLifecycleFact,
        notice: ContextReadyNotice,
        root: &ActorRegistration,
    ) -> (Self, RootLifecycleReceipt) {
        Self::with_notice(fact, LifecycleNotice::Context(notice), root)
    }
    pub(super) fn for_root(
        fact: RootLifecycleFact,
        runtime: &ActorRuntime<WorkerActor>,
        root: &ActorRegistration,
    ) -> (Self, RootLifecycleReceipt) {
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
        fact: RootLifecycleFact,
        notice: LifecycleNotice,
        root: &ActorRegistration,
    ) -> (Self, RootLifecycleReceipt) {
        let receipt = RootLifecycleReceipt(
            Arc::new(Mutex::new(RootLifecycleConsumption::Queued)),
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
        if *state != RootLifecycleConsumption::Queued {
            return Err(ControlInternalError::WrongOwner);
        }
        *state = RootLifecycleConsumption::Submitted(input);
        Ok(())
    }
    pub(super) fn consumed(&self, input: InputId) -> Result<(), ControlInternalError> {
        let mut state = self
            .receipt
            .0
            .lock()
            .expect("original Root lifecycle consumption");
        match *state {
            RootLifecycleConsumption::Submitted(original) if original == input => {
                *state = RootLifecycleConsumption::Consumed(input);
                drop(state);
                self.notice.notify()
            }
            RootLifecycleConsumption::Consumed(original) if original == input => Ok(()),
            _ => Err(ControlInternalError::WrongOwner),
        }
    }
    pub(super) fn retired(&self, input: InputId) -> Result<(), ControlInternalError> {
        let mut state = self
            .receipt
            .0
            .lock()
            .expect("original Root lifecycle retirement");
        if *state != RootLifecycleConsumption::Consumed(input) {
            return Err(ControlInternalError::WrongOwner);
        }
        *state = RootLifecycleConsumption::Retired;
        drop(state);
        self.notice.notify()
    }
}
pub(super) struct InFlightLifecycle {
    pub(super) input: InputId,
    pub(super) original: PendingLifecycle,
}
