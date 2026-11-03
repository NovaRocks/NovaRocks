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

//! An original Participation's host wake is only registration-qualified metadata.
//! Common owns its one host ready queue; there is no Participation scheduler here.

use super::actors::WorkerActor;
#[cfg(test)]
use novarocks_control_runtime::transaction::RuntimeError;
use novarocks_control_runtime::{
    actor_turn::{ActorRegistration, ActorRuntime, ActorTurnError},
    transaction::ScopeHandle,
};
use novarocks_execution_contract::ParticipationRef;
use std::sync::{
    Arc,
    atomic::{AtomicU64, Ordering},
};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ParticipationReadyError {
    Duplicate,
    ActorTurn(ActorTurnError),
    #[cfg(test)]
    Runtime(RuntimeError),
}
struct Endpoint {
    runtime: ActorRuntime<WorkerActor>,
    participation: ParticipationRef,
    _scope: ScopeHandle,
    registration: ActorRegistration,
    version: AtomicU64,
}
/// Clones retain only the original address. They grant no Work, capacity,
/// publication, physical exit, or a second host dispatch claim.
#[derive(Clone)]
pub(crate) struct ParticipationReadyNotice(Arc<Endpoint>);
impl ParticipationReadyNotice {
    pub(super) fn for_registered(
        runtime: &ActorRuntime<WorkerActor>,
        participation: ParticipationRef,
        scope: &ScopeHandle,
        registration: &ActorRegistration,
    ) -> Result<Self, ParticipationReadyError> {
        if !registration.matches_scope(scope) {
            return Err(ParticipationReadyError::ActorTurn(
                ActorTurnError::EffectTicketOriginMismatch,
            ));
        }
        runtime
            .validate_registration(registration)
            .map_err(ParticipationReadyError::ActorTurn)?;
        Ok(Self(Arc::new(Endpoint {
            runtime: runtime.clone(),
            participation,
            _scope: scope.clone(),
            registration: registration.clone(),
            version: AtomicU64::new(0),
        })))
    }
    #[cfg(test)]
    pub(crate) fn same_origin(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.0, &other.0)
    }
    /// Read only the original runtime and actor registration binding. Public
    /// actor or Scope numbers alone cannot identify this notice's owner.
    pub(super) fn matches_registration(
        &self,
        runtime: &ActorRuntime<WorkerActor>,
        registration: &ActorRegistration,
    ) -> bool {
        self.0.runtime.same_runtime(runtime) && self.0.registration.same_registration(registration)
    }
    pub(crate) fn participation(&self) -> ParticipationRef {
        self.0.participation
    }
    /// Wake ordering only. Neither this revision nor a comparison authorizes
    /// admission, a callback, publication, or any resource exit.
    pub(super) fn version(&self) -> u64 {
        self.0.version.load(Ordering::Acquire)
    }
    pub(super) fn notify(&self) {
        self.0
            .version
            .fetch_update(Ordering::Release, Ordering::Relaxed, |version| {
                version.checked_add(1)
            })
            .unwrap_or_else(|_| std::process::abort());
        match self.0.runtime.notify_host_actor(&self.0.registration) {
            Ok(())
            | Err(ActorTurnError::UnknownActor | ActorTurnError::EffectTicketOriginMismatch) => {}
            Err(_) => std::process::abort(),
        }
    }
    /// Actual Scope removal closes this source's useful lifetime. This check
    /// never retires Work, revokes another producer, or refunds any fee.
    #[cfg(test)]
    pub(super) fn unregister_removed(&self) -> Result<(), ParticipationReadyError> {
        match self.0._scope.gates() {
            Err(RuntimeError::UnknownScope) => Ok(()),
            Ok(_) => Err(ParticipationReadyError::Runtime(
                RuntimeError::ScopeNotRetired,
            )),
            Err(error) => Err(ParticipationReadyError::Runtime(error)),
        }
    }
}
