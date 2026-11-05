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

//! The shared directory stores addresses; each original Participation owns its own lock.

use super::{ParticipationHostError, ParticipationHostRecord, WorkerAction, WorkerActor};
use crate::control_runtime::participation_host::OwnerFenceDelivery;
use novarocks_control_runtime::actor_turn::{ActorHostTurn, ActorRegistration, EffectTicket};
use novarocks_control_runtime_core::identity::ActorId;
use novarocks_execution_contract::ParticipationRef;
use novarocks_worker_protocol::control::ControlEffect;
use std::{
    collections::BTreeMap,
    ops::{Deref, DerefMut},
    sync::{Arc, Mutex, MutexGuard},
};

struct ParticipationCell {
    participation: ParticipationRef,
    registration: ActorRegistration,
    host: Mutex<Option<ParticipationHostRecord>>,
}
/// An address never grants a second owner, admission, or an exit fact.
#[derive(Clone)]
pub(crate) struct ParticipationHostAddress(Arc<ParticipationCell>);
pub(crate) struct ParticipationHostGuard<'a>(MutexGuard<'a, Option<ParticipationHostRecord>>);
impl Deref for ParticipationHostGuard<'_> {
    type Target = ParticipationHostRecord;
    fn deref(&self) -> &Self::Target {
        self.0
            .as_ref()
            .expect("locked original Participation owner")
    }
}
impl DerefMut for ParticipationHostGuard<'_> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.0
            .as_mut()
            .expect("locked original Participation owner")
    }
}
impl ParticipationHostGuard<'_> {
    /// Keep this same cell locked until its original parent owner has moved
    /// to Root. A concurrent Root lookup then sees either owner, never a gap.
    pub(in crate::control_runtime) fn take_for_parent(&mut self) -> ParticipationHostRecord {
        self.0
            .take()
            .expect("same original Participation parent owner")
    }
}
impl ParticipationHostAddress {
    pub(super) fn new(host: ParticipationHostRecord) -> Self {
        Self(Arc::new(ParticipationCell {
            participation: host.identity(),
            registration: host.registration().clone(),
            host: Mutex::new(Some(host)),
        }))
    }
    pub(crate) fn participation(&self) -> ParticipationRef {
        self.0.participation
    }
    pub(in crate::control_runtime) fn registration(&self) -> &ActorRegistration {
        &self.0.registration
    }
    pub(crate) fn lock(&self) -> Option<ParticipationHostGuard<'_>> {
        let host = self
            .0
            .host
            .lock()
            .expect("original Participation owner lock");
        host.is_some().then_some(ParticipationHostGuard(host))
    }
    /// A management observation never waits for an actor/host turn.
    pub(in crate::control_runtime) fn try_lock(&self) -> Option<ParticipationHostGuard<'_>> {
        let host = self.0.host.try_lock().ok()?;
        host.is_some().then_some(ParticipationHostGuard(host))
    }
    #[cfg(test)]
    pub(super) fn take(&self) -> Option<ParticipationHostRecord> {
        // Returning the value moves its actual custody out of the cell. No
        // resource or foreign destructor executes under the directory lock.
        self.0
            .host
            .lock()
            .expect("original Participation owner lock")
            .take()
    }
    fn same_origin(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.0, &other.0)
    }
}
#[derive(Default)]
pub(super) struct DirectoryState {
    pub participations: BTreeMap<ParticipationRef, ParticipationHostAddress>,
    pub actors: BTreeMap<ActorId, ParticipationRef>,
    pub executions: BTreeMap<novarocks_types::QueryExecutionId, ParticipationRef>,
}
/// Clone this handle before a host turn; lookup releases the short directory
/// lock before any caller locks the addressed Participation's original inventory.
#[derive(Clone, Default)]
pub(crate) struct ParticipationDirectory(Arc<Mutex<DirectoryState>>);
impl ParticipationDirectory {
    /// Both sparse indices must be empty before the process owner can retire.
    /// This observes actual address inventory, never a Participation's exit fact.
    pub(crate) fn is_empty(&self) -> bool {
        let state = self.state();
        state.participations.is_empty() && state.actors.is_empty() && state.executions.is_empty()
    }
    /// Neutral address inventory only; it grants no execution capacity and
    /// proves no Participation/Scope exit.
    pub(in crate::control_runtime) fn len(&self) -> usize {
        self.state().participations.len()
    }
    pub(super) fn state(&self) -> MutexGuard<'_, DirectoryState> {
        self.0.lock().expect("Participation address directory lock")
    }
    pub(crate) fn participation(
        &self,
        participation: ParticipationRef,
    ) -> Option<ParticipationHostAddress> {
        self.state().participations.get(&participation).cloned()
    }
    /// Task wire identities omit the Frontend process. Resolve that component
    /// only from this exact held execution's original Participation, never by scan or
    /// by inventing a Frontend identity from the Task.
    pub(crate) fn task_participation(
        &self,
        task: novarocks_execution_contract::TaskIdentity,
    ) -> Option<ParticipationRef> {
        let state = self.state();
        let participation = *state.executions.get(&task.query_execution_id())?;
        task.verify_participation(participation).ok()?;
        Some(participation)
    }
    /// The Root dispatcher can release its own inventory lock before this
    /// target handoff. The caller retains its original effect ticket, and a
    /// missing target is a refusal rather than a fabricated fence receipt.
    pub(crate) fn handoff_owner_fence(
        &self,
        ticket: &EffectTicket<WorkerAction>,
    ) -> Result<OwnerFenceDelivery, ParticipationHostError> {
        let WorkerAction::Control {
            effect: ControlEffect::AbortParticipation { participation, .. },
            ..
        } = &ticket.effect
        else {
            return Err(ParticipationHostError::WrongEffect);
        };
        let address = self
            .participation(*participation)
            .ok_or(ParticipationHostError::WrongParticipation)?;
        let mut host = address
            .lock()
            .ok_or(ParticipationHostError::WrongParticipation)?;
        host.handoff_owner_fence(ticket)
    }
    pub(crate) fn host_turn(
        &self,
        turn: &ActorHostTurn<WorkerActor>,
    ) -> Option<ParticipationHostAddress> {
        let state = self.state();
        let participation = state.actors.get(&turn.actor())?;
        state
            .participations
            .get(participation)
            .filter(|address| turn.matches_registration(&address.0.registration))
            .cloned()
    }
    pub(in crate::control_runtime) fn remove_original(&self, address: &ParticipationHostAddress) {
        let mut state = self.state();
        let participation = address.participation();
        assert!(
            state
                .participations
                .get(&participation)
                .is_some_and(|original| original.same_origin(address)),
            "only the same physically removed Participation can leave its directory"
        );
        state.participations.remove(&participation);
        assert_eq!(
            state.executions.remove(&participation.query_execution_id()),
            Some(participation)
        );
        assert_eq!(
            state.actors.remove(&address.0.registration.actor()),
            Some(participation)
        );
    }
}

impl ParticipationDirectory {
    /// Copy actual metadata addresses under the short directory lock only.
    /// A published cell's original owner is taken before its address is removed.
    pub(in crate::control_runtime) fn try_census_addresses(
        &self,
    ) -> Option<Vec<ParticipationHostAddress>> {
        let state = self.0.try_lock().ok()?;
        Some(state.participations.values().cloned().collect())
    }
    /// No Participation lock is held here. Same-ID replacement cannot reuse a cell
    /// while the original metadata address remains in the observed list.
    pub(in crate::control_runtime) fn census_addresses_unchanged(
        &self,
        original: &[ParticipationHostAddress],
    ) -> bool {
        let Ok(state) = self.0.try_lock() else {
            return false;
        };
        state.participations.len() == original.len()
            && original.iter().all(|address| {
                state
                    .participations
                    .get(&address.participation())
                    .is_some_and(|current| current.same_origin(address))
            })
    }
}
