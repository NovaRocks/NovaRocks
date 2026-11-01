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

//! The shared directory stores addresses; each original Context owns its own lock.

use super::{ContextHostError, ContextHostRecord, WorkerAction, WorkerActor};
use crate::control_runtime::context_host::OwnerFenceDelivery;
use novarocks_control_runtime::driver::{ActorHostTurn, ActorRegistration, EffectTicket};
use novarocks_control_runtime_core::identity::ActorId;
use novarocks_execution_contract::QueryContextRef;
use novarocks_worker_protocol::control::ControlEffect;
use std::{
    collections::BTreeMap,
    ops::{Deref, DerefMut},
    sync::{Arc, Mutex, MutexGuard},
};

struct ContextCell {
    context: QueryContextRef,
    registration: ActorRegistration,
    host: Mutex<Option<ContextHostRecord>>,
}
/// An address never grants a second owner, admission, or an exit fact.
#[derive(Clone)]
pub(crate) struct ContextHostAddress(Arc<ContextCell>);
pub(crate) struct ContextHostGuard<'a>(MutexGuard<'a, Option<ContextHostRecord>>);
impl Deref for ContextHostGuard<'_> {
    type Target = ContextHostRecord;
    fn deref(&self) -> &Self::Target {
        self.0.as_ref().expect("locked original Context owner")
    }
}
impl DerefMut for ContextHostGuard<'_> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.0.as_mut().expect("locked original Context owner")
    }
}
impl ContextHostGuard<'_> {
    /// Keep this same cell locked until its original parent owner has moved
    /// to Root. A concurrent Root lookup then sees either owner, never a gap.
    pub(in crate::control_runtime) fn take_for_parent(&mut self) -> ContextHostRecord {
        self.0.take().expect("same original Context parent owner")
    }
}
impl ContextHostAddress {
    pub(super) fn new(host: ContextHostRecord) -> Self {
        Self(Arc::new(ContextCell {
            context: host.identity(),
            registration: host.registration().clone(),
            host: Mutex::new(Some(host)),
        }))
    }
    pub(crate) fn context(&self) -> QueryContextRef {
        self.0.context
    }
    pub(in crate::control_runtime) fn registration(&self) -> &ActorRegistration {
        &self.0.registration
    }
    pub(crate) fn lock(&self) -> Option<ContextHostGuard<'_>> {
        let host = self.0.host.lock().expect("original Context owner lock");
        host.is_some().then_some(ContextHostGuard(host))
    }
    /// A management observation never waits for an actor/host turn.
    pub(in crate::control_runtime) fn try_lock(&self) -> Option<ContextHostGuard<'_>> {
        let host = self.0.host.try_lock().ok()?;
        host.is_some().then_some(ContextHostGuard(host))
    }
    #[cfg(test)]
    pub(super) fn take(&self) -> Option<ContextHostRecord> {
        // Returning the value moves its actual custody out of the cell. No
        // resource or foreign destructor executes under the directory lock.
        self.0
            .host
            .lock()
            .expect("original Context owner lock")
            .take()
    }
    fn same_origin(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.0, &other.0)
    }
}
#[derive(Default)]
pub(super) struct DirectoryState {
    pub contexts: BTreeMap<QueryContextRef, ContextHostAddress>,
    pub actors: BTreeMap<ActorId, QueryContextRef>,
    pub executions: BTreeMap<novarocks_types::QueryExecutionId, QueryContextRef>,
}
/// Clone this handle before a host turn; lookup releases the short directory
/// lock before any caller locks the addressed Context's original inventory.
#[derive(Clone, Default)]
pub(crate) struct ContextDirectory(Arc<Mutex<DirectoryState>>);
impl ContextDirectory {
    /// Both sparse indices must be empty before the process owner can retire.
    /// This observes actual address inventory, never a Context's exit fact.
    pub(crate) fn is_empty(&self) -> bool {
        let state = self.state();
        state.contexts.is_empty() && state.actors.is_empty() && state.executions.is_empty()
    }
    /// Neutral address inventory only; it grants no execution capacity and
    /// proves no Context/Scope exit.
    pub(in crate::control_runtime) fn len(&self) -> usize {
        self.state().contexts.len()
    }
    pub(super) fn state(&self) -> MutexGuard<'_, DirectoryState> {
        self.0.lock().expect("Context address directory lock")
    }
    pub(crate) fn context(&self, context: QueryContextRef) -> Option<ContextHostAddress> {
        self.state().contexts.get(&context).cloned()
    }
    /// Task wire identities omit the Frontend process. Resolve that component
    /// only from this exact held execution's original Context, never by scan or
    /// by inventing a Frontend identity from the Task.
    pub(crate) fn task_context(
        &self,
        task: novarocks_execution_contract::TaskIdentity,
    ) -> Option<QueryContextRef> {
        let state = self.state();
        let context = *state.executions.get(&task.query_execution_id())?;
        task.verify_query_context(context).ok()?;
        Some(context)
    }
    /// The Root dispatcher can release its own inventory lock before this
    /// target handoff. The caller retains its original effect ticket, and a
    /// missing target is a refusal rather than a fabricated fence receipt.
    pub(crate) fn handoff_owner_fence(
        &self,
        ticket: &EffectTicket<WorkerAction>,
    ) -> Result<OwnerFenceDelivery, ContextHostError> {
        let WorkerAction::Control {
            effect: ControlEffect::AbortContext { context, .. },
            ..
        } = &ticket.effect
        else {
            return Err(ContextHostError::WrongEffect);
        };
        let address = self
            .context(*context)
            .ok_or(ContextHostError::WrongContext)?;
        let mut host = address.lock().ok_or(ContextHostError::WrongContext)?;
        host.handoff_owner_fence(ticket)
    }
    pub(crate) fn host_turn(
        &self,
        turn: &ActorHostTurn<WorkerActor>,
    ) -> Option<ContextHostAddress> {
        let state = self.state();
        let context = state.actors.get(&turn.actor())?;
        state
            .contexts
            .get(context)
            .filter(|address| turn.matches_registration(&address.0.registration))
            .cloned()
    }
    pub(in crate::control_runtime) fn remove_original(&self, address: &ContextHostAddress) {
        let mut state = self.state();
        let context = address.context();
        assert!(
            state
                .contexts
                .get(&context)
                .is_some_and(|original| original.same_origin(address)),
            "only the same physically removed Context can leave its directory"
        );
        state.contexts.remove(&context);
        assert_eq!(
            state.executions.remove(&context.query_execution_id()),
            Some(context)
        );
        assert_eq!(
            state.actors.remove(&address.0.registration.actor()),
            Some(context)
        );
    }
}

impl ContextDirectory {
    /// Copy actual metadata addresses under the short directory lock only.
    /// A published cell's original owner is taken before its address is removed.
    pub(in crate::control_runtime) fn try_census_addresses(
        &self,
    ) -> Option<Vec<ContextHostAddress>> {
        let state = self.0.try_lock().ok()?;
        Some(state.contexts.values().cloned().collect())
    }
    /// No Context lock is held here. Same-ID replacement cannot reuse a cell
    /// while the original metadata address remains in the observed list.
    pub(in crate::control_runtime) fn census_addresses_unchanged(
        &self,
        original: &[ContextHostAddress],
    ) -> bool {
        let Ok(state) = self.0.try_lock() else {
            return false;
        };
        state.contexts.len() == original.len()
            && original.iter().all(|address| {
                state
                    .contexts
                    .get(&address.context())
                    .is_some_and(|current| current.same_origin(address))
            })
    }
}
