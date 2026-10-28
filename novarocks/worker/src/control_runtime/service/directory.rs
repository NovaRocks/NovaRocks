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

use super::{ContextHostRecord, WorkerActor};
use novarocks_control_runtime::driver::{ActorHostTurn, ActorRegistration};
use novarocks_control_runtime_core::identity::ActorId;
use novarocks_execution_contract::QueryContextRef;
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
    pub(super) fn take_original(mut self) -> ContextHostRecord {
        self.0.take().expect("same original Context removal owner")
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
    pub(crate) fn lock(&self) -> Option<ContextHostGuard<'_>> {
        let host = self.0.host.lock().expect("original Context owner lock");
        host.is_some().then_some(ContextHostGuard(host))
    }
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
}
/// Clone this handle before a host turn; lookup releases the short directory
/// lock before any caller locks the addressed Context's original inventory.
#[derive(Clone, Default)]
pub(crate) struct ContextDirectory(Arc<Mutex<DirectoryState>>);
impl ContextDirectory {
    pub(super) fn state(&self) -> MutexGuard<'_, DirectoryState> {
        self.0.lock().expect("Context address directory lock")
    }
    pub(crate) fn context(&self, context: QueryContextRef) -> Option<ContextHostAddress> {
        self.state().contexts.get(&context).cloned()
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
    pub(super) fn remove_original(&self, address: &ContextHostAddress) {
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
            state.actors.remove(&address.0.registration.actor()),
            Some(context)
        );
    }
}
