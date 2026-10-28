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

//! Bounded, coalesced addresses of original Context host owners. Readiness
//! grants no Work, storage or exit authority and stores no protocol decision.

use super::actors::WorkerActor;
use novarocks_control_runtime::{
    driver::ActorRuntime,
    transaction::{RuntimeError, ScopeHandle},
};
use novarocks_control_runtime_core::identity::ScopeId;
use novarocks_execution_contract::QueryContextRef;
use std::{
    collections::BTreeMap,
    num::NonZeroUsize,
    sync::{Arc, Mutex, Weak},
};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ContextReadyError {
    Full,
    Duplicate,
    WrongRuntime,
    OwnerLost,
    Runtime(RuntimeError),
}
struct Registered {
    original: Weak<Endpoint>,
    queued: Option<u64>,
}
struct State {
    slots: BTreeMap<ScopeId, Registered>,
    ready: BTreeMap<u64, ScopeId>,
    next_ready: u64,
}
struct Shared {
    runtime: ActorRuntime<WorkerActor>,
    limit: NonZeroUsize,
    state: Mutex<State>,
}
#[derive(Clone)]
pub(crate) struct ContextReady {
    shared: Arc<Shared>,
}
struct Endpoint {
    index: ContextReady,
    context: QueryContextRef,
    scope: ScopeHandle,
}
/// Clones name one registered original owner. An old handle cannot mark a
/// replacement with matching public query/actor IDs or consume another slot.
#[derive(Clone)]
pub(crate) struct ContextReadyNotice(Arc<Endpoint>);
impl ContextReady {
    pub(super) fn same_runtime(&self, runtime: &ActorRuntime<WorkerActor>) -> bool {
        self.shared.runtime.same_runtime(runtime)
    }
    pub(crate) fn new(runtime: ActorRuntime<WorkerActor>, limit: NonZeroUsize) -> Self {
        Self {
            shared: Arc::new(Shared {
                runtime,
                limit,
                state: Mutex::new(State {
                    slots: BTreeMap::new(),
                    ready: BTreeMap::new(),
                    next_ready: 0,
                }),
            }),
        }
    }
    pub(super) fn register(
        &self,
        context: QueryContextRef,
        scope: ScopeHandle,
    ) -> Result<ContextReadyNotice, ContextReadyError> {
        scope.gates().map_err(ContextReadyError::Runtime)?;
        let endpoint = Arc::new(Endpoint {
            index: self.clone(),
            context,
            scope,
        });
        let mut state = self.shared.state.lock().expect("Context host ready index");
        if state.slots.contains_key(&endpoint.scope.id()) {
            return Err(ContextReadyError::Duplicate);
        }
        if state.slots.len() >= self.shared.limit.get() {
            return Err(ContextReadyError::Full);
        }
        state.slots.insert(
            endpoint.scope.id(),
            Registered {
                original: Arc::downgrade(&endpoint),
                queued: None,
            },
        );
        Ok(ContextReadyNotice(endpoint))
    }
    /// One bounded address per call, in actual ready-enqueue FIFO order.
    /// The driver samples its actual runtime version before draining this
    /// index, then uses the runtime's versioned wait; no request scans owners.
    pub(crate) fn next_ready(&self) -> Result<Option<ContextReadyNotice>, ContextReadyError> {
        let mut state = self.shared.state.lock().expect("Context host ready index");
        let Some((&ordinal, &key)) = state.ready.first_key_value() else {
            return Ok(None);
        };
        let registered = state.slots.get(&key).ok_or(ContextReadyError::OwnerLost)?;
        assert_eq!(
            registered.queued,
            Some(ordinal),
            "one original queued Context mark"
        );
        // A lost live owner remains queued and consumes its bounded slot. Do
        // not turn an Unknown owner into implicit removal or an empty queue.
        let endpoint = registered
            .original
            .upgrade()
            .ok_or(ContextReadyError::OwnerLost)?;
        state.ready.remove(&ordinal);
        state
            .slots
            .get_mut(&key)
            .expect("original registered Context")
            .queued = None;
        drop(state);
        Ok(Some(ContextReadyNotice(endpoint)))
    }
}
impl Drop for Endpoint {
    fn drop(&mut self) {
        // Metadata-only bootstrap owners can finish without ever issuing M.
        // Removing an address after their actual Scope removal grants no exit
        // or refund. A lost live owner keeps its slot, rather than disguising
        // unresolved original Work as an unused position.
        if !matches!(self.scope.gates(), Err(RuntimeError::UnknownScope)) {
            return;
        }
        let mut state = self
            .index
            .shared
            .state
            .lock()
            .expect("Context host ready index");
        if state
            .slots
            .get(&self.scope.id())
            .is_some_and(|entry| std::ptr::eq(entry.original.as_ptr(), self))
        {
            let entry = state
                .slots
                .remove(&self.scope.id())
                .expect("original removed Context");
            if let Some(ordinal) = entry.queued {
                state.ready.remove(&ordinal);
            }
        }
    }
}
impl ContextReadyNotice {
    pub(crate) fn same_origin(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.0, &other.0)
    }
    pub(crate) fn context(&self) -> QueryContextRef {
        self.0.context
    }
    pub(crate) fn scope(&self) -> ScopeId {
        self.0.scope.id()
    }
    pub(super) fn notify(&self) {
        let shared = &self.0.index.shared;
        let mut state = shared.state.lock().expect("Context host ready index");
        let Some(registered) = state.slots.get(&self.scope()) else {
            return;
        };
        if !Weak::ptr_eq(&registered.original, &Arc::downgrade(&self.0)) {
            return;
        }
        if registered.queued.is_none() {
            // This monotonic value orders bounded readiness only. It grants
            // no business identity, Work, resource access or retirement proof.
            let ordinal = state
                .next_ready
                .checked_add(1)
                .expect("Context ready ordering exhausted");
            state.next_ready = ordinal;
            state
                .slots
                .get_mut(&self.scope())
                .expect("original registered Context")
                .queued = Some(ordinal);
            state.ready.insert(ordinal, self.scope());
        }
        drop(state);
        shared.runtime.notify_pollers();
    }
    /// Physical child-scope removal precedes Root HostRemoved publication.
    /// At that point M/R/H and their reports are gone, so old aliases can no
    /// longer signal useful host work. Root's remaining paid inputs wake Root
    /// itself and retain the original admission independently of this index.
    pub(super) fn unregister_removed(&self) -> Result<(), ContextReadyError> {
        match self.0.scope.gates() {
            Err(RuntimeError::UnknownScope) => {}
            Ok(_) => return Err(ContextReadyError::Runtime(RuntimeError::ScopeNotRetired)),
            Err(error) => return Err(ContextReadyError::Runtime(error)),
        }
        let shared = &self.0.index.shared;
        let mut state = shared.state.lock().expect("Context host ready index");
        let Some(entry) = state.slots.get(&self.scope()) else {
            return Ok(());
        };
        if !Weak::ptr_eq(&entry.original, &Arc::downgrade(&self.0)) {
            return Err(ContextReadyError::Duplicate);
        }
        let entry = state
            .slots
            .remove(&self.scope())
            .expect("original removed Context");
        if let Some(ordinal) = entry.queued {
            state.ready.remove(&ordinal);
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests;
