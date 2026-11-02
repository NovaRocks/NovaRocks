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

//! Bounded ownership of rich objects. Only a host takes or destroys payloads.

use crate::sync::{Arc, Mutex};
use novarocks_control_runtime_core::identity::{Generation, ScopeId, SlotId};
use std::{collections::BTreeMap, mem::ManuallyDrop};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ResourceError {
    Full,
    Duplicate,
    Missing,
    WrongGeneration,
}
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ResourceInventory {
    pub ids: Vec<SlotId>,
    pub next_after: Option<SlotId>,
    pub scanned: usize,
    pub complete: bool,
}
struct ResourceEntry<T> {
    scope: ScopeId,
    generation: Generation,
    value: ManuallyDrop<T>,
}
struct ResourceState<T> {
    entries: BTreeMap<SlotId, ResourceEntry<T>>,
    capacity: usize,
}
impl<T> Drop for ResourceState<T> {
    fn drop(&mut self) {
        // ManuallyDrop prevents arbitrary source destruction on a poller or during unwind.
        // Losing the last custody owner while objects remain is a runtime invariant failure.
        assert!(
            self.entries.is_empty(),
            "runtime resource owner dropped before its holders exited"
        );
    }
}
pub struct ResourceStore<T> {
    state: Arc<Mutex<ResourceState<T>>>,
}
impl<T> Clone for ResourceStore<T> {
    fn clone(&self) -> Self {
        Self {
            state: self.state.clone(),
        }
    }
}
#[must_use = "A host must consume the resource or retain this custody handle"]
pub struct ResourceHandle<T> {
    store: ResourceStore<T>,
    id: SlotId,
    scope: ScopeId,
    generation: Generation,
}
impl<T> ResourceStore<T> {
    pub fn new(capacity: usize) -> Self {
        Self {
            state: Arc::new(Mutex::new(ResourceState {
                entries: BTreeMap::new(),
                capacity,
            })),
        }
    }
    pub fn len(&self) -> usize {
        self.state
            .lock()
            .expect("runtime resource lock")
            .entries
            .len()
    }
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
    /// Only matching ownership metadata is returned. The cursor bounds each cleanup slice.
    pub fn inventory(
        &self,
        scope: ScopeId,
        generation: Generation,
        after: Option<SlotId>,
        budget: usize,
    ) -> ResourceInventory {
        let state = self.state.lock().expect("runtime resource lock");
        let lower = after.map_or(std::ops::Bound::Unbounded, std::ops::Bound::Excluded);
        let mut range = state.entries.range((lower, std::ops::Bound::Unbounded));
        let mut inventory = ResourceInventory {
            ids: Vec::new(),
            next_after: after,
            scanned: 0,
            complete: false,
        };
        for _ in 0..budget {
            let Some((id, entry)) = range.next() else {
                inventory.complete = true;
                break;
            };
            inventory.scanned += 1;
            inventory.next_after = Some(*id);
            if entry.scope == scope && entry.generation == generation {
                inventory.ids.push(*id);
            }
        }
        inventory
    }
    /// Recovery consumes the same actual custody entry, never a copied shadow owner.
    pub fn take_owned(
        &self,
        id: SlotId,
        scope: ScopeId,
        generation: Generation,
    ) -> Result<T, ResourceError> {
        let entry = {
            let mut state = self.state.lock().expect("runtime resource lock");
            let entry = state.entries.get(&id).ok_or(ResourceError::Missing)?;
            if entry.scope != scope || entry.generation != generation {
                return Err(ResourceError::WrongGeneration);
            }
            state.entries.remove(&id).expect("validated resource")
        };
        Ok(ManuallyDrop::into_inner(entry.value))
    }
    /// The caller already owns the corresponding scope/credit. Error returns its original payload.
    pub fn insert(
        &self,
        id: SlotId,
        scope: ScopeId,
        generation: Generation,
        value: T,
    ) -> Result<ResourceHandle<T>, (ResourceError, T)> {
        let mut state = self.state.lock().expect("runtime resource lock");
        if state.entries.contains_key(&id) {
            return Err((ResourceError::Duplicate, value));
        }
        if state.entries.len() >= state.capacity {
            return Err((ResourceError::Full, value));
        }
        state.entries.insert(
            id,
            ResourceEntry {
                scope,
                generation,
                value: ManuallyDrop::new(value),
            },
        );
        Ok(ResourceHandle {
            store: self.clone(),
            id,
            scope,
            generation,
        })
    }
}
impl<T> ResourceHandle<T> {
    pub const fn id(&self) -> SlotId {
        self.id
    }
    pub const fn scope(&self) -> ScopeId {
        self.scope
    }
    pub const fn generation(&self) -> Generation {
        self.generation
    }
    /// This runs on the host. Payload destruction cannot happen while the pool lock is held.
    pub fn take(self) -> Result<T, ResourceError> {
        self.store.take_owned(self.id, self.scope, self.generation)
    }
}
