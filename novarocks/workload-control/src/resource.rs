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

use crate::{
    WorkError, WorkId, WorkScope,
    scope::{Inner, State},
};
use std::sync::{Arc, Mutex};

/// Explicit local allocation budget. Server must resolve actual configuration;
/// no default claims to know the process's usable memory.
#[derive(Clone, Debug)]
pub struct ResourceConfig {
    pub total_bytes: u64,
    /// A hard partition within total_bytes, unavailable to data allocations.
    pub control_bytes: u64,
    /// Data admission ceiling for this scope's held bytes. Cleanup can use its
    /// control partition beyond this ceiling, within the same process total.
    pub per_scope_bytes: u64,
}

impl ResourceConfig {
    pub fn validate(&self) -> Result<(), WorkError> {
        if self.total_bytes == 0 || self.total_bytes > isize::MAX as u64 {
            return Err(WorkError::InvalidConfig("total_bytes"));
        }
        if self.control_bytes == 0 || self.control_bytes >= self.total_bytes {
            return Err(WorkError::InvalidConfig("control_bytes"));
        }
        if self.per_scope_bytes == 0 || self.per_scope_bytes > self.total_bytes {
            return Err(WorkError::InvalidConfig("per_scope_bytes"));
        }
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum ResourceClass {
    Data,
    Control,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ResourceSnapshot {
    pub total_limit_bytes: u64,
    pub data_reserved_bytes: u64,
    pub data_used_bytes: u64,
    pub control_reserved_bytes: u64,
    pub control_used_bytes: u64,
    pub peak_held_bytes: u64,
}

impl ResourceSnapshot {
    pub fn held_bytes(&self) -> u64 {
        self.data_reserved_bytes
            + self.data_used_bytes
            + self.control_reserved_bytes
            + self.control_used_bytes
    }
}

/// One process-local authority, shared with the responsibility owner. It cannot
/// consume a scope minted by another controller or reserve remote BE memory.
#[derive(Clone)]
pub struct LocalResourceAuthority {
    pub(crate) inner: Arc<Inner>,
}

/// Metadata admission only. The request's desired bytes are not a reservation.
/// The guard is owned by the waiting future and retains its scope until every
/// exit path (including dropping a pending future) removes the registration.
struct ResourceWaitRegistration {
    scope: WorkScope,
    class: ResourceClass,
}

impl ResourceWaitRegistration {
    fn register(scope: &WorkScope, class: ResourceClass) -> Result<Self, WorkError> {
        scope.inner.update_facts(|state| {
            let node = state.nodes.get(&scope.id).ok_or(WorkError::Released)?;
            if class == ResourceClass::Data {
                node.check()?;
            }
            let key = (scope.id, class);
            if state.resource_waiters.contains(&key) {
                return Err(WorkError::AlreadyWaitingForResource(class));
            }
            if state.waiting_records() >= scope.inner.config.waiting_limit {
                return Err(WorkError::Capacity("waiting entries"));
            }
            state.resource_waiters.insert(key);
            state.nodes.get_mut(&scope.id).unwrap().resource_waiters += 1;
            state.record_waiting_peak();
            Ok(Self {
                scope: scope.clone(),
                class,
            })
        })
    }
}

impl Drop for ResourceWaitRegistration {
    fn drop(&mut self) {
        self.scope.inner.update_facts(|state| {
            let removed = state.resource_waiters.remove(&(self.scope.id, self.class));
            debug_assert!(removed, "Resource waiter registration was already removed");
            state
                .nodes
                .get_mut(&self.scope.id)
                .unwrap()
                .resource_waiters -= 1;
            state.collect(self.scope.id);
        });
    }
}

fn check_capacity(
    state: &State,
    config: &ResourceConfig,
    id: WorkId,
    bytes: u64,
    class: ResourceClass,
) -> Result<(), WorkError> {
    let node = state.nodes.get(&id).ok_or(WorkError::Released)?;
    if class == ResourceClass::Data {
        node.check()?;
    }
    let scope_held = node
        .reserved_bytes
        .checked_add(node.used_bytes)
        .and_then(|held| held.checked_add(bytes))
        .ok_or(WorkError::ArithmeticOverflow)?;
    if class == ResourceClass::Data && scope_held > config.per_scope_bytes {
        return Err(WorkError::Capacity("scope allocation bytes"));
    }
    let (reserved, used, limit) = match class {
        ResourceClass::Data => (
            state.data_reserved,
            state.data_used,
            config.total_bytes - config.control_bytes,
        ),
        ResourceClass::Control => (
            state.control_reserved,
            state.control_used,
            config.control_bytes,
        ),
    };
    let held = reserved
        .checked_add(used)
        .and_then(|held| held.checked_add(bytes))
        .ok_or(WorkError::ArithmeticOverflow)?;
    if held > limit {
        return Err(WorkError::Capacity("local allocation bytes"));
    }
    Ok(())
}

fn reserve_bytes(state: &mut State, id: WorkId, bytes: u64, class: ResourceClass) {
    state.nodes.get_mut(&id).unwrap().reserved_bytes += bytes;
    match class {
        ResourceClass::Data => state.data_reserved += bytes,
        ResourceClass::Control => state.control_reserved += bytes,
    }
    state.peak_held_bytes = state
        .peak_held_bytes
        .max(state.data_reserved + state.data_used + state.control_reserved + state.control_used);
}

fn release_reserved(state: &mut State, id: WorkId, bytes: u64, class: ResourceClass) {
    state.nodes.get_mut(&id).unwrap().reserved_bytes -= bytes;
    match class {
        ResourceClass::Data => state.data_reserved -= bytes,
        ResourceClass::Control => state.control_reserved -= bytes,
    }
}

impl LocalResourceAuthority {
    pub fn reserve(
        &self,
        scope: &WorkScope,
        bytes: u64,
        class: ResourceClass,
    ) -> Result<Reservation, WorkError> {
        if !Arc::ptr_eq(&self.inner, &scope.inner) {
            return Err(WorkError::ForeignAuthority);
        }
        if bytes == 0 {
            return Err(WorkError::Capacity("zero-byte reservation"));
        }
        self.inner.update_facts(|state| {
            check_capacity(state, &self.inner.resource_config, scope.id, bytes, class)?;
            let node = state.nodes.get_mut(&scope.id).unwrap();
            node.resource_holders = node
                .resource_holders
                .checked_add(1)
                .ok_or(WorkError::ArithmeticOverflow)?;
            reserve_bytes(state, scope.id, bytes, class);
            Ok(Reservation {
                scope: scope.clone(),
                remaining: bytes,
                class,
            })
        })
    }

    pub fn snapshot(&self) -> ResourceSnapshot {
        let state = self.inner.state.lock().unwrap();
        ResourceSnapshot {
            total_limit_bytes: self.inner.resource_config.total_bytes,
            data_reserved_bytes: state.data_reserved,
            data_used_bytes: state.data_used,
            control_reserved_bytes: state.control_reserved,
            control_used_bytes: state.control_used,
            peak_held_bytes: state.peak_held_bytes,
        }
    }

    /// Wait for a capacity hint; callers retry reserve to acquire the capacity.
    /// On first poll, register one waiter per exact scope/class within the same
    /// global entry limit used by stage admission. A duplicate is rejected.
    /// Data waits inherit cancellation and the earlier parent deadline. Control
    /// waits keep their independent capacity timeout so expired work can clean up.
    pub async fn wait_for_capacity(
        &self,
        scope: &WorkScope,
        bytes: u64,
        class: ResourceClass,
    ) -> Result<(), WorkError> {
        if !Arc::ptr_eq(&self.inner, &scope.inner) {
            return Err(WorkError::ForeignAuthority);
        }
        let limit = match class {
            ResourceClass::Data => {
                self.inner.resource_config.total_bytes - self.inner.resource_config.control_bytes
            }
            ResourceClass::Control => self.inner.resource_config.control_bytes,
        };
        if bytes == 0
            || bytes > limit
            || (class == ResourceClass::Data && bytes > self.inner.resource_config.per_scope_bytes)
        {
            return Err(WorkError::Capacity("unrepresentable allocation"));
        }
        let _registration = ResourceWaitRegistration::register(scope, class)?;
        let cancellation = scope.cancellation()?;
        let wait_deadline = tokio::time::Instant::now()
            .checked_add(self.inner.config.capacity_wait_timeout)
            .ok_or(WorkError::ArithmeticOverflow)?;
        let wait_deadline = match class {
            ResourceClass::Data => cancellation
                .deadline()
                .map_or(wait_deadline, |parent| parent.min(wait_deadline)),
            ResourceClass::Control => wait_deadline,
        };
        let timeout = tokio::time::sleep_until(wait_deadline);
        tokio::pin!(timeout);
        loop {
            let changed = self.inner.changed.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            if class == ResourceClass::Data {
                scope.check()?;
            }
            if tokio::time::Instant::now() >= wait_deadline {
                return Err(WorkError::CapacityWaitTimeout);
            }
            match check_capacity(
                &self.inner.state.lock().unwrap(),
                &self.inner.resource_config,
                scope.id,
                bytes,
                class,
            ) {
                Ok(()) => return Ok(()),
                Err(WorkError::Capacity(_)) => {}
                Err(error) => return Err(error),
            }
            if class == ResourceClass::Control {
                tokio::select! { _ = changed => {}, _ = &mut timeout => {} }
            } else {
                tokio::select! {
                    _ = changed => {},
                    _ = &mut timeout => {},
                    reason = cancellation.cancelled() => return Err(WorkError::Cancelled(reason)),
                }
            }
        }
    }
}

/// Owns unused capacity. Dropping it cannot release bytes already converted
/// into allocation charges. Growth is checked before the allocation occurs.
pub struct Reservation {
    scope: WorkScope,
    remaining: u64,
    class: ResourceClass,
}

impl Reservation {
    pub fn remaining_bytes(&self) -> u64 {
        self.remaining
    }

    pub fn grow(&mut self, additional: u64) -> Result<(), WorkError> {
        self.scope.inner.update_facts(|state| {
            check_capacity(
                state,
                &self.scope.inner.resource_config,
                self.scope.id,
                additional,
                self.class,
            )?;
            if self.remaining == 0 && additional != 0 {
                let node = state.nodes.get_mut(&self.scope.id).unwrap();
                node.resource_holders = node
                    .resource_holders
                    .checked_add(1)
                    .ok_or(WorkError::ArithmeticOverflow)?;
            }
            self.remaining = self
                .remaining
                .checked_add(additional)
                .ok_or(WorkError::ArithmeticOverflow)?;
            reserve_bytes(state, self.scope.id, additional, self.class);
            Ok(())
        })
    }

    /// Attach the returned charge to the backing allocation, before retaining
    /// its bytes. Slices must clone this same charge, not create smaller charges.
    pub fn charge(&mut self, bytes: u64) -> Result<AllocationCharge, WorkError> {
        if bytes == 0 || bytes > self.remaining {
            return Err(WorkError::Capacity("unused reservation"));
        }
        self.scope.inner.update_facts(|state| {
            let node = state.nodes.get_mut(&self.scope.id).unwrap();
            if self.class == ResourceClass::Data {
                node.check()?;
            }
            if bytes < self.remaining {
                node.resource_holders = node
                    .resource_holders
                    .checked_add(1)
                    .ok_or(WorkError::ArithmeticOverflow)?;
            }
            node.reserved_bytes -= bytes;
            node.used_bytes += bytes;
            match self.class {
                ResourceClass::Data => {
                    state.data_reserved -= bytes;
                    state.data_used += bytes;
                }
                ResourceClass::Control => {
                    state.control_reserved -= bytes;
                    state.control_used += bytes;
                }
            }
            self.remaining -= bytes;
            Ok(AllocationCharge {
                allocation: Arc::new(Allocation {
                    inner: Arc::clone(&self.scope.inner),
                    owner: Mutex::new(self.scope.id),
                    bytes,
                    class: self.class,
                }),
            })
        })
    }

    pub fn release_unused(&mut self, bytes: u64) -> Result<(), WorkError> {
        if bytes > self.remaining {
            return Err(WorkError::Capacity("unused reservation"));
        }
        if bytes == 0 {
            return Ok(());
        }
        self.scope.inner.update_facts(|state| {
            release_reserved(state, self.scope.id, bytes, self.class);
            if bytes == self.remaining {
                state
                    .nodes
                    .get_mut(&self.scope.id)
                    .unwrap()
                    .resource_holders -= 1;
                state.collect(self.scope.id);
            }
        });
        self.remaining -= bytes;
        Ok(())
    }
}

impl Drop for Reservation {
    fn drop(&mut self) {
        if self.remaining == 0 {
            return;
        }
        self.scope.inner.update_facts(|state| {
            release_reserved(state, self.scope.id, self.remaining, self.class);
            state
                .nodes
                .get_mut(&self.scope.id)
                .unwrap()
                .resource_holders -= 1;
            state.collect(self.scope.id);
        });
    }
}

struct Allocation {
    inner: Arc<Inner>,
    owner: Mutex<WorkId>,
    bytes: u64,
    class: ResourceClass,
}

impl Drop for Allocation {
    fn drop(&mut self) {
        let id = *self.owner.get_mut().unwrap();
        self.inner.update_facts(|state| {
            let node = state.nodes.get_mut(&id).unwrap();
            node.used_bytes -= self.bytes;
            node.resource_holders -= 1;
            match self.class {
                ResourceClass::Data => state.data_used -= self.bytes,
                ResourceClass::Control => state.control_used -= self.bytes,
            }
            state.collect(id);
        });
    }
}

/// Shared backing-allocation ownership. Only the last clone's destruction
/// supplies a release fact. Cloning cannot double-charge bytes.
#[derive(Clone)]
pub struct AllocationCharge {
    allocation: Arc<Allocation>,
}

impl AllocationCharge {
    pub fn bytes(&self) -> u64 {
        self.allocation.bytes
    }

    /// Transfer all shared aliases together within the same local authority.
    /// No temporary global uncharge occurs; the recipient's scope limit applies.
    pub fn transfer_to(&self, scope: &WorkScope) -> Result<(), WorkError> {
        if !Arc::ptr_eq(&self.allocation.inner, &scope.inner) {
            return Err(WorkError::ForeignAuthority);
        }
        let mut owner = self.allocation.owner.lock().unwrap();
        if *owner == scope.id {
            return if self.allocation.class == ResourceClass::Data {
                scope.check()
            } else {
                Ok(())
            };
        }
        self.allocation.inner.update_facts(|state| {
            let target = state.nodes.get_mut(&scope.id).ok_or(WorkError::Released)?;
            if self.allocation.class == ResourceClass::Data {
                target.check()?;
            }
            let held = target
                .used_bytes
                .checked_add(target.reserved_bytes)
                .and_then(|held| held.checked_add(self.allocation.bytes))
                .ok_or(WorkError::ArithmeticOverflow)?;
            if self.allocation.class == ResourceClass::Data
                && held > self.allocation.inner.resource_config.per_scope_bytes
            {
                return Err(WorkError::Capacity("recipient allocation bytes"));
            }
            target.resource_holders = target
                .resource_holders
                .checked_add(1)
                .ok_or(WorkError::ArithmeticOverflow)?;
            target.used_bytes += self.allocation.bytes;
            let source = state.nodes.get_mut(&owner).unwrap();
            source.resource_holders -= 1;
            source.used_bytes -= self.allocation.bytes;
            state.collect(*owner);
            *owner = scope.id;
            Ok(())
        })
    }
}
