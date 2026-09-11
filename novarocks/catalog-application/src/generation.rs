// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt::{Display, Formatter};
use std::sync::{Arc, Mutex, Weak};

use novarocks_spi::connector::{CatalogHandle, ConnectorControlRuntimeId, ConnectorInstanceId};

/// A fully constructed provider runtime that has not entered Catalog routing.
/// Dropping it releases every provider resource without changing current state.
pub struct PreparedCatalogGeneration<T> {
    runtime_id: ConnectorControlRuntimeId,
    name: ConnectorInstanceId,
    handle: Option<CatalogHandle>,
    runtime: Arc<T>,
}

impl<T> PreparedCatalogGeneration<T> {
    pub fn new(runtime_id: ConnectorControlRuntimeId, handle: CatalogHandle, runtime: T) -> Self {
        let name = handle.catalog_name().clone();
        Self {
            runtime_id,
            name,
            handle: Some(handle),
            runtime: Arc::new(runtime),
        }
    }

    pub fn without_execution_handle(
        runtime_id: ConnectorControlRuntimeId,
        name: ConnectorInstanceId,
        runtime: T,
    ) -> Self {
        Self {
            runtime_id,
            name,
            handle: None,
            runtime: Arc::new(runtime),
        }
    }

    pub fn from_shared(
        runtime_id: ConnectorControlRuntimeId,
        handle: CatalogHandle,
        runtime: Arc<T>,
    ) -> Self {
        let name = handle.catalog_name().clone();
        Self {
            runtime_id,
            name,
            handle: Some(handle),
            runtime,
        }
    }

    pub const fn runtime_id(&self) -> ConnectorControlRuntimeId {
        self.runtime_id
    }

    pub const fn handle(&self) -> Option<&CatalogHandle> {
        self.handle.as_ref()
    }
}

/// FE owner for current and retiring Catalog generations.
///
/// A current-name lookup is allowed only at initial admission. Once a query
/// has a [`CatalogHandle`] and runtime identity, it must use
/// [`Self::acquire_exact`]; an old identity is never rebound to the current
/// generation with the same name.
pub struct CatalogGenerationOwner<T> {
    inner: Arc<OwnerInner<T>>,
}

type RetirementObserver<T> =
    dyn Fn(ConnectorControlRuntimeId, Option<&CatalogHandle>, &T) + Send + Sync + 'static;

impl<T> Clone for CatalogGenerationOwner<T> {
    fn clone(&self) -> Self {
        Self {
            inner: Arc::clone(&self.inner),
        }
    }
}

struct OwnerInner<T> {
    state: Mutex<OwnerState<T>>,
    retirement_observer: Option<Arc<RetirementObserver<T>>>,
}

struct OwnerState<T> {
    current: BTreeMap<ConnectorInstanceId, ConnectorControlRuntimeId>,
    by_handle: BTreeMap<CatalogHandle, BTreeSet<ConnectorControlRuntimeId>>,
    generations: BTreeMap<ConnectorControlRuntimeId, Generation<T>>,
    retired_runtime_ids: BTreeSet<ConnectorControlRuntimeId>,
}

struct Generation<T> {
    name: ConnectorInstanceId,
    handle: Option<CatalogHandle>,
    runtime: Arc<T>,
    state: GenerationState,
    leases: usize,
}

#[derive(Clone, Copy, Eq, PartialEq)]
enum GenerationState {
    Current,
    Retiring,
}

impl<T> CatalogGenerationOwner<T> {
    pub fn new() -> Self {
        Self::with_optional_retirement_observer(None)
    }

    pub fn with_retirement_observer<F>(observer: F) -> Self
    where
        F: Fn(ConnectorControlRuntimeId, Option<&CatalogHandle>, &T) + Send + Sync + 'static,
    {
        Self::with_optional_retirement_observer(Some(Arc::new(observer)))
    }

    fn with_optional_retirement_observer(
        retirement_observer: Option<Arc<RetirementObserver<T>>>,
    ) -> Self {
        Self {
            inner: Arc::new(OwnerInner {
                state: Mutex::new(OwnerState {
                    current: BTreeMap::new(),
                    by_handle: BTreeMap::new(),
                    generations: BTreeMap::new(),
                    retired_runtime_ids: BTreeSet::new(),
                }),
                retirement_observer,
            }),
        }
    }

    /// Publish one already-constructed generation atomically. Any previous
    /// current generation enters retirement and remains reachable by its exact
    /// handle while an admitted lease protects it.
    pub fn publish(
        &self,
        prepared: PreparedCatalogGeneration<T>,
    ) -> Result<ConnectorControlRuntimeId, CatalogGenerationError> {
        let mut state = self.lock()?;
        let name = prepared.name.clone();
        if state.generations.contains_key(&prepared.runtime_id)
            || state.retired_runtime_ids.contains(&prepared.runtime_id)
        {
            return Err(CatalogGenerationError::DuplicateRuntime);
        }

        let previous_id = state.current.get(&name).copied();
        if let Some(previous_id) = previous_id
            && !state.generations.contains_key(&previous_id)
        {
            return Err(CatalogGenerationError::CorruptOwner);
        }

        let runtime_id = prepared.runtime_id;
        if let Some(handle) = &prepared.handle {
            state
                .by_handle
                .entry(handle.clone())
                .or_default()
                .insert(runtime_id);
        }
        state.generations.insert(
            runtime_id,
            Generation {
                name: prepared.name,
                handle: prepared.handle,
                runtime: prepared.runtime,
                state: GenerationState::Current,
                leases: 0,
            },
        );
        state.current.insert(name, runtime_id);
        if let Some(previous_id) = previous_id {
            state
                .generations
                .get_mut(&previous_id)
                .expect("previous generation was validated")
                .state = GenerationState::Retiring;
            let retired = reap_if_unleased(&mut state, previous_id);
            drop(state);
            self.notify_retirement(retired);
        }
        Ok(runtime_id)
    }

    /// Resolve the current generation during initial Catalog admission.
    pub fn acquire_current(
        &self,
        name: &ConnectorInstanceId,
    ) -> Result<CatalogGenerationLease<T>, CatalogGenerationError> {
        let mut state = self.lock()?;
        let runtime_id = state
            .current
            .get(name)
            .copied()
            .ok_or(CatalogGenerationError::NotFound)?;
        acquire(&self.inner, &mut state, runtime_id)
    }

    /// Resolve only the generation identified by both its immutable execution
    /// handle and process-local runtime identity. A content-identical Catalog
    /// replacement may intentionally share a handle with an older runtime.
    pub fn acquire_exact(
        &self,
        handle: &CatalogHandle,
        runtime_id: ConnectorControlRuntimeId,
    ) -> Result<CatalogGenerationLease<T>, CatalogGenerationError> {
        let mut state = self.lock()?;
        let matches = state
            .by_handle
            .get(handle)
            .is_some_and(|runtime_ids| runtime_ids.contains(&runtime_id));
        if !matches {
            return Err(CatalogGenerationError::NotFound);
        }
        acquire(&self.inner, &mut state, runtime_id)
    }

    /// Resolve an already-frozen process-local runtime identity. Legacy
    /// control operations use this during their migration to CatalogHandle;
    /// it never falls through to the current generation.
    pub fn acquire_runtime(
        &self,
        runtime_id: ConnectorControlRuntimeId,
    ) -> Result<CatalogGenerationLease<T>, CatalogGenerationError> {
        let mut state = self.lock()?;
        if !state.generations.contains_key(&runtime_id) {
            return Err(CatalogGenerationError::NotFound);
        }
        acquire(&self.inner, &mut state, runtime_id)
    }

    /// Remove current-name admission and begin retirement. An already-admitted
    /// exact lease remains valid until its owner drops it.
    pub fn retire(&self, name: &ConnectorInstanceId) -> Result<(), CatalogGenerationError> {
        let mut state = self.lock()?;
        let runtime_id = state
            .current
            .get(name)
            .copied()
            .ok_or(CatalogGenerationError::NotFound)?;
        if !state.generations.contains_key(&runtime_id) {
            return Err(CatalogGenerationError::CorruptOwner);
        }
        state.current.remove(name);
        let generation = state
            .generations
            .get_mut(&runtime_id)
            .expect("current generation was validated");
        generation.state = GenerationState::Retiring;
        let retired = reap_if_unleased(&mut state, runtime_id);
        drop(state);
        self.notify_retirement(retired);
        Ok(())
    }

    pub fn retained_generation_count(&self) -> Result<usize, CatalogGenerationError> {
        Ok(self.lock()?.generations.len())
    }

    pub fn retained_handles(&self) -> Result<Vec<CatalogHandle>, CatalogGenerationError> {
        Ok(self.lock()?.by_handle.keys().cloned().collect())
    }

    fn lock(&self) -> Result<std::sync::MutexGuard<'_, OwnerState<T>>, CatalogGenerationError> {
        self.inner
            .state
            .lock()
            .map_err(|_| CatalogGenerationError::OwnerUnavailable)
    }

    fn notify_retirement(&self, retired: Option<RetiredGeneration<T>>) {
        if let (Some(observer), Some(retired)) = (&self.inner.retirement_observer, retired) {
            observer(
                retired.runtime_id,
                retired.handle.as_ref(),
                &retired.runtime,
            );
        }
    }
}

impl<T> Default for CatalogGenerationOwner<T> {
    fn default() -> Self {
        Self::new()
    }
}

fn acquire<T>(
    owner: &Arc<OwnerInner<T>>,
    state: &mut OwnerState<T>,
    runtime_id: ConnectorControlRuntimeId,
) -> Result<CatalogGenerationLease<T>, CatalogGenerationError> {
    let generation = state
        .generations
        .get_mut(&runtime_id)
        .ok_or(CatalogGenerationError::CorruptOwner)?;
    generation.leases = generation
        .leases
        .checked_add(1)
        .ok_or(CatalogGenerationError::LeaseOverflow)?;
    Ok(CatalogGenerationLease {
        owner: Arc::downgrade(owner),
        runtime_id,
        name: generation.name.clone(),
        handle: generation.handle.clone(),
        runtime: Arc::clone(&generation.runtime),
    })
}

struct RetiredGeneration<T> {
    runtime_id: ConnectorControlRuntimeId,
    handle: Option<CatalogHandle>,
    runtime: Arc<T>,
}

fn reap_if_unleased<T>(
    state: &mut OwnerState<T>,
    runtime_id: ConnectorControlRuntimeId,
) -> Option<RetiredGeneration<T>> {
    let remove = state
        .generations
        .get(&runtime_id)
        .is_some_and(|generation| {
            generation.state == GenerationState::Retiring && generation.leases == 0
        });
    if remove && let Some(generation) = state.generations.remove(&runtime_id) {
        if let Some(handle) = &generation.handle
            && let Some(runtime_ids) = state.by_handle.get_mut(handle)
        {
            runtime_ids.remove(&runtime_id);
            if runtime_ids.is_empty() {
                state.by_handle.remove(handle);
            }
        }
        state.retired_runtime_ids.insert(runtime_id);
        return Some(RetiredGeneration {
            runtime_id,
            handle: generation.handle,
            runtime: generation.runtime,
        });
    }
    None
}

/// Move-only proof that one exact Catalog generation remains available.
pub struct CatalogGenerationLease<T> {
    owner: Weak<OwnerInner<T>>,
    runtime_id: ConnectorControlRuntimeId,
    name: ConnectorInstanceId,
    handle: Option<CatalogHandle>,
    runtime: Arc<T>,
}

impl<T> CatalogGenerationLease<T> {
    pub const fn runtime_id(&self) -> ConnectorControlRuntimeId {
        self.runtime_id
    }

    pub const fn name(&self) -> &ConnectorInstanceId {
        &self.name
    }

    pub const fn handle(&self) -> Option<&CatalogHandle> {
        self.handle.as_ref()
    }

    pub fn runtime(&self) -> &T {
        &self.runtime
    }
}

impl<T> Drop for CatalogGenerationLease<T> {
    fn drop(&mut self) {
        let Some(owner) = self.owner.upgrade() else {
            return;
        };
        let Ok(mut state) = owner.state.lock() else {
            return;
        };
        let Some(generation) = state.generations.get_mut(&self.runtime_id) else {
            return;
        };
        debug_assert!(generation.leases > 0, "Catalog lease count underflow");
        generation.leases = generation.leases.saturating_sub(1);
        let retired = reap_if_unleased(&mut state, self.runtime_id);
        drop(state);
        if let (Some(observer), Some(retired)) = (&owner.retirement_observer, retired) {
            observer(
                retired.runtime_id,
                retired.handle.as_ref(),
                &retired.runtime,
            );
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CatalogGenerationError {
    NotFound,
    DuplicateRuntime,
    LeaseOverflow,
    CorruptOwner,
    OwnerUnavailable,
}

impl Display for CatalogGenerationError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(match self {
            Self::NotFound => "Catalog generation is not available",
            Self::DuplicateRuntime => "Catalog runtime identity was already published",
            Self::LeaseOverflow => "Catalog generation lease limit is exhausted",
            Self::CorruptOwner => "Catalog generation owner is inconsistent",
            Self::OwnerUnavailable => "Catalog generation owner is unavailable",
        })
    }
}

impl std::error::Error for CatalogGenerationError {}

#[cfg(test)]
mod tests {
    use super::*;
    use novarocks_spi::connector::CatalogVersion;

    fn name(value: &str) -> ConnectorInstanceId {
        ConnectorInstanceId::try_from_canonical(value).expect("catalog name")
    }

    fn handle(name: &ConnectorInstanceId, marker: u8) -> CatalogHandle {
        CatalogHandle::new(name.clone(), CatalogVersion::from_bytes([marker; 32]))
    }

    fn prepared<T>(handle: CatalogHandle, runtime: T) -> PreparedCatalogGeneration<T> {
        PreparedCatalogGeneration::new(ConnectorControlRuntimeId::new(), handle, runtime)
    }

    #[test]
    fn replacement_keeps_only_the_exact_leased_generation() {
        let catalog = name("warehouse");
        let first_handle = handle(&catalog, 1);
        let second_handle = handle(&catalog, 2);
        let owner = CatalogGenerationOwner::new();
        let first_runtime = owner
            .publish(prepared(first_handle.clone(), "G1"))
            .expect("publish G1");
        let first = owner.acquire_current(&catalog).expect("acquire G1");
        let second_runtime = owner
            .publish(prepared(second_handle.clone(), "G2"))
            .expect("publish G2");

        assert_eq!(owner.acquire_current(&catalog).unwrap().runtime(), &"G2");
        assert_eq!(
            owner
                .acquire_exact(&first_handle, first_runtime)
                .unwrap()
                .runtime(),
            &"G1"
        );
        assert_eq!(
            owner.acquire_runtime(first_runtime).unwrap().runtime(),
            &"G1"
        );
        assert_eq!(owner.retained_generation_count().unwrap(), 2);

        drop(first);
        assert_eq!(owner.retained_generation_count().unwrap(), 1);
        assert!(matches!(
            owner.acquire_exact(&first_handle, first_runtime),
            Err(CatalogGenerationError::NotFound)
        ));
        assert_eq!(
            owner
                .acquire_exact(&second_handle, second_runtime)
                .unwrap()
                .runtime(),
            &"G2"
        );
    }

    #[test]
    fn same_name_rebuild_never_satisfies_an_old_exact_handle() {
        let catalog = name("warehouse");
        let old = handle(&catalog, 3);
        let rebuilt = handle(&catalog, 4);
        let owner = CatalogGenerationOwner::new();
        let old_runtime = owner
            .publish(prepared(old.clone(), 30))
            .expect("publish old object");
        owner
            .publish(prepared(rebuilt, 40))
            .expect("publish rebuilt object");

        assert!(matches!(
            owner.acquire_exact(&old, old_runtime),
            Err(CatalogGenerationError::NotFound)
        ));
        assert_eq!(owner.acquire_current(&catalog).unwrap().runtime(), &40);
    }

    #[test]
    fn retire_closes_new_admission_but_preserves_an_inflight_lease() {
        let catalog = name("warehouse");
        let exact = handle(&catalog, 5);
        let owner = CatalogGenerationOwner::new();
        let runtime_id = owner
            .publish(prepared(exact.clone(), 50))
            .expect("publish generation");
        let inflight = owner
            .acquire_exact(&exact, runtime_id)
            .expect("inflight lease");

        owner.retire(&catalog).expect("retire generation");
        assert!(matches!(
            owner.acquire_current(&catalog),
            Err(CatalogGenerationError::NotFound)
        ));
        assert_eq!(inflight.runtime(), &50);
        assert_eq!(
            owner.acquire_exact(&exact, runtime_id).unwrap().runtime(),
            &50
        );

        drop(inflight);
        assert_eq!(owner.retained_generation_count().unwrap(), 0);
    }

    #[test]
    fn a_content_identical_handle_can_name_two_exact_runtime_generations() {
        let catalog = name("warehouse");
        let shared = handle(&catalog, 6);
        let owner = CatalogGenerationOwner::new();
        let first_runtime = owner
            .publish(prepared(shared.clone(), 60))
            .expect("publish first generation");
        let retained = owner
            .acquire_exact(&shared, first_runtime)
            .expect("retain first generation");
        let second_runtime = owner
            .publish(prepared(shared.clone(), 70))
            .expect("publish second generation");

        assert_eq!(retained.runtime(), &60);
        assert_eq!(
            owner
                .acquire_exact(&shared, second_runtime)
                .unwrap()
                .runtime(),
            &70
        );
    }

    #[test]
    fn a_retired_runtime_identity_cannot_be_published_again() {
        let catalog = name("warehouse");
        let runtime_id = ConnectorControlRuntimeId::new();
        let first = handle(&catalog, 8);
        let replacement = handle(&catalog, 9);
        let owner = CatalogGenerationOwner::new();
        owner
            .publish(PreparedCatalogGeneration::new(runtime_id, first, 80))
            .expect("publish first runtime");
        owner.retire(&catalog).expect("retire first runtime");

        assert_eq!(
            owner.publish(PreparedCatalogGeneration::new(runtime_id, replacement, 90,)),
            Err(CatalogGenerationError::DuplicateRuntime)
        );
    }

    #[test]
    fn retirement_observer_runs_once_after_the_last_exact_lease() {
        let catalog = name("warehouse");
        let exact = handle(&catalog, 10);
        let events = Arc::new(Mutex::new(Vec::new()));
        let observed = Arc::clone(&events);
        let owner = CatalogGenerationOwner::with_retirement_observer(
            move |runtime_id, handle, runtime: &&str| {
                observed.lock().expect("events").push((
                    runtime_id,
                    handle.expect("execution handle").clone(),
                    *runtime,
                ));
            },
        );
        let runtime_id = owner
            .publish(prepared(exact.clone(), "G1"))
            .expect("publish generation");
        let lease = owner
            .acquire_exact(&exact, runtime_id)
            .expect("exact lease");

        owner.retire(&catalog).expect("retire generation");
        assert!(events.lock().expect("events").is_empty());

        drop(lease);
        assert_eq!(
            events.lock().expect("events").as_slice(),
            &[(runtime_id, exact, "G1")]
        );
    }
}
