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

//! Release notifications share the original admission transaction. Registrants
//! are non-cloneable owners, with at most one logical registration each; Drop
//! unregisters every domain of that registration. A wake only requests a retry
//! and never grants capacity. Each registration merges process dimensions and
//! keyed family/dimension domains. Its first matching real release consumes
//! all domains, so a later refused target cannot hide an earlier target's wake.
//!
//! A keyed domain deliberately ignores the particular key: another key's release
//! can produce one spurious wake. The recipient must repeat the original ledger
//! transaction. The previous transport supervisor woke on any global release.
//! No arbitrary key set is retained: secondary indexes are bounded by live
//! registrants times the already configured process/family dimension count,
//! even when permanently oversized requests enumerate arbitrarily many empty keys.
//!
//! Actor turns defer wakes until the poller's lock-free boundary. No mutex is
//! replaced, and the existing inbox -> ledger lock order remains unchanged.
//! Custody retirement uses the same private deferral boundary and flushes its
//! original ledger only after releasing custody locks. Other external releases
//! deliver synchronously after unlocking.

use crate::{
    notify::Notify,
    sync::{Arc, AtomicBool, Mutex, Ordering},
    transaction::{CapacityDenial, LedgerState, RuntimeError, RuntimeLedger},
};
use novarocks_control_runtime_core::{
    capacity::{CapacityWaitTarget, ReservationResources},
    history::HistoryFact,
    identity::CapacityRegistrantId,
};
use std::{
    cell::Cell,
    collections::{BTreeMap, BTreeSet},
    task::Waker,
};

#[cfg(not(loom))]
std::thread_local! { static DEFER_CAPACITY_WAKES: Cell<usize> = const { Cell::new(0) }; }
#[cfg(loom)]
loom::thread_local! { static DEFER_CAPACITY_WAKES: Cell<usize> = const { Cell::new(0) }; }

/// Marks only the finite actor-turn boundary. It changes no actual lock, lock
/// order, resource, or lifetime gate. The wrapper flushes after this guard exits.
pub(crate) struct TurnWakeDeferral;
impl TurnWakeDeferral {
    pub(crate) fn enter() -> Self {
        DEFER_CAPACITY_WAKES.with(|depth| {
            depth.set(
                depth
                    .get()
                    .checked_add(1)
                    .expect("nested actor turns fit usize"),
            )
        });
        Self
    }
}
impl Drop for TurnWakeDeferral {
    fn drop(&mut self) {
        DEFER_CAPACITY_WAKES
            .with(|depth| depth.set(depth.get().checked_sub(1).expect("entered actor turn")));
    }
}
fn deferred() -> bool {
    DEFER_CAPACITY_WAKES.with(|depth| depth.get() != 0)
}

/// Preserve the original custody -> ledger lock order. A release performed
/// under a custody lock queues only a hint; user Wakers run after the caller's
/// entire retirement transaction returns and destroys all its lock guards.
/// Nested actor turns leave delivery to their outer lock-free poller boundary.
pub(crate) fn with_capacity_wakes_deferred<T>(
    ledger: &RuntimeLedger,
    body: impl FnOnce() -> T,
) -> T {
    let result = {
        let _deferral = TurnWakeDeferral::enter();
        body()
    };
    ledger.shared.pending_capacity_wakes.flush();
    result
}

// std Arc/Weak owns only a Waker's physical lifetime (the std Wake contract
// itself uses std Arc). All shared state and ordering still use std/loom locks.
struct RegistrantWake {
    waker: Mutex<Waker>,
    retired: AtomicBool,
}
impl RegistrantWake {
    fn wake(&self) {
        if !self.retired.load(Ordering::Acquire) {
            let waker = self
                .waker
                .lock()
                .expect("capacity registrant waker lock")
                .clone();
            waker.wake();
        }
    }
}

/// A non-cloneable registration owner. Drop cancels its wait; no explicit
/// consumption is required. This grants no reservation, Scope authority or
/// guarantee that a later retry will succeed.
#[must_use = "Retain the registrant while waiting; Drop cancels its pending wait"]
pub struct CapacityRegistrant {
    runtime: RuntimeLedger,
    id: CapacityRegistrantId,
    wake: std::sync::Arc<RegistrantWake>,
}
impl CapacityRegistrant {
    pub fn id(&self) -> CapacityRegistrantId {
        self.id
    }
    pub fn update_waker(&self, waker: &Waker) {
        let previous = std::mem::replace(
            &mut *self
                .wake
                .waker
                .lock()
                .expect("capacity registrant waker lock"),
            waker.clone(),
        );
        drop(previous);
    }
    pub fn cancel(&self) {
        let mut state = self
            .runtime
            .shared
            .state
            .lock()
            .expect("runtime ledger lock");
        state.capacity_waiters.cancel(self.id);
        drop(state);
        self.runtime.shared.pending_capacity_wakes.cancel(self.id);
    }
    pub(crate) fn belongs_to(&self, runtime: &RuntimeLedger) -> bool {
        Arc::ptr_eq(&self.runtime.shared, &runtime.shared)
    }
}
impl Drop for CapacityRegistrant {
    fn drop(&mut self) {
        self.wake.retired.store(true, Ordering::Release);
        self.cancel();
    }
}

/// A non-authorizing condition-variable domain, never an exact-key grant.
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
enum WaitDomain {
    Process(novarocks_control_runtime_core::identity::DimensionId),
    Keyed(
        novarocks_control_runtime_core::identity::FamilyId,
        novarocks_control_runtime_core::identity::DimensionId,
    ),
}
impl From<CapacityWaitTarget> for WaitDomain {
    fn from(target: CapacityWaitTarget) -> Self {
        match target {
            CapacityWaitTarget::Process(dimension) => Self::Process(dimension),
            CapacityWaitTarget::Keyed {
                family, dimension, ..
            } => Self::Keyed(family, dimension),
        }
    }
}
#[derive(Default)]
pub(crate) struct CapacityWaiters {
    targets: BTreeMap<WaitDomain, BTreeMap<CapacityRegistrantId, std::sync::Weak<RegistrantWake>>>,
    registrants: BTreeMap<CapacityRegistrantId, BTreeSet<WaitDomain>>,
    last_id: u64,
}
impl CapacityWaiters {
    pub(crate) fn cancel(&mut self, id: CapacityRegistrantId) {
        if let Some(domains) = self.registrants.remove(&id) {
            for domain in domains {
                // take_released may already have removed the triggering domain.
                if let Some(entries) = self.targets.get_mut(&domain) {
                    entries.remove(&id);
                    if entries.is_empty() {
                        self.targets.remove(&domain);
                    }
                }
            }
        }
    }
    pub(crate) fn register(
        &mut self,
        registrant: &CapacityRegistrant,
        denial: CapacityDenial,
    ) -> bool {
        let domain = WaitDomain::from(denial.wait_target());
        if !self
            .registrants
            .entry(registrant.id)
            .or_default()
            .insert(domain)
        {
            return false;
        }
        self.targets
            .entry(domain)
            .or_default()
            .insert(registrant.id, std::sync::Arc::downgrade(&registrant.wake));
        true
    }
    pub(crate) fn len(&self) -> usize {
        self.registrants.len()
    }
    pub(crate) fn take_released(&mut self, resources: &ReservationResources) -> Vec<CapacityWake> {
        let process = resources
            .quota
            .iter()
            .map(|(dimension, _)| CapacityWaitTarget::Process(dimension));
        let keyed = resources.keyed.iter().flat_map(|quota| {
            quota
                .quota
                .iter()
                .map(move |(dimension, _)| CapacityWaitTarget::Keyed {
                    family: quota.family,
                    key: quota.key,
                    dimension,
                })
        });
        let mut wakes = Vec::new();
        for target in process.chain(keyed) {
            let domain = WaitDomain::from(target);
            if let Some(waiters) = self.targets.remove(&domain) {
                for (id, wake) in waiters {
                    self.cancel(id);
                    // Preserve the actual release key, even for a spurious wake
                    // caused by another key in the same family/dimension domain.
                    wakes.push(CapacityWake { id, target, wake });
                }
            }
        }
        wakes
    }
}

pub(crate) struct CapacityWake {
    id: CapacityRegistrantId,
    target: CapacityWaitTarget,
    wake: std::sync::Weak<RegistrantWake>,
}
impl CapacityWake {
    fn deliver(self) {
        if let Some(wake) = self.wake.upgrade() {
            wake.wake();
        }
    }
}

/// A neutral link to one driver's fixed notifier. It retains neither driver,
/// Scope nor ledger, so it cannot create a runtime ownership cycle.
pub(crate) struct CapacityWakeNotifier {
    pub(crate) changed: Arc<Notify>,
}
#[derive(Default)]
struct PendingState {
    wakes: BTreeMap<CapacityRegistrantId, CapacityWake>,
    notifiers: Vec<std::sync::Weak<CapacityWakeNotifier>>,
}
#[derive(Default)]
pub(crate) struct PendingCapacityWakes {
    state: Mutex<PendingState>,
}
impl PendingCapacityWakes {
    pub(crate) fn attach(&self, notifier: &std::sync::Arc<CapacityWakeNotifier>) {
        let mut state = self.state.lock().expect("pending capacity wakes lock");
        state.notifiers.retain(|old| old.strong_count() != 0);
        if !state
            .notifiers
            .iter()
            .any(|old| old.ptr_eq(&std::sync::Arc::downgrade(notifier)))
        {
            state.notifiers.push(std::sync::Arc::downgrade(notifier));
        }
        if !state.wakes.is_empty() {
            notifier.changed.notify();
        }
    }
    fn enqueue(&self, wakes: Vec<CapacityWake>) {
        let notifiers = {
            let mut state = self.state.lock().expect("pending capacity wakes lock");
            for wake in wakes {
                state.wakes.insert(wake.id, wake);
            }
            state
                .notifiers
                .iter()
                .filter_map(std::sync::Weak::upgrade)
                .collect::<Vec<_>>()
        };
        for notifier in notifiers {
            notifier.changed.notify();
        }
    }
    fn cancel(&self, id: CapacityRegistrantId) {
        self.state
            .lock()
            .expect("pending capacity wakes lock")
            .wakes
            .remove(&id);
    }
    pub(crate) fn flush(&self) -> usize {
        if deferred() {
            return 0;
        }
        let wakes = std::mem::take(
            &mut self
                .state
                .lock()
                .expect("pending capacity wakes lock")
                .wakes,
        );
        let count = wakes.len();
        for (_, wake) in wakes {
            wake.deliver();
        }
        count
    }
}

impl RuntimeLedger {
    pub fn capacity_registrant(&self, waker: Waker) -> Result<CapacityRegistrant, RuntimeError> {
        let mut state = self.shared.state.lock().expect("runtime ledger lock");
        let next = state
            .capacity_waiters
            .last_id
            .checked_add(1)
            .ok_or(RuntimeError::IdentityExhausted)?;
        let id = CapacityRegistrantId::new(next).ok_or(RuntimeError::IdentityExhausted)?;
        state.capacity_waiters.last_id = next;
        drop(state);
        Ok(CapacityRegistrant {
            runtime: self.clone(),
            id,
            wake: std::sync::Arc::new(RegistrantWake {
                waker: Mutex::new(waker),
                retired: AtomicBool::new(false),
            }),
        })
    }
    pub fn capacity_waiter_count(&self) -> usize {
        self.shared
            .state
            .lock()
            .expect("runtime ledger lock")
            .capacity_waiters
            .len()
    }
    pub(crate) fn take_capacity_wakes(
        &self,
        state: &mut LedgerState,
        resources: &ReservationResources,
    ) -> Vec<CapacityWake> {
        let wakes = state.capacity_waiters.take_released(resources);
        for wake in &wakes {
            self.shared.record(|| HistoryFact::CapacityWaitNotified {
                registrant: wake.id,
                target: wake.target,
            });
        }
        wakes
    }
    pub(crate) fn deliver_capacity_wakes(&self, wakes: Vec<CapacityWake>) {
        if wakes.is_empty() {
            return;
        }
        if deferred() {
            self.shared.pending_capacity_wakes.enqueue(wakes);
        } else {
            for wake in wakes {
                wake.deliver();
            }
        }
    }
}

#[cfg(all(test, not(loom)))]
mod tests {
    use super::*;
    use novarocks_control_runtime_core::{
        capacity::{KeyedQuota, ResourceVector},
        identity::{CapacityKey, DimensionId, FamilyId, Generation},
        scope::{WorkAdmission, WorkKind},
    };
    #[test]
    fn arbitrary_permanently_oversized_empty_keys_do_not_grow_wait_indexes() {
        let process = DimensionId::new(1).unwrap();
        let keyed = DimensionId::new(2).unwrap();
        let family = FamilyId::new(1).unwrap();
        let generation = Generation::new(1).unwrap();
        let ledger = RuntimeLedger::with_keyed_families(
            ResourceVector::new([(process, 1)]).unwrap(),
            [(family, ResourceVector::new([(keyed, 1)]).unwrap())],
        )
        .unwrap();
        let scope = ledger.open_scope_next(None, generation).unwrap();
        let registrant = ledger.capacity_registrant(Waker::noop().clone()).unwrap();
        for key in 0..512 {
            let resources = ReservationResources::work_only().with_keyed(KeyedQuota {
                family,
                key: CapacityKey(key),
                quota: ResourceVector::new([(keyed, 2)]).unwrap(),
            });
            assert!(matches!(
                scope.reserve_next_or_wait(
                    generation,
                    resources,
                    WorkKind::Business,
                    WorkAdmission::External,
                    &registrant
                ),
                Err(RuntimeError::CapacityDenied(_))
            ));
            let state = ledger.shared.state.lock().unwrap();
            assert_eq!(state.capacity_waiters.registrants.len(), 1);
            assert_eq!(state.capacity_waiters.targets.len(), 1);
            assert_eq!(state.capacity_waiters.registrants[&registrant.id].len(), 1);
        }
        assert!(ledger.try_keyed_usage_snapshot().unwrap().is_empty());
        assert!(
            scope
                .reserve_next_or_wait(
                    generation,
                    ReservationResources::for_work(ResourceVector::new([(process, 2)]).unwrap()),
                    WorkKind::Business,
                    WorkAdmission::External,
                    &registrant,
                )
                .is_err()
        );
        {
            let state = ledger.shared.state.lock().unwrap();
            assert_eq!(state.capacity_waiters.registrants.len(), 1);
            assert_eq!(state.capacity_waiters.targets.len(), 2);
        }
        drop(registrant);
        let state = ledger.shared.state.lock().unwrap();
        assert!(state.capacity_waiters.registrants.is_empty());
        assert!(state.capacity_waiters.targets.is_empty());
    }
}
