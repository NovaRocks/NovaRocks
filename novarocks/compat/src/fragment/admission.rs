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

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

use crate::thrift::descriptors;
use novarocks::runtime::endpoint::RuntimeEndpoint;
use novarocks::runtime::starrocks_fragment_query::QueryCleanupLease;
use novarocks_types::QueryId;
use novarocks_types::UniqueId;

use crate::fragment::dependency::DependencyResolutionError;

#[derive(Clone)]
pub(crate) struct PrelaunchCancellationToken {
    cancelled: Arc<AtomicBool>,
    frontend_endpoint: Option<RuntimeEndpoint>,
}

impl PrelaunchCancellationToken {
    pub(crate) fn check(&self, dependency_id: u64) -> Result<(), DependencyResolutionError> {
        if self.cancelled.load(Ordering::Acquire) {
            Err(DependencyResolutionError::Cancelled { dependency_id })
        } else {
            Ok(())
        }
    }

    pub(crate) fn frontend_endpoint(&self) -> Option<&RuntimeEndpoint> {
        self.frontend_endpoint.as_ref()
    }
}

#[derive(Clone)]
struct PrelaunchEntry {
    query_id: QueryId,
    generation: u64,
    cancelled: Arc<AtomicBool>,
}

#[derive(Default)]
pub(crate) struct PrelaunchRegistry {
    entries: Mutex<HashMap<UniqueId, PrelaunchEntry>>,
}

impl PrelaunchRegistry {
    pub(crate) fn install<I>(
        self: &Arc<Self>,
        query_id: QueryId,
        generation: u64,
        finst_ids: I,
    ) -> Result<PrelaunchGuard, String>
    where
        I: IntoIterator<Item = UniqueId>,
    {
        let finst_ids = finst_ids.into_iter().collect::<Vec<_>>();
        if finst_ids.is_empty() {
            return Err("prelaunch guard requires at least one fragment instance".to_string());
        }
        let cancelled = Arc::new(AtomicBool::new(false));
        let mut entries = self.entries.lock().expect("prelaunch registry lock");
        for finst_id in &finst_ids {
            if entries.contains_key(finst_id) {
                return Err(format!("fragment instance {finst_id} is already preparing"));
            }
        }
        for finst_id in &finst_ids {
            entries.insert(
                *finst_id,
                PrelaunchEntry {
                    query_id,
                    generation,
                    cancelled: Arc::clone(&cancelled),
                },
            );
        }
        Ok(PrelaunchGuard {
            registry: Arc::clone(self),
            query_id,
            generation,
            finst_ids,
            cancelled,
            frontend_endpoint: None,
            released: false,
        })
    }

    pub(crate) fn cancel_or_run<F>(&self, finst_id: UniqueId, runtime_cancel: F) -> bool
    where
        F: FnOnce(),
    {
        let entries = self.entries.lock().expect("prelaunch registry lock");
        if let Some(entry) = entries.get(&finst_id) {
            entry.cancelled.store(true, Ordering::Release);
            true
        } else {
            // Retain the barrier while runtime cleanup resolves finst-keyed resources.
            runtime_cancel();
            false
        }
    }

    #[cfg(test)]
    pub(crate) fn cancel(&self, finst_id: UniqueId) -> bool {
        self.entries
            .lock()
            .expect("prelaunch registry lock")
            .get(&finst_id)
            .map(|entry| {
                entry.cancelled.store(true, Ordering::Release);
            })
            .is_some()
    }

    #[cfg(test)]
    fn snapshot_count(&self) -> usize {
        self.entries.lock().expect("prelaunch registry lock").len()
    }
}

pub(crate) struct PrelaunchGuard {
    registry: Arc<PrelaunchRegistry>,
    query_id: QueryId,
    generation: u64,
    finst_ids: Vec<UniqueId>,
    cancelled: Arc<AtomicBool>,
    frontend_endpoint: Option<RuntimeEndpoint>,
    released: bool,
}

impl PrelaunchGuard {
    pub(crate) fn cancellation_token(&self) -> PrelaunchCancellationToken {
        PrelaunchCancellationToken {
            cancelled: Arc::clone(&self.cancelled),
            frontend_endpoint: self.frontend_endpoint.clone(),
        }
    }

    pub(crate) fn set_frontend_endpoint(&mut self, endpoint: Option<RuntimeEndpoint>) {
        self.frontend_endpoint = endpoint;
    }

    pub(crate) fn handoff<T, F>(mut self, make_runtime_visible: F) -> Result<T, String>
    where
        F: FnOnce() -> Result<T, String>,
    {
        let registry = Arc::clone(&self.registry);
        let mut entries = registry.entries.lock().expect("prelaunch registry lock");
        if self.cancelled.load(Ordering::Acquire) {
            return Err("fragment preparation was cancelled".to_string());
        }
        let value = make_runtime_visible()?;
        for finst_id in &self.finst_ids {
            if entries.get(finst_id).is_some_and(|entry| {
                entry.query_id == self.query_id && entry.generation == self.generation
            }) {
                entries.remove(finst_id);
            }
        }
        self.released = true;
        Ok(value)
    }

    fn release(&mut self) {
        if self.released {
            return;
        }
        let registry = Arc::clone(&self.registry);
        let mut entries = registry.entries.lock().expect("prelaunch registry lock");
        for finst_id in &self.finst_ids {
            if entries.get(finst_id).is_some_and(|entry| {
                entry.query_id == self.query_id && entry.generation == self.generation
            }) {
                entries.remove(finst_id);
            }
        }
        self.released = true;
    }
}

impl Drop for PrelaunchGuard {
    fn drop(&mut self) {
        self.release();
    }
}

#[derive(Clone)]
struct DescriptorCacheEntry {
    generation: u64,
    descriptor: Arc<descriptors::TDescriptorTable>,
}

#[derive(Default)]
struct DescriptorCacheInner {
    next_generation: u64,
    entries: HashMap<QueryId, DescriptorCacheEntry>,
}

#[derive(Default)]
pub(crate) struct DescriptorTransportCache {
    inner: Arc<Mutex<DescriptorCacheInner>>,
}

#[derive(Debug)]
pub(crate) struct DescriptorPreparation {
    query_id: QueryId,
    generation: u64,
    descriptor: Option<Arc<descriptors::TDescriptorTable>>,
    commit_descriptor: bool,
}

pub(crate) struct DescriptorLeaseFactory {
    inner: Arc<Mutex<DescriptorCacheInner>>,
    query_id: QueryId,
    generation: u64,
}

impl DescriptorLeaseFactory {
    pub(crate) fn into_cleanup_lease(self) -> QueryCleanupLease {
        QueryCleanupLease::from_release(move || {
            let mut inner = self.inner.lock().expect("descriptor cache lock");
            if inner
                .entries
                .get(&self.query_id)
                .is_some_and(|entry| entry.generation == self.generation)
            {
                inner.entries.remove(&self.query_id);
            }
        })
    }
}

impl DescriptorPreparation {
    pub(crate) const fn generation(&self) -> u64 {
        self.generation
    }

    pub(crate) fn descriptor(&self) -> Option<&descriptors::TDescriptorTable> {
        self.descriptor.as_deref()
    }
}

impl DescriptorTransportCache {
    pub(crate) fn prepare(
        &self,
        query_id: QueryId,
        incoming: Option<&descriptors::TDescriptorTable>,
        fallback: Option<&descriptors::TDescriptorTable>,
    ) -> Result<DescriptorPreparation, String> {
        let mut inner = self.inner.lock().expect("descriptor cache lock");
        let existing = inner.entries.get(&query_id).cloned();
        let selected = incoming.or(fallback);
        let cached_marker = selected.map(descriptor_is_cached).unwrap_or(false);
        let concrete = selected.filter(|descriptor| {
            !descriptor_is_cached(descriptor) && !descriptor_is_empty(descriptor)
        });
        if cached_marker {
            let entry = existing.ok_or_else(|| {
                "Query terminates prematurely (missing desc_tbl transport cache)".to_string()
            })?;
            return Ok(DescriptorPreparation {
                query_id,
                generation: entry.generation,
                descriptor: Some(entry.descriptor),
                commit_descriptor: false,
            });
        }
        if let Some(entry) = existing {
            if let Some(concrete) = concrete
                && entry.descriptor.as_ref() != concrete
            {
                return Err("conflicting descriptor table for active query generation".to_string());
            }
            return Ok(DescriptorPreparation {
                query_id,
                generation: entry.generation,
                descriptor: concrete
                    .map(|value| Arc::new(value.clone()))
                    .or(Some(entry.descriptor)),
                commit_descriptor: false,
            });
        }
        inner.next_generation = inner.next_generation.wrapping_add(1).max(1);
        Ok(DescriptorPreparation {
            query_id,
            generation: inner.next_generation,
            descriptor: concrete.map(|value| Arc::new(value.clone())),
            commit_descriptor: concrete.is_some(),
        })
    }

    pub(crate) fn prepare_batch(
        &self,
        query_id: QueryId,
        common: Option<&descriptors::TDescriptorTable>,
        unique: &[Option<&descriptors::TDescriptorTable>],
    ) -> Result<DescriptorPreparation, String> {
        let mut concrete = None;
        let mut marker = None;
        for candidate in common.into_iter().chain(unique.iter().copied().flatten()) {
            if descriptor_is_cached(candidate) {
                marker = Some(candidate);
                continue;
            }
            if descriptor_is_empty(candidate) {
                continue;
            }
            if let Some(existing) = concrete {
                if existing != candidate {
                    return Err(
                        "conflicting concrete descriptor tables in StarRocks batch".to_string()
                    );
                }
            } else {
                concrete = Some(candidate);
            }
        }
        self.prepare(query_id, concrete.or(marker), None)
    }

    pub(crate) fn commit_handoff<T, F>(
        &self,
        preparation: &DescriptorPreparation,
        make_runtime_visible: F,
    ) -> Result<T, String>
    where
        F: FnOnce(Option<DescriptorLeaseFactory>) -> Result<T, String>,
    {
        let mut inner = self.inner.lock().expect("descriptor cache lock");
        let mut inserted = false;
        if let Some(descriptor) = preparation.descriptor.as_ref() {
            if let Some(entry) = inner.entries.get(&preparation.query_id) {
                if entry.generation != preparation.generation
                    || entry.descriptor.as_ref() != descriptor.as_ref()
                {
                    return Err(
                        "descriptor cache generation changed during preparation".to_string()
                    );
                }
            } else if preparation.commit_descriptor {
                inner.entries.insert(
                    preparation.query_id,
                    DescriptorCacheEntry {
                        generation: preparation.generation,
                        descriptor: Arc::clone(descriptor),
                    },
                );
                inserted = true;
            } else {
                return Err("descriptor cache entry disappeared during preparation".to_string());
            }
        }
        let lease_factory = preparation
            .descriptor
            .as_ref()
            .map(|_| DescriptorLeaseFactory {
                inner: Arc::clone(&self.inner),
                query_id: preparation.query_id,
                generation: preparation.generation,
            });
        match make_runtime_visible(lease_factory) {
            Ok(value) => Ok(value),
            Err(error) => {
                if inserted
                    && inner
                        .entries
                        .get(&preparation.query_id)
                        .is_some_and(|entry| entry.generation == preparation.generation)
                {
                    inner.entries.remove(&preparation.query_id);
                }
                Err(error)
            }
        }
    }

    #[cfg(test)]
    fn snapshot_generation(&self, query_id: QueryId) -> Option<u64> {
        self.inner
            .lock()
            .expect("descriptor cache lock")
            .entries
            .get(&query_id)
            .map(|entry| entry.generation)
    }
}

fn descriptor_is_cached(descriptor: &descriptors::TDescriptorTable) -> bool {
    descriptor.is_cached.unwrap_or(false)
}

fn descriptor_is_empty(descriptor: &descriptors::TDescriptorTable) -> bool {
    !descriptor_is_cached(descriptor)
        && descriptor.tuple_descriptors.is_empty()
        && descriptor
            .table_descriptors
            .as_ref()
            .map(Vec::is_empty)
            .unwrap_or(true)
        && descriptor
            .slot_descriptors
            .as_ref()
            .map(Vec::is_empty)
            .unwrap_or(true)
}

#[cfg(test)]
mod tests {
    use std::sync::mpsc;

    use novarocks_types::QueryId;

    use super::{DescriptorTransportCache, PrelaunchRegistry};
    use crate::thrift::descriptors;
    use novarocks_types::UniqueId;

    fn descriptor(tuple_id: i32) -> descriptors::TDescriptorTable {
        descriptors::TDescriptorTable::new(
            vec![],
            vec![descriptors::TTupleDescriptor::new(
                tuple_id, None, None, None, None,
            )],
            None,
            None,
        )
    }

    fn cached_marker() -> descriptors::TDescriptorTable {
        descriptors::TDescriptorTable::new(vec![], vec![], None, Some(true))
    }

    #[test]
    fn batch_descriptor_rejects_conflicting_concrete_tables() {
        let cache = DescriptorTransportCache::default();
        let first = descriptor(1);
        let second = descriptor(2);
        let error = cache
            .prepare_batch(QueryId::new(1, 2), Some(&first), &[Some(&second)])
            .expect_err("conflicting descriptors must fail");
        assert!(error.contains("conflicting concrete descriptor"));
    }

    #[test]
    fn active_descriptor_rejects_a_conflicting_single_fragment() {
        let cache = DescriptorTransportCache::default();
        let query_id = QueryId::new(2, 3);
        let first = descriptor(1);
        let initial = cache
            .prepare(query_id, Some(&first), None)
            .expect("prepare");
        let lease = cache
            .commit_handoff(&initial, |lease| {
                Ok(lease.expect("lease").into_cleanup_lease())
            })
            .expect("commit");

        let second = descriptor(2);
        let error = cache
            .prepare(query_id, Some(&second), None)
            .expect_err("conflicting active descriptor must fail");
        assert!(error.contains("conflicting descriptor table"));
        drop(lease);
    }

    #[test]
    fn cached_marker_reuses_committed_generation() {
        let cache = DescriptorTransportCache::default();
        let query_id = QueryId::new(3, 4);
        let concrete = descriptor(3);
        let preparation = cache
            .prepare(query_id, Some(&concrete), None)
            .expect("prepare");
        let generation = preparation.generation();
        let lease = cache
            .commit_handoff(&preparation, |lease| {
                Ok(lease.expect("lease").into_cleanup_lease())
            })
            .expect("commit");
        let reused = cache
            .prepare(query_id, Some(&cached_marker()), None)
            .expect("cached marker");
        assert_eq!(reused.generation(), generation);
        drop(lease);
        assert_eq!(cache.snapshot_generation(query_id), None);
    }

    #[test]
    fn old_generation_lease_does_not_remove_reused_query_descriptor() {
        let cache = DescriptorTransportCache::default();
        let query_id = QueryId::new(4, 5);
        let first = cache
            .prepare(query_id, Some(&descriptor(4)), None)
            .expect("prepare first generation");
        let first_generation = first.generation();
        let first_lease = cache
            .commit_handoff(&first, |lease| {
                Ok(lease.expect("lease").into_cleanup_lease())
            })
            .expect("commit first generation");
        cache
            .inner
            .lock()
            .expect("descriptor cache lock")
            .entries
            .remove(&query_id);

        let second = cache
            .prepare(query_id, Some(&descriptor(5)), None)
            .expect("prepare reused generation");
        let second_generation = second.generation();
        assert!(second_generation > first_generation);
        let second_lease = cache
            .commit_handoff(&second, |lease| {
                Ok(lease.expect("lease").into_cleanup_lease())
            })
            .expect("commit second generation");
        drop(first_lease);
        assert_eq!(cache.snapshot_generation(query_id), Some(second_generation));
        drop(second_lease);
        assert_eq!(cache.snapshot_generation(query_id), None);
    }

    #[test]
    fn failed_handoff_rolls_back_a_new_descriptor_entry() {
        let cache = DescriptorTransportCache::default();
        let query_id = QueryId::new(6, 7);
        let preparation = cache
            .prepare(query_id, Some(&descriptor(6)), None)
            .expect("prepare");
        let result: Result<(), String> = cache.commit_handoff(&preparation, |_| {
            Err("query preflight rejected handoff".to_string())
        });
        assert!(result.is_err());
        assert_eq!(cache.snapshot_generation(query_id), None);
    }

    #[test]
    fn duplicate_prelaunch_is_rejected_and_batch_handoff_clears_every_entry() {
        let registry = std::sync::Arc::new(PrelaunchRegistry::default());
        let query_id = QueryId::new(8, 9);
        let finst_ids = [UniqueId::new(10, 11), UniqueId::new(12, 13)];
        let guard = registry
            .install(query_id, 1, finst_ids)
            .expect("install batch prelaunch");
        assert!(registry.install(query_id, 1, [finst_ids[0]]).is_err());
        let published = guard.handoff(|| Ok(finst_ids.len())).expect("handoff");
        assert_eq!(published, finst_ids.len());
        assert_eq!(registry.snapshot_count(), 0);
    }

    #[test]
    fn cancelled_prelaunch_handoff_publishes_nothing() {
        let registry = std::sync::Arc::new(PrelaunchRegistry::default());
        let finst_id = UniqueId::new(5, 6);
        let guard = registry
            .install(QueryId::new(7, 8), 1, [finst_id])
            .expect("install");
        assert!(registry.cancel(finst_id));
        assert!(guard.handoff(|| Ok(())).is_err());
        assert_eq!(registry.snapshot_count(), 0);
    }

    #[test]
    fn runtime_cancel_barrier_blocks_reused_finst_install() {
        let registry = std::sync::Arc::new(PrelaunchRegistry::default());
        let finst_id = UniqueId::new(9, 10);
        let (started_tx, started_rx) = mpsc::channel();
        let (release_tx, release_rx) = mpsc::channel();
        let cancel_registry = std::sync::Arc::clone(&registry);
        let cancel = std::thread::spawn(move || {
            cancel_registry.cancel_or_run(finst_id, || {
                started_tx.send(()).expect("start cleanup");
                release_rx.recv().expect("release cleanup");
            })
        });
        started_rx.recv().expect("cleanup started");
        let (installed_tx, installed_rx) = mpsc::channel();
        let install_registry = std::sync::Arc::clone(&registry);
        let install = std::thread::spawn(move || {
            let guard = install_registry
                .install(QueryId::new(11, 12), 1, [finst_id])
                .expect("install after cleanup");
            installed_tx.send(()).expect("installed");
            drop(guard);
        });
        assert!(installed_rx.try_recv().is_err());
        release_tx.send(()).expect("release");
        assert!(!cancel.join().expect("cancel result"));
        installed_rx.recv().expect("install completes");
        install.join().expect("install join");
    }
}
