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

//! The process-lived home for storage authorities.
//!
//! CAD-1 D10: an authority must outlive the query that first needed it. Every
//! credential structure this replaces was per-attempt — the lease id itself is
//! minted from the query execution id — so a second query over the same table
//! paid for a fresh acquisition and a fresh object-store client. With the
//! executor obtaining its own material that cost would be multiplied by the
//! number of executor nodes, and none of those calls ride anything: an executor
//! reads no metadata, so a credential call it makes is pure overhead.
//!
//! Nothing in this registry is keyed by a query. That is the whole contract.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use super::{
    AuthorityMaterial, AuthorityMaterialSource, RefreshExecutor, RefreshPolicy, StorageAuthority,
    StorageAuthorityId,
};
use crate::{FileError, FileResult};

pub const DEFAULT_STORAGE_AUTHORITY_CAPACITY: usize = 256;
pub const DEFAULT_STORAGE_AUTHORITY_IDLE_TTL: Duration = Duration::from_secs(3600);
const MIN_STORAGE_AUTHORITY_CAPACITY: usize = 1;
const MAX_STORAGE_AUTHORITY_CAPACITY: usize = 4096;
const MIN_STORAGE_AUTHORITY_IDLE_TTL: Duration = Duration::from_secs(60);
const MAX_STORAGE_AUTHORITY_IDLE_TTL: Duration = Duration::from_secs(24 * 3600);

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct StorageAuthorityRegistryOptions {
    pub capacity: usize,
    pub idle_ttl: Duration,
}

impl Default for StorageAuthorityRegistryOptions {
    fn default() -> Self {
        Self {
            capacity: DEFAULT_STORAGE_AUTHORITY_CAPACITY,
            idle_ttl: DEFAULT_STORAGE_AUTHORITY_IDLE_TTL,
        }
    }
}

impl StorageAuthorityRegistryOptions {
    pub fn validate(self) -> FileResult<Self> {
        if !(MIN_STORAGE_AUTHORITY_CAPACITY..=MAX_STORAGE_AUTHORITY_CAPACITY)
            .contains(&self.capacity)
        {
            return Err(FileError::invalid(format!(
                "storage authority registry capacity must be in \
                 {MIN_STORAGE_AUTHORITY_CAPACITY}..={MAX_STORAGE_AUTHORITY_CAPACITY}"
            )));
        }
        if !(MIN_STORAGE_AUTHORITY_IDLE_TTL..=MAX_STORAGE_AUTHORITY_IDLE_TTL)
            .contains(&self.idle_ttl)
        {
            return Err(FileError::invalid(
                "storage authority registry idle TTL must be in 60s..=24h",
            ));
        }
        Ok(self)
    }
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct StorageAuthorityRegistryMetrics {
    /// A query found an authority a previous query had already built. Each hit
    /// is one acquisition and one operator construction that did not happen.
    pub hits: u64,
    pub misses: u64,
    pub capacity_evictions: u64,
    pub idle_expirations: u64,
    pub resident: usize,
    pub high_water: usize,
}

struct RegistryEntry {
    authority: Arc<StorageAuthority>,
    last_used: Instant,
}

#[derive(Default)]
struct RegistryInner {
    entries: HashMap<StorageAuthorityId, RegistryEntry>,
    high_water: usize,
}

/// A bounded, process-lived set of authorities.
///
/// One per process, like the catalog manager it mirrors: two registries would
/// be two owners of the same capability, and a query that reached the second
/// would pay the acquisition the first had already paid.
pub struct StorageAuthorityRegistry {
    options: StorageAuthorityRegistryOptions,
    executor: Arc<dyn RefreshExecutor>,
    policy: RefreshPolicy,
    inner: Mutex<RegistryInner>,
    hits: AtomicU64,
    misses: AtomicU64,
    capacity_evictions: AtomicU64,
    idle_expirations: AtomicU64,
    #[cfg(novarocks_dev_controls)]
    close_controls: Mutex<Option<super::StorageAuthorityCloseControls>>,
}

impl std::fmt::Debug for StorageAuthorityRegistry {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("StorageAuthorityRegistry")
            .field("options", &self.options)
            .finish_non_exhaustive()
    }
}

impl StorageAuthorityRegistry {
    pub fn new(
        options: StorageAuthorityRegistryOptions,
        executor: Arc<dyn RefreshExecutor>,
        policy: RefreshPolicy,
    ) -> FileResult<Self> {
        Ok(Self {
            options: options.validate()?,
            executor,
            policy,
            inner: Mutex::new(RegistryInner::default()),
            hits: AtomicU64::new(0),
            misses: AtomicU64::new(0),
            capacity_evictions: AtomicU64::new(0),
            idle_expirations: AtomicU64::new(0),
            #[cfg(novarocks_dev_controls)]
            close_controls: Mutex::new(None),
        })
    }

    /// A registry on the default bounds, which are constants inside the
    /// validated range and therefore cannot be rejected. Exists so a bundle that
    /// returns `Self` can hold one without a panic path in its constructor.
    pub fn with_default_options(executor: Arc<dyn RefreshExecutor>, policy: RefreshPolicy) -> Self {
        let options = StorageAuthorityRegistryOptions::default();
        debug_assert!(options.validate().is_ok(), "defaults must stay in range");
        Self {
            options,
            executor,
            policy,
            inner: Mutex::new(RegistryInner::default()),
            hits: AtomicU64::new(0),
            misses: AtomicU64::new(0),
            capacity_evictions: AtomicU64::new(0),
            idle_expirations: AtomicU64::new(0),
            #[cfg(novarocks_dev_controls)]
            close_controls: Mutex::new(None),
        }
    }

    /// Installed by role composition before authority consumers are opened.
    /// Existing authorities are registered under their exact owner identity.
    #[cfg(novarocks_dev_controls)]
    pub fn attach_test_close_controls(
        &self,
        controls: super::StorageAuthorityCloseControls,
    ) -> Result<(), String> {
        let inner = self.lock_inner();
        let mut installed = self
            .close_controls
            .lock()
            .map_err(|_| "authority close composition lock failed")?;
        if installed.is_some() {
            return Err("authority close controls were already installed".into());
        }
        for entry in inner.entries.values() {
            controls.register(&entry.authority)?;
        }
        *installed = Some(controls);
        Ok(())
    }

    /// The authority for this identity, built once and reused afterwards.
    ///
    /// `initialize` runs only on a miss. Its seed is installed before the
    /// authority becomes visible in the registry. A hit returns the authority a
    /// previous query already built, with whatever material it currently
    /// holds — which is exactly the cross-query reuse CAD-1 D10 requires and
    /// acceptance 16 measures.
    pub fn authority<F>(
        &self,
        id: &StorageAuthorityId,
        now: Instant,
        initialize: F,
    ) -> Arc<StorageAuthority>
    where
        F: FnOnce() -> (Arc<dyn AuthorityMaterialSource>, Option<AuthorityMaterial>),
    {
        let mut inner = self.lock_inner();
        self.expire_idle(&mut inner, now);

        if let Some(entry) = inner.entries.get_mut(id) {
            entry.last_used = now;
            self.hits.fetch_add(1, Ordering::Relaxed);
            return Arc::clone(&entry.authority);
        }

        self.misses.fetch_add(1, Ordering::Relaxed);
        let (source, initial_material) = initialize();
        let authority = Arc::new(StorageAuthority::new(
            id.clone(),
            source,
            Arc::clone(&self.executor),
            self.policy,
        ));
        if let Some(material) = initial_material {
            authority.install_material(material);
        }
        #[cfg(novarocks_dev_controls)]
        if let Some(controls) = self
            .close_controls
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .as_ref()
        {
            // Observer failure is retained for explicit role-owned join. It
            // does not change ordinary storage authority or query semantics.
            controls.register_or_record_failure(&authority);
        }
        inner.entries.insert(
            id.clone(),
            RegistryEntry {
                authority: Arc::clone(&authority),
                last_used: now,
            },
        );
        while inner.entries.len() > self.options.capacity {
            if !self.evict_least_recently_used(&mut inner) {
                break;
            }
        }
        inner.high_water = inner.high_water.max(inner.entries.len());
        authority
    }

    /// Whether an authority for this identity is already resident, without
    /// creating one or counting a hit.
    pub fn is_resident(&self, id: &StorageAuthorityId) -> bool {
        self.lock_inner().entries.contains_key(id)
    }

    pub fn metrics(&self) -> StorageAuthorityRegistryMetrics {
        let inner = self.lock_inner();
        StorageAuthorityRegistryMetrics {
            hits: self.hits.load(Ordering::Relaxed),
            misses: self.misses.load(Ordering::Relaxed),
            capacity_evictions: self.capacity_evictions.load(Ordering::Relaxed),
            idle_expirations: self.idle_expirations.load(Ordering::Relaxed),
            resident: inner.entries.len(),
            high_water: inner.high_water,
        }
    }

    fn lock_inner(&self) -> std::sync::MutexGuard<'_, RegistryInner> {
        self.inner
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    /// Drop authorities nothing has asked for in a while.
    ///
    /// Eviction is not revocation: an authority still held by a running reader
    /// keeps working, because the reader holds the `Arc`. All eviction does is
    /// stop this registry from handing the same one out again.
    fn expire_idle(&self, inner: &mut RegistryInner, now: Instant) {
        let idle_ttl = self.options.idle_ttl;
        let before = inner.entries.len();
        inner.entries.retain(|_, entry| {
            now.checked_duration_since(entry.last_used)
                .is_none_or(|idle| idle < idle_ttl)
        });
        let expired = before.saturating_sub(inner.entries.len());
        if expired > 0 {
            self.idle_expirations
                .fetch_add(expired as u64, Ordering::Relaxed);
        }
    }

    fn evict_least_recently_used(&self, inner: &mut RegistryInner) -> bool {
        let oldest = inner
            .entries
            .iter()
            .min_by_key(|(_, entry)| entry.last_used)
            .map(|(id, _)| id.clone());
        match oldest {
            Some(id) => {
                inner.entries.remove(&id);
                self.capacity_evictions.fetch_add(1, Ordering::Relaxed);
                true
            }
            None => false,
        }
    }
}
