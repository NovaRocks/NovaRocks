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

//! Process assembly and the unique, non-cloneable capacity writer.
use crate::sync::{Arc, AtomicU64, Mutex, Ordering, Weak};
use crate::{
    account::{Account, AccountState, TopUpPolicy},
    error::{CapacityError, ConfigError, MetadataRegistryLabel},
    ids::{AccountKind, ConfigVersion, ExternalRef},
    policy::LimitDimension,
    snapshot::AuthoritySnapshot,
};
use std::sync::OnceLock;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AuthorityConfig {
    pub process_bound_bytes: u64,
    pub capacity_bytes: u64,
    pub headroom_budget_bytes: u64,
    pub max_accounts: u32,
    pub event_capacity: u32,
    pub top_up: TopUpPolicy,
    pub max_active_owners: u32,
    pub metadata_budget_bytes: u64,
}
impl AuthorityConfig {
    pub const fn new(
        process_bound_bytes: u64,
        capacity_bytes: u64,
        headroom_budget_bytes: u64,
    ) -> Self {
        Self {
            process_bound_bytes,
            capacity_bytes,
            headroom_budget_bytes,
            max_accounts: 65_536,
            event_capacity: 65_536,
            top_up: TopUpPolicy::uniform(256 * 1024),
            max_active_owners: 65_536,
            metadata_budget_bytes: 64 * 1024 * 1024,
        }
    }
    pub const fn validate(&self) -> Result<(), ConfigError> {
        if self.process_bound_bytes == 0 {
            return Err(ConfigError::ProcessBoundIsZero);
        }
        if self.max_accounts == 0 {
            return Err(ConfigError::MetadataLimitIsZero {
                registry: MetadataRegistryLabel::Accounts,
            });
        }
        if self.max_active_owners == 0 || self.metadata_budget_bytes == 0 {
            return Err(ConfigError::MetadataLimitIsZero {
                registry: MetadataRegistryLabel::Owners,
            });
        }
        let store_capacity = if cfg!(loom) {
            256
        } else {
            crate::lane::MAX_RECORDS
        };
        let funding_capacity = crate::lane::store::funding_record_capacity(store_capacity);
        if self.max_active_owners > funding_capacity {
            return Err(ConfigError::OwnersExceedFundingRecords {
                owners: self.max_active_owners,
                funding_records: funding_capacity,
            });
        }
        if match self.capacity_bytes.checked_add(self.headroom_budget_bytes) {
            Some(total) => total > self.process_bound_bytes,
            None => true,
        } {
            return Err(ConfigError::CapacityExceedsProcessBound {
                capacity_bytes: self.capacity_bytes,
                headroom_budget_bytes: self.headroom_budget_bytes,
                process_bound_bytes: self.process_bound_bytes,
            });
        }
        Ok(())
    }
}
#[derive(Debug)]
pub(crate) struct Shared {
    pub record_store: crate::lane::RecordStorePin,
    pub observation_lanes: Mutex<crate::lane::registry::ObservationRegistry>,
    pub ceiling: u64,
    pub top_up: TopUpPolicy,
    pub target: AtomicU64,
    pub capacity_revision: AtomicU64,
    pub next_id: AtomicU64,
    pub membership_revision: AtomicU64,
    pub max_accounts: u32,
    pub accounts: Mutex<AccountRegistry>,
    pub writer_taken: Mutex<bool>,
    pub assembly: Mutex<()>,
    pub domains: Mutex<crate::domain::DomainRegistry>,
    pub max_active_owners: u32,
    pub metadata_budget: u64,
    pub storage_bytes: AtomicU64,
    pub maintenance: Mutex<crate::maintenance::MaintenanceState>,
}
#[derive(Debug)]
/// Process-local funding assembly. Allocation facts live in the independent
/// process store, so real late frees remain safe after this authority drops.
/// Shutdown closes production and detaches funding cycles; it is not executor
/// or I/O teardown evidence and does not revoke outstanding allocation facts.
pub struct MemoryAuthority {
    config: AuthorityConfig,
    pub(crate) shared: Arc<Shared>,
    root: Account,
    control: OnceLock<Account>,
}
impl MemoryAuthority {
    pub fn new(config: AuthorityConfig) -> Result<Self, ConfigError> {
        config.validate()?;
        let shared = Arc::new(Shared {
            #[cfg(not(loom))]
            record_store: crate::lane::RecordStorePin::Global,
            #[cfg(loom)]
            record_store: crate::lane::RecordStorePin::owned(256),
            observation_lanes: Mutex::new(crate::lane::registry::ObservationRegistry::new(
                if cfg!(loom) {
                    256
                } else {
                    crate::lane::MAX_RECORDS as usize
                },
            )),
            ceiling: config.capacity_bytes,
            top_up: config.top_up,
            target: AtomicU64::new(config.capacity_bytes),
            capacity_revision: AtomicU64::new(1),
            next_id: AtomicU64::new(2),
            membership_revision: AtomicU64::new(1),
            max_accounts: config.max_accounts,
            accounts: Mutex::new(AccountRegistry {
                records: (0..config.max_accounts).map(|_| None).collect(),
                free_slots: (1..config.max_accounts as usize).rev().collect(),
                upper: 1,
            }),
            writer_taken: Mutex::new(false),
            assembly: Mutex::new(()),
            domains: Mutex::new({
                let slots = (config.metadata_budget_bytes
                    / (crate::domain::OWNER_METADATA_BYTES
                        + (2 * std::mem::size_of::<usize>()) as u64))
                    as usize;
                crate::domain::DomainRegistry {
                    records: (0..slots).map(|_| None).collect(),
                    free_slots: (0..slots).rev().collect(),
                    active: 0,
                    metadata: 0,
                    upper: 0,
                }
            }),
            max_active_owners: config.max_active_owners,
            metadata_budget: config.metadata_budget_bytes,
            storage_bytes: AtomicU64::new(0),
            maintenance: Mutex::new(crate::maintenance::MaintenanceState::default()),
        });
        let root = Account::new_root(shared.clone());
        shared.accounts.lock().unwrap().records[0] = Some(Arc::downgrade(&root.0));
        root.0.slot.store(0, Ordering::Relaxed);
        // Index backing is allocated once and belongs to storage, not to an
        // individual execution account or to an uncounted residual count cap.
        let storage = (std::mem::size_of::<Shared>() + 2 * std::mem::size_of::<usize>()) as u64
            + crate::account::ROOT_ACCOUNT_METADATA_BYTES
            + {
                let registry = shared.domains.lock().unwrap();
                (registry.records.capacity()
                    * std::mem::size_of::<Option<Arc<crate::domain::Domain>>>()
                    + registry.free_slots.capacity() * std::mem::size_of::<usize>())
                    as u64
            }
            + {
                let registry = shared.accounts.lock().unwrap();
                (registry.records.capacity() * std::mem::size_of::<Option<Weak<AccountState>>>()
                    + registry.free_slots.capacity() * std::mem::size_of::<usize>())
                    as u64
            };
        if storage > config.capacity_bytes {
            return Err(ConfigError::CoreMetadataExceedsCapacity {
                metadata_bytes: storage,
                capacity_bytes: config.capacity_bytes,
            });
        }
        shared.storage_bytes.store(storage, Ordering::Relaxed);
        {
            let mut s = root.0.ledger.lock().unwrap();
            s.committed = storage;
            s.peak = storage;
            root.0.committed.store(storage, Ordering::Release);
        }
        Ok(Self {
            config,
            shared,
            root,
            control: OnceLock::new(),
        })
    }
    /// S1 observation storage is diagnostic metadata, independent of funding C.
    pub fn observation_metadata_bytes(&self) -> u64 {
        let registry = self.shared.observation_lanes.lock().unwrap();
        (registry.records.capacity() * std::mem::size_of::<Option<crate::Lane>>()) as u64
            + registry.occupied as u64 * crate::lane::OBSERVATION_LANE_METADATA_BYTES
    }
    /// Bound one original maintenance pass by the actual immutable index
    /// lengths. Stopped domains can retain records after leaving the active
    /// owner cap, so max_active_owners is not this traversal's domain bound.
    /// This observation does not pin membership, complete a busy sweep, or
    /// authorize any allocation.
    pub fn maintenance_scan_bound(&self) -> Result<usize, CapacityError> {
        let domains = self.shared.domains.lock().unwrap().records.len();
        let accounts = self.shared.accounts.lock().unwrap().records.len();
        domains.checked_add(accounts).ok_or(CapacityError::Invalid {
            detail: "maintenance registry scan bound overflows usize",
        })
    }
    pub const fn config(&self) -> AuthorityConfig {
        self.config
    }
    pub fn config_version(&self) -> ConfigVersion {
        ConfigVersion::new(self.shared.capacity_revision.load(Ordering::Acquire))
    }
    pub const fn root(&self) -> &Account {
        &self.root
    }
    pub fn capacity_bytes(&self) -> u64 {
        self.shared.target.load(Ordering::Acquire)
    }
    pub const fn headroom_budget_bytes(&self) -> u64 {
        self.config.headroom_budget_bytes
    }
    pub const fn process_bound_bytes(&self) -> u64 {
        self.config.process_bound_bytes
    }
    pub fn live_accounts(&self) -> u32 {
        self.shared
            .account_list()
            .iter()
            .filter(|a| !a.is_retired())
            .count() as u32
    }
    pub fn create_account(
        &self,
        kind: AccountKind,
        external: ExternalRef,
    ) -> Result<Account, CapacityError> {
        self.root.create_child(kind, external)
    }
    /// Precommit the floor before admitting work. Duplicate or late assembly
    /// is an error; a policy number alone never backs control traffic.
    pub fn install_control_branch(&self, bytes: u64) -> Result<&Account, CapacityError> {
        let _assembly = self.shared.assembly.lock().unwrap();
        if self.control.get().is_some() || self.live_accounts() != 1 {
            return Err(CapacityError::Invalid {
                detail: "control floor must be installed exactly once before work",
            });
        }
        let branch = self
            .root
            .create_child_unlocked(AccountKind::Service, ExternalRef::NONE)?;
        branch.prefund(bytes)?;
        branch.install_policy(bytes, LimitDimension::Service);
        {
            let _gate = self.root.0.gate.write().unwrap();
            branch.0.ledger.lock().unwrap().floor = bytes;
            branch.0.control.store(1, Ordering::Release);
            self.root.0.ledger.lock().unwrap().floor = bytes;
        }
        self.control
            .set(branch)
            .map_err(|_| CapacityError::Invalid {
                detail: "control floor already installed",
            })?;
        Ok(self.control.get().unwrap())
    }
    pub fn control_branch(&self) -> Option<&Account> {
        self.control.get()
    }
    pub fn take_capacity_writer(&self) -> Result<CapacityWriter, CapacityError> {
        let mut taken = self.shared.writer_taken.lock().unwrap();
        if *taken {
            return Err(CapacityError::Invalid {
                detail: "capacity writer already issued",
            });
        }
        *taken = true;
        Ok(CapacityWriter {
            root: self.root.clone(),
        })
    }
    pub fn snapshot(&self) -> AuthoritySnapshot {
        // The root gate pairs target/revision with the root commitment read.
        let root = self.root.snapshot();
        let live_accounts = root.live_accounts;
        AuthoritySnapshot {
            root,
            capacity_bytes: root.capacity_target,
            headroom_budget_bytes: self.config.headroom_budget_bytes,
            process_bound_bytes: self.config.process_bound_bytes,
            config_version: ConfigVersion::new(root.capacity_revision),
            live_accounts,
        }
    }
}
#[derive(Debug)]
pub struct CapacityWriter {
    root: Account,
}
impl CapacityWriter {
    pub fn set_capacity(&mut self, target: u64) -> Result<u64, CapacityError> {
        let shared = &self.root.0.shared;
        if target > shared.ceiling {
            return Err(CapacityError::Invalid {
                detail: "capacity target exceeds configured ceiling",
            });
        }
        let _gate = self.root.0.gate.write().unwrap();
        if shared.target.load(Ordering::Acquire) == target {
            return Ok(shared.capacity_revision.load(Ordering::Acquire));
        }
        let previous = shared.target.load(Ordering::Acquire);
        shared.target.store(target, Ordering::Release);
        let revision = shared.capacity_revision.fetch_add(1, Ordering::AcqRel) + 1;
        drop(_gate);
        if target < previous {
            crate::maintenance::request(
                &self.root.0.shared,
                crate::maintenance::MaintenanceReason::CapacityReduced,
            );
        }
        Ok(revision)
    }
}

impl Shared {
    pub(crate) fn next_identity(&self) -> Result<u64, CapacityError> {
        self.next_id
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |id| id.checked_add(1))
            .map_err(|_| CapacityError::Invalid {
                detail: "memory identity space exhausted",
            })
    }
    pub(crate) fn account_list(&self) -> Vec<Account> {
        let mut list = Vec::with_capacity(self.max_accounts as usize);
        let registry = self.accounts.lock().unwrap();
        list.extend(
            registry
                .records
                .iter()
                .filter_map(|a| a.as_ref().and_then(|a| a.upgrade()).map(Account)),
        );
        list
    }
}

#[derive(Debug)]
pub(crate) struct AccountRegistry {
    pub records: Vec<Option<Weak<AccountState>>>,
    pub free_slots: Vec<usize>,
    pub upper: usize,
}

impl Drop for MemoryAuthority {
    fn drop(&mut self) {
        // Close first so external lane handles cannot activate or refill while
        // their empty registry publication is detached. This produces no
        // teardown receipt and does not revoke an active scope's real facts.
        self.root.close_to_growth();
        self.detach_empty_records();
        // Tokens retain access through the process store, not funding cycles.
        let detached = {
            let mut registry = self.shared.domains.lock().unwrap();
            let records = std::mem::take(&mut registry.records);
            registry.active = 0;
            registry.metadata = 0;
            registry.upper = 0;
            records
        };
        drop(detached);
        let observations = {
            let mut registry = self.shared.observation_lanes.lock().unwrap();
            registry.upper = 0;
            registry.occupied = 0;
            std::mem::take(&mut registry.records)
        };
        drop(observations);
        self.shared.record_store.store().reclaim(usize::MAX);
    }
}
impl MemoryAuthority {
    fn detach_empty_records(&self) {
        let upper = self.shared.domains.lock().unwrap().upper;
        for index in 0..upper {
            let record = self.shared.domains.lock().unwrap().records[index].clone();
            let Some(record) = record else { continue };
            let state = record.state.lock().unwrap();
            if state.active || state.external_handles != 0 || !record.lane.reclaimable() {
                continue;
            }
            let detached = {
                let mut registry = self.shared.domains.lock().unwrap();
                if !registry.records[index]
                    .as_ref()
                    .is_some_and(|published| Arc::ptr_eq(published, &record))
                {
                    continue;
                }
                let detached = registry.records[index].take();
                registry.free_slots.push(index);
                registry.metadata -= record.metadata;
                if state.registered_active {
                    registry.active -= 1;
                }
                detached
            };
            // Destruction may release an affiliation and acquire ancestor
            // gates. Drop outside both the lane and publication locks. An
            // external lane handle still owns its sealed storage, if present.
            drop(state);
            drop(detached);
            drop(record);
        }
    }
}

#[cfg(all(test, not(loom)))]
mod lifetime_tests {
    use super::*;

    fn authority() -> MemoryAuthority {
        let mut config = AuthorityConfig::new(16_384, 16_384, 0);
        config.max_accounts = 3;
        config.max_active_owners = 2;
        config.metadata_budget_bytes =
            2 * (crate::domain::OWNER_METADATA_BYTES + 2 * std::mem::size_of::<usize>() as u64);
        MemoryAuthority::new(config).unwrap()
    }

    #[test]
    fn shutdown_preserves_external_publication_rights_including_zero_bytes() {
        for bytes in [0, 64] {
            let authority = authority();
            let domain = authority.root().create_domain(bytes).unwrap();
            let mut bound = domain.external_bound(bytes).unwrap();
            let record = Arc::downgrade(&domain.0);
            drop(domain);
            authority.root.close_to_growth();
            authority.detach_empty_records();
            let origin = bound.convert_to_live(bytes).unwrap();
            drop(bound);
            authority.detach_empty_records();
            assert!(record.upgrade().is_some());
            // SAFETY: this exact converted allocation is outstanding and its
            // storage has been released; the authority remains alive.
            unsafe { origin.record_deallocation(bytes) };
            authority.detach_empty_records();
            assert!(record.upgrade().is_none());
        }
    }

    #[test]
    fn exhausted_identity_source_refuses_without_wrapping_or_charging_storage() {
        let a = authority();
        a.shared.next_id.store(u64::MAX, Ordering::Relaxed);
        let before = a.root().committed_bytes();
        assert!(matches!(
            a.create_account(AccountKind::Work, ExternalRef::NONE),
            Err(CapacityError::Invalid { .. })
        ));
        assert!(matches!(
            a.root().create_domain(1),
            Err(CapacityError::Invalid { .. })
        ));
        assert_eq!(a.shared.next_id.load(Ordering::Relaxed), u64::MAX);
        assert_eq!(a.root().committed_bytes(), before);
    }
    #[test]
    fn dropping_authority_detaches_unused_child_lane_cycles() {
        let authority = authority();
        let shared = Arc::downgrade(&authority.shared);
        let root = Arc::downgrade(&authority.root.0);
        let account = authority
            .create_account(AccountKind::Work, ExternalRef::NONE)
            .unwrap();
        let weak_account = Arc::downgrade(&account.0);
        let domain = account.create_domain(1).unwrap();
        drop(domain);
        drop(account);
        drop(authority);

        assert!(weak_account.upgrade().is_none());
        assert!(root.upgrade().is_none());
        assert!(shared.upgrade().is_none());
    }

    #[test]
    fn dropping_authority_detaches_unused_root_lane_cycles() {
        let authority = authority();
        let shared = Arc::downgrade(&authority.shared);
        let root = Arc::downgrade(&authority.root.0);
        let domain = authority.root().create_domain(1).unwrap();
        drop(domain);
        drop(authority);

        assert!(root.upgrade().is_none());
        assert!(shared.upgrade().is_none());
    }

    #[test]
    fn externally_held_lane_is_sealed_and_releases_storage_after_shutdown() {
        let authority = authority();
        let shared = Arc::downgrade(&authority.shared);
        let account = authority
            .create_account(AccountKind::Work, ExternalRef::NONE)
            .unwrap();
        let domain = account.create_domain(1).unwrap();
        drop(authority);

        assert!(domain.snapshot().sealed);
        assert!(domain.activate(0, 0).is_err());
        assert!(domain.refill(1).is_err());
        assert!(account.prefund(1).is_err());
        assert!(shared.upgrade().is_some());
        drop(domain);
        drop(account);
        assert!(shared.upgrade().is_none());
    }
}
