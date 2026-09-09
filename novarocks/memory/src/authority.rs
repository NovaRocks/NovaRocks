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

//! The process capacity authority (MEM-1 wave-1 T02).
//!
//! One operating-system process installs exactly one authority. It validates
//! that managed capacity `B` plus the headroom budget `H` fits inside the
//! process bound `P`, owns the root account, and hands out account handles.
//! Roles inside the same process consume the injected authority; they never
//! create a second one over the same physical budget, and they never sample
//! the same resident memory twice and add the results.
//!
//! # `B`, `H` and `P`
//!
//! `P` is what the process is allowed to use, resolved from the deployment's
//! own memory configuration. `B` is the part of it this authority governs
//! hard: every managed commitment is kept at or under `B`. `H` is the part
//! reserved for everything the hard tier cannot cover — a third party's
//! internals, a native library, allocator retention and fragmentation. They
//! partition `P`, so `B + H <= P`, and a configuration that breaks that is
//! rejected rather than quietly promising capacity the process does not have.
//!
//! `H` is a policy number, not a measurement and not a residual: it is what
//! the deployment set aside, and the observation tier measures actual usage
//! against it. The difference between the two is a diagnostic, never an
//! attribution to a query.

use std::sync::Arc;

use crate::account::{AccountHandle, AccountTreeShared, TopUpPolicy};
use crate::error::{CapacityError, ConfigError, MetadataRegistryLabel};
use crate::ids::{ConfigVersion, ExternalRef};
use crate::snapshot::{AuthoritySnapshot, EventRing};

/// How one process's memory authority is sized.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AuthorityConfig {
    /// `P`: what the whole process may use.
    pub process_bound_bytes: u64,
    /// `B`: the part this authority governs hard.
    pub capacity_bytes: u64,
    /// `H`: the part set aside for allocations outside hard governance.
    pub headroom_budget_bytes: u64,
    /// The account-tree limit. The core's own metadata is budgeted like any
    /// other growing state.
    pub max_accounts: u32,
    /// How many observation events are buffered before the oldest is dropped.
    pub event_capacity: u32,
    /// How much capacity a top-up moves at once.
    pub top_up: TopUpPolicy,
}

impl AuthorityConfig {
    /// Builds a configuration that splits `process_bound_bytes` into managed
    /// capacity and headroom, with the default metadata limits.
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
            top_up: TopUpPolicy::DEFAULT,
        }
    }

    /// Checks that the configuration is internally possible.
    ///
    /// This is the one place the `B + H <= P` partition is enforced. It is a
    /// configuration invariant rather than a runtime check because a process
    /// that promised more than it has cannot discover that safely later.
    pub const fn validate(&self) -> Result<(), ConfigError> {
        if self.process_bound_bytes == 0 {
            return Err(ConfigError::ProcessBoundIsZero);
        }
        if self.max_accounts == 0 {
            return Err(ConfigError::MetadataLimitIsZero {
                registry: MetadataRegistryLabel::Accounts,
            });
        }
        if self.event_capacity == 0 {
            return Err(ConfigError::MetadataLimitIsZero {
                registry: MetadataRegistryLabel::Events,
            });
        }
        let declared = match self.capacity_bytes.checked_add(self.headroom_budget_bytes) {
            Some(declared) => declared,
            None => {
                return Err(ConfigError::CapacityExceedsProcessBound {
                    capacity_bytes: self.capacity_bytes,
                    headroom_budget_bytes: self.headroom_budget_bytes,
                    process_bound_bytes: self.process_bound_bytes,
                });
            }
        };
        if declared > self.process_bound_bytes {
            return Err(ConfigError::CapacityExceedsProcessBound {
                capacity_bytes: self.capacity_bytes,
                headroom_budget_bytes: self.headroom_budget_bytes,
                process_bound_bytes: self.process_bound_bytes,
            });
        }
        Ok(())
    }
}

/// One process's memory authority.
///
/// The authority is the trusted assembly point: whoever composes the process
/// creates it once and injects account handles into the roles that need them.
/// A handle grants the right to derive children and request capacity, and
/// nothing more, so a consumer cannot mint a second authority over the same
/// physical budget by constructing types it happens to be able to name.
#[derive(Debug)]
pub struct MemoryAuthority {
    config: AuthorityConfig,
    config_version: ConfigVersion,
    root: AccountHandle,
    shared: Arc<AccountTreeShared>,
}

impl MemoryAuthority {
    /// Installs the authority for this process.
    pub fn new(config: AuthorityConfig) -> Result<Self, ConfigError> {
        config.validate()?;
        let shared = Arc::new(AccountTreeShared::new(
            config.capacity_bytes,
            config.max_accounts,
            config.top_up,
            config.event_capacity,
        ));
        let root =
            AccountHandle::new_root(ExternalRef::NONE, Arc::clone(&shared)).map_err(|_| {
                // The only way the root can be refused is a zero account budget,
                // which `validate` already rejected.
                ConfigError::MetadataLimitIsZero {
                    registry: MetadataRegistryLabel::Accounts,
                }
            })?;
        Ok(Self {
            config,
            config_version: ConfigVersion::new(1),
            root,
            shared,
        })
    }

    /// Returns the configuration this authority was installed with.
    pub const fn config(&self) -> AuthorityConfig {
        self.config
    }

    /// Returns the configuration version snapshots are stamped with.
    pub const fn config_version(&self) -> ConfigVersion {
        self.config_version
    }

    /// Returns the process root account.
    pub const fn root(&self) -> &AccountHandle {
        &self.root
    }

    /// Returns managed capacity `B`.
    pub const fn capacity_bytes(&self) -> u64 {
        self.config.capacity_bytes
    }

    /// Returns the headroom budget `H`.
    ///
    /// This is what the deployment set aside for allocations the hard tier
    /// does not cover. It is not the measured size of those allocations, and
    /// it is not `A_rust - B`.
    pub const fn headroom_budget_bytes(&self) -> u64 {
        self.config.headroom_budget_bytes
    }

    /// Returns the process bound `P`.
    pub const fn process_bound_bytes(&self) -> u64 {
        self.config.process_bound_bytes
    }

    /// Returns the observation event ring.
    pub fn events(&self) -> &EventRing {
        self.shared.events()
    }

    /// Returns the number of live accounts.
    pub fn live_accounts(&self) -> u32 {
        self.shared.live_accounts()
    }

    /// Reads the authority's own facts.
    ///
    /// The root's `L`, `F` and `O` are summed over the tree at read time, so
    /// this describes a short period rather than one instant. It is stamped
    /// with the configuration version so a consumer can tell a stale reading
    /// from a configuration change.
    pub fn snapshot(&self) -> AuthoritySnapshot {
        AuthoritySnapshot {
            root: self.root.snapshot(),
            capacity_bytes: self.config.capacity_bytes,
            headroom_budget_bytes: self.config.headroom_budget_bytes,
            process_bound_bytes: self.config.process_bound_bytes,
            config_version: self.config_version,
            live_accounts: self.shared.live_accounts(),
        }
    }

    /// Derives an account directly under the process root.
    pub fn create_account(
        &self,
        kind: crate::ids::AccountKind,
        external: ExternalRef,
    ) -> Result<AccountHandle, CapacityError> {
        self.root.create_child(kind, external)
    }
}
