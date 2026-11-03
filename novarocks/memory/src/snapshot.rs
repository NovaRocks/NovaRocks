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

//! Versioned accounting observations and separately qualified live samples.
//!
//! Ledger commitment and responsibility classification use one root gate.
//! Hooks remain local and may publish newer live facts during observation;
//! those facts are reported as samples, never as settled capacity or teardown.

use crate::ids::{AccountId, AccountKind, ConfigVersion, PolicyVersion};
use crate::sync::{Arc, Ordering};
use crate::{Account, account::Path, domain::Domain};
use std::time::Instant;

/// One account's ledger facts and the quality of its live observation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AccountSnapshot {
    pub account: AccountId,
    pub kind: AccountKind,
    /// Last incorporated payload L plus separately disclosed metadata.
    /// This is not a simultaneous measurement of outstanding allocations.
    pub live_bytes: u64,
    pub granted_bytes: u64,
    pub bounded_bytes: u64,
    /// Authoritative ledger C, independent of live sampling quality.
    pub committed_bytes: u64,
    /// Protected part of existing C, never an additional charge.
    pub floor_bytes: u64,
    pub policy_limit_bytes: Option<u64>,
    pub excess_bytes: u64,
    /// Ordinary growth is closed by this account or an ancestor constraint.
    pub growth_frozen: bool,
    /// The account's actual maintained C peak, not a sum of child peaks.
    pub peak_committed_bytes: u64,
    pub policy_version: PolicyVersion,
    pub ledger_revision: u64,
    pub capacity_revision: u64,
    pub capacity_target: u64,
    /// Prepaid stable-record metadata and, at root, shared index backing.
    pub metadata_bytes: u64,
    pub storage_metadata_bytes: u64,
    pub settled_payload_live_bytes: u64,
    /// Outstanding payload observed during the sampling interval. Hooks can
    /// change it independently of the gate protecting accounting membership.
    pub sampled_payload_live_bytes: u64,
    pub account_slack_bytes: u64,
    pub subtree_slack_bytes: u64,
    pub active_scopes: u64,
    /// Samples whose current live value differs from incorporated L.
    pub dirty_domains: u64,
    /// Samples whose publication sequence changed across the live read.
    pub changing_live_samples: u64,
    /// Sum of each independent domain's incorporated E, without netting F.
    pub settled_debt_bytes: u64,
    pub sampled_debt_bytes: u64,
    pub live_accounts: u32,
    /// False when a final account Drop has not yet returned disappeared
    /// membership's slack. The bounded observer never waits for quiescence.
    pub classification_complete: bool,
    pub sampling_span_ns: u64,
}

impl AccountSnapshot {
    /// A decomposition check, not proof of snapshot/live or teardown quality.
    pub const fn is_internally_consistent(&self) -> bool {
        match self.live_bytes.checked_add(self.granted_bytes) {
            Some(partial) => match partial.checked_add(self.bounded_bytes) {
                Some(total) => total == self.committed_bytes,
                None => false,
            },
            None => false,
        }
    }
    pub const fn policy_remaining_bytes(&self) -> Option<u64> {
        match self.policy_limit_bytes {
            Some(limit) => Some(limit.saturating_sub(self.committed_bytes)),
            None => None,
        }
    }
}

/// Process responsibility and observation coverage are overlapping views.
/// Allocation/RSS observations must never be added to ledger commitment.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AuthoritySnapshot {
    pub root: AccountSnapshot,
    pub capacity_bytes: u64,
    pub headroom_budget_bytes: u64,
    pub process_bound_bytes: u64,
    pub config_version: ConfigVersion,
    pub live_accounts: u32,
}
impl AuthoritySnapshot {
    pub const fn capacity_remaining_bytes(&self) -> u64 {
        self.capacity_bytes
            .saturating_sub(self.root.committed_bytes)
    }
    /// Capacity may legitimately be below existing C after shrink/debt.
    pub const fn honours_capacity_bound(&self) -> bool {
        self.root.committed_bytes <= self.capacity_bytes
    }
    pub const fn decomposed_committed_bytes(&self) -> u64 {
        self.root
            .live_bytes
            .saturating_add(self.root.granted_bytes)
            .saturating_add(self.root.bounded_bytes)
    }
}

/// Disjoint classifications of one accounting version. Root commitment retains
/// residual liability; query pressure follows only executing Work accounts. Consumers must
/// reject incomplete classification before using query pressure for policy.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PressureProjection {
    pub root_committed: u64,
    pub query_committed: u64,
    pub residual_committed: u64,
    pub residual_query_committed: u64,
    pub residual_metadata: u64,
    pub storage_metadata: u64,
    pub active_metadata: u64,
    pub account_slack: u64,
    pub active_scopes: u64,
    /// Inactive rights based on settled facts; active hook samples are separate.
    pub settled_idle_authorization: u64,
    pub pending_drain_domains: u64,
    pub dirty_domains: u64,
    pub sampled_payload_live: u64,
    pub settled_payload_live: u64,
    pub changing_live_samples: u64,
    pub settled_debt: u64,
    pub sampled_debt: u64,
    pub classified_committed: u64,
    pub unclassified_committed: u64,
    pub classification_complete: bool,
    pub root_revision: u64,
    pub capacity_target: u64,
    pub capacity_revision: u64,
    pub control_floor: u64,
    pub elastic_capacity: u64,
    pub elastic_committed: u64,
    pub elastic_excess: u64,
    pub floor_target_gap: u64,
}
impl PressureProjection {
    pub fn query_pressure(&self) -> u64 {
        self.query_committed
    }
    pub fn non_evictable(&self, evictable_cache: u64) -> Option<u64> {
        self.root_committed.checked_sub(evictable_cache)
    }
    /// Policy-facing pressure refuses an incomplete responsibility sample.
    pub fn complete_query_pressure(&self) -> Option<u64> {
        self.classification_complete.then(|| self.query_pressure())
    }
}

// Capacity is fixed at assembly. Reserve pin buffers before acquiring the
// root gate; filling them under the gate cannot allocate. Pins are declared
// outside the gate's scope, so a final account/domain drop always occurs after
// gate release and cannot recursively acquire its own observation gate.
struct ObservationMembers {
    accounts: Vec<Account>,
    domains: Vec<Arc<Domain>>,
}
impl ObservationMembers {
    fn reserve(account: &Account) -> Self {
        let domain_capacity = account.0.shared.domains.lock().unwrap().records.len();
        Self {
            accounts: Vec::with_capacity(account.0.shared.max_accounts as usize),
            domains: Vec::with_capacity(domain_capacity),
        }
    }
    fn capture(&mut self, account: &Account) {
        let shared = &account.0.shared;
        {
            let registry = shared.accounts.lock().unwrap();
            self.accounts.extend(
                registry
                    .records
                    .iter()
                    .take(registry.upper)
                    .filter_map(|r| r.as_ref().and_then(|r| r.upgrade()).map(Account)),
            );
        }
        {
            let registry = shared.domains.lock().unwrap();
            self.domains.extend(
                registry
                    .records
                    .iter()
                    .take(registry.upper)
                    .filter_map(Clone::clone),
            );
        }
    }
    fn clear(&mut self) {
        self.domains.clear();
        self.accounts.clear();
    }
}

pub(crate) fn account_snapshot(account: &Account) -> AccountSnapshot {
    capture(account).0
}

fn capture(account: &Account) -> (AccountSnapshot, PressureProjection) {
    let started = Instant::now();
    let mut members = ObservationMembers::reserve(account);
    let path = Path::new(account);
    let root = path.node(path.len - 1);
    // An account whose last Arc died can disappear from the weak registry
    // before its Drop obtains the gate to return slack. Bounded retries release
    // all gates and pins, allowing that drop to finish; sustained churn is
    // reported as incomplete instead of hanging a management request.
    for attempt in 0..3 {
        members.clear();
        let observation = {
            let _gate = root.0.gate.write().unwrap();
            members.capture(account);
            capture_locked(account, root, &members, started)
        };
        if observation.1.classification_complete || attempt == 2 {
            return observation;
        }
    }
    unreachable!("bounded observation always returns its final sample")
}

fn capture_locked(
    account: &Account,
    root: &Account,
    members: &ObservationMembers,
    started: Instant,
) -> (AccountSnapshot, PressureProjection) {
    let shared = &account.0.shared;
    let storage = shared.storage_bytes.load(Ordering::Acquire);
    let mut pressure = PressureProjection {
        root_committed: 0,
        query_committed: 0,
        residual_committed: 0,
        residual_query_committed: 0,
        residual_metadata: 0,
        storage_metadata: storage,
        active_metadata: 0,
        account_slack: 0,
        active_scopes: 0,
        settled_idle_authorization: 0,
        pending_drain_domains: 0,
        dirty_domains: 0,
        sampled_payload_live: 0,
        settled_payload_live: 0,
        changing_live_samples: 0,
        settled_debt: 0,
        sampled_debt: 0,
        classified_committed: storage,
        unclassified_committed: 0,
        classification_complete: false,
        root_revision: 0,
        capacity_target: 0,
        capacity_revision: 0,
        control_floor: 0,
        elastic_capacity: 0,
        elastic_committed: 0,
        elastic_excess: 0,
        floor_target_gap: 0,
    };
    let scoped_storage = if account.id() == root.id() {
        storage
    } else {
        0
    };
    let mut metadata = scoped_storage;
    let mut settled_live = 0;
    let mut sampled_live = 0;
    let mut free = 0;
    let mut external = 0;
    let mut slack = 0;
    let mut scopes = 0;
    let mut dirty = 0;
    let mut changing = 0;
    let mut settled_debt = 0;
    let mut sampled_debt = 0;
    let mut live_accounts = 0;
    for member in &members.accounts {
        let state = member.0.ledger.lock().unwrap();
        pressure.account_slack += state.slack;
        pressure.classified_committed += state.slack;
        if Path::new(member)
            .nodes
            .iter()
            .flatten()
            .any(|n| n.kind() == AccountKind::Work)
        {
            pressure.query_committed += state.slack;
        }
        if member.is_descendant_of(account) {
            free += state.slack;
            slack += state.slack;
            if !state.retired {
                live_accounts += 1;
            }
        }
    }
    for domain in &members.domains {
        let state = domain.state.lock().unwrap();
        let total = state.committed + domain.metadata;
        pressure.classified_committed += total;
        if domain.lane.responsibility_class() == crate::lane::ResponsibilityClass::Residual {
            pressure.residual_committed += total;
            pressure.residual_metadata += domain.metadata;
            if domain.lane.0.query_origin {
                pressure.residual_query_committed += total;
            }
        } else {
            pressure.active_metadata += domain.metadata;
            if domain.lane.responsibility_class() == crate::lane::ResponsibilityClass::Query {
                pressure.query_committed += total;
            }
        }
        let before = domain.lane.record().sequence.load(Ordering::Acquire);
        let live = domain.lane.live_bytes();
        let after = domain.lane.record().sequence.load(Ordering::Acquire);
        let current_obligation = live.saturating_add(state.external);
        let domain_settled_debt = state.committed.saturating_sub(state.authorized);
        let domain_sampled_debt = current_obligation.saturating_sub(state.authorized);
        let is_dirty = u64::from(
            live != state.settled_live
                || state.authorized.max(current_obligation) != state.committed,
        );
        let is_changing = u64::from(before != after);
        pressure.sampled_payload_live += live;
        pressure.settled_payload_live += state.settled_live;
        pressure.active_scopes += u64::from(state.active);
        if !state.active {
            pressure.settled_idle_authorization += state
                .authorized
                .saturating_sub(state.settled_live.saturating_add(state.external));
        }
        pressure.pending_drain_domains += u64::from(state.drain_requested);
        pressure.dirty_domains += is_dirty;
        pressure.changing_live_samples += is_changing;
        pressure.settled_debt += domain_settled_debt;
        pressure.sampled_debt += domain_sampled_debt;
        if domain.lane.affiliation().is_descendant_of(account) {
            metadata += domain.metadata;
            settled_live += state.settled_live;
            sampled_live += live;
            external += state.external;
            free += state
                .authorized
                .saturating_sub(state.settled_live.saturating_add(state.external));
            scopes += u64::from(state.active);
            dirty += is_dirty;
            changing += is_changing;
            settled_debt += domain_settled_debt;
            sampled_debt += domain_sampled_debt;
        }
    }
    {
        let state = root.0.ledger.lock().unwrap();
        pressure.root_committed = state.committed;
        pressure.root_revision = state.revision;
        pressure.control_floor = state.floor;
    }
    pressure.capacity_target = shared.target.load(Ordering::Acquire);
    pressure.capacity_revision = shared.capacity_revision.load(Ordering::Acquire);
    pressure.elastic_capacity = pressure
        .capacity_target
        .saturating_sub(pressure.control_floor);
    pressure.elastic_committed = pressure
        .root_committed
        .saturating_sub(pressure.control_floor);
    pressure.elastic_excess = pressure
        .elastic_committed
        .saturating_sub(pressure.elastic_capacity);
    pressure.floor_target_gap = pressure
        .control_floor
        .saturating_sub(pressure.capacity_target);
    pressure.unclassified_committed = pressure
        .root_committed
        .saturating_sub(pressure.classified_committed);
    pressure.classification_complete = pressure.root_committed == pressure.classified_committed;
    let path = Path::new(account);
    let growth_frozen = path.nodes.iter().flatten().any(|node| {
        let state = node.0.ledger.lock().unwrap();
        let limit = if node.id() == root.id() {
            pressure.capacity_target
        } else {
            state.policy.map_or(u64::MAX, |p| p.limit_bytes())
        };
        state.closed || state.committed > limit
    });
    let state = account.0.ledger.lock().unwrap();
    let limit = if account.id() == root.id() {
        pressure.capacity_target
    } else {
        state.policy.map_or(u64::MAX, |p| p.limit_bytes())
    };
    let snapshot = AccountSnapshot {
        account: account.id(),
        kind: account.kind(),
        live_bytes: settled_live + metadata,
        granted_bytes: free,
        bounded_bytes: external,
        committed_bytes: state.committed,
        floor_bytes: state.floor,
        policy_limit_bytes: state.policy.map(|p| p.limit_bytes()),
        excess_bytes: state.committed.saturating_sub(limit),
        growth_frozen,
        peak_committed_bytes: state.peak,
        policy_version: state.version,
        ledger_revision: state.revision,
        capacity_revision: pressure.capacity_revision,
        capacity_target: pressure.capacity_target,
        metadata_bytes: metadata,
        storage_metadata_bytes: scoped_storage,
        settled_payload_live_bytes: settled_live,
        sampled_payload_live_bytes: sampled_live,
        account_slack_bytes: state.slack,
        subtree_slack_bytes: slack,
        active_scopes: scopes,
        dirty_domains: dirty,
        changing_live_samples: changing,
        settled_debt_bytes: settled_debt,
        sampled_debt_bytes: sampled_debt,
        live_accounts,
        classification_complete: pressure.classification_complete,
        sampling_span_ns: started.elapsed().as_nanos().min(u64::MAX as u128) as u64,
    };
    (snapshot, pressure)
}

impl crate::MemoryAuthority {
    pub fn pressure_projection(&self) -> PressureProjection {
        capture(self.root()).1
    }

    /// Root ledger and disjoint pressure classifications from one capture.
    /// Member pins and all temporary allocations are released outside gates.
    pub fn accounting_snapshot(&self) -> (AuthoritySnapshot, PressureProjection) {
        let (root, pressure) = capture(self.root());
        let config = self.config();
        let authority = AuthoritySnapshot {
            root,
            capacity_bytes: root.capacity_target,
            headroom_budget_bytes: config.headroom_budget_bytes,
            process_bound_bytes: config.process_bound_bytes,
            config_version: ConfigVersion::new(root.capacity_revision),
            live_accounts: root.live_accounts,
        };
        (authority, pressure)
    }
}

#[cfg(all(test, not(loom)))]
mod tests {
    use super::*;
    use crate::{
        AuthorityConfig, ExternalRef, MemoryAuthority, OWNER_METADATA_BYTES, TeardownEvidence,
        TopUpPolicy,
    };
    use std::sync::{Arc as StdArc, Barrier};

    fn authority() -> MemoryAuthority {
        let mut config = AuthorityConfig::new(131_072, 65_536, 65_536);
        config.max_accounts = 16;
        config.max_active_owners = 4;
        config.metadata_budget_bytes = 8_192;
        config.top_up = TopUpPolicy::uniform(1);
        MemoryAuthority::new(config).unwrap()
    }

    #[test]
    fn members_created_after_buffer_reservation_are_in_the_committed_capture() {
        let a = authority();
        let mut members = ObservationMembers::reserve(a.root());
        let query = a
            .create_account(AccountKind::Work, ExternalRef::NONE)
            .unwrap();
        let domain = query.create_domain(512).unwrap();
        let (snapshot, pressure) = {
            let _gate = a.root().0.gate.write().unwrap();
            members.capture(a.root());
            capture_locked(a.root(), a.root(), &members, Instant::now())
        };
        assert!(snapshot.classification_complete);
        assert_eq!(
            pressure.complete_query_pressure(),
            Some(512 + OWNER_METADATA_BYTES)
        );
        assert_eq!(pressure.root_committed, pressure.classified_committed);
        assert_eq!(
            pressure.root_committed,
            pressure.storage_metadata + pressure.query_committed
        );
        assert_eq!(snapshot.live_accounts, 2);
        assert_eq!(
            snapshot.metadata_bytes,
            pressure.storage_metadata + OWNER_METADATA_BYTES
        );
        assert_eq!(domain.snapshot().authorized, 512);
    }

    #[test]
    fn outstanding_hooks_are_samples_and_never_disguised_as_settled_live() {
        let a = authority();
        let query = a
            .create_account(AccountKind::Work, ExternalRef::NONE)
            .unwrap();
        let domain = query.create_domain(512).unwrap();
        let mut scope = domain.activate(512, 0).unwrap();
        let origin = scope.record_allocation(256);
        let (before, pressure) = a.accounting_snapshot();
        assert_eq!(before.root.settled_payload_live_bytes, 0);
        assert_eq!(before.root.sampled_payload_live_bytes, 256);
        assert_eq!(before.root.active_scopes, 1);
        assert_eq!(before.root.dirty_domains, 1);
        assert_eq!(before.root.live_bytes, before.root.metadata_bytes);
        assert_eq!(pressure.sampled_payload_live, 256);
        assert_eq!(pressure.settled_payload_live, 0);
        assert!(before.root.is_internally_consistent());
        scope.finish();
        let settled = query.snapshot();
        assert_eq!(settled.settled_payload_live_bytes, 256);
        assert_eq!(settled.sampled_payload_live_bytes, 256);
        assert_eq!(settled.active_scopes, 0);
        assert_eq!(settled.dirty_domains, 0);
        // SAFETY: one matching release for the outstanding published allocation.
        unsafe {
            origin.record_deallocation(256);
        }
        let dirty = query.snapshot();
        assert_eq!(dirty.settled_payload_live_bytes, 256);
        assert_eq!(dirty.sampled_payload_live_bytes, 0);
        assert_eq!(dirty.dirty_domains, 1);
        assert_eq!(dirty.peak_committed_bytes, settled.peak_committed_bytes);
        domain.settle();
        let freed = query.snapshot();
        assert_eq!(freed.settled_payload_live_bytes, 0);
        assert_eq!(freed.dirty_domains, 0);
        assert_eq!(freed.peak_committed_bytes, settled.peak_committed_bytes);
    }

    #[test]
    fn debt_samples_and_settled_debt_preserve_each_domains_independent_rights() {
        let a = authority();
        let query = a
            .create_account(AccountKind::Work, ExternalRef::NONE)
            .unwrap();
        let debtor = query.create_domain(512).unwrap();
        let untouched = query.create_domain(512).unwrap();
        let mut scope = debtor.activate(512, 0).unwrap();
        let origin = scope.record_allocation(768);
        let sampled = query.snapshot();
        assert_eq!(sampled.sampled_debt_bytes, 256);
        assert_eq!(sampled.settled_debt_bytes, 0);
        assert_eq!(sampled.dirty_domains, 1);
        scope.finish();
        let (snapshot, pressure) = a.accounting_snapshot();
        assert_eq!(snapshot.root.sampled_debt_bytes, 256);
        assert_eq!(snapshot.root.settled_debt_bytes, 256);
        assert_eq!(pressure.settled_debt, 256);
        assert_eq!(pressure.sampled_debt, 256);
        assert_eq!(untouched.snapshot().free, 512);
        assert_eq!(
            pressure.complete_query_pressure(),
            Some(1_280 + 2 * OWNER_METADATA_BYTES)
        );
        // SAFETY: one exact release for the published successful allocation.
        unsafe {
            origin.record_deallocation(768);
        }
        let freed = query.snapshot();
        assert_eq!(freed.settled_debt_bytes, 256);
        assert_eq!(freed.sampled_debt_bytes, 0);
        assert_eq!(freed.dirty_domains, 1);
        debtor.settle();
        let reconciled = query.snapshot();
        assert_eq!(reconciled.settled_debt_bytes, 0);
        assert_eq!(reconciled.dirty_domains, 0);
    }

    #[test]
    fn target_revision_floor_and_elastic_pressure_come_from_the_same_capture() {
        let a = authority();
        a.install_control_branch(4_096).unwrap();
        let query = a
            .create_account(AccountKind::Work, ExternalRef::NONE)
            .unwrap();
        let _domain = query.create_domain(1_024).unwrap();
        let mut writer = a.take_capacity_writer().unwrap();
        let revision = writer.set_capacity(0).unwrap();
        let (authority, pressure) = a.accounting_snapshot();
        assert_eq!(authority.capacity_bytes, 0);
        assert_eq!(authority.root.capacity_revision, revision);
        assert_eq!(pressure.capacity_revision, revision);
        assert_eq!(authority.root.ledger_revision, pressure.root_revision);
        assert_eq!(pressure.control_floor, 4_096);
        assert_eq!(pressure.floor_target_gap, 4_096);
        assert_eq!(pressure.elastic_capacity, 0);
        assert_eq!(pressure.elastic_committed, pressure.root_committed - 4_096);
        assert_eq!(pressure.elastic_excess, pressure.elastic_committed);
        assert_eq!(authority.root.excess_bytes, pressure.root_committed);
        assert!(pressure.classification_complete);
        assert!(
            query.snapshot().growth_frozen,
            "ancestor shrink closes ordinary growth"
        );
    }

    #[test]
    fn final_drop_waiting_on_an_ancestor_reports_incomplete_without_hanging_observer() {
        let a = authority();
        let group = a
            .create_account(AccountKind::ResourceGroup, ExternalRef::NONE)
            .unwrap();
        // The still-live group retains its actual account allocation fee.
        let baseline = a.root().committed_bytes();
        let query = group
            .create_child(AccountKind::Work, ExternalRef::NONE)
            .unwrap();
        let with_query_metadata = a.root().committed_bytes();
        assert!(with_query_metadata > baseline);
        query.prefund(1_024).unwrap();
        let slot = query.0.slot.load(Ordering::Acquire) as usize;
        let group_gate = group.0.gate.write().unwrap();
        let start = StdArc::new(Barrier::new(2));
        let ready = start.clone();
        let dropper = std::thread::spawn(move || {
            ready.wait();
            drop(query);
        });
        start.wait();
        while a.shared.accounts.lock().unwrap().records[slot].is_some() {
            std::thread::yield_now();
        }
        // Last Drop removed weak membership but cannot yet acquire group_gate
        // to return the root's slack obligation. Three retries must return
        // explicit incomplete quality, not wait for that writer indefinitely.
        let (snapshot, pressure) = a.accounting_snapshot();
        assert!(!snapshot.root.classification_complete);
        assert!(!pressure.classification_complete);
        assert_eq!(pressure.root_committed, with_query_metadata + 1_024);
        assert_eq!(pressure.unclassified_committed, 1_024);
        assert_eq!(pressure.complete_query_pressure(), None);
        drop(group_gate);
        dropper.join().unwrap();
        let (_, pressure) = a.accounting_snapshot();
        assert!(pressure.classification_complete);
        assert_eq!(pressure.root_committed, baseline);
        assert_eq!(pressure.complete_query_pressure(), Some(0));
    }

    #[test]
    fn pinned_members_are_released_after_gate_so_observation_can_be_the_last_reference() {
        let a = authority();
        let baseline = a.root().committed_bytes();
        let query = a
            .create_account(AccountKind::Work, ExternalRef::NONE)
            .unwrap();
        query.prefund(512).unwrap();
        let mut members = ObservationMembers::reserve(a.root());
        {
            let _gate = a.root().0.gate.write().unwrap();
            members.capture(a.root());
            drop(query);
            let (_, pressure) = capture_locked(a.root(), a.root(), &members, Instant::now());
            assert_eq!(pressure.complete_query_pressure(), Some(512));
        }
        // The observer now owns the final strong account reference. Clearing
        // pins outside the gate allows Account::drop to return slack normally.
        members.clear();
        assert_eq!(a.root().committed_bytes(), baseline);
        let evidence = TeardownEvidence {
            tasks_exited: true,
            operators_destroyed: true,
            io: &[],
            now_ns: 1,
        };
        let replacement = a
            .create_account(AccountKind::Work, ExternalRef::NONE)
            .unwrap();
        replacement.retire(&evidence).unwrap();
    }
}
