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

//! Account-local balances and ordered hierarchical transactions.
use crate::sync::{
    Arc, AtomicU64, Mutex, MutexGuard, Ordering, RwLock, RwLockReadGuard, RwLockWriteGuard,
};
use crate::{
    authority::Shared,
    error::{CapacityError, ConstraintKind, MetadataRegistryLabel, Refusal},
    ids::{AccountId, AccountKind, ExternalRef, PolicyVersion},
    policy::{LimitDimension, PolicyInstallOutcome, PolicyLimit},
    snapshot::AccountSnapshot,
};

pub(crate) const MAX_DEPTH: usize = 16;
/// Physical account boxes are common accounting storage, not workload rights.
pub(crate) const ROOT_ACCOUNT_METADATA_BYTES: u64 =
    (std::mem::size_of::<AccountState>() + 2 * std::mem::size_of::<usize>()) as u64;
pub const ACCOUNT_METADATA_BYTES: u64 = (std::mem::size_of::<AccountState>()
    + std::mem::size_of::<crate::membership::MemberNode>()
    + 4 * std::mem::size_of::<usize>()) as u64;
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TopUpPolicy {
    small_threshold_bytes: u64,
    small_step_bytes: u64,
    medium_threshold_bytes: u64,
    medium_step_bytes: u64,
    large_step_bytes: u64,
}

impl TopUpPolicy {
    /// The default step table: 1 MiB below 16 MiB of commitment, 4 MiB below
    /// 64 MiB, 8 MiB above that.
    pub const DEFAULT: Self = Self {
        small_threshold_bytes: 16 * 1024 * 1024,
        small_step_bytes: 1024 * 1024,
        medium_threshold_bytes: 64 * 1024 * 1024,
        medium_step_bytes: 4 * 1024 * 1024,
        large_step_bytes: 8 * 1024 * 1024,
    };

    /// A table with one uniform step, used by tests that want predictable
    /// top-up arithmetic.
    pub const fn uniform(step_bytes: u64) -> Self {
        let step = if step_bytes == 0 { 1 } else { step_bytes };
        Self {
            small_threshold_bytes: 0,
            small_step_bytes: step,
            medium_threshold_bytes: 0,
            medium_step_bytes: step,
            large_step_bytes: step,
        }
    }

    /// Builds a step table. Zero steps are raised to one byte so a top-up
    /// always makes progress.
    pub const fn new(
        small_threshold_bytes: u64,
        small_step_bytes: u64,
        medium_threshold_bytes: u64,
        medium_step_bytes: u64,
        large_step_bytes: u64,
    ) -> Self {
        Self {
            small_threshold_bytes,
            small_step_bytes: if small_step_bytes == 0 {
                1
            } else {
                small_step_bytes
            },
            medium_threshold_bytes,
            medium_step_bytes: if medium_step_bytes == 0 {
                1
            } else {
                medium_step_bytes
            },
            large_step_bytes: if large_step_bytes == 0 {
                1
            } else {
                large_step_bytes
            },
        }
    }

    /// Returns the step size for an account at the given commitment.
    pub const fn step_for(&self, committed_bytes: u64) -> u64 {
        if committed_bytes < self.small_threshold_bytes {
            self.small_step_bytes
        } else if committed_bytes < self.medium_threshold_bytes {
            self.medium_step_bytes
        } else {
            self.large_step_bytes
        }
    }

    /// Largest quantum retained by the adaptive idle target.
    pub const fn max_step(&self) -> u64 {
        let first = if self.small_step_bytes > self.medium_step_bytes {
            self.small_step_bytes
        } else {
            self.medium_step_bytes
        };
        if first > self.large_step_bytes {
            first
        } else {
            self.large_step_bytes
        }
    }

    /// Returns how much to ask the parent for, given the shortfall and the
    /// account's current commitment.
    ///
    /// The result is at least the shortfall: quantisation may take more than
    /// needed, never less.
    pub const fn amount_for(&self, shortfall_bytes: u64, committed_bytes: u64) -> u64 {
        let step = self.step_for(committed_bytes);
        match shortfall_bytes.div_ceil(step).checked_mul(step) {
            Some(rounded) => rounded,
            // A shortfall this close to the address space cannot be satisfied
            // anyway; asking for the exact amount lets the refusal come from
            // the capacity check rather than from an overflow.
            None => shortfall_bytes,
        }
    }
}

impl Default for TopUpPolicy {
    fn default() -> Self {
        Self::DEFAULT
    }
}

#[derive(Debug, Default)]
pub(crate) struct Ledger {
    pub committed: u64,
    pub slack: u64,
    pub floor: u64,
    pub policy: Option<PolicyLimit>,
    pub closed: bool,
    pub retired: bool,
    pub version: PolicyVersion,
    pub peak: u64,
    pub revision: u64,
}
#[derive(Debug)]
pub(crate) struct AccountState {
    pub members: crate::membership::Membership,
    pub membership: Mutex<Option<Arc<crate::membership::MemberNode>>>,
    pub retired: AtomicU64,
    pub id: AccountId,
    pub kind: AccountKind,
    pub external: ExternalRef,
    pub parent: Option<Account>,
    pub shared: Arc<Shared>,
    pub gate: RwLock<()>,
    pub ledger: Mutex<Ledger>,
    pub interactions: AtomicU64,
    pub metrics: [std::sync::atomic::AtomicU64; 6],
    pub wait_histogram: [std::sync::atomic::AtomicU64; 32],
    pub hold_histogram: [std::sync::atomic::AtomicU64; 32],
    pub committed: AtomicU64,
    pub limit: AtomicU64,
    pub closed: AtomicU64,
    pub control: AtomicU64,
    pub slot: AtomicU64,
    pub storage_backed: AtomicU64,
}
#[derive(Debug, Clone)]
pub struct Account(pub(crate) Arc<AccountState>);

/// Control-path counters exclude funded local attach/settle/detach and hooks.
/// Relaxed observation counters are outside the modeled decision state.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct InteractionSnapshot {
    pub gate_acquisitions: u64,
    pub gate_wait_ns: u64,
    pub gate_hold_ns: u64,
    pub ledger_acquisitions: u64,
    pub ledger_wait_ns: u64,
    pub ledger_hold_ns: u64,
    /// Combined gate/ledger acquisition durations, logarithmic nanosecond bins.
    /// Bin i<31 covers up to 2^i ns; bin31 is overflow with no finite upper bound.
    pub wait_histogram: [u64; 32],
    pub hold_histogram: [u64; 32],
}
pub(crate) struct MeasuredGuard<'a, T> {
    guard: T,
    account: &'a AccountState,
    acquired_at: std::time::Instant,
    metric_base: usize,
}
impl<T: std::ops::Deref> std::ops::Deref for MeasuredGuard<'_, T> {
    type Target = T::Target;
    fn deref(&self) -> &Self::Target {
        &self.guard
    }
}
impl<T: std::ops::DerefMut> std::ops::DerefMut for MeasuredGuard<'_, T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.guard
    }
}
impl<T> Drop for MeasuredGuard<'_, T> {
    fn drop(&mut self) {
        let ns = self.acquired_at.elapsed().as_nanos().min(u64::MAX as u128) as u64;
        self.account.metrics[self.metric_base + 2]
            .fetch_add(ns, std::sync::atomic::Ordering::Relaxed);
        self.account.hold_histogram[duration_bin(ns)]
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    }
}
impl AccountState {
    fn measured<T>(&self, base: usize, acquire: impl FnOnce() -> T) -> MeasuredGuard<'_, T> {
        let start = std::time::Instant::now();
        let guard = acquire();
        let acquired_at = std::time::Instant::now();
        self.metrics[base].fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        self.metrics[base + 1].fetch_add(
            acquired_at
                .duration_since(start)
                .as_nanos()
                .min(u64::MAX as u128) as u64,
            std::sync::atomic::Ordering::Relaxed,
        );
        let wait = acquired_at
            .duration_since(start)
            .as_nanos()
            .min(u64::MAX as u128) as u64;
        self.wait_histogram[duration_bin(wait)].fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        MeasuredGuard {
            guard,
            account: self,
            acquired_at,
            metric_base: base,
        }
    }
}
fn duration_bin(ns: u64) -> usize {
    let bit = u64::BITS - ns.saturating_sub(1).leading_zeros();
    (bit as usize).min(31)
}
pub(crate) type LedgerGuard<'a> = MeasuredGuard<'a, MutexGuard<'a, Ledger>>;
pub(crate) struct Path {
    pub nodes: [Option<Account>; MAX_DEPTH],
    pub len: usize,
}
impl Path {
    pub fn new(account: &Account) -> Self {
        let mut nodes = std::array::from_fn(|_| None);
        let mut next = Some(account.clone());
        let mut len = 0;
        while let Some(node) = next {
            assert!(len < MAX_DEPTH, "validated account depth");
            next = node.0.parent.clone();
            nodes[len] = Some(node);
            len += 1;
        }
        Self { nodes, len }
    }
    pub fn node(&self, i: usize) -> &Account {
        self.nodes[i].as_ref().unwrap()
    }
    pub fn growth_locks(&self, bytes: u64) -> [Option<LedgerGuard<'_>>; MAX_DEPTH] {
        self.demand_locks(bytes, false)
    }
    pub fn requirement_locks(&self, bytes: u64) -> [Option<LedgerGuard<'_>>; MAX_DEPTH] {
        self.demand_locks(bytes, true)
    }
    fn demand_locks(&self, bytes: u64, own_slack: bool) -> [Option<LedgerGuard<'_>>; MAX_DEPTH] {
        let mut locks = std::array::from_fn(|_| None);
        locks[0] = Some(
            self.node(0)
                .0
                .measured(3, || self.node(0).0.ledger.lock().unwrap()),
        );
        let mut demand = if own_slack {
            bytes.saturating_sub(locks[0].as_ref().unwrap().slack)
        } else {
            bytes
        };
        for (i, lock) in locks.iter_mut().enumerate().take(self.len).skip(1) {
            if demand == 0 {
                break;
            }
            let s = self
                .node(i)
                .0
                .measured(3, || self.node(i).0.ledger.lock().unwrap());
            demand = demand.saturating_sub(s.slack.saturating_sub(s.floor));
            *lock = Some(s);
        }
        locks
    }
    pub fn shared_gates(&self) -> [Option<MeasuredGuard<'_, RwLockReadGuard<'_, ()>>>; MAX_DEPTH] {
        std::array::from_fn(|i| {
            if i < self.len {
                Some(
                    self.node(i)
                        .0
                        .measured(0, || self.node(i).0.gate.read().unwrap()),
                )
            } else {
                None
            }
        })
    }
    pub fn exclusive_gates(
        &self,
    ) -> [Option<MeasuredGuard<'_, RwLockWriteGuard<'_, ()>>>; MAX_DEPTH] {
        std::array::from_fn(|i| {
            if i < self.len {
                Some(
                    self.node(i)
                        .0
                        .measured(0, || self.node(i).0.gate.write().unwrap()),
                )
            } else {
                None
            }
        })
    }
    pub fn locks(&self) -> [Option<LedgerGuard<'_>>; MAX_DEPTH] {
        let mut locks = std::array::from_fn(|_| None);
        for (i, lock) in locks.iter_mut().enumerate().take(self.len) {
            *lock = Some(
                self.node(i)
                    .0
                    .measured(3, || self.node(i).0.ledger.lock().unwrap()),
            );
        }
        locks
    }
}

impl Account {
    pub fn id(&self) -> AccountId {
        self.0.id
    }
    pub fn kind(&self) -> AccountKind {
        self.0.kind
    }
    pub fn external_ref(&self) -> ExternalRef {
        self.0.external
    }
    pub(crate) fn new_root(shared: Arc<Shared>) -> Self {
        Self(Arc::new(AccountState {
            members: crate::membership::Membership::default(),
            membership: Mutex::new(None),
            retired: AtomicU64::new(0),
            id: AccountId::new(1),
            kind: AccountKind::Process,
            external: ExternalRef::NONE,
            parent: None,
            shared,
            gate: RwLock::new(()),
            ledger: Mutex::new(Ledger::default()),
            interactions: AtomicU64::new(0),
            metrics: std::array::from_fn(|_| std::sync::atomic::AtomicU64::new(0)),
            wait_histogram: std::array::from_fn(|_| std::sync::atomic::AtomicU64::new(0)),
            hold_histogram: std::array::from_fn(|_| std::sync::atomic::AtomicU64::new(0)),
            committed: AtomicU64::new(0),
            limit: AtomicU64::new(u64::MAX),
            closed: AtomicU64::new(0),
            control: AtomicU64::new(0),
            slot: AtomicU64::new(u64::MAX),
            storage_backed: AtomicU64::new(0),
        }))
    }
    pub fn create_child(
        &self,
        kind: AccountKind,
        external: ExternalRef,
    ) -> Result<Self, CapacityError> {
        let _assembly = self.0.shared.assembly.lock().unwrap();
        self.create_child_unlocked(kind, external)
    }
    pub(crate) fn create_child_unlocked(
        &self,
        kind: AccountKind,
        external: ExternalRef,
    ) -> Result<Self, CapacityError> {
        if kind == AccountKind::Process || Path::new(self).len == MAX_DEPTH {
            return Err(CapacityError::Invalid {
                detail: "invalid account kind or tree depth",
            });
        }
        // Allocation precedes admission locks. Only a weak entry is published
        // while the parent lifecycle gate prevents closing underneath it.
        let child = Self(Arc::new(AccountState {
            members: crate::membership::Membership::default(),
            membership: Mutex::new(None),
            retired: AtomicU64::new(0),
            id: AccountId::new(self.0.shared.next_identity()?),
            kind,
            external,
            parent: Some(self.clone()),
            shared: self.0.shared.clone(),
            gate: RwLock::new(()),
            ledger: Mutex::new(Ledger::default()),
            interactions: AtomicU64::new(0),
            metrics: std::array::from_fn(|_| std::sync::atomic::AtomicU64::new(0)),
            wait_histogram: std::array::from_fn(|_| std::sync::atomic::AtomicU64::new(0)),
            hold_histogram: std::array::from_fn(|_| std::sync::atomic::AtomicU64::new(0)),
            committed: AtomicU64::new(0),
            limit: AtomicU64::new(u64::MAX),
            closed: AtomicU64::new(0),
            control: AtomicU64::new(0),
            slot: AtomicU64::new(u64::MAX),
            storage_backed: AtomicU64::new(0),
        }));
        let member = crate::membership::MemberNode::account(&child);
        let path = Path::new(self);
        let _gates = path.shared_gates();
        for i in 0..path.len {
            if path.node(i).0.closed.load(Ordering::Acquire) != 0 {
                return Err(CapacityError::Closed {
                    account: path.node(i).id(),
                });
            }
        }
        let root = path.node(path.len - 1);
        let mut root_state = root.0.ledger.lock().unwrap();
        let target = self.0.shared.target.load(Ordering::Acquire);
        if ACCOUNT_METADATA_BYTES > target.saturating_sub(root_state.committed) {
            return Err(self.refusal(
                root,
                &root_state,
                ACCOUNT_METADATA_BYTES,
                root_state.committed > target,
            ));
        }
        let mut registry = self.0.shared.accounts.lock().unwrap();
        if registry.free_slots.is_empty() {
            return Err(CapacityError::MetadataExhausted {
                registry: MetadataRegistryLabel::Accounts,
                limit: self.0.shared.max_accounts as u64,
            });
        }
        root_state.committed += ACCOUNT_METADATA_BYTES;
        root_state.peak = root_state.peak.max(root_state.committed);
        root_state.revision += 1;
        node_publish(root, root_state.committed);
        self.0
            .shared
            .storage_bytes
            .fetch_add(ACCOUNT_METADATA_BYTES, Ordering::Release);
        child.0.storage_backed.store(1, Ordering::Release);
        let slot = registry.free_slots.pop().unwrap();
        child.0.slot.store(slot as u64, Ordering::Relaxed);
        registry.records[slot] = Some(Arc::downgrade(&child.0));
        registry.upper = registry.upper.max(slot + 1);
        self.0
            .shared
            .membership_revision
            .fetch_add(1, Ordering::Release);
        drop(registry);
        self.0.members.insert(&member);
        *child.0.membership.lock().unwrap() = Some(member);
        // The node was allocated before admission; linking creates no allocation.
        Ok(child)
    }
    pub fn install_policy(&self, bytes: u64, dimension: LimitDimension) -> PolicyInstallOutcome {
        let path = Path::new(self);
        let _gates = path.exclusive_gates();
        let mut s = self.0.ledger.lock().unwrap();
        s.version = s.version.next();
        s.policy = Some(PolicyLimit::bytes(bytes, dimension, s.version));
        s.revision += 1;
        self.0.limit.store(bytes, Ordering::Release);
        PolicyInstallOutcome {
            version: s.version,
            committed_bytes: s.committed,
            excess_bytes: s.committed.saturating_sub(bytes),
            growth_frozen: s.closed || s.committed > bytes,
        }
    }
    pub fn close_to_growth(&self) {
        self.close_members();
    }
    pub(crate) fn close_members(&self) -> crate::membership::Subtree {
        {
            let path = Path::new(self);
            let _gates = path.exclusive_gates();
            let mut s = self.0.ledger.lock().unwrap();
            s.closed = true;
            s.revision += 1;
            self.0.closed.store(1, Ordering::Release);
        }
        // Publication excludes new subtree admission before collection; no
        // global owner/account history is visited by this lifecycle operation.
        let members = {
            // Handoffs/reclaim take an exclusive gate along their path. A
            // read gate here freezes moves throughout this closed subtree;
            // growth is already refused by the published ancestor close.
            // Release before sealing or acquiring descendant write gates.
            let _stable_membership = self.0.gate.read().unwrap();
            crate::membership::Subtree::collect(self)
        };
        for lane in &members.lanes {
            lane.seal();
        }
        for domain in &members.domains {
            domain.seal();
        }
        for account in &members.accounts {
            let path = Path::new(account);
            let _gates = path.exclusive_gates();
            let mut s = account.0.ledger.lock().unwrap();
            s.closed = true;
            account.0.closed.store(1, Ordering::Release);
        }
        members
    }
    pub fn is_closed_to_growth(&self) -> bool {
        self.0.ledger.lock().unwrap().closed
    }
    pub fn committed_bytes(&self) -> u64 {
        self.0.ledger.lock().unwrap().committed
    }
    pub fn local_free_bytes(&self) -> u64 {
        self.0.ledger.lock().unwrap().slack
    }
    pub fn interactions(&self) -> u64 {
        self.0.interactions.load(Ordering::Relaxed)
    }
    pub fn interaction_snapshot(&self) -> InteractionSnapshot {
        let m = &self.0.metrics;
        let read = |i: usize| m[i].load(std::sync::atomic::Ordering::Relaxed);
        InteractionSnapshot {
            gate_acquisitions: read(0),
            gate_wait_ns: read(1),
            gate_hold_ns: read(2),
            ledger_acquisitions: read(3),
            ledger_wait_ns: read(4),
            ledger_hold_ns: read(5),
            wait_histogram: std::array::from_fn(|i| {
                self.0.wait_histogram[i].load(std::sync::atomic::Ordering::Relaxed)
            }),
            hold_histogram: std::array::from_fn(|i| {
                self.0.hold_histogram[i].load(std::sync::atomic::Ordering::Relaxed)
            }),
        }
    }
    /// Prefunds exclusive account slack. It does not establish a second wallet.
    pub fn prefund(&self, bytes: u64) -> Result<(), CapacityError> {
        self.grow(bytes, true)
    }
    pub(crate) fn refusal(
        &self,
        node: &Self,
        s: &Ledger,
        bytes: u64,
        frozen: bool,
    ) -> CapacityError {
        let root = node.0.parent.is_none();
        let limit = if root {
            self.0.shared.target.load(Ordering::Acquire)
        } else {
            s.policy.map_or(u64::MAX, |p| p.limit_bytes())
        };
        let refusal = Refusal {
            request_account: self.id(),
            constraint_account: node.id(),
            constraint_kind: if frozen {
                ConstraintKind::GrowthFrozen
            } else if root {
                ConstraintKind::ProcessCapacity
            } else if node.id() == self.id() {
                ConstraintKind::AccountPolicy
            } else {
                ConstraintKind::AncestorPolicy
            },
            dimension: s.policy.map(|p| p.dimension()),
            requested: bytes,
            required_growth: bytes,
            ledger_revision: s.revision,
            limit,
            committed: s.committed,
            policy_revision: s.version,
            capacity_revision: self.0.shared.capacity_revision.load(Ordering::Acquire),
        };
        if bytes > if root { self.0.shared.ceiling } else { limit } {
            CapacityError::ImpossibleRequest(refusal)
        } else if !root
            && s.policy
                .is_some_and(|p| p.dimension() == LimitDimension::Work)
        {
            CapacityError::QueryLimit(refusal)
        } else {
            CapacityError::ShortageCandidate(refusal)
        }
    }
    pub(crate) fn grow(&self, bytes: u64, spare: bool) -> Result<(), CapacityError> {
        let path = Path::new(self);
        let _gates = path.shared_gates();
        qualify(self, &path, bytes, spare)?;
        let mut states = path.growth_locks(bytes);
        grow_locked(self, &path, &mut states, bytes, spare)
    }
    pub fn return_slack(&self, bytes: u64) -> u64 {
        // The control treasury contains only prepaid floor backing. Uncovered
        // E may lift C above floor, but never makes that backing refundable.
        if self.0.control.load(Ordering::Acquire) != 0 {
            return 0;
        }
        let path = Path::new(self);
        let _gates = path.exclusive_gates();
        let mut states = path.locks();
        let own = states[0].as_ref().unwrap();
        let amount = bytes.min(own.slack.min(own.committed.saturating_sub(own.floor)));
        release_locked(&path, &mut states, amount);
        amount
    }
    pub fn snapshot(&self) -> AccountSnapshot {
        crate::snapshot::account_snapshot(self)
    }
}

// Design: ADR-0166 (docs/adr/ADR-0166-hierarchical-funding-and-stable-allocation-origins.md)
pub(crate) fn grow_locked(
    request: &Account,
    path: &Path,
    states: &mut [Option<LedgerGuard<'_>>; MAX_DEPTH],
    bytes: u64,
    spare: bool,
) -> Result<(), CapacityError> {
    let mut demand = bytes;
    let mut increments = [0; MAX_DEPTH];
    let mut consumed = [0; MAX_DEPTH];
    for i in 0..path.len {
        let node = path.node(i);
        let Some(s) = states[i].as_ref() else { break };
        if s.closed {
            return Err(CapacityError::Closed { account: node.id() });
        }
        let target = if node.0.parent.is_none() {
            node.0.shared.target.load(Ordering::Acquire)
        } else {
            s.policy.map_or(u64::MAX, |p| p.limit_bytes())
        };
        if spare && s.committed > target {
            return Err(request.refusal(node, s, bytes, true));
        }
        increments[i] = demand;
        if demand > target.saturating_sub(s.committed) {
            return Err(request.refusal(node, s, bytes, false));
        }
        if i + 1 < path.len
            && let Some(parent) = states[i + 1].as_ref()
        {
            let available = parent.slack.saturating_sub(parent.floor);
            consumed[i + 1] = demand.min(available);
            demand -= consumed[i + 1];
        }
    }
    for i in 0..path.len {
        if increments[i] != 0 || consumed[i] != 0 {
            let s = states[i].as_mut().unwrap();
            s.committed = s
                .committed
                .checked_add(increments[i])
                .expect("validated capacity arithmetic");
            node_publish(path.node(i), s.committed);
            s.slack -= consumed[i];
            s.peak = s.peak.max(s.committed);
            s.revision += 1;
            path.node(i).0.interactions.fetch_add(1, Ordering::Relaxed);
        }
    }
    states[0].as_mut().unwrap().slack = states[0]
        .as_ref()
        .unwrap()
        .slack
        .checked_add(bytes)
        .expect("validated slack arithmetic");
    Ok(())
}

pub(crate) fn release_locked(
    path: &Path,
    states: &mut [Option<LedgerGuard<'_>>; MAX_DEPTH],
    bytes: u64,
) {
    // Returning backing never manufactures root slack. The configured target
    // remains an independent ceiling even after unbacked allocation is freed.
    states[0].as_mut().unwrap().slack -= bytes;
    for (i, s) in states.iter_mut().enumerate().take(path.len) {
        let s = s.as_mut().unwrap();
        s.committed -= bytes;
        s.revision += 1;
        node_publish(path.node(i), s.committed);
        path.node(i).0.interactions.fetch_add(1, Ordering::Relaxed);
    }
}

pub(crate) fn node_publish(node: &Account, committed: u64) {
    node.0.committed.store(committed, Ordering::Release);
}
pub(crate) fn qualify(
    request: &Account,
    path: &Path,
    bytes: u64,
    spare: bool,
) -> Result<(), CapacityError> {
    // Qualification is covered by already-held gates. Funded boundaries never
    // enter this path. Unchanged ancestors require no ledger acquisition.
    for i in 0..path.len {
        let node = path.node(i);
        if node.0.closed.load(Ordering::Acquire) != 0 {
            return Err(CapacityError::Closed { account: node.id() });
        }
        let limit = if node.0.parent.is_none() {
            node.0.shared.target.load(Ordering::Acquire)
        } else {
            node.0.limit.load(Ordering::Acquire)
        };
        if spare && node.0.committed.load(Ordering::Acquire) > limit {
            let s = node.0.ledger.lock().unwrap();
            return Err(request.refusal(node, &s, bytes, true));
        }
    }
    Ok(())
}

impl Drop for AccountState {
    fn drop(&mut self) {
        if let Some(parent) = &self.parent
            && let Some(member) = self.membership.lock().unwrap().take()
        {
            parent.0.members.remove(&member);
        }
        let slot = self.slot.load(Ordering::Relaxed);
        if slot != u64::MAX && slot != 0 {
            let mut registry = self.shared.accounts.lock().unwrap();
            if registry.records[slot as usize].is_some() {
                registry.records[slot as usize] = None;
                self.shared
                    .membership_revision
                    .fetch_add(1, Ordering::Release);
                registry.free_slots.push(slot as usize);
            }
        }
        if let Some(parent) = &self.parent {
            // Outstanding domains and children strongly pin their affiliation;
            // only undistributed slack can survive the final account handle.
            let amount = self.ledger.get_mut().unwrap().committed;
            let path = Path::new(parent);
            let _gates = path.exclusive_gates();
            let mut states = path.locks();
            crate::settlement::adjust_commitment(&path, &mut states, amount, 0);
            if self.storage_backed.load(Ordering::Acquire) != 0 {
                let root_index = path.len - 1;
                let root = states[root_index].as_mut().unwrap();
                root.committed -= ACCOUNT_METADATA_BYTES;
                root.revision += 1;
                node_publish(path.node(root_index), root.committed);
                self.shared
                    .storage_bytes
                    .fetch_sub(ACCOUNT_METADATA_BYTES, Ordering::Release);
            }
        }
    }
}
