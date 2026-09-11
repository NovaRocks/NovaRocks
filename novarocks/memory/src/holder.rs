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

//! Shared retention and pins (MEM-1 wave-1 T03).
//!
//! A retention lease records that a scope actually holds shared backing; a pin
//! protects a specific resident frame and its address. Both are exposure, not
//! new physical charges: the same resource may appear in several scopes and
//! those numbers must never be summed. The last pin leaving means only that a
//! frame became unloadable — the real release and the capacity return are
//! separate events.
//!
//! # Exposure is not a charge
//!
//! This is the single rule the whole module is shaped around. When a cache
//! holds one 8 MiB frame and three concurrent statements read it, the process
//! holds 8 MiB, not 24 MiB. The bytes are charged once, to whoever actually
//! allocated the backing. What each statement has is *exposure*: a claim that
//! stops the frame being dropped, and a fact worth reporting per scope so an
//! arbitrator can see who is keeping a resource alive.
//!
//! Reporting exposure per scope is useful; adding it up is a fabrication. So
//! the API is built so the addition is not available:
//!
//! - [`HolderRegistry::exposure_of`] answers for one scope.
//! - [`HolderRegistry::resource_exposure`] answers for one resource, and its
//!   `bytes` is that resource's single size — never a size multiplied by the
//!   number of scopes holding it.
//! - There is no method that totals exposure across scopes, and there
//!   deliberately never will be. A consumer that wants a process-wide number
//!   must read the capacity authority's `L`, which counts each byte once.
//!
//! # Unloadable, released, and returned are three different events
//!
//! Dropping the last pin on a frame does not free memory. It says only that
//! the frame *may now be unloaded* — the owner is free to evict it, and free
//! not to. Eviction is the second event, and returning the capacity to the
//! authority is the third. Collapsing them is how a cache ends up reporting
//! capacity it never gave back, so [`PinOutcome`] reports unload eligibility
//! and states, in both its type and its accessors, that no bytes moved.

use std::collections::BTreeMap;
use std::error::Error;
use std::fmt;
use std::sync::{Arc, Mutex, MutexGuard};

use crate::budget::{BoundedSlots, MetadataBudget};
use crate::error::{CapacityError, MetadataRegistryLabel};
use crate::ids::{AccountId, ExternalRef, HolderId, IdSource, PinId};

/// One scope's exposure to shared backing.
///
/// Every field describes this scope alone. `exposed_bytes` sums the *distinct*
/// resources this scope holds, counting each resource's single size once, so
/// it answers "how much backing would this scope be keeping alive on its own".
/// It is not a share of a total and it is not comparable to a charge: two
/// scopes holding the same 8 MiB frame each report 8 MiB, and the process
/// still holds 8 MiB.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ScopeExposure {
    /// Live retention leases held by this scope. A scope may hold several
    /// leases on one resource, so this can exceed `distinct_resources`.
    pub lease_count: u32,
    /// Live pins held by this scope.
    pub pinned_count: u32,
    /// Distinct resources this scope exposes through a retention lease.
    pub distinct_resources: u32,
    /// Sum of the single sizes of those distinct resources.
    ///
    /// Counts resources exposed by a retention lease only. A pin says a frame
    /// must stay resident, not that this scope holds the backing, so pins are
    /// reported by `pinned_count` and add no bytes here.
    pub exposed_bytes: u64,
}

impl ScopeExposure {
    /// The exposure of a scope the registry has never seen.
    pub const NONE: Self = Self {
        lease_count: 0,
        pinned_count: 0,
        distinct_resources: 0,
        exposed_bytes: 0,
    };

    /// Reports whether this scope holds nothing at all.
    pub const fn is_empty(&self) -> bool {
        self.lease_count == 0 && self.pinned_count == 0
    }
}

/// One resource's exposure across the scopes holding it.
///
/// `bytes` is the resource's single size. It is stated once, whatever
/// `holding_scopes` says, because that is the physical truth: the backing
/// exists once. A consumer looking for "the cost of this resource" has it
/// here; a consumer multiplying it by `holding_scopes` has invented a number.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ResourceExposure {
    /// The resource described.
    pub resource: ExternalRef,
    /// How many distinct scopes currently hold a retention lease on it.
    pub holding_scopes: usize,
    /// The resource's single size. Never a sum over scopes.
    pub bytes: u64,
    /// Live pins on this resource, across all scopes. While this is non-zero
    /// the resident frame must not be unloaded.
    pub pin_count: u32,
    /// The generation a [`HolderRegistry::try_pin`] must present to succeed.
    pub generation: u64,
}

impl ResourceExposure {
    /// Reports whether the resident frame may be unloaded.
    ///
    /// Eligibility is not permission to forget the resource: leases may still
    /// hold it, and unloading is the owner's decision.
    pub const fn is_unloadable(&self) -> bool {
        self.pin_count == 0
    }
}

/// What dropping a pin established about the resident frame.
///
/// The two arms exist so the caller cannot read "the last pin left" as "the
/// memory is gone". They describe residency, never bytes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UnloadEligibility {
    /// Other pins remain, so the frame must stay resident at its address.
    StillPinned {
        /// Pins still held on the resource, across all scopes.
        remaining_pins: u32,
    },
    /// This was the last pin: the frame became eligible for unloading.
    ///
    /// Nothing was freed and nothing was returned. The owner may now evict
    /// the frame, and until it does the backing is still allocated and still
    /// charged to whoever allocated it.
    BecameUnloadable,
}

impl UnloadEligibility {
    /// Reports whether the frame became unloadable with this drop.
    pub const fn became_unloadable(&self) -> bool {
        matches!(self, Self::BecameUnloadable)
    }

    /// Returns the label used in diagnostics.
    pub const fn label(&self) -> &'static str {
        match self {
            Self::StillPinned { .. } => "still-pinned",
            Self::BecameUnloadable => "became-unloadable",
        }
    }
}

impl fmt::Display for UnloadEligibility {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.label())
    }
}

/// What dropping one pin did.
///
/// A pin is protection, so its removal reports residency and nothing else.
/// The two zero-returning accessors are not filler: they are the executable
/// form of the rule that a pin drop is neither a release nor a capacity
/// return, and a caller that routes them into its accounting gets the right
/// answer by construction.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PinOutcome {
    pin: PinId,
    scope: AccountId,
    resource: ExternalRef,
    eligibility: UnloadEligibility,
}

impl PinOutcome {
    /// Returns the pin that was dropped.
    pub const fn pin(&self) -> PinId {
        self.pin
    }

    /// Returns the scope that held the pin.
    pub const fn scope(&self) -> AccountId {
        self.scope
    }

    /// Returns the resource whose frame was protected.
    pub const fn resource(&self) -> ExternalRef {
        self.resource
    }

    /// Returns what this drop established about residency.
    pub const fn eligibility(&self) -> UnloadEligibility {
        self.eligibility
    }

    /// Reports whether the frame became eligible for unloading.
    pub const fn became_unloadable(&self) -> bool {
        self.eligibility.became_unloadable()
    }

    /// Bytes freed by dropping this pin: always zero.
    ///
    /// Unpinning removes protection, not backing. The frame is still resident
    /// and still allocated; only its owner can free it, and only by evicting
    /// it as a separate act.
    pub const fn released_bytes(&self) -> u64 {
        0
    }

    /// Bytes returned to the capacity authority: always zero.
    ///
    /// Capacity comes back when the owner releases the charge it holds on the
    /// backing. A pin never held that charge, so it has nothing to return.
    pub const fn capacity_returned_bytes(&self) -> u64 {
        0
    }
}

/// What dropping one retention lease did.
///
/// Like [`PinOutcome`], this reports exposure and residency only. A lease is
/// the statement "this scope holds shared backing"; withdrawing it removes the
/// statement. The backing is charged to its allocator, so no byte moves here
/// either.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LeaseOutcome {
    holder: HolderId,
    scope: AccountId,
    resource: ExternalRef,
    bytes: u64,
    scope_still_holds: bool,
    remaining_holding_scopes: usize,
    remaining_pins: u32,
}

impl LeaseOutcome {
    /// Returns the lease that was dropped.
    pub const fn holder(&self) -> HolderId {
        self.holder
    }

    /// Returns the scope that held the lease.
    pub const fn scope(&self) -> AccountId {
        self.scope
    }

    /// Returns the resource that was exposed.
    pub const fn resource(&self) -> ExternalRef {
        self.resource
    }

    /// Returns the resource's single size, as the lease declared it.
    pub const fn bytes(&self) -> u64 {
        self.bytes
    }

    /// Reports whether the same scope still holds another lease on the
    /// resource, in which case its exposure did not change.
    pub const fn scope_still_holds(&self) -> bool {
        self.scope_still_holds
    }

    /// Returns how many distinct scopes still hold the resource.
    pub const fn remaining_holding_scopes(&self) -> usize {
        self.remaining_holding_scopes
    }

    /// Returns how many pins still protect the resource's resident frame.
    pub const fn remaining_pins(&self) -> u32 {
        self.remaining_pins
    }

    /// Reports whether no scope holds the resource any more.
    ///
    /// This is the point at which the owner may consider dropping the
    /// backing — subject to pins, which are a separate condition.
    pub const fn became_unheld(&self) -> bool {
        self.remaining_holding_scopes == 0
    }

    /// Bytes returned to the capacity authority: always zero.
    ///
    /// Withdrawing exposure is not a release. Whoever allocated the backing
    /// still holds its charge, and returns it when it frees the backing.
    pub const fn capacity_returned_bytes(&self) -> u64 {
        0
    }
}

/// Why a pin could not be taken.
///
/// Every arm is a refusal to hand out protection, and none of them leaves a
/// half-taken pin behind.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PinRejection {
    /// The resource's generation moved between the caller's observation and
    /// this call, so the frame the caller meant to pin is no longer current.
    ///
    /// `observed` is the generation the registry holds now. A caller that
    /// still wants a pin re-resolves the frame and presents this value.
    GenerationChanged {
        /// The generation the registry currently holds.
        observed: u64,
    },
    /// The registry knows of no exposure for this resource, so there is no
    /// resident frame to protect.
    ///
    /// A resource becomes known when a scope takes a retention lease on it,
    /// and stops being known when its last lease and pin are gone.
    NotResident {
        /// The resource that was asked for.
        resource: ExternalRef,
    },
    /// The bounded holder registry is full.
    ///
    /// Carried as the crate's own [`CapacityError`] so leases and pins refuse
    /// with one exhaustion story rather than two.
    Exhausted {
        /// The typed metadata refusal.
        cause: CapacityError,
    },
}

impl fmt::Display for PinRejection {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::GenerationChanged { observed } => {
                write!(f, "pin rejected: the resource generation is now {observed}")
            }
            Self::NotResident { resource } => {
                write!(f, "pin rejected: {resource} has no recorded exposure")
            }
            Self::Exhausted { cause } => write!(f, "pin rejected: {cause}"),
        }
    }
}

impl Error for PinRejection {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Exhausted { cause } => Some(cause),
            Self::GenerationChanged { .. } | Self::NotResident { .. } => None,
        }
    }
}

impl PinRejection {
    /// Returns the metadata refusal when the rejection was an exhausted
    /// registry.
    pub const fn as_capacity_error(&self) -> Option<&CapacityError> {
        match self {
            Self::Exhausted { cause } => Some(cause),
            Self::GenerationChanged { .. } | Self::NotResident { .. } => None,
        }
    }
}

/// One scope's recorded claim on shared backing.
///
/// Holding this value is the claim. It is RAII because exposure that outlives
/// its holder is worse than no exposure at all: it makes an arbitrator believe
/// a finished statement is still keeping a cache frame alive. Dropping the
/// lease withdraws the claim from the registry, and dropping it during a panic
/// unwind withdraws it too.
///
/// A lease is not a charge and does not allocate. The bytes it declares are
/// the resource's single size, recorded so exposure can be reported; the
/// charge for that backing belongs to whoever allocated it.
#[derive(Debug)]
pub struct RetentionLease {
    state: Arc<HolderState>,
    holder: HolderId,
    scope: AccountId,
    resource: ExternalRef,
    bytes: u64,
    withdrawn: bool,
}

impl RetentionLease {
    /// Returns this lease's identity.
    pub const fn holder(&self) -> HolderId {
        self.holder
    }

    /// Returns the scope holding the lease.
    pub const fn scope(&self) -> AccountId {
        self.scope
    }

    /// Returns the resource being held.
    pub const fn resource(&self) -> ExternalRef {
        self.resource
    }

    /// Returns the resource's single size.
    pub const fn bytes(&self) -> u64 {
        self.bytes
    }

    /// Takes a pin on the held resource's current frame.
    ///
    /// Preferred over [`HolderRegistry::try_pin`] because the scope and the
    /// resource come from the lease, so they cannot be mismatched: a pin is
    /// attributed to the scope that actually holds the backing.
    pub fn try_pin(&self, expected_generation: u64) -> Result<Pin, PinRejection> {
        self.state
            .try_pin(self.scope, self.resource, expected_generation)
    }

    /// Withdraws the exposure and reports what changed.
    ///
    /// Identical in effect to dropping the lease; use it when the caller wants
    /// the outcome. Nothing is freed and no capacity is returned — see
    /// [`LeaseOutcome::capacity_returned_bytes`].
    pub fn withdraw(mut self) -> LeaseOutcome {
        let outcome = self
            .state
            .withdraw_lease(self.holder, self.scope, self.resource, self.bytes);
        self.withdrawn = true;
        outcome
    }
}

impl Drop for RetentionLease {
    fn drop(&mut self) {
        if !self.withdrawn {
            self.state
                .withdraw_lease(self.holder, self.scope, self.resource, self.bytes);
        }
    }
}

/// Protection for one resident frame.
///
/// A pin says: while I exist, this frame must stay at this address. That is a
/// stronger and narrower claim than a retention lease, which only says the
/// backing must not be dropped. An operator reading a frame in place needs the
/// former; a scope that merely depends on a resource needs the latter.
///
/// A pin is taken against a generation, so it can never be attached to a frame
/// that was already replaced. Dropping it reports [`UnloadEligibility`] and
/// changes no charge.
#[derive(Debug)]
pub struct Pin {
    state: Arc<HolderState>,
    pin: PinId,
    scope: AccountId,
    resource: ExternalRef,
    generation: u64,
    withdrawn: bool,
}

impl Pin {
    /// Returns this pin's identity.
    pub const fn pin(&self) -> PinId {
        self.pin
    }

    /// Returns the scope holding the pin.
    pub const fn scope(&self) -> AccountId {
        self.scope
    }

    /// Returns the pinned resource.
    pub const fn resource(&self) -> ExternalRef {
        self.resource
    }

    /// Returns the generation this pin was taken at.
    ///
    /// The frame this pin protects is the one that was current at this
    /// generation. A later [`HolderRegistry::invalidate`] moves the registry's
    /// generation on without disturbing this pin, because the frame it holds
    /// is still resident.
    pub const fn generation(&self) -> u64 {
        self.generation
    }

    /// Drops the protection and reports whether the frame became unloadable.
    ///
    /// Identical in effect to dropping the pin; use it when the caller wants
    /// the outcome.
    pub fn release(mut self) -> PinOutcome {
        let outcome = self.state.release_pin(self.pin, self.scope, self.resource);
        self.withdrawn = true;
        outcome
    }
}

impl Drop for Pin {
    fn drop(&mut self) {
        if !self.withdrawn {
            self.state.release_pin(self.pin, self.scope, self.resource);
        }
    }
}

/// The bounded registry of retention leases and pins.
///
/// One registry serves a whole process. Cloning it shares the same state, so
/// a cache and an operator can both hold handles and still see one truth.
///
/// Leases and pins draw on the same [`MetadataRegistryLabel::Holders`] budget,
/// because they have the same unbounded-growth failure mode: a consumer that
/// stops dropping them. Reaching the limit refuses the new claim; it never
/// evicts an existing one, since an existing record stands for backing that is
/// still alive.
#[derive(Debug, Clone)]
pub struct HolderRegistry {
    state: Arc<HolderState>,
}

impl HolderRegistry {
    /// Creates an empty registry bounded by `budget`.
    pub fn new(budget: &MetadataBudget) -> Self {
        Self {
            state: Arc::new(HolderState {
                slots: BoundedSlots::from_budget(budget, MetadataRegistryLabel::Holders),
                holder_ids: IdSource::new(),
                pin_ids: IdSource::new(),
                tables: Mutex::new(HolderTables::default()),
            }),
        }
    }

    /// Records that `scope` holds `resource`, whose single size is `bytes`.
    ///
    /// The first lease on a resource establishes its size and starts its
    /// generation at zero. A later lease that declares a different size is
    /// refused with [`CapacityError::Unsupported`] rather than silently
    /// preferring one number: a resource with two sizes makes every exposure
    /// reading ambiguous, and the core fails fast on ambiguity instead of
    /// guessing.
    pub fn acquire_lease(
        &self,
        scope: AccountId,
        resource: ExternalRef,
        bytes: u64,
    ) -> Result<RetentionLease, CapacityError> {
        self.state.acquire_lease(scope, resource, bytes)
    }

    /// Takes a pin on `resource` if its generation is still
    /// `expected_generation`.
    ///
    /// The check and the pin are one operation under one lock. Splitting them
    /// into `generation_of` followed by `pin` would leave a window in which
    /// the frame is replaced, and the caller would end up protecting an
    /// address that no longer holds what it read — the exact bug this
    /// signature exists to make unwritable.
    ///
    /// Prefer [`RetentionLease::try_pin`] when the caller already holds a
    /// lease, so the scope and resource cannot disagree with it.
    pub fn try_pin(
        &self,
        scope: AccountId,
        resource: ExternalRef,
        expected_generation: u64,
    ) -> Result<Pin, PinRejection> {
        self.state.try_pin(scope, resource, expected_generation)
    }

    /// Returns the resource's current generation, or `None` if the registry
    /// has no exposure recorded for it.
    ///
    /// Advisory only: acting on it requires [`Self::try_pin`], which rechecks
    /// it atomically.
    pub fn generation_of(&self, resource: ExternalRef) -> Option<u64> {
        self.state.with_tables(|tables| {
            tables
                .resources
                .get(&resource)
                .map(|record| record.generation)
        })
    }

    /// Advances the resource's generation and returns the new value, or `None`
    /// if the registry has no exposure recorded for it.
    ///
    /// This is how an owner states that the frame a caller may have observed
    /// is no longer the current one. It does not break existing pins: each pin
    /// protects the frame it was taken on, and that frame is still resident
    /// until the owner unloads it. What it does is make every later
    /// [`Self::try_pin`] that presents the old generation fail.
    pub fn invalidate(&self, resource: ExternalRef) -> Option<u64> {
        self.state.with_tables(|tables| {
            tables.resources.get_mut(&resource).map(|record| {
                record.generation = record.generation.wrapping_add(1);
                record.generation
            })
        })
    }

    /// Returns one scope's exposure.
    ///
    /// A scope the registry has never seen reports [`ScopeExposure::NONE`],
    /// which is the honest answer: it holds nothing.
    ///
    /// There is no counterpart that totals this across scopes. See the module
    /// documentation for why.
    pub fn exposure_of(&self, scope: AccountId) -> ScopeExposure {
        self.state.with_tables(|tables| {
            let resources = &tables.resources;
            let Some(record) = tables.scopes.get(&scope) else {
                return ScopeExposure::NONE;
            };
            let exposed_bytes = record
                .exposed_resources
                .keys()
                .filter_map(|resource| resources.get(resource))
                .fold(0u64, |total, record| total.saturating_add(record.bytes));
            ScopeExposure {
                lease_count: record.lease_count,
                pinned_count: record.pinned_count,
                distinct_resources: record.exposed_resources.len() as u32,
                exposed_bytes,
            }
        })
    }

    /// Returns one resource's exposure, or `None` if the registry has no
    /// exposure recorded for it.
    ///
    /// `bytes` is the resource's single size regardless of `holding_scopes`.
    pub fn resource_exposure(&self, resource: ExternalRef) -> Option<ResourceExposure> {
        self.state.with_tables(|tables| {
            tables
                .resources
                .get(&resource)
                .map(|record| ResourceExposure {
                    resource,
                    holding_scopes: record.holding_scope_count as usize,
                    bytes: record.bytes,
                    pin_count: record.pin_count,
                    generation: record.generation,
                })
        })
    }

    /// Returns the number of live leases and pins together.
    pub fn live_holders(&self) -> u32 {
        self.state.slots.in_use()
    }

    /// Returns the configured holder limit.
    pub fn holder_limit(&self) -> u32 {
        self.state.slots.limit()
    }

    /// Returns the number of distinct resources with recorded exposure.
    pub fn known_resources(&self) -> usize {
        self.state.with_tables(|tables| tables.resources.len())
    }

    /// Returns the number of scopes with recorded exposure.
    pub fn exposed_scopes(&self) -> usize {
        self.state.with_tables(|tables| tables.scopes.len())
    }
}

/// State shared by the registry and every live lease and pin.
///
/// Leases and pins hold an `Arc` of this rather than a reference to the
/// registry, so a lease is allowed to outlive the handle that created it: its
/// `Drop` must always be able to withdraw the exposure it recorded.
#[derive(Debug)]
struct HolderState {
    slots: BoundedSlots,
    holder_ids: IdSource,
    pin_ids: IdSource,
    tables: Mutex<HolderTables>,
}

impl HolderState {
    /// Runs `body` under the tables lock.
    ///
    /// Poisoning is recovered from rather than propagated: a panic in one
    /// caller must not make every later exposure withdrawal panic too, which
    /// would strand exposure for the rest of the process's life.
    fn with_tables<T>(&self, body: impl FnOnce(&mut HolderTables) -> T) -> T {
        let mut guard: MutexGuard<'_, HolderTables> = self
            .tables
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        body(&mut guard)
    }

    fn acquire_lease(
        self: &Arc<Self>,
        scope: AccountId,
        resource: ExternalRef,
        bytes: u64,
    ) -> Result<RetentionLease, CapacityError> {
        self.slots.try_acquire()?;
        let admitted = self.with_tables(|tables| {
            let record = tables
                .resources
                .entry(resource)
                .or_insert_with(|| ResourceRecord::new(bytes));
            if record.bytes != bytes {
                // The record was already there with another size; a freshly
                // inserted one always agrees, so nothing is left behind here.
                return false;
            }
            record.lease_count = record.lease_count.saturating_add(1);

            let scope_record = tables.scopes.entry(scope).or_default();
            scope_record.lease_count = scope_record.lease_count.saturating_add(1);
            let held = scope_record.exposed_resources.entry(resource).or_insert(0);
            let first_for_scope = *held == 0;
            *held = held.saturating_add(1);

            if first_for_scope {
                // `holding_scope_count` counts distinct scopes, so it moves
                // only when a scope's own refcount leaves zero.
                if let Some(record) = tables.resources.get_mut(&resource) {
                    record.holding_scope_count = record.holding_scope_count.saturating_add(1);
                }
            }
            true
        });
        if !admitted {
            self.slots.release();
            return Err(CapacityError::Unsupported {
                detail: "a resource exposed in several scopes must declare one single size",
            });
        }
        Ok(RetentionLease {
            state: Arc::clone(self),
            holder: HolderId::new(self.holder_ids.next_raw()),
            scope,
            resource,
            bytes,
            withdrawn: false,
        })
    }

    fn withdraw_lease(
        &self,
        holder: HolderId,
        scope: AccountId,
        resource: ExternalRef,
        bytes: u64,
    ) -> LeaseOutcome {
        let outcome = self.with_tables(|tables| {
            let mut scope_still_holds = false;
            let mut scope_dropped_resource = false;
            if let Some(record) = tables.scopes.get_mut(&scope) {
                record.lease_count = record.lease_count.saturating_sub(1);
                if let Some(held) = record.exposed_resources.get_mut(&resource) {
                    *held = held.saturating_sub(1);
                    if *held == 0 {
                        record.exposed_resources.remove(&resource);
                        scope_dropped_resource = true;
                    } else {
                        scope_still_holds = true;
                    }
                }
                if record.is_empty() {
                    tables.scopes.remove(&scope);
                }
            }

            let mut remaining_holding_scopes = 0usize;
            let mut remaining_pins = 0u32;
            if let Some(record) = tables.resources.get_mut(&resource) {
                record.lease_count = record.lease_count.saturating_sub(1);
                if scope_dropped_resource {
                    record.holding_scope_count = record.holding_scope_count.saturating_sub(1);
                }
                remaining_holding_scopes = record.holding_scope_count as usize;
                remaining_pins = record.pin_count;
                if record.is_forgettable() {
                    tables.resources.remove(&resource);
                }
            }

            LeaseOutcome {
                holder,
                scope,
                resource,
                bytes,
                scope_still_holds,
                remaining_holding_scopes,
                remaining_pins,
            }
        });
        self.slots.release();
        outcome
    }

    fn try_pin(
        self: &Arc<Self>,
        scope: AccountId,
        resource: ExternalRef,
        expected_generation: u64,
    ) -> Result<Pin, PinRejection> {
        self.slots
            .try_acquire()
            .map_err(|cause| PinRejection::Exhausted { cause })?;
        let admitted = self.with_tables(|tables| {
            let Some(record) = tables.resources.get_mut(&resource) else {
                return Err(PinRejection::NotResident { resource });
            };
            if record.generation != expected_generation {
                return Err(PinRejection::GenerationChanged {
                    observed: record.generation,
                });
            }
            record.pin_count = record.pin_count.saturating_add(1);
            let scope_record = tables.scopes.entry(scope).or_default();
            scope_record.pinned_count = scope_record.pinned_count.saturating_add(1);
            Ok(())
        });
        match admitted {
            Ok(()) => Ok(Pin {
                state: Arc::clone(self),
                pin: PinId::new(self.pin_ids.next_raw()),
                scope,
                resource,
                generation: expected_generation,
                withdrawn: false,
            }),
            Err(rejection) => {
                // No pin was recorded, so the reserved slot goes straight
                // back: a refused pin must cost the registry nothing.
                self.slots.release();
                Err(rejection)
            }
        }
    }

    fn release_pin(&self, pin: PinId, scope: AccountId, resource: ExternalRef) -> PinOutcome {
        let outcome = self.with_tables(|tables| {
            let mut eligibility = UnloadEligibility::BecameUnloadable;
            if let Some(record) = tables.resources.get_mut(&resource) {
                record.pin_count = record.pin_count.saturating_sub(1);
                eligibility = if record.pin_count == 0 {
                    UnloadEligibility::BecameUnloadable
                } else {
                    UnloadEligibility::StillPinned {
                        remaining_pins: record.pin_count,
                    }
                };
                if record.is_forgettable() {
                    tables.resources.remove(&resource);
                }
            }
            if let Some(record) = tables.scopes.get_mut(&scope) {
                record.pinned_count = record.pinned_count.saturating_sub(1);
                if record.is_empty() {
                    tables.scopes.remove(&scope);
                }
            }
            PinOutcome {
                pin,
                scope,
                resource,
                eligibility,
            }
        });
        self.slots.release();
        outcome
    }
}

/// The registry's two indexes.
///
/// The scope index is the only authority on which scope holds what; the
/// resource index keeps derived counters that are updated in the same critical
/// section, so the two can never be read mid-update.
#[derive(Debug, Default)]
struct HolderTables {
    resources: BTreeMap<ExternalRef, ResourceRecord>,
    scopes: BTreeMap<AccountId, ScopeRecord>,
}

/// One resource's single size, generation and live claim counts.
#[derive(Debug)]
struct ResourceRecord {
    bytes: u64,
    generation: u64,
    lease_count: u32,
    holding_scope_count: u32,
    pin_count: u32,
}

impl ResourceRecord {
    const fn new(bytes: u64) -> Self {
        Self {
            bytes,
            generation: 0,
            lease_count: 0,
            holding_scope_count: 0,
            pin_count: 0,
        }
    }

    /// A resource is forgotten only when nothing claims it any more. Keeping
    /// it while a pin remains is what lets a pin outlive every lease.
    const fn is_forgettable(&self) -> bool {
        self.lease_count == 0 && self.pin_count == 0
    }
}

/// One scope's live claims.
#[derive(Debug, Default)]
struct ScopeRecord {
    lease_count: u32,
    pinned_count: u32,
    exposed_resources: BTreeMap<ExternalRef, u32>,
}

impl ScopeRecord {
    fn is_empty(&self) -> bool {
        self.lease_count == 0 && self.pinned_count == 0 && self.exposed_resources.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const SCOPE_A: AccountId = AccountId::new(1);
    const SCOPE_B: AccountId = AccountId::new(2);
    const FRAME: ExternalRef = ExternalRef::new(0, 0x0f_a1);

    fn registry() -> HolderRegistry {
        HolderRegistry::new(&MetadataBudget::with_defaults())
    }

    #[test]
    fn a_lease_records_exposure_for_its_own_scope_only() {
        let registry = registry();
        let lease = registry
            .acquire_lease(SCOPE_A, FRAME, 8_192)
            .expect("the default budget admits a lease");
        assert_eq!(lease.scope(), SCOPE_A);
        assert_eq!(lease.bytes(), 8_192);

        let exposure = registry.exposure_of(SCOPE_A);
        assert_eq!(exposure.lease_count, 1);
        assert_eq!(exposure.distinct_resources, 1);
        assert_eq!(exposure.exposed_bytes, 8_192);
        assert_eq!(registry.exposure_of(SCOPE_B), ScopeExposure::NONE);
    }

    #[test]
    fn two_leases_in_one_scope_expose_the_size_once() {
        let registry = registry();
        let _first = registry
            .acquire_lease(SCOPE_A, FRAME, 8_192)
            .expect("first");
        let _second = registry
            .acquire_lease(SCOPE_A, FRAME, 8_192)
            .expect("second");
        let exposure = registry.exposure_of(SCOPE_A);
        assert_eq!(exposure.lease_count, 2);
        assert_eq!(exposure.distinct_resources, 1);
        assert_eq!(
            exposure.exposed_bytes, 8_192,
            "exposed bytes sum distinct resources, not leases"
        );
        assert_eq!(registry.live_holders(), 2);
    }

    #[test]
    fn a_second_size_for_one_resource_is_refused_and_costs_no_slot() {
        let registry = registry();
        let _held = registry
            .acquire_lease(SCOPE_A, FRAME, 8_192)
            .expect("first");
        let refused = registry
            .acquire_lease(SCOPE_B, FRAME, 4_096)
            .expect_err("a resource cannot have two sizes");
        assert!(matches!(refused, CapacityError::Unsupported { .. }));
        assert_eq!(registry.live_holders(), 1);
        assert_eq!(registry.exposure_of(SCOPE_B), ScopeExposure::NONE);
    }

    #[test]
    fn invalidate_moves_the_generation_without_breaking_a_live_pin() {
        let registry = registry();
        let lease = registry.acquire_lease(SCOPE_A, FRAME, 512).expect("lease");
        let pin = lease.try_pin(0).expect("the first generation is zero");
        assert_eq!(pin.generation(), 0);

        assert_eq!(registry.invalidate(FRAME), Some(1));
        let exposure = registry.resource_exposure(FRAME).expect("known resource");
        assert_eq!(exposure.generation, 1);
        assert_eq!(exposure.pin_count, 1);
        assert!(!exposure.is_unloadable());
        assert_eq!(pin.generation(), 0, "the pin still names its own frame");
    }

    #[test]
    fn a_pin_can_outlive_every_lease_on_its_resource() {
        let registry = registry();
        let lease = registry.acquire_lease(SCOPE_A, FRAME, 512).expect("lease");
        let pin = lease.try_pin(0).expect("pin");
        let outcome = lease.withdraw();
        assert!(outcome.became_unheld());
        assert_eq!(outcome.remaining_pins(), 1);

        let exposure = registry
            .resource_exposure(FRAME)
            .expect("a pinned resource stays known");
        assert_eq!(exposure.holding_scopes, 0);
        assert_eq!(exposure.pin_count, 1);

        drop(pin);
        assert_eq!(registry.resource_exposure(FRAME), None);
        assert_eq!(registry.live_holders(), 0);
    }

    #[test]
    fn pinning_an_unknown_resource_is_refused_without_reserving_a_slot() {
        let registry = registry();
        let refused = registry
            .try_pin(SCOPE_A, FRAME, 0)
            .expect_err("nothing exposes this resource");
        assert_eq!(refused, PinRejection::NotResident { resource: FRAME });
        assert_eq!(registry.live_holders(), 0);
    }

    #[test]
    fn dropping_the_registry_handle_leaves_live_leases_working() {
        let registry = registry();
        let lease = registry.acquire_lease(SCOPE_A, FRAME, 64).expect("lease");
        let mirror = registry.clone();
        drop(registry);
        assert_eq!(mirror.exposure_of(SCOPE_A).exposed_bytes, 64);
        drop(lease);
        assert_eq!(mirror.exposure_of(SCOPE_A), ScopeExposure::NONE);
        assert_eq!(mirror.known_resources(), 0);
        assert_eq!(mirror.exposed_scopes(), 0);
    }

    #[test]
    fn pin_and_lease_outcomes_report_no_capacity_movement() {
        let registry = registry();
        let lease = registry
            .acquire_lease(SCOPE_A, FRAME, 4_096)
            .expect("lease");
        let pin = lease.try_pin(0).expect("pin");
        let pin_outcome = pin.release();
        assert_eq!(pin_outcome.released_bytes(), 0);
        assert_eq!(pin_outcome.capacity_returned_bytes(), 0);
        assert_eq!(
            pin_outcome.eligibility(),
            UnloadEligibility::BecameUnloadable
        );
        let lease_outcome = lease.withdraw();
        assert_eq!(lease_outcome.capacity_returned_bytes(), 0);
        assert_eq!(lease_outcome.bytes(), 4_096);
    }
}
