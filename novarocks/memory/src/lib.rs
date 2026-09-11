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

//! The neutral memory core: one process-local capacity authority.
//!
//! Design: ADR-0148 (docs/adr/ADR-0148-process-memory-capacity-authority.md)
//!
//! # What this crate is
//!
//! One process has one authority. The authority owns a strict tree of accounts
//! and issues capacity that cannot be granted twice. Four facts are kept
//! separate and are never added into one number:
//!
//! - `L`, known live allocation: backing an owner has proven and still holds.
//! - `F`, unfulfilled grants: issued rights that may still be fulfilled,
//!   including sub-grants handed down and not yet returned.
//! - `O`, third-party remaining upper bound: authorised coverage for a short
//!   bounded step that cannot yet be expressed as known `L`.
//! - `C = L + F + O`, the total commitment, kept at or under the managed
//!   capacity `B`.
//!
//! Holder exposure, reclaim estimates and physical pressure are separate
//! again: they may be displayed per scope but never summed into `C`.
//!
//! # What this crate is not
//!
//! It does not wait. Every operation either grants, fulfils, moves or refuses,
//! and returns immediately with a typed answer. Queueing, priority, reclaim
//! orchestration and kill requests belong to an arbitrator that lives with the
//! workload governance owner; this crate only defines the contract types that
//! arbitrator implements ([`wait`], [`reclaim`], [`pressure`]), so an execution
//! consumer never has to depend on a governance crate.
//!
//! It is also not a physical-memory guarantee. `C <= B` covers the declared
//! hard-governed set. Everything else — a third party's internals, a native
//! library, allocator retention and fragmentation — is measured by the
//! observation tier in [`observe`] against a headroom budget, and reported
//! with its coverage and its blind spots rather than pretended away.
//!
//! # Dependencies
//!
//! None, by design. No Arrow, no SQL, no query context, no work scope, no
//! connector error, no scheduler, no async runtime. Arrow buffer ownership
//! lives in the separate `novarocks-memory-arrow` adapter, and the dependency
//! guard in `tools/ci/check-memory-dependency-boundary.py` enforces the split.

pub mod account;
pub mod authority;
pub mod bound;
pub mod budget;
pub mod charge;
pub mod error;
pub mod grant;
pub mod holder;
pub mod ids;
pub mod observe;
pub mod policy;
pub mod pressure;
pub mod reclaim;
pub mod snapshot;
pub mod wait;

pub use account::{AccountHandle, ShrinkOutcome, TopUpPolicy};
pub use authority::{AuthorityConfig, MemoryAuthority};
pub use bound::{ConvertError, ExternalBound, SetRelation};
pub use budget::{BoundedSlots, MetadataBudget};
pub use charge::{Charge, ChargeState};
pub use error::{
    CapacityError, ConfigError, ConstraintKind, FulfilError, MetadataRegistryLabel, TransferError,
};
pub use grant::CapacityGrant;
pub use ids::{
    AccountId, AccountKind, ConfigVersion, ExternalRef, GrantId, HolderId, PinId, PolicyVersion,
    ReclaimTicketId, ReclaimerId, WaitTicketId,
};
pub use policy::{LimitDimension, LimitUnit, PolicyInstallOutcome, PolicyLimit};
pub use snapshot::{
    AccountSnapshot, AuthoritySnapshot, EventBatch, EventRing, MemoryEvent, MemoryEventKind,
};

// The holder and reclaim registries, the wait and pressure contracts and the
// observation tier keep their module paths at the root. `holder::Pin` would
// read as a second `std::pin::Pin` here, the pressure and coverage vocabularies
// are deliberately two different things, and a caller reaching for the
// observation tier benefits from saying so:
//
// ```text
// novarocks_memory::holder::{HolderRegistry, Pin, RetentionLease}
// novarocks_memory::reclaim::{Reclaimer, ReclaimRegistry, ReclaimOutcome}
// novarocks_memory::wait::{WaitTicket, WaitState}
// novarocks_memory::pressure::{PressureSample, PressureLevel}
// novarocks_memory::observe::{CountingAllocator, AllocatorSnapshot, CoverageDescriptor}
// ```
pub use observe::{AllocatorSnapshot, CountingAllocator, CoverageDescriptor};
