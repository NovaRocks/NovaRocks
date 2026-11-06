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

#![cfg_attr(
    test,
    expect(
        clippy::disallowed_methods,
        reason = "Unit tests exercise thread/task mechanics under their original test owners; the production library is linted separately; see ADR-0218."
    )
)]

//! Carrier-neutral hierarchical memory accounting. Funded boundaries are
//! local; independent funding domains retain independent redemption rights.
//! Allocation facts may exceed backing and remain charged after teardown.
//! The observation tier is independent of allocation attribution.
pub mod account;
#[cfg(not(loom))]
pub mod attribution;
pub mod authority;
pub mod domain;
pub mod error;
pub mod ids;
pub mod lane;
mod membership;
pub mod observe;
pub mod policy;
pub mod snapshot;
mod sync;
pub use account::{ACCOUNT_METADATA_BYTES, Account, InteractionSnapshot, TopUpPolicy};
pub use authority::{AuthorityConfig, CapacityWriter, MemoryAuthority};
pub use domain::{DomainSnapshot, FundingDomain, OWNER_METADATA_BYTES};
pub use error::{CapacityError, ConfigError, ConstraintKind, MetadataRegistryLabel, Refusal};
pub use ids::{AccountId, AccountKind, ConfigVersion, ExternalRef, PolicyVersion};
pub use lane::{CoverageGap, FactToken, Lane, LaneShards, ShardSnapshot, ShardsSnapshot};
pub use observe::{AllocatorSnapshot, CountingAllocator, CoverageDescriptor};
pub use policy::{LimitDimension, LimitUnit, PolicyInstallOutcome, PolicyLimit};
pub use snapshot::{AccountSnapshot, AuthoritySnapshot};

pub mod settlement;
pub mod stock;
pub use settlement::StepReceipt;
pub use stock::FundingScope;

pub mod lifecycle;
pub use lifecycle::{IoExitEvidence, TeardownError, TeardownEvidence, TransferReceipt};

pub mod maintenance;
pub use maintenance::{CoverageReceipt, MaintenanceReason, RequestOutcome, ShortageReceipt};

pub mod bound;
pub mod grant;
pub mod holder;
pub use bound::ExternalBound;
pub use grant::ExplicitGrant;
pub use holder::HolderPin;
#[cfg(all(test, loom))]
mod ledger_loom;
#[cfg(all(test, loom))]
mod owner_loom;

#[cfg(all(test, loom))]
mod lane_loom;
