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

//! A stable sponsor and accounting leaf for governed Arrow retention.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use novarocks_memory::account::{AccountHandle, ShrinkOutcome};
use novarocks_memory::ids::{AccountId, ExternalRef};
use novarocks_memory::reservation::ReservationMetrics;
use novarocks_memory::{CapacityError, Reservation, ReservationSnapshot};

#[derive(Debug, Clone)]
pub struct RetentionDomain {
    sponsor: AccountHandle,
    leaf: Reservation,
    census: Arc<CensusRoot>,
}

#[derive(Debug, Default)]
pub(crate) struct CensusRoot {
    pub(crate) entries: AtomicU64,
    pub(crate) sets: AtomicU64,
    pub(crate) data_bytes: AtomicU64,
    pub(crate) metadata_bytes: AtomicU64,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct DomainCensus {
    pub entries: u64,
    pub sets: u64,
    pub data_bytes: u64,
    pub metadata_bytes: u64,
}

impl RetentionDomain {
    pub fn new(sponsor: &AccountHandle, identity: ExternalRef) -> Result<Self, CapacityError> {
        Ok(Self {
            sponsor: sponsor.clone(),
            leaf: Reservation::new(sponsor, identity)?,
            census: Arc::new(CensusRoot::default()),
        })
    }

    pub fn sponsor_id(&self) -> AccountId {
        self.sponsor.id()
    }

    pub fn leaf_id(&self) -> AccountId {
        self.leaf.account_id()
    }

    pub fn snapshot(&self) -> ReservationSnapshot {
        self.leaf.snapshot()
    }

    pub fn metrics(&self) -> ReservationMetrics {
        self.leaf.metrics()
    }

    /// Diagnostic counts for this payer at a quiescent point. This root has
    /// no address index and is never consulted for admission or settlement.
    pub fn census(&self) -> DomainCensus {
        DomainCensus {
            entries: self.census.entries.load(Ordering::Acquire),
            sets: self.census.sets.load(Ordering::Acquire),
            data_bytes: self.census.data_bytes.load(Ordering::Acquire),
            metadata_bytes: self.census.metadata_bytes.load(Ordering::Acquire),
        }
    }

    pub fn trim(&self) -> ShrinkOutcome {
        self.leaf.trim()
    }

    pub fn close(&self) -> ShrinkOutcome {
        self.leaf.close()
    }

    pub(crate) fn leaf(&self) -> &Reservation {
        &self.leaf
    }

    pub(crate) fn authority_key(&self) -> usize {
        Arc::as_ptr(self.sponsor.account().shared()) as usize
    }

    pub(crate) fn census_root(&self) -> Arc<CensusRoot> {
        self.census.clone()
    }
}
