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

//! Provider-neutral port for the lake-sourced MV Accelerator.
//!
//! The repository owns one closed rebuildable family. Runtime attempts,
//! scheduler state and partition freshness remain with their runtime owners.
//! Physical source custody stays with the caller; an uncertain repository
//! result retains the same issued commit observation rather than a display id.

use std::fmt;

use novarocks_state_store_api::VersionToken;
use serde::{Deserialize, Serialize};

use crate::persistence::definition::MvDesiredRefreshPolicy;
pub use crate::persistence::dependency::CreateMvDependencyRequest;
use crate::persistence::validation::PersistenceDecodeBudget;
use crate::product::MvTarget;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum MvRepositoryErrorKind {
    InvalidRequest,
    NotFound,
    Conflict,
    Corruption,
    Unavailable,
    CommitUnknown,
}

#[derive(Clone, Debug)]
pub struct MvRepositoryError {
    kind: MvRepositoryErrorKind,
    message: String,
    original: Option<novarocks_failure_contract::SharedFailureCause>,
    observation: Option<novarocks_state_store_api::CommitObservation>,
}

impl MvRepositoryError {
    pub fn new(kind: MvRepositoryErrorKind, message: impl Into<String>) -> Self {
        Self {
            kind,
            message: message.into(),
            original: None,
            observation: None,
        }
    }

    pub(crate) fn with_original_failure(
        mut self,
        original: novarocks_failure_contract::SharedFailureCause,
    ) -> Self {
        self.original = Some(original);
        self
    }
    pub(crate) fn with_commit_observation(
        mut self,
        observation: novarocks_state_store_api::CommitObservation,
    ) -> Self {
        self.observation = Some(observation);
        self
    }
    pub fn original_failure(&self) -> Option<&novarocks_failure_contract::SharedFailureCause> {
        self.original.as_ref()
    }
    pub fn commit_observation(&self) -> Option<&novarocks_state_store_api::CommitObservation> {
        self.observation.as_ref()
    }
    pub fn take_commit_observation(
        &mut self,
    ) -> Option<novarocks_state_store_api::CommitObservation> {
        self.observation.take()
    }
    pub const fn kind(&self) -> MvRepositoryErrorKind {
        self.kind
    }

    pub fn message(&self) -> &str {
        &self.message
    }
}

impl fmt::Display for MvRepositoryError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.message)
    }
}

impl std::error::Error for MvRepositoryError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        self.original
            .as_ref()
            .map(|cause| cause as &(dyn std::error::Error + 'static))
    }
}
impl PartialEq for MvRepositoryError {
    fn eq(&self, other: &Self) -> bool {
        self.kind == other.kind
            && self.message == other.message
            && self.original == other.original
            && self.observation.as_ref().map(|value| value.id())
                == other.observation.as_ref().map(|value| value.id())
    }
}
impl Eq for MvRepositoryError {}

/// Reasons authored by the MV repository itself, within the metadata domain.
/// Provider/physical causes retain their own reason instead of using these.
pub mod failure_reason {
    use std::num::NonZeroU16;
    pub const INVALID_REQUEST: NonZeroU16 = NonZeroU16::new(910).unwrap();
    pub const NOT_FOUND: NonZeroU16 = NonZeroU16::new(911).unwrap();
    pub const CONFLICT: NonZeroU16 = NonZeroU16::new(912).unwrap();
    pub const CORRUPTION: NonZeroU16 = NonZeroU16::new(913).unwrap();
    pub const UNAVAILABLE: NonZeroU16 = NonZeroU16::new(914).unwrap();
    pub const COMMIT_UNKNOWN: NonZeroU16 = NonZeroU16::new(915).unwrap();
}
impl novarocks_failure_contract::FailureProjection for MvRepositoryError {
    fn failure_fact(&self) -> novarocks_failure_contract::FailureFact {
        use novarocks_failure_contract::{FailureClass as C, FailureFact, FailureReason, domain};
        if let Some(original) = &self.original {
            return original.failure_fact();
        }
        let (class, reason) = match self.kind {
            MvRepositoryErrorKind::InvalidRequest => (C::Data, failure_reason::INVALID_REQUEST),
            MvRepositoryErrorKind::NotFound => (C::Dependency, failure_reason::NOT_FOUND),
            MvRepositoryErrorKind::Conflict => (C::Dependency, failure_reason::CONFLICT),
            MvRepositoryErrorKind::Corruption => (C::Data, failure_reason::CORRUPTION),
            MvRepositoryErrorKind::Unavailable => (C::Dependency, failure_reason::UNAVAILABLE),
            MvRepositoryErrorKind::CommitUnknown => (C::Dependency, failure_reason::COMMIT_UNKNOWN),
        };
        FailureFact::new(class, FailureReason::new(domain::METADATA, reason))
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct MvTargetLookup {
    pub mv_id: i64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct InitialMvRefreshConfiguration {
    pub policy: MvDesiredRefreshPolicy,
    pub paused: bool,
    pub interval_ms: Option<i64>,
    pub max_staleness_ms: Option<i64>,
}

impl Default for InitialMvRefreshConfiguration {
    fn default() -> Self {
        Self {
            policy: MvDesiredRefreshPolicy::Manual,
            paused: false,
            interval_ms: None,
            max_staleness_ms: None,
        }
    }
}

/// Opaque StateStore version returned only by a successful repository read.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MvProjectionVersion(VersionToken);

impl MvProjectionVersion {
    pub(crate) fn from_store(version: VersionToken) -> Self {
        Self(version)
    }

    pub(crate) fn store_version(&self) -> &VersionToken {
        &self.0
    }
}

/// One explicit inventory promise. It is supplied by the local consumer;
/// planning and maintenance reads retain their separate repository contract.
#[derive(Clone, Copy, Debug)]
pub struct MvProjectionInventoryBound {
    pub entries: usize,
    pub snapshot_bytes: usize,
    pub raw_page_bytes: usize,
    pub single_name_bytes: usize,
    pub continuation_token_bytes: usize,
    pub decode: PersistenceDecodeBudget,
}

/// Local dependency display reads share one raw/decode page promise with a
/// complete thin classification inventory and a separate finite collector.
#[derive(Clone, Copy, Debug)]
pub struct MvDependencyReadBound {
    pub inventory: MvProjectionInventoryBound,
    pub entries: usize,
    pub collection_bytes: usize,
}

/// Thin identities from one complete StateStore snapshot. These are inventory
/// locators, never authority to consume an old projection or manage its target.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MvProjectionInventoryEntry {
    pub mv_id: i64,
    pub target: MvTarget,
    pub object_id: novarocks_spi::connector::ConnectorTableObjectId,
}

pub(crate) struct MvProjectionInventoryBuilder {
    entries: Vec<MvProjectionInventoryEntry>,
    payload_bytes: usize,
    bound: MvProjectionInventoryBound,
}

impl MvProjectionInventoryBuilder {
    pub(crate) fn new(bound: MvProjectionInventoryBound) -> Result<Self, MvRepositoryError> {
        if bound.entries == 0
            || bound.snapshot_bytes == 0
            || bound.raw_page_bytes == 0
            || bound.single_name_bytes == 0
            || bound.continuation_token_bytes == 0
            || bound.decode.max_working_set_bytes == 0
            || bound.decode.max_document_bytes == 0
        {
            return Err(Self::refusal(
                "MV inventory requires nonzero explicit bounds",
            ));
        }
        Ok(Self {
            entries: Vec::new(),
            payload_bytes: 0,
            bound,
        })
    }

    fn refusal(message: &'static str) -> MvRepositoryError {
        MvRepositoryError::new(MvRepositoryErrorKind::InvalidRequest, message)
    }

    pub(crate) fn push_facts(
        &mut self,
        mv_id: i64,
        target: &MvTarget,
        object: &novarocks_spi::connector::ConnectorTableObjectId,
    ) -> Result<(), MvRepositoryError> {
        if self.entries.len() >= self.bound.entries {
            return Err(Self::refusal("MV inventory exceeds its entry bound"));
        }
        if [
            target.catalog().unwrap_or_default(),
            target.namespace(),
            target.name(),
        ]
        .into_iter()
        .any(|name| name.len() > self.bound.single_name_bytes)
        {
            return Err(Self::refusal(
                "MV inventory exceeds its single name byte bound",
            ));
        }
        let payload = [
            target.catalog().map_or(0, str::len),
            target.namespace().len(),
            target.name().len(),
            object.as_bytes().len(),
            4 * 64,
        ]
        .into_iter()
        .try_fold(self.payload_bytes, usize::checked_add)
        .ok_or_else(|| Self::refusal("MV inventory snapshot size overflows"))?;
        let capacity = if self.entries.len() == self.entries.capacity() {
            self.entries
                .capacity()
                .saturating_mul(2)
                .max(1)
                .min(self.bound.entries)
        } else {
            self.entries.capacity()
        };
        // During growth both old and new vector allocations may coexist.
        let vector_slots = if capacity != self.entries.capacity() {
            capacity.checked_add(self.entries.capacity())
        } else {
            Some(capacity)
        };
        let peak = vector_slots
            .and_then(|n| n.checked_mul(std::mem::size_of::<MvProjectionInventoryEntry>()))
            .and_then(|n| n.checked_add(payload))
            .ok_or_else(|| Self::refusal("MV inventory snapshot size overflows"))?;
        if peak > self.bound.snapshot_bytes {
            return Err(Self::refusal(
                "MV inventory exceeds its snapshot byte bound",
            ));
        }
        if capacity != self.entries.capacity() {
            self.entries
                .try_reserve_exact(capacity - self.entries.len())
                .map_err(|_| Self::refusal("MV inventory allocation failed"))?;
        }
        // Copy only the opaque identity bytes. A small locator cannot retain
        // a larger decoder backing through a Bytes alias.
        let object_id = novarocks_spi::connector::ConnectorTableObjectId::try_new(
            bytes::Bytes::copy_from_slice(object.as_bytes()),
        )
        .map_err(|error| {
            MvRepositoryError::new(MvRepositoryErrorKind::Corruption, error.to_string())
        })?;
        self.entries.push(MvProjectionInventoryEntry {
            mv_id,
            target: target.clone(),
            object_id,
        });
        self.payload_bytes = payload;
        Ok(())
    }

    pub(crate) fn finish(self) -> Vec<MvProjectionInventoryEntry> {
        self.entries
    }
}
