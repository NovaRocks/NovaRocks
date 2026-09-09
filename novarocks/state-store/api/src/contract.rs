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

use bytes::Bytes;
use uuid::Uuid;

use super::attempt::{AttemptId, AttemptSupervisor, WriteAttempt};
use super::error::{StateStoreError, StateStoreErrorKind};
use super::limits::{MAX_KEY_BYTES, MAX_VALUE_BYTES, StateStoreLimits};
use super::range::{ContinuationToken, RangeRequest};

macro_rules! opaque_bytes {
    ($name:ident, $validate:expr) => {
        #[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
        pub struct $name(Bytes);

        impl $name {
            pub fn as_bytes(&self) -> &[u8] {
                self.0.as_ref()
            }

            pub fn into_bytes(self) -> Bytes {
                self.0
            }
        }

        impl TryFrom<Bytes> for $name {
            type Error = StateStoreError;

            fn try_from(bytes: Bytes) -> Result<Self, Self::Error> {
                ($validate)(&bytes)?;
                Ok(Self(bytes))
            }
        }
    };
}

opaque_bytes!(Key, |bytes: &Bytes| {
    validate_maximum(
        bytes.len(),
        MAX_KEY_BYTES,
        "key exceeds the common byte limit",
    )
});
opaque_bytes!(Value, |bytes: &Bytes| {
    validate_maximum(
        bytes.len(),
        MAX_VALUE_BYTES,
        "value exceeds the common byte limit",
    )
});
opaque_bytes!(VersionToken, |bytes: &Bytes| {
    validate_non_empty(bytes, "version token must not be empty")
});
opaque_bytes!(StoreRevision, |bytes: &Bytes| {
    validate_non_empty(bytes, "store revision must not be empty")
});

fn validate_maximum(
    actual: usize,
    maximum: usize,
    message: &'static str,
) -> Result<(), StateStoreError> {
    if actual > maximum {
        return Err(StateStoreError::new(
            StateStoreErrorKind::LimitExceeded,
            message,
        ));
    }
    Ok(())
}

fn validate_non_empty(bytes: &Bytes, message: &'static str) -> Result<(), StateStoreError> {
    if bytes.is_empty() {
        return Err(StateStoreError::new(
            StateStoreErrorKind::InvalidRequest,
            message,
        ));
    }
    Ok(())
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StateRecord {
    pub key: Key,
    pub value: Value,
    pub version: VersionToken,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum Precondition {
    Any,
    Absent,
    Present,
    Version(VersionToken),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RangePage {
    pub records: Vec<StateRecord>,
    pub continuation: Option<ContinuationToken>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StoreIdentity {
    pub store_id: Uuid,
    pub cluster_id: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CommitReceipt {
    pub attempt: AttemptId,
    pub revision: StoreRevision,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum CommitOutcome {
    Committed(CommitReceipt),
    Conflict(StateStoreError),
    TransientBeforeCommit(StateStoreError),
    DefiniteFailure(StateStoreError),
    CommitUnknown(StateStoreError),
}

pub(crate) fn validate_page_size(page_size: usize, maximum: usize) -> Result<(), StateStoreError> {
    if page_size == 0 || page_size > maximum {
        return Err(StateStoreError::new(
            StateStoreErrorKind::LimitExceeded,
            "page size is outside the configured limits",
        ));
    }
    Ok(())
}

#[async_trait::async_trait]
pub trait ReadTransaction: Send {
    async fn get(&mut self, key: &Key) -> Result<Option<StateRecord>, StateStoreError>;
    async fn range(&mut self, request: &RangeRequest) -> Result<RangePage, StateStoreError>;
    async fn abort(self: Box<Self>) -> Result<(), StateStoreError>;
}

#[async_trait::async_trait]
pub trait WriteTransaction: ReadTransaction {
    fn attempt(&self) -> AttemptId;
    async fn put(
        &mut self,
        key: Key,
        value: Value,
        precondition: Precondition,
    ) -> Result<(), StateStoreError>;
    async fn delete(&mut self, key: Key, precondition: Precondition)
    -> Result<(), StateStoreError>;
    async fn commit(self: Box<Self>) -> CommitOutcome;
}

#[async_trait::async_trait]
pub trait StateStore: Send + Sync {
    fn limits(&self) -> &StateStoreLimits;

    /// Issues write attempts and accounts for their capacity.
    ///
    /// A caller reserves here before it may begin a write, so responsibility
    /// exists before any work does. Observation of an attempt's outcome also
    /// goes through the handle this hands back, which is why the store itself
    /// has no "ask about an arbitrary id" method any more.
    fn attempts(&self) -> &AttemptSupervisor;

    async fn begin_read(&self) -> Result<Box<dyn ReadTransaction>, StateStoreError>;

    /// Begins the one write this attempt authorises.
    ///
    /// The attempt is consumed: a reservation buys exactly one transaction
    /// body. Failing before anything is dispatched leaves the attempt provably
    /// without effect.
    async fn begin_write(
        &self,
        attempt: WriteAttempt,
        purpose: &str,
    ) -> Result<Box<dyn WriteTransaction>, StateStoreError>;

    async fn identity(&self) -> Result<StoreIdentity, StateStoreError>;
}
