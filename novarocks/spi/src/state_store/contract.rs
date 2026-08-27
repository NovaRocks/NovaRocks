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

use super::error::{StateStoreError, StateStoreErrorKind};
use super::limits::{MAX_KEY_BYTES, MAX_VALUE_BYTES, StateStoreLimits};
use super::metrics::StateStoreMetricsSnapshot;
use super::range::{ChangeCursor, ContinuationToken, RangeRequest};

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

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct TransactionId(Uuid);

impl TransactionId {
    pub const fn as_uuid(&self) -> &Uuid {
        &self.0
    }
}

impl From<Uuid> for TransactionId {
    fn from(value: Uuid) -> Self {
        Self(value)
    }
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
    pub transaction_id: TransactionId,
    pub revision: StoreRevision,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum CommitResolution {
    Committed(CommitReceipt),
    NotCommitted,
    Unresolved,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum CommitOutcome {
    Committed(CommitReceipt),
    Conflict(StateStoreError),
    TransientBeforeCommit(StateStoreError),
    DefiniteFailure(StateStoreError),
    CommitUnknown(StateStoreError),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ChangeHint {
    pub revision: StoreRevision,
    pub key: Key,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ChangePollRequest {
    pub after: Option<ChangeCursor>,
    pub page_size: usize,
}

impl ChangePollRequest {
    pub fn validate(&self, limits: &StateStoreLimits) -> Result<(), StateStoreError> {
        validate_page_size(self.page_size, limits.max_page_size)
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ChangePage {
    pub hints: Vec<ChangeHint>,
    pub next_cursor: ChangeCursor,
    pub high_watermark: StoreRevision,
    pub resync_required: bool,
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
    fn transaction_id(&self) -> &TransactionId;
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
    fn metrics_snapshot(&self) -> StateStoreMetricsSnapshot;
    async fn begin_read(&self) -> Result<Box<dyn ReadTransaction>, StateStoreError>;
    async fn begin_write(
        &self,
        transaction_id: TransactionId,
        purpose: &str,
    ) -> Result<Box<dyn WriteTransaction>, StateStoreError>;
    async fn poll_changes(
        &self,
        request: &ChangePollRequest,
    ) -> Result<ChangePage, StateStoreError>;
    async fn identity(&self) -> Result<StoreIdentity, StateStoreError>;
    async fn resolve_commit(
        &self,
        transaction_id: &TransactionId,
    ) -> Result<CommitResolution, StateStoreError>;
}
