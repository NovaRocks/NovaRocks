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

use std::sync::Arc;

use bytes::Bytes;
use sha2::{Digest, Sha256};

use super::{
    ConnectorBatchReader, ConnectorError, ConnectorErrorKind, ConnectorExecutionDeclaration,
    ConnectorInstanceId, ConnectorInstanceIncarnation, ConnectorOpenReaderRequest,
    ConnectorProviderId, ConnectorRequestContext, ConnectorScanUnitDomainFacts,
    ConnectorScanUnitFactsSummary, ConnectorSplit, ConnectorWriteExecution,
};

/// A hard bound on the independently schedulable physical leaves carried by
/// one frontend-frozen connector split. This is deliberately independent of
/// the native carrier: providers must fail preparation rather than truncate a
/// sealed membership.
pub const MAX_CONNECTOR_PREPARED_SCAN_UNITS_PER_SPLIT: usize = 4096;

/// Immutable identity shared across FE control and BE execution processes.
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct ConnectorExecutionBindingKey {
    pub instance_id: ConnectorInstanceId,
    pub incarnation: ConnectorInstanceIncarnation,
}

/// Provider-private bytes for one leaf of a prepared local scan set.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ConnectorPreparedScanUnitDescriptor {
    payload: Bytes,
    estimated_bytes: Option<u64>,
    domain_facts: ConnectorScanUnitDomainFacts,
}

impl ConnectorPreparedScanUnitDescriptor {
    pub fn try_new(
        payload: Bytes,
        estimated_bytes: Option<u64>,
        domain_facts: ConnectorScanUnitDomainFacts,
    ) -> Result<Self, ConnectorError> {
        if payload.is_empty() {
            return Err(ConnectorError::new(
                ConnectorErrorKind::InvalidRequest,
                "connector prepared scan unit payload must not be empty",
            ));
        }
        Ok(Self {
            payload,
            estimated_bytes,
            domain_facts,
        })
    }

    pub fn payload(&self) -> &Bytes {
        &self.payload
    }

    pub const fn estimated_bytes(&self) -> Option<u64> {
        self.estimated_bytes
    }

    pub fn domain_facts(&self) -> &ConnectorScanUnitDomainFacts {
        &self.domain_facts
    }
}

struct PreparedScanUnitData {
    payload: Bytes,
    estimated_bytes: Option<u64>,
    domain_facts: ConnectorScanUnitDomainFacts,
}

struct PreparedScanUnitSetInner {
    binding_key: ConnectorExecutionBindingKey,
    split_id: Arc<str>,
    membership_digest: [u8; 32],
    shared_payload: Bytes,
    units: Vec<PreparedScanUnitData>,
    preparation_leaf_kind: Option<Arc<str>>,
}

/// Immutable, bounded and non-empty local membership materialized by one BE
/// provider from one frontend-frozen split.
#[derive(Clone)]
pub struct ConnectorPreparedScanUnitSet {
    inner: Arc<PreparedScanUnitSetInner>,
}

impl std::fmt::Debug for ConnectorPreparedScanUnitSet {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ConnectorPreparedScanUnitSet")
            .field("binding_key", &self.inner.binding_key)
            .field("split_id", &self.inner.split_id)
            .field("membership_digest", &self.inner.membership_digest)
            .field("unit_count", &self.inner.units.len())
            .finish_non_exhaustive()
    }
}

impl ConnectorPreparedScanUnitSet {
    pub fn try_new(
        binding_key: ConnectorExecutionBindingKey,
        split: &ConnectorSplit,
        shared_payload: Bytes,
        descriptors: Vec<ConnectorPreparedScanUnitDescriptor>,
        request: &ConnectorPrepareSplitRequest,
    ) -> Result<Self, ConnectorError> {
        Self::try_new_with_preparation_evidence(
            binding_key,
            split,
            shared_payload,
            descriptors,
            None,
            request,
        )
    }

    /// Constructs a sealed set with an optional provider-defined, bounded
    /// observability label. This label is never part of reader authorization
    /// or membership identity and must not expose provider payload contents.
    pub fn try_new_with_preparation_evidence(
        binding_key: ConnectorExecutionBindingKey,
        split: &ConnectorSplit,
        shared_payload: Bytes,
        descriptors: Vec<ConnectorPreparedScanUnitDescriptor>,
        leaf_kind: Option<&str>,
        request: &ConnectorPrepareSplitRequest,
    ) -> Result<Self, ConnectorError> {
        request.check_active()?;
        let preparation_leaf_kind = leaf_kind
            .map(|leaf_kind| {
                (!leaf_kind.is_empty()
                    && leaf_kind.len() <= 64
                    && leaf_kind
                        .bytes()
                        .all(|byte| byte.is_ascii_lowercase() || byte == b'_'))
                .then(|| Arc::<str>::from(leaf_kind))
                .ok_or_else(|| {
                    ConnectorError::new(
                        ConnectorErrorKind::InvalidRequest,
                        "connector prepared scan unit leaf kind must be bounded lowercase ASCII",
                    )
                })
            })
            .transpose()?;
        if split.owner() != &binding_key.instance_id {
            return Err(ConnectorError::new(
                ConnectorErrorKind::InvalidRequest,
                "connector split belongs to another execution binding",
            ));
        }
        if shared_payload.len() > request.context.max_handle_payload_bytes() {
            return Err(ConnectorError::new(
                ConnectorErrorKind::ResourceExhausted,
                "connector prepared scan unit shared payload exceeds the handle budget",
            ));
        }
        if descriptors.is_empty() || descriptors.len() > MAX_CONNECTOR_PREPARED_SCAN_UNITS_PER_SPLIT
        {
            return Err(ConnectorError::new(
                ConnectorErrorKind::InvalidRequest,
                "connector prepared scan unit set must be non-empty and within the unit limit",
            ));
        }
        let mut aggregate_payload_bytes = shared_payload.len();
        let mut all_costs_known = true;
        let mut total_cost = 0_u64;
        let mut units = Vec::with_capacity(descriptors.len());
        for descriptor in descriptors {
            request.check_active()?;
            if descriptor.payload.len() > request.context.max_handle_payload_bytes() {
                return Err(ConnectorError::new(
                    ConnectorErrorKind::ResourceExhausted,
                    "connector prepared scan unit payload exceeds the handle budget",
                ));
            }
            aggregate_payload_bytes = aggregate_payload_bytes
                .checked_add(descriptor.payload.len())
                .ok_or_else(|| {
                    ConnectorError::new(
                        ConnectorErrorKind::ResourceExhausted,
                        "connector prepared scan unit payload accounting overflowed",
                    )
                })?;
            if aggregate_payload_bytes > request.context.max_total_payload_bytes() {
                return Err(ConnectorError::new(
                    ConnectorErrorKind::ResourceExhausted,
                    "connector prepared scan unit set exceeds the aggregate payload budget",
                ));
            }
            match descriptor.estimated_bytes {
                Some(bytes) => {
                    total_cost = total_cost.checked_add(bytes).ok_or_else(|| {
                        ConnectorError::new(
                            ConnectorErrorKind::ResourceExhausted,
                            "connector prepared scan unit cost overflowed",
                        )
                    })?;
                }
                None => all_costs_known = false,
            }
            units.push(PreparedScanUnitData {
                payload: descriptor.payload,
                estimated_bytes: descriptor.estimated_bytes,
                domain_facts: descriptor.domain_facts,
            });
        }
        match (all_costs_known, split.estimated_bytes()) {
            (true, Some(split_cost)) if split_cost == total_cost => {}
            (true, _) => {
                return Err(ConnectorError::new(
                    ConnectorErrorKind::InvalidRequest,
                    "connector prepared scan unit costs do not equal the split cost",
                ));
            }
            (false, None) => {}
            (false, Some(_)) => {
                return Err(ConnectorError::new(
                    ConnectorErrorKind::InvalidRequest,
                    "connector split cost must be unknown when any prepared unit cost is unknown",
                ));
            }
        }
        let membership_digest =
            membership_digest(&binding_key, split.split_id(), &shared_payload, &units);
        Ok(Self {
            inner: Arc::new(PreparedScanUnitSetInner {
                binding_key,
                split_id: Arc::from(split.split_id()),
                membership_digest,
                shared_payload,
                units,
                preparation_leaf_kind,
            }),
        })
    }

    pub fn binding_key(&self) -> &ConnectorExecutionBindingKey {
        &self.inner.binding_key
    }

    pub fn split_id(&self) -> &str {
        &self.inner.split_id
    }

    pub fn membership_digest(&self) -> &[u8; 32] {
        &self.inner.membership_digest
    }

    pub fn shared_payload(&self) -> &Bytes {
        &self.inner.shared_payload
    }

    pub fn len(&self) -> usize {
        self.inner.units.len()
    }

    pub fn preparation_shape(&self) -> &'static str {
        if self.len() == 1 {
            "single"
        } else {
            "one_to_many"
        }
    }

    pub fn preparation_leaf_kind(&self) -> Option<&str> {
        self.inner.preparation_leaf_kind.as_deref()
    }

    pub fn is_empty(&self) -> bool {
        false
    }

    /// Summarizes immutable domain-fact availability without exposing or
    /// interpreting any provider value range.
    pub fn facts_summary(&self) -> ConnectorScanUnitFactsSummary {
        let mut summary = ConnectorScanUnitFactsSummary::default();
        for unit in self.units() {
            summary.combine(unit.domain_facts().summary());
        }
        summary
    }

    pub fn units(&self) -> impl ExactSizeIterator<Item = ConnectorPreparedScanUnit> + '_ {
        (0..self.inner.units.len()).map(|ordinal| ConnectorPreparedScanUnit {
            inner: Arc::clone(&self.inner),
            ordinal: ordinal as u32,
        })
    }
}

/// An unforgeable unit handle tied to one sealed set. The ordinal is created
/// only by [`ConnectorPreparedScanUnitSet::units`].
#[derive(Clone)]
pub struct ConnectorPreparedScanUnit {
    inner: Arc<PreparedScanUnitSetInner>,
    ordinal: u32,
}

impl std::fmt::Debug for ConnectorPreparedScanUnit {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ConnectorPreparedScanUnit")
            .field("split_id", &self.inner.split_id)
            .field("ordinal", &self.ordinal)
            .field("membership_digest", &self.inner.membership_digest)
            .finish_non_exhaustive()
    }
}

impl ConnectorPreparedScanUnit {
    fn data(&self) -> &PreparedScanUnitData {
        &self.inner.units[self.ordinal as usize]
    }

    pub fn binding_key(&self) -> &ConnectorExecutionBindingKey {
        &self.inner.binding_key
    }

    pub fn split_id(&self) -> &str {
        &self.inner.split_id
    }

    pub const fn ordinal(&self) -> u32 {
        self.ordinal
    }

    pub fn membership_digest(&self) -> &[u8; 32] {
        &self.inner.membership_digest
    }

    pub fn shared_payload(&self) -> &Bytes {
        &self.inner.shared_payload
    }

    pub fn payload(&self) -> &Bytes {
        &self.data().payload
    }

    pub fn estimated_bytes(&self) -> Option<u64> {
        self.data().estimated_bytes
    }

    /// Immutable facts sealed with this exact local membership and reader payload.
    pub fn domain_facts(&self) -> &ConnectorScanUnitDomainFacts {
        &self.data().domain_facts
    }
}

#[derive(Clone)]
pub struct ConnectorPrepareSplitRequest {
    pub context: ConnectorRequestContext,
}

impl ConnectorPrepareSplitRequest {
    pub fn check_active(&self) -> Result<(), ConnectorError> {
        if self.context.cancellation().is_cancelled() {
            return Err(ConnectorError::new(
                ConnectorErrorKind::Cancelled,
                "connector split preparation was cancelled",
            ));
        }
        if std::time::Instant::now() >= self.context.deadline() {
            return Err(ConnectorError::new(
                ConnectorErrorKind::DeadlineExceeded,
                "connector split preparation deadline elapsed",
            ));
        }
        Ok(())
    }
}

fn membership_digest(
    binding_key: &ConnectorExecutionBindingKey,
    split_id: &str,
    shared_payload: &Bytes,
    units: &[PreparedScanUnitData],
) -> [u8; 32] {
    let mut hasher = Sha256::new();
    hasher.update(binding_key.instance_id.as_str().as_bytes());
    hasher.update(binding_key.incarnation.to_bytes());
    digest_bytes(&mut hasher, split_id.as_bytes());
    digest_bytes(&mut hasher, shared_payload);
    hasher.update((units.len() as u64).to_le_bytes());
    for unit in units {
        digest_bytes(&mut hasher, &unit.payload);
        match unit.estimated_bytes {
            Some(bytes) => {
                hasher.update([1]);
                hasher.update(bytes.to_le_bytes());
            }
            None => hasher.update([0]),
        }
    }
    hasher.finalize().into()
}

fn digest_bytes(hasher: &mut Sha256, value: &[u8]) {
    hasher.update((value.len() as u64).to_le_bytes());
    hasher.update(value);
}

/// BE-only read capability. A provider implementation cannot perform metadata
/// lookup or split planning through this trait.
pub trait ConnectorReadExecution: Send + Sync {
    fn binding_key(&self) -> &ConnectorExecutionBindingKey;

    fn prepare_split(
        &self,
        split: &ConnectorSplit,
        request: ConnectorPrepareSplitRequest,
    ) -> Result<ConnectorPreparedScanUnitSet, ConnectorError>;

    fn open_unit_reader(
        &self,
        unit: &ConnectorPreparedScanUnit,
        request: ConnectorOpenReaderRequest,
    ) -> Result<Box<dyn ConnectorBatchReader>, ConnectorError>;
}

/// Startup-composed BE execution binding. The provider ID is retained only to
/// validate installer output and for redacted diagnostics; it never travels in
/// a fragment carrier.
pub struct ConnectorExecutionBinding {
    provider_id: ConnectorProviderId,
    key: ConnectorExecutionBindingKey,
    read: Option<Arc<dyn ConnectorReadExecution>>,
    write: Option<Arc<dyn ConnectorWriteExecution>>,
}

impl ConnectorExecutionBinding {
    pub fn try_new(
        provider_id: ConnectorProviderId,
        key: ConnectorExecutionBindingKey,
        read: Arc<dyn ConnectorReadExecution>,
    ) -> Result<Self, ConnectorError> {
        Self::try_new_capabilities(provider_id, key, Some(read), None)
    }

    pub fn try_new_capabilities(
        provider_id: ConnectorProviderId,
        key: ConnectorExecutionBindingKey,
        read: Option<Arc<dyn ConnectorReadExecution>>,
        write: Option<Arc<dyn ConnectorWriteExecution>>,
    ) -> Result<Self, ConnectorError> {
        if read.is_none() && write.is_none() {
            return Err(ConnectorError::new(
                ConnectorErrorKind::InvalidRequest,
                "connector execution binding requires at least one capability",
            ));
        }
        if read.as_ref().is_some_and(|read| read.binding_key() != &key) {
            return Err(ConnectorError::new(
                ConnectorErrorKind::InvalidRequest,
                "connector read execution owner does not match its execution binding",
            ));
        }
        if write
            .as_ref()
            .is_some_and(|write| write.binding_key() != &key)
        {
            return Err(ConnectorError::new(
                ConnectorErrorKind::InvalidRequest,
                "connector write execution owner does not match its execution binding",
            ));
        }
        Ok(Self {
            provider_id,
            key,
            read,
            write,
        })
    }

    pub fn provider_id(&self) -> &ConnectorProviderId {
        &self.provider_id
    }

    pub fn key(&self) -> &ConnectorExecutionBindingKey {
        &self.key
    }

    pub fn read(&self) -> Option<&Arc<dyn ConnectorReadExecution>> {
        self.read.as_ref()
    }

    pub fn write(&self) -> Option<&Arc<dyn ConnectorWriteExecution>> {
        self.write.as_ref()
    }
}

/// Startup-composed provider factory. Implementations use only local process
/// bindings for credentials and clients; declaration payloads are opaque,
/// bounded facts from the control plane.
pub trait ConnectorExecutionInstaller: Send + Sync {
    fn provider_id(&self) -> &ConnectorProviderId;

    fn install(
        &self,
        declaration: &ConnectorExecutionDeclaration,
        context: &ConnectorRequestContext,
    ) -> Result<ConnectorExecutionBinding, ConnectorError>;
}

/// A resolver scoped to one admitted BE query. Generic native decode receives
/// only this interface and therefore cannot install or select providers.
pub trait ConnectorExecutionResolver: Send + Sync {
    fn resolve(
        &self,
        key: &ConnectorExecutionBindingKey,
    ) -> Result<Arc<ConnectorExecutionBinding>, ConnectorError>;
}
