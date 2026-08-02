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

//! Provider-neutral distributed writer contract.
//!
//! The frontend owns planning and external commit state. Backend execution
//! bindings can only stage Arrow batches and return bounded opaque reports.

use std::collections::{BTreeMap, BTreeSet, HashSet};
use std::sync::{Arc, Mutex};

use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use bytes::Bytes;
use sha2::{Digest, Sha256};
use uuid::Uuid;

use super::{
    ConnectorCommittedVersion, ConnectorError, ConnectorErrorKind, ConnectorExecutionBindingKey,
    ConnectorExecutionDeclaration, ConnectorExecutionDistribution, ConnectorMutationFailure,
    ConnectorRequestContext, ConnectorTableHandle, ExternalMutationEvidence,
    ExternalMutationFinalization, ExternalMutationOutcome, MAX_CONNECTOR_HANDLE_PAYLOAD_BYTES,
    MAX_CONNECTOR_TOTAL_PAYLOAD_BYTES, MAX_EXTERNAL_MUTATION_EVIDENCE_BYTES,
};

pub const CONNECTOR_WRITE_CONTRACT_VERSION: u32 = 1;
pub const MAX_CONNECTOR_STAGED_REPORT_FRAME_BYTES: usize = 1024 * 1024;
pub const MAX_CONNECTOR_STAGED_REPORT_PARTS: u32 = 48;
pub const MAX_CONNECTOR_STAGED_REPORT_PAYLOAD_BYTES: usize =
    MAX_CONNECTOR_STAGED_REPORT_FRAME_BYTES * MAX_CONNECTOR_STAGED_REPORT_PARTS as usize;
pub const MAX_CONNECTOR_WRITE_RECEIPT_BYTES: usize = MAX_EXTERNAL_MUTATION_EVIDENCE_BYTES;
pub const MAX_CONNECTOR_WRITE_COHORTS: usize = 4096;
pub const MAX_CONNECTOR_WRITE_OPERATION_WRITERS: usize = 16_384;
pub const MAX_CONNECTOR_WRITE_OPERATION_PAYLOAD_BYTES: usize = 64 * 1024 * 1024;

const CONNECTOR_WRITE_COHORT_ID_DOMAIN: &[u8] = b"novarocks.connector-write-cohort.v1\0";
const CONNECTOR_WRITE_COHORT_SET_DOMAIN: &[u8] = b"novarocks.connector-write-cohort-set.v1\0";
const CONNECTOR_WRITE_ATTEMPT_DOMAIN: &[u8] = b"novarocks.connector-write-attempt.v1\0";
const CONNECTOR_WRITE_OPERATION_DOMAIN: &[u8] = b"novarocks.connector-write-operation.v1\0";

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct ConnectorWriteOperationId(Uuid);

impl ConnectorWriteOperationId {
    pub fn new() -> Self {
        Self(Uuid::now_v7())
    }

    pub fn from_bytes(bytes: [u8; 16]) -> Self {
        Self(Uuid::from_bytes(bytes))
    }

    pub fn to_bytes(self) -> [u8; 16] {
        *self.0.as_bytes()
    }
}

impl std::fmt::Display for ConnectorWriteOperationId {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(formatter)
    }
}

impl std::str::FromStr for ConnectorWriteOperationId {
    type Err = uuid::Error;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        Uuid::parse_str(value).map(Self)
    }
}

impl Default for ConnectorWriteOperationId {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct ConnectorWriteCohortId([u8; 32]);

impl ConnectorWriteCohortId {
    pub fn derive(
        operation_id: ConnectorWriteOperationId,
        role_tag: &[u8],
        semantic_key_digest: [u8; 32],
    ) -> Result<Self, ConnectorError> {
        if role_tag.is_empty() || role_tag.len() > 256 {
            return Err(ConnectorError::new(
                ConnectorErrorKind::InvalidRequest,
                "connector write cohort role tag must contain 1..=256 bytes",
            ));
        }
        let mut hasher = Sha256::new();
        hasher.update(CONNECTOR_WRITE_COHORT_ID_DOMAIN);
        hasher.update(operation_id.to_bytes());
        digest_bytes(&mut hasher, role_tag);
        hasher.update(semantic_key_digest);
        Ok(Self(hasher.finalize().into()))
    }

    pub fn primary(operation_id: ConnectorWriteOperationId) -> Self {
        let semantic_key_digest: [u8; 32] = Sha256::digest(b"primary").into();
        Self::derive(operation_id, b"primary", semantic_key_digest)
            .expect("the fixed primary cohort role is valid")
    }

    pub const fn from_bytes(bytes: [u8; 32]) -> Self {
        Self(bytes)
    }

    pub const fn to_bytes(self) -> [u8; 32] {
        self.0
    }
}

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct ConnectorWriteExecutionId {
    query_id: [u8; 16],
    attempt_id: u64,
}

impl ConnectorWriteExecutionId {
    pub const fn new(query_id: [u8; 16], attempt_id: u64) -> Self {
        Self {
            query_id,
            attempt_id,
        }
    }

    pub const fn query_id(self) -> [u8; 16] {
        self.query_id
    }

    pub const fn attempt_id(self) -> u64 {
        self.attempt_id
    }
}

#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct ConnectorWriterIdentity {
    operation_id: ConnectorWriteOperationId,
    cohort_id: ConnectorWriteCohortId,
    execution_id: ConnectorWriteExecutionId,
    fragment_instance_id: [u8; 16],
    fragment_id: i32,
    backend_num: i32,
    sink_ordinal: u32,
    binding_key: ConnectorExecutionBindingKey,
}

impl ConnectorWriterIdentity {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        operation_id: ConnectorWriteOperationId,
        cohort_id: ConnectorWriteCohortId,
        execution_id: ConnectorWriteExecutionId,
        fragment_instance_id: [u8; 16],
        fragment_id: i32,
        backend_num: i32,
        sink_ordinal: u32,
        binding_key: ConnectorExecutionBindingKey,
    ) -> Self {
        Self {
            operation_id,
            cohort_id,
            execution_id,
            fragment_instance_id,
            fragment_id,
            backend_num,
            sink_ordinal,
            binding_key,
        }
    }

    pub const fn operation_id(&self) -> ConnectorWriteOperationId {
        self.operation_id
    }

    pub const fn cohort_id(&self) -> ConnectorWriteCohortId {
        self.cohort_id
    }

    pub const fn execution_id(&self) -> ConnectorWriteExecutionId {
        self.execution_id
    }

    pub const fn fragment_instance_id(&self) -> [u8; 16] {
        self.fragment_instance_id
    }

    pub const fn fragment_id(&self) -> i32 {
        self.fragment_id
    }

    pub const fn backend_num(&self) -> i32 {
        self.backend_num
    }

    pub const fn sink_ordinal(&self) -> u32 {
        self.sink_ordinal
    }

    pub fn binding_key(&self) -> &ConnectorExecutionBindingKey {
        &self.binding_key
    }
}

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum ConnectorWriteIntent {
    Append,
    Overwrite,
    PartitionOverwrite,
    RowDelta,
}

#[derive(Clone)]
pub struct ConnectorWritePlanningRequest {
    pub operation_id: ConnectorWriteOperationId,
    pub cohort_id: ConnectorWriteCohortId,
    pub execution_id: ConnectorWriteExecutionId,
    pub table: ConnectorTableHandle,
    pub intent: ConnectorWriteIntent,
    pub input_schema: SchemaRef,
    pub expected_writers: Vec<ConnectorWriterIdentity>,
    pub provider_payload: Bytes,
    pub context: ConnectorRequestContext,
}

impl ConnectorWritePlanningRequest {
    pub fn validate(&self, owner: &ConnectorExecutionBindingKey) -> Result<(), ConnectorError> {
        validate_handle_payload(&self.provider_payload)?;
        if self.expected_writers.is_empty() {
            return Err(ConnectorError::new(
                ConnectorErrorKind::InvalidRequest,
                "connector write planning requires at least one expected writer",
            ));
        }
        let mut writers = HashSet::with_capacity(self.expected_writers.len());
        for writer in &self.expected_writers {
            if writer.operation_id != self.operation_id
                || writer.cohort_id != self.cohort_id
                || writer.execution_id != self.execution_id
                || &writer.binding_key != owner
            {
                return Err(ConnectorError::new(
                    ConnectorErrorKind::InvalidRequest,
                    "connector write planning writer does not match the requested owner and operation",
                ));
            }
            if !writers.insert(writer.clone()) {
                return Err(ConnectorError::new(
                    ConnectorErrorKind::InvalidRequest,
                    "connector write planning contains a duplicate writer identity",
                ));
            }
        }
        Ok(())
    }

    pub fn stable_digest(
        &self,
        owner: &ConnectorExecutionBindingKey,
    ) -> Result<[u8; 32], ConnectorError> {
        validate_handle_payload(&self.provider_payload)?;
        if self.table.owner() != &owner.instance_id {
            return Err(ConnectorError::new(
                ConnectorErrorKind::InvalidRequest,
                "connector write planning table does not match the exact control owner",
            ));
        }
        let mut hasher = Sha256::new();
        hasher.update(b"novarocks.connector-write-planning.v1\0");
        digest_owner(&mut hasher, owner);
        hasher.update(self.operation_id.to_bytes());
        hasher.update(self.cohort_id.to_bytes());
        digest_bytes(&mut hasher, self.table.owner().as_str().as_bytes());
        digest_bytes(&mut hasher, self.table.payload());
        hasher.update([write_intent_tag(self.intent)]);
        // Exact-generation replay only compares plans produced by the same
        // binary. Arrow's structural Debug form covers nested field names,
        // types, nullability and metadata without introducing a wire codec.
        digest_bytes(&mut hasher, format!("{:?}", self.input_schema).as_bytes());
        digest_bytes(&mut hasher, &self.provider_payload);
        Ok(hasher.finalize().into())
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ConnectorWriterHandle {
    owner: ConnectorExecutionBindingKey,
    writer: ConnectorWriterIdentity,
    version: u32,
    payload: Bytes,
    payload_digest: [u8; 32],
}

impl ConnectorWriterHandle {
    pub fn try_new(
        owner: ConnectorExecutionBindingKey,
        writer: ConnectorWriterIdentity,
        version: u32,
        payload: Bytes,
    ) -> Result<Self, ConnectorError> {
        validate_handle_payload(&payload)?;
        if version == 0 || writer.binding_key != owner {
            return Err(ConnectorError::new(
                ConnectorErrorKind::InvalidRequest,
                "connector writer handle owner or version is invalid",
            ));
        }
        Ok(Self {
            owner,
            writer,
            version,
            payload_digest: sha256(&payload),
            payload,
        })
    }

    pub fn validate(&self) -> Result<(), ConnectorError> {
        if self.version == 0
            || self.writer.binding_key != self.owner
            || self.payload_digest != sha256(&self.payload)
        {
            return Err(ConnectorError::new(
                ConnectorErrorKind::CorruptData,
                "connector writer handle integrity validation failed",
            ));
        }
        validate_handle_payload(&self.payload)
    }

    pub fn owner(&self) -> &ConnectorExecutionBindingKey {
        &self.owner
    }

    pub fn writer(&self) -> &ConnectorWriterIdentity {
        &self.writer
    }

    pub const fn version(&self) -> u32 {
        self.version
    }

    pub fn payload(&self) -> &Bytes {
        &self.payload
    }

    pub const fn payload_digest(&self) -> [u8; 32] {
        self.payload_digest
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ConnectorWritePlan {
    owner: ConnectorExecutionBindingKey,
    operation_id: ConnectorWriteOperationId,
    cohort_id: ConnectorWriteCohortId,
    execution_id: ConnectorWriteExecutionId,
    handles: Vec<ConnectorWriterHandle>,
    control_payload: Bytes,
}

impl ConnectorWritePlan {
    pub fn try_new(
        owner: ConnectorExecutionBindingKey,
        operation_id: ConnectorWriteOperationId,
        cohort_id: ConnectorWriteCohortId,
        execution_id: ConnectorWriteExecutionId,
        handles: Vec<ConnectorWriterHandle>,
        control_payload: Bytes,
    ) -> Result<Self, ConnectorError> {
        validate_total_handle_payload(&handles, &control_payload)?;
        let mut writers = HashSet::with_capacity(handles.len());
        for handle in &handles {
            handle.validate()?;
            if handle.owner != owner
                || handle.writer.operation_id != operation_id
                || handle.writer.cohort_id != cohort_id
                || handle.writer.execution_id != execution_id
                || !writers.insert(handle.writer.clone())
            {
                return Err(ConnectorError::new(
                    ConnectorErrorKind::InvalidRequest,
                    "connector write plan handles do not form one exact writer manifest",
                ));
            }
        }
        if handles.is_empty() {
            return Err(ConnectorError::new(
                ConnectorErrorKind::InvalidRequest,
                "connector write plan must contain at least one writer handle",
            ));
        }
        Ok(Self {
            owner,
            operation_id,
            cohort_id,
            execution_id,
            handles,
            control_payload,
        })
    }

    pub fn owner(&self) -> &ConnectorExecutionBindingKey {
        &self.owner
    }

    pub const fn operation_id(&self) -> ConnectorWriteOperationId {
        self.operation_id
    }

    pub const fn cohort_id(&self) -> ConnectorWriteCohortId {
        self.cohort_id
    }

    pub const fn execution_id(&self) -> ConnectorWriteExecutionId {
        self.execution_id
    }

    pub fn handles(&self) -> &[ConnectorWriterHandle] {
        &self.handles
    }

    pub fn control_payload(&self) -> &Bytes {
        &self.control_payload
    }
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct ConnectorStagedReportSummary {
    pub input_rows: u64,
    pub staged_bytes: u64,
    pub artifact_count: u64,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ConnectorWriterTerminalState {
    Staged,
    Aborted,
    Failed,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ConnectorStagedReport {
    writer: ConnectorWriterIdentity,
    version: u32,
    state: ConnectorWriterTerminalState,
    summary: ConnectorStagedReportSummary,
    payload: Bytes,
    payload_digest: [u8; 32],
}

impl ConnectorStagedReport {
    pub fn try_new(
        writer: ConnectorWriterIdentity,
        version: u32,
        state: ConnectorWriterTerminalState,
        summary: ConnectorStagedReportSummary,
        payload: Bytes,
    ) -> Result<Self, ConnectorError> {
        validate_report_payload(&payload)?;
        if version == 0 {
            return Err(ConnectorError::new(
                ConnectorErrorKind::InvalidRequest,
                "connector staged report version must be nonzero",
            ));
        }
        Ok(Self {
            writer,
            version,
            state,
            summary,
            payload_digest: sha256(&payload),
            payload,
        })
    }

    pub fn validate(&self) -> Result<(), ConnectorError> {
        if self.version == 0 || self.payload_digest != sha256(&self.payload) {
            return Err(ConnectorError::new(
                ConnectorErrorKind::CorruptData,
                "connector staged report integrity validation failed",
            ));
        }
        validate_report_payload(&self.payload)
    }

    pub fn frames(&self) -> Vec<ConnectorStagedReportFrame> {
        let part_count = self
            .payload
            .len()
            .max(1)
            .div_ceil(MAX_CONNECTOR_STAGED_REPORT_FRAME_BYTES) as u32;
        self.payload
            .chunks(MAX_CONNECTOR_STAGED_REPORT_FRAME_BYTES)
            .enumerate()
            .map(|(part_index, payload)| {
                ConnectorStagedReportFrame::try_new(
                    self.writer.clone(),
                    self.version,
                    self.state,
                    self.summary,
                    part_index as u32,
                    part_count,
                    self.payload.len() as u64,
                    self.payload_digest,
                    Bytes::copy_from_slice(payload),
                )
                .expect("validated connector staged report must frame")
            })
            .collect()
    }

    /// Reassemble the complete, bounded frame sequence for one logical
    /// writer report. Identical duplicate frames are accepted because report
    /// delivery is retryable; conflicting duplicates and incomplete ranges
    /// are rejected before provider payloads become visible to a consumer.
    pub fn try_from_frames(
        frames: impl IntoIterator<Item = ConnectorStagedReportFrame>,
    ) -> Result<Self, ConnectorError> {
        let mut parts = BTreeMap::new();
        let mut first: Option<ConnectorStagedReportFrame> = None;
        for frame in frames {
            frame.validate()?;
            if let Some(expected) = &first {
                if frame.writer != expected.writer
                    || frame.version != expected.version
                    || frame.state != expected.state
                    || frame.summary != expected.summary
                    || frame.part_count != expected.part_count
                    || frame.logical_payload_len != expected.logical_payload_len
                    || frame.logical_payload_digest != expected.logical_payload_digest
                {
                    return Err(ConnectorError::new(
                        ConnectorErrorKind::InvalidRequest,
                        "connector staged report frames disagree on logical report identity",
                    ));
                }
            } else {
                first = Some(frame.clone());
            }
            match parts.entry(frame.part_index) {
                std::collections::btree_map::Entry::Vacant(entry) => {
                    entry.insert(frame);
                }
                std::collections::btree_map::Entry::Occupied(entry) if entry.get() == &frame => {}
                std::collections::btree_map::Entry::Occupied(_) => {
                    return Err(ConnectorError::new(
                        ConnectorErrorKind::CorruptData,
                        "connector staged report has conflicting duplicate frame",
                    ));
                }
            }
        }
        let first = first.ok_or_else(|| {
            ConnectorError::new(
                ConnectorErrorKind::InvalidRequest,
                "connector staged report has no frames",
            )
        })?;
        if parts.len() != first.part_count as usize || parts.keys().copied().ne(0..first.part_count)
        {
            return Err(ConnectorError::new(
                ConnectorErrorKind::InvalidRequest,
                "connector staged report frame range is incomplete",
            ));
        }
        let payload_len = usize::try_from(first.logical_payload_len).map_err(|_| {
            ConnectorError::new(
                ConnectorErrorKind::ResourceExhausted,
                "connector staged report payload length does not fit usize",
            )
        })?;
        let mut payload = Vec::with_capacity(payload_len);
        for frame in parts.into_values() {
            payload.extend_from_slice(&frame.frame_payload);
        }
        if payload.len() != payload_len {
            return Err(ConnectorError::new(
                ConnectorErrorKind::CorruptData,
                "connector staged report frames do not match the declared payload length",
            ));
        }
        let report = Self::try_new(
            first.writer,
            first.version,
            first.state,
            first.summary,
            Bytes::from(payload),
        )?;
        if report.payload_digest != first.logical_payload_digest {
            return Err(ConnectorError::new(
                ConnectorErrorKind::CorruptData,
                "connector staged report frames do not match the declared payload digest",
            ));
        }
        Ok(report)
    }

    pub fn writer(&self) -> &ConnectorWriterIdentity {
        &self.writer
    }

    pub const fn version(&self) -> u32 {
        self.version
    }

    pub const fn state(&self) -> ConnectorWriterTerminalState {
        self.state
    }

    pub const fn summary(&self) -> ConnectorStagedReportSummary {
        self.summary
    }

    pub fn payload(&self) -> &Bytes {
        &self.payload
    }

    pub const fn payload_digest(&self) -> [u8; 32] {
        self.payload_digest
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ConnectorStagedReportFrame {
    writer: ConnectorWriterIdentity,
    version: u32,
    state: ConnectorWriterTerminalState,
    summary: ConnectorStagedReportSummary,
    part_index: u32,
    part_count: u32,
    logical_payload_len: u64,
    logical_payload_digest: [u8; 32],
    frame_payload: Bytes,
    frame_payload_digest: [u8; 32],
}

impl ConnectorStagedReportFrame {
    #[allow(clippy::too_many_arguments)]
    pub fn try_new(
        writer: ConnectorWriterIdentity,
        version: u32,
        state: ConnectorWriterTerminalState,
        summary: ConnectorStagedReportSummary,
        part_index: u32,
        part_count: u32,
        logical_payload_len: u64,
        logical_payload_digest: [u8; 32],
        frame_payload: Bytes,
    ) -> Result<Self, ConnectorError> {
        if version == 0
            || part_count == 0
            || part_count > MAX_CONNECTOR_STAGED_REPORT_PARTS
            || part_index >= part_count
            || logical_payload_len as usize > MAX_CONNECTOR_STAGED_REPORT_PAYLOAD_BYTES
            || frame_payload.len() > MAX_CONNECTOR_STAGED_REPORT_FRAME_BYTES
        {
            return Err(ConnectorError::new(
                ConnectorErrorKind::InvalidRequest,
                "connector staged report frame exceeds its contract bounds",
            ));
        }
        Ok(Self {
            writer,
            version,
            state,
            summary,
            part_index,
            part_count,
            logical_payload_len,
            logical_payload_digest,
            frame_payload_digest: sha256(&frame_payload),
            frame_payload,
        })
    }

    pub fn validate(&self) -> Result<(), ConnectorError> {
        let expected = Self::try_new(
            self.writer.clone(),
            self.version,
            self.state,
            self.summary,
            self.part_index,
            self.part_count,
            self.logical_payload_len,
            self.logical_payload_digest,
            self.frame_payload.clone(),
        )?;
        if expected.frame_payload_digest != self.frame_payload_digest {
            return Err(ConnectorError::new(
                ConnectorErrorKind::CorruptData,
                "connector staged report frame digest does not match its payload",
            ));
        }
        Ok(())
    }

    pub fn writer(&self) -> &ConnectorWriterIdentity {
        &self.writer
    }
    pub const fn version(&self) -> u32 {
        self.version
    }
    pub const fn state(&self) -> ConnectorWriterTerminalState {
        self.state
    }
    pub const fn summary(&self) -> ConnectorStagedReportSummary {
        self.summary
    }
    pub const fn part_index(&self) -> u32 {
        self.part_index
    }
    pub const fn part_count(&self) -> u32 {
        self.part_count
    }
    pub const fn logical_payload_len(&self) -> u64 {
        self.logical_payload_len
    }
    pub const fn logical_payload_digest(&self) -> [u8; 32] {
        self.logical_payload_digest
    }
    pub fn frame_payload(&self) -> &Bytes {
        &self.frame_payload
    }
    pub const fn frame_payload_digest(&self) -> [u8; 32] {
        self.frame_payload_digest
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ConnectorWriteReceipt {
    payload: Bytes,
    digest: [u8; 32],
    committed_version: Option<ConnectorCommittedVersion>,
    resulting_row_count: Option<u64>,
}

impl ConnectorWriteReceipt {
    pub fn try_new(payload: Bytes) -> Result<Self, ConnectorError> {
        validate_receipt_payload(&payload)?;
        Ok(Self {
            digest: sha256(&payload),
            payload,
            committed_version: None,
            resulting_row_count: None,
        })
    }

    pub fn try_new_with_committed_version(
        payload: Bytes,
        committed_version: ConnectorCommittedVersion,
    ) -> Result<Self, ConnectorError> {
        let mut receipt = Self::try_new(payload)?;
        committed_version.validate()?;
        receipt.committed_version = Some(committed_version);
        Ok(receipt)
    }

    pub fn try_new_with_committed_facts(
        payload: Bytes,
        committed_version: ConnectorCommittedVersion,
        resulting_row_count: Option<u64>,
    ) -> Result<Self, ConnectorError> {
        let mut receipt = Self::try_new_with_committed_version(payload, committed_version)?;
        receipt.resulting_row_count = resulting_row_count;
        Ok(receipt)
    }

    pub fn validate(&self) -> Result<(), ConnectorError> {
        validate_receipt_payload(&self.payload)?;
        if self.digest != sha256(&self.payload) {
            return Err(ConnectorError::new(
                ConnectorErrorKind::CorruptData,
                "connector write receipt digest does not match its payload",
            ));
        }
        if let Some(version) = &self.committed_version {
            version.validate()?;
        }
        Ok(())
    }

    pub fn payload(&self) -> &Bytes {
        &self.payload
    }
    pub const fn digest(&self) -> [u8; 32] {
        self.digest
    }
    pub fn committed_version(&self) -> Option<&ConnectorCommittedVersion> {
        self.committed_version.as_ref()
    }
    pub const fn resulting_row_count(&self) -> Option<u64> {
        self.resulting_row_count
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ConnectorWriteCohortDescriptor {
    cohort_id: ConnectorWriteCohortId,
    intent: ConnectorWriteIntent,
    planning_digest: [u8; 32],
}

impl ConnectorWriteCohortDescriptor {
    pub const fn new(
        cohort_id: ConnectorWriteCohortId,
        intent: ConnectorWriteIntent,
        planning_digest: [u8; 32],
    ) -> Self {
        Self {
            cohort_id,
            intent,
            planning_digest,
        }
    }

    pub const fn cohort_id(&self) -> ConnectorWriteCohortId {
        self.cohort_id
    }

    pub const fn intent(&self) -> ConnectorWriteIntent {
        self.intent
    }

    pub const fn planning_digest(&self) -> [u8; 32] {
        self.planning_digest
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ConnectorSealedWriteCohortSet {
    operation_id: ConnectorWriteOperationId,
    cohorts: Vec<ConnectorWriteCohortDescriptor>,
    digest: [u8; 32],
}

impl ConnectorSealedWriteCohortSet {
    pub fn try_new(
        operation_id: ConnectorWriteOperationId,
        cohorts: Vec<ConnectorWriteCohortDescriptor>,
    ) -> Result<Self, ConnectorError> {
        if cohorts.is_empty() || cohorts.len() > MAX_CONNECTOR_WRITE_COHORTS {
            return Err(ConnectorError::new(
                ConnectorErrorKind::ResourceExhausted,
                "connector write cohort set must contain 1..=4096 cohorts",
            ));
        }
        let mut cohorts = cohorts;
        cohorts.sort_by_key(ConnectorWriteCohortDescriptor::cohort_id);
        if cohorts
            .windows(2)
            .any(|pair| pair[0].cohort_id == pair[1].cohort_id)
        {
            return Err(ConnectorError::new(
                ConnectorErrorKind::InvalidRequest,
                "connector write cohort set contains a duplicate cohort ID",
            ));
        }
        let digest = cohort_set_digest(operation_id, &cohorts);
        Ok(Self {
            operation_id,
            cohorts,
            digest,
        })
    }

    pub const fn operation_id(&self) -> ConnectorWriteOperationId {
        self.operation_id
    }

    pub fn cohorts(&self) -> &[ConnectorWriteCohortDescriptor] {
        &self.cohorts
    }

    pub const fn digest(&self) -> [u8; 32] {
        self.digest
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ConnectorWriteAttemptCompletion {
    owner: ConnectorExecutionBindingKey,
    operation_id: ConnectorWriteOperationId,
    cohort_id: ConnectorWriteCohortId,
    execution_id: ConnectorWriteExecutionId,
    manifest_digest: [u8; 32],
    reports: Vec<ConnectorStagedReport>,
    control_payload: Bytes,
    digest: [u8; 32],
}

impl ConnectorWriteAttemptCompletion {
    #[allow(clippy::too_many_arguments)]
    pub fn try_new(
        owner: ConnectorExecutionBindingKey,
        operation_id: ConnectorWriteOperationId,
        cohort_id: ConnectorWriteCohortId,
        execution_id: ConnectorWriteExecutionId,
        manifest_digest: [u8; 32],
        reports: Vec<ConnectorStagedReport>,
        control_payload: Bytes,
    ) -> Result<Self, ConnectorError> {
        validate_handle_payload(&control_payload)?;
        if reports.is_empty() {
            return Err(ConnectorError::new(
                ConnectorErrorKind::InvalidRequest,
                "connector write attempt completion has no staged reports",
            ));
        }
        let mut writers = BTreeSet::new();
        for report in &reports {
            report.validate()?;
            let writer = report.writer();
            if writer.binding_key() != &owner
                || writer.operation_id() != operation_id
                || writer.cohort_id() != cohort_id
                || writer.execution_id() != execution_id
                || !writers.insert(writer.clone())
            {
                return Err(ConnectorError::new(
                    ConnectorErrorKind::InvalidRequest,
                    "connector write attempt reports do not form one exact cohort attempt",
                ));
            }
        }
        let mut reports = reports;
        reports.sort_by(|left, right| left.writer().cmp(right.writer()));
        let digest = attempt_completion_digest(
            &owner,
            operation_id,
            cohort_id,
            execution_id,
            manifest_digest,
            &reports,
            &control_payload,
        );
        Ok(Self {
            owner,
            operation_id,
            cohort_id,
            execution_id,
            manifest_digest,
            reports,
            control_payload,
            digest,
        })
    }

    pub fn owner(&self) -> &ConnectorExecutionBindingKey {
        &self.owner
    }

    pub const fn operation_id(&self) -> ConnectorWriteOperationId {
        self.operation_id
    }

    pub const fn cohort_id(&self) -> ConnectorWriteCohortId {
        self.cohort_id
    }

    pub const fn execution_id(&self) -> ConnectorWriteExecutionId {
        self.execution_id
    }

    pub const fn manifest_digest(&self) -> [u8; 32] {
        self.manifest_digest
    }

    pub fn reports(&self) -> &[ConnectorStagedReport] {
        &self.reports
    }

    pub fn control_payload(&self) -> &Bytes {
        &self.control_payload
    }

    pub const fn digest(&self) -> [u8; 32] {
        self.digest
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ConnectorWriteCohortCompletion {
    cohort_id: ConnectorWriteCohortId,
    accepted: Option<ConnectorWriteAttemptCompletion>,
    superseded: Vec<ConnectorWriteAttemptCompletion>,
}

impl ConnectorWriteCohortCompletion {
    pub fn try_new(
        cohort_id: ConnectorWriteCohortId,
        accepted: Option<ConnectorWriteAttemptCompletion>,
        superseded: Vec<ConnectorWriteAttemptCompletion>,
    ) -> Result<Self, ConnectorError> {
        if accepted.is_none() && superseded.is_empty() {
            return Err(ConnectorError::new(
                ConnectorErrorKind::InvalidRequest,
                "connector write cohort completion has no attempt reports",
            ));
        }
        let mut attempts = BTreeSet::new();
        for attempt in accepted.iter().chain(&superseded) {
            if attempt.cohort_id != cohort_id || !attempts.insert(attempt.execution_id) {
                return Err(ConnectorError::new(
                    ConnectorErrorKind::InvalidRequest,
                    "connector write cohort completion contains a foreign or duplicate attempt",
                ));
            }
        }
        let mut superseded = superseded;
        superseded.sort_by_key(ConnectorWriteAttemptCompletion::execution_id);
        Ok(Self {
            cohort_id,
            accepted,
            superseded,
        })
    }

    pub const fn cohort_id(&self) -> ConnectorWriteCohortId {
        self.cohort_id
    }

    pub fn accepted(&self) -> Option<&ConnectorWriteAttemptCompletion> {
        self.accepted.as_ref()
    }

    pub fn superseded(&self) -> &[ConnectorWriteAttemptCompletion] {
        &self.superseded
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ConnectorWriteOperationCompletion {
    owner: ConnectorExecutionBindingKey,
    sealed: ConnectorSealedWriteCohortSet,
    cohorts: Vec<ConnectorWriteCohortCompletion>,
    aggregate_digest: [u8; 32],
}

impl ConnectorWriteOperationCompletion {
    pub fn try_new(
        owner: ConnectorExecutionBindingKey,
        sealed: ConnectorSealedWriteCohortSet,
        cohorts: Vec<ConnectorWriteCohortCompletion>,
    ) -> Result<Self, ConnectorError> {
        validate_operation_cohorts(&owner, &sealed, &cohorts, true)?;
        let aggregate_digest = operation_completion_digest(&owner, &sealed, &cohorts);
        Ok(Self {
            owner,
            sealed,
            cohorts,
            aggregate_digest,
        })
    }

    pub fn owner(&self) -> &ConnectorExecutionBindingKey {
        &self.owner
    }

    pub fn sealed(&self) -> &ConnectorSealedWriteCohortSet {
        &self.sealed
    }

    pub fn cohorts(&self) -> &[ConnectorWriteCohortCompletion] {
        &self.cohorts
    }

    pub const fn aggregate_digest(&self) -> [u8; 32] {
        self.aggregate_digest
    }
}

#[derive(Clone)]
pub struct ConnectorOpenWriterRequest {
    pub handle: ConnectorWriterHandle,
    pub expected_schema: SchemaRef,
    pub context: ConnectorRequestContext,
}

#[derive(Clone)]
pub struct ConnectorWriteCommitRequest {
    pub completion: ConnectorWriteOperationCompletion,
    pub context: ConnectorRequestContext,
}

impl ConnectorWriteCommitRequest {
    pub fn owner(&self) -> &ConnectorExecutionBindingKey {
        self.completion.owner()
    }

    pub const fn operation_id(&self) -> ConnectorWriteOperationId {
        self.completion.sealed.operation_id
    }

    pub fn sealed(&self) -> &ConnectorSealedWriteCohortSet {
        self.completion.sealed()
    }

    pub fn cohorts(&self) -> &[ConnectorWriteCohortCompletion] {
        self.completion.cohorts()
    }

    pub const fn aggregate_digest(&self) -> [u8; 32] {
        self.completion.aggregate_digest()
    }
}

#[derive(Clone)]
pub struct ConnectorWriteAbortRequest {
    pub owner: ConnectorExecutionBindingKey,
    pub sealed: ConnectorSealedWriteCohortSet,
    pub cohorts: Vec<ConnectorWriteCohortCompletion>,
    pub aggregate_digest: [u8; 32],
    pub context: ConnectorRequestContext,
}

impl ConnectorWriteAbortRequest {
    pub fn try_new(
        owner: ConnectorExecutionBindingKey,
        sealed: ConnectorSealedWriteCohortSet,
        cohorts: Vec<ConnectorWriteCohortCompletion>,
        context: ConnectorRequestContext,
    ) -> Result<Self, ConnectorError> {
        validate_operation_cohorts(&owner, &sealed, &cohorts, false)?;
        let aggregate_digest = operation_completion_digest(&owner, &sealed, &cohorts);
        Ok(Self {
            owner,
            sealed,
            cohorts,
            aggregate_digest,
            context,
        })
    }

    pub const fn operation_id(&self) -> ConnectorWriteOperationId {
        self.sealed.operation_id
    }
}

#[derive(Clone)]
pub struct ConnectorWriteReconcileRequest {
    pub owner: ConnectorExecutionBindingKey,
    pub operation_id: ConnectorWriteOperationId,
    pub cohort_set_digest: [u8; 32],
    pub aggregate_digest: [u8; 32],
    pub evidence: ExternalMutationEvidence,
    pub context: ConnectorRequestContext,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ConnectorWriteAbortOutcome {
    KnownUncommitted {
        cleanup: ExternalMutationFinalization,
    },
    KnownCommitted {
        receipt: ConnectorWriteReceipt,
        finalization: ExternalMutationFinalization,
    },
    CommitUnknown {
        failure: ConnectorMutationFailure,
        evidence: ExternalMutationEvidence,
    },
}

pub trait ConnectorWriteControl: Send + Sync {
    fn binding_key(&self) -> &ConnectorExecutionBindingKey;

    fn plan_write(
        &self,
        request: ConnectorWritePlanningRequest,
    ) -> Result<ConnectorWritePlan, ConnectorError>;

    fn commit(
        &self,
        request: ConnectorWriteCommitRequest,
    ) -> Result<ExternalMutationOutcome<ConnectorWriteReceipt>, ConnectorError>;

    fn abort(
        &self,
        request: ConnectorWriteAbortRequest,
    ) -> Result<ConnectorWriteAbortOutcome, ConnectorError>;

    fn reconcile(
        &self,
        request: ConnectorWriteReconcileRequest,
    ) -> Result<ExternalMutationOutcome<ConnectorWriteReceipt>, ConnectorError>;
}

/// FE-owned resolver for a live write-control generation. The returned lease
/// keeps that exact generation alive through planning, commit, abort and
/// reconcile; callers must not replace it with a later current incarnation.
pub trait ConnectorWriteResolver: Send + Sync {
    fn acquire_current_write(
        &self,
        instance_id: &super::ConnectorInstanceId,
    ) -> Result<ConnectorWriteLease, ConnectorError>;
}

#[derive(Clone)]
pub struct ConnectorWriteLease {
    binding_key: ConnectorExecutionBindingKey,
    control: Arc<dyn ConnectorWriteControl>,
    execution_distribution: Option<Arc<dyn ConnectorExecutionDistribution>>,
    _release: Arc<ConnectorWriteLeaseRelease>,
}

struct ConnectorWriteLeaseRelease {
    release: Mutex<Option<Box<dyn FnOnce() + Send + Sync>>>,
}

impl ConnectorWriteLease {
    pub fn new(
        binding_key: ConnectorExecutionBindingKey,
        control: Arc<dyn ConnectorWriteControl>,
        release: impl FnOnce() + Send + Sync + 'static,
    ) -> Result<Self, ConnectorError> {
        if control.binding_key() != &binding_key {
            return Err(ConnectorError::new(
                ConnectorErrorKind::InvalidRequest,
                "connector write lease control does not match its binding generation",
            ));
        }
        Ok(Self {
            binding_key,
            control,
            execution_distribution: None,
            _release: Arc::new(ConnectorWriteLeaseRelease {
                release: Mutex::new(Some(Box::new(release))),
            }),
        })
    }

    /// Create an exact write lease that can also materialize the BE execution
    /// declaration from the same retained control generation. Production
    /// callers must use this constructor; the narrower `new` remains useful
    /// for isolated control-only conformance tests.
    pub fn new_with_execution_distribution(
        binding_key: ConnectorExecutionBindingKey,
        control: Arc<dyn ConnectorWriteControl>,
        execution_distribution: Arc<dyn ConnectorExecutionDistribution>,
        release: impl FnOnce() + Send + Sync + 'static,
    ) -> Result<Self, ConnectorError> {
        let mut lease = Self::new(binding_key, control, release)?;
        lease.execution_distribution = Some(execution_distribution);
        Ok(lease)
    }

    pub fn binding_key(&self) -> &ConnectorExecutionBindingKey {
        &self.binding_key
    }

    pub fn control(&self) -> &Arc<dyn ConnectorWriteControl> {
        &self.control
    }

    /// Materialize a declaration only through the exact generation held by
    /// this lease. A later active incarnation is deliberately unreachable.
    pub fn execution_declaration(
        &self,
        context: &ConnectorRequestContext,
    ) -> Result<ConnectorExecutionDeclaration, ConnectorError> {
        let distribution = self.execution_distribution.as_ref().ok_or_else(|| {
            ConnectorError::new(
                ConnectorErrorKind::InvalidRequest,
                "connector write lease has no execution distribution capability",
            )
        })?;
        let declaration = distribution.declaration(context)?;
        let key = declaration.binding_key();
        if key != self.binding_key {
            return Err(ConnectorError::new(
                ConnectorErrorKind::InvalidRequest,
                "connector write declaration does not match its retained binding generation",
            ));
        }
        Ok(declaration)
    }
}

impl Drop for ConnectorWriteLeaseRelease {
    fn drop(&mut self) {
        let Ok(mut release) = self.release.lock() else {
            return;
        };
        if let Some(release) = release.take() {
            release();
        }
    }
}

pub trait ConnectorWriteExecution: Send + Sync {
    fn binding_key(&self) -> &ConnectorExecutionBindingKey;

    fn open_writer(
        &self,
        request: ConnectorOpenWriterRequest,
    ) -> Result<Box<dyn ConnectorBatchWriter>, ConnectorError>;
}

pub trait ConnectorBatchWriter: Send {
    fn append(&mut self, batch: RecordBatch) -> Result<(), ConnectorError>;

    fn finish(&mut self) -> Result<ConnectorStagedReport, ConnectorError>;

    fn abort(&mut self) -> Result<(), ConnectorError>;

    fn summary(&self) -> ConnectorStagedReportSummary {
        ConnectorStagedReportSummary::default()
    }
}

fn validate_handle_payload(payload: &Bytes) -> Result<(), ConnectorError> {
    if payload.len() > MAX_CONNECTOR_HANDLE_PAYLOAD_BYTES {
        return Err(ConnectorError::new(
            ConnectorErrorKind::ResourceExhausted,
            "connector write handle payload exceeds the hard limit",
        ));
    }
    Ok(())
}

fn validate_total_handle_payload(
    handles: &[ConnectorWriterHandle],
    control_payload: &Bytes,
) -> Result<(), ConnectorError> {
    validate_handle_payload(control_payload)?;
    let total = handles
        .iter()
        .try_fold(control_payload.len(), |total, handle| {
            handle.payload.len().checked_add(total).ok_or_else(|| {
                ConnectorError::new(
                    ConnectorErrorKind::ResourceExhausted,
                    "connector write plan payload accounting overflowed",
                )
            })
        })?;
    if total > MAX_CONNECTOR_TOTAL_PAYLOAD_BYTES {
        return Err(ConnectorError::new(
            ConnectorErrorKind::ResourceExhausted,
            "connector write plan payload exceeds the hard limit",
        ));
    }
    Ok(())
}

fn validate_report_payload(payload: &Bytes) -> Result<(), ConnectorError> {
    if payload.len() > MAX_CONNECTOR_STAGED_REPORT_PAYLOAD_BYTES {
        return Err(ConnectorError::new(
            ConnectorErrorKind::ResourceExhausted,
            "connector staged report payload exceeds the hard limit",
        ));
    }
    Ok(())
}

fn validate_receipt_payload(payload: &Bytes) -> Result<(), ConnectorError> {
    if payload.len() > MAX_CONNECTOR_WRITE_RECEIPT_BYTES {
        return Err(ConnectorError::new(
            ConnectorErrorKind::ResourceExhausted,
            "connector write receipt exceeds the hard limit",
        ));
    }
    Ok(())
}

fn validate_operation_cohorts(
    owner: &ConnectorExecutionBindingKey,
    sealed: &ConnectorSealedWriteCohortSet,
    cohorts: &[ConnectorWriteCohortCompletion],
    require_complete: bool,
) -> Result<(), ConnectorError> {
    if cohorts.len() > sealed.cohorts.len() {
        return Err(ConnectorError::new(
            ConnectorErrorKind::InvalidRequest,
            "connector write operation contains more completions than sealed cohorts",
        ));
    }
    let expected = sealed
        .cohorts
        .iter()
        .map(ConnectorWriteCohortDescriptor::cohort_id)
        .collect::<BTreeSet<_>>();
    let mut actual = BTreeSet::new();
    let mut writer_count = 0usize;
    let mut payload_bytes = 0usize;
    for cohort in cohorts {
        if !expected.contains(&cohort.cohort_id) || !actual.insert(cohort.cohort_id) {
            return Err(ConnectorError::new(
                ConnectorErrorKind::InvalidRequest,
                "connector write operation contains an unknown or duplicate cohort completion",
            ));
        }
        if require_complete && cohort.accepted.is_none() {
            return Err(ConnectorError::new(
                ConnectorErrorKind::InvalidRequest,
                "connector write commit is missing an accepted cohort attempt",
            ));
        }
        for attempt in cohort.accepted.iter().chain(&cohort.superseded) {
            if &attempt.owner != owner || attempt.operation_id != sealed.operation_id {
                return Err(ConnectorError::new(
                    ConnectorErrorKind::InvalidRequest,
                    "connector write operation attempt has a foreign owner or operation",
                ));
            }
            writer_count = writer_count
                .checked_add(attempt.reports.len())
                .ok_or_else(|| {
                    ConnectorError::new(
                        ConnectorErrorKind::ResourceExhausted,
                        "connector write operation writer accounting overflowed",
                    )
                })?;
            payload_bytes = attempt.reports.iter().try_fold(
                payload_bytes
                    .checked_add(attempt.control_payload.len())
                    .ok_or_else(|| {
                        ConnectorError::new(
                            ConnectorErrorKind::ResourceExhausted,
                            "connector write operation payload accounting overflowed",
                        )
                    })?,
                |total, report| {
                    total.checked_add(report.payload.len()).ok_or_else(|| {
                        ConnectorError::new(
                            ConnectorErrorKind::ResourceExhausted,
                            "connector write operation payload accounting overflowed",
                        )
                    })
                },
            )?;
        }
    }
    if require_complete && actual != expected {
        return Err(ConnectorError::new(
            ConnectorErrorKind::InvalidRequest,
            "connector write commit does not exactly cover the sealed cohort set",
        ));
    }
    if writer_count > MAX_CONNECTOR_WRITE_OPERATION_WRITERS
        || payload_bytes > MAX_CONNECTOR_WRITE_OPERATION_PAYLOAD_BYTES
    {
        return Err(ConnectorError::new(
            ConnectorErrorKind::ResourceExhausted,
            "connector write operation exceeds its aggregate writer or payload budget",
        ));
    }
    Ok(())
}

fn cohort_set_digest(
    operation_id: ConnectorWriteOperationId,
    cohorts: &[ConnectorWriteCohortDescriptor],
) -> [u8; 32] {
    let mut hasher = Sha256::new();
    hasher.update(CONNECTOR_WRITE_COHORT_SET_DOMAIN);
    hasher.update(operation_id.to_bytes());
    hasher.update((cohorts.len() as u64).to_be_bytes());
    for cohort in cohorts {
        hasher.update(cohort.cohort_id.to_bytes());
        hasher.update([write_intent_tag(cohort.intent)]);
        hasher.update(cohort.planning_digest);
    }
    hasher.finalize().into()
}

#[allow(clippy::too_many_arguments)]
fn attempt_completion_digest(
    owner: &ConnectorExecutionBindingKey,
    operation_id: ConnectorWriteOperationId,
    cohort_id: ConnectorWriteCohortId,
    execution_id: ConnectorWriteExecutionId,
    manifest_digest: [u8; 32],
    reports: &[ConnectorStagedReport],
    control_payload: &Bytes,
) -> [u8; 32] {
    let mut hasher = Sha256::new();
    hasher.update(CONNECTOR_WRITE_ATTEMPT_DOMAIN);
    digest_owner(&mut hasher, owner);
    hasher.update(operation_id.to_bytes());
    hasher.update(cohort_id.to_bytes());
    hasher.update(execution_id.query_id());
    hasher.update(execution_id.attempt_id().to_be_bytes());
    hasher.update(manifest_digest);
    digest_bytes(&mut hasher, control_payload);
    hasher.update((reports.len() as u64).to_be_bytes());
    for report in reports {
        digest_writer(&mut hasher, report.writer());
        hasher.update(report.version.to_be_bytes());
        hasher.update([writer_terminal_state_tag(report.state)]);
        hasher.update(report.summary.input_rows.to_be_bytes());
        hasher.update(report.summary.staged_bytes.to_be_bytes());
        hasher.update(report.summary.artifact_count.to_be_bytes());
        hasher.update(report.payload_digest);
    }
    hasher.finalize().into()
}

fn operation_completion_digest(
    owner: &ConnectorExecutionBindingKey,
    sealed: &ConnectorSealedWriteCohortSet,
    cohorts: &[ConnectorWriteCohortCompletion],
) -> [u8; 32] {
    let mut cohorts = cohorts.iter().collect::<Vec<_>>();
    cohorts.sort_by_key(|cohort| cohort.cohort_id);
    let mut hasher = Sha256::new();
    hasher.update(CONNECTOR_WRITE_OPERATION_DOMAIN);
    digest_owner(&mut hasher, owner);
    hasher.update(sealed.operation_id.to_bytes());
    hasher.update(sealed.digest);
    hasher.update((cohorts.len() as u64).to_be_bytes());
    for cohort in cohorts {
        hasher.update(cohort.cohort_id.to_bytes());
        match &cohort.accepted {
            Some(accepted) => {
                hasher.update([1]);
                hasher.update(accepted.digest);
            }
            None => hasher.update([0]),
        }
        hasher.update((cohort.superseded.len() as u64).to_be_bytes());
        for superseded in &cohort.superseded {
            hasher.update(superseded.digest);
        }
    }
    hasher.finalize().into()
}

fn digest_owner(hasher: &mut Sha256, owner: &ConnectorExecutionBindingKey) {
    digest_bytes(hasher, owner.instance_id.as_str().as_bytes());
    hasher.update(owner.incarnation.to_bytes());
}

fn digest_writer(hasher: &mut Sha256, writer: &ConnectorWriterIdentity) {
    hasher.update(writer.operation_id.to_bytes());
    hasher.update(writer.cohort_id.to_bytes());
    hasher.update(writer.execution_id.query_id());
    hasher.update(writer.execution_id.attempt_id().to_be_bytes());
    hasher.update(writer.fragment_instance_id);
    hasher.update(writer.fragment_id.to_be_bytes());
    hasher.update(writer.backend_num.to_be_bytes());
    hasher.update(writer.sink_ordinal.to_be_bytes());
    digest_owner(hasher, &writer.binding_key);
}

fn digest_bytes(hasher: &mut Sha256, bytes: &[u8]) {
    hasher.update((bytes.len() as u64).to_be_bytes());
    hasher.update(bytes);
}

const fn write_intent_tag(intent: ConnectorWriteIntent) -> u8 {
    match intent {
        ConnectorWriteIntent::Append => 1,
        ConnectorWriteIntent::Overwrite => 2,
        ConnectorWriteIntent::PartitionOverwrite => 3,
        ConnectorWriteIntent::RowDelta => 4,
    }
}

const fn writer_terminal_state_tag(state: ConnectorWriterTerminalState) -> u8 {
    match state {
        ConnectorWriterTerminalState::Staged => 1,
        ConnectorWriterTerminalState::Aborted => 2,
        ConnectorWriterTerminalState::Failed => 3,
    }
}

fn sha256(payload: &Bytes) -> [u8; 32] {
    Sha256::digest(payload).into()
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::time::{Duration, Instant};

    use arrow::datatypes::{DataType, Field, Schema};

    use super::*;
    use crate::connector::{
        ConnectorCancellation, ConnectorInstanceId, ConnectorInstanceIncarnation,
    };

    struct NotCancelled;
    impl ConnectorCancellation for NotCancelled {
        fn is_cancelled(&self) -> bool {
            false
        }
    }

    fn key() -> ConnectorExecutionBindingKey {
        ConnectorExecutionBindingKey {
            instance_id: ConnectorInstanceId::parse("unit").expect("instance ID"),
            incarnation: ConnectorInstanceIncarnation::new(),
        }
    }

    fn writer() -> ConnectorWriterIdentity {
        let operation_id = ConnectorWriteOperationId::new();
        ConnectorWriterIdentity::new(
            operation_id,
            ConnectorWriteCohortId::primary(operation_id),
            ConnectorWriteExecutionId::new([1; 16], 2),
            [3; 16],
            4,
            5,
            0,
            key(),
        )
    }

    #[test]
    fn operation_id_round_trips_through_durable_attempt_text() {
        let operation_id = ConnectorWriteOperationId::new();
        let parsed: ConnectorWriteOperationId = operation_id
            .to_string()
            .parse()
            .expect("UUID v7 attempt text must round-trip");
        assert_eq!(parsed, operation_id);
    }

    #[test]
    fn report_frames_are_bounded_and_digest_stable() {
        let report = ConnectorStagedReport::try_new(
            writer(),
            CONNECTOR_WRITE_CONTRACT_VERSION,
            ConnectorWriterTerminalState::Staged,
            ConnectorStagedReportSummary::default(),
            Bytes::from(vec![9; MAX_CONNECTOR_STAGED_REPORT_FRAME_BYTES + 1]),
        )
        .expect("report");
        let frames = report.frames();
        assert_eq!(frames.len(), 2);
        assert_eq!(frames[0].part_index(), 0);
        assert_eq!(frames[1].part_index(), 1);
        assert_eq!(frames[0].logical_payload_digest(), report.payload_digest());
        assert!(frames.iter().all(|frame| frame.validate().is_ok()));
    }

    #[test]
    fn report_reassembly_accepts_retry_duplicates_and_rejects_gaps() {
        let report = ConnectorStagedReport::try_new(
            writer(),
            CONNECTOR_WRITE_CONTRACT_VERSION,
            ConnectorWriterTerminalState::Staged,
            ConnectorStagedReportSummary::default(),
            Bytes::from(vec![9; MAX_CONNECTOR_STAGED_REPORT_FRAME_BYTES + 1]),
        )
        .expect("report");
        let frames = report.frames();
        let reassembled = ConnectorStagedReport::try_from_frames(vec![
            frames[1].clone(),
            frames[0].clone(),
            frames[0].clone(),
        ])
        .expect("identical retry");
        assert_eq!(reassembled, report);

        let error = ConnectorStagedReport::try_from_frames(vec![frames[0].clone()])
            .expect_err("missing final frame");
        assert_eq!(error.kind(), ConnectorErrorKind::InvalidRequest);
    }

    #[test]
    fn plan_rejects_conflicting_writer_manifest() {
        let writer = writer();
        let handle = ConnectorWriterHandle::try_new(
            writer.binding_key().clone(),
            writer.clone(),
            CONNECTOR_WRITE_CONTRACT_VERSION,
            Bytes::new(),
        )
        .expect("handle");
        let error = ConnectorWritePlan::try_new(
            writer.binding_key().clone(),
            writer.operation_id(),
            writer.cohort_id(),
            writer.execution_id(),
            vec![handle.clone(), handle],
            Bytes::new(),
        )
        .expect_err("duplicate writer must fail");
        assert_eq!(error.kind(), ConnectorErrorKind::InvalidRequest);
    }

    #[test]
    fn planning_request_requires_exact_writer_owner() {
        let writer = writer();
        let owner = writer.binding_key().clone();
        let table =
            ConnectorTableHandle::try_new(writer.binding_key().instance_id.clone(), Bytes::new())
                .expect("table handle");
        let request = ConnectorWritePlanningRequest {
            operation_id: writer.operation_id(),
            cohort_id: writer.cohort_id(),
            execution_id: writer.execution_id(),
            table,
            intent: ConnectorWriteIntent::Append,
            input_schema: Arc::new(Schema::new(vec![Field::new("x", DataType::Int64, true)])),
            expected_writers: vec![writer],
            provider_payload: Bytes::new(),
            context: ConnectorRequestContext::try_new(
                Instant::now() + Duration::from_secs(1),
                Arc::new(NotCancelled),
                MAX_CONNECTOR_HANDLE_PAYLOAD_BYTES,
                MAX_CONNECTOR_TOTAL_PAYLOAD_BYTES,
            )
            .expect("context"),
        };
        request.validate(&owner).expect("exact writer owner");
    }

    #[test]
    fn sealed_cohort_set_is_sorted_and_rejects_duplicates() {
        let operation_id = ConnectorWriteOperationId::new();
        let first = ConnectorWriteCohortId::derive(operation_id, b"rewrite", [1; 32])
            .expect("first cohort");
        let second = ConnectorWriteCohortId::derive(operation_id, b"rewrite", [2; 32])
            .expect("second cohort");
        let sealed = ConnectorSealedWriteCohortSet::try_new(
            operation_id,
            vec![
                ConnectorWriteCohortDescriptor::new(
                    second,
                    ConnectorWriteIntent::RowDelta,
                    [4; 32],
                ),
                ConnectorWriteCohortDescriptor::new(first, ConnectorWriteIntent::RowDelta, [3; 32]),
            ],
        )
        .expect("sealed cohorts");
        assert!(sealed.cohorts()[0].cohort_id() < sealed.cohorts()[1].cohort_id());

        let duplicate = ConnectorSealedWriteCohortSet::try_new(
            operation_id,
            vec![
                ConnectorWriteCohortDescriptor::new(first, ConnectorWriteIntent::RowDelta, [3; 32]),
                ConnectorWriteCohortDescriptor::new(first, ConnectorWriteIntent::RowDelta, [3; 32]),
            ],
        )
        .expect_err("duplicate cohort");
        assert_eq!(duplicate.kind(), ConnectorErrorKind::InvalidRequest);
    }

    #[test]
    fn operation_completion_rejects_missing_and_mixed_attempts() {
        let writer = writer();
        let operation_id = writer.operation_id();
        let cohort_id = writer.cohort_id();
        let owner = writer.binding_key().clone();
        let report = ConnectorStagedReport::try_new(
            writer.clone(),
            CONNECTOR_WRITE_CONTRACT_VERSION,
            ConnectorWriterTerminalState::Staged,
            ConnectorStagedReportSummary::default(),
            Bytes::from_static(b"report"),
        )
        .expect("report");
        let accepted = ConnectorWriteAttemptCompletion::try_new(
            owner.clone(),
            operation_id,
            cohort_id,
            writer.execution_id(),
            [7; 32],
            vec![report],
            Bytes::new(),
        )
        .expect("accepted attempt");
        let sealed = ConnectorSealedWriteCohortSet::try_new(
            operation_id,
            vec![ConnectorWriteCohortDescriptor::new(
                cohort_id,
                ConnectorWriteIntent::Append,
                [8; 32],
            )],
        )
        .expect("sealed");
        let missing =
            ConnectorWriteOperationCompletion::try_new(owner.clone(), sealed.clone(), Vec::new())
                .expect_err("missing cohort");
        assert_eq!(missing.kind(), ConnectorErrorKind::InvalidRequest);

        let cohort = ConnectorWriteCohortCompletion::try_new(
            cohort_id,
            Some(accepted.clone()),
            vec![accepted],
        )
        .expect_err("same attempt cannot be accepted and superseded");
        assert_eq!(cohort.kind(), ConnectorErrorKind::InvalidRequest);
    }

    #[test]
    fn receipt_rejects_oversized_payload() {
        let error = ConnectorWriteReceipt::try_new(Bytes::from(vec![
            0;
            MAX_CONNECTOR_WRITE_RECEIPT_BYTES
                + 1
        ]))
        .expect_err("receipt limit");
        assert_eq!(error.kind(), ConnectorErrorKind::ResourceExhausted);
    }
}
