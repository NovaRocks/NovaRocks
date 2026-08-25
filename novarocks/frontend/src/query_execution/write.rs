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

//! Native distributed write report model.

use std::collections::BTreeMap;

use bytes::Bytes;
use novarocks_spi::connector::{
    CONNECTOR_WRITE_CONTRACT_VERSION, ConnectorExecutionBindingKey, ConnectorInstanceId,
    ConnectorInstanceIncarnation, ConnectorStagedReport, ConnectorStagedReportSummary,
    ConnectorWriteCohortId, ConnectorWriteExecutionId, ConnectorWriteOperationId,
    ConnectorWriterIdentity, ConnectorWriterTerminalState, MAX_CONNECTOR_STAGED_REPORT_FRAME_BYTES,
    MAX_CONNECTOR_STAGED_REPORT_PARTS, MAX_CONNECTOR_STAGED_REPORT_PAYLOAD_BYTES,
};
use sha2::{Digest, Sha256};

use crate::query_execution::artifact::WriterRegistrationSet;
use crate::query_execution::contract::{DistributedQueryError, DistributedQueryErrorKind};
use novarocks_proto::lifecycle::QueryExecutionId;
use novarocks_proto::novarocks;
use novarocks_types::UniqueId;

// This is deliberately a wire-level value rather than an Iceberg enum.  The
// SPI's first terminal state is `Staged`; coordinator code must never count a
// failed or aborted backend writer as a commit candidate.
const CONNECTOR_WRITER_TERMINAL_STAGED: u32 = 0;

#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub(crate) struct WriterKey {
    pub(crate) query_id: UniqueId,
    pub(crate) fragment_instance_id: UniqueId,
    pub(crate) backend_num: i32,
}

#[derive(Clone, Debug, PartialEq)]
pub struct WriteCommitInput {
    pub(crate) write_id: UniqueId,
    pub(crate) writers: Vec<WriterCommitInput>,
}

/// Provider-neutral staged writer input reconstructed only after every native
/// frame has passed the coordinator's ownership, bounds, and digest checks.
///
/// This is the only distributed writer commit carrier: opaque SPI reports for
/// one exact control binding and operation. Transaction runners never import
/// a provider DTO.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct ConnectorWriteCommitInput {
    owner: ConnectorExecutionBindingKey,
    operation_id: ConnectorWriteOperationId,
    cohort_id: novarocks_spi::connector::ConnectorWriteCohortId,
    execution_id: ConnectorWriteExecutionId,
    reports: Vec<ConnectorStagedReport>,
}

impl ConnectorWriteCommitInput {
    pub(crate) fn owner(&self) -> &ConnectorExecutionBindingKey {
        &self.owner
    }

    pub(crate) const fn operation_id(&self) -> ConnectorWriteOperationId {
        self.operation_id
    }

    pub(crate) const fn cohort_id(&self) -> novarocks_spi::connector::ConnectorWriteCohortId {
        self.cohort_id
    }

    pub(crate) const fn execution_id(&self) -> ConnectorWriteExecutionId {
        self.execution_id
    }

    pub(crate) fn reports(&self) -> &[ConnectorStagedReport] {
        &self.reports
    }

    /// Extract the generic carrier from a completed write. Once any generic
    /// report is present every registered writer must contribute exactly one
    /// complete logical report.
    #[allow(
        dead_code,
        reason = "The generic carrier extractor remains available to target-gated connector write completion paths."
    )]
    pub(crate) fn try_extract(
        write_commit: &WriteCommitInput,
    ) -> Result<Option<Self>, DistributedQueryError> {
        let grouped = Self::try_extract_grouped(write_commit)?;
        if grouped.len() > 1 {
            return Err(contract_violation(
                "generic connector write commit contains multiple write cohorts",
            ));
        }
        Ok(grouped.into_values().next())
    }

    pub(crate) fn try_extract_grouped(
        write_commit: &WriteCommitInput,
    ) -> Result<BTreeMap<ConnectorWriteCohortId, Self>, DistributedQueryError> {
        let has_generic = write_commit
            .writers
            .iter()
            .any(|writer| !writer.connector_staged_report_frames.is_empty());
        if !has_generic {
            return Ok(BTreeMap::new());
        }
        if write_commit.writers.is_empty() {
            return Err(contract_violation(
                "generic connector write commit has no registered writers",
            ));
        }

        let mut expected_writer_keys = std::collections::BTreeSet::new();
        let mut reports_by_cohort = BTreeMap::<
            ConnectorWriteCohortId,
            BTreeMap<ConnectorWriterIdentity, ConnectorStagedReport>,
        >::new();
        let mut operation_id = None;
        let mut execution_id = None;
        let mut owner = None;
        for writer in &write_commit.writers {
            if !expected_writer_keys.insert(writer.writer_key.clone()) {
                return Err(contract_violation(
                    "generic connector write commit contains duplicate registered writer output",
                ));
            }
            if writer.connector_staged_report_frames.is_empty() {
                return Err(contract_violation(
                    "generic connector write commit is missing a staged report for a registered writer",
                ));
            }
            let report = reassemble_connector_staged_report(writer)?;
            let identity = report.writer().clone();
            if i64::from(identity.fragment_id()) != i64::from(writer.fragment_id)
                || identity.fragment_instance_id()
                    != unique_id_to_be_bytes(writer.writer_key.fragment_instance_id)
                || identity.backend_num() != writer.writer_key.backend_num
                || identity.execution_id().query_id()
                    != unique_id_to_be_bytes(writer.writer_key.query_id)
            {
                return Err(contract_violation(
                    "generic connector staged report does not match its registered writer output",
                ));
            }
            match operation_id {
                Some(expected) if expected != identity.operation_id() => {
                    return Err(contract_violation(
                        "generic connector write commit contains multiple write operations",
                    ));
                }
                None => operation_id = Some(identity.operation_id()),
                _ => {}
            }
            match execution_id {
                Some(expected) if expected != identity.execution_id() => {
                    return Err(contract_violation(
                        "generic connector write commit contains multiple execution attempts",
                    ));
                }
                None => execution_id = Some(identity.execution_id()),
                _ => {}
            }
            match &owner {
                Some(expected) if expected != identity.binding_key() => {
                    return Err(contract_violation(
                        "generic connector write commit contains multiple connector binding generations",
                    ));
                }
                None => owner = Some(identity.binding_key().clone()),
                _ => {}
            }
            let cohort_id = identity.cohort_id();
            if reports_by_cohort
                .entry(cohort_id)
                .or_default()
                .insert(identity, report)
                .is_some()
            {
                return Err(contract_violation(
                    "generic connector write commit contains duplicate logical writer reports",
                ));
            }
        }
        let owner = owner.expect("generic connector reports have an owner");
        let operation_id = operation_id.expect("generic connector reports have an operation");
        let execution_id = execution_id.expect("generic connector reports have an execution");
        Ok(reports_by_cohort
            .into_iter()
            .map(|(cohort_id, reports)| {
                (
                    cohort_id,
                    Self {
                        owner: owner.clone(),
                        operation_id,
                        cohort_id,
                        execution_id,
                        reports: reports.into_values().collect(),
                    },
                )
            })
            .collect())
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct WriterCommitInput {
    pub(crate) writer_id: usize,
    pub(crate) fragment_id: u32,
    pub(crate) writer_key: WriterKey,
    /// Provider-neutral staged report frames.  They remain opaque until the
    /// exact FE control binding reassembles and commits them.
    pub(crate) connector_staged_report_frames: Vec<novarocks::ConnectorStagedReportFrame>,
    pub(crate) load_counters: BTreeMap<String, String>,
    pub(crate) loaded_rows: i64,
    pub(crate) loaded_bytes: i64,
    pub(crate) filtered_rows: i64,
}

#[derive(Clone, Debug, PartialEq)]
pub struct WriteAbortInput {
    pub(crate) write_id: UniqueId,
    pub(crate) reason: String,
    pub(crate) completed_writer_outputs: Vec<WriterCommitInput>,
    pub(crate) incomplete_writers: Vec<WriterKey>,
}

impl WriteAbortInput {
    pub fn reason(&self) -> &str {
        &self.reason
    }
}

pub struct WriteReportOutcome {
    commit: Option<WriteCommitInput>,
    abort: Option<WriteAbortInput>,
}

impl WriteReportOutcome {
    pub fn abort_reason(&self) -> Option<&str> {
        self.abort.as_ref().map(|abort| abort.reason.as_str())
    }

    pub fn into_payloads(self) -> (Option<WriteCommitInput>, Option<WriteAbortInput>) {
        (self.commit, self.abort)
    }
}

/// Pure consuming builder from neutral native reports to the intent-safe write
/// completion payload.
pub struct WriteTerminalBuilder {
    write_id: UniqueId,
    expected: BTreeMap<WriterKey, (usize, u32, QueryExecutionId, Option<ConnectorWriteCohortId>)>,
    completed: BTreeMap<WriterKey, WriterCommitInput>,
    failure: Option<String>,
}

impl WriteTerminalBuilder {
    pub fn new(registrations: WriterRegistrationSet) -> Result<Self, DistributedQueryError> {
        let registrations = registrations.into_registrations();
        let write_id = registrations
            .first()
            .map(|registration| registration.query_id)
            .ok_or_else(|| contract_violation("write execution has no writer registrations"))?;
        let mut expected = BTreeMap::new();
        for (writer_id, registration) in registrations.into_iter().enumerate() {
            let key = WriterKey {
                query_id: registration.query_id,
                fragment_instance_id: registration.fragment_instance_id,
                backend_num: registration.backend_num,
            };
            if key.query_id != write_id {
                return Err(contract_violation(
                    "writer registrations contain multiple query ids",
                ));
            }
            if expected
                .insert(
                    key,
                    (
                        writer_id,
                        registration.fragment_id,
                        registration.execution_id,
                        registration.expected_connector_cohort_id,
                    ),
                )
                .is_some()
            {
                return Err(contract_violation(
                    "writer registrations contain duplicate writer identities",
                ));
            }
        }
        Ok(Self {
            write_id,
            expected,
            completed: BTreeMap::new(),
            failure: None,
        })
    }

    pub fn apply_terminal(
        &mut self,
        fragment: &novarocks::QueryTerminalFragmentSnapshot,
    ) -> Result<(), DistributedQueryError> {
        let key = WriterKey {
            query_id: self.write_id,
            fragment_instance_id: fragment
                .fragment_instance_id
                .as_ref()
                .map(|id| UniqueId::new(id.hi, id.lo))
                .expect("validated terminal fragment always has an instance id"),
            backend_num: fragment.backend_num,
        };
        let Some((writer_id, fragment_id, execution_id, expected_cohort_id)) =
            self.expected.get(&key).copied()
        else {
            return Ok(());
        };
        if fragment.outcome != novarocks::QueryTerminalFragmentOutcome::Succeeded as i32 {
            self.failure.get_or_insert_with(|| {
                match novarocks::QueryTerminalFragmentOutcome::try_from(fragment.outcome) {
                    Ok(novarocks::QueryTerminalFragmentOutcome::Failed) => {
                        format!(
                            "native writer failed with {}: {}",
                            fragment.error_code, fragment.error_detail
                        )
                    }
                    Ok(novarocks::QueryTerminalFragmentOutcome::Cancelled) => {
                        format!("native writer cancelled: {}", fragment.error_detail)
                    }
                    Ok(novarocks::QueryTerminalFragmentOutcome::IncompleteDrain) => {
                        format!(
                            "native writer drain was incomplete: {}",
                            fragment.error_detail
                        )
                    }
                    Ok(novarocks::QueryTerminalFragmentOutcome::Succeeded) => unreachable!(),
                    _ => "native writer reported an invalid terminal outcome".to_string(),
                }
            });
            return Ok(());
        }
        let frames = fragment.connector_staged_report_frames.clone();
        if let Err(error) = validate_connector_staged_report_frames(
            &frames,
            key.query_id,
            key.fragment_instance_id,
            key.backend_num,
            Some(execution_id),
            Some(fragment_id),
        ) {
            self.failure.get_or_insert(error);
            return Ok(());
        }
        if let Some(expected_cohort_id) = expected_cohort_id {
            let Some(writer) = frames.first().and_then(|frame| frame.writer.as_ref()) else {
                self.failure.get_or_insert_with(|| {
                    "connector terminal writer completed without staged report frames".to_string()
                });
                return Ok(());
            };
            let identity = match connector_writer_identity_from_native(writer) {
                Ok(identity) => identity,
                Err(error) => {
                    self.failure.get_or_insert(error.to_string());
                    return Ok(());
                }
            };
            if identity.cohort_id() != expected_cohort_id {
                self.failure.get_or_insert_with(|| {
                    "connector staged report cohort does not match its terminal writer registration"
                        .to_string()
                });
                return Ok(());
            }
        }
        let output = WriterCommitInput {
            writer_id,
            fragment_id,
            writer_key: key.clone(),
            connector_staged_report_frames: frames,
            load_counters: BTreeMap::new(),
            loaded_rows: fragment
                .load_stats
                .as_ref()
                .expect("validated terminal fragment always has load stats")
                .loaded_rows,
            loaded_bytes: fragment
                .load_stats
                .as_ref()
                .expect("validated terminal fragment always has load stats")
                .loaded_bytes,
            filtered_rows: fragment
                .load_stats
                .as_ref()
                .expect("validated terminal fragment always has load stats")
                .filtered_rows,
        };
        if let Some(existing) = self.completed.get(&key) {
            if existing == &output {
                return Ok(());
            }
            self.failure.get_or_insert_with(|| {
                "write terminal set contains conflicting writer output".into()
            });
            return Ok(());
        }
        self.completed.insert(key, output);
        Ok(())
    }

    pub fn latch_failure(&mut self, message: impl Into<String>) {
        self.failure.get_or_insert_with(|| message.into());
    }

    pub fn finish(self) -> Result<WriteReportOutcome, DistributedQueryError> {
        let incomplete_writers = self
            .expected
            .keys()
            .filter(|key| !self.completed.contains_key(*key))
            .cloned()
            .collect::<Vec<_>>();
        let completed_writer_outputs = self.completed.into_values().collect::<Vec<_>>();
        if let Some(reason) = self.failure {
            return Ok(WriteReportOutcome {
                commit: None,
                abort: Some(WriteAbortInput {
                    write_id: self.write_id,
                    reason,
                    completed_writer_outputs,
                    incomplete_writers,
                }),
            });
        }
        if !incomplete_writers.is_empty() {
            return Err(DistributedQueryError::new(
                DistributedQueryErrorKind::Failed,
                "write execution ended before all writer reports arrived",
            ));
        }
        Ok(WriteReportOutcome {
            commit: Some(WriteCommitInput {
                write_id: self.write_id,
                writers: completed_writer_outputs,
            }),
            abort: None,
        })
    }
}

fn contract_violation(message: impl Into<String>) -> DistributedQueryError {
    DistributedQueryError::new(DistributedQueryErrorKind::ContractViolation, message)
}

/// Reassemble one registered writer's opaque frames into the bounded SPI
/// report.  Revalidation here is intentional: `WriterCommitInput` can also
/// be constructed by transaction-recovery paths, so it must not trust that it
/// originated exclusively from `WriteTerminalBuilder`.
pub(crate) fn reassemble_connector_staged_report(
    writer: &WriterCommitInput,
) -> Result<ConnectorStagedReport, DistributedQueryError> {
    validate_connector_staged_report_frames(
        &writer.connector_staged_report_frames,
        writer.writer_key.query_id,
        writer.writer_key.fragment_instance_id,
        writer.writer_key.backend_num,
        None,
        Some(writer.fragment_id),
    )
    .map_err(contract_violation)?;
    let first = writer
        .connector_staged_report_frames
        .first()
        .ok_or_else(|| {
            contract_violation("registered connector writer has no staged report frames")
        })?;
    let identity = connector_writer_identity_from_native(
        first
            .writer
            .as_ref()
            .expect("validated connector frame has writer identity"),
    )?;
    let mut frames = BTreeMap::new();
    for frame in &writer.connector_staged_report_frames {
        match frames.entry(frame.part_index) {
            std::collections::btree_map::Entry::Vacant(entry) => {
                entry.insert(frame);
            }
            std::collections::btree_map::Entry::Occupied(entry) if *entry.get() == frame => {}
            std::collections::btree_map::Entry::Occupied(_) => {
                return Err(contract_violation(
                    "registered connector writer has conflicting duplicate report parts",
                ));
            }
        }
    }
    let payload_len = usize::try_from(first.logical_payload_len).map_err(|_| {
        contract_violation("connector staged report payload length does not fit usize")
    })?;
    let mut payload = Vec::with_capacity(payload_len);
    for frame in frames.into_values() {
        payload.extend_from_slice(&frame.frame_payload);
    }
    if payload.len() != payload_len {
        return Err(contract_violation(
            "connector staged report reassembly produced an unexpected payload length",
        ));
    }
    let report = ConnectorStagedReport::try_new(
        identity,
        first.contract_version,
        ConnectorWriterTerminalState::Staged,
        ConnectorStagedReportSummary {
            input_rows: first.input_rows,
            staged_bytes: first.staged_bytes,
            artifact_count: first.artifact_count,
        },
        Bytes::from(payload),
    )
    .map_err(|error| {
        contract_violation(format!(
            "connector staged report reconstruction failed: {error}"
        ))
    })?;
    if report.payload_digest().as_slice() != first.logical_payload_sha256.as_slice() {
        return Err(contract_violation(
            "connector staged report reassembly changed its logical payload digest",
        ));
    }
    report.validate().map_err(|error| {
        contract_violation(format!(
            "connector staged report reconstruction is corrupt: {error}"
        ))
    })?;
    Ok(report)
}

#[allow(
    dead_code,
    reason = "Retained for native connector staged-report contract regression coverage."
)]
pub(crate) fn decode_connector_staged_report_frame(
    frame: &novarocks::ConnectorStagedReportFrame,
) -> Result<novarocks_spi::connector::ConnectorStagedReportFrame, DistributedQueryError> {
    validate_connector_frame_bounds(frame).map_err(contract_violation)?;
    let writer =
        connector_writer_identity_from_native(frame.writer.as_ref().ok_or_else(|| {
            contract_violation("connector staged report frame is missing writer identity")
        })?)?;
    let state = match frame.terminal_state {
        CONNECTOR_WRITER_TERMINAL_STAGED => ConnectorWriterTerminalState::Staged,
        1 => ConnectorWriterTerminalState::Aborted,
        2 => ConnectorWriterTerminalState::Failed,
        _ => {
            return Err(contract_violation(
                "connector staged report frame has an unknown terminal state",
            ));
        }
    };
    let decoded = novarocks_spi::connector::ConnectorStagedReportFrame::try_new(
        writer,
        frame.contract_version,
        state,
        ConnectorStagedReportSummary {
            input_rows: frame.input_rows,
            staged_bytes: frame.staged_bytes,
            artifact_count: frame.artifact_count,
        },
        frame.part_index,
        frame.part_count,
        frame.logical_payload_len,
        connector_digest_bytes(
            &frame.logical_payload_sha256,
            "connector staged report logical payload digest",
        )?,
        Bytes::copy_from_slice(&frame.frame_payload),
    )
    .map_err(|error| contract_violation(error.to_string()))?;
    if decoded.frame_payload_digest().as_slice() != frame.frame_payload_sha256.as_slice() {
        return Err(contract_violation(
            "connector staged report frame digest does not match its payload",
        ));
    }
    Ok(decoded)
}

fn connector_writer_identity_from_native(
    writer: &novarocks_proto::plan::ConnectorWriterIdentity,
) -> Result<ConnectorWriterIdentity, DistributedQueryError> {
    let operation_id = ConnectorWriteOperationId::from_bytes(connector_id_bytes(
        &writer.operation_id,
        "connector staged report operation id",
    )?);
    let execution_id = ConnectorWriteExecutionId::new(
        connector_id_bytes(
            &writer.execution_query_id,
            "connector staged report execution query id",
        )?,
        writer.execution_attempt_id,
    );
    let fragment_instance_id = writer
        .fragment_instance_id
        .as_ref()
        .map(|id| UniqueId::new(id.hi, id.lo))
        .ok_or_else(|| {
            contract_violation("connector staged report writer is missing fragment instance id")
        })?;
    let binding_key = ConnectorExecutionBindingKey {
        instance_id: ConnectorInstanceId::parse(&writer.connector_instance_id).map_err(
            |error| {
                contract_violation(format!(
                    "connector staged report has invalid binding instance id: {error}"
                ))
            },
        )?,
        incarnation: ConnectorInstanceIncarnation::from_bytes(connector_id_bytes(
            &writer.connector_incarnation,
            "connector staged report binding incarnation",
        )?),
    };
    let fragment_id = writer.fragment_id;
    Ok(ConnectorWriterIdentity::new(
        operation_id,
        novarocks_spi::connector::ConnectorWriteCohortId::from_bytes(connector_digest_bytes(
            &writer.cohort_id,
            "connector staged report cohort id",
        )?),
        execution_id,
        unique_id_to_be_bytes(fragment_instance_id),
        fragment_id,
        writer.backend_num,
        writer.sink_ordinal,
        binding_key,
    ))
}

fn connector_id_bytes(
    bytes: &[u8],
    field: &'static str,
) -> Result<[u8; 16], DistributedQueryError> {
    bytes
        .try_into()
        .map_err(|_| contract_violation(format!("{field} must contain exactly 16 bytes")))
}

fn connector_digest_bytes(
    bytes: &[u8],
    field: &'static str,
) -> Result<[u8; 32], DistributedQueryError> {
    bytes
        .try_into()
        .map_err(|_| contract_violation(format!("{field} must contain exactly 32 bytes")))
}

/// Strictly validate the bounded wire framing for one logical provider staged
/// report.  The coordinator accepts identical duplicated parts (a final
/// report RPC may be retried) but rejects any conflicting duplicate or
/// incomplete logical payload before it reaches a provider control binding.
fn validate_connector_staged_report_frames(
    frames: &[novarocks::ConnectorStagedReportFrame],
    query_id: UniqueId,
    fragment_instance_id: UniqueId,
    backend_num: i32,
    expected_execution_id: Option<QueryExecutionId>,
    expected_fragment_id: Option<u32>,
) -> Result<(), String> {
    if frames.is_empty() {
        return Ok(());
    }

    let first = &frames[0];
    validate_connector_frame_identity(
        first,
        query_id,
        fragment_instance_id,
        backend_num,
        expected_execution_id,
        expected_fragment_id,
    )?;
    validate_connector_frame_bounds(first)?;

    let writer = first
        .writer
        .as_ref()
        .expect("validated connector frame has writer identity");
    let logical_digest = &first.logical_payload_sha256;
    let mut parts = BTreeMap::<u32, &novarocks::ConnectorStagedReportFrame>::new();
    for frame in frames {
        validate_connector_frame_identity(
            frame,
            query_id,
            fragment_instance_id,
            backend_num,
            expected_execution_id,
            expected_fragment_id,
        )?;
        validate_connector_frame_bounds(frame)?;
        let current = frame
            .writer
            .as_ref()
            .expect("validated connector frame has writer identity");
        if current != writer
            || frame.contract_version != first.contract_version
            || frame.terminal_state != first.terminal_state
            || frame.input_rows != first.input_rows
            || frame.staged_bytes != first.staged_bytes
            || frame.artifact_count != first.artifact_count
            || frame.part_count != first.part_count
            || frame.logical_payload_len != first.logical_payload_len
            || frame.logical_payload_sha256 != *logical_digest
        {
            return Err(
                "connector staged report frames disagree on their logical report identity"
                    .to_string(),
            );
        }
        match parts.entry(frame.part_index) {
            std::collections::btree_map::Entry::Vacant(entry) => {
                entry.insert(frame);
            }
            std::collections::btree_map::Entry::Occupied(entry) if *entry.get() == frame => {}
            std::collections::btree_map::Entry::Occupied(_) => {
                return Err(
                    "connector staged report contains conflicting duplicate report part"
                        .to_string(),
                );
            }
        }
    }
    if parts.len() != first.part_count as usize || !parts.keys().copied().eq(0..first.part_count) {
        return Err("connector staged report is missing one or more report parts".to_string());
    }

    let payload_len = parts.values().try_fold(0usize, |total, frame| {
        total
            .checked_add(frame.frame_payload.len())
            .ok_or_else(|| "connector staged report payload length overflowed".to_string())
    })?;
    if payload_len != first.logical_payload_len as usize {
        return Err(
            "connector staged report logical payload length does not match frames".to_string(),
        );
    }
    let mut digest = Sha256::new();
    for frame in parts.values() {
        digest.update(&frame.frame_payload);
    }
    if digest.finalize().as_slice() != logical_digest.as_slice() {
        return Err(
            "connector staged report logical payload digest does not match frames".to_string(),
        );
    }
    Ok(())
}

fn validate_connector_frame_bounds(
    frame: &novarocks::ConnectorStagedReportFrame,
) -> Result<(), String> {
    if frame.contract_version != CONNECTOR_WRITE_CONTRACT_VERSION
        || frame.terminal_state != CONNECTOR_WRITER_TERMINAL_STAGED
        || frame.part_count == 0
        || frame.part_count > MAX_CONNECTOR_STAGED_REPORT_PARTS
        || frame.part_index >= frame.part_count
        || frame.logical_payload_len > MAX_CONNECTOR_STAGED_REPORT_PAYLOAD_BYTES as u64
        || frame.frame_payload.len() > MAX_CONNECTOR_STAGED_REPORT_FRAME_BYTES
        || frame.logical_payload_sha256.len() != 32
        || frame.frame_payload_sha256.len() != 32
    {
        return Err("connector staged report frame violates its bounded wire contract".to_string());
    }
    if Sha256::digest(&frame.frame_payload).as_slice() != frame.frame_payload_sha256.as_slice() {
        return Err("connector staged report frame digest does not match its payload".to_string());
    }
    Ok(())
}

fn validate_connector_frame_identity(
    frame: &novarocks::ConnectorStagedReportFrame,
    query_id: UniqueId,
    fragment_instance_id: UniqueId,
    backend_num: i32,
    expected_execution_id: Option<QueryExecutionId>,
    expected_fragment_id: Option<u32>,
) -> Result<(), String> {
    let writer = frame
        .writer
        .as_ref()
        .ok_or_else(|| "connector staged report frame is missing writer identity".to_string())?;
    let wire_query_id = unique_id_to_be_bytes(query_id);
    if writer.cohort_id.len() != 32 {
        return Err(
            "connector staged report writer cohort id must contain exactly 32 bytes".to_string(),
        );
    }
    if writer.operation_id.len() != 16
        || writer.operation_id.iter().all(|byte| *byte == 0)
        || writer.execution_query_id != wire_query_id
        || writer.execution_attempt_id == 0
        || writer.fragment_instance_id.as_ref().is_none_or(|id| {
            id.hi != fragment_instance_id.high() || id.lo != fragment_instance_id.low()
        })
        || writer.backend_num != backend_num
        || writer.sink_ordinal != 0
        || writer.connector_incarnation.len() != 16
        || writer.connector_incarnation.iter().all(|byte| *byte == 0)
        || ConnectorInstanceId::parse(&writer.connector_instance_id).is_err()
    {
        return Err(
            "connector staged report writer identity does not match its native owner".to_string(),
        );
    }
    if let Some(execution_id) = expected_execution_id
        && (writer.execution_query_id != query_id_to_be_bytes(execution_id)
            || writer.execution_attempt_id != execution_id.attempt_id().get())
    {
        return Err(
            "connector staged report belongs to a different query execution attempt".to_string(),
        );
    }
    if let Some(fragment_id) = expected_fragment_id
        && i64::from(writer.fragment_id) != i64::from(fragment_id)
    {
        return Err("connector staged report belongs to a different writer fragment".to_string());
    }
    Ok(())
}

fn unique_id_to_be_bytes(id: UniqueId) -> [u8; 16] {
    let mut bytes = [0; 16];
    bytes[..8].copy_from_slice(&id.high().to_be_bytes());
    bytes[8..].copy_from_slice(&id.low().to_be_bytes());
    bytes
}

fn query_id_to_be_bytes(execution_id: QueryExecutionId) -> [u8; 16] {
    let mut bytes = [0; 16];
    bytes[..8].copy_from_slice(&execution_id.query_id().high().to_be_bytes());
    bytes[8..].copy_from_slice(&execution_id.query_id().low().to_be_bytes());
    bytes
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::query_execution::contract::QueryId;
    use novarocks_proto::lifecycle::{AttemptId, QueryTerminalSnapshot};
    use novarocks_proto::{common, plan};
    use prost::Message;

    fn query_id() -> UniqueId {
        UniqueId::new(7, 11)
    }

    fn fragment_instance_id() -> UniqueId {
        UniqueId::new(13, 17)
    }

    fn execution_id() -> QueryExecutionId {
        QueryExecutionId::new(
            QueryId::new(query_id().high(), query_id().low()),
            AttemptId::new(23).expect("nonzero attempt"),
        )
        .expect("execution id")
    }

    fn frame(
        payload: &[u8],
        part_index: u32,
        part_count: u32,
        logical_payload: &[u8],
    ) -> novarocks::ConnectorStagedReportFrame {
        novarocks::ConnectorStagedReportFrame {
            contract_version: CONNECTOR_WRITE_CONTRACT_VERSION,
            writer: Some(plan::ConnectorWriterIdentity {
                operation_id: vec![1; 16],
                cohort_id: vec![3; 32],
                execution_query_id: query_id_to_be_bytes(execution_id()).to_vec(),
                execution_attempt_id: execution_id().attempt_id().get(),
                fragment_instance_id: Some(common::UniqueId {
                    hi: fragment_instance_id().high(),
                    lo: fragment_instance_id().low(),
                }),
                fragment_id: 29,
                backend_num: 31,
                sink_ordinal: 0,
                connector_instance_id: "unit".to_string(),
                connector_incarnation: vec![2; 16],
            }),
            terminal_state: CONNECTOR_WRITER_TERMINAL_STAGED,
            input_rows: 37,
            staged_bytes: logical_payload.len() as u64,
            artifact_count: 1,
            part_index,
            part_count,
            logical_payload_len: logical_payload.len() as u64,
            logical_payload_sha256: Sha256::digest(logical_payload).to_vec(),
            frame_payload: payload.to_vec(),
            frame_payload_sha256: Sha256::digest(payload).to_vec(),
        }
    }

    fn writer_commit_input(
        frames: Vec<novarocks::ConnectorStagedReportFrame>,
    ) -> WriterCommitInput {
        WriterCommitInput {
            writer_id: 0,
            fragment_id: 29,
            writer_key: WriterKey {
                query_id: query_id(),
                fragment_instance_id: fragment_instance_id(),
                backend_num: 31,
            },
            connector_staged_report_frames: frames,
            load_counters: BTreeMap::new(),
            loaded_rows: 37,
            loaded_bytes: 11,
            filtered_rows: 0,
        }
    }

    fn generic_commit(frames: Vec<novarocks::ConnectorStagedReportFrame>) -> WriteCommitInput {
        WriteCommitInput {
            write_id: query_id(),
            writers: vec![writer_commit_input(frames)],
        }
    }

    fn writer_terminal_fragment() -> novarocks::QueryTerminalFragmentSnapshot {
        novarocks::QueryTerminalFragmentSnapshot {
            fragment_instance_id: Some(common::UniqueId {
                hi: fragment_instance_id().high(),
                lo: fragment_instance_id().low(),
            }),
            backend_num: 31,
            outcome: novarocks::QueryTerminalFragmentOutcome::Succeeded as i32,
            connector_staged_report_frames: vec![frame(b"report", 0, 1, b"report")],
            load_stats: Some(novarocks::QueryTerminalLoadStats::default()),
            profile: Some(novarocks::FragmentTerminalProfileTelemetry {
                telemetry: Some(
                    novarocks::fragment_terminal_profile_telemetry::Telemetry::Unavailable(
                        novarocks::TerminalTelemetryUnavailable {
                            stage: "test".to_string(),
                            code: "UNAVAILABLE".to_string(),
                        },
                    ),
                ),
            }),
            ..Default::default()
        }
    }

    fn terminal_snapshot_with_p2(
        profile_contribution: novarocks::QueryTerminalProfileContributionTelemetry,
    ) -> QueryTerminalSnapshot {
        QueryTerminalSnapshot::seal(novarocks::QueryTerminalSnapshot {
            version: 1,
            execution_id: Some(execution_id().into()),
            backend: Some(novarocks::ParticipantBackendIdentity {
                backend_id: 31,
                endpoint: Some(novarocks::QueryControlEndpoint {
                    host: "127.0.0.1".to_string(),
                    port: 9030,
                }),
                start_epoch: 1,
            }),
            init_digest: vec![9; 32],
            fragments: vec![writer_terminal_fragment()],
            profile_contribution: Some(profile_contribution),
            ..Default::default()
        })
        .expect("terminal snapshot")
    }

    fn staged_report_frame_bytes(snapshot: &QueryTerminalSnapshot) -> Vec<Vec<u8>> {
        let mut builder = WriteTerminalBuilder {
            write_id: query_id(),
            expected: BTreeMap::from([(
                WriterKey {
                    query_id: query_id(),
                    fragment_instance_id: fragment_instance_id(),
                    backend_num: 31,
                },
                (0, 29, execution_id(), None),
            )]),
            completed: BTreeMap::new(),
            failure: None,
        };
        builder
            .apply_terminal(&snapshot.as_proto().fragments[0])
            .expect("apply writer terminal");
        let (commit, abort) = builder.finish().expect("complete write").into_payloads();
        assert!(abort.is_none(), "P2 telemetry must not abort the write");
        commit
            .expect("commit payload")
            .writers
            .into_iter()
            .flat_map(|writer| writer.connector_staged_report_frames)
            .map(|frame| frame.encode_to_vec())
            .collect()
    }

    fn registrations() -> WriterRegistrationSet {
        WriterRegistrationSet::new([crate::query_execution::artifact::WriterRegistration::new(
            query_id(),
            execution_id(),
            1,
            fragment_instance_id(),
            0,
            None,
        )])
    }

    /// A latched failure produces an abort payload, never an error.
    ///
    /// The distinction is load bearing at the call site: an abort is something
    /// it can carry through to the write completion, whereas treating "no
    /// commit" as a contract violation reports neither the latch nor its
    /// reason, and drops the abort on the floor with the staged files still in
    /// object storage.
    #[test]
    fn a_latched_failure_yields_an_abort_rather_than_no_outcome_at_all() {
        let mut builder = WriteTerminalBuilder::new(registrations()).expect("builder");
        builder.latch_failure("backend 2 lost");

        let outcome = builder.finish().expect("a latch is an abort, not an error");

        assert!(
            outcome.commit.is_none(),
            "a latched failure must not also claim a commit"
        );
        let abort = outcome.abort.expect("a latched failure produces an abort");
        assert_eq!(abort.reason, "backend 2 lost");
        assert_eq!(
            abort.incomplete_writers.len(),
            1,
            "the writer that never reported is named, so cleanup knows what to look for"
        );
    }

    /// Without a latch, a writer set that never reported is a failure in its own
    /// right, and it says so rather than silently producing no commit.
    #[test]
    fn missing_writer_reports_fail_by_name_without_a_latch() {
        let Err(error) = WriteTerminalBuilder::new(registrations())
            .expect("builder")
            .finish()
        else {
            panic!("an unreported writer cannot be committed");
        };
        assert!(
            error
                .message()
                .contains("before all writer reports arrived"),
            "{error}"
        );
    }

    #[test]
    fn p2_unavailable_preserves_staged_report_frame_bytes() {
        let available =
            terminal_snapshot_with_p2(novarocks::QueryTerminalProfileContributionTelemetry {
                telemetry: Some(
                    novarocks::query_terminal_profile_contribution_telemetry::Telemetry::Available(
                        novarocks::QueryTerminalProfileContributionV1 {
                            version: 1,
                            ..Default::default()
                        },
                    ),
                ),
            });
        let unavailable = terminal_snapshot_with_p2(
            novarocks::QueryTerminalProfileContributionTelemetry {
                telemetry: Some(
                    novarocks::query_terminal_profile_contribution_telemetry::Telemetry::Unavailable(
                        novarocks::TerminalTelemetryUnavailable {
                            stage: "runtime_filter_terminal_capture".to_string(),
                            code: "INJECTED_P2_ASSEMBLY_FAILURE".to_string(),
                        },
                    ),
                ),
            },
        );

        assert_eq!(
            staged_report_frame_bytes(&available),
            staged_report_frame_bytes(&unavailable),
            "P2 observation changes must not alter staged write evidence"
        );
    }

    #[test]
    fn staged_report_frames_accept_out_of_order_identical_retries() {
        let first = frame(b"first", 0, 2, b"firstsecond");
        let second = frame(b"second", 1, 2, b"firstsecond");
        assert!(
            validate_connector_staged_report_frames(
                &[second, first.clone(), first],
                query_id(),
                fragment_instance_id(),
                31,
                Some(execution_id()),
                Some(29),
            )
            .is_ok()
        );
    }

    #[test]
    fn staged_report_frames_reject_conflicting_duplicate_part() {
        let first = frame(b"first", 0, 2, b"firstsecond");
        let mut conflicting = first.clone();
        conflicting.frame_payload = b"other".to_vec();
        conflicting.frame_payload_sha256 = Sha256::digest(&conflicting.frame_payload).to_vec();
        let second = frame(b"second", 1, 2, b"firstsecond");
        let error = validate_connector_staged_report_frames(
            &[first, conflicting, second],
            query_id(),
            fragment_instance_id(),
            31,
            Some(execution_id()),
            Some(29),
        )
        .expect_err("conflicting frame duplicate must fail closed");
        assert!(error.contains("conflicting duplicate"));
    }

    #[test]
    fn staged_report_frames_reject_wrong_attempt_and_missing_part() {
        let mut wrong_attempt = frame(b"first", 0, 1, b"first");
        wrong_attempt
            .writer
            .as_mut()
            .expect("writer")
            .execution_attempt_id = 99;
        let error = validate_connector_staged_report_frames(
            &[wrong_attempt],
            query_id(),
            fragment_instance_id(),
            31,
            Some(execution_id()),
            Some(29),
        )
        .expect_err("wrong attempt must fail closed");
        assert!(error.contains("different query execution attempt"));

        let missing = frame(b"first", 0, 2, b"firstsecond");
        let error = validate_connector_staged_report_frames(
            &[missing],
            query_id(),
            fragment_instance_id(),
            31,
            Some(execution_id()),
            Some(29),
        )
        .expect_err("missing part must fail closed");
        assert!(error.contains("missing"));
    }

    #[test]
    fn staged_report_frames_require_a_valid_frame_digest() {
        let staged = frame(b"first", 0, 1, b"first");
        let mut corrupt = staged;
        corrupt.frame_payload_sha256 = vec![0; 32];
        let error = validate_connector_staged_report_frames(
            &[corrupt],
            query_id(),
            fragment_instance_id(),
            31,
            Some(execution_id()),
            Some(29),
        )
        .expect_err("corrupt frame digest must fail closed");
        assert!(error.contains("frame digest"));
    }

    #[test]
    fn connector_write_commit_input_reassembles_bounded_staged_reports() {
        let first = frame(b"first", 0, 2, b"firstsecond");
        let second = frame(b"second", 1, 2, b"firstsecond");
        let input = generic_commit(vec![second, first]);

        let extracted = ConnectorWriteCommitInput::try_extract(&input)
            .expect("valid generic carrier")
            .expect("generic carrier");
        assert_eq!(extracted.operation_id().to_bytes(), [1; 16]);
        assert_eq!(extracted.cohort_id().to_bytes(), [3; 32]);
        assert_eq!(
            extracted.reports()[0].writer().cohort_id().to_bytes(),
            [3; 32]
        );
        assert_eq!(
            extracted.execution_id().query_id(),
            query_id_to_be_bytes(execution_id())
        );
        assert_eq!(
            extracted.execution_id().attempt_id(),
            execution_id().attempt_id().get()
        );
        assert_eq!(extracted.owner().instance_id.as_str(), "unit");
        assert_eq!(extracted.reports().len(), 1);
        assert_eq!(extracted.reports()[0].payload().as_ref(), b"firstsecond");
        assert_eq!(extracted.reports()[0].summary().input_rows, 37);
    }

    #[test]
    fn connector_write_commit_input_rejects_missing_writer_report() {
        let mut missing = generic_commit(vec![frame(b"report", 0, 1, b"report")]);
        let mut absent = writer_commit_input(Vec::new());
        absent.writer_id = 1;
        absent.writer_key.fragment_instance_id = UniqueId::new(41, 43);
        absent.writer_key.backend_num = 47;
        missing.writers.push(absent);
        let error = ConnectorWriteCommitInput::try_extract(&missing)
            .expect_err("missing generic writer report must fail closed");
        assert!(error.message().contains("missing a staged report"));
    }

    #[test]
    fn connector_write_commit_input_requires_exact_cohort_id_width() {
        for invalid in [Vec::new(), vec![7; 31], vec![7; 33]] {
            let mut invalid_frame = frame(b"report", 0, 1, b"report");
            invalid_frame.writer.as_mut().expect("writer").cohort_id = invalid;
            let error =
                ConnectorWriteCommitInput::try_extract(&generic_commit(vec![invalid_frame]))
                    .expect_err("invalid cohort identity width must fail closed");
            assert!(error.message().contains("cohort id"));
        }
    }

    #[test]
    fn connector_write_commit_input_rejects_corrupt_and_duplicate_logical_reports() {
        let mut corrupt = frame(b"report", 0, 1, b"report");
        corrupt.logical_payload_sha256 = vec![0; 32];
        let error = ConnectorWriteCommitInput::try_extract(&generic_commit(vec![corrupt]))
            .expect_err("corrupt generic report must fail closed");
        assert!(error.message().contains("logical payload digest"));

        let first = writer_commit_input(vec![frame(b"report", 0, 1, b"report")]);
        let second = first.clone();
        let error = ConnectorWriteCommitInput::try_extract(&WriteCommitInput {
            write_id: query_id(),
            writers: vec![first, second],
        })
        .expect_err("duplicate logical writer report must fail closed");
        assert!(error.message().contains("duplicate registered writer"));
    }

    #[test]
    fn connector_write_commit_input_rejects_mixed_cohorts_in_one_attempt() {
        let first = writer_commit_input(vec![frame(b"first", 0, 1, b"first")]);
        let mut second_frame = frame(b"second", 0, 1, b"second");
        let second_writer = second_frame.writer.as_mut().expect("writer identity");
        second_writer.cohort_id = vec![4; 32];
        second_writer.fragment_instance_id = Some(common::UniqueId { hi: 41, lo: 43 });
        second_writer.backend_num = 47;
        let mut second = writer_commit_input(vec![second_frame]);
        second.writer_id = 1;
        second.writer_key.fragment_instance_id = UniqueId::new(41, 43);
        second.writer_key.backend_num = 47;

        let error = ConnectorWriteCommitInput::try_extract(&WriteCommitInput {
            write_id: query_id(),
            writers: vec![first, second],
        })
        .expect_err("one attempt cannot mix cohort reports");
        assert!(error.message().contains("multiple write cohorts"));
    }

    #[test]
    fn connector_write_commit_input_groups_three_writers_by_two_cohorts() {
        let first = writer_commit_input(vec![frame(b"first", 0, 1, b"first")]);

        let mut second_frame = frame(b"second", 0, 1, b"second");
        let second_identity = second_frame.writer.as_mut().expect("second identity");
        second_identity.fragment_id = 30;
        second_identity.fragment_instance_id = Some(common::UniqueId { hi: 41, lo: 43 });
        second_identity.backend_num = 47;
        let mut second = writer_commit_input(vec![second_frame]);
        second.writer_id = 1;
        second.fragment_id = 30;
        second.writer_key.fragment_instance_id = UniqueId::new(41, 43);
        second.writer_key.backend_num = 47;

        let mut third_frame = frame(b"third", 0, 1, b"third");
        let third_identity = third_frame.writer.as_mut().expect("third identity");
        third_identity.cohort_id = vec![4; 32];
        third_identity.fragment_id = 31;
        third_identity.fragment_instance_id = Some(common::UniqueId { hi: 53, lo: 59 });
        third_identity.backend_num = 61;
        let mut third = writer_commit_input(vec![third_frame]);
        third.writer_id = 2;
        third.fragment_id = 31;
        third.writer_key.fragment_instance_id = UniqueId::new(53, 59);
        third.writer_key.backend_num = 61;

        let grouped = ConnectorWriteCommitInput::try_extract_grouped(&WriteCommitInput {
            write_id: query_id(),
            writers: vec![first, second, third],
        })
        .expect("mixed operation reports group by signed cohort");
        assert_eq!(grouped.len(), 2);
        assert_eq!(
            grouped
                .iter()
                .find(|(cohort_id, _)| cohort_id.to_bytes() == [3; 32])
                .map(|(_, input)| input)
                .expect("first cohort")
                .reports()
                .len(),
            2
        );
        assert_eq!(
            grouped
                .iter()
                .find(|(cohort_id, _)| cohort_id.to_bytes() == [4; 32])
                .map(|(_, input)| input)
                .expect("second cohort")
                .reports()
                .len(),
            1
        );
    }
}
