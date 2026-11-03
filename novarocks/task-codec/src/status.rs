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

//! Task status, cursor, subscription, and final task info codec.

use novarocks_execution_contract::failure::{
    FailureClass, FailureEvidence, FailureFact, FailureReason, FailureRowPosition,
    RepresentationBoundary, RepresentationFacts, RepresentationLayout, ResourceDimension,
    ResourceFacts, ResourceScope,
};
use std::time::Duration;

use novarocks_execution_contract::task_execution::domain::DomainVersion;
use novarocks_execution_contract::task_execution::identity::TaskIdentity;
use novarocks_execution_contract::task_execution::status::{
    AbortCause, CancelReason, DynamicFilterAdvertisement, FINAL_TASK_INFO_MAX_OPERATORS,
    FinalTaskInfo, OPERATOR_COUNTER_BUDGET, OperatorCounter, OperatorStatistics,
    SAFE_DETAIL_MAX_BYTES, SafeDetail, TaskFailure, TaskFailurePhase, TaskOutputFacts,
    TaskResourceFacts, TaskState, TaskStatus, TaskStatusCursor, TaskStatusVersion, TaskWriterFacts,
    TerminationDetail,
};
use novarocks_proto_models::novarocks;

use novarocks_proto_codec::{FieldPath, ProtocolError};

use crate::identity::{decode_task_identity, encode_task_identity};
use crate::{inconsistent, invalid, invalid_enum, missing, out_of_range};

/// Largest number of per-task cursors one subscription may carry.
pub const MAX_SUBSCRIPTION_CURSORS: usize = 4096;

fn decode_state(value: i32, path: FieldPath) -> Result<TaskState, ProtocolError> {
    match novarocks::TaskState::try_from(value) {
        Ok(novarocks::TaskState::Planned) => Ok(TaskState::Planned),
        Ok(novarocks::TaskState::Running) => Ok(TaskState::Running),
        Ok(novarocks::TaskState::Flushing) => Ok(TaskState::Flushing),
        Ok(novarocks::TaskState::Finished) => Ok(TaskState::Finished),
        Ok(novarocks::TaskState::Canceling) => Ok(TaskState::Canceling),
        Ok(novarocks::TaskState::Canceled) => Ok(TaskState::Canceled),
        Ok(novarocks::TaskState::Aborting) => Ok(TaskState::Aborting),
        Ok(novarocks::TaskState::Aborted) => Ok(TaskState::Aborted),
        Ok(novarocks::TaskState::Failing) => Ok(TaskState::Failing),
        Ok(novarocks::TaskState::Failed) => Ok(TaskState::Failed),
        Ok(novarocks::TaskState::Unspecified) | Err(_) => Err(invalid_enum(
            path,
            "task state must be a known non-default value",
        )),
    }
}

fn encode_state(value: TaskState) -> i32 {
    let encoded = match value {
        TaskState::Planned => novarocks::TaskState::Planned,
        TaskState::Running => novarocks::TaskState::Running,
        TaskState::Flushing => novarocks::TaskState::Flushing,
        TaskState::Finished => novarocks::TaskState::Finished,
        TaskState::Canceling => novarocks::TaskState::Canceling,
        TaskState::Canceled => novarocks::TaskState::Canceled,
        TaskState::Aborting => novarocks::TaskState::Aborting,
        TaskState::Aborted => novarocks::TaskState::Aborted,
        TaskState::Failing => novarocks::TaskState::Failing,
        TaskState::Failed => novarocks::TaskState::Failed,
    };
    encoded as i32
}

pub(crate) fn decode_cancel_reason(
    value: i32,
    path: FieldPath,
) -> Result<CancelReason, ProtocolError> {
    match novarocks::TaskCancelReason::try_from(value) {
        Ok(novarocks::TaskCancelReason::UpstreamNoLongerNeeded) => {
            Ok(CancelReason::UpstreamNoLongerNeeded)
        }
        Ok(novarocks::TaskCancelReason::Unspecified) | Err(_) => Err(invalid_enum(
            path,
            "cancel reason must be a known non-default value",
        )),
    }
}

pub(crate) fn encode_cancel_reason(value: CancelReason) -> i32 {
    let encoded = match value {
        CancelReason::UpstreamNoLongerNeeded => novarocks::TaskCancelReason::UpstreamNoLongerNeeded,
    };
    encoded as i32
}

pub(crate) fn decode_abort_cause(value: i32, path: FieldPath) -> Result<AbortCause, ProtocolError> {
    match novarocks::ParticipationAbortCause::try_from(value) {
        Ok(novarocks::ParticipationAbortCause::QueryFailed) => Ok(AbortCause::QueryFailed),
        Ok(novarocks::ParticipationAbortCause::LeaseExpired) => Ok(AbortCause::LeaseExpired),
        Ok(novarocks::ParticipationAbortCause::PeerTaskFailed) => Ok(AbortCause::PeerTaskFailed),
        Ok(novarocks::ParticipationAbortCause::Unspecified) | Err(_) => Err(invalid_enum(
            path,
            "abort cause must be a known non-default value",
        )),
    }
}

pub(crate) fn encode_abort_cause(value: AbortCause) -> i32 {
    let encoded = match value {
        AbortCause::QueryFailed => novarocks::ParticipationAbortCause::QueryFailed,
        AbortCause::LeaseExpired => novarocks::ParticipationAbortCause::LeaseExpired,
        AbortCause::PeerTaskFailed => novarocks::ParticipationAbortCause::PeerTaskFailed,
    };
    encoded as i32
}

use std::num::NonZeroU16;

fn decode_failure_class(value: i32, path: FieldPath) -> Result<FailureClass, ProtocolError> {
    match novarocks::FailureClass::try_from(value) {
        Ok(novarocks::FailureClass::Data) => Ok(FailureClass::Data),
        Ok(novarocks::FailureClass::Dependency) => Ok(FailureClass::Dependency),
        Ok(novarocks::FailureClass::Resource) => Ok(FailureClass::Resource),
        Ok(novarocks::FailureClass::Representation) => Ok(FailureClass::Representation),
        Ok(novarocks::FailureClass::Invariant) => Ok(FailureClass::Invariant),
        Ok(novarocks::FailureClass::Unexpected) => Ok(FailureClass::Unexpected),
        Ok(novarocks::FailureClass::Control) => Ok(FailureClass::Control),
        Ok(novarocks::FailureClass::Unsupported) => Ok(FailureClass::Unsupported),
        _ => Err(invalid_enum(
            path,
            "failure_class must be a known non-default value",
        )),
    }
}
fn encode_failure_class(value: FailureClass) -> i32 {
    match value {
        FailureClass::Data => novarocks::FailureClass::Data as i32,
        FailureClass::Dependency => novarocks::FailureClass::Dependency as i32,
        FailureClass::Resource => novarocks::FailureClass::Resource as i32,
        FailureClass::Representation => novarocks::FailureClass::Representation as i32,
        FailureClass::Invariant => novarocks::FailureClass::Invariant as i32,
        FailureClass::Unexpected => novarocks::FailureClass::Unexpected as i32,
        FailureClass::Control => novarocks::FailureClass::Control as i32,
        FailureClass::Unsupported => novarocks::FailureClass::Unsupported as i32,
    }
}
fn decode_failure_evidence(value: i32, path: FieldPath) -> Result<FailureEvidence, ProtocolError> {
    match novarocks::FailureEvidence::try_from(value) {
        Ok(novarocks::FailureEvidence::Originating) => Ok(FailureEvidence::Originating),
        Ok(novarocks::FailureEvidence::StopObservation) => Ok(FailureEvidence::StopObservation),
        _ => Err(invalid_enum(
            path,
            "failure_evidence must be a known non-default value",
        )),
    }
}
fn encode_failure_evidence(value: FailureEvidence) -> i32 {
    match value {
        FailureEvidence::Originating => novarocks::FailureEvidence::Originating as i32,
        FailureEvidence::StopObservation => novarocks::FailureEvidence::StopObservation as i32,
    }
}
fn decode_resource_scope(value: i32, path: FieldPath) -> Result<ResourceScope, ProtocolError> {
    match novarocks::FailureResourceScope::try_from(value) {
        Ok(novarocks::FailureResourceScope::Request) => Ok(ResourceScope::Request),
        Ok(novarocks::FailureResourceScope::Operator) => Ok(ResourceScope::Operator),
        Ok(novarocks::FailureResourceScope::Driver) => Ok(ResourceScope::Driver),
        Ok(novarocks::FailureResourceScope::Task) => Ok(ResourceScope::Task),
        Ok(novarocks::FailureResourceScope::Query) => Ok(ResourceScope::Query),
        Ok(novarocks::FailureResourceScope::Process) => Ok(ResourceScope::Process),
        _ => Err(invalid_enum(
            path,
            "resource_scope must be a known non-default value",
        )),
    }
}
fn encode_resource_scope(value: ResourceScope) -> i32 {
    match value {
        ResourceScope::Request => novarocks::FailureResourceScope::Request as i32,
        ResourceScope::Operator => novarocks::FailureResourceScope::Operator as i32,
        ResourceScope::Driver => novarocks::FailureResourceScope::Driver as i32,
        ResourceScope::Task => novarocks::FailureResourceScope::Task as i32,
        ResourceScope::Query => novarocks::FailureResourceScope::Query as i32,
        ResourceScope::Process => novarocks::FailureResourceScope::Process as i32,
    }
}
fn decode_resource_dimension(
    value: i32,
    path: FieldPath,
) -> Result<ResourceDimension, ProtocolError> {
    match novarocks::FailureResourceDimension::try_from(value) {
        Ok(novarocks::FailureResourceDimension::Bytes) => Ok(ResourceDimension::Bytes),
        Ok(novarocks::FailureResourceDimension::Items) => Ok(ResourceDimension::Items),
        Ok(novarocks::FailureResourceDimension::Positions) => Ok(ResourceDimension::Positions),
        _ => Err(invalid_enum(
            path,
            "resource_dimension must be a known non-default value",
        )),
    }
}
fn encode_resource_dimension(value: ResourceDimension) -> i32 {
    match value {
        ResourceDimension::Bytes => novarocks::FailureResourceDimension::Bytes as i32,
        ResourceDimension::Items => novarocks::FailureResourceDimension::Items as i32,
        ResourceDimension::Positions => novarocks::FailureResourceDimension::Positions as i32,
    }
}
fn decode_representation_boundary(
    value: i32,
    path: FieldPath,
) -> Result<RepresentationBoundary, ProtocolError> {
    match novarocks::FailureRepresentationBoundary::try_from(value) {
        Ok(novarocks::FailureRepresentationBoundary::MinimumUnit) => {
            Ok(RepresentationBoundary::MinimumUnit)
        }
        Ok(novarocks::FailureRepresentationBoundary::SingleOutput) => {
            Ok(RepresentationBoundary::SingleOutput)
        }
        _ => Err(invalid_enum(
            path,
            "representation_boundary must be a known non-default value",
        )),
    }
}
fn encode_representation_boundary(value: RepresentationBoundary) -> i32 {
    match value {
        RepresentationBoundary::MinimumUnit => {
            novarocks::FailureRepresentationBoundary::MinimumUnit as i32
        }
        RepresentationBoundary::SingleOutput => {
            novarocks::FailureRepresentationBoundary::SingleOutput as i32
        }
    }
}
fn decode_nonzero_u16(value: u32, path: FieldPath) -> Result<NonZeroU16, ProtocolError> {
    u16::try_from(value)
        .ok()
        .and_then(NonZeroU16::new)
        .ok_or_else(|| out_of_range(path, "identifier must be a nonzero u16"))
}
/// Decode the canonical neutral failure vocabulary without a Task envelope.
pub fn decode_failure_fact(
    src: &novarocks::FailureFact,
    path: FieldPath,
) -> Result<FailureFact, ProtocolError> {
    let class = decode_failure_class(src.class, path.clone().field("class"))?;
    let evidence = decode_failure_evidence(src.evidence, path.clone().field("evidence"))?;
    let reason = src.reason.as_ref().ok_or_else(|| {
        missing(
            path.clone().field("reason"),
            "failure fact requires a reason",
        )
    })?;
    let reason = FailureReason::new(
        decode_nonzero_u16(reason.domain, path.clone().field("reason").field("domain"))?,
        decode_nonzero_u16(reason.code, path.clone().field("reason").field("code"))?,
    );
    let mut fact = match evidence {
        FailureEvidence::Originating => FailureFact::new(class, reason),
        FailureEvidence::StopObservation => {
            if class != FailureClass::Control {
                return Err(inconsistent(
                    path.clone().field("class"),
                    "stop observation requires Control class",
                ));
            }
            FailureFact::stop_observation(reason)
        }
    };
    if let Some(row) = &src.row_position {
        fact = fact.with_row_position(FailureRowPosition {
            selected_ordinal: row.selected_ordinal,
            batch_row: row.batch_row,
        });
    }
    if let Some(r) = &src.resource {
        fact = fact.with_resource(decode_resource_facts(r, path.clone().field("resource"))?);
    }
    if let Some(r) = &src.representation {
        let p = path.field("representation");
        let layout = r.layout.as_ref().ok_or_else(|| {
            missing(
                p.clone().field("layout"),
                "representation requires a layout",
            )
        })?;
        fact = fact.with_representation(RepresentationFacts {
            boundary: decode_representation_boundary(r.boundary, p.clone().field("boundary"))?,
            layout: RepresentationLayout {
                domain: decode_nonzero_u16(
                    layout.domain,
                    p.clone().field("layout").field("domain"),
                )?,
                code: decode_nonzero_u16(layout.code, p.field("layout").field("code"))?,
            },
            required: r.required,
            extent_limit: r.extent_limit,
            representable_prefix: r.representable_prefix,
        });
    }
    Ok(fact)
}
pub(crate) fn decode_resource_facts(
    r: &novarocks::FailureResourceFacts,
    p: FieldPath,
) -> Result<ResourceFacts, ProtocolError> {
    Ok(ResourceFacts {
        request_scope: r
            .request_scope
            .map(|v| decode_resource_scope(v, p.clone().field("request_scope")))
            .transpose()?,
        constraint_scope: r
            .constraint_scope
            .map(|v| decode_resource_scope(v, p.clone().field("constraint_scope")))
            .transpose()?,
        dimension: decode_resource_dimension(r.dimension, p.field("dimension"))?,
        requested: r.requested,
        required_growth: r.required_growth,
        limit: r.limit,
        live: r.live,
        committed: r.committed,
    })
}
pub(crate) fn encode_resource_facts(r: ResourceFacts) -> novarocks::FailureResourceFacts {
    novarocks::FailureResourceFacts {
        request_scope: r.request_scope.map(encode_resource_scope),
        constraint_scope: r.constraint_scope.map(encode_resource_scope),
        dimension: encode_resource_dimension(r.dimension),
        requested: r.requested,
        required_growth: r.required_growth,
        limit: r.limit,
        live: r.live,
        committed: r.committed,
    }
}

/// Encode the canonical neutral failure vocabulary without reclassification.
pub fn encode_failure_fact(fact: FailureFact) -> novarocks::FailureFact {
    novarocks::FailureFact {
        class: encode_failure_class(fact.class()),
        evidence: encode_failure_evidence(fact.evidence()),
        reason: Some(novarocks::FailureReason {
            domain: u32::from(fact.reason().domain()),
            code: u32::from(fact.reason().code()),
        }),
        row_position: fact
            .row_position()
            .map(|row| novarocks::FailureRowPosition {
                selected_ordinal: row.selected_ordinal,
                batch_row: row.batch_row,
            }),
        resource: fact.resource().map(encode_resource_facts),
        representation: fact
            .representation()
            .map(|r| novarocks::FailureRepresentationFacts {
                boundary: encode_representation_boundary(r.boundary),
                layout: Some(novarocks::FailureRepresentationLayout {
                    domain: u32::from(r.layout.domain.get()),
                    code: u32::from(r.layout.code.get()),
                }),
                required: r.required,
                extent_limit: r.extent_limit,
                representable_prefix: r.representable_prefix,
            }),
    }
}

fn decode_failure_phase(value: i32, path: FieldPath) -> Result<TaskFailurePhase, ProtocolError> {
    match novarocks::TaskFailurePhase::try_from(value) {
        Ok(novarocks::TaskFailurePhase::Preparation) => Ok(TaskFailurePhase::Preparation),
        Ok(novarocks::TaskFailurePhase::Execution) => Ok(TaskFailurePhase::Execution),
        Ok(novarocks::TaskFailurePhase::Unspecified) | Err(_) => {
            Err(invalid_enum(path, "task failure phase must be specified"))
        }
    }
}

fn encode_failure_phase(value: TaskFailurePhase) -> i32 {
    match value {
        TaskFailurePhase::Preparation => novarocks::TaskFailurePhase::Preparation as i32,
        TaskFailurePhase::Execution => novarocks::TaskFailurePhase::Execution as i32,
    }
}

/// Decodes bounded, already-redacted diagnostic text.
pub(crate) fn decode_safe_detail(
    value: &str,
    path: FieldPath,
) -> Result<SafeDetail, ProtocolError> {
    if value.len() > SAFE_DETAIL_MAX_BYTES {
        return Err(out_of_range(
            path,
            "safe detail exceeds the redacted text limit",
        ));
    }
    SafeDetail::new(value).map_err(|error| out_of_range(path, error.to_string()))
}

fn decode_termination(
    src: &novarocks::TaskTermination,
    path: FieldPath,
) -> Result<TerminationDetail, ProtocolError> {
    let cause = src
        .cause
        .as_ref()
        .ok_or_else(|| missing(path.clone(), "task termination requires a cause"))?;
    Ok(match cause {
        novarocks::task_termination::Cause::Canceled(value) => {
            TerminationDetail::Canceled(decode_cancel_reason(*value, path.field("canceled"))?)
        }
        novarocks::task_termination::Cause::Aborted(value) => {
            TerminationDetail::Aborted(decode_abort_cause(*value, path.field("aborted"))?)
        }
        novarocks::task_termination::Cause::Failed(failure) => {
            let failure_path = path.field("failed");
            let fact = failure.fact.as_ref().ok_or_else(|| {
                missing(
                    failure_path.clone().field("fact"),
                    "task failure requires producer facts",
                )
            })?;
            let fact = decode_failure_fact(fact, failure_path.clone().field("fact"))?;
            let detail = decode_safe_detail(
                &failure.safe_detail,
                failure_path.clone().field("safe_detail"),
            )?;
            let phase = decode_failure_phase(failure.phase, failure_path.field("phase"))?;
            TerminationDetail::Failed(TaskFailure::new_in_phase(fact, detail, phase))
        }
    })
}

fn encode_termination(value: &TerminationDetail) -> novarocks::TaskTermination {
    let cause = match value {
        TerminationDetail::Canceled(reason) => {
            novarocks::task_termination::Cause::Canceled(encode_cancel_reason(*reason))
        }
        TerminationDetail::Aborted(cause) => {
            novarocks::task_termination::Cause::Aborted(encode_abort_cause(*cause))
        }
        TerminationDetail::Failed(failure) => {
            novarocks::task_termination::Cause::Failed(novarocks::TaskFailure {
                fact: Some(encode_failure_fact(failure.fact())),
                safe_detail: failure.detail().as_str().to_owned(),
                phase: encode_failure_phase(failure.phase()),
            })
        }
    };
    novarocks::TaskTermination { cause: Some(cause) }
}

/// Decodes one immutable status snapshot.
pub fn decode_task_status(
    src: &novarocks::TaskStatus,
    path: FieldPath,
) -> Result<TaskStatus, ProtocolError> {
    let identity = src.identity.as_ref().ok_or_else(|| {
        missing(
            path.clone().field("identity"),
            "task status requires an identity",
        )
    })?;
    let identity = decode_task_identity(identity, path.clone().field("identity"))?;
    let version = TaskStatusVersion::new(src.status_version)
        .map_err(|error| invalid(path.clone().field("status_version"), error.to_string()))?;
    let state = decode_state(src.state, path.clone().field("state"))?;
    let termination = match src.termination.as_ref() {
        Some(termination) => Some(decode_termination(
            termination,
            path.clone().field("termination"),
        )?),
        None => None,
    };
    let output = src.output.as_ref().ok_or_else(|| {
        missing(
            path.clone().field("output"),
            "task status requires output facts",
        )
    })?;
    let mut facts = TaskOutputFacts::new(output.responsibility_complete);
    if let (Some(rows), Some(bytes)) = (output.buffered_rows, output.buffered_bytes) {
        facts = facts.with_buffered(rows, bytes);
    } else if output.buffered_rows.is_some() != output.buffered_bytes.is_some() {
        return Err(inconsistent(
            path.field("output"),
            "buffered rows and buffered bytes must be reported together",
        ));
    }

    let installed = src.installed.ok_or_else(|| {
        missing(
            path.clone().field("installed"),
            "task status requires an installed history fact",
        )
    })?;
    let status =
        TaskStatus::try_new_with_installed(identity, version, state, termination, facts, installed)
            .map_err(|error| inconsistent(path.clone(), error.to_string()))?;

    let status = match (src.dynamic_filter_version, src.dynamic_filter_domain_count) {
        (Some(version), Some(count)) => {
            let version = DomainVersion::new(version).map_err(|error| {
                invalid(
                    path.clone().field("dynamic_filter_version"),
                    error.to_string(),
                )
            })?;
            status.with_dynamic_filters(DynamicFilterAdvertisement::new(version, count))
        }
        (None, None) => status,
        _ => {
            return Err(inconsistent(
                path.clone(),
                "dynamic filter version and domain count must be reported together",
            ));
        }
    };

    let status = match src.resources.as_ref() {
        Some(resources) => status.with_resources(decode_resources(resources)),
        None => status,
    };
    let status = match src.writer.as_ref() {
        Some(writer) => status.with_writer(decode_writer(writer, path.field("writer"))?),
        None => status,
    };
    Ok(status)
}

fn decode_resources(src: &novarocks::TaskResourceFacts) -> TaskResourceFacts {
    let mut facts = TaskResourceFacts::empty();
    if let Some(value) = src.queued_splits {
        facts = facts.with_queued_splits(value);
    }
    if let Some(value) = src.running_drivers {
        facts = facts.with_running_drivers(value);
    }
    if let Some(value) = src.memory_reservation_bytes {
        facts = facts.with_memory_reservation_bytes(value);
    }
    if let Some(value) = src.cpu_time_millis {
        facts = facts.with_cpu_time(Duration::from_millis(value));
    }
    facts
}

fn decode_writer(
    src: &novarocks::TaskWriterFacts,
    path: FieldPath,
) -> Result<TaskWriterFacts, ProtocolError> {
    let mut facts = TaskWriterFacts::empty();
    match (src.written_rows, src.written_bytes) {
        (Some(rows), Some(bytes)) => facts = facts.with_written(rows, bytes),
        (None, None) => {}
        // Dropping the half that was reported would lose a metric silently,
        // which is worse than refusing the snapshot.
        _ => {
            return Err(inconsistent(
                path,
                "written rows and written bytes must be reported together",
            ));
        }
    }
    if let Some(value) = src.prepared_write_entries {
        facts = facts.with_prepared_write_entries(value);
    }
    Ok(facts)
}

pub fn encode_task_status(value: &TaskStatus) -> novarocks::TaskStatus {
    let filters = value.dynamic_filters();
    let resources = value.resources();
    novarocks::TaskStatus {
        identity: Some(encode_task_identity(value.identity())),
        status_version: value.version().get(),
        state: encode_state(value.state()),
        termination: value.termination().map(encode_termination),
        dynamic_filter_version: filters.map(|filters| filters.version().get()),
        dynamic_filter_domain_count: filters.map(DynamicFilterAdvertisement::domain_count),
        output: Some(novarocks::TaskOutputFacts {
            responsibility_complete: value.output().responsibility_complete(),
            buffered_rows: value.output().buffered_rows(),
            buffered_bytes: value.output().buffered_bytes(),
        }),
        resources: Some(novarocks::TaskResourceFacts {
            queued_splits: resources.queued_splits(),
            running_drivers: resources.running_drivers(),
            memory_reservation_bytes: resources.memory_reservation_bytes(),
            cpu_time_millis: resources.cpu_time().map(|value| value.as_millis() as u64),
        }),
        writer: value.writer().map(|writer| novarocks::TaskWriterFacts {
            written_rows: writer.written_rows(),
            written_bytes: writer.written_bytes(),
            prepared_write_entries: writer.prepared_write_entries(),
        }),
        installed: Some(value.installed()),
    }
}

/// Decodes one observation cursor. Version zero means never observed.
pub fn decode_task_status_cursor(
    src: &novarocks::TaskStatusCursor,
    path: FieldPath,
) -> Result<TaskStatusCursor, ProtocolError> {
    let identity = src.identity.as_ref().ok_or_else(|| {
        missing(
            path.clone().field("identity"),
            "status cursor requires an identity",
        )
    })?;
    let identity = decode_task_identity(identity, path.field("identity"))?;
    Ok(match TaskStatusVersion::new(src.current_version) {
        Ok(version) => TaskStatusCursor::at(identity, version),
        Err(_) => TaskStatusCursor::unobserved(identity),
    })
}

pub fn encode_task_status_cursor(value: TaskStatusCursor) -> novarocks::TaskStatusCursor {
    novarocks::TaskStatusCursor {
        identity: Some(encode_task_identity(value.identity())),
        current_version: value.current_version().map_or(0, TaskStatusVersion::get),
    }
}

/// Decodes the final observation of one terminal task.
/// Decodes the final observation of one terminal task.
///
/// `expected` is the task the caller asked about. The wire message carries only
/// the status, so without it a caller could be handed another task's final info
/// and never notice.
pub fn decode_final_task_info(
    expected: TaskIdentity,
    src: &novarocks::FinalTaskInfo,
    path: FieldPath,
) -> Result<FinalTaskInfo, ProtocolError> {
    let status = src.final_status.as_ref().ok_or_else(|| {
        missing(
            path.clone().field("final_status"),
            "final task info requires a terminal status",
        )
    })?;
    let status = decode_task_status(status, path.clone().field("final_status"))?;
    if src.operator_statistics.len() > FINAL_TASK_INFO_MAX_OPERATORS {
        return Err(out_of_range(
            path.clone().field("operator_statistics"),
            "operator statistics count exceeds the hard limit",
        ));
    }
    let mut statistics = Vec::with_capacity(src.operator_statistics.len());
    for (index, entry) in src.operator_statistics.iter().enumerate() {
        let entry_path = path.clone().field("operator_statistics").index(index);
        let operator = decode_safe_detail(&entry.operator, entry_path.field("operator"))?;
        let mut stats = OperatorStatistics::new(entry.plan_node_id, operator);
        // The two row counts are decoded independently because the wire, the
        // domain type and the producing profile all model them independently.
        // Requiring both would drop a counter the producer did observe and
        // report it as never observed.
        if let Some(input) = entry.input_rows {
            stats = stats.with_input_rows(input);
        }
        if let Some(output) = entry.output_rows {
            stats = stats.with_output_rows(output);
        }
        if let Some(millis) = entry.wall_time_millis {
            stats = stats.with_wall_time(Duration::from_millis(millis));
        }
        if entry.counters.len() > OPERATOR_COUNTER_BUDGET {
            return Err(out_of_range(
                entry_path.field("counters"),
                "operator counter count exceeds the hard limit",
            ));
        }
        if !entry.counters.is_empty() {
            let mut counters = Vec::with_capacity(entry.counters.len());
            for (counter_index, counter) in entry.counters.iter().enumerate() {
                let counter_path = entry_path.clone().field("counters").index(counter_index);
                let name = decode_safe_detail(&counter.name, counter_path.field("name"))?;
                counters.push(OperatorCounter::new(name, counter.value));
            }
            stats = stats.with_counters(counters);
        }
        // Preserved rather than recomputed: the producer is the only owner
        // that knows whether its subtree had more counters than fitted, and a
        // set that arrives exactly at the budget is indistinguishable from one
        // that was cut to it.
        if entry.counters_truncated {
            stats = stats.with_counters_truncated();
        }
        statistics.push(stats);
    }
    FinalTaskInfo::try_new(
        expected,
        status,
        statistics,
        src.operator_statistics_truncated,
    )
    .map_err(|error| inconsistent(path, error.to_string()))
}

pub fn encode_final_task_info(value: &FinalTaskInfo) -> novarocks::FinalTaskInfo {
    novarocks::FinalTaskInfo {
        final_status: Some(encode_task_status(value.final_status())),
        operator_statistics: value
            .operator_statistics()
            .iter()
            .map(|entry| novarocks::TaskOperatorStatistics {
                plan_node_id: entry.plan_node_id(),
                operator: entry.operator().as_str().to_owned(),
                input_rows: entry.input_rows(),
                output_rows: entry.output_rows(),
                wall_time_millis: entry.wall_time().map(|value| value.as_millis() as u64),
                counters: entry
                    .counters()
                    .iter()
                    .map(|counter| novarocks::TaskOperatorCounter {
                        name: counter.name().as_str().to_owned(),
                        value: counter.value(),
                    })
                    .collect(),
                counters_truncated: entry.counters_truncated(),
            })
            .collect(),
        operator_statistics_truncated: value.operator_statistics_truncated(),
    }
}
