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

use std::time::Duration;

use novarocks_execution::task_execution::domain::DomainVersion;
use novarocks_execution::task_execution::identity::TaskIdentity;
use novarocks_execution::task_execution::status::{
    AbortCause, CancelReason, DynamicFilterAdvertisement, FINAL_TASK_INFO_MAX_OPERATORS,
    FinalTaskInfo, OperatorStatistics, SAFE_DETAIL_MAX_BYTES, SafeDetail, TaskFailure,
    TaskFailureCategory, TaskOutputFacts, TaskResourceFacts, TaskState, TaskStatus,
    TaskStatusCursor, TaskStatusVersion, TaskWriterFacts, TerminationDetail,
};
use novarocks_proto_models::novarocks;

use crate::task_execution::identity::{decode_task_identity, encode_task_identity};
use crate::task_execution::{inconsistent, invalid, invalid_enum, missing, out_of_range};
use crate::{FieldPath, ProtocolError};

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
    match novarocks::QueryContextAbortCause::try_from(value) {
        Ok(novarocks::QueryContextAbortCause::QueryFailed) => Ok(AbortCause::QueryFailed),
        Ok(novarocks::QueryContextAbortCause::LeaseExpired) => Ok(AbortCause::LeaseExpired),
        Ok(novarocks::QueryContextAbortCause::PeerTaskFailed) => Ok(AbortCause::PeerTaskFailed),
        Ok(novarocks::QueryContextAbortCause::Unspecified) | Err(_) => Err(invalid_enum(
            path,
            "abort cause must be a known non-default value",
        )),
    }
}

pub(crate) fn encode_abort_cause(value: AbortCause) -> i32 {
    let encoded = match value {
        AbortCause::QueryFailed => novarocks::QueryContextAbortCause::QueryFailed,
        AbortCause::LeaseExpired => novarocks::QueryContextAbortCause::LeaseExpired,
        AbortCause::PeerTaskFailed => novarocks::QueryContextAbortCause::PeerTaskFailed,
    };
    encoded as i32
}

fn decode_failure_category(
    value: i32,
    path: FieldPath,
) -> Result<TaskFailureCategory, ProtocolError> {
    match novarocks::TaskFailureCategory::try_from(value) {
        Ok(novarocks::TaskFailureCategory::Execution) => Ok(TaskFailureCategory::Execution),
        Ok(novarocks::TaskFailureCategory::ResourceExhausted) => {
            Ok(TaskFailureCategory::ResourceExhausted)
        }
        Ok(novarocks::TaskFailureCategory::Exchange) => Ok(TaskFailureCategory::Exchange),
        Ok(novarocks::TaskFailureCategory::Protocol) => Ok(TaskFailureCategory::Protocol),
        Ok(novarocks::TaskFailureCategory::Internal) => Ok(TaskFailureCategory::Internal),
        Ok(novarocks::TaskFailureCategory::Unspecified) | Err(_) => Err(invalid_enum(
            path,
            "failure category must be a known non-default value",
        )),
    }
}

fn encode_failure_category(value: TaskFailureCategory) -> i32 {
    let encoded = match value {
        TaskFailureCategory::Execution => novarocks::TaskFailureCategory::Execution,
        TaskFailureCategory::ResourceExhausted => novarocks::TaskFailureCategory::ResourceExhausted,
        TaskFailureCategory::Exchange => novarocks::TaskFailureCategory::Exchange,
        TaskFailureCategory::Protocol => novarocks::TaskFailureCategory::Protocol,
        TaskFailureCategory::Internal => novarocks::TaskFailureCategory::Internal,
    };
    encoded as i32
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
            let category =
                decode_failure_category(failure.category, failure_path.clone().field("category"))?;
            let detail =
                decode_safe_detail(&failure.safe_detail, failure_path.field("safe_detail"))?;
            TerminationDetail::Failed(TaskFailure::new(category, detail))
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
                category: encode_failure_category(failure.category()),
                safe_detail: failure.detail().as_str().to_owned(),
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

    let status = TaskStatus::try_new(identity, version, state, termination, facts)
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
            })
            .collect(),
        operator_statistics_truncated: value.operator_statistics_truncated(),
    }
}
