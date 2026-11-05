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

//! Shared names and activation requirements of test-consumed log evidence.
//!
//! Constants keep producers and Rust consumers on one vocabulary. Metadata
//! selects observation requests; it never grants a role session or permission.
//! Domain owners still decide when a real event may be emitted. Markers without
//! a Native observation action retain their existing fixture or control gates.

/// A Native observation requested by the SQL runner and bound by the role owner.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash)]
pub enum ObservationAction {
    TaskProtocol,
    ReaderMarker,
    WriterMarker,
    CatalogMaterialization,
    CatalogLifecycle,
    ExchangeSnapshot,
}

impl ObservationAction {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::TaskProtocol => "task-protocol",
            Self::ReaderMarker => "reader-marker",
            Self::WriterMarker => "writer-marker",
            Self::CatalogMaterialization => "catalog-materialization",
            Self::CatalogLifecycle => "catalog-lifecycle",
            Self::ExchangeSnapshot => "exchange-snapshot",
        }
    }
}

/// The domain that owns the event, independently of its activation requirement.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum MarkerFamily {
    Participation,
    Task,
    Reader,
    Writer,
    Catalog,
    Exchange,
    Frontend,
    Process,
    MaterializedView,
    Fixture,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum MarkerActivation {
    Observation(ObservationAction),
    FaultInjection,
    /// No Native observation request; the original producer's gates still apply.
    Unconditional,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ObservationMarker {
    pub name: &'static str,
    pub family: MarkerFamily,
    pub activation: MarkerActivation,
    pub keys: &'static [&'static str],
}

macro_rules! marker_registry {
    ($($constant:ident => ($name:literal, $family:ident, $activation:expr, [$($key:literal),*])),* $(,)?) => {
        $(pub const $constant: &str = $name;)*
        static MARKERS: &[ObservationMarker] = &[
            $(ObservationMarker {
                name: $constant,
                family: MarkerFamily::$family,
                activation: $activation,
                keys: &[$($key),*],
            },)*
        ];
    };
}

use MarkerActivation::{FaultInjection, Observation, Unconditional};
use ObservationAction::{
    CatalogLifecycle, CatalogMaterialization, ExchangeSnapshot, ReaderMarker, TaskProtocol,
    WriterMarker,
};

marker_registry! {
    MEM_1_M07_HELD_LIVE_FENCE => ("NOVAROCKS_MEM_1_M07_HELD_LIVE_FENCE", Fixture, Unconditional, ["phase", "sequence", "sizes"]),
    MEM_1_M07_HELD_LIVE_LOG_OWNER => ("NOVAROCKS_MEM_1_M07_HELD_LIVE_LOG_OWNER", Fixture, Unconditional, ["role", "dev", "ino"]),
    CATALOG_INSTALL_FAILED => ("NOVAROCKS_CATALOG_INSTALL_FAILED", Catalog, Observation(CatalogLifecycle), ["execution_id"]),
    CATALOG_INSTALL_STARTED => ("NOVAROCKS_CATALOG_INSTALL_STARTED", Catalog, Observation(CatalogLifecycle), ["catalog_count", "execution_id"]),
    CATALOG_RUNTIME_MATERIALIZED => ("NOVAROCKS_CATALOG_RUNTIME_MATERIALIZED", Catalog, Observation(CatalogMaterialization), ["catalog"]),
    CONNECTOR_PAGE_SOURCE_CLOSE => ("NOVAROCKS_CONNECTOR_PAGE_SOURCE_CLOSE", Reader, Observation(ReaderMarker), ["plan_node", "sequence"]),
    CONNECTOR_PAGE_SOURCE_OPEN => ("NOVAROCKS_CONNECTOR_PAGE_SOURCE_OPEN", Reader, Observation(ReaderMarker), ["plan_node", "sequence"]),
    CONNECTOR_UNIT_READER_CLOSE => ("NOVAROCKS_CONNECTOR_UNIT_READER_CLOSE", Reader, Observation(ReaderMarker), ["batches_delivered", "catalog_version", "instance", "provider", "rows_decoded", "scheduled_split_sequence_id"]),
    CONNECTOR_UNIT_READER_OPEN => ("NOVAROCKS_CONNECTOR_UNIT_READER_OPEN", Reader, Observation(ReaderMarker), ["catalog_version", "instance", "provider", "scheduled_split_sequence_id"]),
    CONNECTOR_WRITER_OPENED => ("NOVAROCKS_CONNECTOR_WRITER_OPENED", Writer, Observation(WriterMarker), ["driver_id", "execution_id", "plan_node", "task_key", "write_target", "writer_ordinal"]),
    EXACT_MYSQL_TARGET_BOUND => ("NOVAROCKS_EXACT_MYSQL_TARGET_BOUND", Frontend, Unconditional, ["connection_generation", "connection_id", "session_connection_id", "session_key", "sql_sha256", "statement_generation"]),
    EXCHANGE_SNAPSHOT => ("NOVAROCKS_EXCHANGE_SNAPSHOT", Exchange, Observation(ExchangeSnapshot), ["attempt", "block_epoch", "blocked_generation", "close", "current_generation", "custody_through", "destination_task_key", "destination_task", "driver_id", "event", "expected_senders", "frozen_generation", "granted_through", "node_id", "reason", "sender_count", "sender_task_key", "sender_ordinal", "sequence", "snapshot", "source_generation", "source_name", "task_key"]),
    GRPC_FETCH_TYPED => ("NOVAROCKS_GRPC_FETCH_TYPED", Task, Observation(TaskProtocol), ["attempt", "backend", "eos", "packet_seq", "payload_bytes", "protocol", "query_hi", "query_lo", "stage", "status", "task"]),
    ICEBERG_WRITE_PHASE => ("NOVAROCKS_ICEBERG_WRITE_PHASE", Writer, Observation(WriterMarker), ["attempt", "elapsed_ms", "phase", "session_id"]),
    LOOPBACK_S3_REJECTED_REQUEST => ("NOVAROCKS_LOOPBACK_S3_REJECTED_REQUEST", Fixture, Unconditional, ["reason"]),
    MEM_1_M07_BUILD => ("NOVAROCKS_MEM_1_M07_BUILD", Fixture, Unconditional, ["build_identity", "commit", "exact_mysql_write"]),
    MEM_1_M07_CLOSING_PRESSURE_BUILD => ("NOVAROCKS_MEM_1_M07_CLOSING_PRESSURE_BUILD", Fixture, Unconditional, ["build_identity", "closing_pressure", "commit"]),
    MEM_1_M07_CLOSING_PRESSURE_FE => ("NOVAROCKS_MEM_1_M07_CLOSING_PRESSURE_FE", Fixture, Unconditional, ["frontend_process_id"]),
    MEM_1_M07_EXACT_MYSQL_FE => ("NOVAROCKS_MEM_1_M07_EXACT_MYSQL_FE", Fixture, Unconditional, ["frontend_process_id"]),
    MEM_1_M07_HMS_LISTING_BUILD => ("NOVAROCKS_MEM_1_M07_HMS_LISTING_BUILD", Fixture, Unconditional, ["build_identity", "commit", "hms_listing_observe"]),
    MEM_1_M07_ROOT_OBSERVATION_BUILD => ("NOVAROCKS_MEM_1_M07_ROOT_OBSERVATION_BUILD", Fixture, Unconditional, ["build_identity", "commit", "root_observation"]),
    MEM_1_M07_ROOT_OBSERVATION_FE => ("NOVAROCKS_MEM_1_M07_ROOT_OBSERVATION_FE", Fixture, Unconditional, ["frontend_process_id"]),
    MV_DOCUMENT_EXPIRY_PLANNED => ("NOVAROCKS_MV_DOCUMENT_EXPIRY_PLANNED", MaterializedView, FaultInjection, ["token"]),
    MV_PROJECTOR_PHASE => ("NOVAROCKS_MV_PROJECTOR_PHASE", MaterializedView, FaultInjection, ["action", "phase", "publication_id"]),
    MV_RECOVERY_PHASE => ("NOVAROCKS_MV_RECOVERY_PHASE", MaterializedView, FaultInjection, ["phase", "token"]),
    PREPARED_FAILURE_EXIT_RELEASED => ("NOVAROCKS_PREPARED_FAILURE_EXIT_RELEASED", Task, FaultInjection, ["backend", "execution_id", "reason", "stage", "task", "token"]),
    QUERY_PROCESS_NAMESPACE => ("NOVAROCKS_QUERY_PROCESS_NAMESPACE", Frontend, Unconditional, ["query_process_namespace"]),
    READY => ("NOVAROCKS_READY", Process, Unconditional, ["advertise_host", "control_grpc_port", "grpc_port", "mysql_port", "pid", "role", "trino_port"]),
    RESULT_HANDOFF_TERMINAL_OBSERVED => ("NOVAROCKS_RESULT_HANDOFF_TERMINAL_OBSERVED", Frontend, Unconditional, ["statement", "token"]),
    RUNTIME_FILTER_ROW_GROUP_PRUNED => ("NOVAROCKS_RUNTIME_FILTER_ROW_GROUP_PRUNED", Reader, Observation(ReaderMarker), ["checkpoint", "row_group_ordinal", "scheduled_split_sequence_id"]),
    PARTICIPATION_ABORT_APPLIED => ("NOVAROCKS_PARTICIPATION_ABORT_APPLIED", Participation, Observation(TaskProtocol), ["backend", "execution_id", "frontend"]),
    PARTICIPATION_ESTABLISH_APPLIED => ("NOVAROCKS_PARTICIPATION_ESTABLISH_APPLIED", Participation, Observation(TaskProtocol), ["backend", "execution_id", "frontend"]),
    PARTICIPATION_ESTABLISH_CONFLICT => ("NOVAROCKS_PARTICIPATION_ESTABLISH_CONFLICT", Participation, Observation(TaskProtocol), ["backend", "execution_id", "frontend"]),
    PARTICIPATION_ESTABLISH_IDEMPOTENT => ("NOVAROCKS_PARTICIPATION_ESTABLISH_IDEMPOTENT", Participation, Observation(TaskProtocol), ["backend", "execution_id", "frontend"]),
    PARTICIPATION_LEASE_EXPIRED => ("NOVAROCKS_PARTICIPATION_LEASE_EXPIRED", Participation, Observation(TaskProtocol), ["backend", "execution_id", "frontend"]),
    PARTICIPATION_TERMINATION_COMPLETED => ("NOVAROCKS_PARTICIPATION_TERMINATION_COMPLETED", Participation, Observation(TaskProtocol), ["backend", "cause", "execution_id", "frontend", "retained_tasks"]),
    TASK_CREATE_ACK_DROPPED => ("NOVAROCKS_TASK_CREATE_ACK_DROPPED", Task, FaultInjection, ["backend_index", "execution_id", "stage", "task", "token"]),
    TASK_CREATE_APPLIED => ("NOVAROCKS_TASK_CREATE_APPLIED", Task, Observation(TaskProtocol), ["backend", "execution_id", "stage", "task"]),
    TASK_CREATE_BEFORE_WORKER_HELD => ("NOVAROCKS_TASK_CREATE_BEFORE_WORKER_HELD", Task, FaultInjection, ["backend_index", "execution_id", "outbound_edges", "process_id", "stage", "task", "token"]),
    TASK_CREATE_IDEMPOTENT => ("NOVAROCKS_TASK_CREATE_IDEMPOTENT", Task, Observation(TaskProtocol), ["backend", "execution_id", "stage", "task"]),
    TASK_CREATE_RECEIPT_FOREIGN_TASK => ("NOVAROCKS_TASK_CREATE_RECEIPT_FOREIGN_TASK", Task, FaultInjection, ["backend_index", "execution_id", "foreign_task", "stage", "task", "token"]),
    TASK_CREATE_REJECTED_AFTER_APPLY => ("NOVAROCKS_TASK_CREATE_REJECTED_AFTER_APPLY", Task, FaultInjection, ["backend_index", "execution_id", "stage", "task", "token"]),
    TASK_DYNAMIC_FILTER_ADVERTISED => ("NOVAROCKS_TASK_DYNAMIC_FILTER_ADVERTISED", Reader, Observation(ReaderMarker), ["channel", "execution_id", "stage", "task", "version"]),
    PARTICIPATION_ESTABLISH_ACK_DROPPED => ("NOVAROCKS_PARTICIPATION_ESTABLISH_ACK_DROPPED", Participation, FaultInjection, ["backend_index", "execution_id", "token"]),
    PARTICIPATION_ESTABLISH_OBSERVED => ("NOVAROCKS_PARTICIPATION_ESTABLISH_OBSERVED", Participation, FaultInjection, ["backend_index", "execution_id", "process_id", "token"]),
    TASK_EXECUTION_FAILURE_AFTER_VISIBLE_PREFIX_ACCEPTED => ("NOVAROCKS_TASK_EXECUTION_FAILURE_AFTER_VISIBLE_PREFIX_ACCEPTED", Task, FaultInjection, ["accepted_consumed", "backend", "execution_id", "stage", "task", "token"]),
    TASK_EXECUTION_FAILURE_INJECTED => ("NOVAROCKS_TASK_EXECUTION_FAILURE_INJECTED", Task, FaultInjection, ["backend_index", "execution_id", "stage", "task", "token"]),
    PARTICIPATION_LEASE_RENEWAL_ACK_DROPPED => ("NOVAROCKS_PARTICIPATION_LEASE_RENEWAL_ACK_DROPPED", Participation, FaultInjection, ["backend_index", "execution_id", "token"]),
    PARTICIPATION_LEASE_RENEWAL_STOPPED => ("NOVAROCKS_PARTICIPATION_LEASE_RENEWAL_STOPPED", Participation, FaultInjection, ["backend_index", "execution_id", "token"]),
    PARTICIPATION_LEASE_RENEWED => ("NOVAROCKS_PARTICIPATION_LEASE_RENEWED", Participation, Observation(TaskProtocol), ["backend", "execution_id", "frontend", "sequence"]),
    TASK_PREPARED_CLIENT_ROOT => ("NOVAROCKS_TASK_PREPARED_CLIENT_ROOT", Task, Observation(TaskProtocol), ["backend", "execution_id", "stage", "task"]),
    TASK_PREPARED_DOP => ("NOVAROCKS_TASK_PREPARED_DOP", Task, Observation(TaskProtocol), ["backend", "dop", "execution_id", "stage", "task", "task_key"]),
    PARTICIPATION_RELEASE_APPLIED => ("NOVAROCKS_PARTICIPATION_RELEASE_APPLIED", Participation, Observation(TaskProtocol), ["backend", "execution_id", "frontend", "runtime_filter"]),
    TASK_RUNTIME_FILTER_FEEDBACK_CONTRACT_DIGEST_CORRUPT => ("NOVAROCKS_TASK_RUNTIME_FILTER_FEEDBACK_CONTRACT_DIGEST_CORRUPT", Task, FaultInjection, ["backend_index", "execution_id", "stage", "task", "token"]),
    TASK_RUNTIME_FILTER_FEEDBACK_FOREIGN_ATTEMPT => ("NOVAROCKS_TASK_RUNTIME_FILTER_FEEDBACK_FOREIGN_ATTEMPT", Task, FaultInjection, ["backend_index", "execution_id", "stage", "task", "token"]),
    TASK_RUNTIME_FILTER_FEEDBACK_UNAVAILABLE => ("NOVAROCKS_TASK_RUNTIME_FILTER_FEEDBACK_UNAVAILABLE", Task, FaultInjection, ["backend_index", "execution_id", "stage", "task", "token"]),
    TASK_SPLIT_ASSIGNMENT_ACCEPTED => ("NOVAROCKS_TASK_SPLIT_ASSIGNMENT_ACCEPTED", Reader, Observation(ReaderMarker), ["accepted_through", "duplicate", "enqueued", "execution_id", "plan_node", "task_key"]),
    TASK_SPLIT_ASSIGNMENT_DUPLICATE => ("NOVAROCKS_TASK_SPLIT_ASSIGNMENT_DUPLICATE", Reader, Observation(ReaderMarker), ["duplicate", "duplicate_splits_skipped", "execution_id", "plan_node", "task_key"]),
    TASK_SPLIT_NO_MORE => ("NOVAROCKS_TASK_SPLIT_NO_MORE", Reader, Observation(ReaderMarker), ["execution_id", "plan_node", "task_key"]),
    TASK_STATUS_FOREIGN_PROCESS => ("NOVAROCKS_TASK_STATUS_FOREIGN_PROCESS", Task, FaultInjection, ["backend", "backend_index", "execution_id", "foreign_backend", "stage", "task", "token"]),
    TASK_STATUS_SUBSCRIPTION_DROPPED => ("NOVAROCKS_TASK_STATUS_SUBSCRIPTION_DROPPED", Task, FaultInjection, ["backend_index", "execution_id", "token"]),
    TASK_TERMINAL_RETAINED => ("NOVAROCKS_TASK_TERMINAL_RETAINED", Task, Observation(TaskProtocol), ["backend", "execution_id", "stage", "state", "task"]),
    TASK_UPDATE_TERMINAL_ACK_DROPPED => ("NOVAROCKS_TASK_UPDATE_TERMINAL_ACK_DROPPED", Task, FaultInjection, ["backend_index", "execution_id", "token"]),
    WRITER_DIAGNOSTIC => ("NOVAROCKS_WRITER_DIAGNOSTIC", Writer, Observation(ExchangeSnapshot), ["aborting", "block_epoch", "contributors", "core_next", "counts", "current_generation", "cursor", "dest_node", "driver_id", "emitted", "end_sent", "event", "exiting", "expected", "failed", "finish_counted", "finished", "finishing", "frozen_generation", "generation", "generations", "last_driver", "output_claimed", "pending_chunks", "pending_payloads", "reason", "receiver_occupied", "reduction", "remaining", "remaining_drivers", "sender_state", "service", "sink", "source", "summary_end", "summary_wait", "task", "task_key", "writer_end", "writer_pending"]),
}

pub fn markers() -> &'static [ObservationMarker] {
    MARKERS
}

pub fn lookup(name: &str) -> Option<&'static ObservationMarker> {
    markers().iter().find(|marker| marker.name == name)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashSet;

    #[test]
    fn registered_markers_have_unique_names_and_fields() {
        let mut names = HashSet::new();
        for marker in markers() {
            assert!(marker.name.starts_with("NOVAROCKS_"));
            assert!(
                names.insert(marker.name),
                "duplicate marker {}",
                marker.name
            );
            assert_eq!(lookup(marker.name), Some(marker));
            let mut fields = HashSet::new();
            for key in marker.keys {
                assert!(!key.is_empty());
                assert!(
                    fields.insert(key),
                    "duplicate field {} in {}",
                    key,
                    marker.name
                );
            }
        }
        assert!(lookup("NOVAROCKS_UNKNOWN_MARKER").is_none());
    }

    #[test]
    fn release_and_terminal_events_require_task_observation() {
        for name in [PARTICIPATION_RELEASE_APPLIED, TASK_TERMINAL_RETAINED] {
            assert_eq!(lookup(name).unwrap().activation, Observation(TaskProtocol));
        }
    }
}
