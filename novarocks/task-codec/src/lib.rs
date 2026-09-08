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

//! Central codec for the native task protocol.
//!
//! This module is the only place that converts between the transport-neutral
//! task domain and the generated wire messages. It validates structure,
//! identity, enum closure, and every payload bound, and it does no scheduler,
//! connector, or plan business of its own.
//!
//! One conversion is deliberately asymmetric. The physical fragment plan has
//! no transport-neutral representation to convert into, so
//! [`descriptor::WireFragmentPlan`] keeps the generated message as its private
//! stored representation and satisfies the neutral
//! `PhysicalFragmentPlan` capability with typed accessors. That is the single
//! recorded exception; every other value crosses this boundary as a neutral
//! Rust type.

pub mod descriptor;
pub mod domain;
pub mod identity;
pub mod lease;
pub mod operation;
pub mod status;

use novarocks_proto_codec::{FieldPath, ProtocolError, ProtocolErrorKind};

pub(crate) fn missing(path: FieldPath, detail: impl Into<String>) -> ProtocolError {
    ProtocolError::new(path, ProtocolErrorKind::MissingField, detail)
}

pub(crate) fn invalid(path: FieldPath, detail: impl Into<String>) -> ProtocolError {
    ProtocolError::new(path, ProtocolErrorKind::InvalidValue, detail)
}

pub(crate) fn invalid_enum(path: FieldPath, detail: impl Into<String>) -> ProtocolError {
    ProtocolError::new(path, ProtocolErrorKind::InvalidEnum, detail)
}

pub(crate) fn out_of_range(path: FieldPath, detail: impl Into<String>) -> ProtocolError {
    ProtocolError::new(path, ProtocolErrorKind::OutOfRange, detail)
}

pub(crate) fn inconsistent(path: FieldPath, detail: impl Into<String>) -> ProtocolError {
    ProtocolError::new(path, ProtocolErrorKind::InconsistentFields, detail)
}

pub(crate) fn duplicate(path: FieldPath, detail: impl Into<String>) -> ProtocolError {
    ProtocolError::new(path, ProtocolErrorKind::DuplicateField, detail)
}

#[cfg(test)]
mod tests {
    use super::descriptor::{decode_task_descriptor, decode_topology, encode_topology};
    use super::identity::{
        decode_query_context_ref, decode_task_identity, encode_query_context_ref,
        encode_task_identity,
    };
    use super::lease::{
        LeaseGrant, decode_lease_grant, decode_lease_receipt, encode_lease_grant,
        encode_lease_receipt,
    };
    use super::operation::{
        DecodedOperation, DecodedUpdateQueryContext, decode_fetch_task_result,
        decode_get_final_task_info, decode_operation_batch, decode_receipt_batch,
        encode_fetch_task_result, encode_get_final_task_info, encode_operation_outcome,
        encode_query_context_state,
    };
    use super::status::{decode_task_status, encode_task_status};
    use novarocks_execution::task_execution::domain::{
        CredentialEpoch, DomainVersion, EdgeOpenVersion, ExchangeEdgeId, PlanNodeId,
    };
    use novarocks_execution::task_execution::identity::{
        QueryContextRef, TaskIdentity, TaskOperationId,
    };
    use novarocks_execution::task_execution::lease::{LeaseReceipt, LeaseSequence, LeaseValidFor};
    use novarocks_execution::task_execution::operation::{
        MaxWait, OperationKind, OperationOutcome, ReleaseOutcome, TransportBudget,
    };
    use novarocks_execution::task_execution::status::{
        AbortCause, CancelReason, SafeDetail, TaskFailure, TaskFailureCategory, TaskOutputFacts,
        TaskState, TaskStatus, TaskStatusVersion, TerminationDetail,
    };
    use novarocks_execution::task_execution::transition::QueryContextState;
    use novarocks_proto_codec::{FieldPath, ProtocolErrorKind};
    use novarocks_proto_models::{catalog, common, filter, novarocks, plan};
    use novarocks_types::NativeCompatibilityId;
    use novarocks_types::identity::{
        AttemptId, BackendProcessId, FrontendProcessId, QueryExecutionId, QueryId, StageId, TaskId,
    };
    use std::time::Duration;

    const SECRET_SENTINEL: &str = "NOVAROCKS_SECRET_SENTINEL";

    fn execution() -> QueryExecutionId {
        QueryExecutionId::new(QueryId::new(11, 12), AttemptId::new(3).expect("nonzero"))
            .expect("nonzero query")
    }

    fn backend() -> BackendProcessId {
        BackendProcessId::new_v7()
    }

    fn identity(stage: u32, task: u32, process: BackendProcessId) -> TaskIdentity {
        TaskIdentity::new(
            execution(),
            StageId::new(stage).expect("nonzero stage"),
            TaskId::new(task).expect("nonzero task"),
            process,
        )
    }

    fn context(process: BackendProcessId) -> QueryContextRef {
        QueryContextRef::new(execution(), FrontendProcessId::new_v7(), process)
    }

    /// A descriptor and an envelope that actually pair.
    ///
    /// These go through the same `validate_initial_credential_lease_envelopes`
    /// the old lifecycle stack uses, which requires each envelope to match its
    /// descriptor exactly and every secret scalar to be non-empty. A
    /// `default()` descriptor beside an `s3: None` envelope satisfies neither,
    /// so building the pair properly is what makes these tests exercise the
    /// real rule.
    fn credential_descriptor(epoch: u64) -> novarocks::CredentialLeaseDescriptor {
        use novarocks_spi::connector::{
            CatalogHandle, CatalogVersion, ConnectorInstanceId, CredentialLeaseDescriptor,
            CredentialLeaseId, CredentialLeaseProvider, StorageAccessDomainId,
            StorageCredentialScopePrefix,
        };

        novarocks_proto_codec::lifecycle::encode_credential_lease_descriptor(
            &CredentialLeaseDescriptor::try_new(
                CredentialLeaseId::try_from_bytes([1; 16]).expect("lease"),
                epoch,
                CatalogHandle::new(
                    ConnectorInstanceId::parse("warehouse").expect("instance"),
                    CatalogVersion::from_bytes([7; 32]),
                ),
                CredentialLeaseProvider::S3,
                vec![
                    StorageCredentialScopePrefix::try_from_normalized("s3://bucket/data")
                        .expect("prefix"),
                ],
                99,
                true,
                StorageAccessDomainId::from_bytes([8; 32]),
            )
            .expect("descriptor"),
        )
    }

    fn credential_envelope(epoch: u64, secret: &str) -> novarocks::CredentialLeaseSecretEnvelope {
        use novarocks_spi::connector::CredentialLeaseId;

        novarocks_proto_codec::lifecycle::encode_credential_lease_secret_envelope(
            &novarocks_proto_codec::lifecycle::CredentialLeaseSecretEnvelope::try_new_from_wire_scalars(
                CredentialLeaseId::try_from_bytes([1; 16]).expect("lease"),
                epoch,
                "access-key-id".to_owned(),
                secret.to_owned(),
                "session-token".to_owned(),
                99,
            )
            .expect("envelope"),
        )
    }

    fn unique(hi: i64, lo: i64) -> common::UniqueId {
        common::UniqueId { hi, lo }
    }

    fn query_options(dop: i32) -> novarocks::QueryOptions {
        novarocks::QueryOptions {
            pipeline_dop: dop,
            ..Default::default()
        }
    }

    fn fragment_plan(
        finst: common::UniqueId,
        dop: i32,
        destinations: Vec<novarocks::Destination>,
        per_exch_num_senders: std::collections::HashMap<i32, i32>,
    ) -> novarocks::TaskFragmentPlan {
        novarocks::TaskFragmentPlan {
            plan: Some(plan::PlanFragment {
                fragment_id: 4,
                sink: Some(plan::DataSink {
                    kind: Some(plan::data_sink::Kind::Result(true)),
                }),
                ..Default::default()
            }),
            instance_params: Some(novarocks::InstanceParams {
                query_id: Some(unique(11, 12)),
                fragment_instance_id: Some(finst),
                backend_num: 0,
                per_node_scan_ranges: Default::default(),
                per_exch_num_senders,
                destinations,
                query_options: Some(query_options(dop)),
                typed_result_sink: true,
            }),
        }
    }

    /// A descriptor with no exchange topology at all, which is the shape of a
    /// single-fragment root task.
    fn simple_descriptor(process: BackendProcessId) -> novarocks::TaskDescriptor {
        novarocks::TaskDescriptor {
            identity: Some(encode_task_identity(identity(2, 3, process))),
            fragment_instance_id: Some(unique(7, 8)),
            pipeline_dop: 4,
            split_plan_nodes: vec![10, 11],
            topology: Some(novarocks::TaskExchangeTopology::default()),
            fragment: Some(fragment_plan(
                unique(7, 8),
                4,
                Vec::new(),
                Default::default(),
            )),
        }
    }

    #[test]
    fn identity_round_trips_and_rejects_every_malformed_component() {
        let process = backend();
        let value = identity(2, 3, process);
        let encoded = encode_task_identity(value);
        assert_eq!(
            decode_task_identity(&encoded, FieldPath::root("identity")),
            Ok(value)
        );

        let missing_execution = novarocks::TaskIdentity {
            query_execution_id: None,
            ..encoded.clone()
        };
        assert_eq!(
            decode_task_identity(&missing_execution, FieldPath::root("identity"))
                .expect_err("execution id is required")
                .kind(),
            ProtocolErrorKind::MissingField
        );

        let zero_stage = novarocks::TaskIdentity {
            stage_id: 0,
            ..encoded.clone()
        };
        let error = decode_task_identity(&zero_stage, FieldPath::root("identity"))
            .expect_err("stage id must be nonzero");
        assert_eq!(error.kind(), ProtocolErrorKind::InvalidValue);
        assert_eq!(error.path().to_string(), "identity.stage_id");

        let zero_task = novarocks::TaskIdentity {
            task_id: 0,
            ..encoded.clone()
        };
        assert_eq!(
            decode_task_identity(&zero_task, FieldPath::root("identity"))
                .expect_err("task id must be nonzero")
                .path()
                .to_string(),
            "identity.task_id"
        );

        let short_process = novarocks::TaskIdentity {
            backend_process_id: Some(novarocks::BackendProcessId {
                value: vec![0u8; 8],
            }),
            ..encoded.clone()
        };
        assert_eq!(
            decode_task_identity(&short_process, FieldPath::root("identity"))
                .expect_err("process id must be 16 bytes")
                .detail(),
            "identity must be exactly 16 bytes"
        );

        let nil_process = novarocks::TaskIdentity {
            backend_process_id: Some(novarocks::BackendProcessId {
                value: vec![0u8; 16],
            }),
            ..encoded
        };
        assert!(
            decode_task_identity(&nil_process, FieldPath::root("identity")).is_err(),
            "a nil process id is not a process"
        );
    }

    #[test]
    fn a_context_reference_round_trips_with_its_frontend_fence() {
        let value = context(backend());
        let encoded = encode_query_context_ref(value);
        assert_eq!(
            decode_query_context_ref(&encoded, FieldPath::root("query_context")),
            Ok(value)
        );
        let missing_frontend = novarocks::QueryContextRef {
            frontend_process_id: None,
            ..encoded
        };
        assert_eq!(
            decode_query_context_ref(&missing_frontend, FieldPath::root("query_context"))
                .expect_err("the frontend fence is required")
                .path()
                .to_string(),
            "query_context.frontend_process_id"
        );
    }

    #[test]
    fn lease_grants_and_receipts_round_trip_and_reject_zero_durations() {
        let grant = LeaseGrant::new(
            LeaseSequence::INITIAL,
            LeaseValidFor::new(Duration::from_secs(30)).expect("representable"),
        );
        let encoded = encode_lease_grant(grant);
        assert_eq!(encoded.valid_for_millis, 30_000);
        assert_eq!(
            decode_lease_grant(&encoded, FieldPath::root("lease")),
            Ok(grant)
        );

        let zero = novarocks::QueryExecutionLeaseGrant {
            sequence: 1,
            valid_for_millis: 0,
        };
        assert_eq!(
            decode_lease_grant(&zero, FieldPath::root("lease"))
                .expect_err("a zero lease is not a lease")
                .detail(),
            "duration must be greater than zero"
        );

        let receipt = LeaseReceipt::new(
            LeaseSequence::new(4),
            LeaseValidFor::new(Duration::from_secs(45)).expect("representable"),
            Duration::from_secs(30),
        );
        let encoded = encode_lease_receipt(receipt);
        assert_eq!(encoded.requested_valid_for_millis, 45_000);
        assert_eq!(encoded.effective_valid_for_millis, 30_000);
        assert_eq!(
            decode_lease_receipt(&encoded, FieldPath::root("lease")),
            Ok(receipt)
        );

        let zero_effective = novarocks::QueryExecutionLeaseReceipt {
            effective_valid_for_millis: 0,
            ..encoded
        };
        assert!(
            decode_lease_receipt(&zero_effective, FieldPath::root("lease")).is_err(),
            "the frontend schedules from the effective duration, so it cannot be zero"
        );
    }

    #[test]
    fn topology_round_trips_both_addresses_and_rejects_a_duplicate_source() {
        let process = backend();
        let wire = novarocks::TaskExchangeTopology {
            outbound: vec![novarocks::TaskExchangeEdge {
                edge_id: 1,
                destination_node_id: 20,
                partitioning: novarocks::ExchangePartitioning::Hash as i32,
                destinations: vec![novarocks::TaskExchangeDestination {
                    task: Some(encode_task_identity(identity(3, 1, process))),
                    fragment_instance_id: Some(unique(3, 1)),
                    endpoint: Some(novarocks::QueryControlEndpoint {
                        host: "127.0.0.1".to_owned(),
                        port: 9060,
                    }),
                    destination_node_id: 20,
                    sender_ordinal: 0,
                    sender_count: 1,
                }],
            }],
            inbound: vec![novarocks::TaskExchangeInbound {
                destination_node_id: 30,
                sources: vec![
                    novarocks::TaskExchangeSource {
                        task: Some(encode_task_identity(identity(1, 1, process))),
                        fragment_instance_id: Some(unique(1, 1)),
                        sender_ordinal: 0,
                    },
                    novarocks::TaskExchangeSource {
                        task: Some(encode_task_identity(identity(1, 2, process))),
                        fragment_instance_id: Some(unique(1, 2)),
                        sender_ordinal: 1,
                    },
                ],
            }],
        };
        let decoded = decode_topology(&wire, FieldPath::root("topology")).expect("legal topology");
        assert_eq!(decoded.outbound().len(), 1);
        assert_eq!(
            decoded.outbound()[0].destinations()[0].fragment_instance_id(),
            novarocks_types::UniqueId::new(3, 1)
        );
        assert_eq!(decoded.inbound()[0].expected_sender_count().get(), 2);
        assert_eq!(encode_topology(&decoded), wire);

        let mut duplicate = wire.clone();
        duplicate.inbound[0]
            .sources
            .push(novarocks::TaskExchangeSource {
                task: Some(encode_task_identity(identity(1, 1, process))),
                fragment_instance_id: Some(unique(1, 1)),
                sender_ordinal: 0,
            });
        assert_eq!(
            decode_topology(&duplicate, FieldPath::root("topology"))
                .expect_err("a repeated source inflates the sender count")
                .kind(),
            ProtocolErrorKind::DuplicateField
        );

        let mut wrong_ordinal = wire.clone();
        wrong_ordinal.inbound[0].sources[1].sender_ordinal = 0;
        assert_eq!(
            decode_topology(&wrong_ordinal, FieldPath::root("topology"))
                .expect_err("each source owns exactly one ordinal")
                .kind(),
            ProtocolErrorKind::InconsistentFields
        );

        let mut unspecified = wire;
        unspecified.outbound[0].partitioning = 0;
        assert_eq!(
            decode_topology(&unspecified, FieldPath::root("topology"))
                .expect_err("the default enum value is not a partitioning")
                .kind(),
            ProtocolErrorKind::InvalidEnum
        );
    }

    #[test]
    fn a_descriptor_round_trips_and_its_projection_is_proved_against_the_plan() {
        let process = backend();
        let wire = simple_descriptor(process);
        let (descriptor, fragment) =
            decode_task_descriptor(&wire, FieldPath::root("descriptor")).expect("legal descriptor");
        assert_eq!(descriptor.identity(), identity(2, 3, process));
        assert_eq!(
            descriptor.fragment_instance_id(),
            novarocks_types::UniqueId::new(7, 8)
        );
        assert_eq!(descriptor.pipeline_dop().get(), 4);
        assert!(descriptor.accepts_split_plan_node(PlanNodeId::new(10).expect("nonnegative")));
        assert!(!descriptor.accepts_split_plan_node(PlanNodeId::new(12).expect("nonnegative")));
        assert_eq!(fragment.plan().fragment_id, 4);
        assert!(fragment.instance_params().typed_result_sink);

        // A descriptor whose parallelism disagrees with the plan it carries is
        // a projection that has drifted, and it must not be installable.
        let mut drifted = wire.clone();
        drifted.pipeline_dop = 8;
        let error = decode_task_descriptor(&drifted, FieldPath::root("descriptor"))
            .expect_err("parallelism must agree with the plan");
        assert_eq!(error.kind(), ProtocolErrorKind::InconsistentFields);

        let mut wrong_finst = wire.clone();
        wrong_finst.fragment_instance_id = Some(unique(9, 9));
        assert_eq!(
            decode_task_descriptor(&wrong_finst, FieldPath::root("descriptor"))
                .expect_err("the kernel key must agree with the plan")
                .kind(),
            ProtocolErrorKind::InconsistentFields
        );

        let mut duplicate_node = wire.clone();
        duplicate_node.split_plan_nodes = vec![10, 10];
        assert!(
            decode_task_descriptor(&duplicate_node, FieldPath::root("descriptor")).is_err(),
            "a repeated split plan node is ambiguous"
        );

        let mut negative_node = wire.clone();
        negative_node.split_plan_nodes = vec![-1];
        assert_eq!(
            decode_task_descriptor(&negative_node, FieldPath::root("descriptor"))
                .expect_err("a plan node id is nonnegative")
                .kind(),
            ProtocolErrorKind::OutOfRange
        );

        let mut no_sink = wire;
        no_sink
            .fragment
            .as_mut()
            .expect("fragment")
            .plan
            .as_mut()
            .expect("plan")
            .sink = None;
        assert_eq!(
            decode_task_descriptor(&no_sink, FieldPath::root("descriptor"))
                .expect_err("a fragment without a sink cannot be installed")
                .kind(),
            ProtocolErrorKind::MissingField
        );
    }

    #[test]
    fn a_descriptor_destination_must_match_an_instance_destination_exactly() {
        let process = backend();
        let target = identity(3, 1, process);
        let mut senders = std::collections::HashMap::new();
        senders.insert(30, 1);
        let wire = novarocks::TaskDescriptor {
            identity: Some(encode_task_identity(identity(2, 3, process))),
            fragment_instance_id: Some(unique(7, 8)),
            pipeline_dop: 1,
            split_plan_nodes: Vec::new(),
            topology: Some(novarocks::TaskExchangeTopology {
                outbound: vec![novarocks::TaskExchangeEdge {
                    edge_id: 1,
                    destination_node_id: 20,
                    partitioning: novarocks::ExchangePartitioning::Random as i32,
                    destinations: vec![novarocks::TaskExchangeDestination {
                        task: Some(encode_task_identity(target)),
                        fragment_instance_id: Some(unique(3, 1)),
                        endpoint: Some(novarocks::QueryControlEndpoint {
                            host: "127.0.0.1".to_owned(),
                            port: 9060,
                        }),
                        destination_node_id: 20,
                        sender_ordinal: 0,
                        sender_count: 2,
                    }],
                }],
                inbound: Vec::new(),
            }),
            fragment: Some(fragment_plan(
                unique(7, 8),
                1,
                vec![novarocks::Destination {
                    finst_id: Some(unique(3, 1)),
                    endpoint: "127.0.0.1:9060".to_owned(),
                    source_finst_id: Some(unique(7, 8)),
                    sender_ordinal: 0,
                    sender_count: 2,
                }],
                senders,
            )),
        };
        assert!(
            decode_task_descriptor(&wire, FieldPath::root("descriptor")).is_ok(),
            "matching addresses must be accepted"
        );

        let mut mismatched = wire.clone();
        mismatched
            .fragment
            .as_mut()
            .expect("fragment")
            .instance_params
            .as_mut()
            .expect("instance")
            .destinations[0]
            .sender_count = 3;
        assert_eq!(
            decode_task_descriptor(&mismatched, FieldPath::root("descriptor"))
                .expect_err("a drifted sender count must fail closed")
                .kind(),
            ProtocolErrorKind::InconsistentFields
        );

        let mut extra = wire;
        extra
            .fragment
            .as_mut()
            .expect("fragment")
            .instance_params
            .as_mut()
            .expect("instance")
            .destinations
            .push(novarocks::Destination {
                finst_id: Some(unique(4, 1)),
                endpoint: "127.0.0.1:9061".to_owned(),
                source_finst_id: Some(unique(7, 8)),
                sender_ordinal: 1,
                sender_count: 2,
            });
        assert!(
            decode_task_descriptor(&extra, FieldPath::root("descriptor")).is_err(),
            "an instance destination with no topology entry is unfenced"
        );
    }

    #[test]
    fn a_status_snapshot_round_trips_every_termination_family() {
        let process = backend();
        let value = identity(2, 3, process);

        let finished = TaskStatus::try_new(
            value,
            TaskStatusVersion::new(5).expect("nonzero"),
            TaskState::Finished,
            None,
            TaskOutputFacts::new(true),
        )
        .expect("legal");
        let encoded = encode_task_status(&finished);
        assert_eq!(
            decode_task_status(&encoded, FieldPath::root("status")),
            Ok(finished.clone())
        );

        for termination in [
            TerminationDetail::Canceled(CancelReason::UpstreamNoLongerNeeded),
            TerminationDetail::Aborted(AbortCause::LeaseExpired),
            TerminationDetail::Failed(TaskFailure::new(
                TaskFailureCategory::Exchange,
                SafeDetail::new("destination failed").expect("fits"),
            )),
        ] {
            let state = match &termination {
                TerminationDetail::Canceled(_) => TaskState::Canceled,
                TerminationDetail::Aborted(_) => TaskState::Aborted,
                TerminationDetail::Failed(_) => TaskState::Failed,
            };
            let status = TaskStatus::try_new(
                value,
                TaskStatusVersion::new(6).expect("nonzero"),
                state,
                Some(termination),
                TaskOutputFacts::default(),
            )
            .expect("legal");
            let encoded = encode_task_status(&status);
            assert_eq!(
                decode_task_status(&encoded, FieldPath::root("status")),
                Ok(status),
                "{state} must round trip with its cause"
            );
        }

        // A terminating state without its cause is not a snapshot.
        let mut no_cause = encode_task_status(&finished);
        no_cause.state = novarocks::TaskState::Failed as i32;
        assert_eq!(
            decode_task_status(&no_cause, FieldPath::root("status"))
                .expect_err("FAILED requires a cause")
                .kind(),
            ProtocolErrorKind::InconsistentFields
        );

        let mut zero_version = encode_task_status(&finished);
        zero_version.status_version = 0;
        assert!(
            decode_task_status(&zero_version, FieldPath::root("status")).is_err(),
            "a status version is nonzero"
        );

        let mut unspecified_state = encode_task_status(&finished);
        unspecified_state.state = 0;
        assert_eq!(
            decode_task_status(&unspecified_state, FieldPath::root("status"))
                .expect_err("the default enum value is not a state")
                .kind(),
            ProtocolErrorKind::InvalidEnum
        );
    }

    #[test]
    fn a_half_reported_writer_pair_is_refused_rather_than_dropped() {
        let process = backend();
        let value = identity(2, 3, process);
        let finished = TaskStatus::try_new(
            value,
            TaskStatusVersion::new(5).expect("nonzero"),
            TaskState::Finished,
            None,
            TaskOutputFacts::new(true),
        )
        .expect("legal");

        let mut half = encode_task_status(&finished);
        half.writer = Some(novarocks::TaskWriterFacts {
            written_rows: Some(7),
            written_bytes: None,
            prepared_write_entries: None,
        });
        assert_eq!(
            decode_task_status(&half, FieldPath::root("status"))
                .expect_err("a half-reported pair loses a metric silently otherwise")
                .kind(),
            ProtocolErrorKind::InconsistentFields
        );

        let mut both = encode_task_status(&finished);
        both.writer = Some(novarocks::TaskWriterFacts {
            written_rows: Some(7),
            written_bytes: Some(64),
            prepared_write_entries: Some(2),
        });
        let decoded =
            decode_task_status(&both, FieldPath::root("status")).expect("a complete pair is legal");
        let writer = decoded.writer().expect("writer facts");
        assert_eq!(writer.written_rows(), Some(7));
        assert_eq!(writer.written_bytes(), Some(64));
        assert_eq!(writer.prepared_write_entries(), Some(2));

        // The output pair has always behaved this way; the writer pair now
        // matches it.
        let mut half_output = encode_task_status(&finished);
        half_output.output = Some(novarocks::TaskOutputFacts {
            responsibility_complete: true,
            buffered_rows: Some(1),
            buffered_bytes: None,
        });
        assert_eq!(
            decode_task_status(&half_output, FieldPath::root("status"))
                .expect_err("the output pair is equally all-or-nothing")
                .kind(),
            ProtocolErrorKind::InconsistentFields
        );
    }

    #[test]
    fn final_task_info_is_checked_against_the_task_the_caller_asked_about() {
        use super::status::decode_final_task_info;

        let process = backend();
        let asked_about = identity(2, 3, process);
        let answered_about = identity(2, 4, process);
        let terminal = TaskStatus::try_new(
            answered_about,
            TaskStatusVersion::new(9).expect("nonzero"),
            TaskState::Finished,
            None,
            TaskOutputFacts::new(true),
        )
        .expect("legal");
        let wire = novarocks::FinalTaskInfo {
            final_status: Some(encode_task_status(&terminal)),
            operator_statistics: Vec::new(),
            operator_statistics_truncated: false,
        };

        // The wire message carries only a status, so without the expected
        // identity a caller would accept another task's final info.
        assert_eq!(
            decode_final_task_info(asked_about, &wire, FieldPath::root("info"))
                .expect_err("final info must answer the task that was asked about")
                .kind(),
            ProtocolErrorKind::InconsistentFields
        );
        let matched = decode_final_task_info(answered_about, &wire, FieldPath::root("info"))
            .expect("the matching task is legal");
        assert_eq!(matched.final_status().version().get(), 9);

        // A non-terminal status is never a final info.
        let running = TaskStatus::try_new(
            answered_about,
            TaskStatusVersion::new(9).expect("nonzero"),
            TaskState::Running,
            None,
            TaskOutputFacts::default(),
        )
        .expect("legal");
        let not_terminal = novarocks::FinalTaskInfo {
            final_status: Some(encode_task_status(&running)),
            operator_statistics: Vec::new(),
            operator_statistics_truncated: false,
        };
        assert!(
            decode_final_task_info(answered_about, &not_terminal, FieldPath::root("info")).is_err()
        );
    }

    /// Catches a decoder that requires both row counts before reporting
    /// either. The producing profile carries the two counters independently,
    /// so demanding the pair would turn a counter that was measured into one
    /// that was never reported.
    #[test]
    fn one_operator_row_count_survives_the_wire_without_the_other() {
        use super::status::{decode_final_task_info, encode_final_task_info};
        use novarocks_execution::task_execution::{FinalTaskInfo, OperatorStatistics};

        let process = backend();
        let task = identity(2, 5, process);
        let terminal = TaskStatus::try_new(
            task,
            TaskStatusVersion::new(3).expect("nonzero"),
            TaskState::Finished,
            None,
            TaskOutputFacts::new(true),
        )
        .expect("legal");
        let info = FinalTaskInfo::try_new(
            task,
            terminal,
            vec![
                OperatorStatistics::new(7, SafeDetail::new("SCAN").expect("fits"))
                    .with_output_rows(11),
            ],
            false,
        )
        .expect("legal");

        let decoded = decode_final_task_info(
            task,
            &encode_final_task_info(&info),
            FieldPath::root("info"),
        )
        .expect("a half pair is legal");

        let entries = decoded.operator_statistics();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].input_rows(), None);
        assert_eq!(entries[0].output_rows(), Some(11));
    }

    #[test]
    fn an_absent_state_and_the_client_only_outcomes_have_no_wire_form() {
        assert_eq!(encode_query_context_state(QueryContextState::Absent), None);
        assert!(encode_query_context_state(QueryContextState::Active).is_some());
        for client_only in [
            OperationOutcome::RetryableTransportUnknown,
            OperationOutcome::RetryableObservationLoss,
            OperationOutcome::NormalDestinationCanceled,
            OperationOutcome::DestinationFailure,
        ] {
            assert_eq!(
                encode_operation_outcome(client_only),
                None,
                "{client_only:?} is not a server-reported category"
            );
        }
        assert!(encode_operation_outcome(OperationOutcome::Accepted).is_some());
        assert!(encode_operation_outcome(OperationOutcome::ResourceExhausted).is_some());
    }

    #[test]
    fn the_two_observation_reads_round_trip_their_exact_task_identity() {
        let process = backend();
        let root = identity(4, 7, process);

        let poll = encode_fetch_task_result(root, MaxWait::default_for(OperationKind::CancelTask));
        let (decoded, max_wait) =
            decode_fetch_task_result(&poll, FieldPath::root("fetch")).expect("a legal poll");
        assert_eq!(decoded, root);
        assert_eq!(max_wait, MaxWait::DEFAULT_UPDATE);

        // A poll that names no task cannot be answered from "the" result
        // buffer, because the address is the only thing that says which one.
        let anonymous = novarocks::FetchTaskResultRequest {
            root_task: None,
            max_wait_millis: 1_000,
        };
        assert_eq!(
            decode_fetch_task_result(&anonymous, FieldPath::root("fetch"))
                .expect_err("no identity")
                .kind(),
            ProtocolErrorKind::MissingField
        );
        // Zero is not "wait as little as possible": it is a duration this
        // contract does not represent.
        let zero = novarocks::FetchTaskResultRequest {
            root_task: Some(encode_task_identity(root)),
            max_wait_millis: 0,
        };
        assert!(decode_fetch_task_result(&zero, FieldPath::root("fetch")).is_err());

        let read = encode_get_final_task_info(root);
        let operation = TaskOperationId::new_v7();
        let decoded = decode_get_final_task_info(&read, operation, FieldPath::root("info"))
            .expect("a legal read");
        assert_eq!(decoded.identity(), root);
        assert_eq!(decoded.envelope().operation_id(), operation);
    }

    fn envelope(kind: OperationKind) -> (TaskOperationId, novarocks::TaskOperationEnvelope) {
        let id = TaskOperationId::new_v7();
        let millis = match kind {
            OperationKind::CreateTask | OperationKind::UpdateQueryContext => 15_000,
            _ => 5_000,
        };
        (
            id,
            novarocks::TaskOperationEnvelope {
                operation_id: Some(super::identity::encode_task_operation_id(id)),
                max_wait_millis: millis,
            },
        )
    }

    #[test]
    fn a_batch_decodes_each_item_independently_with_its_own_envelope() {
        let process = backend();
        let (create_id, create_envelope) = envelope(OperationKind::CreateTask);
        let (cancel_id, cancel_envelope) = envelope(OperationKind::CancelTask);
        let request = novarocks::ApplyTaskOperationsRequest {
            operations: vec![
                novarocks::TaskOperation {
                    envelope: Some(create_envelope),
                    operation: Some(novarocks::task_operation::Operation::CreateTask(
                        novarocks::CreateTaskRequest {
                            query_context: Some(encode_query_context_ref(context(process))),
                            descriptor: Some(simple_descriptor(process)),
                            initial_domains: Vec::new(),
                        },
                    )),
                },
                novarocks::TaskOperation {
                    envelope: Some(cancel_envelope),
                    operation: Some(novarocks::task_operation::Operation::CancelTask(
                        novarocks::CancelTaskRequest {
                            identity: Some(encode_task_identity(identity(2, 3, process))),
                            reason: novarocks::TaskCancelReason::UpstreamNoLongerNeeded as i32,
                        },
                    )),
                },
            ],
        };
        let decoded =
            decode_operation_batch(&request, TransportBudget::DEFAULT, FieldPath::root("batch"))
                .expect("legal batch");
        assert_eq!(decoded.len(), 2);
        assert_eq!(decoded[0].kind(), OperationKind::CreateTask);
        assert_eq!(
            decoded[0].envelope().max_wait().get(),
            Duration::from_secs(15)
        );
        assert_eq!(decoded[1].kind(), OperationKind::CancelTask);
        assert_eq!(
            decoded[1].envelope().max_wait().get(),
            Duration::from_secs(5)
        );
        match &decoded[0] {
            DecodedOperation::CreateTask(create) => {
                assert_eq!(create.descriptor().identity(), identity(2, 3, process));
                assert_eq!(create.request().envelope().operation_id(), create_id);
            }
            other => panic!("expected a create, got kind {:?}", other.kind()),
        }
        match &decoded[1] {
            DecodedOperation::CancelTask(cancel) => {
                assert_eq!(cancel.reason(), CancelReason::UpstreamNoLongerNeeded);
                assert_eq!(cancel.envelope().operation_id(), cancel_id);
            }
            other => panic!("expected a cancel, got kind {:?}", other.kind()),
        }
    }

    #[test]
    fn a_batch_that_exceeds_its_item_budget_is_refused_before_anything_is_walked() {
        let process = backend();
        let (_, envelope_value) = envelope(OperationKind::CancelTask);
        let one = novarocks::TaskOperation {
            envelope: Some(envelope_value),
            operation: Some(novarocks::task_operation::Operation::CancelTask(
                novarocks::CancelTaskRequest {
                    identity: Some(encode_task_identity(identity(2, 3, process))),
                    reason: novarocks::TaskCancelReason::UpstreamNoLongerNeeded as i32,
                },
            )),
        };
        let too_many = novarocks::ApplyTaskOperationsRequest {
            operations: vec![one.clone(); TransportBudget::DEFAULT.max_batch_items() + 1],
        };
        assert_eq!(
            decode_operation_batch(
                &too_many,
                TransportBudget::DEFAULT,
                FieldPath::root("batch")
            )
            .expect_err("over the item budget")
            .kind(),
            ProtocolErrorKind::OutOfRange
        );

        let empty = novarocks::ApplyTaskOperationsRequest {
            operations: Vec::new(),
        };
        assert!(
            decode_operation_batch(&empty, TransportBudget::DEFAULT, FieldPath::root("batch"))
                .is_err(),
            "an empty batch is not a batch"
        );
    }

    #[test]
    fn a_batch_response_must_answer_every_item_in_request_order() {
        let first = TaskOperationId::new_v7();
        let second = TaskOperationId::new_v7();
        let receipt = |id: TaskOperationId, outcome: novarocks::TaskOperationOutcome| {
            novarocks::TaskOperationReceipt {
                operation_id: Some(super::identity::encode_task_operation_id(id)),
                outcome: outcome as i32,
                safe_detail: String::new(),
                safe_field_path: None,
                ack: None,
            }
        };

        // A partial failure is reported per item; it never becomes a
        // batch-global failure.
        let response = novarocks::ApplyTaskOperationsResponse {
            receipts: vec![
                receipt(first, novarocks::TaskOperationOutcome::Accepted),
                receipt(second, novarocks::TaskOperationOutcome::DomainConflict),
            ],
        };
        let decoded =
            decode_receipt_batch(&response, &[first, second], FieldPath::root("response"))
                .expect("legal response");
        assert_eq!(decoded[0].outcome(), OperationOutcome::Accepted);
        assert_eq!(decoded[1].outcome(), OperationOutcome::DomainConflict);

        let swapped = novarocks::ApplyTaskOperationsResponse {
            receipts: vec![
                receipt(second, novarocks::TaskOperationOutcome::Accepted),
                receipt(first, novarocks::TaskOperationOutcome::Accepted),
            ],
        };
        assert!(
            decode_receipt_batch(&swapped, &[first, second], FieldPath::root("response")).is_err(),
            "a receipt must correspond to its own request item"
        );

        let short = novarocks::ApplyTaskOperationsResponse {
            receipts: vec![receipt(first, novarocks::TaskOperationOutcome::Accepted)],
        };
        assert!(
            decode_receipt_batch(&short, &[first, second], FieldPath::root("response")).is_err(),
            "every item must be answered"
        );
    }

    #[test]
    fn an_establish_must_carry_lease_sequence_zero_and_one_envelope_per_descriptor() {
        let process = backend();
        let (_, envelope_value) = envelope(OperationKind::UpdateQueryContext);
        let establish = |sequence: u64,
                         descriptors: usize,
                         envelopes: usize|
         -> novarocks::ApplyTaskOperationsRequest {
            novarocks::ApplyTaskOperationsRequest {
                operations: vec![novarocks::TaskOperation {
                    envelope: Some(envelope_value.clone()),
                    operation: Some(novarocks::task_operation::Operation::UpdateQueryContext(
                        novarocks::UpdateQueryContextRequest {
                            command: Some(
                                novarocks::update_query_context_request::Command::Establish(
                                    novarocks::EstablishQueryContextRequest {
                                        query_context: Some(encode_query_context_ref(context(
                                            process,
                                        ))),
                                        catalog_set: Some(catalog::CatalogSet::default()),
                                        initial_runtime_filter: Some(
                                            novarocks::RuntimeFilterContribution::default(),
                                        ),
                                        initial_credential: Some(
                                            novarocks::QueryContextCredentialDomain {
                                                lease_id: 1,
                                                epoch: 1,
                                                descriptors: vec![
                                                    credential_descriptor(1);
                                                    descriptors
                                                ],
                                                envelopes: vec![
                                                    credential_envelope(
                                                        1,
                                                        SECRET_SENTINEL
                                                    );
                                                    envelopes
                                                ],
                                            },
                                        ),
                                        initial_lease: Some(novarocks::QueryExecutionLeaseGrant {
                                            sequence,
                                            valid_for_millis: 30_000,
                                        }),
                                        query_options: Some(query_options(1)),
                                        native_compatibility_id: Some(
                                            novarocks::NativeCompatibilityId {
                                                value: [0x71; 32].to_vec(),
                                            },
                                        ),
                                    },
                                ),
                            ),
                        },
                    )),
                }],
            }
        };

        let decoded = decode_operation_batch(
            &establish(0, 1, 1),
            TransportBudget::DEFAULT,
            FieldPath::root("batch"),
        )
        .expect("sequence zero with matched descriptors is the legal shape");
        let DecodedOperation::UpdateQueryContext(DecodedUpdateQueryContext::Establish(request)) =
            &decoded[0]
        else {
            panic!("fixture carries an establish");
        };
        assert_eq!(
            request.native_compatibility_id(),
            NativeCompatibilityId::new([0x71; 32]),
            "the exact typed compatibility identity reaches admission"
        );

        let mut missing_compatibility = establish(0, 1, 1);
        let Some(novarocks::task_operation::Operation::UpdateQueryContext(update)) =
            missing_compatibility.operations[0].operation.as_mut()
        else {
            panic!("fixture carries an update query context");
        };
        let Some(novarocks::update_query_context_request::Command::Establish(establish_request)) =
            update.command.as_mut()
        else {
            panic!("fixture carries an establish");
        };
        establish_request.native_compatibility_id = None;
        let error = decode_operation_batch(
            &missing_compatibility,
            TransportBudget::DEFAULT,
            FieldPath::root("batch"),
        )
        .expect_err("an establish must carry its native compatibility identity");
        assert_eq!(error.kind(), ProtocolErrorKind::MissingField);
        assert_eq!(
            error.path().to_string(),
            "batch.operations[0].update_query_context.establish.native_compatibility_id"
        );

        for invalid_len in [31, 33] {
            let mut invalid_compatibility = establish(0, 1, 1);
            let Some(novarocks::task_operation::Operation::UpdateQueryContext(update)) =
                invalid_compatibility.operations[0].operation.as_mut()
            else {
                panic!("fixture carries an update query context");
            };
            let Some(novarocks::update_query_context_request::Command::Establish(
                establish_request,
            )) = update.command.as_mut()
            else {
                panic!("fixture carries an establish");
            };
            establish_request.native_compatibility_id = Some(novarocks::NativeCompatibilityId {
                value: vec![0x71; invalid_len],
            });
            let error = decode_operation_batch(
                &invalid_compatibility,
                TransportBudget::DEFAULT,
                FieldPath::root("batch"),
            )
            .expect_err("a native compatibility identity has an exact wire width");
            assert_eq!(error.kind(), ProtocolErrorKind::InvalidValue);
            assert_eq!(
                error.path().to_string(),
                "batch.operations[0].update_query_context.establish.native_compatibility_id.value"
            );
            assert!(
                error.detail().contains(&format!("got {invalid_len}")),
                "unexpected detail: {}",
                error.detail()
            );
        }
        let mut missing_options = establish(0, 1, 1);
        let Some(novarocks::task_operation::Operation::UpdateQueryContext(update)) =
            missing_options.operations[0].operation.as_mut()
        else {
            panic!("fixture carries an update query context");
        };
        let Some(novarocks::update_query_context_request::Command::Establish(establish_request)) =
            update.command.as_mut()
        else {
            panic!("fixture carries an establish");
        };
        establish_request.query_options = None;
        assert_eq!(
            decode_operation_batch(
                &missing_options,
                TransportBudget::DEFAULT,
                FieldPath::root("batch")
            )
            .expect_err("query options are an immutable establish fact")
            .path()
            .to_string(),
            "batch.operations[0].update_query_context.establish.query_options"
        );
        assert_eq!(
            decode_operation_batch(
                &establish(1, 1, 1),
                TransportBudget::DEFAULT,
                FieldPath::root("batch")
            )
            .expect_err("an establish may only carry the initial sequence")
            .detail(),
            "an initial lease must carry sequence zero"
        );
        // The rule itself belongs to the shared credential validator, so this
        // asserts the refusal comes from there rather than from a second
        // cardinality check maintained here.
        assert_eq!(
            decode_operation_batch(
                &establish(0, 2, 1),
                TransportBudget::DEFAULT,
                FieldPath::root("batch")
            )
            .expect_err("a descriptor without its secret cannot be installed")
            .detail(),
            "credential lease descriptors and confidential envelopes must have identical \
             cardinality"
        );
    }

    #[test]
    fn credential_material_never_reaches_a_rendering_or_an_error() {
        let process = backend();
        let (_, envelope_value) = envelope(OperationKind::UpdateQueryContext);
        let request = novarocks::ApplyTaskOperationsRequest {
            operations: vec![novarocks::TaskOperation {
                envelope: Some(envelope_value),
                operation: Some(novarocks::task_operation::Operation::UpdateQueryContext(
                    novarocks::UpdateQueryContextRequest {
                        command: Some(
                            novarocks::update_query_context_request::Command::AdvanceDomain(
                                novarocks::AdvanceQueryContextDomainRequest {
                                    query_context: Some(encode_query_context_ref(context(
                                        process,
                                    ))),
                                    domain: Some(novarocks::QueryContextDomainUpdate {
                                        domain: Some(
                                            novarocks::query_context_domain_update::Domain::Credential(
                                                novarocks::QueryContextCredentialDomain {
                                                    lease_id: 1,
                                                    epoch: 2,
                                                    descriptors: vec![credential_descriptor(2)],
                                                    envelopes: vec![credential_envelope(
                                                        2,
                                                        SECRET_SENTINEL,
                                                    )],
                                                },
                                            ),
                                        ),
                                    }),
                                },
                            ),
                        ),
                    },
                )),
            }],
        };
        let decoded =
            decode_operation_batch(&request, TransportBudget::DEFAULT, FieldPath::root("batch"))
                .expect("legal advance");
        let sentinel = SECRET_SENTINEL;
        match &decoded[0] {
            DecodedOperation::UpdateQueryContext(command) => {
                assert!(!command.may_create(), "an advance never creates a context");
                let neutral = match command {
                    super::operation::DecodedUpdateQueryContext::AdvanceDomain {
                        domain, ..
                    } => domain.as_neutral(),
                    _ => panic!("expected an advance"),
                };
                let rendered = format!("{neutral:?}");
                assert!(
                    !rendered.contains(sentinel),
                    "credential material leaked into a rendering: {rendered}"
                );
                assert!(rendered.contains("<redacted>"), "{rendered}");
                assert!(rendered.contains("epoch"), "{rendered}");
            }
            other => panic!("expected a context update, got kind {:?}", other.kind()),
        }
    }

    #[test]
    fn task_domains_decode_their_tokens_and_leave_progression_to_the_domain() {
        let process = backend();
        let (_, envelope_value) = envelope(OperationKind::UpdateTask);
        let update = |domain: novarocks::TaskDomainUpdate| novarocks::ApplyTaskOperationsRequest {
            operations: vec![novarocks::TaskOperation {
                envelope: Some(envelope_value.clone()),
                operation: Some(novarocks::task_operation::Operation::UpdateTask(
                    novarocks::UpdateTaskRequest {
                        identity: Some(encode_task_identity(identity(2, 3, process))),
                        domains: vec![domain],
                    },
                )),
            }],
        };

        let open = update(novarocks::TaskDomainUpdate {
            domain: Some(novarocks::task_domain_update::Domain::OpenExchangeEdges(
                novarocks::OpenExchangeEdgesDomain {
                    version: 1,
                    edge_ids: vec![1, 2],
                },
            )),
        });
        assert!(
            decode_operation_batch(&open, TransportBudget::DEFAULT, FieldPath::root("batch"))
                .is_ok()
        );

        // The defect this catches: the decoder pinned an edge open to version
        // one. A producer that feeds several exchange nodes has its edges
        // decided one at a time and mints a version per decision, so pinning
        // the wire rejected every edge after the first -- which is every
        // multi-cast query. Whether a version is a legal progression belongs
        // to the receiving domain, which sees the accepted edge sets; the
        // decoder only sees one message.
        let second_version = update(novarocks::TaskDomainUpdate {
            domain: Some(novarocks::task_domain_update::Domain::OpenExchangeEdges(
                novarocks::OpenExchangeEdgesDomain {
                    version: 2,
                    edge_ids: vec![3],
                },
            )),
        });
        assert!(
            decode_operation_batch(
                &second_version,
                TransportBudget::DEFAULT,
                FieldPath::root("batch")
            )
            .is_ok(),
            "a producer's later edge-open decision carries a later version"
        );

        let zero_version = update(novarocks::TaskDomainUpdate {
            domain: Some(novarocks::task_domain_update::Domain::OpenExchangeEdges(
                novarocks::OpenExchangeEdgesDomain {
                    version: 0,
                    edge_ids: vec![1],
                },
            )),
        });
        assert!(
            decode_operation_batch(
                &zero_version,
                TransportBudget::DEFAULT,
                FieldPath::root("batch")
            )
            .is_err(),
            "an edge-open version is nonzero"
        );

        let empty_edges = update(novarocks::TaskDomainUpdate {
            domain: Some(novarocks::task_domain_update::Domain::OpenExchangeEdges(
                novarocks::OpenExchangeEdgesDomain {
                    version: 1,
                    edge_ids: Vec::new(),
                },
            )),
        });
        assert!(
            decode_operation_batch(
                &empty_edges,
                TransportBudget::DEFAULT,
                FieldPath::root("batch")
            )
            .is_err(),
            "an edge-open must name an edge"
        );

        let filter = update(novarocks::TaskDomainUpdate {
            domain: Some(novarocks::task_domain_update::Domain::DynamicFilter(
                novarocks::TaskDynamicFilterDomain {
                    version: 0,
                    envelope: Some(filter::RuntimeFilterEnvelope::default()),
                },
            )),
        });
        assert!(
            decode_operation_batch(&filter, TransportBudget::DEFAULT, FieldPath::root("batch"))
                .is_err(),
            "a domain version is nonzero"
        );

        let no_domain = novarocks::ApplyTaskOperationsRequest {
            operations: vec![novarocks::TaskOperation {
                envelope: Some(envelope(OperationKind::UpdateTask).1),
                operation: Some(novarocks::task_operation::Operation::UpdateTask(
                    novarocks::UpdateTaskRequest {
                        identity: Some(encode_task_identity(identity(2, 3, process))),
                        domains: Vec::new(),
                    },
                )),
            }],
        };
        assert!(
            decode_operation_batch(
                &no_domain,
                TransportBudget::DEFAULT,
                FieldPath::root("batch")
            )
            .is_err(),
            "an update with no domain change is meaningless"
        );
    }

    #[test]
    fn a_split_assignment_projects_its_watermark_tokens() {
        let process = backend();
        let (_, envelope_value) = envelope(OperationKind::UpdateTask);
        let assignment = novarocks_proto_models::connector_read::SplitAssignment {
            plan_node_id: 10,
            splits: Vec::new(),
            no_more_splits: true,
        };
        let request = novarocks::ApplyTaskOperationsRequest {
            operations: vec![novarocks::TaskOperation {
                envelope: Some(envelope_value),
                operation: Some(novarocks::task_operation::Operation::UpdateTask(
                    novarocks::UpdateTaskRequest {
                        identity: Some(encode_task_identity(identity(2, 3, process))),
                        domains: vec![novarocks::TaskDomainUpdate {
                            domain: Some(novarocks::task_domain_update::Domain::SplitAssignment(
                                novarocks::TaskSplitAssignmentDomain {
                                    assignment: Some(assignment.clone()),
                                },
                            )),
                        }],
                    },
                )),
            }],
        };
        let decoded =
            decode_operation_batch(&request, TransportBudget::DEFAULT, FieldPath::root("batch"))
                .expect("a standalone terminal marker is a legal assignment");
        match &decoded[0] {
            DecodedOperation::UpdateTask(update) => match &update.domains()[0] {
                super::domain::DecodedTaskDomain::SplitAssignment { intent, .. } => {
                    assert_eq!(intent.node(), PlanNodeId::new(10).expect("nonnegative"));
                    assert!(intent.no_more_splits());
                }
                _ => panic!("expected a split assignment"),
            },
            other => panic!("expected an update, got kind {:?}", other.kind()),
        }

        // An assignment with no splits and no terminal marker says nothing.
        let mut silent = request;
        if let Some(novarocks::task_operation::Operation::UpdateTask(update)) =
            silent.operations[0].operation.as_mut()
            && let Some(novarocks::task_domain_update::Domain::SplitAssignment(split)) =
                update.domains[0].domain.as_mut()
            && let Some(assignment) = split.assignment.as_mut()
        {
            assignment.no_more_splits = false;
        }
        assert!(
            decode_operation_batch(&silent, TransportBudget::DEFAULT, FieldPath::root("batch"))
                .is_err(),
            "an assignment with neither splits nor a terminal marker is empty"
        );
    }

    #[test]
    fn an_envelope_must_carry_an_identity_and_a_bounded_wait() {
        let process = backend();
        let cancel = |envelope: Option<novarocks::TaskOperationEnvelope>| {
            novarocks::ApplyTaskOperationsRequest {
                operations: vec![novarocks::TaskOperation {
                    envelope,
                    operation: Some(novarocks::task_operation::Operation::CancelTask(
                        novarocks::CancelTaskRequest {
                            identity: Some(encode_task_identity(identity(2, 3, process))),
                            reason: novarocks::TaskCancelReason::UpstreamNoLongerNeeded as i32,
                        },
                    )),
                }],
            }
        };
        assert!(
            decode_operation_batch(
                &cancel(None),
                TransportBudget::DEFAULT,
                FieldPath::root("batch")
            )
            .is_err(),
            "an operation without an envelope has no identity or deadline"
        );

        let zero_wait = cancel(Some(novarocks::TaskOperationEnvelope {
            operation_id: Some(super::identity::encode_task_operation_id(
                TaskOperationId::new_v7(),
            )),
            max_wait_millis: 0,
        }));
        assert!(
            decode_operation_batch(
                &zero_wait,
                TransportBudget::DEFAULT,
                FieldPath::root("batch")
            )
            .is_err(),
            "a zero wait is not a deadline"
        );

        let unbounded_wait = cancel(Some(novarocks::TaskOperationEnvelope {
            operation_id: Some(super::identity::encode_task_operation_id(
                TaskOperationId::new_v7(),
            )),
            max_wait_millis: 10_000_000,
        }));
        assert_eq!(
            decode_operation_batch(
                &unbounded_wait,
                TransportBudget::DEFAULT,
                FieldPath::root("batch")
            )
            .expect_err("over the representable range")
            .kind(),
            ProtocolErrorKind::OutOfRange
        );
    }

    #[test]
    fn a_create_whose_context_names_another_backend_fails_closed() {
        let process = backend();
        let (_, envelope_value) = envelope(OperationKind::CreateTask);
        let request = novarocks::ApplyTaskOperationsRequest {
            operations: vec![novarocks::TaskOperation {
                envelope: Some(envelope_value),
                operation: Some(novarocks::task_operation::Operation::CreateTask(
                    novarocks::CreateTaskRequest {
                        query_context: Some(encode_query_context_ref(context(backend()))),
                        descriptor: Some(simple_descriptor(process)),
                        initial_domains: Vec::new(),
                    },
                )),
            }],
        };
        assert!(
            decode_operation_batch(&request, TransportBudget::DEFAULT, FieldPath::root("batch"))
                .is_err(),
            "a create must address the context of its own backend"
        );
    }

    /// The frontend encodes and the backend decodes, so the two halves have to
    /// agree. Going through the encode side and back is the only check that
    /// catches them drifting apart.
    #[test]
    fn what_the_frontend_encodes_is_what_the_backend_decodes() {
        use super::descriptor::{WireFragmentPlan, decode_task_descriptor};
        use super::operation::{
            encode_cancel_task, encode_create_task, encode_operation_batch,
            encode_release_query_context, encode_renew_lease,
        };
        use novarocks_execution::task_execution::operation::{
            CancelTask, CreateTask, ReleaseQueryContext, RenewQueryExecutionLease,
        };

        let process = backend();
        let identity = identity(2, 3, process);
        let context = context(process);

        let wire_descriptor = simple_descriptor(process);
        let (descriptor, _) = decode_task_descriptor(&wire_descriptor, FieldPath::root("d"))
            .expect("legal descriptor");
        let fragment = WireFragmentPlan::parse(
            wire_descriptor.fragment.clone().expect("fragment"),
            FieldPath::root("fragment"),
        )
        .expect("legal fragment plan");

        let create_id = TaskOperationId::new_v7();
        let create = CreateTask::try_new(create_id, context, descriptor, Vec::new())
            .expect("matching context");
        let cancel_id = TaskOperationId::new_v7();
        let cancel = CancelTask::new(cancel_id, identity, CancelReason::UpstreamNoLongerNeeded);
        let renew_id = TaskOperationId::new_v7();
        let renew = RenewQueryExecutionLease::new(
            renew_id,
            context,
            LeaseSequence::new(1),
            LeaseValidFor::new(Duration::from_secs(5)).expect("representable"),
        );
        let release_id = TaskOperationId::new_v7();
        let release = ReleaseQueryContext::new(release_id, context);

        let batch = encode_operation_batch(
            vec![
                encode_create_task(&create, &fragment, Vec::new()),
                encode_cancel_task(cancel),
                encode_renew_lease(&renew),
                encode_release_query_context(release),
            ],
            TransportBudget::DEFAULT,
        )
        .expect("inside the transport budget");

        let decoded =
            decode_operation_batch(&batch, TransportBudget::DEFAULT, FieldPath::root("batch"))
                .expect("what was encoded must decode");
        assert_eq!(decoded.len(), 4);
        assert_eq!(
            decoded
                .iter()
                .map(|operation| operation.envelope().operation_id())
                .collect::<Vec<_>>(),
            vec![create_id, cancel_id, renew_id, release_id],
            "operation ids must survive in request order"
        );
        assert_eq!(
            decoded
                .iter()
                .map(DecodedOperation::kind)
                .collect::<Vec<_>>(),
            vec![
                OperationKind::CreateTask,
                OperationKind::CancelTask,
                OperationKind::UpdateQueryContext,
                OperationKind::ReleaseQueryContext,
            ]
        );
        match &decoded[0] {
            DecodedOperation::CreateTask(create) => {
                assert_eq!(create.descriptor().identity(), identity);
                assert_eq!(create.request().context(), context);
                assert_eq!(create.fragment().plan().fragment_id, 4);
            }
            other => panic!("expected a create, got {other:?}"),
        }
        match &decoded[2] {
            DecodedOperation::UpdateQueryContext(command) => {
                assert!(!command.may_create(), "a renewal never creates a context");
                assert_eq!(command.context(), context);
            }
            other => panic!("expected a context update, got {other:?}"),
        }
    }

    #[test]
    fn an_encoded_batch_over_the_budget_is_refused_before_the_wire() {
        use super::operation::{encode_cancel_task, encode_operation_batch};
        use novarocks_execution::task_execution::operation::CancelTask;

        let process = backend();
        let one = encode_cancel_task(CancelTask::new(
            TaskOperationId::new_v7(),
            identity(2, 3, process),
            CancelReason::UpstreamNoLongerNeeded,
        ));
        let over = vec![one; TransportBudget::DEFAULT.max_batch_items() + 1];
        assert_eq!(
            encode_operation_batch(over, TransportBudget::DEFAULT)
                .expect_err("over the item budget")
                .kind(),
            ProtocolErrorKind::OutOfRange,
            "the sender refuses its own oversized batch rather than shipping it"
        );
        assert!(
            encode_operation_batch(Vec::new(), TransportBudget::DEFAULT).is_err(),
            "an empty batch is not a batch on either side"
        );
    }

    #[test]
    fn a_credential_receipt_reports_its_lease_and_epoch() {
        use super::operation::encode_query_context_domain_receipt;
        use novarocks_execution::task_execution::domain::{CredentialLeaseId, DomainProgression};
        use novarocks_execution::task_execution::operation::QueryContextDomainReceipt;

        let receipt = QueryContextDomainReceipt::Credential {
            lease_id: CredentialLeaseId::new(7),
            accepted_epoch: CredentialEpoch::new(4).expect("nonzero"),
            progression: DomainProgression::Apply,
        };
        let encoded =
            encode_query_context_domain_receipt(&receipt).expect("an applied receipt encodes");
        match encoded.receipt.clone().expect("a receipt body") {
            novarocks::query_context_domain_receipt::Receipt::Credential(credential) => {
                assert_eq!(
                    credential.lease_id, 7,
                    "the lease must be reported, not zero"
                );
                assert_eq!(credential.accepted_epoch, 4);
            }
            other => panic!("expected a credential receipt, got {other:?}"),
        }
    }

    #[test]
    fn a_domain_version_may_skip_values_but_a_credential_epoch_may_not() {
        assert!(DomainVersion::new(9).is_ok());
        assert!(DomainVersion::new(0).is_err());
        assert!(CredentialEpoch::new(0).is_err());
        assert_eq!(
            CredentialEpoch::FIRST.next().expect("no overflow"),
            CredentialEpoch::new(2).expect("nonzero")
        );
        assert_eq!(EdgeOpenVersion::FIRST.get(), 1);
        assert!(ExchangeEdgeId::new(0).is_err());
    }

    // -----------------------------------------------------------------------
    // Acknowledgement bodies
    //
    // These close the direction the wire could not previously answer: the
    // frontend could send an operation and read its outcome, but could not
    // read what the backend accepted. An applied create whose body cannot be
    // decoded settles as a missing receipt, which reads to the scheduler as
    // "installed nothing".
    // -----------------------------------------------------------------------

    #[test]
    fn a_neutral_task_domain_encodes_back_to_what_it_was_decoded_from() {
        use super::domain::{decode_task_domain, encode_neutral_task_domain};

        // The frontend holds its domains neutrally, behind a fingerprint. If
        // the encode direction could not project them back, the only way to
        // put one on the wire would be to rebuild it from the neutral fields
        // -- and the split payload has no neutral form at all, so that would
        // silently ship an empty assignment.
        let wire = novarocks::TaskDomainUpdate {
            domain: Some(novarocks::task_domain_update::Domain::SplitAssignment(
                novarocks::TaskSplitAssignmentDomain {
                    assignment: Some(novarocks_proto_models::connector_read::SplitAssignment {
                        plan_node_id: 7,
                        // The terminal marker with no splits: the case a task
                        // that received nothing still has to hear, and the one
                        // whose payload has no neutral form to rebuild from.
                        no_more_splits: true,
                        splits: Vec::new(),
                    }),
                },
            )),
        };
        let decoded =
            decode_task_domain(&wire, FieldPath::root("domain")).expect("a legal assignment");
        let reencoded =
            encode_neutral_task_domain(&decoded.as_neutral(), FieldPath::root("domain"))
                .expect("a decoded domain re-encodes");
        assert_eq!(reencoded, wire, "the round trip changed the request");
    }

    #[test]
    fn every_accepted_domain_receipt_survives_a_round_trip() {
        use super::operation::{
            decode_query_context_domain_receipt, decode_task_domain_receipt,
            encode_query_context_domain_receipt, encode_task_domain_receipt,
        };
        use novarocks_execution::task_execution::domain::{
            CredentialLeaseId, DomainProgression, SplitSequence, SplitWatermark,
        };
        use novarocks_execution::task_execution::operation::{
            PlanNodeSplitReceipt, QueryContextDomainReceipt, TaskDomainReceipt,
        };

        let progressions = [
            DomainProgression::Apply,
            DomainProgression::Idempotent,
            DomainProgression::Older,
        ];
        for progression in progressions {
            let watermark =
                SplitWatermark::empty().apply_batch(SplitSequence::new(4).expect("nonzero"), true);
            let task_cases = vec![
                TaskDomainReceipt::SplitAssignment {
                    nodes: vec![
                        PlanNodeSplitReceipt::new(
                            PlanNodeId::new(7).expect("nonnegative"),
                            watermark,
                        )
                        .with_queued_splits(3),
                    ],
                    progression,
                },
                TaskDomainReceipt::TaskDynamicFilter {
                    accepted_version: Some(DomainVersion::new(9).expect("nonzero")),
                    progression,
                },
                // Nothing accepted yet is a legal receipt, not a malformed one.
                TaskDomainReceipt::TaskDynamicFilter {
                    accepted_version: None,
                    progression,
                },
                TaskDomainReceipt::OpenExchangeEdges {
                    opened: vec![
                        ExchangeEdgeId::new(1).expect("nonzero"),
                        ExchangeEdgeId::new(4).expect("nonzero"),
                    ],
                    progression,
                },
            ];
            for case in task_cases {
                let encoded =
                    encode_task_domain_receipt(&case).expect("an applied receipt encodes");
                let decoded = decode_task_domain_receipt(&encoded, FieldPath::root("receipt"))
                    .expect("decodes");
                assert_eq!(decoded, case, "a task domain receipt lost information");
            }

            let context_cases = vec![
                QueryContextDomainReceipt::CatalogBinding {
                    accepted_version: Some(DomainVersion::new(2).expect("nonzero")),
                    progression,
                },
                QueryContextDomainReceipt::SharedDynamicFilter {
                    accepted_version: None,
                    progression,
                },
                QueryContextDomainReceipt::Credential {
                    lease_id: CredentialLeaseId::new(7),
                    accepted_epoch: CredentialEpoch::new(4).expect("nonzero"),
                    progression,
                },
            ];
            for case in context_cases {
                let encoded =
                    encode_query_context_domain_receipt(&case).expect("an applied receipt encodes");
                let decoded =
                    decode_query_context_domain_receipt(&encoded, FieldPath::root("receipt"))
                        .expect("decodes");
                assert_eq!(decoded, case, "a context domain receipt lost information");
            }
        }
    }

    #[test]
    fn a_conflicting_domain_never_ships_inside_an_applied_acknowledgement() {
        use super::operation::{
            encode_create_task_ack, encode_query_context_domain_receipt,
            encode_task_domain_receipt, encode_update_task_ack,
        };
        use novarocks_execution::task_execution::domain::{DomainConflict, DomainProgression};
        use novarocks_execution::task_execution::operation::{
            CreateTaskReceipt, QueryContextDomainReceipt, TaskDomainReceipt, UpdateTaskReceipt,
        };

        // A conflicting domain refuses its whole operation, so no ack body can
        // represent one. The encoders must refuse rather than pick the nearest
        // representable neighbour, which would report a refusal as an apply.
        for conflict in [
            DomainConflict::SameTokenDifferentContent,
            DomainConflict::Gap,
            DomainConflict::AfterSeal,
            DomainConflict::UnknownMember,
            DomainConflict::NotMonotonic,
        ] {
            let task = TaskDomainReceipt::TaskDynamicFilter {
                accepted_version: None,
                progression: DomainProgression::Conflict(conflict),
            };
            assert!(
                encode_task_domain_receipt(&task).is_none(),
                "{conflict:?} must not encode as an accepted task domain"
            );
            let context = QueryContextDomainReceipt::CatalogBinding {
                accepted_version: None,
                progression: DomainProgression::Conflict(conflict),
            };
            assert!(
                encode_query_context_domain_receipt(&context).is_none(),
                "{conflict:?} must not encode as an accepted context domain"
            );

            // And the refusal must propagate: an ack whose domain list cannot
            // be encoded must not ship a shortened list, which would read as
            // "that domain was never in the request".
            let process = backend();
            let task_identity = identity(1, 1, process);
            assert!(
                encode_create_task_ack(&CreateTaskReceipt::new(
                    task_identity,
                    vec![task.clone()],
                    TaskStatus::created(task_identity),
                ))
                .is_none(),
                "a create acknowledgement must refuse an unrepresentable domain"
            );
            assert!(
                encode_update_task_ack(&UpdateTaskReceipt::new(task_identity, vec![task]))
                    .is_none(),
                "an update acknowledgement must refuse an unrepresentable domain"
            );
        }
    }

    #[test]
    fn an_acknowledgement_for_another_task_is_not_this_task_s_proof() {
        use super::operation::{
            decode_create_task_ack, decode_query_context_ack, decode_update_task_ack,
            encode_create_task_ack, encode_query_context_ack, encode_update_task_ack,
        };
        use novarocks_execution::task_execution::operation::{
            CreateTaskReceipt, QueryContextReceipt, UpdateTaskReceipt,
        };

        let process = backend();
        let mine = identity(1, 1, process);
        let theirs = identity(1, 2, process);

        let create = encode_create_task_ack(&CreateTaskReceipt::new(
            theirs,
            Vec::new(),
            TaskStatus::created(theirs),
        ))
        .expect("an applied create encodes");
        assert_eq!(
            decode_create_task_ack(&create, mine, FieldPath::root("ack"))
                .expect_err("a create acknowledgement for another task is refused")
                .kind(),
            ProtocolErrorKind::InvalidValue
        );
        // The same body against its own task decodes, so the fence above is the
        // identity check and not a broken decoder.
        let decoded = decode_create_task_ack(&create, theirs, FieldPath::root("ack"))
            .expect("its own acknowledgement decodes");
        assert_eq!(decoded.identity(), theirs);
        assert_eq!(decoded.current_status().identity(), theirs);

        let update = encode_update_task_ack(&UpdateTaskReceipt::new(theirs, Vec::new()))
            .expect("an applied update encodes");
        assert_eq!(
            decode_update_task_ack(&update, mine, FieldPath::root("ack"))
                .expect_err("an update acknowledgement for another task is refused")
                .kind(),
            ProtocolErrorKind::InvalidValue
        );

        let my_context = context(process);
        let their_context = context(process);
        let ack = encode_query_context_ack(
            &QueryContextReceipt::new(their_context, QueryContextState::Active),
            None,
        )
        .expect("an active receipt encodes");
        assert_eq!(
            decode_query_context_ack(&ack, my_context, FieldPath::root("ack"))
                .expect_err("a context acknowledgement for another context is refused")
                .kind(),
            ProtocolErrorKind::InvalidValue
        );
        let (receipt, cause) =
            decode_query_context_ack(&ack, their_context, FieldPath::root("ack"))
                .expect("its own acknowledgement decodes");
        assert_eq!(receipt.state(), QueryContextState::Active);
        assert!(cause.is_none(), "an active context has no cause");
    }

    #[test]
    fn a_terminated_context_reports_why_rather_than_dropping_it() {
        use super::operation::{
            decode_query_context_ack, decode_release_ack, encode_query_context_ack,
            encode_release_ack,
        };
        use novarocks_execution::task_execution::operation::QueryContextReceipt;

        // The cause is the only statement of why a context the frontend still
        // believed in is gone. Validating and discarding it would leave the
        // frontend with a terminal state and no reason.
        let process = backend();
        let ctx = context(process);
        let ack = encode_query_context_ack(
            &QueryContextReceipt::new(ctx, QueryContextState::TerminalRetained),
            Some(AbortCause::LeaseExpired),
        )
        .expect("a terminally retained receipt encodes");
        let (receipt, cause) =
            decode_query_context_ack(&ack, ctx, FieldPath::root("ack")).expect("decodes");
        assert_eq!(receipt.state(), QueryContextState::TerminalRetained);
        assert_eq!(cause, Some(AbortCause::LeaseExpired));

        // The release acknowledgement answers the same question, so it must
        // not be the one path that validates the cause and throws it away.
        let mut release = encode_release_ack(
            ctx,
            ReleaseOutcome::AlreadyTerminal,
            QueryContextState::TerminalRetained,
            None,
        )
        .expect("a terminally retained release encodes");
        release.termination_cause = Some(super::operation::encode_abort_cause_field(
            AbortCause::LeaseExpired,
        ));
        let decoded = decode_release_ack(&release, FieldPath::root("ack")).expect("decodes");
        let (acked, outcome, state, cause) = (
            decoded.context,
            decoded.outcome,
            decoded.state,
            decoded.termination_cause,
        );
        assert!(
            decoded.runtime_filter.is_none(),
            "a release that sealed no participant carries no contribution"
        );

        // The release acknowledgement is the only message that carries a
        // backend's terminal runtime-filter observation. A codec that dropped
        // it would leave every part of the carrier correct and the frontend
        // with nothing.
        let sealed = novarocks_proto_codec::lifecycle::terminal::QueryTerminalProfileContributionTelemetry::parse(
            novarocks::QueryTerminalProfileContributionTelemetry {
                telemetry: Some(
                    novarocks::query_terminal_profile_contribution_telemetry::Telemetry::Available(
                        novarocks::QueryTerminalProfileContributionV1 {
                            version: novarocks_proto_codec::lifecycle::terminal::QUERY_TERMINAL_PROFILE_CONTRIBUTION_VERSION_V1,
                            channels: vec![novarocks::QueryTerminalRuntimeFilterChannelV1 {
                                channel_binding_id: 1,
                                channel_id: 7,
                                install_state:
                                    novarocks::QueryTerminalRuntimeFilterChannelInstallStateV1::Installed
                                        as i32,
                                terminal_state:
                                    novarocks::QueryTerminalRuntimeFilterChannelTerminalStateV1::Completed
                                        as i32,
                                latest_published_logical_version: Some(3),
                                published_count: 1,
                                completed_count: 1,
                                unavailable_count: 0,
                                cancelled_count: 0,
                            }],
                            ..Default::default()
                        },
                    ),
                ),
            },
        )
        .expect("the sealed contribution satisfies the terminal contract");
        let carried = encode_release_ack(
            ctx,
            ReleaseOutcome::Released,
            QueryContextState::TerminalRetained,
            Some(&sealed),
        )
        .expect("a release carrying a contribution encodes");
        assert_eq!(
            decode_release_ack(&carried, FieldPath::root("ack"))
                .expect("decodes")
                .runtime_filter
                .expect("the contribution survives the round trip"),
            sealed
        );

        // A contribution that does not satisfy the terminal contract is
        // refused on the release that carried it, not discovered several hops
        // later.
        let mut malformed = carried.clone();
        malformed.runtime_filter =
            Some(novarocks::QueryTerminalProfileContributionTelemetry { telemetry: None });
        assert!(
            decode_release_ack(&malformed, FieldPath::root("ack")).is_err(),
            "an empty telemetry oneof is neither available nor unavailable"
        );
        assert_eq!(acked, ctx);
        assert_eq!(outcome, ReleaseOutcome::AlreadyTerminal);
        assert_eq!(state, QueryContextState::TerminalRetained);
        assert_eq!(cause, Some(AbortCause::LeaseExpired));
    }
}
