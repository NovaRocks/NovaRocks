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

use crate::{FieldPath, ProtocolError, ProtocolErrorKind};

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
        DecodedOperation, decode_operation_batch, decode_receipt_batch, encode_operation_outcome,
        encode_query_context_state,
    };
    use super::status::{decode_task_status, encode_task_status};
    use crate::{FieldPath, ProtocolErrorKind};
    use novarocks_execution::task_execution::domain::{
        CredentialEpoch, DomainVersion, EdgeOpenVersion, ExchangeEdgeId, PlanNodeId,
    };
    use novarocks_execution::task_execution::identity::{
        QueryContextRef, TaskIdentity, TaskOperationId,
    };
    use novarocks_execution::task_execution::lease::{LeaseReceipt, LeaseSequence, LeaseValidFor};
    use novarocks_execution::task_execution::operation::{
        OperationKind, OperationOutcome, TransportBudget,
    };
    use novarocks_execution::task_execution::status::{
        AbortCause, CancelReason, SafeDetail, TaskFailure, TaskFailureCategory, TaskOutputFacts,
        TaskState, TaskStatus, TaskStatusVersion, TerminationDetail,
    };
    use novarocks_execution::task_execution::transition::QueryContextState;
    use novarocks_proto_models::{catalog, common, filter, novarocks, plan};
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
                sources: vec![novarocks::TaskExchangeSource {
                    task: Some(encode_task_identity(identity(1, 1, process))),
                    fragment_instance_id: Some(unique(1, 1)),
                }],
            }],
        };
        let decoded = decode_topology(&wire, FieldPath::root("topology")).expect("legal topology");
        assert_eq!(decoded.outbound().len(), 1);
        assert_eq!(
            decoded.outbound()[0].destinations()[0].fragment_instance_id(),
            novarocks_types::UniqueId::new(3, 1)
        );
        assert_eq!(decoded.inbound()[0].expected_sender_count().get(), 1);
        assert_eq!(encode_topology(&decoded), wire);

        let mut duplicate = wire.clone();
        duplicate.inbound[0]
            .sources
            .push(novarocks::TaskExchangeSource {
                task: Some(encode_task_identity(identity(1, 1, process))),
                fragment_instance_id: Some(unique(1, 1)),
            });
        assert_eq!(
            decode_topology(&duplicate, FieldPath::root("topology"))
                .expect_err("a repeated source inflates the sender count")
                .kind(),
            ProtocolErrorKind::DuplicateField
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
                    operation: Some(
                        novarocks::task_operation::Operation::UpdateQueryContext(
                            novarocks::UpdateQueryContextRequest {
                                command: Some(
                                    novarocks::update_query_context_request::Command::Establish(
                                        novarocks::EstablishQueryContextRequest {
                                            query_context: Some(encode_query_context_ref(
                                                context(process),
                                            )),
                                            catalog_set: Some(catalog::CatalogSet::default()),
                                            initial_runtime_filter: Some(
                                                novarocks::RuntimeFilterContribution::default(),
                                            ),
                                            initial_credential: Some(
                                                novarocks::QueryContextCredentialDomain {
                                                    lease_id: 1,
                                                    epoch: 1,
                                                    descriptors: vec![
                                                        novarocks::CredentialLeaseDescriptor::default();
                                                        descriptors
                                                    ],
                                                    envelopes: vec![
                                                        novarocks::CredentialLeaseSecretEnvelope {
                                                            lease_id: vec![1u8; 16],
                                                            epoch: 1,
                                                            s3: None,
                                                        };
                                                        envelopes
                                                    ],
                                                },
                                            ),
                                            initial_lease: Some(
                                                novarocks::QueryExecutionLeaseGrant {
                                                    sequence,
                                                    valid_for_millis: 30_000,
                                                },
                                            ),
                                            query_options: Some(query_options(1)),
                                            native_compatibility_id: None,
                                        },
                                    ),
                                ),
                            },
                        ),
                    ),
                }],
            }
        };

        assert!(
            decode_operation_batch(
                &establish(0, 1, 1),
                TransportBudget::DEFAULT,
                FieldPath::root("batch")
            )
            .is_ok(),
            "sequence zero with matched descriptors is the legal shape"
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
        assert_eq!(
            decode_operation_batch(
                &establish(0, 2, 1),
                TransportBudget::DEFAULT,
                FieldPath::root("batch")
            )
            .expect_err("a descriptor without its secret cannot be installed")
            .detail(),
            "each credential descriptor requires exactly one envelope"
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
                                                    descriptors: vec![
                                                        novarocks::CredentialLeaseDescriptor::default(),
                                                    ],
                                                    envelopes: vec![
                                                        novarocks::CredentialLeaseSecretEnvelope {
                                                            lease_id: vec![2u8; 16],
                                                            epoch: 2,
                                                            s3: Some(
                                                                novarocks::CredentialLeaseS3SecretMaterial {
                                                                    access_key_id: "AKIA".to_owned(),
                                                                    secret_access_key: SECRET_SENTINEL
                                                                        .to_owned(),
                                                                    session_token: String::new(),
                                                                    session_token_expires_at_unix_ms: 1,
                                                                },
                                                            ),
                                                        },
                                                    ],
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
    fn task_domains_decode_their_tokens_and_refuse_a_reconfigure() {
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

        let reconfigure = update(novarocks::TaskDomainUpdate {
            domain: Some(novarocks::task_domain_update::Domain::OpenExchangeEdges(
                novarocks::OpenExchangeEdgesDomain {
                    version: 2,
                    edge_ids: vec![1],
                },
            )),
        });
        assert_eq!(
            decode_operation_batch(
                &reconfigure,
                TransportBudget::DEFAULT,
                FieldPath::root("batch")
            )
            .expect_err("this release has no reconfigure")
            .detail(),
            "this release opens an edge exactly once, at version one"
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
}
