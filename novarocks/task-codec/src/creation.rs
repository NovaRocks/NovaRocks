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

//! Creation carrier codec.
//!
//! A create request carries two immutable byte fields. The static fragment is
//! one fragment's plan, encoded once per plan version by the frontend and sent
//! unchanged to every task of that fragment. The creation metadata is the
//! task-local half: its context, descriptor, initial domains and assignment,
//! each fact with exactly one owner.
//!
//! Ingress decodes the metadata once for every request, because identity,
//! bounds and local structure decide whether a request may enter the protocol
//! at all. It does not decode the static fragment. Only the backend that wins
//! a task's creation interprets that plan, through
//! [`decode_static_package`]; a request that names an identity which already
//! exists is answered from that task's lifecycle, and its static bytes are
//! never read.
// Design: ADR-0161 (docs/adr/ADR-0161-pure-local-programs-and-supervised-task-lifecycle.md)

pub mod size;

use std::any::Any;
use std::collections::BTreeSet;

use novarocks_execution_contract::task_execution::creation::{CreationContent, FrozenBytes};
use novarocks_execution_contract::task_execution::descriptor::TaskDescriptor;
use novarocks_execution_contract::task_execution::identity::QueryContextRef;
use novarocks_execution_contract::task_execution::operation::OperationEnvelope;
use novarocks_proto_codec::{FieldPath, ProtocolError};
use novarocks_proto_models::novarocks;
use prost::Message;
use prost::encoding::{encoded_len_varint, key_len};

use crate::descriptor::{MAX_TOPOLOGY_ENTRIES, encode_task_descriptor};
use crate::identity::encode_query_context_ref;
use crate::operation::encode_envelope;
use crate::{duplicate, inconsistent, invalid, missing, out_of_range};

/// Largest number of scan plan nodes one task's initial assignment may name.
///
/// It is the same bound as the descriptor's split plan nodes: both are sets of
/// one task's scan nodes, drawn from the same plan.
pub const MAX_INITIAL_SCAN_NODES: usize = 1024;

/// A task assignment that passed its local structure checks.
///
/// It is retained, unread, for the backend that wins the task's creation. A
/// request that never wins drops it. Nothing compares it.
#[derive(Debug)]
pub struct DecodedTaskAssignment {
    instance_ordinal: u32,
    encoded: Box<[u8]>,
}

/// A private, already validated carrier could not be restored by its owner.
#[derive(Debug)]
pub struct AssignmentRestorationError(prost::DecodeError);
impl std::fmt::Display for AssignmentRestorationError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "validated task assignment could not be restored: {}",
            self.0
        )
    }
}
impl std::error::Error for AssignmentRestorationError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(&self.0)
    }
}
impl novarocks_execution_contract::failure::FailureProjection for AssignmentRestorationError {
    fn failure_fact(&self) -> novarocks_execution_contract::failure::FailureFact {
        use novarocks_execution_contract::failure::*;
        // Protocol codes 1000..1009 belong to retained task-carrier restoration.
        FailureFact::new(
            FailureClass::Invariant,
            FailureReason::new(domain::PROTOCOL, std::num::NonZeroU16::new(1000).unwrap()),
        )
    }
}

impl DecodedTaskAssignment {
    /// This task's position among its fragment's instances.
    pub const fn instance_ordinal(&self) -> u32 {
        self.instance_ordinal
    }

    /// Restores the validated private assignment only when its winner prepares.
    pub fn into_wire(self) -> Result<novarocks::TaskAssignment, AssignmentRestorationError> {
        novarocks::TaskAssignment::decode(self.encoded.as_ref()).map_err(AssignmentRestorationError)
    }
}

impl CreationContent for DecodedTaskAssignment {
    fn encoded_len(&self) -> usize {
        self.encoded.len()
    }

    fn retained_bytes(&self) -> usize {
        std::mem::size_of::<Self>().saturating_add(self.encoded.len())
    }

    fn into_stored(self: Box<Self>) -> Box<dyn Any + Send> {
        self
    }
}

/// Recovers the assignment this codec decoded for a create request.
///
/// `None` means the content is not one this codec produced. The caller is the
/// creation winner, which fails that creation rather than guessing.
pub fn take_task_assignment(content: Box<dyn CreationContent>) -> Option<DecodedTaskAssignment> {
    content
        .into_stored()
        .downcast::<DecodedTaskAssignment>()
        .ok()
        .map(|assignment| *assignment)
}

/// Validates the local structure of one task assignment.
///
/// Every check reads only the metadata it arrived in: the instance ordinal
/// must be representable by the kernel, the initial scan nodes must be a
/// bounded, strictly ordered set, and the sink edges must bind each of the
/// descriptor's outbound edges exactly once. Whether the static plan has the
/// scan nodes and sink branches these name is proved by the creation winner,
/// the only owner that decodes that plan.
pub fn decode_task_assignment(
    src: novarocks::TaskAssignment,
    descriptor: &TaskDescriptor,
    path: FieldPath,
) -> Result<DecodedTaskAssignment, ProtocolError> {
    if i32::try_from(src.instance_ordinal).is_err() {
        return Err(out_of_range(
            path.clone().field("instance_ordinal"),
            "instance ordinal exceeds the kernel's instance width",
        ));
    }

    if src.initial_scan_ranges.len() > MAX_INITIAL_SCAN_NODES {
        return Err(out_of_range(
            path.clone().field("initial_scan_ranges"),
            "initial scan node count exceeds the hard limit",
        ));
    }
    let mut previous = None;
    for (index, node) in src.initial_scan_ranges.iter().enumerate() {
        let node_path = path
            .clone()
            .field("initial_scan_ranges")
            .index(index)
            .field("plan_node_id");
        if node.plan_node_id < 0 {
            return Err(out_of_range(node_path, "plan node id must be nonnegative"));
        }
        if previous.is_some_and(|previous| previous >= node.plan_node_id) {
            return Err(duplicate(
                node_path,
                "initial scan nodes must be strictly ascending and unique",
            ));
        }
        previous = Some(node.plan_node_id);
    }

    let outbound = descriptor.topology().outbound();
    if src.sink_edge_ids.len() > MAX_TOPOLOGY_ENTRIES {
        return Err(out_of_range(
            path.clone().field("sink_edge_ids"),
            "sink edge count exceeds the hard limit",
        ));
    }
    if src.sink_edge_ids.len() != outbound.len() {
        return Err(inconsistent(
            path.clone().field("sink_edge_ids"),
            "sink edges must bind each outbound topology edge exactly once",
        ));
    }
    let mut bound = BTreeSet::new();
    for (index, &edge_id) in src.sink_edge_ids.iter().enumerate() {
        let edge_path = path.clone().field("sink_edge_ids").index(index);
        if edge_id == 0 || !bound.insert(edge_id) {
            return Err(duplicate(
                edge_path,
                "sink edge id must be nonzero and bound only once",
            ));
        }
        if !outbound.iter().any(|edge| edge.edge_id().get() == edge_id) {
            return Err(inconsistent(
                edge_path,
                "sink edge id is absent from the task topology",
            ));
        }
    }

    // This private carrier has no fingerprint and never participates in replay
    // comparison. One bounded encode drops the transient DTO (including its
    // opaque HashMap allocations) before Accepted retains the assignment.
    // Boxed bytes have an exact allocation length; decoding belongs to prepare.
    let instance_ordinal = src.instance_ordinal;
    let encoded = src.encode_to_vec().into_boxed_slice();
    Ok(DecodedTaskAssignment {
        instance_ordinal,
        encoded,
    })
}

/// One fragment's compiled-package carrier, decoded by the backend that won a
/// task's creation.
///
/// Only the carrier law is proved here. The package bytes are interpreted by
/// the package receiver, after its generated resource preflight.
#[derive(Debug)]
pub struct DecodedStaticPackage {
    package: prost::bytes::Bytes,
}

impl DecodedStaticPackage {
    pub fn package(&self) -> &prost::bytes::Bytes {
        &self.package
    }

    pub fn into_package(self) -> prost::bytes::Bytes {
        self.package
    }
}

/// Decodes one fragment's compiled-package carrier.
///
/// The package owns its plan version, contract revision and parallelism domain.
/// This outer carrier owns only the exact immutable package bytes.
pub fn decode_static_package(
    bytes: &FrozenBytes,
    path: FieldPath,
) -> Result<DecodedStaticPackage, ProtocolError> {
    let frozen = novarocks::FrozenFragment::decode(bytes.to_bytes())
        .map_err(|error| invalid(path.clone(), format!("invalid frozen fragment: {error}")))?;
    if frozen.package.is_empty() {
        return Err(missing(
            path.field("package"),
            "a compiled carrier requires its fragment package",
        ));
    }
    Ok(DecodedStaticPackage {
        package: frozen.package,
    })
}

/// Builds the task-local creation carrier from its single owners.
///
/// The frontend calls this once to learn the carrier's exact length and once
/// to freeze it, and discards the returned message both times.
pub fn encode_creation_metadata(
    context: QueryContextRef,
    descriptor: &TaskDescriptor,
    assignment: novarocks::TaskAssignment,
    initial_domains: Vec<novarocks::TaskDomainUpdate>,
) -> novarocks::CreationMetadata {
    novarocks::CreationMetadata {
        query_context: Some(encode_query_context_ref(context)),
        descriptor: Some(encode_task_descriptor(descriptor)),
        initial_domains,
        assignment: Some(assignment),
    }
}

/// The exact encoded size of one create operation built from frozen
/// carriers, computed from their lengths without touching either.
///
/// Only the envelope is encoded, and it is a few bytes: an operation id and a
/// wait. That is what lets a sender price a resend whose envelope changed
/// without walking or re-encoding the carriers it reuses.
pub fn create_task_operation_encoded_len(
    envelope: OperationEnvelope,
    frozen_fragment_len: usize,
    creation_metadata_len: usize,
) -> usize {
    let create =
        bytes_field_len(4, frozen_fragment_len) + bytes_field_len(5, creation_metadata_len);
    message_field_len(1, encode_envelope(envelope).encoded_len()) + message_field_len(2, create)
}

fn bytes_field_len(tag: u32, len: usize) -> usize {
    // A proto3 singular bytes field is omitted when empty.
    if len == 0 {
        0
    } else {
        key_len(tag) + encoded_len_varint(len as u64) + len
    }
}

fn message_field_len(tag: u32, len: usize) -> usize {
    key_len(tag) + encoded_len_varint(len as u64) + len
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::operation::encode_create_task;
    use novarocks_execution_contract::task_execution::descriptor::ExchangeTopology;
    use novarocks_execution_contract::task_execution::identity::{TaskIdentity, TaskOperationId};
    use novarocks_execution_contract::task_execution::operation::OperationKind;
    use novarocks_types::UniqueId;
    use novarocks_types::identity::{
        AttemptId, BackendProcessId, QueryExecutionId, QueryId, StageId, TaskId,
    };
    use prost::bytes::Bytes;
    use std::num::NonZeroUsize;

    fn descriptor() -> TaskDescriptor {
        let execution =
            QueryExecutionId::new(QueryId::new(1, 2), AttemptId::new(1).expect("nonzero"))
                .expect("nonzero");
        TaskDescriptor::try_new(
            TaskIdentity::new(
                execution,
                StageId::new(1).expect("nonzero"),
                TaskId::new(1).expect("nonzero"),
                BackendProcessId::new_v7(),
            ),
            UniqueId::new(1, 1),
            NonZeroUsize::new(2).expect("nonzero"),
            Vec::new(),
            ExchangeTopology::default(),
        )
        .expect("legal descriptor")
    }

    fn scan(node: i32) -> novarocks::TaskScanRanges {
        novarocks::TaskScanRanges {
            plan_node_id: node,
            ranges: Vec::new(),
        }
    }

    fn frozen(fragment: novarocks::FrozenFragment) -> FrozenBytes {
        FrozenBytes::freeze(Bytes::from(fragment.encode_to_vec()))
    }

    #[test]
    fn a_compiled_package_carrier_retains_exact_bytes_and_refuses_missing_or_malformed_input() {
        let package = Bytes::from_static(b"opaque fragment package");
        let decoded = decode_static_package(
            &frozen(novarocks::FrozenFragment {
                package: package.clone(),
            }),
            FieldPath::root("f"),
        )
        .expect("package carrier");
        assert_eq!(decoded.package(), &package);
        assert_eq!(decoded.into_package(), package);
        assert!(
            decode_static_package(
                &frozen(novarocks::FrozenFragment::default()),
                FieldPath::root("f")
            )
            .is_err()
        );
        assert!(
            decode_static_package(
                &FrozenBytes::freeze(Bytes::from_static(&[0x3a, 0x80])),
                FieldPath::root("f")
            )
            .is_err()
        );
    }

    #[test]
    fn assignment_retains_exact_owned_bytes_and_restores_nested_scan_facts() {
        let file = novarocks::FileScanRange {
            full_path: Some("s3://warehouse/file.parquet".to_owned()),
            included_positions: vec![1, 2, 3],
            file_pruning_min_max_values: (0..128)
                .map(|key| (key, novarocks::FilePruningMinMaxValue::default()))
                .collect(),
            ..Default::default()
        };
        let assignment = novarocks::TaskAssignment {
            instance_ordinal: 0,
            initial_scan_ranges: vec![novarocks::TaskScanRanges {
                plan_node_id: 1,
                ranges: vec![novarocks::ScanRangeParams {
                    range: Some(novarocks::ScanRange {
                        kind: Some(novarocks::scan_range::Kind::File(file)),
                    }),
                    ..Default::default()
                }],
            }],
            sink_edge_ids: Vec::new(),
        };
        let decoded =
            decode_task_assignment(assignment.clone(), &descriptor(), FieldPath::root("a"))
                .unwrap();
        assert_eq!(decoded.encoded_len(), assignment.encoded_len());
        assert_eq!(
            decoded.retained_bytes(),
            std::mem::size_of::<DecodedTaskAssignment>() + decoded.encoded_len()
        );
        assert_eq!(decoded.into_wire().unwrap(), assignment);
    }

    #[test]
    fn an_assignment_orders_its_scan_nodes_and_binds_every_outbound_edge() {
        let descriptor = descriptor();
        let legal = novarocks::TaskAssignment {
            instance_ordinal: 3,
            initial_scan_ranges: vec![scan(1), scan(4)],
            sink_edge_ids: Vec::new(),
        };
        let decoded = decode_task_assignment(legal.clone(), &descriptor, FieldPath::root("a"))
            .expect("legal assignment");
        assert_eq!(decoded.instance_ordinal(), 3);
        assert_eq!(CreationContent::encoded_len(&decoded), legal.encoded_len());

        let mut unordered = legal.clone();
        unordered.initial_scan_ranges = vec![scan(4), scan(1)];
        assert!(decode_task_assignment(unordered, &descriptor, FieldPath::root("a")).is_err());

        let mut repeated = legal.clone();
        repeated.initial_scan_ranges = vec![scan(4), scan(4)];
        assert!(decode_task_assignment(repeated, &descriptor, FieldPath::root("a")).is_err());

        let mut unbound = legal.clone();
        unbound.sink_edge_ids = vec![1];
        assert!(
            decode_task_assignment(unbound, &descriptor, FieldPath::root("a")).is_err(),
            "a sink edge the topology does not freeze cannot be bound"
        );

        let mut wide = legal;
        wide.instance_ordinal = u32::MAX;
        assert!(decode_task_assignment(wide, &descriptor, FieldPath::root("a")).is_err());
    }

    #[test]
    fn the_codec_recovers_only_its_own_assignment() {
        let decoded = decode_task_assignment(
            novarocks::TaskAssignment::default(),
            &descriptor(),
            FieldPath::root("a"),
        )
        .expect("legal assignment");
        assert!(take_task_assignment(Box::new(decoded)).is_some());

        #[derive(Debug)]
        struct Foreign;
        impl CreationContent for Foreign {
            fn retained_bytes(&self) -> usize {
                std::mem::size_of::<Self>()
            }
            fn encoded_len(&self) -> usize {
                0
            }
            fn into_stored(self: Box<Self>) -> Box<dyn Any + Send> {
                self
            }
        }
        assert!(take_task_assignment(Box::new(Foreign)).is_none());
    }

    #[test]
    fn the_priced_create_length_is_the_encoded_operation_length() {
        let envelope = OperationEnvelope::with_default_wait(
            TaskOperationId::new_v7(),
            OperationKind::CreateTask,
        );
        for (fragment, metadata) in [(0, 0), (1, 1), (127, 128), (16_384, 3), (70_000, 200_000)] {
            let frozen_fragment = FrozenBytes::freeze(Bytes::from(vec![7_u8; fragment]));
            let creation_metadata = FrozenBytes::freeze(Bytes::from(vec![9_u8; metadata]));
            let operation = encode_create_task(envelope, &frozen_fragment, &creation_metadata);
            assert_eq!(
                create_task_operation_encoded_len(envelope, fragment, metadata),
                operation.encoded_len(),
                "fragment {fragment} metadata {metadata}"
            );
        }
    }
}

#[cfg(test)]
mod restoration_failure_tests {
    use super::*;
    use novarocks_execution_contract::failure::{FailureClass, FailureProjection};
    use std::error::Error;
    #[test]
    fn restoration_keeps_sdk_source_without_relabeling_it_as_input_or_capacity() {
        let error = AssignmentRestorationError(prost::DecodeError::new(
            "resource exhausted in carrier fixture",
        ));
        assert_eq!(error.failure_fact().class(), FailureClass::Invariant);
        assert_eq!(error.failure_fact().resource(), None);
        assert!(error.source().unwrap().is::<prost::DecodeError>());
    }
}
