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
//! [`decode_static_fragment`]; a request that names an identity which already
//! exists is answered from that task's lifecycle, and its static bytes are
//! never read.
// Design: ADR-0146 (docs/adr/ADR-0146-logical-execution-owns-attempts-and-result-visibility.md)

use std::any::Any;
use std::collections::BTreeSet;
use std::num::NonZeroUsize;

use novarocks_execution_contract::FragmentContractVersion;
use novarocks_execution_contract::FragmentSinkKind;
use novarocks_execution_contract::task_execution::creation::{CreationContent, FrozenBytes};
use novarocks_execution_contract::task_execution::descriptor::TaskDescriptor;
use novarocks_execution_contract::task_execution::identity::QueryContextRef;
use novarocks_execution_contract::task_execution::operation::OperationEnvelope;
use novarocks_proto_codec::{FieldPath, ProtocolError};
use novarocks_proto_models::{novarocks, plan};
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

impl DecodedTaskAssignment {
    /// This task's position among its fragment's instances.
    pub const fn instance_ordinal(&self) -> u32 {
        self.instance_ordinal
    }

    /// Restores the validated private assignment only when its winner prepares.
    pub fn into_wire(self) -> Result<novarocks::TaskAssignment, prost::DecodeError> {
        novarocks::TaskAssignment::decode(self.encoded.as_ref())
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

/// One fragment's static plan, decoded by the backend that won a task's
/// creation.
#[derive(Debug)]
pub struct DecodedStaticFragment {
    frozen: novarocks::FrozenFragment,
    sink_kind: FragmentSinkKind,
}

impl DecodedStaticFragment {
    /// The plan fragment, for the backend's own plan decoder.
    pub fn plan(&self) -> &plan::PlanFragment {
        self.frozen
            .plan
            .as_ref()
            .expect("a decoded static fragment always has a plan")
    }

    pub fn into_plan(self) -> plan::PlanFragment {
        self.frozen
            .plan
            .expect("a decoded static fragment always has a plan")
    }

    /// What this fragment's validated sink does.
    pub const fn sink_kind(&self) -> FragmentSinkKind {
        self.sink_kind
    }

    pub fn plan_version(&self) -> &[u8] {
        &self.frozen.plan_version
    }

    pub const fn plan_contract_revision(&self) -> u32 {
        self.frozen.plan_contract_revision
    }

    /// Proves one task's parallelism lies inside this fragment's frozen
    /// parallelism domain.
    pub fn verify_task_dop(
        &self,
        pipeline_dop: NonZeroUsize,
        path: FieldPath,
    ) -> Result<(), ProtocolError> {
        let domain = self
            .frozen
            .pipeline_dop_domain
            .as_ref()
            .expect("a decoded static fragment always has a DOP domain");
        let dop = u32::try_from(pipeline_dop.get())
            .map_err(|_| out_of_range(path.clone(), "task parallelism exceeds the wire width"))?;
        if dop < domain.min
            || dop > domain.max
            || (domain.requires_power_of_two && !dop.is_power_of_two())
        {
            return Err(inconsistent(
                path,
                "task parallelism is outside the frozen fragment DOP domain",
            ));
        }
        Ok(())
    }
}

/// Decodes and validates one fragment's static plan carrier.
///
/// The header is proved here: plan version, plan and fragment contract
/// revisions, a nonempty parallelism domain, and a plan with a known sink.
/// The plan tree itself is interpreted by the backend's plan decoder.
pub fn decode_static_fragment(
    bytes: &FrozenBytes,
    path: FieldPath,
) -> Result<DecodedStaticFragment, ProtocolError> {
    let frozen = novarocks::FrozenFragment::decode(bytes.to_bytes())
        .map_err(|error| invalid(path.clone(), format!("invalid frozen fragment: {error}")))?;
    if frozen.plan_version.len() != 16 || frozen.plan_version.iter().all(|byte| *byte == 0) {
        return Err(invalid(
            path.clone().field("plan_version"),
            "plan version must contain 16 nonzero identity bytes",
        ));
    }
    if frozen.plan_contract_revision == 0 {
        return Err(invalid(
            path.clone().field("plan_contract_revision"),
            "plan contract revision must be nonzero",
        ));
    }
    if frozen.fragment_contract_version != u32::from(FragmentContractVersion::CURRENT.get()) {
        return Err(invalid(
            path.clone().field("fragment_contract_version"),
            "fragment contract version is unsupported",
        ));
    }
    let dop = frozen.pipeline_dop_domain.as_ref().ok_or_else(|| {
        missing(
            path.clone().field("pipeline_dop_domain"),
            "frozen fragment requires a pipeline DOP domain",
        )
    })?;
    if dop.min == 0 || dop.max < dop.min {
        return Err(invalid(
            path.clone().field("pipeline_dop_domain"),
            "pipeline DOP domain must be nonempty and nonzero",
        ));
    }
    let plan = frozen.plan.as_ref().ok_or_else(|| {
        missing(
            path.clone().field("plan"),
            "frozen fragment requires a plan fragment",
        )
    })?;
    let sink = plan.sink.as_ref().ok_or_else(|| {
        missing(
            path.clone().field("plan").field("sink"),
            "plan fragment requires a sink",
        )
    })?;
    let sink_kind = decode_sink_kind(sink, path.field("plan").field("sink"))?;
    Ok(DecodedStaticFragment { frozen, sink_kind })
}

fn decode_sink_kind(
    sink: &plan::DataSink,
    path: FieldPath,
) -> Result<FragmentSinkKind, ProtocolError> {
    let kind = sink
        .kind
        .as_ref()
        .ok_or_else(|| missing(path, "data sink requires a kind"))?;
    Ok(match kind {
        plan::data_sink::Kind::Result(_) => FragmentSinkKind::Result,
        plan::data_sink::Kind::Noop(_) => FragmentSinkKind::Noop,
        plan::data_sink::Kind::DataStream(_) => FragmentSinkKind::DataStream,
        plan::data_sink::Kind::MultiCastDataStream(_) => FragmentSinkKind::MultiCastDataStream,
        plan::data_sink::Kind::ChangeStreamRouter(_) => FragmentSinkKind::SplitDataStream,
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

    #[test]
    fn a_static_fragment_header_is_proved_before_its_plan_is_used() {
        let frozen = novarocks::FrozenFragment {
            plan_version: vec![1; 16],
            plan_contract_revision: 1,
            fragment_contract_version: u32::from(FragmentContractVersion::CURRENT.get()),
            pipeline_dop_domain: Some(novarocks::PipelineDopDomain {
                min: 1,
                max: 4,
                requires_power_of_two: true,
            }),
            plan: Some(plan::PlanFragment {
                sink: Some(plan::DataSink {
                    kind: Some(plan::data_sink::Kind::Result(true)),
                }),
                ..Default::default()
            }),
        };
        let bytes = FrozenBytes::freeze(frozen.encode_to_vec().into());
        let decoded = decode_static_fragment(&bytes, FieldPath::root("f")).expect("legal");
        assert_eq!(decoded.sink_kind(), FragmentSinkKind::Result);
        assert!(
            decoded
                .verify_task_dop(NonZeroUsize::new(2).expect("nonzero"), FieldPath::root("d"))
                .is_ok()
        );
        assert!(
            decoded
                .verify_task_dop(NonZeroUsize::new(3).expect("nonzero"), FieldPath::root("d"))
                .is_err(),
            "a power-of-two domain refuses three"
        );

        let mut no_sink = frozen.clone();
        no_sink.plan.as_mut().expect("plan").sink = None;
        assert!(
            decode_static_fragment(
                &FrozenBytes::freeze(no_sink.encode_to_vec().into()),
                FieldPath::root("f")
            )
            .is_err()
        );

        let mut zero_version = frozen;
        zero_version.plan_version = vec![0; 16];
        assert!(
            decode_static_fragment(
                &FrozenBytes::freeze(zero_version.encode_to_vec().into()),
                FieldPath::root("f")
            )
            .is_err()
        );
        assert!(
            decode_static_fragment(
                &FrozenBytes::freeze(Bytes::from_static(&[0x0a, 0x80])),
                FieldPath::root("f")
            )
            .is_err(),
            "malformed bytes are refused by the one protobuf interpreter"
        );
    }
}
