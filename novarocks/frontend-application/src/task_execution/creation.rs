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

//! One task's creation, from its move-only inputs to the two frozen carriers
//! its create request sends.
//!
//! The exchange membership of an attempt is held once, per logical edge, in
//! [`AttemptTopology`]; a task holds only the ids of the edges it produces on
//! and consumes from. A task's creation metadata -- the one place that
//! membership is spelled out per task -- does not exist until the task is
//! first admitted to a send queue. At that point its [`TaskCreationSeed`] is
//! consumed and encoded exactly once into [`FrozenCreationParts`], and every
//! later send of that create, including a resend after an unknown outcome,
//! clones the same two shared byte handles.
//!
//! The encoded length is learned earlier, once, because admission has to
//! price a request before it is allowed to exist. Nothing here compares or
//! digests a creation's content: the backend recognises a replay by the task
//! identity it names, and what keeps a replay's content unchanged is that
//! this owner can only ever hand out the parts it froze.
// Design: ADR-0158 (docs/adr/ADR-0158-task-creation-is-frozen-once-and-replayed-by-identity.md)

use std::collections::BTreeMap;
use std::num::{NonZeroU32, NonZeroUsize};
use std::sync::Arc;

use novarocks_execution::exec::fragment::program::FragmentNodeId;
use novarocks_execution::exec::fragment::sink::DataStreamPartitionType;
use novarocks_execution::runtime::endpoint::RuntimeEndpoint;
use novarocks_execution::task_execution::{
    ExchangeDestination, ExchangeEdge, ExchangeEdgeId, ExchangeInbound, ExchangeSource,
    ExchangeTopology, FrozenBytes, OperationEnvelope, PlanNodeId, QueryContextRef, TaskDescriptor,
    TaskIdentity,
};
use novarocks_proto_codec::lifecycle::ScanRangeParams;
use novarocks_proto_models::novarocks as proto;
use novarocks_task_codec::creation::{create_task_operation_encoded_len, encode_creation_metadata};
use novarocks_types::UniqueId;
use prost::Message;

use super::error::TaskExecutionError;
use super::intent::OPERATION_FIXED_BYTES;
use crate::native::fragment_encoder::frozen::FragmentArtifact;

/// One producer of a shared exchange edge.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct EdgeProducer {
    task: TaskIdentity,
    kernel_key: UniqueId,
    sender_ordinal: u32,
}

impl EdgeProducer {
    pub(crate) const fn new(task: TaskIdentity, kernel_key: UniqueId, sender_ordinal: u32) -> Self {
        Self {
            task,
            kernel_key,
            sender_ordinal,
        }
    }
}

/// One destination of a shared exchange edge.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct EdgeDestination {
    task: TaskIdentity,
    kernel_key: UniqueId,
    endpoint: RuntimeEndpoint,
}

impl EdgeDestination {
    pub(crate) const fn new(
        task: TaskIdentity,
        kernel_key: UniqueId,
        endpoint: RuntimeEndpoint,
    ) -> Self {
        Self {
            task,
            kernel_key,
            endpoint,
        }
    }
}

/// One logical exchange edge of an attempt, with its complete membership
/// held once.
///
/// The sender count is the size of the target node's complete producer union,
/// which may span several edges; each producer's ordinal is its position in
/// that union. Every destination counts a producer at the same position, which
/// is why the ordinal belongs to the producer and never to a destination.
#[derive(Clone, Debug)]
pub(crate) struct SharedEdge {
    edge_id: ExchangeEdgeId,
    destination_node_id: FragmentNodeId,
    partitioning: DataStreamPartitionType,
    delivery: novarocks_execution_contract::ExchangeDeliveryRequirement,
    sender_count: NonZeroU32,
    producers: Box<[EdgeProducer]>,
    destinations: Box<[EdgeDestination]>,
}

impl SharedEdge {
    pub(crate) fn new(
        edge_id: ExchangeEdgeId,
        destination_node_id: FragmentNodeId,
        partitioning: DataStreamPartitionType,
        delivery: novarocks_execution_contract::ExchangeDeliveryRequirement,
        sender_count: NonZeroU32,
        producers: Vec<EdgeProducer>,
        destinations: Vec<EdgeDestination>,
    ) -> Self {
        Self {
            edge_id,
            destination_node_id,
            partitioning,
            delivery,
            sender_count,
            producers: producers.into_boxed_slice(),
            destinations: destinations.into_boxed_slice(),
        }
    }

    #[cfg(test)]
    pub(crate) const fn edge_id(&self) -> ExchangeEdgeId {
        self.edge_id
    }

    /// This edge as one of its producers freezes it.
    fn for_producer(&self, producer: TaskIdentity) -> Result<ExchangeEdge, TaskExecutionError> {
        let ordinal = self
            .producers
            .iter()
            .find(|candidate| candidate.task == producer)
            .map(|candidate| candidate.sender_ordinal)
            .ok_or_else(|| {
                TaskExecutionError::Schedule(format!(
                    "task {producer} is not a producer of exchange edge {}",
                    self.edge_id
                ))
            })?;
        let destinations = self
            .destinations
            .iter()
            .map(|destination| {
                ExchangeDestination::new(
                    destination.task,
                    destination.kernel_key,
                    destination.endpoint.clone(),
                    self.destination_node_id,
                )
            })
            .collect();
        Ok(ExchangeEdge::try_new(
            self.edge_id,
            self.destination_node_id,
            self.partitioning,
            destinations,
            ordinal,
            self.sender_count,
        )?)
    }
}

/// The frozen exchange membership of one attempt.
///
/// It is built and validated once, shared by every task creation of the
/// attempt, and dropped when the last creation that needs it is frozen. The
/// per-task topology that a creation's wire metadata spells out is derived
/// from it on demand, one task at a time, and never retained.
#[derive(Debug)]
pub(crate) struct AttemptTopology {
    edges: BTreeMap<ExchangeEdgeId, SharedEdge>,
}

impl AttemptTopology {
    /// Validates every edge once and every distinct inbound source set once.
    ///
    /// Each check here is the same one the per-task topology would run, so a
    /// membership that could not be frozen for some task is refused before
    /// any task is created from it, not when that task is first sent.
    pub(crate) fn try_new(
        edges: Vec<SharedEdge>,
        views: &[&TaskTopologyView],
    ) -> Result<Self, TaskExecutionError> {
        let mut by_id = BTreeMap::new();
        for edge in edges {
            if let Some(producer) = edge.producers.first() {
                // One producer is enough to prove the destination set and the
                // ordinal are legal: every producer shares both except the
                // ordinal, which is checked per producer below.
                edge.for_producer(producer.task)?;
            } else {
                return Err(TaskExecutionError::Schedule(format!(
                    "exchange edge {} has no producer",
                    edge.edge_id
                )));
            }
            for producer in edge.producers.iter() {
                if producer.sender_ordinal >= edge.sender_count.get() {
                    return Err(TaskExecutionError::Schedule(format!(
                        "producer {} of exchange edge {} holds ordinal {} outside sender count {}",
                        producer.task, edge.edge_id, producer.sender_ordinal, edge.sender_count
                    )));
                }
            }
            if by_id.insert(edge.edge_id, edge).is_some() {
                return Err(TaskExecutionError::Schedule(
                    "attempt topology repeats an exchange edge".to_owned(),
                ));
            }
        }
        let topology = Self { edges: by_id };
        let mut validated = std::collections::BTreeSet::new();
        for view in views {
            for (node, edge_ids) in view.inbound.iter() {
                if validated.insert((*node, edge_ids.clone())) {
                    topology.inbound(*node, edge_ids)?;
                }
            }
        }
        Ok(topology)
    }

    fn edge(&self, edge_id: ExchangeEdgeId) -> Result<&SharedEdge, TaskExecutionError> {
        self.edges.get(&edge_id).ok_or_else(|| {
            TaskExecutionError::Schedule(format!("exchange edge {edge_id} is absent"))
        })
    }

    fn inbound(
        &self,
        node: FragmentNodeId,
        edge_ids: &[ExchangeEdgeId],
    ) -> Result<ExchangeInbound, TaskExecutionError> {
        let mut sources = Vec::new();
        let mut delivery = None;
        for edge_id in edge_ids {
            let edge = self.edge(*edge_id)?;
            if delivery.is_some_and(|frozen| frozen != edge.delivery) {
                return Err(TaskExecutionError::Schedule(format!(
                    "exchange input {} has conflicting delivery requirements",
                    node.get()
                )));
            }
            delivery = Some(edge.delivery);
            sources.extend(edge.producers.iter().map(|producer| {
                ExchangeSource::new(producer.task, producer.kernel_key, producer.sender_ordinal)
            }));
        }
        let delivery = delivery.ok_or_else(|| {
            TaskExecutionError::Schedule(format!(
                "exchange input {} has no producer edge",
                node.get()
            ))
        })?;
        Ok(ExchangeInbound::try_new(node, sources, delivery)?)
    }
}

/// Where one task sits in its attempt's shared topology: edge ids only.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub(crate) struct TaskTopologyView {
    outbound: Box<[ExchangeEdgeId]>,
    inbound: Box<[(FragmentNodeId, Box<[ExchangeEdgeId]>)]>,
}

impl TaskTopologyView {
    /// `inbound` names, per exchange node this task consumes, every edge that
    /// feeds that node.
    pub(crate) fn new(
        outbound: Vec<ExchangeEdgeId>,
        inbound: BTreeMap<FragmentNodeId, Vec<ExchangeEdgeId>>,
    ) -> Self {
        Self {
            outbound: outbound.into_boxed_slice(),
            inbound: inbound
                .into_iter()
                .map(|(node, edges)| (node, edges.into_boxed_slice()))
                .collect(),
        }
    }

    /// The edges this task produces on, which all start closed.
    pub(crate) fn outbound(&self) -> &[ExchangeEdgeId] {
        &self.outbound
    }
}

/// The encoded sizes a creation is priced by before it is frozen.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub(crate) struct CreationLengths {
    metadata: usize,
    assignment: usize,
}

#[derive(Clone, Copy, Debug)]
pub(crate) struct CreationSizing {
    pub(crate) lengths: CreationLengths,
    /// All newly requested descriptor/projection buffers plus the final
    /// encoded metadata buffer; original borrowed source backing is excluded.
    pub(crate) new_backing_bytes: usize,
}

/// The move-only inputs of one task's creation.
///
/// It owns this task's initial scan ranges and its place in the shared
/// topology. It is consumed by [`TaskCreationSeed::freeze`], after which only
/// the frozen parts remain and the ranges it carried are released.
pub(crate) struct TaskCreationSeed {
    topology: Arc<AttemptTopology>,
    view: TaskTopologyView,
    fragment: Arc<FragmentArtifact>,
    identity: TaskIdentity,
    context: QueryContextRef,
    kernel_key: UniqueId,
    pipeline_dop: NonZeroUsize,
    split_plan_nodes: Vec<PlanNodeId>,
    instance_ordinal: u32,
    initial_scan_ranges: BTreeMap<i32, Vec<ScanRangeParams>>,
    sink_edges: Arc<[ExchangeEdgeId]>,
}

impl std::fmt::Debug for TaskCreationSeed {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("TaskCreationSeed")
            .field("identity", &self.identity)
            .field("fragment", &self.fragment.facts().fragment_id())
            .field("initial_scan_nodes", &self.initial_scan_ranges.len())
            .finish()
    }
}

/// The per-task facts a seed is built from, besides its shared topology and
/// frozen fragment.
pub(crate) struct TaskCreationFacts {
    pub(crate) identity: TaskIdentity,
    pub(crate) context: QueryContextRef,
    pub(crate) kernel_key: UniqueId,
    pub(crate) pipeline_dop: NonZeroUsize,
    pub(crate) split_plan_nodes: Vec<PlanNodeId>,
    pub(crate) instance_ordinal: u32,
    pub(crate) initial_scan_ranges: BTreeMap<i32, Vec<ScanRangeParams>>,
}

impl TaskCreationSeed {
    /// Binds one task's facts to its shared topology and frozen fragment.
    ///
    /// `sink_edges` lists, in the fragment's static sink branch order, the
    /// edge each branch serves; it is shared by every task of the fragment.
    pub(crate) fn new(
        topology: Arc<AttemptTopology>,
        view: TaskTopologyView,
        fragment: Arc<FragmentArtifact>,
        sink_edges: Arc<[ExchangeEdgeId]>,
        facts: TaskCreationFacts,
    ) -> Self {
        Self {
            topology,
            view,
            fragment,
            identity: facts.identity,
            context: facts.context,
            kernel_key: facts.kernel_key,
            pipeline_dop: facts.pipeline_dop,
            split_plan_nodes: facts.split_plan_nodes,
            instance_ordinal: facts.instance_ordinal,
            initial_scan_ranges: facts.initial_scan_ranges,
            sink_edges,
        }
    }

    pub(crate) const fn identity(&self) -> TaskIdentity {
        self.identity
    }

    pub(crate) const fn context(&self) -> QueryContextRef {
        self.context
    }

    pub(crate) fn split_plan_nodes(&self) -> &[PlanNodeId] {
        &self.split_plan_nodes
    }

    /// The edges this task produces on, which all start closed.
    pub(crate) fn outbound_edges(&self) -> &[ExchangeEdgeId] {
        self.view.outbound()
    }

    pub(crate) const fn fragment(&self) -> &Arc<FragmentArtifact> {
        &self.fragment
    }

    /// This task's protocol descriptor, derived from the shared topology.
    pub(crate) fn descriptor(&self) -> Result<TaskDescriptor, TaskExecutionError> {
        let outbound = self
            .view
            .outbound
            .iter()
            .map(|edge_id| self.topology.edge(*edge_id)?.for_producer(self.identity))
            .collect::<Result<Vec<_>, _>>()?;
        let inbound = self
            .view
            .inbound
            .iter()
            .map(|(node, edge_ids)| self.topology.inbound(*node, edge_ids))
            .collect::<Result<Vec<_>, _>>()?;
        Ok(TaskDescriptor::try_new(
            self.identity,
            self.kernel_key,
            self.pipeline_dop,
            self.split_plan_nodes.clone(),
            ExchangeTopology::try_new(outbound, inbound)?,
        )?)
    }

    /// Prices this creation without freezing it.
    ///
    /// The metadata message is built once to be measured and dropped at once;
    /// only the lengths survive. The caller keeps them, so a creation that
    /// waits behind backpressure is never measured twice.
    pub(crate) fn lengths(&self) -> Result<CreationLengths, TaskExecutionError> {
        #[cfg(test)]
        tests::record_pricing();
        crate::metrics::task_creation::create_priced();
        self.size_without_projection().map(|sizing| sizing.lengths)
    }

    /// The same frozen facts as `freeze`, visited without descriptor/assignment
    /// DTOs or cloned ranges. This runs before a paid input or queue is taken.
    pub(crate) fn size_without_projection(&self) -> Result<CreationSizing, TaskExecutionError> {
        use novarocks_task_codec::creation::size as wire_size;
        use prost::encoding::encoded_len_varint;
        use std::alloc::Layout;
        let failure = || TaskExecutionError::Schedule("creation sizing overflow".into());
        let add = |a: usize, b: usize| a.checked_add(b).ok_or_else(failure);
        let mut topology_bytes = 0usize;
        let mut requested = 0usize;
        let mut add_array =
            |size: usize, align: usize, count: usize| -> Result<(), TaskExecutionError> {
                let element = Layout::from_size_align(size, align).map_err(|_| failure())?;
                let bytes = element.size().checked_mul(count).ok_or_else(failure)?;
                requested = add(requested, bytes)?;
                Ok(())
            };
        macro_rules! array {
            ($ty:ty,$count:expr) => {
                add_array(
                    std::mem::size_of::<$ty>(),
                    std::mem::align_of::<$ty>(),
                    $count,
                )?
            };
        }
        array!(ExchangeEdge, self.view.outbound.len());
        array!(proto::TaskExchangeEdge, self.view.outbound.len());
        array!(ExchangeInbound, self.view.inbound.len());
        array!(proto::TaskExchangeInbound, self.view.inbound.len());
        array!(PlanNodeId, self.split_plan_nodes.len());
        array!(i32, self.split_plan_nodes.len());
        // The original descriptor and generated descriptor coexist during
        // projection. Source ranges move into the final assignment; no deep
        // range clone is needed by pricing or freezing.
        let mut destination_count = 0usize;
        let mut source_count = 0usize;
        let mut host_bytes = 0usize;
        for edge_id in self.view.outbound.iter() {
            let edge = self.topology.edge(*edge_id)?;
            let ordinal = edge
                .producers
                .iter()
                .find(|p| p.task == self.identity)
                .map(|p| p.sender_ordinal)
                .ok_or_else(|| {
                    TaskExecutionError::Schedule("creation producer is absent".into())
                })?;
            let mut bytes = wire_size::edge_header(
                edge.edge_id.get(),
                edge.destination_node_id.get(),
                edge.partitioning,
                ordinal,
                edge.sender_count.get(),
            );
            destination_count = add(destination_count, edge.destinations.len())?;
            for destination in edge.destinations.iter() {
                host_bytes = add(host_bytes, destination.endpoint.host().len())?;
                let destination = wire_size::destination(
                    destination.task,
                    destination.kernel_key,
                    destination.endpoint.host(),
                    destination.endpoint.port() as u16,
                    edge.destination_node_id.get(),
                )
                .map_err(|_| failure())?;
                bytes = add(
                    bytes,
                    wire_size::message(4, destination).map_err(|_| failure())?,
                )?;
            }
            topology_bytes = add(
                topology_bytes,
                wire_size::message(1, bytes).map_err(|_| failure())?,
            )?;
        }
        for (node, edge_ids) in self.view.inbound.iter() {
            let mut bytes = 0usize;
            let mut delivery = None;
            for edge_id in edge_ids.iter() {
                let edge = self.topology.edge(*edge_id)?;
                if delivery.is_some_and(|d| d != edge.delivery) {
                    return Err(TaskExecutionError::Schedule(
                        "creation delivery requirements conflict".into(),
                    ));
                }
                delivery = Some(edge.delivery);
                source_count = add(source_count, edge.producers.len())?;
                for source in edge.producers.iter() {
                    let source =
                        wire_size::source(source.task, source.kernel_key, source.sender_ordinal)
                            .map_err(|_| failure())?;
                    bytes = add(bytes, wire_size::message(2, source).map_err(|_| failure())?)?;
                }
            }
            let delivery = delivery.ok_or_else(|| {
                TaskExecutionError::Schedule("creation input has no producer edge".into())
            })?;
            bytes = add(bytes, wire_size::inbound_header(node.get(), delivery))?;
            topology_bytes = add(
                topology_bytes,
                wire_size::message(2, bytes).map_err(|_| failure())?,
            )?;
        }
        array!(ExchangeDestination, destination_count);
        array!(proto::TaskExchangeDestination, destination_count);
        array!(ExchangeSource, source_count);
        array!(proto::TaskExchangeSource, source_count);
        // Every projected task identity has one original 16-byte process ID.
        // Three more belong to descriptor/context headers; only the actual
        // generated creation constructors allocate these original Vec values.
        array!(
            u8,
            add(add(destination_count, source_count)?, 3)?
                .checked_mul(16)
                .ok_or_else(failure)?
        );
        let split_bytes = self
            .split_plan_nodes
            .iter()
            .try_fold(0usize, |bytes, node| {
                add(bytes, encoded_len_varint(node.get() as u64))
            })?;
        let descriptor = wire_size::descriptor(
            self.identity,
            self.kernel_key,
            self.pipeline_dop.get(),
            split_bytes,
            topology_bytes,
        )
        .map_err(|_| failure())?;
        let mut assignment = wire_size::uint(1, self.instance_ordinal as u64);
        let mut range_count = 0usize;
        for (&node, ranges) in &self.initial_scan_ranges {
            range_count = add(range_count, ranges.len())?;
            let mut bytes = wire_size::int(1, node as i64);
            for range in ranges {
                bytes = add(
                    bytes,
                    wire_size::message(2, range.as_proto().encoded_len()).map_err(|_| failure())?,
                )?;
            }
            assignment = add(
                assignment,
                wire_size::message(2, bytes).map_err(|_| failure())?,
            )?;
        }
        let sink_bytes = self.sink_edges.iter().try_fold(0usize, |bytes, edge| {
            add(bytes, encoded_len_varint(edge.get() as u64))
        })?;
        assignment = add(
            assignment,
            wire_size::packed(3, sink_bytes).map_err(|_| failure())?,
        )?;
        let metadata =
            wire_size::metadata(self.context, descriptor, assignment).map_err(|_| failure())?;
        array!(proto::TaskScanRanges, self.initial_scan_ranges.len());
        array!(proto::ScanRangeParams, range_count);
        array!(u32, self.sink_edges.len());
        requested = add(requested, host_bytes.checked_mul(2).ok_or_else(failure)?)?;
        requested = add(requested, metadata)?;
        Ok(CreationSizing {
            lengths: CreationLengths {
                metadata,
                assignment,
            },
            new_backing_bytes: requested,
        })
    }

    /// Encodes this creation's metadata, once, consuming the seed.
    ///
    /// `priced` are the lengths admission reserved against. The frozen
    /// metadata must be exactly that long: a creation may not become larger
    /// than the reservation that admitted it.
    pub(crate) fn freeze(
        self,
        priced: CreationLengths,
    ) -> Result<Arc<FrozenCreationParts>, TaskExecutionError> {
        let descriptor = self.descriptor()?;
        let assignment = assignment_message(
            self.instance_ordinal,
            self.initial_scan_ranges.into_iter().map(|(node, ranges)| {
                (
                    node,
                    ranges
                        .into_iter()
                        .map(ScanRangeParams::into_proto)
                        .collect(),
                )
            }),
            &self.sink_edges,
        );
        let assignment_len = assignment.encoded_len();
        let metadata = encode_creation_metadata(self.context, &descriptor, assignment, Vec::new())
            .encode_to_vec();
        if metadata.len() != priced.metadata || assignment_len != priced.assignment {
            return Err(TaskExecutionError::Schedule(format!(
                "task {} froze {} metadata bytes and a {}-byte assignment but was admitted for {} and {}",
                self.identity,
                metadata.len(),
                assignment_len,
                priced.metadata,
                priced.assignment
            )));
        }
        #[cfg(test)]
        tests::record_metadata_freeze();
        let retained = crate::metrics::task_creation::create_frozen(metadata.len());
        Ok(Arc::new(FrozenCreationParts {
            fragment: self.fragment,
            metadata: FrozenBytes::freeze_encoded(metadata),
            assignment_len,
            _retained: retained,
        }))
    }
}

fn assignment_message(
    instance_ordinal: u32,
    initial_scan_ranges: impl Iterator<Item = (i32, Vec<proto::ScanRangeParams>)>,
    sink_edges: &[ExchangeEdgeId],
) -> proto::TaskAssignment {
    proto::TaskAssignment {
        instance_ordinal,
        // A BTreeMap iterates in ascending plan node order, which is the order
        // the codec requires.
        initial_scan_ranges: initial_scan_ranges
            .map(|(plan_node_id, ranges)| proto::TaskScanRanges {
                plan_node_id,
                ranges,
            })
            .collect(),
        sink_edge_ids: sink_edges.iter().map(|edge| edge.get()).collect(),
    }
}

/// A creation's two carriers, frozen once and shared by every send.
#[derive(Debug)]
pub(crate) struct FrozenCreationParts {
    fragment: Arc<FragmentArtifact>,
    metadata: FrozenBytes,
    assignment_len: usize,
    /// This creation's share of the retained creation-payload gauges,
    /// returned when the last send or owner holding these parts drops them.
    _retained: crate::metrics::task_creation::RetainedPayload,
}

impl FrozenCreationParts {
    pub(crate) const fn fragment(&self) -> &Arc<FragmentArtifact> {
        &self.fragment
    }

    pub(crate) const fn metadata(&self) -> &FrozenBytes {
        &self.metadata
    }

    pub(crate) fn lengths(&self) -> CreationLengths {
        CreationLengths {
            metadata: self.metadata.len(),
            assignment: self.assignment_len,
        }
    }
}

/// The plan-carrier bytes one creation holds: its fragment's static plan and
/// its task assignment, which the backend bounds together.
pub(crate) fn plan_carrier_bytes(fragment: &FragmentArtifact, lengths: CreationLengths) -> usize {
    fragment.content().len().saturating_add(lengths.assignment)
}

/// What one create operation occupies in a frontend queue.
///
/// The operation's own encoded length is exact and computed from the two
/// carrier lengths alone; the fixed allowance covers its framing inside a
/// batch, as it does for every other operation.
pub(crate) fn create_queued_bytes(
    envelope: OperationEnvelope,
    fragment: &FragmentArtifact,
    lengths: CreationLengths,
) -> usize {
    create_task_operation_encoded_len(envelope, fragment.content().len(), lengths.metadata)
        .saturating_add(OPERATION_FIXED_BYTES)
}

/// One create operation, ready to send.
///
/// Every send of one task's create holds the same parts. The envelope is the
/// only thing a send could change, and it is priced separately.
#[derive(Debug)]
struct CreateTaskBody {
    envelope: OperationEnvelope,
    identity: TaskIdentity,
    parts: Arc<FrozenCreationParts>,
}
/// The by-value handle drops the actual shared body/header before its paid
/// input lease. No grant is embedded in the allocation whose header it pays.
#[derive(Debug)]
pub(crate) struct CreateTaskIntent {
    original: Arc<CreateTaskBody>,
    retention: Option<novarocks_resource_contract::ExistingOwnerRetentionLease>,
}
impl Clone for CreateTaskIntent {
    fn clone(&self) -> Self {
        // Checked retain happens first; failure leaves the original unchanged.
        let retention = self.retention.as_ref().map(|lease| {
            lease
                .try_clone()
                .expect("original Native input alias count remains representable")
        });
        Self {
            original: Arc::clone(&self.original),
            retention,
        }
    }
}
impl CreateTaskIntent {
    pub(crate) fn new(
        envelope: OperationEnvelope,
        identity: TaskIdentity,
        parts: Arc<FrozenCreationParts>,
        retention: Option<novarocks_resource_contract::ExistingOwnerRetentionLease>,
    ) -> Self {
        Self {
            original: Arc::new(CreateTaskBody {
                envelope,
                identity,
                parts,
            }),
            retention,
        }
    }
    pub(crate) fn envelope(&self) -> OperationEnvelope {
        self.original.envelope
    }
    pub(crate) fn identity(&self) -> TaskIdentity {
        self.original.identity
    }
    pub(crate) fn parts(&self) -> &FrozenCreationParts {
        &self.original.parts
    }
    pub(crate) fn queued_bytes(&self) -> usize {
        create_queued_bytes(
            self.envelope(),
            &self.original.parts.fragment,
            self.original.parts.lengths(),
        )
    }
    pub(crate) fn plan_carrier_bytes(&self) -> usize {
        plan_carrier_bytes(&self.original.parts.fragment, self.original.parts.lengths())
    }
    #[cfg(test)]
    pub(crate) fn same_original(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.original, &other.original)
    }
    #[cfg(test)]
    pub(crate) fn original_strong_count(&self) -> usize {
        Arc::strong_count(&self.original)
    }
    #[cfg(test)]
    pub(crate) fn weak_original_lifetimes(
        &self,
    ) -> (
        std::sync::Weak<dyn std::any::Any + Send + Sync>,
        std::sync::Weak<dyn std::any::Any + Send + Sync>,
    ) {
        // Coercion clones only existing Arc counters. Both temporary strong
        // aliases leave before returning these non-owning test observers.
        let original: Arc<dyn std::any::Any + Send + Sync> = Arc::clone(&self.original) as _;
        let parts: Arc<dyn std::any::Any + Send + Sync> = Arc::clone(&self.original.parts) as _;
        (Arc::downgrade(&original), Arc::downgrade(&parts))
    }
    #[cfg(test)]
    pub(crate) fn same_parts(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.original.parts, &other.original.parts)
    }
}
/// The actual two Arc requested layouts created by freeze/replay ownership.
/// Their original borrowed source pointees are not cloned or charged here.
pub(crate) fn creation_owner_header_bytes() -> Result<usize, TaskExecutionError> {
    fn arc<T>() -> Result<usize, TaskExecutionError> {
        std::alloc::Layout::new::<[usize; 2]>()
            .extend(std::alloc::Layout::new::<T>())
            .map(|(layout, _)| layout.pad_to_align().size())
            .map_err(|_| {
                TaskExecutionError::Workload(
                    novarocks_workload_control::WorkError::ArithmeticOverflow,
                )
            })
    }
    arc::<CreateTaskBody>()?
        .checked_add(arc::<FrozenCreationParts>()?)
        .ok_or(TaskExecutionError::Workload(
            novarocks_workload_control::WorkError::ArithmeticOverflow,
        ))
}

#[cfg(test)]
pub(crate) mod tests {
    use std::cell::Cell;
    use std::collections::BTreeMap;
    use std::num::{NonZeroU32, NonZeroUsize};
    use std::sync::Arc;

    use novarocks_execution::exec::fragment::program::FragmentNodeId;
    use novarocks_execution::exec::fragment::sink::DataStreamPartitionType;
    use novarocks_execution::runtime::endpoint::RuntimeEndpoint;
    use novarocks_execution::task_execution::{
        ExchangeEdgeId, OperationEnvelope, OperationKind, QueryContextRef, TaskIdentity,
        TaskOperationId,
    };
    use novarocks_physical_plan::PipelineDopDomain;
    use novarocks_proto_models::novarocks as proto;
    use novarocks_task_codec::operation::encode_create_task;
    use novarocks_types::UniqueId;
    use novarocks_types::identity::{
        AttemptId, BackendProcessId, FrontendProcessId, QueryExecutionId, QueryId, StageId, TaskId,
    };
    use prost::Message;

    use super::*;
    use crate::native::fragment_encoder::frozen::FragmentArtifact;

    thread_local! {
        static METADATA_FREEZES: Cell<usize> = const { Cell::new(0) };
        static PRICINGS: Cell<usize> = const { Cell::new(0) };
    }

    pub(super) fn record_metadata_freeze() {
        METADATA_FREEZES.with(|count| count.set(count.get() + 1));
    }

    pub(super) fn record_pricing() {
        PRICINGS.with(|count| count.set(count.get() + 1));
    }

    /// How many creations this thread has priced, which is how many times a
    /// creation's metadata message was built only to be measured.
    pub(crate) fn pricings_on_this_thread() -> usize {
        PRICINGS.with(Cell::get)
    }

    /// How many creation metadata carriers this thread has frozen. Tests read
    /// the delta around the call they measure.
    pub(crate) fn metadata_freezes_on_this_thread() -> usize {
        METADATA_FREEZES.with(Cell::get)
    }

    fn execution() -> QueryExecutionId {
        QueryExecutionId::new(QueryId::new(3, 4), AttemptId::new(1).expect("nonzero"))
            .expect("nonzero query")
    }

    fn task(stage: u32, id: u32, backend: BackendProcessId) -> TaskIdentity {
        TaskIdentity::new(
            execution(),
            StageId::new(stage).expect("nonzero stage"),
            TaskId::new(id).expect("nonzero task"),
            backend,
        )
    }

    fn endpoint() -> RuntimeEndpoint {
        RuntimeEndpoint::new("127.0.0.1", 9060).expect("endpoint")
    }

    fn producer_artifact() -> Arc<FragmentArtifact> {
        crate::native::fragment_encoder::frozen::tests::frozen_package(
            1,
            0,
            [5; 16],
            PipelineDopDomain {
                min: 1,
                max: 4,
                requires_power_of_two: false,
            },
            Some((
                2,
                20,
                novarocks_execution_contract::ExchangeDeliveryRequirement::DemandDriven,
            )),
            0,
        )
    }

    /// Two producers of stage 1 feed three consumers of stage 2 on one edge.
    struct Fixture {
        topology: Arc<AttemptTopology>,
        producers: [TaskIdentity; 2],
        backend: BackendProcessId,
        frontend: FrontendProcessId,
    }

    fn fixture() -> Fixture {
        let backend = BackendProcessId::new_v7();
        let producers = [task(1, 1, backend), task(1, 2, backend)];
        let consumers = [
            task(2, 3, backend),
            task(2, 4, backend),
            task(2, 5, backend),
        ];
        let edge = SharedEdge::new(
            ExchangeEdgeId::new(1).expect("nonzero edge"),
            FragmentNodeId::new(20),
            DataStreamPartitionType::HashPartitioned,
            novarocks_execution_contract::ExchangeDeliveryRequirement::DemandDriven,
            NonZeroU32::new(2).expect("nonzero"),
            producers
                .iter()
                .enumerate()
                .map(|(ordinal, task)| {
                    EdgeProducer::new(*task, UniqueId::new(1, ordinal as i64), ordinal as u32)
                })
                .collect(),
            consumers
                .iter()
                .enumerate()
                .map(|(index, task)| {
                    EdgeDestination::new(*task, UniqueId::new(2, index as i64), endpoint())
                })
                .collect(),
        );
        let view = TaskTopologyView::new(vec![edge.edge_id()], BTreeMap::new());
        let topology =
            Arc::new(AttemptTopology::try_new(vec![edge], &[&view]).expect("a legal topology"));
        Fixture {
            topology,
            producers,
            backend,
            frontend: FrontendProcessId::new_v7(),
        }
    }

    #[test]
    fn inbound_preserves_must_drain_and_rejects_mixed_delivery() {
        let fixture = fixture();
        let mut edge = fixture.topology.edges.values().next().unwrap().clone();
        edge.delivery = novarocks_execution_contract::ExchangeDeliveryRequirement::MustDrain;
        let first_id = edge.edge_id;
        let node = edge.destination_node_id;
        let mut topology = AttemptTopology::try_new(vec![edge.clone()], &[]).unwrap();
        let input = topology.inbound(node, &[first_id]).unwrap();
        assert_eq!(
            input.delivery(),
            novarocks_execution_contract::ExchangeDeliveryRequirement::MustDrain
        );
        edge.edge_id = ExchangeEdgeId::new(2).unwrap();
        edge.delivery = novarocks_execution_contract::ExchangeDeliveryRequirement::DemandDriven;
        let second_id = edge.edge_id;
        topology.edges.insert(second_id, edge);
        let error = topology.inbound(node, &[first_id, second_id]).unwrap_err();
        assert!(
            error
                .to_string()
                .contains("conflicting delivery requirements")
        );
        assert!(topology.inbound(node, &[]).is_err());
    }

    fn scan_range() -> ScanRangeParams {
        ScanRangeParams::parse(proto::ScanRangeParams {
            range: Some(proto::ScanRange {
                kind: Some(proto::scan_range::Kind::File(proto::FileScanRange {
                    file_format: "PARQUET".to_owned(),
                    full_path: Some("s3://bucket/data.parquet".to_owned()),
                    ..Default::default()
                })),
            }),
            ..Default::default()
        })
        .expect("a legal scan range")
    }

    fn seed(fixture: &Fixture, producer: usize) -> TaskCreationSeed {
        let identity = fixture.producers[producer];
        TaskCreationSeed::new(
            Arc::clone(&fixture.topology),
            TaskTopologyView::new(
                vec![ExchangeEdgeId::new(1).expect("nonzero edge")],
                BTreeMap::new(),
            ),
            producer_artifact(),
            Arc::from(vec![ExchangeEdgeId::new(1).expect("nonzero edge")]),
            TaskCreationFacts {
                identity,
                context: QueryContextRef::new(execution(), fixture.frontend, fixture.backend),
                kernel_key: UniqueId::new(1, producer as i64),
                pipeline_dop: NonZeroUsize::new(2).expect("nonzero"),
                split_plan_nodes: Vec::new(),
                instance_ordinal: producer as u32,
                initial_scan_ranges: BTreeMap::from([(7, vec![scan_range()]), (9, Vec::new())]),
            },
        )
    }

    #[test]
    fn a_producers_descriptor_expands_the_shared_edge_at_its_own_position() {
        let fixture = fixture();
        let descriptor = seed(&fixture, 1).descriptor().expect("a legal descriptor");
        let [edge] = descriptor.topology().outbound() else {
            panic!("one outbound edge");
        };
        assert_eq!(edge.destinations().len(), 3);
        assert_eq!(
            (edge.sender_ordinal(), edge.sender_count().get()),
            (1, 2),
            "the second producer counts at ordinal one of two"
        );
    }

    #[test]
    fn freezing_encodes_the_priced_metadata_once_and_the_codec_decodes_it() {
        let fixture = fixture();
        let seed = seed(&fixture, 0);
        let fragment = Arc::clone(seed.fragment());
        let priced = seed.lengths().expect("priced");
        let before = metadata_freezes_on_this_thread();
        let parts = seed.freeze(priced).expect("frozen");
        assert_eq!(metadata_freezes_on_this_thread() - before, 1);
        assert_eq!(parts.lengths(), priced);
        assert!(
            parts
                .fragment()
                .content()
                .shares_backing_with(fragment.content()),
            "a creation reuses its fragment's frozen static bytes"
        );

        let metadata = proto::CreationMetadata::decode(parts.metadata().bytes().clone())
            .expect("frozen metadata decodes");
        let assignment = metadata.assignment.expect("assignment");
        assert_eq!(assignment.instance_ordinal, 0);
        assert_eq!(assignment.sink_edge_ids, vec![1]);
        assert_eq!(
            assignment
                .initial_scan_ranges
                .iter()
                .map(|node| (node.plan_node_id, node.ranges.len()))
                .collect::<Vec<_>>(),
            vec![(7, 1), (9, 0)],
            "an empty entry still names the node as this task's"
        );

        let envelope = OperationEnvelope::with_default_wait(
            TaskOperationId::new_v7(),
            OperationKind::CreateTask,
        );
        let operation = encode_create_task(envelope, parts.fragment().content(), parts.metadata());
        assert_eq!(
            create_queued_bytes(envelope, parts.fragment(), parts.lengths()),
            operation.encoded_len() + OPERATION_FIXED_BYTES,
            "the queued size is the exact operation size plus the fixed allowance"
        );
    }

    #[test]
    fn a_creation_may_not_outgrow_the_length_it_was_admitted_at() {
        let fixture = fixture();
        let seed = seed(&fixture, 0);
        let priced = seed.lengths().expect("priced");
        let understated = CreationLengths {
            metadata: priced.metadata - 1,
            assignment: priced.assignment,
        };
        assert!(
            seed.freeze(understated).is_err(),
            "a reservation smaller than the frozen metadata is refused"
        );
    }
}
