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

//! The immutable task descriptor: every protocol fact a task is created with.
//!
//! A descriptor is frozen exactly once, at creation, and compared for exact
//! equality on every replay. It owns the protocol facts natively — identity,
//! the complete push exchange topology, the derived kernel key, the split
//! plan nodes, and the shape of the fragment — and it owns the physical plan
//! through an immutable, codec-owned handle.
//!
//! That last part is a recorded compromise, not an oversight. The only
//! transport-neutral plan representation in this engine, `ExecPlan`, is not a
//! value: its scan, writer, and finish nodes hold `Arc<dyn ..>` leaves whose
//! only production implementors live in the backend, and it carries no serde.
//! So the frontend cannot author one. The plan therefore keeps the generated
//! message as its stored representation, private behind
//! [`PhysicalFragmentPlan`], which exposes only typed accessors. No business
//! owner on either side can reach or walk the generated payload, and this
//! crate still links no protobuf.

use std::fmt;
use std::num::{NonZeroU32, NonZeroUsize};
use std::sync::Arc;

use novarocks_types::UniqueId;
use novarocks_types::identity::BackendProcessId;

use crate::exec::chunk::ChunkSchemaRef;
use crate::exec::fragment::program::{FragmentContractVersion, FragmentNodeId, FragmentSinkKind};
use crate::exec::fragment::sink::DataStreamPartitionType;
use crate::runtime::endpoint::RuntimeEndpoint;
use crate::task_execution::domain::{
    CodecOwnedContent, ContentFingerprint, ExchangeEdgeId, PlanNodeId,
};
use crate::task_execution::identity::{IdentityMismatch, TaskIdentity};

/// The physical fragment plan of one task, owned by the central codec.
///
/// The neutral layer names this capability but never its representation. An
/// implementation privately holds whatever the wire needs and answers only
/// these typed questions, which is what keeps a generated message out of
/// every business owner's reach.
pub trait PhysicalFragmentPlan: CodecOwnedContent {
    /// The fragment contract version this plan was built against.
    fn contract_version(&self) -> FragmentContractVersion;

    /// What this fragment's sink does.
    fn sink_kind(&self) -> FragmentSinkKind;
}

/// One frozen destination of one push exchange edge.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ExchangeDestination {
    task: TaskIdentity,
    endpoint: RuntimeEndpoint,
    destination_node_id: FragmentNodeId,
    sender_ordinal: u32,
    sender_count: NonZeroU32,
}

impl ExchangeDestination {
    pub fn try_new(
        task: TaskIdentity,
        endpoint: RuntimeEndpoint,
        destination_node_id: FragmentNodeId,
        sender_ordinal: u32,
        sender_count: NonZeroU32,
    ) -> Result<Self, DescriptorError> {
        if sender_ordinal >= sender_count.get() {
            return Err(DescriptorError::SenderOrdinalOutOfRange {
                ordinal: sender_ordinal,
                count: sender_count.get(),
            });
        }
        Ok(Self {
            task,
            endpoint,
            destination_node_id,
            sender_ordinal,
            sender_count,
        })
    }

    pub const fn task(&self) -> TaskIdentity {
        self.task
    }

    pub const fn endpoint(&self) -> &RuntimeEndpoint {
        &self.endpoint
    }

    pub const fn destination_node_id(&self) -> FragmentNodeId {
        self.destination_node_id
    }

    pub const fn sender_ordinal(&self) -> u32 {
        self.sender_ordinal
    }

    pub const fn sender_count(&self) -> NonZeroU32 {
        self.sender_count
    }
}

/// One outbound push exchange edge of a producer task.
///
/// The destination set is frozen here and nowhere else. Opening an edge grants
/// send permission; it never adds a destination, and this release has no
/// operation that could.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ExchangeEdge {
    edge_id: ExchangeEdgeId,
    destination_node_id: FragmentNodeId,
    partitioning: DataStreamPartitionType,
    destinations: Vec<ExchangeDestination>,
}

impl ExchangeEdge {
    pub fn try_new(
        edge_id: ExchangeEdgeId,
        destination_node_id: FragmentNodeId,
        partitioning: DataStreamPartitionType,
        destinations: Vec<ExchangeDestination>,
    ) -> Result<Self, DescriptorError> {
        if destinations.is_empty() {
            return Err(DescriptorError::EdgeWithoutDestinations(edge_id));
        }
        for destination in &destinations {
            if destination.destination_node_id() != destination_node_id {
                return Err(DescriptorError::EdgeDestinationNodeMismatch(edge_id));
            }
        }
        Ok(Self {
            edge_id,
            destination_node_id,
            partitioning,
            destinations,
        })
    }

    pub const fn edge_id(&self) -> ExchangeEdgeId {
        self.edge_id
    }

    pub const fn destination_node_id(&self) -> FragmentNodeId {
        self.destination_node_id
    }

    pub const fn partitioning(&self) -> DataStreamPartitionType {
        self.partitioning
    }

    pub fn destinations(&self) -> &[ExchangeDestination] {
        &self.destinations
    }
}

/// One inbound exchange node of a consumer task.
///
/// The source set and the expected sender count are frozen together, so an
/// inbound frame can be checked against the topology before anything decodes
/// an Arrow payload or allocates a receiver.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ExchangeInbound {
    node_id: FragmentNodeId,
    sources: Vec<TaskIdentity>,
    expected_schema: ChunkSchemaRef,
}

impl ExchangeInbound {
    pub fn try_new(
        node_id: FragmentNodeId,
        sources: Vec<TaskIdentity>,
        expected_schema: ChunkSchemaRef,
    ) -> Result<Self, DescriptorError> {
        if sources.is_empty() {
            return Err(DescriptorError::InboundWithoutSources(node_id));
        }
        let mut sorted = sources.clone();
        sorted.sort_unstable();
        sorted.dedup();
        if sorted.len() != sources.len() {
            return Err(DescriptorError::DuplicateInboundSource(node_id));
        }
        Ok(Self {
            node_id,
            sources,
            expected_schema,
        })
    }

    pub const fn node_id(&self) -> FragmentNodeId {
        self.node_id
    }

    pub fn sources(&self) -> &[TaskIdentity] {
        &self.sources
    }

    /// How many senders this node waits for, which is exactly the size of the
    /// frozen source set.
    pub fn expected_sender_count(&self) -> NonZeroU32 {
        NonZeroU32::new(self.sources.len() as u32).expect("a validated inbound has sources")
    }

    pub const fn expected_schema(&self) -> &ChunkSchemaRef {
        &self.expected_schema
    }

    pub fn accepts_source(&self, source: TaskIdentity) -> bool {
        self.sources.contains(&source)
    }
}

/// The complete, frozen push exchange topology of one task.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct ExchangeTopology {
    outbound: Vec<ExchangeEdge>,
    inbound: Vec<ExchangeInbound>,
}

impl ExchangeTopology {
    pub fn try_new(
        outbound: Vec<ExchangeEdge>,
        inbound: Vec<ExchangeInbound>,
    ) -> Result<Self, DescriptorError> {
        let mut edge_ids: Vec<ExchangeEdgeId> =
            outbound.iter().map(ExchangeEdge::edge_id).collect();
        edge_ids.sort_unstable();
        let unique = edge_ids.len();
        edge_ids.dedup();
        if edge_ids.len() != unique {
            return Err(DescriptorError::DuplicateEdgeId);
        }
        let mut node_ids: Vec<FragmentNodeId> =
            inbound.iter().map(ExchangeInbound::node_id).collect();
        node_ids.sort_unstable();
        let unique = node_ids.len();
        node_ids.dedup();
        if node_ids.len() != unique {
            return Err(DescriptorError::DuplicateInboundNode);
        }
        Ok(Self { outbound, inbound })
    }

    pub fn outbound(&self) -> &[ExchangeEdge] {
        &self.outbound
    }

    pub fn inbound(&self) -> &[ExchangeInbound] {
        &self.inbound
    }

    /// Every edge this task produces on, all of which start closed.
    pub fn edge_ids(&self) -> impl Iterator<Item = ExchangeEdgeId> + '_ {
        self.outbound.iter().map(ExchangeEdge::edge_id)
    }

    pub fn edge(&self, edge_id: ExchangeEdgeId) -> Option<&ExchangeEdge> {
        self.outbound.iter().find(|edge| edge.edge_id() == edge_id)
    }

    pub fn inbound_node(&self, node_id: FragmentNodeId) -> Option<&ExchangeInbound> {
        self.inbound.iter().find(|node| node.node_id() == node_id)
    }
}

/// Why a descriptor is not a legal value.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum DescriptorError {
    SenderOrdinalOutOfRange {
        ordinal: u32,
        count: u32,
    },
    EdgeWithoutDestinations(ExchangeEdgeId),
    EdgeDestinationNodeMismatch(ExchangeEdgeId),
    InboundWithoutSources(FragmentNodeId),
    DuplicateInboundSource(FragmentNodeId),
    DuplicateEdgeId,
    DuplicateInboundNode,
    DuplicateSplitPlanNode(PlanNodeId),
    PlanTooLarge {
        limit: usize,
        actual: usize,
    },
    ContractVersionMismatch {
        descriptor: FragmentContractVersion,
        plan: FragmentContractVersion,
    },
}

impl fmt::Display for DescriptorError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::SenderOrdinalOutOfRange { ordinal, count } => write!(
                formatter,
                "sender ordinal {ordinal} is not below sender count {count}"
            ),
            Self::EdgeWithoutDestinations(edge) => {
                write!(formatter, "exchange edge {edge} has no destination")
            }
            Self::EdgeDestinationNodeMismatch(edge) => write!(
                formatter,
                "exchange edge {edge} has a destination on another node"
            ),
            Self::InboundWithoutSources(node) => {
                write!(formatter, "inbound exchange node {node:?} has no source")
            }
            Self::DuplicateInboundSource(node) => write!(
                formatter,
                "inbound exchange node {node:?} repeats a source task"
            ),
            Self::DuplicateEdgeId => formatter.write_str("topology repeats an exchange edge id"),
            Self::DuplicateInboundNode => {
                formatter.write_str("topology repeats an inbound exchange node")
            }
            Self::DuplicateSplitPlanNode(node) => {
                write!(formatter, "descriptor repeats split plan node {node}")
            }
            Self::PlanTooLarge { limit, actual } => write!(
                formatter,
                "encoded plan is {actual} bytes, limit is {limit}"
            ),
            Self::ContractVersionMismatch { descriptor, plan } => write!(
                formatter,
                "descriptor contract version {} does not match plan version {}",
                descriptor.get(),
                plan.get()
            ),
        }
    }
}

impl std::error::Error for DescriptorError {}

/// Why an inbound exchange frame is rejected.
///
/// Every one of these is decided from the frozen descriptor alone, before any
/// Arrow payload is decoded and before a receiver is allocated.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub enum IngressRejection {
    /// The frame addresses a different task or a replaced process.
    Identity(IdentityMismatch),
    /// The destination node is not an inbound exchange node of this task.
    UnknownDestinationNode(FragmentNodeId),
    /// The sending task is not in this node's frozen source set.
    SourceNotFrozen,
    /// The frame's sender count disagrees with the frozen source set.
    SenderCountMismatch { expected: u32, received: u32 },
    /// The frame's sender ordinal is not below the expected sender count.
    SenderOrdinalOutOfRange { ordinal: u32, expected: u32 },
}

impl fmt::Display for IngressRejection {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Identity(mismatch) => write!(formatter, "inbound frame {mismatch}"),
            Self::UnknownDestinationNode(node) => {
                write!(formatter, "inbound frame names unknown node {node:?}")
            }
            Self::SourceNotFrozen => {
                formatter.write_str("inbound frame comes from a task outside the frozen source set")
            }
            Self::SenderCountMismatch { expected, received } => write!(
                formatter,
                "inbound frame declares {received} senders, topology froze {expected}"
            ),
            Self::SenderOrdinalOutOfRange { ordinal, expected } => write!(
                formatter,
                "inbound frame sender ordinal {ordinal} is not below {expected}"
            ),
        }
    }
}

impl std::error::Error for IngressRejection {}

/// Largest encoded plan a descriptor may carry.
pub const TASK_DESCRIPTOR_MAX_PLAN_ENCODED_BYTES: usize = 16 * 1024 * 1024;

/// The immutable creation contract of one task.
#[derive(Clone, Debug)]
pub struct TaskDescriptor {
    identity: TaskIdentity,
    fragment_instance_id: UniqueId,
    contract_version: FragmentContractVersion,
    pipeline_dop: NonZeroUsize,
    split_plan_nodes: Vec<PlanNodeId>,
    topology: ExchangeTopology,
    plan: Arc<dyn PhysicalFragmentPlan>,
}

impl TaskDescriptor {
    /// Freezes a descriptor, rejecting every shape the protocol does not
    /// allow.
    ///
    /// `fragment_instance_id` is the execution kernel's local key. It is
    /// derived one-to-one from `identity` by the frontend and is never a
    /// second wire admission identity: nothing selects a backend, retries, or
    /// reaches a terminal state through it.
    pub fn try_new(
        identity: TaskIdentity,
        fragment_instance_id: UniqueId,
        pipeline_dop: NonZeroUsize,
        split_plan_nodes: Vec<PlanNodeId>,
        topology: ExchangeTopology,
        plan: Arc<dyn PhysicalFragmentPlan>,
    ) -> Result<Self, DescriptorError> {
        let mut sorted = split_plan_nodes.clone();
        sorted.sort_unstable();
        let unique = sorted.len();
        sorted.dedup();
        if sorted.len() != unique {
            let duplicate = split_plan_nodes
                .iter()
                .enumerate()
                .find(|(index, node)| split_plan_nodes[..*index].contains(node))
                .map(|(_, node)| *node)
                .expect("a duplicate exists");
            return Err(DescriptorError::DuplicateSplitPlanNode(duplicate));
        }
        let encoded_len = plan.encoded_len();
        if encoded_len > TASK_DESCRIPTOR_MAX_PLAN_ENCODED_BYTES {
            return Err(DescriptorError::PlanTooLarge {
                limit: TASK_DESCRIPTOR_MAX_PLAN_ENCODED_BYTES,
                actual: encoded_len,
            });
        }
        Ok(Self {
            identity,
            fragment_instance_id,
            contract_version: plan.contract_version(),
            pipeline_dop,
            split_plan_nodes,
            topology,
            plan,
        })
    }

    pub const fn identity(&self) -> TaskIdentity {
        self.identity
    }

    /// The execution kernel's local fragment instance key.
    pub const fn fragment_instance_id(&self) -> UniqueId {
        self.fragment_instance_id
    }

    pub const fn contract_version(&self) -> FragmentContractVersion {
        self.contract_version
    }

    pub const fn pipeline_dop(&self) -> NonZeroUsize {
        self.pipeline_dop
    }

    /// The plan nodes that accept split assignments. A split naming any other
    /// node is rejected as an unknown domain member.
    pub fn split_plan_nodes(&self) -> &[PlanNodeId] {
        &self.split_plan_nodes
    }

    pub fn accepts_split_plan_node(&self, node: PlanNodeId) -> bool {
        self.split_plan_nodes.contains(&node)
    }

    pub const fn topology(&self) -> &ExchangeTopology {
        &self.topology
    }

    pub fn sink_kind(&self) -> FragmentSinkKind {
        self.plan.sink_kind()
    }

    /// The physical plan, reachable only through its typed accessors.
    pub fn plan(&self) -> &Arc<dyn PhysicalFragmentPlan> {
        &self.plan
    }

    /// A secret-free content identity of the whole descriptor.
    ///
    /// This is what a create replay is compared on. It deliberately folds the
    /// plan's own fingerprint rather than any part of its representation.
    pub fn fingerprint(&self) -> ContentFingerprint {
        self.plan.fingerprint()
    }

    /// Whether an inbound exchange frame may be admitted.
    ///
    /// This runs before an Arrow payload is decoded and before a receiver is
    /// allocated. It does not claim to prevent the byte buffer the gRPC and
    /// protobuf receive layers have already allocated.
    pub fn authorize_inbound_frame(
        &self,
        destination: TaskIdentity,
        destination_node_id: FragmentNodeId,
        source: TaskIdentity,
        sender_ordinal: u32,
        sender_count: u32,
    ) -> Result<(), IngressRejection> {
        self.identity
            .verify_matches(destination)
            .map_err(IngressRejection::Identity)?;
        let node = self.topology.inbound_node(destination_node_id).ok_or(
            IngressRejection::UnknownDestinationNode(destination_node_id),
        )?;
        if !node.accepts_source(source) {
            return Err(IngressRejection::SourceNotFrozen);
        }
        let expected = node.expected_sender_count().get();
        if sender_count != expected {
            return Err(IngressRejection::SenderCountMismatch {
                expected,
                received: sender_count,
            });
        }
        if sender_ordinal >= expected {
            return Err(IngressRejection::SenderOrdinalOutOfRange {
                ordinal: sender_ordinal,
                expected,
            });
        }
        Ok(())
    }

    /// The backend process this task belongs to.
    pub const fn backend_process_id(&self) -> BackendProcessId {
        self.identity.backend_process_id()
    }
}

/// Descriptor equality is exact, and it compares the plan through its
/// fingerprint rather than its representation.
impl PartialEq for TaskDescriptor {
    fn eq(&self, other: &Self) -> bool {
        self.identity == other.identity
            && self.fragment_instance_id == other.fragment_instance_id
            && self.contract_version == other.contract_version
            && self.pipeline_dop == other.pipeline_dop
            && self.split_plan_nodes == other.split_plan_nodes
            && self.topology == other.topology
            && self.plan.fingerprint() == other.plan.fingerprint()
    }
}

impl Eq for TaskDescriptor {}

#[cfg(test)]
mod tests {
    use super::{
        DescriptorError, ExchangeDestination, ExchangeEdge, ExchangeInbound, ExchangeTopology,
        IngressRejection, PhysicalFragmentPlan, TASK_DESCRIPTOR_MAX_PLAN_ENCODED_BYTES,
        TaskDescriptor,
    };
    use crate::exec::chunk::{ChunkSchema, ChunkSchemaRef};
    use crate::exec::fragment::program::{
        FragmentContractVersion, FragmentNodeId, FragmentSinkKind,
    };
    use crate::exec::fragment::sink::DataStreamPartitionType;
    use crate::runtime::endpoint::RuntimeEndpoint;
    use crate::task_execution::domain::{
        CodecOwnedContent, ContentFingerprint, EdgeSendPermission, ExchangeEdgeDomain,
        ExchangeEdgeId, PlanNodeId,
    };
    use crate::task_execution::identity::{IdentityField, IdentityMismatch, TaskIdentity};
    use novarocks_types::UniqueId;
    use novarocks_types::identity::{
        AttemptId, BackendProcessId, QueryExecutionId, QueryId, StageId, TaskId,
    };
    use std::num::{NonZeroU32, NonZeroUsize};
    use std::sync::Arc;

    /// A stand-in for the codec-owned plan. It answers only the typed
    /// questions the neutral layer is allowed to ask.
    #[derive(Debug)]
    struct FakePlan {
        fingerprint: u8,
        encoded_len: usize,
    }

    impl FakePlan {
        fn arc(fingerprint: u8) -> Arc<dyn PhysicalFragmentPlan> {
            Arc::new(Self {
                fingerprint,
                encoded_len: 1024,
            })
        }

        fn oversized() -> Arc<dyn PhysicalFragmentPlan> {
            Arc::new(Self {
                fingerprint: 1,
                encoded_len: TASK_DESCRIPTOR_MAX_PLAN_ENCODED_BYTES + 1,
            })
        }
    }

    impl CodecOwnedContent for FakePlan {
        fn fingerprint(&self) -> ContentFingerprint {
            ContentFingerprint::from_bytes([self.fingerprint; 16])
        }

        fn encoded_len(&self) -> usize {
            self.encoded_len
        }
    }

    impl PhysicalFragmentPlan for FakePlan {
        fn contract_version(&self) -> FragmentContractVersion {
            FragmentContractVersion::CURRENT
        }

        fn sink_kind(&self) -> FragmentSinkKind {
            FragmentSinkKind::DataStream
        }
    }

    fn execution() -> QueryExecutionId {
        QueryExecutionId::new(QueryId::new(7, 9), AttemptId::new(1).expect("nonzero"))
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
        RuntimeEndpoint::new("127.0.0.1", 9060).expect("valid endpoint")
    }

    fn schema() -> ChunkSchemaRef {
        Arc::new(ChunkSchema::empty())
    }

    fn count(value: u32) -> NonZeroU32 {
        NonZeroU32::new(value).expect("nonzero")
    }

    fn edge_id(value: u32) -> ExchangeEdgeId {
        ExchangeEdgeId::new(value).expect("nonzero edge")
    }

    fn destination(
        task: TaskIdentity,
        node: i32,
        ordinal: u32,
        senders: u32,
    ) -> ExchangeDestination {
        ExchangeDestination::try_new(
            task,
            endpoint(),
            FragmentNodeId::new(node),
            ordinal,
            count(senders),
        )
        .expect("legal destination")
    }

    fn descriptor(
        identity: TaskIdentity,
        inbound: Vec<ExchangeInbound>,
        outbound: Vec<ExchangeEdge>,
        plan_fingerprint: u8,
    ) -> TaskDescriptor {
        TaskDescriptor::try_new(
            identity,
            UniqueId::new(1, 2),
            NonZeroUsize::new(4).expect("nonzero dop"),
            vec![PlanNodeId::new(3).expect("nonnegative")],
            ExchangeTopology::try_new(outbound, inbound).expect("legal topology"),
            FakePlan::arc(plan_fingerprint),
        )
        .expect("legal descriptor")
    }

    #[test]
    fn a_destination_ordinal_must_be_below_its_sender_count() {
        assert_eq!(
            ExchangeDestination::try_new(
                task(1, 1, BackendProcessId::new_v7()),
                endpoint(),
                FragmentNodeId::new(10),
                3,
                count(3)
            ),
            Err(DescriptorError::SenderOrdinalOutOfRange {
                ordinal: 3,
                count: 3
            })
        );
        assert!(
            ExchangeDestination::try_new(
                task(1, 1, BackendProcessId::new_v7()),
                endpoint(),
                FragmentNodeId::new(10),
                2,
                count(3)
            )
            .is_ok()
        );
    }

    #[test]
    fn an_edge_needs_destinations_that_all_sit_on_its_node() {
        let backend = BackendProcessId::new_v7();
        assert_eq!(
            ExchangeEdge::try_new(
                edge_id(1),
                FragmentNodeId::new(10),
                DataStreamPartitionType::HashPartitioned,
                Vec::new()
            ),
            Err(DescriptorError::EdgeWithoutDestinations(edge_id(1)))
        );
        assert_eq!(
            ExchangeEdge::try_new(
                edge_id(1),
                FragmentNodeId::new(10),
                DataStreamPartitionType::HashPartitioned,
                vec![destination(task(2, 1, backend), 11, 0, 1)]
            ),
            Err(DescriptorError::EdgeDestinationNodeMismatch(edge_id(1)))
        );
        let edge = ExchangeEdge::try_new(
            edge_id(1),
            FragmentNodeId::new(10),
            DataStreamPartitionType::HashPartitioned,
            vec![
                destination(task(2, 1, backend), 10, 0, 2),
                destination(task(2, 2, backend), 10, 1, 2),
            ],
        )
        .expect("legal edge");
        assert_eq!(edge.destinations().len(), 2);
        assert_eq!(
            edge.partitioning(),
            DataStreamPartitionType::HashPartitioned
        );
    }

    #[test]
    fn an_inbound_nodes_sender_count_is_exactly_its_frozen_source_set() {
        let backend = BackendProcessId::new_v7();
        assert_eq!(
            ExchangeInbound::try_new(FragmentNodeId::new(10), Vec::new(), schema()),
            Err(DescriptorError::InboundWithoutSources(FragmentNodeId::new(
                10
            )))
        );
        let repeated = task(1, 1, backend);
        assert_eq!(
            ExchangeInbound::try_new(FragmentNodeId::new(10), vec![repeated, repeated], schema()),
            Err(DescriptorError::DuplicateInboundSource(
                FragmentNodeId::new(10)
            ))
        );
        let inbound = ExchangeInbound::try_new(
            FragmentNodeId::new(10),
            vec![task(1, 1, backend), task(1, 2, backend)],
            schema(),
        )
        .expect("legal inbound");
        assert_eq!(inbound.expected_sender_count(), count(2));
        assert!(inbound.accepts_source(task(1, 2, backend)));
        assert!(!inbound.accepts_source(task(1, 3, backend)));
    }

    #[test]
    fn topology_rejects_duplicate_edges_and_duplicate_inbound_nodes() {
        let backend = BackendProcessId::new_v7();
        let edge = |id: u32| {
            ExchangeEdge::try_new(
                edge_id(id),
                FragmentNodeId::new(10),
                DataStreamPartitionType::Random,
                vec![destination(task(2, 1, backend), 10, 0, 1)],
            )
            .expect("legal edge")
        };
        assert_eq!(
            ExchangeTopology::try_new(vec![edge(1), edge(1)], Vec::new()),
            Err(DescriptorError::DuplicateEdgeId)
        );

        let inbound = || {
            ExchangeInbound::try_new(FragmentNodeId::new(20), vec![task(1, 1, backend)], schema())
                .expect("legal inbound")
        };
        assert_eq!(
            ExchangeTopology::try_new(Vec::new(), vec![inbound(), inbound()]),
            Err(DescriptorError::DuplicateInboundNode)
        );

        let topology = ExchangeTopology::try_new(vec![edge(1), edge(2)], vec![inbound()])
            .expect("legal topology");
        assert_eq!(topology.edge_ids().count(), 2);
        assert!(topology.edge(edge_id(2)).is_some());
        assert!(topology.edge(edge_id(3)).is_none());
        assert!(topology.inbound_node(FragmentNodeId::new(20)).is_some());
        assert!(topology.inbound_node(FragmentNodeId::new(21)).is_none());
    }

    #[test]
    fn every_edge_the_descriptor_freezes_starts_closed() {
        let backend = BackendProcessId::new_v7();
        let outbound = vec![
            ExchangeEdge::try_new(
                edge_id(1),
                FragmentNodeId::new(10),
                DataStreamPartitionType::Random,
                vec![destination(task(2, 1, backend), 10, 0, 1)],
            )
            .expect("legal edge"),
            ExchangeEdge::try_new(
                edge_id(2),
                FragmentNodeId::new(11),
                DataStreamPartitionType::Unpartitioned,
                vec![destination(task(3, 1, backend), 11, 0, 1)],
            )
            .expect("legal edge"),
        ];
        let descriptor = descriptor(task(1, 1, backend), Vec::new(), outbound, 1);
        let edges = ExchangeEdgeDomain::from_frozen_edges(descriptor.topology().edge_ids());
        assert_eq!(
            edges.permission(edge_id(1)),
            Some(EdgeSendPermission::Closed)
        );
        assert_eq!(
            edges.permission(edge_id(2)),
            Some(EdgeSendPermission::Closed)
        );
        assert!(!edges.all_open());
    }

    #[test]
    fn a_descriptor_rejects_duplicate_split_nodes_and_an_oversized_plan() {
        let identity = task(1, 1, BackendProcessId::new_v7());
        let node = PlanNodeId::new(3).expect("nonnegative");
        assert_eq!(
            TaskDescriptor::try_new(
                identity,
                UniqueId::new(1, 2),
                NonZeroUsize::new(1).expect("nonzero"),
                vec![node, node],
                ExchangeTopology::default(),
                FakePlan::arc(1),
            ),
            Err(DescriptorError::DuplicateSplitPlanNode(node))
        );
        assert_eq!(
            TaskDescriptor::try_new(
                identity,
                UniqueId::new(1, 2),
                NonZeroUsize::new(1).expect("nonzero"),
                Vec::new(),
                ExchangeTopology::default(),
                FakePlan::oversized(),
            ),
            Err(DescriptorError::PlanTooLarge {
                limit: TASK_DESCRIPTOR_MAX_PLAN_ENCODED_BYTES,
                actual: TASK_DESCRIPTOR_MAX_PLAN_ENCODED_BYTES + 1,
            })
        );
    }

    #[test]
    fn descriptor_equality_compares_the_plan_by_fingerprint_only() {
        let backend = BackendProcessId::new_v7();
        let identity = task(1, 1, backend);
        let first = descriptor(identity, Vec::new(), Vec::new(), 7);
        let same = descriptor(identity, Vec::new(), Vec::new(), 7);
        let different_plan = descriptor(identity, Vec::new(), Vec::new(), 8);

        assert_eq!(first, same, "an exact replay must compare equal");
        assert_ne!(
            first, different_plan,
            "a different plan must be a create conflict"
        );

        let other_task = descriptor(task(1, 2, backend), Vec::new(), Vec::new(), 7);
        assert_ne!(first, other_task);
    }

    #[test]
    fn the_descriptor_exposes_only_typed_plan_facts() {
        let descriptor = descriptor(
            task(1, 1, BackendProcessId::new_v7()),
            Vec::new(),
            Vec::new(),
            3,
        );
        assert_eq!(
            descriptor.contract_version(),
            FragmentContractVersion::CURRENT
        );
        assert_eq!(descriptor.sink_kind(), FragmentSinkKind::DataStream);
        assert_eq!(
            descriptor.fingerprint(),
            ContentFingerprint::from_bytes([3; 16])
        );
        assert_eq!(descriptor.plan().encoded_len(), 1024);
        assert_eq!(descriptor.fragment_instance_id(), UniqueId::new(1, 2));
        assert_eq!(descriptor.pipeline_dop().get(), 4);
        assert!(descriptor.accepts_split_plan_node(PlanNodeId::new(3).expect("nonnegative")));
        assert!(!descriptor.accepts_split_plan_node(PlanNodeId::new(4).expect("nonnegative")));
    }

    #[test]
    fn inbound_admission_checks_every_frozen_fact_before_any_decode() {
        let backend = BackendProcessId::new_v7();
        let identity = task(9, 1, backend);
        let source_a = task(1, 1, backend);
        let source_b = task(1, 2, backend);
        let inbound =
            ExchangeInbound::try_new(FragmentNodeId::new(20), vec![source_a, source_b], schema())
                .expect("legal inbound");
        let descriptor = descriptor(identity, vec![inbound], Vec::new(), 1);

        assert_eq!(
            descriptor.authorize_inbound_frame(identity, FragmentNodeId::new(20), source_a, 0, 2),
            Ok(())
        );

        // Wrong destination task or replaced process.
        assert_eq!(
            descriptor.authorize_inbound_frame(
                task(9, 2, backend),
                FragmentNodeId::new(20),
                source_a,
                0,
                2
            ),
            Err(IngressRejection::Identity(IdentityMismatch::new(
                IdentityField::Task
            )))
        );
        assert_eq!(
            descriptor.authorize_inbound_frame(
                task(9, 1, BackendProcessId::new_v7()),
                FragmentNodeId::new(20),
                source_a,
                0,
                2
            ),
            Err(IngressRejection::Identity(IdentityMismatch::new(
                IdentityField::BackendProcess
            )))
        );

        // Unknown node.
        assert_eq!(
            descriptor.authorize_inbound_frame(identity, FragmentNodeId::new(21), source_a, 0, 2),
            Err(IngressRejection::UnknownDestinationNode(
                FragmentNodeId::new(21)
            ))
        );

        // A sender outside the frozen source set.
        assert_eq!(
            descriptor.authorize_inbound_frame(
                identity,
                FragmentNodeId::new(20),
                task(1, 3, backend),
                0,
                2
            ),
            Err(IngressRejection::SourceNotFrozen)
        );

        // Sender count and ordinal must match the frozen set.
        assert_eq!(
            descriptor.authorize_inbound_frame(identity, FragmentNodeId::new(20), source_a, 0, 3),
            Err(IngressRejection::SenderCountMismatch {
                expected: 2,
                received: 3
            })
        );
        assert_eq!(
            descriptor.authorize_inbound_frame(identity, FragmentNodeId::new(20), source_b, 2, 2),
            Err(IngressRejection::SenderOrdinalOutOfRange {
                ordinal: 2,
                expected: 2
            })
        );
    }

    #[test]
    fn a_task_with_no_inbound_node_rejects_every_frame() {
        let backend = BackendProcessId::new_v7();
        let identity = task(1, 1, backend);
        let descriptor = descriptor(identity, Vec::new(), Vec::new(), 1);
        assert_eq!(
            descriptor.authorize_inbound_frame(
                identity,
                FragmentNodeId::new(20),
                task(2, 1, backend),
                0,
                1
            ),
            Err(IngressRejection::UnknownDestinationNode(
                FragmentNodeId::new(20)
            ))
        );
    }
}
