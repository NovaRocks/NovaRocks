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

//! The task descriptor: the protocol facts a running task is addressed by.
//!
//! A descriptor owns its facts natively — identity, the complete push exchange
//! topology, the derived kernel key, the split plan nodes, and the task's
//! parallelism. It deliberately holds no physical plan. The static plan
//! travels as the immutable bytes its fragment was encoded to once, and only
//! the backend that wins a task's creation decodes it; a descriptor is what
//! remains addressable after that, for the task's whole life.
//!
//! A create replay is decided by the descriptor's identity alone, never by
//! comparing descriptors. Equality below is an ordinary exact comparison of
//! typed facts, for owners that need to prove two values are the same; it is
//! not a creation contract.

use std::fmt;
use std::net::SocketAddr;
use std::num::{NonZeroU32, NonZeroUsize};
use std::str::FromStr;

use novarocks_types::identity::BackendProcessId;
use novarocks_types::{NativeEndpoint, UniqueId};

use crate::task_execution::domain::{ExchangeEdgeId, PlanNodeId};
use crate::task_execution::identity::TaskIdentity;

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct FragmentContractVersion(u16);

impl FragmentContractVersion {
    pub const CURRENT: Self = Self(1);

    pub const fn new(value: u16) -> Self {
        Self(value)
    }

    pub const fn get(self) -> u16 {
        self.0
    }
}

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct FragmentNodeId(i32);

impl FragmentNodeId {
    pub const fn new(value: i32) -> Self {
        Self(value)
    }

    pub const fn get(self) -> i32 {
        self.0
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum FragmentSinkKind {
    Result,
    Noop,
    DataStream,
    MultiCastDataStream,
    SplitDataStream,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DataStreamPartitionType {
    Unpartitioned,
    Random,
    HashPartitioned,
    BucketShuffleHashPartitioned,
}

impl DataStreamPartitionType {
    pub const fn display_name(self) -> &'static str {
        match self {
            Self::Unpartitioned => "UNPARTITIONED",
            Self::Random => "RANDOM",
            Self::HashPartitioned => "HASH_PARTITIONED",
            Self::BucketShuffleHashPartitioned => "BUCKET_SHUFFLE_HASH_PARTITIONED",
        }
    }

    pub const fn requires_exprs(self) -> bool {
        matches!(
            self,
            Self::HashPartitioned | Self::BucketShuffleHashPartitioned
        )
    }
}

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct RuntimeEndpoint {
    endpoint: NativeEndpoint,
}

impl fmt::Display for RuntimeEndpoint {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.endpoint.fmt(formatter)
    }
}

impl FromStr for RuntimeEndpoint {
    type Err = String;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        Self::parse(value)
    }
}

impl RuntimeEndpoint {
    pub fn new(host: impl Into<String>, port: i32) -> Result<Self, String> {
        let host = host.into();
        if !(1..=i32::from(u16::MAX)).contains(&port) {
            return Err(format!(
                "native runtime endpoint port {port} must be in 1..={}",
                u16::MAX
            ));
        }
        Ok(Self {
            endpoint: NativeEndpoint::from_host_port(&host, port as u16)?,
        })
    }

    pub fn host(&self) -> &str {
        self.endpoint.host()
    }

    pub fn retained_host_capacity(&self) -> usize {
        self.endpoint.host_capacity()
    }

    pub fn port(&self) -> i32 {
        i32::from(self.endpoint.port())
    }

    pub fn from_socket_addr(addr: SocketAddr) -> Self {
        Self {
            endpoint: NativeEndpoint::from_socket_addr(addr),
        }
    }

    pub fn parse(src: &str) -> Result<Self, String> {
        let endpoint = src
            .parse::<NativeEndpoint>()
            .map_err(|error| format!("native runtime endpoint is invalid: {error}"))?;
        Ok(Self { endpoint })
    }

    pub fn as_host_port(&self) -> String {
        self.endpoint.as_host_port()
    }

    pub fn native_endpoint(&self) -> &NativeEndpoint {
        &self.endpoint
    }
}

/// One frozen destination of one push exchange edge.
///
/// It carries both addresses on purpose. The task identity is the protocol
/// fence; the fragment instance id is the execution kernel's key, and it is
/// what an actual exchange frame carries. Freezing the one-to-one mapping here
/// is what makes the two provably the same target rather than two
/// independently derived guesses.
///
/// The producer's sender position is not here. Every destination of one edge
/// counts the producer at the same position, so that fact belongs to the
/// [`ExchangeEdge`] and exists once.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ExchangeDestination {
    task: TaskIdentity,
    task_key: UniqueId,
    endpoint: RuntimeEndpoint,
    destination_node_id: FragmentNodeId,
}

impl ExchangeDestination {
    pub const fn new(
        task: TaskIdentity,
        task_key: UniqueId,
        endpoint: RuntimeEndpoint,
        destination_node_id: FragmentNodeId,
    ) -> Self {
        Self {
            task,
            task_key,
            endpoint,
            destination_node_id,
        }
    }

    /// The execution kernel's key for this destination.
    pub const fn task_key(&self) -> UniqueId {
        self.task_key
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
}

/// One outbound push exchange edge of a producer task.
///
/// The destination set is frozen here and nowhere else. Opening an edge grants
/// send permission; it never adds a destination, and this release has no
/// operation that could.
///
/// The sender position is the producer's exact place in the complete inbound
/// producer union of the target exchange node, and the count is that union's
/// size. The union spans every edge feeding the node, so the count is not the
/// number of producers on this edge, and it is one value for every
/// destination because they all count the same producers.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ExchangeEdge {
    edge_id: ExchangeEdgeId,
    destination_node_id: FragmentNodeId,
    partitioning: DataStreamPartitionType,
    destinations: Vec<ExchangeDestination>,
    sender_ordinal: u32,
    sender_count: NonZeroU32,
}

impl ExchangeEdge {
    pub fn try_new(
        edge_id: ExchangeEdgeId,
        destination_node_id: FragmentNodeId,
        partitioning: DataStreamPartitionType,
        destinations: Vec<ExchangeDestination>,
        sender_ordinal: u32,
        sender_count: NonZeroU32,
    ) -> Result<Self, DescriptorError> {
        if sender_ordinal >= sender_count.get() {
            return Err(DescriptorError::SenderOrdinalOutOfRange {
                ordinal: sender_ordinal,
                count: sender_count.get(),
            });
        }
        if destinations.is_empty() {
            return Err(DescriptorError::EdgeWithoutDestinations(edge_id));
        }
        for destination in &destinations {
            if destination.destination_node_id() != destination_node_id {
                return Err(DescriptorError::EdgeDestinationNodeMismatch(edge_id));
            }
        }
        // A destination set is a set, on both addresses. A repeated task would
        // double-count senders on this edge, and a repeated kernel key would
        // make a send ambiguous. This mirrors what an inbound node already
        // requires of its source set.
        let mut by_task: Vec<TaskIdentity> = destinations
            .iter()
            .map(|destination| destination.task())
            .collect();
        by_task.sort_unstable();
        by_task.dedup();
        let mut by_key: Vec<UniqueId> = destinations
            .iter()
            .map(|destination| destination.task_key())
            .collect();
        by_key.sort_unstable();
        by_key.dedup();
        if by_task.len() != destinations.len() || by_key.len() != destinations.len() {
            return Err(DescriptorError::DuplicateEdgeDestination(edge_id));
        }
        Ok(Self {
            edge_id,
            destination_node_id,
            partitioning,
            destinations,
            sender_ordinal,
            sender_count,
        })
    }

    pub const fn edge_id(&self) -> ExchangeEdgeId {
        self.edge_id
    }

    /// This producer's position in the target node's complete producer union.
    pub const fn sender_ordinal(&self) -> u32 {
        self.sender_ordinal
    }

    /// The size of the target node's complete producer union.
    pub const fn sender_count(&self) -> NonZeroU32 {
        self.sender_count
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

/// One frozen source of one inbound exchange node.
///
/// An inbound frame identifies its sender by the kernel key, so the frozen set
/// has to be searchable by that key. Carrying the task identity alongside it
/// is what turns "a frame from some instance" into "a frame from exactly this
/// task on exactly this process".
#[derive(Copy, Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct ExchangeSource {
    task: TaskIdentity,
    task_key: UniqueId,
    sender_ordinal: u32,
}

impl ExchangeSource {
    pub const fn new(task: TaskIdentity, task_key: UniqueId, sender_ordinal: u32) -> Self {
        Self {
            task,
            task_key,
            sender_ordinal,
        }
    }

    pub const fn task(self) -> TaskIdentity {
        self.task
    }

    pub const fn task_key(self) -> UniqueId {
        self.task_key
    }

    pub const fn sender_ordinal(self) -> u32 {
        self.sender_ordinal
    }
}

/// One inbound exchange node of a consumer task.
///
/// The source set and the expected sender count are frozen together, so an
/// inbound frame can be checked against the topology before anything decodes
/// an Arrow payload or allocates a receiver.
///
/// The expected chunk schema is deliberately absent. It is a property of the
/// plan, and the receiver registry already refuses a mismatched registration;
/// duplicating it here would create a second authority over the same fact
/// without adding a check the pre-decode capability is allowed to make.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ExchangeInbound {
    node_id: FragmentNodeId,
    sources: Vec<ExchangeSource>,
    delivery: crate::ExchangeDeliveryRequirement,
    position_shard_source_dops: Option<Vec<NonZeroU32>>,
}

impl ExchangeInbound {
    pub fn try_new(
        node_id: FragmentNodeId,
        sources: Vec<ExchangeSource>,
        delivery: crate::ExchangeDeliveryRequirement,
    ) -> Result<Self, DescriptorError> {
        if sources.is_empty() {
            return Err(DescriptorError::InboundWithoutSources(node_id));
        }
        let mut by_task: Vec<TaskIdentity> = sources.iter().map(|source| source.task()).collect();
        by_task.sort_unstable();
        by_task.dedup();
        let mut by_key: Vec<UniqueId> = sources.iter().map(|source| source.task_key()).collect();
        by_key.sort_unstable();
        by_key.dedup();
        // Both addresses must be unique: two tasks sharing one kernel key
        // would make a frame ambiguous, and one task appearing twice would
        // inflate the expected sender count.
        if by_task.len() != sources.len() || by_key.len() != sources.len() {
            return Err(DescriptorError::DuplicateInboundSource(node_id));
        }
        let mut ordinals = sources
            .iter()
            .map(|source| source.sender_ordinal())
            .collect::<Vec<_>>();
        ordinals.sort_unstable();
        let expected = (0..sources.len() as u32).collect::<Vec<_>>();
        if ordinals != expected {
            return Err(DescriptorError::InvalidInboundSenderOrdinals {
                node: node_id,
                expected,
                received: ordinals,
            });
        }
        Ok(Self {
            node_id,
            sources,
            delivery,
            position_shard_source_dops: None,
        })
    }

    pub fn try_with_position_shard_source_dops(
        mut self,
        dops: Vec<NonZeroU32>,
    ) -> Result<Self, DescriptorError> {
        if self.delivery != crate::ExchangeDeliveryRequirement::MustDrain
            || dops.len() != self.sources.len()
            || self.position_shard_source_dops.is_some()
        {
            return Err(DescriptorError::InvalidPositionShardSourceDops(
                self.node_id,
            ));
        }
        self.position_shard_source_dops = Some(dops);
        Ok(self)
    }

    /// Actual producer widths indexed by the frozen sender ordinal.
    pub fn position_shard_source_dops(&self) -> Option<&[NonZeroU32]> {
        self.position_shard_source_dops.as_deref()
    }

    pub const fn node_id(&self) -> FragmentNodeId {
        self.node_id
    }

    pub const fn delivery(&self) -> crate::ExchangeDeliveryRequirement {
        self.delivery
    }

    pub fn sources(&self) -> &[ExchangeSource] {
        &self.sources
    }

    /// How many senders this node waits for, which is exactly the size of the
    /// frozen source set.
    pub fn expected_sender_count(&self) -> NonZeroU32 {
        NonZeroU32::new(self.sources.len() as u32).expect("a validated inbound has sources")
    }

    pub fn accepts_source(&self, source: TaskIdentity) -> bool {
        self.sources.iter().any(|frozen| frozen.task() == source)
    }

    /// Resolves the frozen source that an inbound frame's kernel key names.
    pub fn source_by_kernel_key(&self, key: UniqueId) -> Option<ExchangeSource> {
        self.sources
            .iter()
            .copied()
            .find(|frozen| frozen.task_key() == key)
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
    DuplicateEdgeDestination(ExchangeEdgeId),
    InboundWithoutSources(FragmentNodeId),
    DuplicateInboundSource(FragmentNodeId),
    InvalidInboundSenderOrdinals {
        node: FragmentNodeId,
        expected: Vec<u32>,
        received: Vec<u32>,
    },
    InvalidPositionShardSourceDops(FragmentNodeId),
    DuplicateEdgeId,
    DuplicateInboundNode,
    DuplicateSplitPlanNode(PlanNodeId),
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
            Self::DuplicateEdgeDestination(edge) => {
                write!(formatter, "exchange edge {edge} repeats a destination")
            }
            Self::InboundWithoutSources(node) => {
                write!(formatter, "inbound exchange node {node:?} has no source")
            }
            Self::DuplicateInboundSource(node) => write!(
                formatter,
                "inbound exchange node {node:?} repeats a source task"
            ),
            Self::InvalidInboundSenderOrdinals {
                node,
                expected,
                received,
            } => write!(
                formatter,
                "inbound exchange node {node:?} sender ordinals must be contiguous {expected:?}, received {received:?}"
            ),
            Self::InvalidPositionShardSourceDops(node) => write!(
                formatter,
                "inbound exchange node {node:?} requires a single MustDrain position-shard DOP roster for its full sender set"
            ),
            Self::DuplicateEdgeId => formatter.write_str("topology repeats an exchange edge id"),
            Self::DuplicateInboundNode => {
                formatter.write_str("topology repeats an inbound exchange node")
            }
            Self::DuplicateSplitPlanNode(node) => {
                write!(formatter, "descriptor repeats split plan node {node}")
            }
        }
    }
}

impl std::error::Error for DescriptorError {}

impl crate::failure::FailureProjection for DescriptorError {
    fn failure_fact(&self) -> crate::failure::FailureFact {
        use crate::failure::{FailureClass, FailureFact, FailureReason, domain};
        let code = match self {
            Self::SenderOrdinalOutOfRange { .. } => 1020,
            Self::EdgeWithoutDestinations(_) => 1021,
            Self::EdgeDestinationNodeMismatch(_) => 1022,
            Self::DuplicateEdgeDestination(_) => 1023,
            Self::InboundWithoutSources(_) => 1024,
            Self::DuplicateInboundSource(_) => 1025,
            Self::InvalidInboundSenderOrdinals { .. } => 1026,
            Self::DuplicateEdgeId => 1027,
            Self::DuplicateInboundNode => 1028,
            Self::DuplicateSplitPlanNode(_) => 1029,
            Self::InvalidPositionShardSourceDops(_) => 1030,
        };
        FailureFact::new(
            FailureClass::Invariant,
            FailureReason::new(domain::PROTOCOL, std::num::NonZeroU16::new(code).unwrap()),
        )
    }
}

/// The protocol facts one task is addressed by.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TaskDescriptor {
    identity: TaskIdentity,
    task_key: UniqueId,
    pipeline_dop: NonZeroUsize,
    split_plan_nodes: Vec<PlanNodeId>,
    topology: ExchangeTopology,
}

impl TaskDescriptor {
    /// Builds a descriptor, rejecting every shape the protocol does not
    /// allow.
    ///
    /// `task_key` is the execution kernel's local key. It is
    /// derived one-to-one from `identity` by the frontend and is never a
    /// second wire admission identity: nothing selects a backend, retries, or
    /// reaches a terminal state through it.
    pub fn try_new(
        identity: TaskIdentity,
        task_key: UniqueId,
        pipeline_dop: NonZeroUsize,
        split_plan_nodes: Vec<PlanNodeId>,
        topology: ExchangeTopology,
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
        Ok(Self {
            identity,
            task_key,
            pipeline_dop,
            split_plan_nodes,
            topology,
        })
    }

    pub const fn identity(&self) -> TaskIdentity {
        self.identity
    }

    /// The execution kernel's local fragment instance key.
    pub const fn task_key(&self) -> UniqueId {
        self.task_key
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

    /// The backend process this task belongs to.
    pub const fn backend_process_id(&self) -> BackendProcessId {
        self.identity.backend_process_id()
    }
}

#[cfg(test)]
mod tests {
    use super::{
        DescriptorError, ExchangeDestination, ExchangeEdge, ExchangeInbound, ExchangeSource,
        ExchangeTopology, TaskDescriptor,
    };
    use crate::task_execution::domain::{ExchangeEdgeId, PlanNodeId};
    use crate::task_execution::identity::TaskIdentity;
    use crate::{DataStreamPartitionType, FragmentNodeId, RuntimeEndpoint};
    use novarocks_types::UniqueId;
    use novarocks_types::identity::{
        AttemptId, BackendProcessId, QueryExecutionId, QueryId, StageId, TaskId,
    };
    use std::num::{NonZeroU32, NonZeroUsize};

    const OWN_KEY: UniqueId = UniqueId::new(1, 2);

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

    /// The kernel key the frontend derives for one task. The mapping only has
    /// to be one-to-one, which a distinct value per task id satisfies.
    fn key(stage: u32, id: u32) -> UniqueId {
        UniqueId::new(i64::from(stage), i64::from(id))
    }

    fn endpoint() -> RuntimeEndpoint {
        RuntimeEndpoint::new("127.0.0.1", 9060).expect("valid endpoint")
    }

    fn count(value: u32) -> NonZeroU32 {
        NonZeroU32::new(value).expect("nonzero")
    }

    fn edge_id(value: u32) -> ExchangeEdgeId {
        ExchangeEdgeId::new(value).expect("nonzero edge")
    }

    fn destination(target: TaskIdentity, node: i32) -> ExchangeDestination {
        ExchangeDestination::new(
            target,
            key(target.stage_id().get(), target.task_id().get()),
            endpoint(),
            FragmentNodeId::new(node),
        )
    }

    fn edge(
        id: u32,
        node: i32,
        destinations: Vec<ExchangeDestination>,
        ordinal: u32,
        senders: u32,
    ) -> Result<ExchangeEdge, DescriptorError> {
        ExchangeEdge::try_new(
            edge_id(id),
            FragmentNodeId::new(node),
            DataStreamPartitionType::HashPartitioned,
            destinations,
            ordinal,
            count(senders),
        )
    }

    fn source(from: TaskIdentity) -> ExchangeSource {
        ExchangeSource::new(
            from,
            key(from.stage_id().get(), from.task_id().get()),
            from.task_id().get() - 1,
        )
    }

    fn descriptor(
        identity: TaskIdentity,
        inbound: Vec<ExchangeInbound>,
        outbound: Vec<ExchangeEdge>,
    ) -> TaskDescriptor {
        TaskDescriptor::try_new(
            identity,
            OWN_KEY,
            NonZeroUsize::new(4).expect("nonzero dop"),
            vec![PlanNodeId::new(3).expect("nonnegative")],
            ExchangeTopology::try_new(outbound, inbound).expect("legal topology"),
        )
        .expect("legal descriptor")
    }

    #[test]
    fn an_edges_sender_ordinal_must_be_below_its_sender_count() {
        let backend = BackendProcessId::new_v7();
        assert_eq!(
            edge(1, 10, vec![destination(task(2, 1, backend), 10)], 3, 3),
            Err(DescriptorError::SenderOrdinalOutOfRange {
                ordinal: 3,
                count: 3
            })
        );
        let legal =
            edge(1, 10, vec![destination(task(2, 1, backend), 10)], 2, 3).expect("legal edge");
        assert_eq!(legal.sender_ordinal(), 2);
        assert_eq!(legal.sender_count(), count(3));
    }

    /// The sender count is the target node's complete producer union, which
    /// several edges share. An edge carrying one destination may therefore
    /// count more senders than it has producers, and every destination of an
    /// edge reads the same position.
    #[test]
    fn an_edges_sender_position_is_shared_by_every_destination() {
        let backend = BackendProcessId::new_v7();
        let edge = edge(
            4,
            10,
            vec![
                destination(task(2, 1, backend), 10),
                destination(task(2, 2, backend), 10),
                destination(task(2, 3, backend), 10),
            ],
            5,
            7,
        )
        .expect("legal edge");
        assert_eq!(edge.destinations().len(), 3);
        assert_eq!((edge.sender_ordinal(), edge.sender_count()), (5, count(7)));
    }

    #[test]
    fn an_edge_needs_destinations_that_all_sit_on_its_node() {
        let backend = BackendProcessId::new_v7();
        assert_eq!(
            edge(1, 10, Vec::new(), 0, 1),
            Err(DescriptorError::EdgeWithoutDestinations(edge_id(1)))
        );
        assert_eq!(
            edge(1, 10, vec![destination(task(2, 1, backend), 11)], 0, 1),
            Err(DescriptorError::EdgeDestinationNodeMismatch(edge_id(1)))
        );
        let edge = edge(
            1,
            10,
            vec![
                destination(task(2, 1, backend), 10),
                destination(task(2, 2, backend), 10),
            ],
            0,
            1,
        )
        .expect("legal edge");
        assert_eq!(edge.destinations().len(), 2);
        assert_eq!(
            edge.partitioning(),
            DataStreamPartitionType::HashPartitioned
        );
    }

    #[test]
    fn an_edges_destination_set_is_a_set_on_both_addresses() {
        let backend = BackendProcessId::new_v7();
        let target = task(2, 1, backend);
        let repeated = destination(target, 10);
        assert_eq!(
            edge(1, 10, vec![repeated.clone(), repeated], 0, 1),
            Err(DescriptorError::DuplicateEdgeDestination(edge_id(1))),
            "a repeated destination would double-count senders on this edge"
        );

        // Two distinct tasks may not share one kernel key either: a send
        // naming that key would be ambiguous.
        let shared_key = ExchangeDestination::new(
            task(2, 2, backend),
            key(2, 1),
            endpoint(),
            FragmentNodeId::new(10),
        );
        assert_eq!(
            edge(1, 10, vec![destination(target, 10), shared_key], 0, 1),
            Err(DescriptorError::DuplicateEdgeDestination(edge_id(1)))
        );

        assert!(
            edge(
                1,
                10,
                vec![
                    destination(task(2, 1, backend), 10),
                    destination(task(2, 2, backend), 10),
                ],
                0,
                1,
            )
            .is_ok(),
            "two distinct destinations remain legal"
        );
    }

    #[test]
    fn an_inbound_nodes_sender_count_is_exactly_its_frozen_source_set() {
        let backend = BackendProcessId::new_v7();
        assert_eq!(
            ExchangeInbound::try_new(
                FragmentNodeId::new(10),
                Vec::new(),
                crate::ExchangeDeliveryRequirement::DemandDriven
            ),
            Err(DescriptorError::InboundWithoutSources(FragmentNodeId::new(
                10
            )))
        );
        let repeated = source(task(1, 1, backend));
        assert_eq!(
            ExchangeInbound::try_new(
                FragmentNodeId::new(10),
                vec![repeated, repeated],
                crate::ExchangeDeliveryRequirement::DemandDriven
            ),
            Err(DescriptorError::DuplicateInboundSource(
                FragmentNodeId::new(10)
            ))
        );
        // Two distinct tasks may not share one kernel key either: a frame
        // naming that key would be ambiguous.
        assert_eq!(
            ExchangeInbound::try_new(
                FragmentNodeId::new(10),
                vec![
                    ExchangeSource::new(task(1, 1, backend), key(9, 9), 0),
                    ExchangeSource::new(task(1, 2, backend), key(9, 9), 1),
                ],
                crate::ExchangeDeliveryRequirement::DemandDriven,
            ),
            Err(DescriptorError::DuplicateInboundSource(
                FragmentNodeId::new(10)
            ))
        );
        let inbound = ExchangeInbound::try_new(
            FragmentNodeId::new(10),
            vec![source(task(1, 1, backend)), source(task(1, 2, backend))],
            crate::ExchangeDeliveryRequirement::DemandDriven,
        )
        .expect("legal inbound");
        assert_eq!(inbound.expected_sender_count(), count(2));
        assert!(inbound.accepts_source(task(1, 2, backend)));
        assert!(!inbound.accepts_source(task(1, 3, backend)));
        assert_eq!(
            inbound.source_by_kernel_key(key(1, 2)).map(|s| s.task()),
            Some(task(1, 2, backend))
        );
        assert_eq!(inbound.source_by_kernel_key(key(4, 4)), None);

        for (invalid, received) in [
            (
                vec![
                    ExchangeSource::new(task(1, 1, backend), key(1, 1), 0),
                    ExchangeSource::new(task(1, 2, backend), key(1, 2), 0),
                ],
                vec![0, 0],
            ),
            (
                vec![
                    ExchangeSource::new(task(1, 1, backend), key(1, 1), 0),
                    ExchangeSource::new(task(1, 2, backend), key(1, 2), 2),
                ],
                vec![0, 2],
            ),
        ] {
            assert_eq!(
                ExchangeInbound::try_new(
                    FragmentNodeId::new(11),
                    invalid,
                    crate::ExchangeDeliveryRequirement::DemandDriven
                ),
                Err(DescriptorError::InvalidInboundSenderOrdinals {
                    node: FragmentNodeId::new(11),
                    expected: vec![0, 1],
                    received,
                })
            );
        }
    }

    #[test]
    fn topology_rejects_duplicate_edges_and_duplicate_inbound_nodes() {
        let backend = BackendProcessId::new_v7();
        let random_edge = |id: u32| {
            ExchangeEdge::try_new(
                edge_id(id),
                FragmentNodeId::new(10),
                DataStreamPartitionType::Random,
                vec![destination(task(2, 1, backend), 10)],
                0,
                count(1),
            )
            .expect("legal edge")
        };
        assert_eq!(
            ExchangeTopology::try_new(vec![random_edge(1), random_edge(1)], Vec::new()),
            Err(DescriptorError::DuplicateEdgeId)
        );

        let inbound = || {
            ExchangeInbound::try_new(
                FragmentNodeId::new(20),
                vec![source(task(1, 1, backend))],
                crate::ExchangeDeliveryRequirement::DemandDriven,
            )
            .expect("legal inbound")
        };
        assert_eq!(
            ExchangeTopology::try_new(Vec::new(), vec![inbound(), inbound()]),
            Err(DescriptorError::DuplicateInboundNode)
        );

        let topology =
            ExchangeTopology::try_new(vec![random_edge(1), random_edge(2)], vec![inbound()])
                .expect("legal topology");
        assert_eq!(topology.edge_ids().count(), 2);
        assert!(topology.edge(edge_id(2)).is_some());
        assert!(topology.edge(edge_id(3)).is_none());
        assert!(topology.inbound_node(FragmentNodeId::new(20)).is_some());
        assert!(topology.inbound_node(FragmentNodeId::new(21)).is_none());
    }

    #[test]
    fn descriptor_holds_the_exact_outbound_edge_facts() {
        let backend = BackendProcessId::new_v7();
        let outbound = vec![
            ExchangeEdge::try_new(
                edge_id(1),
                FragmentNodeId::new(10),
                DataStreamPartitionType::Random,
                vec![destination(task(2, 1, backend), 10)],
                0,
                count(1),
            )
            .expect("legal edge"),
            ExchangeEdge::try_new(
                edge_id(2),
                FragmentNodeId::new(11),
                DataStreamPartitionType::Unpartitioned,
                vec![destination(task(3, 1, backend), 11)],
                1,
                count(2),
            )
            .expect("legal edge"),
        ];
        let descriptor = descriptor(task(1, 1, backend), Vec::new(), outbound);
        let edges = descriptor.topology().outbound();
        assert_eq!(edges.len(), 2);
        assert_eq!(edges[0].edge_id(), edge_id(1));
        assert_eq!(edges[0].destination_node_id(), FragmentNodeId::new(10));
        assert_eq!(edges[0].partitioning(), DataStreamPartitionType::Random);
        assert_eq!(edges[0].destinations().len(), 1);
        assert_eq!(edges[0].destinations()[0].task(), task(2, 1, backend));
        assert_eq!(edges[1].edge_id(), edge_id(2));
        assert_eq!(edges[1].destination_node_id(), FragmentNodeId::new(11));
        assert_eq!(
            edges[1].partitioning(),
            DataStreamPartitionType::Unpartitioned
        );
        assert_eq!(edges[1].destinations().len(), 1);
        assert_eq!(edges[1].destinations()[0].task(), task(3, 1, backend));
        assert_eq!(
            (edges[1].sender_ordinal(), edges[1].sender_count()),
            (1, count(2))
        );
    }

    #[test]
    fn a_descriptor_rejects_duplicate_split_nodes() {
        let identity = task(1, 1, BackendProcessId::new_v7());
        let node = PlanNodeId::new(3).expect("nonnegative");
        assert_eq!(
            TaskDescriptor::try_new(
                identity,
                OWN_KEY,
                NonZeroUsize::new(1).expect("nonzero"),
                vec![node, node],
                ExchangeTopology::default(),
            ),
            Err(DescriptorError::DuplicateSplitPlanNode(node))
        );
    }

    #[test]
    fn descriptor_equality_is_an_exact_comparison_of_typed_facts() {
        let backend = BackendProcessId::new_v7();
        let identity = task(9, 1, backend);
        let source_a = task(1, 1, backend);
        let source_b = task(1, 2, backend);
        let inbound = |first: u32, second: u32| {
            vec![
                ExchangeInbound::try_new(
                    FragmentNodeId::new(20),
                    vec![
                        ExchangeSource::new(source_a, key(1, 1), first),
                        ExchangeSource::new(source_b, key(1, 2), second),
                    ],
                    crate::ExchangeDeliveryRequirement::DemandDriven,
                )
                .expect("legal inbound"),
            ]
        };
        let first = descriptor(identity, inbound(0, 1), Vec::new());
        assert_eq!(first, descriptor(identity, inbound(0, 1), Vec::new()));
        assert_ne!(
            first,
            descriptor(identity, inbound(1, 0), Vec::new()),
            "a different source-to-ordinal ownership is a different value"
        );
        assert_ne!(
            first,
            descriptor(task(9, 2, backend), inbound(0, 1), Vec::new())
        );
    }

    #[test]
    fn the_descriptor_exposes_its_typed_facts() {
        let descriptor = descriptor(
            task(1, 1, BackendProcessId::new_v7()),
            Vec::new(),
            Vec::new(),
        );
        assert_eq!(descriptor.task_key(), OWN_KEY);
        assert_eq!(descriptor.pipeline_dop().get(), 4);
        assert!(descriptor.accepts_split_plan_node(PlanNodeId::new(3).expect("nonnegative")));
        assert!(!descriptor.accepts_split_plan_node(PlanNodeId::new(4).expect("nonnegative")));
    }
}

#[cfg(test)]
mod descriptor_failure_projection_tests {
    use super::*;
    use crate::failure::{FailureClass, FailureProjection, domain};
    #[test]
    fn invalid_sender_geometry_is_a_descriptor_invariant_not_capacity() {
        for (ordinal, count) in [(0, 0), (1, 1), (u32::MAX, u32::MAX)] {
            let fact = DescriptorError::SenderOrdinalOutOfRange { ordinal, count }.failure_fact();
            assert_eq!(fact.class(), FailureClass::Invariant);
            assert_eq!(fact.reason().domain(), domain::PROTOCOL.get());
            assert_eq!(fact.reason().code(), 1020);
            assert_eq!(fact.resource(), None);
        }
    }
}
