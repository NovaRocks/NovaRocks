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

//! Task descriptor codec, including the wire-backed physical fragment plan.
//!
//! [`WireFragmentPlan`] is the one place in this protocol where a generated
//! message is the stored representation of a value. The reason is recorded in
//! ADR-0135: the only transport-neutral plan form in this engine, `ExecPlan`,
//! is not a value — its scan, writer, and finish nodes hold `Arc<dyn ..>`
//! leaves implemented only in the backend, and it carries no serde — so the
//! frontend has nothing neutral to author instead.
//!
//! The wrapper keeps that message private and answers the neutral
//! `PhysicalFragmentPlan` capability with typed accessors. The frontend's
//! remote task, the backend's registry, and every retained record hold only
//! the descriptor. The backend's own plan decoder is the single consumer that
//! reads the message back out, which is what [`WireFragmentPlan::plan`] and
//! [`WireFragmentPlan::instance_params`] exist for.
// Design: ADR-0135 (docs/adr/ADR-0135-native-distributed-work-as-tasks.md)

use std::num::{NonZeroU32, NonZeroUsize};
use std::sync::Arc;

use novarocks_execution::exec::fragment::program::{
    FragmentContractVersion, FragmentNodeId, FragmentSinkKind,
};
use novarocks_execution::exec::fragment::sink::DataStreamPartitionType;
use novarocks_execution::runtime::endpoint::RuntimeEndpoint;
use novarocks_execution::task_execution::descriptor::{
    ExchangeDestination, ExchangeEdge, ExchangeInbound, ExchangeSource, ExchangeTopology,
    PhysicalFragmentPlan, TASK_DESCRIPTOR_MAX_PLAN_ENCODED_BYTES, TaskDescriptor,
};
use novarocks_execution::task_execution::domain::{
    CodecOwnedContent, ContentFingerprint, ExchangeEdgeId, PlanNodeId,
};
use novarocks_proto_models::{novarocks, plan};
use prost::Message;
use sha2::{Digest, Sha256};

use crate::task_execution::identity::{decode_task_identity, encode_task_identity};
use crate::task_execution::{
    duplicate, inconsistent, invalid, invalid_enum, missing, out_of_range,
};
use crate::{FieldPath, ProtocolError};

/// Largest number of destinations on one exchange edge.
pub const MAX_EDGE_DESTINATIONS: usize = 4096;

/// Largest number of frozen sources on one inbound exchange node.
pub const MAX_INBOUND_SOURCES: usize = 4096;

/// Largest number of outbound edges or inbound nodes on one task.
pub const MAX_TOPOLOGY_ENTRIES: usize = 256;

/// Largest number of split-bearing plan nodes on one task.
pub const MAX_SPLIT_PLAN_NODES: usize = 1024;

/// Domain separation tag for the descriptor fingerprint. It keeps this digest
/// from ever colliding with a digest computed for another purpose.
const FRAGMENT_PLAN_FINGERPRINT_DOMAIN: &[u8] = b"novarocks.task_execution.fragment_plan.v1";

/// The physical fragment plan of one task, holding the generated message
/// privately.
#[derive(Clone, Debug)]
pub struct WireFragmentPlan {
    wire: novarocks::TaskFragmentPlan,
    fingerprint: ContentFingerprint,
    encoded_len: usize,
    contract_version: FragmentContractVersion,
    sink_kind: FragmentSinkKind,
}

impl WireFragmentPlan {
    /// Validates and wraps a fragment plan.
    ///
    /// The encoded size is checked before anything walks the contents, so an
    /// oversized plan costs one length computation rather than a full
    /// traversal.
    pub fn parse(
        wire: novarocks::TaskFragmentPlan,
        path: FieldPath,
    ) -> Result<Self, ProtocolError> {
        let encoded_len = wire.encoded_len();
        if encoded_len > TASK_DESCRIPTOR_MAX_PLAN_ENCODED_BYTES {
            return Err(out_of_range(
                path,
                "fragment plan exceeds the encoded size limit",
            ));
        }
        let plan = wire.plan.as_ref().ok_or_else(|| {
            missing(
                path.clone().field("plan"),
                "fragment plan requires a plan fragment",
            )
        })?;
        if wire.instance_params.is_none() {
            return Err(missing(
                path.clone().field("instance_params"),
                "fragment plan requires instance parameters",
            ));
        }
        let sink = plan.sink.as_ref().ok_or_else(|| {
            missing(
                path.clone().field("plan").field("sink"),
                "plan fragment requires a sink",
            )
        })?;
        let sink_kind = decode_sink_kind(sink, path.field("plan").field("sink"))?;
        let fingerprint = fingerprint_of(&wire);
        Ok(Self {
            wire,
            fingerprint,
            encoded_len,
            // The fragment contract version is not a wire field: both roles
            // build against one compiled contract, and a mismatch is caught by
            // the compatibility island rather than negotiated per fragment.
            contract_version: FragmentContractVersion::CURRENT,
            sink_kind,
        })
    }

    /// The plan fragment, for the backend's own plan decoder.
    pub fn plan(&self) -> &plan::PlanFragment {
        self.wire
            .plan
            .as_ref()
            .expect("a validated fragment plan always has a plan")
    }

    /// The per-instance parameters, for the backend's own plan decoder.
    pub fn instance_params(&self) -> &novarocks::InstanceParams {
        self.wire
            .instance_params
            .as_ref()
            .expect("a validated fragment plan always has instance parameters")
    }

    pub const fn as_proto(&self) -> &novarocks::TaskFragmentPlan {
        &self.wire
    }

    pub fn into_proto(self) -> novarocks::TaskFragmentPlan {
        self.wire
    }
}

impl CodecOwnedContent for WireFragmentPlan {
    fn fingerprint(&self) -> ContentFingerprint {
        self.fingerprint
    }

    fn encoded_len(&self) -> usize {
        self.encoded_len
    }

    /// The stored plan, for the backend that has to decode and run it.
    ///
    /// A descriptor hands its plan around as `Arc<dyn PhysicalFragmentPlan>`,
    /// and that trait answers only the two questions the neutral layer is
    /// allowed to ask. Without this, the plan could reach the one owner that
    /// must submit it and still be unusable there.
    fn stored_representation(&self) -> Option<&(dyn std::any::Any + 'static)> {
        Some(self)
    }
}

impl PhysicalFragmentPlan for WireFragmentPlan {
    fn contract_version(&self) -> FragmentContractVersion {
        self.contract_version
    }

    fn sink_kind(&self) -> FragmentSinkKind {
        self.sink_kind
    }
}

/// Fingerprints a fragment plan.
///
/// A plan carries no credential material: credentials belong to the query
/// context's confidential domain and never travel in a descriptor. So the
/// whole encoding can safely take part in this digest.
fn fingerprint_of(wire: &novarocks::TaskFragmentPlan) -> ContentFingerprint {
    let mut hasher = Sha256::new();
    hasher.update(FRAGMENT_PLAN_FINGERPRINT_DOMAIN);
    hasher.update(wire.encode_to_vec());
    let digest = hasher.finalize();
    let mut bytes = [0u8; 16];
    bytes.copy_from_slice(&digest[..16]);
    ContentFingerprint::from_bytes(bytes)
}

fn decode_sink_kind(
    sink: &plan::DataSink,
    path: FieldPath,
) -> Result<FragmentSinkKind, ProtocolError> {
    let kind = sink
        .kind
        .as_ref()
        .ok_or_else(|| missing(path.clone(), "data sink requires a kind"))?;
    Ok(match kind {
        plan::data_sink::Kind::Result(_) => FragmentSinkKind::Result,
        plan::data_sink::Kind::Noop(_) => FragmentSinkKind::Noop,
        plan::data_sink::Kind::DataStream(_) => FragmentSinkKind::DataStream,
        plan::data_sink::Kind::MultiCastDataStream(_) => FragmentSinkKind::MultiCastDataStream,
        plan::data_sink::Kind::ChangeStreamRouter(_) => FragmentSinkKind::SplitDataStream,
    })
}

fn decode_partitioning(
    value: i32,
    path: FieldPath,
) -> Result<DataStreamPartitionType, ProtocolError> {
    match novarocks::ExchangePartitioning::try_from(value) {
        Ok(novarocks::ExchangePartitioning::Unpartitioned) => {
            Ok(DataStreamPartitionType::Unpartitioned)
        }
        Ok(novarocks::ExchangePartitioning::Random) => Ok(DataStreamPartitionType::Random),
        Ok(novarocks::ExchangePartitioning::Hash) => Ok(DataStreamPartitionType::HashPartitioned),
        Ok(novarocks::ExchangePartitioning::BucketShuffleHash) => {
            Ok(DataStreamPartitionType::BucketShuffleHashPartitioned)
        }
        Ok(novarocks::ExchangePartitioning::Unspecified) | Err(_) => Err(invalid_enum(
            path,
            "exchange partitioning must be a known non-default value",
        )),
    }
}

fn encode_partitioning(value: DataStreamPartitionType) -> i32 {
    let encoded = match value {
        DataStreamPartitionType::Unpartitioned => novarocks::ExchangePartitioning::Unpartitioned,
        DataStreamPartitionType::Random => novarocks::ExchangePartitioning::Random,
        DataStreamPartitionType::HashPartitioned => novarocks::ExchangePartitioning::Hash,
        DataStreamPartitionType::BucketShuffleHashPartitioned => {
            novarocks::ExchangePartitioning::BucketShuffleHash
        }
    };
    encoded as i32
}

fn decode_unique_id(
    src: Option<&novarocks_proto_models::common::UniqueId>,
    path: FieldPath,
    detail: &'static str,
) -> Result<novarocks_types::UniqueId, ProtocolError> {
    let value = src.ok_or_else(|| missing(path, detail))?;
    Ok(novarocks_types::UniqueId::new(value.hi, value.lo))
}

fn encode_unique_id(value: novarocks_types::UniqueId) -> novarocks_proto_models::common::UniqueId {
    novarocks_proto_models::common::UniqueId {
        hi: value.high(),
        lo: value.low(),
    }
}

fn decode_nonnegative_node(value: i32, path: FieldPath) -> Result<FragmentNodeId, ProtocolError> {
    if value < 0 {
        return Err(out_of_range(path, "node id must be nonnegative"));
    }
    Ok(FragmentNodeId::new(value))
}

fn decode_destination(
    src: &novarocks::TaskExchangeDestination,
    path: FieldPath,
) -> Result<ExchangeDestination, ProtocolError> {
    let task = src.task.as_ref().ok_or_else(|| {
        missing(
            path.clone().field("task"),
            "exchange destination requires a task identity",
        )
    })?;
    let task = decode_task_identity(task, path.clone().field("task"))?;
    let fragment_instance_id = decode_unique_id(
        src.fragment_instance_id.as_ref(),
        path.clone().field("fragment_instance_id"),
        "exchange destination requires a fragment instance id",
    )?;
    let endpoint = src.endpoint.as_ref().ok_or_else(|| {
        missing(
            path.clone().field("endpoint"),
            "exchange destination requires an endpoint",
        )
    })?;
    let endpoint = RuntimeEndpoint::new(endpoint.host.clone(), endpoint.port as i32)
        .map_err(|error| invalid(path.clone().field("endpoint"), error))?;
    let node = decode_nonnegative_node(
        src.destination_node_id,
        path.clone().field("destination_node_id"),
    )?;
    let sender_count = NonZeroU32::new(src.sender_count).ok_or_else(|| {
        invalid(
            path.clone().field("sender_count"),
            "sender count must be nonzero",
        )
    })?;
    ExchangeDestination::try_new(
        task,
        fragment_instance_id,
        endpoint,
        node,
        src.sender_ordinal,
        sender_count,
    )
    .map_err(|error| out_of_range(path, error.to_string()))
}

fn encode_destination(value: &ExchangeDestination) -> novarocks::TaskExchangeDestination {
    novarocks::TaskExchangeDestination {
        task: Some(encode_task_identity(value.task())),
        fragment_instance_id: Some(encode_unique_id(value.fragment_instance_id())),
        endpoint: Some(novarocks::QueryControlEndpoint {
            host: value.endpoint().host().to_owned(),
            // A validated runtime endpoint always holds a nonzero u16 port.
            port: value.endpoint().port() as u32,
        }),
        destination_node_id: value.destination_node_id().get(),
        sender_ordinal: value.sender_ordinal(),
        sender_count: value.sender_count().get(),
    }
}

fn decode_edge(
    src: &novarocks::TaskExchangeEdge,
    path: FieldPath,
) -> Result<ExchangeEdge, ProtocolError> {
    let edge_id = ExchangeEdgeId::new(src.edge_id)
        .map_err(|error| invalid(path.clone().field("edge_id"), error.to_string()))?;
    let node = decode_nonnegative_node(
        src.destination_node_id,
        path.clone().field("destination_node_id"),
    )?;
    let partitioning = decode_partitioning(src.partitioning, path.clone().field("partitioning"))?;
    if src.destinations.is_empty() {
        return Err(missing(
            path.clone().field("destinations"),
            "exchange edge requires at least one destination",
        ));
    }
    if src.destinations.len() > MAX_EDGE_DESTINATIONS {
        return Err(out_of_range(
            path.clone().field("destinations"),
            "exchange edge destination count exceeds the hard limit",
        ));
    }
    let mut destinations = Vec::with_capacity(src.destinations.len());
    for (index, destination) in src.destinations.iter().enumerate() {
        destinations.push(decode_destination(
            destination,
            path.clone().field("destinations").index(index),
        )?);
    }
    ExchangeEdge::try_new(edge_id, node, partitioning, destinations)
        .map_err(|error| inconsistent(path, error.to_string()))
}

fn encode_edge(value: &ExchangeEdge) -> novarocks::TaskExchangeEdge {
    novarocks::TaskExchangeEdge {
        edge_id: value.edge_id().get(),
        destination_node_id: value.destination_node_id().get(),
        partitioning: encode_partitioning(value.partitioning()),
        destinations: value
            .destinations()
            .iter()
            .map(encode_destination)
            .collect(),
    }
}

fn decode_inbound(
    src: &novarocks::TaskExchangeInbound,
    path: FieldPath,
) -> Result<ExchangeInbound, ProtocolError> {
    let node = decode_nonnegative_node(
        src.destination_node_id,
        path.clone().field("destination_node_id"),
    )?;
    if src.sources.is_empty() {
        return Err(missing(
            path.clone().field("sources"),
            "inbound exchange node requires at least one frozen source",
        ));
    }
    if src.sources.len() > MAX_INBOUND_SOURCES {
        return Err(out_of_range(
            path.clone().field("sources"),
            "inbound source count exceeds the hard limit",
        ));
    }
    let mut sources = Vec::with_capacity(src.sources.len());
    for (index, source) in src.sources.iter().enumerate() {
        let source_path = path.clone().field("sources").index(index);
        let task = source.task.as_ref().ok_or_else(|| {
            missing(
                source_path.clone().field("task"),
                "exchange source requires a task identity",
            )
        })?;
        let task = decode_task_identity(task, source_path.clone().field("task"))?;
        let key = decode_unique_id(
            source.fragment_instance_id.as_ref(),
            source_path.field("fragment_instance_id"),
            "exchange source requires a fragment instance id",
        )?;
        sources.push(ExchangeSource::new(task, key));
    }
    ExchangeInbound::try_new(node, sources).map_err(|error| duplicate(path, error.to_string()))
}

fn encode_inbound(value: &ExchangeInbound) -> novarocks::TaskExchangeInbound {
    novarocks::TaskExchangeInbound {
        destination_node_id: value.node_id().get(),
        sources: value
            .sources()
            .iter()
            .map(|source| novarocks::TaskExchangeSource {
                task: Some(encode_task_identity(source.task())),
                fragment_instance_id: Some(encode_unique_id(source.fragment_instance_id())),
            })
            .collect(),
    }
}

/// Decodes the frozen push exchange topology of one task.
pub fn decode_topology(
    src: &novarocks::TaskExchangeTopology,
    path: FieldPath,
) -> Result<ExchangeTopology, ProtocolError> {
    if src.outbound.len() > MAX_TOPOLOGY_ENTRIES {
        return Err(out_of_range(
            path.clone().field("outbound"),
            "outbound edge count exceeds the hard limit",
        ));
    }
    if src.inbound.len() > MAX_TOPOLOGY_ENTRIES {
        return Err(out_of_range(
            path.clone().field("inbound"),
            "inbound node count exceeds the hard limit",
        ));
    }
    let mut outbound = Vec::with_capacity(src.outbound.len());
    for (index, edge) in src.outbound.iter().enumerate() {
        outbound.push(decode_edge(
            edge,
            path.clone().field("outbound").index(index),
        )?);
    }
    let mut inbound = Vec::with_capacity(src.inbound.len());
    for (index, node) in src.inbound.iter().enumerate() {
        inbound.push(decode_inbound(
            node,
            path.clone().field("inbound").index(index),
        )?);
    }
    ExchangeTopology::try_new(outbound, inbound).map_err(|error| duplicate(path, error.to_string()))
}

pub fn encode_topology(value: &ExchangeTopology) -> novarocks::TaskExchangeTopology {
    novarocks::TaskExchangeTopology {
        outbound: value.outbound().iter().map(encode_edge).collect(),
        inbound: value.inbound().iter().map(encode_inbound).collect(),
    }
}

/// Decodes an immutable task descriptor.
///
/// The neutral protocol fields are proved against the plan carrier before
/// either role sees the result: the fragment instance id, the parallelism, the
/// destination set, and the per-exchange sender counts must all agree with
/// `instance_params`. That is what keeps the projection from becoming a second
/// authority that can silently drift from the plan the kernel will run.
/// Returns the neutral descriptor together with the typed plan handle, so the
/// backend's own decoder can reach the plan without downcasting a trait
/// object.
pub fn decode_task_descriptor(
    src: &novarocks::TaskDescriptor,
    path: FieldPath,
) -> Result<(TaskDescriptor, Arc<WireFragmentPlan>), ProtocolError> {
    let identity = src.identity.as_ref().ok_or_else(|| {
        missing(
            path.clone().field("identity"),
            "task descriptor requires an identity",
        )
    })?;
    let identity = decode_task_identity(identity, path.clone().field("identity"))?;

    let fragment_instance_id = src.fragment_instance_id.as_ref().ok_or_else(|| {
        missing(
            path.clone().field("fragment_instance_id"),
            "task descriptor requires a fragment instance id",
        )
    })?;
    let fragment_instance_id =
        novarocks_types::UniqueId::new(fragment_instance_id.hi, fragment_instance_id.lo);

    let pipeline_dop = NonZeroUsize::new(src.pipeline_dop as usize).ok_or_else(|| {
        invalid(
            path.clone().field("pipeline_dop"),
            "pipeline parallelism must be nonzero",
        )
    })?;

    if src.split_plan_nodes.len() > MAX_SPLIT_PLAN_NODES {
        return Err(out_of_range(
            path.clone().field("split_plan_nodes"),
            "split plan node count exceeds the hard limit",
        ));
    }
    let mut split_plan_nodes = Vec::with_capacity(src.split_plan_nodes.len());
    for (index, node) in src.split_plan_nodes.iter().enumerate() {
        let node_path = path.clone().field("split_plan_nodes").index(index);
        split_plan_nodes.push(
            PlanNodeId::new(*node).map_err(|error| out_of_range(node_path, error.to_string()))?,
        );
    }

    let topology = src.topology.as_ref().ok_or_else(|| {
        missing(
            path.clone().field("topology"),
            "task descriptor requires an exchange topology",
        )
    })?;
    let topology = decode_topology(topology, path.clone().field("topology"))?;

    let fragment = src.fragment.as_ref().ok_or_else(|| {
        missing(
            path.clone().field("fragment"),
            "task descriptor requires a fragment plan",
        )
    })?;
    let fragment = Arc::new(WireFragmentPlan::parse(
        fragment.clone(),
        path.clone().field("fragment"),
    )?);

    verify_projection(
        &fragment,
        fragment_instance_id,
        pipeline_dop,
        &topology,
        path.clone(),
    )?;

    let descriptor = TaskDescriptor::try_new(
        identity,
        fragment_instance_id,
        pipeline_dop,
        split_plan_nodes,
        topology,
        Arc::clone(&fragment) as Arc<dyn PhysicalFragmentPlan>,
    )
    .map_err(|error| inconsistent(path, error.to_string()))?;
    Ok((descriptor, fragment))
}

/// Proves the descriptor's neutral projection agrees with the plan carrier.
fn verify_projection(
    fragment: &WireFragmentPlan,
    fragment_instance_id: novarocks_types::UniqueId,
    pipeline_dop: NonZeroUsize,
    topology: &ExchangeTopology,
    path: FieldPath,
) -> Result<(), ProtocolError> {
    let instance = fragment.instance_params();

    let wire_finst = decode_unique_id(
        instance.fragment_instance_id.as_ref(),
        path.clone()
            .field("fragment")
            .field("instance_params")
            .field("fragment_instance_id"),
        "instance parameters require a fragment instance id",
    )?;
    if wire_finst != fragment_instance_id {
        return Err(inconsistent(
            path.clone().field("fragment_instance_id"),
            "descriptor fragment instance id disagrees with instance parameters",
        ));
    }

    let options = instance.query_options.as_ref().ok_or_else(|| {
        missing(
            path.clone()
                .field("fragment")
                .field("instance_params")
                .field("query_options"),
            "instance parameters require query options",
        )
    })?;
    if options.pipeline_dop <= 0 || options.pipeline_dop as usize != pipeline_dop.get() {
        return Err(inconsistent(
            path.clone().field("pipeline_dop"),
            "descriptor parallelism disagrees with instance query options",
        ));
    }

    let declared_destinations: usize = topology
        .outbound()
        .iter()
        .map(|edge| edge.destinations().len())
        .sum();
    if declared_destinations != instance.destinations.len() {
        return Err(inconsistent(
            path.clone().field("topology").field("outbound"),
            "descriptor destination count disagrees with instance parameters",
        ));
    }
    for edge in topology.outbound() {
        for destination in edge.destinations() {
            let matched = instance.destinations.iter().any(|wire| {
                wire.finst_id.as_ref().is_some_and(|id| {
                    novarocks_types::UniqueId::new(id.hi, id.lo)
                        == destination.fragment_instance_id()
                }) && wire.sender_ordinal == destination.sender_ordinal()
                    && wire.sender_count == destination.sender_count().get()
            });
            if !matched {
                return Err(inconsistent(
                    path.clone().field("topology").field("outbound"),
                    "descriptor destination has no matching instance destination",
                ));
            }
        }
    }

    for node in topology.inbound() {
        let wire_senders = instance
            .per_exch_num_senders
            .get(&node.node_id().get())
            .copied()
            .ok_or_else(|| {
                inconsistent(
                    path.clone().field("topology").field("inbound"),
                    "inbound exchange node is absent from instance sender counts",
                )
            })?;
        if wire_senders < 0 || wire_senders as u32 != node.expected_sender_count().get() {
            return Err(inconsistent(
                path.clone().field("topology").field("inbound"),
                "inbound sender count disagrees with instance parameters",
            ));
        }
    }

    Ok(())
}

/// Encodes an immutable task descriptor.
pub fn encode_task_descriptor(
    value: &TaskDescriptor,
    fragment: &WireFragmentPlan,
) -> novarocks::TaskDescriptor {
    novarocks::TaskDescriptor {
        identity: Some(encode_task_identity(value.identity())),
        fragment_instance_id: Some(encode_unique_id(value.fragment_instance_id())),
        pipeline_dop: value.pipeline_dop().get() as u32,
        split_plan_nodes: value
            .split_plan_nodes()
            .iter()
            .map(|node| node.get())
            .collect(),
        topology: Some(encode_topology(value.topology())),
        fragment: Some(fragment.as_proto().clone()),
    }
}
