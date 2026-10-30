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

//! Task descriptor codec.
//!
//! The descriptor is a neutral value on both sides: identity, kernel key,
//! parallelism, split plan nodes, and the complete push exchange topology.
//! It carries no physical plan. The static plan travels as its own immutable
//! carrier, which only the backend that wins a task's creation decodes; see
//! [`crate::creation`].
// Design: ADR-0161 (docs/adr/ADR-0161-pure-local-programs-and-supervised-task-lifecycle.md)

use std::num::{NonZeroU32, NonZeroUsize};

use novarocks_execution_contract::DataStreamPartitionType;
use novarocks_execution_contract::FragmentNodeId;
use novarocks_execution_contract::RuntimeEndpoint;
use novarocks_execution_contract::task_execution::descriptor::{
    ExchangeDestination, ExchangeEdge, ExchangeInbound, ExchangeSource, ExchangeTopology,
    TaskDescriptor,
};
use novarocks_execution_contract::task_execution::domain::{ExchangeEdgeId, PlanNodeId};
use novarocks_proto_models::novarocks;

use crate::identity::{decode_task_identity, encode_task_identity};
use crate::{duplicate, inconsistent, invalid, invalid_enum, missing, out_of_range};
use novarocks_proto_codec::{FieldPath, ProtocolError};

/// Largest number of destinations on one exchange edge.
pub const MAX_EDGE_DESTINATIONS: usize = 4096;

/// Largest number of frozen sources on one inbound exchange node.
pub const MAX_INBOUND_SOURCES: usize = 4096;

/// Largest number of outbound edges or inbound nodes on one task.
pub const MAX_TOPOLOGY_ENTRIES: usize = 256;

/// Largest number of split-bearing plan nodes on one task.
pub const MAX_SPLIT_PLAN_NODES: usize = 1024;

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
    let node = decode_nonnegative_node(src.destination_node_id, path.field("destination_node_id"))?;
    Ok(ExchangeDestination::new(
        task,
        fragment_instance_id,
        endpoint,
        node,
    ))
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
    let sender_count = NonZeroU32::new(src.sender_count).ok_or_else(|| {
        invalid(
            path.clone().field("sender_count"),
            "sender count must be nonzero",
        )
    })?;
    if src.sender_ordinal >= sender_count.get() {
        return Err(out_of_range(
            path.clone().field("sender_ordinal"),
            "sender ordinal must be strictly below the sender count",
        ));
    }
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
    ExchangeEdge::try_new(
        edge_id,
        node,
        partitioning,
        destinations,
        src.sender_ordinal,
        sender_count,
    )
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
        sender_ordinal: value.sender_ordinal(),
        sender_count: value.sender_count().get(),
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
    let delivery = match novarocks::ExchangeDeliveryRequirement::try_from(src.delivery) {
        Ok(novarocks::ExchangeDeliveryRequirement::DemandDriven) => {
            novarocks_execution_contract::ExchangeDeliveryRequirement::DemandDriven
        }
        Ok(novarocks::ExchangeDeliveryRequirement::MustDrain) => {
            novarocks_execution_contract::ExchangeDeliveryRequirement::MustDrain
        }
        Ok(novarocks::ExchangeDeliveryRequirement::Unspecified) | Err(_) => {
            return Err(invalid_enum(
                path.field("delivery"),
                "inbound delivery must be a known non-default value",
            ));
        }
    };
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
        sources.push(ExchangeSource::new(task, key, source.sender_ordinal));
    }
    ExchangeInbound::try_new(node, sources, delivery).map_err(|error| match error {
        novarocks_execution_contract::task_execution::descriptor::DescriptorError::DuplicateInboundSource(
            _,
        ) => duplicate(path, error.to_string()),
        novarocks_execution_contract::task_execution::descriptor::DescriptorError::InvalidInboundSenderOrdinals {
            ..
        } => inconsistent(path, error.to_string()),
        _ => inconsistent(path, error.to_string()),
    })
}

fn encode_inbound(value: &ExchangeInbound) -> novarocks::TaskExchangeInbound {
    novarocks::TaskExchangeInbound {
        destination_node_id: value.node_id().get(),
        delivery: (match value.delivery() {
            novarocks_execution_contract::ExchangeDeliveryRequirement::DemandDriven => {
                novarocks::ExchangeDeliveryRequirement::DemandDriven
            }
            novarocks_execution_contract::ExchangeDeliveryRequirement::MustDrain => {
                novarocks::ExchangeDeliveryRequirement::MustDrain
            }
        }) as i32,
        sources: value
            .sources()
            .iter()
            .map(|source| novarocks::TaskExchangeSource {
                task: Some(encode_task_identity(source.task())),
                fragment_instance_id: Some(encode_unique_id(source.fragment_instance_id())),
                sender_ordinal: source.sender_ordinal(),
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
/// Only the descriptor's own structure is proved here: identity, a nonzero
/// parallelism, bounded and unique split plan nodes, and a locally consistent
/// topology. Its relation to the static plan is proved by the backend that
/// wins the task's creation, which is the only owner that decodes that plan.
pub fn decode_task_descriptor(
    src: &novarocks::TaskDescriptor,
    path: FieldPath,
) -> Result<TaskDescriptor, ProtocolError> {
    let identity = src.identity.as_ref().ok_or_else(|| {
        missing(
            path.clone().field("identity"),
            "task descriptor requires an identity",
        )
    })?;
    let identity = decode_task_identity(identity, path.clone().field("identity"))?;

    let fragment_instance_id = decode_unique_id(
        src.fragment_instance_id.as_ref(),
        path.clone().field("fragment_instance_id"),
        "task descriptor requires a fragment instance id",
    )?;

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

    TaskDescriptor::try_new(
        identity,
        fragment_instance_id,
        pipeline_dop,
        split_plan_nodes,
        topology,
    )
    .map_err(|error| inconsistent(path, error.to_string()))
}

/// Encodes an immutable task descriptor.
pub fn encode_task_descriptor(value: &TaskDescriptor) -> novarocks::TaskDescriptor {
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
    }
}

#[cfg(test)]
mod inbound_delivery_tests {
    use super::*;

    #[test]
    fn inbound_delivery_rejects_unspecified_or_unknown() {
        let inbound = |delivery| novarocks::TaskExchangeInbound {
            destination_node_id: 1,
            sources: Vec::new(),
            delivery,
        };
        for delivery in [0, -1, 99] {
            let error = decode_inbound(&inbound(delivery), FieldPath::root("inbound")).unwrap_err();
            assert_eq!(
                error.kind(),
                &novarocks_proto_codec::ProtocolErrorKind::InvalidEnum
            );
            assert_eq!(error.path().to_string(), "inbound.delivery");
        }
    }
}
