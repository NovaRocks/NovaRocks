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

//! Allocation-free sizes of the original creation grammar. These helpers
//! create no DTO, buffer, request, resource allowance or authority.
use super::*;
use novarocks_execution_contract::task_execution::identity::TaskIdentity;
use novarocks_execution_contract::{DataStreamPartitionType, ExchangeDeliveryRequirement};
use novarocks_types::UniqueId;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CreationSizeOverflow;
fn add(a: usize, b: usize) -> Result<usize, CreationSizeOverflow> {
    a.checked_add(b).ok_or(CreationSizeOverflow)
}
pub fn message(tag: u32, bytes: usize) -> Result<usize, CreationSizeOverflow> {
    add(
        add(
            key_len(tag),
            encoded_len_varint(u64::try_from(bytes).map_err(|_| CreationSizeOverflow)?),
        )?,
        bytes,
    )
}
pub fn uint(tag: u32, value: u64) -> usize {
    if value == 0 {
        0
    } else {
        key_len(tag) + encoded_len_varint(value)
    }
}
pub fn int(tag: u32, value: i64) -> usize {
    uint(tag, value as u64)
}
pub fn packed(tag: u32, value_bytes: usize) -> Result<usize, CreationSizeOverflow> {
    if value_bytes == 0 {
        Ok(0)
    } else {
        message(tag, value_bytes)
    }
}
pub fn unique(value: UniqueId) -> usize {
    int(1, value.high()) + int(2, value.low())
}
pub fn task(value: TaskIdentity) -> Result<usize, CreationSizeOverflow> {
    // QueryExecutionId's original codec creates only inline scalar messages.
    let query = novarocks_proto_codec::lifecycle::identity::encode_query_execution_id(
        value.query_execution_id(),
    )
    .encoded_len();
    let process = message(1, 16)?;
    add(
        add(message(1, query)?, uint(2, value.stage_id().get() as u64))?,
        add(uint(3, value.task_id().get() as u64), message(4, process)?)?,
    )
}
pub fn context(value: QueryContextRef) -> Result<usize, CreationSizeOverflow> {
    let query = novarocks_proto_codec::lifecycle::identity::encode_query_execution_id(
        value.query_execution_id(),
    )
    .encoded_len();
    let process = message(1, 16)?;
    add(
        message(1, query)?,
        add(message(2, process)?, message(3, process)?)?,
    )
}
pub fn destination(
    task_id: TaskIdentity,
    key: UniqueId,
    host: &str,
    port: u16,
    node: i32,
) -> Result<usize, CreationSizeOverflow> {
    let endpoint = add(
        if host.is_empty() {
            0
        } else {
            message(1, host.len())?
        },
        uint(2, port as u64),
    )?;
    add(
        add(message(1, task(task_id)?)?, message(2, unique(key))?)?,
        add(message(3, endpoint)?, int(4, node as i64))?,
    )
}
pub fn source(
    task_id: TaskIdentity,
    key: UniqueId,
    ordinal: u32,
) -> Result<usize, CreationSizeOverflow> {
    add(
        add(message(1, task(task_id)?)?, message(2, unique(key))?)?,
        uint(3, ordinal as u64),
    )
}
pub fn edge_header(
    edge: u32,
    node: i32,
    partition: DataStreamPartitionType,
    ordinal: u32,
    count: u32,
) -> usize {
    let partition = match partition {
        DataStreamPartitionType::Unpartitioned => 1,
        DataStreamPartitionType::Random => 2,
        DataStreamPartitionType::HashPartitioned => 3,
        DataStreamPartitionType::BucketShuffleHashPartitioned => 4,
    };
    uint(1, edge as u64)
        + int(2, node as i64)
        + uint(3, partition)
        + uint(5, ordinal as u64)
        + uint(6, count as u64)
}
pub fn inbound_header(node: i32, delivery: ExchangeDeliveryRequirement) -> usize {
    int(1, node as i64)
        + uint(
            3,
            match delivery {
                ExchangeDeliveryRequirement::DemandDriven => 1,
                ExchangeDeliveryRequirement::MustDrain => 2,
            },
        )
}
pub fn descriptor(
    identity: TaskIdentity,
    key: UniqueId,
    dop: usize,
    split_bytes: usize,
    topology: usize,
) -> Result<usize, CreationSizeOverflow> {
    add(
        add(message(1, task(identity)?)?, message(2, unique(key))?)?,
        add(
            add(uint(3, dop as u64), packed(4, split_bytes)?)?,
            message(5, topology)?,
        )?,
    )
}
pub fn metadata(
    context_id: QueryContextRef,
    descriptor: usize,
    assignment: usize,
) -> Result<usize, CreationSizeOverflow> {
    add(
        add(message(1, context(context_id)?)?, message(2, descriptor)?)?,
        message(5, assignment)?,
    )
}
