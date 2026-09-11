// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. See the License for the
// specific language governing permissions and limitations
// under the License.

use std::fmt;

use novarocks_execution_contract::{ExchangeSource, FragmentNodeId, TaskDescriptor};
use novarocks_types::UniqueId;

/// Why this worker rejects an inbound exchange frame.
///
/// Every variant is decided from the frozen descriptor before an Arrow
/// payload is decoded and before a receiver is allocated.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub enum IngressRejection {
    UnknownDestinationTask,
    UnknownDestinationNode(FragmentNodeId),
    SourceNotFrozen,
    SenderCountMismatch { expected: u32, received: u32 },
    SenderOrdinalOutOfRange { ordinal: u32, expected: u32 },
    SenderOrdinalMismatch { expected: u32, received: u32 },
}

impl fmt::Display for IngressRejection {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::UnknownDestinationTask => {
                formatter.write_str("inbound frame names another task as its destination")
            }
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
            Self::SenderOrdinalMismatch { expected, received } => write!(
                formatter,
                "inbound frame sender ordinal {received} does not match its frozen ordinal {expected}"
            ),
        }
    }
}

impl std::error::Error for IngressRejection {}

/// Authorizes an exchange frame from frozen descriptor facts before payload
/// decoding or receiver allocation.
pub fn authorize_inbound_frame(
    descriptor: &TaskDescriptor,
    destination_kernel_key: UniqueId,
    destination_node_id: FragmentNodeId,
    source_kernel_key: UniqueId,
    sender_ordinal: u32,
    sender_count: u32,
) -> Result<ExchangeSource, IngressRejection> {
    if destination_kernel_key != descriptor.fragment_instance_id() {
        return Err(IngressRejection::UnknownDestinationTask);
    }
    let node = descriptor
        .topology()
        .inbound_node(destination_node_id)
        .ok_or(IngressRejection::UnknownDestinationNode(
            destination_node_id,
        ))?;
    let source = node
        .source_by_kernel_key(source_kernel_key)
        .ok_or(IngressRejection::SourceNotFrozen)?;
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
    if sender_ordinal != source.sender_ordinal() {
        return Err(IngressRejection::SenderOrdinalMismatch {
            expected: source.sender_ordinal(),
            received: sender_ordinal,
        });
    }
    Ok(source)
}
