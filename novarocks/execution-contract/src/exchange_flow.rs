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

//! Exact exchange stream identities, delivery requirements and normal-close facts.
//! These declarations do not authorize capacity or infer consumer completion.

// Design: ADR-0208 (docs/adr/ADR-0208-logical-write-counts-and-strict-stream-delivery.md)
use std::fmt;

use novarocks_types::UniqueId;

use crate::{ExchangeSource, FragmentNodeId, TaskIdentity};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ExchangeFlowContractError {
    CrossAttemptStream,
    NegativeDestinationNode,
    NegativeSequence,
    SequenceExhausted,
}

impl fmt::Display for ExchangeFlowContractError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::CrossAttemptStream => "exchange stream crosses query attempts",
            Self::NegativeDestinationNode => "exchange destination node must be nonnegative",
            Self::NegativeSequence => "exchange sequence must be nonnegative",
            Self::SequenceExhausted => "exchange sequence domain is exhausted",
        })
    }
}
impl std::error::Error for ExchangeFlowContractError {}

/// One source-to-input stream; endpoint reuse never changes its identity.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct ExchangeStreamIdentity {
    source: ExchangeSource,
    destination: TaskIdentity,
    destination_fragment_instance_id: UniqueId,
    destination_node: FragmentNodeId,
}

impl ExchangeStreamIdentity {
    pub fn try_new(
        source: ExchangeSource,
        destination: TaskIdentity,
        destination_fragment_instance_id: UniqueId,
        destination_node: FragmentNodeId,
    ) -> Result<Self, ExchangeFlowContractError> {
        if source.task().query_execution_id() != destination.query_execution_id() {
            return Err(ExchangeFlowContractError::CrossAttemptStream);
        }
        if destination_node.get() < 0 {
            return Err(ExchangeFlowContractError::NegativeDestinationNode);
        }
        Ok(Self {
            source,
            destination,
            destination_fragment_instance_id,
            destination_node,
        })
    }
    pub const fn source(self) -> ExchangeSource {
        self.source
    }
    pub const fn destination(self) -> TaskIdentity {
        self.destination
    }
    pub const fn destination_fragment_instance_id(self) -> UniqueId {
        self.destination_fragment_instance_id
    }
    pub const fn destination_node(self) -> FragmentNodeId {
        self.destination_node
    }
}

/// Allocated at the exact stream's serialized admission point, including EOS.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct ExchangeFrameSequence(i64);

impl ExchangeFrameSequence {
    pub const FIRST: Self = Self(0);
    pub fn try_new(sequence: i64) -> Result<Self, ExchangeFlowContractError> {
        if sequence < 0 {
            return Err(ExchangeFlowContractError::NegativeSequence);
        }
        Ok(Self(sequence))
    }
    pub const fn get(self) -> i64 {
        self.0
    }
    pub fn checked_next(self) -> Result<Self, ExchangeFlowContractError> {
        self.0
            .checked_add(1)
            .map(Self)
            .ok_or(ExchangeFlowContractError::SequenceExhausted)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ExchangeDeliveryRequirement {
    DemandDriven,
    MustDrain,
}

/// Authored by actual input exhaustion, not inferred from Task status or ACK.
/// Consumed covers every item through the stream's final EOS sequence, including
/// data carried by that final message (the retained-stream end-after-data case).
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ExchangeCloseDisposition {
    Consumed {
        final_sequence: ExchangeFrameSequence,
    },
    Withdrawn,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ExchangeStreamClose {
    pub stream: ExchangeStreamIdentity,
    pub disposition: ExchangeCloseDisposition,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sequence_exhaustion_and_negative_input_are_not_wrapped() {
        assert_eq!(
            ExchangeFrameSequence::try_new(-1),
            Err(ExchangeFlowContractError::NegativeSequence)
        );
        assert_eq!(
            ExchangeFrameSequence::FIRST.checked_next().unwrap().get(),
            1
        );
        assert_eq!(
            ExchangeFrameSequence::try_new(i64::MAX)
                .unwrap()
                .checked_next(),
            Err(ExchangeFlowContractError::SequenceExhausted)
        );
    }
}
