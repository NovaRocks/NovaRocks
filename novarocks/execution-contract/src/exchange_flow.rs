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

// Existing stream consumers use the same nonzero ordered-unit domain and
// source-authored close disposition as the receiver receipt kernel.
pub use crate::ordered_retained_stream::exchange::{
    ExchangeUnitCloseDisposition as ExchangeCloseDisposition,
    ExchangeUnitSequence as ExchangeFrameSequence,
};
use crate::{ExchangeSource, FragmentNodeId, TaskIdentity};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ExchangeFlowContractError {
    CrossAttemptStream,
    NegativeDestinationNode,
}

impl fmt::Display for ExchangeFlowContractError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::CrossAttemptStream => "exchange stream crosses query attempts",
            Self::NegativeDestinationNode => "exchange destination node must be nonnegative",
        })
    }
}
impl std::error::Error for ExchangeFlowContractError {}

/// One source-to-input stream; endpoint reuse never changes its identity.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct ExchangeStreamIdentity {
    source: ExchangeSource,
    destination: TaskIdentity,
    destination_task_key: UniqueId,
    destination_node: FragmentNodeId,
}

impl ExchangeStreamIdentity {
    pub fn try_new(
        source: ExchangeSource,
        destination: TaskIdentity,
        destination_task_key: UniqueId,
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
            destination_task_key,
            destination_node,
        })
    }
    pub const fn source(self) -> ExchangeSource {
        self.source
    }
    pub const fn destination(self) -> TaskIdentity {
        self.destination
    }
    pub const fn destination_task_key(self) -> UniqueId {
        self.destination_task_key
    }
    pub const fn destination_node(self) -> FragmentNodeId {
        self.destination_node
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ExchangeDeliveryRequirement {
    DemandDriven,
    MustDrain,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ExchangeStreamClose {
    pub stream: ExchangeStreamIdentity,
    pub disposition: ExchangeCloseDisposition,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ordered_retained_stream::exchange::ExchangeReceiptError;

    #[test]
    fn sequence_exhaustion_and_zero_input_are_not_wrapped() {
        assert_eq!(ExchangeFrameSequence::FIRST.get(), 1);
        assert_eq!(
            ExchangeFrameSequence::try_new(0),
            Err(ExchangeReceiptError::ZeroSequence)
        );
        assert_eq!(
            ExchangeFrameSequence::FIRST.checked_next().unwrap().get(),
            2
        );
        assert_eq!(
            ExchangeFrameSequence::try_new(u64::MAX)
                .unwrap()
                .checked_next(),
            Err(ExchangeReceiptError::SequenceOverflow)
        );
    }
}
