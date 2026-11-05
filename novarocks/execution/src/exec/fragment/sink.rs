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

use crate::runtime::endpoint::TaskDestination;
use crate::runtime::task::{ExecutionFailure, ExecutionResult};
pub use novarocks_execution_contract::DataStreamPartitionType;
use novarocks_types::SlotId;
use std::collections::HashSet;

/// Construction-time input for a distributed stream sink. The private
/// operator factories consume this value but do not define the public
/// fragment-construction contract.
#[derive(Clone)]
pub struct DataStreamSinkFactoryInput {
    pub dest_node_id: i32,
    pub delivery: novarocks_execution_contract::ExchangeDeliveryRequirement,
    pub output_partition_type: DataStreamPartitionType,
    pub output_columns: Vec<SlotId>,
    pub destinations: Vec<TaskDestination>,
}

impl DataStreamSinkFactoryInput {
    pub fn try_from_static_program(
        dest_node_id: i32,
        output_partition_type: DataStreamPartitionType,
        delivery: novarocks_execution_contract::ExchangeDeliveryRequirement,
        output_columns: Vec<SlotId>,
        destinations: Vec<TaskDestination>,
    ) -> ExecutionResult<Self> {
        let mut seen = HashSet::new();
        if let Some(slot_id) = output_columns
            .iter()
            .find(|slot_id| !seen.insert(**slot_id))
        {
            return Err(ExecutionFailure::from_domain(
                SinkInputFailure::DuplicateSlot(*slot_id),
            ));
        }
        Ok(Self {
            dest_node_id,
            output_partition_type,
            delivery,
            output_columns,
            destinations,
        })
    }

    pub fn try_new(
        dest_node_id: i32,
        output_partition_type: DataStreamPartitionType,
        delivery: novarocks_execution_contract::ExchangeDeliveryRequirement,
        output_columns: Vec<i32>,
        destinations: Vec<TaskDestination>,
    ) -> ExecutionResult<Self> {
        let mut seen = HashSet::new();
        let mut parsed_output_columns = Vec::with_capacity(output_columns.len());
        for raw in output_columns {
            let slot_id = SlotId::new(u32::try_from(raw).map_err(|source| {
                ExecutionFailure::from_domain(SinkInputFailure::InvalidSlot { raw, source })
            })?);
            if !seen.insert(slot_id) {
                return Err(ExecutionFailure::from_domain(
                    SinkInputFailure::DuplicateSlot(slot_id),
                ));
            }
            parsed_output_columns.push(slot_id);
        }
        Self::try_from_static_program(
            dest_node_id,
            output_partition_type,
            delivery,
            parsed_output_columns,
            destinations,
        )
    }
}

#[derive(Debug)]
enum SinkInputFailure {
    DuplicateSlot(SlotId),
    InvalidSlot {
        raw: i32,
        source: std::num::TryFromIntError,
    },
}
impl std::fmt::Display for SinkInputFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::DuplicateSlot(slot) => write!(
                f,
                "DATA_STREAM_SINK: duplicate output_columns slot id: {slot}"
            ),
            Self::InvalidSlot { raw, .. } => write!(
                f,
                "DATA_STREAM_SINK: invalid output_columns slot id: invalid slot id: {raw}"
            ),
        }
    }
}
impl std::error::Error for SinkInputFailure {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::InvalidSlot { source, .. } => Some(source),
            _ => None,
        }
    }
}
impl novarocks_failure_contract::FailureProjection for SinkInputFailure {
    fn failure_fact(&self) -> novarocks_failure_contract::FailureFact {
        novarocks_failure_contract::FailureFact::new(
            novarocks_failure_contract::FailureClass::Invariant,
            crate::runtime::task::ExecutionReason::PlanSinkInvariant.reason(),
        )
    }
}

#[cfg(test)]
mod tests {
    use super::DataStreamPartitionType;
    #[test]
    fn negative_stream_slot_preserves_the_actual_integer_failure() {
        use novarocks_failure_contract::{FailureClass, FailureProjection};
        use std::error::Error;
        let failure = super::DataStreamSinkFactoryInput::try_new(
            17,
            DataStreamPartitionType::Unpartitioned,
            novarocks_execution_contract::ExchangeDeliveryRequirement::DemandDriven,
            vec![-1],
            vec![],
        )
        .err()
        .expect("negative slot must fail construction");
        assert_eq!(failure.failure_fact().class(), FailureClass::Invariant);
        let crate::runtime::task::ExecutionFailureCause::Domain(source) = failure.cause() else {
            panic!("construction must retain its nominal source");
        };
        let source = source.downcast_ref::<super::SinkInputFailure>().unwrap();
        assert!(matches!(
            source,
            super::SinkInputFailure::InvalidSlot { raw: -1, .. }
        ));
        assert!(source.source().unwrap().is::<std::num::TryFromIntError>());
    }
}
