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

//! Planner-owned physical execution vocabulary.
//!
//! Aggregate phase / TopN phase / join distribution fallback / aggregate output
//! layout, expressed as planner-owned types. These types are intended to support
//! planner-owned `PhysicalPlanNode` payloads without direct
//! `crate::optimizer::*` dependencies as bridge wiring and architecture
//! guards land in follow-up tasks.

use crate::column_id::ColumnId;
use crate::common::OutputColumn;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AggMode {
    Single,
    Local,
    Global,
    /// Dedup by distinct-column + merge non-DISTINCT states (shuffle-receive
    /// phase of 3/4-phase DISTINCT aggregation).
    DistinctGlobal,
    /// Per-instance scalar rollup of DistinctGlobal output (4-phase scalar DISTINCT).
    DistinctLocal,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Default)]
pub enum TopNPhase {
    Partial,
    #[default]
    Final,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum JoinDistribution {
    Unknown,
    Shuffle,
    Broadcast,
    Colocate,
    Singleton,
}

#[derive(Clone, Copy, Debug, Hash, Eq, PartialEq)]
pub enum HashSource {
    ShuffleAgg,
    ShuffleJoin,
    MapGrouping,
    MapSet,
}

impl HashSource {
    pub(crate) const fn exchange_algorithm(
        self,
    ) -> novarocks_type_contract::PartitionHashAlgorithm {
        use novarocks_type_contract::PartitionHashAlgorithm;
        match self {
            Self::ShuffleAgg | Self::ShuffleJoin => PartitionHashAlgorithm::NativeExchangeV1,
            Self::MapGrouping | Self::MapSet => PartitionHashAlgorithm::NativeExchangeV2,
        }
    }
}

#[derive(Clone, Debug)]
pub(crate) struct AggregateOutputLayout {
    pub group_key_columns: Vec<OutputColumn>,
    pub aggregate_columns: Vec<OutputColumn>,
}

impl AggregateOutputLayout {
    pub(crate) fn new(
        group_key_columns: Vec<OutputColumn>,
        aggregate_columns: Vec<OutputColumn>,
    ) -> Self {
        Self {
            group_key_columns,
            aggregate_columns,
        }
    }

    #[allow(
        dead_code,
        reason = "Full aggregate output materialization is retained for physical-plan inspection paths."
    )]
    pub(crate) fn full_output_columns(&self) -> Vec<OutputColumn> {
        self.group_key_columns
            .iter()
            .chain(self.aggregate_columns.iter())
            .cloned()
            .collect()
    }

    #[allow(
        dead_code,
        reason = "Aggregate layout membership checks are retained for physical-plan validation paths."
    )]
    pub(crate) fn contains_column_id(&self, column_id: ColumnId) -> bool {
        self.group_key_columns
            .iter()
            .chain(self.aggregate_columns.iter())
            .any(|column| column.column_id == column_id)
    }
}
