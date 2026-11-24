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

//! What deciding how many tasks to run, and where, reads about one plan.
//!
//! Scheduling reads no semantics. It reads which fragments exist, in what
//! order, which of them read a provider and how much work that read starts
//! with, and how rows flow between fragments. Every one of those is a
//! property both plan representations have, so they are stated here as
//! values rather than reached for through whichever representation produced
//! them.

use novarocks_execution_contract::ExchangeEdgeId;
use novarocks_physical_plan::PlanVersionId;

use super::NativeScanWork;

/// Which plan a scan belongs to.
///
/// Two plan versions' scans must never join to each other.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum PlanSeal {
    Version(PlanVersionId),
}

/// One provider read of one plan, as scheduling and work accounting name it.
///
/// It is a join key, not an address. The only thing done with it is to match
/// a scan the plan declared against the work that scan was given, and the
/// seal is what makes a mismatch impossible to express rather than merely
/// unlikely.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct PlanScanIdentity {
    plan: PlanSeal,
    fragment_id: u32,
    node_id: i32,
}

impl PlanScanIdentity {
    pub const fn new(plan: PlanSeal, fragment_id: u32, node_id: i32) -> Self {
        Self {
            plan,
            fragment_id,
            node_id,
        }
    }

    pub const fn plan(self) -> PlanSeal {
        self.plan
    }

    pub const fn fragment_id(self) -> u32 {
        self.fragment_id
    }

    pub const fn node_id(self) -> i32 {
        self.node_id
    }
}

/// One provider read and the work it starts with.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ScanSchedulingFacts {
    pub scan: PlanScanIdentity,
    pub work: NativeScanWork,
}

/// One fragment, as scheduling reads it.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FragmentSchedulingFacts {
    pub fragment_id: u32,
    pub placement: novarocks_physical_plan::StagePlacementPolicy,
    pub scans: Vec<ScanSchedulingFacts>,
}

/// How rows move across one fragment boundary.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SchedulingStreamKind {
    Gather,
    Broadcast,
    Partitioned,
    Other,
}

/// One exchange edge, as scheduling reads it.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct SchedulingEdgeFacts {
    pub edge_id: ExchangeEdgeId,
    pub source_fragment_id: u32,
    pub target_fragment_id: u32,
    pub target_exchange_node_id: i32,
    pub stream_kind: SchedulingStreamKind,
    pub hash_partitioned: bool,
}

/// Everything scheduling reads about one plan.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ExecutionSchedulingFacts {
    pub topological_fragment_order: Vec<u32>,
    pub execution_anchor_fragment_id: u32,
    pub fragments: Vec<FragmentSchedulingFacts>,
    pub edges: Vec<SchedulingEdgeFacts>,
}
