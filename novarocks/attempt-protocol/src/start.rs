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

//! Frozen protocol projection; executable packages and rich payloads stay with
//! their original host records. This module cannot encode or decode a package.

use crate::config::{CoordinationBudgets, FrozenTransportLimits, TaskUpdateRetryPolicy};
use crate::ids::IdSeed;
use novarocks_coordination_contract::AttemptActivation;
use novarocks_execution_contract::{
    AdmissionEpochCapability, ExchangeDeliveryRequirement, ExchangeEdgeId, ParticipationRef,
    PlanNodeId, TaskIdentity,
};
use novarocks_result_contract::PerKind;
use novarocks_types::{BackendProcessId, NativeCompatibilityId, StageId, TaskId, UniqueId};
use std::collections::{BTreeMap, BTreeSet};
use std::num::{NonZeroU64, NonZeroUsize};

/// Opaque original host resource identity. Copying this reference retains no
/// bytes, permits or captures and cannot create a second resource author.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct PayloadRef(pub NonZeroU64);

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct FrozenCreation {
    pub resource: PayloadRef,
    pub queued_bytes: usize,
    pub plan_carrier_bytes: usize,
    pub descriptor_encoded_bytes: usize,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FrozenTask {
    pub identity: TaskIdentity,
    pub participation: ParticipationRef,
    pub task_key: UniqueId,
    pub fragment_id: i32,
    pub split_plan_nodes: Box<[PlanNodeId]>,
    pub creation: FrozenCreation,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FrozenExchangeEdge {
    pub edge: ExchangeEdgeId,
    pub producer_stage: StageId,
    pub consumer_stage: StageId,
    pub destination_node: PlanNodeId,
    pub delivery: ExchangeDeliveryRequirement,
    pub producers: Box<[TaskIdentity]>,
    pub destinations: Box<[TaskIdentity]>,
}

#[derive(Clone, Debug)]
pub struct FrozenTaskGraph {
    pub root: TaskIdentity,
    pub tasks: BTreeMap<TaskId, FrozenTask>,
    pub stages: BTreeMap<StageId, Box<[TaskIdentity]>>,
    pub edges: BTreeMap<ExchangeEdgeId, FrozenExchangeEdge>,
}

#[derive(Clone, Debug)]
pub struct FrozenAttemptPlan {
    pub graph: FrozenTaskGraph,
    pub writers: BTreeSet<TaskIdentity>,
    /// Independent obligations per summary kind; never collapse their union.
    pub summary_sources: PerKind<BTreeSet<TaskIdentity>>,
    pub compatibility: NativeCompatibilityId,
    pub admission_epochs: BTreeMap<BackendProcessId, AdmissionEpochCapability>,
    pub preparation_positions: BTreeMap<BackendProcessId, NonZeroUsize>,
    pub deployment_window: NonZeroUsize,
    /// Frozen original binding material, resolved only by the host executor.
    pub establish_material: BTreeMap<ParticipationRef, PayloadRef>,
    pub coordination: CoordinationBudgets,
    pub transport: FrozenTransportLimits,
    pub retry: TaskUpdateRetryPolicy,
    pub pumps: Box<[(PayloadRef, crate::effect::PumpKind)]>,
    pub initial_lease: std::time::Duration,
    pub steady_lease: std::time::Duration,
}

#[derive(Debug)]
pub struct AttemptStart {
    pub activation: AttemptActivation,
    pub plan: FrozenAttemptPlan,
    pub ids: IdSeed,
}
