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
use crate::input::AttemptSourceId;
use novarocks_coordination_contract::{AttemptActivation, MonotonicInstant};
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

/// Original Establish carrier quote, frozen by its host allocation author.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct FrozenEstablish {
    pub material: PayloadRef,
    pub queued_bytes: usize,
}

#[derive(Clone, Debug)]
pub struct FrozenAttemptPlan {
    pub graph: FrozenTaskGraph,
    /// Same activation's original QA limit, used only to preprice/preallocate
    /// retained authorization refs. It grants no authorization or retry right.
    pub authorization_limits: novarocks_coordination_contract::AttemptAuthorizationLimits,
    pub writers: BTreeSet<TaskIdentity>,
    /// Independent obligations per summary kind; never collapse their union.
    pub summary_sources: PerKind<BTreeSet<TaskIdentity>>,
    pub compatibility: NativeCompatibilityId,
    pub admission_epochs: BTreeMap<BackendProcessId, AdmissionEpochCapability>,
    pub preparation_positions: BTreeMap<BackendProcessId, NonZeroUsize>,
    pub deployment_window: NonZeroUsize,
    /// Frozen original binding material, resolved only by the host executor.
    pub establish_material: BTreeMap<ParticipationRef, FrozenEstablish>,
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
    pub initial_now: MonotonicInstant,
    pub sources: AttemptSources,
    /// Whether the original host registered a development root observer.
    pub observe_dev_root: bool,
}

impl AttemptStart {
    /// Fit the largest indivisible primary input or dispatch continuation.
    /// Each dispatch wake charges its Timer and round-tail entry separately.
    pub fn minimum_core_step_units(&self) -> Option<usize> {
        let batch = self.plan.transport.max_batch_items;
        let dispatch =
            crate::dispatch::BatchDispatchPass::max_batch_units(batch)?.checked_add(2)?;
        Some(
            dispatch
                .max(batch.checked_mul(2)?)
                .max(self.plan.establish_material.len())
                .max(3),
        )
    }
}

/// Fixed roles from the original actor registration. These identities and
/// mailbox geometry are metadata, with no admission or access authority.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct AttemptSources {
    pub actor: AttemptSourceId,
    pub publication: AttemptSourceId,
    pub qa: AttemptSourceId,
    pub process: AttemptSourceId,
    pub native: AttemptSourceId,
    pub observation: AttemptSourceId,
    pub capacity: AttemptSourceId,
    pub membership: AttemptSourceId,
    pub mailbox: [usize; 4],
}
