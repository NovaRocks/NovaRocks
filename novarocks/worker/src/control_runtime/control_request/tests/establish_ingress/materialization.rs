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

//! Real Node M/R lifetime boundaries. The shared Node fixture starts before
//! the first RPC and retains its original rows through complete physical exit.
mod node_flow;
mod node_task_flow;

use super::*;
use crate::{
    HostRejection, ParticipationHostFactory, ParticipationHostInstance,
    ReleasedParticipationEvidence, SharedFactsRequest,
    control_runtime::{
        participation_executors::{ParticipationExecutorStartRefusal, ParticipationExecutors},
        participation_host::PARTICIPATION_BOOTSTRAP_STORAGE,
    },
    control_runtime_policy::{ControlRuntimePolicy, IngressPositionLimits},
};
use novarocks_execution_contract::ParticipationState;
use novarocks_worker_protocol::{
    lease::LeaseBounds,
    participation::{ParticipationConfig, ParticipationCore},
};
use std::{
    sync::{Condvar, Mutex},
    time::Instant,
};

fn participation_actor() -> ActorId {
    ActorId::new(4).unwrap()
}
const BRIDGE_STORAGE: StorageNeed = StorageNeed {
    inputs: 1,
    batches: 1,
    effects: crate::control_runtime::actors::ParticipationActor::MAX_STEP_ACTIONS,
    work: 1,
    captures: 1,
    requests: 0,
    cuts: 0,
    replies: 0,
    completions: 0,
};
fn start_original_participation_executors(
    f: &mut Fixture,
) -> Result<ParticipationExecutors, ParticipationExecutorStartRefusal> {
    let one = NonZeroUsize::new(1).unwrap();
    // One physical Participation M job per simultaneously held Establish R; the
    // remaining original R position belongs to the control request.
    let jobs = NonZeroUsize::new(f.positions.checked_sub(1).unwrap()).unwrap();
    let limits = crate::control_runtime::host_executor::HostExecutorLimits {
        jobs,
        per_group: one,
        workers: one,
    };
    let capacity = f.root.task_capacity_signal().unwrap();
    f.root
        .service()
        .start_participation_executors(limits, limits, capacity)
}

#[test]
fn the_same_instance_waits_for_materialization_exit_while_its_control_plane_closes() {
    node_flow::blocked_materialization_keeps_control_live();
}
#[test]
fn accepted_creator_retires_its_original_rpc_while_active_participation_keeps_the_credential_holder()
 {
    node_flow::accepted_creator_retains_only_participation_custody();
}
#[test]
fn a_core_release_before_queued_materialization_never_calls_factory_and_retires_the_original_roles()
{
    node_flow::queued_materialization_withdraws_without_factory();
}
