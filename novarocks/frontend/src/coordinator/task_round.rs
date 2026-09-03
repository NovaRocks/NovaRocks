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

//! Assembling one attempt's task protocol from its frozen plan.
//!
//! Everything here is address translation and ownership handover: the schedule
//! decided where each fragment instance runs, the encoder produced its plan,
//! and this turns those into the graph, the transport and the runner that
//! drive them. It decides nothing about placement, sequencing or completion.

use std::collections::BTreeMap;
use std::sync::Arc;

use novarocks_execution::runtime::endpoint::RuntimeEndpoint;
use novarocks_execution::task_execution::{DispatchBudget, TransportBudget};
use novarocks_sql::plan_read::FragmentEdge;
use novarocks_types::identity::{BackendProcessId, FrontendProcessId, QueryExecutionId};

use crate::native::data_runtime::FrontendDataRuntime;
use crate::native::task_transport::{
    AttemptWireFacts, NativeTaskOperationSink, TaskAckIntake, TaskStatusSubscriber,
};
use crate::query_execution::artifact::ValidatedNativeSubmission;
use crate::query_execution::schedule::SchedulingPlan;
use crate::task_execution::clock::ProcessMonotonicClock;
use crate::task_execution::error::TaskExecutionError;
use crate::task_execution::execution::QueryTaskExecution;
use crate::task_execution::graph::{TaskGraphInputs, build_task_graph};
use crate::task_execution::intent::TaskOperationSink;
use crate::task_execution::round::{AcknowledgementObserver, StatusSubscriptions, TaskRound};
use crate::task_execution::sources::{AttemptEstablishFacts, SubmissionFragmentPlans};
use crate::task_execution::split_transport::SplitDeliveryBridge;
use crate::task_execution::status_intake::{StatusIntake, StatusIntakeWake};

/// How many status events one attempt may hold before the transport is told it
/// lost observations.
///
/// The runner folds a bounded number per turn, so this only has to absorb a
/// burst between turns rather than a whole attempt's history.
const STATUS_INTAKE_CAPACITY: usize = 4096;

/// Everything one attempt needs to run its tasks.
pub(crate) struct AttemptTransport {
    pub(crate) budget: DispatchBudget,
    pub(crate) transport: TransportBudget,
    pub(crate) status_subscription_error_budget: u32,
    pub(crate) attempt: AttemptWireFacts,
    pub(crate) data_runtime: FrontendDataRuntime,
}

/// One assembled attempt: its runner and the split-delivery bridge that shares
/// its substrate.
///
/// The bridge is returned rather than hidden inside the runner because its two
/// halves belong to different threads: the runner drains its owner half on its
/// own turn, while the split-assignment worker blocks on the transport half.
pub(crate) struct AssembledRound {
    pub(crate) round: TaskRound,
    pub(crate) split_delivery: Arc<SplitDeliveryBridge>,
}

/// Builds the runner for one attempt.
///
/// The backend set is frozen here and shared by the operation sink and the
/// status subscriber, so an operation and the subscription that observes its
/// task can never be addressed to different processes.
pub(crate) fn assemble_round(
    execution_id: QueryExecutionId,
    frontend_process_id: FrontendProcessId,
    schedule: &SchedulingPlan,
    edges: &[FragmentEdge],
    backend_process_ids: &BTreeMap<usize, BackendProcessId>,
    backends: &[(BackendProcessId, RuntimeEndpoint)],
    submissions: Vec<ValidatedNativeSubmission>,
    establish: AttemptEstablishFacts,
    wake: Arc<dyn StatusIntakeWake>,
    transport: AttemptTransport,
) -> Result<AssembledRound, TaskExecutionError> {
    let plans = SubmissionFragmentPlans::index(submissions, schedule)?;
    let graph = build_task_graph(
        TaskGraphInputs::from_schedule(
            execution_id,
            frontend_process_id,
            schedule,
            edges,
            backend_process_ids,
            transport.transport,
        ),
        &plans,
    )?;

    // Taken while the graph is still whole: the bridge resolves the driver's
    // kernel-key addresses, and the substrate takes the graph's descriptors
    // away in `QueryTaskExecution::new`.
    let split_delivery = SplitDeliveryBridge::for_graph(&graph);

    let acks = TaskAckIntake::new(Arc::clone(&wake));
    let sink = NativeTaskOperationSink::new(
        backends,
        transport.transport,
        transport.attempt,
        acks.handle(),
        transport.data_runtime.clone(),
    )
    .map_err(TaskExecutionError::Schedule)?;
    let sink = split_delivery.sink(Arc::new(sink) as Arc<dyn TaskOperationSink>);

    let intake = StatusIntake::new(STATUS_INTAKE_CAPACITY, Arc::clone(&wake));
    let subscriber = Arc::new(
        TaskStatusSubscriber::new(
            backends,
            intake.handle(),
            transport.status_subscription_error_budget,
            transport.data_runtime,
        )
        .map_err(TaskExecutionError::Schedule)?,
    );

    let execution = QueryTaskExecution::new(
        graph,
        transport.budget,
        transport.transport,
        Arc::new(ProcessMonotonicClock::new()),
        sink,
        intake,
    )?;

    let round = TaskRound::new(
        execution,
        acks,
        Box::new(establish),
        subscriber as Arc<dyn StatusSubscriptions>,
    )
    .observing(Arc::clone(&split_delivery) as Arc<dyn AcknowledgementObserver>);
    Ok(AssembledRound {
        round,
        split_delivery,
    })
}
