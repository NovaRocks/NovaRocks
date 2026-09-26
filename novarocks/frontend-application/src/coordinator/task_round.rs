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
use novarocks_execution::task_execution::AdmissionEpochCapability;
use novarocks_query_application::coordination::DispatchBudget;
use novarocks_task_codec::TransportBudget;
use novarocks_types::identity::{BackendProcessId, FrontendProcessId, QueryExecutionId};

use crate::native::data_runtime::FrontendDataRuntime;
use crate::native::task_transport::{
    AttemptWireFacts, CoveredTaskStatusSubscriber, NativeTaskOperationSink, TaskAckIntake,
};
use crate::query_execution::artifact::ValidatedNativeSubmission;
use crate::query_execution::schedule::SchedulingPlan;
use crate::runtime_filter::feedback::RuntimeFilterFeedbackState;
use crate::task_execution::clock::{ProcessMonotonicClock, TaskProtocolClock};
use crate::task_execution::error::TaskExecutionError;
use crate::task_execution::execution::QueryTaskExecution;
use crate::task_execution::feedback_pump::{DynamicFilterFeedbackPump, TaskDynamicFilterReads};
use crate::task_execution::graph::{TaskGraphInputs, build_task_graph};
use crate::task_execution::intent::TaskOperationSink;
use crate::task_execution::round::{AcknowledgementObserver, TaskRound};
use crate::task_execution::sources::{AttemptEstablishFacts, SubmissionFragmentPlans};
use crate::task_execution::split_transport::SplitDeliveryBridge;
use crate::task_execution::status_intake::{ObservationIntake, StatusIntake, StatusIntakeWake};

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

/// Everything the two per-attempt feedback loops are built from.
///
/// It is one struct rather than seven parameters so the one call site cannot
/// silently drop a fact by reordering: every field here is a loop's only
/// source, and a loop with a missing source is a loop that never runs.
pub(crate) struct AttemptPumps {
    pub(crate) execution_id: QueryExecutionId,
    pub(crate) feedback_state: Arc<RuntimeFilterFeedbackState>,
    /// How many feedback channels the sealed filter deployment declared.
    pub(crate) declared_feedback_channels: usize,
    pub(crate) reads: Arc<dyn TaskDynamicFilterReads>,
}

/// Installs one attempt's per-turn owners and declares the set complete.
///
/// One loop now, not two. The credential rotation owner used to be installed
/// here and returned so the caller could stop it during the drain; a consumer
/// that renews for itself has nothing for a coordinator turn to drive, so
/// there is nothing to stop (CAD-1 D3).
pub(crate) fn install_attempt_pumps(round: &mut TaskRound, pumps: AttemptPumps) {
    // The dynamic filter reader closes the loop the connector split sources
    // are already waiting on. Without it every channel stays pending, each
    // source waits out its own initial cap and then enumerates unpruned --
    // which changes pruning and latency but no rows, so no result assertion
    // anywhere can see its absence.
    if let Some(pump) = DynamicFilterFeedbackPump::new(
        pumps.feedback_state,
        pumps.reads,
        pumps.declared_feedback_channels,
    ) {
        round.add_pump(Box::new(pump));
    }
    round.seal_pumps();
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
    edges: &[crate::query_execution::attempt_plan_facts::AttemptEdgeFacts],
    backend_process_ids: &BTreeMap<usize, BackendProcessId>,
    admission_epochs: &BTreeMap<BackendProcessId, AdmissionEpochCapability>,
    preparing_positions: &BTreeMap<BackendProcessId, usize>,
    backends: &[(BackendProcessId, RuntimeEndpoint)],
    submissions: Vec<ValidatedNativeSubmission>,
    establish: AttemptEstablishFacts,
    wake: Arc<dyn StatusIntakeWake>,
    transport: AttemptTransport,
) -> Result<AssembledRound, TaskExecutionError> {
    let mut plans = SubmissionFragmentPlans::index(submissions, schedule)?;
    let graph = build_task_graph(
        TaskGraphInputs::from_schedule(
            execution_id,
            frontend_process_id,
            schedule,
            edges,
            backend_process_ids,
            transport.transport,
        ),
        &mut plans,
    )?;

    // Taken while the graph is still whole: the bridge resolves the driver's
    // kernel-key addresses, and the substrate takes the graph's creation
    // seeds away in `QueryTaskExecution::new`.
    let split_delivery = SplitDeliveryBridge::for_graph(&graph);
    let connector_blocking_io = transport.data_runtime.connector_blocking_io().clone();

    let acks = TaskAckIntake::new(Arc::clone(&wake));
    let native_compatibility_id = transport.attempt.native_compatibility_id;
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
    let clock = Arc::new(ProcessMonotonicClock::new()) as Arc<dyn TaskProtocolClock>;
    let observation = Arc::new(
        ObservationIntake::for_task_attempt(Arc::clone(&wake), Arc::clone(&clock)).map_err(
            |error| {
                TaskExecutionError::Schedule(format!(
                    "covered observation capacity is invalid: {error:?}"
                ))
            },
        )?,
    );
    let subscriber = Arc::new(
        CoveredTaskStatusSubscriber::new(
            backends,
            Arc::clone(&observation),
            transport.status_subscription_error_budget,
            transport.data_runtime,
        )
        .map_err(TaskExecutionError::Schedule)?,
    );

    let execution = QueryTaskExecution::new(
        graph,
        transport.budget,
        transport.transport,
        native_compatibility_id,
        admission_epochs,
        preparing_positions,
        clock,
        sink,
        intake,
    )?;

    let round = TaskRound::new_covered(
        execution,
        acks,
        Box::new(establish),
        subscriber,
        observation,
    )
    .observing(Arc::clone(&split_delivery) as Arc<dyn AcknowledgementObserver>)
    .with_connector_blocking_io(connector_blocking_io);
    Ok(AssembledRound {
        round,
        split_delivery,
    })
}
