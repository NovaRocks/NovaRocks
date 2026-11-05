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

//! Native rendering of already-decided Worker task lifecycle facts.
//!
//! The Worker remains the only task/participation lifecycle owner. This adapter
//! renders its settled facts as role-local evidence, metrics, and result-buffer
//! cleanup without gaining a second transition or admission authority.

use std::sync::Arc;

use novarocks_execution_contract::task_execution::identity::{ParticipationRef, TaskIdentity};
use novarocks_worker::{
    RuntimeFilterReleaseObservation, TaskExecutionMetrics, TaskExecutionPorts, TaskProtocolEvent,
    TaskProtocolObserver, TaskResultLifecycle,
};

#[derive(Debug)]
struct NativeTaskExecutionPorts {
    #[cfg(novarocks_dev_controls)]
    observations: Option<Arc<crate::test_observation_controls::NativeObservationControls>>,
}

impl TaskProtocolObserver for NativeTaskExecutionPorts {
    fn observe(&self, event: TaskProtocolEvent) {
        #[cfg(novarocks_dev_controls)]
        if let Some(controls) = self.observations.as_deref() {
            emit(controls, event);
        }
        #[cfg(not(novarocks_dev_controls))]
        let _ = event;
    }
}

impl TaskResultLifecycle for NativeTaskExecutionPorts {
    fn discard_task(&self, identity: TaskIdentity) {
        novarocks_worker::result_buffer::discard_task(identity);
    }

    fn retire_task_result(&self, identity: TaskIdentity) {
        novarocks_worker::result_buffer::retire_task_result(identity);
    }
}

impl TaskExecutionMetrics for NativeTaskExecutionPorts {
    fn record_task_created(&self) {
        crate::backend_metrics::record_task_execution_task_created();
    }
}

/// Builds the Native role-local effects injected into the Worker task owner.
pub fn backend_task_execution_ports(
    execution_health: Arc<novarocks_execution::runtime::execution_health::ExecutionHealth>,
    #[cfg(novarocks_dev_controls)] observations: Option<
        Arc<crate::test_observation_controls::NativeObservationControls>,
    >,
) -> TaskExecutionPorts {
    let adapter = Arc::new(NativeTaskExecutionPorts {
        #[cfg(novarocks_dev_controls)]
        observations,
    });
    let observer: Arc<dyn TaskProtocolObserver> = adapter.clone();
    let result_lifecycle: Arc<dyn TaskResultLifecycle> = adapter.clone();
    let metrics: Arc<dyn TaskExecutionMetrics> = adapter;
    TaskExecutionPorts::new(observer, result_lifecycle, metrics, execution_health)
}

#[cfg(novarocks_dev_controls)]
fn emit_participation(
    observer: &dyn novarocks_execution::runtime::exchange::TestEventObserver,
    name: &str,
    participation: ParticipationRef,
) {
    emit_participation_with(observer, name, participation, "");
}

#[cfg(novarocks_dev_controls)]
fn emit_participation_with(
    observer: &dyn novarocks_execution::runtime::exchange::TestEventObserver,
    name: &str,
    participation: ParticipationRef,
    extra: &str,
) {
    let execution = participation.query_execution_id();
    let separator = if extra.is_empty() { "" } else { " " };
    observer.observe(&format!(
        "{name} execution_id={}:{}:{} frontend={} backend={}{separator}{extra}",
        execution.query_id().high(),
        execution.query_id().low(),
        execution.attempt_id().get(),
        participation.frontend_process_id(),
        participation.backend_process_id(),
    ));
}

#[cfg(novarocks_dev_controls)]
fn emit_task(
    observer: &dyn novarocks_execution::runtime::exchange::TestEventObserver,
    name: &str,
    identity: TaskIdentity,
) {
    let execution = identity.query_execution_id();
    observer.observe(&format!(
        "{name} execution_id={}:{}:{} stage={} task={} backend={}",
        execution.query_id().high(),
        execution.query_id().low(),
        execution.attempt_id().get(),
        identity.stage_id().get(),
        identity.task_id().get(),
        identity.backend_process_id(),
    ));
}

/// Test-only observation of the exact effective DOP admitted with a task.
/// The execution kernel uses this positive descriptor value unchanged as its
/// base graph DOP; this marker carries no scheduling authority.
#[cfg(novarocks_dev_controls)]
pub(crate) fn emit_prepared_task_dop(
    controls: Option<&crate::test_observation_controls::NativeObservationControls>,
    identity: TaskIdentity,
    task_key: novarocks_types::UniqueId,
    dop: usize,
) {
    let Some(controls) = controls else { return };
    let Some(observer) = task_observer(controls, identity) else {
        return;
    };
    let execution = identity.query_execution_id();
    observer.observe(&format!(
        "{} execution_id={}:{}:{} stage={} task={} backend={} task_key={:x}:{:x} dop={dop}",
        novarocks_failpoint::observation_marker::TASK_PREPARED_DOP,
        execution.query_id().high(),
        execution.query_id().low(),
        execution.attempt_id().get(),
        identity.stage_id().get(),
        identity.task_id().get(),
        identity.backend_process_id(),
        task_key.high(),
        task_key.low(),
    ));
}

/// Test-only identity of a successfully prepared ClientRows root. This
/// renders the exact bound channel contract, with no installation authority.
#[cfg(novarocks_dev_controls)]
pub(crate) fn emit_prepared_client_root(
    controls: Option<&crate::test_observation_controls::NativeObservationControls>,
    identity: TaskIdentity,
    contract: &novarocks_result_contract::RootOutputContract,
) {
    let Some(controls) = controls else { return };
    let Some(observer) = task_observer(controls, identity) else {
        return;
    };
    if contract.profile() == novarocks_result_contract::RootProfileId::V1
        && contract.kind() == novarocks_result_contract::RootOutputKind::ClientRows
    {
        emit_task(
            observer.as_ref(),
            novarocks_failpoint::observation_marker::TASK_PREPARED_CLIENT_ROOT,
            identity,
        );
    }
}

#[cfg(novarocks_dev_controls)]
fn emit(
    controls: &crate::test_observation_controls::NativeObservationControls,
    event: TaskProtocolEvent,
) {
    let (execution, process) = match &event {
        TaskProtocolEvent::TaskCreateApplied { identity }
        | TaskProtocolEvent::TaskCreateIdempotent { identity }
        | TaskProtocolEvent::TaskTerminalRetained { identity, .. } => {
            (identity.query_execution_id(), identity.backend_process_id())
        }
        TaskProtocolEvent::ParticipationEstablishApplied { participation }
        | TaskProtocolEvent::ParticipationEstablishIdempotent { participation }
        | TaskProtocolEvent::ParticipationEstablishConflict { participation }
        | TaskProtocolEvent::LeaseRenewed { participation, .. }
        | TaskProtocolEvent::ParticipationReleaseApplied { participation, .. }
        | TaskProtocolEvent::ParticipationLeaseExpired { participation }
        | TaskProtocolEvent::ParticipationTerminationCompleted { participation, .. }
        | TaskProtocolEvent::ParticipationAbortApplied { participation } => (
            participation.query_execution_id(),
            participation.backend_process_id(),
        ),
    };
    if !controls.owns_backend(process) {
        return;
    }
    let Some(observer) = resolve_observer(controls, execution) else {
        return;
    };
    match event {
        TaskProtocolEvent::ParticipationEstablishApplied { participation } => {
            emit_participation(
                observer.as_ref(),
                novarocks_failpoint::observation_marker::PARTICIPATION_ESTABLISH_APPLIED,
                participation,
            );
        }
        TaskProtocolEvent::ParticipationEstablishIdempotent { participation } => {
            emit_participation(
                observer.as_ref(),
                novarocks_failpoint::observation_marker::PARTICIPATION_ESTABLISH_IDEMPOTENT,
                participation,
            );
        }
        TaskProtocolEvent::ParticipationEstablishConflict { participation } => {
            emit_participation(
                observer.as_ref(),
                novarocks_failpoint::observation_marker::PARTICIPATION_ESTABLISH_CONFLICT,
                participation,
            );
        }
        TaskProtocolEvent::LeaseRenewed {
            participation,
            sequence,
        } => {
            let execution = participation.query_execution_id();
            observer.observe(&format!(
                "{} execution_id={}:{}:{} frontend={} backend={} sequence={}",
                novarocks_failpoint::observation_marker::PARTICIPATION_LEASE_RENEWED,
                execution.query_id().high(),
                execution.query_id().low(),
                execution.attempt_id().get(),
                participation.frontend_process_id(),
                participation.backend_process_id(),
                sequence.get(),
            ));
        }
        TaskProtocolEvent::TaskCreateApplied { identity } => emit_task(
            observer.as_ref(),
            novarocks_failpoint::observation_marker::TASK_CREATE_APPLIED,
            identity,
        ),
        TaskProtocolEvent::TaskCreateIdempotent { identity } => emit_task(
            observer.as_ref(),
            novarocks_failpoint::observation_marker::TASK_CREATE_IDEMPOTENT,
            identity,
        ),
        TaskProtocolEvent::ParticipationReleaseApplied {
            participation,
            runtime_filter,
        } => {
            let runtime_filter = match runtime_filter {
                RuntimeFilterReleaseObservation::Absent => "absent",
                RuntimeFilterReleaseObservation::Available => "available",
                RuntimeFilterReleaseObservation::Unavailable => "unavailable",
            };
            emit_participation_with(
                observer.as_ref(),
                novarocks_failpoint::observation_marker::PARTICIPATION_RELEASE_APPLIED,
                participation,
                &format!("runtime_filter={runtime_filter}"),
            );
        }
        TaskProtocolEvent::ParticipationLeaseExpired { participation } => {
            emit_participation(
                observer.as_ref(),
                novarocks_failpoint::observation_marker::PARTICIPATION_LEASE_EXPIRED,
                participation,
            );
        }
        TaskProtocolEvent::ParticipationTerminationCompleted {
            participation,
            cause,
            retained_tasks,
        } => {
            let execution = participation.query_execution_id();
            observer.observe(&format!(
                "{} execution_id={}:{}:{} frontend={} backend={} cause={} retained_tasks={retained_tasks}", novarocks_failpoint::observation_marker::PARTICIPATION_TERMINATION_COMPLETED,
                execution.query_id().high(),
                execution.query_id().low(),
                execution.attempt_id().get(),
                participation.frontend_process_id(),
                participation.backend_process_id(),
                cause,
            ));
        }
        TaskProtocolEvent::TaskTerminalRetained { identity, state } => {
            let execution = identity.query_execution_id();
            observer.observe(&format!(
                "{} execution_id={}:{}:{} stage={} task={} backend={} state={state}",
                novarocks_failpoint::observation_marker::TASK_TERMINAL_RETAINED,
                execution.query_id().high(),
                execution.query_id().low(),
                execution.attempt_id().get(),
                identity.stage_id().get(),
                identity.task_id().get(),
                identity.backend_process_id(),
            ));
        }
        TaskProtocolEvent::ParticipationAbortApplied { participation } => {
            let execution = participation.query_execution_id();
            observer.observe(&format!(
                "{} execution_id={}:{}:{} frontend={} backend={}",
                novarocks_failpoint::observation_marker::PARTICIPATION_ABORT_APPLIED,
                execution.query_id().high(),
                execution.query_id().low(),
                execution.attempt_id().get(),
                participation.frontend_process_id(),
                participation.backend_process_id(),
            ));
        }
    }
}

#[cfg(novarocks_dev_controls)]
fn resolve_observer(
    controls: &crate::test_observation_controls::NativeObservationControls,
    execution: novarocks_types::QueryExecutionId,
) -> Option<Arc<dyn novarocks_execution::runtime::exchange::TestEventObserver>> {
    match controls.observation_for(
        novarocks_failpoint::observation_marker::ObservationAction::TaskProtocol.as_str(),
        execution,
    ) {
        Ok(observer) => observer,
        Err(error) => {
            eprintln!("Native task observation rejected: {error}");
            None
        }
    }
}
#[cfg(novarocks_dev_controls)]
fn task_observer(
    controls: &crate::test_observation_controls::NativeObservationControls,
    identity: TaskIdentity,
) -> Option<Arc<dyn novarocks_execution::runtime::exchange::TestEventObserver>> {
    if !controls.owns_backend(identity.backend_process_id()) {
        return None;
    }
    resolve_observer(controls, identity.query_execution_id())
}
