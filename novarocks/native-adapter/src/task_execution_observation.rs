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
//! The Worker remains the only task/context lifecycle owner. This adapter
//! renders its settled facts as role-local evidence, metrics, and result-buffer
//! cleanup without gaining a second transition or admission authority.

use std::sync::Arc;

use novarocks_execution_contract::task_execution::identity::{QueryContextRef, TaskIdentity};
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
fn emit_context(
    observer: &dyn novarocks_execution::runtime::exchange::TestEventObserver,
    name: &str,
    context: QueryContextRef,
) {
    emit_context_with(observer, name, context, "");
}

#[cfg(novarocks_dev_controls)]
fn emit_context_with(
    observer: &dyn novarocks_execution::runtime::exchange::TestEventObserver,
    name: &str,
    context: QueryContextRef,
    extra: &str,
) {
    let execution = context.query_execution_id();
    let separator = if extra.is_empty() { "" } else { " " };
    observer.observe(&format!(
        "{name} execution_id={}:{}:{} frontend={} backend={}{separator}{extra}",
        execution.query_id().high(),
        execution.query_id().low(),
        execution.attempt_id().get(),
        context.frontend_process_id(),
        context.backend_process_id(),
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
    fragment_instance_id: novarocks_types::UniqueId,
    dop: usize,
) {
    let Some(controls) = controls else { return };
    let Some(observer) = task_observer(controls, identity) else {
        return;
    };
    let execution = identity.query_execution_id();
    observer.observe(&format!(
        "NOVAROCKS_TASK_PREPARED_DOP execution_id={}:{}:{} stage={} task={} backend={} finst={:x}:{:x} dop={dop}",
        execution.query_id().high(),
        execution.query_id().low(),
        execution.attempt_id().get(),
        identity.stage_id().get(),
        identity.task_id().get(),
        identity.backend_process_id(),
        fragment_instance_id.high(),
        fragment_instance_id.low(),
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
            "NOVAROCKS_TASK_PREPARED_CLIENT_ROOT",
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
        TaskProtocolEvent::ContextEstablishApplied { context }
        | TaskProtocolEvent::ContextEstablishIdempotent { context }
        | TaskProtocolEvent::ContextEstablishConflict { context }
        | TaskProtocolEvent::LeaseRenewed { context, .. }
        | TaskProtocolEvent::ContextReleaseApplied { context, .. }
        | TaskProtocolEvent::ContextLeaseExpired { context }
        | TaskProtocolEvent::ContextTerminationCompleted { context, .. }
        | TaskProtocolEvent::ContextAbortApplied { context } => {
            (context.query_execution_id(), context.backend_process_id())
        }
    };
    if !controls.owns_backend(process) {
        return;
    }
    let Some(observer) = resolve_observer(controls, execution) else {
        return;
    };
    match event {
        TaskProtocolEvent::ContextEstablishApplied { context } => {
            emit_context(
                observer.as_ref(),
                "NOVAROCKS_TASK_CONTEXT_ESTABLISH_APPLIED",
                context,
            );
        }
        TaskProtocolEvent::ContextEstablishIdempotent { context } => {
            emit_context(
                observer.as_ref(),
                "NOVAROCKS_TASK_CONTEXT_ESTABLISH_IDEMPOTENT",
                context,
            );
        }
        TaskProtocolEvent::ContextEstablishConflict { context } => {
            emit_context(
                observer.as_ref(),
                "NOVAROCKS_TASK_CONTEXT_ESTABLISH_CONFLICT",
                context,
            );
        }
        TaskProtocolEvent::LeaseRenewed { context, sequence } => {
            let execution = context.query_execution_id();
            observer.observe(&format!(
                "NOVAROCKS_TASK_LEASE_RENEWED execution_id={}:{}:{} frontend={} backend={} sequence={}",
                execution.query_id().high(),
                execution.query_id().low(),
                execution.attempt_id().get(),
                context.frontend_process_id(),
                context.backend_process_id(),
                sequence.get(),
            ));
        }
        TaskProtocolEvent::TaskCreateApplied { identity } => {
            emit_task(observer.as_ref(), "NOVAROCKS_TASK_CREATE_APPLIED", identity)
        }
        TaskProtocolEvent::TaskCreateIdempotent { identity } => emit_task(
            observer.as_ref(),
            "NOVAROCKS_TASK_CREATE_IDEMPOTENT",
            identity,
        ),
        TaskProtocolEvent::ContextReleaseApplied {
            context,
            runtime_filter,
        } => {
            let runtime_filter = match runtime_filter {
                RuntimeFilterReleaseObservation::Absent => "absent",
                RuntimeFilterReleaseObservation::Available => "available",
                RuntimeFilterReleaseObservation::Unavailable => "unavailable",
            };
            emit_context_with(
                observer.as_ref(),
                "NOVAROCKS_TASK_RELEASE_APPLIED",
                context,
                &format!("runtime_filter={runtime_filter}"),
            );
        }
        TaskProtocolEvent::ContextLeaseExpired { context } => {
            emit_context(
                observer.as_ref(),
                "NOVAROCKS_TASK_CONTEXT_LEASE_EXPIRED",
                context,
            );
        }
        TaskProtocolEvent::ContextTerminationCompleted {
            context,
            cause,
            retained_tasks,
        } => {
            let execution = context.query_execution_id();
            observer.observe(&format!(
                "NOVAROCKS_TASK_CONTEXT_TERMINATION_COMPLETED execution_id={}:{}:{} frontend={} backend={} cause={} retained_tasks={retained_tasks}",
                execution.query_id().high(),
                execution.query_id().low(),
                execution.attempt_id().get(),
                context.frontend_process_id(),
                context.backend_process_id(),
                cause,
            ));
        }
        TaskProtocolEvent::TaskTerminalRetained { identity, state } => {
            let execution = identity.query_execution_id();
            observer.observe(&format!(
                "NOVAROCKS_TASK_TERMINAL_RETAINED execution_id={}:{}:{} stage={} task={} backend={} state={state}",
                execution.query_id().high(),
                execution.query_id().low(),
                execution.attempt_id().get(),
                identity.stage_id().get(),
                identity.task_id().get(),
                identity.backend_process_id(),
            ));
        }
        TaskProtocolEvent::ContextAbortApplied { context } => {
            let execution = context.query_execution_id();
            observer.observe(&format!(
                "NOVAROCKS_TASK_CONTEXT_ABORT_APPLIED execution_id={}:{}:{} frontend={} backend={}",
                execution.query_id().high(),
                execution.query_id().low(),
                execution.attempt_id().get(),
                context.frontend_process_id(),
                context.backend_process_id(),
            ));
        }
    }
}

#[cfg(novarocks_dev_controls)]
fn resolve_observer(
    controls: &crate::test_observation_controls::NativeObservationControls,
    execution: novarocks_types::QueryExecutionId,
) -> Option<Arc<dyn novarocks_execution::runtime::exchange::TestEventObserver>> {
    match controls.observation_for("grpc-fragment", execution) {
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
