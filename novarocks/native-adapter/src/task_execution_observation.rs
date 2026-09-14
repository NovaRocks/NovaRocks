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
struct NativeTaskExecutionPorts;

impl TaskProtocolObserver for NativeTaskExecutionPorts {
    fn observe(&self, event: TaskProtocolEvent) {
        emit(event);
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
pub fn backend_task_execution_ports() -> TaskExecutionPorts {
    let adapter = Arc::new(NativeTaskExecutionPorts);
    let observer: Arc<dyn TaskProtocolObserver> = adapter.clone();
    let result_lifecycle: Arc<dyn TaskResultLifecycle> = adapter.clone();
    let metrics: Arc<dyn TaskExecutionMetrics> = adapter;
    TaskExecutionPorts::new(observer, result_lifecycle, metrics)
}

fn enabled() -> bool {
    crate::debug_environment::debug_emit_connector_reader_marker()
}

fn emit_context(name: &str, context: QueryContextRef) {
    emit_context_with(name, context, "");
}

fn emit_context_with(name: &str, context: QueryContextRef, extra: &str) {
    let execution = context.query_execution_id();
    let separator = if extra.is_empty() { "" } else { " " };
    println!(
        "{name} execution_id={}:{}:{} frontend={} backend={}{separator}{extra}",
        execution.query_id().high(),
        execution.query_id().low(),
        execution.attempt_id().get(),
        context.frontend_process_id(),
        context.backend_process_id(),
    );
    let _ = std::io::Write::flush(&mut std::io::stdout());
}

fn emit_task(name: &str, identity: TaskIdentity) {
    let execution = identity.query_execution_id();
    println!(
        "{name} execution_id={}:{}:{} stage={} task={} backend={}",
        execution.query_id().high(),
        execution.query_id().low(),
        execution.attempt_id().get(),
        identity.stage_id().get(),
        identity.task_id().get(),
        identity.backend_process_id(),
    );
    let _ = std::io::Write::flush(&mut std::io::stdout());
}

fn emit(event: TaskProtocolEvent) {
    if !enabled() {
        return;
    }
    match event {
        TaskProtocolEvent::ContextEstablishApplied { context } => {
            emit_context("NOVAROCKS_TASK_CONTEXT_ESTABLISH_APPLIED", context);
        }
        TaskProtocolEvent::ContextEstablishIdempotent { context } => {
            emit_context("NOVAROCKS_TASK_CONTEXT_ESTABLISH_IDEMPOTENT", context);
        }
        TaskProtocolEvent::ContextEstablishConflict { context } => {
            emit_context("NOVAROCKS_TASK_CONTEXT_ESTABLISH_CONFLICT", context);
        }
        TaskProtocolEvent::LeaseRenewed { context, sequence } => {
            let execution = context.query_execution_id();
            println!(
                "NOVAROCKS_TASK_LEASE_RENEWED execution_id={}:{}:{} frontend={} backend={} sequence={}",
                execution.query_id().high(),
                execution.query_id().low(),
                execution.attempt_id().get(),
                context.frontend_process_id(),
                context.backend_process_id(),
                sequence.get(),
            );
            let _ = std::io::Write::flush(&mut std::io::stdout());
        }
        TaskProtocolEvent::TaskCreateApplied { identity } => {
            emit_task("NOVAROCKS_TASK_CREATE_APPLIED", identity)
        }
        TaskProtocolEvent::TaskCreateIdempotent { identity } => {
            emit_task("NOVAROCKS_TASK_CREATE_IDEMPOTENT", identity)
        }
        TaskProtocolEvent::TaskCreateConflict { identity } => {
            emit_task("NOVAROCKS_TASK_CREATE_CONFLICT", identity)
        }
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
                "NOVAROCKS_TASK_RELEASE_APPLIED",
                context,
                &format!("runtime_filter={runtime_filter}"),
            );
        }
        TaskProtocolEvent::ContextLeaseExpired { context } => {
            emit_context("NOVAROCKS_TASK_CONTEXT_LEASE_EXPIRED", context);
        }
        TaskProtocolEvent::ContextTerminationCompleted {
            context,
            cause,
            retained_tasks,
        } => {
            let execution = context.query_execution_id();
            println!(
                "NOVAROCKS_TASK_CONTEXT_TERMINATION_COMPLETED execution_id={}:{}:{} frontend={} backend={} cause={} retained_tasks={retained_tasks}",
                execution.query_id().high(),
                execution.query_id().low(),
                execution.attempt_id().get(),
                context.frontend_process_id(),
                context.backend_process_id(),
                cause,
            );
            let _ = std::io::Write::flush(&mut std::io::stdout());
        }
        TaskProtocolEvent::TaskTerminalRetained {
            identity,
            state,
            bytes,
        } => {
            let execution = identity.query_execution_id();
            println!(
                "NOVAROCKS_TASK_TERMINAL_RETAINED execution_id={}:{}:{} stage={} task={} backend={} state={state} bytes={bytes}",
                execution.query_id().high(),
                execution.query_id().low(),
                execution.attempt_id().get(),
                identity.stage_id().get(),
                identity.task_id().get(),
                identity.backend_process_id(),
            );
            let _ = std::io::Write::flush(&mut std::io::stdout());
        }
        TaskProtocolEvent::ContextAbortApplied { context } => {
            let execution = context.query_execution_id();
            println!(
                "NOVAROCKS_TASK_CONTEXT_ABORT_APPLIED execution_id={}:{}:{} frontend={} backend={}",
                execution.query_id().high(),
                execution.query_id().low(),
                execution.attempt_id().get(),
                context.frontend_process_id(),
                context.backend_process_id(),
            );
            let _ = std::io::Write::flush(&mut std::io::stdout());
        }
    }
}
