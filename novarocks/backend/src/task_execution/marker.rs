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

//! Backend rendering for typed Worker task-protocol evidence.

use novarocks_execution_contract::task_execution::identity::{QueryContextRef, TaskIdentity};
use novarocks_worker::{RuntimeFilterReleaseObservation, TaskProtocolEvent};

/// Whether this process emits task-protocol operation evidence.
///
/// It shares the connector-reader switch rather than owning a second one: a
/// distributed case arms one marker environment and then asserts on whatever
/// evidence the run owes it.
fn enabled() -> bool {
    novarocks_native_adapter::debug_environment::debug_emit_connector_reader_marker()
}

/// One context-scoped line. The backend process id is part of the identity so
/// a per-backend count still means something when logs are merged.
fn emit_context(name: &str, context: QueryContextRef) {
    emit_context_with(name, context, "");
}

/// One context-scoped line with extra `key=value` words appended.
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

/// One task-scoped line, carrying the stage and task that name the subject.
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

/// How one establish settled.
///
/// A terminal receipt, an identity mismatch, and an illegal state emit
/// nothing: none of them is a progression of the establish domain, and naming
/// them here would make a count of applies or conflicts unreadable.
pub(super) fn emit(event: TaskProtocolEvent) {
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
