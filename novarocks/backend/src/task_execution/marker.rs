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

//! Stable evidence for the core task-protocol operations.
//!
//! The protocol's central promise is its per-domain progression: an operation
//! either applies, is recognised as an exact replay, or is refused as a
//! conflict. Those three must not look the same in a log, because
//! distinguishing them is precisely what the protocol claims to do — an
//! idempotent replay that was silently misread as a fresh apply, or a conflict
//! that was silently accepted, is the failure these markers exist to expose.
//!
//! Every emitter here takes the receipt's already-typed
//! [`OperationOutcome`](novarocks_execution::task_execution::operation::OperationOutcome)
//! rather than re-deriving the verdict from state. That is deliberate: the
//! owner classified the progression once, under its own lock, and a second
//! classification here could disagree with the answer the frontend was given.
//!
//! Each emitter is called where its operation's outcome is decided — at the
//! owner's entry point, on the receipt it is about to return — never where a
//! request, driver, or host is constructed. A marker logged at construction
//! says only that something exists, which is exactly the defect this arc has
//! produced repeatedly.

use novarocks_execution::task_execution::identity::{QueryContextRef, TaskIdentity};
use novarocks_execution::task_execution::lease::LeaseSequence;
use novarocks_execution::task_execution::operation::{OperationOutcome, ReleaseOutcome};

use super::receipt::{CreateTaskOutcome, QueryContextOutcome, ReleaseQueryContextOutcome};

/// Whether this process emits task-protocol operation evidence.
///
/// It shares the connector-reader switch rather than owning a second one: a
/// distributed case arms one marker environment and then asserts on whatever
/// evidence the run owes it.
fn enabled() -> bool {
    crate::config::debug_emit_connector_reader_marker()
}

/// One context-scoped line. The backend process id is part of the identity so
/// a per-backend count still means something when logs are merged.
fn emit_context(name: &str, context: QueryContextRef) {
    let execution = context.query_execution_id();
    println!(
        "{name} execution_id={}:{}:{} frontend={} backend={}",
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
pub(super) fn establish_query_context(context: QueryContextRef, receipt: &QueryContextOutcome) {
    if !enabled() {
        return;
    }
    match receipt.outcome() {
        OperationOutcome::Accepted => {
            emit_context("NOVAROCKS_TASK_CONTEXT_ESTABLISH_APPLIED", context);
        }
        OperationOutcome::Idempotent => {
            emit_context("NOVAROCKS_TASK_CONTEXT_ESTABLISH_IDEMPOTENT", context);
        }
        OperationOutcome::ContextConflict => {
            emit_context("NOVAROCKS_TASK_CONTEXT_ESTABLISH_CONFLICT", context);
        }
        _ => {}
    }
}

/// How one lease renewal settled.
///
/// Only an applied renewal is evidence: an idempotent or stale renewal extends
/// nothing, so counting it would overstate how long the lease was actually
/// held open.
pub(super) fn renew_query_execution_lease(
    context: QueryContextRef,
    sequence: LeaseSequence,
    receipt: &QueryContextOutcome,
) {
    if !enabled() || receipt.outcome() != OperationOutcome::Accepted {
        return;
    }
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

/// How one create settled.
pub(super) fn create_task(identity: TaskIdentity, receipt: &CreateTaskOutcome) {
    if !enabled() {
        return;
    }
    match receipt.outcome() {
        OperationOutcome::Accepted => emit_task("NOVAROCKS_TASK_CREATE_APPLIED", identity),
        OperationOutcome::Idempotent => emit_task("NOVAROCKS_TASK_CREATE_IDEMPOTENT", identity),
        OperationOutcome::CreateConflict => emit_task("NOVAROCKS_TASK_CREATE_CONFLICT", identity),
        _ => {}
    }
}

/// How one release settled.
///
/// Only the release that actually moved the context out of `Active` emits: a
/// replay of an already-released context, and a release that found the context
/// terminal, both report `Released` without having released anything.
pub(super) fn release_query_context(
    context: QueryContextRef,
    receipt: &ReleaseQueryContextOutcome,
) {
    if !enabled() || receipt.outcome() != OperationOutcome::Accepted {
        return;
    }
    if receipt.acknowledgement().map(|ack| ack.release()) != Some(ReleaseOutcome::Released) {
        return;
    }
    emit_context("NOVAROCKS_TASK_RELEASE_APPLIED", context);
}

/// One applied context abort, with the cause the frontend sent.
///
/// Client cancellation reaches a backend as this operation, so without it
/// there is no cluster-visible evidence that a KILL QUERY was delivered at
/// all -- and the retired protocol's own abort markers are gone, so a case
/// asserting cancellation has nothing left to count.
pub(super) fn abort_query_context(context: QueryContextRef, receipt: &QueryContextOutcome) {
    if !enabled() || receipt.outcome() != OperationOutcome::Accepted {
        return;
    }
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
