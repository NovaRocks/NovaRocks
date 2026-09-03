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

//! The serial runner of one attempt.
//!
//! Everything the task protocol decides belongs to [`QueryTaskExecution`],
//! which is a single-owner state machine. This is the loop that steps it: one
//! turn submits whatever became due, settles whatever came back, and reports
//! what the caller needs to decide whether to keep going.
//!
//! It deliberately owns no policy. A turn does not decide that a query failed,
//! timed out, or finished -- it moves the state machine and lets the caller
//! read the verdicts the machine already computes.

// The coordinator drives this runner when it cuts over to the task substrate.
// `expect` rather than `allow` so this fails once that lands rather than
// outliving its reason.
#![cfg_attr(
    not(test),
    expect(
        dead_code,
        reason = "the coordinator drives this runner when it cuts over to the task substrate"
    )
)]

use std::sync::Arc;

use novarocks_execution::task_execution::identity::TaskIdentity;
use novarocks_execution::task_execution::status::TerminationDetail;

use novarocks_execution::task_execution::identity::QueryContextRef;
use novarocks_execution::task_execution::status::TaskStatusCursor;

use super::context_owner::ContextEstablishSource;
use super::error::TaskExecutionError;
use super::execution::QueryTaskExecution;
use crate::native::task_transport::{TaskAckIntake, TaskStatusSubscriber};

/// The one thing the runner asks of the status transport.
///
/// Narrower than the subscriber it is implemented by: the runner starts
/// subscriptions and never reads, cancels, or migrates one, so depending on
/// the whole transport would let it grow a second reason to touch it.
pub(crate) trait StatusSubscriptions: Send + Sync {
    fn ensure(
        &self,
        context: QueryContextRef,
        cursors: Vec<TaskStatusCursor>,
    ) -> Result<(), String>;
}

impl StatusSubscriptions for TaskStatusSubscriber {
    fn ensure(
        &self,
        context: QueryContextRef,
        cursors: Vec<TaskStatusCursor>,
    ) -> Result<(), String> {
        Self::ensure(self, context, cursors)
    }
}

/// How many status events one turn folds in.
///
/// A turn is bounded so a backend flooding status cannot starve the
/// submissions the same runner owes every other backend. The number matters
/// only as a fairness bound: whatever is left stays queued and the next turn
/// takes it.
const STATUS_EVENTS_PER_TURN: usize = 256;

/// What one turn moved.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(crate) struct TurnReport {
    pub(crate) operations: usize,
    pub(crate) acknowledgements: usize,
    pub(crate) status_events: usize,
}

impl TurnReport {
    /// Whether this turn did nothing at all.
    ///
    /// A caller uses it to decide whether to park rather than spin; it is not
    /// a completion signal, because an attempt with nothing due is not an
    /// attempt that finished.
    pub(crate) const fn is_idle(self) -> bool {
        self.operations == 0 && self.acknowledgements == 0 && self.status_events == 0
    }
}

/// One attempt's runner.
pub(crate) struct TaskRound {
    execution: QueryTaskExecution,
    acks: TaskAckIntake,
    establish: Box<dyn ContextEstablishSource>,
    subscriber: Arc<dyn StatusSubscriptions>,
}

impl TaskRound {
    pub(crate) fn new(
        execution: QueryTaskExecution,
        acks: TaskAckIntake,
        establish: Box<dyn ContextEstablishSource>,
        subscriber: Arc<dyn StatusSubscriptions>,
    ) -> Self {
        Self {
            execution,
            acks,
            establish,
            subscriber,
        }
    }

    /// Steps the attempt once.
    ///
    /// The order is deliberate. Acknowledgements settle first, because a
    /// settled operation frees dispatch permits and may make the next
    /// submission legal; status folds next, because it can retire tasks and
    /// open edges; submission runs last so it sees both. Doing it the other
    /// way round would submit against a state one turn stale and hold permits
    /// that were already free.
    pub(crate) fn turn(&mut self) -> Result<TurnReport, TaskExecutionError> {
        let mut report = TurnReport::default();

        for ack in self.acks.drain() {
            report.acknowledgements += 1;
            self.execution.acknowledge(&ack)?;
        }

        let status = self.execution.apply_status(STATUS_EVENTS_PER_TURN)?;
        report.status_events = status.accepted + status.ignored;

        let pumped = self.execution.pump(self.establish.as_ref())?;
        report.operations = pumped.operations;

        // Every context that exists needs its one subscription. Starting it
        // here rather than at establish time keeps the runner the only thing
        // that touches the subscriber, and `ensure` is idempotent, so a
        // context that already has one costs a map lookup.
        for &context in self.execution.graph().contexts() {
            let cursors = self.execution.status_cursors(context);
            self.subscriber
                .ensure(context, cursors)
                .map_err(TaskExecutionError::Schedule)?;
        }

        Ok(report)
    }

    /// The root task the client's result comes from.
    pub(crate) fn root_task(&self) -> TaskIdentity {
        self.execution.graph().root_identity()
    }

    /// Records one packet this frontend received from the root's result plane.
    pub(crate) fn consume_root_result_packet(
        &mut self,
        packet_sequence: u64,
        end_of_stream: bool,
    ) -> Result<(), TaskExecutionError> {
        let root = self.root_task();
        self.execution
            .consume_root_result_packet(root, packet_sequence, end_of_stream)
    }

    /// Whether the client may be told the read is complete.
    pub(crate) fn client_visible_completion(&self) -> bool {
        self.execution.client_visible_completion()
    }

    /// Whether every task terminated and every context was released.
    pub(crate) fn attempt_drained(&self) -> bool {
        self.execution.attempt_drained()
    }

    /// The first termination cause this attempt latched.
    pub(crate) fn failure_cause(&self) -> Option<&TerminationDetail> {
        self.execution.failure_cause()
    }

    pub(crate) const fn execution(&self) -> &QueryTaskExecution {
        &self.execution
    }

    pub(crate) const fn execution_mut(&mut self) -> &mut QueryTaskExecution {
        &mut self.execution
    }
}
