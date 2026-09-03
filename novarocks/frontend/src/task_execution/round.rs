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

use std::sync::Arc;

use novarocks_execution::task_execution::identity::TaskIdentity;
use novarocks_execution::task_execution::status::TerminationDetail;

use novarocks_execution::task_execution::identity::QueryContextRef;
use novarocks_execution::task_execution::status::TaskStatusCursor;

use super::context_owner::ContextEstablishSource;
use super::error::TaskExecutionError;
use super::execution::QueryTaskExecution;
use super::intent::OperationAcknowledgement;
use super::remote_task::RemoteTaskState;
use crate::native::task_transport::{TaskAckIntake, TaskStatusSubscriber};

/// Something that must see every acknowledgement this runner settles.
///
/// The runner is the only thing that drains acknowledgements, so an owner that
/// bound work to a released operation can learn its verdict nowhere else. That
/// is exactly split delivery's position: its sender blocks on the substrate
/// settling the operation that carried its submission, and a failed-closed
/// acknowledgement carries no receipt to route by, so the sender would wait
/// out its whole timeout for a verdict that had already arrived.
///
/// Observation happens before the state machine settles the operation, so an
/// observer learns the real outcome even when settling it fails the attempt.
pub(crate) trait AcknowledgementObserver: Send + Sync {
    /// Records one acknowledgement, or reports why it could not be recorded.
    ///
    /// The error is a frontend bug rather than a query outcome, which is why
    /// it is reported rather than logged: an observer that silently missed an
    /// acknowledgement leaves whatever bound to it waiting forever.
    fn observe_acknowledgement(&self, ack: &OperationAcknowledgement) -> Result<(), String>;
}

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
    observers: Vec<Arc<dyn AcknowledgementObserver>>,
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
            observers: Vec::new(),
        }
    }

    /// Adds one owner that must see every acknowledgement this runner settles.
    pub(crate) fn observing(mut self, observer: Arc<dyn AcknowledgementObserver>) -> Self {
        self.observers.push(observer);
        self
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
            // Before the state machine settles it: settling can fail the
            // attempt, and an observer that learned nothing in that case would
            // leave a blocked owner waiting for a verdict that did arrive.
            for observer in &self.observers {
                observer
                    .observe_acknowledgement(&ack)
                    .map_err(TaskExecutionError::Schedule)?;
            }
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

    /// Whether every query context of this attempt has been established.
    ///
    /// This is the task protocol's ControlReady: past it, every backend that
    /// hosts a task has acknowledged this attempt's shared facts. A caller
    /// uses it to close a pre-ready retry window, so it must be an observation
    /// of acknowledgements rather than of having sent them.
    pub(crate) fn contexts_established(&self) -> bool {
        self.execution.graph().contexts().all(|&context| {
            self.execution
                .owner(context)
                .is_some_and(|owner| !owner.needs_establish())
        })
    }

    /// Whether every task of this attempt has been created.
    ///
    /// This is the task protocol's Stage and Start: past it, every backend
    /// holds the exact task the schedule placed on it. A task that already
    /// went terminal does not count as created -- the question is whether the
    /// attempt finished starting, and one that lost a task did not.
    pub(crate) fn tasks_created(&self) -> bool {
        self.execution.graph().tasks().all(|task| {
            self.execution
                .task(task.task_id())
                .is_some_and(|task| matches!(task.state(), RemoteTaskState::Created))
        })
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

    /// Takes the state machine back out.
    ///
    /// Only a test needs this: it lets one assert the runner's own view of a
    /// state built through the state machine's API, without a second way to
    /// construct that state.
    #[cfg(test)]
    pub(crate) fn into_execution(self) -> QueryTaskExecution {
        self.execution
    }

    pub(crate) const fn execution(&self) -> &QueryTaskExecution {
        &self.execution
    }

    pub(crate) const fn execution_mut(&mut self) -> &mut QueryTaskExecution {
        &mut self.execution
    }
}
