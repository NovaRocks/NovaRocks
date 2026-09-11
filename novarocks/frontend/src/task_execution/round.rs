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
use novarocks_query_application::api::{QueryExecutionError, QueryExecutionErrorKind};
use novarocks_query_application::coordination::{
    AcceptedAttemptFailure, AcceptedRootStatusSender, AcceptedRootStatusSource,
    accepted_root_status_projection_with_seal_port,
};

use super::blocking_io::ConnectorBlockingIoSupervisor;
use super::context_owner::{ContextEstablishSource, QueryContextOwner};
use super::error::TaskExecutionError;
use super::execution::QueryTaskExecution;
use super::intent::OperationAcknowledgement;
use crate::native::task_transport::{SubscriptionState, TaskAckIntake, TaskStatusSubscriber};

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

/// One attempt-local owner the runner drives once per turn.
///
/// Two loops need exactly this and nothing more: the dynamic-filter feedback
/// reader, which must fetch from every task whose freshly folded status
/// advertises a newer version, and the credential rotation owner, which must
/// mint an `AdvanceDomain` for every context that still owes the current
/// epoch. Both are attempt-local state machines with their own progression,
/// and neither belongs inside [`QueryTaskExecution`]: one reads over a data
/// plane and the other talks to a credential provider, and the state machine
/// opens no connection at all.
///
/// So they hang here instead, on one seam, rather than as two call sites
/// wedged into the loop. A pump may read the state machine and enqueue work
/// into it; it settles its own operations through
/// [`AcknowledgementObserver`], which the runner already drains for every
/// acknowledgement.
pub(crate) trait TurnPump: Send {
    /// A stable name for this owner, for the installation counter.
    fn name(&self) -> &'static str;

    /// Drives this owner once, reporting how many things it moved.
    ///
    /// A returned error fails the attempt. That is the point for the credential
    /// owner -- a rotation that cannot complete before its hard deadline leaves
    /// backends on an expiring secret, which the old supervisor also treated as
    /// an abort -- so a pump that means "nothing happened" must say zero rather
    /// than fail.
    fn drive(&mut self, execution: &mut QueryTaskExecution) -> Result<usize, TaskExecutionError>;
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

    /// Replaces a subscription after the local intake reported observation
    /// loss, replaying from the serial runner's authoritative cursors.
    fn resubscribe(
        &self,
        context: QueryContextRef,
        cursors: Vec<TaskStatusCursor>,
    ) -> Result<(), String>;

    /// The subscription's state once it has settled somewhere resubscribing
    /// cannot repair, and `None` while it can still recover.
    ///
    /// A backend whose process is gone stops answering: its stream breaks, the
    /// bounded resubscription budget runs out, and the state settles. Nothing
    /// else in the attempt is obliged to notice, so reporting it here is what
    /// lets the round decide the attempt on transport evidence instead of
    /// leaving it to a statement deadline.
    fn settled_fatally(&self, context: QueryContextRef) -> Option<SubscriptionState>;
}

impl StatusSubscriptions for TaskStatusSubscriber {
    fn ensure(
        &self,
        context: QueryContextRef,
        cursors: Vec<TaskStatusCursor>,
    ) -> Result<(), String> {
        Self::ensure(self, context, cursors)
    }

    fn resubscribe(
        &self,
        context: QueryContextRef,
        cursors: Vec<TaskStatusCursor>,
    ) -> Result<(), String> {
        Self::resubscribe(self, context, cursors)
    }

    fn settled_fatally(&self, context: QueryContextRef) -> Option<SubscriptionState> {
        Self::state(self, context).filter(|state| state.is_fatal())
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
    pub(crate) resubscriptions: usize,
    /// What the per-attempt pumps moved: filter versions ingested, credential
    /// rotations started or advanced.
    pub(crate) pumped: usize,
}

impl TurnReport {
    /// Whether this turn did nothing at all.
    ///
    /// A caller uses it to decide whether to park rather than spin; it is not
    /// a completion signal, because an attempt with nothing due is not an
    /// attempt that finished.
    pub(crate) const fn is_idle(self) -> bool {
        self.operations == 0
            && self.acknowledgements == 0
            && self.status_events == 0
            && self.resubscriptions == 0
            && self.pumped == 0
    }
}

/// One attempt's runner.
pub(crate) struct TaskRound {
    execution: QueryTaskExecution,
    acks: TaskAckIntake,
    establish: Box<dyn ContextEstablishSource>,
    subscriber: Arc<dyn StatusSubscriptions>,
    observers: Vec<Arc<dyn AcknowledgementObserver>>,
    pumps: Vec<Box<dyn TurnPump>>,
    pumps_sealed: bool,
    connector_blocking_io: Option<ConnectorBlockingIoSupervisor>,
    root_status_sender: Option<AcceptedRootStatusSender>,
    root_status_source: Option<AcceptedRootStatusSource>,
    pending_success_seal:
        Option<novarocks_query_application::coordination::AcceptedRootSuccessSealRequest>,
}

impl TaskRound {
    pub(crate) fn new(
        execution: QueryTaskExecution,
        acks: TaskAckIntake,
        establish: Box<dyn ContextEstablishSource>,
        subscriber: Arc<dyn StatusSubscriptions>,
    ) -> Self {
        let (root_status_sender, root_status_source) =
            accepted_root_status_projection_with_seal_port(
                execution.graph().root_identity(),
                Arc::new(execution.intake().handle()),
            );
        Self {
            execution,
            acks,
            establish,
            subscriber,
            observers: Vec::new(),
            pumps: Vec::new(),
            pumps_sealed: false,
            connector_blocking_io: None,
            root_status_sender: Some(root_status_sender),
            root_status_source: Some(root_status_source),
            pending_success_seal: None,
        }
    }

    /// Transfers the single accepted-root projection to the result-pump
    /// owner. The round retains only its sender and remains the sole publisher.
    pub(crate) fn take_root_status_source(&mut self) -> Option<AcceptedRootStatusSource> {
        self.root_status_source.take()
    }

    /// Installs the process owner used by blocking Connector calls.
    pub(crate) fn with_connector_blocking_io(
        mut self,
        supervisor: ConnectorBlockingIoSupervisor,
    ) -> Self {
        self.connector_blocking_io = Some(supervisor);
        self
    }

    pub(crate) fn connector_blocking_io(&self) -> Option<&ConnectorBlockingIoSupervisor> {
        self.connector_blocking_io.as_ref()
    }

    /// Adds one owner that must see every acknowledgement this runner settles.
    pub(crate) fn observing(mut self, observer: Arc<dyn AcknowledgementObserver>) -> Self {
        self.observers.push(observer);
        self
    }

    /// Adds one acknowledgement observer after construction.
    ///
    /// The credential rotation owner is both: it is driven every turn and it
    /// settles its own advances from the acknowledgement stream, and it is
    /// built after the runner because it needs the attempt's frozen credential
    /// table.
    pub(crate) fn add_observer(&mut self, observer: Arc<dyn AcknowledgementObserver>) {
        self.observers.push(observer);
    }

    /// Adds one attempt-local owner this runner drives on every turn.
    ///
    /// Taken by `&mut self` rather than by value because the two production
    /// pumps are built from things that only exist after the runner does: the
    /// filter reader needs the root result transport, and the credential owner
    /// needs the attempt's frozen credential table.
    pub(crate) fn add_pump(&mut self, pump: Box<dyn TurnPump>) {
        crate::native::task_transport::observe_attempt_pump_installed(pump.name());
        self.pumps.push(pump);
    }

    /// Declares that this attempt's per-turn owners are all installed.
    ///
    /// Required before the first turn, and this is the point of it: the two
    /// loops this seam exists for were both fully built, fully unit-tested and
    /// never handed to a runner, and nothing failed. An attempt that never
    /// declares its owners now fails on its first turn instead, so *forgetting*
    /// the installation is no longer a silent absence. Declaring zero is legal
    /// and explicit -- an attempt with no filter channels and no rotatable
    /// credential really has nothing to drive.
    pub(crate) fn seal_pumps(&mut self) {
        self.pumps_sealed = true;
    }

    /// How many per-turn pumps are installed.
    ///
    /// The production observable of the same fact is the installation counter
    /// `observe_attempt_pump_installed` raises; this is the direct accessor a
    /// test asserts on. A pump that is built but never handed to the runner is
    /// exactly the defect these loops had before, and it is invisible to any
    /// test of the pump itself.
    #[cfg(test)]
    pub(crate) fn installed_pumps(&self) -> usize {
        self.pumps.len()
    }

    /// Steps the attempt once.
    ///
    /// The order is deliberate. Acknowledgements settle first, because a
    /// settled operation frees dispatch permits and may make the next
    /// submission legal; status folds next, because it can retire tasks and
    /// open edges; submission runs last so it sees both. Doing it the other
    /// way round would submit against a state one turn stale and hold permits
    /// that were already free.
    ///
    /// The pumps sit between the status fold and submission, and both of the
    /// two reasons are load-bearing. They run *after* status because that is
    /// where their input comes from: a dynamic-filter fetch is triggered by an
    /// advertisement this turn folded, and reading it before the fold would act
    /// on last turn's version. They run *before* submission because that is
    /// where their output goes: a credential rotation that became due this turn
    /// enqueues an `AdvanceDomain` which this same turn then releases, instead
    /// of sitting in the dispatcher for a whole turn while the credential it
    /// replaces keeps expiring.
    pub(crate) fn turn(&mut self) -> Result<TurnReport, TaskExecutionError> {
        if !self.pumps_sealed {
            return Err(TaskExecutionError::Schedule(
                "the attempt's per-turn owners were never declared".to_owned(),
            ));
        }
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

        for context in self.execution.take_status_reconciliations() {
            self.subscriber
                .resubscribe(context, self.execution.status_cursors(context))
                .map_err(TaskExecutionError::Schedule)?;
            report.resubscriptions += 1;
        }

        let status_budget = if self.pending_success_seal.is_some() {
            1
        } else {
            STATUS_EVENTS_PER_TURN
        };
        let status = self.execution.apply_status(status_budget)?;
        report.status_events = status.accepted + status.ignored;
        if let Some(request) = status.success_seal {
            if self.pending_success_seal.replace(request).is_some() {
                return Err(TaskExecutionError::Schedule(
                    "more than one result success-seal request reached one TaskRound".to_owned(),
                ));
            }
        }
        if status.resubscribe
            && let Some(request) = self.pending_success_seal.take()
        {
            request.reject(QueryExecutionError::new(
                QueryExecutionErrorKind::Failed,
                "success seal refused because Task status observation was incomplete before its linearization point",
            ));
        }
        self.publish_root_status()?;
        if !status.resubscribe {
            self.try_settle_success_seal()?;
        }
        if status.resubscribe {
            for &context in self.execution.graph().contexts() {
                if self
                    .execution
                    .owner(context)
                    .is_none_or(QueryContextOwner::needs_establish)
                {
                    continue;
                }
                self.subscriber
                    .resubscribe(context, self.execution.status_cursors(context))
                    .map_err(TaskExecutionError::Schedule)?;
                report.resubscriptions += 1;
            }
        }

        for pump in &mut self.pumps {
            report.pumped += pump.drive(&mut self.execution)?;
        }

        let pumped = self.execution.pump(self.establish.as_ref())?;
        report.operations = pumped.operations;

        // Every context that exists needs its one subscription. Starting it
        // here rather than at establish time keeps the runner the only thing
        // that touches the subscriber, and `ensure` is idempotent, so a
        // context that already has one costs a map lookup.
        //
        // A context is subscribed only once its own establish has been
        // acknowledged. A backend holds no context until it applies the
        // establish, so a subscription that overtakes it names a context that
        // backend does not hold yet -- and that refusal is classified as
        // fatal, which permanently blinds the frontend to that backend's task
        // status. Waiting for the acknowledgement is the ordering every other
        // operation on this protocol already obeys; tolerating the refusal
        // instead would make an observation loss the normal case.
        for &context in self.execution.graph().contexts() {
            if self
                .execution
                .owner(context)
                .is_none_or(QueryContextOwner::needs_establish)
            {
                continue;
            }
            let cursors = self.execution.status_cursors(context);
            self.subscriber
                .ensure(context, cursors)
                .map_err(TaskExecutionError::Schedule)?;
            // A settled subscription is this attempt's evidence that the
            // backend process is gone. It is read after `ensure` on purpose:
            // `ensure` is what restarts a stream that can still recover, so
            // asking first would report a state the very next call repairs.
            if let Some(state) = self.subscriber.settled_fatally(context) {
                return Err(TaskExecutionError::ParticipantUnobservable {
                    backend: context.backend_process_id(),
                    state: state.as_str(),
                });
            }
        }

        Ok(report)
    }

    /// Publishes the root snapshot only after the whole bounded status fold
    /// has updated the attempt failure latch. Re-reading the held root on every
    /// turn is intentional: a derived root failure may first be published as
    /// pending, then be refined at the same status version when another task
    /// supplies the authoritative non-derived cause on a later turn.
    fn publish_root_status(&mut self) -> Result<(), TaskExecutionError> {
        let Some(sender) = self.root_status_sender.as_ref() else {
            // Status ordered after the consumed seal is residual convergence
            // information. It cannot revise the already fixed business result.
            return Ok(());
        };
        let root = self.execution.graph().root_identity();
        let Some(task) = self.execution.task(root.task_id()) else {
            return Err(TaskExecutionError::Schedule(
                "the frozen root Task is absent from its TaskRound".to_owned(),
            ));
        };
        if !task.create_acknowledged() {
            return Ok(());
        }
        let Some(status) = task.status().cloned() else {
            return Ok(());
        };
        let attempt_failure = match self.execution.failure_cause() {
            Some(cause) if cause.is_derived() => AcceptedAttemptFailure::DerivedPending,
            Some(authoritative) => AcceptedAttemptFailure::Authoritative(authoritative.clone()),
            None => AcceptedAttemptFailure::None,
        };
        sender
            .publish_attempt_observation(status, attempt_failure)
            .map_err(|error| {
                TaskExecutionError::Schedule(format!(
                    "publish accepted root Task status projection failed: {error}"
                ))
            })
    }

    fn try_settle_success_seal(&mut self) -> Result<(), TaskExecutionError> {
        if self.pending_success_seal.is_none() {
            return Ok(());
        }
        let root = self.execution.graph().root_identity();
        let root_status = self
            .execution
            .task(root.task_id())
            .and_then(|task| task.status());
        let root_finished = root_status.is_some_and(|status| {
            status.identity() == root
                && status.state() == novarocks_execution::task_execution::TaskState::Finished
        });
        if root_finished && self.execution.failure_cause().is_none() && self.tasks_created() {
            let sender = self.root_status_sender.take().ok_or_else(|| {
                TaskExecutionError::Schedule(
                    "success seal reached TaskRound after its publisher was consumed".to_owned(),
                )
            })?;
            return self
                .pending_success_seal
                .take()
                .expect("the success request was checked before settlement")
                .accept(sender)
                .map_err(|error| {
                    TaskExecutionError::Schedule(format!(
                        "seal accepted root Task success projection failed: {error}"
                    ))
                });
        }
        let terminal_without_success = match self.execution.failure_cause() {
            // A derived cause deliberately freezes the decision until the
            // same attempt owner learns the originating cause. Rejecting the
            // seal here would turn an incomplete failure fact into a final
            // classification.
            Some(cause) if cause.is_derived() => false,
            Some(_) => true,
            None => root_status.is_some_and(|status| {
                status.is_terminal()
                    && status.state() != novarocks_execution::task_execution::TaskState::Finished
            }),
        };
        if terminal_without_success {
            let error = QueryExecutionError::new(
                QueryExecutionErrorKind::Failed,
                "success seal reached TaskRound without exact root Finished and an empty attempt failure latch",
            );
            self.pending_success_seal
                .take()
                .expect("the success request was checked before rejection")
                .reject(error);
        }
        Ok(())
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

    /// Whether every task of this attempt has acknowledged its exact create.
    ///
    /// This is the task protocol's Stage and Start: past it, every backend
    /// holds or historically held the exact task the schedule placed on it.
    /// Lifecycle status cannot answer this: a task may skip the locally
    /// visible Created state and reach Terminal before its create receipt is
    /// settled, while the later receipt still proves admission.
    pub(crate) fn tasks_created(&self) -> bool {
        self.execution.graph().tasks().all(|task| {
            self.execution
                .task(task.task_id())
                .is_some_and(|task| task.create_acknowledged())
        })
    }

    /// The historical create-receipt barrier for starting result fetches.
    pub(crate) fn result_pump_ready(&self) -> bool {
        self.tasks_created()
    }

    #[cfg(test)]
    pub(crate) fn root_success_sealed(&self) -> bool {
        self.root_status_sender.is_none()
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
