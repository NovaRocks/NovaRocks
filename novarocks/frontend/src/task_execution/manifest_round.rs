// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. See the License for the
// specific language governing permissions and limitations
// under the License.

//! TaskRound assembly from one exact application-bound Task manifest.

use std::collections::BTreeMap;
use std::sync::Arc;

use novarocks_query_application::api::{
    NativeAttemptConvergence, NativeAttemptPreparationFailure, NativeAttemptTerminal,
    NativeContextConvergence, QueryExecutionError, QueryExecutionErrorKind,
};
use novarocks_query_application::coordination::DispatchBudget;
use novarocks_query_application::coordination::{
    AcceptedRootStatusSource, AttemptFailureClass, NativeAttemptDrive,
};
use novarocks_task_codec::TransportBudget;
use novarocks_workload_control::{CancellationReason, CancellationView};

use super::abort_effect::NativeAbortEffectIntake;
use super::actor_gate::{ActorGateOwner, ActorGatedTaskOperationSink};
use super::clock::ProcessMonotonicClock;
use super::error::TaskExecutionError;
use super::execution::QueryTaskExecution;
use super::graph::build_task_graph_from_manifest;
use super::intent::TaskOperationSink;
use super::round::{AcknowledgementObserver, StatusSubscriptions, TaskRound, TurnPump};
use super::sources::{AttemptEstablishFacts, SubmissionFragmentPlans};
use super::split_transport::SplitDeliveryBridge;
use super::status_intake::{NotifyWake, StatusIntake, StatusIntakeWake};
use crate::common::backend_topology::{
    BackendProcessObservation, BackendProcessObservationService,
};
use crate::native::data_runtime::FrontendDataRuntime;
use crate::native::task_transport::{
    AttemptWireFacts, NativeTaskOperationSink, TaskAckIntake, TaskStatusSubscriber,
};
use crate::query_execution::artifact::{TaskManifestBinding, ValidatedNativeSubmission};

const STATUS_INTAKE_CAPACITY: usize = 4096;
const IDLE_RECHECK: std::time::Duration = std::time::Duration::from_millis(5);

/// Process-owned runtime policy used to instantiate one exact manifest.
pub(crate) struct ManifestAttemptTransport {
    pub(crate) dispatch_budget: DispatchBudget,
    pub(crate) transport_budget: TransportBudget,
    pub(crate) status_subscription_error_budget: u32,
    pub(crate) wire: AttemptWireFacts,
    pub(crate) data_runtime: FrontendDataRuntime,
    /// Live membership is used only as residual process-lifecycle evidence. It
    /// cannot change the frozen manifest or schedule a successor.
    pub(crate) convergence_source: BackendProcessObservationService,
}

#[derive(Clone, Debug)]
struct ManifestConvergenceTarget {
    context: novarocks_execution_contract::QueryContextRef,
    process: novarocks_types::BackendProcessId,
    endpoint: novarocks_execution::runtime::endpoint::RuntimeEndpoint,
}

/// One active Task protocol owner assembled without a scheduling capability.
pub(crate) struct ManifestAssembledRound {
    pub(crate) round: TaskRound,
    pub(crate) split_delivery: Arc<SplitDeliveryBridge>,
    pub(crate) actor_gate: ActorGateOwner,
    convergence_source: BackendProcessObservationService,
    convergence_targets: Box<[ManifestConvergenceTarget]>,
    notify: Arc<tokio::sync::Notify>,
    terminal: Option<NativeAttemptTerminal>,
    pending_terminal: Option<NativeAttemptTerminal>,
    convergence_started: bool,
}

/// The completion fact this Task owner must wait for before it reports the
/// physical attempt terminal to Query Application.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ManifestAttemptCompletion {
    /// A row result pump owns output and asks TaskRound to seal the exact root
    /// success after it has consumed Worker EOS.
    AcceptedRootSuccessSeal,
    /// No result stream is attached. Completion requires full Task/context
    /// convergence, which is the conservative completion-only contract.
    AttemptDrained,
}

pub(crate) fn assemble_manifest_round(
    manifest: &TaskManifestBinding,
    submissions: Vec<ValidatedNativeSubmission>,
    establish: AttemptEstablishFacts,
    transport: ManifestAttemptTransport,
) -> Result<ManifestAssembledRound, TaskExecutionError> {
    let notify = Arc::new(tokio::sync::Notify::new());
    let wake = Arc::new(NotifyWake::new(Arc::clone(&notify))) as Arc<dyn StatusIntakeWake>;
    let plans = SubmissionFragmentPlans::index_manifest(submissions, manifest)?;
    let graph = build_task_graph_from_manifest(manifest, &plans, transport.transport_budget)?;
    let split_delivery = SplitDeliveryBridge::for_graph(&graph);
    let connector_blocking_io = transport.data_runtime.connector_blocking_io().clone();
    let convergence_source = Arc::clone(&transport.convergence_source);

    let mut backends = Vec::with_capacity(manifest.contexts().len());
    let mut admission_epochs = BTreeMap::new();
    let mut convergence_targets = Vec::with_capacity(manifest.contexts().len());
    for context in manifest.contexts() {
        let backend = context.backend();
        if backend.native_compatibility_id() != transport.wire.native_compatibility_id {
            return Err(TaskExecutionError::Schedule(format!(
                "manifest backend {} compatibility differs from the attempt wire identity",
                backend.process_id()
            )));
        }
        backends.push((backend.process_id(), backend.endpoint().clone()));
        convergence_targets.push(ManifestConvergenceTarget {
            context: context.context(),
            process: backend.process_id(),
            endpoint: backend.endpoint().clone(),
        });
        if admission_epochs
            .insert(backend.process_id(), backend.admission_epoch_capability())
            .is_some()
        {
            return Err(TaskExecutionError::Schedule(format!(
                "manifest repeats admission authority for backend {}",
                backend.process_id()
            )));
        }
    }

    let acks = TaskAckIntake::new(Arc::clone(&wake));
    let native = Arc::new(
        NativeTaskOperationSink::new(
            &backends,
            transport.transport_budget,
            transport.wire.clone(),
            acks.handle(),
            transport.data_runtime.clone(),
        )
        .map_err(TaskExecutionError::Schedule)?,
    ) as Arc<dyn TaskOperationSink>;
    let (actor_sink, actor_gate, actor_observer) = ActorGatedTaskOperationSink::pair(
        native,
        Arc::clone(&wake),
        transport.wire.native_compatibility_id,
    );
    let sink = split_delivery.sink(Arc::new(actor_sink) as Arc<dyn TaskOperationSink>);

    let intake = StatusIntake::new(STATUS_INTAKE_CAPACITY, Arc::clone(&wake));
    let subscriber = Arc::new(
        TaskStatusSubscriber::new(
            &backends,
            intake.handle(),
            transport.status_subscription_error_budget,
            transport.data_runtime,
        )
        .map_err(TaskExecutionError::Schedule)?,
    );
    let execution = QueryTaskExecution::new(
        graph,
        transport.dispatch_budget,
        transport.transport_budget,
        transport.wire.native_compatibility_id,
        &admission_epochs,
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
    .observing(Arc::clone(&split_delivery) as Arc<dyn AcknowledgementObserver>)
    .observing(actor_observer)
    .with_connector_blocking_io(connector_blocking_io);
    Ok(ManifestAssembledRound {
        round,
        split_delivery,
        actor_gate,
        convergence_source,
        convergence_targets: convergence_targets.into_boxed_slice(),
        notify,
        terminal: None,
        pending_terminal: None,
        convergence_started: false,
    })
}

impl ManifestAssembledRound {
    /// Installs the actor's one bounded Abort-effect intake before the attempt
    /// starts. The intake is part of C3's manifest-validated runtime resource
    /// projection; this owner never constructs a second actor port.
    pub(crate) fn install_abort_effect_intake(&mut self, intake: NativeAbortEffectIntake) {
        self.round.install_abort_effect_intake(intake);
    }

    /// Installs one manifest-validated RF, credential, or split-source pump.
    pub(crate) fn install_pump(&mut self, pump: Box<dyn TurnPump>) {
        self.round.add_pump(pump);
    }

    /// Completes C3's runtime-resource attachment. A round cannot turn before
    /// this explicit declaration, including when the exact pump set is empty.
    pub(crate) fn seal_runtime_owners(&mut self) {
        self.round.seal_pumps();
    }

    pub(crate) fn take_root_status_source(&mut self) -> Option<AcceptedRootStatusSource> {
        self.round.take_root_status_source()
    }

    /// Drives one fair Task-protocol turn and then releases at most one batch
    /// through the async actor gate. ACK observation intentionally happens in
    /// the round first, so actor settlement precedes authorization of the next
    /// batch while Task state remains single-owner.
    async fn advance(&mut self, drive: &NativeAttemptDrive) -> Result<bool, TaskExecutionError> {
        for pending in self.split_delivery.take_pending() {
            let delivery = pending.delivery();
            let task = pending.task();
            let admitted = self
                .round
                .execution_mut()
                .enqueue_task_update(task, pending.into_update());
            let reported = match &admitted {
                Ok(admission) => Ok(*admission),
                Err(error) => Err(error),
            };
            self.split_delivery
                .admit(delivery, reported)
                .map_err(|error| TaskExecutionError::Schedule(error.to_string()))?;
            admitted?;
        }
        let report = self.round.turn();
        // ACK observers run inside TaskRound before a state-machine error is
        // returned. Always let the actor gate settle those observed ACKs so a
        // local protocol failure cannot strand authorization responsibility.
        let gated = self.actor_gate.drive(drive).await;
        match (report, gated) {
            (Ok(report), Ok(gated)) => Ok(!report.is_idle() || gated != 0),
            (Err(error), _) => Err(error),
            (Ok(_), Err(error)) => Err(error),
        }
    }

    /// Runs the active attempt without occupying an OS thread. The method is
    /// borrowed and memoizes its terminal, so cancellation or panic of the
    /// caller's future cannot move the Task owner out of its supervisor.
    pub(crate) async fn run(
        &mut self,
        drive: &NativeAttemptDrive,
        cancellation: CancellationView,
        completion: ManifestAttemptCompletion,
    ) -> NativeAttemptTerminal {
        if let Some(terminal) = &self.terminal {
            return terminal.clone();
        }
        loop {
            let notify = Arc::clone(&self.notify);
            let notified = notify.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();

            let moved = match self.advance(drive).await {
                Ok(moved) => moved,
                Err(error) => {
                    self.pending_terminal
                        .get_or_insert_with(|| task_protocol_failure(error));
                    false
                }
            };
            if self.pending_terminal.is_some() && self.actor_gate.prepare_convergence() {
                let terminal = self
                    .pending_terminal
                    .take()
                    .expect("pending Task terminal was checked");
                self.terminal = Some(terminal.clone());
                return terminal;
            }
            if let Some(detail) = self.round.failure_cause() {
                // Derived peer failure is a placeholder. The same TaskRound
                // remains live until the originating Worker publishes the
                // authoritative cause, so recovery classification never fixes
                // an incomplete failure fact.
                if !detail.is_derived() {
                    let terminal =
                        NativeAttemptTerminal::Failed(NativeAttemptPreparationFailure::new(
                            AttemptFailureClass::ExecutionFailure,
                            QueryExecutionError::new(
                                QueryExecutionErrorKind::Failed,
                                format!("Native Task attempt terminated: {detail:?}"),
                            ),
                        ));
                    if self.actor_gate.prepare_convergence() {
                        self.terminal = Some(terminal.clone());
                        return terminal;
                    }
                }
            }
            let completed = match completion {
                ManifestAttemptCompletion::AcceptedRootSuccessSeal => {
                    self.round.accepted_root_success_sealed()
                }
                ManifestAttemptCompletion::AttemptDrained => self.round.attempt_drained(),
            };
            if completed && self.actor_gate.prepare_convergence() {
                let terminal = NativeAttemptTerminal::Completed;
                self.terminal = Some(terminal.clone());
                return terminal;
            }
            if let Some(reason) = cancellation.reason()
                && self.actor_gate.prepare_convergence()
            {
                let terminal = cancellation_failure(reason);
                self.terminal = Some(terminal.clone());
                return terminal;
            }
            if moved {
                continue;
            }
            tokio::select! {
                _ = notified => {}
                reason = cancellation.cancelled() => {
                    if self.actor_gate.prepare_convergence() {
                        let terminal = cancellation_failure(reason);
                        self.terminal = Some(terminal.clone());
                        return terminal;
                    }
                }
                _ = tokio::time::sleep(IDLE_RECHECK) => {}
            }
        }
    }

    /// Continues the same Task owner until every exact context has a positive
    /// closure fact. A Worker release proves local stop. Exact process
    /// replacement closes residual responsibility without claiming resource
    /// release. Cancellation and unobservability are never closure evidence.
    pub(crate) async fn converge(
        &mut self,
        _cancellation: CancellationView,
    ) -> NativeAttemptConvergence {
        if !self.convergence_started {
            self.split_delivery
                .abandon("Native attempt entered convergence");
            self.convergence_started = true;
        }
        loop {
            if self.round.attempt_drained() {
                return NativeAttemptConvergence::all_workers_stopped_and_contexts_fenced();
            }
            if let Some(convergence) = self.context_convergence() {
                return NativeAttemptConvergence::contexts(convergence);
            }
            let notify = Arc::clone(&self.notify);
            let notified = notify.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            let moved = self.round.turn().is_ok_and(|report| !report.is_idle());
            if moved {
                continue;
            }
            tokio::select! {
                _ = notified => {}
                _ = tokio::time::sleep(IDLE_RECHECK) => {}
            }
        }
    }

    fn context_convergence(&self) -> Option<Box<[NativeContextConvergence]>> {
        let mut convergence = Vec::with_capacity(self.convergence_targets.len());
        for target in &self.convergence_targets {
            if self.round.execution().context_released(target.context) {
                convergence.push(NativeContextConvergence::worker_stopped_and_context_fenced(
                    target.context,
                ));
                continue;
            }
            match self
                .convergence_source
                .observe_process_at_endpoint(target.process, &target.endpoint)
            {
                Ok(BackendProcessObservation::Replaced { .. }) => {
                    convergence.push(NativeContextConvergence::worker_process_replaced(
                        target.context,
                    ));
                }
                Ok(
                    BackendProcessObservation::Current | BackendProcessObservation::Unobservable,
                )
                | Err(_) => return None,
            }
        }
        Some(convergence.into_boxed_slice())
    }
}

fn task_protocol_failure(error: TaskExecutionError) -> NativeAttemptTerminal {
    let (class, kind) = match &error {
        TaskExecutionError::Capacity(_) => (
            AttemptFailureClass::ResourceGovernance,
            QueryExecutionErrorKind::Rejected,
        ),
        TaskExecutionError::ParticipantUnobservable { .. }
        | TaskExecutionError::QueueResidenceExpired { .. }
        | TaskExecutionError::ResultStream(_) => (
            AttemptFailureClass::RecoverableInfrastructure,
            QueryExecutionErrorKind::Failed,
        ),
        TaskExecutionError::OperationFailed { .. }
        | TaskExecutionError::IllegalTaskTransition { .. } => (
            AttemptFailureClass::ExecutionFailure,
            QueryExecutionErrorKind::Failed,
        ),
        _ => (
            AttemptFailureClass::ContractViolation,
            QueryExecutionErrorKind::InvalidRequest,
        ),
    };
    NativeAttemptTerminal::Failed(NativeAttemptPreparationFailure::new(
        class,
        QueryExecutionError::new(kind, format!("Native Task protocol failed: {error}")),
    ))
}

fn cancellation_failure(reason: CancellationReason) -> NativeAttemptTerminal {
    let (class, kind) = match reason {
        CancellationReason::DeadlineExceeded
        | CancellationReason::FrontendDrainDeadlineExceeded => (
            AttemptFailureClass::DeadlineExceeded,
            QueryExecutionErrorKind::DeadlineExceeded,
        ),
        _ => (
            AttemptFailureClass::Cancelled,
            QueryExecutionErrorKind::Cancelled,
        ),
    };
    NativeAttemptTerminal::Failed(NativeAttemptPreparationFailure::new(
        class,
        QueryExecutionError::new(kind, format!("Native Task attempt cancelled: {reason:?}")),
    ))
}
