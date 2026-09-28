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
use novarocks_query_application::coordination::{
    AcceptedRootStatusSource, AttemptFailureClass, NativeAttemptDrive,
    ReplacementWorkerAdmissionEvidence,
};
use novarocks_query_application::coordination::{DispatchBudget, OperationDispatchResult};
use novarocks_task_codec::TransportBudget;
use novarocks_workload_control::{CancellationReason, CancellationView};

use super::abort_effect::NativeAbortEffectIntake;
use super::actor_gate::{ActorGateOwner, ActorGatedTaskOperationSink};
use super::clock::{ProcessMonotonicClock, TaskProtocolClock};
use super::error::{ParticipantObservationFailure, TaskExecutionError};
use super::execution::QueryTaskExecution;
use super::graph::build_task_graph_from_manifest;
use super::intent::TaskOperationSink;
use super::round::{AcknowledgementObserver, TaskRound, TurnPump};
use super::sources::{AttemptEstablishFacts, SubmissionFragmentPlans};
use super::split_transport::SplitDeliveryBridge;
use super::status_intake::{NotifyWake, ObservationIntake, StatusIntake, StatusIntakeWake};
use crate::native::data_runtime::FrontendDataRuntime;
use crate::native::task_transport::{
    AttemptWireFacts, CoveredTaskStatusSubscriber, NativeTaskOperationSink, TaskAckIntake,
};
use crate::query_execution::artifact::{TaskManifestBinding, ValidatedNativeSubmission};
use crate::query_execution::split_assignment::TaskUpdateTransport;
use crate::query_execution::split_assignment_round::{
    RoundSplitAssignmentPlan, SplitAssignmentRoundGuard,
};
use novarocks_query_application::api::{
    BackendProcessObservation, BackendProcessObservationService,
};

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
    convergence_cleanup_started: bool,
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
    replacement_admissions: Option<Box<[ReplacementWorkerAdmissionEvidence]>>,
    transport: ManifestAttemptTransport,
) -> Result<ManifestAssembledRound, TaskExecutionError> {
    let notify = Arc::new(tokio::sync::Notify::new());
    let wake = Arc::new(NotifyWake::new(Arc::clone(&notify))) as Arc<dyn StatusIntakeWake>;
    let mut plans = SubmissionFragmentPlans::index_manifest(submissions, manifest)?;
    let graph = build_task_graph_from_manifest(manifest, &mut plans, transport.transport_budget)?;
    let split_delivery = SplitDeliveryBridge::for_graph(&graph);
    let connector_blocking_io = transport.data_runtime.connector_blocking_io().clone();
    let convergence_source = Arc::clone(&transport.convergence_source);

    let mut backends = Vec::with_capacity(manifest.contexts().len());
    let mut admission_epochs = BTreeMap::new();
    let mut preparing_positions = BTreeMap::new();
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
        preparing_positions.insert(
            backend.process_id(),
            backend.target().descriptor().preparing_positions(),
        );
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
    let covered_subscriber = Arc::new(
        CoveredTaskStatusSubscriber::new(
            &backends,
            Arc::clone(&observation),
            transport.status_subscription_error_budget,
            transport.data_runtime,
        )
        .map_err(TaskExecutionError::Schedule)?,
    );
    let mut execution = QueryTaskExecution::new(
        graph,
        transport.dispatch_budget,
        transport.transport_budget,
        transport.wire.native_compatibility_id,
        &admission_epochs,
        &preparing_positions,
        clock,
        sink,
        intake,
    )?;
    if let Some(admissions) = replacement_admissions {
        execution.adopt_replacement_admissions(admissions)?;
    }
    let round = TaskRound::new_covered(
        execution,
        acks,
        Box::new(establish),
        covered_subscriber,
        observation,
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
        convergence_cleanup_started: false,
    })
}

impl ManifestAssembledRound {
    pub(crate) fn install_split_assignment(
        &mut self,
        execution: novarocks_types::QueryExecutionId,
        plan: RoundSplitAssignmentPlan,
        data_runtime: FrontendDataRuntime,
    ) -> Option<SplitAssignmentRoundGuard> {
        SplitAssignmentRoundGuard::install(
            &mut self.round,
            execution,
            plan,
            Arc::clone(&self.split_delivery) as Arc<dyn TaskUpdateTransport>,
            data_runtime,
            Arc::new(NotifyWake::new(Arc::clone(&self.notify))) as Arc<dyn StatusIntakeWake>,
        )
    }

    pub(crate) fn abort_wake(&self) -> Arc<dyn StatusIntakeWake> {
        Arc::new(NotifyWake::new(Arc::clone(&self.notify)))
    }

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
        // returned. Settle all observed ACKs before publishing that error,
        // without authorizing another batch for the terminal attempt.
        match report {
            Ok(report) => {
                let gated = self.actor_gate.drive(drive).await?;
                Ok(!report.is_idle() || gated != 0)
            }
            Err(error) => {
                let _ = self
                    .actor_gate
                    .settle_terminal_acknowledgements(drive)
                    .await;
                Err(error)
            }
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
            let success_sealed = completion == ManifestAttemptCompletion::AcceptedRootSuccessSeal
                && self.round.accepted_root_success_sealed();
            if success_sealed {
                if let Some(residual) = self.pending_terminal.take() {
                    tracing::warn!(
                        ?residual,
                        "Task failure after the accepted root seal belongs to cleanup"
                    );
                }
                if self.actor_gate.prepare_convergence() {
                    let terminal = NativeAttemptTerminal::Completed;
                    self.terminal = Some(terminal.clone());
                    return terminal;
                }
            }
            if !success_sealed && self.pending_terminal.is_some() {
                // A Task-protocol failure is already an authoritative attempt
                // terminal. Closing the actor gate prevents any further
                // admission, but outstanding actor-owned transport effects
                // are residual convergence work: waiting for them here would
                // deadlock a failure that needs Query Application to issue its
                // Abort effects first.
                let _ = self.actor_gate.prepare_convergence();
                let terminal = self
                    .pending_terminal
                    .take()
                    .expect("pending Task terminal was checked");
                self.terminal = Some(terminal.clone());
                return terminal;
            }
            if !success_sealed && let Some(detail) = self.round.failure_cause() {
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
            if !success_sealed
                && let Some(reason) = cancellation.reason()
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
                reason = cancellation.cancelled(), if !success_sealed => {
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
        cancellation: CancellationView,
    ) -> NativeAttemptConvergence {
        if !self.convergence_started {
            begin_convergence(
                &mut self.round,
                &self.split_delivery,
                self.terminal.as_ref(),
            );
            self.convergence_cleanup_started =
                !matches!(self.terminal, Some(NativeAttemptTerminal::Completed));
            self.convergence_started = true;
        }
        loop {
            if !self.convergence_cleanup_started
                && (cancellation.reason().is_some() || self.round.failure_cause().is_some())
            {
                tracing::warn!(
                    terminal = ?self.terminal,
                    cancellation = ?cancellation.reason(),
                    failure = ?self.round.failure_cause(),
                    "Native convergence entered terminal cleanup after a late cancellation or Task failure"
                );
                begin_terminal_cleanup(&mut self.round, &self.split_delivery);
                self.convergence_cleanup_started = true;
            }
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
            let moved = match self.round.turn() {
                Ok(report) => !report.is_idle(),
                Err(error) => {
                    if !self.convergence_cleanup_started {
                        tracing::warn!(
                            terminal = ?self.terminal,
                            error = %error,
                            "Native convergence Task turn failed before cleanup"
                        );
                    }
                    if !self.convergence_cleanup_started {
                        begin_terminal_cleanup(&mut self.round, &self.split_delivery);
                        self.convergence_cleanup_started = true;
                    }
                    false
                }
            };
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
            if self
                .round
                .execution()
                .context_never_established(target.context)
            {
                // Establish is the only operation that can create this
                // Worker context. A context whose Establish was never minted
                // could not have hosted any of its planned Tasks, even if a
                // queued admission or Create request is still settling.
                convergence.push(NativeContextConvergence::never_established(target.context));
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

/// Successful root sealing fixes the client-visible result before upstream
/// tasks necessarily stop. Keep their status streams and context lifecycle
/// live until the exact terminal statuses can authorize Release.
pub(super) fn begin_convergence(
    round: &mut TaskRound,
    split_delivery: &SplitDeliveryBridge,
    terminal: Option<&NativeAttemptTerminal>,
) {
    if matches!(terminal, Some(NativeAttemptTerminal::Completed)) {
        round.begin_normal_drain();
        split_delivery.abandon("Native attempt entered normal drain");
    } else {
        begin_terminal_cleanup(round, split_delivery);
    }
}

fn begin_terminal_cleanup(round: &mut TaskRound, split_delivery: &SplitDeliveryBridge) {
    round.begin_terminal_cleanup();
    split_delivery.abandon("Native attempt entered convergence");
}

fn task_protocol_failure(error: TaskExecutionError) -> NativeAttemptTerminal {
    let topology_requirement = match &error {
        TaskExecutionError::ParticipantUnobservable {
            backend,
            state: ParticipantObservationFailure::Transport(_),
        }
        | TaskExecutionError::PreReadyEstablishTransportUnknown { backend }
        | TaskExecutionError::PreReadyEstablishRejected {
            backend,
            outcome: novarocks_execution_contract::OperationOutcome::IdentityMismatch,
        } => novarocks_query_application::api::NativeAttemptTopologyRequirement::ExcludeProcess(
            *backend,
        ),
        _ => novarocks_query_application::api::NativeAttemptTopologyRequirement::LiveSnapshot,
    };
    let (class, kind) = match &error {
        TaskExecutionError::Capacity(_) => (
            AttemptFailureClass::ResourceGovernance,
            QueryExecutionErrorKind::Rejected,
        ),
        TaskExecutionError::DispatchRejected {
            result: OperationDispatchResult::IngressRejected(_),
            ..
        } => (
            AttemptFailureClass::ResourceGovernance,
            QueryExecutionErrorKind::Rejected,
        ),
        TaskExecutionError::DispatchRejected {
            result: OperationDispatchResult::NonWorkerRejected,
            ..
        } => (
            AttemptFailureClass::ContractViolation,
            QueryExecutionErrorKind::InvalidRequest,
        ),
        TaskExecutionError::ParticipantUnobservable {
            state: ParticipantObservationFailure::IdentityViolation(_),
            ..
        } => (
            AttemptFailureClass::ContractViolation,
            QueryExecutionErrorKind::InvalidRequest,
        ),
        TaskExecutionError::ParticipantUnobservable { .. }
        | TaskExecutionError::QueueResidenceExpired { .. }
        | TaskExecutionError::PreReadyEstablishTransportUnknown { .. }
        | TaskExecutionError::PreReadyEstablishRejected { .. }
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
    NativeAttemptTerminal::Failed(
        NativeAttemptPreparationFailure::new(
            class,
            QueryExecutionError::new(kind, format!("Native Task protocol failed: {error}")),
        )
        .with_topology_requirement(topology_requirement),
    )
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

#[cfg(test)]
mod tests {
    use super::*;

    struct UnusedConvergenceSource;

    impl novarocks_query_application::api::BackendProcessObservationPort for UnusedConvergenceSource {
        fn observe_process_at_endpoint(
            &self,
            _: novarocks_types::BackendProcessId,
            _: &novarocks_execution::runtime::endpoint::RuntimeEndpoint,
        ) -> Result<BackendProcessObservation, novarocks_query_application::api::BackendTopologyError>
        {
            panic!("attempt terminal arbitration must not inspect residual convergence");
        }
    }

    async fn terminal_regression_drive(
        context: novarocks_execution_contract::QueryContextRef,
    ) -> (
        novarocks_query_application::test_support::LogicalExecutionTestHarness,
        NativeAttemptDrive,
        CancellationView,
        novarocks_workload_control::WorkCancellationRequester,
        super::super::abort_effect::NativeAbortEffectIntake,
    ) {
        use novarocks_query_application::coordination::{
            AbortQueryContextEffectPort, ExecutionEffect, LogicalExecutionActorConfig,
        };
        use novarocks_query_application::test_support::LogicalExecutionTestHarness;
        use novarocks_workload_control::{
            ResourceConfig, Stage, WorkClass, WorkRequest, WorkloadConfig, WorkloadControl,
        };
        use std::num::NonZeroUsize;
        let control = WorkloadControl::try_new(
            WorkloadConfig::default(),
            ResourceConfig {
                total_bytes: 1 << 20,
                control_bytes: 1 << 10,
                per_scope_bytes: (1 << 20) - (1 << 10),
            },
        )
        .unwrap();
        control.mark_ready().unwrap();
        let governed = control
            .try_begin_root(WorkRequest::new(WorkClass::Query))
            .unwrap();
        let cancellation = governed.owner.scope().cancellation().unwrap();
        let requester = governed.owner.cancellation_requester();
        let execution_stage = governed
            .owner
            .scope()
            .try_acquire(Stage::Execution)
            .unwrap();
        let (adapter, intake) = super::super::abort_effect::NativeAbortEffectAdapter::bounded(
            NonZeroUsize::new(2).unwrap(),
            Arc::new(super::super::status_intake::CountingWake::default()),
        );
        let config = LogicalExecutionActorConfig::single_attempt_completion(
            context.query_execution_id(),
            ExecutionEffect::None,
            NonZeroUsize::new(2).unwrap(),
            vec![context],
            NonZeroUsize::new(2).unwrap(),
            NonZeroUsize::new(2).unwrap(),
            governed.owner,
            execution_stage,
        )
        .unwrap()
        .with_abort_query_context_effect_port(
            adapter as Arc<dyn AbortQueryContextEffectPort>,
            NonZeroUsize::new(2).unwrap(),
        );
        let mut logical = LogicalExecutionTestHarness::install(
            tokio::runtime::Handle::current(),
            config,
            context.query_execution_id(),
            vec![context],
        )
        .unwrap();
        logical.activate_initial().await.unwrap();
        let drive = logical.native_attempt_drive();
        (logical, drive, cancellation, requester, intake)
    }

    #[derive(Debug)]
    struct BackpressuredGateTransport;

    impl TaskOperationSink for BackpressuredGateTransport {
        fn try_reserve_queue(
            &self,
            _: super::super::intent::TaskOperationQueueRequest,
        ) -> super::super::intent::TaskOperationQueueAdmission {
            super::super::intent::TaskOperationQueueAdmission::Admitted(
                super::super::intent::test_queue_permit(),
            )
        }

        fn try_submit(
            &self,
            batch: super::super::intent::DispatchBatch,
        ) -> super::super::intent::TaskOperationSubmit {
            super::super::intent::TaskOperationSubmit::Backpressured(batch)
        }
    }

    #[tokio::test]
    async fn accepted_root_seal_wins_residual_native_advance_error_before_reply_consumption() {
        use super::super::intent::OperationAcknowledgement;
        use novarocks_execution::task_execution::{OperationKind, TaskOperationId};
        for (accept_before_error, cancel_with_pending_gate) in
            [(true, false), (false, false), (true, true)]
        {
            let (mut round, acks) = super::super::tests::manifest_terminal_regression_fixture();
            let context = *round.execution().graph().contexts().next().unwrap();
            let (mut logical, drive, cancellation, requester, _abort_intake) =
                terminal_regression_drive(context).await;
            let source = round.take_root_status_source().unwrap();
            let mut reply = source.begin_success_seal_request().unwrap();
            if accept_before_error {
                round.turn().unwrap();
                assert!(round.accepted_root_success_sealed());
            }
            // This Native ACK is deliberately not a sent operation. It takes
            // the actual advance error path before run inspects the seal; the
            // result pump still holds its unread seal reply throughout run.
            acks.publish(OperationAcknowledgement::transport_unknown(
                TaskOperationId::new_v7(),
                OperationKind::UpdateTask,
            ));
            let (sink, actor_gate, _) = ActorGatedTaskOperationSink::pair(
                Arc::new(BackpressuredGateTransport),
                Arc::new(super::super::status_intake::CountingWake::default()),
                novarocks_types::NativeCompatibilityId::new([0x41; 32]),
            );
            let cancellation_after_gate_closes = if cancel_with_pending_gate {
                use super::super::intent::{DispatchBatch, OperationIntent, TaskOperationSubmit};
                use novarocks_execution::task_execution::{
                    AcquireQueryContextAdmissionTicket, AdmissionEpochCapability, LeaseValidFor,
                };
                use novarocks_query_application::coordination::DispatchLane;
                let operation = OperationIntent::AcquireQueryContextAdmissionTicket(
                    AcquireQueryContextAdmissionTicket::new(
                        TaskOperationId::new_v7(),
                        context,
                        LeaseValidFor::new(std::time::Duration::from_secs(30)).unwrap(),
                        novarocks_types::NativeCompatibilityId::new([0x41; 32]),
                        AdmissionEpochCapability::try_from_bytes([9; 16]).unwrap(),
                    ),
                );
                let bytes = operation.queue_request().queued_bytes();
                let batch = DispatchBatch::test_fixture(
                    context.backend_process_id(),
                    DispatchLane::Lifecycle,
                    vec![operation],
                    bytes,
                );
                let TaskOperationSubmit::Backpressured(mut retained) = sink.try_submit(batch)
                else {
                    panic!("actor authorization must retain the unsent carrier");
                };
                Some(tokio::spawn(async move {
                    loop {
                        tokio::task::yield_now().await;
                        match sink.try_submit(retained) {
                            TaskOperationSubmit::Backpressured(batch) => retained = batch,
                            TaskOperationSubmit::Rejected { reason, .. } => {
                                assert!(reason.contains("closing"));
                                // This rejection removes the authorized carrier only
                                // after run's first convergence attempt returned false.
                                requester.request(CancellationReason::Requested).unwrap();
                                break;
                            }
                            other => panic!("unsent carrier unexpectedly escaped: {other:?}"),
                        }
                    }
                }))
            } else {
                None
            };
            let split_delivery = SplitDeliveryBridge::for_graph(round.execution().graph());
            let mut assembled = ManifestAssembledRound {
                round,
                split_delivery,
                actor_gate,
                convergence_source: Arc::new(UnusedConvergenceSource),
                convergence_targets: Box::new([]),
                notify: Arc::new(tokio::sync::Notify::new()),
                terminal: None,
                pending_terminal: None,
                convergence_started: false,
                convergence_cleanup_started: false,
            };
            let terminal = tokio::time::timeout(
                std::time::Duration::from_secs(2),
                assembled.run(
                    &drive,
                    cancellation,
                    ManifestAttemptCompletion::AcceptedRootSuccessSeal,
                ),
            )
            .await
            .expect("terminal arbitration must finish without residual cleanup");
            if let Some(release) = cancellation_after_gate_closes {
                release.await.unwrap();
            }
            if accept_before_error {
                assert!(matches!(terminal, NativeAttemptTerminal::Completed));
                assert_eq!(reply.try_recv().unwrap(), Ok(()));
            } else {
                assert!(matches!(terminal, NativeAttemptTerminal::Failed(_)));
                assert!(!assembled.round.accepted_root_success_sealed());
                assert!(!matches!(reply.try_recv(), Ok(Ok(()))));
            }
            drop(assembled);
            drop(drive);
            logical.abandon_running_attempt();
            tokio::time::timeout(std::time::Duration::from_secs(2), async {
                while logical
                    .stand_down_snapshot(context)
                    .await
                    .unwrap()
                    .is_none()
                {
                    tokio::task::yield_now().await;
                }
            })
            .await
            .unwrap();
            // This closes only the fixture actor's residual scope during
            // explicit teardown, after the terminal assertions above.
            logical
                .observe_worker_process_replaced(context)
                .await
                .unwrap();
            logical
                .finish_until(std::time::Instant::now() + std::time::Duration::from_secs(2))
                .await
                .unwrap();
        }
    }

    #[test]
    fn foreign_status_identity_violation_refuses_attempt_recovery() {
        let terminal = task_protocol_failure(TaskExecutionError::ParticipantUnobservable {
            backend: novarocks_types::identity::BackendProcessId::new_v7(),
            state: ParticipantObservationFailure::IdentityViolation("process_mismatch"),
        });
        let NativeAttemptTerminal::Failed(failure) = terminal else {
            panic!("task protocol refusal must fail the attempt");
        };
        assert_eq!(failure.class(), AttemptFailureClass::ContractViolation);
        assert_eq!(
            failure.error().kind(),
            QueryExecutionErrorKind::InvalidRequest
        );
    }

    #[test]
    fn pre_ready_establish_unknown_preserves_whole_attempt_recovery() {
        let backend = novarocks_types::identity::BackendProcessId::new_v7();
        let terminal =
            task_protocol_failure(TaskExecutionError::PreReadyEstablishTransportUnknown {
                backend,
            });
        let NativeAttemptTerminal::Failed(failure) = terminal else {
            panic!("transport uncertainty must terminate the old attempt");
        };
        assert_eq!(
            failure.class(),
            AttemptFailureClass::RecoverableInfrastructure
        );
        assert_eq!(failure.error().kind(), QueryExecutionErrorKind::Failed);
        assert_eq!(
            failure.topology_requirement(),
            novarocks_query_application::api::NativeAttemptTopologyRequirement::ExcludeProcess(
                backend
            )
        );
    }

    #[test]
    fn pre_ready_establish_rejection_preserves_whole_attempt_recovery() {
        let terminal = task_protocol_failure(TaskExecutionError::PreReadyEstablishRejected {
            backend: novarocks_types::identity::BackendProcessId::new_v7(),
            outcome: novarocks_execution::task_execution::OperationOutcome::InvalidStateOrRequest,
        });
        let NativeAttemptTerminal::Failed(failure) = terminal else {
            panic!("pre-ready Establish rejection must terminate the old attempt");
        };
        assert_eq!(
            failure.class(),
            AttemptFailureClass::RecoverableInfrastructure
        );
        assert_eq!(failure.error().kind(), QueryExecutionErrorKind::Failed);
    }
    #[test]
    fn pre_ready_identity_mismatch_excludes_the_failed_process() {
        let backend = novarocks_types::identity::BackendProcessId::new_v7();
        let NativeAttemptTerminal::Failed(failure) =
            task_protocol_failure(TaskExecutionError::PreReadyEstablishRejected {
                backend,
                outcome: novarocks_execution_contract::OperationOutcome::IdentityMismatch,
            })
        else {
            panic!("identity mismatch must fail the attempt");
        };
        assert_eq!(
            failure.topology_requirement(),
            novarocks_query_application::api::NativeAttemptTopologyRequirement::ExcludeProcess(
                backend
            )
        );
    }
    #[test]
    fn lost_status_transport_preserves_failed_process_for_replacement() {
        let backend = novarocks_types::identity::BackendProcessId::new_v7();
        let NativeAttemptTerminal::Failed(failure) =
            task_protocol_failure(TaskExecutionError::ParticipantUnobservable {
                backend,
                state: ParticipantObservationFailure::Transport("stream_lost"),
            })
        else {
            panic!("lost participant must fail the attempt");
        };
        assert_eq!(
            failure.class(),
            AttemptFailureClass::RecoverableInfrastructure
        );
        assert_eq!(
            failure.topology_requirement(),
            novarocks_query_application::api::NativeAttemptTopologyRequirement::ExcludeProcess(
                backend
            )
        );
    }
}
