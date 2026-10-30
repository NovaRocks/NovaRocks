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

use crate::task_protocol_config::DispatchBudget;
use novarocks_query_application::api::{
    NativeActiveConvergenceClosure, NativeActiveConvergenceRequest,
    NativeAttemptPreparationFailure, NativeAttemptTerminal, StatementNativeHealthFailure,
};
use novarocks_query_application::coordination::{
    AcceptedRootStatusSource, NativeAttemptDrive, ReplacementWorkerAdmissionEvidence,
};
use novarocks_task_codec::TransportBudget;
use novarocks_workload_control::{CancellationReason, CancellationView};

use super::abort_effect::NativeAbortEffectIntake;
use super::actor_gate::{ActorGateOwner, ActorGatedTaskOperationSink};
use super::attempt_convergence::{
    NativeAttemptConvergenceMode, NativeAttemptConvergenceOwner, NativeAttemptConvergenceTarget,
    advance_task_round,
};
use super::error::{ParticipantObservationFailure, TaskExecutionError};
use super::execution::QueryTaskExecution;
use super::graph::build_task_graph_from_manifest;
use super::intent::TaskOperationSink;
use super::round::{AcknowledgementObserver, TaskRound};
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
    RoundSplitAssignmentPlan, SplitAssignmentFailureSource, SplitAssignmentRoundGuard,
};
use novarocks_coordination_contract::MonotonicClock;
use novarocks_query_application::api::BackendProcessObservationService;

const STATUS_INTAKE_CAPACITY: usize = 4096;
const IDLE_RECHECK: std::time::Duration = std::time::Duration::from_millis(5);

/// Process-owned runtime policy used to instantiate one exact manifest.
pub(crate) struct ManifestAttemptTransport {
    pub(crate) clock: Arc<dyn MonotonicClock>,
    pub(crate) dispatch_budget: DispatchBudget,
    pub(crate) transport_budget: TransportBudget,
    pub(crate) status_subscription_error_budget: u32,
    pub(crate) wire: AttemptWireFacts,
    pub(crate) data_runtime: FrontendDataRuntime,
    pub(crate) subscription_scope: novarocks_workload_control::WorkScope,
    pub(crate) source_cohort:
        crate::query_execution::native_execution_adapter::source_cohort::NativeSourceCohort,
    pub(crate) native_actions:
        novarocks_workload_control::joint_admission::NativeAsyncActionAuthority,
    /// Exact process evidence bounds active pre-ready observation and residual
    /// convergence. It cannot change the frozen manifest or schedule a successor.
    pub(crate) convergence_source: BackendProcessObservationService,
}

/// One active Task protocol owner assembled without a scheduling capability.
pub(crate) struct ManifestAssembledRound {
    pub(crate) round: TaskRound,
    pub(crate) split_delivery: Arc<SplitDeliveryBridge>,
    pub(crate) actor_gate: ActorGateOwner,
    protocol_facts: Option<super::protocol_facts::AttemptProtocolFactsReceiver>,
    convergence: NativeAttemptConvergenceOwner,
    notify: Arc<tokio::sync::Notify>,
    terminal: Option<NativeAttemptTerminal>,
    pending_terminal: Option<PendingAttemptTerminal>,
    /// Only an exact Establish uncertainty or correlated identity rejection
    /// observes replacement within its original admission receipt horizon.
    pending_pre_ready: Option<PendingPreReadyObservation>,
    split_failure: Option<SplitAssignmentFailureSource>,
    profile_diagnostics: Option<super::profile_diagnostics::TaskFinalInfoCollector>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum PendingPreReadyObservation {
    TransportUnknown(novarocks_types::BackendProcessId),
    RejectedIdentity {
        context: novarocks_execution_contract::QueryContextRef,
        operation_id: novarocks_execution_contract::TaskOperationId,
        kind: novarocks_execution_contract::OperationKind,
    },
}
impl PendingPreReadyObservation {
    fn is_unknown(self) -> bool {
        matches!(self, Self::TransportUnknown(_))
    }
}

#[derive(Debug)]
struct PendingAttemptTerminal {
    terminal: NativeAttemptTerminal,
    allows_accepted_task_cause: bool,
}

/// The completion fact this Task owner must wait for before it reports the
/// physical attempt terminal to Query Application.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ManifestAttemptCompletion {
    /// A row result pump owns output and asks TaskRound to seal the exact root
    /// success after it has consumed Worker EOS.
    AcceptedRootSuccessSeal,
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
    // Freeze declaration before submissions are consumed; never infer writers
    // from the subset of Task statuses that happens to report writer facts.
    let writers = manifest
        .writer_cohorts()
        .map(|cohort| cohort.task())
        .collect::<std::collections::BTreeSet<_>>();
    let summary_sources = manifest
        .summary_sources()
        .map(|(task, _)| task)
        .collect::<std::collections::BTreeSet<_>>();
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
        backends.push(backend.target().descriptor().clone());
        preparing_positions.insert(
            backend.process_id(),
            backend.target().descriptor().preparing_positions(),
        );
        convergence_targets.push(NativeAttemptConvergenceTarget::new(
            context.context(),
            backend.endpoint().clone(),
        ));
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
            transport.native_actions,
        )
        .map_err(TaskExecutionError::NativeTransport)?,
    ) as Arc<dyn TaskOperationSink>;
    let (actor_sink, actor_gate, actor_observer) = ActorGatedTaskOperationSink::pair(
        native,
        Arc::clone(&wake),
        transport.wire.native_compatibility_id,
    );
    let sink = split_delivery.sink(Arc::new(actor_sink) as Arc<dyn TaskOperationSink>);

    let intake = StatusIntake::new(STATUS_INTAKE_CAPACITY, Arc::clone(&wake));
    let clock = Arc::clone(&transport.clock);
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
        CoveredTaskStatusSubscriber::new_with_source_cohort(
            &backends,
            Arc::clone(&observation),
            transport.status_subscription_error_budget,
            transport.data_runtime,
            transport.subscription_scope,
            transport.source_cohort,
        )
        .map_err(TaskExecutionError::NativeTransport)?,
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
        execution.adopt_replacement_admissions(&admissions)?;
    }
    let mut round = TaskRound::new_covered(
        execution,
        acks,
        Box::new(establish),
        covered_subscriber,
        observation,
    )
    .observing(Arc::clone(&split_delivery) as Arc<dyn AcknowledgementObserver>)
    .observing(actor_observer)
    .with_connector_blocking_io(connector_blocking_io);
    let protocol_facts =
        round.install_protocol_facts(manifest.plan(), &writers, &summary_sources)?;
    let convergence =
        NativeAttemptConvergenceOwner::new(&round, convergence_source, convergence_targets)?;
    Ok(ManifestAssembledRound {
        round,
        split_delivery,
        actor_gate,
        protocol_facts: Some(protocol_facts),
        convergence,
        notify,
        terminal: None,
        pending_terminal: None,
        pending_pre_ready: None,
        split_failure: None,
        profile_diagnostics: None,
    })
}

impl ManifestAssembledRound {
    /// Read only the terminal selected by this original attempt owner.
    pub(crate) fn observed_failure(
        &self,
    ) -> Option<novarocks_query_application::execution_error::ExecutionFailureInput> {
        match self.terminal.as_ref() {
            Some(NativeAttemptTerminal::Failed(failure)) => Some(failure.input().clone()),
            _ => None,
        }
    }

    pub(crate) fn install_split_assignment(
        &mut self,
        execution: novarocks_types::QueryExecutionId,
        plan: RoundSplitAssignmentPlan,
        data_runtime: FrontendDataRuntime,
    ) -> Option<SplitAssignmentRoundGuard> {
        let guard = SplitAssignmentRoundGuard::install(
            &mut self.round,
            execution,
            plan,
            Arc::clone(&self.split_delivery) as Arc<dyn TaskUpdateTransport>,
            data_runtime,
            Arc::new(NotifyWake::new(Arc::clone(&self.notify))) as Arc<dyn StatusIntakeWake>,
        );
        self.split_failure = guard
            .as_ref()
            .map(SplitAssignmentRoundGuard::failure_source);
        self.convergence
            .bind_split_failure_source(self.split_failure.clone());
        if let Some(diagnostics) = self.profile_diagnostics.as_mut() {
            diagnostics.set_split_profile_source(
                guard
                    .as_ref()
                    .map(SplitAssignmentRoundGuard::profile_source),
            );
        }
        guard
    }

    pub(crate) fn install_profile_diagnostics(
        &mut self,
        transport: Arc<dyn crate::native::fragment_transport::TaskResultTransport>,
    ) -> Result<super::profile_diagnostics::ProfileDiagnosticObserver, TaskExecutionError> {
        if self.profile_diagnostics.is_some() {
            return Err(TaskExecutionError::Schedule(
                "profile diagnostics are already installed".to_owned(),
            ));
        }
        let mut collector = super::profile_diagnostics::TaskFinalInfoCollector::new(
            transport,
            true,
            &self.round,
            Arc::new(NotifyWake::new(Arc::clone(&self.notify))),
        );
        let observer = collector.enable_profile_observation()?;
        self.profile_diagnostics = Some(collector);
        Ok(observer)
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

    /// Transfer only this attempt's original receiver to its fixed FA consumer.
    /// A replacement owns a distinct pair, not a shared "latest" projection.
    pub(crate) fn take_protocol_facts(
        &mut self,
    ) -> Option<super::protocol_facts::AttemptProtocolFactsReceiver> {
        self.protocol_facts.take()
    }

    pub(crate) fn take_root_status_source(&mut self) -> Option<AcceptedRootStatusSource> {
        self.round.take_root_status_source()
    }

    /// Drives one fair Task-protocol turn and then releases at most one batch
    /// through the async actor gate. ACK observation intentionally happens in
    /// the round first, so actor settlement precedes authorization of the next
    /// batch while Task state remains single-owner.
    async fn advance(&mut self, drive: &NativeAttemptDrive) -> Result<bool, TaskExecutionError> {
        let report = advance_task_round(&mut self.round, &self.split_delivery);
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

    async fn receive_pending(
        &mut self,
        drive: &NativeAttemptDrive,
    ) -> Result<bool, TaskExecutionError> {
        let report = self.round.receive_without_dispatch();
        // ACK observers are still settled by their original actor owner.
        // No authorize_intents call is legal while evidence is pending.
        let settled = self
            .actor_gate
            .settle_terminal_acknowledgements(drive)
            .await;
        let report = report?;
        Ok(!report.is_idle() || settled? != 0)
    }

    fn record_protocol_failure(&mut self, error: TaskExecutionError) {
        let observation = match &error {
            TaskExecutionError::PreReadyEstablishTransportUnknown { backend } => {
                Some(PendingPreReadyObservation::TransportUnknown(*backend))
            }
            TaskExecutionError::PreReadyEstablishRejected {
                context,
                operation_id,
                rejection:
                    novarocks_execution_contract::operation::OperationResult::Verdict {
                        outcome: novarocks_execution_contract::OperationOutcome::IdentityMismatch,
                        ..
                    },
            } if self
                .round
                .execution()
                .rejected_establish_observation_remaining(*context, *operation_id)
                .is_some_and(|remaining| !remaining.is_zero()) =>
            {
                Some(PendingPreReadyObservation::RejectedIdentity {
                    context: *context,
                    operation_id: *operation_id,
                    kind: novarocks_execution_contract::OperationKind::UpdateQueryContext,
                })
            }
            TaskExecutionError::OperationFailed {
                context,
                operation_id,
                kind: novarocks_execution_contract::OperationKind::CreateTask,
                rejection:
                    novarocks_execution_contract::operation::OperationResult::Verdict {
                        outcome: novarocks_execution_contract::OperationOutcome::IdentityMismatch,
                        ..
                    },
            } if self
                .round
                .execution()
                .rejected_create_observation_remaining(*context, *operation_id)
                .is_some_and(|remaining| !remaining.is_zero()) =>
            {
                Some(PendingPreReadyObservation::RejectedIdentity {
                    context: *context,
                    operation_id: *operation_id,
                    kind: novarocks_execution_contract::OperationKind::CreateTask,
                })
            }
            _ => None,
        };
        if self.pending_terminal.is_none()
            || (self
                .pending_pre_ready
                .is_some_and(PendingPreReadyObservation::is_unknown)
                && !observation.is_some_and(PendingPreReadyObservation::is_unknown))
        {
            tracing::warn!(
                ?error,
                "Task protocol recorded its original attempt failure"
            );
            self.pending_pre_ready = observation;
            // Only ordinary observation loss can yield to a Task cause already
            // accepted at this same serial cut. Protocol/identity/control errors
            // retain their original priority; no projected reason is reclassified.
            let allows_accepted_task_cause = matches!(
                &error,
                TaskExecutionError::ParticipantUnobservable {
                    state: ParticipantObservationFailure::ConnectionLost
                        | ParticipantObservationFailure::Transport(_),
                    ..
                }
            );
            self.pending_terminal = Some(PendingAttemptTerminal {
                terminal: task_protocol_failure(error),
                allows_accepted_task_cause,
            });
        }
    }

    /// A positive exact replacement is a separate originating fact. Neither
    /// expiry nor uncertainty can supply it, and no recovery policy lives here.
    fn pending_pre_ready_must_wait(&mut self) -> bool {
        let Some(observation) = self.pending_pre_ready else {
            return false;
        };
        let backend = match observation {
            PendingPreReadyObservation::TransportUnknown(backend) => backend,
            PendingPreReadyObservation::RejectedIdentity {
                context,
                operation_id,
                kind,
            } => {
                // Both paths retain their original horizon on the Task clock.
                // Create uses the existing effective context lease captured at
                // its exact rejection, never a newly started grace interval.
                let within_horizon = |this: &Self| {
                    let execution = this.round.execution();
                    match kind {
                        novarocks_execution_contract::OperationKind::UpdateQueryContext => {
                            execution
                                .rejected_establish_observation_remaining(context, operation_id)
                        }
                        novarocks_execution_contract::OperationKind::CreateTask => {
                            execution.rejected_create_observation_remaining(context, operation_id)
                        }
                        _ => None,
                    }
                    .is_some_and(|remaining| !remaining.is_zero())
                };
                if !within_horizon(self) {
                    self.pending_pre_ready = None;
                    return false;
                }
                if let Some(replacement) = self
                    .convergence
                    .replacement_process(context.backend_process_id())
                {
                    // The observation cannot cross the original horizon while
                    // querying the registry and then authorize a late retry.
                    if within_horizon(self)
                        && let Some(PendingAttemptTerminal {
                            terminal: NativeAttemptTerminal::Failed(failure), ..
                        }) = self.pending_terminal.as_mut()
                        && let Ok(witness) = novarocks_query_application::api::
                            PreReadyProcessReplacementWitness::try_new(
                                failure.input(), context, operation_id, replacement,
                            )
                    {
                        // Rejection keeps the same original failure in place.
                        let _ = failure.attach_pre_ready_replacement(witness);
                    }
                    self.pending_pre_ready = None;
                    return false;
                }
                return true;
            }
        };
        if self.convergence.process_replaced(backend) {
            use novarocks_execution_contract::{SafeDetail, failure::FailureProjection};
            use novarocks_query_application::execution_error::{
                ExecutionFailureInput, ExecutionFailureSite, QueryFailureReason,
            };
            self.pending_terminal = Some(PendingAttemptTerminal {
                terminal: NativeAttemptTerminal::Failed(
                    NativeAttemptPreparationFailure::from_input(ExecutionFailureInput::new(
                        QueryFailureReason::BackendLost.failure_fact(),
                        SafeDetail::truncating("the frozen pre-ready backend process was replaced"),
                        ExecutionFailureSite::Backend { process: backend },
                    )),
                ),
                allows_accepted_task_cause: false,
            });
            self.pending_pre_ready = None;
            return false;
        }
        self.round
            .execution()
            .pre_ready_observation_remaining(backend)
            .is_some_and(|remaining| !remaining.is_zero())
    }

    fn terminal_after_pre_ready_stop(
        &mut self,
        reason: CancellationReason,
    ) -> NativeAttemptTerminal {
        match self.pending_pre_ready.take() {
            Some(PendingPreReadyObservation::RejectedIdentity { .. }) => {
                // Cancellation ends independent observation, not the already
                // accepted definitive Worker failure. No witness was accepted.
                self.pending_terminal
                    .take()
                    .expect("a pending rejection retains its original terminal")
                    .terminal
            }
            _ => cancellation_failure(reason),
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
        // The actual QA running capability binds facts before any Task turn.
        // Refusing a foreign drive never overwrites this owner's memoized result.
        if let Err(error) = self.round.bind_protocol_drive(drive) {
            return task_protocol_failure(error);
        }
        if let Some(terminal) = &self.terminal {
            return terminal.clone();
        }
        let mut process_changes: Option<tokio::sync::watch::Receiver<u64>> = None;
        let mut process_changes_open = true;
        loop {
            let notify = Arc::clone(&self.notify);
            let notified = notify.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();

            let was_pending_pre_ready = self.pending_pre_ready.is_some();
            let advanced = if was_pending_pre_ready {
                self.receive_pending(drive).await
            } else {
                self.advance(drive).await
            };
            let moved = match advanced {
                Ok(moved) => moved,
                Err(error) => {
                    tracing::warn!(
                        ?error,
                        "Active Task protocol failed before attempt terminal"
                    );
                    self.record_protocol_failure(error);
                    false
                }
            };
            // Read the original source's failure on ordinary and pending
            // pre-ready turns. A normally exhausted source is not a failure.
            if let Some(error) = self
                .split_failure
                .as_ref()
                .and_then(SplitAssignmentFailureSource::failure)
            {
                self.record_protocol_failure(TaskExecutionError::SplitAssignment(error));
            }
            // Root output is its own accepted cut. A strict writer/source
            // tail can still owe the independent internal consumer a terminal
            // fact, so the original run must keep folding its Task statuses.
            // Normal drain starts only after those frozen obligations finish;
            // a DemandDriven read has no such tail and keeps its early return.
            let success_ready = completion == ManifestAttemptCompletion::AcceptedRootSuccessSeal
                && self.round.accepted_root_success_sealed()
                && self.round.execution().normal_drain_started();
            if success_ready {
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
            if !success_ready
                && (self.pending_terminal.is_none()
                    || self.pending_pre_ready.is_some_and(PendingPreReadyObservation::is_unknown)
                    || self.pending_terminal.as_ref().is_some_and(|pending| {
                        pending.allows_accepted_task_cause
                    }))
                && let Some(input) = self.round.execution().selected_task_failure().and_then(novarocks_query_application::execution_error::ExecutionFailureInput::from_selected_task)
            {
                // Peer placeholders still wait for their original author.
                // Task/control inputs retain their real identity and phase.
                let terminal=NativeAttemptTerminal::Failed(NativeAttemptPreparationFailure::from_input(input));
                let pending_unknown = self.pending_pre_ready.is_some_and(PendingPreReadyObservation::is_unknown);
                let ordinary_observation_terminal = self.pending_terminal.as_ref()
                    .is_some_and(|pending| pending.allows_accepted_task_cause);
                // The ordinary observation already permits a logical terminal
                // without waiting for the gate. Select its accepted Task cause
                // at that same cut; retained transport remains convergence work.
                if self.actor_gate.prepare_convergence() || pending_unknown
                    || ordinary_observation_terminal {
                    self.terminal=Some(terminal.clone());
                    return terminal;
                }
            }
            if !success_ready
                && matches!(
                    self.pending_pre_ready,
                    Some(PendingPreReadyObservation::RejectedIdentity { .. })
                )
            {
                // QA owns whether this exact attempt may still observe a
                // replacement. A refusal is already a terminal; a closed
                // recovery window must not delay it or acquire an observer.
                let allowed = if let Some(PendingAttemptTerminal {
                    terminal: NativeAttemptTerminal::Failed(failure),
                    ..
                }) = self.pending_terminal.as_ref()
                {
                    drive
                        .may_observe_pre_ready_replacement(failure.input())
                        .await
                        .unwrap_or(false)
                } else {
                    false
                };
                if !allowed {
                    self.pending_pre_ready = None;
                }
            }
            if !success_ready
                && self.pending_pre_ready.is_some()
                && let Some(reason) = cancellation.reason()
            {
                // Publish control without waiting for residual transports
                // whose abort/close owner is the enclosing supervisor.
                let _ = self.actor_gate.prepare_convergence();
                let terminal = self.terminal_after_pre_ready_stop(reason);
                self.terminal = Some(terminal.clone());
                return terminal;
            }
            if !success_ready && self.pending_pre_ready.is_some() && process_changes.is_none() {
                // Subscribe for this exact pending Establish before its first
                // process read. Ordinary attempts never acquire this observer.
                process_changes = Some(self.convergence.subscribe_process_changes());
            }
            let waiting_for_process = !success_ready && self.pending_pre_ready_must_wait();
            if !success_ready && self.pending_terminal.is_some() && !waiting_for_process {
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
                    .expect("pending Task terminal was checked")
                    .terminal;
                self.terminal = Some(terminal.clone());
                return terminal;
            }
            if !success_ready
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
                reason = cancellation.cancelled(), if !success_ready => {
                    if self.actor_gate.prepare_convergence() || waiting_for_process {
                        let terminal = self.terminal_after_pre_ready_stop(reason);
                        self.terminal = Some(terminal.clone());
                        return terminal;
                    }
                }
                changed = async {
                    process_changes.as_mut()
                        .expect("a pending process read subscribed before observation")
                        .changed().await
                }, if waiting_for_process && process_changes_open => {
                    process_changes_open = changed.is_ok();
                }
                _ = tokio::time::sleep(IDLE_RECHECK) => {}
            }
        }
    }

    /// Keeps the original local pumps until their real close/join exits. The
    /// original remote deadline only ends remote tracking; it never supplies a
    /// Worker stop fact or substitutes for a local pump exit.
    pub(crate) async fn converge(
        &mut self,
        request: NativeActiveConvergenceRequest,
    ) -> Result<NativeActiveConvergenceClosure, StatementNativeHealthFailure> {
        if let Some(ready) = request.computation_ready() {
            self.round
                .verify_computation_ready(ready)
                .map_err(|error| {
                    StatementNativeHealthFailure::actual_exit_unproven(
                        "Native convergence rejected a foreign computation cut",
                        Arc::new(error),
                    )
                })?;
            if let Some(diagnostics) = self.profile_diagnostics.as_mut() {
                diagnostics
                    .begin_profile_observation(ready)
                    .map_err(|error| {
                        StatementNativeHealthFailure::actual_exit_unproven(
                            "Native convergence could not bind its original diagnostic observation",
                            Arc::new(error),
                        )
                    })?;
                if let Some(deadline) = diagnostics.observation_deadline() {
                    self.round
                        .execution_mut()
                        .retain_profile_diagnostics_until(deadline);
                }
            }
        } else if let Some(diagnostics) = self.profile_diagnostics.as_mut() {
            // Initialization or execution failed before computation readiness.
            // There is no diagnostic window to invent or extend.
            diagnostics.stop_starting();
        }
        let mode = if matches!(self.terminal, Some(NativeAttemptTerminal::Failed(_))) {
            NativeAttemptConvergenceMode::Cleanup
        } else if request.computation_ready().is_some()
            || matches!(self.terminal, Some(NativeAttemptTerminal::Completed))
        {
            NativeAttemptConvergenceMode::Normal
        } else {
            NativeAttemptConvergenceMode::Cleanup
        };
        self.convergence
            .drive(
                &mut self.round,
                &self.split_delivery,
                mode,
                request,
                &self.notify,
                self.profile_diagnostics.as_mut(),
            )
            .await
    }

    /// Only the stable pre-run owner may use this local close author. It
    /// closes normal dispatch before touching the same original round sources.
    pub(crate) async fn converge_unstarted(&mut self) -> Result<(), StatementNativeHealthFailure> {
        if !self.actor_gate.prepare_convergence() {
            return Err(self.convergence.retain_unstarted_gate_failure());
        }
        self.convergence
            .drive_unstarted(
                &mut self.round,
                &self.split_delivery,
                &self.notify,
                self.profile_diagnostics.as_mut(),
            )
            .await
    }

    #[cfg(test)]
    fn context_convergence(
        &self,
    ) -> Option<Box<[novarocks_query_application::api::NativeContextConvergence]>> {
        self.convergence.observed_contexts(&self.round)
    }
}

// The original raw TaskRound fixture still starts its normal drain here.
// Production convergence is owned by NativeAttemptConvergenceOwner.
#[cfg(test)]
pub(super) fn begin_convergence(
    round: &mut TaskRound,
    split_delivery: &SplitDeliveryBridge,
    terminal: Option<&NativeAttemptTerminal>,
) {
    let mode = if matches!(terminal, Some(NativeAttemptTerminal::Completed)) {
        NativeAttemptConvergenceMode::Normal
    } else {
        NativeAttemptConvergenceMode::Cleanup
    };
    super::attempt_convergence::begin_convergence(round, split_delivery, mode);
}

pub(crate) fn task_protocol_failure(error: TaskExecutionError) -> NativeAttemptTerminal {
    NativeAttemptTerminal::Failed(NativeAttemptPreparationFailure::from_input(
        error.preparation_input(),
    ))
}

fn cancellation_failure(reason: CancellationReason) -> NativeAttemptTerminal {
    use novarocks_execution_contract::{SafeDetail, failure::FailureProjection};
    use novarocks_query_application::execution_error::{
        ExecutionFailureInput, ExecutionFailureSite, QueryFailureReason,
    };
    let fact = match reason {
        CancellationReason::DeadlineExceeded
        | CancellationReason::FrontendDrainDeadlineExceeded => QueryFailureReason::DeadlineExceeded,
        _ => QueryFailureReason::Cancelled,
    }
    .failure_fact();
    NativeAttemptTerminal::Failed(NativeAttemptPreparationFailure::from_input(
        ExecutionFailureInput::new(
            fact,
            SafeDetail::truncating(&format!("Native Task attempt cancelled: {reason:?}")),
            ExecutionFailureSite::Coordinator,
        ),
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use novarocks_coordination_contract::AttemptFailureClass;
    use novarocks_query_application::api::{BackendProcessObservation, QueryExecutionErrorKind};

    fn protocol_context(
        backend: novarocks_types::BackendProcessId,
    ) -> novarocks_execution_contract::QueryContextRef {
        use novarocks_types::{AttemptId, FrontendProcessId, QueryExecutionId, QueryId};
        novarocks_execution_contract::QueryContextRef::new(
            QueryExecutionId::new(QueryId::new(31, 32), AttemptId::new(1).unwrap()).unwrap(),
            FrontendProcessId::new_v7(),
            backend,
        )
    }

    #[test]
    fn preparation_task_capability_uses_the_same_client_identity_as_result_observation() {
        use novarocks_failure_contract::FailureProjection;
        let backend = novarocks_types::BackendProcessId::new_v7();
        let query = novarocks_types::QueryExecutionId::new(
            novarocks_types::QueryId::new(31, 32),
            novarocks_types::AttemptId::new(1).unwrap(),
        )
        .unwrap();
        let identity = novarocks_execution_contract::TaskIdentity::new(
            query,
            novarocks_types::StageId::new(1).unwrap(),
            novarocks_types::TaskId::new(1).unwrap(),
            backend,
        );
        for original in [
            novarocks_plan_codec::PhysicalEncodeError::UnsupportedCapability(
                "FULL OUTER hash join".into(),
            ),
            novarocks_plan_codec::PhysicalEncodeError::Invalid("FULL OUTER hash join".into()),
        ] {
            let fact = original.failure_fact();
            let selected = novarocks_coordination_contract::SelectedTaskTermination::new(
                identity,
                novarocks_execution_contract::TerminationDetail::Failed(
                    novarocks_execution_contract::TaskFailure::new_in_phase(
                        fact,
                        novarocks_execution_contract::SafeDetail::truncating(
                            "FULL OUTER hash join",
                        ),
                        novarocks_execution_contract::TaskFailurePhase::Preparation,
                    ),
                ),
            );
            let input = novarocks_query_application::execution_error::ExecutionFailureInput::from_selected_task(&selected).unwrap();
            assert_eq!(input.fact(), fact);
            assert_eq!(
                input.site(),
                novarocks_query_application::execution_error::ExecutionFailureSite::Task {
                    identity,
                    phase: novarocks_execution_contract::TaskFailurePhase::Preparation
                }
            );
            let failure = NativeAttemptPreparationFailure::from_input(input.clone());
            assert_eq!(failure.input(), &input);
            let observed =
                novarocks_query_application::coordination::RootResultFetchFailure::from_input(
                    input.clone(),
                );
            assert_eq!(failure.class(), observed.class());
            assert_eq!(failure.error().execution_failure(), Some(&input));
            assert_eq!(observed.input().descriptor(), input.descriptor());
            assert!(failure.error().message().contains("FULL OUTER hash join"));
        }
    }

    struct UnusedConvergenceSource;

    impl novarocks_query_application::api::BackendProcessObservationPort for UnusedConvergenceSource {
        fn subscribe_process_changes(&self) -> tokio::sync::watch::Receiver<u64> {
            panic!(
                "ordinary, sealed, Task-failed or cancelled attempts must not subscribe to process observation"
            );
        }

        fn observe_process_at_endpoint(
            &self,
            _: novarocks_types::BackendProcessId,
            _: &novarocks_execution::runtime::endpoint::RuntimeEndpoint,
        ) -> Result<BackendProcessObservation, novarocks_query_application::api::BackendTopologyError>
        {
            panic!("attempt terminal arbitration must not inspect residual convergence");
        }
    }

    fn unstarted_close_round(round: TaskRound) -> ManifestAssembledRound {
        let targets = round
            .execution()
            .graph()
            .contexts()
            .copied()
            .map(|context| {
                NativeAttemptConvergenceTarget::new(
                    context,
                    novarocks_execution::runtime::endpoint::RuntimeEndpoint::new("127.0.0.1", 1)
                        .unwrap(),
                )
            })
            .collect();
        let split_delivery = SplitDeliveryBridge::for_graph(round.execution().graph());
        let (_, actor_gate, _) = ActorGatedTaskOperationSink::pair(
            Arc::new(BackpressuredGateTransport),
            Arc::new(super::super::status_intake::CountingWake::default()),
            novarocks_types::NativeCompatibilityId::new([0x41; 32]),
        );
        let convergence =
            NativeAttemptConvergenceOwner::new(&round, Arc::new(UnusedConvergenceSource), targets)
                .unwrap();
        ManifestAssembledRound {
            round,
            split_delivery,
            actor_gate,
            protocol_facts: None,
            convergence,
            notify: Arc::new(tokio::sync::Notify::new()),
            terminal: None,
            pending_terminal: None,
            pending_pre_ready: None,
            split_failure: None,
            profile_diagnostics: None,
        }
    }

    fn unstarted_close_cancellation() -> (
        novarocks_workload_control::WorkloadControl,
        novarocks_workload_control::RootWork,
        CancellationView,
    ) {
        use novarocks_workload_control::{
            ResourceConfig, WorkClass, WorkRequest, WorkloadConfig, WorkloadControl,
        };
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
        let root = control
            .try_begin_root(WorkRequest::new(WorkClass::Query))
            .unwrap();
        let cancellation = root.owner.scope().cancellation().unwrap();
        (control, root, cancellation)
    }

    #[tokio::test]
    async fn unstarted_declared_empty_pumps_require_exact_never_established_facts() {
        let mut round = super::super::tests::manifest_unstarted_regression_fixture();
        round.seal_pumps();
        let mut assembled = unstarted_close_round(round);
        let (_control, root, _cancellation) = unstarted_close_cancellation();
        assembled.converge_unstarted().await.unwrap();
        let facts = assembled
            .convergence
            .observed_contexts(&assembled.round)
            .unwrap();
        let expected = assembled
            .round
            .execution()
            .graph()
            .contexts()
            .copied()
            .collect::<Vec<_>>();
        assert!(!expected.is_empty());
        assert_eq!(facts.len(), expected.len());
        for (fact, context) in facts.iter().zip(expected) {
            assert_eq!(fact.context(), context);
            assert_eq!(
                fact.kind(),
                novarocks_query_application::api::NativeContextConvergenceKind::NeverEstablished
            );
        }
        drop(assembled);
        root.owner.complete();
        root.business.release();
    }

    #[tokio::test]
    async fn unstarted_undeclared_empty_pumps_are_not_actual_close_proof() {
        let round = super::super::tests::manifest_unstarted_regression_fixture();
        assert!(round.pumps_closed());
        assert!(!round.pumps_declared());
        let mut assembled = unstarted_close_round(round);
        let (_control, root, _cancellation) = unstarted_close_cancellation();
        let failure = assembled.converge_unstarted().await.unwrap_err();
        assert_eq!(
            failure.detail(),
            "Native dormant cleanup requires its original source declaration"
        );
        assert!(!assembled.round.pumps_declared());
        // Explicit fixture disposition, not a successful source-exit receipt.
        drop(failure);
        drop(assembled);
        root.owner.complete();
        root.business.release();
    }

    #[tokio::test]
    async fn unstarted_round_waits_for_the_original_source_close_job_after_borrowed_cancel() {
        use crate::query_execution::split_assignment::SplitAssignmentDriverError;
        use crate::query_execution::split_assignment_round::failure_observation_plan;
        use std::sync::atomic::Ordering;
        let round = super::super::tests::manifest_unstarted_regression_fixture();
        let context = *round.execution().graph().contexts().next().unwrap();
        assert!(round.execution().context_never_established(context));
        let mut assembled = unstarted_close_round(round);
        let data_runtime = FrontendDataRuntime::new(tokio::runtime::Handle::current());
        let mut executors =
            crate::task_execution::blocking_io::tests::OriginalExecutorsFixture::install(
                data_runtime.connector_blocking_io(),
            );
        let (release, released) = std::sync::mpsc::channel();
        let (plan, enumerations, closes) = failure_observation_plan(
            context.query_execution_id(),
            data_runtime.connector_blocking_io().clone(),
            Some(released),
        );
        let guard = assembled
            .install_split_assignment(context.query_execution_id(), plan, data_runtime)
            .unwrap();
        assembled.round.seal_pumps();
        let (_control, root, _cancellation) = unstarted_close_cancellation();
        let mut close = Box::pin(assembled.converge_unstarted());
        assert!(futures::poll!(&mut close).is_pending());
        drop(close);
        // No borrowed Future owns or aborts the real original close job.
        assert!(!guard.is_finished());
        let observe_then_release = async {
            while closes.load(Ordering::SeqCst) == 0 {
                tokio::task::yield_now().await;
            }
            assert!(
                !guard.is_finished(),
                "the actual source is blocked inside its own close"
            );
            assert_eq!(enumerations.load(Ordering::SeqCst), 0);
            release.send(()).unwrap();
        };
        let (result, ()) = tokio::time::timeout(std::time::Duration::from_secs(2), async {
            tokio::join!(assembled.converge_unstarted(), observe_then_release)
        })
        .await
        .unwrap();
        result.unwrap();
        assert!(guard.is_finished());
        assert!(assembled.round.pumps_closed());
        assert!(
            assembled
                .round
                .execution()
                .context_never_established(context)
        );
        let facts = assembled
            .convergence
            .observed_contexts(&assembled.round)
            .unwrap();
        assert_eq!(facts.len(), 1);
        assert_eq!(facts[0].context(), context);
        assert_eq!(
            facts[0].kind(),
            novarocks_query_application::api::NativeContextConvergenceKind::NeverEstablished
        );
        assert_eq!(enumerations.load(Ordering::SeqCst), 0);
        assert_eq!(closes.load(Ordering::SeqCst), 1);
        let profile = guard.profile_source().current();
        let cleanup = guard.published_close_failure_for_test();
        drop(guard);
        assert!(matches!(
            profile,
            crate::query_execution::split_assignment_round::SplitAssignmentProfileRead::Failed
        ));
        assert!(matches!(
            cleanup,
            Some(SplitAssignmentDriverError::SourceClose { .. })
        ));
        drop(assembled);
        root.owner.complete();
        root.business.release();
        executors.shutdown().await;
    }

    #[tokio::test]
    async fn unstarted_round_refuses_a_real_original_admission_ticket() {
        let (round, _) = super::super::tests::pre_ready_observation_round();
        let context = *round.execution().graph().contexts().next().unwrap();
        assert!(!round.execution().context_never_established(context));
        let mut assembled = unstarted_close_round(round);
        let (_control, root, _cancellation) = unstarted_close_cancellation();
        let failure = assembled.converge_unstarted().await.unwrap_err();
        assert_eq!(
            failure.detail(),
            "Native dormant cleanup requires every exact context to be never established"
        );
        let same_failure = assembled.converge_unstarted().await.unwrap_err();
        assert!(std::ptr::eq(failure.cause(), same_failure.cause()));
        drop(same_failure);
        assert!(
            !assembled
                .round
                .execution()
                .context_never_established(context)
        );
        // No Worker/remote closure is minted and the original round stays here.
        drop(failure);
        drop(assembled);
        root.owner.complete();
        root.business.release();
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
        terminal_regression_drive_with_recovery(context, false).await
    }

    async fn terminal_regression_drive_with_recovery(
        context: novarocks_execution_contract::QueryContextRef,
        recovery: bool,
    ) -> (
        novarocks_query_application::test_support::LogicalExecutionTestHarness,
        NativeAttemptDrive,
        CancellationView,
        novarocks_workload_control::WorkCancellationRequester,
        super::super::abort_effect::NativeAbortEffectIntake,
    ) {
        terminal_regression_drive_for_contexts(vec![context], recovery).await
    }

    async fn terminal_regression_drive_for_contexts(
        contexts: Vec<novarocks_execution_contract::QueryContextRef>,
        recovery: bool,
    ) -> (
        novarocks_query_application::test_support::LogicalExecutionTestHarness,
        NativeAttemptDrive,
        CancellationView,
        novarocks_workload_control::WorkCancellationRequester,
        super::super::abort_effect::NativeAbortEffectIntake,
    ) {
        let context = *contexts.first().expect("the original graph has contexts");
        use novarocks_coordination_contract::ExecutionEffect;
        use novarocks_query_application::coordination::{
            AbortQueryContextEffectPort, LogicalExecutionActorConfig,
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
        let config = if recovery {
            // Install the real read output contract as well as recovery policy.
            // The harness owns its output and retires it through finish_until.
            LogicalExecutionActorConfig::read_only_pre_visibility_recovery_rows(
                context.query_execution_id(),
                NonZeroUsize::new(2).unwrap(),
                contexts.clone(),
                NonZeroUsize::new(2).unwrap(),
                NonZeroUsize::new(2).unwrap(),
                std::num::NonZeroU32::new(2).unwrap(),
                Arc::new(ObservationOnlyQualification {
                    capacity: tokio::sync::watch::channel(0).0,
                }),
                governed.owner,
                execution_stage,
                std::time::Duration::from_secs(30),
                novarocks_query_application::api::ResultSchema::new(Vec::new()),
                NonZeroUsize::new(1).unwrap(),
                novarocks_query_application::api::ResultRowCarrier::relayed(
                    novarocks_result_contract::RootOutputKind::ClientRows,
                    Some(
                        novarocks_result_contract::ClientRowProfile::try_new(
                            novarocks_result_contract::RootProfileV1::SEGMENT_BYTES,
                            novarocks_result_contract::RootProfileV1::ROW_PAYLOAD_BYTES,
                        )
                        .unwrap(),
                    ),
                )
                .unwrap(),
            )
        } else {
            LogicalExecutionActorConfig::single_attempt_completion(
                context.query_execution_id(),
                ExecutionEffect::None,
                NonZeroUsize::new(2).unwrap(),
                contexts.clone(),
                NonZeroUsize::new(2).unwrap(),
                NonZeroUsize::new(2).unwrap(),
                governed.owner,
                execution_stage,
            )
        }
        .unwrap()
        .with_abort_query_context_effect_port(
            adapter as Arc<dyn AbortQueryContextEffectPort>,
            NonZeroUsize::new(2).unwrap(),
        );
        let mut logical = LogicalExecutionTestHarness::install(
            tokio::runtime::Handle::current(),
            config,
            context.query_execution_id(),
            contexts.clone(),
        )
        .unwrap();
        logical.activate_initial().await.unwrap();
        let drive = logical.native_attempt_drive();
        (logical, drive, cancellation, requester, intake)
    }

    #[derive(Debug)]
    struct ObservationOnlyQualification {
        capacity: tokio::sync::watch::Sender<u64>,
    }

    impl novarocks_query_application::coordination::ReplacementQualificationEffectPort
        for ObservationOnlyQualification
    {
        fn subscribe_capacity(&self) -> tokio::sync::watch::Receiver<u64> {
            self.capacity.subscribe()
        }

        fn try_reserve(
            &self,
            _: &novarocks_query_application::coordination::ReplacementQualificationRequest,
        ) -> novarocks_query_application::coordination::ReplacementQualificationEffectAdmission
        {
            panic!("observing a replacement must not qualify or start a new attempt")
        }
    }

    #[derive(Debug)]
    struct BackpressuredGateTransport;

    impl TaskOperationSink for BackpressuredGateTransport {
        fn try_reserve_queue(
            &self,
            _: super::super::intent::TaskOperationQueueRequest,
            _: &mut super::super::intent::TaskOperationQueuePreparation,
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

    #[test]
    fn abort_closure_is_not_worker_exit_and_only_the_original_receipt_closes_resources() {
        use super::super::status_intake::{ObservationFrame, ObservationIntakeEntry};
        use novarocks_execution_contract::{
            QueryContextConvergenceReceipt, QueryContextConvergenceState,
            QueryContextConvergenceVersion,
        };
        use novarocks_task_codec::operation::{CoveredStatusStreamEvent, CoveredStatusStreamFact};
        struct CurrentProcess;
        impl novarocks_query_application::api::BackendProcessObservationPort for CurrentProcess {
            fn observe_process_at_endpoint(
                &self,
                _: novarocks_types::BackendProcessId,
                _: &novarocks_execution::runtime::endpoint::RuntimeEndpoint,
            ) -> Result<
                BackendProcessObservation,
                novarocks_query_application::api::BackendTopologyError,
            > {
                Ok(BackendProcessObservation::Current)
            }
        }
        let (round, _) = super::super::tests::manifest_terminal_regression_fixture();
        let context = *round.execution().graph().contexts().next().unwrap();
        let split_delivery = SplitDeliveryBridge::for_graph(round.execution().graph());
        let (_, actor_gate, _) = ActorGatedTaskOperationSink::pair(
            Arc::new(BackpressuredGateTransport),
            Arc::new(super::super::status_intake::CountingWake::default()),
            novarocks_types::NativeCompatibilityId::new([0x41; 32]),
        );
        let convergence = NativeAttemptConvergenceOwner::new(
            &round,
            Arc::new(CurrentProcess),
            vec![NativeAttemptConvergenceTarget::new(
                context,
                novarocks_execution::runtime::endpoint::RuntimeEndpoint::new("127.0.0.1", 1)
                    .unwrap(),
            )],
        )
        .unwrap();
        let mut assembled = ManifestAssembledRound {
            round,
            split_delivery,
            actor_gate,
            protocol_facts: None,
            convergence,
            notify: Arc::new(tokio::sync::Notify::new()),
            terminal: None,
            pending_terminal: None,
            pending_pre_ready: None,
            split_failure: None,
            profile_diagnostics: None,
        };
        assembled.convergence.begin(
            &mut assembled.round,
            &assembled.split_delivery,
            NativeAttemptConvergenceMode::Cleanup,
        );
        assembled
            .round
            .execution_mut()
            .observe_actor_abort_context_closed(context)
            .unwrap();
        assert!(
            assembled
                .round
                .execution()
                .owner(context)
                .unwrap()
                .is_released()
        );
        assert!(assembled.context_convergence().is_none());
        assert!(!assembled.round.execution().worker_contexts_stopped());
        let receipt = QueryContextConvergenceReceipt::new(
            context,
            QueryContextConvergenceVersion::FIRST,
            QueryContextConvergenceState::WorkerStoppedAndContextFenced,
        );
        assembled
            .round
            .execution_mut()
            .apply_covered_entries(vec![ObservationIntakeEntry::Frame(Box::new(
                ObservationFrame::Covered {
                    context,
                    generation: 1,
                    event: CoveredStatusStreamEvent {
                        fact: CoveredStatusStreamFact::ContextConvergence(receipt),
                        source_revision: None,
                    },
                },
            ))])
            .unwrap();
        assert_eq!(
            assembled
                .round
                .execution()
                .context_convergence_receipt(context),
            Some(receipt)
        );
        assert!(assembled.round.execution().worker_contexts_stopped());
        assert_eq!(assembled.context_convergence().unwrap().len(), 1);
    }

    #[derive(Clone)]
    struct ReleaseJoinObservation(BackendProcessObservation);

    impl novarocks_query_application::api::BackendProcessObservationPort for ReleaseJoinObservation {
        fn observe_process_at_endpoint(
            &self,
            _: novarocks_types::BackendProcessId,
            _: &novarocks_execution::runtime::endpoint::RuntimeEndpoint,
        ) -> Result<BackendProcessObservation, novarocks_query_application::api::BackendTopologyError>
        {
            Ok(self.0)
        }
    }

    fn release_join_round(
        round: TaskRound,
        observation: BackendProcessObservation,
    ) -> ManifestAssembledRound {
        let context = *round.execution().graph().contexts().next().unwrap();
        let split_delivery = SplitDeliveryBridge::for_graph(round.execution().graph());
        let (_, actor_gate, _) = ActorGatedTaskOperationSink::pair(
            Arc::new(BackpressuredGateTransport),
            Arc::new(super::super::status_intake::CountingWake::default()),
            novarocks_types::NativeCompatibilityId::new([0x41; 32]),
        );
        let convergence = NativeAttemptConvergenceOwner::new(
            &round,
            Arc::new(ReleaseJoinObservation(observation)),
            vec![NativeAttemptConvergenceTarget::new(
                context,
                novarocks_execution::runtime::endpoint::RuntimeEndpoint::new("127.0.0.1", 1)
                    .unwrap(),
            )],
        )
        .unwrap();
        let mut assembled = ManifestAssembledRound {
            round,
            split_delivery,
            actor_gate,
            protocol_facts: None,
            convergence,
            notify: Arc::new(tokio::sync::Notify::new()),
            terminal: Some(NativeAttemptTerminal::Completed),
            pending_terminal: None,
            pending_pre_ready: None,
            split_failure: None,
            profile_diagnostics: None,
        };
        assembled.convergence.begin(
            &mut assembled.round,
            &assembled.split_delivery,
            NativeAttemptConvergenceMode::Normal,
        );
        assembled
    }

    fn publish_release_join_exit(assembled: &mut ManifestAssembledRound) {
        use super::super::status_intake::{ObservationFrame, ObservationIntakeEntry};
        use novarocks_execution_contract::{
            QueryContextConvergenceReceipt, QueryContextConvergenceState,
            QueryContextConvergenceVersion,
        };
        use novarocks_task_codec::operation::{CoveredStatusStreamEvent, CoveredStatusStreamFact};
        let context = *assembled
            .round
            .execution()
            .graph()
            .contexts()
            .next()
            .unwrap();
        let receipt = QueryContextConvergenceReceipt::new(
            context,
            QueryContextConvergenceVersion::FIRST,
            QueryContextConvergenceState::WorkerStoppedAndContextFenced,
        );
        assembled
            .round
            .execution_mut()
            .apply_covered_entries(vec![ObservationIntakeEntry::Frame(Box::new(
                ObservationFrame::Covered {
                    context,
                    generation: 1,
                    event: CoveredStatusStreamEvent {
                        fact: CoveredStatusStreamFact::ContextConvergence(receipt),
                        source_revision: None,
                    },
                },
            ))])
            .unwrap();
    }

    #[test]
    fn completed_convergence_joins_covered_exit_and_original_release_in_both_orders() {
        use super::super::intent::{AckPayload, OperationAcknowledgement, OperationIntent};
        use novarocks_execution_contract::{
            OperationKind, OperationOutcome, QueryContextReceipt, QueryContextState, ReleaseOutcome,
        };
        let expected = super::super::tests::fixture_runtime_filter_contribution();
        let mut outcomes = Vec::new();
        for covered_first in [true, false] {
            let (round, acks, release) =
                super::super::tests::manifest_normal_release_ack_regression_fixture();
            let mut assembled = release_join_round(round, BackendProcessObservation::Current);
            let context = *assembled
                .round
                .execution()
                .graph()
                .contexts()
                .next()
                .unwrap();
            let OperationIntent::ReleaseQueryContext(request) = &release else {
                unreachable!()
            };
            let ack = OperationAcknowledgement::new(
                release.operation_id(),
                OperationKind::ReleaseQueryContext,
                OperationOutcome::Accepted,
                AckPayload::Release {
                    receipt: QueryContextReceipt::new(
                        request.context(),
                        QueryContextState::TerminalRetained,
                    ),
                    outcome: ReleaseOutcome::Released,
                    runtime_filter: Some(expected.clone()),
                },
            );
            assert!(assembled.context_convergence().is_none());
            if covered_first {
                publish_release_join_exit(&mut assembled);
                assert!(assembled.round.execution().worker_contexts_stopped());
                assert!(
                    !assembled
                        .round
                        .execution()
                        .owner(context)
                        .unwrap()
                        .is_released()
                );
                assert!(
                    assembled.context_convergence().is_none(),
                    "physical exit must not discard the pending Release acknowledgement"
                );
                acks.publish(ack);
                assembled.round.turn().unwrap();
            } else {
                acks.publish(ack);
                assembled.round.turn().unwrap();
                assert!(
                    assembled
                        .round
                        .execution()
                        .owner(context)
                        .unwrap()
                        .is_released()
                );
                assert!(!assembled.round.execution().worker_contexts_stopped());
                assert!(
                    assembled.context_convergence().is_none(),
                    "Release acknowledgement is not physical exit evidence"
                );
                publish_release_join_exit(&mut assembled);
            }
            assert_eq!(assembled.context_convergence().unwrap().len(), 1);
            let contributions = assembled
                .round
                .execution()
                .released_runtime_filter_contributions();
            assert!(contributions.is_complete());
            assert_eq!(contributions.contexts(), 1);
            assert_eq!(
                contributions.contributions(),
                &[(context.backend_process_id(), expected.clone())]
            );
            outcomes.push(
                contributions
                    .contributions()
                    .iter()
                    .map(|(_, value)| value.clone())
                    .collect::<Vec<_>>(),
            );
        }
        assert_eq!(
            outcomes[0], outcomes[1],
            "arrival order cannot change the sealed contribution"
        );
    }

    #[test]
    fn cleanup_convergence_keeps_physical_exit_without_requiring_normal_release() {
        let (round, _acks, _release) =
            super::super::tests::manifest_normal_release_ack_regression_fixture();
        let mut assembled = release_join_round(round, BackendProcessObservation::Current);
        let context = *assembled
            .round
            .execution()
            .graph()
            .contexts()
            .next()
            .unwrap();
        assembled.convergence.begin(
            &mut assembled.round,
            &assembled.split_delivery,
            NativeAttemptConvergenceMode::Cleanup,
        );
        publish_release_join_exit(&mut assembled);
        assert!(
            !assembled
                .round
                .execution()
                .owner(context)
                .unwrap()
                .is_released()
        );
        assert_eq!(assembled.context_convergence().unwrap().len(), 1);
        let contributions = assembled
            .round
            .execution()
            .released_runtime_filter_contributions();
        assert!(!contributions.is_complete());
        assert!(contributions.contributions().is_empty());
    }

    #[test]
    fn exact_process_replacement_does_not_wait_for_release_or_invent_a_contribution() {
        use novarocks_query_application::api::NativeContextConvergenceKind;
        for covered_first in [false, true] {
            let (round, _acks, _release) =
                super::super::tests::manifest_normal_release_ack_regression_fixture();
            let mut assembled = release_join_round(
                round,
                BackendProcessObservation::Replaced {
                    current_process: novarocks_types::BackendProcessId::new_v7(),
                },
            );
            if covered_first {
                publish_release_join_exit(&mut assembled);
            }
            let convergence = assembled.context_convergence().unwrap();
            assert_eq!(convergence.len(), 1);
            assert_eq!(
                convergence[0].kind(),
                NativeContextConvergenceKind::WorkerProcessReplaced
            );
            let contributions = assembled
                .round
                .execution()
                .released_runtime_filter_contributions();
            assert!(!contributions.is_complete());
            assert!(contributions.contributions().is_empty());
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
                use crate::task_protocol_config::DispatchLane;
                use novarocks_execution::task_execution::{
                    AcquireQueryContextAdmissionTicket, AdmissionEpochCapability, LeaseValidFor,
                };
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
            let convergence = NativeAttemptConvergenceOwner::new(
                &round,
                Arc::new(UnusedConvergenceSource),
                round
                    .execution()
                    .graph()
                    .contexts()
                    .map(|&context| {
                        NativeAttemptConvergenceTarget::new(
                            context,
                            novarocks_execution::runtime::endpoint::RuntimeEndpoint::new(
                                "127.0.0.1",
                                1,
                            )
                            .unwrap(),
                        )
                    })
                    .collect(),
            )
            .unwrap();
            let mut assembled = ManifestAssembledRound {
                round,
                split_delivery,
                actor_gate,
                protocol_facts: None,
                convergence,
                notify: Arc::new(tokio::sync::Notify::new()),
                terminal: None,
                pending_terminal: None,
                pending_pre_ready: None,
                split_failure: None,
                profile_diagnostics: None,
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
        assert_eq!(failure.error().kind(), QueryExecutionErrorKind::Failed);
    }

    #[test]
    fn pre_ready_establish_unknown_is_observation_loss_without_backend_loss_evidence() {
        let backend = novarocks_types::identity::BackendProcessId::new_v7();
        let terminal =
            task_protocol_failure(TaskExecutionError::PreReadyEstablishTransportUnknown {
                backend,
            });
        let NativeAttemptTerminal::Failed(failure) = terminal else {
            panic!("transport uncertainty must terminate the old attempt");
        };
        assert_eq!(failure.class(), AttemptFailureClass::ExecutionFailure);
        assert_eq!(failure.error().kind(), QueryExecutionErrorKind::Failed);
        assert_eq!(
            failure.topology_requirement(),
            novarocks_query_application::api::NativeAttemptTopologyRequirement::LiveSnapshot
        );
    }

    #[test]
    fn pre_ready_establish_rejection_retains_its_protocol_fact() {
        let terminal = task_protocol_failure(TaskExecutionError::PreReadyEstablishRejected {
            context: protocol_context(novarocks_types::identity::BackendProcessId::new_v7()),
            operation_id: novarocks_execution_contract::TaskOperationId::new_v7(),
            rejection: novarocks_execution_contract::operation::OperationResult::Verdict {
                outcome:
                    novarocks_execution::task_execution::OperationOutcome::InvalidStateOrRequest,
                detail: None,
                acknowledgement: None,
            },
        });
        let NativeAttemptTerminal::Failed(failure) = terminal else {
            panic!("pre-ready Establish rejection must terminate the old attempt");
        };
        assert_eq!(failure.class(), AttemptFailureClass::ContractViolation);
        assert_eq!(failure.error().kind(), QueryExecutionErrorKind::Failed);
    }
    #[test]
    fn pre_ready_identity_mismatch_is_an_operation_fact_without_process_loss() {
        let backend = novarocks_types::identity::BackendProcessId::new_v7();
        let NativeAttemptTerminal::Failed(failure) =
            task_protocol_failure(TaskExecutionError::PreReadyEstablishRejected {
                context: protocol_context(backend),
                operation_id: novarocks_execution_contract::TaskOperationId::new_v7(),
                rejection: novarocks_execution_contract::operation::OperationResult::Verdict {
                    outcome: novarocks_execution_contract::OperationOutcome::IdentityMismatch,
                    detail: None,
                    acknowledgement: None,
                },
            })
        else {
            panic!("identity mismatch must fail the attempt");
        };
        assert_eq!(
            failure.topology_requirement(),
            novarocks_query_application::api::NativeAttemptTopologyRequirement::LiveSnapshot
        );
    }
    #[test]
    fn lost_status_transport_does_not_manufacture_failed_process_evidence() {
        let backend = novarocks_types::identity::BackendProcessId::new_v7();
        let NativeAttemptTerminal::Failed(failure) =
            task_protocol_failure(TaskExecutionError::ParticipantUnobservable {
                backend,
                state: ParticipantObservationFailure::Transport("stream_lost"),
            })
        else {
            panic!("lost participant must fail the attempt");
        };
        assert_eq!(failure.class(), AttemptFailureClass::ExecutionFailure);
        assert_eq!(
            failure.topology_requirement(),
            novarocks_query_application::api::NativeAttemptTopologyRequirement::LiveSnapshot
        );
    }

    struct PendingProcessObservation {
        expected: novarocks_types::BackendProcessId,
        observation: std::sync::Mutex<
            Result<
                BackendProcessObservation,
                novarocks_query_application::api::BackendTopologyError,
            >,
        >,
        changes: tokio::sync::watch::Sender<u64>,
        calls: std::sync::atomic::AtomicUsize,
        subscriptions: std::sync::atomic::AtomicUsize,
    }
    impl PendingProcessObservation {
        fn new(
            expected: novarocks_types::BackendProcessId,
            observation: BackendProcessObservation,
        ) -> Self {
            let (changes, _) = tokio::sync::watch::channel(0);
            Self {
                expected,
                observation: std::sync::Mutex::new(Ok(observation)),
                changes,
                calls: std::sync::atomic::AtomicUsize::new(0),
                subscriptions: std::sync::atomic::AtomicUsize::new(0),
            }
        }
        fn replace(&self) {
            *self.observation.lock().unwrap() = Ok(BackendProcessObservation::Replaced {
                current_process: novarocks_types::BackendProcessId::new_v7(),
            });
            self.changes.send_modify(|epoch| *epoch += 1);
        }
    }
    impl novarocks_query_application::api::BackendProcessObservationPort for PendingProcessObservation {
        fn subscribe_process_changes(&self) -> tokio::sync::watch::Receiver<u64> {
            self.subscriptions
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            self.changes.subscribe()
        }
        fn observe_process_at_endpoint(
            &self,
            expected: novarocks_types::BackendProcessId,
            endpoint: &novarocks_execution::runtime::endpoint::RuntimeEndpoint,
        ) -> Result<BackendProcessObservation, novarocks_query_application::api::BackendTopologyError>
        {
            assert_eq!(expected, self.expected);
            assert_eq!(
                *endpoint,
                novarocks_execution::runtime::endpoint::RuntimeEndpoint::new("127.0.0.1", 1)
                    .unwrap()
            );
            self.calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            self.observation.lock().unwrap().clone()
        }
    }

    fn pending_pre_ready_round(
        round: TaskRound,
        source: Arc<dyn novarocks_query_application::api::BackendProcessObservationPort>,
    ) -> ManifestAssembledRound {
        let backend = round
            .execution()
            .graph()
            .contexts()
            .next()
            .unwrap()
            .backend_process_id();
        pending_pre_ready_round_with_failure(
            round,
            source,
            TaskExecutionError::PreReadyEstablishTransportUnknown { backend },
        )
    }

    fn pending_pre_ready_round_with_failure(
        round: TaskRound,
        source: Arc<dyn novarocks_query_application::api::BackendProcessObservationPort>,
        error: TaskExecutionError,
    ) -> ManifestAssembledRound {
        let context = *round.execution().graph().contexts().next().unwrap();
        let split_delivery = SplitDeliveryBridge::for_graph(round.execution().graph());
        let (_, actor_gate, _) = ActorGatedTaskOperationSink::pair(
            Arc::new(BackpressuredGateTransport),
            Arc::new(super::super::status_intake::CountingWake::default()),
            novarocks_types::NativeCompatibilityId::new([0x41; 32]),
        );
        let convergence = NativeAttemptConvergenceOwner::new(
            &round,
            source,
            vec![NativeAttemptConvergenceTarget::new(
                context,
                novarocks_execution::runtime::endpoint::RuntimeEndpoint::new("127.0.0.1", 1)
                    .unwrap(),
            )],
        )
        .unwrap();
        let mut assembled = ManifestAssembledRound {
            round,
            split_delivery,
            actor_gate,
            protocol_facts: None,
            convergence,
            notify: Arc::new(tokio::sync::Notify::new()),
            terminal: None,
            pending_terminal: None,
            pending_pre_ready: None,
            split_failure: None,
            profile_diagnostics: None,
        };
        assembled.record_protocol_failure(error);
        assembled
    }

    // This fixture sends the SAME released admission and Establish through
    // the real QA ledger and transport receipt before the Task owner observes
    // the rejection. The observation query must match that original issue.
    async fn original_rejected_establish_drive(
        recovery: bool,
    ) -> (
        TaskRound,
        TaskExecutionError,
        novarocks_query_application::test_support::LogicalExecutionTestHarness,
        NativeAttemptDrive,
        CancellationView,
        novarocks_workload_control::WorkCancellationRequester,
        super::super::abort_effect::NativeAbortEffectIntake,
    ) {
        use novarocks_execution_contract::{OperationKind, OperationOutcome};
        use novarocks_query_application::coordination::{
            AdmissionIssueSettlement, EstablishIssueIdentity, EstablishIssueSubmit,
            EstablishTransportAdmission, EstablishTransportReservation, EstablishTransportSink,
            EstablishTransportSubmission,
        };
        #[derive(Debug)]
        struct RejectedTransport;
        impl EstablishTransportSink for RejectedTransport {
            fn try_reserve(&self, _: EstablishIssueIdentity) -> EstablishTransportAdmission {
                EstablishTransportAdmission::Admitted(Box::new(Self))
            }
        }
        impl EstablishTransportReservation for RejectedTransport {
            fn submit(self: Box<Self>, submission: EstablishTransportSubmission) {
                submission
                    .worker_settled(OperationOutcome::IdentityMismatch)
                    .unwrap();
            }
        }
        let (mut round, clock, acks, admission, ticket, establish) =
            super::super::tests::pre_ready_original_establish_requests();
        let context = *round.execution().graph().contexts().next().unwrap();
        let (logical, drive, cancellation, requester, abort) =
            terminal_regression_drive_with_recovery(context, recovery).await;
        let issue = drive.begin_admission_issue(admission).await.unwrap();
        drive
            .settle_admission_issue(
                issue,
                AdmissionIssueSettlement::applied(
                    admission.envelope().operation_id(),
                    OperationOutcome::Accepted,
                    ticket,
                )
                .unwrap(),
            )
            .await
            .unwrap();
        let operation_id = establish.envelope().operation_id();
        let authorized = drive
            .authorize_establish(establish, admission.native_compatibility_id())
            .await
            .unwrap();
        assert_eq!(
            authorized.try_submit(&RejectedTransport).unwrap(),
            EstablishIssueSubmit::Accepted
        );
        clock.advance(std::time::Duration::from_secs(3));
        acks.publish(
            super::super::intent::OperationAcknowledgement::worker_receipt(
                operation_id,
                OperationKind::UpdateQueryContext,
                OperationOutcome::IdentityMismatch,
                super::super::intent::AckPayload::None,
            ),
        );
        let error = round.receive_without_dispatch().unwrap_err();
        assert!(
            matches!(&error, TaskExecutionError::PreReadyEstablishRejected {
            operation_id: observed, ..
        } if *observed == operation_id)
        );
        (round, error, logical, drive, cancellation, requester, abort)
    }

    async fn original_rejected_create_drive(
        recovery: bool,
        establish_unknown: bool,
    ) -> (
        TaskRound,
        Arc<super::super::clock::ManualClock>,
        TaskExecutionError,
        novarocks_query_application::test_support::LogicalExecutionTestHarness,
        NativeAttemptDrive,
        CancellationView,
        novarocks_workload_control::WorkCancellationRequester,
        super::super::abort_effect::NativeAbortEffectIntake,
    ) {
        use novarocks_query_application::coordination::{
            AdmissionIssueSettlement, EstablishIssueIdentity, EstablishIssueSubmit,
            EstablishTransportAdmission, EstablishTransportReservation, EstablishTransportSink,
            EstablishTransportSubmission,
        };
        #[derive(Debug)]
        struct OriginalTransport {
            establish_unknown: bool,
        }
        impl EstablishTransportSink for OriginalTransport {
            fn try_reserve(&self, _: EstablishIssueIdentity) -> EstablishTransportAdmission {
                EstablishTransportAdmission::Admitted(Box::new(Self {
                    establish_unknown: self.establish_unknown,
                }))
            }
        }
        impl EstablishTransportReservation for OriginalTransport {
            fn submit(self: Box<Self>, submission: EstablishTransportSubmission) {
                if self.establish_unknown {
                    let _late = submission.transport_unknown().unwrap();
                } else {
                    submission
                        .worker_settled(novarocks_execution_contract::OperationOutcome::Accepted)
                        .unwrap();
                }
            }
        }
        let (round, clock, error, trace) = if establish_unknown {
            super::super::tests::pre_ready_original_unknown_establish_create_refusal()
        } else {
            super::super::tests::pre_ready_original_create_refusal()
        };
        let context = *round.execution().graph().contexts().next().unwrap();
        let (logical, drive, cancellation, requester, abort) =
            terminal_regression_drive_with_recovery(context, recovery).await;
        let mut native_compatibility_id = None;
        for (intent, ack) in trace {
            match intent {
                super::super::intent::OperationIntent::AcquireQueryContextAdmissionTicket(
                    request,
                ) => {
                    let super::super::intent::AckPayload::AdmissionTicket(ticket) = ack.payload()
                    else {
                        panic!("original admission receipt")
                    };
                    native_compatibility_id = Some(request.native_compatibility_id());
                    let issue = drive.begin_admission_issue(request).await.unwrap();
                    drive
                        .settle_admission_issue(
                            issue,
                            AdmissionIssueSettlement::applied(
                                issue.operation_id(),
                                novarocks_execution_contract::OperationOutcome::Accepted,
                                *ticket,
                            )
                            .unwrap(),
                        )
                        .await
                        .unwrap();
                }
                super::super::intent::OperationIntent::EstablishQueryContext(request) => {
                    let context = request.context();
                    let operation_id = request.envelope().operation_id();
                    let parent = logical
                        .establish_snapshot(context)
                        .await
                        .unwrap()
                        .expect("the original admission owns this context");
                    // Unknown replay re-signs QA's retained original request;
                    // it cannot authorize a fresh payload or operation.
                    let issue = if parent.identity().is_some() {
                        drive.reauthorize_establish(context).await.unwrap()
                    } else {
                        drive
                            .authorize_establish(
                                request,
                                native_compatibility_id
                                    .expect("original admission precedes Establish"),
                            )
                            .await
                            .unwrap()
                    };
                    assert_eq!(issue.identity().context(), context);
                    assert_eq!(issue.identity().operation_id(), operation_id);
                    assert_eq!(
                        issue
                            .try_submit(&OriginalTransport { establish_unknown })
                            .unwrap(),
                        EstablishIssueSubmit::Accepted
                    );
                }
                _ => panic!("startup trace contains only original context operations"),
            }
        }
        (
            round,
            clock,
            error,
            logical,
            drive,
            cancellation,
            requester,
            abort,
        )
    }

    #[tokio::test]
    async fn rejected_create_waits_for_positive_replacement_under_current_qa_policy() {
        // Distinguish policy-closed and cancellation-closed from a positive
        // process observation; the original public Invariant never changes.
        for (establish_unknown, case) in [false, true]
            .into_iter()
            .flat_map(|establish_unknown| (0..3).map(move |case| (establish_unknown, case)))
        {
            let (round, _clock, error, logical, drive, cancellation, requester, _abort) =
                original_rejected_create_drive(case != 1, establish_unknown).await;
            let context = *round.execution().graph().contexts().next().unwrap();
            let original = error.preparation_input();
            let source = Arc::new(PendingProcessObservation::new(
                context.backend_process_id(),
                BackendProcessObservation::Unobservable,
            ));
            let mut assembled = pending_pre_ready_round_with_failure(round, source.clone(), error);
            let change = async {
                if case != 1 {
                    while source.calls.load(std::sync::atomic::Ordering::SeqCst) == 0 {
                        tokio::task::yield_now().await;
                    }
                    if case == 2 {
                        requester.request(CancellationReason::Requested).unwrap();
                    } else {
                        source.replace();
                    }
                }
            };
            let (terminal, ()) = tokio::time::timeout(std::time::Duration::from_secs(2), async {
                tokio::join!(
                    assembled.run(
                        &drive,
                        cancellation,
                        ManifestAttemptCompletion::AcceptedRootSuccessSeal
                    ),
                    change
                )
            })
            .await
            .unwrap();
            let NativeAttemptTerminal::Failed(failure) = terminal else {
                panic!("original Create rejection")
            };
            assert_eq!(failure.input(), &original);
            assert_eq!(failure.class(), AttemptFailureClass::ContractViolation);
            assert_eq!(failure.pre_ready_replacement().is_some(), case == 0);
            if case == 1 {
                assert_eq!(source.calls.load(std::sync::atomic::Ordering::SeqCst), 0);
                assert_eq!(
                    source
                        .subscriptions
                        .load(std::sync::atomic::Ordering::SeqCst),
                    0
                );
            }
            drop(assembled);
            drop(drive);
            finish_pending_actor(logical, context).await;
        }
    }

    #[tokio::test]
    async fn rejected_create_unknown_establish_expires_without_a_replacement_witness() {
        for (state, late_positive) in [
            BackendProcessObservation::Current,
            BackendProcessObservation::Unobservable,
        ]
        .into_iter()
        .flat_map(|state| [false, true].into_iter().map(move |late| (state, late)))
        {
            let (round, clock, error, logical, drive, _cancellation, _requester, _abort) =
                original_rejected_create_drive(true, true).await;
            let context = *round.execution().graph().contexts().next().unwrap();
            let original = error.preparation_input();
            let TaskExecutionError::OperationFailed { operation_id, .. } = &error else {
                panic!("original Create rejection")
            };
            assert_eq!(
                round
                    .execution()
                    .rejected_create_observation_remaining(context, *operation_id),
                Some(std::time::Duration::from_secs(7)),
            );
            let source = Arc::new(PendingProcessObservation::new(
                context.backend_process_id(),
                state,
            ));
            let mut assembled = pending_pre_ready_round_with_failure(round, source.clone(), error);
            assert!(assembled.pending_pre_ready_must_wait());
            clock.advance(std::time::Duration::from_secs(6));
            assert!(assembled.pending_pre_ready_must_wait());
            clock.advance(std::time::Duration::from_secs(1));
            let calls = source.calls.load(std::sync::atomic::Ordering::SeqCst);
            assert!(!assembled.pending_pre_ready_must_wait());
            // A positive fact after expiry cannot revive the original cut.
            // The other branch retains only Current or Unobservable evidence.
            if late_positive {
                clock.advance(std::time::Duration::from_secs(1));
                source.replace();
            }
            assert!(!assembled.pending_pre_ready_must_wait());
            assert_eq!(
                source.calls.load(std::sync::atomic::Ordering::SeqCst),
                calls
            );
            let NativeAttemptTerminal::Failed(failure) =
                &assembled.pending_terminal.as_ref().unwrap().terminal
            else {
                panic!("original Create rejection")
            };
            assert_eq!(failure.input(), &original);
            assert_eq!(failure.class(), AttemptFailureClass::ContractViolation);
            assert!(failure.pre_ready_replacement().is_none());
            assert!(!assembled.pending_pre_ready_must_wait());
            drop(assembled);
            drop(drive);
            finish_pending_actor(logical, context).await;
        }
    }

    #[test]
    fn rejected_create_observation_ends_without_positive_replacement_at_original_expiry() {
        for state in [
            BackendProcessObservation::Current,
            BackendProcessObservation::Unobservable,
        ] {
            let (round, clock, error, _) = super::super::tests::pre_ready_original_create_refusal();
            let context = *round.execution().graph().contexts().next().unwrap();
            let original = error.preparation_input();
            let source = Arc::new(PendingProcessObservation::new(
                context.backend_process_id(),
                state,
            ));
            let mut assembled = pending_pre_ready_round_with_failure(round, source.clone(), error);
            assert!(assembled.pending_pre_ready_must_wait());
            // Even positive evidence arriving after the original lease cannot
            // revive this candidate or start another observation interval.
            source.replace();
            clock.advance(std::time::Duration::from_secs(30));
            let calls = source.calls.load(std::sync::atomic::Ordering::SeqCst);
            assert!(!assembled.pending_pre_ready_must_wait());
            assert_eq!(
                source.calls.load(std::sync::atomic::Ordering::SeqCst),
                calls
            );
            let NativeAttemptTerminal::Failed(failure) =
                &assembled.pending_terminal.as_ref().unwrap().terminal
            else {
                panic!("original rejection")
            };
            assert_eq!(failure.input(), &original);
            assert!(failure.pre_ready_replacement().is_none());
        }
    }

    #[tokio::test]
    async fn rejected_establish_waits_for_exact_replacement_without_rewriting_failure() {
        let (round, error, logical, drive, cancellation, _, _abort) =
            original_rejected_establish_drive(true).await;
        let context = *round.execution().graph().contexts().next().unwrap();
        let original = error.preparation_input();
        let source = Arc::new(PendingProcessObservation::new(
            context.backend_process_id(),
            BackendProcessObservation::Unobservable,
        ));
        let mut assembled = pending_pre_ready_round_with_failure(round, source.clone(), error);
        let replace = async {
            while source.calls.load(std::sync::atomic::Ordering::SeqCst) == 0 {
                tokio::task::yield_now().await;
            }
            source.replace();
        };
        let (terminal, ()) = tokio::time::timeout(std::time::Duration::from_secs(2), async {
            tokio::join!(
                assembled.run(
                    &drive,
                    cancellation,
                    ManifestAttemptCompletion::AcceptedRootSuccessSeal
                ),
                replace
            )
        })
        .await
        .unwrap();
        let NativeAttemptTerminal::Failed(failure) = terminal else {
            panic!("original rejection")
        };
        assert_eq!(failure.input(), &original);
        assert_eq!(failure.class(), AttemptFailureClass::ContractViolation);
        let witness = failure
            .pre_ready_replacement()
            .expect("separate positive process fact");
        assert_eq!(witness.context(), context);
        assert_ne!(witness.replacement_process(), context.backend_process_id());
        drop(assembled);
        drop(drive);
        finish_pending_actor(logical, context).await;
    }

    #[tokio::test]
    async fn rejected_establish_no_recovery_does_not_observe_or_delay_the_original_failure() {
        let (round, error, logical, drive, cancellation, _, _abort) =
            original_rejected_establish_drive(false).await;
        let context = *round.execution().graph().contexts().next().unwrap();
        let original = error.preparation_input();
        let source = Arc::new(PendingProcessObservation::new(
            context.backend_process_id(),
            BackendProcessObservation::Unobservable,
        ));
        let mut assembled = pending_pre_ready_round_with_failure(round, source.clone(), error);
        let NativeAttemptTerminal::Failed(failure) = tokio::time::timeout(
            std::time::Duration::from_secs(2),
            assembled.run(
                &drive,
                cancellation,
                ManifestAttemptCompletion::AcceptedRootSuccessSeal,
            ),
        )
        .await
        .unwrap() else {
            panic!("original rejection")
        };
        assert_eq!(failure.input(), &original);
        assert!(failure.pre_ready_replacement().is_none());
        assert_eq!(
            source
                .subscriptions
                .load(std::sync::atomic::Ordering::SeqCst),
            0
        );
        assert_eq!(source.calls.load(std::sync::atomic::Ordering::SeqCst), 0);
        drop(assembled);
        drop(drive);
        finish_pending_actor(logical, context).await;
    }

    #[tokio::test]
    async fn rejected_establish_late_stop_ends_observation_without_replacing_the_task_failure() {
        for stop_before_run in [false, true] {
            let (round, error, logical, drive, cancellation, requester, _abort) =
                original_rejected_establish_drive(true).await;
            let context = *round.execution().graph().contexts().next().unwrap();
            let original = error.preparation_input();
            let source = Arc::new(PendingProcessObservation::new(
                context.backend_process_id(),
                BackendProcessObservation::Unobservable,
            ));
            let mut assembled = pending_pre_ready_round_with_failure(round, source.clone(), error);
            if stop_before_run {
                requester.request(CancellationReason::Requested).unwrap();
            }
            let stop = async {
                if !stop_before_run {
                    while source.calls.load(std::sync::atomic::Ordering::SeqCst) == 0 {
                        tokio::task::yield_now().await;
                    }
                    requester.request(CancellationReason::Requested).unwrap();
                }
            };
            let (terminal, ()) = tokio::time::timeout(std::time::Duration::from_secs(2), async {
                tokio::join!(
                    assembled.run(
                        &drive,
                        cancellation,
                        ManifestAttemptCompletion::AcceptedRootSuccessSeal,
                    ),
                    stop
                )
            })
            .await
            .unwrap();
            let NativeAttemptTerminal::Failed(failure) = terminal else {
                panic!("original rejection")
            };
            // This is the Task producer's fact. QA still owns its independently
            // accepted logical cancellation and terminal priority.
            assert_eq!(failure.input(), &original);
            assert!(failure.pre_ready_replacement().is_none());
            if stop_before_run {
                assert_eq!(
                    source
                        .subscriptions
                        .load(std::sync::atomic::Ordering::SeqCst),
                    0
                );
                assert_eq!(source.calls.load(std::sync::atomic::Ordering::SeqCst), 0);
            } else {
                assert!(source.calls.load(std::sync::atomic::Ordering::SeqCst) > 0);
            }
            drop(assembled);
            drop(drive);
            finish_pending_actor(logical, context).await;
        }
    }

    #[test]
    fn rejected_establish_expiry_cannot_observe_or_authorize_replacement() {
        let (round, clock, error) =
            super::super::tests::pre_ready_identity_rejected_observation_round();
        let context = *round.execution().graph().contexts().next().unwrap();
        let original = error.preparation_input();
        let source = Arc::new(PendingProcessObservation::new(
            context.backend_process_id(),
            BackendProcessObservation::Replaced {
                current_process: novarocks_types::BackendProcessId::new_v7(),
            },
        ));
        let mut assembled = pending_pre_ready_round_with_failure(round, source.clone(), error);
        clock.advance(super::super::context_owner::ADMISSION_TICKET_VALID_FOR);
        assert!(!assembled.pending_pre_ready_must_wait());
        assert_eq!(source.calls.load(std::sync::atomic::Ordering::SeqCst), 0);
        let NativeAttemptTerminal::Failed(failure) =
            &assembled.pending_terminal.as_ref().unwrap().terminal
        else {
            panic!("original rejection")
        };
        assert_eq!(failure.input(), &original);
        assert!(failure.pre_ready_replacement().is_none());
    }

    #[test]
    fn pending_unknown_uses_original_ticket_horizon_and_only_positive_exact_replacement() {
        for state in [
            BackendProcessObservation::Current,
            BackendProcessObservation::Unobservable,
            BackendProcessObservation::Replaced {
                current_process: novarocks_types::BackendProcessId::new_v7(),
            },
        ] {
            let (round, clock) = super::super::tests::pre_ready_observation_round();
            let backend = round
                .execution()
                .graph()
                .contexts()
                .next()
                .unwrap()
                .backend_process_id();
            let source = Arc::new(PendingProcessObservation::new(backend, state));
            let mut assembled = pending_pre_ready_round(round, source.clone());
            assert!(
                !assembled
                    .convergence
                    .process_replaced(novarocks_types::BackendProcessId::new_v7())
            );
            assert_eq!(source.calls.load(std::sync::atomic::Ordering::SeqCst), 0);
            let replaced = matches!(state, BackendProcessObservation::Replaced { .. });
            assert_eq!(assembled.pending_pre_ready_must_wait(), !replaced);
            if !replaced {
                clock.advance(super::super::context_owner::ADMISSION_TICKET_VALID_FOR);
                assert!(!assembled.pending_pre_ready_must_wait());
            }
            let Some(NativeAttemptTerminal::Failed(failure)) =
                assembled.pending_terminal.map(|pending| pending.terminal)
            else {
                panic!("expected original failure")
            };
            assert_eq!(
                failure.class(),
                if replaced {
                    AttemptFailureClass::RecoverableInfrastructure
                } else {
                    AttemptFailureClass::ExecutionFailure
                }
            );
        }
        // The observer itself cannot authorize a foreign or equal process.
        for error in [false, true] {
            let (round, _) = super::super::tests::pre_ready_observation_round();
            let backend = round
                .execution()
                .graph()
                .contexts()
                .next()
                .unwrap()
                .backend_process_id();
            let source = Arc::new(PendingProcessObservation::new(
                backend,
                BackendProcessObservation::Replaced {
                    current_process: backend,
                },
            ));
            if error {
                *source.observation.lock().unwrap() = Err(
                    novarocks_query_application::api::BackendTopologyError::Unavailable {
                        message: "original observer".into(),
                    },
                );
            }
            let mut assembled = pending_pre_ready_round(round, source);
            assert!(assembled.pending_pre_ready_must_wait());
            let Some(NativeAttemptTerminal::Failed(failure)) = assembled
                .pending_terminal
                .as_ref()
                .map(|pending| &pending.terminal)
            else {
                panic!("expected original failure")
            };
            assert_eq!(failure.class(), AttemptFailureClass::ExecutionFailure);
        }
    }

    async fn finish_pending_actor(
        mut logical: novarocks_query_application::test_support::LogicalExecutionTestHarness,
        context: novarocks_execution_contract::QueryContextRef,
    ) {
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
        logical
            .observe_worker_process_replaced(context)
            .await
            .unwrap();
        logical
            .finish_until(std::time::Instant::now() + std::time::Duration::from_secs(2))
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn pending_unknown_without_query_deadline_observes_replacement_before_original_ticket_expiry()
     {
        let (round, _) = super::super::tests::pre_ready_observation_round();
        let context = *round.execution().graph().contexts().next().unwrap();
        let (logical, drive, cancellation, _, _abort) = terminal_regression_drive(context).await;
        assert_eq!(cancellation.deadline(), None);
        let source = Arc::new(PendingProcessObservation::new(
            context.backend_process_id(),
            BackendProcessObservation::Unobservable,
        ));
        let mut assembled = pending_pre_ready_round(round, source.clone());
        let update = async {
            while source.calls.load(std::sync::atomic::Ordering::SeqCst) == 0 {
                tokio::task::yield_now().await;
            }
            source.replace();
        };
        let (terminal, ()) = tokio::time::timeout(std::time::Duration::from_secs(2), async {
            tokio::join!(
                assembled.run(
                    &drive,
                    cancellation,
                    ManifestAttemptCompletion::AcceptedRootSuccessSeal
                ),
                update,
            )
        })
        .await
        .unwrap();
        let NativeAttemptTerminal::Failed(failure) = terminal else {
            panic!("expected backend loss")
        };
        assert_eq!(
            failure.class(),
            AttemptFailureClass::RecoverableInfrastructure
        );
        assert_eq!(
            failure.topology_requirement(),
            novarocks_query_application::api::NativeAttemptTopologyRequirement::ExcludeProcess(
                context.backend_process_id()
            )
        );
        drop(assembled);
        drop(drive);
        finish_pending_actor(logical, context).await;
    }

    #[tokio::test]
    async fn pending_unknown_cancellation_returns_original_owner_without_waiting_for_replacement() {
        let (round, _) = super::super::tests::pre_ready_observation_round();
        let context = *round.execution().graph().contexts().next().unwrap();
        let (logical, drive, cancellation, requester, _abort) =
            terminal_regression_drive(context).await;
        let mut assembled = pending_pre_ready_round(round, Arc::new(UnusedConvergenceSource));
        requester.request(CancellationReason::Requested).unwrap();
        let terminal = assembled
            .run(
                &drive,
                cancellation,
                ManifestAttemptCompletion::AcceptedRootSuccessSeal,
            )
            .await;
        let NativeAttemptTerminal::Failed(failure) = terminal else {
            panic!("expected cancellation")
        };
        assert_eq!(failure.class(), AttemptFailureClass::Cancelled);
        drop(assembled);
        drop(drive);
        finish_pending_actor(logical, context).await;
    }

    #[tokio::test]
    async fn pending_unknown_without_query_deadline_stops_at_original_ticket_horizon() {
        let (round, clock) = super::super::tests::pre_ready_observation_round();
        let context = *round.execution().graph().contexts().next().unwrap();
        let (logical, drive, cancellation, _, _abort) = terminal_regression_drive(context).await;
        assert_eq!(cancellation.deadline(), None);
        let source = Arc::new(PendingProcessObservation::new(
            context.backend_process_id(),
            BackendProcessObservation::Current,
        ));
        let mut assembled = pending_pre_ready_round(round, source.clone());
        let expire = async {
            while source.calls.load(std::sync::atomic::Ordering::SeqCst) == 0 {
                tokio::task::yield_now().await;
            }
            clock.advance(super::super::context_owner::ADMISSION_TICKET_VALID_FOR);
        };
        let (terminal, ()) = tokio::time::timeout(std::time::Duration::from_secs(2), async {
            tokio::join!(
                assembled.run(
                    &drive,
                    cancellation,
                    ManifestAttemptCompletion::AcceptedRootSuccessSeal
                ),
                expire,
            )
        })
        .await
        .unwrap();
        let NativeAttemptTerminal::Failed(failure) = terminal else {
            panic!("expected original uncertainty")
        };
        assert_eq!(failure.class(), AttemptFailureClass::ExecutionFailure);
        assert_eq!(
            failure.topology_requirement(),
            novarocks_query_application::api::NativeAttemptTopologyRequirement::LiveSnapshot
        );
        drop(assembled);
        drop(drive);
        finish_pending_actor(logical, context).await;
    }

    #[tokio::test]
    async fn pending_pre_ready_keeps_close_diagnostics_without_replacing_original_uncertainty() {
        use crate::query_execution::split_assignment::SplitAssignmentDriverError;
        use crate::query_execution::split_assignment_round::failure_observation_plan;
        use novarocks_execution_contract::failure::FailureProjection;
        use std::sync::atomic::Ordering;
        use std::time::Duration;

        let (round, clock) = super::super::tests::pre_ready_observation_round();
        let context = *round.execution().graph().contexts().next().unwrap();
        let (logical, drive, cancellation, _, _abort) = terminal_regression_drive(context).await;
        assert_eq!(cancellation.deadline(), None);
        let process = Arc::new(PendingProcessObservation::new(
            context.backend_process_id(),
            BackendProcessObservation::Current,
        ));
        let mut assembled = pending_pre_ready_round(round, process.clone());
        let Some(NativeAttemptTerminal::Failed(original)) = assembled
            .pending_terminal
            .as_ref()
            .map(|pending| &pending.terminal)
        else {
            panic!("the original Establish uncertainty must already exist");
        };
        let original_input = original.input().clone();
        let data_runtime = FrontendDataRuntime::new(tokio::runtime::Handle::current());
        let mut executors =
            crate::task_execution::blocking_io::tests::OriginalExecutorsFixture::install(
                data_runtime.connector_blocking_io(),
            );
        let (release, released) = std::sync::mpsc::channel();
        let (plan, enumerations, closes) = failure_observation_plan(
            context.query_execution_id(),
            data_runtime.connector_blocking_io().clone(),
            Some(released),
        );
        let guard = assembled
            .install_split_assignment(context.query_execution_id(), plan, data_runtime)
            .unwrap();
        let original_source = guard.failure_source();
        assembled.round.begin_input_close();
        let cleanup_then_expire = async {
            while process.calls.load(Ordering::SeqCst) == 0 || closes.load(Ordering::SeqCst) == 0 {
                tokio::task::yield_now().await;
            }
            release.send(()).unwrap();
            while !guard.is_finished() {
                tokio::task::yield_now().await;
            }
            assert!(original_source.failure().is_none());
            // Require another real pending turn after close returned. The run
            // must still await its own Establish evidence, rather than ending
            // because an unrelated cleanup diagnostic arrived.
            let calls_after_close = process.calls.load(Ordering::SeqCst);
            while process.calls.load(Ordering::SeqCst) <= calls_after_close {
                tokio::task::yield_now().await;
            }
            clock.advance(super::super::context_owner::ADMISSION_TICKET_VALID_FOR);
        };
        let (terminal, ()) = tokio::time::timeout(Duration::from_secs(2), async {
            tokio::join!(
                assembled.run(
                    &drive,
                    cancellation,
                    ManifestAttemptCompletion::AcceptedRootSuccessSeal
                ),
                cleanup_then_expire
            )
        })
        .await
        .unwrap();
        let NativeAttemptTerminal::Failed(failure) = terminal else {
            panic!("expected the original bounded Establish uncertainty");
        };
        assert_eq!(failure.input(), &original_input);
        assert_eq!(failure.class(), AttemptFailureClass::ExecutionFailure);
        assert_eq!(
            failure.topology_requirement(),
            novarocks_query_application::api::NativeAttemptTopologyRequirement::LiveSnapshot
        );
        assert!(guard.failure().is_none());
        assert!(original_source.failure().is_none());
        assert_eq!(enumerations.load(Ordering::SeqCst), 0);
        assert_eq!(closes.load(Ordering::SeqCst), 1);
        assert!(assembled.round.pumps_closed());
        let profile = guard.profile_source().current();
        let cleanup = guard.published_close_failure_for_test().unwrap();
        drop(guard);
        assert!(matches!(
            profile,
            crate::query_execution::split_assignment_round::SplitAssignmentProfileRead::Failed
        ));
        assert!(matches!(
            cleanup,
            SplitAssignmentDriverError::SourceClose { .. }
        ));
        let input = cleanup.execution_failure().unwrap();
        let provider = novarocks_spi::connector::ConnectorError::new(
            novarocks_spi::connector::ConnectorErrorKind::ResourceExhausted,
            "diagnostic only",
        );
        assert_eq!(input.fact(), provider.failure_fact());
        assert_eq!(
            input.detail().as_str(),
            "closing the connector split source failed"
        );
        assert_eq!(
            assembled
                .round
                .take_pump_close_error()
                .unwrap()
                .preparation_input(),
            input
        );
        assembled.round.receive_without_dispatch().unwrap();
        assert!(assembled.round.take_pump_close_error().is_none());
        drop(assembled);
        drop(drive);
        finish_pending_actor(logical, context).await;
        executors.shutdown().await;
    }

    #[tokio::test]
    async fn ordinary_manifest_run_preserves_the_original_enumeration_fact_without_backend_recovery()
     {
        use crate::query_execution::split_assignment::SplitAssignmentDriverError;
        use crate::query_execution::split_assignment_round::failure_observation_plan;
        use novarocks_execution_contract::failure::FailureProjection;
        use novarocks_query_application::execution_error::QueryFailureReason;
        use std::sync::atomic::Ordering;
        use std::time::Duration;

        let (round, _) = super::super::tests::pre_ready_observation_round();
        let context = *round.execution().graph().contexts().next().unwrap();
        let (logical, drive, cancellation, _, _abort) = terminal_regression_drive(context).await;
        let mut assembled = pending_pre_ready_round(round, Arc::new(UnusedConvergenceSource));
        assembled.pending_terminal = None;
        assembled.pending_pre_ready = None;
        let data_runtime = FrontendDataRuntime::new(tokio::runtime::Handle::current());
        let mut executors =
            crate::task_execution::blocking_io::tests::OriginalExecutorsFixture::install(
                data_runtime.connector_blocking_io(),
            );
        let (plan, enumerations, closes) = failure_observation_plan(
            context.query_execution_id(),
            data_runtime.connector_blocking_io().clone(),
            None,
        );
        let guard = assembled
            .install_split_assignment(context.query_execution_id(), plan, data_runtime)
            .unwrap();
        let original_source = guard.failure_source();
        assert!(original_source.failure().is_none());
        let terminal = tokio::time::timeout(
            Duration::from_secs(2),
            assembled.run(
                &drive,
                cancellation,
                ManifestAttemptCompletion::AcceptedRootSuccessSeal,
            ),
        )
        .await
        .unwrap();
        let NativeAttemptTerminal::Failed(failure) = terminal else {
            panic!("expected the installed source's enumeration failure");
        };
        let original = guard.failure().unwrap();
        assert!(matches!(
            original,
            SplitAssignmentDriverError::SourceEnumeration { .. }
        ));
        assert_eq!(original_source.failure(), Some(original.clone()));
        assert_eq!(failure.input(), &original.execution_failure().unwrap());
        let provider = novarocks_spi::connector::ConnectorError::new(
            novarocks_spi::connector::ConnectorErrorKind::ResourceExhausted,
            "diagnostic only",
        );
        assert_eq!(failure.input().fact(), provider.failure_fact());
        assert_ne!(
            failure.input().fact(),
            QueryFailureReason::SplitAssignmentFailed.failure_fact()
        );
        assert_ne!(
            failure.input().fact().reason(),
            QueryFailureReason::BackendLost.failure_fact().reason()
        );
        assert_eq!(
            failure.input().detail().as_str(),
            "enumerating the connector split source failed"
        );
        assert!(!failure.error().message().contains("opaque provider text"));
        assert_eq!(failure.class(), AttemptFailureClass::ResourceGovernance);
        assert_eq!(
            failure.topology_requirement(),
            novarocks_query_application::api::NativeAttemptTopologyRequirement::LiveSnapshot
        );
        assert_eq!(enumerations.load(Ordering::SeqCst), 1);
        // Selecting the original failure is earlier than its cleanup. Retain
        // the same pump and guard until the original close job actually exits.
        // The selected attempt terminal is separate from local convergence.
        // Request the same original input close before observing its real exit.
        assembled.round.begin_input_close();
        tokio::time::timeout(Duration::from_secs(2), async {
            while !guard.is_finished() || !assembled.round.pumps_closed() {
                assembled.round.receive_without_dispatch().unwrap();
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        assert_eq!(closes.load(Ordering::SeqCst), 1);
        let profile = guard.profile_source().current();
        let published = guard.published_close_failure_for_test().unwrap();
        drop(guard);
        assert!(matches!(
            profile,
            crate::query_execution::split_assignment_round::SplitAssignmentProfileRead::Failed
        ));
        assert_eq!(published, original);
        drop(assembled);
        drop(drive);
        finish_pending_actor(logical, context).await;
        executors.shutdown().await;
    }

    #[tokio::test]
    async fn pending_unknown_cannot_hide_an_accepted_originating_task_failure() {
        let round = super::super::tests::manifest_originating_failure_observation_round();
        let context = *round.execution().graph().contexts().next().unwrap();
        let (logical, drive, cancellation, _, _abort) = terminal_regression_drive(context).await;
        let mut assembled = pending_pre_ready_round(round, Arc::new(UnusedConvergenceSource));
        let root = assembled.round.execution().graph().root_identity();
        let terminal = assembled
            .run(
                &drive,
                cancellation,
                ManifestAttemptCompletion::AcceptedRootSuccessSeal,
            )
            .await;
        let NativeAttemptTerminal::Failed(failure) = terminal else {
            panic!("expected original Task failure")
        };
        assert_eq!(
            failure.input().detail().as_str(),
            "original preparing Task failure"
        );
        assert_eq!(
            failure.input().site(),
            novarocks_query_application::execution_error::ExecutionFailureSite::Task {
                identity: root,
                phase: novarocks_execution_contract::TaskFailurePhase::Preparation,
            }
        );
        assert_eq!(failure.class(), AttemptFailureClass::ExecutionFailure);
        drop(assembled);
        drop(drive);
        finish_pending_actor(logical, context).await;
    }

    #[test]
    fn pending_unknown_without_original_establish_window_never_invents_a_deadline() {
        for replaced in [false, true] {
            let round = super::super::tests::manifest_preparing_observation_round();
            let backend = round
                .execution()
                .graph()
                .contexts()
                .next()
                .unwrap()
                .backend_process_id();
            assert_eq!(
                round.execution().pre_ready_observation_remaining(backend),
                None
            );
            let state = if replaced {
                BackendProcessObservation::Replaced {
                    current_process: novarocks_types::BackendProcessId::new_v7(),
                }
            } else {
                BackendProcessObservation::Unobservable
            };
            let source = Arc::new(PendingProcessObservation::new(backend, state));
            let mut assembled = pending_pre_ready_round(round, source);
            assert!(!assembled.pending_pre_ready_must_wait());
            let Some(NativeAttemptTerminal::Failed(failure)) =
                assembled.pending_terminal.map(|pending| pending.terminal)
            else {
                panic!("expected failure")
            };
            assert_eq!(
                failure.class(),
                if replaced {
                    AttemptFailureClass::RecoverableInfrastructure
                } else {
                    AttemptFailureClass::ExecutionFailure
                }
            );
        }
    }
    #[test]
    fn a_real_unsent_replay_deadline_stops_unknown_observation_without_backend_loss() {
        use novarocks_execution_contract::failure::FailureProjection;
        use novarocks_query_application::execution_error::{
            ExecutionFailureSite, QueryFailureReason,
        };
        let (round, error) = super::super::tests::pre_ready_expired_replay_observation_round();
        assert!(matches!(
            error,
            TaskExecutionError::QueueResidenceExpired {
                kind: novarocks_execution_contract::OperationKind::UpdateQueryContext,
                ..
            }
        ));
        let mut assembled = pending_pre_ready_round(round, Arc::new(UnusedConvergenceSource));
        assembled.record_protocol_failure(error);
        assert_eq!(assembled.pending_pre_ready, None);
        assert!(!assembled.pending_pre_ready_must_wait());
        let Some(NativeAttemptTerminal::Failed(failure)) =
            assembled.pending_terminal.map(|pending| pending.terminal)
        else {
            panic!("a definitive local deadline must retain its original author");
        };
        assert_eq!(
            failure.input().fact(),
            QueryFailureReason::TransportDeadline.failure_fact()
        );
        assert_eq!(failure.input().site(), ExecutionFailureSite::Coordinator);
        assert_eq!(failure.class(), AttemptFailureClass::ExecutionFailure);
    }

    #[tokio::test]
    async fn pending_unknown_closed_process_watch_keeps_original_ticket_and_failure() {
        struct ClosedProcessObservation {
            expected: novarocks_types::BackendProcessId,
            calls: std::sync::atomic::AtomicUsize,
        }
        impl novarocks_query_application::api::BackendProcessObservationPort for ClosedProcessObservation {
            // The default subscription is closed. Its closure is not a
            // replacement and must not grant a fresh observation horizon.
            fn observe_process_at_endpoint(
                &self,
                expected: novarocks_types::BackendProcessId,
                endpoint: &novarocks_execution::runtime::endpoint::RuntimeEndpoint,
            ) -> Result<
                BackendProcessObservation,
                novarocks_query_application::api::BackendTopologyError,
            > {
                assert_eq!(expected, self.expected);
                assert_eq!(
                    *endpoint,
                    novarocks_execution::runtime::endpoint::RuntimeEndpoint::new("127.0.0.1", 1)
                        .unwrap()
                );
                self.calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                Ok(BackendProcessObservation::Unobservable)
            }
        }
        let (round, clock) = super::super::tests::pre_ready_observation_round();
        let context = *round.execution().graph().contexts().next().unwrap();
        let (logical, drive, cancellation, _, _abort) = terminal_regression_drive(context).await;
        let source = Arc::new(ClosedProcessObservation {
            expected: context.backend_process_id(),
            calls: std::sync::atomic::AtomicUsize::new(0),
        });
        let mut assembled = pending_pre_ready_round(round, source.clone());
        let expire = async {
            while source.calls.load(std::sync::atomic::Ordering::SeqCst) == 0 {
                tokio::task::yield_now().await;
            }
            clock.advance(super::super::context_owner::ADMISSION_TICKET_VALID_FOR);
        };
        let (terminal, ()) = tokio::time::timeout(std::time::Duration::from_secs(2), async {
            tokio::join!(
                assembled.run(
                    &drive,
                    cancellation,
                    ManifestAttemptCompletion::AcceptedRootSuccessSeal
                ),
                expire,
            )
        })
        .await
        .unwrap();
        let NativeAttemptTerminal::Failed(failure) = terminal else {
            panic!("expected original uncertainty")
        };
        assert_eq!(failure.class(), AttemptFailureClass::ExecutionFailure);
        assert_eq!(
            failure.topology_requirement(),
            novarocks_query_application::api::NativeAttemptTopologyRequirement::LiveSnapshot
        );
        drop(assembled);
        drop(drive);
        finish_pending_actor(logical, context).await;
    }
    #[tokio::test]
    async fn pending_receiver_definitive_error_keeps_normal_priority_over_cancel_view() {
        use novarocks_execution_contract::failure::FailureProjection;
        use novarocks_query_application::execution_error::QueryFailureReason;
        let round = super::super::tests::pre_ready_invalid_ack_observation_round();
        let context = *round.execution().graph().contexts().next().unwrap();
        let (logical, drive, cancellation, requester, _abort) =
            terminal_regression_drive(context).await;
        let mut assembled = pending_pre_ready_round(round, Arc::new(UnusedConvergenceSource));
        requester.request(CancellationReason::Requested).unwrap();
        // The receiver sees a genuine protocol invariant in this same turn.
        // Existing non-unknown error priority must not depend on whether an
        // earlier transport candidate had been awaiting process evidence.
        let terminal = assembled
            .run(
                &drive,
                cancellation,
                ManifestAttemptCompletion::AcceptedRootSuccessSeal,
            )
            .await;
        let NativeAttemptTerminal::Failed(failure) = terminal else {
            panic!("expected protocol failure")
        };
        assert_eq!(
            failure.input().fact(),
            QueryFailureReason::PreparationProtocol.failure_fact()
        );
        assert_eq!(failure.class(), AttemptFailureClass::ContractViolation);
        drop(assembled);
        drop(drive);
        finish_pending_actor(logical, context).await;
    }

    #[tokio::test]
    async fn same_cut_original_task_failure_precedes_ordinary_subscription_loss_in_both_orders() {
        use crate::native::task_transport::SubscriptionState;
        use novarocks_query_application::execution_error::{
            ExecutionFailureInput, ExecutionFailureSite,
        };
        for state in [
            SubscriptionState::ConnectionLossExhausted,
            SubscriptionState::BudgetExhausted,
        ] {
            for observation_first in [true, false] {
                let round = super::super::tests::manifest_task_and_subscription_failure_round(
                    observation_first,
                    state,
                );
                let context = *round.execution().graph().contexts().next().unwrap();
                let root = round.execution().graph().root_identity();
                let (logical, drive, cancellation, _, _abort) =
                    terminal_regression_drive(context).await;
                let mut assembled =
                    pending_pre_ready_round(round, Arc::new(UnusedConvergenceSource));
                // This is an ordinary active attempt, not the special pending
                // Establish-unknown path that already preserves original causes.
                assembled.pending_terminal = None;
                assembled.pending_pre_ready = None;
                let terminal = assembled
                    .run(
                        &drive,
                        cancellation.clone(),
                        ManifestAttemptCompletion::AcceptedRootSuccessSeal,
                    )
                    .await;
                let original = ExecutionFailureInput::from_selected_task(
                    assembled.round.execution().selected_task_failure().unwrap(),
                )
                .unwrap();
                let NativeAttemptTerminal::Failed(failure) = &terminal else {
                    panic!("ordinary observation loss cannot hide the accepted Task failure");
                };
                assert_eq!(failure.input(), &original);
                assert_eq!(original.detail().as_str(), "same-cut original Task failure");
                assert_eq!(
                    original.site(),
                    ExecutionFailureSite::Task {
                        identity: root,
                        phase: novarocks_execution_contract::TaskFailurePhase::Preparation,
                    }
                );
                assert_eq!(failure.class(), AttemptFailureClass::ExecutionFailure);
                assert!(
                    assembled
                        .pending_terminal
                        .as_ref()
                        .unwrap()
                        .allows_accepted_task_cause
                );
                // The chosen terminal is memoized; later polling does not select again.
                assert_eq!(
                    assembled
                        .run(
                            &drive,
                            cancellation,
                            ManifestAttemptCompletion::AcceptedRootSuccessSeal
                        )
                        .await,
                    terminal
                );
                drop(assembled);
                drop(drive);
                finish_pending_actor(logical, context).await;
            }
        }
    }

    #[tokio::test]
    async fn same_cut_task_cause_does_not_wait_for_the_original_held_gate_carrier() {
        use super::super::intent::{DispatchBatch, OperationIntent, TaskOperationSubmit};
        use crate::{
            native::task_transport::SubscriptionState, task_protocol_config::DispatchLane,
        };
        use novarocks_execution_contract::{
            AcquireQueryContextAdmissionTicket, AdmissionEpochCapability, LeaseValidFor,
            TaskOperationId,
        };
        use novarocks_query_application::execution_error::ExecutionFailureInput;
        for state in [
            SubscriptionState::ConnectionLossExhausted,
            SubscriptionState::BudgetExhausted,
        ] {
            let round =
                super::super::tests::manifest_task_and_subscription_failure_round(true, state);
            let context = *round.execution().graph().contexts().next().unwrap();
            let (logical, drive, cancellation, _, _abort) =
                terminal_regression_drive(context).await;
            let (sink, actor_gate, _) = ActorGatedTaskOperationSink::pair(
                Arc::new(BackpressuredGateTransport),
                Arc::new(super::super::status_intake::CountingWake::default()),
                novarocks_types::NativeCompatibilityId::new([0x41; 32]),
            );
            let operation = OperationIntent::AcquireQueryContextAdmissionTicket(
                AcquireQueryContextAdmissionTicket::new(
                    TaskOperationId::new_v7(),
                    context,
                    LeaseValidFor::new(std::time::Duration::from_secs(30)).unwrap(),
                    novarocks_types::NativeCompatibilityId::new([0x41; 32]),
                    AdmissionEpochCapability::try_from_bytes([9; 16]).unwrap(),
                ),
            );
            let operation_id = operation.operation_id();
            let bytes = operation.queue_request().queued_bytes();
            let TaskOperationSubmit::Backpressured(retained) =
                sink.try_submit(DispatchBatch::test_fixture(
                    context.backend_process_id(),
                    DispatchLane::Lifecycle,
                    vec![operation],
                    bytes,
                ))
            else {
                panic!("the same original carrier must remain at the actor gate");
            };
            let mut assembled = pending_pre_ready_round(round, Arc::new(UnusedConvergenceSource));
            assembled.pending_terminal = None;
            assembled.pending_pre_ready = None;
            assembled.actor_gate = actor_gate;
            let terminal = tokio::time::timeout(
                std::time::Duration::from_secs(2),
                assembled.run(
                    &drive,
                    cancellation,
                    ManifestAttemptCompletion::AcceptedRootSuccessSeal,
                ),
            )
            .await
            .expect("the logical terminal does not wait for its residual gate carrier");
            let original = ExecutionFailureInput::from_selected_task(
                assembled.round.execution().selected_task_failure().unwrap(),
            )
            .unwrap();
            let NativeAttemptTerminal::Failed(failure) = terminal else {
                panic!("accepted Task failure must be selected before gate convergence");
            };
            assert_eq!(failure.input(), &original);
            assert!(
                !assembled.actor_gate.prepare_convergence(),
                "publishing the terminal cannot fabricate actual gate exit"
            );
            match sink.try_submit(retained) {
                TaskOperationSubmit::Rejected { batch, reason } => {
                    assert_eq!(batch.operations()[0].operation_id(), operation_id);
                    assert!(reason.contains("closing"));
                }
                other => {
                    panic!("the same unsent carrier must retire at its original gate: {other:?}")
                }
            }
            assert!(assembled.actor_gate.prepare_convergence());
            drop(assembled);
            drop(drive);
            finish_pending_actor(logical, context).await;
        }
    }

    #[tokio::test]
    async fn same_cut_subscription_identity_violation_keeps_priority_over_original_task_failure() {
        use crate::native::task_transport::SubscriptionState;
        use novarocks_execution_contract::failure::FailureProjection;
        use novarocks_query_application::execution_error::{
            ExecutionFailureSite, QueryFailureReason,
        };
        let round = super::super::tests::manifest_task_and_subscription_failure_round(
            false,
            SubscriptionState::ProcessMismatch,
        );
        let context = *round.execution().graph().contexts().next().unwrap();
        let (logical, drive, cancellation, _, _abort) = terminal_regression_drive(context).await;
        let mut assembled = pending_pre_ready_round(round, Arc::new(UnusedConvergenceSource));
        assembled.pending_terminal = None;
        assembled.pending_pre_ready = None;
        let terminal = assembled
            .run(
                &drive,
                cancellation,
                ManifestAttemptCompletion::AcceptedRootSuccessSeal,
            )
            .await;
        assert!(
            assembled
                .round
                .execution()
                .selected_task_failure()
                .is_some()
        );
        let NativeAttemptTerminal::Failed(failure) = terminal else {
            panic!("identity violation must remain an attempt failure");
        };
        assert_eq!(failure.class(), AttemptFailureClass::ContractViolation);
        assert_eq!(
            failure.input().fact(),
            QueryFailureReason::PreparationProtocol.failure_fact()
        );
        assert_eq!(failure.input().site(), ExecutionFailureSite::Coordinator);
        drop(assembled);
        drop(drive);
        finish_pending_actor(logical, context).await;
    }

    #[tokio::test]
    async fn pending_receiver_task_failure_cannot_hide_a_later_same_turn_protocol_error() {
        use novarocks_execution_contract::failure::FailureProjection;
        use novarocks_query_application::execution_error::{
            ExecutionFailureInput, ExecutionFailureSite, QueryFailureReason,
        };
        let round =
            super::super::round::pending_task_failure_then_invalid_covered_frame_round().await;
        let context = *round.execution().graph().contexts().next().unwrap();
        let root = round.execution().graph().root_identity();
        let (logical, drive, cancellation, _, _abort) = terminal_regression_drive(context).await;
        let mut assembled = pending_pre_ready_round(round, Arc::new(UnusedConvergenceSource));
        let terminal = assembled
            .run(
                &drive,
                cancellation,
                ManifestAttemptCompletion::AcceptedRootSuccessSeal,
            )
            .await;
        // The first valid frame really reached its original Task owner before
        // the second frame failed. No transaction rollback erases that fact.
        let selected = ExecutionFailureInput::from_selected_task(
            assembled.round.execution().selected_task_failure().unwrap(),
        )
        .unwrap();
        assert_eq!(
            selected.fact(),
            QueryFailureReason::PreparationFailed.failure_fact()
        );
        assert_eq!(
            selected.site(),
            ExecutionFailureSite::Task {
                identity: root,
                phase: novarocks_execution_contract::TaskFailurePhase::Preparation,
            }
        );
        assert_eq!(
            selected.detail().as_str(),
            "original Task before invalid frame"
        );
        let NativeAttemptTerminal::Failed(failure) = terminal else {
            panic!("the invalid covered frame must preserve the protocol failure");
        };
        assert_eq!(
            failure.input().fact(),
            QueryFailureReason::PreparationProtocol.failure_fact()
        );
        assert_eq!(failure.input().site(), ExecutionFailureSite::Coordinator);
        assert_eq!(failure.class(), AttemptFailureClass::ContractViolation);
        drop(assembled);
        drop(drive);
        finish_pending_actor(logical, context).await;
    }

    #[derive(Debug)]
    struct OriginalSelectiveTransport {
        worker: Arc<super::super::tests::RecordingSink>,
        target: std::sync::Mutex<
            Option<(
                novarocks_types::BackendProcessId,
                novarocks_execution_contract::OperationKind,
            )>,
        >,
        block: std::sync::atomic::AtomicBool,
        held: std::sync::Mutex<Option<super::super::intent::OperationIntent>>,
    }
    impl OriginalSelectiveTransport {
        fn new(worker: Arc<super::super::tests::RecordingSink>) -> Self {
            Self {
                worker,
                target: std::sync::Mutex::new(None),
                block: std::sync::atomic::AtomicBool::new(true),
                held: std::sync::Mutex::new(None),
            }
        }
        fn held(&self) -> Option<super::super::intent::OperationIntent> {
            self.held.lock().unwrap().clone()
        }
    }
    impl TaskOperationSink for OriginalSelectiveTransport {
        fn try_reserve_queue(
            &self,
            request: super::super::intent::TaskOperationQueueRequest,
            preparation: &mut super::super::intent::TaskOperationQueuePreparation,
        ) -> super::super::intent::TaskOperationQueueAdmission {
            self.worker.try_reserve_queue(request, preparation)
        }
        fn try_submit(
            &self,
            batch: super::super::intent::DispatchBatch,
        ) -> super::super::intent::TaskOperationSubmit {
            let target = *self.target.lock().unwrap();
            if self.block.load(std::sync::atomic::Ordering::SeqCst)
                && let Some(intent) = batch
                    .operations()
                    .iter()
                    .find(|intent| target == Some((intent.backend_process_id(), intent.kind())))
            {
                let mut held = self.held.lock().unwrap();
                if let Some(original) = held.as_ref() {
                    assert_eq!(
                        original.operation_id(),
                        intent.operation_id(),
                        "the fixture holds the original immutable carrier"
                    );
                } else {
                    *held = Some(intent.clone());
                }
                return super::super::intent::TaskOperationSubmit::Backpressured(batch);
            }
            self.worker.try_submit(batch)
        }
    }

    // The test answers only intents actually accepted by the Worker-facing
    // original transport. It never calls the gated sink or dispatcher submit.
    fn answer_original_worker_intents(
        worker: &super::super::tests::RecordingSink,
        acks: &crate::native::task_transport::TaskAckIntakeHandle,
        unknown_target: Option<novarocks_execution_contract::QueryContextRef>,
        transport: &OriginalSelectiveTransport,
        late: &mut Option<super::super::intent::OperationAcknowledgement>,
    ) {
        use super::super::intent::{OperationAcknowledgement, OperationIntent};
        for (_, intents) in worker.take() {
            for intent in intents {
                let reply = match &intent {
                    OperationIntent::AcquireQueryContextAdmissionTicket(_) => {
                        super::super::tests::admission_ack(&intent)
                    }
                    OperationIntent::EstablishQueryContext(request) => {
                        let known = super::super::tests::establish_ack(&intent);
                        if unknown_target == Some(request.context()) {
                            if let Some(original) = late.as_ref() {
                                assert_eq!(original.operation_id(), known.operation_id());
                            }
                            *late = Some(known);
                            transport
                                .block
                                .store(true, std::sync::atomic::Ordering::SeqCst);
                            OperationAcknowledgement::transport_unknown(
                                intent.operation_id(),
                                intent.kind(),
                            )
                        } else {
                            known
                        }
                    }
                    OperationIntent::CreateTask(_) => {
                        super::super::tests::accepted_create_ack(&intent)
                    }
                    // Ordinary edge/Quiesce/Release custody remains with the
                    // original round; it is not an actor authorization verdict.
                    _ => continue,
                };
                acks.publish(reply);
            }
        }
    }

    #[tokio::test]
    async fn sealed_round_owns_queued_admission_and_unsent_establish_until_its_gate_closes() {
        use novarocks_execution_contract::{
            OperationKind, TaskOutputFacts, TaskState, TaskStatus, TaskStatusVersion,
        };
        for (mode, cancel_before_startup, late_after_handoff) in [
            (0, false, false),
            (1, false, false),
            (2, false, false),
            (2, false, true),
            (0, true, false),
        ] {
            let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(2);
            let worker = Arc::new(super::super::tests::RecordingSink::default());
            let transport = Arc::new(OriginalSelectiveTransport::new(Arc::clone(&worker)));
            let (mut round, actor_gate, acks, statuses, clock) =
                super::super::tests::manifest_original_gated_round_fixture(
                    Arc::clone(&worker),
                    Arc::clone(&transport) as Arc<dyn TaskOperationSink>,
                );
            let root = round.root_task();
            let contexts = round
                .execution()
                .graph()
                .contexts()
                .copied()
                .collect::<Vec<_>>();
            let target = *contexts
                .iter()
                .rfind(|context| context.backend_process_id() != root.backend_process_id())
                .unwrap();
            *transport.target.lock().unwrap() = Some((
                target.backend_process_id(),
                if mode == 0 {
                    OperationKind::AcquireQueryContextAdmissionTicket
                } else {
                    OperationKind::UpdateQueryContext
                },
            ));
            transport
                .block
                .store(mode != 2, std::sync::atomic::Ordering::SeqCst);
            let (mut logical, drive, cancellation, _, _abort) =
                terminal_regression_drive_for_contexts(contexts.clone(), false).await;
            let source = round.take_root_status_source().unwrap();
            let split_delivery = SplitDeliveryBridge::for_graph(round.execution().graph());
            let convergence = NativeAttemptConvergenceOwner::new(
                &round,
                Arc::new(UnusedConvergenceSource),
                contexts
                    .iter()
                    .map(|&context| {
                        NativeAttemptConvergenceTarget::new(
                            context,
                            novarocks_execution::runtime::endpoint::RuntimeEndpoint::new(
                                "127.0.0.1",
                                1,
                            )
                            .unwrap(),
                        )
                    })
                    .collect(),
            )
            .unwrap();
            let mut assembled = ManifestAssembledRound {
                round,
                split_delivery,
                actor_gate,
                protocol_facts: None,
                convergence,
                notify: Arc::new(tokio::sync::Notify::new()),
                terminal: None,
                pending_terminal: None,
                pending_pre_ready: None,
                split_failure: None,
                profile_diagnostics: None,
            };
            let mut late = None;
            if cancel_before_startup {
                use std::future::Future;
                let mut borrowed = Box::pin(assembled.advance(&drive));
                let mut cx = std::task::Context::from_waker(std::task::Waker::noop());
                assert!(matches!(
                    borrowed.as_mut().poll(&mut cx),
                    std::task::Poll::Pending
                ));
                drop(borrowed);
                assert!(
                    assembled
                        .actor_gate
                        .active_authorization_for_test()
                        .is_some(),
                    "the actual cancelled authorization remains in its original slot"
                );
                assert!(!assembled.round.accepted_root_success_sealed());
            }
            tokio::time::timeout_at(deadline, async {
                loop {
                    assembled.advance(&drive).await.unwrap();
                    answer_original_worker_intents(
                        &worker,
                        &acks,
                        (mode == 2).then_some(target),
                        &transport,
                        &mut late,
                    );
                    let other_tasks_owned = assembled
                        .round
                        .execution()
                        .graph()
                        .tasks()
                        .filter(|task| task.context() != target)
                        .all(|task| {
                            assembled
                                .round
                                .execution()
                                .task(task.identity().task_id())
                                .unwrap()
                                .create_ownership_proven()
                        });
                    if transport.held().is_some() && other_tasks_owned {
                        break;
                    }
                    clock.advance(std::time::Duration::from_millis(1));
                    tokio::task::yield_now().await;
                }
            })
            .await
            .expect("the original non-target Workers accept their actual startup requests");
            let held = transport.held().unwrap();
            if mode == 2 {
                assert_eq!(late.as_ref().unwrap().operation_id(), held.operation_id());
                assert!(
                    assembled
                        .actor_gate
                        .retained_late_establish_for_test(held.operation_id())
                );
            }
            if mode == 2 && !late_after_handoff {
                // Before handoff the original round and actor-gate owner still
                // own this late receipt. Feed only its actual intake: advance
                // cancels the exact queued replay and applies the Worker fact.
                acks.publish(late.take().unwrap());
                tokio::time::timeout_at(deadline, assembled.advance(&drive))
                    .await
                    .unwrap()
                    .unwrap();
                assert!(
                    assembled
                        .round
                        .execution()
                        .owner(target)
                        .unwrap()
                        .establish_acknowledged(),
                    "the active original owner must account its late Worker receipt"
                );
                assert!(
                    !assembled
                        .actor_gate
                        .retained_late_establish_for_test(held.operation_id())
                );
                tokio::time::timeout_at(deadline, async {
                    loop {
                        if logical.establish_snapshot(target).await.unwrap().is_some_and(|snapshot| {
                            snapshot.state() == novarocks_query_application::coordination::EstablishIssueState::WorkerSettled(
                                novarocks_query_application::coordination::EstablishWorkerSettlement::Applied)
                        }) { break; }
                        tokio::task::yield_now().await;
                    }
                }).await.unwrap();
            }
            // The Worker may publish its latest exact Finished snapshot even
            // when intermediate progress versions were coalesced.
            statuses.publish(super::super::status_intake::StatusEvent::Published(
                Box::new(
                    TaskStatus::try_new(
                        root,
                        TaskStatusVersion::new(4).unwrap(),
                        TaskState::Finished,
                        None,
                        TaskOutputFacts::new(true),
                    )
                    .unwrap(),
                ),
            ));
            let mut reply = Some(source.begin_success_seal_request().unwrap());
            let (publish_late, publish_after_handoff) = tokio::sync::oneshot::channel();
            let late_worker = late.map(|receipt| {
                let acks = acks.clone();
                let reply = reply.take().unwrap();
                tokio::spawn(async move {
                    assert_eq!(reply.await.unwrap(), Ok(()));
                    publish_after_handoff.await.unwrap();
                    // This same original Worker receipt arrives after local
                    // handoff. Publishing is not proof that the retired active
                    // owner applied it, nor is it Worker physical exit.
                    acks.publish(receipt);
                })
            });
            let terminal = tokio::time::timeout_at(
                deadline,
                assembled.run(
                    &drive,
                    cancellation,
                    ManifestAttemptCompletion::AcceptedRootSuccessSeal,
                ),
            )
            .await;
            let seal_reply = reply.as_mut().map(|reply| reply.try_recv());
            let sealed = assembled.round.accepted_root_success_sealed();
            let empty = assembled.actor_gate.prepare_convergence();
            let retained = format!("{:?}", assembled.actor_gate);
            let target_established = assembled
                .round
                .execution()
                .owner(target)
                .unwrap()
                .establish_acknowledged();
            let target_released = assembled
                .round
                .execution()
                .owner(target)
                .unwrap()
                .is_released();
            if late_after_handoff {
                // The snapshot names the latest replay, captured by the gate
                // but never sent by its inner transport. Convergence marks
                // that replay DefinitelyUnsent; the older unknown send still
                // requires the original ticket/context Abort responsibility.
                tokio::time::timeout_at(deadline, async {
                    loop {
                        if logical.establish_snapshot(target).await.unwrap().is_some_and(|snapshot| {
                            snapshot.state() == novarocks_query_application::coordination::EstablishIssueState::DefinitelyUnsent
                        }) { break; }
                        tokio::task::yield_now().await;
                    }
                }).await.unwrap();
            }
            // Do not make the result seal or run wait for a later receipt.
            // Its publisher is released only after the original local handoff.
            if late_after_handoff {
                publish_late.send(()).unwrap();
                tokio::time::timeout_at(deadline, late_worker.unwrap())
                    .await
                    .unwrap()
                    .unwrap();
                assert_eq!(logical.establish_snapshot(target).await.unwrap().unwrap().state(),
                    novarocks_query_application::coordination::EstablishIssueState::DefinitelyUnsent,
                    "publishing into the original intake is not an applied actor settlement");
            } else {
                assert!(late_worker.is_none());
            }
            let actor_error = logical.actor_snapshot().await.unwrap().establish_error;
            // Explicit fixture teardown is not a simulated physical-exit
            // assertion. The regression verdict below uses only local gate/round.
            drop(assembled);
            drop(drive);
            logical.abandon_running_attempt();
            for &context in &contexts {
                while logical
                    .stand_down_snapshot(context)
                    .await
                    .unwrap()
                    .is_none()
                {
                    tokio::task::yield_now().await;
                }
                if context == target {
                    let closure = logical
                        .stand_down_snapshot(context)
                        .await
                        .unwrap()
                        .unwrap()
                        .closure();
                    assert_ne!(closure,
                        novarocks_query_application::coordination::ContextClosureState::NoRemoteResponsibility,
                        "local gate closure cannot turn a pending admission or held ticket into NoGrant");
                    assert!(
                        !closure.residual_resource_settled(),
                        "publishing a late receipt is not the Registry's physical closure fact"
                    );
                }
                logical
                    .observe_worker_process_replaced(context)
                    .await
                    .unwrap();
            }
            logical
                .finish_until(std::time::Instant::now() + std::time::Duration::from_secs(2))
                .await
                .unwrap();
            assert!(sealed);
            if let Some(reply) = seal_reply {
                assert_eq!(reply.unwrap(), Ok(()));
            }
            assert!(
                actor_error.is_none(),
                "the exact original settlement must not create a ledger error: {actor_error:?}"
            );
            assert!(
                !target_released,
                "Native gate completion is not the Worker Release receipt"
            );
            if mode == 2 && !late_after_handoff {
                assert!(
                    target_established,
                    "the pre-handoff original Worker receipt was applied"
                );
            } else {
                assert!(
                    !target_established,
                    "an unsent or unobserved late carrier cannot fabricate an established context"
                );
            }
            assert!(
                matches!(terminal, Ok(NativeAttemptTerminal::Completed)),
                "original queued carrier did not converge: {terminal:?}; gate={retained}"
            );
            assert!(empty, "Completed must not hide an orphan gate: {retained}");
        }
    }
    include!("manifest_round_native_tail_tests.rs");
}
