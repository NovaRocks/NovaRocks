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

//! Scripted application contracts. Governance is always the real WC owner;
//! Native scripts own no opaque Native resources and never assert their exit.
use super::*;
use novarocks_coordination_contract::logical_port::LogicalToAttempt;
use novarocks_coordination_contract::{
    ExecutionEffect, IdSeed, MonotonicInstant, OperationIds, RecoveryMode,
};
use novarocks_execution_contract::{
    AcquireParticipationAdmissionTicket, AdmissionEpochCapability, AdmissionTicketId,
    LeaseValidFor, ParticipationAdmissionTicketReceipt, ParticipationRef,
};
use novarocks_logical_execution_protocol::{
    effect::{ConsumerEffect, NativeEffect},
    input::{
        ActionResult, ConsumerFact, NativeFact, QualificationSettlement, QualifiedAdmission,
        QualifiedReplacementFacts,
    },
    start::{
        InitialAttempt, LogicalBudgets, LogicalFinishBoundary, LogicalOutputMode, LogicalPolicy,
        ObservationBudgets,
    },
};
use novarocks_query_application::{
    api::LogicalFuture,
    coordination::executor::{NativeCleanupActive, NativeCleanupInventory},
};
use novarocks_types::{
    AttemptId, BackendProcessId, FrontendProcessId, NativeCompatibilityId, QueryExecutionId,
    QueryId,
};
use novarocks_workload_control::{Stage, WorkClass, WorkRequest, WorkloadControl};
use std::{
    collections::VecDeque,
    num::{NonZeroU32, NonZeroUsize},
    time::Instant,
};
use tokio::sync::Notify;

#[derive(Default)]
pub(crate) struct Gate {
    open: AtomicBool,
    changed: Notify,
}
impl Gate {
    pub(crate) fn open(&self) {
        if !self.open.swap(true, Ordering::AcqRel) {
            self.changed.notify_waiters();
        }
    }
    pub(crate) fn is_open(&self) -> bool {
        self.open.load(Ordering::Acquire)
    }
    async fn wait(&self) {
        loop {
            let changed = self.changed.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            if self.is_open() {
                return;
            }
            changed.await;
        }
    }
}
#[derive(Clone)]
pub(crate) struct SuccessorScript {
    pub(crate) execution: QueryExecutionId,
    pub(crate) participations: Vec<ParticipationRef>,
}
type PreparedRosters = Vec<(QueryExecutionId, Arc<[ParticipationRef]>)>;
#[derive(Clone, Default)]
pub(crate) struct LogicalScript {
    pub(crate) run: Arc<Gate>,
    pub(crate) snapshot: Arc<Gate>,
    pub(crate) omit_run_result: Arc<AtomicBool>,
    pub(crate) panic_run: Arc<AtomicBool>,
    pub(crate) core_panic: Arc<AtomicBool>,
    pub(crate) successors: Arc<Mutex<VecDeque<SuccessorScript>>>,
    seen: Arc<Mutex<Vec<LogicalEffectBody>>>,
    native_calls: Arc<Mutex<Vec<NativeEffect>>>,
    governor_seen: Arc<Mutex<Vec<GovernanceObligationSnapshot>>>,
    prepared_rosters: Arc<Mutex<PreparedRosters>>,
}
pub(crate) fn participations(execution: QueryExecutionId, n: usize) -> Vec<ParticipationRef> {
    let mut process = OperationIds::new(IdSeed::new(500, 1));
    let frontend = FrontendProcessId::try_from_bytes(process.issue().to_bytes()).unwrap();
    let mut ids = OperationIds::new(IdSeed::new(
        1000 + u128::from(execution.query_id().low().unsigned_abs()) * 10_000
            + u128::from(execution.attempt_id().get()) * 100,
        1,
    ));
    (0..n)
        .map(|_| {
            ParticipationRef::new(
                execution,
                frontend,
                BackendProcessId::try_from_bytes(ids.issue().to_bytes()).unwrap(),
            )
        })
        .collect()
}
impl LogicalScript {
    pub(crate) fn native_compatibility_id() -> NativeCompatibilityId {
        NativeCompatibilityId::new([1; 32])
    }
    pub(crate) fn start(ordinal: u64, roster: usize, max_attempts: u32) -> LogicalStart {
        let query = QueryId::new(9000, i64::try_from(ordinal).unwrap());
        let execution = QueryExecutionId::new(query, AttemptId::new(1).unwrap()).unwrap();
        let activation = AttemptActivationIdentity::from_parts(ordinal + 100, execution, 1);
        LogicalStart {
            identity: query,
            actor_instance: activation.actor(),
            policy: LogicalPolicy {
                recovery: if max_attempts > 1 {
                    RecoveryMode::RestartAttemptBeforeVisibility
                } else {
                    RecoveryMode::NoRecovery
                },
                effect: ExecutionEffect::None,
                output: LogicalOutputMode::CompletionOnly,
                finish: LogicalFinishBoundary::TaskCompletion,
                retain_diagnostics: false,
                root_carrier: None,
                internal_plan: None,
            },
            budgets: LogicalBudgets {
                max_attempts: NonZeroU32::new(max_attempts).unwrap(),
                replacement_reservation_valid_for: (max_attempts > 1)
                    .then_some(Duration::from_secs(1)),
                remote_cleanup_timeout: Duration::from_secs(1),
                admission_issues_per_participation: NonZeroUsize::new(2).unwrap(),
                establish_per_participation: NonZeroUsize::new(2).unwrap(),
                abort_per_participation: NonZeroUsize::new(2).unwrap(),
                observation: ObservationBudgets {
                    diagnostics: Duration::from_millis(20),
                    convergence: Duration::from_secs(1),
                },
            },
            first_attempt: InitialAttempt {
                activation,
                participations: participations(execution, roster).into_boxed_slice(),
            },
            statement_deadline: None,
            id_seed: IdSeed::new(u128::from(ordinal) + 1000, 100),
            started_at: MonotonicInstant::ORIGIN,
        }
    }
    pub(super) fn prepared_roster(
        &self,
        execution: QueryExecutionId,
    ) -> Option<Arc<[ParticipationRef]>> {
        self.prepared_rosters
            .lock()
            .unwrap()
            .iter()
            .find(|(e, _)| *e == execution)
            .map(|(_, r)| r.clone())
    }
    pub(crate) fn effects(&self) -> Vec<LogicalEffectBody> {
        self.seen.lock().unwrap().clone()
    }
    pub(crate) fn native_cleanup(&self) -> Vec<NativeEffect> {
        self.native_calls.lock().unwrap().clone()
    }
    pub(crate) fn governor_snapshot(&self) -> Vec<GovernanceObligationSnapshot> {
        self.governor_seen.lock().unwrap().clone()
    }
    pub(crate) fn successor_activation(&self) -> Option<AttemptActivationIdentity> {
        self.effects().iter().rev().find_map(|e| match e {
            LogicalEffectBody::Native(NativeEffect::ActivateAttempt { activation, .. }) => {
                Some(*activation)
            }
            _ => None,
        })
    }
}
pub(super) fn governor(start: &LogicalStart, control: &WorkloadControl) -> Arc<GovernanceExecutor> {
    let root = control
        .try_begin_root(WorkRequest::new(WorkClass::Query))
        .unwrap();
    let stage = root.owner.scope().try_acquire(Stage::Execution).unwrap();
    root.business.release();
    Arc::new(
        GovernanceExecutor::new(start.identity, start.actor_instance, root.owner, stage).unwrap(),
    )
}
struct NativeState {
    active: Vec<NativeCleanupActive>,
    preparation_retired: bool,
    body_closed: bool,
}
struct ScriptOriginals {
    script: LogicalScript,
    governor: Arc<GovernanceExecutor>,
    links: Arc<LogicalAttemptLinks>,
    initial: AttemptActivationIdentity,
    native: Mutex<NativeState>,
    failure: Mutex<Option<QueryExecutionError>>,
    installed_at: Instant,
    started_at: MonotonicInstant,
    clock: Arc<crate::task_execution::clock::ManualClock>,
}
impl ScriptOriginals {
    fn remember_governor(&self) {
        *self.script.governor_seen.lock().unwrap() = self.governor.cleanup_inventory();
    }
    async fn native(&self, effect: NativeEffect) -> NativeFact {
        self.script
            .native_calls
            .lock()
            .unwrap()
            .push(effect.clone());
        match effect {
            NativeEffect::ActivateAttempt { activation, .. } => {
                let changes = self.links.changes();
                loop {
                    let changed = changes.notified();
                    tokio::pin!(changed);
                    changed.as_mut().enable();
                    if let Some(endpoint) = self.links.endpoint(activation) {
                        let mut startup = endpoint.startup();
                        startup
                            .wait_ready()
                            .await
                            .expect("same original AP bootstrap exited");
                        self.native
                            .lock()
                            .unwrap()
                            .active
                            .push(NativeCleanupActive {
                                activation,
                                resources_exited: false,
                                adopted: false,
                                dormant_retired: false,
                                terminal_cut: None,
                            });
                        return NativeFact::AttemptActivated {
                            activation,
                            root: None,
                        };
                    }
                    changed.await;
                }
            }
            NativeEffect::RunAttempt { activation } => {
                assert!(
                    !self.script.panic_run.load(Ordering::Acquire),
                    "scripted original Native run factory panic"
                );
                self.script.run.wait().await;
                NativeFact::RunCompleted { activation }
            }
            NativeEffect::PrepareSuccessor { replacement, .. } => {
                let original = self
                    .script
                    .successors
                    .lock()
                    .unwrap()
                    .pop_front()
                    .expect("test supplied exact successor backend response");
                assert_eq!(
                    original.execution, replacement,
                    "same original PrepareSuccessor execution"
                );
                let roster: Arc<[ParticipationRef]> = original.participations.into();
                self.script
                    .prepared_rosters
                    .lock()
                    .unwrap()
                    .push((replacement, roster.clone()));
                NativeFact::SuccessorPrepared {
                    execution: replacement,
                    participations: roster,
                }
            }
            NativeEffect::DispatchQualification {
                identity,
                operation_id,
                replacement_participations,
                issued_at,
                expires_at,
                ..
            } => {
                let valid_for =
                    LeaseValidFor::new(expires_at.saturating_duration_since(issued_at)).unwrap();
                let mut ids =
                    OperationIds::new(IdSeed::new(3000 + u128::from(identity.generation), 1));
                let admissions = replacement_participations
                    .iter()
                    .map(|p| {
                        let operation = ids.issue();
                        let raw = operation.to_bytes();
                        QualifiedAdmission {
                            request: AcquireParticipationAdmissionTicket::new(
                                operation,
                                *p,
                                valid_for,
                                LogicalScript::native_compatibility_id(),
                                AdmissionEpochCapability::try_from_bytes(raw).unwrap(),
                            ),
                            receipt: ParticipationAdmissionTicketReceipt::new(
                                AdmissionTicketId::try_from_bytes(raw).unwrap(),
                                *p,
                                valid_for,
                            ),
                        }
                    })
                    .collect::<Vec<_>>()
                    .into_boxed_slice();
                NativeFact::QualificationSettled {
                    identity,
                    operation_id,
                    dispatched_at: Some(novarocks_coordination_contract::MonotonicClock::now(
                        self.clock.as_ref(),
                    )),
                    settlement: QualificationSettlement::Qualified(QualifiedReplacementFacts {
                        identity,
                        operation_id,
                        topology_revision: 1,
                        admissions,
                    }),
                }
            }
            NativeEffect::ActivateReservation {
                identity,
                operation_id,
            } => NativeFact::ReservationActivated {
                identity,
                operation_id,
            },
            NativeEffect::FinishResources { activation }
            | NativeEffect::AbandonResources { activation } => {
                let mut state = self.native.lock().unwrap();
                let original = state
                    .active
                    .iter_mut()
                    .find(|a| a.activation == activation)
                    .expect("same activated script owner");
                original.resources_exited = true;
                self.remember_governor();
                NativeFact::Applied
            }
            NativeEffect::RecordTerminalCut {
                activation,
                cut,
                remote_deadline,
            } => {
                if let Some(original) = self
                    .native
                    .lock()
                    .unwrap()
                    .active
                    .iter_mut()
                    .find(|a| a.activation == activation)
                {
                    original.terminal_cut = Some((cut, remote_deadline));
                }
                NativeFact::Applied
            }
            NativeEffect::ConvergeDormant { execution, .. } => {
                NativeFact::DormantConverged { execution }
            }
            NativeEffect::RetirePreparation => {
                self.native.lock().unwrap().preparation_retired = true;
                NativeFact::Applied
            }
            NativeEffect::CancelQualification { .. }
            | NativeEffect::AbandonReservation { .. }
            | NativeEffect::RecordStreamingSuccess { .. }
            | NativeEffect::RecordComputationReady { .. } => NativeFact::Applied,
        }
    }
}
impl LogicalFailureOriginals for ScriptOriginals {
    fn governance(&self) -> Arc<GovernanceExecutor> {
        self.governor.clone()
    }
    fn begin_failure(&self, error: QueryExecutionError) {
        *self.failure.lock().unwrap() = Some(error);
        self.script.run.open();
        self.script.snapshot.open();
    }
    fn fail_one_pending_request(&self) -> bool {
        false
    }
    fn cleanup_inventory(&self) -> Result<NativeCleanupInventory, QueryExecutionError> {
        Ok(NativeCleanupInventory {
            initial_activation: self.initial,
            preparations: Vec::new(),
            active: self.native.lock().unwrap().active.clone(),
            qualifications: Vec::new(),
        })
    }
    fn original_cleanup_deadline(&self, deadline: MonotonicInstant) -> Instant {
        self.installed_at + deadline.saturating_duration_since(self.started_at)
    }
    fn execute_native(&self, effect: NativeEffect) -> LogicalFuture<'_, NativeFact> {
        Box::pin(self.native(effect))
    }
    fn close_registry_ingress_after_owner_failure(
        &self,
    ) -> LogicalFuture<'_, Result<(), QueryExecutionError>> {
        Box::pin(async {
            assert!(self.failure.lock().unwrap().is_some());
            Ok(())
        })
    }
    fn adopt_pending_activation_for_cleanup(
        &self,
    ) -> LogicalFuture<'_, Result<Option<AttemptActivationIdentity>, QueryExecutionError>> {
        Box::pin(async { Ok(None) })
    }
    fn converge_dormant_for_owner_failure(
        &self,
        _: QueryExecutionId,
    ) -> LogicalFuture<'_, Result<(), QueryExecutionError>> {
        Box::pin(async { Ok(()) })
    }
    fn retire_adopted_dormant_for_cleanup(
        &self,
        _: AttemptActivationIdentity,
    ) -> LogicalFuture<'_, Result<(), QueryExecutionError>> {
        Box::pin(async { Ok(()) })
    }
    fn close_original_body_for_cleanup(
        &self,
    ) -> LogicalFuture<'_, Result<(), QueryExecutionError>> {
        Box::pin(async {
            let mut state = self.native.lock().unwrap();
            assert!(state.preparation_retired && state.active.iter().all(|a| a.resources_exited));
            state.body_closed = true;
            self.remember_governor();
            Ok(())
        })
    }
}
pub(super) struct ScriptExecutor {
    bootstrap: Arc<LogicalBootstrapPublisher>,
    state: Arc<LogicalInstalledState>,
    originals: Arc<ScriptOriginals>,
}
impl ScriptExecutor {
    pub(super) fn new(
        parts: &LogicalParts,
        script: LogicalScript,
        governor: Arc<GovernanceExecutor>,
        start: LogicalStart,
        clock: Arc<crate::task_execution::clock::ManualClock>,
    ) -> Self {
        Self {
            bootstrap: parts.bootstrap.clone(),
            state: parts.state.clone(),
            originals: Arc::new(ScriptOriginals {
                script,
                governor,
                links: parts.links.clone(),
                initial: start.first_attempt.activation,
                native: Mutex::new(NativeState {
                    active: Vec::new(),
                    preparation_retired: false,
                    body_closed: false,
                }),
                failure: Mutex::new(None),
                installed_at: Instant::now(),
                started_at: start.started_at,
                clock,
            }),
        }
    }
}
impl LogicalJobExecutor for ScriptExecutor {
    fn failure_executors(&self) -> Option<Arc<dyn LogicalFailureOriginals>> {
        Some(self.originals.clone())
    }
    fn deliver_port_now(
        &self,
        runtime: &ActorRuntime<FrontendActor>,
        ticket: &novarocks_control_runtime::actor_turn::EffectTicket<FrontendEffect>,
        original: LogicalToAttempt,
    ) -> Option<Result<(), QueryExecutionError>> {
        runtime
            .validate_effect_ticket(ticket)
            .expect("same live LP source ticket");
        self.originals
            .script
            .seen
            .lock()
            .unwrap()
            .push(LogicalEffectBody::AttemptPort(Box::new(original.clone())));
        let endpoint = self
            .originals
            .links
            .endpoint(original.activation)
            .expect("same activated AP endpoint");
        Some(
            endpoint
                .deliver(runtime, ticket, original.clone())
                .map_err(|e| {
                    panic!("original endpoint refused LP handoff: {e:?}; message={original:?}")
                }),
        )
    }
    fn execute(&self, body: LogicalHostEffect) -> LogicalFuture<'_, Option<ActionResult>> {
        Box::pin(async move {
            let effect = match body {
                LogicalHostEffect::Bootstrap => {
                    self.bootstrap.publish_started().await.unwrap();
                    self.state.stopped().await;
                    return None;
                }
                LogicalHostEffect::ClientCloseObservation => {
                    panic!("script CompletionOnly has no client observer")
                }
                LogicalHostEffect::JoinReadiness(ready) => {
                    self.state.readiness_changed(ready);
                    return None;
                }
                LogicalHostEffect::PortDelivered(_) => return None,
                LogicalHostEffect::Protocol(effect) => *effect,
            };
            self.originals
                .script
                .seen
                .lock()
                .unwrap()
                .push(effect.body.clone());
            match effect.body {
                LogicalEffectBody::Native(NativeEffect::RunAttempt { .. })
                    if self
                        .originals
                        .script
                        .omit_run_result
                        .load(Ordering::Acquire) =>
                {
                    None
                }
                LogicalEffectBody::Native(effect) => {
                    Some(ActionResult::Native(self.originals.native(effect).await))
                }
                LogicalEffectBody::Governance(effect) => {
                    let fact = self.originals.governor.execute(effect).await;
                    self.originals.remember_governor();
                    Some(ActionResult::Governance(fact))
                }
                LogicalEffectBody::Consumer(effect) => Some(ActionResult::Consumer(match effect {
                    ConsumerEffect::PublishDiagnostics { .. } => ConsumerFact::DiagnosticsPublished,
                    ConsumerEffect::PublishConvergence { .. } => ConsumerFact::ConvergencePublished,
                    ConsumerEffect::PublishReady { .. } => ConsumerFact::ReadyHandedOff,
                    ConsumerEffect::WriteSchema { .. } => ConsumerFact::SchemaWritten,
                    ConsumerEffect::ReserveEndSlot { .. } => ConsumerFact::EndSlotReady,
                    ConsumerEffect::EnqueueEnd { .. } => ConsumerFact::EndEnqueued,
                    ConsumerEffect::ExposeOutput { .. } => ConsumerFact::OutputExposed,
                    _ => ConsumerFact::Applied,
                })),
                LogicalEffectBody::AttemptLifecycleDiagnostic(_) => None,
                LogicalEffectBody::SnapshotReply { source, token } => {
                    self.originals.script.snapshot.wait().await;
                    self.state.snapshot_reply(source, token);
                    None
                }
                LogicalEffectBody::RegistryJoinReply {
                    source,
                    token,
                    ready,
                } => {
                    self.state.registry_reply(source, token, ready);
                    None
                }
                LogicalEffectBody::Retirable => {
                    self.state.retirable();
                    None
                }
                other => panic!("unsupported scripted application contract: {other:?}"),
            }
        })
    }
}
