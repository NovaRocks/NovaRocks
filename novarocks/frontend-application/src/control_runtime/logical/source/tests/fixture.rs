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

use super::super::*;
use crate::control_runtime::{
    actor::{FrontendActorKind, FrontendEffect},
    host::{FrontendLanePolicy, HostBinding, HostRegistry},
    logical::{
        adapter::LogicalActor,
        application::LogicalHostReplies,
        funding::SourceFundingObligation,
        geometry::{LogicalGeometry, OUTPUT_UNITS},
        inputs::{LogicalBootstrapPublisher, LogicalInputs},
    },
    metrics::FrontendHostObservations,
};
use novarocks_control_runtime::{
    actor_turn::{Clock, EffectTicket, StandingInputObligation},
    transaction::RuntimeLedger,
};
use novarocks_control_runtime_core::{
    capacity::ResourceVector,
    identity::{ActorId, Tick},
    owned_storage::OwnedSlotNeed,
    scope::WorkKind,
};
use novarocks_coordination_contract::{
    AttemptActionId, AttemptActivationIdentity, AttemptConvergenceClosure, ExecutionEffect, IdSeed,
    MonotonicInstant, OperationIds, QueryExecutionError, RecoveryMode,
    logical_port::{AttemptToLogical, AttemptToLogicalBody, LogicalToAttempt},
};
use novarocks_execution_contract::ParticipationRef;
use novarocks_logical_execution_protocol::{
    LogicalExecutionCore, LogicalRequestToken,
    effect::{ConsumerEffect, GovernanceEffect, LogicalEffectBody, NativeEffect},
    input::{ActionResult, ConsumerFact, GovernanceFact, NativeFact},
    start::{
        InitialAttempt, LogicalBudgets, LogicalFinishBoundary, LogicalOutputMode, LogicalPolicy,
        LogicalStart, ObservationBudgets,
    },
};
use novarocks_query_application::api::LogicalFuture;
use novarocks_types::{AttemptId, BackendProcessId, FrontendProcessId, QueryExecutionId, QueryId};
use std::{
    future::Future,
    num::{NonZeroU32, NonZeroU64, NonZeroUsize},
    pin::Pin,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    task::{Context, Poll, Wake, Waker},
    time::Duration,
};
use tokio::sync::Notify;

pub(super) fn activation() -> AttemptActivationIdentity {
    AttemptActivationIdentity::from_parts(
        71,
        QueryExecutionId::new(QueryId::new(17, 19), AttemptId::new(1).unwrap()).unwrap(),
        12,
    )
}
pub(super) fn request_token(n: u64) -> LogicalRequestToken {
    LogicalRequestToken::new(NonZeroU64::new(n).unwrap())
}
pub(super) fn participations(n: usize) -> Vec<ParticipationRef> {
    let mut ids = OperationIds::new(IdSeed::new(41, 1));
    let frontend = FrontendProcessId::try_from_bytes(ids.issue().to_bytes()).unwrap();
    (0..n)
        .map(|_| {
            ParticipationRef::new(
                activation().execution(),
                frontend,
                BackendProcessId::try_from_bytes(ids.issue().to_bytes()).unwrap(),
            )
        })
        .collect()
}
pub(super) fn start() -> LogicalStart {
    LogicalStart {
        identity: activation().execution().query_id(),
        actor_instance: activation().actor(),
        policy: LogicalPolicy {
            recovery: RecoveryMode::NoRecovery,
            effect: ExecutionEffect::None,
            output: LogicalOutputMode::CompletionOnly,
            finish: LogicalFinishBoundary::TaskCompletion,
            retain_diagnostics: false,
            root_carrier: None,
            internal_plan: None,
        },
        budgets: LogicalBudgets {
            max_attempts: NonZeroU32::new(1).unwrap(),
            replacement_reservation_valid_for: None,
            remote_cleanup_timeout: Duration::from_millis(100),
            admission_issues_per_participation: NonZeroUsize::new(2).unwrap(),
            establish_per_participation: NonZeroUsize::new(2).unwrap(),
            abort_per_participation: NonZeroUsize::new(2).unwrap(),
            observation: ObservationBudgets {
                diagnostics: Duration::from_millis(20),
                convergence: Duration::from_millis(90),
            },
        },
        first_attempt: InitialAttempt {
            activation: activation(),
            participations: Box::new([]),
        },
        statement_deadline: None,
        id_seed: IdSeed::new(7, 100),
        started_at: MonotonicInstant::ORIGIN,
    }
}
#[derive(Default)]
pub(super) struct Gate {
    open: AtomicBool,
    changed: Notify,
}
impl Gate {
    pub(super) fn open(&self) {
        if !self.open.swap(true, Ordering::AcqRel) {
            self.changed.notify_waiters();
        }
    }
    pub(super) fn is_open(&self) -> bool {
        self.open.load(Ordering::Acquire)
    }
    async fn wait(&self) {
        let changed = self.changed.notified();
        tokio::pin!(changed);
        changed.as_mut().enable();
        if !self.is_open() {
            changed.await;
        }
    }
}
#[derive(Debug)]
pub(super) struct OriginalPanic;
struct ScriptExecutor {
    bootstrap: Arc<LogicalBootstrapPublisher>,
    state: Arc<LogicalInstalledState>,
    seen: Arc<Mutex<Vec<LogicalEffectBody>>>,
    run: Arc<Gate>,
    snapshot: Arc<Gate>,
    panic: Option<Arc<OriginalPanic>>,
    omit_run_result: Arc<AtomicBool>,
    drops: Arc<AtomicUsize>,
    join_ready: Arc<AtomicBool>,
}
impl Drop for ScriptExecutor {
    fn drop(&mut self) {
        self.drops.fetch_add(1, Ordering::SeqCst);
    }
}
impl LogicalJobExecutor for ScriptExecutor {
    fn deliver_port_now(
        &self,
        runtime: &ActorRuntime<FrontendActor>,
        ticket: &EffectTicket<FrontendEffect>,
        original: LogicalToAttempt,
    ) -> Option<Result<(), QueryExecutionError>> {
        runtime
            .validate_effect_ticket(ticket)
            .expect("same original live logical source ticket qualifies synchronous AP handoff");
        self.seen
            .lock()
            .unwrap()
            .push(LogicalEffectBody::AttemptPort(Box::new(original)));
        Some(Ok(()))
    }
    fn execute(&self, body: LogicalHostEffect) -> LogicalFuture<'_, Option<ActionResult>> {
        if let LogicalHostEffect::Protocol(effect) = &body {
            self.seen.lock().unwrap().push(effect.body.clone());
            if matches!(
                effect.body,
                LogicalEffectBody::Native(NativeEffect::RunAttempt { .. })
            ) && let Some(original) = &self.panic
            {
                std::panic::panic_any(Arc::clone(original));
            }
        }
        Box::pin(async move {
            let effect = match body {
                LogicalHostEffect::Bootstrap => {
                    self.bootstrap
                        .publish_started()
                        .await
                        .expect("same original bootstrap position");
                    // This fixture's original cancellation observer has no
                    // foreign producer. Its actual controlled body stops only
                    // after Core commits Retirable, just as the live observer.
                    self.state.stopped().await;
                    return None;
                }
                LogicalHostEffect::ClientCloseObservation => {
                    panic!("CompletionOnly has no client observer")
                }
                LogicalHostEffect::JoinReadiness(ready) => {
                    self.state.readiness_changed(ready);
                    self.join_ready.store(ready, Ordering::Release);
                    return None;
                }
                LogicalHostEffect::PortDelivered(_) => return None,
                LogicalHostEffect::Protocol(effect) => *effect,
            };
            match effect.body {
                LogicalEffectBody::Native(NativeEffect::ActivateAttempt { activation, .. }) => {
                    Some(ActionResult::Native(NativeFact::AttemptActivated {
                        activation,
                        root: None,
                    }))
                }
                LogicalEffectBody::Native(NativeEffect::RunAttempt { activation }) => {
                    if self.omit_run_result.load(Ordering::Acquire) {
                        return None;
                    }
                    self.run.wait().await;
                    Some(ActionResult::Native(NativeFact::RunCompleted {
                        activation,
                    }))
                }
                LogicalEffectBody::Native(
                    NativeEffect::FinishResources { .. }
                    | NativeEffect::AbandonResources { .. }
                    | NativeEffect::RetirePreparation
                    | NativeEffect::RecordTerminalCut { .. },
                ) => Some(ActionResult::Native(NativeFact::Applied)),
                LogicalEffectBody::Consumer(
                    ConsumerEffect::PublishCompletion | ConsumerEffect::PublishFailure(_),
                ) => Some(ActionResult::Consumer(ConsumerFact::Applied)),
                LogicalEffectBody::Governance(body) => Some(ActionResult::Governance(match body {
                    GovernanceEffect::RegisterObligation { activation, kind } => {
                        GovernanceFact::ObligationRegistered { activation, kind }
                    }
                    GovernanceEffect::ResolveObligation { activation, kind } => {
                        GovernanceFact::ObligationResolved { activation, kind }
                    }
                    GovernanceEffect::EndRemoteTracking { activation, kind } => {
                        GovernanceFact::RemoteTrackingEnded { activation, kind }
                    }
                    GovernanceEffect::ReleaseExecutionStage => GovernanceFact::StageReleased,
                    GovernanceEffect::CompleteWorkOwner => GovernanceFact::WorkOwnerCompleted,
                    GovernanceEffect::RequestShutdownCancellation
                    | GovernanceEffect::RequestClientCancellation => {
                        GovernanceFact::CancellationRequested
                    }
                })),
                LogicalEffectBody::AttemptPort(_) => panic!(
                    "original AP port must hand off synchronously under its live source ticket"
                ),
                LogicalEffectBody::AttemptLifecycleDiagnostic(_) => None,
                LogicalEffectBody::SnapshotReply { source, token } => {
                    self.snapshot.wait().await;
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
                unexpected => panic!(
                    "unexpected application effect in closed CompletionOnly script: {unexpected:?}"
                ),
            }
        })
    }
}
struct FixedClock;
impl Clock for FixedClock {
    fn now(&self) -> Tick {
        Tick(1)
    }
}
struct HostWake {
    runtime: ActorRuntime<FrontendActor>,
    registration: ActorRegistration,
    progress: Arc<AtomicBool>,
}
impl Wake for HostWake {
    fn wake(self: Arc<Self>) {
        self.wake_by_ref();
    }
    fn wake_by_ref(self: &Arc<Self>) {
        self.progress.store(true, Ordering::Release);
        let _ = self.runtime.notify_host_actor(&self.registration);
    }
}

pub(super) struct Fixture {
    pub(super) ledger: RuntimeLedger,
    pub(super) runtime: ActorRuntime<FrontendActor>,
    pub(super) host: HostRegistry,
    pub(super) registration: ActorRegistration,
    pub(super) geometry: LogicalGeometry,
    pub(super) state: Arc<LogicalInstalledState>,
    pub(super) failure: Arc<LogicalInfrastructureFailure>,
    pub(super) run: Arc<Gate>,
    pub(super) snapshot: Arc<Gate>,
    pub(super) panic: Arc<OriginalPanic>,
    pub(super) omit_run_result: Arc<AtomicBool>,
    pub(super) executor_drops: Arc<AtomicUsize>,
    pub(super) join_ready: Arc<AtomicBool>,
    pub(super) convert_timers: bool,
    inputs: Arc<LogicalInputBank>,
    seen: Arc<Mutex<Vec<LogicalEffectBody>>>,
    ap: Vec<StandingInputObligation<FrontendActor>>,
    ap_action: u64,
    ap_closed: bool,
}
impl Fixture {
    pub(super) fn new(panic_run: bool) -> Self {
        let ledger = RuntimeLedger::new(ResourceVector::default());
        let funding = LogicalFundingObligation::reserve(&ledger).unwrap();
        let start = start();
        start.validate().unwrap();
        let geometry = LogicalGeometry::derive(funding.scope().id(), &start).unwrap();
        let failure_work = funding.reserve_reported(WorkKind::Cleanup, 0).unwrap();
        let disposal_work = funding.reserve_reported(WorkKind::Cleanup, 0).unwrap();
        let core = LogicalExecutionCore::new(start.clone());
        let actor = LogicalActor::new(core, funding.step_reservations());
        let runtime = ActorRuntime::new();
        let registration = runtime
            .register_supervised_bound_with_source_cut(
                geometry.actor,
                funding.scope().clone(),
                FrontendActor::Logical(Box::new(actor)),
                geometry.config,
                failure_work.work(),
                geometry.source(0).unwrap(),
            )
            .unwrap_or_else(|(error, _)| {
                panic!("same original logical actor registration: {error:?}")
            });
        let progress_requested = Arc::new(AtomicBool::new(true));
        let wake = Waker::from(Arc::new(HostWake {
            runtime: runtime.clone(),
            registration: registration.clone(),
            progress: Arc::clone(&progress_requested),
        }));
        let marker_author = SourceFundingObligation::start(
            funding
                .reserve_step(
                    WorkKind::Settlement,
                    OwnedSlotNeed {
                        inputs: 1,
                        work_headers: 1,
                        ..Default::default()
                    },
                )
                .unwrap(),
        );
        let marker = runtime
            .reserve_admitted_input_for(
                &registration,
                geometry
                    .source(geometry.purposes + geometry.attempts + 6)
                    .unwrap(),
                InputClass::Lifecycle,
                marker_author.work(),
            )
            .unwrap()
            .into_standing_input()
            .unwrap_or_else(|_| panic!("same original paid failure marker"));
        let failure = LogicalInfrastructureFailure::new(marker, wake.clone());
        let state = LogicalInstalledState::new(wake.clone(), Arc::clone(&failure));
        let inputs = LogicalInputs::reserve(
            &runtime,
            registration.clone(),
            &funding,
            &geometry,
            &start,
            Arc::clone(&state),
            Arc::clone(&failure),
            wake.clone(),
        );
        let bootstrap = inputs.bootstrap();
        let inputs = LogicalInputBank::new(inputs, wake.clone());
        let mut ap = Vec::new();
        {
            let mut bank = inputs.try_loan().unwrap();
            let author = bank.ap_author(0).expect("same prepaid AP source Work");
            // This paid script is the fake AP fact producer; 91 is its fixed
            // metadata source key, never an authority reconstructed from ID.
            for class in [
                InputClass::Lifecycle,
                InputClass::Ordinary,
                InputClass::Ordinary,
            ] {
                ap.push(
                    runtime
                        .reserve_admitted_input_for(
                            &registration,
                            ActorId::new(91).unwrap(),
                            class,
                            author,
                        )
                        .unwrap()
                        .into_standing_input()
                        .unwrap_or_else(|_| panic!("same AP author and original role")),
                );
            }
        }
        let seen = Arc::new(Mutex::new(Vec::new()));
        let run = Arc::new(Gate::default());
        let snapshot = Arc::new(Gate::default());
        let panic = Arc::new(OriginalPanic);
        let omit_run_result = Arc::new(AtomicBool::new(false));
        let executor_drops = Arc::new(AtomicUsize::new(0));
        let join_ready = Arc::new(AtomicBool::new(false));
        let application: Arc<dyn LogicalJobExecutor> = Arc::new(ScriptExecutor {
            bootstrap,
            state: Arc::clone(&state),
            seen: Arc::clone(&seen),
            run: Arc::clone(&run),
            snapshot: Arc::clone(&snapshot),
            panic: panic_run.then(|| Arc::clone(&panic)),
            omit_run_result: omit_run_result.clone(),
            drops: Arc::clone(&executor_drops),
            join_ready: Arc::clone(&join_ready),
        });
        let executor = Arc::new(LogicalEffectExecutor {
            runtime: Handle::current(),
            original: Arc::downgrade(&application),
        });
        let scope = funding.scope().clone();
        let links = super::super::super::links::LogicalAttemptLinks::new(
            geometry.attempts,
            wake.clone(),
            failure.reporter(),
        );
        let source = LogicalEffectSource::reserve(LogicalSourceAssembly {
            runtime: &runtime,
            registration: registration.clone(),
            funding,
            geometry: &geometry,
            start: &start,
            failure: Arc::clone(&failure),
            executor: application,
            wake: wake.clone(),
            data_runtime: Handle::current(),
            inputs: Arc::clone(&inputs),
            state: Arc::clone(&state),
            links: links.clone(),
            failure_work,
            disposal_work,
            marker_author,
            progress_requested,
        });
        let host = HostRegistry::new(
            runtime.clone(),
            ledger.clone(),
            Arc::new(FrontendHostObservations::new(Arc::new(FixedClock))),
        );
        let exit = host
            .register_logical(
                HostBinding {
                    scope,
                    registration: registration.clone(),
                    kind: FrontendActorKind::Logical,
                    config: geometry.config,
                    policy: FrontendLanePolicy::logical(OUTPUT_UNITS),
                    source: Box::new(source),
                    executor,
                    transport: None,
                    ordered_observations: None,
                    startup: None,
                    capacity: None,
                    membership: None,
                    owner_controls: None,
                },
                start.actor_instance,
                links,
            )
            .unwrap_or_else(|original| {
                panic!("original logical host registration: {}", original.reason)
            });
        state.bind_exit(exit);
        runtime.notify_host_actor(&registration).unwrap();
        Self {
            ledger,
            runtime,
            host,
            registration,
            geometry,
            state,
            failure,
            run,
            snapshot,
            panic,
            omit_run_result,
            executor_drops,
            join_ready,
            convert_timers: true,
            inputs,
            seen,
            ap,
            ap_action: 1000,
            ap_closed: false,
        }
    }
    pub(super) fn count(&self, predicate: impl Fn(&LogicalEffectBody) -> bool) -> usize {
        self.seen
            .lock()
            .unwrap()
            .iter()
            .filter(|body| predicate(body))
            .count()
    }
    pub(super) fn has(&self, predicate: impl Fn(&LogicalEffectBody) -> bool) -> bool {
        self.count(predicate) != 0
    }
    pub(super) fn actors_only(&self) {
        for _ in 0..64 {
            if self.convert_timers {
                self.runtime.advance_time(Tick(1), 64).unwrap();
            }
            if !self.runtime.poll_one(Tick(1)).unwrap() {
                break;
            }
        }
    }
    pub(super) async fn round(&self) {
        self.actors_only();
        for _ in 0..64 {
            let Some(turn) = self.runtime.claim_host_actor() else {
                break;
            };
            self.host.drive(Tick(1), turn);
        }
        tokio::task::yield_now().await;
    }
    pub(super) async fn until(&mut self, predicate: impl Fn(&Self) -> bool) {
        for _ in 0..1024 {
            if predicate(self) {
                return;
            }
            self.round().await;
        }
        let snapshot = self.runtime.snapshot(self.registration.actor());
        panic!(
            "original logical path did not progress: actor={snapshot:?}, deadline={:?}, host_empty={}, ledger_empty={}, effects={:?}",
            self.runtime.next_deadline(),
            self.host.is_empty(),
            self.ledger.is_empty(),
            self.seen.lock().unwrap()
        );
    }
    pub(super) async fn wait<T>(&mut self, future: LogicalFuture<'_, T>) -> T {
        self.wait_named("unnamed controlled future", future).await
    }
    pub(super) async fn wait_named<T>(
        &mut self,
        label: &'static str,
        mut future: LogicalFuture<'_, T>,
    ) -> T {
        for _ in 0..1024 {
            if let Poll::Ready(value) = poll(future.as_mut()) {
                return value;
            }
            self.round().await;
        }
        panic!(
            "original controlled future did not settle: wait={label}, {}",
            self.diagnostic()
        );
    }
    fn diagnostic(&self) -> String {
        let actor = self.registration.actor();
        let inputs = self.runtime.input_inventory(actor, None, usize::MAX);
        let mut effects = Vec::new();
        if let Ok(originals) = &inputs {
            for original in originals {
                if let Ok(rows) =
                    self.runtime
                        .effect_inventory(actor, original.input, None, usize::MAX)
                    && !rows.is_empty()
                {
                    let rows = rows
                        .into_iter()
                        .map(|(descriptor, state)| {
                            let body = self.runtime.pending_effect_metadata(
                                actor,
                                original.input,
                                descriptor.id,
                            );
                            (descriptor, state, body)
                        })
                        .collect::<Vec<_>>();
                    effects.push((original.input, rows));
                }
            }
        }
        format!(
            "actor={:?}, deadline={:?}, host_empty={}, ledger_empty={}, inputs={inputs:?}, effects={effects:?}, seen={:?}",
            self.runtime.snapshot(actor),
            self.runtime.next_deadline(),
            self.host.is_empty(),
            self.ledger.is_empty(),
            self.seen.lock().unwrap()
        )
    }
    pub(super) async fn port(&mut self, body: AttemptToLogicalBody) {
        assert!(!self.ap_closed);
        assert!(
            self.ap[0].retire_ready_after_exit().unwrap(),
            "one exact original AP fact position"
        );
        self.ap_action += 1;
        self.ap[0]
            .fill(FrontendInput::Logical(Box::new(LogicalInput::AttemptPort(
                Box::new(AttemptToLogical {
                    activation: activation(),
                    action: AttemptActionId::new(NonZeroU64::new(self.ap_action).unwrap()),
                    body,
                }),
            ))))
            .unwrap_or_else(|(error, _)| panic!("original AP fact fill: {error:?}"));
        for _ in 0..1024 {
            self.round().await;
            if self.ap[0].retire_ready_after_exit().unwrap() {
                return;
            }
        }
        panic!("original AP input has not retired");
    }
    pub(super) async fn local_converged(&mut self) {
        self.until(|f|f.has(|e|matches!(e,LogicalEffectBody::AttemptPort(p) if matches!(p.body,novarocks_coordination_contract::logical_port::LogicalToAttemptBody::BeginConvergence{..})))).await;
        self.port(AttemptToLogicalBody::ConvergenceProgress {
            local_exited: true,
            closure: Some(Arc::new(AttemptConvergenceClosure::Evidence(Vec::new()))),
            lifecycle_runtime_filters: None,
        })
        .await;
    }
    pub(super) fn close_ap(&mut self) {
        assert!(!self.ap_closed);
        for position in &mut self.ap {
            assert!(position.retire_ready_after_exit().unwrap());
            position.close_after_exit().unwrap();
        }
        self.inputs
            .try_loan()
            .unwrap()
            .ap_inputs_closed_after_producer_exit(0)
            .unwrap();
        self.ap_closed = true;
    }
}
pub(super) fn poll<F: Future + ?Sized>(mut future: Pin<&mut F>) -> Poll<F::Output> {
    future
        .as_mut()
        .poll(&mut Context::from_waker(Waker::noop()))
}
