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

//! Original S4 tier: current AP Core, real H2 Source and exact Root carrier.
//! Open/ordinary protocol acknowledgements are explicit protocol-fixture
//! inputs. They claim no Native Create, Worker, logical Ready or resource grant.
#[path = "current_covered_recovery/terminal.rs"]
mod terminal;

use super::*;
use crate::{
    control_runtime::{
        actor::{FrontendActor, FrontendEffect, FrontendInput, TestActor},
        logical::adapter::{LogicalEffectEnvelope, LogicalHostEffect},
        transport_withdrawal::AttemptEffectEnvelope,
    },
    native::{
        data_runtime::FrontendDataRuntime, task_transport::tests::Loopback, transport::Client,
    },
    query_execution::native_execution_adapter::source_cohort::tests as wc,
    task_execution::host::{
        adapter::descriptor,
        custody::{OriginalTransportJoinObligation, tests::original_transport_join_id},
        observations::{
            MetadataRetirementRefusal, ObservationBudget, ObservationError,
            ObservationMetadataLayout, ObservationMetadataObligation, OrderedObservationInput,
            RootControlBinding, StreamKey,
            credits::ObservationCredits,
            host_turn::OrderedObservationHost,
            read_progress::ReadProgressInput,
            source::{BoundObservationSource, ObservationMetadataAuthor},
        },
        port::{
            control::{
                ControlBinding, ControlPositions, ControlReaderObligation, SourcePublishedSlot,
            },
            facts::{AttemptFactBank, FactRole},
            root_control::RootControlRequests,
        },
        subscriptions::{
            SubscriptionEnd, SubscriptionFailureRecord, SubscriptionPhasePublisher,
            SubscriptionSource,
        },
    },
};
use novarocks_attempt_protocol::{
    AttemptCore, Core, InputSelection,
    capacity::{QueueStage, StepContext},
    config::DispatchLane,
    effect::{AttemptEffect, EffectBody, Publication},
    input::{
        AttemptInput, AttemptInputClaim, AttemptInputId, AttemptSourceCut, AttemptSourceId,
        ExecutorFact, ObservationEvent, ObservationFact, PublicationApplied, ReceivedObservation,
    },
    observation::SubscriptionRequest,
    port_output::AttemptToLogicalBody,
    start::AttemptSources,
};
use novarocks_control_runtime::{
    actor::{ActorConfig, ActorStep},
    actor_turn::{ActorRegistration, ActorRuntime, InputAuthor, StandingInputObligation},
    causal_input::CausalInputObligation,
    completion::ReportSlot,
    exit_work::{PanicSink, run_future},
    obligation::{
        OwnedPositionObligation, ReservationObligation, WorkObligation, WorkRetirementObserver,
    },
    reported_work::ReportedWork,
    transaction::{RuntimeLedger, ScopeHandle},
};
use novarocks_control_runtime_core::{
    capacity::ReservationResources,
    completion::ConsumerKind,
    effect::{EffectKind, EffectState},
    identity::{ActionId, ActorId, ConsumerId, Generation, InputId, ScopedWorkRef, Tick},
    mailbox::{InputClass, InputEnvelope},
    owned_storage::{OwnedSlotKind, OwnedSlotNeed},
    scope::{WorkAdmission, WorkKind},
};
use novarocks_coordination_contract::{
    AttemptActionId, AttemptActivationIdentity, AttemptConvergenceMode, AttemptRequestToken,
    LogicalToAttempt, LogicalToAttemptBody, MonotonicClock, MonotonicInstant,
};
use novarocks_execution_contract::{
    CancelReason, ParticipationRef, ParticipationState, QuiesceParticipationReceipt, TaskIdentity,
    TaskOutputFacts, TaskState, TaskStatus, TaskStatusVersion, TerminationDetail,
};
use novarocks_logical_execution_protocol::{
    LogicalActionId,
    effect::{LogicalEffect, LogicalEffectBody},
};
use novarocks_proto_codec::FieldPath;
use novarocks_task_codec::operation::{
    CoveredCatchUpComplete, CoveredObservationBookmark, CoveredStatusStreamEvent,
    CoveredStatusStreamFact, decode_covered_subscribe_task_status, encode_covered_status_event,
};
use novarocks_workload_control::{ResultWindowClass, WorkClass, WorkRequest};
use std::{
    collections::{BTreeMap, VecDeque},
    future::poll_fn,
    num::NonZeroU64,
    sync::{Arc, Mutex},
    task::Waker,
    time::Duration,
};
use tokio::runtime::Handle;
#[derive(Debug, Default)]
struct ManualClock(Mutex<Duration>);
impl MonotonicClock for ManualClock {
    fn now(&self) -> MonotonicInstant {
        MonotonicInstant::from_origin(*self.0.lock().unwrap())
    }
}
impl ManualClock {
    fn advance(&self, by: Duration) {
        *self.0.lock().unwrap() += by;
    }
}
fn n(value: u64) -> NonZeroU64 {
    NonZeroU64::new(value).unwrap()
}
fn producer(source: AttemptSourceId) -> ActorId {
    ActorId::new(source.0.get()).unwrap()
}
fn project(input: AttemptInput) -> FrontendInput {
    FrontendInput::Attempt(Box::new(input))
}

#[derive(Default)]
struct Trace {
    consumed: Vec<InputId>,
    processed: Vec<InputId>,
    observations: Vec<ReceivedObservation>,
    effects: VecDeque<AttemptEffect>,
    last: BTreeMap<NonZeroU64, Arc<SubscriptionRequest>>,
    deadline: Option<MonotonicInstant>,
    seal_payload: Option<Arc<[novarocks_coordination_contract::ResultParticipationWithdrawal]>>,
    // Passive public Core turn summaries only; no private decision state.
    turns: Vec<String>,
    phases: Vec<novarocks_attempt_protocol::input::SubscriptionPhaseSnapshot>,
}
struct PaidUnits<'a, 'step>(&'a mut ActorStep<'step, FrontendEffect>);
impl StepContext for PaidUnits<'_, '_> {
    fn charge(&mut self, units: usize) -> bool {
        units <= self.0.remaining().saturating_sub(1) && self.0.charge(units)
    }
    fn reserve_queue_stage(
        &mut self,
        _: DispatchLane,
        _: bool,
        _: novarocks_types::BackendProcessId,
        _: usize,
        _: usize,
    ) -> QueueStage {
        // AdmitQueuedInput remains a SCRIPTED protocol intent. No queue grant
        // is fabricated by this lower-tier Root/observation test.
        panic!("the observation fixture never grants or dispatches Native work");
    }
}
#[derive(Clone)]
enum SeedMode {
    Recovery,
    Terminal(novarocks_coordination_contract::ExecutionFailure),
}
impl SeedMode {
    fn failure(&self) -> Option<&novarocks_coordination_contract::ExecutionFailure> {
        match self {
            Self::Recovery => None,
            Self::Terminal(original) => Some(original),
        }
    }
    fn check_round(&self, result: &novarocks_attempt_protocol::AttemptStep) {
        if let Some(Err(failure)) = &result.round_result {
            let original = self.failure().expect("Root recovery has no terminal input");
            assert_eq!(
                failure,
                &novarocks_attempt_protocol::failure::AttemptFailure::Logical(original.clone())
            );
        }
    }
}
struct CurrentCore {
    mode: SeedMode,
    core: Core,
    sources: AttemptSources,
    next: u64,
    author: Arc<Mutex<Option<InputAuthor>>>,
    trace: Arc<Mutex<Trace>>,
}
impl TestActor for CurrentCore {
    fn handle(
        &mut self,
        at: Tick,
        envelope: InputEnvelope,
        input: FrontendInput,
        step: &mut ActorStep<'_, FrontendEffect>,
    ) {
        let input = match input {
            FrontendInput::Attempt(input) => input,
            FrontendInput::Timer => Box::new(AttemptInput::Timer),
            _ => panic!("same AP protocol or original kernel Timer input"),
        };
        assert!(step.charge(1));
        let mut input = *input;
        let mut source = AttemptSourceId(n(envelope.source.get()));
        // The ordered writer is already bound to the exact original LP pair.
        // Its Root controls and observations share the Core observation phase.
        if matches!(
            &input,
            AttemptInput::Executor(ExecutorFact::Observations(_))
                | AttemptInput::Port(LogicalToAttempt {
                    body: LogicalToAttemptBody::SealRequest { .. }
                        | LogicalToAttemptBody::TerminalControlDemand { .. },
                    ..
                })
        ) {
            source = self.sources.observation;
        }
        if let AttemptInput::Executor(ExecutorFact::Observations(batch)) = &input {
            self.trace
                .lock()
                .unwrap()
                .observations
                .extend(batch.items().iter().cloned());
        }
        self.trace.lock().unwrap().consumed.push(envelope.input);
        let now = MonotonicInstant::from_origin(Duration::from_nanos(at.0));
        let mut id = AttemptInputId(n(envelope.input.get()));
        // A fixed, bounded fixture script; EVERY Core unit charges this SAME
        // real ActorStep. No uncharged ProtocolUnits(64) loop is retained.
        for _ in 0..64 {
            let input_kind = match &input {
                AttemptInput::Timer => "Timer",
                AttemptInput::Port(port) => match &port.body {
                    LogicalToAttemptBody::BeginConvergence { .. } => "BeginConvergence",
                    LogicalToAttemptBody::Stop { .. } => "Stop",
                    LogicalToAttemptBody::SealRequest { .. } => "SealRequest",
                    _ => "OtherOwnerPort",
                },
                AttemptInput::Executor(ExecutorFact::PublicationApplied { .. }) => {
                    "PublicationApplied"
                }
                AttemptInput::Executor(ExecutorFact::Observations(_)) => "Observations",
                AttemptInput::Executor(ExecutorFact::SubscriptionProgress(_)) => {
                    "SubscriptionProgress"
                }
                AttemptInput::Executor(ExecutorFact::SubscriptionExited { .. }) => {
                    "SubscriptionExited"
                }
                _ => "OtherOriginalInput",
            };
            let phase = match &input {
                AttemptInput::Executor(ExecutorFact::SubscriptionProgress(snapshot)) => {
                    Some(*snapshot)
                }
                _ => None,
            };
            let result = self
                .core
                .handle(
                    now,
                    AttemptInputClaim {
                        source,
                        input: id,
                        native_cut: Some(AttemptSourceCut {
                            source: self.sources.native,
                            tail: None,
                        }),
                        selected_cut: Some(AttemptSourceCut {
                            source: self.sources.observation,
                            tail: (source == self.sources.observation).then_some(id),
                        }),
                    },
                    input,
                    &mut PaidUnits(step),
                )
                .unwrap();
            {
                let mut trace = self.trace.lock().unwrap();
                trace.deadline = self.core.next_deadline();
                if let Some(phase) = phase {
                    trace.phases.push(phase);
                }
                let summary = format!(
                    "source={source:?}, input={input_kind}, selection={:?}, deadline={:?}, round={:?}, stop_producing={}",
                    result.selection,
                    trace.deadline,
                    result.round_result.as_ref().map(Result::is_ok),
                    result.stop_producing,
                );
                trace.turns.push(summary);
            }
            assert!(result.diagnostic_failure.is_none());
            self.mode.check_round(&result);
            assert!(result.abort_settlement.is_none());
            if result.stop_producing {
                step.stop_producing();
            }
            if result.root_control_processed.is_some() {
                assert_eq!(
                    result.root_control_processed,
                    Some(AttemptInputId(n(envelope.input.get())))
                );
                self.trace.lock().unwrap().processed.push(envelope.input);
            }
            let mut applied = None;
            if let Some(effect) = result.effect {
                match &effect.body {
                    EffectBody::Port(port)
                        if matches!(
                            &port.body,
                            AttemptToLogicalBody::RootStatus(_)
                                | AttemptToLogicalBody::SealResult { .. }
                        ) =>
                    {
                        if let AttemptToLogicalBody::SealResult {
                            result: Ok(payload),
                            ..
                        } = &port.body
                        {
                            assert!(
                                self.trace
                                    .lock()
                                    .unwrap()
                                    .seal_payload
                                    .replace(payload.clone())
                                    .is_none()
                            );
                        }
                        let author: InputAuthor =
                            if matches!(&port.body, AttemptToLogicalBody::SealResult { .. }) {
                                self.author.lock().unwrap().as_ref().unwrap().clone()
                            } else {
                                step.input_author().cloned().unwrap_or_else(|| {
                                    self.author.lock().unwrap().as_ref().unwrap().clone()
                                })
                            };
                        step.effect_from_author(
                            &author,
                            descriptor(effect.id.get(), author.work(), EffectKind::Reply),
                            FrontendEffect::Attempt(AttemptEffectEnvelope::new(effect)),
                        )
                        .unwrap();
                        match result.selection {
                            InputSelection::Only(source) => step.select_input_source_with_owner(
                                producer(source),
                                producer(self.sources.owner),
                            ),
                            InputSelection::Prefer(source) => step.prefer_input_source_with_owner(
                                producer(source),
                                producer(self.sources.owner),
                            ),
                        }
                        return;
                    }
                    EffectBody::Publish(Publication::ProtocolBatch(_)) => {
                        // Ordinary protocol publication is intentionally
                        // scripted; the Root output above never uses this ACK.
                        applied = Some((
                            effect.id,
                            PublicationApplied::ProtocolBatch { published: true },
                        ));
                    }
                    EffectBody::OpenSubscription(request)
                    | EffectBody::AdjustReadWindow(request) => {
                        self.trace
                            .lock()
                            .unwrap()
                            .last
                            .insert(request.generation, request.clone());
                        self.trace.lock().unwrap().effects.push_back(effect);
                    }
                    EffectBody::Port(port)
                        if matches!(
                            &port.body,
                            AttemptToLogicalBody::ConvergenceProgress { .. }
                        ) =>
                    {
                        // Passive diagnostic output is retained, not applied
                        // as a fabricated LP closure or physical-exit fact.
                        self.trace.lock().unwrap().effects.push_back(effect);
                    }
                    EffectBody::ObserveProcess { .. }
                    | EffectBody::AdmitQueuedInput { .. }
                    | EffectBody::CloseSubscription { .. }
                    | EffectBody::PrepareLocalConvergence
                    | EffectBody::ReleaseCreationPayload { .. }
                    | EffectBody::ConfirmReadState { .. } => {
                        self.trace.lock().unwrap().effects.push_back(effect);
                    }
                    EffectBody::Port(port)
                        if self.mode.failure().is_none()
                            && matches!(&port.body, AttemptToLogicalBody::AttemptTerminal(
                                novarocks_attempt_protocol::port_output::AttemptTerminal::Completed)) =>
                    {
                        // The SAME public Core selected its passive Completed
                        // fact after the original Seal and normal drain. This
                        // grants neither a new Seal nor actual Source exit.
                        self.trace.lock().unwrap().effects.push_back(effect);
                    }
                    EffectBody::Port(port)
                        if self.mode.failure().is_some()
                            && matches!(&port.body, AttemptToLogicalBody::AttemptTerminal(_)) =>
                    {
                        self.trace.lock().unwrap().effects.push_back(effect);
                    }
                    other => {
                        panic!("unexpected real Core effect in observation fixture: {other:?}")
                    }
                }
            }
            let continuation = match (result.selection, applied) {
                (InputSelection::Only(selected), Some((effect, applied))) => {
                    assert_eq!(selected, self.sources.publication);
                    Some((
                        selected,
                        AttemptInput::Executor(ExecutorFact::PublicationApplied {
                            effect,
                            result: Ok(applied),
                        }),
                    ))
                }
                (InputSelection::Only(selected), None) if selected == self.sources.actor => {
                    Some((selected, AttemptInput::Timer))
                }
                _ => None,
            };
            let Some((selected, value)) = continuation else {
                match result.selection {
                    InputSelection::Only(source) => step.select_input_source_with_owner(
                        producer(source),
                        producer(self.sources.owner),
                    ),
                    InputSelection::Prefer(source) => step.prefer_input_source_with_owner(
                        producer(source),
                        producer(self.sources.owner),
                    ),
                }
                return;
            };
            source = selected;
            input = value;
            self.next = self.next.checked_add(1).unwrap();
            id = AttemptInputId(n(self.next));
        }
        panic!("the finite original observation script exceeded its frozen allowance");
    }
    fn next_deadline(&self) -> Option<Tick> {
        self.core
            .next_deadline()
            .map(|at| Tick(u64::try_from(at.since_origin().as_nanos()).unwrap()))
    }
}
struct Logical {
    next: Arc<Mutex<Option<FrontendEffect>>>,
    seen: Arc<Mutex<Vec<novarocks_coordination_contract::logical_port::AttemptToLogical>>>,
    work: ScopedWorkRef,
}
impl TestActor for Logical {
    fn handle(
        &mut self,
        _: Tick,
        _: InputEnvelope,
        input: FrontendInput,
        step: &mut ActorStep<'_, FrontendEffect>,
    ) {
        assert!(step.charge(1));
        match input {
            FrontendInput::Test(_) => step
                .effect(
                    descriptor(1, self.work, EffectKind::Cleanup),
                    self.next.lock().unwrap().take().unwrap(),
                )
                .unwrap(),
            FrontendInput::Logical(input) => {
                let novarocks_logical_execution_protocol::input::LogicalInput::AttemptPort(message) =
                    *input
                else {
                    panic!("exact original AP fact");
                };
                self.seen.lock().unwrap().push(*message);
            }
            _ => panic!("same logical financial author"),
        }
    }
}
struct Metadata(Mutex<Option<OwnedPositionObligation>>);
struct Backing(Option<OwnedPositionObligation>);
impl ObservationMetadataAuthor for Metadata {
    fn retain(
        &self,
        required: ObservationMetadataLayout,
        participants: usize,
    ) -> Result<Box<dyn ObservationMetadataObligation>, ObservationError> {
        assert_eq!((required.total_positions, participants), (3, 1));
        Ok(Box::new(Backing(self.0.lock().unwrap().take())))
    }
}
impl ObservationMetadataObligation for Backing {
    fn retire_after_exit(mut self: Box<Self>) -> Result<(), MetadataRetirementRefusal> {
        self.0.take().unwrap().retire_after_exit().unwrap();
        Ok(())
    }
}
struct Rig {
    ledger: RuntimeLedger,
    scope: ScopeHandle,
    lp_scope: ScopeHandle,
    work: WorkObligation,
    payer: ReservationObligation,
    lp_work: WorkObligation,
    lp_payer: ReservationObligation,
    runtime: ActorRuntime<FrontendActor>,
    ap: ActorRegistration,
    lp: ActorRegistration,
    sources: AttemptSources,
    activation: AttemptActivationIdentity,
    root: TaskIdentity,
    tasks: Vec<TaskIdentity>,
    participation: ParticipationRef,
    author: Arc<Mutex<Option<InputAuthor>>>,
    trace: Arc<Mutex<Trace>>,
    next: Arc<Mutex<Option<FrontendEffect>>>,
    seen: Arc<Mutex<Vec<novarocks_coordination_contract::logical_port::AttemptToLogical>>>,
    trigger: StandingInputObligation<FrontendActor>,
    posted: Vec<CausalInputObligation<FrontendActor>>,
    ingress: Option<OrderedObservationInput<FrontendActor>>,
    progress: Option<ReadProgressInput>,
    progress_id: novarocks_control_runtime::actor_turn::StandingInputId,
    refused: Mutex<Vec<Box<dyn std::any::Any + Send>>>,
    ordered: Option<OrderedObservationHost>,
    roots: Option<Arc<RootControlRequests>>,
    slot: SourcePublishedSlot,
    reader: Option<ControlReaderObligation>,
    facts: Option<Arc<AttemptFactBank>>,
    publisher: Option<Arc<AttemptPublicationOwner>>,
    clock: Arc<ManualClock>,
    logical_action: u64,
}
impl Rig {
    fn new() -> Self {
        Self::with_mode(SeedMode::Recovery)
    }
    fn with_mode(mode: SeedMode) -> Self {
        let graph = crate::task_execution::graph_manifest_tests::unstarted_chain_graph();
        let root = graph.root_identity();
        let participation = graph.task(root.task_id()).unwrap().participation();
        let tasks = graph
            .tasks()
            .map(|task| task.identity())
            .collect::<Vec<_>>();
        assert_eq!(tasks.len(), 3);
        assert!(
            graph
                .tasks()
                .all(|task| task.participation() == participation)
        );
        let plan = novarocks_query_application::api::PlanSeal::Version(
            novarocks_physical_plan::PlanVersionId::try_new([192; 16]).unwrap(),
        );
        let (protocol, _receiver) = AttemptProtocolFactsPublisher::for_graph(
            plan,
            &graph,
            &BTreeSet::new(),
            &novarocks_result_contract::PerKind {
                write_counts: None,
                delete_verification: None,
            },
        )
        .unwrap();
        let mut payload = n(3);
        let (frozen, _creations) = graph.into_attempt_graph(&mut payload).unwrap();
        let mut start = crate::task_execution::host::adapter::tests::start();
        let epoch = *start.plan.admission_epochs.values().next().unwrap();
        let positions = *start.plan.preparation_positions.values().next().unwrap();
        let material = *start.plan.establish_material.values().next().unwrap();
        start.activation.identity =
            AttemptActivationIdentity::from_parts(92, root.query_execution_id(), 1);
        start.plan.authorization_limits.activation = start.activation.identity;
        start.plan.graph = frozen;
        start.plan.establish_material = [(participation, material)].into();
        start.plan.admission_epochs = [(participation.backend_process_id(), epoch)].into();
        start.plan.preparation_positions = [(participation.backend_process_id(), positions)].into();
        // Physical LP source 2 is frozen independently from actual AP actor 1.
        start.sources.owner = AttemptSourceId(n(2));
        start.sources.qa = AttemptSourceId(n(2));
        start.sources.publication = AttemptSourceId(n(3));
        start.sources.observation = AttemptSourceId(n(4));
        start.sources.subscription = AttemptSourceId(n(5));
        start.sources.native = AttemptSourceId(n(6));
        start.sources.transport = AttemptSourceId(n(7));
        start.sources.capacity = AttemptSourceId(n(8));
        start.sources.membership = AttemptSourceId(n(9));
        start.sources.process = AttemptSourceId(n(10));
        let activation = start.activation.identity;
        let sources = start.sources;
        let max_step = start
            .minimum_core_step_units()
            .unwrap()
            .checked_mul(64)
            .unwrap()
            + 2;
        let (core, preissued) = preinstalled_protocol(start, &mode);
        let budget = ObservationBudget {
            queued_frames: 1,
            total_positions: 3,
            max_frame_bytes: 2048,
            control_bytes: 512,
            total_bytes: 3 * 2048 + 512,
        };
        let ledger = RuntimeLedger::new(budget.byte_window().unwrap());
        let generation = Generation::new(1).unwrap();
        let scope = ledger.open_scope_next(None, generation).unwrap();
        let lp_scope = ledger.open_scope_next(None, generation).unwrap();
        let (mut work, mut payer) = scope
            .reserve_next(
                generation,
                ReservationResources::new(
                    budget.byte_window().unwrap(),
                    OwnedSlotNeed {
                        work_headers: 1,
                        inputs: budget.total_positions + 3 + 2 + 2,
                        captures: 1,
                        ..Default::default()
                    },
                ),
                WorkKind::Settlement,
                WorkAdmission::External,
            )
            .unwrap()
            .bind(WorkKind::Settlement)
            .unwrap();
        let (mut lp_work, lp_payer) = lp_scope
            .reserve_next(
                generation,
                ReservationResources::new(
                    Default::default(),
                    OwnedSlotNeed {
                        work_headers: 1,
                        inputs: 5,
                        ..Default::default()
                    },
                ),
                WorkKind::Settlement,
                WorkAdmission::External,
            )
            .unwrap()
            .bind(WorkKind::Settlement)
            .unwrap();
        work.start().unwrap();
        lp_work.start().unwrap();
        let runtime = ActorRuntime::new();
        let trace = Arc::new(Mutex::new(Trace::default()));
        for effect in preissued {
            if let EffectBody::OpenSubscription(request) = &effect.body {
                trace
                    .lock()
                    .unwrap()
                    .last
                    .insert(request.generation, request.clone());
            }
            trace.lock().unwrap().effects.push_back(effect);
        }
        let root_author = Arc::new(Mutex::new(None));
        let ap = runtime
            .register_bound(
                ActorId::new(1).unwrap(),
                scope.clone(),
                FrontendActor::Test(Box::new(CurrentCore {
                    mode,
                    core,
                    sources,
                    next: 1 << 32,
                    author: root_author.clone(),
                    trace: trace.clone(),
                })),
                ActorConfig {
                    mailbox: [16; 4],
                    max_batches: 4,
                    max_effects_per_step: 1,
                    max_dependencies_per_effect: 0,
                    turn_budget: (max_step + 1) * 4,
                    max_step_work: max_step,
                },
            )
            .unwrap_or_else(|_| panic!("current Core on the original AP Scope"));
        let next = Arc::new(Mutex::new(None));
        let seen = Arc::new(Mutex::new(Vec::new()));
        let lp = runtime
            .register_bound(
                ActorId::new(2).unwrap(),
                lp_scope.clone(),
                FrontendActor::Test(Box::new(Logical {
                    next: next.clone(),
                    seen: seen.clone(),
                    work: lp_work.reference(),
                })),
                ActorConfig {
                    mailbox: [8; 4],
                    max_batches: 4,
                    max_effects_per_step: 1,
                    max_dependencies_per_effect: 0,
                    // Logical.handle charges one traversal unit and its
                    // one actual effect emission charges another unit.
                    turn_budget: 12,
                    max_step_work: 2,
                },
            )
            .unwrap_or_else(|_| panic!("same original logical financial author"));
        let binding =
            RootControlBinding::for_registered_source(ap.clone(), activation, root).unwrap();
        let roots = RootControlRequests::new(binding.clone());
        let publisher = AttemptPublicationOwner::new(
            PublicationRecord {
                activation,
                protocol,
                profile: None,
                diagnostic_records: None,
                ready: None,
                #[cfg(novarocks_dev_controls)]
                root_observer: None,
            },
            None,
        );
        publisher.bind_port_root(roots.clone());
        let facts = AttemptFactBank::reserve(
            &runtime,
            &lp,
            &ap,
            activation,
            &lp_work,
            Waker::noop().clone(),
            Waker::noop().clone(),
        )
        .unwrap();
        let trigger = runtime
            .reserve_admitted_input_for(&lp, ap.actor(), InputClass::Control, &lp_work)
            .unwrap()
            .into_standing_input()
            .unwrap();
        let reserve = |class, owner| {
            runtime
                .reserve_admitted_input_for(&ap, producer(sources.observation), class, owner)
                .unwrap()
                .into_standing_input()
                .unwrap()
        };
        let credits =
            ObservationCredits::bind(budget, [participation], &work, &mut payer, None).unwrap();
        let mut capture = payer.take_position(OwnedSlotKind::Capture).unwrap();
        capture.bind().unwrap();
        let progress_role = runtime
            .reserve_admitted_input_for(
                &ap,
                producer(sources.subscription),
                InputClass::Ordinary,
                &work,
            )
            .unwrap()
            .into_standing_input()
            .unwrap();
        let progress_id = progress_role.id();
        let progress = ReadProgressInput::new(&ap, participation, progress_role);
        let pool = BoundObservationSource::new(
            binding.clone(),
            roots.clone(),
            credits,
            [(participation, progress.clone())].into(),
            Arc::new(Metadata(Mutex::new(Some(capture)))),
        );
        let control = runtime
            .reserve_admitted_input_for(&ap, lp.actor(), InputClass::Control, &lp_work)
            .unwrap()
            .into_standing_input()
            .unwrap();
        binding.bind_reply_author(control.author().unwrap());
        *root_author.lock().unwrap() = Some(binding.reply_author().unwrap().clone());
        let clock = Arc::new(ManualClock::default());
        let ingress = OrderedObservationInput::new(
            reserve(InputClass::Lifecycle, &work),
            control,
            pool,
            binding,
            clock.clone(),
            project,
            Waker::noop().clone(),
        )
        .unwrap_or_else(|_| panic!("same ordered Source"));
        roots.bind_ingress(&ingress).unwrap();
        let observer = ConsumerId::new(1).unwrap();
        let (storage, mut storage_payer) = lp_scope
            .reserve_child_next(
                generation,
                lp_payer.id(),
                ReservationResources::new(
                    Default::default(),
                    OwnedSlotNeed {
                        work_headers: 1,
                        completions: 1,
                        captures: 1,
                        ..Default::default()
                    },
                ),
                WorkKind::Settlement,
            )
            .unwrap()
            .bind(WorkKind::Settlement)
            .unwrap();
        let report = ReportSlot::reserve_next(
            &mut storage_payer,
            generation,
            1,
            [(observer, ConsumerKind::Observer)],
        )
        .unwrap();
        let controls = |class| {
            runtime
                .reserve_admitted_input_for(&ap, lp.actor(), class, &work)
                .unwrap()
                .into_standing_input()
                .unwrap()
        };
        let (slot, reader) = SourcePublishedSlot::bind(
            &runtime,
            ControlBinding {
                lp: lp.clone(),
                ap: ap.clone(),
                activation,
                positions: ControlPositions {
                    stop: controls(InputClass::Control),
                    convergence: controls(InputClass::Control),
                    changed: controls(InputClass::Ordinary),
                },
                storage: ReportedWork::try_new(storage, storage_payer, report).unwrap(),
                observer,
                roots: roots.clone(),
            },
        )
        .unwrap_or_else(|e| panic!("same paid reader: {e:?}"));
        let ordered =
            OrderedObservationHost::new(ingress.clone()).with_read_progress([progress.clone()]);
        Self {
            ledger,
            scope,
            lp_scope,
            work,
            payer,
            lp_work,
            lp_payer,
            runtime,
            ap,
            lp,
            sources,
            activation,
            root,
            tasks,
            participation,
            author: root_author,
            trace,
            next,
            seen,
            trigger,
            posted: Vec::new(),
            ingress: Some(ingress),
            progress: Some(progress),
            progress_id,
            refused: Mutex::new(Vec::new()),
            ordered: Some(ordered),
            roots: Some(roots),
            slot,
            reader: Some(reader),
            facts: Some(facts),
            publisher: Some(publisher),
            clock,
            logical_action: 0,
        }
    }
    fn tick(&self) -> Tick {
        Tick(u64::try_from(self.clock.now().since_origin().as_nanos()).unwrap())
    }
    fn advance_original_deadline(&mut self) {
        let at = self
            .trace
            .lock()
            .unwrap()
            .deadline
            .expect("same original Core deadline");
        let now = self.clock.now();
        assert!(at >= now);
        self.clock.advance(at.since_origin() - now.since_origin());
        self.drain(&[]);
    }
    async fn service_confirmations(&mut self) {
        loop {
            let next = {
                let mut trace = self.trace.lock().unwrap();
                trace
                    .effects
                    .iter()
                    .position(|effect| matches!(&effect.body, EffectBody::ConfirmReadState { .. }))
                    .map(|index| trace.effects.remove(index).unwrap())
            };
            let Some(effect) = next else {
                break;
            };
            let EffectBody::ConfirmReadState {
                token,
                participation,
                generation,
                requested_at,
            } = effect.body
            else {
                unreachable!()
            };
            let original = self
                .ingress
                .as_ref()
                .unwrap()
                .borrow_read_confirmation(StreamKey {
                    participation,
                    generation,
                })
                .unwrap();
            let snapshot = original.snapshot(token, requested_at).await.unwrap();
            self.post(
                self.sources.subscription,
                AttemptInput::Executor(ExecutorFact::ReadState(snapshot)),
            );
            drop(original);
        }
    }
    fn sync_source(&mut self, source: &Arc<SubscriptionSource<FrontendActor>>) {
        let updates = {
            let mut trace = self.trace.lock().unwrap();
            let mut updates = Vec::new();
            let mut index = 0;
            while index < trace.effects.len() {
                if matches!(&trace.effects[index].body, EffectBody::AdjustReadWindow(_)) {
                    updates.push(trace.effects.remove(index).unwrap());
                } else {
                    index += 1;
                }
            }
            updates
        };
        for effect in updates {
            let EffectBody::AdjustReadWindow(request) = effect.body else {
                unreachable!()
            };
            if request.generation == n(1) {
                assert_eq!(request.participation, self.participation);
                // The preinstalled metadata-only generation has no Native Source row.
                continue;
            }
            source.adjust_read_window(request).unwrap();
        }
    }
    fn refuse<T: std::any::Any + Send>(&self, original: T) -> ! {
        self.refused.lock().unwrap().push(Box::new(original));
        panic!("the original fixture refusal remains in its whole-run custody");
    }
    fn post(&mut self, source: AttemptSourceId, input: AttemptInput) {
        let original = CausalInputObligation::reserve_admitted(
            &self.runtime,
            &self.ap,
            producer(source),
            crate::task_execution::host::adapter::input_class(&input),
            &self.work,
        )
        .unwrap();
        self.posted.push(original);
        let result = self
            .posted
            .last_mut()
            .unwrap()
            .fill_with_receipt(None, project(input));
        if let Err(original) = result {
            self.refuse(original);
        }
    }
    fn retire_logical_inputs(&mut self) -> bool {
        let mut retired = false;
        while let Some((_, input)) = self
            .runtime
            .committed_batches(self.lp.actor(), None, 8)
            .unwrap()
            .first()
            .copied()
        {
            self.runtime.retire_input(self.lp.actor(), input).unwrap();
            retired = true;
        }
        retired
    }
    fn original_lifecycle_loan(
        &mut self,
    ) -> crate::task_execution::host::port::facts::FactPositionLoanObligation {
        // A later Root publication cannot borrow the SAME one-slot Lifecycle
        // position until the LP really consumes/retires the prior occurrence.
        // Keep the next AP effect Pending: do not take its current ticket yet.
        for _ in 0..16 {
            if let Some(original) = self.facts.as_ref().unwrap().try_loan(FactRole::Lifecycle) {
                return original;
            }
            let timers = self.runtime.advance_time(self.tick(), 16).unwrap();
            let consumed = self.runtime.poll_one(self.tick()).unwrap();
            let retired = self.retire_logical_inputs();
            let progressed = self
                .facts
                .as_ref()
                .unwrap()
                .progress(3, false, false)
                .unwrap()
                .progressed;
            assert!(
                timers != 0 || consumed || retired || progressed,
                "the original full Lifecycle slot needs actual LP input progress"
            );
        }
        panic!("the original Lifecycle position did not return after bounded actual progress");
    }
    fn command(&mut self, body: LogicalToAttemptBody) {
        self.logical_action += 1;
        let command = LogicalToAttempt {
            activation: self.activation,
            body,
        };
        *self.next.lock().unwrap() =
            Some(FrontendEffect::Logical(Arc::new(LogicalEffectEnvelope {
                reservation: self.lp_payer.id(),
                body: LogicalHostEffect::Protocol(Box::new(LogicalEffect {
                    action: LogicalActionId::new(n(self.logical_action)),
                    body: LogicalEffectBody::AttemptPort(Box::new(command.clone())),
                })),
            })));
        let input = self.trigger.fill(FrontendInput::Test(0)).unwrap();
        for _ in 0..8 {
            if self
                .runtime
                .effect_inventory(self.lp.actor(), input, None, 1)
                .is_ok_and(|effects| !effects.is_empty())
            {
                break;
            }
            assert!(self.runtime.poll_one(self.tick()).unwrap());
        }
        let ticket = self
            .runtime
            .take_effect(self.lp.actor(), input, ActionId::new(1).unwrap())
            .unwrap();
        let delivered_state = if matches!(&command.body, LogicalToAttemptBody::Stop { .. }) {
            // Stop fills its SAME original standing input. This is a real
            // handoff, not a snapshot publication or a fake Published fact.
            self.slot
                .deliver_stop(&self.runtime, &ticket, command)
                .unwrap();
            EffectState::Started
        } else {
            self.slot.commit(&self.runtime, &ticket, &command).unwrap();
            EffectState::Published
        };
        assert_eq!(
            self.runtime
                .effect_inventory(self.lp.actor(), input, None, 1)
                .unwrap()[0]
                .1,
            delivered_state
        );
        self.runtime.effect_completed(ticket).unwrap();
        assert_eq!(
            self.runtime
                .effect_inventory(self.lp.actor(), input, None, 1)
                .unwrap()[0]
                .1,
            EffectState::Completed
        );
        assert!(self.trigger.retire_ready_after_exit().unwrap());
        self.reader.as_mut().unwrap().begin_pass();
    }
    fn drain(&mut self, phases: &[&SubscriptionPhasePublisher<FrontendActor>]) {
        for _ in 0..256 {
            // Dispatch only the SAME original clock's already-due kernel
            // timers. poll_one alone does not scan the timer index.
            let mut changed = self.runtime.advance_time(self.tick(), 16).unwrap() != 0;
            let process = {
                let mut trace = self.trace.lock().unwrap();
                trace
                    .effects
                    .iter()
                    .position(|effect| matches!(&effect.body, EffectBody::ObserveProcess { .. }))
                    .map(|index| trace.effects.remove(index).unwrap())
            };
            if let Some(effect) = process {
                let EffectBody::ObserveProcess {
                    request,
                    participation,
                } = effect.body
                else {
                    unreachable!("selected the same original process request")
                };
                assert_eq!(participation, self.participation);
                // This finite protocol component owns no membership sampler.
                // Explicitly return Unobserved for this SAME original request;
                // no Current/replacement/closure or actual process exit is authored.
                self.post(
                    self.sources.process,
                    AttemptInput::Executor(ExecutorFact::ProcessSnapshot {
                        request,
                        participation,
                        sampled_at: self.clock.now(),
                        observation:
                            novarocks_attempt_protocol::input::MembershipObservation::Unobserved,
                    }),
                );
                changed = true;
            }
            if self
                .reader
                .as_mut()
                .unwrap()
                .progress_ordered(5)
                .unwrap()
                .progressed
            {
                changed = true;
            }
            if self.runtime.poll_one(self.tick()).unwrap() {
                changed = true;
            }
            let batches = self
                .runtime
                .committed_batches(self.ap.actor(), None, 16)
                .unwrap();
            for (_, input) in batches {
                self.ordered
                    .as_mut()
                    .unwrap()
                    .index_committed(&self.runtime, &[input])
                    .unwrap();
                self.reader
                    .as_mut()
                    .unwrap()
                    .index_committed(&self.runtime, &[input])
                    .unwrap();
                let phase = phases
                    .iter()
                    .find(|phase| phase.current_input().unwrap() == Some(input));
                for (descriptor, _) in self
                    .runtime
                    .effect_inventory(self.ap.actor(), input, None, 4)
                    .unwrap()
                {
                    let mut loan = self.original_lifecycle_loan();
                    let ticket = self
                        .runtime
                        .take_effect(self.ap.actor(), input, descriptor.id)
                        .unwrap();
                    let FrontendEffect::Attempt(effect) = &ticket.effect else {
                        panic!("real AP output");
                    };
                    let EffectBody::Port(port) = &effect.protocol.body else {
                        panic!("real Root port");
                    };
                    let original = effect.clone();
                    let projected =
                        novarocks_attempt_protocol::port_projection::project_attempt_message(
                            port,
                            &mut |_| None,
                            &mut |_, _| None,
                            &mut |_, _, _| None,
                        )
                        .unwrap();
                    let ack = if matches!(&port.body, AttemptToLogicalBody::RootStatus(_)) {
                        let receipt = loan
                            .publish_root_status_now(&self.runtime, &ticket, projected)
                            .unwrap();
                        Some(
                            self.publisher
                                .as_ref()
                                .unwrap()
                                .acknowledge_root_fact(&original, receipt)
                                .0,
                        )
                    } else {
                        let handoff = loan
                            .publish_root_reply_now(&self.runtime, &ticket, projected.clone())
                            .unwrap();
                        assert_eq!(self.seal_count(), 0, "handoff is not LP consumption");
                        let receipt = self
                            .roots
                            .as_ref()
                            .unwrap()
                            .acknowledge_fact_handoff(handoff)
                            .unwrap_or_else(|original| self.refuse(original));
                        assert!(
                            !self
                                .roots
                                .as_ref()
                                .unwrap()
                                .reply_delivered(&projected)
                                .unwrap(),
                            "the same row acknowledges once"
                        );
                        self.publisher
                            .as_ref()
                            .unwrap()
                            .acknowledge_root_reply(&original, receipt)
                            .unwrap()
                            .map(|p| p.0)
                    };
                    drop(loan);
                    self.runtime.effect_completed(ticket).unwrap();
                    if let Some(ack) = ack {
                        self.post(self.sources.publication, ack);
                    }
                }
                if self.trace.lock().unwrap().processed.contains(&input) {
                    self.ordered
                        .as_mut()
                        .unwrap()
                        .root_control_processed(input)
                        .unwrap();
                }
                let role = self
                    .runtime
                    .input_custody(self.ap.actor(), input)
                    .unwrap()
                    .and_then(|custody| custody.standing_input_id);
                let ordered = role.is_some_and(|role| {
                    self.ingress
                        .as_ref()
                        .unwrap()
                        .standing_ids()
                        .contains(&role)
                });
                let progress = role == Some(self.progress_id);
                if !ordered && !progress {
                    // The reader retains its OWN coalescing occurrence until
                    // progress_ordered. Other exact roles retire here.
                    let is_reader = role.is_some_and(|id| {
                        !phases.iter().any(|p| p.role_id() == id)
                            && !self
                                .posted
                                .iter()
                                .any(|p| p.in_flight().unwrap().contains(&Some(input)))
                    });
                    if !is_reader {
                        self.runtime.retire_input(self.ap.actor(), input).unwrap();
                        if let Some(phase) = phase {
                            phase.input_retired(input).unwrap();
                        }
                    }
                }
                self.ordered
                    .as_mut()
                    .unwrap()
                    .retire_ready(&self.runtime)
                    .unwrap();
                self.ordered
                    .as_mut()
                    .unwrap()
                    .retire_read_progress(&mut 1)
                    .unwrap();
            }
            for posted in &mut self.posted {
                posted.retire_ready_after_exit().unwrap();
            }
            self.posted.retain_mut(|p| {
                if p.in_flight().unwrap().iter().any(Option::is_some) {
                    true
                } else {
                    p.close_after_exit().unwrap();
                    false
                }
            });
            self.retire_logical_inputs();
            self.facts
                .as_ref()
                .unwrap()
                .progress(3, false, false)
                .unwrap();
            if !changed {
                break;
            }
        }
    }
}
// Before ANY actual Actor/Scope/Native job is bound, establish the original
// declared pure-protocol prefix through public Core inputs and emitted actions.
// These reservation/grant references are metadata stubs, like strict_tail's
// pure fixture. None is ever presented to Native IO, WC, CR or the later actor.
fn preinstalled_protocol(
    start: novarocks_attempt_protocol::start::AttemptStart,
    mode: &SeedMode,
) -> (Core, Vec<AttemptEffect>) {
    use novarocks_attempt_protocol::{
        capacity::ReservationToken,
        effect::{OperationIntent, TaskDomainIntent},
        input::{
            AckPayload, OperationObservation, OperationReply, OperationReplyResult,
            QueuedInputGrantRef, SendGeneration, SubscriptionPhaseSnapshot,
        },
        port_output::AttemptAuthorization,
    };
    use novarocks_coordination_contract::{AttemptAuthorizationDecision, AttemptAuthorizationRef};
    use novarocks_execution_contract::{
        AdmissionTicketId, CreateTaskReceipt, DomainProgression, LeaseReceipt, OperationKind,
        OperationOutcome, ParticipationAdmissionTicketReceipt, ParticipationReceipt,
        TaskDomainReceipt, UpdateTaskReceipt, operation::OperationResult,
    };
    struct ScriptUnits {
        remaining: usize,
        next: u64,
    }
    impl StepContext for ScriptUnits {
        fn charge(&mut self, n: usize) -> bool {
            if n > self.remaining {
                return false;
            }
            self.remaining -= n;
            true
        }
        fn reserve_queue_stage(
            &mut self,
            _: DispatchLane,
            _: bool,
            _: novarocks_types::BackendProcessId,
            records: usize,
            _: usize,
        ) -> QueueStage {
            assert!(records > 0);
            self.next += 1;
            QueueStage::Granted(ReservationToken(n(self.next)))
        }
    }
    let sources = start.sources;
    let activation = start.activation.identity;
    let required = start.plan.graph.tasks.len();
    let root = start.plan.graph.root;
    let tasks = start
        .plan
        .graph
        .tasks
        .values()
        .map(|task| task.identity)
        .collect::<Vec<_>>();
    let participation = start.plan.graph.tasks[&root.task_id()].participation;
    let leaf = *tasks.iter().find(|task| **task != root).unwrap();
    let mut core = Core::new(start).unwrap();
    // Pure metadata from the same public Core output, before any actual
    // Actor, Work or Native source exists. This grants no physical capacity.
    let establish_metadata = |operation: &OperationIntent| {
        let OperationIntent::EstablishParticipation {
            envelope,
            participation,
            ticket,
            initial_lease_sequence,
            initial_lease_valid_for,
            material,
            queued_bytes,
        } = operation
        else {
            panic!("the original Establish authorization retains its exact operation");
        };
        (
            *envelope,
            *participation,
            *ticket,
            *initial_lease_sequence,
            *initial_lease_valid_for,
            *material,
            *queued_bytes,
        )
    };
    let mut original_establishes = BTreeMap::new();
    let mut units = ScriptUnits {
        remaining: 4096,
        next: 100_000,
    };
    let mut next = 1u64;
    let mut grant = 200_000u64;
    let mut created = BTreeSet::new();
    let mut original_open = None;
    let mut next_open = None;
    let mut generations = BTreeMap::new();
    let mut retained = Vec::new();
    let mut closing = None;
    let mut prefix = false;
    let mut initial_closed = false;
    let mut pending = VecDeque::from([
        (sources.actor, AttemptInput::Bootstrap),
        (sources.actor, AttemptInput::StartProtocol),
    ]);
    let mut selection = InputSelection::Only(sources.actor);
    for _ in 0..4096 {
        let chosen = match selection {
            InputSelection::Only(source) => pending
                .iter()
                .position(|(candidate, _)| *candidate == source),
            InputSelection::Prefer(source) => pending
                .iter()
                .position(|(candidate, _)| *candidate == source)
                .or_else(|| (!pending.is_empty()).then_some(0)),
        };
        let (source, input) = if let Some(index) = chosen {
            pending.remove(index).unwrap()
        } else if matches!(selection,InputSelection::Only(source) if source==sources.actor)
            || core
                .next_deadline()
                .is_some_and(|at| at == MonotonicInstant::ORIGIN)
        {
            (sources.actor, AttemptInput::Timer)
        } else if pending.is_empty() && created.len() == required && original_open.is_some() {
            if !prefix && mode.failure().is_some() {
                use novarocks_execution_contract::{AbortCause, AbortParticipation};
                let mut logical_ids = novarocks_coordination_contract::OperationIds::new(
                    novarocks_coordination_contract::IdSeed::new(1_000_000, 100),
                );
                let request = AbortParticipation::new(
                    logical_ids.issue(),
                    participation,
                    AbortCause::QueryFailed,
                );
                let action = AttemptActionId::new(n(500_001));
                closing = Some((action, request));
                pending.push_back((
                    sources.owner,
                    AttemptInput::Port(LogicalToAttempt {
                        activation,
                        body: LogicalToAttemptBody::Abort { action, request },
                    }),
                ));
                pending.push_back((sources.owner,AttemptInput::Port(LogicalToAttempt {activation,
                    body:LogicalToAttemptBody::Stop {reason:novarocks_coordination_contract::AttemptStopReason::LogicalExecutionFailed,
                        failure:mode.failure().unwrap().clone()}})));
                prefix = true;
                continue;
            }
            if mode.failure().is_some() {
                assert!(retained.iter().any(|effect:&AttemptEffect|matches!(&effect.body,
                    EffectBody::Port(port) if matches!(&port.body,
                        AttemptToLogicalBody::AbortSettlement {
                            settlement:novarocks_attempt_protocol::port_output::AbortIssueSettlement::Worker(OperationOutcome::Accepted),..}))),
                    "the original exact closing ACK must be applied before binding");
                assert!(
                    retained.iter().all(|effect| !matches!(
                        &effect.body,
                        EffectBody::CloseSubscription { .. }
                    )),
                    "closing ACK/terminal cleanup cannot stop the required residual observation"
                );
                return (core, retained);
            }
            if !prefix {
                use novarocks_attempt_protocol::input::{ObservationBatch, ObservationOpening};
                let facts = tasks
                    .iter()
                    .filter(|task| **task != leaf)
                    .map(|task| {
                        ObservationFact::Status(
                            TaskStatus::try_new(
                                *task,
                                TaskStatusVersion::new(4).unwrap(),
                                TaskState::Finished,
                                None,
                                TaskOutputFacts::new(true),
                            )
                            .unwrap(),
                        )
                    })
                    .chain([
                        ObservationFact::Gone(leaf),
                        ObservationFact::CatchUpComplete {
                            generation: 1,
                            initial_cut: 1,
                        },
                    ]);
                let items = facts
                    .map(|fact| ReceivedObservation {
                        participation,
                        generation: n(1),
                        received_at: MonotonicInstant::ORIGIN,
                        opening: ObservationOpening {
                            started_at: MonotonicInstant::ORIGIN,
                            opened_at: Some(MonotonicInstant::ORIGIN),
                        },
                        event: ObservationEvent::Fact {
                            source_revision: None,
                            fact,
                        },
                    })
                    .collect();
                pending.push_back((
                    sources.observation,
                    AttemptInput::Executor(ExecutorFact::Observations(
                        ObservationBatch::new(items).unwrap(),
                    )),
                ));
                prefix = true;
                continue;
            }
            assert!(
                initial_closed,
                "the metadata-only prefix must settle its own virtual generation"
            );
            retained.extend(next_open);
            return (core, retained);
        } else {
            panic!("missing exact scripted preinstallation input: {selection:?}");
        };
        if matches!(&input,AttemptInput::Executor(ExecutorFact::SubscriptionExited {generation,..}) if *generation==n(1))
        {
            assert!(!std::mem::replace(&mut initial_closed, true));
        }
        if let AttemptInput::Executor(ExecutorFact::Operation(observation)) = &input
            && observation.reply.kind == OperationKind::CreateTask
        {
            let OperationReplyResult::Worker(OperationResult::Verdict {
                acknowledgement: Some(AckPayload::Create(receipt)),
                ..
            }) = &observation.reply.result
            else {
                panic!("same original Create receipt");
            };
            assert!(created.insert(receipt.identity()));
        }
        let id = AttemptInputId(n(next));
        next += 1;
        units.remaining = 4096;
        let result = core
            .handle(
                MonotonicInstant::ORIGIN,
                AttemptInputClaim {
                    source,
                    input: id,
                    native_cut: Some(AttemptSourceCut {
                        source: sources.native,
                        tail: (source == sources.native).then_some(id),
                    }),
                    selected_cut: Some(AttemptSourceCut {
                        source: sources.observation,
                        tail: (source == sources.observation).then_some(id),
                    }),
                },
                input,
                &mut units,
            )
            .unwrap();
        if mode.failure().is_none() {
            assert!(!result.stop_producing);
        }
        mode.check_round(&result);
        if let Some(effect) = result.abort_settlement {
            let Some((action, request)) = closing else {
                panic!("no preinstalled actor Abort exists");
            };
            assert!(
                matches!(&effect.body,EffectBody::Port(port) if matches!(&port.body,
                AttemptToLogicalBody::AbortSettlement {abort_action,operation_id,participation:exact,
                    receipt_generation:Some(_),settlement:novarocks_attempt_protocol::port_output::AbortIssueSettlement::Worker(OperationOutcome::Accepted),..}
                    if *abort_action==action&&*operation_id==request.envelope().operation_id()&&*exact==participation))
            );
            retained.push(effect);
        }
        selection = result.selection;
        let Some(effect) = result.effect else {
            continue;
        };
        let mut post = |source, input| pending.push_back((source, input));
        match effect.body {
            EffectBody::EstablishAuthorization { operation, request } => {
                assert_eq!(request.activation, activation);
                assert_eq!(request.action, effect.id);
                let AttemptToLogicalBody::AuthorizationRequest {
                    token,
                    request:
                        AttemptAuthorization::Establish {
                            participation: exact,
                            operation_id,
                        },
                } = request.body
                else {
                    panic!("same public Establish authorization request");
                };
                let original = establish_metadata(&operation);
                assert_eq!(exact, participation);
                assert_eq!(original.1, exact);
                assert_eq!(operation.operation_id(), operation_id);
                let authorization = AttemptAuthorizationRef::new(n(next + 300_000));
                assert!(
                    original_establishes
                        .insert(operation_id, (original, authorization))
                        .is_none(),
                    "the first Establish has exactly one original authorization"
                );
                post(
                    sources.qa,
                    AttemptInput::Port(LogicalToAttempt {
                        activation,
                        body: LogicalToAttemptBody::AuthorizationReply {
                            token,
                            decision: Ok(AttemptAuthorizationDecision::EstablishAuthorized(
                                authorization,
                            )),
                        },
                    }),
                );
            }
            EffectBody::Publish(Publication::ProtocolBatch(_)) => post(
                sources.publication,
                AttemptInput::Executor(ExecutorFact::PublicationApplied {
                    effect: effect.id,
                    result: Ok(PublicationApplied::ProtocolBatch { published: true }),
                }),
            ),
            EffectBody::Port(port)
                if mode.failure().is_some()
                    && matches!(
                        &port.body,
                        AttemptToLogicalBody::AttemptTerminal(_)
                            | AttemptToLogicalBody::ConvergenceProgress { .. }
                    ) =>
            {
                retained.push(AttemptEffect {
                    id: effect.id,
                    class: effect.class,
                    body: EffectBody::Port(port),
                });
            }
            EffectBody::Port(port) => match port.body {
                AttemptToLogicalBody::AuthorizationRequest { token, request } => {
                    let decision = match request {
                        AttemptAuthorization::BeginAdmission(_) => {
                            AttemptAuthorizationDecision::AdmissionStarted(
                                AttemptAuthorizationRef::new(n(next + 300_000)),
                            )
                        }
                        AttemptAuthorization::SettleAdmission {
                            outcome, receipt, ..
                        } => {
                            assert_eq!(outcome, Some(OperationOutcome::Accepted));
                            assert!(receipt.is_some());
                            AttemptAuthorizationDecision::AdmissionGranted
                        }
                        other => panic!("unexpected pristine preinstall authorization: {other:?}"),
                    };
                    post(
                        sources.qa,
                        AttemptInput::Port(LogicalToAttempt {
                            activation,
                            body: LogicalToAttemptBody::AuthorizationReply {
                                token,
                                decision: Ok(decision),
                            },
                        }),
                    );
                }
                AttemptToLogicalBody::EstablishSettlement {
                    token,
                    authorization,
                    participation: exact,
                    operation_id,
                    ..
                } => {
                    let original = original_establishes
                        .get(&operation_id)
                        .expect("settlement belongs to the original authorized Establish");
                    assert_eq!(exact, original.0.1);
                    assert_eq!(authorization, original.1);
                    post(
                        sources.qa,
                        AttemptInput::Port(LogicalToAttempt {
                            activation,
                            body: LogicalToAttemptBody::EstablishSettlementReply {
                                token,
                                authorization,
                                result: Ok(()),
                            },
                        }),
                    );
                }
                AttemptToLogicalBody::RootStatus(_) => post(
                    sources.publication,
                    AttemptInput::Executor(ExecutorFact::PublicationApplied {
                        effect: effect.id,
                        result: Ok(PublicationApplied::RootStatus),
                    }),
                ),
                other => panic!("unexpected pristine preinstallation port: {other:?}"),
            },
            EffectBody::AdmitQueuedInput {
                operation,
                generation,
                ..
            } => {
                if matches!(&operation, OperationIntent::EstablishParticipation { .. }) {
                    assert_eq!(
                        establish_metadata(&operation),
                        original_establishes
                            .get(&operation.operation_id())
                            .expect("WC metadata follows the same authorized Establish")
                            .0
                    );
                }
                grant += 1;
                let original =
                    QueuedInputGrantRef::new(n(grant), generation, operation.queued_bytes());
                assert!(
                    generations
                        .insert(operation.operation_id(), generation)
                        .is_none()
                );
                post(
                    sources.transport,
                    AttemptInput::Executor(ExecutorFact::QueuedInputGranted {
                        operation: operation.operation_id(),
                        grant: original,
                        queued_bytes: operation.queued_bytes(),
                    }),
                );
            }
            EffectBody::SendBatch { batch, .. } => {
                post(
                    sources.native,
                    AttemptInput::Executor(ExecutorFact::SendStarted {
                        effect: effect.id,
                        started_at: MonotonicInstant::ORIGIN,
                    }),
                );
                for operation in batch.operations.iter() {
                    let payload = match operation {
                        OperationIntent::AcquireParticipationAdmissionTicket(request) => {
                            AckPayload::AdmissionTicket(ParticipationAdmissionTicketReceipt::new(
                                AdmissionTicketId::try_from_bytes([0x63; 16]).unwrap(),
                                request.participation(),
                                request.valid_for(),
                            ))
                        }
                        OperationIntent::EstablishParticipation {
                            participation,
                            initial_lease_sequence,
                            initial_lease_valid_for,
                            ..
                        } => {
                            assert_eq!(
                                establish_metadata(operation),
                                original_establishes
                                    .get(&operation.operation_id())
                                    .expect("dispatch retains the same authorized Establish")
                                    .0
                            );
                            post(
                                sources.native,
                                AttemptInput::Executor(ExecutorFact::EstablishRpcStarted {
                                    effect: effect.id,
                                    operation: operation.operation_id(),
                                    participation: *participation,
                                    started_at: MonotonicInstant::ORIGIN,
                                }),
                            );
                            AckPayload::Participation(
                                ParticipationReceipt::new(
                                    *participation,
                                    ParticipationState::Active,
                                )
                                .with_lease(LeaseReceipt::new(
                                    *initial_lease_sequence,
                                    *initial_lease_valid_for,
                                    initial_lease_valid_for.get(),
                                )),
                            )
                        }
                        OperationIntent::CreateTask { identity, .. } => {
                            AckPayload::Create(Box::new(CreateTaskReceipt::new(
                                *identity,
                                Vec::new(),
                                TaskStatus::created(*identity).with_installed(),
                            )))
                        }
                        OperationIntent::UpdateTask {
                            identity, domains, ..
                        } => {
                            let receipts=domains.iter().map(|domain|match domain {
                                TaskDomainIntent::OpenExchangeEdges {version,edges}=>TaskDomainReceipt::OpenExchangeEdges {
                                    accepted_version:*version,opened:edges.clone(),progression:DomainProgression::Apply},
                                TaskDomainIntent::CloseExchangeDestination {version,..}=>TaskDomainReceipt::CloseExchangeDestination {
                                    accepted_version:*version,progression:DomainProgression::Apply},
                                other=>panic!("no split or dynamic input exists in the frozen preinstalled prefix: {other:?}"),
                            }).collect();
                            AckPayload::Update(UpdateTaskReceipt::new(*identity, receipts))
                        }
                        OperationIntent::CancelTask(request) => {
                            assert!(mode.failure().is_none());
                            assert!(created.contains(&request.identity()));
                            assert_ne!(request.identity(), root);
                            assert_eq!(request.reason(), CancelReason::UpstreamNoLongerNeeded);
                            // Exact pure-protocol ACK only: the original
                            // Task terminal still requires its observation.
                            AckPayload::None
                        }
                        OperationIntent::AbortParticipation(request) => {
                            let Some((_, original)) = closing else {
                                panic!("no original logical Abort");
                            };
                            assert_eq!(request.envelope(), original.envelope());
                            AckPayload::Participation(ParticipationReceipt::new(
                                participation,
                                ParticipationState::Aborting,
                            ))
                        }
                        other => panic!("unexpected pristine Native protocol operand: {other:?}"),
                    };
                    let generation: SendGeneration =
                        *generations.get(&operation.operation_id()).unwrap();
                    post(
                        sources.native,
                        AttemptInput::Executor(ExecutorFact::Operation(OperationObservation {
                            generation,
                            reply: OperationReply {
                                operation_id: operation.operation_id(),
                                kind: operation.kind(),
                                result: OperationReplyResult::Worker(OperationResult::Verdict {
                                    outcome: OperationOutcome::Accepted,
                                    detail: None,
                                    acknowledgement: match payload {
                                        AckPayload::None => None,
                                        original => Some(original),
                                    },
                                }),
                            },
                        })),
                    );
                }
            }
            EffectBody::OpenSubscription(request) => {
                if request.generation == n(1) {
                    assert!(original_open.replace(effect.id).is_none());
                    if mode.failure().is_some() {
                        // Preserve the SAME original Open1 for the independently
                        // paid actual Source. No physical phase/exit is scripted.
                        retained.push(AttemptEffect {
                            id: effect.id,
                            class: effect.class,
                            body: EffectBody::OpenSubscription(request),
                        });
                        continue;
                    }
                    post(
                        sources.subscription,
                        AttemptInput::Executor(ExecutorFact::SubscriptionProgress(
                            SubscriptionPhaseSnapshot {
                                participation: request.participation,
                                generation: request.generation,
                                opening_started_at: MonotonicInstant::ORIGIN,
                                opened_at: Some(MonotonicInstant::ORIGIN),
                            },
                        )),
                    );
                } else {
                    assert_eq!(request.generation, n(2));
                    assert!(
                        next_open
                            .replace(AttemptEffect {
                                id: effect.id,
                                class: effect.class,
                                body: EffectBody::OpenSubscription(request)
                            })
                            .is_none()
                    );
                }
            }
            EffectBody::CloseSubscription {
                participation: exact,
                generation,
            } => {
                assert!(
                    mode.failure().is_none(),
                    "terminal Core requested Close before actual participation proof"
                );
                assert_eq!(exact, participation);
                assert_eq!(generation, n(1));
                // Pure virtual-prefix input only: there is no actual Native
                // job, Work, Scope or Join to claim here, before any binding.
                post(
                    sources.subscription,
                    AttemptInput::Executor(ExecutorFact::SubscriptionExited {
                        effect: original_open.unwrap(),
                        participation,
                        generation,
                    }),
                );
            }
            EffectBody::PrepareLocalConvergence if mode.failure().is_some() => {}
            EffectBody::AdjustReadWindow(_) | EffectBody::ReleaseCreationPayload { .. } => {}
            other => panic!("unexpected pristine preinstalled effect: {other:?}"),
        }
    }
    panic!("the original finite preinstalled public protocol prefix did not settle");
}

struct Original {
    effect: AttemptActionId,
    request: Arc<SubscriptionRequest>,
    role: OriginalTransportJoinObligation,
    phase: Arc<SubscriptionPhasePublisher<FrontendActor>>,
    header: WorkRetirementObserver,
    panic: PanicSink,
    failure: Arc<SubscriptionFailureRecord>,
    result: Arc<Mutex<Option<SubscriptionEnd>>>,
}
impl Rig {
    fn start(
        &self,
        source: &Arc<SubscriptionSource<FrontendActor>>,
        effect: AttemptActionId,
        request: Arc<SubscriptionRequest>,
        client: Client,
    ) -> Original {
        let phase = SubscriptionPhasePublisher::new(
            self.runtime
                .reserve_admitted_input_for(
                    &self.ap,
                    producer(self.sources.subscription),
                    InputClass::Lifecycle,
                    &self.work,
                )
                .unwrap()
                .into_standing_input()
                .unwrap(),
            StreamKey {
                participation: request.participation,
                generation: request.generation,
            },
            project,
        );
        let (work, mut payer) = self
            .scope
            .reserve_next(
                Generation::new(1).unwrap(),
                ReservationResources::new(
                    Default::default(),
                    OwnedSlotNeed {
                        work_headers: 1,
                        completions: 1,
                        captures: 1,
                        ..Default::default()
                    },
                ),
                WorkKind::Cleanup,
                WorkAdmission::Derived(self.work.id()),
            )
            .unwrap()
            .bind(WorkKind::Cleanup)
            .unwrap();
        let header = work.retirement_observer();
        let observer = ConsumerId::new(1).unwrap();
        let report = ReportSlot::reserve_next(
            &mut payer,
            Generation::new(1).unwrap(),
            1,
            [(observer, ConsumerKind::Observer)],
        )
        .unwrap();
        let generation = source.prepare(request.clone(), &phase).unwrap();
        let panic = PanicSink::new();
        let panic_in = panic.clone();
        let (role, exit, handoff) = OriginalTransportJoinObligation::prepare(
            ReportedWork::try_new(work, payer, report).unwrap(),
            observer,
            panic.clone(),
        )
        .unwrap_or_else(|e| panic!("same original Native Work: {}", e.error));
        assert!(handoff.owns_exit(&exit));
        let failure = SubscriptionFailureRecord::new(
            effect,
            StreamKey {
                participation: request.participation,
                generation: request.generation,
            },
        );
        let failed = failure.clone();
        let result = Arc::new(Mutex::new(None));
        let out = result.clone();
        handoff
            .spawn(&Handle::current(), exit, move |exit, _| {
                let running = run_future(exit, generation.run_native(client, failed), panic_in);
                async move {
                    let Ok(Ok(end)) = running.await else {
                        panic!("actual Native generation");
                    };
                    *out.lock().unwrap() = Some(end);
                }
            })
            .unwrap();
        Original {
            effect,
            request,
            role,
            phase,
            header,
            panic,
            failure,
            result,
        }
    }
    fn take_open(
        &mut self,
        generation: NonZeroU64,
    ) -> Option<(AttemptActionId, Arc<SubscriptionRequest>)> {
        let mut trace = self.trace.lock().unwrap();
        let index = trace.effects.iter().position(|effect| {
            matches!(&effect.body,
            EffectBody::OpenSubscription(request) if request.generation==generation)
        })?;
        let effect = trace.effects.remove(index).unwrap();
        let EffectBody::OpenSubscription(request) = effect.body else {
            unreachable!()
        };
        Some((effect.id, request))
    }
    fn seal_count(&self) -> usize {
        self.seen
            .lock()
            .unwrap()
            .iter()
            .filter(|message| {
                matches!(&message.body,
            novarocks_coordination_contract::logical_port::AttemptToLogicalBody::SealResult {..})
            })
            .count()
    }
    fn finish(&mut self) {
        self.drain(&[]);
        assert!(self.posted.is_empty());
        self.runtime.close_timer_source(self.ap.actor()).unwrap();
        self.slot.close_publication();
        drop(self.author.lock().unwrap().take());
        assert!(
            self.reader
                .as_mut()
                .unwrap()
                .close_after_exit(&self.runtime)
                .unwrap()
        );
        self.facts.as_ref().unwrap().close_admission();
        self.facts.as_ref().unwrap().original_producer_exited();
        assert!(
            self.facts
                .as_ref()
                .unwrap()
                .progress(3, true, false)
                .unwrap()
                .complete
        );
        assert!(self.facts.as_ref().unwrap().is_closed());
        self.trigger.close_after_exit().unwrap();
        assert!(self.ordered.as_mut().unwrap().ready_for_close());
        let original_progress = self
            .runtime
            .standing_input_inventory(self.ap.actor(), None, 32)
            .unwrap()
            .into_iter()
            .find(|row| row.standing_input_id == self.progress_id)
            .expect("the original ingress still owns its ReadProgress standing");
        assert!(
            original_progress.in_flight.is_none(),
            "the original ReadProgress occurrence already consumed and retired"
        );
        // SourceMetadata owns the original progress map. Its actual ingress
        // retirement closes this SAME role once; a fixture alias cannot close
        // it first and make that owner hit UnknownStandingInput.
        self.ingress.as_ref().unwrap().close_after_exit().unwrap();
        assert!(
            !self
                .runtime
                .standing_input_inventory(self.ap.actor(), None, 32)
                .unwrap()
                .iter()
                .any(|row| row.standing_input_id == self.progress_id),
            "the original ingress really closed the same ReadProgress standing"
        );
        drop((
            self.ordered.take(),
            self.ingress.take(),
            self.progress.take(),
            self.roots.take(),
            self.reader.take(),
            self.facts.take(),
            self.publisher.take(),
        ));
        for (work, payer, owner) in [
            (&mut self.work, &mut self.payer, &self.scope),
            (&mut self.lp_work, &mut self.lp_payer, &self.lp_scope),
        ] {
            let header = work.retirement_observer();
            work.body_exited().unwrap();
            work.captures_exited().unwrap();
            payer.finish().unwrap();
            owner.close_external().unwrap();
            self.ledger.reap_scope_ready(owner.id(), 32).unwrap();
            assert!(header.is_retired());
        }
        while let Some(turn) = self.runtime.claim_host_actor() {
            turn.release(false).unwrap();
        }
        self.runtime.remove_actor(self.ap.actor()).unwrap();
        self.runtime.remove_actor(self.lp.actor()).unwrap();
        self.ledger.remove_scope(self.scope.id()).unwrap();
        self.ledger.remove_scope(self.lp_scope.id()).unwrap();
        assert!(self.runtime.is_empty() && self.ledger.is_empty());
    }
}
fn frame(
    fact: CoveredStatusStreamFact,
    revision: Option<u64>,
) -> novarocks_proto_models::novarocks::TaskStatusStreamEvent {
    encode_covered_status_event(&CoveredStatusStreamEvent {
        fact,
        source_revision: revision,
    })
    .unwrap()
}
async fn drive_until(
    rig: &mut Rig,
    source: &Arc<SubscriptionSource<FrontendActor>>,
    phases: &[&SubscriptionPhasePublisher<FrontendActor>],
    ready: impl Fn(&Rig) -> bool,
) {
    tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            rig.drain(phases);
            rig.service_confirmations().await;
            rig.sync_source(source);
            if ready(rig) {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("the same original current AP advances");
}
async fn settle_original(
    rig: &mut Rig,
    source: &Arc<SubscriptionSource<FrontendActor>>,
    original: &mut Original,
) {
    let join = original_transport_join_id(&original.role).unwrap();
    let failure = tokio::time::timeout(
        Duration::from_secs(3),
        poll_fn(|cx| original.role.poll_shared_completion(cx)),
    )
    .await
    .unwrap()
    .unwrap();
    assert!(failure.is_none());
    assert_eq!(original_transport_join_id(&original.role), Some(join));
    // Actual Join has stopped the only phase producer. Flush its original
    // final coalesced snapshot, then require that SAME occurrence to be
    // consumed and retired before the normal phase/stream close.
    match original.phase.close_after_exit() {
        Ok(()) => {}
        Err(crate::task_execution::host::observations::ObservationError::Busy) => {
            let input = original
                .phase
                .current_input()
                .unwrap()
                .expect("the original final phase has an actual occurrence");
            let custody = rig
                .runtime
                .input_custody(rig.ap.actor(), input)
                .unwrap()
                .unwrap();
            assert_eq!(custody.standing_input_id, Some(original.phase.role_id()));
            rig.drain(&[&original.phase]);
            assert!(
                rig.trace.lock().unwrap().consumed.contains(&input),
                "the same AP consumed its actual final phase"
            );
            assert!(
                rig.runtime
                    .input_custody(rig.ap.actor(), input)
                    .unwrap()
                    .is_none(),
                "the exact original final phase input retired"
            );
            assert!(original.phase.current_input().unwrap().is_none());
            original.phase.close_after_exit().unwrap();
        }
        Err(error) => panic!("original joined phase close refused: {error:?}"),
    }
    rig.drain(&[&original.phase]);
    rig.service_confirmations().await;
    rig.drain(&[&original.phase]);
    rig.sync_source(source);
    assert!(!original.panic.has_panic());
    assert!(original.role.take_join_failure().is_none());
    assert!(
        original
            .failure
            .take(original.failure.reference())
            .is_none()
    );
    assert!(original.role.try_retire().unwrap());
    assert!(rig.scope.reap_observed_header(&original.header).unwrap());
    let applied = source.reap_generation(original.request.generation);
    if applied.is_err() {
        let key = StreamKey {
            participation: original.request.participation,
            generation: original.request.generation,
        };
        eprintln!(
            "original joined subscription reap refused: error={:?}, key={key:?}, phase_input={:?}, ingress={:?}, original_result={:?}, actual_custody={:?}, header_retired={}, progress_role={:?}, ap={:?}, ap_inputs={:?}, ap_standings={:?}",
            applied.as_ref().err(),
            original.phase.current_input(),
            rig.ingress.as_ref().unwrap().test_probe().stream(key),
            *original.result.lock().unwrap(),
            original.role.exit_custody().phase(),
            original.header.is_retired(),
            rig.progress_id,
            rig.runtime.snapshot(rig.ap.actor()),
            rig.runtime.input_inventory(rig.ap.actor(), None, 32),
            rig.runtime
                .standing_input_inventory(rig.ap.actor(), None, 32),
        );
        let trace = rig.trace.lock().unwrap();
        eprintln!(
            "original joined subscription public trace: deadline={:?}, turns={:?}, observations={:?}, effects={:?}",
            trace.deadline, trace.turns, trace.observations, trace.effects,
        );
    }
    let applied = applied.unwrap();
    assert_eq!(applied.participation, original.request.participation);
    // ONLY the actual F/captures/Join/report and phase/claims exit author this fact.
    rig.post(
        rig.sources.subscription,
        AttemptInput::Executor(ExecutorFact::SubscriptionExited {
            effect: original.effect,
            participation: original.request.participation,
            generation: original.request.generation,
        }),
    );
    rig.drain(&[]);
}
struct Run {
    rig: Rig,
    peer: Option<Loopback>,
    native: Option<FrontendDataRuntime>,
    control: Option<novarocks_workload_control::WorkloadControl>,
    root: Option<novarocks_workload_control::RootWork>,
    driver: Option<novarocks_workload_control::joint_admission::JointProgressDriver>,
    capacity: Option<novarocks_workload_control::ResultCapacityHandle>,
    window: Option<novarocks_workload_control::ResultWindowGrant>,
    cohort:
        Option<crate::query_execution::native_execution_adapter::source_cohort::NativeSourceCohort>,
    source: Option<Arc<SubscriptionSource<FrontendActor>>>,
    jobs: [Option<Original>; 2],
}
struct Retained(Option<Box<Run>>);
impl Drop for Retained {
    fn drop(&mut self) {
        if let Some(original) = self.0.take() {
            // An assertion unwind retains SAME actor/Scope/Source/Work/Join/
            // peer/WC originals together. No async cleanup, abort or positive
            // exit is invented by a test destructor.
            static UNKNOWN: std::sync::OnceLock<Mutex<Vec<Box<dyn std::any::Any + Send>>>> =
                std::sync::OnceLock::new();
            UNKNOWN
                .get_or_init(Default::default)
                .lock()
                .unwrap()
                .push(original);
        }
    }
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn covered_loopback_backpressure_recovery_quiesce_settles_actual_root_seal() {
    let mut retained = Retained(Some(Box::new(Run {
        rig: Rig::new(),
        peer: None,
        native: None,
        control: None,
        root: None,
        driver: None,
        capacity: None,
        window: None,
        cohort: None,
        source: None,
        jobs: [None, None],
    })));
    let c = retained.0.as_mut().unwrap();
    let rig = &mut c.rig;
    rig.drain(&[]);
    let leaf = *rig
        .tasks
        .iter()
        .find(|identity| **identity != rig.root)
        .unwrap();
    for _ in 0..4 {
        if rig.trace.lock().unwrap().last.contains_key(&n(2)) {
            break;
        }
        rig.advance_original_deadline();
    }
    let (effect, request) = rig
        .take_open(n(2))
        .expect("current AP successor generation two");
    c.peer = Some(Loopback::start().await);
    let peer = c.peer.as_ref().unwrap();
    peer.expect_covered_frames_then_hold(vec![
        frame(
            CoveredStatusStreamFact::Status(
                TaskStatus::try_new(
                    leaf,
                    TaskStatusVersion::new(2).unwrap(),
                    TaskState::Running,
                    None,
                    TaskOutputFacts::default(),
                )
                .unwrap(),
            ),
            None,
        ),
        frame(
            CoveredStatusStreamFact::CatchUpComplete(CoveredCatchUpComplete {
                generation: 2,
                initial_cut: 2,
            }),
            None,
        ),
        frame(
            CoveredStatusStreamFact::Bookmark(CoveredObservationBookmark {
                generation: 2,
                sequence: 1,
                covered_prefix: 2,
                source_cut: 3,
            }),
            None,
        ),
    ]);
    peer.expect_covered_frames_then_hold(Vec::new());
    let (control, driver, capacity) = wc::fixture();
    c.control = Some(control);
    c.driver = Some(driver);
    c.capacity = Some(capacity);
    c.root = Some(
        c.control
            .as_ref()
            .unwrap()
            .try_begin_root(WorkRequest::new(WorkClass::Query))
            .unwrap(),
    );
    let scope = c.root.as_ref().unwrap().owner.scope();
    wc::bind(&scope, c.driver.as_mut().unwrap());
    c.window = Some(
        c.capacity
            .as_ref()
            .unwrap()
            .try_acquire(&scope, ResultWindowClass::Client)
            .unwrap(),
    );
    c.cohort = Some(wc::cohort(&scope, 1, c.driver.as_mut().unwrap()));
    c.source = Some(
        SubscriptionSource::from_cohort(
            rig.participation,
            &scope,
            c.cohort.as_ref().unwrap(),
            rig.ingress.as_ref().unwrap().clone(),
        )
        .unwrap(),
    );
    let source = c.source.as_ref().unwrap();
    let source_scope = source.scope().unwrap();
    c.native = Some(FrontendDataRuntime::new(Handle::current()));
    let client = Client::for_backend_in(
        Arc::from([peer.descriptor(rig.participation.backend_process_id())]),
        0,
        c.native.as_ref().unwrap().clone(),
    );
    let [first_slot, second_slot] = &mut c.jobs;
    *first_slot = Some(rig.start(source, effect, request, client.clone()));
    let first = first_slot.as_mut().unwrap();
    tokio::time::timeout(Duration::from_secs(3), async {
        while peer.subscription_requests().len() != 1 || peer.open_covered_subscriptions() != 1 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    // Real queue=1/positions=3: two accepted original claims stop the third read.
    let probe = rig.ingress.as_ref().unwrap().test_probe();
    let second_key = StreamKey {
        participation: rig.participation,
        generation: n(2),
    };
    tokio::time::timeout(Duration::from_secs(3), async {
        while !probe.stream(second_key).is_some_and(|state| {
            state.accepted_unretired == 2 && state.in_flight && state.queued == 1 && state.paused
        }) {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    tokio::time::sleep(Duration::from_millis(5200)).await;
    assert_eq!(peer.subscription_requests().len(), 1);
    assert_eq!(*first.result.lock().unwrap(), None);
    let paused_state = probe.stream(second_key).unwrap();
    assert_eq!(paused_state.accepted_unretired, 2);
    assert!(paused_state.paused);
    assert!(paused_state.in_flight);
    assert_eq!(paused_state.queued, 1);
    assert!(source.reap_generation(n(2)).is_err());
    rig.clock.advance(Duration::from_millis(5200));
    drive_until(rig, source, &[&first.phase], |rig| {
        rig.trace.lock().unwrap().observations.iter().any(|row| {
            matches!(
                &row.event,
                ObservationEvent::Fact {
                    fact: ObservationFact::Bookmark { generation: 2, .. },
                    ..
                }
            )
        })
    })
    .await;
    assert!(
        rig.trace
            .lock()
            .unwrap()
            .phases
            .iter()
            .any(|phase| phase.participation == rig.participation
                && phase.generation == n(2)
                && phase.opened_at.is_some()),
        "the SAME actual H2 opening phase reached the public AP Core"
    );
    // The SAME real Open2 has now published and the AP consumed its actual
    // opening phase. Start normal convergence through the current LP ticket;
    // Unobserved process sampling grants no positive remote closure.
    rig.command(LogicalToAttemptBody::BeginConvergence {
        mode: AttemptConvergenceMode::Normal,
        remote_deadline: MonotonicInstant::from_origin(Duration::from_secs(60)),
        computation_ready: None,
        retain_diagnostics: false,
    });
    rig.drain(&[&first.phase]);
    let quiesce_issued = rig.trace.lock().unwrap().effects.iter().any(|effect| {
        matches!(
            &effect.body,
            EffectBody::AdmitQueuedInput {
                operation:
                    novarocks_attempt_protocol::effect::OperationIntent::QuiesceParticipation(_),
                ..
            }
        )
    });
    if !quiesce_issued {
        {
            let trace = rig.trace.lock().unwrap();
            eprintln!(
                "original Normal Quiesce absent: clock={:?}, indexed_deadline={:?}, original_turns={:?}, observations={:?}, effects={:?}",
                rig.clock.now(),
                trace.deadline,
                trace.turns,
                trace.observations,
                trace.effects,
            );
        }
        // Read original kernel metadata after releasing the fixture trace lock.
        eprintln!(
            "original Normal Quiesce runtime: lp={:?}, ap={:?}, unresolved_work={:?}",
            rig.runtime.snapshot(rig.lp.actor()),
            rig.runtime.snapshot(rig.ap.actor()),
            rig.ledger.unresolved_work(),
        );
    }
    assert!(
        quiesce_issued,
        "the same current-ticket normal drain issues its exact Quiesce intent"
    );
    let seal = AttemptRequestToken::new(n(1));
    rig.command(LogicalToAttemptBody::SealRequest { token: seal });
    rig.drain(&[&first.phase]);
    assert_eq!(
        rig.seal_count(),
        0,
        "Gone's missing terminal blocks the same real Seal row"
    );
    assert_eq!(
        rig.roots.as_ref().unwrap().fixture_seal_state(),
        Some((seal, true, false))
    );
    peer.close_held_subscriptions();
    settle_original(rig, source, first).await;
    assert_eq!(
        *first.result.lock().unwrap(),
        Some(SubscriptionEnd::RemoteClosed)
    );
    for _ in 0..4 {
        if rig.trace.lock().unwrap().last.contains_key(&n(3)) {
            break;
        }
        rig.advance_original_deadline();
    }
    let (effect, request) = rig
        .take_open(n(3))
        .expect("Core generation three after actual original exit");
    assert!(
        request
            .status_cursors
            .iter()
            .any(|cursor| cursor.identity() == leaf
                && cursor.current_version() == Some(TaskStatusVersion::new(2).unwrap()))
    );
    *second_slot = Some(rig.start(source, effect, request, client));
    let second = second_slot.as_mut().unwrap();
    drive_until(rig, source, &[&second.phase], |_| {
        peer.subscription_requests().len() == 2 && peer.open_covered_subscriptions() == 1
    })
    .await;
    let requests = peer.subscription_requests();
    let replay = decode_covered_subscribe_task_status(
        &requests[1],
        FieldPath::root("same_ap_applied_replay"),
    )
    .unwrap();
    assert_eq!(replay.generation, n(3));
    assert!(
        replay
            .status_cursors
            .iter()
            .any(|cursor| cursor.identity() == leaf
                && cursor.current_version() == Some(TaskStatusVersion::new(2).unwrap()))
    );
    for (version, state) in [(3, TaskState::Canceling), (4, TaskState::Canceled)] {
        peer.publish_held_covered_event(frame(
            CoveredStatusStreamFact::Status(
                TaskStatus::try_new(
                    leaf,
                    TaskStatusVersion::new(version).unwrap(),
                    state,
                    Some(TerminationDetail::Canceled(
                        CancelReason::UpstreamNoLongerNeeded,
                    )),
                    TaskOutputFacts::new(false),
                )
                .unwrap(),
            ),
            None,
        ))
        .await;
    }
    peer.publish_held_covered_event(frame(
        CoveredStatusStreamFact::Quiesce(QuiesceParticipationReceipt::new(
            rig.participation,
            1,
            rig.tasks.clone(),
            ParticipationState::Quiescing,
        )),
        None,
    ))
    .await;
    drive_until(rig, source, &[&second.phase], |rig| {
        rig.trace
            .lock()
            .unwrap()
            .last
            .get(&n(3))
            .is_some_and(|request| {
                request
                    .quiesce_cursor
                    .is_some_and(|cursor| cursor.fence_version.is_some())
            })
    })
    .await;
    assert_eq!(
        rig.seal_count(),
        0,
        "terminal and Quiesce do not complete an initial cut"
    );
    assert_eq!(
        rig.roots.as_ref().unwrap().fixture_seal_state(),
        Some((seal, true, false))
    );
    peer.publish_held_covered_event(frame(
        CoveredStatusStreamFact::CatchUpComplete(CoveredCatchUpComplete {
            generation: 3,
            initial_cut: 5,
        }),
        None,
    ))
    .await;
    drive_until(rig, source, &[&second.phase], |rig| rig.seal_count() == 1).await;
    assert_eq!(
        rig.roots.as_ref().unwrap().fixture_seal_state(),
        Some((seal, true, true))
    );
    {
        let seen = rig.seen.lock().unwrap();
        let reply = seen
            .iter()
            .find(|message| {
                matches!(&message.body,
        novarocks_coordination_contract::logical_port::AttemptToLogicalBody::SealResult {..})
            })
            .unwrap();
        assert_eq!(reply.activation, rig.activation);
        assert!(matches!(&reply.body,
        novarocks_coordination_contract::logical_port::AttemptToLogicalBody::SealResult {token,result}
            if *token==seal&&result.is_ok()));
        if let novarocks_coordination_contract::logical_port::AttemptToLogicalBody::SealResult {
            result: Ok(payload),
            ..
        } = &reply.body
        {
            assert!(Arc::ptr_eq(
                payload,
                rig.trace.lock().unwrap().seal_payload.as_ref().unwrap()
            ));
        } else {
            unreachable!()
        }
    }

    source.close_generation(n(3)).unwrap();
    settle_original(rig, source, second).await;
    assert_eq!(
        *second.result.lock().unwrap(),
        Some(SubscriptionEnd::CoreClosed)
    );
    source.close_new_jobs();
    source.close_after_jobs().unwrap();
    wc::drive(c.driver.as_mut().unwrap());
    tokio::time::timeout(Duration::from_secs(2), source_scope.wait_released())
        .await
        .unwrap();
    drop((first_slot.take(), second_slot.take(), source_scope, scope));
    drop((
        c.source.take(),
        c.cohort.take(),
        c.window.take(),
        c.capacity.take(),
    ));
    let deadline = std::time::Instant::now() + Duration::from_secs(3);
    c.native
        .as_mut()
        .unwrap()
        .drain_original_apply_until(deadline)
        .await
        .unwrap();
    c.native
        .as_mut()
        .unwrap()
        .drain_native_outgoing_until(deadline)
        .await
        .unwrap();
    assert!(c.peer.as_mut().unwrap().settle().await);
    drop((c.native.take(), c.peer.take()));
    // Complete the same actual Root only after all original Source jobs,
    // aliases and child accounting above have actually exited. Keep the
    // unique process Control and driver in whole-original custody on failure.
    let root = c.root.take().unwrap();
    root.owner.complete_after_terminal_cancel_settled();
    root.business.release();
    c.control.as_ref().unwrap().close_admission();
    while let Some(original) = c.control.as_ref().unwrap().next_control() {
        original.acknowledge();
    }
    wc::drive(c.driver.as_mut().unwrap());
    if let Err(refused) = c.control.take().unwrap().shutdown() {
        let (error, original) = refused.into_parts();
        c.control = Some(original);
        panic!("same original Root/Source WC shutdown remains unproven: {error:?}");
    }
    drop(c.driver.take());
    rig.finish();
    drop(retained.0.take());
}
