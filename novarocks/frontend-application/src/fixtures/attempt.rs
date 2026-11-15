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

//! Scripted protocol backend. The actor, prepaid ports, jobs and HostExit are
//! real originals; only the attempt business decisions are supplied by scripts.
use super::*;
use crate::control_runtime::{
    actor::TestActor,
    host::{
        EffectBindRefusal, EffectExecutor, EffectSource, HostExit, PendingHandoff,
        PreparedEffectBinding, PreparedEffectJob, SourceProgress,
    },
    transport_withdrawal::AttemptEffectEnvelope,
};
use crate::task_execution::host::{
    funding::{AttemptHostFunding, FundingProgress},
    observations::{
        ObservationBudget, OrderedObservationInput, RootControlBinding,
        backing::{ObservationBackingQuote, PaidObservationMetadata},
        credits::ObservationCredits,
        host_turn::OrderedObservationHost,
        source::BoundObservationSource,
    },
    port::{
        AttemptPortEndpoints,
        abort::{AbortIngressRetirement, AbortIngressRetirementProgress, AttemptAbortIngress},
        control::{ControlBinding, ControlPositions, ControlReaderObligation, SourcePublishedSlot},
        facts::{AttemptFactBank, role_for},
        requests::AttemptRequests,
        root_control::RootControlRequests,
    },
    startup::{AttemptStartupSource, StartupInput},
};
use novarocks_attempt_protocol::{
    effect::{AttemptEffect, EffectBody, EffectClass},
    failure::AttemptFailure,
    input::AttemptInput,
};
use novarocks_control_runtime::{
    actor::{ActorConfig, ActorStep},
    actor_turn::{ActorHostTurn, ActorTurnError, EffectTicket, StandingInputObligation},
    causal_input::{CAUSAL_INPUT_POSITIONS, CausalInputObligation},
    completion::ReportSlot,
    exit_custody::{ExitCustody, ExitCustodyPhase},
    exit_work::{PanicSink, run_future_in_generation},
    header_readiness::HeaderReadyRegistration,
    obligation::{ReservationObligation, WorkObligation, WorkRetirementObserver},
};
use novarocks_control_runtime_core::{
    capacity::{ReservationResources, ResourceVector},
    effect::{EffectDescriptor, EffectKind},
    identity::{ActionId, ActorId, ConsumerId, Generation, InputId, ReservationId},
    mailbox::InputEnvelope,
};
use novarocks_coordination_contract::{
    AttemptActionId, AttemptRequestToken,
    logical_port::{
        AttemptToLogical, AttemptToLogicalBody, LogicalToAttempt, LogicalToAttemptBody,
    },
};
use novarocks_execution_contract::TaskIdentity;
use novarocks_types::{BackendProcessId, StageId, TaskId};
use std::{
    collections::{BTreeMap, VecDeque},
    num::{NonZeroU64, NonZeroUsize},
    sync::Weak,
};

const EPOCH: Generation = Generation::new(1).unwrap();
const OBSERVER: ConsumerId = ConsumerId::new(1).unwrap();

pub(super) struct AttemptContext {
    pub(super) runtime: ActorRuntime<FrontendActor>,
    pub(super) ledger: RuntimeLedger,
    pub(super) host: Arc<HostRegistry>,
    pub(super) logical: ActorRegistration,
    pub(super) inputs: Arc<LogicalInputBank>,
    pub(super) links: Arc<LogicalAttemptLinks>,
    pub(super) loan: super::super::links::AttemptActivationLinkLoan,
    pub(super) activation: AttemptActivationIdentity,
    pub(super) logical_wake: Waker,
    pub(super) participations: Arc<[novarocks_execution_contract::ParticipationRef]>,
    pub(super) input_loan: super::super::input_bank::LogicalInputsLoan,
}
#[derive(Default)]
struct ScriptState {
    next: u64,
    queue: VecDeque<(AttemptActionId, AttemptToLogicalBody)>,
    received: Vec<LogicalToAttempt>,
    published: Vec<AttemptActionId>,
    steps: usize,
    signal: Option<Weak<ScriptSignal>>,
    closing: bool,
    ping_requested: bool,
    reservations: BTreeMap<AttemptActionId, ReservationId>,
    original: BTreeMap<AttemptActionId, AttemptToLogical>,
    fact_inputs: Vec<InputId>,
}
#[derive(Clone, Default)]
pub(crate) struct AttemptScript(Arc<Mutex<ScriptState>>);
impl AttemptScript {
    pub(crate) fn enqueue(&self, body: AttemptToLogicalBody) -> AttemptActionId {
        let mut s = self.0.lock().unwrap();
        assert!(!s.closing, "closed original script producer");
        s.next = s.next.checked_add(1).expect("script action identity");
        let action = AttemptActionId::new(NonZeroU64::new(s.next).unwrap());
        s.queue.push_back((action, body));
        drop(s);
        self.ping();
        action
    }
    pub(crate) fn received(&self) -> Vec<LogicalToAttempt> {
        self.0.lock().unwrap().received.clone()
    }
    pub(crate) fn published(&self) -> Vec<AttemptActionId> {
        self.0.lock().unwrap().published.clone()
    }
    pub(crate) fn steps(&self) -> usize {
        self.0.lock().unwrap().steps
    }
    pub(crate) fn ping(&self) {
        let signal = {
            let mut state = self.0.lock().unwrap();
            state.ping_requested = true;
            state.signal.as_ref().and_then(Weak::upgrade)
        };
        if let Some(signal) = signal {
            signal.wake.wake_by_ref();
        }
    }
}
struct ScriptSignal {
    wake: Waker,
}
struct AttemptWake {
    runtime: ActorRuntime<FrontendActor>,
    registration: ActorRegistration,
    dirty: Arc<AtomicBool>,
}
impl Wake for AttemptWake {
    fn wake(self: Arc<Self>) {
        self.wake_by_ref();
    }
    fn wake_by_ref(self: &Arc<Self>) {
        self.dirty.store(true, Ordering::Release);
        let _ = self.runtime.notify_host_actor(&self.registration);
    }
}
#[derive(Clone)]
pub(crate) struct AttemptCase {
    pub(crate) activation: AttemptActivationIdentity,
    pub(crate) script: AttemptScript,
    pub(crate) registration: ActorRegistration,
    endpoints: Arc<AttemptPortEndpoints>,
    exit: Arc<HostExit>,
    // Weak input-bank projection avoids retaining the logical application.
    _signal: Arc<ScriptSignal>,
    runtime: ActorRuntime<FrontendActor>,
    logical: ActorRegistration,
}
impl AttemptCase {
    pub(crate) fn close(&self) {
        self.script.0.lock().unwrap().closing = true;
        self.exit.request_close().unwrap();
    }
    pub(crate) fn occupied(&self) -> (bool, bool) {
        let inputs = self.script.0.lock().unwrap().fact_inputs.clone();
        (
            !self.endpoints.requests.is_empty(),
            inputs.into_iter().any(|id| {
                self.runtime
                    .input_custody(self.logical.actor(), id)
                    .is_ok_and(|original| original.is_some())
            }),
        )
    }
    pub(crate) fn exited(&self) -> bool {
        *self.exit.observe_actual_exit().borrow()
    }
}
struct ScriptActor {
    script: AttemptScript,
    activation: AttemptActivationIdentity,
    scope: ScopeHandle,
    parent: ReservationId,
}
impl TestActor for ScriptActor {
    fn handle(
        &mut self,
        _: Tick,
        envelope: InputEnvelope,
        value: FrontendInput,
        step: &mut ActorStep<'_, FrontendEffect>,
    ) {
        assert!(step.charge(1));
        self.script.0.lock().unwrap().steps += 1;
        match value {
            FrontendInput::AttemptStartup(
                StartupInput::Bootstrap | StartupInput::StartProtocol,
            ) => return,
            FrontendInput::Attempt(value) => {
                let AttemptInput::Port(original) = *value else {
                    panic!("script AP accepts original port input");
                };
                let projected = project_received(original);
                let root = matches!(
                    projected.body,
                    LogicalToAttemptBody::SealRequest { .. }
                        | LogicalToAttemptBody::TerminalControlDemand { .. }
                );
                self.script.0.lock().unwrap().received.push(projected);
                if root {
                    let author = step
                        .input_author()
                        .expect("original ordered root input author")
                        .clone();
                    step.effect_from_author(
                        &author,
                        EffectDescriptor {
                            id: ActionId::new(u64::MAX).unwrap(),
                            work: author.work(),
                            kind: EffectKind::Settlement,
                            dependencies: vec![],
                        },
                        FrontendEffect::AttemptRootControlProcessed(envelope.input),
                    )
                    .unwrap();
                }
                return;
            }
            FrontendInput::Test(_) => {}
            _ => panic!("unexpected original script AP input"),
        }
        let Some((action, body)) = self.script.0.lock().unwrap().queue.pop_front() else {
            return;
        };
        let guard = self
            .scope
            .reserve_child_next(
                EPOCH,
                self.parent,
                ReservationResources::new(
                    ResourceVector::default(),
                    OwnedSlotNeed {
                        work_headers: 1,
                        completions: 1,
                        captures: 1,
                        ..Default::default()
                    },
                ),
                WorkKind::Settlement,
            )
            .unwrap();
        let reservation = guard.reservation_id();
        let work = novarocks_control_runtime_core::identity::ScopedWorkRef {
            scope: self.scope.id(),
            work: guard.work_id(),
        };
        assert!(step.reservation(guard).is_ok());
        let message = AttemptToLogical {
            activation: self.activation,
            action,
            body,
        };
        let protocol = script_protocol(&message);
        {
            let mut state = self.script.0.lock().unwrap();
            assert!(state.reservations.insert(action, reservation).is_none());
            assert!(state.original.insert(action, message).is_none());
        }
        step.effect(
            EffectDescriptor {
                id: ActionId::new(action.get()).unwrap(),
                work,
                kind: EffectKind::Settlement,
                dependencies: vec![],
            },
            FrontendEffect::Attempt(AttemptEffectEnvelope::new(AttemptEffect {
                id: action,
                class: EffectClass::Settlement,
                body: EffectBody::Port(protocol),
            })),
        )
        .unwrap();
    }
}

fn project_failure(original: AttemptFailure) -> novarocks_coordination_contract::ExecutionFailure {
    match original {
        AttemptFailure::Logical(original) => original,
        other => {
            novarocks_attempt_protocol::port_projection::project_failure(&other, &mut |_| None)
                .unwrap()
        }
    }
}
fn project_received(
    original: novarocks_coordination_contract::LogicalToAttempt<AttemptFailure>,
) -> LogicalToAttempt {
    use novarocks_coordination_contract::LogicalToAttemptBody as O;
    let body = match original.body {
        O::EstablishDetachReply { .. } => panic!("retired Detach adapter reply has no logical-port projection"),
        O::AuthorizationReply {token,decision} => LogicalToAttemptBody::AuthorizationReply {token,decision:decision.map_err(|e|match e {
            novarocks_coordination_contract::AttemptAuthorizationFailure::Rejected(f)=>novarocks_coordination_contract::logical_port::AttemptAuthorizationFailure::Rejected(project_failure(f)),
            novarocks_coordination_contract::AttemptAuthorizationFailure::EstablishBudgetExhausted(f)=>novarocks_coordination_contract::logical_port::AttemptAuthorizationFailure::EstablishBudgetExhausted(project_failure(f)),
        })},
        O::EstablishSettlementReply {token,authorization,result}=>LogicalToAttemptBody::EstablishSettlementReply {token,authorization,result:result.map_err(project_failure)},
        O::PreReadyReplacementReply {token,may_observe}=>LogicalToAttemptBody::PreReadyReplacementReply {token,may_observe},
        O::PreReadyReplacementOwnerFailed {token,failure}=>LogicalToAttemptBody::PreReadyReplacementOwnerFailed {token,failure:project_failure(failure)},
        O::PreReadyReplacementChanged {token}=>LogicalToAttemptBody::PreReadyReplacementChanged {token},
        O::Abort {action,request}=>LogicalToAttemptBody::Abort {action,request},
        O::Stop {reason,failure}=>LogicalToAttemptBody::Stop {reason,failure:project_failure(failure)},
        O::SealRequest {token}=>LogicalToAttemptBody::SealRequest {token},
        O::TerminalControlDemand {token,root}=>LogicalToAttemptBody::TerminalControlDemand {token,root},
        O::BeginConvergence {mode,remote_deadline,computation_ready,retain_diagnostics}=>LogicalToAttemptBody::BeginConvergence {mode,remote_deadline,computation_ready,retain_diagnostics},
    };
    LogicalToAttempt {
        activation: original.activation,
        body,
    }
}
fn script_protocol(
    message: &AttemptToLogical,
) -> novarocks_coordination_contract::AttemptToLogical<AttemptFailure> {
    use novarocks_coordination_contract::{
        AttemptToLogicalBody as O, logical_port::AttemptTerminal as T,
    };
    let body = match &message.body {
        AttemptToLogicalBody::AuthorizationRequest { token, request } => {
            use novarocks_coordination_contract::{
                AttemptAuthorization as A, logical_port::AttemptAuthorization as N,
            };
            let request = match request {
                N::BeginAdmission(r) => A::BeginAdmission(*r),
                N::SettleAdmission {
                    issue,
                    participation,
                    operation_id,
                    outcome,
                    receipt,
                    prior_transport_unknown,
                } => A::SettleAdmission {
                    issue: *issue,
                    participation: *participation,
                    operation_id: *operation_id,
                    outcome: *outcome,
                    receipt: *receipt,
                    prior_transport_unknown: *prior_transport_unknown,
                },
                N::Establish {
                    participation,
                    operation_id,
                    ..
                } => A::Establish {
                    participation: *participation,
                    operation_id: *operation_id,
                },
                N::ReauthorizeEstablish {
                    participation,
                    operation_id,
                } => A::ReauthorizeEstablish {
                    participation: *participation,
                    operation_id: *operation_id,
                },
            };
            O::AuthorizationRequest {
                token: *token,
                request,
            }
        }
        AttemptToLogicalBody::PreReadyReplacementQuery { token, refusal } => {
            O::PreReadyReplacementQuery {
                token: *token,
                refusal: AttemptFailure::Logical(refusal.clone()),
            }
        }
        AttemptToLogicalBody::EstablishSettlement {
            token,
            authorization,
            participation,
            operation_id,
            settlement,
        } => O::EstablishSettlement {
            token: *token,
            authorization: *authorization,
            participation: *participation,
            operation_id: *operation_id,
            settlement: *settlement,
        },
        AttemptToLogicalBody::ConvergenceProgress {
            local_exited,
            closure,
            lifecycle_runtime_filters,
        } => O::ConvergenceProgress {
            local_exited: *local_exited,
            closure: closure.clone(),
            lifecycle_runtime_filters: lifecycle_runtime_filters.clone(),
        },
        AttemptToLogicalBody::AttemptTerminal(terminal) => O::AttemptTerminal(match terminal {
            T::Completed => novarocks_coordination_contract::AttemptTerminal::Completed,
            T::Failed(f) => novarocks_coordination_contract::AttemptTerminal::Failed(
                AttemptFailure::Logical(f.clone()),
            ),
            T::Stopped { reason, failure } => {
                novarocks_coordination_contract::AttemptTerminal::Stopped {
                    reason: *reason,
                    failure: AttemptFailure::Logical(failure.clone()),
                }
            }
        }),
        AttemptToLogicalBody::RootStatus(value) => O::RootStatus(value.clone()),
        AttemptToLogicalBody::SealResult { token, result } => O::SealResult {
            token: *token,
            result: result.clone().map_err(AttemptFailure::Logical),
        },
        AttemptToLogicalBody::TerminalControlResult { token, result } => O::TerminalControlResult {
            token: *token,
            result: result.clone().map_err(AttemptFailure::Logical),
        },
        AttemptToLogicalBody::AttemptLost {
            failure,
            unknown_participations,
            unresolved_actions,
        } => O::AttemptLost {
            failure: AttemptFailure::Logical(failure.clone()),
            unknown_participations: unknown_participations.clone(),
            unresolved_actions: unresolved_actions.clone(),
        },
        AttemptToLogicalBody::AbortSettlement { .. } => {
            panic!("script Abort settlement requires an original physical receipt backend")
        }
    };
    novarocks_coordination_contract::AttemptToLogical {
        activation: message.activation,
        action: message.action,
        body,
    }
}

// A test assertion during construction is not a physical cleanup receipt.
// Keep every already-created original in stable custody while unwinding;
// successful construction transfers it once into the installed Source.
struct ConstructionOriginal<T: Send + 'static>(Option<T>);
impl<T: Send + 'static> ConstructionOriginal<T> {
    fn new(original: T) -> Self {
        Self(Some(original))
    }
    fn take(&mut self) -> T {
        self.0.take().expect("same original construction transfer")
    }
}
impl<T: Send + 'static> std::ops::Deref for ConstructionOriginal<T> {
    type Target = T;
    fn deref(&self) -> &T {
        self.0.as_ref().unwrap()
    }
}
impl<T: Send + 'static> std::ops::DerefMut for ConstructionOriginal<T> {
    fn deref_mut(&mut self) -> &mut T {
        self.0.as_mut().unwrap()
    }
}
fn retain_construction(original: impl std::any::Any + Send) {
    static ORIGINALS: std::sync::OnceLock<Mutex<Vec<Box<dyn std::any::Any + Send>>>> =
        std::sync::OnceLock::new();
    ORIGINALS
        .get_or_init(Default::default)
        .lock()
        .unwrap_or_else(|p| p.into_inner())
        .push(Box::new(original));
}
impl<T: Send + 'static> Drop for ConstructionOriginal<T> {
    fn drop(&mut self) {
        if std::thread::panicking()
            && let Some(original) = self.0.take()
        {
            retain_construction(original);
        }
    }
}
pub(super) struct AttemptBackend;
impl AttemptBackend {
    pub(super) fn activate(context: AttemptContext, script: AttemptScript) -> AttemptCase {
        let AttemptContext {
            runtime,
            ledger,
            host,
            logical,
            inputs,
            links,
            loan,
            activation,
            logical_wake,
            participations,
            mut input_loan,
        } = context;
        assert!(
            links.endpoint(activation).is_none(),
            "one original scripted endpoint install"
        );
        let budget = ObservationBudget::for_task_attempt();
        let quote = ObservationBackingQuote::new(budget, participations.len()).unwrap();
        let quota = quote
            .quota()
            .checked_add(&budget.byte_window().unwrap())
            .unwrap();
        let mut funding = ConstructionOriginal::new(
            loan.reserve_attempt(
                &ledger,
                EPOCH,
                ReservationResources::new(
                    quota,
                    OwnedSlotNeed {
                        inputs: budget.total_positions + 2 + participations.len(),
                        work_headers: 1,
                        ..Default::default()
                    },
                ),
            )
            .unwrap(),
        );
        let scope = funding.scope().clone();
        let (_, parent_payer) = funding.parent();
        let parent_id = parent_payer.id();
        let source_guard = scope
            .reserve_child_next(
                EPOCH,
                parent_id,
                ReservationResources::new(
                    ResourceVector::default(),
                    OwnedSlotNeed {
                        inputs: 5 + participations.len(),
                        work_headers: 1,
                        ..Default::default()
                    },
                ),
                WorkKind::Settlement,
            )
            .unwrap();
        let mut source_author = ConstructionOriginal::new(
            super::super::funding::SourceFundingObligation::start(source_guard),
        );
        // Count the actual pre-paid standing roles of THIS AP. The 4098
        // observation credit positions are backing storage, not extra Core
        // mailbox roles. Causal replies follow canonical AP Port Lifecycle.
        let mut mailbox = [0; 4];
        mailbox[InputClass::Lifecycle.index()] = 2 + 3 * CAUSAL_INPUT_POSITIONS;
        mailbox[InputClass::Control.index()] = participations
            .len()
            .checked_add(3)
            .expect("original AP Control inventory");
        mailbox[InputClass::Ordinary.index()] = participations
            .len()
            .checked_add(2)
            .expect("original AP Ordinary inventory");
        mailbox[InputClass::Timer.index()] = 1;
        let config = ActorConfig {
            mailbox,
            max_batches: 64,
            // Each handle spends one traversal unit and at most one emission
            // unit (charged by effect()). It emits either one port effect OR
            // one root-processed notice, with at most one real step guard.
            max_effects_per_step: 1,
            max_dependencies_per_effect: 0,
            turn_budget: participations.len() + 12,
            max_step_work: 2,
        };
        // Runtime-owned actor identities follow actual child Scope allocation;
        // these addresses confer neither Work nor application identity.
        let actor = ActorId::new(
            scope
                .id()
                .get()
                .checked_mul(10)
                .unwrap()
                .checked_sub(9)
                .unwrap(),
        )
        .unwrap();
        let registration = runtime
            .register_bound(
                actor,
                scope.clone(),
                FrontendActor::Test(Box::new(ScriptActor {
                    script: script.clone(),
                    activation,
                    scope: scope.clone(),
                    parent: parent_id,
                })),
                config,
            )
            .unwrap_or_else(|(error, original)| {
                retain_construction((error,original));
                panic!("actual original script AP registration refused: {error:?}; actor={actor:?}; scope={:?}; config={config:?}", scope.id());
            });
        let dirty = Arc::new(AtomicBool::new(true));
        let wake = Waker::from(Arc::new(AttemptWake {
            runtime: runtime.clone(),
            registration: registration.clone(),
            dirty: dirty.clone(),
        }));
        let signal = Arc::new(ScriptSignal { wake: wake.clone() });
        script.0.lock().unwrap().signal = Some(Arc::downgrade(&signal));
        let reserve = |class| {
            runtime
                .reserve_admitted_input_for(
                    &registration,
                    logical.actor(),
                    class,
                    source_author.work(),
                )
                .unwrap()
                .into_standing_input()
                .unwrap_or_else(|_| panic!("original paid AP role"))
        };
        let startup_role = reserve(InputClass::Lifecycle);
        let mut trigger = ConstructionOriginal::new(reserve(InputClass::Lifecycle));
        let mut positions = ConstructionOriginal::new(ControlPositions {
            stop: reserve(InputClass::Control),
            convergence: reserve(InputClass::Control),
            changed: reserve(InputClass::Ordinary),
        });
        let abort = Arc::new(AttemptAbortIngress::new(
            logical.clone(),
            registration.clone(),
            activation,
            participations
                .iter()
                .copied()
                .map(|p| (p, reserve(InputClass::Control))),
        ));
        let abort_retirement = abort.retirement();
        let (startup_source, startup) =
            AttemptStartupSource::new(registration.clone(), startup_role);
        let mut startup_source = ConstructionOriginal::new(startup_source);
        let root = TaskIdentity::new(
            activation.execution(),
            StageId::new(1).unwrap(),
            TaskId::new(1).unwrap(),
            participations
                .first()
                .map(|p| p.backend_process_id())
                .unwrap_or_else(BackendProcessId::new_v7),
        );
        let roots = RootControlRequests::new(
            RootControlBinding::for_registered_source(registration.clone(), activation, root)
                .unwrap(),
        );
        let (mut result_author, observation_source, progress_roles) = {
            let (observation, payer, root_control) = funding.observation_and_root_control();
            let result_guard = root_control
                .scope()
                .reserve_next(
                    EPOCH,
                    ReservationResources::new(
                        ResourceVector::default(),
                        OwnedSlotNeed {
                            inputs: 3 * CAUSAL_INPUT_POSITIONS,
                            work_headers: 1,
                            ..Default::default()
                        },
                    ),
                    WorkKind::Settlement,
                    novarocks_control_runtime_core::scope::WorkAdmission::External,
                )
                .unwrap();
            let result_author = ConstructionOriginal::new(
                super::super::funding::SourceFundingObligation::start(result_guard),
            );
            let metadata =
                PaidObservationMetadata::take_from(quote, observation, payer, wake.clone())
                    .unwrap();
            funding_observation_source(
                &runtime,
                &registration,
                &roots,
                observation,
                payer,
                metadata,
                &participations,
                &wake,
                budget,
            )
            .map(|(source, roles)| (result_author, source, roles))
            .unwrap()
        };
        let mut purposes: ConstructionOriginal<[Purpose; 3]> =
            ConstructionOriginal::new(std::array::from_fn(|_| {
                Purpose::new(
                    CausalInputObligation::reserve_admitted(
                        &runtime,
                        &registration,
                        logical.actor(),
                        InputClass::Lifecycle,
                        result_author.work(),
                    )
                    .unwrap(),
                )
            }));
        roots.binding().bind_reply_author(
            purposes[0]
                .position
                .as_ref()
                .unwrap()
                .event_author()
                .unwrap(),
        );
        roots.bind_ingress(&observation_source).unwrap();
        let mut ordered = ConstructionOriginal::new(
            OrderedObservationHost::new(observation_source).with_read_progress(progress_roles),
        );
        funding.bind_observation_exit(ordered.backing_exit().unwrap());
        let requests = AttemptRequests::new(
            activation,
            registration.clone(),
            logical.clone(),
            NonZeroUsize::new(2).unwrap(),
            wake.clone(),
        );
        let facts = {
            let author = input_loan
                .ap_author_from_link(&loan)
                .expect("same original activation author");
            AttemptFactBank::reserve(
                &runtime,
                &logical,
                &registration,
                activation,
                author,
                wake.clone(),
                logical_wake,
            )
            .unwrap_or_else(|original| {
                retain_construction(original);
                panic!("original fact binding refused");
            })
        };
        drop(input_loan);
        let storage = loan.reserve_port_storage().unwrap();
        let (control, reader) = SourcePublishedSlot::bind(
            &runtime,
            ControlBinding {
                lp: logical.clone(),
                ap: registration.clone(),
                activation,
                positions: positions.take(),
                storage,
                observer: OBSERVER,
                roots: roots.clone(),
            },
        )
        .unwrap_or_else(|original| {
            retain_construction(original);
            panic!("original control binding refused");
        });
        let mut reader = ConstructionOriginal::new(reader);
        let endpoints = AttemptPortEndpoints::bind(
            logical.clone(),
            registration.clone(),
            activation,
            startup,
            requests.clone(),
            facts.clone(),
            control,
            abort.clone(),
            roots,
        )
        .unwrap_or_else(|original| {
            retain_construction(original);
            panic!("same original endpoint pair refused");
        });
        let mut disposal = ConstructionOriginal::new({
            let (_, payer) = funding.parent();
            let guard = scope
                .reserve_child_next(
                    EPOCH,
                    payer.id(),
                    ReservationResources::new(
                        ResourceVector::default(),
                        OwnedSlotNeed {
                            work_headers: 1,
                            completions: 1,
                            captures: 1,
                            ..Default::default()
                        },
                    ),
                    WorkKind::Settlement,
                )
                .unwrap();
            super::super::funding::reported_step(guard, WorkKind::Settlement)
        });
        let source = ScriptSource {
            script: script.clone(),
            runtime: runtime.clone(),
            logical: logical.clone(),
            registration: registration.clone(),
            inputs: Arc::downgrade(&inputs),
            index: loan.index(),
            funding: funding.take(),
            ledger: ledger.clone(),
            source_author: Some(source_author.take()),
            result_author: Some(result_author.take()),
            startup: startup_source.take(),
            trigger: Some(trigger.take()),
            trigger_input: None,
            trigger_indexed: false,
            purposes: purposes.take(),
            pending: None,
            fact_loan: None,
            requests,
            facts,
            reader: reader.take(),
            abort,
            abort_retirement,
            endpoints: endpoints.clone(),
            closing: false,
            producer_exited: false,
            final_retired: false,
            wake: wake.clone(),
            disposal: Some(disposal.take()),
            ordered_close: None,
            ordered_closed: false,
            cursor: 0,
            inputs_closed: false,
            reader_closed: false,
            abort_closed: false,
            dirty,
            remaining: 8,
            rescan: false,
        };
        let exit = host
            .register(HostBinding {
                scope,
                registration: registration.clone(),
                kind: FrontendActorKind::Test,
                config,
                policy: FrontendLanePolicy::attempt(1),
                source: Box::new(source),
                executor: Arc::new(RejectExecutor),
                transport: None,
                ordered_observations: Some(ordered.take()),
                startup: None,
                capacity: None,
                membership: None,
                owner_controls: None,
            })
            .unwrap_or_else(|original| {
                let reason = original.reason;
                retain_construction(original);
                panic!("original script AP Host: {reason}");
            });
        // No startup fill or notify occurs before BOTH exact bindings exist.
        loan.bind_registered(&registration, &exit);
        loan.bind_endpoints(endpoints.clone());
        drop(loan);
        runtime.notify_host_actor(&registration).unwrap();
        AttemptCase {
            activation,
            script,
            registration,
            endpoints,
            exit: Arc::new(exit),
            _signal: signal,
            runtime,
            logical,
        }
    }
}
fn funding_observation_source(
    runtime: &ActorRuntime<FrontendActor>,
    registration: &ActorRegistration,
    roots: &Arc<RootControlRequests>,
    work: &WorkObligation,
    payer: &mut ReservationObligation,
    metadata: Arc<PaidObservationMetadata>,
    participations: &[novarocks_execution_contract::ParticipationRef],
    wake: &Waker,
    budget: ObservationBudget,
) -> Result<
    (
        OrderedObservationInput<FrontendActor>,
        Vec<crate::task_execution::host::observations::read_progress::ReadProgressInput>,
    ),
    crate::task_execution::host::observations::ObservationError,
> {
    let reserve = |class| {
        runtime
            .reserve_admitted_input_for(registration, registration.actor(), class, work)
            .unwrap()
            .into_standing_input()
            .unwrap_or_else(|_| panic!("original ordered script AP role"))
    };
    let ordinary = reserve(InputClass::Ordinary);
    let control = reserve(InputClass::Control);
    let credits = ObservationCredits::bind(
        budget,
        participations.iter().copied(),
        work,
        payer,
        Some(metadata.pin()),
    )?;
    let progress: Vec<_> = participations
        .iter()
        .copied()
        .map(|p| {
            crate::task_execution::host::observations::read_progress::ReadProgressInput::new(
                registration,
                p,
                reserve(InputClass::Ordinary),
            )
            .with_backing(Some(metadata.pin()))
        })
        .collect();
    let rows = participations
        .iter()
        .copied()
        .zip(progress.iter().cloned())
        .collect();
    let source = BoundObservationSource::new(
        roots.binding().clone(),
        roots.clone(),
        credits,
        rows,
        metadata,
    );
    OrderedObservationInput::new(
        ordinary,
        control,
        source,
        roots.binding().clone(),
        Arc::new(crate::task_execution::clock::ManualClock::new()),
        |input| FrontendInput::Attempt(Box::new(input)),
        wake.clone(),
    )
    .map(|input| (input, progress))
    .map_err(|(error, _, _)| error)
}
struct RejectExecutor;
impl EffectExecutor for RejectExecutor {
    fn handoff(&self, job: PreparedEffectJob) -> Result<(), PreparedEffectJob> {
        Err(job)
    }
}

struct Purpose {
    position: Option<CausalInputObligation<FrontendActor>>,
    custody: Option<ExitCustody>,
    report: Option<ReportSlot>,
    header: Option<WorkRetirementObserver>,
    readiness: Option<HeaderReadyRegistration>,
    join: Option<tokio::task::JoinHandle<CausalInputObligation<FrontendActor>>>,
    request: Option<AttemptRequestToken>,
    report_consumed: bool,
}
impl Purpose {
    fn new(position: CausalInputObligation<FrontendActor>) -> Self {
        Self {
            position: Some(position),
            custody: None,
            report: None,
            header: None,
            readiness: None,
            join: None,
            request: None,
            report_consumed: false,
        }
    }
    fn running(&self) -> bool {
        self.custody.is_some()
    }
}
struct OrderedClose {
    custody: ExitCustody,
    report: ReportSlot,
    header: WorkRetirementObserver,
    readiness: Option<HeaderReadyRegistration>,
    join: tokio::task::JoinHandle<()>,
    joined: bool,
    report_consumed: bool,
}
type FactPublisher = Box<
    dyn FnOnce(
            &ActorRuntime<FrontendActor>,
            &EffectTicket<FrontendEffect>,
            AttemptToLogical,
        ) -> Option<InputId>
        + Send,
>;
struct ScriptSource {
    script: AttemptScript,
    runtime: ActorRuntime<FrontendActor>,
    logical: ActorRegistration,
    registration: ActorRegistration,
    inputs: Weak<LogicalInputBank>,
    index: usize,
    funding: AttemptHostFunding,
    ledger: RuntimeLedger,
    source_author: Option<super::super::funding::SourceFundingObligation>,
    result_author: Option<super::super::funding::SourceFundingObligation>,
    startup: AttemptStartupSource,
    trigger: Option<StandingInputObligation<FrontendActor>>,
    trigger_input: Option<InputId>,
    trigger_indexed: bool,
    purposes: [Purpose; 3],
    pending: Option<usize>,
    fact_loan: Option<FactPublisher>,
    requests: Arc<AttemptRequests>,
    facts: Arc<AttemptFactBank>,
    reader: ControlReaderObligation,
    abort: Arc<AttemptAbortIngress>,
    abort_retirement: AbortIngressRetirement,
    endpoints: Arc<AttemptPortEndpoints>,
    closing: bool,
    producer_exited: bool,
    final_retired: bool,
    wake: Waker,
    disposal: Option<novarocks_control_runtime::reported_work::ReportedWork>,
    ordered_close: Option<OrderedClose>,
    ordered_closed: bool,
    cursor: usize,
    inputs_closed: bool,
    reader_closed: bool,
    abort_closed: bool,
    dirty: Arc<AtomicBool>,
    remaining: usize,
    rescan: bool,
}
fn role_index(message: &AttemptToLogical) -> usize {
    use crate::task_execution::host::port::facts::FactRole;
    match role_for(&message.body) {
        FactRole::Lifecycle => 0,
        FactRole::Authorization => 1,
        FactRole::PreReady => 2,
    }
}
fn request_token(message: &AttemptToLogical) -> Option<AttemptRequestToken> {
    match message.body {
        AttemptToLogicalBody::AuthorizationRequest { token, .. }
        | AttemptToLogicalBody::EstablishSettlement { token, .. }
        | AttemptToLogicalBody::PreReadyReplacementQuery { token, .. } => Some(token),
        _ => None,
    }
}
fn spawn_job(
    job: PreparedEffectJob,
    wake: Waker,
) -> tokio::task::JoinHandle<CausalInputObligation<FrontendActor>> {
    tokio::spawn(async move {
        let PreparedEffectJob {
            effect,
            exit,
            result,
            captures,
            transport,
        } = job;
        assert!(transport.is_none());
        run_future_in_generation(
            exit,
            async move {
                drop(effect);
                drop(captures);
            },
            PanicSink::new(),
            EPOCH,
        )
        .await
        .unwrap();
        wake.wake_by_ref();
        result
    })
}
impl ScriptSource {
    fn retire_purpose(&mut self, index: usize) -> bool {
        use std::{
            future::Future,
            task::{Context, Poll},
        };
        let mut progressed = false;
        if self
            .runtime
            .snapshot(self.registration.actor())
            .unwrap()
            .failed
        {
            let proof = self.runtime.failure_proof_for(&self.registration).unwrap();
            if let Some(original) = self
                .requests
                .take_one_after_attempt_failure(&proof)
                .unwrap()
            {
                let purpose = self
                    .purposes
                    .iter_mut()
                    .find(|p| p.request == Some(original.token))
                    .expect("same original request purpose");
                purpose.request = None;
                purpose.join = Some(spawn_job(original.job, self.wake.clone()));
                progressed = true;
            }
        }
        let purpose = &mut self.purposes[index];
        if let Some(token) = purpose.request {
            let job = self.requests.take_replied(token).map(|same| same.job);
            if let Some(job) = job {
                purpose.request = None;
                purpose.join = Some(spawn_job(job, self.wake.clone()));
                progressed = true;
            }
        }
        if let Some(join) = purpose.join.as_mut()
            && let Poll::Ready(returned) =
                std::pin::Pin::new(join).poll(&mut Context::from_waker(&self.wake))
        {
            purpose.position = Some(returned.expect("actual original script job Join"));
            purpose.join = None;
            progressed = true;
        }
        if purpose.join.is_none() && purpose.request.is_none() && purpose.custody.is_some() {
            let position = purpose
                .position
                .as_mut()
                .expect("same original result returned after Join");
            if self
                .runtime
                .snapshot(self.registration.actor())
                .unwrap()
                .failed
            {
                // No new fill is attempted. The actual failed input, if any,
                // remains this original role's responsibility through dispose.
                match position.take_failed_payload() {
                    Ok(Some(original)) => {
                        original.dispose().unwrap();
                        progressed = true;
                    }
                    Ok(None) | Err(ActorTurnError::InputAuthorMismatch) => {}
                    Err(error) => panic!("original failed result payload: {error:?}"),
                }
            }
            if position.retire_ready_after_exit().unwrap() {
                let report = purpose.report.as_ref().unwrap();
                if !purpose.report_consumed {
                    report.consume(OBSERVER).unwrap();
                    purpose.report_consumed = true;
                    progressed = true;
                }
                let custody = purpose.custody.as_ref().unwrap();
                let already_retired = custody.phase() == ExitCustodyPhase::Retired;
                let newly_retired = !already_retired && custody.try_retire().unwrap();
                progressed |= newly_retired;
                if (already_retired || newly_retired)
                    && self
                        .funding
                        .scope()
                        .reap_observed_header(purpose.header.as_ref().unwrap())
                        .unwrap()
                {
                    if let Some(mut readiness) = purpose.readiness.take() {
                        readiness.cancel();
                    }
                    purpose.custody = None;
                    purpose.report = None;
                    purpose.header = None;
                    purpose.report_consumed = false;
                    progressed = true;
                } else {
                    purpose
                        .readiness
                        .as_mut()
                        .expect("same original pending job header registration")
                        .arm(
                            &self.runtime,
                            &self.registration,
                            purpose.header.as_ref().unwrap(),
                        )
                        .expect("arm same original script job header");
                }
            }
        }
        progressed
    }
    fn progress_trigger(&mut self, failed: bool) -> bool {
        let mut progressed = false;
        if self.trigger_input.is_some() && (self.trigger_indexed || failed) {
            let position = self.trigger.as_mut().unwrap();
            if failed && let Some(original) = position.take_failed_role_payload().unwrap() {
                original.dispose().unwrap();
            }
            if position.retire_ready_after_exit().unwrap() {
                self.trigger_input = None;
                self.trigger_indexed = false;
                progressed = true;
            }
        }
        if self.trigger_input.is_none()
            && !self.producer_exited
            && !failed
            && self.startup.is_ready()
        {
            let pending = {
                let mut state = self.script.0.lock().unwrap();
                let pending = state.ping_requested || !state.queue.is_empty();
                state.ping_requested = false;
                pending
            };
            if pending {
                self.trigger_input = Some(
                    self.trigger
                        .as_ref()
                        .unwrap()
                        .fill(FrontendInput::Test(0))
                        .unwrap(),
                );
                progressed = true;
            }
        }
        progressed
    }
}
impl EffectSource for ScriptSource {
    fn prepay_result(
        &mut self,
        effect: &FrontendEffect,
    ) -> Option<CausalInputObligation<FrontendActor>> {
        let FrontendEffect::Attempt(effect) = effect else {
            panic!("script original protocol effect");
        };
        let state = self.script.0.lock().unwrap();
        let message = state
            .original
            .get(&effect.protocol.id)
            .expect("same original scripted message");
        let index = role_index(message);
        if self.purposes[index].running() {
            return None;
        }
        if request_token(message).is_some() && !self.requests.has_capacity() {
            return None;
        }
        let role = role_for(&message.body);
        let publish: FactPublisher = if self
            .runtime
            .snapshot(self.logical.actor())
            .is_ok_and(|s| s.failed)
        {
            let proof = self.runtime.failure_proof_for(&self.logical).unwrap();
            let mut loan = self
                .facts
                .try_failed_receiver_loan(&self.runtime, &proof, role)
                .unwrap()?;
            Box::new(move |runtime, ticket, message| {
                loan.publish_now(runtime, ticket, &proof, message).unwrap();
                None
            })
        } else {
            let mut loan = self.facts.try_loan(role)?;
            Box::new(move |runtime, ticket, message| {
                Some(loan.publish_now(runtime, ticket, message).unwrap())
            })
        };
        let result = self.purposes[index].position.take()?;
        assert!(self.pending.replace(index).is_none());
        self.fact_loan = Some(publish);
        Some(result)
    }
    fn bind_job(
        &mut self,
        _: &ActorRuntime<FrontendActor>,
        _: &ActorHostTurn<FrontendActor>,
        _: &EffectTicket<FrontendEffect>,
        _: CausalInputObligation<FrontendActor>,
    ) -> PreparedEffectJob {
        unreachable!("script ports bind their original retained jobs under the current ticket")
    }
    fn try_bind_effect(
        &mut self,
        runtime: &ActorRuntime<FrontendActor>,
        _: &ActorHostTurn<FrontendActor>,
        ticket: &EffectTicket<FrontendEffect>,
        result: CausalInputObligation<FrontendActor>,
    ) -> Result<PreparedEffectBinding, EffectBindRefusal> {
        let index = self.pending.take().unwrap();
        let (message, reservation) = {
            let mut state = self.script.0.lock().unwrap();
            let FrontendEffect::Attempt(effect) = &ticket.effect else {
                unreachable!()
            };
            let action = effect.protocol.id;
            (
                state.original.remove(&action).unwrap(),
                state.reservations.remove(&action).unwrap(),
            )
        };
        let guard = runtime.take_step_reservation(ticket, reservation).unwrap();
        let mut original = super::super::funding::reported_step(guard, WorkKind::Settlement);
        let readiness = original
            .header_readiness_registration()
            .expect("same original script job header readiness");
        let report = original.report().clone();
        let header = original.work().retirement_observer();
        let (custody, exit) = ExitCustody::from_admitted(original);
        let job = PreparedEffectJob {
            effect: ticket.effect.clone(),
            exit,
            result,
            captures: Box::new(()),
            transport: None,
        };
        let purpose = &mut self.purposes[index];
        assert!(purpose.custody.replace(custody).is_none());
        purpose.report = Some(report);
        purpose.header = Some(header);
        assert!(
            purpose.readiness.replace(readiness).is_none(),
            "one original job header registration"
        );
        let request = request_token(&message);
        // The exact rich original and exit authority enter their real table
        // BEFORE this same AP ticket publishes its request to the LP.
        if let Some(token) = request {
            self.requests.insert(&message, job).unwrap();
            purpose.request = Some(token);
        } else {
            purpose.join = Some(spawn_job(job, self.wake.clone()));
        }
        let publish = self.fact_loan.take().unwrap();
        let input = publish(runtime, ticket, message.clone());
        if matches!(
            message.body,
            AttemptToLogicalBody::SealResult { .. }
                | AttemptToLogicalBody::TerminalControlResult { .. }
        ) {
            self.endpoints.roots.reply_delivered(&message).unwrap();
        }
        let mut state = self.script.0.lock().unwrap();
        state.published.push(message.action);
        if let Some(input) = input {
            state.fact_inputs.push(input);
        }
        drop(state);
        Ok(PreparedEffectBinding::Retained)
    }
    fn return_unclaimed_result(&mut self, result: CausalInputObligation<FrontendActor>) {
        let index = self.pending.take().unwrap();
        assert!(self.purposes[index].position.replace(result).is_none());
        self.fact_loan = None;
    }
    fn core_consumed(&mut self, inputs: &[InputId]) {
        self.startup.indexed(inputs);
        self.reader.index_committed(inputs).unwrap();
        if self.trigger_input.is_some_and(|id| inputs.contains(&id)) {
            self.trigger_indexed = true;
        }
        if !inputs.is_empty() {
            if self.remaining == 0 {
                self.remaining = 8;
            } else {
                self.rescan = true;
            }
            // The common Host indexes committed inputs after this Source's
            // paid retirement visit. This real consumption therefore owes a
            // new visit; an empty scan never creates a wake or resets a pass.
            self.wake.wake_by_ref();
        }
    }
    fn owner_failed_pending(&mut self, pending: PendingHandoff) {
        std::panic::resume_unwind(Box::new(pending));
    }
    fn close_external(&mut self) {
        if !self.closing {
            self.closing = true;
            self.script.0.lock().unwrap().closing = true;
            // The original Host close notification may reach an idle Source
            // without going through AttemptWake. Its real first close owes
            // one finite pass in this already-running Host visit.
            if self.remaining == 0 {
                self.remaining = 8;
            } else {
                self.rescan = true;
            }
        }
    }
    fn result_admission_closed(&self) -> bool {
        self.producer_exited
    }
    fn can_close_ordered_observations(&self, _: bool) -> bool {
        self.producer_exited
            && self.reader_closed
            && self.facts.is_closed()
            && self.purposes.iter().all(|p| !p.running())
    }
    fn close_ordered_observations(&mut self, original: OrderedObservationHost) {
        let mut paid = self
            .disposal
            .take()
            .expect("same prepaid ordered disposal Work");
        let readiness = paid
            .header_readiness_registration()
            .expect("same original ordered disposal header readiness");
        let report = paid.report().clone();
        let header = paid.work().retirement_observer();
        let (custody, exit) = ExitCustody::from_admitted(paid);
        let wake = self.wake.clone();
        let bank = self
            .inputs
            .upgrade()
            .expect("issued AP keeps its original input bank alive");
        let index = self.index;
        let join = tokio::spawn(async move {
            run_future_in_generation(
                exit,
                async move {
                    original.close_on_executor();
                    let mut original = bank
                        .loan()
                        .await
                        .expect("issued AP author has not retired before actual callback");
                    original
                        .ap_inputs_closed_after_producer_exit(index)
                        .unwrap();
                },
                PanicSink::new(),
                EPOCH,
            )
            .await
            .unwrap();
            wake.wake_by_ref();
        });
        self.ordered_close = Some(OrderedClose {
            custody,
            report,
            header,
            readiness: Some(readiness),
            join,
            joined: false,
            report_consumed: false,
        });
    }
    fn progress_retirement(
        &mut self,
        runtime: &ActorRuntime<FrontendActor>,
        turn: &ActorHostTurn<FrontendActor>,
        _: Tick,
        budget: usize,
    ) -> SourceProgress {
        self.progress_originals(runtime, turn, budget)
    }
}

impl ScriptSource {
    fn progress_originals(
        &mut self,
        runtime: &ActorRuntime<FrontendActor>,
        turn: &ActorHostTurn<FrontendActor>,
        budget: usize,
    ) -> SourceProgress {
        use std::{
            future::Future,
            task::{Context, Poll},
        };
        if self.final_retired {
            return SourceProgress {
                units: 0,
                progressed: false,
                more: false,
                retired: true,
            };
        }
        let failed = runtime.snapshot(self.registration.actor()).unwrap().failed;
        let logical_failed = runtime
            .snapshot(self.logical.actor())
            .map(|s| s.failed)
            .unwrap_or(true);
        // A real original header notification is one paid metadata visit.
        // Host self-yields do not pass through AttemptWake and cannot reset
        // an otherwise idle pass. Never consume a header at zero budget.
        let header_units = usize::from(budget != 0 && !turn.take_header_readiness(1).is_empty());
        let mut progressed = header_units != 0;
        let mut more = false;
        let mut retired = false;
        if budget != 0 && (self.dirty.swap(false, Ordering::AcqRel) || header_units != 0) {
            self.reader.begin_pass();
            if self.remaining == 0 {
                self.remaining = 8;
            } else {
                self.rescan = true;
            }
        }
        let row_units = budget.saturating_sub(header_units).min(self.remaining);
        let units = header_units + row_units;
        for _ in 0..row_units {
            let stage = self.cursor;
            self.cursor = (self.cursor + 1) % 8;
            self.remaining -= 1;
            match stage {
                0 => {
                    if !self.startup.is_closed() {
                        more |= self.startup.progress(failed, self.closing).unwrap();
                    }
                }
                1 => {
                    if !self.producer_exited || self.trigger_input.is_some() {
                        progressed |= self.progress_trigger(failed);
                    }
                }
                2 => {
                    if failed && !self.reader_closed {
                        let proof = runtime.failure_proof_for(&self.registration).unwrap();
                        self.reader_closed =
                            self.reader.close_after_failure(runtime, &proof).unwrap();
                        progressed |= self.reader_closed;
                    } else if !self.reader_closed {
                        let p = self.reader.progress_ordered(1).unwrap();
                        progressed |= p.progressed;
                        more |= p.more;
                    }
                }
                3 => {
                    let p = self
                        .facts
                        .progress(1, self.producer_exited, logical_failed)
                        .unwrap();
                    progressed |= p.progressed;
                    more |= p.more;
                }
                4..=6 => {
                    progressed |= self.retire_purpose(stage - 4);
                }
                7 => {
                    let quiescent = {
                        let state = self.script.0.lock().unwrap();
                        state.queue.is_empty()
                            && state.original.is_empty()
                            && state.reservations.is_empty()
                    };
                    if (self.closing || failed) && quiescent && !self.producer_exited {
                        self.producer_exited = true;
                        self.endpoints.original_producer_exited();
                        self.abort.close_external();
                        self.requests.close_admission();
                        progressed = true;
                    }
                    if self.producer_exited && self.purposes.iter().all(|p| !p.running()) {
                        if let Some(position) = self.trigger.as_mut()
                            && self.trigger_input.is_none()
                        {
                            position.close_after_exit().unwrap();
                            self.trigger = None;
                            progressed = true;
                        }
                        for purpose in &mut self.purposes {
                            if let Some(position) = purpose.position.as_mut() {
                                position.close_after_exit().unwrap();
                                purpose.position = None;
                                progressed = true;
                            }
                        }
                        match self.abort_retirement.progress().unwrap() {
                            AbortIngressRetirementProgress::Advanced => {
                                progressed = true;
                                more = true;
                            }
                            AbortIngressRetirementProgress::Blocked => {}
                            AbortIngressRetirementProgress::Complete => {
                                if !self.abort_closed {
                                    self.abort_closed = true;
                                    progressed = true;
                                }
                            }
                        }
                        if !failed && !self.reader_closed {
                            self.reader_closed = self.reader.close_after_exit(runtime).unwrap();
                            progressed |= self.reader_closed;
                        }
                        if let Some(close) = &mut self.ordered_close {
                            if !close.joined
                                && let Poll::Ready(joined) = std::pin::Pin::new(&mut close.join)
                                    .poll(&mut Context::from_waker(&self.wake))
                            {
                                joined.expect("original ordered disposal Join");
                                close.joined = true;
                                self.inputs_closed = true;
                                progressed = true;
                            }
                            if close.joined {
                                if !close.report_consumed {
                                    close.report.consume(OBSERVER).unwrap();
                                    close.report_consumed = true;
                                    progressed = true;
                                }
                                let already_retired =
                                    close.custody.phase() == ExitCustodyPhase::Retired;
                                let newly_retired =
                                    !already_retired && close.custody.try_retire().unwrap();
                                progressed |= newly_retired;
                                if (already_retired || newly_retired)
                                    && self
                                        .funding
                                        .scope()
                                        .reap_observed_header(&close.header)
                                        .unwrap()
                                {
                                    if let Some(mut readiness) = close.readiness.take() {
                                        readiness.cancel();
                                    }
                                    self.ordered_closed = true;
                                    self.ordered_close = None;
                                    progressed = true;
                                } else {
                                    close
                                        .readiness
                                        .as_mut()
                                        .expect("same original pending ordered header registration")
                                        .arm(runtime, &self.registration, &close.header)
                                        .expect("arm same original ordered disposal header");
                                }
                            }
                        }
                        if self.trigger.is_none()
                            && self.purposes.iter().all(|p| p.position.is_none())
                            && self.ordered_closed
                            && self.inputs_closed
                            && self.reader_closed
                            && self.abort_closed
                            && self.startup.is_closed()
                        {
                            if let Some(author) = &mut self.source_author
                                && author.progress(runtime, &self.registration).unwrap()
                            {
                                self.source_author = None;
                                progressed = true;
                            }
                            if let Some(author) = &mut self.result_author
                                && author.progress(runtime, &self.registration).unwrap()
                            {
                                self.result_author = None;
                                progressed = true;
                            }
                            if self.source_author.is_none() && self.result_author.is_none() {
                                match self
                                    .funding
                                    .progress(&self.ledger, runtime, &turn.registration())
                                    .unwrap()
                                {
                                    FundingProgress::Waiting => {}
                                    FundingProgress::More => {
                                        progressed = true;
                                        more = true;
                                    }
                                    FundingProgress::Retired => {
                                        self.final_retired = true;
                                        retired = true;
                                        progressed = true;
                                    }
                                }
                            }
                        }
                    }
                }
                _ => unreachable!(),
            }
        }
        // A bounded continuation is owed only for real transitions or a
        // partial reader/bank pass, never merely because a source is busy.
        self.rescan |= more || progressed;
        if self.remaining == 0 && self.rescan && !retired {
            self.remaining = 8;
            self.rescan = false;
        }
        SourceProgress {
            units,
            progressed,
            more: !retired && self.remaining != 0,
            retired,
        }
    }
}
