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

//! A paid reply lane for one original attempt port.

use super::{
    ActorTransportWork, BodyOutcome, OriginalQueuedInput, covered::SubscriptionEndpoints,
    prepare_actor_transport, process_observation::AttemptProcessObserver, queued_job::CreationSeed,
};
use crate::{
    control_runtime::{
        actor::{FrontendActor, FrontendEffect, FrontendInput},
        host::PreparedEffectJob,
        transport_withdrawal::AttemptEffectEnvelope,
    },
    task_execution::{
        bridge::{
            abort::{
                AbortInputRetirement, AbortReceiptSlot, AbortSettlementOwner,
                AppliedAbortSettlement,
            },
            qa::AttemptQaBridge,
            terminal::{ConvergencePublication, TerminalOutbox, TerminalPublication},
        },
        host::{
            custody::ReturnedActorTransportResult,
            observations::{
                ObservationError, OrderedObservationInput, ReadConfirmationBorrow, StreamKey,
            },
            publication::{AttemptPublicationOwner, OriginalHostFailure},
        },
    },
};
use novarocks_attempt_protocol::{
    effect::EffectBody, failure::HostFailureRef, input::AttemptInput,
};
use novarocks_control_runtime::{
    actor_turn::{ActorTurnError, EffectTicket},
    causal_input::{CausalFillFailure, CausalInputObligation},
    completion::ReportSlot,
    exit_custody::ExitCustody,
    obligation::{ReservationObligation, WorkObligation, WorkRetirementObserver},
    reported_work::ReportedWork,
    transaction::{RuntimeError, ScopeHandle},
};
use novarocks_control_runtime_core::{
    capacity::{CapacityError, ReservationResources, ResourceVector},
    completion::ConsumerKind,
    identity::{ConsumerId, Generation, ReservationId},
    owned_storage::OwnedSlotNeed,
    scope::{ScopeError, WorkKind},
};
use novarocks_coordination_contract::{
    AttemptActionId, AttemptAuthorization, AttemptToLogical, AttemptToLogicalBody,
};
use std::{
    collections::{BTreeMap, VecDeque},
    sync::Arc,
};
use tokio::sync::Mutex;

type Position = CausalInputObligation<FrontendActor>;

#[derive(Clone)]
pub(crate) enum AttemptCallOwner {
    Qa(Arc<Mutex<AttemptQaBridge>>),
    Process(Arc<AttemptProcessObserver>),
    Publication(Arc<AttemptPublicationOwner>),
    Release,
    ReadConfirmation(OrderedObservationInput<FrontendActor>),
    SubscriptionOpen,
    SubscriptionControl(Arc<SubscriptionEndpoints>),
}

impl AttemptCallOwner {
    fn matches(&self, effect: &AttemptEffectEnvelope) -> bool {
        match self {
            Self::Qa(_) => effect.is_qa_call(),
            Self::Process(_) => effect.is_process_call(),
            Self::Publication(_) => effect.is_publication_call(),
            Self::Release => effect.is_transport_release(),
            Self::ReadConfirmation(_) => effect.is_read_confirmation(),
            Self::SubscriptionOpen => effect.is_subscription_open(),
            Self::SubscriptionControl(_) => effect.is_subscription_control(),
        }
    }
}

pub(crate) enum AttemptCallReply {
    Input {
        input: AttemptInput,
        failure: Option<(HostFailureRef, OriginalHostFailure)>,
    },
    Terminal(Box<TerminalPublication>),
    Convergence(ConvergencePublication),
    AbortSettled(AppliedAbortSettlement),
    NoInput,
}

pub(super) enum TransportRelease {
    QueuedInput(OriginalQueuedInput),
    Creation {
        request: Option<crate::task_execution::intent::OperationIntent>,
        seed: CreationSeed,
    },
}
impl TransportRelease {
    fn dispose(self) {
        match self {
            Self::QueuedInput(original) => drop(original),
            Self::Creation { request, seed } => {
                drop(request);
                drop(seed);
            }
        }
    }
}

pub(crate) struct BoundAttemptCall {
    owner: AttemptCallOwner,
    abort: Option<(Arc<AbortSettlementOwner>, Option<Arc<AbortReceiptSlot>>)>,
    release: Option<TransportRelease>,
    confirmation: Option<Result<ReadConfirmationBorrow<FrontendActor>, ObservationError>>,
}
impl BoundAttemptCall {
    pub(super) fn matches(&self, effect: &AttemptEffectEnvelope) -> bool {
        self.owner.matches(effect)
    }

    pub(super) fn dispose(self) {
        assert!(matches!(self.owner, AttemptCallOwner::Release));
        if let Some(original) = self.release {
            original.dispose();
        }
    }

    pub(super) async fn run(
        self,
        effect: Arc<AttemptEffectEnvelope>,
    ) -> Result<AttemptCallReply, Box<dyn std::any::Any + Send>> {
        if let Some((owner, receipt)) = self.abort {
            let EffectBody::Port(port) = &effect.protocol.body else {
                unreachable!("original Abort settlement port");
            };
            assert_eq!(port.action, effect.protocol.id);
            return owner
                .apply(port, receipt.as_deref())
                .map(AttemptCallReply::AbortSettled)
                .map_err(|error| Box::new(error) as Box<dyn std::any::Any + Send>);
        }
        match self.owner {
            AttemptCallOwner::Qa(bridge) => {
                let mut bridge = bridge.lock().await;
                if let EffectBody::Port(port) = &effect.protocol.body
                    && matches!(
                        port.body,
                        AttemptToLogicalBody::AttemptTerminal(_)
                            | AttemptToLogicalBody::ConvergenceProgress { .. }
                    )
                {
                    if port.activation != bridge.activation() || port.action != effect.protocol.id {
                        return Err(Box::new(
                            crate::task_execution::bridge::qa::AttemptQaPortError::WrongRequest,
                        ));
                    }
                    return Ok(match &port.body {
                        AttemptToLogicalBody::AttemptTerminal(_) => AttemptCallReply::Terminal(
                            Box::new(TerminalPublication::new(port.clone())),
                        ),
                        AttemptToLogicalBody::ConvergenceProgress {
                            local_exited,
                            closure,
                        } => AttemptCallReply::Convergence(ConvergencePublication {
                            activation: port.activation,
                            action: port.action,
                            local_exited: *local_exited,
                            closure: closure.clone(),
                        }),
                        _ => unreachable!("validated original logical publication"),
                    });
                }
                let reply = match &effect.protocol.body {
                    EffectBody::EstablishAuthorization { operation, request } => {
                        bridge.authorize(operation, request).await
                    }
                    EffectBody::Port(port) => bridge.handle_port(port).await,
                    _ => unreachable!("same original call effect"),
                }
                .map_err(|error| Box::new(error) as Box<dyn std::any::Any + Send>)?;
                Ok(AttemptCallReply::Input {
                    input: AttemptInput::Port(reply.reply),
                    failure: reply.failure.map(|(reference, original)| {
                        (reference, OriginalHostFailure::Task(original))
                    }),
                })
            }
            AttemptCallOwner::Process(observer) => {
                let EffectBody::ObserveProcess {
                    request,
                    participation,
                } = effect.protocol.body
                else {
                    unreachable!("same original Process effect");
                };
                let sample = observer
                    .read(request, participation)
                    .map_err(|error| Box::new(error) as Box<dyn std::any::Any + Send>)?;
                Ok(AttemptCallReply::Input {
                    input: AttemptInput::Executor(sample),
                    failure: None,
                })
            }
            AttemptCallOwner::Publication(owner) => match &effect.protocol.body {
                EffectBody::Publish(publication) => {
                    let (input, failure) = owner.publish(effect.protocol.id, publication).await;
                    Ok(AttemptCallReply::Input { input, failure })
                }
                EffectBody::Port(port) => owner
                    .deliver_root_reply(port)
                    .await
                    .map(|result| match result {
                        Some((input, failure)) => AttemptCallReply::Input { input, failure },
                        None => AttemptCallReply::NoInput,
                    })
                    .map_err(|error| Box::new(error) as Box<dyn std::any::Any + Send>),
                _ => unreachable!("same original publication effect"),
            },
            AttemptCallOwner::ReadConfirmation(_) => {
                let EffectBody::ConfirmReadState {
                    token,
                    requested_at,
                    ..
                } = effect.protocol.body
                else {
                    unreachable!("original read confirmation effect");
                };
                let snapshot = match self.confirmation.expect("bound original read confirmation") {
                    Ok(original) => original.snapshot(token, requested_at).await,
                    Err(error) => Err(error),
                };
                let snapshot = match snapshot {
                    Ok(snapshot) => snapshot,
                    Err(ObservationError::Closed) => return Ok(AttemptCallReply::NoInput),
                    Err(error) => return Err(Box::new(error)),
                };
                Ok(AttemptCallReply::Input {
                    input: AttemptInput::Executor(
                        novarocks_attempt_protocol::input::ExecutorFact::ReadState(snapshot),
                    ),
                    failure: None,
                })
            }
            AttemptCallOwner::SubscriptionControl(original) => {
                super::covered::control(&original, &effect.protocol.body)
                    .map_err(|error| Box::new(error) as Box<dyn std::any::Any + Send>)?;
                Ok(AttemptCallReply::NoInput)
            }
            AttemptCallOwner::SubscriptionOpen => {
                unreachable!("original subscription generation carrier")
            }
            AttemptCallOwner::Release => unreachable!("transport release uses blocking Work"),
        }
    }
}

struct Live {
    effect: Arc<AttemptEffectEnvelope>,
    header: WorkRetirementObserver,
    result: Option<Position>,
    abort_input: Option<AbortInputRetirement>,
    retired: bool,
}

#[must_use = "retain the original call result, input credit, and refused publication"]
pub(crate) struct AttemptCallResultRefusal {
    pub(crate) original: ReturnedActorTransportResult,
    pub(crate) publication: Option<CausalFillFailure<FrontendInput>>,
}

pub(crate) struct AttemptCallSource {
    scope: ScopeHandle,
    generation: Generation,
    parent: ReservationId,
    observer: ConsumerId,
    owner: Option<AttemptCallOwner>,
    terminal: Option<Arc<TerminalOutbox>>,
    abort: Option<Arc<AbortSettlementOwner>>,
    idle: Option<Position>,
    pending: Option<(Arc<AttemptEffectEnvelope>, ReportedWork)>,
    live: Option<Live>,
    failures: BTreeMap<HostFailureRef, OriginalHostFailure>,
    refused_root_input: Option<CausalFillFailure<FrontendInput>>,
}

impl AttemptCallSource {
    pub(crate) fn new(
        parent: &WorkObligation,
        payer: &ReservationObligation,
        generation: Generation,
        observer: ConsumerId,
        owner: AttemptCallOwner,
        position: Position,
    ) -> Self {
        let scope = parent.scope().clone();
        assert!(payer.belongs_to_bound_work(&scope, parent.id()));
        Self {
            scope,
            generation,
            parent: payer.id(),
            observer,
            owner: Some(owner),
            terminal: None,
            abort: None,
            idle: Some(position),
            pending: None,
            live: None,
            failures: BTreeMap::new(),
            refused_root_input: None,
        }
    }

    pub(super) fn commit_root_reply(
        &self,
        runtime: &novarocks_control_runtime::actor_turn::ActorRuntime<FrontendActor>,
        ticket: &EffectTicket<FrontendEffect>,
        port: &AttemptToLogical<novarocks_attempt_protocol::failure::AttemptFailure>,
        projection: Option<(
            HostFailureRef,
            novarocks_query_application::execution_error::ExecutionFailureInput,
        )>,
    ) -> Result<
        (),
        (
            ActorTurnError,
            Arc<AttemptToLogical<novarocks_attempt_protocol::failure::AttemptFailure>>,
        ),
    > {
        let Some(AttemptCallOwner::Publication(owner)) = &self.owner else {
            unreachable!("original publication Source");
        };
        owner.commit_root_reply(runtime, ticket, Arc::new(port.clone()), projection)
    }

    pub(super) fn project_failure(
        &self,
        reference: HostFailureRef,
    ) -> Option<novarocks_query_application::execution_error::ExecutionFailureInput> {
        self.failures
            .get(&reference)
            .map(OriginalHostFailure::preparation_input)
    }

    pub(crate) fn install_terminal(&mut self, original: Arc<TerminalOutbox>) {
        assert!(matches!(self.owner, Some(AttemptCallOwner::Qa(_))));
        assert!(self.terminal.replace(original).is_none());
    }

    pub(crate) fn install_abort(&mut self, original: Arc<AbortSettlementOwner>) {
        assert!(matches!(self.owner, Some(AttemptCallOwner::Qa(_))));
        assert!(self.abort.replace(original).is_none());
    }

    pub(super) fn close_abort_ingress(&self) {
        if let Some(original) = &self.abort {
            original.close_external();
        }
    }

    pub(super) fn matches_scope(&self, scope: &ScopeHandle) -> bool {
        self.scope.same_scope(scope)
    }

    pub(crate) fn prepay(
        &mut self,
        effect: &Arc<AttemptEffectEnvelope>,
    ) -> Result<Option<Position>, RuntimeError> {
        assert!(
            self.owner
                .as_ref()
                .is_some_and(|owner| owner.matches(effect))
        );
        assert!(self.pending.is_none());
        if self.idle.is_none() {
            return Ok(None);
        }
        let kind = match effect.protocol.body {
            EffectBody::ReleaseQueuedInput { .. } | EffectBody::ReleaseCreationPayload { .. } => {
                WorkKind::Cleanup
            }
            EffectBody::EstablishAuthorization { .. }
            | EffectBody::Port(AttemptToLogical {
                body:
                    AttemptToLogicalBody::AuthorizationRequest {
                        request:
                            AttemptAuthorization::BeginAdmission(_)
                            | AttemptAuthorization::ReauthorizeEstablish { .. },
                        ..
                    },
                ..
            }) => WorkKind::Business,
            _ => WorkKind::Settlement,
        };
        let work = match self.reserve_work(self.generation, kind) {
            Ok(work) => work,
            Err(
                RuntimeError::Scope(error) | RuntimeError::Capacity(CapacityError::Scope(error)),
            ) if matches!(
                error,
                ScopeError::Failed | ScopeError::GenerationMismatch | ScopeError::Stopped
            ) =>
            {
                return Ok(None);
            }
            Err(error) => return Err(error),
        };
        self.pending = Some((effect.clone(), work));
        Ok(self.idle.take())
    }

    pub(super) fn reserve_disposal(&self) -> Result<ReportedWork, RuntimeError> {
        self.reserve_work(self.scope.snapshot()?.generation, WorkKind::Cleanup)
    }

    pub(super) fn observer(&self) -> ConsumerId {
        self.observer
    }

    pub(super) fn reap_disposal(
        &self,
        header: &novarocks_control_runtime::obligation::WorkRetirementObserver,
    ) -> Result<bool, RuntimeError> {
        self.scope.reap_observed_header(header)
    }

    fn reserve_work(
        &self,
        generation: Generation,
        kind: WorkKind,
    ) -> Result<ReportedWork, RuntimeError> {
        let resources = ReservationResources::new(
            ResourceVector::default(),
            OwnedSlotNeed {
                work_headers: 1,
                completions: 1,
                captures: 1,
                ..Default::default()
            },
        );
        let guard = self
            .scope
            .reserve_child_next(generation, self.parent, resources, kind)?;
        let (work, mut payer) = guard.bind(kind).expect("original call child guard");
        let report = ReportSlot::reserve_next(
            &mut payer,
            generation,
            1,
            [(self.observer, ConsumerKind::Observer)],
        )
        .expect("original call exit report");
        Ok(ReportedWork::try_new(work, payer, report).expect("same original call work"))
    }

    pub(crate) fn bind(
        &mut self,
        ticket: &EffectTicket<FrontendEffect>,
        result: Position,
    ) -> PreparedEffectJob {
        self.bind_with_abort_receipt(ticket, result, None)
    }

    pub(super) fn bind_with_abort_receipt(
        &mut self,
        ticket: &EffectTicket<FrontendEffect>,
        result: Position,
        receipt: Option<Arc<AbortReceiptSlot>>,
    ) -> PreparedEffectJob {
        self.bind_with_captures(ticket, result, receipt, || None)
    }

    pub(super) fn bind_with_release(
        &mut self,
        ticket: &EffectTicket<FrontendEffect>,
        result: Position,
        take_original: impl FnOnce() -> Option<TransportRelease>,
    ) -> PreparedEffectJob {
        assert!(
            self.owner
                .as_ref()
                .is_some_and(|owner| matches!(owner, AttemptCallOwner::Release))
        );
        self.bind_with_captures(ticket, result, None, take_original)
    }

    fn bind_with_captures(
        &mut self,
        ticket: &EffectTicket<FrontendEffect>,
        result: Position,
        receipt: Option<Arc<AbortReceiptSlot>>,
        take_original: impl FnOnce() -> Option<TransportRelease>,
    ) -> PreparedEffectJob {
        let (effect, work) = self.begin_binding(ticket);
        let owner = self.owner.as_ref().expect("original call owner").clone();
        let abort = effect.is_abort_settlement().then(|| {
            (
                self.abort
                    .as_ref()
                    .expect("original Abort settlement owner")
                    .clone(),
                receipt,
            )
        });
        prepare_actor_transport(effect, result, work, VecDeque::new(), self.observer, |_| {
            let confirmation = match (&owner, &ticket.effect) {
                (AttemptCallOwner::ReadConfirmation(ingress), FrontendEffect::Attempt(effect)) => {
                    let EffectBody::ConfirmReadState {
                        participation,
                        generation,
                        ..
                    } = effect.protocol.body
                    else {
                        unreachable!("original read confirmation effect");
                    };
                    Some(ingress.borrow_read_confirmation(StreamKey {
                        participation,
                        generation,
                    }))
                }
                _ => None,
            };
            ActorTransportWork::Call(BoundAttemptCall {
                owner,
                abort,
                release: take_original(),
                confirmation,
            })
        })
        .unwrap_or_else(|refused| std::panic::resume_unwind(Box::new(refused)))
    }

    pub(super) fn pending_work(&self) -> &WorkObligation {
        self.pending
            .as_ref()
            .expect("original prepaid call")
            .1
            .work()
    }

    pub(super) fn bind_subscription(
        &mut self,
        ticket: &EffectTicket<FrontendEffect>,
        result: Position,
        bind: impl FnOnce() -> ActorTransportWork,
    ) -> PreparedEffectJob {
        assert!(matches!(
            self.owner,
            Some(AttemptCallOwner::SubscriptionOpen)
        ));
        let (effect, work) = self.begin_binding(ticket);
        prepare_actor_transport(effect, result, work, VecDeque::new(), self.observer, |_| {
            bind()
        })
        .unwrap_or_else(|refused| std::panic::resume_unwind(Box::new(refused)))
    }

    fn begin_binding(
        &mut self,
        ticket: &EffectTicket<FrontendEffect>,
    ) -> (Arc<AttemptEffectEnvelope>, ReportedWork) {
        let (effect, work) = self.pending.take().expect("same original call prepayment");
        assert!(
            matches!(&ticket.effect, FrontendEffect::Attempt(original) if Arc::ptr_eq(original, &effect))
        );
        let header = work.work().retirement_observer();
        assert!(
            self.live
                .replace(Live {
                    effect: effect.clone(),
                    header,
                    result: None,
                    abort_input: None,
                    retired: false,
                })
                .is_none()
        );
        (effect, work)
    }

    pub(crate) fn return_unclaimed(&mut self, result: Position) {
        let (_, work) = self
            .pending
            .take()
            .expect("same original unclaimed QA role");
        let header = work.work().retirement_observer();
        let (custody, exit) = ExitCustody::from_admitted(work);
        exit.revoke_unused_and_exit()
            .expect("original unused QA work");
        assert!(custody.try_retire().expect("unused QA report"));
        assert!(
            self.scope
                .reap_observed_header(&header)
                .expect("unused QA header")
        );
        assert!(self.idle.replace(result).is_none());
    }

    pub(super) fn owns(&self, effect: AttemptActionId) -> bool {
        self.live
            .as_ref()
            .is_some_and(|live| live.effect.protocol.id == effect)
    }

    pub(super) fn is_quiescent(&self) -> bool {
        self.pending.is_none() && self.live.is_none()
    }

    pub(super) fn take_unselected_failure(
        &mut self,
    ) -> Option<(HostFailureRef, OriginalHostFailure)> {
        self.failures.pop_first()
    }

    pub(super) fn needs_progress(&self, closing: bool, retire_failures: bool) -> bool {
        self.live
            .as_ref()
            .is_some_and(|live| live.retired && live.result.is_some())
            || closing
                && self.pending.is_none()
                && self.live.is_none()
                && self.owner.is_some()
                && (self.idle.is_some() || self.failures.is_empty() || retire_failures)
    }

    pub(crate) fn accept(
        &mut self,
        mut original: ReturnedActorTransportResult,
    ) -> Result<(), AttemptCallResultRefusal> {
        let live = self
            .live
            .as_mut()
            .expect("original call result has a live role");
        assert!(Arc::ptr_eq(&original.effect, &live.effect));
        if !matches!(&original.outcome, Some(BodyOutcome::Call(_))) {
            return Err(AttemptCallResultRefusal {
                original,
                publication: None,
            });
        }
        let Some(BodyOutcome::Call(reply)) = original.outcome.take() else {
            unreachable!("validated original call outcome");
        };
        match reply {
            AttemptCallReply::Input { input, failure } => {
                if let Some((reference, failure)) = failure {
                    assert!(self.failures.insert(reference, failure).is_none());
                }
                let input = FrontendInput::Attempt(Box::new(input));
                if let Err(publication) = original.original.fill_with_receipt(None, input) {
                    let fenced = matches!(
                        publication.error,
                        ActorTurnError::ActorFailed | ActorTurnError::Closed
                    );
                    if fenced
                        && (original.effect.is_read_confirmation()
                            || original.effect.is_subscription_open())
                    {
                        // The same actor's fence refused the bounded passive
                        // snapshot. The original result role remains ours.
                        assert!(matches!(&publication.event, FrontendInput::Attempt(input)
                            if matches!(**input, AttemptInput::Executor(novarocks_attempt_protocol::input::ExecutorFact::ReadState(_) | novarocks_attempt_protocol::input::ExecutorFact::SubscriptionExited { .. }))));
                        drop(publication);
                    } else if fenced && original.effect.is_root_control_reply() {
                        // The actual QA delivery is already done. Keep the
                        // refused original Core input and retire its paid role
                        // after Join, even though the Core cannot consume it.
                        assert!(self.refused_root_input.replace(publication).is_none());
                    } else {
                        return Err(AttemptCallResultRefusal {
                            original,
                            publication: Some(publication),
                        });
                    }
                }
            }
            AttemptCallReply::Terminal(port) => {
                let publication = if let Some(outbox) = self.terminal.as_ref() {
                    outbox.publish(port)
                } else {
                    Err(port)
                };
                if let Err(refused) = publication {
                    original.outcome = Some(BodyOutcome::Call(AttemptCallReply::Terminal(refused)));
                    return Err(AttemptCallResultRefusal {
                        original,
                        publication: None,
                    });
                }
            }
            AttemptCallReply::Convergence(progress) => {
                let publication = if let Some(outbox) = self.terminal.as_ref() {
                    outbox.publish_convergence(progress)
                } else {
                    Err(progress)
                };
                if let Err(refused) = publication {
                    original.outcome =
                        Some(BodyOutcome::Call(AttemptCallReply::Convergence(refused)));
                    return Err(AttemptCallResultRefusal {
                        original,
                        publication: None,
                    });
                }
            }
            AttemptCallReply::NoInput => {}
            AttemptCallReply::AbortSettled(applied) => {
                if applied.result.is_err() {
                    original.outcome =
                        Some(BodyOutcome::Call(AttemptCallReply::AbortSettled(applied)));
                    return Err(AttemptCallResultRefusal {
                        original,
                        publication: None,
                    });
                }
                live.abort_input = applied.retirement;
            }
        }
        assert!(live.result.replace(original.original).is_none());
        Ok(())
    }

    pub(super) fn take_refused_root_input(&mut self) -> Option<CausalFillFailure<FrontendInput>> {
        self.refused_root_input.take()
    }

    pub(crate) fn transport_retired(&mut self, effect: AttemptActionId) {
        let live = self.live.as_mut().expect("original call retirement");
        assert_eq!(live.effect.protocol.id, effect);
        assert!(!std::mem::replace(&mut live.retired, true));
    }

    pub(crate) fn retire_result(&mut self) -> Result<bool, ActorTurnError> {
        let Some(live) = self.live.as_mut() else {
            return Ok(false);
        };
        let Some(result) = live.result.as_ref() else {
            return Ok(false);
        };
        if !live.retired || !result.retire_ready_after_exit()? {
            return Ok(false);
        }
        if let Some(original) = live.abort_input.as_mut()
            && !original.retire_ready()?
        {
            return Ok(false);
        }
        if !self.scope.reap_observed_header(&live.header)? {
            return Ok(false);
        }
        let result = self.live.take().unwrap().result.unwrap();
        assert!(self.idle.replace(result).is_none());
        Ok(true)
    }

    pub(crate) fn take_failure(
        &mut self,
        reference: HostFailureRef,
    ) -> Option<OriginalHostFailure> {
        self.failures.remove(&reference)
    }

    pub(crate) fn close_idle(&mut self) -> Result<bool, ActorTurnError> {
        let Some(position) = self.idle.as_mut() else {
            return Ok(false);
        };
        position.close_after_exit()?;
        self.idle.take();
        Ok(true)
    }

    pub(crate) fn take_drained_owner(
        &mut self,
    ) -> Option<(
        AttemptCallOwner,
        Option<Arc<AbortSettlementOwner>>,
        Option<Arc<TerminalOutbox>>,
    )> {
        if self.idle.is_none()
            && self.pending.is_none()
            && self.live.is_none()
            && self.failures.is_empty()
        {
            let original = self.owner.take()?;
            if matches!(
                original,
                AttemptCallOwner::Release | AttemptCallOwner::SubscriptionOpen
            ) {
                return None;
            }
            Some((original, self.abort.take(), self.terminal.take()))
        } else {
            None
        }
    }

    pub(super) fn is_drained(&self) -> bool {
        self.owner.is_none()
    }
}
