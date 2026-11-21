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
    task_execution::host::{
        custody::ReturnedActorTransportResult,
        observations::{
            ObservationError, OrderedObservationInput, ReadConfirmationBorrow, StreamKey,
        },
        publication::{AttemptPublicationOwner, OriginalHostFailure},
    },
};
use novarocks_attempt_protocol::port_output::{
    AttemptAuthorization, AttemptToLogical, AttemptToLogicalBody,
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
use novarocks_coordination_contract::AttemptActionId;
use std::{
    collections::{BTreeMap, VecDeque},
    sync::Arc,
};

type Position = CausalInputObligation<FrontendActor>;

#[derive(Clone)]
pub(crate) enum AttemptCallOwner {
    LogicalPort(super::super::port::facts::FactRole),
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
            Self::LogicalPort(role) => {
                effect.is_attempt_port()
                    && !effect.is_publication_call()
                    && super::super::port::facts::FactRole::for_protocol_effect(
                        &effect.protocol.body,
                    ) == Some(*role)
            }
            Self::Process(_) => effect.is_process_call(),
            Self::Publication(_) => effect.is_publication_call(),
            Self::Release => effect.is_transport_release(),
            Self::ReadConfirmation(_) => effect.is_read_confirmation(),
            Self::SubscriptionOpen => effect.is_subscription_open(),
            Self::SubscriptionControl(_) => effect.is_subscription_control(),
        }
    }
}

#[expect(
    clippy::large_enum_variant,
    reason = "The reply moves the original input and its paired failure into the reserved result custody."
)]
pub(crate) enum AttemptCallReply {
    Input {
        input: AttemptInput,
        failure: Option<(HostFailureRef, OriginalHostFailure)>,
    },
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
    pub(super) fn dispose(self) {
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
    release: Option<TransportRelease>,
    confirmation: Option<Result<ReadConfirmationBorrow<FrontendActor>, ObservationError>>,
    root_fact: Option<super::super::port::facts::RootStatusFactReceipt>,
    root_reply: Option<super::super::port::root_control::RootReplyFactReceipt>,
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
        match self.owner {
            AttemptCallOwner::LogicalPort(_) => {
                // The current-ticket Source already published the fact, or
                // the original LP reply already filled this job's result.
                // This SAME job now performs only real capture/Join exit.
                Ok(AttemptCallReply::NoInput)
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
                    let (input, failure) = if let Some(receipt) = self.root_fact {
                        owner.acknowledge_root_fact(&effect, receipt)
                    } else {
                        owner.publish(effect.protocol.id, publication).await
                    };
                    Ok(AttemptCallReply::Input { input, failure })
                }
                EffectBody::Port(_) => {
                    if let Some(receipt) = self.root_fact {
                        let (input, failure) = owner.acknowledge_root_fact(&effect, receipt);
                        return Ok(AttemptCallReply::Input { input, failure });
                    }
                    let receipt = self
                        .root_reply
                        .expect("original Port root reply handoff receipt");
                    let result = owner.acknowledge_root_reply(&effect, receipt);
                    result
                        .map(|result| match result {
                            Some((input, failure)) => AttemptCallReply::Input { input, failure },
                            None => AttemptCallReply::NoInput,
                        })
                        .map_err(|error| Box::new(error) as Box<dyn std::any::Any + Send>)
                }
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
    not_started: Option<novarocks_control_runtime::exit_custody::ExitCustodyError>,
    retired: bool,
}

#[must_use = "retain the original call result, input credit, and refused publication"]
#[allow(
    dead_code,
    reason = "Call refusal retains the original paid input and publication through failure custody."
)]
pub(crate) struct AttemptCallResultRefusal {
    pub(crate) original: ReturnedActorTransportResult,
    pub(crate) publication: Option<CausalFillFailure<FrontendInput>>,
}

type DrainedCallOwner = AttemptCallOwner;

pub(crate) struct AttemptCallSource {
    scope: ScopeHandle,
    generation: Generation,
    parent: ReservationId,
    observer: ConsumerId,
    owner: Option<AttemptCallOwner>,
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
            idle: Some(position),
            pending: None,
            live: None,
            failures: BTreeMap::new(),
            refused_root_input: None,
        }
    }

    pub(super) fn project_failure(
        &self,
        reference: HostFailureRef,
    ) -> Option<novarocks_query_application::execution_error::ExecutionFailureInput> {
        self.failures
            .get(&reference)
            .map(OriginalHostFailure::preparation_input)
    }

    pub(super) fn matches_scope(&self, scope: &ScopeHandle) -> bool {
        self.scope.same_scope(scope)
    }

    pub(super) fn handles(&self, original: &AttemptEffectEnvelope) -> bool {
        self.owner
            .as_ref()
            .is_some_and(|owner| owner.matches(original))
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
            if tracing::enabled!(target: "novarocks_original_wait", tracing::Level::DEBUG) {
                let live = self.live.as_ref().map(|live| {
                    (
                        live.effect.protocol.id,
                        live.retired,
                        live.result.is_some(),
                        live.result.as_ref().map(Position::in_flight),
                        live.header.header_holders(),
                    )
                });
                // Both kernel observations above return their own locks
                // before any tracing subscriber can run.
                tracing::debug!(
                    target: "novarocks_original_wait",
                    scope = ?self.scope.id(),
                    requested_action = ?effect.protocol.id,
                    activation = ?match &effect.protocol.body {
                        EffectBody::Port(message) => Some(message.activation),
                        EffectBody::EstablishAuthorization { request, .. } => Some(request.activation),
                        _ => None,
                    },
                    idle = false,
                    pending = self.pending.is_some(),
                    live = ?live,
                    "attempt original call prepay refused at live result credit"
                );
            }
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
                tracing::debug!(
                    target: "novarocks_original_wait",
                    scope = ?self.scope.id(),
                    requested_action = ?effect.protocol.id,
                    activation = ?match &effect.protocol.body {
                        EffectBody::Port(message) => Some(message.activation),
                        EffectBody::EstablishAuthorization { request, .. } => Some(request.activation),
                        _ => None,
                    },
                    kind = ?kind,
                    gate = ?error,
                    "attempt original call prepay refused at work gate"
                );
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
        self.bind_with_captures(ticket, result, None, None, || None)
    }

    pub(super) fn bind_with_root_fact(
        &mut self,
        ticket: &EffectTicket<FrontendEffect>,
        result: Position,
        receipt: super::super::port::facts::RootStatusFactReceipt,
    ) -> PreparedEffectJob {
        assert!(matches!(self.owner, Some(AttemptCallOwner::Publication(_))));
        self.bind_with_captures(ticket, result, Some(receipt), None, || None)
    }

    pub(super) fn bind_with_root_reply(
        &mut self,
        ticket: &EffectTicket<FrontendEffect>,
        result: Position,
        receipt: super::super::port::root_control::RootReplyFactReceipt,
    ) -> PreparedEffectJob {
        assert!(matches!(self.owner, Some(AttemptCallOwner::Publication(_))));
        self.bind_with_captures(ticket, result, None, Some(receipt), || None)
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
        self.bind_with_captures(ticket, result, None, None, take_original)
    }

    fn bind_with_captures(
        &mut self,
        ticket: &EffectTicket<FrontendEffect>,
        result: Position,
        root_fact: Option<super::super::port::facts::RootStatusFactReceipt>,
        root_reply: Option<super::super::port::root_control::RootReplyFactReceipt>,
        take_original: impl FnOnce() -> Option<TransportRelease>,
    ) -> PreparedEffectJob {
        let (effect, work) = self.begin_binding(ticket);
        let owner = self.owner.as_ref().expect("original call owner").clone();
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
                release: take_original(),
                confirmation,
                root_fact,
                root_reply,
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
                    not_started: None,
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

    /// Check the same original result before the subscription Source retains
    /// it across final-phase retirement. This grants no result or exit right.
    pub(super) fn matches_original_result(&self, original: &ReturnedActorTransportResult) -> bool {
        self.live
            .as_ref()
            .is_some_and(|live| Arc::ptr_eq(&original.effect, &live.effect))
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
        if matches!(self.owner, Some(AttemptCallOwner::LogicalPort(_)))
            && matches!(original.outcome, Some(BodyOutcome::CallNotStarted(_)))
        {
            let Some(BodyOutcome::CallNotStarted(reason)) = original.outcome.take() else {
                unreachable!()
            };
            live.not_started = Some(reason);
            // Retain the SAME causal position and the raw start-fence reason.
            // No new reply or transport-withdrawal fact is published here.
            assert!(live.result.replace(original.original).is_none());
            return Ok(());
        }
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
            AttemptCallReply::NoInput => {}
        }
        assert!(live.result.replace(original.original).is_none());
        tracing::debug!(
            action = ?live.effect.protocol.id,
            retired = live.retired,
            "attempt original call result accepted into source custody"
        );
        Ok(())
    }

    /// An actual same-actor failure fences delivery, never actual producer
    /// exit. The original tracker already observed the latter before returning
    /// this result. Preserve the raw outcome for controlled disposal instead
    /// of filling a dead Core or claiming that its old role was empty.
    pub(super) fn accept_after_owner_failure(
        &mut self,
        mut original: ReturnedActorTransportResult,
        proof: &novarocks_control_runtime::actor_turn::ActorFailureProof,
    ) -> Result<Box<dyn std::any::Any + Send>, ReturnedActorTransportResult> {
        if !proof.registration().matches_scope(&self.scope)
            || !self
                .live
                .as_ref()
                .is_some_and(|live| Arc::ptr_eq(&live.effect, &original.effect))
            || !matches!(
                original.outcome,
                Some(BodyOutcome::Call(_) | BodyOutcome::CallNotStarted(_))
            )
        {
            return Err(original);
        }
        let live = self.live.as_mut().expect("same original failed-actor call");
        if let Some(BodyOutcome::CallNotStarted(reason)) = &original.outcome {
            live.not_started = Some(*reason);
        }
        let retained = Box::new((
            original.effect,
            original.outcome.take(),
            original.send_reports,
            original.withdrawal,
        ));
        assert!(live.result.replace(original.original).is_none());
        Ok(retained)
    }

    pub(super) fn take_refused_root_input(&mut self) -> Option<CausalFillFailure<FrontendInput>> {
        self.refused_root_input.take()
    }

    pub(crate) fn transport_retired(&mut self, effect: AttemptActionId) {
        let live = self.live.as_mut().expect("original call retirement");
        assert_eq!(live.effect.protocol.id, effect);
        assert!(!std::mem::replace(&mut live.retired, true));
        tracing::debug!(
            target: "novarocks_original_credit",
            action = ?effect,
            activation = ?match &live.effect.protocol.body {
                EffectBody::Port(message) => Some(message.activation),
                EffectBody::EstablishAuthorization { request, .. } => Some(request.activation),
                _ => None,
            },
            result = live.result.is_some(),
            "attempt original call transport actually retired"
        );
    }

    /// Reports progress, including disposal of a failed payload before the
    /// original inputs and work header retire and the result returns to idle.
    pub(crate) fn retire_result(&mut self) -> Result<bool, ActorTurnError> {
        let Some(live) = self.live.as_mut() else {
            return Ok(false);
        };
        let Some(result) = live.result.as_ref() else {
            return Ok(false);
        };
        if !live.retired {
            return Ok(false);
        }
        if !result.retire_ready_after_exit()? {
            return match result.take_failed_payload() {
                Ok(Some(original)) => {
                    original.dispose()?;
                    Ok(true)
                }
                Ok(None)
                | Err(ActorTurnError::ActorNotRetired | ActorTurnError::InputAuthorMismatch) => {
                    Ok(false)
                }
                Err(error) => Err(error),
            };
        }
        if !self.scope.reap_observed_header(&live.header)? {
            return Ok(false);
        }
        if live.not_started.is_some() {
            assert!(
                matches!(self.owner, Some(AttemptCallOwner::LogicalPort(_)))
                    || self.scope.snapshot()?.failed,
                "only an original metadata call may consume its unused start fence"
            );
        }
        let retired_action = live.effect.protocol.id;
        let retired_activation = match &live.effect.protocol.body {
            EffectBody::Port(message) => Some(message.activation),
            EffectBody::EstablishAuthorization { request, .. } => Some(request.activation),
            _ => None,
        };
        let result = self.live.take().unwrap().result.unwrap();
        assert!(self.idle.replace(result).is_none());
        tracing::debug!(
            target: "novarocks_original_credit",
            action = ?retired_action,
            activation = ?retired_activation,
            "attempt original call causal result and header returned to idle"
        );
        Ok(true)
    }

    #[cfg(test)]
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

    pub(crate) fn take_drained_owner(&mut self) -> Option<DrainedCallOwner> {
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
            Some(original)
        } else {
            None
        }
    }

    pub(super) fn is_drained(&self) -> bool {
        self.owner.is_none()
    }
}

#[cfg(test)]
mod retained_tests {
    use super::*;
    use crate::control_runtime::transport_withdrawal::AttemptEffectEnvelope;
    use crate::task_execution::host::{
        custody::{TransportRetirements, TransportSourceEvent},
        port::{facts::FactRole, requests::test_support::*},
    };
    use novarocks_attempt_protocol::effect::{AttemptEffect, EffectClass};
    use novarocks_attempt_protocol::port_output::{
        AttemptAuthorization, AttemptToLogical, AttemptToLogicalBody,
    };
    use novarocks_control_runtime::causal_input::CausalInputObligation;
    use novarocks_control_runtime_core::mailbox::InputClass;
    use novarocks_coordination_contract::{IdSeed, OperationIds, logical_port::LogicalToAttempt};
    use novarocks_execution_contract::ParticipationRef;
    use novarocks_types::{BackendProcessId, FrontendProcessId};
    use std::time::Duration;

    #[tokio::test]
    async fn retained_port_start_fence_preserves_original_reply_and_actual_join_retirement() {
        for (replied, owner_failed) in [(false, false), (true, false), (false, true), (true, true)]
        {
            let mut f = std::mem::ManuallyDrop::new(Fixture::new());
            let mut ids = OperationIds::new(IdSeed::new(71, 1));
            let participation = ParticipationRef::new(
                activation().execution(),
                FrontendProcessId::try_from_bytes(ids.issue().to_bytes()).unwrap(),
                BackendProcessId::try_from_bytes(ids.issue().to_bytes()).unwrap(),
            );
            let effect = AttemptEffectEnvelope::new(AttemptEffect {
                id: action(71),
                class: EffectClass::Business,
                body: EffectBody::Port(AttemptToLogical {
                    activation: activation(),
                    action: action(71),
                    body: AttemptToLogicalBody::AuthorizationRequest {
                        token: token(71),
                        request: AttemptAuthorization::ReauthorizeEstablish {
                            participation,
                            operation_id: ids.issue(),
                        },
                    },
                }),
            });
            let position = CausalInputObligation::reserve_admitted(
                &f.runtime,
                &f.attempt.registration,
                f.logical.registration.actor(),
                InputClass::Lifecycle,
                &f.attempt.work,
            )
            .unwrap();
            let mut calls = AttemptCallSource::new(
                &f.attempt.work,
                &f.attempt.payer,
                EPOCH,
                OBSERVER,
                AttemptCallOwner::LogicalPort(FactRole::Authorization),
                position,
            );
            let result = calls.prepay(&effect).unwrap().unwrap();
            let ticket = f.emit(false, FrontendEffect::Attempt(effect.clone()));
            let mut job = calls.bind(&ticket, result);
            let mut retirements = TransportRetirements::new(
                f.runtime.clone(),
                f.attempt.registration.clone(),
                tokio::runtime::Handle::current(),
                1,
            );
            retirements
                .track(effect, job.transport.take().unwrap())
                .unwrap_or_else(|_| panic!("same original retained Join"));
            let failure_position = if owner_failed {
                Some(
                    f.runtime
                        .reserve_admitted_input_for(
                            &f.attempt.registration,
                            f.attempt.registration.actor(),
                            InputClass::Lifecycle,
                            &f.attempt.work,
                        )
                        .unwrap()
                        .into_standing_input()
                        .unwrap_or_else(|_| panic!("same original failure role")),
                )
            } else {
                None
            };
            if replied {
                let reply = LogicalToAttempt { activation: activation(),
                    body: novarocks_coordination_contract::logical_port::LogicalToAttemptBody::AuthorizationReply {
                        token: token(71),
                        decision: Err(novarocks_coordination_contract::logical_port::AttemptAuthorizationFailure::Rejected(
                            novarocks_coordination_contract::ExecutionFailure::from_input(
                                novarocks_attempt_protocol::port_projection::owner_failure()))),
                    },
                };
                let lp_effect = logical_effect(72, reply.clone(), &f.logical.payer);
                let lp = f.emit(true, lp_effect);
                // Queue the original failure first in the SAME Lifecycle
                // class, then commit the LP reply before any AP poll. This
                // exercises an actual queued reply surviving its owner fence.
                if let Some(original) = &failure_position {
                    original
                        .fill(FrontendInput::LogicalInfrastructureFailed)
                        .unwrap();
                }
                job.result
                    .fill_event_from_effect(
                        &f.runtime,
                        &lp,
                        None,
                        FrontendInput::Attempt(Box::new(AttemptInput::Port(reply.clone()))),
                    )
                    .unwrap();
                f.runtime.effect_completed(lp).unwrap();
            }
            f.runtime.effect_completed(ticket).unwrap();
            let proof = if owner_failed {
                if !replied {
                    failure_position
                        .as_ref()
                        .unwrap()
                        .fill(FrontendInput::LogicalInfrastructureFailed)
                        .unwrap();
                }
                f.consume();
                Some(
                    f.runtime
                        .failure_proof_for(&f.attempt.registration)
                        .unwrap(),
                )
            } else {
                f.attempt.scope.request_stop().unwrap();
                None
            };
            let mut cleanup = None;
            super::super::handoff_actor_transport(&tokio::runtime::Handle::current(), job)
                .unwrap_or_else(|_| panic!("same tracked original metadata job"));
            tokio::time::timeout(Duration::from_secs(2), async {
                loop {
                    f.consume();
                    while let Some(turn) = f.runtime.claim_host_actor() {
                        let more = if turn.matches_registration(&f.attempt.registration) {
                            let progress = retirements.progress(&turn, 8, |_, event| match event {
                                TransportSourceEvent::Result(original) => {
                                    if let Some(proof) = &proof {
                                        let original = calls.accept_after_owner_failure(original, proof)
                                            .unwrap_or_else(|_| panic!("same actual failed-actor result"));
                                        let work = calls.reserve_disposal().unwrap();
                                        let header = work.work().retirement_observer();
                                        let disposal = crate::task_execution::host::custody::HostDisposal::start(
                                            &tokio::runtime::Handle::current(), work, OBSERVER, original, drop)
                                            .unwrap_or_else(|_| panic!("same original failed result disposal"));
                                        assert!(cleanup.replace((disposal, header)).is_none());
                                    } else {
                                        calls.accept(original).unwrap_or_else(|_| panic!("original unused call result"));
                                    }
                                },
                                TransportSourceEvent::Retired => {
                                    calls.transport_retired(action(71))
                                }
                                _ => panic!("unused call has no transport failure or extra report"),
                            });
                            calls.retire_result().unwrap();
                            progress.more
                        } else {
                            false
                        };
                        turn.release(more).unwrap();
                    }
                    if let Some((original, header)) = &cleanup {
                        let wake = retirements.host_waker();
                        match original.poll_complete(&mut std::task::Context::from_waker(&wake)) {
                            std::task::Poll::Ready(Ok(())) => {
                                if calls.reap_disposal(header).unwrap() { cleanup = None; }
                            }
                            std::task::Poll::Ready(Err(error)) => panic!("original failed result disposal: {error:?}"),
                            std::task::Poll::Pending => {},
                        }
                    }
                    if retirements.is_empty() && calls.is_quiescent() && cleanup.is_none() {
                        break;
                    }
                    tokio::task::yield_now().await;
                }
            })
            .await
            .unwrap();
            let seen = f.attempt.seen.lock().unwrap();
            assert_eq!(
                seen.iter()
                    .filter(|input| matches!(input,
                FrontendInput::Attempt(input) if matches!(input.as_ref(), AttemptInput::Port(_))))
                    .count(),
                usize::from(replied && !owner_failed),
                "no fabricated reply or withdrawal"
            );
            drop(seen);
            if let Some(mut original) = failure_position {
                use novarocks_control_runtime::actor_turn::FailureSourceState;
                let actor = f.attempt.registration.actor();
                let FailureSourceState::ClosedPending(input) =
                    f.runtime.failure_source_state(actor).unwrap()
                else {
                    panic!("same original failure supervisor batch");
                };
                assert!(
                    f.runtime
                        .effect_inventory(actor, input, None, 1)
                        .unwrap()
                        .is_empty()
                );
                f.runtime.retire_input(actor, input).unwrap();
                assert!(original.retire_ready_after_exit().unwrap());
                original.close_after_exit().unwrap();
                assert!(f.runtime.take_failure_payload(actor).unwrap().is_some());
            }
            calls.close_idle().unwrap();
            drop(calls.take_drained_owner().unwrap());
            assert!(calls.is_drained());
            std::mem::ManuallyDrop::into_inner(f).finish();
        }
    }
}
