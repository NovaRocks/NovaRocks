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

//! Original covered subscription jobs and their actual retirement in the attempt host.

use super::{
    ActorTransportWork, AttemptCallOwner, AttemptCallSource, BodyOutcome,
    call_source::AttemptCallReply, releases::ReleaseCapture,
};
use crate::{
    control_runtime::{
        actor::{FrontendActor, FrontendEffect, FrontendInput},
        host::{PreparedEffectJob, SourceProgress},
        membership::FrozenMember,
        transport_withdrawal::AttemptEffectEnvelope,
    },
    native::{data_runtime::FrontendDataRuntime, transport::Client},
    query_execution::native_execution_adapter::source_cohort::NativeSourceCohort,
    task_execution::host::{
        RetirementScan,
        custody::{ReturnedActorTransportResult, TransportSourceEvent},
        observations::{ObservationError, OrderedObservationInput, RootControlBinding, StreamKey},
        publication::OriginalHostFailure,
        subscriptions::{
            SubscriptionFailureRecord, SubscriptionGeneration, SubscriptionPhasePublisher,
            SubscriptionRunError, SubscriptionSource, SubscriptionSourceError,
        },
    },
};
use novarocks_attempt_protocol::{
    effect::EffectBody,
    failure::HostFailureRef,
    input::{AttemptInput, ExecutorFact},
};
use novarocks_control_runtime::{
    actor_turn::{ActorRegistration, ActorRuntime, ActorTurnError, EffectTicket, StandingInputId},
    causal_input::CausalInputObligation,
    obligation::{ReservationObligation, WorkObligation},
};
use novarocks_control_runtime_core::{
    identity::{ActorId, ConsumerId, Generation, InputId},
    mailbox::InputClass,
};
use novarocks_coordination_contract::AttemptActionId;
use novarocks_execution_contract::{BackendProcessDescriptor, ParticipationRef};
use novarocks_query_application::execution_error::ExecutionFailureInput;
use novarocks_types::{BackendProcessId, NativeCompatibilityId, QueryExecutionId};
use novarocks_workload_control::{WorkOwner, WorkScope};
use std::{
    any::Any,
    collections::{BTreeMap, VecDeque},
    sync::Arc,
};

type Position = CausalInputObligation<FrontendActor>;

pub(crate) struct SubscriptionEndpoint {
    pub(crate) source: Arc<SubscriptionSource<FrontendActor>>,
    pub(crate) client: Client,
}

pub(crate) type SubscriptionEndpoints = BTreeMap<ParticipationRef, SubscriptionEndpoint>;

pub(crate) struct FrozenSubscriptionInputs<'a> {
    pub(crate) backends: &'a Arc<[BackendProcessDescriptor]>,
    pub(crate) compatibility: NativeCompatibilityId,
    pub(crate) data: &'a FrontendDataRuntime,
    pub(crate) scope: &'a WorkScope,
    pub(crate) cohort: &'a NativeSourceCohort,
    pub(crate) ingress: &'a OrderedObservationInput<FrontendActor>,
}

pub(crate) struct FrozenSubscriptionStageInputs<'a> {
    pub(crate) backends: &'a Arc<[BackendProcessDescriptor]>,
    pub(crate) compatibility: NativeCompatibilityId,
    pub(crate) data: &'a FrontendDataRuntime,
    pub(crate) scope: &'a WorkScope,
    pub(crate) cohort: &'a NativeSourceCohort,
    pub(crate) execution: QueryExecutionId,
}

struct StagedSubscriptionEndpoint {
    participation: ParticipationRef,
    backend: usize,
    owner: Option<WorkOwner>,
}

/// The original WC children are staged before actor registration. There are
/// no channels, registered generations, or ingress aliases in this holder.
pub(crate) struct StagedSubscriptionEndpoints {
    backends: Arc<[BackendProcessDescriptor]>,
    compatibility: NativeCompatibilityId,
    data: FrontendDataRuntime,
    execution: QueryExecutionId,
    entries: Vec<StagedSubscriptionEndpoint>,
}

impl StagedSubscriptionEndpoints {
    pub(crate) fn data(&self) -> &FrontendDataRuntime {
        &self.data
    }

    pub(crate) fn bind(
        mut self,
        roster: &[FrozenMember],
        compatibility: NativeCompatibilityId,
        ingress: &OrderedObservationInput<FrontendActor>,
    ) -> Result<Arc<SubscriptionEndpoints>, SubscriptionEndpointError> {
        if self.execution != ingress.root_binding().activation().execution()
            || self.compatibility != compatibility
            || self.entries.len() != roster.len()
            || !self.entries.iter().zip(roster).all(|(entry, member)| {
                entry.participation == member.participation
                    && self.backends[entry.backend].endpoint() == &member.endpoint
            })
        {
            return Err(SubscriptionEndpointError::Roster);
        }

        let endpoints = self
            .entries
            .iter_mut()
            .map(|entry| {
                let source = SubscriptionSource::from_owner(
                    entry.participation,
                    entry.owner.take().expect("original staged WC child"),
                    ingress.clone(),
                );
                let client =
                    Client::for_backend_in(self.backends.clone(), entry.backend, self.data.clone());
                (entry.participation, SubscriptionEndpoint { source, client })
            })
            .collect();
        Ok(Arc::new(endpoints))
    }
}

impl Drop for StagedSubscriptionEndpoints {
    fn drop(&mut self) {
        for entry in &mut self.entries {
            if let Some(owner) = entry.owner.take() {
                // No subscription job can depend on an unpublished child.
                owner.complete_after_terminal_cancel_settled();
            }
        }
    }
}

#[derive(Debug)]
pub(crate) enum SubscriptionEndpointError {
    Roster,
    Source {
        participation: ParticipationRef,
        original: SubscriptionSourceError,
    },
}

/// Open no channel and consume no WC child until the entire frozen roster
/// matches. Stage the actual children before installing any runtime source.
pub(crate) fn stage_subscription_endpoints(
    roster: &[FrozenMember],
    original: FrozenSubscriptionStageInputs<'_>,
) -> Result<StagedSubscriptionEndpoints, SubscriptionEndpointError> {
    let backends: BTreeMap<_, _> = original
        .backends
        .iter()
        .enumerate()
        .map(|(index, backend)| (backend.process_id(), index))
        .collect();
    if roster.is_empty()
        || backends.len() != original.backends.len()
        || backends.len() != roster.len()
        || roster.windows(2).any(|pair| {
            pair[0].participation.frontend_process_id()
                != pair[1].participation.frontend_process_id()
                || pair[0].participation.backend_process_id()
                    >= pair[1].participation.backend_process_id()
        })
        || !roster.iter().all(|member| {
            member.participation.query_execution_id() == original.execution
                && backends
                    .get(&member.participation.backend_process_id())
                    .is_some_and(|&index| {
                        let backend = &original.backends[index];
                        backend.endpoint() == &member.endpoint
                            && backend.native_compatibility_id() == original.compatibility
                    })
        })
    {
        return Err(SubscriptionEndpointError::Roster);
    }
    let mut staged = StagedSubscriptionEndpoints {
        backends: original.backends.clone(),
        compatibility: original.compatibility,
        data: original.data.clone(),
        execution: original.execution,
        entries: Vec::with_capacity(roster.len()),
    };
    for member in roster {
        let participation = member.participation;
        staged.entries.push(StagedSubscriptionEndpoint {
            participation,
            backend: backends[&participation.backend_process_id()],
            owner: None,
        });
        let entry = staged.entries.last_mut().expect("original staged receiver");
        original
            .cohort
            .take_child_in(original.scope, &mut entry.owner)
            .map_err(|original| SubscriptionEndpointError::Source {
                participation,
                original: SubscriptionSourceError::Work(original),
            })?
            .notify();
    }
    Ok(staged)
}

pub(crate) fn bind_subscription_endpoints(
    roster: &[FrozenMember],
    original: FrozenSubscriptionInputs<'_>,
) -> Result<Arc<SubscriptionEndpoints>, SubscriptionEndpointError> {
    stage_subscription_endpoints(
        roster,
        FrozenSubscriptionStageInputs {
            backends: original.backends,
            compatibility: original.compatibility,
            data: original.data,
            scope: original.scope,
            cohort: original.cohort,
            execution: original.ingress.root_binding().activation().execution(),
        },
    )?
    .bind(roster, original.compatibility, original.ingress)
}

/// An unpublished endpoint has no generation or in-flight request. Return
/// only the children already transferred into this particular roster.
pub(crate) fn retire_unpublished_endpoints(endpoints: &SubscriptionEndpoints) {
    for endpoint in endpoints.values() {
        endpoint.source.close_admission();
        endpoint
            .source
            .take_owner_after_jobs()
            .expect("unpublished subscription has no jobs")
            .expect("one original WC child")
            .complete_after_terminal_cancel_settled();
    }
}

#[derive(Debug)]
pub(super) enum SubscriptionControlError {
    WrongEffect,
    WrongParticipation,
    Source(SubscriptionSourceError),
}
impl SubscriptionEndpoint {
    fn matches(&self, participation: ParticipationRef, root: &RootControlBinding) -> bool {
        self.client.backend_process_id() == participation.backend_process_id()
            && self.source.matches(participation, root)
    }
}
pub(super) fn control(
    endpoints: &SubscriptionEndpoints,
    effect: &EffectBody,
) -> Result<(), SubscriptionControlError> {
    let participation = match effect {
        EffectBody::AdjustReadWindow(request) => request.participation,
        EffectBody::CloseSubscription { participation, .. } => *participation,
        _ => return Err(SubscriptionControlError::WrongEffect),
    };
    let endpoint = endpoints
        .get(&participation)
        .ok_or(SubscriptionControlError::WrongParticipation)?;
    match effect {
        EffectBody::AdjustReadWindow(request) => {
            endpoint.source.adjust_read_window(request.clone())
        }
        EffectBody::CloseSubscription { generation, .. } => {
            endpoint.source.close_generation(*generation)
        }
        _ => unreachable!("validated original subscription control"),
    }
    .map_err(SubscriptionControlError::Source)
}

pub(crate) struct BoundSubscription {
    effect: AttemptActionId,
    key: StreamKey,
    original: Option<SubscriptionGeneration<FrontendActor>>,
    client: Client,
    failure: Arc<SubscriptionFailureRecord>,
}
impl BoundSubscription {
    pub(super) fn matches(&self, original: &AttemptEffectEnvelope) -> bool {
        matches!(&original.protocol.body, EffectBody::OpenSubscription(request)
            if self.effect == original.protocol.id && self.key == stream_key(request))
    }

    pub(super) async fn run(self) -> Result<(), SubscriptionRunError> {
        if let Some(original) = self.original {
            original.run_native(self.client, self.failure).await?;
        }
        Ok(())
    }

    pub(super) fn exit_fact(&self) -> ExecutorFact {
        ExecutorFact::SubscriptionExited {
            effect: self.effect,
            participation: self.key.participation,
            generation: self.key.generation,
        }
    }
}

fn stream_key(request: &novarocks_attempt_protocol::observation::SubscriptionRequest) -> StreamKey {
    StreamKey {
        participation: request.participation,
        generation: request.generation,
    }
}

struct Pending {
    key: StreamKey,
    phase: Arc<SubscriptionPhasePublisher<FrontendActor>>,
}

struct Active {
    key: StreamKey,
    phase: Arc<SubscriptionPhasePublisher<FrontendActor>>,
    indexed: Option<InputId>,
    failure: Arc<SubscriptionFailureRecord>,
    body_exited: bool,
    call_retired: bool,
    phase_closed: bool,
    registered: bool,
    stop_requested: bool,
}

pub(crate) struct AttemptSubscriptions {
    runtime: ActorRuntime<FrontendActor>,
    registration: ActorRegistration,
    source: ActorId,
    generation_limit: usize,
    endpoints: Option<Arc<SubscriptionEndpoints>>,
    calls: BTreeMap<ParticipationRef, AttemptCallSource>,
    pending: Option<Pending>,
    active: BTreeMap<AttemptActionId, Active>,
    phase_roles: BTreeMap<StandingInputId, AttemptActionId>,
    active_order: VecDeque<AttemptActionId>,
    active_scan: RetirementScan,
    close_order: VecDeque<ParticipationRef>,
    closing: bool,
    terminal_failures: BTreeMap<HostFailureRef, Arc<SubscriptionFailureRecord>>,
}

impl AttemptSubscriptions {
    pub(crate) fn bind(
        runtime: ActorRuntime<FrontendActor>,
        registration: ActorRegistration,
        source: ActorId,
        root: &RootControlBinding,
        parent: &WorkObligation,
        payer: &ReservationObligation,
        observer: ConsumerId,
        endpoints: Arc<SubscriptionEndpoints>,
    ) -> Result<Self, ActorTurnError> {
        if !registration.matches_scope(parent.scope())
            || !payer.belongs_to_bound_work(parent.scope(), parent.id())
            || !endpoints
                .iter()
                .all(|(&key, endpoint)| endpoint.matches(key, root))
        {
            return Err(ActorTurnError::InputAuthorMismatch);
        }
        let mut calls = BTreeMap::<_, AttemptCallSource>::new();
        for &participation in endpoints.keys() {
            let position = match Position::reserve_admitted(
                &runtime,
                &registration,
                source,
                InputClass::Lifecycle,
                parent,
            ) {
                Ok(position) => position,
                Err(error) => {
                    for call in calls.values_mut() {
                        assert!(
                            call.close_idle()
                                .expect("original unused subscription position")
                        );
                    }
                    return Err(error);
                }
            };
            calls.insert(
                participation,
                AttemptCallSource::new(
                    parent,
                    payer,
                    registration.initial_epoch(),
                    observer,
                    AttemptCallOwner::SubscriptionOpen,
                    position,
                ),
            );
        }
        let generation_limit = endpoints
            .values()
            .next()
            .map_or(0, |endpoint| endpoint.source.stream_limit());
        Ok(Self {
            runtime,
            registration,
            source,
            generation_limit,
            close_order: endpoints.keys().copied().collect(),
            endpoints: Some(endpoints),
            calls,
            pending: None,
            active: BTreeMap::new(),
            phase_roles: BTreeMap::new(),
            active_order: VecDeque::new(),
            active_scan: RetirementScan::default(),
            closing: false,
            terminal_failures: BTreeMap::new(),
        })
    }

    pub(crate) fn backing_bytes(
        generations: usize,
        participants: usize,
    ) -> Result<usize, ObservationError> {
        use crate::task_execution::host::observations::backing;
        let generation_bytes = backing::sum([
            SubscriptionSource::<FrontendActor>::generation_backing_bytes()?,
            std::alloc::Layout::new::<BoundSubscription>().size(),
        ])?;
        let failure_bytes = SubscriptionFailureRecord::layout()
            .map_err(|_| ObservationError::Budget)?
            .size();
        let sized =
            |size: usize, count: usize| size.checked_mul(count).ok_or(ObservationError::Budget);
        backing::sum([
            std::alloc::Layout::new::<Self>().size(),
            backing::arc::<SubscriptionEndpoints>()?.size(),
            backing::tree::<ParticipationRef, SubscriptionEndpoint>(participants)?,
            backing::tree::<BackendProcessId, usize>(participants)?,
            backing::array::<StagedSubscriptionEndpoint>(participants)?,
            backing::tree::<ParticipationRef, AttemptCallSource>(participants)?,
            sized(
                backing::arc::<SubscriptionSource<FrontendActor>>()?.size(),
                participants,
            )?,
            backing::tree::<AttemptActionId, Active>(generations)?,
            backing::tree::<StandingInputId, AttemptActionId>(generations)?,
            backing::tree::<HostFailureRef, Arc<SubscriptionFailureRecord>>(participants)?,
            sized(backing::array::<AttemptActionId>(generations)?, 2)?,
            backing::array::<ParticipationRef>(participants)?,
            sized(generation_bytes, generations)?,
            sized(
                failure_bytes,
                generations
                    .checked_add(participants)
                    .ok_or(ObservationError::Budget)?,
            )?,
        ])
    }

    pub(crate) fn matches(&self, registration: &ActorRegistration, source: ActorId) -> bool {
        self.registration.same_registration(registration) && self.source == source
    }

    pub(crate) fn participants(&self) -> usize {
        self.calls.len()
    }

    pub(super) fn prepay(&mut self, effect: &Arc<AttemptEffectEnvelope>) -> Option<Position> {
        assert!(self.pending.is_none());
        if self.closing {
            return None;
        }
        let EffectBody::OpenSubscription(request) = &effect.protocol.body else {
            unreachable!("original open subscription prepayment");
        };
        let key = stream_key(request);
        let endpoint = &self
            .endpoints
            .as_ref()
            .expect("original subscription endpoints")[&key.participation];
        if !endpoint.source.can_register_generation() {
            return None;
        }
        let call = self
            .calls
            .get_mut(&key.participation)
            .expect("original subscription call");
        let result = call
            .prepay(effect)
            .expect("original subscription prepayment")?;
        let phase = self.runtime.reserve_admitted_input_for(
            &self.registration,
            self.source,
            InputClass::Lifecycle,
            call.pending_work(),
        );
        let phase = match phase {
            Ok(phase) => phase
                .into_standing_input()
                .unwrap_or_else(|(error, original)| {
                    original
                        .revoke_unused()
                        .expect("original unused subscription phase");
                    panic!("subscription phase could not bind: {error:?}");
                }),
            Err(error) => {
                call.return_unclaimed(result);
                assert!(
                    matches!(
                        error,
                        ActorTurnError::ActorFailed
                            | ActorTurnError::Closed
                            | ActorTurnError::InputCapacity
                    ),
                    "original subscription phase prepayment: {error:?}"
                );
                return None;
            }
        };
        self.pending = Some(Pending {
            key,
            phase: SubscriptionPhasePublisher::new(phase, key, |input| {
                FrontendInput::Attempt(Box::new(input))
            }),
        });
        Some(result)
    }

    pub(super) fn return_unclaimed(&mut self, result: Position) {
        let Pending { key, phase } = self
            .pending
            .take()
            .expect("original subscription prepayment");
        phase
            .close_after_exit()
            .expect("original unused phase publisher");
        self.calls
            .get_mut(&key.participation)
            .unwrap()
            .return_unclaimed(result);
    }

    pub(super) fn bind_job(
        &mut self,
        ticket: &EffectTicket<FrontendEffect>,
        result: Position,
    ) -> PreparedEffectJob {
        let Pending { key, phase } = self
            .pending
            .take()
            .expect("original subscription prepayment");
        let FrontendEffect::Attempt(effect) = &ticket.effect else {
            unreachable!()
        };
        let EffectBody::OpenSubscription(request) = &effect.protocol.body else {
            unreachable!()
        };
        assert_eq!(stream_key(request), key);
        let id = effect.protocol.id;
        let endpoint = &self.endpoints.as_ref().unwrap()[&key.participation];
        let failure = SubscriptionFailureRecord::new(id, key);
        let mut registered = false;
        let job = self
            .calls
            .get_mut(&key.participation)
            .unwrap()
            .bind_subscription(ticket, result, || {
                let original = match endpoint.source.prepare(request.clone(), &phase) {
                    Ok(original) => {
                        registered = true;
                        Some(original)
                    }
                    // The ordered writer's real failure fence can win after claim.
                    Err(
                        SubscriptionSourceError::Closed
                        | SubscriptionSourceError::Ingress(ObservationError::Closed),
                    ) => None,
                    Err(error) => panic!("original subscription registration: {error:?}"),
                };
                ActorTransportWork::Subscription(Box::new(BoundSubscription {
                    effect: id,
                    key,
                    original,
                    client: endpoint.client.clone(),
                    failure: failure.clone(),
                }))
            });
        let role = phase.role_id();
        assert!(self.phase_roles.insert(role, id).is_none());
        assert!(
            self.active
                .insert(
                    id,
                    Active {
                        key,
                        phase,
                        indexed: None,
                        failure,
                        body_exited: false,
                        call_retired: false,
                        phase_closed: false,
                        registered,
                        stop_requested: false,
                    }
                )
                .is_none()
        );
        let queued = self.active_order.len();
        assert!(
            queued < self.generation_limit,
            "original observation stream inventory"
        );
        if queued == self.active_order.capacity() {
            let next = queued.saturating_mul(2).max(4).min(self.generation_limit);
            self.active_order.reserve_exact(next - queued);
        }
        self.active_order.push_back(id);
        self.active_scan.request();
        job
    }

    pub(super) fn owns(&self, effect: AttemptActionId) -> bool {
        self.active.contains_key(&effect)
    }

    pub(super) fn core_consumed(&mut self, inputs: &[InputId]) {
        let mut advanced = false;
        for &input in inputs {
            let Some(role) = self
                .runtime
                .input_custody(self.registration.actor(), input)
                .expect("original subscription actor")
                .and_then(|row| row.standing_input_id)
            else {
                continue;
            };
            if let Some(effect) = self.phase_roles.get(&role) {
                let row = self
                    .active
                    .get_mut(effect)
                    .expect("original subscription phase role");
                assert!(row.indexed.replace(input).is_none());
                advanced = true;
            }
        }
        if advanced {
            self.active_scan.request();
            self.runtime
                .notify_host_actor(&self.registration)
                .expect("original subscription host wake");
        }
    }

    pub(super) fn accept(
        &mut self,
        effect: AttemptActionId,
        original: ReturnedActorTransportResult,
    ) -> Result<(), Box<dyn Any + Send>> {
        self.active_scan.request();
        let row = self
            .active
            .get_mut(&effect)
            .expect("original subscription result");
        if let Some(BodyOutcome::Call(reply)) = &original.outcome {
            let AttemptCallReply::Input {
                input:
                    AttemptInput::Executor(ExecutorFact::SubscriptionExited {
                        effect: exited,
                        participation,
                        generation,
                    }),
                ..
            } = reply
            else {
                panic!("expected the original subscription exit");
            };
            assert_eq!(
                (*exited, *participation, *generation),
                (effect, row.key.participation, row.key.generation)
            );
            row.body_exited = true;
        }
        self.calls
            .get_mut(&row.key.participation)
            .unwrap()
            .accept(original)
            .map_err(|refused| Box::new(refused) as Box<dyn Any + Send>)
    }

    pub(super) fn transport_retired(&mut self, effect: AttemptActionId) {
        self.active_scan.request();
        let row = &self.active[&effect];
        self.calls
            .get_mut(&row.key.participation)
            .unwrap()
            .transport_retired(effect);
    }

    fn failure(&self, reference: HostFailureRef) -> Option<&SubscriptionFailureRecord> {
        self.active
            .get(&AttemptActionId::new(reference.0))
            .map(|row| row.failure.as_ref())
            .or_else(|| self.terminal_failures.get(&reference).map(Arc::as_ref))
    }

    pub(super) fn project_failure(
        &self,
        reference: HostFailureRef,
    ) -> Option<ExecutionFailureInput> {
        self.failure(reference)?.project(reference)
    }

    pub(super) fn take_failure(&self, reference: HostFailureRef) -> Option<OriginalHostFailure> {
        self.failure(reference)?
            .take(reference)
            .map(OriginalHostFailure::Subscription)
    }

    pub(super) fn request_retirement_scan(&mut self) {
        self.active_scan.request();
    }

    pub(super) fn progress(
        &mut self,
        failed: bool,
        closing: bool,
        retire_failures: bool,
        budget: usize,
        mut dispose: impl FnMut(ReleaseCapture),
    ) -> SourceProgress {
        if !self.closing && (failed || closing) {
            self.closing = true;
            self.active_scan.request();
        }
        let mut units = 0;
        let mut progressed = false;
        if self.closing {
            while units < budget
                && let Some(participation) = self.close_order.pop_front()
            {
                self.endpoints.as_ref().unwrap()[&participation]
                    .source
                    .close_admission();
                progressed = true;
                units += 1;
            }
        }
        let visits = self
            .active_scan
            .take_visits(self.active_order.len(), budget - units);
        for _ in 0..visits {
            let effect = self
                .active_order
                .pop_front()
                .expect("original generation scan");
            let (advanced, reaped) = self.advance_active(effect, failed, &mut dispose);
            progressed |= advanced;
            if !reaped {
                self.active_order.push_back(effect);
            }
        }
        units += visits;
        if self.closing && self.close_order.is_empty() && self.active.is_empty() {
            while units < budget
                && let Some((participation, mut call)) = self.calls.pop_first()
            {
                if let Some(owner) = self.endpoints.as_ref().unwrap()[&participation]
                    .source
                    .take_owner_after_jobs()
                    .expect("all original subscription generations reaped")
                {
                    dispose(ReleaseCapture::Subscription(owner));
                }
                assert!(
                    call.close_idle()
                        .expect("original subscription result position")
                );
                assert!(call.take_drained_owner().is_none() && call.is_drained());
                progressed = true;
                units += 1;
            }
            if retire_failures {
                while units < budget
                    && let Some((_, original)) = self.terminal_failures.pop_first()
                {
                    dispose(ReleaseCapture::Original(Box::new(original)));
                    progressed = true;
                    units += 1;
                }
            }
            if units < budget
                && self.calls.is_empty()
                && self.terminal_failures.is_empty()
                && let Some(original) = self.endpoints.take()
            {
                dispose(ReleaseCapture::Original(Box::new(original)));
                progressed = true;
                units += 1;
            }
        }
        if progressed {
            self.active_scan.request();
        }
        SourceProgress {
            units,
            progressed,
            more: self.active_scan.has_work(self.active_order.len())
                || self.closing && !self.close_order.is_empty()
                || progressed && !self.is_drained(),
            retired: self.is_drained(),
        }
    }

    fn advance_active(
        &mut self,
        effect: AttemptActionId,
        failed: bool,
        dispose: &mut impl FnMut(ReleaseCapture),
    ) -> (bool, bool) {
        let row = self
            .active
            .get_mut(&effect)
            .expect("original subscription generation");
        let mut progressed = false;
        if self.closing && row.registered && !row.body_exited && !row.stop_requested {
            self.endpoints.as_ref().unwrap()[&row.key.participation]
                .source
                .close_generation(row.key.generation)
                .expect("original live subscription");
            row.stop_requested = true;
            progressed = true;
        }
        if !row.phase_closed {
            if failed {
                progressed |= row
                    .phase
                    .retire_after_failure()
                    .expect("original failed subscription phase");
                row.indexed = None;
            } else if let Some(input) = row.indexed
                && row
                    .phase
                    .retire_indexed_after_exit(input)
                    .expect("original indexed subscription phase")
            {
                row.indexed = None;
                progressed = true;
            }
            if row.body_exited {
                match row.phase.close_after_exit() {
                    Ok(()) => {
                        assert_eq!(self.phase_roles.remove(&row.phase.role_id()), Some(effect));
                        row.phase_closed = true;
                        progressed = true;
                    }
                    Err(ObservationError::Busy) => {}
                    Err(error) => panic!("original subscription phase close: {error:?}"),
                }
            }
        }
        if row.phase_closed && !row.call_retired {
            row.call_retired = self
                .calls
                .get_mut(&row.key.participation)
                .unwrap()
                .retire_result()
                .expect("original subscription exit result");
            progressed |= row.call_retired;
        }
        if !row.call_retired {
            return (progressed, false);
        }
        if row.registered {
            let endpoint = &self.endpoints.as_ref().unwrap()[&row.key.participation];
            match endpoint.source.reap_generation(row.key.generation) {
                Ok(original) => dispose(ReleaseCapture::Original(Box::new(original))),
                Err(
                    SubscriptionSourceError::JobsStillLive
                    | SubscriptionSourceError::Ingress(ObservationError::Busy),
                ) => {
                    return (progressed, false);
                }
                Err(error) => panic!("original subscription generation retirement: {error:?}"),
            }
        }
        let row = self
            .active
            .remove(&effect)
            .expect("same original generation");
        if row.failure.is_terminal_candidate() {
            assert!(
                self.terminal_failures
                    .insert(row.failure.reference(), row.failure)
                    .is_none()
            );
        } else {
            dispose(ReleaseCapture::Original(Box::new(row.failure)));
        }
        (true, true)
    }

    pub(super) fn local_exited(&self) -> bool {
        // Retained original failure evidence can outlive the actual readers.
        self.closing
            && self.close_order.is_empty()
            && self.active.is_empty()
            && self.calls.is_empty()
    }

    pub(super) fn is_drained(&self) -> bool {
        self.endpoints.is_none()
    }
}

#[cfg(test)]
mod tests;
