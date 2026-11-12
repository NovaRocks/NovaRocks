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

//! Bind the pure AttemptCore to the original FE actor turn and work authors.

use super::failure_bridge::PendingFailureBridge;
use super::failure_lifecycle::FailureLifecycle;
use super::failure_publication::{FailureContext, FailurePublications};
use super::failure_supervisor::FailureSupervisor;
use super::observations::{
    ObservationBudget, ObservationError, ObservationSourcePool, OrderedObservationInput,
    RootControlBinding,
    backing::{ObservationBackingPin, ObservationBackingQuote, PaidObservationMetadata},
    credits::ObservationCredits,
    host_turn::OrderedObservationHost,
    read_progress::ReadProgressInput,
    source::BoundObservationSource,
};
use super::pumps::{AttemptPumpSource, DiagnosticRecords, DynamicFilterRecords, split::SplitPump};
use super::startup::{AttemptStartup, AttemptStartupSource, StartupInput};
#[cfg(test)]
use super::transport::{NativeReceiptInput, SendKey};
use crate::{
    control_runtime::{
        actor::{FrontendActor, FrontendActorKind, FrontendEffect, FrontendInput},
        host::{EffectExecutor, EffectSource, FrontendLanePolicy, HostBinding},
        membership::{FrozenMember, MembershipRegistration, MembershipTarget, membership_input},
    },
    native::{
        data_runtime::FrontendDataRuntime,
        task_transport::wire::AttemptWireFacts,
        transport_capacity::{
            NativeTransportCapacity, TransportCapacityLane, TransportCapacityRefusal,
        },
    },
    task_execution::{
        bridge::{control::AttemptOwnerControls, root_control::RootControlRequests},
        host::custody::TransportRetirements,
    },
};
use novarocks_attempt_protocol::{
    AttemptCore, Core, InputSelection,
    capacity::{QueueStage, ReservationToken, StepContext},
    config::DispatchLane,
    effect::{AttemptEffect, EffectBody, EffectClass, PumpKind},
    failure::AttemptFailure,
    input::{
        AttemptInput, AttemptInputClaim, AttemptInputId, AttemptSourceCut, AttemptSourceId,
        ExecutorFact,
    },
    start::{AttemptStart, PayloadRef},
};
use novarocks_control_runtime::{
    actor::{ActorConfig, ActorStep, InputSourceCut},
    actor_turn::{
        ActorRegistration, ActorRuntime, ActorTurnError, InputAuthor, StandingInputObligation,
    },
    capacity_notification::CapacityRegistrant,
    causal_input::CausalInputObligation,
    coalescing_input::{CoalescingInput, Merge},
    obligation::{ReservationObligation, StepRegistration, WorkObligation},
    transaction::{RuntimeError, RuntimeLedger, ScopeHandle},
};
use novarocks_control_runtime_core::{
    capacity::ReservationResources,
    effect::{EffectDescriptor, EffectKind},
    identity::{
        ActionId, ActorId, ConsumerId, Generation, InputId, ReservationId, ScopedWorkRef, Tick,
    },
    mailbox::{InputClass, InputEnvelope},
    owned_storage::OwnedSlotNeed,
    scope::WorkKind,
};
use novarocks_coordination_contract::{LogicalToAttemptBody, MonotonicClock, MonotonicInstant};
use novarocks_execution_contract::{ParticipationRef, TaskOperationId};
use novarocks_types::{BackendProcessId, NativeCompatibilityId};
use novarocks_workload_control::joint_admission::NativeAsyncActionAuthority;
use std::{
    collections::{BTreeMap, VecDeque},
    num::NonZeroU64,
    sync::{Arc, OnceLock},
    task::Waker,
    time::Duration,
};

// The Core returns at most one primary effect, one Abort settlement and one
// passive report. ActorStep charges their actual handoff separately.
pub(super) const OUTPUT_UNITS: usize = 3;

pub(crate) struct AttemptActor {
    actor: ActorId,
    core: Core,
    scope: ScopeHandle,
    capacity: NativeTransportCapacity,
    binding: Arc<OnceLock<AttemptBinding>>,
}

pub(crate) struct AttemptActorRegistration {
    pub(crate) original: ActorRegistration,
    scope: ScopeHandle,
    transport_capacity: NativeTransportCapacity,
    compatibility: NativeCompatibilityId,
    native_source: ActorId,
    transport_source: ActorId,
    subscription_source: ActorId,
    qa_source: ActorId,
    process_source: ActorId,
    publication_source: ActorId,
    capacity_source: ActorId,
    observation_source: ActorId,
    membership_source: ActorId,
    members: Option<Arc<[FrozenMember]>>,
    failure_members: Arc<[FrozenMember]>,
    failure_lifecycle: Option<FailureLifecycle>,
    failure_publications: Arc<FailurePublications>,
    failure_work: ScopedWorkRef,
    dynamic_pump: Option<PayloadRef>,
    split_pump: Option<PayloadRef>,
    retain_diagnostics: bool,
    root_control: RootControlBinding,
    ordered_host: Option<OrderedObservationHost>,
    ordered_bound: bool,
    read_progress_roles: Option<Vec<ReadProgressInput>>,
    startup_reserved: bool,
    abort_bound: bool,
    config: ActorConfig,
    binding: Arc<OnceLock<AttemptBinding>>,
}

pub(crate) struct ObservationSourceBacking<'a> {
    pub(crate) work: &'a WorkObligation,
    pub(crate) positions: &'a mut ReservationObligation,
}

pub(crate) struct AttemptEffectAuthors {
    pub(crate) business: InputAuthor,
    pub(crate) publication: InputAuthor,
    pub(crate) reply: InputAuthor,
    pub(crate) cleanup: InputAuthor,
    pub(crate) settlement: InputAuthor,
}

struct AttemptBinding {
    root_control: RootControlBinding,
    authors: AttemptEffectAuthors,
    generation: Generation,
    queue_parent: ReservationId,
    capacity_registrant: Arc<CapacityRegistrant>,
}

#[must_use]
pub(crate) struct PreparedAttemptHost {
    pub(crate) binding: HostBinding,
    pub(crate) startup: AttemptStartup,
    pub(crate) controls: AttemptOwnerControls,
    pub(crate) membership: MembershipRegistration,
    pub(crate) failure: PendingFailureBridge,
    pub(crate) terminal: Arc<crate::task_execution::bridge::terminal::TerminalOutbox>,
}
type AttemptHostPreparationRefusal = (
    AttemptRegistrationError,
    AttemptCapacitySource,
    MembershipRegistration,
    Box<dyn EffectSource>,
    AttemptTransportBinding,
);

pub(crate) struct AttemptTransportBinding {
    pub(crate) retirement: TransportRetirements,
    pub(crate) admission: super::transport::AdmissionSource,
    pub(crate) qa: super::transport::AttemptCallSource,
    pub(crate) abort: Arc<crate::task_execution::bridge::abort::AbortSettlementOwner>,
    pub(crate) process: super::transport::AttemptCallSource,
    pub(crate) publication: super::transport::AttemptCallSource,
    pub(crate) release: super::transport::AttemptCallSource,
    pub(crate) read_confirmation: super::transport::AttemptCallSource,
    pub(crate) subscription_control: super::transport::AttemptCallSource,
    pub(crate) subscriptions: super::transport::AttemptSubscriptions,
    pub(crate) send: super::transport::AttemptSendSource,
    pub(crate) pumps: AttemptPumpSource,
    pub(crate) diagnostics: Arc<super::publication::AttemptDiagnosticSources>,
    pub(crate) next_payload: NonZeroU64,
    pub(crate) executor: super::transport::ActorTransportExecutor,
    pub(crate) failure: FailureSupervisor,
}

pub(crate) struct PreparedNativeSubscriptions {
    pub(crate) original: super::transport::AttemptSubscriptions,
    pub(crate) control: super::transport::AttemptCallSource,
    pub(crate) endpoints: Arc<super::transport::SubscriptionEndpoints>,
    data: FrontendDataRuntime,
}

pub(crate) struct AttemptSendInputs<'a> {
    pub(crate) ledger: &'a RuntimeLedger,
    pub(crate) authority: NativeAsyncActionAuthority,
    pub(crate) clock: Arc<dyn MonotonicClock>,
    pub(crate) observer: ConsumerId,
    pub(crate) result_counts: [usize; 2],
}

type TransportResultPositions = [VecDeque<CausalInputObligation<FrontendActor>>; 2];

struct CapacityReleased;
impl Merge for CapacityReleased {
    fn merge(&mut self, _: Self) {}
    fn unit() -> Self {
        Self
    }
}

/// The host retains the only standing input and cancels its real ledger
/// wait before retiring it after the original holder exits.
pub(crate) struct AttemptCapacitySource {
    registration: ActorRegistration,
    input: CoalescingInput<FrontendActor, CapacityReleased>,
    registrant: Arc<CapacityRegistrant>,
    indexed: Option<InputId>,
    closing: bool,
    closed: bool,
}

impl AttemptCapacitySource {
    pub(crate) fn new(
        registration: ActorRegistration,
        standing: StandingInputObligation<FrontendActor>,
        registrant: Arc<CapacityRegistrant>,
    ) -> Self {
        assert!(standing.matches_registration(&registration));
        let input = CoalescingInput::new(standing, |_| FrontendInput::CapacityReleased);
        registrant.update_waker(&input.waker());
        Self {
            registration,
            input,
            registrant,
            indexed: None,
            closing: false,
            closed: false,
        }
    }

    pub(crate) fn matches(&self, registration: &ActorRegistration) -> bool {
        self.registration.same_registration(registration)
    }

    pub(crate) fn index_committed(&mut self, inputs: &[InputId]) -> Result<bool, ActorTurnError> {
        if inputs.is_empty() {
            return Ok(false);
        }
        let Some(input) = self
            .input
            .in_flight()?
            .filter(|input| inputs.contains(input))
        else {
            return Ok(false);
        };
        assert!(self.indexed.replace(input).is_none());
        Ok(true)
    }

    pub(crate) fn needs_progress(&self, failed: bool, closing: bool) -> bool {
        self.indexed.is_some() || failed || closing
    }

    pub(crate) fn progress(&mut self, failed: bool, closing: bool) -> Result<bool, ActorTurnError> {
        if (closing || failed) && !self.closing {
            self.close();
            self.closing = true;
        }
        if failed {
            if let Some(payload) = self.input.take_failed_role_payload()? {
                // This Source only ever fills a neutral CapacityReleased
                // marker. The actual FIFO payload must still be disposed.
                payload.dispose()?;
                return Ok(true);
            }
            self.input.retire_failed_after_exit()?;
            self.indexed = None;
        } else if let Some(input) = self.indexed
            && self.retire_indexed(input)?
        {
            self.indexed = None;
            return Ok(self.closing);
        }
        if self.closing && self.input.in_flight()?.is_none() {
            self.input.close_after_exit()?;
            self.closed = true;
        }
        Ok(false)
    }

    pub(crate) fn is_closed(&self) -> bool {
        self.closed
    }

    fn retire_indexed(&self, input: InputId) -> Result<bool, ActorTurnError> {
        match self.input.retire_indexed_after_exit(input) {
            Ok(retired) => Ok(retired),
            Err(ActorTurnError::Closed | ActorTurnError::ActorFailed) => Ok(true),
            Err(error) => Err(error),
        }
    }

    #[cfg(test)]
    fn retire_ready(&self) -> Result<bool, ActorTurnError> {
        match self.input.in_flight()? {
            Some(input) => self.retire_indexed(input),
            None => Ok(false),
        }
    }

    fn close(&self) {
        self.input.close();
        self.registrant.cancel();
    }

    #[cfg(test)]
    fn close_after_exit(&self) -> Result<(), ActorTurnError> {
        self.close();
        self.input.close_after_exit()
    }
}

#[derive(Clone, Debug)]
pub(crate) struct AttemptNotice {
    pub(crate) definitely_unsent_abort: Option<TaskOperationId>,
    pub(crate) retired_authorization: Option<TaskOperationId>,
    pub(crate) diagnostic_failure: Option<AttemptFailure>,
    pub(crate) round_failure: Option<AttemptFailure>,
}

impl AttemptNotice {
    pub(crate) fn requires_reporting(&self) -> bool {
        self.diagnostic_failure.is_some() || self.round_failure.is_some()
    }
}

#[derive(Debug)]
#[allow(
    dead_code,
    reason = "Installation reports the original preparation or registration error through Debug."
)]
pub(crate) enum AttemptRegistrationError {
    InvalidConfig(&'static str),
    Core(AttemptFailure),
    Runtime(ActorTurnError),
    Subscription(super::transport::SubscriptionEndpointError),
}

impl AttemptActorRegistration {
    pub(crate) fn prepare_auxiliary_source(
        &self,
        runtime: &ActorRuntime<FrontendActor>,
        work: &WorkObligation,
    ) -> Result<
        (
            super::auxiliary::AttemptAuxiliarySource,
            super::auxiliary::AttemptInfrastructureHealth,
        ),
        ActorTurnError,
    > {
        let input =
            self.reserve_standing(runtime, self.original.actor(), InputClass::Lifecycle, work)?;
        Ok(super::auxiliary::AttemptAuxiliarySource::new(
            self.original.clone(),
            input,
        ))
    }

    pub(crate) fn prepare_pump_source(
        &self,
        runtime: &ActorRuntime<FrontendActor>,
        parent: &WorkObligation,
        payer: &ReservationObligation,
        observer: ConsumerId,
        dynamic: Option<Arc<DynamicFilterRecords>>,
        diagnostics: Option<Arc<DiagnosticRecords>>,
        split: Option<&Arc<SplitPump>>,
    ) -> Result<AttemptPumpSource, AttemptRegistrationError> {
        if self.dynamic_pump.is_some() != dynamic.is_some()
            || self.retain_diagnostics != diagnostics.is_some()
            || self.split_pump != split.map(|pump| pump.reference())
        {
            return Err(AttemptRegistrationError::InvalidConfig(
                "pump Sources differ from the original attempt plan",
            ));
        }
        AttemptPumpSource::bind(
            runtime,
            &self.original,
            // Pump callbacks are independent facts, outside the Native ACK prefix.
            self.transport_source,
            parent,
            payer,
            observer,
            self.dynamic_pump.zip(dynamic),
            diagnostics,
            split,
        )
        .map_err(AttemptRegistrationError::Runtime)
    }

    /// The original QA reply position is separate from Native admission and
    /// observation credits. Only one original QA request can be in flight.
    pub(crate) fn prepare_qa_source(
        &self,
        runtime: &ActorRuntime<FrontendActor>,
        parent: &WorkObligation,
        payer: &ReservationObligation,
        observer: novarocks_control_runtime_core::identity::ConsumerId,
        bridge: crate::task_execution::bridge::qa::AttemptQaBridge,
    ) -> Result<super::transport::AttemptCallSource, AttemptRegistrationError> {
        if bridge.activation() != self.root_control.activation() {
            return Err(AttemptRegistrationError::InvalidConfig(
                "QA Source belongs to another original activation",
            ));
        }
        self.prepare_call_source(
            runtime,
            parent,
            payer,
            observer,
            self.qa_source,
            super::transport::AttemptCallOwner::Qa(Arc::new(tokio::sync::Mutex::new(bridge))),
        )
    }

    pub(crate) fn prepare_process_source(
        &self,
        runtime: &ActorRuntime<FrontendActor>,
        parent: &WorkObligation,
        payer: &ReservationObligation,
        observer: novarocks_control_runtime_core::identity::ConsumerId,
        source: novarocks_query_application::api::BackendProcessObservationService,
        clock: Arc<dyn novarocks_control_runtime::actor_turn::Clock>,
    ) -> Result<super::transport::AttemptCallSource, AttemptRegistrationError> {
        let original = super::transport::AttemptProcessObserver::new(
            source,
            self.failure_members.clone(),
            clock,
        );
        self.prepare_call_source(
            runtime,
            parent,
            payer,
            observer,
            self.process_source,
            super::transport::AttemptCallOwner::Process(Arc::new(original)),
        )
    }

    pub(crate) fn prepare_publication_source(
        &self,
        runtime: &ActorRuntime<FrontendActor>,
        parent: &WorkObligation,
        payer: &ReservationObligation,
        observer: novarocks_control_runtime_core::identity::ConsumerId,
        owner: Arc<super::publication::AttemptPublicationOwner>,
    ) -> Result<super::transport::AttemptCallSource, AttemptRegistrationError> {
        if owner.activation() != self.root_control.activation()
            || !owner.matches_root(&self.root_control)
        {
            return Err(AttemptRegistrationError::InvalidConfig(
                "publication Source belongs to another original activation",
            ));
        }
        self.prepare_call_source(
            runtime,
            parent,
            payer,
            observer,
            self.publication_source,
            super::transport::AttemptCallOwner::Publication(owner),
        )
    }

    pub(crate) fn prepare_transport_release_source(
        &self,
        runtime: &ActorRuntime<FrontendActor>,
        parent: &WorkObligation,
        payer: &ReservationObligation,
        observer: novarocks_control_runtime_core::identity::ConsumerId,
    ) -> Result<super::transport::AttemptCallSource, AttemptRegistrationError> {
        self.prepare_call_source(
            runtime,
            parent,
            payer,
            observer,
            self.transport_source,
            super::transport::AttemptCallOwner::Release,
        )
    }

    pub(crate) fn prepare_read_confirmation_source(
        &self,
        runtime: &ActorRuntime<FrontendActor>,
        parent: &WorkObligation,
        payer: &ReservationObligation,
        observer: novarocks_control_runtime_core::identity::ConsumerId,
        ingress: OrderedObservationInput<FrontendActor>,
    ) -> Result<super::transport::AttemptCallSource, AttemptRegistrationError> {
        if !ingress.root_binding().same_original(&self.root_control) {
            return Err(AttemptRegistrationError::InvalidConfig(
                "read confirmation Source belongs to another original activation",
            ));
        }
        self.prepare_call_source(
            runtime,
            parent,
            payer,
            observer,
            self.subscription_source,
            super::transport::AttemptCallOwner::ReadConfirmation(ingress),
        )
    }

    pub(crate) fn bind_staged_subscriptions(
        &self,
        runtime: &ActorRuntime<FrontendActor>,
        parent: &WorkObligation,
        payer: &ReservationObligation,
        observer: novarocks_control_runtime_core::identity::ConsumerId,
        staged: super::transport::StagedSubscriptionEndpoints,
        ingress: &OrderedObservationInput<FrontendActor>,
    ) -> Result<PreparedNativeSubscriptions, AttemptRegistrationError> {
        if !ingress.root_binding().same_original(&self.root_control)
            || !self.scope.same_scope(parent.scope())
            || !payer.belongs_to_bound_work(&self.scope, parent.id())
        {
            return Err(AttemptRegistrationError::InvalidConfig(
                "subscription Source differs from the original activation or payer",
            ));
        }
        let data = staged.data().clone();
        let endpoints = staged
            .bind(&self.failure_members, self.compatibility, ingress)
            .map_err(AttemptRegistrationError::Subscription)?;
        let mut control = match self.prepare_call_source(
            runtime,
            parent,
            payer,
            observer,
            self.subscription_source,
            super::transport::AttemptCallOwner::SubscriptionControl(endpoints.clone()),
        ) {
            Ok(original) => original,
            Err(error) => {
                super::transport::retire_unpublished_endpoints(&endpoints);
                return Err(error);
            }
        };
        match super::transport::AttemptSubscriptions::bind(
            runtime.clone(),
            self.original.clone(),
            self.subscription_source,
            &self.root_control,
            parent,
            payer,
            observer,
            endpoints.clone(),
        ) {
            Ok(original) => Ok(PreparedNativeSubscriptions {
                original,
                control,
                endpoints,
                data,
            }),
            Err(error) => {
                assert!(
                    control
                        .close_idle()
                        .expect("original unpublished subscription control")
                );
                super::transport::retire_unpublished_endpoints(&endpoints);
                Err(AttemptRegistrationError::Runtime(error))
            }
        }
    }

    /// Use the exact clients already selected from this attempt's frozen roster.
    /// The returned paid result inventory is independent of admission results.
    pub(crate) fn prepare_send_routes(
        &self,
        runtime: &ActorRuntime<FrontendActor>,
        parent: &WorkObligation,
        subscriptions: &PreparedNativeSubscriptions,
        inputs: AttemptSendInputs<'_>,
    ) -> Result<
        (
            Arc<BTreeMap<BackendProcessId, super::transport::SendContext>>,
            TransportResultPositions,
        ),
        AttemptRegistrationError,
    > {
        if !self.scope.belongs_to_ledger(inputs.ledger)
            || !subscriptions
                .original
                .matches(&self.original, self.subscription_source)
        {
            return Err(AttemptRegistrationError::InvalidConfig(
                "send Source differs from the original attempt or frozen transport",
            ));
        }
        let idle = self.reserve_transport_results(runtime, parent, inputs.result_counts)?;
        let contexts = subscriptions
            .endpoints
            .iter()
            .map(|(participation, endpoint)| {
                let context = super::transport::SendContext {
                    registration: self.original.clone(),
                    ledger: inputs.ledger.clone(),
                    scope: self.scope.clone(),
                    layout: self.transport_capacity.clone(),
                    transport: self.transport_capacity.transport_budget(),
                    authority: inputs.authority.clone(),
                    wire: AttemptWireFacts {
                        native_compatibility_id: self.compatibility,
                    },
                    client: endpoint.client.clone(),
                    apply: subscriptions.data.apply_send_port().clone(),
                    clock: inputs.clock.clone(),
                    observer: inputs.observer,
                };
                (participation.backend_process_id(), context)
            })
            .collect();
        Ok((Arc::new(contexts), idle))
    }

    pub(crate) fn reserve_transport_result(
        &self,
        runtime: &ActorRuntime<FrontendActor>,
        parent: &WorkObligation,
    ) -> Result<CausalInputObligation<FrontendActor>, ActorTurnError> {
        if !self.scope.same_scope(parent.scope()) {
            return Err(ActorTurnError::InputAuthorMismatch);
        }
        CausalInputObligation::reserve_admitted(
            runtime,
            &self.original,
            self.transport_source,
            InputClass::Lifecycle,
            parent,
        )
    }

    fn prepare_call_source(
        &self,
        runtime: &ActorRuntime<FrontendActor>,
        parent: &WorkObligation,
        payer: &ReservationObligation,
        observer: novarocks_control_runtime_core::identity::ConsumerId,
        source: ActorId,
        owner: super::transport::AttemptCallOwner,
    ) -> Result<super::transport::AttemptCallSource, AttemptRegistrationError> {
        let binding = self
            .binding
            .get()
            .ok_or(AttemptRegistrationError::InvalidConfig(
                "call Source requires its original attempt binding",
            ))?;
        if !self.scope.same_scope(parent.scope())
            || !payer.belongs_to_bound_work(&self.scope, parent.id())
        {
            return Err(AttemptRegistrationError::InvalidConfig(
                "call Source belongs to another original attempt",
            ));
        }
        let position = CausalInputObligation::reserve_admitted(
            runtime,
            &self.original,
            source,
            InputClass::Lifecycle,
            parent,
        )
        .map_err(AttemptRegistrationError::Runtime)?;
        Ok(super::transport::AttemptCallSource::new(
            parent,
            payer,
            binding.generation,
            observer,
            owner,
            position,
        ))
    }

    /// Pay the two independent admission result inventories against the
    /// original Native mailbox before opening the attempt. The Source may
    /// later derive fresh per-generation Work from the same bound payer.
    pub(crate) fn prepare_admission_source(
        &self,
        runtime: &ActorRuntime<FrontendActor>,
        parent: &WorkObligation,
        payer: &ReservationObligation,
        observer: novarocks_control_runtime_core::identity::ConsumerId,
        authority: novarocks_workload_control::joint_admission::NativeAsyncActionAuthority,
        counts: [usize; 2],
    ) -> Result<super::transport::AdmissionSource, AttemptRegistrationError> {
        let binding = self
            .binding
            .get()
            .ok_or(AttemptRegistrationError::InvalidConfig(
                "admission Source requires its original attempt binding",
            ))?;
        if !self.scope.same_scope(parent.scope())
            || !payer.belongs_to_bound_work(&self.scope, parent.id())
        {
            return Err(AttemptRegistrationError::InvalidConfig(
                "admission Source payer belongs to another original Work",
            ));
        }
        let positions = self.reserve_transport_results(runtime, parent, counts)?;
        Ok(super::transport::AdmissionSource::new(
            parent,
            payer,
            binding.generation,
            observer,
            authority,
            positions,
        )
        .unwrap_or_else(|original| std::panic::resume_unwind(Box::new(original))))
    }

    fn reserve_transport_results(
        &self,
        runtime: &ActorRuntime<FrontendActor>,
        parent: &WorkObligation,
        counts: [usize; 2],
    ) -> Result<TransportResultPositions, AttemptRegistrationError> {
        let mut positions = counts.map(VecDeque::with_capacity);
        for (lane, count) in counts.into_iter().enumerate() {
            for _ in 0..count {
                match self.reserve_transport_result(runtime, parent) {
                    Ok(position) => positions[lane].push_back(position),
                    Err(error) => {
                        for mut original in positions
                            .iter_mut()
                            .flat_map(|positions| positions.drain(..))
                        {
                            original
                                .close_after_exit()
                                .expect("unpublished transport result position");
                        }
                        return Err(AttemptRegistrationError::Runtime(error));
                    }
                }
            }
        }
        Ok(positions)
    }

    pub(crate) fn prepare_abort_ingress(
        &mut self,
        runtime: &ActorRuntime<FrontendActor>,
        work: &WorkObligation,
        capacity: tokio::sync::watch::Sender<u64>,
        worker_closed: Option<
            Arc<dyn Fn(novarocks_execution_contract::ParticipationRef) + Send + Sync>,
        >,
    ) -> Result<crate::task_execution::bridge::abort::AttemptAbortIngress, ActorTurnError> {
        if self.abort_bound || !self.scope.same_scope(work.scope()) {
            return Err(ActorTurnError::InputAuthorMismatch);
        }
        let mut positions = Vec::with_capacity(self.failure_members.len());
        for member in &*self.failure_members {
            match self.reserve_standing(runtime, self.original.actor(), InputClass::Control, work) {
                Ok(position) => positions.push((member.participation, position)),
                Err(error) => {
                    for (_, mut position) in positions {
                        position
                            .close_after_exit()
                            .expect("unpublished Abort Control position");
                    }
                    return Err(error);
                }
            }
        }
        self.abort_bound = true;
        Ok(
            crate::task_execution::bridge::abort::AttemptAbortIngress::new(
                self.original.clone(),
                self.root_control.activation(),
                positions,
                capacity,
                worker_closed,
            ),
        )
    }

    fn prepare_owner_controls(
        &self,
        runtime: &ActorRuntime<FrontendActor>,
        work: &WorkObligation,
    ) -> Result<AttemptOwnerControls, ActorTurnError> {
        let mut stop =
            self.reserve_standing(runtime, self.original.actor(), InputClass::Control, work)?;
        let convergence = match self.reserve_standing(
            runtime,
            self.original.actor(),
            InputClass::Control,
            work,
        ) {
            Ok(position) => position,
            Err(error) => {
                stop.close_after_exit().expect("unpublished Stop position");
                return Err(error);
            }
        };
        Ok(AttemptOwnerControls::new(
            self.root_control.activation(),
            stop,
            convergence,
        ))
    }

    pub(crate) fn prepare_host(
        &mut self,
        runtime: &ActorRuntime<FrontendActor>,
        startup_work: &WorkObligation,
        capacity: AttemptCapacitySource,
        membership: MembershipRegistration,
        source: Box<dyn EffectSource>,
        executor: &Arc<dyn EffectExecutor>,
        transport: AttemptTransportBinding,
    ) -> Result<PreparedAttemptHost, AttemptHostPreparationRefusal> {
        let transient_calls = [
            &transport.qa,
            &transport.process,
            &transport.publication,
            &transport.release,
            &transport.read_confirmation,
            &transport.subscription_control,
        ]
        .len();
        let required_joins = transport
            .admission
            .idle_positions()
            .checked_add(
                transport
                    .send
                    .idle
                    .iter()
                    .map(std::collections::VecDeque::len)
                    .sum(),
            )
            .and_then(|count| count.checked_add(transient_calls))
            .and_then(|count| count.checked_add(transport.subscriptions.participants()))
            .and_then(|count| count.checked_add(transport.pumps.capacity()));
        if self.startup_reserved
            || !self.ordered_bound
            || self.read_progress_roles.is_none()
            || !membership
                .target
                .registration
                .same_registration(&self.original)
            || !capacity.matches(&self.original)
            || !self.binding.get().is_some_and(|binding| {
                Arc::ptr_eq(&binding.capacity_registrant, &capacity.registrant)
            })
            || !self.scope.same_scope(startup_work.scope())
            || !transport
                .abort
                .matches(&self.original, self.root_control.activation())
            || !transport
                .subscriptions
                .matches(&self.original, self.subscription_source)
            || required_joins.is_none_or(|count| count > transport.retirement.capacity())
            || !transport.pumps.matches(&self.original)
            || !transport
                .retirement
                .matches_registration(runtime, &self.original)
            || !transport
                .failure
                .matches(runtime, &self.original, self.failure_work)
        {
            return Err((
                AttemptRegistrationError::InvalidConfig(
                    "attempt startup requires its original bound Sources and Work",
                ),
                capacity,
                membership,
                source,
                transport,
            ));
        }
        let mut input = match self.reserve_standing(
            runtime,
            self.original.actor(),
            InputClass::Lifecycle,
            startup_work,
        ) {
            Ok(input) => input,
            Err(error) => {
                return Err((
                    AttemptRegistrationError::Runtime(error),
                    capacity,
                    membership,
                    source,
                    transport,
                ));
            }
        };
        let mut local_exit = match self.reserve_standing(
            runtime,
            self.original.actor(),
            InputClass::Lifecycle,
            startup_work,
        ) {
            Ok(position) => position,
            Err(error) => {
                input
                    .close_after_exit()
                    .expect("unpublished startup position");
                return Err((
                    AttemptRegistrationError::Runtime(error),
                    capacity,
                    membership,
                    source,
                    transport,
                ));
            }
        };
        let mut controls = match self.prepare_owner_controls(runtime, startup_work) {
            Ok(controls) => controls,
            Err(error) => {
                local_exit
                    .close_after_exit()
                    .expect("unpublished local exit position");
                input
                    .close_after_exit()
                    .expect("unpublished startup position");
                return Err((
                    AttemptRegistrationError::Runtime(error),
                    capacity,
                    membership,
                    source,
                    transport,
                ));
            }
        };
        let owner_controls = controls
            .bind_host(self.original.clone())
            .with_local_exit(local_exit);
        self.startup_reserved = true;
        let AttemptTransportBinding {
            retirement,
            admission,
            mut qa,
            abort,
            process,
            publication,
            release,
            read_confirmation,
            subscription_control,
            subscriptions,
            send,
            pumps,
            diagnostics,
            next_payload,
            executor: native,
            failure,
        } = transport;
        let terminal = crate::task_execution::bridge::terminal::TerminalOutbox::new(
            self.root_control.activation(),
        );
        qa.install_terminal(terminal.clone());
        qa.install_abort(abort.clone());
        let (failure_bridge, residual) = PendingFailureBridge::new(
            self.original.clone(),
            self.root_control.activation(),
            self.failure_members.clone(),
            self.failure_publications.clone(),
        );
        let (abort_drain, abort_retirement) = tokio::sync::oneshot::channel();
        let cleanup = super::transport::NativeFailureCleanup::new(
            send.contexts.clone(),
            failure.reservation_id(),
            native.0.clone(),
            residual,
        )
        .with_abort_drain(abort.clone(), abort_drain);
        let ordered_host = self
            .ordered_host
            .take()
            .expect("original ordered writer was bound")
            .with_read_progress(
                self.read_progress_roles
                    .take()
                    .expect("original progress roles"),
            );
        let (startup_source, startup) = AttemptStartupSource::new(self.original.clone(), input);
        Ok(PreparedAttemptHost {
            binding: HostBinding {
                scope: self.scope.clone(),
                registration: self.original.clone(),
                kind: FrontendActorKind::Attempt,
                config: self.config,
                policy: FrontendLanePolicy::attempt(self.config.max_effects_per_step),
                startup: Some(startup_source),
                capacity: Some(capacity),
                membership: None,
                owner_controls: Some(owner_controls),
                ordered_observations: Some(ordered_host),
                source: Box::new(
                    super::transport::AttemptTransportSource::new(
                        runtime.clone(),
                        self.original.clone(),
                        self.native_source,
                        self.scope.clone(),
                        admission,
                        [
                            Some(qa),
                            Some(process),
                            Some(publication),
                            Some(release),
                            Some(read_confirmation),
                            Some(subscription_control),
                        ],
                        Some(subscriptions),
                        send,
                        (
                            FailureContext {
                                lifecycle: self
                                    .failure_lifecycle
                                    .take()
                                    .expect("original failure lifecycle"),
                                publications: self.failure_publications.clone(),
                            },
                            super::transport::AttemptFailureBinding {
                                supervisor: failure,
                                port: Box::new(cleanup),
                                abort_drain: Some(abort_retirement),
                            },
                        ),
                        source,
                    )
                    .with_releases(native.0.clone(), retirement.host_waker())
                    .with_abort_retirement(&abort, retirement.host_waker())
                    .with_pumps(pumps, diagnostics, next_payload),
                ),
                executor: Arc::new(super::transport::AttemptTransportExecutor {
                    native,
                    other: executor.clone(),
                }),
                transport: Some(retirement),
            },
            startup,
            controls,
            membership,
            failure: failure_bridge,
            terminal,
        })
    }

    pub(crate) fn root_control_binding(&self) -> &RootControlBinding {
        &self.root_control
    }

    pub(crate) fn failure_publications(&self) -> &Arc<FailurePublications> {
        &self.failure_publications
    }

    pub(crate) fn bind_membership(
        &mut self,
        runtime: &ActorRuntime<FrontendActor>,
        work: &WorkObligation,
    ) -> Result<MembershipRegistration, AttemptRegistrationError> {
        if self.members.is_none() || !self.scope.same_scope(work.scope()) {
            return Err(AttemptRegistrationError::InvalidConfig(
                "attempt membership requires its unused original roster and Work",
            ));
        }
        let standing = self
            .reserve_standing(runtime, self.membership_source, InputClass::Ordinary, work)
            .map_err(AttemptRegistrationError::Runtime)?;
        Ok(MembershipTarget::register(
            self.original.clone(),
            self.members
                .take()
                .expect("the original roster was checked"),
            CoalescingInput::new(standing, membership_input),
        ))
    }

    /// Reserve one stable Ordinary notification role for each frozen
    /// participation on the same original observation Source.
    pub(crate) fn bind_read_progress(
        &mut self,
        runtime: &ActorRuntime<FrontendActor>,
        observation_work: &WorkObligation,
        backing: Option<&ObservationBackingPin>,
    ) -> Result<BTreeMap<ParticipationRef, ReadProgressInput>, ObservationError> {
        if self.read_progress_roles.is_some() || self.startup_reserved {
            return Err(ObservationError::Busy);
        }
        if !self.scope.same_scope(observation_work.scope()) {
            return Err(ObservationError::Runtime(
                ActorTurnError::InputAuthorMismatch,
            ));
        }
        let mut roles: BTreeMap<_, ReadProgressInput> = BTreeMap::new();
        for member in self.failure_members.iter() {
            let standing = match self.reserve_standing(
                runtime,
                self.subscription_source,
                InputClass::Ordinary,
                observation_work,
            ) {
                Ok(standing) => standing,
                Err(error) => {
                    for role in roles.values() {
                        role.close_after_exit().expect("unpublished progress role");
                    }
                    return Err(ObservationError::Runtime(error));
                }
            };
            roles.insert(
                member.participation,
                ReadProgressInput::new(&self.original, member.participation, standing)
                    .with_backing(backing.cloned()),
            );
        }
        self.read_progress_roles = Some(roles.values().cloned().collect());
        Ok(roles)
    }

    pub(crate) fn observation_resources(
        participants: usize,
    ) -> Result<ReservationResources, ObservationError> {
        let budget = ObservationBudget::for_task_attempt();
        let backing = ObservationBackingQuote::new(budget, participants)?;
        let quota = budget
            .byte_window()
            .and_then(|window| Ok(window.checked_add(backing.quota())?))
            .map_err(|error| ObservationError::Runtime(error.into()))?;
        Ok(ReservationResources::new(
            quota,
            OwnedSlotNeed {
                work_headers: 1,
                inputs: budget.total_positions,
                ..Default::default()
            },
        ))
    }

    /// Assemble the attempt's real credits, stream publishers, and canonical
    /// QA request owner before the shared writer is published.
    pub(crate) fn bind_observation_source(
        &mut self,
        runtime: &ActorRuntime<FrontendActor>,
        backing: ObservationSourceBacking<'_>,
        root_control_work: &WorkObligation,
        requests: &Arc<RootControlRequests>,
        clock: Arc<dyn MonotonicClock>,
        progress: Waker,
    ) -> Result<OrderedObservationInput<FrontendActor>, ObservationError> {
        if self.ordered_bound || self.read_progress_roles.is_some() {
            return Err(ObservationError::Busy);
        }
        if !requests.matches(&self.root_control) || self.scope.same_scope(root_control_work.scope())
        {
            return Err(ObservationError::InvalidRootControl);
        }
        let budget = ObservationBudget::for_task_attempt();
        let quote = ObservationBackingQuote::new(budget, self.failure_members.len())?;
        let metadata = PaidObservationMetadata::take_from(
            quote,
            backing.work,
            backing.positions,
            progress.clone(),
        )?;
        let roles = match self.bind_read_progress(runtime, backing.work, Some(&metadata.pin())) {
            Ok(roles) => roles,
            Err(error) => {
                metadata.retire_unpublished();
                return Err(error);
            }
        };
        let credits = match ObservationCredits::bind(
            budget,
            roles.keys().copied(),
            backing.work,
            backing.positions,
            Some(metadata.pin()),
        ) {
            Ok(credits) => credits,
            Err(error) => {
                self.read_progress_roles = None;
                for role in roles.values() {
                    role.close_after_exit()
                        .expect("unpublished read progress role");
                }
                drop(roles);
                metadata.retire_unpublished();
                return Err(error);
            }
        };
        let source = BoundObservationSource::new(
            self.root_control.clone(),
            requests.clone(),
            credits,
            roles,
            metadata.clone(),
        );
        match self.bind_observations(
            runtime,
            backing.work,
            root_control_work,
            source.clone(),
            clock,
            progress,
        ) {
            Ok(ingress) => {
                requests.bind_ingress(&ingress);
                Ok(ingress)
            }
            Err(error) => {
                self.read_progress_roles = None;
                source
                    .retire_unpublished()
                    .expect("unpublished observation Source");
                drop(source);
                metadata.retire_unpublished();
                Err(error)
            }
        }
    }

    /// One original writer owns both mailbox classes for this registration.
    /// A later Control input therefore cannot overtake its Lifecycle prefix.
    pub(crate) fn bind_observations(
        &mut self,
        runtime: &ActorRuntime<FrontendActor>,
        observation_work: &WorkObligation,
        root_control_work: &WorkObligation,
        pool: Arc<dyn ObservationSourcePool>,
        clock: Arc<dyn MonotonicClock>,
        progress: Waker,
    ) -> Result<OrderedObservationInput<FrontendActor>, ObservationError> {
        if self.ordered_bound {
            return Err(ObservationError::Busy);
        }
        if !self
            .root_control
            .same_original(&pool.fixed_root_control_binding())
            || !self.scope.same_scope(observation_work.scope())
            || self.scope.same_scope(root_control_work.scope())
        {
            return Err(ObservationError::InvalidRootControl);
        }
        let mut observations = self
            .reserve_standing(
                runtime,
                self.observation_source,
                InputClass::Lifecycle,
                observation_work,
            )
            .map_err(ObservationError::Runtime)?;
        let controls = match self.reserve_standing(
            runtime,
            self.observation_source,
            InputClass::Control,
            root_control_work,
        ) {
            Ok(controls) => controls,
            Err(error) => {
                observations
                    .close_after_exit()
                    .expect("unpublished observation position is unused");
                return Err(ObservationError::Runtime(error));
            }
        };
        let reply_author = controls
            .author()
            .expect("original admitted root-control position");
        let ingress = OrderedObservationInput::new(
            observations,
            controls,
            pool,
            self.root_control.clone(),
            clock,
            |input| FrontendInput::Attempt(Box::new(input)),
            progress,
        )
        .map_err(|(error, mut observations, mut controls)| {
            observations
                .close_after_exit()
                .expect("unpublished observation position is unused");
            controls
                .close_after_exit()
                .expect("unpublished root-control position is unused");
            error
        })?;
        self.root_control.bind_reply_author(reply_author);
        self.ordered_host = Some(OrderedObservationHost::new(ingress.clone()));
        self.ordered_bound = true;
        Ok(ingress)
    }

    #[cfg(test)]
    pub(crate) fn reserve_native_receipt(
        &self,
        runtime: &ActorRuntime<FrontendActor>,
        work: &WorkObligation,
        key: SendKey,
        kind: novarocks_execution_contract::OperationKind,
    ) -> Result<NativeReceiptInput, ActorTurnError> {
        if !self.scope.same_scope(work.scope()) {
            return Err(ActorTurnError::InputAuthorMismatch);
        }
        let position =
            self.reserve_standing(runtime, self.native_source, InputClass::Lifecycle, work)?;
        Ok(NativeReceiptInput::new(key, kind, position))
    }

    fn reserve_standing(
        &self,
        runtime: &ActorRuntime<FrontendActor>,
        source: ActorId,
        class: InputClass,
        work: &WorkObligation,
    ) -> Result<StandingInputObligation<FrontendActor>, ActorTurnError> {
        runtime
            .reserve_admitted_input_for(&self.original, source, class, work)?
            .into_standing_input()
            .map_err(|(error, original)| {
                original
                    .revoke_unused()
                    .expect("the unused input position can be revoked");
                error
            })
    }

    /// Attach already admitted roles from this exact registration before
    /// opening protocol ingress. The caller continues to own their custody.
    pub(crate) fn bind(
        &mut self,
        runtime: &ActorRuntime<FrontendActor>,
        authors: AttemptEffectAuthors,
        queue_parent: &ReservationObligation,
        capacity_work: &WorkObligation,
    ) -> Result<AttemptCapacitySource, AttemptRegistrationError> {
        use AttemptRegistrationError::{InvalidConfig, Runtime};
        if self.binding.get().is_some() {
            return Err(InvalidConfig(
                "attempt effect authors were already installed",
            ));
        }
        for author in [
            &authors.business,
            &authors.publication,
            &authors.reply,
            &authors.cleanup,
            &authors.settlement,
        ] {
            if !self.original.matches_input_author(author)
                || author.work().scope != self.original.scope_id()
                || author.generation() != self.original.initial_epoch()
            {
                return Err(InvalidConfig(
                    "attempt effect author belongs to a different registration",
                ));
            }
        }
        if !queue_parent.belongs_to(&self.scope) || queue_parent.is_released() {
            return Err(InvalidConfig(
                "attempt queue parent belongs to a different scope",
            ));
        }
        if !self.scope.same_scope(capacity_work.scope()) {
            return Err(InvalidConfig(
                "attempt capacity Work belongs to a different scope",
            ));
        }
        // Reserve the registrant id before taking the paid standing position.
        // No wait can be installed until the real waker below is bound.
        let registrant = Arc::new(
            self.scope
                .capacity_registrant(Waker::noop().clone())
                .map_err(|error| Runtime(error.into()))?,
        );
        let standing = self
            .reserve_standing(
                runtime,
                self.capacity_source,
                InputClass::Ordinary,
                capacity_work,
            )
            .map_err(Runtime)?;
        assert!(
            self.binding
                .set(AttemptBinding {
                    root_control: self.root_control.clone(),
                    authors,
                    generation: self.original.initial_epoch(),
                    queue_parent: queue_parent.id(),
                    capacity_registrant: registrant.clone(),
                })
                .is_ok(),
            "the registration binds its Sources once"
        );
        Ok(AttemptCapacitySource::new(
            self.original.clone(),
            standing,
            registrant,
        ))
    }

    pub(crate) fn bind_source_parent(
        &mut self,
        runtime: &ActorRuntime<FrontendActor>,
        work: &WorkObligation,
        payer: &ReservationObligation,
    ) -> Result<AttemptCapacitySource, AttemptRegistrationError> {
        // An unused position establishes CR's exact registration/work
        // provenance. The author keeps that provenance after the position
        // is revoked. Work lifetime remains owned by AttemptHostFunding.
        let original = runtime
            .reserve_admitted_input_for(
                &self.original,
                self.native_source,
                InputClass::Lifecycle,
                work,
            )
            .map_err(AttemptRegistrationError::Runtime)?;
        let author = original.author();
        original
            .revoke_unused()
            .map_err(AttemptRegistrationError::Runtime)?;
        let author = author.map_err(AttemptRegistrationError::Runtime)?;
        self.bind(
            runtime,
            AttemptEffectAuthors {
                business: author.clone(),
                publication: author.clone(),
                reply: author.clone(),
                cleanup: author.clone(),
                settlement: author,
            },
            payer,
            work,
        )
    }
}

impl AttemptActor {
    pub(crate) fn register(
        runtime: &ActorRuntime<FrontendActor>,
        start: AttemptStart,
        members: &Arc<[FrozenMember]>,
        scope: ScopeHandle,
        capacity: NativeTransportCapacity,
        config: ActorConfig,
        failure_work: &WorkObligation,
    ) -> Result<AttemptActorRegistration, AttemptRegistrationError> {
        config
            .validate()
            .map_err(AttemptRegistrationError::InvalidConfig)?;
        let minimum =
            start
                .minimum_core_step_units()
                .ok_or(AttemptRegistrationError::InvalidConfig(
                    "attempt step budget overflows",
                ))?;
        if config.mailbox != start.sources.mailbox
            || config.max_effects_per_step < OUTPUT_UNITS
            || config.max_step_work.saturating_sub(OUTPUT_UNITS) < minimum
        {
            return Err(AttemptRegistrationError::InvalidConfig(
                "attempt config cannot fit its original input and output",
            ));
        }
        let actor = runtime_source(start.sources.actor);
        let native = runtime_source(start.sources.native);
        let transport_source = runtime_source(start.sources.transport);
        let subscription_source = runtime_source(start.sources.subscription);
        let qa_source = runtime_source(start.sources.qa);
        let process_source = runtime_source(start.sources.process);
        let publication_source = runtime_source(start.sources.publication);
        let capacity_source = runtime_source(start.sources.capacity);
        let observation_source = runtime_source(start.sources.observation);
        let membership_source = runtime_source(start.sources.membership);
        if !members
            .iter()
            .map(|row| &row.participation)
            .eq(start.plan.establish_material.keys())
        {
            return Err(AttemptRegistrationError::InvalidConfig(
                "attempt membership roster differs from the frozen participations",
            ));
        }
        let activation = start.activation.identity;
        let root = start.plan.graph.root;
        if activation.execution() != root.query_execution_id() {
            return Err(AttemptRegistrationError::InvalidConfig(
                "attempt root differs from its activation",
            ));
        }
        let binding = Arc::new(OnceLock::new());
        let compatibility = start.plan.compatibility;
        let dynamic_pump = start
            .plan
            .pumps
            .iter()
            .find_map(|(id, kind)| (*kind == PumpKind::DynamicFilters).then_some(*id));
        let split_pump = start
            .plan
            .pumps
            .iter()
            .find_map(|(id, kind)| (*kind == PumpKind::SplitAssignment).then_some(*id));
        let retain_diagnostics = start.activation.policy.retain_diagnostics;
        let core = Core::new(start).map_err(AttemptRegistrationError::Core)?;
        let adapter = Self {
            actor,
            core,
            scope: scope.clone(),
            capacity: capacity.clone(),
            binding: binding.clone(),
        };
        let original = runtime
            .register_supervised_bound_with_source_cut(
                actor,
                scope.clone(),
                FrontendActor::Attempt(Box::new(adapter)),
                config,
                failure_work,
                native,
            )
            .map_err(|(error, _)| AttemptRegistrationError::Runtime(error))?;
        let root_control =
            RootControlBinding::for_registered_source(original.clone(), activation, root)
                .expect("the registered root belongs to the same activation");
        let failure_publications = Arc::new(FailurePublications::new(
            original.clone(),
            activation,
            &scope,
        ));
        Ok(AttemptActorRegistration {
            original,
            scope,
            transport_capacity: capacity,
            compatibility,
            native_source: native,
            transport_source,
            subscription_source,
            qa_source,
            process_source,
            publication_source,
            capacity_source,
            observation_source,
            membership_source,
            members: Some(members.clone()),
            failure_members: members.clone(),
            failure_lifecycle: Some(FailureLifecycle::freeze(
                members.iter().map(|member| member.participation),
            )),
            failure_publications,
            failure_work: failure_work.reference(),
            dynamic_pump,
            split_pump,
            retain_diagnostics,
            root_control,
            ordered_host: None,
            ordered_bound: false,
            read_progress_roles: None,
            startup_reserved: false,
            abort_bound: false,
            config,
            binding,
        })
    }

    pub(crate) fn handle(
        &mut self,
        now: Tick,
        input: InputEnvelope,
        value: FrontendInput,
        step: &mut ActorStep<'_, FrontendEffect>,
    ) {
        let claim = AttemptInputClaim {
            source: attempt_source(input.source),
            input: AttemptInputId(positive(input.input.get())),
            native_cut: step.input_source_cut().map(project_cut),
            selected_cut: step.selected_source_cut().map(project_cut),
        };
        let class = input.class;
        let input = match value {
            FrontendInput::Timer => AttemptInput::Timer,
            FrontendInput::AttemptInfrastructureFailed => {
                assert_eq!(input.source, self.actor);
                assert_eq!(class, InputClass::Lifecycle);
                std::panic::panic_any(AttemptFailure::OwnerFailed);
            }
            FrontendInput::AttemptStartup(StartupInput::Bootstrap) => AttemptInput::Bootstrap,
            FrontendInput::AttemptStartup(StartupInput::StartProtocol) => {
                AttemptInput::StartProtocol
            }
            FrontendInput::CapacityReleased => AttemptInput::CapacityReleased,
            FrontendInput::Membership(view) => AttemptInput::Membership(view.0),
            FrontendInput::Attempt(input) => *input,
            #[cfg(test)]
            FrontendInput::Test(_) => panic!("test input entered the production attempt actor"),
        };
        assert_eq!(
            class,
            input_class(&input),
            "attempt input used the wrong paid mailbox class"
        );
        assert!(
            !matches!(input, AttemptInput::StartProtocol) || self.binding.get().is_some(),
            "attempt Sources are bound before starting protocol"
        );
        let mut context = TurnContext {
            step,
            scope: &self.scope,
            capacity: &self.capacity,
            binding: self.binding.get(),
            queue_work: None,
        };
        let mut result = self
            .core
            .handle(
                MonotonicInstant::from_origin(Duration::from_nanos(now.0)),
                claim,
                input,
                &mut context,
            )
            .unwrap_or_else(|error| {
                std::panic::panic_any(AttemptFailure::TaskProtocol(Box::new(error)))
            });
        if result.stop_producing {
            context.step.stop_producing();
        }
        match result.selection {
            InputSelection::Only(source) => context
                .step
                .select_input_source(Some(runtime_source(source))),
            InputSelection::Prefer(source) => {
                context.step.prefer_input_source(runtime_source(source))
            }
        }

        // A late receipt can produce both a settlement and a cleanup. Commit
        // them in the exact original action order, independent of field order.
        if let (Some(primary), Some(settlement)) = (&result.effect, &result.abort_settlement)
            && settlement.id.get() < primary.id.get()
        {
            std::mem::swap(&mut result.effect, &mut result.abort_settlement);
        }
        for effect in [result.effect, result.abort_settlement]
            .into_iter()
            .flatten()
        {
            context.emit(effect);
        }
        assert!(
            context.queue_work.is_none(),
            "an original queue guard must accompany its SendBatch"
        );
        if let Some(action) = result.notice_action {
            let notice = AttemptNotice {
                definitely_unsent_abort: result.definitely_unsent_abort,
                retired_authorization: result.retired_authorization,
                diagnostic_failure: result.diagnostic_failure,
                round_failure: result.round_result.and_then(Result::err),
            };
            let metadata = if let Some(processed) = result.root_control_processed {
                assert!(
                    notice.definitely_unsent_abort.is_none()
                        && notice.retired_authorization.is_none()
                        && notice.diagnostic_failure.is_none()
                        && notice.round_failure.is_none(),
                    "the root-control turn owns only its original processing notice"
                );
                FrontendEffect::AttemptRootControlProcessed(
                    InputId::new(processed.0.get()).expect("original input id"),
                )
            } else {
                FrontendEffect::AttemptNotice(Arc::new(notice))
            };
            let binding = context
                .binding
                .expect("attempt notice uses its original Source");
            let author = &binding.authors.settlement;
            context
                .step
                .effect_from_author(
                    author,
                    descriptor(action.get(), author.work(), EffectKind::Settlement),
                    metadata,
                )
                .unwrap();
        }
    }

    pub(crate) fn next_deadline(&self) -> Option<Tick> {
        self.core
            .next_deadline()
            .map(|at| Tick(u64::try_from(at.since_origin().as_nanos()).unwrap_or(u64::MAX)))
    }
}

pub(crate) fn input_class(input: &AttemptInput) -> InputClass {
    match input {
        AttemptInput::Timer => InputClass::Timer,
        AttemptInput::CapacityReleased
        | AttemptInput::Membership(_)
        | AttemptInput::Executor(
            ExecutorFact::ReadProgress(_)
            | ExecutorFact::PumpTaskUpdate { .. }
            | ExecutorFact::SplitProfile { .. },
        ) => InputClass::Ordinary,
        AttemptInput::Port(port)
            if matches!(
                &port.body,
                LogicalToAttemptBody::Abort { .. }
                    | LogicalToAttemptBody::Stop { .. }
                    | LogicalToAttemptBody::SealRequest { .. }
                    | LogicalToAttemptBody::TerminalControlDemand { .. }
                    | LogicalToAttemptBody::BeginConvergence { .. }
            ) =>
        {
            InputClass::Control
        }
        AttemptInput::Bootstrap
        | AttemptInput::StartProtocol
        | AttemptInput::Port(_)
        | AttemptInput::Executor(_) => InputClass::Lifecycle,
    }
}

struct TurnContext<'step, 'runtime> {
    step: &'step mut ActorStep<'runtime, FrontendEffect>,
    scope: &'step ScopeHandle,
    capacity: &'step NativeTransportCapacity,
    binding: Option<&'step AttemptBinding>,
    queue_work: Option<(ReservationToken, ScopedWorkRef)>,
}

impl StepContext for TurnContext<'_, '_> {
    fn charge(&mut self, units: usize) -> bool {
        units <= self.step.remaining().saturating_sub(OUTPUT_UNITS) && self.step.charge(units)
    }

    fn reserve_queue_stage(
        &mut self,
        _: DispatchLane,
        control_progress: bool,
        target: BackendProcessId,
        records: usize,
        queued_bytes: usize,
    ) -> QueueStage {
        let binding = self
            .binding
            .expect("attempt queue author is installed before protocol start");
        let guard = match binding.reserve_queue(
            self.scope,
            self.capacity,
            control_progress,
            target,
            records,
            queued_bytes,
        ) {
            Ok(guard) => guard,
            Err(stage) => return stage,
        };
        let token = ReservationToken(positive(guard.reservation_id().get()));
        let work = ScopedWorkRef {
            scope: guard.scope().id(),
            work: guard.work_id(),
        };
        assert!(
            self.queue_work.replace((token, work)).is_none(),
            "one Core queue guard per step"
        );
        assert!(
            self.step.reservation(guard).is_ok(),
            "original queue guard fits the same actor step"
        );
        QueueStage::Granted(token)
    }
}

impl AttemptBinding {
    fn reserve_queue(
        &self,
        scope: &ScopeHandle,
        capacity: &NativeTransportCapacity,
        control_progress: bool,
        target: BackendProcessId,
        records: usize,
        queued_bytes: usize,
    ) -> Result<StepRegistration, QueueStage> {
        let (lane, kind) = if control_progress {
            (TransportCapacityLane::Control, WorkKind::Settlement)
        } else {
            (TransportCapacityLane::Ordinary, WorkKind::Business)
        };
        let resources =
            super::transport::send_queue_resources(capacity, target, lane, records, queued_bytes)
                .expect("Core queues only validated Native resources");
        match scope.reserve_child_next_or_wait(
            self.generation,
            self.queue_parent,
            resources,
            kind,
            &self.capacity_registrant,
        ) {
            Ok(guard) => Ok(guard),
            Err(RuntimeError::CapacityDenied(denial)) => Err(
                match NativeTransportCapacity::classify_denial(denial)
                    .expect("queue denial belongs to the original transport dimensions")
                {
                    TransportCapacityRefusal::ProcessFull => QueueStage::ProcessFull,
                    TransportCapacityRefusal::TargetFull(backend) => {
                        QueueStage::TargetFull(backend)
                    }
                },
            ),
            Err(error) => panic!("original attempt queue reservation failed: {error}"),
        }
    }
}

impl TurnContext<'_, '_> {
    fn emit(&mut self, effect: AttemptEffect) {
        let (kind, author) = {
            let binding = self
                .binding
                .expect("attempt effect uses its original Source");
            let authors = &binding.authors;
            match effect.class {
                EffectClass::Business => (EffectKind::Business, &authors.business),
                EffectClass::Publication => (EffectKind::Publication, &authors.publication),
                EffectClass::Reply => {
                    let author = match &effect.body {
                        EffectBody::Port(port) if matches!(port.body,
                            novarocks_coordination_contract::AttemptToLogicalBody::SealResult { .. }
                            | novarocks_coordination_contract::AttemptToLogicalBody::TerminalControlResult { .. }
                        ) => binding.root_control.reply_author().expect("original root-control reply author"),
                        _ => &authors.reply,
                    };
                    (EffectKind::Reply, author)
                }
                EffectClass::Cleanup => (EffectKind::Cleanup, &authors.cleanup),
                EffectClass::Settlement => (EffectKind::Settlement, &authors.settlement),
            }
        };
        let id = effect.id.get();
        if let EffectBody::SendBatch { reservation, .. } = &effect.body {
            let (token, work) = self
                .queue_work
                .take()
                .expect("SendBatch has its original queue guard");
            assert_eq!(token, *reservation);
            self.step
                .effect(
                    descriptor(id, work, kind),
                    FrontendEffect::Attempt(
                        crate::control_runtime::transport_withdrawal::AttemptEffectEnvelope::new(
                            effect,
                        ),
                    ),
                )
                .unwrap();
        } else {
            self.step
                .effect_from_author(
                    author,
                    descriptor(id, author.work(), kind),
                    FrontendEffect::Attempt(
                        crate::control_runtime::transport_withdrawal::AttemptEffectEnvelope::new(
                            effect,
                        ),
                    ),
                )
                .unwrap();
        }
    }
}

pub(crate) fn descriptor(id: u64, work: ScopedWorkRef, kind: EffectKind) -> EffectDescriptor {
    EffectDescriptor {
        id: ActionId::new(id).expect("original nonzero action"),
        work,
        kind,
        dependencies: Vec::new(),
    }
}

fn positive(value: u64) -> NonZeroU64 {
    NonZeroU64::new(value).expect("original nonzero runtime identity")
}
fn attempt_source(source: ActorId) -> AttemptSourceId {
    AttemptSourceId(positive(source.get()))
}
fn runtime_source(source: AttemptSourceId) -> ActorId {
    ActorId::new(source.0.get()).expect("original nonzero attempt source")
}
fn project_cut(cut: InputSourceCut) -> AttemptSourceCut {
    AttemptSourceCut {
        source: attempt_source(cut.source),
        tail: cut.tail.map(|id| AttemptInputId(positive(id.get()))),
    }
}

#[cfg(test)]
pub(super) mod tests;
