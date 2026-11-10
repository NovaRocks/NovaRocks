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
    ObservationError, ObservationSourcePool, OrderedObservationInput, RootControlBinding,
};
use super::transport::{NativeProgressInput, NativeReceiptInput, SendKey};
use crate::control_runtime::transport_withdrawal::AttemptEffectEnvelope;
use crate::{
    control_runtime::{
        actor::{FrontendActor, FrontendActorKind, FrontendEffect, FrontendInput},
        host::{EffectExecutor, EffectSource, FrontendLanePolicy, HostBinding},
        membership::{FrozenMember, MembershipTarget, membership_input},
    },
    native::transport_capacity::{
        NativeTransportCapacity, TransportCapacityLane, TransportCapacityRefusal,
    },
    task_execution::host::custody::TransportRetirements,
};
use novarocks_attempt_protocol::{
    AttemptCore, Core, InputSelection,
    capacity::{QueueStage, ReservationToken, StepContext},
    config::DispatchLane,
    effect::{AttemptEffect, EffectBody, EffectClass},
    failure::AttemptFailure,
    input::{
        AttemptInput, AttemptInputClaim, AttemptInputId, AttemptSourceCut, AttemptSourceId,
        ExecutorFact,
    },
    start::AttemptStart,
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
    transaction::{RuntimeError, ScopeHandle},
};
use novarocks_control_runtime_core::{
    effect::{EffectDescriptor, EffectKind},
    identity::{ActionId, ActorId, Generation, InputId, ReservationId, ScopedWorkRef, Tick},
    mailbox::{InputClass, InputEnvelope},
    scope::WorkKind,
};
use novarocks_coordination_contract::{LogicalToAttemptBody, MonotonicClock, MonotonicInstant};
use novarocks_execution_contract::TaskOperationId;
use novarocks_types::BackendProcessId;
use std::{
    num::NonZeroU64,
    sync::{Arc, OnceLock},
    task::Waker,
    time::Duration,
};

// The Core returns at most one primary effect, one Abort settlement and one
// passive report. ActorStep charges their actual handoff separately.
const OUTPUT_UNITS: usize = 3;

pub(crate) struct AttemptActor {
    core: Core,
    scope: ScopeHandle,
    capacity: NativeTransportCapacity,
    binding: Arc<OnceLock<AttemptBinding>>,
}

pub(crate) struct AttemptActorRegistration {
    pub(crate) original: ActorRegistration,
    scope: ScopeHandle,
    native_source: ActorId,
    qa_source: ActorId,
    process_source: ActorId,
    capacity_source: ActorId,
    observation_source: ActorId,
    membership_source: ActorId,
    members: Option<Arc<[FrozenMember]>>,
    failure_members: Arc<[FrozenMember]>,
    failure_lifecycle: Option<FailureLifecycle>,
    failure_publications: Arc<FailurePublications>,
    failure_work: ScopedWorkRef,
    root_control: RootControlBinding,
    ordered_bound: bool,
    startup_reserved: bool,
    config: ActorConfig,
    binding: Arc<OnceLock<AttemptBinding>>,
}

pub(crate) struct AttemptEffectAuthors {
    pub(crate) business: InputAuthor,
    pub(crate) publication: InputAuthor,
    pub(crate) reply: InputAuthor,
    pub(crate) cleanup: InputAuthor,
    pub(crate) settlement: InputAuthor,
}

struct AttemptBinding {
    authors: AttemptEffectAuthors,
    generation: Generation,
    queue_parent: ReservationId,
    capacity_registrant: Arc<CapacityRegistrant>,
}

#[must_use]
pub(crate) struct PreparedAttemptHost {
    pub(crate) binding: HostBinding,
    pub(crate) startup: AttemptStartup,
    pub(crate) failure: PendingFailureBridge,
}
pub(crate) struct AttemptTransportBinding {
    pub(crate) retirement: TransportRetirements,
    pub(crate) admission: super::transport::AdmissionSource,
    pub(crate) qa: super::transport::AttemptCallSource,
    pub(crate) process: super::transport::AttemptCallSource,
    pub(crate) send: super::transport::AttemptSendSource,
    pub(crate) executor: super::transport::ActorTransportExecutor,
    pub(crate) failure: FailureSupervisor,
}

/// One paid original Lifecycle position, reused only after actual input
/// retirement. The application opens external ingress after StartProtocol.
#[must_use]
pub(crate) struct AttemptStartup {
    registration: ActorRegistration,
    input: StandingInputObligation<FrontendActor>,
    next: Option<AttemptInput>,
}
impl AttemptStartup {
    /// `Ok(None)` proves that the StartProtocol input has actually retired.
    pub(crate) fn advance(&mut self) -> Result<Option<InputId>, ActorTurnError> {
        if self.input.in_flight()?.is_some() {
            return Err(ActorTurnError::StandingInputBusy);
        }
        let Some(next) = self.next.take() else {
            return Ok(None);
        };
        let after = matches!(next, AttemptInput::Bootstrap).then_some(AttemptInput::StartProtocol);
        match self.input.fill(FrontendInput::Attempt(Box::new(next))) {
            Ok(input) => {
                self.next = after;
                Ok(Some(input))
            }
            Err((error, FrontendInput::Attempt(input))) => {
                self.next = Some(*input);
                Err(error)
            }
            Err(_) => unreachable!("failed startup returns its original input"),
        }
    }

    pub(crate) fn close_after_exit(&mut self) -> Result<(), ActorTurnError> {
        self.input.close_after_exit()
    }

    pub(crate) fn ready_for(
        &self,
        registration: &ActorRegistration,
    ) -> Result<bool, ActorTurnError> {
        Ok(self.registration.same_registration(registration)
            && self.next.is_none()
            && self.input.in_flight()?.is_none())
    }
}

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
    runtime: ActorRuntime<FrontendActor>,
    input: CoalescingInput<FrontendActor, CapacityReleased>,
    registrant: Arc<CapacityRegistrant>,
}

impl AttemptCapacitySource {
    /// Call after the host has indexed the original committed input. An
    /// occurrence with outstanding work stays in the actual mailbox role.
    pub(crate) fn retire_ready(&mut self) -> Result<bool, ActorTurnError> {
        let Some(input) = self.input.in_flight()? else {
            return Ok(false);
        };
        let actor = self.registration.actor();
        match self.runtime.input_retirement_status(actor, input) {
            Ok(()) => {}
            Err(
                ActorTurnError::InputStillQueued
                | ActorTurnError::EffectsRemain
                | ActorTurnError::ReservationsRemain
                | ActorTurnError::MetadataStillPending,
            ) => return Ok(false),
            Err(error) => return Err(error),
        }
        self.runtime.retire_input(actor, input)?;
        match self.input.after_retire() {
            Ok(_) | Err(ActorTurnError::Closed | ActorTurnError::ActorFailed) => Ok(true),
            Err(error) => Err(error),
        }
    }

    pub(crate) fn close(&self) {
        self.input.close();
        self.registrant.cancel();
    }

    pub(crate) fn close_after_exit(&self) -> Result<(), ActorTurnError> {
        self.close();
        self.input.close_after_exit()
    }
}

#[derive(Clone, Debug)]
pub(crate) struct AttemptNotice {
    pub(crate) root_control_processed: Option<AttemptInputId>,
    pub(crate) definitely_unsent_abort: Option<TaskOperationId>,
    pub(crate) retired_authorization: Option<TaskOperationId>,
    pub(crate) diagnostic_failure: Option<AttemptFailure>,
    pub(crate) round_failure: Option<AttemptFailure>,
}

#[derive(Debug)]
pub(crate) enum AttemptRegistrationError {
    InvalidConfig(&'static str),
    Core(AttemptFailure),
    Runtime(ActorTurnError),
}

impl AttemptActorRegistration {
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
        use std::collections::VecDeque;
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
        let mut positions = counts.map(VecDeque::with_capacity);
        for (lane, count) in counts.into_iter().enumerate() {
            for _ in 0..count {
                match CausalInputObligation::reserve_admitted(
                    runtime,
                    &self.original,
                    self.native_source,
                    InputClass::Lifecycle,
                    parent,
                ) {
                    Ok(position) => positions[lane].push_back(position),
                    Err(error) => {
                        for mut original in positions
                            .iter_mut()
                            .flat_map(|positions| positions.drain(..))
                        {
                            original
                                .close_after_exit()
                                .expect("unpublished admission position");
                        }
                        return Err(AttemptRegistrationError::Runtime(error));
                    }
                }
            }
        }
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

    pub(crate) fn prepare_host(
        &mut self,
        runtime: &ActorRuntime<FrontendActor>,
        startup_work: &WorkObligation,
        source: Box<dyn EffectSource>,
        executor: &Arc<dyn EffectExecutor>,
        transport: AttemptTransportBinding,
    ) -> Result<
        PreparedAttemptHost,
        (
            AttemptRegistrationError,
            Box<dyn EffectSource>,
            AttemptTransportBinding,
        ),
    > {
        if self.startup_reserved
            || !self.ordered_bound
            || self.binding.get().is_none()
            || !self.scope.same_scope(startup_work.scope())
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
                source,
                transport,
            ));
        }
        let input = match self.reserve_standing(
            runtime,
            self.original.actor(),
            InputClass::Lifecycle,
            startup_work,
        ) {
            Ok(input) => input,
            Err(error) => {
                return Err((AttemptRegistrationError::Runtime(error), source, transport));
            }
        };
        self.startup_reserved = true;
        let AttemptTransportBinding {
            retirement,
            admission,
            qa,
            process,
            send,
            executor: native,
            failure,
        } = transport;
        let (failure_bridge, residual) = PendingFailureBridge::new(
            self.original.clone(),
            self.root_control.activation(),
            self.failure_members.clone(),
            self.failure_publications.clone(),
        );
        let cleanup = super::transport::NativeFailureCleanup::new(
            send.contexts.clone(),
            failure.reservation_id(),
            native.0.clone(),
            residual,
        );
        Ok(PreparedAttemptHost {
            binding: HostBinding {
                scope: self.scope.clone(),
                registration: self.original.clone(),
                kind: FrontendActorKind::Attempt,
                config: self.config,
                policy: FrontendLanePolicy::attempt(self.config.max_effects_per_step),
                source: Box::new(super::transport::AttemptTransportSource::new(
                    runtime.clone(),
                    self.original.clone(),
                    self.native_source,
                    self.scope.clone(),
                    admission,
                    [Some(qa), Some(process)],
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
                        },
                    ),
                    source,
                )),
                executor: Arc::new(super::transport::AttemptTransportExecutor {
                    native,
                    other: executor.clone(),
                }),
                transport: Some(retirement),
            },
            startup: AttemptStartup {
                registration: self.original.clone(),
                input,
                next: Some(AttemptInput::Bootstrap),
            },
            failure: failure_bridge,
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
    ) -> Result<Arc<MembershipTarget>, AttemptRegistrationError> {
        if self.members.is_none() || !self.scope.same_scope(work.scope()) {
            return Err(AttemptRegistrationError::InvalidConfig(
                "attempt membership requires its unused original roster and Work",
            ));
        }
        let standing = self
            .reserve_standing(runtime, self.membership_source, InputClass::Ordinary, work)
            .map_err(AttemptRegistrationError::Runtime)?;
        Ok(Arc::new(MembershipTarget::new(
            self.original.clone(),
            self.members
                .take()
                .expect("the original roster was checked"),
            CoalescingInput::new(standing, membership_input),
        )))
    }

    /// One original writer owns both mailbox classes for this registration.
    /// A later Control input therefore cannot overtake its Lifecycle prefix.
    pub(crate) fn bind_observations(
        &mut self,
        runtime: &ActorRuntime<FrontendActor>,
        observation_work: &WorkObligation,
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
            observation_work,
        ) {
            Ok(controls) => controls,
            Err(error) => {
                observations
                    .close_after_exit()
                    .expect("unpublished observation position is unused");
                return Err(ObservationError::Runtime(error));
            }
        };
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
        self.ordered_bound = true;
        Ok(ingress)
    }

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

    pub(crate) fn reserve_native_progress(
        &self,
        runtime: &ActorRuntime<FrontendActor>,
        work: &WorkObligation,
    ) -> Result<NativeProgressInput, ActorTurnError> {
        if !self.scope.same_scope(work.scope()) {
            return Err(ActorTurnError::InputAuthorMismatch);
        }
        self.reserve_standing(runtime, self.native_source, InputClass::Lifecycle, work)
            .map(NativeProgressInput::new)
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
        let input = CoalescingInput::new(standing, |_| FrontendInput::CapacityReleased);
        registrant.update_waker(&input.waker());
        assert!(
            self.binding
                .set(AttemptBinding {
                    authors,
                    generation: self.original.initial_epoch(),
                    queue_parent: queue_parent.id(),
                    capacity_registrant: registrant.clone(),
                })
                .is_ok(),
            "the registration binds its Sources once"
        );
        Ok(AttemptCapacitySource {
            registration: self.original.clone(),
            runtime: runtime.clone(),
            input,
            registrant,
        })
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
        let qa_source = runtime_source(start.sources.qa);
        let process_source = runtime_source(start.sources.process);
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
        let core = Core::new(start).map_err(AttemptRegistrationError::Core)?;
        let adapter = Self {
            core,
            scope: scope.clone(),
            capacity,
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
            native_source: native,
            qa_source,
            process_source,
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
            root_control,
            ordered_bound: false,
            startup_reserved: false,
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
                root_control_processed: result.root_control_processed,
                definitely_unsent_abort: result.definitely_unsent_abort,
                retired_authorization: result.retired_authorization,
                diagnostic_failure: result.diagnostic_failure,
                round_failure: result.round_result.and_then(Result::err),
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
                    FrontendEffect::AttemptNotice(Arc::new(notice)),
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
        | AttemptInput::Executor(ExecutorFact::ReadProgress(_)) => InputClass::Ordinary,
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
            let authors = &self
                .binding
                .expect("attempt effect uses its original Source")
                .authors;
            match effect.class {
                EffectClass::Business => (EffectKind::Business, &authors.business),
                EffectClass::Publication => (EffectKind::Publication, &authors.publication),
                EffectClass::Reply => (EffectKind::Reply, &authors.reply),
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
mod tests;
