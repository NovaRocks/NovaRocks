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

//! The host binds a committed SendBatch to its original queue-stage guard.

use super::{
    ActorSendReports, ActorTransportWork, AdmissionSource, BoundSend, FrozenSendRequests,
    PreparedSendBatch, QueueWindow, SendBindingError, SourceLayouts, actor_transport_layouts,
    original_run, prepare_actor_transport,
};
use crate::{
    control_runtime::{
        actor::{FrontendActor, FrontendEffect},
        host::PreparedEffectJob,
        transport_withdrawal::AttemptEffectEnvelope,
    },
    native::{
        apply_send_owner::{ApplySendPort, shared_apply_layouts},
        task_transport::wire::AttemptWireFacts,
        transport::Client,
        transport_capacity::{
            NativeTransportCapacity, TransportCapacityError, TransportCapacityLane,
        },
    },
    task_execution::host::custody::storage_layouts,
};
use novarocks_attempt_protocol::effect::EffectBody;
use novarocks_control_runtime::{
    actor_turn::{ActorRegistration, ActorRuntime, ActorTurnError, EffectTicket},
    causal_input::CausalInputObligation,
    completion::ReportSlot,
    reported_work::ReportedWork,
    transaction::{RuntimeError, RuntimeLedger, ScopeHandle},
};
use novarocks_control_runtime_core::{
    capacity::{CapacityError, ReservationResources, ResourceVector},
    identity::{ConsumerId, ReservationId},
    owned_storage::OwnedSlotNeed,
    scope::{ScopeError, WorkKind},
};
use novarocks_coordination_contract::MonotonicClock;
use novarocks_native_adapter::native_codec::NativeUnaryEncodingPlan;
use novarocks_task_codec::TransportBudget;
use novarocks_types::BackendProcessId;
use novarocks_workload_control::{
    WorkError,
    joint_admission::{NativeAsyncActionAuthority, NativeAsyncActionQuote},
};
use std::{alloc::Layout, collections::VecDeque, sync::Arc};

/// Native, Physical, Action and Reply each dispose under an actual role.
const APPLY_DISPOSAL_ROLES: usize = 4;
const ROLE_STORAGE: OwnedSlotNeed = OwnedSlotNeed {
    work_headers: 1,
    completions: 1,
    captures: 1,
    ..OwnedSlotNeed::work_only()
};

/// The Core pays the exact transport quotas and the bound report/capture
/// positions together, before emitting the passive reservation address.
pub(crate) fn send_queue_resources(
    layout: &NativeTransportCapacity,
    backend: BackendProcessId,
    lane: TransportCapacityLane,
    items: usize,
    bytes: usize,
) -> Result<ReservationResources, TransportCapacityError> {
    let mut resources = layout.queue_resources(backend, lane, items, bytes)?;
    resources.owned_slots = ROLE_STORAGE;
    Ok(resources)
}

#[derive(Clone)]
pub(crate) struct SendContext {
    pub(crate) registration: ActorRegistration,
    pub(crate) ledger: RuntimeLedger,
    pub(crate) scope: ScopeHandle,
    pub(crate) layout: NativeTransportCapacity,
    pub(crate) transport: TransportBudget,
    pub(crate) authority: NativeAsyncActionAuthority,
    pub(crate) wire: AttemptWireFacts,
    pub(crate) client: Client,
    pub(crate) apply: ApplySendPort,
    pub(crate) clock: Arc<dyn MonotonicClock>,
    pub(crate) observer: ConsumerId,
}

#[derive(Debug)]
pub(crate) enum SendPreflightError {
    Batch(SendBindingError),
    WrongClient,
    WrongBudget,
    WrongOwner,
    Reports,
    Quote(WorkError),
}

#[must_use = "an unclaimed send still owns the original result and reports"]
pub(crate) struct SendClaimRefusal {
    pub(crate) error: ActorTurnError,
    pub(crate) prepared: PreparedActorSend,
    pub(crate) result: CausalInputObligation<FrontendActor>,
}

/// Ready to claim the same Core guard. No grants, queue partition, report
/// leg or start authority has been spent by the preflight.
pub(crate) struct PreparedActorSend {
    effect: Arc<AttemptEffectEnvelope>,
    batch: PreparedSendBatch,
    context: SendContext,
    quote: NativeAsyncActionQuote,
    encoding: NativeUnaryEncodingPlan,
    reports: Arc<ActorSendReports>,
}

impl PreparedActorSend {
    pub(crate) fn revoke_unused_reports(self) {
        super::send_reports::close_unlaunched(self.reports);
    }
    pub(crate) fn prepare(
        effect: Arc<AttemptEffectEnvelope>,
        requests: &FrozenSendRequests,
        admission: &AdmissionSource,
        context: SendContext,
        reports: Arc<ActorSendReports>,
    ) -> Result<Self, (SendPreflightError, SendContext, Arc<ActorSendReports>)> {
        let prepared = (|| {
            let batch = PreparedSendBatch::resolve(effect.clone(), requests)
                .map_err(SendPreflightError::Batch)?;
            batch
                .check_grants(admission.grants())
                .map_err(SendPreflightError::Batch)?;
            let EffectBody::SendBatch {
                batch: protocol, ..
            } = &effect.protocol.body
            else {
                unreachable!("validated original SendBatch");
            };
            if context.client.backend_process_id() != protocol.backend {
                return Err(SendPreflightError::WrongClient);
            }
            if !context.layout.accepts_transport_budget(context.transport) {
                return Err(SendPreflightError::WrongBudget);
            }
            if !context.scope.belongs_to_ledger(&context.ledger)
                || !context.registration.matches_scope(&context.scope)
                || !admission.matches_scope(&context.scope)
            {
                return Err(SendPreflightError::WrongOwner);
            }
            if !reports.matches(effect.protocol.id, batch.keys(), batch.operations())
                || !reports.matches_receiver(&context.registration)
            {
                return Err(SendPreflightError::Reports);
            }
            let (quote, encoding) =
                quote(batch.operations(), context.transport).map_err(SendPreflightError::Quote)?;
            Ok((batch, quote, encoding))
        })();
        match prepared {
            Ok((batch, quote, encoding)) => Ok(Self {
                effect,
                batch,
                context,
                quote,
                encoding,
                reports,
            }),
            Err(error) => Err((error, context, reports)),
        }
    }

    pub(crate) fn bind(
        self,
        runtime: &ActorRuntime<FrontendActor>,
        ticket: &EffectTicket<FrontendEffect>,
        result: CausalInputObligation<FrontendActor>,
        admission: &mut AdmissionSource,
    ) -> Result<PreparedEffectJob, SendClaimRefusal> {
        self.bind_tracked(runtime, ticket, result, admission)
            .map(|(job, _)| job)
    }

    pub(super) fn bind_tracked(
        self,
        runtime: &ActorRuntime<FrontendActor>,
        ticket: &EffectTicket<FrontendEffect>,
        result: CausalInputObligation<FrontendActor>,
        admission: &mut AdmissionSource,
    ) -> Result<
        (
            PreparedEffectJob,
            novarocks_control_runtime::obligation::WorkRetirementObserver,
        ),
        SendClaimRefusal,
    > {
        let EffectBody::SendBatch { batch, reservation } = &self.effect.protocol.body else {
            unreachable!("validated original SendBatch");
        };
        let checked = (|| {
            runtime.validate_registration(&self.context.registration)?;
            if !matches!(&ticket.effect, FrontendEffect::Attempt(effect) if Arc::ptr_eq(effect, &self.effect))
                || ticket.actor != self.context.registration.actor()
                || !runtime.owns_scope(ticket.actor, &self.context.scope)?
                || !admission.matches_scope(&self.context.scope)
                || !self
                    .context
                    .registration
                    .matches_input_author(&result.event_author()?)
            {
                return Err(ActorTurnError::ReservationOwnerMismatch);
            }
            self.batch
                .check_grants(admission.grants())
                .map_err(|_| ActorTurnError::InputNotReserved)?;
            runtime.take_step_reservation(ticket, ReservationId::new(reservation.0.get()).unwrap())
        })();
        let guard = match checked {
            Ok(guard) => guard,
            Err(error) => {
                return Err(SendClaimRefusal {
                    error,
                    prepared: self,
                    result,
                });
            }
        };
        let (lane, kind) = if batch.control_progress {
            (TransportCapacityLane::Control, WorkKind::Settlement)
        } else {
            (TransportCapacityLane::Ordinary, WorkKind::Business)
        };
        let (work, mut capacity) = guard.bind(kind).expect("original queue guard kind");
        let header = work.retirement_observer();
        let window = QueueWindow::split(
            &mut capacity,
            self.context.layout.clone(),
            batch.backend,
            lane,
            batch.operations.len(),
            batch.queued_bytes,
        )
        .expect("original committed queue geometry");
        let report =
            ReportSlot::reserve_exit_observer_for_admitted(&mut capacity, self.context.observer)
                .expect("original guard's paid exit report");
        let mut cleanup = VecDeque::with_capacity(APPLY_DISPOSAL_ROLES);
        for _ in 0..APPLY_DISPOSAL_ROLES {
            cleanup.push_back(cleanup_role(
                &self.context.scope,
                capacity.id(),
                self.context.observer,
            ));
        }
        let original = ReportedWork::try_new(work, capacity, report)
            .expect("same original queue Work and exit report");
        let backend = batch.backend;
        let control_progress = batch.control_progress;
        let context = self.context;
        let job = prepare_actor_transport(
            self.effect,
            result,
            original,
            cleanup,
            context.observer,
            |registration| {
                let (queue, resolved) = admission
                    .bind_send(self.batch, window, registration)
                    .unwrap_or_else(|refused| std::panic::resume_unwind(Box::new(refused)));
                ActorTransportWork::Send(Box::new(BoundSend {
                    backend,
                    control_progress,
                    scope_generation: ticket.generation,
                    ledger: context.ledger,
                    scope: context.scope,
                    layout: context.layout,
                    transport: context.transport,
                    queue,
                    resolved,
                    authority: context.authority,
                    original_quote: self.quote,
                    encoding: self.encoding,
                    wire: context.wire,
                    client: context.client,
                    apply: context.apply,
                    reports: self.reports,
                    clock: context.clock,
                    original_captures: Box::new(()),
                }))
            },
        )
        .unwrap_or_else(|refused| std::panic::resume_unwind(Box::new(refused)));
        Ok((job, header))
    }
}

fn cleanup_role(scope: &ScopeHandle, parent: ReservationId, observer: ConsumerId) -> ReportedWork {
    let reserve = || {
        let epoch = scope.gates()?.generation;
        scope.reserve_child_next(
            epoch,
            parent,
            ReservationResources::new(ResourceVector::default(), ROLE_STORAGE),
            WorkKind::Cleanup,
        )
    };
    // A real Scope failure can advance the epoch once while binding. An
    // original cleanup child may still be derived under that failed epoch.
    let guard = match reserve() {
        Err(
            RuntimeError::Scope(ScopeError::GenerationMismatch)
            | RuntimeError::Capacity(CapacityError::Scope(ScopeError::GenerationMismatch)),
        ) => reserve(),
        original => original,
    }
    .expect("original send cleanup role");
    let (work, mut capacity) = guard
        .bind(WorkKind::Cleanup)
        .expect("original cleanup guard");
    let report = ReportSlot::reserve_exit_observer_for_admitted(&mut capacity, observer)
        .expect("paid original disposal report");
    ReportedWork::try_new(work, capacity, report).expect("same original disposal Work")
}

fn quote(
    operations: &[crate::task_execution::intent::OperationIntent],
    budget: TransportBudget,
) -> Result<(NativeAsyncActionQuote, NativeUnaryEncodingPlan), WorkError> {
    let [effect, handoff, bound, result, retirement, send] =
        actor_transport_layouts(true).map_err(|_| WorkError::ArithmeticOverflow)?;
    let local = storage_layouts().map_err(|_| WorkError::ArithmeticOverflow)?;
    let native = shared_apply_layouts()?;
    let disposal_value = [
        native.native_value,
        native.physical_value,
        native.action_value,
        native.reply_value,
    ]
    .into_iter()
    .max_by_key(Layout::size)
    .unwrap();
    let layouts = [
        effect,
        handoff,
        bound,
        result,
        retirement,
        send,
        local.transport_record,
        local.transport_registration,
        local.exit_custody,
        local.panic_sink,
        local.disposal_record,
        local.disposal_carrier,
        local.exit_custody,
        local.panic_sink,
        Layout::array::<ReportedWork>(APPLY_DISPOSAL_ROLES)
            .map_err(|_| WorkError::ArithmeticOverflow)?,
        native.origin,
        native.reply_backing,
        native.physical_return_cell,
        native.disposal_input,
        native.disposal_input,
        disposal_value,
    ];
    original_run(operations, budget, SourceLayouts { original: &layouts })
}
