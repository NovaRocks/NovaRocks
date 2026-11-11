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

//! Paid per-purpose inputs, original transport joins and canonical pump records.

use super::{
    BoundPumpWork, DiagnosticRecords, DiagnosticRegistration, DynamicFilterRecords,
    DynamicReadRegistration, PumpOutcome,
    split::{SplitFailureRecord, SplitPump, SplitRunnerPublication},
    split_source::{OriginalSplitCleanup, SplitPayloadStore, SplitSource},
};
use crate::task_execution::error::TaskExecutionError;
use crate::{
    control_runtime::{
        actor::{FrontendActor, FrontendEffect, FrontendInput},
        host::{PreparedEffectJob, SourceProgress},
        transport_withdrawal::AttemptEffectEnvelope,
    },
    task_execution::host::{
        RetirementScan,
        custody::{
            ReturnedActorTransportResult, TransportRetirement, TransportSourceEvent,
            storage_layouts,
        },
        observations::{ObservationError, backing},
        publication::OriginalHostFailure,
        transport::{BodyOutcome, BoundActorTransport, prepare_actor_pump},
    },
};
use novarocks_attempt_protocol::{
    effect::{EffectBody, EffectClass, PumpKind},
    failure::HostFailureRef,
    input::{AttemptInput, ExecutorFact},
    start::PayloadRef,
};
use novarocks_control_runtime::{
    actor_turn::{ActorRegistration, ActorRuntime, ActorTurnError, EffectTicket},
    causal_input::CausalInputObligation,
    completion::ReportSlot,
    exit_custody::ExitCustody,
    obligation::{ReservationObligation, WorkObligation, WorkRetirementObserver},
    reported_work::ReportedWork,
    transaction::{RuntimeError, ScopeHandle},
};
use novarocks_control_runtime_core::{
    capacity::{CapacityError, ReservationResources, ResourceVector},
    completion::ConsumerKind,
    identity::{ActorId, ConsumerId, Generation, InputId, ReservationId},
    mailbox::InputClass,
    owned_storage::OwnedSlotNeed,
    scope::{ScopeError, WorkKind},
};
use novarocks_coordination_contract::AttemptActionId;
use novarocks_query_application::execution_error::ExecutionFailureInput;
use std::{
    alloc::Layout,
    any::Any,
    collections::BTreeMap,
    num::NonZeroU64,
    sync::Arc,
    task::{Context, Poll, Waker},
};
use tokio::runtime::Handle;

type Position = CausalInputObligation<FrontendActor>;
type Original = Box<dyn Any + Send>;

// An original result can emit its successor before its own input retires.
// Both positions remain paid while the physical read remains serial.
pub(super) const REPLY_ROLES: usize = 2;
const ROLES_PER_PUMP: usize = REPLY_ROLES + 1;

#[derive(Clone, Copy, PartialEq, Eq)]
enum Purpose {
    DynamicControl,
    DynamicReply,
    DiagnosticRead,
    DiagnosticDisposal,
    SplitStart,
    SplitControl,
}

struct Lane {
    purpose: Purpose,
    idle: Option<Position>,
    live: Option<Live>,
}

struct Pending {
    lane: usize,
    effect: Arc<AttemptEffectEnvelope>,
    work: ReportedWork,
}

struct Live {
    effect: Arc<AttemptEffectEnvelope>,
    header: WorkRetirementObserver,
    position: Option<Position>,
    joined: bool,
}

struct Dynamic {
    pump: PayloadRef,
    records: Arc<DynamicFilterRecords>,
}

// The existing blocking release owner closes the original feedback state
// even if Core failed before issuing its StopPump effect.
struct DynamicDisposal(Arc<DynamicFilterRecords>);
impl Drop for DynamicDisposal {
    fn drop(&mut self) {
        self.0.close_external();
    }
}

pub(crate) struct AttemptPumpSource {
    registration: ActorRegistration,
    scope: ScopeHandle,
    generation: Generation,
    parent: ReservationId,
    observer: ConsumerId,
    lanes: Vec<Lane>,
    pending: Option<Pending>,
    dynamic: Option<Dynamic>,
    diagnostics: Option<Arc<DiagnosticRecords>>,
    split: Option<SplitSource>,
    split_cleanup: Option<OriginalSplitCleanup>,
    split_failure: Option<Arc<SplitFailureRecord>>,
    dynamic_failures: BTreeMap<HostFailureRef, TaskExecutionError>,
    scan: usize,
    retirement_scan: RetirementScan,
    closing: bool,
    retiring_failures: bool,
}

impl AttemptPumpSource {
    pub(crate) const fn role_count(dynamic: bool, diagnostics: bool) -> usize {
        (dynamic as usize + diagnostics as usize) * ROLES_PER_PUMP
    }

    pub(crate) fn backing_bytes(
        diagnostic_tasks: usize,
        dynamic: bool,
        diagnostics: bool,
    ) -> Result<usize, ObservationError> {
        let dynamic_bytes = if dynamic {
            DynamicFilterRecords::backing_bytes()?
        } else {
            0
        };
        let diagnostic_bytes = if diagnostics {
            DiagnosticRecords::backing_bytes(diagnostic_tasks)?
        } else {
            0
        };
        let roles = Self::role_count(dynamic, diagnostics);
        let join = storage_layouts().map_err(|_| ObservationError::Budget)?;
        let per_job = backing::sum([
            backing::arc::<AttemptEffectEnvelope>()?.size(),
            Layout::new::<BoundActorTransport>().size(),
            Layout::new::<BoundPumpWork>().size(),
            Layout::new::<TransportRetirement>().size(),
            backing::array::<AttemptInput>(
                novarocks_control_runtime::causal_input::CAUSAL_INPUT_POSITIONS,
            )?,
            join.transport_record.size(),
            join.transport_registration.size(),
            join.exit_custody.size(),
            join.panic_sink.size(),
        ])?;
        backing::sum([
            Layout::new::<Self>().size(),
            backing::array::<Lane>(roles)?,
            per_job.checked_mul(roles).ok_or(ObservationError::Budget)?,
            dynamic_bytes,
            diagnostic_bytes,
        ])
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) fn bind(
        runtime: &ActorRuntime<FrontendActor>,
        registration: &ActorRegistration,
        source: ActorId,
        parent: &WorkObligation,
        payer: &ReservationObligation,
        observer: ConsumerId,
        dynamic: Option<(PayloadRef, Arc<DynamicFilterRecords>)>,
        diagnostics: Option<Arc<DiagnosticRecords>>,
        split: Option<&Arc<SplitPump>>,
    ) -> Result<Self, ActorTurnError> {
        let scope = parent.scope();
        if !registration.matches_scope(scope) || !payer.belongs_to_bound_work(scope, parent.id()) {
            return Err(ActorTurnError::InputAuthorMismatch);
        }
        let dynamic_lanes = [
            Purpose::DynamicControl,
            Purpose::DynamicReply,
            Purpose::DynamicReply,
        ];
        let diagnostic_lanes = [
            Purpose::DiagnosticRead,
            Purpose::DiagnosticRead,
            Purpose::DiagnosticDisposal,
        ];
        let purposes = dynamic
            .is_some()
            .then_some(dynamic_lanes)
            .into_iter()
            .flatten()
            .chain(
                diagnostics
                    .is_some()
                    .then_some(diagnostic_lanes)
                    .into_iter()
                    .flatten(),
            )
            .chain(
                split
                    .map(|_| [Purpose::SplitStart, Purpose::SplitControl])
                    .into_iter()
                    .flatten(),
            );
        let reserve =
            |class| Position::reserve_admitted(runtime, registration, source, class, parent);
        let mut lanes = Vec::with_capacity(
            Self::role_count(dynamic.is_some(), diagnostics.is_some())
                + 2 * usize::from(split.is_some()),
        );
        let mut callbacks = Vec::with_capacity(2 * usize::from(split.is_some()));
        let bound = (|| {
            for purpose in purposes {
                lanes.push(Lane {
                    purpose,
                    idle: Some(reserve(InputClass::Lifecycle)?),
                    live: None,
                });
            }
            let Some(original) = split else {
                return Ok(None);
            };
            callbacks.push(reserve(InputClass::Lifecycle)?);
            callbacks.push(reserve(InputClass::Ordinary)?);
            let delivery = runtime
                .reserve_admitted_input_for(registration, source, InputClass::Ordinary, parent)?
                .into_standing_input()
                .map_err(|(error, original)| {
                    original
                        .revoke_unused()
                        .expect("unused split delivery input");
                    error
                })?;
            let profile = callbacks.pop().expect("original split profile");
            let failure = callbacks.pop().expect("original split failure");
            Ok(Some(SplitSource::new(
                original.clone(),
                delivery,
                SplitRunnerPublication::new(failure, profile),
            )))
        })();
        let split_failure = split.map(|original| original.failure_record());
        let split = match bound {
            Ok(split) => split,
            Err(error) => {
                for position in lanes
                    .iter_mut()
                    .filter_map(|lane| lane.idle.as_mut())
                    .chain(&mut callbacks)
                {
                    position
                        .close_after_exit()
                        .expect("unused original pump position");
                }
                return Err(error);
            }
        };
        Ok(Self {
            registration: registration.clone(),
            scope: scope.clone(),
            generation: registration.initial_epoch(),
            parent: payer.id(),
            observer,
            lanes,
            pending: None,
            dynamic: dynamic.map(|(pump, records)| Dynamic { pump, records }),
            diagnostics,
            split,
            split_cleanup: None,
            split_failure,
            dynamic_failures: BTreeMap::new(),
            scan: 0,
            retirement_scan: RetirementScan::default(),
            closing: false,
            retiring_failures: false,
        })
    }

    pub(crate) fn capacity(&self) -> usize {
        self.lanes.len()
    }

    pub(crate) fn matches(&self, registration: &ActorRegistration) -> bool {
        self.registration.same_registration(registration)
    }

    fn purpose(&self, effect: &AttemptEffectEnvelope) -> Option<Purpose> {
        let dynamic = self.dynamic.as_ref();
        let split = self
            .split
            .as_ref()
            .and_then(SplitSource::original)
            .map(|pump| pump.reference());
        match effect.protocol.body {
            EffectBody::StartPump {
                pump,
                kind: PumpKind::DynamicFilters,
            }
            | EffectBody::StopPump { pump }
                if dynamic.is_some_and(|row| row.pump == pump) =>
            {
                Some(Purpose::DynamicControl)
            }
            EffectBody::StartPump {
                pump,
                kind: PumpKind::SplitAssignment,
            } if split == Some(pump) => Some(Purpose::SplitStart),
            EffectBody::StopPump { pump }
            | EffectBody::PumpUpdateAdmitted { pump, .. }
            | EffectBody::PumpOperationReleased { pump, .. }
            | EffectBody::PumpOperationSettled { pump, .. }
                if split == Some(pump) =>
            {
                Some(Purpose::SplitControl)
            }
            EffectBody::ReadDynamicFilters { .. }
            | EffectBody::AdmitDynamicFilterFeedback { .. }
                if dynamic.is_some() =>
            {
                Some(Purpose::DynamicReply)
            }
            EffectBody::CollectDiagnostics { .. } if self.diagnostics.is_some() => {
                Some(Purpose::DiagnosticRead)
            }
            EffectBody::DisposeDiagnosticInfo { .. } if self.diagnostics.is_some() => {
                Some(Purpose::DiagnosticDisposal)
            }
            _ => None,
        }
    }

    pub(crate) fn handles(&self, effect: &AttemptEffectEnvelope) -> bool {
        self.purpose(effect).is_some()
    }

    pub(crate) fn prepay(
        &mut self,
        effect: &Arc<AttemptEffectEnvelope>,
    ) -> Result<Option<Position>, RuntimeError> {
        assert!(self.pending.is_none());
        let purpose = self.purpose(effect).expect("original pump role");
        if self.closing || !self.ready(effect) {
            return Ok(None);
        }
        let Some(lane) = self
            .lanes
            .iter()
            .position(|lane| lane.purpose == purpose && lane.idle.is_some())
        else {
            return Ok(None);
        };
        let kind = if matches!(effect.protocol.class, EffectClass::Cleanup) {
            WorkKind::Cleanup
        } else {
            // Core has already admitted this effect. A concurrent orderly
            // stop still owes the exact result for any issued request.
            WorkKind::Settlement
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
        self.pending = Some(Pending {
            lane,
            effect: effect.clone(),
            work,
        });
        Ok(self.lanes[lane].idle.take())
    }

    fn ready(&self, effect: &AttemptEffectEnvelope) -> bool {
        match effect.protocol.body {
            EffectBody::ReadDynamicFilters { .. } => {
                self.dynamic_records().read_count() < REPLY_ROLES
            }
            EffectBody::CollectDiagnostics { .. } => {
                self.diagnostics.as_ref().unwrap().read_count() < REPLY_ROLES
            }
            _ => true,
        }
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
        let (work, mut payer) = guard.bind(kind).expect("original pump guard");
        let report = ReportSlot::reserve_next(
            &mut payer,
            generation,
            1,
            [(self.observer, ConsumerKind::Observer)],
        )
        .expect("original pump exit report");
        Ok(ReportedWork::try_new(work, payer, report).expect("same original pump work"))
    }

    pub(crate) fn bind_job(
        &mut self,
        ticket: &EffectTicket<FrontendEffect>,
        position: Position,
    ) -> PreparedEffectJob {
        let Pending { lane, effect, work } = self.pending.take().expect("original pump prepayment");
        assert!(
            matches!(&ticket.effect, FrontendEffect::Attempt(original) if Arc::ptr_eq(original, &effect))
        );
        let header = work.work().retirement_observer();
        let job = prepare_actor_pump(
            effect.clone(),
            position,
            work,
            self.observer,
            self.generation,
            |custody| self.bind_work(lane, effect.clone(), custody),
        )
        .unwrap_or_else(|refused| std::panic::resume_unwind(Box::new(refused)));
        assert!(
            self.lanes[lane]
                .live
                .replace(Live {
                    effect,
                    header,
                    position: None,
                    joined: false
                })
                .is_none()
        );
        job
    }

    fn bind_work(
        &mut self,
        lane: usize,
        effect: Arc<AttemptEffectEnvelope>,
        custody: ExitCustody,
    ) -> BoundPumpWork {
        match self.lanes[lane].purpose {
            Purpose::SplitStart => {
                let runner = self
                    .split
                    .as_mut()
                    .expect("original split source")
                    .bind_runner(effect.protocol.id, custody)
                    .expect("original split runner");
                return BoundPumpWork::StartSplit(effect, runner);
            }
            Purpose::SplitControl => {
                let original = self
                    .split
                    .as_ref()
                    .and_then(SplitSource::original)
                    .expect("original split control");
                return BoundPumpWork::ControlSplit(effect, original.clone());
            }
            _ => {}
        }
        let key = NonZeroU64::new(effect.protocol.id.get()).expect("original action identity");
        let failure = HostFailureRef(key);
        match effect.protocol.body {
            EffectBody::StartPump { .. } => BoundPumpWork::Start(effect),
            EffectBody::StopPump { .. } => {
                BoundPumpWork::Stop(effect, self.dynamic_records().clone())
            }
            EffectBody::ReadDynamicFilters { request, task, .. } => {
                let records = self.dynamic_records();
                records
                    .register(DynamicReadRegistration {
                        request,
                        task,
                        feedback: PayloadRef(key),
                        failure,
                        custody,
                    })
                    .unwrap_or_else(|(error, original)| {
                        std::panic::resume_unwind(Box::new((error, original)))
                    });
                BoundPumpWork::Read(effect, records.clone())
            }
            EffectBody::AdmitDynamicFilterFeedback {
                request,
                task,
                feedback,
                ..
            } => {
                let records = self.dynamic_records();
                records
                    .register_admission(request, task, feedback, custody)
                    .unwrap_or_else(|original| std::panic::resume_unwind(Box::new(original)));
                BoundPumpWork::Admit(effect, records.clone())
            }
            EffectBody::CollectDiagnostics { request, task, .. } => {
                let records = self
                    .diagnostics
                    .as_ref()
                    .expect("original diagnostic source");
                records
                    .register(DiagnosticRegistration {
                        request,
                        task,
                        failure,
                        custody,
                        #[cfg(test)]
                        original_retention: None,
                    })
                    .unwrap_or_else(|(error, original)| {
                        std::panic::resume_unwind(Box::new((error, original)))
                    });
                BoundPumpWork::Collect(effect, records.clone())
            }
            EffectBody::DisposeDiagnosticInfo { .. } => BoundPumpWork::Dispose(
                effect,
                self.diagnostics
                    .as_ref()
                    .expect("original diagnostic source")
                    .clone(),
            ),
            _ => unreachable!("original pump effect"),
        }
    }

    fn dynamic_records(&self) -> &Arc<DynamicFilterRecords> {
        &self
            .dynamic
            .as_ref()
            .expect("original dynamic filter source")
            .records
    }

    pub(crate) fn return_unclaimed(&mut self, position: Position) {
        let Pending { lane, work, .. } = self
            .pending
            .take()
            .expect("original unclaimed pump position");
        let header = work.work().retirement_observer();
        let (custody, exit) = ExitCustody::from_admitted(work);
        exit.revoke_unused_and_exit()
            .expect("unused original pump work");
        assert!(custody.try_retire().expect("unused original pump report"));
        assert!(
            self.scope
                .reap_observed_header(&header)
                .expect("unused original pump header")
        );
        assert!(self.lanes[lane].idle.replace(position).is_none());
    }

    fn lane(&self, effect: AttemptActionId) -> Option<usize> {
        self.lanes.iter().position(|lane| {
            lane.live
                .as_ref()
                .is_some_and(|live| live.effect.protocol.id == effect)
        })
    }

    pub(crate) fn owns(&self, effect: AttemptActionId) -> bool {
        self.lane(effect).is_some()
    }

    pub(crate) fn transport_event(
        &mut self,
        effect: AttemptActionId,
        event: TransportSourceEvent,
    ) -> Result<(), Original> {
        self.retirement_scan.request();
        let lane = self.lane(effect).expect("same original pump lane");
        let live = self.lanes[lane].live.as_mut().unwrap();
        match event {
            TransportSourceEvent::Result(original) => {
                assert!(Arc::ptr_eq(&original.effect, &live.effect));
                let ReturnedActorTransportResult {
                    mut original,
                    outcome,
                    ..
                } = original;
                let result = match outcome {
                    Some(BodyOutcome::Pump(PumpOutcome::Completed(fact))) => {
                        if let Some(fact) = fact {
                            let predecessor = matches!(
                                live.effect.protocol.body,
                                EffectBody::CollectDiagnostics { .. }
                            )
                            .then(|| executor_input(ExecutorFact::ActualExit { effect }));
                            match original.fill_with_receipt(predecessor, executor_input(fact)) {
                                Ok(_) => Ok(()),
                                // Any accepted predecessor stays in its original runtime slot.
                                // The refused values contain only neutral pump facts.
                                Err(refused)
                                    if matches!(
                                        refused.error,
                                        ActorTurnError::ActorFailed | ActorTurnError::Closed
                                    ) =>
                                {
                                    Ok(())
                                }
                                Err(refused) => Err(Box::new(refused) as Original),
                            }
                        } else {
                            Ok(())
                        }
                    }
                    Some(BodyOutcome::Pump(PumpOutcome::Fenced)) => Ok(()),
                    original => Err(Box::new(original) as Original),
                };
                assert!(live.position.replace(original).is_none());
                result
            }
            TransportSourceEvent::Retired => {
                assert!(!std::mem::replace(&mut live.joined, true));
                Ok(())
            }
            original => Err(Box::new(original)),
        }
    }

    pub(crate) fn core_consumed(
        &mut self,
        runtime: &ActorRuntime<FrontendActor>,
        inputs: &[InputId],
    ) {
        if !inputs.is_empty()
            && (self
                .split
                .as_ref()
                .is_some_and(|split| inputs.iter().any(|&input| split.owns_input(input)))
                || self.lanes.iter().any(|lane| {
                    lane.live
                        .as_ref()
                        .and_then(|live| live.position.as_ref())
                        .is_some_and(|position| {
                            position
                                .in_flight()
                                .expect("original pump result positions")
                                .into_iter()
                                .flatten()
                                .any(|input| inputs.contains(&input))
                        })
                }))
        {
            self.retirement_scan.request();
            runtime
                .notify_host_actor(&self.registration)
                .expect("original pump wake");
        }
    }

    pub(crate) fn project_failure(
        &self,
        reference: HostFailureRef,
    ) -> Option<ExecutionFailureInput> {
        self.dynamic
            .as_ref()
            .and_then(|dynamic| dynamic.records.project_failure(reference))
            .or_else(|| {
                self.dynamic_failures
                    .get(&reference)
                    .map(TaskExecutionError::preparation_input)
            })
            .or_else(|| self.split_failure.as_ref()?.project_failure(reference))
    }

    pub(crate) fn take_failure(
        &mut self,
        reference: HostFailureRef,
    ) -> Option<OriginalHostFailure> {
        self.dynamic
            .as_ref()
            .and_then(|dynamic| dynamic.records.take_failure(reference))
            .or_else(|| self.dynamic_failures.remove(&reference))
            .or_else(|| self.split_failure.as_ref()?.take_failure(reference))
            .map(OriginalHostFailure::Task)
    }

    pub(crate) fn is_drained(&self) -> bool {
        self.local_exited() && self.split_failure.is_none() && self.dynamic_failures.is_empty()
    }

    pub(crate) fn local_exited(&self) -> bool {
        (self.closing || self.lanes.is_empty())
            && self.dynamic.is_none()
            && self.diagnostics.is_none()
            && self.split.as_ref().is_none_or(SplitSource::is_drained)
            && self
                .lanes
                .iter()
                .all(|lane| lane.idle.is_none() && lane.live.is_none())
            && self.pending.is_none()
    }

    pub(crate) fn has_split_work(&self) -> bool {
        self.split.as_ref().is_some_and(|split| !split.is_drained())
    }

    pub(crate) fn progress_split(
        &mut self,
        failed: bool,
        closing: bool,
        core_drained: bool,
        can_release: bool,
        payloads: &mut impl SplitPayloadStore,
        executor: &Handle,
        wake: &Waker,
        dispose: impl FnOnce(Original),
    ) -> Result<bool, Original> {
        if let Some(cleanup) = &mut self.split_cleanup {
            return match cleanup.poll_retired(&self.scope, &mut Context::from_waker(wake)) {
                Poll::Pending => Ok(false),
                Poll::Ready(Err(error)) => Err(Box::new(error)),
                Poll::Ready(Ok(result)) => {
                    self.split_cleanup.take();
                    self.retirement_scan.request();
                    let split = self.split.as_mut().expect("original split source");
                    if let Some(unresolved) =
                        split.original().and_then(|original| original.unresolved())
                    {
                        return Err(Box::new(unresolved));
                    }
                    split
                        .runner_retired(None)
                        .map_err(|error| Box::new(error) as Original)?;
                    result
                        .map(|()| true)
                        .map_err(|error| Box::new(error) as Original)
                }
            };
        }
        let Some(split) = &self.split else {
            return Ok(false);
        };
        if split.dormant_cleanup_ready() {
            let generation = self
                .scope
                .gates()
                .map_err(|error| Box::new(error) as Original)?
                .generation;
            let work = match self.reserve_work(generation, WorkKind::Cleanup) {
                Ok(work) => work,
                Err(
                    RuntimeError::Scope(ScopeError::GenerationMismatch)
                    | RuntimeError::Capacity(CapacityError::Scope(ScopeError::GenerationMismatch)),
                ) => return Ok(true),
                Err(error) => return Err(Box::new(error)),
            };
            let split = self.split.as_mut().expect("original split source");
            self.split_cleanup = Some(OriginalSplitCleanup::start(
                split,
                executor,
                work,
                self.observer,
            ));
            return Ok(true);
        }
        // The original control effect can still have the Native delivery
        // as a capture even after the provider runner itself has returned.
        let controls_exited = self
            .lanes
            .iter()
            .all(|lane| lane.purpose != Purpose::SplitControl || lane.live.is_none());
        let advanced = self
            .split
            .as_mut()
            .expect("original split source")
            .progress(
                failed,
                closing,
                core_drained && controls_exited,
                can_release,
                payloads,
                dispose,
            )
            .map_err(|error| Box::new(error) as Original)?;
        if advanced {
            self.retirement_scan.request();
        }
        Ok(advanced)
    }

    pub(crate) fn request_retirement_scan(&mut self) {
        self.retirement_scan.request();
    }

    pub(crate) fn needs_retirement_scan(&self, closing: bool, retire_failures: bool) -> bool {
        !self.is_drained()
            && (closing && !self.closing
                || retire_failures && !self.retiring_failures
                || self.retirement_scan.has_work(self.lanes.len() + 1))
    }

    pub(crate) fn progress(
        &mut self,
        failed: bool,
        closing: bool,
        retire_failures: bool,
        budget: usize,
        can_release: bool,
        mut dispose: impl FnMut(Original),
    ) -> SourceProgress {
        if !self.closing && (failed || closing) {
            self.closing = true;
            self.retirement_scan.request();
        }
        if retire_failures && !self.retiring_failures {
            self.retiring_failures = true;
            self.retirement_scan.request();
        }
        let mut units = 0;
        let mut progressed = false;
        if self.is_drained() {
            return SourceProgress {
                units: 0,
                progressed: false,
                more: false,
                retired: true,
            };
        }
        let visits = self.lanes.len() + 1;
        let mut release = can_release;
        for _ in 0..self.retirement_scan.take_visits(visits, budget) {
            let lane = self.scan;
            self.scan = (self.scan + 1) % visits;
            units += 1;
            if lane < self.lanes.len() {
                progressed |= self
                    .advance_lane(lane, failed)
                    .expect("original pump input retirement");
            } else if release && let Some(original) = self.take_released(retire_failures) {
                dispose(original);
                release = false;
                progressed = true;
            }
        }
        if progressed {
            self.retirement_scan.request();
        }
        SourceProgress {
            units,
            progressed,
            more: self.retirement_scan.has_work(visits) && !self.is_drained(),
            retired: self.is_drained(),
        }
    }

    fn advance_lane(&mut self, lane: usize, failed: bool) -> Result<bool, ActorTurnError> {
        let role = &mut self.lanes[lane];
        let mut advanced = false;
        if let Some(live) = role.live.as_ref()
            && live.joined
            && let Some(position) = live.position.as_ref()
        {
            if failed {
                match position.take_failed_payload() {
                    Ok(Some(neutral)) => {
                        neutral.dispose()?;
                        return Ok(true);
                    }
                    Ok(None) | Err(ActorTurnError::InputAuthorMismatch) => {}
                    Err(error) => return Err(error),
                }
            }
            if position.retire_ready_after_exit()?
                && self.scope.reap_observed_header(&live.header)?
            {
                let live = role.live.take().unwrap();
                let position = live.position.expect("original returned pump position");
                assert!(role.idle.replace(position).is_none());
                if role.purpose == Purpose::SplitStart {
                    let split = self.split.as_mut().expect("original split source");
                    if split
                        .original()
                        .is_some_and(|original| original.unresolved().is_some())
                    {
                        // The original provider obligation stays with SplitSource.
                        // The raw health failure was returned by the runner.
                        return Ok(true);
                    }
                    split
                        .runner_retired(Some(live.effect.protocol.id))
                        .expect("original split runner retirement");
                }
                match live.effect.protocol.body {
                    EffectBody::ReadDynamicFilters { request, .. } => self
                        .dynamic_records()
                        .resolve_result(request, false)
                        .expect("original retired dynamic read"),
                    EffectBody::AdmitDynamicFilterFeedback { request, .. } => self
                        .dynamic_records()
                        .resolve_result(request, true)
                        .expect("original retired feedback admission"),
                    EffectBody::CollectDiagnostics { request, .. } => self
                        .diagnostics
                        .as_ref()
                        .unwrap()
                        .resolve_result(request)
                        .expect("original retired diagnostic read"),
                    _ => {}
                }
                return Ok(true);
            }
        }
        if self.closing
            && let Some(mut idle) = role.idle.take()
        {
            idle.close_after_exit()?;
            advanced = true;
        }
        Ok(advanced)
    }

    fn take_released(&mut self, retire_failures: bool) -> Option<Original> {
        if let Some(dynamic) = &self.dynamic
            && let Some((failure, original)) = dynamic.records.take_retired(self.closing)
        {
            if let Some((reference, failure)) = failure {
                assert!(self.dynamic_failures.insert(reference, failure).is_none());
            }
            return Some(original);
        }
        if let Some(diagnostics) = &self.diagnostics
            && let Some(original) = diagnostics.take_retired_one()
        {
            return Some(original);
        }
        if !self.closing || self.lanes.iter().any(|lane| lane.live.is_some()) {
            return None;
        }
        if let Some(dynamic) = self.dynamic.take() {
            assert_eq!(dynamic.records.read_count(), 0);
            return Some(Box::new(DynamicDisposal(dynamic.records)));
        }
        if let Some(diagnostics) = &self.diagnostics {
            assert_eq!(diagnostics.read_count(), 0);
            if let Some(original) = diagnostics.take_closing_information() {
                return Some(original);
            }
        }
        if let Some(original) = self.diagnostics.take() {
            return Some(Box::new(original));
        }
        // The local pump cut may precede delivery of the terminal selected
        // from these failures. Only final result-admission closure releases them.
        if retire_failures {
            if let Some((_, original)) = self.dynamic_failures.pop_first() {
                return Some(Box::new(original));
            }
            if self.split.as_ref().is_none_or(SplitSource::is_drained) {
                return self
                    .split_failure
                    .take()
                    .map(|original| Box::new(original) as Original);
            }
        }
        None
    }
}

fn executor_input(fact: ExecutorFact) -> FrontendInput {
    FrontendInput::Attempt(Box::new(AttemptInput::Executor(fact)))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        native::data_runtime::FrontendDataRuntime,
        query_execution::{
            split_assignment::SplitAssignmentDriverError,
            split_assignment_round::failure_observation_plan,
        },
        task_execution::{
            blocking_io::tests::{OriginalExecutorsFixture, until},
            error::TaskExecutionError,
            host::{
                custody::tests::WithdrawalInbox,
                pumps::{split::SplitPayloadRegistration, split_source::SplitPayloadError},
            },
            status_intake::CountingWake,
        },
    };
    use novarocks_control_runtime::transaction::RuntimeLedger;
    use novarocks_control_runtime_core::scope::WorkAdmission;
    use novarocks_proto_codec::lifecycle::QueryExecutionId;
    use novarocks_types::{AttemptId, QueryId};
    use std::{
        collections::BTreeMap,
        sync::{atomic::Ordering, mpsc},
        time::Duration,
    };

    struct NoPayloads;
    impl SplitPayloadStore for NoPayloads {
        fn install(
            &mut self,
            _: &SplitPayloadRegistration,
        ) -> Result<Option<PayloadRef>, SplitPayloadError> {
            panic!("dormant split does not produce a delivery")
        }
        fn detach(&mut self, _: PayloadRef) -> Result<Option<Original>, SplitPayloadError> {
            panic!("dormant split has no retained request")
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn original_source_joins_dormant_split_close_before_returning_its_roles() {
        let handle = Handle::current();
        let data = FrontendDataRuntime::new(handle.clone());
        let blocking = data.connector_blocking_io().clone();
        let mut workers = OriginalExecutorsFixture::install(&blocking);
        let execution =
            QueryExecutionId::new(QueryId::new(19, 25), AttemptId::new(1).unwrap()).unwrap();
        let (release, released) = mpsc::channel();
        let (plan, enumerations, closes) =
            failure_observation_plan(execution, blocking, Some(released));
        let original = SplitPump::prepare(
            PayloadRef(NonZeroU64::MIN),
            execution,
            plan,
            BTreeMap::new(),
            Arc::new(CountingWake::default()),
            data,
            Arc::default(),
        )
        .unwrap();

        let ledger = RuntimeLedger::new(ResourceVector::default());
        let generation = Generation::new(1).unwrap();
        let scope = ledger.open_scope_next(None, generation).unwrap();
        let (mut parent, mut payer) = scope
            .reserve_next(
                generation,
                ReservationResources::work_only(),
                WorkKind::Cleanup,
                WorkAdmission::External,
            )
            .unwrap()
            .bind(WorkKind::Cleanup)
            .unwrap();
        parent.start().unwrap();
        let inbox = WithdrawalInbox::with_commands(&scope, [], [16; 4]);
        let mut source = AttemptPumpSource::bind(
            inbox.runtime(),
            inbox.registration(),
            inbox.source(),
            &parent,
            &payer,
            ConsumerId::new(1).unwrap(),
            None,
            None,
            Some(&original),
        )
        .unwrap();
        let wake = Waker::noop();
        let mut payloads = NoPayloads;
        assert!(
            source
                .progress_split(false, true, true, true, &mut payloads, &handle, wake, drop)
                .unwrap()
        );
        assert!(
            source
                .progress_split(false, true, true, true, &mut payloads, &handle, wake, drop)
                .unwrap()
        );
        until(|| closes.load(Ordering::SeqCst) == 1);
        assert!(
            !source
                .progress_split(false, true, true, true, &mut payloads, &handle, wake, drop)
                .unwrap()
        );
        source.progress(false, true, true, 4, true, drop);
        assert!(!source.is_drained());
        assert!(!scope.join_ready().unwrap());

        release.send(()).unwrap();
        let mut original_failure = None;
        tokio::time::timeout(Duration::from_secs(2), async {
            while !source.is_drained() {
                if let Err(error) = source.progress_split(
                    false,
                    true,
                    true,
                    true,
                    &mut payloads,
                    &handle,
                    wake,
                    drop,
                ) {
                    assert!(original_failure.is_none());
                    original_failure = Some(
                        *error
                            .downcast::<TaskExecutionError>()
                            .expect("original provider failure"),
                    );
                }
                source.progress(false, true, true, 4, true, drop);
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        assert!(matches!(
            original_failure,
            Some(TaskExecutionError::SplitAssignment(
                SplitAssignmentDriverError::SourceClose { .. }
            ))
        ));
        assert_eq!(closes.load(Ordering::SeqCst), 1);
        assert_eq!(enumerations.load(Ordering::SeqCst), 0);
        inbox.assert_no_result();
        parent.body_exited().unwrap();
        parent.captures_exited().unwrap();
        payer.finish().unwrap();
        scope.close_external().unwrap();
        ledger.reap_scope_ready(scope.id(), 32).unwrap();
        assert!(scope.join_ready().unwrap());
        inbox
            .runtime()
            .close_actor_admission(inbox.registration().actor())
            .unwrap();
        while let Some(turn) = inbox.runtime().claim_host_actor() {
            turn.release(false).unwrap();
        }
        inbox
            .runtime()
            .seal_actor(inbox.registration().actor())
            .unwrap();
        inbox.finish();
        workers.shutdown().await;
    }
}
